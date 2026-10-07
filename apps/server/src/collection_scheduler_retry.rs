//! Bounded scheduler SQL retries and dispatch-local contention recovery.
//! Database retries exclude Kubernetes creation and uncertain storage outcomes.
use openlegal_domain::legal::DatabaseError;
use openlegal_server::ServerError;
use std::{future::Future, time::Duration};
use tokio_util::sync::CancellationToken;

pub(crate) async fn retry_storage<T, F, Fut>(
    cancel: &CancellationToken,
    mut operation: F,
) -> Result<T, DatabaseError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, DatabaseError>>,
{
    let mut backoff = [100, 200, 400].into_iter();
    loop {
        if cancel.is_cancelled() {
            return Err(DatabaseError::Cancelled);
        }
        match operation().await {
            Err(DatabaseError::StorageContended) => {
                let Some(delay) = backoff.next() else {
                    return Err(DatabaseError::StorageContended);
                };
                tracing::warn!(
                    retry_delay_ms = delay,
                    "collection scheduler storage contention"
                );
                tokio::select! {
                    _ = cancel.cancelled() => return Err(DatabaseError::Cancelled),
                    _ = tokio::time::sleep(Duration::from_millis(delay)) => {}
                }
            }
            result => return result,
        }
    }
}

/// Yield only the dispatch cycle after its bounded SQL retries are exhausted.
/// Heartbeats deliberately do not use this helper: failure to establish their
/// liveness remains fatal, including a server-reported SQL rejection.
pub(crate) async fn dispatch_storage_result<T>(
    cancel: &CancellationToken,
    stage: &'static str,
    result: Result<T, ServerError>,
) -> Result<Option<T>, ServerError> {
    match result {
        Ok(value) => Ok(Some(value)),
        Err(error)
            if error.downcast_ref::<DatabaseError>() == Some(&DatabaseError::StorageContended) =>
        {
            tracing::warn!(
                stage,
                error_category = "StorageContended",
                "collection dispatch yielded until the next cycle"
            );
            tokio::select! {
                _ = cancel.cancelled() => {},
                _ = tokio::time::sleep(Duration::from_secs(5)) => {},
            }
            Ok(None)
        }
        Err(error) => Err(error),
    }
}

/// Create exactly once, then retry only the marker for the captured launch epoch.
/// If that marker is still rejected, retain the durable `launching` claim and
/// its lease. The request Pod can settle it directly and reconciliation can use
/// its epoch-derived Job name; the dispatcher must never create it again.
pub(crate) async fn create_and_mark_launch<C, F, Fut>(
    cancel: &CancellationToken,
    create: C,
    mark: F,
) -> Result<(), ServerError>
where
    C: Future<Output = Result<(), ServerError>>,
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<(), DatabaseError>>,
{
    if cancel.is_cancelled() {
        return Err(DatabaseError::Cancelled.into());
    }
    create.await?;
    dispatch_storage_result(
        cancel,
        "launch_marker",
        retry_storage(cancel, mark).await.map_err(ServerError::from),
    )
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn repeated_dispatch_exhaustion_does_not_cancel_an_inflight_provider_sibling() {
        use std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        };
        let cancel = CancellationToken::new();
        let sibling_token = cancel.child_token();
        let settled = Arc::new(AtomicUsize::new(0));
        let completed = settled.clone();
        let sibling = tokio::spawn(async move {
            tokio::select! {
                _ = sibling_token.cancelled() => Err(DatabaseError::Cancelled),
                _ = tokio::time::sleep(Duration::from_secs(2)) => {
                    completed.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            }
        });
        let mut attempts = 0;
        for _ in 0..2 {
            let retry = retry_storage(&cancel, || {
                attempts += 1;
                std::future::ready(Err::<(), _>(DatabaseError::StorageContended))
            })
            .await;
            let result =
                dispatch_storage_result(&cancel, "fixture_claim", retry.map_err(ServerError::from))
                    .await
                    .unwrap();
            assert!(result.is_none());
        }
        assert_eq!(
            attempts, 8,
            "each new cycle has its own four bounded SQL attempts"
        );
        sibling.await.unwrap().unwrap();
        assert_eq!(settled.load(Ordering::SeqCst), 1);
        assert!(!cancel.is_cancelled());
        let recovered = dispatch_storage_result(&cancel, "fixture_claim", Ok(19))
            .await
            .unwrap();
        assert_eq!(recovered, Some(19));
    }

    #[tokio::test(start_paused = true)]
    async fn created_job_marker_exhaustion_never_replays_creation() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let cancel = CancellationToken::new();
        let creates = AtomicUsize::new(0);
        let markers = AtomicUsize::new(0);
        create_and_mark_launch(
            &cancel,
            async {
                creates.fetch_add(1, Ordering::SeqCst);
                Ok(())
            },
            || {
                markers.fetch_add(1, Ordering::SeqCst);
                std::future::ready(Err(DatabaseError::StorageContended))
            },
        )
        .await
        .unwrap();
        assert_eq!(creates.load(Ordering::SeqCst), 1);
        assert_eq!(markers.load(Ordering::SeqCst), 4);
        assert!(!cancel.is_cancelled());
    }

    #[tokio::test(start_paused = true)]
    async fn creation_uncertainty_marker_uncertainty_and_lost_ownership_remain_fatal() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let cancel = CancellationToken::new();
        let markers = AtomicUsize::new(0);
        let unknown_creation = create_and_mark_launch(
            &cancel,
            async { Err(DatabaseError::StorageUnavailable.into()) },
            || {
                markers.fetch_add(1, Ordering::SeqCst);
                std::future::ready(Ok(()))
            },
        )
        .await
        .unwrap_err();
        assert_eq!(
            unknown_creation.downcast_ref::<DatabaseError>(),
            Some(&DatabaseError::StorageUnavailable)
        );
        assert_eq!(markers.load(Ordering::SeqCst), 0);
        for failure in [
            DatabaseError::StorageUnavailable,
            DatabaseError::StorageCorrupt,
            DatabaseError::Conflict,
        ] {
            markers.store(0, Ordering::SeqCst);
            let failed = create_and_mark_launch(&cancel, async { Ok(()) }, || {
                markers.fetch_add(1, Ordering::SeqCst);
                std::future::ready(Err(failure))
            })
            .await
            .unwrap_err();
            assert_eq!(failed.downcast_ref::<DatabaseError>(), Some(&failure));
            assert_eq!(markers.load(Ordering::SeqCst), 1);
        }
        // The heartbeat uses retry_storage directly, so even known exhausted
        // contention still reaches the scheduler supervisor as a fatal error.
        let heartbeat = retry_storage(&cancel, || {
            std::future::ready(Err::<(), _>(DatabaseError::StorageContended))
        })
        .await;
        assert_eq!(heartbeat, Err(DatabaseError::StorageContended));
    }

    #[tokio::test(start_paused = true)]
    async fn dispatch_cooldown_is_cancel_aware_and_does_not_soften_other_errors() {
        let cancel = CancellationToken::new();
        let token = cancel.clone();
        let start = tokio::time::Instant::now();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            token.cancel();
        });
        assert!(
            dispatch_storage_result::<()>(
                &cancel,
                "fixture",
                Err(DatabaseError::StorageContended.into())
            )
            .await
            .unwrap()
            .is_none()
        );
        assert_eq!(start.elapsed(), Duration::from_millis(50));
        for failure in [
            DatabaseError::StorageUnavailable,
            DatabaseError::StorageCorrupt,
            DatabaseError::Conflict,
        ] {
            let error = dispatch_storage_result::<()>(
                &CancellationToken::new(),
                "fixture",
                Err(failure.into()),
            )
            .await
            .unwrap_err();
            assert_eq!(error.downcast_ref::<DatabaseError>(), Some(&failure));
        }
    }

    #[tokio::test(start_paused = true)]
    async fn retries_only_known_rejections_and_stops_after_four_attempts() {
        let cancel = CancellationToken::new();
        let start = tokio::time::Instant::now();
        let mut attempts = 0;
        let result = retry_storage(&cancel, || {
            attempts += 1;
            std::future::ready(Err::<(), _>(DatabaseError::StorageContended))
        })
        .await;
        assert_eq!(result, Err(DatabaseError::StorageContended));
        assert_eq!(attempts, 4);
        assert_eq!(start.elapsed(), Duration::from_millis(700));

        for error in [
            DatabaseError::StorageUnavailable,
            DatabaseError::StorageCorrupt,
            DatabaseError::Conflict,
            DatabaseError::Capacity,
        ] {
            let mut attempts = 0;
            let result = retry_storage(&cancel, || {
                attempts += 1;
                std::future::ready(Err::<(), _>(error))
            })
            .await;
            assert_eq!(result, Err(error));
            assert_eq!(
                attempts, 1,
                "uncertain outcomes and lost ownership cannot retry"
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn recovered_operation_returns_once_and_cancellation_interrupts_backoff() {
        let cancel = CancellationToken::new();
        let mut attempts = 0;
        assert_eq!(
            retry_storage(&cancel, || {
                attempts += 1;
                std::future::ready(if attempts < 3 {
                    Err(DatabaseError::StorageContended)
                } else {
                    Ok("recovered")
                })
            })
            .await,
            Ok("recovered")
        );
        assert_eq!(attempts, 3);

        let cancel_task = cancel.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            cancel_task.cancel();
        });
        let mut attempts = 0;
        let result = retry_storage(&cancel, || {
            attempts += 1;
            std::future::ready(Err::<(), _>(DatabaseError::StorageContended))
        })
        .await;
        assert_eq!(result, Err(DatabaseError::Cancelled));
        assert_eq!(attempts, 1);
    }
}
