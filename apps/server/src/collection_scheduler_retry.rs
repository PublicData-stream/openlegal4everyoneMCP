//! Bounded retries for explicitly selected scheduler database operations.
//! A Kubernetes creation attempt and an uncertain storage outcome never enter here.
use openlegal_domain::legal::DatabaseError;
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

#[cfg(test)]
mod tests {
    use super::*;

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
