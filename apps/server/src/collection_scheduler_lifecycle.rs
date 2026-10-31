//! Keep both collector supervisors alive until their cancellation drainage ends.
use openlegal_domain::legal::DatabaseError;
use openlegal_server::ServerError;
use std::future::Future;
use tokio_util::sync::CancellationToken;

/// A completed sibling must not drop the runtime's JoinSet before its own drain.
/// This preserves settlement already in progress; it does not change how an
/// in-flight provider request handles cancellation or uncertain outcomes.
pub(crate) async fn run_and_drain<S, B>(
    scheduler: S,
    background: B,
    cancel: CancellationToken,
) -> Result<(), ServerError>
where
    S: Future<Output = Result<(), ServerError>>,
    B: Future<Output = Result<(), ServerError>>,
{
    // Check only on the first poll. A never-started sibling must not begin
    // startup writes after cancellation; an already-started sibling still drains.
    let scheduler_cancel = cancel.clone();
    let scheduler = async move {
        if scheduler_cancel.is_cancelled() {
            return Ok(());
        }
        scheduler.await
    };
    let background_cancel = cancel.clone();
    let background = async move {
        if background_cancel.is_cancelled() {
            return Ok(());
        }
        background.await
    };
    tokio::pin!(scheduler, background);
    let (primary, secondary, stage) = tokio::select! {
        result = &mut scheduler => {
            cancel.cancel();
            (result, background.await, "background")
        },
        result = &mut background => {
            cancel.cancel();
            (result, scheduler.await, "scheduler")
        },
    };
    let secondary = match secondary {
        Err(error) if error.downcast_ref::<DatabaseError>() == Some(&DatabaseError::Cancelled) => {
            Ok(())
        }
        result => result,
    };
    if let Err(error) = primary {
        if secondary.is_err() {
            tracing::warn!(stage, "collection sibling failed during cancellation drain");
        }
        return Err(error);
    }
    secondary
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };
    use tokio::{sync::oneshot, task::JoinSet, time::Duration};

    struct DrainGuard {
        finished: Arc<AtomicBool>,
        dropped_early: Arc<AtomicBool>,
    }

    impl Drop for DrainGuard {
        fn drop(&mut self) {
            if !self.finished.load(Ordering::SeqCst) {
                self.dropped_early.store(true, Ordering::SeqCst);
            }
        }
    }

    async fn bounded<T>(receiver: oneshot::Receiver<T>) -> T {
        tokio::time::timeout(Duration::from_secs(1), receiver)
            .await
            .expect("fixture must make progress")
            .expect("fixture sender must survive drainage")
    }

    #[tokio::test(start_paused = true)]
    async fn heartbeat_failure_awaits_runtime_joinset_settlement_before_close() {
        let cancel = CancellationToken::new();
        let background_cancel = cancel.clone();
        let settled = Arc::new(AtomicBool::new(false));
        let drained = Arc::new(AtomicBool::new(false));
        let dropped_early = Arc::new(AtomicBool::new(false));
        let (settling_tx, settling_rx) = oneshot::channel();
        let (draining_tx, draining_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let child_settled = settled.clone();
        let child_dropped = dropped_early.clone();
        let background_drained = drained.clone();
        let background_dropped = dropped_early.clone();
        let background = async move {
            let _guard = DrainGuard {
                finished: background_drained.clone(),
                dropped_early: background_dropped,
            };
            let mut tasks = JoinSet::new();
            tasks.spawn(async move {
                let _guard = DrainGuard {
                    finished: child_settled.clone(),
                    dropped_early: child_dropped,
                };
                settling_tx.send(()).unwrap();
                release_rx.await.unwrap();
                child_settled.store(true, Ordering::SeqCst);
            });
            background_cancel.cancelled().await;
            draining_tx.send(()).unwrap();
            while let Some(result) = tasks.join_next().await {
                result.unwrap();
            }
            background_drained.store(true, Ordering::SeqCst);
            Ok(())
        };
        let scheduler = async {
            bounded(settling_rx).await;
            Err(DatabaseError::StorageContended.into())
        };
        let task = tokio::spawn(run_and_drain(scheduler, background, cancel.clone()));
        bounded(draining_rx).await;
        assert!(cancel.is_cancelled());
        assert!(!task.is_finished());
        assert!(!settled.load(Ordering::SeqCst));
        assert!(!dropped_early.load(Ordering::SeqCst));
        tokio::task::yield_now().await;
        release_tx.send(()).unwrap();
        let error = tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert_eq!(
            error.downcast_ref::<DatabaseError>(),
            Some(&DatabaseError::StorageContended)
        );
        // Main's existing close calls run only after this result is available.
        assert!(settled.load(Ordering::SeqCst));
        assert!(drained.load(Ordering::SeqCst));
        assert!(!dropped_early.load(Ordering::SeqCst));
    }

    #[tokio::test(start_paused = true)]
    async fn background_failure_awaits_scheduler_and_preserves_primary_error() {
        let cancel = CancellationToken::new();
        let scheduler_cancel = cancel.clone();
        let finished = Arc::new(AtomicBool::new(false));
        let dropped_early = Arc::new(AtomicBool::new(false));
        let completed = finished.clone();
        let dropped = dropped_early.clone();
        let (started_tx, started_rx) = oneshot::channel();
        let (draining_tx, draining_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let scheduler = async move {
            let _guard = DrainGuard {
                finished: completed.clone(),
                dropped_early: dropped,
            };
            started_tx.send(()).unwrap();
            scheduler_cancel.cancelled().await;
            draining_tx.send(()).unwrap();
            release_rx.await.unwrap();
            completed.store(true, Ordering::SeqCst);
            Err(DatabaseError::Cancelled.into())
        };
        let background = async {
            bounded(started_rx).await;
            Err(DatabaseError::StorageUnavailable.into())
        };
        let task = tokio::spawn(run_and_drain(scheduler, background, cancel));
        bounded(draining_rx).await;
        assert!(!task.is_finished());
        assert!(!dropped_early.load(Ordering::SeqCst));
        release_tx.send(()).unwrap();
        let error = tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert_eq!(
            error.downcast_ref::<DatabaseError>(),
            Some(&DatabaseError::StorageUnavailable)
        );
        assert!(finished.load(Ordering::SeqCst));
        assert!(!dropped_early.load(Ordering::SeqCst));
    }

    #[tokio::test(start_paused = true)]
    async fn secondary_failure_never_replaces_primary_in_either_direction() {
        for scheduler_first in [true, false] {
            let cancel = CancellationToken::new();
            let sibling_cancel = cancel.clone();
            let (started_tx, started_rx) = oneshot::channel();
            let primary = async {
                bounded(started_rx).await;
                Err(DatabaseError::StorageContended.into())
            };
            let secondary = async move {
                started_tx.send(()).unwrap();
                sibling_cancel.cancelled().await;
                Err(DatabaseError::StorageCorrupt.into())
            };
            let error = if scheduler_first {
                run_and_drain(primary, secondary, cancel).await
            } else {
                run_and_drain(secondary, primary, cancel).await
            }
            .unwrap_err();
            assert_eq!(
                error.downcast_ref::<DatabaseError>(),
                Some(&DatabaseError::StorageContended)
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn primary_success_only_suppresses_typed_secondary_cancellation() {
        for scheduler_first in [true, false] {
            for failure in [DatabaseError::Cancelled, DatabaseError::StorageUnavailable] {
                let cancel = CancellationToken::new();
                let sibling_cancel = cancel.clone();
                let (started_tx, started_rx) = oneshot::channel();
                let primary = async {
                    bounded(started_rx).await;
                    Ok(())
                };
                let secondary = async move {
                    started_tx.send(()).unwrap();
                    sibling_cancel.cancelled().await;
                    Err(failure.into())
                };
                let result = if scheduler_first {
                    run_and_drain(primary, secondary, cancel).await
                } else {
                    run_and_drain(secondary, primary, cancel).await
                };
                if failure == DatabaseError::Cancelled {
                    assert!(result.is_ok());
                } else {
                    assert_eq!(
                        result.unwrap_err().downcast_ref::<DatabaseError>(),
                        Some(&failure)
                    );
                }
            }
        }
        let cancel = CancellationToken::new();
        let sibling_cancel = cancel.clone();
        let (started_tx, started_rx) = oneshot::channel();
        let error = run_and_drain(
            async {
                bounded(started_rx).await;
                Ok(())
            },
            async move {
                started_tx.send(()).unwrap();
                sibling_cancel.cancelled().await;
                Err(std::io::Error::other("cancelled").into())
            },
            cancel,
        )
        .await
        .unwrap_err();
        assert!(error.downcast_ref::<std::io::Error>().is_some());
    }

    #[tokio::test(start_paused = true)]
    async fn a_ready_completion_skips_the_unstarted_sibling() {
        let constructed = Arc::new(AtomicUsize::new(0));
        let entered = Arc::new(AtomicUsize::new(0));
        let make = || {
            constructed.fetch_add(1, Ordering::SeqCst);
            let entered = entered.clone();
            async move {
                entered.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        };
        run_and_drain(make(), make(), CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(constructed.load(Ordering::SeqCst), 2);
        assert_eq!(entered.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn a_cold_sibling_never_starts_an_operation_after_either_completion() {
        #[derive(Debug)]
        struct Completed(usize);
        impl std::fmt::Display for Completed {
            fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("synthetic completed supervisor")
            }
        }
        impl std::error::Error for Completed {}

        let cancel = CancellationToken::new();
        let entered = Arc::new([AtomicUsize::new(0), AtomicUsize::new(0)]);
        let make = |id: usize| {
            let token = cancel.clone();
            let entered = entered.clone();
            async move {
                assert!(
                    !token.is_cancelled(),
                    "cold startup must precede cancellation"
                );
                entered[id].fetch_add(1, Ordering::SeqCst);
                Err(Box::new(Completed(id)) as ServerError)
            }
        };
        let error = run_and_drain(make(0), make(1), cancel.clone())
            .await
            .unwrap_err();
        let completed = error.downcast_ref::<Completed>().unwrap().0;
        assert!(cancel.is_cancelled());
        // Both branches are immediately ready: whichever completes first owns
        // the error, and the other original future must remain unpolled.
        assert_eq!(entered[completed].load(Ordering::SeqCst), 1);
        assert_eq!(entered[1 - completed].load(Ordering::SeqCst), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn both_started_futures_enter_and_complete_once() {
        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        let entered = Arc::new([AtomicUsize::new(0), AtomicUsize::new(0)]);
        let finished = Arc::new([AtomicUsize::new(0), AtomicUsize::new(0)]);
        let make = |id: usize| {
            let barrier = barrier.clone();
            let entered = entered.clone();
            let finished = finished.clone();
            async move {
                entered[id].fetch_add(1, Ordering::SeqCst);
                barrier.wait().await;
                finished[id].fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        };
        run_and_drain(make(0), make(1), CancellationToken::new())
            .await
            .unwrap();
        for id in 0..2 {
            assert_eq!(entered[id].load(Ordering::SeqCst), 1);
            assert_eq!(finished[id].load(Ordering::SeqCst), 1);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn initial_parent_cancellation_skips_both_cold_futures() {
        let parent = CancellationToken::new();
        let collector = parent.child_token();
        parent.cancel();
        let entered = Arc::new(AtomicUsize::new(0));
        let make = || {
            let entered = entered.clone();
            async move {
                entered.fetch_add(1, Ordering::SeqCst);
                Err(DatabaseError::StorageUnavailable.into())
            }
        };
        run_and_drain(make(), make(), collector).await.unwrap();
        assert_eq!(entered.load(Ordering::SeqCst), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn user_cancellation_still_waits_for_the_sibling_cleanup() {
        let parent = CancellationToken::new();
        let collector = parent.child_token();
        let scheduler_cancel = collector.clone();
        let background_cancel = collector.clone();
        let (scheduler_started_tx, scheduler_started_rx) = oneshot::channel();
        let (background_started_tx, background_started_rx) = oneshot::channel();
        let (draining_tx, draining_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let scheduler = async move {
            scheduler_started_tx.send(()).unwrap();
            scheduler_cancel.cancelled().await;
            Ok(())
        };
        let background = async move {
            background_started_tx.send(()).unwrap();
            background_cancel.cancelled().await;
            draining_tx.send(()).unwrap();
            release_rx.await.unwrap();
            Err(DatabaseError::Cancelled.into())
        };
        let task = tokio::spawn(run_and_drain(scheduler, background, collector));
        bounded(scheduler_started_rx).await;
        bounded(background_started_rx).await;
        parent.cancel();
        bounded(draining_rx).await;
        assert!(!task.is_finished());
        release_tx.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(parent.is_cancelled());
    }

    #[tokio::test(start_paused = true)]
    async fn a_primary_cancellation_is_not_reclassified_as_sibling_shutdown() {
        let cancel = CancellationToken::new();
        let sibling_cancel = cancel.clone();
        let error = run_and_drain(
            async { Err(DatabaseError::Cancelled.into()) },
            async move {
                sibling_cancel.cancelled().await;
                Ok(())
            },
            cancel,
        )
        .await
        .unwrap_err();
        assert_eq!(
            error.downcast_ref::<DatabaseError>(),
            Some(&DatabaseError::Cancelled)
        );
    }
}
