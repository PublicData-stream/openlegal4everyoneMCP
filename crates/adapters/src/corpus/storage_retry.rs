//! Retry server-rejected corpus SQL phases without replaying external effects.
use super::{DatabaseError, check};
use std::{future::Future, time::Duration};
use tokio::time::{Instant, sleep_until, timeout_at};
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

const DELAYS: [Duration; 3] = [
    Duration::from_millis(100),
    Duration::from_millis(200),
    Duration::from_millis(400),
];
const DEADLINE: Duration = Duration::from_secs(10);

/// Call only for a complete SQL transaction or an independently replayable SQL
/// statement. `StorageContended` means an explicit server rejection; transport
/// errors and a locally interrupted in-flight SQL operation have unknown outcome.
/// Cancellation during a backoff is immediate. In-flight transactions retain
/// their own pre-commit cancellation checks rather than being dropped on cancel.
pub(super) async fn retry_storage<T, F, Fut>(
    cancel: &CancellationToken,
    operation: &'static str,
    mut work: F,
) -> Result<T, DatabaseError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, DatabaseError>>,
{
    let deadline = Instant::now() + DEADLINE;
    for (attempt, delay) in DELAYS
        .into_iter()
        .map(Some)
        .chain(std::iter::once(None))
        .enumerate()
    {
        check(cancel)?;
        let span = tracing::info_span!("corpus_storage_phase", operation, attempt = attempt + 1);
        let result = match timeout_at(deadline, work().instrument(span)).await {
            Ok(result) => result,
            // Do not label a dropped COMMIT as a proven rollback or retry it.
            Err(_) => return Err(DatabaseError::StorageUnavailable),
        };
        match (result, delay) {
            (Err(DatabaseError::StorageContended), Some(delay)) => {
                tracing::warn!(
                    operation,
                    attempt = attempt + 1,
                    error_category = "StorageContended",
                    "retrying server-rejected corpus SQL phase"
                );
                let wake = Instant::now() + delay;
                if wake >= deadline {
                    return Err(DatabaseError::StorageContended);
                }
                tokio::select! {
                    _ = cancel.cancelled() => return Err(DatabaseError::Cancelled),
                    _ = sleep_until(wake) => {}
                }
            }
            (result, _) => return result,
        }
    }
    Err(DatabaseError::StorageContended)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test]
    async fn retries_only_explicit_rejections_and_stops_after_four_attempts() {
        let calls = AtomicUsize::new(0);
        let result: Result<(), _> = retry_storage(&CancellationToken::new(), "fixture", || async {
            calls.fetch_add(1, Ordering::SeqCst);
            Err(DatabaseError::StorageContended)
        })
        .await;
        assert_eq!(result, Err(DatabaseError::StorageContended));
        assert_eq!(calls.load(Ordering::SeqCst), 4);
        for error in [
            DatabaseError::StorageUnavailable,
            DatabaseError::StorageCorrupt,
        ] {
            calls.store(0, Ordering::SeqCst);
            let result: Result<(), _> =
                retry_storage(&CancellationToken::new(), "fixture", || async {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Err(error)
                })
                .await;
            assert_eq!(result, Err(error));
            assert_eq!(calls.load(Ordering::SeqCst), 1);
        }
    }

    #[tokio::test]
    async fn cancellation_during_backoff_prevents_another_transaction() {
        let cancel = CancellationToken::new();
        let calls = AtomicUsize::new(0);
        let result: Result<(), _> = retry_storage(&cancel, "fixture", || async {
            calls.fetch_add(1, Ordering::SeqCst);
            cancel.cancel();
            Err(DatabaseError::StorageContended)
        })
        .await;
        assert_eq!(result, Err(DatabaseError::Cancelled));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}
