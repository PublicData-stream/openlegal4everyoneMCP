//! Optional publication and response barrier for a configured durable replica.
use futures::future::BoxFuture;
use openlegal_domain::RetrievalError;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

/// Check a fresh WAL prefix after the operation being protected. Implementations
/// must bind the primary/replica identity and recovery epoch for the whole check;
/// a readiness bit, successful COMMIT tag or cached receipt is not a barrier.
pub trait DurabilityFence: Send + Sync + 'static {
    /// Irreversible uncertainty revokes production admission across processes.
    /// Memory-only fixtures and legacy adapters have no external authority.
    fn mark_uncertain(&self) {}
    fn check(
        &self,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> BoxFuture<'static, Result<(), RetrievalError>>;
}
