//! Fallible durable storage port. The application supplies identity, time and policy.
use crate::StoredPayload;
use futures::future::BoxFuture;
use openlegal_domain::{
    Query, RetrievalError,
    history::{SnapshotEnvelope, SnapshotPage},
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

mod identity;
pub use identity::{
    canonical_identity, digest_hex, identity_digest, immutable_payload_digest, processed_bytes,
    processed_digest, validate_payload,
};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(deny_unknown_fields)]
pub struct HistoryKey {
    pub namespace: String,
    pub provider: String,
    pub dataset: String,
    pub query: Query,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(deny_unknown_fields)]
pub struct PersistentKey {
    pub history: HistoryKey,
    pub processor_version: String,
    pub schema_version: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RetentionPolicy {
    pub retention_days: u64,
    pub max_blob_bytes: u64,
    pub max_snapshots_per_query: usize,
    pub max_snapshots: usize,
    pub max_queries: usize,
}
impl Default for RetentionPolicy {
    fn default() -> Self {
        Self {
            retention_days: 30,
            max_blob_bytes: 1024 * 1024 * 1024,
            max_snapshots_per_query: 100,
            max_snapshots: 10_000,
            max_queries: 4096,
        }
    }
}
impl RetentionPolicy {
    pub fn validate(&self) -> Result<(), RetrievalError> {
        if !(1..=3650).contains(&self.retention_days)
            || !(16 * 1024 * 1024..=64 * 1024 * 1024 * 1024).contains(&self.max_blob_bytes)
            || !(1..=1000).contains(&self.max_snapshots_per_query)
            || !(1..=1_000_000).contains(&self.max_snapshots)
            || !(1..=1_000_000).contains(&self.max_queries)
            || self.max_snapshots_per_query > self.max_snapshots
        {
            return Err(RetrievalError::InvalidInput);
        }
        Ok(())
    }
    pub fn retains(&self, captured: u64, now: u64) -> bool {
        now.checked_sub(captured)
            .is_none_or(|age| age < self.retention_days.saturating_mul(86400))
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct StorageMetrics {
    pub hits: u64,
    pub misses: u64,
    pub writes: u64,
    pub evictions: u64,
    pub corruptions: u64,
    pub recoveries: u64,
    pub saturation: u64,
    pub bytes: u64,
    pub snapshots: usize,
    pub queries: usize,
    pub unchanged_validations: u64,
    pub blob_reads: u64,
    pub blob_writes: u64,
    pub deduplicated_puts: u64,
    pub failures: u64,
    pub orphan_cleanups: u64,
    pub pool_connections: u64,
    pub pool_idle: u64,
    pub staging_bytes: u64,
    pub deletion_queue: u64,
}

pub struct StoredResult {
    pub payload: Arc<StoredPayload>,
    pub epoch: u64,
}

/// The query incarnation and all accepted mutations observed before upstream work.
/// An absent query has no identifier and revision zero. IDs are opaque to this layer.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ObservationToken {
    pub query_id: Option<String>,
    pub revision: u64,
}

pub struct LookupResult {
    pub value: Option<StoredResult>,
    pub observation: ObservationToken,
}

pub struct PublicationRequest {
    pub key: PersistentKey,
    pub value: Arc<StoredPayload>,
    pub expected: ObservationToken,
    pub now: u64,
    pub authorize: CommitAuthorization,
    pub cancellation: CancellationToken,
}

pub enum PublicationOutcome {
    Accepted(StoredResult),
    Conflict,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StorageStatus {
    Ready,
    Recovering,
    IntegrityBlocked,
    Maintaining,
    Closed,
}

impl StorageStatus {
    pub fn error(self) -> Option<RetrievalError> {
        match self {
            Self::Ready => None,
            Self::Maintaining => Some(RetrievalError::Busy),
            Self::IntegrityBlocked => Some(RetrievalError::StorageCorrupt),
            Self::Recovering | Self::Closed => Some(RetrievalError::StorageUnavailable),
        }
    }
}

/// Called after staging and again immediately before authorizing irreversible commit.
pub type CommitAuthorization = Arc<dyn Fn() -> bool + Send + Sync>;

/// Implementations own and drain work even when an awaiting caller is dropped.
/// An epoch change invalidates all L1 promotions from earlier storage operations.
/// Degraded status gates serving without stopping the service supervisor. Recovery
/// belongs to the adapter and never clears a known integrity fault on health alone.
pub trait PersistentStore: Send + Sync + 'static {
    fn lookup(
        &self,
        key: PersistentKey,
        now: u64,
        cancellation: CancellationToken,
    ) -> BoxFuture<'static, Result<LookupResult, RetrievalError>>;
    fn publish(
        &self,
        request: PublicationRequest,
    ) -> BoxFuture<'static, Result<PublicationOutcome, RetrievalError>>;
    fn list(
        &self,
        key: HistoryKey,
        cursor: Option<String>,
        limit: usize,
        now: u64,
        cancellation: CancellationToken,
    ) -> BoxFuture<'static, Result<SnapshotPage, RetrievalError>>;
    fn get(
        &self,
        key: HistoryKey,
        id: String,
        now: u64,
        cancellation: CancellationToken,
    ) -> BoxFuture<'static, Result<SnapshotEnvelope, RetrievalError>>;
    fn maintain(&self, now: u64) -> BoxFuture<'static, Result<(), RetrievalError>>;
    /// Bounded dependency probes and recovery reconciliation. Called by an owned
    /// service task independently of retention; never from a readiness handler.
    fn health(&self, _now: u64) -> BoxFuture<'static, Result<(), RetrievalError>> {
        Box::pin(async { Ok(()) })
    }
    fn close(&self) -> BoxFuture<'static, Result<(), RetrievalError>>;
    fn epoch(&self) -> u64;
    /// Changes on availability/integrity failures, but not ordinary retention.
    /// This fences upstream work that spans a recovery too brief for a caller or
    /// supervisor to observe the intermediate nonready status.
    fn recovery_epoch(&self) -> u64 {
        0
    }
    fn healthy(&self) -> bool;
    fn status(&self) -> StorageStatus {
        if self.healthy() {
            StorageStatus::Ready
        } else {
            StorageStatus::Recovering
        }
    }
    fn policy(&self) -> RetentionPolicy;
    fn metrics(&self) -> StorageMetrics;
}
