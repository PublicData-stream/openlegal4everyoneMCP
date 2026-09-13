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
    pub max_bytes: u64,
    pub max_snapshots_per_query: usize,
}
impl Default for RetentionPolicy {
    fn default() -> Self {
        Self {
            retention_days: 30,
            max_bytes: 1024 * 1024 * 1024,
            max_snapshots_per_query: 100,
        }
    }
}
impl RetentionPolicy {
    pub fn validate(&self) -> Result<(), RetrievalError> {
        if !(1..=3650).contains(&self.retention_days)
            || !(16 * 1024 * 1024..=64 * 1024 * 1024 * 1024).contains(&self.max_bytes)
            || !(1..=1000).contains(&self.max_snapshots_per_query)
        {
            return Err(RetrievalError::InvalidInput);
        }
        Ok(())
    }
    pub fn retains(&self, captured: u64, now: u64) -> bool {
        now.checked_sub(captured)
            .is_none_or(|age| age < self.retention_days * 86400)
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
}

pub struct StoredResult {
    pub payload: Arc<StoredPayload>,
    pub epoch: u64,
}

/// Called by the parent after staging, immediately before authorizing irreversible commit.
pub type CommitAuthorization = Arc<dyn Fn() -> bool + Send + Sync>;

/// Implementations own and drain work even when an awaiting caller is dropped.
/// An epoch change invalidates all L1 promotions from earlier disk operations.
pub trait PersistentStore: Send + Sync + 'static {
    fn lookup(
        &self,
        key: PersistentKey,
        now: u64,
        cancellation: CancellationToken,
    ) -> BoxFuture<'static, Result<Option<StoredResult>, RetrievalError>>;
    fn publish(
        &self,
        key: PersistentKey,
        value: Arc<StoredPayload>,
        now: u64,
        authorize: CommitAuthorization,
        cancellation: CancellationToken,
    ) -> BoxFuture<'static, Result<StoredResult, RetrievalError>>;
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
    fn close(&self) -> BoxFuture<'static, Result<(), RetrievalError>>;
    fn epoch(&self) -> u64;
    fn healthy(&self) -> bool;
    fn policy(&self) -> RetentionPolicy;
    fn metrics(&self) -> StorageMetrics;
}
