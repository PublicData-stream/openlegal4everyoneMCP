//! Shared bounded retrieval policy for all serving transports.
//!
//! One service is intended for one backend process. Replicas require a separate
//! deployment-wide coordination design. Adapters never add their own retry loops.

mod service;
pub mod text_diff;
pub use service::{CacheKey, CacheStore, MetricsSnapshot, RetrievalService, StoredPayload};

use futures::future::BoxFuture;
use openlegal_domain::{Query, RetrievalData, RetrievalError};
use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

pub const FRESH_SECONDS: u64 = 60;
pub const RETENTION_SECONDS: u64 = 300;
pub const MAX_CACHE_ENTRIES: usize = 256;
pub const MAX_CACHE_BYTES: usize = 32 * 1024 * 1024;
pub const MAX_RAW_BYTES: usize = 1024 * 1024;
pub const MAX_PROCESSED_BYTES: usize = 64 * 1024;

/// An adapter's validated candidate; the service checks identity and bounds again
/// before atomically retaining source bytes and publishing a normalized result.
pub struct FetchedPayload {
    pub raw: Vec<u8>,
    pub data: RetrievalData,
    pub source_reference: String,
}

pub trait Upstream: Send + Sync + 'static {
    /// Exactly one attempt, including bounded pure processing; no internal retries.
    fn fetch(
        &self,
        query: Query,
        cancellation: CancellationToken,
    ) -> BoxFuture<'static, Result<FetchedPayload, RetrievalError>>;
}

pub struct Source {
    pub id: String,
    pub provider: String,
    pub dataset: String,
    pub processor_version: String,
    pub upstream: Arc<dyn Upstream>,
}

/// Monotonic Unix-seconds clock: age decisions cannot become younger after wall
/// clock adjustment. Tests can provide a controlled implementation.
pub trait Clock: Send + Sync + 'static {
    fn now(&self) -> u64;
}

pub struct SystemClock {
    started: Instant,
    unix_seconds: u64,
}
impl Default for SystemClock {
    fn default() -> Self {
        Self {
            started: Instant::now(),
            unix_seconds: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        }
    }
}
impl Clock for SystemClock {
    fn now(&self) -> u64 {
        self.unix_seconds
            .saturating_add(self.started.elapsed().as_secs())
    }
}
