//! Shared bounded retrieval policy for all serving transports.
//!
//! One service is intended for one backend process. Replicas require a separate
//! deployment-wide coordination design. Adapters never add their own retry loops.

pub mod blob;
pub mod citation;
pub mod database;
pub mod demand_collection;
pub mod document;
pub mod durability;
pub mod legal_reference;
pub mod persistence;
mod service;
pub mod text_diff;
pub mod upstream_policy;
pub use service::{CacheKey, CacheStore, MetricsSnapshot, RetrievalService, StoredPayload};

use futures::future::BoxFuture;
use openlegal_domain::{Query, RetrievalData, RetrievalError};
use std::{
    sync::{Arc, LazyLock},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
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
    anchor: &'static ClockAnchor,
}

struct ClockAnchor {
    started: Instant,
    unix_time: Duration,
}

// Publication and later reads must share one timeline even when their clocks
// are constructed on opposite sides of a second boundary or a wall-clock change.
static SYSTEM_CLOCK_ANCHOR: LazyLock<ClockAnchor> = LazyLock::new(|| ClockAnchor {
    started: Instant::now(),
    unix_time: SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default(),
});

impl ClockAnchor {
    fn unix_seconds_after(&self, elapsed: Duration) -> u64 {
        self.unix_time.saturating_add(elapsed).as_secs()
    }
}

impl Default for SystemClock {
    fn default() -> Self {
        Self {
            anchor: &SYSTEM_CLOCK_ANCHOR,
        }
    }
}
impl Clock for SystemClock {
    fn now(&self) -> u64 {
        self.anchor
            .unix_seconds_after(self.anchor.started.elapsed())
    }
}

#[cfg(test)]
mod clock_tests {
    use super::*;

    #[test]
    fn fractional_epoch_and_elapsed_cross_the_same_second_boundary() {
        let anchor = ClockAnchor {
            started: Instant::now(),
            unix_time: Duration::from_millis(100_900),
        };
        assert_eq!(anchor.unix_seconds_after(Duration::from_millis(99)), 100);
        assert_eq!(anchor.unix_seconds_after(Duration::from_millis(100)), 101);
        assert_eq!(anchor.unix_seconds_after(Duration::from_millis(200)), 101);
        assert_eq!(anchor.unix_seconds_after(Duration::MAX), u64::MAX);
    }

    #[test]
    fn independently_constructed_clocks_share_one_nondecreasing_timeline() {
        let publication = SystemClock::default();
        let published_at = publication.now();
        let reader = SystemClock::default();
        assert!(std::ptr::eq(publication.anchor, reader.anchor));
        let served_at = reader.now();
        assert!(served_at >= published_at);
        assert!(publication.now() >= served_at);
    }

    #[tokio::test(start_paused = true)]
    async fn tokio_virtual_time_does_not_advance_the_system_epoch() {
        let clock = SystemClock::default();
        let started = Instant::now();
        let before = clock.now();
        tokio::time::advance(Duration::from_secs(86400)).await;
        let after = clock.now();
        let later_clock = SystemClock::default();
        let elapsed = started.elapsed();
        assert!(after >= before);
        assert!(after - before <= elapsed.as_secs().saturating_add(1));
        assert!(later_clock.now() >= after);
    }
}

pub mod search;

pub mod database_read;
