use super::*;
use crate::persistence::{
    LookupResult, ObservationToken, PublicationOutcome, PublicationRequest, RetentionPolicy,
    StorageMetrics, StorageStatus,
};
use crate::{FetchedPayload, Upstream};
use futures::future::BoxFuture;
use openlegal_domain::history::{SnapshotReference, SnapshotSummary};
use std::sync::atomic::AtomicUsize;

#[derive(Default)]
struct Cache(HashMap<CacheKey, Arc<StoredPayload>>);
impl CacheStore for Cache {
    fn get(&mut self, key: &CacheKey) -> Option<Arc<StoredPayload>> {
        self.0.get(key).cloned()
    }
    fn publish(&mut self, key: CacheKey, value: Arc<StoredPayload>) {
        self.0.insert(key, value);
    }
    fn expire_before(&mut self, before: u64) {
        self.0.retain(|_, v| v.provenance.validated_at >= before);
    }
    fn clear(&mut self) {
        self.0.clear();
    }
    fn stats(&self) -> (usize, usize) {
        (self.0.len(), self.0.values().map(|v| v.bytes).sum())
    }
}
struct ClockValue(AtomicU64);
impl Clock for ClockValue {
    fn now(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
}
struct UpstreamMock {
    calls: Arc<AtomicUsize>,
    unavailable: Arc<AtomicBool>,
    storage: Arc<StorageState>,
}
impl Upstream for UpstreamMock {
    fn fetch(
        &self,
        query: Query,
        _: CancellationToken,
    ) -> BoxFuture<'static, Result<FetchedPayload, RetrievalError>> {
        let calls = self.calls.clone();
        let unavailable = self.unavailable.clone();
        let storage = self.storage.clone();
        Box::pin(async move {
            calls.fetch_add(1, Ordering::SeqCst);
            if storage.mode.load(Ordering::SeqCst) == 13 {
                storage.upstream_started.notify_one();
                storage.upstream_release.notified().await;
            }
            if unavailable.load(Ordering::SeqCst) {
                return Err(RetrievalError::Unavailable);
            }
            let Query::Get { source, id } = query else {
                return Err(RetrievalError::InvalidInput);
            };
            Ok(FetchedPayload {
                raw: b"fixture".to_vec(),
                source_reference: "https://example.test/record".into(),
                data: RetrievalData::Get(Record {
                    source,
                    id,
                    title: "Fiction".into(),
                    body: "Synthetic text".into(),
                    synthetic: true,
                }),
            })
        })
    }
}
#[derive(Default)]
struct StorageState {
    values: Mutex<HashMap<PersistentKey, Arc<StoredPayload>>>,
    epoch: AtomicU64,
    recovery_epoch: AtomicU64,
    mode: AtomicUsize,
    lookups: AtomicUsize,
    writes: AtomicUsize,
    closed: AtomicBool,
    revision: AtomicU64,
    publication_started: tokio::sync::Notify,
    publication_release: tokio::sync::Notify,
    upstream_started: tokio::sync::Notify,
    upstream_release: tokio::sync::Notify,
    health_calls: AtomicU64,
    maintenance_calls: AtomicU64,
    list_clock: Mutex<Option<Arc<ClockValue>>>,
}
struct Storage(Arc<StorageState>);
impl PersistentStore for Storage {
    fn lookup(
        &self,
        key: PersistentKey,
        _: u64,
        cancellation: CancellationToken,
    ) -> BoxFuture<'static, Result<LookupResult, RetrievalError>> {
        let state = self.0.clone();
        Box::pin(async move {
            state.lookups.fetch_add(1, Ordering::SeqCst);
            tokio::task::yield_now().await;
            match state.mode.load(Ordering::SeqCst) {
                1 => return Err(RetrievalError::StorageUnavailable),
                4 => {
                    cancellation.cancelled().await;
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    return Err(RetrievalError::Cancelled);
                }
                _ => {}
            }
            let epoch = state.epoch.load(Ordering::SeqCst);
            if state.mode.load(Ordering::SeqCst) == 3 {
                state.epoch.fetch_add(1, Ordering::SeqCst);
            }
            let value = state
                .values
                .lock()
                .unwrap()
                .get(&key)
                .cloned()
                .map(|payload| StoredResult { payload, epoch });
            Ok(LookupResult {
                observation: ObservationToken {
                    query_id: value.as_ref().map(|_| "mock-query-uuid".into()),
                    revision: state.revision.load(Ordering::SeqCst),
                },
                value,
            })
        })
    }
    fn publish(
        &self,
        request: PublicationRequest,
    ) -> BoxFuture<'static, Result<PublicationOutcome, RetrievalError>> {
        let state = self.0.clone();
        Box::pin(async move {
            let PublicationRequest {
                key,
                value,
                expected,
                now,
                authorize,
                cancellation,
            } = request;
            let mode = state.mode.load(Ordering::SeqCst);
            if mode == 10 {
                state.publication_started.notify_one();
                state.publication_release.notified().await;
            }
            if mode == 7 {
                let mut values = state.values.lock().unwrap();
                let old = values.get(&key).unwrap();
                let mut provenance = old.provenance.clone();
                provenance.validated_at = now;
                values.insert(
                    key,
                    Arc::new(StoredPayload {
                        data: value.data.clone(),
                        provenance,
                        raw: value.raw.clone(),
                        bytes: value.bytes,
                        snapshot: value.snapshot.clone().or_else(|| {
                            Some(SnapshotReference {
                                snapshot_id: "b".repeat(64),
                                captured_at: now,
                            })
                        }),
                    }),
                );
                state.revision.fetch_add(1, Ordering::SeqCst);
                return Ok(PublicationOutcome::Conflict);
            }
            if matches!(mode, 8 | 9) || expected.revision != state.revision.load(Ordering::SeqCst) {
                return Ok(PublicationOutcome::Conflict);
            }
            if state.mode.load(Ordering::SeqCst) == 2 {
                return Err(RetrievalError::StorageCapacity);
            }
            if cancellation.is_cancelled() || !authorize() {
                return Err(RetrievalError::Cancelled);
            }
            state.writes.fetch_add(1, Ordering::SeqCst);
            let value = Arc::new(StoredPayload {
                data: value.data.clone(),
                provenance: value.provenance.clone(),
                raw: value.raw.clone(),
                bytes: value.bytes,
                snapshot: Some(SnapshotReference {
                    snapshot_id: "a".repeat(64),
                    captured_at: now,
                }),
            });
            state.values.lock().unwrap().insert(key, value.clone());
            state.revision.fetch_add(1, Ordering::SeqCst);
            Ok(PublicationOutcome::Accepted(StoredResult {
                payload: value,
                epoch: state.epoch.load(Ordering::SeqCst),
            }))
        })
    }
    fn list(
        &self,
        key: HistoryKey,
        _: Option<String>,
        _: usize,
        _: u64,
        _: CancellationToken,
    ) -> BoxFuture<'static, Result<SnapshotPage, RetrievalError>> {
        let state = self.0.clone();
        Box::pin(async move {
            let next_cursor = state.list_clock.lock().unwrap().as_ref().map(|clock| {
                clock.0.store(1000 + 30 * 86400, Ordering::SeqCst);
                "fixed-high-water-cursor".to_owned()
            });
            Ok(SnapshotPage {
                snapshots: state
                    .values
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|(k, _)| k.history == key)
                    .map(|(_, v)| summary(v))
                    .collect(),
                next_cursor,
                synthetic: true,
            })
        })
    }
    fn get(
        &self,
        key: HistoryKey,
        id: String,
        _: u64,
        _: CancellationToken,
    ) -> BoxFuture<'static, Result<SnapshotEnvelope, RetrievalError>> {
        let state = self.0.clone();
        Box::pin(async move {
            let values = state.values.lock().unwrap();
            let (_, value) = values
                .iter()
                .find(|(k, v)| k.history == key && v.snapshot.as_ref().unwrap().snapshot_id == id)
                .ok_or(RetrievalError::SnapshotUnavailable)?;
            Ok(SnapshotEnvelope {
                schema_version: 1,
                snapshot: summary(value),
                query: key.query,
                data: value.data.clone(),
                provenance: value.provenance.clone(),
                historical: true,
                synthetic: true,
                clock_anomaly: false,
            })
        })
    }
    fn maintain(&self, _: u64) -> BoxFuture<'static, Result<(), RetrievalError>> {
        self.0.maintenance_calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Ok(()) })
    }
    fn health(&self, _: u64) -> BoxFuture<'static, Result<(), RetrievalError>> {
        let state = self.0.clone();
        Box::pin(async move {
            state.health_calls.fetch_add(1, Ordering::SeqCst);
            if state.mode.load(Ordering::SeqCst) == 14 {
                state.epoch.fetch_add(1, Ordering::SeqCst);
                state.mode.store(0, Ordering::SeqCst);
            }
            Ok(())
        })
    }
    fn close(&self) -> BoxFuture<'static, Result<(), RetrievalError>> {
        self.0.closed.store(true, Ordering::SeqCst);
        Box::pin(async { Ok(()) })
    }
    fn epoch(&self) -> u64 {
        self.0.epoch.load(Ordering::SeqCst)
    }
    fn recovery_epoch(&self) -> u64 {
        self.0.recovery_epoch.load(Ordering::SeqCst)
    }
    fn healthy(&self) -> bool {
        !matches!(self.0.mode.load(Ordering::SeqCst), 6 | 11 | 12 | 14)
    }
    fn status(&self) -> StorageStatus {
        match self.0.mode.load(Ordering::SeqCst) {
            6 | 14 => StorageStatus::Recovering,
            11 => StorageStatus::IntegrityBlocked,
            12 => StorageStatus::Maintaining,
            _ => StorageStatus::Ready,
        }
    }
    fn policy(&self) -> RetentionPolicy {
        RetentionPolicy::default()
    }
    fn metrics(&self) -> StorageMetrics {
        StorageMetrics::default()
    }
}
fn summary(value: &StoredPayload) -> SnapshotSummary {
    let snapshot = value.snapshot.as_ref().unwrap();
    SnapshotSummary {
        snapshot_id: snapshot.snapshot_id.clone(),
        captured_at: snapshot.captured_at,
        sequence: 1,
        processor_version: value.provenance.processor_version.clone(),
        schema_version: 1,
        payload_sha256: value.provenance.payload_sha256.clone(),
    }
}
struct Fixture {
    service: Arc<RetrievalService>,
    storage: Arc<StorageState>,
    clock: Arc<ClockValue>,
    calls: Arc<AtomicUsize>,
    unavailable: Arc<AtomicBool>,
}
fn build(
    storage: Arc<StorageState>,
    clock: Arc<ClockValue>,
    calls: Arc<AtomicUsize>,
    unavailable: Arc<AtomicBool>,
) -> Arc<RetrievalService> {
    RetrievalService::with_persistence(
        vec![Source {
            id: "mock".into(),
            provider: "test".into(),
            dataset: "fiction".into(),
            processor_version: "1".into(),
            upstream: Arc::new(UpstreamMock {
                calls,
                unavailable,
                storage: storage.clone(),
            }),
        }],
        clock,
        Box::<Cache>::default(),
        Arc::new(Storage(storage)),
        "namespace".into(),
    )
    .unwrap()
}
fn fixture() -> Fixture {
    let storage = Arc::new(StorageState::default());
    let clock = Arc::new(ClockValue(AtomicU64::new(1000)));
    let calls = Arc::new(AtomicUsize::new(0));
    let unavailable = Arc::new(AtomicBool::new(false));
    let service = build(
        storage.clone(),
        clock.clone(),
        calls.clone(),
        unavailable.clone(),
    );
    Fixture {
        service,
        storage,
        clock,
        calls,
        unavailable,
    }
}
fn query() -> Query {
    Query::Get {
        source: "mock".into(),
        id: "001".into(),
    }
}
async fn get(
    service: &Arc<RetrievalService>,
) -> Result<RetrievalEnvelope<RetrievalData>, RetrievalError> {
    service
        .retrieve(
            query(),
            FreshnessRequirement::AllowStale,
            CancellationToken::new(),
            None,
        )
        .await
}

#[tokio::test]
async fn restart_reuses_persistence_without_upstream_and_history_never_fetches() {
    let f = fixture();
    let first = get(&f.service).await.unwrap();
    assert!(first.snapshot.is_some());
    f.service.shutdown().await.unwrap();
    let restarted = build(
        f.storage.clone(),
        f.clock.clone(),
        f.calls.clone(),
        f.unavailable.clone(),
    );
    let hit = get(&restarted).await.unwrap();
    assert_eq!(first, hit);
    assert_eq!(f.calls.load(Ordering::SeqCst), 1);
    let page = restarted
        .list_snapshots(query(), None, 10, CancellationToken::new())
        .await
        .unwrap();
    let snapshot = restarted
        .get_snapshot(
            query(),
            page.snapshots[0].snapshot_id.clone(),
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert!(snapshot.historical);
    assert_eq!(f.calls.load(Ordering::SeqCst), 1);
    restarted.shutdown().await.unwrap();
}

#[tokio::test]
async fn equivalent_l2_misses_coalesce_and_durable_write_precedes_l1() {
    let f = fixture();
    let (a, b) = tokio::join!(get(&f.service), get(&f.service));
    assert_eq!(a.unwrap(), b.unwrap());
    assert_eq!(f.storage.lookups.load(Ordering::SeqCst), 1);
    assert_eq!(f.storage.writes.load(Ordering::SeqCst), 1);
    assert_eq!(f.calls.load(Ordering::SeqCst), 1);
    assert_eq!(f.service.metrics().cache_entries, 1);
    f.service.shutdown().await.unwrap();
}

#[tokio::test]
async fn storage_error_is_not_a_miss_and_failed_commit_never_populates_l1() {
    for (mode, error, expected_calls) in [
        (1, RetrievalError::StorageUnavailable, 0),
        (2, RetrievalError::StorageCapacity, 1),
    ] {
        let f = fixture();
        f.storage.mode.store(mode, Ordering::SeqCst);
        assert_eq!(get(&f.service).await, Err(error));
        assert_eq!(f.calls.load(Ordering::SeqCst), expected_calls);
        assert_eq!(f.service.metrics().cache_entries, 0);
        assert_eq!(f.storage.writes.load(Ordering::SeqCst), 0);
        f.service.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn changed_epoch_invalidates_l1_and_rejects_an_obsolete_storage_result() {
    let f = fixture();
    get(&f.service).await.unwrap();
    f.storage.epoch.fetch_add(1, Ordering::SeqCst);
    f.storage.mode.store(3, Ordering::SeqCst);
    assert_eq!(
        get(&f.service).await,
        Err(RetrievalError::StorageUnavailable)
    );
    assert_eq!(f.service.metrics().cache_entries, 0);
    assert_eq!(f.calls.load(Ordering::SeqCst), 1);
    f.storage.mode.store(0, Ordering::SeqCst);
    get(&f.service).await.unwrap();
    assert_eq!(f.calls.load(Ordering::SeqCst), 1);
    f.service.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn storage_stale_fallback_preserves_fresh_only_and_storage_errors_are_not_stale() {
    let f = fixture();
    get(&f.service).await.unwrap();
    f.clock.0.store(1060, Ordering::SeqCst);
    f.unavailable.store(true, Ordering::SeqCst);
    assert_eq!(
        get(&f.service).await.unwrap().freshness.state,
        FreshnessState::Stale
    );
    assert_eq!(
        f.service
            .retrieve(
                query(),
                FreshnessRequirement::FreshOnly,
                CancellationToken::new(),
                None
            )
            .await,
        Err(RetrievalError::FreshnessUnavailable)
    );
    f.storage.mode.store(1, Ordering::SeqCst);
    assert_eq!(
        get(&f.service).await,
        Err(RetrievalError::StorageUnavailable)
    );
    f.service.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn cancelled_storage_flight_stays_registered_until_reconciliation() {
    let f = fixture();
    f.storage.mode.store(4, Ordering::SeqCst);
    let cancellation = CancellationToken::new();
    let token = cancellation.clone();
    let service = f.service.clone();
    let request = tokio::spawn(async move {
        service
            .retrieve(query(), FreshnessRequirement::AllowStale, token, None)
            .await
    });
    while f.storage.lookups.load(Ordering::SeqCst) == 0 {
        tokio::task::yield_now().await;
    }
    cancellation.cancel();
    assert_eq!(request.await.unwrap(), Err(RetrievalError::Cancelled));
    assert_eq!(f.service.metrics().in_flight, 1);
    assert_eq!(get(&f.service).await, Err(RetrievalError::Busy));
    tokio::time::advance(Duration::from_millis(101)).await;
    tokio::task::yield_now().await;
    assert_eq!(f.service.metrics().in_flight, 0);
    assert_eq!(f.calls.load(Ordering::SeqCst), 0);
    f.service.shutdown().await.unwrap();
}

#[tokio::test]
async fn future_timestamp_cannot_serve_fresh_but_explicit_history_is_flagged() {
    let f = fixture();
    let first = get(&f.service).await.unwrap();
    f.clock.0.store(999, Ordering::SeqCst);
    let historical = f
        .service
        .get_snapshot(
            query(),
            first.snapshot.unwrap().snapshot_id,
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert!(historical.clock_anomaly);
    get(&f.service).await.unwrap();
    assert_eq!(f.calls.load(Ordering::SeqCst), 2);
    f.service.shutdown().await.unwrap();
}

#[tokio::test]
async fn capture_retention_prevents_recently_validated_old_l1_head() {
    let f = fixture();
    get(&f.service).await.unwrap();
    let now = 1000 + 30 * 86400;
    f.clock.0.store(now, Ordering::SeqCst);
    {
        let mut values = f.storage.values.lock().unwrap();
        let value = values.values_mut().next().unwrap();
        let old = &**value;
        let mut provenance = old.provenance.clone();
        provenance.validated_at = now;
        *value = Arc::new(StoredPayload {
            data: old.data.clone(),
            raw: old.raw.clone(),
            bytes: old.bytes,
            snapshot: old.snapshot.clone(),
            provenance,
        });
    }
    f.storage.epoch.fetch_add(1, Ordering::SeqCst);
    let fresh = get(&f.service).await.unwrap();
    assert_eq!(fresh.snapshot.unwrap().captured_at, now);
    assert_eq!(f.calls.load(Ordering::SeqCst), 2);
    f.service.shutdown().await.unwrap();
}

#[tokio::test]
async fn fresh_l2_hit_does_not_require_an_upstream_permit() {
    let f = fixture();
    get(&f.service).await.unwrap();
    f.storage.epoch.fetch_add(1, Ordering::SeqCst);
    let semaphore = f.service.providers.get("test").unwrap().active.clone();
    let _first = semaphore.clone().try_acquire_owned().unwrap();
    let _second = semaphore.try_acquire_owned().unwrap();
    get(&f.service).await.unwrap();
    assert_eq!(f.calls.load(Ordering::SeqCst), 1);
    assert_eq!(f.storage.lookups.load(Ordering::SeqCst), 2);
    f.service.shutdown().await.unwrap();
}

#[tokio::test]
async fn unhealthy_store_rejects_l1_and_fails_service_readiness() {
    let f = fixture();
    get(&f.service).await.unwrap();
    f.storage.mode.store(6, Ordering::SeqCst);
    assert_eq!(
        get(&f.service).await,
        Err(RetrievalError::StorageUnavailable)
    );
    assert!(!f.service.shutdown.is_cancelled());
    assert!(!f.service.storage_ready());
    assert_eq!(f.calls.load(Ordering::SeqCst), 1);
    f.storage.epoch.fetch_add(1, Ordering::SeqCst);
    f.storage.mode.store(0, Ordering::SeqCst);
    assert!(f.service.storage_ready());
    get(&f.service).await.unwrap();
    assert_eq!(f.calls.load(Ordering::SeqCst), 1);
    f.service.shutdown().await.unwrap();
}

#[tokio::test]
async fn concurrent_publication_rereads_winner_once_without_replaying_or_refetching() {
    let f = fixture();
    get(&f.service).await.unwrap();
    f.clock.0.store(1060, Ordering::SeqCst);
    f.storage.mode.store(7, Ordering::SeqCst);
    let winner = get(&f.service).await.unwrap();
    assert_eq!(winner.provenance.validated_at, 1060);
    assert_eq!(winner.snapshot.unwrap().snapshot_id, "b".repeat(64));
    assert_eq!(f.storage.lookups.load(Ordering::SeqCst), 3);
    assert_eq!(f.storage.writes.load(Ordering::SeqCst), 1);
    assert_eq!(f.calls.load(Ordering::SeqCst), 2);
    f.service.shutdown().await.unwrap();
}

#[tokio::test]
async fn conflict_with_absent_or_stale_head_is_not_an_upstream_retry_or_stale_fallback() {
    let absent = fixture();
    absent.storage.mode.store(8, Ordering::SeqCst);
    assert_eq!(get(&absent.service).await, Err(RetrievalError::Busy));
    assert_eq!(absent.calls.load(Ordering::SeqCst), 1);
    assert_eq!(absent.storage.lookups.load(Ordering::SeqCst), 2);
    assert_eq!(absent.service.metrics().cache_entries, 0);
    absent.service.shutdown().await.unwrap();

    let stale = fixture();
    get(&stale.service).await.unwrap();
    stale.clock.0.store(1060, Ordering::SeqCst);
    stale.storage.mode.store(9, Ordering::SeqCst);
    assert_eq!(
        get(&stale.service).await,
        Err(RetrievalError::FreshnessUnavailable)
    );
    assert_eq!(stale.calls.load(Ordering::SeqCst), 2);
    stale.service.shutdown().await.unwrap();
}

#[tokio::test]
async fn cancellation_at_publication_fence_drains_without_l1_or_durable_success() {
    let f = fixture();
    f.storage.mode.store(10, Ordering::SeqCst);
    let cancellation = CancellationToken::new();
    let token = cancellation.clone();
    let service = f.service.clone();
    let request = tokio::spawn(async move {
        service
            .retrieve(query(), FreshnessRequirement::AllowStale, token, None)
            .await
    });
    f.storage.publication_started.notified().await;
    cancellation.cancel();
    assert_eq!(request.await.unwrap(), Err(RetrievalError::Cancelled));
    assert_eq!(get(&f.service).await, Err(RetrievalError::Busy));
    f.storage.publication_release.notify_one();
    f.service.shutdown().await.unwrap();
    assert_eq!(f.storage.writes.load(Ordering::SeqCst), 0);
    assert_eq!(f.service.metrics().cache_entries, 0);
}

#[tokio::test]
async fn integrity_and_maintenance_gate_history_and_l1_without_supervisor_shutdown() {
    let f = fixture();
    let first = get(&f.service).await.unwrap();
    for (mode, error) in [
        (11, RetrievalError::StorageCorrupt),
        (12, RetrievalError::Busy),
    ] {
        f.storage.mode.store(mode, Ordering::SeqCst);
        assert_eq!(get(&f.service).await, Err(error));
        assert_eq!(
            f.service
                .list_snapshots(query(), None, 10, CancellationToken::new())
                .await
                .unwrap_err(),
            error
        );
        assert_eq!(
            f.service
                .get_snapshot(
                    query(),
                    first.snapshot.as_ref().unwrap().snapshot_id.clone(),
                    CancellationToken::new()
                )
                .await
                .unwrap_err(),
            error
        );
        assert!(!f.service.storage_ready());
        assert!(!f.service.shutdown.is_cancelled());
    }
    assert_eq!(f.calls.load(Ordering::SeqCst), 1);
    f.storage.mode.store(0, Ordering::SeqCst);
    f.service.shutdown().await.unwrap();
}

#[tokio::test]
async fn recovered_storage_rejects_a_pre_outage_publication_generation() {
    let f = fixture();
    f.storage.mode.store(10, Ordering::SeqCst);
    let service = f.service.clone();
    let request = tokio::spawn(async move { get(&service).await });
    f.storage.publication_started.notified().await;
    f.storage.epoch.fetch_add(1, Ordering::SeqCst);
    f.storage.mode.store(6, Ordering::SeqCst);
    assert_eq!(
        get(&f.service).await,
        Err(RetrievalError::StorageUnavailable)
    );
    assert!(!f.service.shutdown.is_cancelled());
    f.storage.mode.store(0, Ordering::SeqCst);
    f.storage.epoch.fetch_add(1, Ordering::SeqCst);
    assert_eq!(get(&f.service).await, Err(RetrievalError::Busy));
    f.storage.publication_release.notify_one();
    assert_eq!(request.await.unwrap(), Err(RetrievalError::Cancelled));
    assert_eq!(f.storage.writes.load(Ordering::SeqCst), 0);
    assert_eq!(f.service.metrics().cache_entries, 0);
    get(&f.service).await.unwrap();
    assert_eq!(f.storage.writes.load(Ordering::SeqCst), 1);
    assert_eq!(f.calls.load(Ordering::SeqCst), 2);
    f.service.shutdown().await.unwrap();
}

#[tokio::test]
async fn fast_recovery_during_upstream_work_is_fenced_without_observing_nonready_status() {
    let f = fixture();
    f.storage.mode.store(13, Ordering::SeqCst);
    let service = f.service.clone();
    let request = tokio::spawn(async move { get(&service).await });
    f.storage.upstream_started.notified().await;
    // Recovery completed while this flight was fetching; no request or periodic
    // monitor saw the intermediate outage. Ordinary retention has a separate epoch.
    f.storage.recovery_epoch.fetch_add(1, Ordering::SeqCst);
    f.storage.epoch.fetch_add(2, Ordering::SeqCst);
    f.storage.mode.store(0, Ordering::SeqCst);
    f.storage.upstream_release.notify_one();
    assert_eq!(
        request.await.unwrap(),
        Err(RetrievalError::StorageUnavailable)
    );
    assert_eq!(f.storage.writes.load(Ordering::SeqCst), 0);
    assert_eq!(f.service.metrics().cache_entries, 0);
    assert!(f.service.storage_ready());
    get(&f.service).await.unwrap();
    assert_eq!(f.storage.writes.load(Ordering::SeqCst), 1);
    assert_eq!(f.calls.load(Ordering::SeqCst), 2);
    f.service.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn owned_health_probe_recovers_before_retention_maintenance_is_due() {
    let f = fixture();
    get(&f.service).await.unwrap();
    f.storage.mode.store(14, Ordering::SeqCst);
    f.storage.recovery_epoch.fetch_add(1, Ordering::SeqCst);
    f.storage.epoch.fetch_add(1, Ordering::SeqCst);
    assert_eq!(
        get(&f.service).await,
        Err(RetrievalError::StorageUnavailable)
    );
    tokio::time::advance(Duration::from_secs(5)).await;
    tokio::task::yield_now().await;
    assert!(f.service.storage_ready());
    assert_eq!(f.storage.health_calls.load(Ordering::SeqCst), 1);
    assert_eq!(f.storage.maintenance_calls.load(Ordering::SeqCst), 0);
    get(&f.service).await.unwrap();
    assert_eq!(f.calls.load(Ordering::SeqCst), 1);
    f.service.shutdown().await.unwrap();
}

#[tokio::test]
async fn history_expiring_while_awaited_returns_busy_without_malformed_continuation() {
    let f = fixture();
    get(&f.service).await.unwrap();
    *f.storage.list_clock.lock().unwrap() = Some(f.clock.clone());
    assert_eq!(
        f.service
            .list_snapshots(query(), None, 10, CancellationToken::new())
            .await
            .unwrap_err(),
        RetrievalError::Busy
    );
    assert_eq!(f.calls.load(Ordering::SeqCst), 1);
    f.service.shutdown().await.unwrap();
}
