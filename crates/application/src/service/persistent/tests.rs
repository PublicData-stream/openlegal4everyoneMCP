use super::*;
use crate::persistence::{CommitAuthorization, RetentionPolicy, StorageMetrics};
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
}
impl Upstream for UpstreamMock {
    fn fetch(
        &self,
        query: Query,
        _: CancellationToken,
    ) -> BoxFuture<'static, Result<FetchedPayload, RetrievalError>> {
        let calls = self.calls.clone();
        let unavailable = self.unavailable.clone();
        Box::pin(async move {
            calls.fetch_add(1, Ordering::SeqCst);
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
struct DiskState {
    values: Mutex<HashMap<PersistentKey, Arc<StoredPayload>>>,
    epoch: AtomicU64,
    mode: AtomicUsize,
    lookups: AtomicUsize,
    writes: AtomicUsize,
    closed: AtomicBool,
}
struct Disk(Arc<DiskState>);
impl PersistentStore for Disk {
    fn lookup(
        &self,
        key: PersistentKey,
        _: u64,
        cancellation: CancellationToken,
    ) -> BoxFuture<'static, Result<Option<StoredResult>, RetrievalError>> {
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
            Ok(state
                .values
                .lock()
                .unwrap()
                .get(&key)
                .cloned()
                .map(|payload| StoredResult { payload, epoch }))
        })
    }
    fn publish(
        &self,
        key: PersistentKey,
        value: Arc<StoredPayload>,
        now: u64,
        authorize: CommitAuthorization,
        cancellation: CancellationToken,
    ) -> BoxFuture<'static, Result<StoredResult, RetrievalError>> {
        let state = self.0.clone();
        Box::pin(async move {
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
            Ok(StoredResult {
                payload: value,
                epoch: state.epoch.load(Ordering::SeqCst),
            })
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
            Ok(SnapshotPage {
                snapshots: state
                    .values
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|(k, _)| k.history == key)
                    .map(|(_, v)| summary(v))
                    .collect(),
                next_cursor: None,
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
        Box::pin(async { Ok(()) })
    }
    fn close(&self) -> BoxFuture<'static, Result<(), RetrievalError>> {
        self.0.closed.store(true, Ordering::SeqCst);
        Box::pin(async { Ok(()) })
    }
    fn epoch(&self) -> u64 {
        self.0.epoch.load(Ordering::SeqCst)
    }
    fn healthy(&self) -> bool {
        self.0.mode.load(Ordering::SeqCst) != 6
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
    disk: Arc<DiskState>,
    clock: Arc<ClockValue>,
    calls: Arc<AtomicUsize>,
    unavailable: Arc<AtomicBool>,
}
fn build(
    disk: Arc<DiskState>,
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
            upstream: Arc::new(UpstreamMock { calls, unavailable }),
        }],
        clock,
        Box::<Cache>::default(),
        Arc::new(Disk(disk)),
        "namespace".into(),
    )
    .unwrap()
}
fn fixture() -> Fixture {
    let disk = Arc::new(DiskState::default());
    let clock = Arc::new(ClockValue(AtomicU64::new(1000)));
    let calls = Arc::new(AtomicUsize::new(0));
    let unavailable = Arc::new(AtomicBool::new(false));
    let service = build(
        disk.clone(),
        clock.clone(),
        calls.clone(),
        unavailable.clone(),
    );
    Fixture {
        service,
        disk,
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
async fn restart_reuses_disk_without_upstream_and_history_never_fetches() {
    let f = fixture();
    let first = get(&f.service).await.unwrap();
    assert!(first.snapshot.is_some());
    f.service.shutdown().await.unwrap();
    let restarted = build(
        f.disk.clone(),
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
    assert_eq!(f.disk.lookups.load(Ordering::SeqCst), 1);
    assert_eq!(f.disk.writes.load(Ordering::SeqCst), 1);
    assert_eq!(f.calls.load(Ordering::SeqCst), 1);
    assert_eq!(f.service.metrics().cache_entries, 1);
    f.service.shutdown().await.unwrap();
}

#[tokio::test]
async fn disk_error_is_not_a_miss_and_failed_commit_never_populates_l1() {
    for (mode, error, expected_calls) in [
        (1, RetrievalError::StorageUnavailable, 0),
        (2, RetrievalError::StorageCapacity, 1),
    ] {
        let f = fixture();
        f.disk.mode.store(mode, Ordering::SeqCst);
        assert_eq!(get(&f.service).await, Err(error));
        assert_eq!(f.calls.load(Ordering::SeqCst), expected_calls);
        assert_eq!(f.service.metrics().cache_entries, 0);
        assert_eq!(f.disk.writes.load(Ordering::SeqCst), 0);
        f.service.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn changed_epoch_invalidates_l1_and_rejects_an_obsolete_disk_result() {
    let f = fixture();
    get(&f.service).await.unwrap();
    f.disk.epoch.fetch_add(1, Ordering::SeqCst);
    f.disk.mode.store(3, Ordering::SeqCst);
    assert_eq!(
        get(&f.service).await,
        Err(RetrievalError::StorageUnavailable)
    );
    assert_eq!(f.service.metrics().cache_entries, 0);
    assert_eq!(f.calls.load(Ordering::SeqCst), 1);
    f.disk.mode.store(0, Ordering::SeqCst);
    get(&f.service).await.unwrap();
    assert_eq!(f.calls.load(Ordering::SeqCst), 1);
    f.service.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn disk_stale_fallback_preserves_fresh_only_and_storage_errors_are_not_stale() {
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
    f.disk.mode.store(1, Ordering::SeqCst);
    assert_eq!(
        get(&f.service).await,
        Err(RetrievalError::StorageUnavailable)
    );
    f.service.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn cancelled_disk_flight_stays_registered_until_reconciliation() {
    let f = fixture();
    f.disk.mode.store(4, Ordering::SeqCst);
    let cancellation = CancellationToken::new();
    let token = cancellation.clone();
    let service = f.service.clone();
    let request = tokio::spawn(async move {
        service
            .retrieve(query(), FreshnessRequirement::AllowStale, token, None)
            .await
    });
    while f.disk.lookups.load(Ordering::SeqCst) == 0 {
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
        let mut values = f.disk.values.lock().unwrap();
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
    f.disk.epoch.fetch_add(1, Ordering::SeqCst);
    let fresh = get(&f.service).await.unwrap();
    assert_eq!(fresh.snapshot.unwrap().captured_at, now);
    assert_eq!(f.calls.load(Ordering::SeqCst), 2);
    f.service.shutdown().await.unwrap();
}

#[tokio::test]
async fn fresh_l2_hit_does_not_require_an_upstream_permit() {
    let f = fixture();
    get(&f.service).await.unwrap();
    f.disk.epoch.fetch_add(1, Ordering::SeqCst);
    let semaphore = f.service.providers.get("test").unwrap().active.clone();
    let _first = semaphore.clone().try_acquire_owned().unwrap();
    let _second = semaphore.try_acquire_owned().unwrap();
    get(&f.service).await.unwrap();
    assert_eq!(f.calls.load(Ordering::SeqCst), 1);
    assert_eq!(f.disk.lookups.load(Ordering::SeqCst), 2);
    f.service.shutdown().await.unwrap();
}

#[tokio::test]
async fn unhealthy_store_rejects_l1_and_fails_service_readiness() {
    let f = fixture();
    get(&f.service).await.unwrap();
    f.disk.mode.store(6, Ordering::SeqCst);
    assert_eq!(
        get(&f.service).await,
        Err(RetrievalError::StorageUnavailable)
    );
    assert!(f.service.shutdown.is_cancelled());
    assert_eq!(f.calls.load(Ordering::SeqCst), 1);
    assert_eq!(f.service.shutdown().await, Err(RetrievalError::Internal));
}
