//! Real PostgreSQL 18 integration evidence; fixtures contain fictional source data.
#[path = "../../../test-support/postgres.rs"]
mod support;

use futures::future::BoxFuture;
use openlegal_adapters::{
    MemoryCache,
    blob::FsBlobStore,
    postgres::{PostgresStore, StartupError, StartupMode},
};
use openlegal_application::{
    Clock, FetchedPayload, RetrievalService, Source, StoredPayload, Upstream,
    persistence::{
        HistoryKey, ObservationToken, PersistentKey, PersistentStore, PublicationOutcome,
        PublicationRequest, RetentionPolicy, StorageStatus, canonical_identity, digest_hex,
        identity_digest,
    },
};
use openlegal_domain::{
    FreshnessRequirement, Provenance, Query, Record, RetrievalData, RetrievalError as Error,
};
use sqlx::{PgPool, Row, postgres::PgPoolOptions};
use std::sync::{
    Arc,
    atomic::{AtomicU64, AtomicUsize, Ordering},
};
use support::{TestDatabase, options};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

fn token() -> CancellationToken {
    CancellationToken::new()
}

fn key(id: &str) -> PersistentKey {
    PersistentKey {
        history: HistoryKey {
            namespace: "postgres_fixture".into(),
            provider: "synthetic".into(),
            dataset: "records".into(),
            query: Query::Get {
                source: "fixture".into(),
                id: id.into(),
            },
        },
        processor_version: "v1".into(),
        schema_version: 1,
    }
}

fn candidate(key: &PersistentKey, body: &str, raw: &[u8], now: u64) -> Arc<StoredPayload> {
    let Query::Get { source, id } = &key.history.query else {
        panic!("record fixture")
    };
    Arc::new(StoredPayload {
        data: RetrievalData::Get(Record {
            source: source.clone(),
            id: id.clone(),
            title: "Fictional record".into(),
            body: body.into(),
            synthetic: true,
        }),
        provenance: Provenance {
            provider: key.history.provider.clone(),
            dataset: key.history.dataset.clone(),
            source_reference: "https://example.test/fictional-source".into(),
            payload_sha256: digest_hex(raw),
            processor_version: key.processor_version.clone(),
            retrieved_at: now,
            validated_at: now,
        },
        raw: raw.to_vec(),
        bytes: raw.len() + body.len() + 2048,
        snapshot: None,
    })
}

fn request(
    key: PersistentKey,
    value: Arc<StoredPayload>,
    expected: ObservationToken,
    now: u64,
) -> PublicationRequest {
    PublicationRequest {
        key,
        value,
        expected,
        now,
        authorize: Arc::new(|| true),
        cancellation: token(),
    }
}

async fn publish(
    store: &PostgresStore,
    key: &PersistentKey,
    body: &str,
    raw: &[u8],
    now: u64,
) -> Arc<StoredPayload> {
    let expected = store
        .lookup(key.clone(), now, token())
        .await
        .unwrap()
        .observation;
    match store
        .publish(request(
            key.clone(),
            candidate(key, body, raw, now),
            expected,
            now,
        ))
        .await
        .unwrap()
    {
        PublicationOutcome::Accepted(result) => result.payload,
        PublicationOutcome::Conflict => panic!("sequential publication unexpectedly conflicted"),
    }
}

async fn sql(fixture: &TestDatabase) -> PgPool {
    PgPoolOptions::new()
        .max_connections(2)
        .connect(&fixture.url)
        .await
        .expect("isolated test SQL connection")
}

async fn open_policy(
    fixture: &TestDatabase,
    policy: RetentionPolicy,
    now: u64,
    mode: StartupMode,
) -> Result<Arc<PostgresStore>, Error> {
    let blobs = FsBlobStore::open(&fixture.directory.path().join("blobs")).await?;
    PostgresStore::open(&fixture.url, options(), blobs, policy, now, mode)
        .await
        .map_err(|error| match error {
            StartupError::Storage(error) => error,
            _ => Error::StorageCorrupt,
        })
}

struct TestClock(AtomicU64);
impl Clock for TestClock {
    fn now(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
}
struct MockUpstream(Arc<AtomicUsize>);
impl Upstream for MockUpstream {
    fn fetch(
        &self,
        query: Query,
        _: CancellationToken,
    ) -> BoxFuture<'static, Result<FetchedPayload, Error>> {
        let calls = self.0.clone();
        Box::pin(async move {
            calls.fetch_add(1, Ordering::SeqCst);
            let Query::Get { source, id } = query else {
                return Err(Error::InvalidInput);
            };
            Ok(FetchedPayload {
                raw: b"fictional upstream bytes".to_vec(),
                source_reference: "https://example.test/fictional-source".into(),
                data: RetrievalData::Get(Record {
                    source,
                    id,
                    title: "Fictional record".into(),
                    body: "Synthetic text".into(),
                    synthetic: true,
                }),
            })
        })
    }
}

fn service(
    store: Arc<PostgresStore>,
    clock: Arc<TestClock>,
    calls: Arc<AtomicUsize>,
) -> Arc<RetrievalService> {
    RetrievalService::with_persistence(
        vec![Source {
            id: "fixture".into(),
            provider: "synthetic".into(),
            dataset: "records".into(),
            processor_version: "v1".into(),
            upstream: Arc::new(MockUpstream(calls)),
        }],
        clock,
        Box::new(MemoryCache::new()),
        store,
        "postgres_fixture".into(),
    )
    .unwrap()
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh PostgreSQL 18"]
async fn cold_retrieval_commits_evidence_l1_and_restart_reuses_persistence() {
    let fixture = TestDatabase::new().await;
    let database = sql(&fixture).await;
    let calls = Arc::new(AtomicUsize::new(0));
    let clock = Arc::new(TestClock(AtomicU64::new(1000)));
    let app = service(fixture.open(1000).await, clock.clone(), calls.clone());
    let query = key("001").history.query;
    let first = app
        .retrieve(
            query.clone(),
            FreshnessRequirement::AllowStale,
            token(),
            None,
        )
        .await
        .unwrap();
    let second = app
        .retrieve(
            query.clone(),
            FreshnessRequirement::FreshOnly,
            token(),
            None,
        )
        .await
        .unwrap();
    assert_eq!(first, second);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(app.metrics().cache_entries, 1);
    let row = sqlx::query("SELECT b.storage_key, b.size_bytes FROM openlegal.cache_snapshot s JOIN openlegal.blob_object b ON b.sha256=s.raw_blob_sha256 WHERE s.public_id=$1")
        .bind(&first.snapshot.as_ref().unwrap().snapshot_id).fetch_one(&database).await.unwrap();
    let bytes = std::fs::read(
        fixture
            .directory
            .path()
            .join("blobs")
            .join(row.get::<String, _>("storage_key")),
    )
    .unwrap();
    assert_eq!(bytes, b"fictional upstream bytes");
    assert_eq!(bytes.len() as i64, row.get::<i64, _>("size_bytes"));
    app.shutdown().await.unwrap();
    let restarted = service(fixture.open(1000).await, clock, calls.clone());
    assert_eq!(
        restarted
            .retrieve(query, FreshnessRequirement::FreshOnly, token(), None)
            .await
            .unwrap(),
        first
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(restarted.metrics().cache_entries, 1);
    restarted.shutdown().await.unwrap();
    database.close().await;
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh PostgreSQL 18"]
async fn immutable_occurrences_keep_original_provenance_and_deduplicate_only_current_head() {
    let fixture = TestDatabase::new().await;
    let store = fixture.open(100).await;
    let k = key("001");
    let a = publish(&store, &k, "A", b"A", 100).await;
    let unchanged = publish(&store, &k, "A", b"A", 110).await;
    assert_eq!(a.snapshot, unchanged.snapshot);
    assert_eq!(unchanged.provenance.retrieved_at, 100);
    assert_eq!(unchanged.provenance.validated_at, 110);
    let b = publish(&store, &k, "B", b"B", 120).await;
    let a2 = publish(&store, &k, "A", b"A", 130).await;
    assert_ne!(a.snapshot, a2.snapshot);
    assert_ne!(b.snapshot, a2.snapshot);
    let page = store
        .list(k.history.clone(), None, 20, 130, token())
        .await
        .unwrap();
    assert_eq!(
        page.snapshots
            .iter()
            .map(|s| s.sequence)
            .collect::<Vec<_>>(),
        [3, 2, 1]
    );
    let original = store
        .get(
            k.history.clone(),
            a.snapshot.as_ref().unwrap().snapshot_id.clone(),
            130,
            token(),
        )
        .await
        .unwrap();
    assert_eq!(original.provenance.retrieved_at, 100);
    assert_eq!(original.provenance.validated_at, 100);
    assert!(original.historical && original.synthetic);
    let database = sql(&fixture).await;
    let blobs: i64 = sqlx::query_scalar("SELECT count(*) FROM openlegal.blob_object WHERE ready")
        .fetch_one(&database)
        .await
        .unwrap();
    assert_eq!(blobs, 2);
    let totals: (i64, i64) = sqlx::query_as(
        "SELECT snapshots,referenced_bytes FROM openlegal.cache_storage WHERE singleton",
    )
    .fetch_one(&database)
    .await
    .unwrap();
    assert_eq!(totals, (3, 2));
    store.close().await.unwrap();
    database.close().await;
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh PostgreSQL 18"]
async fn matching_source_bytes_do_not_merge_queries_or_processor_heads() {
    let fixture = TestDatabase::new().await;
    let store = fixture.open(100).await;
    let a = key("001");
    let b = key("1");
    let first = publish(&store, &a, "same", b"same", 100).await;
    let second = publish(&store, &b, "same", b"same", 101).await;
    assert_ne!(first.snapshot, second.snapshot);
    assert_eq!(
        store
            .get(
                b.history.clone(),
                first.snapshot.as_ref().unwrap().snapshot_id.clone(),
                101,
                token()
            )
            .await
            .unwrap_err(),
        Error::SnapshotUnavailable
    );
    let mut newer = a.clone();
    newer.processor_version = "v2".into();
    let processed = publish(
        &store,
        &newer,
        "different processed interpretation",
        b"same",
        102,
    )
    .await;
    assert_eq!(
        store
            .lookup(a.clone(), 103, token())
            .await
            .unwrap()
            .value
            .unwrap()
            .payload
            .snapshot,
        first.snapshot
    );
    assert_eq!(
        store
            .lookup(newer, 103, token())
            .await
            .unwrap()
            .value
            .unwrap()
            .payload
            .snapshot,
        processed.snapshot
    );
    let mut incompatible = a;
    incompatible.schema_version = 2;
    assert!(
        store
            .lookup(incompatible, 103, token())
            .await
            .unwrap()
            .value
            .is_none()
    );
    let database = sql(&fixture).await;
    let counts: (i64,i64,i64) = sqlx::query_as("SELECT (SELECT count(*) FROM openlegal.cache_query),(SELECT count(*) FROM openlegal.cache_snapshot),(SELECT count(*) FROM openlegal.blob_object)").fetch_one(&database).await.unwrap();
    assert_eq!(counts, (2, 3, 1));
    assert!(
        sqlx::query(
            "UPDATE openlegal.cache_head SET schema_version=2 WHERE processor_version='v1'"
        )
        .execute(&database)
        .await
        .is_err()
    );
    store.close().await.unwrap();
    database.close().await;
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh PostgreSQL 18"]
async fn history_cursors_keep_high_water_query_incarnation_and_full_unsigned_timestamps() {
    let fixture = TestDatabase::new().await;
    let store = fixture.open(u64::MAX - 100).await;
    let k = key("001");
    let a = publish(&store, &k, "A", b"A", u64::MAX - 30).await;
    publish(&store, &k, "B", b"B", u64::MAX - 20).await;
    publish(&store, &k, "C", b"C", u64::MAX - 10).await;
    let first = store
        .list(k.history.clone(), None, 1, u64::MAX, token())
        .await
        .unwrap();
    assert_eq!(first.snapshots[0].sequence, 3);
    let cursor = first.next_cursor.unwrap();
    publish(&store, &k, "D", b"D", u64::MAX).await;
    let rest = store
        .list(
            k.history.clone(),
            Some(cursor.clone()),
            20,
            u64::MAX,
            token(),
        )
        .await
        .unwrap();
    assert_eq!(
        rest.snapshots
            .iter()
            .map(|s| s.sequence)
            .collect::<Vec<_>>(),
        [2, 1]
    );
    assert!(rest.next_cursor.is_none());
    publish(&store, &key("other"), "A", b"A", u64::MAX).await;
    assert_eq!(
        store
            .list(key("other").history, Some(cursor), 20, u64::MAX, token())
            .await
            .unwrap_err(),
        Error::InvalidInput
    );
    let exact = store
        .get(
            k.history.clone(),
            a.snapshot.as_ref().unwrap().snapshot_id.clone(),
            u64::MAX,
            token(),
        )
        .await
        .unwrap();
    assert_eq!(exact.provenance.retrieved_at, u64::MAX - 30);
    assert_eq!(exact.snapshot.captured_at, u64::MAX - 30);
    let rollback = publish(&store, &k, "D", b"D", u64::MAX - 40).await;
    assert_eq!(
        rollback.snapshot.as_ref().unwrap().captured_at,
        u64::MAX - 40
    );
    assert!(
        store
            .get(
                k.history,
                a.snapshot.as_ref().unwrap().snapshot_id.clone(),
                u64::MAX - 40,
                token()
            )
            .await
            .unwrap()
            .clock_anomaly
    );
    store.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh PostgreSQL 18"]
async fn age_retention_hides_evidence_before_sweep_and_keeps_shared_referenced_blobs() {
    let fixture = TestDatabase::new().await;
    let policy = RetentionPolicy {
        retention_days: 1,
        ..RetentionPolicy::default()
    };
    let store = open_policy(&fixture, policy, 100, StartupMode::Serve)
        .await
        .unwrap();
    let old = key("old");
    let live = key("live");
    let capture = publish(&store, &old, "shared", b"shared", 100).await;
    publish(&store, &live, "shared", b"shared", 200).await;
    let deadline = 100 + 86400;
    assert!(
        store
            .lookup(old.clone(), deadline, token())
            .await
            .unwrap()
            .value
            .is_none()
    );
    assert!(
        store
            .list(old.history.clone(), None, 20, deadline, token())
            .await
            .unwrap()
            .snapshots
            .is_empty()
    );
    assert_eq!(
        store
            .get(
                old.history,
                capture.snapshot.as_ref().unwrap().snapshot_id.clone(),
                deadline,
                token()
            )
            .await
            .unwrap_err(),
        Error::SnapshotUnavailable
    );
    let database = sql(&fixture).await;
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM openlegal.cache_snapshot")
            .fetch_one(&database)
            .await
            .unwrap(),
        2
    );
    store.maintain(deadline).await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM openlegal.cache_snapshot")
            .fetch_one(&database)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        store
            .lookup(live, deadline, token())
            .await
            .unwrap()
            .value
            .unwrap()
            .payload
            .raw,
        b"shared"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM openlegal.blob_object")
            .fetch_one(&database)
            .await
            .unwrap(),
        1
    );
    store.close().await.unwrap();
    database.close().await;
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh PostgreSQL 18"]
async fn count_caps_evict_logically_and_lowered_startup_requires_explicit_prune() {
    let fixture = TestDatabase::new().await;
    let policy = RetentionPolicy {
        max_snapshots_per_query: 2,
        max_snapshots: 3,
        max_queries: 2,
        ..RetentionPolicy::default()
    };
    let store = open_policy(&fixture, policy.clone(), 100, StartupMode::Serve)
        .await
        .unwrap();
    let k = key("001");
    let old = publish(&store, &k, "A", b"A", 100).await;
    publish(&store, &k, "B", b"B", 101).await;
    publish(&store, &k, "C", b"C", 102).await;
    assert_eq!(
        store
            .get(
                k.history.clone(),
                old.snapshot.as_ref().unwrap().snapshot_id.clone(),
                102,
                token()
            )
            .await
            .unwrap_err(),
        Error::SnapshotUnavailable
    );
    assert_eq!(
        store
            .list(k.history.clone(), None, 20, 102, token())
            .await
            .unwrap()
            .snapshots
            .iter()
            .map(|s| s.sequence)
            .collect::<Vec<_>>(),
        [3, 2]
    );
    publish(&store, &key("002"), "D", b"D", 103).await;
    publish(&store, &key("003"), "E", b"E", 104).await;
    let database = sql(&fixture).await;
    let counts: (i64, i64) =
        sqlx::query_as("SELECT snapshots,queries FROM openlegal.cache_storage WHERE singleton")
            .fetch_one(&database)
            .await
            .unwrap();
    assert!(counts.0 <= 3 && counts.1 <= 2);
    store.close().await.unwrap();
    let lower = RetentionPolicy {
        max_snapshots_per_query: 1,
        max_snapshots: 1,
        max_queries: 1,
        ..policy
    };
    assert!(matches!(
        open_policy(&fixture, lower.clone(), 104, StartupMode::Serve).await,
        Err(Error::StorageCapacity)
    ));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM openlegal.cache_snapshot")
            .fetch_one(&database)
            .await
            .unwrap(),
        counts.0
    );
    let maintenance = open_policy(&fixture, lower.clone(), 104, StartupMode::Maintain)
        .await
        .unwrap();
    maintenance.prune(104).await.unwrap();
    maintenance.close().await.unwrap();
    let reopened = open_policy(&fixture, lower, 104, StartupMode::Serve)
        .await
        .unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM openlegal.cache_snapshot")
            .fetch_one(&database)
            .await
            .unwrap(),
        1
    );
    reopened.close().await.unwrap();
    database.close().await;
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh PostgreSQL 18"]
async fn competing_publications_use_cas_and_per_query_sequences_never_race() {
    let fixture = TestDatabase::new().await;
    let first = fixture.open(100).await;
    let second = fixture.open(100).await;
    let k = key("001");
    let expected = first
        .lookup(k.clone(), 100, token())
        .await
        .unwrap()
        .observation;
    let (a, b) = tokio::join!(
        first.publish(request(
            k.clone(),
            candidate(&k, "A", b"A", 100),
            expected.clone(),
            100
        )),
        second.publish(request(
            k.clone(),
            candidate(&k, "B", b"B", 100),
            expected,
            100
        )),
    );
    let outcomes = [a.unwrap(), b.unwrap()];
    assert_eq!(
        outcomes
            .iter()
            .filter(|o| matches!(o, PublicationOutcome::Accepted(_)))
            .count(),
        1
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|o| matches!(o, PublicationOutcome::Conflict))
            .count(),
        1
    );
    for sequence in 2..=5 {
        let text = format!("capture-{sequence}");
        publish(&first, &k, &text, text.as_bytes(), 100 + sequence).await;
    }
    assert_eq!(
        first
            .list(k.history, None, 20, 106, token())
            .await
            .unwrap()
            .snapshots
            .iter()
            .map(|s| s.sequence)
            .collect::<Vec<_>>(),
        [5, 4, 3, 2, 1]
    );
    let left = key("left");
    let right = key("right");
    let (l, r) = tokio::join!(
        publish(&first, &left, "left", b"left", 107),
        publish(&second, &right, "right", b"right", 107)
    );
    assert_ne!(l.snapshot, r.snapshot);
    first.close().await.unwrap();
    second.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh PostgreSQL 18"]
async fn cancellation_before_commit_leaves_only_reclaimable_unreferenced_bytes() {
    let fixture = TestDatabase::new().await;
    let store = fixture.open(100).await;
    let database = sql(&fixture).await;
    let k = key("001");
    for allowed_checks in 0..=2 {
        let checks = Arc::new(AtomicUsize::new(0));
        let check = checks.clone();
        let expected = store
            .lookup(k.clone(), 100, token())
            .await
            .unwrap()
            .observation;
        let mut publication = request(
            k.clone(),
            candidate(&k, "cancelled", b"cancelled", 100),
            expected,
            100,
        );
        publication.authorize =
            Arc::new(move || check.fetch_add(1, Ordering::SeqCst) < allowed_checks);
        assert!(matches!(
            store.publish(publication).await,
            Err(Error::Cancelled)
        ));
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM openlegal.cache_snapshot")
                .fetch_one(&database)
                .await
                .unwrap(),
            0
        );
        assert!(
            store
                .lookup(k.clone(), 100, token())
                .await
                .unwrap()
                .value
                .is_none()
        );
    }
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM openlegal.blob_object WHERE NOT ready")
            .fetch_one(&database)
            .await
            .unwrap(),
        1
    );
    store.maintain(161).await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM openlegal.blob_object")
            .fetch_one(&database)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM openlegal.blob_deletion")
            .fetch_one(&database)
            .await
            .unwrap(),
        0
    );
    publish(&store, &k, "after cancellation", b"after cancellation", 162).await;
    assert_eq!(
        store
            .list(k.history, None, 20, 162, token())
            .await
            .unwrap()
            .snapshots[0]
            .sequence,
        1
    );
    store.close().await.unwrap();
    database.close().await;
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh PostgreSQL 18"]
async fn missing_or_corrupt_referenced_blobs_are_sticky_integrity_errors() {
    for missing in [false, true] {
        let fixture = TestDatabase::new().await;
        let store = fixture.open(100).await;
        let database = sql(&fixture).await;
        let k = key("001");
        publish(&store, &k, "ABC", b"ABC", 100).await;
        let storage_key: String =
            sqlx::query_scalar("SELECT storage_key FROM openlegal.blob_object WHERE ready")
                .fetch_one(&database)
                .await
                .unwrap();
        let path = fixture.directory.path().join("blobs").join(storage_key);
        if missing {
            std::fs::remove_file(&path).unwrap();
        } else {
            std::fs::write(&path, b"BAD").unwrap();
        }
        assert!(matches!(
            store.lookup(k.clone(), 101, token()).await,
            Err(Error::StorageCorrupt)
        ));
        assert_eq!(store.status(), StorageStatus::IntegrityBlocked);
        assert_eq!(store.health(101).await.unwrap_err(), Error::StorageCorrupt);
        assert!(matches!(
            store.lookup(k, 101, token()).await,
            Err(Error::StorageCorrupt)
        ));
        store.close().await.unwrap();
        database.close().await;
    }
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh PostgreSQL 18"]
async fn sha_collision_or_mutated_structured_identity_never_merges_queries() {
    let fixture = TestDatabase::new().await;
    let store = fixture.open(100).await;
    let database = sql(&fixture).await;
    let original = key("001");
    let other = key("002");
    publish(&store, &original, "A", b"A", 100).await;
    // Deterministic collision seam: retain A's hash while replacing the independently
    // stored structured/canonical identity with B. The lookup must verify both.
    sqlx::query(
        "UPDATE openlegal.cache_query SET query=$1,canonical_identity=$2 WHERE query_hash=$3",
    )
    .bind(serde_json::to_value(&other.history.query).unwrap())
    .bind(canonical_identity(&other.history).unwrap())
    .bind(identity_digest(&original.history).unwrap().as_slice())
    .execute(&database)
    .await
    .unwrap();
    assert!(matches!(
        store.lookup(original, 101, token()).await,
        Err(Error::StorageCorrupt)
    ));
    assert_eq!(store.status(), StorageStatus::IntegrityBlocked);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM openlegal.cache_snapshot")
            .fetch_one(&database)
            .await
            .unwrap(),
        1
    );
    store.close().await.unwrap();
    database.close().await;
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh PostgreSQL 18"]
async fn native_uuid_defaults_and_constraints_preserve_relational_integrity() {
    let fixture = TestDatabase::new().await;
    let store = fixture.open(100).await;
    let database = sql(&fixture).await;
    let k = key("001");
    publish(&store, &k, "A", b"A", 100).await;
    let versions:Vec<i16>=sqlx::query_scalar("SELECT pg_catalog.uuid_extract_version(id) FROM openlegal.cache_storage UNION ALL SELECT pg_catalog.uuid_extract_version(id) FROM openlegal.cache_query UNION ALL SELECT pg_catalog.uuid_extract_version(id) FROM openlegal.cache_snapshot UNION ALL SELECT pg_catalog.uuid_extract_version(generation) FROM openlegal.blob_object").fetch_all(&database).await.unwrap();
    assert_eq!(versions, [7, 7, 7, 7]);
    let defaults:Vec<String>=sqlx::query_scalar("SELECT column_default FROM information_schema.columns WHERE table_schema='openlegal' AND ((table_name IN ('cache_storage','cache_query','cache_snapshot','blob_deletion') AND column_name='id') OR (table_name='blob_object' AND column_name='generation')) ORDER BY table_name").fetch_all(&database).await.unwrap();
    assert_eq!(defaults.len(), 5);
    assert!(defaults.iter().all(|value| value.contains("uuidv7()")));
    assert!(
        sqlx::query(
            "UPDATE openlegal.cache_snapshot SET source_reference='https://example.test/changed'"
        )
        .execute(&database)
        .await
        .is_err()
    );
    assert!(
        sqlx::query("DELETE FROM openlegal.blob_object WHERE ready")
            .execute(&database)
            .await
            .is_err()
    );
    assert!(
        sqlx::query("UPDATE openlegal.blob_object SET ready=false WHERE ready")
            .execute(&database)
            .await
            .is_err()
    );
    assert!(
        sqlx::query("UPDATE openlegal.cache_head SET snapshot_id=$1")
            .bind(Uuid::nil())
            .execute(&database)
            .await
            .is_err()
    );
    assert!(
        sqlx::query("INSERT INTO openlegal.cache_storage DEFAULT VALUES")
            .execute(&database)
            .await
            .is_err()
    );
    assert!(
        sqlx::query("UPDATE openlegal.cache_query SET next_sequence=0")
            .execute(&database)
            .await
            .is_err()
    );
    assert!(
        sqlx::query("UPDATE openlegal.cache_head SET validated_at=$1::text::numeric")
            .bind("18446744073709551616")
            .execute(&database)
            .await
            .is_err()
    );
    assert!(
        sqlx::query("UPDATE openlegal.cache_query SET query_hash=$1")
            .bind(vec![0u8; 31])
            .execute(&database)
            .await
            .is_err()
    );
    assert!(sqlx::query("INSERT INTO openlegal.cache_snapshot(public_id,query_id,sequence,captured_at,retrieved_at,original_validated_at,processor_version,schema_version,raw_blob_sha256,processed_sha256,processed_data,source_reference,envelope_sha256) SELECT $1,query_id,sequence,captured_at,retrieved_at,original_validated_at,processor_version,schema_version,raw_blob_sha256,processed_sha256,processed_data,source_reference,envelope_sha256 FROM openlegal.cache_snapshot LIMIT 1").bind("f".repeat(64)).execute(&database).await.is_err());
    store.close().await.unwrap();
    database.close().await;
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh PostgreSQL 18"]
async fn unique_blob_budget_counts_shared_bytes_once_and_evicts_at_its_boundary() {
    let fixture = TestDatabase::new().await;
    let policy = RetentionPolicy {
        max_blob_bytes: 16 * 1024 * 1024,
        ..RetentionPolicy::default()
    };
    let store = open_policy(&fixture, policy, 100, StartupMode::Serve)
        .await
        .unwrap();
    let shared = vec![0; 1024 * 1024];
    let old = publish(
        &store,
        &key("first"),
        "large synthetic evidence",
        &shared,
        100,
    )
    .await;
    publish(
        &store,
        &key("alias"),
        "same bytes under another query",
        &shared,
        101,
    )
    .await;
    let database = sql(&fixture).await;
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT referenced_bytes FROM openlegal.cache_storage WHERE singleton"
        )
        .fetch_one(&database)
        .await
        .unwrap(),
        1024 * 1024
    );
    for n in 1u8..=16 {
        let raw = vec![n; 1024 * 1024];
        publish(
            &store,
            &key(&format!("record_{n}")),
            "large synthetic evidence",
            &raw,
            101 + u64::from(n),
        )
        .await;
        let referenced: i64 = sqlx::query_scalar(
            "SELECT referenced_bytes FROM openlegal.cache_storage WHERE singleton",
        )
        .fetch_one(&database)
        .await
        .unwrap();
        assert!(referenced <= 16 * 1024 * 1024);
    }
    let totals: (i64, i64) = sqlx::query_as(
        "SELECT snapshots,referenced_bytes FROM openlegal.cache_storage WHERE singleton",
    )
    .fetch_one(&database)
    .await
    .unwrap();
    assert_eq!(totals, (16, 16 * 1024 * 1024));
    assert_eq!(
        store
            .get(
                key("first").history,
                old.snapshot.as_ref().unwrap().snapshot_id.clone(),
                118,
                token()
            )
            .await
            .unwrap_err(),
        Error::SnapshotUnavailable
    );
    assert!(
        store
            .lookup(key("alias"), 118, token())
            .await
            .unwrap()
            .value
            .is_none()
    );
    store.close().await.unwrap();
    database.close().await;
}

struct DatabaseProxy {
    address: std::net::SocketAddr,
    cancellation: CancellationToken,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl DatabaseProxy {
    async fn start(address: std::net::SocketAddr, upstream: std::net::SocketAddr) -> Self {
        let socket = tokio::net::TcpSocket::new_v4().unwrap();
        socket.set_reuseaddr(true).unwrap();
        socket.bind(address).unwrap();
        let listener = socket.listen(16).unwrap();
        let address = listener.local_addr().unwrap();
        let cancellation = token();
        let shutdown = cancellation.clone();
        let task = tokio::spawn(async move {
            let mut connections = tokio::task::JoinSet::new();
            loop {
                tokio::select! { biased;
                    _=shutdown.cancelled()=>break,
                    accepted=listener.accept()=>{
                        let Ok((mut client,_))=accepted else {break};
                        connections.spawn(async move {
                            if let Ok(mut server)=tokio::net::TcpStream::connect(upstream).await {
                                let _=tokio::io::copy_bidirectional(&mut client,&mut server).await;
                            }
                        });
                    }
                }
                while connections.try_join_next().is_some() {}
            }
            connections.abort_all();
            while connections.join_next().await.is_some() {}
        });
        Self {
            address,
            cancellation,
            task: Some(task),
        }
    }
    async fn stop(mut self) {
        self.cancellation.cancel();
        self.task.take().unwrap().await.unwrap();
    }
}
impl Drop for DatabaseProxy {
    fn drop(&mut self) {
        self.cancellation.cancel();
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh PostgreSQL 18"]
async fn actual_database_connection_outage_is_not_a_miss_and_recovers_in_place() {
    let fixture = TestDatabase::new().await;
    let mut url = url::Url::parse(&fixture.url).unwrap();
    let upstream = std::net::SocketAddr::new(
        url.host_str().unwrap().parse().unwrap(),
        url.port().unwrap(),
    );
    let proxy = DatabaseProxy::start("127.0.0.1:0".parse().unwrap(), upstream).await;
    let address = proxy.address;
    url.set_host(Some("127.0.0.1")).unwrap();
    url.set_port(Some(address.port())).unwrap();
    let blobs = FsBlobStore::open(&fixture.directory.path().join("blobs"))
        .await
        .unwrap();
    let store = PostgresStore::open(
        url.as_str(),
        options(),
        blobs,
        RetentionPolicy::default(),
        100,
        StartupMode::Serve,
    )
    .await
    .unwrap();
    let k = key("001");
    let original = publish(&store, &k, "A", b"A", 100).await;
    let epoch = store.epoch();
    let generation = store.recovery_epoch();
    proxy.stop().await;
    assert_eq!(
        store.health(101).await.unwrap_err(),
        Error::StorageUnavailable
    );
    assert_eq!(store.status(), StorageStatus::Recovering);
    assert!(store.epoch() > epoch && store.recovery_epoch() > generation);
    assert!(matches!(
        store.lookup(key("never_captured"), 101, token()).await,
        Err(Error::StorageUnavailable)
    ));
    let proxy = DatabaseProxy::start(address, upstream).await;
    store.health(102).await.unwrap();
    assert!(store.healthy());
    let restored = store
        .lookup(k, 102, token())
        .await
        .unwrap()
        .value
        .unwrap()
        .payload;
    assert_eq!(restored.snapshot, original.snapshot);
    assert_eq!(restored.raw, b"A");
    store.close().await.unwrap();
    proxy.stop().await;
}
