use super::*;
use openlegal_application::{
    StoredPayload,
    persistence::{PublicationOutcome, PublicationRequest, identity_digest},
};
use openlegal_domain::{Provenance, Query, Record, RetrievalData};
use sha2::{Digest, Sha256};
#[path = "../../../../test-support/postgres.rs"]
mod fixture;

fn key() -> PersistentKey {
    PersistentKey {
        history: HistoryKey {
            namespace: "test".into(),
            provider: "synthetic".into(),
            dataset: "records".into(),
            query: Query::Get {
                source: "layout_a".into(),
                id: "001".into(),
            },
        },
        processor_version: "v1".into(),
        schema_version: 1,
    }
}
fn payload(now: u64) -> Arc<StoredPayload> {
    let raw = b"synthetic evidence".to_vec();
    Arc::new(StoredPayload {
        data: RetrievalData::Get(Record {
            source: "layout_a".into(),
            id: "001".into(),
            title: "Fiction".into(),
            body: "Body".into(),
            synthetic: true,
        }),
        provenance: Provenance {
            provider: "synthetic".into(),
            dataset: "records".into(),
            source_reference: "https://example.test/record/001".into(),
            payload_sha256: data::hex(&Sha256::digest(&raw)),
            processor_version: "v1".into(),
            retrieved_at: now,
            validated_at: now,
        },
        raw,
        bytes: 2048,
        snapshot: None,
    })
}
async fn request(store: &PostgresStore, now: u64) -> PublicationRequest {
    let expected = store
        .lookup(key(), now, CancellationToken::new())
        .await
        .unwrap()
        .observation;
    PublicationRequest {
        key: key(),
        value: payload(now),
        expected,
        now,
        authorize: Arc::new(|| true),
        cancellation: CancellationToken::new(),
    }
}
fn gate(store: &PostgresStore, point: TestPoint, error: Option<Error>) -> Arc<TestGate> {
    let gate = Arc::new(TestGate {
        reached: Semaphore::new(0),
        resume: Semaphore::new(0),
        error,
    });
    store
        .inner
        .hooks
        .lock()
        .unwrap()
        .insert(point, gate.clone());
    gate
}
async fn reached(gate: &TestGate) {
    tokio::time::timeout(Duration::from_secs(5), gate.reached.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh PostgreSQL 18"]
async fn cancellation_after_blob_and_before_commit_never_installs_a_snapshot() {
    for point in [TestPoint::AfterBlob, TestPoint::BeforeCommit] {
        let db = fixture::TestDatabase::new().await;
        let store = db.open(100).await;
        let request = request(&store, 100).await;
        let cancellation = request.cancellation.clone();
        let gate = gate(&store, point, None);
        let task = tokio::spawn(store.publish(request));
        reached(&gate).await;
        cancellation.cancel();
        gate.resume.add_permits(1);
        assert!(matches!(task.await.unwrap(), Err(Error::Cancelled)));
        let snapshots: i64 = sqlx::query_scalar("SELECT count(*) FROM openlegal.cache_snapshot")
            .fetch_one(&store.inner.pool)
            .await
            .unwrap();
        assert_eq!(snapshots, 0);
        assert!(
            store
                .lookup(key(), 100, CancellationToken::new())
                .await
                .unwrap()
                .value
                .is_none()
        );
        store.close().await.unwrap();
    }
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh PostgreSQL 18"]
async fn lost_commit_acknowledgement_is_reconciled_without_replaying_publication() {
    let db = fixture::TestDatabase::new().await;
    let store = db.open(100).await;
    let request = request(&store, 100).await;
    let gate = gate(
        &store,
        TestPoint::AfterCommit,
        Some(Error::StorageUnavailable),
    );
    let task = tokio::spawn(store.publish(request));
    reached(&gate).await;
    gate.resume.add_permits(1);
    assert!(matches!(
        task.await.unwrap(),
        Err(Error::StorageUnavailable)
    ));
    assert!(!store.healthy());
    store.health(100).await.unwrap();
    assert!(store.healthy());
    assert!(
        store
            .lookup(key(), 100, CancellationToken::new())
            .await
            .unwrap()
            .value
            .is_some()
    );
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM openlegal.cache_snapshot")
        .fetch_one(&store.inner.pool)
        .await
        .unwrap();
    assert_eq!(count, 1);
    store.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh PostgreSQL 18"]
async fn recovery_fence_waits_for_transaction_and_cannot_clear_new_integrity_failure() {
    let db = fixture::TestDatabase::new().await;
    let store = db.open(100).await;
    let tx = DbTransaction::begin(&store.inner.pool).await.unwrap();
    store.record(&Err::<(), _>(Error::StorageUnavailable));
    assert_eq!(
        store.health(100).await.err(),
        Some(Error::StorageUnavailable)
    );
    assert!(!store.healthy());
    tx.commit().await.unwrap();
    let gate = gate(&store, TestPoint::BeforeRecoverReady, None);
    let task = tokio::spawn(store.health(100));
    reached(&gate).await;
    store.record(&Err::<(), _>(Error::StorageCorrupt));
    gate.resume.add_permits(1);
    let _ = task.await.unwrap();
    assert_eq!(store.status(), StorageStatus::IntegrityBlocked);
    assert_eq!(store.health(100).await.err(), Some(Error::StorageCorrupt));
    store.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh PostgreSQL 18"]
async fn late_delete_cannot_remove_same_digest_republished_in_another_generation() {
    let db = fixture::TestDatabase::new().await;
    let store = db.open(100).await;
    let pending = request(&store, 100).await;
    let upload_gate = gate(&store, TestPoint::AfterBlob, None);
    let task = tokio::spawn(store.publish(pending));
    reached(&upload_gate).await;
    // Retire only the pending location while its publisher is paused outside SQL.
    let mut tx = DbTransaction::begin(&store.inner.pool).await.unwrap();
    let row =
        sqlx::query("SELECT sha256,generation,size_bytes,storage_key FROM openlegal.blob_object")
            .fetch_one(tx.conn().unwrap())
            .await
            .unwrap();
    let old = data::location(&row).unwrap();
    sqlx::query("INSERT INTO openlegal.blob_deletion(storage_key,sha256,generation,size_bytes,queued_at) SELECT storage_key,sha256,generation,size_bytes,160 FROM openlegal.blob_object").execute(tx.conn().unwrap()).await.unwrap();
    sqlx::query("DELETE FROM openlegal.blob_object")
        .execute(tx.conn().unwrap())
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let deletion = gate(&store, TestPoint::BeforeDelete, None);
    let gc_store = store.clone();
    let gc = tokio::spawn(async move { gc_store.maintain(160).await });
    reached(&deletion).await;
    let published = store.publish(request(&store, 160).await).await.unwrap();
    assert!(matches!(published, PublicationOutcome::Accepted(_)));
    deletion.resume.add_permits(1);
    gc.await.unwrap().unwrap();
    upload_gate.resume.add_permits(1);
    assert!(matches!(
        task.await.unwrap(),
        Ok(PublicationOutcome::Conflict) | Err(Error::Busy)
    ));
    assert!(
        store
            .inner
            .blobs
            .get(old, CancellationToken::new())
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .lookup(key(), 160, CancellationToken::new())
            .await
            .unwrap()
            .value
            .is_some()
    );
    store.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh PostgreSQL 18"]
async fn corrupt_gc_location_cannot_delete_live_evidence() {
    let db = fixture::TestDatabase::new().await;
    let store = db.open(100).await;
    assert!(matches!(
        store.publish(request(&store, 100).await).await.unwrap(),
        PublicationOutcome::Accepted(_)
    ));
    sqlx::query("INSERT INTO openlegal.blob_deletion(storage_key,sha256,generation,size_bytes,queued_at) SELECT storage_key,sha256,pg_catalog.uuidv7(),size_bytes,100 FROM openlegal.blob_object").execute(&store.inner.pool).await.unwrap();
    assert_eq!(store.maintain(100).await.err(), Some(Error::StorageCorrupt));
    let location =
        sqlx::query("SELECT sha256,generation,size_bytes,storage_key FROM openlegal.blob_object")
            .fetch_one(&store.inner.pool)
            .await
            .unwrap();
    assert!(
        store
            .inner
            .blobs
            .get(data::location(&location).unwrap(), CancellationToken::new())
            .await
            .unwrap()
            .is_some()
    );
    assert_eq!(store.status(), StorageStatus::IntegrityBlocked);
    store.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh PostgreSQL 18"]
async fn hash_collision_never_merges_a_different_structured_identity() {
    let db = fixture::TestDatabase::new().await;
    let store = db.open(100).await;
    assert!(matches!(
        store.publish(request(&store, 100).await).await.unwrap(),
        PublicationOutcome::Accepted(_)
    ));
    sqlx::query("UPDATE openlegal.cache_query SET query=jsonb_set(query,'{id}','\"different\"'::jsonb) WHERE query_hash=$1").bind(identity_digest(&key().history).unwrap().as_slice()).execute(&store.inner.pool).await.unwrap();
    assert!(matches!(
        store.lookup(key(), 100, CancellationToken::new()).await,
        Err(Error::StorageCorrupt)
    ));
    store.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh PostgreSQL 18"]
async fn newer_availability_failure_and_shutdown_fence_an_older_successful_recovery_probe() {
    for shutdown in [false, true] {
        let db = fixture::TestDatabase::new().await;
        let store = db.open(100).await;
        store.record(&Err::<(), _>(Error::StorageUnavailable));
        let gate = gate(&store, TestPoint::BeforeRecoverReady, None);
        let task = tokio::spawn(store.health(100));
        reached(&gate).await;
        if shutdown {
            store.close().await.unwrap();
        } else {
            store.record(&Err::<(), _>(Error::StorageUnavailable));
        }
        gate.resume.add_permits(1);
        assert_eq!(task.await.unwrap().err(), Some(Error::StorageUnavailable));
        assert!(!store.healthy());
        if shutdown {
            assert_eq!(store.status(), StorageStatus::Closed);
        } else {
            assert_eq!(store.status(), StorageStatus::Recovering);
            store.health(100).await.unwrap();
            assert!(store.healthy());
            store.close().await.unwrap();
        }
    }
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh PostgreSQL 18"]
async fn health_probe_during_retention_barrier_does_not_fail_a_valid_publication() {
    let db = fixture::TestDatabase::new().await;
    let blobs = crate::blob::FsBlobStore::open(&db.directory.path().join("blobs"))
        .await
        .unwrap();
    let policy = RetentionPolicy {
        max_snapshots_per_query: 1,
        ..RetentionPolicy::default()
    };
    let store = PostgresStore::open(
        &db.url,
        fixture::options(),
        blobs,
        policy,
        100,
        StartupMode::Serve,
    )
    .await
    .unwrap();
    assert!(matches!(
        store.publish(request(&store, 100).await).await.unwrap(),
        PublicationOutcome::Accepted(_)
    ));
    let mut second = request(&store, 101).await;
    if let RetrievalData::Get(record) = &mut Arc::get_mut(&mut second.value).unwrap().data {
        record.body = "Changed".into();
    }
    let gate = gate(&store, TestPoint::BeforeCommit, None);
    let task = tokio::spawn(store.publish(second));
    reached(&gate).await;
    assert_eq!(store.status(), StorageStatus::Maintaining);
    assert_eq!(store.health(101).await.err(), Some(Error::Busy));
    assert_eq!(store.status(), StorageStatus::Maintaining);
    gate.resume.add_permits(1);
    assert!(matches!(
        task.await.unwrap().unwrap(),
        PublicationOutcome::Accepted(_)
    ));
    assert!(store.healthy());
    store.health(101).await.unwrap();
    let metrics = store.metrics();
    assert_eq!(metrics.snapshots, 1);
    assert!(metrics.blob_reads > 0);
    assert!(metrics.blob_writes > 0);
    assert!(metrics.pool_connections <= 4);
    store.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh PostgreSQL 18"]
async fn retiring_a_reserved_ready_blob_is_contention_not_corruption() {
    let db = fixture::TestDatabase::new().await;
    let store = db.open(100).await;
    assert!(matches!(
        store.publish(request(&store, 100).await).await.unwrap(),
        PublicationOutcome::Accepted(_)
    ));
    let gc_store = db.open(101).await;
    let row =
        sqlx::query("SELECT sha256,generation,size_bytes,storage_key FROM openlegal.blob_object")
            .fetch_one(&store.inner.pool)
            .await
            .unwrap();
    let retired = data::location(&row).unwrap();
    let pending = request(&store, 101).await;
    let reservation = gate(&store, TestPoint::AfterBlobReservation, None);
    let task = tokio::spawn(store.publish(pending));
    reached(&reservation).await;

    // Another instance expires the last reference while this publisher is outside SQL.
    let deadline = 100 + RetentionPolicy::default().retention_days * 86_400;
    gc_store.maintain(deadline).await.unwrap();
    assert!(
        store
            .inner
            .blobs
            .get(retired.clone(), CancellationToken::new())
            .await
            .unwrap()
            .is_none()
    );
    reservation.resume.add_permits(1);
    assert!(matches!(task.await.unwrap(), Err(Error::Busy)));
    assert_eq!(store.status(), StorageStatus::Ready);
    assert_eq!(store.metrics().corruptions, 0);
    let counts: (i64, i64) = sqlx::query_as(
        "SELECT (SELECT count(*) FROM openlegal.cache_snapshot), (SELECT count(*) FROM openlegal.blob_object)",
    )
    .fetch_one(&store.inner.pool)
    .await
    .unwrap();
    assert_eq!(counts, (0, 0));

    assert!(matches!(
        store
            .publish(request(&store, deadline).await)
            .await
            .unwrap(),
        PublicationOutcome::Accepted(_)
    ));
    let new_key: String = sqlx::query_scalar("SELECT storage_key FROM openlegal.blob_object")
        .fetch_one(&store.inner.pool)
        .await
        .unwrap();
    assert_ne!(new_key, retired.storage_key);
    assert!(
        store
            .lookup(key(), deadline, CancellationToken::new())
            .await
            .unwrap()
            .value
            .is_some()
    );
    gc_store.close().await.unwrap();
    store.close().await.unwrap();
}
