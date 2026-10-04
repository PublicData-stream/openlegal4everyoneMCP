//! Demand admission evidence using fictional targets and isolated PostgreSQL.
#[path = "../../../test-support/postgres.rs"]
mod support;
use openlegal_adapters::{blob::FsBlobStore, corpus::PgCorpusStore};
use openlegal_application::{
    demand_collection::DemandCollectionStore, persistence::PersistentStore,
};
use openlegal_domain::collection::{
    CollectionRequest, CollectionSearchMode, CollectionTarget, DemandCollectionState,
};
use tokio_util::sync::CancellationToken;

fn request() -> CollectionRequest {
    CollectionRequest {
        target: CollectionTarget::Search {
            mode: CollectionSearchMode::Literal,
            term: "Fictional".into(),
            datasets: vec![],
        },
    }
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn explicit_failure_and_partial_cooldown_start_at_completion_while_success_keeps_daily_receipt()
 {
    let fixture = support::TestDatabase::new().await;
    let base = fixture.open(100).await;
    let pool = base.pool();
    let blobs = FsBlobStore::open(&fixture.directory.path().join("explicit-cooldown"))
        .await
        .unwrap();
    let store = PgCorpusStore::new(pool.clone(), blobs);
    store.heartbeat_collection_scheduler().await.unwrap();
    let first = store.request_collection(request()).await.unwrap();
    let (id, _) = store.claim_collection_request().await.unwrap().unwrap();
    sqlx::query("UPDATE openlegal.collection_request SET created_at=created_at-7200 WHERE id=$1")
        .bind(uuid::Uuid::parse_str(&id).unwrap())
        .execute(&pool)
        .await
        .unwrap();
    store
        .settle_collection_request_with_reason(&id, "failed", Some("source_inventory_incomplete"))
        .await
        .unwrap();
    assert_eq!(
        store
            .request_collection(request())
            .await
            .unwrap()
            .request_id,
        first.request_id
    );
    sqlx::query("UPDATE openlegal.collection_request SET completed_at=floor(extract(epoch from clock_timestamp()))::bigint-3600 WHERE id=$1")
        .bind(uuid::Uuid::parse_str(&id).unwrap()).execute(&pool).await.unwrap();
    let partial = store.request_collection(request()).await.unwrap();
    assert_ne!(partial.request_id, first.request_id);
    let (id, _) = store.claim_collection_request().await.unwrap().unwrap();
    store
        .settle_collection_request_with_reason(&id, "done", Some("download_failed"))
        .await
        .unwrap();
    assert_eq!(
        store
            .request_collection(request())
            .await
            .unwrap()
            .request_id,
        partial.request_id
    );
    sqlx::query("UPDATE openlegal.collection_request SET completed_at=floor(extract(epoch from clock_timestamp()))::bigint-3600 WHERE id=$1")
        .bind(uuid::Uuid::parse_str(&id).unwrap()).execute(&pool).await.unwrap();
    let success = store.request_collection(request()).await.unwrap();
    assert_ne!(success.request_id, partial.request_id);
    let (id, _) = store.claim_collection_request().await.unwrap().unwrap();
    store.settle_collection_request(&id, "done").await.unwrap();
    sqlx::query("UPDATE openlegal.collection_request SET completed_at=floor(extract(epoch from clock_timestamp()))::bigint-4000 WHERE id=$1")
        .bind(uuid::Uuid::parse_str(&id).unwrap()).execute(&pool).await.unwrap();
    assert_eq!(
        store
            .request_collection(request())
            .await
            .unwrap()
            .request_id,
        success.request_id
    );
    assert_eq!(
        store
            .collection_status(&first.request_id)
            .await
            .unwrap()
            .status,
        "failed"
    );
    assert_eq!(
        store
            .collection_status(&partial.request_id)
            .await
            .unwrap()
            .reason
            .as_deref(),
        Some("download_failed")
    );
    base.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn recently_completed_already_fresh_receipt_does_not_extend_authoritative_head_freshness() {
    use openlegal_application::database::Publication;
    use openlegal_domain::legal::{Dataset, LegalRecord, ObjectId};
    let fixture = support::TestDatabase::new().await;
    let base = fixture.open(100).await;
    let pool = base.pool();
    let blobs = FsBlobStore::open(&fixture.directory.path().join("demand-head-ttl"))
        .await
        .unwrap();
    let store = PgCorpusStore::new(pool.clone(), blobs);
    let object = ObjectId {
        jurisdiction: "kr".into(),
        provider: "law_go_kr".into(),
        dataset: Dataset::NationalStatute,
        id: "123".into(),
    };
    let now: i64 =
        sqlx::query_scalar("SELECT floor(extract(epoch from clock_timestamp()))::bigint")
            .fetch_one(&pool)
            .await
            .unwrap();
    let state = store.state(&object).await.unwrap();
    store
        .publish(
            Publication {
                record: LegalRecord {
                    object: object.clone(),
                    revision_id: "r1".into(),
                    title: "Fictional statute".into(),
                    body: "Fictional body".into(),
                    sections: vec![],
                    metadata: Default::default(),
                    publication_date: Some("20260101".into()),
                    effective_date: Some("20260101".into()),
                    source_url: "https://example.test/fictional".into(),
                    representation: "provider_text_v1".into(),
                },
                raw: b"Fictional body".to_vec(),
                additional_evidence: vec![],
                processor_version: "fixture_v1".into(),
                retrieved_at: now as u64,
                now: now as u64,
                expected_version: state.version,
                install_head: true,
                job_id: None,
            },
            CancellationToken::new(),
        )
        .await
        .unwrap();
    store.heartbeat_collection_scheduler().await.unwrap();
    let request = CollectionRequest {
        target: CollectionTarget::Object { object },
    };
    let receipt = store.request_collection(request.clone()).await.unwrap();
    let (id, _) = store.claim_collection_request().await.unwrap().unwrap();
    store
        .settle_collection_request_with_reason(&id, "skipped", Some("already_fresh"))
        .await
        .unwrap();
    assert_eq!(
        store
            .request_demand(request.clone(), CancellationToken::new())
            .await
            .unwrap()
            .status,
        DemandCollectionState::Fresh
    );
    sqlx::query("UPDATE openlegal.corpus_object SET validated_at=floor(extract(epoch from clock_timestamp()))::numeric-3600 WHERE head_capture IS NOT NULL").execute(&pool).await.unwrap();
    let refresh = store
        .request_demand(request, CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(refresh.status, DemandCollectionState::Pending);
    assert_ne!(refresh.receipt.unwrap().request_id, receipt.request_id);
    assert_eq!(
        store
            .collection_status(&receipt.request_id)
            .await
            .unwrap()
            .reason
            .as_deref(),
        Some("already_fresh")
    );
    base.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn automatic_hourly_refresh_preserves_explicit_daily_receipts_and_shared_active_work() {
    let fixture = support::TestDatabase::new().await;
    let base = fixture.open(100).await;
    let pool = base.pool();
    let blobs = FsBlobStore::open(&fixture.directory.path().join("demand-hourly"))
        .await
        .unwrap();
    let store = PgCorpusStore::new(pool.clone(), blobs);
    store.heartbeat_collection_scheduler().await.unwrap();
    let first = store.request_collection(request()).await.unwrap();
    let joining = store
        .request_demand(request(), CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(joining.receipt.unwrap().request_id, first.request_id);
    let (id, _) = store.claim_collection_request().await.unwrap().unwrap();
    store.settle_collection_request(&id, "done").await.unwrap();
    assert_eq!(
        store
            .request_demand(request(), CancellationToken::new())
            .await
            .unwrap()
            .status,
        DemandCollectionState::Fresh
    );
    sqlx::query("UPDATE openlegal.collection_request SET completed_at=floor(extract(epoch from clock_timestamp()))::bigint-3600 WHERE id=$1")
        .bind(uuid::Uuid::parse_str(&id).unwrap()).execute(&pool).await.unwrap();
    // Before the automatic refresh, explicit callers still reuse the daily receipt.
    assert_eq!(
        store
            .request_collection(request())
            .await
            .unwrap()
            .request_id,
        id
    );
    let refreshed = store
        .request_demand(request(), CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(refreshed.status, DemandCollectionState::Pending);
    let next_id = refreshed.receipt.unwrap().request_id;
    assert_ne!(next_id, id);
    assert_eq!(store.collection_status(&id).await.unwrap().status, "done");
    assert_eq!(
        store
            .request_collection(request())
            .await
            .unwrap()
            .request_id,
        next_id
    );
    assert_eq!(
        store
            .request_demand(request(), CancellationToken::new())
            .await
            .unwrap()
            .receipt
            .unwrap()
            .request_id,
        next_id
    );
    base.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn valid_empty_search_is_fresh_but_partial_failure_is_never_fresh() {
    let fixture = support::TestDatabase::new().await;
    let base = fixture.open(100).await;
    let pool = base.pool();
    let blobs = FsBlobStore::open(&fixture.directory.path().join("demand-empty"))
        .await
        .unwrap();
    let store = PgCorpusStore::new(pool.clone(), blobs);
    store.heartbeat_collection_scheduler().await.unwrap();
    let receipt = store
        .request_demand(request(), CancellationToken::new())
        .await
        .unwrap()
        .receipt
        .unwrap();
    let (id, _) = store.claim_collection_request().await.unwrap().unwrap();
    assert_eq!(id, receipt.request_id);
    store
        .settle_collection_request_with_reason(&id, "skipped", Some("no_matches"))
        .await
        .unwrap();
    assert_eq!(
        store
            .request_demand(request(), CancellationToken::new())
            .await
            .unwrap()
            .status,
        DemandCollectionState::Fresh
    );
    sqlx::query("UPDATE openlegal.collection_request SET completed_at=floor(extract(epoch from clock_timestamp()))::bigint-3600 WHERE id=$1")
        .bind(uuid::Uuid::parse_str(&id).unwrap()).execute(&pool).await.unwrap();
    store
        .request_demand(request(), CancellationToken::new())
        .await
        .unwrap();
    let (id, _) = store.claim_collection_request().await.unwrap().unwrap();
    store
        .settle_collection_request_with_reason(&id, "done", Some("download_failed"))
        .await
        .unwrap();
    let failed = store
        .request_demand(request(), CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(failed.status, DemandCollectionState::Unavailable);
    assert_eq!(failed.receipt.unwrap().request_id, id);
    base.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn deferred_requests_count_toward_bounded_admission_and_cancel_does_not_enqueue() {
    let fixture = support::TestDatabase::new().await;
    let base = fixture.open(100).await;
    let pool = base.pool();
    let blobs = FsBlobStore::open(&fixture.directory.path().join("demand-bound"))
        .await
        .unwrap();
    let store = PgCorpusStore::new(pool.clone(), blobs);
    store.heartbeat_collection_scheduler().await.unwrap();
    let cancel = CancellationToken::new();
    cancel.cancel();
    assert!(store.request_demand(request(), cancel).await.is_err());
    for i in 0..128 {
        let target = CollectionRequest {
            target: CollectionTarget::Search {
                mode: CollectionSearchMode::Literal,
                term: format!("fictional{i}"),
                datasets: vec![],
            },
        };
        let receipt = store.request_collection(target).await.unwrap();
        sqlx::query("UPDATE openlegal.collection_request SET status='deferred',lease_until=floor(extract(epoch from clock_timestamp()))::bigint+3600 WHERE id=$1")
            .bind(uuid::Uuid::parse_str(&receipt.request_id).unwrap()).execute(&pool).await.unwrap();
    }
    assert!(
        store
            .request_demand(request(), CancellationToken::new())
            .await
            .is_err()
    );
    base.close().await.unwrap();
}
