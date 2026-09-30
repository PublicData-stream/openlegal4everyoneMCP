//! Explicit collection reasons using isolated PostgreSQL and fictional targets.
#[path = "../../../test-support/postgres.rs"]
mod support;
use openlegal_adapters::{blob::FsBlobStore, corpus::PgCorpusStore};
use openlegal_application::persistence::PersistentStore;
use openlegal_domain::{
    collection::{CollectionRequest, CollectionTarget},
    legal::{DatabaseError, Dataset, ObjectId},
};

fn request(id: &str) -> CollectionRequest {
    CollectionRequest {
        target: CollectionTarget::Object {
            object: ObjectId {
                jurisdiction: "kr".into(),
                provider: "law_go_kr".into(),
                dataset: Dataset::NationalStatute,
                id: id.into(),
            },
        },
    }
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn skip_reasons_survive_coalescing_and_terminal_receipts_but_clear_on_retry() {
    let fixture = support::TestDatabase::new().await;
    let base = fixture.open(100).await;
    let blobs = FsBlobStore::open(&fixture.directory.path().join("collection-reasons"))
        .await
        .unwrap();
    let store = PgCorpusStore::new(base.pool(), blobs);
    store.heartbeat_collection_scheduler().await.unwrap();
    for (index, reason) in [
        "collection_pending",
        "already_fresh",
        "collection_already_in_progress",
        "head_observation_superseded",
        "publication_superseded",
        "no_matches",
        "multiple_skip_reasons",
    ]
    .into_iter()
    .enumerate()
    {
        let requested = request(&format!("{index:03}"));
        let queued = store.request_collection(requested.clone()).await.unwrap();
        let (id, _) = store.claim_collection_request().await.unwrap().unwrap();
        assert_eq!(id, queued.request_id);
        assert_eq!(
            store
                .settle_collection_request_with_reason(&id, "skipped", Some("untrusted_detail"))
                .await,
            Err(DatabaseError::InvalidInput)
        );
        store
            .settle_collection_request_with_reason(&id, "skipped", Some(reason))
            .await
            .unwrap();
        let status = store.collection_status(&id).await.unwrap();
        assert_eq!(status.status, "skipped");
        assert_eq!(status.reason.as_deref(), Some(reason));
        assert_eq!(status.retry_after_seconds, 0);
        let coalesced = store.request_collection(requested.clone()).await.unwrap();
        assert_eq!(coalesced.request_id, id);
        assert_eq!(coalesced.reason.as_deref(), Some(reason));
        let payload: serde_json::Value = sqlx::query_scalar(
            "SELECT payload FROM openlegal.collection_request WHERE id=$1::uuid",
        )
        .bind(&id)
        .fetch_one(&base.pool())
        .await
        .unwrap();
        assert_eq!(payload, serde_json::json!({}));
        assert!(
            sqlx::query("UPDATE openlegal.collection_request SET reason='untrusted_detail' WHERE id=$1::uuid")
                .bind(&id).execute(&base.pool()).await.is_err()
        );
        sqlx::query(
            "UPDATE openlegal.collection_request SET created_at=created_at-3601 WHERE id=$1::uuid",
        )
        .bind(&id)
        .execute(&base.pool())
        .await
        .unwrap();
        let retry = store.request_collection(requested).await.unwrap();
        assert_ne!(retry.request_id, id);
        assert_eq!(retry.status, "queued");
        assert!(retry.reason.is_none());
        assert_eq!(
            store
                .collection_status(&id)
                .await
                .unwrap()
                .reason
                .as_deref(),
            Some(reason)
        );
        let (retry_id, _) = store.claim_collection_request().await.unwrap().unwrap();
        assert_eq!(retry_id, retry.request_id);
        store
            .settle_collection_request(&retry_id, "done")
            .await
            .unwrap();
    }
    base.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn skip_reason_migration_preserves_legacy_null_and_existing_failure_reasons() {
    let fixture = support::TestDatabase::new().await;
    let base = fixture.open(100).await;
    let blobs = FsBlobStore::open(&fixture.directory.path().join("legacy-collection-reasons"))
        .await
        .unwrap();
    let store = PgCorpusStore::new(base.pool(), blobs);
    store.heartbeat_collection_scheduler().await.unwrap();
    let old_null = store.request_collection(request("001")).await.unwrap();
    let (id, _) = store.claim_collection_request().await.unwrap().unwrap();
    assert_eq!(id, old_null.request_id);
    store
        .settle_collection_request(&id, "skipped")
        .await
        .unwrap();
    let old_failure = store.request_collection(request("002")).await.unwrap();
    let (failure_id, _) = store.claim_collection_request().await.unwrap().unwrap();
    assert_eq!(failure_id, old_failure.request_id);
    store
        .settle_collection_request_with_reason(
            &failure_id,
            "failed",
            Some("source_inventory_incomplete"),
        )
        .await
        .unwrap();
    // Recreate the old CHECK in this isolated fixture, then apply the exact
    // forward migration to preexisting legacy receipts.
    sqlx::raw_sql(
        "ALTER TABLE openlegal.collection_request DROP CONSTRAINT collection_request_reason_check,
         ADD CONSTRAINT collection_request_reason_check CHECK (reason IN (
           'ambiguous', 'source_inventory_incomplete', 'not_found',
           'source_data_invalid', 'source_unavailable', 'download_failed',
           'identity_conflict', 'worker_failed', 'worker_lost'))",
    )
    .execute(&base.pool())
    .await
    .unwrap();
    sqlx::raw_sql(include_str!(
        "../migrations/0011_collection_skip_reasons.sql"
    ))
    .execute(&base.pool())
    .await
    .unwrap();
    assert!(store.collection_status(&id).await.unwrap().reason.is_none());
    assert_eq!(
        store
            .collection_status(&failure_id)
            .await
            .unwrap()
            .reason
            .as_deref(),
        Some("source_inventory_incomplete")
    );
    base.close().await.unwrap();
}
