//! PostgreSQL rejection evidence for scheduler retry classification.
#[path = "../../../test-support/postgres.rs"]
mod support;

use openlegal_adapters::{blob::FsBlobStore, corpus::PgCorpusStore};
use openlegal_application::persistence::PersistentStore;
use openlegal_domain::{
    collection::{CollectionRequest, CollectionTarget},
    legal::{DatabaseError, Dataset, ObjectId},
};
use sqlx::postgres::PgPoolOptions;

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn rejected_heartbeat_and_claim_preserve_queue_and_recover_after_contention() {
    let fixture = support::TestDatabase::new().await;
    let base = fixture.open(100).await;
    let pool = base.pool();
    let scheduler_pool = PgPoolOptions::new()
        .max_connections(1)
        .after_connect(|connection, _| {
            Box::pin(async move {
                sqlx::query("SET lock_timeout='100ms'")
                    .execute(&mut *connection)
                    .await?;
                sqlx::query("SET statement_timeout='2s'")
                    .execute(connection)
                    .await?;
                Ok(())
            })
        })
        .connect_with((*pool.connect_options()).clone())
        .await
        .unwrap();
    let blobs = FsBlobStore::open(&fixture.directory.path().join("scheduler-contention"))
        .await
        .unwrap();
    let store = PgCorpusStore::new(scheduler_pool.clone(), blobs);
    store.heartbeat_collection_scheduler().await.unwrap();
    let receipt = store
        .request_collection(CollectionRequest {
            target: CollectionTarget::Object {
                object: ObjectId {
                    jurisdiction: "kr".into(),
                    provider: "law_go_kr".into(),
                    dataset: Dataset::NationalStatute,
                    id: "001".into(),
                },
            },
        })
        .await
        .unwrap();

    let mut owner = pool.begin().await.unwrap();
    sqlx::query("SELECT singleton FROM openlegal.corpus_control WHERE singleton FOR UPDATE")
        .fetch_one(&mut *owner)
        .await
        .unwrap();
    assert_eq!(
        store.heartbeat_collection_scheduler().await,
        Err(DatabaseError::StorageContended)
    );
    assert!(matches!(
        store.claim_collection_request().await,
        Err(DatabaseError::StorageContended)
    ));
    assert_eq!(
        store
            .collection_status(&receipt.request_id)
            .await
            .unwrap()
            .status,
        "queued",
        "a rejected claim cannot launch or consume the queued request"
    );

    // With lock timeout disabled the same lock causes PostgreSQL's statement
    // cancellation response (57014), rather than an uncertain client timeout.
    sqlx::query("SET lock_timeout=0")
        .execute(&scheduler_pool)
        .await
        .unwrap();
    sqlx::query("SET statement_timeout='100ms'")
        .execute(&scheduler_pool)
        .await
        .unwrap();
    assert_eq!(
        store.heartbeat_collection_scheduler().await,
        Err(DatabaseError::StorageContended)
    );
    owner.rollback().await.unwrap();
    sqlx::query("SET statement_timeout='2s'")
        .execute(&scheduler_pool)
        .await
        .unwrap();
    store.heartbeat_collection_scheduler().await.unwrap();
    let (claimed, _) = store.claim_collection_request().await.unwrap().unwrap();
    assert_eq!(claimed, receipt.request_id);
    assert!(store.claim_collection_request().await.unwrap().is_none());

    // A closed pool supplies no server rejection and must remain fail-closed.
    scheduler_pool.close().await;
    assert_eq!(
        store.heartbeat_collection_scheduler().await,
        Err(DatabaseError::StorageUnavailable)
    );
    base.close().await.unwrap();
}
