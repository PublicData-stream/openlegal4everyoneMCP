//! Offline and real-PostgreSQL evidence for bounded collection operation policy.
#[path = "../../../test-support/postgres.rs"]
mod support;
use openlegal_adapters::{
    blob::FsBlobStore,
    corpus::{CollectionLaunch, PgCorpusStore},
};
use openlegal_application::{persistence::PersistentStore, upstream_policy::RequestLimit};
use openlegal_domain::{
    collection::{CollectionRequest, CollectionTarget},
    legal::{Dataset, ObjectId},
};
use std::collections::BTreeMap;

fn object(id: &str) -> ObjectId {
    ObjectId {
        jurisdiction: "kr".into(),
        provider: "law_go_kr".into(),
        dataset: Dataset::NationalStatute,
        id: id.into(),
    }
}
fn request(id: &str) -> CollectionRequest {
    CollectionRequest {
        target: CollectionTarget::Object { object: object(id) },
    }
}
fn metadata() -> BTreeMap<String, String> {
    BTreeMap::from([("collection_origin".into(), "explicit".into())])
}
#[test]
fn collection_operation_deadlines_keep_startup_and_recovery_margins() {
    for (seconds, deadline) in [(60, 360), (7200, 7500), (86400, 86700)] {
        let launch = CollectionLaunch {
            id: "fixture".into(),
            request: request("001"),
            timeout_secs: seconds,
            attempt_limit: RequestLimit::Unlimited,
            launched_at: 1000,
            observed_at: 1001,
        };
        assert_eq!(launch.job_deadline_secs(), deadline);
        assert_eq!(launch.recovery_at(), 1000 + seconds + 900);
        assert_eq!(launch.remaining_secs(1001), seconds - 1);
        assert_eq!(launch.remaining_secs(1000 + seconds), 0);
    }
}
#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn explicit_launch_policy_is_private_durable_and_keeps_its_original_epoch() {
    let fixture = support::TestDatabase::new().await;
    let base = fixture.open(100).await;
    let pool = base.pool();
    let blobs = FsBlobStore::open(&fixture.directory.path().join("launch-policy"))
        .await
        .unwrap();
    let store = PgCorpusStore::new(pool.clone(), blobs);
    store.heartbeat_collection_scheduler().await.unwrap();
    let receipt = store.request_collection(request("001")).await.unwrap();
    let launch = store
        .claim_collection_request_with_policy(86400, RequestLimit::Unlimited)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(launch.id, receipt.request_id);
    assert_eq!(launch.timeout_secs, 86400);
    assert_eq!(launch.attempt_limit, RequestLimit::Unlimited);
    let before: (i64, serde_json::Value) =
        sqlx::query_as("SELECT lease_until,payload FROM openlegal.collection_request WHERE id=$1")
            .bind(uuid::Uuid::parse_str(&launch.id).unwrap())
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(before.0 as u64, launch.recovery_at());
    assert_eq!(before.1, serde_json::to_value(request("001")).unwrap());
    sqlx::query("UPDATE openlegal.provider_request_budget SET on_demand_timeout_secs=60,on_demand_attempt_limit=1 WHERE singleton").execute(&pool).await.unwrap();
    store
        .mark_collection_running(&launch.id, "openlegal-request-fixture")
        .await
        .unwrap();
    let loaded = store.load_collection_launch(&launch.id).await.unwrap();
    assert_eq!(loaded.timeout_secs, 86400);
    assert_eq!(loaded.attempt_limit, RequestLimit::Unlimited);
    assert_eq!(loaded.launched_at, launch.launched_at);
    let lease: i64 =
        sqlx::query_scalar("SELECT lease_until FROM openlegal.collection_request WHERE id=$1")
            .bind(uuid::Uuid::parse_str(&launch.id).unwrap())
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(lease, before.0);
    sqlx::query("UPDATE openlegal.collection_request SET expires_at=floor(extract(epoch from clock_timestamp()))::bigint-1,created_at=floor(extract(epoch from clock_timestamp()))::bigint-86401 WHERE id=$1").bind(uuid::Uuid::parse_str(&launch.id).unwrap()).execute(&pool).await.unwrap();
    store.prune_collection_requests().await.unwrap();
    assert_eq!(
        store
            .request_collection(request("001"))
            .await
            .unwrap()
            .request_id,
        launch.id
    );
    assert!(store.load_collection_launch(&launch.id).await.is_ok());
    store
        .settle_collection_request(&launch.id, "done")
        .await
        .unwrap();
    store.heartbeat_collection_scheduler().await.unwrap();
    store.request_collection(request("002")).await.unwrap();
    let default = store.claim_collection_request().await.unwrap().unwrap();
    let default = store.load_collection_launch(&default.0).await.unwrap();
    assert_eq!(default.timeout_secs, 7200);
    assert_eq!(default.attempt_limit, RequestLimit::Limited(32));
    base.close().await.unwrap();
}
#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn original_explicit_owner_survives_joining_until_request_and_claim_fences_expire() {
    let fixture = support::TestDatabase::new().await;
    let base = fixture.open(100).await;
    let pool = base.pool();
    let blobs = FsBlobStore::open(&fixture.directory.path().join("operation-owner"))
        .await
        .unwrap();
    let store = PgCorpusStore::new(pool.clone(), blobs);
    store.heartbeat_collection_scheduler().await.unwrap();
    store.request_collection(request("001")).await.unwrap();
    let original = store
        .claim_collection_request_with_policy(86400, RequestLimit::Unlimited)
        .await
        .unwrap()
        .unwrap();
    let queued = store
        .enqueue_job_for_collection_request(
            object("001"),
            "r1".into(),
            None,
            true,
            false,
            original.observed_at,
            metadata(),
            Some(0),
            &original,
        )
        .await
        .unwrap();
    store.request_collection(request("002")).await.unwrap();
    let joining = store
        .claim_collection_request_with_policy(60, RequestLimit::Limited(1))
        .await
        .unwrap()
        .unwrap();
    let rejoined = store
        .enqueue_job_for_collection_request(
            object("001"),
            "r1".into(),
            None,
            true,
            false,
            joining.observed_at,
            metadata(),
            None,
            &joining,
        )
        .await
        .unwrap();
    assert_eq!(queued.id, rejoined.id);
    assert!(
        !store
            .adopt_explicit_job(
                &queued.id,
                &joining.id,
                joining.recovery_at(),
                joining.observed_at
            )
            .await
            .unwrap()
    );
    let owner: (uuid::Uuid, i64) = sqlx::query_as(
        "SELECT explicit_request_id,explicit_recovery_at FROM openlegal.corpus_job WHERE id=$1",
    )
    .bind(uuid::Uuid::parse_str(&queued.id).unwrap())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(owner.0.to_string(), original.id);
    assert_eq!(owner.1 as u64, original.recovery_at());
    assert!(
        store
            .claim_explicit_job(&queued.id, original.observed_at, 600)
            .await
            .unwrap()
            .is_none()
    );
    let active = store
        .claim_explicit_job_for_request(&queued.id, &original.id, original.observed_at, 600)
        .await
        .unwrap()
        .unwrap();
    store
        .settle_collection_request(&original.id, "failed")
        .await
        .unwrap();
    assert!(
        !store
            .adopt_explicit_job(
                &queued.id,
                &joining.id,
                joining.recovery_at(),
                original.observed_at + 1
            )
            .await
            .unwrap()
    );
    // End the second request too; recovery still respects the live detail claim.
    store
        .settle_collection_request(&joining.id, "done")
        .await
        .unwrap();
    store
        .requeue_due_details(original.observed_at + 1)
        .await
        .unwrap();
    assert!(
        store
            .claim_job(original.observed_at + 1)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .claim_explicit_job(&queued.id, original.observed_at + 601, 600)
            .await
            .unwrap()
            .is_none()
    );
    store
        .requeue_due_details(original.observed_at + 601)
        .await
        .unwrap();
    assert!(
        store
            .claim_job(original.observed_at + 602)
            .await
            .unwrap()
            .is_none()
    );
    store
        .requeue_due_details(original.recovery_at())
        .await
        .unwrap();
    let recovered = store
        .claim_job(original.recovery_at() + 1)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(recovered.id, active.id);
    assert_eq!(recovered.attempts, 2);
    base.close().await.unwrap();
}
#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn lower_retry_limit_exhausts_pending_work_without_truncating_active_claims_or_reviving_terminal_work()
 {
    let fixture = support::TestDatabase::new().await;
    let base = fixture.open(100).await;
    let pool = base.pool();
    let blobs = FsBlobStore::open(&fixture.directory.path().join("retry-policy"))
        .await
        .unwrap();
    let store = PgCorpusStore::new(pool.clone(), blobs);
    let first = store
        .enqueue_job(object("001"), "r1".into(), None, true, true, 100)
        .await
        .unwrap();
    let second = store
        .enqueue_job(object("002"), "r1".into(), None, true, true, 101)
        .await
        .unwrap();
    let active = store.claim_job_with_lease(102, 600).await.unwrap().unwrap();
    assert_eq!(active.id, first.id);
    sqlx::query("UPDATE openlegal.corpus_job SET attempts=2 WHERE id=$1")
        .bind(uuid::Uuid::parse_str(&second.id).unwrap())
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE openlegal.provider_request_budget SET max_job_attempts=1 WHERE singleton")
        .execute(&pool)
        .await
        .unwrap();
    assert!(store.claim_job(103).await.unwrap().is_none());
    let statuses: Vec<String> =
        sqlx::query_scalar("SELECT status FROM openlegal.corpus_job ORDER BY created_at")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(statuses, vec!["running", "failed"]);
    store.fail_claim(&active, true).await.unwrap();
    sqlx::query("UPDATE openlegal.provider_request_budget SET max_job_attempts=10 WHERE singleton")
        .execute(&pool)
        .await
        .unwrap();
    assert!(store.claim_job(104).await.unwrap().is_none());
    let terminal: i64 =
        sqlx::query_scalar("SELECT count(*) FROM openlegal.corpus_job WHERE status='failed'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(terminal, 2);
    base.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn explicit_terminal_gap_retry_waits_original_window_then_starts_fresh_unowned_cycle() {
    let fixture = support::TestDatabase::new().await;
    let base = fixture.open(100).await;
    let pool = base.pool();
    let blobs = FsBlobStore::open(&fixture.directory.path().join("owned-gap"))
        .await
        .unwrap();
    let store = PgCorpusStore::new(pool, blobs);
    store.heartbeat_collection_scheduler().await.unwrap();
    store.request_collection(request("001")).await.unwrap();
    let launch = store
        .claim_collection_request_with_policy(86400, RequestLimit::Unlimited)
        .await
        .unwrap()
        .unwrap();
    let queued = store
        .enqueue_job_for_collection_request(
            object("001"),
            "r1".into(),
            None,
            true,
            false,
            launch.observed_at,
            metadata(),
            Some(0),
            &launch,
        )
        .await
        .unwrap();
    let active = store
        .claim_explicit_job_for_request(&queued.id, &launch.id, launch.observed_at, 600)
        .await
        .unwrap()
        .unwrap();
    store
        .skip_claim(&active, "download_failed", launch.observed_at)
        .await
        .unwrap();
    store
        .settle_collection_request(&launch.id, "done")
        .await
        .unwrap();
    assert_eq!(
        store
            .requeue_due_details(launch.observed_at + 3601)
            .await
            .unwrap(),
        0
    );
    assert!(
        store
            .claim_job(launch.observed_at + 3602)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        store
            .requeue_due_details(launch.recovery_at())
            .await
            .unwrap(),
        1
    );
    let recovered = store
        .claim_job(launch.recovery_at() + 1)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(recovered.id, active.id);
    assert_eq!(recovered.attempts, 1);
    assert!(!recovered.source_metadata.contains_key("collection_origin"));
    base.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn gap_retry_skips_a_locked_terminal_job_and_preserves_concurrent_new_owner() {
    let fixture = support::TestDatabase::new().await;
    let base = fixture.open(100).await;
    let pool = base.pool();
    let blobs = FsBlobStore::open(&fixture.directory.path().join("gap-owner-race"))
        .await
        .unwrap();
    let store = PgCorpusStore::new(pool.clone(), blobs);
    store.heartbeat_collection_scheduler().await.unwrap();
    store.request_collection(request("001")).await.unwrap();
    let launch = store
        .claim_collection_request_with_policy(86400, RequestLimit::Unlimited)
        .await
        .unwrap()
        .unwrap();
    let queued = store
        .enqueue_job(
            object("001"),
            "r1".into(),
            None,
            true,
            false,
            launch.observed_at,
        )
        .await
        .unwrap();
    let active = store.claim_job(launch.observed_at).await.unwrap().unwrap();
    store
        .skip_claim(&active, "download_failed", launch.observed_at)
        .await
        .unwrap();
    let job_id = uuid::Uuid::parse_str(&queued.id).unwrap();
    let owner_id = uuid::Uuid::parse_str(&launch.id).unwrap();
    // Hold the job row while its fresh ownership is uncommitted. A concurrent
    // retry sees the old failed row, but must not wait and overwrite the new one.
    let mut reassignment = pool.begin().await.unwrap();
    sqlx::query("SELECT id FROM openlegal.corpus_job WHERE id=$1 FOR UPDATE")
        .bind(job_id)
        .fetch_one(&mut *reassignment)
        .await
        .unwrap();
    sqlx::query("UPDATE openlegal.corpus_job SET status='pending',attempts=0,explicit_request_id=$2,explicit_recovery_at=$3,source_metadata=jsonb_set(source_metadata,'{collection_origin}','\"explicit\"'::jsonb) WHERE id=$1")
        .bind(job_id).bind(owner_id).bind(launch.recovery_at() as i64).execute(&mut *reassignment).await.unwrap();
    let retry = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        store.requeue_due_details(launch.observed_at + 3601),
    )
    .await;
    reassignment.commit().await.unwrap();
    assert_eq!(
        retry
            .expect("busy job rows must be skipped without waiting")
            .unwrap(),
        0
    );
    let row: (String, uuid::Uuid, i64) = sqlx::query_as("SELECT status,explicit_request_id,explicit_recovery_at FROM openlegal.corpus_job WHERE id=$1").bind(job_id).fetch_one(&pool).await.unwrap();
    assert_eq!(
        row,
        ("pending".into(), owner_id, launch.recovery_at() as i64)
    );
    assert!(
        store
            .claim_job(launch.observed_at + 3602)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        store
            .claim_explicit_job_for_request(&queued.id, &launch.id, launch.observed_at + 3602, 600)
            .await
            .unwrap()
            .unwrap()
            .id,
        queued.id
    );
    base.close().await.unwrap();
}
