//! PostgreSQL corpus contracts, using exclusively fictional legal-shaped records.
#[path = "../../../test-support/postgres.rs"]
mod support;
use openlegal_adapters::{
    blob::FsBlobStore,
    corpus::{PageGapObservation, PgCorpusStore},
    law_go_kr::{LawClient, ProviderRequestLimits, RequestBudgetMode},
};
use openlegal_application::{
    citation::CitationLease,
    database::{DatabaseStore, Publication},
    document::{DocumentError, DocumentInput, DocumentOutput, DocumentProcessor},
    persistence::PersistentStore,
};
use openlegal_domain::collection::{CollectionRequest, CollectionTarget};
use openlegal_domain::legal::*;
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, sync::Arc};
use tokio_util::sync::CancellationToken;

struct UnusedDocumentProcessor;
impl DocumentProcessor for UnusedDocumentProcessor {
    fn process(
        &self,
        _input: DocumentInput,
        _cancellation: CancellationToken,
    ) -> futures::future::BoxFuture<'static, Result<DocumentOutput, DocumentError>> {
        Box::pin(async { Err(DocumentError::InvalidInput) })
    }
}

struct FixtureClock;
impl openlegal_application::Clock for FixtureClock {
    fn now(&self) -> u64 {
        0
    }
}
fn token() -> CancellationToken {
    CancellationToken::new()
}
#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn explicit_collection_requests_coalesce_and_clear_completed_payloads() {
    let fixture = support::TestDatabase::new().await;
    let base = fixture.open(100).await;
    let blobs = FsBlobStore::open(&fixture.directory.path().join("explicit-requests"))
        .await
        .unwrap();
    let store = PgCorpusStore::new(base.pool(), blobs);
    let mut requested = object();
    requested.provider = "law_go_kr".into();
    let request = CollectionRequest {
        target: CollectionTarget::Object { object: requested },
    };
    assert_eq!(
        store.request_collection(request.clone()).await.err(),
        Some(DatabaseError::Capacity)
    );
    store.heartbeat_collection_scheduler().await.unwrap();
    sqlx::query("UPDATE openlegal.corpus_control SET collection_scheduler_seen_at=floor(extract(epoch from clock_timestamp()))::bigint-31 WHERE singleton")
        .execute(&base.pool()).await.unwrap();
    assert_eq!(
        store.request_collection(request.clone()).await.err(),
        Some(DatabaseError::Capacity)
    );
    store.heartbeat_collection_scheduler().await.unwrap();
    let first = store.request_collection(request.clone()).await.unwrap();
    let second = store.request_collection(request).await.unwrap();
    assert_eq!(first.request_id, second.request_id);
    assert_eq!(first.status, "queued");
    let (id, claimed) = store.claim_collection_request().await.unwrap().unwrap();
    assert_eq!(id, first.request_id);
    claimed.validate().unwrap();
    store
        .mark_collection_running(&id, "openlegal-request-test")
        .await
        .unwrap();
    assert!(store.load_collection_request(&id).await.is_ok());
    assert_eq!(
        store.unsettled_collection_jobs().await.unwrap(),
        vec![(id.clone(), Some("openlegal-request-test".into()))]
    );
    store
        .fail_finished_collection_job(&id, "openlegal-request-other")
        .await
        .unwrap();
    assert_eq!(
        store.collection_status(&id).await.unwrap().status,
        "running"
    );
    store
        .settle_collection_request_with_reason(&id, "deferred", Some("source_inventory_incomplete"))
        .await
        .unwrap();
    assert_eq!(
        store
            .collection_status(&id)
            .await
            .unwrap()
            .reason
            .as_deref(),
        Some("source_inventory_incomplete")
    );
    assert_eq!(
        store.request_collection(claimed).await.unwrap().status,
        "deferred"
    );
    sqlx::query("UPDATE openlegal.collection_request SET lease_until=floor(extract(epoch from clock_timestamp()))::bigint-1 WHERE id=$1")
        .bind(uuid::Uuid::parse_str(&id).unwrap()).execute(&base.pool()).await.unwrap();
    let (reclaimed, _) = store.claim_collection_request().await.unwrap().unwrap();
    assert_eq!(reclaimed, id);
    store
        .mark_collection_running(&id, "openlegal-request-test")
        .await
        .unwrap();
    store.settle_collection_request(&id, "done").await.unwrap();
    assert_eq!(store.collection_status(&id).await.unwrap().status, "done");
    assert!(store.collection_status(&id).await.unwrap().reason.is_none());
    assert!(store.load_collection_request(&id).await.is_err());
    assert!(store.unsettled_collection_jobs().await.unwrap().is_empty());
    base.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn terminal_failed_job_clears_request_without_touching_provider_budget() {
    let fixture = support::TestDatabase::new().await;
    let base = fixture.open(100).await;
    let blobs = FsBlobStore::open(&fixture.directory.path().join("failed-request"))
        .await
        .unwrap();
    let store = PgCorpusStore::new(base.pool(), blobs);
    store.heartbeat_collection_scheduler().await.unwrap();
    let mut requested = object();
    requested.provider = "law_go_kr".into();
    let request = CollectionRequest {
        target: CollectionTarget::Object { object: requested },
    };
    let receipt = store.request_collection(request.clone()).await.unwrap();
    let (id, _) = store.claim_collection_request().await.unwrap().unwrap();
    assert_eq!(id, receipt.request_id);
    store
        .mark_collection_running(&id, "openlegal-request-failed")
        .await
        .unwrap();
    store
        .fail_finished_collection_job(&id, "openlegal-request-failed")
        .await
        .unwrap();
    let failed = store.collection_status(&id).await.unwrap();
    assert_eq!(failed.status, "failed");
    assert_eq!(failed.reason.as_deref(), Some("worker_failed"));
    assert!(store.unsettled_collection_jobs().await.unwrap().is_empty());
    assert!(store.load_collection_request(&id).await.is_err());
    store
        .fail_finished_collection_job(&id, "openlegal-request-failed")
        .await
        .unwrap();
    let mut another = object();
    another.provider = "law_go_kr".into();
    another.id = "002".into();
    let launch = store
        .request_collection(CollectionRequest {
            target: CollectionTarget::Object { object: another },
        })
        .await
        .unwrap();
    let (launch_id, _) = store.claim_collection_request().await.unwrap().unwrap();
    assert_eq!(launch_id, launch.request_id);
    assert_eq!(
        store.unsettled_collection_jobs().await.unwrap(),
        vec![(launch_id.clone(), None)]
    );
    store
        .fail_finished_collection_job(&launch_id, "openlegal-request-any")
        .await
        .unwrap();
    assert_eq!(
        store.collection_status(&launch_id).await.unwrap().status,
        "failed"
    );
    let coalesced = store.request_collection(request.clone()).await.unwrap();
    assert_eq!(coalesced.request_id, id);
    sqlx::query("UPDATE openlegal.collection_request SET created_at=created_at-3601 WHERE id=$1")
        .bind(uuid::Uuid::parse_str(&id).unwrap())
        .execute(&base.pool())
        .await
        .unwrap();
    let retried = store.request_collection(request).await.unwrap();
    assert_ne!(retried.request_id, id);
    assert_eq!(retried.status, "queued");
    assert!(retried.reason.is_none());
    let original = store.collection_status(&id).await.unwrap();
    assert_eq!(original.status, "failed");
    assert_eq!(original.reason.as_deref(), Some("worker_failed"));
    base.close().await.unwrap();
}
#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn explicit_detail_job_is_fenced_and_incomplete_capture_retries_without_freshness_claim() {
    let fixture = support::TestDatabase::new().await;
    let base = fixture.open(100).await;
    let blobs = FsBlobStore::open(&fixture.directory.path().join("explicit-detail"))
        .await
        .unwrap();
    let store = PgCorpusStore::with_publication_clock(base.pool(), blobs, Arc::new(FixtureClock));
    let mut metadata = BTreeMap::new();
    metadata.insert("collection_origin".into(), "explicit".into());
    metadata.insert("title".into(), "Fictional statute".into());
    let queued = store
        .enqueue_job_with_metadata_fenced(
            object(),
            "r1".into(),
            None,
            true,
            false,
            100,
            metadata.clone(),
            Some(0),
        )
        .await
        .unwrap();
    assert_eq!(
        store
            .enqueue_job_with_metadata_fenced(
                object(),
                "r1".into(),
                None,
                true,
                false,
                101,
                metadata,
                Some(0)
            )
            .await
            .err(),
        Some(DatabaseError::Conflict)
    );
    assert!(store.claim_job(101).await.unwrap().is_none());
    let claimed = store
        .claim_explicit_job(&queued.id, 101, 3720)
        .await
        .unwrap()
        .unwrap();
    let mut incomplete = record("r1", "partial body");
    incomplete
        .metadata
        .insert("attachment_status".into(), "incomplete".into());
    let first = store
        .publish(
            Publication {
                record: incomplete.clone(),
                raw: b"partial body".to_vec(),
                additional_evidence: vec![],
                processor_version: "fixture_v1".into(),
                retrieved_at: 102,
                now: 102,
                expected_version: claimed.expected_version,
                install_head: true,
                job_id: Some(claimed.id),
            },
            token(),
        )
        .await
        .unwrap();
    assert!(store.detail_gap_active(&object(), "r1").await.unwrap());
    assert_eq!(store.requeue_due_details(3702).await.unwrap(), 0);
    assert_eq!(store.requeue_due_details(8200).await.unwrap(), 1);
    let retry = store.claim_job(8201).await.unwrap().unwrap();
    let same = store
        .publish(
            Publication {
                record: incomplete,
                raw: b"partial body".to_vec(),
                additional_evidence: vec![],
                processor_version: "fixture_v1".into(),
                retrieved_at: 8201,
                now: 8201,
                expected_version: retry.expected_version,
                install_head: true,
                job_id: Some(retry.id),
            },
            token(),
        )
        .await
        .unwrap();
    assert_eq!(same.capture_id, first.capture_id);
    assert_eq!(same.validated_at, first.validated_at);
    let head_validated_at: String = sqlx::query_scalar("SELECT validated_at::text FROM openlegal.corpus_object WHERE object_key=(SELECT object_key FROM openlegal.corpus_capture WHERE id=$1)")
        .bind(&first.capture_id).fetch_one(&base.pool()).await.unwrap();
    assert_eq!(head_validated_at, "102");
    base.close().await.unwrap();
}
#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn collection_gaps_are_durable_bounded_notices_and_retries() {
    let fixture = support::TestDatabase::new().await;
    let base = fixture.open(100).await;
    let blobs = FsBlobStore::open(&fixture.directory.path().join("collection-gaps"))
        .await
        .unwrap();
    let store = PgCorpusStore::with_publication_clock(base.pool(), blobs, Arc::new(FixtureClock));
    store
        .record_page_gap(
            Dataset::Treaty,
            false,
            Some(1),
            1,
            PageGapObservation {
                reason: "source_data_invalid",
                rows: 2,
                now: 100,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        store
            .due_gap_page(Dataset::Treaty, false, Some(1), 100)
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        store
            .due_gap_page(Dataset::Treaty, false, Some(1), 3700)
            .await
            .unwrap(),
        Some(1)
    );
    let notices = store
        .collection_notices(&[Dataset::Treaty], None)
        .await
        .unwrap();
    assert_eq!(notices.len(), 1);
    assert_eq!(notices[0].affected_count, 2);
    store
        .resolve_page_gap(Dataset::Treaty, false, Some(1), 1, 3701)
        .await
        .unwrap();
    assert!(
        store
            .collection_notices(&[Dataset::Treaty], None)
            .await
            .unwrap()
            .is_empty()
    );
    store
        .enqueue_job(object(), "r1".into(), None, true, true, 4000)
        .await
        .unwrap();
    let job = store.claim_job(4001).await.unwrap().unwrap();
    store
        .skip_claim(&job, "download_failed", 4002)
        .await
        .unwrap();
    let notices = store
        .collection_notices(&[Dataset::NationalStatute], Some(&job.object))
        .await
        .unwrap();
    assert_eq!(notices.len(), 1);
    assert_eq!(notices[0].code, "download_failed");
    assert!(
        store
            .detail_gap_active(&job.object, &job.revision_id)
            .await
            .unwrap()
    );
    assert_eq!(store.requeue_due_details(7602).await.unwrap(), 1);
    let retried = store.claim_job(7603).await.unwrap().unwrap();
    assert_eq!(retried.revision_id, "r1");
    store
        .resolve_detail_gap(&job.object, &job.revision_id, 7604)
        .await
        .unwrap();
    assert!(
        !store
            .detail_gap_active(&job.object, &job.revision_id)
            .await
            .unwrap()
    );
    base.close().await.unwrap();
}
#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn historical_revalidation_deduplicates_bytes_but_captures_corrections() {
    let fixture = support::TestDatabase::new().await;
    let base = fixture.open(100).await;
    let blobs = FsBlobStore::open(&fixture.directory.path().join("historical-revalidation"))
        .await
        .unwrap();
    let store = PgCorpusStore::with_publication_clock(
        base.pool(),
        blobs,
        std::sync::Arc::new(FixtureClock),
    );
    let publish_history = |body: &'static str, now: u64| {
        let store = store.clone();
        async move {
            let state = store.state(&object()).await.unwrap();
            store
                .publish(
                    Publication {
                        additional_evidence: Vec::new(),
                        record: record("r1", body),
                        raw: body.as_bytes().to_vec(),
                        processor_version: "fixture_v1".into(),
                        retrieved_at: now,
                        now,
                        expected_version: state.version,
                        install_head: false,
                        job_id: None,
                    },
                    token(),
                )
                .await
                .unwrap()
        }
    };
    let first = publish_history("original", 100).await;
    let watermark = store.watermark().await.unwrap();
    let unchanged = publish_history("original", 3700).await;
    assert_eq!(unchanged.capture_id, first.capture_id);
    assert_eq!(store.watermark().await.unwrap(), watermark);
    assert!(
        store
            .revision_capture_recent(&object(), "r1", 3700)
            .await
            .unwrap()
    );
    let revision = store
        .resolve(
            object(),
            RevisionSelector::Revision { id: "r1".into() },
            3700,
            token(),
        )
        .await
        .unwrap();
    assert_eq!(revision.validated_at, 3700);
    let original_capture = store
        .resolve(
            object(),
            RevisionSelector::Capture {
                id: first.capture_id.clone(),
            },
            3700,
            token(),
        )
        .await
        .unwrap();
    assert_eq!(original_capture.validated_at, 100);
    let corrected = publish_history("corrected", 7300).await;
    assert_ne!(corrected.capture_id, first.capture_id);
    assert_eq!(store.watermark().await.unwrap(), watermark + 1);
    base.close().await.unwrap();
}
#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn provider_budget_and_inventory_cursor_are_durable() {
    let fixture = support::TestDatabase::new().await;
    let base = fixture.open(100).await;
    let pool = base.pool();
    let mode = RequestBudgetMode::Pilot;
    LawClient::reserve_provider_request_budget(&pool, &mode, &token())
        .await
        .unwrap();
    let counts: (i64, i64) = sqlx::query_as(
        "SELECT daily_used,pilot_used FROM openlegal.provider_request_budget WHERE singleton",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(counts, (1, 1));
    assert_eq!(
        LawClient::reserve_provider_request_budget(&pool, &mode, &token()).await,
        Err(DatabaseError::BudgetExhausted)
    );
    sqlx::query("UPDATE openlegal.provider_request_budget SET unresolved_response=false,next_allowed_at=floor(extract(epoch from clock_timestamp()))::bigint+120")
        .execute(&pool).await.unwrap();
    assert_eq!(
        LawClient::reserve_provider_request_budget(&pool, &mode, &token()).await,
        Err(DatabaseError::BudgetExhausted)
    );
    let paused: (i64, i64) = sqlx::query_as(
        "SELECT daily_used,pilot_used FROM openlegal.provider_request_budget WHERE singleton",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(paused, (1, 1));
    sqlx::query(
        "UPDATE openlegal.provider_request_budget SET operator_suspended=true,next_allowed_at=0",
    )
    .execute(&pool)
    .await
    .unwrap();
    assert_eq!(
        LawClient::reserve_provider_request_budget(&pool, &mode, &token()).await,
        Err(DatabaseError::BudgetExhausted)
    );
    sqlx::query("UPDATE openlegal.provider_request_budget SET operator_suspended=false,unresolved_response=false,daily_used=999,pilot_used=99,next_allowed_at=0")
        .execute(&pool).await.unwrap();
    LawClient::reserve_provider_request_budget(&pool, &mode, &token())
        .await
        .unwrap();
    sqlx::query("UPDATE openlegal.provider_request_budget SET unresolved_response=false")
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(
        LawClient::reserve_provider_request_budget(&pool, &mode, &token()).await,
        Err(DatabaseError::BudgetExhausted)
    );
    let counts: (i64, i64) = sqlx::query_as(
        "SELECT daily_used,pilot_used FROM openlegal.provider_request_budget WHERE singleton",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(counts, (1000, 100));
    sqlx::query("UPDATE openlegal.provider_request_budget SET on_demand_used=999,unresolved_response=false,next_allowed_at=0")
        .execute(&pool).await.unwrap();
    LawClient::reserve_provider_request_budget(&pool, &RequestBudgetMode::OnDemand, &token())
        .await
        .unwrap();
    let counts: (i64, i64) = sqlx::query_as(
        "SELECT daily_used,on_demand_used FROM openlegal.provider_request_budget WHERE singleton",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(counts, (1000, 1000));
    sqlx::query(
        "UPDATE openlegal.provider_request_budget SET unresolved_response=false,next_allowed_at=0",
    )
    .execute(&pool)
    .await
    .unwrap();
    assert_eq!(
        LawClient::reserve_provider_request_budget(&pool, &RequestBudgetMode::OnDemand, &token())
            .await,
        Err(DatabaseError::BudgetExhausted)
    );
    let blobs = FsBlobStore::open(&fixture.directory.path().join("budget-corpus"))
        .await
        .unwrap();
    let store = PgCorpusStore::with_publication_clock(
        pool.clone(),
        blobs,
        std::sync::Arc::new(FixtureClock),
    );
    assert_eq!(
        store
            .inventory_cursor(Dataset::Treaty, false)
            .await
            .unwrap(),
        1
    );
    store
        .set_inventory_item_offset(Dataset::Treaty, false, 1, 16)
        .await
        .unwrap();
    assert_eq!(
        store
            .inventory_item_offset(Dataset::Treaty, false, 1)
            .await
            .unwrap(),
        16
    );
    store
        .advance_inventory_cursor(Dataset::Treaty, false, 1, false)
        .await
        .unwrap();
    assert_eq!(
        store
            .inventory_item_offset(Dataset::Treaty, false, 2)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        store
            .inventory_cursor(Dataset::Treaty, false)
            .await
            .unwrap(),
        2
    );
    assert_eq!(
        store.inventory_cursor(Dataset::Treaty, true).await.unwrap(),
        1
    );
    assert_eq!(
        store
            .advance_inventory_cursor(Dataset::Treaty, false, 1, false)
            .await,
        Err(DatabaseError::Conflict)
    );
    store
        .advance_inventory_cursor(Dataset::Treaty, false, 2, true)
        .await
        .unwrap();
    assert_eq!(
        store
            .inventory_cursor(Dataset::Treaty, false)
            .await
            .unwrap(),
        1
    );
    publish(&store, "r1", "synthetic body", 100).await;
    store
        .enqueue_job(object(), "r1".into(), None, true, false, 100)
        .await
        .unwrap();
    assert!(
        store
            .active_detail_job(&object(), "r1", true)
            .await
            .unwrap()
    );
    assert!(
        store
            .head_revision_ready(&object(), "r1", 100)
            .await
            .unwrap()
    );
    assert!(
        !store
            .head_revision_ready(&object(), "r2", 100)
            .await
            .unwrap()
    );
    assert!(
        !store
            .head_revision_ready(&object(), "r1", 3701)
            .await
            .unwrap()
    );
    assert!(
        store
            .head_revision_published(&object(), "r1")
            .await
            .unwrap()
    );
    assert!(
        store
            .revision_capture_recent(&object(), "r1", 100)
            .await
            .unwrap()
    );
    assert!(
        !store
            .revision_capture_recent(&object(), "r1", 3701)
            .await
            .unwrap()
    );
    let old_head_job = store.claim_job(101).await.unwrap().unwrap();
    store.fail_claim(&old_head_job, false).await.unwrap();
    assert!(
        !store
            .active_detail_job(&object(), "r1", true)
            .await
            .unwrap()
    );
    let mut manual = BTreeMap::new();
    manual.insert("title".into(), "untrusted manual title".into());
    store
        .enqueue_job_with_metadata(object(), "r2".into(), None, false, false, 200, manual)
        .await
        .unwrap();
    let old_manual_job = store.claim_job(201).await.unwrap().unwrap();
    let mut live = BTreeMap::new();
    live.insert("title".into(), "fresh list title".into());
    store
        .enqueue_job_with_metadata(object(), "r2".into(), None, true, true, 202, live.clone())
        .await
        .unwrap();
    let promoted = store.claim_job(203).await.unwrap().unwrap();
    assert!(promoted.install_head);
    assert_eq!(promoted.source_metadata, live);
    assert_eq!(
        store.defer_budget_claim(&old_manual_job, 1000).await,
        Err(DatabaseError::Conflict)
    );
    store.fail_claim(&promoted, false).await.unwrap();
    store
        .enqueue_job(object(), "budget-sample".into(), None, true, true, 100)
        .await
        .unwrap();
    let first = store.claim_job(101).await.unwrap().unwrap();
    store.defer_budget_claim(&first, 1000).await.unwrap();
    assert!(store.claim_job(999).await.unwrap().is_none());
    let resumed = store.claim_job(1000).await.unwrap().unwrap();
    assert_eq!(resumed.attempts, 1);
    store.fail_claim(&resumed, false).await.unwrap();
    base.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn provider_policy_migration_preserves_legacy_attempts_and_extreme_pauses() {
    let fixture = support::TestDatabase::new().await;
    let pool = sqlx::PgPool::connect(&fixture.url).await.unwrap();
    // Recreate the version-11 budget shape in this disposable migrated database.
    sqlx::raw_sql("DROP TABLE openlegal.upstream_daily_budget; ALTER TABLE openlegal.collection_request DROP COLUMN operation_timeout_secs,DROP COLUMN operation_attempt_limit,DROP COLUMN launched_at; ALTER TABLE openlegal.corpus_job DROP COLUMN explicit_request_id,DROP COLUMN explicit_recovery_at,DROP CONSTRAINT corpus_job_attempts_check,ADD CONSTRAINT corpus_job_attempts_check CHECK(attempts BETWEEN 0 AND 3); ALTER TABLE openlegal.provider_request_budget DROP COLUMN pilot_attempt_limit,DROP COLUMN on_demand_attempt_limit,DROP COLUMN interval_ms,DROP COLUMN pilot_timeout_secs,DROP COLUMN on_demand_timeout_secs,DROP COLUMN pilot_duration_secs,DROP COLUMN max_job_attempts,DROP CONSTRAINT provider_request_budget_pilot_used_check,ALTER COLUMN daily_used TYPE integer,ALTER COLUMN on_demand_used TYPE integer,ALTER COLUMN pilot_used TYPE integer,ADD CONSTRAINT provider_request_budget_pilot_used_check CHECK(pilot_used BETWEEN 0 AND 100); ALTER TABLE openlegal.provider_request_budget DROP CONSTRAINT provider_request_budget_daily_used_check, DROP CONSTRAINT provider_request_budget_on_demand_used_check, DROP COLUMN continuous_daily_limit, DROP COLUMN on_demand_daily_limit, DROP COLUMN min_interval_secs, DROP COLUMN next_request_at_ms, ADD CONSTRAINT provider_request_budget_daily_used_check CHECK(daily_used BETWEEN 0 AND 1000), ADD CONSTRAINT provider_request_budget_on_demand_used_check CHECK(on_demand_used BETWEEN 0 AND 1000); UPDATE openlegal.provider_request_budget SET daily_used=1000,on_demand_used=9,pilot_used=78,operator_suspended=true,unresolved_response=true,next_allowed_at=922337203685477580; DELETE FROM public._sqlx_migrations WHERE version IN (12,13);")
        .execute(&pool).await.unwrap();
    // Unrelated later migrations remain installed; compare the same legacy
    // versions on both sides of the provider-budget upgrade.
    let checksums: Vec<(i64, Vec<u8>)> = sqlx::query_as(
        "SELECT version,checksum FROM public._sqlx_migrations WHERE version<=11 ORDER BY version",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    openlegal_adapters::postgres::PostgresStore::migrate(&fixture.url, support::options())
        .await
        .unwrap();
    let after: Vec<(i64, Vec<u8>)> = sqlx::query_as(
        "SELECT version,checksum FROM public._sqlx_migrations WHERE version<=11 ORDER BY version",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(checksums, after);
    let ledger: (i64,i64,i64,bool,bool,i64,i64,i32,i32,i32) = sqlx::query_as("SELECT daily_used,on_demand_used,pilot_used,operator_suspended,unresolved_response,next_allowed_at,next_request_at_ms,continuous_daily_limit,on_demand_daily_limit,min_interval_secs FROM openlegal.provider_request_budget WHERE singleton").fetch_one(&pool).await.unwrap();
    assert_eq!(
        ledger,
        (
            1000,
            9,
            78,
            true,
            true,
            922337203685477580,
            9223372036854775000,
            1000,
            1000,
            5
        )
    );
    let limits = ProviderRequestLimits::new(50_000, 1000, 1).unwrap();
    LawClient::configure_provider_request_limits(&pool, &limits)
        .await
        .unwrap();
    sqlx::query("UPDATE openlegal.provider_request_budget SET operator_suspended=false,unresolved_response=false").execute(&pool).await.unwrap();
    assert_eq!(
        LawClient::reserve_provider_request_budget(&pool, &RequestBudgetMode::Continuous, &token())
            .await,
        Err(DatabaseError::BudgetExhausted)
    );
    pool.close().await;
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn provider_policy_preserves_counters_spacing_and_independent_caps() {
    let fixture = support::TestDatabase::new().await;
    let base = fixture.open(100).await;
    let pool = base.pool();
    sqlx::query("UPDATE openlegal.provider_request_budget SET utc_day=floor(extract(epoch from clock_timestamp()))::bigint/86400,daily_used=1000,on_demand_used=1")
        .execute(&pool).await.unwrap();
    let selected = ProviderRequestLimits::new(50_000, 1000, 1).unwrap();
    assert_eq!(
        LawClient::configure_provider_request_limits(&pool, &selected)
            .await
            .unwrap(),
        0
    );
    let mode = RequestBudgetMode::Continuous;
    LawClient::reserve_provider_request_budget(&pool, &mode, &token())
        .await
        .unwrap();
    let first: i64 = sqlx::query_scalar(
        "SELECT next_request_at_ms FROM openlegal.provider_request_budget WHERE singleton",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        LawClient::reserve_provider_request_budget(&pool, &mode, &token()).await,
        Err(DatabaseError::BudgetExhausted)
    );
    sqlx::query("UPDATE openlegal.provider_request_budget SET unresolved_response=false")
        .execute(&pool)
        .await
        .unwrap();
    LawClient::reserve_provider_request_budget(&pool, &mode, &token())
        .await
        .unwrap();
    let second: i64 = sqlx::query_scalar(
        "SELECT next_request_at_ms FROM openlegal.provider_request_budget WHERE singleton",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(
        second - first >= 1001,
        "durable one-second spacing must survive separate reservations"
    );
    let counts: (i64, i64) = sqlx::query_as(
        "SELECT daily_used,on_demand_used FROM openlegal.provider_request_budget WHERE singleton",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(counts, (1002, 1));
    sqlx::query("UPDATE openlegal.provider_request_budget SET unresolved_response=false,daily_used=50000,next_request_at_ms=0").execute(&pool).await.unwrap();
    assert_eq!(
        LawClient::reserve_provider_request_budget(&pool, &mode, &token()).await,
        Err(DatabaseError::BudgetExhausted)
    );
    LawClient::reserve_provider_request_budget(&pool, &RequestBudgetMode::OnDemand, &token())
        .await
        .unwrap();
    let lower = ProviderRequestLimits::new(500, 1, 1).unwrap();
    LawClient::configure_provider_request_limits(&pool, &lower)
        .await
        .unwrap();
    sqlx::query("UPDATE openlegal.provider_request_budget SET unresolved_response=false,next_request_at_ms=0").execute(&pool).await.unwrap();
    assert_eq!(
        LawClient::reserve_provider_request_budget(&pool, &mode, &token()).await,
        Err(DatabaseError::BudgetExhausted)
    );
    assert_eq!(
        LawClient::reserve_provider_request_budget(&pool, &RequestBudgetMode::OnDemand, &token())
            .await,
        Err(DatabaseError::BudgetExhausted)
    );
    let counts: (i64, i64) = sqlx::query_as(
        "SELECT daily_used,on_demand_used FROM openlegal.provider_request_budget WHERE singleton",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(counts, (50000, 2));
    base.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn provider_policy_wakes_only_budget_waits_and_preserves_pauses_and_claim_fences() {
    let fixture = support::TestDatabase::new().await;
    let base = fixture.open(100).await;
    let pool = base.pool();
    let blobs = FsBlobStore::open(&fixture.directory.path().join("policy-wake"))
        .await
        .unwrap();
    let store = PgCorpusStore::new(pool.clone(), blobs);
    let now: i64 =
        sqlx::query_scalar("SELECT floor(extract(epoch from clock_timestamp()))::bigint")
            .fetch_one(&pool)
            .await
            .unwrap();
    store
        .enqueue_job(object(), "policy-wait".into(), None, true, true, now as u64)
        .await
        .unwrap();
    let old = store.claim_job(now as u64).await.unwrap().unwrap();
    store
        .defer_budget_claim(&old, (now + 86400) as u64)
        .await
        .unwrap();
    sqlx::query("UPDATE openlegal.provider_request_budget SET utc_day=floor(extract(epoch from clock_timestamp()))::bigint/86400,daily_used=1000,on_demand_used=1,operator_suspended=true,next_allowed_at=$1")
        .bind(now+120).execute(&pool).await.unwrap();
    let selected = ProviderRequestLimits::new(50_000, 1000, 1).unwrap();
    assert_eq!(
        LawClient::configure_provider_request_limits(&pool, &selected)
            .await
            .unwrap(),
        0
    );
    let default = ProviderRequestLimits::new(1000, 1000, 5).unwrap();
    LawClient::configure_provider_request_limits(&pool, &default)
        .await
        .unwrap();
    sqlx::query("UPDATE openlegal.provider_request_budget SET operator_suspended=false,unresolved_response=true").execute(&pool).await.unwrap();
    assert_eq!(
        LawClient::configure_provider_request_limits(&pool, &selected)
            .await
            .unwrap(),
        0
    );
    LawClient::configure_provider_request_limits(&pool, &default)
        .await
        .unwrap();
    sqlx::query("UPDATE openlegal.provider_request_budget SET unresolved_response=false")
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(
        LawClient::configure_provider_request_limits(&pool, &selected)
            .await
            .unwrap(),
        1
    );
    let lease: String =
        sqlx::query_scalar("SELECT lease_until::text FROM openlegal.corpus_job WHERE id=$1::uuid")
            .bind(&old.id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        lease.parse::<i64>().unwrap(),
        now + 120,
        "Retry-After must survive wake"
    );
    assert!(store.claim_job((now + 119) as u64).await.unwrap().is_none());
    let resumed = store.claim_job((now + 120) as u64).await.unwrap().unwrap();
    assert_eq!(resumed.attempts, 1);
    assert!(resumed.expected_version > old.expected_version);
    assert_eq!(
        store.defer_budget_claim(&old, (now + 500) as u64).await,
        Err(DatabaseError::Conflict)
    );
    let category: Option<String> =
        sqlx::query_scalar("SELECT error_category FROM openlegal.corpus_job WHERE id=$1::uuid")
            .bind(&old.id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(category, None);
    LawClient::configure_provider_request_limits(&pool, &default)
        .await
        .unwrap();
    assert_eq!(
        LawClient::configure_provider_request_limits(&pool, &selected)
            .await
            .unwrap(),
        0,
        "a reclaimed job must not be woken again"
    );
    let counts: (i64, i64, bool, bool) = sqlx::query_as("SELECT daily_used,on_demand_used,operator_suspended,unresolved_response FROM openlegal.provider_request_budget WHERE singleton").fetch_one(&pool).await.unwrap();
    assert_eq!(counts, (1000, 1, false, false));
    store.fail_claim(&resumed, false).await.unwrap();
    base.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn source_rejection_suspends_restart_admission_and_clears_resolved_attempt() {
    let fixture = support::TestDatabase::new().await;
    let base = fixture.open(100).await;
    let pool = base.pool();
    let client = LawClient::new(
        "fixture-credential".into(),
        Arc::new(UnusedDocumentProcessor),
    )
    .unwrap()
    .with_request_budget(pool.clone(), RequestBudgetMode::Pilot);
    LawClient::reserve_provider_request_budget(&pool, &RequestBudgetMode::Pilot, &token())
        .await
        .unwrap();
    client.suspend_after_source_rejection().await.unwrap();
    let flags: (bool, bool) = sqlx::query_as(
        "SELECT operator_suspended,unresolved_response FROM openlegal.provider_request_budget WHERE singleton",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(flags, (true, false));
    assert_eq!(
        LawClient::reserve_provider_request_budget(&pool, &RequestBudgetMode::Pilot, &token())
            .await,
        Err(DatabaseError::BudgetExhausted)
    );
    base.close().await.unwrap();
}
fn object() -> ObjectId {
    ObjectId {
        jurisdiction: "kr".into(),
        provider: "fictional_test".into(),
        dataset: Dataset::NationalStatute,
        id: "001".into(),
    }
}
fn record(revision: &str, body: &str) -> LegalRecord {
    LegalRecord {
        object: object(),
        revision_id: revision.into(),
        title: "Fictional statute".into(),
        body: body.into(),
        metadata: BTreeMap::new(),
        publication_date: Some("20260101".into()),
        effective_date: Some("20260201".into()),
        source_url: "https://example.test/fictional".into(),
        representation: "provider_text_v1".into(),
        sections: vec![],
    }
}
async fn publish(store: &PgCorpusStore, revision: &str, body: &str, now: u64) -> Capture {
    let state = store.state(&object()).await.unwrap();
    store
        .publish(
            Publication {
                additional_evidence: Vec::new(),
                record: record(revision, body),
                raw: body.as_bytes().to_vec(),
                processor_version: "fixture_v1".into(),
                retrieved_at: now,
                now,
                expected_version: state.version,
                install_head: true,
                job_id: None,
            },
            token(),
        )
        .await
        .unwrap()
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn citation_lease_handoff_renews_retention_without_reviving_expired_history() {
    let fixture = support::TestDatabase::new().await;
    let base = fixture.open(100).await;
    let blobs = FsBlobStore::open(&fixture.directory.path().join("citation-retention"))
        .await
        .unwrap();
    let store = PgCorpusStore::with_publication_clock(base.pool(), blobs, Arc::new(FixtureClock));
    let old = publish(&store, "old", "old retained evidence", 100).await;
    publish(&store, "new", "new retained evidence", 200).await;
    let now = 3_000_000;
    let selected = vec![(object(), old.capture_id.clone())];
    assert_eq!(
        store.renew(selected.clone(), now, token()).await,
        Err(DatabaseError::RevisionUnavailable)
    );
    let watermark = store.watermark().await.unwrap();
    store.acknowledge_index(watermark).await.unwrap();
    let session = "a".repeat(64);
    store
        .pin_session(session.clone(), watermark, vec![], now)
        .await
        .unwrap();
    store.renew(selected.clone(), now, token()).await.unwrap();
    store.release_session(&session).await.unwrap();
    assert_eq!(store.maintain(now + 599, 250).await.unwrap(), 0);
    assert!(store.outbox(watermark, 10).await.unwrap().is_empty());
    assert_eq!(
        store
            .resolve(
                object(),
                RevisionSelector::Capture {
                    id: old.capture_id.clone()
                },
                now + 599,
                token()
            )
            .await
            .unwrap()
            .record
            .body,
        "old retained evidence"
    );
    store
        .renew(selected.clone(), now + 590, token())
        .await
        .unwrap();
    // A second request using an older clock cannot shorten shared retention.
    store
        .renew(selected.clone(), now + 580, token())
        .await
        .unwrap();
    let expiry: String = sqlx::query_scalar(
        "SELECT expires_at::text FROM openlegal.corpus_citation_lease WHERE capture_id=$1",
    )
    .bind(&old.capture_id)
    .fetch_one(&base.pool())
    .await
    .unwrap();
    assert_eq!(expiry.parse::<u64>().unwrap(), now + 1190);
    assert_eq!(store.maintain(now + 1189, 250).await.unwrap(), 0);
    assert_eq!(
        store
            .resolve(
                object(),
                RevisionSelector::Capture {
                    id: old.capture_id.clone()
                },
                now + 1190,
                token()
            )
            .await
            .err(),
        Some(DatabaseError::RevisionUnavailable)
    );
    assert_eq!(
        store.renew(selected, now + 1190, token()).await,
        Err(DatabaseError::RevisionUnavailable)
    );
    assert_eq!(store.maintain(now + 1190, 250).await.unwrap(), 0);
    let retirement = store.outbox(watermark, 10).await.unwrap().remove(0);
    assert_eq!(retirement.capture_id.as_ref(), Some(&old.capture_id));
    store.acknowledge_index(retirement.sequence).await.unwrap();
    assert_eq!(store.maintain(now + 1191, 250).await.unwrap(), 1);
    assert_eq!(
        store
            .resolve(object(), RevisionSelector::Head, now + 1191, token())
            .await
            .unwrap()
            .record
            .revision_id,
        "new"
    );
    base.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn citation_lease_identity_cancellation_and_withdrawal_are_atomic() {
    let fixture = support::TestDatabase::new().await;
    let base = fixture.open(100).await;
    let blobs = FsBlobStore::open(&fixture.directory.path().join("citation-identity"))
        .await
        .unwrap();
    let store = PgCorpusStore::with_publication_clock(base.pool(), blobs, Arc::new(FixtureClock));
    let capture = publish(&store, "r1", "retained fixture", 100).await;
    let mut other = object();
    other.id = "002".into();
    assert_eq!(
        store
            .renew(vec![(other, capture.capture_id.clone())], 110, token())
            .await,
        Err(DatabaseError::RevisionUnavailable)
    );
    assert_eq!(
        store
            .renew(
                vec![
                    (object(), capture.capture_id.clone()),
                    (object(), "f".repeat(64))
                ],
                110,
                token()
            )
            .await,
        Err(DatabaseError::RevisionUnavailable)
    );
    let cancelled = token();
    cancelled.cancel();
    assert_eq!(
        store
            .renew(vec![(object(), capture.capture_id.clone())], 110, cancelled)
            .await,
        Err(DatabaseError::Cancelled)
    );
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM openlegal.corpus_citation_lease")
        .fetch_one(&base.pool())
        .await
        .unwrap();
    assert_eq!(count, 0);
    store
        .renew(
            vec![
                (object(), capture.capture_id.clone()),
                (object(), capture.capture_id.clone()),
            ],
            110,
            token(),
        )
        .await
        .unwrap();
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM openlegal.corpus_citation_lease")
        .fetch_one(&base.pool())
        .await
        .unwrap();
    assert_eq!(count, 1);
    let version = store.state(&object()).await.unwrap().version;
    store.withdraw(&object(), version, 111).await.unwrap();
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM openlegal.corpus_citation_lease")
        .fetch_one(&base.pool())
        .await
        .unwrap();
    assert_eq!(count, 0);
    assert_eq!(
        store
            .renew(vec![(object(), capture.capture_id.clone())], 112, token())
            .await,
        Err(DatabaseError::Withdrawn)
    );
    assert_eq!(
        store
            .resolve(
                object(),
                RevisionSelector::Capture {
                    id: capture.capture_id
                },
                112,
                token()
            )
            .await
            .err(),
        Some(DatabaseError::Withdrawn)
    );
    base.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn citation_capacity_is_separate_from_sessions_and_batch_renewal_is_atomic() {
    let fixture = support::TestDatabase::new().await;
    let base = fixture.open(100).await;
    let blobs = FsBlobStore::open(&fixture.directory.path().join("citation-capacity"))
        .await
        .unwrap();
    let store = PgCorpusStore::with_publication_clock(base.pool(), blobs, Arc::new(FixtureClock));
    let first = publish(&store, "r1", "first fixture", 100).await;
    let second = publish(&store, "r2", "second fixture", 200).await;
    let third = publish(&store, "r3", "third fixture", 220).await;
    store
        .renew(vec![(object(), first.capture_id.clone())], 250, token())
        .await
        .unwrap();
    // Capacity-only rows are isolated synthetic bookkeeping fixtures. Their
    // bodies are never resolved as evidence or added to an index.
    sqlx::query("INSERT INTO openlegal.corpus_capture(id,object_key,revision_id,sequence,captured_at,event_sequence,payload,payload_sha256,raw_sha256,raw_size,storage_key) SELECT repeat('d',56)||lpad(to_hex(n),8,'0'),c.object_key,'capacity-'||n,c.sequence+n+100,c.captured_at,c.event_sequence,c.payload,c.payload_sha256,c.raw_sha256,c.raw_size,'citation-capacity-'||n FROM openlegal.corpus_capture c CROSS JOIN generate_series(1,2047) n WHERE c.id=$1")
        .bind(&first.capture_id).execute(&base.pool()).await.unwrap();
    sqlx::query("INSERT INTO openlegal.corpus_citation_lease SELECT id,1000 FROM openlegal.corpus_capture WHERE storage_key LIKE 'citation-capacity-%'")
        .execute(&base.pool()).await.unwrap();
    // Existing captures can renew at capacity; duplicate hits consume one slot.
    store
        .renew(
            vec![
                (object(), first.capture_id.clone()),
                (object(), first.capture_id.clone()),
            ],
            260,
            token(),
        )
        .await
        .unwrap();
    assert_eq!(
        store
            .renew(
                vec![
                    (object(), first.capture_id.clone()),
                    (object(), second.capture_id.clone())
                ],
                270,
                token()
            )
            .await,
        Err(DatabaseError::Capacity)
    );
    let expiry: String = sqlx::query_scalar(
        "SELECT expires_at::text FROM openlegal.corpus_citation_lease WHERE capture_id=$1",
    )
    .bind(&first.capture_id)
    .fetch_one(&base.pool())
    .await
    .unwrap();
    assert_eq!(expiry, "860");
    let count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM openlegal.corpus_citation_lease WHERE capture_id=$1",
    )
    .bind(&second.capture_id)
    .fetch_one(&base.pool())
    .await
    .unwrap();
    assert_eq!(count, 0);
    sqlx::query("UPDATE openlegal.corpus_citation_lease SET expires_at=270 WHERE capture_id=$1")
        .bind(format!("{}00000001", "d".repeat(56)))
        .execute(&base.pool())
        .await
        .unwrap();
    store
        .renew(vec![(object(), third.capture_id)], 270, token())
        .await
        .unwrap();
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM openlegal.corpus_citation_lease")
        .fetch_one(&base.pool())
        .await
        .unwrap();
    assert_eq!(count, 2048);
    let sessions: i64 = sqlx::query_scalar("SELECT count(*) FROM openlegal.corpus_session")
        .fetch_one(&base.pool())
        .await
        .unwrap();
    assert_eq!(sessions, 0);
    base.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh and the provisioned Korean dictionary"]
async fn citable_search_transfers_historical_hits_and_releases_partial_generation() {
    use openlegal_adapters::{
        corpus_search::CorpusSearch, korean_analysis::KoreanAnalyzer, search_index::CorpusIndex,
    };
    use openlegal_application::search::{SearchMode, SearchService};
    use openlegal_domain::legal_search::{Filters, SearchRequest};
    use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
    let fixture = support::TestDatabase::new().await;
    let base = fixture.open(100).await;
    let blobs = FsBlobStore::open(&fixture.directory.path().join("citation-search"))
        .await
        .unwrap();
    let store = Arc::new(PgCorpusStore::with_publication_clock(
        base.pool(),
        blobs,
        Arc::new(FixtureClock),
    ));
    let first = publish(&store, "r1", "historical citation fixture one", 100).await;
    let second = publish(&store, "r2", "historical citation fixture two", 150).await;
    let head = publish(&store, "r3", "unrelated fixture", 200).await;
    let dictionary =
        std::env::var_os("OPENLEGAL_TEST_MECAB_DICTIONARY").expect("run scripts/test-postgres.sh");
    let index = CorpusIndex::open(
        &fixture.directory.path().join("citation-index"),
        KoreanAnalyzer::open(std::path::Path::new(&dictionary)).unwrap(),
    )
    .unwrap();
    index.apply_capture(first, false, 1).unwrap();
    index.apply_capture(second, false, 2).unwrap();
    index.apply_capture(head, true, 3).unwrap();
    store.acknowledge_index(3).await.unwrap();
    let search = SearchService::new(Arc::new(CorpusSearch::new(index, store.clone())));
    let request = SearchRequest {
        query: "citation".into(),
        filters: Filters::default(),
        include_history: true,
        include_ocr: false,
        sections: vec!["body".into()],
        limit: 1,
        cursor: None,
        literal: true,
        ignore_case: false,
        context_lines: 0,
    };
    let page = search
        .search_citable(
            request.clone(),
            Instant::now() + Duration::from_secs(10),
            token(),
        )
        .await
        .unwrap();
    assert_eq!(page.hits.len(), 1);
    assert!(page.next_cursor.is_some());
    let sessions: i64 = sqlx::query_scalar("SELECT count(*) FROM openlegal.corpus_session")
        .fetch_one(&base.pool())
        .await
        .unwrap();
    assert_eq!(sessions, 0);
    let leases: i64 = sqlx::query_scalar("SELECT count(*) FROM openlegal.corpus_citation_lease")
        .fetch_one(&base.pool())
        .await
        .unwrap();
    assert_eq!(leases, 1);
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let capture = store
        .resolve(
            object(),
            RevisionSelector::Capture {
                id: page.hits[0].capture_id.clone(),
            },
            now,
            token(),
        )
        .await
        .unwrap();
    assert!(capture.record.body.contains("historical citation"));
    let mut continuation = request;
    continuation.cursor = page.next_cursor;
    assert_eq!(
        search
            .search(
                SearchMode::Query,
                continuation,
                Instant::now() + Duration::from_secs(10),
                token()
            )
            .await
            .err(),
        Some(DatabaseError::SessionExpired)
    );
    base.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn object_status_distinguishes_unobserved_processing_and_incomplete() {
    let fixture = support::TestDatabase::new().await;
    let base = fixture.open(100).await;
    let blobs = FsBlobStore::open(&fixture.directory.path().join("object-status"))
        .await
        .unwrap();
    let store = PgCorpusStore::new(base.pool(), blobs);
    let absent = store.object_status(&object(), 100).await.unwrap();
    assert_eq!(absent.state, ObjectCollectionState::NotObserved);
    assert!(absent.job.is_none());
    assert_eq!(
        store
            .resolve(object(), RevisionSelector::Head, 100, token())
            .await
            .err(),
        Some(DatabaseError::NotObserved)
    );
    store
        .enqueue_job(object(), "r1".into(), None, true, true, 100)
        .await
        .unwrap();
    let waiting = store.object_status(&object(), 100).await.unwrap();
    assert_eq!(waiting.state, ObjectCollectionState::ProcessingPending);
    assert_eq!(waiting.job.unwrap().status, "pending");
    let claimed = store.claim_job(101).await.unwrap().unwrap();
    let running = store.object_status(&object(), 101).await.unwrap();
    assert_eq!(running.state, ObjectCollectionState::ProcessingPending);
    assert_eq!(running.job.as_ref().unwrap().started_at, Some(101));
    assert!(matches!(running.eta, ObjectCompletionEta::Unknown { .. }));
    for sample in 0..20 {
        let mut other = object();
        other.id = format!("eta-sample-{sample}");
        store
            .enqueue_job(other.clone(), "r1".into(), None, true, true, 80)
            .await
            .unwrap();
        sqlx::query("UPDATE openlegal.corpus_job j SET status='done',started_at=80,completed_at=$2 FROM openlegal.corpus_object o WHERE j.object_key=o.object_key AND o.identity->>'id'=$1")
            .bind(&other.id)
            .bind(90 + sample)
            .execute(&base.pool())
            .await
            .unwrap();
    }
    let estimated = store.object_status(&object(), 101).await.unwrap();
    assert!(matches!(
        estimated.eta,
        ObjectCompletionEta::Range {
            earliest_at: 115,
            latest_at: 128,
            sample_size: 20
        }
    ));
    store
        .skip_claim(&claimed, "source_data_invalid", 102)
        .await
        .unwrap();
    let incomplete = store.object_status(&object(), 102).await.unwrap();
    assert_eq!(
        incomplete.state,
        ObjectCollectionState::CollectionIncomplete
    );
    assert_eq!(incomplete.retry_at, Some(3702));
    assert_eq!(incomplete.job.unwrap().completed_at, Some(102));
    assert_eq!(
        store
            .resolve(object(), RevisionSelector::Head, 102, token())
            .await
            .err(),
        Some(DatabaseError::CollectionIncomplete)
    );
    base.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn expired_explicit_jobs_become_claimable_by_continuous_workers() {
    let fixture = support::TestDatabase::new().await;
    let base = fixture.open(100).await;
    let blobs = FsBlobStore::open(&fixture.directory.path().join("orphan-explicit"))
        .await
        .unwrap();
    let store = PgCorpusStore::new(base.pool(), blobs);
    let mut metadata = BTreeMap::new();
    metadata.insert("collection_origin".into(), "explicit".into());
    let pending = store
        .enqueue_job_with_metadata(
            object(),
            "pending-orphan".into(),
            None,
            true,
            true,
            100,
            metadata.clone(),
        )
        .await
        .unwrap();
    assert!(store.claim_job(101).await.unwrap().is_none());
    store.requeue_due_details(8201).await.unwrap();
    assert_eq!(store.claim_job(8202).await.unwrap().unwrap().id, pending.id);

    let mut second = object();
    second.id = "running-orphan".into();
    let running = store
        .enqueue_job_with_metadata(second, "r1".into(), None, true, true, 100, metadata)
        .await
        .unwrap();
    assert_eq!(
        store
            .claim_explicit_job(&running.id, 101, 600)
            .await
            .unwrap()
            .unwrap()
            .id,
        running.id
    );
    store.requeue_due_details(702).await.unwrap();
    assert!(store.claim_job(703).await.unwrap().is_none());
    store.requeue_due_details(8201).await.unwrap();
    assert_eq!(store.claim_job(8202).await.unwrap().unwrap().id, running.id);
    base.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn incomplete_attachment_evidence_never_replaces_a_complete_head() {
    let fixture = support::TestDatabase::new().await;
    let base = fixture.open(100).await;
    let blobs = FsBlobStore::open(&fixture.directory.path().join("partial-head"))
        .await
        .unwrap();
    let store = PgCorpusStore::with_publication_clock(base.pool(), blobs, Arc::new(FixtureClock));
    let complete = publish(&store, "r1", "complete body", 100).await;
    store
        .enqueue_job(object(), "r2".into(), None, true, true, 200)
        .await
        .unwrap();
    assert_eq!(
        store
            .resolve(object(), RevisionSelector::Head, 200, token())
            .await
            .err(),
        Some(DatabaseError::ProcessingPending)
    );
    let job = store.claim_job(201).await.unwrap().unwrap();
    let mut partial = record("r2", "new body");
    partial.publication_date = Some("20260301".into());
    partial.effective_date = Some("20260401".into());
    partial
        .metadata
        .insert("attachment_status".into(), "incomplete".into());
    let rejected = b"<html>fictional busy page</html>".to_vec();
    let expected_digest = Sha256::digest(&rejected).to_vec();
    let observed = store
        .publish(
            Publication {
                record: partial,
                raw: b"new body".to_vec(),
                additional_evidence: vec![rejected],
                processor_version: "fixture_v1".into(),
                retrieved_at: 200,
                now: 202,
                expected_version: job.expected_version,
                install_head: true,
                job_id: Some(job.id),
            },
            token(),
        )
        .await
        .unwrap();
    let state = store.state(&object()).await.unwrap();
    assert_eq!(state.head_capture, Some(complete.capture_id.clone()));
    assert!(state.pending);
    assert!(store.detail_gap_active(&object(), "r2").await.unwrap());
    assert_eq!(
        store
            .resolve(object(), RevisionSelector::Head, 202, token())
            .await
            .err(),
        Some(DatabaseError::CollectionIncomplete)
    );
    let retained = store
        .resolve(
            object(),
            RevisionSelector::Capture {
                id: complete.capture_id.clone(),
            },
            202,
            token(),
        )
        .await
        .unwrap();
    assert_eq!(retained.capture_id, complete.capture_id);
    assert_eq!(retained.validated_at, 100);
    let head_validated_at: String = sqlx::query_scalar(
        "SELECT validated_at::text FROM openlegal.corpus_object WHERE head_capture=$1",
    )
    .bind(&complete.capture_id)
    .fetch_one(&base.pool())
    .await
    .unwrap();
    assert_eq!(head_validated_at, "100");
    let partial = store
        .resolve(
            object(),
            RevisionSelector::Capture {
                id: observed.capture_id.clone(),
            },
            202,
            token(),
        )
        .await
        .unwrap();
    assert_eq!(partial.record.body, "new body");
    assert_eq!(partial.record.metadata["attachment_status"], "incomplete");
    let stored_digest: Vec<u8> = sqlx::query_scalar(
        "SELECT raw_sha256 FROM openlegal.corpus_capture_blob WHERE capture_id=$1 AND ordinal=1",
    )
    .bind(&observed.capture_id)
    .fetch_one(&base.pool())
    .await
    .unwrap();
    assert_eq!(stored_digest, expected_digest);
    assert_eq!(
        store
            .resolve(
                object(),
                RevisionSelector::Revision { id: "r2".into() },
                202,
                token(),
            )
            .await
            .unwrap()
            .capture_id,
        observed.capture_id
    );
    assert!(
        !store
            .head_revision_ready(&object(), "r2", 202)
            .await
            .unwrap()
    );
    store
        .enqueue_job(object(), "r1".into(), None, true, true, 203)
        .await
        .unwrap();
    let same_revision_job = store.claim_job(204).await.unwrap().unwrap();
    let mut same_revision = record("r1", "incomplete body");
    same_revision
        .metadata
        .insert("attachment_status".into(), "incomplete".into());
    let same_revision_capture = store
        .publish(
            Publication {
                record: same_revision,
                raw: b"incomplete body".to_vec(),
                additional_evidence: vec![b"<html>second fictional busy page</html>".to_vec()],
                processor_version: "fixture_v1".into(),
                retrieved_at: 205,
                now: 205,
                expected_version: same_revision_job.expected_version,
                install_head: true,
                job_id: Some(same_revision_job.id),
            },
            token(),
        )
        .await
        .unwrap();
    assert_ne!(same_revision_capture.capture_id, complete.capture_id);
    assert_eq!(
        store.state(&object()).await.unwrap().head_capture,
        Some(complete.capture_id.clone())
    );
    assert_eq!(
        store
            .resolve(object(), RevisionSelector::Head, 205, token())
            .await
            .unwrap()
            .validated_at,
        100
    );
    assert_eq!(
        store
            .resolve(
                object(),
                RevisionSelector::Revision { id: "r1".into() },
                205,
                token()
            )
            .await
            .unwrap()
            .capture_id,
        complete.capture_id
    );
    store
        .mark_inventory_complete(&object(), true)
        .await
        .unwrap();
    for selector in [
        RevisionSelector::Revision { id: "r1".into() },
        RevisionSelector::PublicationDate {
            date: "20260101".into(),
        },
        RevisionSelector::EffectiveDate {
            date: "20260201".into(),
        },
    ] {
        assert_eq!(
            store
                .resolve(object(), selector.clone(), 205, token())
                .await
                .unwrap()
                .capture_id,
            complete.capture_id
        );
        assert_eq!(
            store
                .resolve_metadata(object(), selector, 205, token())
                .await
                .unwrap()
                .capture_id,
            complete.capture_id
        );
    }
    base.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn incomplete_attachment_capture_retains_private_response_without_claiming_coverage() {
    let fixture = support::TestDatabase::new().await;
    let base = fixture.open(100).await;
    let blobs = FsBlobStore::open(&fixture.directory.path().join("partial-evidence"))
        .await
        .unwrap();
    let store = PgCorpusStore::with_publication_clock(base.pool(), blobs, Arc::new(FixtureClock));
    let mut partial = record("r1", "identified provider body");
    partial.object.provider = "law_go_kr".into();
    partial
        .metadata
        .insert("attachment_status".into(), "incomplete".into());
    partial
        .metadata
        .insert("attachment_expected_count".into(), "1".into());
    partial
        .metadata
        .insert("attachment_available_count".into(), "0".into());
    let object = partial.object.clone();
    let rejected = b"<html>fictional busy page</html>".to_vec();
    let expected_digest = Sha256::digest(&rejected).to_vec();
    let capture = store
        .publish(
            Publication {
                record: partial,
                raw: b"identified provider body".to_vec(),
                additional_evidence: vec![rejected],
                processor_version: "fixture_v1".into(),
                retrieved_at: 100,
                now: 100,
                expected_version: store.state(&object).await.unwrap().version,
                install_head: true,
                job_id: None,
            },
            token(),
        )
        .await
        .unwrap();
    sqlx::query("UPDATE openlegal.corpus_object SET desired_head_revision='r1' WHERE identity->>'provider'='law_go_kr'")
        .execute(&base.pool()).await.unwrap();
    let stored_digest: Vec<u8> = sqlx::query_scalar(
        "SELECT raw_sha256 FROM openlegal.corpus_capture_blob WHERE capture_id=$1 AND ordinal=1",
    )
    .bind(&capture.capture_id)
    .fetch_one(&base.pool())
    .await
    .unwrap();
    assert_eq!(stored_digest, expected_digest);
    assert!(!store.head_revision_ready(&object, "r1", 100).await.unwrap());
    assert!(
        !store
            .revision_capture_recent(&object, "r1", 100)
            .await
            .unwrap()
    );
    assert!(!store.current_coverage_ready(100).await.unwrap());
    assert_eq!(
        store
            .resolve(object, RevisionSelector::Head, 100, token())
            .await
            .unwrap()
            .capture_id,
        capture.capture_id
    );
    base.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn corpus_capture_identity_unchanged_validation_and_conflict() {
    let fixture = support::TestDatabase::new().await;
    let base = fixture.open(100).await;
    let blobs =
        FsBlobStore::open_with_limit(&fixture.directory.path().join("corpus"), 100 * 1024 * 1024)
            .await
            .unwrap();
    let store = PgCorpusStore::with_publication_clock(
        base.pool(),
        blobs,
        std::sync::Arc::new(FixtureClock),
    );
    let a = publish(&store, "r1", "A", 100).await;
    let unchanged = publish(&store, "r1", "A", 110).await;
    assert_eq!(a.capture_id, unchanged.capture_id);
    assert_eq!(unchanged.retrieved_at, 100);
    assert_eq!(unchanged.validated_at, 110);
    let b = publish(&store, "r1", "B", 120).await;
    assert_ne!(a.capture_id, b.capture_id);
    let a2 = publish(&store, "r1", "A", 130).await;
    assert_ne!(a.capture_id, a2.capture_id);
    let past = store
        .resolve(
            object(),
            RevisionSelector::Capture {
                id: a.capture_id.clone(),
            },
            140,
            token(),
        )
        .await
        .unwrap();
    assert_eq!(past.record.body, "A");
    assert_eq!(past.validated_at, 100);
    let head = store
        .resolve(object(), RevisionSelector::Head, 140, token())
        .await
        .unwrap();
    assert_eq!(head.capture_id, a2.capture_id);
    let stale = store
        .publish(
            Publication {
                additional_evidence: Vec::new(),
                record: record("r2", "obsolete"),
                raw: b"obsolete".to_vec(),
                processor_version: "fixture_v1".into(),
                retrieved_at: 140,
                now: 140,
                expected_version: 0,
                install_head: true,
                job_id: None,
            },
            token(),
        )
        .await;
    assert!(matches!(stale, Err(DatabaseError::Conflict)));
    let page = store
        .history(object(), HistoryKind::Captures, None, 100, 140, token())
        .await
        .unwrap();
    assert_eq!(page.entries.len(), 3);
    assert_eq!(page.entries[0].sequence, 3);
    base.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn corpus_revision_history_checkpoint_order_and_bounded_pages() {
    let fixture = support::TestDatabase::new().await;
    let base = fixture.open(100).await;
    let blobs = FsBlobStore::open(&fixture.directory.path().join("history-order"))
        .await
        .unwrap();
    let store = PgCorpusStore::with_publication_clock(base.pool(), blobs, Arc::new(FixtureClock));
    let longest_id = format!("{}:\"Z!", "한".repeat(84));
    assert_eq!(longest_id.len(), 256);
    // This is the expected presentation order, independent of observation order.
    // Publication-only and effective-date entries share one checkpoint date;
    // C collation breaks ties, including punctuation and non-ASCII IDs.
    let revisions = [
        ("latest", Some("20000101"), Some("20270101")),
        ("A", None, Some("20260101")),
        ("a:quoted\"한", None, Some("20260101")),
        ("publication-only", Some("20260101"), None),
        (longest_id.as_str(), None, Some("20260101")),
        ("old-effective", Some("20990101"), Some("20250101")),
        ("old-publication", Some("20240101"), None),
        ("unknown:A", None, None),
        ("unknown:a", None, None),
    ];
    let expected: Vec<_> = revisions.iter().map(|r| r.0.to_owned()).collect();
    for (case, order) in [
        ("reverse", vec![8, 7, 6, 5, 4, 3, 2, 1, 0]),
        ("shuffled", vec![0, 4, 2, 8, 5, 1, 7, 3, 6]),
    ] {
        let mut object = object();
        object.id = case.into();
        let mut sequences = BTreeMap::new();
        for (position, index) in order.into_iter().enumerate() {
            let (id, publication, effective) = revisions[index];
            store
                .record_revision_catalog(&object, id, publication, effective, 100)
                .await
                .unwrap();
            sequences.insert(id.to_owned(), position as u64 + 1);
        }
        for limit in [1, 3, 100] {
            let mut seen = Vec::new();
            let mut cursor = None;
            loop {
                let page = store
                    .history(
                        object.clone(),
                        HistoryKind::Revisions,
                        cursor,
                        limit,
                        101,
                        token(),
                    )
                    .await
                    .unwrap();
                assert!(!page.entries.is_empty());
                assert!(page.entries.len() <= limit);
                for entry in page.entries {
                    assert_eq!(entry.sequence, sequences[&entry.revision_id]);
                    assert!(entry.capture_id.is_none());
                    assert!(entry.captured_at.is_none());
                    seen.push(entry.revision_id);
                }
                if let Some(next) = page.next_cursor {
                    assert!(next.len() <= 512);
                    assert!(next.contains(":r2:"));
                    assert!(seen.len() < expected.len());
                    cursor = Some(next);
                } else {
                    break;
                }
            }
            assert_eq!(seen, expected, "{case}, page size {limit}");
        }
    }
    base.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn corpus_revision_history_cursors_validate_and_fence_changes() {
    let fixture = support::TestDatabase::new().await;
    let base = fixture.open(100).await;
    let blobs = FsBlobStore::open(&fixture.directory.path().join("history-cursors"))
        .await
        .unwrap();
    let store = PgCorpusStore::with_publication_clock(base.pool(), blobs, Arc::new(FixtureClock));
    let object = object();
    for (id, date) in [("new", "20260101"), ("old", "20250101")] {
        store
            .record_revision_catalog(&object, id, None, Some(date), 100)
            .await
            .unwrap();
    }
    let page = store
        .history(
            object.clone(),
            HistoryKind::Revisions,
            None,
            1,
            101,
            token(),
        )
        .await
        .unwrap();
    let cursor = page.next_cursor.unwrap();
    let fields: Vec<_> = cursor.splitn(5, ':').collect();
    let prefix = format!("{}:r2:{}:", fields[0], fields[2]);
    let legacy = format!("{}:r:{}:{}", fields[0], fields[2], page.entries[0].sequence);
    for invalidated in [
        legacy,
        "malformed".into(),
        cursor.replacen(":r2:", ":c:", 1),
        cursor.replacen(fields[0], &"0".repeat(64), 1),
    ] {
        assert!(matches!(
            store
                .history(
                    object.clone(),
                    HistoryKind::Revisions,
                    Some(invalidated),
                    1,
                    101,
                    token()
                )
                .await,
            Err(DatabaseError::SnapshotInvalidated)
        ));
    }
    for invalid in [
        format!("{prefix}20260230:new"),
        format!("{prefix}20260101:"),
        format!("{prefix}20260101:{}", "x".repeat(257)),
        format!("{prefix}20260101:bad\nrevision"),
        "x".repeat(513),
    ] {
        assert!(matches!(
            store
                .history(
                    object.clone(),
                    HistoryKind::Revisions,
                    Some(invalid),
                    1,
                    101,
                    token()
                )
                .await,
            Err(DatabaseError::InvalidInput)
        ));
    }
    assert!(matches!(
        store
            .history(
                object.clone(),
                HistoryKind::Captures,
                Some(cursor.clone()),
                1,
                101,
                token()
            )
            .await,
        Err(DatabaseError::SnapshotInvalidated)
    ));
    // Metadata changes alter the catalog fence without a publication.
    store
        .record_revision_catalog(&object, "old", None, Some("20280101"), 102)
        .await
        .unwrap();
    assert!(matches!(
        store
            .history(
                object.clone(),
                HistoryKind::Revisions,
                Some(cursor),
                1,
                103,
                token()
            )
            .await,
        Err(DatabaseError::SnapshotInvalidated)
    ));
    let page = store
        .history(
            object.clone(),
            HistoryKind::Revisions,
            None,
            1,
            103,
            token(),
        )
        .await
        .unwrap();
    assert_eq!(page.entries[0].revision_id, "old");
    let cursor = page.next_cursor;
    // Body publication alters the independent object-version fence.
    publish(&store, "old", "corrected bytes", 104).await;
    assert!(matches!(
        store
            .history(object, HistoryKind::Revisions, cursor, 1, 105, token())
            .await,
        Err(DatabaseError::SnapshotInvalidated)
    ));
    base.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn corpus_revision_history_corrections_and_eviction_keep_checkpoint_order() {
    let fixture = support::TestDatabase::new().await;
    let base = fixture.open(100).await;
    let blobs = FsBlobStore::open(&fixture.directory.path().join("history-corrections"))
        .await
        .unwrap();
    let store = PgCorpusStore::with_publication_clock(base.pool(), blobs, Arc::new(FixtureClock));
    // A correction to old legal text is the newest observation, while its
    // official checkpoint remains older than the current checkpoint.
    let mut captures = Vec::new();
    for (id, date, body, head, time) in [
        ("old", "20200101", "old original", false, 100),
        ("new", "20260101", "new original", true, 110),
        ("old", "20200101", "old corrected", false, 120),
    ] {
        let mut record = record(id, body);
        record.publication_date = None;
        record.effective_date = Some(date.into());
        let capture = store
            .publish(
                Publication {
                    record,
                    raw: body.as_bytes().to_vec(),
                    additional_evidence: vec![],
                    processor_version: "fixture_v1".into(),
                    retrieved_at: time,
                    now: time,
                    expected_version: store.state(&object()).await.unwrap().version,
                    install_head: head,
                    job_id: None,
                },
                token(),
            )
            .await
            .unwrap();
        captures.push(capture);
    }
    let revision_page = store
        .history(object(), HistoryKind::Revisions, None, 10, 130, token())
        .await
        .unwrap();
    assert_eq!(revision_page.entries[0].revision_id, "new");
    assert_eq!(revision_page.entries[0].sequence, 2);
    assert_eq!(revision_page.entries[1].revision_id, "old");
    assert_eq!(revision_page.entries[1].sequence, 3);
    assert_eq!(
        revision_page.entries[1].capture_id.as_ref(),
        Some(&captures[2].capture_id)
    );
    let capture_page = store
        .history(object(), HistoryKind::Captures, None, 1, 130, token())
        .await
        .unwrap();
    assert_eq!(
        capture_page.entries[0].capture_id.as_ref(),
        Some(&captures[2].capture_id)
    );
    let next = store
        .history(
            object(),
            HistoryKind::Captures,
            capture_page.next_cursor,
            10,
            130,
            token(),
        )
        .await
        .unwrap();
    assert_eq!(
        next.entries.iter().map(|e| e.sequence).collect::<Vec<_>>(),
        [2, 1]
    );
    assert!(next.next_cursor.is_none());
    // Real retirement and index acknowledgment evict old bodies but retain the
    // catalog, dates and chronology. The current body remains protected.
    store
        .acknowledge_index(store.watermark().await.unwrap())
        .await
        .unwrap();
    assert_eq!(store.maintain(300, 250).await.unwrap(), 0);
    store
        .acknowledge_index(store.watermark().await.unwrap())
        .await
        .unwrap();
    assert_eq!(store.maintain(301, 250).await.unwrap(), 2);
    let evicted = store
        .history(object(), HistoryKind::Revisions, None, 10, 302, token())
        .await
        .unwrap();
    assert_eq!(
        evicted
            .entries
            .iter()
            .map(|e| e.revision_id.as_str())
            .collect::<Vec<_>>(),
        ["new", "old"]
    );
    assert_eq!(evicted.entries[1].sequence, 3);
    assert!(evicted.entries[1].capture_id.is_none());
    base.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn corpus_dates_need_complete_inventory_and_unique_revision() {
    let fixture = support::TestDatabase::new().await;
    let base = fixture.open(100).await;
    let blobs = FsBlobStore::open(&fixture.directory.path().join("corpus"))
        .await
        .unwrap();
    let store = PgCorpusStore::with_publication_clock(
        base.pool(),
        blobs,
        std::sync::Arc::new(FixtureClock),
    );
    publish(&store, "r1", "A", 100).await;
    let selector = RevisionSelector::PublicationDate {
        date: "20260101".into(),
    };
    assert!(matches!(
        store
            .resolve(object(), selector.clone(), 110, token())
            .await,
        Err(DatabaseError::HistoryIncomplete)
    ));
    store
        .mark_inventory_complete(&object(), true)
        .await
        .unwrap();
    assert_eq!(
        store
            .resolve(object(), selector.clone(), 110, token())
            .await
            .unwrap()
            .record
            .revision_id,
        "r1"
    );
    publish(&store, "r2", "B", 120).await;
    assert!(matches!(
        store.resolve(object(), selector, 130, token()).await,
        Err(DatabaseError::AmbiguousRevision)
    ));
    let page = store
        .history(object(), HistoryKind::Revisions, None, 1, 130, token())
        .await
        .unwrap();
    publish(&store, "r3", "C", 140).await;
    assert!(matches!(
        store
            .history(
                object(),
                HistoryKind::Revisions,
                page.next_cursor,
                1,
                150,
                token()
            )
            .await,
        Err(DatabaseError::SnapshotInvalidated)
    ));
    base.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn corpus_jobs_coalesce_and_observed_head_is_pending() {
    let fixture = support::TestDatabase::new().await;
    let base = fixture.open(100).await;
    let blobs = FsBlobStore::open(&fixture.directory.path().join("corpus"))
        .await
        .unwrap();
    let store = PgCorpusStore::with_publication_clock(
        base.pool(),
        blobs,
        std::sync::Arc::new(FixtureClock),
    );
    let first = store.enqueue(object(), "r1".into(), 100).await.unwrap();
    let second = store.enqueue(object(), "r1".into(), 101).await.unwrap();
    assert_eq!(first.id, second.id);
    assert!(matches!(
        store
            .resolve(object(), RevisionSelector::Head, 102, token())
            .await,
        Err(DatabaseError::ProcessingPending)
    ));
    let claimed = store.claim_job(102).await.unwrap().unwrap();
    assert_eq!(claimed.id, first.id);
    assert!(store.claim_job(103).await.unwrap().is_none());
    store
        .publish(
            Publication {
                additional_evidence: Vec::new(),
                record: record("r1", "A"),
                raw: b"A".to_vec(),
                processor_version: "fixture_v1".into(),
                retrieved_at: 103,
                now: 103,
                expected_version: claimed.expected_version,
                install_head: true,
                job_id: Some(claimed.id),
            },
            token(),
        )
        .await
        .unwrap();
    assert!(store.claim_job(104).await.unwrap().is_none());
    assert_eq!(
        store
            .resolve(object(), RevisionSelector::Head, 104, token())
            .await
            .unwrap()
            .record
            .body,
        "A"
    );
    base.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn corpus_index_ack_and_session_pins_precede_physical_retention() {
    let fixture = support::TestDatabase::new().await;
    let base = fixture.open(100).await;
    let blobs = FsBlobStore::open(&fixture.directory.path().join("corpus"))
        .await
        .unwrap();
    let store = PgCorpusStore::with_publication_clock(
        base.pool(),
        blobs,
        std::sync::Arc::new(FixtureClock),
    );
    let first = publish(&store, "r1", "A", 100).await;
    publish(&store, "r2", "B", 200).await;
    let watermark = store.watermark().await.unwrap();
    store.acknowledge_index(watermark).await.unwrap();
    let session = "a".repeat(64);
    store
        .pin_session(session.clone(), watermark, vec![], 250)
        .await
        .unwrap();
    assert_eq!(store.maintain(300, 250).await.unwrap(), 0);
    let events = store.outbox(watermark, 10).await.unwrap();
    assert_eq!(events.len(), 1);
    assert!(events[0].removed);
    assert_eq!(events[0].capture_id.as_ref(), Some(&first.capture_id));
    store.acknowledge_index(events[0].sequence).await.unwrap();
    assert_eq!(store.maintain(301, 250).await.unwrap(), 0);
    store.release_session(&session).await.unwrap();
    assert_eq!(store.maintain(302, 250).await.unwrap(), 1);
    assert!(matches!(
        store
            .resolve(
                object(),
                RevisionSelector::Capture {
                    id: first.capture_id
                },
                303,
                token()
            )
            .await,
        Err(DatabaseError::RevisionUnavailable)
    ));
    assert_eq!(
        store
            .resolve(object(), RevisionSelector::Head, 303, token())
            .await
            .unwrap()
            .record
            .revision_id,
        "r2"
    );
    let history = store
        .history(object(), HistoryKind::Revisions, None, 10, 303, token())
        .await
        .unwrap();
    assert_eq!(history.entries.len(), 2);
    assert!(
        history
            .entries
            .iter()
            .any(|e| e.revision_id == "r1" && e.capture_id.is_none())
    );
    let watermark = store.watermark().await.unwrap();
    store
        .pin_session("b".repeat(64), watermark, vec![], 304)
        .await
        .unwrap();
    let resolved = store
        .resolve(object(), RevisionSelector::Head, 304, token())
        .await
        .unwrap();
    let version = store.state(&object()).await.unwrap().version;
    store.withdraw(&object(), version, 305).await.unwrap();
    assert!(matches!(
        store
            .pin_session(
                "c".repeat(64),
                store.watermark().await.unwrap(),
                vec![resolved.capture_id],
                306
            )
            .await,
        Err(DatabaseError::SnapshotInvalidated)
    ));

    assert!(matches!(
        store.check_session(&"b".repeat(64), 306).await,
        Err(DatabaseError::SnapshotInvalidated)
    ));
    assert!(matches!(
        store
            .resolve(object(), RevisionSelector::Head, 306, token())
            .await,
        Err(DatabaseError::Withdrawn)
    ));
    base.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn corpus_attachment_evidence_catalog_and_index_replay() {
    let fixture = support::TestDatabase::new().await;
    let base = fixture.open(100).await;
    let blobs = FsBlobStore::open(&fixture.directory.path().join("corpus"))
        .await
        .unwrap();
    let store = PgCorpusStore::with_publication_clock(
        base.pool(),
        blobs,
        std::sync::Arc::new(FixtureClock),
    );
    store
        .record_revision_catalog(&object(), "uncaptured", Some("20250101"), None, 90)
        .await
        .unwrap();
    let catalog = store
        .history(object(), HistoryKind::Revisions, None, 10, 100, token())
        .await
        .unwrap();
    assert_eq!(catalog.entries.len(), 1);
    assert!(catalog.entries[0].capture_id.is_none());
    assert!(catalog.entries[0].captured_at.is_none());
    let bytes = b"fictional pdf evidence".to_vec();
    let hash: String = Sha256::digest(&bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    let mut r = record("r1", "provider body");
    r.sections.push(LegalSection {
        id: "attachment:1".into(),
        title: "Fictional attachment".into(),
        text: "extracted text".into(),
        kind: SectionKind::Extracted,
        source_document_sha256: Some(hash),
        page: Some(1),
    });
    let a = store
        .publish(
            Publication {
                record: r,
                raw: b"provider body".to_vec(),
                additional_evidence: vec![bytes],
                processor_version: "fixture_v1".into(),
                retrieved_at: 100,
                now: 100,
                expected_version: store.state(&object()).await.unwrap().version,
                install_head: true,
                job_id: None,
            },
            token(),
        )
        .await
        .unwrap();
    let first = store.outbox(0, 10).await.unwrap().remove(0);
    assert_eq!(
        store
            .index_capture(&first, token())
            .await
            .unwrap()
            .unwrap()
            .capture_id,
        a.capture_id
    );
    publish(&store, "r2", "next", 200).await;
    assert!(matches!(
        store
            .resolve(
                object(),
                RevisionSelector::Capture {
                    id: a.capture_id.clone()
                },
                3_000_000,
                token()
            )
            .await,
        Err(DatabaseError::RevisionUnavailable)
    ));
    assert!(
        store
            .index_capture(&first, token())
            .await
            .unwrap()
            .is_some()
    );
    store.maintain(3_000_000, 250).await.unwrap();
    store
        .acknowledge_index(store.watermark().await.unwrap())
        .await
        .unwrap();
    assert_eq!(store.maintain(3_000_001, 250).await.unwrap(), 1);
    assert!(
        store
            .index_capture(&first, token())
            .await
            .unwrap()
            .is_none()
    );
    let metadata = store
        .resolve_metadata(
            object(),
            RevisionSelector::Capture {
                id: a.capture_id.clone(),
            },
            3_000_001,
            token(),
        )
        .await
        .unwrap();
    assert_eq!(metadata.revision_id, "r1");
    let captures = store
        .history(
            object(),
            HistoryKind::Captures,
            None,
            10,
            3_000_001,
            token(),
        )
        .await
        .unwrap();
    assert_eq!(captures.entries.len(), 2);
    let left: i64 = sqlx::query_scalar("SELECT count(*) FROM openlegal.corpus_capture_blob")
        .fetch_one(&base.pool())
        .await
        .unwrap();
    assert_eq!(left, 0);
    assert!(store.healthy());
    base.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn corpus_claim_fences_dead_workers_and_superseded_head_jobs() {
    let fixture = support::TestDatabase::new().await;
    let base = fixture.open(100).await;
    let blobs = FsBlobStore::open(&fixture.directory.path().join("corpus"))
        .await
        .unwrap();
    let store = PgCorpusStore::with_publication_clock(
        base.pool(),
        blobs,
        std::sync::Arc::new(FixtureClock),
    );
    store.enqueue(object(), "r1".into(), 100).await.unwrap();
    let dead = store.claim_job(101).await.unwrap().unwrap();
    let retry = store.claim_job(702).await.unwrap().unwrap();
    assert_eq!(dead.id, retry.id);
    assert!(retry.expected_version > dead.expected_version);
    store.fail_claim(&dead, false).await.unwrap();
    assert!(store.claim_job(703).await.unwrap().is_none());
    store.enqueue(object(), "r2".into(), 704).await.unwrap();
    let replacement = store.claim_job(705).await.unwrap().unwrap();
    assert_eq!(replacement.revision_id, "r2");
    assert!(matches!(
        store
            .publish(
                Publication {
                    record: record("r1", "obsolete"),
                    raw: b"obsolete".to_vec(),
                    additional_evidence: vec![],
                    processor_version: "fixture_v1".into(),
                    retrieved_at: 706,
                    now: 706,
                    expected_version: retry.expected_version,
                    install_head: true,
                    job_id: Some(retry.id)
                },
                token()
            )
            .await,
        Err(DatabaseError::Conflict)
    ));
    base.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn corpus_queue_capacity_preserves_observed_head_replacement() {
    let fixture = support::TestDatabase::new().await;
    let base = fixture.open(100).await;
    let blobs = FsBlobStore::open(&fixture.directory.path().join("corpus"))
        .await
        .unwrap();
    let store = PgCorpusStore::with_publication_clock(
        base.pool(),
        blobs,
        std::sync::Arc::new(FixtureClock),
    );
    let old = publish(&store, "old", "old body", 100).await;
    let previous = store.state(&object()).await.unwrap();
    for n in 0..128 {
        let mut other = object();
        other.id = format!("queued_{n}");
        store
            .enqueue_job(other, "history".into(), None, false, false, 101)
            .await
            .unwrap();
    }
    assert!(matches!(
        store
            .enqueue_job(object(), "replacement".into(), None, true, true, 102)
            .await,
        Err(DatabaseError::Capacity)
    ));
    let after = store.state(&object()).await.unwrap();
    assert!(after.pending);
    assert!(after.version > previous.version);
    assert_eq!(after.head_capture, Some(old.capture_id.clone()));
    assert!(matches!(
        store
            .resolve(object(), RevisionSelector::Head, 103, token())
            .await,
        Err(DatabaseError::CollectionIncomplete)
    ));
    assert!(matches!(
        store
            .revalidate(&object(), &old.capture_id, previous.version, 103)
            .await,
        Err(DatabaseError::Conflict)
    ));
    let desired: String = sqlx::query_scalar(
        "SELECT desired_head_revision FROM openlegal.corpus_object WHERE identity->>'id'='001'",
    )
    .fetch_one(&base.pool())
    .await
    .unwrap();
    assert_eq!(desired, "replacement");
    base.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn corpus_catalog_refresh_and_historical_enqueue_preserve_running_claim() {
    let fixture = support::TestDatabase::new().await;
    let base = fixture.open(100).await;
    let blobs = FsBlobStore::open(&fixture.directory.path().join("corpus"))
        .await
        .unwrap();
    let store = PgCorpusStore::with_publication_clock(
        base.pool(),
        blobs,
        std::sync::Arc::new(FixtureClock),
    );
    let mut o = object();
    o.provider = "law_go_kr".into();
    store
        .record_revision_catalog(&o, "r1", Some("20260101"), Some("20260201"), 100)
        .await
        .unwrap();
    store
        .enqueue_job(o.clone(), "r1".into(), None, true, true, 101)
        .await
        .unwrap();
    let claim = store.claim_job(102).await.unwrap().unwrap();
    let before = store.state(&o).await.unwrap();
    assert!(!store.current_coverage_ready(102).await.unwrap());
    store
        .record_revision_catalog(&o, "r1", Some("20260101"), Some("20260201"), 103)
        .await
        .unwrap();
    assert_eq!(
        store.state(&o).await.unwrap().catalog_version,
        before.catalog_version
    );
    store
        .mark_dataset_inventory_complete(Dataset::NationalStatute, true)
        .await
        .unwrap();
    store
        .mark_dataset_inventory_complete(Dataset::NationalStatute, false)
        .await
        .unwrap();
    store
        .record_revision_catalog(&o, "older", Some("20250101"), None, 104)
        .await
        .unwrap();
    store
        .enqueue_job(o.clone(), "older".into(), None, false, false, 104)
        .await
        .unwrap();
    assert_eq!(
        store.state(&o).await.unwrap().version,
        claim.expected_version
    );
    let mut r = record("r1", "normalized");
    r.object = o.clone();
    store
        .publish(
            Publication {
                record: r,
                raw: b"normalized".to_vec(),
                additional_evidence: vec![],
                processor_version: "fixture_v1".into(),
                retrieved_at: 105,
                now: 105,
                expected_version: claim.expected_version,
                install_head: true,
                job_id: Some(claim.id),
            },
            token(),
        )
        .await
        .unwrap();
    assert!(store.current_coverage_ready(106).await.unwrap());
    assert!(!store.current_coverage_ready(105 + 86400).await.unwrap());
    assert_eq!(
        store
            .resolve(o, RevisionSelector::Head, 106, token())
            .await
            .unwrap()
            .record
            .body,
        "normalized"
    );
    base.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn corpus_raw_admission_counts_outstanding_staging_reservations() {
    let fixture = support::TestDatabase::new().await;
    let base = fixture.open(100).await;
    let blobs = FsBlobStore::open(&fixture.directory.path().join("corpus"))
        .await
        .unwrap();
    let store = PgCorpusStore::with_publication_clock(
        base.pool(),
        blobs,
        std::sync::Arc::new(FixtureClock),
    );
    sqlx::query("UPDATE openlegal.corpus_control SET raw_bytes=$1,staged_bytes=5")
        .bind(1024_i64 * 1024 * 1024 * 1024 - 5)
        .execute(&base.pool())
        .await
        .unwrap();
    let result = store
        .publish(
            Publication {
                record: record("r1", "A"),
                raw: vec![b'A'],
                additional_evidence: vec![],
                processor_version: "fixture_v1".into(),
                retrieved_at: 100,
                now: 100,
                expected_version: 0,
                install_head: true,
                job_id: None,
            },
            token(),
        )
        .await;
    let staged: i64 = sqlx::query_scalar("SELECT count(*) FROM openlegal.corpus_staging")
        .fetch_one(&base.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE openlegal.corpus_control SET raw_bytes=0,staged_bytes=0")
        .execute(&base.pool())
        .await
        .unwrap();
    assert!(matches!(result, Err(DatabaseError::Capacity)));
    assert_eq!(staged, 0);
    base.close().await.unwrap();
}
