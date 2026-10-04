//! Supplemental pages survive restart, retain exact bytes and use fenced leases.
#[path = "../../../test-support/postgres.rs"]
mod support;
use openlegal_adapters::{
    blob::FsBlobStore,
    corpus::{PgCorpusStore, SourceObservationInput, SupplementJobStatus},
    law_go_kr::supplements::{self, SupplementRequest, SupplementSeed, SupplementSource},
    postgres::PostgresStore,
};
use openlegal_application::persistence::PersistentStore;
use openlegal_domain::{
    legal::{DatabaseError, Dataset, ObjectId},
    rights::SourceRights,
};
use serde_json::Value;
use std::{collections::BTreeMap, sync::Arc};
use tokio_util::sync::CancellationToken;

async fn setup() -> (support::TestDatabase, Arc<PostgresStore>, PgCorpusStore) {
    let fixture = support::TestDatabase::new().await;
    let base = fixture.open(100).await;
    let blobs = FsBlobStore::open(&fixture.directory.path().join("supplement-jobs"))
        .await
        .unwrap();
    let store = PgCorpusStore::new(base.pool(), blobs);
    (fixture, base, store)
}
fn global(source: SupplementSource) -> SupplementRequest {
    supplements::request(source, SupplementSeed::Global, 1).unwrap()
}
fn record(source: SupplementSource, number: u32) -> SupplementRequest {
    supplements::request(
        source,
        SupplementSeed::Record {
            object: ObjectId {
                jurisdiction: "kr".into(),
                provider: "law_go_kr".into(),
                dataset: Dataset::NationalStatute,
                id: number.to_string(),
            },
            record_number: number.to_string(),
        },
        1,
    )
    .unwrap()
}
async fn retain(store: &PgCorpusStore, request: &SupplementRequest, now: u64) -> String {
    store
        .retain_source_observation(
            SourceObservationInput {
                source_key: request.observation_key().unwrap(),
                raw: Some(b"<list>unchanged exact evidence</list>".to_vec()),
                media_type: "application/xml".into(),
                rights: SourceRights::legal_information(),
                metadata: BTreeMap::new(),
                observed_at: now,
            },
            CancellationToken::new(),
        )
        .await
        .unwrap()
        .observation_id
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn supplement_queue_is_durable_deduplicated_and_has_no_128_seed_drop_limit() {
    let (fixture, base, store) = setup().await;
    let request = global(SupplementSource::StatuteAnnexInventory);
    assert!(store.enqueue_supplement(&request, 100).await.unwrap());
    assert!(!store.enqueue_supplement(&request, 101).await.unwrap());
    for number in 1..=130 {
        assert!(
            store
                .enqueue_supplement(&record(SupplementSource::RelatedStatutes, number), 100)
                .await
                .unwrap()
        );
    }
    let descriptor: Value = sqlx::query_scalar(
        "SELECT descriptor FROM openlegal.provider_supplement_job WHERE job_key=$1",
    )
    .bind(request.observation_key().unwrap())
    .fetch_one(&base.pool())
    .await
    .unwrap();
    assert_eq!(descriptor.as_array().unwrap().len(), 3);
    assert_eq!(descriptor[0], "statute_annex_inventory");
    assert_eq!(descriptor[1], serde_json::json!({"kind":"global"}));
    assert_eq!(descriptor[2], 1);
    assert!(!descriptor.to_string().contains("https:"));
    assert_eq!(
        store.supplement_progress().await.unwrap()["pending_or_running"],
        131
    );
    let blobs = FsBlobStore::open(&fixture.directory.path().join("supplement-jobs"))
        .await
        .unwrap();
    let restarted = PgCorpusStore::new(base.pool(), blobs);
    assert_eq!(
        restarted
            .claim_supplement(102)
            .await
            .unwrap()
            .unwrap()
            .request
            .source(),
        SupplementSource::StatuteAnnexInventory
    );
    assert!(sqlx::query("UPDATE openlegal.provider_supplement_job SET descriptor='[\"related_statutes\",{\"kind\":\"global\"},1]' WHERE job_key=$1")
        .bind(request.observation_key().unwrap()).execute(&base.pool()).await.is_err());
    base.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn expired_supplement_owner_cannot_settle_a_reclaimed_job() {
    let (_fixture, base, store) = setup().await;
    let request = global(SupplementSource::StatuteAnnexInventory);
    store.enqueue_supplement(&request, 100).await.unwrap();
    let old = store.claim_supplement(100).await.unwrap().unwrap();
    assert_eq!(old.lease_until, 700);
    let id = retain(&store, &request, 101).await;
    assert!(store.claim_supplement(699).await.unwrap().is_none());
    let current = store.claim_supplement(700).await.unwrap().unwrap();
    assert_ne!(old.lease_owner, current.lease_owner);
    assert_eq!(current.observation_id.as_deref(), Some(id.as_str()));
    assert_eq!(
        store
            .settle_supplement(&old, SupplementJobStatus::Done, Some(&id), 1, 701)
            .await
            .err(),
        Some(DatabaseError::Conflict)
    );
    store
        .settle_supplement(&current, SupplementJobStatus::Done, Some(&id), 1, 701)
        .await
        .unwrap();
    assert_eq!(store.supplement_progress().await.unwrap()["done"], 1);
    base.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn incomplete_retries_reuse_evidence_and_daily_revalidation_requires_fresh_observation() {
    let (_fixture, base, store) = setup().await;
    let request = global(SupplementSource::StatuteAnnexInventory);
    store.enqueue_supplement(&request, 100).await.unwrap();
    let job = store.claim_supplement(100).await.unwrap().unwrap();
    let id = retain(&store, &request, 101).await;
    store
        .settle_supplement(&job, SupplementJobStatus::Incomplete, Some(&id), 0, 200)
        .await
        .unwrap();
    assert_eq!(store.supplement_progress().await.unwrap()["incomplete"], 1);
    assert!(store.claim_supplement(3799).await.unwrap().is_none());
    let retry = store.claim_supplement(3800).await.unwrap().unwrap();
    assert_eq!(retry.observation_id.as_deref(), Some(id.as_str()));
    assert_eq!(
        store
            .source_observation_bytes(&id, CancellationToken::new())
            .await
            .unwrap(),
        b"<list>unchanged exact evidence</list>"
    );
    store
        .settle_supplement(&retry, SupplementJobStatus::Done, Some(&id), 1, 3801)
        .await
        .unwrap();
    assert!(!store.enqueue_supplement(&request, 90200).await.unwrap());
    assert!(store.enqueue_supplement(&request, 90201).await.unwrap());
    let fresh = store.claim_supplement(90201).await.unwrap().unwrap();
    assert!(fresh.observation_id.is_none());
    assert_eq!(
        store
            .settle_supplement(&fresh, SupplementJobStatus::Done, Some(&id), 1, 90202)
            .await
            .err(),
        Some(DatabaseError::InvalidInput)
    );
    // Same bytes refresh validated_at while keeping the original observation ID.
    assert_eq!(retain(&store, &request, 90202).await, id);
    let recovered = store.claim_supplement(90801).await.unwrap().unwrap();
    assert_eq!(recovered.observation_id.as_deref(), Some(id.as_str()));
    store
        .settle_supplement(&recovered, SupplementJobStatus::Done, Some(&id), 1, 90802)
        .await
        .unwrap();
    base.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn supplement_claims_alternate_global_and_seed_work_then_rotate_sources() {
    let (_fixture, base, store) = setup().await;
    for source in [
        SupplementSource::StatuteAnnexInventory,
        SupplementSource::OrdinanceAnnexInventory,
    ] {
        store
            .enqueue_supplement(&global(source), 100)
            .await
            .unwrap();
    }
    for number in 1..=20 {
        store
            .enqueue_supplement(&record(SupplementSource::RelatedStatutes, number), 100)
            .await
            .unwrap();
    }
    store
        .enqueue_supplement(&record(SupplementSource::StatuteHierarchy, 100), 100)
        .await
        .unwrap();
    let a = store.claim_supplement(100).await.unwrap().unwrap();
    let b = store.claim_supplement(100).await.unwrap().unwrap();
    let c = store.claim_supplement(100).await.unwrap().unwrap();
    let d = store.claim_supplement(100).await.unwrap().unwrap();
    assert!(matches!(a.request.seed(), SupplementSeed::Global));
    assert!(!matches!(b.request.seed(), SupplementSeed::Global));
    assert!(matches!(c.request.seed(), SupplementSeed::Global));
    assert!(!matches!(d.request.seed(), SupplementSeed::Global));
    assert_ne!(a.request.source(), c.request.source());
    assert_ne!(b.request.source(), d.request.source());
    base.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn page_settlement_and_successor_are_atomic_and_counts_resume_cumulatively() {
    let (_fixture, base, store) = setup().await;
    let request = global(SupplementSource::StatuteAnnexInventory);
    store.enqueue_supplement(&request, 100).await.unwrap();
    let first = store.claim_supplement(100).await.unwrap().unwrap();
    let id = retain(&store, &request, 101).await;
    let second = request.next_page().unwrap();
    let wrong = global(SupplementSource::OrdinanceAnnexInventory);
    assert_eq!(
        store
            .settle_supplement_with_successor(
                &first,
                SupplementJobStatus::Done,
                Some(&id),
                100,
                101,
                Some(&wrong)
            )
            .await
            .err(),
        Some(DatabaseError::InvalidInput)
    );
    assert_eq!(
        store.supplement_progress().await.unwrap()["pending_or_running"],
        1
    );
    store
        .settle_supplement_with_successor(
            &first,
            SupplementJobStatus::Done,
            Some(&id),
            100,
            101,
            Some(&second),
        )
        .await
        .unwrap();
    let job = store.claim_supplement(102).await.unwrap().unwrap();
    assert_eq!(job.request.page(), 2);
    assert_eq!(job.observed_before, 100);
    let id = retain(&store, &second, 102).await;
    let third = second.next_page().unwrap();
    store
        .settle_supplement_with_successor(
            &job,
            SupplementJobStatus::Done,
            Some(&id),
            7,
            103,
            Some(&third),
        )
        .await
        .unwrap();
    assert_eq!(
        store
            .claim_supplement(104)
            .await
            .unwrap()
            .unwrap()
            .observed_before,
        107
    );
    base.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn wrong_source_observation_is_rejected_and_deferred_jobs_remain_visible() {
    let (_fixture, base, store) = setup().await;
    let request = global(SupplementSource::StatuteAnnexInventory);
    store.enqueue_supplement(&request, 100).await.unwrap();
    let job = store.claim_supplement(100).await.unwrap().unwrap();
    let other = global(SupplementSource::OrdinanceAnnexInventory);
    let wrong_id = retain(&store, &other, 101).await;
    assert_eq!(
        store
            .settle_supplement(&job, SupplementJobStatus::Done, Some(&wrong_id), 1, 102)
            .await
            .err(),
        Some(DatabaseError::InvalidInput)
    );
    store
        .settle_supplement(&job, SupplementJobStatus::Pending, None, 0, 103)
        .await
        .unwrap();
    let metadata = record(SupplementSource::StatuteOverview, 100);
    store.enqueue_supplement(&metadata, 104).await.unwrap();
    // A crash after metadata retention must resume the Deferred path without
    // attempting to read nonexistent original bytes.
    store
        .retain_source_observation(
            SourceObservationInput {
                source_key: metadata.observation_key().unwrap(),
                raw: None,
                media_type: "application/xml".into(),
                rights: SourceRights::default(),
                metadata: BTreeMap::new(),
                observed_at: 104,
            },
            CancellationToken::new(),
        )
        .await
        .unwrap();
    let job = store.claim_supplement(104).await.unwrap().unwrap();
    assert!(job.observation_id.is_none());
    assert_eq!(job.request.source(), SupplementSource::StatuteOverview);
    assert_eq!(
        store
            .settle_supplement(&job, SupplementJobStatus::Done, Some(&wrong_id), 1, 105)
            .await
            .err(),
        Some(DatabaseError::InvalidInput)
    );
    store
        .settle_supplement(&job, SupplementJobStatus::Deferred, None, 0, 105)
        .await
        .unwrap();
    let progress = store.supplement_progress().await.unwrap();
    assert_eq!(progress["deferred"], 1);
    assert_eq!(progress["pending_or_running"], 1);
    base.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn supplement_completion_requires_every_global_root_and_no_owed_work() {
    let (_fixture, base, store) = setup().await;
    assert_eq!(
        store.supplement_progress().await.unwrap()["complete"],
        false
    );
    let globals = supplements::global_requests(1).unwrap();
    for request in &globals {
        store.enqueue_supplement(request, 100).await.unwrap();
        retain(&store, request, 101).await;
    }
    for index in 0..globals.len() {
        assert_eq!(
            store.supplement_progress().await.unwrap()["complete"],
            false
        );
        let job = store.claim_supplement(102).await.unwrap().unwrap();
        store
            .settle_supplement(
                &job,
                SupplementJobStatus::Done,
                job.observation_id.as_deref(),
                1,
                103,
            )
            .await
            .unwrap();
        assert_eq!(
            store.supplement_progress().await.unwrap()["completed_global_roots"],
            index + 1
        );
    }
    assert_eq!(store.supplement_progress().await.unwrap()["complete"], true);
    let request = record(SupplementSource::RelatedStatutes, 100);
    store.enqueue_supplement(&request, 104).await.unwrap();
    assert_eq!(
        store.supplement_progress().await.unwrap()["complete"],
        false
    );
    let job = store.claim_supplement(104).await.unwrap().unwrap();
    store
        .settle_supplement(&job, SupplementJobStatus::Incomplete, None, 0, 105)
        .await
        .unwrap();
    assert_eq!(
        store.supplement_progress().await.unwrap()["complete"],
        false
    );
    base.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn incomplete_evidence_is_reused_before_24h_then_refetched_without_losing_old_bytes() {
    let (_fixture, base, store) = setup().await;
    let request = global(SupplementSource::StatuteAnnexInventory);
    store.enqueue_supplement(&request, 100).await.unwrap();
    let first = store.claim_supplement(100).await.unwrap().unwrap();
    let original_id = retain(&store, &request, 101).await;
    store
        .settle_supplement(
            &first,
            SupplementJobStatus::Incomplete,
            Some(&original_id),
            0,
            200,
        )
        .await
        .unwrap();

    let retry = store.claim_supplement(3800).await.unwrap().unwrap();
    assert_eq!(retry.observation_id.as_deref(), Some(original_id.as_str()));
    store
        .settle_supplement(
            &retry,
            SupplementJobStatus::Incomplete,
            Some(&original_id),
            0,
            3801,
        )
        .await
        .unwrap();
    let last_retry = store.claim_supplement(82900).await.unwrap().unwrap();
    assert_eq!(
        last_retry.observation_id.as_deref(),
        Some(original_id.as_str())
    );
    store
        .settle_supplement(
            &last_retry,
            SupplementJobStatus::Incomplete,
            Some(&original_id),
            0,
            82900,
        )
        .await
        .unwrap();
    assert!(store.claim_supplement(86499).await.unwrap().is_none());

    // Exactly one day since the initial request, despite a recent parser retry.
    let refresh = store.claim_supplement(86500).await.unwrap().unwrap();
    assert!(refresh.observation_id.is_none());
    let epoch: String = sqlx::query_scalar(
        "SELECT requested_at::text FROM openlegal.provider_supplement_job WHERE job_key=$1",
    )
    .bind(&refresh.key)
    .fetch_one(&base.pool())
    .await
    .unwrap();
    assert_eq!(epoch, "86500");
    assert_eq!(
        store
            .settle_supplement(
                &refresh,
                SupplementJobStatus::Done,
                Some(&original_id),
                1,
                86501
            )
            .await
            .err(),
        Some(DatabaseError::InvalidInput)
    );
    // A failed fresh download must not resurrect the old observation fallback.
    store
        .settle_supplement(&refresh, SupplementJobStatus::Incomplete, None, 0, 86501)
        .await
        .unwrap();
    let refreshed_retry = store.claim_supplement(90101).await.unwrap().unwrap();
    assert!(refreshed_retry.observation_id.is_none());

    let corrected = store
        .retain_source_observation(
            SourceObservationInput {
                source_key: request.observation_key().unwrap(),
                raw: Some(b"<list>corrected valid response</list>".to_vec()),
                media_type: "application/xml".into(),
                rights: SourceRights::legal_information(),
                metadata: BTreeMap::new(),
                observed_at: 90102,
            },
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_ne!(corrected.observation_id, original_id);
    store
        .settle_supplement(
            &refreshed_retry,
            SupplementJobStatus::Done,
            Some(&corrected.observation_id),
            1,
            90103,
        )
        .await
        .unwrap();
    assert_eq!(
        store
            .source_observation_bytes(&original_id, CancellationToken::new())
            .await
            .unwrap(),
        b"<list>unchanged exact evidence</list>"
    );
    assert_eq!(
        store
            .source_observation_bytes(&corrected.observation_id, CancellationToken::new())
            .await
            .unwrap(),
        b"<list>corrected valid response</list>"
    );
    assert_eq!(store.supplement_progress().await.unwrap()["incomplete"], 0);
    assert_eq!(store.supplement_progress().await.unwrap()["done"], 1);
    base.close().await.unwrap();
}
