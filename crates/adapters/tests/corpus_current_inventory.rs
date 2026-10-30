//! Real PostgreSQL evidence binding and durable current-inventory ambiguity fences.
#[path = "../../../test-support/postgres.rs"]
mod support;
use openlegal_adapters::{
    blob::FsBlobStore,
    corpus::{CloneView, PgCorpusStore, SourceObservationInput},
    law_go_kr::{InventoryItem, InventoryPage, catalog},
};
use openlegal_application::{
    database::{DatabaseStore, Publication},
    persistence::PersistentStore,
};
use openlegal_domain::{legal::*, rights::SourceRights};
use serde_json::Value;
use std::{collections::BTreeMap, sync::Arc};
use tokio_util::sync::CancellationToken;

struct FixtureClock;
impl openlegal_application::Clock for FixtureClock {
    fn now(&self) -> u64 {
        0
    }
}
fn view() -> CloneView {
    CloneView {
        dataset: Dataset::SchoolRule,
        historical: false,
        treaty_class: None,
    }
}
fn item(id: &str, revision: &str, effective: Option<&str>) -> InventoryItem {
    InventoryItem {
        object: ObjectId {
            jurisdiction: "kr".into(),
            provider: "law_go_kr".into(),
            dataset: Dataset::SchoolRule,
            id: id.into(),
        },
        revision_id: revision.into(),
        effective_date: effective.map(str::to_owned),
        publication_date: None,
        title: format!("Fictional rule {id}"),
        data_source: None,
        case_number: None,
        treaty_class_code: None,
        amendment_type: None,
    }
}
fn page(items: Vec<InventoryItem>) -> InventoryPage {
    InventoryPage {
        source_evidence: None,
        source_observation: None,
        total: Some(items.len() as u64),
        items,
        done: true,
        rejected_rows: 0,
        incomplete: false,
    }
}
async fn bind(
    store: &PgCorpusStore,
    number: u32,
    mut result: InventoryPage,
    now: u64,
) -> InventoryPage {
    let mut metadata = result.current_revision_metadata().unwrap();
    metadata.extend(BTreeMap::from([
        ("dataset".into(), "school_rule".into()),
        ("historical".into(), "false".into()),
        ("page".into(), number.to_string()),
    ]));
    result.source_observation = Some(
        store
            .retain_source_observation(
                SourceObservationInput {
                    source_key: format!(
                        "law_go_kr:{}:school_rule:false:0:{number}:current_revisions_v1",
                        catalog::source_family(Dataset::SchoolRule).list_guide
                    ),
                    raw: None,
                    media_type: "application/xml".into(),
                    rights: SourceRights::default(),
                    metadata,
                    observed_at: now,
                },
                CancellationToken::new(),
            )
            .await
            .unwrap(),
    );
    result
}
async fn publish(
    store: &PgCorpusStore,
    candidate: &InventoryItem,
    expected_version: u64,
    job_id: Option<String>,
    now: u64,
) -> Result<Capture, DatabaseError> {
    store
        .publish(
            Publication {
                record: LegalRecord {
                    object: candidate.object.clone(),
                    revision_id: candidate.revision_id.clone(),
                    title: candidate.title.clone(),
                    body: "Fictional original body".into(),
                    metadata: BTreeMap::new(),
                    publication_date: None,
                    effective_date: candidate.effective_date.clone(),
                    source_url: "https://example.test/current-inventory".into(),
                    representation: "provider_text_v1".into(),
                    sections: vec![],
                },
                raw: b"fictional immutable original".to_vec(),
                additional_evidence: vec![],
                processor_version: "fixture-v1".into(),
                retrieved_at: now,
                now,
                expected_version,
                install_head: true,
                job_id,
            },
            CancellationToken::new(),
        )
        .await
}

async fn cleanup_snapshot(pool: &sqlx::PgPool) -> Value {
    sqlx::query_scalar(
        "SELECT jsonb_build_object('jobs',(SELECT jsonb_agg(to_jsonb(j) ORDER BY j.id) FROM openlegal.corpus_job j),'objects',(SELECT jsonb_agg(to_jsonb(o) ORDER BY o.object_key) FROM openlegal.corpus_object o),'budget',(SELECT to_jsonb(b) FROM openlegal.provider_request_budget b),'gaps',(SELECT count(*) FROM openlegal.provider_collection_gap))",
    )
    .fetch_one(pool)
    .await
    .unwrap()
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn quarantined_head_cleanup_preserves_reused_history_claim_and_needs_no_gap_capacity() {
    let fixture = support::TestDatabase::new().await;
    let base = fixture.open(100).await;
    let pool = base.pool();
    let blobs = FsBlobStore::open(&fixture.directory.path().join("current-late-cleanup"))
        .await
        .unwrap();
    let store = PgCorpusStore::new(pool.clone(), blobs);
    let a = item("2092170", "1001", None);
    store
        .enqueue_job(
            a.object.clone(),
            a.revision_id.clone(),
            None,
            true,
            true,
            100,
        )
        .await
        .unwrap();
    let head = store.claim_job(101).await.unwrap().unwrap();
    let conflicting = bind(
        &store,
        1,
        page(vec![a.clone(), item("2092170", "1002", None)]),
        102,
    )
    .await;
    assert_eq!(
        store
            .observe_current_inventory(view(), 1, &conflicting, 102)
            .await
            .unwrap(),
        vec![a.object.clone()]
    );
    let quarantined = cleanup_snapshot(&pool).await;
    assert_eq!(quarantined["jobs"][0]["status"], "failed");
    assert_eq!(
        quarantined["jobs"][0]["error_category"],
        "source_data_invalid"
    );
    for reason in [
        "source_unavailable",
        "source_data_invalid",
        "download_failed",
        "index_input_limit",
    ] {
        store.skip_claim(&head, reason, 103).await.unwrap();
    }
    assert_eq!(cleanup_snapshot(&pool).await, quarantined);

    // Quarantine must leave another object's work claimable.
    let b = item("2092171", "2001", None);
    store
        .enqueue_job(b.object.clone(), b.revision_id, None, true, true, 104)
        .await
        .unwrap();
    let unrelated = store.claim_job(105).await.unwrap().unwrap();
    assert_eq!(unrelated.object, b.object);

    // The same-revision history upsert can reuse the old HEAD's job UUID.
    let historical = store
        .enqueue_job(a.object.clone(), a.revision_id, None, false, false, 106)
        .await
        .unwrap();
    assert_eq!(historical.id, head.id);
    let historical = store.claim_job(107).await.unwrap().unwrap();
    assert_eq!(historical.id, head.id);
    assert!(!historical.install_head);
    assert!(historical.expected_version > head.expected_version);

    // No gap is needed for obsolete cleanup, even when all 100000 slots are
    // occupied. Its provider response must drain without a new worker failure.
    sqlx::query("INSERT INTO openlegal.provider_collection_gap(gap_key,dataset,scope,page,reason,first_seen_at,last_seen_at,retry_at) SELECT 'fixture-full-'||n::text,'school_rule','page',n,'source_data_invalid',100,100,3700 FROM generate_series(1,100000) n")
        .execute(&pool).await.unwrap();
    let before = cleanup_snapshot(&pool).await;
    for reason in ["download_failed", "index_input_limit"] {
        store.skip_claim(&head, reason, 108).await.unwrap();
    }
    assert_eq!(cleanup_snapshot(&pool).await, before);
    assert_eq!(before["gaps"], 100000);
    assert!(!store.detail_gap_active(&a.object, "1001").await.unwrap());
    assert_eq!(
        store.skip_claim(&unrelated, "download_failed", 108).await,
        Err(DatabaseError::Capacity)
    );
    assert_eq!(cleanup_snapshot(&pool).await, before);

    // Neither history nor a claim without an older version receives the no-op.
    let mut not_head = head.clone();
    not_head.install_head = false;
    assert_eq!(
        store.skip_claim(&not_head, "download_failed", 109).await,
        Err(DatabaseError::Conflict)
    );
    let mut not_older = head.clone();
    not_older.expected_version = historical.expected_version;
    not_older.attempts += 1;
    assert_eq!(
        store.skip_claim(&not_older, "download_failed", 109).await,
        Err(DatabaseError::Conflict)
    );
    assert_eq!(cleanup_snapshot(&pool).await, before);
    base.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn unheld_stale_and_repeated_index_limit_cleanup_remain_conflicts() {
    let fixture = support::TestDatabase::new().await;
    let base = fixture.open(100).await;
    let pool = base.pool();
    let blobs = FsBlobStore::open(&fixture.directory.path().join("current-normal-cleanup"))
        .await
        .unwrap();
    let store = PgCorpusStore::new(pool.clone(), blobs);
    let a = item("2092170", "1001", None);
    store
        .enqueue_job(a.object, a.revision_id, None, true, true, 100)
        .await
        .unwrap();
    let claimed = store.claim_job(101).await.unwrap().unwrap();
    let before = cleanup_snapshot(&pool).await;
    let mut stale = claimed.clone();
    stale.expected_version += 1;
    assert_eq!(
        store.skip_claim(&stale, "download_failed", 102).await,
        Err(DatabaseError::Conflict)
    );
    assert_eq!(cleanup_snapshot(&pool).await, before);
    store
        .skip_claim(&claimed, "index_input_limit", 103)
        .await
        .unwrap();
    let skipped = cleanup_snapshot(&pool).await;
    assert_eq!(skipped["jobs"][0]["error_category"], "index_input_limit");
    assert_eq!(skipped["gaps"], 1);
    assert_eq!(
        store.skip_claim(&claimed, "index_input_limit", 104).await,
        Err(DatabaseError::Conflict)
    );
    assert_eq!(cleanup_snapshot(&pool).await, skipped);
    base.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn whole_page_registers_interleaved_conflict_before_any_head_claim() {
    let fixture = support::TestDatabase::new().await;
    let base = fixture.open(100).await;
    let blobs = FsBlobStore::open(&fixture.directory.path().join("current-batch"))
        .await
        .unwrap();
    let store = PgCorpusStore::with_publication_clock(base.pool(), blobs, Arc::new(FixtureClock));
    let a = item("2092170", "1001", Some("20181012"));
    let b = item("2092171", "2001", Some("20200101"));
    let a2 = item("2092170", "1002", Some("20161122"));
    let result = bind(&store, 1, page(vec![a.clone(), b.clone(), a2.clone()]), 100).await;
    assert_eq!(
        store
            .observe_current_inventory(view(), 1, &result, 100)
            .await
            .unwrap(),
        vec![a.object.clone()]
    );
    assert!(store.head_inventory_conflicted(&a.object).await.unwrap());
    assert!(!store.head_inventory_conflicted(&b.object).await.unwrap());
    assert!(
        store
            .enqueue_job(
                a.object.clone(),
                a.revision_id.clone(),
                a.effective_date.clone(),
                true,
                true,
                101
            )
            .await
            .is_err()
    );
    store
        .enqueue_job(
            b.object.clone(),
            b.revision_id.clone(),
            b.effective_date.clone(),
            true,
            true,
            101,
        )
        .await
        .unwrap();
    assert_eq!(
        store.claim_job(102).await.unwrap().unwrap().object,
        b.object
    );
    let state = store.state(&a.object).await.unwrap();
    assert!(state.pending);
    assert_eq!(state.version, 1);
    let counts:(i64,i64)=sqlx::query_as("SELECT (SELECT count(*) FROM openlegal.current_inventory_candidate),(SELECT count(*) FROM openlegal.corpus_capture)").fetch_one(&base.pool()).await.unwrap();
    assert_eq!(counts, (3, 0));
    base.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn cross_page_reopen_fences_late_head_and_preserves_history_usage_and_capture() {
    let fixture = support::TestDatabase::new().await;
    let base = fixture.open(100).await;
    let blobs = FsBlobStore::open(&fixture.directory.path().join("current-reopen"))
        .await
        .unwrap();
    let store =
        PgCorpusStore::with_publication_clock(base.pool(), blobs.clone(), Arc::new(FixtureClock));
    let a = item("2092170", "1001", Some("20181012"));
    let original = publish(&store, &a, 0, None, 100).await.unwrap();
    let first = bind(&store, 1, page(vec![a.clone()]), 101).await;
    assert!(
        store
            .observe_current_inventory(view(), 1, &first, 101)
            .await
            .unwrap()
            .is_empty()
    );
    let pending = store
        .enqueue_job(
            a.object.clone(),
            a.revision_id.clone(),
            a.effective_date.clone(),
            true,
            true,
            102,
        )
        .await
        .unwrap();
    let claimed = store.claim_job(103).await.unwrap().unwrap();
    assert_eq!(pending.id, claimed.id);
    let historical = store
        .enqueue_job(
            a.object.clone(),
            "0999".into(),
            Some("20100101".into()),
            false,
            false,
            104,
        )
        .await
        .unwrap();
    let budget: Value =
        sqlx::query_scalar("SELECT to_jsonb(b) FROM openlegal.provider_request_budget b")
            .fetch_one(&base.pool())
            .await
            .unwrap();
    let before = store.state(&a.object).await.unwrap();
    let validated_before: String = sqlx::query_scalar(
        "SELECT validated_at::text FROM openlegal.corpus_object WHERE identity->>'id'='2092170'",
    )
    .fetch_one(&base.pool())
    .await
    .unwrap();
    let restarted =
        PgCorpusStore::with_publication_clock(base.pool(), blobs, Arc::new(FixtureClock));
    let a2 = item("2092170", "1002", Some("20161122"));
    let second = bind(&restarted, 2, page(vec![a2]), 105).await;
    assert_eq!(
        restarted
            .observe_current_inventory(view(), 2, &second, 105)
            .await
            .unwrap(),
        vec![a.object.clone()]
    );
    let after = restarted.state(&a.object).await.unwrap();
    assert_eq!(after.version, before.version + 1);
    assert_eq!(after.head_capture, Some(original.capture_id.clone()));
    let validated_after: String = sqlx::query_scalar(
        "SELECT validated_at::text FROM openlegal.corpus_object WHERE identity->>'id'='2092170'",
    )
    .fetch_one(&base.pool())
    .await
    .unwrap();
    assert_eq!(validated_after, validated_before);
    assert!(
        restarted
            .current_inventory_search_blocked(&a.object)
            .await
            .unwrap()
    );
    let failed: (String, Option<String>, i32) = sqlx::query_as(
        "SELECT status,error_category,attempts FROM openlegal.corpus_job WHERE id=$1::uuid",
    )
    .bind(&claimed.id)
    .fetch_one(&base.pool())
    .await
    .unwrap();
    assert_eq!(
        failed,
        ("failed".into(), Some("source_data_invalid".into()), 1)
    );
    assert_eq!(
        publish(
            &restarted,
            &a,
            claimed.expected_version,
            Some(claimed.id),
            106
        )
        .await
        .unwrap_err(),
        DatabaseError::Conflict
    );
    let historic_status: String =
        sqlx::query_scalar("SELECT status FROM openlegal.corpus_job WHERE id=$1::uuid")
            .bind(&historical.id)
            .fetch_one(&base.pool())
            .await
            .unwrap();
    assert_eq!(historic_status, "pending");
    assert_eq!(
        restarted.claim_job(107).await.unwrap().unwrap().id,
        historical.id
    );
    assert_eq!(
        sqlx::query_scalar::<_, Value>(
            "SELECT to_jsonb(b) FROM openlegal.provider_request_budget b"
        )
        .fetch_one(&base.pool())
        .await
        .unwrap(),
        budget
    );
    assert_eq!(
        restarted
            .resolve(
                a.object.clone(),
                RevisionSelector::Capture {
                    id: original.capture_id.clone()
                },
                107,
                CancellationToken::new()
            )
            .await
            .unwrap()
            .capture_id,
        original.capture_id
    );
    let version = restarted.state(&a.object).await.unwrap().version;
    let later = bind(
        &restarted,
        3,
        page(vec![item("2092170", "1003", None)]),
        108,
    )
    .await;
    assert_eq!(
        restarted
            .observe_current_inventory(view(), 3, &later, 108)
            .await
            .unwrap(),
        vec![a.object.clone()]
    );
    assert_eq!(restarted.state(&a.object).await.unwrap().version, version);
    let count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM openlegal.current_inventory_candidate")
            .fetch_one(&base.pool())
            .await
            .unwrap();
    assert_eq!(count, 2);
    base.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn exact_stored_identity_manifest_required_before_view_or_object_writes() {
    let fixture = support::TestDatabase::new().await;
    let base = fixture.open(100).await;
    let blobs = FsBlobStore::open(&fixture.directory.path().join("current-proof"))
        .await
        .unwrap();
    let store = PgCorpusStore::new(base.pool(), blobs);
    let mut result = bind(&store, 1, page(vec![item("2092170", "1001", None)]), 100).await;
    result.items[0].revision_id = "1002".into();
    assert_eq!(
        store
            .observe_current_inventory(view(), 1, &result, 100)
            .await
            .unwrap_err(),
        DatabaseError::InvalidInput
    );
    let counts:(i64,i64,i64)=sqlx::query_as("SELECT (SELECT count(*) FROM openlegal.corpus_object),(SELECT count(*) FROM openlegal.provider_clone_view),(SELECT count(*) FROM openlegal.current_inventory_candidate)").fetch_one(&base.pool()).await.unwrap();
    assert_eq!(counts, (0, 0, 0));
    result.items[0].revision_id = "1001".into();
    assert_eq!(
        store
            .observe_current_inventory(view(), 2, &result, 100)
            .await
            .unwrap_err(),
        DatabaseError::InvalidInput
    );
    let historical = CloneView {
        historical: true,
        ..view()
    };
    assert_eq!(
        store
            .observe_current_inventory(historical, 1, &result, 100)
            .await
            .unwrap_err(),
        DatabaseError::InvalidInput
    );
    base.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn duplicate_null_checkpoint_and_publication_date_do_not_choose_a_winner() {
    let fixture = support::TestDatabase::new().await;
    let base = fixture.open(100).await;
    let blobs = FsBlobStore::open(&fixture.directory.path().join("current-dedup"))
        .await
        .unwrap();
    let store = PgCorpusStore::new(base.pool(), blobs);
    let a = item("2092170", "1001", None);
    let first = bind(&store, 1, page(vec![a.clone(), a.clone()]), 100).await;
    assert!(
        store
            .observe_current_inventory(view(), 1, &first, 100)
            .await
            .unwrap()
            .is_empty()
    );
    let mut published = a.clone();
    published.publication_date = Some("20260101".into());
    let second = bind(&store, 2, page(vec![published]), 101).await;
    assert!(
        store
            .observe_current_inventory(view(), 2, &second, 101)
            .await
            .unwrap()
            .is_empty()
    );
    let count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM openlegal.current_inventory_candidate")
            .fetch_one(&base.pool())
            .await
            .unwrap();
    assert_eq!(count, 1);
    let third = bind(
        &store,
        3,
        page(vec![item("2092170", "1001", Some("20260101"))]),
        102,
    )
    .await;
    assert_eq!(
        store
            .observe_current_inventory(view(), 3, &third, 102)
            .await
            .unwrap(),
        vec![a.object]
    );
    base.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn old_cycle_pruning_keeps_held_proof_and_discards_only_unheld_candidates() {
    let fixture = support::TestDatabase::new().await;
    let base = fixture.open(100).await;
    let blobs = FsBlobStore::open(&fixture.directory.path().join("current-cycle"))
        .await
        .unwrap();
    let store = PgCorpusStore::new(base.pool(), blobs);
    let a = item("2092170", "1001", None);
    let b = item("2092171", "2001", None);
    let first = bind(
        &store,
        1,
        page(vec![a.clone(), b.clone(), item("2092170", "1002", None)]),
        100,
    )
    .await;
    store
        .observe_current_inventory(view(), 1, &first, 100)
        .await
        .unwrap();
    sqlx::query("UPDATE openlegal.provider_clone_view SET cycle=cycle+1")
        .execute(&base.pool())
        .await
        .unwrap();
    let second = bind(
        &store,
        2,
        page(vec![
            item("2092170", "1003", None),
            item("2092171", "2002", None),
        ]),
        101,
    )
    .await;
    assert_eq!(
        store
            .observe_current_inventory(view(), 2, &second, 101)
            .await
            .unwrap(),
        vec![a.object]
    );
    let counts:(i64,i64)=sqlx::query_as("SELECT count(*) FILTER(WHERE cycle=1),count(*) FILTER(WHERE cycle=2) FROM openlegal.current_inventory_candidate").fetch_one(&base.pool()).await.unwrap();
    assert_eq!(counts, (2, 1));
    assert!(!store.head_inventory_conflicted(&b.object).await.unwrap());
    // Simulate the state produced by the separately audited release CLI; this
    // test verifies that immutable conflict provenance survives later pruning.
    sqlx::query("UPDATE openlegal.corpus_object SET current_inventory_conflict=false WHERE identity->>'id'='2092170'")
        .execute(&base.pool()).await.unwrap();
    assert!(
        store
            .current_inventory_search_blocked(&item("2092170", "1003", None).object)
            .await
            .unwrap()
    );
    sqlx::query("UPDATE openlegal.provider_clone_view SET cycle=cycle+1")
        .execute(&base.pool())
        .await
        .unwrap();
    let third = bind(
        &store,
        3,
        page(vec![
            item("2092170", "1003", None),
            item("2092171", "2003", None),
        ]),
        102,
    )
    .await;
    assert!(
        store
            .observe_current_inventory(view(), 3, &third, 102)
            .await
            .unwrap()
            .is_empty()
    );
    let preserved: i64=sqlx::query_scalar("SELECT count(*) FROM openlegal.current_inventory_candidate WHERE cycle=1 AND conflict_proof").fetch_one(&base.pool()).await.unwrap();
    assert_eq!(preserved, 2);
    base.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn versioned_current_source_preserves_old_raw_archive_without_weakening_binding() {
    let fixture = support::TestDatabase::new().await;
    let base = fixture.open(100).await;
    let blobs = FsBlobStore::open(&fixture.directory.path().join("current-raw-source"))
        .await
        .unwrap();
    let store = PgCorpusStore::new(base.pool(), blobs);
    let raw = b"fictional unchanged permitted list bytes".to_vec();
    let source_key = "law_go_kr:schlPubRulListGuide:school_rule:false:0:1".to_owned();
    let old = store
        .retain_source_observation(
            SourceObservationInput {
                source_key: source_key.clone(),
                raw: Some(raw.clone()),
                media_type: "application/xml".into(),
                rights: SourceRights::legal_information(),
                metadata: BTreeMap::from([
                    ("dataset".into(), "school_rule".into()),
                    ("historical".into(), "false".into()),
                    ("page".into(), "1".into()),
                ]),
                observed_at: 100,
            },
            CancellationToken::new(),
        )
        .await
        .unwrap();
    let mut result = page(vec![item("2092170", "1001", None)]);
    let mut metadata = old.metadata.clone();
    metadata.extend(result.current_revision_metadata().unwrap());
    let new = store
        .retain_source_observation(
            SourceObservationInput {
                source_key: format!("{source_key}:current_revisions_v1"),
                raw: Some(raw.clone()),
                media_type: "application/xml".into(),
                rights: SourceRights::legal_information(),
                metadata,
                observed_at: 101,
            },
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_ne!(old.observation_id, new.observation_id);
    result.source_observation = Some(old.clone());
    assert_eq!(
        store
            .observe_current_inventory(view(), 1, &result, 101)
            .await
            .unwrap_err(),
        DatabaseError::InvalidInput
    );
    result.source_observation = Some(new);
    assert!(
        store
            .observe_current_inventory(view(), 1, &result, 101)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        store
            .source_observation(&old.observation_id, CancellationToken::new())
            .await
            .unwrap()
            .metadata,
        old.metadata
    );
    assert_eq!(
        store
            .source_observation_bytes(&old.observation_id, CancellationToken::new())
            .await
            .unwrap(),
        raw
    );
    base.close().await.unwrap();
}
