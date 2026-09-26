//! PostgreSQL corpus contracts, using exclusively fictional legal-shaped records.
#[path = "../../../test-support/postgres.rs"]
mod support;
use openlegal_adapters::{
    blob::FsBlobStore,
    corpus::PgCorpusStore,
    law_go_kr::{LawClient, RequestBudgetMode},
};
use openlegal_application::{
    database::{DatabaseStore, Publication},
    persistence::PersistentStore,
};
use openlegal_domain::legal::*;
use std::collections::BTreeMap;
use tokio_util::sync::CancellationToken;

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
    let counts: (i32, i32) = sqlx::query_as(
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
    let paused: (i32, i32) = sqlx::query_as(
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
    let counts: (i32, i32) = sqlx::query_as(
        "SELECT daily_used,pilot_used FROM openlegal.provider_request_budget WHERE singleton",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(counts, (1000, 100));
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
        .advance_inventory_cursor(Dataset::Treaty, false, 1, false)
        .await
        .unwrap();
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
    use sha2::{Digest, Sha256};
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
        Err(DatabaseError::ProcessingPending)
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
