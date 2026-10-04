//! Independent PostgreSQL verification of durable canonical clone checkpoints.
#[path = "../../../test-support/postgres.rs"]
mod support;
use openlegal_adapters::{
    blob::FsBlobStore,
    corpus::{CloneView, PageGapObservation, PgCorpusStore},
    law_go_kr::{InventoryItem, InventoryPage},
};
use openlegal_application::{database::Publication, persistence::PersistentStore};
use openlegal_domain::legal::*;
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
        dataset: Dataset::NationalStatute,
        historical: false,
        treaty_class: None,
    }
}
fn item(id: &str, revision: &str) -> InventoryItem {
    InventoryItem {
        object: ObjectId {
            jurisdiction: "kr".into(),
            provider: "fictional_test".into(),
            dataset: Dataset::NationalStatute,
            id: id.into(),
        },
        revision_id: revision.into(),
        effective_date: None,
        publication_date: None,
        title: format!("Fictional {id}"),
        data_source: None,
        case_number: None,
        treaty_class_code: None,
        amendment_type: None,
    }
}
fn page(items: Vec<InventoryItem>, total: Option<u64>, done: bool) -> InventoryPage {
    InventoryPage {
        source_evidence: None,
        items,
        done,
        total,
        rejected_rows: 0,
        incomplete: false,
    }
}
async fn checkpoint(store: &PgCorpusStore, view: CloneView, items: Vec<InventoryItem>) {
    let (number, _) = store.clone_cursor(view).await.unwrap();
    let result = page(items, Some(0), true);
    store
        .clone_page_scheduled(
            view,
            number,
            result.items.len(),
            &result,
            100,
            &CancellationToken::new(),
        )
        .await
        .unwrap();
}
async fn all_empty_views_stable(store: &PgCorpusStore) {
    for current in CloneView::all() {
        checkpoint(store, current, vec![]).await;
        checkpoint(store, current, vec![]).await;
    }
}
async fn progress_view(store: &PgCorpusStore, view: CloneView) -> Value {
    let key = view.key().unwrap();
    store.clone_progress().await.unwrap()["views"]
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["view"] == key)
        .unwrap()
        .clone()
}
async fn accept_body(store: &PgCorpusStore, inventory: &InventoryItem, incomplete: bool) {
    let mut metadata = BTreeMap::new();
    if incomplete {
        metadata.insert("attachment_status".into(), "incomplete".into());
    }
    store
        .publish(
            Publication {
                record: LegalRecord {
                    object: inventory.object.clone(),
                    revision_id: inventory.revision_id.clone(),
                    title: inventory.title.clone(),
                    body: "Fictional original body".into(),
                    metadata,
                    publication_date: None,
                    effective_date: None,
                    source_url: "https://example.test/fixture".into(),
                    representation: "provider_text_v1".into(),
                    sections: vec![],
                },
                raw: b"fictional raw".to_vec(),
                additional_evidence: vec![],
                processor_version: "fixture_v1".into(),
                retrieved_at: 100,
                now: 100,
                expected_version: store.state(&inventory.object).await.unwrap().version,
                install_head: true,
                job_id: None,
            },
            CancellationToken::new(),
        )
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn clone_checkpoint_survives_restart_and_requires_two_complete_matching_traversals() {
    let fixture = support::TestDatabase::new().await;
    let base = fixture.open(100).await;
    let blobs = FsBlobStore::open(&fixture.directory.path().join("clone-restart"))
        .await
        .unwrap();
    let store =
        PgCorpusStore::with_publication_clock(base.pool(), blobs.clone(), Arc::new(FixtureClock));
    let first = page(vec![item("a", "r1"), item("b", "r2")], Some(2), false);
    assert_eq!(store.clone_cursor(view()).await.unwrap(), (1, 0));
    store
        .clone_page_scheduled(view(), 1, 1, &first, 100, &CancellationToken::new())
        .await
        .unwrap();
    let restarted =
        PgCorpusStore::with_publication_clock(base.pool(), blobs, Arc::new(FixtureClock));
    assert_eq!(restarted.clone_cursor(view()).await.unwrap(), (1, 1));
    restarted
        .clone_page_scheduled(view(), 1, 2, &first, 101, &CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(restarted.clone_cursor(view()).await.unwrap(), (2, 0));
    restarted
        .clone_page_scheduled(
            view(),
            2,
            0,
            &page(vec![], Some(2), true),
            102,
            &CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(progress_view(&restarted, view()).await["stable_cycles"], 1);
    assert_eq!(restarted.clone_cursor(view()).await.unwrap(), (1, 0));
    let same_inventory = page(vec![item("b", "r2"), item("a", "r1")], Some(2), true);
    restarted
        .clone_page_scheduled(
            view(),
            1,
            2,
            &same_inventory,
            103,
            &CancellationToken::new(),
        )
        .await
        .unwrap();
    let stable = progress_view(&restarted, view()).await;
    assert_eq!(stable["stable_cycles"], 2);
    assert_eq!(stable["cycle"], 3);
    assert_eq!(stable["observed"], 2);
    assert_eq!(stable["missing_bodies"], 2);
    let changed = page(vec![item("a", "r3"), item("b", "r2")], Some(2), true);
    restarted
        .clone_page_scheduled(view(), 1, 2, &changed, 104, &CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(progress_view(&restarted, view()).await["stable_cycles"], 1);
    assert!(
        !restarted.clone_progress().await.unwrap()["initial_canonical_clone_complete"]
            .as_bool()
            .unwrap()
    );
    base.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn clone_member_identity_is_unique_and_duplicate_rows_cannot_satisfy_totals() {
    let fixture = support::TestDatabase::new().await;
    let base = fixture.open(100).await;
    let blobs = FsBlobStore::open(&fixture.directory.path().join("clone-duplicates"))
        .await
        .unwrap();
    let store = PgCorpusStore::with_publication_clock(base.pool(), blobs, Arc::new(FixtureClock));
    store.clone_cursor(view()).await.unwrap();
    let duplicate = page(
        vec![item("a", "r:1"), item("a", "r:1"), item("a", "r:1:2")],
        Some(3),
        true,
    );
    store
        .clone_page_scheduled(view(), 1, 3, &duplicate, 100, &CancellationToken::new())
        .await
        .unwrap();
    let progress = progress_view(&store, view()).await;
    assert_eq!(progress["observed"], 2);
    assert_eq!(progress["stable_cycles"], 0);
    let unique = page(vec![item("a", "r:1"), item("a", "r:1:2")], Some(2), true);
    store
        .clone_page_scheduled(view(), 1, 2, &unique, 101, &CancellationToken::new())
        .await
        .unwrap();
    store
        .clone_page_scheduled(view(), 1, 2, &unique, 102, &CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(progress_view(&store, view()).await["stable_cycles"], 2);
    base.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn clone_total_changes_rejected_rows_and_incomplete_pages_invalidate_traversals() {
    let fixture = support::TestDatabase::new().await;
    let base = fixture.open(100).await;
    let blobs = FsBlobStore::open(&fixture.directory.path().join("clone-invalid-traversal"))
        .await
        .unwrap();
    let store = PgCorpusStore::with_publication_clock(base.pool(), blobs, Arc::new(FixtureClock));
    store.clone_cursor(view()).await.unwrap();
    store
        .clone_page_scheduled(
            view(),
            1,
            1,
            &page(vec![item("a", "r1")], Some(2), false),
            100,
            &CancellationToken::new(),
        )
        .await
        .unwrap();
    store
        .clone_page_scheduled(
            view(),
            2,
            1,
            &page(vec![item("b", "r2")], Some(3), true),
            101,
            &CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(progress_view(&store, view()).await["stable_cycles"], 0);
    let mut incomplete = page(vec![item("a", "r1")], Some(1), true);
    incomplete.incomplete = true;
    store
        .clone_page_scheduled(view(), 1, 1, &incomplete, 102, &CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(progress_view(&store, view()).await["stable_cycles"], 0);
    let mut rejected = page(vec![item("a", "r1")], Some(1), true);
    rejected.rejected_rows = 1;
    store
        .clone_page_scheduled(view(), 1, 1, &rejected, 103, &CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(progress_view(&store, view()).await["stable_cycles"], 0);
    store
        .clone_page_scheduled(
            view(),
            1,
            1,
            &page(vec![item("a", "r1")], None, true),
            104,
            &CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(progress_view(&store, view()).await["stable_cycles"], 0);
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    assert_eq!(
        store
            .clone_page_scheduled(view(), 1, 0, &page(vec![], Some(0), true), 105, &cancelled)
            .await,
        Err(DatabaseError::Cancelled)
    );
    assert_eq!(store.clone_cursor(view()).await.unwrap(), (1, 0));
    base.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn clone_completion_requires_complete_bodies_no_jobs_and_no_open_gaps() {
    let fixture = support::TestDatabase::new().await;
    let base = fixture.open(100).await;
    let pool = base.pool();
    let blobs = FsBlobStore::open(&fixture.directory.path().join("clone-acceptance"))
        .await
        .unwrap();
    let store = PgCorpusStore::with_publication_clock(pool.clone(), blobs, Arc::new(FixtureClock));
    all_empty_views_stable(&store).await;
    assert_eq!(
        store.clone_progress().await.unwrap()["initial_canonical_clone_complete"],
        true
    );
    let observed = item("complete", "r1");
    let inventory = page(vec![observed.clone()], Some(1), true);
    store
        .clone_page_scheduled(view(), 1, 1, &inventory, 100, &CancellationToken::new())
        .await
        .unwrap();
    store
        .clone_page_scheduled(view(), 1, 1, &inventory, 101, &CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(
        store.clone_progress().await.unwrap()["initial_canonical_clone_complete"],
        false
    );
    accept_body(&store, &observed, true).await;
    assert_eq!(progress_view(&store, view()).await["missing_bodies"], 1);
    assert_eq!(
        store.clone_progress().await.unwrap()["initial_canonical_clone_complete"],
        false
    );
    accept_body(&store, &observed, false).await;
    assert_eq!(
        store.clone_progress().await.unwrap()["initial_canonical_clone_complete"],
        false,
        "accepted bytes still await indexing"
    );
    store
        .acknowledge_index(store.watermark().await.unwrap())
        .await
        .unwrap();
    assert_eq!(
        store.clone_progress().await.unwrap()["initial_canonical_clone_complete"],
        true
    );
    store
        .enqueue(observed.object.clone(), "r1".into(), 200)
        .await
        .unwrap();
    assert_eq!(
        store.clone_progress().await.unwrap()["initial_canonical_clone_complete"],
        false
    );
    sqlx::query("UPDATE openlegal.corpus_job SET status='failed'")
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(
        store.clone_progress().await.unwrap()["initial_canonical_clone_complete"],
        false
    );
    sqlx::query("UPDATE openlegal.corpus_job SET status='done'")
        .execute(&pool)
        .await
        .unwrap();
    accept_body(&store, &observed, false).await;
    store
        .record_page_gap(
            Dataset::NationalStatute,
            false,
            None,
            1,
            PageGapObservation {
                reason: "source_data_invalid",
                rows: 1,
                now: 200,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        store.clone_progress().await.unwrap()["initial_canonical_clone_complete"],
        false
    );
    store
        .resolve_page_gap(Dataset::NationalStatute, false, None, 1, 201)
        .await
        .unwrap();
    assert_eq!(
        store.clone_progress().await.unwrap()["initial_canonical_clone_complete"],
        true
    );
    base.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn treaty_classes_have_independent_durable_cursors_and_invalid_views_are_rejected() {
    let fixture = support::TestDatabase::new().await;
    let base = fixture.open(100).await;
    let blobs = FsBlobStore::open(&fixture.directory.path().join("clone-treaty-classes"))
        .await
        .unwrap();
    let store = PgCorpusStore::with_publication_clock(base.pool(), blobs, Arc::new(FixtureClock));
    let first = CloneView {
        dataset: Dataset::Treaty,
        historical: false,
        treaty_class: Some(1),
    };
    let second = CloneView {
        treaty_class: Some(2),
        ..first
    };
    let mut treaty = item("treaty", "r1");
    treaty.object.dataset = Dataset::Treaty;
    treaty.treaty_class_code = Some("440101".into());
    store.clone_cursor(first).await.unwrap();
    assert_eq!(store.clone_cursor(second).await.unwrap(), (1, 0));
    store
        .clone_page_scheduled(
            first,
            1,
            1,
            &page(vec![treaty], Some(2), false),
            100,
            &CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(store.clone_cursor(first).await.unwrap(), (2, 0));
    assert_eq!(store.clone_cursor(second).await.unwrap(), (1, 0));
    assert_eq!(
        CloneView {
            treaty_class: Some(1),
            ..view()
        }
        .key(),
        Err(DatabaseError::InvalidInput)
    );
    assert_eq!(
        CloneView {
            treaty_class: None,
            ..first
        }
        .key(),
        Err(DatabaseError::InvalidInput)
    );
    assert_eq!(
        CloneView {
            historical: true,
            ..first
        }
        .key(),
        Err(DatabaseError::UnsupportedHistory)
    );
    base.close().await.unwrap();
}
