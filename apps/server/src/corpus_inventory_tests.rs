//! Offline synthetic inventory replay and HEAD-read races against real PostgreSQL.
//! No LAW client, provider credentials, or legal-provider HTTP is used.
#[path = "../../../test-support/postgres.rs"]
mod postgres;

use super::*;
use futures::future::BoxFuture;
use openlegal_application::{
    blob::{BlobLocation, BlobMetrics, BlobPage, BlobPutResult},
    persistence::PersistentStore,
};
use openlegal_domain::{RetrievalError, legal::LegalRecord};
use serde_json::{Value, json};
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::Notify;

async fn fixture_sql(fixture: &postgres::TestDatabase, sql: &str) -> String {
    let container = std::env::var("OPENLEGAL_TEST_POSTGRES_CONTAINER").unwrap();
    let database = url::Url::parse(&fixture.url).unwrap();
    let output = tokio::time::timeout(
        Duration::from_secs(15),
        tokio::process::Command::new("docker")
            .args([
                "exec",
                &container,
                "psql",
                "-U",
                "postgres",
                "-d",
                database.path().trim_start_matches('/'),
                "-X",
                "-At",
                "-v",
                "ON_ERROR_STOP=1",
                "-c",
                sql,
            ])
            .kill_on_drop(true)
            .output(),
    )
    .await
    .expect("fixture SQL deadline")
    .expect("fixture SQL process");
    assert!(
        output.status.success(),
        "fictional fixture SQL failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap().trim().to_owned()
}

async fn fixture_json(fixture: &postgres::TestDatabase, sql: &str) -> Value {
    serde_json::from_str(&fixture_sql(fixture, sql).await).unwrap()
}

async fn setup() -> (
    postgres::TestDatabase,
    Arc<openlegal_adapters::postgres::PostgresStore>,
    Arc<CorpusRuntime>,
) {
    let fixture = postgres::TestDatabase::new().await;
    let persistent = fixture.open(now()).await;
    let config = DatabaseConfig {
        max_raw_bytes: Default::default(),
        auto_collection: true,
        blob_path: fixture.directory.path().join("inventory-blobs"),
        index_path: fixture.directory.path().join("inventory-index"),
        mecab_dictionary_path: std::env::var_os("OPENLEGAL_TEST_MECAB_DICTIONARY")
            .expect("PostgreSQL gate must provide the pinned dictionary")
            .into(),
        widget_html: "unused.html".into(),
        ingestion: None,
    };
    let runtime = CorpusRuntime::open(&config, &persistent).await.unwrap();
    assert!(runtime.provider.is_none());
    // Existing charged usage must survive replay, even though these fixtures
    // never construct a provider or reserve a new HTTP attempt.
    fixture_sql(
        &fixture,
        "UPDATE openlegal.provider_request_budget SET daily_used=27,on_demand_used=9 WHERE singleton;",
    )
    .await;
    (fixture, persistent, runtime)
}

fn view() -> CloneView {
    CloneView {
        dataset: Dataset::NationalStatute,
        historical: false,
        treaty_class: None,
    }
}

fn item(id: &str) -> InventoryItem {
    InventoryItem {
        object: ObjectId {
            jurisdiction: "kr".into(),
            provider: "fictional_inventory".into(),
            dataset: Dataset::NationalStatute,
            id: id.into(),
        },
        revision_id: "r1".into(),
        effective_date: None,
        publication_date: None,
        title: "Fictional inventory replay".into(),
        data_source: None,
        case_number: None,
        treaty_class_code: None,
        amendment_type: None,
    }
}

fn page() -> InventoryPage {
    InventoryPage {
        source_evidence: None,
        items: ["first", "second", "third"].map(item).to_vec(),
        done: false,
        total: Some(6),
        rejected_rows: 0,
        incomplete: false,
    }
}

async fn queue(runtime: &CorpusRuntime, candidate: &InventoryItem) {
    runtime
        .store
        .enqueue_job(
            candidate.object.clone(),
            candidate.revision_id.clone(),
            None,
            true,
            true,
            now(),
        )
        .await
        .unwrap();
}

async fn prepare_partial(
    fixture: &postgres::TestDatabase,
    runtime: &CorpusRuntime,
    result: &InventoryPage,
) {
    assert_eq!(runtime.store.clone_cursor(view()).await.unwrap(), (1, 0));
    assert_eq!(
        runtime
            .store
            .clone_page_offset(view(), 1, result)
            .await
            .unwrap(),
        0
    );
    queue(runtime, &result.items[0]).await;
    queue(runtime, &result.items[1]).await;
    runtime
        .store
        .clone_page_scheduled(view(), 1, 2, result, now(), &CancellationToken::new())
        .await
        .unwrap();
    fixture_sql(fixture, "UPDATE openlegal.corpus_job j SET status='failed',error_category='processing_failed',lease_until=NULL FROM openlegal.corpus_object o WHERE o.object_key=j.object_key AND o.identity->>'id'='first';").await;
    assert_eq!(
        runtime
            .first_unscheduled_item(&result.items, true, &CancellationToken::new())
            .await
            .unwrap(),
        Some(0)
    );
}

async fn saturate(runtime: &CorpusRuntime) {
    for number in 0..16 {
        queue(runtime, &item(&format!("capacity-{number}"))).await;
    }
    assert!(
        runtime
            .store
            .active_jobs_for_dataset(Dataset::NationalStatute)
            .await
            .unwrap()
            >= 16
    );
}

async fn release_capacity(fixture: &postgres::TestDatabase) {
    fixture_sql(fixture, "UPDATE openlegal.corpus_job j SET status='failed',error_category='processing_failed',lease_until=NULL FROM openlegal.corpus_object o WHERE o.object_key=j.object_key AND o.identity->>'id' LIKE 'capacity-%';").await;
}

async fn checkpoint(fixture: &postgres::TestDatabase) -> Value {
    fixture_json(fixture, "SELECT jsonb_build_array(next_page,item_offset,cycle,stable_cycles) FROM openlegal.provider_clone_view WHERE view_key='national_statute:false:0';").await
}

async fn budget(fixture: &postgres::TestDatabase) -> Value {
    fixture_json(
        fixture,
        "SELECT to_jsonb(b) FROM openlegal.provider_request_budget b WHERE singleton;",
    )
    .await
}

async fn assert_replayed_jobs(fixture: &postgres::TestDatabase) {
    assert_eq!(fixture_json(fixture, "SELECT jsonb_agg(o.identity->>'id' ORDER BY o.identity->>'id') FROM openlegal.corpus_job j JOIN openlegal.corpus_object o USING(object_key) WHERE j.status IN ('pending','running');").await, json!(["first", "second", "third"]));
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh and pinned MeCab-Ko dictionary"]
async fn replay_capacity_preserves_watermark_and_later_schedules_missing_and_new_items() {
    let (fixture, persistent, runtime) = setup().await;
    let result = page();
    let cancel = CancellationToken::new();
    let original_budget = budget(&fixture).await;
    prepare_partial(&fixture, &runtime, &result).await;
    saturate(&runtime).await;
    runtime
        .schedule_inventory_page(view(), 1, &result, &cancel)
        .await
        .unwrap();
    assert_eq!(checkpoint(&fixture).await, json!([1, 2, 1, 0]));
    runtime
        .schedule_inventory_page(view(), 1, &result, &cancel)
        .await
        .unwrap();
    assert_eq!(checkpoint(&fixture).await, json!([1, 2, 1, 0]));
    assert!(!cancel.is_cancelled());
    let progress = runtime.store.clone_progress().await.unwrap();
    assert_eq!(progress["initial_canonical_clone_complete"], false);
    assert_eq!(progress["full_available_clone_complete"], false);
    assert_eq!(progress["views"][0]["missing_bodies"], 3);
    release_capacity(&fixture).await;
    runtime
        .schedule_inventory_page(view(), 1, &result, &cancel)
        .await
        .unwrap();
    assert_eq!(checkpoint(&fixture).await, json!([2, 0, 1, 0]));
    assert_replayed_jobs(&fixture).await;
    assert_eq!(
        runtime.store.clone_progress().await.unwrap()["views"][0]["missing_bodies"],
        3
    );
    assert_eq!(budget(&fixture).await, original_budget);
    runtime.close().await.unwrap();
    persistent.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh and pinned MeCab-Ko dictionary"]
async fn exhausted_replay_sql_rejection_preserves_prior_checkpoint_without_cancellation() {
    let (fixture, persistent, runtime) = setup().await;
    let result = page();
    let cancel = CancellationToken::new();
    let original_budget = budget(&fixture).await;
    prepare_partial(&fixture, &runtime, &result).await;
    // The sequence survives rollback and proves all four real rejected INSERTs.
    fixture_sql(&fixture, "CREATE SEQUENCE public.replay_rejections; CREATE FUNCTION public.reject_replay() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM nextval('public.replay_rejections'); RAISE EXCEPTION USING ERRCODE='55P03', MESSAGE='fictional replay contention'; END $$; CREATE TRIGGER reject_replay BEFORE INSERT ON openlegal.corpus_job FOR EACH ROW EXECUTE FUNCTION public.reject_replay();").await;
    assert_eq!(
        runtime
            .schedule_inventory_page(view(), 1, &result, &cancel)
            .await,
        Err(DatabaseError::StorageContended)
    );
    assert_eq!(checkpoint(&fixture).await, json!([1, 2, 1, 0]));
    assert_eq!(
        fixture_sql(&fixture, "SELECT last_value FROM public.replay_rejections;").await,
        "4"
    );
    assert!(!cancel.is_cancelled());
    assert_eq!(budget(&fixture).await, original_budget);
    fixture_sql(
        &fixture,
        "DROP TRIGGER reject_replay ON openlegal.corpus_job; DROP FUNCTION public.reject_replay();",
    )
    .await;
    runtime
        .schedule_inventory_page(view(), 1, &result, &cancel)
        .await
        .unwrap();
    assert_replayed_jobs(&fixture).await;
    assert_eq!(checkpoint(&fixture).await, json!([2, 0, 1, 0]));
    assert_eq!(budget(&fixture).await, original_budget);
    runtime.close().await.unwrap();
    persistent.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh and pinned MeCab-Ko dictionary"]
async fn changed_inventory_resets_watermark_and_keeps_genuine_checkpoint_conflicts() {
    let (fixture, persistent, runtime) = setup().await;
    let result = page();
    let cancel = CancellationToken::new();
    prepare_partial(&fixture, &runtime, &result).await;
    assert_eq!(
        runtime
            .store
            .clone_page_scheduled(view(), 1, 1, &result, now(), &cancel)
            .await,
        Err(DatabaseError::Conflict)
    );
    assert_eq!(checkpoint(&fixture).await, json!([1, 2, 1, 0]));
    saturate(&runtime).await;
    let mut changed = result.clone();
    changed.items.insert(0, item("inserted"));
    runtime
        .schedule_inventory_page(view(), 1, &changed, &cancel)
        .await
        .unwrap();
    assert_eq!(checkpoint(&fixture).await, json!([1, 0, 1, 0]));
    assert_eq!(
        runtime.store.clone_page_offset(view(), 2, &changed).await,
        Err(DatabaseError::Conflict)
    );
    assert_eq!(
        runtime
            .store
            .clone_page_scheduled(view(), 2, 0, &changed, now(), &cancel)
            .await,
        Err(DatabaseError::Conflict)
    );
    release_capacity(&fixture).await;
    runtime
        .schedule_inventory_page(view(), 1, &changed, &cancel)
        .await
        .unwrap();
    assert_eq!(fixture_json(&fixture, "SELECT jsonb_agg(o.identity->>'id' ORDER BY o.identity->>'id') FROM openlegal.corpus_job j JOIN openlegal.corpus_object o USING(object_key) WHERE j.status IN ('pending','running');").await, json!(["first", "inserted", "second", "third"]));
    assert_eq!(checkpoint(&fixture).await, json!([2, 0, 1, 0]));
    assert_eq!(
        runtime
            .store
            .clone_page_scheduled(view(), 1, 4, &changed, now(), &cancel)
            .await,
        Err(DatabaseError::Conflict)
    );
    assert!(!cancel.is_cancelled());
    runtime.close().await.unwrap();
    persistent.close().await.unwrap();
}

struct PauseOneRead {
    inner: Arc<dyn BlobStore>,
    armed: Arc<AtomicBool>,
    selected: Arc<Notify>,
    release: Arc<Notify>,
}

impl BlobStore for PauseOneRead {
    fn put_if_absent(
        &self,
        location: BlobLocation,
        bytes: Vec<u8>,
        cancel: CancellationToken,
    ) -> BoxFuture<'static, Result<BlobPutResult, RetrievalError>> {
        self.inner.put_if_absent(location, bytes, cancel)
    }

    fn get(
        &self,
        location: BlobLocation,
        cancel: CancellationToken,
    ) -> BoxFuture<'static, Result<Option<Vec<u8>>, RetrievalError>> {
        let inner = self.inner.clone();
        let armed = self.armed.clone();
        let selected = self.selected.clone();
        let release = self.release.clone();
        Box::pin(async move {
            let bytes = inner.get(location, cancel).await?;
            if armed.swap(false, Ordering::SeqCst) {
                selected.notify_one();
                tokio::time::timeout(Duration::from_secs(15), release.notified())
                    .await
                    .expect("fixture concurrent publication deadline");
            }
            Ok(bytes)
        })
    }

    fn delete_if_present(
        &self,
        location: BlobLocation,
        cancel: CancellationToken,
    ) -> BoxFuture<'static, Result<(), RetrievalError>> {
        self.inner.delete_if_present(location, cancel)
    }

    fn enumerate(
        &self,
        cursor: Option<String>,
        limit: usize,
        cancel: CancellationToken,
    ) -> BoxFuture<'static, Result<BlobPage, RetrievalError>> {
        self.inner.enumerate(cursor, limit, cancel)
    }

    fn cleanup_staging(
        &self,
        at: u64,
        limit: usize,
        cancel: CancellationToken,
    ) -> BoxFuture<'static, Result<usize, RetrievalError>> {
        self.inner.cleanup_staging(at, limit, cancel)
    }

    fn health(&self, cancel: CancellationToken) -> BoxFuture<'static, Result<(), RetrievalError>> {
        self.inner.health(cancel)
    }

    fn close(&self) -> BoxFuture<'static, Result<(), RetrievalError>> {
        self.inner.close()
    }

    fn metrics(&self) -> BlobMetrics {
        self.inner.metrics()
    }
}

async fn publish_corrected(store: &PgCorpusStore, body: &str) -> Capture {
    let candidate = item("head-race");
    let version = store.state(&candidate.object).await.unwrap().version;
    let at = now();
    store
        .publish(
            Publication {
                record: LegalRecord {
                    object: candidate.object,
                    revision_id: candidate.revision_id,
                    title: candidate.title,
                    body: body.into(),
                    sections: vec![],
                    metadata: Default::default(),
                    publication_date: None,
                    effective_date: None,
                    source_url: "https://example.test/fictional-inventory".into(),
                    representation: "fictional_text".into(),
                },
                raw: body.as_bytes().to_vec(),
                additional_evidence: vec![],
                processor_version: "fictional_v1".into(),
                retrieved_at: at,
                now: at,
                expected_version: version,
                install_head: true,
                job_id: None,
            },
            CancellationToken::new(),
        )
        .await
        .unwrap()
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh and pinned MeCab-Ko dictionary"]
async fn concurrent_corrected_head_publication_yields_only_stale_read_before_enqueue() {
    let (fixture, persistent, mut runtime) = setup().await;
    let original_budget = budget(&fixture).await;
    let armed = Arc::new(AtomicBool::new(false));
    let selected = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let store = Arc::new(PgCorpusStore::new(
        persistent.pool(),
        Arc::new(PauseOneRead {
            inner: runtime.blobs.clone(),
            armed: armed.clone(),
            selected: selected.clone(),
            release: release.clone(),
        }),
    ));
    Arc::get_mut(&mut runtime).unwrap().store = store.clone();
    let old = publish_corrected(&store, "Fictional original text").await;
    armed.store(true, Ordering::SeqCst);
    let cancel = CancellationToken::new();
    let worker = runtime.clone();
    let worker_cancel = cancel.clone();
    let refresh =
        tokio::spawn(async move { worker.refresh(item("head-race"), true, worker_cancel).await });
    tokio::time::timeout(Duration::from_secs(15), selected.notified())
        .await
        .expect("refresh selected the old capture");
    // The barrier was disarmed before waiting, so the real publication's own
    // previous-HEAD read can proceed and install corrected bytes for the same revision.
    let new = publish_corrected(&store, "Fictional corrected text").await;
    assert_ne!(old.capture_id, new.capture_id);
    release.notify_one();
    assert_eq!(refresh.await.unwrap(), Err(DatabaseError::Capacity));
    assert!(!cancel.is_cancelled());
    assert_eq!(
        fixture_sql(&fixture, "SELECT count(*) FROM openlegal.corpus_job;").await,
        "0"
    );
    assert_eq!(
        fixture_sql(&fixture, "SELECT count(*) FROM openlegal.corpus_capture;").await,
        "2"
    );
    runtime
        .refresh(item("head-race"), true, cancel.clone())
        .await
        .unwrap();
    assert_eq!(
        fixture_sql(&fixture, "SELECT count(*) FROM openlegal.corpus_job;").await,
        "0"
    );
    assert_eq!(
        store
            .resolve(
                item("head-race").object,
                RevisionSelector::Head,
                now(),
                cancel.clone()
            )
            .await
            .unwrap()
            .capture_id,
        new.capture_id
    );
    assert_eq!(
        store
            .resolve(
                item("head-race").object,
                RevisionSelector::Capture {
                    id: old.capture_id.clone()
                },
                now(),
                cancel
            )
            .await
            .unwrap()
            .capture_id,
        old.capture_id
    );
    assert_eq!(budget(&fixture).await, original_budget);
    runtime.close().await.unwrap();
    persistent.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh and pinned MeCab-Ko dictionary"]
async fn pre_reservation_contention_refunds_repeated_claims_and_fences_stale_owners() {
    let (fixture, persistent, runtime) = setup().await;
    let original_budget = budget(&fixture).await;
    let candidate = item("pre-reservation-contention");
    queue(&runtime, &candidate).await;
    let mut previous = None;
    // More yields than max_job_attempts must not exhaust a never-reserved job.
    for _ in 0..5 {
        let claim = runtime.store.claim_job(now()).await.unwrap().unwrap();
        assert_eq!(claim.attempts, 1);
        if let Some(old) = previous.take() {
            release_detail_contention(&runtime.store, &old, false)
                .await
                .unwrap();
            let still_running = fixture_json(&fixture,
                "SELECT jsonb_build_array(status,attempts,expected_version) FROM openlegal.corpus_job;").await;
            assert_eq!(still_running, json!(["running", 1, claim.expected_version]));
        }
        release_detail_contention(&runtime.store, &claim, false)
            .await
            .unwrap();
        assert_eq!(fixture_json(&fixture,
            "SELECT jsonb_build_array(status,attempts,lease_until,error_category) FROM openlegal.corpus_job;").await,
            json!(["pending",0,null,null]));
        assert_eq!(budget(&fixture).await, original_budget);
        previous = Some(claim);
    }
    let reserved = runtime.store.claim_job(now()).await.unwrap().unwrap();
    // Model the observer set before transmitting reservation COMMIT, including
    // a lost acknowledgement: never refund this potentially charged execution.
    release_detail_contention(&runtime.store, &reserved, true)
        .await
        .unwrap();
    assert_eq!(fixture_json(&fixture,
        "SELECT jsonb_build_array(status,attempts,error_category,lease_until IS NOT NULL) FROM openlegal.corpus_job;").await,
        json!(["running",1,"processing_failed",true]));
    assert!(runtime.store.claim_job(now()).await.unwrap().is_none());
    assert_eq!(budget(&fixture).await, original_budget);
    runtime.close().await.unwrap();
    persistent.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh and pinned MeCab-Ko dictionary"]
async fn post_reservation_contention_preserves_the_last_charged_execution() {
    let (fixture, persistent, runtime) = setup().await;
    let candidate = item("charged-contention");
    queue(&runtime, &candidate).await;
    fixture_sql(&fixture,
        "UPDATE openlegal.provider_request_budget SET max_job_attempts=3; UPDATE openlegal.corpus_job SET attempts=2;").await;
    let original_budget = budget(&fixture).await;
    let claim = runtime.store.claim_job(now()).await.unwrap().unwrap();
    assert_eq!(claim.attempts, 3);
    release_detail_contention(&runtime.store, &claim, true)
        .await
        .unwrap();
    assert_eq!(fixture_json(&fixture,
        "SELECT jsonb_build_array(status,attempts,error_category,lease_until) FROM openlegal.corpus_job;").await,
        json!(["failed",3,"processing_failed",null]));
    assert_eq!(budget(&fixture).await, original_budget);
    runtime.close().await.unwrap();
    persistent.close().await.unwrap();
}
