//! Explicit disposable PostgreSQL contention soak; never part of ordinary tests.
//! All legal-shaped data are synthetic. No provider client or HTTP call exists.
#[path = "../../../test-support/postgres.rs"]
mod support;

use openlegal_adapters::{
    blob::FsBlobStore,
    corpus::{CloneView, PgCorpusStore},
    korean_analysis::KoreanAnalyzer,
    search_index::CorpusIndex,
};
use openlegal_application::{
    database::{DatabaseStore, Publication},
    persistence::PersistentStore,
};
use openlegal_domain::legal::{
    Capture, DatabaseError as E, Dataset, LegalRecord, ObjectId, RevisionSelector,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio_util::sync::CancellationToken;

const SEED_JOBS: usize = 20_000;
const SEED_CAPTURES: usize = 128;
type Result<T> = std::result::Result<T, E>;

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn digest(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

fn object(id: &str) -> ObjectId {
    ObjectId {
        jurisdiction: "kr".into(),
        provider: "fictional_test".into(),
        dataset: Dataset::NationalStatute,
        id: id.into(),
    }
}

fn record(object: ObjectId, revision: String, large: bool) -> LegalRecord {
    let body = format!(
        "제1조(시험) synthetic retained index evidence {}\n",
        object.id
    )
    .repeat(if large { 2048 } else { 32 });
    LegalRecord {
        object,
        revision_id: revision,
        title: "Synthetic contention fixture".into(),
        body,
        metadata: BTreeMap::new(),
        publication_date: Some("20260101".into()),
        effective_date: Some("20260201".into()),
        source_url: "https://example.test/fictional".into(),
        representation: "synthetic_fixture_v1".into(),
        sections: vec![],
    }
}

#[derive(Default)]
struct Metrics {
    published: AtomicU64,
    indexed: AtomicU64,
    maintained: AtomicU64,
    status_reads: AtomicU64,
    contended: AtomicU64,
    maximum_operation_us: AtomicU64,
}

impl Metrics {
    fn elapsed(&self, start: Instant) {
        self.maximum_operation_us
            .fetch_max(start.elapsed().as_micros() as u64, Ordering::Relaxed);
    }
    fn json(&self) -> Value {
        json!({"published":self.published.load(Ordering::Relaxed),"index_ack":self.indexed.load(Ordering::Relaxed),
            "maintenance_passes":self.maintained.load(Ordering::Relaxed),"status_reads":self.status_reads.load(Ordering::Relaxed),
            "known_storage_contention_yields":self.contended.load(Ordering::Relaxed),
            "maximum_storage_operation_us":self.maximum_operation_us.load(Ordering::Relaxed)})
    }
    fn yielded<T>(&self, value: Result<T>) -> Result<Option<T>> {
        match value {
            Ok(value) => Ok(Some(value)),
            Err(E::StorageContended) => {
                self.contended.fetch_add(1, Ordering::Relaxed);
                Ok(None)
            }
            Err(error) => Err(error),
        }
    }
}

async fn seed(store: &PgCorpusStore, pool: &sqlx::PgPool) -> Result<Vec<Capture>> {
    for view in CloneView::all() {
        store.clone_cursor(view).await?;
    }
    sqlx::query("UPDATE openlegal.provider_clone_view SET stable_cycles=2,cycle=3")
        .execute(pool)
        .await
        .map_err(|_| E::StorageUnavailable)?;
    // These completed jobs/members are workload rows, not fabricated captures.
    // Batched insertion stays under the same two-second statement deadline.
    for offset in (0..SEED_JOBS).step_by(1000) {
        sqlx::query("INSERT INTO openlegal.corpus_object(object_key,identity) SELECT repeat(md5('soak:'||n),2),jsonb_build_object('jurisdiction','kr','provider','fictional_test','dataset','national_statute','id','seed_'||n) FROM generate_series($1::integer,$2::integer) n")
            .bind(offset as i32 + 1).bind(offset as i32 + 1000).execute(pool).await.map_err(|_| E::StorageUnavailable)?;
        sqlx::query("INSERT INTO openlegal.corpus_job(object_key,revision_id,expected_version,status,created_at,completed_at) SELECT repeat(md5('soak:'||n),2),'r1',0,'done',100,100 FROM generate_series($1::integer,$2::integer) n")
            .bind(offset as i32 + 1).bind(offset as i32 + 1000).execute(pool).await.map_err(|_| E::StorageUnavailable)?;
        for historical in [false, true] {
            let view = CloneView {
                dataset: Dataset::NationalStatute,
                historical,
                treaty_class: None,
            };
            sqlx::query("INSERT INTO openlegal.provider_clone_member(view_key,object_key,revision_id,seen_cycle,required_body) SELECT $1,repeat(md5('soak:'||n),2),'r1',2,true FROM generate_series($2::integer,$3::integer) n")
                .bind(view.key()?).bind(offset as i32 + 1).bind(offset as i32 + 1000).execute(pool).await.map_err(|_| E::StorageUnavailable)?;
        }
    }
    let mut captures = Vec::new();
    for n in 0..SEED_CAPTURES {
        let object = object(&format!("archive_{}", n / 2));
        let current = n % 2 == 1;
        let record = record(object.clone(), format!("r{}", n % 2 + 1), n % 16 == 0);
        let state = store.state(&object).await?;
        let timestamp = now();
        let published = store
            .publish(
                Publication {
                    raw: record.body.as_bytes().to_vec(),
                    record,
                    additional_evidence: vec![],
                    processor_version: "soak_fixture_v1".into(),
                    now: timestamp,
                    retrieved_at: timestamp,
                    expected_version: state.version,
                    install_head: current,
                    job_id: None,
                },
                CancellationToken::new(),
            )
            .await?;
        let view = CloneView {
            dataset: Dataset::NationalStatute,
            historical: !current,
            treaty_class: None,
        };
        let key = digest(&serde_json::to_vec(&object).map_err(|_| E::StorageCorrupt)?);
        sqlx::query("INSERT INTO openlegal.provider_clone_member(view_key,object_key,revision_id,seen_cycle,required_body) VALUES($1,$2,$3,2,true)")
            .bind(view.key()?).bind(key).bind(&published.record.revision_id).execute(pool).await.map_err(|_| E::StorageUnavailable)?;
        captures.push(published);
    }
    Ok(captures)
}

async fn publish_loop(
    store: Arc<PgCorpusStore>,
    metrics: Arc<Metrics>,
    stop: CancellationToken,
    worker: u32,
) -> Result<()> {
    let mut iteration = 0_u64;
    while !stop.is_cancelled() {
        let started = Instant::now();
        let object = object(&format!("live_{worker}_{iteration}"));
        if metrics
            .yielded(
                store
                    .enqueue_job(
                        object,
                        "r1".into(),
                        None,
                        iteration.is_multiple_of(2),
                        false,
                        now(),
                    )
                    .await,
            )?
            .is_some()
            && let Some(Some(job)) = metrics.yielded(store.claim_job(now()).await)?
        {
            let record = record(job.object.clone(), job.revision_id.clone(), false);
            // Never replay publication on error: an unknown COMMIT terminates the soak.
            store
                .publish(
                    Publication {
                        raw: record.body.as_bytes().to_vec(),
                        record,
                        additional_evidence: vec![],
                        processor_version: "soak_fixture_v1".into(),
                        retrieved_at: now(),
                        now: now(),
                        expected_version: job.expected_version,
                        install_head: job.install_head,
                        job_id: Some(job.id),
                    },
                    CancellationToken::new(),
                )
                .await?;
            metrics.published.fetch_add(1, Ordering::Relaxed);
        }
        metrics.elapsed(started);
        iteration += 1;
        tokio::select! { _ = stop.cancelled() => break, _ = tokio::time::sleep(Duration::from_secs(1)) => {} }
    }
    Ok(())
}

async fn replay(store: &PgCorpusStore, index: &Arc<CorpusIndex>, after: &mut u64) -> Result<()> {
    for event in store.outbox(*after, 32).await? {
        if event.sequence != *after + 1 {
            return Err(E::StorageCorrupt);
        }
        let capture = store
            .index_capture(&event, CancellationToken::new())
            .await?
            .ok_or(E::StorageCorrupt)?;
        let index = index.clone();
        let sequence = event.sequence;
        tokio::task::spawn_blocking(move || {
            index.apply_capture(capture, event.install_head, sequence)
        })
        .await
        .map_err(|_| E::StorageCorrupt)??;
        // The real Tantivy commit and reader reload precede acknowledgement.
        store.acknowledge_index(sequence).await?;
        *after = sequence;
    }
    Ok(())
}

async fn archive_hash(store: &PgCorpusStore, captures: &[Capture]) -> Result<String> {
    let mut hashes = Vec::new();
    for expected in captures {
        let retained = store
            .resolve(
                expected.record.object.clone(),
                RevisionSelector::Capture {
                    id: expected.capture_id.clone(),
                },
                now(),
                CancellationToken::new(),
            )
            .await?;
        if retained.record != expected.record || retained.raw_sha256 != expected.raw_sha256 {
            return Err(E::StorageCorrupt);
        }
        hashes.push(format!(
            "{}:{}:{}",
            retained.capture_id,
            retained.raw_sha256,
            digest(retained.record.body.as_bytes())
        ));
    }
    Ok(digest(hashes.join("\n").as_bytes()))
}

async fn run(seconds: u64) -> Result<()> {
    let fixture = support::TestDatabase::new().await;
    let base = fixture.open(now()).await;
    let pool = base.pool();
    let store = Arc::new(PgCorpusStore::new(
        pool.clone(),
        FsBlobStore::open(&fixture.directory.path().join("corpus-blobs"))
            .await
            .map_err(|_| E::StorageUnavailable)?,
    ));
    store
        .configure_archive_capacity(Some(512 * 1024 * 1024))
        .await?;
    let dictionary = std::env::var_os("OPENLEGAL_TEST_MECAB_DICTIONARY").ok_or(E::InvalidInput)?;
    let analyzer = KoreanAnalyzer::open(std::path::Path::new(&dictionary))?;
    let index = CorpusIndex::open(&fixture.directory.path().join("corpus-index"), analyzer)?;
    let captures = seed(&store, &pool).await?;
    let original_hash = archive_hash(&store, &captures).await?;
    let mut indexed = 0;
    while indexed < SEED_CAPTURES as u64 {
        replay(&store, &index, &mut indexed).await?;
    }
    for n in 0..8 {
        store
            .enqueue_job(
                object(&format!("pending_{n}")),
                "r1".into(),
                None,
                n % 2 == 0,
                false,
                now(),
            )
            .await?;
    }
    let metrics = Arc::new(Metrics::default());
    metrics.indexed.store(indexed, Ordering::Relaxed);
    let stop = CancellationToken::new();
    let _stop_on_drop = stop.clone().drop_guard();
    let started = Instant::now();
    println!(
        "{}",
        json!({"kind":"started","schema_version":1,"duration_secs":seconds,"seed_completed_jobs":SEED_JOBS,
        "seed_real_captures":SEED_CAPTURES,"archive_hash":original_hash,"provider_http_requests":0,
        "pool_acquire_timeout_ms":1000,"lock_timeout_ms":1000,"statement_timeout_ms":2000,"transaction_timeout_ms":5000,
        "index":"real_tantivy_full_dictionary","latency_scope":"whole_storage_operations"})
    );
    let mut workers = Vec::new();
    for worker in 0..2 {
        let store = store.clone();
        let metrics = metrics.clone();
        let stop = stop.clone();
        workers.push(tokio::spawn(async move {
            let result = publish_loop(store, metrics, stop.clone(), worker).await;
            if result.is_err() {
                stop.cancel();
            }
            result
        }));
    }
    let s = store.clone();
    let m = metrics.clone();
    let cancellation = stop.clone();
    let i = index.clone();
    workers.push(tokio::spawn(async move {
        let result = async {
            while !cancellation.is_cancelled() {
                let started = Instant::now();
                // No replay retries after a failed index commit or uncertain ack.
                replay(&s, &i, &mut indexed).await?;
                m.indexed.store(indexed, Ordering::Relaxed); m.elapsed(started);
                tokio::select! { _ = cancellation.cancelled() => break, _ = tokio::time::sleep(Duration::from_millis(250)) => {} }
            } Ok(())
        }.await;
        if result.is_err() { cancellation.cancel(); } result
    }));
    let s = store.clone();
    let m = metrics.clone();
    let cancellation = stop.clone();
    workers.push(tokio::spawn(async move {
        let result = async {
            while !cancellation.is_cancelled() {
                let started = Instant::now();
                if m.yielded(s.maintain(now(), 0).await)?.is_some() { m.maintained.fetch_add(1, Ordering::Relaxed); }
                m.elapsed(started);
                tokio::select! { _ = cancellation.cancelled() => break, _ = tokio::time::sleep(Duration::from_millis(250)) => {} }
            } Ok(())
        }.await;
        if result.is_err() { cancellation.cancel(); } result
    }));
    let mut samples = 0;
    let mut last_progress_check = Instant::now();
    let mut last_published = 0;
    let mut last_indexed = indexed;
    while started.elapsed() < Duration::from_secs(seconds) && !stop.is_cancelled() {
        let read_started = Instant::now();
        let progress = store.clone_progress().await?;
        metrics.elapsed(read_started);
        metrics.status_reads.fetch_add(1, Ordering::Relaxed);
        if last_progress_check.elapsed() >= Duration::from_secs(60) {
            let published = metrics.published.load(Ordering::Relaxed);
            let indexed = metrics.indexed.load(Ordering::Relaxed);
            if published <= last_published || indexed <= last_indexed {
                stop.cancel();
                return Err(E::StorageUnavailable);
            }
            last_published = published;
            last_indexed = indexed;
            last_progress_check = Instant::now();
        }
        if progress["initial_canonical_clone_complete"] != false
            || progress["atomic_upstream_snapshot"] != false
        {
            stop.cancel();
            return Err(E::StorageCorrupt);
        }
        println!(
            "{}",
            json!({"kind":"sample","elapsed_secs":started.elapsed().as_secs_f64(),"metrics":metrics.json(),
            "status_storage_operation_us":read_started.elapsed().as_micros() as u64,
            "active_jobs":progress["active_jobs"],"index_ready":progress["index_ready"],"initial_canonical_clone_complete":progress["initial_canonical_clone_complete"]})
        );
        samples += 1;
        tokio::select! { _ = stop.cancelled() => break, _ = tokio::time::sleep(Duration::from_secs(5)) => {} }
    }
    let observed = started.elapsed().as_secs_f64();
    stop.cancel();
    for worker in workers {
        worker.await.map_err(|_| E::StorageCorrupt)??;
    }
    if observed < seconds as f64 || metrics.published.load(Ordering::Relaxed) == 0 {
        return Err(E::StorageUnavailable);
    }
    let watermark = store.watermark().await?;
    let mut acknowledged = store.acknowledged_index().await?;
    while acknowledged < watermark {
        replay(&store, &index, &mut acknowledged).await?;
    }
    if index.snapshot()?.generation != watermark {
        return Err(E::StorageCorrupt);
    }
    metrics.indexed.store(acknowledged, Ordering::Relaxed);
    let final_hash = archive_hash(&store, &captures).await?;
    if final_hash != original_hash {
        return Err(E::StorageCorrupt);
    }
    for pair in captures.as_chunks::<2>().0 {
        let head = store
            .resolve(
                pair[1].record.object.clone(),
                RevisionSelector::Head,
                now(),
                CancellationToken::new(),
            )
            .await?;
        if head.capture_id != pair[1].capture_id {
            return Err(E::StorageCorrupt);
        }
        let historical = store
            .resolve(
                pair[0].record.object.clone(),
                RevisionSelector::Revision {
                    id: pair[0].record.revision_id.clone(),
                },
                now(),
                CancellationToken::new(),
            )
            .await?;
        if historical.capture_id != pair[0].capture_id {
            return Err(E::StorageCorrupt);
        }
    }
    let states: Vec<(String, i64)> = sqlx::query_as(
        "SELECT status,count(*) FROM openlegal.corpus_job GROUP BY status ORDER BY status",
    )
    .fetch_all(&pool)
    .await
    .map_err(|_| E::StorageUnavailable)?;
    if states
        .iter()
        .any(|(status, count)| *count > 0 && !matches!(status.as_str(), "done" | "pending"))
    {
        return Err(E::StorageCorrupt);
    }
    println!(
        "{}",
        json!({"kind":"completed","status":"passed","schema_version":1,"observed_monotonic_secs":observed,
        "samples":samples,"metrics":metrics.json(),"final_event_watermark":watermark,"final_index_ack":acknowledged,
        "initial_archive_hash":original_hash,"final_archive_hash":final_hash,"jobs":states,"provider_http_requests":0})
    );
    base.close().await.map_err(|_| E::StorageUnavailable)?;
    Ok(())
}

#[tokio::main]
async fn main() {
    let args: Vec<_> = std::env::args().skip(1).collect();
    let seconds = if args.is_empty() {
        Some(7200)
    } else if args.len() == 2 && args[0] == "--duration-secs" {
        args[1].parse::<u64>().ok()
    } else {
        None
    };
    let Some(seconds) = seconds.filter(|s| (1..=86400).contains(s)) else {
        eprintln!("corpus_contention_soak: invalid_arguments");
        std::process::exit(2);
    };
    if let Err(error) = run(seconds).await {
        // Typed error names only; URLs, SQL statements and credentials stay private.
        eprintln!("corpus_contention_soak: {error:?}");
        std::process::exit(1);
    }
}
