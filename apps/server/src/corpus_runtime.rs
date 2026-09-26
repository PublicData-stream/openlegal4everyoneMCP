//! Operator-selected corpus composition. Public tools never launch ingestion themselves.
use crate::{
    ServerError,
    config::{DatabaseConfig, IngestionMode},
};
use openlegal_adapters::{
    blob::FsBlobStore,
    corpus::{CorpusRuntimeLease, PgCorpusStore},
    corpus_search::CorpusSearch,
    korean_analysis::KoreanAnalyzer,
    law_go_kr::{InventoryItem, LawClient, RequestBudgetMode},
    search_index::CorpusIndex,
};
use openlegal_application::{
    Clock, SystemClock,
    blob::BlobStore,
    database::{DatabaseService, DatabaseStore, Publication},
    search::SearchService,
};
use openlegal_domain::legal::{DatabaseError, Dataset, RevisionSelector};
use std::{sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;
pub struct CorpusRuntime {
    pub store: Arc<PgCorpusStore>,
    pub database: Arc<DatabaseService>,
    pub reader: Arc<openlegal_application::database_read::DatabaseReader>,
    pub search: Arc<SearchService>,
    index: Arc<CorpusIndex>,
    lease: CorpusRuntimeLease,
    blobs: Arc<FsBlobStore>,
    provider: Option<LawClient>,
    retain_history_bodies: bool,
    ingestion_mode: Option<IngestionMode>,
    pilot_candidates: Vec<InventoryItem>,
    inventory_verified: Arc<std::sync::atomic::AtomicBool>,
}
fn now() -> u64 {
    SystemClock::default().now()
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ManualPilotManifest {
    version: u8,
    candidates: Vec<InventoryItem>,
}

async fn load_pilot_candidates(config: &DatabaseConfig) -> Result<Vec<InventoryItem>, ServerError> {
    let Some(path) = config
        .ingestion
        .as_ref()
        .filter(|c| c.enabled)
        .and_then(|c| c.manual_candidates_path.as_ref())
    else {
        return Ok(Vec::new());
    };
    use tokio::io::AsyncReadExt;
    let mut bytes = Vec::new();
    tokio::fs::File::open(path)
        .await?
        .take(32 * 1024 + 1)
        .read_to_end(&mut bytes)
        .await?;
    if bytes.len() > 32 * 1024 {
        return Err("manual pilot candidate manifest exceeds 32 KiB".into());
    }
    let manifest: ManualPilotManifest = serde_json::from_slice(&bytes)?;
    if manifest.version != 1 || manifest.candidates.len() > 18 {
        return Err("invalid manual pilot candidate manifest version or size".into());
    }
    let mut seen = std::collections::HashSet::new();
    let mut counts = std::collections::HashMap::new();
    for item in &manifest.candidates {
        item.validate_for_detail()?;
        if item.object.jurisdiction != "kr"
            || item.object.provider != "law_go_kr"
            || item.title.len() > 512
            || item.revision_id.len() > 128
            || !item.object.id.bytes().all(|b| b.is_ascii_digit())
            || !seen.insert((item.object.clone(), item.revision_id.clone()))
        {
            return Err("invalid or duplicate manual pilot candidate".into());
        }
        let class = if item.object.dataset == Dataset::Treaty {
            match item.treaty_class_code.as_deref() {
                Some("440101") => 1,
                Some("440102") => 2,
                _ => return Err("invalid manual treaty class".into()),
            }
        } else {
            if item.treaty_class_code.is_some() {
                return Err("unexpected manual treaty class".into());
            }
            0
        };
        let count = counts.entry((item.object.dataset, class)).or_insert(0usize);
        *count += 1;
        if *count > 2 {
            return Err("too many manual pilot candidates for a category".into());
        }
    }
    Ok(manifest.candidates)
}

/// Rebuild derived state into a fresh configured index directory while corpus
/// serving, ingestion and retention are stopped. No provider is constructed.
pub async fn rebuild_corpus_index(
    config: &DatabaseConfig,
    persistent: &Arc<openlegal_adapters::postgres::PostgresStore>,
    cancel: CancellationToken,
) -> Result<u64, ServerError> {
    config.validate()?;
    let blobs =
        FsBlobStore::open_with_limit(&std::path::absolute(&config.blob_path)?, 100 * 1024 * 1024)
            .await?;
    let store = PgCorpusStore::new(persistent.pool(), blobs.clone());
    let result: Result<u64, ServerError> = async {
        store.health().await?;
        let lease = store.acquire_runtime_lease().await?;
        let result = rebuild_with_lease(config, &store, &lease, &cancel).await;
        let closed = lease.close().await;
        let generation = result?;
        closed?;
        Ok(generation)
    }
    .await;
    let closed = blobs.close().await;
    let generation = result?;
    closed?;
    Ok(generation)
}

async fn rebuild_with_lease(
    config: &DatabaseConfig,
    store: &PgCorpusStore,
    lease: &CorpusRuntimeLease,
    cancel: &CancellationToken,
) -> Result<u64, ServerError> {
    if cancel.is_cancelled() {
        return Err(DatabaseError::Cancelled.into());
    }
    let target = store.watermark().await?;
    let index_path = std::path::absolute(&config.index_path)?;
    let dictionary_path = std::path::absolute(&config.mecab_dictionary_path)?;
    let index = tokio::task::spawn_blocking(move || {
        CorpusIndex::create_rebuild(&index_path, KoreanAnalyzer::open(&dictionary_path)?)
    })
    .await??;
    let mut generation = 0;
    while generation < target {
        lease.check().await?;
        let events = store.outbox(generation, 100).await?;
        if events.is_empty() {
            return Err(DatabaseError::StorageCorrupt.into());
        }
        for event in events {
            if cancel.is_cancelled() {
                return Err(DatabaseError::Cancelled.into());
            }
            if event.sequence != generation + 1 || event.sequence > target {
                return Err(DatabaseError::StorageCorrupt.into());
            }
            let sequence = event.sequence;
            apply_index_event(&index, store, event, cancel).await?;
            generation = sequence;
        }
    }
    lease.check().await?;
    if cancel.is_cancelled() {
        return Err(DatabaseError::Cancelled.into());
    }
    if store.watermark().await? != target || index.snapshot()?.generation != target {
        return Err(DatabaseError::Conflict.into());
    }
    tokio::task::spawn_blocking(move || index.finish_rebuild()).await??;
    // Never acknowledge intermediate replay generations: the old index may
    // already have acknowledged a later event. The durable fence is monotonic.
    store.acknowledge_index(target).await?;
    Ok(target)
}

async fn apply_index_event(
    index: &Arc<CorpusIndex>,
    store: &PgCorpusStore,
    event: openlegal_application::database::OutboxEntry,
    cancel: &CancellationToken,
) -> Result<(), DatabaseError> {
    let index = index.clone();
    let sequence = event.sequence;
    if event.withdrawn {
        tokio::task::spawn_blocking(move || index.remove_object(&event.object, sequence))
            .await
            .map_err(|_| DatabaseError::Capacity)??;
    } else if event.removed {
        let id = event.capture_id.ok_or(DatabaseError::StorageCorrupt)?;
        tokio::task::spawn_blocking(move || index.remove_capture(&event.object, &id, sequence))
            .await
            .map_err(|_| DatabaseError::Capacity)??;
    } else {
        let capture = store.index_capture(&event, cancel.clone()).await?;
        let worker_cancel = cancel.clone();
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        tokio::task::spawn_blocking(move || match capture {
            Some(capture) => index.apply_capture_with_budget(
                capture,
                event.install_head,
                sequence,
                deadline,
                &worker_cancel,
            ),
            None => index.advance_generation(sequence),
        })
        .await
        .map_err(|_| DatabaseError::Capacity)??;
    }
    Ok(())
}
impl CorpusRuntime {
    pub async fn open(
        config: &DatabaseConfig,
        persistent: &Arc<openlegal_adapters::postgres::PostgresStore>,
    ) -> Result<Arc<Self>, ServerError> {
        config.validate()?;
        let pilot_candidates = load_pilot_candidates(config).await?;
        let provider = if let Some(c) = config.ingestion.as_ref().filter(|c| c.enabled) {
            let processor = openlegal_adapters::document_jobs::KubernetesDocumentProcessor::new(
                c.kubectl.clone(),
                c.kubeconfig.clone(),
                c.context.clone(),
                c.namespace.clone(),
                c.worker_image.clone(),
            )
            .map_err(|_| "invalid document sandbox configuration")?;
            let secret = std::env::var(&c.credential_env)
                .map_err(|_| "legal provider credential environment is missing")?;
            let mode = match c.mode {
                IngestionMode::Pilot => RequestBudgetMode::Pilot,
                IngestionMode::Continuous => RequestBudgetMode::Continuous,
            };
            Some(
                LawClient::new(secret, Arc::new(processor))?
                    .with_request_budget(persistent.pool(), mode),
            )
        } else {
            None
        };
        let blobs = FsBlobStore::open_with_limit(
            &std::path::absolute(&config.blob_path)?,
            100 * 1024 * 1024,
        )
        .await?;
        let store = Arc::new(PgCorpusStore::new(persistent.pool(), blobs.clone()));
        if let Err(e) = store.health().await {
            let _ = blobs.close().await;
            return Err(e.into());
        }
        let lease = match store.acquire_runtime_lease().await {
            Ok(lease) => lease,
            Err(error) => {
                let _ = blobs.close().await;
                return Err(error.into());
            }
        };
        // The incremental scanner cannot preserve a prior release's complete
        // inventory claim, even when this serving instance has ingestion off.
        for dataset in [
            Dataset::NationalStatute,
            Dataset::AdministrativeRule,
            Dataset::Ordinance,
        ] {
            if let Err(error) = store.mark_dataset_inventory_complete(dataset, false).await {
                let _ = lease.close().await;
                let _ = blobs.close().await;
                return Err(error.into());
            }
        }
        let index_path = std::path::absolute(&config.index_path)?;
        let dictionary_path = std::path::absolute(&config.mecab_dictionary_path)?;
        let opened: Result<_, ServerError> = async {
            let index = tokio::task::spawn_blocking(move || {
                CorpusIndex::open(&index_path, KoreanAnalyzer::open(&dictionary_path)?)
            })
            .await??;
            let generation = index.snapshot()?.generation;
            if generation < store.acknowledged_index().await?
                || generation > store.watermark().await?
            {
                return Err("corpus index generation is incompatible with PostgreSQL; stop serving and run --rebuild-corpus-index with a fresh index_path".into());
            }
            Ok(index)
        }
        .await;
        let index = match opened {
            Ok(index) => index,
            Err(error) => {
                let _ = lease.close().await;
                let _ = blobs.close().await;
                return Err(error);
            }
        };
        let database = Arc::new(DatabaseService::new(
            store.clone(),
            Arc::new(SystemClock::default()),
        ));
        let reader = Arc::new(openlegal_application::database_read::DatabaseReader::new(
            database.clone(),
            Arc::new(openlegal_adapters::corpus_read::CorpusReadRetention(
                store.clone(),
            )),
            Arc::new(SystemClock::default()),
        ));
        let inventory_verified = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let search = Arc::new(SearchService::new(Arc::new(
            CorpusSearch::new(index.clone(), store.clone())
                .with_coverage(inventory_verified.clone()),
        )));
        Ok(Arc::new(Self {
            store,
            database,
            reader,
            search,
            index,
            lease,
            blobs,
            provider,
            ingestion_mode: config
                .ingestion
                .as_ref()
                .filter(|c| c.enabled)
                .map(|c| c.mode),
            pilot_candidates,
            inventory_verified,
            retain_history_bodies: config
                .ingestion
                .as_ref()
                .is_some_and(|c| c.retain_history_bodies),
        }))
    }
    pub async fn close(&self) -> Result<(), ServerError> {
        let blobs = self.blobs.close().await;
        let lease = self.lease.close().await;
        blobs?;
        lease?;
        Ok(())
    }
    pub async fn run(self: Arc<Self>, cancel: CancellationToken) -> Result<(), ServerError> {
        let child = cancel.child_token();
        let mut tasks = tokio::task::JoinSet::new();
        if self.provider.is_some() {
            let ingestion = child.child_token();
            let runtime = self.clone();
            let token = ingestion.clone();
            tasks.spawn(async move {
                let provider = runtime
                    .provider
                    .as_ref()
                    .ok_or(DatabaseError::InvalidInput)?;
                match runtime.ingestion_mode {
                    Some(IngestionMode::Pilot) => {
                        let result = runtime.ingest_pilot(provider, token.clone()).await;
                        if token.is_cancelled() && result == Err(DatabaseError::Cancelled) {
                            Ok(())
                        } else {
                            result
                        }
                    }
                    Some(IngestionMode::Continuous) => runtime.ingest(provider, token).await,
                    None => Err(DatabaseError::InvalidInput),
                }
            });
            let runtime = self.clone();
            let token = ingestion;
            tasks.spawn(async move { runtime.process_jobs(token).await });
        }
        let mut maintenance = tokio::time::Instant::now();
        let result = loop {
            tokio::select! {
                _ = cancel.cancelled() => break Ok(()),
                result = tasks.join_next(), if !tasks.is_empty() => break match result {
                    Some(Ok(Ok(()))) if cancel.is_cancelled() => Ok(()),
                    Some(Ok(Ok(()))) if self.ingestion_mode == Some(IngestionMode::Pilot) => continue,
                    Some(Ok(Err(e))) => Err(e),
                    _ => Err(DatabaseError::StorageUnavailable),
                },
                _ = tokio::time::sleep(Duration::from_secs(1)) => {
                    if let Err(e) = self.lease.check().await {
                        break Err(e);
                    }
                    if let Err(e) = self.index_events(&child).await {
                        break Err(e);
                    }
                    if let Err(e) = self.store.health().await {
                        break Err(e);
                    }
                    if maintenance.elapsed() >= Duration::from_secs(60) {
                        if let Err(e) = self.store.maintain(now(), now().saturating_sub(30 * 86400)).await {
                            break Err(e);
                        }
                        maintenance = tokio::time::Instant::now();
                    }
                }
            }
        };
        child.cancel();
        while let Some(joined) = tasks.join_next().await {
            joined??;
        }
        result?;
        Ok(())
    }
    async fn index_events(&self, cancel: &CancellationToken) -> Result<(), DatabaseError> {
        let mut generation = self.index.snapshot()?.generation;
        for event in self.store.outbox(generation, 100).await? {
            if cancel.is_cancelled() {
                break;
            }
            if event.sequence != generation + 1 {
                return Err(DatabaseError::StorageCorrupt);
            }
            self.lease.check().await?;
            let sequence = event.sequence;
            apply_index_event(&self.index, &self.store, event, cancel).await?;
            generation = sequence;
            self.store.acknowledge_index(generation).await?;
        }
        Ok(())
    }
    async fn ingest(
        &self,
        provider: &LawClient,
        cancel: CancellationToken,
    ) -> Result<(), DatabaseError> {
        loop {
            self.inventory_verified
                .store(false, std::sync::atomic::Ordering::Release);
            for dataset in [
                Dataset::NationalStatute,
                Dataset::AdministrativeRule,
                Dataset::Ordinance,
                Dataset::Treaty,
                Dataset::Precedent,
                Dataset::ConstitutionalDecision,
                Dataset::LegalInterpretation,
                Dataset::AdministrativeAppeal,
            ] {
                if cancel.is_cancelled() {
                    return Ok(());
                }
                let page = self.store.inventory_cursor(dataset, false).await?;
                if page > 1 {
                    // Moving offset pages can shift records behind the cursor.
                    // Alternate a front-page refresh with one-page overlap;
                    // neither observation certifies a complete inventory.
                    let revisit = if (now() / 3600).is_multiple_of(2) {
                        1
                    } else {
                        page - 1
                    };
                    match provider
                        .inventory_page(dataset, revisit, false, None, cancel.clone())
                        .await
                    {
                        Ok((overlap, _, _)) => {
                            for item in overlap {
                                self.refresh_with_backpressure(
                                    provider,
                                    item,
                                    true,
                                    cancel.clone(),
                                )
                                .await?;
                            }
                        }
                        Err(DatabaseError::Capacity | DatabaseError::BudgetExhausted) => break,
                        Err(error) => return Err(error),
                    }
                }
                let (items, done, _) = match provider
                    .inventory_page(dataset, page, false, None, cancel.clone())
                    .await
                {
                    Ok(result) => result,
                    Err(DatabaseError::Capacity | DatabaseError::BudgetExhausted) => break,
                    Err(error) => return Err(error),
                };
                for item in &items {
                    self.refresh_with_backpressure(provider, item.clone(), true, cancel.clone())
                        .await?;
                }
                if !self.await_page_publication(&items, true, &cancel).await? {
                    continue;
                }
                self.store
                    .advance_inventory_cursor(dataset, false, page, done)
                    .await?;
            }
            for dataset in [
                Dataset::NationalStatute,
                Dataset::AdministrativeRule,
                Dataset::Ordinance,
            ] {
                if cancel.is_cancelled() {
                    return Ok(());
                }
                let page = self.store.inventory_cursor(dataset, true).await?;
                let (items, done, _) = match provider
                    .inventory_page(dataset, page, true, None, cancel.clone())
                    .await
                {
                    Ok(result) => result,
                    Err(DatabaseError::Capacity | DatabaseError::BudgetExhausted) => break,
                    Err(error) => return Err(error),
                };
                for item in &items {
                    self.store
                        .record_revision_catalog(
                            &item.object,
                            &item.revision_id,
                            item.publication_date.as_deref(),
                            item.effective_date.as_deref(),
                            now(),
                        )
                        .await?;
                    if self.retain_history_bodies {
                        self.refresh_with_backpressure(
                            provider,
                            item.clone(),
                            false,
                            cancel.clone(),
                        )
                        .await?;
                    }
                }
                if self.retain_history_bodies
                    && !self.await_page_publication(&items, false, &cancel).await?
                {
                    continue;
                }
                self.store
                    .advance_inventory_cursor(dataset, true, page, done)
                    .await?;
            }
            // A few moving pages cannot establish an atomic, complete upstream
            // catalog. Keep exact-date selectors and completeness claims closed.
            tokio::select! {_=cancel.cancelled()=>return Ok(()),_=tokio::time::sleep(Duration::from_secs(3600))=>{}}
        }
    }
    /// A page cursor only passes records whose required detail publication has
    /// succeeded. Failed jobs keep that page due for a later scan instead of
    /// disappearing behind a durable cursor.
    async fn await_page_publication(
        &self,
        items: &[InventoryItem],
        head: bool,
        cancel: &CancellationToken,
    ) -> Result<bool, DatabaseError> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(900);
        loop {
            if cancel.is_cancelled() {
                return Ok(false);
            }
            let mut ready = true;
            for item in items {
                if head {
                    if !self
                        .store
                        .head_revision_ready(&item.object, &item.revision_id, now())
                        .await?
                    {
                        ready = false;
                        break;
                    }
                    continue;
                }
                if !self
                    .store
                    .revision_capture_recent(&item.object, &item.revision_id, now())
                    .await?
                {
                    ready = false;
                    break;
                }
            }
            if ready {
                return Ok(true);
            }
            if tokio::time::Instant::now() >= deadline {
                return Ok(false);
            }
            tokio::select! {
                _ = cancel.cancelled() => return Ok(false),
                _ = tokio::time::sleep(Duration::from_secs(5)) => {}
            }
        }
    }
    /// Select a few identities from each documented list family. The pilot
    /// never certifies inventory completeness or walks historical catalogs.
    /// The shared PostgreSQL request ledger enforces 100 attempts in 30 minutes.
    async fn ingest_pilot(
        &self,
        provider: &LawClient,
        cancel: CancellationToken,
    ) -> Result<(), DatabaseError> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(1800);
        self.inventory_verified
            .store(false, std::sync::atomic::Ordering::Release);
        let mut list_failures = 0usize;
        let mut selected_total = 0usize;
        let mut scanned_families = 0usize;
        for (dataset, class) in [
            (Dataset::NationalStatute, None),
            (Dataset::AdministrativeRule, None),
            (Dataset::Ordinance, None),
            (Dataset::Treaty, Some(1)),
            (Dataset::Treaty, Some(2)),
            (Dataset::Precedent, None),
            (Dataset::ConstitutionalDecision, None),
            (Dataset::LegalInterpretation, None),
            (Dataset::AdministrativeAppeal, None),
        ] {
            if cancel.is_cancelled() {
                return Ok(());
            }
            for item in self.pilot_candidates.iter().filter(|item| {
                item.object.dataset == dataset
                    && (dataset != Dataset::Treaty
                        || item.treaty_class_code.as_deref()
                            == Some(if class == Some(1) { "440101" } else { "440102" }))
            }) {
                // Manual exports may be stale or include repealed records.
                // Only a fresh current-list observation may install HEAD.
                let mut hint = item.clone();
                hint.title.clear();
                hint.data_source = None;
                hint.case_number = None;
                hint.treaty_class_code = None;
                match tokio::time::timeout_at(
                    deadline,
                    self.refresh_with_backpressure(provider, hint, false, cancel.clone()),
                )
                .await
                {
                    Ok(Ok(())) => {}
                    Ok(Err(DatabaseError::Cancelled)) if cancel.is_cancelled() => return Ok(()),
                    Err(_) => {
                        cancel.cancel();
                        return Ok(());
                    }
                    Ok(Err(error)) => {
                        eprintln!(
                            "law provider pilot: manual candidate for {dataset:?} class {class:?} failed: {error:?}"
                        );
                        return Err(error);
                    }
                }
            }
            let mut selected = 0;
            for page in 1..=5 {
                let page_result = tokio::time::timeout_at(
                    deadline,
                    provider.inventory_page_class(
                        dataset,
                        page,
                        false,
                        None,
                        class,
                        cancel.clone(),
                    ),
                )
                .await;
                let (items, done, _) = match page_result {
                    Ok(Ok(page)) => page,
                    Ok(Err(DatabaseError::BudgetExhausted)) => {
                        cancel.cancel();
                        return Ok(());
                    }
                    Ok(Err(DatabaseError::Capacity | DatabaseError::ProcessingPending)) => {
                        eprintln!(
                            "law provider pilot: {dataset:?} class {class:?} inventory page {page} could not be processed within admission limits; stopping pilot"
                        );
                        cancel.cancel();
                        return Ok(());
                    }
                    // A rejected provider list is not evidence that retained
                    // storage is corrupt. Record the incomplete family and
                    // continue the bounded pilot without taking serving down.
                    Ok(Err(DatabaseError::StorageCorrupt)) => {
                        eprintln!(
                            "law provider pilot: rejected {dataset:?} class {class:?} inventory page {page}; family incomplete"
                        );
                        list_failures += 1;
                        break;
                    }
                    // SourceRejected also covers HTTP authentication/client
                    // rejection. Durably stop the pilot so a restart cannot
                    // repeat requests with a bad credential.
                    Ok(Err(DatabaseError::SourceRejected)) => {
                        eprintln!(
                            "law provider pilot: source rejected {dataset:?} class {class:?} inventory page {page}; suspending provider requests; preceding families may be partial"
                        );
                        cancel.cancel();
                        return Ok(());
                    }
                    Ok(Err(DatabaseError::Cancelled)) if cancel.is_cancelled() => return Ok(()),
                    Err(_) => {
                        cancel.cancel();
                        return Ok(());
                    }
                    Ok(Err(error)) => {
                        eprintln!(
                            "law provider pilot: {dataset:?} class {class:?} inventory page {page} failed: {error:?}"
                        );
                        return Err(error);
                    }
                };
                for item in items {
                    if dataset == Dataset::Treaty {
                        let expected = if class == Some(1) { "440101" } else { "440102" };
                        if item.treaty_class_code.as_deref() != Some(expected) {
                            continue;
                        }
                    }
                    match tokio::time::timeout_at(
                        deadline,
                        self.refresh_with_backpressure(provider, item, true, cancel.clone()),
                    )
                    .await
                    {
                        Ok(Ok(())) => {}
                        Ok(Err(DatabaseError::Cancelled)) if cancel.is_cancelled() => return Ok(()),
                        Err(_) => {
                            cancel.cancel();
                            return Ok(());
                        }
                        Ok(Err(error)) => {
                            eprintln!(
                                "law provider pilot: {dataset:?} class {class:?} observed candidate failed: {error:?}"
                            );
                            return Err(error);
                        }
                    }
                    selected += 1;
                    selected_total += 1;
                    if selected >= 2 {
                        break;
                    }
                }
                if selected >= 2 || done {
                    break;
                }
            }
            if selected == 0 {
                eprintln!("law provider pilot: no usable current-list candidate for {dataset:?}");
            }
            scanned_families += 1;
        }
        eprintln!(
            "law provider pilot: scan finished; families_visited={scanned_families}, list_failures={list_failures}, head_candidates_queued={selected_total}; publication and inventory completeness not established"
        );
        tokio::select! {
            _ = cancel.cancelled() => {},
            _ = tokio::time::sleep_until(deadline) => cancel.cancel(),
        }
        Ok(())
    }
    async fn refresh_with_backpressure(
        &self,
        provider: &LawClient,
        item: InventoryItem,
        install_head: bool,
        cancel: CancellationToken,
    ) -> Result<(), DatabaseError> {
        loop {
            match self
                .refresh(provider, item.clone(), install_head, cancel.clone())
                .await
            {
                Err(DatabaseError::Capacity) => {
                    tokio::select! {
                        _ = cancel.cancelled() => return Err(DatabaseError::Cancelled),
                        _ = tokio::time::sleep(Duration::from_secs(5)) => {}
                    }
                }
                result => return result,
            }
        }
    }
    async fn refresh(
        &self,
        _provider: &LawClient,
        item: InventoryItem,
        install_head: bool,
        cancel: CancellationToken,
    ) -> Result<(), DatabaseError> {
        let replacement = if install_head {
            let old = self
                .store
                .resolve(item.object.clone(), RevisionSelector::Head, now(), cancel)
                .await;
            let replacement = match &old {
                Ok(c) => c.record.revision_id != item.revision_id,
                Err(
                    DatabaseError::NotFound
                    | DatabaseError::ProcessingPending
                    | DatabaseError::RevisionUnavailable,
                ) => true,
                Err(e) => return Err(*e),
            };
            if old
                .as_ref()
                .is_ok_and(|c| !replacement && now().saturating_sub(c.validated_at) < 3600)
            {
                return Ok(());
            }
            replacement
        } else {
            if self
                .store
                .revision_capture_recent(&item.object, &item.revision_id, now())
                .await?
            {
                return Ok(());
            }
            true
        };
        let mut metadata = std::collections::BTreeMap::new();
        metadata.insert("title".into(), item.title.clone());
        if let Some(v) = &item.case_number {
            metadata.insert("case_number".into(), v.clone());
        }
        if let Some(v) = &item.data_source {
            metadata.insert("data_source".into(), v.clone());
        }
        if let Some(v) = &item.treaty_class_code {
            metadata.insert("treaty_class_code".into(), v.clone());
        }
        self.store
            .enqueue_job_with_metadata(
                item.object,
                item.revision_id,
                item.effective_date,
                install_head,
                install_head && replacement,
                now(),
                metadata,
            )
            .await?;
        Ok(())
    }
    async fn process_jobs(&self, cancel: CancellationToken) -> Result<(), DatabaseError> {
        let provider = self.provider.as_ref().ok_or(DatabaseError::InvalidInput)?;
        loop {
            if cancel.is_cancelled() {
                return Ok(());
            }
            let Some(job) = self.store.claim_job(now()).await? else {
                tokio::select! {_=cancel.cancelled()=>return Ok(()),_=tokio::time::sleep(Duration::from_secs(1))=>{}}
                continue;
            };
            let item = InventoryItem {
                object: job.object.clone(),
                revision_id: job.revision_id.clone(),
                effective_date: job.effective_date.clone(),
                title: job
                    .source_metadata
                    .get("title")
                    .cloned()
                    .unwrap_or_default(),
                data_source: job.source_metadata.get("data_source").cloned(),
                case_number: job.source_metadata.get("case_number").cloned(),
                publication_date: None,
                treaty_class_code: job.source_metadata.get("treaty_class_code").cloned(),
            };
            let attempt = cancel.child_token();
            let _attempt_guard = attempt.clone().drop_guard();
            let detail = match tokio::time::timeout(
                Duration::from_secs(500),
                provider.detail(&item, attempt.clone()),
            )
            .await
            {
                Ok(result) => result,
                Err(_) => {
                    attempt.cancel();
                    Err(DatabaseError::Capacity)
                }
            };
            match detail {
                Ok(detail) => {
                    let candidate = detail.record.clone();
                    let index = self.index.clone();
                    let admission_cancel = attempt.child_token();
                    let _admission_guard = admission_cancel.clone().drop_guard();
                    let worker_cancel = admission_cancel.clone();
                    let deadline = std::time::Instant::now() + Duration::from_secs(10);
                    let admission = tokio::time::timeout(
                        Duration::from_secs(10),
                        tokio::task::spawn_blocking(move || {
                            index.validate_record_with_budget(&candidate, deadline, &worker_cancel)
                        }),
                    )
                    .await;
                    if !matches!(admission, Ok(Ok(Ok(())))) {
                        admission_cancel.cancel();
                        self.store.fail_claim(&job, false).await?;
                        continue;
                    }
                    let publication = tokio::time::timeout(
                        Duration::from_secs(40),
                        self.store.publish(
                            Publication {
                                record: detail.record,
                                raw: detail.raw,
                                additional_evidence: detail.additional_evidence,
                                processor_version: detail.processor_version,
                                retrieved_at: detail.retrieved_at,
                                now: now(),
                                expected_version: job.expected_version,
                                install_head: job.install_head,
                                job_id: Some(job.id.clone()),
                            },
                            attempt.clone(),
                        ),
                    )
                    .await
                    .unwrap_or_else(|_| {
                        attempt.cancel();
                        Err(DatabaseError::Capacity)
                    });
                    match publication {
                        Ok(_) => {}
                        Err(DatabaseError::Conflict | DatabaseError::Withdrawn) => {
                            self.store.fail_claim(&job, false).await?;
                        }
                        Err(DatabaseError::Capacity) => {
                            self.store.fail_claim(&job, true).await?;
                        }
                        Err(e) => return Err(e),
                    }
                }
                Err(DatabaseError::Cancelled) => {
                    self.store.fail_claim(&job, true).await?;
                    return Ok(());
                }
                Err(DatabaseError::BudgetExhausted) => {
                    let resume_at = provider.next_admissible_epoch().await?;
                    match self.store.defer_budget_claim(&job, resume_at).await {
                        Ok(()) | Err(DatabaseError::Conflict) => {}
                        Err(error) => return Err(error),
                    }
                    if self.ingestion_mode == Some(IngestionMode::Pilot) {
                        cancel.cancel();
                        return Ok(());
                    }
                    tokio::select! {_=cancel.cancelled()=>return Ok(()),_=tokio::time::sleep(Duration::from_secs(60))=>{}}
                }
                Err(DatabaseError::SourceRejected)
                    if self.ingestion_mode == Some(IngestionMode::Pilot) =>
                {
                    self.store.fail_claim(&job, false).await?;
                    eprintln!(
                        "law provider pilot: detail source rejected for {:?}; suspending provider requests; queued HEAD may remain pending",
                        job.object.dataset
                    );
                    cancel.cancel();
                    return Ok(());
                }
                Err(
                    DatabaseError::SourceRejected
                    | DatabaseError::InvalidInput
                    | DatabaseError::StorageCorrupt
                    | DatabaseError::NotFound
                    | DatabaseError::UnsupportedHistory
                    | DatabaseError::HistoryIncomplete,
                ) => {
                    self.store.fail_claim(&job, false).await?;
                }
                Err(_) => {
                    self.store.fail_claim(&job, true).await?;
                    tokio::select! {_=cancel.cancelled()=>return Ok(()),_=tokio::time::sleep(Duration::from_secs(5u64.saturating_pow(job.attempts.min(3))))=>{}}
                }
            }
        }
    }
}

#[cfg(test)]
mod manual_pilot_tests {
    use super::load_pilot_candidates;
    use crate::config::{DatabaseConfig, IngestionConfig, IngestionMode};

    #[tokio::test]
    async fn explicit_manifest_accepts_bounded_identity_hints_and_rejects_duplicates() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("candidates.json");
        let config = DatabaseConfig {
            blob_path: "blobs".into(),
            index_path: "index".into(),
            mecab_dictionary_path: "dictionary".into(),
            widget_html: "widget".into(),
            ingestion: Some(IngestionConfig {
                credential_env: "PROVIDER_CREDENTIAL".into(),
                kubectl: "/usr/local/bin/kubectl".into(),
                kubeconfig: "/run/kubeconfig".into(),
                context: "test".into(),
                namespace: "test".into(),
                worker_image: "example.invalid/worker@sha256:placeholder".into(),
                enabled: true,
                mode: IngestionMode::Pilot,
                manual_candidates_path: Some(path.clone()),
                retain_history_bodies: false,
            }),
        };
        let item = serde_json::json!({
            "object": {"jurisdiction":"kr","provider":"law_go_kr","dataset":"treaty","id":"17"},
            "revision_id":"17","effective_date":null,"publication_date":null,
            "title":"synthetic treaty","data_source":null,"case_number":null,
            "treaty_class_code":"440101"
        });
        tokio::fs::write(
            &path,
            serde_json::to_vec(&serde_json::json!({
                "version": 1, "candidates": [item.clone()]
            }))
            .unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(load_pilot_candidates(&config).await.unwrap().len(), 1);
        tokio::fs::write(
            &path,
            serde_json::to_vec(&serde_json::json!({
                "version": 1, "candidates": [item.clone(), item]
            }))
            .unwrap(),
        )
        .await
        .unwrap();
        assert!(load_pilot_candidates(&config).await.is_err());
    }
}
