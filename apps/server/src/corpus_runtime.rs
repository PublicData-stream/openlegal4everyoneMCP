//! Operator-selected corpus composition. Public tools never launch ingestion themselves.
use crate::{ServerError, config::DatabaseConfig};
use openlegal_adapters::{
    blob::FsBlobStore,
    corpus::PgCorpusStore,
    corpus_search::CorpusSearch,
    law_go_kr::{InventoryItem, LawClient},
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
    blobs: Arc<FsBlobStore>,
    provider: Option<LawClient>,
    retain_history_bodies: bool,
    inventory_verified: Arc<std::sync::atomic::AtomicBool>,
}
fn now() -> u64 {
    SystemClock::default().now()
}
impl CorpusRuntime {
    pub async fn open(
        config: &DatabaseConfig,
        persistent: &Arc<openlegal_adapters::postgres::PostgresStore>,
    ) -> Result<Arc<Self>, ServerError> {
        config.validate()?;
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
            Some(LawClient::new(secret, Arc::new(processor))?)
        } else {
            None
        };
        let index_path = std::path::absolute(&config.index_path)?;
        let index = tokio::task::spawn_blocking(move || CorpusIndex::open(&index_path)).await??;
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
            blobs,
            provider,
            inventory_verified,
            retain_history_bodies: config
                .ingestion
                .as_ref()
                .is_some_and(|c| c.retain_history_bodies),
        }))
    }
    pub async fn close(&self) -> Result<(), ServerError> {
        self.blobs.close().await?;
        Ok(())
    }
    pub async fn run(self: Arc<Self>, cancel: CancellationToken) -> Result<(), ServerError> {
        let child = cancel.child_token();
        let mut tasks = tokio::task::JoinSet::new();
        if self.provider.is_some() {
            let runtime = self.clone();
            let token = child.clone();
            tasks.spawn(async move {
                let provider = runtime
                    .provider
                    .as_ref()
                    .ok_or(DatabaseError::InvalidInput)?;
                runtime.ingest(provider, token).await
            });
            let runtime = self.clone();
            let token = child.clone();
            tasks.spawn(async move { runtime.process_jobs(token).await });
        }
        let mut maintenance = tokio::time::Instant::now();
        let result = loop {
            tokio::select! {
                _ = cancel.cancelled() => break Ok(()),
                result = tasks.join_next(), if !tasks.is_empty() => break match result {
                    Some(Ok(Ok(()))) if cancel.is_cancelled() => Ok(()),
                    Some(Ok(Err(e))) => Err(e),
                    _ => Err(DatabaseError::StorageUnavailable),
                },
                _ = tokio::time::sleep(Duration::from_secs(1)) => {
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
            let index = self.index.clone();
            let sequence = event.sequence;
            if event.withdrawn {
                tokio::task::spawn_blocking(move || index.remove_object(&event.object, sequence))
                    .await
                    .map_err(|_| DatabaseError::Capacity)??;
            } else if event.removed {
                let id = event.capture_id.ok_or(DatabaseError::StorageCorrupt)?;
                tokio::task::spawn_blocking(move || {
                    index.remove_capture(&event.object, &id, sequence)
                })
                .await
                .map_err(|_| DatabaseError::Capacity)??;
            } else {
                let capture = self.store.index_capture(&event, cancel.clone()).await?;
                tokio::task::spawn_blocking(move || match capture {
                    Some(capture) => index.apply_capture(capture, event.install_head, sequence),
                    None => index.advance_generation(sequence),
                })
                .await
                .map_err(|_| DatabaseError::Capacity)??;
            }
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
            let mut stable_all = true;
            for dataset in [
                Dataset::NationalStatute,
                Dataset::Ordinance,
                Dataset::Precedent,
            ] {
                if cancel.is_cancelled() {
                    return Ok(());
                }
                if dataset != Dataset::Precedent {
                    self.store
                        .mark_dataset_inventory_complete(dataset, false)
                        .await?;
                }
                let mut stable = true;
                for historical in [false, true] {
                    if historical && dataset == Dataset::Precedent {
                        continue;
                    }
                    let first = self
                        .scan_inventory(provider, dataset, historical, cancel.clone())
                        .await;
                    let second = if first.is_ok() {
                        self.scan_inventory(provider, dataset, historical, cancel.clone())
                            .await
                    } else {
                        Err(DatabaseError::HistoryIncomplete)
                    };
                    if first.is_err() || first.ok() != second.ok() {
                        stable = false;
                        break;
                    }
                }
                stable_all &= stable;
                if stable && dataset != Dataset::Precedent {
                    self.store
                        .mark_dataset_inventory_complete(dataset, true)
                        .await?;
                }
            }
            self.inventory_verified
                .store(stable_all, std::sync::atomic::Ordering::Release);
            tokio::select! {_=cancel.cancelled()=>return Ok(()),_=tokio::time::sleep(Duration::from_secs(3600))=>{}}
        }
    }
    /// Provider pagination is not atomic. Two identical observed traversals are
    /// required before using this catalog for exact unique date selection.
    async fn scan_inventory(
        &self,
        provider: &LawClient,
        dataset: Dataset,
        historical: bool,
        cancel: CancellationToken,
    ) -> Result<(Vec<u8>, u64), DatabaseError> {
        use sha2::{Digest, Sha256};
        let mut page = 1;
        let mut expected = None;
        let mut digest = Sha256::new();
        let mut seen = std::collections::HashSet::new();
        loop {
            let (items, done, total) = provider
                .inventory_page(dataset, page, historical, None, cancel.clone())
                .await?;
            if total > 2_000_000 || expected.is_some_and(|n| n != total) {
                return Err(DatabaseError::HistoryIncomplete);
            }
            expected = Some(total);
            for item in items {
                if cancel.is_cancelled() {
                    return Err(DatabaseError::Cancelled);
                }
                let bytes = serde_json::to_vec(&(
                    &item.object,
                    &item.revision_id,
                    &item.publication_date,
                    &item.effective_date,
                ))
                .map_err(|_| DatabaseError::StorageCorrupt)?;
                let identity = serde_json::to_vec(&(&item.object, &item.revision_id))
                    .map_err(|_| DatabaseError::StorageCorrupt)?;
                let key = Sha256::digest(&identity).to_vec();
                if !seen.insert(key) {
                    return Err(DatabaseError::HistoryIncomplete);
                }
                digest.update((bytes.len() as u64).to_be_bytes());
                digest.update(bytes);
                if dataset != Dataset::Precedent {
                    self.store
                        .record_revision_catalog(
                            &item.object,
                            &item.revision_id,
                            item.publication_date.as_deref(),
                            item.effective_date.as_deref(),
                            now(),
                        )
                        .await?;
                }
                if !historical || self.retain_history_bodies {
                    loop {
                        match self
                            .refresh(provider, item.clone(), !historical, cancel.clone())
                            .await
                        {
                            Ok(()) => break,
                            Err(DatabaseError::Capacity) => {
                                tokio::select! {_=cancel.cancelled()=>return Err(DatabaseError::Cancelled),_=tokio::time::sleep(Duration::from_secs(5))=>{}}
                            }
                            Err(e) => return Err(e),
                        }
                    }
                }
            }
            if done {
                if seen.len() as u64 != total {
                    return Err(DatabaseError::HistoryIncomplete);
                }
                return Ok((digest.finalize().to_vec(), total));
            }
            page += 1;
        }
    }
    async fn refresh(
        &self,
        _provider: &LawClient,
        item: InventoryItem,
        install_head: bool,
        cancel: CancellationToken,
    ) -> Result<(), DatabaseError> {
        let selector = if install_head {
            RevisionSelector::Head
        } else {
            RevisionSelector::Revision {
                id: item.revision_id.clone(),
            }
        };
        let old = self
            .store
            .resolve(item.object.clone(), selector, now(), cancel)
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
        let mut metadata = std::collections::BTreeMap::new();
        metadata.insert("title".into(), item.title.clone());
        if let Some(v) = &item.case_number {
            metadata.insert("case_number".into(), v.clone());
        }
        if let Some(v) = &item.data_source {
            metadata.insert("data_source".into(), v.clone());
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
                    let admission = tokio::time::timeout(
                        Duration::from_secs(10),
                        tokio::task::spawn_blocking(move || index.validate_record(&candidate)),
                    )
                    .await;
                    if !matches!(admission, Ok(Ok(Ok(())))) {
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
