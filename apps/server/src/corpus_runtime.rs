//! Operator-selected corpus composition and bounded demand collection.
use crate::{
    ServerError,
    config::{DatabaseConfig, IngestionMode},
};
use openlegal_adapters::{
    blob::FsBlobStore,
    corpus::{
        CloneView, CollectionLaunch, CorpusRuntimeLease, PageGapObservation, PgCorpusStore,
        SourceObservationInput, SupplementJobStatus,
    },
    corpus_search::CorpusSearch,
    korean_analysis::KoreanAnalyzer,
    law_go_kr::{InventoryItem, InventoryPage, LawClient, RequestBudgetMode},
    search_index::CorpusIndex,
};
use openlegal_application::{
    Clock, SystemClock,
    blob::BlobStore,
    database::{DatabaseService, DatabaseStore, Publication},
    search::SearchService,
};
use openlegal_domain::collection::{CollectionRequest, CollectionSearchMode, CollectionTarget};
use openlegal_domain::legal::{Capture, DatabaseError, Dataset, ObjectId, RevisionSelector};
use std::{sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;
pub struct CorpusRuntime {
    pub store: Arc<PgCorpusStore>,
    pub database: Arc<DatabaseService>,
    pub reader: Arc<openlegal_application::database_read::DatabaseReader>,
    pub search: Arc<SearchService>,
    index: Arc<CorpusIndex>,
    lease: Option<CorpusRuntimeLease>,
    blobs: Arc<FsBlobStore>,
    provider: Option<LawClient>,
    retain_history_bodies: bool,
    detail_timeout_secs: u64,
    detail_job_workers: u32,
    scan_interval_secs: u64,
    adaptive_polling: bool,
    ingestion_mode: Option<IngestionMode>,
    pilot_candidates: Vec<InventoryItem>,
    inventory_verified: Arc<std::sync::atomic::AtomicBool>,
}
async fn pilot_watchdog(duration: Duration, cancel: CancellationToken) {
    tokio::select! {
        _ = cancel.cancelled() => {},
        _ = tokio::time::sleep(duration) => cancel.cancel(),
    }
}

fn now() -> u64 {
    SystemClock::default().now()
}

fn comparable_dates_advance(
    current_effective: Option<&str>,
    current_publication: Option<&str>,
    previous_effective: Option<&str>,
    previous_publication: Option<&str>,
) -> bool {
    let pairs = [
        current_effective.zip(previous_effective),
        current_publication.zip(previous_publication),
    ];
    pairs
        .iter()
        .flatten()
        .any(|(current, previous)| current > previous)
        && pairs
            .iter()
            .flatten()
            .all(|(current, previous)| current >= previous)
}

fn select_precedent_case(
    pages: &[InventoryPage],
    case_number: &str,
    expected_id: Option<&str>,
) -> Result<InventoryItem, DatabaseError> {
    let observed = pages
        .iter()
        .map(|page| page.items.len() as u64)
        .sum::<u64>();
    let complete = pages.last().is_some_and(|page| page.done)
        && pages.first().and_then(|page| page.total) == Some(observed)
        && pages.iter().all(|page| {
            !page.incomplete
                && page.items.iter().all(|item| {
                    item.case_number
                        .as_deref()
                        .is_some_and(|case| !case.is_empty())
                })
                && page.total == pages[0].total
        });
    // Offset pagination is not an atomic snapshot. A multi-page response can
    // identify a caller-specified provider ID, but cannot prove uniqueness of
    // a case number across page movements.
    if !complete || (pages.len() > 1 && expected_id.is_none()) {
        return Err(DatabaseError::SourceInventoryIncomplete);
    }
    let mut exact = pages
        .iter()
        .flat_map(|page| &page.items)
        .filter(|item| item.case_number.as_deref() == Some(case_number));
    if let Some(id) = expected_id {
        if let Some(item) = exact.find(|item| item.object.id == id) {
            return Ok(item.clone());
        }
        if pages
            .iter()
            .flat_map(|page| &page.items)
            .any(|item| item.object.id == id || item.case_number.as_deref() == Some(case_number))
        {
            return Err(DatabaseError::Conflict);
        }
        return Err(DatabaseError::NotFound);
    }
    let first = exact.next().ok_or(DatabaseError::NotFound)?;
    if exact.any(|item| item.object.id != first.object.id) {
        return Err(DatabaseError::AmbiguousCollection);
    }
    Ok(first.clone())
}

fn provider_failure_reason(error: DatabaseError) -> &'static str {
    match error {
        DatabaseError::SourceUnavailable => "source_unavailable",
        DatabaseError::SourceDataInvalid => "source_data_invalid",
        DatabaseError::SourceDownloadFailed => "download_failed",
        _ => "worker_failed",
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CollectionSkipReason {
    Pending,
    AlreadyFresh,
    AlreadyInProgress,
    HeadObservationSuperseded,
    PublicationSuperseded,
}

impl CollectionSkipReason {
    fn code(self) -> &'static str {
        match self {
            Self::Pending => "collection_pending",
            Self::AlreadyFresh => "already_fresh",
            Self::AlreadyInProgress => "collection_already_in_progress",
            Self::HeadObservationSuperseded => "head_observation_superseded",
            Self::PublicationSuperseded => "publication_superseded",
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum CollectionItemOutcome {
    Published,
    Skipped(CollectionSkipReason),
}

#[derive(Default)]
struct ExplicitCollectionSummary {
    published: usize,
    provider_failure: Option<&'static str>,
    inventory_incomplete: bool,
    skip_reason: Option<CollectionSkipReason>,
    multiple_skip_reasons: bool,
}

impl ExplicitCollectionSummary {
    fn observe_page(&mut self, page: &InventoryPage) {
        // Explicit search samples one page; `done=false` does not make that
        // structurally valid sample an invalid inventory response.
        self.inventory_incomplete |= page.incomplete;
    }

    fn observe_item(&mut self, outcome: CollectionItemOutcome) {
        match outcome {
            CollectionItemOutcome::Published => self.published += 1,
            CollectionItemOutcome::Skipped(reason) => {
                if let Some(previous) = self.skip_reason {
                    self.multiple_skip_reasons |= previous != reason;
                } else {
                    self.skip_reason = Some(reason);
                }
            }
        }
    }

    fn observe_provider_failure(&mut self, error: DatabaseError) {
        self.provider_failure
            .get_or_insert(provider_failure_reason(error));
    }

    fn settlement(&self) -> (&'static str, Option<&'static str>) {
        if self.published > 0 {
            return (
                "done",
                self.provider_failure.or(self
                    .inventory_incomplete
                    .then_some("source_inventory_incomplete")),
            );
        }
        if let Some(reason) = self.provider_failure {
            return ("skipped", Some(reason));
        }
        if self.inventory_incomplete {
            return ("failed", Some("source_inventory_incomplete"));
        }
        let reason = if self.multiple_skip_reasons {
            "multiple_skip_reasons"
        } else {
            self.skip_reason
                .map(CollectionSkipReason::code)
                .unwrap_or("no_matches")
        };
        ("skipped", Some(reason))
    }
}

fn select_explicit_object(
    page: InventoryPage,
    object: &ObjectId,
) -> Result<InventoryItem, DatabaseError> {
    let absent = if page.incomplete {
        DatabaseError::SourceInventoryIncomplete
    } else {
        DatabaseError::NotFound
    };
    page.items
        .into_iter()
        .find(|item| &item.object == object)
        .ok_or(absent)
}

fn head_observation_superseded(item: &InventoryItem, head: &Capture, list_started_at: u64) -> bool {
    head.record.revision_id != item.revision_id
        && (head.captured_at >= list_started_at
            || head.validated_at >= list_started_at
            || !comparable_dates_advance(
                item.effective_date.as_deref(),
                item.publication_date.as_deref(),
                head.record.effective_date.as_deref(),
                head.record.publication_date.as_deref(),
            ))
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
    /// Execute one explicit collection in an isolated request Job Pod. The
    /// temporary index is used only to validate what serving can later index.
    pub async fn execute_collection_request(
        &self,
        launch: &CollectionLaunch,
        cancel: CancellationToken,
    ) -> Result<(), DatabaseError> {
        let provider = self
            .provider
            .as_ref()
            .ok_or(DatabaseError::InvalidInput)?
            .on_demand_client_with_limit(launch.attempt_limit)?
            .with_collection_launch(launch)?;
        let result = self
            .collect_explicit(&provider, launch, launch.request.clone(), cancel)
            .await;
        let (status, reason) = match &result {
            Ok(summary) => summary.settlement(),
            Err(DatabaseError::BudgetExhausted | DatabaseError::Capacity) => ("deferred", None),
            Err(DatabaseError::SourceUnavailable) => ("skipped", Some("source_unavailable")),
            Err(DatabaseError::SourceDataInvalid) => ("skipped", Some("source_data_invalid")),
            Err(DatabaseError::SourceDownloadFailed) => ("skipped", Some("download_failed")),
            Err(DatabaseError::NotFound) => ("skipped", Some("not_found")),
            Err(DatabaseError::AmbiguousCollection) => ("failed", Some("ambiguous")),
            Err(DatabaseError::SourceInventoryIncomplete) => {
                ("failed", Some("source_inventory_incomplete"))
            }
            Err(DatabaseError::Conflict) => ("failed", Some("identity_conflict")),
            Err(_) => ("failed", Some("worker_failed")),
        };
        let deferred_until = match &result {
            Err(DatabaseError::Capacity) => Some(now().saturating_add(5)),
            Err(DatabaseError::BudgetExhausted) => Some(provider.next_admissible_epoch().await?),
            _ => None,
        };
        self.store
            .settle_collection_launch(launch, status, reason, deferred_until)
            .await?;
        match result {
            Ok(_)
            | Err(
                DatabaseError::BudgetExhausted
                | DatabaseError::Capacity
                | DatabaseError::NotFound
                | DatabaseError::SourceUnavailable
                | DatabaseError::SourceDataInvalid
                | DatabaseError::SourceDownloadFailed,
            ) => Ok(()),
            Err(error) => Err(error),
        }
    }

    async fn collect_explicit(
        &self,
        provider: &LawClient,
        launch: &CollectionLaunch,
        request: CollectionRequest,
        cancel: CancellationToken,
    ) -> Result<ExplicitCollectionSummary, DatabaseError> {
        let mut summary = ExplicitCollectionSummary::default();
        match request.target {
            CollectionTarget::Object { object } => {
                let list_started_at = now();
                let page = provider
                    .inventory_page_class(
                        object.dataset,
                        1,
                        false,
                        Some(&object.id),
                        None,
                        cancel.clone(),
                    )
                    .await?;
                summary.observe_page(&page);
                let item = select_explicit_object(page, &object)?;
                summary.observe_item(
                    self.collect_explicit_item(provider, launch, item, list_started_at, cancel)
                        .await?,
                );
            }
            CollectionTarget::PrecedentCase {
                case_number,
                expected_id,
            } => {
                let list_started_at = now();
                let mut pages = Vec::new();
                for page_number in 1..=3 {
                    let page = provider
                        .inventory_precedent_case_page(&case_number, page_number, cancel.clone())
                        .await?;
                    let done = page.done;
                    pages.push(page);
                    if done {
                        break;
                    }
                }
                let item = select_precedent_case(&pages, &case_number, expected_id.as_deref())?;
                summary.observe_item(
                    self.collect_explicit_item(provider, launch, item, list_started_at, cancel)
                        .await?,
                );
            }
            CollectionTarget::Search {
                mode,
                term,
                datasets,
            } => {
                let datasets = if datasets.is_empty() {
                    vec![
                        Dataset::NationalStatute,
                        Dataset::AdministrativeRule,
                        Dataset::Ordinance,
                        Dataset::Treaty,
                        Dataset::Precedent,
                        Dataset::ConstitutionalDecision,
                        Dataset::LegalInterpretation,
                        Dataset::AdministrativeAppeal,
                    ]
                } else {
                    datasets
                };
                for dataset in datasets {
                    let classes: &[Option<u8>] = if dataset == Dataset::Treaty {
                        &[Some(1), Some(2)]
                    } else {
                        &[None]
                    };
                    for class in classes {
                        let list_started_at = now();
                        let page = match provider
                            .inventory_search_page_class(
                                dataset,
                                1,
                                &term,
                                matches!(mode, CollectionSearchMode::Literal),
                                *class,
                                cancel.clone(),
                            )
                            .await
                        {
                            Ok(page) => page,
                            Err(
                                error @ (DatabaseError::SourceUnavailable
                                | DatabaseError::SourceDataInvalid
                                | DatabaseError::SourceDownloadFailed),
                            ) => {
                                summary.observe_provider_failure(error);
                                continue;
                            }
                            Err(error) => return Err(error),
                        };
                        summary.observe_page(&page);
                        for item in page.items.into_iter().take(20) {
                            match self
                                .collect_explicit_item(
                                    provider,
                                    launch,
                                    item,
                                    list_started_at,
                                    cancel.clone(),
                                )
                                .await
                            {
                                Ok(outcome) => summary.observe_item(outcome),
                                Err(
                                    error @ (DatabaseError::SourceUnavailable
                                    | DatabaseError::SourceDataInvalid
                                    | DatabaseError::SourceDownloadFailed),
                                ) => {
                                    summary.observe_provider_failure(error);
                                }
                                Err(error) => return Err(error),
                            }
                        }
                    }
                }
            }
        }
        Ok(summary)
    }

    async fn collect_explicit_item(
        &self,
        provider: &LawClient,
        launch: &CollectionLaunch,
        item: InventoryItem,
        list_started_at: u64,
        cancel: CancellationToken,
    ) -> Result<CollectionItemOutcome, DatabaseError> {
        let observed = self.store.state(&item.object).await?;
        let budget_wait = observed.pending
            && self
                .store
                .background_budget_wait_available(
                    &item.object,
                    &item.revision_id,
                    item.effective_date.as_deref(),
                    observed.version,
                )
                .await?;
        if observed.pending && !budget_wait {
            return Ok(CollectionItemOutcome::Skipped(
                CollectionSkipReason::Pending,
            ));
        }
        if let Some(capture_id) = &observed.head_capture {
            let selector = if budget_wait {
                RevisionSelector::Capture {
                    id: capture_id.clone(),
                }
            } else {
                RevisionSelector::Head
            };
            match self
                .store
                .resolve(item.object.clone(), selector, now(), cancel.clone())
                .await
            {
                Ok(head) if head_observation_superseded(&item, &head, list_started_at) => {
                    return Ok(CollectionItemOutcome::Skipped(
                        CollectionSkipReason::HeadObservationSuperseded,
                    ));
                }
                Ok(_) => {}
                Err(DatabaseError::NotFound | DatabaseError::RevisionUnavailable) => {}
                Err(error) => return Err(error),
            }
        }
        if self
            .store
            .head_revision_ready(&item.object, &item.revision_id, now())
            .await?
        {
            return Ok(CollectionItemOutcome::Skipped(
                CollectionSkipReason::AlreadyFresh,
            ));
        }
        let mut metadata = std::collections::BTreeMap::new();
        metadata.insert("collection_origin".into(), "explicit".into());
        metadata.insert("title".into(), item.title.clone());
        if let Some(value) = &item.data_source {
            metadata.insert("data_source".into(), value.clone());
        }
        if let Some(value) = &item.case_number {
            metadata.insert("case_number".into(), value.clone());
        }
        if let Some(value) = &item.treaty_class_code {
            metadata.insert("treaty_class_code".into(), value.clone());
        }
        if let Some(value) = &item.amendment_type {
            metadata.insert("amendment_type".into(), value.clone());
        }
        let queued = if budget_wait {
            let Some(job) = self
                .store
                .adopt_background_budget_wait(
                    item.object.clone(),
                    item.revision_id.clone(),
                    item.effective_date.clone(),
                    observed.version,
                    metadata,
                    launch,
                    now(),
                )
                .await?
            else {
                return Ok(CollectionItemOutcome::Skipped(
                    CollectionSkipReason::AlreadyInProgress,
                ));
            };
            job
        } else {
            self.store
                .enqueue_job_for_collection_request(
                    item.object.clone(),
                    item.revision_id.clone(),
                    item.effective_date.clone(),
                    true,
                    false,
                    now(),
                    metadata,
                    Some(observed.version),
                    launch,
                )
                .await?
        };
        if queued
            .source_metadata
            .get("collection_origin")
            .map(String::as_str)
            != Some("explicit")
        {
            return Ok(CollectionItemOutcome::Skipped(
                CollectionSkipReason::AlreadyInProgress,
            ));
        }
        if !self
            .store
            .adopt_explicit_job(&queued.id, &launch.id, launch.recovery_at(), now())
            .await?
        {
            return Ok(CollectionItemOutcome::Skipped(
                CollectionSkipReason::AlreadyInProgress,
            ));
        }
        let Some(job) = self
            .store
            .claim_explicit_job_for_request(
                &queued.id,
                &launch.id,
                now(),
                self.detail_timeout_secs.saturating_add(120).max(600),
            )
            .await?
        else {
            self.store
                .release_unclaimed_explicit_job_for_request(&queued.id, &launch.id)
                .await?;
            return Err(DatabaseError::Capacity);
        };
        let attempt = cancel.child_token();
        let reserved = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let detail_provider = provider.clone().with_reservation_observer(reserved.clone());
        let detail = tokio::time::timeout(
            Duration::from_secs(self.detail_timeout_secs),
            detail_provider.detail(&item, attempt.clone()),
        )
        .await
        .unwrap_or_else(|_| {
            attempt.cancel();
            Err(DatabaseError::Capacity)
        });
        let detail = match detail {
            Ok(detail) => detail,
            Err(
                error @ (DatabaseError::Capacity
                | DatabaseError::Cancelled
                | DatabaseError::BudgetExhausted),
            ) if !reserved.load(std::sync::atomic::Ordering::Acquire) => {
                match self.store.release_admission_wait(&job).await {
                    Ok(()) | Err(DatabaseError::Conflict) => {}
                    Err(error) => return Err(error),
                }
                return Err(error);
            }
            Err(
                error @ (DatabaseError::SourceUnavailable
                | DatabaseError::SourceDataInvalid
                | DatabaseError::SourceDownloadFailed),
            ) => {
                let reason = match error {
                    DatabaseError::SourceUnavailable => "source_unavailable",
                    DatabaseError::SourceDataInvalid => "source_data_invalid",
                    _ => "download_failed",
                };
                self.store.skip_claim(&job, reason, now()).await?;
                return Err(error);
            }
            Err(error) => {
                self.store.fail_claim(&job, false).await?;
                return Err(error);
            }
        };
        let index = self.index.clone();
        let record = detail.record.clone();
        let worker_cancel = cancel.clone();
        let validation = tokio::task::spawn_blocking(move || {
            index.validate_record_with_budget(
                &record,
                std::time::Instant::now() + Duration::from_secs(10),
                &worker_cancel,
            )
        })
        .await
        .map_err(|_| DatabaseError::Capacity)
        .and_then(|result| result);
        if let Err(error) = validation {
            self.store.fail_claim(&job, false).await?;
            return Err(error);
        }
        match self
            .store
            .publish(
                Publication {
                    record: detail.record,
                    raw: detail.raw,
                    additional_evidence: detail.additional_evidence,
                    processor_version: detail.processor_version,
                    retrieved_at: detail.retrieved_at,
                    now: now(),
                    expected_version: job.expected_version,
                    install_head: true,
                    job_id: Some(job.id.clone()),
                },
                cancel,
            )
            .await
        {
            Ok(_) => Ok(CollectionItemOutcome::Published),
            Err(DatabaseError::Conflict) => {
                self.store.fail_claim(&job, false).await?;
                Ok(CollectionItemOutcome::Skipped(
                    CollectionSkipReason::PublicationSuperseded,
                ))
            }
            Err(error) => {
                self.store.fail_claim(&job, false).await?;
                Err(error)
            }
        }
    }
    async fn observed_page(
        &self,
        provider: &LawClient,
        dataset: Dataset,
        page: u32,
        historical: bool,
        class: Option<u8>,
        cancel: CancellationToken,
    ) -> Result<Option<InventoryPage>, DatabaseError> {
        match provider
            .inventory_page_class(dataset, page, historical, None, class, cancel.clone())
            .await
        {
            Ok(mut result) => {
                if let Some(evidence) = result.source_evidence.take() {
                    let family = openlegal_adapters::law_go_kr::catalog::source_family(dataset);
                    let mut metadata = std::collections::BTreeMap::new();
                    metadata.insert("dataset".into(), dataset.as_str().into());
                    metadata.insert("page".into(), page.to_string());
                    metadata.insert("historical".into(), historical.to_string());
                    if evidence.credentials_redacted {
                        metadata.insert("credentials_redacted".into(), "true".into());
                    }
                    if let Some(total) = result.total {
                        metadata.insert("total".into(), total.to_string());
                    }
                    let rights = if family.metadata_only {
                        openlegal_domain::rights::SourceRights::default()
                    } else {
                        openlegal_domain::rights::SourceRights::legal_information()
                    };
                    if family.metadata_only {
                        metadata.insert("status".into(), "rights_unverified_metadata_only".into());
                    }
                    self.store
                        .retain_source_observation(
                            SourceObservationInput {
                                source_key: format!(
                                    "law_go_kr:{}:{}:{}:{}:{}",
                                    family.list_guide,
                                    dataset.as_str(),
                                    historical,
                                    class.unwrap_or(0),
                                    page
                                ),
                                raw: (!family.metadata_only).then_some(evidence.raw),
                                media_type: "application/xml".into(),
                                rights,
                                metadata,
                                observed_at: evidence.retrieved_at,
                            },
                            cancel.clone(),
                        )
                        .await?;
                }

                if result.incomplete {
                    self.store
                        .record_page_gap(
                            dataset,
                            historical,
                            class,
                            page,
                            PageGapObservation {
                                reason: "source_data_invalid",
                                rows: result.rejected_rows,
                                now: now(),
                            },
                        )
                        .await?;
                } else {
                    self.store
                        .resolve_page_gap(dataset, historical, class, page, now())
                        .await?;
                }
                Ok(Some(result))
            }
            Err(
                error @ (DatabaseError::SourceUnavailable
                | DatabaseError::SourceDataInvalid
                | DatabaseError::SourceDownloadFailed),
            ) => {
                let reason = match error {
                    DatabaseError::SourceUnavailable => "source_unavailable",
                    DatabaseError::SourceDataInvalid => "source_data_invalid",
                    DatabaseError::SourceDownloadFailed => "download_failed",
                    _ => unreachable!(),
                };
                self.store
                    .record_page_gap(
                        dataset,
                        historical,
                        class,
                        page,
                        PageGapObservation {
                            reason,
                            rows: 1,
                            now: now(),
                        },
                    )
                    .await?;
                Ok(None)
            }
            Err(error) => Err(error),
        }
    }
    pub async fn open(
        config: &DatabaseConfig,
        persistent: &Arc<openlegal_adapters::postgres::PostgresStore>,
    ) -> Result<Arc<Self>, ServerError> {
        Self::open_inner(config, persistent, true).await
    }

    /// Background collection uses a disposable validation index and never owns
    /// the serving index or its process-lifetime lease.
    pub async fn open_background(
        config: &DatabaseConfig,
        persistent: &Arc<openlegal_adapters::postgres::PostgresStore>,
    ) -> Result<Arc<Self>, ServerError> {
        let mut background = config.clone();
        background.index_path =
            std::env::temp_dir().join(format!("openlegal-collection-{}", std::process::id()));
        Self::open_inner(&background, persistent, false).await
    }

    async fn open_inner(
        config: &DatabaseConfig,
        persistent: &Arc<openlegal_adapters::postgres::PostgresStore>,
        serving: bool,
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
                c.document_worker.limits()?,
            )
            .map_err(|_| "invalid document sandbox configuration")?;
            let secret = std::env::var(&c.credential_env)
                .map_err(|_| "legal provider credential environment is missing")?;
            let mode = match c.mode {
                IngestionMode::Pilot => RequestBudgetMode::Pilot,
                IngestionMode::Continuous => RequestBudgetMode::Continuous,
            };
            let client = LawClient::new(secret, Arc::new(processor))?;
            let client = match &c.proxy {
                Some(proxy) => client.with_socks5_proxy(proxy.load()?),
                None => client,
            };
            LawClient::configure_provider_request_limits(
                &persistent.pool(),
                &c.provider_requests.limits()?,
            )
            .await?;
            Some(client.with_request_budget(persistent.pool(), mode))
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
        if serving {
            store
                .configure_archive_capacity(config.max_raw_bytes.as_option())
                .await?;
        }
        let provider = match provider {
            Some(client) => Some(
                client
                    .with_source_archive(store.clone())
                    .with_collection_events(store.collection_events().await?),
            ),
            None => None,
        };
        let lease = if serving {
            match store.acquire_runtime_lease().await {
                Ok(lease) => Some(lease),
                Err(error) => {
                    let _ = blobs.close().await;
                    return Err(error.into());
                }
            }
        } else {
            None
        };
        // The incremental scanner cannot preserve a prior release's complete
        // inventory claim, even when this serving instance has ingestion off.
        for dataset in [
            Dataset::NationalStatute,
            Dataset::AdministrativeRule,
            Dataset::Ordinance,
        ] {
            if serving
                && let Err(error) = store.mark_dataset_inventory_complete(dataset, false).await
            {
                if let Some(lease) = &lease {
                    let _ = lease.close().await;
                }
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
            if serving && (generation < store.acknowledged_index().await?
                || generation > store.watermark().await?)
            {
                return Err("corpus index generation is incompatible with PostgreSQL; stop serving and run --rebuild-corpus-index with a fresh index_path".into());
            }
            Ok(index)
        }
        .await;
        let index = match opened {
            Ok(index) => index,
            Err(error) => {
                if let Some(lease) = &lease {
                    let _ = lease.close().await;
                }
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
            detail_timeout_secs: config
                .ingestion
                .as_ref()
                .map_or(3600, |c| c.detail_timeout_secs),
            detail_job_workers: config
                .ingestion
                .as_ref()
                .map_or(1, |c| c.detail_job_workers),
            scan_interval_secs: config
                .ingestion
                .as_ref()
                .map_or(3600, |c| c.scan_interval_secs),
            adaptive_polling: config.ingestion.as_ref().is_none_or(|c| c.adaptive_polling),
        }))
    }
    pub async fn close(&self) -> Result<(), ServerError> {
        self.store.close_collection_events().await;
        let blobs = self.blobs.close().await;
        let lease = if let Some(lease) = &self.lease {
            lease.close().await
        } else {
            Ok(())
        };
        blobs?;
        lease?;
        Ok(())
    }
    pub async fn run(self: Arc<Self>, cancel: CancellationToken) -> Result<(), ServerError> {
        let child = cancel.child_token();
        let mut tasks = tokio::task::JoinSet::new();
        if self.provider.is_some() {
            let ingestion = child.child_token();
            if self.ingestion_mode == Some(IngestionMode::Pilot) {
                let duration = self
                    .provider
                    .as_ref()
                    .ok_or(DatabaseError::InvalidInput)?
                    .begin_pilot()
                    .await?;
                let watchdog = ingestion.clone();
                tasks.spawn(async move {
                    pilot_watchdog(duration, watchdog).await;
                    Ok::<(), DatabaseError>(())
                });
            }
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
            if self.ingestion_mode == Some(IngestionMode::Continuous) {
                let runtime = self.clone();
                let token = ingestion.clone();
                tasks.spawn(async move { runtime.process_supplements(token).await });
            }
            for slot in 0..self.detail_job_workers {
                let runtime = self.clone();
                let token = ingestion.clone();
                tasks.spawn(async move { runtime.process_jobs(slot, token).await });
            }
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
                    if let Some(lease) = &self.lease {
                        if let Err(e) = lease.check().await { break Err(e); }
                        if let Err(e) = self.index_events(&child).await { break Err(e); }
                    }
                    if let Err(e) = self.store.health().await {
                        break Err(e);
                    }
                    if self.lease.is_some() && maintenance.elapsed() >= Duration::from_secs(60) {
                        if let Err(e) = self.store.maintain(now(), 0).await {
                            break Err(e);
                        }
                        if let Err(e) = self.store.prune_collection_requests().await {
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
            self.lease
                .as_ref()
                .ok_or(DatabaseError::StorageUnavailable)?
                .check()
                .await?;
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
        let mut events = if self.adaptive_polling {
            Some(self.store.collection_events().await?)
        } else {
            None
        };
        loop {
            for request in openlegal_adapters::law_go_kr::supplements::global_requests(1)? {
                self.store.enqueue_supplement(&request, now()).await?;
            }
            self.store.requeue_due_details(now()).await?;
            self.inventory_verified
                .store(false, std::sync::atomic::Ordering::Release);
            for view in CloneView::all() {
                if cancel.is_cancelled() {
                    return Ok(());
                }
                let (page, _) = self.store.clone_cursor(view).await?;
                let result = match self
                    .observed_page(
                        provider,
                        view.dataset,
                        page,
                        view.historical,
                        view.treaty_class,
                        cancel.clone(),
                    )
                    .await
                {
                    Ok(Some(result)) => result,
                    Ok(None) => continue,
                    Err(DatabaseError::BudgetExhausted | DatabaseError::Capacity) => break,
                    Err(error) => return Err(error),
                };
                let mut offset = self.store.clone_page_offset(view, page, &result).await?;
                if offset > result.items.len() {
                    offset = 0;
                }
                if let Some(missing) = self
                    .first_unscheduled_item(&result.items, !view.historical, &cancel)
                    .await?
                {
                    offset = offset.min(missing);
                }
                while offset < result.items.len() {
                    let item = &result.items[offset];
                    if view.historical {
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
                    if (!view.historical || self.retain_history_bodies)
                        && !self
                            .refresh_with_fair_capacity(
                                provider,
                                item.clone(),
                                !view.historical,
                                cancel.clone(),
                            )
                            .await?
                    {
                        break;
                    }
                    if item.object.provider == "law_go_kr" {
                        for request in openlegal_adapters::law_go_kr::supplements::seeded_requests(
                            openlegal_adapters::law_go_kr::supplements::record_seed(item)?,
                            1,
                        )? {
                            self.store.enqueue_supplement(&request, now()).await?;
                        }
                    }
                    offset += 1;
                }
                self.store
                    .clone_page_scheduled(view, page, offset, &result, now(), &cancel)
                    .await?;
            }
            if let Some(events) = &mut events {
                events
                    .wait(&cancel, Duration::from_secs(self.scan_interval_secs.min(5)))
                    .await
                    .or_else(|error| {
                        if cancel.is_cancelled() {
                            Ok(())
                        } else {
                            Err(error)
                        }
                    })?;
            } else {
                tokio::select! {_=cancel.cancelled()=>return Ok(()),_=tokio::time::sleep(Duration::from_secs(self.scan_interval_secs))=>{}}
            }
        }
    }
    /// Each lease is durable and UUID fenced. Raw observations survive parser
    /// retries, which never spend another upstream request for unchanged bytes.
    async fn process_supplements(&self, cancel: CancellationToken) -> Result<(), DatabaseError> {
        use openlegal_adapters::law_go_kr::supplements::{self, SupplementOutcome};
        let provider = self.provider.as_ref().ok_or(DatabaseError::InvalidInput)?;
        loop {
            if cancel.is_cancelled() {
                return Ok(());
            }
            let Some(job) = self.store.claim_supplement(now()).await? else {
                tokio::select! {_=cancel.cancelled()=>return Ok(()),_=tokio::time::sleep(Duration::from_secs(5))=>{}}
                continue;
            };
            let attempt = cancel.child_token();
            let _guard = attempt.clone().drop_guard();
            // Finish within the 600-second fenced lease, including parsing and storage.
            let outcome = tokio::time::timeout(Duration::from_secs(480), async {
            let fetched = if let Some(id) = &job.observation_id {
                let raw = self
                    .store
                    .source_observation_bytes(id, attempt.clone())
                    .await?;
                let observed = self.store.source_observation(id, attempt.clone()).await?;
                let result = provider
                    .analyze_supplement(&job.request, &raw, job.observed_before, attempt.clone())
                    .await;
                match result {
                    Ok((page, processor_version)) => Ok(SupplementOutcome::Captured(
                        supplements::SupplementCapture {
                            observation_key: job.key.clone(),
                            source_url: job.request.source_url()?.into(),
                            raw,
                            retrieved_at: observed.observed_at,
                            credentials_redacted: observed.metadata.get("credentials_redacted").is_some_and(|value| value == "true"),
                            processor_version,
                            processing_error: None,
                            page,
                        },
                    )),
                    Err(error) => Err(error),
                }
            } else {
                provider
                    .fetch_supplement(&job.request, job.observed_before, attempt.clone())
                    .await
            };
            match fetched {
                Ok(SupplementOutcome::Deferred {
                    observation_key,
                    source_url,
                    reason,
                }) => {
                    let mut metadata = std::collections::BTreeMap::new();
                    metadata.insert("status".into(), reason.into());
                    metadata.insert("source_url".into(), source_url);
                    let observed = self
                        .store
                        .retain_source_observation(
                            SourceObservationInput {
                                source_key: observation_key,
                                raw: None,
                                media_type: "application/xml".into(),
                                rights: openlegal_domain::rights::SourceRights::default(),
                                metadata,
                                observed_at: now(),
                            },
                            attempt.clone(),
                        )
                        .await?;
                    self.store
                        .settle_supplement(
                            &job,
                            SupplementJobStatus::Deferred,
                            Some(&observed.observation_id),
                            0,
                            now(),
                        )
                        .await?;
                }
                Ok(SupplementOutcome::Captured(capture)) => {
                    let mut metadata = std::collections::BTreeMap::new();
                    metadata.insert("source_url".into(), capture.source_url);
                    metadata.insert("processor_version".into(), capture.processor_version);
                    if capture.credentials_redacted {
                        metadata.insert("credentials_redacted".into(), "true".into());
                    }
                    metadata.insert("incomplete".into(), capture.page.incomplete.to_string());
                    if let Some(error) = capture.processing_error {
                        metadata.insert("processing_error".into(), format!("{error:?}"));
                    }
                    let observed = self
                        .store
                        .retain_source_observation(
                            SourceObservationInput {
                                source_key: capture.observation_key,
                                raw: Some(capture.raw),
                                media_type: if job.request.format()
                                    == openlegal_application::document::DocumentFormat::Html
                                {
                                    "text/html"
                                } else {
                                    "application/xml"
                                }
                                .into(),
                                rights: openlegal_domain::rights::SourceRights::legal_information(),
                                metadata,
                                observed_at: capture.retrieved_at,
                            },
                            attempt.clone(),
                        )
                        .await?;
                    for seed in capture.page.seeds {
                        for request in supplements::seeded_requests(seed, 1)? {
                            self.store.enqueue_supplement(&request, now()).await?;
                        }
                    }
                    let status = if capture.page.incomplete || capture.page.done.is_none() {
                        SupplementJobStatus::Incomplete
                    } else {
                        SupplementJobStatus::Done
                    };
                    let next = if status == SupplementJobStatus::Done
                        && capture.page.done == Some(false)
                    {
                        Some(job.request.next_page()?)
                    } else {
                        None
                    };
                    self.store
                        .settle_supplement_with_successor(
                            &job,
                            status,
                            Some(&observed.observation_id),
                            capture.page.observed_rows,
                            now(),
                            next.as_ref(),
                        )
                        .await?;
                }
                Err(DatabaseError::BudgetExhausted | DatabaseError::Capacity) => {
                    self.store
                        .settle_supplement(&job, SupplementJobStatus::Pending, None, 0, now())
                        .await?;
                    tokio::select! {_=cancel.cancelled()=>return Ok(()),_=tokio::time::sleep(Duration::from_secs(5))=>{}}
                }
                Err(
                    DatabaseError::SourceUnavailable
                    | DatabaseError::SourceDataInvalid
                    | DatabaseError::SourceDownloadFailed
                    | DatabaseError::ProcessingPending,
                ) => {
                    self.store
                        .settle_supplement(
                            &job,
                            SupplementJobStatus::Incomplete,
                            job.observation_id.as_deref(),
                            0,
                            now(),
                        )
                        .await?;
                }
                Err(DatabaseError::Cancelled) if cancel.is_cancelled() => return Ok(()),
                Err(error) => return Err(error),
            }
                Ok::<(), DatabaseError>(())
            }).await;
            match outcome {
                Ok(Ok(())) | Ok(Err(DatabaseError::Conflict)) => {}
                Ok(Err(error)) => return Err(error),
                Err(_) => {
                    attempt.cancel();
                    match self
                        .store
                        .settle_supplement(
                            &job,
                            SupplementJobStatus::Incomplete,
                            job.observation_id.as_deref(),
                            0,
                            now(),
                        )
                        .await
                    {
                        Ok(()) | Err(DatabaseError::Conflict) => {}
                        Err(error) => return Err(error),
                    }
                }
            }
        }
    }
    /// Move the saved page offset only to work that has neither published nor
    /// been queued. Rewinding to an active first-page job would starve later
    /// entries each time the list is revisited.
    async fn first_unscheduled_item(
        &self,
        items: &[InventoryItem],
        head: bool,
        cancel: &CancellationToken,
    ) -> Result<Option<usize>, DatabaseError> {
        for (index, item) in items.iter().enumerate() {
            if cancel.is_cancelled() {
                return Err(DatabaseError::Cancelled);
            }
            if self
                .store
                .detail_gap_active(&item.object, &item.revision_id)
                .await?
            {
                continue;
            }
            let published = if head {
                self.store
                    .head_revision_published(&item.object, &item.revision_id)
                    .await?
            } else {
                self.store
                    .revision_capture_published(&item.object, &item.revision_id, now())
                    .await?
            };
            if !published
                && !self
                    .store
                    .active_detail_job(&item.object, &item.revision_id, head)
                    .await?
            {
                return Ok(Some(index));
            }
        }
        Ok(None)
    }
    /// Select a few identities from each documented list family. The pilot
    /// never certifies inventory completeness or walks historical catalogs.
    /// The durable ledger returns the remaining operator-selected pilot window.
    async fn ingest_pilot(
        &self,
        provider: &LawClient,
        cancel: CancellationToken,
    ) -> Result<(), DatabaseError> {
        let deadline = tokio::time::Instant::now() + provider.begin_pilot().await?;
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
                hint.amendment_type = None;
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
                    self.observed_page(provider, dataset, page, false, class, cancel.clone()),
                )
                .await;
                let page_data = match page_result {
                    Ok(Ok(Some(page_data))) => page_data,
                    Ok(Ok(None)) => {
                        list_failures += 1;
                        continue;
                    }
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
                    Ok(Err(DatabaseError::SourceRejected | DatabaseError::SourceUnauthorized)) => {
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
                if page_data.incomplete {
                    list_failures += 1;
                }
                let done = page_data.done;
                for item in page_data.items {
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
    async fn refresh_with_fair_capacity(
        &self,
        provider: &LawClient,
        item: InventoryItem,
        install_head: bool,
        cancel: CancellationToken,
    ) -> Result<bool, DatabaseError> {
        if self
            .store
            .active_jobs_for_dataset(item.object.dataset)
            .await?
            >= 16
        {
            return Ok(false);
        }
        match self.refresh(provider, item, install_head, cancel).await {
            Ok(()) => Ok(true),
            Err(DatabaseError::Capacity) => Ok(false),
            Err(error) => Err(error),
        }
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
                    | DatabaseError::NotObserved
                    | DatabaseError::CollectionIncomplete
                    | DatabaseError::ProcessingPending
                    | DatabaseError::RevisionUnavailable,
                ) => true,
                Err(e) => return Err(*e),
            };
            if old.as_ref().is_ok_and(|c| {
                !replacement
                    && c.record
                        .metadata
                        .get("attachment_status")
                        .map(String::as_str)
                        != Some("incomplete")
                    && now().saturating_sub(c.validated_at) < 3600
            }) {
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
        if let Some(v) = &item.amendment_type {
            metadata.insert("amendment_type".into(), v.clone());
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
    async fn process_jobs(
        &self,
        slot: u32,
        cancel: CancellationToken,
    ) -> Result<(), DatabaseError> {
        let provider = self.provider.as_ref().ok_or(DatabaseError::InvalidInput)?;
        let mut events = self.store.collection_events().await?;
        const DATASETS: &[Dataset] = Dataset::ALL;
        let mut next_dataset = slot as usize % DATASETS.len();
        loop {
            if cancel.is_cancelled() {
                return Ok(());
            }
            let preferred = DATASETS[next_dataset];
            next_dataset = (next_dataset + 1) % DATASETS.len();
            let claim = self
                .store
                .claim_job_with_lease_for_dataset(
                    now(),
                    self.detail_timeout_secs.saturating_add(120).max(600),
                    preferred,
                )
                .await?;
            let Some(job) = (if claim.is_some() {
                claim
            } else {
                self.store
                    .claim_job_with_lease(
                        now(),
                        self.detail_timeout_secs.saturating_add(120).max(600),
                    )
                    .await?
            }) else {
                match events.wait(&cancel, Duration::from_secs(1)).await {
                    Ok(()) => {}
                    Err(DatabaseError::Cancelled) => return Ok(()),
                    Err(error) => return Err(error),
                }
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
                amendment_type: job.source_metadata.get("amendment_type").cloned(),
            };
            let attempt = cancel.child_token();
            let _attempt_guard = attempt.clone().drop_guard();
            let reserved = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let detail_provider = provider.clone().with_reservation_observer(reserved.clone());
            let detail = match tokio::time::timeout(
                Duration::from_secs(self.detail_timeout_secs),
                detail_provider.detail(&item, attempt.clone()),
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
                Ok(mut detail) => {
                    for observation in std::mem::take(&mut detail.source_observations) {
                        let retained = self
                            .store
                            .retain_source_observation(observation, attempt.clone())
                            .await?;
                        detail
                            .record
                            .metadata
                            .insert("original_observation_id".into(), retained.observation_id);
                    }
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
                    if item.object.dataset == Dataset::NationalStatute {
                        let numbers: Vec<String> = detail
                            .record
                            .metadata
                            .get("provider_provisions_json")
                            .map(|value| {
                                serde_json::from_str(value)
                                    .map_err(|_| DatabaseError::StorageCorrupt)
                            })
                            .transpose()?
                            .unwrap_or_default();
                        for number in numbers {
                            let seed = openlegal_adapters::law_go_kr::supplements::SupplementSeed::Provision { object: item.object.clone(), number };
                            for request in
                                openlegal_adapters::law_go_kr::supplements::seeded_requests(
                                    seed, 1,
                                )?
                            {
                                self.store.enqueue_supplement(&request, now()).await?;
                            }
                        }
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
                    if reserved.load(std::sync::atomic::Ordering::Acquire) {
                        self.store.fail_claim(&job, true).await?;
                    } else {
                        match self.store.release_admission_wait(&job).await {
                            Ok(()) | Err(DatabaseError::Conflict) => {}
                            Err(error) => return Err(error),
                        }
                    }
                    return Ok(());
                }
                Err(DatabaseError::BudgetExhausted) => {
                    let resume_at = provider.next_admissible_epoch().await?;
                    let deferred = if reserved.load(std::sync::atomic::Ordering::Acquire) {
                        self.store
                            .defer_reserved_budget_claim(&job, resume_at)
                            .await
                    } else {
                        self.store.defer_budget_claim(&job, resume_at).await
                    };
                    match deferred {
                        Ok(()) | Err(DatabaseError::Conflict) => {}
                        Err(error) => return Err(error),
                    }
                    if self.ingestion_mode == Some(IngestionMode::Pilot) {
                        cancel.cancel();
                        return Ok(());
                    }
                    let wait = Duration::from_secs(resume_at.saturating_sub(now()).clamp(1, 60));
                    match events.wait(&cancel, wait).await {
                        Ok(()) => {}
                        Err(DatabaseError::Cancelled) => return Ok(()),
                        Err(error) => return Err(error),
                    }
                }
                Err(DatabaseError::Capacity)
                    if !reserved.load(std::sync::atomic::Ordering::Acquire) =>
                {
                    match self.store.release_admission_wait(&job).await {
                        Ok(()) | Err(DatabaseError::Conflict) => {}
                        Err(error) => return Err(error),
                    }
                    // Our own pending transition emits a wakeup; a bounded
                    // pause avoids immediately reclaiming a ticket-full job.
                    tokio::select! {
                        _ = cancel.cancelled() => return Ok(()),
                        _ = tokio::time::sleep(Duration::from_secs(5)) => {}
                    }
                }
                Err(
                    error @ (DatabaseError::SourceUnavailable
                    | DatabaseError::SourceDataInvalid
                    | DatabaseError::SourceDownloadFailed),
                ) => {
                    let reason = match error {
                        DatabaseError::SourceUnavailable => "source_unavailable",
                        DatabaseError::SourceDataInvalid => "source_data_invalid",
                        DatabaseError::SourceDownloadFailed => "download_failed",
                        _ => unreachable!(),
                    };
                    self.store.skip_claim(&job, reason, now()).await?;
                }
                Err(DatabaseError::SourceRejected | DatabaseError::SourceUnauthorized) => {
                    self.store.fail_claim(&job, false).await?;
                    eprintln!(
                        "law provider: detail source rejected for {:?}; suspending provider requests; queued HEAD may remain pending",
                        job.object.dataset
                    );
                    cancel.cancel();
                    return Ok(());
                }
                Err(
                    DatabaseError::InvalidInput
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
mod explicit_collection_tests {
    use super::*;
    use openlegal_domain::legal::LegalRecord;
    use std::collections::BTreeMap;

    fn item() -> InventoryItem {
        InventoryItem {
            object: ObjectId {
                jurisdiction: "kr".into(),
                provider: "law_go_kr".into(),
                dataset: Dataset::NationalStatute,
                id: "001".into(),
            },
            revision_id: "200:20260102".into(),
            effective_date: Some("20260102".into()),
            publication_date: Some("20260101".into()),
            title: "Fictional statute".into(),
            data_source: None,
            case_number: None,
            treaty_class_code: None,
            amendment_type: None,
        }
    }

    fn page(items: Vec<InventoryItem>, incomplete: bool) -> InventoryPage {
        InventoryPage {
            source_evidence: None,
            items,
            total: Some(200),
            done: false,
            rejected_rows: usize::from(incomplete),
            incomplete,
        }
    }

    #[test]
    fn explicit_object_selection_preserves_valid_rows_without_claiming_absence_from_bad_rows() {
        let wanted = item().object;
        for incomplete in [false, true] {
            let selected = select_explicit_object(page(vec![item()], incomplete), &wanted).unwrap();
            assert_eq!(selected.object, wanted);
            let missing = select_explicit_object(page(vec![], incomplete), &wanted).unwrap_err();
            assert_eq!(
                missing,
                if incomplete {
                    DatabaseError::SourceInventoryIncomplete
                } else {
                    DatabaseError::NotFound
                }
            );
        }
    }

    #[test]
    fn explicit_collection_settlement_preserves_partial_publication_and_failure_precedence() {
        for published in [false, true] {
            for incomplete in [false, true] {
                for failure in [
                    None,
                    Some(DatabaseError::SourceUnavailable),
                    Some(DatabaseError::SourceDataInvalid),
                    Some(DatabaseError::SourceDownloadFailed),
                ] {
                    let mut summary = ExplicitCollectionSummary::default();
                    summary.observe_page(&page(vec![item()], incomplete));
                    summary.observe_item(CollectionItemOutcome::Skipped(
                        CollectionSkipReason::AlreadyFresh,
                    ));
                    if let Some(error) = failure {
                        summary.observe_provider_failure(error);
                        // A later failure never replaces the first one.
                        summary.observe_provider_failure(DatabaseError::SourceDownloadFailed);
                    }
                    if published {
                        summary.observe_item(CollectionItemOutcome::Published);
                    }
                    let expected = match (published, failure, incomplete) {
                        (true, Some(error), _) => ("done", Some(provider_failure_reason(error))),
                        (false, Some(error), _) => {
                            ("skipped", Some(provider_failure_reason(error)))
                        }
                        (true, None, true) => ("done", Some("source_inventory_incomplete")),
                        (false, None, true) => ("failed", Some("source_inventory_incomplete")),
                        (true, None, false) => ("done", None),
                        (false, None, false) => ("skipped", Some("already_fresh")),
                    };
                    assert_eq!(summary.settlement(), expected);
                }
            }
        }
    }

    #[test]
    fn skipped_collection_reports_actual_distinct_causes_and_empty_samples() {
        let causes = [
            (CollectionSkipReason::Pending, "collection_pending"),
            (CollectionSkipReason::AlreadyFresh, "already_fresh"),
            (
                CollectionSkipReason::AlreadyInProgress,
                "collection_already_in_progress",
            ),
            (
                CollectionSkipReason::HeadObservationSuperseded,
                "head_observation_superseded",
            ),
            (
                CollectionSkipReason::PublicationSuperseded,
                "publication_superseded",
            ),
        ];
        for (cause, code) in causes {
            let mut summary = ExplicitCollectionSummary::default();
            summary.observe_item(CollectionItemOutcome::Skipped(cause));
            summary.observe_item(CollectionItemOutcome::Skipped(cause));
            assert_eq!(summary.settlement(), ("skipped", Some(code)));
            let different = if cause == CollectionSkipReason::Pending {
                CollectionSkipReason::AlreadyFresh
            } else {
                CollectionSkipReason::Pending
            };
            summary.observe_item(CollectionItemOutcome::Skipped(different));
            assert_eq!(
                summary.settlement(),
                ("skipped", Some("multiple_skip_reasons"))
            );
        }
        let mut empty = ExplicitCollectionSummary::default();
        empty.observe_page(&page(vec![], false));
        assert_eq!(empty.settlement(), ("skipped", Some("no_matches")));
        empty.observe_page(&page(vec![], true));
        assert_eq!(
            empty.settlement(),
            ("failed", Some("source_inventory_incomplete"))
        );
    }

    #[test]
    fn explicit_head_guard_preserves_observation_and_date_fences() {
        let mut candidate = item();
        let mut head = Capture {
            capture_id: "a".repeat(64),
            sequence: 1,
            record: LegalRecord {
                object: candidate.object.clone(),
                revision_id: "100:20260101".into(),
                title: "Fictional statute".into(),
                body: "Fictional body".into(),
                sections: vec![],
                metadata: BTreeMap::new(),
                publication_date: Some("20260101".into()),
                effective_date: Some("20260101".into()),
                source_url: "https://example.invalid/fictional".into(),
                representation: "provider_record".into(),
            },
            retrieved_at: 90,
            captured_at: 90,
            validated_at: 90,
            processor_version: "fictional_v1".into(),
            raw_sha256: "b".repeat(64),
        };
        assert!(!head_observation_superseded(&candidate, &head, 100));
        head.captured_at = 100;
        assert!(head_observation_superseded(&candidate, &head, 100));
        head.captured_at = 90;
        head.validated_at = 100;
        assert!(head_observation_superseded(&candidate, &head, 100));
        head.validated_at = 90;
        candidate.effective_date = None;
        assert!(head_observation_superseded(&candidate, &head, 100));
        candidate.effective_date = Some("20251231".into());
        assert!(head_observation_superseded(&candidate, &head, 100));
        candidate.revision_id = head.record.revision_id.clone();
        head.captured_at = 101;
        assert!(!head_observation_superseded(&candidate, &head, 100));
    }
}

#[cfg(test)]
mod manual_pilot_tests {
    use super::{comparable_dates_advance, load_pilot_candidates, select_precedent_case};
    use crate::config::{DatabaseConfig, IngestionConfig, IngestionMode};
    use openlegal_adapters::law_go_kr::{InventoryItem, InventoryPage};
    use openlegal_domain::legal::{DatabaseError, Dataset, ObjectId};

    fn precedent_page(
        cases: &[(&str, Option<&str>)],
        total: u64,
        done: bool,
        incomplete: bool,
    ) -> InventoryPage {
        InventoryPage {
            source_evidence: None,
            items: cases
                .iter()
                .map(|(id, case_number)| InventoryItem {
                    object: ObjectId {
                        jurisdiction: "kr".into(),
                        provider: "law_go_kr".into(),
                        dataset: Dataset::Precedent,
                        id: (*id).into(),
                    },
                    revision_id: (*id).into(),
                    effective_date: None,
                    publication_date: None,
                    title: "Fictional precedent".into(),
                    data_source: None,
                    case_number: case_number.map(str::to_owned),
                    treaty_class_code: None,
                    amendment_type: None,
                })
                .collect(),
            done,
            total: Some(total),
            rejected_rows: usize::from(incomplete),
            incomplete,
        }
    }

    #[tokio::test(start_paused = true)]
    async fn pilot_window_cancels_inventory_and_detail_workers_together() {
        let shared = tokio_util::sync::CancellationToken::new();
        let inventory = shared.child_token();
        let detail = shared.child_token();
        let watchdog = tokio::spawn(super::pilot_watchdog(
            std::time::Duration::from_secs(60),
            shared,
        ));
        tokio::task::yield_now().await;
        tokio::time::advance(std::time::Duration::from_secs(59)).await;
        assert!(!inventory.is_cancelled());
        assert!(!detail.is_cancelled());
        tokio::time::advance(std::time::Duration::from_secs(1)).await;
        watchdog.await.unwrap();
        assert!(inventory.is_cancelled());
        assert!(detail.is_cancelled());
    }

    #[test]
    fn explicit_precedent_case_needs_complete_exact_unique_provider_identity() {
        let one = precedent_page(&[("204234", Some("2018도14262"))], 1, true, false);
        assert_eq!(
            select_precedent_case(&[one], "2018도14262", Some("204234"))
                .unwrap()
                .object
                .id,
            "204234"
        );
        let many = precedent_page(
            &[
                ("204234", Some("2018도14262")),
                ("204235", Some("2018도14262")),
            ],
            2,
            true,
            false,
        );
        assert_eq!(
            select_precedent_case(&[many], "2018도14262", None).unwrap_err(),
            DatabaseError::AmbiguousCollection
        );
        let wrong_id = precedent_page(&[("204235", Some("2018도14262"))], 1, true, false);
        assert_eq!(
            select_precedent_case(&[wrong_id], "2018도14262", Some("204234")).unwrap_err(),
            DatabaseError::Conflict
        );
        let absent = precedent_page(&[("204235", Some("2019도1"))], 1, true, false);
        assert_eq!(
            select_precedent_case(&[absent], "2018도14262", None).unwrap_err(),
            DatabaseError::NotFound
        );
        for page in [
            precedent_page(&[("204234", Some("2018도14262"))], 2, false, false),
            precedent_page(&[("204234", Some("2018도14262"))], 2, true, false),
            precedent_page(&[("204234", Some("2018도14262"))], 1, true, true),
            precedent_page(&[("204234", None)], 1, true, false),
        ] {
            assert_eq!(
                select_precedent_case(&[page], "2018도14262", Some("204234")).unwrap_err(),
                DatabaseError::SourceInventoryIncomplete
            );
        }
    }

    #[test]
    fn explicit_head_date_guard_accepts_only_nonregressing_known_dates() {
        assert!(comparable_dates_advance(
            Some("20261001"),
            Some("20260901"),
            Some("20260901"),
            Some("20260901")
        ));
        assert!(!comparable_dates_advance(
            Some("20261001"),
            Some("20260801"),
            Some("20260901"),
            Some("20260901")
        ));
        assert!(!comparable_dates_advance(
            None,
            None,
            Some("20260901"),
            None
        ));
        assert!(!comparable_dates_advance(
            Some("20260901"),
            Some("20260901"),
            Some("20260901"),
            Some("20260901")
        ));
    }

    #[tokio::test]
    async fn explicit_manifest_accepts_bounded_identity_hints_and_rejects_duplicates() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("candidates.json");
        let config = DatabaseConfig {
            max_raw_bytes: Default::default(),
            auto_collection: true,
            blob_path: "blobs".into(),
            index_path: "index".into(),
            mecab_dictionary_path: "dictionary".into(),
            widget_html: "widget".into(),
            ingestion: Some(IngestionConfig {
                adaptive_polling: true,
                credential_env: "PROVIDER_CREDENTIAL".into(),
                proxy: None,
                kubectl: "/usr/local/bin/kubectl".into(),
                kubeconfig: "/run/kubeconfig".into(),
                context: "test".into(),
                namespace: "test".into(),
                worker_image: "example.invalid/worker@sha256:placeholder".into(),
                collection_namespace: "openlegal-serving".into(),
                collection_job_template_path: "/etc/openlegal/collection-job.json".into(),
                document_worker: Default::default(),
                provider_requests: Default::default(),
                enabled: true,
                mode: IngestionMode::Pilot,
                manual_candidates_path: Some(path.clone()),
                retain_history_bodies: false,
                detail_timeout_secs: 3600,
                detail_job_workers: 1,
                scan_interval_secs: 3600,
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
