//! Thin MCP adapters for the shared corpus and comparison services.
use crate::{
    ServerError,
    demand_result::{self, CollectionSearchInput, ReadResult, WithCollection},
    registry::{ToolError, ToolModule, ToolOptions, ToolOutput, ToolRegistry},
};
use openlegal_application::{
    Clock,
    database::DatabaseService,
    demand_collection::{DemandCollectionCoordinator, validate_collection_term},
    search::{SearchMode, SearchService},
    text_diff::TextDiffService,
};
use openlegal_domain::{
    collection::{CollectionReceipt, CollectionRequest, CollectionStatusInput},
    legal::*,
    legal_search::{QuerySearchRequest, SearchPage, SearchRequest},
    text_diff::{CompareInput, ComparisonSummary},
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
pub const WIDGET_URI: &str = "ui://openlegal/database-v1.html";
pub struct DatabaseTools {
    pub demand: Option<Arc<DemandCollectionCoordinator>>,
    pub database: Arc<DatabaseService>,
    pub reader: Arc<openlegal_application::database_read::DatabaseReader>,
    pub search: Arc<SearchService>,
    pub comparison: Arc<TextDiffService>,
    pub store: Arc<openlegal_adapters::corpus::PgCorpusStore>,
}
#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct CorpusStatusInput {}
/// The progress payload is a JSON object, including its diagnostic
/// fields. MCP rejects an unconstrained `Value` output schema at registration.
#[derive(Serialize, JsonSchema)]
#[serde(transparent)]
struct CorpusStatusOutput(serde_json::Map<String, serde_json::Value>);
#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ReadInput {
    object: ObjectId,
    #[serde(default)]
    selector: RevisionSelector,
    #[serde(default)]
    fresh_only: bool,
    #[serde(default)]
    offset: usize,
    section: Option<String>,
    session: Option<String>,
    #[serde(default)]
    sections_offset: usize,
}
#[derive(Serialize, JsonSchema)]
struct ContentPage {
    session: String,
    schema_version: u32,
    metadata: MetadataResult,
    section: String,
    text: String,
    offset: usize,
    next_offset: Option<usize>,
    sections: Vec<SectionSummary>,
    section_count: usize,
    next_sections_offset: Option<usize>,
}
#[derive(Serialize, JsonSchema)]
struct SectionSummary {
    id: String,
    title: String,
    kind: SectionKind,
    bytes: usize,
}
#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct HistoryInput {
    object: ObjectId,
    kind: HistoryKind,
    cursor: Option<String>,
    #[serde(default = "page_limit")]
    limit: usize,
}
fn page_limit() -> usize {
    20
}
#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct DiffInput {
    object: ObjectId,
    before: RevisionSelector,
    after: RevisionSelector,
    #[serde(default)]
    include_ocr: bool,
}
#[derive(Serialize, JsonSchema)]
struct DiffOutput {
    schema_version: u32,
    before: MetadataResult,
    after: MetadataResult,
    comparison: ComparisonSummary,
    includes_ocr: bool,
}
#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct Empty {}
#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ObjectStatusInput {
    object: ObjectId,
}
#[derive(Serialize, JsonSchema)]
struct Show {
    schema_version: u32,
}
fn output<T>(structured: T) -> ToolOutput<T> {
    ToolOutput {
        structured,
        text: Some(
            "Legal corpus result; content, provenance and freshness are in structuredContent."
                .into(),
        ),
        meta: None,
    }
}
impl ToolModule for DatabaseTools {
    fn register(self, registry: &mut ToolRegistry) -> Result<(), ServerError> {
        let service = self.reader.clone();
        let notices = self.store.clone();
        let demand = self.demand.clone();
        registry.register_core_rich_result::<ReadInput, ReadResult<ContentPage>, _, _>(
            "database.get",
            "Read one legal object at HEAD or an exact retained revision/capture. HEAD includes TTL and fetch/cache times. Content pages contain up to 32 KiB. Continue using selector kind=capture and the returned metadata.capture_id with next_offset and the same section; historical content never falls back to HEAD. When automatic collection is enabled, an initial missing or stale national-statute HEAD may enqueue bounded refresh and return collection status or a pending receipt.",
            demand_result::options(demand.as_ref().is_some_and(|d| d.enabled())),
            move |input, ctx| {
                let service = service.clone();
                let notices = notices.clone();
                let demand = demand.clone();
                async move {
                    if input.section.as_ref().is_some_and(|s| s.len() > 256)
                        || input.offset > 64 * 1024 * 1024
                        || input.offset > 0
                            && !matches!(input.selector, RevisionSelector::Capture { .. })
                    {
                        return Err(ToolError::InvalidInput);
                    }
                    let request = GetRequest { object: input.object, selector: input.selector, fresh_only: input.fresh_only };
                    let initial_head = matches!(request.selector, RevisionSelector::Head) && input.session.is_none() && input.offset == 0 && input.sections_offset == 0;
                    let local = service.get(request.clone(),
                            input.session,
                            ctx.request.cancellation.clone(),
                        )
                        .await;
                    let (result, session) = match local {
                        Ok(local) => local,
                        Err(error) if initial_head && demand_result::refreshable_error(error) => {
                            if let Some(demand) = demand {
                                let collection = demand.head(&request, true, &ctx.request.cancellation).await;
                                return Ok(demand_result::admission_result(demand_result::pending(error, collection).map(output), &notices).await);
                            }
                            return Ok(demand_result::admission_result(Err(map_error(error)), &notices).await);
                        }
                        Err(error) => return Ok(demand_result::admission_result(Err(map_error(error)), &notices).await),
                    };
                    let record = &result.capture.record;
                    let section = input.section.unwrap_or_else(|| "body".into());
                    let text = match section.as_str() {
                        "body" => record.body.as_str(),
                        "title" => record.title.as_str(),
                        id => record
                            .sections
                            .iter()
                            .find(|s| s.id == id)
                            .map(|s| s.text.as_str())
                            .ok_or(ToolError::NotFound)?,
                    };
                    if input.offset > text.len() || !text.is_char_boundary(input.offset) {
                        return Err(ToolError::InvalidInput);
                    }
                    let mut end = (input.offset + 32768).min(text.len());
                    while !text.is_char_boundary(end) {
                        end -= 1;
                    }
                    let page = text[input.offset..end].to_string();
                    let next_offset = (end < text.len()).then_some(end);
                    if input.sections_offset > record.sections.len() {
                        return Err(ToolError::InvalidInput);
                    }
                    let section_count = record.sections.len();
                    let mut sections = Vec::new();
                    let mut used = 0usize;
                    for s in record.sections.iter().skip(input.sections_offset).take(100) {
                        let cost = s.id.len() + s.title.len() + 128;
                        if used + cost > 32768 {
                            break;
                        }
                        used += cost;
                        sections.push(SectionSummary {
                            id: s.id.clone(),
                            title: s.title.clone(),
                            kind: s.kind.clone(),
                            bytes: s.text.len(),
                        });
                    }
                    let section_end = input.sections_offset + sections.len();
                    let next_sections_offset =
                        (section_end < section_count).then_some(section_end);
                    let mut metadata: MetadataResult = result.into();
                    metadata.collection_notices = notices.collection_notices(&[metadata.object.dataset], Some(&metadata.object)).await.map_err(map_error)?;
                    let collection = if initial_head {
                        if let Some(demand) = demand {
                            Some(demand.head(&request, metadata.freshness.as_ref().is_some_and(|f| f.state != openlegal_domain::FreshnessState::Fresh), &ctx.request.cancellation).await)
                        } else { None }
                    } else { None };
                    Ok(demand_result::admission_result(Ok(output(ReadResult::Ready(WithCollection { result: ContentPage {
                        session,
                        schema_version: 1,
                        metadata,
                        section,
                        text: page,
                        offset: input.offset,
                        next_offset,
                        sections,
                        section_count,
                        next_sections_offset,
                    }, collection }))), &notices).await)
                }
            },
        )?;
        let service = self.database.clone();
        let notices = self.store.clone();
        let demand = self.demand.clone();
        registry.register_core_rich_result::<GetRequest, ReadResult<MetadataResult>, _, _>(
            "database.get_metadata",
            "Retrieve metadata and provenance for HEAD or an exact checkpoint, with HEAD freshness and upstream retrieval/validation/cache times. No legal body content is returned. Eligible missing or stale HEAD may enqueue bounded refresh when automatic collection is enabled.",
            demand_result::options(demand.as_ref().is_some_and(|d| d.enabled())),
            move |input, ctx| {
                let service = service.clone();
                let notices = notices.clone();
                let demand = demand.clone();
                async move {
                    let object = input.object.clone();
                    let mut result = match service.get_metadata(input.clone(), ctx.request.cancellation.clone()).await {
                        Ok(result) => result,
                        Err(error) if matches!(input.selector, RevisionSelector::Head) && demand_result::refreshable_error(error) => {
                            if let Some(demand) = demand {
                                let collection = demand.head(&input, true, &ctx.request.cancellation).await;
                                return Ok(demand_result::admission_result(demand_result::pending(error, collection).map(output), &notices).await);
                            }
                            return Ok(demand_result::admission_result(Err(map_error(error)), &notices).await);
                        }
                        Err(error) => return Ok(demand_result::admission_result(Err(map_error(error)), &notices).await),
                    };
                    result.collection_notices = notices.collection_notices(&[object.dataset], Some(&object)).await.map_err(map_error)?;
                    let collection = if matches!(input.selector, RevisionSelector::Head) {
                        if let Some(demand) = demand { Some(demand.head(&input, result.freshness.as_ref().is_some_and(|f| f.state != openlegal_domain::FreshnessState::Fresh), &ctx.request.cancellation).await) } else { None }
                    } else { None };
                    Ok(demand_result::admission_result(Ok(output(ReadResult::Ready(WithCollection { result, collection }))), &notices).await)
                }
            },
        )?;
        let clone_store = self.store.clone();
        registry.register_rich::<CorpusStatusInput, CorpusStatusOutput, _, _>(
            "database.corpus_status",
            "Read durable progress of finite canonical corpus cloning, including independent current/history/treaty views, stable traversals, missing bodies and open gaps. This does not contact the provider. Canonical clone completion is not an atomic upstream snapshot or completeness of deferred supplementary sources.",
            ToolOptions::default(),
            move |_, ctx| { let store=clone_store.clone(); async move {
                match store.clone_progress_cancellable(&ctx.request.cancellation).await.map_err(map_error)? {
                    serde_json::Value::Object(progress) => await_corpus_status_sidecar(
                        &ctx.request.cancellation,
                        demand_result::admission_output(output(CorpusStatusOutput(progress)), &store),
                    ).await,
                    _ => Err(ToolError::StorageCorrupt),
                }
            } }
        )?;
        let status_store = self.store.clone();
        registry.register_rich::<ObjectStatusInput, ObjectStatus, _, _>(
            "database.object_status",
            "Read local observation, current collection job, index visibility and a bounded processing estimate for one legal object. This does not contact the provider or enqueue collection.",
            ToolOptions::default(),
            move |input, ctx| {
                let store = status_store.clone();
                async move {
                    if ctx.request.cancellation.is_cancelled() {
                        return Err(ToolError::Unavailable);
                    }
                    let result = store
                        .object_status(&input.object, openlegal_application::SystemClock::default().now())
                        .await
                        .map_err(map_error)?;
                    if ctx.request.cancellation.is_cancelled() {
                        return Err(ToolError::Unavailable);
                    }
                    let snapshot = store.provider_admission_snapshot().await;
                    Ok(demand_result::object_admission_output(output(result), snapshot))
                }
            },
        )?;
        let service = self.database.clone();
        registry.register_typed::<HistoryInput, HistoryPage, _, _>(
            "database.history",
            "List provider revision checkpoints ordered by effective date, falling back to publication date, newest first; missing dates follow dated revisions. Capture observations are separately ordered newest capture first. A retained catalog entry does not promise retained body content. Treaty and decision/precedent provider revision history is unsupported; capture history remains available.",
            ToolOptions::default(),
            move |input, ctx| {
                let service = service.clone();
                async move {
                    service
                        .history(
                            input.object,
                            input.kind,
                            input.cursor,
                            input.limit,
                            ctx.request.cancellation,
                        )
                        .await
                        .map(output)
                        .map_err(map_error)
                }
            },
        )?;
        let search = self.search.clone();
        let demand = self.demand.clone();
        let admission = self.store.clone();
        registry.register_demand_rich::<CollectionSearchInput<QuerySearchRequest>, WithCollection<SearchPage>, _, _>(
            "database.query",
            "Search the managed corpus using the query DSL (AND, OR, NOT, grouping, in:title:, in:body:, in:case_number:, analyzed words and prefixes). Bare title:, body:, and case_number: are invalid; use the in: prefix. Double quotes require an exact case-sensitive source substring. Alternatively, literal: true searches the entire query as a source substring; ignore_case applies only with literal: true. Literal excerpts surround the first matching source substring. Korean Lindera and MeCab-Ko analysis uses NFC and ASCII lowercase with no stopwords. Positive expressions must match within one engine; NOT excludes a match by either engine. Stable bounded pages may contain zero hits and a continuation; coverage and index lag are explicit. Optional collection_term is a separate bounded literal provider term; it never changes local query semantics. Eligible first-page searches may enqueue collection when enabled.",
            demand_result::options(demand.as_ref().is_some_and(|d| d.enabled())),
            move |input, ctx| {
                let search = search.clone();
                let demand = demand.clone();
                let admission = admission.clone();
                async move {
                    let request: SearchRequest = input.request.into();
                    validate_collection_term(input.collection_term.as_deref(), &request.filters.datasets).map_err(map_error)?;
                    let result = search
                        .search(
                            SearchMode::Query,
                            request.clone(),
                            ctx.deadline.into_std(),
                            ctx.request.cancellation.clone(),
                        )
                        .await
                        .map_err(map_error)?;
                    let collection = if let Some(demand) = demand { Some(demand.search(&request, SearchMode::Query, input.collection_term.as_deref(), &ctx.request.cancellation).await.map_err(map_error)?) } else { None };
                    Ok(demand_result::admission_output(output(WithCollection { result, collection }), &admission).await)
                }
            },
        )?;
        let search = self.search.clone();
        let demand = self.demand.clone();
        let admission = self.store.clone();
        registry.register_demand_rich::<CollectionSearchInput<SearchRequest>, WithCollection<SearchPage>, _, _>(
            "database.rg",
            "Search managed legal content and case numbers using bounded ripgrep regex matching, case sensitive and line oriented by default. Typed literal, ignore_case, context_lines and filters are supported; filesystem paths and CLI arguments are never accepted. Continuations retain a fixed corpus generation for ten minutes. Optional collection_term is a separate bounded literal provider term; literal first-page searches may enqueue collection when enabled. Regex and filtered searches require an explicit term.",
            demand_result::options(demand.as_ref().is_some_and(|d| d.enabled())),
            move |input, ctx| {
                let search = search.clone();
                let demand = demand.clone();
                let admission = admission.clone();
                async move {
                    let request = input.request;
                    validate_collection_term(input.collection_term.as_deref(), &request.filters.datasets).map_err(map_error)?;
                    let result = search
                        .search(
                            SearchMode::Ripgrep,
                            request.clone(),
                            ctx.deadline.into_std(),
                            ctx.request.cancellation.clone(),
                        )
                        .await
                        .map_err(map_error)?;
                    let collection = if let Some(demand) = demand { Some(demand.search(&request, SearchMode::Ripgrep, input.collection_term.as_deref(), &ctx.request.cancellation).await.map_err(map_error)?) } else { None };
                    Ok(demand_result::admission_output(output(WithCollection { result, collection }), &admission).await)
                }
            },
        )?;
        let requests = self.store.clone();
        registry.register_collection_request::<CollectionRequest, CollectionReceipt, _, _>(
            move |input, _| {
                let requests = requests.clone();
                async move {
                    let receipt = requests
                        .request_collection(input)
                        .await
                        .map(output)
                        .map_err(map_error);
                    Ok(demand_result::admission_result(receipt, &requests).await)
                }
            },
        )?;
        let requests = self.store.clone();
        registry.register_core_rich_result::<CollectionStatusInput, CollectionReceipt, _, _>(
            "database.collection_status",
            "Read the status of an explicit collection request by request_id. This does not initiate collection.",
            ToolOptions::default(),
            move |input, _| {
                let requests = requests.clone();
                async move {
                    let receipt = requests.collection_status(&input.request_id).await.map(output).map_err(map_error);
                    Ok(demand_result::admission_result(receipt, &requests).await)
                }
            },
        )?;
        let service = self.database;
        let comparison = self.comparison;
        registry.register_typed::<DiffInput, DiffOutput, _, _>(
            "database.diff",
            "Compare two exact checkpoints of the same legal object using line-oriented Myers diff and Unicode scalar highlights. Returns a paged text comparison handle plus both immutable capture identities. This is a textual comparison, not a determination of legal applicability. OCR is excluded unless include_ocr is true. Each resolved comparison text is limited to 1 MiB.",
            ToolOptions::default(),
            move |input, ctx| {
                let service = service.clone();
                let comparison = comparison.clone();
                async move {
                    let (a, b) = service
                        .resolve_diff(
                            input.object,
                            input.before,
                            input.after,
                            ctx.request.cancellation.clone(),
                        )
                        .await
                        .map_err(map_error)?;
                    let before = diff_text(&a, input.include_ocr)?;
                    let after = diff_text(&b, input.include_ocr)?;
                    let result = comparison
                        .compare(
                            CompareInput {
                                before,
                                after,
                                before_label: Some(a.capture.record.revision_id.clone()),
                                after_label: Some(b.capture.record.revision_id.clone()),
                            },
                            ctx.request.cancellation,
                            ctx.deadline,
                        )
                        .await
                        .map_err(crate::text_diff::map_error)?;
                    Ok(output(DiffOutput {
                        schema_version: 1,
                        before: a.into(),
                        after: b.into(),
                        comparison: result,
                        includes_ocr: input.include_ocr,
                    }))
                }
            },
        )?;
        registry.register_typed::<Empty, Show, _, _>(
            "database.show",
            "Open the legal corpus search, history and checkpoint comparison app.",
            ToolOptions {
                meta: Some(rmcp::model::MetaObject(serde_json::from_value(
                    serde_json::json!({"ui":{"resourceUri":WIDGET_URI}}),
                )?)),
                ..ToolOptions::default()
            },
            |_, _| async { Ok(output(Show { schema_version: 1 })) },
        )?;
        Ok(())
    }
}
fn diff_text(result: &GetResult, ocr: bool) -> Result<String, ToolError> {
    let r = &result.capture.record;
    if r.title.len().saturating_add(r.body.len()).saturating_add(2) > 1024 * 1024 {
        return Err(ToolError::ResourceLimit);
    }
    let mut text = format!("{}\n\n{}", r.title, r.body);
    for s in &r.sections {
        if s.kind == SectionKind::Extracted || (ocr && s.kind == SectionKind::Ocr) {
            if text.len() + s.text.len() + 2 > 1024 * 1024 {
                return Err(ToolError::ResourceLimit);
            }
            text.push_str("\n\n");
            text.push_str(&s.text);
        }
    }
    if text.len() > 1024 * 1024 {
        return Err(ToolError::ResourceLimit);
    }
    Ok(text)
}
pub(crate) fn map_error(e: DatabaseError) -> ToolError {
    match e {
        DatabaseError::SourceUnavailable
        | DatabaseError::SourceDataInvalid
        | DatabaseError::SourceUnauthorized => ToolError::Unavailable,
        DatabaseError::SourceTransient | DatabaseError::SourceDownloadFailed => {
            ToolError::StorageUnavailable
        }
        DatabaseError::InvalidInput => ToolError::InvalidInput,
        DatabaseError::InvalidFieldShorthand => ToolError::InvalidFieldShorthand,
        DatabaseError::InvalidRegex => ToolError::InvalidRegex,
        DatabaseError::NotFound => ToolError::NotFound,
        DatabaseError::NotObserved => ToolError::NotObserved,
        DatabaseError::CollectionIncomplete => ToolError::CollectionIncomplete,
        DatabaseError::SourceInventoryIncomplete => ToolError::SourceInventoryIncomplete,
        DatabaseError::StorageUnavailable | DatabaseError::StorageContended => {
            ToolError::StorageUnavailable
        }
        DatabaseError::StorageCorrupt => ToolError::StorageCorrupt,
        DatabaseError::Capacity | DatabaseError::BudgetExhausted => ToolError::ResourceLimit,
        DatabaseError::AmbiguousRevision => ToolError::Ambiguous,
        DatabaseError::AmbiguousCollection => ToolError::Ambiguous,
        DatabaseError::FreshnessUnavailable => ToolError::FreshnessUnavailable,
        DatabaseError::RevisionUnavailable => ToolError::SnapshotUnavailable,
        DatabaseError::ProcessingPending => ToolError::ProcessingPending,
        DatabaseError::UnsupportedHistory => ToolError::UnsupportedHistory,
        DatabaseError::HistoryIncomplete => ToolError::HistoryIncomplete,
        DatabaseError::SessionExpired => ToolError::SessionExpired,
        DatabaseError::SnapshotInvalidated => ToolError::SnapshotInvalidated,
        DatabaseError::Withdrawn => ToolError::Withdrawn,
        _ => ToolError::Unavailable,
    }
}
pub async fn load_widget(
    path: &std::path::Path,
    source: &crate::config::SourceOffer,
) -> Result<crate::resources::ResourceRegistry, ServerError> {
    crate::widget::load_widget(path, source, crate::widget::WidgetKind::Database).await
}

// Provider diagnostics take a read-only FOR SHARE lock. Dropping that read
// cannot replay an upstream operation or leave a write COMMIT ambiguous. The
// progress query and its admission sidecar both honor the request cancellation.
async fn await_corpus_status_sidecar(
    cancel: &tokio_util::sync::CancellationToken,
    sidecar: impl std::future::Future<Output = crate::registry::RichToolOutput<CorpusStatusOutput>>,
) -> Result<crate::registry::RichToolOutput<CorpusStatusOutput>, ToolError> {
    tokio::select! {
        biased;
        _ = cancel.cancelled() => Err(ToolError::Unavailable),
        output = sidecar => Ok(output),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn status_cancellation_drops_a_blocked_read_only_admission_sidecar() {
        use std::sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        };
        struct Dropped(Arc<AtomicBool>);
        impl Drop for Dropped {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }
        let dropped = Arc::new(AtomicBool::new(false));
        let guard = Dropped(dropped.clone());
        let (entered, waiting) = tokio::sync::oneshot::channel();
        let cancel = tokio_util::sync::CancellationToken::new();
        let request_cancel = cancel.clone();
        let task = tokio::spawn(async move {
            await_corpus_status_sidecar(&request_cancel, async move {
                let _guard = guard;
                entered.send(()).unwrap();
                std::future::pending().await
            })
            .await
        });
        waiting.await.unwrap();
        cancel.cancel();
        let result = tokio::time::timeout(std::time::Duration::from_millis(250), task)
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(result, Err(ToolError::Unavailable)));
        assert!(
            dropped.load(Ordering::SeqCst),
            "cancelled sidecar must release its read future"
        );
    }

    #[tokio::test]
    async fn status_sidecar_preserves_ready_output_and_does_not_poll_after_cancellation() {
        let cancel = tokio_util::sync::CancellationToken::new();
        let fields = json!({"initial_canonical_clone_complete":false,"views":[]})
            .as_object()
            .unwrap()
            .clone();
        let expected = serde_json::Value::Object(fields.clone());
        let result = await_corpus_status_sidecar(&cancel, async {
            crate::registry::RichToolOutput::new(CorpusStatusOutput(fields))
        })
        .await
        .unwrap();
        assert_eq!(
            serde_json::to_value(result.output.structured).unwrap(),
            expected
        );
        cancel.cancel();
        let result = await_corpus_status_sidecar(&cancel, async {
            panic!("pre-cancelled diagnostics must not be polled")
        })
        .await;
        assert!(matches!(result, Err(ToolError::Unavailable)));
    }

    #[test]
    fn corpus_progress_registers_as_an_object_without_changing_wire_fields() {
        let progress = json!({"initial_canonical_clone_complete":false,
            "full_available_clone_complete":false,"views":[]});
        let fields = progress.as_object().unwrap().clone();
        assert_eq!(
            serde_json::to_value(CorpusStatusOutput(fields.clone())).unwrap(),
            progress
        );
        let mut registry = ToolRegistry::new();
        registry
            .register_typed::<CorpusStatusInput, CorpusStatusOutput, _, _>(
                "database.corpus_status",
                "Read local clone progress",
                ToolOptions::default(),
                move |_, _| {
                    let fields = fields.clone();
                    async move { Ok(output(CorpusStatusOutput(fields))) }
                },
            )
            .unwrap();
    }

    #[test]
    fn query_input_excludes_ripgrep_context_but_preserves_search_options() {
        let query_schema = serde_json::to_value(schemars::schema_for!(QuerySearchRequest)).unwrap();
        let rg_schema = serde_json::to_value(schemars::schema_for!(SearchRequest)).unwrap();
        assert!(query_schema["properties"].get("context_lines").is_none());
        assert_eq!(query_schema["additionalProperties"], false);
        assert!(rg_schema["properties"].get("context_lines").is_some());
        for context in [0, 1] {
            let input = json!({"query":"medical", "context_lines":context});
            assert!(serde_json::from_value::<QuerySearchRequest>(input.clone()).is_err());
            let rg: SearchRequest = serde_json::from_value(input).unwrap();
            assert_eq!(rg.context_lines, context);
        }
        let query: QuerySearchRequest = serde_json::from_value(json!({
            "query":"Medical", "literal":true, "ignore_case":true,
            "sections":["body"], "include_history":true, "include_ocr":true,
            "filters":{"object_id":"one"}, "limit":1, "cursor":"next"
        }))
        .unwrap();
        let internal: SearchRequest = query.into();
        assert_eq!(internal.context_lines, 0);
        assert_eq!(internal.query, "Medical");
        assert!(internal.literal && internal.ignore_case);
        assert!(internal.include_history && internal.include_ocr);
        assert_eq!(internal.sections, ["body"]);
        assert_eq!(internal.filters.object_id.as_deref(), Some("one"));
        assert_eq!(internal.cursor.as_deref(), Some("next"));
        assert_eq!(internal.limit, 1);
        let omitted: QuerySearchRequest =
            serde_json::from_value(json!({"query":"medical"})).unwrap();
        assert_eq!(omitted.limit, 20);
        assert_eq!(SearchRequest::from(omitted).context_lines, 0);
    }
}
