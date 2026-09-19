//! Thin MCP adapters for the shared corpus and comparison services.
use crate::{
    ServerError,
    registry::{ToolError, ToolModule, ToolOptions, ToolOutput, ToolRegistry},
};
use openlegal_application::{
    database::DatabaseService,
    search::{SearchMode, SearchService},
    text_diff::TextDiffService,
};
use openlegal_domain::{
    legal::*,
    legal_search::{SearchPage, SearchRequest},
    text_diff::{CompareInput, ComparisonSummary},
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
pub const WIDGET_URI: &str = "ui://openlegal/database-v1.html";
pub struct DatabaseTools {
    pub database: Arc<DatabaseService>,
    pub reader: Arc<openlegal_application::database_read::DatabaseReader>,
    pub search: Arc<SearchService>,
    pub comparison: Arc<TextDiffService>,
}
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
        registry.register_typed::<ReadInput, ContentPage, _, _>(
            "database.get",
            "Read one legal object at HEAD or an exact retained revision/capture. HEAD includes TTL and fetch/cache times. Content pages contain up to 32 KiB. Continue using selector kind=capture and the returned metadata.capture_id with next_offset and the same section; historical content never falls back to HEAD.",
            ToolOptions::default(),
            move |input, ctx| {
                let service = service.clone();
                async move {
                    if input.section.as_ref().is_some_and(|s| s.len() > 256)
                        || input.offset > 64 * 1024 * 1024
                        || input.offset > 0
                            && !matches!(input.selector, RevisionSelector::Capture { .. })
                    {
                        return Err(ToolError::InvalidInput);
                    }
                    let (result, session) = service
                        .get(
                            GetRequest {
                                object: input.object,
                                selector: input.selector,
                                fresh_only: input.fresh_only,
                            },
                            input.session,
                            ctx.request.cancellation,
                        )
                        .await
                        .map_err(map_error)?;
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
                    Ok(output(ContentPage {
                        session,
                        schema_version: 1,
                        metadata: result.into(),
                        section,
                        text: page,
                        offset: input.offset,
                        next_offset,
                        sections,
                        section_count,
                        next_sections_offset,
                    }))
                }
            },
        )?;
        let service = self.database.clone();
        registry.register_typed::<GetRequest, MetadataResult, _, _>(
            "database.get_metadata",
            "Retrieve metadata and provenance for HEAD or an exact checkpoint, with HEAD freshness and upstream retrieval/validation/cache times. No legal body content is returned.",
            ToolOptions::default(),
            move |input, ctx| {
                let service = service.clone();
                async move {
                    service
                        .get_metadata(input, ctx.request.cancellation)
                        .await
                        .map(output)
                        .map_err(map_error)
                }
            },
        )?;
        let service = self.database.clone();
        registry.register_typed::<HistoryInput, HistoryPage, _, _>(
            "database.history",
            "List provider revision checkpoints or separate capture observations of one object, newest first. A retained catalog entry does not promise retained body content. Precedent provider revision history is unsupported; capture history remains available.",
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
        for (name, mode, description) in [
            (
                "database.query",
                SearchMode::Query,
                "Search the managed corpus using the query DSL (AND, OR, NOT, grouping, title/body fields, analyzed words and prefixes). Korean Lindera and MeCab-Ko analysis uses NFC and ASCII lowercase with no stopwords. Positive expressions must match within one engine; NOT excludes a match by either engine. Double quotes require an exact source substring. Stable bounded pages may contain zero hits and a continuation; coverage and index lag are explicit.",
            ),
            (
                "database.rg",
                SearchMode::Ripgrep,
                "Search managed legal content using bounded ripgrep regex matching, case sensitive and line oriented by default. Typed literal, ignore_case, context_lines and filters are supported; filesystem paths and CLI arguments are never accepted. Continuations retain a fixed corpus generation for ten minutes.",
            ),
        ] {
            let search = self.search.clone();
            registry.register_typed::<SearchRequest, SearchPage, _, _>(
                name,
                description,
                ToolOptions::default(),
                move |input, ctx| {
                    let search = search.clone();
                    async move {
                        search
                            .search(
                                mode,
                                input,
                                ctx.deadline.into_std(),
                                ctx.request.cancellation,
                            )
                            .await
                            .map(output)
                            .map_err(map_error)
                    }
                },
            )?;
        }
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
        DatabaseError::InvalidInput => ToolError::InvalidInput,
        DatabaseError::NotFound => ToolError::NotFound,
        DatabaseError::StorageUnavailable => ToolError::StorageUnavailable,
        DatabaseError::StorageCorrupt => ToolError::StorageCorrupt,
        DatabaseError::Capacity => ToolError::ResourceLimit,
        DatabaseError::AmbiguousRevision => ToolError::Ambiguous,
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
