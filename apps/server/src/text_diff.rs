//! MCP composition for supplied text comparisons and bearer-authorized deletion.
use crate::{
    ServerError,
    config::SourceOffer,
    progress::ProgressStage,
    registry::{
        ToolError, ToolExecutionContext, ToolModule, ToolOptions, ToolOutput, ToolRegistry,
    },
    resources::ResourceRegistry,
};
use openlegal_application::text_diff::TextDiffService;
use openlegal_domain::text_diff::{
    CompareInput, ComparisonSummary, PageRequest, PageResponse, TextDiffError,
};
use rmcp::model::MetaObject;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::{path::Path, sync::Arc};

pub const WIDGET_URI: &str = "ui://openlegal/text-diff-v1.html";

/// Probe the composition-selected server executable before listeners bind.
pub async fn service(worker_path: &Path) -> Result<Arc<TextDiffService>, ServerError> {
    let engine = openlegal_adapters::text_diff::SimilarDiffEngine::new(worker_path).await?;
    Ok(TextDiffService::new(
        Arc::new(engine),
        Arc::new(openlegal_adapters::text_diff::OsHandleGenerator),
    ))
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct HandleInput {
    #[schemars(length(min = 64, max = 64), regex(pattern = r"^[0-9a-f]{64}$"))]
    comparison_id: String,
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ShowInput {
    #[schemars(length(min = 64, max = 64), regex(pattern = r"^[0-9a-f]{64}$"))]
    comparison_id: Option<String>,
    #[schemars(length(max = 1048576))]
    before: Option<String>,
    #[schemars(length(max = 1048576))]
    after: Option<String>,
    #[schemars(length(max = 128))]
    before_label: Option<String>,
    #[schemars(length(max = 128))]
    after_label: Option<String>,
}

#[derive(Serialize, JsonSchema)]
struct ShowOutput {
    schema_version: u32,
    comparison: Option<ComparisonSummary>,
}

#[derive(Serialize, JsonSchema)]
struct DeleteOutput {
    schema_version: u32,
    deleted: bool,
}

pub struct TextDiffTools {
    pub service: Arc<TextDiffService>,
}

impl ToolModule for TextDiffTools {
    fn register(self, registry: &mut ToolRegistry) -> Result<(), ServerError> {
        let service = self.service.clone();
        registry.register_typed::<CompareInput, ComparisonSummary, _, _>(
            "compare_texts",
            "Compare two supplied UTF-8 texts exactly using Rust line and Unicode scalar character diffs. Maximum per text: 1 MiB, 100000 lines, 16 KiB per line; no NUL. Returns a bearer handle for paged results retained for ten minutes. Use show_text_diff to display the handle; anyone with it can read or delete the result. Not a legal equivalence assessment.",
            ToolOptions::default(),
            move |input, context| { let service = service.clone(); async move {
                compare(&service, input, context).await.map(ToolOutput::new)
            } },
        )?;
        let service = self.service.clone();
        registry.register_typed::<ShowInput, ShowOutput, _, _>(
            "show_text_diff",
            "Open the editable text comparison app. Supply no arguments for an empty editor, before and after to create a comparison, or comparison_id to open an existing result without recomputing. Do not mix these modes. Existing original texts load on demand for editing.",
            ToolOptions { meta: Some(MetaObject(serde_json::from_value(serde_json::json!({
                "ui": {"resourceUri": WIDGET_URI}
            }))?)), ..ToolOptions::default() },
            move |input, context| { let service = service.clone(); async move {
                let comparison = match (input.comparison_id, input.before, input.after) {
                    (Some(id), None, None) if input.before_label.is_none() && input.after_label.is_none() => {
                        Some(service.summary(&id).map_err(map_error)?)
                    }
                    (None, Some(before), Some(after)) => Some(compare(&service, CompareInput {
                        before, after, before_label: input.before_label, after_label: input.after_label,
                    }, context).await?),
                    (None, None, None) if input.before_label.is_none() && input.after_label.is_none() => None,
                    _ => return Err(ToolError::InvalidInput),
                };
                Ok(ToolOutput::new(ShowOutput { schema_version: 1, comparison }))
            } },
        )?;
        let service = self.service.clone();
        registry.register_typed::<PageRequest, PageResponse, _, _>(
            "get_text_diff_page",
            "Read a numbered changes/before/after page using a comparison bearer handle. Pages are bounded, ordered, and do not extend the ten-minute expiry. Original text pages concatenate exactly; change fragments include original source ranges.",
            ToolOptions::default(),
            move |input, _| { let service = service.clone(); async move {
                let page = service.page(input).map_err(map_error)?;
                // rmcp otherwise duplicates the complete page as a JSON text block.
                // Keep a single bounded payload, plus a concise non-UI explanation.
                Ok(ToolOutput {
                    structured: page,
                    text: Some("Text comparison page. Exact content and original source ranges are in structuredContent.".into()),
                    meta: None,
                })
            } },
        )?;
        registry.register_text_diff_delete::<HandleInput, DeleteOutput, _, _>(move |input, _| {
            let service = self.service.clone();
            async move {
                service.delete(&input.comparison_id).map_err(map_error)?;
                Ok(ToolOutput::new(DeleteOutput {
                    schema_version: 1,
                    deleted: true,
                }))
            }
        })
    }
}

async fn compare(
    service: &Arc<TextDiffService>,
    input: CompareInput,
    context: ToolExecutionContext,
) -> Result<ComparisonSummary, ToolError> {
    context.progress.report(ProgressStage::Processing).await?;
    let summary = service
        .compare(input, context.request.cancellation, context.deadline)
        .await
        .map_err(map_error)?;
    // The result already exists; do not discard its handle if a progress sink closes now.
    let _ = context.progress.report(ProgressStage::Complete).await;
    Ok(summary)
}

pub(crate) fn map_error(error: TextDiffError) -> ToolError {
    match error {
        TextDiffError::InvalidInput => ToolError::InvalidInput,
        TextDiffError::NotFound => ToolError::NotFound,
        TextDiffError::Busy => ToolError::RateLimited,
        TextDiffError::ResourceLimit => ToolError::ResourceLimit,
        TextDiffError::Unavailable | TextDiffError::Cancelled => ToolError::Unavailable,
        TextDiffError::Internal => ToolError::Internal,
    }
}

pub async fn load_widget(
    path: &Path,
    source: &SourceOffer,
) -> Result<ResourceRegistry, ServerError> {
    crate::widget::load_widget(path, source, crate::widget::WidgetKind::TextDiff).await
}

pub fn widget_resources(
    html: String,
    source: &SourceOffer,
) -> Result<ResourceRegistry, ServerError> {
    crate::widget::widget_resources(html, source, crate::widget::WidgetKind::TextDiff)
}
