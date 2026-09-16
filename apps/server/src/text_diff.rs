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
    ApplyPatchInput, ApplyPatchResult, AttachmentHandle, AttachmentPage, AttachmentRead,
    AttachmentSummary, AttachmentUpload, CompareInput, ComparisonSummary, DiffInput, DiffResult,
    PageRequest, PageResponse, TextDiffError,
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
        for name in ["show_text_diff", "text.diff.show"] {
            let service = self.service.clone();
            registry.register_typed::<ShowInput, ShowOutput, _, _>(
            name,
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
        }
        for name in ["get_text_diff_page", "text.diff.page"] {
            let service = self.service.clone();
            registry.register_typed::<PageRequest, PageResponse, _, _>(
            name,
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
        }
        let deletion = self.service.clone();
        registry.register_text_diff_delete::<HandleInput, DeleteOutput, _, _>(
            move |input, _| {
                let service = deletion.clone();
                async move {
                    service.delete(&input.comparison_id).map_err(map_error)?;
                    Ok(ToolOutput::new(DeleteOutput {
                        schema_version: 1,
                        deleted: true,
                    }))
                }
            },
        )?;
        register_canonical(registry, self.service)
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

fn register_canonical(
    registry: &mut ToolRegistry,
    service: Arc<TextDiffService>,
) -> Result<(), ServerError> {
    let comparison = service.clone();
    registry.register_typed::<DiffInput, DiffResult, _, _>(
        "text.diff", "Compare UTF-8 text strings or sealed text attachment references. Returns paged character differences, a complete unified patch attachment, and an explanation of Rust Myers line/scalar comparison. Exact bytes are preserved; not a legal equivalence assessment.",
        ToolOptions::default(), move |input, context| { let service = comparison.clone(); async move {
            context.progress.report(ProgressStage::Processing).await?;
            let result = service.compare_sources(input, context.request.cancellation, context.deadline).await.map_err(map_error)?;
            let _ = context.progress.report(ProgressStage::Complete).await;
            Ok(ToolOutput::new(result))
        } }
    )?;
    let application = service.clone();
    registry.register_typed::<ApplyPatchInput, ApplyPatchResult, _, _>(
        "text.apply_patch", "Apply a single-text unified patch to a supplied UTF-8 target, inline or through sealed attachments. Exact declared positions and context must match; no fuzz, offsets, binary patches, multi-file changes or filesystem writes. Returns a complete text attachment; conflicts fail atomically.",
        ToolOptions::default(), move |input, context| { let service = application.clone(); async move {
            context.progress.report(ProgressStage::Processing).await?;
            let result = service.apply_patch(input, context.request.cancellation, context.deadline).await.map_err(map_error)?;
            let _ = context.progress.report(ProgressStage::Complete).await;
            Ok(ToolOutput::new(result))
        } }
    )?;
    let upload = service.clone();
    registry.register_attachment_builtin::<AttachmentUpload, AttachmentSummary, _, _>(
        "text.attachment.upload", "Create or append a temporary UTF-8 text/patch attachment in chunks of at most 32 KiB. Initial upload supplies kind and total_bytes; continuations supply attachment_id and byte offset. Exact retries succeed. Final seals immutable bytes; ten-minute expiry never extends. Bearer handles authorize read and deletion.",
        ToolOptions { annotations: rmcp::model::ToolAnnotations::from_raw(None, Some(false), Some(false), Some(false), Some(false)), meta: None },
        move |input, context| { let service = upload.clone(); async move {
            if context.request.cancellation.is_cancelled() { return Err(ToolError::Unavailable); }
            service.upload_attachment(input).map(ToolOutput::new).map_err(map_error)
        } }
    )?;
    let reading = service.clone();
    registry.register_typed::<AttachmentRead, AttachmentPage, _, _>(
        "text.attachment.read", "Read a sealed attachment at a UTF-8 byte boundary. Returns up to 32 KiB with next_offset, total bytes and fixed expiry. Missing and expired handles are indistinguishable.",
        ToolOptions::default(), move |input, _| { let service = reading.clone(); async move {
            let page = service.read_attachment(input).map_err(map_error)?;
            Ok(ToolOutput { structured: page, text: Some("Attachment chunk is in structuredContent.".into()), meta: None })
        } }
    )?;
    let deleting = service.clone();
    registry.register_attachment_builtin::<AttachmentHandle, DeleteOutput, _, _>(
        "text.attachment.delete", "Delete a temporary attachment using its bearer handle. Repeated deletion succeeds. Bytes already acquired by a reader or running operation cannot be retracted.",
        ToolOptions { annotations: rmcp::model::ToolAnnotations::from_raw(None, Some(false), Some(true), Some(true), Some(false)), meta: None },
        move |input, _| { let service = deleting.clone(); async move {
            service.delete_attachment(&input.attachment_id).map_err(map_error)?;
            Ok(ToolOutput::new(DeleteOutput { schema_version: 1, deleted: true }))
        } }
    )?;
    registry.register_attachment_builtin::<HandleInput, DeleteOutput, _, _>(
        "text.diff.delete",
        "Delete a transient comparison using its bearer handle; repeated deletion succeeds.",
        ToolOptions {
            annotations: rmcp::model::ToolAnnotations::from_raw(
                None,
                Some(false),
                Some(true),
                Some(true),
                Some(false),
            ),
            meta: None,
        },
        move |input, _| {
            let service = service.clone();
            async move {
                service.delete(&input.comparison_id).map_err(map_error)?;
                Ok(ToolOutput::new(DeleteOutput {
                    schema_version: 1,
                    deleted: true,
                }))
            }
        },
    )
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
