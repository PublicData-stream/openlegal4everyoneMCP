//! Public history uses exact application identities; no filesystem paths are inputs.
use crate::{
    ServerError,
    demo::map_error,
    progress::ProgressStage,
    registry::{ToolError, ToolOptions, ToolOutput, ToolRegistry},
};
use openlegal_application::{RetrievalService, text_diff::TextDiffService};
use openlegal_domain::{
    Query,
    history::{SnapshotEnvelope, SnapshotPage, valid_snapshot_id},
    text_diff::ComparisonSummary,
};
use schemars::JsonSchema;
use serde::Deserialize;
use std::sync::Arc;

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ListInput {
    query: Query,
    #[schemars(length(max = 256))]
    cursor: Option<String>,
    #[serde(default = "page_limit")]
    #[schemars(range(min = 1, max = 20))]
    limit: usize,
}
fn page_limit() -> usize {
    10
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct GetInput {
    query: Query,
    #[schemars(length(min = 64, max = 64), regex(pattern = "^[0-9a-f]{64}$"))]
    snapshot_id: String,
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct CompareInput {
    #[schemars(length(min = 1, max = 64), regex(pattern = "^[A-Za-z0-9_-]+$"))]
    source: String,
    #[schemars(length(min = 1, max = 128), regex(pattern = "^[A-Za-z0-9_-]+$"))]
    id: String,
    #[schemars(length(min = 64, max = 64), regex(pattern = "^[0-9a-f]{64}$"))]
    before_snapshot_id: String,
    #[schemars(length(min = 64, max = 64), regex(pattern = "^[0-9a-f]{64}$"))]
    after_snapshot_id: String,
}

pub(crate) fn register(
    registry: &mut ToolRegistry,
    service: Arc<RetrievalService>,
    comparison: Option<Arc<TextDiffService>>,
) -> Result<(), ServerError> {
    let listing = service.clone();
    registry.register_typed::<ListInput, SnapshotPage, _, _>(
        "demo_list_snapshots", "List retained synthetic captures for one exact record or search-page query. Captures are local observations, not legal revisions. Newest first; retention may leave gaps. Never fetches upstream.",
        ToolOptions::default(), move |input, context| { let service = listing.clone(); async move {
            input.query.validate().map_err(map_error)?;
            context.progress.report(ProgressStage::CheckingCache).await?;
            let page = service.list_snapshots(input.query, input.cursor, input.limit, context.request.cancellation).await.map_err(map_error)?;
            context.progress.report(ProgressStage::Complete).await?;
            Ok(ToolOutput::new(page))
        } }
    )?;
    let detail = service.clone();
    registry.register_typed::<GetInput, SnapshotEnvelope, _, _>(
        "demo_get_snapshot", "Read an exact retained synthetic snapshot with capture provenance. Historical observations have no fresh/current claim. Missing snapshots never substitute current data or fetch upstream.",
        ToolOptions::default(), move |input, context| { let service = detail.clone(); async move {
            input.query.validate().map_err(map_error)?;
            if !valid_snapshot_id(&input.snapshot_id) { return Err(ToolError::InvalidInput); }
            context.progress.report(ProgressStage::CheckingCache).await?;
            let result = service.get_snapshot(input.query, input.snapshot_id, context.request.cancellation).await.map_err(map_error)?;
            context.progress.report(ProgressStage::Complete).await?;
            Ok(ToolOutput::new(result))
        } }
    )?;
    if let Some(comparison) = comparison {
        registry.register_typed::<CompareInput, ComparisonSummary, _, _>(
            "demo_compare_record_snapshots", "Compare two exact retained snapshots of the same synthetic record, in before/after order. Compares title + two LF bytes + body without normalization. Uses the transient Rust comparison service; no upstream fetch or legal equivalence claim.",
            ToolOptions::default(), move |input, context| { let service = service.clone(); let comparison = comparison.clone(); async move {
                let query = Query::Get { source: input.source, id: input.id };
                query.validate().map_err(map_error)?;
                if !valid_snapshot_id(&input.before_snapshot_id) || !valid_snapshot_id(&input.after_snapshot_id) { return Err(ToolError::InvalidInput); }
                context.progress.report(ProgressStage::CheckingCache).await?;
                let before = service.get_snapshot(query.clone(), input.before_snapshot_id, context.request.cancellation.clone()).await.map_err(map_error)?;
                let after = service.get_snapshot(query, input.after_snapshot_id, context.request.cancellation.clone()).await.map_err(map_error)?;
                context.progress.report(ProgressStage::Processing).await?;
                let result = comparison.compare_snapshots(before, after, context.request.cancellation, context.deadline).await.map_err(crate::text_diff::map_error)?;
                let _ = context.progress.report(ProgressStage::Complete).await;
                Ok(ToolOutput::new(result))
            } }
        )?;
    }
    Ok(())
}
