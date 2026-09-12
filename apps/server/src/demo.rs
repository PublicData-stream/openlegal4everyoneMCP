//! Composition of fictional read-only tools with the shared retrieval service.

use crate::{
    ServerError,
    config::SourceOffer,
    progress::ProgressStage,
    registry::{
        ToolError, ToolExecutionContext, ToolModule, ToolOptions, ToolOutput, ToolRegistry,
    },
    resources::ResourceRegistry,
};
use openlegal_adapters::{DestinationMode, HttpUpstream};
use openlegal_application::{RetrievalService, Source};
use openlegal_domain::{
    FreshnessRequirement, ProgressStage as RetrievalStage, Query, RetrievalData, RetrievalEnvelope,
    RetrievalError,
};
use openlegal_normalization::{LayoutAProcessor, LayoutBProcessor, PayloadProcessor};
use rmcp::model::{MetaObject, Resource, ResourceContents};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::{path::Path, sync::Arc};
use tokio::{io::AsyncReadExt, sync::watch};

pub const WIDGET_URI: &str = "ui://openlegal-demo/records-v1.html";
pub const WIDGET_MIME: &str = "text/html;profile=mcp-app";

/// Both layouts share provider budgets while retaining distinct source/cache identity.
pub fn service(upstream: &str) -> Result<Arc<RetrievalService>, RetrievalError> {
    let mut sources = Vec::new();
    for (id, processor) in [
        (
            "layout_a",
            Arc::new(LayoutAProcessor) as Arc<dyn PayloadProcessor>,
        ),
        (
            "layout_b",
            Arc::new(LayoutBProcessor) as Arc<dyn PayloadProcessor>,
        ),
    ] {
        sources.push(Source {
            id: id.into(),
            provider: "synthetic".into(),
            dataset: "records".into(),
            processor_version: processor.version().into(),
            upstream: Arc::new(HttpUpstream::new(
                upstream,
                DestinationMode::MockLoopback,
                processor,
            )?),
        });
    }
    RetrievalService::new(
        sources,
        Box::new(openlegal_adapters::MemoryCache::default()),
    )
}

#[derive(Clone, Copy, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum DemoSource {
    LayoutA,
    LayoutB,
}

impl DemoSource {
    fn id(self) -> String {
        match self {
            Self::LayoutA => "layout_a",
            Self::LayoutB => "layout_b",
        }
        .into()
    }
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct SearchInput {
    source: DemoSource,
    #[serde(default)]
    #[schemars(length(max = 256))]
    query: String,
    #[serde(default)]
    #[schemars(range(max = 1000))]
    page: u32,
    #[serde(default = "default_page_size")]
    #[schemars(range(min = 1, max = 20))]
    page_size: u32,
    #[serde(default)]
    fresh_only: bool,
}
fn default_page_size() -> u32 {
    5
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct GetInput {
    source: DemoSource,
    #[schemars(length(min = 1, max = 128), regex(pattern = r"^[A-Za-z0-9_-]+$"))]
    id: String,
    #[serde(default)]
    fresh_only: bool,
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct RecordReference {
    source: DemoSource,
    #[schemars(length(min = 1, max = 128), regex(pattern = r"^[A-Za-z0-9_-]+$"))]
    id: String,
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ShowInput {
    #[schemars(length(max = 20))]
    records: Vec<RecordReference>,
    #[serde(default)]
    fresh_only: bool,
}

#[derive(Serialize, JsonSchema)]
struct ShowOutput {
    records: Vec<RetrievalEnvelope<RetrievalData>>,
    synthetic: bool,
}

/// A compiled module; every operation uses the same application instance.
pub struct DemoTools {
    pub service: Arc<RetrievalService>,
}

impl ToolModule for DemoTools {
    fn register(self, registry: &mut ToolRegistry) -> Result<(), ServerError> {
        let search_service = self.service.clone();
        registry.register_typed::<SearchInput, RetrievalEnvelope<RetrievalData>, _, _>(
            "demo_search_records",
            "Search fictional demonstration records. Results are synthetic, not law. Use IDs with demo_get_record or demo_show_records.",
            ToolOptions::default(),
            move |input, context| {
                let service = search_service.clone();
                async move {
                    retrieve(&service, Query::Search {
                        source: input.source.id(), query: input.query,
                        page: input.page, page_size: input.page_size,
                    }, input.fresh_only, context).await.map(ToolOutput::new)
                }
            },
        )?;
        let detail_service = self.service.clone();
        registry.register_typed::<GetInput, RetrievalEnvelope<RetrievalData>, _, _>(
            "demo_get_record", "Read one fictional record by its source and exact ID; includes provenance and freshness, not legal evidence.",
            ToolOptions::default(),
            move |input, context| {
                let service = detail_service.clone();
                async move {
                    retrieve(&service, Query::Get { source: input.source.id(), id: input.id },
                        input.fresh_only, context).await.map(ToolOutput::new)
                }
            },
        )?;
        let options = ToolOptions {
            meta: Some(MetaObject(serde_json::from_value(serde_json::json!({
                "ui": {"resourceUri": WIDGET_URI},
            }))?)),
            ..ToolOptions::default()
        };
        registry.register_typed::<ShowInput, ShowOutput, _, _>(
            "demo_show_records", "Open the interactive synthetic record browser for up to 20 source/ID pairs returned by demo_search_records. An empty list opens an empty browser.",
            options,
            move |input, context| {
                let service = self.service.clone();
                async move {
                    let queries: Vec<_> = input.records.into_iter().map(|record| Query::Get {
                        source: record.source.id(), id: record.id,
                    }).collect();
                    // Validate all references before any source access.
                    for query in &queries { query.validate().map_err(map_error)?; }
                    context.progress.report(ProgressStage::CheckingCache).await?;
                    let mut records = Vec::with_capacity(queries.len());
                    for query in queries {
                        context.progress.report(ProgressStage::Fetching).await?;
                        let record = service.retrieve(query, requirement(input.fresh_only),
                            context.request.cancellation.clone(), None).await.map_err(map_error)?;
                        records.push(record);
                        // Bound aggregate construction, not only the final MCP serialization.
                        crate::registry::ensure_serialized_limit(&records, 64 * 1024)
                            .map_err(|_| ToolError::ResourceLimit)?;
                    }
                    context.progress.report(ProgressStage::Complete).await?;
                    Ok(ToolOutput::new(ShowOutput { records, synthetic: true }))
                }
            },
        )
    }
}

fn requirement(fresh_only: bool) -> FreshnessRequirement {
    if fresh_only {
        FreshnessRequirement::FreshOnly
    } else {
        FreshnessRequirement::AllowStale
    }
}

async fn retrieve(
    service: &Arc<RetrievalService>,
    query: Query,
    fresh_only: bool,
    context: ToolExecutionContext,
) -> Result<RetrievalEnvelope<RetrievalData>, ToolError> {
    query.validate().map_err(map_error)?;
    context
        .progress
        .report(ProgressStage::CheckingCache)
        .await?;
    let (progress, mut stages) = watch::channel(RetrievalStage::Accepted);
    let operation = service.retrieve(
        query,
        requirement(fresh_only),
        context.request.cancellation.clone(),
        Some(progress),
    );
    tokio::pin!(operation);
    let mut progress_open = true;
    loop {
        tokio::select! {
            biased;
            result = &mut operation => {
                let result = result.map_err(map_error)?;
                context.progress.report(ProgressStage::Complete).await?;
                return Ok(result);
            }
            changed = stages.changed(), if progress_open => {
                if changed.is_err() { progress_open = false; continue; }
                let stage = *stages.borrow_and_update();
                let stage = match stage {
                    RetrievalStage::Accepted => ProgressStage::CheckingCache,
                    RetrievalStage::Refreshing => ProgressStage::Fetching,
                    RetrievalStage::Validating => ProgressStage::Processing,
                    RetrievalStage::Complete => ProgressStage::Complete,
                };
                context.progress.report(stage).await?;
            }
        }
    }
}

fn map_error(error: RetrievalError) -> ToolError {
    match error {
        RetrievalError::InvalidInput | RetrievalError::UnknownSource => ToolError::InvalidInput,
        RetrievalError::NotFound => ToolError::NotFound,
        RetrievalError::Ambiguous => ToolError::Ambiguous,
        RetrievalError::Busy | RetrievalError::Throttled { .. } => ToolError::RateLimited,
        RetrievalError::FreshnessUnavailable => ToolError::FreshnessUnavailable,
        RetrievalError::NormalizationFailed | RetrievalError::InvalidPayload => {
            ToolError::NormalizationFailed
        }
        RetrievalError::ResourceLimit => ToolError::ResourceLimit,
        RetrievalError::Unavailable | RetrievalError::Cancelled | RetrievalError::Shutdown => {
            ToolError::Unavailable
        }
        RetrievalError::Internal => ToolError::Internal,
    }
}

/// Read an operator-selected local asset once, with a bound even if it grows while read.
pub async fn load_widget(
    path: &Path,
    source: &SourceOffer,
) -> Result<ResourceRegistry, ServerError> {
    let file = tokio::fs::File::open(path).await?;
    if !file.metadata().await?.is_file() {
        return Err("widget asset must be a regular file".into());
    }
    let mut bytes = Vec::new();
    file.take(1024 * 1024 + 1).read_to_end(&mut bytes).await?;
    if bytes.len() > 1024 * 1024 {
        return Err("widget asset exceeds 1 MiB".into());
    }
    widget_resources(String::from_utf8(bytes)?, source)
}

pub fn widget_resources(
    html: String,
    source: &SourceOffer,
) -> Result<ResourceRegistry, ServerError> {
    const MARKER: &str = "__OPENLEGAL_SOURCE_URL__";
    const METADATA: &str =
        "<meta name=\"openlegal-source-url\" content=\"__OPENLEGAL_SOURCE_URL__\">";
    if html.matches(MARKER).count() != 1 || html.matches(METADATA).count() != 1 {
        return Err("widget must contain exactly one source URL metadata placeholder".into());
    }
    let escaped = source
        .url()
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;");
    let html = html.replacen(MARKER, &escaped, 1);
    // Registration checks the expanded text and serialized resource; handler startup also
    // checks the result against the configured message limit before binding listeners.
    let mut resources = ResourceRegistry::new();
    let metadata = MetaObject(serde_json::from_value(serde_json::json!({
        "ui": {"prefersBorder": true, "csp": {
            "connectDomains": [], "resourceDomains": [], "frameDomains": []
        }}
    }))?);
    resources.register(
        Resource::new(WIDGET_URI, "Synthetic record browser")
            .with_mime_type(WIDGET_MIME)
            .with_meta(metadata.clone()),
        ResourceContents::text(html, WIDGET_URI)
            .with_mime_type(WIDGET_MIME)
            .with_meta(metadata),
    )?;
    Ok(resources)
}

#[cfg(test)]
mod widget_source_tests {
    use super::*;

    const META: &str = "<meta name=\"openlegal-source-url\" content=\"__OPENLEGAL_SOURCE_URL__\">";

    #[test]
    fn source_metadata_is_escaped_and_missing_duplicate_or_misplaced_markers_fail() {
        let source = SourceOffer::new("https://source.test/path?q=1&v=2#'quoted'").unwrap();
        let resources = widget_resources(format!("<html>{META}</html>"), &source).unwrap();
        let resource = &resources.resources[WIDGET_URI];
        let ResourceContents::TextResourceContents { text, .. } = &resource.result.contents[0]
        else {
            panic!("expected text")
        };
        assert!(text.contains("content=\"https://source.test/path?q=1&amp;v=2#&#39;quoted&#39;\""));
        assert!(!text.contains("__OPENLEGAL_SOURCE_URL__"));
        for html in [
            "<html>missing</html>".to_owned(),
            format!("{META}{META}"),
            "<script>__OPENLEGAL_SOURCE_URL__</script>".to_owned(),
            format!("{META}__OPENLEGAL_SOURCE_URL__"),
        ] {
            assert!(widget_resources(html, &source).is_err());
        }
    }

    #[test]
    fn expanded_widget_must_fit_raw_and_configured_serialized_limits() {
        let source =
            SourceOffer::new(&format!("https://source.test/?{}", "&".repeat(1800))).unwrap();
        let near_raw_limit = format!("{META}{}", "a".repeat(1024 * 1024 - META.len()));
        assert_eq!(near_raw_limit.len(), 1024 * 1024);
        assert!(widget_resources(near_raw_limit, &source).is_err());
        let small = SourceOffer::new("https://source.test/").unwrap();
        let html = format!("{META}<p>synthetic</p>");
        assert!(
            widget_resources(html.clone(), &small)
                .unwrap()
                .validate_limits(4096)
                .is_ok()
        );
        assert!(
            widget_resources(html, &source)
                .unwrap()
                .validate_limits(4096)
                .is_err()
        );
    }

    #[tokio::test]
    async fn local_widget_loading_applies_required_source_metadata() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("index.html");
        let source = SourceOffer::new("https://source.test/running").unwrap();
        tokio::fs::write(&path, "<html>outdated build</html>")
            .await
            .unwrap();
        assert!(load_widget(&path, &source).await.is_err());
        tokio::fs::write(&path, META).await.unwrap();
        assert!(load_widget(&path, &source).await.is_ok());
    }
}
