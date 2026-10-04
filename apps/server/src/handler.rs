//! One MCP handler shared by every endpoint; no transport-specific tool dispatch.

use crate::{
    ServerError,
    config::{Limits, SourceOffer},
    progress::{ProgressReporter, validate_progress_token},
    rate_limit::RateLimiter,
    registry::{
        ToolContext, ToolError, ToolExecutionContext, ToolRegistry, ensure_serialized_limit,
    },
    resources::ResourceRegistry,
};
use openlegal_application::citation::CitationService;
use openlegal_domain::{citation::CitationSource, legal::DatabaseError};
use rmcp::{ErrorData, RoleServer, ServerHandler, model::*, service::RequestContext};
use std::{
    borrow::Cow,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

#[derive(Default)]
pub struct Counters {
    pub calls: AtomicU64,
    pub failures: AtomicU64,
    pub rate_limited: AtomicU64,
}

#[derive(Clone)]
pub struct McpHandler {
    registry: Arc<ToolRegistry>,
    source: SourceOffer,
    resources: Arc<ResourceRegistry>,
    citations: Option<Arc<CitationService>>,
    limits: Arc<Limits>,
    calls: Arc<Semaphore>,
    rate: Arc<RateLimiter>,
    verified_rate: Option<Arc<RateLimiter>>,
    verified_tunnel: bool,
    pub counters: Arc<Counters>,
}

impl McpHandler {
    pub fn new(
        registry: ToolRegistry,
        limits: Arc<Limits>,
        source: SourceOffer,
    ) -> Result<Self, ServerError> {
        Self::with_resources(registry, ResourceRegistry::new(), limits, source)
    }

    /// Construct one handler sharing an immutable tool and static resource registry.
    pub fn with_resources(
        registry: ToolRegistry,
        resources: ResourceRegistry,
        limits: Arc<Limits>,
        source: SourceOffer,
    ) -> Result<Self, ServerError> {
        limits.validate()?;
        ensure_serialized_limit(
            &crate::registry::server_info(&source),
            limits.tool_result_limit(),
        )
        .map_err(|_| "source offer exceeds configured tool result budget")?;
        resources.validate_limits(limits.max_message_bytes)?;
        for tool in registry.tools.values() {
            if let Some(meta) = &tool.definition.meta {
                for uri in [
                    meta.0.get("openai/outputTemplate"),
                    meta.0.get("ui").and_then(|ui| ui.get("resourceUri")),
                ]
                .into_iter()
                .flatten()
                {
                    let uri = uri
                        .as_str()
                        .ok_or("tool resource reference must be a URI string")?;
                    if !resources.resources.contains_key(uri) {
                        return Err("tool refers to an unregistered UI resource".into());
                    }
                }
            }
        }
        let tools: Vec<_> = registry
            .tools
            .values()
            .map(|tool| &tool.definition)
            .collect();
        ensure_serialized_limit(&tools, limits.max_message_bytes / 2)?;
        Ok(Self {
            registry: Arc::new(registry),
            source,
            resources: Arc::new(resources),
            citations: None,
            calls: Arc::new(Semaphore::new(limits.max_in_flight)),
            rate: Arc::new(RateLimiter::new(
                limits.rate_limit.enabled,
                limits.rate_limit.calls_per_second,
                limits.rate_limit.burst,
            )),
            verified_rate: limits
                .rate_limit
                .verified_tunnel
                .as_ref()
                .map(|config| Arc::new(RateLimiter::verified_tunnel(config))),
            verified_tunnel: false,
            limits,
            counters: Arc::new(Counters::default()),
        })
    }

    /// Select the shared alternate bucket only after the transport verifies edge mTLS.
    pub(crate) fn for_verified_tunnel(&self) -> Self {
        let mut handler = self.clone();
        handler.verified_tunnel = true;
        handler
    }

    /// Add the optional corpus resolver before endpoint binding.
    pub fn with_citations(
        mut self,
        citations: Option<Arc<CitationService>>,
    ) -> Result<Self, ServerError> {
        if citations.is_some() {
            Arc::get_mut(&mut self.registry)
                .ok_or("tool registry is already shared")?
                .enable_citation_references()?;
            let definitions: Vec<_> = self
                .registry
                .tools
                .values()
                .map(|tool| &tool.definition)
                .collect();
            ensure_serialized_limit(&definitions, self.limits.max_message_bytes / 2)?;
            ensure_serialized_limit(
                &crate::citation::resource_template(),
                self.limits.max_message_bytes / 2,
            )?;
        }
        self.citations = citations;
        Ok(self)
    }

    pub fn citation_service(&self) -> Option<Arc<CitationService>> {
        self.citations.clone()
    }

    pub(crate) fn message_limit(&self) -> usize {
        self.limits.max_message_bytes
    }

    /// Source pages and dynamic resources share tool admission and rate buckets.
    pub async fn citation_source(
        &self,
        id: &str,
        cancel: CancellationToken,
    ) -> Result<CitationSource, DatabaseError> {
        self.citation_lookup(id, cancel, false).await
    }

    /// Human pages can navigate a document while metadata resources stay metadata-only.
    pub async fn citation_page(
        &self,
        id: &str,
        cancel: CancellationToken,
    ) -> Result<CitationSource, DatabaseError> {
        self.citation_lookup(id, cancel, true).await
    }

    pub async fn citation_original(
        &self,
        capture_id: &str,
        ordinal: u32,
        cancel: CancellationToken,
    ) -> Result<openlegal_domain::rights::OriginalEvidence, DatabaseError> {
        let service = self.citations.as_ref().ok_or(DatabaseError::NotFound)?;
        let _permit = self
            .calls
            .clone()
            .try_acquire_owned()
            .map_err(|_| DatabaseError::Capacity)?;
        self.counters.calls.fetch_add(1, Ordering::Relaxed);
        let rate = if self.verified_tunnel {
            self.verified_rate.as_ref().unwrap_or(&self.rate)
        } else {
            &self.rate
        };
        if !rate.try_admit().await {
            self.counters.failures.fetch_add(1, Ordering::Relaxed);
            self.counters.rate_limited.fetch_add(1, Ordering::Relaxed);
            return Err(DatabaseError::Capacity);
        }
        let cancel = cancel.child_token();
        let _guard = cancel.clone().drop_guard();
        let lookup = service.original_evidence(capture_id, ordinal, cancel.clone());
        let result = tokio::select! {
            biased;
            _ = cancel.cancelled() => Err(DatabaseError::Cancelled),
            result = tokio::time::timeout(Duration::from_secs(self.limits.call_timeout_secs), lookup) => {
                result.unwrap_or(Err(DatabaseError::Capacity))
            }
        };
        if result.is_err() {
            self.counters.failures.fetch_add(1, Ordering::Relaxed);
        }
        result
    }

    async fn citation_lookup(
        &self,
        id: &str,
        cancel: CancellationToken,
        page: bool,
    ) -> Result<CitationSource, DatabaseError> {
        let service = self.citations.as_ref().ok_or(DatabaseError::NotFound)?;
        let _permit = self
            .calls
            .clone()
            .try_acquire_owned()
            .map_err(|_| DatabaseError::Capacity)?;
        self.counters.calls.fetch_add(1, Ordering::Relaxed);
        let rate = if self.verified_tunnel {
            self.verified_rate.as_ref().unwrap_or(&self.rate)
        } else {
            &self.rate
        };
        if !rate.try_admit().await {
            self.counters.failures.fetch_add(1, Ordering::Relaxed);
            self.counters.rate_limited.fetch_add(1, Ordering::Relaxed);
            return Err(DatabaseError::Capacity);
        }
        let cancel = cancel.child_token();
        let _guard = cancel.clone().drop_guard();
        let lookup = async {
            if page {
                service.page(id, cancel.clone()).await
            } else {
                service.source(id, cancel.clone()).await
            }
        };
        let result = tokio::select! {
            biased;
            _ = cancel.cancelled() => Err(DatabaseError::Cancelled),
            result = tokio::time::timeout(Duration::from_secs(self.limits.call_timeout_secs), lookup) => {
                result.unwrap_or(Err(DatabaseError::Capacity))
            }
        };
        if result.is_err() {
            self.counters.failures.fetch_add(1, Ordering::Relaxed);
        }
        result
    }
}

impl ServerHandler for McpHandler {
    fn supported_protocol_versions(&self) -> Cow<'static, [ProtocolVersion]> {
        Cow::Owned(vec![
            ProtocolVersion::V_2026_07_28,
            ProtocolVersion::V_2025_11_25,
        ])
    }

    fn get_info(&self) -> ServerInfo {
        let mut info = ServerInfo::default();
        info.capabilities = if self.resources.resources.is_empty() && self.citations.is_none() {
            ServerCapabilities::builder().enable_tools().build()
        } else {
            ServerCapabilities::builder()
                .enable_tools()
                .enable_resources()
                .build()
        };
        info.server_info =
            Implementation::new("openlegal4everyone.stream", env!("CARGO_PKG_VERSION"));
        let corpus = if self.citations.is_some() {
            "Retained legal source citations identify exact captures and bounded projections. Citation URLs do not promise permanent evidence retention or legal applicability."
        } else {
            "Public read-only server foundation."
        };
        info.instructions = Some(format!(
            "{corpus} \
             Licensed under {} ({}). Corresponding source for this running server and widget: {}",
            SourceOffer::LICENSE,
            SourceOffer::LICENSE_URL,
            self.source.url()
        ));
        info
    }

    fn get_tool(&self, name: &str) -> Option<Tool> {
        self.registry
            .tools
            .get(name)
            .map(|tool| tool.definition.clone())
    }

    async fn list_tools(
        &self,
        request: Option<PaginatedRequestParams>,
        _: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, ErrorData> {
        if request.and_then(|request| request.cursor).is_some() {
            return Err(ErrorData::invalid_params("unsupported cursor", None));
        }
        Ok(ListToolsResult {
            tools: self
                .registry
                .tools
                .values()
                .map(|tool| tool.definition.clone())
                .collect(),
            ..Default::default()
        })
    }

    async fn list_resources(
        &self,
        request: Option<PaginatedRequestParams>,
        _: RequestContext<RoleServer>,
    ) -> Result<ListResourcesResult, ErrorData> {
        if request.and_then(|request| request.cursor).is_some() {
            return Err(ErrorData::invalid_params("unsupported cursor", None));
        }
        Ok(ListResourcesResult {
            resources: self
                .resources
                .resources
                .values()
                .map(|resource| resource.definition.clone())
                .collect(),
            ..Default::default()
        })
    }

    async fn read_resource(
        &self,
        request: ReadResourceRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<ReadResourceResponse, ErrorData> {
        if let Some(resource) = self.resources.resources.get(&request.uri) {
            return Ok(resource.result.clone().into());
        }
        let id = crate::citation::id_from_resource_uri(&request.uri)?;
        let source = self
            .citation_source(id, context.ct.clone())
            .await
            .map_err(crate::citation::resource_error)?;
        let result = crate::citation::resource_result(&request.uri, source)?;
        ensure_serialized_limit(&result, self.limits.tool_result_limit())
            .map_err(|_| ErrorData::internal_error("Resource exceeds its result limit.", None))?;
        ensure_serialized_limit(&result, self.limits.max_message_bytes / 2)
            .map_err(|_| ErrorData::internal_error("Resource exceeds its message limit.", None))?;
        Ok(result.into())
    }

    async fn list_resource_templates(
        &self,
        request: Option<PaginatedRequestParams>,
        _: RequestContext<RoleServer>,
    ) -> Result<ListResourceTemplatesResult, ErrorData> {
        if request.and_then(|request| request.cursor).is_some() {
            return Err(ErrorData::invalid_params("unsupported cursor", None));
        }
        let mut result = ListResourceTemplatesResult::default();
        if self.citations.is_some() {
            result
                .resource_templates
                .push(crate::citation::resource_template());
        }
        ensure_serialized_limit(&result, self.limits.max_message_bytes / 2)
            .map_err(|_| ErrorData::internal_error("Resource templates exceed limit.", None))?;
        Ok(result)
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResponse, ErrorData> {
        let progress_token = context.meta.get_progress_token();
        if context.meta.contains_key("progressToken") && progress_token.is_none() {
            return Err(ErrorData::invalid_params("invalid progress token", None));
        }
        if progress_token
            .as_ref()
            .is_some_and(|token| !validate_progress_token(token))
        {
            return Err(ErrorData::invalid_params(
                "progress token exceeds limit",
                None,
            ));
        }
        let tool = self
            .registry
            .tools
            .get(request.name.as_ref())
            .ok_or_else(|| ErrorData::invalid_params("unknown tool", None))?;
        let arguments = serde_json::Value::Object(request.arguments.unwrap_or_default());
        let citation_object = arguments.get("object").cloned();
        if !tool.validator.is_valid(&arguments) {
            return Err(ErrorData::invalid_params("invalid tool arguments", None));
        }
        let _permit = self
            .calls
            .clone()
            .try_acquire_owned()
            .map_err(|_| ErrorData::internal_error("server busy", None))?;
        self.counters.calls.fetch_add(1, Ordering::Relaxed);
        let rate = if self.verified_tunnel {
            self.verified_rate.as_ref().unwrap_or(&self.rate)
        } else {
            &self.rate
        };
        if !rate.try_admit().await {
            self.counters.failures.fetch_add(1, Ordering::Relaxed);
            self.counters.rate_limited.fetch_add(1, Ordering::Relaxed);
            return Ok(map_tool_error(ToolError::RateLimited)?.into());
        }
        let deadline =
            tokio::time::Instant::now() + Duration::from_secs(self.limits.call_timeout_secs);
        let progress = ProgressReporter::new(
            context.peer.clone(),
            progress_token,
            context.ct.clone(),
            deadline,
            self.limits.max_message_bytes / 4,
        );
        let _progress_guard = progress.close_guard();
        let execution = ToolExecutionContext {
            request: ToolContext {
                cancellation: context.ct.clone(),
            },
            deadline,
            progress,
            result_limit: self.limits.tool_result_limit(),
        };
        let result = tokio::select! {
            biased;
            _ = context.ct.cancelled() => Err(ErrorData::internal_error("request cancelled", None)),
            result = tokio::time::timeout_at(deadline, (tool.invoke)(arguments, execution)) => {
                match result {
                    Ok(Ok(value)) => Ok(value),
                    Ok(Err(error)) => match map_tool_error(error) {
                        Ok(result) => {
                            self.counters.failures.fetch_add(1, Ordering::Relaxed);
                            return Ok(result.into());
                        }
                        Err(error) => Err(error),
                    },
                    Err(_) => { context.ct.cancel(); Err(ErrorData::internal_error("tool deadline exceeded", None)) }
                }
            }
        };
        match result {
            Ok(mut value) => {
                if let Some(service) = &self.citations {
                    crate::citation::append_native_references(
                        request.name.as_ref(),
                        service,
                        citation_object.as_ref(),
                        &mut value,
                    );
                }
                if tool
                    .output_validator
                    .as_ref()
                    .is_some_and(|validator| !validator.is_valid(&value.structured))
                {
                    self.counters.failures.fetch_add(1, Ordering::Relaxed);
                    return Ok(CallToolResult::structured_error(
                        serde_json::json!({"code":"internal","message":"Tool execution failed."}),
                    )
                    .into());
                }
                if ensure_serialized_limit(&value.structured, self.limits.tool_result_limit())
                    .is_err()
                {
                    self.counters.failures.fetch_add(1, Ordering::Relaxed);
                    return Ok(map_tool_error(ToolError::ResourceLimit)?.into());
                }
                let mut result = if let Some(text) = value.text {
                    let mut result = CallToolResult::success(vec![ContentBlock::text(text)]);
                    result.structured_content = Some(value.structured);
                    result
                } else {
                    CallToolResult::structured(value.structured)
                };
                result.meta = value.meta;
                result.content.extend(value.additional_content);
                if value.strict_result_limit
                    && ensure_serialized_limit(&result, self.limits.tool_result_limit()).is_err()
                {
                    self.counters.failures.fetch_add(1, Ordering::Relaxed);
                    return Ok(map_tool_error(ToolError::ResourceLimit)?.into());
                }
                // Leave room for the JSON-RPC envelope, SDK metadata and SSE framing.
                ensure_serialized_limit(&result, self.limits.max_message_bytes / 2)
                    .map_err(|_| ErrorData::internal_error("tool result exceeds limit", None))?;
                Ok(result.into())
            }
            Err(error) => {
                self.counters.failures.fetch_add(1, Ordering::Relaxed);
                Err(error)
            }
        }
    }
}

fn map_tool_error(error: ToolError) -> Result<CallToolResult, ErrorData> {
    let (code, message) = match error {
        ToolError::CollectionUnavailable { cause } => {
            use crate::registry::CollectionReadFailure;
            let original = match cause {
                CollectionReadFailure::NotObserved => ToolError::NotObserved,
                CollectionReadFailure::ProcessingPending => ToolError::ProcessingPending,
                CollectionReadFailure::CollectionIncomplete => ToolError::CollectionIncomplete,
                CollectionReadFailure::FreshnessUnavailable => ToolError::FreshnessUnavailable,
            };
            let mut result = map_tool_error(original)?;
            if let Some(structured) = result.structured_content.as_mut() {
                structured["collection"] =
                    serde_json::json!({"status":"unavailable", "reason":"collection_unavailable"});
            }
            return Ok(result);
        }
        ToolError::InvalidInput => {
            return Err(ErrorData::invalid_params("invalid tool arguments", None));
        }
        ToolError::InvalidFieldShorthand => {
            return Err(ErrorData::invalid_params(
                "use in:title:, in:body:, or in:case_number: for field search",
                None,
            ));
        }
        ToolError::UnsupportedJurisdiction => {
            return Ok(CallToolResult::structured_error(serde_json::json!({
                "code": "unsupported_jurisdiction",
                "message": "No reference profile is implemented for this ISO 3166-1 alpha-3 jurisdiction.",
                "supported": openlegal_domain::jurisdiction::Jurisdiction::supported_codes()
            })));
        }
        ToolError::InvalidTimezone => {
            return Err(ErrorData::invalid_params(
                "timezone must be an IANA time zone name such as Asia/Seoul or America/New_York",
                None,
            ));
        }
        ToolError::InvalidUtf8Boundary { offset } => {
            return Ok(CallToolResult::structured_error(serde_json::json!({
                "code": "invalid_utf8_boundary",
                "message": "The byte offset is not on a UTF-8 character boundary.",
                "offset": offset
            })));
        }
        ToolError::ProcessingPending => (
            "processing_pending",
            "An observed source object is awaiting processing.",
        ),
        ToolError::NotObserved => (
            "not_observed",
            "This object has not been observed in the local corpus; upstream absence is unknown.",
        ),
        ToolError::CollectionIncomplete => (
            "collection_incomplete",
            "This observed object has no publishable current representation.",
        ),
        ToolError::SourceInventoryIncomplete => (
            "source_inventory_incomplete",
            "The bounded provider inventory did not establish a complete result.",
        ),
        ToolError::UnsupportedHistory => (
            "unsupported_history",
            "Provider revision history is not supported for this dataset.",
        ),
        ToolError::HistoryIncomplete => (
            "history_incomplete",
            "The revision inventory is insufficient for this selector.",
        ),
        ToolError::SessionExpired => ("session_expired", "The retained query session has expired."),
        ToolError::SnapshotInvalidated => (
            "snapshot_invalidated",
            "The retained query was invalidated by a source withdrawal.",
        ),
        ToolError::Withdrawn => ("withdrawn", "The source object was withdrawn."),
        ToolError::StorageUnavailable => (
            "storage_unavailable",
            "Persistent storage is temporarily unavailable.",
        ),
        ToolError::StorageCorrupt => ("storage_corrupt", "Retained data failed integrity checks."),
        ToolError::StorageCapacity => (
            "storage_capacity",
            "Persistent storage capacity is exhausted.",
        ),
        ToolError::SnapshotUnavailable => (
            "snapshot_unavailable",
            "The exact snapshot is not retained.",
        ),
        ToolError::NotFound => ("not_found", "Requested data was not found."),
        ToolError::InvalidRegex => ("invalid_regex", "The regular expression is invalid."),
        ToolError::PatchConflict => ("patch_conflict", "Patch context did not match the target."),
        ToolError::AttachmentKindMismatch => (
            "attachment_kind_mismatch",
            "Attachment kind is incompatible with this operation.",
        ),
        ToolError::Unavailable => ("unavailable", "Service is temporarily unavailable."),
        ToolError::RateLimited => ("rate_limited", "Request rate limit exceeded."),
        ToolError::Ambiguous => ("ambiguous", "The requested data is ambiguous."),
        ToolError::FreshnessUnavailable => (
            "freshness_unavailable",
            "Data meeting the freshness requirement is unavailable.",
        ),
        ToolError::NormalizationFailed => (
            "normalization_failed",
            "Source data could not be processed.",
        ),
        ToolError::ResourceLimit => (
            "resource_limit",
            "The operation exceeds its resource limit.",
        ),
        ToolError::Internal => ("internal", "Tool execution failed."),
    };
    Ok(CallToolResult::structured_error(serde_json::json!({
        "code": code,
        "message": message
    })))
}

#[cfg(test)]
mod source_tests {
    use super::*;

    #[test]
    fn source_offer_must_fit_the_configured_tool_result_budget_before_binding() {
        let source =
            SourceOffer::new(&format!("https://source.test/{}", "a".repeat(1900))).unwrap();
        let registry = crate::registry::server_info_registry(source.clone()).unwrap();
        let limits = Arc::new(Limits {
            max_message_bytes: 4096,
            ..Default::default()
        });
        assert!(McpHandler::new(registry, limits, source.clone()).is_err());
        let registry = crate::registry::server_info_registry(source.clone()).unwrap();
        assert!(McpHandler::new(registry, Arc::new(Limits::default()), source).is_ok());
    }
}
