//! One MCP handler shared by every endpoint; no transport-specific tool dispatch.

use crate::{
    ServerError,
    config::{Limits, SourceOffer},
    progress::{ProgressReporter, validate_progress_token},
    registry::{
        ToolContext, ToolError, ToolExecutionContext, ToolRegistry, ensure_serialized_limit,
    },
    resources::ResourceRegistry,
};
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

#[derive(Default)]
pub struct Counters {
    pub calls: AtomicU64,
    pub failures: AtomicU64,
}

#[derive(Clone)]
pub struct McpHandler {
    registry: Arc<ToolRegistry>,
    source: SourceOffer,
    resources: Arc<ResourceRegistry>,
    limits: Arc<Limits>,
    calls: Arc<Semaphore>,
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
            limits.max_message_bytes / 8,
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
            calls: Arc::new(Semaphore::new(limits.max_in_flight)),
            limits,
            counters: Arc::new(Counters::default()),
        })
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
        info.capabilities = if self.resources.resources.is_empty() {
            ServerCapabilities::builder().enable_tools().build()
        } else {
            ServerCapabilities::builder()
                .enable_tools()
                .enable_resources()
                .build()
        };
        info.server_info =
            Implementation::new("openlegal4everyone.stream", env!("CARGO_PKG_VERSION"));
        info.instructions = Some(format!(
            "Public read-only server foundation. No legal-data provider is connected. \
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
        _: RequestContext<RoleServer>,
    ) -> Result<ReadResourceResponse, ErrorData> {
        let resource = self
            .resources
            .resources
            .get(&request.uri)
            .ok_or_else(|| ErrorData::resource_not_found("Resource was not found.", None))?;
        Ok(resource.result.clone().into())
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
        if !tool.validator.is_valid(&arguments) {
            return Err(ErrorData::invalid_params("invalid tool arguments", None));
        }
        let _permit = self
            .calls
            .clone()
            .try_acquire_owned()
            .map_err(|_| ErrorData::internal_error("server busy", None))?;
        self.counters.calls.fetch_add(1, Ordering::Relaxed);
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
            result_limit: self.limits.max_message_bytes / 8,
        };
        let result = tokio::select! {
            biased;
            _ = context.ct.cancelled() => Err(ErrorData::internal_error("request cancelled", None)),
            result = tokio::time::timeout_at(deadline, (tool.invoke)(arguments, execution)) => {
                match result {
                    Ok(Ok(value)) => Ok(value),
                    Ok(Err(ToolError::InvalidInput)) => Err(ErrorData::invalid_params("invalid tool arguments", None)),
                    Ok(Err(error)) => {
                        self.counters.failures.fetch_add(1, Ordering::Relaxed);
                        let (code, message) = match error {
                            ToolError::ProcessingPending => ("processing_pending", "An observed source replacement is awaiting processing."),
                            ToolError::UnsupportedHistory => ("unsupported_history", "Provider revision history is not supported for this dataset."),
                            ToolError::HistoryIncomplete => ("history_incomplete", "The revision inventory is insufficient for this selector."),
                            ToolError::SessionExpired => ("session_expired", "The retained query session has expired."),
                            ToolError::SnapshotInvalidated => ("snapshot_invalidated", "The retained query was invalidated by a source withdrawal."),
                            ToolError::Withdrawn => ("withdrawn", "The source object was withdrawn."),
                            ToolError::StorageUnavailable => ("storage_unavailable", "Persistent storage is temporarily unavailable."),
                            ToolError::StorageCorrupt => ("storage_corrupt", "Retained data failed integrity checks."),
                            ToolError::StorageCapacity => ("storage_capacity", "Persistent storage capacity is exhausted."),
                            ToolError::SnapshotUnavailable => ("snapshot_unavailable", "The exact snapshot is not retained."),
                            ToolError::NotFound => ("not_found", "Requested data was not found."),
                            ToolError::Unavailable => ("unavailable", "Service is temporarily unavailable."),
                            ToolError::RateLimited => ("rate_limited", "Request rate limit exceeded."),
                            ToolError::Ambiguous => ("ambiguous", "The requested data is ambiguous."),
                            ToolError::FreshnessUnavailable => ("freshness_unavailable", "Data meeting the freshness requirement is unavailable."),
                            ToolError::NormalizationFailed => ("normalization_failed", "Source data could not be processed."),
                            ToolError::ResourceLimit => ("resource_limit", "The operation exceeds its resource limit."),
                            _ => ("internal", "Tool execution failed."),
                        };
                        return Ok(CallToolResult::structured_error(serde_json::json!({"code":code,"message":message})).into());
                    },
                    Err(_) => { context.ct.cancel(); Err(ErrorData::internal_error("tool deadline exceeded", None)) }
                }
            }
        };
        match result {
            Ok(value) => {
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
                ensure_serialized_limit(&value.structured, self.limits.max_message_bytes / 8)
                    .map_err(|_| ErrorData::internal_error("tool result exceeds limit", None))?;
                let mut result = if let Some(text) = value.text {
                    let mut result = CallToolResult::success(vec![ContentBlock::text(text)]);
                    result.structured_content = Some(value.structured);
                    result
                } else {
                    CallToolResult::structured(value.structured)
                };
                result.meta = value.meta;
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
