//! One MCP handler shared by every endpoint; no transport-specific tool dispatch.

use crate::{
    ServerError,
    config::Limits,
    registry::{ToolContext, ToolError, ToolRegistry, ensure_serialized_limit},
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
    limits: Arc<Limits>,
    calls: Arc<Semaphore>,
    pub counters: Arc<Counters>,
}

impl McpHandler {
    pub fn new(registry: ToolRegistry, limits: Arc<Limits>) -> Result<Self, ServerError> {
        limits.validate()?;
        let tools: Vec<_> = registry
            .tools
            .values()
            .map(|tool| &tool.definition)
            .collect();
        ensure_serialized_limit(&tools, limits.max_message_bytes / 2)?;
        Ok(Self {
            registry: Arc::new(registry),
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
        info.capabilities = ServerCapabilities::builder().enable_tools().build();
        info.server_info =
            Implementation::new("openlegal4everyone.stream", env!("CARGO_PKG_VERSION"));
        info.instructions =
            Some("Public read-only server foundation. No legal-data provider is connected.".into());
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

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResponse, ErrorData> {
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
        let result = tokio::select! {
            biased;
            _ = context.ct.cancelled() => Err(ErrorData::internal_error("request cancelled", None)),
            result = tokio::time::timeout(Duration::from_secs(self.limits.call_timeout_secs),
                (tool.invoke)(arguments, ToolContext { cancellation: context.ct.clone() })) => {
                match result {
                    Ok(Ok(value)) => Ok(value),
                    Ok(Err(ToolError::InvalidInput)) => Err(ErrorData::invalid_params("invalid tool arguments", None)),
                    Ok(Err(error)) => {
                        self.counters.failures.fetch_add(1, Ordering::Relaxed);
                        let (code, message) = match error {
                            ToolError::NotFound => ("not_found", "Requested data was not found."),
                            ToolError::Unavailable => ("unavailable", "Service is temporarily unavailable."),
                            ToolError::RateLimited => ("rate_limited", "Request rate limit exceeded."),
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
                ensure_serialized_limit(&value, self.limits.max_message_bytes / 8)
                    .map_err(|_| ErrorData::internal_error("tool result exceeds limit", None))?;
                let result = CallToolResult::structured(value);
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
