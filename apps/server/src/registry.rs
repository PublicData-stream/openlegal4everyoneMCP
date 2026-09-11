//! Startup-only registration of trusted, read-only tool implementations.

use crate::ServerError;
use futures::{FutureExt, future::BoxFuture};
use rmcp::model::{Tool, ToolAnnotations};
use schemars::JsonSchema;
use serde::de::DeserializeOwned;
use serde_json::Value;
use std::{collections::BTreeMap, future::Future, sync::Arc};
use tokio_util::sync::CancellationToken;

/// Cancellation is cooperative. Tools must not detach untracked work.
#[derive(Clone)]
pub struct ToolContext {
    pub cancellation: CancellationToken,
}

/// Public, sanitized failures. Keep provider errors and diagnostics inside the module.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ToolError {
    InvalidInput,
    NotFound,
    Unavailable,
    RateLimited,
    Internal,
}

/// Implemented by Rust modules compiled into the host; registration performs no network I/O.
pub trait ToolModule {
    fn register(self, registry: &mut ToolRegistry) -> Result<(), ServerError>;
}

type Invoke =
    dyn Fn(Value, ToolContext) -> BoxFuture<'static, Result<Value, ToolError>> + Send + Sync;

pub(crate) struct RegisteredTool {
    pub definition: Tool,
    pub validator: jsonschema::Validator,
    pub invoke: Arc<Invoke>,
}

#[derive(Default)]
pub struct ToolRegistry {
    pub(crate) tools: BTreeMap<String, RegisteredTool>,
}

impl ToolRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a typed, read-only handler. Input is schema-validated before deserialization.
    /// Any external state access must go through explicitly supplied application services.
    pub fn register<I, F, Fut>(
        &mut self,
        name: &str,
        description: &str,
        handler: F,
    ) -> Result<(), ServerError>
    where
        I: DeserializeOwned + JsonSchema + Send + 'static,
        F: Fn(I, ToolContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Value, ToolError>> + Send + 'static,
    {
        self.register_with_annotations::<I, F, Fut>(
            name,
            description,
            ToolAnnotations::from_raw(None, Some(true), Some(false), Some(true), Some(true)),
            handler,
        )
    }

    /// Supply accurate annotations; this anonymous foundation accepts only read-only tools.
    pub fn register_with_annotations<I, F, Fut>(
        &mut self,
        name: &str,
        description: &str,
        annotations: ToolAnnotations,
        handler: F,
    ) -> Result<(), ServerError>
    where
        I: DeserializeOwned + JsonSchema + Send + 'static,
        F: Fn(I, ToolContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Value, ToolError>> + Send + 'static,
    {
        if annotations.read_only_hint != Some(true) {
            return Err("anonymous tools must be read-only".into());
        }
        if name.is_empty()
            || name.len() > 64
            || !name
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"_.-".contains(&b))
        {
            return Err("tool names must contain 1-64 ASCII letters, digits, _, . or -".into());
        }
        if self.tools.contains_key(name) {
            return Err(format!("duplicate tool: {name}").into());
        }
        if self.tools.len() >= 128 || description.is_empty() || description.len() > 4096 {
            return Err("tool registry or description exceeds its limit".into());
        }
        let schema = serde_json::to_value(schemars::schema_for!(I))?;
        if schema.get("type").and_then(Value::as_str) != Some("object") {
            return Err("MCP tool input must be an object".into());
        }
        ensure_serialized_limit(&schema, 64 * 1024)?;
        // Default features disable HTTP/file reference retrieval. Unresolved references fail startup.
        let validator =
            jsonschema::validator_for(&schema).map_err(|_| "invalid or unresolved tool schema")?;
        let object = schema
            .as_object()
            .ok_or("input schema must be an object")?
            .clone();
        let definition = Tool::new(name.to_owned(), description.to_owned(), Arc::new(object))
            .with_annotations(annotations);
        let handler = Arc::new(handler);
        let invoke = Arc::new(move |value: Value, context: ToolContext| {
            let handler = handler.clone();
            async move {
                let input = serde_json::from_value(value).map_err(|_| ToolError::InvalidInput)?;
                handler(input, context).await
            }
            .boxed()
        });
        self.tools.insert(
            name.to_owned(),
            RegisteredTool {
                definition,
                validator,
                invoke,
            },
        );
        Ok(())
    }

    pub fn register_module(&mut self, module: impl ToolModule) -> Result<(), ServerError> {
        module.register(self)
    }
}

/// Count serialized bytes without allocating a second, unbounded copy of a result.
pub(crate) fn ensure_serialized_limit(
    value: &impl serde::Serialize,
    limit: usize,
) -> Result<(), ServerError> {
    struct Counter {
        remaining: usize,
    }
    impl std::io::Write for Counter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if bytes.len() > self.remaining {
                return Err(std::io::Error::other("serialized value exceeds limit"));
            }
            self.remaining -= bytes.len();
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    serde_json::to_writer(Counter { remaining: limit }, value)?;
    Ok(())
}

#[derive(serde::Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct EmptyInput {}

pub fn server_info_registry() -> Result<ToolRegistry, ServerError> {
    let mut registry = ToolRegistry::new();
    registry.register::<EmptyInput, _, _>("server_info", "Read server identity and supported protocols; does not retrieve legal data.", |_, _| async {
        Ok(serde_json::json!({"product":"openlegal4everyone.stream", "version":env!("CARGO_PKG_VERSION"),
            "transports":["streamable-http", "webtransport-v1"], "protocolVersions":["2026-07-28", "2025-11-25"]}))
    })?;
    Ok(registry)
}
