//! Startup-only registration of trusted, read-only tool implementations.

use crate::{ServerError, progress::ProgressReporter};
use futures::{FutureExt, future::BoxFuture};
use rmcp::model::{MetaObject, Tool, ToolAnnotations};
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
    ProcessingPending,
    UnsupportedHistory,
    HistoryIncomplete,
    SessionExpired,
    SnapshotInvalidated,
    Withdrawn,
    StorageUnavailable,
    StorageCorrupt,
    StorageCapacity,
    SnapshotUnavailable,
    InvalidInput,
    NotFound,
    Unavailable,
    RateLimited,
    Ambiguous,
    FreshnessUnavailable,
    NormalizationFailed,
    ResourceLimit,
    Internal,
}

/// Implemented by Rust modules compiled into the host; registration performs no network I/O.
pub trait ToolModule {
    fn register(self, registry: &mut ToolRegistry) -> Result<(), ServerError>;
}

/// Rich output for typed tools. Result metadata reaches clients and must contain no secrets.
#[derive(serde::Serialize)]
pub struct ToolOutput<T> {
    pub structured: T,
    pub text: Option<String>,
    pub meta: Option<MetaObject>,
}
impl<T> ToolOutput<T> {
    pub fn new(structured: T) -> Self {
        Self {
            structured,
            text: None,
            meta: None,
        }
    }
}

/// Descriptor annotations and static client metadata for a typed read-only tool.
pub struct ToolOptions {
    pub annotations: ToolAnnotations,
    pub meta: Option<MetaObject>,
}
impl Default for ToolOptions {
    fn default() -> Self {
        Self {
            annotations: ToolAnnotations::from_raw(
                None,
                Some(true),
                Some(false),
                Some(true),
                Some(true),
            ),
            meta: None,
        }
    }
}

/// Extended context for typed tools; the original ToolContext remains source compatible.
#[derive(Clone)]
pub struct ToolExecutionContext {
    pub request: ToolContext,
    pub deadline: tokio::time::Instant,
    pub progress: ProgressReporter,
    pub(crate) result_limit: usize,
}

pub(crate) struct InvocationOutput {
    pub structured: Value,
    pub text: Option<String>,
    pub meta: Option<MetaObject>,
}

type Invoke = dyn Fn(Value, ToolExecutionContext) -> BoxFuture<'static, Result<InvocationOutput, ToolError>>
    + Send
    + Sync;

pub(crate) struct RegisteredTool {
    pub definition: Tool,
    pub validator: jsonschema::Validator,
    pub output_validator: Option<jsonschema::Validator>,
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
        self.register_internal(name, description, annotations, handler, false)
    }

    fn register_internal<I, F, Fut>(
        &mut self,
        name: &str,
        description: &str,
        annotations: ToolAnnotations,
        handler: F,
        ephemeral_delete: bool,
    ) -> Result<(), ServerError>
    where
        I: DeserializeOwned + JsonSchema + Send + 'static,
        F: Fn(I, ToolContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Value, ToolError>> + Send + 'static,
    {
        if annotations.read_only_hint != Some(true)
            && !(ephemeral_delete
                && matches!(
                    name,
                    "delete_text_diff"
                        | "text.diff.delete"
                        | "text.attachment.upload"
                        | "text.attachment.delete"
                ))
        {
            return Err("anonymous extension tools must be read-only".into());
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
        let invoke = Arc::new(move |value: Value, context: ToolExecutionContext| {
            let handler = handler.clone();
            async move {
                let input = serde_json::from_value(value).map_err(|_| ToolError::InvalidInput)?;
                handler(input, context.request)
                    .await
                    .map(|structured| InvocationOutput {
                        structured,
                        text: None,
                        meta: None,
                    })
            }
            .boxed()
        });
        self.tools.insert(
            name.to_owned(),
            RegisteredTool {
                definition,
                validator,
                output_validator: None,
                invoke,
            },
        );
        Ok(())
    }

    /// Register a tool with a runtime-enforced object output schema and optional UI metadata.
    pub fn register_typed<I, O, F, Fut>(
        &mut self,
        name: &str,
        description: &str,
        options: ToolOptions,
        handler: F,
    ) -> Result<(), ServerError>
    where
        I: DeserializeOwned + JsonSchema + Send + 'static,
        O: serde::Serialize + JsonSchema + Send + 'static,
        F: Fn(I, ToolExecutionContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<ToolOutput<O>, ToolError>> + Send + 'static,
    {
        self.register_typed_internal(name, description, options, handler, false)
    }

    /// The sole mutation exception: deletion of a bearer-authorized transient comparison.
    /// Kept crate-private so ordinary extension registration cannot opt into writes.
    pub(crate) fn register_text_diff_delete<I, O, F, Fut>(
        &mut self,
        handler: F,
    ) -> Result<(), ServerError>
    where
        I: DeserializeOwned + JsonSchema + Send + 'static,
        O: serde::Serialize + JsonSchema + Send + 'static,
        F: Fn(I, ToolExecutionContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<ToolOutput<O>, ToolError>> + Send + 'static,
    {
        self.register_typed_internal(
            "delete_text_diff",
            "Delete a temporary comparison using its bearer handle. This also removes access for anyone sharing the handle. Repeated deletion succeeds.",
            ToolOptions {
                annotations: ToolAnnotations::from_raw(None, Some(false), Some(true), Some(true), Some(false)),
                meta: None,
            },
            handler,
            true,
        )
    }

    /// Only the enumerated built-in transient operations may mutate anonymous state.
    /// This is not an extension-facing permission to register arbitrary writes.
    pub(crate) fn register_attachment_builtin<I, O, F, Fut>(
        &mut self,
        name: &str,
        description: &str,
        options: ToolOptions,
        handler: F,
    ) -> Result<(), ServerError>
    where
        I: DeserializeOwned + JsonSchema + Send + 'static,
        O: serde::Serialize + JsonSchema + Send + 'static,
        F: Fn(I, ToolExecutionContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<ToolOutput<O>, ToolError>> + Send + 'static,
    {
        if !matches!(
            name,
            "text.attachment.upload" | "text.attachment.delete" | "text.diff.delete"
        ) {
            return Err("unsupported built-in transient operation".into());
        }
        self.register_typed_internal(name, description, options, handler, true)
    }

    fn register_typed_internal<I, O, F, Fut>(
        &mut self,
        name: &str,
        description: &str,
        options: ToolOptions,
        handler: F,
        ephemeral_delete: bool,
    ) -> Result<(), ServerError>
    where
        I: DeserializeOwned + JsonSchema + Send + 'static,
        O: serde::Serialize + JsonSchema + Send + 'static,
        F: Fn(I, ToolExecutionContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<ToolOutput<O>, ToolError>> + Send + 'static,
    {
        // Validate every fallible addition before registration so failed startup registration
        // does not leave a partially configured tool behind.
        let schema = serde_json::to_value(schemars::schema_for!(O))?;
        if schema.get("type").and_then(Value::as_str) != Some("object") {
            return Err("MCP tool output must be an object".into());
        }
        ensure_serialized_limit(&schema, 64 * 1024)?;
        ensure_serialized_limit(&options.meta, 16 * 1024)?;
        let output_validator = jsonschema::validator_for(&schema)
            .map_err(|_| "invalid or unresolved tool output schema")?;
        let object = schema
            .as_object()
            .ok_or("output schema must be an object")?
            .clone();
        let handler = Arc::new(handler);
        let invoke = Arc::new(move |value: Value, context: ToolExecutionContext| {
            let handler = handler.clone();
            async move {
                let input = serde_json::from_value(value).map_err(|_| ToolError::InvalidInput)?;
                let limit = context.result_limit;
                let output: ToolOutput<O> = handler(input, context).await?;
                ensure_serialized_limit(&output, limit).map_err(|_| ToolError::ResourceLimit)?;
                Ok(InvocationOutput {
                    structured: serde_json::to_value(output.structured)
                        .map_err(|_| ToolError::Internal)?,
                    text: output.text,
                    meta: output.meta,
                })
            }
            .boxed()
        });
        self.register_internal::<I, _, _>(
            name,
            description,
            options.annotations,
            |_, _| async { Err(ToolError::Internal) },
            ephemeral_delete,
        )?;
        let tool = self
            .tools
            .get_mut(name)
            .ok_or("registered tool disappeared")?;
        tool.definition = tool
            .definition
            .clone()
            .with_raw_output_schema(Arc::new(object));
        if let Some(meta) = options.meta {
            tool.definition = tool.definition.clone().with_meta(meta);
        }
        tool.output_validator = Some(output_validator);
        tool.invoke = invoke;
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

/// Register identity and the operator-configured corresponding-source offer.
pub fn server_info_registry(
    source: crate::config::SourceOffer,
) -> Result<ToolRegistry, ServerError> {
    let mut registry = ToolRegistry::new();
    registry.register::<EmptyInput, _, _>("server_info", "Read server identity, license, corresponding source and supported protocols; does not retrieve legal data.", move |_, _| {
        let source = source.clone();
        async move {
            Ok(server_info(&source))
        }
    })?;
    Ok(registry)
}

/// Canonical source metadata also used to verify configured response budgets at startup.
pub(crate) fn server_info(source: &crate::config::SourceOffer) -> Value {
    serde_json::json!({"product":"openlegal4everyone.stream", "version":env!("CARGO_PKG_VERSION"),
                "license": crate::config::SourceOffer::LICENSE,
                "licenseUrl": crate::config::SourceOffer::LICENSE_URL,
                "sourceUrl": source.url(),
                "transports":["streamable-http", "webtransport-v1"], "protocolVersions":["2026-07-28", "2025-11-25"]})
}
