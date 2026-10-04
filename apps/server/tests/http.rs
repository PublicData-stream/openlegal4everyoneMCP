use openlegal_server::{
    config::{AccessPolicy, Limits},
    endpoint::{Endpoint, EndpointContext},
    handler::McpHandler,
    http::HttpEndpoint,
    registry::{ToolError, ToolModule, ToolRegistry},
};
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct Input {
    #[schemars(range(min = 1, max = 10))]
    count: usize,
}
#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct Empty {}
struct SyntheticModule;
impl ToolModule for SyntheticModule {
    fn register(self, registry: &mut ToolRegistry) -> Result<(), openlegal_server::ServerError> {
        registry.register::<Input, _, _>(
            "synthetic",
            "Synthetic extension fixture",
            |input, _| async move { Ok(json!({"count": input.count})) },
        )
    }
}

struct Active(Arc<AtomicUsize>);
impl Drop for Active {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}
struct Server {
    url: String,
    context: EndpointContext,
    task: tokio::task::JoinHandle<()>,
    active: Arc<AtomicUsize>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.context.shutdown.cancel();
        self.task.abort();
    }
}
impl Server {
    async fn start() -> Self {
        let mut registry = ToolRegistry::new();
        registry.register_module(SyntheticModule).unwrap();
        let active = Arc::new(AtomicUsize::new(0));
        let count = active.clone();
        registry
            .register::<Empty, _, _>("slow", "Synthetic slow fixture", move |_, _| {
                let count = count.clone();
                async move {
                    count.fetch_add(1, Ordering::SeqCst);
                    let _guard = Active(count);
                    tokio::time::sleep(Duration::from_millis(1200)).await;
                    Ok(json!({"completed":true}))
                }
            })
            .unwrap();
        for (name, error) in [
            ("invalid", ToolError::InvalidInput),
            ("invalid_field_shorthand", ToolError::InvalidFieldShorthand),
            ("invalid_regex", ToolError::InvalidRegex),
            ("patch_conflict", ToolError::PatchConflict),
            (
                "invalid_utf8_boundary",
                ToolError::InvalidUtf8Boundary { offset: 8 },
            ),
            (
                "attachment_kind_mismatch",
                ToolError::AttachmentKindMismatch,
            ),
            ("not_found", ToolError::NotFound),
            ("freshness_unavailable", ToolError::FreshnessUnavailable),
            ("resource_limit", ToolError::ResourceLimit),
            ("unavailable", ToolError::Unavailable),
            ("internal", ToolError::Internal),
        ] {
            registry
                .register::<Empty, _, _>(name, "Synthetic typed error", move |_, _| async move {
                    Err(error)
                })
                .unwrap();
        }
        let limits = Arc::new(Limits {
            max_message_bytes: 16384,
            io_timeout_secs: 1,
            call_timeout_secs: 3,
            shutdown_timeout_secs: 1,
            ..Limits::default()
        });
        let context = EndpointContext {
            handler: McpHandler::new(
                registry,
                limits.clone(),
                openlegal_server::config::SourceOffer::new("https://source.test/running").unwrap(),
            )
            .unwrap(),
            limits: limits.clone(),
            shutdown: CancellationToken::new(),
            ready: Arc::new(AtomicBool::new(true)),
            buffers: Arc::new(Semaphore::new(limits.max_buffer_bytes)),
            original_buffers: Arc::new(Semaphore::new(limits.max_original_buffer_bytes)),
            requests: Arc::new(Semaphore::new(limits.max_in_flight)),
            connections: Arc::new(Semaphore::new(limits.max_connections)),
        };
        let bound = HttpEndpoint {
            tls: None,
            edge_mtls: None,
            bind: "127.0.0.1:0".parse().unwrap(),
            access: AccessPolicy {
                allowed_hosts: vec!["backend.test".into()],
                allowed_origins: vec!["https://allowed.test".into()],
            },
        }
        .bind(context.clone())
        .await
        .unwrap();
        let url = format!("http://{}/mcp", bound.addresses[0]);
        let task = tokio::spawn(async move { bound.run.await.unwrap() });
        Self {
            url,
            context,
            task,
            active,
        }
    }
    fn raw_request(&self, body: &Value) -> reqwest::RequestBuilder {
        reqwest::Client::new()
            .post(&self.url)
            .header("host", "backend.test")
            .header("accept", "application/json, text/event-stream")
            .json(body)
    }
    fn request(&self, version: &str, method: &str, mut params: Value) -> reqwest::RequestBuilder {
        if version == "2026-07-28" {
            params["_meta"] = meta();
        }
        let mut request = self
            .raw_request(&json!({"jsonrpc":"2.0","id":1,"method":method,"params":params}))
            .header("mcp-protocol-version", version)
            .header("mcp-method", method);
        if let Some(name) = params.get("name").and_then(Value::as_str) {
            request = request.header("mcp-name", name);
        }
        request
    }
}
fn codex_initialize(version: &str) -> Value {
    json!({"jsonrpc":"2.0","id":0,"method":"initialize","params":{
        "protocolVersion":version,
        "capabilities":{"elicitation":{"form":{},"url":{}}},
        "clientInfo":{"name":"codex-mcp-client","title":"Codex","version":"0.159.0-alpha.12.1"}
    }})
}
fn meta() -> Value {
    json!({"io.modelcontextprotocol/protocolVersion":"2026-07-28",
    "io.modelcontextprotocol/clientInfo":{"name":"http-fixture","version":"1"},"io.modelcontextprotocol/clientCapabilities":{}})
}
async fn reply(response: reqwest::Response) -> Value {
    decode(response, 200).await
}
async fn decode(response: reqwest::Response, expected: u16) -> Value {
    decode_id(response, expected, 1).await
}
async fn decode_id(response: reqwest::Response, expected: u16, id: u64) -> Value {
    let status = response.status();
    let text = response.text().await.unwrap();
    assert_eq!(status, expected, "{text}");
    if text.trim_start().starts_with('{') {
        return serde_json::from_str(&text).unwrap();
    }
    text.lines()
        .filter_map(|line| line.strip_prefix("data:"))
        .filter_map(|data| serde_json::from_str::<Value>(data.trim()).ok())
        .find(|value| value.get("id") == Some(&json!(id)))
        .unwrap_or_else(|| panic!("no RPC response: {text}"))
}

#[tokio::test]
async fn captured_codex_initialize_negotiates_before_tool_discovery() {
    let server = Server::start().await;
    let response = server
        .raw_request(&codex_initialize("2025-06-18"))
        .send()
        .await
        .unwrap();
    assert!(response.headers().get("mcp-session-id").is_none());
    let initialized = decode_id(response, 200, 0).await;
    assert_eq!(initialized["id"], 0);
    let negotiated = initialized["result"]["protocolVersion"].as_str().unwrap();
    assert_eq!(negotiated, "2025-11-25");
    let response = server
        .raw_request(&json!({"jsonrpc":"2.0","method":"notifications/initialized"}))
        .header("mcp-protocol-version", negotiated)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 202);
    assert!(response.bytes().await.unwrap().is_empty());
    let listed = reply(
        server
            .raw_request(&json!({"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}))
            .header("mcp-protocol-version", negotiated)
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert!(listed["result"].get("resultType").is_none());
    assert!(
        listed["result"]["tools"]
            .as_array()
            .unwrap()
            .iter()
            .any(|tool| tool["name"] == "synthetic")
    );
}

#[tokio::test]
async fn initialize_negotiates_proposals_with_consistent_supported_headers() {
    let server = Server::start().await;
    for (proposal, header) in [
        ("2025-11-25", None),
        ("2099-01-01", None),
        ("2026-07-28", None),
        ("2025-11-25", Some("2025-11-25")),
        ("2026-07-28", Some("2026-07-28")),
    ] {
        let mut request = server.raw_request(&codex_initialize(proposal));
        if let Some(header) = header {
            request = request.header("mcp-protocol-version", header);
        }
        let response = request.send().await.unwrap();
        assert!(response.headers().get("mcp-session-id").is_none());
        let result = decode_id(response, 200, 0).await;
        assert_eq!(
            result["result"]["protocolVersion"], "2025-11-25",
            "{proposal} {header:?}"
        );
    }
    for (proposal, header, code) in [
        ("2025-06-18", "2025-06-18", -32022),
        ("2099-01-01", "2099-01-01", -32022),
        ("2025-06-18", "2025-11-25", -32020),
        ("2025-11-25", "2026-07-28", -32020),
        ("2026-07-28", "2025-11-25", -32020),
    ] {
        let result = decode_id(
            server
                .raw_request(&codex_initialize(proposal))
                .header("mcp-protocol-version", header)
                .send()
                .await
                .unwrap(),
            400,
            0,
        )
        .await;
        assert_eq!(result["id"], 0);
        assert_eq!(
            result["error"]["code"], code,
            "{proposal} {header}: {result}"
        );
    }
}

#[tokio::test]
async fn initialize_rejects_malformed_versions_and_ambiguous_headers() {
    let server = Server::start().await;
    for version in [None, Some(Value::Null), Some(json!(123)), Some(json!({}))] {
        let mut body = codex_initialize("2025-06-18");
        match version {
            Some(version) => body["params"]["protocolVersion"] = version,
            None => {
                body["params"]
                    .as_object_mut()
                    .unwrap()
                    .remove("protocolVersion");
            }
        }
        let result = decode_id(server.raw_request(&body).send().await.unwrap(), 400, 0).await;
        assert_eq!(result["error"]["code"], -32022);
    }
    for (name, value) in [
        ("mcp-protocol-version", "2025-11-25"),
        ("mcp-method", "initialize"),
        ("mcp-name", "fixture"),
    ] {
        let mut headers = reqwest::header::HeaderMap::new();
        let name = reqwest::header::HeaderName::from_bytes(name.as_bytes()).unwrap();
        headers.append(name.clone(), value.parse().unwrap());
        headers.append(name, value.parse().unwrap());
        let result = decode_id(
            server
                .raw_request(&codex_initialize("2025-11-25"))
                .headers(headers)
                .send()
                .await
                .unwrap(),
            400,
            0,
        )
        .await;
        assert_eq!(result["error"]["code"], -32020);
    }
    let result = decode_id(
        server
            .raw_request(&codex_initialize("2025-11-25"))
            .header(
                "mcp-protocol-version",
                reqwest::header::HeaderValue::from_bytes(&[0x80]).unwrap(),
            )
            .send()
            .await
            .unwrap(),
        400,
        0,
    )
    .await;
    assert_eq!(result["error"]["code"], -32020);
    let mut body = codex_initialize("2025-11-25");
    body["params"]["_meta"] = meta();
    let result = decode_id(
        server
            .raw_request(&body)
            .header("mcp-protocol-version", "2025-11-25")
            .send()
            .await
            .unwrap(),
        400,
        0,
    )
    .await;
    assert_eq!(result["error"]["code"], -32020);
    assert_eq!(
        server
            .context
            .handler
            .counters
            .calls
            .load(Ordering::Relaxed),
        0
    );
}

#[tokio::test]
async fn subsequent_requests_require_supported_protocol_headers() {
    let server = Server::start().await;
    for method in ["notifications/initialized", "tools/list"] {
        for header in [None, Some("2025-06-18")] {
            let mut body = json!({"jsonrpc":"2.0","method":method,"params":{}});
            if method == "tools/list" {
                body["id"] = json!(1);
            }
            let mut request = server.raw_request(&body);
            if let Some(header) = header {
                request = request.header("mcp-protocol-version", header);
            }
            let result = decode(request.send().await.unwrap(), 400).await;
            assert_eq!(
                result["error"]["code"], -32022,
                "{method} {header:?}: {result}"
            );
            assert_eq!(
                result["error"]["data"]["supported"],
                json!(["2026-07-28", "2025-11-25"])
            );
        }
    }
    let mut params = json!({"_meta":meta()});
    params["_meta"]["io.modelcontextprotocol/protocolVersion"] = json!("2099-01-01");
    let result = decode(
        server
            .raw_request(
                &json!({"jsonrpc":"2.0","id":1,"method":"server/discover","params":params}),
            )
            .header("mcp-protocol-version", "2099-01-01")
            .header("mcp-method", "server/discover")
            .send()
            .await
            .unwrap(),
        400,
    )
    .await;
    assert_eq!(result["error"]["code"], -32022);
    let discovery = reply(
        server
            .request("2026-07-28", "server/discover", json!({}))
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(discovery["result"]["resultType"], "complete");
    assert_eq!(
        discovery["result"]["supportedVersions"],
        json!(["2026-07-28", "2025-11-25"])
    );
    assert_eq!(
        server
            .context
            .handler
            .counters
            .calls
            .load(Ordering::Relaxed),
        0
    );
}

#[tokio::test]
async fn both_revisions_share_registered_tools_and_validation() {
    let server = Server::start().await;
    for version in ["2026-07-28", "2025-11-25"] {
        if version == "2025-11-25" {
            let result=reply(server.request(version,"initialize",json!({"protocolVersion":version,"clientInfo":{"name":"test","version":"1"},"capabilities":{}})).send().await.unwrap()).await;
            assert_eq!(result["result"]["protocolVersion"], version);
        } else {
            let result = reply(
                server
                    .request(version, "server/discover", json!({}))
                    .send()
                    .await
                    .unwrap(),
            )
            .await;
            assert_eq!(result["result"]["resultType"], "complete");
        }
        let list = reply(
            server
                .request(version, "tools/list", json!({}))
                .send()
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(
            list["result"].get("resultType").is_some(),
            version == "2026-07-28"
        );
        assert!(
            list["result"]["tools"]
                .as_array()
                .unwrap()
                .iter()
                .any(|tool| tool["name"] == "synthetic")
        );
        let result = reply(
            server
                .request(
                    version,
                    "tools/call",
                    json!({"name":"synthetic","arguments":{"count":3}}),
                )
                .send()
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(result["result"]["structuredContent"]["count"], 3);
        let result = decode(
            server
                .request(
                    version,
                    "tools/call",
                    json!({"name":"synthetic","arguments":{"count":11}}),
                )
                .send()
                .await
                .unwrap(),
            if version == "2026-07-28" { 400 } else { 200 },
        )
        .await;
        assert_eq!(result["error"]["code"], -32602);
    }
}

#[tokio::test]
async fn rejects_boundary_errors_before_dispatch() {
    let server = Server::start().await;
    assert_eq!(
        server
            .request("2026-07-28", "tools/list", json!({}))
            .header("origin", "https://denied.test")
            .send()
            .await
            .unwrap()
            .status(),
        403
    );
    assert_eq!(
        server
            .request("2026-07-28", "tools/list", json!({}))
            .header("host", "wrong.test")
            .send()
            .await
            .unwrap()
            .status(),
        403
    );
    let unknown = server
        .request("2030-01-01", "tools/list", json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(unknown.status(), 400);
    assert_eq!(
        unknown.json::<Value>().await.unwrap()["error"]["code"],
        -32022
    );
    assert_eq!(
        server
            .request("2026-07-28", "tools/list", json!({}))
            .header("mcp-method", "tools/call")
            .send()
            .await
            .unwrap()
            .status(),
        400
    );
    assert_eq!(
        server
            .request(
                "2026-07-28",
                "tools/list",
                json!({"padding":"x".repeat(20000)})
            )
            .send()
            .await
            .unwrap()
            .status(),
        413
    );
    assert_eq!(
        server
            .context
            .handler
            .counters
            .calls
            .load(Ordering::Relaxed),
        0
    );
}

#[tokio::test]
async fn long_tool_can_finish_after_io_interval_and_errors_stay_typed() {
    let server = Server::start().await;
    let response = reply(
        server
            .request(
                "2026-07-28",
                "tools/call",
                json!({"name":"slow","arguments":{}}),
            )
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(response["result"]["structuredContent"]["completed"], true);
    for version in ["2026-07-28", "2025-11-25"] {
        for (name, expected) in [
            ("invalid", None),
            ("invalid_field_shorthand", None),
            (
                "invalid_regex",
                Some(
                    json!({"code":"invalid_regex","message":"The regular expression is invalid."}),
                ),
            ),
            (
                "patch_conflict",
                Some(
                    json!({"code":"patch_conflict","message":"Patch context did not match the target."}),
                ),
            ),
            (
                "invalid_utf8_boundary",
                Some(
                    json!({"code":"invalid_utf8_boundary","message":"The byte offset is not on a UTF-8 character boundary.","offset":8}),
                ),
            ),
            (
                "attachment_kind_mismatch",
                Some(
                    json!({"code":"attachment_kind_mismatch","message":"Attachment kind is incompatible with this operation."}),
                ),
            ),
            (
                "not_found",
                Some(json!({"code":"not_found","message":"Requested data was not found."})),
            ),
            (
                "freshness_unavailable",
                Some(
                    json!({"code":"freshness_unavailable","message":"Data meeting the freshness requirement is unavailable."}),
                ),
            ),
            (
                "resource_limit",
                Some(
                    json!({"code":"resource_limit","message":"The operation exceeds its resource limit."}),
                ),
            ),
            (
                "unavailable",
                Some(json!({"code":"unavailable","message":"Service is temporarily unavailable."})),
            ),
            (
                "internal",
                Some(json!({"code":"internal","message":"Tool execution failed."})),
            ),
        ] {
            let response = decode(
                server
                    .request(version, "tools/call", json!({"name":name,"arguments":{}}))
                    .send()
                    .await
                    .unwrap(),
                if expected.is_none() && version == "2026-07-28" {
                    400
                } else {
                    200
                },
            )
            .await;
            if let Some(expected) = expected {
                assert!(
                    response.get("error").is_none(),
                    "{version} {name}: {response}"
                );
                assert_eq!(response["result"]["isError"], true, "{version} {name}");
                assert_eq!(
                    response["result"]["structuredContent"], expected,
                    "{version} {name}"
                );
            } else {
                assert_eq!(response["error"]["code"], -32602, "{version} {name}");
                assert!(response.get("result").is_none(), "{version} {name}");
            }
        }
    }
}

#[tokio::test]
async fn disconnect_and_shutdown_release_handler_and_admission() {
    let server = Server::start().await;
    let request = server.request(
        "2026-07-28",
        "tools/call",
        json!({"name":"slow","arguments":{}}),
    );
    let client = tokio::spawn(async move { request.send().await });
    tokio::time::timeout(Duration::from_secs(1), async {
        while server.active.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    client.abort();
    let _ = client.await;
    server.context.shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(2), async {
        while server.active.load(Ordering::SeqCst) > 0
            || server.context.requests.available_permits() != server.context.limits.max_in_flight
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        server.context.buffers.available_permits(),
        server.context.limits.max_buffer_bytes
    );
}
