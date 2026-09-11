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
            handler: McpHandler::new(registry, limits.clone()).unwrap(),
            limits: limits.clone(),
            shutdown: CancellationToken::new(),
            ready: Arc::new(AtomicBool::new(true)),
            buffers: Arc::new(Semaphore::new(limits.max_buffer_bytes)),
            requests: Arc::new(Semaphore::new(limits.max_in_flight)),
            connections: Arc::new(Semaphore::new(limits.max_connections)),
        };
        let bound = HttpEndpoint {
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
    fn request(&self, version: &str, method: &str, mut params: Value) -> reqwest::RequestBuilder {
        if version == "2026-07-28" {
            params["_meta"] = meta();
        }
        let mut request = reqwest::Client::new()
            .post(&self.url)
            .header("host", "backend.test")
            .header("accept", "application/json, text/event-stream")
            .header("mcp-protocol-version", version)
            .header("mcp-method", method);
        if let Some(name) = params.get("name").and_then(Value::as_str) {
            request = request.header("mcp-name", name);
        }
        request.json(&json!({"jsonrpc":"2.0","id":1,"method":method,"params":params}))
    }
}
fn meta() -> Value {
    json!({"io.modelcontextprotocol/protocolVersion":"2026-07-28",
    "io.modelcontextprotocol/clientInfo":{"name":"http-fixture","version":"1"},"io.modelcontextprotocol/clientCapabilities":{}})
}
async fn reply(response: reqwest::Response) -> Value {
    decode(response, 200).await
}
async fn decode(response: reqwest::Response, expected: u16) -> Value {
    let status = response.status();
    let text = response.text().await.unwrap();
    assert_eq!(status, expected, "{text}");
    if text.trim_start().starts_with('{') {
        return serde_json::from_str(&text).unwrap();
    }
    text.lines()
        .filter_map(|line| line.strip_prefix("data:"))
        .filter_map(|data| serde_json::from_str::<Value>(data.trim()).ok())
        .find(|value| value.get("id") == Some(&json!(1)))
        .unwrap_or_else(|| panic!("no RPC response: {text}"))
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
    for name in ["invalid", "unavailable", "internal"] {
        let response = decode(
            server
                .request(
                    "2026-07-28",
                    "tools/call",
                    json!({"name":name,"arguments":{}}),
                )
                .send()
                .await
                .unwrap(),
            if name == "invalid" { 400 } else { 200 },
        )
        .await;
        if name == "invalid" {
            assert_eq!(response["error"]["code"], -32602);
        } else {
            assert_eq!(response["result"]["isError"], true);
            assert_eq!(response["result"]["structuredContent"]["code"], name);
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
