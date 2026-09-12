//! Wire tests for additive typed tools, UI resources and bounded progress.
use openlegal_server::{
    ServerBuilder,
    config::{AccessPolicy, Limits},
    http::HttpEndpoint,
    progress::ProgressStage,
    registry::{ToolOptions, ToolOutput, ToolRegistry},
    resources::ResourceRegistry,
};
use rmcp::model::{MetaObject, Resource, ResourceContents};
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

#[derive(serde::Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
struct Empty {}

#[derive(serde::Serialize, schemars::JsonSchema)]
struct Output {
    #[schemars(range(min = 1, max = 10))]
    value: u32,
}

struct Server {
    url: String,
    shutdown: CancellationToken,
    task: tokio::task::JoinHandle<Result<(), openlegal_server::ServerError>>,
    finish: Arc<Notify>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.shutdown.cancel();
        self.task.abort();
    }
}
impl Server {
    async fn start() -> Self {
        Self::start_with_limit(Limits::default().max_message_bytes).await
    }

    async fn start_with_limit(max_message_bytes: usize) -> Self {
        let mut resources = ResourceRegistry::new();
        resources
            .register(
                Resource::new("ui://demo/widget.html", "demo")
                    .with_mime_type("text/html;profile=mcp-app"),
                ResourceContents::text(
                    "<!doctype html><p>Synthetic results</p>",
                    "ui://demo/widget.html",
                )
                .with_mime_type("text/html;profile=mcp-app")
                .with_meta(MetaObject(
                    json!({"ui":{"prefersBorder":true}})
                        .as_object()
                        .unwrap()
                        .clone(),
                )),
            )
            .unwrap();
        let mut registry = ToolRegistry::new();
        let finish = Arc::new(Notify::new());
        let tool_finish = finish.clone();
        registry
            .register_typed::<Empty, Output, _, _>(
                "progress",
                "Synthetic progress fixture",
                ToolOptions {
                    meta: Some(MetaObject(
                        json!({"ui":{"resourceUri":"ui://demo/widget.html"}})
                            .as_object()
                            .unwrap()
                            .clone(),
                    )),
                    ..Default::default()
                },
                move |_, context| {
                    let finish = tool_finish.clone();
                    async move {
                        context
                            .progress
                            .report(ProgressStage::CheckingCache)
                            .await?;
                        context
                            .progress
                            .report(ProgressStage::CheckingCache)
                            .await?;
                        context.progress.report(ProgressStage::Fetching).await?;
                        finish.notified().await;
                        context.progress.report(ProgressStage::Complete).await?;
                        context.progress.report(ProgressStage::Processing).await?;
                        Ok(ToolOutput {
                            structured: Output { value: 3 },
                            text: Some("Synthetic result".into()),
                            meta: Some(MetaObject(
                                json!({"synthetic":true}).as_object().unwrap().clone(),
                            )),
                        })
                    }
                },
            )
            .unwrap();
        registry
            .register_typed::<Empty, Output, _, _>(
                "invalid_output",
                "Synthetic invalid output",
                ToolOptions::default(),
                |_, _| async { Ok(ToolOutput::new(Output { value: 99 })) },
            )
            .unwrap();
        registry
            .register::<Empty, _, _>("legacy", "Unchanged registration", |_, _| async {
                Ok(json!({"old":true}))
            })
            .unwrap();
        let mut builder = ServerBuilder::new(
            registry,
            Limits {
                max_message_bytes,
                ..Default::default()
            },
            openlegal_server::config::SourceOffer::new("https://source.test/running").unwrap(),
        )
        .with_resources(resources);
        builder
            .register_endpoint(HttpEndpoint {
                bind: "127.0.0.1:0".parse().unwrap(),
                access: AccessPolicy {
                    allowed_hosts: vec!["test.local".into()],
                    allowed_origins: vec![],
                },
            })
            .unwrap();
        let running = builder.bind().await.unwrap();
        let url = format!("http://{}/mcp", running.addresses()[0].1[0]);
        let shutdown = CancellationToken::new();
        let token = shutdown.clone();
        let task = tokio::spawn(running.run(token));
        Self {
            url,
            shutdown,
            task,
            finish,
        }
    }

    async fn post(
        &self,
        version: &str,
        id: u64,
        method: &str,
        mut params: Value,
    ) -> reqwest::Response {
        if version == "2026-07-28" {
            let meta = params
                .as_object_mut()
                .unwrap()
                .entry("_meta")
                .or_insert(json!({}));
            meta["io.modelcontextprotocol/protocolVersion"] = json!(version);
            meta["io.modelcontextprotocol/clientInfo"] = json!({"name":"fixture","version":"1"});
            meta["io.modelcontextprotocol/clientCapabilities"] = json!({});
        }
        let mut request = reqwest::Client::new()
            .post(&self.url)
            .header("Host", "test.local")
            .header("Accept", "application/json, text/event-stream")
            .header("MCP-Protocol-Version", version)
            .header("Mcp-Method", method);
        if let Some(name) = params
            .get("name")
            .or_else(|| params.get("uri"))
            .and_then(Value::as_str)
        {
            request = request.header("Mcp-Name", name);
        }
        request
            .json(&json!({"jsonrpc":"2.0","id":id,"method":method,"params":params}))
            .send()
            .await
            .unwrap()
    }
}

fn messages(text: &str) -> Vec<Value> {
    text.lines()
        .filter_map(|line| line.strip_prefix("data:"))
        .filter_map(|json| serde_json::from_str(json.trim()).ok())
        .collect()
}
async fn result(response: reqwest::Response) -> Value {
    let status = response.status();
    let body = tokio::time::timeout(Duration::from_secs(3), response.text())
        .await
        .unwrap()
        .unwrap();
    let value: Value = messages(&body)
        .pop()
        .or_else(|| serde_json::from_str(&body).ok())
        .unwrap_or_else(|| panic!("invalid RPC response {status}: {body}"));
    if value.get("error").is_some() {
        assert!(
            [200, 400, 404].contains(&status.as_u16()),
            "unexpected RPC error status {status}: {body}"
        );
    } else {
        assert_eq!(status, 200, "unexpected success status: {body}");
    }
    value
}

#[tokio::test]
async fn typed_tools_resources_and_legacy_registration_work_in_both_revisions() {
    let server = Server::start().await;
    for version in ["2025-11-25", "2026-07-28"] {
        let listed = result(server.post(version, 1, "tools/list", json!({})).await).await;
        let tools = listed["result"]["tools"].as_array().unwrap();
        let typed = tools
            .iter()
            .find(|tool| tool["name"] == "progress")
            .unwrap();
        assert_eq!(typed["outputSchema"]["type"], "object");
        assert_eq!(typed["_meta"]["ui"]["resourceUri"], "ui://demo/widget.html");
        let old = result(
            server
                .post(
                    version,
                    2,
                    "tools/call",
                    json!({"name":"legacy","arguments":{}}),
                )
                .await,
        )
        .await;
        assert_eq!(old["result"]["structuredContent"]["old"], true);
        let invalid = result(
            server
                .post(
                    version,
                    3,
                    "tools/call",
                    json!({"name":"invalid_output","arguments":{}}),
                )
                .await,
        )
        .await;
        assert_eq!(invalid["result"]["isError"], true);
        assert_eq!(invalid["result"]["structuredContent"]["code"], "internal");
        assert!(!invalid.to_string().contains("99"));
        let resources = result(server.post(version, 4, "resources/list", json!({})).await).await;
        assert_eq!(
            resources["result"]["resources"][0]["uri"],
            "ui://demo/widget.html"
        );
        let read = result(
            server
                .post(
                    version,
                    5,
                    "resources/read",
                    json!({"uri":"ui://demo/widget.html"}),
                )
                .await,
        )
        .await;
        assert!(
            read["result"]["contents"][0]["text"]
                .as_str()
                .unwrap()
                .contains("Synthetic")
        );
        assert_eq!(
            read["result"]["contents"][0]["_meta"]["ui"]["prefersBorder"],
            true
        );
        let unknown = result(
            server
                .post(
                    version,
                    6,
                    "resources/read",
                    json!({"uri":"file:///etc/passwd"}),
                )
                .await,
        )
        .await;
        assert!(unknown.get("error").is_some());
    }
}

#[tokio::test]
async fn progress_arrives_before_completion_is_monotonic_and_preserves_rich_result() {
    use futures::StreamExt;
    let server = Server::start().await;
    for version in ["2025-11-25", "2026-07-28"] {
        let response = server
            .post(
                version,
                7,
                "tools/call",
                json!({"name":"progress","arguments":{},"_meta":{"progressToken":"bounded-token"}}),
            )
            .await;
        assert_eq!(response.status(), 200);
        let mut stream = response.bytes_stream();
        let mut body = String::new();
        tokio::time::timeout(Duration::from_secs(3), async {
            while messages(&body).is_empty() {
                body.push_str(std::str::from_utf8(&stream.next().await.unwrap().unwrap()).unwrap());
            }
        })
        .await
        .unwrap();
        assert_eq!(messages(&body)[0]["method"], "notifications/progress");
        assert!(messages(&body).iter().all(|m| m.get("result").is_none()));
        server.finish.notify_one();
        tokio::time::timeout(Duration::from_secs(3), async {
            while let Some(chunk) = stream.next().await {
                body.push_str(std::str::from_utf8(&chunk.unwrap()).unwrap());
            }
        })
        .await
        .unwrap();
        let values = messages(&body);
        let stages: Vec<_> = values
            .iter()
            .filter(|m| m["method"] == "notifications/progress")
            .map(|m| {
                assert_eq!(m["params"]["progressToken"], "bounded-token");
                m["params"]["progress"].as_f64().unwrap()
            })
            .collect();
        assert_eq!(stages, vec![1.0, 3.0, 5.0]);
        let final_result = values.last().unwrap();
        assert_eq!(final_result["result"]["structuredContent"]["value"], 3);
        assert_eq!(
            final_result["result"]["content"][0]["text"],
            "Synthetic result"
        );
        assert_eq!(final_result["result"]["_meta"]["synthetic"], true);
    }
}

#[tokio::test]
async fn no_progress_token_yields_only_final_and_oversized_token_is_rejected() {
    let server = Server::start().await;
    server.finish.notify_one();
    let response = server
        .post(
            "2026-07-28",
            8,
            "tools/call",
            json!({"name":"progress","arguments":{}}),
        )
        .await;
    let body = response.text().await.unwrap();
    assert_eq!(messages(&body).len(), 1);
    assert!(messages(&body)[0].get("result").is_some());
    let response = result(
        server
            .post(
                "2026-07-28",
                9,
                "tools/call",
                json!({"name":"progress","arguments":{},"_meta":{"progressToken":"x".repeat(129)}}),
            )
            .await,
    )
    .await;
    assert_eq!(response["error"]["code"], -32602);
}

#[tokio::test]
async fn escaped_progress_tokens_cannot_consume_the_final_response_budget() {
    let server = Server::start_with_limit(4096).await;
    server.finish.notify_one();
    let response = server
        .post(
            "2026-07-28",
            10,
            "tools/call",
            json!({
                "name":"progress", "arguments":{}, "_meta":{"progressToken":"\u{0}".repeat(128)}
            }),
        )
        .await;
    assert_eq!(response.status(), 200);
    let body = response.text().await.unwrap();
    assert!(body.len() <= 4096);
    let messages = messages(&body);
    assert!(
        messages
            .iter()
            .filter(|message| message["method"] == "notifications/progress")
            .count()
            < 3
    );
    assert_eq!(
        messages.last().unwrap()["result"]["structuredContent"]["value"],
        3
    );
}
