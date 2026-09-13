//! Real-Git MCP boundary checks; inputs and UI resources are entirely synthetic.
use openlegal_adapters::text_diff::{GitDiffEngine, OsHandleGenerator};
use openlegal_application::text_diff::TextDiffService;
use openlegal_server::{
    ServerBuilder,
    config::{AccessPolicy, Limits, SourceOffer},
    demo::DemoTools,
    http::HttpEndpoint,
    registry::server_info_registry,
    resources::ResourceRegistry,
    text_diff::{TextDiffTools, WIDGET_URI},
};
use rmcp::model::{Resource, ResourceContents};
use serde_json::{Value, json};
use std::{path::Path, sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;

const SOURCE: &str = "https://source.test/running?section=diff&revision=1";
const ESCAPED_SOURCE: &str = "https://source.test/running?section=diff&amp;revision=1";
const HTML: &str = "<html><meta name=\"openlegal-source-url\" content=\"__OPENLEGAL_SOURCE_URL__\">Synthetic comparison fixture</html>";

struct Server {
    url: String,
    shutdown: CancellationToken,
    task: Option<tokio::task::JoinHandle<Result<(), openlegal_server::ServerError>>>,
}

impl Drop for Server {
    fn drop(&mut self) {
        self.shutdown.cancel();
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

impl Server {
    async fn start(include_demo: bool) -> Self {
        let source = SourceOffer::new(SOURCE).unwrap();
        let engine = GitDiffEngine::new(Path::new("/usr/bin/git")).await.unwrap();
        let service = TextDiffService::new(Arc::new(engine), Arc::new(OsHandleGenerator));
        let mut registry = server_info_registry(source.clone()).unwrap();
        registry
            .register_module(TextDiffTools {
                service: service.clone(),
            })
            .unwrap();
        let mut resources =
            openlegal_server::text_diff::widget_resources(HTML.into(), &source).unwrap();
        let demo_service =
            include_demo.then(|| openlegal_server::demo::service("http://127.0.0.1:9").unwrap());
        if let Some(demo) = &demo_service {
            registry
                .register_module(DemoTools {
                    service: demo.clone(),
                })
                .unwrap();
            resources
                .extend(openlegal_server::demo::widget_resources(HTML.into(), &source).unwrap())
                .unwrap();
        }
        let mut builder = ServerBuilder::new(
            registry,
            Limits {
                max_message_bytes: 16 * 1024 * 1024,
                max_buffer_bytes: 256 * 1024 * 1024,
                ..Limits::default()
            },
            source,
        )
        .with_resources(resources);
        builder
            .register_endpoint(HttpEndpoint {
                bind: "127.0.0.1:0".parse().unwrap(),
                access: AccessPolicy {
                    allowed_hosts: vec!["diff.test".into()],
                    allowed_origins: vec![],
                },
            })
            .unwrap();
        builder
            .register_worker("text-diff", move |shutdown| async move {
                service.run(shutdown).await?;
                Ok(())
            })
            .unwrap();
        if let Some(demo) = demo_service {
            builder
                .register_worker("retrieval", move |shutdown| async move {
                    demo.run(shutdown).await?;
                    Ok(())
                })
                .unwrap();
        }
        let server = builder.bind().await.unwrap();
        let url = format!("http://{}/mcp", server.addresses()[0].1[0]);
        let shutdown = CancellationToken::new();
        let task = Some(tokio::spawn(server.run(shutdown.clone())));
        Self {
            url,
            shutdown,
            task,
        }
    }

    async fn stop(mut self) {
        self.shutdown.cancel();
        tokio::time::timeout(Duration::from_secs(5), self.task.take().unwrap())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }

    async fn rpc(&self, version: &str, method: &str, mut params: Value) -> Value {
        if version == "2026-07-28" {
            params["_meta"] = json!({
                "io.modelcontextprotocol/protocolVersion": version,
                "io.modelcontextprotocol/clientInfo": {"name":"text-diff-test", "version":"1"},
                "io.modelcontextprotocol/clientCapabilities": {}
            });
        }
        let bounded_page = method == "tools/call" && params["name"] == "get_text_diff_page";
        let mut request = reqwest::Client::new()
            .post(&self.url)
            .header("host", "diff.test")
            .header("accept", "application/json, text/event-stream")
            .header("mcp-protocol-version", version)
            .header("mcp-method", method)
            .timeout(Duration::from_secs(10));
        if let Some(name) = params
            .get("name")
            .or_else(|| params.get("uri"))
            .and_then(Value::as_str)
        {
            request = request.header("mcp-name", name);
        }
        let response = request
            .json(&json!({"jsonrpc":"2.0", "id":1, "method":method, "params":params}))
            .send()
            .await
            .unwrap();
        let status = response.status();
        let body = response.text().await.unwrap();
        if bounded_page {
            assert!(
                body.len() <= 256 * 1024,
                "complete page response exceeds 256 KiB: {}",
                body.len()
            );
        }
        let value: Value = if body.trim_start().starts_with('{') {
            serde_json::from_str(&body).unwrap()
        } else {
            body.lines()
                .filter_map(|line| line.strip_prefix("data:"))
                .map(|line| serde_json::from_str::<Value>(line.trim()).unwrap())
                .find(|value| value.get("id") == Some(&json!(1)))
                .unwrap_or_else(|| panic!("RPC response missing: {body}"))
        };
        if value.get("error").is_some() {
            assert!(
                [200, 400, 404].contains(&status.as_u16()),
                "{status}: {body}"
            );
        } else {
            assert_eq!(status, 200, "{body}");
        }
        value
    }

    async fn call(&self, version: &str, name: &str, arguments: Value) -> Value {
        self.rpc(
            version,
            "tools/call",
            json!({"name":name, "arguments":arguments}),
        )
        .await
    }

    async fn successful_call(&self, version: &str, name: &str, arguments: Value) -> Value {
        let response = self.call(version, name, arguments).await;
        assert!(response.get("error").is_none(), "{response}");
        assert_ne!(response["result"]["isError"], true, "{name}: {response}");
        assert_eq!(response["result"]["structuredContent"]["schema_version"], 1);
        response["result"]["structuredContent"].clone()
    }
}

#[tokio::test]
async fn independent_comparison_tools_have_typed_schemas_and_accurate_deletion_annotations() {
    let server = Server::start(false).await;
    for version in ["2025-11-25", "2026-07-28"] {
        let listed = server.rpc(version, "tools/list", json!({})).await;
        let tools = listed["result"]["tools"].as_array().unwrap();
        assert_eq!(tools.len(), 5);
        assert!(
            tools
                .iter()
                .all(|tool| !tool["name"].as_str().unwrap().starts_with("demo_"))
        );
        for name in [
            "compare_texts",
            "show_text_diff",
            "get_text_diff_page",
            "delete_text_diff",
        ] {
            let tool = tools.iter().find(|tool| tool["name"] == name).unwrap();
            assert_eq!(tool["inputSchema"]["type"], "object");
            assert_eq!(tool["outputSchema"]["type"], "object");
            assert_eq!(
                tool["annotations"]["readOnlyHint"],
                name != "delete_text_diff"
            );
            if name == "delete_text_diff" {
                assert_eq!(tool["annotations"]["destructiveHint"], true);
                assert_eq!(tool["annotations"]["idempotentHint"], true);
                assert_eq!(tool["annotations"]["openWorldHint"], false);
            }
            if name == "show_text_diff" {
                assert_eq!(tool["_meta"]["ui"]["resourceUri"], WIDGET_URI);
            }
        }
        let blank = server
            .successful_call(version, "show_text_diff", json!({}))
            .await;
        assert_eq!(blank["comparison"], Value::Null);
        let resources = server.rpc(version, "resources/list", json!({})).await;
        assert_eq!(
            resources["result"]["resources"].as_array().unwrap().len(),
            1
        );
        assert_eq!(resources["result"]["resources"][0]["uri"], WIDGET_URI);
    }
    server.stop().await;
}

#[tokio::test]
async fn both_revisions_compare_page_reopen_and_delete_without_demo() {
    let server = Server::start(false).await;
    for version in ["2025-11-25", "2026-07-28"] {
        let before = format!("\u{feff}{}tail", "법률 😀\r\n".repeat(6000));
        let after = before.replace("tail", "updated");
        let summary = server.successful_call(version, "compare_texts", json!({
            "before":before, "after":after, "before_label":"../../Before", "after_label":"After"
        })).await;
        assert_eq!(summary["equal"], false);
        assert_eq!(summary["additions"], 1);
        assert_eq!(summary["deletions"], 1);
        assert_eq!(summary["before"]["label"], "../../Before");
        assert_eq!(summary["before"]["bom"], true);
        assert_eq!(summary["before"]["crlf"], 6000);
        assert_eq!(summary["before"]["lf"], 0);
        assert_eq!(summary["before"]["final_newline"], false);
        let handle = summary["comparison_id"].as_str().unwrap();
        assert_eq!(handle.len(), 64);
        for (view, original) in [("before", &before), ("after", &after)] {
            let mut reconstructed = String::new();
            let mut number = 0;
            loop {
                let page = server
                    .successful_call(
                        version,
                        "get_text_diff_page",
                        json!({"comparison_id":handle,"view":view,"page":number}),
                    )
                    .await;
                assert_eq!(page["comparison_id"], handle);
                assert_eq!(page["view"], view);
                assert_eq!(page["page"], number);
                let text = page["text"].as_str().unwrap();
                assert!(text.len() <= 32 * 1024);
                assert!(serde_json::to_vec(&page).unwrap().len() <= 256 * 1024);
                reconstructed.push_str(text);
                number += 1;
                if number == page["total_pages"].as_u64().unwrap() {
                    break;
                }
                assert!(number <= 4);
            }
            assert!(number > 1, "fixture must exercise UTF-8 chunk boundaries");
            assert_eq!(&reconstructed, original);
        }
        let changes = server
            .successful_call(
                version,
                "get_text_diff_page",
                json!({"comparison_id":handle,"page":0}),
            )
            .await;
        let fragment = &changes["fragments"][0];
        assert!(fragment["patch"].as_str().unwrap().contains("-tail"));
        assert!(fragment["patch"].as_str().unwrap().contains("+updated"));
        assert!(fragment["before_start"].as_u64().unwrap() > 5990);
        let shown = server
            .successful_call(version, "show_text_diff", json!({"comparison_id":handle}))
            .await;
        assert_eq!(
            shown["comparison"], summary,
            "opening must not recompute or extend expiry"
        );
        for _ in 0..2 {
            assert_eq!(
                server
                    .successful_call(version, "delete_text_diff", json!({"comparison_id":handle}))
                    .await["deleted"],
                true
            );
        }
        for (name, arguments) in [
            ("show_text_diff", json!({"comparison_id":handle})),
            (
                "get_text_diff_page",
                json!({"comparison_id":handle,"page":0}),
            ),
        ] {
            let missing = server.call(version, name, arguments).await;
            assert_eq!(missing["result"]["isError"], true);
            assert_eq!(missing["result"]["structuredContent"]["code"], "not_found");
        }
        let pair = server
            .successful_call(version, "show_text_diff", json!({"before":"", "after":""}))
            .await;
        assert_eq!(pair["comparison"]["equal"], true);
        assert_eq!(pair["comparison"]["change_pages"], 0);
        assert_eq!(server.successful_call(version, "get_text_diff_page", json!({"comparison_id":pair["comparison"]["comparison_id"], "view":"before", "page":0})).await["text"], "");
        server
            .successful_call(
                version,
                "delete_text_diff",
                json!({"comparison_id":pair["comparison"]["comparison_id"]}),
            )
            .await;
    }
    server.stop().await;
}

#[tokio::test]
async fn invalid_modes_inputs_labels_and_handles_fail_at_the_public_boundary() {
    let server = Server::start(false).await;
    for version in ["2025-11-25", "2026-07-28"] {
        for (name, arguments) in [
            ("show_text_diff", json!({"before":"one"})),
            ("show_text_diff", json!({"after":"two"})),
            (
                "show_text_diff",
                json!({"before_label":"label without text"}),
            ),
            (
                "show_text_diff",
                json!({"comparison_id":"a".repeat(64),"before":"one","after":"two"}),
            ),
            (
                "show_text_diff",
                json!({"comparison_id":"a".repeat(64),"after_label":"label"}),
            ),
            (
                "compare_texts",
                json!({"before":"one","after":"two","before_label":"\nPRIVATE_INPUT_MARKER"}),
            ),
            (
                "compare_texts",
                json!({"before":"one","after":"two","after_label":"법".repeat(43)}),
            ),
            (
                "compare_texts",
                json!({"before":"one","after":"two","before_label":""}),
            ),
            (
                "compare_texts",
                json!({"before":"private\u{0}input","after":"two"}),
            ),
            (
                "compare_texts",
                json!({"before":"x".repeat(16 * 1024 + 1),"after":"two"}),
            ),
            (
                "compare_texts",
                json!({"before":"\n".repeat(100_001),"after":"two"}),
            ),
            (
                "compare_texts",
                json!({"before":format!("{}\n", "x".repeat(1023)).repeat(1025),"after":"two"}),
            ),
            ("compare_texts", json!({"before":5,"after":"two"})),
            (
                "compare_texts",
                json!({"before":"one","after":"two","git_option":"--external-diff"}),
            ),
            (
                "get_text_diff_page",
                json!({"comparison_id":"../private", "page":0}),
            ),
            (
                "get_text_diff_page",
                json!({"comparison_id":"a".repeat(64), "view":"unknown", "page":0}),
            ),
            (
                "get_text_diff_page",
                json!({"comparison_id":"a".repeat(64), "page":-1}),
            ),
            ("delete_text_diff", json!({"comparison_id":"A".repeat(64)})),
        ] {
            let invalid = server.call(version, name, arguments).await;
            assert_eq!(invalid["error"]["code"], -32602, "{name}: {invalid}");
            assert!(!invalid.to_string().contains("PRIVATE_INPUT_MARKER"));
        }
        let unknown = server
            .successful_call(
                version,
                "delete_text_diff",
                json!({"comparison_id":"a".repeat(64)}),
            )
            .await;
        assert_eq!(unknown["deleted"], true);
    }
    server.stop().await;
}

#[tokio::test]
async fn comparison_and_demo_resources_coexist_with_one_source_offer() {
    let server = Server::start(true).await;
    for version in ["2025-11-25", "2026-07-28"] {
        let resources = server.rpc(version, "resources/list", json!({})).await;
        assert_eq!(
            resources["result"]["resources"].as_array().unwrap().len(),
            2
        );
        for uri in [WIDGET_URI, openlegal_server::demo::WIDGET_URI] {
            let read = server
                .rpc(version, "resources/read", json!({"uri":uri}))
                .await;
            let resource = &read["result"]["contents"][0];
            assert_eq!(resource["mimeType"], "text/html;profile=mcp-app");
            assert!(resource["text"].as_str().unwrap().contains(ESCAPED_SOURCE));
            assert!(
                !resource["text"]
                    .as_str()
                    .unwrap()
                    .contains("__OPENLEGAL_SOURCE_URL__")
            );
            assert_eq!(resource["_meta"]["ui"]["csp"]["connectDomains"], json!([]));
        }
        let tools = server.rpc(version, "tools/list", json!({})).await;
        let names: Vec<_> = tools["result"]["tools"]
            .as_array()
            .unwrap()
            .iter()
            .map(|tool| tool["name"].as_str().unwrap())
            .collect();
        assert!(names.contains(&"compare_texts"));
        assert!(names.contains(&"demo_show_records"));
    }
    server.stop().await;
}

#[test]
fn comparison_asset_allowance_is_narrow_and_preserves_serialized_bounds() {
    let source = SourceOffer::new(SOURCE).unwrap();
    let final_size = 3 * 1024 * 1024;
    let replaced_size = HTML
        .replace("__OPENLEGAL_SOURCE_URL__", ESCAPED_SOURCE)
        .len();
    let html = format!("{HTML}{}", "x".repeat(final_size - replaced_size));
    assert!(openlegal_server::text_diff::widget_resources(html.clone(), &source).is_ok());
    assert!(openlegal_server::text_diff::widget_resources(format!("{html}x"), &source).is_err());
    assert!(openlegal_server::demo::widget_resources(html, &source).is_err());
    // Generic registration cannot acquire the comparison exception by URI alone.
    let mut generic = ResourceRegistry::new();
    assert!(
        generic
            .register(
                Resource::new(WIDGET_URI, "unprivileged asset")
                    .with_mime_type("text/html;profile=mcp-app"),
                ResourceContents::text("x".repeat(1024 * 1024 + 1), WIDGET_URI)
                    .with_mime_type("text/html;profile=mcp-app"),
            )
            .is_err()
    );
    let escaped = format!("{HTML}{}", "\u{1}".repeat(2 * 1024 * 1024));
    assert!(
        openlegal_server::text_diff::widget_resources(escaped, &source).is_err(),
        "raw size cannot bypass serialized resource bounds"
    );
}

#[tokio::test]
async fn heavily_escaped_source_and_change_pages_fit_complete_wire_budget() {
    let server = Server::start(false).await;
    let before = format!("{}\n", "\u{1}".repeat(16 * 1024)).repeat(5);
    let after = before.replace('\u{1}', "\u{2}");
    for version in ["2025-11-25", "2026-07-28"] {
        let summary = server
            .successful_call(
                version,
                "compare_texts",
                json!({"before":before,"after":after}),
            )
            .await;
        for view in ["changes", "before", "after"] {
            let page = server
                .successful_call(
                    version,
                    "get_text_diff_page",
                    json!({"comparison_id":summary["comparison_id"],"view":view,"page":0}),
                )
                .await;
            assert!(
                serde_json::to_vec(&page).unwrap().len() > 190 * 1024,
                "fixture must exercise a large escaped page"
            );
        }
        server
            .successful_call(
                version,
                "delete_text_diff",
                json!({"comparison_id":summary["comparison_id"]}),
            )
            .await;
    }
    server.stop().await;
}
