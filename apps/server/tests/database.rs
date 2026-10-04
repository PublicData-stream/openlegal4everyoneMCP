//! Fictional corpus through the real PostgreSQL, index, workers and MCP boundary.
#[path = "../../../test-support/postgres.rs"]
mod postgres;
use futures::FutureExt;
use openlegal_application::{
    Clock, SystemClock, database::Publication, persistence::PersistentStore,
};
use openlegal_domain::legal::{Dataset, LegalRecord, ObjectId};
use openlegal_server::{
    ServerBuilder,
    config::{AccessPolicy, DatabaseConfig, Limits, SourceOffer},
    corpus_runtime::CorpusRuntime,
    database::DatabaseTools,
    http::HttpEndpoint,
    registry::server_info_registry,
};
use serde_json::{Value, json};
use std::{collections::BTreeMap, panic::AssertUnwindSafe, time::Duration};
use tokio_util::sync::CancellationToken;
async fn call(url: &str, revision: &str, name: &str, arguments: Value) -> Result<Value, String> {
    rpc(
        url,
        revision,
        "tools/call",
        json!({"name":name,"arguments":arguments}),
    )
    .await
}

async fn rpc(url: &str, revision: &str, method: &str, mut params: Value) -> Result<Value, String> {
    let context = |stage: &str, error: String| format!("{revision} {method} {stage}: {error}");
    let name = params["name"].as_str().map(str::to_owned);
    if revision == "2026-07-28" {
        params["_meta"] = json!({"io.modelcontextprotocol/protocolVersion":revision,"io.modelcontextprotocol/clientInfo":{"name":"fictional-corpus-test","version":"1"},"io.modelcontextprotocol/clientCapabilities":{}});
    }
    let mut request = reqwest::Client::new()
        .post(url)
        .header("host", "database.test")
        .header("accept", "application/json, text/event-stream")
        .header("mcp-protocol-version", revision)
        .header("mcp-method", method)
        .timeout(Duration::from_secs(15))
        .json(&json!({"jsonrpc":"2.0","id":1,"method":method,"params":params}));
    if let Some(name) = name {
        request = request.header("mcp-name", name);
    }
    let response = request
        .send()
        .await
        .map_err(|e| context("send", format!("{e:?}")))?;
    if response.status() != 200 && !(revision == "2026-07-28" && response.status() == 400) {
        return Err(context("status", response.status().to_string()));
    }
    let text = response
        .text()
        .await
        .map_err(|e| context("body", format!("{e:?}")))?;
    if text.len() >= 1024 * 1024 {
        return Err(context("body", "response exceeds fixture limit".into()));
    }
    if text.trim_start().starts_with('{') {
        serde_json::from_str(&text).map_err(|e| context("JSON decode", e.to_string()))
    } else {
        for data in text.lines().filter_map(|line| line.strip_prefix("data:")) {
            let value: Value =
                serde_json::from_str(data).map_err(|e| context("SSE decode", e.to_string()))?;
            if value["id"] == 1 {
                return Ok(value);
            }
        }
        Err(context("SSE decode", "missing response with id 1".into()))
    }
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh PostgreSQL 18 environment"]
async fn corpus_tools_preserve_provenance_paging_search_and_checkpoint_diff() {
    let fixture = postgres::TestDatabase::new().await;
    let time = SystemClock::default().now();
    let persistent = fixture.open(time).await;
    let runtime = CorpusRuntime::open(
        &DatabaseConfig {
            auto_collection: true,
            blob_path: fixture.directory.path().join("corpus-blobs"),
            index_path: fixture.directory.path().join("corpus-index"),
            mecab_dictionary_path: std::env::var_os("OPENLEGAL_TEST_MECAB_DICTIONARY")
                .expect("PostgreSQL gate must provide the pinned MeCab-Ko dictionary")
                .into(),
            widget_html: "unused-fixture.html".into(),
            ingestion: None,
        },
        &persistent,
    )
    .await
    .unwrap();
    let object = ObjectId {
        jurisdiction: "kr".into(),
        provider: "fictional".into(),
        dataset: Dataset::NationalStatute,
        id: "one".into(),
    };
    let mut captures = Vec::new();
    for (revision, word) in [("r1", "before"), ("r2", "after")] {
        let body = format!("{word}\n{}", "Fictional line\n".repeat(4000));
        assert!(body.len() > 32768 && body.len() < 65536);
        let version = runtime
            .store
            .state(&object)
            .await
            .map(|s| s.version)
            .unwrap_or(0);
        let capture = runtime
            .store
            .publish(
                Publication {
                    record: LegalRecord {
                        object: object.clone(),
                        revision_id: revision.into(),
                        title: "Fictional 대한민국 ABC".into(),
                        body,
                        metadata: BTreeMap::new(),
                        publication_date: None,
                        effective_date: None,
                        source_url: "https://example.test/fictional".into(),
                        representation: "fictional_v1".into(),
                        sections: vec![],
                    },
                    raw: word.as_bytes().to_vec(),
                    additional_evidence: vec![],
                    processor_version: "fictional_v1".into(),
                    retrieved_at: time,
                    now: time,
                    expected_version: version,
                    install_head: true,
                    job_id: None,
                },
                CancellationToken::new(),
            )
            .await
            .unwrap();
        captures.push(capture);
    }
    let comparison = openlegal_server::text_diff::service(std::path::Path::new(env!(
        "CARGO_BIN_EXE_openlegal-server"
    )))
    .await
    .unwrap();
    let source = SourceOffer::new("https://example.test/source").unwrap();
    let mut registry = server_info_registry(source.clone()).unwrap();
    registry
        .register_module(DatabaseTools {
            demand: None,
            database: runtime.database.clone(),
            reader: runtime.reader.clone(),
            search: runtime.search.clone(),
            comparison: comparison.clone(),
            store: runtime.store.clone(),
        })
        .unwrap();
    registry
        .register_module(openlegal_server::text_diff::TextDiffTools {
            service: comparison.clone(),
        })
        .unwrap();
    let html = "<html><meta name=\"openlegal-source-url\" content=\"__OPENLEGAL_SOURCE_URL__\">Fictional corpus fixture</html>";
    let mut resources =
        openlegal_server::text_diff::widget_resources(html.into(), &source).unwrap();
    let widget = fixture.directory.path().join("database.html");
    tokio::fs::write(&widget, html).await.unwrap();
    resources
        .extend(
            openlegal_server::database::load_widget(&widget, &source)
                .await
                .unwrap(),
        )
        .unwrap();
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
            tls: None,
            edge_mtls: None,
            bind: "127.0.0.1:0".parse().unwrap(),
            access: AccessPolicy {
                allowed_hosts: vec!["database.test".into()],
                allowed_origins: vec![],
            },
        })
        .unwrap();
    let worker = runtime.clone();
    builder
        .register_worker("corpus", move |cancel| async move {
            let result = worker.run(cancel).await;
            if let Err(error) = &result {
                eprintln!("corpus worker failed: {error}");
            }
            result
        })
        .unwrap();
    builder
        .register_worker("text", move |cancel| async move {
            comparison.run(cancel).await.inspect_err(|error| {
                eprintln!("text worker failed: {error}");
            })?;
            Ok(())
        })
        .unwrap();
    let server = builder.bind().await.unwrap();
    let url = format!("http://{}/mcp", server.addresses()[0].1[0]);
    let shutdown = CancellationToken::new();
    let mut task = tokio::spawn(server.run(shutdown.clone()));
    let scenario = async {
        for protocol in ["2025-11-25", "2026-07-28"] {
            let listed = rpc(&url, protocol, "tools/list", json!({})).await?;
            let tools = listed["result"]["tools"].as_array().unwrap();
            for (name, supports_context) in [("database.query", false), ("database.rg", true)] {
                let tool = tools.iter().find(|tool| tool["name"] == name).unwrap();
                assert_eq!(
                    tool["inputSchema"]["properties"]
                        .get("context_lines")
                        .is_some(),
                    supports_context,
                    "{tool}"
                );
            }
            // Settle a fictional request in storage; no scheduler/provider work
            // runs. Then verify retained reasons through both public tools.
            runtime
                .store
                .heartbeat_collection_scheduler()
                .await
                .unwrap();
            let target = json!({"target":{"kind":"object","object":{"jurisdiction":"kr","provider":"law_go_kr","dataset":"national_statute","id":"123"}}});
            let receipt = call(
                &url,
                protocol,
                "database.request_collection",
                target.clone(),
            )
            .await?;
            let request_id = receipt["result"]["structuredContent"]["request_id"]
                .as_str()
                .unwrap();
            if receipt["result"]["structuredContent"]["status"] == "queued" {
                let (claimed, _) = runtime
                    .store
                    .claim_collection_request()
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(claimed, request_id);
                runtime
                    .store
                    .settle_collection_request_with_reason(
                        request_id,
                        "skipped",
                        Some("already_fresh"),
                    )
                    .await
                    .unwrap();
            }
            let status = call(
                &url,
                protocol,
                "database.collection_status",
                json!({"request_id":request_id}),
            )
            .await?;
            assert_eq!(status["result"]["structuredContent"]["status"], "skipped");
            assert_eq!(
                status["result"]["structuredContent"]["reason"],
                "already_fresh"
            );
            let deduplicated = call(&url, protocol, "database.request_collection", target).await?;
            assert_eq!(
                deduplicated["result"]["structuredContent"]["request_id"],
                request_id
            );
            assert_eq!(
                deduplicated["result"]["structuredContent"]["reason"],
                "already_fresh"
            );
            let get = call(&url, protocol, "database.get", json!({"object":object})).await?;
            assert!(get["error"].is_null(), "{get}");
            assert_ne!(get["result"]["isError"], true, "{get}");
            let page = &get["result"]["structuredContent"];
            assert_eq!(page["metadata"]["revision_id"], "r2");
            assert_eq!(page["metadata"]["capture_id"], captures[1].capture_id);
            assert_eq!(page["metadata"]["freshness"]["state"], "fresh");
            assert_eq!(page["offset"], 0);
            assert_eq!(page["next_offset"], 32768);
            assert_eq!(page["text"].as_str().unwrap().len(), 32768);
            let next = call(
                &url,
                protocol,
                "database.get",
                json!({
                    "object":object,
                    "selector":{"kind":"capture","id":page["metadata"]["capture_id"]},
                    "session":page["session"],
                    "offset":page["next_offset"]
                }),
            )
            .await?;
            assert_ne!(next["result"]["isError"], true, "{next}");
            let next = &next["result"]["structuredContent"];
            assert_eq!(next["session"], page["session"]);
            assert_eq!(next["metadata"]["capture_id"], captures[1].capture_id);
            assert_eq!(next["metadata"]["revision_id"], "r2");
            assert!(next["metadata"]["freshness"].is_null());
            assert_eq!(next["offset"], 32768);
            assert!(next["next_offset"].is_null());
            assert_eq!(
                format!(
                    "{}{}",
                    page["text"].as_str().unwrap(),
                    next["text"].as_str().unwrap()
                ),
                captures[1].record.body,
            );
            let metadata = call(
                &url,
                protocol,
                "database.get_metadata",
                json!({"object":object}),
            )
            .await?;
            assert_eq!(
                metadata["result"]["structuredContent"]["capture_id"],
                captures[1].capture_id
            );
            assert!(
                metadata["result"]["structuredContent"]
                    .get("body")
                    .is_none()
            );
            let history = call(
                &url,
                protocol,
                "database.history",
                json!({"object":object,"kind":"revisions"}),
            )
            .await?;
            let entries = history["result"]["structuredContent"]["entries"]
                .as_array()
                .unwrap();
            assert_eq!(entries.len(), 2);
            for capture in &captures {
                assert!(
                    entries
                        .iter()
                        .any(|entry| entry["revision_id"] == capture.record.revision_id
                            && entry["capture_id"] == capture.capture_id),
                    "{history}"
                );
            }
            let diff = call(
                &url, protocol, "database.diff",
                json!({"object":object,"before":{"kind":"revision","id":"r1"},"after":{"kind":"revision","id":"r2"}}),
            ).await?;
            assert_ne!(diff["result"]["isError"], true, "{diff}");
            let diff = &diff["result"]["structuredContent"];
            for (side, capture) in [("before", &captures[0]), ("after", &captures[1])] {
                assert_eq!(diff[side]["capture_id"], capture.capture_id);
                assert_eq!(diff[side]["revision_id"], capture.record.revision_id);
            }
            assert_eq!(diff["comparison"]["equal"], false);
            assert_eq!(diff["comparison"]["additions"], 1);
            assert_eq!(diff["comparison"]["deletions"], 1);
            let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
            loop {
                let result =
                    call(&url, protocol, "database.query", json!({"query":"\"ABC\""})).await?;
                assert_ne!(result["result"]["isError"], true, "{result}");
                let hits = result["result"]["structuredContent"]["hits"]
                    .as_array()
                    .unwrap();
                if !hits.is_empty() && result["result"]["structuredContent"]["index_lag"] == 0 {
                    assert_eq!(hits.len(), 1);
                    assert_eq!(hits[0]["revision_id"], "r2");
                    assert_eq!(hits[0]["capture_id"], captures[1].capture_id);
                    assert_eq!(hits[0]["match_scope"], "object");
                    break;
                }
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "{protocol} index catch-up: {result}"
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            let scoped = call(
                &url,
                protocol,
                "database.query",
                json!({"query":"in:title:ABC","limit":1}),
            )
            .await?;
            assert_eq!(
                scoped["result"]["structuredContent"]["hits"]
                    .as_array()
                    .unwrap()
                    .len(),
                1,
                "{scoped}"
            );
            let shorthand = call(
                &url,
                protocol,
                "database.query",
                json!({"query":"title:ABC"}),
            )
            .await?;
            assert_eq!(shorthand["error"]["code"], -32602, "{shorthand}");
            assert!(
                shorthand["error"]["message"]
                    .as_str()
                    .unwrap()
                    .contains("in:title:"),
                "{shorthand}"
            );
            let exact = call(
                &url,
                protocol,
                "database.query",
                json!({"query":"abc","literal":true}),
            )
            .await?;
            assert!(
                exact["result"]["structuredContent"]["hits"]
                    .as_array()
                    .unwrap()
                    .is_empty(),
                "{exact}"
            );
            let insensitive = call(
                &url,
                protocol,
                "database.query",
                json!({"query":"abc","literal":true,"ignore_case":true}),
            )
            .await?;
            assert_eq!(
                insensitive["result"]["structuredContent"]["hits"]
                    .as_array()
                    .unwrap()
                    .len(),
                1,
                "{insensitive}"
            );
            let literal_hit = &insensitive["result"]["structuredContent"]["hits"][0];
            assert_eq!(literal_hit["match_scope"], "object");
            assert_eq!(literal_hit["section"], "object");
            assert_eq!(literal_hit["line"], 0);
            assert_eq!(literal_hit["excerpt_section"], "title");
            assert!(literal_hit["text"].as_str().unwrap().contains("ABC"));
            for context in [0, 1] {
                let unsupported = call(
                    &url,
                    protocol,
                    "database.query",
                    json!({"query":"ABC", "context_lines":context}),
                )
                .await?;
                assert_eq!(unsupported["error"]["code"], -32602, "{unsupported}");
            }
            let invalid_mode = call(
                &url,
                protocol,
                "database.query",
                json!({"query":"ABC","ignore_case":true}),
            )
            .await?;
            assert_eq!(invalid_mode["error"]["code"], -32602, "{invalid_mode}");
            for _ in 0..34 {
                let terminal = call(
                    &url,
                    protocol,
                    "database.query",
                    json!({"query":"\"missing fictional phrase\""}),
                )
                .await?;
                assert!(terminal["error"].is_null(), "{terminal}");
                assert!(
                    terminal["result"]["structuredContent"]["next_cursor"].is_null(),
                    "{terminal}"
                );
            }
            let regex = call(&url, protocol, "database.rg", json!({"query":"^after$"})).await?;
            let hits = regex["result"]["structuredContent"]["hits"]
                .as_array()
                .unwrap();
            assert_eq!(hits.len(), 1);
            let hit = &hits[0];
            assert_eq!(hit["revision_id"], "r2");
            assert_eq!(hit["capture_id"], captures[1].capture_id);
            assert_eq!(hit["section"], "body");
            assert_eq!(hit["line"], 1);
            assert_eq!(hit["text"], "after\n");
            assert_eq!(hit["byte_start"], 0);
            assert_eq!(hit["byte_end"], 5);
            let context = call(
                &url,
                protocol,
                "database.rg",
                json!({"query":"^after$", "context_lines":1}),
            )
            .await?;
            let context_hit = &context["result"]["structuredContent"]["hits"][0];
            assert_eq!(context_hit["text"], "after\nFictional line\n");
            assert_eq!(context_hit["byte_start"], 0);
            assert_eq!(context_hit["byte_end"], 5);
            // Rejected patterns must not consume the 32 retained search-session slots.
            for _ in 0..33 {
                let invalid = call(&url, protocol, "database.rg", json!({"query":"(["})).await?;
                assert!(invalid.get("error").is_none(), "{invalid}");
                assert_eq!(invalid["result"]["isError"], true, "{invalid}");
                assert_eq!(
                    invalid["result"]["structuredContent"],
                    json!({"code":"invalid_regex","message":"The regular expression is invalid."}),
                    "{invalid}"
                );
            }
            let literal = call(
                &url,
                protocol,
                "database.rg",
                json!({"query":"([","literal":true}),
            )
            .await?;
            assert_ne!(literal["result"]["isError"], true, "{literal}");
            let still_usable =
                call(&url, protocol, "database.rg", json!({"query":"^after$"})).await?;
            assert_ne!(still_usable["result"]["isError"], true, "{still_usable}");
        }
        Ok::<(), String>(())
    };
    let scenario = AssertUnwindSafe(scenario).catch_unwind();
    let (scenario_result, server_result) = tokio::select! {
        result = &mut task => (
            Ok(Err(format!("server exited before the scenario completed: {result:?}"))),
            Some(result),
        ),
        result = scenario => (result, None),
    };
    shutdown.cancel();
    let server_result = match server_result {
        Some(result) => Ok(result),
        None => tokio::time::timeout(Duration::from_secs(20), &mut task).await,
    };
    if server_result.is_err() {
        task.abort();
        let _ = (&mut task).await;
    }
    let runtime_result = tokio::time::timeout(Duration::from_secs(15), runtime.close()).await;
    let persistent_result = tokio::time::timeout(Duration::from_secs(15), persistent.close()).await;
    // Report cleanup alongside the original failure, including assertion panics.
    eprintln!(
        "server: {server_result:?}; corpus close: {runtime_result:?}; persistence close: {persistent_result:?}"
    );
    match scenario_result {
        Err(panic) => std::panic::resume_unwind(panic),
        Ok(Err(error)) => panic!("{error}"),
        Ok(Ok(())) => {}
    }
    server_result.unwrap().unwrap().unwrap();
    runtime_result.unwrap().unwrap();
    persistent_result.unwrap().unwrap();
}
