//! Fictional corpus through the real PostgreSQL, index, workers and MCP boundary.
#[path = "../../../test-support/postgres.rs"]
mod postgres;
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
use std::{collections::BTreeMap, time::Duration};
use tokio_util::sync::CancellationToken;
async fn call(url: &str, revision: &str, name: &str, arguments: Value) -> Value {
    let mut params = json!({"name":name,"arguments":arguments});
    if revision == "2026-07-28" {
        params["_meta"] = json!({"io.modelcontextprotocol/protocolVersion":revision,"io.modelcontextprotocol/clientInfo":{"name":"fictional-corpus-test","version":"1"},"io.modelcontextprotocol/clientCapabilities":{}});
    }
    let response = reqwest::Client::new()
        .post(url)
        .header("host", "database.test")
        .header("accept", "application/json, text/event-stream")
        .header("mcp-protocol-version", revision)
        .header("mcp-method", "tools/call")
        .header("mcp-name", name)
        .timeout(Duration::from_secs(15))
        .json(&json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":params}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let text = response.text().await.unwrap();
    assert!(text.len() < 1024 * 1024);
    if text.trim_start().starts_with('{') {
        serde_json::from_str(&text).unwrap()
    } else {
        text.lines()
            .filter_map(|l| l.strip_prefix("data:"))
            .filter_map(|l| serde_json::from_str::<Value>(l).ok())
            .find(|v| v["id"] == 1)
            .unwrap()
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
                        body: format!("{word}\n{}", "Fictional line\n".repeat(4000)),
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
            database: runtime.database.clone(),
            reader: runtime.reader.clone(),
            search: runtime.search.clone(),
            comparison: comparison.clone(),
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
            bind: "127.0.0.1:0".parse().unwrap(),
            access: AccessPolicy {
                allowed_hosts: vec!["database.test".into()],
                allowed_origins: vec![],
            },
        })
        .unwrap();
    let worker = runtime.clone();
    builder
        .register_worker(
            "corpus",
            move |cancel| async move { worker.run(cancel).await },
        )
        .unwrap();
    builder
        .register_worker("text", move |cancel| async move {
            comparison.run(cancel).await?;
            Ok(())
        })
        .unwrap();
    let server = builder.bind().await.unwrap();
    let url = format!("http://{}/mcp", server.addresses()[0].1[0]);
    let shutdown = CancellationToken::new();
    let task = tokio::spawn(server.run(shutdown.clone()));
    for protocol in ["2025-11-25", "2026-07-28"] {
        let get = call(&url, protocol, "database.get", json!({"object":object})).await;
        assert!(get["error"].is_null(), "{get}");
        assert_ne!(get["result"]["isError"], true, "{get}");
        let page = &get["result"]["structuredContent"];
        assert_eq!(page["metadata"]["revision_id"], "r2");
        assert_eq!(page["metadata"]["freshness"]["state"], "fresh");
        assert_eq!(page["text"].as_str().unwrap().len(), 32768);
        let next=call(&url,protocol,"database.get",json!({"object":object,"selector":{"kind":"capture","id":page["metadata"]["capture_id"]},"session":page["session"],"offset":page["next_offset"]})).await;
        assert_ne!(next["result"]["isError"], true, "{next}");
        assert!(next["result"]["structuredContent"]["metadata"]["freshness"].is_null());
        let metadata = call(
            &url,
            protocol,
            "database.get_metadata",
            json!({"object":object}),
        )
        .await;
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
        .await;
        assert_eq!(
            history["result"]["structuredContent"]["entries"]
                .as_array()
                .unwrap()
                .len(),
            2
        );
        let diff=call(&url,protocol,"database.diff",json!({"object":object,"before":{"kind":"revision","id":"r1"},"after":{"kind":"revision","id":"r2"}})).await;
        assert_ne!(diff["result"]["isError"], true, "{diff}");
        assert_eq!(
            diff["result"]["structuredContent"]["before"]["capture_id"],
            captures[0].capture_id
        );
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            let result = call(&url, protocol, "database.query", json!({"query":"\"ABC\""})).await;
            assert_ne!(result["result"]["isError"], true, "{result}");
            let hits = result["result"]["structuredContent"]["hits"]
                .as_array()
                .unwrap();
            if !hits.is_empty() && result["result"]["structuredContent"]["index_lag"] == 0 {
                assert_eq!(hits[0]["revision_id"], "r2");
                assert_eq!(hits[0]["match_scope"], "object");
                break;
            }
            assert!(tokio::time::Instant::now() < deadline);
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let regex = call(&url, protocol, "database.rg", json!({"query":"^after$"})).await;
        assert_eq!(regex["result"]["structuredContent"]["hits"][0]["line"], 1);
    }
    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(15), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    runtime.close().await.unwrap();
    persistent.close().await.unwrap();
}
