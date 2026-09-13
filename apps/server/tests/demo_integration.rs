//! Cross-transport acceptance with a real isolated HTTP upstream and no legal data.
#[path = "../../../crates/adapters/examples/support/mod.rs"]
mod mock;
#[path = "../../../test-support/postgres.rs"]
mod postgres;
use openlegal_server::{
    ServerBuilder,
    config::{AccessPolicy, Limits},
    demo::{DemoTools, WIDGET_URI},
    framing::{FrameReader, write_json},
    http::HttpEndpoint,
    webtransport::WebTransportEndpoint,
};
use serde_json::{Value, json};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use wtransport::{ClientConfig, Endpoint, tls::rustls};

fn params(version: &str, token: &str, mut value: Value) -> Value {
    value["_meta"] = json!({"progressToken":token});
    if version == "2026-07-28" {
        value["_meta"]["io.modelcontextprotocol/protocolVersion"] = json!(version);
        value["_meta"]["io.modelcontextprotocol/clientInfo"] =
            json!({"name":"demo-acceptance","version":"1"});
        value["_meta"]["io.modelcontextprotocol/clientCapabilities"] = json!({});
    }
    value
}
async fn http(url: &str, version: &str, method: &str, arguments: Value) -> Vec<Value> {
    let mut request = reqwest::Client::new()
        .post(url)
        .header("host", "demo.test")
        .header("accept", "application/json, text/event-stream")
        .header("mcp-protocol-version", version)
        .header("mcp-method", method);
    if let Some(name) = arguments
        .get("name")
        .or_else(|| arguments.get("uri"))
        .and_then(Value::as_str)
    {
        request = request.header("mcp-name", name);
    }
    let response = request
        .json(&json!({"jsonrpc":"2.0","id":10,"method":method,"params":arguments}))
        .send()
        .await
        .unwrap();
    let status = response.status();
    let body = response.text().await.unwrap();
    assert_eq!(status, 200, "{body}");
    if body.trim_start().starts_with('{') {
        return vec![serde_json::from_str(&body).unwrap()];
    }
    body.lines()
        .filter_map(|line| line.strip_prefix("data:"))
        .map(|line| serde_json::from_str(line.trim()).unwrap())
        .collect()
}
async fn wt_reply(reader: &mut FrameReader<wtransport::RecvStream>, id: u64) -> Vec<Value> {
    let mut messages = Vec::new();
    loop {
        let frame = reader.read().await.unwrap().unwrap();
        let value: Value = serde_json::from_slice(&frame.bytes).unwrap();
        let done = value.get("id") == Some(&json!(id));
        messages.push(value);
        assert!(messages.len() <= 8);
        if done {
            return messages;
        }
    }
}
fn assert_progress(messages: &[Value]) {
    assert!(messages.len() >= 2, "{messages:?}");
    let mut last = 0.0;
    for message in &messages[..messages.len() - 1] {
        assert_eq!(message["method"], "notifications/progress");
        let current = message["params"]["progress"].as_f64().unwrap();
        assert!(current > last);
        last = current;
    }
    assert!(messages.last().unwrap().get("result").is_some());
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh PostgreSQL 18 environment"]
async fn shared_http_and_webtransport_retrieval_progress_and_resources() {
    for version in ["2026-07-28", "2025-11-25"] {
        let count = Arc::new(AtomicUsize::new(0));
        let counter = count.clone();
        let mock = mock::router().layer(axum::middleware::from_fn(
            move |request: axum::extract::Request, next: axum::middleware::Next| {
                let counter = counter.clone();
                async move {
                    counter.fetch_add(1, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    next.run(request).await
                }
            },
        ));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let source_url = format!("http://{}", listener.local_addr().unwrap());
        let stop = CancellationToken::new();
        let signal = stop.clone();
        let upstream = tokio::spawn(async move {
            axum::serve(listener, mock)
                .with_graceful_shutdown(signal.cancelled_owned())
                .await
                .unwrap();
        });
        let dir = tempfile::tempdir().unwrap();
        let database = postgres::TestDatabase::new().await;
        let store = database
            .open({
                use openlegal_application::Clock;
                openlegal_application::SystemClock::default().now()
            })
            .await;
        let service = openlegal_server::demo::service_with_store(&source_url, store).unwrap();
        let mut registry = openlegal_server::registry::server_info_registry(
            openlegal_server::config::SourceOffer::new("https://source.test/running").unwrap(),
        )
        .unwrap();
        registry
            .register_module(DemoTools {
                comparison: None,
                service: service.clone(),
            })
            .unwrap();
        let rcgen::CertifiedKey { cert, signing_key } =
            rcgen::generate_simple_self_signed(vec!["localhost".into(), "127.0.0.1".into()])
                .unwrap();
        let certificate = dir.path().join("cert.pem");
        let key = dir.path().join("key.pem");
        std::fs::write(&certificate, cert.pem()).unwrap();
        std::fs::write(&key, signing_key.serialize_pem()).unwrap();
        let port = std::net::UdpSocket::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let mut builder = ServerBuilder::new(registry, Limits::default(), openlegal_server::config::SourceOffer::new("https://source.test/running").unwrap()).with_resources(
            openlegal_server::demo::widget_resources(
                "<html><meta name=\"openlegal-source-url\" content=\"__OPENLEGAL_SOURCE_URL__\">synthetic widget fixture</html>".into(),
                &openlegal_server::config::SourceOffer::new("https://source.test/running").unwrap(),
            )
            .unwrap(),
        );
        builder
            .register_endpoint(HttpEndpoint {
                bind: "127.0.0.1:0".parse().unwrap(),
                access: AccessPolicy {
                    allowed_hosts: vec!["demo.test".into()],
                    allowed_origins: vec![],
                },
            })
            .unwrap();
        builder
            .register_endpoint(WebTransportEndpoint {
                bind: format!("127.0.0.1:{port}").parse().unwrap(),
                certificate,
                private_key: key,
                access: AccessPolicy {
                    allowed_hosts: vec![format!("127.0.0.1:{port}")],
                    allowed_origins: vec![],
                },
            })
            .unwrap();
        let worker = service.clone();
        builder
            .register_worker("retrieval", move |shutdown| async move {
                worker.run(shutdown).await?;
                Ok(())
            })
            .unwrap();
        let server = builder.bind().await.unwrap();
        let http_url = format!(
            "http://{}/mcp",
            server
                .addresses()
                .iter()
                .find(|(name, _)| name == "http")
                .unwrap()
                .1[0]
        );
        let shutdown = CancellationToken::new();
        let running = tokio::spawn(server.run(shutdown.clone()));
        let mut roots = rustls::RootCertStore::empty();
        roots.add(cert.der().clone()).unwrap();
        let mut tls = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
        tls.alpn_protocols = vec![wtransport::tls::WEBTRANSPORT_ALPN.to_vec()];
        let client = Endpoint::client(
            ClientConfig::builder()
                .with_bind_default()
                .with_custom_tls(tls)
                .build(),
        )
        .unwrap();
        let connection = client
            .connect(format!("https://127.0.0.1:{port}/mcp-wt/v1"))
            .await
            .unwrap();
        let (mut tx, rx) = connection.open_bi().await.unwrap().await.unwrap();
        let budget = Arc::new(Semaphore::new(4 * 1024 * 1024));
        let mut rx = FrameReader::new(
            rx,
            1024 * 1024,
            budget.clone(),
            Duration::from_secs(5),
            Duration::from_secs(5),
        );
        let (discovery_method, discovery_params) = if version == "2025-11-25" {
            (
                "initialize",
                json!({"protocolVersion":version,"capabilities":{},"clientInfo":{"name":"test","version":"1"}}),
            )
        } else {
            ("server/discover", params(version, "discovery", json!({})))
        };
        let http_discovery = http(
            &http_url,
            version,
            discovery_method,
            discovery_params.clone(),
        )
        .await;
        write_json(
            &mut tx,
            &json!({"jsonrpc":"2.0","id":1,"method":discovery_method,"params":discovery_params}),
            1024 * 1024,
            &budget,
            Duration::from_secs(5),
        )
        .await
        .unwrap();
        let wt_discovery = wt_reply(&mut rx, 1).await;
        let instructions = http_discovery.last().unwrap()["result"]["instructions"]
            .as_str()
            .unwrap();
        assert_eq!(
            wt_discovery.last().unwrap()["result"]["instructions"],
            instructions
        );
        assert!(instructions.contains("AGPL-3.0-only"));
        assert!(instructions.contains("https://www.gnu.org/licenses/agpl-3.0.html"));
        assert!(instructions.contains("https://source.test/running"));
        if version == "2025-11-25" {
            write_json(
                &mut tx,
                &json!({"jsonrpc":"2.0","method":"notifications/initialized"}),
                1024 * 1024,
                &budget,
                Duration::from_secs(5),
            )
            .await
            .unwrap();
        }
        let info_params = params(
            version,
            "source-info",
            json!({"name":"server_info", "arguments":{}}),
        );
        let http_info = http(&http_url, version, "tools/call", info_params.clone()).await;
        write_json(
            &mut tx,
            &json!({"jsonrpc":"2.0","id":12,"method":"tools/call","params":info_params}),
            1024 * 1024,
            &budget,
            Duration::from_secs(5),
        )
        .await
        .unwrap();
        let wt_info = wt_reply(&mut rx, 12).await;
        let info = &http_info.last().unwrap()["result"]["structuredContent"];
        assert_eq!(
            &wt_info.last().unwrap()["result"]["structuredContent"],
            info
        );
        assert_eq!(info["license"], "AGPL-3.0-only");
        assert_eq!(
            info["licenseUrl"],
            "https://www.gnu.org/licenses/agpl-3.0.html"
        );
        assert_eq!(info["sourceUrl"], "https://source.test/running");
        assert_eq!(
            count.load(Ordering::SeqCst),
            0,
            "source offers must not fetch upstream data"
        );
        let input = json!({"name":"demo_search_records","arguments":{"source":"layout_a","query":"","page":0,"page_size":5}});
        let http_future = http(
            &http_url,
            version,
            "tools/call",
            params(version, "http", input.clone()),
        );
        let wt_future = async {
            write_json(&mut tx,&json!({"jsonrpc":"2.0","id":2,"method":"tools/call","params":params(version,"wt",input)}),1024*1024,&budget,Duration::from_secs(5)).await.unwrap();
            wt_reply(&mut rx, 2).await
        };
        let (http_messages, wt_messages) = tokio::join!(http_future, wt_future);
        assert_progress(&http_messages);
        assert_progress(&wt_messages);
        let a = &http_messages.last().unwrap()["result"]["structuredContent"];
        let b = &wt_messages.last().unwrap()["result"]["structuredContent"];
        assert_eq!(a, b);
        assert_eq!(a["data"]["total"], 12);
        assert_eq!(a["synthetic"], true);
        assert_eq!(
            count.load(Ordering::SeqCst),
            1,
            "cross-transport callers must share a refresh"
        );
        let cached = http(
            &http_url,
            version,
            "tools/call",
            params(
                version,
                "cached",
                json!({"name":"demo_search_records","arguments":{"source":"layout_a"}}),
            ),
        )
        .await;
        assert_eq!(
            cached.last().unwrap()["result"]["structuredContent"]["data"]["total"],
            12
        );
        assert_eq!(count.load(Ordering::SeqCst), 1);
        let read = http(
            &http_url,
            version,
            "resources/read",
            params(version, "resource", json!({"uri":WIDGET_URI})),
        )
        .await;
        assert_eq!(
            read.last().unwrap()["result"]["contents"][0]["mimeType"],
            "text/html;profile=mcp-app"
        );
        let widget_html = read.last().unwrap()["result"]["contents"][0]["text"]
            .as_str()
            .unwrap();
        assert!(widget_html.contains("content=\"https://source.test/running\""));
        assert!(!widget_html.contains("__OPENLEGAL_SOURCE_URL__"));
        write_json(&mut tx, &json!({"jsonrpc":"2.0","id":13,"method":"resources/read","params":params(version,"wt-resource",json!({"uri":WIDGET_URI}))}),
            1024 * 1024, &budget, Duration::from_secs(5)).await.unwrap();
        let wt_resource = wt_reply(&mut rx, 13).await;
        assert_eq!(
            wt_resource.last().unwrap()["result"]["contents"][0]["text"],
            widget_html
        );
        let show=http(&http_url,version,"tools/call",params(version,"show",json!({"name":"demo_show_records","arguments":{"records":[{"source":"layout_a","id":"001"}]}}))).await;
        assert_eq!(
            show.last().unwrap()["result"]["structuredContent"]["records"][0]["data"]["id"],
            "001"
        );
        let snapshot_id = a["snapshot"]["snapshot_id"].as_str().unwrap();
        let history_query =
            json!({"operation":"search", "source":"layout_a", "query":"", "page":0, "page_size":5});
        let before_history = count.load(Ordering::SeqCst);
        let listing = http(
            &http_url,
            version,
            "tools/call",
            params(
                version,
                "history-list",
                json!({"name":"demo_list_snapshots", "arguments":{"query":history_query}}),
            ),
        )
        .await;
        assert_eq!(
            listing.last().unwrap()["result"]["structuredContent"]["snapshots"][0]["snapshot_id"],
            snapshot_id
        );
        let exact_params = params(
            version,
            "history-get",
            json!({"name":"demo_get_snapshot", "arguments":{"query":history_query, "snapshot_id":snapshot_id}}),
        );
        let exact = http(&http_url, version, "tools/call", exact_params.clone()).await;
        assert_progress(&exact);
        let historical = &exact.last().unwrap()["result"]["structuredContent"];
        assert_eq!(historical["historical"], true);
        assert_eq!(historical["data"], a["data"]);
        assert!(historical.get("freshness").is_none());
        write_json(
            &mut tx,
            &json!({"jsonrpc":"2.0","id":14,"method":"tools/call","params":exact_params}),
            1024 * 1024,
            &budget,
            Duration::from_secs(5),
        )
        .await
        .unwrap();
        let wt_history = wt_reply(&mut rx, 14).await;
        assert_progress(&wt_history);
        assert_eq!(
            &wt_history.last().unwrap()["result"]["structuredContent"],
            historical
        );
        assert_eq!(
            count.load(Ordering::SeqCst),
            before_history,
            "history never fetches upstream"
        );
        connection.close(0u32.into(), b"done");
        client.close(0u32.into(), b"done");
        shutdown.cancel();
        running.await.unwrap().unwrap();
        let reopened = database
            .open({
                use openlegal_application::Clock;
                openlegal_application::SystemClock::default().now()
            })
            .await;
        let restarted = openlegal_server::demo::service_with_store(&source_url, reopened).unwrap();
        let disk_hit = restarted
            .retrieve(
                openlegal_domain::Query::Search {
                    source: "layout_a".into(),
                    query: "".into(),
                    page: 0,
                    page_size: 5,
                },
                openlegal_domain::FreshnessRequirement::FreshOnly,
                CancellationToken::new(),
                None,
            )
            .await
            .unwrap();
        assert_eq!(disk_hit.snapshot.unwrap().snapshot_id, snapshot_id);
        assert_eq!(
            count.load(Ordering::SeqCst),
            before_history,
            "restart reuses fresh durable capture"
        );
        restarted.shutdown().await.unwrap();
        stop.cancel();
        upstream.await.unwrap();
        assert!(
            service
                .retrieve(
                    openlegal_domain::Query::Get {
                        source: "layout_a".into(),
                        id: "001".into()
                    },
                    openlegal_domain::FreshnessRequirement::AllowStale,
                    CancellationToken::new(),
                    None
                )
                .await
                .is_err()
        );
    }
}
