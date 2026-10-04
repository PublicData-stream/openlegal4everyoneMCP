//! Automatic admission through actual transports, PostgreSQL and an empty local
//! corpus. Ingestion is disabled; these fixtures never call a legal provider.
#[path = "../../../test-support/postgres.rs"]
mod postgres;
use futures::FutureExt;
use openlegal_application::{
    Clock, SystemClock, citation::CitationService, demand_collection::DemandCollectionCoordinator,
    persistence::PersistentStore,
};
use openlegal_server::{
    ServerBuilder,
    citation::CitationTools,
    config::{AccessPolicy, DatabaseConfig, Limits, SourceOffer},
    corpus_runtime::CorpusRuntime,
    database::DatabaseTools,
    framing::{FrameReader, write_json},
    http::HttpEndpoint,
    registry::server_info_registry,
    webtransport::{PATH, WebTransportEndpoint},
};
use serde_json::{Value, json};
use std::{panic::AssertUnwindSafe, sync::Arc, time::Duration};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use wtransport::{ClientConfig, Endpoint as QuicEndpoint, endpoint::ConnectOptions, tls::rustls};

const REVISION: &str = "2026-07-28";
const MAX_FRAME: usize = 1024 * 1024;

enum Wire {
    Http(String),
    WebTransport {
        tx: wtransport::SendStream,
        rx: Box<FrameReader<wtransport::RecvStream>>,
    },
}
impl Wire {
    async fn rpc(&mut self, id: u64, method: &str, mut params: Value) -> (Option<u16>, Value) {
        let name = params["name"].as_str().map(str::to_owned);
        params["_meta"] = json!({"io.modelcontextprotocol/protocolVersion":REVISION,"io.modelcontextprotocol/clientInfo":{"name":"demand-fixture","version":"1"},"io.modelcontextprotocol/clientCapabilities":{}});
        let body = json!({"jsonrpc":"2.0","id":id,"method":method,"params":params});
        match self {
            Self::Http(url) => {
                let mut request = reqwest::Client::new()
                    .post(url.as_str())
                    .header("host", "demand.test")
                    .header("accept", "application/json, text/event-stream")
                    .header("mcp-protocol-version", REVISION)
                    .header("mcp-method", method)
                    .timeout(Duration::from_secs(15))
                    .json(&body);
                if let Some(name) = name {
                    request = request.header("mcp-name", name);
                }
                let response = request.send().await.unwrap();
                let status = response.status().as_u16();
                let text = response.text().await.unwrap();
                assert!(text.len() < MAX_FRAME);
                let response = if text.trim_start().starts_with('{') {
                    serde_json::from_str(&text).unwrap()
                } else {
                    text.lines()
                        .filter_map(|line| line.strip_prefix("data:"))
                        .map(|line| serde_json::from_str::<Value>(line).unwrap())
                        .find(|value| value["id"] == id)
                        .expect("matching SSE response")
                };
                (Some(status), response)
            }
            Self::WebTransport { tx, rx } => {
                write_json(
                    tx,
                    &body,
                    MAX_FRAME,
                    &Arc::new(Semaphore::new(MAX_FRAME * 2)),
                    Duration::from_secs(15),
                )
                .await
                .unwrap();
                let response =
                    serde_json::from_slice(&rx.read().await.unwrap().unwrap().bytes).unwrap();
                (None, response)
            }
        }
    }
    async fn call(&mut self, id: u64, name: &str, arguments: Value) -> Value {
        let (status, response) = self
            .rpc(id, "tools/call", json!({"name":name,"arguments":arguments}))
            .await;
        assert!(status.is_none() || status == Some(200), "{response}");
        response
    }
}

fn object(id: &str) -> Value {
    json!({"jurisdiction":"kr","provider":"law_go_kr","dataset":"national_statute","id":id})
}

async fn fixture_sql(fixture: &postgres::TestDatabase, sql: &str) -> String {
    let container = std::env::var("OPENLEGAL_TEST_POSTGRES_CONTAINER").unwrap();
    let database = url::Url::parse(&fixture.url)
        .unwrap()
        .path()
        .trim_start_matches('/')
        .to_owned();
    let output = tokio::time::timeout(
        Duration::from_secs(15),
        tokio::process::Command::new("docker")
            .args([
                "exec", &container, "psql", "-U", "postgres", "-d", &database, "-At", "-c", sql,
            ])
            .kill_on_drop(true)
            .output(),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(output.status.success(), "isolated fixture SQL failed");
    String::from_utf8(output.stdout).unwrap().trim().to_owned()
}
async fn queued_count(fixture: &postgres::TestDatabase) -> i64 {
    fixture_sql(fixture, "SELECT count(*) FROM openlegal.collection_request")
        .await
        .parse()
        .unwrap()
}

async fn exercise(
    wire: &mut Wire,
    runtime: &CorpusRuntime,
    fixture: &postgres::TestDatabase,
    enabled: bool,
    index: u64,
) {
    runtime
        .store
        .heartbeat_collection_scheduler()
        .await
        .unwrap();
    let (_, listed) = wire.rpc(1, "tools/list", json!({})).await;
    let tools = listed["result"]["tools"].as_array().unwrap();
    for name in [
        "database.get",
        "database.get_metadata",
        "database.query",
        "search",
    ] {
        let tool = tools.iter().find(|tool| tool["name"] == name).unwrap();
        assert_eq!(tool["annotations"]["readOnlyHint"], !enabled, "{name}");
    }
    let target = object(&format!("10{index}"));
    let before = queued_count(fixture).await;
    let missing = wire.call(2, "database.get", json!({"object":target})).await;
    if enabled {
        assert_eq!(
            missing["result"]["structuredContent"]["state"], "pending",
            "{missing}"
        );
        let collection = &missing["result"]["structuredContent"]["collection"];
        assert_eq!(collection["status"], "pending");
        let id = collection["receipt"]["request_id"].as_str().unwrap();
        let explicit = wire
            .call(
                3,
                "database.request_collection",
                json!({"target":{"kind":"object","object":target}}),
            )
            .await;
        assert_eq!(explicit["result"]["structuredContent"]["request_id"], id);
        assert_eq!(queued_count(fixture).await, before + 1);
    } else {
        assert_eq!(missing["result"]["isError"], true, "{missing}");
        assert_eq!(
            missing["result"]["structuredContent"]["code"], "not_observed",
            "{missing}"
        );
        assert_eq!(queued_count(fixture).await, before);
    }
    let before_historical = queued_count(fixture).await;
    let historical = wire
        .call(
            4,
            "database.get",
            json!({"object":target,"selector":{"kind":"capture","id":"a".repeat(64)}}),
        )
        .await;
    assert_eq!(historical["result"]["isError"], true, "{historical}");
    assert_eq!(queued_count(fixture).await, before_historical);
    let (status, invalid) = wire.rpc(5, "tools/call", json!({"name":"database.query","arguments":{"query":"Fictional","collection_term":"invalid.*"}})).await;
    assert_eq!(invalid["error"]["code"], -32602, "{invalid}");
    assert!(status.is_none() || status == Some(400));
    assert_eq!(queued_count(fixture).await, before_historical);
    let query = "in:title:Fictional AND NOT in:body:absent";
    let local = wire
        .call(
            6,
            "database.query",
            json!({"query":query,"collection_term":"Fictional"}),
        )
        .await;
    assert_ne!(local["result"]["isError"], true, "{local}");
    assert_eq!(local["result"]["structuredContent"]["hits"], json!([]));
    assert_eq!(
        local["result"]["structuredContent"]["collection"]["status"],
        if enabled { "pending" } else { "disabled" }
    );
    let compat = wire
        .call(
            7,
            "search",
            json!({"query":query,"collection_term":"Fictional"}),
        )
        .await;
    assert_ne!(compat["result"]["isError"], true, "{compat}");
    assert_eq!(compat["result"]["structuredContent"], json!({"results":[]}));
    let first_text: Value =
        serde_json::from_str(compat["result"]["content"][0]["text"].as_str().unwrap()).unwrap();
    assert_eq!(first_text, compat["result"]["structuredContent"]);
    assert_eq!(
        compat["result"]["_meta"]["openlegal/collection"]["status"],
        if enabled { "pending" } else { "disabled" }
    );
    if enabled {
        assert_eq!(
            compat["result"]["_meta"]["openlegal/collection"]["receipt"]["request_id"],
            local["result"]["structuredContent"]["collection"]["receipt"]["request_id"]
        );
        fixture_sql(
            fixture,
            "UPDATE openlegal.corpus_control SET collection_scheduler_seen_at=0 WHERE singleton",
        )
        .await;
        let unavailable = wire
            .call(
                8,
                "database.get_metadata",
                json!({"object":object(&format!("20{index}"))}),
            )
            .await;
        assert_eq!(unavailable["result"]["isError"], true, "{unavailable}");
        assert_eq!(
            unavailable["result"]["structuredContent"]["code"], "not_observed",
            "{unavailable}"
        );
        assert_eq!(
            unavailable["result"]["structuredContent"]["collection"]["status"],
            "unavailable"
        );
        let before_unavailable_query = queued_count(fixture).await;
        let local_unavailable = wire
            .call(
                9,
                "database.query",
                json!({"query":query,"collection_term":format!("Unavailable{index}")}),
            )
            .await;
        assert_ne!(
            local_unavailable["result"]["isError"], true,
            "{local_unavailable}"
        );
        assert_eq!(
            local_unavailable["result"]["structuredContent"]["hits"],
            json!([])
        );
        assert_eq!(
            local_unavailable["result"]["structuredContent"]["collection"]["status"],
            "unavailable"
        );
        assert_eq!(queued_count(fixture).await, before_unavailable_query);
    }
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh PostgreSQL 18 environment"]
async fn automatic_collection_contracts_hold_through_http_and_webtransport() {
    for enabled in [false, true] {
        let fixture = postgres::TestDatabase::new().await;
        let persistent = fixture.open(SystemClock::default().now()).await;
        let runtime = CorpusRuntime::open(
            &DatabaseConfig {
                max_raw_bytes: Default::default(),
                auto_collection: enabled,
                blob_path: fixture.directory.path().join("blobs-corpus"),
                index_path: fixture.directory.path().join("index"),
                mecab_dictionary_path: std::env::var_os("OPENLEGAL_TEST_MECAB_DICTIONARY")
                    .expect("PostgreSQL gate provides pinned dictionary")
                    .into(),
                widget_html: "unused.html".into(),
                ingestion: None,
            },
            &persistent,
        )
        .await
        .unwrap();
        let demand = Arc::new(DemandCollectionCoordinator::new(
            runtime.store.clone(),
            enabled,
        ));
        let citations = Arc::new(
            CitationService::new(
                runtime.database.clone(),
                runtime.search.clone(),
                runtime.store.clone(),
                Arc::new(SystemClock::default()),
                "https://reference.test".into(),
            )
            .unwrap(),
        );
        let comparison = openlegal_server::text_diff::service(std::path::Path::new(env!(
            "CARGO_BIN_EXE_openlegal-server"
        )))
        .await
        .unwrap();
        let source = SourceOffer::new("https://example.test/source").unwrap();
        // database.show advertises this static UI resource even though these
        // transport scenarios only exercise retrieval and collection tools.
        let widget = fixture.directory.path().join("database.html");
        tokio::fs::write(&widget, "<html><meta name=\"openlegal-source-url\" content=\"__OPENLEGAL_SOURCE_URL__\">Fictional demand fixture</html>")
            .await.unwrap();
        let resources = openlegal_server::database::load_widget(&widget, &source)
            .await
            .unwrap();
        let mut registry = server_info_registry(source.clone()).unwrap();
        registry
            .register_module(DatabaseTools {
                demand: Some(demand.clone()),
                database: runtime.database.clone(),
                reader: runtime.reader.clone(),
                search: runtime.search.clone(),
                comparison,
                store: runtime.store.clone(),
            })
            .unwrap();
        registry
            .register_module(CitationTools {
                service: citations.clone(),
                demand: Some(demand),
            })
            .unwrap();
        let identity =
            rcgen::generate_simple_self_signed(vec!["localhost".into(), "127.0.0.1".into()])
                .unwrap();
        let certificate = fixture.directory.path().join("cert.pem");
        let private_key = fixture.directory.path().join("key.pem");
        std::fs::write(&certificate, identity.cert.pem()).unwrap();
        std::fs::write(&private_key, identity.signing_key.serialize_pem()).unwrap();
        let quic_port = std::net::UdpSocket::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let mut builder = ServerBuilder::new(
            registry,
            Limits {
                max_message_bytes: MAX_FRAME,
                max_buffer_bytes: MAX_FRAME * 32,
                ..Limits::default()
            },
            source,
        )
        .with_citations(citations)
        .with_resources(resources);
        builder
            .register_endpoint(HttpEndpoint {
                tls: None,
                edge_mtls: None,
                bind: "127.0.0.1:0".parse().unwrap(),
                access: AccessPolicy {
                    allowed_hosts: vec!["demand.test".into()],
                    allowed_origins: vec![],
                },
            })
            .unwrap();
        builder
            .register_endpoint(WebTransportEndpoint {
                edge_mtls: None,
                bind: format!("127.0.0.1:{quic_port}").parse().unwrap(),
                certificate,
                private_key,
                access: AccessPolicy {
                    allowed_hosts: vec![format!("127.0.0.1:{quic_port}")],
                    allowed_origins: vec!["https://allowed.test".into()],
                },
            })
            .unwrap();
        let server = builder.bind().await.unwrap();
        let addresses = server.addresses();
        let http = addresses.iter().find(|(name, _)| name == "http").unwrap().1[0];
        let quic = addresses
            .iter()
            .find(|(name, _)| name == "webtransport")
            .unwrap()
            .1[0];
        let shutdown = CancellationToken::new();
        let mut task = tokio::spawn(server.run(shutdown.clone()));
        let scenario = AssertUnwindSafe(async {
            exercise(
                &mut Wire::Http(format!("http://{http}/mcp")),
                &runtime,
                &fixture,
                enabled,
                1,
            )
            .await;
            let mut roots = rustls::RootCertStore::empty();
            roots
                .add(rustls::pki_types::CertificateDer::from(
                    identity.cert.der().to_vec(),
                ))
                .unwrap();
            let mut tls = rustls::ClientConfig::builder_with_provider(Arc::new(
                rustls::crypto::ring::default_provider(),
            ))
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_root_certificates(roots)
            .with_no_client_auth();
            tls.alpn_protocols = vec![wtransport::tls::WEBTRANSPORT_ALPN.to_vec()];
            let client = QuicEndpoint::client(
                ClientConfig::builder()
                    .with_bind_default()
                    .with_custom_tls(tls)
                    .build(),
            )
            .unwrap();
            let url = format!("https://127.0.0.1:{}{PATH}", quic.port());
            let connection = client
                .connect(
                    ConnectOptions::builder(&url)
                        .add_header("origin", "https://allowed.test")
                        .build(),
                )
                .await
                .unwrap();
            let (tx, rx) = connection.open_bi().await.unwrap().await.unwrap();
            let rx = FrameReader::new(
                rx,
                MAX_FRAME,
                Arc::new(Semaphore::new(MAX_FRAME * 2)),
                Duration::from_secs(15),
                Duration::from_secs(15),
            );
            exercise(
                &mut Wire::WebTransport {
                    tx,
                    rx: Box::new(rx),
                },
                &runtime,
                &fixture,
                enabled,
                2,
            )
            .await;
        })
        .catch_unwind()
        .await;
        shutdown.cancel();
        let server_result = tokio::time::timeout(Duration::from_secs(20), &mut task).await;
        if server_result.is_err() {
            task.abort();
            let _ = (&mut task).await;
        }
        runtime.close().await.unwrap();
        persistent.close().await.unwrap();
        if let Err(panic) = scenario {
            std::panic::resume_unwind(panic);
        }
        server_result.unwrap().unwrap().unwrap();
    }
}
