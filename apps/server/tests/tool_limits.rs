//! Public limit behavior through both MCP transports and protocol revisions.
use openlegal_server::{
    ServerBuilder,
    config::{AccessPolicy, Limits, RateLimitConfig, SourceOffer},
    framing::{FrameReader, write_json},
    http::{HealthEndpoint, HttpEndpoint},
    registry::{ToolError, ToolOptions, ToolOutput, ToolRegistry},
    webtransport::WebTransportEndpoint,
};
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use wtransport::{ClientConfig, Endpoint, tls::rustls};

#[derive(serde::Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
struct Empty {}

#[derive(serde::Serialize, schemars::JsonSchema)]
struct LargeOutput {
    data: String,
}

struct Server {
    http_url: String,
    health_url: String,
    wt_url: String,
    cert: rustls::pki_types::CertificateDer<'static>,
    shutdown: CancellationToken,
    task: tokio::task::JoinHandle<Result<(), openlegal_server::ServerError>>,
    _directory: tempfile::TempDir,
}

impl Drop for Server {
    fn drop(&mut self) {
        self.shutdown.cancel();
        self.task.abort();
    }
}

impl Server {
    async fn start(limits: Limits) -> Self {
        let source = SourceOffer::new("https://source.test/running").unwrap();
        let mut registry = ToolRegistry::new();
        registry
            .register::<Empty, _, _>("small", "Small output", |_, _| async {
                Ok(json!({"ok":true}))
            })
            .unwrap();
        registry
            .register::<Empty, _, _>("missing_data", "Application failure", |_, _| async {
                Err(ToolError::NotFound)
            })
            .unwrap();
        registry
            .register::<Empty, _, _>("fits", "Result at byte boundary", |_, _| async {
                Ok(json!({"data":"x".repeat(339)}))
            })
            .unwrap();
        registry
            .register::<Empty, _, _>("exceeds", "Result beyond byte boundary", |_, _| async {
                Ok(json!({"data":"x".repeat(340)}))
            })
            .unwrap();
        registry
            .register_typed::<Empty, LargeOutput, _, _>(
                "typed_exceeds",
                "Typed result beyond byte boundary",
                ToolOptions::default(),
                |_, _| async {
                    Ok(ToolOutput::new(LargeOutput {
                        data: "x".repeat(400),
                    }))
                },
            )
            .unwrap();
        let directory = tempfile::tempdir().unwrap();
        let rcgen::CertifiedKey { cert, signing_key } =
            rcgen::generate_simple_self_signed(vec!["127.0.0.1".into()]).unwrap();
        let certificate = directory.path().join("cert.pem");
        let private_key = directory.path().join("key.pem");
        std::fs::write(&certificate, cert.pem()).unwrap();
        std::fs::write(&private_key, signing_key.serialize_pem()).unwrap();
        let port = std::net::UdpSocket::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let mut builder = ServerBuilder::new(registry, limits, source);
        builder
            .register_endpoint(HttpEndpoint {
                bind: "127.0.0.1:0".parse().unwrap(),
                access: AccessPolicy {
                    allowed_hosts: vec!["test.local".into()],
                    allowed_origins: vec![],
                },
            })
            .unwrap();
        builder
            .register_endpoint(WebTransportEndpoint {
                bind: format!("127.0.0.1:{port}").parse().unwrap(),
                certificate,
                private_key,
                access: AccessPolicy {
                    allowed_hosts: vec![format!("127.0.0.1:{port}")],
                    allowed_origins: vec![],
                },
            })
            .unwrap();
        builder
            .register_endpoint(HealthEndpoint {
                bind: "127.0.0.1:0".parse().unwrap(),
            })
            .unwrap();
        let server = builder.bind().await.unwrap();
        let addresses = server.addresses();
        let http = addresses.iter().find(|(name, _)| name == "http").unwrap().1[0];
        let health = addresses
            .iter()
            .find(|(name, _)| name == "health")
            .unwrap()
            .1[0];
        let shutdown = CancellationToken::new();
        let task = tokio::spawn(server.run(shutdown.clone()));
        Self {
            http_url: format!("http://{http}/mcp"),
            health_url: format!("http://{health}/metrics"),
            wt_url: format!("https://127.0.0.1:{port}/mcp-wt/v1"),
            cert: cert.der().clone(),
            shutdown,
            task,
            _directory: directory,
        }
    }

    async fn http(&self, version: &str, method: &str, mut params: Value) -> Value {
        add_meta(version, &mut params);
        let mut request = reqwest::Client::new()
            .post(&self.http_url)
            .header("host", "test.local")
            .header("accept", "application/json, text/event-stream")
            .header("mcp-protocol-version", version)
            .header("mcp-method", method);
        if let Some(name) = params.get("name").and_then(Value::as_str) {
            request = request.header("mcp-name", name);
        }
        let response = request
            .json(&json!({"jsonrpc":"2.0","id":1,"method":method,"params":params}))
            .send()
            .await
            .unwrap();
        let status = response.status();
        let body = response.text().await.unwrap();
        assert!([200, 400].contains(&status.as_u16()), "{status}: {body}");
        body.lines()
            .filter_map(|line| line.strip_prefix("data:"))
            .filter_map(|line| serde_json::from_str::<Value>(line.trim()).ok())
            .find(|value| value.get("id").is_some())
            .or_else(|| serde_json::from_str(&body).ok())
            .unwrap_or_else(|| panic!("missing response: {body}"))
    }

    async fn wt(
        &self,
    ) -> (
        Endpoint<wtransport::endpoint::endpoint_side::Client>,
        wtransport::Connection,
        wtransport::SendStream,
        FrameReader<wtransport::RecvStream>,
    ) {
        let mut roots = rustls::RootCertStore::empty();
        roots.add(self.cert.clone()).unwrap();
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
        let connection = client.connect(&self.wt_url).await.unwrap();
        let (tx, rx) = connection.open_bi().await.unwrap().await.unwrap();
        let reader = FrameReader::new(
            rx,
            4096,
            Arc::new(Semaphore::new(8192)),
            Duration::from_secs(3),
            Duration::from_secs(3),
        );
        (client, connection, tx, reader)
    }
}

fn add_meta(version: &str, params: &mut Value) {
    if version == "2026-07-28" {
        params["_meta"] = json!({
            "io.modelcontextprotocol/protocolVersion":version,
            "io.modelcontextprotocol/clientInfo":{"name":"limit-fixture","version":"1"},
            "io.modelcontextprotocol/clientCapabilities":{}
        });
    }
}

async fn wt_call(
    tx: &mut wtransport::SendStream,
    rx: &mut FrameReader<wtransport::RecvStream>,
    version: &str,
    id: u64,
    method: &str,
    mut params: Value,
) -> Value {
    add_meta(version, &mut params);
    write_json(
        tx,
        &json!({"jsonrpc":"2.0","id":id,"method":method,"params":params}),
        4096,
        &Arc::new(Semaphore::new(8192)),
        Duration::from_secs(3),
    )
    .await
    .unwrap();
    let response: Value = serde_json::from_slice(&rx.read().await.unwrap().unwrap().bytes).unwrap();
    assert_eq!(response["id"], id);
    response
}

async fn wt_ready(
    tx: &mut wtransport::SendStream,
    rx: &mut FrameReader<wtransport::RecvStream>,
    version: &str,
) {
    if version == "2025-11-25" {
        let response = wt_call(
            tx,
            rx,
            version,
            90,
            "initialize",
            json!({"protocolVersion":version,"capabilities":{},"clientInfo":{"name":"fixture","version":"1"}}),
        )
        .await;
        assert_eq!(response["result"]["protocolVersion"], version);
        write_json(
            tx,
            &json!({"jsonrpc":"2.0","method":"notifications/initialized"}),
            4096,
            &Arc::new(Semaphore::new(8192)),
            Duration::from_secs(3),
        )
        .await
        .unwrap();
    }
}

fn call(name: &str) -> Value {
    json!({"name":name,"arguments":{}})
}

fn assert_tool_error(value: &Value, code: &str) {
    assert!(value.get("error").is_none(), "{value}");
    assert_eq!(value["result"]["isError"], true, "{value}");
    assert_eq!(value["result"]["structuredContent"]["code"], code);
}

#[tokio::test]
async fn one_rate_bucket_is_shared_by_both_transports() {
    for version in ["2025-11-25", "2026-07-28"] {
        let server = Server::start(Limits {
            max_message_bytes: 4096,
            rate_limit: RateLimitConfig {
                enabled: true,
                calls_per_second: 1,
                burst: 3,
            },
            ..Limits::default()
        })
        .await;
        let (_client, _connection, mut tx, mut rx) = server.wt().await;
        wt_ready(&mut tx, &mut rx, version).await;
        let invalid = server.http(version, "tools/call", call("missing")).await;
        assert_eq!(invalid["error"]["code"], -32602);
        let invalid_arguments = server
            .http(
                version,
                "tools/call",
                json!({"name":"small","arguments":{"unexpected":true}}),
            )
            .await;
        assert_eq!(invalid_arguments["error"]["code"], -32602);
        let application_failure = server
            .http(version, "tools/call", call("missing_data"))
            .await;
        assert_tool_error(&application_failure, "not_found");
        let first = server.http(version, "tools/call", call("small")).await;
        assert_eq!(first["result"]["structuredContent"]["ok"], true);
        let second = wt_call(&mut tx, &mut rx, version, 2, "tools/call", call("small")).await;
        assert_eq!(second["result"]["structuredContent"]["ok"], true);
        let third = server.http(version, "tools/call", call("small")).await;
        assert_tool_error(&third, "rate_limited");
        let fourth = wt_call(&mut tx, &mut rx, version, 3, "tools/call", call("small")).await;
        assert_tool_error(&fourth, "rate_limited");
        let still_invalid = server.http(version, "tools/call", call("missing")).await;
        assert_eq!(still_invalid["error"]["code"], -32602);
        assert!(server.http(version, "tools/list", json!({})).await["result"]["tools"].is_array());
        let metrics = reqwest::get(&server.health_url)
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        assert!(
            metrics.contains("openlegal_tool_calls_total 5\n"),
            "{metrics}"
        );
        assert!(
            metrics.contains("openlegal_tool_failures_total 3\n"),
            "{metrics}"
        );
        assert!(
            metrics.contains("openlegal_tool_rate_limited_total 2\n"),
            "{metrics}"
        );
    }
}

#[tokio::test]
async fn configured_result_boundary_is_a_tool_error_on_both_transports() {
    for version in ["2025-11-25", "2026-07-28"] {
        let server = Server::start(Limits {
            max_message_bytes: 4096,
            max_tool_result_bytes: Some(350),
            rate_limit: RateLimitConfig {
                enabled: false,
                ..RateLimitConfig::default()
            },
            ..Limits::default()
        })
        .await;
        let (_client, _connection, mut tx, mut rx) = server.wt().await;
        wt_ready(&mut tx, &mut rx, version).await;
        for name in ["fits", "exceeds", "typed_exceeds"] {
            let http = server.http(version, "tools/call", call(name)).await;
            let wt = wt_call(&mut tx, &mut rx, version, 5, "tools/call", call(name)).await;
            if name == "fits" {
                assert_eq!(
                    http["result"]["structuredContent"]["data"]
                        .as_str()
                        .unwrap()
                        .len(),
                    339
                );
                assert_eq!(
                    wt["result"]["structuredContent"]["data"]
                        .as_str()
                        .unwrap()
                        .len(),
                    339
                );
            } else {
                assert_tool_error(&http, "resource_limit");
                assert_tool_error(&wt, "resource_limit");
            }
        }
    }
}
