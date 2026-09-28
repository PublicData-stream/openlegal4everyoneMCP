//! Public limit behavior through both MCP transports and protocol revisions.
use openlegal_server::{
    ServerBuilder,
    config::{AccessPolicy, EdgeMtlsConfig, HttpTlsConfig, Limits, RateLimitConfig, SourceOffer},
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
    client_cert: Option<rustls::pki_types::CertificateDer<'static>>,
    client_key: Option<Vec<u8>>,
    client_identity_pem: Option<Vec<u8>>,
    wrong_client_cert: Option<rustls::pki_types::CertificateDer<'static>>,
    wrong_client_key: Option<Vec<u8>>,
    wrong_client_identity_pem: Option<Vec<u8>>,
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
        Self::start_with_mtls(limits, false).await
    }

    async fn start_with_mtls(limits: Limits, mtls: bool) -> Self {
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
        let (
            edge_mtls,
            client_cert,
            client_key,
            client_identity_pem,
            wrong_client_cert,
            wrong_client_key,
            wrong_client_identity_pem,
        ) = if mtls {
            use rcgen::{
                BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, Issuer,
                KeyPair, KeyUsagePurpose,
            };
            let ca_key = KeyPair::generate().unwrap();
            let mut ca_params = CertificateParams::new(Vec::<String>::new()).unwrap();
            ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
            ca_params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
            let ca = ca_params.self_signed(&ca_key).unwrap();
            let ca_file = directory.path().join("edge-ca.pem");
            std::fs::write(&ca_file, ca.pem()).unwrap();
            let issuer = Issuer::from_params(&ca_params, &ca_key);
            let make_client = |san: &str| {
                let key = KeyPair::generate().unwrap();
                let mut params = CertificateParams::new(vec![san.to_owned()]).unwrap();
                params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
                let certificate = params.signed_by(&key, &issuer).unwrap();
                (
                    certificate.der().clone(),
                    key.serialize_der(),
                    format!("{}{}", certificate.pem(), key.serialize_pem()).into_bytes(),
                )
            };
            let (client_cert, client_key, client_identity_pem) =
                make_client("oxibelt.openlegal.internal");
            let (wrong_client_cert, wrong_client_key, wrong_client_identity_pem) =
                make_client("wrong.openlegal.internal");
            (
                Some(EdgeMtlsConfig {
                    client_ca_file: ca_file,
                    required_client_dns_san: "oxibelt.openlegal.internal".into(),
                }),
                Some(client_cert),
                Some(client_key),
                Some(client_identity_pem),
                Some(wrong_client_cert),
                Some(wrong_client_key),
                Some(wrong_client_identity_pem),
            )
        } else {
            (None, None, None, None, None, None, None)
        };
        let port = std::net::UdpSocket::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let mut builder = ServerBuilder::new(registry, limits, source);
        builder
            .register_endpoint(HttpEndpoint {
                tls: mtls.then(|| HttpTlsConfig {
                    certificate: certificate.clone(),
                    private_key: private_key.clone(),
                }),
                edge_mtls: edge_mtls.clone(),
                bind: "127.0.0.1:0".parse().unwrap(),
                access: AccessPolicy {
                    allowed_hosts: vec!["test.local".into()],
                    allowed_origins: vec![],
                },
            })
            .unwrap();
        builder
            .register_endpoint(WebTransportEndpoint {
                edge_mtls,
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
            http_url: format!("{}://{http}/mcp", if mtls { "https" } else { "http" }),
            health_url: format!("http://{health}/metrics"),
            wt_url: format!("https://127.0.0.1:{port}/mcp-wt/v1"),
            cert: cert.der().clone(),
            client_cert,
            client_key,
            client_identity_pem,
            wrong_client_cert,
            wrong_client_key,
            wrong_client_identity_pem,
            shutdown,
            task,
            _directory: directory,
        }
    }

    async fn http(&self, version: &str, method: &str, mut params: Value) -> Value {
        add_meta(version, &mut params);
        let client = if let Some(identity) = &self.client_identity_pem {
            reqwest::Client::builder()
                .add_root_certificate(reqwest::Certificate::from_der(self.cert.as_ref()).unwrap())
                .identity(reqwest::Identity::from_pem(identity).unwrap())
                .build()
                .unwrap()
        } else {
            reqwest::Client::new()
        };
        let mut request = client
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
        let client = self.wt_client(self.client_cert.as_ref(), self.client_key.as_deref());
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

    fn wt_client(
        &self,
        cert: Option<&rustls::pki_types::CertificateDer<'static>>,
        key: Option<&[u8]>,
    ) -> Endpoint<wtransport::endpoint::endpoint_side::Client> {
        let mut roots = rustls::RootCertStore::empty();
        roots.add(self.cert.clone()).unwrap();
        let builder = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots);
        let mut tls = match (cert, key) {
            (Some(cert), Some(key)) => builder
                .with_client_auth_cert(
                    vec![cert.clone()],
                    rustls::pki_types::PrivateKeyDer::try_from(key.to_vec()).unwrap(),
                )
                .unwrap(),
            _ => builder.with_no_client_auth(),
        };
        tls.alpn_protocols = vec![wtransport::tls::WEBTRANSPORT_ALPN.to_vec()];
        Endpoint::client(
            ClientConfig::builder()
                .with_bind_default()
                .with_custom_tls(tls)
                .build(),
        )
        .unwrap()
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
                verified_tunnel: None,
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
async fn verified_edge_uses_one_replacement_bucket_across_transports() {
    use openlegal_server::config::VerifiedTunnelRateLimitConfig;
    for version in ["2025-11-25", "2026-07-28"] {
        let server = Server::start_with_mtls(
            Limits {
                max_message_bytes: 4096,
                rate_limit: RateLimitConfig {
                    enabled: true,
                    calls_per_second: 1,
                    burst: 1,
                    verified_tunnel: Some(VerifiedTunnelRateLimitConfig {
                        calls_per_second: 1,
                        burst: 3,
                    }),
                },
                ..Limits::default()
            },
            true,
        )
        .await;
        let (_client, _connection, mut tx, mut rx) = server.wt().await;
        wt_ready(&mut tx, &mut rx, version).await;
        let first = server.http(version, "tools/call", call("small")).await;
        assert_eq!(first["result"]["structuredContent"]["ok"], true);
        let second = wt_call(&mut tx, &mut rx, version, 2, "tools/call", call("small")).await;
        assert_eq!(second["result"]["structuredContent"]["ok"], true);
        let third = server.http(version, "tools/call", call("small")).await;
        assert_eq!(third["result"]["structuredContent"]["ok"], true);
        let fourth = wt_call(&mut tx, &mut rx, version, 3, "tools/call", call("small")).await;
        assert_tool_error(&fourth, "rate_limited");

        let request = || {
            reqwest::Client::builder()
                .add_root_certificate(reqwest::Certificate::from_der(server.cert.as_ref()).unwrap())
        };
        let anonymous = request()
            .build()
            .unwrap()
            .post(&server.http_url)
            .header("host", "test.local")
            .header("x-verified-tunnel", "true")
            .body("{}")
            .send()
            .await;
        assert!(
            anonymous.is_err(),
            "header must not replace edge client authentication"
        );
        let wrong = request()
            .identity(
                reqwest::Identity::from_pem(server.wrong_client_identity_pem.as_ref().unwrap())
                    .unwrap(),
            )
            .build()
            .unwrap()
            .post(&server.http_url)
            .header("host", "test.local")
            .body("{}")
            .send()
            .await;
        assert!(wrong.is_err(), "wrong edge DNS SAN must fail the handshake");

        use rcgen::{
            BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
            KeyUsagePurpose,
        };
        let unrelated_ca_key = KeyPair::generate().unwrap();
        let mut unrelated_ca = CertificateParams::new(Vec::<String>::new()).unwrap();
        unrelated_ca.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        unrelated_ca.key_usages = vec![KeyUsagePurpose::KeyCertSign];
        let unrelated_issuer = Issuer::from_params(&unrelated_ca, &unrelated_ca_key);
        let unrelated_key = KeyPair::generate().unwrap();
        let mut unrelated_leaf =
            CertificateParams::new(vec!["oxibelt.openlegal.internal".into()]).unwrap();
        unrelated_leaf.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
        let unrelated_cert = unrelated_leaf
            .signed_by(&unrelated_key, &unrelated_issuer)
            .unwrap();
        let unrelated_pem = format!("{}{}", unrelated_cert.pem(), unrelated_key.serialize_pem());
        let wrong_ca_http = request()
            .identity(reqwest::Identity::from_pem(unrelated_pem.as_bytes()).unwrap())
            .build()
            .unwrap()
            .post(&server.http_url)
            .header("host", "test.local")
            .body("{}")
            .send()
            .await;
        assert!(
            wrong_ca_http.is_err(),
            "untrusted edge CA must fail HTTP handshake"
        );

        let unrelated_der = unrelated_cert.der().clone();
        let unrelated_key_der = unrelated_key.serialize_der();
        for (case, cert, key) in [
            ("missing certificate", None, None),
            (
                "wrong DNS SAN",
                server.wrong_client_cert.as_ref(),
                server.wrong_client_key.as_deref(),
            ),
            (
                "wrong CA",
                Some(&unrelated_der),
                Some(unrelated_key_der.as_slice()),
            ),
        ] {
            let client = server.wt_client(cert, key);
            let handshake =
                tokio::time::timeout(Duration::from_secs(5), client.connect(&server.wt_url))
                    .await
                    .expect("WebTransport mTLS rejection should finish promptly");
            assert!(
                handshake.is_err(),
                "{case} must fail WebTransport handshake"
            );
        }
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
