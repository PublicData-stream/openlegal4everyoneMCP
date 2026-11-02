//! Actual MCP wire authorization: unfenced persistent data never reaches clients.
use futures::{FutureExt, future::BoxFuture};
use openlegal_application::durability::DurabilityFence;
use openlegal_domain::RetrievalError;
use openlegal_server::{
    ServerBuilder,
    config::{AccessPolicy, Limits, SourceOffer},
    framing::{FrameReader, write_json},
    http::HttpEndpoint,
    registry::{RichToolOutput, ToolOptions},
    webtransport::WebTransportEndpoint,
};
use serde_json::{Value, json};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{sync::Semaphore, time::Instant};
use tokio_util::sync::CancellationToken;
use wtransport::{ClientConfig, Endpoint, tls::rustls};

#[derive(serde::Deserialize, schemars::JsonSchema)]
struct Empty {}

#[derive(serde::Serialize, schemars::JsonSchema)]
struct Retained {
    capture: String,
}

struct ControlledFence {
    allow: AtomicBool,
    stall: AtomicBool,
    calls: AtomicUsize,
}
impl DurabilityFence for ControlledFence {
    fn check(
        &self,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> BoxFuture<'static, Result<(), RetrievalError>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let allowed = self.allow.load(Ordering::SeqCst);
        let stall = self.stall.load(Ordering::SeqCst);
        async move {
            // Deliberately ignores the port's deadline to prove the public
            // request boundary enforces its own remaining budget.
            if stall {
                futures::future::pending::<()>().await;
            }
            if cancellation.is_cancelled() || Instant::now() >= deadline {
                Err(RetrievalError::Cancelled)
            } else if allowed {
                Ok(())
            } else {
                Err(RetrievalError::StorageUnavailable)
            }
        }
        .boxed()
    }
}

async fn rpc(url: &str, version: &str, name: &str) -> Value {
    let mut params = json!({"name":name,"arguments":{}});
    if version == "2026-07-28" {
        params["_meta"] = json!({
            "io.modelcontextprotocol/protocolVersion":version,
            "io.modelcontextprotocol/clientInfo":{"name":"durability-fixture","version":"1"},
            "io.modelcontextprotocol/clientCapabilities":{}
        });
    }
    let response = reqwest::Client::new()
        .post(url)
        .header("Host", "test.local")
        .header("Accept", "application/json, text/event-stream")
        .header("MCP-Protocol-Version", version)
        .header("Mcp-Method", "tools/call")
        .header("Mcp-Name", name)
        .json(&json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":params}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let body = tokio::time::timeout(Duration::from_secs(3), response.text())
        .await
        .unwrap()
        .unwrap();
    body.lines()
        .filter_map(|line| line.strip_prefix("data:"))
        .filter_map(|line| serde_json::from_str::<Value>(line.trim()).ok())
        .find(|value| value.get("id").is_some())
        .or_else(|| serde_json::from_str(&body).ok())
        .expect("RPC result")
}

async fn wt_rpc(
    tx: &mut wtransport::SendStream,
    rx: &mut FrameReader<wtransport::RecvStream>,
    version: &str,
    method: &str,
    mut params: Value,
) -> Value {
    if version == "2026-07-28" {
        params["_meta"] = json!({
            "io.modelcontextprotocol/protocolVersion":version,
            "io.modelcontextprotocol/clientInfo":{"name":"durability-fixture","version":"1"},
            "io.modelcontextprotocol/clientCapabilities":{}
        });
    }
    write_json(
        tx,
        &json!({"jsonrpc":"2.0","id":1,"method":method,"params":params}),
        4096,
        &Arc::new(Semaphore::new(8192)),
        Duration::from_secs(3),
    )
    .await
    .unwrap();
    serde_json::from_slice(&rx.read().await.unwrap().unwrap().bytes).unwrap()
}

#[tokio::test]
async fn persistent_output_and_sidecars_require_fresh_authorization_in_both_revisions() {
    let source = SourceOffer::new("https://source.test/durability").unwrap();
    let mut registry = openlegal_server::registry::server_info_registry(source.clone()).unwrap();
    for name in ["retained", "text.diff"] {
        registry
            .register_rich::<Empty, Retained, _, _>(
                name,
                "A retained-data fixture",
                ToolOptions::default(),
                |_, _| async {
                    let mut value = RichToolOutput::new(Retained {
                        capture: "private-capture-marker".into(),
                    });
                    value.output.meta = Some(rmcp::model::MetaObject(
                        json!({"capture":"private-metadata-marker"})
                            .as_object()
                            .unwrap()
                            .clone(),
                    ));
                    value
                        .additional_content
                        .push(rmcp::model::ContentBlock::text("private-sidecar-marker"));
                    Ok(value)
                },
            )
            .unwrap();
    }
    let fence = Arc::new(ControlledFence {
        allow: AtomicBool::new(false),
        stall: AtomicBool::new(false),
        calls: AtomicUsize::new(0),
    });
    let mut builder = ServerBuilder::new(
        registry,
        Limits {
            call_timeout_secs: 1,
            ..Limits::default()
        },
        source,
    )
    .with_durability_fence(Some(fence.clone()));
    builder
        .register_endpoint(HttpEndpoint {
            bind: "127.0.0.1:0".parse().unwrap(),
            tls: None,
            edge_mtls: None,
            access: AccessPolicy {
                allowed_hosts: vec!["test.local".into()],
                allowed_origins: vec![],
            },
        })
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
    builder
        .register_endpoint(WebTransportEndpoint {
            bind: format!("127.0.0.1:{port}").parse().unwrap(),
            certificate,
            private_key,
            edge_mtls: None,
            access: AccessPolicy {
                allowed_hosts: vec![format!("127.0.0.1:{port}")],
                allowed_origins: vec![],
            },
        })
        .unwrap();
    let running = builder.bind().await.unwrap();
    let address = running
        .addresses()
        .iter()
        .find(|(name, _)| name == "http")
        .unwrap()
        .1[0];
    let url = format!("http://{address}/mcp");
    let stop = CancellationToken::new();
    let task = tokio::spawn(running.run(stop.clone()));
    for version in ["2025-11-25", "2026-07-28"] {
        fence.allow.store(false, Ordering::SeqCst);
        let rejected = rpc(&url, version, "retained").await;
        assert_eq!(
            rejected["result"]["structuredContent"]["code"],
            "storage_unavailable"
        );
        assert!(!rejected.to_string().contains("private-"));
        // Extension names cannot impersonate an independent built-in when that
        // built-in module is absent from the registry.
        let impersonated = rpc(&url, version, "text.diff").await;
        assert_eq!(
            impersonated["result"]["structuredContent"]["code"],
            "storage_unavailable"
        );
        assert!(!impersonated.to_string().contains("private-"));
        let before = fence.calls.load(Ordering::SeqCst);
        let info = rpc(&url, version, "server_info").await;
        assert_ne!(info["result"]["isError"], true);
        assert_eq!(fence.calls.load(Ordering::SeqCst), before);
        fence.allow.store(true, Ordering::SeqCst);
        let accepted = rpc(&url, version, "retained").await;
        assert_eq!(
            accepted["result"]["structuredContent"]["capture"],
            "private-capture-marker"
        );
        assert!(accepted.to_string().contains("private-sidecar-marker"));
        fence.stall.store(true, Ordering::SeqCst);
        let began = Instant::now();
        let expired = rpc(&url, version, "retained").await;
        assert!(began.elapsed() < Duration::from_secs(3));
        assert_eq!(
            expired["result"]["structuredContent"]["code"],
            "storage_unavailable"
        );
        assert!(!expired.to_string().contains("private-"));
        fence.stall.store(false, Ordering::SeqCst);
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
        let mut rx = FrameReader::new(
            rx,
            4096,
            Arc::new(Semaphore::new(8192)),
            Duration::from_secs(3),
            Duration::from_secs(3),
        );
        if version == "2025-11-25" {
            let initialized = wt_rpc(&mut tx, &mut rx, version, "initialize", json!({"protocolVersion":version,"capabilities":{},"clientInfo":{"name":"fixture","version":"1"}})).await;
            assert_eq!(initialized["result"]["protocolVersion"], version);
            write_json(
                &mut tx,
                &json!({"jsonrpc":"2.0","method":"notifications/initialized"}),
                4096,
                &Arc::new(Semaphore::new(8192)),
                Duration::from_secs(3),
            )
            .await
            .unwrap();
        }
        fence.allow.store(false, Ordering::SeqCst);
        let rejected = wt_rpc(
            &mut tx,
            &mut rx,
            version,
            "tools/call",
            json!({"name":"retained","arguments":{}}),
        )
        .await;
        assert_eq!(
            rejected["result"]["structuredContent"]["code"],
            "storage_unavailable"
        );
        assert!(!rejected.to_string().contains("private-"));
        let impersonated = wt_rpc(
            &mut tx,
            &mut rx,
            version,
            "tools/call",
            json!({"name":"text.diff","arguments":{}}),
        )
        .await;
        assert_eq!(
            impersonated["result"]["structuredContent"]["code"],
            "storage_unavailable"
        );
        assert!(!impersonated.to_string().contains("private-"));
        fence.allow.store(true, Ordering::SeqCst);
        let accepted = wt_rpc(
            &mut tx,
            &mut rx,
            version,
            "tools/call",
            json!({"name":"retained","arguments":{}}),
        )
        .await;
        assert_eq!(
            accepted["result"]["structuredContent"]["capture"],
            "private-capture-marker"
        );
        assert!(accepted.to_string().contains("private-sidecar-marker"));
        fence.stall.store(true, Ordering::SeqCst);
        let began = Instant::now();
        let expired = wt_rpc(
            &mut tx,
            &mut rx,
            version,
            "tools/call",
            json!({"name":"retained","arguments":{}}),
        )
        .await;
        assert!(began.elapsed() < Duration::from_secs(3));
        assert_eq!(
            expired["result"]["structuredContent"]["code"],
            "storage_unavailable"
        );
        assert!(!expired.to_string().contains("private-"));
        fence.stall.store(false, Ordering::SeqCst);
    }
    stop.cancel();
    task.await.unwrap().unwrap();
}
