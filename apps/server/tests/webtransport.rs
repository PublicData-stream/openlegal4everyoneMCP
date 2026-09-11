use openlegal_server::{
    config::{AccessPolicy, Limits},
    endpoint::{Endpoint, EndpointContext},
    framing::{FrameReader, write_json},
    handler::McpHandler,
    registry::{ToolError, ToolRegistry},
    webtransport::{PATH, WebTransportEndpoint},
};
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};
use tokio::{sync::Semaphore, task::JoinHandle, time::timeout};
use tokio_util::sync::CancellationToken;
use wtransport::{ClientConfig, Endpoint as QuicEndpoint, endpoint::ConnectOptions, tls::rustls};

struct Server {
    address: std::net::SocketAddr,
    cert: Vec<u8>,
    shutdown: CancellationToken,
    task: JoinHandle<()>,
    _directory: tempfile::TempDir,
    context: EndpointContext,
    active_plugins: Arc<std::sync::atomic::AtomicUsize>,
}
struct PluginGuard(Arc<std::sync::atomic::AtomicUsize>);
impl Drop for PluginGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
    }
}
impl Drop for Server {
    fn drop(&mut self) {
        self.shutdown.cancel();
        self.task.abort();
    }
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct Empty {}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct FailureInput {
    kind: String,
}

impl Server {
    async fn start() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let rcgen::CertifiedKey { cert, signing_key } =
            rcgen::generate_simple_self_signed(vec!["localhost".into(), "127.0.0.1".into()])
                .unwrap();
        let certificate = directory.path().join("cert.pem");
        let private_key = directory.path().join("key.pem");
        std::fs::write(&certificate, cert.pem()).unwrap();
        std::fs::write(&private_key, signing_key.serialize_pem()).unwrap();
        // Reserve a unique ephemeral port; endpoint ownership takes over immediately.
        let port = std::net::UdpSocket::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let bind = format!("127.0.0.1:{port}").parse().unwrap();
        let limits = Arc::new(Limits {
            max_message_bytes: 4096,
            max_buffer_bytes: 64 * 4096,
            io_timeout_secs: 1,
            shutdown_timeout_secs: 1,
            ..Limits::default()
        });
        let mut registry = ToolRegistry::new();
        registry
            .register::<Empty, _, _>("server_info", "Synthetic transport fixture", |_, _| async {
                Ok(json!({"synthetic":true}))
            })
            .unwrap();
        let active_plugins = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let plugin_counter = active_plugins.clone();
        registry
            .register::<Empty, _, _>(
                "slow",
                "Synthetic cancellation fixture",
                move |_, context| {
                    let active = plugin_counter.clone();
                    async move {
                        active.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        let _guard = PluginGuard(active);
                        context.cancellation.cancelled().await;
                        Ok(json!({"late":true}))
                    }
                },
            )
            .unwrap();
        registry
            .register::<FailureInput, _, _>(
                "failure",
                "Synthetic typed error fixture",
                |input, _| async move {
                    Err(match input.kind.as_str() {
                        "invalid" => ToolError::InvalidInput,
                        "unavailable" => ToolError::Unavailable,
                        _ => ToolError::Internal,
                    })
                },
            )
            .unwrap();
        let shutdown = CancellationToken::new();
        let context = EndpointContext {
            handler: McpHandler::new(registry, limits.clone()).unwrap(),
            limits: limits.clone(),
            shutdown: shutdown.clone(),
            buffers: Arc::new(Semaphore::new(limits.max_buffer_bytes)),
            requests: Arc::new(Semaphore::new(limits.max_in_flight)),
            connections: Arc::new(Semaphore::new(limits.max_connections)),
            ready: Arc::new(std::sync::atomic::AtomicBool::new(true)),
        };
        let bound = WebTransportEndpoint {
            bind,
            certificate,
            private_key,
            access: AccessPolicy {
                allowed_hosts: vec![format!("127.0.0.1:{port}")],
                allowed_origins: vec!["https://allowed.test".into()],
            },
        }
        .bind(context.clone())
        .await
        .unwrap();
        let task = tokio::spawn(async {
            bound.run.await.unwrap();
        });
        Self {
            address: bind,
            cert: cert.der().to_vec(),
            shutdown,
            task,
            _directory: directory,
            context,
            active_plugins,
        }
    }

    fn client(&self, trusted: bool) -> QuicEndpoint<wtransport::endpoint::endpoint_side::Client> {
        let mut roots = rustls::RootCertStore::empty();
        if trusted {
            roots
                .add(rustls::pki_types::CertificateDer::from(self.cert.clone()))
                .unwrap();
        }
        let mut tls = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
        tls.alpn_protocols = vec![wtransport::tls::WEBTRANSPORT_ALPN.to_vec()];
        QuicEndpoint::client(
            ClientConfig::builder()
                .with_bind_default()
                .with_custom_tls(tls)
                .build(),
        )
        .unwrap()
    }

    fn url(&self, path: &str) -> String {
        format!("https://127.0.0.1:{}{path}", self.address.port())
    }

    async fn wait_for_plugin(&self) {
        timeout(Duration::from_secs(2), async {
            while self
                .active_plugins
                .load(std::sync::atomic::Ordering::SeqCst)
                == 0
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    fn assert_cleanup_complete(&self) {
        assert_eq!(
            self.active_plugins
                .load(std::sync::atomic::Ordering::SeqCst),
            0,
            "plugin future must be dropped before session admission is released"
        );
        assert_eq!(
            self.context.requests.available_permits(),
            self.context.limits.max_in_flight
        );
        assert_eq!(
            self.context.buffers.available_permits(),
            self.context.limits.max_buffer_bytes
        );
    }
}

fn modern(id: u64, method: &str, mut params: Value) -> Value {
    params["_meta"] = json!({"io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientInfo":{"name":"fixture","version":"1"},"io.modelcontextprotocol/clientCapabilities":{}});
    json!({"jsonrpc":"2.0","id":id,"method":method,"params":params})
}

async fn send(stream: &mut wtransport::SendStream, value: &Value) {
    write_json(
        stream,
        value,
        4096,
        &Arc::new(Semaphore::new(8192)),
        Duration::from_secs(2),
    )
    .await
    .unwrap();
}
fn reader(recv: wtransport::RecvStream) -> FrameReader<wtransport::RecvStream> {
    FrameReader::new(
        recv,
        4096,
        Arc::new(Semaphore::new(8192)),
        Duration::from_secs(2),
        Duration::from_secs(2),
    )
}
async fn response(reader: &mut FrameReader<wtransport::RecvStream>) -> Value {
    serde_json::from_slice(&reader.read().await.unwrap().unwrap().bytes).unwrap()
}

#[tokio::test]
async fn unsupported_modern_versions_negotiate_without_legacy_fallback() {
    let server = Server::start().await;
    let client = server.client(true);
    for fallback in [false, true] {
        let connection = client.connect(server.url(PATH)).await.unwrap();
        let (mut tx, rx) = connection.open_bi().await.unwrap().await.unwrap();
        let mut rx = reader(rx);
        let mut unsupported = modern(1, "server/discover", json!({}));
        unsupported["params"]["_meta"]["io.modelcontextprotocol/protocolVersion"] =
            json!("2099-01-01");
        send(&mut tx, &unsupported).await;
        let error = response(&mut rx).await;
        assert_eq!(error["id"], 1);
        assert_eq!(error["error"]["code"], -32022);
        assert_eq!(error["error"]["data"]["requested"], "2099-01-01");
        let supported = error["error"]["data"]["supported"].as_array().unwrap();
        assert!(supported.contains(&json!("2026-07-28")));
        assert!(supported.contains(&json!("2025-11-25")));
        assert_eq!(
            server.context.requests.available_permits(),
            server.context.limits.max_in_flight
        );
        assert_eq!(
            server
                .active_plugins
                .load(std::sync::atomic::Ordering::SeqCst),
            0
        );
        if fallback {
            send(&mut tx, &json!({"jsonrpc":"2.0","id":2,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"fixture","version":"1"}}})).await;
            timeout(Duration::from_secs(2), connection.closed())
                .await
                .unwrap();
        } else {
            send(&mut tx, &modern(2, "server/discover", json!({}))).await;
            let discover = response(&mut rx).await;
            assert_eq!(discover["id"], 2);
            assert!(discover.get("error").is_none(), "{discover}");
            // A later unsupported inline request gets the same structured error.
            unsupported["id"] = json!(3);
            send(&mut tx, &unsupported).await;
            assert_eq!(response(&mut rx).await["error"]["code"], -32022);
            connection.close(0_u32.into(), b"done");
        }
    }
}

#[tokio::test]
async fn rejects_mixed_initialize_and_bounds_negotiation_replies() {
    let server = Server::start().await;
    let client = server.client(true);
    let connection = client.connect(server.url(PATH)).await.unwrap();
    let (mut tx, _rx) = connection.open_bi().await.unwrap().await.unwrap();
    send(&mut tx, &modern(1, "initialize", json!({"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"fixture","version":"1"}}))).await;
    timeout(Duration::from_secs(2), connection.closed())
        .await
        .unwrap();

    let connection = client.connect(server.url(PATH)).await.unwrap();
    let (mut tx, rx) = connection.open_bi().await.unwrap().await.unwrap();
    let mut rx = reader(rx);
    for id in 0..=server.context.limits.max_calls_per_connection {
        let mut request = modern(id as u64, "server/discover", json!({}));
        request["params"]["_meta"]["io.modelcontextprotocol/protocolVersion"] = json!("2099-01-01");
        send(&mut tx, &request).await;
        if id < server.context.limits.max_calls_per_connection {
            assert_eq!(response(&mut rx).await["error"]["code"], -32022);
        }
    }
    timeout(Duration::from_secs(2), connection.closed())
        .await
        .unwrap();
}

#[tokio::test]
async fn validates_access_before_loading_certificates_or_binding() {
    let server = Server::start().await;
    for (access, expected) in [
        (
            AccessPolicy {
                allowed_hosts: vec![],
                allowed_origins: vec![],
            },
            "configure explicit allowed host authorities",
        ),
        (
            AccessPolicy {
                allowed_hosts: vec!["localhost".into()],
                allowed_origins: vec!["https://allowed.test/path".into()],
            },
            "allowed origins must be HTTP(S) origins without paths",
        ),
    ] {
        let result = WebTransportEndpoint {
            bind: server.address,
            certificate: server._directory.path().join("missing-cert.pem"),
            private_key: server._directory.path().join("missing-key.pem"),
            access,
        }
        .bind(server.context.clone())
        .await;
        match result {
            Ok(_) => panic!("invalid access policy accepted"),
            Err(error) => assert_eq!(error.to_string(), expected),
        }
    }
}

#[tokio::test]
async fn typed_tool_errors_preserve_input_and_operational_categories() {
    let server = Server::start().await;
    let client = server.client(true);
    let connection = timeout(Duration::from_secs(3), client.connect(server.url(PATH)))
        .await
        .unwrap()
        .unwrap();
    let (mut tx, rx) = connection.open_bi().await.unwrap().await.unwrap();
    let mut rx = reader(rx);
    for (id, kind) in [(1, "invalid"), (2, "unavailable"), (3, "internal")] {
        send(
            &mut tx,
            &modern(
                id,
                "tools/call",
                json!({"name":"failure","arguments":{"kind":kind}}),
            ),
        )
        .await;
        let result = response(&mut rx).await;
        assert_eq!(result["id"], id);
        if kind == "invalid" {
            assert_eq!(result["error"]["code"], -32602);
            assert!(result.get("result").is_none());
        } else {
            assert!(result.get("error").is_none());
            assert_eq!(result["result"]["isError"], true);
            assert_eq!(result["result"]["structuredContent"]["code"], kind);
        }
    }
    connection.close(0_u32.into(), b"done");
}

#[tokio::test]
async fn both_revisions_call_the_same_registered_tool() {
    let server = Server::start().await;
    for legacy in [false, true] {
        let client = server.client(true);
        let connection = timeout(Duration::from_secs(3), client.connect(server.url(PATH)))
            .await
            .unwrap()
            .unwrap();
        let (mut tx, rx) = connection.open_bi().await.unwrap().await.unwrap();
        let mut rx = reader(rx);
        if legacy {
            send(&mut tx, &json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"fixture","version":"1"}}})).await;
            assert_eq!(
                response(&mut rx).await["result"]["protocolVersion"],
                "2025-11-25"
            );
            send(
                &mut tx,
                &json!({"jsonrpc":"2.0","method":"notifications/initialized"}),
            )
            .await;
        }
        let request = if legacy {
            json!({"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"server_info","arguments":{}}})
        } else {
            modern(
                2,
                "tools/call",
                json!({"name":"server_info","arguments":{}}),
            )
        };
        send(&mut tx, &request).await;
        let result = response(&mut rx).await;
        assert!(result.get("error").is_none(), "{result}");
        assert_eq!(result["result"]["structuredContent"]["synthetic"], true);
        connection.close(0_u32.into(), b"done");
    }
}

#[tokio::test]
async fn rejects_wrong_path_origin_and_untrusted_certificate() {
    let server = Server::start().await;
    let client = server.client(true);
    for options in [
        ConnectOptions::builder(server.url("/wrong")).build(),
        ConnectOptions::builder(server.url(PATH))
            .add_header("origin", "https://denied.test")
            .build(),
    ] {
        assert!(
            timeout(Duration::from_secs(3), client.connect(options))
                .await
                .unwrap()
                .is_err()
        );
    }
    let untrusted = server.client(false);
    assert!(
        timeout(Duration::from_secs(3), untrusted.connect(server.url(PATH)))
            .await
            .unwrap()
            .is_err()
    );
}

#[tokio::test]
async fn rejects_extra_stream_and_oversized_frame_without_dispatch() {
    let server = Server::start().await;
    let client = server.client(true);
    for extra_stream in [false, true] {
        let connection = client.connect(server.url(PATH)).await.unwrap();
        let (mut tx, rx) = connection.open_bi().await.unwrap().await.unwrap();
        if extra_stream {
            send(&mut tx, &modern(1, "tools/list", json!({}))).await;
            assert_eq!(response(&mut reader(rx)).await["id"], 1);
            let (mut second, _) = connection.open_bi().await.unwrap().await.unwrap();
            send(&mut second, &modern(2, "tools/list", json!({}))).await;
        } else {
            tx.write_all(&vec![b'x'; 4097]).await.unwrap();
        }
        timeout(Duration::from_secs(3), connection.closed())
            .await
            .unwrap();
    }
    assert_eq!(
        server
            .context
            .handler
            .counters
            .calls
            .load(std::sync::atomic::Ordering::Relaxed),
        0
    );
}

#[tokio::test]
async fn cancellation_releases_admission_and_drops_late_reply() {
    let server = Server::start().await;
    let client = server.client(true);
    let connection = client.connect(server.url(PATH)).await.unwrap();
    let (mut tx, rx) = connection.open_bi().await.unwrap().await.unwrap();
    let mut rx = reader(rx);
    send(
        &mut tx,
        &modern(2, "tools/call", json!({"name":"slow","arguments":{}})),
    )
    .await;
    // A second request proves that the slow request reached the concurrent service loop.
    send(&mut tx, &modern(3, "tools/list", json!({}))).await;
    assert_eq!(response(&mut rx).await["id"], 3);
    send(
        &mut tx,
        &json!({"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":2}}),
    )
    .await;
    send(&mut tx, &modern(4, "tools/list", json!({}))).await;
    assert_eq!(response(&mut rx).await["id"], 4);
    timeout(Duration::from_secs(2), async {
        loop {
            if server.context.requests.available_permits() == server.context.limits.max_in_flight {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn rejects_duplicate_active_ids_and_mixed_lifecycles() {
    let server = Server::start().await;
    let client = server.client(true);
    for duplicate in [false, true] {
        let connection = client.connect(server.url(PATH)).await.unwrap();
        let (mut tx, rx) = connection.open_bi().await.unwrap().await.unwrap();
        if duplicate {
            send(
                &mut tx,
                &modern(1, "tools/call", json!({"name":"slow","arguments":{}})),
            )
            .await;
            send(&mut tx, &modern(1, "tools/list", json!({}))).await;
        } else {
            send(&mut tx, &modern(1, "tools/list", json!({}))).await;
            assert_eq!(response(&mut reader(rx)).await["id"], 1);
            send(&mut tx, &json!({"jsonrpc":"2.0","id":2,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"fixture","version":"1"}}})).await;
        }
        timeout(Duration::from_secs(3), connection.closed())
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn connection_admission_is_bounded_before_sdk_task_dispatch() {
    let server = Server::start().await;
    let client = server.client(true);
    let connection = client.connect(server.url(PATH)).await.unwrap();
    let (mut tx, _rx) = connection.open_bi().await.unwrap().await.unwrap();
    for id in 0..=server.context.limits.max_calls_per_connection {
        send(
            &mut tx,
            &modern(
                id as u64,
                "tools/call",
                json!({"name":"slow","arguments":{}}),
            ),
        )
        .await;
    }
    timeout(Duration::from_secs(3), connection.closed())
        .await
        .unwrap();
    assert!(
        server
            .context
            .handler
            .counters
            .calls
            .load(std::sync::atomic::Ordering::Relaxed)
            <= server.context.limits.max_calls_per_connection as u64
    );
    timeout(Duration::from_secs(3), async {
        while server.context.requests.available_permits() != server.context.limits.max_in_flight {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn shutdown_closes_an_active_session_and_releases_connection_admission() {
    let mut server = Server::start().await;
    let client = server.client(true);
    let connection = client.connect(server.url(PATH)).await.unwrap();
    let (mut tx, _rx) = connection.open_bi().await.unwrap().await.unwrap();
    send(
        &mut tx,
        &modern(1, "tools/call", json!({"name":"slow","arguments":{}})),
    )
    .await;
    server.wait_for_plugin().await;
    server.shutdown.cancel();
    timeout(Duration::from_secs(3), connection.closed())
        .await
        .unwrap();
    timeout(Duration::from_secs(3), &mut server.task)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        server.context.connections.available_permits(),
        server.context.limits.max_connections
    );
    server.assert_cleanup_complete();
}

#[tokio::test]
async fn terminal_peer_events_join_handlers_before_releasing_the_session() {
    let server = Server::start().await;
    let client = server.client(true);
    for extra_stream in [false, true] {
        let connection = client.connect(server.url(PATH)).await.unwrap();
        let (mut tx, _rx) = connection.open_bi().await.unwrap().await.unwrap();
        send(
            &mut tx,
            &modern(1, "tools/call", json!({"name":"slow","arguments":{}})),
        )
        .await;
        server.wait_for_plugin().await;
        if extra_stream {
            let (mut extra, _rx) = connection.open_bi().await.unwrap().await.unwrap();
            send(&mut extra, &modern(2, "tools/list", json!({}))).await;
            timeout(Duration::from_secs(2), connection.closed())
                .await
                .unwrap();
        } else {
            connection.close(0_u32.into(), b"fixture disconnect");
        }
        timeout(Duration::from_secs(2), async {
            while server.context.connections.available_permits()
                != server.context.limits.max_connections
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        server.assert_cleanup_complete();
    }
}
