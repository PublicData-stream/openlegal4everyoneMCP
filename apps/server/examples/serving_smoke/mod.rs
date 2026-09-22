//! Bounded, payload-free deployment smoke profile. Deliberately independent of
//! the reference client's opt-in synthetic-provider and stress profiles.
use std::{sync::Arc, time::Duration};

use openlegal_server::{
    config::SourceOffer,
    framing::{FrameReader, write_json},
};
use rustls::pki_types::{CertificateDer, pem::PemObject};
use serde_json::{Value, json};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite},
    sync::Semaphore,
    time::timeout,
};
use url::Url;
use wtransport::{
    ClientConfig, Endpoint, endpoint::ConnectOptions, error::ConnectingError, tls::rustls,
};

const LIMIT: usize = 16 * 1024 * 1024;
const OPERATION: Duration = Duration::from_secs(10);
const BEFORE: &str = "alpha\nbeta\n";
const AFTER: &str = "alpha\ngamma\n";
const DENIED_ORIGIN: &str = "https://smoke-denied.invalid";
const CHECKS: &[&str] = &[
    "configuration",
    "connect",
    "discovery",
    "tools_list",
    "server_info",
    "text_diff",
    "comparison_content",
    "patch_content",
    "comparison_cleanup",
    "attachment_cleanup",
    "invalid_origin",
];
type Result<T> = std::result::Result<T, &'static str>;

struct Args<'a> {
    endpoint: &'a str,
    ca: &'a str,
    revision: &'a str,
    origin: &'a str,
}

fn validate_url(raw: &str, origin: bool) -> Result<()> {
    if raw.len() > 2048
        || raw
            .bytes()
            .any(|b| b.is_ascii_control() || b.is_ascii_whitespace())
        || raw.contains('\\')
        || (origin
            && raw
                .split_once("://")
                .is_none_or(|(_, rest)| rest.contains('/')))
    {
        return Err("invalid_endpoint");
    }
    let url = Url::parse(raw).map_err(|_| "invalid_endpoint")?;
    if url.scheme() != "https"
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || url.port() == Some(0)
        || raw
            .split_once("://")
            .is_none_or(|(_, rest)| rest.split('/').next().is_some_and(|s| s.contains('@')))
        || url.path() != if origin { "/" } else { "/mcp-wt/v1" }
    {
        return Err("invalid_endpoint");
    }
    Ok(())
}

fn arguments(args: &[String]) -> Result<Args<'_>> {
    if args.len() != 5
        || args[4] != "--serving-smoke"
        || args[..4]
            .iter()
            .any(|a| a.starts_with("--") || a.is_empty())
    {
        return Err("invalid_arguments");
    }
    validate_url(&args[0], false)?;
    validate_url(&args[3], true)?;
    if !["2025-11-25", "2026-07-28"].contains(&args[2].as_str()) {
        return Err("unsupported_revision");
    }
    if Url::parse(&args[3])
        .map_err(|_| "invalid_endpoint")?
        .origin()
        .ascii_serialization()
        == DENIED_ORIGIN
    {
        return Err("denied_origin_conflict");
    }
    Ok(Args {
        endpoint: &args[0],
        ca: &args[1],
        revision: &args[2],
        origin: &args[3],
    })
}

// Admit certificates only: PEM APIs otherwise silently skip arbitrary sections.
fn certificate_roots(bytes: &[u8]) -> Result<rustls::RootCertStore> {
    let mut input = std::str::from_utf8(bytes)
        .map_err(|_| "invalid_ca_bundle")?
        .trim();
    let mut roots = rustls::RootCertStore::empty();
    while !input.is_empty() {
        if !input.starts_with("-----BEGIN CERTIFICATE-----") {
            return Err("invalid_ca_bundle");
        }
        let end = input
            .find("-----END CERTIFICATE-----")
            .ok_or("invalid_ca_bundle")?
            + "-----END CERTIFICATE-----".len();
        let cert = CertificateDer::from_pem_slice(&input.as_bytes()[..end])
            .map_err(|_| "invalid_ca_bundle")?;
        roots.add(cert).map_err(|_| "invalid_ca_bundle")?;
        input = input[end..].trim();
    }
    if roots.is_empty() {
        return Err("invalid_ca_bundle");
    }
    Ok(roots)
}

fn record(report: &mut Value, id: &str, outcome: Result<()>) {
    if let Some(checks) = report["checks"].as_array_mut() {
        for check in checks {
            if check["id"] == id {
                check["status"] = json!(if outcome.is_ok() { "passed" } else { "failed" });
                if let Err(reason) = outcome {
                    check["reason_code"] = json!(reason);
                }
            }
        }
    }
}

pub async fn run(args: &[String]) -> bool {
    let mut report = json!({"schema_version":1,"checks": CHECKS.iter().map(|id| json!({"id":id,"status":"not_run"})).collect::<Vec<_>>()});
    let result = timeout(Duration::from_secs(90), execute(args, &mut report)).await;
    if result.is_err()
        && let Some(checks) = report["checks"].as_array_mut()
    {
        checks.push(json!({"id":"deadline","status":"failed","reason_code":"run_timeout"}));
    }
    let passed = report["checks"]
        .as_array()
        .is_some_and(|checks| checks.iter().all(|c| c["status"] == "passed"));
    println!("{report}");
    passed
}

async fn execute(raw: &[String], report: &mut Value) -> Result<()> {
    let configured = async {
        let args = arguments(raw)?;
        let mut file = tokio::fs::File::open(args.ca)
            .await
            .map_err(|_| "ca_read_failed")?;
        let mut pem = Vec::new();
        (&mut file)
            .take(1024 * 1024 + 1)
            .read_to_end(&mut pem)
            .await
            .map_err(|_| "ca_read_failed")?;
        if pem.len() > 1024 * 1024 {
            return Err("invalid_ca_bundle");
        }
        let roots = certificate_roots(&pem)?;
        let mut tls = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .map_err(|_| "tls_configuration_failed")?
        .with_root_certificates(roots)
        .with_no_client_auth();
        tls.alpn_protocols = vec![wtransport::tls::WEBTRANSPORT_ALPN.to_vec()];
        let endpoint = Endpoint::client(
            ClientConfig::builder()
                .with_bind_default()
                .with_custom_tls(tls)
                .build(),
        )
        .map_err(|_| "endpoint_failed")?;
        Ok((args, endpoint))
    };
    let configured = timeout(OPERATION, configured)
        .await
        .unwrap_or(Err("operation_timeout"));
    record(
        report,
        "configuration",
        configured.as_ref().map(|_| ()).map_err(|e| *e),
    );
    let (args, endpoint) = configured?;
    let connected = timeout(OPERATION, async {
        let connection = endpoint
            .connect(
                ConnectOptions::builder(args.endpoint)
                    .add_header("origin", args.origin)
                    .build(),
            )
            .await
            .map_err(|_| "connection_failed")?;
        let streams = connection
            .open_bi()
            .await
            .map_err(|_| "stream_failed")?
            .await
            .map_err(|_| "stream_failed")?;
        Ok((connection, streams))
    })
    .await
    .unwrap_or(Err("operation_timeout"));
    record(
        report,
        "connect",
        connected.as_ref().map(|_| ()).map_err(|e| *e),
    );
    let (connection, (mut send, recv)) = connected?;
    let budget = Arc::new(Semaphore::new(2 * LIMIT + 8192));
    let mut reader = FrameReader::new(recv, LIMIT, budget.clone(), OPERATION, OPERATION);
    let mut client = Client {
        send: &mut send,
        reader: &mut reader,
        budget: &budget,
        revision: args.revision,
        id: 0,
    };
    let mut owned = Owned::default();
    // Reserve time for cleanup even when the non-mutating verification stalls.
    let positive = timeout(
        Duration::from_secs(50),
        positive(&mut client, report, &mut owned),
    )
    .await
    .unwrap_or(Err("positive_timeout"));
    if let Err(reason) = positive
        && let Some(checks) = report["checks"].as_array_mut()
    {
        checks.push(json!({"id":"positive","status":"failed","reason_code":reason}));
    }
    cleanup(&mut client, report, &owned).await;
    connection.close(0_u32.into(), b"smoke complete");
    if positive.is_ok() {
        let rejected = timeout(
            OPERATION,
            endpoint.connect(
                ConnectOptions::builder(args.endpoint)
                    .add_header("origin", DENIED_ORIGIN)
                    .build(),
            ),
        )
        .await;
        let result = match rejected {
            Ok(Err(ConnectingError::SessionRejected)) => Ok(()),
            Ok(Ok(unexpected)) => {
                unexpected.close(0_u32.into(), b"smoke complete");
                Err("origin_accepted")
            }
            Ok(Err(_)) => Err("negative_transport_failure"),
            Err(_) => Err("operation_timeout"),
        };
        record(report, "invalid_origin", result);
        if result.is_ok() {
            // wtransport does not expose a rejected CONNECT response's status.
            if let Some(checks) = report["checks"].as_array_mut()
                && let Some(check) = checks.iter_mut().find(|c| c["id"] == "invalid_origin")
            {
                check["reason_code"] = json!("transport_session_rejected");
            }
        }
    }
    endpoint.close(0_u32.into(), b"smoke complete");
    Ok(())
}

struct Client<'a> {
    send: &'a mut wtransport::SendStream,
    reader: &'a mut FrameReader<wtransport::RecvStream>,
    budget: &'a Arc<Semaphore>,
    revision: &'a str,
    id: u32,
}
impl Client<'_> {
    async fn rpc(&mut self, method: &str, mut params: Value, progress: bool) -> Result<Value> {
        self.id += 1;
        if self.revision == "2026-07-28" {
            params["_meta"] = json!({"io.modelcontextprotocol/protocolVersion":self.revision,"io.modelcontextprotocol/clientInfo":{"name":"openlegal-serving-smoke","version":"1"},"io.modelcontextprotocol/clientCapabilities":{}});
        }
        if progress {
            params["_meta"]["progressToken"] = json!(format!("serving-smoke-{}", self.id));
        }
        let request = json!({"jsonrpc":"2.0","id":self.id,"method":method,"params":params});
        timeout(
            OPERATION,
            exchange(self.send, self.reader, self.budget, &request),
        )
        .await
        .unwrap_or(Err("operation_timeout"))
    }
    async fn tool(&mut self, name: &str, arguments: Value, progress: bool) -> Result<Value> {
        Ok(self
            .rpc(
                "tools/call",
                json!({"name":name,"arguments":arguments}),
                progress,
            )
            .await?["structuredContent"]
            .clone())
    }
}

#[derive(Default)]
struct Owned {
    comparison: Option<String>,
    attachment: Option<String>,
}
fn handle(value: &Value) -> Option<String> {
    value
        .as_str()
        .filter(|s| {
            s.len() == 64
                && s.bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        })
        .map(str::to_owned)
}

async fn positive(client: &mut Client<'_>, report: &mut Value, owned: &mut Owned) -> Result<()> {
    let discovered = if client.revision == "2025-11-25" {
        client.rpc("initialize", json!({"protocolVersion":client.revision,"capabilities":{},"clientInfo":{"name":"openlegal-serving-smoke","version":"1"}}), false).await
    } else {
        client.rpc("server/discover", json!({}), false).await
    };
    let discovery = discovered.and_then(|value| {
        let version_matches = if client.revision == "2025-11-25" {
            value["protocolVersion"] == client.revision
        } else {
            value["supportedVersions"]
                .as_array()
                .is_some_and(|versions| versions.iter().any(|v| v == client.revision))
        };
        if !version_matches || value["instructions"].as_str().is_none() {
            return Err("invalid_discovery");
        }
        Ok(value)
    });
    record(
        report,
        "discovery",
        discovery.as_ref().map(|_| ()).map_err(|e| *e),
    );
    let discovery = discovery?;
    if client.revision == "2025-11-25" {
        timeout(
            OPERATION,
            write_json(
                client.send,
                &json!({"jsonrpc":"2.0","method":"notifications/initialized"}),
                LIMIT,
                client.budget,
                OPERATION,
            ),
        )
        .await
        .map_err(|_| "operation_timeout")?
        .map_err(|_| "write_failed")?;
    }
    let listed = client
        .rpc("tools/list", json!({}), false)
        .await
        .and_then(|value| {
            let tools = value["tools"].as_array().ok_or("missing_tools")?;
            for name in [
                "server_info",
                "text.diff",
                "text.diff.page",
                "text.diff.delete",
                "text.attachment.read",
                "text.attachment.delete",
            ] {
                if !tools.iter().any(|t| t["name"] == name) {
                    return Err("missing_tools");
                }
            }
            Ok(())
        });
    record(report, "tools_list", listed);
    listed?;
    let info = client
        .tool("server_info", json!({}), false)
        .await
        .and_then(|info| {
            let source = info["sourceUrl"].as_str().ok_or("invalid_source_offer")?;
            SourceOffer::new(source).map_err(|_| "invalid_source_offer")?;
            let instructions = discovery["instructions"]
                .as_str()
                .ok_or("invalid_discovery")?;
            if info["license"] != SourceOffer::LICENSE
                || info["licenseUrl"] != SourceOffer::LICENSE_URL
                || ![source, SourceOffer::LICENSE, SourceOffer::LICENSE_URL]
                    .iter()
                    .all(|s| instructions.contains(s))
            {
                return Err("invalid_source_offer");
            }
            Ok(())
        });
    record(report, "server_info", info);
    info?;
    let diff = client
        .tool("text.diff", json!({"before":BEFORE,"after":AFTER}), true)
        .await
        .and_then(|diff| {
            owned.comparison = handle(&diff["comparison"]["comparison_id"]);
            owned.attachment = handle(&diff["patch"]["attachment_id"]);
            if owned.comparison.is_none()
                || owned.attachment.is_none()
                || diff["schema_version"] != 1
                || diff["comparison"]["equal"] != false
                || diff["comparison"]["additions"] != 1
                || diff["comparison"]["deletions"] != 1
                || diff["patch"]["kind"] != "patch"
                || diff["patch"]["sealed"] != true
            {
                return Err("invalid_comparison");
            }
            Ok(())
        });
    record(report, "text_diff", diff);
    diff?;
    let content = async {
        for (view, expected) in [("before", BEFORE), ("after", AFTER)] {
            let page = client
                .tool(
                    "text.diff.page",
                    json!({"comparison_id":owned.comparison,"page":0,"view":view}),
                    false,
                )
                .await?;
            if page["comparison_id"].as_str() != owned.comparison.as_deref()
                || page["page"] != 0
                || page["total_pages"] != 1
                || page["view"] != view
                || page["text"] != expected
            {
                return Err("invalid_comparison_content");
            }
        }
        Ok(())
    }
    .await;
    record(report, "comparison_content", content);
    content?;
    let patch = client
        .tool(
            "text.attachment.read",
            json!({"attachment_id":owned.attachment,"offset":0}),
            false,
        )
        .await
        .and_then(|page| {
            let patch = page["text"].as_str().ok_or("invalid_patch_content")?;
            if page["attachment"]["attachment_id"].as_str() != owned.attachment.as_deref()
                || page["offset"] != 0
                || page["complete"] != true
                || page["next_offset"].as_u64() != Some(patch.len() as u64)
                || openlegal_normalization::patch::apply_patch(BEFORE, patch)
                    .map_err(|_| "invalid_patch_content")?
                    != AFTER
            {
                return Err("invalid_patch_content");
            }
            Ok(())
        });
    record(report, "patch_content", patch);
    patch
}

async fn cleanup(client: &mut Client<'_>, report: &mut Value, owned: &Owned) {
    for (id, name, field, handle) in [
        (
            "comparison_cleanup",
            "text.diff.delete",
            "comparison_id",
            &owned.comparison,
        ),
        (
            "attachment_cleanup",
            "text.attachment.delete",
            "attachment_id",
            &owned.attachment,
        ),
    ] {
        if let Some(handle) = handle {
            let result = client
                .tool(name, json!({field:handle}), false)
                .await
                .and_then(|value| {
                    if value["deleted"] == true {
                        Ok(())
                    } else {
                        Err("cleanup_not_confirmed")
                    }
                });
            record(report, id, result);
        }
    }
}

#[derive(Default)]
struct Responses {
    notifications: usize,
    bytes: usize,
    last_progress: f64,
}
impl Responses {
    fn accept(&mut self, bytes: &[u8], request: &Value) -> Result<Option<Value>> {
        self.bytes = self
            .bytes
            .checked_add(bytes.len())
            .ok_or("response_limit")?;
        if self.bytes > LIMIT {
            return Err("response_limit");
        }
        let value: Value = serde_json::from_slice(bytes).map_err(|_| "malformed_json")?;
        if value["jsonrpc"] != "2.0" {
            return Err("invalid_rpc");
        }
        if value["method"] == "notifications/progress" {
            self.notifications += 1;
            let progress = value["params"]["progress"]
                .as_f64()
                .ok_or("invalid_progress")?;
            let token = request
                .pointer("/params/_meta/progressToken")
                .ok_or("unexpected_progress")?;
            if value.get("id").is_some()
                || self.notifications > 5
                || !progress.is_finite()
                || progress <= self.last_progress
                || &value["params"]["progressToken"] != token
            {
                return Err("invalid_progress");
            }
            if let Some(total) = value["params"].get("total")
                && total
                    .as_f64()
                    .is_none_or(|total| !total.is_finite() || total < progress)
            {
                return Err("invalid_progress");
            }
            self.last_progress = progress;
            return Ok(None);
        }
        if value.get("method").is_some()
            || value["id"] != request["id"]
            || value.get("error").is_some()
            || !value["result"].is_object()
            || value["result"]["isError"] == true
        {
            return Err("invalid_rpc");
        }
        if request.pointer("/params/_meta/progressToken").is_some() && self.notifications == 0 {
            return Err("missing_progress");
        }
        Ok(Some(value["result"].clone()))
    }
}

async fn exchange<W: AsyncWrite + Unpin, R: AsyncRead + Unpin>(
    send: &mut W,
    reader: &mut FrameReader<R>,
    budget: &Arc<Semaphore>,
    request: &Value,
) -> Result<Value> {
    write_json(send, request, LIMIT, budget, OPERATION)
        .await
        .map_err(|_| "write_failed")?;
    let mut responses = Responses::default();
    loop {
        let frame = reader
            .read()
            .await
            .map_err(|_| "frame_failed")?
            .ok_or("unexpected_eof")?;
        if let Some(result) = responses.accept(&frame.bytes, request)? {
            return Ok(result);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn args() -> Vec<String> {
        [
            "https://localhost:4433/mcp-wt/v1",
            "ca.pem",
            "2026-07-28",
            "https://client.invalid",
            "--serving-smoke",
        ]
        .map(str::to_owned)
        .to_vec()
    }
    #[test]
    fn validates_explicit_arguments() {
        assert!(arguments(&args()).is_ok());
        for bad in [
            "http://localhost/mcp-wt/v1",
            "https://secret@localhost/mcp-wt/v1",
            "https://localhost/mcp-wt/v1?token=secret",
            "https://localhost/mcp-wt/v1#fragment",
            "https://localhost/other",
            "https://localhost:0/mcp-wt/v1",
            "https://localhost\\evil/mcp-wt/v1",
        ] {
            let mut a = args();
            a[0] = bad.into();
            assert!(arguments(&a).is_err());
        }
        let mut a = args();
        a[3] = DENIED_ORIGIN.into();
        assert!(arguments(&a).is_err());
        for bad in [
            "http://client.test",
            "https://client.test/",
            "https://client.test/path",
        ] {
            let mut a = args();
            a[3] = bad.into();
            assert!(arguments(&a).is_err());
        }
        let mut a = args();
        a.push("--demo".into());
        assert!(arguments(&a).is_err());
        let mut a = args();
        a.remove(3);
        assert!(arguments(&a).is_err());
    }
    #[test]
    fn ca_bundle_requires_only_valid_certificates() {
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()])
            .unwrap()
            .cert
            .pem();
        assert_eq!(
            certificate_roots(format!("{cert}\n{cert}").as_bytes())
                .unwrap()
                .len(),
            2
        );
        for invalid in [
            "",
            "garbage",
            "-----BEGIN CERTIFICATE-----\ninvalid\n-----END CERTIFICATE-----",
            "-----BEGIN PRIVATE KEY-----\nsecret\n-----END PRIVATE KEY-----",
        ] {
            assert!(certificate_roots(invalid.as_bytes()).is_err());
            assert!(certificate_roots(format!("{cert}{invalid}garbage").as_bytes()).is_err());
        }
    }
    fn request() -> Value {
        json!({"jsonrpc":"2.0","id":1,"params":{"_meta":{"progressToken":"smoke"}}})
    }
    fn notification(progress: usize) -> Vec<u8> {
        serde_json::to_vec(&json!({"jsonrpc":"2.0","method":"notifications/progress","params":{"progressToken":"smoke","progress":progress}})).unwrap()
    }
    #[test]
    fn progress_and_final_response_are_strictly_bounded_and_correlated() {
        let mut state = Responses::default();
        assert!(
            state
                .accept(&notification(1), &request())
                .unwrap()
                .is_none()
        );
        assert!(state.accept(&notification(1), &request()).is_err());
        let mut state = Responses::default();
        for progress in 1..=5 {
            assert!(state.accept(&notification(progress), &request()).is_ok());
        }
        assert!(state.accept(&notification(6), &request()).is_err());
        let mut state = Responses::default();
        assert!(
            state
                .accept(br#"{"jsonrpc":"2.0","id":1,"result":{}}"#, &request())
                .is_err()
        );
        assert!(state.accept(&notification(1), &request()).is_ok());
        assert!(
            state
                .accept(br#"{"jsonrpc":"2.0","id":2,"result":{}}"#, &request())
                .is_err()
        );
        assert!(
            state
                .accept(br#"{"jsonrpc":"2.0","id":1,"result":{}}"#, &request())
                .unwrap()
                .is_some()
        );
        assert!(
            Responses::default()
                .accept(&notification(1), &json!({"id":1}))
                .is_err()
        );
        assert!(
            Responses::default()
                .accept(b"not json", &request())
                .is_err()
        );
        assert!(
            Responses {
                bytes: LIMIT,
                ..Responses::default()
            }
            .accept(b"{}", &request())
            .is_err()
        );
    }
    #[tokio::test(start_paused = true)]
    async fn stalled_frames_are_bounded_without_eof() {
        let (mut write, read) = tokio::io::duplex(4096);
        let budget = Arc::new(Semaphore::new(2 * LIMIT + 8192));
        let mut reader = FrameReader::new(read, LIMIT, budget.clone(), OPERATION, OPERATION);
        use tokio::io::AsyncWriteExt;
        write.write_all(b"{\"jsonrpc\":").await.unwrap();
        assert!(!matches!(
            timeout(OPERATION, reader.read()).await,
            Ok(Ok(_))
        ));
    }
    #[tokio::test]
    async fn exchange_finishes_after_correlated_result_without_eof() {
        use tokio::io::AsyncWriteExt;
        let (client, mut server) = tokio::io::duplex(4096);
        let (read, mut send) = tokio::io::split(client);
        let budget = Arc::new(Semaphore::new(2 * LIMIT + 8192));
        let mut reader = FrameReader::new(read, LIMIT, budget.clone(), OPERATION, OPERATION);
        let mut bytes = notification(1);
        bytes.extend_from_slice(b"\n{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"ok\":true}}\n");
        server.write_all(&bytes).await.unwrap();
        let result = timeout(
            OPERATION,
            exchange(&mut send, &mut reader, &budget, &request()),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(result["ok"], true);
        // `server` remains open throughout the completed exchange.
        drop(server);
    }

    #[tokio::test(start_paused = true)]
    async fn progress_frames_do_not_extend_operation_deadline() {
        use tokio::io::AsyncWriteExt;
        let (client, mut server) = tokio::io::duplex(4096);
        let (read, mut send) = tokio::io::split(client);
        let budget = Arc::new(Semaphore::new(2 * LIMIT + 8192));
        let mut reader = FrameReader::new(read, LIMIT, budget.clone(), OPERATION, OPERATION);
        let remote = tokio::spawn(async move {
            for progress in 1..=3 {
                let mut bytes = notification(progress);
                bytes.push(b'\n');
                server.write_all(&bytes).await.unwrap();
                tokio::time::sleep(Duration::from_secs(9)).await;
            }
        });
        assert!(
            timeout(
                OPERATION,
                exchange(&mut send, &mut reader, &budget, &request())
            )
            .await
            .is_err()
        );
        remote.abort();
    }
}
