//! Native reference client: wt_client URL CA_PATH REVISION [ORIGIN] [--demo] [--text-diff].
//! Deployment profile: wt_client URL CA_PATH REVISION ORIGIN --serving-smoke.

mod serving_smoke;

use openlegal_server::{
    ServerError,
    framing::{FrameReader, write_json},
};
use serde_json::{Value, json};
use std::{io, sync::Arc, time::Duration};
use tokio::{sync::Semaphore, time::timeout};
use wtransport::{
    ClientConfig, Endpoint,
    endpoint::ConnectOptions,
    tls::{Certificate, rustls},
};

#[tokio::main]
async fn main() -> Result<(), ServerError> {
    let mut args: Vec<String> = std::env::args().skip(1).collect();
    if args.iter().any(|arg| arg == "--serving-smoke") {
        let passed = serving_smoke::run(&args).await;
        if !passed {
            std::process::exit(1);
        }
        return Ok(());
    }
    let demo = args.iter().any(|arg| arg == "--demo");
    let text_diff = args.iter().any(|arg| arg == "--text-diff");
    args.retain(|arg| arg != "--demo" && arg != "--text-diff");
    if !(3..=4).contains(&args.len()) {
        return Err(io::Error::other(
            "usage: wt_client URL CA_PATH REVISION [ORIGIN] [--demo] [--text-diff]",
        )
        .into());
    }
    let revision = &args[2];
    if revision != "2025-11-25" && revision != "2026-07-28" {
        return Err(io::Error::other("unsupported revision").into());
    }
    let certificate = Certificate::load_pemfile(&args[1]).await?;
    let mut roots = rustls::RootCertStore::empty();
    roots.add(rustls::pki_types::CertificateDer::from(
        certificate.der().to_vec(),
    ))?;
    let mut tls = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()?
    .with_root_certificates(roots)
    .with_no_client_auth();
    tls.alpn_protocols = vec![wtransport::tls::WEBTRANSPORT_ALPN.to_vec()];
    let endpoint = Endpoint::client(
        ClientConfig::builder()
            .with_bind_default()
            .with_custom_tls(tls)
            .build(),
    )?;
    let mut options = ConnectOptions::builder(&args[0]);
    if let Some(origin) = args.get(3) {
        options = options.add_header("origin", origin);
    }
    let connection = timeout(Duration::from_secs(10), endpoint.connect(options.build())).await??;
    let (mut send, recv) = timeout(Duration::from_secs(10), async {
        Ok::<_, ServerError>(connection.open_bi().await?.await?)
    })
    .await??;
    let budget = Arc::new(Semaphore::new(256 * 1024 * 1024));
    let mut reader = FrameReader::new(
        recv,
        MAX_MESSAGE_BYTES,
        budget.clone(),
        Duration::from_secs(10),
        Duration::from_secs(10),
    );
    let discovery = if revision == "2025-11-25" {
        let response = exchange(&mut send, &mut reader, &budget, json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":revision,"capabilities":{},"clientInfo":{"name":"openlegal-wt-client","version":"1"}}})).await?;
        write_json(
            &mut send,
            &json!({"jsonrpc":"2.0","method":"notifications/initialized"}),
            1024 * 1024,
            &budget,
            Duration::from_secs(10),
        )
        .await?;
        response
    } else {
        exchange(&mut send, &mut reader, &budget, json!({"jsonrpc":"2.0","id":1,"method":"server/discover","params":{"_meta":{"io.modelcontextprotocol/protocolVersion":revision,"io.modelcontextprotocol/clientInfo":{"name":"openlegal-wt-client","version":"1"},"io.modelcontextprotocol/clientCapabilities":{}}}})).await?
    };
    let instructions = discovery["result"]["instructions"]
        .as_str()
        .ok_or("license/source instructions missing")?;
    if demo {
        for (id, method, mut params) in [
            (
                4,
                "tools/call",
                json!({"name":"demo_search_records","arguments":{"source":"layout_a"}}),
            ),
            (
                5,
                "tools/call",
                json!({"name":"demo_get_record","arguments":{"source":"layout_a","id":"001"}}),
            ),
            (
                6,
                "tools/call",
                json!({"name":"demo_show_records","arguments":{"records":[{"source":"layout_b","id":"001"}]}}),
            ),
            (
                7,
                "resources/read",
                json!({"uri":openlegal_server::demo::WIDGET_URI}),
            ),
        ] {
            params["_meta"] = json!({"progressToken":format!("demo-{id}")});
            if revision == "2026-07-28" {
                params["_meta"]["io.modelcontextprotocol/protocolVersion"] = json!(revision);
                params["_meta"]["io.modelcontextprotocol/clientInfo"] =
                    json!({"name":"openlegal-wt-client","version":"1"});
                params["_meta"]["io.modelcontextprotocol/clientCapabilities"] = json!({});
            }
            let response = exchange(
                &mut send,
                &mut reader,
                &budget,
                json!({"jsonrpc":"2.0","id":id,"method":method,"params":params}),
            )
            .await?;
            if method == "resources/read" {
                if response["result"]["contents"][0]["mimeType"]
                    != openlegal_server::demo::WIDGET_MIME
                {
                    return Err("demo resource missing".into());
                }
            } else if response["result"]["structuredContent"]["synthetic"] != true {
                return Err("synthetic result missing".into());
            }
            println!("Demo {method} verified");
        }
    }
    for (id, method, mut params) in [
        (2, "tools/list", json!({})),
        (
            3,
            "tools/call",
            json!({"name":"server_info","arguments":{}}),
        ),
    ] {
        if revision == "2026-07-28" {
            params["_meta"] = json!({"io.modelcontextprotocol/protocolVersion":revision,"io.modelcontextprotocol/clientInfo":{"name":"openlegal-wt-client","version":"1"},"io.modelcontextprotocol/clientCapabilities":{}});
        }
        let response = exchange(
            &mut send,
            &mut reader,
            &budget,
            json!({"jsonrpc":"2.0","id":id,"method":method,"params":params}),
        )
        .await?;
        if method == "tools/list"
            && !response["result"]["tools"]
                .as_array()
                .is_some_and(|tools| tools.iter().any(|tool| tool["name"] == "server_info"))
        {
            return Err(io::Error::other("server_info tool missing").into());
        }
        if method == "tools/call" {
            let info = &response["result"]["structuredContent"];
            let source = info["sourceUrl"].as_str().ok_or("source offer missing")?;
            openlegal_server::config::SourceOffer::new(source)?;
            if info["license"] != openlegal_server::config::SourceOffer::LICENSE
                || info["licenseUrl"] != openlegal_server::config::SourceOffer::LICENSE_URL
                || !instructions.contains(source)
                || !instructions.contains(openlegal_server::config::SourceOffer::LICENSE)
                || !instructions.contains(openlegal_server::config::SourceOffer::LICENSE_URL)
            {
                return Err("license/source metadata disagrees with server instructions".into());
            }
        }
        println!("WebTransport {revision}: {method} verified");
    }
    if demo && text_diff {
        history_smoke(&mut send, &mut reader, &budget, revision).await?;
    }
    if text_diff {
        text_diff_smoke(&mut send, &mut reader, &budget, revision).await?;
    }
    connection.close(0_u32.into(), b"reference client complete");
    endpoint.close(0_u32.into(), b"reference client complete");
    timeout(Duration::from_secs(2), endpoint.wait_idle()).await?;
    Ok(())
}

// This reference client can exercise the opt-in comparison profile; server
// defaults stay unchanged. Resource JSON and escaped maximum inputs fit here.
const MAX_MESSAGE_BYTES: usize = 16 * 1024 * 1024;

async fn rpc(
    send: &mut wtransport::SendStream,
    reader: &mut FrameReader<wtransport::RecvStream>,
    budget: &Arc<Semaphore>,
    revision: &str,
    method: &str,
    mut params: Value,
) -> Result<Value, ServerError> {
    if revision == "2026-07-28" {
        let progress_token = params
            .get("_meta")
            .and_then(|m| m.get("progressToken"))
            .cloned();
        params["_meta"] = json!({
            "io.modelcontextprotocol/protocolVersion":revision,
            "io.modelcontextprotocol/clientInfo":{"name":"openlegal-wt-client","version":"1"},
            "io.modelcontextprotocol/clientCapabilities":{}
        });
        if let Some(token) = progress_token {
            params["_meta"]["progressToken"] = token;
        }
    }
    // Only fixed operation names are logged; supplied texts and bearer handles
    // must never enter reference-client diagnostics.
    let operation = params.get("name").and_then(Value::as_str).unwrap_or(method);
    println!("WebTransport {revision}: text comparison operation {operation}");
    // Calls are sequential, so this ID has no active predecessor.
    let response = exchange(
        send,
        reader,
        budget,
        json!({"jsonrpc":"2.0","id":20,"method":method,"params":params}),
    )
    .await?;
    Ok(response["result"].clone())
}

async fn text_diff_smoke(
    send: &mut wtransport::SendStream,
    reader: &mut FrameReader<wtransport::RecvStream>,
    budget: &Arc<Semaphore>,
    revision: &str,
) -> Result<(), ServerError> {
    let listed = rpc(send, reader, budget, revision, "tools/list", json!({})).await?;
    let tools = listed["tools"].as_array().ok_or("tool discovery missing")?;
    for name in [
        "compare_texts",
        "show_text_diff",
        "get_text_diff_page",
        "delete_text_diff",
    ] {
        let tool = tools
            .iter()
            .find(|tool| tool["name"] == name)
            .ok_or("comparison tool missing")?;
        if name == "delete_text_diff"
            && (tool["annotations"]["readOnlyHint"] != false
                || tool["annotations"]["destructiveHint"] != true
                || tool["annotations"]["idempotentHint"] != true)
        {
            return Err("deletion annotations disagree with behavior".into());
        }
    }
    let blank = rpc(
        send,
        reader,
        budget,
        revision,
        "tools/call",
        json!({"name":"show_text_diff","arguments":{}}),
    )
    .await?;
    if !blank["structuredContent"]["comparison"].is_null() {
        return Err("blank comparison editor missing".into());
    }
    let compared = rpc(send, reader, budget, revision, "tools/call",
        json!({"name":"compare_texts","arguments":{"before":"first\nold\n","after":"first\nnew\n"}})).await?;
    let summary = &compared["structuredContent"];
    if summary["schema_version"] != 1
        || summary["equal"] != false
        || summary["additions"] != 1
        || summary["deletions"] != 1
    {
        return Err("comparison summary mismatch".into());
    }
    let handle = summary["comparison_id"]
        .as_str()
        .ok_or("comparison handle missing")?;
    let shown = rpc(
        send,
        reader,
        budget,
        revision,
        "tools/call",
        json!({"name":"show_text_diff","arguments":{"comparison_id":handle}}),
    )
    .await?;
    if shown["structuredContent"]["comparison"]["comparison_id"] != handle {
        return Err("existing comparison editor mismatch".into());
    }
    for view in ["changes", "before", "after"] {
        let result = rpc(send, reader, budget, revision, "tools/call",
            json!({"name":"get_text_diff_page","arguments":{"comparison_id":handle,"view":view,"page":0}})).await?;
        let page = &result["structuredContent"];
        if page["schema_version"] != 1
            || page["comparison_id"] != handle
            || page["view"] != view
            || page["page"] != 0
            || page["total_pages"] != 1
            || serde_json::to_vec(page)?.len() > 256 * 1024
        {
            return Err("comparison page mismatch or overflow".into());
        }
        if view == "changes" {
            if page["fragments"][0]["inline_changes"]
                != json!([
                    {"row_index":1,"ranges":[[0,3]]},
                    {"row_index":2,"ranges":[[0,3]]}
                ])
            {
                return Err("text comparison scalar annotations mismatch".into());
            }
            let patch = page["fragments"][0]["patch"]
                .as_str()
                .ok_or("change fragment missing")?;
            if !patch.contains("-old") || !patch.contains("+new") {
                return Err("change fragment content mismatch".into());
            }
        } else if page["text"]
            != if view == "before" {
                "first\nold\n"
            } else {
                "first\nnew\n"
            }
        {
            return Err("original text page mismatch".into());
        }
    }
    for _ in 0..2 {
        let deleted = rpc(
            send,
            reader,
            budget,
            revision,
            "tools/call",
            json!({"name":"delete_text_diff","arguments":{"comparison_id":handle}}),
        )
        .await?;
        if deleted["structuredContent"]["schema_version"] != 1
            || deleted["structuredContent"]["deleted"] != true
        {
            return Err("idempotent deletion failed".into());
        }
    }
    println!("WebTransport {revision}: comparing maximum-size supplied texts");
    // 1 MiB per input, including line terminators; JSON escapes nearly 12 MiB.
    let large = format!("{}\n", "\u{1}".repeat(1023)).repeat(1024);
    let maximum = rpc(
        send,
        reader,
        budget,
        revision,
        "tools/call",
        json!({"name":"compare_texts","arguments":{"before":large,"after":large}}),
    )
    .await?;
    let summary = &maximum["structuredContent"];
    if summary["equal"] != true
        || summary["before"]["bytes"] != 1024 * 1024
        || summary["after"]["bytes"] != 1024 * 1024
        || summary["additions"] != 0
        || summary["deletions"] != 0
    {
        return Err("maximum input comparison mismatch".into());
    }
    let deleted = rpc(
        send,
        reader,
        budget,
        revision,
        "tools/call",
        json!({"name":"delete_text_diff","arguments":{"comparison_id":summary["comparison_id"]}}),
    )
    .await?;
    if deleted["structuredContent"]["deleted"] != true {
        return Err("maximum input result deletion failed".into());
    }
    let server_info = rpc(
        send,
        reader,
        budget,
        revision,
        "tools/call",
        json!({"name":"server_info","arguments":{}}),
    )
    .await?;
    let source_url = server_info["structuredContent"]["sourceUrl"]
        .as_str()
        .ok_or("source offer missing")?;
    println!("WebTransport {revision}: reading comparison widget resource");
    let resource = rpc(
        send,
        reader,
        budget,
        revision,
        "resources/read",
        json!({"uri":"ui://openlegal/text-diff-v1.html"}),
    )
    .await?;
    let contents = &resource["contents"][0];
    let html = contents["text"]
        .as_str()
        .ok_or("comparison resource missing")?;
    if contents["mimeType"] != "text/html;profile=mcp-app"
        || !html.contains(
            &source_url
                .replace('&', "&amp;")
                .replace('<', "&lt;")
                .replace('>', "&gt;")
                .replace('"', "&quot;")
                .replace('\'', "&#39;"),
        )
        || html.contains("__OPENLEGAL_SOURCE_URL__")
    {
        return Err("comparison resource metadata mismatch".into());
    }
    println!("WebTransport {revision}: comparison lifecycle, maximum inputs and resource verified");
    Ok(())
}

async fn exchange(
    send: &mut wtransport::SendStream,
    reader: &mut FrameReader<wtransport::RecvStream>,
    budget: &Arc<Semaphore>,
    request: Value,
) -> Result<Value, ServerError> {
    write_json(
        send,
        &request,
        MAX_MESSAGE_BYTES,
        budget,
        Duration::from_secs(10),
    )
    .await?;
    let mut notifications = 0;
    let mut total_bytes = 0;
    let mut last_progress = 0.0;
    let response: Value = loop {
        let frame = reader
            .read()
            .await?
            .ok_or_else(|| io::Error::other("server closed before response"))?;
        total_bytes += frame.bytes.len();
        if total_bytes > MAX_MESSAGE_BYTES {
            return Err("response stream exceeds limit".into());
        }
        let response: Value = serde_json::from_slice(&frame.bytes)?;
        if response["method"] == "notifications/progress" {
            notifications += 1;
            let progress = response["params"]["progress"]
                .as_f64()
                .ok_or("invalid progress")?;
            if notifications > 5
                || progress <= last_progress
                || response["params"]["progressToken"]
                    != request["params"]["_meta"]["progressToken"]
            {
                return Err("invalid or excessive progress".into());
            }
            last_progress = progress;
            continue;
        }
        break response;
    };
    if response["id"] != request["id"]
        || response.get("error").is_some()
        || response["result"]["isError"] == true
        || response.get("result").is_none()
    {
        return Err(io::Error::other("unexpected or failed MCP response").into());
    }
    if request["method"] == "tools/call"
        && request.pointer("/params/_meta/progressToken").is_some()
        && notifications == 0
    {
        return Err("successful token-bearing tool call emitted no progress".into());
    }
    Ok(response)
}

async fn history_smoke(
    send: &mut wtransport::SendStream,
    reader: &mut FrameReader<wtransport::RecvStream>,
    budget: &Arc<Semaphore>,
    revision: &str,
) -> Result<(), ServerError> {
    let tools = rpc(send, reader, budget, revision, "tools/list", json!({})).await?;
    if !tools["tools"]
        .as_array()
        .is_some_and(|tools| tools.iter().any(|t| t["name"] == "demo_list_snapshots"))
    {
        return Ok(());
    }
    let query = json!({"operation":"get","source":"layout_a","id":"001"});
    let listed = rpc(send, reader, budget, revision, "tools/call", json!({"name":"demo_list_snapshots","arguments":{"query":query},"_meta":{"progressToken":"history-list"}})).await?;
    let snapshot = listed["structuredContent"]["snapshots"][0]["snapshot_id"]
        .as_str()
        .ok_or("history snapshot missing")?;
    let exact = rpc(send, reader, budget, revision, "tools/call", json!({"name":"demo_get_snapshot","arguments":{"query":query,"snapshot_id":snapshot},"_meta":{"progressToken":"history-get"}})).await?;
    if exact["structuredContent"]["historical"] != true
        || exact["structuredContent"].get("freshness").is_some()
    {
        return Err("historical envelope invalid".into());
    }
    let compared = rpc(send, reader, budget, revision, "tools/call", json!({"name":"demo_compare_record_snapshots","arguments":{"source":"layout_a","id":"001","before_snapshot_id":snapshot,"after_snapshot_id":snapshot},"_meta":{"progressToken":"history-compare"}})).await?;
    let summary = &compared["structuredContent"];
    if summary["equal"] != true || summary["origin"]["before"]["snapshot_id"] != snapshot {
        return Err("snapshot comparison origin invalid".into());
    }
    let deleted = rpc(
        send,
        reader,
        budget,
        revision,
        "tools/call",
        json!({"name":"delete_text_diff","arguments":{"comparison_id":summary["comparison_id"]}}),
    )
    .await?;
    if deleted["structuredContent"]["deleted"] != true {
        return Err("snapshot comparison cleanup failed".into());
    }
    println!("WebTransport {revision}: exact history and snapshot comparison verified");
    Ok(())
}
