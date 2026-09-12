//! Native reference client: wt_client URL CA_PATH REVISION [ORIGIN] [--demo].

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
    let demo = args.last().is_some_and(|arg| arg == "--demo");
    if demo {
        args.pop();
    }
    if !(3..=4).contains(&args.len()) {
        return Err(
            io::Error::other("usage: wt_client URL CA_PATH REVISION [ORIGIN] [--demo]").into(),
        );
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
    let budget = Arc::new(Semaphore::new(16 * 1024 * 1024));
    let mut reader = FrameReader::new(
        recv,
        4 * 1024 * 1024,
        budget.clone(),
        Duration::from_secs(10),
        Duration::from_secs(10),
    );
    if revision == "2025-11-25" {
        exchange(&mut send, &mut reader, &budget, json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":revision,"capabilities":{},"clientInfo":{"name":"openlegal-wt-client","version":"1"}}})).await?;
        write_json(
            &mut send,
            &json!({"jsonrpc":"2.0","method":"notifications/initialized"}),
            1024 * 1024,
            &budget,
            Duration::from_secs(10),
        )
        .await?;
    }
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
        println!("{response}");
    }
    connection.close(0_u32.into(), b"reference client complete");
    endpoint.close(0_u32.into(), b"reference client complete");
    timeout(Duration::from_secs(2), endpoint.wait_idle()).await?;
    Ok(())
}

async fn exchange(
    send: &mut wtransport::SendStream,
    reader: &mut FrameReader<wtransport::RecvStream>,
    budget: &Arc<Semaphore>,
    request: Value,
) -> Result<Value, ServerError> {
    write_json(send, &request, 1024 * 1024, budget, Duration::from_secs(10)).await?;
    let mut notifications = 0;
    let mut total_bytes = 0;
    let mut last_progress = 0.0;
    let response: Value = loop {
        let frame = reader
            .read()
            .await?
            .ok_or_else(|| io::Error::other("server closed before response"))?;
        total_bytes += frame.bytes.len();
        if total_bytes > 4 * 1024 * 1024 {
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
