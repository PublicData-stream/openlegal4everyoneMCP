//! Native reference client: wt_client URL CA_PATH REVISION [ORIGIN].

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
    let args: Vec<String> = std::env::args().skip(1).collect();
    if !(3..=4).contains(&args.len()) {
        return Err(io::Error::other("usage: wt_client URL CA_PATH REVISION [ORIGIN]").into());
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
    let budget = Arc::new(Semaphore::new(4 * 1024 * 1024));
    let mut reader = FrameReader::new(
        recv,
        1024 * 1024,
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
    let frame = reader
        .read()
        .await?
        .ok_or_else(|| io::Error::other("server closed before response"))?;
    let response: Value = serde_json::from_slice(&frame.bytes)?;
    if response["id"] != request["id"]
        || response.get("error").is_some()
        || response["result"]["isError"] == true
        || response.get("result").is_none()
    {
        return Err(io::Error::other("unexpected or failed MCP response").into());
    }
    Ok(response)
}
