//! Local fictional HTTP provider. Never bind to a non-loopback interface.
mod support;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let address: std::net::SocketAddr = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:8081".into())
        .parse()?;
    if !address.ip().is_loopback() {
        return Err("mock must bind a literal loopback address".into());
    }
    let listener = tokio::net::TcpListener::bind(address).await?;
    println!("Synthetic mock listening on {}", listener.local_addr()?);
    axum::serve(listener, support::router())
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    Ok(())
}
