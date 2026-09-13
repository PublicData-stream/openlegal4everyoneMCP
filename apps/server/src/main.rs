use openlegal_server::{
    ServerBuilder, ServerError,
    config::{AccessPolicy, Config},
    http::{HealthEndpoint, HttpEndpoint},
    webtransport::WebTransportEndpoint,
};
use tokio_util::sync::CancellationToken;
use tracing_subscriber::{EnvFilter, prelude::*};

fn main() -> Result<(), ServerError> {
    let mut args = std::env::args_os().skip(1);
    let path = args.next().ok_or("usage: openlegal-server CONFIG.toml")?;
    if args.next().is_some() {
        return Err("usage: openlegal-server CONFIG.toml".into());
    }
    if path == "--text-diff-worker" {
        openlegal_adapters::text_diff::run_worker()?;
        return Ok(());
    }
    run_server(path)
}

#[tokio::main]
async fn run_server(path: std::ffi::OsString) -> Result<(), ServerError> {
    // SDK diagnostics can include caller payloads even at info/warn; never enable them here.
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    logging_subscriber(filter, std::io::stdout).init();
    let config: Config = toml::from_str(&tokio::fs::read_to_string(path).await?)?;
    config.limits.validate()?;
    if let Some(diff) = &config.text_diff {
        diff.validate(&config.limits)?;
    }
    let mut registry = openlegal_server::registry::server_info_registry(config.source.url.clone())?;
    let diff_service = if config.text_diff.is_some() {
        Some(openlegal_server::text_diff::service(&std::env::current_exe()?).await?)
    } else {
        None
    };
    if let Some(service) = &diff_service {
        registry.register_module(openlegal_server::text_diff::TextDiffTools {
            service: service.clone(),
        })?;
    }
    let demo_service = config
        .demo
        .as_ref()
        .map(|demo| openlegal_server::demo::service(&demo.upstream))
        .transpose()?;
    if let Some(service) = &demo_service {
        registry.register_module(openlegal_server::demo::DemoTools {
            service: service.clone(),
        })?;
    }
    let mut resources = openlegal_server::resources::ResourceRegistry::new();
    if let Some(demo) = &config.demo {
        resources.extend(
            openlegal_server::demo::load_widget(&demo.widget_html, &config.source.url).await?,
        )?;
    }
    if let Some(diff) = &config.text_diff {
        resources.extend(
            openlegal_server::text_diff::load_widget(&diff.widget_html, &config.source.url).await?,
        )?;
    }
    let mut builder = ServerBuilder::new(registry, config.limits, config.source.url.clone())
        .with_resources(resources);
    if let Some(service) = &diff_service {
        let service = service.clone();
        builder.register_worker("text_diff", move |shutdown| async move {
            service.run(shutdown).await?;
            Ok(())
        })?;
    }
    if let Some(service) = &demo_service {
        let service = service.clone();
        builder.register_worker("retrieval", move |shutdown| async move {
            service.run(shutdown).await?;
            Ok(())
        })?;
    }
    builder.register_endpoint(HttpEndpoint {
        bind: config.http.bind,
        access: AccessPolicy {
            allowed_hosts: config.http.allowed_hosts,
            allowed_origins: config.http.allowed_origins,
        },
    })?;
    builder.register_endpoint(WebTransportEndpoint {
        bind: config.webtransport.bind,
        certificate: config.webtransport.certificate,
        private_key: config.webtransport.private_key,
        access: AccessPolicy {
            allowed_hosts: config.webtransport.allowed_hosts,
            allowed_origins: config.webtransport.allowed_origins,
        },
    })?;
    let health = HealthEndpoint {
        bind: config.health.bind,
    };
    if demo_service.is_some() || diff_service.is_some() {
        builder.register_endpoint(health.with_metrics(move || {
            let mut metrics = String::new();
            if let Some(service) = &demo_service {
                metrics.push_str(&service.metrics_prometheus());
            }
            if let Some(service) = &diff_service {
                metrics.push_str(&service.metrics_prometheus());
            }
            metrics
        }))?;
    } else {
        builder.register_endpoint(health)?;
    }
    let server = builder.bind().await?;
    for (id, addresses) in server.addresses() {
        tracing::info!(endpoint = id, ?addresses, "listener bound");
    }
    let shutdown = CancellationToken::new();
    let signal_token = shutdown.clone();
    let signal_task = tokio::spawn(async move {
        #[cfg(unix)]
        {
            let mut terminate =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
            tokio::select! { result = tokio::signal::ctrl_c() => result?, _ = terminate.recv() => {} }
        }
        #[cfg(not(unix))]
        tokio::signal::ctrl_c().await?;
        signal_token.cancel();
        Ok::<_, std::io::Error>(())
    });
    let result = server.run(shutdown).await;
    signal_task.abort();
    let _ = signal_task.await;
    result
}

fn logging_subscriber<W>(filter: EnvFilter, writer: W) -> impl tracing::Subscriber + Send + Sync
where
    W: for<'writer> tracing_subscriber::fmt::MakeWriter<'writer> + Send + Sync + 'static,
{
    tracing_subscriber::registry().with(
        tracing_subscriber::fmt::layer()
            .with_writer(writer)
            .with_filter(filter)
            .with_filter(tracing_subscriber::filter::filter_fn(|metadata| {
                metadata.target() != "rmcp" && !metadata.target().starts_with("rmcp::")
            })),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    #[derive(Clone)]
    struct Capture(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);
    impl std::io::Write for Capture {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0
                .lock()
                .map_err(|_| std::io::Error::other("capture poisoned"))?
                .extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Capture {
        type Writer = Self;
        fn make_writer(&'a self) -> Self {
            self.clone()
        }
    }
    #[test]
    fn specific_runtime_directives_cannot_enable_sdk_payload_logs() {
        let capture = Capture(Default::default());
        let subscriber =
            logging_subscriber(EnvFilter::new("trace,rmcp::service=trace"), capture.clone());
        tracing::subscriber::with_default(subscriber, || {
            tracing::warn!(target:"rmcp::service","synthetic private input");
            tracing::info!(target:"openlegal_server","listener ready");
        });
        let output = String::from_utf8(capture.0.lock().unwrap().clone()).unwrap();
        assert!(!output.contains("synthetic private input"));
        assert!(output.contains("listener ready"));
    }
}
