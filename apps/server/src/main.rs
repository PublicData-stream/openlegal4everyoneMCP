use openlegal_server::{
    ServerBuilder, ServerError,
    config::{AccessPolicy, BlobConfig, CacheConfig, Config},
    http::{HealthEndpoint, HttpEndpoint},
    webtransport::WebTransportEndpoint,
};
use tokio_util::sync::CancellationToken;
use tracing_subscriber::{EnvFilter, prelude::*};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Command {
    Serve,
    Migrate,
    Maintain,
}

fn main() -> Result<(), ServerError> {
    let mut args = std::env::args_os().skip(1);
    let first = args
        .next()
        .ok_or("usage: openlegal-server [--migrate|--maintain] CONFIG.toml")?;
    if first == "--text-diff-worker" {
        if args.next().is_some() {
            return Err("text-diff worker takes no arguments".into());
        }
        openlegal_adapters::text_diff::run_worker()?;
        return Ok(());
    }
    let (command, path) = if first == "--migrate" || first == "--maintain" {
        let command = if first == "--migrate" {
            Command::Migrate
        } else {
            Command::Maintain
        };
        (
            command,
            args.next()
                .ok_or("storage administration requires CONFIG.toml")?,
        )
    } else {
        if first.to_string_lossy().starts_with("--") {
            return Err("unknown server command".into());
        }
        (Command::Serve, first)
    };
    if args.next().is_some() {
        return Err("usage: openlegal-server [--migrate|--maintain] CONFIG.toml".into());
    }
    run_server(path, command)
}

fn now() -> u64 {
    use openlegal_application::Clock;
    openlegal_application::SystemClock::default().now()
}

async fn open_storage(
    cache: &CacheConfig,
    mode: openlegal_adapters::postgres::StartupMode,
) -> Result<std::sync::Arc<openlegal_adapters::postgres::PostgresStore>, ServerError> {
    use openlegal_application::blob::BlobStore;
    let CacheConfig::Persistent { postgres, blob, .. } = cache else {
        return Err("storage administration requires cache mode persistent".into());
    };
    let options = postgres.options()?;
    let url = postgres.connection_url(false)?;
    let policy = cache.policy()?;
    let BlobConfig::Filesystem { path } = blob;
    let blobs = openlegal_adapters::blob::FsBlobStore::open(&std::path::absolute(path)?).await?;
    match openlegal_adapters::postgres::PostgresStore::open(
        &url,
        options,
        blobs.clone(),
        policy,
        now(),
        mode,
    )
    .await
    {
        Ok(store) => Ok(store),
        Err(error) => {
            let _ = blobs.close().await;
            Err(error.into())
        }
    }
}

#[tokio::main]
async fn run_server(path: std::ffi::OsString, command: Command) -> Result<(), ServerError> {
    // SDK/driver diagnostics can contain secrets or payloads; filtering below is mandatory.
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    logging_subscriber(filter, std::io::stdout).init();
    let config: Config = toml::from_str(&tokio::fs::read_to_string(path).await?)?;
    if command != Command::Serve {
        let cache = config
            .cache
            .as_ref()
            .ok_or("storage administration requires cache mode persistent")?;
        let CacheConfig::Persistent { postgres, .. } = cache else {
            return Err("storage administration requires cache mode persistent".into());
        };
        if command == Command::Migrate {
            // This branch never reads runtime credentials, opens blobs, or initializes serving.
            openlegal_adapters::postgres::PostgresStore::migrate(
                &postgres.connection_url(true)?,
                postgres.options()?,
            )
            .await?;
        } else {
            use openlegal_application::persistence::PersistentStore;
            let store =
                open_storage(cache, openlegal_adapters::postgres::StartupMode::Maintain).await?;
            let result = store.prune(now()).await;
            let closed = store.close().await;
            result?;
            closed?;
        }
        return Ok(());
    }
    config.limits.validate()?;
    config.validate_storage()?;
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
    let persistent = match &config.cache {
        Some(cache @ CacheConfig::Persistent { .. }) => {
            Some(open_storage(cache, openlegal_adapters::postgres::StartupMode::Serve).await?)
        }
        _ => None,
    };
    let mut corpus_runtime = None;
    let result: Result<(), ServerError> = async {
        if let Some(database) = &config.database {
            let runtime = openlegal_server::corpus_runtime::CorpusRuntime::open(
                database,
                persistent.as_ref().ok_or("database storage unavailable")?,
            )
            .await?;
            registry.register_module(openlegal_server::database::DatabaseTools {
                database: runtime.database.clone(),
                reader: runtime.reader.clone(),
                search: runtime.search.clone(),
                comparison: diff_service.clone().ok_or("database comparison unavailable")?,
            })?;
            corpus_runtime = Some(runtime);
        }
        let demo_service = config
            .demo
            .as_ref()
            .map(|demo| {
                if let Some(store) = &persistent {
                    openlegal_server::demo::service_with_store(&demo.upstream, store.clone())
                } else {
                    openlegal_server::demo::service(&demo.upstream)
                }
            })
            .transpose()?;
        if let Some(service) = &demo_service {
            registry.register_module(openlegal_server::demo::DemoTools {
                service: service.clone(),
                comparison: diff_service.clone(),
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
        if let Some(database) = &config.database {
            resources.extend(
                openlegal_server::database::load_widget(&database.widget_html, &config.source.url)
                    .await?,
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
        if let Some(runtime) = &corpus_runtime {
            let runtime = runtime.clone();
            builder.register_worker("legal_corpus", move |shutdown| async move {
                runtime.run(shutdown).await
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
        if demo_service.is_some() || diff_service.is_some() || corpus_runtime.is_some() {
            let readiness_service = demo_service.clone();
            let corpus_readiness = corpus_runtime.clone();
            builder.register_endpoint(
                health
                    .with_metrics(move || {
                        let mut metrics = String::new();
                        if let Some(service) = &demo_service {
                            metrics.push_str(&service.metrics_prometheus());
                        }
                        if let Some(service) = &diff_service {
                            metrics.push_str(&service.metrics_prometheus());
                        }
                        metrics
                    })
                    .with_readiness(move || {
                        readiness_service
                            .as_ref()
                            .is_none_or(|service| service.storage_ready())
                            && corpus_readiness
                                .as_ref()
                                .is_none_or(|runtime| runtime.store.healthy())
                    }),
            )?;
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
    .await;
    // Every startup and serving exit owns persistence cleanup, including bind/widget errors.
    if let Some(runtime) = corpus_runtime {
        let closed = runtime.close().await;
        if result.is_ok() {
            closed?;
        }
    }
    if let Some(store) = persistent {
        use openlegal_application::persistence::PersistentStore;
        let closed = store.close().await;
        if result.is_ok() {
            closed?;
        }
    }
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
                metadata.target() != "rmcp"
                    && !metadata.target().starts_with("rmcp::")
                    && metadata.target() != "sqlx"
                    && !metadata.target().starts_with("sqlx::")
                    && !metadata.target().starts_with("sqlx_")
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
        let subscriber = logging_subscriber(
            EnvFilter::new("trace,rmcp::service=trace,sqlx=trace"),
            capture.clone(),
        );
        tracing::subscriber::with_default(subscriber, || {
            tracing::warn!(target:"rmcp::service","synthetic private input");
            tracing::warn!(target:"sqlx_postgres::options::parse", "synthetic private input");
            tracing::warn!(target:"sqlx::query","synthetic private database input");
            tracing::info!(target:"openlegal_server","listener ready");
        });
        let output = String::from_utf8(capture.0.lock().unwrap().clone()).unwrap();
        assert!(!output.contains("synthetic private input"));
        assert!(!output.contains("synthetic private database input"));
        assert!(output.contains("listener ready"));
    }
}
