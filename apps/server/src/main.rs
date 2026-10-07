use openlegal_server::{
    ServerBuilder, ServerError,
    config::{AccessPolicy, BlobConfig, CacheConfig, Config},
    http::{HealthEndpoint, HttpEndpoint},
    webtransport::WebTransportEndpoint,
};
use tokio_util::sync::CancellationToken;
use tracing_subscriber::{EnvFilter, prelude::*};

mod collection_scheduler_retry;
mod provider_admin;
use collection_scheduler_retry::{create_and_mark_launch, dispatch_storage_result, retry_storage};

#[derive(Clone, Debug, PartialEq, Eq)]
enum Command {
    Serve,
    Migrate,
    Maintain,
    RebuildCorpusIndex,
    CollectionScheduler,
    CollectionJob(String, Option<u64>),
}

fn main() -> Result<(), ServerError> {
    let mut args = std::env::args_os().skip(1);
    let first = args.next().ok_or(
        "usage: openlegal-server [--migrate|--maintain|--rebuild-corpus-index|--collection-scheduler|--collection-job ID] CONFIG.toml",
    )?;
    if first == "--provider-admin" {
        return provider_admin::run(args.collect());
    }
    if first == "--text-diff-worker" {
        if args.next().is_some() {
            return Err("text-diff worker takes no arguments".into());
        }
        openlegal_adapters::text_diff::run_worker()?;
        return Ok(());
    }
    let (command, path) = if first == "--collection-job" {
        let id = args.next().ok_or("collection job requires request id")?;
        let identity = id
            .into_string()
            .map_err(|_| "collection request id must be UTF-8")?;
        let (id, epoch) = parse_collection_identity(&identity)?;
        (
            Command::CollectionJob(id, epoch),
            args.next().ok_or("collection job requires CONFIG.toml")?,
        )
    } else if first == "--migrate"
        || first == "--maintain"
        || first == "--rebuild-corpus-index"
        || first == "--collection-scheduler"
    {
        let command = if first == "--migrate" {
            Command::Migrate
        } else if first == "--maintain" {
            Command::Maintain
        } else if first == "--collection-scheduler" {
            Command::CollectionScheduler
        } else {
            Command::RebuildCorpusIndex
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
        return Err(
            "usage: openlegal-server [--migrate|--maintain|--rebuild-corpus-index|--collection-scheduler|--collection-job ID] CONFIG.toml"
                .into(),
        );
    }
    run_server(path, command)
}

fn now() -> u64 {
    use openlegal_application::Clock;
    openlegal_application::SystemClock::default().now()
}

fn parse_collection_identity(identity: &str) -> Result<(String, Option<u64>), ServerError> {
    let (id, epoch) = match identity.split_once('@') {
        Some((id, epoch)) => (id, Some(epoch.parse::<u64>()?)),
        None => (identity, None),
    };
    if id.len() != 36
        || !id.bytes().enumerate().all(|(index, byte)| {
            if [8, 13, 18, 23].contains(&index) {
                byte == b'-'
            } else {
                byte.is_ascii_hexdigit()
            }
        })
    {
        return Err("invalid collection request identifier".into());
    }
    Ok((id.to_owned(), epoch))
}

fn collection_job_name(id: &str, epoch: u64) -> Result<String, ServerError> {
    parse_collection_identity(id)?;
    let name = format!(
        "openlegal-request-{}-{epoch}",
        id.replace('-', "").to_ascii_lowercase()
    );
    if name.len() > 63 {
        return Err("collection launch name exceeds Kubernetes limit".into());
    }
    Ok(name)
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

/// Collection Pods may overlap briefly with other storage users during startup.
/// Retry only admission contention, before a provider client can make a request.
async fn open_collection_storage(
    cache: &CacheConfig,
) -> Result<std::sync::Arc<openlegal_adapters::postgres::PostgresStore>, ServerError> {
    use openlegal_adapters::postgres::{StartupError, StartupMode};
    use openlegal_domain::RetrievalError;

    for attempt in 0..5 {
        match open_storage(cache, StartupMode::Serve).await {
            Ok(store) => return Ok(store),
            Err(error) => {
                let busy = matches!(
                    error.downcast_ref::<StartupError>(),
                    Some(StartupError::Storage(RetrievalError::Busy))
                ) || matches!(
                    error.downcast_ref::<RetrievalError>(),
                    Some(RetrievalError::Busy)
                );
                if !busy {
                    return Err(error);
                }
                tokio::time::sleep(std::time::Duration::from_millis(200 << attempt)).await;
            }
        }
    }
    open_storage(cache, StartupMode::Serve).await
}

#[tokio::main]
async fn run_server(path: std::ffi::OsString, command: Command) -> Result<(), ServerError> {
    // SDK/driver diagnostics can contain secrets or payloads; filtering below is mandatory.
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    logging_subscriber(filter, std::io::stdout).init();
    let config: Config = toml::from_str(&tokio::fs::read_to_string(path).await?)?;
    if matches!(
        command,
        Command::CollectionScheduler | Command::CollectionJob(_, _)
    ) {
        config.validate_storage()?;
        let database = config
            .database
            .as_ref()
            .ok_or("collection requires [database]")?;
        let ingestion = database
            .ingestion
            .as_ref()
            .filter(|ingestion| ingestion.enabled)
            .ok_or("collection requires enabled [database.ingestion]")?;
        let cache = config
            .cache
            .as_ref()
            .ok_or("collection requires persistent storage")?;
        let persistent = open_collection_storage(cache).await?;
        let runtime =
            openlegal_server::corpus_runtime::CorpusRuntime::open_background(database, &persistent)
                .await?;
        let cancel = CancellationToken::new();
        let signal_cancel = cancel.clone();
        let signal = tokio::spawn(async move {
            let _ = tokio::signal::ctrl_c().await;
            signal_cancel.cancel();
        });
        let result = match &command {
            Command::CollectionScheduler => {
                let background = runtime.clone();
                let collector = cancel.child_token();
                tokio::select! {
                    result = run_collection_scheduler(runtime.store.clone(), ingestion, collector.clone()) => {
                        collector.cancel();
                        result
                    },
                    result = background.run(collector.clone()) => {
                        collector.cancel();
                        result
                    },
                }
            }
            Command::CollectionJob(id, epoch) => {
                let id = id.clone();
                let launch = if let Some(epoch) = epoch {
                    runtime
                        .store
                        .load_collection_launch_for_epoch(&id, *epoch)
                        .await?
                } else {
                    runtime.store.load_collection_launch(&id).await?
                };
                match tokio::time::timeout(
                    std::time::Duration::from_secs(launch.remaining_secs(launch.observed_at)),
                    runtime.execute_collection_request(&launch, cancel.clone()),
                )
                .await
                {
                    Ok(result) => result.map_err(ServerError::from),
                    Err(_) => {
                        cancel.cancel();
                        runtime
                            .store
                            .settle_collection_launch(
                                &launch,
                                "failed",
                                Some("worker_failed"),
                                None,
                            )
                            .await?;
                        Err("collection request exceeded its operation deadline".into())
                    }
                }
            }
            _ => unreachable!(),
        };
        signal.abort();
        let closed = runtime.close().await;
        let storage_closed = {
            use openlegal_application::persistence::PersistentStore;
            persistent.close().await
        };
        result?;
        closed?;
        storage_closed?;
        return Ok(());
    }
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
        } else if command == Command::RebuildCorpusIndex {
            use openlegal_application::persistence::PersistentStore;
            config.validate_storage()?;
            let database = config
                .database
                .as_ref()
                .ok_or("index rebuild requires [database]")?;
            let store =
                open_storage(cache, openlegal_adapters::postgres::StartupMode::Maintain).await?;
            let cancel = CancellationToken::new();
            let signal_cancel = cancel.clone();
            let signal = tokio::spawn(async move {
                let _ = tokio::signal::ctrl_c().await;
                signal_cancel.cancel();
            });
            let result =
                openlegal_server::corpus_runtime::rebuild_corpus_index(database, &store, cancel)
                    .await;
            signal.abort();
            let closed = store.close().await;
            let generation = result?;
            closed?;
            tracing::info!(generation, "corpus index rebuild complete");
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
    config.validate_transport_security()?;
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
    let mut citations = None;
    let result: Result<(), ServerError> = async {
        if let Some(database) = &config.database {
            let runtime = openlegal_server::corpus_runtime::CorpusRuntime::open(
                database,
                persistent.as_ref().ok_or("database storage unavailable")?,
            )
            .await?;
            let demand = std::sync::Arc::new(openlegal_application::demand_collection::DemandCollectionCoordinator::new(runtime.store.clone(), database.auto_collection));
            registry.register_module(openlegal_server::database::DatabaseTools {
                demand: Some(demand.clone()),
                database: runtime.database.clone(),
                reader: runtime.reader.clone(),
                search: runtime.search.clone(),
                comparison: diff_service.clone().ok_or("database comparison unavailable")?,
                store: runtime.store.clone(),
            })?;
            let lookup = std::sync::Arc::new(
                openlegal_application::legal_reference::ReferenceLookup::new(
                    runtime.database.clone(),
                    runtime.search.clone(),
                ),
            );
            registry.register_module(openlegal_server::legal_reference::LegalReferenceTools {
                demand: Some(demand.clone()),
                lookup: lookup.clone(),
            })?;
            registry.register_module(openlegal_server::legal_analysis::LegalAnalysisTools { lookup })?;
            if let Some(options) = &config.citations {
                let service = std::sync::Arc::new(
                    openlegal_application::citation::CitationService::new(
                        runtime.database.clone(),
                        runtime.search.clone(),
                        runtime.store.clone(),
                        std::sync::Arc::new(openlegal_application::SystemClock::default()),
                        options.base_url.clone(),
                    )?,
                );
                registry.register_module(openlegal_server::citation::CitationTools {
                    admission_store: Some(runtime.store.clone()),
                    demand: Some(demand.clone()),
                    service: service.clone(),
                })?;
                citations = Some(service);
            }
            corpus_runtime = Some(runtime);
        }
        let demo_options = config
            .demo
            .as_ref()
            .map(|demo| -> Result<_, openlegal_server::ServerError> {
                let proxy = demo.proxy.as_ref().map(|proxy| proxy.load()).transpose()?;
                let policy = demo.provider_requests.policy()?;
                Ok((proxy, policy))
            })
            .transpose()?;
        let demo_service = if let Some((proxy, policy)) = demo_options {
            let demo = config.demo.as_ref().ok_or("missing demo configuration")?;
            Some(if let Some(store) = &persistent {
                openlegal_server::demo::service_with_store_proxy_and_policy(&demo.upstream, store.clone(), proxy, policy, store.clone()).await?
            } else {
                openlegal_server::demo::service_with_proxy_and_policy(&demo.upstream, proxy, policy)?
            })
        } else { None };
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
        if let Some(service) = citations {
            builder = builder.with_citations(service);
        }
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
            tls: config.http.tls,
            edge_mtls: config.edge_mtls.clone(),
            access: AccessPolicy {
                allowed_hosts: config.http.allowed_hosts,
                allowed_origins: config.http.allowed_origins,
            },
        })?;
        builder.register_endpoint(WebTransportEndpoint {
            bind: config.webtransport.bind,
            edge_mtls: config.edge_mtls,
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

async fn run_collection_scheduler(
    store: std::sync::Arc<openlegal_adapters::corpus::PgCorpusStore>,
    ingestion: &openlegal_server::config::IngestionConfig,
    cancel: CancellationToken,
) -> Result<(), ServerError> {
    use tokio::io::AsyncWriteExt;
    let template = tokio::fs::read(&ingestion.collection_job_template_path).await?;
    if template.len() > 64 * 1024 {
        return Err("collection Job template exceeds 64 KiB".into());
    }
    let template: serde_json::Value = serde_json::from_slice(&template)?;
    if template
        .pointer("/spec/activeDeadlineSeconds")
        .and_then(serde_json::Value::as_u64)
        != Some(7500)
        || template
            .pointer("/spec/template/spec/containers")
            .and_then(serde_json::Value::as_array)
            .is_none_or(|items| items.len() != 1)
    {
        return Err("invalid collection Job template deadline or containers".into());
    }
    let heartbeat_store = store.clone();
    let heartbeat = async {
        loop {
            if cancel.is_cancelled() {
                return Ok(());
            }
            retry_storage(&cancel, || heartbeat_store.heartbeat_collection_scheduler()).await?;
            tokio::select! {
                _ = cancel.cancelled() => return Ok(()),
                _ = tokio::time::sleep(std::time::Duration::from_secs(5)) => {}
            }
        }
    };
    let dispatch = async {
        // LISTEN before the first queue scan; notifications only prompt a recheck.
        let mut events = store.collection_events().await?;
        loop {
            if cancel.is_cancelled() {
                return Ok(());
            }
            if dispatch_storage_result(
                &cancel,
                "request_reap",
                retry_storage(&cancel, || store.reap_stale_collection_requests())
                    .await
                    .map_err(ServerError::from),
            )
            .await?
            .is_none()
            {
                continue;
            }
            if dispatch_storage_result(
                &cancel,
                "request_reconcile",
                reconcile_failed_collection_jobs(&store, ingestion, &cancel).await,
            )
            .await?
            .is_none()
            {
                continue;
            }
            let policy = ingestion.provider_requests.limits()?;
            let claim = dispatch_storage_result(
                &cancel,
                "request_claim",
                retry_storage(&cancel, || {
                    store.claim_collection_request_with_policy(
                        policy.on_demand_timeout_secs,
                        policy.on_demand_attempt_limit,
                    )
                })
                .await
                .map_err(ServerError::from),
            )
            .await?;
            let Some(claim) = claim else {
                continue;
            };
            let Some(launch) = claim else {
                match events
                    .wait(&cancel, std::time::Duration::from_secs(5))
                    .await
                {
                    Ok(()) => {}
                    Err(_) if cancel.is_cancelled() => return Ok(()),
                    Err(error) => return Err(error.into()),
                }
                continue;
            };
            let id = launch.id.clone();
            let name = collection_job_name(&id, launch.launched_at)?;
            let job = render_collection_job(&template, &launch, &ingestion.collection_namespace)?;
            let bytes = serde_json::to_vec(&job)?;
            create_and_mark_launch(
                &cancel,
                async {
                    let mut child = tokio::process::Command::new(&ingestion.kubectl)
                        .args([
                            "--kubeconfig",
                            ingestion
                                .kubeconfig
                                .to_str()
                                .ok_or("invalid kubeconfig path")?,
                            "--context",
                            &ingestion.context,
                            "-n",
                            &ingestion.collection_namespace,
                            "create",
                            "-f",
                            "-",
                        ])
                        .env_clear()
                        .env("PATH", "/usr/local/bin:/usr/bin:/bin")
                        .env("HOME", "/tmp")
                        .env("TMPDIR", "/tmp")
                        .stdin(std::process::Stdio::piped())
                        .stdout(std::process::Stdio::null())
                        .stderr(std::process::Stdio::null())
                        .kill_on_drop(true)
                        .spawn()?;
                    child
                        .stdin
                        .take()
                        .ok_or("kubectl stdin unavailable")?
                        .write_all(&bytes)
                        .await?;
                    let outcome =
                        tokio::time::timeout(std::time::Duration::from_secs(30), child.wait())
                            .await;
                    match outcome {
                        Ok(Ok(status)) if status.success() => Ok(()),
                        _ => {
                            // Creation may have reached the API server. Keep the claim
                            // fenced until its Job deadline and operator reconciliation.
                            Err("collection Job creation outcome uncertain".into())
                        }
                    }
                },
                || store.mark_collection_launch_running(&launch, &name),
            )
            .await?;
        }
    };
    let result = tokio::select! { result = heartbeat => result, result = dispatch => result };
    if cancel.is_cancelled() {
        Ok(())
    } else {
        result
    }
}

fn render_collection_job(
    template: &serde_json::Value,
    launch: &openlegal_adapters::corpus::CollectionLaunch,
    namespace: &str,
) -> Result<serde_json::Value, ServerError> {
    if template
        .pointer("/spec/activeDeadlineSeconds")
        .and_then(serde_json::Value::as_u64)
        != Some(7500)
        || template
            .pointer("/spec/template/spec/containers")
            .and_then(serde_json::Value::as_array)
            .is_none_or(|items| items.len() != 1)
        || !(60..=86400).contains(&launch.timeout_secs)
    {
        return Err("invalid collection Job template or operation deadline".into());
    }
    if launch.id.len() != 36
        || !launch.id.bytes().enumerate().all(|(index, byte)| {
            if [8, 13, 18, 23].contains(&index) {
                byte == b'-'
            } else {
                byte.is_ascii_hexdigit()
            }
        })
    {
        return Err("invalid collection request identifier".into());
    }
    let mut job = template.clone();
    job["spec"]["activeDeadlineSeconds"] = launch.job_deadline_secs().into();
    job["metadata"]["name"] = collection_job_name(&launch.id, launch.launched_at)?.into();
    job["metadata"]["namespace"] = namespace.into();
    job["spec"]["template"]["spec"]["containers"][0]["args"] = serde_json::json!([
        "--collection-job",
        format!("{}@{}", launch.id, launch.launched_at),
        "/etc/openlegal/server.toml"
    ]);
    Ok(job)
}

fn collection_job_failed(job: &serde_json::Value, name: &str) -> bool {
    job.pointer("/metadata/name")
        .and_then(serde_json::Value::as_str)
        == Some(name)
        && job
            .pointer("/status/conditions")
            .and_then(serde_json::Value::as_array)
            .is_some_and(|conditions| {
                conditions.iter().any(|condition| {
                    condition.get("type").and_then(serde_json::Value::as_str) == Some("Failed")
                        && condition.get("status").and_then(serde_json::Value::as_str)
                            == Some("True")
                })
            })
}

async fn reconcile_failed_collection_jobs(
    store: &openlegal_adapters::corpus::PgCorpusStore,
    ingestion: &openlegal_server::config::IngestionConfig,
    cancel: &CancellationToken,
) -> Result<(), ServerError> {
    for (id, stored_name, epoch) in
        retry_storage(cancel, || store.unsettled_collection_launches()).await?
    {
        if cancel.is_cancelled() {
            return Ok(());
        }
        let expected = collection_job_name(&id, epoch)?;
        // Existing launched Jobs retain their stored name during upgrade.
        let legacy = format!("openlegal-request-{}", id.replace('-', ""));
        if stored_name
            .as_deref()
            .is_some_and(|stored| stored != expected && stored != legacy)
        {
            return Err("collection request Job name is inconsistent".into());
        }
        let name = stored_name.unwrap_or(expected);
        let output = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            tokio::process::Command::new(&ingestion.kubectl)
                .args([
                    "--kubeconfig",
                    ingestion
                        .kubeconfig
                        .to_str()
                        .ok_or("invalid kubeconfig path")?,
                    "--context",
                    &ingestion.context,
                    "-n",
                    &ingestion.collection_namespace,
                    "get",
                    "job",
                    &name,
                    "-o=json",
                ])
                .env_clear()
                .env("PATH", "/usr/local/bin:/usr/bin:/bin")
                .env("HOME", "/tmp")
                .env("TMPDIR", "/tmp")
                .kill_on_drop(true)
                .output(),
        )
        .await;
        let Ok(Ok(output)) = output else {
            // API timeouts are not proof that a Job failed. The DB lease remains
            // fenced until a terminal condition can be observed.
            continue;
        };
        if !output.status.success() || output.stdout.len() > 128 * 1024 {
            continue;
        }
        let Ok(job) = serde_json::from_slice::<serde_json::Value>(&output.stdout) else {
            continue;
        };
        if collection_job_failed(&job, &name) {
            retry_storage(cancel, || {
                store.fail_finished_collection_launch(&id, &name, epoch)
            })
            .await?;
        }
    }
    Ok(())
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
    #[test]
    fn only_a_matching_terminal_job_failure_can_settle_a_request() {
        let job = serde_json::json!({"metadata":{"name":"openlegal-request-test"},"status":{"conditions":[{"type":"Failed","status":"True"}]}});
        assert!(collection_job_failed(&job, "openlegal-request-test"));
        assert!(!collection_job_failed(&job, "openlegal-request-other"));
        let mut pending = job.clone();
        pending["status"]["conditions"][0]["status"] = "False".into();
        assert!(!collection_job_failed(&pending, "openlegal-request-test"));
        pending["status"]["conditions"][0]["type"] = "Complete".into();
        pending["status"]["conditions"][0]["status"] = "True".into();
        assert!(!collection_job_failed(&pending, "openlegal-request-test"));
    }
    #[test]
    fn collection_job_rendering_uses_captured_operation_deadline_with_default_template() {
        use openlegal_domain::{
            collection::{CollectionRequest, CollectionTarget},
            legal::{Dataset, ObjectId},
        };
        let template: serde_json::Value = serde_json::from_str(include_str!(
            "../../../deploy/kubernetes/ingestion/collection-job.json"
        ))
        .unwrap();
        let mut launch = openlegal_adapters::corpus::CollectionLaunch {
            id: "00000000-0000-4000-8000-000000000001".into(),
            request: CollectionRequest {
                target: CollectionTarget::Object {
                    object: ObjectId {
                        jurisdiction: "kr".into(),
                        provider: "law_go_kr".into(),
                        dataset: Dataset::NationalStatute,
                        id: "001".into(),
                    },
                },
            },
            timeout_secs: 7200,
            attempt_limit: openlegal_application::upstream_policy::RequestLimit::Limited(32),
            launched_at: 100,
            observed_at: 100,
        };
        for (seconds, deadline) in [(60, 360), (7200, 7500), (86400, 86700)] {
            launch.timeout_secs = seconds;
            let rendered = render_collection_job(&template, &launch, "openlegal-serving").unwrap();
            assert_eq!(rendered["spec"]["activeDeadlineSeconds"], deadline);
            assert_eq!(
                rendered["metadata"]["name"],
                "openlegal-request-00000000000040008000000000000001-100"
            );
            assert_eq!(
                rendered["spec"]["template"]["spec"]["securityContext"],
                template["spec"]["template"]["spec"]["securityContext"]
            );
            assert_eq!(
                rendered["spec"]["template"]["spec"]["volumes"],
                template["spec"]["template"]["spec"]["volumes"]
            );
        }
        assert_eq!(template["spec"]["activeDeadlineSeconds"], 7500);
        let first = render_collection_job(&template, &launch, "openlegal-serving").unwrap();
        launch.launched_at += 5;
        let retry = render_collection_job(&template, &launch, "openlegal-serving").unwrap();
        assert_ne!(first["metadata"]["name"], retry["metadata"]["name"]);
        assert_ne!(
            first["spec"]["template"]["spec"]["containers"][0]["args"],
            retry["spec"]["template"]["spec"]["containers"][0]["args"]
        );
        assert_eq!(
            parse_collection_identity(&format!("{}@105", launch.id)).unwrap(),
            (launch.id.clone(), Some(105))
        );
        assert!(parse_collection_identity(&format!("{}@bad", launch.id)).is_err());
        assert!(parse_collection_identity("bad@105").is_err());
        launch.timeout_secs = 86401;
        assert!(render_collection_job(&template, &launch, "openlegal-serving").is_err());
        launch.timeout_secs = 60;
        let mut unsafe_template = template.clone();
        unsafe_template["spec"]["activeDeadlineSeconds"] = 0.into();
        assert!(render_collection_job(&unsafe_template, &launch, "openlegal-serving").is_err());
    }
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
