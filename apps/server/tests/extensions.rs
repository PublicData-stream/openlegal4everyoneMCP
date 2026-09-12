use futures::FutureExt;
use openlegal_server::{
    ServerBuilder, ServerError,
    config::{AccessPolicy, Limits},
    endpoint::{Binding, BoundEndpoint, Endpoint, EndpointContext, Network},
    registry::{ToolError, ToolRegistry},
};
use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio_util::sync::CancellationToken;

struct ProbeEndpoint {
    id: &'static str,
    address: SocketAddr,
    fail_bind: bool,
    fail_run: bool,
    fail_stop: bool,
    drain_completed: Arc<AtomicBool>,
    dropped: Arc<AtomicBool>,
}
struct OwnedListener {
    _listener: tokio::net::TcpListener,
    dropped: Arc<AtomicBool>,
}
impl Drop for OwnedListener {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::SeqCst);
    }
}
impl Endpoint for ProbeEndpoint {
    fn id(&self) -> &str {
        self.id
    }
    fn bindings(&self) -> Vec<Binding> {
        vec![Binding {
            network: Network::Tcp,
            address: self.address,
        }]
    }
    async fn bind(self, context: EndpointContext) -> Result<BoundEndpoint, ServerError> {
        if self.fail_bind {
            return Err("synthetic bind failure".into());
        }
        let listener = tokio::net::TcpListener::bind(self.address).await?;
        let address = listener.local_addr()?;
        let guard = OwnedListener {
            _listener: listener,
            dropped: self.dropped,
        };
        Ok(BoundEndpoint {
            id: self.id.into(),
            addresses: vec![address],
            run: async move {
                let _guard = guard;
                if self.fail_run {
                    return Err("synthetic endpoint failure".into());
                }
                context.shutdown.cancelled().await;
                if self.fail_stop {
                    return Err("synthetic shutdown failure".into());
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                self.drain_completed.store(true, Ordering::SeqCst);
                Ok(())
            }
            .boxed(),
        })
    }
}
fn endpoint(id: &'static str) -> ProbeEndpoint {
    ProbeEndpoint {
        id,
        address: "127.0.0.1:0".parse().unwrap(),
        fail_bind: false,
        fail_run: false,
        fail_stop: false,
        drain_completed: Arc::new(AtomicBool::new(false)),
        dropped: Arc::new(AtomicBool::new(false)),
    }
}

#[tokio::test]
async fn shutdown_error_does_not_skip_joining_other_endpoints() {
    let first = endpoint("first");
    let completed = first.drain_completed.clone();
    let mut second = endpoint("second");
    second.fail_stop = true;
    let mut builder = ServerBuilder::new(
        ToolRegistry::new(),
        Limits::default(),
        openlegal_server::config::SourceOffer::new("https://source.test/running").unwrap(),
    );
    builder.register_endpoint(first).unwrap();
    builder.register_endpoint(second).unwrap();
    let server = builder.bind().await.unwrap();
    let shutdown = CancellationToken::new();
    shutdown.cancel();
    assert!(server.run(shutdown).await.is_err());
    assert!(completed.load(Ordering::SeqCst));
}

#[tokio::test]
async fn failed_startup_releases_previously_bound_listener() {
    let first = endpoint("first");
    let dropped = first.dropped.clone();
    let mut second = endpoint("second");
    second.fail_bind = true;
    let mut builder = ServerBuilder::new(
        ToolRegistry::new(),
        Limits::default(),
        openlegal_server::config::SourceOffer::new("https://source.test/running").unwrap(),
    );
    builder.register_endpoint(first).unwrap();
    builder.register_endpoint(second).unwrap();
    assert!(builder.bind().await.is_err());
    assert!(dropped.load(Ordering::SeqCst));
}

#[tokio::test]
async fn failure_of_required_endpoint_stops_and_joins_others() {
    let first = endpoint("first");
    let dropped = first.dropped.clone();
    let mut second = endpoint("second");
    second.fail_run = true;
    let mut builder = ServerBuilder::new(
        ToolRegistry::new(),
        Limits::default(),
        openlegal_server::config::SourceOffer::new("https://source.test/running").unwrap(),
    );
    builder.register_endpoint(first).unwrap();
    builder.register_endpoint(second).unwrap();
    let running = builder.bind().await.unwrap();
    assert!(running.run(CancellationToken::new()).await.is_err());
    assert!(dropped.load(Ordering::SeqCst));
}

#[test]
fn duplicate_ids_and_wildcard_bindings_are_rejected() {
    let mut builder = ServerBuilder::new(
        ToolRegistry::new(),
        Limits::default(),
        openlegal_server::config::SourceOffer::new("https://source.test/running").unwrap(),
    );
    let mut first = endpoint("first");
    first.address = "0.0.0.0:8080".parse().unwrap();
    builder.register_endpoint(first).unwrap();
    assert!(builder.register_endpoint(endpoint("first")).is_err());
    let mut second = endpoint("second");
    second.address = "127.0.0.1:8080".parse().unwrap();
    assert!(builder.register_endpoint(second).is_err());
}

#[derive(serde::Deserialize, schemars::JsonSchema)]
struct Input {}
#[test]
fn registry_rejects_invalid_names_duplicates_and_nonobject_inputs() {
    let mut registry = ToolRegistry::new();
    registry
        .register::<Input, _, _>("valid", "description", |_, _| async {
            Ok(serde_json::json!({}))
        })
        .unwrap();
    assert!(
        registry
            .register::<Input, _, _>("valid", "duplicate", |_, _| async {
                Err(ToolError::Internal)
            })
            .is_err()
    );
    assert!(
        registry
            .register::<Input, _, _>("bad name", "description", |_, _| async {
                Err(ToolError::Internal)
            })
            .is_err()
    );
    assert!(
        registry
            .register::<String, _, _>("string_input", "description", |_, _| async {
                Err(ToolError::Internal)
            })
            .is_err()
    );
}

#[test]
fn empty_origin_policy_rejects_present_origins_and_limits_validate() {
    let policy = AccessPolicy {
        allowed_hosts: vec!["backend:8080".into()],
        allowed_origins: vec![],
    };
    policy.validate().unwrap();
    assert!(policy.permits("backend:8080", None));
    assert!(!policy.permits("backend:8080", Some("https://caller.test")));
    assert!(!policy.permits("backend:8080", Some("null")));
    assert!(
        Limits {
            max_message_bytes: usize::MAX,
            ..Limits::default()
        }
        .validate()
        .is_err()
    );
}

#[tokio::test]
async fn worker_failure_stops_endpoints_and_joins_other_workers() {
    let first = endpoint("http");
    let dropped = first.dropped.clone();
    let completed = Arc::new(AtomicBool::new(false));
    let worker_completed = completed.clone();
    let mut builder = ServerBuilder::new(
        ToolRegistry::new(),
        Limits::default(),
        openlegal_server::config::SourceOffer::new("https://source.test/running").unwrap(),
    );
    builder.register_endpoint(first).unwrap();
    builder
        .register_worker("owned", move |shutdown| async move {
            shutdown.cancelled().await;
            tokio::task::yield_now().await;
            worker_completed.store(true, Ordering::SeqCst);
            Ok(())
        })
        .unwrap();
    builder
        .register_worker("failing", |_| async {
            Err("synthetic worker failure".into())
        })
        .unwrap();
    assert!(
        builder
            .bind()
            .await
            .unwrap()
            .run(CancellationToken::new())
            .await
            .is_err()
    );
    assert!(dropped.load(Ordering::SeqCst));
    assert!(completed.load(Ordering::SeqCst));
}

#[tokio::test]
async fn failed_binding_never_starts_application_worker() {
    let mut failed = endpoint("failed");
    failed.fail_bind = true;
    let started = Arc::new(AtomicBool::new(false));
    let worker_started = started.clone();
    let mut builder = ServerBuilder::new(
        ToolRegistry::new(),
        Limits::default(),
        openlegal_server::config::SourceOffer::new("https://source.test/running").unwrap(),
    );
    builder.register_endpoint(failed).unwrap();
    builder
        .register_worker("worker", move |_| {
            worker_started.store(true, Ordering::SeqCst);
            async { Ok(()) }
        })
        .unwrap();
    assert!(builder.bind().await.is_err());
    assert!(!started.load(Ordering::SeqCst));
}

#[test]
fn static_resources_reject_duplicates_mismatch_and_unknown_tool_references() {
    use openlegal_server::{
        handler::McpHandler,
        registry::{ToolOptions, ToolOutput},
        resources::ResourceRegistry,
    };
    use rmcp::model::{MetaObject, Resource, ResourceContents};
    let descriptor = Resource::new("ui://demo/widget.html", "widget").with_mime_type("text/html");
    let content =
        ResourceContents::text("<p>Demo</p>", "ui://demo/widget.html").with_mime_type("text/html");
    let mut resources = ResourceRegistry::new();
    resources
        .register(descriptor.clone(), content.clone())
        .unwrap();
    assert!(resources.register(descriptor.clone(), content).is_err());
    assert!(
        ResourceRegistry::new()
            .register(
                descriptor,
                ResourceContents::text("wrong", "ui://other/widget.html")
            )
            .is_err()
    );
    #[derive(serde::Serialize, schemars::JsonSchema)]
    struct Output {
        value: u32,
    }
    let mut registry = ToolRegistry::new();
    registry
        .register_typed::<Input, Output, _, _>(
            "widget",
            "Widget fixture",
            ToolOptions {
                meta: Some(MetaObject(
                    serde_json::json!({"ui":{"resourceUri":"ui://missing/widget.html"}})
                        .as_object()
                        .unwrap()
                        .clone(),
                )),
                ..Default::default()
            },
            |_, _| async { Ok(ToolOutput::new(Output { value: 1 })) },
        )
        .unwrap();
    assert!(
        McpHandler::with_resources(
            registry,
            resources,
            Arc::new(Limits::default()),
            openlegal_server::config::SourceOffer::new("https://source.test/running").unwrap()
        )
        .is_err()
    );
}

#[tokio::test(start_paused = true)]
async fn worker_shutdown_deadline_aborts_and_drops_owned_future() {
    struct Dropped(Arc<AtomicBool>);
    impl Drop for Dropped {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }
    let dropped = Arc::new(AtomicBool::new(false));
    let guard = Dropped(dropped.clone());
    let mut builder = ServerBuilder::new(
        ToolRegistry::new(),
        Limits {
            shutdown_timeout_secs: 1,
            ..Default::default()
        },
        openlegal_server::config::SourceOffer::new("https://source.test/running").unwrap(),
    );
    builder.register_endpoint(endpoint("http")).unwrap();
    builder
        .register_worker("stuck", |_| async move {
            let _guard = guard;
            std::future::pending::<()>().await;
            Ok(())
        })
        .unwrap();
    let shutdown = CancellationToken::new();
    shutdown.cancel();
    assert!(builder.bind().await.unwrap().run(shutdown).await.is_err());
    assert!(dropped.load(Ordering::SeqCst));
}

#[test]
fn typed_registration_rejects_nonobject_output_without_leaving_a_tool() {
    use openlegal_server::registry::{ToolOptions, ToolOutput};
    let mut registry = ToolRegistry::new();
    assert!(
        registry
            .register_typed::<Input, String, _, _>(
                "typed",
                "Invalid output",
                ToolOptions::default(),
                |_, _| async { Ok(ToolOutput::new("value".into())) }
            )
            .is_err()
    );
    registry
        .register::<Input, _, _>(
            "typed",
            "Available after failed registration",
            |_, _| async { Ok(serde_json::json!({})) },
        )
        .unwrap();
}

#[test]
fn static_resource_serialized_size_is_checked_against_server_limits() {
    use openlegal_server::{handler::McpHandler, resources::ResourceRegistry};
    use rmcp::model::{Resource, ResourceContents};
    let mut resources = ResourceRegistry::new();
    resources
        .register(
            Resource::new("ui://demo/large.html", "large").with_mime_type("text/html"),
            ResourceContents::text("x".repeat(5000), "ui://demo/large.html")
                .with_mime_type("text/html"),
        )
        .unwrap();
    let limits = Arc::new(Limits {
        max_message_bytes: 4096,
        ..Default::default()
    });
    assert!(
        McpHandler::with_resources(
            ToolRegistry::new(),
            resources,
            limits,
            openlegal_server::config::SourceOffer::new("https://source.test/running").unwrap()
        )
        .is_err()
    );
}
