//! Streamable HTTP with bounded request/response bodies and explicit origin validation.

use crate::{
    ServerError,
    config::AccessPolicy,
    endpoint::{Binding, BoundEndpoint, Endpoint, EndpointContext, Network},
};
use axum::{
    Router,
    body::{Body, to_bytes},
    extract::{Request, State},
    http::{StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::get,
};
use futures::{FutureExt, StreamExt};
use rmcp::transport::streamable_http_server::{
    StreamableHttpServerConfig, StreamableHttpService, session::local::LocalSessionManager,
};
use std::{
    net::SocketAddr,
    sync::{Arc, atomic::Ordering},
    time::Duration,
};

async fn serve_bounded(
    listener: tokio::net::TcpListener,
    app: Router,
    context: EndpointContext,
) -> Result<(), ServerError> {
    use hyper_util::{
        rt::{TokioExecutor, TokioIo, TokioTimer},
        server::conn::auto::Builder,
        service::TowerToHyperService,
    };
    let mut tasks = tokio::task::JoinSet::new();
    loop {
        tokio::select! {
            biased;
            _ = context.shutdown.cancelled() => break,
            Some(result) = tasks.join_next(), if !tasks.is_empty() => {
                if result.is_err() { tracing::warn!("HTTP connection task failed"); }
            }
            accepted = listener.accept() => {
                let (stream, _) = accepted?;
                let Ok(permit) = context.connections.clone().try_acquire_owned() else { continue; };
                let app = app.clone();
                let context = context.clone();
                tasks.spawn(async move {
                    let _permit = permit;
                    let state = crate::timed_io::ConnectionState::new();
                    let app = app.layer(axum::Extension(state.clone()));
                    let mut builder = Builder::new(TokioExecutor::new());
                    builder.http1().timer(TokioTimer::new())
                        .header_read_timeout(Duration::from_secs(context.limits.io_timeout_secs))
                        .max_headers(64).max_buf_size(32 * 1024);
                    builder.http2().timer(TokioTimer::new())
                        .max_concurrent_streams(context.limits.max_calls_per_connection as u32)
                        .max_header_list_size(32 * 1024)
                        .keep_alive_interval(Duration::from_secs(context.limits.idle_timeout_secs))
                        .keep_alive_timeout(Duration::from_secs(context.limits.io_timeout_secs));
                    let io = crate::timed_io::TimedIo::new(stream, Duration::from_secs(context.limits.io_timeout_secs));
                    let connection = builder.serve_connection(TokioIo::new(io), TowerToHyperService::new(app));
                    tokio::pin!(connection);
                    tokio::select! {
                        _ = state.cancel.cancelled() => {},
                        _ = state.idle(Duration::from_secs(context.limits.idle_timeout_secs)) => {},
                        _ = context.shutdown.cancelled() => {
                            connection.as_mut().graceful_shutdown();
                            let _ = tokio::time::timeout(Duration::from_secs(context.limits.shutdown_timeout_secs), connection).await;
                        }
                        _ = &mut connection => {}
                    }
                });
            }
        }
    }
    while tasks.join_next().await.is_some() {}
    Ok(())
}

pub struct HttpEndpoint {
    pub bind: SocketAddr,
    pub access: AccessPolicy,
}

impl Endpoint for HttpEndpoint {
    fn id(&self) -> &str {
        "http"
    }
    fn bindings(&self) -> Vec<Binding> {
        vec![Binding {
            network: Network::Tcp,
            address: self.bind,
        }]
    }
    async fn bind(self, context: EndpointContext) -> Result<BoundEndpoint, ServerError> {
        self.access.validate()?;
        let listener = tokio::net::TcpListener::bind(self.bind).await?;
        let address = listener.local_addr()?;
        let handler = context.handler.clone();
        // Legacy initialize remains supported; the read-only registry needs no persistent sessions.
        let config = StreamableHttpServerConfig::default()
            .with_legacy_session_mode(false)
            .with_json_response(false)
            .with_cancellation_token(context.shutdown.clone())
            .with_max_request_body_bytes(context.limits.max_message_bytes)
            .with_allowed_hosts(self.access.allowed_hosts.clone())
            .with_allowed_origins(self.access.allowed_origins.clone());
        let service = StreamableHttpService::new(
            move || Ok(handler.clone()),
            Arc::new(LocalSessionManager::default()),
            config,
        );
        let app =
            Router::new()
                .nest_service("/mcp", service)
                .layer(middleware::from_fn_with_state(
                    (context.clone(), self.access),
                    guard,
                ));
        let run = async move { serve_bounded(listener, app, context).await }.boxed();
        Ok(BoundEndpoint {
            id: "http".into(),
            addresses: vec![address],
            run,
        })
    }
}

async fn guard(
    State((context, access)): State<(EndpointContext, AccessPolicy)>,
    request: Request,
    next: Next,
) -> Response {
    if context.shutdown.is_cancelled() {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    let authority = request.uri().authority().map(|a| a.as_str()).or_else(|| {
        request
            .headers()
            .get(header::HOST)
            .and_then(|v| v.to_str().ok())
    });
    let origin = request.headers().get(header::ORIGIN);
    if request.headers().get_all(header::ORIGIN).iter().count() > 1
        || request.headers().get_all(header::HOST).iter().count() > 1
        || origin.is_some_and(|value| value.to_str().is_err())
        || !authority.is_some_and(|authority| {
            access.permits(authority, origin.and_then(|v| v.to_str().ok()))
        })
    {
        return StatusCode::FORBIDDEN.into_response();
    }
    let Ok(request_permit) = context.requests.clone().try_acquire_owned() else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    // A watchdog outside body polling also covers HTTP/2 flow-control stalls.
    let watchdog = request
        .extensions()
        .get::<Arc<crate::timed_io::ConnectionState>>()
        .map(|state| {
            state.request(Duration::from_secs(
                context.limits.call_timeout_secs + context.limits.io_timeout_secs,
            ))
        });
    // Reserve capacity before collecting, including room for the SDK's decoding and serialization copies.
    let Ok(buffer_permit) = context
        .buffers
        .clone()
        .try_acquire_many_owned((context.limits.max_message_bytes * 4) as u32)
    else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    let (parts, body) = request.into_parts();
    let bytes = match tokio::time::timeout(
        Duration::from_secs(context.limits.io_timeout_secs),
        to_bytes(body, context.limits.max_message_bytes),
    )
    .await
    {
        Ok(Ok(bytes)) => bytes,
        Ok(Err(_)) => return StatusCode::PAYLOAD_TOO_LARGE.into_response(),
        Err(_) => return StatusCode::REQUEST_TIMEOUT.into_response(),
    };
    if parts.method == http::Method::POST
        && let Some(response) = validate_version(&parts.headers, &bytes)
    {
        return response;
    }
    let response = match tokio::time::timeout(
        Duration::from_secs(context.limits.call_timeout_secs),
        next.run(Request::from_parts(parts, Body::from(bytes))),
    )
    .await
    {
        Ok(response) => response,
        Err(_) => return StatusCode::GATEWAY_TIMEOUT.into_response(),
    };
    let (mut parts, body) = response.into_parts();
    parts.headers.insert(
        header::CACHE_CONTROL,
        http::HeaderValue::from_static("no-store"),
    );
    parts
        .headers
        .insert("x-accel-buffering", http::HeaderValue::from_static("no"));
    let deadline =
        tokio::time::Instant::now() + Duration::from_secs(context.limits.call_timeout_secs);
    let state = (
        body.into_data_stream(),
        request_permit,
        buffer_permit,
        0usize,
        context,
        deadline,
        watchdog,
    );
    let stream = futures::stream::try_unfold(
        state,
        |(mut stream, request_permit, buffer_permit, used, context, deadline, watchdog)| async move {
            let chunk = tokio::select! {
                biased;
                _ = context.shutdown.cancelled() => return Err(std::io::Error::other("server shutdown")),
                result = tokio::time::timeout_at(deadline, stream.next()) => result.map_err(|_| std::io::Error::other("response deadline"))?,
            };
            match chunk {
                Some(Ok(bytes)) if used + bytes.len() <= context.limits.max_message_bytes => {
                    let used = used + bytes.len();
                    Ok(Some((
                        bytes,
                        (
                            stream,
                            request_permit,
                            buffer_permit,
                            used,
                            context,
                            deadline,
                            watchdog,
                        ),
                    )))
                }
                Some(_) => Err(std::io::Error::other("response exceeds limit or failed")),
                None => Ok(None),
            }
        },
    );
    Response::from_parts(parts, Body::from_stream(stream))
}

fn validate_version(headers: &http::HeaderMap, bytes: &[u8]) -> Option<Response> {
    use serde_json::{Value, json};
    let Ok(value) = serde_json::from_slice::<Value>(bytes) else {
        return None;
    };
    let id = value.get("id").cloned().unwrap_or(Value::Null);
    let invalid_id = id.as_str().is_some_and(|id| id.len() > 128);
    let id = if invalid_id { Value::Null } else { id };
    let reject = |code: i32, message: &str, data: Value| {
        Some(
            (
                StatusCode::BAD_REQUEST,
                axum::Json(json!({"jsonrpc":"2.0", "id":id,
            "error":{"code":code,"message":message,"data":data}})),
            )
                .into_response(),
        )
    };
    if invalid_id {
        return reject(-32600, "request identifier exceeds limit", Value::Null);
    }
    for name in ["mcp-protocol-version", "mcp-method", "mcp-name"] {
        if headers.get_all(name).iter().count() > 1
            || headers.get(name).is_some_and(|v| v.to_str().is_err())
        {
            return reject(-32020, "invalid or duplicate protocol header", Value::Null);
        }
    }
    let version = headers
        .get("mcp-protocol-version")
        .and_then(|v| v.to_str().ok());
    let initialize = value.get("method").and_then(Value::as_str) == Some("initialize");
    let body_version = value
        .pointer("/params/_meta/io.modelcontextprotocol~1protocolVersion")
        .and_then(Value::as_str);
    if let (Some(header), Some(body)) = (version, body_version)
        && header != body
    {
        return reject(-32020, "protocol version header mismatch", Value::Null);
    }
    let supported = json!({"supported":["2026-07-28","2025-11-25"]});
    if initialize {
        if value
            .pointer("/params/protocolVersion")
            .and_then(Value::as_str)
            != Some("2025-11-25")
        {
            return reject(-32022, "unsupported initialization protocol", supported);
        }
        if version.is_some_and(|v| v != "2025-11-25") {
            return reject(
                -32020,
                "initialization protocol header mismatch",
                Value::Null,
            );
        }
    } else if !matches!(version, Some("2025-11-25" | "2026-07-28")) {
        return reject(-32022, "unsupported or missing protocol version", supported);
    }
    None
}

/// Private operational listener; deployment must not route it publicly.
pub struct HealthEndpoint {
    pub bind: SocketAddr,
}

/// A private health listener with a trusted, bounded application metrics callback.
pub struct MetricsHealthEndpoint {
    endpoint: HealthEndpoint,
    metrics: Arc<dyn Fn() -> String + Send + Sync>,
    readiness: Arc<dyn Fn() -> bool + Send + Sync>,
}

impl HealthEndpoint {
    pub fn with_metrics(
        self,
        metrics: impl Fn() -> String + Send + Sync + 'static,
    ) -> MetricsHealthEndpoint {
        MetricsHealthEndpoint {
            endpoint: self,
            metrics: Arc::new(metrics),
            readiness: Arc::new(|| true),
        }
    }
}

impl MetricsHealthEndpoint {
    /// Read already-observed dependency health without performing I/O in a probe.
    pub fn with_readiness(mut self, readiness: impl Fn() -> bool + Send + Sync + 'static) -> Self {
        self.readiness = Arc::new(readiness);
        self
    }
}

impl Endpoint for HealthEndpoint {
    fn id(&self) -> &str {
        "health"
    }
    fn bindings(&self) -> Vec<Binding> {
        vec![Binding {
            network: Network::Tcp,
            address: self.bind,
        }]
    }
    async fn bind(self, context: EndpointContext) -> Result<BoundEndpoint, ServerError> {
        bind_health(self.bind, context, Arc::new(String::new), Arc::new(|| true)).await
    }
}

impl Endpoint for MetricsHealthEndpoint {
    fn id(&self) -> &str {
        "health"
    }
    fn bindings(&self) -> Vec<Binding> {
        self.endpoint.bindings()
    }
    async fn bind(self, context: EndpointContext) -> Result<BoundEndpoint, ServerError> {
        bind_health(self.endpoint.bind, context, self.metrics, self.readiness).await
    }
}

async fn bind_health(
    bind: SocketAddr,
    context: EndpointContext,
    metrics: Arc<dyn Fn() -> String + Send + Sync>,
    readiness: Arc<dyn Fn() -> bool + Send + Sync>,
) -> Result<BoundEndpoint, ServerError> {
    let listener = tokio::net::TcpListener::bind(bind).await?;
    let address = listener.local_addr()?;
    let app = Router::new()
        .route("/live", get(|| async { "live" }))
        .route(
            "/ready",
            get(move |State(context): State<EndpointContext>| {
                let readiness = readiness.clone();
                async move {
                    if context.ready.load(Ordering::Acquire) && readiness() {
                        StatusCode::OK
                    } else {
                        StatusCode::SERVICE_UNAVAILABLE
                    }
                }
            }),
        )
        .route(
            "/metrics",
            get(move |State(context): State<EndpointContext>| {
                let metrics = metrics.clone();
                async move {
                    let mut output = format!(
                        "openlegal_tool_calls_total {}\nopenlegal_tool_failures_total {}\n",
                        context.handler.counters.calls.load(Ordering::Relaxed),
                        context.handler.counters.failures.load(Ordering::Relaxed)
                    );
                    let extra = metrics();
                    if extra.len() <= 16 * 1024 {
                        output.push_str(&extra);
                    }
                    output
                }
            }),
        )
        .with_state(context.clone());
    Ok(BoundEndpoint {
        id: "health".into(),
        addresses: vec![address],
        run: async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(context.shutdown.cancelled_owned())
                .await?;
            Ok(())
        }
        .boxed(),
    })
}
