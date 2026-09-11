//! WebTransport binding v1: one reliable bidirectional NDJSON stream, no datagrams.

use std::{
    borrow::Cow,
    collections::HashMap,
    io,
    net::SocketAddr,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};

use rmcp::{
    RoleServer, ServiceExt,
    model::{
        ClientJsonRpcMessage, ClientNotification, ClientRequest, ErrorData, GetMeta,
        ProtocolVersion, RequestId, ServerInfo, ServerJsonRpcMessage, ServerResult,
    },
    service::{NotificationContext, RequestContext, Service},
    transport::Transport,
};
use tokio::{
    sync::{Mutex as AsyncMutex, OwnedSemaphorePermit},
    task::JoinSet,
    time::timeout,
};
use tokio_util::sync::CancellationToken;
use wtransport::{Endpoint as QuicEndpoint, Identity, ServerConfig};

use crate::{
    ServerError,
    config::AccessPolicy,
    endpoint::{Binding, BoundEndpoint, Endpoint, EndpointContext, Network},
    framing::{FrameReader, write_json},
    handler::McpHandler,
};

pub const PATH: &str = "/mcp-wt/v1";

/// A separately bound TLS/QUIC listener. Certificates must cover the backend authority.
pub struct WebTransportEndpoint {
    pub bind: SocketAddr,
    pub certificate: PathBuf,
    pub private_key: PathBuf,
    pub access: AccessPolicy,
}

impl Endpoint for WebTransportEndpoint {
    fn id(&self) -> &str {
        "webtransport"
    }
    fn bindings(&self) -> Vec<Binding> {
        vec![Binding {
            network: Network::Udp,
            address: self.bind,
        }]
    }
    async fn bind(self, context: EndpointContext) -> Result<BoundEndpoint, ServerError> {
        self.access.validate()?;
        context.limits.validate()?;
        let identity = Identity::load_pemfiles(&self.certificate, &self.private_key).await?;
        let mut transport_config = wtransport::quinn::TransportConfig::default();
        transport_config
            // CONNECT, one application stream, and one extra to reject promptly.
            .max_concurrent_bidi_streams(3_u32.into())
            // HTTP/3 control and QPACK streams, plus one rejectable app stream.
            .max_concurrent_uni_streams(4_u32.into())
            .stream_receive_window((context.limits.max_message_bytes as u32).into())
            .receive_window((context.limits.max_message_bytes as u32 * 2).into())
            .send_window(context.limits.max_message_bytes as u64 * 2)
            .datagram_receive_buffer_size(Some(4096))
            .datagram_send_buffer_size(4096);
        let mut config = ServerConfig::builder()
            .with_bind_address(self.bind)
            .with_custom_transport(identity, transport_config)
            .max_idle_timeout(Some(Duration::from_secs(context.limits.idle_timeout_secs)))?
            .build();
        config.quic_config_mut()
            .max_incoming(context.limits.max_connections)
            .incoming_buffer_size(65536)
            .incoming_buffer_size_total(context.limits.max_buffer_bytes.min(8 * 1024 * 1024) as u64);
        let endpoint = QuicEndpoint::server(config)?;
        let address = endpoint.local_addr()?;
        Ok(BoundEndpoint {
            id: self.id().into(),
            addresses: vec![address],
            run: Box::pin(async move {
                let mut sessions = JoinSet::new();
                let mut task_failed = false;
                loop {
                    tokio::select! {
                        biased;
                        () = context.shutdown.cancelled() => break,
                        result = sessions.join_next(), if !sessions.is_empty() => {
                            if result.is_some_and(|result| result.is_err()) {
                                task_failed = true;
                                context.shutdown.cancel();
                                break;
                            }
                        }
                        incoming = endpoint.accept() => {
                            let Ok(permit) = context.connections.clone().try_acquire_owned() else { incoming.refuse(); continue; };
                            let context = context.clone();
                            let access = self.access.clone();
                            sessions.spawn(async move {
                                let _permit = permit;
                                // Connection errors are isolated. Never log peer-controlled payloads.
                                let _ = serve_connection(incoming, access, context).await;
                            });
                        }
                    }
                }
                endpoint.close(0_u32.into(), b"server shutdown");
                if timeout(
                    Duration::from_secs(context.limits.shutdown_timeout_secs),
                    async {
                        while let Some(result) = sessions.join_next().await {
                            task_failed |= result.is_err();
                        }
                        endpoint.wait_idle().await;
                    },
                )
                .await
                .is_err()
                {
                    sessions.abort_all();
                    while sessions.join_next().await.is_some() {}
                    return Err(io::Error::other("WebTransport shutdown deadline exceeded").into());
                }
                if task_failed {
                    return Err(io::Error::other("WebTransport connection task failed").into());
                }
                Ok(())
            }),
        })
    }
}

async fn serve_connection(
    incoming: wtransport::endpoint::IncomingSession,
    access: AccessPolicy,
    context: EndpointContext,
) -> Result<(), ServerError> {
    let io_timeout = Duration::from_secs(context.limits.io_timeout_secs);
    let request = tokio::select! {
        () = context.shutdown.cancelled() => return Ok(()),
        request = timeout(io_timeout, incoming) => request??,
    };
    if request.path() != PATH {
        request.not_found().await;
        return Ok(());
    }
    if !access.permits(request.authority(), request.origin()) {
        request.forbidden().await;
        return Ok(());
    }
    let connection = timeout(io_timeout, request.accept()).await??;
    let cancel = context.shutdown.child_token();
    let result = async {
        let (send, recv) = tokio::select! {
            () = cancel.cancelled() => return Ok(()),
            stream = timeout(io_timeout, connection.accept_bi()) => stream??,
            _ = connection.accept_uni() => return Err(io::Error::other("unidirectional application stream not supported").into()),
            _ = connection.receive_datagram() => return Err(io::Error::other("application datagrams not supported").into()),
        };
        let ledger = Arc::new(Mutex::new(HashMap::new()));
        let mut transport = BoundedTransport {
            reader: FrameReader::new(recv, context.limits.max_message_bytes, context.buffers.clone(), Duration::from_secs(context.limits.idle_timeout_secs), io_timeout),
            writer: Arc::new(AsyncMutex::new(send)), context: context.clone(), ledger: ledger.clone(), cancel: cancel.clone(), era: Era::Undecided, initialized: false, prefetched: None,
        };
        let service = TrackedHandler { inner: context.handler.clone(), ledger, cancel: cancel.clone() };
        let serving = async {
            // Select the lifecycle before entering the SDK. Modern first requests
            // must run concurrently, so their cancellation can already be received.
            let first_request = timeout(io_timeout, async {
                let mut negotiation_replies = 0;
                loop {
                    let first = transport.next().await?.ok_or_else(|| io::Error::other("missing first request"))?;
                    if let ClientJsonRpcMessage::Request(request) = &first {
                        let reply = if transport.era == Era::Undecided {
                            Some(ServerJsonRpcMessage::response(ServerResult::EmptyResult(rmcp::model::EmptyResult {}), request.id.clone()))
                        } else if transport.era == Era::Modern {
                            request.request.get_meta().protocol_version()
                                .filter(|version| version != &ProtocolVersion::V_2026_07_28)
                                .map(|version| ServerJsonRpcMessage::error(
                                    ErrorData::unsupported_protocol_version(version, &service.supported_protocol_versions()),
                                    Some(request.id.clone()),
                                ))
                        } else { None };
                        if let Some(reply) = reply {
                            if negotiation_replies >= context.limits.max_calls_per_connection {
                                return Err(io::Error::other("protocol negotiation admission exhausted"));
                            }
                            negotiation_replies += 1;
                            transport.send(reply).await?;
                            continue;
                        }
                    }
                    return Ok::<_, io::Error>(first);
                }
            });
            let first = tokio::select! {
                biased;
                () = cancel.cancelled() => return Ok(()),
                first = first_request => first??,
            };
            let modern = transport.era == Era::Modern;
            transport.prefetched = Some(first);
            let running = if modern {
                rmcp::service::serve_directly_with_ct(service, transport, None, cancel.clone())
            } else { service.serve_with_ct(transport, cancel.clone()).await? };
            running.waiting().await?;
            Ok::<(), ServerError>(())
        };
        // Keep ownership of the SDK join future when another terminal event wins.
        // Dropping `RunningService::waiting()` would detach its internal JoinHandle.
        tokio::pin!(serving);
        let terminal = tokio::select! {
            result = &mut serving => return result,
            () = cancel.cancelled() => Ok(()),
            _ = connection.closed() => Ok(()),
            _ = connection.accept_bi() => Err(io::Error::other("only one application stream is allowed").into()),
            _ = connection.accept_uni() => Err(io::Error::other("unidirectional application stream not supported").into()),
            _ = connection.receive_datagram() => Err(io::Error::other("application datagrams not supported").into()),
        };
        cancel.cancel();
        connection.close(0_u32.into(), b"session ended");
        serving.await?;
        terminal
    }.await;
    cancel.cancel();
    connection.close(0_u32.into(), b"session ended");
    result
}

struct Admission {
    permits: Option<(OwnedSemaphorePermit, OwnedSemaphorePermit)>,
    cancelled: bool,
    completed: bool,
    _retired: Option<OwnedSemaphorePermit>,
}
impl Admission {
    fn retire(&mut self) {
        if let Some((_request, mut frame)) = self.permits.take() {
            // Retained cancellation IDs and ledger entries remain byte-accounted.
            self._retired = frame.split(512);
        }
    }
}
type Ledger = Arc<Mutex<HashMap<RequestId, Admission>>>;

#[derive(Clone, Copy, PartialEq)]
enum Era {
    Undecided,
    Legacy,
    Modern,
}

struct BoundedTransport {
    reader: FrameReader<wtransport::RecvStream>,
    writer: Arc<AsyncMutex<wtransport::SendStream>>,
    context: EndpointContext,
    ledger: Ledger,
    cancel: CancellationToken,
    era: Era,
    initialized: bool,
    prefetched: Option<ClientJsonRpcMessage>,
}

impl BoundedTransport {
    async fn next(&mut self) -> io::Result<Option<ClientJsonRpcMessage>> {
        loop {
            let Some(frame) = self.reader.read().await? else {
                return Ok(None);
            };
            let message: ClientJsonRpcMessage =
                serde_json::from_slice(&frame.bytes).map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidData, "invalid JSON-RPC frame")
                })?;
            match &message {
                ClientJsonRpcMessage::Request(request) => {
                    if matches!(&request.id, RequestId::String(id) if id.len() > 128) {
                        return Err(io::Error::other("request ID exceeds limit"));
                    }
                    let initialize = matches!(request.request, ClientRequest::InitializeRequest(_));
                    let version = request.request.get_meta().protocol_version();
                    if initialize && version.is_some() {
                        return Err(io::Error::other("mixed protocol lifecycle"));
                    }
                    match self.era {
                        Era::Undecided if initialize => self.era = Era::Legacy,
                        Era::Undecided if version.is_some() => self.era = Era::Modern,
                        Era::Undecided
                            if matches!(request.request, ClientRequest::PingRequest(_)) => {}
                        Era::Undecided => {
                            return Err(io::Error::other(
                                "protocol metadata or initialization required",
                            ));
                        }
                        Era::Modern if initialize || version.is_none() => {
                            return Err(io::Error::other("mixed protocol lifecycle"));
                        }
                        Era::Legacy if initialize || version.is_some() => {
                            return Err(io::Error::other("mixed protocol lifecycle"));
                        }
                        _ => {}
                    }
                    let mut ledger = self
                        .ledger
                        .lock()
                        .map_err(|_| io::Error::other("request ledger unavailable"))?;
                    let active = ledger
                        .values()
                        .filter(|entry| entry.permits.is_some())
                        .count();
                    // Cancelled IDs remain bounded tombstones, preventing old queued replies
                    // from being mistaken for a newly reused request ID.
                    if ledger.contains_key(&request.id)
                        || ledger.len() >= self.context.limits.max_in_flight
                        || active >= self.context.limits.max_calls_per_connection
                    {
                        return Err(io::Error::other("connection request admission exhausted"));
                    }
                    let request_permit = self
                        .context
                        .requests
                        .clone()
                        .try_acquire_owned()
                        .map_err(|_| io::Error::other("global request admission exhausted"))?;
                    ledger.insert(
                        request.id.clone(),
                        Admission {
                            permits: Some((request_permit, frame.permit)),
                            cancelled: false,
                            completed: false,
                            _retired: None,
                        },
                    );
                    return Ok(Some(message));
                }
                ClientJsonRpcMessage::Notification(notification) => {
                    match &notification.notification {
                        ClientNotification::CancelledNotification(cancelled) => {
                            let Some(id) = &cancelled.params.request_id else {
                                continue;
                            };
                            let mut ledger = self
                                .ledger
                                .lock()
                                .map_err(|_| io::Error::other("request ledger unavailable"))?;
                            let Some(entry) = ledger.get_mut(id) else {
                                continue;
                            };
                            if entry.cancelled {
                                continue;
                            }
                            entry.cancelled = true;
                            if entry.completed {
                                entry.retire();
                            }
                            return Ok(Some(message));
                        }
                        ClientNotification::InitializedNotification(_)
                            if self.era == Era::Legacy && !self.initialized =>
                        {
                            self.initialized = true;
                            return Ok(Some(message));
                        }
                        // No other client notifications are advertised by this foundation.
                        _ => return Err(io::Error::other("unsupported notification")),
                    }
                }
                _ => return Err(io::Error::other("client responses are not supported")),
            }
        }
    }
}

impl Transport<RoleServer> for BoundedTransport {
    type Error = io::Error;
    fn send(
        &mut self,
        message: ServerJsonRpcMessage,
    ) -> impl Future<Output = io::Result<()>> + Send + 'static {
        let writer = self.writer.clone();
        let context = self.context.clone();
        let ledger = self.ledger.clone();
        let cancel = self.cancel.clone();
        async move {
            let id = match &message {
                ServerJsonRpcMessage::Response(response) => Some(response.id.clone()),
                ServerJsonRpcMessage::Error(error) => error.id.clone(),
                _ => None,
            };
            let mut writer = tokio::select! { () = cancel.cancelled() => return Ok(()), writer = writer.lock() => writer };
            if cancel.is_cancelled() {
                return Ok(());
            }
            if let Some(id) = &id {
                let suppress = ledger
                    .lock()
                    .map_err(|_| io::Error::other("request ledger unavailable"))?
                    .get(id)
                    .is_none_or(|entry| entry.cancelled);
                if suppress {
                    return Ok(());
                }
            }
            let result = tokio::select! {
                biased;
                () = cancel.cancelled() => Ok(()),
                result = write_json(
                    &mut *writer,
                    &message,
                    context.limits.max_message_bytes,
                    &context.buffers,
                    Duration::from_secs(context.limits.io_timeout_secs),
                ) => result,
            };
            if let Some(id) = id {
                let mut ledger = ledger
                    .lock()
                    .map_err(|_| io::Error::other("request ledger unavailable"))?;
                if ledger.get(&id).is_some_and(|entry| !entry.cancelled) {
                    ledger.remove(&id);
                }
            }
            if result.is_err() {
                cancel.cancel();
            }
            result
        }
    }
    async fn receive(&mut self) -> Option<ClientJsonRpcMessage> {
        if let Some(message) = self.prefetched.take() {
            return Some(message);
        }
        match self.next().await {
            Ok(Some(message)) => Some(message),
            _ => {
                self.cancel.cancel();
                None
            }
        }
    }
    async fn close(&mut self) -> io::Result<()> {
        self.cancel.cancel();
        Ok(())
    }
}

struct TrackedHandler {
    inner: McpHandler,
    ledger: Ledger,
    cancel: CancellationToken,
}

impl Service<RoleServer> for TrackedHandler {
    async fn handle_request(
        &self,
        request: ClientRequest,
        context: RequestContext<RoleServer>,
    ) -> Result<ServerResult, ErrorData> {
        let id = context.id.clone();
        let token = context.ct.clone();
        // Inline versions always belong to the modern lifecycle in this binding;
        // advertising a legacy handshake version does not permit it inline.
        let result = if let Some(version) = context.meta.protocol_version()
            && version != ProtocolVersion::V_2026_07_28
        {
            Err(ErrorData::unsupported_protocol_version(
                version,
                &self.supported_protocol_versions(),
            ))
        } else {
            tokio::select! {
                () = self.cancel.cancelled() => Err(ErrorData::internal_error("connection closed", None)),
                () = token.cancelled() => Err(ErrorData::internal_error("request cancelled", None)),
                result = self.inner.handle_request(request, context) => result,
            }
        };
        if let Ok(mut ledger) = self.ledger.lock()
            && let Some(entry) = ledger.get_mut(&id)
        {
            entry.completed = true;
            if entry.cancelled || self.cancel.is_cancelled() {
                entry.retire();
            }
        }
        result
    }
    async fn handle_notification(
        &self,
        notification: ClientNotification,
        context: NotificationContext<RoleServer>,
    ) -> Result<(), ErrorData> {
        self.inner.handle_notification(notification, context).await
    }
    fn get_info(&self) -> ServerInfo {
        self.inner.get_info()
    }
    fn supported_protocol_versions(&self) -> Cow<'static, [ProtocolVersion]> {
        self.inner.supported_protocol_versions()
    }
}
