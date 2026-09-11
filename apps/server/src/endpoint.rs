//! Startup registration and structured supervision of required endpoint adapters.

use crate::{ServerError, config::Limits, handler::McpHandler, registry::ToolRegistry};
use futures::{FutureExt, future::BoxFuture};
use std::{
    collections::HashSet,
    future::Future,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::{sync::Semaphore, task::JoinSet};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Network {
    Tcp,
    Udp,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Binding {
    pub network: Network,
    pub address: SocketAddr,
}

#[derive(Clone)]
pub struct EndpointContext {
    pub handler: McpHandler,
    pub limits: Arc<Limits>,
    pub shutdown: CancellationToken,
    pub buffers: Arc<Semaphore>,
    pub requests: Arc<Semaphore>,
    pub connections: Arc<Semaphore>,
    pub ready: Arc<AtomicBool>,
}

/// A bound listener and its owned serving future. Dropping it must close its listeners.
pub struct BoundEndpoint {
    pub id: String,
    pub addresses: Vec<SocketAddr>,
    pub run: BoxFuture<'static, Result<(), ServerError>>,
}

/// Endpoint implementations own their protocol mechanics, never independent tool policy.
pub trait Endpoint: Send + 'static {
    fn id(&self) -> &str;
    fn bindings(&self) -> Vec<Binding>;
    fn bind(
        self,
        context: EndpointContext,
    ) -> impl Future<Output = Result<BoundEndpoint, ServerError>> + Send;
}

type Binder = Box<
    dyn FnOnce(EndpointContext) -> BoxFuture<'static, Result<BoundEndpoint, ServerError>> + Send,
>;

pub struct ServerBuilder {
    registry: ToolRegistry,
    limits: Limits,
    ids: HashSet<String>,
    bindings: Vec<Binding>,
    endpoints: Vec<Binder>,
}

impl ServerBuilder {
    pub fn new(registry: ToolRegistry, limits: Limits) -> Self {
        Self {
            registry,
            limits,
            ids: HashSet::new(),
            bindings: Vec::new(),
            endpoints: Vec::new(),
        }
    }

    pub fn register_endpoint(&mut self, endpoint: impl Endpoint) -> Result<(), ServerError> {
        if endpoint.id().is_empty() || self.ids.contains(endpoint.id()) {
            return Err("duplicate or empty endpoint identifier".into());
        }
        let bindings = endpoint.bindings();
        for (index, candidate) in bindings.iter().enumerate() {
            if self
                .bindings
                .iter()
                .chain(bindings[..index].iter())
                .any(|other| bindings_conflict(*candidate, *other))
            {
                return Err("conflicting endpoint listener bindings".into());
            }
        }
        self.ids.insert(endpoint.id().to_owned());
        self.bindings.extend(bindings);
        self.endpoints
            .push(Box::new(move |context| endpoint.bind(context).boxed()));
        Ok(())
    }

    pub async fn bind(self) -> Result<RunningServer, ServerError> {
        self.limits.validate()?;
        if self.endpoints.is_empty() {
            return Err("no endpoints registered".into());
        }
        let limits = Arc::new(self.limits);
        let context = EndpointContext {
            handler: McpHandler::new(self.registry, limits.clone())?,
            buffers: Arc::new(Semaphore::new(limits.max_buffer_bytes)),
            requests: Arc::new(Semaphore::new(limits.max_in_flight)),
            connections: Arc::new(Semaphore::new(limits.max_connections)),
            limits,
            shutdown: CancellationToken::new(),
            ready: Arc::new(AtomicBool::new(false)),
        };
        let mut endpoints = Vec::new();
        for bind in self.endpoints {
            match bind(context.clone()).await {
                Ok(endpoint) => endpoints.push(endpoint),
                Err(error) => {
                    context.shutdown.cancel();
                    return Err(error);
                }
            }
        }
        Ok(RunningServer { context, endpoints })
    }
}

fn bindings_conflict(a: Binding, b: Binding) -> bool {
    a.network == b.network
        && a.address.port() != 0
        && a.address.port() == b.address.port()
        && (a.address.ip() == b.address.ip()
            || a.address.ip().is_unspecified()
            || b.address.ip().is_unspecified())
}

pub struct RunningServer {
    context: EndpointContext,
    endpoints: Vec<BoundEndpoint>,
}

impl RunningServer {
    pub fn addresses(&self) -> Vec<(String, Vec<SocketAddr>)> {
        self.endpoints
            .iter()
            .map(|endpoint| (endpoint.id.clone(), endpoint.addresses.clone()))
            .collect()
    }

    pub fn shutdown_token(&self) -> CancellationToken {
        self.context.shutdown.clone()
    }

    pub async fn run(self, shutdown: CancellationToken) -> Result<(), ServerError> {
        let mut tasks = JoinSet::new();
        for endpoint in self.endpoints {
            tasks.spawn(endpoint.run);
        }
        self.context.ready.store(true, Ordering::Release);
        let failure: Result<(), ServerError> = tokio::select! {
            biased;
            _ = shutdown.cancelled() => Ok(()),
            _ = self.context.shutdown.cancelled() => Ok(()),
            result = tasks.join_next() => match result {
                Some(Ok(Err(error))) => Err(error),
                Some(Err(error)) => Err(error.into()),
                _ => Err("required endpoint stopped unexpectedly".into()),
            }
        };
        self.context.ready.store(false, Ordering::Release);
        self.context.shutdown.cancel();
        let drain = async {
            let mut first_error: Option<ServerError> = None;
            while let Some(result) = tasks.join_next().await {
                let error = match result {
                    Ok(Ok(())) => None,
                    Ok(Err(error)) => Some(error),
                    Err(error) => Some(error.into()),
                };
                if first_error.is_none() {
                    first_error = error;
                }
            }
            match first_error {
                Some(error) => Err(error),
                None => Ok(()),
            }
        };
        let drained = tokio::time::timeout(
            Duration::from_secs(self.context.limits.shutdown_timeout_secs),
            drain,
        )
        .await;
        if drained.is_err() {
            tasks.abort_all();
            while tasks.join_next().await.is_some() {}
        }
        failure?;
        drained.map_err(|_| "endpoint shutdown deadline exceeded")??;
        Ok(())
    }
}
