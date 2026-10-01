#[path = "../examples/support/mod.rs"]
mod support;

use openlegal_adapters::{DestinationMode, HttpUpstream, upstream_proxy::Socks5Proxy};
use openlegal_application::Upstream;
use openlegal_domain::{Query, RetrievalData};
use openlegal_normalization::LayoutAProcessor;
use std::{
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::{JoinHandle, JoinSet},
    time::timeout,
};
use tokio_util::sync::CancellationToken;

const TEST_TIMEOUT: Duration = Duration::from_secs(10);

struct RunningServer {
    stop: CancellationToken,
    task: Option<JoinHandle<()>>,
}

impl RunningServer {
    async fn shutdown(&mut self) {
        self.stop.cancel();
        if let Some(task) = self.task.take() {
            timeout(TEST_TIMEOUT, task).await.unwrap().unwrap();
        }
    }
}

impl Drop for RunningServer {
    fn drop(&mut self) {
        self.stop.cancel();
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

struct HttpFixture {
    address: SocketAddr,
    requests: Arc<AtomicUsize>,
    server: RunningServer,
}

impl HttpFixture {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let requests = Arc::new(AtomicUsize::new(0));
        let observed = requests.clone();
        let router = support::router().layer(axum::middleware::from_fn(
            move |request: axum::extract::Request, next: axum::middleware::Next| {
                let observed = observed.clone();
                async move {
                    observed.fetch_add(1, Ordering::SeqCst);
                    next.run(request).await
                }
            },
        ));
        let stop = CancellationToken::new();
        let signal = stop.clone();
        let task = tokio::spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(signal.cancelled_owned())
                .await
                .unwrap();
        });
        Self {
            address,
            requests,
            server: RunningServer {
                stop,
                task: Some(task),
            },
        }
    }

    fn upstream(&self, proxy_url: &str) -> HttpUpstream {
        HttpUpstream::new(
            &format!("http://{}", self.address),
            DestinationMode::MockLoopback,
            Arc::new(LayoutAProcessor),
        )
        .unwrap()
        .with_socks5_proxy(Socks5Proxy::new(proxy_url).unwrap())
    }
}

#[derive(Clone)]
enum ProxyBehavior {
    Forward,
    Authenticate,
    RejectAuthentication,
    RejectConnect,
}

struct ProxyFixture {
    url: String,
    destinations: Arc<Mutex<Vec<SocketAddr>>>,
    authentications: Arc<AtomicUsize>,
    connections: Arc<AtomicUsize>,
    server: RunningServer,
}

impl ProxyFixture {
    async fn start(behavior: ProxyBehavior) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let url = if matches!(behavior, ProxyBehavior::Authenticate) {
            format!("socks5://operator:local-test-password@{address}")
        } else {
            format!("socks5://{address}")
        };
        let destinations = Arc::new(Mutex::new(Vec::new()));
        let observed_destinations = destinations.clone();
        let authentications = Arc::new(AtomicUsize::new(0));
        let observed_authentications = authentications.clone();
        let connections = Arc::new(AtomicUsize::new(0));
        let observed_connections = connections.clone();
        let stop = CancellationToken::new();
        let signal = stop.clone();
        let task = tokio::spawn(async move {
            let mut clients = JoinSet::new();
            loop {
                tokio::select! {
                    () = signal.cancelled() => break,
                    accepted = listener.accept() => {
                        let (stream, _) = accepted.unwrap();
                        observed_connections.fetch_add(1, Ordering::SeqCst);
                        let behavior = behavior.clone();
                        let destinations = observed_destinations.clone();
                        let authentications = observed_authentications.clone();
                        clients.spawn(async move {
                            // The whole connection, including forwarding, has a hard bound.
                            timeout(TEST_TIMEOUT, proxy_connection(
                                stream, behavior, destinations, authentications,
                            )).await.unwrap();
                        });
                    }
                    Some(completed) = clients.join_next(), if !clients.is_empty() => {
                        completed.unwrap();
                    }
                }
            }
            clients.abort_all();
            while let Some(completed) = clients.join_next().await {
                if let Err(error) = completed {
                    assert!(error.is_cancelled(), "proxy task failed: {error}");
                }
            }
        });
        Self {
            url,
            destinations,
            authentications,
            connections,
            server: RunningServer {
                stop,
                task: Some(task),
            },
        }
    }
}

async fn proxy_connection(
    mut client: TcpStream,
    behavior: ProxyBehavior,
    destinations: Arc<Mutex<Vec<SocketAddr>>>,
    authentications: Arc<AtomicUsize>,
) {
    let mut greeting = [0_u8; 2];
    client.read_exact(&mut greeting).await.unwrap();
    assert_eq!(greeting[0], 5);
    let mut methods = vec![0_u8; usize::from(greeting[1])];
    client.read_exact(&mut methods).await.unwrap();
    if matches!(behavior, ProxyBehavior::RejectAuthentication) {
        client.write_all(&[5, 0xff]).await.unwrap();
        return;
    }
    if matches!(behavior, ProxyBehavior::Authenticate) {
        assert!(methods.contains(&2));
        client.write_all(&[5, 2]).await.unwrap();
        assert_eq!(client.read_u8().await.unwrap(), 1);
        let username_length = client.read_u8().await.unwrap();
        let mut username = vec![0_u8; usize::from(username_length)];
        client.read_exact(&mut username).await.unwrap();
        let password_length = client.read_u8().await.unwrap();
        let mut password = vec![0_u8; usize::from(password_length)];
        client.read_exact(&mut password).await.unwrap();
        assert_eq!(username, b"operator");
        assert_eq!(password, b"local-test-password");
        authentications.fetch_add(1, Ordering::SeqCst);
        client.write_all(&[1, 0]).await.unwrap();
    } else {
        assert!(methods.contains(&0));
        client.write_all(&[5, 0]).await.unwrap();
    }
    let mut request = [0_u8; 4];
    client.read_exact(&mut request).await.unwrap();
    assert_eq!(&request[..3], &[5, 1, 0]);
    // A domain-name request is forbidden: destination validation must precede SOCKS.
    let ip = match request[3] {
        1 => {
            let mut bytes = [0_u8; 4];
            client.read_exact(&mut bytes).await.unwrap();
            IpAddr::V4(Ipv4Addr::from(bytes))
        }
        4 => {
            let mut bytes = [0_u8; 16];
            client.read_exact(&mut bytes).await.unwrap();
            IpAddr::V6(Ipv6Addr::from(bytes))
        }
        kind => panic!("expected a pinned destination IP, received SOCKS address type {kind}"),
    };
    let destination = SocketAddr::new(ip, client.read_u16().await.unwrap());
    assert!(destination.ip().is_loopback());
    destinations.lock().unwrap().push(destination);
    if matches!(behavior, ProxyBehavior::RejectConnect) {
        client
            .write_all(&[5, 2, 0, 1, 0, 0, 0, 0, 0, 0])
            .await
            .unwrap();
        return;
    }
    let mut target = TcpStream::connect(destination).await.unwrap();
    client
        .write_all(&[5, 0, 0, 1, 0, 0, 0, 0, 0, 0])
        .await
        .unwrap();
    // Early connection closure by the HTTP client is a normal test outcome.
    let _ = tokio::io::copy_bidirectional(&mut client, &mut target).await;
}

fn detail_query() -> Query {
    Query::Get {
        source: "layout_a".into(),
        id: "001".into(),
    }
}

async fn assert_fetch_succeeds(upstream: &HttpUpstream) {
    let fetched = timeout(
        TEST_TIMEOUT,
        upstream.fetch(detail_query(), CancellationToken::new()),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(matches!(fetched.data, RetrievalData::Get(_)));
    assert!(!fetched.raw.is_empty());
}

#[tokio::test]
async fn independently_configured_upstreams_use_their_own_socks5_proxy() {
    let mut first_http = HttpFixture::start().await;
    let mut second_http = HttpFixture::start().await;
    let mut first_proxy = ProxyFixture::start(ProxyBehavior::Forward).await;
    let mut second_proxy = ProxyFixture::start(ProxyBehavior::Forward).await;
    let first_upstream = first_http.upstream(&first_proxy.url);
    let second_upstream = second_http.upstream(&second_proxy.url);

    assert_fetch_succeeds(&first_upstream).await;
    assert_fetch_succeeds(&second_upstream).await;

    assert_eq!(
        *first_proxy.destinations.lock().unwrap(),
        [first_http.address]
    );
    assert_eq!(
        *second_proxy.destinations.lock().unwrap(),
        [second_http.address]
    );
    assert_eq!(first_http.requests.load(Ordering::SeqCst), 1);
    assert_eq!(second_http.requests.load(Ordering::SeqCst), 1);
    first_proxy.server.shutdown().await;
    second_proxy.server.shutdown().await;
    first_http.server.shutdown().await;
    second_http.server.shutdown().await;
}

#[tokio::test]
async fn socks5_username_password_authentication_forwards_the_request() {
    let mut http = HttpFixture::start().await;
    let mut proxy = ProxyFixture::start(ProxyBehavior::Authenticate).await;
    let upstream = http.upstream(&proxy.url);

    assert_fetch_succeeds(&upstream).await;

    assert_eq!(proxy.authentications.load(Ordering::SeqCst), 1);
    assert_eq!(*proxy.destinations.lock().unwrap(), [http.address]);
    assert_eq!(http.requests.load(Ordering::SeqCst), 1);
    proxy.server.shutdown().await;
    http.server.shutdown().await;
}

#[tokio::test]
async fn unavailable_proxy_never_falls_back_to_a_reachable_direct_target() {
    let mut http = HttpFixture::start().await;
    let unavailable = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_url = format!("socks5://{}", unavailable.local_addr().unwrap());
    drop(unavailable);
    let upstream = http.upstream(&proxy_url);

    let result = timeout(
        TEST_TIMEOUT,
        upstream.fetch(detail_query(), CancellationToken::new()),
    )
    .await
    .unwrap();

    assert!(result.is_err());
    assert_eq!(http.requests.load(Ordering::SeqCst), 0);
    http.server.shutdown().await;
}

#[tokio::test]
async fn rejected_proxy_never_falls_back_to_a_reachable_direct_target() {
    for behavior in [
        ProxyBehavior::RejectAuthentication,
        ProxyBehavior::RejectConnect,
    ] {
        let mut http = HttpFixture::start().await;
        let mut proxy = ProxyFixture::start(behavior).await;
        let upstream = http.upstream(&proxy.url);

        let result = timeout(
            TEST_TIMEOUT,
            upstream.fetch(detail_query(), CancellationToken::new()),
        )
        .await
        .unwrap();

        assert!(result.is_err());
        assert!(proxy.connections.load(Ordering::SeqCst) > 0);
        assert_eq!(http.requests.load(Ordering::SeqCst), 0);
        proxy.server.shutdown().await;
        http.server.shutdown().await;
    }
}
