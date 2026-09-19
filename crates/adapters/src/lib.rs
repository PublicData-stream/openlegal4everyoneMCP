//! Bounded HTTP mechanics. Retry, cache and refresh policy belong to application.

#[cfg(test)]
extern crate self as openlegal_adapters;

pub mod blob;
mod cache;
pub mod corpus;
pub mod document_jobs;
pub mod postgres;
pub mod text_diff;
pub use cache::MemoryCache;

use futures::{FutureExt, future::BoxFuture};
use openlegal_application::{FetchedPayload, Upstream};
use openlegal_domain::{Query, RetrievalError};
use openlegal_normalization::{MAX_PAYLOAD_BYTES, PayloadProcessor, WorkBudget};
use std::{
    net::{IpAddr, SocketAddr},
    sync::Arc,
    time::{Duration, SystemTime},
};
use tokio_util::sync::CancellationToken;
use url::Url;

#[derive(Clone, Copy, Debug)]
pub enum DestinationMode {
    /// Public HTTPS only, with checked DNS results pinned to each connection.
    PublicHttps,
    /// Explicit isolated test exception: literal loopback HTTP address and port only.
    MockLoopback,
}

#[derive(Clone)]
pub struct HttpUpstream {
    origin: Url,
    mode: DestinationMode,
    processor: Arc<dyn PayloadProcessor>,
    resolver: Option<hickory_resolver::TokioResolver>,
}

impl HttpUpstream {
    pub fn new(
        base_url: &str,
        mode: DestinationMode,
        processor: Arc<dyn PayloadProcessor>,
    ) -> Result<Self, RetrievalError> {
        if base_url.len() > 512 {
            return Err(RetrievalError::InvalidInput);
        }
        let origin = Url::parse(base_url).map_err(|_| RetrievalError::InvalidInput)?;
        if !origin.username().is_empty()
            || origin.password().is_some()
            || origin.host_str().is_none()
            || origin.path() != "/"
            || origin.query().is_some()
            || origin.fragment().is_some()
        {
            return Err(RetrievalError::InvalidInput);
        }
        match mode {
            DestinationMode::PublicHttps
                if origin.scheme() != "https" || origin.port_or_known_default() != Some(443) =>
            {
                return Err(RetrievalError::InvalidInput);
            }
            DestinationMode::MockLoopback
                if origin.scheme() != "http"
                    || !literal_ip(&origin).is_some_and(|ip| ip.is_loopback()) =>
            {
                return Err(RetrievalError::InvalidInput);
            }
            _ => {}
        }
        if matches!(mode, DestinationMode::PublicHttps)
            && literal_ip(&origin).is_some_and(|ip| !public_address(ip))
        {
            return Err(RetrievalError::InvalidInput);
        }
        let resolver = if literal_ip(&origin).is_none() {
            let mut builder = hickory_resolver::TokioResolver::builder_tokio()
                .map_err(|_| RetrievalError::InvalidInput)?;
            let options = builder.options_mut();
            options.timeout = Duration::from_secs(2);
            options.attempts = 1;
            options.num_concurrent_reqs = 1;
            options.max_active_requests = 2;
            options.cache_size = 16;
            options.use_hosts_file = hickory_resolver::config::ResolveHosts::Never;
            Some(builder.build().map_err(|_| RetrievalError::InvalidInput)?)
        } else {
            None
        };
        Ok(Self {
            origin,
            mode,
            processor,
            resolver,
        })
    }

    async fn fetch_once(
        &self,
        query: Query,
        cancellation: CancellationToken,
    ) -> Result<FetchedPayload, RetrievalError> {
        query.validate()?;
        let mut url = self.origin.clone();
        match &query {
            Query::Search {
                source,
                query,
                page,
                page_size,
            } => {
                url.set_path(&format!("/{source}/search"));
                url.query_pairs_mut()
                    .append_pair("query", query)
                    .append_pair("page", &page.to_string())
                    .append_pair("page_size", &page_size.to_string());
            }
            Query::Get { source, id } => url.set_path(&format!("/{source}/records/{id}")),
        }
        let host = self
            .origin
            .host_str()
            .ok_or(RetrievalError::InvalidInput)?
            .trim_matches(['[', ']']);
        let port = self
            .origin
            .port_or_known_default()
            .ok_or(RetrievalError::InvalidInput)?;
        let addresses: Vec<SocketAddr> = if let Some(ip) = literal_ip(&self.origin) {
            vec![SocketAddr::new(ip, port)]
        } else {
            // Resolve once for this attempt, validate every answer, and pin that exact set.
            let addresses = self
                .resolver
                .as_ref()
                .ok_or(RetrievalError::Internal)?
                .lookup_ip(format!("{}.", host.trim_end_matches('.')))
                .await
                .map_err(|_| RetrievalError::Unavailable)?;
            let mut checked = Vec::new();
            for ip in addresses.iter() {
                if checked.len() >= 16 || !public_address(ip) {
                    return Err(RetrievalError::InvalidPayload);
                }
                checked.push(SocketAddr::new(ip, port));
            }
            if checked.is_empty() {
                return Err(RetrievalError::Unavailable);
            }
            checked
        };
        if addresses.iter().any(|address| match self.mode {
            DestinationMode::PublicHttps => !public_address(address.ip()),
            DestinationMode::MockLoopback => !address.ip().is_loopback(),
        }) {
            return Err(RetrievalError::InvalidPayload);
        }
        let client = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(5))
            .resolve_to_addrs(host, &addresses)
            .build()
            .map_err(|_| RetrievalError::Internal)?;
        let mut response = client
            .get(url)
            .header("accept", "application/json")
            .header("accept-encoding", "identity")
            .send()
            .await
            .map_err(|_| RetrievalError::Unavailable)?;
        match response.status().as_u16() {
            200 => {}
            429 | 503 => {
                return Err(RetrievalError::Throttled {
                    retry_after_secs: response
                        .headers()
                        .get("retry-after")
                        .and_then(|v| v.to_str().ok())
                        .and_then(retry_after),
                });
            }
            502 | 504 => return Err(RetrievalError::Unavailable),
            // Only the documented synthetic endpoint defines this as record absence.
            404 if matches!(self.mode, DestinationMode::MockLoopback)
                && matches!(query, Query::Get { .. }) =>
            {
                return Err(RetrievalError::NotFound);
            }
            _ => return Err(RetrievalError::InvalidPayload),
        }
        if response
            .headers()
            .get("content-encoding")
            .is_some_and(|v| v != "identity")
            || !response
                .headers()
                .get("content-type")
                .and_then(|v| v.to_str().ok())
                .is_some_and(|v| {
                    v.split(';')
                        .next()
                        .is_some_and(|mime| mime.trim().eq_ignore_ascii_case("application/json"))
                })
        {
            return Err(RetrievalError::InvalidPayload);
        }
        if response
            .content_length()
            .is_some_and(|n| n > MAX_PAYLOAD_BYTES as u64)
        {
            return Err(RetrievalError::ResourceLimit);
        }
        let mut raw = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| RetrievalError::Unavailable)?
        {
            if raw.len().saturating_add(chunk.len()) > MAX_PAYLOAD_BYTES {
                return Err(RetrievalError::ResourceLimit);
            }
            raw.extend_from_slice(&chunk);
        }
        if cancellation.is_cancelled() {
            return Err(RetrievalError::Cancelled);
        }
        let data = self
            .processor
            .process(&raw, &query, &mut WorkBudget::default())?;
        if cancellation.is_cancelled() {
            return Err(RetrievalError::Cancelled);
        }
        // Never reflect search query strings, credentials, or source-supplied URLs.
        let source_reference = match &query {
            Query::Get { source, id } => format!("synthetic:{source}:{id}"),
            Query::Search { source, .. } => format!("synthetic:{source}:search"),
        };
        Ok(FetchedPayload {
            raw,
            data,
            source_reference,
        })
    }
}

impl Upstream for HttpUpstream {
    fn fetch(
        &self,
        query: Query,
        cancellation: CancellationToken,
    ) -> BoxFuture<'static, Result<FetchedPayload, RetrievalError>> {
        let this = self.clone();
        async move {
            tokio::select! {
                biased;
                () = cancellation.cancelled() => Err(RetrievalError::Cancelled),
                result = tokio::time::timeout(Duration::from_secs(5), this.fetch_once(query, cancellation.clone())) =>
                    result.unwrap_or(Err(RetrievalError::Unavailable)),
            }
        }.boxed()
    }
}

fn literal_ip(url: &Url) -> Option<IpAddr> {
    match url.host()? {
        url::Host::Ipv4(ip) => Some(IpAddr::V4(ip)),
        url::Host::Ipv6(ip) => Some(IpAddr::V6(ip)),
        url::Host::Domain(_) => None,
    }
}

/// Conservative global-unicast policy. Special-purpose and transition ranges fail closed.
fn public_address(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => {
            let [a, b, c, _] = ip.octets();
            !(a == 0
                || a == 10
                || a == 127
                || a >= 224
                || (a == 100 && (64..=127).contains(&b))
                || (a == 169 && b == 254)
                || (a == 172 && (16..=31).contains(&b))
                || (a == 192
                    && (b == 168 || (b == 0 && (c == 0 || c == 2)) || (b == 88 && c == 99)))
                || (a == 198 && ((b == 18 || b == 19) || (b == 51 && c == 100)))
                || (a == 203 && b == 0 && c == 113))
        }
        IpAddr::V6(ip) => {
            let segments = ip.segments();
            (segments[0] & 0xe000 == 0x2000)
                && !(segments[0] == 0x2001 && (segments[1] < 0x0200 || segments[1] == 0x0db8))
                && segments[0] != 0x2002
                && !(segments[0] == 0x3fff && segments[1] & 0xf000 == 0)
        }
    }
}

fn retry_after(value: &str) -> Option<u64> {
    let seconds = value.parse::<u64>().ok().or_else(|| {
        httpdate::parse_http_date(value).ok().map(|date| {
            date.duration_since(SystemTime::now())
                .unwrap_or_default()
                .as_secs()
                .saturating_add(1)
        })
    })?;
    Some(seconds)
}

#[cfg(test)]
mod tests {
    use super::*;
    use openlegal_normalization::LayoutAProcessor;

    #[tokio::test]
    async fn dns_wait_is_async_cancellable_and_bounded() {
        use hickory_resolver::{
            config::{NameServerConfig, ResolverConfig},
            net::runtime::TokioRuntimeProvider,
        };
        let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let mut nameserver = NameServerConfig::udp("127.0.0.1".parse().unwrap());
        nameserver.connections[0].port = socket.local_addr().unwrap().port();
        let mut builder = hickory_resolver::TokioResolver::builder_with_config(
            ResolverConfig::from_name_servers(vec![nameserver]),
            TokioRuntimeProvider::default(),
        );
        builder.options_mut().timeout = Duration::from_millis(50);
        builder.options_mut().attempts = 1;
        builder.options_mut().num_concurrent_reqs = 1;
        builder.options_mut().max_active_requests = 2;
        builder.options_mut().use_hosts_file = hickory_resolver::config::ResolveHosts::Never;
        let mut upstream = HttpUpstream::new(
            "https://provider.example./",
            DestinationMode::PublicHttps,
            Arc::new(LayoutAProcessor),
        )
        .unwrap();
        upstream.resolver = Some(builder.build().unwrap());
        let cancellation = CancellationToken::new();
        let future = upstream.fetch(
            Query::Get {
                source: "layout_a".into(),
                id: "001".into(),
            },
            cancellation.clone(),
        );
        let task = tokio::spawn(future);
        let mut query = [0; 4096];
        tokio::time::timeout(Duration::from_secs(1), socket.recv_from(&mut query))
            .await
            .unwrap()
            .unwrap();
        cancellation.cancel();
        assert_eq!(
            tokio::time::timeout(Duration::from_millis(200), task)
                .await
                .unwrap()
                .unwrap()
                .err(),
            Some(RetrievalError::Cancelled)
        );
        let result = tokio::time::timeout(
            Duration::from_secs(1),
            upstream.fetch(
                Query::Get {
                    source: "layout_a".into(),
                    id: "001".into(),
                },
                CancellationToken::new(),
            ),
        )
        .await
        .unwrap();
        assert_eq!(result.err(), Some(RetrievalError::Unavailable));
    }

    #[test]
    fn configuration_and_special_destinations_fail_closed() {
        for address in [
            "127.0.0.1",
            "10.0.0.1",
            "100.64.0.1",
            "169.254.169.254",
            "192.0.2.1",
            "198.19.0.1",
            "203.0.113.1",
            "224.0.0.1",
            "::1",
            "::ffff:8.8.8.8",
            "64:ff9b::808:808",
            "2001:db8::1",
            "2002:808:808::1",
            "3fff::1",
        ] {
            assert!(!public_address(address.parse().unwrap()), "{address}");
        }
        for address in ["8.8.8.8", "2606:4700:4700::1111"] {
            assert!(public_address(address.parse().unwrap()));
        }
        for base in [
            "http://example.com",
            "https://127.0.0.1",
            "https://example.com:8443",
            "https://user:secret@example.com",
            "https://example.com/path",
            "https://example.com/?q=x",
        ] {
            assert!(
                HttpUpstream::new(
                    base,
                    DestinationMode::PublicHttps,
                    Arc::new(LayoutAProcessor)
                )
                .is_err()
            );
        }
        for base in [
            "http://localhost:8081",
            "http://10.0.0.1:8081",
            "https://127.0.0.1:8081",
        ] {
            assert!(
                HttpUpstream::new(
                    base,
                    DestinationMode::MockLoopback,
                    Arc::new(LayoutAProcessor)
                )
                .is_err()
            );
        }
    }
}

pub mod korean_analysis;
pub mod korean_dictionary;
pub mod search_index;

pub mod law_go_kr;

pub mod corpus_search;

pub mod corpus_read;

pub mod korean_query;
