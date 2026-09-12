//! Explicit configuration and resource policy for both transports.

use crate::ServerError;
use serde::Deserialize;
use std::{net::SocketAddr, path::PathBuf};

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Limits {
    pub max_message_bytes: usize,
    pub max_buffer_bytes: usize,
    pub max_in_flight: usize,
    pub max_connections: usize,
    pub max_calls_per_connection: usize,
    pub io_timeout_secs: u64,
    pub call_timeout_secs: u64,
    pub idle_timeout_secs: u64,
    pub shutdown_timeout_secs: u64,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_message_bytes: 1024 * 1024,
            max_buffer_bytes: 64 * 1024 * 1024,
            max_in_flight: 64,
            max_connections: 128,
            max_calls_per_connection: 8,
            io_timeout_secs: 10,
            call_timeout_secs: 30,
            idle_timeout_secs: 60,
            shutdown_timeout_secs: 15,
        }
    }
}

impl Limits {
    pub fn validate(&self) -> Result<(), ServerError> {
        if !(4096..=16 * 1024 * 1024).contains(&self.max_message_bytes)
            || self.max_buffer_bytes < self.max_message_bytes * 4
            || self.max_buffer_bytes > u32::MAX as usize
            || self.max_in_flight == 0
            || self.max_in_flight > 65536
            || self.max_connections == 0
            || self.max_connections > 65536
            || self.max_calls_per_connection == 0
            || self.max_calls_per_connection > self.max_in_flight
            || [
                self.io_timeout_secs,
                self.call_timeout_secs,
                self.idle_timeout_secs,
                self.shutdown_timeout_secs,
            ]
            .iter()
            .any(|v| *v == 0 || *v > 86400)
        {
            return Err("invalid resource limits".into());
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AccessPolicy {
    pub allowed_hosts: Vec<String>,
    pub allowed_origins: Vec<String>,
}

impl AccessPolicy {
    pub fn validate(&self) -> Result<(), ServerError> {
        if self.allowed_hosts.is_empty()
            || self
                .allowed_hosts
                .iter()
                .any(|host| host.parse::<http::uri::Authority>().is_err() || host.contains('@'))
        {
            return Err("configure explicit allowed host authorities".into());
        }
        if self
            .allowed_origins
            .iter()
            .any(|origin| normalized_origin(origin).is_none())
        {
            return Err("allowed origins must be HTTP(S) origins without paths".into());
        }
        Ok(())
    }

    /// Native clients may omit Origin. An empty origin allowlist rejects every present Origin.
    pub fn permits(&self, authority: &str, origin: Option<&str>) -> bool {
        self.allowed_hosts
            .iter()
            .any(|host| host.eq_ignore_ascii_case(authority))
            && origin.is_none_or(|origin| {
                normalized_origin(origin).is_some_and(|normalized| {
                    self.allowed_origins
                        .iter()
                        .any(|allowed| normalized_origin(allowed).as_ref() == Some(&normalized))
                })
            })
    }
}

fn normalized_origin(value: &str) -> Option<String> {
    let parsed = url::Url::parse(value).ok()?;
    if !matches!(parsed.scheme(), "http" | "https")
        || parsed.host_str().is_none()
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.path() != "/"
        || parsed.query().is_some()
        || parsed.fragment().is_some()
    {
        return None;
    }
    Some(parsed.origin().ascii_serialization())
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HttpConfig {
    pub bind: SocketAddr,
    pub allowed_hosts: Vec<String>,
    pub allowed_origins: Vec<String>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WebTransportConfig {
    pub bind: SocketAddr,
    pub certificate: PathBuf,
    pub private_key: PathBuf,
    pub allowed_hosts: Vec<String>,
    pub allowed_origins: Vec<String>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HealthConfig {
    pub bind: SocketAddr,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub http: HttpConfig,
    pub webtransport: WebTransportConfig,
    pub health: HealthConfig,
    #[serde(default)]
    pub limits: Limits,
    /// Explicit, isolated synthetic workflow. Absent in ordinary server configurations.
    #[serde(default)]
    pub demo: Option<DemoConfig>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DemoConfig {
    /// Loopback HTTP mock only; never an arbitrary caller-provided URL.
    pub upstream: String,
    /// Locally built, trusted HTML loaded once before serving.
    pub widget_html: PathBuf,
}
