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
    pub source: SourceConfig,
    pub http: HttpConfig,
    pub webtransport: WebTransportConfig,
    pub health: HealthConfig,
    #[serde(default)]
    pub limits: Limits,
    /// Explicit, isolated synthetic workflow. Absent in ordinary server configurations.
    #[serde(default)]
    pub demo: Option<DemoConfig>,
    /// Optional text comparison; independent of synthetic upstream configuration.
    #[serde(default)]
    pub text_diff: Option<TextDiffConfig>,
    #[serde(default)]
    pub cache: Option<CacheConfig>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CacheConfig {
    pub filesystem: FilesystemCacheConfig,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FilesystemCacheConfig {
    pub path: PathBuf,
    #[serde(default = "default_retention_days")]
    pub retention_days: u64,
    #[serde(default = "default_cache_bytes")]
    pub max_bytes: u64,
    #[serde(default = "default_snapshot_count")]
    pub max_snapshots_per_query: usize,
}
fn default_retention_days() -> u64 {
    30
}
fn default_cache_bytes() -> u64 {
    1024 * 1024 * 1024
}
fn default_snapshot_count() -> usize {
    100
}
impl FilesystemCacheConfig {
    pub fn policy(
        &self,
    ) -> Result<openlegal_application::persistence::RetentionPolicy, ServerError> {
        if self.path.as_os_str().is_empty() {
            return Err("filesystem cache requires a path".into());
        }
        let policy = openlegal_application::persistence::RetentionPolicy {
            retention_days: self.retention_days,
            max_bytes: self.max_bytes,
            max_snapshots_per_query: self.max_snapshots_per_query,
        };
        policy.validate()?;
        Ok(policy)
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TextDiffConfig {
    pub widget_html: PathBuf,
}

impl TextDiffConfig {
    pub fn validate(&self, limits: &Limits) -> Result<(), ServerError> {
        if self.widget_html.as_os_str().is_empty() {
            return Err("text_diff requires a widget_html path".into());
        }
        if limits.max_message_bytes != 16 * 1024 * 1024
            || limits.max_buffer_bytes < 256 * 1024 * 1024
        {
            return Err(
                "text_diff requires 16 MiB messages and at least 256 MiB transport buffers".into(),
            );
        }
        limits.validate()
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DemoConfig {
    /// Loopback HTTP mock only; never an arbitrary caller-provided URL.
    pub upstream: String,
    /// Locally built, trusted HTML loaded once before serving.
    pub widget_html: PathBuf,
}

/// Operator-advertised corresponding source; validation never fetches the URL.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(try_from = "String")]
pub struct SourceOffer(String);

impl SourceOffer {
    pub const MAX_URL_BYTES: usize = 2048;
    pub const LICENSE: &str = "AGPL-3.0-only";
    pub const LICENSE_URL: &str = "https://www.gnu.org/licenses/agpl-3.0.html";

    /// Require an explicit absolute HTTPS destination without embedded credentials.
    /// Both input and normalized output are bounded, including percent-encoding expansion.
    pub fn new(value: &str) -> Result<Self, ServerError> {
        if value.len() > Self::MAX_URL_BYTES
            || value.chars().any(char::is_control)
            || value.trim() != value
            || value.contains('\\')
        {
            return Err("source URL exceeds its limit or contains control characters".into());
        }
        let (scheme, rest) = value
            .split_once("://")
            .ok_or("source URL must be absolute HTTPS")?;
        let authority = rest.split(['/', '?', '#', '\\']).next().unwrap_or_default();
        let parsed = url::Url::parse(value).map_err(|_| "invalid source URL")?;
        if !scheme.eq_ignore_ascii_case("https")
            || parsed.scheme() != "https"
            || authority.is_empty()
            || authority.contains('@')
            || parsed.host_str().is_none()
            || !parsed.username().is_empty()
            || parsed.password().is_some()
            || parsed.as_str().len() > Self::MAX_URL_BYTES
        {
            return Err("source URL must be absolute HTTPS with a host and no credentials".into());
        }
        Ok(Self(parsed.into()))
    }

    pub fn url(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for SourceOffer {
    type Error = ServerError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(&value)
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceConfig {
    pub url: SourceOffer,
}

#[cfg(test)]
mod source_tests {
    use super::*;

    #[test]
    fn source_offer_validates_and_normalizes_without_fetching() {
        assert_eq!(
            SourceOffer::new("HTTPS://Example.test:443/source?q=1&v=2")
                .unwrap()
                .url(),
            "https://example.test/source?q=1&v=2"
        );
        for invalid in [
            "",
            "/source",
            "http://example.test/source",
            "https:example.test",
            "https:///example.test",
            "https://user:secret@example.test",
            "https://@example.test",
            "https://example.test/\nsource",
            "https://example.test/\u{7f}",
            "https://example.test/?a=\\b",
            "https://example.test/#a\\b",
            "https://example.test/path ",
            " https://example.test",
        ] {
            assert!(SourceOffer::new(invalid).is_err(), "{invalid:?}");
        }
        let prefix = "https://example.test/";
        let at_limit = format!(
            "{prefix}{}",
            "a".repeat(SourceOffer::MAX_URL_BYTES - prefix.len())
        );
        assert!(SourceOffer::new(&at_limit).is_ok());
        assert!(SourceOffer::new(&(at_limit + "a")).is_err());
        // Unicode input fits but percent-encoding expansion must also fit.
        assert!(SourceOffer::new(&format!("{prefix}{}", "é".repeat(500))).is_err());
    }

    #[test]
    fn config_requires_valid_source_before_startup() {
        let base = r#"
[http]
bind = "127.0.0.1:8080"
allowed_hosts = ["example.test"]
allowed_origins = []
[webtransport]
bind = "127.0.0.1:8081"
certificate = "missing.pem"
private_key = "missing.key"
allowed_hosts = ["example.test"]
allowed_origins = []
[health]
bind = "127.0.0.1:8082"
"#;
        assert!(toml::from_str::<Config>(base).is_err());
        for source in [
            "",
            "url = 'http://example.test'",
            "url = 'https://secret@example.test'",
        ] {
            assert!(toml::from_str::<Config>(&format!("{base}\n[source]\n{source}")).is_err());
        }
        let config: Config = toml::from_str(&format!(
            "{base}\n[source]\nurl = 'https://example.test/source'"
        ))
        .unwrap();
        assert_eq!(config.source.url.url(), "https://example.test/source");
    }
}

#[cfg(test)]
mod text_diff_config_tests {
    use super::*;

    #[test]
    fn comparison_requires_widget_and_an_explicit_large_message_profile() {
        let mut config = TextDiffConfig {
            widget_html: "text-diff.html".into(),
        };
        assert!(config.validate(&Limits::default()).is_err());
        let limits = Limits {
            max_message_bytes: 16 * 1024 * 1024,
            max_buffer_bytes: 256 * 1024 * 1024,
            ..Default::default()
        };
        assert!(config.validate(&limits).is_ok());
        assert!(
            toml::from_str::<TextDiffConfig>(
                "git_path = \"/usr/bin/git\"\nwidget_html = \"text-diff.html\""
            )
            .is_err()
        );
        config.widget_html = "".into();
        assert!(config.validate(&limits).is_err());
    }
}

#[cfg(test)]
mod cache_config_tests {
    use super::*;

    #[test]
    fn filesystem_limits_are_explicit_bounded_and_existing_configuration_is_optional() {
        let config: FilesystemCacheConfig = toml::from_str("path = 'cache'").unwrap();
        let policy = config.policy().unwrap();
        assert_eq!(policy.retention_days, 30);
        assert_eq!(policy.max_bytes, 1024 * 1024 * 1024);
        assert_eq!(policy.max_snapshots_per_query, 100);
        for bad in [
            "path = ''",
            "path = 'cache'\nretention_days = 0",
            "path = 'cache'\nmax_bytes = 1",
            "path = 'cache'\nmax_snapshots_per_query = 1001",
        ] {
            assert!(
                toml::from_str::<FilesystemCacheConfig>(bad)
                    .unwrap()
                    .policy()
                    .is_err()
            );
        }
        assert!(toml::from_str::<FilesystemCacheConfig>("path = 'cache'\nshared = true").is_err());
        let demo: Config =
            toml::from_str(include_str!("../../../deploy/demo/server.toml")).unwrap();
        demo.cache.unwrap().filesystem.policy().unwrap();
        demo.text_diff.unwrap().validate(&demo.limits).unwrap();
    }
}
