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
    #[serde(default)]
    pub database: Option<DatabaseConfig>,
}

/// Persistence is an explicit operator decision whenever retrieval is enabled.
#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum CacheConfig {
    Memory {},
    Persistent {
        #[serde(default = "default_retention_days")]
        retention_days: u64,
        #[serde(default = "default_cache_bytes")]
        max_blob_bytes: u64,
        #[serde(default = "default_snapshot_count")]
        max_snapshots_per_query: usize,
        #[serde(default = "default_global_snapshot_count")]
        max_snapshots: usize,
        #[serde(default = "default_query_count")]
        max_queries: usize,
        postgres: PostgresConfig,
        blob: BlobConfig,
    },
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PostgresConfig {
    #[serde(default = "default_url_env")]
    pub url_env: String,
    #[serde(default = "default_migration_url_env")]
    pub migration_url_env: String,
    #[serde(default = "default_pool_connections")]
    pub max_connections: u32,
    #[serde(default)]
    pub tls_mode: PostgresTlsConfig,
    pub ca_file: Option<PathBuf>,
}

#[derive(Clone, Copy, Debug, Default, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum PostgresTlsConfig {
    #[default]
    VerifyFull,
    /// Explicit opt-in for isolated demonstrations or operator-protected connections.
    Plaintext,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum BlobConfig {
    Filesystem { path: PathBuf },
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
fn default_global_snapshot_count() -> usize {
    10_000
}
fn default_query_count() -> usize {
    4096
}
fn default_pool_connections() -> u32 {
    16
}
fn default_url_env() -> String {
    "OPENLEGAL_DATABASE_URL".into()
}
fn default_migration_url_env() -> String {
    "OPENLEGAL_MIGRATION_DATABASE_URL".into()
}

impl CacheConfig {
    pub fn policy(
        &self,
    ) -> Result<openlegal_application::persistence::RetentionPolicy, ServerError> {
        let Self::Persistent {
            retention_days,
            max_blob_bytes,
            max_snapshots_per_query,
            max_snapshots,
            max_queries,
            ..
        } = self
        else {
            return Err("memory cache has no persistent retention policy".into());
        };
        let policy = openlegal_application::persistence::RetentionPolicy {
            retention_days: *retention_days,
            max_blob_bytes: *max_blob_bytes,
            max_snapshots_per_query: *max_snapshots_per_query,
            max_snapshots: *max_snapshots,
            max_queries: *max_queries,
        };
        policy.validate()?;
        Ok(policy)
    }

    pub fn validate(&self) -> Result<(), ServerError> {
        if let Self::Persistent { postgres, blob, .. } = self {
            self.policy()?;
            postgres.options()?;
            match blob {
                BlobConfig::Filesystem { path } if path.as_os_str().is_empty() => {
                    return Err("blob store requires a dedicated path".into());
                }
                _ => {}
            }
        }
        Ok(())
    }
}

impl PostgresConfig {
    pub fn options(&self) -> Result<openlegal_adapters::postgres::PostgresOptions, ServerError> {
        fn valid_env(value: &str) -> bool {
            let mut bytes = value.bytes();
            value.len() <= 128
                && bytes
                    .next()
                    .is_some_and(|b| b.is_ascii_alphabetic() || b == b'_')
                && bytes.all(|b| b.is_ascii_alphanumeric() || b == b'_')
        }
        if !valid_env(&self.url_env)
            || !valid_env(&self.migration_url_env)
            || self.url_env == self.migration_url_env
        {
            return Err("configure distinct valid runtime and migration environment names".into());
        }
        if !(2..=64).contains(&self.max_connections) {
            return Err("PostgreSQL max_connections must be between 2 and 64".into());
        }
        if self
            .ca_file
            .as_ref()
            .is_some_and(|path| path.as_os_str().is_empty())
        {
            return Err("PostgreSQL CA file path cannot be empty".into());
        }
        let tls = match self.tls_mode {
            PostgresTlsConfig::VerifyFull => {
                openlegal_adapters::postgres::PostgresTls::VerifyFull {
                    ca_file: self.ca_file.clone(),
                }
            }
            PostgresTlsConfig::Plaintext if self.ca_file.is_none() => {
                openlegal_adapters::postgres::PostgresTls::Plaintext
            }
            PostgresTlsConfig::Plaintext => {
                return Err("plaintext PostgreSQL cannot configure a TLS CA file".into());
            }
        };
        Ok(openlegal_adapters::postgres::PostgresOptions {
            max_connections: self.max_connections,
            tls,
        })
    }

    /// Resolve exactly the required secret; never put its value in errors or logs.
    pub fn connection_url(&self, migration: bool) -> Result<String, ServerError> {
        let name = if migration {
            &self.migration_url_env
        } else {
            &self.url_env
        };
        std::env::var(name)
            .ok()
            .filter(|value| !value.is_empty() && value.len() <= 8192)
            .ok_or_else(|| {
                "required PostgreSQL connection environment value is missing or invalid".into()
            })
    }
}

impl Config {
    pub fn validate_storage(&self) -> Result<(), ServerError> {
        if (self.demo.is_some() || self.database.is_some()) && self.cache.is_none() {
            return Err("retrieval requires an explicit [cache] mode: memory or persistent".into());
        }
        if self.demo.is_none() && self.database.is_none() && self.cache.is_some() {
            return Err("cache configuration requires a registered retrieval source".into());
        }
        if let Some(database) = &self.database {
            database.validate()?;
            if self.text_diff.is_none() {
                return Err("database requires text_diff for checkpoint comparisons".into());
            }
            let Some(CacheConfig::Persistent {
                blob: BlobConfig::Filesystem { path },
                ..
            }) = &self.cache
            else {
                return Err("database requires persistent PostgreSQL storage".into());
            };
            let a = std::path::absolute(path)?;
            let b = std::path::absolute(&database.blob_path)?;
            let c = std::path::absolute(&database.index_path)?;
            if a.starts_with(&b)
                || b.starts_with(&a)
                || a.starts_with(&c)
                || c.starts_with(&a)
                || b.starts_with(&c)
                || c.starts_with(&b)
            {
                return Err("cache blobs, corpus blobs, and corpus index require separate non-nested directories".into());
            }
        }
        if let Some(cache) = &self.cache {
            cache.validate()?;
        }
        Ok(())
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

    fn persistent(extra: &str) -> String {
        format!(
            "mode = 'persistent'\n{extra}\n[postgres]\n[blob]\nkind = 'filesystem'\npath = 'blobs'\n"
        )
    }

    #[test]
    fn storage_mode_and_persistent_limits_are_explicit() {
        let config: CacheConfig = toml::from_str(&persistent("")).unwrap();
        let policy = config.policy().unwrap();
        assert_eq!(policy.retention_days, 30);
        assert_eq!(policy.max_blob_bytes, 1024 * 1024 * 1024);
        assert_eq!(policy.max_snapshots_per_query, 100);
        assert_eq!(policy.max_snapshots, 10_000);
        config.validate().unwrap();
        toml::from_str::<CacheConfig>("mode = 'memory'")
            .unwrap()
            .validate()
            .unwrap();
        for bad in [
            "",
            "[filesystem]\npath='cache'",
            "mode='memory'\nmax_blob_bytes=123",
            "mode='persistent'",
            "mode='memory'\n[postgres]\n",
        ] {
            assert!(toml::from_str::<CacheConfig>(bad).is_err(), "{bad}");
        }
        for bad in [
            "retention_days=0",
            "max_blob_bytes=1",
            "max_snapshots_per_query=1001",
            "max_snapshots=0",
        ] {
            assert!(
                toml::from_str::<CacheConfig>(&persistent(bad))
                    .unwrap()
                    .validate()
                    .is_err()
            );
        }
        let demo: Config =
            toml::from_str(include_str!("../../../deploy/demo/server.toml")).unwrap();
        demo.validate_storage().unwrap();
        let mut missing_mode = demo.clone();
        missing_mode.cache = None;
        assert!(missing_mode.validate_storage().is_err());
        missing_mode.cache = Some(CacheConfig::Memory {});
        missing_mode.validate_storage().unwrap();
        missing_mode.demo = None;
        assert!(missing_mode.validate_storage().is_err());
        demo.text_diff.unwrap().validate(&demo.limits).unwrap();
    }

    #[test]
    fn postgres_defaults_use_distinct_secrets_and_verified_tls() {
        let config: PostgresConfig = toml::from_str("").unwrap();
        assert_eq!(config.url_env, "OPENLEGAL_DATABASE_URL");
        assert_eq!(config.migration_url_env, "OPENLEGAL_MIGRATION_DATABASE_URL");
        assert!(matches!(config.tls_mode, PostgresTlsConfig::VerifyFull));
        config.options().unwrap();
        for bad in [
            "url_env='secret://user:password'",
            "url_env='SAME'\nmigration_url_env='SAME'",
            "max_connections=0",
            "max_connections=1",
            "max_connections=65",
            "tls_mode='plaintext'\nca_file='ca.pem'",
            "ca_file=''",
            "url_env=''",
            "url_env='9INVALID'",
        ] {
            assert!(
                toml::from_str::<PostgresConfig>(bad)
                    .unwrap()
                    .options()
                    .is_err()
            );
        }
        assert!(toml::from_str::<PostgresConfig>("url='postgres://secret'").is_err());
        assert!(toml::from_str::<PostgresConfig>("tls_mode='prefer'").is_err());
    }
}

/// Serving an existing corpus does not require provider credentials or Kubernetes.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DatabaseConfig {
    pub blob_path: PathBuf,
    pub index_path: PathBuf,
    /// Operator-provisioned, pinned MeCab-Ko dictionary. Never downloaded at runtime.
    pub mecab_dictionary_path: PathBuf,
    pub widget_html: PathBuf,
    pub ingestion: Option<IngestionConfig>,
}
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IngestionConfig {
    pub credential_env: String,
    pub kubectl: PathBuf,
    pub kubeconfig: PathBuf,
    pub context: String,
    pub namespace: String,
    pub worker_image: String,
    /// Explicit operator authorization for managed background upstream traffic.
    pub enabled: bool,
    #[serde(default)]
    pub retain_history_bodies: bool,
}
impl DatabaseConfig {
    pub fn validate(&self) -> Result<(), ServerError> {
        if [
            &self.blob_path,
            &self.index_path,
            &self.mecab_dictionary_path,
            &self.widget_html,
        ]
        .iter()
        .any(|p| p.as_os_str().is_empty())
        {
            return Err("database paths must be explicit".into());
        }
        if let Some(i) = &self.ingestion
            && (i.credential_env.is_empty()
                || i.credential_env.len() > 128
                || !i
                    .credential_env
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'_')
                || !i.kubectl.is_absolute()
                || !i.kubeconfig.is_absolute())
        {
            return Err("ingestion requires explicit executable, kubeconfig and credential environment name".into());
        }
        Ok(())
    }
}

#[cfg(test)]
mod database_config_tests {
    use super::DatabaseConfig;

    #[test]
    fn corpus_requires_an_explicit_dictionary_without_loading_it() {
        let base =
            "blob_path='corpus-blobs'\nindex_path='corpus-index'\nwidget_html='database.html'\n";
        assert!(toml::from_str::<DatabaseConfig>(base).is_err());
        let empty: DatabaseConfig =
            toml::from_str(&format!("{base}mecab_dictionary_path=''\n")).unwrap();
        assert!(empty.validate().is_err());
        let configured: DatabaseConfig = toml::from_str(&format!(
            "{base}mecab_dictionary_path='operator-dictionary'\n"
        ))
        .unwrap();
        configured.validate().unwrap();
    }
}
