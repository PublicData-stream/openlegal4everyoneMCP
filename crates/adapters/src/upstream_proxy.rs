//! Explicit operator-selected routing for one upstream, independent of ambient proxies.
use percent_encoding::percent_decode_str;
use std::fmt;
use url::Url;

/// A trusted SOCKS5 next hop. Destination DNS is still checked and pinned by the
/// upstream adapter; the proxy receives an IP address, while TLS verifies the
/// original upstream hostname. No fallback to a direct connection is permitted.
#[derive(Clone)]
pub struct Socks5Proxy(reqwest::Proxy);

#[derive(Clone, Copy, Debug)]
pub struct InvalidSocks5Proxy;

impl fmt::Display for InvalidSocks5Proxy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("invalid SOCKS5 proxy configuration")
    }
}
impl std::error::Error for InvalidSocks5Proxy {}

impl fmt::Debug for Socks5Proxy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Socks5Proxy([redacted])")
    }
}

impl Socks5Proxy {
    /// Parse a bounded `socks5://[username:password@]host:port` operator setting.
    /// Private next hops are allowed explicitly; this does not relax destination
    /// controls. Remote-DNS `socks5h` is rejected to preserve checked IP pinning.
    pub fn new(value: &str) -> Result<Self, InvalidSocks5Proxy> {
        if value.len() > 2048
            || value.trim() != value
            || value.chars().any(char::is_control)
            || value.contains('\\')
        {
            return Err(InvalidSocks5Proxy);
        }
        let url = Url::parse(value).map_err(|_| InvalidSocks5Proxy)?;
        // Non-special URL schemes accept opaque/percent-encoded hosts that
        // reqwest's HTTP-URI proxy matcher can silently discard. Only accept an
        // ASCII DNS/IP authority here, so mandatory routing cannot disappear.
        // Internationalized names must be supplied in ASCII/Punycode form.
        if let Some(url::Host::Domain(host)) = url.host()
            && (host.is_empty()
                || !host
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'-')))
        {
            return Err(InvalidSocks5Proxy);
        }
        if url.scheme() != "socks5"
            || url.host_str().is_none()
            || url.port().is_none_or(|port| port == 0)
            || !matches!(url.path(), "" | "/")
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return Err(InvalidSocks5Proxy);
        }
        if !url.username().is_empty() || url.password().is_some() {
            for encoded in [url.username(), url.password().ok_or(InvalidSocks5Proxy)?] {
                let decoded = percent_decode_str(encoded)
                    .decode_utf8()
                    .map_err(|_| InvalidSocks5Proxy)?;
                if decoded.is_empty()
                    || decoded.len() > 255
                    || decoded.chars().any(char::is_control)
                {
                    return Err(InvalidSocks5Proxy);
                }
            }
        }
        let proxy = reqwest::Proxy::all(url)
            .map_err(|_| InvalidSocks5Proxy)?
            .no_proxy(None);
        Ok(Self(proxy))
    }

    pub(crate) fn apply(&self, builder: reqwest::ClientBuilder) -> reqwest::ClientBuilder {
        builder.proxy(self.0.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_explicit_next_hops_and_bounds_decoded_credentials() {
        for valid in [
            "socks5://127.0.0.1:1080",
            "socks5://[::1]:1080/",
            "socks5://proxy.example:1080",
            "socks5://user:p%40ss@10.0.0.1:1080",
        ] {
            assert!(Socks5Proxy::new(valid).is_ok());
        }
        let encoded = "%61".repeat(255);
        assert!(Socks5Proxy::new(&format!("socks5://{encoded}:pass@127.0.0.1:1080")).is_ok());
        assert!(Socks5Proxy::new(&format!("socks5://{encoded}a:pass@127.0.0.1:1080")).is_err());
    }

    #[test]
    fn rejects_ambiguous_routing_and_redacts_secrets() {
        for invalid in [
            "http://proxy.example:1080",
            "socks5h://proxy.example:1080",
            "socks4://proxy.example:1080",
            "socks5://proxy.example",
            "socks5://proxy.example:0",
            "socks5://proxy.example:1080/path",
            "socks5://proxy.example:1080?secret=hidden",
            "socks5://proxy.example:1080#secret",
            " socks5://proxy.example:1080",
            "socks5://user@proxy.example:1080",
            "socks5://:pass@proxy.example:1080",
            "socks5://user:@proxy.example:1080",
            "socks5://user:%00@proxy.example:1080",
            "socks5://user:%ff@proxy.example:1080",
            "socks5://proxy%2f.example:1080",
            "socks5://proxy%20.example:1080",
            "socks5://한국.example:1080",
        ] {
            let error = Socks5Proxy::new(invalid).unwrap_err();
            assert_eq!(error.to_string(), "invalid SOCKS5 proxy configuration");
        }
        let proxy =
            Socks5Proxy::new("socks5://secret-user:secret-pass@proxy.example:1080").unwrap();
        assert_eq!(format!("{proxy:?}"), "Socks5Proxy([redacted])");
    }
}
