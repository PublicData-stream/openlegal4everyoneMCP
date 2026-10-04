//! Evidence-backed reuse conditions attached to original materials, independently
//! of the containing legal record. Unknown rights never imply permission.
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Public provenance never carries API credentials, userinfo or fragments.
pub fn public_source_url(value: &str) -> bool {
    url::Url::parse(value).is_ok_and(|url| {
        url.scheme() == "https"
            && url.host_str().is_some()
            && url.username().is_empty()
            && url.password().is_none()
            && url.fragment().is_none()
            && !url.query_pairs().any(|(key, _)| {
                matches!(
                    key.to_ascii_lowercase().as_str(),
                    "oc" | "key" | "apikey" | "api_key" | "token" | "servicekey" | "password"
                )
            })
    })
}
#[derive(Clone, Debug, Default, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SourceRights {
    pub basis: String,
    pub evidence_url: String,
    pub attribution: String,
    pub commercial_use: Option<bool>,
    pub derivatives: Option<bool>,
    pub redistribution: Option<bool>,
}

impl SourceRights {
    pub fn kogl(kind: u8, evidence_url: String, attribution: String) -> Self {
        if !(0..=4).contains(&kind) {
            return Self::default();
        }
        Self {
            basis: format!("KOGL-{kind}"),
            evidence_url,
            attribution,
            commercial_use: Some(matches!(kind, 0 | 1 | 3)),
            derivatives: Some(matches!(kind, 0..=2)),
            redistribution: Some(true),
        }
    }
    pub fn legal_information() -> Self {
        Self {
            basis: "LAW OPEN DATA legal-information reuse policy".into(),
            evidence_url: "https://open.law.go.kr/LSO/information/guide.do".into(),
            attribution: "법제처 국가법령정보센터 / 원 제공기관".into(),
            commercial_use: Some(true),
            derivatives: Some(true),
            redistribution: Some(true),
        }
    }
    pub fn can_store(&self) -> bool {
        self.redistribution == Some(true)
            && !self.basis.trim().is_empty()
            && !self.attribution.trim().is_empty()
            && public_source_url(&self.evidence_url)
            && self.commercial_use.is_some()
            && self.derivatives.is_some()
    }
    pub fn can_process(&self) -> bool {
        self.can_store() && self.derivatives == Some(true)
    }
    pub fn warnings(&self) -> Vec<&'static str> {
        let mut warnings = Vec::new();
        if !self.can_store() {
            warnings.push("rights_unverified");
        }
        if self.commercial_use == Some(false) {
            warnings.push("noncommercial_only");
        }
        if self.derivatives == Some(false) {
            warnings.push("no_derivatives_original_only");
        }
        warnings
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct OriginalResource {
    /// Zero identifies the provider response; positive ordinals identify attached evidence.
    pub ordinal: u32,
    pub title: String,
    pub media_type: String,
    pub source_url: String,
    pub retained: bool,
    pub rights: SourceRights,
}

pub fn resources(metadata: &std::collections::BTreeMap<String, String>) -> Vec<OriginalResource> {
    metadata
        .get("original_resources")
        .and_then(|value| serde_json::from_str(value).ok())
        .unwrap_or_default()
}

pub fn warnings(metadata: &std::collections::BTreeMap<String, String>) -> Vec<&'static str> {
    let mut result = std::collections::BTreeSet::new();
    for resource in resources(metadata) {
        result.extend(resource.rights.warnings());
    }
    result.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn unproven_flags_and_credential_provenance_never_authorize_storage() {
        let valid = SourceRights::legal_information();
        for field in ["basis", "attribution", "evidence"] {
            let mut rights = valid.clone();
            match field {
                "basis" => rights.basis = " ".into(),
                "attribution" => rights.attribution.clear(),
                _ => rights.evidence_url.clear(),
            }
            assert!(!rights.can_store());
            assert!(!rights.can_process());
        }
        for url in [
            "http://example.test/license",
            "https://user@example.test/license",
            "https://example.test/?OC=secret",
            "https://example.test/#token",
            "invalid",
        ] {
            let mut rights = valid.clone();
            rights.evidence_url = url.into();
            assert!(!rights.can_store());
            assert!(!public_source_url(url));
        }
    }
    #[test]
    fn supplementary_rights_do_not_inherit_primary_permission() {
        assert!(!SourceRights::default().can_store());
        assert!(!SourceRights::default().can_process());
        assert!(SourceRights::legal_information().can_process());
        for kind in 0..=4 {
            let rights =
                SourceRights::kogl(kind, "https://example.test/license".into(), "issuer".into());
            assert!(rights.can_store());
            assert_eq!(rights.can_process(), matches!(kind, 0..=2));
            assert_eq!(rights.commercial_use, Some(matches!(kind, 0 | 1 | 3)));
        }
    }
}

/// Exact original material; transport adapters must serve bytes without alteration.
#[derive(Debug)]
pub struct OriginalEvidence {
    /// Only credential-bearing transport query values may differ from wire bytes.
    pub credentials_redacted: bool,
    pub bytes: Vec<u8>,
    pub media_type: String,
    pub title: String,
    pub rights: SourceRights,
}
