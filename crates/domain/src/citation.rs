//! Capture-specific citations. Provider browser links identify upstream records,
//! not immutable copies of retained text or a determination of applicability.
use crate::legal::{DatabaseError as E, Dataset, MetadataResult, ObjectId};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub const MAX_CITATION_TEXT_BYTES: usize = 8192;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum CitationProjection {
    Document,
    Section {
        section: String,
    },
    Passage {
        section: String,
        start: usize,
        end: usize,
    },
    Metadata,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CitationId {
    pub object: ObjectId,
    pub capture_id: String,
    pub projection: CitationProjection,
}

impl CitationId {
    pub fn validate(&self) -> Result<(), E> {
        self.object.validate()?;
        if !crate::history::valid_snapshot_id(&self.capture_id) {
            return Err(E::InvalidInput);
        }
        let section = match &self.projection {
            CitationProjection::Document | CitationProjection::Metadata => None,
            CitationProjection::Section { section } => Some(section),
            CitationProjection::Passage {
                section,
                start,
                end,
            } => {
                if start >= end || end - start > MAX_CITATION_TEXT_BYTES || *end > 64 * 1024 * 1024
                {
                    return Err(E::InvalidInput);
                }
                Some(section)
            }
        };
        if section.is_some_and(|s| s.is_empty() || s.len() > 256 || s.chars().any(char::is_control))
        {
            return Err(E::InvalidInput);
        }
        Ok(())
    }

    /// Versioned canonical path identity. Section segments have a prefix so even
    /// the provider locator `..` cannot become URL path traversal.
    pub fn encode(&self) -> Result<String, E> {
        self.validate()?;
        let dataset = dataset_name(self.object.dataset);
        let mut id = format!(
            "v1/{}/{}/{}/{}/{}",
            escape(&self.object.jurisdiction),
            escape(&self.object.provider),
            dataset,
            escape(&self.object.id),
            self.capture_id
        );
        match &self.projection {
            CitationProjection::Document => id.push_str("/doc"),
            CitationProjection::Metadata => id.push_str("/metadata"),
            CitationProjection::Section { section } => {
                id.push_str(&format!("/section/s-{}", escape(section)))
            }
            CitationProjection::Passage {
                section,
                start,
                end,
            } => id.push_str(&format!("/passage/s-{}/{start}/{end}", escape(section))),
        }
        Ok(id)
    }

    pub fn decode(id: &str) -> Result<Self, E> {
        if id.len() > 2048 {
            return Err(E::InvalidInput);
        }
        let parts: Vec<_> = id.split('/').collect();
        if parts.len() < 7 || parts[0] != "v1" {
            return Err(E::InvalidInput);
        }
        let object = ObjectId {
            jurisdiction: unescape(parts[1])?,
            provider: unescape(parts[2])?,
            dataset: parse_dataset(parts[3])?,
            id: unescape(parts[4])?,
        };
        let section = |value: &str| unescape(value.strip_prefix("s-").ok_or(E::InvalidInput)?);
        let projection = match (parts[6], parts.len()) {
            ("doc", 7) => CitationProjection::Document,
            ("metadata", 7) => CitationProjection::Metadata,
            ("section", 8) => CitationProjection::Section {
                section: section(parts[7])?,
            },
            ("passage", 10) => CitationProjection::Passage {
                section: section(parts[7])?,
                start: parts[8].parse().map_err(|_| E::InvalidInput)?,
                end: parts[9].parse().map_err(|_| E::InvalidInput)?,
            },
            _ => return Err(E::InvalidInput),
        };
        let result = Self {
            object,
            capture_id: parts[5].into(),
            projection,
        };
        if result.encode()? != id {
            return Err(E::InvalidInput);
        }
        Ok(result)
    }

    pub fn resource_uri(&self) -> Result<String, E> {
        Ok(format!("openlegal://source/{}", self.encode()?))
    }

    /// Base origins are explicit configuration, never derived from request Host.
    pub fn reference_url(&self, base_url: &str) -> Result<String, E> {
        let mut url = reference_base(base_url)?;
        url.set_path(&format!("/source/{}", self.encode()?));
        Ok(url.into())
    }
}

pub fn reference_base(value: &str) -> Result<url::Url, E> {
    let url = url::Url::parse(value).map_err(|_| E::InvalidInput)?;
    if url.scheme() != "https"
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || url.path() != "/"
    {
        return Err(E::InvalidInput);
    }
    Ok(url)
}

fn escape(value: &str) -> String {
    url::form_urlencoded::byte_serialize(value.as_bytes())
        .collect::<String>()
        .replace('+', "%20")
}
fn unescape(value: &str) -> Result<String, E> {
    let form = format!("v={value}");
    let result = url::form_urlencoded::parse(form.as_bytes())
        .next()
        .ok_or(E::InvalidInput)?
        .1
        .into_owned();
    if escape(&result) != value {
        return Err(E::InvalidInput);
    }
    Ok(result)
}
pub fn dataset_name(dataset: Dataset) -> &'static str {
    dataset.as_str()
}
fn parse_dataset(value: &str) -> Result<Dataset, E> {
    Dataset::from_name(value).ok_or(E::InvalidInput)
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct CitationSearchResult {
    pub id: String,
    pub title: String,
    pub url: String,
}
/// Internal envelope: compatibility adapters emit only `results` in JSON and
/// carry qualifications in separate content blocks.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct CitationSearch {
    pub results: Vec<CitationSearchResult>,
    pub partial: bool,
    pub warnings: Vec<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct CitationDocument {
    pub id: String,
    pub title: String,
    pub text: String,
    pub url: String,
    pub metadata: BTreeMap<String, String>,
}
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct CitationDescriptor {
    pub id: String,
    pub uri: String,
    pub url: String,
    pub title: String,
    pub body_available: Option<bool>,
    pub official_url: Option<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct CitationSource {
    pub descriptor: CitationDescriptor,
    pub metadata: MetadataResult,
    pub document: Option<CitationDocument>,
    pub previous: Option<CitationDescriptor>,
    pub next: Option<CitationDescriptor>,
    pub unavailable: bool,
}

/// Only evidenced official browser routes. Missing ordinance routing evidence
/// remains unresolved instead of producing a plausible title-only link.
pub fn official_browser_url(metadata: &MetadataResult) -> Option<String> {
    if metadata.object.provider != "law_go_kr" || metadata.object.jurisdiction != "kr" {
        return None;
    }
    let serial = metadata.metadata.get("provider_record_number")?;
    if serial.is_empty()
        || serial.len() > 64
        || !serial.bytes().all(|b| b.is_ascii_digit())
        || serial.bytes().all(|b| b == b'0')
    {
        return None;
    }
    let (path, key) = match metadata.object.dataset {
        Dataset::NationalStatute => ("lsInfoP.do", "lsiSeq"),
        Dataset::AdministrativeRule => ("admRulInfoP.do", "admRulSeq"),
        Dataset::Treaty => ("trtyInfoP.do", "trtySeq"),
        Dataset::Precedent => ("precInfoP.do", "precSeq"),
        Dataset::ConstitutionalDecision => ("detcInfoP.do", "detcSeq"),
        Dataset::LegalInterpretation => ("expcInfoP.do", "expcSeq"),
        Dataset::AdministrativeAppeal => ("deccInfoP.do", "deccSeq"),
        _ => return None,
    };
    let mut url = url::Url::parse(&format!("https://www.law.go.kr/LSW/{path}")).ok()?;
    url.query_pairs_mut().append_pair(key, serial);
    match metadata.object.dataset {
        Dataset::NationalStatute => {
            let date = metadata.effective_date.as_deref()?;
            if !crate::legal::valid_date(date) || metadata.revision_id != format!("{serial}:{date}")
            {
                return None;
            }
            url.query_pairs_mut()
                .append_pair("efYd", date)
                .append_pair("urlMode", "lsInfoP")
                .append_pair("chrClsCd", "010201");
        }
        Dataset::AdministrativeRule => {
            url.query_pairs_mut().append_pair("chrClsCd", "010201");
        }
        Dataset::Treaty => {
            url.query_pairs_mut()
                .append_pair("chrClsCd", "010202")
                .append_pair("mode", "4");
        }
        _ => {}
    }
    Some(url.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn id() -> CitationId {
        CitationId {
            object: ObjectId {
                jurisdiction: "kr".into(),
                provider: "fictional".into(),
                dataset: Dataset::Precedent,
                id: "0001".into(),
            },
            capture_id: "a".repeat(64),
            projection: CitationProjection::Section {
                section: "../조문 /?#+%".into(),
            },
        }
    }
    #[test]
    fn identity_round_trip_is_canonical_and_safe() {
        let expected = id();
        let encoded = expected.encode().unwrap();
        assert_eq!(CitationId::decode(&encoded).unwrap(), expected);
        let url = expected
            .reference_url("https://references.example.test")
            .unwrap();
        assert!(url.contains("/section/s-"));
        assert!(!url.contains('?'));
        assert!(!url.contains('#'));
        assert!(CitationId::decode(&encoded.replace("%2F", "%2f")).is_err());
        assert!(CitationId::decode(&encoded.replace("0001", "1")).unwrap() != expected);
    }

    #[test]
    fn all_supported_datasets_round_trip_every_citation_projection() {
        for dataset in Dataset::ALL {
            for projection in [
                CitationProjection::Document,
                CitationProjection::Metadata,
                CitationProjection::Section {
                    section: "article:0001000:source_ordinal:2".into(),
                },
                CitationProjection::Passage {
                    section: "본문 /~".into(),
                    start: 0,
                    end: 12,
                },
            ] {
                let mut expected = id();
                expected.object.dataset = *dataset;
                expected.projection = projection;
                let encoded = expected.encode().unwrap();
                assert_eq!(
                    CitationId::decode(&encoded).unwrap(),
                    expected,
                    "{dataset:?}"
                );
                assert_eq!(
                    expected.resource_uri().unwrap(),
                    format!("openlegal://source/{encoded}")
                );
            }
        }
    }

    #[test]
    fn acr_decision_title_citation_uses_the_registered_dataset() {
        let encoded = format!(
            "v1/kr/law_go_kr/acr_decision/2097/{}/section/s-title",
            "a".repeat(64)
        );
        let parsed = CitationId::decode(&encoded).unwrap();
        assert_eq!(parsed.object.dataset, Dataset::AcrDecision);
        assert_eq!(parsed.object.id, "2097");
        assert_eq!(
            parsed.projection,
            CitationProjection::Section {
                section: "title".into()
            }
        );
        assert_eq!(parsed.encode().unwrap(), encoded);
    }
    #[test]
    fn rejects_noncanonical_ranges_and_origin_inputs() {
        let mut value = id();
        value.projection = CitationProjection::Passage {
            section: "body".into(),
            start: 0,
            end: 8192,
        };
        let encoded = value.encode().unwrap();
        assert!(CitationId::decode(&encoded.replace("/0/", "/00/")).is_err());
        value.projection = CitationProjection::Passage {
            section: "body".into(),
            start: 0,
            end: 8193,
        };
        assert!(value.encode().is_err());
        for base in [
            "http://example.test",
            "https://user@example.test",
            "https://example.test/path",
            "https://example.test?token=x",
        ] {
            assert!(reference_base(base).is_err());
        }
    }
    fn metadata(dataset: Dataset) -> MetadataResult {
        MetadataResult {
            object: ObjectId {
                jurisdiction: "kr".into(),
                provider: "law_go_kr".into(),
                dataset,
                id: "0001".into(),
            },
            revision_id: "123:20260101".into(),
            capture_id: "a".repeat(64),
            title: "Synthetic route fixture".into(),
            metadata: BTreeMap::from([("provider_record_number".into(), "123".into())]),
            publication_date: None,
            effective_date: Some("20260101".into()),
            source_url: "https://example.test/synthetic".into(),
            retrieved_at: 1,
            captured_at: 2,
            validated_at: 2,
            processor_version: "fixture".into(),
            raw_sha256: "b".repeat(64),
            freshness: None,
            collection_notices: Vec::new(),
        }
    }
    #[test]
    fn official_routes_use_selected_serial_without_claiming_capture_immutability() {
        for (dataset, path, key) in [
            (Dataset::NationalStatute, "lsInfoP.do", "lsiSeq"),
            (Dataset::AdministrativeRule, "admRulInfoP.do", "admRulSeq"),
            (Dataset::Treaty, "trtyInfoP.do", "trtySeq"),
            (Dataset::Precedent, "precInfoP.do", "precSeq"),
            (Dataset::ConstitutionalDecision, "detcInfoP.do", "detcSeq"),
            (Dataset::LegalInterpretation, "expcInfoP.do", "expcSeq"),
            (Dataset::AdministrativeAppeal, "deccInfoP.do", "deccSeq"),
        ] {
            let value = metadata(dataset);
            let link = url::Url::parse(&official_browser_url(&value).unwrap()).unwrap();
            assert_eq!(link.path(), format!("/LSW/{path}"));
            assert!(link.query_pairs().any(|(k, v)| k == key && v == "123"));
            assert!(!link.as_str().contains("0001"));
            assert!(!link.as_str().contains(&value.capture_id));
        }
        assert!(official_browser_url(&metadata(Dataset::Ordinance)).is_none());
    }
    #[test]
    fn unresolved_or_conflicting_source_identity_never_gets_a_guessed_link() {
        let mut value = metadata(Dataset::NationalStatute);
        value.effective_date = Some("20260102".into());
        assert!(official_browser_url(&value).is_none());
        value = metadata(Dataset::Precedent);
        for serial in ["", "0", "123&OC=secret", "-1", "123 456"] {
            value
                .metadata
                .insert("provider_record_number".into(), serial.into());
            assert!(official_browser_url(&value).is_none());
        }
        value = metadata(Dataset::Precedent);
        value.object.provider = "other".into();
        assert!(official_browser_url(&value).is_none());
    }
}
