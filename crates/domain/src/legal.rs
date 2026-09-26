//! Provider-owned legal identities, revisions and separately retained observations.
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, fmt};

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq, Hash, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Dataset {
    NationalStatute,
    AdministrativeRule,
    Ordinance,
    Treaty,
    Precedent,
    ConstitutionalDecision,
    LegalInterpretation,
    AdministrativeAppeal,
}
impl Dataset {
    pub fn has_provider_revisions(self) -> bool {
        matches!(
            self,
            Self::NationalStatute | Self::AdministrativeRule | Self::Ordinance
        )
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Hash, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ObjectId {
    #[serde(default = "default_jurisdiction")]
    pub jurisdiction: String,
    pub provider: String,
    pub dataset: Dataset,
    pub id: String,
}
fn default_jurisdiction() -> String {
    "kr".into()
}
impl ObjectId {
    pub fn validate(&self) -> Result<(), DatabaseError> {
        if !crate::valid_identifier(&self.jurisdiction, 32)
            || !crate::valid_identifier(&self.provider, 64)
            || !crate::valid_identifier(&self.id, 128)
        {
            return Err(DatabaseError::InvalidInput);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum RevisionSelector {
    #[default]
    Head,
    Revision {
        id: String,
    },
    PublicationDate {
        date: String,
    },
    EffectiveDate {
        date: String,
    },
    Capture {
        id: String,
    },
}
impl RevisionSelector {
    pub fn validate(&self) -> Result<(), DatabaseError> {
        let valid = match self {
            Self::Head => true,
            Self::Revision { id } => {
                !id.is_empty() && id.len() <= 256 && !id.chars().any(char::is_control)
            }
            Self::Capture { id } => crate::history::valid_snapshot_id(id),
            Self::PublicationDate { date } | Self::EffectiveDate { date } => valid_date(date),
        };
        if valid {
            Ok(())
        } else {
            Err(DatabaseError::InvalidInput)
        }
    }
}
/// Exact Gregorian date only; no date-only value is converted to an instant.
pub fn valid_date(value: &str) -> bool {
    if value.len() != 8 || !value.bytes().all(|b| b.is_ascii_digit()) {
        return false;
    }
    let Ok(year) = value[..4].parse::<u32>() else {
        return false;
    };
    let Ok(month) = value[4..6].parse::<u32>() else {
        return false;
    };
    let Ok(day) = value[6..].parse::<u32>() else {
        return false;
    };
    let max = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) => 29,
        2 => 28,
        _ => 0,
    };
    year > 0 && day > 0 && day <= max
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GetRequest {
    pub object: ObjectId,
    #[serde(default)]
    pub selector: RevisionSelector,
    #[serde(default)]
    pub fresh_only: bool,
}

/// Text is a named derived projection. Original evidence and technical provenance
/// remain attached to each capture; metadata never establishes guessed legal facts.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct LegalRecord {
    pub object: ObjectId,
    pub revision_id: String,
    pub title: String,
    pub body: String,
    pub metadata: BTreeMap<String, String>,
    pub publication_date: Option<String>,
    pub effective_date: Option<String>,
    pub source_url: String,
    pub representation: String,
    #[serde(default)]
    pub sections: Vec<LegalSection>,
}
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SectionKind {
    ProviderText,
    Extracted,
    Ocr,
}
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, JsonSchema)]
pub struct LegalSection {
    pub id: String,
    pub title: String,
    pub text: String,
    pub kind: SectionKind,
    pub source_document_sha256: Option<String>,
    pub page: Option<u32>,
}
impl LegalRecord {
    pub fn validate(&self) -> Result<(), DatabaseError> {
        self.object.validate()?;
        RevisionSelector::Revision {
            id: self.revision_id.clone(),
        }
        .validate()?;
        let url = url::Url::parse(&self.source_url).map_err(|_| DatabaseError::InvalidInput)?;
        if url.scheme() != "https"
            || url.host_str().is_none()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query_pairs().any(|(k, _)| {
                [
                    "oc",
                    "key",
                    "apikey",
                    "api_key",
                    "token",
                    "servicekey",
                    "password",
                ]
                .contains(&k.to_ascii_lowercase().as_str())
            })
            || self
                .sections
                .iter()
                .map(|s| &s.id)
                .collect::<std::collections::BTreeSet<_>>()
                .len()
                != self.sections.len()
            || self.sections.len() > 10000
            || self.sections.iter().any(|s| {
                matches!(s.id.as_str(), "body" | "title")
                    || s.id.is_empty()
                    || s.id.len() > 256
                    || s.title.len() > 16384
                    || s.text.len() > 16 * 1024 * 1024
                    || s.source_document_sha256
                        .as_ref()
                        .is_some_and(|d| !crate::history::valid_snapshot_id(d))
            })
            || self.sections.iter().map(|s| s.text.len()).sum::<usize>() > 64 * 1024 * 1024
        {
            return Err(DatabaseError::InvalidInput);
        }
        if self.title.len() > 16384
            || self.body.len() > 16 * 1024 * 1024
            || self.source_url.len() > 2048
            || !self.source_url.starts_with("https://")
            || self.representation.is_empty()
            || self.representation.len() > 128
            || self.metadata.len() > 128
            || self
                .metadata
                .iter()
                .any(|(k, v)| k.len() > 128 || v.len() > 65536)
            || self
                .metadata
                .iter()
                .map(|(k, v)| k.len() + v.len())
                .sum::<usize>()
                > 65536
            || self
                .publication_date
                .as_ref()
                .is_some_and(|d| !valid_date(d))
            || self.effective_date.as_ref().is_some_and(|d| !valid_date(d))
        {
            return Err(DatabaseError::InvalidInput);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct Capture {
    pub capture_id: String,
    pub sequence: u64,
    pub record: LegalRecord,
    pub retrieved_at: u64,
    pub captured_at: u64,
    pub validated_at: u64,
    pub processor_version: String,
    pub raw_sha256: String,
}
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct HeadFreshness {
    pub state: crate::FreshnessState,
    pub served_at: u64,
    pub cached_at: u64,
    pub age_seconds: u64,
    pub fresh_ttl_seconds: u64,
    pub fresh_remaining_seconds: u64,
    pub stale_remaining_seconds: u64,
    pub fresh_expires_at: u64,
    pub stale_expires_at: u64,
}
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct GetResult {
    pub capture: Capture,
    pub freshness: Option<HeadFreshness>,
}
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct MetadataResult {
    pub object: ObjectId,
    pub revision_id: String,
    pub capture_id: String,
    pub title: String,
    pub metadata: BTreeMap<String, String>,
    pub publication_date: Option<String>,
    pub effective_date: Option<String>,
    pub source_url: String,
    pub retrieved_at: u64,
    pub captured_at: u64,
    pub validated_at: u64,
    pub processor_version: String,
    pub raw_sha256: String,
    pub freshness: Option<HeadFreshness>,
}
impl From<GetResult> for MetadataResult {
    fn from(v: GetResult) -> Self {
        let c = v.capture;
        let r = c.record;
        Self {
            object: r.object,
            revision_id: r.revision_id,
            capture_id: c.capture_id,
            title: r.title,
            metadata: r.metadata,
            publication_date: r.publication_date,
            effective_date: r.effective_date,
            source_url: r.source_url,
            retrieved_at: c.retrieved_at,
            captured_at: c.captured_at,
            validated_at: c.validated_at,
            processor_version: c.processor_version,
            raw_sha256: c.raw_sha256,
            freshness: v.freshness,
        }
    }
}
#[derive(Clone, Copy, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum HistoryKind {
    Revisions,
    Captures,
}
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct HistoryEntry {
    pub revision_id: String,
    pub capture_id: Option<String>,
    pub sequence: u64,
    pub captured_at: Option<u64>,
    pub publication_date: Option<String>,
    pub effective_date: Option<String>,
}
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct HistoryPage {
    pub entries: Vec<HistoryEntry>,
    pub next_cursor: Option<String>,
    pub inventory_complete: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DatabaseError {
    SourceRejected,
    InvalidInput,
    NotFound,
    RevisionUnavailable,
    AmbiguousRevision,
    HistoryIncomplete,
    UnsupportedHistory,
    ProcessingPending,
    FreshnessUnavailable,
    Withdrawn,
    StorageUnavailable,
    StorageCorrupt,
    Capacity,
    BudgetExhausted,
    Conflict,
    Cancelled,
    SessionExpired,
    SnapshotInvalidated,
}
impl fmt::Display for DatabaseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "database operation: {self:?}")
    }
}
impl std::error::Error for DatabaseError {}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn validates_dates_without_inventing_instants() {
        assert!(valid_date("20240229"));
        for d in ["20230229", "20241301", "00000101", "20240100", "2024-01-01"] {
            assert!(!valid_date(d));
        }
    }
    #[test]
    fn reserved_and_duplicate_section_identifiers_are_rejected() {
        let mut r = LegalRecord {
            object: ObjectId {
                jurisdiction: "kr".into(),
                provider: "fictional".into(),
                dataset: Dataset::NationalStatute,
                id: "1".into(),
            },
            revision_id: "1".into(),
            title: "Fictional".into(),
            body: "body".into(),
            sections: vec![],
            metadata: BTreeMap::new(),
            publication_date: None,
            effective_date: None,
            source_url: "https://example.test/fictional".into(),
            representation: "provider".into(),
        };
        let section = LegalSection {
            id: "body".into(),
            title: String::new(),
            text: "text".into(),
            kind: SectionKind::ProviderText,
            source_document_sha256: None,
            page: None,
        };
        r.sections.push(section);
        assert!(r.validate().is_err());
        r.sections[0].id = "article:1".into();
        assert!(r.validate().is_ok());
        r.sections.push(r.sections[0].clone());
        assert!(r.validate().is_err());
    }
    #[test]
    fn namespaces_and_selectors_are_checked() {
        assert!(
            ObjectId {
                jurisdiction: "kr".into(),
                provider: "law_go_kr".into(),
                dataset: Dataset::Ordinance,
                id: "001".into()
            }
            .validate()
            .is_ok()
        );
        assert!(
            RevisionSelector::Capture { id: "001".into() }
                .validate()
                .is_err()
        );
    }
}
