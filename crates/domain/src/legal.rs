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
    EnglishStatute,
    SchoolRule,
    LocalPublicCorporationRule,
    PublicInstitutionRule,
    LegalTerm,
    PpcDecision,
    EiacDecision,
    FtcDecision,
    AcrDecision,
    FscDecision,
    NlrcDecision,
    KccDecision,
    IaciacDecision,
    OcltDecision,
    EccDecision,
    SfcDecision,
    NhrckDecision,
    MoelInterpretation,
    MolitInterpretation,
    MoefInterpretation,
    MofInterpretation,
    MoisInterpretation,
    MeInterpretation,
    KcsInterpretation,
    NtsInterpretation,
    MoeInterpretation,
    MsitInterpretation,
    MpvaInterpretation,
    MndInterpretation,
    MafraInterpretation,
    McstInterpretation,
    MojInterpretation,
    MohwInterpretation,
    MotieInterpretation,
    MogefInterpretation,
    MofaInterpretation,
    MssInterpretation,
    MouInterpretation,
    MolegInterpretation,
    MfdsInterpretation,
    MpmInterpretation,
    KmaInterpretation,
    KhsInterpretation,
    RdaInterpretation,
    NpaInterpretation,
    DapaInterpretation,
    MmaInterpretation,
    KfsInterpretation,
    NfaInterpretation,
    OkaInterpretation,
    PpsInterpretation,
    KdcaInterpretation,
    KostatInterpretation,
    KipoInterpretation,
    KcgInterpretation,
    NaaccInterpretation,
    TtSpecialAppeal,
    KmstSpecialAppeal,
    AcrSpecialAppeal,
    AdapSpecialAppeal,
    AuditConsultation,
}
impl Dataset {
    /// Closed provider-independent data families; caller input cannot create routes.
    pub const ALL: &'static [Self] = &[
        Self::NationalStatute,
        Self::AdministrativeRule,
        Self::Ordinance,
        Self::Treaty,
        Self::Precedent,
        Self::ConstitutionalDecision,
        Self::LegalInterpretation,
        Self::AdministrativeAppeal,
        Self::EnglishStatute,
        Self::SchoolRule,
        Self::LocalPublicCorporationRule,
        Self::PublicInstitutionRule,
        Self::LegalTerm,
        Self::PpcDecision,
        Self::EiacDecision,
        Self::FtcDecision,
        Self::AcrDecision,
        Self::FscDecision,
        Self::NlrcDecision,
        Self::KccDecision,
        Self::IaciacDecision,
        Self::OcltDecision,
        Self::EccDecision,
        Self::SfcDecision,
        Self::NhrckDecision,
        Self::MoelInterpretation,
        Self::MolitInterpretation,
        Self::MoefInterpretation,
        Self::MofInterpretation,
        Self::MoisInterpretation,
        Self::MeInterpretation,
        Self::KcsInterpretation,
        Self::NtsInterpretation,
        Self::MoeInterpretation,
        Self::MsitInterpretation,
        Self::MpvaInterpretation,
        Self::MndInterpretation,
        Self::MafraInterpretation,
        Self::McstInterpretation,
        Self::MojInterpretation,
        Self::MohwInterpretation,
        Self::MotieInterpretation,
        Self::MogefInterpretation,
        Self::MofaInterpretation,
        Self::MssInterpretation,
        Self::MouInterpretation,
        Self::MolegInterpretation,
        Self::MfdsInterpretation,
        Self::MpmInterpretation,
        Self::KmaInterpretation,
        Self::KhsInterpretation,
        Self::RdaInterpretation,
        Self::NpaInterpretation,
        Self::DapaInterpretation,
        Self::MmaInterpretation,
        Self::KfsInterpretation,
        Self::NfaInterpretation,
        Self::OkaInterpretation,
        Self::PpsInterpretation,
        Self::KdcaInterpretation,
        Self::KostatInterpretation,
        Self::KipoInterpretation,
        Self::KcgInterpretation,
        Self::NaaccInterpretation,
        Self::TtSpecialAppeal,
        Self::KmstSpecialAppeal,
        Self::AcrSpecialAppeal,
        Self::AdapSpecialAppeal,
        Self::AuditConsultation,
    ];
    pub fn as_str(self) -> &'static str {
        match self {
            Self::NationalStatute => "national_statute",
            Self::AdministrativeRule => "administrative_rule",
            Self::Ordinance => "ordinance",
            Self::Treaty => "treaty",
            Self::Precedent => "precedent",
            Self::ConstitutionalDecision => "constitutional_decision",
            Self::LegalInterpretation => "legal_interpretation",
            Self::AdministrativeAppeal => "administrative_appeal",
            Self::EnglishStatute => "english_statute",
            Self::SchoolRule => "school_rule",
            Self::LocalPublicCorporationRule => "local_public_corporation_rule",
            Self::PublicInstitutionRule => "public_institution_rule",
            Self::LegalTerm => "legal_term",
            Self::PpcDecision => "ppc_decision",
            Self::EiacDecision => "eiac_decision",
            Self::FtcDecision => "ftc_decision",
            Self::AcrDecision => "acr_decision",
            Self::FscDecision => "fsc_decision",
            Self::NlrcDecision => "nlrc_decision",
            Self::KccDecision => "kcc_decision",
            Self::IaciacDecision => "iaciac_decision",
            Self::OcltDecision => "oclt_decision",
            Self::EccDecision => "ecc_decision",
            Self::SfcDecision => "sfc_decision",
            Self::NhrckDecision => "nhrck_decision",
            Self::MoelInterpretation => "moel_interpretation",
            Self::MolitInterpretation => "molit_interpretation",
            Self::MoefInterpretation => "moef_interpretation",
            Self::MofInterpretation => "mof_interpretation",
            Self::MoisInterpretation => "mois_interpretation",
            Self::MeInterpretation => "me_interpretation",
            Self::KcsInterpretation => "kcs_interpretation",
            Self::NtsInterpretation => "nts_interpretation",
            Self::MoeInterpretation => "moe_interpretation",
            Self::MsitInterpretation => "msit_interpretation",
            Self::MpvaInterpretation => "mpva_interpretation",
            Self::MndInterpretation => "mnd_interpretation",
            Self::MafraInterpretation => "mafra_interpretation",
            Self::McstInterpretation => "mcst_interpretation",
            Self::MojInterpretation => "moj_interpretation",
            Self::MohwInterpretation => "mohw_interpretation",
            Self::MotieInterpretation => "motie_interpretation",
            Self::MogefInterpretation => "mogef_interpretation",
            Self::MofaInterpretation => "mofa_interpretation",
            Self::MssInterpretation => "mss_interpretation",
            Self::MouInterpretation => "mou_interpretation",
            Self::MolegInterpretation => "moleg_interpretation",
            Self::MfdsInterpretation => "mfds_interpretation",
            Self::MpmInterpretation => "mpm_interpretation",
            Self::KmaInterpretation => "kma_interpretation",
            Self::KhsInterpretation => "khs_interpretation",
            Self::RdaInterpretation => "rda_interpretation",
            Self::NpaInterpretation => "npa_interpretation",
            Self::DapaInterpretation => "dapa_interpretation",
            Self::MmaInterpretation => "mma_interpretation",
            Self::KfsInterpretation => "kfs_interpretation",
            Self::NfaInterpretation => "nfa_interpretation",
            Self::OkaInterpretation => "oka_interpretation",
            Self::PpsInterpretation => "pps_interpretation",
            Self::KdcaInterpretation => "kdca_interpretation",
            Self::KostatInterpretation => "kostat_interpretation",
            Self::KipoInterpretation => "kipo_interpretation",
            Self::KcgInterpretation => "kcg_interpretation",
            Self::NaaccInterpretation => "naacc_interpretation",
            Self::TtSpecialAppeal => "tt_special_appeal",
            Self::KmstSpecialAppeal => "kmst_special_appeal",
            Self::AcrSpecialAppeal => "acr_special_appeal",
            Self::AdapSpecialAppeal => "adap_special_appeal",
            Self::AuditConsultation => "audit_consultation",
        }
    }
    pub fn from_name(name: &str) -> Option<Self> {
        Self::ALL
            .iter()
            .copied()
            .find(|dataset| dataset.as_str() == name)
    }
    pub fn has_provider_revisions(self) -> bool {
        matches!(
            self,
            Self::NationalStatute
                | Self::AdministrativeRule
                | Self::Ordinance
                | Self::EnglishStatute
                | Self::SchoolRule
                | Self::LocalPublicCorporationRule
                | Self::PublicInstitutionRule
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
        if self.metadata.contains_key("original_resources") {
            let resources: Vec<crate::rights::OriginalResource> =
                serde_json::from_str(&self.metadata["original_resources"])
                    .map_err(|_| DatabaseError::InvalidInput)?;
            if resources.len() > 65
                || resources
                    .iter()
                    .map(|r| r.ordinal)
                    .collect::<std::collections::BTreeSet<_>>()
                    .len()
                    != resources.len()
            {
                return Err(DatabaseError::InvalidInput);
            }
            for resource in &resources {
                if !crate::rights::public_source_url(&resource.source_url)
                    || (!resource.rights.evidence_url.is_empty()
                        && !crate::rights::public_source_url(&resource.rights.evidence_url))
                {
                    return Err(DatabaseError::InvalidInput);
                }
                if !resource.rights.can_process() {
                    if resource.ordinal == 0 && (!self.body.is_empty() || !self.sections.is_empty())
                    {
                        return Err(DatabaseError::InvalidInput);
                    }
                    let prefix = format!("attachment:{}:", resource.ordinal);
                    if resource.ordinal > 0
                        && self.sections.iter().any(|s| s.id.starts_with(&prefix))
                    {
                        return Err(DatabaseError::InvalidInput);
                    }
                }
            }
        }
        // Bound the complete decoded record, including repeated titles. This
        // also bounds escaped JSON retained by the database driver on reads.
        let aggregate = self
            .title
            .len()
            .saturating_add(self.body.len())
            .saturating_add(
                self.metadata
                    .iter()
                    .map(|(k, v)| k.len() + v.len())
                    .sum::<usize>(),
            )
            .saturating_add(
                self.sections
                    .iter()
                    .map(|section| section.id.len() + section.title.len() + section.text.len())
                    .sum::<usize>(),
            );
        if aggregate > 64 * 1024 * 1024 {
            return Err(DatabaseError::InvalidInput);
        }
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
    #[serde(default)]
    pub collection_notices: Vec<CollectionNotice>,
}
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct CollectionNotice {
    pub dataset: Dataset,
    pub scope: String,
    pub code: String,
    pub affected_count: u64,
    pub last_seen_at: u64,
    pub retry_at: u64,
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
            collection_notices: Vec::new(),
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

/// Local collection state; unobserved never asserts absence from the provider.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ObjectCollectionState {
    NotObserved,
    ProcessingPending,
    CollectionIncomplete,
    Published,
    Withdrawn,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ObjectCompletionEta {
    Unknown {
        reason: String,
    },
    Range {
        earliest_at: u64,
        latest_at: u64,
        sample_size: u32,
    },
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct ObjectJobStatus {
    pub id: String,
    pub status: String,
    pub attempts: u32,
    pub created_at: u64,
    pub started_at: Option<u64>,
    pub completed_at: Option<u64>,
    pub error_category: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct ObjectStatus {
    pub schema_version: u32,
    pub object: ObjectId,
    pub state: ObjectCollectionState,
    pub head_capture_id: Option<String>,
    /// None when no published HEAD can be compared with the index watermark.
    pub indexed: Option<bool>,
    pub job: Option<ObjectJobStatus>,
    pub retry_at: Option<u64>,
    pub eta: ObjectCompletionEta,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DatabaseError {
    SourceRejected,
    /// A known provider response for one item/page is unavailable (404/410).
    SourceUnavailable,
    /// Downloaded provider bytes failed bounded format or content validation.
    SourceDataInvalid,
    /// A bounded provider download failed at DNS, TLS, transport, timeout, or selected HTTP status.
    SourceDownloadFailed,
    /// A provider authentication/authorization response requires operator review.
    SourceUnauthorized,
    /// A completed provider response indicates a temporary server failure.
    SourceTransient,
    InvalidInput,
    InvalidFieldShorthand,
    InvalidRegex,
    NotFound,
    /// The object has no retained provider observation in this corpus.
    NotObserved,
    /// Provider observation exists, but no publishable current HEAD is available.
    CollectionIncomplete,
    RevisionUnavailable,
    AmbiguousRevision,
    AmbiguousCollection,
    SourceInventoryIncomplete,
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
    fn closed_dataset_names_preserve_existing_wire_values() {
        for (dataset, name) in [
            (Dataset::NationalStatute, "national_statute"),
            (Dataset::AdministrativeRule, "administrative_rule"),
            (Dataset::Ordinance, "ordinance"),
            (Dataset::Treaty, "treaty"),
            (Dataset::Precedent, "precedent"),
            (Dataset::ConstitutionalDecision, "constitutional_decision"),
            (Dataset::LegalInterpretation, "legal_interpretation"),
            (Dataset::AdministrativeAppeal, "administrative_appeal"),
        ] {
            assert_eq!(dataset.as_str(), name);
            assert_eq!(Dataset::from_name(name), Some(dataset));
        }
        for dataset in Dataset::ALL {
            let json = serde_json::to_value(dataset).unwrap();
            assert_eq!(json.as_str(), Some(dataset.as_str()));
        }
        assert_eq!(Dataset::from_name("arbitrary_provider_target"), None);
    }
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
    fn repeated_titles_count_toward_total_decoded_record_bound() {
        let mut record = LegalRecord {
            object: ObjectId {
                jurisdiction: "kr".into(),
                provider: "fictional".into(),
                dataset: Dataset::NationalStatute,
                id: "1".into(),
            },
            revision_id: "1".into(),
            title: "Fictional".into(),
            body: String::new(),
            sections: Vec::new(),
            metadata: BTreeMap::new(),
            publication_date: None,
            effective_date: None,
            source_url: "https://example.test/fictional".into(),
            representation: "provider".into(),
        };
        // Each section is individually small; repeated titles exceed the aggregate cap.
        for i in 0..8192 {
            record.sections.push(LegalSection {
                id: format!("article:{i}"),
                title: "x".repeat(8192),
                text: "text".into(),
                kind: SectionKind::ProviderText,
                source_document_sha256: None,
                page: None,
            });
        }
        assert_eq!(record.validate(), Err(DatabaseError::InvalidInput));
        record.sections.truncate(8000);
        assert_eq!(record.validate(), Ok(()));
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
