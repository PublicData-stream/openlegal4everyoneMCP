//! Finite supplemental LAW views, seeded only by observed identifiers or terms.
//! This module never expands an arbitrary search space or follows an input URL.
use super::{
    InventoryItem, LawClient, catalog, first, identity_rows, local, numeric_id, revision_parts,
    text,
};
use openlegal_application::document::{DocumentFormat, DocumentInput, DocumentNode};
use openlegal_domain::legal::{DatabaseError, Dataset, ObjectId, valid_date};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use tokio_util::sync::CancellationToken;
use url::Url;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SupplementSource {
    StatuteAbbreviations,
    StatuteAnnexInventory,
    AdministrativeRuleAnnexInventory,
    OrdinanceAnnexInventory,
    LegalKnowledgeTermInventory,
    EverydayTermInventory,
    StatuteOrdinanceInventory,
    OrdinanceStatuteInventory,
    StatuteHierarchyInventory,
    StatuteComparisonInventory,
    StatuteThreeColumnInventory,
    StatuteOverviewInventory,
    AdministrativeRuleComparisonInventory,
    StatuteOrdinanceStatus,
    StatuteChangeInventory,
    ProvisionChangeInventory,
    StatuteDelegation,
    StatuteHierarchy,
    StatuteComparison,
    StatuteReferencedComparison,
    StatuteDelegatedComparison,
    StatuteOverview,
    AdministrativeRuleComparison,
    ProvisionHistory,
    LegalTermEverydayRelations,
    EverydayTermLegalRelations,
    LegalTermProvisionRelations,
    ProvisionLegalTermRelations,
    RelatedStatutes,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SeedKind {
    Global,
    NationalRecord,
    AdministrativeRuleRecord,
    Provision,
    LegalTerm,
    EverydayTerm,
    ChangeDay,
    ChangeWindow,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Pagination {
    Singleton,
    Numbered,
    ProviderDefault,
}
#[derive(Clone, Copy, Debug)]
pub struct SupplementContract {
    pub source: SupplementSource,
    pub guide: &'static str,
    pub seed_kind: SeedKind,
    pub pagination: Pagination,
    pub row_identity_fields: &'static [&'static str],
    pub metadata_only: bool,
    pub reason: &'static str,
}
#[rustfmt::skip]
pub const SUPPLEMENT_CONTRACTS: &[SupplementContract] = &[
    SupplementContract { source: SupplementSource::StatuteAbbreviations, guide: "lsAbrvListGuide", seed_kind: SeedKind::Global, pagination: Pagination::Singleton, row_identity_fields: &["법령ID"], metadata_only: false, reason: "documented_legal_information_or_inventory_metadata" },
    SupplementContract { source: SupplementSource::StatuteAnnexInventory, guide: "lsBylListGuide", seed_kind: SeedKind::Global, pagination: Pagination::Numbered, row_identity_fields: &["별표일련번호"], metadata_only: false, reason: "documented_legal_information_or_inventory_metadata" },
    SupplementContract { source: SupplementSource::AdministrativeRuleAnnexInventory, guide: "admrulBylListGuide", seed_kind: SeedKind::Global, pagination: Pagination::Numbered, row_identity_fields: &["별표일련번호"], metadata_only: false, reason: "documented_legal_information_or_inventory_metadata" },
    SupplementContract { source: SupplementSource::OrdinanceAnnexInventory, guide: "ordinBylListGuide", seed_kind: SeedKind::Global, pagination: Pagination::Numbered, row_identity_fields: &["별표일련번호"], metadata_only: false, reason: "documented_legal_information_or_inventory_metadata" },
    SupplementContract { source: SupplementSource::LegalKnowledgeTermInventory, guide: "lstrmAIGuide", seed_kind: SeedKind::Global, pagination: Pagination::Numbered, row_identity_fields: &["법령용어명"], metadata_only: false, reason: "documented_legal_information_or_inventory_metadata" },
    SupplementContract { source: SupplementSource::EverydayTermInventory, guide: "dlytrmGuide", seed_kind: SeedKind::Global, pagination: Pagination::Numbered, row_identity_fields: &["일상용어명"], metadata_only: false, reason: "documented_legal_information_or_inventory_metadata" },
    SupplementContract { source: SupplementSource::StatuteOrdinanceInventory, guide: "lsOrdinConListGuide", seed_kind: SeedKind::Global, pagination: Pagination::Numbered, row_identity_fields: &["법령ID"], metadata_only: false, reason: "documented_legal_information_or_inventory_metadata" },
    SupplementContract { source: SupplementSource::OrdinanceStatuteInventory, guide: "ordinLsConListGuide", seed_kind: SeedKind::Global, pagination: Pagination::Numbered, row_identity_fields: &["자치법규ID"], metadata_only: false, reason: "documented_legal_information_or_inventory_metadata" },
    SupplementContract { source: SupplementSource::StatuteHierarchyInventory, guide: "lsStmdListGuide", seed_kind: SeedKind::Global, pagination: Pagination::Numbered, row_identity_fields: &["법령ID"], metadata_only: false, reason: "documented_legal_information_or_inventory_metadata" },
    SupplementContract { source: SupplementSource::StatuteComparisonInventory, guide: "oldAndNewListGuide", seed_kind: SeedKind::Global, pagination: Pagination::Numbered, row_identity_fields: &["신구법ID"], metadata_only: false, reason: "documented_legal_information_or_inventory_metadata" },
    SupplementContract { source: SupplementSource::StatuteThreeColumnInventory, guide: "thdCmpListGuide", seed_kind: SeedKind::Global, pagination: Pagination::Numbered, row_identity_fields: &["법령ID"], metadata_only: false, reason: "documented_legal_information_or_inventory_metadata" },
    SupplementContract { source: SupplementSource::StatuteOverviewInventory, guide: "oneViewListGuide", seed_kind: SeedKind::Global, pagination: Pagination::Numbered, row_identity_fields: &["법령일련번호"], metadata_only: false, reason: "documented_legal_information_or_inventory_metadata" },
    SupplementContract { source: SupplementSource::AdministrativeRuleComparisonInventory, guide: "admrulOldAndNewListGuide", seed_kind: SeedKind::Global, pagination: Pagination::Numbered, row_identity_fields: &["신구법ID"], metadata_only: false, reason: "documented_legal_information_or_inventory_metadata" },
    SupplementContract { source: SupplementSource::StatuteOrdinanceStatus, guide: "lsOrdinConGuide", seed_kind: SeedKind::Global, pagination: Pagination::Singleton, row_identity_fields: &[], metadata_only: false, reason: "documented_legal_information_or_inventory_metadata" },
    SupplementContract { source: SupplementSource::StatuteChangeInventory, guide: "lsChgListGuide", seed_kind: SeedKind::ChangeDay, pagination: Pagination::Numbered, row_identity_fields: &["법령ID"], metadata_only: false, reason: "documented_legal_information_or_inventory_metadata" },
    SupplementContract { source: SupplementSource::ProvisionChangeInventory, guide: "lsDayJoRvsListGuide", seed_kind: SeedKind::ChangeWindow, pagination: Pagination::ProviderDefault, row_identity_fields: &["법령ID"], metadata_only: false, reason: "documented_legal_information_or_inventory_metadata" },
    SupplementContract { source: SupplementSource::StatuteDelegation, guide: "lsDelegated", seed_kind: SeedKind::NationalRecord, pagination: Pagination::Singleton, row_identity_fields: &["법령ID"], metadata_only: false, reason: "documented_legal_information_or_inventory_metadata" },
    SupplementContract { source: SupplementSource::StatuteHierarchy, guide: "lsStmdInfoGuide", seed_kind: SeedKind::NationalRecord, pagination: Pagination::Singleton, row_identity_fields: &["법령ID"], metadata_only: false, reason: "documented_legal_information_or_inventory_metadata" },
    SupplementContract { source: SupplementSource::StatuteComparison, guide: "oldAndNewInfoGuide", seed_kind: SeedKind::NationalRecord, pagination: Pagination::Singleton, row_identity_fields: &["법령ID"], metadata_only: false, reason: "documented_legal_information_or_inventory_metadata" },
    SupplementContract { source: SupplementSource::StatuteReferencedComparison, guide: "thdCmpInfoGuide", seed_kind: SeedKind::NationalRecord, pagination: Pagination::Singleton, row_identity_fields: &["법령ID"], metadata_only: false, reason: "documented_legal_information_or_inventory_metadata" },
    SupplementContract { source: SupplementSource::StatuteDelegatedComparison, guide: "thdCmpInfoGuide", seed_kind: SeedKind::NationalRecord, pagination: Pagination::Singleton, row_identity_fields: &["법령ID"], metadata_only: false, reason: "documented_legal_information_or_inventory_metadata" },
    SupplementContract { source: SupplementSource::StatuteOverview, guide: "oneViewInfoGuide", seed_kind: SeedKind::NationalRecord, pagination: Pagination::Singleton, row_identity_fields: &["법령일련번호"], metadata_only: true, reason: "external_explanatory_content_reuse_unverified" },
    SupplementContract { source: SupplementSource::AdministrativeRuleComparison, guide: "admrulOldAndNewInfoGuide", seed_kind: SeedKind::AdministrativeRuleRecord, pagination: Pagination::Singleton, row_identity_fields: &["행정규칙ID"], metadata_only: false, reason: "documented_legal_information_or_inventory_metadata" },
    SupplementContract { source: SupplementSource::ProvisionHistory, guide: "lsJoChgListGuide", seed_kind: SeedKind::Provision, pagination: Pagination::Numbered, row_identity_fields: &["법령일련번호"], metadata_only: false, reason: "documented_legal_information_or_inventory_metadata" },
    SupplementContract { source: SupplementSource::LegalTermEverydayRelations, guide: "lstrmRltGuide", seed_kind: SeedKind::LegalTerm, pagination: Pagination::Singleton, row_identity_fields: &["일상용어명"], metadata_only: false, reason: "documented_legal_information_or_inventory_metadata" },
    SupplementContract { source: SupplementSource::EverydayTermLegalRelations, guide: "dlytrmRltGuide", seed_kind: SeedKind::EverydayTerm, pagination: Pagination::Singleton, row_identity_fields: &["법령용어명"], metadata_only: false, reason: "documented_legal_information_or_inventory_metadata" },
    SupplementContract { source: SupplementSource::LegalTermProvisionRelations, guide: "lstrmRltJoGuide", seed_kind: SeedKind::LegalTerm, pagination: Pagination::Singleton, row_identity_fields: &["법령명"], metadata_only: false, reason: "documented_legal_information_or_inventory_metadata" },
    SupplementContract { source: SupplementSource::ProvisionLegalTermRelations, guide: "joRltLstrmGuide", seed_kind: SeedKind::Provision, pagination: Pagination::Singleton, row_identity_fields: &["법령용어명"], metadata_only: false, reason: "documented_legal_information_or_inventory_metadata" },
    SupplementContract { source: SupplementSource::RelatedStatutes, guide: "lsRltGuide", seed_kind: SeedKind::NationalRecord, pagination: Pagination::Singleton, row_identity_fields: &["관련법령ID"], metadata_only: false, reason: "documented_legal_information_or_inventory_metadata" },
 ];
impl SupplementSource {
    pub fn contract(self) -> &'static SupplementContract {
        match self {
            Self::StatuteAbbreviations => &SUPPLEMENT_CONTRACTS[0],
            Self::StatuteAnnexInventory => &SUPPLEMENT_CONTRACTS[1],
            Self::AdministrativeRuleAnnexInventory => &SUPPLEMENT_CONTRACTS[2],
            Self::OrdinanceAnnexInventory => &SUPPLEMENT_CONTRACTS[3],
            Self::LegalKnowledgeTermInventory => &SUPPLEMENT_CONTRACTS[4],
            Self::EverydayTermInventory => &SUPPLEMENT_CONTRACTS[5],
            Self::StatuteOrdinanceInventory => &SUPPLEMENT_CONTRACTS[6],
            Self::OrdinanceStatuteInventory => &SUPPLEMENT_CONTRACTS[7],
            Self::StatuteHierarchyInventory => &SUPPLEMENT_CONTRACTS[8],
            Self::StatuteComparisonInventory => &SUPPLEMENT_CONTRACTS[9],
            Self::StatuteThreeColumnInventory => &SUPPLEMENT_CONTRACTS[10],
            Self::StatuteOverviewInventory => &SUPPLEMENT_CONTRACTS[11],
            Self::AdministrativeRuleComparisonInventory => &SUPPLEMENT_CONTRACTS[12],
            Self::StatuteOrdinanceStatus => &SUPPLEMENT_CONTRACTS[13],
            Self::StatuteChangeInventory => &SUPPLEMENT_CONTRACTS[14],
            Self::ProvisionChangeInventory => &SUPPLEMENT_CONTRACTS[15],
            Self::StatuteDelegation => &SUPPLEMENT_CONTRACTS[16],
            Self::StatuteHierarchy => &SUPPLEMENT_CONTRACTS[17],
            Self::StatuteComparison => &SUPPLEMENT_CONTRACTS[18],
            Self::StatuteReferencedComparison => &SUPPLEMENT_CONTRACTS[19],
            Self::StatuteDelegatedComparison => &SUPPLEMENT_CONTRACTS[20],
            Self::StatuteOverview => &SUPPLEMENT_CONTRACTS[21],
            Self::AdministrativeRuleComparison => &SUPPLEMENT_CONTRACTS[22],
            Self::ProvisionHistory => &SUPPLEMENT_CONTRACTS[23],
            Self::LegalTermEverydayRelations => &SUPPLEMENT_CONTRACTS[24],
            Self::EverydayTermLegalRelations => &SUPPLEMENT_CONTRACTS[25],
            Self::LegalTermProvisionRelations => &SUPPLEMENT_CONTRACTS[26],
            Self::ProvisionLegalTermRelations => &SUPPLEMENT_CONTRACTS[27],
            Self::RelatedStatutes => &SUPPLEMENT_CONTRACTS[28],
        }
    }
    pub fn guide(self) -> &'static catalog::GuideEntry {
        // Source contracts are compile-time closed and cross-checked by tests.
        &catalog::GUIDE_ENTRIES[match self {
            Self::StatuteAbbreviations => 22,
            Self::StatuteAnnexInventory => 67,
            Self::AdministrativeRuleAnnexInventory => 68,
            Self::OrdinanceAnnexInventory => 69,
            Self::LegalKnowledgeTermInventory => 100,
            Self::EverydayTermInventory => 101,
            Self::StatuteOrdinanceInventory => 13,
            Self::OrdinanceStatuteInventory => 32,
            Self::StatuteHierarchyInventory => 16,
            Self::StatuteComparisonInventory => 18,
            Self::StatuteThreeColumnInventory => 20,
            Self::StatuteOverviewInventory => 24,
            Self::AdministrativeRuleComparisonInventory => 28,
            Self::StatuteOrdinanceStatus => 14,
            Self::StatuteChangeInventory => 10,
            Self::ProvisionChangeInventory => 11,
            Self::StatuteDelegation => 15,
            Self::StatuteHierarchy => 17,
            Self::StatuteComparison => 19,
            Self::StatuteReferencedComparison => 21,
            Self::StatuteDelegatedComparison => 21,
            Self::StatuteOverview => 25,
            Self::AdministrativeRuleComparison => 29,
            Self::ProvisionHistory => 12,
            Self::LegalTermEverydayRelations => 102,
            Self::EverydayTermLegalRelations => 103,
            Self::LegalTermProvisionRelations => 104,
            Self::ProvisionLegalTermRelations => 105,
            Self::RelatedStatutes => 106,
        }]
    }
}

/// A seed is provider evidence, not proof of a legal date or a stable term ID.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum SupplementSeed {
    Global,
    Record {
        object: ObjectId,
        record_number: String,
    },
    Provision {
        object: ObjectId,
        number: String,
    },
    LegalTerm {
        name: String,
    },
    EverydayTerm {
        name: String,
    },
    /// `regDt` is a provider change date; do not substitute promulgation dates.
    ChangeDay {
        date: String,
    },
    ChangeWindow {
        from: String,
        to: String,
    },
}
pub fn record_seed(item: &InventoryItem) -> Result<SupplementSeed, DatabaseError> {
    item.validate_for_detail()?;
    let (record_number, _) = revision_parts(item)?;
    Ok(SupplementSeed::Record {
        object: item.object.clone(),
        record_number,
    })
}

/// Credential-free immutable request; parameters can only be set by this module.
#[derive(Clone, Debug)]
pub struct SupplementRequest {
    source: SupplementSource,
    seed: SupplementSeed,
    page: u32,
    parameters: BTreeMap<String, String>,
}
impl SupplementRequest {
    pub fn source(&self) -> SupplementSource {
        self.source
    }
    pub fn guide(&self) -> &'static str {
        self.source.contract().guide
    }
    pub fn seed(&self) -> &SupplementSeed {
        &self.seed
    }
    pub fn page(&self) -> u32 {
        self.page
    }
    pub fn parameters(&self) -> &BTreeMap<String, String> {
        &self.parameters
    }
    pub fn metadata_only(&self) -> bool {
        self.source.contract().metadata_only
    }
    pub fn format(&self) -> DocumentFormat {
        if self.source == SupplementSource::StatuteOrdinanceStatus {
            DocumentFormat::Html
        } else {
            DocumentFormat::Xml
        }
    }
    pub fn source_url(&self) -> Result<Url, DatabaseError> {
        let entry = self.source.guide();
        let mut url = Url::parse(&format!("https://www.law.go.kr{}", entry.path))
            .map_err(|_| DatabaseError::InvalidInput)?;
        url.query_pairs_mut()
            .append_pair("target", entry.target)
            .append_pair(
                "type",
                if self.format() == DocumentFormat::Html {
                    "HTML"
                } else {
                    "XML"
                },
            )
            .extend_pairs(self.parameters.iter());
        Ok(url)
    }
    /// Different aliases and repeated parent captures yield one canonical key.
    pub fn observation_key(&self) -> Result<String, DatabaseError> {
        let digest = Sha256::digest(self.source_url()?.as_str().as_bytes());
        let digest: String = digest.iter().map(|byte| format!("{byte:02x}")).collect();
        Ok(format!("law_go_kr:{}:{digest}", self.guide()))
    }
    pub fn next_page(&self) -> Result<Self, DatabaseError> {
        request(
            self.source,
            self.seed.clone(),
            self.page.checked_add(1).ok_or(DatabaseError::Capacity)?,
        )
    }
}
fn provider_object(object: &ObjectId, dataset: Dataset) -> Result<(), DatabaseError> {
    object.validate()?;
    if object.provider != "law_go_kr"
        || object.jurisdiction != "kr"
        || object.dataset != dataset
        || !numeric_id(&object.id)
        || object.id.bytes().all(|byte| byte == b'0')
    {
        return Err(DatabaseError::InvalidInput);
    }
    Ok(())
}
fn term_name(name: &str) -> Result<(), DatabaseError> {
    if name.is_empty() || name.len() > 512 || name.chars().any(char::is_control) {
        return Err(DatabaseError::InvalidInput);
    }
    Ok(())
}
pub fn request(
    source: SupplementSource,
    seed: SupplementSeed,
    page: u32,
) -> Result<SupplementRequest, DatabaseError> {
    if page == 0 || page > 1_000_000 {
        return Err(DatabaseError::InvalidInput);
    }
    if source.guide().kind == catalog::GuideKind::NeedsVerification {
        return Err(DatabaseError::SourceUnavailable);
    }
    let spec = source.contract();
    if spec.pagination == Pagination::Singleton && page != 1 {
        return Err(DatabaseError::InvalidInput);
    }
    let mut parameters = BTreeMap::new();
    match (&seed, spec.seed_kind) {
        (SupplementSeed::Global, SeedKind::Global) => {}
        (
            SupplementSeed::Record {
                object,
                record_number,
            },
            SeedKind::NationalRecord,
        ) => {
            provider_object(object, Dataset::NationalStatute)?;
            if !numeric_id(record_number) || record_number.bytes().all(|byte| byte == b'0') {
                return Err(DatabaseError::InvalidInput);
            }
            if source == SupplementSource::RelatedStatutes {
                parameters.insert("ID".into(), object.id.clone());
            } else {
                parameters.insert("MST".into(), record_number.clone());
            }
        }
        (
            SupplementSeed::Record {
                object,
                record_number,
            },
            SeedKind::AdministrativeRuleRecord,
        ) => {
            provider_object(object, Dataset::AdministrativeRule)?;
            if !numeric_id(record_number) || record_number.bytes().all(|byte| byte == b'0') {
                return Err(DatabaseError::InvalidInput);
            }
            parameters.insert("ID".into(), record_number.clone());
        }
        (SupplementSeed::Provision { object, number }, SeedKind::Provision) => {
            provider_object(object, Dataset::NationalStatute)?;
            if number.len() != 6
                || !number.bytes().all(|byte| byte.is_ascii_digit())
                || number.bytes().all(|byte| byte == b'0')
            {
                return Err(DatabaseError::InvalidInput);
            }
            parameters.insert("ID".into(), object.id.clone());
            parameters.insert("JO".into(), number.clone());
        }
        (SupplementSeed::LegalTerm { name }, SeedKind::LegalTerm)
        | (SupplementSeed::EverydayTerm { name }, SeedKind::EverydayTerm) => {
            term_name(name)?;
            parameters.insert("query".into(), name.clone());
        }
        (SupplementSeed::ChangeDay { date }, SeedKind::ChangeDay) => {
            if !valid_date(date) {
                return Err(DatabaseError::InvalidInput);
            }
            parameters.insert("regDt".into(), date.clone());
        }
        (SupplementSeed::ChangeWindow { from, to }, SeedKind::ChangeWindow) => {
            if !valid_date(from) || !valid_date(to) || from > to {
                return Err(DatabaseError::InvalidInput);
            }
            parameters.insert("fromRegDt".into(), from.clone());
            parameters.insert("toRegDt".into(), to.clone());
        }
        _ => return Err(DatabaseError::InvalidInput),
    }
    match spec.pagination {
        Pagination::Singleton => {}
        Pagination::Numbered => {
            parameters.insert("display".into(), "100".into());
            parameters.insert("page".into(), page.to_string());
        }
        Pagination::ProviderDefault => {
            parameters.insert("page".into(), page.to_string());
        }
    }
    if source == SupplementSource::StatuteReferencedComparison {
        parameters.insert("knd".into(), "1".into());
    }
    if source == SupplementSource::StatuteDelegatedComparison {
        parameters.insert("knd".into(), "2".into());
    }
    let result = SupplementRequest {
        source,
        seed,
        page,
        parameters,
    };
    let entry = source.guide();
    if !matches!(entry.path, "/DRF/lawSearch.do" | "/DRF/lawService.do")
        || entry.target.is_empty()
        || !entry
            .target
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric())
        || result
            .parameters
            .keys()
            .any(|key| !entry.request_fields.contains(&key.as_str()))
    {
        return Err(DatabaseError::SourceDataInvalid);
    }
    Ok(result)
}
/// One request per unique finite global view; no mobile, format or query aliases.
pub fn global_requests(page: u32) -> Result<Vec<SupplementRequest>, DatabaseError> {
    SUPPLEMENT_CONTRACTS
        .iter()
        .filter(|contract| contract.seed_kind == SeedKind::Global)
        .filter(|contract| page == 1 || contract.pagination != Pagination::Singleton)
        .map(|contract| request(contract.source, SupplementSeed::Global, page))
        .collect()
}
/// Derive every eligible view from a verified canonical record/provision seed.
pub fn seeded_requests(
    seed: SupplementSeed,
    page: u32,
) -> Result<Vec<SupplementRequest>, DatabaseError> {
    let kinds: &[SeedKind] = match &seed {
        SupplementSeed::Global => &[SeedKind::Global],
        SupplementSeed::Record { object, .. } if object.dataset == Dataset::NationalStatute => {
            &[SeedKind::NationalRecord]
        }
        SupplementSeed::Record { object, .. } if object.dataset == Dataset::AdministrativeRule => {
            &[SeedKind::AdministrativeRuleRecord]
        }
        SupplementSeed::Provision { .. } => &[SeedKind::Provision],
        SupplementSeed::LegalTerm { .. } => &[SeedKind::LegalTerm],
        SupplementSeed::EverydayTerm { .. } => &[SeedKind::EverydayTerm],
        SupplementSeed::ChangeDay { .. } => &[SeedKind::ChangeDay],
        SupplementSeed::ChangeWindow { .. } => &[SeedKind::ChangeWindow],
        _ => return Ok(Vec::new()),
    };
    SUPPLEMENT_CONTRACTS
        .iter()
        .filter(|contract| kinds.contains(&contract.seed_kind))
        .filter(|contract| page == 1 || contract.pagination != Pagination::Singleton)
        .map(|contract| request(contract.source, seed.clone(), page))
        .collect()
}

#[derive(Clone, Debug)]
pub struct SupplementPage {
    pub total: Option<u64>,
    pub observed_rows: u64,
    /// None means the documented response cannot establish traversal completion.
    pub done: Option<bool>,
    pub incomplete: bool,
    pub seeds: Vec<SupplementSeed>,
    /// Source assertions only; callers do not automatically follow these links.
    pub source_links: Vec<String>,
}
fn direct(node: &DocumentNode, name: &str) -> Option<String> {
    if let DocumentNode::Element { children, .. } = node {
        children.iter().find_map(|child| {
            if matches!(child,DocumentNode::Element{name:field,..} if local(field)==name) {
                let mut value = String::new();
                text(child, &mut value);
                Some(value)
            } else {
                None
            }
        })
    } else {
        None
    }
}
/// `observed_before` is the persisted row count for ProviderDefault pagination;
/// provider-default page size is not invented when `display` is undocumented.
pub fn inspect_page(
    request: &SupplementRequest,
    tree: &DocumentNode,
    observed_before: u64,
) -> Result<SupplementPage, DatabaseError> {
    let spec = request.source.contract();
    let total = first(tree, "totalCnt")
        .or_else(|| first(tree, "검색결과개수"))
        .and_then(|value| value.parse::<u64>().ok());
    let mut rows = Vec::new();
    if !spec.row_identity_fields.is_empty() {
        identity_rows(tree, spec.row_identity_fields, &mut rows);
    }
    let observed_rows = rows.len() as u64;
    let consumed = match spec.pagination {
        Pagination::Numbered => u64::from(request.page - 1) * 100 + observed_rows,
        Pagination::ProviderDefault => observed_before
            .checked_add(observed_rows)
            .ok_or(DatabaseError::Capacity)?,
        Pagination::Singleton => observed_rows,
    };
    let mut incomplete = match spec.pagination {
        Pagination::Numbered => {
            total.is_none()
                || observed_rows > 100
                || total.is_some_and(|total| {
                    total < consumed
                        || observed_rows
                            != total
                                .saturating_sub(u64::from(request.page - 1) * 100)
                                .min(100)
                })
        }
        Pagination::ProviderDefault => {
            total.is_none()
                || total.is_some_and(|total| {
                    total < consumed || (observed_rows == 0 && consumed < total)
                })
        }
        Pagination::Singleton => {
            if spec.row_identity_fields.is_empty() {
                true
            } else {
                (rows.is_empty() && total != Some(0))
                    || total.is_some_and(|total| observed_rows < total)
            }
        }
    };
    let done = if incomplete {
        None
    } else {
        match spec.pagination {
            Pagination::Numbered | Pagination::ProviderDefault => {
                total.map(|total| consumed >= total)
            }
            Pagination::Singleton => Some(true),
        }
    };
    let mut seeds = Vec::new();
    let mut seen = BTreeSet::new();
    for row in &rows {
        if matches!(
            request.source,
            SupplementSource::LegalKnowledgeTermInventory | SupplementSource::EverydayTermInventory
        ) {
            let field = if request.source == SupplementSource::EverydayTermInventory {
                "일상용어명"
            } else {
                "법령용어명"
            };
            if let Some(name) = direct(row, field) {
                if term_name(&name).is_err() {
                    incomplete = true;
                    continue;
                }
                if seen.insert(name.clone()) {
                    seeds.push(if field == "일상용어명" {
                        SupplementSeed::EverydayTerm { name }
                    } else {
                        SupplementSeed::LegalTerm { name }
                    });
                }
            }
        }
        let dataset = if request.source == SupplementSource::AdministrativeRuleComparisonInventory {
            Dataset::AdministrativeRule
        } else {
            Dataset::NationalStatute
        };
        let (id_field, serial_field) = if dataset == Dataset::AdministrativeRule {
            ("행정규칙ID", "행정규칙일련번호")
        } else {
            ("법령ID", "법령일련번호")
        };
        if let (Some(id), Some(record_number)) = (direct(row, id_field), direct(row, serial_field))
        {
            if !numeric_id(&id)
                || !numeric_id(&record_number)
                || id.bytes().all(|byte| byte == b'0')
                || record_number.bytes().all(|byte| byte == b'0')
            {
                incomplete = true;
                continue;
            }
            let object = ObjectId {
                jurisdiction: "kr".into(),
                provider: "law_go_kr".into(),
                dataset,
                id,
            };
            let seed_key = format!("{}:{}:{record_number}", dataset.as_str(), object.id);
            if seen.insert(seed_key) {
                seeds.push(SupplementSeed::Record {
                    object,
                    record_number,
                });
            }
        }
    }
    let mut links = BTreeSet::new();
    for field in request
        .source
        .guide()
        .response_fields
        .iter()
        .filter(|name| name.ends_with("링크") || name.ends_with("URL"))
    {
        let mut nodes = Vec::new();
        super::elements(tree, field, &mut nodes);
        for node in nodes {
            let mut value = String::new();
            text(node, &mut value);
            let base =
                Url::parse("https://www.law.go.kr").map_err(|_| DatabaseError::InvalidInput)?;
            if let Ok(mut url) = base.join(&value) {
                if !matches!(url.scheme(), "http" | "https")
                    || !url.username().is_empty()
                    || url.password().is_some()
                {
                    continue;
                }
                let parameters: Vec<_> = url
                    .query_pairs()
                    .filter(|(key, _)| {
                        !matches!(
                            key.to_ascii_lowercase().as_str(),
                            "oc" | "key"
                                | "apikey"
                                | "api_key"
                                | "token"
                                | "servicekey"
                                | "password"
                        )
                    })
                    .map(|(key, value)| (key.into_owned(), value.into_owned()))
                    .collect();
                url.set_query(None);
                url.set_fragment(None);
                if url.scheme() == "http" {
                    url.set_scheme("https")
                        .map_err(|_| DatabaseError::InvalidInput)?;
                }
                if !parameters.is_empty() {
                    url.query_pairs_mut().extend_pairs(parameters);
                }
                links.insert(url.to_string());
            }
        }
    }
    Ok(SupplementPage {
        total,
        observed_rows,
        done: if incomplete { None } else { done },
        incomplete,
        seeds,
        source_links: links.into_iter().collect(),
    })
}
#[derive(Clone, Debug)]
pub struct SupplementCapture {
    pub observation_key: String,
    pub source_url: String,
    pub raw: Vec<u8>,
    /// The retained XML includes the disclosed transport-link OC redaction.
    pub credentials_redacted: bool,
    pub retrieved_at: u64,
    pub processor_version: String,
    /// Retained raw evidence survives projection failures without claiming completion.
    pub processing_error: Option<DatabaseError>,
    pub page: SupplementPage,
}
#[derive(Clone, Debug)]
pub enum SupplementOutcome {
    Captured(SupplementCapture),
    Deferred {
        observation_key: String,
        source_url: String,
        reason: &'static str,
    },
}
impl LawClient {
    /// Uses the same destination, durable admission and document bounds as
    /// original collection. Unverified explanatory works spend no request budget.
    pub async fn fetch_supplement(
        &self,
        request: &SupplementRequest,
        observed_before: u64,
        cancel: CancellationToken,
    ) -> Result<SupplementOutcome, DatabaseError> {
        let observation_key = request.observation_key()?;
        let source_url = request.source_url()?.to_string();
        if request.metadata_only() {
            return Ok(SupplementOutcome::Deferred {
                observation_key,
                source_url,
                reason: request.source.contract().reason,
            });
        }
        let entry = request.source.guide();
        let file = entry
            .path
            .strip_prefix("/DRF/")
            .ok_or(DatabaseError::InvalidInput)?;
        let mut url = self.api(file, entry.target)?;
        if request.format() == DocumentFormat::Html {
            let parameters: Vec<_> = url
                .query_pairs()
                .filter(|(key, _)| key != "type")
                .map(|(key, value)| (key.into_owned(), value.into_owned()))
                .collect();
            url.set_query(None);
            url.query_pairs_mut()
                .extend_pairs(parameters)
                .append_pair("type", "HTML");
        }
        url.query_pairs_mut()
            .extend_pairs(request.parameters.iter());
        let (raw, retrieved_at) = self
            .fetch_original(url, request.format(), None, cancel.clone())
            .await?;
        let result = self
            .analyze_supplement(request, &raw, observed_before, cancel)
            .await;
        let (page, processor_version, processing_error) = match result {
            Ok((page, version)) => (page, version, None),
            Err(DatabaseError::Cancelled) => return Err(DatabaseError::Cancelled),
            Err(error) => (
                SupplementPage {
                    total: None,
                    observed_rows: 0,
                    done: None,
                    incomplete: true,
                    seeds: Vec::new(),
                    source_links: Vec::new(),
                },
                "unprocessed".into(),
                Some(error),
            ),
        };
        let credentials_redacted = super::has_credential_redaction(&raw);
        Ok(SupplementOutcome::Captured(SupplementCapture {
            observation_key,
            source_url,
            raw,
            credentials_redacted,
            retrieved_at,
            processor_version,
            processing_error,
            page,
        }))
    }
    /// Reprocess retained bytes after worker recovery without spending another
    /// upstream request or downloading a potentially corrected response.
    pub async fn analyze_supplement(
        &self,
        request: &SupplementRequest,
        raw: &[u8],
        observed_before: u64,
        cancel: CancellationToken,
    ) -> Result<(SupplementPage, String), DatabaseError> {
        if request.metadata_only() || raw.len() > 16 * 1024 * 1024 {
            return Err(DatabaseError::InvalidInput);
        }
        let source_sha256 = Sha256::digest(raw)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        let output = self
            .processor
            .process(
                DocumentInput {
                    format: request.format(),
                    raw: raw.to_vec(),
                    source_sha256,
                    ocr: false,
                },
                cancel,
            )
            .await
            .map_err(super::document_error)?;
        let tree = output
            .tree
            .as_ref()
            .ok_or(DatabaseError::SourceDataInvalid)?;
        let page = inspect_page(request, tree, observed_before)?;
        Ok((page, output.processor_version))
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum GuideCoverageStatus {
    Original { datasets: Vec<Dataset> },
    Supplemental { sources: Vec<SupplementSource> },
    RepresentationAlias,
    QueryOnly,
    NeedsVerification { reason: &'static str },
}
#[derive(Clone, Debug, Serialize)]
pub struct GuideCoverage {
    pub guide: &'static str,
    pub status: GuideCoverageStatus,
}
/// Coverage is explicit for every index entry, including omitted query functions
/// and unresolved contracts; an API count never becomes an object count.
pub fn guide_coverage() -> Vec<GuideCoverage> {
    catalog::GUIDE_ENTRIES
        .iter()
        .map(|entry| {
            let datasets: BTreeSet<_> = catalog::SOURCE_FAMILIES
                .iter()
                .filter(|family| {
                    family.list_guide == entry.guide || family.detail_guide == Some(entry.guide)
                })
                .map(|family| family.dataset.as_str())
                .collect();
            let sources: Vec<_> = SUPPLEMENT_CONTRACTS
                .iter()
                .filter(|contract| contract.guide == entry.guide)
                .map(|contract| contract.source)
                .collect();
            let status = if entry.kind == catalog::GuideKind::NeedsVerification {
                GuideCoverageStatus::NeedsVerification {
                    reason: entry.reason,
                }
            } else if !datasets.is_empty() {
                GuideCoverageStatus::Original {
                    datasets: datasets
                        .into_iter()
                        .filter_map(Dataset::from_name)
                        .collect(),
                }
            } else if !sources.is_empty() {
                GuideCoverageStatus::Supplemental { sources }
            } else {
                match entry.kind {
                    catalog::GuideKind::RepresentationAlias => {
                        GuideCoverageStatus::RepresentationAlias
                    }
                    catalog::GuideKind::QueryOnly => GuideCoverageStatus::QueryOnly,
                    _ => GuideCoverageStatus::NeedsVerification {
                        reason: entry.reason,
                    },
                }
            };
            GuideCoverage {
                guide: entry.guide,
                status,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use openlegal_application::document::{DocumentError, DocumentOutput, DocumentProcessor};
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };
    struct RejectingProcessor;
    impl DocumentProcessor for RejectingProcessor {
        fn process(
            &self,
            _input: DocumentInput,
            _cancel: CancellationToken,
        ) -> futures::future::BoxFuture<'static, Result<DocumentOutput, DocumentError>> {
            Box::pin(async { Err(DocumentError::InvalidDocument) })
        }
    }
    fn field(name: &str, value: &str) -> DocumentNode {
        DocumentNode::Element {
            name: name.into(),
            attributes: vec![],
            children: vec![DocumentNode::Text {
                value: value.into(),
            }],
        }
    }
    fn branch(name: &str, children: Vec<DocumentNode>) -> DocumentNode {
        DocumentNode::Element {
            name: name.into(),
            attributes: vec![],
            children,
        }
    }
    fn object() -> ObjectId {
        ObjectId {
            jurisdiction: "kr".into(),
            provider: "law_go_kr".into(),
            dataset: Dataset::NationalStatute,
            id: "000123".into(),
        }
    }
    #[test]
    fn finite_globals_and_parent_views_are_canonical_and_credential_free() {
        let globals = global_requests(1).unwrap();
        assert_eq!(globals.len(), 14);
        let keys: BTreeSet<_> = globals
            .iter()
            .map(|request| request.observation_key().unwrap())
            .collect();
        assert_eq!(keys.len(), globals.len());
        for request in &globals {
            assert!(
                request
                    .observation_key()
                    .unwrap()
                    .starts_with(&format!("law_go_kr:{}:", request.guide()))
            );
            assert!(!request.parameters().contains_key("OC"));
            assert!(
                !request
                    .source_url()
                    .unwrap()
                    .query_pairs()
                    .any(|(key, _)| key == "OC")
            );
            assert!(!request.source().guide().guide.starts_with("mob"));
        }
        let seed = SupplementSeed::Record {
            object: object(),
            record_number: "000999".into(),
        };
        let views = seeded_requests(seed, 1).unwrap();
        assert_eq!(views.len(), 7);
        let keys: BTreeSet<_> = views
            .iter()
            .map(|request| request.observation_key().unwrap())
            .collect();
        assert_eq!(keys.len(), views.len());
        let comparisons: Vec<_> = views
            .iter()
            .filter(|request| request.source().guide().target == "thdCmp")
            .collect();
        assert_eq!(comparisons.len(), 2);
        assert_eq!(
            comparisons[0].parameters().get("knd").map(String::as_str),
            Some("1")
        );
        assert_eq!(
            comparisons[1].parameters().get("knd").map(String::as_str),
            Some("2")
        );
        assert_eq!(
            views
                .iter()
                .filter(|request| request.metadata_only())
                .count(),
            1
        );
    }
    #[test]
    fn seed_shapes_and_provider_default_page_size_follow_the_guides() {
        assert!(
            request(
                SupplementSource::StatuteDelegation,
                SupplementSeed::Global,
                1
            )
            .is_err()
        );
        let req = request(
            SupplementSource::ProvisionHistory,
            SupplementSeed::Provision {
                object: object(),
                number: "000202".into(),
            },
            2,
        )
        .unwrap();
        assert_eq!(
            req.parameters().get("ID").map(String::as_str),
            Some("000123")
        );
        assert_eq!(
            req.parameters().get("JO").map(String::as_str),
            Some("000202")
        );
        assert_eq!(
            request(
                SupplementSource::ProvisionChangeInventory,
                SupplementSeed::ChangeWindow {
                    from: "20260101".into(),
                    to: "20260131".into(),
                },
                1,
            )
            .err(),
            Some(DatabaseError::SourceUnavailable)
        );
        assert_eq!(
            request(
                SupplementSource::StatuteChangeInventory,
                SupplementSeed::ChangeDay {
                    date: "20260101".into()
                },
                1
            )
            .err(),
            Some(DatabaseError::SourceUnavailable)
        );
        assert!(
            request(
                SupplementSource::StatuteChangeInventory,
                SupplementSeed::ChangeDay {
                    date: "20260230".into()
                },
                1
            )
            .is_err()
        );
        assert!(
            request(
                SupplementSource::ProvisionHistory,
                SupplementSeed::Provision {
                    object: object(),
                    number: "2".into()
                },
                1
            )
            .is_err()
        );
    }
    #[test]
    fn pagination_rejects_omitted_counts_and_underfilled_pages() {
        let req = request(
            SupplementSource::StatuteAnnexInventory,
            SupplementSeed::Global,
            1,
        )
        .unwrap();
        let tree = branch(
            "Result",
            vec![
                field("totalCnt", "2"),
                branch("annex", vec![field("별표일련번호", "001")]),
            ],
        );
        assert!(inspect_page(&req, &tree, 0).unwrap().incomplete);
        let empty = branch("Result", vec![field("totalCnt", "0")]);
        assert_eq!(inspect_page(&req, &empty, 0).unwrap().done, Some(true));
        assert!(
            inspect_page(&req, &branch("Result", vec![]), 0)
                .unwrap()
                .incomplete
        );
        let req = req.next_page().unwrap();
        let tree = branch(
            "Result",
            vec![
                field("totalCnt", "101"),
                branch("annex", vec![field("별표일련번호", "001")]),
            ],
        );
        assert_eq!(inspect_page(&req, &tree, 100).unwrap().done, Some(true));
    }
    #[test]
    fn observed_terms_are_finite_seeds_and_links_strip_authentication() {
        let req = request(
            SupplementSource::LegalKnowledgeTermInventory,
            SupplementSeed::Global,
            1,
        )
        .unwrap();
        let row = branch(
            "법령용어",
            vec![
                field("법령용어명", "가상 용어"),
                field(
                    "용어간관계링크",
                    "/DRF/lawService.do?OC=fictional-secret&target=lstrmRlt&query=test",
                ),
            ],
        );
        let tree = branch("Result", vec![field("검색결과개수", "2"), row.clone(), row]);
        let page = inspect_page(&req, &tree, 0).unwrap();
        assert_eq!(page.seeds.len(), 1);
        assert_eq!(page.source_links.len(), 1);
        assert!(!page.source_links[0].contains("fictional-secret"));
        assert!(!page.source_links[0].contains("OC="));
        let requests = seeded_requests(page.seeds[0].clone(), 1).unwrap();
        assert_eq!(requests.len(), 2);
        assert!(requests.iter().all(
            |request| request.parameters().get("query").map(String::as_str) == Some("가상 용어")
        ));
    }
    #[test]
    fn comparison_ids_do_not_become_canonical_statute_ids() {
        let req = request(
            SupplementSource::StatuteComparisonInventory,
            SupplementSeed::Global,
            1,
        )
        .unwrap();
        let tree = branch(
            "Result",
            vec![
                field("totalCnt", "1"),
                branch(
                    "oldAndNew",
                    vec![field("신구법ID", "0001"), field("신구법일련번호", "0010")],
                ),
            ],
        );
        let page = inspect_page(&req, &tree, 0).unwrap();
        assert_eq!(page.done, Some(true));
        assert!(page.seeds.is_empty());
    }
    #[test]
    fn every_documented_finite_or_identified_route_has_explicit_coverage() {
        let coverage = guide_coverage();
        assert_eq!(coverage.len(), catalog::GUIDE_ENTRIES.len());
        for contract in SUPPLEMENT_CONTRACTS {
            assert_eq!(contract.source.guide().guide, contract.guide);
            assert!(!contract.source.guide().target.is_empty());
        }
        for (entry, coverage) in catalog::GUIDE_ENTRIES.iter().zip(&coverage) {
            if matches!(
                entry.kind,
                catalog::GuideKind::FiniteMetadata | catalog::GuideKind::IdentifiedSupplement
            ) {
                assert!(matches!(
                    coverage.status,
                    GuideCoverageStatus::Supplemental { .. }
                ));
            }
        }
        assert!(matches!(
            coverage
                .iter()
                .find(|entry| entry.guide == "datDelHstGuide")
                .unwrap()
                .status,
            GuideCoverageStatus::NeedsVerification { .. }
        ));
        assert!(matches!(
            coverage
                .iter()
                .find(|entry| entry.guide == "aiSearchGuide")
                .unwrap()
                .status,
            GuideCoverageStatus::QueryOnly
        ));
        for guide in ["lsChgListGuide", "lsDayJoRvsListGuide"] {
            assert!(matches!(
                coverage
                    .iter()
                    .find(|entry| entry.guide == guide)
                    .unwrap()
                    .status,
                GuideCoverageStatus::NeedsVerification {
                    reason: "complete_change_date_universe_not_documented"
                }
            ));
        }
    }
    #[tokio::test]
    async fn unknown_explanatory_rights_spend_no_upstream_attempts() {
        let observer = Arc::new(AtomicBool::new(false));
        let client = LawClient::new("fictional".into(), Arc::new(RejectingProcessor))
            .unwrap()
            .with_local_cap(0)
            .with_reservation_observer(observer.clone());
        let request = request(
            SupplementSource::StatuteOverview,
            SupplementSeed::Record {
                object: object(),
                record_number: "999".into(),
            },
            1,
        )
        .unwrap();
        assert!(matches!(
            client
                .fetch_supplement(&request, 0, CancellationToken::new())
                .await
                .unwrap(),
            SupplementOutcome::Deferred {
                reason: "external_explanatory_content_reuse_unverified",
                ..
            }
        ));
        assert!(!observer.load(Ordering::Acquire));
        assert_eq!(
            client
                .analyze_supplement(&request, b"<document/>", 0, CancellationToken::new())
                .await
                .err(),
            Some(DatabaseError::InvalidInput)
        );
    }
    #[tokio::test]
    async fn retained_payload_reprocessing_never_reserves_another_request() {
        let observer = Arc::new(AtomicBool::new(false));
        let client = LawClient::new("fictional".into(), Arc::new(RejectingProcessor))
            .unwrap()
            .with_local_cap(0)
            .with_reservation_observer(observer.clone());
        let request = request(
            SupplementSource::StatuteAnnexInventory,
            SupplementSeed::Global,
            1,
        )
        .unwrap();
        assert_eq!(
            client
                .analyze_supplement(&request, b"<invalid/>", 0, CancellationToken::new())
                .await
                .err(),
            Some(DatabaseError::SourceDataInvalid)
        );
        assert!(!observer.load(Ordering::Acquire));
    }
}
