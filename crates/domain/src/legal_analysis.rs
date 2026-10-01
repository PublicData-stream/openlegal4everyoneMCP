//! Contracts for change watching, lineage, citator signals, article impact and
//! structured article reads over the retained corpus. Like the reference tools,
//! these results report retained evidence and never assert provider absence or
//! legal effect.
use crate::{
    legal::{CollectionNotice, Dataset, ObjectId, SectionKind},
    legal_reference::{InForceSelection, LawNameResolution},
};
use schemars::JsonSchema;
use serde::Serialize;
use std::collections::BTreeMap;

/// How a revision repealed its law, from the provider amendment type (`제개정구분명`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum RepealKind {
    /// `폐지`
    Repealed,
    /// `타법폐지`: repealed by another act.
    RepealedByOtherLaw,
    /// `일괄폐지`
    RepealedInBatch,
}
impl RepealKind {
    /// `폐지제정` (repealed and re-enacted) and every amendment type are not repeals.
    pub fn from_amendment_type(value: &str) -> Option<Self> {
        let compact: String = value.chars().filter(|c| !c.is_whitespace()).collect();
        match compact.as_str() {
            "폐지" => Some(Self::Repealed),
            "타법폐지" => Some(Self::RepealedByOtherLaw),
            "일괄폐지" => Some(Self::RepealedInBatch),
            _ => None,
        }
    }
}

/// Repeal state read from the latest retained revision of an object.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum RepealStatus {
    /// The latest retained revision is a repeal whose effective date is today or earlier.
    Repealed,
    /// The latest retained revision is a repeal taking effect after today.
    RepealScheduled,
    /// The latest retained revision records an amendment type that is not a repeal.
    /// A repeal the corpus has not collected yet is not reflected.
    NoRepealRecorded,
    /// The latest retained revision records no amendment type, or could not be read.
    Unknown,
}

#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct RepealRecord {
    pub revision_id: String,
    /// The provider amendment type exactly as recorded, such as `타법폐지`.
    pub amendment_type: String,
    pub kind: RepealKind,
    pub effective_date: Option<String>,
    pub publication_date: Option<String>,
}

/// A retained revision whose effective date is after today.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, JsonSchema)]
pub struct UpcomingRevision {
    pub revision_id: String,
    pub effective_date: Option<String>,
    pub publication_date: Option<String>,
    /// The provider amendment type, when recorded, for the first five upcoming revisions.
    pub amendment_type: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum WatchStatus {
    /// HEAD revision equals the supplied previous revision.
    Unchanged,
    /// HEAD revision differs from the supplied previous revision.
    Changed,
    /// No previous revision was supplied for this object.
    New,
    NotObserved,
    Ambiguous,
    /// HEAD metadata could not be read; `detail` gives the corpus error code.
    Unavailable,
}

#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct WatchEntry {
    /// The supplied law name, or `dataset:id` for a supplied object.
    pub input: String,
    pub resolution: Option<LawNameResolution>,
    pub object: Option<ObjectId>,
    pub title: Option<String>,
    pub status: WatchStatus,
    pub detail: Option<String>,
    pub previous_revision_id: Option<String>,
    pub head_revision_id: Option<String>,
    pub effective_date: Option<String>,
    pub publication_date: Option<String>,
    /// The provider amendment type of the HEAD revision, when recorded.
    pub amendment_type: Option<String>,
    /// Retained revisions whose effective date is after today (promulgated, not yet effective).
    pub upcoming: Vec<UpcomingRevision>,
    /// None for datasets without provider revisions or when no object was read.
    pub repeal_status: Option<RepealStatus>,
    pub repeal: Option<RepealRecord>,
}

#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct WatchResult {
    pub schema_version: u32,
    /// Calendar date (`YYYYMMDD`) in `timezone` used for `upcoming` and repeal status.
    pub today: String,
    /// IANA time zone that defined `today`.
    pub timezone: String,
    pub entries: Vec<WatchEntry>,
    /// `dataset:id` to HEAD revision ID; pass it back as `previous` next time.
    pub snapshot: BTreeMap<String, String>,
    pub changed: u32,
    pub with_upcoming: u32,
    /// Entries whose repeal status is `repealed` or `repeal_scheduled`.
    pub repealed: u32,
    pub corpus_complete: bool,
    pub collection_notices: Vec<CollectionNotice>,
}

/// A run of consecutive retained revisions that carry the same title.
#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct TitlePeriod {
    pub title: String,
    pub first_revision_id: String,
    pub first_effective_date: Option<String>,
    pub last_revision_id: String,
    pub last_effective_date: Option<String>,
}

/// A line in another retained text that mentions this law's title and `폐지`.
/// It is a lead to read, unlike the provider amendment type in [`RepealRecord`].
#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct RepealMention {
    pub object: ObjectId,
    pub title: String,
    pub line: String,
    pub revision_id: String,
    pub capture_id: String,
}

#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct LineageResult {
    pub schema_version: u32,
    pub object: ObjectId,
    pub resolution: Option<LawNameResolution>,
    /// `published`, or the corpus error code returned for HEAD (for example `withdrawn`).
    pub head_state: String,
    pub current_title: Option<String>,
    /// Calendar date (`YYYYMMDD`) in `timezone` used for `upcoming` and repeal status.
    pub today: String,
    /// IANA time zone that defined `today`.
    pub timezone: String,
    /// Oldest first.
    pub titles: Vec<TitlePeriod>,
    pub renamed: bool,
    pub upcoming: Vec<UpcomingRevision>,
    /// Repeal state from the provider amendment type of the latest retained revision.
    pub repeal_status: RepealStatus,
    pub repeal: Option<RepealRecord>,
    pub latest_amendment_type: Option<String>,
    pub repeal_mentions: Vec<RepealMention>,
    /// Catalog revisions with no retained capture title.
    pub revisions_without_title: u32,
    pub inventory_complete: bool,
    pub corpus_complete: bool,
    pub collection_notices: Vec<CollectionNotice>,
}

#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct CitingDecision {
    pub object: ObjectId,
    pub title: String,
    pub case_number: Option<String>,
    pub judgment_date: Option<String>,
    pub authority: Option<String>,
    /// The title, or the citing line with overruling language, mentions `전원합의체`.
    pub en_banc: bool,
    /// An overruling phrase found on a line that cites the case number.
    pub overruling_phrase: Option<String>,
    /// Up to three lines citing the case number, each at most 400 characters.
    pub lines: Vec<String>,
    pub revision_id: String,
    pub capture_id: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum CitatorSignal {
    /// A citing line contains overruling language; read it before relying on the case.
    OverrulingLanguageFound,
    /// No citing line contains the recognized phrases. This does not prove the case is good law.
    NoneFound,
}

#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct CitatorResult {
    pub schema_version: u32,
    pub case_number: String,
    /// Retained records carrying the case number.
    pub targets: Vec<crate::legal_reference::CaseRecordMatch>,
    /// Newest judgment first; undated last.
    pub citing: Vec<CitingDecision>,
    pub signal: CitatorSignal,
    pub truncated: bool,
    pub corpus_complete: bool,
    pub collection_notices: Vec<CollectionNotice>,
    pub basis: String,
}

#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct ImpactReference {
    pub object: ObjectId,
    pub title: String,
    pub line: String,
    pub revision_id: String,
    pub capture_id: String,
}

#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct ImpactBucket {
    pub dataset: Dataset,
    pub object_count: u32,
    /// Up to 20 objects, one line each.
    pub references: Vec<ImpactReference>,
}

/// A citation found in the article's own text.
#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct OutboundReference {
    pub text: String,
    /// None for a reference within the same law (`제3조`, `이 법 제3조`) or an unresolved
    /// `같은 법`; `같은 법` with an antecedent carries that law's name.
    pub law_name: Option<String>,
    pub article: String,
}

#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct ImpactResult {
    pub schema_version: u32,
    pub object: ObjectId,
    pub resolution: Option<LawNameResolution>,
    pub law_title: String,
    pub article: String,
    pub article_title: Option<String>,
    pub inbound: Vec<ImpactBucket>,
    pub outbound: Vec<OutboundReference>,
    /// Mermaid `graph LR` source summarizing inbound and outbound references.
    pub mermaid: String,
    pub truncated: bool,
    pub corpus_complete: bool,
    pub collection_notices: Vec<CollectionNotice>,
}

#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct ArticleUnit {
    pub article: String,
    pub title: String,
    pub text: String,
    pub truncated: bool,
    pub deleted: bool,
    /// Enclosing headings, outermost first, such as `제2장 거래`.
    pub path: Vec<String>,
}

#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct OutlineEntry {
    /// `heading` or `article`.
    pub kind: String,
    pub label: String,
    pub title: String,
}

#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct AnnexText {
    pub label: String,
    pub section_id: String,
    pub title: String,
    pub kind: SectionKind,
    pub text: String,
    pub truncated: bool,
    /// Little Korean text or an image tag: the table may exist only as an image.
    pub sparse: bool,
}

#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct ArticleReadResult {
    pub schema_version: u32,
    pub object: ObjectId,
    pub resolution: Option<LawNameResolution>,
    pub law_title: String,
    pub revision_id: String,
    pub capture_id: String,
    pub effective_date: Option<String>,
    pub source_url: String,
    /// Present when a `date` selected the revision.
    pub selection: Option<InForceSelection>,
    pub articles: Vec<ArticleUnit>,
    /// Every article whose text contains the keyword, when `keyword` was given.
    pub keyword_matches: Vec<String>,
    /// Headings and articles, when no article, chapter, keyword or annex was given.
    pub outline: Vec<OutlineEntry>,
    pub annexes: Vec<AnnexText>,
    /// Labels of every annex found in the capture.
    pub annex_index: Vec<String>,
    pub truncated: bool,
    pub warnings: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::RepealKind;

    #[test]
    fn repeal_kinds_follow_provider_amendment_types() {
        assert_eq!(
            RepealKind::from_amendment_type("타법 폐지"),
            Some(RepealKind::RepealedByOtherLaw)
        );
        assert_eq!(
            RepealKind::from_amendment_type("폐지"),
            Some(RepealKind::Repealed)
        );
        for other in ["폐지제정", "일부개정", "타법개정", ""] {
            assert_eq!(RepealKind::from_amendment_type(other), None);
        }
    }
}
