//! Contracts for change watching, lineage, citator signals, article impact and
//! structured article reads over the retained corpus. Like the reference tools,
//! these results report retained evidence and never assert provider absence or
//! legal effect.
use crate::{
    legal::{CollectionNotice, Dataset, ObjectId, SectionKind},
    legal_reference::{InForceSelection, LawNameResolution, RevisionChoice},
};
use schemars::JsonSchema;
use serde::Serialize;
use std::collections::BTreeMap;

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
    /// Retained revisions whose effective date is after today (promulgated, not yet effective).
    pub upcoming: Vec<RevisionChoice>,
}

#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct WatchResult {
    pub schema_version: u32,
    /// Korean calendar date used for `upcoming`.
    pub today: String,
    pub entries: Vec<WatchEntry>,
    /// `dataset:id` to HEAD revision ID; pass it back as `previous` next time.
    pub snapshot: BTreeMap<String, String>,
    pub changed: u32,
    pub with_upcoming: u32,
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
    pub today: String,
    /// Oldest first.
    pub titles: Vec<TitlePeriod>,
    pub renamed: bool,
    pub upcoming: Vec<RevisionChoice>,
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
