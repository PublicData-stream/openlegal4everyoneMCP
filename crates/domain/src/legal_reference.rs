//! Contracts for law-name resolution, citation checks and date-based revision selection.
//! Results describe what the retained corpus shows; they never assert that a provider
//! lacks an object or decide which law legally applies to particular facts.
use crate::{
    jurisdiction::Jurisdiction,
    legal::{CollectionNotice, ObjectId},
};
use schemars::JsonSchema;
use serde::Serialize;
use std::fmt;

/// An article locator. In KOR it is `제{number}조`, the administrative-rule form
/// `제{number}-{part}조`, either followed by `의{branch}`; `Display` uses that form,
/// and other jurisdictions format it through their reference profile.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, JsonSchema)]
pub struct ArticleNumber {
    pub number: u32,
    pub part: Option<u32>,
    pub branch: Option<u32>,
}
impl fmt::Display for ArticleNumber {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "제{}", self.number)?;
        if let Some(part) = self.part {
            write!(f, "-{part}")?;
        }
        write!(f, "조")?;
        if let Some(branch) = self.branch {
            write!(f, "의{branch}")?;
        }
        Ok(())
    }
}

/// How a supplied law name was rewritten before corpus lookup.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, JsonSchema)]
pub struct LawNameResolution {
    pub input: String,
    /// Bracket, whitespace and middle-dot normalization only.
    pub normalized: String,
    /// The name looked up in the corpus, after any alias expansion.
    pub resolved: String,
    /// The alias table entry that produced `resolved`, if any.
    pub matched_alias: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum TitleStatus {
    /// The title belongs to the object's current HEAD capture.
    Current,
    /// The title appears only in a retained historical capture of the object.
    Former,
}

/// One corpus object whose title matched a resolved name.
#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct TitleMatch {
    pub object: ObjectId,
    pub matched_title: String,
    pub title_status: TitleStatus,
    /// Present when the matched title is former and HEAD metadata was readable.
    pub current_title: Option<String>,
    pub revision_id: String,
    pub capture_id: String,
}

#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct ResolveNameResult {
    pub schema_version: u32,
    /// The jurisdiction whose naming rules were applied.
    pub jurisdiction: Jurisdiction,
    pub resolution: LawNameResolution,
    pub matches: Vec<TitleMatch>,
    /// True only when every search page reported complete current corpus coverage.
    pub corpus_complete: bool,
    pub collection_notices: Vec<CollectionNotice>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum StatuteCitationStatus {
    /// The article (and any cited paragraph/subparagraph) exists in the checked capture,
    /// and any cited article title is consistent with the retained title.
    Verified,
    ArticleNotFound,
    ArticleDeleted,
    ParagraphNotFound,
    SubparagraphNotFound,
    /// The article exists but the cited title differs from the retained title.
    TitleMismatch,
    /// No retained object has this name; this is not a statement that the law does not exist.
    LawNotObserved,
    /// More than one retained object has this name.
    LawAmbiguous,
    /// The citation refers to an unnamed law, such as `이 법` or `같은 법` without an antecedent.
    LawNameUnresolved,
    /// The matched object could not be read (stale, pending, withdrawn or storage failure).
    Unavailable,
}

#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct StatuteCitationResult {
    /// The cited text exactly as it appears in the input.
    pub text: String,
    /// UTF-8 byte offsets of `text` in the input.
    pub byte_start: usize,
    pub byte_end: usize,
    /// The law name written in the input, or the antecedent name for `같은 법`.
    pub law_name: Option<String>,
    /// True when the law name was taken from an earlier citation.
    pub inherited: bool,
    /// Names tried when no retained law matched an unbracketed citation, longest first.
    pub law_name_candidates: Vec<String>,
    pub resolution: Option<LawNameResolution>,
    pub article: String,
    pub paragraph: Option<u32>,
    pub subparagraph: Option<u32>,
    pub cited_article_title: Option<String>,
    pub status: StatuteCitationStatus,
    /// Reason code accompanying `unavailable`, `law_ambiguous` or a partial check.
    pub detail: Option<String>,
    pub object: Option<ObjectId>,
    pub law_title: Option<String>,
    pub law_title_status: Option<TitleStatus>,
    pub revision_id: Option<String>,
    pub capture_id: Option<String>,
    pub retained_article_title: Option<String>,
    /// Character-bigram similarity (0-100) between the cited and retained article titles.
    pub title_similarity: Option<u8>,
    /// First and last article locators in the checked capture, for not-found results.
    pub article_range: Option<String>,
    /// Highest numbered paragraph (①, ②, ...) in the article; None when unnumbered.
    pub paragraph_count: Option<u32>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum CaseCitationStatus {
    Observed,
    /// No retained record has this case number; the decision may still exist.
    NotObserved,
}

#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct CaseRecordMatch {
    pub object: ObjectId,
    pub title: String,
    pub revision_id: String,
    pub capture_id: String,
}

#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct CaseCitationResult {
    pub text: String,
    pub byte_start: usize,
    pub byte_end: usize,
    pub case_number: String,
    pub status: CaseCitationStatus,
    pub matches: Vec<CaseRecordMatch>,
}

#[derive(Clone, Debug, Default, Serialize, JsonSchema)]
pub struct CitationSummary {
    pub verified: u32,
    /// Article, paragraph, subparagraph, deletion or title problems in a matched law.
    pub failed: u32,
    /// Not observed, ambiguous, unresolved or unreadable citations.
    pub unchecked: u32,
    pub cases_observed: u32,
    pub cases_not_observed: u32,
}

#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct CitationVerification {
    pub schema_version: u32,
    /// The jurisdiction whose citation rules were applied.
    pub jurisdiction: Jurisdiction,
    pub statutes: Vec<StatuteCitationResult>,
    pub cases: Vec<CaseCitationResult>,
    pub summary: CitationSummary,
    /// True when more citations were found than one call checks.
    pub truncated: bool,
    pub corpus_complete: bool,
    pub collection_notices: Vec<CollectionNotice>,
}

/// A provider revision considered by date-based selection.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, JsonSchema)]
pub struct RevisionChoice {
    pub revision_id: String,
    pub effective_date: Option<String>,
    pub publication_date: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum InForceStatus {
    /// The retained inventory is complete and one revision took effect on or before the date.
    Determined,
    /// A revision was selected, but the retained inventory is incomplete.
    Provisional,
    /// Every dated retained revision takes effect after the date.
    NotYetEffective,
    /// No retained revision carries an effective date.
    Undetermined,
}

#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct InForceSelection {
    pub date: String,
    pub status: InForceStatus,
    pub selected: Option<RevisionChoice>,
    /// Other revisions with the selected effective date; the latest publication is selected.
    pub same_effective_date: Vec<RevisionChoice>,
    /// The earliest retained revision taking effect after the date.
    pub next_change: Option<RevisionChoice>,
    /// Provision-level effective dates in the selected revision that fall after the date.
    pub later_provision_dates: Vec<String>,
    pub warnings: Vec<String>,
}

#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct ArticleText {
    pub revision_id: String,
    pub capture_id: String,
    pub title: String,
    pub text: String,
    pub truncated: bool,
    pub deleted: bool,
}

#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct ArticleAtDate {
    pub article: String,
    /// None when the article is absent from the selected revision.
    pub at_date: Option<ArticleText>,
    pub head: Option<ArticleText>,
    /// Whether the selected and HEAD article texts differ, when both were found.
    pub changed_since: Option<bool>,
}

#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct InForceResult {
    pub schema_version: u32,
    /// The jurisdiction whose naming and article rules were applied; absent when an
    /// object of a jurisdiction without a profile was selected by date only.
    pub jurisdiction: Option<Jurisdiction>,
    pub object: ObjectId,
    pub law_title: String,
    pub resolution: Option<LawNameResolution>,
    pub selection: InForceSelection,
    pub article: Option<ArticleAtDate>,
    pub compare: Option<InForceSelection>,
    /// Exact selectors for `database.diff` when both dates selected a revision.
    pub diff_before: Option<crate::legal::RevisionSelector>,
    pub diff_after: Option<crate::legal::RevisionSelector>,
    pub basis: String,
}
