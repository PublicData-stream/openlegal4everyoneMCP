//! MCP tools for Korean law-name resolution, citation checks and date-based
//! revision selection over the managed corpus. Parsing is delegated to
//! `openlegal_normalization::kr_legal_reference`; corpus access and selection
//! policy to `openlegal_application::legal_reference`.
use crate::{
    ServerError,
    database::map_error,
    registry::{ToolError, ToolModule, ToolOptions, ToolOutput, ToolRegistry},
};
use openlegal_application::legal_reference::{ReferenceLookup, later_dates, select_in_force};
use openlegal_domain::{
    legal::{
        CollectionNotice, DatabaseError, Dataset, GetRequest, GetResult, ObjectId,
        RevisionSelector, valid_date,
    },
    legal_reference::*,
};
use openlegal_normalization::kr_legal_reference::{self as kr, ArticleLookup, LawReference};
use schemars::JsonSchema;
use serde::Deserialize;
use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
    time::Instant,
};
use tokio_util::sync::CancellationToken;

pub(crate) const ARTICLE_TEXT_LIMIT: usize = 16 * 1024;
const MAX_FORMER_TITLE_OBJECTS: usize = 20;
const MAX_CACHED_CAPTURES: usize = 8;
/// Cited and retained article titles below this similarity are reported as mismatches.
const TITLE_MATCH_THRESHOLD: u8 = 50;
const BASIS: &str = "Selected the retained provider revision with the latest effective date on or before the date; among equal effective dates, the latest publication is selected. This is not a legal determination: supplementary provisions (부칙), transitional rules, retroactivity and provision-level effective dates can change which text applies to particular facts.";

pub struct LegalReferenceTools {
    pub lookup: Arc<ReferenceLookup>,
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ResolveNameInput {
    /// A law name or common abbreviation, such as `산안법 시행령`.
    name: String,
    /// Datasets to search; defaults to national statutes.
    #[serde(default)]
    datasets: Vec<Dataset>,
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct VerifyInput {
    /// Text containing citations such as `「민법」 제750조` or `2007다27670`.
    text: String,
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct InForceInput {
    /// Exact object; give either this or `law_name`.
    object: Option<ObjectId>,
    /// National statute name or abbreviation; give either this or `object`.
    law_name: Option<String>,
    /// Date as YYYYMMDD.
    date: String,
    /// Optional article such as `제44조` or `44의2`.
    article: Option<String>,
    /// Optional second date (YYYYMMDD) to select for comparison.
    compare_date: Option<String>,
}

pub(crate) fn output<T>(structured: T) -> ToolOutput<T> {
    ToolOutput {
        structured,
        text: Some(
            "Legal reference result; matches, statuses and provenance are in structuredContent."
                .into(),
        ),
        meta: None,
    }
}

pub(crate) fn code(error: DatabaseError) -> String {
    serde_json::to_value(error)
        .ok()
        .and_then(|v| v.as_str().map(str::to_string))
        .unwrap_or_else(|| "unavailable".into())
}

pub(crate) fn valid_text(value: &str, max: usize) -> bool {
    !value.trim().is_empty()
        && value.len() <= max
        && !value
            .chars()
            .any(|c| c.is_control() && c != '\n' && c != '\r' && c != '\t')
}

/// Title matches keyed by `kr::name_key`.
pub(crate) struct TitleIndex {
    pub(crate) matches: BTreeMap<String, Vec<TitleMatch>>,
    pub(crate) corpus_complete: bool,
    pub(crate) notices: Vec<CollectionNotice>,
}

pub(crate) async fn find_titles(
    lookup: &ReferenceLookup,
    names: &[String],
    datasets: &[Dataset],
    deadline: Instant,
    cancel: &CancellationToken,
) -> Result<TitleIndex, ToolError> {
    let mut wanted: BTreeMap<String, String> = BTreeMap::new();
    for name in names {
        let key = kr::name_key(name);
        if !key.is_empty() {
            wanted.entry(key).or_insert_with(|| kr::title_pattern(name));
        }
    }
    let mut index = TitleIndex {
        matches: BTreeMap::new(),
        corpus_complete: true,
        notices: Vec::new(),
    };
    if wanted.is_empty() {
        return Ok(index);
    }
    let patterns: Vec<String> = wanted.values().cloned().collect();
    let current = lookup
        .find_lines(
            "title",
            datasets.to_vec(),
            &patterns,
            None,
            false,
            deadline,
            cancel.clone(),
        )
        .await
        .map_err(map_error)?;
    index.corpus_complete &= current.corpus_complete;
    index.notices = current.collection_notices;
    for hit in current.hits {
        let key = kr::name_key(&hit.title);
        if !wanted.contains_key(&key) {
            continue;
        }
        let list = index.matches.entry(key).or_default();
        if !list.iter().any(|m| m.object == hit.object) {
            list.push(TitleMatch {
                object: hit.object,
                matched_title: hit.title,
                title_status: TitleStatus::Current,
                current_title: None,
                revision_id: hit.revision_id,
                capture_id: hit.capture_id,
            });
        }
    }
    let missing: Vec<String> = wanted
        .iter()
        .filter(|(key, _)| !index.matches.contains_key(*key))
        .map(|(_, pattern)| pattern.clone())
        .collect();
    if missing.is_empty() {
        return Ok(index);
    }
    let former = lookup
        .find_lines(
            "title",
            datasets.to_vec(),
            &missing,
            None,
            true,
            deadline,
            cancel.clone(),
        )
        .await
        .map_err(map_error)?;
    index.corpus_complete &= former.corpus_complete;
    let mut checked = 0;
    for hit in former.hits {
        let key = kr::name_key(&hit.title);
        if !wanted.contains_key(&key)
            || index
                .matches
                .get(&key)
                .is_some_and(|list| list.iter().any(|m| m.object == hit.object))
        {
            continue;
        }
        if checked == MAX_FORMER_TITLE_OBJECTS {
            index.corpus_complete = false;
            break;
        }
        checked += 1;
        let head = lookup
            .database()
            .get_metadata(
                GetRequest {
                    object: hit.object.clone(),
                    selector: RevisionSelector::Head,
                    fresh_only: false,
                },
                cancel.clone(),
            )
            .await
            .ok();
        let still_current = head.as_ref().is_some_and(|h| kr::name_key(&h.title) == key);
        index.matches.entry(key).or_default().push(TitleMatch {
            object: hit.object,
            matched_title: hit.title,
            title_status: if still_current {
                TitleStatus::Current
            } else {
                TitleStatus::Former
            },
            current_title: head.filter(|_| !still_current).map(|h| h.title),
            revision_id: hit.revision_id,
            capture_id: hit.capture_id,
        });
    }
    Ok(index)
}

pub(crate) fn statute_datasets() -> Vec<Dataset> {
    vec![Dataset::NationalStatute]
}

async fn resolve_name(
    lookup: &ReferenceLookup,
    input: ResolveNameInput,
    deadline: Instant,
    cancel: CancellationToken,
) -> Result<ResolveNameResult, ToolError> {
    if !valid_text(&input.name, kr::MAX_LAW_NAME_BYTES) || input.name.contains('\n') {
        return Err(ToolError::InvalidInput);
    }
    let datasets = if input.datasets.is_empty() {
        statute_datasets()
    } else {
        input.datasets
    };
    let resolution = kr::resolve_law_name(&input.name);
    let index = find_titles(
        lookup,
        std::slice::from_ref(&resolution.resolved),
        &datasets,
        deadline,
        &cancel,
    )
    .await?;
    let matches = index
        .matches
        .get(&kr::name_key(&resolution.resolved))
        .cloned()
        .unwrap_or_default();
    Ok(ResolveNameResult {
        schema_version: 1,
        resolution,
        matches,
        corpus_complete: index.corpus_complete,
        collection_notices: index.notices,
    })
}

/// The law a citation was resolved to before reading the corpus.
#[derive(Clone)]
pub(crate) struct ChosenLaw {
    pub(crate) name: String,
    pub(crate) resolution: Option<LawNameResolution>,
    pub(crate) start: usize,
    pub(crate) inherited: bool,
    pub(crate) candidates: Vec<String>,
    pub(crate) unresolved: bool,
}

pub(crate) fn resolution_for(
    cache: &mut HashMap<String, LawNameResolution>,
    name: &str,
) -> LawNameResolution {
    cache
        .entry(name.to_string())
        .or_insert_with(|| kr::resolve_law_name(name))
        .clone()
}

/// Assign a law to every extracted citation in order. Returns the names whose titles
/// still need a lookup; the caller looks them up and runs the pass again.
pub(crate) fn choose_laws(
    citations: &[kr::ExtractedStatute],
    index: &BTreeMap<String, Vec<TitleMatch>>,
    looked_up: &std::collections::BTreeSet<String>,
    resolutions: &mut HashMap<String, LawNameResolution>,
) -> (Vec<ChosenLaw>, Vec<String>) {
    let mut chosen = Vec::with_capacity(citations.len());
    let mut pending = Vec::new();
    let mut last: Option<ChosenLaw> = None;
    for citation in citations {
        let law = match &citation.law {
            LawReference::Named {
                candidates,
                generic_tail,
            } => {
                let found = candidates.iter().find(|(name, _)| {
                    let resolved = resolution_for(resolutions, name).resolved;
                    index.contains_key(&kr::name_key(&resolved))
                });
                match found {
                    Some((name, start)) => ChosenLaw {
                        name: name.clone(),
                        resolution: Some(resolution_for(resolutions, name)),
                        start: *start,
                        inherited: false,
                        candidates: Vec::new(),
                        unresolved: false,
                    },
                    None => {
                        let (name, start) = candidates
                            .last()
                            .cloned()
                            .unwrap_or_else(|| (String::new(), citation.article_start));
                        let bracketed = candidates.len() == 1;
                        ChosenLaw {
                            resolution: (!*generic_tail)
                                .then(|| resolution_for(resolutions, &name)),
                            name,
                            start: if bracketed {
                                start
                            } else {
                                citation.article_start
                            },
                            inherited: false,
                            candidates: if bracketed {
                                Vec::new()
                            } else {
                                candidates.iter().map(|(n, _)| n.clone()).collect()
                            },
                            unresolved: *generic_tail,
                        }
                    }
                }
            }
            LawReference::Same { suffix, start } => match &last {
                Some(previous) if !previous.unresolved => {
                    let name = match suffix {
                        Some(suffix) => format!("{} {suffix}", kr::base_law_name(&previous.name)),
                        None => previous.name.clone(),
                    };
                    let resolution = match suffix {
                        Some(_) => resolution_for(resolutions, &name),
                        None => previous
                            .resolution
                            .clone()
                            .unwrap_or_else(|| resolution_for(resolutions, &name)),
                    };
                    if !looked_up.contains(&kr::name_key(&resolution.resolved)) {
                        pending.push(resolution.resolved.clone());
                    }
                    ChosenLaw {
                        name,
                        resolution: Some(resolution),
                        start: *start,
                        inherited: true,
                        candidates: Vec::new(),
                        unresolved: false,
                    }
                }
                _ => ChosenLaw {
                    name: String::new(),
                    resolution: None,
                    start: *start,
                    inherited: true,
                    candidates: Vec::new(),
                    unresolved: true,
                },
            },
            LawReference::Continued => match &last {
                Some(previous) => ChosenLaw {
                    start: citation.article_start,
                    inherited: true,
                    ..previous.clone()
                },
                None => ChosenLaw {
                    name: String::new(),
                    resolution: None,
                    start: citation.article_start,
                    inherited: true,
                    candidates: Vec::new(),
                    unresolved: true,
                },
            },
        };
        last = Some(law.clone());
        chosen.push(law);
    }
    (chosen, pending)
}

type CachedRead = Result<Arc<GetResult>, DatabaseError>;

/// A few recently read captures; citations often repeat one law.
struct CaptureCache {
    entries: Vec<((ObjectId, String), CachedRead)>,
}
impl CaptureCache {
    async fn read(
        &mut self,
        lookup: &ReferenceLookup,
        object: &ObjectId,
        selector: RevisionSelector,
        cancel: &CancellationToken,
    ) -> Result<Arc<GetResult>, DatabaseError> {
        let key = (
            object.clone(),
            serde_json::to_string(&selector).unwrap_or_default(),
        );
        if let Some((_, cached)) = self.entries.iter().find(|(k, _)| *k == key) {
            return cached.clone();
        }
        let result = lookup
            .database()
            .get(
                GetRequest {
                    object: object.clone(),
                    selector,
                    fresh_only: false,
                },
                cancel.clone(),
            )
            .await
            .map(Arc::new);
        if matches!(result, Err(DatabaseError::Cancelled)) {
            return result;
        }
        if self.entries.len() == MAX_CACHED_CAPTURES {
            let _evicted = self.entries.remove(0);
        }
        self.entries.push((key, result.clone()));
        result
    }
}

fn range(first: Option<ArticleNumber>, last: Option<ArticleNumber>) -> Option<String> {
    Some(format!("{}~{}", first?, last?))
}

async fn check_statute(
    lookup: &ReferenceLookup,
    cache: &mut CaptureCache,
    matches: &[TitleMatch],
    citation: &kr::ExtractedStatute,
    result: &mut StatuteCitationResult,
    cancel: &CancellationToken,
) -> Result<(), ToolError> {
    let target = match pick_match(matches) {
        Ok(one) => one,
        Err(0) => {
            result.status = StatuteCitationStatus::LawNotObserved;
            return Ok(());
        }
        Err(many) => {
            result.status = StatuteCitationStatus::LawAmbiguous;
            result.detail = Some(format!("{many}_objects"));
            return Ok(());
        }
    };
    result.object = Some(target.object.clone());
    result.law_title = Some(target.matched_title.clone());
    result.law_title_status = Some(target.title_status);
    let selector = match target.title_status {
        TitleStatus::Current => RevisionSelector::Head,
        TitleStatus::Former => RevisionSelector::Capture {
            id: target.capture_id.clone(),
        },
    };
    let read = match cache.read(lookup, &target.object, selector, cancel).await {
        Ok(read) => read,
        Err(DatabaseError::Cancelled) => return Err(ToolError::Unavailable),
        Err(error) => {
            result.status = StatuteCitationStatus::Unavailable;
            result.detail = Some(code(error));
            return Ok(());
        }
    };
    let record = &read.capture.record;
    result.revision_id = Some(record.revision_id.clone());
    result.capture_id = Some(read.capture.capture_id.clone());
    let article = match kr::locate_article(&record.sections, citation.article) {
        ArticleLookup::Found(article) => article,
        ArticleLookup::NotFound { first, last } => {
            result.status = StatuteCitationStatus::ArticleNotFound;
            result.article_range = range(first, last);
            return Ok(());
        }
    };
    result.retained_article_title = Some(article.title.clone()).filter(|t| !t.is_empty());
    result.paragraph_count = article.paragraphs.iter().copied().max();
    result.status = if article.deleted {
        StatuteCitationStatus::ArticleDeleted
    } else if citation.paragraph.is_some_and(|p| {
        if article.paragraphs.is_empty() {
            p != 1
        } else {
            !article.paragraphs.contains(&p)
        }
    }) {
        StatuteCitationStatus::ParagraphNotFound
    } else if citation
        .subparagraph
        .is_some_and(|s| !kr::has_subparagraph(&article, citation.paragraph, s))
    {
        StatuteCitationStatus::SubparagraphNotFound
    } else {
        result.title_similarity = citation
            .cited_title
            .as_deref()
            .and_then(|cited| kr::title_similarity(cited, &article.title));
        if result
            .title_similarity
            .is_some_and(|s| s < TITLE_MATCH_THRESHOLD)
        {
            StatuteCitationStatus::TitleMismatch
        } else {
            StatuteCitationStatus::Verified
        }
    };
    Ok(())
}

async fn verify(
    lookup: &ReferenceLookup,
    input: VerifyInput,
    deadline: Instant,
    cancel: CancellationToken,
) -> Result<CitationVerification, ToolError> {
    if !valid_text(&input.text, kr::MAX_CITATION_TEXT_BYTES) {
        return Err(ToolError::InvalidInput);
    }
    let text = input.text;
    let extraction = kr::extract_citations(&text);
    let mut resolutions = HashMap::new();
    let mut names = Vec::new();
    for citation in &extraction.statutes {
        if let LawReference::Named { candidates, .. } = &citation.law {
            for (name, _) in candidates {
                names.push(resolution_for(&mut resolutions, name).resolved);
            }
        }
    }
    let datasets = statute_datasets();
    let mut index = find_titles(lookup, &names, &datasets, deadline, &cancel).await?;
    let mut looked_up: std::collections::BTreeSet<String> =
        names.iter().map(|n| kr::name_key(n)).collect();
    let (mut chosen, pending) = choose_laws(
        &extraction.statutes,
        &index.matches,
        &looked_up,
        &mut resolutions,
    );
    if !pending.is_empty() {
        let more = find_titles(lookup, &pending, &datasets, deadline, &cancel).await?;
        index.corpus_complete &= more.corpus_complete;
        index.matches.extend(more.matches);
        looked_up.extend(pending.iter().map(|n| kr::name_key(n)));
        chosen = choose_laws(
            &extraction.statutes,
            &index.matches,
            &looked_up,
            &mut resolutions,
        )
        .0;
    }
    let mut cache = CaptureCache {
        entries: Vec::new(),
    };
    let mut statutes = Vec::with_capacity(chosen.len());
    let mut summary = CitationSummary::default();
    for (citation, law) in extraction.statutes.iter().zip(chosen) {
        let mut result = StatuteCitationResult {
            text: text[law.start..citation.byte_end].to_string(),
            byte_start: law.start,
            byte_end: citation.byte_end,
            law_name: Some(law.name.clone()).filter(|n| !n.is_empty()),
            inherited: law.inherited,
            law_name_candidates: law.candidates.clone(),
            resolution: law.resolution.clone(),
            article: citation.article.to_string(),
            paragraph: citation.paragraph,
            subparagraph: citation.subparagraph,
            cited_article_title: citation.cited_title.clone(),
            status: StatuteCitationStatus::LawNameUnresolved,
            detail: None,
            object: None,
            law_title: None,
            law_title_status: None,
            revision_id: None,
            capture_id: None,
            retained_article_title: None,
            title_similarity: None,
            article_range: None,
            paragraph_count: None,
        };
        if !law.unresolved
            && let Some(resolution) = &law.resolution
        {
            let matches = index
                .matches
                .get(&kr::name_key(&resolution.resolved))
                .cloned()
                .unwrap_or_default();
            check_statute(lookup, &mut cache, &matches, citation, &mut result, &cancel).await?;
        }
        match result.status {
            StatuteCitationStatus::Verified => summary.verified += 1,
            StatuteCitationStatus::ArticleNotFound
            | StatuteCitationStatus::ArticleDeleted
            | StatuteCitationStatus::ParagraphNotFound
            | StatuteCitationStatus::SubparagraphNotFound
            | StatuteCitationStatus::TitleMismatch => summary.failed += 1,
            _ => summary.unchecked += 1,
        }
        statutes.push(result);
    }
    let patterns: Vec<String> = extraction
        .cases
        .iter()
        .map(|case| {
            let mut literal = String::new();
            for c in case.case_number.chars() {
                kr::push_escaped(&mut literal, c);
            }
            format!("(?:.*[^0-9])?{literal}(?:[^0-9].*)?")
        })
        .collect();
    let case_hits = lookup
        .find_lines(
            "case_number",
            vec![Dataset::Precedent, Dataset::ConstitutionalDecision],
            &patterns,
            None,
            false,
            deadline,
            cancel.clone(),
        )
        .await
        .map_err(map_error)?;
    index.corpus_complete &= case_hits.corpus_complete;
    if !extraction.cases.is_empty() {
        index.notices = case_hits.collection_notices.clone();
    }
    let mut cases = Vec::with_capacity(extraction.cases.len());
    for case in &extraction.cases {
        let mut matches: Vec<CaseRecordMatch> = Vec::new();
        for hit in &case_hits.hits {
            if contains_case_number(&hit.text, &case.case_number)
                && !matches.iter().any(|m| m.object == hit.object)
            {
                matches.push(CaseRecordMatch {
                    object: hit.object.clone(),
                    title: hit.title.clone(),
                    revision_id: hit.revision_id.clone(),
                    capture_id: hit.capture_id.clone(),
                });
            }
        }
        let status = if matches.is_empty() {
            summary.cases_not_observed += 1;
            CaseCitationStatus::NotObserved
        } else {
            summary.cases_observed += 1;
            CaseCitationStatus::Observed
        };
        cases.push(CaseCitationResult {
            text: text[case.byte_start..case.byte_end].to_string(),
            byte_start: case.byte_start,
            byte_end: case.byte_end,
            case_number: case.case_number.clone(),
            status,
            matches,
        });
    }
    Ok(CitationVerification {
        schema_version: 1,
        statutes,
        cases,
        summary,
        truncated: extraction.truncated,
        corpus_complete: index.corpus_complete,
        collection_notices: index.notices,
    })
}

pub(crate) fn contains_case_number(line: &str, number: &str) -> bool {
    line.match_indices(number).any(|(at, _)| {
        let before = line[..at].chars().next_back();
        let after = line[at + number.len()..].chars().next();
        !before.is_some_and(|c| c.is_ascii_digit()) && !after.is_some_and(|c| c.is_ascii_digit())
    })
}

fn article_text(read: &GetResult, wanted: ArticleNumber) -> Option<ArticleText> {
    let record = &read.capture.record;
    let ArticleLookup::Found(article) = kr::locate_article(&record.sections, wanted) else {
        return None;
    };
    let mut end = article.text.len().min(ARTICLE_TEXT_LIMIT);
    while !article.text.is_char_boundary(end) {
        end -= 1;
    }
    Some(ArticleText {
        revision_id: record.revision_id.clone(),
        capture_id: read.capture.capture_id.clone(),
        title: article.title.clone(),
        text: article.text[..end].to_string(),
        truncated: end < article.text.len(),
        deleted: article.deleted,
    })
}

fn same_text(a: &str, b: &str) -> bool {
    a.split_whitespace().eq(b.split_whitespace())
}

pub(crate) async fn selection_for(
    lookup: &ReferenceLookup,
    object: &ObjectId,
    entries: &[openlegal_domain::legal::HistoryEntry],
    complete: bool,
    date: &str,
    cancel: &CancellationToken,
) -> Result<InForceSelection, ToolError> {
    let mut selection = select_in_force(entries, complete, date);
    if let Some(selected) = &selection.selected {
        match lookup
            .database()
            .get_metadata(
                GetRequest {
                    object: object.clone(),
                    selector: RevisionSelector::Revision {
                        id: selected.revision_id.clone(),
                    },
                    fresh_only: false,
                },
                cancel.clone(),
            )
            .await
        {
            Ok(metadata) => {
                if let Some(dates) = metadata.metadata.get("provision_effective_dates") {
                    selection.later_provision_dates = later_dates(dates, date);
                    if !selection.later_provision_dates.is_empty() {
                        selection
                            .warnings
                            .push("provision_dates_after_date".to_string());
                    }
                }
            }
            Err(DatabaseError::Cancelled) => return Err(ToolError::Unavailable),
            Err(error) => selection
                .warnings
                .push(format!("selected_metadata_{}", code(error))),
        }
    }
    Ok(selection)
}

/// Pick the single retained object a tool targets: the supplied object, or the one
/// object in `datasets` whose current (or, failing that, former) title matches a name.
pub(crate) async fn resolve_object(
    lookup: &ReferenceLookup,
    object: Option<ObjectId>,
    law_name: Option<String>,
    datasets: &[Dataset],
    deadline: Instant,
    cancel: &CancellationToken,
) -> Result<(ObjectId, Option<LawNameResolution>), ToolError> {
    let (object, resolution) = match (object, law_name) {
        (Some(object), None) => (object, None),
        (None, Some(name)) => {
            if !valid_text(&name, kr::MAX_LAW_NAME_BYTES) || name.contains('\n') {
                return Err(ToolError::InvalidInput);
            }
            let resolution = kr::resolve_law_name(&name);
            let index = find_titles(
                lookup,
                std::slice::from_ref(&resolution.resolved),
                datasets,
                deadline,
                cancel,
            )
            .await?;
            let matches = index
                .matches
                .get(&kr::name_key(&resolution.resolved))
                .cloned()
                .unwrap_or_default();
            match pick_match(&matches) {
                Ok(one) => (one.object.clone(), Some(resolution)),
                Err(0) => return Err(ToolError::NotObserved),
                Err(_) => return Err(ToolError::Ambiguous),
            }
        }
        _ => return Err(ToolError::InvalidInput),
    };
    object.validate().map_err(map_error)?;
    Ok((object, resolution))
}

/// The single current match, or the single former match when none is current;
/// otherwise the number of candidates. National statutes take precedence over
/// administrative rules and ordinances with the same title.
pub(crate) fn pick_match(matches: &[TitleMatch]) -> Result<&TitleMatch, usize> {
    let statute = |m: &&TitleMatch| m.object.dataset == Dataset::NationalStatute;
    let pool: Vec<&TitleMatch> = if matches.iter().any(|m| statute(&m)) {
        matches.iter().filter(statute).collect()
    } else {
        matches.iter().collect()
    };
    let current: Vec<&TitleMatch> = pool
        .iter()
        .copied()
        .filter(|m| m.title_status == TitleStatus::Current)
        .collect();
    let pool = if current.is_empty() { pool } else { current };
    match pool.as_slice() {
        [one] => Ok(one),
        many => Err(many.len()),
    }
}

async fn in_force_at(
    lookup: &ReferenceLookup,
    input: InForceInput,
    deadline: Instant,
    cancel: CancellationToken,
) -> Result<InForceResult, ToolError> {
    if !valid_date(&input.date)
        || input
            .compare_date
            .as_deref()
            .is_some_and(|d| !valid_date(d))
        || input.object.is_some() == input.law_name.is_some()
    {
        return Err(ToolError::InvalidInput);
    }
    let article = match &input.article {
        Some(value) => Some(kr::parse_article_number(value).ok_or(ToolError::InvalidInput)?),
        None => None,
    };
    let (object, resolution) = resolve_object(
        lookup,
        input.object,
        input.law_name,
        &statute_datasets(),
        deadline,
        &cancel,
    )
    .await?;
    object.validate().map_err(map_error)?;
    if !object.dataset.has_provider_revisions() {
        return Err(ToolError::UnsupportedHistory);
    }
    let inventory = lookup
        .revisions(object.clone(), cancel.clone())
        .await
        .map_err(map_error)?;
    let selection = selection_for(
        lookup,
        &object,
        &inventory.entries,
        inventory.complete,
        &input.date,
        &cancel,
    )
    .await?;
    let compare = match &input.compare_date {
        Some(date) => Some(
            selection_for(
                lookup,
                &object,
                &inventory.entries,
                inventory.complete,
                date,
                &cancel,
            )
            .await?,
        ),
        None => None,
    };
    let head = lookup
        .database()
        .get(
            GetRequest {
                object: object.clone(),
                selector: RevisionSelector::Head,
                fresh_only: false,
            },
            cancel.clone(),
        )
        .await;
    if matches!(head, Err(DatabaseError::Cancelled)) {
        return Err(ToolError::Unavailable);
    }
    let mut cache = CaptureCache {
        entries: Vec::new(),
    };
    let law_title = match &head {
        Ok(head) => head.capture.record.title.clone(),
        Err(error) => match &selection.selected {
            Some(selected) => cache
                .read(
                    lookup,
                    &object,
                    RevisionSelector::Revision {
                        id: selected.revision_id.clone(),
                    },
                    &cancel,
                )
                .await
                .map(|r| r.capture.record.title.clone())
                .map_err(map_error)?,
            None => return Err(map_error(*error)),
        },
    };
    let article = match article {
        Some(wanted) => {
            let at_date = match &selection.selected {
                Some(selected) => cache
                    .read(
                        lookup,
                        &object,
                        RevisionSelector::Revision {
                            id: selected.revision_id.clone(),
                        },
                        &cancel,
                    )
                    .await
                    .ok()
                    .and_then(|read| article_text(&read, wanted)),
                None => None,
            };
            let head = head
                .as_ref()
                .ok()
                .and_then(|read| article_text(read, wanted));
            Some(ArticleAtDate {
                article: wanted.to_string(),
                changed_since: match (&at_date, &head) {
                    (Some(a), Some(b)) => Some(!same_text(&a.text, &b.text)),
                    _ => None,
                },
                at_date,
                head,
            })
        }
        None => None,
    };
    let (diff_before, diff_after) = match (&compare, &input.compare_date) {
        (Some(other), Some(other_date)) => match (&selection.selected, &other.selected) {
            (Some(a), Some(b)) => {
                let (before, after) = if input.date.as_str() <= other_date.as_str() {
                    (a, b)
                } else {
                    (b, a)
                };
                (
                    Some(RevisionSelector::Revision {
                        id: before.revision_id.clone(),
                    }),
                    Some(RevisionSelector::Revision {
                        id: after.revision_id.clone(),
                    }),
                )
            }
            _ => (None, None),
        },
        _ => (None, None),
    };
    Ok(InForceResult {
        schema_version: 1,
        object,
        law_title,
        resolution,
        selection,
        article,
        compare,
        diff_before,
        diff_after,
        basis: BASIS.into(),
    })
}

impl ToolModule for LegalReferenceTools {
    fn register(self, registry: &mut ToolRegistry) -> Result<(), ServerError> {
        let lookup = self.lookup.clone();
        registry.register_typed::<ResolveNameInput, ResolveNameResult, _, _>(
            "law.resolve_name",
            "Resolve a Korean law name or common abbreviation (for example 산안법, 중처법 시행령, 개인정보보호법) to retained corpus objects. Spacing and middle-dot variants are ignored; any alias expansion is reported in resolution. Matches carry object identity and whether the title is current or only appears in a retained historical capture. No match means the corpus has no retained object with that title, not that the law does not exist.",
            ToolOptions::default(),
            move |input, ctx| {
                let lookup = lookup.clone();
                async move {
                    resolve_name(&lookup, input, ctx.deadline.into_std(), ctx.request.cancellation)
                        .await
                        .map(output)
                }
            },
        )?;
        let lookup = self.lookup.clone();
        registry.register_typed::<VerifyInput, CitationVerification, _, _>(
            "citation.verify",
            "Check Korean statute citations (「법령명」 제N조제M항제K호, 같은 법 시행령, abbreviations) and court case numbers (2007다27670, 2016헌마123) in supplied text against the retained corpus. Each statute citation reports verified, article_not_found (with the retained article range), article_deleted, paragraph_not_found, subparagraph_not_found, title_mismatch, law_not_observed, law_ambiguous, law_name_unresolved or unavailable, with the checked capture. Current titles are checked at HEAD; a former title is checked in the capture that carried it. Case numbers report observed or not_observed. Not observed never means nonexistent. Input is limited to 50,000 bytes, 50 statute citations and 30 case numbers.",
            ToolOptions::default(),
            move |input, ctx| {
                let lookup = lookup.clone();
                async move {
                    verify(&lookup, input, ctx.deadline.into_std(), ctx.request.cancellation)
                        .await
                        .map(output)
                }
            },
        )?;
        let lookup = self.lookup;
        registry.register_typed::<InForceInput, InForceResult, _, _>(
            "law.in_force_at",
            "Select the retained revision of a national statute, administrative rule or ordinance whose effective date is the latest on or before a date (YYYYMMDD). Give an object or a national statute law_name. Reports determined or provisional (incomplete inventory), not_yet_effective or undetermined, the next change, same-day alternatives and provision-level dates after the date. Optionally returns one article at that revision and at HEAD, and a compare_date selection with database.diff selectors. This is not a legal-applicability ruling; supplementary provisions and transitional rules can change the applicable text.",
            ToolOptions::default(),
            move |input, ctx| {
                let lookup = lookup.clone();
                async move {
                    in_force_at(&lookup, input, ctx.deadline.into_std(), ctx.request.cancellation)
                        .await
                        .map(output)
                }
            },
        )?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_corpus::{Record, Stored, article, deadline, object};

    #[allow(clippy::too_many_arguments)]
    fn record(
        object: ObjectId,
        n: u64,
        revision: &'static str,
        title: &'static str,
        effective: Option<&'static str>,
        sections: Vec<openlegal_domain::legal::LegalSection>,
        metadata: &'static [(&'static str, &'static str)],
        head: bool,
    ) -> Stored {
        Record {
            object,
            n,
            revision,
            title,
            effective,
            sections,
            metadata,
            head,
        }
        .build()
    }

    fn lookup() -> ReferenceLookup {
        let civil = object(Dataset::NationalStatute, "1");
        let civil_2023 = vec![
            article("0003001", "", "제3조 삭제 <2020. 1. 1.>"),
            article(
                "0750001",
                "불법행위의 내용",
                "제750조(불법행위의 내용) 고의 또는 과실로 손해를 가한 자는 배상한다.",
            ),
            article(
                "0751001",
                "재산 이외의 손해의 배상",
                "제751조(재산 이외의 손해의 배상)\n① 첫째 항\n② 둘째 항\n1. 첫째 호",
            ),
        ];
        let mut civil_2020 = civil_2023.clone();
        civil_2020[1].text = "제750조(불법행위의 내용) 과실로 손해를 가한 자는 배상한다.".into();
        let renamed = object(Dataset::NationalStatute, "3");
        crate::test_corpus::lookup(vec![
            record(
                civil.clone(),
                1,
                "100:20200101",
                "민법",
                Some("20200101"),
                civil_2020,
                &[],
                false,
            ),
            record(
                civil,
                2,
                "200:20230601",
                "민법",
                Some("20230601"),
                civil_2023,
                &[("provision_effective_dates", "20230601,20240101")],
                true,
            ),
            record(
                object(Dataset::NationalStatute, "2"),
                3,
                "300:20240101",
                "산업안전보건법 시행령",
                Some("20240101"),
                vec![article(
                    "0005002",
                    "적용 범위",
                    "제5조의2(적용 범위) 적용한다.",
                )],
                &[],
                true,
            ),
            record(
                renamed.clone(),
                4,
                "400:20100101",
                "구 시험법",
                Some("20100101"),
                vec![article("0002001", "정의", "제2조(정의) 옛 정의")],
                &[],
                false,
            ),
            record(
                renamed,
                5,
                "500:20200101",
                "새 시험법",
                Some("20200101"),
                vec![article("0001001", "목적", "제1조(목적) 새 목적")],
                &[],
                true,
            ),
            record(
                object(Dataset::NationalStatute, "4"),
                6,
                "600:20200101",
                "쌍둥이법",
                Some("20200101"),
                vec![],
                &[],
                true,
            ),
            record(
                object(Dataset::NationalStatute, "5"),
                7,
                "700:20200101",
                "쌍둥이법",
                Some("20200101"),
                vec![],
                &[],
                true,
            ),
            record(
                object(Dataset::Precedent, "9"),
                8,
                "9",
                "손해배상",
                None,
                vec![],
                &[("case_number", "2007다27670,27687")],
                true,
            ),
        ])
    }

    #[test]
    fn registers_read_only_tools_with_titles_and_schemas() {
        let mut registry = ToolRegistry::new();
        registry
            .register_module(LegalReferenceTools {
                lookup: Arc::new(lookup()),
            })
            .unwrap();
        for (name, title) in [
            ("law.resolve_name", "Resolve Korean law name"),
            ("citation.verify", "Verify legal citations"),
            ("law.in_force_at", "Find law version in force on a date"),
        ] {
            let tool = &registry.tools[name].definition;
            assert_eq!(tool.title.as_deref(), Some(title));
            let annotations = tool.annotations.as_ref().unwrap();
            assert_eq!(annotations.read_only_hint, Some(true));
            assert!(tool.output_schema.is_some());
        }
    }

    #[tokio::test]
    async fn resolves_aliases_current_and_former_titles() {
        let lookup = lookup();
        let found = resolve_name(
            &lookup,
            ResolveNameInput {
                name: "산안법 시행령".into(),
                datasets: vec![],
            },
            deadline(),
            CancellationToken::new(),
        )
        .await
        .unwrap();
        assert_eq!(found.resolution.matched_alias.as_deref(), Some("산안법"));
        assert_eq!(found.matches.len(), 1);
        assert_eq!(found.matches[0].object.id, "2");
        assert_eq!(found.matches[0].title_status, TitleStatus::Current);
        let former = resolve_name(
            &lookup,
            ResolveNameInput {
                name: "구시험법".into(),
                datasets: vec![],
            },
            deadline(),
            CancellationToken::new(),
        )
        .await
        .unwrap();
        assert_eq!(former.matches[0].title_status, TitleStatus::Former);
        assert_eq!(
            former.matches[0].current_title.as_deref(),
            Some("새 시험법")
        );
        let none = resolve_name(
            &lookup,
            ResolveNameInput {
                name: "없는법".into(),
                datasets: vec![],
            },
            deadline(),
            CancellationToken::new(),
        )
        .await
        .unwrap();
        assert!(none.matches.is_empty() && none.corpus_complete);
        for bad in ["", "a\nb", &"가".repeat(200)] {
            let input = ResolveNameInput {
                name: bad.into(),
                datasets: vec![],
            };
            assert!(matches!(
                resolve_name(&lookup, input, deadline(), CancellationToken::new()).await,
                Err(ToolError::InvalidInput)
            ));
        }
    }

    #[tokio::test]
    async fn verifies_statute_and_case_citations() {
        let lookup = lookup();
        let text = "민법 제750조(불법행위의 내용), 같은 법 제751조제2항제1호 및 제9999조. \
                    민법 제750조(계약해제)와 민법 제3조, 민법 제751조제3항. \
                    산안법 시행령 제5조의2, 「구 시험법」 제2조, 쌍둥이법 제1조, 가상법 제1조, \
                    이 법 제2조. 대법원 2007다27670 판결과 2018도14262.";
        let result = verify(
            &lookup,
            VerifyInput { text: text.into() },
            deadline(),
            CancellationToken::new(),
        )
        .await
        .unwrap();
        let statuses: Vec<StatuteCitationStatus> =
            result.statutes.iter().map(|s| s.status).collect();
        use StatuteCitationStatus::*;
        assert_eq!(
            statuses,
            [
                Verified,
                Verified,
                ArticleNotFound,
                TitleMismatch,
                ArticleDeleted,
                ParagraphNotFound,
                Verified,
                Verified,
                LawAmbiguous,
                LawNotObserved,
                LawNameUnresolved,
            ]
        );
        let first = &result.statutes[0];
        assert_eq!(first.text, "민법 제750조(불법행위의 내용)");
        assert_eq!(&text[first.byte_start..first.byte_end], first.text);
        assert_eq!(first.title_similarity, Some(100));
        assert_eq!(first.revision_id.as_deref(), Some("200:20230601"));
        assert!(result.statutes[1].inherited);
        assert_eq!(result.statutes[1].law_name.as_deref(), Some("민법"));
        assert_eq!(
            result.statutes[2].article_range.as_deref(),
            Some("제3조~제751조")
        );
        assert_eq!(result.statutes[3].title_similarity, Some(0));
        assert_eq!(result.statutes[5].paragraph_count, Some(2));
        assert_eq!(
            result.statutes[6].law_title.as_deref(),
            Some("산업안전보건법 시행령")
        );
        let former = &result.statutes[7];
        assert_eq!(former.law_title_status, Some(TitleStatus::Former));
        assert_eq!(former.revision_id.as_deref(), Some("400:20100101"));
        assert_eq!(result.statutes[8].detail.as_deref(), Some("2_objects"));
        assert_eq!(result.statutes[9].law_name.as_deref(), Some("가상법"));
        assert!(result.statutes[9].law_name_candidates.is_empty());
        assert_eq!(result.summary.verified, 4);
        assert_eq!(result.summary.failed, 4);
        assert_eq!(result.summary.unchecked, 3);
        assert_eq!(result.cases.len(), 2);
        assert_eq!(result.cases[0].status, CaseCitationStatus::Observed);
        assert_eq!(result.cases[0].matches[0].object.id, "9");
        assert_eq!(result.cases[1].status, CaseCitationStatus::NotObserved);
        assert_eq!(result.summary.cases_observed, 1);
        assert!(result.corpus_complete && !result.truncated);
    }

    #[tokio::test]
    async fn selects_revision_in_force_with_article_and_comparison() {
        let lookup = lookup();
        let result = in_force_at(
            &lookup,
            InForceInput {
                object: None,
                law_name: Some("민 법".into()),
                date: "20220101".into(),
                article: Some("750".into()),
                compare_date: Some("20231231".into()),
            },
            deadline(),
            CancellationToken::new(),
        )
        .await
        .unwrap();
        assert_eq!(result.object.id, "1");
        assert_eq!(result.law_title, "민법");
        assert_eq!(result.selection.status, InForceStatus::Determined);
        assert_eq!(
            result.selection.selected.as_ref().unwrap().revision_id,
            "100:20200101"
        );
        assert_eq!(
            result.selection.next_change.as_ref().unwrap().revision_id,
            "200:20230601"
        );
        let article = result.article.unwrap();
        assert_eq!(article.changed_since, Some(true));
        assert!(article.at_date.unwrap().text.contains("과실로 손해"));
        assert!(article.head.unwrap().text.contains("고의 또는 과실"));
        let compare = result.compare.unwrap();
        assert_eq!(compare.selected.unwrap().revision_id, "200:20230601");
        assert_eq!(compare.later_provision_dates, ["20240101"]);
        assert!(
            compare
                .warnings
                .contains(&"provision_dates_after_date".to_string())
        );
        assert_eq!(
            result.diff_before,
            Some(RevisionSelector::Revision {
                id: "100:20200101".into()
            })
        );
        assert_eq!(
            result.diff_after,
            Some(RevisionSelector::Revision {
                id: "200:20230601".into()
            })
        );
        let early = in_force_at(
            &lookup,
            InForceInput {
                object: Some(object(Dataset::NationalStatute, "1")),
                law_name: None,
                date: "19991231".into(),
                article: None,
                compare_date: None,
            },
            deadline(),
            CancellationToken::new(),
        )
        .await
        .unwrap();
        assert_eq!(early.selection.status, InForceStatus::NotYetEffective);
        assert!(early.selection.selected.is_none());
        for (object, name, date) in [
            (None, None, "20220101"),
            (
                Some(object(Dataset::NationalStatute, "1")),
                Some("민법"),
                "20220101",
            ),
            (
                Some(object(Dataset::NationalStatute, "1")),
                None,
                "2022-01-01",
            ),
        ] {
            let input = InForceInput {
                object,
                law_name: name.map(Into::into),
                date: date.into(),
                article: None,
                compare_date: None,
            };
            assert!(matches!(
                in_force_at(&lookup, input, deadline(), CancellationToken::new()).await,
                Err(ToolError::InvalidInput)
            ));
        }
        let errors = [
            ("쌍둥이법", ToolError::Ambiguous),
            ("없는법", ToolError::NotObserved),
        ];
        for (name, expected) in errors {
            let input = InForceInput {
                object: None,
                law_name: Some(name.into()),
                date: "20220101".into(),
                article: None,
                compare_date: None,
            };
            assert_eq!(
                in_force_at(&lookup, input, deadline(), CancellationToken::new())
                    .await
                    .err(),
                Some(expected)
            );
        }
        let precedent = InForceInput {
            object: Some(object(Dataset::Precedent, "9")),
            law_name: None,
            date: "20220101".into(),
            article: None,
            compare_date: None,
        };
        assert_eq!(
            in_force_at(&lookup, precedent, deadline(), CancellationToken::new())
                .await
                .err(),
            Some(ToolError::UnsupportedHistory)
        );
    }
}
