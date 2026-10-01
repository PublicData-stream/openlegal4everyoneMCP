//! MCP tools that analyze retained Korean legal texts: batch change watching, title
//! lineage with repeal mentions, citator signals for court decisions, article impact
//! maps and structured article and annex reads. Like the reference tools, results
//! report retained evidence only and never assert provider absence or legal effect.
use crate::{
    ServerError,
    database::map_error,
    legal_reference::{
        ARTICLE_TEXT_LIMIT, choose_laws, code, contains_case_number, find_titles, output,
        pick_match, resolution_for, resolve_object, selection_for, valid_text,
    },
    registry::{ToolError, ToolModule, ToolOptions, ToolRegistry},
};
use openlegal_application::{Clock, SystemClock, legal_reference::ReferenceLookup};
use openlegal_domain::{
    jurisdiction::Jurisdiction,
    legal::{
        DatabaseError, Dataset, GetRequest, HistoryEntry, ObjectId, RevisionSelector, valid_date,
    },
    legal_analysis::*,
    legal_reference::{ArticleNumber, CaseRecordMatch},
};
use openlegal_normalization::kr_legal_reference::{
    self as kr, ArticleLookup, KOREA, LawReference, LocatedArticle, OutlineUnit,
};
use openlegal_normalization::legal_reference::ReferenceProfile;
use schemars::JsonSchema;
use serde::Deserialize;
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::Arc,
    time::Instant,
};
use tokio_util::sync::CancellationToken;

/// Datasets whose objects carry provider revisions and articles.
const REVISIONED: [Dataset; 3] = [
    Dataset::NationalStatute,
    Dataset::AdministrativeRule,
    Dataset::Ordinance,
];
const MAX_WATCH_LAWS: usize = 100;
const MAX_UPCOMING_DETAILS: usize = 5;
const MAX_REPEAL_TITLES: usize = 10;
const MAX_REPEAL_MENTIONS: usize = 20;
const MAX_CITING: usize = 50;
const MAX_CITING_LINES: usize = 3;
const LINE_CHARS: usize = 400;
const MAX_BUCKET_REFERENCES: usize = 20;
const MAX_OUTBOUND: usize = 30;
const MAX_OUTLINE: usize = 600;
const MAX_CONTEXT: u8 = 3;
const MAX_KEYWORD_BYTES: usize = 200;
const READ_BUDGET: usize = 64 * 1024;
const ANNEX_TEXT_LIMIT: usize = 32 * 1024;
const CITATOR_BASIS: &str = "Lists retained decisions whose text cites the case number and flags recognized overruling phrases (변경하기로 한다, 견해를 변경, 더 이상 유지할 수 없 and similar) on those lines. It is a reading aid, not a citator service: a decision can be limited or overruled without quoting its number, and an unrecognized wording or an uncollected decision is not detected.";
/// Inbound reference searches; the corpus filter accepts at most three datasets.
const IMPACT_GROUPS: [&[Dataset]; 3] = [
    &[
        Dataset::Precedent,
        Dataset::ConstitutionalDecision,
        Dataset::LegalInterpretation,
    ],
    &[
        Dataset::NationalStatute,
        Dataset::AdministrativeRule,
        Dataset::Ordinance,
    ],
    &[Dataset::AdministrativeAppeal],
];

pub struct LegalAnalysisTools {
    pub lookup: Arc<ReferenceLookup>,
}

fn default_true() -> bool {
    true
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct WatchTarget {
    /// Law, administrative rule or ordinance name; give either this or `object`.
    law_name: Option<String>,
    /// Exact object; give either this or `law_name`.
    object: Option<ObjectId>,
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct WatchInput {
    /// 1 to 100 laws to check.
    laws: Vec<WatchTarget>,
    /// The `snapshot` returned by an earlier call (`dataset:id` to HEAD revision ID).
    #[serde(default)]
    previous: BTreeMap<String, String>,
    /// Report retained revisions that take effect after today. Defaults to true.
    #[serde(default = "default_true")]
    include_upcoming: bool,
    /// Omit entries that are unchanged and have no upcoming revision.
    #[serde(default)]
    changes_only: bool,
    /// IANA time zone whose calendar date is "today" for upcoming revisions, such as
    /// `Asia/Seoul`, `America/New_York` or `UTC`. Defaults to the KOR zone `Asia/Seoul`.
    timezone: Option<String>,
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct TargetInput {
    /// Exact object; give either this or `law_name`.
    object: Option<ObjectId>,
    /// Law, administrative rule or ordinance name; national statutes win a tie.
    law_name: Option<String>,
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct LineageInput {
    /// Exact object; give either this or `law_name`.
    object: Option<ObjectId>,
    /// Law, administrative rule or ordinance name; national statutes win a tie.
    law_name: Option<String>,
    /// IANA time zone whose calendar date is "today" for upcoming revisions, such as
    /// `Asia/Seoul`, `America/New_York` or `UTC`. Defaults to the KOR zone `Asia/Seoul`.
    timezone: Option<String>,
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct CitingInput {
    /// One court case number, such as `2007다27670` or `2016헌마123`.
    case_number: String,
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ImpactInput {
    /// Exact object; give either this or `law_name`.
    object: Option<ObjectId>,
    /// Law, administrative rule or ordinance name; national statutes win a tie.
    law_name: Option<String>,
    /// Article such as `제750조`, `44의2` or `제9-5조`.
    article: String,
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ArticleReadInput {
    /// Exact object; give either this or `law_name`.
    object: Option<ObjectId>,
    /// Law, administrative rule or ordinance name; national statutes win a tie.
    law_name: Option<String>,
    /// Exact revision; defaults to HEAD. Give at most one of `selector` and `date`.
    selector: Option<RevisionSelector>,
    /// Read the revision in force on this date (YYYYMMDD) instead of HEAD.
    date: Option<String>,
    /// Article such as `제44조`, `44의2` or `제9-5조`.
    article: Option<String>,
    /// Neighbouring articles to include on each side of `article` (0 to 3).
    #[serde(default)]
    context: u8,
    /// Heading such as `제2장` or `제3절`; returns the articles under it.
    chapter: Option<String>,
    /// Returns the articles whose text contains this text, ignoring spacing.
    keyword: Option<String>,
    /// Annex such as `별표 1` or `1의2`.
    annex: Option<String>,
}

fn dataset_code(dataset: Dataset) -> String {
    serde_json::to_value(dataset)
        .ok()
        .and_then(|v| v.as_str().map(str::to_string))
        .unwrap_or_default()
}

fn object_key(object: &ObjectId) -> String {
    format!("{}:{}", dataset_code(object.dataset), object.id)
}

fn dataset_label(dataset: Dataset) -> &'static str {
    match dataset {
        Dataset::NationalStatute => "법령",
        Dataset::AdministrativeRule => "행정규칙",
        Dataset::Ordinance => "자치법규",
        Dataset::Treaty => "조약",
        Dataset::Precedent => "판례",
        Dataset::ConstitutionalDecision => "헌재결정례",
        Dataset::LegalInterpretation => "법령해석례",
        Dataset::AdministrativeAppeal => "행정심판례",
    }
}

/// At most `max` bytes of `text`, cut at a character boundary.
fn clip(text: &str, max: usize) -> (String, bool) {
    let mut end = text.len().min(max);
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    (text[..end].to_string(), end < text.len())
}

fn clip_chars(text: &str, max: usize) -> String {
    let text = text.trim();
    match text.char_indices().nth(max) {
        Some((end, _)) => format!("{}…", &text[..end]),
        None => text.to_string(),
    }
}

fn compact(text: &str) -> String {
    text.chars().filter(|c| !c.is_whitespace()).collect()
}

fn chronological(entries: &[HistoryEntry]) -> Vec<&HistoryEntry> {
    let mut ordered: Vec<&HistoryEntry> = entries.iter().collect();
    ordered.sort_by(|a, b| {
        (&a.effective_date, &a.publication_date, &a.revision_id).cmp(&(
            &b.effective_date,
            &b.publication_date,
            &b.revision_id,
        ))
    });
    ordered.dedup_by(|a, b| a.revision_id == b.revision_id);
    ordered
}

/// The provider amendment type recorded for one revision, if readable.
async fn amendment_type(
    lookup: &ReferenceLookup,
    object: &ObjectId,
    revision_id: &str,
    cancel: &CancellationToken,
) -> Result<Option<String>, ToolError> {
    let result = lookup
        .database()
        .get_metadata(
            GetRequest {
                object: object.clone(),
                selector: RevisionSelector::Revision {
                    id: revision_id.to_string(),
                },
                fresh_only: false,
            },
            cancel.clone(),
        )
        .await;
    match result {
        Ok(metadata) => Ok(metadata.metadata.get("amendment_type").cloned()),
        Err(DatabaseError::Cancelled) => Err(ToolError::Unavailable),
        Err(_) => Ok(None),
    }
}

struct RevisionStatus {
    upcoming: Vec<UpcomingRevision>,
    repeal_status: RepealStatus,
    repeal: Option<RepealRecord>,
    latest_amendment_type: Option<String>,
}

/// Upcoming revisions (earliest first) and the repeal state recorded on the latest
/// retained revision. Repeal is read from the provider amendment type, never inferred.
async fn revision_status(
    lookup: &ReferenceLookup,
    object: &ObjectId,
    entries: &[HistoryEntry],
    today: &str,
    cancel: &CancellationToken,
) -> Result<RevisionStatus, ToolError> {
    let ordered = chronological(entries);
    let mut known: HashMap<&str, Option<String>> = HashMap::new();
    let mut upcoming = Vec::new();
    for entry in ordered
        .iter()
        .filter(|e| e.effective_date.as_deref().is_some_and(|d| d > today))
    {
        let amendment = if upcoming.len() < MAX_UPCOMING_DETAILS {
            let amendment = amendment_type(lookup, object, &entry.revision_id, cancel).await?;
            known.insert(entry.revision_id.as_str(), amendment.clone());
            amendment
        } else {
            None
        };
        upcoming.push(UpcomingRevision {
            revision_id: entry.revision_id.clone(),
            effective_date: entry.effective_date.clone(),
            publication_date: entry.publication_date.clone(),
            amendment_type: amendment,
        });
    }
    let mut status = RevisionStatus {
        upcoming,
        repeal_status: RepealStatus::Unknown,
        repeal: None,
        latest_amendment_type: None,
    };
    let Some(latest) = ordered.last() else {
        return Ok(status);
    };
    status.latest_amendment_type = match known.get(latest.revision_id.as_str()) {
        Some(amendment) => amendment.clone(),
        None => amendment_type(lookup, object, &latest.revision_id, cancel).await?,
    };
    if let Some(value) = &status.latest_amendment_type {
        match RepealKind::from_amendment_type(value) {
            Some(kind) => {
                let scheduled = latest.effective_date.as_deref().is_some_and(|d| d > today);
                status.repeal_status = if scheduled {
                    RepealStatus::RepealScheduled
                } else {
                    RepealStatus::Repealed
                };
                status.repeal = Some(RepealRecord {
                    revision_id: latest.revision_id.clone(),
                    amendment_type: value.clone(),
                    kind,
                    effective_date: latest.effective_date.clone(),
                    publication_date: latest.publication_date.clone(),
                });
            }
            None => status.repeal_status = RepealStatus::NoRepealRecorded,
        }
    }
    Ok(status)
}

fn is_repeal(status: Option<RepealStatus>) -> bool {
    matches!(
        status,
        Some(RepealStatus::Repealed | RepealStatus::RepealScheduled)
    )
}

async fn head_metadata(
    lookup: &ReferenceLookup,
    object: &ObjectId,
    cancel: &CancellationToken,
) -> Result<Result<openlegal_domain::legal::MetadataResult, DatabaseError>, ToolError> {
    let result = lookup
        .database()
        .get_metadata(
            GetRequest {
                object: object.clone(),
                selector: RevisionSelector::Head,
                fresh_only: false,
            },
            cancel.clone(),
        )
        .await;
    match result {
        Err(DatabaseError::Cancelled) => Err(ToolError::Unavailable),
        other => Ok(other),
    }
}

async fn watch(
    lookup: &ReferenceLookup,
    input: WatchInput,
    day: &Today,
    deadline: Instant,
    cancel: CancellationToken,
) -> Result<WatchResult, ToolError> {
    if input.laws.is_empty()
        || input.laws.len() > MAX_WATCH_LAWS
        || input.previous.len() > 2 * MAX_WATCH_LAWS
        || input
            .previous
            .iter()
            .any(|(k, v)| !valid_text(k, 256) || !valid_text(v, 256))
    {
        return Err(ToolError::InvalidInput);
    }
    let mut resolutions = HashMap::new();
    let mut names = Vec::new();
    for target in &input.laws {
        match (&target.law_name, &target.object) {
            (Some(name), None) => {
                if !valid_text(name, kr::MAX_LAW_NAME_BYTES) || name.contains('\n') {
                    return Err(ToolError::InvalidInput);
                }
                names.push(resolution_for(&KOREA, &mut resolutions, name).resolved);
            }
            (None, Some(object)) => object.validate().map_err(map_error)?,
            _ => return Err(ToolError::InvalidInput),
        }
    }
    let index = find_titles(lookup, &KOREA, &names, &REVISIONED, deadline, &cancel).await?;
    let today = day.date.as_str();
    let mut result = WatchResult {
        schema_version: 1,
        today: day.date.clone(),
        timezone: day.timezone.clone(),
        entries: Vec::new(),
        snapshot: BTreeMap::new(),
        changed: 0,
        with_upcoming: 0,
        repealed: 0,
        corpus_complete: index.corpus_complete,
        collection_notices: index.notices.clone(),
    };
    for target in input.laws {
        let mut entry = WatchEntry {
            input: String::new(),
            resolution: None,
            object: None,
            title: None,
            status: WatchStatus::NotObserved,
            detail: None,
            previous_revision_id: None,
            head_revision_id: None,
            effective_date: None,
            publication_date: None,
            amendment_type: None,
            upcoming: Vec::new(),
            repeal_status: None,
            repeal: None,
        };
        let object = match (target.law_name, target.object) {
            (Some(name), None) => {
                let resolution = resolution_for(&KOREA, &mut resolutions, &name);
                entry.input = name;
                let matches = index
                    .matches
                    .get(&kr::name_key(&resolution.resolved))
                    .cloned()
                    .unwrap_or_default();
                entry.resolution = Some(resolution);
                match pick_match(&matches) {
                    Ok(one) => one.object.clone(),
                    Err(count) => {
                        if count > 0 {
                            entry.status = WatchStatus::Ambiguous;
                            entry.detail = Some(format!("{count}_objects"));
                        }
                        result.entries.push(entry);
                        continue;
                    }
                }
            }
            (None, Some(object)) => {
                entry.input = object_key(&object);
                object
            }
            _ => return Err(ToolError::InvalidInput),
        };
        let key = object_key(&object);
        entry.previous_revision_id = input.previous.get(&key).cloned();
        match head_metadata(lookup, &object, &cancel).await? {
            Ok(head) => {
                entry.status = match &entry.previous_revision_id {
                    None => WatchStatus::New,
                    Some(previous) if *previous == head.revision_id => WatchStatus::Unchanged,
                    Some(_) => WatchStatus::Changed,
                };
                result.snapshot.insert(key, head.revision_id.clone());
                entry.title = Some(head.title);
                entry.amendment_type = head.metadata.get("amendment_type").cloned();
                entry.head_revision_id = Some(head.revision_id);
                entry.effective_date = head.effective_date;
                entry.publication_date = head.publication_date;
            }
            Err(error) => {
                entry.status = if error == DatabaseError::NotObserved {
                    WatchStatus::NotObserved
                } else {
                    WatchStatus::Unavailable
                };
                entry.detail = Some(code(error));
                if let Some(previous) = &entry.previous_revision_id {
                    result.snapshot.insert(key, previous.clone());
                }
            }
        }
        // A repealed law can leave the provider's current list, so the revision
        // catalog is read even when HEAD is unavailable.
        if object.dataset.has_provider_revisions() {
            match lookup.revisions(object.clone(), cancel.clone()).await {
                Ok(inventory) if !inventory.entries.is_empty() => {
                    let status =
                        revision_status(lookup, &object, &inventory.entries, today, &cancel)
                            .await?;
                    if input.include_upcoming {
                        entry.upcoming = status.upcoming;
                    }
                    entry.repeal_status = Some(status.repeal_status);
                    entry.repeal = status.repeal;
                }
                Ok(_) => {}
                Err(DatabaseError::Cancelled) => return Err(ToolError::Unavailable),
                Err(error) => {
                    entry
                        .detail
                        .get_or_insert_with(|| format!("revisions_{}", code(error)));
                }
            }
        }
        entry.object = Some(object);
        if entry.status == WatchStatus::Changed {
            result.changed += 1;
        }
        if !entry.upcoming.is_empty() {
            result.with_upcoming += 1;
        }
        if is_repeal(entry.repeal_status) {
            result.repealed += 1;
        }
        if !input.changes_only
            || entry.status != WatchStatus::Unchanged
            || !entry.upcoming.is_empty()
            || is_repeal(entry.repeal_status)
        {
            result.entries.push(entry);
        }
    }
    Ok(result)
}

fn repeal_patterns(title: &str) -> [String; 2] {
    let name = kr::title_pattern(title);
    [
        format!("(?:.*[^가-힣])?「?{name}.*폐지.*"),
        format!(".*폐지.*[^가-힣]「?{name}.*"),
    ]
}

async fn lineage(
    lookup: &ReferenceLookup,
    input: TargetInput,
    day: &Today,
    deadline: Instant,
    cancel: CancellationToken,
) -> Result<LineageResult, ToolError> {
    let today = day.date.as_str();
    let (object, resolution) = resolve_object(
        lookup,
        &KOREA,
        input.object,
        input.law_name,
        &REVISIONED,
        deadline,
        &cancel,
    )
    .await?;
    if !object.dataset.has_provider_revisions() {
        return Err(ToolError::UnsupportedHistory);
    }
    let (head_state, current_title) = match head_metadata(lookup, &object, &cancel).await? {
        Ok(head) => ("published".to_string(), Some(head.title)),
        Err(error) => (code(error), None),
    };
    let inventory = lookup
        .revisions(object.clone(), cancel.clone())
        .await
        .map_err(map_error)?;
    let found = lookup
        .find_lines(
            "title",
            vec![object.dataset],
            &[".+".to_string()],
            Some(&object.id),
            true,
            deadline,
            cancel.clone(),
        )
        .await
        .map_err(map_error)?;
    let mut by_revision: HashMap<&str, &str> = HashMap::new();
    for hit in found.hits.iter().filter(|h| h.object == object) {
        by_revision
            .entry(hit.revision_id.as_str())
            .or_insert(hit.title.as_str());
    }
    let mut titles: Vec<TitlePeriod> = Vec::new();
    let mut revisions_without_title = 0;
    for entry in chronological(&inventory.entries) {
        let Some(title) = by_revision.get(entry.revision_id.as_str()) else {
            revisions_without_title += 1;
            continue;
        };
        match titles.last_mut() {
            Some(last) if kr::name_key(&last.title) == kr::name_key(title) => {
                last.last_revision_id = entry.revision_id.clone();
                last.last_effective_date = entry.effective_date.clone();
            }
            _ => titles.push(TitlePeriod {
                title: title.to_string(),
                first_revision_id: entry.revision_id.clone(),
                first_effective_date: entry.effective_date.clone(),
                last_revision_id: entry.revision_id.clone(),
                last_effective_date: entry.effective_date.clone(),
            }),
        }
    }
    let status = revision_status(lookup, &object, &inventory.entries, today, &cancel).await?;
    let distinct: BTreeSet<String> = titles.iter().map(|t| kr::name_key(&t.title)).collect();
    let mut searched: Vec<&str> = Vec::new();
    for title in current_title
        .iter()
        .map(String::as_str)
        .chain(titles.iter().rev().map(|t| t.title.as_str()))
    {
        if searched.len() < MAX_REPEAL_TITLES
            && !searched
                .iter()
                .any(|s| kr::name_key(s) == kr::name_key(title))
        {
            searched.push(title);
        }
    }
    let patterns: Vec<String> = searched
        .iter()
        .flat_map(|title| repeal_patterns(title))
        .collect();
    let mut corpus_complete = found.corpus_complete;
    let mut collection_notices = found.collection_notices;
    let mut repeal_mentions: Vec<RepealMention> = Vec::new();
    if !patterns.is_empty() {
        let mentions = lookup
            .find_lines(
                "body",
                vec![Dataset::NationalStatute, Dataset::AdministrativeRule],
                &patterns,
                None,
                false,
                deadline,
                cancel.clone(),
            )
            .await
            .map_err(map_error)?;
        corpus_complete &= mentions.corpus_complete;
        collection_notices = mentions.collection_notices;
        for hit in mentions.hits {
            let line = clip_chars(&hit.text, LINE_CHARS);
            if repeal_mentions.len() == MAX_REPEAL_MENTIONS {
                break;
            }
            if !repeal_mentions
                .iter()
                .any(|m| m.object == hit.object && m.line == line)
            {
                repeal_mentions.push(RepealMention {
                    object: hit.object,
                    title: hit.title,
                    line,
                    revision_id: hit.revision_id,
                    capture_id: hit.capture_id,
                });
            }
        }
    }
    Ok(LineageResult {
        schema_version: 1,
        object,
        resolution,
        head_state,
        current_title,
        today: day.date.clone(),
        timezone: day.timezone.clone(),
        renamed: distinct.len() > 1,
        titles,
        upcoming: status.upcoming,
        repeal_status: status.repeal_status,
        repeal: status.repeal,
        latest_amendment_type: status.latest_amendment_type,
        repeal_mentions,
        revisions_without_title,
        inventory_complete: inventory.complete,
        corpus_complete,
        collection_notices,
    })
}

fn case_pattern(number: &str) -> String {
    let mut literal = String::new();
    for c in number.chars() {
        kr::push_escaped(&mut literal, c);
    }
    format!("(?:.*[^0-9])?{literal}(?:[^0-9].*)?")
}

async fn citing(
    lookup: &ReferenceLookup,
    input: CitingInput,
    deadline: Instant,
    cancel: CancellationToken,
) -> Result<CitatorResult, ToolError> {
    if !valid_text(&input.case_number, 128) {
        return Err(ToolError::InvalidInput);
    }
    let number = kr::parse_case_number(&input.case_number).ok_or(ToolError::InvalidInput)?;
    let pattern = vec![case_pattern(&number)];
    let courts = vec![Dataset::Precedent, Dataset::ConstitutionalDecision];
    let found_targets = lookup
        .find_lines(
            "case_number",
            courts.clone(),
            &pattern,
            None,
            false,
            deadline,
            cancel.clone(),
        )
        .await
        .map_err(map_error)?;
    let mut targets: Vec<CaseRecordMatch> = Vec::new();
    for hit in found_targets.hits {
        if contains_case_number(&hit.text, &number)
            && !targets.iter().any(|t| t.object == hit.object)
        {
            targets.push(CaseRecordMatch {
                object: hit.object,
                title: hit.title,
                revision_id: hit.revision_id,
                capture_id: hit.capture_id,
            });
        }
    }
    let found = lookup
        .find_lines(
            "body",
            courts,
            &pattern,
            None,
            false,
            deadline,
            cancel.clone(),
        )
        .await
        .map_err(map_error)?;
    let mut truncated = found_targets.truncated || found.truncated;
    let mut citing: Vec<CitingDecision> = Vec::new();
    for hit in found.hits {
        if targets.iter().any(|t| t.object == hit.object)
            || !contains_case_number(&hit.text, &number)
        {
            continue;
        }
        let position = match citing.iter().position(|c| c.object == hit.object) {
            Some(position) => position,
            None if citing.len() == MAX_CITING => {
                truncated = true;
                continue;
            }
            None => {
                citing.push(CitingDecision {
                    en_banc: compact(&hit.title).contains("전원합의체"),
                    object: hit.object.clone(),
                    title: hit.title.clone(),
                    case_number: None,
                    judgment_date: None,
                    authority: None,
                    overruling_phrase: None,
                    lines: Vec::new(),
                    revision_id: hit.revision_id.clone(),
                    capture_id: hit.capture_id.clone(),
                });
                citing.len() - 1
            }
        };
        let decision = &mut citing[position];
        let line = clip_chars(&hit.text, LINE_CHARS);
        if decision.lines.contains(&line) {
            continue;
        }
        match kr::overruling_phrase(&hit.text) {
            Some(phrase) if decision.overruling_phrase.is_none() => {
                decision.overruling_phrase = Some(phrase.to_string());
                decision.en_banc |= compact(&hit.text).contains("전원합의체");
                decision.lines.insert(0, line);
                decision.lines.truncate(MAX_CITING_LINES);
            }
            _ if decision.lines.len() < MAX_CITING_LINES => decision.lines.push(line),
            _ => {}
        }
    }
    for decision in &mut citing {
        if let Ok(metadata) = head_metadata(lookup, &decision.object, &cancel).await? {
            decision.case_number = metadata.metadata.get("case_number").cloned();
            decision.judgment_date = metadata.metadata.get("judgment_date").cloned();
            decision.authority = metadata.metadata.get("authority").cloned();
        }
    }
    citing.sort_by(|a, b| b.judgment_date.cmp(&a.judgment_date));
    let signal = if citing.iter().any(|c| c.overruling_phrase.is_some()) {
        CitatorSignal::OverrulingLanguageFound
    } else {
        CitatorSignal::NoneFound
    };
    Ok(CitatorResult {
        schema_version: 1,
        case_number: number,
        targets,
        citing,
        signal,
        truncated,
        corpus_complete: found_targets.corpus_complete && found.corpus_complete,
        collection_notices: found.collection_notices,
        basis: CITATOR_BASIS.into(),
    })
}

/// A regular-expression fragment matching an article locator and the text after it,
/// without matching a longer locator such as `제5조의2` for `제5조`.
fn article_pattern(article: ArticleNumber) -> String {
    let mut pattern = format!("제 ?{}", article.number);
    if let Some(part) = article.part {
        pattern.push_str(&format!("-{part}"));
    }
    pattern.push_str(" ?조");
    match article.branch {
        Some(branch) => pattern.push_str(&format!("의 ?{branch}(?:[^0-9].*)?")),
        None => pattern.push_str("(?:의[^0-9].*|[^0-9의].*)?"),
    }
    pattern
}

fn mermaid_label(text: &str) -> String {
    text.replace('"', "#quot;").replace(['\n', '\r'], " ")
}

fn mermaid(center: &str, inbound: &[ImpactBucket], outbound: &[OutboundReference]) -> String {
    let mut out = format!("graph LR\n  A[\"{}\"]\n", mermaid_label(center));
    for (i, bucket) in inbound.iter().enumerate() {
        out.push_str(&format!(
            "  I{i}[\"{} {}건\"] --> A\n",
            dataset_label(bucket.dataset),
            bucket.object_count
        ));
    }
    for (i, reference) in outbound.iter().enumerate() {
        let label = match &reference.law_name {
            Some(name) => format!("{name} {}", reference.article),
            None => reference.article.clone(),
        };
        out.push_str(&format!("  A --> O{i}[\"{}\"]\n", mermaid_label(&label)));
    }
    out
}

/// Citations in an article's own text. Named laws are resolved like `citation.verify`;
/// bare locators and `이 법` references are reported without a law name.
async fn outbound_references(
    lookup: &ReferenceLookup,
    article: &LocatedArticle<'_>,
    deadline: Instant,
    cancel: &CancellationToken,
) -> Result<(Vec<OutboundReference>, bool), ToolError> {
    let text = article.text;
    let extraction = kr::extract_citations(text);
    let mut resolutions = HashMap::new();
    let mut names = Vec::new();
    for citation in &extraction.statutes {
        if let LawReference::Named { candidates, .. } = &citation.law {
            for (name, _) in candidates {
                names.push(resolution_for(&KOREA, &mut resolutions, name).resolved);
            }
        }
    }
    let datasets = KOREA.statute_datasets();
    let mut index = find_titles(lookup, &KOREA, &names, datasets, deadline, cancel).await?;
    let mut looked_up: BTreeSet<String> = names.iter().map(|n| kr::name_key(n)).collect();
    let (mut chosen, pending) = choose_laws(
        &KOREA,
        &extraction.statutes,
        &index.matches,
        &looked_up,
        &mut resolutions,
    );
    if !pending.is_empty() {
        let more = find_titles(lookup, &KOREA, &pending, datasets, deadline, cancel).await?;
        index.matches.extend(more.matches);
        looked_up.extend(pending.iter().map(|n| kr::name_key(n)));
        chosen = choose_laws(
            &KOREA,
            &extraction.statutes,
            &index.matches,
            &looked_up,
            &mut resolutions,
        )
        .0;
    }
    let mut found: Vec<(usize, OutboundReference)> = Vec::new();
    let mut covered: Vec<(usize, usize)> = Vec::new();
    for (citation, law) in extraction.statutes.iter().zip(chosen) {
        covered.push((citation.article_start, citation.byte_end));
        let law_name = match (&law.resolution, law.unresolved || law.name.is_empty()) {
            (Some(resolution), false) => Some(
                index
                    .matches
                    .get(&kr::name_key(&resolution.resolved))
                    .and_then(|m| pick_match(m).ok())
                    .map(|m| m.matched_title.clone())
                    .unwrap_or_else(|| law.name.clone()),
            ),
            _ => None,
        };
        found.push((
            law.start,
            OutboundReference {
                text: text[law.start..citation.byte_end].to_string(),
                law_name,
                article: citation.article.to_string(),
            },
        ));
    }
    for (number, start, end) in kr::article_mentions(text) {
        if start == 0 || covered.iter().any(|(s, e)| start < *e && *s < end) {
            continue;
        }
        found.push((
            start,
            OutboundReference {
                text: text[start..end].to_string(),
                law_name: None,
                article: number.to_string(),
            },
        ));
    }
    found.sort_by_key(|(start, _)| *start);
    let mut references: Vec<OutboundReference> = Vec::new();
    let mut truncated = extraction.truncated;
    for (_, reference) in found {
        let own = reference.law_name.is_none() && reference.article == article.number.to_string();
        if own
            || references
                .iter()
                .any(|r| r.law_name == reference.law_name && r.article == reference.article)
        {
            continue;
        }
        if references.len() == MAX_OUTBOUND {
            truncated = true;
            break;
        }
        references.push(reference);
    }
    Ok((references, truncated))
}

async fn impact(
    lookup: &ReferenceLookup,
    input: ImpactInput,
    deadline: Instant,
    cancel: CancellationToken,
) -> Result<ImpactResult, ToolError> {
    let wanted = kr::parse_article_number(&input.article).ok_or(ToolError::InvalidInput)?;
    let (object, resolution) = resolve_object(
        lookup,
        &KOREA,
        input.object,
        input.law_name,
        &REVISIONED,
        deadline,
        &cancel,
    )
    .await?;
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
        .await
        .map_err(map_error)?;
    let record = &head.capture.record;
    let ArticleLookup::Found(article) = kr::locate_article(&record.sections, wanted) else {
        return Err(ToolError::NotFound);
    };
    let pattern = vec![format!(
        "(?:.*[^가-힣])?「?{}」? ?{}",
        kr::title_pattern(&record.title),
        article_pattern(article.number)
    )];
    let mut inbound: Vec<ImpactBucket> = Vec::new();
    let mut corpus_complete = true;
    let mut truncated = false;
    let mut collection_notices = Vec::new();
    for group in IMPACT_GROUPS {
        let found = lookup
            .find_lines(
                "body",
                group.to_vec(),
                &pattern,
                None,
                false,
                deadline,
                cancel.clone(),
            )
            .await
            .map_err(map_error)?;
        corpus_complete &= found.corpus_complete;
        truncated |= found.truncated;
        collection_notices = found.collection_notices;
        let mut seen: HashMap<Dataset, BTreeSet<String>> = HashMap::new();
        for hit in found.hits {
            if hit.object == object {
                continue;
            }
            let dataset = hit.object.dataset;
            if !seen
                .entry(dataset)
                .or_default()
                .insert(hit.object.id.clone())
            {
                continue;
            }
            let position = match inbound.iter().position(|b| b.dataset == dataset) {
                Some(position) => position,
                None => {
                    inbound.push(ImpactBucket {
                        dataset,
                        object_count: 0,
                        references: Vec::new(),
                    });
                    inbound.len() - 1
                }
            };
            let bucket = &mut inbound[position];
            bucket.object_count += 1;
            if bucket.references.len() < MAX_BUCKET_REFERENCES {
                bucket.references.push(ImpactReference {
                    line: clip_chars(&hit.text, LINE_CHARS),
                    object: hit.object,
                    title: hit.title,
                    revision_id: hit.revision_id,
                    capture_id: hit.capture_id,
                });
            }
        }
    }
    let (outbound, outbound_truncated) =
        outbound_references(lookup, &article, deadline, &cancel).await?;
    let label = article.number.to_string();
    Ok(ImpactResult {
        schema_version: 1,
        mermaid: mermaid(&format!("{} {label}", record.title), &inbound, &outbound),
        object: object.clone(),
        resolution,
        law_title: record.title.clone(),
        article: label,
        article_title: Some(article.title.clone()).filter(|t| !t.is_empty()),
        inbound,
        outbound,
        truncated: truncated || outbound_truncated,
        corpus_complete,
        collection_notices,
    })
}

/// `제2장`, `2장` or `제3절` as a heading level and number.
fn parse_heading(value: &str) -> Option<(char, u32)> {
    let compact = compact(value);
    let compact = if compact.starts_with('제') {
        compact
    } else {
        format!("제{compact}")
    };
    kr::heading_at(&compact)
}

/// A heading line split into its locator (`제2장`) and its name.
fn heading_parts(text: &str) -> (String, String) {
    let text = text.trim();
    let end = text
        .find(|c: char| c.is_whitespace() || c == '<' || c == '(')
        .unwrap_or(text.len());
    (text[..end].to_string(), clip_chars(&text[end..], 100))
}

struct Placed<'a> {
    article: LocatedArticle<'a>,
    path: Vec<String>,
}

fn article_unit(placed: &Placed<'_>, budget: &mut usize) -> Option<ArticleUnit> {
    if *budget == 0 {
        return None;
    }
    let (text, truncated) = clip(placed.article.text, ARTICLE_TEXT_LIMIT.min(*budget));
    *budget -= text.len();
    Some(ArticleUnit {
        article: placed.article.number.to_string(),
        title: placed.article.title.clone(),
        text,
        truncated,
        deleted: placed.article.deleted,
        path: placed.path.clone(),
    })
}

async fn read_articles(
    lookup: &ReferenceLookup,
    input: ArticleReadInput,
    deadline: Instant,
    cancel: CancellationToken,
) -> Result<ArticleReadResult, ToolError> {
    if (input.selector.is_some() && input.date.is_some())
        || input.date.as_deref().is_some_and(|d| !valid_date(d))
        || input.context > MAX_CONTEXT
        || input.keyword.as_deref().is_some_and(|k| {
            !valid_text(k, MAX_KEYWORD_BYTES) || k.contains('\n') || compact(k).is_empty()
        })
    {
        return Err(ToolError::InvalidInput);
    }
    let wanted_article = match &input.article {
        Some(value) => Some(kr::parse_article_number(value).ok_or(ToolError::InvalidInput)?),
        None => None,
    };
    let wanted_heading = match &input.chapter {
        Some(value) => Some(parse_heading(value).ok_or(ToolError::InvalidInput)?),
        None => None,
    };
    let wanted_annex = match &input.annex {
        Some(value) => Some(kr::parse_annex_label(value).ok_or(ToolError::InvalidInput)?),
        None => None,
    };
    let (object, resolution) = resolve_object(
        lookup,
        &KOREA,
        input.object,
        input.law_name,
        &REVISIONED,
        deadline,
        &cancel,
    )
    .await?;
    let (selector, selection) = match (input.selector, input.date) {
        (Some(selector), None) => (selector, None),
        (None, Some(date)) => {
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
                &date,
                &cancel,
            )
            .await?;
            let Some(selected) = &selection.selected else {
                return Err(ToolError::NotFound);
            };
            (
                RevisionSelector::Revision {
                    id: selected.revision_id.clone(),
                },
                Some(selection),
            )
        }
        _ => (RevisionSelector::Head, None),
    };
    let read = lookup
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
        .map_err(map_error)?;
    let record = &read.capture.record;
    let units = kr::outline(&record.sections);

    let mut placed: Vec<Placed<'_>> = Vec::new();
    let mut outline_entries: Vec<OutlineEntry> = Vec::new();
    let mut stack: Vec<(u8, String)> = Vec::new();
    let mut chapter: Vec<usize> = Vec::new();
    let mut chapter_rank: Option<u8> = None;
    let mut chapter_found = false;
    for unit in units {
        match unit {
            OutlineUnit::Heading {
                level,
                number,
                text,
            } => {
                let rank = kr::heading_rank(level);
                if chapter_rank.is_some_and(|open| rank <= open) {
                    chapter_rank = None;
                }
                if !chapter_found && wanted_heading == Some((level, number)) {
                    chapter_found = true;
                    chapter_rank = Some(rank);
                }
                while stack.last().is_some_and(|(r, _)| *r >= rank) {
                    stack.pop();
                }
                let (label, title) = heading_parts(text);
                stack.push((rank, format!("{label} {title}").trim().to_string()));
                outline_entries.push(OutlineEntry {
                    kind: "heading".into(),
                    label,
                    title,
                });
            }
            OutlineUnit::Article(article) => {
                if chapter_rank.is_some() {
                    chapter.push(placed.len());
                }
                outline_entries.push(OutlineEntry {
                    kind: "article".into(),
                    label: article.number.to_string(),
                    title: article.title.clone(),
                });
                placed.push(Placed {
                    path: stack.iter().map(|(_, label)| label.clone()).collect(),
                    article,
                });
            }
        }
    }

    let mut warnings = Vec::new();
    let mut picked: Vec<usize> = Vec::new();
    let mut want_outline = wanted_article.is_none()
        && wanted_heading.is_none()
        && input.keyword.is_none()
        && wanted_annex.is_none();
    if let Some(wanted) = wanted_article {
        let alternate = ArticleNumber {
            number: wanted.number,
            part: None,
            branch: wanted.part,
        };
        let found = placed
            .iter()
            .position(|p| p.article.number == wanted)
            .or_else(|| {
                (wanted.part.is_some() && wanted.branch.is_none())
                    .then(|| placed.iter().position(|p| p.article.number == alternate))
                    .flatten()
            });
        match found {
            Some(at) => {
                let context = usize::from(input.context);
                let end = (at + context + 1).min(placed.len());
                picked.extend(at.saturating_sub(context)..end);
            }
            None => {
                warnings.push("article_not_found".to_string());
                want_outline = true;
            }
        }
    }
    if wanted_heading.is_some() {
        if chapter_found {
            picked.extend(chapter.iter().copied());
        } else {
            warnings.push("heading_not_found".to_string());
            want_outline = true;
        }
    }
    let mut keyword_matches = Vec::new();
    if let Some(keyword) = &input.keyword {
        let wanted = compact(keyword);
        for (at, unit) in placed.iter().enumerate() {
            if compact(unit.article.text).contains(&wanted) {
                keyword_matches.push(unit.article.number.to_string());
                picked.push(at);
            }
        }
        if keyword_matches.is_empty() {
            warnings.push("keyword_not_found".to_string());
        }
    }
    if placed.is_empty() {
        warnings.push("no_article_structure".to_string());
    }

    let mut truncated = false;
    let mut budget = READ_BUDGET;
    let mut articles = Vec::new();
    let mut emitted = BTreeSet::new();
    for at in picked {
        if !emitted.insert(at) {
            continue;
        }
        match article_unit(&placed[at], &mut budget) {
            Some(unit) => {
                truncated |= unit.truncated;
                articles.push(unit);
            }
            None => truncated = true,
        }
    }
    let all_annexes = kr::annexes(&record.sections);
    let mut annex_index: Vec<String> = Vec::new();
    for annex in &all_annexes {
        let label = annex.label.to_string();
        if !annex_index.contains(&label) {
            annex_index.push(label);
        }
    }
    let mut annexes = Vec::new();
    if let Some(wanted) = wanted_annex {
        let matches: Vec<_> = all_annexes.iter().filter(|a| a.label == wanted).collect();
        if matches.is_empty() {
            warnings.push("annex_not_found".to_string());
        }
        if matches.iter().any(|a| a.sparse) {
            warnings.push("annex_text_sparse".to_string());
        }
        for annex in matches {
            if budget == 0 {
                truncated = true;
                break;
            }
            let (text, cut) = clip(annex.text, ANNEX_TEXT_LIMIT.min(budget));
            budget -= text.len();
            truncated |= cut;
            annexes.push(AnnexText {
                label: annex.label.to_string(),
                section_id: annex.section_id.to_string(),
                title: annex.title.to_string(),
                kind: annex.kind.clone(),
                text,
                truncated: cut,
                sparse: annex.sparse,
            });
        }
    }
    let outline = if want_outline {
        if outline_entries.len() > MAX_OUTLINE {
            truncated = true;
            outline_entries.truncate(MAX_OUTLINE);
        }
        outline_entries
    } else {
        Vec::new()
    };
    Ok(ArticleReadResult {
        schema_version: 1,
        object: object.clone(),
        resolution,
        law_title: record.title.clone(),
        revision_id: record.revision_id.clone(),
        capture_id: read.capture.capture_id.clone(),
        effective_date: record.effective_date.clone(),
        source_url: record.source_url.clone(),
        selection,
        articles,
        keyword_matches,
        outline,
        annexes,
        annex_index,
        truncated,
        warnings,
    })
}

/// The calendar date that counts as "today" and the time zone that defined it.
struct Today {
    /// `YYYYMMDD`.
    date: String,
    /// The IANA time zone name.
    timezone: String,
}

const MAX_TIMEZONE_BYTES: usize = 64;

impl Today {
    /// The date of `unix_seconds` in `timezone`, or in the default jurisdiction's
    /// zone. Zones come from the IANA database bundled into the binary, so results
    /// do not depend on the host's zoneinfo files.
    fn at(unix_seconds: u64, timezone: Option<&str>) -> Result<Self, ToolError> {
        let name = timezone
            .map(str::trim)
            .unwrap_or(Jurisdiction::DEFAULT.default_timezone());
        if name.is_empty()
            || name.len() > MAX_TIMEZONE_BYTES
            || !name
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"/_+-".contains(&b))
        {
            return Err(ToolError::InvalidTimezone);
        }
        let zone = jiff::tz::TimeZone::get(name).map_err(|_| ToolError::InvalidTimezone)?;
        let instant = i64::try_from(unix_seconds)
            .ok()
            .and_then(|seconds| jiff::Timestamp::from_second(seconds).ok())
            .ok_or(ToolError::InvalidInput)?;
        let date = instant.to_zoned(zone).date();
        Ok(Self {
            date: format!("{:04}{:02}{:02}", date.year(), date.month(), date.day()),
            timezone: name.to_string(),
        })
    }

    fn now(timezone: Option<&str>) -> Result<Self, ToolError> {
        Self::at(SystemClock::default().now(), timezone)
    }
}

impl ToolModule for LegalAnalysisTools {
    fn register(self, registry: &mut ToolRegistry) -> Result<(), ServerError> {
        let lookup = self.lookup.clone();
        registry.register_typed::<WatchInput, WatchResult, _, _>(
            "law.watch",
            "Check up to 100 laws, administrative rules or ordinances at once for changes. Give each by law_name or object and pass the snapshot from the previous call as previous: each entry reports changed, unchanged or new against it, the HEAD revision and dates, retained revisions that take effect after today (promulgated but not yet in force; today is the calendar date in timezone, an IANA name defaulting to Asia/Seoul) with their provider amendment types, and repeal_status (repealed, repeal_scheduled, no_repeal_recorded or unknown) read from the provider amendment type (폐지, 타법폐지, 일괄폐지) of the latest retained revision. Names that match no retained object report not_observed, which never means the law does not exist. Save the returned snapshot for the next check.",
            ToolOptions::default(),
            move |input, ctx| {
                let lookup = lookup.clone();
                async move {
                    let today = Today::now(input.timezone.as_deref())?;
                    watch(
                        &lookup,
                        input,
                        &today,
                        ctx.deadline.into_std(),
                        ctx.request.cancellation,
                    )
                    .await
                    .map(output)
                }
            },
        )?;
        let lookup = self.lookup.clone();
        registry.register_typed::<LineageInput, LineageResult, _, _>(
            "law.lineage",
            "Trace one law's retained identity: title periods across retained revisions (renamed is true when the title changed), retained revisions taking effect after today (the calendar date in timezone, an IANA name defaulting to Asia/Seoul) with their amendment types, the HEAD state (published, or a corpus code such as withdrawn), and repeal_status with the repealing revision when the latest retained revision's provider amendment type is 폐지, 타법폐지 or 일괄폐지 (repeal_scheduled when it takes effect after today). Also returns up to 20 lines in other retained statutes and rules that mention one of its titles together with 폐지; those mentions are leads to read, not a repeal record.",
            ToolOptions::default(),
            move |input, ctx| {
                let lookup = lookup.clone();
                async move {
                    let today = Today::now(input.timezone.as_deref())?;
                    let target = TargetInput {
                        object: input.object,
                        law_name: input.law_name,
                    };
                    lineage(
                        &lookup,
                        target,
                        &today,
                        ctx.deadline.into_std(),
                        ctx.request.cancellation,
                    )
                    .await
                    .map(output)
                }
            },
        )?;
        let lookup = self.lookup.clone();
        registry.register_typed::<CitingInput, CitatorResult, _, _>(
            "precedent.citing",
            "Find retained court and Constitutional Court decisions whose text cites a case number, newest judgment first, with up to three citing lines each. Flags overruling language (변경하기로 한다, 견해를 변경, 더 이상 유지할 수 없 and similar) on citing lines as overruling_language_found. none_found does not establish that the decision is still good law: overruling without the number, unusual wording and uncollected decisions are not detected.",
            ToolOptions::default(),
            move |input, ctx| {
                let lookup = lookup.clone();
                async move {
                    citing(&lookup, input, ctx.deadline.into_std(), ctx.request.cancellation)
                        .await
                        .map(output)
                }
            },
        )?;
        let lookup = self.lookup.clone();
        registry.register_typed::<ImpactInput, ImpactResult, _, _>(
            "article.impact",
            "Map references to one article at HEAD: retained decisions, interpretations, appeals, statutes, rules and ordinances whose text cites 「law title」 제N조 (counts per dataset and up to 20 example lines each), the citations in the article's own text, and Mermaid graph source. Inbound matching uses the current full title, so abbreviations such as 법 제5조 in a decree and references under former titles are not counted.",
            ToolOptions::default(),
            move |input, ctx| {
                let lookup = lookup.clone();
                async move {
                    impact(&lookup, input, ctx.deadline.into_std(), ctx.request.cancellation)
                        .await
                        .map(output)
                }
            },
        )?;
        let lookup = self.lookup;
        registry.register_typed::<ArticleReadInput, ArticleReadResult, _, _>(
            "law.article",
            "Read parts of a long statute, administrative rule or ordinance without the whole document: one article with up to three neighbours, every article under a heading (제2장, 제3절), every article containing a keyword, or an annex (별표 1의2). Rules that keep many articles in one text block are split at article lines, and 제9-5조 style numbers are supported. With none of those, returns the heading and article outline. Selects HEAD, an exact selector, or the revision in force on a date. Annexes stored only as images are flagged as sparse.",
            ToolOptions::default(),
            move |input, ctx| {
                let lookup = lookup.clone();
                async move {
                    read_articles(&lookup, input, ctx.deadline.into_std(), ctx.request.cancellation)
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
    use crate::test_corpus::{Record, Stored, article, deadline, lookup, object, section};
    use openlegal_domain::legal::SectionKind;

    fn day(date: &str) -> Today {
        Today {
            date: date.to_string(),
            timezone: "Asia/Seoul".to_string(),
        }
    }

    #[test]
    fn today_follows_the_requested_iana_zone_and_defaults_to_seoul() {
        let date = |unix, zone: Option<&str>| Today::at(unix, zone).map(|t| t.date);
        assert_eq!(date(0, None).unwrap(), "19700101");
        assert_eq!(date(15 * 3600 - 1, None).unwrap(), "19700101");
        assert_eq!(date(15 * 3600, None).unwrap(), "19700102");
        assert_eq!(date(1_709_164_800, None).unwrap(), "20240229");
        assert_eq!(date(1_790_812_800, None).unwrap(), "20261001");
        assert_eq!(Today::at(0, None).unwrap().timezone, "Asia/Seoul");
        // 2026-10-01T03:00:00Z is still 2026-09-30 in New York (EDT, UTC-4).
        let early = 1_790_823_600;
        assert_eq!(date(early, Some("America/New_York")).unwrap(), "20260930");
        assert_eq!(date(early, Some("UTC")).unwrap(), "20261001");
        assert_eq!(date(early, Some("Asia/Seoul")).unwrap(), "20261001");
        // Daylight saving time: 2026-01-15T04:30:00Z is 2026-01-14 in New York (EST, UTC-5).
        assert_eq!(
            date(1_768_451_400, Some("America/New_York")).unwrap(),
            "20260114"
        );
        for invalid in ["", "Mars/Olympus", "Asia/Seoul;", "../etc/passwd", "A b"] {
            assert_eq!(
                Today::at(0, Some(invalid)).err(),
                Some(ToolError::InvalidTimezone),
                "{invalid}"
            );
        }
    }

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

    fn body(text: &str) -> openlegal_domain::legal::LegalSection {
        section(
            "source_ordinal:1",
            "판례내용",
            text,
            SectionKind::ProviderText,
        )
    }

    fn corpus() -> ReferenceLookup {
        let civil = object(Dataset::NationalStatute, "1");
        let renamed = object(Dataset::NationalStatute, "3");
        lookup(vec![
            record(
                civil.clone(),
                1,
                "100:20200101",
                "민법",
                Some("20200101"),
                vec![article(
                    "0750001",
                    "불법행위의 내용",
                    "제750조(불법행위의 내용) 과실로 손해를 가한 자는 배상한다.",
                )],
                &[("amendment_type", "제정")],
                false,
            ),
            record(
                civil.clone(),
                2,
                "200:20230601",
                "민법",
                Some("20230601"),
                vec![
                    article("0001000", "", "제1장 총칙"),
                    article("0001001", "목적", "제1조(목적) 이 법은 목적을 정한다."),
                    article("0002000", "", "제2장 불법행위"),
                    article(
                        "0750001",
                        "불법행위의 내용",
                        "제750조(불법행위의 내용) 고의 또는 과실로 손해를 가한 자는 제751조 및 「상법」 제5조에 따라 배상한다. 이 법 제1조를 준용한다.",
                    ),
                    article(
                        "0751001",
                        "재산 이외의 손해의 배상",
                        "제751조(재산 이외의 손해의 배상)\n① 첫째 항",
                    ),
                ],
                &[("amendment_type", "일부개정")],
                true,
            ),
            record(
                civil,
                3,
                "300:20990101",
                "민법",
                Some("20990101"),
                vec![],
                &[("amendment_type", "일부개정")],
                false,
            ),
            record(
                object(Dataset::NationalStatute, "7"),
                7,
                "700:20200101",
                "폐지예정법",
                Some("20200101"),
                vec![article("0001001", "목적", "제1조(목적) 목적")],
                &[("amendment_type", "제정")],
                true,
            ),
            record(
                object(Dataset::NationalStatute, "7"),
                31,
                "701:20990101",
                "폐지예정법",
                Some("20990101"),
                vec![article("0001001", "목적", "제1조(목적) 목적")],
                &[("amendment_type", "폐지")],
                false,
            ),
            record(
                object(Dataset::NationalStatute, "8"),
                32,
                "800:20100101",
                "옛법",
                Some("20100101"),
                vec![article("0001001", "목적", "제1조(목적) 목적")],
                &[("amendment_type", "제정")],
                false,
            ),
            record(
                object(Dataset::NationalStatute, "8"),
                33,
                "801:20150101",
                "옛법",
                Some("20150101"),
                vec![article("0001001", "목적", "제1조(목적) 목적")],
                &[("amendment_type", "타법폐지")],
                false,
            ),
            record(
                renamed.clone(),
                4,
                "400:20100101",
                "구 시험법",
                Some("20100101"),
                vec![article("0001001", "목적", "제1조(목적) 옛 목적")],
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
                object(Dataset::NationalStatute, "6"),
                6,
                "600:20200101",
                "시험정리법",
                Some("20200101"),
                vec![article(
                    "0001001",
                    "폐지",
                    "제1조(폐지) 「구 시험법」은 폐지한다.",
                )],
                &[],
                true,
            ),
            record(
                object(Dataset::Precedent, "9"),
                9,
                "9",
                "손해배상",
                None,
                vec![body("원심판결을 파기한다.")],
                &[("case_number", "2007다27670")],
                true,
            ),
            record(
                object(Dataset::Precedent, "10"),
                10,
                "10",
                "손해배상(전원합의체)",
                None,
                vec![body(
                    "대법원 2007다27670 판결은 이 판결의 견해에 배치되는 범위에서 이를 변경하기로 한다.\n민법 제750조의 해석에 관한 것이다.",
                )],
                &[
                    ("case_number", "2015다1"),
                    ("judgment_date", "20200101"),
                    ("authority", "대법원"),
                ],
                true,
            ),
            record(
                object(Dataset::Precedent, "11"),
                11,
                "11",
                "부당이득",
                None,
                vec![body(
                    "2007다27670 판결 참조.\n민법 제750조에 따르면 배상한다.\n2007다276701 사건은 다르다.",
                )],
                &[("case_number", "2009다2"), ("judgment_date", "20100101")],
                true,
            ),
            record(
                object(Dataset::Precedent, "12"),
                12,
                "12",
                "다른 사건",
                None,
                vec![body(
                    "민법 제7500조와 민법 제750조의2, 국민법 제750조는 다르다.",
                )],
                &[("case_number", "2009다3")],
                true,
            ),
            record(
                object(Dataset::Ordinance, "20"),
                20,
                "20:20200101",
                "서울특별시 배상 조례",
                Some("20200101"),
                vec![article(
                    "0001001",
                    "목적",
                    "제1조(목적) 「민법」 제750조에 따라 정한다.",
                )],
                &[],
                true,
            ),
            record(
                object(Dataset::AdministrativeRule, "30"),
                30,
                "30:20200101",
                "긴 행정규칙",
                Some("20200101"),
                vec![
                    section(
                        "source_ordinal:1",
                        "조문내용",
                        "제1장 총칙\n제1조(목적) 목적이다.\n제2조(정의) 정의한다.\n제2장 운영\n제9-5조(특례) 특례를 둔다.\n① 첫째\n제10조 삭제",
                        SectionKind::ProviderText,
                    ),
                    section(
                        "source_ordinal:2",
                        "별표내용",
                        "[별표 1] 수수료 기준표 (제9-5조 관련)\n일반 신청 수수료는 만원으로 하고 재발급 수수료는 오천원으로 한다.",
                        SectionKind::ProviderText,
                    ),
                    section(
                        "attachment:1",
                        "별표 1의2",
                        "[별표 1의2]\n<img src=\"x\">",
                        SectionKind::Extracted,
                    ),
                ],
                &[],
                true,
            ),
        ])
    }

    fn target(law_name: Option<&str>, object: Option<ObjectId>) -> WatchTarget {
        WatchTarget {
            law_name: law_name.map(Into::into),
            object,
        }
    }

    #[test]
    fn registers_read_only_tools_with_titles_and_schemas() {
        let mut registry = ToolRegistry::new();
        registry
            .register_module(LegalAnalysisTools {
                lookup: Arc::new(corpus()),
            })
            .unwrap();
        for (name, title) in [
            ("law.watch", "Watch laws for changes"),
            ("law.lineage", "Trace law renames and repeal mentions"),
            ("precedent.citing", "Find decisions citing a case"),
            ("article.impact", "Map references to a law article"),
            ("law.article", "Read law articles and annexes"),
        ] {
            let tool = &registry.tools[name].definition;
            assert_eq!(tool.title.as_deref(), Some(title));
            let annotations = tool.annotations.as_ref().unwrap();
            assert_eq!(annotations.read_only_hint, Some(true));
            assert!(tool.output_schema.is_some());
        }
    }

    #[tokio::test]
    async fn watches_laws_against_a_snapshot() {
        let lookup = corpus();
        let previous = BTreeMap::from([
            ("national_statute:1".to_string(), "100:20200101".to_string()),
            ("national_statute:3".to_string(), "500:20200101".to_string()),
        ]);
        let input = |changes_only| WatchInput {
            laws: vec![
                target(Some("민 법"), None),
                target(Some("구 시험법"), None),
                target(Some("없는법"), None),
                target(None, Some(object(Dataset::NationalStatute, "99"))),
                target(None, Some(object(Dataset::Precedent, "9"))),
                target(Some("폐지예정법"), None),
                target(Some("옛법"), None),
            ],
            previous: previous.clone(),
            include_upcoming: true,
            changes_only,
            timezone: None,
        };
        let result = watch(
            &lookup,
            input(false),
            &day("20240101"),
            deadline(),
            CancellationToken::new(),
        )
        .await
        .unwrap();
        let statuses: Vec<WatchStatus> = result.entries.iter().map(|e| e.status).collect();
        use WatchStatus::*;
        assert_eq!(
            statuses,
            [
                Changed,
                Unchanged,
                NotObserved,
                NotObserved,
                New,
                New,
                NotObserved
            ]
        );
        let civil = &result.entries[0];
        assert_eq!(civil.head_revision_id.as_deref(), Some("200:20230601"));
        assert_eq!(civil.previous_revision_id.as_deref(), Some("100:20200101"));
        assert_eq!(civil.upcoming.len(), 1);
        assert_eq!(civil.upcoming[0].revision_id, "300:20990101");
        assert_eq!(
            civil.upcoming[0].amendment_type.as_deref(),
            Some("일부개정")
        );
        assert_eq!(civil.amendment_type.as_deref(), Some("일부개정"));
        assert_eq!(civil.repeal_status, Some(RepealStatus::NoRepealRecorded));
        assert_eq!(result.entries[1].title.as_deref(), Some("새 시험법"));
        assert_eq!(result.entries[1].repeal_status, Some(RepealStatus::Unknown));
        assert_eq!(result.entries[4].repeal_status, None);
        let scheduled = &result.entries[5];
        assert_eq!(scheduled.repeal_status, Some(RepealStatus::RepealScheduled));
        assert_eq!(
            scheduled.upcoming[0].amendment_type.as_deref(),
            Some("폐지")
        );
        let repealed = &result.entries[6];
        assert_eq!(repealed.repeal_status, Some(RepealStatus::Repealed));
        let record = repealed.repeal.as_ref().unwrap();
        assert_eq!(record.kind, RepealKind::RepealedByOtherLaw);
        assert_eq!(record.effective_date.as_deref(), Some("20150101"));
        assert_eq!(result.entries[3].detail.as_deref(), Some("not_observed"));
        assert_eq!(result.entries[4].input, "precedent:9");
        assert_eq!(
            result.snapshot,
            BTreeMap::from([
                ("national_statute:1".to_string(), "200:20230601".to_string()),
                ("national_statute:3".to_string(), "500:20200101".to_string()),
                ("national_statute:7".to_string(), "700:20200101".to_string()),
                ("precedent:9".to_string(), "9".to_string()),
            ])
        );
        assert_eq!(
            (result.changed, result.with_upcoming, result.repealed),
            (1, 2, 2)
        );
        let changes = watch(
            &lookup,
            input(true),
            &day("20240101"),
            deadline(),
            CancellationToken::new(),
        )
        .await
        .unwrap();
        assert_eq!(changes.entries.len(), 6);
        assert!(changes.entries.iter().all(|e| e.status != Unchanged));
        assert_eq!(changes.snapshot.len(), 4);
        for laws in [
            vec![],
            vec![target(None, None)],
            vec![target(Some(""), None)],
        ] {
            let input = WatchInput {
                laws,
                previous: BTreeMap::new(),
                include_upcoming: true,
                changes_only: false,
                timezone: None,
            };
            assert_eq!(
                watch(
                    &lookup,
                    input,
                    &day("20240101"),
                    deadline(),
                    CancellationToken::new()
                )
                .await
                .err(),
                Some(ToolError::InvalidInput)
            );
        }
    }

    #[tokio::test]
    async fn traces_titles_upcoming_revisions_and_repeal_mentions() {
        let lookup = corpus();
        let renamed = lineage(
            &lookup,
            TargetInput {
                object: None,
                law_name: Some("새 시험법".into()),
            },
            &day("20240101"),
            deadline(),
            CancellationToken::new(),
        )
        .await
        .unwrap();
        assert_eq!(renamed.object.id, "3");
        assert!(renamed.renamed);
        let titles: Vec<&str> = renamed.titles.iter().map(|t| t.title.as_str()).collect();
        assert_eq!(titles, ["구 시험법", "새 시험법"]);
        assert_eq!(renamed.head_state, "published");
        assert_eq!(renamed.repeal_mentions.len(), 1);
        assert_eq!(renamed.repeal_mentions[0].object.id, "6");
        assert!(renamed.repeal_mentions[0].line.contains("폐지한다"));
        assert_eq!(renamed.repeal_status, RepealStatus::Unknown);
        let civil = lineage(
            &lookup,
            TargetInput {
                object: Some(object(Dataset::NationalStatute, "1")),
                law_name: None,
            },
            &day("20240101"),
            deadline(),
            CancellationToken::new(),
        )
        .await
        .unwrap();
        assert!(!civil.renamed);
        assert_eq!(civil.titles.len(), 1);
        assert_eq!(civil.titles[0].first_revision_id, "100:20200101");
        assert_eq!(civil.titles[0].last_revision_id, "300:20990101");
        assert_eq!(civil.upcoming[0].revision_id, "300:20990101");
        assert!(civil.repeal_mentions.is_empty());
        assert_eq!(civil.repeal_status, RepealStatus::NoRepealRecorded);
        assert_eq!(civil.latest_amendment_type.as_deref(), Some("일부개정"));
        let repealed = lineage(
            &lookup,
            TargetInput {
                object: None,
                law_name: Some("옛법".into()),
            },
            &day("20240101"),
            deadline(),
            CancellationToken::new(),
        )
        .await
        .unwrap();
        assert_eq!(repealed.head_state, "not_observed");
        assert_eq!(repealed.repeal_status, RepealStatus::Repealed);
        let record = repealed.repeal.unwrap();
        assert_eq!(record.revision_id, "801:20150101");
        assert_eq!(record.amendment_type, "타법폐지");
        let scheduled = lineage(
            &lookup,
            TargetInput {
                object: Some(object(Dataset::NationalStatute, "7")),
                law_name: None,
            },
            &day("20240101"),
            deadline(),
            CancellationToken::new(),
        )
        .await
        .unwrap();
        assert_eq!(scheduled.repeal_status, RepealStatus::RepealScheduled);
        assert_eq!(scheduled.repeal.unwrap().kind, RepealKind::Repealed);
        let precedent = TargetInput {
            object: Some(object(Dataset::Precedent, "9")),
            law_name: None,
        };
        assert_eq!(
            lineage(
                &lookup,
                precedent,
                &day("20240101"),
                deadline(),
                CancellationToken::new()
            )
            .await
            .err(),
            Some(ToolError::UnsupportedHistory)
        );
    }

    #[tokio::test]
    async fn lists_citing_decisions_with_overruling_signals() {
        let lookup = corpus();
        let result = citing(
            &lookup,
            CitingInput {
                case_number: "2007다27670".into(),
            },
            deadline(),
            CancellationToken::new(),
        )
        .await
        .unwrap();
        assert_eq!(result.targets.len(), 1);
        assert_eq!(result.targets[0].object.id, "9");
        let ids: Vec<&str> = result.citing.iter().map(|c| c.object.id.as_str()).collect();
        assert_eq!(ids, ["10", "11"]);
        let overruling = &result.citing[0];
        assert_eq!(
            overruling.overruling_phrase.as_deref(),
            Some("변경하기로 한다")
        );
        assert!(overruling.en_banc);
        assert_eq!(overruling.judgment_date.as_deref(), Some("20200101"));
        assert_eq!(overruling.authority.as_deref(), Some("대법원"));
        assert_eq!(result.citing[1].lines, ["2007다27670 판결 참조."]);
        assert_eq!(result.signal, CitatorSignal::OverrulingLanguageFound);
        assert!(!result.truncated && result.corpus_complete);
        let none = citing(
            &lookup,
            CitingInput {
                case_number: "2009다2".into(),
            },
            deadline(),
            CancellationToken::new(),
        )
        .await
        .unwrap();
        assert!(none.citing.is_empty());
        assert_eq!(none.signal, CitatorSignal::NoneFound);
        for bad in ["", "민법 제750조", "2007다27670 2009다2"] {
            let input = CitingInput {
                case_number: bad.into(),
            };
            assert_eq!(
                citing(&lookup, input, deadline(), CancellationToken::new())
                    .await
                    .err(),
                Some(ToolError::InvalidInput)
            );
        }
    }

    #[tokio::test]
    async fn maps_inbound_and_outbound_article_references() {
        let lookup = corpus();
        let result = impact(
            &lookup,
            ImpactInput {
                object: None,
                law_name: Some("민법".into()),
                article: "750".into(),
            },
            deadline(),
            CancellationToken::new(),
        )
        .await
        .unwrap();
        assert_eq!(result.article, "제750조");
        assert_eq!(result.article_title.as_deref(), Some("불법행위의 내용"));
        let buckets: Vec<(Dataset, u32)> = result
            .inbound
            .iter()
            .map(|b| (b.dataset, b.object_count))
            .collect();
        assert_eq!(buckets, [(Dataset::Precedent, 2), (Dataset::Ordinance, 1)]);
        let outbound: Vec<(Option<&str>, &str)> = result
            .outbound
            .iter()
            .map(|r| (r.law_name.as_deref(), r.article.as_str()))
            .collect();
        assert_eq!(
            outbound,
            [(None, "제751조"), (Some("상법"), "제5조"), (None, "제1조")]
        );
        assert!(
            result
                .mermaid
                .starts_with("graph LR\n  A[\"민법 제750조\"]")
        );
        assert!(result.mermaid.contains("I0[\"판례 2건\"] --> A"));
        assert!(result.mermaid.contains("A --> O1[\"상법 제5조\"]"));
        let missing = ImpactInput {
            object: Some(object(Dataset::NationalStatute, "1")),
            law_name: None,
            article: "제9999조".into(),
        };
        assert_eq!(
            impact(&lookup, missing, deadline(), CancellationToken::new())
                .await
                .err(),
            Some(ToolError::NotFound)
        );
    }

    fn read_input(object_id: &str) -> ArticleReadInput {
        ArticleReadInput {
            object: Some(object(Dataset::AdministrativeRule, object_id)),
            law_name: None,
            selector: None,
            date: None,
            article: None,
            context: 0,
            chapter: None,
            keyword: None,
            annex: None,
        }
    }

    async fn read(input: ArticleReadInput) -> Result<ArticleReadResult, ToolError> {
        read_articles(&corpus(), input, deadline(), CancellationToken::new()).await
    }

    fn labels(result: &ArticleReadResult) -> Vec<&str> {
        result.articles.iter().map(|a| a.article.as_str()).collect()
    }

    #[tokio::test]
    async fn reads_rule_articles_headings_keywords_and_annexes() {
        let around = read(ArticleReadInput {
            article: Some("9-5".into()),
            context: 1,
            ..read_input("30")
        })
        .await
        .unwrap();
        assert_eq!(labels(&around), ["제2조", "제9-5조", "제10조"]);
        assert_eq!(around.articles[1].path, ["제2장 운영"]);
        assert_eq!(around.articles[1].title, "특례");
        assert!(around.articles[2].deleted);
        assert!(around.outline.is_empty() && around.warnings.is_empty());
        assert_eq!(around.annex_index, ["별표 1", "별표 1의2"]);
        let chapter = read(ArticleReadInput {
            chapter: Some("1장".into()),
            ..read_input("30")
        })
        .await
        .unwrap();
        assert_eq!(labels(&chapter), ["제1조", "제2조"]);
        let keyword = read(ArticleReadInput {
            keyword: Some("특 례".into()),
            ..read_input("30")
        })
        .await
        .unwrap();
        assert_eq!(keyword.keyword_matches, ["제9-5조"]);
        let annex = read(ArticleReadInput {
            annex: Some("별표 1".into()),
            ..read_input("30")
        })
        .await
        .unwrap();
        assert_eq!(annex.annexes.len(), 1);
        assert!(!annex.annexes[0].sparse && annex.articles.is_empty());
        let image = read(ArticleReadInput {
            annex: Some("1의2".into()),
            ..read_input("30")
        })
        .await
        .unwrap();
        assert_eq!(image.annexes[0].kind, SectionKind::Extracted);
        assert_eq!(image.warnings, ["annex_text_sparse"]);
        let outline = read(read_input("30")).await.unwrap();
        let entries: Vec<(&str, &str)> = outline
            .outline
            .iter()
            .map(|e| (e.kind.as_str(), e.label.as_str()))
            .collect();
        assert_eq!(
            entries,
            [
                ("heading", "제1장"),
                ("article", "제1조"),
                ("article", "제2조"),
                ("heading", "제2장"),
                ("article", "제9-5조"),
                ("article", "제10조"),
            ]
        );
        let missing = read(ArticleReadInput {
            article: Some("99".into()),
            chapter: Some("제9장".into()),
            annex: Some("별표 7".into()),
            ..read_input("30")
        })
        .await
        .unwrap();
        assert_eq!(
            missing.warnings,
            ["article_not_found", "heading_not_found", "annex_not_found"]
        );
        assert_eq!(missing.outline.len(), 6);
    }

    #[tokio::test]
    async fn reads_statute_articles_by_date() {
        let at_date = read(ArticleReadInput {
            object: None,
            law_name: Some("민법".into()),
            date: Some("20210101".into()),
            article: Some("750".into()),
            ..read_input("30")
        })
        .await
        .unwrap();
        assert_eq!(at_date.object.id, "1");
        assert_eq!(at_date.revision_id, "100:20200101");
        assert!(at_date.articles[0].text.contains("과실로 손해"));
        assert_eq!(
            at_date.selection.unwrap().status,
            openlegal_domain::legal_reference::InForceStatus::Determined
        );
        let head = read(ArticleReadInput {
            object: Some(object(Dataset::NationalStatute, "1")),
            article: Some("제750조".into()),
            ..read_input("30")
        })
        .await
        .unwrap();
        assert_eq!(head.revision_id, "200:20230601");
        assert_eq!(head.articles[0].path, ["제2장 불법행위"]);
        let early = ArticleReadInput {
            object: Some(object(Dataset::NationalStatute, "1")),
            date: Some("19990101".into()),
            ..read_input("30")
        };
        assert_eq!(read(early).await.err(), Some(ToolError::NotFound));
        let invalid = [
            ArticleReadInput {
                selector: Some(RevisionSelector::Head),
                date: Some("20210101".into()),
                ..read_input("30")
            },
            ArticleReadInput {
                context: 4,
                ..read_input("30")
            },
            ArticleReadInput {
                annex: Some("부록".into()),
                ..read_input("30")
            },
            ArticleReadInput {
                keyword: Some(" ".into()),
                ..read_input("30")
            },
        ];
        for input in invalid {
            assert_eq!(read(input).await.err(), Some(ToolError::InvalidInput));
        }
    }
}
