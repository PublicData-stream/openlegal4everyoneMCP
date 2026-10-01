//! Corpus lookups and date-based revision selection for legal references.
//!
//! Lookups only read the managed corpus. An absent match means the corpus has no
//! retained observation; it never establishes that a provider lacks the object.
use crate::{
    database::DatabaseService,
    search::{SearchMode, SearchService},
};
use openlegal_domain::{
    legal::{CollectionNotice, DatabaseError, Dataset, HistoryEntry, HistoryKind, ObjectId},
    legal_reference::{InForceSelection, InForceStatus, RevisionChoice},
    legal_search::{Filters, SearchHit, SearchRequest},
};
use std::{sync::Arc, time::Instant};
use tokio_util::sync::CancellationToken;

/// Line hits collected by one lookup before it reports truncation.
pub const MAX_LINE_HITS: usize = 500;
const MAX_PAGES: usize = 20;
const MAX_QUERY_BYTES: usize = 4000;
const PAGE_LIMIT: usize = 100;
const MAX_REVISION_PAGES: usize = 20;

#[derive(Debug, Default)]
pub struct LineLookup {
    pub hits: Vec<SearchHit>,
    /// True only when every page reported complete current corpus coverage.
    pub corpus_complete: bool,
    pub collection_notices: Vec<CollectionNotice>,
    /// The page or hit bound stopped the lookup before the corpus was exhausted.
    pub truncated: bool,
}

#[derive(Debug)]
pub struct RevisionInventory {
    pub entries: Vec<HistoryEntry>,
    pub complete: bool,
}

pub struct ReferenceLookup {
    database: Arc<DatabaseService>,
    search: Arc<SearchService>,
}

impl ReferenceLookup {
    pub fn new(database: Arc<DatabaseService>, search: Arc<SearchService>) -> Self {
        Self { database, search }
    }

    pub fn database(&self) -> &DatabaseService {
        &self.database
    }

    /// Find section lines that equal one of the regular-expression `alternatives`,
    /// optionally within one provider object ID.
    /// Alternatives are combined into anchored `^(?:a|b)$` searches of bounded size and
    /// each search is paged to completion so no search session is left open.
    #[allow(clippy::too_many_arguments)]
    pub async fn find_lines(
        &self,
        section: &str,
        datasets: Vec<Dataset>,
        alternatives: &[String],
        object_id: Option<&str>,
        include_history: bool,
        deadline: Instant,
        cancel: CancellationToken,
    ) -> Result<LineLookup, DatabaseError> {
        let mut result = LineLookup {
            corpus_complete: true,
            ..LineLookup::default()
        };
        for query in anchored_queries(alternatives)? {
            let mut cursor = None;
            let mut pages = 0;
            loop {
                let page = self
                    .search
                    .search(
                        SearchMode::Ripgrep,
                        SearchRequest {
                            query: query.clone(),
                            filters: Filters {
                                datasets: datasets.clone(),
                                object_id: object_id.map(str::to_string),
                                ..Filters::default()
                            },
                            include_history,
                            include_ocr: false,
                            sections: vec![section.to_string()],
                            limit: PAGE_LIMIT,
                            cursor: cursor.take(),
                            literal: false,
                            ignore_case: false,
                            context_lines: 0,
                        },
                        deadline,
                        cancel.clone(),
                    )
                    .await?;
                pages += 1;
                result.corpus_complete &= page.corpus_complete;
                result.collection_notices = page.collection_notices;
                for hit in page.hits {
                    if result.hits.len() == MAX_LINE_HITS {
                        result.truncated = true;
                        break;
                    }
                    result.hits.push(hit);
                }
                match page.next_cursor {
                    Some(next) if pages < MAX_PAGES && !result.truncated => cursor = Some(next),
                    Some(_) => {
                        result.truncated = true;
                        break;
                    }
                    None => break,
                }
            }
        }
        if result.truncated {
            result.corpus_complete = false;
        }
        Ok(result)
    }

    /// Read the retained revision catalog, newest effective date first.
    pub async fn revisions(
        &self,
        object: ObjectId,
        cancel: CancellationToken,
    ) -> Result<RevisionInventory, DatabaseError> {
        let mut entries = Vec::new();
        let mut cursor = None;
        let mut complete = true;
        for page_number in 0..MAX_REVISION_PAGES {
            let page = self
                .database
                .history(
                    object.clone(),
                    HistoryKind::Revisions,
                    cursor.take(),
                    PAGE_LIMIT,
                    cancel.clone(),
                )
                .await?;
            complete &= page.inventory_complete;
            entries.extend(page.entries);
            match page.next_cursor {
                Some(next) if page_number + 1 < MAX_REVISION_PAGES => cursor = Some(next),
                Some(_) => complete = false,
                None => break,
            }
        }
        Ok(RevisionInventory { entries, complete })
    }
}

fn anchored_queries(alternatives: &[String]) -> Result<Vec<String>, DatabaseError> {
    let mut queries = Vec::new();
    let mut current = String::new();
    for alternative in alternatives {
        if alternative.is_empty() || alternative.len() + 8 > MAX_QUERY_BYTES {
            return Err(DatabaseError::InvalidInput);
        }
        if !current.is_empty() && current.len() + alternative.len() + 4 > MAX_QUERY_BYTES {
            queries.push(format!("{current})$"));
            current.clear();
        }
        current.push_str(if current.is_empty() { "^(?:" } else { "|" });
        current.push_str(alternative);
    }
    if !current.is_empty() {
        queries.push(format!("{current})$"));
    }
    Ok(queries)
}

fn choice(entry: &HistoryEntry) -> RevisionChoice {
    RevisionChoice {
        revision_id: entry.revision_id.clone(),
        effective_date: entry.effective_date.clone(),
        publication_date: entry.publication_date.clone(),
    }
}

/// Select the retained revision whose effective date is the latest one on or before
/// `date`. Among revisions with that effective date, the latest publication date
/// (then revision ID) is selected and the others are reported. This is a selection
/// over retained provider effective-date views, not a legal-applicability ruling.
pub fn select_in_force(
    entries: &[HistoryEntry],
    inventory_complete: bool,
    date: &str,
) -> InForceSelection {
    let mut warnings = Vec::new();
    let dated: Vec<&HistoryEntry> = entries
        .iter()
        .filter(|e| {
            e.effective_date
                .as_deref()
                .is_some_and(openlegal_domain::legal::valid_date)
        })
        .collect();
    if dated.len() < entries.len() {
        warnings.push("undated_revisions_ignored".to_string());
    }
    if !inventory_complete {
        warnings.push("inventory_incomplete".to_string());
    }
    let effective = |e: &HistoryEntry| e.effective_date.clone().unwrap_or_default();
    let order = |e: &&HistoryEntry| {
        (
            effective(e),
            e.publication_date.clone().unwrap_or_default(),
            e.revision_id.clone(),
        )
    };
    let best_date = dated
        .iter()
        .map(|e| effective(e))
        .filter(|d| d.as_str() <= date)
        .max();
    let next_date = dated
        .iter()
        .map(|e| effective(e))
        .filter(|d| d.as_str() > date)
        .min();
    let next_change = next_date.and_then(|next| {
        dated
            .iter()
            .filter(|e| effective(e) == next)
            .max_by_key(|e| order(e))
            .map(|e| choice(e))
    });
    let Some(best_date) = best_date else {
        return InForceSelection {
            date: date.to_string(),
            status: if dated.is_empty() {
                InForceStatus::Undetermined
            } else {
                InForceStatus::NotYetEffective
            },
            selected: None,
            same_effective_date: Vec::new(),
            next_change,
            later_provision_dates: Vec::new(),
            warnings,
        };
    };
    let mut group: Vec<&HistoryEntry> = dated
        .into_iter()
        .filter(|e| effective(e) == best_date)
        .collect();
    group.sort_by_key(order);
    let selected = group.pop().map(choice);
    if !group.is_empty() {
        warnings.push("multiple_revisions_same_effective_date".to_string());
    }
    InForceSelection {
        date: date.to_string(),
        status: if inventory_complete {
            InForceStatus::Determined
        } else {
            InForceStatus::Provisional
        },
        selected,
        same_effective_date: group.into_iter().rev().map(choice).collect(),
        next_change,
        later_provision_dates: Vec::new(),
        warnings,
    }
}

/// The Korean (UTC+9) calendar date of a Unix time, as `YYYYMMDD`.
pub fn kst_date(unix_seconds: u64) -> String {
    let days = ((unix_seconds + 9 * 3600) / 86400) as i64;
    // Civil-from-days (Howard Hinnant), valid for all dates after 1970.
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    format!("{year:04}{month:02}{day:02}")
}

/// Distinct valid `YYYYMMDD` dates after `date` found in a provider date list such as
/// `provision_effective_dates`, sorted ascending.
pub fn later_dates(value: &str, date: &str) -> Vec<String> {
    let bytes = value.as_bytes();
    let mut found = std::collections::BTreeSet::new();
    let mut i = 0;
    while i < bytes.len() {
        if !bytes[i].is_ascii_digit() {
            i += 1;
            continue;
        }
        let start = i;
        while i < bytes.len() && bytes[i].is_ascii_digit() {
            i += 1;
        }
        let run = &value[start..i];
        if run.len() == 8 && openlegal_domain::legal::valid_date(run) && run > date {
            found.insert(run.to_string());
        }
    }
    found.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        Clock,
        database::DatabaseStore,
        search::{SearchBackend, SearchBudget},
    };
    use futures::future::BoxFuture;
    use openlegal_domain::{
        legal::{Capture, HistoryPage, RevisionSelector},
        legal_search::SearchPage,
    };
    use std::sync::Mutex;

    fn entry(id: &str, effective: Option<&str>, publication: Option<&str>) -> HistoryEntry {
        HistoryEntry {
            revision_id: id.into(),
            capture_id: None,
            sequence: 0,
            captured_at: None,
            publication_date: publication.map(Into::into),
            effective_date: effective.map(Into::into),
        }
    }

    #[test]
    fn selects_latest_effective_view_and_reports_neighbors() {
        let entries = vec![
            entry("c", Some("20240101"), Some("20231201")),
            entry("b2", Some("20230101"), Some("20221115")),
            entry("b1", Some("20230101"), Some("20221001")),
            entry("a", Some("20200101"), Some("20191201")),
            entry("u", None, None),
        ];
        let s = select_in_force(&entries, true, "20230510");
        assert_eq!(s.status, InForceStatus::Determined);
        assert_eq!(s.selected.unwrap().revision_id, "b2");
        assert_eq!(s.same_effective_date[0].revision_id, "b1");
        assert_eq!(s.next_change.unwrap().revision_id, "c");
        assert_eq!(
            s.warnings,
            [
                "undated_revisions_ignored",
                "multiple_revisions_same_effective_date"
            ]
        );
        let exact = select_in_force(&entries, false, "20240101");
        assert_eq!(exact.status, InForceStatus::Provisional);
        assert_eq!(exact.selected.unwrap().revision_id, "c");
        assert!(exact.next_change.is_none());
        assert!(exact.warnings.contains(&"inventory_incomplete".to_string()));
        let early = select_in_force(&entries, true, "19991231");
        assert_eq!(early.status, InForceStatus::NotYetEffective);
        assert_eq!(early.next_change.unwrap().revision_id, "a");
        let none = select_in_force(&[entry("u", None, None)], true, "20240101");
        assert_eq!(none.status, InForceStatus::Undetermined);
    }

    #[test]
    fn korean_dates_use_utc_plus_nine() {
        assert_eq!(kst_date(0), "19700101");
        assert_eq!(kst_date(15 * 3600 - 1), "19700101");
        assert_eq!(kst_date(15 * 3600), "19700102");
        assert_eq!(kst_date(1_709_164_800), "20240229");
        assert_eq!(kst_date(1_790_812_800), "20261001");
    }

    #[test]
    fn later_dates_keeps_valid_future_dates_only() {
        assert_eq!(
            later_dates(
                "20200101, 20250701,20251301 2025070199 20250701",
                "20240101"
            ),
            ["20250701"]
        );
    }

    #[test]
    fn anchored_queries_are_bounded() {
        let alternatives: Vec<String> = (0..1000).map(|i| format!("법령{i}")).collect();
        let queries = anchored_queries(&alternatives).unwrap();
        assert!(queries.len() > 1);
        assert!(
            queries
                .iter()
                .all(|q| q.len() <= MAX_QUERY_BYTES && q.starts_with("^(?:") && q.ends_with(")$"))
        );
        assert_eq!(
            queries
                .iter()
                .map(|q| q.matches('|').count() + 1)
                .sum::<usize>(),
            1000
        );
        assert!(anchored_queries(&[String::new()]).is_err());
        assert!(anchored_queries(&[]).unwrap().is_empty());
    }

    struct Fixed;
    impl Clock for Fixed {
        fn now(&self) -> u64 {
            100
        }
    }
    struct History;
    impl DatabaseStore for History {
        fn resolve(
            &self,
            _: ObjectId,
            _: RevisionSelector,
            _: u64,
            _: CancellationToken,
        ) -> BoxFuture<'static, Result<Capture, DatabaseError>> {
            Box::pin(async { Err(DatabaseError::NotObserved) })
        }
        fn history(
            &self,
            _: ObjectId,
            _: HistoryKind,
            cursor: Option<String>,
            limit: usize,
            _: u64,
            _: CancellationToken,
        ) -> BoxFuture<'static, Result<HistoryPage, DatabaseError>> {
            Box::pin(async move {
                assert_eq!(limit, PAGE_LIMIT);
                Ok(match cursor.as_deref() {
                    None => HistoryPage {
                        entries: vec![entry("2", Some("20240101"), None)],
                        next_cursor: Some("next".into()),
                        inventory_complete: true,
                    },
                    Some("next") => HistoryPage {
                        entries: vec![entry("1", Some("20200101"), None)],
                        next_cursor: None,
                        inventory_complete: true,
                    },
                    Some(_) => unreachable!("test cursor"),
                })
            })
        }
    }
    struct Pages(Mutex<Vec<SearchRequest>>);
    impl SearchBackend for Pages {
        fn search(
            &self,
            _: SearchMode,
            request: SearchRequest,
            _: SearchBudget,
            _: CancellationToken,
        ) -> BoxFuture<'static, Result<SearchPage, DatabaseError>> {
            let first = request.cursor.is_none();
            self.0.lock().unwrap().push(request);
            Box::pin(async move {
                Ok(SearchPage {
                    schema_version: 1,
                    hits: vec![SearchHit {
                        match_scope: "line".into(),
                        excerpt_section: "title".into(),
                        includes_ocr: false,
                        object: object(),
                        revision_id: "r".into(),
                        capture_id: "c".into(),
                        title: "민법".into(),
                        section: "title".into(),
                        line: 1,
                        text: "민법".into(),
                        byte_start: 0,
                        byte_end: 6,
                        derived_ocr: false,
                    }],
                    next_cursor: first.then(|| "more".to_string()),
                    generation: 1,
                    corpus_complete: !first,
                    scanned_bytes: 0,
                    analyzer_version: "v".into(),
                    index_lag: 0,
                    collection_notices: vec![],
                })
            })
        }
    }
    fn object() -> ObjectId {
        ObjectId {
            jurisdiction: "kr".into(),
            provider: "law_go_kr".into(),
            dataset: Dataset::NationalStatute,
            id: "1".into(),
        }
    }

    #[tokio::test]
    async fn lookups_page_to_completion() {
        let pages = Arc::new(Pages(Mutex::new(Vec::new())));
        let lookup = ReferenceLookup::new(
            Arc::new(DatabaseService::new(Arc::new(History), Arc::new(Fixed))),
            Arc::new(SearchService::new(pages.clone())),
        );
        let found = lookup
            .find_lines(
                "title",
                vec![Dataset::NationalStatute],
                &["민 ?법".to_string()],
                None,
                false,
                Instant::now() + std::time::Duration::from_secs(5),
                CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(found.hits.len(), 2);
        assert!(!found.corpus_complete && !found.truncated);
        {
            let requests = pages.0.lock().unwrap();
            assert_eq!(requests.len(), 2);
            assert_eq!(requests[0].query, "^(?:민 ?법)$");
            assert_eq!(requests[0].sections, ["title"]);
            assert_eq!(requests[1].cursor.as_deref(), Some("more"));
        }
        let inventory = lookup
            .revisions(object(), CancellationToken::new())
            .await
            .unwrap();
        assert!(inventory.complete);
        assert_eq!(inventory.entries.len(), 2);
    }
}
