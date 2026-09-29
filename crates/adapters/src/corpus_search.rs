//! Search generations bind local index readers to durable corpus retention pins.
use crate::{
    corpus::PgCorpusStore,
    korean_query::CompiledQuery,
    search_index::{CorpusIndex, IndexSnapshot, IndexedCapture, filters_match},
};
use futures::future::BoxFuture;
use grep_matcher::Matcher;
use grep_regex::{RegexMatcher, RegexMatcherBuilder};
use openlegal_application::{
    database::DatabaseStore,
    search::{SearchBackend, SearchBudget, SearchMode},
};
use openlegal_domain::search_query::ParseErrorKind;
use openlegal_domain::{
    legal::{DatabaseError as E, RevisionSelector, SectionKind},
    legal_search::{SearchHit, SearchPage, SearchRequest},
};
use openlegal_normalization::search_query::SearchQueryProcessor;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::{Instant, SystemTime, UNIX_EPOCH},
};
use tokio_util::sync::CancellationToken;
#[derive(Clone, Default)]
struct Position {
    after: String,
    line: usize,
}
#[derive(Clone)]
struct Session {
    snapshot: IndexSnapshot,
    fingerprint: String,
    expires: u64,
    corpus_complete: bool,
    query: Option<Arc<CompiledQuery>>,
    literal: Option<Arc<RegexMatcher>>,
    regex: Option<Arc<RegexMatcher>>,
}
#[derive(Clone)]
struct Cursor {
    session: String,
    position: Position,
}
#[derive(Default)]
struct State {
    sessions: BTreeMap<String, Session>,
    cursors: BTreeMap<String, Cursor>,
}
struct SearchSessionGuard {
    id: String,
    state: Arc<Mutex<State>>,
    store: Arc<PgCorpusStore>,
    armed: bool,
}
impl SearchSessionGuard {
    fn new(id: String, state: Arc<Mutex<State>>, store: Arc<PgCorpusStore>) -> Self {
        Self {
            id,
            state,
            store,
            armed: true,
        }
    }
    fn clear_local(&self) {
        if let Ok(mut state) = self.state.lock() {
            state.sessions.remove(&self.id);
            state.cursors.retain(|_, cursor| cursor.session != self.id);
        }
    }
    async fn release(&mut self) -> Result<(), E> {
        self.clear_local();
        self.store.release_session(&self.id).await?;
        self.armed = false;
        Ok(())
    }
    fn retain(&mut self) {
        self.armed = false;
    }
}
impl Drop for SearchSessionGuard {
    fn drop(&mut self) {
        if self.armed {
            self.clear_local();
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                let store = self.store.clone();
                let id = self.id.clone();
                handle.spawn(async move {
                    // A canceled request has no response channel for cleanup errors.
                    // The durable pin also has its own ten-minute expiry.
                    let _ = store.release_session(&id).await;
                });
            }
        }
    }
}
#[derive(Clone)]
pub struct CorpusSearch {
    index: Arc<CorpusIndex>,
    store: Arc<PgCorpusStore>,
    state: Arc<Mutex<State>>,
    inventory_verified: Arc<std::sync::atomic::AtomicBool>,
}
fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
fn random() -> Result<String, E> {
    let mut bytes = [0; 32];
    getrandom::fill(&mut bytes).map_err(|_| E::Capacity)?;
    Ok(bytes.iter().map(|v| format!("{v:02x}")).collect())
}
impl CorpusSearch {
    pub fn new(index: Arc<CorpusIndex>, store: Arc<PgCorpusStore>) -> Self {
        Self {
            index,
            store,
            state: Default::default(),
            inventory_verified: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }
    pub fn with_coverage(mut self, verified: Arc<std::sync::atomic::AtomicBool>) -> Self {
        self.inventory_verified = verified;
        self
    }
    async fn execute(
        &self,
        mode: SearchMode,
        mut request: SearchRequest,
        mut budget: SearchBudget,
        cancel: CancellationToken,
    ) -> Result<SearchPage, E> {
        self.store.health().await?;
        let supplied = request.cursor.take();
        let fingerprint = format!(
            "{}:{}",
            if matches!(mode, SearchMode::Query) {
                "query"
            } else {
                "rg"
            },
            serde_json::to_string(&request).map_err(|_| E::InvalidInput)?
        );
        let mut new_guard: Option<SearchSessionGuard> = None;
        let (id, session, position) = if let Some(cursor) = supplied {
            let state = self.state.lock().map_err(|_| E::Capacity)?;
            let c = state.cursors.get(&cursor).ok_or(E::SessionExpired)?;
            let s = state.sessions.get(&c.session).ok_or(E::SessionExpired)?;
            if s.fingerprint != fingerprint {
                return Err(E::InvalidInput);
            }
            if s.expires <= now() {
                return Err(E::SessionExpired);
            }
            (c.session.clone(), s.clone(), c.position.clone())
        } else {
            let snapshot_index = self.index.clone();
            let query_text = request.query.clone();
            let compile_cancel = cancel.clone();
            let deadline = budget.deadline;
            let (prepared, returned_budget) = tokio::task::spawn_blocking(move || {
                let prepared = (|| {
                    let query = if matches!(mode, SearchMode::Query) && !request.literal {
                        let parsed = SearchQueryProcessor::new(&["title", "body", "case_number"])
                            .map_err(|_| E::InvalidInput)?
                            .parse(&query_text)
                            .map_err(|error| match error.kind {
                                ParseErrorKind::FieldShorthand => E::InvalidFieldShorthand,
                                _ => E::InvalidInput,
                            })?;
                        Some(Arc::new(snapshot_index.compile_query(
                            &parsed.expression,
                            deadline,
                            &compile_cancel,
                        )?))
                    } else {
                        None
                    };
                    let literal = if matches!(mode, SearchMode::Query) && request.literal {
                        Some(Arc::new(
                            RegexMatcherBuilder::new()
                                .case_insensitive(request.ignore_case)
                                .fixed_strings(true)
                                .size_limit(1024 * 1024)
                                .dfa_size_limit(1024 * 1024)
                                .build(&query_text)
                                .map_err(|_| E::InvalidInput)?,
                        ))
                    } else {
                        None
                    };
                    let regex = if matches!(mode, SearchMode::Ripgrep) {
                        Some(Arc::new(
                            RegexMatcherBuilder::new()
                                .multi_line(true)
                                .line_terminator(Some(b'\n'))
                                .case_insensitive(request.ignore_case)
                                .fixed_strings(request.literal)
                                .size_limit(1024 * 1024)
                                .dfa_size_limit(1024 * 1024)
                                .build(&query_text)
                                .map_err(|_| E::InvalidRegex)?,
                        ))
                    } else {
                        None
                    };
                    Ok::<_, E>((snapshot_index.snapshot()?, query, literal, regex))
                })();
                (prepared, budget)
            })
            .await
            .map_err(|_| E::Capacity)?;
            budget = returned_budget;
            let (snapshot, query, literal, regex) = prepared?;
            let id = random()?;
            let session = Session {
                snapshot: snapshot.clone(),
                query,
                literal,
                regex,
                fingerprint,
                expires: now() + 600,
                corpus_complete: !request.include_history
                    && !request.include_ocr
                    && request
                        .sections
                        .iter()
                        .all(|s| s == "title" || s == "body" || s == "case_number")
                    && self
                        .inventory_verified
                        .load(std::sync::atomic::Ordering::Acquire)
                    && self.store.watermark().await? == snapshot.generation
                    && self.store.current_coverage_ready(now()).await?,
            };
            {
                let mut state = self.state.lock().map_err(|_| E::Capacity)?;
                state.sessions.retain(|_, v| v.expires > now());
                let active: std::collections::HashSet<String> =
                    state.sessions.keys().cloned().collect();
                state.cursors.retain(|_, v| active.contains(&v.session));
                if state.sessions.len() >= 32 {
                    return Err(E::Capacity);
                }
                state.sessions.insert(id.clone(), session.clone());
            }
            new_guard = Some(SearchSessionGuard::new(
                id.clone(),
                self.state.clone(),
                self.store.clone(),
            ));
            self.store
                .pin_session(id.clone(), session.snapshot.generation, Vec::new(), now())
                .await?;
            (id, session, Position::default())
        };
        self.store.check_session(&id, now()).await?;
        let index = self.index.clone();
        let worker_session = session.clone();
        let worker_cancel = cancel.clone();
        let notice_datasets = if request.filters.datasets.is_empty() {
            vec![
                openlegal_domain::legal::Dataset::NationalStatute,
                openlegal_domain::legal::Dataset::AdministrativeRule,
                openlegal_domain::legal::Dataset::Ordinance,
                openlegal_domain::legal::Dataset::Treaty,
                openlegal_domain::legal::Dataset::Precedent,
                openlegal_domain::legal::Dataset::ConstitutionalDecision,
                openlegal_domain::legal::Dataset::LegalInterpretation,
                openlegal_domain::legal::Dataset::AdministrativeAppeal,
            ]
        } else {
            request.filters.datasets.clone()
        };
        let (result, _lease) = tokio::task::spawn_blocking(move || {
            let result = scan(
                &index,
                &worker_session,
                &request,
                position,
                &budget,
                &worker_cancel,
            );
            (result, budget.lease)
        })
        .await
        .map_err(|_| E::Capacity)?;
        let result = result?;
        if cancel.is_cancelled() {
            return Err(E::Cancelled);
        }
        // A source withdrawal invalidates the entire snapshot before returning any page.
        self.store.check_session(&id, now()).await?;
        for hit in &result.0 {
            self.store
                .resolve(
                    hit.object.clone(),
                    RevisionSelector::Capture {
                        id: hit.capture_id.clone(),
                    },
                    now(),
                    cancel.clone(),
                )
                .await?;
        }
        let collection_notices = self
            .store
            .collection_notices(&notice_datasets, None)
            .await?;
        let index_lag = self
            .store
            .watermark()
            .await?
            .saturating_sub(session.snapshot.generation);
        let next_cursor = if let Some(position) = result.1 {
            let token = random()?;
            let mut state = self.state.lock().map_err(|_| E::Capacity)?;
            if state.cursors.len() >= 4096 {
                return Err(E::Capacity);
            }
            state.cursors.insert(
                token.clone(),
                Cursor {
                    session: id.clone(),
                    position,
                },
            );
            Some(token)
        } else {
            None
        };
        let terminal = next_cursor.is_none();
        let page = SearchPage {
            schema_version: 1,
            hits: result.0,
            next_cursor,
            generation: session.snapshot.generation,
            corpus_complete: session.corpus_complete && collection_notices.is_empty(),
            scanned_bytes: result.2 as u64,
            analyzer_version: session.snapshot.analyzer_version.clone(),
            index_lag,
            collection_notices,
        };
        if terminal {
            if let Some(guard) = new_guard.as_mut() {
                guard.release().await?;
            } else {
                SearchSessionGuard::new(id.clone(), self.state.clone(), self.store.clone())
                    .release()
                    .await?;
            }
        } else if let Some(guard) = new_guard.as_mut() {
            guard.retain();
        }
        Ok(page)
    }
}
impl SearchBackend for CorpusSearch {
    fn search(
        &self,
        mode: SearchMode,
        request: SearchRequest,
        budget: SearchBudget,
        cancel: CancellationToken,
    ) -> BoxFuture<'static, Result<SearchPage, E>> {
        let this = self.clone();
        Box::pin(async move { this.execute(mode, request, budget, cancel).await })
    }
}
type ScanResult = (Vec<SearchHit>, Option<Position>, usize);
fn scan(
    index: &CorpusIndex,
    session: &Session,
    request: &SearchRequest,
    mut position: Position,
    budget: &SearchBudget,
    cancel: &CancellationToken,
) -> Result<ScanResult, E> {
    let snapshot = &session.snapshot;
    let expression = session.query.as_deref();
    let literal = session.literal.as_deref();
    let regex = session.regex.as_deref();
    let scan_deadline = budget
        .deadline
        .checked_sub(std::time::Duration::from_secs(2))
        .unwrap_or(budget.deadline);
    let mut hits = Vec::new();
    let mut scanned = 0usize;
    loop {
        if cancel.is_cancelled() {
            return Err(E::Cancelled);
        }
        if Instant::now() >= scan_deadline || scanned >= budget.bytes {
            return Ok((hits, Some(position), scanned));
        }
        let batch = match snapshot.batch(&position.after, 1, scan_deadline, cancel) {
            Err(E::BudgetExhausted) => {
                return Ok((hits, Some(position), scanned));
            }
            result => result?,
        };
        let Some((key, mut doc)) = batch.into_iter().next() else {
            return Ok((hits, None, scanned));
        };
        if (!request.include_history && !doc.current)
            || !filters_match(&request.filters, &doc.capture)
        {
            position = Position {
                after: key,
                line: 0,
            };
            continue;
        }
        let original_title = doc.capture.record.title.clone();
        let mut sections = vec![
            ("title".to_string(), doc.capture.record.title.clone(), false),
            ("body".to_string(), doc.capture.record.body.clone(), false),
        ];
        // The stored capture payload contains metadata even in existing index
        // generations, so this projection does not require an index rebuild.
        if let Some(case_number) = doc.capture.record.metadata.get("case_number") {
            sections.push(("case_number".into(), case_number.clone(), false));
        }
        for s in &doc.capture.record.sections {
            if s.kind != SectionKind::ProviderText
                && (s.kind != SectionKind::Ocr || request.include_ocr)
                && !(s.id == "case_number"
                    && doc.capture.record.metadata.contains_key("case_number"))
            {
                sections.push((s.id.clone(), s.text.clone(), s.kind == SectionKind::Ocr));
            }
        }
        if !request.sections.is_empty() {
            sections.retain(|(id, _, _)| request.sections.contains(id));
            for s in &doc.capture.record.sections {
                if request.sections.contains(&s.id)
                    && s.kind == SectionKind::ProviderText
                    && !(s.id == "case_number"
                        && doc.capture.record.metadata.contains_key("case_number"))
                {
                    sections.push((s.id.clone(), s.text.clone(), false));
                }
            }
        }
        let bytes = sections
            .iter()
            .map(|(_, text, _)| text.len())
            .sum::<usize>();
        if bytes > budget.bytes {
            return Err(E::Capacity);
        }
        if scanned.saturating_add(bytes) > budget.bytes {
            return Ok((hits, Some(position), scanned));
        }
        scanned += bytes;
        if let Some(expression) = &expression {
            let has_metadata_case = doc.capture.record.metadata.contains_key("case_number");
            if !sections.iter().any(|(name, _, _)| name == "case_number") {
                doc.capture.record.metadata.remove("case_number");
            }
            if expression.only_title() {
                doc.capture.record.body.clear();
                doc.body_tokens.clear();
            } else {
                let selected_body = sections
                    .iter()
                    .filter(|(name, _, _)| {
                        name != "title" && (name != "case_number" || !has_metadata_case)
                    })
                    .map(|(_, text, _)| text.as_str())
                    .collect::<Vec<_>>()
                    .join("\n");
                if expression.needs_tokens() && selected_body != doc.capture.record.body {
                    doc.body_tokens =
                        match index.tokens_with_budget(&selected_body, scan_deadline, cancel) {
                            Err(E::BudgetExhausted) => {
                                return Ok((hits, Some(position), scanned - bytes));
                            }
                            result => result?,
                        };
                }
                doc.capture.record.body = selected_body;
            }
            if !expression.needs_tokens() {
                doc.body_tokens.clear();
                doc.title_tokens.clear();
            }
            if !sections.iter().any(|(name, _, _)| name == "title") {
                doc.capture.record.title.clear();
                doc.title_tokens.clear();
            }
            let matches = match index.matches(expression, &doc, scan_deadline, cancel) {
                Err(E::BudgetExhausted) => {
                    return Ok((hits, Some(position), scanned - bytes));
                }
                result => result?,
            };
            if matches {
                let (name, source, ocr) = sections
                    .iter()
                    .find(|(_, text, _)| !text.is_empty())
                    .cloned()
                    .unwrap_or_default();
                let text = source.chars().take(512).collect::<String>();
                let mut found = hit(&doc, "object", 0, 0, text.len(), text, ocr);
                found.title = original_title;
                found.match_scope = "object".into();
                found.excerpt_section = name;
                found.includes_ocr = sections.iter().any(|(_, _, o)| *o);
                hits.push(found);
            }
            position = Position {
                after: key,
                line: 0,
            };
        } else if let Some(literal) = literal {
            let mut matched = None;
            for (name, source, ocr) in &sections {
                if literal
                    .find(source.as_bytes())
                    .map_err(|_| E::InvalidInput)?
                    .is_some()
                {
                    matched = Some((name, source, ocr));
                    break;
                }
            }
            if let Some((name, source, ocr)) = matched {
                let text = source.chars().take(512).collect::<String>();
                let mut found = hit(&doc, "object", 0, 0, text.len(), text, *ocr);
                found.title = original_title;
                found.excerpt_section = name.clone();
                found.includes_ocr = sections.iter().any(|(_, _, o)| *o);
                hits.push(found);
            }
            position = Position {
                after: key,
                line: 0,
            };
        } else if let Some(regex) = &regex {
            let mut line_index = 0;
            let mut next_line = position.line;
            for (name, text, ocr) in sections {
                let lines: Vec<&str> = text.split_inclusive('\n').collect();
                let mut offset = 0;
                for (i, line) in lines.iter().enumerate() {
                    if cancel.is_cancelled() {
                        return Err(E::Cancelled);
                    }
                    if Instant::now() >= scan_deadline {
                        return Ok((
                            hits,
                            Some(Position {
                                after: position.after,
                                line: line_index,
                            }),
                            scanned,
                        ));
                    }
                    if line_index >= position.line
                        && let Some(found) =
                            regex.find(line.as_bytes()).map_err(|_| E::InvalidInput)?
                    {
                        let start = i.saturating_sub(request.context_lines as usize);
                        let end = (i + request.context_lines as usize + 1).min(lines.len());
                        let snippet = lines[start..end].concat();
                        if snippet.len() > 32768 {
                            return Err(E::Capacity);
                        }
                        hits.push(hit(
                            &doc,
                            &name,
                            i as u64 + 1,
                            offset + found.start(),
                            offset + found.end(),
                            snippet,
                            ocr,
                        ));
                        if hits.len() >= request.limit {
                            return Ok((
                                hits,
                                Some(Position {
                                    after: position.after,
                                    line: line_index + 1,
                                }),
                                scanned,
                            ));
                        }
                    }
                    offset += line.len();
                    line_index += 1;
                    next_line = line_index;
                }
            }
            let _ = next_line;
            position = Position {
                after: key,
                line: 0,
            };
        }
        if hits.len() >= request.limit {
            return Ok((hits, Some(position), scanned));
        }
    }
}
fn hit(
    doc: &IndexedCapture,
    section: &str,
    line: u64,
    start: usize,
    end: usize,
    text: String,
    ocr: bool,
) -> SearchHit {
    SearchHit {
        match_scope: "line".into(),
        excerpt_section: section.into(),
        includes_ocr: ocr,
        object: doc.capture.record.object.clone(),
        revision_id: doc.capture.record.revision_id.clone(),
        capture_id: doc.capture.capture_id.clone(),
        title: doc.capture.record.title.clone(),
        section: section.into(),
        line,
        text,
        byte_start: start,
        byte_end: end,
        derived_ocr: ocr,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::korean_analysis::KoreanAnalyzer;
    use openlegal_domain::{
        legal::{Capture, Dataset, LegalRecord, LegalSection, ObjectId},
        legal_search::Filters,
    };
    use tokio::sync::Semaphore;

    fn capture(id: &str) -> Capture {
        Capture {
            capture_id: format!("{id:0>64}"),
            sequence: 1,
            record: LegalRecord {
                object: ObjectId {
                    jurisdiction: "kr".into(),
                    provider: "fixture".into(),
                    dataset: Dataset::Precedent,
                    id: id.into(),
                },
                revision_id: "r1".into(),
                title: "Fictional judgment".into(),
                body: "Unrelated text".into(),
                metadata: BTreeMap::from([("case_number".into(), "2018도14262".into())]),
                publication_date: None,
                effective_date: None,
                source_url: "https://example.test/fictional".into(),
                representation: "fictional_text".into(),
                sections: vec![],
            },
            retrieved_at: 1,
            captured_at: 1,
            validated_at: 1,
            processor_version: "fixture_v1".into(),
            raw_sha256: "c".repeat(64),
        }
    }

    fn request(query: &str, sections: &[&str], limit: usize) -> SearchRequest {
        SearchRequest {
            query: query.into(),
            filters: Filters::default(),
            include_history: false,
            include_ocr: false,
            sections: sections.iter().map(|s| (*s).into()).collect(),
            limit,
            cursor: None,
            literal: false,
            ignore_case: false,
            context_lines: 0,
        }
    }

    fn budget() -> SearchBudget {
        SearchBudget {
            bytes: 64 * 1024 * 1024,
            deadline: Instant::now() + std::time::Duration::from_secs(10),
            lease: Arc::new(Semaphore::new(1)).try_acquire_owned().unwrap(),
        }
    }

    fn session(index: &CorpusIndex, mode: SearchMode, query: &str) -> Session {
        let parsed = SearchQueryProcessor::new(&["title", "body", "case_number"])
            .unwrap()
            .parse(query)
            .unwrap();
        let query = matches!(mode, SearchMode::Query).then(|| {
            Arc::new(
                index
                    .compile_query(
                        &parsed.expression,
                        Instant::now() + std::time::Duration::from_secs(10),
                        &CancellationToken::new(),
                    )
                    .unwrap(),
            )
        });
        let regex = matches!(mode, SearchMode::Ripgrep).then(|| {
            Arc::new(
                RegexMatcherBuilder::new()
                    .multi_line(true)
                    .line_terminator(Some(b'\n'))
                    .build(&parsed.source)
                    .unwrap(),
            )
        });
        Session {
            snapshot: index.snapshot().unwrap(),
            fingerprint: String::new(),
            expires: now() + 600,
            corpus_complete: false,
            query,
            literal: None,
            regex,
        }
    }

    #[test]
    fn existing_capture_metadata_is_searchable_without_index_rebuild() {
        let dir = tempfile::tempdir().unwrap();
        let index = CorpusIndex::open(dir.path(), KoreanAnalyzer::fixture()).unwrap();
        index.apply_capture(capture("1"), true, 1).unwrap();
        drop(index);
        let index = CorpusIndex::open(dir.path(), KoreanAnalyzer::fixture()).unwrap();
        for (query, sections, expected) in [
            ("\"2018도14262\"", vec![], 1),
            ("2018도14262", vec![], 1),
            ("in:case_number:\"2018도14262\"", vec![], 1),
            ("in:body:\"2018도14262\"", vec![], 0),
            ("\"2018도14262\"", vec!["body"], 0),
        ] {
            let result = scan(
                &index,
                &session(&index, SearchMode::Query, query),
                &request(query, &sections, 20),
                Position::default(),
                &budget(),
                &CancellationToken::new(),
            )
            .unwrap();
            assert_eq!(result.0.len(), expected, "{query} {sections:?}");
        }
        for (sections, expected) in [(vec![], 1), (vec!["case_number"], 1), (vec!["body"], 0)] {
            let result = scan(
                &index,
                &session(&index, SearchMode::Ripgrep, "2018도14262"),
                &request("2018도14262", &sections, 20),
                Position::default(),
                &budget(),
                &CancellationToken::new(),
            )
            .unwrap();
            assert_eq!(result.0.len(), expected, "{sections:?}");
            if expected == 1 {
                let hit = &result.0[0];
                assert_eq!(hit.section, "case_number");
                assert_eq!(hit.line, 1);
                assert_eq!((hit.byte_start, hit.byte_end), (0, "2018도14262".len()));
            }
        }
    }

    #[test]
    fn case_number_rg_cursor_resumes_after_one_hit_per_capture() {
        let dir = tempfile::tempdir().unwrap();
        let index = CorpusIndex::open(dir.path(), KoreanAnalyzer::fixture()).unwrap();
        for id in ["1", "2"] {
            index
                .apply_capture(capture(id), true, id.parse().unwrap())
                .unwrap();
        }
        let search = session(&index, SearchMode::Ripgrep, "2018도14262");
        let first = scan(
            &index,
            &search,
            &request("2018도14262", &[], 1),
            Position::default(),
            &budget(),
            &CancellationToken::new(),
        )
        .unwrap();
        assert_eq!(first.0.len(), 1);
        let second = scan(
            &index,
            &search,
            &request("2018도14262", &[], 1),
            first.1.unwrap(),
            &budget(),
            &CancellationToken::new(),
        )
        .unwrap();
        assert_eq!(second.0.len(), 1);
        assert_ne!(first.0[0].object.id, second.0[0].object.id);
    }

    #[test]
    fn literal_query_matches_contiguous_source_text_and_respects_case() {
        let dir = tempfile::tempdir().unwrap();
        let index = CorpusIndex::open(dir.path(), KoreanAnalyzer::fixture()).unwrap();
        let mut separated = capture("1");
        separated.record.body = "119 구조".into();
        let mut contiguous = capture("2");
        contiguous.record.body = "119구조".into();
        index.apply_capture(separated, true, 1).unwrap();
        index.apply_capture(contiguous, true, 2).unwrap();
        for (query, ignore_case, expected) in [
            ("119구조", false, 1),
            ("119 구조", false, 1),
            ("119구조", true, 1),
            ("FICTIONAL", false, 0),
            ("FICTIONAL", true, 2),
        ] {
            let mut search = session(&index, SearchMode::Query, "");
            search.query = None;
            search.literal = Some(Arc::new(
                RegexMatcherBuilder::new()
                    .fixed_strings(true)
                    .case_insensitive(ignore_case)
                    .build(query)
                    .unwrap(),
            ));
            let result = scan(
                &index,
                &search,
                &request(query, &[], 20),
                Position::default(),
                &budget(),
                &CancellationToken::new(),
            )
            .unwrap();
            assert_eq!(result.0.len(), expected, "{query} {ignore_case}");
            if query == "119구조" {
                assert_eq!(result.0[0].object.id, "2");
                assert_eq!(result.0[0].excerpt_section, "body");
            }
        }
    }

    #[test]
    fn title_scope_does_not_analyze_unrelated_extracted_text() {
        let dir = tempfile::tempdir().unwrap();
        let index = CorpusIndex::open(dir.path(), KoreanAnalyzer::fixture()).unwrap();
        let mut record = capture("1");
        record.record.sections.push(LegalSection {
            id: "attachment".into(),
            title: "Fictional attachment".into(),
            text: "x".repeat(129),
            kind: SectionKind::Extracted,
            source_document_sha256: None,
            page: None,
        });
        index.apply_capture(record, true, 1).unwrap();
        let query = "in:title:Fictional";
        let result = scan(
            &index,
            &session(&index, SearchMode::Query, query),
            &request(query, &[], 20),
            Position::default(),
            &budget(),
            &CancellationToken::new(),
        )
        .unwrap();
        assert_eq!(result.0.len(), 1);
        assert_eq!(result.0[0].object.id, "1");
    }

    #[test]
    fn deadline_page_can_resume_without_skipping_a_capture() {
        let dir = tempfile::tempdir().unwrap();
        let index = CorpusIndex::open(dir.path(), KoreanAnalyzer::fixture()).unwrap();
        index.apply_capture(capture("1"), true, 1).unwrap();
        let query = "in:title:Fictional";
        let search = session(&index, SearchMode::Query, query);
        let mut short = budget();
        short.deadline = Instant::now() + std::time::Duration::from_millis(10);
        let first = scan(
            &index,
            &search,
            &request(query, &[], 20),
            Position::default(),
            &short,
            &CancellationToken::new(),
        )
        .unwrap();
        assert!(first.0.is_empty());
        let resumed = scan(
            &index,
            &search,
            &request(query, &[], 20),
            first.1.unwrap(),
            &budget(),
            &CancellationToken::new(),
        )
        .unwrap();
        assert_eq!(resumed.0.len(), 1);
    }
}
