//! Search generations bind local index readers to durable corpus retention pins.
use crate::{
    corpus::PgCorpusStore,
    korean_query::CompiledQuery,
    search_index::{CorpusIndex, IndexSnapshot, IndexedCapture, filters_match},
};
use futures::future::BoxFuture;
use grep_matcher::Matcher;
use grep_regex::RegexMatcherBuilder;
use openlegal_application::{
    database::DatabaseStore,
    search::{SearchBackend, SearchBudget, SearchMode},
};
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
                    let query = if matches!(mode, SearchMode::Query) {
                        let parsed = SearchQueryProcessor::new(&["title", "body"])
                            .map_err(|_| E::InvalidInput)?
                            .parse(&query_text)
                            .map_err(|_| E::InvalidInput)?;
                        Some(Arc::new(snapshot_index.compile_query(
                            &parsed.expression,
                            deadline,
                            &compile_cancel,
                        )?))
                    } else {
                        None
                    };
                    Ok::<_, E>((snapshot_index.snapshot()?, query))
                })();
                (prepared, budget)
            })
            .await
            .map_err(|_| E::Capacity)?;
            budget = returned_budget;
            let (snapshot, query) = prepared?;
            let id = random()?;
            let session = Session {
                snapshot: snapshot.clone(),
                query,
                fingerprint,
                expires: now() + 600,
                corpus_complete: !request.include_history
                    && !request.include_ocr
                    && request.sections.iter().all(|s| s == "title" || s == "body")
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
            if let Err(e) = self
                .store
                .pin_session(id.clone(), session.snapshot.generation, Vec::new(), now())
                .await
            {
                self.state
                    .lock()
                    .map_err(|_| E::Capacity)?
                    .sessions
                    .remove(&id);
                return Err(e);
            }
            (id, session, Position::default())
        };
        self.store.check_session(&id, now()).await?;
        let index = self.index.clone();
        let worker_session = session.clone();
        let worker_cancel = cancel.clone();
        let (result, _lease) = tokio::task::spawn_blocking(move || {
            let result = scan(
                &index,
                &worker_session,
                mode,
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
        let next_cursor = if let Some(position) = result.1 {
            let token = random()?;
            let mut state = self.state.lock().map_err(|_| E::Capacity)?;
            if state.cursors.len() >= 4096 {
                return Err(E::Capacity);
            }
            state.cursors.insert(
                token.clone(),
                Cursor {
                    session: id,
                    position,
                },
            );
            Some(token)
        } else {
            None
        };
        Ok(SearchPage {
            schema_version: 1,
            hits: result.0,
            next_cursor,
            generation: session.snapshot.generation,
            corpus_complete: session.corpus_complete,
            scanned_bytes: result.2 as u64,
            analyzer_version: session.snapshot.analyzer_version.clone(),
            index_lag: self
                .store
                .watermark()
                .await?
                .saturating_sub(session.snapshot.generation),
        })
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
    mode: SearchMode,
    request: &SearchRequest,
    mut position: Position,
    budget: &SearchBudget,
    cancel: &CancellationToken,
) -> Result<ScanResult, E> {
    let snapshot = &session.snapshot;
    let expression = session.query.as_deref();
    let regex = if matches!(mode, SearchMode::Ripgrep) {
        Some(
            RegexMatcherBuilder::new()
                .multi_line(true)
                .line_terminator(Some(b'\n'))
                .case_insensitive(request.ignore_case)
                .fixed_strings(request.literal)
                .size_limit(1024 * 1024)
                .dfa_size_limit(1024 * 1024)
                .build(&request.query)
                .map_err(|_| E::InvalidInput)?,
        )
    } else {
        None
    };
    let mut hits = Vec::new();
    let mut scanned = 0usize;
    loop {
        if cancel.is_cancelled() {
            return Err(E::Cancelled);
        }
        if Instant::now() >= budget.deadline || scanned >= budget.bytes {
            return Ok((hits, Some(position), scanned));
        }
        let batch = snapshot.batch(&position.after, 1, budget.deadline, cancel)?;
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
        for s in &doc.capture.record.sections {
            if s.kind != SectionKind::ProviderText
                && (s.kind != SectionKind::Ocr || request.include_ocr)
            {
                sections.push((s.id.clone(), s.text.clone(), s.kind == SectionKind::Ocr));
            }
        }
        if !request.sections.is_empty() {
            sections.retain(|(id, _, _)| request.sections.contains(id));
            for s in &doc.capture.record.sections {
                if request.sections.contains(&s.id) && s.kind == SectionKind::ProviderText {
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
            doc.capture.record.body = sections
                .iter()
                .filter(|(name, _, _)| name != "title")
                .map(|(_, text, _)| text.as_str())
                .collect::<Vec<_>>()
                .join("\n");
            doc.body_tokens =
                index.tokens_with_budget(&doc.capture.record.body, budget.deadline, cancel)?;
            if !sections.iter().any(|(name, _, _)| name == "title") {
                doc.capture.record.title.clear();
                doc.title_tokens.clear();
            }
            if index.matches(expression, &doc) {
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
                    if Instant::now() >= budget.deadline {
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
