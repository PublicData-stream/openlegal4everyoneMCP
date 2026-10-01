//! In-memory corpus and search backend for legal reference tool tests. Searches use
//! the same ripgrep regex engine as the corpus adapter, line by line.
use futures::future::BoxFuture;
use grep_matcher::Matcher;
use grep_regex::RegexMatcherBuilder;
use openlegal_application::{
    Clock,
    database::{DatabaseService, DatabaseStore},
    legal_reference::ReferenceLookup,
    search::{SearchBackend, SearchBudget, SearchMode, SearchService},
};
use openlegal_domain::{
    legal::{
        Capture, DatabaseError, Dataset, HistoryEntry, HistoryKind, HistoryPage, LegalRecord,
        LegalSection, ObjectId, RevisionSelector, SectionKind,
    },
    legal_search::{SearchHit, SearchPage, SearchRequest},
};
use std::{sync::Arc, time::Instant};
use tokio_util::sync::CancellationToken;

pub const NOW: u64 = 1_000_000;

struct Fixed;
impl Clock for Fixed {
    fn now(&self) -> u64 {
        NOW
    }
}

#[derive(Clone)]
pub struct Stored {
    pub capture: Capture,
    pub head: bool,
}

pub struct TestCorpus(pub Vec<Stored>);

pub fn object(dataset: Dataset, id: &str) -> ObjectId {
    ObjectId {
        jurisdiction: "kr".into(),
        provider: "law_go_kr".into(),
        dataset,
        id: id.into(),
    }
}

pub fn section(id: &str, title: &str, text: &str, kind: SectionKind) -> LegalSection {
    LegalSection {
        id: id.into(),
        title: title.into(),
        text: text.into(),
        kind,
        source_document_sha256: None,
        page: None,
    }
}

pub fn article(key: &str, title: &str, text: &str) -> LegalSection {
    section(
        &format!("article:{key}"),
        title,
        text,
        SectionKind::ProviderText,
    )
}

/// A fictional retained capture. `n` makes the capture ID unique.
pub struct Record<'a> {
    pub object: ObjectId,
    pub n: u64,
    pub revision: &'a str,
    pub title: &'a str,
    pub effective: Option<&'a str>,
    pub sections: Vec<LegalSection>,
    pub metadata: &'a [(&'a str, &'a str)],
    pub head: bool,
}

impl Record<'_> {
    pub fn build(self) -> Stored {
        Stored {
            capture: Capture {
                capture_id: format!("{:064x}", self.n),
                sequence: self.n,
                record: LegalRecord {
                    object: self.object,
                    revision_id: self.revision.into(),
                    title: self.title.into(),
                    body: self
                        .sections
                        .iter()
                        .filter(|s| s.kind == SectionKind::ProviderText)
                        .map(|s| s.text.as_str())
                        .collect::<Vec<_>>()
                        .join("\n"),
                    metadata: self
                        .metadata
                        .iter()
                        .map(|(k, v)| ((*k).into(), (*v).into()))
                        .collect(),
                    publication_date: self.effective.map(|_| "20000101".into()),
                    effective_date: self.effective.map(Into::into),
                    source_url: "https://example.test/fictional".into(),
                    representation: "provider_effective_original".into(),
                    sections: self.sections,
                },
                retrieved_at: NOW,
                captured_at: NOW,
                validated_at: NOW,
                processor_version: "v1".into(),
                raw_sha256: "b".repeat(64),
            },
            head: self.head,
        }
    }
}

impl DatabaseStore for TestCorpus {
    fn resolve(
        &self,
        object: ObjectId,
        selector: RevisionSelector,
        _: u64,
        _: CancellationToken,
    ) -> BoxFuture<'static, Result<Capture, DatabaseError>> {
        let found = self
            .0
            .iter()
            .find(|s| {
                s.capture.record.object == object
                    && match &selector {
                        RevisionSelector::Head => s.head,
                        RevisionSelector::Revision { id } => s.capture.record.revision_id == *id,
                        RevisionSelector::Capture { id } => s.capture.capture_id == *id,
                        _ => false,
                    }
            })
            .map(|s| s.capture.clone())
            .ok_or(DatabaseError::NotObserved);
        Box::pin(async move { found })
    }
    fn history(
        &self,
        object: ObjectId,
        _: HistoryKind,
        _: Option<String>,
        _: usize,
        _: u64,
        _: CancellationToken,
    ) -> BoxFuture<'static, Result<HistoryPage, DatabaseError>> {
        let entries = self
            .0
            .iter()
            .filter(|s| s.capture.record.object == object)
            .map(|s| HistoryEntry {
                revision_id: s.capture.record.revision_id.clone(),
                capture_id: Some(s.capture.capture_id.clone()),
                sequence: s.capture.sequence,
                captured_at: Some(NOW),
                publication_date: s.capture.record.publication_date.clone(),
                effective_date: s.capture.record.effective_date.clone(),
            })
            .collect();
        Box::pin(async move {
            Ok(HistoryPage {
                entries,
                next_cursor: None,
                inventory_complete: true,
            })
        })
    }
}

fn section_text(record: &LegalRecord, section: &str) -> Option<String> {
    match section {
        "title" => Some(record.title.clone()),
        "body" => Some(record.body.clone()),
        "case_number" => record.metadata.get("case_number").cloned(),
        id => record
            .sections
            .iter()
            .find(|s| s.id == id)
            .map(|s| s.text.clone()),
    }
}

impl SearchBackend for TestCorpus {
    fn search(
        &self,
        mode: SearchMode,
        request: SearchRequest,
        _: SearchBudget,
        _: CancellationToken,
    ) -> BoxFuture<'static, Result<SearchPage, DatabaseError>> {
        assert!(matches!(mode, SearchMode::Ripgrep));
        let matcher = RegexMatcherBuilder::new()
            .multi_line(true)
            .line_terminator(Some(b'\n'))
            .build(&request.query)
            .expect("generated pattern compiles");
        let mut hits = Vec::new();
        for stored in self
            .0
            .iter()
            .filter(|s| request.include_history || s.head)
            .filter(|s| {
                request.filters.datasets.is_empty()
                    || request
                        .filters
                        .datasets
                        .contains(&s.capture.record.object.dataset)
            })
            .filter(|s| {
                request
                    .filters
                    .object_id
                    .as_ref()
                    .is_none_or(|id| *id == s.capture.record.object.id)
            })
        {
            let record = &stored.capture.record;
            for name in &request.sections {
                let Some(text) = section_text(record, name) else {
                    continue;
                };
                for (number, line) in text.lines().enumerate() {
                    if matcher.is_match(line.as_bytes()).expect("match") {
                        hits.push(SearchHit {
                            match_scope: "line".into(),
                            excerpt_section: name.clone(),
                            includes_ocr: false,
                            object: record.object.clone(),
                            revision_id: record.revision_id.clone(),
                            capture_id: stored.capture.capture_id.clone(),
                            title: record.title.clone(),
                            section: name.clone(),
                            line: number as u64 + 1,
                            text: line.to_string(),
                            byte_start: 0,
                            byte_end: 0,
                            derived_ocr: false,
                        });
                    }
                }
            }
        }
        Box::pin(async move {
            Ok(SearchPage {
                schema_version: 1,
                hits,
                next_cursor: None,
                generation: 1,
                corpus_complete: true,
                scanned_bytes: 0,
                analyzer_version: "test".into(),
                index_lag: 0,
                collection_notices: vec![],
            })
        })
    }
}

pub fn lookup(records: Vec<Stored>) -> ReferenceLookup {
    let corpus = Arc::new(TestCorpus(records));
    ReferenceLookup::new(
        Arc::new(DatabaseService::new(corpus.clone(), Arc::new(Fixed))),
        Arc::new(SearchService::new(corpus)),
    )
}

pub fn deadline() -> Instant {
    Instant::now() + std::time::Duration::from_secs(5)
}
