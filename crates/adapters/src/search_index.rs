//! Rebuildable Tantivy corpus generations and Korean analysis. No provider I/O.
use crate::{
    korean_analysis::{AnalyzedText, KoreanAnalyzer},
    korean_query::CompiledQuery,
};
use openlegal_domain::{
    legal::{Capture, DatabaseError as E},
    legal_search::{DateKind, Filters},
};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Instant,
};
use tantivy::{
    DocAddress, DocSet, Index, IndexReader, IndexWriter, ReloadPolicy, Searcher, TERMINATED, Term,
    schema::{Field, IndexRecordOption, STORED, STRING, Schema, Value},
};
use tokio_util::sync::CancellationToken;
const INDEX_FORMAT: u32 = 2;
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct IndexMetadata {
    format: u32,
    analyzer: String,
    generation: u64,
    complete: bool,
}
#[derive(Clone, Serialize, Deserialize)]
pub struct IndexedCapture {
    pub capture: Capture,
    pub current: bool,
    pub title_tokens: AnalyzedText,
    pub body_tokens: AnalyzedText,
}
#[derive(Clone)]
pub struct IndexSnapshot {
    pub reader: Searcher,
    pub generation: u64,
    pub analyzer_version: String,
    key: Field,
    payload: Field,
}
pub struct CorpusIndex {
    index: Index,
    reader: IndexReader,
    writer: Mutex<IndexWriter>,
    key: Field,
    object: Field,
    payload: Field,
    analyzer: Arc<KoreanAnalyzer>,
    complete: AtomicBool,
    mutation: Mutex<()>,
}
fn err(_: impl std::fmt::Debug) -> E {
    E::StorageCorrupt
}
fn object_key(c: &Capture) -> String {
    format!(
        "{}/{}/{:?}/{}",
        c.record.object.jurisdiction,
        c.record.object.provider,
        c.record.object.dataset,
        c.record.object.id
    )
}
fn record_key(c: &Capture) -> String {
    format!(
        "{}/{}/{}",
        object_key(c),
        hex(c.record.revision_id.as_bytes()),
        c.capture_id
    )
}
fn hex(v: &[u8]) -> String {
    v.iter().map(|b| format!("{b:02x}")).collect()
}
impl CorpusIndex {
    pub fn open(path: &Path, analyzer: Arc<KoreanAnalyzer>) -> Result<Arc<Self>, E> {
        Self::open_inner(path, analyzer, false)
    }
    /// Creates only a fresh destination; incomplete generations cannot be served.
    pub fn create_rebuild(path: &Path, analyzer: Arc<KoreanAnalyzer>) -> Result<Arc<Self>, E> {
        if path.exists() && std::fs::read_dir(path).map_err(err)?.next().is_some() {
            return Err(E::Conflict);
        }
        Self::open_inner(path, analyzer, true)
    }
    fn open_inner(
        path: &Path,
        analyzer: Arc<KoreanAnalyzer>,
        rebuilding: bool,
    ) -> Result<Arc<Self>, E> {
        let mut schema = Schema::builder();
        let key = schema.add_text_field("key", STRING | STORED);
        let object = schema.add_text_field("object", STRING);
        let payload = schema.add_text_field("payload", STORED);
        let schema = schema.build();
        std::fs::create_dir_all(path).map_err(err)?;
        let directory = tantivy::directory::MmapDirectory::open(path).map_err(err)?;
        let existing = Index::exists(&directory).map_err(err)?;
        let index = Index::open_or_create(directory, schema).map_err(err)?;
        if existing {
            let meta = index.load_metas().map_err(err)?;
            let meta: IndexMetadata =
                serde_json::from_str(meta.payload.as_deref().ok_or(E::StorageCorrupt)?)
                    .map_err(err)?;
            if meta.format != INDEX_FORMAT || meta.analyzer != analyzer.identity() || !meta.complete
            {
                return Err(E::StorageCorrupt);
            }
        }
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()
            .map_err(err)?;
        let writer = index
            .writer_with_num_threads(1, 32 * 1024 * 1024)
            .map_err(err)?;
        let result = Arc::new(Self {
            index,
            reader,
            writer: Mutex::new(writer),
            key,
            object,
            payload,
            analyzer,
            complete: AtomicBool::new(!rebuilding),
            mutation: Mutex::new(()),
        });
        if !existing {
            result.advance_generation(0)?;
        }
        Ok(result)
    }
    pub fn tokens(&self, text: &str) -> Result<AnalyzedText, E> {
        self.tokens_with_budget(
            text,
            Instant::now() + std::time::Duration::from_secs(10),
            &CancellationToken::new(),
        )
    }
    pub fn tokens_with_budget(
        &self,
        text: &str,
        deadline: Instant,
        cancel: &CancellationToken,
    ) -> Result<AnalyzedText, E> {
        self.analyzer.analyze(text, deadline, cancel)
    }
    pub fn compile_query(
        &self,
        expression: &openlegal_domain::search_query::Expr,
        deadline: Instant,
        cancel: &CancellationToken,
    ) -> Result<CompiledQuery, E> {
        CompiledQuery::compile(expression, &self.analyzer, deadline, cancel)
    }
    fn metadata(&self, generation: u64, complete: bool) -> Result<String, E> {
        serde_json::to_string(&IndexMetadata {
            format: INDEX_FORMAT,
            analyzer: self.analyzer.identity().into(),
            generation,
            complete,
        })
        .map_err(err)
    }
    /// Durable completion happens before the database acknowledgment.
    pub fn finish_rebuild(&self) -> Result<(), E> {
        let _mutation = self.mutation.lock().map_err(err)?;
        let generation = self.snapshot()?.generation;
        let mut writer = self.writer.lock().map_err(err)?;
        let mut commit = writer.prepare_commit().map_err(err)?;
        commit.set_payload(&self.metadata(generation, true)?);
        commit.commit().map_err(err)?;
        self.reader.reload().map_err(err)?;
        self.complete.store(true, Ordering::Release);
        Ok(())
    }
    pub fn snapshot(&self) -> Result<IndexSnapshot, E> {
        let _writer = self.writer.lock().map_err(err)?;
        let meta = self.index.load_metas().map_err(err)?;
        let metadata: IndexMetadata =
            serde_json::from_str(meta.payload.as_deref().ok_or(E::StorageCorrupt)?).map_err(err)?;
        if metadata.format != INDEX_FORMAT || metadata.analyzer != self.analyzer.identity() {
            return Err(E::StorageCorrupt);
        }
        Ok(IndexSnapshot {
            reader: self.reader.searcher(),
            generation: metadata.generation,
            analyzer_version: metadata.analyzer,
            key: self.key,
            payload: self.payload,
        })
    }
    /// A whole object's revision set is replaced atomically; prior readers retain old segments.
    pub fn replace_object(
        &self,
        captures: Vec<Capture>,
        head: Option<&str>,
        generation: u64,
    ) -> Result<(), E> {
        if captures.is_empty() {
            return Err(E::InvalidInput);
        }
        let object = object_key(&captures[0]);
        let _mutation = self.mutation.lock().map_err(err)?;
        if captures.len() > 1000 {
            return Err(E::Capacity);
        }
        let mut aggregate = 0usize;
        let mut documents = Vec::new();
        for capture in captures {
            if object_key(&capture) != object {
                return Err(E::InvalidInput);
            }
            capture.record.validate()?;
            let indexed = IndexedCapture {
                current: head == Some(capture.capture_id.as_str()),
                title_tokens: self.tokens(&capture.record.title)?,
                body_tokens: self.tokens(&capture.record.body)?,
                capture,
            };
            let payload = serde_json::to_string(&indexed).map_err(err)?;
            aggregate = aggregate.saturating_add(payload.len());
            if aggregate > 64 * 1024 * 1024 || payload.len() > 48 * 1024 * 1024 {
                return Err(E::Capacity);
            }
            documents.push(tantivy::doc!(self.key=>record_key(&indexed.capture),self.object=>object.clone(),self.payload=>payload));
        }
        let mut writer = self.writer.lock().map_err(err)?;
        writer.delete_term(Term::from_field_text(self.object, &object));
        for doc in documents {
            writer.add_document(doc).map_err(err)?;
        }
        let mut commit = writer.prepare_commit().map_err(err)?;
        commit.set_payload(&self.metadata(generation, self.complete.load(Ordering::Acquire))?);
        commit.commit().map_err(err)?;
        self.reader.reload().map_err(err)?;
        Ok(())
    }
    pub fn remove_object(
        &self,
        object: &openlegal_domain::legal::ObjectId,
        generation: u64,
    ) -> Result<(), E> {
        let _mutation = self.mutation.lock().map_err(err)?;
        let key = format!(
            "{} / {} / {:?} / {}",
            object.jurisdiction, object.provider, object.dataset, object.id
        )
        .replace(" / ", "/");
        let mut writer = self.writer.lock().map_err(err)?;
        writer.delete_term(Term::from_field_text(self.object, &key));
        let mut commit = writer.prepare_commit().map_err(err)?;
        commit.set_payload(&self.metadata(generation, self.complete.load(Ordering::Acquire))?);
        commit.commit().map_err(err)?;
        self.reader.reload().map_err(err)?;
        Ok(())
    }
    pub fn matches(&self, expression: &CompiledQuery, doc: &IndexedCapture) -> bool {
        expression.matches(
            &doc.capture.record.title,
            &doc.title_tokens,
            &doc.capture.record.body,
            &doc.body_tokens,
        )
    }
}
impl IndexSnapshot {
    /// Merge bounded lexicographic term ranges instead of collecting the entire corpus.
    pub fn batch(
        &self,
        after: &str,
        limit: usize,
        deadline: Instant,
        cancel: &CancellationToken,
    ) -> Result<Vec<(String, IndexedCapture)>, E> {
        if limit == 0 || limit > 100 || self.reader.segment_readers().len() > 128 {
            return Err(E::Capacity);
        }
        let mut candidates = BTreeMap::new();
        for (ordinal, segment) in self.reader.segment_readers().iter().enumerate() {
            if cancel.is_cancelled() {
                return Err(E::Cancelled);
            }
            if Instant::now() >= deadline {
                return Err(E::Capacity);
            }
            let inverted = segment.inverted_index(self.key).map_err(err)?;
            let mut stream = inverted
                .terms()
                .range()
                .gt(after.as_bytes())
                .into_stream()
                .map_err(err)?;
            let mut visited = 0;
            while visited < limit && stream.advance() {
                if cancel.is_cancelled() {
                    return Err(E::Cancelled);
                }
                if Instant::now() >= deadline {
                    return Err(E::Capacity);
                }
                let key = String::from_utf8(stream.key().to_vec()).map_err(err)?;
                let mut postings = inverted
                    .read_postings_from_terminfo(stream.value(), IndexRecordOption::Basic)
                    .map_err(err)?;
                while postings.doc() != TERMINATED {
                    let id = postings.doc();
                    if !segment.is_deleted(id) {
                        visited += 1;
                        candidates.insert(key.clone(), DocAddress::new(ordinal as u32, id));
                    }
                    postings.advance();
                }
            }
        }
        let mut result = Vec::new();
        for (key, address) in candidates.into_iter().take(limit) {
            let doc: tantivy::TantivyDocument = self.reader.doc(address).map_err(err)?;
            let text = doc
                .get_first(self.payload)
                .and_then(|v| v.as_str())
                .ok_or(E::StorageCorrupt)?;
            if text.len() > 48 * 1024 * 1024 {
                return Err(E::StorageCorrupt);
            }
            result.push((key, serde_json::from_str(text).map_err(err)?));
        }
        Ok(result)
    }
}
pub fn filters_match(filters: &Filters, c: &Capture) -> bool {
    let r = &c.record;
    if !filters.datasets.is_empty() && !filters.datasets.contains(&r.object.dataset) {
        return false;
    }
    if filters
        .object_id
        .as_ref()
        .is_some_and(|v| v != &r.object.id)
        || filters
            .authority
            .as_ref()
            .is_some_and(|v| r.metadata.get("authority") != Some(v))
        || filters
            .document_type
            .as_ref()
            .is_some_and(|v| r.metadata.get("document_type") != Some(v))
    {
        return false;
    }
    let date = match filters.date_kind {
        Some(DateKind::Publication) => r.publication_date.as_ref(),
        Some(DateKind::Effective) => r.effective_date.as_ref(),
        Some(DateKind::Judgment) => r.metadata.get("judgment_date"),
        None => None,
    };
    if filters
        .date_from
        .as_ref()
        .is_some_and(|min| date.is_none_or(|v| v < min))
        || filters
            .date_to
            .as_ref()
            .is_some_and(|max| date.is_none_or(|v| v > max))
    {
        return false;
    }
    true
}
impl CorpusIndex {
    /// Pre-publication admission for the managed provider pipeline. Successful
    /// publication must not introduce an index event that cannot be represented.
    pub fn validate_record(&self, record: &openlegal_domain::legal::LegalRecord) -> Result<(), E> {
        self.validate_record_with_budget(
            record,
            Instant::now() + std::time::Duration::from_secs(10),
            &CancellationToken::new(),
        )
    }
    pub fn validate_record_with_budget(
        &self,
        record: &openlegal_domain::legal::LegalRecord,
        deadline: Instant,
        cancel: &CancellationToken,
    ) -> Result<(), E> {
        record.validate()?;
        let title = self.tokens_with_budget(&record.title, deadline, cancel)?;
        let body = self.tokens_with_budget(&record.body, deadline, cancel)?;
        let size = serde_json::to_vec(&(record, title, body))
            .map_err(err)?
            .len();
        // Leave room for immutable capture/provenance envelope and index flags.
        if size > 48 * 1024 * 1024 - 8192 {
            return Err(E::Capacity);
        }
        Ok(())
    }
    fn indexed_document(
        &self,
        capture: Capture,
        current: bool,
        deadline: Instant,
        cancel: &CancellationToken,
    ) -> Result<tantivy::TantivyDocument, E> {
        capture.record.validate()?;
        let indexed = IndexedCapture {
            title_tokens: self.tokens_with_budget(&capture.record.title, deadline, cancel)?,
            body_tokens: self.tokens_with_budget(&capture.record.body, deadline, cancel)?,
            capture,
            current,
        };
        let value = serde_json::to_string(&indexed).map_err(err)?;
        if value.len() > 48 * 1024 * 1024 {
            return Err(E::Capacity);
        }
        Ok(
            tantivy::doc!(self.key=>record_key(&indexed.capture),self.object=>object_key(&indexed.capture),self.payload=>value),
        )
    }
    fn commit_changes(
        &self,
        keys: Vec<String>,
        documents: Vec<tantivy::TantivyDocument>,
        generation: u64,
    ) -> Result<(), E> {
        let mut writer = self.writer.lock().map_err(err)?;
        for key in keys {
            writer.delete_term(Term::from_field_text(self.key, &key));
        }
        for doc in documents {
            writer.add_document(doc).map_err(err)?;
        }
        let mut commit = writer.prepare_commit().map_err(err)?;
        commit.set_payload(&self.metadata(generation, self.complete.load(Ordering::Acquire))?);
        commit.commit().map_err(err)?;
        self.reader.reload().map_err(err)?;
        Ok(())
    }
    /// Replace only affected captures. At most the old HEAD and new capture are buffered.
    pub fn apply_capture(
        &self,
        capture: Capture,
        install_head: bool,
        generation: u64,
    ) -> Result<(), E> {
        self.apply_capture_with_budget(
            capture,
            install_head,
            generation,
            Instant::now() + std::time::Duration::from_secs(10),
            &CancellationToken::new(),
        )
    }
    pub fn apply_capture_with_budget(
        &self,
        capture: Capture,
        install_head: bool,
        generation: u64,
        deadline: Instant,
        cancel: &CancellationToken,
    ) -> Result<(), E> {
        let _mutation = self.mutation.lock().map_err(err)?;
        let snapshot = self.snapshot()?;
        let prefix = format!("{}/", object_key(&capture));
        let mut after = prefix.clone();
        let mut keys = Vec::new();
        let mut documents = Vec::new();
        let mut current = install_head;
        let mut seen = 0;
        while let Some((key, old)) = snapshot
            .batch(&after, 1, deadline, cancel)?
            .into_iter()
            .next()
        {
            if !key.starts_with(&prefix) {
                break;
            }
            after = key.clone();
            seen += 1;
            if seen > 1000 {
                return Err(E::Capacity);
            }
            if old.capture.capture_id == capture.capture_id {
                current |= old.current;
                keys.push(key);
            } else if old.capture.record.revision_id == capture.record.revision_id
                && (!old.current || install_head)
            {
                keys.push(key);
            } else if old.current && install_head {
                keys.push(key);
                if !documents.is_empty() {
                    return Err(E::StorageCorrupt);
                }
                documents.push(self.indexed_document(old.capture, false, deadline, cancel)?);
            }
        }
        documents.push(self.indexed_document(capture, current, deadline, cancel)?);
        if cancel.is_cancelled() {
            return Err(E::Cancelled);
        }
        if Instant::now() >= deadline {
            return Err(E::Capacity);
        }
        self.commit_changes(keys, documents, generation)
    }
    pub fn remove_capture(
        &self,
        object: &openlegal_domain::legal::ObjectId,
        capture_id: &str,
        generation: u64,
    ) -> Result<(), E> {
        let _mutation = self.mutation.lock().map_err(err)?;
        let snapshot = self.snapshot()?;
        let prefix = format!(
            "{}/{}/{:?}/{}/",
            object.jurisdiction, object.provider, object.dataset, object.id
        );
        let mut after = prefix.clone();
        let mut keys = Vec::new();
        let deadline = Instant::now() + std::time::Duration::from_secs(10);
        let mut seen = 0;
        loop {
            let Some((key, old)) = snapshot
                .batch(&after, 1, deadline, &CancellationToken::new())?
                .into_iter()
                .next()
            else {
                break;
            };
            if !key.starts_with(&prefix) {
                break;
            }
            after = key.clone();
            seen += 1;
            if seen > 1000 {
                return Err(E::Capacity);
            }
            if old.capture.capture_id == capture_id {
                keys.push(key);
            }
        }
        self.commit_changes(keys, Vec::new(), generation)
    }
    pub fn advance_generation(&self, generation: u64) -> Result<(), E> {
        let _mutation = self.mutation.lock().map_err(err)?;
        self.commit_changes(Vec::new(), Vec::new(), generation)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use openlegal_domain::legal::{Dataset, LegalRecord, ObjectId};
    #[test]
    fn metadata_fences_legacy_mismatched_and_interrupted_indexes() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("index");
        let analyzer = KoreanAnalyzer::fixture();
        let index = CorpusIndex::create_rebuild(&path, analyzer.clone()).unwrap();
        index
            .apply_capture(capture("a", "r1", "대한민국"), true, 4)
            .unwrap();
        drop(index);
        assert!(CorpusIndex::open(&path, analyzer.clone()).is_err());
        assert!(CorpusIndex::create_rebuild(&path, analyzer.clone()).is_err());

        let ready = directory.path().join("ready");
        let index = CorpusIndex::create_rebuild(&ready, analyzer.clone()).unwrap();
        index
            .apply_capture(capture("a", "r1", "대한민국"), true, 4)
            .unwrap();
        index.finish_rebuild().unwrap();
        drop(index);
        let index = CorpusIndex::open(&ready, analyzer.clone()).unwrap();
        assert_eq!(index.snapshot().unwrap().generation, 4);
        assert_eq!(
            index.snapshot().unwrap().analyzer_version,
            analyzer.identity()
        );
        drop(index);
        for payload in [
            "4".to_string(),
            "broken".to_string(),
            serde_json::to_string(&IndexMetadata {
                format: INDEX_FORMAT,
                analyzer: "different-dictionary".into(),
                generation: 4,
                complete: true,
            })
            .unwrap(),
        ] {
            let raw = Index::open_in_dir(&ready).unwrap();
            let mut writer = raw
                .writer_with_num_threads::<tantivy::TantivyDocument>(1, 32 * 1024 * 1024)
                .unwrap();
            let mut commit = writer.prepare_commit().unwrap();
            commit.set_payload(&payload);
            commit.commit().unwrap();
            drop(writer);
            assert!(CorpusIndex::open(&ready, analyzer.clone()).is_err());
        }
    }
    pub(super) fn capture(id: &str, revision: &str, text: &str) -> Capture {
        Capture {
            capture_id: if revision == "r1" {
                "a".repeat(64)
            } else {
                "b".repeat(64)
            },
            sequence: 1,
            record: LegalRecord {
                object: ObjectId {
                    jurisdiction: "kr".into(),
                    provider: "fixture".into(),
                    dataset: Dataset::NationalStatute,
                    id: id.into(),
                },
                revision_id: revision.into(),
                title: "Fictional statute".into(),
                body: text.into(),
                metadata: BTreeMap::new(),
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
    #[test]
    fn exact_negation_and_korean_analysis_use_separate_semantics() {
        let dir = tempfile::tempdir().unwrap();
        let index = CorpusIndex::open(dir.path(), KoreanAnalyzer::fixture()).unwrap();
        let c = capture("a", "r1", "대한민국 ABC café");
        let doc = IndexedCapture {
            title_tokens: index.tokens(&c.record.title).unwrap(),
            body_tokens: index.tokens(&c.record.body).unwrap(),
            capture: c,
            current: true,
        };
        let parser =
            openlegal_normalization::search_query::SearchQueryProcessor::new(&["title", "body"])
                .unwrap();
        for (query, expected) in [
            ("abc", true),
            ("\"ABC\"", true),
            ("\"abc\"", false),
            ("NOT \"abc\"", true),
            ("NOT \"ABC\"", false),
            ("cafe\u{301}", true),
        ] {
            assert_eq!(
                index.matches(
                    &index
                        .compile_query(
                            &parser.parse(query).unwrap().expression,
                            Instant::now() + std::time::Duration::from_secs(10),
                            &CancellationToken::new()
                        )
                        .unwrap(),
                    &doc
                ),
                expected,
                "{query}"
            );
        }
        assert!(!index.tokens("대한민국 법률").unwrap().lindera.is_empty());
    }
    #[test]
    fn old_generation_survives_replacement_and_deleted_terms_do_not_end_scan() {
        let dir = tempfile::tempdir().unwrap();
        let index = CorpusIndex::open(dir.path(), KoreanAnalyzer::fixture()).unwrap();
        let a = capture("a", "r1", "old");
        index.apply_capture(a.clone(), true, 1).unwrap();
        let old = index.snapshot().unwrap();
        let b = capture("b", "r1", "second");
        index.apply_capture(b, true, 2).unwrap();
        index.remove_object(&a.record.object, 3).unwrap();
        let current = index.snapshot().unwrap();
        assert_eq!(current.generation, 3);
        let rows = current
            .batch(
                "",
                1,
                Instant::now() + std::time::Duration::from_secs(10),
                &CancellationToken::new(),
            )
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].1.capture.record.object.id, "b");
        assert_eq!(
            old.batch(
                "",
                1,
                Instant::now() + std::time::Duration::from_secs(10),
                &CancellationToken::new()
            )
            .unwrap()[0]
                .1
                .capture
                .record
                .body,
            "old"
        );
    }
}
