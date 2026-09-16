//! Rebuildable Tantivy corpus generations and Korean analysis. No provider I/O.
use lindera::{
    dictionary::{DictionaryKind, load_embedded_dictionary},
    mode::Mode,
    segmenter::Segmenter,
};
use openlegal_domain::{
    legal::{Capture, DatabaseError as E},
    legal_search::{DateKind, Filters},
    search_query::{Expr, ExprKind},
};
use serde::{Deserialize, Serialize};
use std::{
    borrow::Cow,
    collections::{BTreeMap, HashSet},
    path::Path,
    sync::{Arc, Mutex},
    time::Instant,
};
use tantivy::{
    DocAddress, DocSet, Index, IndexReader, IndexWriter, ReloadPolicy, Searcher, TERMINATED, Term,
    schema::{Field, IndexRecordOption, STORED, STRING, Schema, Value},
};
use tokio_util::sync::CancellationToken;
use unicode_normalization::UnicodeNormalization;
pub const ANALYZER_VERSION: &str = "ko_lindera5_surface_nfc_ascii_v1";
#[derive(Clone, Serialize, Deserialize)]
pub struct IndexedCapture {
    pub capture: Capture,
    pub current: bool,
    pub title_tokens: Vec<String>,
    pub body_tokens: Vec<String>,
}
#[derive(Clone)]
pub struct IndexSnapshot {
    pub reader: Searcher,
    pub generation: u64,
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
    analyzer: Segmenter,
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
    pub fn open(path: &Path) -> Result<Arc<Self>, E> {
        let mut schema = Schema::builder();
        let key = schema.add_text_field("key", STRING | STORED);
        let object = schema.add_text_field("object", STRING);
        let payload = schema.add_text_field("payload", STORED);
        let schema = schema.build();
        std::fs::create_dir_all(path).map_err(err)?;
        let index = Index::open_or_create(
            tantivy::directory::MmapDirectory::open(path).map_err(err)?,
            schema,
        )
        .map_err(err)?;
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()
            .map_err(err)?;
        let writer = index
            .writer_with_num_threads(1, 32 * 1024 * 1024)
            .map_err(err)?;
        let dictionary = load_embedded_dictionary(DictionaryKind::KoDic).map_err(err)?;
        Ok(Arc::new(Self {
            index,
            reader,
            writer: Mutex::new(writer),
            key,
            object,
            payload,
            analyzer: Segmenter::new(Mode::Normal, dictionary, None),
            mutation: Mutex::new(()),
        }))
    }
    pub fn tokens(&self, text: &str) -> Result<Vec<String>, E> {
        let normalized: String = text.nfc().collect::<String>().to_ascii_lowercase();
        let mut result = Vec::new();
        // Paragraph-size calls prevent one legal document from allocating an unbounded lattice.
        for line in normalized.split_inclusive('\n') {
            if line.len() > 65536 {
                return Err(E::Capacity);
            }
            for token in self.analyzer.segment(Cow::Borrowed(line)).map_err(err)? {
                if !token.surface.trim().is_empty() {
                    result.push(token.surface.into_owned());
                }
                if result.len() > 262144 {
                    return Err(E::Capacity);
                }
            }
        }
        Ok(result)
    }
    pub fn snapshot(&self) -> Result<IndexSnapshot, E> {
        let _writer = self.writer.lock().map_err(err)?;
        let meta = self.index.load_metas().map_err(err)?;
        let generation = meta
            .payload
            .as_deref()
            .unwrap_or("0")
            .parse()
            .map_err(err)?;
        Ok(IndexSnapshot {
            reader: self.reader.searcher(),
            generation,
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
        commit.set_payload(&generation.to_string());
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
        commit.set_payload(&generation.to_string());
        commit.commit().map_err(err)?;
        self.reader.reload().map_err(err)?;
        Ok(())
    }
    pub fn matches(
        &self,
        expression: &Expr,
        doc: &IndexedCapture,
        fields: Option<&str>,
    ) -> Result<bool, E> {
        let texts: Vec<(&str, &[String])> = match fields {
            Some("title") => vec![(&doc.capture.record.title, &doc.title_tokens)],
            Some("body") => vec![(&doc.capture.record.body, &doc.body_tokens)],
            Some(_) => return Err(E::InvalidInput),
            None => vec![
                (&doc.capture.record.title, &doc.title_tokens),
                (&doc.capture.record.body, &doc.body_tokens),
            ],
        };
        Ok(match &expression.kind {
            ExprKind::MatchAll => true,
            ExprKind::Term(term) | ExprKind::Prefix(term) => {
                let terms = self.tokens(term)?;
                if terms.is_empty() {
                    return Err(E::InvalidInput);
                }
                texts.iter().any(|(_, tokens)| {
                    let set: HashSet<&str> = tokens.iter().map(String::as_str).collect();
                    terms.iter().enumerate().all(|(i, t)| {
                        if matches!(expression.kind, ExprKind::Prefix(_)) && i + 1 == terms.len() {
                            set.iter().any(|v| v.starts_with(t))
                        } else {
                            set.contains(t.as_str())
                        }
                    })
                })
            }
            ExprKind::Exact(text) => texts.iter().any(|(value, _)| value.contains(text)),
            ExprKind::Not(child) => !self.matches(child, doc, fields)?,
            ExprKind::And(children) => {
                let mut found = true;
                for child in children {
                    if !self.matches(child, doc, fields)? {
                        found = false;
                        break;
                    }
                }
                found
            }
            ExprKind::Or(children) => {
                let mut found = false;
                for child in children {
                    if self.matches(child, doc, fields)? {
                        found = true;
                        break;
                    }
                }
                found
            }
            ExprKind::Field { name, expression } => self.matches(expression, doc, Some(name))?,
            ExprKind::Group { expression, .. } => self.matches(expression, doc, fields)?,
        })
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
        record.validate()?;
        let title = self.tokens(&record.title)?;
        let body = self.tokens(&record.body)?;
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
    ) -> Result<tantivy::TantivyDocument, E> {
        capture.record.validate()?;
        let indexed = IndexedCapture {
            title_tokens: self.tokens(&capture.record.title)?,
            body_tokens: self.tokens(&capture.record.body)?,
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
        commit.set_payload(&generation.to_string());
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
        let _mutation = self.mutation.lock().map_err(err)?;
        let snapshot = self.snapshot()?;
        let prefix = format!("{}/", object_key(&capture));
        let mut after = prefix.clone();
        let mut keys = Vec::new();
        let mut documents = Vec::new();
        let mut current = install_head;
        let mut seen = 0;
        let deadline = Instant::now() + std::time::Duration::from_secs(10);
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
                documents.push(self.indexed_document(old.capture, false)?);
            }
        }
        documents.push(self.indexed_document(capture, current)?);
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
        let index = CorpusIndex::open(dir.path()).unwrap();
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
                index
                    .matches(&parser.parse(query).unwrap().expression, &doc, None)
                    .unwrap(),
                expected,
                "{query}"
            );
        }
        assert!(!index.tokens("대한민국 법률").unwrap().is_empty());
    }
    #[test]
    fn old_generation_survives_replacement_and_deleted_terms_do_not_end_scan() {
        let dir = tempfile::tempdir().unwrap();
        let index = CorpusIndex::open(dir.path()).unwrap();
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
