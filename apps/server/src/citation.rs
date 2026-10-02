//! Citation compatibility tools and exact retained-source resource references.

use crate::{
    ServerError,
    registry::{InvocationOutput, RichToolOutput, ToolModule, ToolOptions, ToolRegistry},
};
use openlegal_application::citation::{CitationService, passage_ids, projection_title};
use openlegal_domain::{
    citation::{
        CitationDescriptor, CitationDocument, CitationId, CitationProjection, CitationSource,
        MAX_CITATION_TEXT_BYTES,
    },
    legal::{DatabaseError, MetadataResult, ObjectId},
};
use rmcp::{
    ErrorData,
    model::{
        CacheScope, ContentBlock, MetaObject, ReadResourceResult, Resource, ResourceContents,
        ResourceTemplate,
    },
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{collections::BTreeSet, sync::Arc};

pub const RESOURCE_PREFIX: &str = "openlegal://source/";
const MAX_REFERENCES: usize = 20;
const MAX_REFERENCE_NODES: usize = 2048;

pub struct CitationTools {
    pub service: Arc<CitationService>,
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct SearchInput {
    query: String,
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct FetchInput {
    id: String,
}

#[derive(Serialize, JsonSchema)]
struct SearchOutput {
    results: Vec<openlegal_domain::citation::CitationSearchResult>,
}

impl ToolModule for CitationTools {
    fn register(self, registry: &mut ToolRegistry) -> Result<(), ServerError> {
        let service = self.service.clone();
        registry.register_rich::<SearchInput, SearchOutput, _, _>(
            "search",
            "Search retained legal source items using the corpus query syntax. Returns capture-fixed source IDs and browser-openable citation URLs; fetch retrieves the complete bounded item. Partial coverage is qualified in additional text.",
            ToolOptions::default(),
            move |input, context| {
                let service = service.clone();
                async move {
                    let search = service.search(input.query, context.deadline.into_std(), context.request.cancellation)
                        .await.map_err(crate::database::map_error)?;
                    let mut output = RichToolOutput::new(SearchOutput { results: search.results });
                    if search.partial {
                        output.additional_content.push(ContentBlock::text("Search results are partial. They do not establish complete corpus coverage or absence of other matching sources."));
                    }
                    for warning in search.warnings {
                        output.additional_content.push(ContentBlock::text(format!("Search qualification: {warning}")));
                    }
                    // Structured JSON is emitted as the first text block by the shared handler.
                    Ok(output)
                }
            },
        )?;
        let service = self.service;
        registry.register_rich::<FetchInput, CitationDocument, _, _>(
            "fetch",
            "Retrieve the complete text of one capture-fixed source item returned by search. IDs identify a document, source section or bounded passage; historical evidence never falls back to HEAD. Metadata-only IDs describe provenance rather than legal body evidence.",
            ToolOptions::default(),
            move |input, context| {
                let service = service.clone();
                async move {
                    service.fetch(&input.id, context.request.cancellation).await
                        .map(RichToolOutput::new).map_err(crate::database::map_error)
                }
            },
        )?;
        Ok(())
    }
}

pub fn id_from_resource_uri(uri: &str) -> Result<&str, ErrorData> {
    let id = uri
        .strip_prefix(RESOURCE_PREFIX)
        .ok_or_else(|| ErrorData::resource_not_found("Resource was not found.", None))?;
    CitationId::decode(id)
        .map_err(|_| ErrorData::invalid_params("Invalid source resource URI.", None))?;
    Ok(id)
}

pub fn resource_template() -> ResourceTemplate {
    ResourceTemplate::new("openlegal://source/{+id}", "retained-legal-source")
        .with_title("Retained legal source")
        .with_description("An exact retained document, section, passage or provenance descriptor. Source IDs are returned by legal tools; arbitrary URLs and filesystem paths are unsupported.")
        .with_mime_type("text/plain")
}

pub(crate) fn resource_error(error: DatabaseError) -> ErrorData {
    match error {
        DatabaseError::InvalidInput => {
            ErrorData::invalid_params("Invalid source resource URI.", None)
        }
        DatabaseError::NotFound
        | DatabaseError::NotObserved
        | DatabaseError::RevisionUnavailable
        | DatabaseError::Withdrawn
        | DatabaseError::SnapshotInvalidated => {
            ErrorData::resource_not_found("The exact retained source is unavailable.", None)
        }
        _ => ErrorData::internal_error("The source resource is unavailable.", None),
    }
}

fn metadata_text(metadata: &MetadataResult) -> Result<String, ErrorData> {
    serde_json::to_string(&json!({
        "object": metadata.object,
        "capture_id": metadata.capture_id,
        "revision_id": metadata.revision_id,
        "title": metadata.title,
        "publication_date": metadata.publication_date,
        "effective_date": metadata.effective_date,
        "retrieved_at": metadata.retrieved_at,
        "captured_at": metadata.captured_at,
        "validated_at": metadata.validated_at,
        "processor_version": metadata.processor_version,
        "raw_sha256": metadata.raw_sha256,
        "source_url": metadata.source_url,
        "evidence": "metadata_only"
    }))
    .map_err(|_| ErrorData::internal_error("Source metadata could not be encoded.", None))
}

pub(crate) fn resource_result(
    uri: &str,
    source: CitationSource,
) -> Result<ReadResourceResult, ErrorData> {
    let id = CitationId::decode(id_from_resource_uri(uri)?).map_err(resource_error)?;
    let provenance = source
        .document
        .as_ref()
        .map(|document| document.metadata.clone());
    let text = if matches!(id.projection, CitationProjection::Metadata) {
        metadata_text(&source.metadata)?
    } else {
        source
            .document
            .ok_or_else(|| {
                ErrorData::resource_not_found("The exact retained body is unavailable.", None)
            })?
            .text
    };
    let meta = MetaObject(
        json!({
            "openlegal/source": source.descriptor,
            "openlegal/provenance": provenance,
            "openlegal/bodyUnavailable": source.unavailable,
            "openlegal/previous": source.previous,
            "openlegal/next": source.next,
        })
        .as_object()
        .ok_or_else(|| ErrorData::internal_error("Source metadata could not be encoded.", None))?
        .clone(),
    );
    Ok(
        ReadResourceResult::new(vec![ResourceContents::text(text, uri).with_meta(meta)])
            .with_ttl_ms(0)
            .with_cache_scope(CacheScope::Public),
    )
}

fn link(descriptor: &CitationDescriptor) -> ContentBlock {
    let meta = MetaObject(
        json!({"openlegal/source": descriptor})
            .as_object()
            .expect("object literal")
            .clone(),
    );
    ContentBlock::resource_link(
        Resource::new(&descriptor.uri, &descriptor.id)
            .with_title(&descriptor.title)
            .with_mime_type("text/plain")
            .with_meta(meta),
    )
}

pub(crate) fn is_native_citation_tool(name: &str) -> bool {
    matches!(
        name,
        "database.get"
            | "database.get_metadata"
            | "database.query"
            | "database.rg"
            | "database.history"
            | "database.diff"
            | "database.object_status"
            | "database.show"
            | "law.resolve_name"
            | "law.in_force_at"
            | "citation.verify"
            | "law.watch"
            | "law.lineage"
            | "precedent.citing"
            | "article.impact"
            | "law.article"
    )
}

struct Candidate {
    id: CitationId,
    title: String,
    official_url: Option<String>,
    body_available: Option<bool>,
}

struct ReferenceScan {
    candidates: Vec<Candidate>,
    warnings: BTreeSet<&'static str>,
    visited: usize,
}

impl ReferenceScan {
    fn add(
        &mut self,
        id: CitationId,
        title: &str,
        official_url: Option<String>,
        body_available: Option<bool>,
    ) {
        if self.candidates.iter().any(|candidate| candidate.id == id) {
            return;
        }
        if self.candidates.len() >= MAX_REFERENCES {
            self.warnings.insert("source_references_truncated");
            return;
        }
        if matches!(id.projection, CitationProjection::Metadata) {
            self.warnings
                .insert("some_source_references_are_metadata_only");
        }
        let title = projection_title(title, &id.projection);
        self.candidates.push(Candidate {
            id,
            title,
            official_url,
            body_available,
        });
    }

    fn scan(&mut self, value: &Value, inherited: Option<&ObjectId>, depth: usize) {
        self.visited += 1;
        if self.visited > MAX_REFERENCE_NODES || depth > 16 {
            self.warnings.insert("source_references_truncated");
            return;
        }
        match value {
            Value::Array(values) => {
                for value in values {
                    self.scan(value, inherited, depth + 1);
                }
            }
            Value::Object(fields) => {
                let object = fields
                    .get("object")
                    .and_then(|v| serde_json::from_value::<ObjectId>(v.clone()).ok());
                let metadata = fields
                    .get("metadata")
                    .and_then(|v| serde_json::from_value::<MetadataResult>(v.clone()).ok())
                    .or_else(|| {
                        fields
                            .contains_key("raw_sha256")
                            .then(|| serde_json::from_value::<MetadataResult>(value.clone()).ok())
                            .flatten()
                    });
                let object = object
                    .as_ref()
                    .or_else(|| metadata.as_ref().map(|m| &m.object))
                    .or(inherited);
                let capture = fields
                    .get("capture_id")
                    .and_then(Value::as_str)
                    .or_else(|| metadata.as_ref().map(|m| m.capture_id.as_str()));
                if let (Some(object), Some(capture)) = (object, capture) {
                    let title = fields
                        .get("title")
                        .or_else(|| fields.get("law_title"))
                        .or_else(|| fields.get("current_title"))
                        .and_then(Value::as_str)
                        .or_else(|| metadata.as_ref().map(|m| m.title.as_str()))
                        .unwrap_or(&object.id);
                    let official_url = metadata
                        .as_ref()
                        .and_then(openlegal_domain::citation::official_browser_url);
                    if let (Some(section), Some(text), Some(offset)) = (
                        fields.get("section").and_then(Value::as_str),
                        fields.get("text").and_then(Value::as_str),
                        fields
                            .get("offset")
                            .and_then(Value::as_u64)
                            .and_then(|v| usize::try_from(v).ok()),
                    ) {
                        match passage_ids(object, capture, section, text, offset) {
                            Ok(ids) => {
                                for id in ids {
                                    self.add(id, title, official_url.clone(), Some(true));
                                }
                            }
                            Err(_) => {
                                self.warnings.insert("source_reference_unavailable");
                            }
                        }
                        return;
                    }
                    let bounds = fields
                        .get("byte_start")
                        .and_then(Value::as_u64)
                        .zip(fields.get("byte_end").and_then(Value::as_u64));
                    let section = fields
                        .get("excerpt_section")
                        .or_else(|| fields.get("section"))
                        .and_then(Value::as_str);
                    let projection = match (section, bounds) {
                        (Some(section), Some((start, end)))
                            if end > start && end - start <= MAX_CITATION_TEXT_BYTES as u64 =>
                        {
                            match (usize::try_from(start), usize::try_from(end)) {
                                (Ok(start), Ok(end)) => CitationProjection::Passage {
                                    section: section.into(),
                                    start,
                                    end,
                                },
                                _ => CitationProjection::Metadata,
                            }
                        }
                        _ => CitationProjection::Metadata,
                    };
                    self.add(
                        CitationId {
                            object: object.clone(),
                            capture_id: capture.into(),
                            projection,
                        },
                        title,
                        official_url,
                        None,
                    );
                } else if (fields.contains_key("object") && object.is_some())
                    || (fields
                        .iter()
                        .any(|(key, value)| key.ends_with("revision_id") && value.is_string())
                        && (fields.contains_key("capture_id") || object.is_some()))
                {
                    self.warnings.insert("source_reference_unavailable");
                }
                for (name, value) in fields {
                    if matches!(name.as_str(), "object" | "metadata") {
                        continue;
                    }
                    self.scan(value, object, depth + 1);
                }
            }
            _ => {}
        }
    }
}

pub(crate) fn append_native_references(
    name: &str,
    service: &CitationService,
    input_object: Option<&Value>,
    output: &mut InvocationOutput,
) {
    if !is_native_citation_tool(name) {
        return;
    }
    let input_object =
        input_object.and_then(|v| serde_json::from_value::<ObjectId>(v.clone()).ok());
    let mut scan = ReferenceScan {
        candidates: Vec::new(),
        warnings: BTreeSet::new(),
        visited: 0,
    };
    // Status already supplies the observed HEAD capture; treat it as exact
    // provenance without resolving a newer HEAD or changing the native DTO.
    let status = if name == "database.object_status" {
        let mut status = output.structured.clone();
        if let Some(fields) = status.as_object_mut()
            && let Some(capture) = fields.get("head_capture_id").cloned()
        {
            fields.insert("capture_id".into(), capture);
        }
        Some(status)
    } else {
        None
    };
    scan.scan(
        status.as_ref().unwrap_or(&output.structured),
        input_object.as_ref(),
        0,
    );
    let mut references = Vec::new();
    for candidate in scan.candidates {
        match service.descriptor(
            &candidate.id,
            &candidate.title,
            candidate.body_available,
            candidate.official_url,
        ) {
            Ok(descriptor) => {
                output.additional_content.push(link(&descriptor));
                references.push(descriptor);
            }
            Err(_) => {
                scan.warnings.insert("source_reference_unavailable");
            }
        }
    }
    if let Some(structured) = output.structured.as_object_mut() {
        structured.insert(
            "references".into(),
            serde_json::to_value(references).unwrap_or_else(|_| json!([])),
        );
    }
    for warning in scan.warnings {
        output.additional_content.push(ContentBlock::text(format!(
            "Source reference qualification: {warning}."
        )));
    }
    output.strict_result_limit = true;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::{Limits, RateLimitConfig, SourceOffer},
        handler::McpHandler,
        test_corpus::{NOW, Record, TestCorpus, article, object, section},
    };
    use futures::future::BoxFuture;
    use openlegal_application::{
        Clock,
        citation::CitationLease,
        database::{DatabaseService, DatabaseStore},
        search::{SearchBackend, SearchBudget, SearchMode, SearchService},
    };
    use openlegal_domain::legal::{
        Capture, Dataset, GetResult, HistoryKind, HistoryPage, RevisionSelector, SectionKind,
    };
    use openlegal_domain::legal_search::{SearchHit, SearchPage, SearchRequest};
    use tokio_util::sync::CancellationToken;

    struct Fixed;
    impl Clock for Fixed {
        fn now(&self) -> u64 {
            NOW
        }
    }
    struct Leases;
    impl CitationLease for Leases {
        fn renew(
            &self,
            _: Vec<(ObjectId, String)>,
            _: u64,
            _: CancellationToken,
        ) -> BoxFuture<'static, Result<(), DatabaseError>> {
            Box::pin(async { Ok(()) })
        }
    }
    struct Citable {
        hit: SearchHit,
    }
    impl SearchBackend for Citable {
        fn search(
            &self,
            _: SearchMode,
            _: SearchRequest,
            _: SearchBudget,
            _: CancellationToken,
        ) -> BoxFuture<'static, Result<SearchPage, DatabaseError>> {
            Box::pin(async { Err(DatabaseError::StorageUnavailable) })
        }
        fn search_citable(
            &self,
            _: SearchRequest,
            _: SearchBudget,
            _: CancellationToken,
        ) -> BoxFuture<'static, Result<SearchPage, DatabaseError>> {
            let hit = self.hit.clone();
            Box::pin(async move {
                Ok(SearchPage {
                    schema_version: 1,
                    hits: vec![hit],
                    next_cursor: Some("synthetic-continuation".into()),
                    generation: 1,
                    corpus_complete: false,
                    scanned_bytes: 32,
                    analyzer_version: "fictional".into(),
                    index_lag: 1,
                    collection_notices: Vec::new(),
                })
            })
        }
    }
    struct Retained {
        corpus: Arc<TestCorpus>,
        expired: MetadataResult,
    }
    impl DatabaseStore for Retained {
        fn resolve(
            &self,
            object: ObjectId,
            selector: RevisionSelector,
            now: u64,
            cancel: CancellationToken,
        ) -> BoxFuture<'static, Result<Capture, DatabaseError>> {
            if object == self.expired.object
                && selector
                    == (RevisionSelector::Capture {
                        id: self.expired.capture_id.clone(),
                    })
            {
                Box::pin(async { Err(DatabaseError::RevisionUnavailable) })
            } else {
                self.corpus.resolve(object, selector, now, cancel)
            }
        }
        fn resolve_metadata(
            &self,
            object: ObjectId,
            selector: RevisionSelector,
            now: u64,
            cancel: CancellationToken,
        ) -> BoxFuture<'static, Result<MetadataResult, DatabaseError>> {
            if object == self.expired.object
                && selector
                    == (RevisionSelector::Capture {
                        id: self.expired.capture_id.clone(),
                    })
            {
                let metadata = self.expired.clone();
                Box::pin(async move { Ok(metadata) })
            } else {
                self.corpus.resolve_metadata(object, selector, now, cancel)
            }
        }
        fn history(
            &self,
            object: ObjectId,
            kind: HistoryKind,
            cursor: Option<String>,
            limit: usize,
            now: u64,
            cancel: CancellationToken,
        ) -> BoxFuture<'static, Result<HistoryPage, DatabaseError>> {
            self.corpus
                .history(object, kind, cursor, limit, now, cancel)
        }
    }

    fn fixture() -> (Arc<CitationService>, MetadataResult, CitationId) {
        let mut ocr = section(
            "첨부/50% A",
            "Fictional OCR attachment",
            "보충 <script>alert('fictional')</script> & evidence",
            SectionKind::Ocr,
        );
        ocr.page = Some(3);
        ocr.source_document_sha256 = Some("d".repeat(64));
        let capture = Record {
            object: object(Dataset::NationalStatute, "fictional"),
            n: 1,
            revision: "r1",
            title: "Fictional statute",
            effective: None,
            sections: vec![article("1", "Fictional", "가나다 citation evidence"), ocr],
            metadata: &[],
            head: true,
        }
        .build();
        let id = CitationId {
            object: capture.capture.record.object.clone(),
            capture_id: capture.capture.capture_id.clone(),
            projection: CitationProjection::Document,
        };
        let metadata: MetadataResult = GetResult {
            capture: capture.capture.clone(),
            freshness: None,
        }
        .into();
        let search = Arc::new(Citable {
            hit: SearchHit {
                match_scope: "object".into(),
                excerpt_section: "body".into(),
                includes_ocr: false,
                object: id.object.clone(),
                revision_id: "r1".into(),
                capture_id: id.capture_id.clone(),
                title: "Fictional statute".into(),
                section: "body".into(),
                line: 1,
                text: capture.capture.record.body.clone(),
                byte_start: 0,
                byte_end: capture.capture.record.body.len(),
                derived_ocr: false,
            },
        });
        let replacement = Record {
            object: object(Dataset::NationalStatute, "expired"),
            n: 2,
            revision: "replacement",
            title: "Replacement HEAD",
            effective: None,
            sections: vec![article(
                "1",
                "Replacement",
                "Replacement HEAD must not substitute expired evidence",
            )],
            metadata: &[],
            head: true,
        }
        .build();
        let mut expired = metadata.clone();
        expired.object = replacement.capture.record.object.clone();
        expired.capture_id = "f".repeat(64);
        expired.title = "Expired fictional evidence".into();
        let corpus = Arc::new(TestCorpus(vec![capture, replacement]));
        let store = Arc::new(Retained { corpus, expired });
        let clock = Arc::new(Fixed);
        let service = Arc::new(
            CitationService::new(
                Arc::new(DatabaseService::new(store, clock.clone())),
                Arc::new(SearchService::new(search)),
                Arc::new(Leases),
                clock,
                "https://references.example.test".into(),
            )
            .unwrap(),
        );
        (service, metadata, id)
    }

    #[test]
    fn native_page_references_preserve_utf8_capture_and_byte_offsets() {
        let (_, metadata, _) = fixture();
        let text = "가".repeat(10_000);
        let mut scan = ReferenceScan {
            candidates: Vec::new(),
            warnings: BTreeSet::new(),
            visited: 0,
        };
        scan.scan(
            &json!({"metadata":metadata,"section":"body","text":text,"offset":9}),
            None,
            0,
        );
        assert_eq!(scan.candidates.len(), 4);
        let mut expected_start = 9;
        for candidate in scan.candidates {
            assert_eq!(candidate.id.capture_id, metadata.capture_id);
            let CitationProjection::Passage {
                section,
                start,
                end,
            } = candidate.id.projection
            else {
                panic!("text page must emit passage references")
            };
            assert_eq!(section, "body");
            assert_eq!(start, expected_start);
            assert!(end - start <= MAX_CITATION_TEXT_BYTES);
            assert!(text.is_char_boundary(start - 9));
            assert!(text.is_char_boundary(end - 9));
            expected_start = end;
        }
        assert_eq!(expected_start, 9 + text.len());
    }

    #[test]
    fn catalog_only_history_is_not_associated_with_a_plausible_capture() {
        let (_, metadata, _) = fixture();
        let mut scan = ReferenceScan {
            candidates: Vec::new(),
            warnings: BTreeSet::new(),
            visited: 0,
        };
        scan.scan(&json!({"entries":[{"revision_id":"missing","capture_id":null},{"revision_id":"r1","capture_id":metadata.capture_id}]}), Some(&metadata.object), 0);
        assert_eq!(scan.candidates.len(), 1);
        assert!(matches!(
            scan.candidates[0].id.projection,
            CitationProjection::Metadata
        ));
        assert!(scan.warnings.contains("source_reference_unavailable"));
    }

    #[test]
    fn exact_search_references_deduplicate_and_bound_fanout() {
        let (_, metadata, _) = fixture();
        let hits: Vec<_> = (0..25).map(|n| json!({"object":metadata.object,"capture_id":metadata.capture_id,"title":"Fictional","excerpt_section":"body","byte_start":n,"byte_end":n+1})).collect();
        let mut scan = ReferenceScan {
            candidates: Vec::new(),
            warnings: BTreeSet::new(),
            visited: 0,
        };
        scan.scan(&json!({"hits":hits}), None, 0);
        assert_eq!(scan.candidates.len(), MAX_REFERENCES);
        assert!(scan.warnings.contains("source_references_truncated"));
    }

    #[test]
    fn analysis_nested_reference_collections_preserve_exact_evidence() {
        use openlegal_domain::legal_analysis::{ImpactBucket, ImpactReference, ImpactResult};
        let (_, metadata, _) = fixture();
        let result = ImpactResult {
            schema_version: 1,
            object: metadata.object.clone(),
            resolution: None,
            law_title: metadata.title.clone(),
            article: "1".into(),
            article_title: None,
            inbound: vec![ImpactBucket {
                dataset: Dataset::Precedent,
                object_count: 1,
                references: vec![ImpactReference {
                    object: object(Dataset::Precedent, "citing-decision"),
                    title: "Fictional citing decision".into(),
                    line: "Fictional evidence".into(),
                    revision_id: "r2".into(),
                    capture_id: "c".repeat(64),
                }],
            }],
            outbound: Vec::new(),
            mermaid: String::new(),
            truncated: false,
            corpus_complete: true,
            collection_notices: Vec::new(),
        };
        let mut scan = ReferenceScan {
            candidates: Vec::new(),
            warnings: BTreeSet::new(),
            visited: 0,
        };
        scan.scan(&serde_json::to_value(result).unwrap(), None, 0);
        assert_eq!(scan.candidates.len(), 1);
        assert_eq!(scan.candidates[0].id.object.id, "citing-decision");
        assert_eq!(scan.candidates[0].id.capture_id, "c".repeat(64));
        assert!(matches!(
            scan.candidates[0].id.projection,
            CitationProjection::Metadata
        ));
        let mut scan = ReferenceScan {
            candidates: Vec::new(),
            warnings: BTreeSet::new(),
            visited: 0,
        };
        scan.scan(&json!({"entries":[{"object":metadata.object,"head_revision_id":"r1","previous_revision_id":"r0"}]}), None, 0);
        assert!(scan.candidates.is_empty());
        assert!(scan.warnings.contains("source_reference_unavailable"));
    }

    #[test]
    fn native_reference_descriptors_match_the_advertised_additive_schema() {
        let (service, metadata, _) = fixture();
        let mut registry = ToolRegistry::new();
        registry
            .register_typed::<SearchInput, MetadataResult, _, _>(
                "database.get_metadata",
                "Fictional metadata fixture",
                ToolOptions::default(),
                |_, _| async { Err(crate::registry::ToolError::Internal) },
            )
            .unwrap();
        registry.enable_citation_references().unwrap();
        let tool = registry.tools.get("database.get_metadata").unwrap();
        assert!(
            tool.definition.output_schema.as_ref().unwrap()["properties"]
                .get("references")
                .is_some()
        );
        let mut output = InvocationOutput {
            structured: serde_json::to_value(metadata).unwrap(),
            text: None,
            meta: None,
            additional_content: Vec::new(),
            strict_result_limit: false,
        };
        append_native_references("database.get_metadata", &service, None, &mut output);
        assert!(
            tool.output_validator
                .as_ref()
                .unwrap()
                .is_valid(&output.structured)
        );
        assert_eq!(output.structured["references"].as_array().unwrap().len(), 1);
        assert!(matches!(
            output.additional_content[0],
            ContentBlock::ResourceLink(_)
        ));
        assert!(output.strict_result_limit);
        output.structured["references"][0]["id"] = json!(123);
        assert!(
            !tool
                .output_validator
                .as_ref()
                .unwrap()
                .is_valid(&output.structured)
        );
    }

    #[test]
    fn observed_status_head_and_lineage_titles_use_existing_exact_identity() {
        let (service, metadata, _) = fixture();
        let mut output = InvocationOutput {
            structured: json!({"object":metadata.object,"head_capture_id":metadata.capture_id,"state":"published"}),
            text: None,
            meta: None,
            additional_content: Vec::new(),
            strict_result_limit: false,
        };
        append_native_references("database.object_status", &service, None, &mut output);
        let id =
            CitationId::decode(output.structured["references"][0]["id"].as_str().unwrap()).unwrap();
        assert_eq!(id.capture_id, metadata.capture_id);
        assert!(matches!(id.projection, CitationProjection::Metadata));
        assert!(output.structured.get("capture_id").is_none());
        let mut output = InvocationOutput {
            structured: json!({"object":metadata.object,"capture_id":metadata.capture_id,"current_title":"Fictional renamed statute"}),
            text: None,
            meta: None,
            additional_content: Vec::new(),
            strict_result_limit: false,
        };
        append_native_references("law.lineage", &service, None, &mut output);
        assert_eq!(
            output.structured["references"][0]["title"],
            "Fictional renamed statute"
        );
    }

    #[tokio::test]
    async fn dynamic_resources_keep_exact_body_or_metadata_and_revalidate() {
        let (service, _, id) = fixture();
        let encoded = id.encode().unwrap();
        let uri = id.resource_uri().unwrap();
        assert_eq!(id_from_resource_uri(&uri).unwrap(), encoded);
        let source = service
            .source(&encoded, CancellationToken::new())
            .await
            .unwrap();
        let result = resource_result(&uri, source).unwrap();
        let wire = serde_json::to_value(result).unwrap();
        assert_eq!(wire["resultType"], "complete");
        assert_eq!(wire["ttlMs"], 0);
        assert_eq!(wire["cacheScope"], "public");
        assert_eq!(wire["contents"][0]["text"], "가나다 citation evidence");
        assert_eq!(wire["contents"][0]["uri"], uri);
        let meta_id = CitationId {
            projection: CitationProjection::Metadata,
            ..id
        };
        let source = service
            .source(&meta_id.encode().unwrap(), CancellationToken::new())
            .await
            .unwrap();
        let result = resource_result(&meta_id.resource_uri().unwrap(), source).unwrap();
        let ResourceContents::TextResourceContents { text, .. } = &result.contents[0] else {
            panic!("text resource expected")
        };
        let meta: Value = serde_json::from_str(text).unwrap();
        assert_eq!(meta["evidence"], "metadata_only");
        assert!(meta.get("body").is_none());
        let ocr_id = CitationId {
            projection: CitationProjection::Section {
                section: "첨부/50% A".into(),
            },
            ..meta_id
        };
        let source = service
            .source(&ocr_id.encode().unwrap(), CancellationToken::new())
            .await
            .unwrap();
        let wire =
            serde_json::to_value(resource_result(&ocr_id.resource_uri().unwrap(), source).unwrap())
                .unwrap();
        let provenance = &wire["contents"][0]["_meta"]["openlegal/provenance"];
        assert_eq!(provenance["derived_ocr"], "true");
        assert_eq!(provenance["source_document_sha256"], "d".repeat(64));
        assert_eq!(provenance["page"], "3");
        assert_eq!(provenance["section_kind"], "ocr");
        assert_eq!(provenance["representation"], "provider_effective_original");
        assert!(id_from_resource_uri("file:///etc/passwd").is_err());
        assert!(id_from_resource_uri("openlegal://source/v1/not-an-id").is_err());
    }

    #[tokio::test]
    async fn source_pages_share_call_rate_admission_and_cancellation() {
        let (service, _, mut id) = fixture();
        id.projection = CitationProjection::Metadata;
        let handler = McpHandler::new(
            ToolRegistry::new(),
            Arc::new(Limits {
                rate_limit: RateLimitConfig {
                    calls_per_second: 1,
                    burst: 1,
                    ..RateLimitConfig::default()
                },
                ..Limits::default()
            }),
            SourceOffer::new("https://source.test/running").unwrap(),
        )
        .unwrap()
        .with_citations(Some(service.clone()))
        .unwrap();
        assert!(
            rmcp::ServerHandler::get_info(&handler)
                .capabilities
                .resources
                .is_some()
        );
        let encoded = id.encode().unwrap();
        handler
            .citation_source(&encoded, CancellationToken::new())
            .await
            .unwrap();
        assert!(matches!(
            handler
                .citation_source(&encoded, CancellationToken::new())
                .await,
            Err(DatabaseError::Capacity)
        ));
        assert_eq!(
            handler
                .counters
                .rate_limited
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );
        let cancelled = CancellationToken::new();
        cancelled.cancel();
        let handler = McpHandler::new(
            ToolRegistry::new(),
            Arc::new(Limits {
                rate_limit: RateLimitConfig {
                    enabled: false,
                    ..RateLimitConfig::default()
                },
                ..Limits::default()
            }),
            SourceOffer::new("https://source.test/running").unwrap(),
        )
        .unwrap()
        .with_citations(Some(service))
        .unwrap();
        assert!(matches!(
            handler.citation_source(&encoded, cancelled).await,
            Err(DatabaseError::Cancelled)
        ));
    }

    async fn rpc(url: &str, version: &str, method: &str, mut params: Value) -> Value {
        if version == "2026-07-28" {
            params["_meta"] = json!({"io.modelcontextprotocol/protocolVersion":version,"io.modelcontextprotocol/clientInfo":{"name":"fictional-citation-test","version":"1"},"io.modelcontextprotocol/clientCapabilities":{}});
        }
        let mut request = reqwest::Client::new()
            .post(url)
            .header("Host", "test.local")
            .header("Accept", "application/json, text/event-stream")
            .header("MCP-Protocol-Version", version)
            .header("Mcp-Method", method);
        if let Some(name) = params
            .get("name")
            .or_else(|| params.get("uri"))
            .and_then(Value::as_str)
        {
            request = request.header("Mcp-Name", name);
        }
        let response = request
            .json(&json!({"jsonrpc":"2.0","id":1,"method":method,"params":params}))
            .send()
            .await
            .unwrap();
        let status = response.status();
        let body = response.text().await.unwrap();
        assert_eq!(status, 200, "{body}");
        let value: Value = body
            .lines()
            .rev()
            .filter_map(|line| line.strip_prefix("data:"))
            .find_map(|line| serde_json::from_str(line.trim()).ok())
            .or_else(|| serde_json::from_str(&body).ok())
            .unwrap();
        assert!(value.get("error").is_none(), "{value}");
        value["result"].clone()
    }

    #[tokio::test]
    async fn compatibility_tools_and_dynamic_resources_work_in_both_http_revisions() {
        use crate::{ServerBuilder, config::AccessPolicy, http::HttpEndpoint};
        let (service, metadata, _) = fixture();
        let mut registry = ToolRegistry::new();
        registry
            .register_module(CitationTools {
                service: service.clone(),
            })
            .unwrap();
        let fixture_metadata = metadata.clone();
        registry
            .register_typed::<openlegal_domain::legal::GetRequest, MetadataResult, _, _>(
                "database.get_metadata",
                "Fictional exact metadata",
                ToolOptions::default(),
                move |_, _| {
                    let metadata = fixture_metadata.clone();
                    async move { Ok(crate::registry::ToolOutput::new(metadata)) }
                },
            )
            .unwrap();
        let mut builder = ServerBuilder::new(
            registry,
            Limits::default(),
            SourceOffer::new("https://source.test/running").unwrap(),
        )
        .with_citations(service);
        builder
            .register_endpoint(HttpEndpoint {
                tls: None,
                edge_mtls: None,
                bind: "127.0.0.1:0".parse().unwrap(),
                access: AccessPolicy {
                    allowed_hosts: vec!["test.local".into()],
                    allowed_origins: Vec::new(),
                },
            })
            .unwrap();
        let server = builder.bind().await.unwrap();
        let url = format!("http://{}/mcp", server.addresses()[0].1[0]);
        let shutdown = CancellationToken::new();
        let token = shutdown.clone();
        let task = tokio::spawn(server.run(token));
        let _guard = shutdown.clone().drop_guard();
        for version in ["2025-11-25", "2026-07-28"] {
            let search = rpc(
                &url,
                version,
                "tools/call",
                json!({"name":"search","arguments":{"query":"citation"}}),
            )
            .await;
            assert_eq!(search["structuredContent"].as_object().unwrap().len(), 1);
            let first: Value =
                serde_json::from_str(search["content"][0]["text"].as_str().unwrap()).unwrap();
            assert_eq!(first, search["structuredContent"]);
            assert!(
                search["content"].as_array().unwrap().len() > 1,
                "Partial search must be qualified"
            );
            let result = &search["structuredContent"]["results"][0];
            let fetch = rpc(
                &url,
                version,
                "tools/call",
                json!({"name":"fetch","arguments":{"id":result["id"]}}),
            )
            .await;
            assert_eq!(fetch["structuredContent"]["id"], result["id"]);
            assert_eq!(fetch["structuredContent"]["url"], result["url"]);
            let first: Value =
                serde_json::from_str(fetch["content"][0]["text"].as_str().unwrap()).unwrap();
            assert_eq!(first, fetch["structuredContent"]);
            assert_eq!(
                fetch["structuredContent"]["text"],
                "가나다 citation evidence"
            );
            let templates = rpc(&url, version, "resources/templates/list", json!({})).await;
            assert_eq!(
                templates["resourceTemplates"][0]["uriTemplate"],
                "openlegal://source/{+id}"
            );
            let uri = format!("{RESOURCE_PREFIX}{}", result["id"].as_str().unwrap());
            let read = rpc(&url, version, "resources/read", json!({"uri":uri})).await;
            assert_eq!(
                read["contents"][0]["text"],
                fetch["structuredContent"]["text"]
            );
            assert_eq!(read["ttlMs"], 0);
            assert_eq!(read["cacheScope"], "public");
            assert_eq!(read.get("resultType").is_some(), version == "2026-07-28");
            let native = rpc(&url, version, "tools/call", json!({"name":"database.get_metadata","arguments":{"object":metadata.object,"selector":{"kind":"capture","id":metadata.capture_id}}})).await;
            assert_eq!(
                native["structuredContent"]["references"]
                    .as_array()
                    .unwrap()
                    .len(),
                1
            );
            assert_eq!(native["content"][1]["type"], "resource_link");
            assert_eq!(
                native["content"][1]["uri"],
                native["structuredContent"]["references"][0]["uri"]
            );
            assert!(native["content"].as_array().unwrap().iter().any(|block| {
                block
                    .get("text")
                    .and_then(Value::as_str)
                    .is_some_and(|text| text.contains("metadata_only"))
            }));
        }
        let origin = url.strip_suffix("/mcp").unwrap();
        let ocr_id = CitationId {
            object: metadata.object.clone(),
            capture_id: metadata.capture_id.clone(),
            projection: CitationProjection::Section {
                section: "첨부/50% A".into(),
            },
        };
        let encoded = ocr_id.encode().unwrap();
        assert!(encoded.contains("%2F"));
        assert!(encoded.contains("%25"));
        let client = reqwest::Client::new();
        let browser_url = format!("{origin}/source/{encoded}");
        let english = client
            .get(&browser_url)
            .header("Host", "test.local")
            .send()
            .await
            .unwrap();
        assert_eq!(english.status(), 200);
        assert_eq!(english.headers()["cache-control"], "no-store");
        assert!(
            english.headers()["content-security-policy"]
                .to_str()
                .unwrap()
                .contains("default-src 'none'")
        );
        assert_eq!(english.headers()["x-content-type-options"], "nosniff");
        let english = english.text().await.unwrap();
        assert!(english.contains("lang=\"en\""));
        assert!(english.contains(&metadata.capture_id));
        assert!(english.contains("&lt;script&gt;"));
        assert!(!english.contains("<script>"));
        assert!(english.contains("OCR-derived text"));
        let korean = client
            .get(format!("{browser_url}?lang=ko"))
            .header("Host", "test.local")
            .send()
            .await
            .unwrap();
        assert_eq!(korean.status(), 200);
        let korean = korean.text().await.unwrap();
        assert!(korean.contains("lang=\"ko\""));
        assert!(korean.contains("저장 버전"));
        let projection = |html: &str| {
            html.split_once("<pre>")
                .unwrap()
                .1
                .split_once("</pre>")
                .unwrap()
                .0
                .to_string()
        };
        assert_eq!(
            projection(&english),
            projection(&korean),
            "Language labels must not translate retained text"
        );
        let head = client
            .head(&browser_url)
            .header("Host", "test.local")
            .send()
            .await
            .unwrap();
        assert_eq!(head.status(), 200);
        assert_eq!(head.headers()["content-type"], "text/html; charset=utf-8");
        assert!(head.bytes().await.unwrap().is_empty());
        let malformed = client
            .get(format!("{origin}/source/v1/not-an-id"))
            .header("Host", "test.local")
            .send()
            .await
            .unwrap();
        assert_eq!(malformed.status(), 400);
        let invalid_language = client
            .get(format!("{browser_url}?lang=fr"))
            .header("Host", "test.local")
            .send()
            .await
            .unwrap();
        assert_eq!(invalid_language.status(), 400);
        let unknown = CitationId {
            object: object(Dataset::NationalStatute, "unknown"),
            capture_id: "a".repeat(64),
            projection: CitationProjection::Document,
        };
        let response = client
            .get(format!("{origin}/source/{}", unknown.encode().unwrap()))
            .header("Host", "test.local")
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 404);
        let expired = CitationId {
            object: object(Dataset::NationalStatute, "expired"),
            capture_id: "f".repeat(64),
            projection: CitationProjection::Document,
        };
        let response = client
            .get(format!("{origin}/source/{}", expired.encode().unwrap()))
            .header("Host", "test.local")
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 410);
        let expired_page = response.text().await.unwrap();
        assert!(expired_page.contains("Expired fictional evidence"));
        assert!(expired_page.contains(&expired.capture_id));
        assert!(!expired_page.contains("Replacement HEAD"));
        assert!(!expired_page.contains("가나다 citation evidence"));
        shutdown.cancel();
        task.await.unwrap().unwrap();
    }
}
