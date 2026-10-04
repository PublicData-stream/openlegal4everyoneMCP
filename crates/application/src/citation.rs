//! Shared, read-only citation projection over retained exact captures. Leases
//! protect evidence briefly; they never authorize collection or HEAD fallback.
use crate::{Clock, database::DatabaseService, search::SearchService};
use futures::future::BoxFuture;
use openlegal_domain::{
    citation::{
        CitationDescriptor, CitationDocument, CitationId, CitationProjection, CitationSearch,
        CitationSearchResult, CitationSource, MAX_CITATION_TEXT_BYTES, dataset_name,
        official_browser_url, reference_base,
    },
    legal::{
        Capture, DatabaseError as E, GetRequest, GetResult, MetadataResult, ObjectId,
        RevisionSelector, SectionKind,
    },
    legal_search::{Filters, SearchHit, SearchRequest},
};
use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio_util::sync::CancellationToken;

pub const CITATION_LEASE_SECONDS: u64 = 600;
pub const MAX_CITATION_LEASES: usize = 2048;
pub const MAX_CITATION_SEARCH_RESULTS: usize = 20;

pub trait CitationLease: Send + Sync + 'static {
    /// Atomic deduplicated renewal. Implementations validate public eligibility
    /// before pinning: physical existence alone cannot revive expired text.
    fn renew(
        &self,
        objects: Vec<(ObjectId, String)>,
        now: u64,
        cancel: CancellationToken,
    ) -> BoxFuture<'static, Result<(), E>>;
}

pub struct CitationService {
    database: Arc<DatabaseService>,
    search: Arc<SearchService>,
    leases: Arc<dyn CitationLease>,
    clock: Arc<dyn Clock>,
    base_url: String,
}

impl CitationService {
    pub fn new(
        database: Arc<DatabaseService>,
        search: Arc<SearchService>,
        leases: Arc<dyn CitationLease>,
        clock: Arc<dyn Clock>,
        base_url: String,
    ) -> Result<Self, E> {
        let base_url = reference_base(&base_url)?
            .as_str()
            .trim_end_matches('/')
            .to_string();
        Ok(Self {
            database,
            search,
            leases,
            clock,
            base_url,
        })
    }

    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    pub fn original_url(&self, capture_id: &str, ordinal: u32) -> Result<String, E> {
        if !openlegal_domain::history::valid_snapshot_id(capture_id) {
            return Err(E::InvalidInput);
        }
        Ok(format!(
            "{}/source-file/{capture_id}/{ordinal}",
            self.base_url
        ))
    }
    pub async fn original_evidence(
        &self,
        capture_id: &str,
        ordinal: u32,
        cancel: CancellationToken,
    ) -> Result<openlegal_domain::rights::OriginalEvidence, E> {
        self.database
            .original_evidence(capture_id, ordinal, cancel)
            .await
    }
    pub fn reference_url(&self, id: &CitationId) -> Result<String, E> {
        id.reference_url(&self.base_url)
    }

    pub fn descriptor(
        &self,
        id: &CitationId,
        title: &str,
        body_available: Option<bool>,
        official_url: Option<String>,
    ) -> Result<CitationDescriptor, E> {
        Ok(CitationDescriptor {
            id: id.encode()?,
            uri: id.resource_uri()?,
            url: self.reference_url(id)?,
            title: title.to_string(),
            body_available,
            official_url,
        })
    }

    pub async fn search(
        &self,
        query: String,
        deadline: Instant,
        cancel: CancellationToken,
    ) -> Result<CitationSearch, E> {
        let deadline = deadline.min(Instant::now() + Duration::from_secs(10));
        let cancel = cancel.child_token();
        let _guard = cancel.clone().drop_guard();
        tokio::select! {
            _ = cancel.cancelled() => Err(E::Cancelled),
            _ = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => Err(E::Capacity),
            result = self.search_inner(query, deadline, cancel.clone()) => result,
        }
    }

    async fn search_inner(
        &self,
        query: String,
        deadline: Instant,
        cancel: CancellationToken,
    ) -> Result<CitationSearch, E> {
        let request = SearchRequest {
            query,
            filters: Filters::default(),
            include_history: false,
            include_ocr: false,
            sections: Vec::new(),
            limit: MAX_CITATION_SEARCH_RESULTS,
            cursor: None,
            literal: false,
            ignore_case: false,
            context_lines: 0,
        };
        // The backend establishes citation leases while its generation is still
        // protected. A plain search followed by renew would permit a GC race.
        let page = self
            .search
            .search_citable(request, deadline, cancel.clone())
            .await?;
        if page.hits.len() > MAX_CITATION_SEARCH_RESULTS {
            return Err(E::StorageCorrupt);
        }
        let mut results = Vec::new();
        let mut rights_warnings = std::collections::BTreeSet::new();
        for hit in &page.hits {
            let result = self
                .database
                .get(exact_request(&hit.object, &hit.capture_id), cancel.clone())
                .await?;
            rights_warnings.extend(openlegal_domain::rights::warnings(
                &result.capture.record.metadata,
            ));
            let id = projection_for_hit(&result.capture, hit)?;
            let text = project_text(&result.capture, &id)?;
            if text.len() > MAX_CITATION_TEXT_BYTES {
                return Err(E::Capacity);
            }
            results.push(CitationSearchResult {
                id: id.encode()?,
                title: projection_title(&result.capture.record.title, &id.projection),
                url: self.reference_url(&id)?,
            });
        }
        let mut warnings: Vec<String> = Vec::new();
        if !page.corpus_complete {
            warnings.push("The retained corpus is incomplete; absence of a match does not establish absence of law or decisions.".into());
        }
        if page.next_cursor.is_some() {
            warnings.push("The bounded search stopped before scanning every candidate; refine the query to find additional matches.".into());
        }
        if page.index_lag > 0 {
            warnings.push(
                "The search index has not yet processed every retained corpus update.".into(),
            );
        }
        if !page.collection_notices.is_empty() {
            warnings.push("Some source collection or processing is incomplete.".into());
        }
        let partial = !warnings.is_empty();
        warnings.extend(rights_warnings.into_iter().map(|warning| {
            format!("Source reuse condition: {warning}. Consult the source rights and attribution.")
        }));
        Ok(CitationSearch {
            results,
            partial,
            warnings,
        })
    }

    async fn resolve(&self, id: &CitationId, cancel: CancellationToken) -> Result<GetResult, E> {
        id.validate()?;
        if cancel.is_cancelled() {
            return Err(E::Cancelled);
        }
        self.leases
            .renew(
                vec![(id.object.clone(), id.capture_id.clone())],
                self.clock.now(),
                cancel.clone(),
            )
            .await?;
        self.database
            .get(exact_request(&id.object, &id.capture_id), cancel)
            .await
    }

    pub async fn fetch(&self, id: &str, cancel: CancellationToken) -> Result<CitationDocument, E> {
        let id = CitationId::decode(id)?;
        if matches!(id.projection, CitationProjection::Metadata) {
            return Err(E::RevisionUnavailable);
        }
        let result = self.resolve(&id, cancel).await?;
        self.document(&id, &result.capture)
    }

    pub async fn describe(
        &self,
        id: &str,
        cancel: CancellationToken,
    ) -> Result<CitationDescriptor, E> {
        let id = CitationId::decode(id)?;
        let metadata = self
            .database
            .get_metadata(exact_request(&id.object, &id.capture_id), cancel)
            .await?;
        self.descriptor(
            &id,
            &projection_title(&metadata.title, &id.projection),
            None,
            official_browser_url(&metadata),
        )
    }

    /// Exact retained metadata can survive body expiry. An expired locator is a
    /// requested locator, not verified evidence that its article/range existed.
    pub async fn source(
        &self,
        encoded: &str,
        cancel: CancellationToken,
    ) -> Result<CitationSource, E> {
        let id = CitationId::decode(encoded)?;
        let metadata = self
            .database
            .get_metadata(exact_request(&id.object, &id.capture_id), cancel.clone())
            .await?;
        let official = official_browser_url(&metadata);
        if matches!(id.projection, CitationProjection::Metadata) {
            let descriptor = self.descriptor(&id, &metadata.title, None, official)?;
            return Ok(CitationSource {
                descriptor,
                metadata,
                document: None,
                previous: None,
                next: None,
                unavailable: false,
            });
        }
        let resolved = match self.resolve(&id, cancel).await {
            Ok(value) => value,
            Err(E::RevisionUnavailable) => {
                let descriptor = self.descriptor(&id, &metadata.title, Some(false), official)?;
                return Ok(CitationSource {
                    descriptor,
                    metadata,
                    document: None,
                    previous: None,
                    next: None,
                    unavailable: true,
                });
            }
            Err(error) => return Err(error),
        };
        let document = self.document(&id, &resolved.capture)?;
        let descriptor = self.descriptor(&id, &document.title, Some(true), official.clone())?;
        let (previous, next) = adjacent_ids(&resolved.capture, &id)?;
        let describe = |other: CitationId| {
            self.descriptor(
                &other,
                &projection_title(&metadata.title, &other.projection),
                Some(true),
                official.clone(),
            )
        };
        let previous = previous.map(describe).transpose()?;
        let next = next.map(describe).transpose()?;
        Ok(CitationSource {
            descriptor,
            metadata,
            document: Some(document),
            previous,
            next,
            unavailable: false,
        })
    }

    /// Browser pages make metadata references useful by opening the first exact
    /// body unit when it remains retained. MCP metadata reads stay metadata-only.
    pub async fn page(
        &self,
        encoded: &str,
        cancel: CancellationToken,
    ) -> Result<CitationSource, E> {
        let requested = CitationId::decode(encoded)?;
        if !matches!(
            requested.projection,
            CitationProjection::Metadata | CitationProjection::Document
        ) {
            return self.source(encoded, cancel).await;
        }
        let metadata = self
            .database
            .get_metadata(
                exact_request(&requested.object, &requested.capture_id),
                cancel.clone(),
            )
            .await?;
        let official = official_browser_url(&metadata);
        let resolved = match self.resolve(&requested, cancel).await {
            Ok(value) => value,
            Err(E::RevisionUnavailable) => {
                let descriptor =
                    self.descriptor(&requested, &metadata.title, Some(false), official)?;
                return Ok(CitationSource {
                    descriptor,
                    metadata,
                    document: None,
                    previous: None,
                    next: None,
                    unavailable: true,
                });
            }
            Err(error) => return Err(error),
        };
        let body = &resolved.capture.record.body;
        let projection = if body.len() <= MAX_CITATION_TEXT_BYTES {
            CitationProjection::Document
        } else {
            let mut end = MAX_CITATION_TEXT_BYTES;
            while !body.is_char_boundary(end) {
                end -= 1;
            }
            CitationProjection::Passage {
                section: "body".into(),
                start: 0,
                end,
            }
        };
        let unit = CitationId {
            projection,
            ..requested.clone()
        };
        let document = self.document(&unit, &resolved.capture)?;
        let descriptor =
            self.descriptor(&requested, &metadata.title, Some(true), official.clone())?;
        let (_, next) = adjacent_ids(&resolved.capture, &unit)?;
        let next = next
            .map(|id| {
                self.descriptor(
                    &id,
                    &projection_title(&metadata.title, &id.projection),
                    Some(true),
                    official.clone(),
                )
            })
            .transpose()?;
        Ok(CitationSource {
            descriptor,
            metadata,
            document: Some(document),
            previous: None,
            next,
            unavailable: false,
        })
    }

    fn document(&self, id: &CitationId, capture: &Capture) -> Result<CitationDocument, E> {
        let text = project_text(capture, id)?;
        let metadata: MetadataResult = GetResult {
            capture: capture.clone(),
            freshness: None,
        }
        .into();
        let mut compact = BTreeMap::from([
            ("jurisdiction".into(), id.object.jurisdiction.clone()),
            ("provider".into(), id.object.provider.clone()),
            ("dataset".into(), dataset_name(id.object.dataset).into()),
            ("object_id".into(), id.object.id.clone()),
            ("revision_id".into(), capture.record.revision_id.clone()),
            ("capture_id".into(), capture.capture_id.clone()),
            ("retrieved_at".into(), capture.retrieved_at.to_string()),
            ("captured_at".into(), capture.captured_at.to_string()),
            ("validated_at".into(), capture.validated_at.to_string()),
            (
                "processor_version".into(),
                capture.processor_version.clone(),
            ),
            ("raw_sha256".into(), capture.raw_sha256.clone()),
            (
                "representation".into(),
                capture.record.representation.clone(),
            ),
            ("source_reference".into(), capture.record.source_url.clone()),
        ]);
        if let Some(url) = official_browser_url(&metadata) {
            compact.insert("official_browser_url".into(), url);
        }
        for (key, value) in [
            ("publication_date", &capture.record.publication_date),
            ("effective_date", &capture.record.effective_date),
        ] {
            if let Some(value) = value {
                compact.insert(key.into(), value.clone());
            }
        }
        for key in ["original_resources", "source_rights", "rights_warnings"] {
            if let Some(value) = capture.record.metadata.get(key) {
                compact.insert(key.into(), value.clone());
            }
        }
        for key in [
            "case_number",
            "authority",
            "court",
            "judgment_date",
            "attachment_status",
            "transport_credentials_redacted",
            "section_locator_semantics",
        ] {
            if let Some(value) = capture.record.metadata.get(key).filter(|v| v.len() <= 1024) {
                compact.insert(key.into(), value.clone());
            }
        }
        let section = match &id.projection {
            CitationProjection::Section { section }
            | CitationProjection::Passage { section, .. } => Some(section),
            _ => None,
        };
        if let Some(section) = section {
            compact.insert("section".into(), section.clone());
            if let Some(unit) = capture.record.sections.iter().find(|s| &s.id == section) {
                let kind = match unit.kind {
                    SectionKind::ProviderText => "provider_text",
                    SectionKind::Extracted => "extracted",
                    SectionKind::Ocr => "ocr",
                };
                compact.insert("section_kind".into(), kind.into());
                compact.insert(
                    "derived_ocr".into(),
                    (unit.kind == SectionKind::Ocr).to_string(),
                );
                if let Some(sha) = &unit.source_document_sha256 {
                    compact.insert("source_document_sha256".into(), sha.clone());
                }
                if let Some(page) = unit.page {
                    compact.insert("page".into(), page.to_string());
                }
            } else if section != "body" {
                compact.insert("section_kind".into(), "provider_projection".into());
            }
        }
        if section.is_none_or(|section| section == "body") {
            let mut extracted = false;
            let mut ocr = false;
            for unit in &capture.record.sections {
                if !unit.text.is_empty() && text.contains(&unit.text) {
                    extracted |= unit.kind == SectionKind::Extracted;
                    ocr |= unit.kind == SectionKind::Ocr;
                }
            }
            compact.insert(
                "section_kind".into(),
                if extracted || ocr {
                    "mixed"
                } else {
                    "provider_projection"
                }
                .into(),
            );
            compact.insert("derived_ocr".into(), ocr.to_string());
            compact.insert("includes_extracted".into(), extracted.to_string());
            compact.insert("text_qualification".into(), if extracted || ocr {
                "This normalized body unit contains text also retained in extracted or OCR sections; it must not be represented as wholly original provider text."
            } else {
                "This body unit is a normalized projection. Its representation and capture sections retain technical provenance; absence of a matching complete derived section does not establish wholly original provider text."
            }.into());
        }
        if let CitationProjection::Passage { start, end, .. } = &id.projection {
            compact.insert("byte_start".into(), start.to_string());
            compact.insert("byte_end".into(), end.to_string());
        }
        Ok(CitationDocument {
            id: id.encode()?,
            title: projection_title(&capture.record.title, &id.projection),
            text,
            url: self.reference_url(id)?,
            metadata: compact,
        })
    }
}

fn exact_request(object: &ObjectId, capture: &str) -> GetRequest {
    GetRequest {
        object: object.clone(),
        selector: RevisionSelector::Capture { id: capture.into() },
        fresh_only: false,
    }
}

pub fn projection_title(title: &str, projection: &CitationProjection) -> String {
    match projection {
        CitationProjection::Document | CitationProjection::Metadata => title.into(),
        CitationProjection::Section { section } => format!("{title} [{section}]"),
        CitationProjection::Passage {
            section,
            start,
            end,
        } => format!("{title} [{section}, bytes {start}–{end}]"),
    }
}

pub fn section_text<'a>(capture: &'a Capture, section: &str) -> Result<&'a str, E> {
    match section {
        "body" => Ok(&capture.record.body),
        "title" => Ok(&capture.record.title),
        "case_number" => capture
            .record
            .metadata
            .get("case_number")
            .map(String::as_str)
            .ok_or(E::NotFound),
        other => capture
            .record
            .sections
            .iter()
            .find(|s| s.id == other)
            .map(|s| s.text.as_str())
            .ok_or(E::NotFound),
    }
}

pub fn project_text(capture: &Capture, id: &CitationId) -> Result<String, E> {
    id.validate()?;
    if capture.record.object != id.object || capture.capture_id != id.capture_id {
        return Err(E::RevisionUnavailable);
    }
    let text = match &id.projection {
        CitationProjection::Document => capture.record.body.as_str(),
        CitationProjection::Metadata => return Err(E::RevisionUnavailable),
        CitationProjection::Section { section } => section_text(capture, section)?,
        CitationProjection::Passage {
            section,
            start,
            end,
        } => {
            let text = section_text(capture, section)?;
            text.get(*start..*end).ok_or(E::InvalidInput)?
        }
    };
    if text.len() > MAX_CITATION_TEXT_BYTES {
        return Err(E::Capacity);
    }
    Ok(text.to_string())
}

/// Projection policy is independent of caller byte budgets: small primary bodies
/// use a whole-document identity, otherwise an exact section or bounded passage.
pub fn projection_for_hit(capture: &Capture, hit: &SearchHit) -> Result<CitationId, E> {
    if capture.record.object != hit.object
        || capture.capture_id != hit.capture_id
        || capture.record.revision_id != hit.revision_id
    {
        return Err(E::RevisionUnavailable);
    }
    let section = &hit.excerpt_section;
    let text = section_text(capture, section)?;
    if hit.byte_start >= hit.byte_end
        || text.get(hit.byte_start..hit.byte_end) != Some(hit.text.as_str())
    {
        return Err(E::InvalidInput);
    }
    let projection = if capture.record.body.len() <= MAX_CITATION_TEXT_BYTES
        && !hit.includes_ocr
        && section == "body"
    {
        CitationProjection::Document
    } else {
        if text.len() <= MAX_CITATION_TEXT_BYTES {
            CitationProjection::Section {
                section: section.clone(),
            }
        } else {
            // Prefer a complete source article when its unambiguous text occurs
            // in the selected body and contains the complete supporting excerpt.
            let article = if section == "body" {
                capture
                    .record
                    .sections
                    .iter()
                    .filter(|s| {
                        s.kind == SectionKind::ProviderText
                            && s.id.starts_with("article:")
                            && !s.text.is_empty()
                            && s.text.len() <= MAX_CITATION_TEXT_BYTES
                    })
                    .find(|s| {
                        let mut occurrences = text.match_indices(&s.text);
                        let Some((start, _)) = occurrences.next() else {
                            return false;
                        };
                        occurrences.next().is_none()
                            && start <= hit.byte_start
                            && hit.byte_end <= start + s.text.len()
                    })
            } else {
                None
            };
            if let Some(article) = article {
                CitationProjection::Section {
                    section: article.id.clone(),
                }
            } else {
                // Keep the complete validated excerpt. An external backend must
                // provide a bounded supporting excerpt instead of asking this
                // layer to guess which part of oversized evidence matched.
                let excerpt_bytes = hit.byte_end - hit.byte_start;
                if excerpt_bytes > MAX_CITATION_TEXT_BYTES {
                    return Err(E::Capacity);
                }
                let leading_context = MAX_CITATION_TEXT_BYTES.saturating_sub(excerpt_bytes) / 2;
                let mut start = hit.byte_start.saturating_sub(leading_context);
                while !text.is_char_boundary(start) {
                    start += 1;
                }
                let mut end = start
                    .saturating_add(MAX_CITATION_TEXT_BYTES)
                    .min(text.len());
                while !text.is_char_boundary(end) {
                    end -= 1;
                }
                CitationProjection::Passage {
                    section: section.clone(),
                    start,
                    end,
                }
            }
        }
    };
    let id = CitationId {
        object: hit.object.clone(),
        capture_id: hit.capture_id.clone(),
        projection,
    };
    id.validate()?;
    Ok(id)
}

/// Split a supplied exact source slice without changing its original offsets.
pub fn passage_ids(
    object: &ObjectId,
    capture_id: &str,
    section: &str,
    text: &str,
    offset: usize,
) -> Result<Vec<CitationId>, E> {
    let mut ids = Vec::new();
    let mut start = 0;
    while start < text.len() {
        let mut end = (start + MAX_CITATION_TEXT_BYTES).min(text.len());
        while !text.is_char_boundary(end) {
            end -= 1;
        }
        let id = CitationId {
            object: object.clone(),
            capture_id: capture_id.into(),
            projection: CitationProjection::Passage {
                section: section.into(),
                start: offset.checked_add(start).ok_or(E::InvalidInput)?,
                end: offset.checked_add(end).ok_or(E::InvalidInput)?,
            },
        };
        id.validate()?;
        ids.push(id);
        start = end;
    }
    Ok(ids)
}

fn adjacent_ids(
    capture: &Capture,
    id: &CitationId,
) -> Result<(Option<CitationId>, Option<CitationId>), E> {
    let CitationProjection::Passage {
        section,
        start,
        end,
    } = &id.projection
    else {
        return Ok((None, None));
    };
    let text = section_text(capture, section)?;
    let previous = if *start > 0 {
        let mut before = start.saturating_sub(MAX_CITATION_TEXT_BYTES);
        while !text.is_char_boundary(before) {
            before += 1;
        }
        Some(CitationId {
            projection: CitationProjection::Passage {
                section: section.clone(),
                start: before,
                end: *start,
            },
            ..id.clone()
        })
    } else {
        None
    };
    let next = if *end < text.len() {
        let mut after = end.saturating_add(MAX_CITATION_TEXT_BYTES).min(text.len());
        while !text.is_char_boundary(after) {
            after -= 1;
        }
        Some(CitationId {
            projection: CitationProjection::Passage {
                section: section.clone(),
                start: *end,
                end: after,
            },
            ..id.clone()
        })
    } else {
        None
    };
    Ok((previous, next))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        database::DatabaseStore,
        search::{SearchBackend, SearchBudget, SearchMode},
    };
    use openlegal_domain::legal::{Dataset, LegalRecord};
    use openlegal_domain::{
        legal::{HistoryKind, HistoryPage},
        legal_search::SearchPage,
    };
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    fn capture(text: String) -> Capture {
        Capture {
            capture_id: "a".repeat(64),
            sequence: 1,
            record: LegalRecord {
                object: ObjectId {
                    jurisdiction: "kr".into(),
                    provider: "fictional".into(),
                    dataset: Dataset::NationalStatute,
                    id: "001".into(),
                },
                revision_id: "r1".into(),
                title: "Fictional law".into(),
                body: text,
                metadata: BTreeMap::new(),
                publication_date: None,
                effective_date: None,
                source_url: "https://example.test/fictional".into(),
                representation: "fictional".into(),
                sections: Vec::new(),
            },
            retrieved_at: 10,
            captured_at: 20,
            validated_at: 20,
            processor_version: "fixture".into(),
            raw_sha256: "b".repeat(64),
        }
    }
    #[test]
    fn unicode_units_reconstruct_original_without_budget_dependent_ids() {
        let capture = capture("가나다🙂\n".repeat(3000));
        let ids = passage_ids(
            &capture.record.object,
            &capture.capture_id,
            "body",
            &capture.record.body,
            0,
        )
        .unwrap();
        let texts: Vec<_> = ids
            .iter()
            .map(|id| project_text(&capture, id).unwrap())
            .collect();
        assert!(
            texts
                .iter()
                .all(|text| text.len() <= MAX_CITATION_TEXT_BYTES)
        );
        assert_eq!(texts.concat(), capture.record.body);
        for id in &ids {
            assert_eq!(CitationId::decode(&id.encode().unwrap()).unwrap(), *id);
        }
        let (previous, next) = adjacent_ids(&capture, &ids[1]).unwrap();
        assert!(previous.is_some() && next.is_some());
        assert_eq!(
            project_text(&capture, &previous.unwrap()).unwrap(),
            texts[0]
        );
    }
    #[test]
    fn exact_identity_and_utf8_boundaries_never_fall_back() {
        let capture = capture("민법".into());
        let mut id = CitationId {
            object: capture.record.object.clone(),
            capture_id: capture.capture_id.clone(),
            projection: CitationProjection::Document,
        };
        assert_eq!(project_text(&capture, &id).unwrap(), "민법");
        id.capture_id = "c".repeat(64);
        assert_eq!(
            project_text(&capture, &id).unwrap_err(),
            E::RevisionUnavailable
        );
        id.capture_id = capture.capture_id.clone();
        id.projection = CitationProjection::Passage {
            section: "body".into(),
            start: 1,
            end: 3,
        };
        assert_eq!(project_text(&capture, &id).unwrap_err(), E::InvalidInput);
    }

    struct TestClock;
    impl Clock for TestClock {
        fn now(&self) -> u64 {
            100
        }
    }
    struct TestStore {
        capture: Capture,
        body_error: Option<E>,
        protected: Arc<AtomicBool>,
    }
    impl DatabaseStore for TestStore {
        fn resolve(
            &self,
            object: ObjectId,
            selector: RevisionSelector,
            _now: u64,
            _cancel: CancellationToken,
        ) -> BoxFuture<'static, Result<Capture, E>> {
            assert!(
                self.protected.load(Ordering::SeqCst),
                "source retrieval preceded retention protection"
            );
            let capture = self.capture.clone();
            let error = self.body_error;
            Box::pin(async move {
                assert_eq!(
                    selector,
                    RevisionSelector::Capture {
                        id: capture.capture_id.clone()
                    }
                );
                assert_eq!(object, capture.record.object);
                match error {
                    Some(error) => Err(error),
                    None => Ok(capture),
                }
            })
        }
        fn resolve_metadata(
            &self,
            object: ObjectId,
            selector: RevisionSelector,
            _now: u64,
            _cancel: CancellationToken,
        ) -> BoxFuture<'static, Result<MetadataResult, E>> {
            let capture = self.capture.clone();
            Box::pin(async move {
                assert_eq!(object, capture.record.object);
                assert_eq!(
                    selector,
                    RevisionSelector::Capture {
                        id: capture.capture_id.clone()
                    }
                );
                Ok(GetResult {
                    capture,
                    freshness: None,
                }
                .into())
            })
        }
        fn history(
            &self,
            _object: ObjectId,
            _kind: HistoryKind,
            _cursor: Option<String>,
            _limit: usize,
            _now: u64,
            _cancel: CancellationToken,
        ) -> BoxFuture<'static, Result<HistoryPage, E>> {
            Box::pin(async { Err(E::UnsupportedHistory) })
        }
    }
    struct TestLease {
        error: Option<E>,
        calls: AtomicUsize,
        protected: Arc<AtomicBool>,
    }
    impl CitationLease for TestLease {
        fn renew(
            &self,
            objects: Vec<(ObjectId, String)>,
            now: u64,
            _cancel: CancellationToken,
        ) -> BoxFuture<'static, Result<(), E>> {
            assert_eq!(objects.len(), 1);
            assert_eq!(now, 100);
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.protected.store(true, Ordering::SeqCst);
            let error = self.error;
            Box::pin(async move { error.map_or(Ok(()), Err) })
        }
    }
    struct TestSearch {
        capture: Capture,
        protected: Arc<AtomicBool>,
    }
    impl SearchBackend for TestSearch {
        fn search(
            &self,
            _mode: SearchMode,
            _request: SearchRequest,
            _budget: SearchBudget,
            _cancel: CancellationToken,
        ) -> BoxFuture<'static, Result<SearchPage, E>> {
            Box::pin(async { panic!("citations must use the protected search path") })
        }
        fn search_citable(
            &self,
            request: SearchRequest,
            _budget: SearchBudget,
            _cancel: CancellationToken,
        ) -> BoxFuture<'static, Result<SearchPage, E>> {
            assert_eq!(request.query, "in:body:민법");
            assert!(!request.include_history && !request.include_ocr && request.cursor.is_none());
            self.protected.store(true, Ordering::SeqCst);
            let capture = self.capture.clone();
            Box::pin(async move {
                Ok(SearchPage {
                    schema_version: 1,
                    hits: vec![SearchHit {
                        match_scope: "object".into(),
                        excerpt_section: "body".into(),
                        includes_ocr: false,
                        object: capture.record.object,
                        revision_id: capture.record.revision_id,
                        capture_id: capture.capture_id,
                        title: capture.record.title,
                        section: "object".into(),
                        line: 0,
                        text: capture.record.body.clone(),
                        byte_start: 0,
                        byte_end: capture.record.body.len(),
                        derived_ocr: false,
                    }],
                    next_cursor: Some("partial-marker".into()),
                    generation: 1,
                    corpus_complete: false,
                    scanned_bytes: 6,
                    analyzer_version: "fixture".into(),
                    index_lag: 1,
                    collection_notices: Vec::new(),
                })
            })
        }
    }
    fn service(
        body_error: Option<E>,
        lease_error: Option<E>,
    ) -> (CitationService, Arc<TestLease>, CitationId) {
        service_for_capture(capture("민법".into()), body_error, lease_error)
    }
    fn service_for_capture(
        capture: Capture,
        body_error: Option<E>,
        lease_error: Option<E>,
    ) -> (CitationService, Arc<TestLease>, CitationId) {
        let id = CitationId {
            object: capture.record.object.clone(),
            capture_id: capture.capture_id.clone(),
            projection: CitationProjection::Document,
        };
        let clock: Arc<dyn Clock> = Arc::new(TestClock);
        let protected = Arc::new(AtomicBool::new(false));
        let database = Arc::new(DatabaseService::new(
            Arc::new(TestStore {
                capture: capture.clone(),
                body_error,
                protected: protected.clone(),
            }),
            clock.clone(),
        ));
        let lease = Arc::new(TestLease {
            error: lease_error,
            calls: AtomicUsize::new(0),
            protected: protected.clone(),
        });
        let search = Arc::new(SearchService::new(Arc::new(TestSearch {
            capture,
            protected,
        })));
        (
            CitationService::new(
                database,
                search,
                lease.clone(),
                clock,
                "https://references.example.test".into(),
            )
            .unwrap(),
            lease,
            id,
        )
    }
    #[tokio::test]
    async fn fetch_uses_exact_capture_and_renews_before_read() {
        let (service, lease, id) = service(None, None);
        let result = service
            .fetch(&id.encode().unwrap(), CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(result.text, "민법");
        assert_eq!(result.id, id.encode().unwrap());
        assert!(
            result
                .url
                .starts_with("https://references.example.test/source/v1/")
        );
        assert_eq!(lease.calls.load(Ordering::SeqCst), 1);
        assert_eq!(result.metadata["capture_id"], id.capture_id);
    }
    #[tokio::test]
    async fn expired_source_keeps_metadata_without_fallback_or_text() {
        let (service, _, id) = service(None, Some(E::RevisionUnavailable));
        let source = service
            .source(&id.encode().unwrap(), CancellationToken::new())
            .await
            .unwrap();
        assert!(source.unavailable && source.document.is_none());
        assert_eq!(source.metadata.capture_id, id.capture_id);
        assert_eq!(source.descriptor.body_available, Some(false));
        assert_eq!(
            service
                .fetch(&id.encode().unwrap(), CancellationToken::new())
                .await
                .unwrap_err(),
            E::RevisionUnavailable
        );
        let (service, _, id) = self::service(None, Some(E::Withdrawn));
        assert_eq!(
            service
                .source(&id.encode().unwrap(), CancellationToken::new())
                .await
                .unwrap_err(),
            E::Withdrawn
        );
    }
    #[tokio::test]
    async fn metadata_reads_and_precanceled_fetch_do_not_create_leases() {
        let (service, lease, mut id) = service(None, None);
        id.projection = CitationProjection::Metadata;
        let source = service
            .source(&id.encode().unwrap(), CancellationToken::new())
            .await
            .unwrap();
        assert!(!source.unavailable && source.document.is_none());
        assert_eq!(lease.calls.load(Ordering::SeqCst), 0);
        id.projection = CitationProjection::Document;
        let cancel = CancellationToken::new();
        cancel.cancel();
        assert_eq!(
            service
                .fetch(&id.encode().unwrap(), cancel)
                .await
                .unwrap_err(),
            E::Cancelled
        );
        assert_eq!(lease.calls.load(Ordering::SeqCst), 0);
    }
    #[tokio::test]
    async fn search_uses_protected_path_and_keeps_qualifications_separate() {
        let (service, lease, _) = service(None, None);
        let search = service
            .search(
                "in:body:민법".into(),
                Instant::now() + Duration::from_secs(10),
                CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(search.results.len(), 1);
        assert!(search.partial && search.warnings.len() == 3);
        assert_eq!(lease.calls.load(Ordering::SeqCst), 0);
        let fetched = service
            .fetch(&search.results[0].id, CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(fetched.text, "민법");
        assert_eq!(fetched.url, search.results[0].url);
    }
    #[tokio::test]
    async fn browser_metadata_pages_open_body_but_mcp_metadata_does_not() {
        let (service, lease, mut id) =
            service_for_capture(capture("대한민국\n".repeat(3000)), None, None);
        assert_eq!(
            service
                .fetch(&id.encode().unwrap(), CancellationToken::new())
                .await
                .unwrap_err(),
            E::Capacity
        );
        id.projection = CitationProjection::Metadata;
        let before = lease.calls.load(Ordering::SeqCst);
        let resource = service
            .source(&id.encode().unwrap(), CancellationToken::new())
            .await
            .unwrap();
        assert!(resource.document.is_none() && resource.descriptor.body_available.is_none());
        assert_eq!(lease.calls.load(Ordering::SeqCst), before);
        let page = service
            .page(&id.encode().unwrap(), CancellationToken::new())
            .await
            .unwrap();
        let document = page.document.unwrap();
        assert_eq!(page.descriptor.id, id.encode().unwrap());
        assert!(document.text.len() <= MAX_CITATION_TEXT_BYTES);
        assert!(page.next.is_some());
        assert_eq!(
            service
                .fetch(&document.id, CancellationToken::new())
                .await
                .unwrap()
                .text,
            document.text
        );
    }
    #[test]
    fn search_prefers_complete_provider_article() {
        let mut capture = capture("x".repeat(10000));
        let article_text = "제750조 손해배상";
        capture
            .record
            .body
            .replace_range(5000..5000 + article_text.len(), article_text);
        capture
            .record
            .sections
            .push(openlegal_domain::legal::LegalSection {
                id: "article:750".into(),
                title: "손해배상".into(),
                text: article_text.into(),
                kind: SectionKind::ProviderText,
                source_document_sha256: None,
                page: None,
            });
        let hit = SearchHit {
            match_scope: "object".into(),
            excerpt_section: "body".into(),
            includes_ocr: false,
            object: capture.record.object.clone(),
            revision_id: capture.record.revision_id.clone(),
            capture_id: capture.capture_id.clone(),
            title: capture.record.title.clone(),
            section: "object".into(),
            line: 0,
            text: article_text.into(),
            byte_start: 5000,
            byte_end: 5000 + article_text.len(),
            derived_ocr: false,
        };
        let id = projection_for_hit(&capture, &hit).unwrap();
        assert_eq!(
            id.projection,
            CitationProjection::Section {
                section: "article:750".into()
            }
        );
        assert_eq!(project_text(&capture, &id).unwrap(), article_text);
    }
    fn source_hit(capture: &Capture, section: &str, start: usize, end: usize) -> SearchHit {
        SearchHit {
            match_scope: "object".into(),
            excerpt_section: section.into(),
            includes_ocr: false,
            object: capture.record.object.clone(),
            revision_id: capture.record.revision_id.clone(),
            capture_id: capture.capture_id.clone(),
            title: capture.record.title.clone(),
            section: "object".into(),
            line: 0,
            text: section_text(capture, section).unwrap()[start..end].into(),
            byte_start: start,
            byte_end: end,
            derived_ocr: false,
        }
    }
    #[tokio::test]
    async fn cross_article_context_fetch_preserves_supporting_evidence() {
        let first = "Article A background text.";
        let second = "Article B needle supporting evidence.";
        let body = format!(
            "{}\n{first}\n{second}\n{}",
            "x\n".repeat(2500),
            "x\n".repeat(2500)
        );
        let mut capture = capture(body);
        for (id, text) in [("article:A", first), ("article:B", second)] {
            capture
                .record
                .sections
                .push(openlegal_domain::legal::LegalSection {
                    id: id.into(),
                    title: id.into(),
                    text: text.into(),
                    kind: SectionKind::ProviderText,
                    source_document_sha256: None,
                    page: None,
                });
        }
        let start = capture.record.body.find(first).unwrap() + "Article A ".len();
        let end = capture.record.body.find(second).unwrap() + second.len();
        let hit = source_hit(&capture, "body", start, end);
        let id = projection_for_hit(&capture, &hit).unwrap();
        assert!(matches!(id.projection, CitationProjection::Passage { .. }));
        let (service, _, _) = service_for_capture(capture, None, None);
        let fetched = service
            .fetch(&id.encode().unwrap(), CancellationToken::new())
            .await
            .unwrap();
        assert!(fetched.text.contains(&hit.text));
        assert!(fetched.text.contains("needle supporting evidence"));
    }
    #[test]
    fn field_projections_and_original_hit_bounds_preserve_exact_evidence() {
        let mut capture = capture("A different short body".into());
        capture
            .record
            .metadata
            .insert("case_number".into(), "2026 fictional 42".into());
        for section in ["title", "case_number"] {
            let hit = source_hit(
                &capture,
                section,
                0,
                section_text(&capture, section).unwrap().len(),
            );
            let id = projection_for_hit(&capture, &hit).unwrap();
            assert_eq!(
                id.projection,
                CitationProjection::Section {
                    section: section.into()
                }
            );
            assert_eq!(project_text(&capture, &id).unwrap(), hit.text);
            let mut invalid = hit.clone();
            invalid.text.push('x');
            assert_eq!(
                projection_for_hit(&capture, &invalid).unwrap_err(),
                E::InvalidInput
            );
            invalid = hit.clone();
            invalid.byte_end = invalid.byte_start;
            assert_eq!(
                projection_for_hit(&capture, &invalid).unwrap_err(),
                E::InvalidInput
            );
        }
        capture.record.body = "가나다".into();
        let mut hit = source_hit(&capture, "body", 0, capture.record.body.len());
        hit.byte_start = 1;
        assert_eq!(
            projection_for_hit(&capture, &hit).unwrap_err(),
            E::InvalidInput
        );
    }
    #[test]
    fn near_limit_unicode_excerpt_is_preserved_completely() {
        let mut capture = capture(format!(
            "{}{}{}",
            "가".repeat(2000),
            "🙂".repeat(2047),
            "다".repeat(2000)
        ));
        let start = "가".repeat(2000).len();
        let end = start + "🙂".repeat(2047).len();
        let hit = source_hit(&capture, "body", start, end);
        let id = projection_for_hit(&capture, &hit).unwrap();
        let text = project_text(&capture, &id).unwrap();
        assert!(text.len() <= MAX_CITATION_TEXT_BYTES);
        assert!(text.contains(&hit.text));
        capture.record.body = "🙂".repeat(5000);
        let oversized = source_hit(&capture, "body", 0, 8196);
        assert_eq!(
            projection_for_hit(&capture, &oversized).unwrap_err(),
            E::Capacity
        );
    }
    #[tokio::test]
    async fn derived_section_citations_preserve_kind_page_and_evidence() {
        for (kind, expected) in [
            (SectionKind::Ocr, "ocr"),
            (SectionKind::Extracted, "extracted"),
        ] {
            let mut capture = capture("Primary provider body".into());
            capture
                .record
                .sections
                .push(openlegal_domain::legal::LegalSection {
                    id: "attachment:1".into(),
                    title: "Synthetic attachment".into(),
                    text: "Synthetic extracted text".into(),
                    kind: kind.clone(),
                    source_document_sha256: Some("c".repeat(64)),
                    page: Some(7),
                });
            let (service, _, mut id) = service_for_capture(capture, None, None);
            id.projection = CitationProjection::Section {
                section: "attachment:1".into(),
            };
            let document = service
                .fetch(&id.encode().unwrap(), CancellationToken::new())
                .await
                .unwrap();
            assert_eq!(document.metadata["section_kind"], expected);
            assert_eq!(
                document.metadata["derived_ocr"],
                (kind == SectionKind::Ocr).to_string()
            );
            assert_eq!(document.metadata["page"], "7");
            assert_eq!(document.metadata["source_document_sha256"], "c".repeat(64));
        }
        let mut capture = capture("Synthetic OCR text\nprovider text".into());
        capture
            .record
            .sections
            .push(openlegal_domain::legal::LegalSection {
                id: "ocr:1".into(),
                title: "Synthetic OCR".into(),
                text: "Synthetic OCR text".into(),
                kind: SectionKind::Ocr,
                source_document_sha256: Some("c".repeat(64)),
                page: None,
            });
        let (service, _, id) = service_for_capture(capture, None, None);
        let document = service
            .fetch(&id.encode().unwrap(), CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(document.metadata["section_kind"], "mixed");
        assert_eq!(document.metadata["derived_ocr"], "true");
        assert!(
            document.metadata["text_qualification"]
                .contains("must not be represented as wholly original")
        );
    }
}
