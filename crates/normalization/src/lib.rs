//! Pure, bounded processors for two explicitly synthetic JSON representations.

use openlegal_domain::{
    Query, Record, RetrievalData, RetrievalError, SearchPage, valid_identifier,
};
use serde::Deserialize;

pub const MAX_PAYLOAD_BYTES: usize = 1024 * 1024;
pub const MAX_PROCESSED_BYTES: usize = 64 * 1024;
const MAX_STRING_BYTES: usize = 16 * 1024;

/// Explicit, deterministic work accounting supplied to trusted processors.
/// It is not a sandbox: implementations must charge work and bound allocations.
pub struct WorkBudget {
    remaining: usize,
}
impl Default for WorkBudget {
    fn default() -> Self {
        Self {
            remaining: 2 * MAX_PAYLOAD_BYTES,
        }
    }
}
impl WorkBudget {
    pub fn spend(&mut self, amount: usize) -> Result<(), RetrievalError> {
        self.remaining = self
            .remaining
            .checked_sub(amount)
            .ok_or(RetrievalError::ResourceLimit)?;
        Ok(())
    }
}

pub trait PayloadProcessor: Send + Sync + 'static {
    fn version(&self) -> &'static str;
    fn process(
        &self,
        bytes: &[u8],
        query: &Query,
        budget: &mut WorkBudget,
    ) -> Result<RetrievalData, RetrievalError>;
}

pub struct LayoutAProcessor;
pub struct LayoutBProcessor;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ARecord {
    id: String,
    title: String,
    body: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ASearch {
    kind: String,
    items: Vec<ARecord>,
    page: u32,
    page_size: u32,
    total: u32,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AGet {
    kind: String,
    item: ARecord,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BRecord {
    key: String,
    label: String,
    text: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Pagination {
    index: u32,
    size: u32,
    count: u32,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BSearch {
    format: String,
    results: Vec<BRecord>,
    pagination: Pagination,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BGet {
    format: String,
    document: BRecord,
}

fn record(
    query: &Query,
    id: String,
    title: String,
    body: String,
) -> Result<Record, RetrievalError> {
    if !valid_identifier(&id, 128)
        || title.is_empty()
        || title.len() > 1024
        || body.len() > MAX_STRING_BYTES
    {
        return Err(RetrievalError::NormalizationFailed);
    }
    Ok(Record {
        source: query.source().to_owned(),
        id,
        title,
        body,
        synthetic: true,
    })
}

fn search(
    query: &Query,
    records: Vec<Record>,
    page: u32,
    page_size: u32,
    total: u32,
) -> Result<RetrievalData, RetrievalError> {
    let Query::Search {
        page: requested_page,
        page_size: requested_size,
        ..
    } = query
    else {
        return Err(RetrievalError::NormalizationFailed);
    };
    if page != *requested_page
        || page_size != *requested_size
        || records.len() > page_size as usize
        || total > 20_000
        || records.len() as u64
            != u64::from(page_size)
                .min(u64::from(total).saturating_sub(u64::from(page) * u64::from(page_size)))
    {
        return Err(RetrievalError::NormalizationFailed);
    }
    let mut ids = std::collections::HashSet::new();
    if records.iter().any(|r| !ids.insert(&r.id)) {
        return Err(RetrievalError::NormalizationFailed);
    }
    Ok(RetrievalData::Search(SearchPage {
        records,
        page,
        page_size,
        total,
    }))
}

fn finish(query: &Query, output: RetrievalData) -> Result<RetrievalData, RetrievalError> {
    if let (Query::Get { id, .. }, RetrievalData::Get(record)) = (query, &output)
        && id != &record.id
    {
        return Err(RetrievalError::NormalizationFailed);
    }
    // Typed input construction is bounded independently; counting avoids a second result allocation.
    struct Counter(usize);
    impl std::io::Write for Counter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0 = self
                .0
                .checked_sub(bytes.len())
                .ok_or_else(|| std::io::Error::other("output limit"))?;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    serde_json::to_writer(Counter(MAX_PROCESSED_BYTES), &output)
        .map_err(|_| RetrievalError::ResourceLimit)?;
    Ok(output)
}

/// Scan resource limits before Serde allocates an object graph. Syntax is still
/// checked by Serde; this scan only rejects excessive depth, strings, work or collections.
fn preflight(bytes: &[u8], budget: &mut WorkBudget) -> Result<(), RetrievalError> {
    if bytes.len() > MAX_PAYLOAD_BYTES {
        return Err(RetrievalError::ResourceLimit);
    }
    let mut stack: Vec<usize> = Vec::new();
    let (mut in_string, mut escaped, mut length) = (false, false, 0usize);
    for &byte in bytes {
        budget.spend(1)?;
        if in_string {
            length += 1;
            if length > MAX_STRING_BYTES {
                return Err(RetrievalError::ResourceLimit);
            }
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                in_string = false;
            }
            continue;
        }
        match byte {
            b'"' => {
                in_string = true;
                length = 0;
            }
            b'{' | b'[' => {
                if stack.len() >= 32 {
                    return Err(RetrievalError::ResourceLimit);
                }
                stack.push(1);
            }
            b'}' | b']' => {
                stack.pop();
            }
            b',' => {
                if let Some(count) = stack.last_mut() {
                    *count += 1;
                    if *count > 1000 {
                        return Err(RetrievalError::ResourceLimit);
                    }
                }
            }
            _ => {}
        }
    }
    Ok(())
}

fn decode<T: for<'de> Deserialize<'de>>(bytes: &[u8]) -> Result<T, RetrievalError> {
    serde_json::from_slice(bytes).map_err(|_| RetrievalError::NormalizationFailed)
}

impl PayloadProcessor for LayoutAProcessor {
    fn version(&self) -> &'static str {
        "synthetic-layout-a-v1"
    }
    fn process(
        &self,
        bytes: &[u8],
        query: &Query,
        budget: &mut WorkBudget,
    ) -> Result<RetrievalData, RetrievalError> {
        query.validate()?;
        preflight(bytes, budget)?;
        budget.spend(bytes.len())?; // Account for the bounded decoding pass as well as the lexical scan.
        let result = match query {
            Query::Search { .. } => {
                let parsed: ASearch = decode(bytes)?;
                if parsed.kind != "synthetic-layout-a" {
                    return Err(RetrievalError::NormalizationFailed);
                }
                let records = parsed
                    .items
                    .into_iter()
                    .map(|r| record(query, r.id, r.title, r.body))
                    .collect::<Result<Vec<_>, _>>()?;
                search(query, records, parsed.page, parsed.page_size, parsed.total)?
            }
            Query::Get { .. } => {
                let parsed: AGet = decode(bytes)?;
                if parsed.kind != "synthetic-layout-a" {
                    return Err(RetrievalError::NormalizationFailed);
                }
                RetrievalData::Get(record(
                    query,
                    parsed.item.id,
                    parsed.item.title,
                    parsed.item.body,
                )?)
            }
        };
        finish(query, result)
    }
}

impl PayloadProcessor for LayoutBProcessor {
    fn version(&self) -> &'static str {
        "synthetic-layout-b-v1"
    }
    fn process(
        &self,
        bytes: &[u8],
        query: &Query,
        budget: &mut WorkBudget,
    ) -> Result<RetrievalData, RetrievalError> {
        query.validate()?;
        preflight(bytes, budget)?;
        budget.spend(bytes.len())?; // Account for the bounded decoding pass as well as the lexical scan.
        let result = match query {
            Query::Search { .. } => {
                let parsed: BSearch = decode(bytes)?;
                if parsed.format != "synthetic-layout-b" {
                    return Err(RetrievalError::NormalizationFailed);
                }
                let records = parsed
                    .results
                    .into_iter()
                    .map(|r| record(query, r.key, r.label, r.text))
                    .collect::<Result<Vec<_>, _>>()?;
                search(
                    query,
                    records,
                    parsed.pagination.index,
                    parsed.pagination.size,
                    parsed.pagination.count,
                )?
            }
            Query::Get { .. } => {
                let parsed: BGet = decode(bytes)?;
                if parsed.format != "synthetic-layout-b" {
                    return Err(RetrievalError::NormalizationFailed);
                }
                RetrievalData::Get(record(
                    query,
                    parsed.document.key,
                    parsed.document.label,
                    parsed.document.text,
                )?)
            }
        };
        finish(query, result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn query() -> Query {
        Query::Get {
            source: "layout_a".into(),
            id: "001".into(),
        }
    }
    #[test]
    fn preserves_identity_and_maps_distinct_layouts() {
        let a = br#"{"kind":"synthetic-layout-a","item":{"id":"001","title":"Synthetic","body":"Example"}}"#;
        let b = br#"{"format":"synthetic-layout-b","document":{"key":"001","label":"Synthetic","text":"Example"}}"#;
        assert_eq!(
            LayoutAProcessor
                .process(a, &query(), &mut WorkBudget::default())
                .unwrap(),
            LayoutBProcessor
                .process(b, &query(), &mut WorkBudget::default())
                .unwrap()
        );
        let mismatch = Query::Get {
            source: "layout_a".into(),
            id: "1".into(),
        };
        assert_eq!(
            LayoutAProcessor.process(a, &mismatch, &mut WorkBudget::default()),
            Err(RetrievalError::NormalizationFailed)
        );
    }
    #[test]
    fn rejects_duplicate_unknown_missing_fields_and_excessive_work() {
        for bytes in [br#"{"kind":"synthetic-layout-a","item":{"id":"001","id":"001","title":"A","body":"B"}}"#.as_slice(), br#"{"kind":"synthetic-layout-a","item":{"id":"001","title":"A"}}"#, br#"{"kind":"synthetic-layout-a","item":{"id":"001","title":"A","body":"B","url":"http://example.test"}}"#] {
            assert!(LayoutAProcessor.process(bytes, &query(), &mut WorkBudget::default()).is_err());
        }
        let nested = "[".repeat(33) + &"]".repeat(33);
        assert_eq!(
            preflight(nested.as_bytes(), &mut WorkBudget::default()),
            Err(RetrievalError::ResourceLimit)
        );
        let oversized_string = format!("\"{}\"", "a".repeat(MAX_STRING_BYTES + 1));
        assert_eq!(
            preflight(oversized_string.as_bytes(), &mut WorkBudget::default()),
            Err(RetrievalError::ResourceLimit)
        );
        assert_eq!(
            preflight(b"{}", &mut WorkBudget { remaining: 1 }),
            Err(RetrievalError::ResourceLimit)
        );
    }
    #[test]
    fn search_requires_requested_page_consistent_total_and_unique_ids() {
        let query = Query::Search {
            source: "layout_a".into(),
            query: "Synthetic".into(),
            page: 0,
            page_size: 5,
        };
        let base = serde_json::json!({"kind":"synthetic-layout-a","items":[{"id":"001","title":"Synthetic","body":"Example"}],"page":0,"page_size":5,"total":1});
        let valid = LayoutAProcessor
            .process(
                &serde_json::to_vec(&base).unwrap(),
                &query,
                &mut WorkBudget::default(),
            )
            .unwrap();
        assert!(matches!(
            valid,
            RetrievalData::Search(SearchPage { total: 1, .. })
        ));
        for (field, value) in [("page", 1), ("page_size", 4), ("total", 0)] {
            let mut invalid = base.clone();
            invalid[field] = value.into();
            assert_eq!(
                LayoutAProcessor.process(
                    &serde_json::to_vec(&invalid).unwrap(),
                    &query,
                    &mut WorkBudget::default()
                ),
                Err(RetrievalError::NormalizationFailed)
            );
        }
        let mut duplicate = base.clone();
        duplicate["items"]
            .as_array_mut()
            .unwrap()
            .push(base["items"][0].clone());
        duplicate["total"] = 2.into();
        assert_eq!(
            LayoutAProcessor.process(
                &serde_json::to_vec(&duplicate).unwrap(),
                &query,
                &mut WorkBudget::default()
            ),
            Err(RetrievalError::NormalizationFailed)
        );
        let b = br#"{"format":"synthetic-layout-b","results":[{"key":"001","label":"Synthetic","text":"Example"}],"pagination":{"index":0,"size":5,"count":1}}"#;
        assert_eq!(
            LayoutBProcessor
                .process(b, &query, &mut WorkBudget::default())
                .unwrap(),
            valid
        );
    }

    #[test]
    fn raw_collection_and_processed_output_bounds_are_independent() {
        assert_eq!(
            preflight(
                &vec![b' '; MAX_PAYLOAD_BYTES + 1],
                &mut WorkBudget::default()
            ),
            Err(RetrievalError::ResourceLimit)
        );
        let array = format!("[{}]", vec!["0"; 1001].join(","));
        assert_eq!(
            preflight(array.as_bytes(), &mut WorkBudget::default()),
            Err(RetrievalError::ResourceLimit)
        );
        let query = Query::Search {
            source: "layout_a".into(),
            query: String::new(),
            page: 0,
            page_size: 20,
        };
        let items: Vec<_> = (0..20).map(|id| serde_json::json!({"id":id.to_string(),"title":"Synthetic","body":"a".repeat(4096)})).collect();
        let bytes = serde_json::to_vec(&serde_json::json!({"kind":"synthetic-layout-a","items":items,"page":0,"page_size":20,"total":20})).unwrap();
        assert!(bytes.len() < MAX_PAYLOAD_BYTES);
        assert_eq!(
            LayoutAProcessor.process(&bytes, &query, &mut WorkBudget::default()),
            Err(RetrievalError::ResourceLimit)
        );
    }

    #[test]
    fn synthetic_pages_are_complete_including_final_and_out_of_range_pages() {
        for (page, total, count, valid) in [
            (0, 10, 0, false),
            (0, 10, 1, false),
            (1, 6, 1, true),
            (2, 6, 0, true),
            (0, 0, 0, true),
        ] {
            let query = Query::Search {
                source: "layout_a".into(),
                query: String::new(),
                page,
                page_size: 5,
            };
            let items: Vec<_> = (0..count).map(|id| serde_json::json!({"id":id.to_string(),"title":"Synthetic","body":"Example"})).collect();
            let bytes = serde_json::to_vec(&serde_json::json!({"kind":"synthetic-layout-a","items":items,"page":page,"page_size":5,"total":total})).unwrap();
            assert_eq!(
                LayoutAProcessor
                    .process(&bytes, &query, &mut WorkBudget::default())
                    .is_ok(),
                valid,
                "page={page},total={total},count={count}"
            );
        }
    }
}
