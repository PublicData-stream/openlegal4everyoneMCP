//! Legal corpus policy, independent of transport, provider and storage mechanics.
use crate::Clock;
use futures::future::BoxFuture;
use openlegal_domain::{FreshnessState, legal::*};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

pub const FRESH_SECONDS: u64 = 3600;
pub const MAX_STALE_SECONDS: u64 = 86400;
pub const MAX_SOURCE_BYTES: usize = 100 * 1024 * 1024;
pub const SESSION_SECONDS: u64 = 600;
pub const HISTORY_RETENTION_SECONDS: u64 = 30 * 86400;

pub trait DatabaseStore: Send + Sync + 'static {
    /// Must verify retained raw evidence as well as the processed representation.
    /// HEAD refuses an observed replacement awaiting processing; explicit historical
    /// selectors never fall back to a current representation.
    fn resolve(
        &self,
        object: ObjectId,
        selector: RevisionSelector,
        now: u64,
        cancel: CancellationToken,
    ) -> BoxFuture<'static, Result<Capture, DatabaseError>>;
    /// Retained metadata may outlive raw and normalized bodies. Implementations
    /// must verify its independent checksum and preserve exact selector identity.
    fn resolve_metadata(
        &self,
        object: ObjectId,
        selector: RevisionSelector,
        now: u64,
        cancel: CancellationToken,
    ) -> BoxFuture<'static, Result<MetadataResult, DatabaseError>> {
        let result = self.resolve(object, selector, now, cancel);
        Box::pin(async move {
            result.await.map(|capture| {
                GetResult {
                    capture,
                    freshness: None,
                }
                .into()
            })
        })
    }
    fn history(
        &self,
        object: ObjectId,
        kind: HistoryKind,
        cursor: Option<String>,
        limit: usize,
        now: u64,
        cancel: CancellationToken,
    ) -> BoxFuture<'static, Result<HistoryPage, DatabaseError>>;
}

pub struct DatabaseService {
    store: Arc<dyn DatabaseStore>,
    clock: Arc<dyn Clock>,
}
impl DatabaseService {
    pub fn new(store: Arc<dyn DatabaseStore>, clock: Arc<dyn Clock>) -> Self {
        Self { store, clock }
    }
    pub fn store(&self) -> Arc<dyn DatabaseStore> {
        self.store.clone()
    }
    pub async fn get(
        &self,
        request: GetRequest,
        cancel: CancellationToken,
    ) -> Result<GetResult, DatabaseError> {
        request.object.validate()?;
        request.selector.validate()?;
        if cancel.is_cancelled() {
            return Err(DatabaseError::Cancelled);
        }
        let head = matches!(request.selector, RevisionSelector::Head);
        let capture = self
            .store
            .resolve(
                request.object,
                request.selector,
                self.clock.now(),
                cancel.clone(),
            )
            .await?;
        if cancel.is_cancelled() {
            return Err(DatabaseError::Cancelled);
        }
        let now = self.clock.now();
        let freshness = freshness(
            head,
            request.fresh_only,
            capture.validated_at,
            capture.retrieved_at,
            capture.captured_at,
            now,
        )?;
        Ok(GetResult { capture, freshness })
    }
    pub async fn get_metadata(
        &self,
        request: GetRequest,
        cancel: CancellationToken,
    ) -> Result<MetadataResult, DatabaseError> {
        request.object.validate()?;
        request.selector.validate()?;
        if cancel.is_cancelled() {
            return Err(DatabaseError::Cancelled);
        }
        let head = matches!(request.selector, RevisionSelector::Head);
        let mut metadata = self
            .store
            .resolve_metadata(
                request.object,
                request.selector,
                self.clock.now(),
                cancel.clone(),
            )
            .await?;
        if cancel.is_cancelled() {
            return Err(DatabaseError::Cancelled);
        }
        metadata.freshness = freshness(
            head,
            request.fresh_only,
            metadata.validated_at,
            metadata.retrieved_at,
            metadata.captured_at,
            self.clock.now(),
        )?;
        Ok(metadata)
    }
    pub async fn history(
        &self,
        object: ObjectId,
        kind: HistoryKind,
        cursor: Option<String>,
        limit: usize,
        cancel: CancellationToken,
    ) -> Result<HistoryPage, DatabaseError> {
        object.validate()?;
        if !(1..=100).contains(&limit) || cursor.as_ref().is_some_and(|v| v.len() > 512) {
            return Err(DatabaseError::InvalidInput);
        }
        if matches!(kind, HistoryKind::Revisions) && !object.dataset.has_provider_revisions() {
            return Err(DatabaseError::UnsupportedHistory);
        }
        self.store
            .history(object, kind, cursor, limit, self.clock.now(), cancel)
            .await
    }
    /// Inputs resolve once. The caller sends these exact bodies to the comparison
    /// service and carries both capture IDs in comparison provenance.
    pub async fn resolve_diff(
        &self,
        object: ObjectId,
        before: RevisionSelector,
        after: RevisionSelector,
        cancel: CancellationToken,
    ) -> Result<(GetResult, GetResult), DatabaseError> {
        let a = self
            .get(
                GetRequest {
                    object: object.clone(),
                    selector: before,
                    fresh_only: false,
                },
                cancel.clone(),
            )
            .await?;
        let b = self
            .get(
                GetRequest {
                    object,
                    selector: after,
                    fresh_only: false,
                },
                cancel,
            )
            .await?;
        if a.capture.record.object != b.capture.record.object
            || a.capture.record.representation != b.capture.record.representation
        {
            return Err(DatabaseError::InvalidInput);
        }
        Ok((a, b))
    }
}

/// Immutable input to a single atomic corpus publication. Raw bytes are staged
/// durably before metadata publication; expected_version fences obsolete work.
pub struct Publication {
    pub record: LegalRecord,
    pub raw: Vec<u8>,
    pub additional_evidence: Vec<Vec<u8>>,
    pub processor_version: String,
    pub retrieved_at: u64,
    pub now: u64,
    pub expected_version: u64,
    pub install_head: bool,
    pub job_id: Option<String>,
}
#[derive(Clone, Debug)]
pub struct ObjectState {
    pub catalog_version: u64,
    pub version: u64,
    pub head_capture: Option<String>,
    pub pending: bool,
    pub withdrawn: bool,
    pub inventory_complete: bool,
}
#[derive(Clone, Debug)]
pub struct Job {
    pub source_metadata: std::collections::BTreeMap<String, String>,
    pub effective_date: Option<String>,
    pub install_head: bool,
    pub id: String,
    pub object: ObjectId,
    pub revision_id: String,
    pub expected_version: u64,
    pub attempts: u32,
}
#[derive(Clone, Debug)]
pub struct OutboxEntry {
    pub sequence: u64,
    pub object: ObjectId,
    pub capture_id: Option<String>,
    pub object_version: u64,
    pub withdrawn: bool,
    pub install_head: bool,
    pub removed: bool,
}
#[derive(Clone, Debug)]
pub struct CorpusPage {
    pub captures: Vec<Capture>,
    pub next_cursor: Option<String>,
}

fn freshness(
    head: bool,
    fresh_only: bool,
    validated_at: u64,
    retrieved_at: u64,
    captured_at: u64,
    now: u64,
) -> Result<Option<HeadFreshness>, DatabaseError> {
    if head {
        let age = now
            .checked_sub(validated_at)
            .ok_or(DatabaseError::FreshnessUnavailable)?;
        if retrieved_at > now
            || captured_at > now
            || age >= MAX_STALE_SECONDS
            || (fresh_only && age >= FRESH_SECONDS)
        {
            return Err(DatabaseError::FreshnessUnavailable);
        }
        Ok(Some(HeadFreshness {
            state: if age < FRESH_SECONDS {
                FreshnessState::Fresh
            } else {
                FreshnessState::Stale
            },
            served_at: now,
            cached_at: captured_at,
            age_seconds: age,
            fresh_ttl_seconds: FRESH_SECONDS,
            fresh_remaining_seconds: FRESH_SECONDS.saturating_sub(age),
            stale_remaining_seconds: MAX_STALE_SECONDS.saturating_sub(age),
            fresh_expires_at: validated_at.saturating_add(FRESH_SECONDS),
            stale_expires_at: validated_at.saturating_add(MAX_STALE_SECONDS),
        }))
    } else {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    struct Fixed(u64);
    impl Clock for Fixed {
        fn now(&self) -> u64 {
            self.0
        }
    }
    struct Mock;
    impl DatabaseStore for Mock {
        fn resolve(
            &self,
            object: ObjectId,
            _: RevisionSelector,
            _: u64,
            _: CancellationToken,
        ) -> BoxFuture<'static, Result<Capture, DatabaseError>> {
            Box::pin(async move {
                Ok(Capture {
                    capture_id: "a".repeat(64),
                    sequence: 1,
                    record: LegalRecord {
                        object,
                        revision_id: "r1".into(),
                        title: "Title".into(),
                        body: "Body".into(),
                        metadata: BTreeMap::new(),
                        publication_date: None,
                        effective_date: None,
                        source_url: "https://example.test".into(),
                        representation: "text_v1".into(),
                        sections: vec![],
                    },
                    retrieved_at: 100,
                    captured_at: 100,
                    validated_at: 100,
                    processor_version: "v1".into(),
                    raw_sha256: "b".repeat(64),
                })
            })
        }
        fn history(
            &self,
            _: ObjectId,
            _: HistoryKind,
            _: Option<String>,
            _: usize,
            _: u64,
            _: CancellationToken,
        ) -> BoxFuture<'static, Result<HistoryPage, DatabaseError>> {
            Box::pin(async { Err(DatabaseError::NotFound) })
        }
    }
    fn request(selector: RevisionSelector) -> GetRequest {
        GetRequest {
            object: ObjectId {
                jurisdiction: "kr".into(),
                provider: "law_go_kr".into(),
                dataset: Dataset::NationalStatute,
                id: "001".into(),
            },
            selector,
            fresh_only: false,
        }
    }
    #[tokio::test]
    async fn freshness_boundaries_and_historical_metadata() {
        for (now, expected) in [
            (3699, Some(FreshnessState::Fresh)),
            (3700, Some(FreshnessState::Stale)),
            (86500, None),
        ] {
            let service = DatabaseService::new(Arc::new(Mock), Arc::new(Fixed(now)));
            let result = service
                .get(request(RevisionSelector::Head), CancellationToken::new())
                .await;
            match expected {
                Some(s) => assert_eq!(result.unwrap().freshness.unwrap().state, s),
                None => assert!(matches!(result, Err(DatabaseError::FreshnessUnavailable))),
            }
        }
        let service = DatabaseService::new(Arc::new(Mock), Arc::new(Fixed(999999)));
        let result = service
            .get_metadata(
                request(RevisionSelector::Revision { id: "r1".into() }),
                CancellationToken::new(),
            )
            .await
            .unwrap();
        assert!(result.freshness.is_none());
        assert_eq!(result.title, "Title");
    }
}
