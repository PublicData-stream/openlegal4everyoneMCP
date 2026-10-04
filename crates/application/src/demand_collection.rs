//! Bounded demand collection policy shared by serving transports. This service
//! only records work; provider calls belong to collection workers.
use crate::search::SearchMode;
use futures::future::BoxFuture;
use openlegal_domain::{
    collection::{
        CollectionRequest, CollectionSearchMode, CollectionTarget, DemandCollectionState as State,
        DemandCollectionStatus,
    },
    legal::{DatabaseError, Dataset, GetRequest, RevisionSelector},
    legal_search::SearchRequest,
};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

pub trait DemandCollectionStore: Send + Sync + 'static {
    /// Atomically recheck HEAD/query freshness and share active canonical work.
    /// Storage admission failure must never cause direct upstream traffic.
    fn request_demand(
        &self,
        request: CollectionRequest,
        cancel: CancellationToken,
    ) -> BoxFuture<'static, Result<DemandCollectionStatus, DatabaseError>>;
}

pub struct DemandCollectionCoordinator {
    store: Arc<dyn DemandCollectionStore>,
    enabled: bool,
}

impl DemandCollectionCoordinator {
    pub fn new(store: Arc<dyn DemandCollectionStore>, enabled: bool) -> Self {
        Self { store, enabled }
    }

    pub fn enabled(&self) -> bool {
        self.enabled
    }

    pub async fn head(
        &self,
        request: &GetRequest,
        needs_refresh: bool,
        cancel: &CancellationToken,
    ) -> DemandCollectionStatus {
        if !self.enabled {
            return DemandCollectionStatus::new(State::Disabled);
        }
        if !matches!(request.selector, RevisionSelector::Head) {
            return DemandCollectionStatus::reason(State::Unsupported, "historical_selector");
        }
        let target = CollectionRequest {
            target: CollectionTarget::Object {
                object: request.object.clone(),
            },
        };
        if target.validate().is_err() {
            return DemandCollectionStatus::reason(State::Unsupported, "unsupported_object");
        }
        if !needs_refresh {
            return DemandCollectionStatus::new(State::Fresh);
        }
        self.request(target, cancel).await
    }

    pub async fn search(
        &self,
        request: &SearchRequest,
        mode: SearchMode,
        collection_term: Option<&str>,
        cancel: &CancellationToken,
    ) -> Result<DemandCollectionStatus, DatabaseError> {
        // An explicit term is a separate, provider-bounded literal candidate
        // search. Validate even when collection is disabled or continuation-only.
        validate_collection_term(collection_term, &request.filters.datasets)?;
        if !self.enabled {
            return Ok(DemandCollectionStatus::new(State::Disabled));
        }
        if request.cursor.is_some() {
            return Ok(DemandCollectionStatus::reason(
                State::Unsupported,
                "continuation",
            ));
        }
        let target = if let Some(term) = collection_term {
            search_target(
                term,
                &request.filters.datasets,
                CollectionSearchMode::Literal,
            )
        } else {
            let filters = &request.filters;
            if request.include_history
                || request.include_ocr
                || !request.sections.is_empty()
                || filters.authority.is_some()
                || filters.object_id.is_some()
                || filters.document_type.is_some()
                || filters.date_kind.is_some()
                || filters.date_from.is_some()
                || filters.date_to.is_some()
                || (matches!(mode, SearchMode::Ripgrep) && !request.literal)
            {
                return Ok(DemandCollectionStatus::reason(
                    State::Unsupported,
                    "unsupported_search",
                ));
            }
            let mode = if request.literal {
                CollectionSearchMode::Literal
            } else {
                CollectionSearchMode::Query
            };
            let target = search_target(&request.query, &filters.datasets, mode);
            if target.validate().is_err() {
                return Ok(DemandCollectionStatus::reason(
                    State::Unsupported,
                    "unsupported_search",
                ));
            }
            target
        };
        Ok(self.request(target, cancel).await)
    }

    pub async fn term(
        &self,
        term: &str,
        datasets: &[Dataset],
        cancel: &CancellationToken,
    ) -> DemandCollectionStatus {
        if !self.enabled {
            return DemandCollectionStatus::new(State::Disabled);
        }
        let target = search_target(term, datasets, CollectionSearchMode::Literal);
        if target.validate().is_err() {
            return DemandCollectionStatus::reason(State::Unsupported, "unsupported_term");
        }
        self.request(target, cancel).await
    }

    async fn request(
        &self,
        request: CollectionRequest,
        cancel: &CancellationToken,
    ) -> DemandCollectionStatus {
        if cancel.is_cancelled() {
            return unavailable(DatabaseError::Cancelled);
        }
        self.store
            .request_demand(request.normalized(), cancel.clone())
            .await
            .unwrap_or_else(unavailable)
    }
}

pub fn validate_collection_term(
    term: Option<&str>,
    datasets: &[Dataset],
) -> Result<(), DatabaseError> {
    if let Some(term) = term {
        search_target(term, datasets, CollectionSearchMode::Literal).validate()?;
    }
    Ok(())
}

fn search_target(
    term: &str,
    datasets: &[Dataset],
    mode: CollectionSearchMode,
) -> CollectionRequest {
    CollectionRequest {
        target: CollectionTarget::Search {
            mode,
            term: term.into(),
            datasets: datasets.to_vec(),
        },
    }
}
fn unavailable(error: DatabaseError) -> DemandCollectionStatus {
    let reason = serde_json::to_value(error)
        .ok()
        .and_then(|value| value.as_str().map(str::to_owned))
        .unwrap_or_else(|| "unavailable".into());
    DemandCollectionStatus::reason(State::Unavailable, reason)
}

#[cfg(test)]
mod tests {
    use super::*;
    use openlegal_domain::legal_search::Filters;
    use std::sync::atomic::{AtomicUsize, Ordering};
    #[derive(Default)]
    struct Store(AtomicUsize);
    impl DemandCollectionStore for Store {
        fn request_demand(
            &self,
            _: CollectionRequest,
            _: CancellationToken,
        ) -> BoxFuture<'static, Result<DemandCollectionStatus, DatabaseError>> {
            self.0.fetch_add(1, Ordering::Relaxed);
            Box::pin(async { Ok(DemandCollectionStatus::new(State::Pending)) })
        }
    }
    fn request(query: &str) -> SearchRequest {
        SearchRequest {
            query: query.into(),
            filters: Filters::default(),
            include_history: false,
            include_ocr: false,
            sections: vec![],
            limit: 20,
            cursor: None,
            literal: false,
            ignore_case: false,
            context_lines: 0,
        }
    }
    #[tokio::test]
    async fn implicit_search_never_discards_semantics_and_override_is_explicit() {
        let store = Arc::new(Store::default());
        let coordinator = DemandCollectionCoordinator::new(store.clone(), true);
        let cancel = CancellationToken::new();
        for term in ["in:title:민법", "민법 OR 형법", "민.*", "\"민법\""] {
            assert_eq!(
                coordinator
                    .search(&request(term), SearchMode::Query, None, &cancel)
                    .await
                    .unwrap()
                    .status,
                State::Unsupported
            );
        }
        let mut input = request("민법");
        input.filters.authority = Some("test".into());
        assert_eq!(
            coordinator
                .search(&input, SearchMode::Query, None, &cancel)
                .await
                .unwrap()
                .status,
            State::Unsupported
        );
        assert_eq!(
            coordinator
                .search(&input, SearchMode::Query, Some("민법"), &cancel)
                .await
                .unwrap()
                .status,
            State::Pending
        );
        input.cursor = Some("continuation".into());
        assert_eq!(
            coordinator
                .search(&input, SearchMode::Query, Some("민법"), &cancel)
                .await
                .unwrap()
                .status,
            State::Unsupported
        );
        assert_eq!(store.0.load(Ordering::Relaxed), 1);
        assert!(
            coordinator
                .search(&input, SearchMode::Query, Some("민.*"), &cancel)
                .await
                .is_err()
        );
    }
    #[tokio::test]
    async fn disabled_and_precancelled_requests_do_not_enqueue() {
        let store = Arc::new(Store::default());
        let cancel = CancellationToken::new();
        let disabled = DemandCollectionCoordinator::new(store.clone(), false);
        assert_eq!(
            disabled.term("민법", &[], &cancel).await.status,
            State::Disabled
        );
        cancel.cancel();
        let enabled = DemandCollectionCoordinator::new(store.clone(), true);
        assert_eq!(
            enabled.term("민법", &[], &cancel).await.status,
            State::Unavailable
        );
        assert_eq!(store.0.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn only_supported_stale_head_requests_reach_storage() {
        use openlegal_domain::legal::ObjectId;
        let store = Arc::new(Store::default());
        let coordinator = DemandCollectionCoordinator::new(store.clone(), true);
        let cancel = CancellationToken::new();
        let mut input = GetRequest {
            object: ObjectId {
                jurisdiction: "kr".into(),
                provider: "law_go_kr".into(),
                dataset: Dataset::NationalStatute,
                id: "123".into(),
            },
            selector: RevisionSelector::Head,
            fresh_only: true,
        };
        assert_eq!(
            coordinator.head(&input, false, &cancel).await.status,
            State::Fresh
        );
        assert_eq!(store.0.load(Ordering::Relaxed), 0);
        assert_eq!(
            coordinator.head(&input, true, &cancel).await.status,
            State::Pending
        );
        input.selector = RevisionSelector::Capture { id: "a".repeat(64) };
        assert_eq!(
            coordinator.head(&input, true, &cancel).await.status,
            State::Unsupported
        );
        input.selector = RevisionSelector::Revision { id: "r1".into() };
        assert_eq!(
            coordinator.head(&input, true, &cancel).await.status,
            State::Unsupported
        );
        input.selector = RevisionSelector::Head;
        input.object.dataset = Dataset::Treaty;
        assert_eq!(
            coordinator.head(&input, true, &cancel).await.status,
            State::Unsupported
        );
        let disabled = DemandCollectionCoordinator::new(store.clone(), false);
        assert_eq!(
            disabled.head(&input, true, &cancel).await.status,
            State::Disabled
        );
        assert_eq!(store.0.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn explicit_override_maps_only_its_term_and_datasets() {
        use std::sync::Mutex;
        struct Recording(Mutex<Option<CollectionRequest>>);
        impl DemandCollectionStore for Recording {
            fn request_demand(
                &self,
                request: CollectionRequest,
                _: CancellationToken,
            ) -> BoxFuture<'static, Result<DemandCollectionStatus, DatabaseError>> {
                *self.0.lock().unwrap() = Some(request);
                Box::pin(async { Ok(DemandCollectionStatus::new(State::Pending)) })
            }
        }
        let store = Arc::new(Recording(Mutex::new(None)));
        let coordinator = DemandCollectionCoordinator::new(store.clone(), true);
        let mut input = request("in:title:민법 AND NOT in:body:계약");
        input.filters.datasets = vec![Dataset::NationalStatute];
        input.filters.date_kind = Some(openlegal_domain::legal_search::DateKind::Effective);
        input.filters.date_from = Some("20260101".into());
        coordinator
            .search(
                &input,
                SearchMode::Query,
                Some("민법"),
                &CancellationToken::new(),
            )
            .await
            .unwrap();
        let stored = store.0.lock().unwrap().take().unwrap();
        let CollectionTarget::Search {
            mode,
            term,
            datasets,
        } = stored.target
        else {
            panic!("expected search target")
        };
        assert!(matches!(mode, CollectionSearchMode::Literal));
        assert_eq!(term, "민법");
        assert_eq!(datasets, [Dataset::NationalStatute]);
    }

    #[tokio::test]
    async fn invalid_explicit_term_is_rejected_even_disabled_and_regex_never_maps_implicitly() {
        let store = Arc::new(Store::default());
        let disabled = DemandCollectionCoordinator::new(store.clone(), false);
        let cancel = CancellationToken::new();
        assert!(
            disabled
                .search(
                    &request("valid"),
                    SearchMode::Query,
                    Some("invalid.*"),
                    &cancel
                )
                .await
                .is_err()
        );
        let enabled = DemandCollectionCoordinator::new(store.clone(), true);
        assert_eq!(
            enabled
                .search(&request("민법"), SearchMode::Ripgrep, None, &cancel)
                .await
                .unwrap()
                .status,
            State::Unsupported
        );
        assert_eq!(store.0.load(Ordering::Relaxed), 0);
        let mut literal = request("민법");
        literal.literal = true;
        assert_eq!(
            enabled
                .search(&literal, SearchMode::Ripgrep, None, &cancel)
                .await
                .unwrap()
                .status,
            State::Pending
        );
        assert_eq!(store.0.load(Ordering::Relaxed), 1);
    }
}
