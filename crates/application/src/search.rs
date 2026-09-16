//! Application admission policy for corpus searches. Backends receive finite budgets.
use futures::future::BoxFuture;
use openlegal_domain::{
    legal::DatabaseError,
    legal_search::{SearchPage, SearchRequest},
};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
#[derive(Clone, Copy)]
pub enum SearchMode {
    Query,
    Ripgrep,
}
pub struct SearchBudget {
    pub bytes: usize,
    pub deadline: Instant,
    pub lease: tokio::sync::OwnedSemaphorePermit,
}
pub trait SearchBackend: Send + Sync + 'static {
    fn search(
        &self,
        mode: SearchMode,
        request: SearchRequest,
        budget: SearchBudget,
        cancel: CancellationToken,
    ) -> BoxFuture<'static, Result<SearchPage, DatabaseError>>;
}
pub struct SearchService {
    backend: Arc<dyn SearchBackend>,
    slots: Arc<Semaphore>,
}
impl SearchService {
    pub fn new(backend: Arc<dyn SearchBackend>) -> Self {
        Self {
            backend,
            slots: Arc::new(Semaphore::new(2)),
        }
    }
    pub async fn search(
        &self,
        mode: SearchMode,
        request: SearchRequest,
        deadline: Instant,
        cancel: CancellationToken,
    ) -> Result<SearchPage, DatabaseError> {
        validate(&request)?;
        let cancel = cancel.child_token();
        let _guard = cancel.clone().drop_guard();
        let permit = self
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| DatabaseError::Capacity)?;
        let deadline = deadline.min(Instant::now() + Duration::from_secs(10));
        let work = self.backend.search(
            mode,
            request,
            SearchBudget {
                bytes: 64 * 1024 * 1024,
                deadline,
                lease: permit,
            },
            cancel.clone(),
        );
        tokio::select! {
            _ = cancel.cancelled() => Err(DatabaseError::Cancelled),
            _ = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
                Err(DatabaseError::Capacity)
            },
            result = work => result,
        }
    }
}
pub fn validate(r: &SearchRequest) -> Result<(), DatabaseError> {
    if r.query.len() > 4096
        || !(1..=100).contains(&r.limit)
        || r.filters.datasets.len() > 3
        || r.sections.len() > 32
        || r.sections.iter().any(|v| v.is_empty() || v.len() > 256)
        || r.context_lines > 3
        || r.cursor.as_ref().is_some_and(|v| v.len() > 1024)
        || [
            &r.filters.authority,
            &r.filters.object_id,
            &r.filters.document_type,
        ]
        .iter()
        .any(|v| {
            v.as_ref()
                .is_some_and(|s| s.len() > 256 || s.chars().any(char::is_control))
        })
    {
        return Err(DatabaseError::InvalidInput);
    }
    if (r.filters.date_from.is_some() || r.filters.date_to.is_some())
        && r.filters.date_kind.is_none()
    {
        return Err(DatabaseError::InvalidInput);
    }
    for date in [&r.filters.date_from, &r.filters.date_to]
        .into_iter()
        .flatten()
    {
        if !openlegal_domain::legal::valid_date(date) {
            return Err(DatabaseError::InvalidInput);
        }
    }
    if let (Some(a), Some(b)) = (&r.filters.date_from, &r.filters.date_to)
        && a > b
    {
        return Err(DatabaseError::InvalidInput);
    }
    Ok(())
}
