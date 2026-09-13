//! Persistent resolution. The service owns freshness, admission and cancellation.
use super::*;
use crate::persistence::{
    HistoryKey, PersistentKey, PublicationOutcome, PublicationRequest, StorageStatus, StoredResult,
};
use openlegal_domain::history::{SnapshotEnvelope, SnapshotPage, valid_snapshot_id};

const RESOLUTION_DEADLINE: Duration = Duration::from_secs(20);

impl RetrievalService {
    pub fn history_enabled(&self) -> bool {
        self.persistence.is_some()
    }

    pub fn storage_ready(&self) -> bool {
        !self.shutdown.is_cancelled() && self.storage_error().is_none()
    }

    pub(super) fn storage_error(&self) -> Option<RetrievalError> {
        self.persistence.as_ref().and_then(|store| {
            store
                .status()
                .error()
                .or_else(|| (!store.healthy()).then_some(RetrievalError::StorageUnavailable))
        })
    }

    pub(super) fn storage_healthy(&self) -> bool {
        self.storage_error().is_none()
    }

    pub(super) fn sync_epoch(&self, state: &mut State) {
        if let Some(store) = &self.persistence {
            let epoch = store.epoch();
            let recovery_epoch = store.recovery_epoch();
            if recovery_epoch != state.storage_recovery_epoch {
                state.storage_recovery_epoch = recovery_epoch;
                state.cache.clear();
                for flight in state.flights.values() {
                    flight.cancellation.cancel();
                }
            }
            if state.storage_epoch != epoch {
                state.cache.clear();
                state.storage_epoch = epoch;
            }
            if !store.healthy() || store.status() != StorageStatus::Ready {
                state.cache.clear();
                // Availability recovery must not stop the supervisor. Cancel
                // owned generations and let them drain before admitting replacements.
                // Retention barriers gate promotion but do not cancel their own publisher.
                if store.status() != StorageStatus::Maintaining {
                    for flight in state.flights.values() {
                        flight.cancellation.cancel();
                    }
                }
            }
        }
    }

    pub(super) fn usable_age(&self, value: &StoredPayload) -> Option<u64> {
        let now = self.clock.now();
        if value.provenance.retrieved_at > now {
            return None;
        }
        if let Some(store) = &self.persistence {
            let snapshot = value.snapshot.as_ref()?;
            if snapshot.captured_at > now || !store.policy().retains(snapshot.captured_at, now) {
                return None;
            }
        }
        now.checked_sub(value.provenance.validated_at)
    }

    fn persistent_key(&self, key: &CacheKey) -> PersistentKey {
        PersistentKey {
            history: HistoryKey {
                namespace: self.namespace.clone(),
                provider: key.provider.clone(),
                dataset: key.dataset.clone(),
                query: key.query.clone(),
            },
            processor_version: key.processor_version.clone(),
            schema_version: key.schema_version,
        }
    }

    fn history_key(&self, query: Query) -> Result<HistoryKey, RetrievalError> {
        query.validate()?;
        if self.shutdown.is_cancelled() {
            return Err(RetrievalError::Shutdown);
        }
        if let Some(error) = self.storage_error() {
            return Err(error);
        }
        let source = self
            .sources
            .get(query.source())
            .ok_or(RetrievalError::UnknownSource)?;
        Ok(self
            .persistent_key(&CacheKey::for_source(source, query)?)
            .history)
    }

    pub async fn list_snapshots(
        &self,
        query: Query,
        cursor: Option<String>,
        limit: usize,
        cancellation: CancellationToken,
    ) -> Result<SnapshotPage, RetrievalError> {
        if !(1..=20).contains(&limit) || cursor.as_ref().is_some_and(|c| c.len() > 256) {
            return Err(RetrievalError::InvalidInput);
        }
        let key = self.history_key(query)?;
        let store = self
            .persistence
            .as_ref()
            .ok_or(RetrievalError::SnapshotUnavailable)?;
        if cancellation.is_cancelled() {
            return Err(RetrievalError::Cancelled);
        }
        let epoch = store.epoch();
        let operation_cancel = cancellation.child_token();
        let _cancel_on_drop = operation_cancel.clone().drop_guard();
        let result = store
            .list(key, cursor, limit, self.clock.now(), operation_cancel)
            .await;
        if let Some(error) = self.storage_error() {
            return Err(error);
        }
        if store.epoch() != epoch {
            return Err(RetrievalError::Busy);
        }
        if cancellation.is_cancelled() {
            return Err(RetrievalError::Cancelled);
        }
        let page = result?;
        let now = self.clock.now();
        if page
            .snapshots
            .iter()
            .any(|snapshot| !store.policy().retains(snapshot.captured_at, now))
        {
            // Expiry during the storage operation must not produce an empty or
            // truncated page carrying a continuation cursor for different rows.
            return Err(RetrievalError::Busy);
        }
        Ok(page)
    }

    pub async fn get_snapshot(
        &self,
        query: Query,
        id: String,
        cancellation: CancellationToken,
    ) -> Result<SnapshotEnvelope, RetrievalError> {
        if !valid_snapshot_id(&id) {
            return Err(RetrievalError::InvalidInput);
        }
        let key = self.history_key(query)?;
        let store = self
            .persistence
            .as_ref()
            .ok_or(RetrievalError::SnapshotUnavailable)?;
        if cancellation.is_cancelled() {
            return Err(RetrievalError::Cancelled);
        }
        let epoch = store.epoch();
        let operation_cancel = cancellation.child_token();
        let _cancel_on_drop = operation_cancel.clone().drop_guard();
        let result = store.get(key, id, self.clock.now(), operation_cancel).await;
        if let Some(error) = self.storage_error() {
            return Err(error);
        }
        if store.epoch() != epoch {
            return Err(RetrievalError::Busy);
        }
        if cancellation.is_cancelled() {
            return Err(RetrievalError::Cancelled);
        }
        let mut snapshot = result?;
        let now = self.clock.now();
        if !store.policy().retains(snapshot.snapshot.captured_at, now) {
            return Err(RetrievalError::SnapshotUnavailable);
        }
        snapshot.clock_anomaly |= snapshot.snapshot.captured_at > now
            || snapshot.provenance.retrieved_at > now
            || snapshot.provenance.validated_at > now;
        Ok(snapshot)
    }

    pub(super) async fn resolve_persistent(
        self: &Arc<Self>,
        source: &Source,
        provider: &Provider,
        key: CacheKey,
        query: Query,
        cancellation: &CancellationToken,
        generation: u64,
    ) -> PublishedOutcome {
        let work = self.resolve_storage_then_upstream(
            source,
            provider,
            &key,
            query,
            cancellation,
            generation,
        );
        tokio::pin!(work);
        let result = tokio::select! {
            biased;
            _ = cancellation.cancelled() => {
                // Drain owned storage work instead of dropping an uncertain transaction.
                let _ = work.await;
                Err(RetrievalError::Cancelled)
            },
            result = &mut work => result,
            _ = tokio::time::sleep(RESOLUTION_DEADLINE) => {
                cancellation.cancel();
                let _ = work.await;
                Err(RetrievalError::StorageUnavailable)
            },
        };
        if let Ok(mut state) = self.state.lock() {
            self.sync_epoch(&mut state);
        }
        result
    }

    async fn resolve_storage_then_upstream(
        self: &Arc<Self>,
        source: &Source,
        provider: &Provider,
        key: &CacheKey,
        query: Query,
        cancellation: &CancellationToken,
        generation: u64,
    ) -> PublishedOutcome {
        let store = self.persistence.as_ref().ok_or(RetrievalError::Internal)?;
        let recovery_epoch = store.recovery_epoch();
        if cancellation.is_cancelled() {
            return Err(RetrievalError::Cancelled);
        }
        let storage_key = self.persistent_key(key);
        let lookup = store
            .lookup(storage_key.clone(), self.clock.now(), cancellation.clone())
            .await?;
        if let Some(found) = lookup.value {
            if cancellation.is_cancelled() {
                return Err(RetrievalError::Cancelled);
            }
            let age = self.usable_age(&found.payload);
            if age.is_some_and(|age| age <= RETENTION_SECONDS) {
                self.promote_persistent(key, generation, &found)?;
                if age.is_some_and(|age| age < FRESH_SECONDS) {
                    self.counters.hits.fetch_add(1, Ordering::Relaxed);
                    return Ok((found.payload, Some(found.epoch)));
                }
            }
        }
        if cancellation.is_cancelled() {
            return Err(RetrievalError::Cancelled);
        }
        // A corrupt/unavailable store is never interpreted as a miss. Only a
        // successful lookup can reach upstream admission.
        if let Some(error) = self.storage_error() {
            return Err(error);
        }
        let permit = provider
            .active
            .clone()
            .try_acquire_owned()
            .map_err(|_| self.busy())?;
        self.set_stage(&query, generation, ProgressStage::Refreshing);
        let candidate = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return Err(RetrievalError::Cancelled),
            result = tokio::time::timeout(REFRESH_DEADLINE, self.refresh(source, provider, query, cancellation, generation)) => {
                result.unwrap_or(Err(RetrievalError::Unavailable))?
            }
        };
        drop(permit);
        if store.recovery_epoch() != recovery_epoch {
            return Err(RetrievalError::StorageUnavailable);
        }
        if let Some(error) = self.storage_error() {
            return Err(error);
        }
        let weak = Arc::downgrade(self);
        let auth_key = key.clone();
        let authorize = Arc::new(move || {
            let Some(service) = weak.upgrade() else {
                return false;
            };
            let Ok(state) = service.state.lock() else {
                return false;
            };
            !state.stopping
                && !service.shutdown.is_cancelled()
                && service.persistence.as_ref().is_some_and(|store| {
                    store.recovery_epoch() == recovery_epoch
                        && matches!(
                            store.status(),
                            StorageStatus::Ready | StorageStatus::Maintaining
                        )
                })
                && state.flights.get(&auth_key).is_some_and(|flight| {
                    flight.generation == generation
                        && flight.waiters > 0
                        && !flight.cancellation.is_cancelled()
                })
        });
        let committed = match store
            .publish(PublicationRequest {
                key: storage_key.clone(),
                value: candidate,
                expected: lookup.observation,
                now: self.clock.now(),
                authorize,
                cancellation: cancellation.clone(),
            })
            .await?
        {
            PublicationOutcome::Accepted(committed) => committed,
            PublicationOutcome::Conflict => {
                if cancellation.is_cancelled() {
                    return Err(RetrievalError::Cancelled);
                }
                // A competing accepted observation wins. Never rebase and replay
                // our older candidate or create a second upstream refresh.
                store
                    .lookup(storage_key, self.clock.now(), cancellation.clone())
                    .await?
                    .value
                    .ok_or(RetrievalError::Busy)?
            }
        };
        if cancellation.is_cancelled() {
            return Err(RetrievalError::Cancelled);
        }
        if store.recovery_epoch() != recovery_epoch {
            return Err(RetrievalError::StorageUnavailable);
        }
        if !self
            .usable_age(&committed.payload)
            .is_some_and(|age| age < FRESH_SECONDS)
        {
            return Err(RetrievalError::FreshnessUnavailable);
        }
        Ok((committed.payload, Some(committed.epoch)))
    }

    fn promote_persistent(
        &self,
        key: &CacheKey,
        generation: u64,
        found: &StoredResult,
    ) -> Result<(), RetrievalError> {
        let mut state = self.state.lock().map_err(|_| RetrievalError::Internal)?;
        self.sync_epoch(&mut state);
        if !self.storage_healthy() || state.storage_epoch != found.epoch {
            return Err(RetrievalError::StorageUnavailable);
        }
        if state.stopping
            || !state.flights.get(key).is_some_and(|flight| {
                flight.generation == generation
                    && flight.waiters > 0
                    && !flight.cancellation.is_cancelled()
            })
        {
            return Err(RetrievalError::Cancelled);
        }
        state.cache.publish(key.clone(), found.payload.clone());
        Ok(())
    }
}

#[cfg(test)]
#[path = "persistent/tests.rs"]
mod tests;
