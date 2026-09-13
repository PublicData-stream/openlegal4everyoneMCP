//! L2 orchestration. The adapter owns filesystem work; this service owns policy.
use super::*;
use crate::persistence::{HistoryKey, PersistentKey, StoredResult};
use openlegal_domain::history::{SnapshotEnvelope, SnapshotPage, valid_snapshot_id};

const RESOLUTION_DEADLINE: Duration = Duration::from_secs(20);

impl RetrievalService {
    pub fn history_enabled(&self) -> bool {
        self.persistence.is_some()
    }

    pub(super) fn storage_healthy(&self) -> bool {
        self.persistence
            .as_ref()
            .is_none_or(|store| store.healthy())
    }

    pub(super) fn fail_storage(&self) {
        if self.shutdown.is_cancelled() {
            return;
        }
        self.failed.store(true, Ordering::Relaxed);
        self.shutdown.cancel();
    }

    pub(super) fn sync_epoch(&self, state: &mut State) {
        if let Some(store) = &self.persistence {
            let epoch = store.epoch();
            if state.storage_epoch != epoch {
                state.cache.clear();
                state.storage_epoch = epoch;
            }
            if !store.healthy() {
                self.fail_storage();
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
        let operation_cancel = cancellation.child_token();
        let _cancel_on_drop = operation_cancel.clone().drop_guard();
        let result = store
            .list(key, cursor, limit, self.clock.now(), operation_cancel)
            .await;
        if !store.healthy() {
            self.fail_storage();
            return Err(RetrievalError::StorageUnavailable);
        }
        if cancellation.is_cancelled() {
            return Err(RetrievalError::Cancelled);
        }
        let mut page = result?;
        page.snapshots.retain(|snapshot| {
            store
                .policy()
                .retains(snapshot.captured_at, self.clock.now())
        });
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
        let operation_cancel = cancellation.child_token();
        let _cancel_on_drop = operation_cancel.clone().drop_guard();
        let result = store.get(key, id, self.clock.now(), operation_cancel).await;
        if !store.healthy() {
            self.fail_storage();
            return Err(RetrievalError::StorageUnavailable);
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
        let work = self.resolve_disk_then_upstream(
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
                // Await cancellation/reconciliation instead of dropping active disk I/O.
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
        if !self.storage_healthy() {
            self.fail_storage();
        }
        result
    }

    async fn resolve_disk_then_upstream(
        self: &Arc<Self>,
        source: &Source,
        provider: &Provider,
        key: &CacheKey,
        query: Query,
        cancellation: &CancellationToken,
        generation: u64,
    ) -> PublishedOutcome {
        let store = self.persistence.as_ref().ok_or(RetrievalError::Internal)?;
        if cancellation.is_cancelled() {
            return Err(RetrievalError::Cancelled);
        }
        let disk_key = self.persistent_key(key);
        if let Some(found) = store
            .lookup(disk_key.clone(), self.clock.now(), cancellation.clone())
            .await?
        {
            if cancellation.is_cancelled() {
                return Err(RetrievalError::Cancelled);
            }
            let age = self.usable_age(&found.payload);
            if age.is_some_and(|age| age <= RETENTION_SECONDS) {
                self.promote_disk(key, generation, &found)?;
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
        if !store.healthy() {
            return Err(RetrievalError::StorageUnavailable);
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
                && state.flights.get(&auth_key).is_some_and(|flight| {
                    flight.generation == generation
                        && flight.waiters > 0
                        && !flight.cancellation.is_cancelled()
                })
        });
        let committed = store
            .publish(
                disk_key,
                candidate,
                self.clock.now(),
                authorize,
                cancellation.clone(),
            )
            .await?;
        if cancellation.is_cancelled() {
            return Err(RetrievalError::Cancelled);
        }
        if !self
            .usable_age(&committed.payload)
            .is_some_and(|age| age < FRESH_SECONDS)
        {
            return Err(RetrievalError::FreshnessUnavailable);
        }
        Ok((committed.payload, Some(committed.epoch)))
    }

    fn promote_disk(
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
