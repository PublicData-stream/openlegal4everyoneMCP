//! Health-probe provenance is deliberately separate from arbitrary storage errors.
//! In particular, an unacknowledged publication must never enter this recovery path.
use super::*;
use openlegal_domain::RetrievalError;
use std::future::Future;
use std::sync::{Mutex, atomic::AtomicU64};
use std::time::Duration;

pub(super) const PUBLIC_HEALTH_WAITERS: usize = 16;
const PUBLIC_HEALTH_WAIT: Duration = Duration::from_secs(2);

struct CommitGuard {
    store: PgCorpusStore,
    armed: bool,
}
impl CommitGuard {
    fn confirmed(&mut self) {
        self.armed = false;
    }
}
impl Drop for CommitGuard {
    fn drop(&mut self) {
        if self.armed {
            self.store.record_commit_uncertain();
        }
    }
}

// Field drop order is part of the shutdown proof: uncertainty is published
// before the active-work count can reach zero, even on panic/task abortion.
struct OwnedCommit {
    guard: CommitGuard,
    _work: OwnedWork,
}

/// A statement remains uncommitted while it borrows request data. Only its
/// pooled transaction is handed to the independently owned ACK task.
pub(super) trait CorpusExecuteExt<'q>: Sized {
    fn corpus_execute<'a>(
        self,
        store: &'a PgCorpusStore,
    ) -> BoxFuture<'a, Result<sqlx::postgres::PgQueryResult, DatabaseError>>
    where
        'q: 'a;
}
impl<'q> CorpusExecuteExt<'q>
    for sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>
{
    fn corpus_execute<'a>(
        self,
        store: &'a PgCorpusStore,
    ) -> BoxFuture<'a, Result<sqlx::postgres::PgQueryResult, DatabaseError>>
    where
        'q: 'a,
    {
        Box::pin(async move {
            let _mutation = store.availability.enter()?;
            if store.durability_fence.is_none() {
                // Preserve legacy autocommit and its error mapping exactly.
                return self.execute(&store.pool).await.map_err(db);
            }
            let mut tx = begin_storage(&store.pool, "corpus_statement").await?;
            let value = self.execute(&mut *tx).await.map_err(db)?;
            // Cancellation before this handoff drops an uncommitted transaction.
            store.commit_storage(tx, "corpus_statement").await?;
            Ok(value)
        })
    }
}

#[derive(Default)]
struct WorkState {
    closing: bool,
    pending: usize,
    commits: usize,
}
struct OwnedWork {
    availability: Arc<Availability>,
    commit: bool,
}
impl Drop for OwnedWork {
    fn drop(&mut self) {
        let mut work = self
            .availability
            .work
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        work.pending -= 1;
        if self.commit {
            work.commits -= 1;
        }
        self.availability.drained.notify_waiters();
    }
}

#[derive(Default)]
pub(super) struct Availability {
    failed: AtomicBool,
    uncertain: AtomicBool,
    generation: AtomicU64,
    recoveries: AtomicU64,
    transition: Mutex<()>,
    work: Mutex<WorkState>,
    drained: tokio::sync::Notify,
}

impl Availability {
    fn enter(self: &Arc<Self>) -> Result<OwnedWork, DatabaseError> {
        self.enter_work(None)
    }
    fn enter_commit(self: &Arc<Self>, limit: usize) -> Result<OwnedWork, DatabaseError> {
        self.enter_work(Some(limit))
    }
    fn enter_work(self: &Arc<Self>, limit: Option<usize>) -> Result<OwnedWork, DatabaseError> {
        let mut work = self
            .work
            .lock()
            .map_err(|_| DatabaseError::StorageUnavailable)?;
        if work.closing {
            return Err(DatabaseError::StorageUnavailable);
        }
        if limit.is_some_and(|limit| work.commits >= limit) {
            // No COMMIT has been submitted. The caller still owns a rollbackable
            // transaction, so capacity refusal is contention, never uncertainty.
            return Err(DatabaseError::StorageContended);
        }
        work.pending += 1;
        if limit.is_some() {
            work.commits += 1;
        }
        Ok(OwnedWork {
            availability: self.clone(),
            commit: limit.is_some(),
        })
    }
    async fn close_and_drain(&self) {
        loop {
            let notified = self.drained.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let mut work = self.work.lock().unwrap_or_else(|e| e.into_inner());
                work.closing = true;
                if work.pending == 0 {
                    return;
                }
            }
            notified.await;
        }
    }

    pub(super) fn healthy(&self) -> bool {
        !self.failed.load(Ordering::Acquire) && !self.uncertain.load(Ordering::Acquire)
    }
    fn fail(&self) {
        let _guard = self.transition.lock().ok();
        self.generation.fetch_add(1, Ordering::AcqRel);
        self.failed.store(true, Ordering::Release);
    }
    pub(super) fn fail_uncertain(&self) {
        let _guard = self.transition.lock().ok();
        self.uncertain.store(true, Ordering::Release);
        self.generation.fetch_add(1, Ordering::AcqRel);
        self.failed.store(true, Ordering::Release);
    }
    fn complete(&self, generation: u64, cancel: &CancellationToken) -> Result<(), DatabaseError> {
        let work = self
            .work
            .lock()
            .map_err(|_| DatabaseError::StorageUnavailable)?;
        if work.closing || (self.failed.load(Ordering::Acquire) && work.pending != 0) {
            return Err(DatabaseError::StorageUnavailable);
        }
        let _guard = self
            .transition
            .lock()
            .map_err(|_| DatabaseError::StorageUnavailable)?;
        check(cancel)?;
        if self.uncertain.load(Ordering::Acquire)
            || self.generation.load(Ordering::Acquire) != generation
        {
            return Err(DatabaseError::StorageUnavailable);
        }
        if self.failed.swap(false, Ordering::AcqRel) {
            self.recoveries.fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
    }
}

/// Only a returned `Availability` from the dependency probe may keep the runtime
/// alive. This type is never constructed from a publication/worker error.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CorpusHealthFailure {
    Contended,
    Availability(DatabaseError),
    Fatal(DatabaseError),
}
impl CorpusHealthFailure {
    pub fn error(self) -> DatabaseError {
        match self {
            Self::Contended => DatabaseError::StorageContended,
            Self::Availability(error) | Self::Fatal(error) => error,
        }
    }
}

/// Consumed only after the runtime rechecks its still-owned lease. A newer
/// observed dependency failure prevents this proof from reopening admission.
#[derive(Debug)]
pub struct CorpusHealthProof {
    generation: u64,
    cancel: CancellationToken,
}

impl PgCorpusStore {
    fn tracks_failures(&self) -> bool {
        self.track_uncertainty || self.durability_fence.is_some()
    }
    fn commit_guard(&self) -> CommitGuard {
        CommitGuard {
            store: self.clone(),
            armed: self.tracks_failures(),
        }
    }
    pub(super) fn record_commit_uncertain(&self) {
        // The pool-only constructor remains the legacy fixture/audit adapter.
        // Production composition and every strict fence keep a sticky stop.
        if self.tracks_failures() {
            self.availability.fail_uncertain();
            if let Some(fence) = &self.durability_fence {
                fence.mark_uncertain();
            }
        }
    }
    /// Already observed, bounded counters only; readiness/metrics perform no I/O.
    pub fn metrics_prometheus(&self) -> String {
        let blobs = self.blobs.metrics();
        format!(
            "openlegal_corpus_storage_ready {}\nopenlegal_corpus_storage_failures_total {}\nopenlegal_corpus_storage_recoveries_total {}\nopenlegal_corpus_blob_active_jobs {}\nopenlegal_corpus_blob_queued_jobs {}\nopenlegal_corpus_blob_started_jobs_total {}\nopenlegal_corpus_blob_queue_wait_microseconds_total {}\nopenlegal_corpus_blob_filesystem_work_microseconds_total {}\n",
            u8::from(self.healthy()),
            self.availability.generation.load(Ordering::Acquire),
            self.availability.recoveries.load(Ordering::Relaxed),
            blobs.active_jobs,
            blobs.queued_jobs,
            blobs.started_jobs,
            blobs.queue_wait_us,
            blobs.filesystem_work_us,
        )
    }
    fn dependency_failure(&self, error: DatabaseError) -> CorpusHealthFailure {
        match error {
            DatabaseError::StorageContended => CorpusHealthFailure::Contended,
            DatabaseError::StorageUnavailable => {
                if self.tracks_failures() {
                    self.availability.fail();
                }
                CorpusHealthFailure::Availability(error)
            }
            _ => {
                if error == DatabaseError::StorageCorrupt {
                    self.blocked.store(true, Ordering::Release);
                }
                CorpusHealthFailure::Fatal(error)
            }
        }
    }

    pub(super) fn record_blob_error(&self, error: RetrievalError) -> DatabaseError {
        let mapped = blob_error(error);
        if mapped == DatabaseError::StorageCorrupt {
            self.blocked.store(true, Ordering::Release);
        } else if mapped == DatabaseError::StorageUnavailable && self.tracks_failures() {
            self.availability.fail();
        }
        mapped
    }

    /// Fresh pre-admission/GC proof. Capacity and caller cancellation are not
    /// dependency failures. COMMIT uses the distinct owned, non-replayable path.
    pub(super) async fn durable(&self, cancel: CancellationToken) -> Result<(), DatabaseError> {
        self.check_durability(cancel)
            .await
            .map_err(|error| match error {
                RetrievalError::Busy => DatabaseError::StorageContended,
                RetrievalError::Cancelled => DatabaseError::Cancelled,
                error => self.record_blob_error(error),
            })
    }

    async fn check_durability(&self, cancel: CancellationToken) -> Result<(), RetrievalError> {
        self.check_durability_at(tokio::time::Instant::now() + Duration::from_secs(2), cancel)
            .await
    }

    async fn check_durability_at(
        &self,
        deadline: tokio::time::Instant,
        cancel: CancellationToken,
    ) -> Result<(), RetrievalError> {
        if let Some(fence) = &self.durability_fence {
            tokio::time::timeout_at(deadline, fence.check(deadline, cancel))
                .await
                .map_err(|_| RetrievalError::StorageUnavailable)??;
        }
        Ok(())
    }

    /// Close mutation admission and wait for every submitted ACK task, including
    /// tasks whose public request receiver was dropped. Call before blob/lease
    /// closure and before declaring the shared runtime guard clean.
    pub async fn drain_storage_work(&self) -> Result<(), DatabaseError> {
        self.availability.close_and_drain().await;
        if self.availability.uncertain.load(Ordering::Acquire) {
            Err(DatabaseError::StorageUnavailable)
        } else {
            Ok(())
        }
    }

    pub(super) async fn commit_storage(
        &self,
        tx: sqlx::Transaction<'static, sqlx::Postgres>,
        operation: &'static str,
    ) -> Result<(), DatabaseError> {
        if !self.tracks_failures() {
            return commit_sql(tx, operation).await;
        }
        self.owned_commit(commit_sql(tx, operation)).await
    }

    async fn owned_commit(
        &self,
        commit: impl Future<Output = Result<(), DatabaseError>> + Send + 'static,
    ) -> Result<(), DatabaseError> {
        // Admission and accounting happen before spawn without an intervening
        // await. Closing cannot miss a task at the handoff boundary.
        let active = self
            .availability
            .enter_commit(self.pool.options().get_max_connections() as usize)?;
        let store = self.clone();
        let (send, receive) = tokio::sync::oneshot::channel();
        let owned = OwnedCommit {
            guard: store.commit_guard(),
            _work: active,
        };
        tokio::spawn(async move {
            let mut owned = owned;
            let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
            let committed = if store.durability_fence.is_some() {
                tokio::time::timeout_at(deadline, commit)
                    .await
                    .unwrap_or(Err(DatabaseError::StorageUnavailable))
            } else {
                commit.await
            };
            let result = match committed {
                Ok(()) => store
                    .check_durability_at(deadline, CancellationToken::new())
                    .await
                    .map_err(|error| {
                        store.record_blob_error(error);
                        // COMMIT has already been acknowledged locally. Every
                        // failed ACK proof is uncertain, never replayable as
                        // statement contention or ordinary request cancellation.
                        DatabaseError::StorageUnavailable
                    }),
                Err(error) => Err(error),
            };
            if result.is_ok() {
                owned.guard.confirmed();
            }
            // Publish uncertainty and finish physical accounting before any
            // other runtime thread can observe the failed result.
            drop(owned);
            // Dropping a public receiver never drops COMMIT or its fresh fence.
            let _ = send.send(result);
        });
        receive
            .await
            .unwrap_or(Err(DatabaseError::StorageUnavailable))
    }

    fn health_probe_allowed(&self) -> Result<(), CorpusHealthFailure> {
        if self.blocked.load(Ordering::Acquire) || self.pool.is_closed() {
            return Err(CorpusHealthFailure::Fatal(DatabaseError::StorageCorrupt));
        }
        if self.availability.uncertain.load(Ordering::Acquire) {
            return Err(CorpusHealthFailure::Fatal(
                DatabaseError::StorageUnavailable,
            ));
        }
        Ok(())
    }

    /// One active background coordinator uses the reserved probe pool. A second
    /// background caller yields without queueing or opening another connection.
    pub async fn probe_health(
        &self,
        index_generation: Option<u64>,
        cancel: CancellationToken,
    ) -> Result<CorpusHealthProof, CorpusHealthFailure> {
        self.health_probe_allowed()?;
        let _probe = self
            .health_probe
            .try_lock()
            .map_err(|_| CorpusHealthFailure::Contended)?;
        self.probe_health_locked(index_generation, cancel, _probe)
            .await
    }

    /// Public reads may wait for the coordinator, then perform their own fresh
    /// proof. Waiting never reuses the background result or renews a caller's
    /// outer request deadline. Cancellation/drop releases admission and the
    /// mutex queue; local contention is not an observed dependency failure.
    pub(super) async fn wait_for_health_probe(
        &self,
        index_generation: Option<u64>,
        cancel: CancellationToken,
    ) -> Result<CorpusHealthProof, CorpusHealthFailure> {
        self.health_probe_allowed()?;
        check(&cancel).map_err(CorpusHealthFailure::Fatal)?;
        let _waiter = self
            .health_waiters
            .try_acquire()
            .map_err(|_| CorpusHealthFailure::Contended)?;
        let deadline = tokio::time::Instant::now() + PUBLIC_HEALTH_WAIT;
        let probe = tokio::select! {
            biased;
            _ = cancel.cancelled() => return Err(CorpusHealthFailure::Fatal(DatabaseError::Cancelled)),
            result = tokio::time::timeout_at(deadline, self.health_probe.lock()) => result,
        };
        self.health_probe_allowed()?;
        check(&cancel).map_err(CorpusHealthFailure::Fatal)?;
        let probe = probe.map_err(|_| CorpusHealthFailure::Contended)?;
        if tokio::time::Instant::now() >= deadline {
            return Err(CorpusHealthFailure::Contended);
        }
        self.probe_health_locked(index_generation, cancel, probe)
            .await
    }

    async fn probe_health_locked(
        &self,
        index_generation: Option<u64>,
        cancel: CancellationToken,
        _probe: tokio::sync::MutexGuard<'_, ()>,
    ) -> Result<CorpusHealthProof, CorpusHealthFailure> {
        // A sticky stop may have arrived while a public caller was waiting.
        self.health_probe_allowed()?;
        check(&cancel).map_err(CorpusHealthFailure::Fatal)?;
        let generation = self.availability.generation.load(Ordering::Acquire);
        let recovering = !self.availability.healthy();
        let started = tokio::time::Instant::now();
        let sql = async {
            retry_storage(&cancel, "corpus_health", || async {
                sqlx::query("SELECT singleton FROM openlegal.corpus_control")
                    .fetch_one(&self.probe_pool).await.map_err(db)
            }).await?;
            if recovering {
                tokio::time::timeout(Duration::from_secs(5), crate::postgres::verify_schema(&self.probe_pool))
                    .await.map_err(|_| DatabaseError::StorageUnavailable)?
                    .map_err(blob_error)?;
                // The control-row lock fences older corpus publication commits.
                let mut tx = begin_storage(&self.probe_pool, "corpus_recovery").await?;
                let (ack, watermark): (i64, i64) = sqlx::query_as(
                    "SELECT index_ack,next_event-1 FROM openlegal.corpus_control WHERE singleton FOR UPDATE",
                ).fetch_one(&mut *tx).await.map_err(db)?;
                if let Some(current) = index_generation {
                    let ack = u64::try_from(ack).map_err(corrupt)?;
                    let watermark = u64::try_from(watermark).map_err(corrupt)?;
                    if current < ack || current > watermark {
                        return Err(DatabaseError::StorageCorrupt);
                    }
                }
                if self.durability_fence.is_some() {
                    // Even this row-lock-only transaction can enter SyncRep;
                    // its failure is probe provenance, never publication replay.
                    tokio::time::timeout(Duration::from_secs(2), commit_sql(tx, "corpus_recovery"))
                        .await.map_err(|_| DatabaseError::StorageUnavailable)??;
                } else {
                    commit_sql(tx, "corpus_recovery").await?;
                }
            }
            Ok(())
        }.await;
        if let Err(error) = sql {
            tracing::warn!(
                target: "openlegal_adapters::corpus",
                operation = "corpus_health",
                health_phase = "sql",
                elapsed_ms = started.elapsed().as_millis().min(999_999_999_999) as u64,
                error = sql_error_name(error),
                "corpus health dependency failed"
            );
            return Err(self.dependency_failure(error));
        }
        let started = tokio::time::Instant::now();
        if let Err(error) = self.blobs.health(cancel.clone()).await {
            tracing::warn!(
                target: "openlegal_adapters::corpus",
                operation = "corpus_health",
                health_phase = "blob",
                elapsed_ms = started.elapsed().as_millis().min(999_999_999_999) as u64,
                error = blob_error_name(error),
                "corpus health dependency failed"
            );
            if error == RetrievalError::Busy {
                return Err(CorpusHealthFailure::Contended);
            }
            let mapped = self.record_blob_error(error);
            return Err(match error {
                RetrievalError::StorageUnavailable | RetrievalError::StorageCapacity => {
                    CorpusHealthFailure::Availability(mapped)
                }
                _ => CorpusHealthFailure::Fatal(mapped),
            });
        }
        if let Err(error) = self.check_durability(cancel.clone()).await {
            if error == RetrievalError::Busy {
                return Err(CorpusHealthFailure::Contended);
            }
            let mapped = self.record_blob_error(error);
            return Err(match error {
                RetrievalError::StorageUnavailable | RetrievalError::StorageCapacity => {
                    CorpusHealthFailure::Availability(mapped)
                }
                _ => CorpusHealthFailure::Fatal(mapped),
            });
        }
        check(&cancel).map_err(CorpusHealthFailure::Fatal)?;
        Ok(CorpusHealthProof { generation, cancel })
    }

    pub fn complete_health(&self, proof: CorpusHealthProof) -> Result<(), CorpusHealthFailure> {
        if self.blocked.load(Ordering::Acquire) || self.pool.is_closed() {
            return Err(CorpusHealthFailure::Fatal(DatabaseError::StorageCorrupt));
        }
        self.availability
            .complete(proof.generation, &proof.cancel)
            .map_err(|error| {
                if error == DatabaseError::StorageUnavailable
                    && !self.availability.uncertain.load(Ordering::Acquire)
                {
                    CorpusHealthFailure::Availability(error)
                } else {
                    CorpusHealthFailure::Fatal(error)
                }
            })
    }
}

fn sql_error_name(error: DatabaseError) -> &'static str {
    match error {
        DatabaseError::StorageContended => "StorageContended",
        DatabaseError::StorageUnavailable => "StorageUnavailable",
        DatabaseError::Cancelled => "Cancelled",
        _ => "Other",
    }
}
fn blob_error_name(error: RetrievalError) -> &'static str {
    use RetrievalError as E;
    match error {
        E::StorageUnavailable => "StorageUnavailable",
        E::StorageCorrupt => "StorageCorrupt",
        E::StorageCapacity => "StorageCapacity",
        E::SnapshotUnavailable => "SnapshotUnavailable",
        E::InvalidInput => "InvalidInput",
        E::UnknownSource => "UnknownSource",
        E::NotFound => "NotFound",
        E::Ambiguous => "Ambiguous",
        E::Busy => "Busy",
        E::Unavailable => "Unavailable",
        E::FreshnessUnavailable => "FreshnessUnavailable",
        E::NormalizationFailed => "NormalizationFailed",
        E::InvalidPayload => "InvalidPayload",
        E::ResourceLimit => "ResourceLimit",
        E::Cancelled => "Cancelled",
        E::Shutdown => "Shutdown",
        E::Internal => "Internal",
        E::Throttled { .. } => "Throttled",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use openlegal_application::blob::{BlobMetrics, BlobPage, BlobPutResult};
    use std::sync::atomic::AtomicUsize;

    #[derive(Default)]
    struct ProbeBlob {
        calls: AtomicUsize,
        deletions: AtomicUsize,
        error: Mutex<Option<RetrievalError>>,
        pause: Mutex<Option<Arc<ProbePause>>>,
    }
    struct ProbePause {
        entered: tokio::sync::Notify,
        release: tokio::sync::Semaphore,
    }
    fn unused<T: Send + 'static>() -> BoxFuture<'static, Result<T, RetrievalError>> {
        Box::pin(async { Err(RetrievalError::Internal) })
    }
    impl BlobStore for ProbeBlob {
        fn put_if_absent(
            &self,
            _: BlobLocation,
            _: Vec<u8>,
            _: CancellationToken,
        ) -> BoxFuture<'static, Result<BlobPutResult, RetrievalError>> {
            unused()
        }
        fn get(
            &self,
            _: BlobLocation,
            _: CancellationToken,
        ) -> BoxFuture<'static, Result<Option<Vec<u8>>, RetrievalError>> {
            unused()
        }
        fn delete_if_present(
            &self,
            _: BlobLocation,
            _: CancellationToken,
        ) -> BoxFuture<'static, Result<(), RetrievalError>> {
            self.deletions.fetch_add(1, Ordering::SeqCst);
            Box::pin(async { Ok(()) })
        }
        fn enumerate(
            &self,
            _: Option<String>,
            _: usize,
            _: CancellationToken,
        ) -> BoxFuture<'static, Result<BlobPage, RetrievalError>> {
            unused()
        }
        fn cleanup_staging(
            &self,
            _: u64,
            _: usize,
            _: CancellationToken,
        ) -> BoxFuture<'static, Result<usize, RetrievalError>> {
            unused()
        }
        fn health(
            &self,
            cancel: CancellationToken,
        ) -> BoxFuture<'static, Result<(), RetrievalError>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let error = *self.error.lock().unwrap();
            let pause = self.pause.lock().unwrap().clone();
            Box::pin(async move {
                if let Some(pause) = pause {
                    pause.entered.notify_one();
                    let permit = tokio::select! {
                        _ = cancel.cancelled() => return Err(RetrievalError::Cancelled),
                        permit = pause.release.acquire() => permit.map_err(|_| RetrievalError::Shutdown)?,
                    };
                    permit.forget();
                }
                error.map_or(Ok(()), Err)
            })
        }
        fn close(&self) -> BoxFuture<'static, Result<(), RetrievalError>> {
            Box::pin(async { Ok(()) })
        }
        fn metrics(&self) -> BlobMetrics {
            BlobMetrics::default()
        }
    }

    fn disconnected_store() -> PgCorpusStore {
        // No query is executed by the tests using this pool.
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://fixture:fixture@localhost/unused")
            .unwrap();
        let mut store = PgCorpusStore::new(pool, Arc::new(ProbeBlob::default()));
        store.track_uncertainty = true;
        store
    }

    #[test]
    fn newer_failure_rejects_older_success_without_clearing_admission() {
        let state = Availability::default();
        state.fail();
        let proof = state.generation.load(Ordering::Acquire);
        state.fail();
        assert_eq!(
            state.complete(proof, &CancellationToken::new()),
            Err(DatabaseError::StorageUnavailable)
        );
        assert!(!state.healthy());
        state
            .complete(
                state.generation.load(Ordering::Acquire),
                &CancellationToken::new(),
            )
            .unwrap();
        assert!(state.healthy());
    }

    #[tokio::test]
    async fn availability_closes_readiness_and_admission_but_integrity_stays_sticky() {
        let store = disconnected_store();
        assert!(store.healthy());
        assert_eq!(
            store.dependency_failure(DatabaseError::StorageUnavailable),
            CorpusHealthFailure::Availability(DatabaseError::StorageUnavailable)
        );
        assert!(!store.healthy());
        assert_eq!(store.gate().await, Err(DatabaseError::StorageUnavailable));
        let proof = CorpusHealthProof {
            generation: store.availability.generation.load(Ordering::Acquire),
            cancel: CancellationToken::new(),
        };
        store.complete_health(proof).unwrap();
        assert!(store.healthy());
        store.dependency_failure(DatabaseError::StorageCorrupt);
        assert_eq!(store.gate().await, Err(DatabaseError::StorageCorrupt));
        assert!(matches!(
            store.probe_health(None, CancellationToken::new()).await,
            Err(CorpusHealthFailure::Fatal(DatabaseError::StorageCorrupt))
        ));
    }

    #[tokio::test]
    async fn a_second_health_coordinator_yields_without_querying_or_reopening() {
        let store = disconnected_store();
        store.availability.fail();
        let _active = store.health_probe.lock().await;
        assert!(matches!(
            store.probe_health(None, CancellationToken::new()).await,
            Err(CorpusHealthFailure::Contended)
        ));
        assert!(!store.healthy());
    }

    #[tokio::test]
    async fn public_health_waits_for_active_probe_without_false_dependency_failure() {
        let store = disconnected_store();
        let _active = store.health_probe.lock().await;
        let generation = store.availability.generation.load(Ordering::Acquire);
        let mut public = Box::pin(store.health());
        assert!(
            futures::poll!(public.as_mut()).is_pending(),
            "a public read must wait for its fresh probe instead of treating another coordinator as unavailable"
        );
        assert!(matches!(
            store.probe_health(None, CancellationToken::new()).await,
            Err(CorpusHealthFailure::Contended)
        ));
        drop(public);
        assert!(store.healthy());
        assert_eq!(
            store.availability.generation.load(Ordering::Acquire),
            generation
        );
        assert!(!store.availability.uncertain.load(Ordering::Acquire));
    }

    #[tokio::test(start_paused = true)]
    async fn public_health_wait_budget_is_contention_without_poisoning_availability() {
        let store = disconnected_store();
        let _active = store.health_probe.lock().await;
        let generation = store.availability.generation.load(Ordering::Acquire);
        let mut public = Box::pin(store.health());
        assert!(futures::poll!(public.as_mut()).is_pending());
        tokio::time::advance(PUBLIC_HEALTH_WAIT).await;
        assert_eq!(public.await, Err(DatabaseError::StorageContended));
        assert!(store.healthy());
        assert_eq!(
            store.availability.generation.load(Ordering::Acquire),
            generation
        );
        assert_eq!(
            store.health_waiters.available_permits(),
            PUBLIC_HEALTH_WAITERS
        );
        assert_eq!(store.probe_pool.size(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn a_late_available_coordinator_does_not_renew_the_public_wait_deadline() {
        let store = disconnected_store();
        let active = store.health_probe.lock().await;
        let mut public = Box::pin(store.health());
        assert!(futures::poll!(public.as_mut()).is_pending());
        tokio::time::advance(PUBLIC_HEALTH_WAIT).await;
        drop(active);
        assert_eq!(public.await, Err(DatabaseError::StorageContended));
        assert_eq!(store.probe_pool.size(), 0);
        assert!(store.healthy());
        assert_eq!(store.availability.generation.load(Ordering::Acquire), 0);
    }

    #[tokio::test]
    async fn public_health_waiters_are_bounded_and_cancel_or_drop_releases_admission() {
        let store = disconnected_store();
        let _active = store.health_probe.lock().await;
        let mut waiting = Vec::new();
        for _ in 0..PUBLIC_HEALTH_WAITERS {
            let mut request = Box::pin(store.health());
            assert!(futures::poll!(request.as_mut()).is_pending());
            waiting.push(request);
        }
        assert_eq!(store.health().await, Err(DatabaseError::StorageContended));
        assert_eq!(store.health_waiters.available_permits(), 0);
        drop(waiting.pop());
        assert_eq!(store.health_waiters.available_permits(), 1);
        let cancel = CancellationToken::new();
        let mut cancelled = Box::pin(store.wait_for_health_probe(None, cancel.clone()));
        assert!(futures::poll!(cancelled.as_mut()).is_pending());
        cancel.cancel();
        assert!(matches!(
            cancelled.await,
            Err(CorpusHealthFailure::Fatal(DatabaseError::Cancelled))
        ));
        drop(waiting);
        assert_eq!(
            store.health_waiters.available_permits(),
            PUBLIC_HEALTH_WAITERS
        );
        assert_eq!(store.probe_pool.size(), 0);
        assert!(store.healthy());
        assert_eq!(store.availability.generation.load(Ordering::Acquire), 0);
        assert!(!store.availability.uncertain.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn public_wait_cannot_cross_a_new_uncertain_commit_or_issue_sql_after_it() {
        let store = disconnected_store();
        let active = store.health_probe.lock().await;
        let mut public = Box::pin(store.health());
        assert!(futures::poll!(public.as_mut()).is_pending());
        store.record_commit_uncertain();
        drop(active);
        assert_eq!(public.await, Err(DatabaseError::StorageUnavailable));
        assert_eq!(store.probe_pool.size(), 0);
        assert!(!store.healthy());
        assert!(store.availability.uncertain.load(Ordering::Acquire));
    }

    struct FailedFence(Arc<AtomicUsize>);
    impl openlegal_application::durability::DurabilityFence for FailedFence {
        fn check(
            &self,
            _: tokio::time::Instant,
            _: CancellationToken,
        ) -> BoxFuture<'static, Result<(), RetrievalError>> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Box::pin(async { Err(RetrievalError::StorageUnavailable) })
        }
    }

    #[tokio::test]
    async fn replication_failure_is_not_replayed_and_closes_admission() {
        let mut store = disconnected_store();
        let calls = Arc::new(AtomicUsize::new(0));
        store.durability_fence = Some(Arc::new(FailedFence(calls.clone())));
        assert_eq!(
            store.durable(CancellationToken::new()).await,
            Err(DatabaseError::StorageUnavailable)
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(!store.healthy());
    }

    struct AdmissionFailure(RetrievalError);
    impl openlegal_application::durability::DurabilityFence for AdmissionFailure {
        fn check(
            &self,
            _: tokio::time::Instant,
            _: CancellationToken,
        ) -> BoxFuture<'static, Result<(), RetrievalError>> {
            let error = self.0;
            Box::pin(async move { Err(error) })
        }
    }

    #[tokio::test]
    async fn admission_contention_and_cancellation_do_not_poison_or_degrade_readiness() {
        for (error, expected) in [
            (RetrievalError::Busy, DatabaseError::StorageContended),
            (RetrievalError::Cancelled, DatabaseError::Cancelled),
        ] {
            let mut store = disconnected_store();
            store.durability_fence = Some(Arc::new(AdmissionFailure(error)));
            let generation = store.availability.generation.load(Ordering::Acquire);
            assert_eq!(store.gate().await, Err(expected));
            assert!(store.healthy());
            assert_eq!(
                store.availability.generation.load(Ordering::Acquire),
                generation
            );
            assert!(!store.availability.uncertain.load(Ordering::Acquire));
            // The exact same error AFTER COMMIT is a different boundary.
            assert_eq!(
                store.owned_commit(async { Ok(()) }).await,
                Err(DatabaseError::StorageUnavailable)
            );
            store.drain_storage_work().await.unwrap_err();
            assert!(!store.healthy());
            assert!(store.availability.uncertain.load(Ordering::Acquire));
        }
    }

    #[tokio::test]
    async fn cancelled_recovery_proof_cannot_reopen_admission() {
        let store = disconnected_store();
        store.availability.fail();
        let cancel = CancellationToken::new();
        let proof = CorpusHealthProof {
            generation: store.availability.generation.load(Ordering::Acquire),
            cancel: cancel.clone(),
        };
        cancel.cancel();
        assert_eq!(
            store.complete_health(proof),
            Err(CorpusHealthFailure::Fatal(DatabaseError::Cancelled))
        );
        assert!(!store.healthy());
    }

    #[tokio::test]
    async fn uncertain_commit_blocks_even_a_matching_new_health_proof() {
        let mut store = disconnected_store();
        store.track_uncertainty = true;
        store.record_commit_uncertain();
        let proof = CorpusHealthProof {
            generation: store.availability.generation.load(Ordering::Acquire),
            cancel: CancellationToken::new(),
        };
        assert_eq!(
            store.complete_health(proof),
            Err(CorpusHealthFailure::Fatal(
                DatabaseError::StorageUnavailable
            ))
        );
        assert!(matches!(
            store.probe_health(None, CancellationToken::new()).await,
            Err(CorpusHealthFailure::Fatal(
                DatabaseError::StorageUnavailable
            ))
        ));
        assert!(!store.healthy());
    }

    #[tokio::test]
    async fn dropping_public_receiver_keeps_owned_commit_until_ack_and_drain() {
        let store = disconnected_store();
        let (ack, wait) = tokio::sync::oneshot::channel();
        let mut request = Box::pin(store.owned_commit(async move {
            wait.await.unwrap();
            Ok(())
        }));
        assert!(futures::poll!(request.as_mut()).is_pending());
        drop(request);
        assert!(store.healthy());
        let mut drain = Box::pin(store.drain_storage_work());
        assert!(futures::poll!(drain.as_mut()).is_pending());
        assert!(matches!(
            store.availability.enter(),
            Err(DatabaseError::StorageUnavailable)
        ));
        ack.send(()).unwrap();
        drain.await.unwrap();
        assert!(store.healthy());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn failure_is_globally_poisoned_before_receiver_observes_the_result() {
        use std::sync::{Condvar, atomic::AtomicBool};
        struct MarkFence {
            entered: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
            release: Arc<(Mutex<bool>, Condvar)>,
            marked: Arc<AtomicBool>,
        }
        impl openlegal_application::durability::DurabilityFence for MarkFence {
            fn check(
                &self,
                _: tokio::time::Instant,
                _: CancellationToken,
            ) -> BoxFuture<'static, Result<(), RetrievalError>> {
                Box::pin(async { Err(RetrievalError::StorageUnavailable) })
            }
            fn mark_uncertain(&self) {
                if let Some(entered) = self.entered.lock().unwrap().take() {
                    let _ = entered.send(());
                }
                let (lock, wake) = &*self.release;
                let mut released = lock.lock().unwrap();
                while !*released {
                    released = wake.wait(released).unwrap();
                }
                self.marked.store(true, Ordering::SeqCst);
            }
        }
        struct Release(Arc<(Mutex<bool>, Condvar)>);
        impl Drop for Release {
            fn drop(&mut self) {
                let (lock, wake) = &*self.0;
                *lock.lock().unwrap() = true;
                wake.notify_all();
            }
        }
        let release = Arc::new((Mutex::new(false), Condvar::new()));
        let cleanup = Release(release.clone());
        let marked = Arc::new(AtomicBool::new(false));
        let (entered, ready) = tokio::sync::oneshot::channel();
        let mut store = disconnected_store();
        store.durability_fence = Some(Arc::new(MarkFence {
            entered: Mutex::new(Some(entered)),
            release,
            marked: marked.clone(),
        }));
        let mut caller = Box::pin(store.owned_commit(async { Ok(()) }));
        assert!(futures::poll!(caller.as_mut()).is_pending());
        ready.await.unwrap();
        // This is a deterministic receiver poll while the poison callback is
        // blocked, not a timing assertion about task scheduling.
        assert!(futures::poll!(caller.as_mut()).is_pending());
        assert!(!marked.load(Ordering::SeqCst));
        drop(cleanup);
        assert_eq!(caller.await, Err(DatabaseError::StorageUnavailable));
        assert!(marked.load(Ordering::SeqCst));
        assert!(store.availability.uncertain.load(Ordering::Acquire));
        store.drain_storage_work().await.unwrap_err();
    }

    #[tokio::test]
    async fn mutation_cancellation_before_commit_is_not_uncertain_and_blocks_recovery_until_drain()
    {
        let store = disconnected_store();
        let mutation = store.availability.enter().unwrap();
        store.availability.fail();
        let generation = store.availability.generation.load(Ordering::Acquire);
        assert_eq!(
            store
                .availability
                .complete(generation, &CancellationToken::new()),
            Err(DatabaseError::StorageUnavailable)
        );
        drop(mutation);
        store
            .availability
            .complete(generation, &CancellationToken::new())
            .unwrap();
        assert!(store.healthy());
    }

    #[tokio::test]
    async fn detached_ack_capacity_refuses_before_submission_without_poison() {
        let mut store = disconnected_store();
        store.pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect_lazy("postgres://fixture:fixture@localhost/unused")
            .unwrap();
        let (ack, wait) = tokio::sync::oneshot::channel();
        let mut first = Box::pin(store.owned_commit(async move {
            wait.await.unwrap();
            Ok(())
        }));
        assert!(futures::poll!(first.as_mut()).is_pending());
        let submitted = Arc::new(AtomicUsize::new(0));
        for _ in 0..16 {
            let submitted = submitted.clone();
            assert_eq!(
                store
                    .owned_commit(async move {
                        submitted.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    })
                    .await,
                Err(DatabaseError::StorageContended)
            );
        }
        assert_eq!(submitted.load(Ordering::SeqCst), 0);
        assert!(store.healthy());
        assert_eq!(store.availability.work.lock().unwrap().commits, 1);
        ack.send(()).unwrap();
        first.await.unwrap();
        store.drain_storage_work().await.unwrap();
        assert_eq!(store.availability.work.lock().unwrap().commits, 0);
    }

    struct PendingFence;
    impl openlegal_application::durability::DurabilityFence for PendingFence {
        fn check(
            &self,
            _: tokio::time::Instant,
            _: CancellationToken,
        ) -> BoxFuture<'static, Result<(), RetrievalError>> {
            Box::pin(std::future::pending())
        }
    }

    #[tokio::test(start_paused = true)]
    async fn dropped_public_receiver_retains_postcommit_fence_and_real_timeout_is_sticky() {
        for waiting_for_commit in [true, false] {
            let mut store = disconnected_store();
            store.durability_fence = Some(Arc::new(PendingFence));
            let mut request = Box::pin(store.owned_commit(async move {
                if waiting_for_commit {
                    std::future::pending::<()>().await;
                }
                Ok(())
            }));
            assert!(futures::poll!(request.as_mut()).is_pending());
            tokio::task::yield_now().await;
            drop(request);
            assert!(store.healthy());
            tokio::time::advance(Duration::from_secs(2)).await;
            assert_eq!(
                store.drain_storage_work().await,
                Err(DatabaseError::StorageUnavailable)
            );
            assert!(!store.healthy());
        }
    }

    #[tokio::test]
    #[ignore = "requires scripts/test-postgres.sh"]
    async fn a_postcommit_peer_failure_is_sticky_without_replaying_the_commit() {
        let fixture = crate::test_support::TestDatabase::new().await;
        let base = fixture.open(100).await;
        let mut store = PgCorpusStore::new_with_persistence(&base, Arc::new(ProbeBlob::default()));
        let calls = Arc::new(AtomicUsize::new(0));
        store.durability_fence = Some(Arc::new(FailedFence(calls.clone())));
        let mut tx = begin_storage(&store.pool, "fixture_postcommit")
            .await
            .unwrap();
        sqlx::query("UPDATE openlegal.corpus_control SET max_raw_bytes=1024 WHERE singleton")
            .execute(&mut *tx)
            .await
            .unwrap();
        assert_eq!(
            store.commit_storage(tx, "fixture_postcommit").await,
            Err(DatabaseError::StorageUnavailable)
        );
        let value: String = sqlx::query_scalar(
            "SELECT max_raw_bytes::text FROM openlegal.corpus_control WHERE singleton",
        )
        .fetch_one(&base.pool())
        .await
        .unwrap();
        assert_eq!(value, "1024");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(matches!(
            store.probe_health(None, CancellationToken::new()).await,
            Err(CorpusHealthFailure::Fatal(
                DatabaseError::StorageUnavailable
            ))
        ));
    }

    struct RejectThirdFence(Arc<AtomicUsize>);
    impl openlegal_application::durability::DurabilityFence for RejectThirdFence {
        fn check(
            &self,
            _: tokio::time::Instant,
            _: CancellationToken,
        ) -> BoxFuture<'static, Result<(), RetrievalError>> {
            let call = self.0.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move {
                if call == 2 {
                    Err(RetrievalError::StorageUnavailable)
                } else {
                    Ok(())
                }
            })
        }
    }

    #[tokio::test]
    #[ignore = "requires scripts/test-postgres.sh"]
    async fn peer_must_know_the_tombstone_before_any_physical_unlink() {
        let fixture = crate::test_support::TestDatabase::new().await;
        let base = fixture.open(100).await;
        let blobs = Arc::new(ProbeBlob::default());
        let mut store = PgCorpusStore::new_with_persistence(&base, blobs.clone());
        let calls = Arc::new(AtomicUsize::new(0));
        store.durability_fence = Some(Arc::new(RejectThirdFence(calls.clone())));
        sqlx::query("INSERT INTO openlegal.corpus_blob_deletion(storage_key,raw_sha256,raw_size) VALUES($1,$2,1)")
            .bind(format!("00/{}-00000000-0000-0000-0000-000000000001", "0".repeat(64)))
            .bind(vec![0_u8;32]).execute(&base.pool()).await.unwrap();
        assert_eq!(
            store.maintain(100, 0).await,
            Err(DatabaseError::StorageUnavailable)
        );
        assert_eq!(calls.load(Ordering::SeqCst), 3);
        assert_eq!(blobs.deletions.load(Ordering::SeqCst), 0);
        let queued: i64 = sqlx::query_scalar("SELECT count(*) FROM openlegal.corpus_blob_deletion")
            .fetch_one(&base.pool())
            .await
            .unwrap();
        assert_eq!(queued, 1);
    }

    #[tokio::test]
    #[ignore = "requires scripts/test-postgres.sh"]
    async fn confirmed_probe_outage_recovers_only_after_schema_and_index_checks() {
        let fixture = crate::test_support::TestDatabase::new().await;
        let base = fixture.open(100).await;
        let blobs = Arc::new(ProbeBlob::default());
        let store = PgCorpusStore::new_with_persistence(&base, blobs.clone());
        store.health().await.unwrap();
        *blobs.error.lock().unwrap() = Some(RetrievalError::StorageUnavailable);
        assert!(matches!(
            store.probe_health(Some(0), CancellationToken::new()).await,
            Err(CorpusHealthFailure::Availability(
                DatabaseError::StorageUnavailable
            ))
        ));
        assert!(!store.healthy());
        *blobs.error.lock().unwrap() = None;
        let proof = store
            .probe_health(Some(0), CancellationToken::new())
            .await
            .unwrap();
        assert!(
            !store.healthy(),
            "probe completion alone is not a lease acknowledgement"
        );
        store.complete_health(proof).unwrap();
        assert!(store.healthy());
        store.availability.fail();
        assert!(matches!(
            store.probe_health(Some(1), CancellationToken::new()).await,
            Err(CorpusHealthFailure::Fatal(DatabaseError::StorageCorrupt))
        ));
        assert!(!store.healthy());
        assert_eq!(store.gate().await, Err(DatabaseError::StorageCorrupt));
    }

    #[tokio::test]
    #[ignore = "requires scripts/test-postgres.sh"]
    async fn public_health_queues_behind_background_then_checks_its_own_sql_and_blob() {
        let fixture = crate::test_support::TestDatabase::new().await;
        let base = fixture.open(100).await;
        let blobs = Arc::new(ProbeBlob::default());
        let store = PgCorpusStore::new_with_persistence(&base, blobs.clone());
        let pause = Arc::new(ProbePause {
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::Semaphore::new(0),
        });
        *blobs.pause.lock().unwrap() = Some(pause.clone());
        let background_store = store.clone();
        let background = tokio::spawn(async move {
            background_store
                .probe_health(None, CancellationToken::new())
                .await
        });
        tokio::time::timeout(Duration::from_secs(5), pause.entered.notified())
            .await
            .expect("background reaches its blocking Blob health proof");
        assert_eq!(blobs.calls.load(Ordering::SeqCst), 1);
        let mut public = Box::pin(store.health());
        assert!(futures::poll!(public.as_mut()).is_pending());
        assert!(matches!(
            store.probe_health(None, CancellationToken::new()).await,
            Err(CorpusHealthFailure::Contended)
        ));
        assert!(store.healthy());
        assert_eq!(blobs.calls.load(Ordering::SeqCst), 1);

        // Once the background finishes, a cached success would bypass this
        // newly acquired SQL blocker. The public caller must perform its own
        // SELECT on the same single reserved probe connection before Blob IO.
        let mut blocker = base.pool().begin().await.unwrap();
        sqlx::query("LOCK TABLE openlegal.corpus_control IN ACCESS EXCLUSIVE MODE")
            .execute(&mut *blocker)
            .await
            .unwrap();
        *blobs.pause.lock().unwrap() = None;
        pause.release.add_permits(1);
        store
            .complete_health(background.await.unwrap().unwrap())
            .unwrap();
        let blocked_query = async {
            loop {
                let waiting: i64 = sqlx::query_scalar(
                    "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() \
                     AND pid<>pg_backend_pid() AND wait_event_type='Lock' \
                     AND query='SELECT singleton FROM openlegal.corpus_control'",
                )
                .fetch_one(&base.pool())
                .await
                .unwrap();
                if waiting == 1 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        };
        tokio::select! {
            result = public.as_mut() => panic!("public probe skipped its own locked SQL: {result:?}"),
            result = tokio::time::timeout(Duration::from_secs(3), blocked_query) =>
                result.expect("one public SQL health probe waits on the fixture lock"),
        }
        assert_eq!(store.probe_pool.options().get_max_connections(), 1);
        assert_eq!(store.probe_pool.size(), 1);
        assert_eq!(blobs.calls.load(Ordering::SeqCst), 1);
        blocker.rollback().await.unwrap();
        public.await.unwrap();
        assert_eq!(blobs.calls.load(Ordering::SeqCst), 2);
        assert!(store.healthy());

        // A fresh failure after a successful background/public proof must not
        // be hidden by caching, and genuine dependency failure still degrades.
        *blobs.error.lock().unwrap() = Some(RetrievalError::StorageUnavailable);
        assert_eq!(store.health().await, Err(DatabaseError::StorageUnavailable));
        assert_eq!(blobs.calls.load(Ordering::SeqCst), 3);
        assert!(!store.healthy());
        assert_eq!(store.gate().await, Err(DatabaseError::StorageUnavailable));
        assert!(!store.availability.uncertain.load(Ordering::Acquire));
    }

    #[tokio::test]
    #[ignore = "requires scripts/test-postgres.sh"]
    async fn internal_probe_failure_never_becomes_retryable_availability() {
        let fixture = crate::test_support::TestDatabase::new().await;
        let base = fixture.open(100).await;
        let blobs = Arc::new(ProbeBlob::default());
        *blobs.error.lock().unwrap() = Some(RetrievalError::Internal);
        let store = PgCorpusStore::new_with_persistence(&base, blobs);
        assert!(matches!(
            store.probe_health(None, CancellationToken::new()).await,
            Err(CorpusHealthFailure::Fatal(
                DatabaseError::StorageUnavailable
            ))
        ));
    }
}
