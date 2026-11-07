//! Immutable physical blob generations. PostgreSQL owns every authoritative reference.
//!
//! Blocking jobs retain their admission permits until the actual filesystem call
//! completes, including after an awaiting caller disappears. A deadline cannot
//! interrupt kernel I/O; it closes healthy admission until a successful probe.
use futures::future::BoxFuture;
use openlegal_application::blob::{
    BlobLocation, BlobMetrics, BlobPage, BlobPutResult, BlobRootIdentity, BlobStore,
};
use openlegal_domain::RetrievalError as Error;
use std::{
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::sync::{Notify, Semaphore, watch};
use tokio_util::sync::CancellationToken;

mod filesystem;
use filesystem::Filesystem;
mod mirrored;
pub use mirrored::MirroredBlobStore;

const JOBS: usize = 16;
const HEALTH_WAITERS: usize = 16;
const DEADLINE: Duration = Duration::from_secs(5);
const MAX_BATCH: usize = 128;
static STARTUPS: Semaphore = Semaphore::const_new(JOBS);

// Unrelated libtest runtimes can otherwise make real fsync fixtures exhaust
// their 2s/5s production deadlines. Hold this TEST-ONLY permit for a full test,
// including recovery/close, rather than relaxing any runtime operation budget.
// Within-test parallel jobs, both mirror legs, and 64-peer guards remain real.
#[cfg(test)]
static TEST_IO_ADMISSION: Semaphore = Semaphore::const_new(1);
#[cfg(test)]
pub(crate) async fn test_io_admission() -> tokio::sync::SemaphorePermit<'static> {
    TEST_IO_ADMISSION
        .acquire()
        .await
        .expect("test admission is never closed")
}
#[cfg(test)]
pub(crate) fn test_io_admission_blocking() -> tokio::sync::SemaphorePermit<'static> {
    futures::executor::block_on(test_io_admission())
}

#[derive(Default)]
struct Counters {
    reads: AtomicU64,
    writes: AtomicU64,
    deduplicated_puts: AtomicU64,
    deletions: AtomicU64,
    failures: AtomicU64,
    corruptions: AtomicU64,
    saturation: AtomicU64,
    active_jobs: AtomicU64,
    queued_jobs: AtomicU64,
    started_jobs: AtomicU64,
    queue_wait_us: AtomicU64,
    filesystem_work_us: AtomicU64,
}

#[derive(Default)]
struct BootstrapProgress {
    phase: AtomicU64,
    queued_us: AtomicU64,
    open_us: AtomicU64,
    health_us: AtomicU64,
    expired: AtomicBool,
}
impl BootstrapProgress {
    fn phase_name(&self) -> &'static str {
        match self.phase.load(Ordering::Acquire) {
            0 => "queued",
            1 => "open",
            2 => "health",
            _ => "complete",
        }
    }
    fn failure(&self, kind: &'static str, started: std::time::Instant, budget: Duration) {
        #[cfg(test)]
        eprintln!(
            "blob_bootstrap failure_kind={} phase={} elapsed_us={} budget_ms={} queued_us={} open_us={} health_us={}",
            kind,
            self.phase_name(),
            elapsed_us(started),
            budget.as_millis(),
            self.queued_us.load(Ordering::Acquire),
            self.open_us.load(Ordering::Acquire),
            self.health_us.load(Ordering::Acquire)
        );
        tracing::warn!(
            target: "openlegal_adapters::blob",
            operation = "blob_bootstrap",
            failure_kind = kind,
            phase = self.phase_name(),
            elapsed_us = elapsed_us(started),
            bootstrap_timeout_ms = budget.as_millis() as u64,
            queued_us = self.queued_us.load(Ordering::Acquire),
            open_us = self.open_us.load(Ordering::Acquire),
            health_us = self.health_us.load(Ordering::Acquire),
            "blob bootstrap failed"
        );
    }
}

fn elapsed_us(start: std::time::Instant) -> u64 {
    start.elapsed().as_micros().min(999_999_999_999) as u64
}
fn add_timing(counter: &AtomicU64, value: u64) {
    let _ = counter.try_update(Ordering::Relaxed, Ordering::Relaxed, |old| {
        Some(old.saturating_add(value).min(999_999_999_999))
    });
}

#[derive(Clone, Copy)]
struct HealthOutcome {
    result: Result<(), Error>,
    generation: u64,
    recovering: bool,
}

struct HealthFlight {
    outcome: watch::Receiver<Option<HealthOutcome>>,
}

struct Shared {
    files: Filesystem,
    max_object_bytes: usize,
    operation_timeout: Duration,
    slots: Arc<Semaphore>,
    probe_slots: Arc<Semaphore>,
    health_waiters: Semaphore,
    health_flight: Mutex<Option<Arc<HealthFlight>>>,
    closed: AtomicBool,
    healthy: AtomicBool,
    counters: Counters,
    completed: Notify,
    admission: Mutex<()>,
    failure_epoch: AtomicU64,
    #[cfg(test)]
    health_hook: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
}

fn failed(shared: &Shared, error: Error) {
    // Serialize failure publication with successful recovery and shutdown.
    let _admission = shared.admission.lock().ok();
    shared.failure_epoch.fetch_add(1, Ordering::AcqRel);
    shared.healthy.store(false, Ordering::Release);
    shared.counters.failures.fetch_add(1, Ordering::Relaxed);
    if error == Error::StorageCorrupt {
        shared.counters.corruptions.fetch_add(1, Ordering::Relaxed);
    }
    shared.completed.notify_waiters();
}

/// Dedicated-directory immutable blob adapter; no query indexes or history files.
pub struct FsBlobStore {
    shared: Arc<Shared>,
}

impl FsBlobStore {
    /// Opens a private absolute directory and probes the required durability APIs.
    pub async fn open(path: &Path) -> Result<Arc<Self>, Error> {
        Self::open_with_limit(path, openlegal_application::MAX_RAW_BYTES).await
    }

    /// A separate dedicated corpus root may hold larger evidence objects.
    pub async fn open_with_limit(path: &Path, max_object_bytes: usize) -> Result<Arc<Self>, Error> {
        Self::open_with_limit_and_timeout(path, max_object_bytes, DEADLINE).await
    }

    /// Opens a dedicated root with a positive operation budget of at most 60 seconds.
    ///
    /// The budget covers startup, each owned blocking job (including health), and
    /// close drainage. Expiry does not interrupt kernel I/O or release its admission
    /// permit; late completion cannot restore health without a successful probe.
    pub async fn open_with_limit_and_timeout(
        path: &Path,
        max_object_bytes: usize,
        operation_timeout: Duration,
    ) -> Result<Arc<Self>, Error> {
        if !(1..=100 * 1024 * 1024).contains(&max_object_bytes)
            || operation_timeout.is_zero()
            || operation_timeout > Duration::from_secs(60)
        {
            return Err(Error::InvalidInput);
        }
        let path = path.to_owned();
        let startup = STARTUPS.try_acquire().map_err(|_| Error::Busy)?;
        // Bootstrap is one owned job, independent of the application runtime locks.
        let started = std::time::Instant::now();
        let progress = Arc::new(BootstrapProgress::default());
        let worker_progress = progress.clone();
        let task = tokio::task::spawn_blocking(move || {
            let _startup = startup;
            worker_progress
                .queued_us
                .store(elapsed_us(started), Ordering::Release);
            worker_progress.phase.store(1, Ordering::Release);
            let open_started = std::time::Instant::now();
            let opened = Filesystem::open_with_limit(&path, max_object_bytes);
            worker_progress
                .open_us
                .store(elapsed_us(open_started), Ordering::Release);
            let result = opened.and_then(|files| {
                worker_progress.phase.store(2, Ordering::Release);
                let health_started = std::time::Instant::now();
                let health = files.health();
                worker_progress
                    .health_us
                    .store(elapsed_us(health_started), Ordering::Release);
                health?;
                worker_progress.phase.store(3, Ordering::Release);
                Ok(files)
            });
            if worker_progress.expired.load(Ordering::Acquire) {
                worker_progress.failure(
                    "physical_completion_after_deadline",
                    started,
                    operation_timeout,
                );
            }
            result
        });
        let files = match tokio::time::timeout(operation_timeout, task).await {
            Err(_) => {
                progress.expired.store(true, Ordering::Release);
                progress.failure("logical_deadline", started, operation_timeout);
                return Err(Error::StorageUnavailable);
            }
            Ok(Err(_)) => {
                progress.failure("worker_join", started, operation_timeout);
                return Err(Error::StorageUnavailable);
            }
            Ok(Ok(Err(error))) => {
                progress.failure("physical_error", started, operation_timeout);
                return Err(error);
            }
            Ok(Ok(Ok(files))) => files,
        };
        Ok(Arc::new(Self {
            shared: Arc::new(Shared {
                files,
                max_object_bytes,
                operation_timeout,
                slots: Arc::new(Semaphore::new(JOBS)),
                probe_slots: Arc::new(Semaphore::new(1)),
                health_waiters: Semaphore::new(HEALTH_WAITERS),
                health_flight: Mutex::new(None),
                closed: AtomicBool::new(false),
                healthy: AtomicBool::new(true),
                counters: Counters::default(),
                completed: Notify::new(),
                admission: Mutex::new(()),
                failure_epoch: AtomicU64::new(0),
                #[cfg(test)]
                health_hook: Mutex::new(None),
            }),
        }))
    }

    fn job<T: Send + 'static>(
        &self,
        cancellation: CancellationToken,
        probe: bool,
        action: impl FnOnce(&Filesystem, &CancellationToken, &Counters) -> Result<T, Error>
        + Send
        + 'static,
    ) -> BoxFuture<'static, Result<T, Error>> {
        let shared = self.shared.clone();
        Box::pin(async move {
            if cancellation.is_cancelled() {
                return Err(Error::Cancelled);
            }
            let permit = {
                let _admission = shared.admission.lock().map_err(|_| Error::Internal)?;
                if shared.closed.load(Ordering::Acquire) {
                    return Err(Error::Shutdown);
                }
                if !probe && !shared.healthy.load(Ordering::Acquire) {
                    return Err(Error::StorageUnavailable);
                }
                let slots = if probe {
                    &shared.probe_slots
                } else {
                    &shared.slots
                };
                let permit = slots.clone().try_acquire_owned().map_err(|_| {
                    shared.counters.saturation.fetch_add(1, Ordering::Relaxed);
                    Error::Busy
                })?;
                shared.counters.active_jobs.fetch_add(1, Ordering::AcqRel);
                permit
            };
            let token = cancellation.child_token();
            let _cancel_on_drop = token.clone().drop_guard();
            let worker = shared.clone();
            let worker_token = token.clone();
            let queued = std::time::Instant::now();
            worker.counters.queued_jobs.fetch_add(1, Ordering::Relaxed);
            let task = tokio::task::spawn_blocking(move || {
                worker.counters.queued_jobs.fetch_sub(1, Ordering::Relaxed);
                worker.counters.started_jobs.fetch_add(1, Ordering::Relaxed);
                add_timing(&worker.counters.queue_wait_us, elapsed_us(queued));
                struct Finished(Arc<Shared>);
                impl Drop for Finished {
                    fn drop(&mut self) {
                        self.0.counters.active_jobs.fetch_sub(1, Ordering::AcqRel);
                        self.0.completed.notify_waiters();
                    }
                }
                let _finished = Finished(worker.clone());
                let _permit = permit;
                struct FilesystemTiming(Arc<Shared>, std::time::Instant);
                impl Drop for FilesystemTiming {
                    fn drop(&mut self) {
                        add_timing(&self.0.counters.filesystem_work_us, elapsed_us(self.1));
                    }
                }
                let _timing = FilesystemTiming(worker.clone(), std::time::Instant::now());
                let result = if worker_token.is_cancelled() {
                    Err(Error::Cancelled)
                } else {
                    action(&worker.files, &worker_token, &worker.counters)
                };
                if let Err(error) = result.as_ref()
                    && !matches!(
                        error,
                        Error::Cancelled | Error::InvalidInput | Error::Busy | Error::Shutdown
                    )
                {
                    #[cfg(test)]
                    eprintln!(
                        "blob_owned failure_kind=physical_error error_category={error:?} queue_us={} active_jobs={}",
                        worker.counters.queue_wait_us.load(Ordering::Acquire),
                        worker.counters.active_jobs.load(Ordering::Acquire)
                    );
                    failed(&worker, *error);
                }
                result
            });
            let (reply, completion) = tokio::sync::oneshot::channel();
            // The deadline supervisor is owned independently of the requesting
            // future, so dropping a waiter cannot disable its health watchdog.
            tokio::spawn(async move {
                let started = tokio::time::Instant::now();
                let result = match tokio::time::timeout(shared.operation_timeout, task).await {
                    Ok(Ok(result)) => result,
                    Ok(Err(_)) => {
                        token.cancel();
                        failed(&shared, Error::StorageUnavailable);
                        #[cfg(test)]
                        eprintln!(
                            "blob_owned failure_kind=worker_join probe={} elapsed_ms={} budget_ms={}",
                            probe,
                            started.elapsed().as_millis(),
                            shared.operation_timeout.as_millis()
                        );
                        tracing::warn!(
                            operation = if probe { "blob_health" } else { "blob_job" },
                            error_category = "worker_join",
                            elapsed_ms = started.elapsed().as_millis().min(999_999_999_999) as u64,
                            operation_timeout_ms = shared.operation_timeout.as_millis() as u64,
                            probe,
                            "blob owned job failed"
                        );
                        Err(Error::StorageUnavailable)
                    }
                    Err(_) => {
                        token.cancel();
                        failed(&shared, Error::StorageUnavailable);
                        #[cfg(test)]
                        eprintln!(
                            "blob_owned failure_kind=logical_deadline probe={} elapsed_ms={} budget_ms={} queued_jobs={} active_jobs={}",
                            probe,
                            started.elapsed().as_millis(),
                            shared.operation_timeout.as_millis(),
                            shared.counters.queued_jobs.load(Ordering::Acquire),
                            shared.counters.active_jobs.load(Ordering::Acquire)
                        );
                        tracing::warn!(
                            operation = if probe { "blob_health" } else { "blob_job" },
                            error_category = "local_deadline",
                            elapsed_ms = started.elapsed().as_millis().min(999_999_999_999) as u64,
                            operation_timeout_ms = shared.operation_timeout.as_millis() as u64,
                            probe,
                            "blob owned job failed"
                        );
                        Err(Error::StorageUnavailable)
                    }
                };
                let _ = reply.send(result);
            });
            tokio::select! { biased;
                _ = cancellation.cancelled() => Err(Error::Cancelled),
                result = completion => result.unwrap_or(Err(Error::StorageUnavailable)),
            }
        })
    }

    // Internal owned supervisors retain their aggregate permits until physical
    // completion. This deliberately has no new watchdog or caller-facing budget.
    fn physical_drain(&self) -> BoxFuture<'static, ()> {
        let shared = self.shared.clone();
        Box::pin(async move {
            loop {
                let notified = shared.completed.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if shared.counters.active_jobs.load(Ordering::Acquire) == 0 {
                    return;
                }
                notified.await;
            }
        })
    }
}

impl BlobStore for FsBlobStore {
    fn root_identities(&self) -> Option<Vec<BlobRootIdentity>> {
        Some(vec![self.shared.files.cached_root_identity()])
    }
    fn put_if_absent(
        &self,
        location: BlobLocation,
        bytes: Vec<u8>,
        cancellation: CancellationToken,
    ) -> BoxFuture<'static, Result<BlobPutResult, Error>> {
        if bytes.len() > self.shared.max_object_bytes {
            return Box::pin(async { Err(Error::InvalidInput) });
        }
        self.job(cancellation, false, move |files, token, counters| {
            let result = files.put(&location, &bytes, token)?;
            match result {
                BlobPutResult::Created => {
                    counters.writes.fetch_add(1, Ordering::Relaxed);
                }
                BlobPutResult::AlreadyPresent => {
                    counters.deduplicated_puts.fetch_add(1, Ordering::Relaxed);
                }
            }
            Ok(result)
        })
    }
    fn get(
        &self,
        location: BlobLocation,
        cancellation: CancellationToken,
    ) -> BoxFuture<'static, Result<Option<Vec<u8>>, Error>> {
        self.job(cancellation, false, move |files, token, counters| {
            check_cancel(token)?;
            let value = files.get(&location)?;
            counters.reads.fetch_add(1, Ordering::Relaxed);
            Ok(value)
        })
    }
    fn confirm_durable(
        &self,
        location: BlobLocation,
        cancellation: CancellationToken,
    ) -> BoxFuture<'static, Result<(), Error>> {
        self.job(cancellation, false, move |files, token, _| {
            check_cancel(token)?;
            files.confirm_durable(&location)?;
            check_cancel(token)
        })
    }
    fn delete_if_present(
        &self,
        location: BlobLocation,
        cancellation: CancellationToken,
    ) -> BoxFuture<'static, Result<(), Error>> {
        self.job(cancellation, false, move |files, token, counters| {
            check_cancel(token)?;
            if files.delete(&location)? {
                counters.deletions.fetch_add(1, Ordering::Relaxed);
            }
            Ok(())
        })
    }
    fn enumerate(
        &self,
        cursor: Option<String>,
        limit: usize,
        cancellation: CancellationToken,
    ) -> BoxFuture<'static, Result<BlobPage, Error>> {
        self.job(cancellation, false, move |files, token, _| {
            files.enumerate(cursor, limit, token)
        })
    }
    fn cleanup_staging(
        &self,
        now: u64,
        limit: usize,
        cancellation: CancellationToken,
    ) -> BoxFuture<'static, Result<usize, Error>> {
        self.job(cancellation, false, move |files, token, _| {
            files.cleanup_staging(now, limit, token)
        })
    }
    fn health(&self, cancellation: CancellationToken) -> BoxFuture<'static, Result<(), Error>> {
        let shared = self.shared.clone();
        Box::pin(async move {
            check_cancel(&cancellation)?;
            let _waiter = shared.health_waiters.try_acquire().map_err(|_| {
                shared.counters.saturation.fetch_add(1, Ordering::Relaxed);
                Error::Busy
            })?;
            let mut outcome = {
                let _admission = shared.admission.lock().map_err(|_| Error::Internal)?;
                if shared.closed.load(Ordering::Acquire) {
                    return Err(Error::Shutdown);
                }
                let mut active = shared.health_flight.lock().map_err(|_| Error::Internal)?;
                if let Some(flight) = active.as_ref() {
                    flight.outcome.clone()
                } else {
                    let generation = shared.failure_epoch.load(Ordering::Acquire);
                    let recovering = !shared.healthy.load(Ordering::Acquire);
                    let (sender, receiver) = watch::channel(None);
                    let flight = Arc::new(HealthFlight {
                        outcome: receiver.clone(),
                    });
                    *active = Some(flight.clone());
                    let worker = shared.clone();
                    // The shared probe and watchdog outlive individual waiters.
                    // Only a still-awaited result below may restore admission.
                    tokio::spawn(async move {
                        let store = FsBlobStore {
                            shared: worker.clone(),
                        };
                        let probe_shared = worker.clone();
                        let result = store
                            .job(CancellationToken::new(), true, move |files, token, _| {
                                check_cancel(token)?;
                                {
                                    let _admission = probe_shared
                                        .admission
                                        .lock()
                                        .map_err(|_| Error::Internal)?;
                                    if probe_shared.failure_epoch.load(Ordering::Acquire)
                                        != generation
                                    {
                                        return Err(Error::StorageUnavailable);
                                    }
                                    if recovering
                                        && !probe_shared.healthy.load(Ordering::Acquire)
                                        && probe_shared.counters.active_jobs.load(Ordering::Acquire)
                                            != 1
                                    {
                                        return Err(Error::Busy);
                                    }
                                }
                                files.health()?;
                                #[cfg(test)]
                                {
                                    let hook = probe_shared
                                        .health_hook
                                        .lock()
                                        .map_err(|_| Error::Internal)?
                                        .clone();
                                    if let Some(hook) = hook {
                                        hook();
                                    }
                                }
                                check_cancel(token)?;
                                Ok(())
                            })
                            .await;
                        let result = match worker.health_flight.lock() {
                            Ok(mut active) => {
                                if active
                                    .as_ref()
                                    .is_some_and(|current| Arc::ptr_eq(current, &flight))
                                {
                                    *active = None;
                                }
                                result
                            }
                            Err(_) => Err(Error::Internal),
                        };
                        // The coordinator alone owns the sender, so unexpected
                        // termination closes the channel instead of stranding waiters.
                        sender.send_replace(Some(HealthOutcome {
                            result,
                            generation,
                            recovering,
                        }));
                    });
                    receiver
                }
            };
            let HealthOutcome {
                result,
                generation,
                recovering,
            } = loop {
                if let Some(result) = *outcome.borrow_and_update() {
                    break result;
                }
                tokio::select! { biased;
                    _ = cancellation.cancelled() => return Err(Error::Cancelled),
                    changed = outcome.changed() => changed.map_err(|_| Error::StorageUnavailable)?,
                }
            };
            check_cancel(&cancellation)?;
            result?;
            let _admission = shared.admission.lock().map_err(|_| Error::Internal)?;
            if shared.closed.load(Ordering::Acquire) {
                return Err(Error::Shutdown);
            }
            if cancellation.is_cancelled() {
                return Err(Error::Cancelled);
            }
            if shared.failure_epoch.load(Ordering::Acquire) != generation {
                return Err(Error::StorageUnavailable);
            }
            // Recovery proved older jobs drained before probing. While unhealthy,
            // admission forbids new ordinary jobs; an unchanged failure epoch
            // preserves that proof. New probes or work admitted by another
            // successful waiter of this epoch do not invalidate recovery.
            if recovering {
                shared.healthy.store(true, Ordering::Release);
            }
            Ok(())
        })
    }
    fn close(&self) -> BoxFuture<'static, Result<(), Error>> {
        let shared = self.shared.clone();
        match shared.admission.lock() {
            Ok(_admission) => {
                shared.closed.store(true, Ordering::Release);
                shared.healthy.store(false, Ordering::Release);
            }
            Err(_) => return Box::pin(async { Err(Error::Internal) }),
        }
        Box::pin(async move {
            tokio::time::timeout(shared.operation_timeout, async {
                loop {
                    let notified = shared.completed.notified();
                    tokio::pin!(notified);
                    notified.as_mut().enable();
                    if shared.counters.active_jobs.load(Ordering::Acquire) == 0 {
                        return;
                    }
                    notified.await;
                }
            })
            .await
            .map_err(|_| Error::StorageUnavailable)
        })
    }
    fn metrics(&self) -> BlobMetrics {
        let c = &self.shared.counters;
        BlobMetrics {
            reads: c.reads.load(Ordering::Relaxed),
            writes: c.writes.load(Ordering::Relaxed),
            deduplicated_puts: c.deduplicated_puts.load(Ordering::Relaxed),
            deletions: c.deletions.load(Ordering::Relaxed),
            failures: c.failures.load(Ordering::Relaxed),
            corruptions: c.corruptions.load(Ordering::Relaxed),
            saturation: c.saturation.load(Ordering::Relaxed),
            active_jobs: c.active_jobs.load(Ordering::Relaxed),
            queued_jobs: c.queued_jobs.load(Ordering::Relaxed),
            started_jobs: c.started_jobs.load(Ordering::Relaxed),
            queue_wait_us: c.queue_wait_us.load(Ordering::Relaxed),
            filesystem_work_us: c.filesystem_work_us.load(Ordering::Relaxed),
        }
    }
}

fn check_cancel(token: &CancellationToken) -> Result<(), Error> {
    if token.is_cancelled() {
        Err(Error::Cancelled)
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests;
