//! Immutable physical blob generations. PostgreSQL owns every authoritative reference.
//!
//! Blocking jobs retain their admission permits until the actual filesystem call
//! completes, including after an awaiting caller disappears. A deadline cannot
//! interrupt kernel I/O; it closes healthy admission until a successful probe.
use futures::future::BoxFuture;
use openlegal_application::blob::{BlobLocation, BlobMetrics, BlobPage, BlobPutResult, BlobStore};
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

const JOBS: usize = 16;
const HEALTH_WAITERS: usize = 16;
const DEADLINE: Duration = Duration::from_secs(5);
const MAX_BATCH: usize = 128;
static STARTUPS: Semaphore = Semaphore::const_new(JOBS);

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
        if !(1..=100 * 1024 * 1024).contains(&max_object_bytes) {
            return Err(Error::InvalidInput);
        }
        let path = path.to_owned();
        let startup = STARTUPS.try_acquire().map_err(|_| Error::Busy)?;
        // Bootstrap is one owned job, independent of the application runtime locks.
        let files = tokio::time::timeout(
            DEADLINE,
            tokio::task::spawn_blocking(move || {
                let _startup = startup;
                let files = Filesystem::open_with_limit(&path, max_object_bytes)?;
                files.health()?;
                Ok::<_, Error>(files)
            }),
        )
        .await
        .map_err(|_| Error::StorageUnavailable)?
        .map_err(|_| Error::StorageUnavailable)??;
        Ok(Arc::new(Self {
            shared: Arc::new(Shared {
                files,
                max_object_bytes,
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
            let task = tokio::task::spawn_blocking(move || {
                struct Finished(Arc<Shared>);
                impl Drop for Finished {
                    fn drop(&mut self) {
                        self.0.counters.active_jobs.fetch_sub(1, Ordering::AcqRel);
                        self.0.completed.notify_waiters();
                    }
                }
                let _finished = Finished(worker.clone());
                let _permit = permit;
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
                    failed(&worker, *error);
                }
                result
            });
            let (reply, completion) = tokio::sync::oneshot::channel();
            // The deadline supervisor is owned independently of the requesting
            // future, so dropping a waiter cannot disable its health watchdog.
            tokio::spawn(async move {
                let result = match tokio::time::timeout(DEADLINE, task).await {
                    Ok(Ok(result)) => result,
                    Ok(Err(_)) | Err(_) => {
                        token.cancel();
                        failed(&shared, Error::StorageUnavailable);
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
}

impl BlobStore for FsBlobStore {
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
            tokio::time::timeout(DEADLINE, async {
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
