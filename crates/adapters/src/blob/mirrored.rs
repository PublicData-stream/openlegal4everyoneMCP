//! Strict durable mirroring. Partial physical effects are never repaired or read around.
use super::{DEADLINE, Error, FsBlobStore, JOBS, check_cancel};
use futures::{FutureExt, future::BoxFuture};
use openlegal_application::blob::{BlobLocation, BlobMetrics, BlobPage, BlobPutResult, BlobStore};
use std::{
    future::Future,
    os::unix::fs::MetadataExt,
    path::{Component, Path},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::sync::{Notify, Semaphore, watch};
use tokio_util::sync::CancellationToken;

struct Shared {
    primary: Arc<FsBlobStore>,
    mirror: Arc<FsBlobStore>,
    budget: Duration,
    slots: Arc<Semaphore>,
    probe: Arc<Semaphore>,
    active: AtomicU64,
    closed: AtomicBool,
    healthy: AtomicBool,
    generation: AtomicU64,
    admission: Mutex<()>,
    completed: Notify,
    health_waiters: Semaphore,
    health_flight: Mutex<Option<Arc<MirrorFlight>>>,
}

struct MirrorFlight {
    outcome: watch::Receiver<Option<Result<u64, Error>>>,
}

/// One caller-assigned generation is acknowledged only after both roots are durable.
/// Reads/enumeration use the primary only. No fallback, repair or automatic retry exists.
#[derive(Clone)]
pub struct MirroredBlobStore {
    shared: Arc<Shared>,
}

fn pair<A, B>(a: Result<A, Error>, b: Result<B, Error>) -> Result<(A, B), Error> {
    // A second-leg integrity failure must not be hidden by a first-leg outage.
    if matches!(&a, Err(Error::StorageCorrupt)) || matches!(&b, Err(Error::StorageCorrupt)) {
        return Err(Error::StorageCorrupt);
    }
    match (a, b) {
        (Ok(a), Ok(b)) => Ok((a, b)),
        (Err(e), _) | (_, Err(e)) => Err(e),
    }
}
fn failure(shared: &Shared, error: Error) {
    if matches!(
        error,
        Error::Cancelled | Error::Busy | Error::InvalidInput | Error::Shutdown
    ) {
        return;
    }
    let _lock = shared.admission.lock().ok();
    shared.generation.fetch_add(1, Ordering::AcqRel);
    shared.healthy.store(false, Ordering::Release);
}
fn validate_paths(primary: &Path, mirror: &Path) -> Result<(), Error> {
    for path in [primary, mirror] {
        if !path.is_absolute()
            || path
                .components()
                .any(|v| !matches!(v, Component::RootDir | Component::Normal(_)))
        {
            return Err(Error::InvalidInput);
        }
    }
    if primary.starts_with(mirror) || mirror.starts_with(primary) {
        return Err(Error::InvalidInput);
    }
    Ok(())
}
fn distinct_roots(
    primary: &Path,
    mirror: &Path,
    first: (u64, u64),
    second: (u64, u64),
) -> Result<(), Error> {
    let identity = |p: &Path| -> Result<_, Error> {
        let metadata = p.metadata().map_err(|_| Error::StorageUnavailable)?;
        Ok((metadata.dev(), metadata.ino()))
    };
    if identity(primary)? != first || identity(mirror)? != second {
        return Err(Error::StorageUnavailable);
    }
    // Also reject bind aliases of the same root or an ancestor of either root.
    for (path, other) in [(primary, second), (mirror, first)] {
        for ancestor in path.ancestors() {
            if identity(ancestor)? == other {
                return Err(Error::InvalidInput);
            }
        }
    }
    Ok(())
}

impl MirroredBlobStore {
    pub async fn open(primary: &Path, mirror: &Path) -> Result<Arc<Self>, Error> {
        Self::open_with_limit(primary, mirror, openlegal_application::MAX_RAW_BYTES).await
    }
    pub async fn open_with_limit(
        primary: &Path,
        mirror: &Path,
        limit: usize,
    ) -> Result<Arc<Self>, Error> {
        Self::open_with_limit_and_timeout(primary, mirror, limit, DEADLINE).await
    }
    /// The same positive budget applies once across both legs, including startup.
    pub async fn open_with_limit_and_timeout(
        primary: &Path,
        mirror: &Path,
        limit: usize,
        budget: Duration,
    ) -> Result<Arc<Self>, Error> {
        validate_paths(primary, mirror)?;
        if budget.is_zero() || budget > Duration::from_secs(60) {
            return Err(Error::InvalidInput);
        }
        let end = tokio::time::Instant::now() + budget;
        let (a, b) = tokio::time::timeout_at(end, async {
            let (a, b) = tokio::join!(
                FsBlobStore::open_with_limit_and_timeout(primary, limit, budget),
                FsBlobStore::open_with_limit_and_timeout(mirror, limit, budget)
            );
            pair(a, b)
        })
        .await
        .map_err(|_| Error::StorageUnavailable)??;
        let p = primary.to_owned();
        let m = mirror.to_owned();
        let first_files = a.shared.clone();
        let second_files = b.shared.clone();
        let startup = super::STARTUPS.try_acquire().map_err(|_| Error::Busy)?;
        tokio::time::timeout_at(
            end,
            tokio::task::spawn_blocking(move || {
                let _startup = startup;
                distinct_roots(
                    &p,
                    &m,
                    first_files.files.root_identity(),
                    second_files.files.root_identity(),
                )
            }),
        )
        .await
        .map_err(|_| Error::StorageUnavailable)?
        .map_err(|_| Error::StorageUnavailable)??;
        Ok(Arc::new(Self {
            shared: Arc::new(Shared {
                primary: a,
                mirror: b,
                budget,
                slots: Arc::new(Semaphore::new(JOBS)),
                probe: Arc::new(Semaphore::new(1)),
                active: AtomicU64::new(0),
                closed: AtomicBool::new(false),
                healthy: AtomicBool::new(true),
                generation: AtomicU64::new(0),
                admission: Mutex::new(()),
                completed: Notify::new(),
                health_waiters: Semaphore::new(super::HEALTH_WAITERS),
                health_flight: Mutex::new(None),
            }),
        }))
    }
    /// Logical permits may outlive a cancelled waiter until physical work drains.
    pub fn active_operations(&self) -> u64 {
        self.shared.active.load(Ordering::Acquire)
    }
    fn operation<T, F, Fut>(
        &self,
        cancellation: CancellationToken,
        probe: bool,
        action: F,
    ) -> BoxFuture<'static, Result<T, Error>>
    where
        T: Send + 'static,
        F: FnOnce(Arc<Shared>, CancellationToken) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, Error>> + Send + 'static,
    {
        let shared = self.shared.clone();
        Box::pin(async move {
            check_cancel(&cancellation)?;
            let permit = {
                let _lock = shared.admission.lock().map_err(|_| Error::Internal)?;
                if shared.closed.load(Ordering::Acquire) {
                    return Err(Error::Shutdown);
                }
                if !probe && !shared.healthy.load(Ordering::Acquire) {
                    return Err(Error::StorageUnavailable);
                }
                let slots = if probe { &shared.probe } else { &shared.slots };
                let permit = slots.clone().try_acquire_owned().map_err(|_| Error::Busy)?;
                shared.active.fetch_add(1, Ordering::AcqRel);
                permit
            };
            let token = cancellation.child_token();
            let _drop = token.clone().drop_guard();
            let worker = shared.clone();
            let work_token = token.clone();
            let task = tokio::spawn(async move {
                struct Finished(Arc<Shared>);
                impl Drop for Finished {
                    fn drop(&mut self) {
                        self.0.active.fetch_sub(1, Ordering::AcqRel);
                        self.0.completed.notify_waiters();
                    }
                }
                let _finished = Finished(worker.clone());
                let _permit = permit;
                let result = std::panic::AssertUnwindSafe(action(worker.clone(), work_token))
                    .catch_unwind()
                    .await
                    .unwrap_or(Err(Error::StorageUnavailable));
                if let Err(error) = &result {
                    failure(&worker, *error);
                    // Fs cancellation/deadline returns before kernel work can stop.
                    // Keep the aggregate permit until BOTH underlying physical queues drain.
                    tokio::join!(
                        worker.primary.physical_drain(),
                        worker.mirror.physical_drain()
                    );
                }
                result
            });
            let (send, recv) = tokio::sync::oneshot::channel();
            tokio::spawn(async move {
                let result = match tokio::time::timeout(shared.budget, task).await {
                    Ok(Ok(value)) => value,
                    Ok(Err(_)) | Err(_) => {
                        token.cancel();
                        failure(&shared, Error::StorageUnavailable);
                        Err(Error::StorageUnavailable)
                    }
                };
                let _ = send.send(result);
            });
            tokio::select! { biased;
                _ = cancellation.cancelled() => Err(Error::Cancelled),
                value = recv => value.unwrap_or(Err(Error::StorageUnavailable)),
            }
        })
    }
}

impl BlobStore for MirroredBlobStore {
    fn root_identities(&self) -> Option<Vec<openlegal_application::blob::BlobRootIdentity>> {
        Some(vec![
            self.shared.primary.shared.files.cached_root_identity(),
            self.shared.mirror.shared.files.cached_root_identity(),
        ])
    }
    fn put_if_absent(
        &self,
        location: BlobLocation,
        bytes: Vec<u8>,
        cancellation: CancellationToken,
    ) -> BoxFuture<'static, Result<BlobPutResult, Error>> {
        self.operation(cancellation, false, move |s, t| async move {
            let (a, b) = tokio::join!(
                s.primary
                    .put_if_absent(location.clone(), bytes.clone(), t.child_token()),
                s.mirror.put_if_absent(location, bytes, t.child_token())
            );
            let (a, b) = pair(a, b)?;
            Ok(
                if a == BlobPutResult::AlreadyPresent && b == BlobPutResult::AlreadyPresent {
                    BlobPutResult::AlreadyPresent
                } else {
                    BlobPutResult::Created
                },
            )
        })
    }
    fn confirm_durable(
        &self,
        location: BlobLocation,
        cancellation: CancellationToken,
    ) -> BoxFuture<'static, Result<(), Error>> {
        self.operation(cancellation, false, move |s, t| async move {
            let (a, b) = tokio::join!(
                s.primary.confirm_durable(location.clone(), t.child_token()),
                s.mirror.confirm_durable(location, t.child_token())
            );
            pair(a, b)?;
            Ok(())
        })
    }
    fn get(
        &self,
        location: BlobLocation,
        cancellation: CancellationToken,
    ) -> BoxFuture<'static, Result<Option<Vec<u8>>, Error>> {
        // A mirror outage fences publication, not healthy primary lookup.
        self.shared.primary.get(location, cancellation)
    }
    fn enumerate(
        &self,
        cursor: Option<String>,
        limit: usize,
        cancellation: CancellationToken,
    ) -> BoxFuture<'static, Result<BlobPage, Error>> {
        self.shared.primary.enumerate(cursor, limit, cancellation)
    }
    fn delete_if_present(
        &self,
        location: BlobLocation,
        cancellation: CancellationToken,
    ) -> BoxFuture<'static, Result<(), Error>> {
        self.operation(cancellation, false, move |s, t| async move {
            let (a, b) = tokio::join!(
                s.primary
                    .delete_if_present(location.clone(), t.child_token()),
                s.mirror.delete_if_present(location, t.child_token())
            );
            pair(a, b)?;
            Ok(())
        })
    }
    fn cleanup_staging(
        &self,
        now: u64,
        limit: usize,
        cancellation: CancellationToken,
    ) -> BoxFuture<'static, Result<usize, Error>> {
        self.operation(cancellation, false, move |s, t| async move {
            let (a, b) = tokio::join!(
                s.primary.cleanup_staging(now, limit, t.child_token()),
                s.mirror.cleanup_staging(now, limit, t.child_token())
            );
            let (a, b) = pair(a, b)?;
            Ok(a.saturating_add(b))
        })
    }
    fn health(&self, cancellation: CancellationToken) -> BoxFuture<'static, Result<(), Error>> {
        let shared = self.shared.clone();
        let store = self.clone();
        Box::pin(async move {
            check_cancel(&cancellation)?;
            let _waiter = shared
                .health_waiters
                .try_acquire()
                .map_err(|_| Error::Busy)?;
            let mut outcome = {
                let mut active = shared.health_flight.lock().map_err(|_| Error::Internal)?;
                if shared.closed.load(Ordering::Acquire) {
                    return Err(Error::Shutdown);
                }
                if let Some(flight) = active.as_ref() {
                    flight.outcome.clone()
                } else {
                    let (sender, receiver) = watch::channel(None);
                    let flight = Arc::new(MirrorFlight {
                        outcome: receiver.clone(),
                    });
                    *active = Some(flight.clone());
                    let worker = shared.clone();
                    // Individual callers never own or cancel the shared probe.
                    tokio::spawn(async move {
                        let result = store
                            .operation(CancellationToken::new(), true, move |s, t| async move {
                                let generation = s.generation.load(Ordering::Acquire);
                                if !s.healthy.load(Ordering::Acquire) {
                                    // Primary lookups remain available during a
                                    // mirror outage, so they are not aggregate
                                    // operations. Include their physical work
                                    // in the recovery drainage proof as well.
                                    if s.active.load(Ordering::Acquire) != 1
                                        || s.primary.metrics().active_jobs != 0
                                        || s.mirror.metrics().active_jobs != 0
                                    {
                                        return Err(Error::Busy);
                                    }
                                }
                                let (a, b) = tokio::join!(
                                    s.primary.health(t.child_token()),
                                    s.mirror.health(t.child_token())
                                );
                                pair(a, b)?;
                                Ok(generation)
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
                        sender.send_replace(Some(result));
                    });
                    receiver
                }
            };
            let generation = loop {
                if let Some(value) = *outcome.borrow_and_update() {
                    break value?;
                }
                tokio::select! { biased;
                    _ = cancellation.cancelled() => return Err(Error::Cancelled),
                    changed = outcome.changed() => changed.map_err(|_|Error::StorageUnavailable)?,
                }
            };
            check_cancel(&cancellation)?;
            let _lock = shared.admission.lock().map_err(|_| Error::Internal)?;
            if shared.closed.load(Ordering::Acquire) {
                return Err(Error::Shutdown);
            }
            check_cancel(&cancellation)?;
            if shared.generation.load(Ordering::Acquire) != generation
                || !shared.primary.shared.healthy.load(Ordering::Acquire)
                || !shared.mirror.shared.healthy.load(Ordering::Acquire)
            {
                return Err(Error::StorageUnavailable);
            }
            shared.healthy.store(true, Ordering::Release);
            Ok(())
        })
    }
    fn close(&self) -> BoxFuture<'static, Result<(), Error>> {
        let shared = self.shared.clone();
        let admission_error = if let Ok(_lock) = shared.admission.lock() {
            shared.closed.store(true, Ordering::Release);
            shared.healthy.store(false, Ordering::Release);
            false
        } else {
            shared.closed.store(true, Ordering::Release);
            shared.healthy.store(false, Ordering::Release);
            true
        };
        // Calling BOTH close methods synchronously fences both admissions even if
        // their later drainage fails. Never short-circuit the second close.
        let first = shared.primary.close();
        let second = shared.mirror.close();
        Box::pin(async move {
            tokio::time::timeout(shared.budget, async {
                let (a, b) = tokio::join!(first, second);
                let combined = pair(a, b);
                loop {
                    let notified = shared.completed.notified();
                    tokio::pin!(notified);
                    notified.as_mut().enable();
                    if shared.active.load(Ordering::Acquire) == 0 {
                        break;
                    }
                    notified.await;
                }
                combined?;
                if admission_error {
                    return Err(Error::Internal);
                }
                Ok(())
            })
            .await
            .map_err(|_| Error::StorageUnavailable)?
        })
    }
    fn metrics(&self) -> BlobMetrics {
        let a = self.shared.primary.metrics();
        let b = self.shared.mirror.metrics();
        BlobMetrics {
            reads: a.reads,
            writes: a.writes.saturating_add(b.writes),
            deduplicated_puts: a.deduplicated_puts.saturating_add(b.deduplicated_puts),
            deletions: a.deletions.saturating_add(b.deletions),
            failures: a.failures.saturating_add(b.failures),
            corruptions: a.corruptions.saturating_add(b.corruptions),
            saturation: a.saturation.saturating_add(b.saturation),
            active_jobs: a.active_jobs.saturating_add(b.active_jobs),
            queued_jobs: a.queued_jobs.saturating_add(b.queued_jobs),
            started_jobs: a.started_jobs.saturating_add(b.started_jobs),
            queue_wait_us: a
                .queue_wait_us
                .saturating_add(b.queue_wait_us)
                .min(999_999_999_999),
            filesystem_work_us: a
                .filesystem_work_us
                .saturating_add(b.filesystem_work_us)
                .min(999_999_999_999),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::{Digest, Sha256};
    use std::{fs, os::unix::fs::PermissionsExt, sync::Condvar};
    static FIXTURES: Semaphore = Semaphore::const_new(1);
    fn location(bytes: &[u8]) -> BlobLocation {
        let digest: [u8; 32] = Sha256::digest(bytes).into();
        let text: String = digest.iter().map(|b| format!("{b:02x}")).collect();
        BlobLocation {
            digest,
            size_bytes: bytes.len() as u64,
            storage_key: format!("{}/{text}-01990000-0000-7000-8000-000000000001", &text[..2]),
        }
    }
    fn token() -> CancellationToken {
        CancellationToken::new()
    }
    async fn fixture() -> (tempfile::TempDir, Arc<MirroredBlobStore>) {
        let root = tempfile::tempdir().unwrap();
        fs::set_permissions(root.path(), fs::Permissions::from_mode(0o700)).unwrap();
        // These tests exercise mirroring/cancellation/closure, not the default
        // startup latency. Keep slow disposable CI fsyncs out of their setup;
        // deadline tests install their own short budget after opening.
        let store = MirroredBlobStore::open_with_limit_and_timeout(
            &root.path().join("primary"),
            &root.path().join("mirror"),
            openlegal_application::MAX_RAW_BYTES,
            Duration::from_secs(30),
        )
        .await
        .unwrap();
        (root, store)
    }
    #[tokio::test]
    async fn opened_root_identities_are_cached_and_ordered_primary_then_replica() {
        let _io = crate::blob::test_io_admission().await;
        let _serial = FIXTURES.acquire().await.unwrap();
        let (root, store) = fixture().await;
        let identities = store.root_identities().unwrap();
        assert_eq!(identities.len(), 2);
        let primary = std::fs::metadata(root.path().join("primary")).unwrap();
        let replica = std::fs::metadata(root.path().join("mirror")).unwrap();
        assert_eq!(
            (identities[0].device, identities[0].inode),
            (primary.dev(), primary.ino())
        );
        assert_eq!(
            (identities[1].device, identities[1].inode),
            (replica.dev(), replica.ino())
        );
        assert_ne!(identities[0], identities[1]);
        // No pathname lookup or filesystem read may be needed by the getter.
        fs::rename(
            root.path().join("primary"),
            root.path().join("retained-primary"),
        )
        .unwrap();
        fs::rename(
            root.path().join("mirror"),
            root.path().join("retained-mirror"),
        )
        .unwrap();
        assert_eq!(store.root_identities(), Some(identities));
        store.close().await.unwrap();
    }
    async fn drained(store: &MirroredBlobStore) {
        tokio::time::timeout(Duration::from_secs(2), async {
            while store.active_operations() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
    struct Release(Arc<(Mutex<bool>, Condvar)>);
    impl Drop for Release {
        fn drop(&mut self) {
            let (lock, ready) = &*self.0;
            *lock.lock().unwrap() = true;
            ready.notify_all();
        }
    }
    fn pause_health(
        store: &MirroredBlobStore,
    ) -> (Release, tokio::sync::mpsc::UnboundedReceiver<()>) {
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        let worker = gate.clone();
        let (send, recv) = tokio::sync::mpsc::unbounded_channel();
        *store.shared.primary.shared.health_hook.lock().unwrap() = Some(Arc::new(move || {
            let _ = send.send(());
            let (lock, ready) = &*worker;
            let mut done = lock.lock().unwrap();
            while !*done {
                done = ready.wait(done).unwrap();
            }
        }));
        (Release(gate), recv)
    }
    #[tokio::test]
    async fn both_generations_durable_dedup_and_primary_only_read() {
        let _io = crate::blob::test_io_admission().await;
        let _fixture = FIXTURES.acquire().await.unwrap();
        let (root, s) = fixture().await;
        let bytes = b"fictional mirrored evidence";
        let loc = location(bytes);
        assert_eq!(
            s.put_if_absent(loc.clone(), bytes.to_vec(), token())
                .await
                .unwrap(),
            BlobPutResult::Created
        );
        assert_eq!(
            s.put_if_absent(loc.clone(), bytes.to_vec(), token())
                .await
                .unwrap(),
            BlobPutResult::AlreadyPresent
        );
        s.confirm_durable(loc.clone(), token()).await.unwrap();
        for name in ["primary", "mirror"] {
            assert_eq!(
                fs::read(root.path().join(name).join(&loc.storage_key)).unwrap(),
                bytes
            );
        }
        fs::remove_file(root.path().join("primary").join(&loc.storage_key)).unwrap();
        assert_eq!(s.get(loc.clone(), token()).await.unwrap(), None);
        assert_eq!(
            s.confirm_durable(loc.clone(), token()).await,
            Err(Error::StorageCorrupt)
        );
        assert!(!root.path().join("primary").join(&loc.storage_key).exists());
        assert_eq!(
            fs::read(root.path().join("mirror").join(&loc.storage_key)).unwrap(),
            bytes
        );
    }
    #[tokio::test]
    async fn partial_put_never_acknowledges_or_repairs_corrupt_mirror() {
        let _io = crate::blob::test_io_admission().await;
        let _fixture = FIXTURES.acquire().await.unwrap();
        let (root, s) = fixture().await;
        let bytes = b"fictional pending generation";
        let loc = location(bytes);
        s.shared
            .mirror
            .put_if_absent(loc.clone(), bytes.to_vec(), token())
            .await
            .unwrap();
        let path = root.path().join("mirror").join(&loc.storage_key);
        fs::write(&path, b"corrupt").unwrap();
        assert_eq!(
            s.put_if_absent(loc.clone(), bytes.to_vec(), token()).await,
            Err(Error::StorageCorrupt)
        );
        drained(&s).await;
        assert_eq!(
            fs::read(root.path().join("primary").join(&loc.storage_key)).unwrap(),
            bytes
        );
        assert_eq!(fs::read(&path).unwrap(), b"corrupt");
        assert_eq!(
            s.get(loc.clone(), token()).await.unwrap(),
            Some(bytes.to_vec())
        );
        assert_eq!(
            s.put_if_absent(loc, bytes.to_vec(), token()).await,
            Err(Error::StorageUnavailable)
        );
        assert_eq!(fs::read(path).unwrap(), b"corrupt");
    }
    #[tokio::test]
    async fn unlink_attempts_other_leg_even_if_primary_fails() {
        let _io = crate::blob::test_io_admission().await;
        let _fixture = FIXTURES.acquire().await.unwrap();
        let (root, s) = fixture().await;
        let bytes = b"fictional retired generation";
        let loc = location(bytes);
        s.put_if_absent(loc.clone(), bytes.to_vec(), token())
            .await
            .unwrap();
        let path = root.path().join("primary").join(&loc.storage_key);
        fs::remove_file(&path).unwrap();
        fs::create_dir(&path).unwrap();
        assert_eq!(
            s.delete_if_present(loc.clone(), token()).await,
            Err(Error::StorageCorrupt)
        );
        drained(&s).await;
        assert!(!root.path().join("mirror").join(&loc.storage_key).exists());
        assert!(path.is_dir());
    }
    #[tokio::test]
    async fn missing_confirmation_never_creates_objects() {
        let _io = crate::blob::test_io_admission().await;
        let _fixture = FIXTURES.acquire().await.unwrap();
        let (root, s) = fixture().await;
        let loc = location(b"absent");
        assert_eq!(
            s.confirm_durable(loc.clone(), token()).await,
            Err(Error::StorageCorrupt)
        );
        drained(&s).await;
        for name in ["primary", "mirror"] {
            assert!(!root.path().join(name).join(&loc.storage_key).exists());
        }
    }
    #[tokio::test]
    async fn late_cancelled_probe_holds_permit_and_cannot_recover() {
        let _io = crate::blob::test_io_admission().await;
        let _fixture = FIXTURES.acquire().await.unwrap();
        let (_root, s) = fixture().await;
        failure(&s.shared, Error::StorageUnavailable);
        let (release, mut entered) = pause_health(&s);
        let cancellation = token();
        let waiter = tokio::spawn(s.health(cancellation.clone()));
        entered.recv().await.unwrap();
        cancellation.cancel();
        assert_eq!(waiter.await.unwrap(), Err(Error::Cancelled));
        assert!(s.active_operations() > 0);
        drop(release);
        drained(&s).await;
        assert!(!s.shared.healthy.load(Ordering::Acquire));
        *s.shared.primary.shared.health_hook.lock().unwrap() = None;
        s.health(token()).await.unwrap();
        assert!(s.shared.healthy.load(Ordering::Acquire));
    }
    #[tokio::test]
    async fn stale_success_cannot_clear_new_failure() {
        let _io = crate::blob::test_io_admission().await;
        let _fixture = FIXTURES.acquire().await.unwrap();
        let (_root, s) = fixture().await;
        let (release, mut entered) = pause_health(&s);
        let waiter = tokio::spawn(s.health(token()));
        entered.recv().await.unwrap();
        failure(&s.shared, Error::StorageUnavailable);
        drop(release);
        assert_eq!(waiter.await.unwrap(), Err(Error::StorageUnavailable));
        assert!(!s.shared.healthy.load(Ordering::Acquire));
    }
    #[tokio::test]
    async fn cancelling_one_health_waiter_does_not_cancel_shared_probe() {
        let _io = crate::blob::test_io_admission().await;
        let _fixture = FIXTURES.acquire().await.unwrap();
        let (_root, s) = fixture().await;
        failure(&s.shared, Error::StorageUnavailable);
        let (release, mut entered) = pause_health(&s);
        let cancellation = token();
        let first = tokio::spawn(s.health(cancellation.clone()));
        entered.recv().await.unwrap();
        let second = tokio::spawn(s.health(token()));
        tokio::task::yield_now().await;
        cancellation.cancel();
        assert_eq!(first.await.unwrap(), Err(Error::Cancelled));
        drop(release);
        assert_eq!(second.await.unwrap(), Ok(()));
        assert!(s.shared.healthy.load(Ordering::Acquire));
    }
    #[tokio::test]
    async fn close_fences_both_legs_despite_primary_drain_timeout() {
        let _io = crate::blob::test_io_admission().await;
        let _fixture = FIXTURES.acquire().await.unwrap();
        let (_root, mut s) = fixture().await;
        Arc::get_mut(&mut Arc::get_mut(&mut s).unwrap().shared)
            .unwrap()
            .budget = Duration::from_millis(30);
        let (release, mut entered) = pause_health(&s);
        let waiter = tokio::spawn(s.health(token()));
        entered.recv().await.unwrap();
        assert_eq!(s.close().await, Err(Error::StorageUnavailable));
        assert!(s.shared.primary.shared.closed.load(Ordering::Acquire));
        assert!(s.shared.mirror.shared.closed.load(Ordering::Acquire));
        assert_eq!(
            s.shared.mirror.get(location(b"absent"), token()).await,
            Err(Error::Shutdown)
        );
        drop(release);
        let _ = waiter.await;
        drained(&s).await;
    }
    #[tokio::test]
    async fn distinct_nonoverlapping_roots_required() {
        let _io = crate::blob::test_io_admission().await;
        let _fixture = FIXTURES.acquire().await.unwrap();
        let r = tempfile::tempdir().unwrap();
        assert!(matches!(
            MirroredBlobStore::open(r.path(), r.path()).await,
            Err(Error::InvalidInput)
        ));
        assert!(matches!(
            MirroredBlobStore::open(r.path(), &r.path().join("nested")).await,
            Err(Error::InvalidInput)
        ));
        assert!(matches!(
            MirroredBlobStore::open(Path::new("relative"), r.path()).await,
            Err(Error::InvalidInput)
        ));
    }
}
