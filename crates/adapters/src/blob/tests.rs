use super::*;
use sha2::{Digest, Sha256};
use std::{
    fs,
    os::unix::fs::{PermissionsExt, symlink},
    sync::{Condvar, Mutex},
};

// Independent test runtimes share the production startup semaphore. Bound only
// fixture construction so a high-core test runner does not exhaust its 16 slots;
// ordinary jobs and intentional within-test concurrency remain unrestricted.
static FIXTURE_STARTUPS: Semaphore = Semaphore::const_new(8);

async fn start_blob<F: std::future::Future>(constructor: F) -> F::Output {
    let _permit = FIXTURE_STARTUPS.acquire().await.unwrap();
    constructor.await
}

fn location(bytes: &[u8], generation: u64) -> BlobLocation {
    let digest: [u8; 32] = Sha256::digest(bytes).into();
    let hex: String = digest.iter().map(|b| format!("{b:02x}")).collect();
    BlobLocation {
        digest,
        size_bytes: bytes.len() as u64,
        storage_key: format!(
            "{}/{hex}-01990000-0000-7000-8000-{generation:012x}",
            &hex[..2]
        ),
    }
}
fn root() -> tempfile::TempDir {
    let root = tempfile::tempdir().unwrap();
    fs::set_permissions(root.path(), fs::Permissions::from_mode(0o700)).unwrap();
    root
}
fn token() -> CancellationToken {
    CancellationToken::new()
}

struct ReleaseOnDrop(Arc<(Mutex<bool>, Condvar)>);
impl Drop for ReleaseOnDrop {
    fn drop(&mut self) {
        let (lock, ready) = &*self.0;
        if let Ok(mut released) = lock.lock() {
            *released = true;
            ready.notify_all();
        }
    }
}

#[tokio::test]
async fn immutable_generations_deduplicate_and_survive_restart() {
    let root = root();
    let bytes = b"fictional evidence";
    let first = location(bytes, 1);
    let second = location(bytes, 2);
    let blobs = start_blob(FsBlobStore::open(root.path())).await.unwrap();
    assert_eq!(
        blobs
            .put_if_absent(first.clone(), bytes.to_vec(), token())
            .await
            .unwrap(),
        BlobPutResult::Created
    );
    assert_eq!(
        blobs
            .put_if_absent(first.clone(), bytes.to_vec(), token())
            .await
            .unwrap(),
        BlobPutResult::AlreadyPresent
    );
    assert_eq!(
        blobs.get(first.clone(), token()).await.unwrap(),
        Some(bytes.to_vec())
    );
    blobs
        .put_if_absent(second.clone(), bytes.to_vec(), token())
        .await
        .unwrap();
    blobs
        .delete_if_present(first.clone(), token())
        .await
        .unwrap();
    blobs
        .delete_if_present(first.clone(), token())
        .await
        .unwrap();
    assert!(blobs.get(first, token()).await.unwrap().is_none());
    assert_eq!(
        blobs.get(second.clone(), token()).await.unwrap(),
        Some(bytes.to_vec())
    );
    blobs.close().await.unwrap();
    let reopened = start_blob(FsBlobStore::open(root.path())).await.unwrap();
    assert_eq!(
        reopened.get(second, token()).await.unwrap(),
        Some(bytes.to_vec())
    );
    reopened.close().await.unwrap();
}

#[tokio::test]
async fn corrupt_reads_and_existing_puts_never_repair_evidence() {
    let root = root();
    let blobs = start_blob(FsBlobStore::open(root.path())).await.unwrap();
    let object = location(b"ABC", 1);
    blobs
        .put_if_absent(object.clone(), b"ABC".to_vec(), token())
        .await
        .unwrap();
    fs::write(root.path().join(&object.storage_key), b"BAD").unwrap();
    assert_eq!(
        blobs.get(object.clone(), token()).await.unwrap_err(),
        Error::StorageCorrupt
    );
    blobs.health(token()).await.unwrap();
    assert_eq!(
        blobs
            .put_if_absent(object.clone(), b"ABC".to_vec(), token())
            .await
            .unwrap_err(),
        Error::StorageCorrupt
    );
    assert_eq!(
        fs::read(root.path().join(&object.storage_key)).unwrap(),
        b"BAD"
    );
    assert_eq!(blobs.metrics().corruptions, 2);
    blobs.close().await.unwrap();
}

#[tokio::test]
async fn size_identity_and_path_validation_are_independent() {
    let root = root();
    let blobs = start_blob(FsBlobStore::open(root.path())).await.unwrap();
    let object = location(b"ABC", 1);
    assert_eq!(
        blobs
            .put_if_absent(object.clone(), b"BAD".to_vec(), token())
            .await
            .unwrap_err(),
        Error::InvalidInput
    );
    for key in [
        "../outside".to_owned(),
        format!("00/{}", object.storage_key),
        "/absolute".into(),
    ] {
        let mut bad = object.clone();
        bad.storage_key = key;
        assert_eq!(
            blobs.get(bad, token()).await.unwrap_err(),
            Error::InvalidInput
        );
    }
    blobs
        .put_if_absent(object.clone(), b"ABC".to_vec(), token())
        .await
        .unwrap();
    fs::write(root.path().join(&object.storage_key), b"ABCD").unwrap();
    assert_eq!(
        blobs.get(object, token()).await.unwrap_err(),
        Error::StorageCorrupt
    );
    blobs.close().await.unwrap();
}

#[tokio::test]
async fn symlinks_hardlinks_and_permissive_files_fail_closed() {
    let root = root();
    let blobs = start_blob(FsBlobStore::open(root.path())).await.unwrap();
    let object = location(b"ABC", 1);
    blobs
        .put_if_absent(object.clone(), b"ABC".to_vec(), token())
        .await
        .unwrap();
    let path = root.path().join(&object.storage_key);
    fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
    assert_eq!(
        blobs.get(object.clone(), token()).await.unwrap_err(),
        Error::StorageCorrupt
    );
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
    blobs.health(token()).await.unwrap();
    fs::hard_link(&path, root.path().join("foreign")).unwrap();
    assert_eq!(
        blobs.get(object.clone(), token()).await.unwrap_err(),
        Error::StorageCorrupt
    );
    fs::remove_file(root.path().join("foreign")).unwrap();
    fs::remove_file(&path).unwrap();
    symlink(root.path().join("foreign"), &path).unwrap();
    blobs.health(token()).await.unwrap();
    assert_eq!(
        blobs.get(object, token()).await.unwrap_err(),
        Error::StorageCorrupt
    );
    blobs.close().await.unwrap();
}

#[tokio::test]
async fn root_and_shard_symlinks_are_rejected() {
    let root = root();
    symlink(root.path(), root.path().join("linked")).unwrap();
    assert!(
        start_blob(FsBlobStore::open(&root.path().join("linked")))
            .await
            .is_err()
    );
    let blobs = start_blob(FsBlobStore::open(root.path())).await.unwrap();
    let object = location(b"ABC", 1);
    symlink(root.path(), root.path().join(&object.storage_key[..2])).unwrap();
    assert!(
        blobs
            .put_if_absent(object, b"ABC".to_vec(), token())
            .await
            .is_err()
    );
    blobs.close().await.unwrap();
}

#[tokio::test]
async fn bounded_enumeration_ignores_foreign_files_and_lists_all_generations() {
    let root = root();
    let blobs = start_blob(FsBlobStore::open(root.path())).await.unwrap();
    let mut expected = Vec::new();
    for i in 1..=8 {
        let object = location(b"same", i);
        blobs
            .put_if_absent(object.clone(), b"same".to_vec(), token())
            .await
            .unwrap();
        expected.push(object.storage_key);
    }
    fs::write(root.path().join("foreign"), b"operator file").unwrap();
    let mut cursor = None;
    let mut found = Vec::new();
    for _ in 0..300 {
        let page = blobs.enumerate(cursor, 3, token()).await.unwrap();
        assert!(page.objects.len() <= 3);
        found.extend(page.objects.into_iter().map(|o| o.storage_key));
        cursor = page.next_cursor;
        if cursor.is_none() {
            break;
        }
    }
    assert!(cursor.is_none());
    found.sort();
    expected.sort();
    assert_eq!(found, expected);
    assert!(root.path().join("foreign").exists());
    blobs.close().await.unwrap();
}

#[tokio::test]
async fn staged_garbage_is_cleaned_without_removing_final_or_foreign_objects() {
    let root = root();
    let blobs = start_blob(FsBlobStore::open(root.path())).await.unwrap();
    let object = location(b"ABC", 1);
    blobs
        .put_if_absent(object.clone(), b"ABC".to_vec(), token())
        .await
        .unwrap();
    let (prefix, name) = object.storage_key.split_once('/').unwrap();
    let staged = root
        .path()
        .join(prefix)
        .join(format!(".stage-{name}.{}", "a".repeat(64)));
    fs::write(&staged, b"partial").unwrap();
    fs::set_permissions(&staged, fs::Permissions::from_mode(0o600)).unwrap();
    let foreign = root.path().join(prefix).join("foreign.tmp");
    fs::write(&foreign, b"foreign").unwrap();
    let mut removed = 0;
    for _ in 0..5 {
        removed += blobs.cleanup_staging(u64::MAX, 128, token()).await.unwrap();
    }
    assert_eq!(removed, 1);
    assert!(!staged.exists());
    assert!(foreign.exists());
    assert_eq!(
        blobs.get(object, token()).await.unwrap(),
        Some(b"ABC".to_vec())
    );
    blobs.close().await.unwrap();
}

#[tokio::test]
async fn canceled_and_dropped_waiters_keep_actual_job_admission_owned() {
    let root = root();
    let blobs = start_blob(FsBlobStore::open(root.path())).await.unwrap();
    let gate = Arc::new((Mutex::new(false), Condvar::new()));
    let _release_on_failure = ReleaseOnDrop(gate.clone());
    let (entered, mut entries) = tokio::sync::mpsc::unbounded_channel();
    let mut waiters = Vec::new();
    for _ in 0..JOBS {
        let gate = gate.clone();
        let entered = entered.clone();
        let operation = blobs.job(token(), false, move |_, _, _| {
            entered.send(()).unwrap();
            let (lock, ready) = &*gate;
            let mut released = lock.lock().unwrap();
            while !*released {
                released = ready.wait(released).unwrap();
            }
            Ok(())
        });
        waiters.push(tokio::spawn(operation));
    }
    for _ in 0..JOBS {
        entries.recv().await.unwrap();
    }
    for waiter in waiters {
        waiter.abort();
        let _ = waiter.await;
    }
    assert_eq!(blobs.metrics().active_jobs, JOBS as u64);
    assert_eq!(
        blobs
            .job(token(), false, |_, _, _| Ok(()))
            .await
            .unwrap_err(),
        Error::Busy
    );
    // The reserved probe slot remains usable even with every ordinary slot held.
    blobs.health(token()).await.unwrap();
    assert!(blobs.shared.healthy.load(Ordering::Acquire));
    assert_eq!(blobs.metrics().active_jobs, JOBS as u64);
    let (lock, ready) = &*gate;
    *lock.lock().unwrap() = true;
    ready.notify_all();
    blobs.close().await.unwrap();
    assert_eq!(blobs.metrics().active_jobs, 0);
}

#[tokio::test]
async fn concurrent_immutable_puts_publish_one_complete_object() {
    let root = root();
    let blobs = start_blob(FsBlobStore::open(root.path())).await.unwrap();
    let bytes = vec![42; 64 * 1024];
    let object = location(&bytes, 1);
    let operations = (0..8).map(|_| blobs.put_if_absent(object.clone(), bytes.clone(), token()));
    let results = futures::future::join_all(operations).await;
    assert_eq!(
        results
            .iter()
            .filter(|v| **v == Ok(BlobPutResult::Created))
            .count(),
        1
    );
    assert!(results.iter().all(Result::is_ok));
    assert_eq!(blobs.get(object, token()).await.unwrap(), Some(bytes));
    blobs.close().await.unwrap();
}

#[test]
fn interrupted_publication_leaves_absence_or_a_complete_immutable_object() {
    for failure in 0..3 {
        let root = root();
        let object = location(b"source evidence", 1);
        let files = Filesystem::open(root.path()).unwrap();
        filesystem::FAIL_AFTER.set(Some(failure));
        let result = files.put(&object, b"source evidence", &token());
        filesystem::FAIL_AFTER.set(None);
        assert_eq!(result.unwrap_err(), Error::StorageUnavailable);
        drop(files);
        let restarted = Filesystem::open(root.path()).unwrap();
        let observed = restarted.get(&object).unwrap();
        if failure == 0 {
            assert!(observed.is_none());
        } else {
            assert_eq!(observed, Some(b"source evidence".to_vec()));
        }
        // A deduplicated retry completes durability even after an interrupted rename.
        restarted
            .put(&object, b"source evidence", &token())
            .unwrap();
        assert_eq!(
            restarted.get(&object).unwrap(),
            Some(b"source evidence".to_vec())
        );
    }
}

#[tokio::test]
async fn cancellation_before_admission_writes_nothing() {
    let root = root();
    let blobs = start_blob(FsBlobStore::open(root.path())).await.unwrap();
    let object = location(b"source", 1);
    let cancellation = token();
    cancellation.cancel();
    assert_eq!(
        blobs
            .put_if_absent(object.clone(), b"source".to_vec(), cancellation)
            .await
            .unwrap_err(),
        Error::Cancelled
    );
    assert!(blobs.get(object, token()).await.unwrap().is_none());
    blobs.close().await.unwrap();
}

#[tokio::test]
async fn dropped_waiter_cannot_disable_deadline_health_failure() {
    let root = root();
    let blobs = start_blob(FsBlobStore::open(root.path())).await.unwrap();
    let gate = Arc::new((Mutex::new(false), Condvar::new()));
    let _release_on_failure = ReleaseOnDrop(gate.clone());
    let worker_gate = gate.clone();
    let (entered, entry) = tokio::sync::oneshot::channel();
    let waiter = tokio::spawn(blobs.job(token(), false, move |_, _, _| {
        entered.send(()).unwrap();
        let (lock, ready) = &*worker_gate;
        let mut released = lock.lock().unwrap();
        while !*released {
            released = ready.wait(released).unwrap();
        }
        Ok(())
    }));
    entry.await.unwrap();
    let notification = blobs.shared.completed.notified();
    tokio::pin!(notification);
    notification.as_mut().enable();
    waiter.abort();
    let _ = waiter.await;
    tokio::time::timeout(DEADLINE + Duration::from_secs(2), notification)
        .await
        .unwrap();
    assert!(!blobs.shared.healthy.load(Ordering::Acquire));
    assert_eq!(blobs.metrics().active_jobs, 1);
    let (lock, ready) = &*gate;
    *lock.lock().unwrap() = true;
    ready.notify_all();
    blobs.close().await.unwrap();
}

#[test]
fn existing_root_and_shard_still_require_parent_directory_durability() {
    let root = root();
    // Simulate the directory already existing after an interrupted creator.
    filesystem::PARENT_SYNC_FAIL.set(true);
    let opened = Filesystem::open(root.path());
    filesystem::PARENT_SYNC_FAIL.set(false);
    assert!(matches!(opened, Err(Error::StorageUnavailable)));

    let files = Filesystem::open(root.path()).unwrap();
    let object = location(b"ABC", 1);
    let shard = root.path().join(&object.storage_key[..2]);
    fs::create_dir(&shard).unwrap();
    fs::set_permissions(&shard, fs::Permissions::from_mode(0o700)).unwrap();
    filesystem::PARENT_SYNC_FAIL.set(true);
    let published = files.put(&object, b"ABC", &token());
    filesystem::PARENT_SYNC_FAIL.set(false);
    assert_eq!(published.unwrap_err(), Error::StorageUnavailable);
    assert!(!root.path().join(&object.storage_key).exists());
    files.put(&object, b"ABC", &token()).unwrap();
    assert_eq!(files.get(&object).unwrap(), Some(b"ABC".to_vec()));
}

fn pause_health(blobs: &FsBlobStore) -> (ReleaseOnDrop, tokio::sync::mpsc::UnboundedReceiver<()>) {
    let gate = Arc::new((Mutex::new(false), Condvar::new()));
    let worker_gate = gate.clone();
    let (entered, entry) = tokio::sync::mpsc::unbounded_channel();
    *blobs.shared.health_hook.lock().unwrap() = Some(Arc::new(move || {
        let _ = entered.send(());
        let (lock, ready) = &*worker_gate;
        let mut released = lock.lock().unwrap();
        while !*released {
            released = ready.wait(released).unwrap();
        }
    }));
    (ReleaseOnDrop(gate), entry)
}

#[tokio::test]
async fn overlapping_health_checks_do_not_report_storage_failure() {
    let root = root();
    let blobs = start_blob(FsBlobStore::open(root.path())).await.unwrap();
    let (release, mut entered) = pause_health(&blobs);
    let first = tokio::spawn(blobs.health(token()));
    entered.recv().await.unwrap();
    let second = blobs.health(token());
    tokio::pin!(second);
    // The second caller must wait for a real probe result, not report Busy
    // merely because another health check owns the reserved physical slot.
    let admission = futures::poll!(second.as_mut());
    assert!(
        admission.is_pending(),
        "overlapping health result: {admission:?}"
    );
    assert_eq!(blobs.metrics().active_jobs, 1);
    assert!(entered.try_recv().is_err());
    drop(release);
    first.await.unwrap().unwrap();
    second.await.unwrap();
    blobs.health(token()).await.unwrap();
    entered.recv().await.unwrap();
    blobs.close().await.unwrap();
}

#[tokio::test]
async fn successful_probe_cannot_clear_a_newer_failure() {
    let root = root();
    let blobs = start_blob(FsBlobStore::open(root.path())).await.unwrap();
    let (release, mut entered) = pause_health(&blobs);
    let probe = tokio::spawn(blobs.health(token()));
    entered.recv().await.unwrap();
    let mut second = blobs.health(token());
    assert!(futures::poll!(second.as_mut()).is_pending());
    assert_eq!(
        blobs
            .job(token(), false, |_, _, _| Err::<(), _>(
                Error::StorageUnavailable
            ))
            .await
            .unwrap_err(),
        Error::StorageUnavailable
    );
    drop(release);
    assert_eq!(probe.await.unwrap().unwrap_err(), Error::StorageUnavailable);
    assert_eq!(second.await.unwrap_err(), Error::StorageUnavailable);
    assert!(!blobs.shared.healthy.load(Ordering::Acquire));
    *blobs.shared.health_hook.lock().unwrap() = None;
    blobs.health(token()).await.unwrap();
    assert!(blobs.shared.healthy.load(Ordering::Acquire));
    blobs.close().await.unwrap();
}

#[tokio::test]
async fn recovery_probe_requires_older_jobs_to_drain() {
    let root = root();
    let blobs = start_blob(FsBlobStore::open(root.path())).await.unwrap();
    let gate = Arc::new((Mutex::new(false), Condvar::new()));
    let release = ReleaseOnDrop(gate.clone());
    let (entered, entry) = tokio::sync::oneshot::channel();
    let operation = tokio::spawn(blobs.job(token(), false, move |_, _, _| {
        entered.send(()).unwrap();
        let (lock, ready) = &*gate;
        let mut released = lock.lock().unwrap();
        while !*released {
            released = ready.wait(released).unwrap();
        }
        Ok(())
    }));
    entry.await.unwrap();
    assert_eq!(
        blobs
            .job(token(), false, |_, _, _| Err::<(), _>(
                Error::StorageUnavailable
            ))
            .await
            .unwrap_err(),
        Error::StorageUnavailable
    );
    assert_eq!(blobs.health(token()).await.unwrap_err(), Error::Busy);
    assert!(!blobs.shared.healthy.load(Ordering::Acquire));
    drop(release);
    operation.await.unwrap().unwrap();
    blobs.health(token()).await.unwrap();
    assert!(blobs.shared.healthy.load(Ordering::Acquire));
    blobs.close().await.unwrap();
}

#[tokio::test]
async fn late_probe_completion_cannot_clear_its_deadline_failure() {
    let root = root();
    let blobs = start_blob(FsBlobStore::open(root.path())).await.unwrap();
    let (release, mut entered) = pause_health(&blobs);
    let probe = tokio::spawn(blobs.health(token()));
    entered.recv().await.unwrap();
    let mut second = blobs.health(token());
    assert!(futures::poll!(second.as_mut()).is_pending());
    assert_eq!(probe.await.unwrap().unwrap_err(), Error::StorageUnavailable);
    assert_eq!(second.await.unwrap_err(), Error::StorageUnavailable);
    assert_eq!(blobs.metrics().active_jobs, 1);
    assert_eq!(blobs.health(token()).await.unwrap_err(), Error::Busy);
    assert!(!blobs.shared.healthy.load(Ordering::Acquire));
    let completed = blobs.shared.completed.notified();
    tokio::pin!(completed);
    completed.as_mut().enable();
    drop(release);
    while blobs.metrics().active_jobs != 0 {
        completed.as_mut().await;
        completed.set(blobs.shared.completed.notified());
        completed.as_mut().enable();
    }
    assert!(!blobs.shared.healthy.load(Ordering::Acquire));
    *blobs.shared.health_hook.lock().unwrap() = None;
    blobs.health(token()).await.unwrap();
    assert!(blobs.shared.healthy.load(Ordering::Acquire));
    blobs.close().await.unwrap();
}

#[tokio::test]
async fn initiating_health_waiter_cancellation_and_drop_leave_other_waiter_live() {
    for cancel in [true, false] {
        let root = root();
        let blobs = start_blob(FsBlobStore::open(root.path())).await.unwrap();
        failed(&blobs.shared, Error::StorageUnavailable);
        let (release, mut entered) = pause_health(&blobs);
        let cancellation = token();
        let mut first = blobs.health(cancellation.clone());
        assert!(futures::poll!(first.as_mut()).is_pending());
        entered.recv().await.unwrap();
        let mut second = blobs.health(token());
        assert!(futures::poll!(second.as_mut()).is_pending());
        if cancel {
            cancellation.cancel();
            assert_eq!(first.await.unwrap_err(), Error::Cancelled);
        } else {
            drop(first);
        }
        assert_eq!(blobs.metrics().active_jobs, 1);
        drop(release);
        second.await.unwrap();
        assert!(blobs.shared.healthy.load(Ordering::Acquire));
        blobs.close().await.unwrap();
    }
}

#[tokio::test]
async fn abandoned_shared_recovery_cannot_restore_health() {
    let root = root();
    let blobs = start_blob(FsBlobStore::open(root.path())).await.unwrap();
    failed(&blobs.shared, Error::StorageUnavailable);
    let (release, mut entered) = pause_health(&blobs);
    let mut first = blobs.health(token());
    assert!(futures::poll!(first.as_mut()).is_pending());
    entered.recv().await.unwrap();
    let cancellation = token();
    let mut second = blobs.health(cancellation.clone());
    assert!(futures::poll!(second.as_mut()).is_pending());
    let mut outcome = blobs
        .shared
        .health_flight
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .outcome
        .clone();
    drop(first);
    cancellation.cancel();
    assert_eq!(second.await.unwrap_err(), Error::Cancelled);
    drop(release);
    outcome.changed().await.unwrap();
    assert!(!blobs.shared.healthy.load(Ordering::Acquire));
    assert_eq!(blobs.metrics().active_jobs, 0);
    blobs.health(token()).await.unwrap();
    assert!(blobs.shared.healthy.load(Ordering::Acquire));
    blobs.close().await.unwrap();
}

#[tokio::test]
async fn shared_health_waiters_are_bounded_and_release_admission() {
    let root = root();
    let blobs = start_blob(FsBlobStore::open(root.path())).await.unwrap();
    let (release, mut entered) = pause_health(&blobs);
    let mut waiters = Vec::new();
    for _ in 0..HEALTH_WAITERS {
        let mut waiter = blobs.health(token());
        assert!(futures::poll!(waiter.as_mut()).is_pending());
        waiters.push(waiter);
    }
    entered.recv().await.unwrap();
    assert_eq!(blobs.health(token()).await.unwrap_err(), Error::Busy);
    assert!(blobs.shared.healthy.load(Ordering::Acquire));
    assert_eq!(blobs.metrics().active_jobs, 1);
    drop(waiters.pop());
    let mut replacement = blobs.health(token());
    assert!(futures::poll!(replacement.as_mut()).is_pending());
    drop(release);
    for waiter in waiters {
        waiter.await.unwrap();
    }
    replacement.await.unwrap();
    assert_eq!(
        blobs.shared.health_waiters.available_permits(),
        HEALTH_WAITERS
    );
    blobs.close().await.unwrap();
}

#[tokio::test]
async fn close_prevents_shared_recovery() {
    let root = root();
    let blobs = start_blob(FsBlobStore::open(root.path())).await.unwrap();
    failed(&blobs.shared, Error::StorageUnavailable);
    let (release, mut entered) = pause_health(&blobs);
    let mut first = blobs.health(token());
    assert!(futures::poll!(first.as_mut()).is_pending());
    entered.recv().await.unwrap();
    let mut second = blobs.health(token());
    assert!(futures::poll!(second.as_mut()).is_pending());
    let close = blobs.close();
    drop(release);
    assert_eq!(first.await.unwrap_err(), Error::Shutdown);
    assert_eq!(second.await.unwrap_err(), Error::Shutdown);
    close.await.unwrap();
    assert!(!blobs.shared.healthy.load(Ordering::Acquire));
}

#[tokio::test]
async fn second_recovery_waiter_accepts_already_recovered_epoch_with_ordinary_work() {
    let root = root();
    let blobs = start_blob(FsBlobStore::open(root.path())).await.unwrap();
    failed(&blobs.shared, Error::StorageUnavailable);
    let (release, mut entered) = pause_health(&blobs);
    let mut first = blobs.health(token());
    assert!(futures::poll!(first.as_mut()).is_pending());
    entered.recv().await.unwrap();
    let mut second = blobs.health(token());
    assert!(futures::poll!(second.as_mut()).is_pending());
    drop(release);
    first.await.unwrap();
    let gate = Arc::new((Mutex::new(false), Condvar::new()));
    let release = ReleaseOnDrop(gate.clone());
    let (entered, entry) = tokio::sync::oneshot::channel();
    let ordinary = tokio::spawn(blobs.job(token(), false, move |_, _, _| {
        entered.send(()).unwrap();
        let (lock, ready) = &*gate;
        let mut released = lock.lock().unwrap();
        while !*released {
            released = ready.wait(released).unwrap();
        }
        Ok(())
    }));
    entry.await.unwrap();
    second.await.unwrap();
    drop(release);
    ordinary.await.unwrap().unwrap();
    blobs.close().await.unwrap();
}

#[tokio::test]
async fn newer_probe_does_not_block_completed_recovery_acknowledgement() {
    let root = root();
    let blobs = start_blob(FsBlobStore::open(root.path())).await.unwrap();
    failed(&blobs.shared, Error::StorageUnavailable);
    let (release, mut entered) = pause_health(&blobs);
    let mut first = blobs.health(token());
    assert!(futures::poll!(first.as_mut()).is_pending());
    entered.recv().await.unwrap();
    let mut outcome = blobs
        .shared
        .health_flight
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .outcome
        .clone();
    drop(release);
    outcome.changed().await.unwrap();
    assert!(!blobs.shared.healthy.load(Ordering::Acquire));
    let (release, mut entered) = pause_health(&blobs);
    let mut newer = blobs.health(token());
    assert!(futures::poll!(newer.as_mut()).is_pending());
    entered.recv().await.unwrap();
    assert_eq!(blobs.metrics().active_jobs, 1);
    first.await.unwrap();
    assert!(blobs.shared.healthy.load(Ordering::Acquire));
    drop(release);
    newer.await.unwrap();
    blobs.close().await.unwrap();
}

#[tokio::test]
async fn configured_timeout_bounds_are_checked_before_creating_a_root() {
    let parent = root();
    let path = parent.path().join("not-created");
    for operation_timeout in [
        Duration::ZERO,
        Duration::from_secs(60) + Duration::from_nanos(1),
        Duration::MAX,
    ] {
        assert!(matches!(
            start_blob(FsBlobStore::open_with_limit_and_timeout(
                &path,
                1024,
                operation_timeout
            ))
            .await,
            Err(Error::InvalidInput)
        ));
        assert!(!path.exists());
    }
    let blobs = start_blob(FsBlobStore::open_with_limit_and_timeout(
        &path,
        1024,
        Duration::from_secs(60),
    ))
    .await
    .unwrap();
    blobs.health(token()).await.unwrap();
    blobs.close().await.unwrap();
}

// Freeze only after the real blocking hook has been entered. Keeping a runnable
// task prevents Tokio from auto-advancing to an unrelated future timer while a
// filesystem worker is blocked on a native condition variable. Resume before
// releasing that worker so fsync completion again uses the normal real deadline.
struct FrozenClock(tokio::task::JoinHandle<()>);
impl FrozenClock {
    async fn new() -> Self {
        tokio::time::pause();
        let keeper = Self(tokio::spawn(async {
            loop {
                tokio::task::yield_now().await;
            }
        }));
        // Native hook entry can precede the owned watchdog's first poll. These
        // current-thread tests queue this marker after the already-spawned
        // watchdogs; awaiting it lets them arm before any positive time advance.
        tokio::spawn(async {}).await.unwrap();
        keeper
    }
}
impl Drop for FrozenClock {
    fn drop(&mut self) {
        self.0.abort();
        tokio::time::resume();
    }
}

// Once a timer has fired, let its independently owned supervisor/coordinator run
// without advancing time further. The real bound catches a broken notification
// path instead of allowing a frozen-clock test to hang indefinitely.
async fn settle(mut completed: impl FnMut() -> bool) {
    let end = std::time::Instant::now() + Duration::from_secs(10);
    while !completed() {
        assert!(
            std::time::Instant::now() < end,
            "timer completion did not settle"
        );
        tokio::task::yield_now().await;
    }
}

async fn configured_jobs_drain(blobs: &FsBlobStore) {
    tokio::time::timeout(DEADLINE + Duration::from_secs(2), async {
        loop {
            let completed = blobs.shared.completed.notified();
            tokio::pin!(completed);
            completed.as_mut().enable();
            if blobs.metrics().active_jobs == 0 {
                return;
            }
            completed.await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn configured_health_budgets_are_independent_and_late_success_stays_unhealthy() {
    let short_root = root();
    let long_root = root();
    let short = start_blob(FsBlobStore::open_with_limit_and_timeout(
        short_root.path(),
        1024,
        Duration::from_secs(5),
    ))
    .await
    .unwrap();
    let long = start_blob(FsBlobStore::open_with_limit_and_timeout(
        long_root.path(),
        1024,
        Duration::from_secs(30),
    ))
    .await
    .unwrap();
    let (short_release, mut short_entered) = pause_health(&short);
    let (long_release, mut long_entered) = pause_health(&long);
    let short_probe = tokio::spawn(short.health(token()));
    short_entered.recv().await.unwrap();
    let long_probe = tokio::spawn(long.health(token()));
    long_entered.recv().await.unwrap();
    let clock = FrozenClock::new().await;
    tokio::time::advance(Duration::from_secs(5) + Duration::from_millis(1)).await;
    settle(|| short_probe.is_finished()).await;
    assert_eq!(short_probe.await.unwrap(), Err(Error::StorageUnavailable));
    assert!(!long_probe.is_finished());
    assert_eq!(short.metrics().active_jobs, 1);
    assert_eq!(long.metrics().active_jobs, 1);
    drop(clock);
    drop(short_release);
    drop(long_release);
    long_probe.await.unwrap().unwrap();
    configured_jobs_drain(&short).await;
    assert!(!short.shared.healthy.load(Ordering::Acquire));
    assert_eq!(
        short.job(token(), false, |_, _, _| Ok(())).await,
        Err(Error::StorageUnavailable)
    );
    *short.shared.health_hook.lock().unwrap() = None;
    short.health(token()).await.unwrap();
    assert!(short.shared.healthy.load(Ordering::Acquire));
    short.close().await.unwrap();
    long.close().await.unwrap();
}

#[tokio::test]
async fn configured_watchdog_survives_ordinary_waiter_cancellation_and_drop() {
    for cancel in [true, false] {
        let root = root();
        let blobs = start_blob(FsBlobStore::open_with_limit_and_timeout(
            root.path(),
            1024,
            Duration::from_secs(5),
        ))
        .await
        .unwrap();
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        let release = ReleaseOnDrop(gate.clone());
        let (entered, entry) = tokio::sync::oneshot::channel();
        let cancellation = token();
        let waiter = tokio::spawn(blobs.job(cancellation.clone(), false, move |_, _, _| {
            entered.send(()).unwrap();
            let (lock, ready) = &*gate;
            let mut released = lock.lock().unwrap();
            while !*released {
                released = ready.wait(released).unwrap();
            }
            Ok(())
        }));
        entry.await.unwrap();
        let clock = FrozenClock::new().await;
        if cancel {
            cancellation.cancel();
            assert_eq!(waiter.await.unwrap(), Err(Error::Cancelled));
        } else {
            waiter.abort();
            assert!(waiter.await.unwrap_err().is_cancelled());
        }
        tokio::time::advance(Duration::from_secs(5) + Duration::from_millis(1)).await;
        settle(|| !blobs.shared.healthy.load(Ordering::Acquire)).await;
        assert_eq!(blobs.metrics().active_jobs, 1);
        assert_eq!(blobs.health(token()).await, Err(Error::Busy));
        drop(clock);
        drop(release);
        configured_jobs_drain(&blobs).await;
        assert!(!blobs.shared.healthy.load(Ordering::Acquire));
        blobs.health(token()).await.unwrap();
        blobs.close().await.unwrap();
    }
}

#[tokio::test]
async fn configured_close_budget_does_not_release_a_blocked_jobs_admission() {
    let root = root();
    let blobs = start_blob(FsBlobStore::open_with_limit_and_timeout(
        root.path(),
        1024,
        Duration::from_secs(30),
    ))
    .await
    .unwrap();
    let gate = Arc::new((Mutex::new(false), Condvar::new()));
    let release = ReleaseOnDrop(gate.clone());
    let (entered, entry) = tokio::sync::oneshot::channel();
    let waiter = tokio::spawn(blobs.job(token(), false, move |_, _, _| {
        entered.send(()).unwrap();
        let (lock, ready) = &*gate;
        let mut released = lock.lock().unwrap();
        while !*released {
            released = ready.wait(released).unwrap();
        }
        Ok(())
    }));
    entry.await.unwrap();
    let clock = FrozenClock::new().await;
    let closing = blobs.close();
    tokio::pin!(closing);
    assert!(futures::poll!(closing.as_mut()).is_pending());
    tokio::time::advance(Duration::from_secs(29)).await;
    assert!(futures::poll!(closing.as_mut()).is_pending());
    tokio::time::advance(Duration::from_secs(1) + Duration::from_millis(1)).await;
    assert_eq!(closing.await, Err(Error::StorageUnavailable));
    settle(|| blobs.shared.counters.failures.load(Ordering::Acquire) > 0).await;
    assert_eq!(blobs.metrics().active_jobs, 1);
    assert_eq!(blobs.shared.slots.available_permits(), JOBS - 1);
    assert_eq!(
        blobs.job(token(), false, |_, _, _| Ok(())).await,
        Err(Error::Shutdown)
    );
    drop(clock);
    drop(release);
    assert_eq!(waiter.await.unwrap(), Err(Error::StorageUnavailable));
    configured_jobs_drain(&blobs).await;
    assert!(!blobs.shared.healthy.load(Ordering::Acquire));
    blobs.close().await.unwrap();
}

#[tokio::test]
async fn configured_health_watchdog_survives_every_waiter_leaving() {
    for cancel in [true, false] {
        let root = root();
        let blobs = start_blob(FsBlobStore::open_with_limit_and_timeout(
            root.path(),
            1024,
            Duration::from_secs(5),
        ))
        .await
        .unwrap();
        failed(&blobs.shared, Error::StorageUnavailable);
        let (release, mut entered) = pause_health(&blobs);
        let cancellation = token();
        let mut first = blobs.health(cancellation.clone());
        assert!(futures::poll!(first.as_mut()).is_pending());
        entered.recv().await.unwrap();
        let clock = FrozenClock::new().await;
        let outcome = blobs
            .shared
            .health_flight
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .outcome
            .clone();
        if cancel {
            cancellation.cancel();
            assert_eq!(first.await, Err(Error::Cancelled));
        } else {
            drop(first);
        }
        tokio::time::advance(Duration::from_secs(5) + Duration::from_millis(1)).await;
        settle(|| outcome.borrow().is_some()).await;
        assert_eq!(
            outcome.borrow().as_ref().unwrap().result,
            Err(Error::StorageUnavailable)
        );
        assert_eq!(blobs.metrics().active_jobs, 1);
        assert!(!blobs.shared.healthy.load(Ordering::Acquire));
        drop(clock);
        drop(release);
        configured_jobs_drain(&blobs).await;
        assert!(!blobs.shared.healthy.load(Ordering::Acquire));
        *blobs.shared.health_hook.lock().unwrap() = None;
        blobs.health(token()).await.unwrap();
        blobs.close().await.unwrap();
    }
}
