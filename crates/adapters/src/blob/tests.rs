use super::*;
use sha2::{Digest, Sha256};
use std::{
    fs,
    os::unix::fs::{PermissionsExt, symlink},
    sync::{Condvar, Mutex},
};

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
    let blobs = FsBlobStore::open(root.path()).await.unwrap();
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
    let reopened = FsBlobStore::open(root.path()).await.unwrap();
    assert_eq!(
        reopened.get(second, token()).await.unwrap(),
        Some(bytes.to_vec())
    );
    reopened.close().await.unwrap();
}

#[tokio::test]
async fn corrupt_reads_and_existing_puts_never_repair_evidence() {
    let root = root();
    let blobs = FsBlobStore::open(root.path()).await.unwrap();
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
    let blobs = FsBlobStore::open(root.path()).await.unwrap();
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
    let blobs = FsBlobStore::open(root.path()).await.unwrap();
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
        FsBlobStore::open(&root.path().join("linked"))
            .await
            .is_err()
    );
    let blobs = FsBlobStore::open(root.path()).await.unwrap();
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
    let blobs = FsBlobStore::open(root.path()).await.unwrap();
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
    let blobs = FsBlobStore::open(root.path()).await.unwrap();
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
    let blobs = FsBlobStore::open(root.path()).await.unwrap();
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
    let blobs = FsBlobStore::open(root.path()).await.unwrap();
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
    let blobs = FsBlobStore::open(root.path()).await.unwrap();
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
    let blobs = FsBlobStore::open(root.path()).await.unwrap();
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
async fn successful_probe_cannot_clear_a_newer_failure() {
    let root = root();
    let blobs = FsBlobStore::open(root.path()).await.unwrap();
    let (release, mut entered) = pause_health(&blobs);
    let probe = tokio::spawn(blobs.health(token()));
    entered.recv().await.unwrap();
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
    assert!(!blobs.shared.healthy.load(Ordering::Acquire));
    *blobs.shared.health_hook.lock().unwrap() = None;
    blobs.health(token()).await.unwrap();
    assert!(blobs.shared.healthy.load(Ordering::Acquire));
    blobs.close().await.unwrap();
}

#[tokio::test]
async fn recovery_probe_requires_older_jobs_to_drain() {
    let root = root();
    let blobs = FsBlobStore::open(root.path()).await.unwrap();
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
    let blobs = FsBlobStore::open(root.path()).await.unwrap();
    let (release, mut entered) = pause_health(&blobs);
    let probe = tokio::spawn(blobs.health(token()));
    entered.recv().await.unwrap();
    assert_eq!(probe.await.unwrap().unwrap_err(), Error::StorageUnavailable);
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
