use super::*;
use std::os::unix::fs::PermissionsExt;

// An intentionally stalled, trusted fixture executable verifies parent process
// ownership without involving a network or an unbounded filesystem operation.
fn stalled_worker(directory: &Path) -> PathBuf {
    let path = directory.join("worker.sh");
    let ready = protocol::encode(
        &Reply {
            response: Response::Ready,
            metrics: StorageMetrics::default(),
            invalidates_memory: false,
        },
        &[],
    )
    .unwrap();
    let escaped: String = ready.iter().map(|v| format!("\\{v:03o}")).collect();
    let script = format!(
        "#!/bin/sh\nprintf '%s' \"$$\" > '{}.pid'\nprintf '{}'\nexec /bin/sleep 60\n",
        path.display(),
        escaped
    );
    std::fs::write(&path, script).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700)).unwrap();
    path
}
fn key() -> PersistentKey {
    PersistentKey {
        history: HistoryKey {
            namespace: "test".into(),
            provider: "synthetic".into(),
            dataset: "records".into(),
            query: openlegal_domain::Query::Get {
                source: "layout_a".into(),
                id: "001".into(),
            },
        },
        processor_version: "v1".into(),
        schema_version: 1,
    }
}
fn pid(executable: &Path) -> u32 {
    std::fs::read_to_string(format!("{}.pid", executable.display()))
        .unwrap()
        .parse()
        .unwrap()
}
fn gone(pid: u32) -> bool {
    !Path::new(&format!("/proc/{pid}")).exists()
}

#[tokio::test]
async fn cancellation_is_acknowledged_only_after_kill_reap_and_recovery() {
    let _fixture_gate = super::PROCESS_FIXTURE_GATE.lock().await;
    let temp = tempfile::tempdir().unwrap();
    let executable = stalled_worker(temp.path());
    let store = FsCache::open(
        &executable,
        &temp.path().join("cache"),
        RetentionPolicy::default(),
    )
    .await
    .unwrap();
    let original = pid(&executable);
    let token = CancellationToken::new();
    let call = tokio::spawn(store.lookup(key(), 100, token.clone()));
    tokio::time::sleep(Duration::from_millis(50)).await;
    token.cancel();
    let result = tokio::time::timeout(Duration::from_secs(4), call)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(result, Err(Error::Cancelled)));
    assert!(gone(original));
    assert!(store.healthy());
    assert!(store.epoch() > 1);
    let replacement = pid(&executable);
    store.close().await.unwrap();
    assert!(gone(replacement));
}

#[tokio::test]
async fn close_cancels_active_work_and_reaps_without_waiting_for_operation_timeout() {
    let _fixture_gate = super::PROCESS_FIXTURE_GATE.lock().await;
    let temp = tempfile::tempdir().unwrap();
    let executable = stalled_worker(temp.path());
    let store = FsCache::open(
        &executable,
        &temp.path().join("cache"),
        RetentionPolicy::default(),
    )
    .await
    .unwrap();
    let original = pid(&executable);
    let call = tokio::spawn(store.lookup(key(), 100, CancellationToken::new()));
    tokio::time::sleep(Duration::from_millis(50)).await;
    tokio::time::timeout(Duration::from_secs(3), store.close())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(call.await.unwrap(), Err(Error::Shutdown)));
    assert!(gone(original));
    assert!(!store.healthy());
    store.close().await.unwrap();
}

#[tokio::test]
async fn dropped_waiter_still_has_an_owned_child_cleanup() {
    let _fixture_gate = super::PROCESS_FIXTURE_GATE.lock().await;
    let temp = tempfile::tempdir().unwrap();
    let executable = stalled_worker(temp.path());
    let store = FsCache::open(
        &executable,
        &temp.path().join("cache"),
        RetentionPolicy::default(),
    )
    .await
    .unwrap();
    let original = pid(&executable);
    let call = tokio::spawn(store.lookup(key(), 100, CancellationToken::new()));
    tokio::time::sleep(Duration::from_millis(50)).await;
    call.abort();
    let _ = call.await;
    tokio::time::timeout(Duration::from_secs(4), async {
        loop {
            if gone(original) && store.shared.available.load(Ordering::Acquire) && store.epoch() > 1
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    store.close().await.unwrap();
}

#[test]
fn raw_payload_uses_a_binary_section_and_oversized_headers_are_rejected_before_allocation() {
    let raw = vec![255; 1024 * 1024];
    let encoded = protocol::encode(&Request::Commit, &raw).unwrap();
    assert!(encoded.len() < raw.len() + 100);
    let (mut metadata, mut decoded) = protocol::read::<Request>(&mut encoded.as_slice()).unwrap();
    assert_eq!(decoded, raw);
    assert!(metadata.attach(std::mem::take(&mut decoded)).is_err());
    let mut malformed = encoded[..16].to_vec();
    malformed[8..12].copy_from_slice(&u32::MAX.to_be_bytes());
    assert!(matches!(
        protocol::read::<Request>(&mut malformed.as_slice()),
        Err(Error::ResourceLimit)
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn newly_written_worker_fixtures_start_and_reap_under_parallel_launch_pressure() {
    let _fixture_gate = super::PROCESS_FIXTURE_GATE.lock().await;
    let mut workers = tokio::task::JoinSet::new();
    // Prepare and close every executable writer before any fork. CLOEXEC closes
    // an inherited writer only at exec, leaving an ETXTBSY window otherwise.
    let batches: Vec<Vec<_>> = (0..4)
        .map(|_| {
            (0..64)
                .map(|_| {
                    let temp = tempfile::tempdir().unwrap();
                    let executable = stalled_worker(temp.path());
                    (temp, executable)
                })
                .collect()
        })
        .collect();
    for batch in batches {
        workers.spawn(async move {
            for (temp, executable) in batch {
                let cache = FsCache::open(
                    &executable,
                    &temp.path().join("cache"),
                    RetentionPolicy::default(),
                )
                .await
                .unwrap();
                cache.close().await.unwrap();
            }
        });
    }
    while let Some(result) = workers.join_next().await {
        result.unwrap();
    }
}

#[tokio::test]
async fn executable_open_for_write_is_rejected_without_starting_a_worker() {
    let _fixture_gate = super::PROCESS_FIXTURE_GATE.lock().await;
    let temp = tempfile::tempdir().unwrap();
    let executable = stalled_worker(temp.path());
    let writer = std::fs::OpenOptions::new()
        .write(true)
        .open(&executable)
        .unwrap();
    assert!(matches!(
        FsCache::open(
            &executable,
            &temp.path().join("cache"),
            RetentionPolicy::default()
        )
        .await,
        Err(Error::StorageUnavailable)
    ));
    assert!(!Path::new(&format!("{}.pid", executable.display())).exists());
    drop(writer);
    let cache = FsCache::open(
        &executable,
        &temp.path().join("cache"),
        RetentionPolicy::default(),
    )
    .await
    .unwrap();
    cache.close().await.unwrap();
}
