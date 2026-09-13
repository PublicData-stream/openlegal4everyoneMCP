use super::*;
use openlegal_domain::{Provenance, Record};
fn private_root() -> tempfile::TempDir {
    use std::os::unix::fs::PermissionsExt;
    let root = tempfile::tempdir().unwrap();
    std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    root
}

fn key(id: &str) -> PersistentKey {
    PersistentKey {
        history: HistoryKey {
            namespace: "fixture".into(),
            provider: "synthetic".into(),
            dataset: "records".into(),
            query: Query::Get {
                source: "layout_a".into(),
                id: id.into(),
            },
        },
        processor_version: "v1".into(),
        schema_version: 1,
    }
}
fn payload(key: &PersistentKey, body: &str, now: u64) -> Payload {
    let Query::Get { id, .. } = &key.history.query else {
        panic!("get fixture")
    };
    Payload {
        data: RetrievalData::Get(Record {
            source: "layout_a".into(),
            id: id.clone(),
            title: "Fictional".into(),
            body: body.into(),
            synthetic: true,
        }),
        provenance: Provenance {
            provider: "synthetic".into(),
            dataset: "records".into(),
            source_reference: "https://example.test/fixture".into(),
            payload_sha256: digest(body.as_bytes()),
            processor_version: key.processor_version.clone(),
            retrieved_at: now,
            validated_at: now,
        },
        raw: body.as_bytes().to_vec(),
        snapshot: None,
    }
}
fn publish(engine: &mut Engine, key: &PersistentKey, body: &str, now: u64) -> Payload {
    assert!(matches!(
        engine
            .stage(key.clone(), payload(key, body, now), now)
            .unwrap(),
        Response::Prepared
    ));
    let Response::Payload(Some(value)) = engine.commit().unwrap() else {
        panic!("committed payload")
    };
    value
}
fn policy() -> RetentionPolicy {
    RetentionPolicy {
        max_bytes: 16 * 1024 * 1024,
        ..RetentionPolicy::default()
    }
}

#[test]
fn occurrence_identity_deduplicates_only_the_current_head_and_survives_restart() {
    let _fixture_gate = super::super::PROCESS_FIXTURE_GATE.blocking_lock();
    let root = private_root();
    let key = key("001");
    let mut engine = Engine::open(root.path(), policy()).unwrap();
    let a = publish(&mut engine, &key, "A", 100);
    let repeated = publish(&mut engine, &key, "A", 110);
    assert_eq!(a.snapshot, repeated.snapshot);
    assert_eq!(repeated.provenance.retrieved_at, 100);
    assert_eq!(repeated.provenance.validated_at, 110);
    let b = publish(&mut engine, &key, "B", 120);
    let a2 = publish(&mut engine, &key, "A", 130);
    assert_ne!(a.snapshot, a2.snapshot);
    assert_ne!(b.snapshot, a2.snapshot);
    let Response::List(page) = engine.list(&key.history, None, 20, 130).unwrap() else {
        panic!()
    };
    assert_eq!(
        page.snapshots
            .iter()
            .map(|v| v.sequence)
            .collect::<Vec<_>>(),
        vec![3, 2, 1]
    );
    assert_eq!(page.snapshots[2].captured_at, 100);
    drop(engine);
    let mut reopened = Engine::open(root.path(), policy()).unwrap();
    let Response::Payload(Some(value)) = reopened.lookup(&key, 131).unwrap() else {
        panic!()
    };
    assert_eq!(value.snapshot, a2.snapshot);
    assert_eq!(value.provenance.validated_at, 130);
    let Response::Snapshot(historical) = reopened
        .get(&key.history, &a.snapshot.unwrap().snapshot_id, 131)
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(historical.provenance.validated_at, 100);
}

#[test]
fn every_commit_failpoint_recovers_an_old_or_new_complete_head() {
    let _fixture_gate = super::super::PROCESS_FIXTURE_GATE.blocking_lock();
    let mut old_seen = false;
    let mut new_seen = false;
    for cut in 0..12 {
        let root = private_root();
        let key = key("001");
        let mut engine = Engine::open(root.path(), policy()).unwrap();
        publish(&mut engine, &key, "old", 100);
        FAIL_AFTER.with(|f| f.set(Some(cut)));
        let result = engine
            .stage(key.clone(), payload(&key, "new", 101), 101)
            .and_then(|_| engine.commit());
        FAIL_AFTER.with(|f| f.set(None));
        drop(engine);
        let mut reopened = Engine::open(root.path(), policy()).unwrap();
        let Response::Payload(Some(value)) = reopened.lookup(&key, 102).unwrap() else {
            panic!("lost head after cut {cut}")
        };
        let RetrievalData::Get(record) = value.data else {
            panic!()
        };
        match record.body.as_str() {
            "old" => old_seen = true,
            "new" => new_seen = true,
            _ => panic!("partial head"),
        }
        if result.is_ok() {
            assert_eq!(record.body, "new");
        }
        assert!(
            !names(&reopened.root)
                .unwrap()
                .iter()
                .any(|v| v.starts_with(".staged"))
        );
    }
    assert!(old_seen && new_seen);
}

#[test]
fn prepared_but_unauthorized_snapshot_is_never_promoted() {
    let _fixture_gate = super::super::PROCESS_FIXTURE_GATE.blocking_lock();
    let root = private_root();
    let key = key("001");
    let mut engine = Engine::open(root.path(), policy()).unwrap();
    let first = publish(&mut engine, &key, "A", 100);
    engine
        .stage(key.clone(), payload(&key, "B", 101), 101)
        .unwrap();
    drop(engine);
    let mut reopened = Engine::open(root.path(), policy()).unwrap();
    let Response::Payload(Some(value)) = reopened.lookup(&key, 102).unwrap() else {
        panic!()
    };
    assert_eq!(value.snapshot, first.snapshot);
    assert_eq!(reopened.metrics.snapshots, 1);
}

#[test]
fn checksum_rejects_normalized_data_corruption_and_isolation_keeps_other_query() {
    let _fixture_gate = super::super::PROCESS_FIXTURE_GATE.blocking_lock();
    let root = private_root();
    let a = key("001");
    let b = key("002");
    let mut engine = Engine::open(root.path(), policy()).unwrap();
    let first = publish(&mut engine, &a, "AAAA", 100);
    publish(&mut engine, &b, "B", 100);
    let id = first.snapshot.unwrap().snapshot_id;
    let path = root.path().join(format!("{id}.snapshot"));
    let mut bytes = std::fs::read(&path).unwrap();
    let offset = bytes.windows(4).position(|v| v == b"AAAA").unwrap();
    bytes[offset] = b'Z';
    std::fs::write(&path, bytes).unwrap();
    assert!(matches!(engine.lookup(&a, 101), Err(Error::StorageCorrupt)));
    engine.staged_isolation = Some((key_id(&a.history).unwrap(), Some(id), true, 101));
    assert!(matches!(engine.commit().unwrap(), Response::Payload(None)));
    assert!(matches!(
        engine.lookup(&a, 101).unwrap(),
        Response::Payload(None)
    ));
    assert!(matches!(
        engine.lookup(&b, 101).unwrap(),
        Response::Payload(Some(_))
    ));
    let repaired = publish(&mut engine, &a, "fixed", 102);
    assert_eq!(repaired.snapshot.unwrap().captured_at, 102);
}

#[test]
fn retention_boundary_clock_rollback_and_per_query_cap() {
    let _fixture_gate = super::super::PROCESS_FIXTURE_GATE.blocking_lock();
    let root = private_root();
    let key = key("001");
    let mut p = policy();
    p.retention_days = 1;
    p.max_snapshots_per_query = 2;
    let mut engine = Engine::open(root.path(), p).unwrap();
    let a = publish(&mut engine, &key, "A", 100);
    publish(&mut engine, &key, "B", 101);
    publish(&mut engine, &key, "C", 102);
    assert_eq!(engine.metrics.snapshots, 2);
    assert!(matches!(
        engine.get(&key.history, &a.snapshot.unwrap().snapshot_id, 103),
        Err(Error::SnapshotUnavailable)
    ));
    assert!(matches!(
        engine.lookup(&key, 99).unwrap(),
        Response::Payload(None)
    ));
    let Response::List(page) = engine.list(&key.history, None, 20, 86_501).unwrap() else {
        panic!()
    };
    assert_eq!(page.snapshots.len(), 1);
    engine.maintain(86_502).unwrap();
    assert_eq!(engine.metrics.snapshots, 0);
}

#[test]
fn cursors_are_scoped_to_key_incarnation_and_have_a_fixed_high_watermark() {
    let _fixture_gate = super::super::PROCESS_FIXTURE_GATE.blocking_lock();
    let root = private_root();
    let key = key("001");
    let mut engine = Engine::open(root.path(), policy()).unwrap();
    publish(&mut engine, &key, "A", 100);
    publish(&mut engine, &key, "B", 101);
    let Response::List(page) = engine.list(&key.history, None, 1, 102).unwrap() else {
        panic!()
    };
    let cursor = page.next_cursor.unwrap();
    publish(&mut engine, &key, "C", 103);
    let Response::List(page) = engine
        .list(&key.history, Some(cursor.clone()), 20, 104)
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(
        page.snapshots
            .iter()
            .map(|v| v.sequence)
            .collect::<Vec<_>>(),
        vec![1]
    );
    engine.maintain(31 * 86400).unwrap();
    engine.evict_oldest(None).unwrap();
    publish(&mut engine, &key, "new", 31 * 86400);
    assert!(matches!(
        engine.list(&key.history, Some(cursor), 20, 31 * 86400),
        Err(Error::InvalidInput)
    ));
}

#[test]
fn eviction_prefers_non_heads_then_oldest_validation() {
    let _fixture_gate = super::super::PROCESS_FIXTURE_GATE.blocking_lock();
    let root = private_root();
    let a = key("001");
    let b = key("002");
    let mut engine = Engine::open(root.path(), policy()).unwrap();
    publish(&mut engine, &a, "A", 1);
    publish(&mut engine, &b, "B", 2);
    publish(&mut engine, &b, "C", 3);
    engine.evict_oldest(None).unwrap();
    assert!(matches!(
        engine.lookup(&a, 4).unwrap(),
        Response::Payload(Some(_))
    ));
    let Response::List(page) = engine.list(&b.history, None, 20, 4).unwrap() else {
        panic!()
    };
    assert_eq!(page.snapshots.len(), 1);
    assert_eq!(page.snapshots[0].sequence, 2);
}

#[test]
fn root_lock_symlink_and_nonregular_files_fail_closed() {
    let _fixture_gate = super::super::PROCESS_FIXTURE_GATE.blocking_lock();
    let root = private_root();
    let engine = Engine::open(root.path(), policy()).unwrap();
    assert!(Engine::open(root.path(), policy()).is_err());
    drop(engine);
    let parent = tempfile::tempdir().unwrap();
    let link = parent.path().join("cache");
    std::os::unix::fs::symlink(root.path(), &link).unwrap();
    assert!(Engine::open(&link, policy()).is_err());
    std::fs::remove_file(root.path().join("FORMAT")).unwrap();
    std::os::unix::fs::symlink("/dev/zero", root.path().join("FORMAT")).unwrap();
    assert!(Engine::open(root.path(), policy()).is_err());
}

#[test]
fn expired_reference_removal_is_crash_consistent_before_file_deletion() {
    let _fixture_gate = super::super::PROCESS_FIXTURE_GATE.blocking_lock();
    for cut in 0..7 {
        let root = private_root();
        let key = key("001");
        let mut engine = Engine::open(root.path(), policy()).unwrap();
        publish(&mut engine, &key, "A", 100);
        FAIL_AFTER.with(|v| v.set(Some(cut)));
        let _ = engine.maintain(31 * 86400);
        FAIL_AFTER.with(|v| v.set(None));
        drop(engine);
        let reopened = Engine::open(root.path(), policy()).unwrap();
        let Response::List(page) = reopened.list(&key.history, None, 20, 31 * 86400).unwrap()
        else {
            panic!()
        };
        assert!(page.snapshots.is_empty());
        for m in reopened.manifests.values() {
            for e in &m.entries {
                assert!(reopened.snapshot(&m.key, e).is_ok());
            }
        }
    }
}

#[test]
fn processor_heads_remain_independent_and_clock_rollback_creates_a_new_occurrence() {
    let _fixture_gate = super::super::PROCESS_FIXTURE_GATE.blocking_lock();
    let root = private_root();
    let v1 = key("001");
    let mut v2 = v1.clone();
    v2.processor_version = "v2".into();
    let mut engine = Engine::open(root.path(), policy()).unwrap();
    let a = publish(&mut engine, &v1, "A", 100);
    let b = publish(&mut engine, &v2, "B", 101);
    let Response::Payload(Some(value)) = engine.lookup(&v1, 102).unwrap() else {
        panic!()
    };
    assert_eq!(value.snapshot, a.snapshot);
    let Response::Payload(Some(value)) = engine.lookup(&v2, 102).unwrap() else {
        panic!()
    };
    assert_eq!(value.snapshot, b.snapshot);
    let rollback = publish(&mut engine, &v1, "A", 90);
    assert_ne!(rollback.snapshot, a.snapshot);
    assert_eq!(rollback.snapshot.unwrap().captured_at, 90);
    let Response::Snapshot(old) = engine
        .get(&v1.history, &a.snapshot.unwrap().snapshot_id, 90)
        .unwrap()
    else {
        panic!()
    };
    assert!(old.clock_anomaly);
}

#[test]
fn expired_target_never_deduplicates_after_a_bounded_global_sweep() {
    let _fixture_gate = super::super::PROCESS_FIXTURE_GATE.blocking_lock();
    let root = private_root();
    let mut engine = Engine::open(root.path(), policy()).unwrap();
    for n in 0..40 {
        publish(&mut engine, &key(&format!("{n:03}")), "A", 100);
    }
    // Choose a key beyond the hash table's first maintenance batch.
    let target = engine.manifests.values().nth(39).unwrap().key.clone();
    let target = PersistentKey {
        history: target,
        processor_version: "v1".into(),
        schema_version: 1,
    };
    let old = engine
        .manifests
        .get(&key_id(&target.history).unwrap())
        .unwrap()
        .entries[0]
        .summary
        .snapshot_id
        .clone();
    let replacement = publish(&mut engine, &target, "A", 31 * 86400);
    assert_ne!(replacement.snapshot.as_ref().unwrap().snapshot_id, old);
    assert_eq!(replacement.snapshot.unwrap().captured_at, 31 * 86400);
}

#[test]
fn permissive_modes_and_wrong_owner_are_rejected() {
    let _fixture_gate = super::super::PROCESS_FIXTURE_GATE.blocking_lock();
    use std::os::unix::fs::PermissionsExt;
    let root = private_root();
    std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o755)).unwrap();
    assert!(Engine::open(root.path(), policy()).is_err());
    std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    drop(Engine::open(root.path(), policy()).unwrap());
    std::fs::set_permissions(
        root.path().join("FORMAT"),
        std::fs::Permissions::from_mode(0o644),
    )
    .unwrap();
    assert!(Engine::open(root.path(), policy()).is_err());
    if rustix::process::geteuid().is_root() {
        let other = private_root();
        fs::chown(
            other.path(),
            Some(rustix::process::Uid::from_raw(65534)),
            None,
        )
        .unwrap();
        assert!(Engine::open(other.path(), policy()).is_err());
    }
}

#[test]
fn lowering_retained_count_limit_fails_startup_without_deleting_history() {
    let _fixture_gate = super::super::PROCESS_FIXTURE_GATE.blocking_lock();
    let root = private_root();
    let key = key("001");
    let mut engine = Engine::open(root.path(), policy()).unwrap();
    publish(&mut engine, &key, "A", 100);
    publish(&mut engine, &key, "B", 101);
    drop(engine);
    let mut lower = policy();
    lower.max_snapshots_per_query = 1;
    assert!(matches!(
        Engine::open(root.path(), lower),
        Err(Error::StorageCapacity)
    ));
    let engine = Engine::open(root.path(), policy()).unwrap();
    let Response::List(page) = engine.list(&key.history, None, 20, 102).unwrap() else {
        panic!()
    };
    assert_eq!(page.snapshots.len(), 2);
}

#[test]
fn quarantine_is_bounded_accounted_and_never_served() {
    let _fixture_gate = super::super::PROCESS_FIXTURE_GATE.blocking_lock();
    let root = private_root();
    let key = key("001");
    let mut engine = Engine::open(root.path(), policy()).unwrap();
    for i in 0..34 {
        let value = publish(&mut engine, &key, "A", 100 + i);
        let id = value.snapshot.unwrap().snapshot_id;
        engine.staged_isolation = Some((key_id(&key.history).unwrap(), Some(id), true, 100 + i));
        assert!(matches!(engine.commit().unwrap(), Response::Payload(None)));
    }
    assert_eq!(engine.quarantine.len(), 32);
    assert!(engine.metrics.bytes > engine.quarantine.iter().map(|q| q.2).sum::<u64>());
    assert!(matches!(
        engine.lookup(&key, 135).unwrap(),
        Response::Payload(None)
    ));
    drop(engine);
    let mut reopened = Engine::open(root.path(), policy()).unwrap();
    assert_eq!(reopened.quarantine.len(), 32);
    reopened.maintain(31 * 86400).unwrap();
    assert!(reopened.quarantine.is_empty());
}

fn large_publish(engine: &mut Engine, key: &PersistentKey, version: u8, now: u64) {
    let mut value = payload(key, "bounded processed fixture", now);
    value.raw = vec![version; 1024 * 1024];
    value.provenance.payload_sha256 = digest(&value.raw);
    engine.stage(key.clone(), value, now).unwrap();
    engine.commit().unwrap();
}

#[test]
fn corrupt_manifest_reclaims_near_capacity_orphans_before_same_call_repair() {
    let _fixture_gate = super::super::PROCESS_FIXTURE_GATE.blocking_lock();
    let root = private_root();
    let key = key("001");
    let mut engine = Engine::open(root.path(), policy()).unwrap();
    for version in 0..11 {
        large_publish(&mut engine, &key, version, 100 + u64::from(version));
    }
    assert!(engine.metrics.bytes > 10 * 1024 * 1024);
    let id = key_id(&key.history).unwrap();
    drop(engine);
    std::fs::write(
        root.path().join(format!("{id}.manifest")),
        b"corrupt known manifest",
    )
    .unwrap();
    let mut engine = Engine::open(root.path(), policy()).unwrap();
    assert!(matches!(
        engine.lookup(&key, 120),
        Err(Error::StorageCorrupt)
    ));
    engine.staged_isolation = Some((id, None, true, 120));
    assert!(matches!(engine.commit().unwrap(), Response::Payload(None)));
    assert_eq!(engine.metrics.snapshots, 0);
    assert!(engine.metrics.bytes < 1024 * 1024);
    large_publish(&mut engine, &key, 12, 121);
    assert!(matches!(
        engine.lookup(&key, 122).unwrap(),
        Response::Payload(Some(_))
    ));
}

#[test]
fn lowered_byte_limit_must_preserve_cleanup_workspace_without_mutating_history() {
    let _fixture_gate = super::super::PROCESS_FIXTURE_GATE.blocking_lock();
    let root = private_root();
    let key = key("001");
    let mut original = policy();
    original.max_bytes = 32 * 1024 * 1024;
    let mut engine = Engine::open(root.path(), original.clone()).unwrap();
    for version in 0..20 {
        large_publish(&mut engine, &key, version, 100 + u64::from(version));
    }
    let bytes = engine.metrics.bytes;
    assert!(bytes > 16 * 1024 * 1024);
    drop(engine);
    let mut lower = original.clone();
    lower.max_bytes = bytes + 1;
    assert!(matches!(
        Engine::open(root.path(), lower),
        Err(Error::StorageCapacity)
    ));
    let reopened = Engine::open(root.path(), original).unwrap();
    assert_eq!(reopened.metrics.snapshots, 20);
    assert_eq!(reopened.metrics.bytes, bytes);
}

#[test]
fn unchanged_full_count_revalidation_needs_no_invalidation_barrier() {
    let _fixture_gate = super::super::PROCESS_FIXTURE_GATE.blocking_lock();
    let root = private_root();
    let key = key("001");
    let mut p = policy();
    p.max_snapshots_per_query = 1;
    let mut engine = Engine::open(root.path(), p).unwrap();
    let first = publish(&mut engine, &key, "A", 100);
    // Global count admission must also distinguish updates from new occurrences.
    engine.metrics.snapshots = MAX_SNAPSHOTS;
    assert!(matches!(
        engine
            .prepare_publish(key.clone(), payload(&key, "A", 101), 101)
            .unwrap(),
        Response::Prepared
    ));
    assert!(engine.staged_publish.is_none());
    assert!(engine.staged_maintenance.is_none());
    assert!(engine.staged_isolation.is_none());
    let Response::Payload(Some(value)) = engine.commit().unwrap() else {
        panic!()
    };
    assert_eq!(first.snapshot, value.snapshot);
    assert_eq!(value.provenance.retrieved_at, 100);
}

#[test]
fn bootstrap_marker_recovers_every_prepublication_failure_without_accepting_unknown_format() {
    let _fixture_gate = super::super::PROCESS_FIXTURE_GATE.blocking_lock();
    for cut in 0..6 {
        let root = private_root();
        FAIL_AFTER.with(|v| v.set(Some(cut)));
        let _ = Engine::open(root.path(), policy());
        FAIL_AFTER.with(|v| v.set(None));
        let engine = Engine::open(root.path(), policy()).unwrap();
        assert_eq!(engine.read_bytes("FORMAT", 128).unwrap(), FORMAT);
        assert!(
            !names(&engine.root)
                .unwrap()
                .iter()
                .any(|v| v == ".staged-format")
        );
    }
    let root = private_root();
    drop(Engine::open(root.path(), policy()).unwrap());
    std::fs::write(root.path().join("FORMAT"), b"unsupported future format").unwrap();
    assert!(Engine::open(root.path(), policy()).is_err());
    assert_eq!(
        std::fs::read(root.path().join("FORMAT")).unwrap(),
        b"unsupported future format"
    );
}

#[test]
fn recovery_rejects_unsafe_staging_and_orphans_before_deleting_any_file() {
    use std::os::unix::fs::{PermissionsExt, symlink};
    let _fixture_gate = super::super::PROCESS_FIXTURE_GATE.blocking_lock();
    let orphan = format!("{}.snapshot", "a".repeat(64));
    for name in [".staged-format", ".staged-manifest", orphan.as_str()] {
        for kind in ["symlink", "hardlink", "nonprivate"] {
            let root = private_root();
            drop(Engine::open(root.path(), policy()).unwrap());
            let outside = tempfile::tempdir().unwrap();
            let evidence = outside.path().join("evidence");
            std::fs::write(&evidence, b"must remain unchanged").unwrap();
            std::fs::set_permissions(&evidence, std::fs::Permissions::from_mode(0o600)).unwrap();
            let victim = root.path().join(name);
            match kind {
                "symlink" => symlink(&evidence, &victim).unwrap(),
                "hardlink" => std::fs::hard_link(&evidence, &victim).unwrap(),
                _ => {
                    std::fs::write(&victim, b"nonprivate staging").unwrap();
                    std::fs::set_permissions(&victim, std::fs::Permissions::from_mode(0o644))
                        .unwrap();
                }
            }
            assert!(
                Engine::open(root.path(), policy()).is_err(),
                "{name}: {kind}"
            );
            assert!(std::fs::symlink_metadata(&victim).is_ok(), "{name}: {kind}");
            assert_eq!(std::fs::read(&evidence).unwrap(), b"must remain unchanged");
        }
    }
}
