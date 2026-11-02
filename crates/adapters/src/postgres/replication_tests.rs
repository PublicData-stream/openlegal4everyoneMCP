use super::*;
use crate::test_support as fixture;
use sqlx::Connection;
#[tokio::test]
async fn actual_cache_root_verifier_rejection_precedes_any_postgres_connection() {
    let _io = crate::blob::test_io_admission().await;
    struct RejectRoot(Arc<AtomicBool>);
    impl openlegal_application::blob::BlobRootVerifier for RejectRoot {
        fn verify(
            &self,
            role: &str,
            _: openlegal_application::blob::BlobRootIdentity,
        ) -> Result<(), Error> {
            assert_eq!(role, "cache_primary");
            self.0.store(true, Ordering::Release);
            Err(Error::StorageCorrupt)
        }
    }
    let directory = tempfile::tempdir_in("/tmp").unwrap();
    let blobs = crate::blob::MirroredBlobStore::open(
        &directory.path().join("primary"),
        &directory.path().join("replica"),
    )
    .await
    .unwrap();
    let called = Arc::new(AtomicBool::new(false));
    let mut replication =
        crate::replication::ReplicationOptions::for_epoch(uuid::Uuid::from_u128(1), "1".repeat(64));
    replication.root_verifier = Some(Arc::new(RejectRoot(called.clone())));
    let result = PostgresStore::open_with_replication(
        "postgresql://127.0.0.1:1/no-server",
        PostgresOptions {
            max_connections: 4,
            tls: PostgresTls::VerifyFull { ca_file: None },
        },
        blobs.clone(),
        RetentionPolicy::default(),
        100,
        StartupMode::Serve,
        Some(replication),
    )
    .await;
    assert!(called.load(Ordering::Acquire));
    assert!(matches!(
        result,
        Err(StartupError::Storage(Error::StorageCorrupt))
    ));
    blobs.close().await.unwrap();
}
#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn strict_health_capacity_never_becomes_recovery_or_uncertain_commit() {
    struct BusyFence;
    impl DurabilityFence for BusyFence {
        fn check(
            &self,
            _: tokio::time::Instant,
            _: CancellationToken,
        ) -> BoxFuture<'static, Result<(), Error>> {
            Box::pin(async { Err(Error::Busy) })
        }
    }
    let fixture = fixture::TestDatabase::new().await;
    let mut store = fixture.open(100).await;
    Arc::get_mut(&mut Arc::get_mut(&mut store).unwrap().inner)
        .unwrap()
        .durability = Some(Arc::new(BusyFence));
    assert_eq!(store.health(100).await, Err(Error::Busy));
    assert!(store.healthy());
    assert_eq!(store.status(), StorageStatus::Ready);
}

#[tokio::test]
async fn strict_constructor_rejects_plaintext_before_connecting() {
    let _io = crate::blob::test_io_admission().await;
    let directory = tempfile::tempdir_in("/tmp").unwrap();
    let blobs = crate::blob::FsBlobStore::open(&directory.path().join("blobs"))
        .await
        .unwrap();
    let result = PostgresStore::open_with_replication(
        "postgresql://127.0.0.1:1/not-a-server",
        fixture::options(),
        blobs,
        RetentionPolicy::default(),
        100,
        StartupMode::Serve,
        Some(crate::replication::ReplicationOptions::for_epoch(
            uuid::Uuid::from_u128(1),
            fixture::replication::FIXTURE_BLOB_BINDING_SHA256.into(),
        )),
    )
    .await;
    assert!(matches!(
        result,
        Err(StartupError::Storage(Error::InvalidInput))
    ));
}

#[tokio::test]
#[ignore = "requires scripts/test-replication.sh real TLS PG18 pair"]
async fn replication_pair_readonly_fence_covers_durable_cut_with_idle_unflushed_wal() {
    fn lsn(value: &serde_json::Value) -> u64 {
        let (high, low) = value.as_str().unwrap().split_once('/').unwrap();
        (u64::from_str_radix(high, 16).unwrap() << 32) | u64::from_str_radix(low, 16).unwrap()
    }
    // These are real SQL/flush observations. The binding remains the fixture's
    // sentinel, not a claim of independently qualified physical Blob media.
    const OBSERVE: &str = "SELECT jsonb_build_object(
        'insert_lsn',pg_current_wal_insert_lsn()::text,
        'local_flush_lsn',pg_current_wal_flush_lsn()::text,
        'proof',openlegal_replication.capture_prefix(),
        'position',openlegal_checkpoint.database_position(),
        'candidate',openlegal_replication.observe_candidate($1),
        'synchronous_commit',current_setting('synchronous_commit'),
        'fsync',current_setting('fsync'),'full_page_writes',current_setting('full_page_writes'))";

    let pair = fixture::replication::TestReplication::new().await;
    let mut admin_options = pair.options();
    admin_options.max_connections = 1;
    let admin = super::pool(&pair.admin_url, admin_options, Duration::from_secs(1))
        .await
        .unwrap();
    let mut connection = admin.acquire().await.unwrap();
    sqlx::query("CREATE TABLE openlegal.idle_wal_fixture(id integer PRIMARY KEY)")
        .execute(&mut *connection)
        .await
        .unwrap();
    let blobs = crate::blob::MirroredBlobStore::open(
        &pair.directory.path().join("idle-primary-blobs"),
        &pair.directory.path().join("idle-replica-blobs"),
    )
    .await
    .unwrap();
    // Finish the actual store's startup/accounting writes BEFORE producing the
    // idle tail. No constructor/health mutation may erase the regression case.
    let store = PostgresStore::open_with_replication(
        &pair.runtime_url,
        pair.options(),
        blobs,
        RetentionPolicy::default(),
        100,
        StartupMode::Serve,
        Some(crate::replication::ReplicationOptions::for_epoch(
            pair.epoch,
            fixture::replication::FIXTURE_BLOB_BINDING_SHA256.into(),
        )),
    )
    .await
    .unwrap();
    let proposed: serde_json::Value = sqlx::query_scalar(
        "SELECT (to_jsonb(c)-'singleton')||jsonb_build_object(
          'standby_client_serial',c.standby_client_serial::text)
         FROM openlegal_replication.control c WHERE singleton",
    )
    .fetch_one(&mut *connection)
    .await
    .unwrap();
    // A real permanent-row COMMIT, using the normal synchronous session policy.
    sqlx::query("INSERT INTO openlegal.idle_wal_fixture VALUES(1)")
        .execute(&mut *connection)
        .await
        .unwrap();
    // Fixture-only alignment BEFORE the idle event leaves a fresh WAL page.
    // Never switch/checkpoint/commit after the event to make a fence pass.
    let _: String = sqlx::query_scalar("SELECT pg_switch_wal()::text")
        .fetch_one(&mut *connection)
        .await
        .unwrap();
    let committed_cut: String = sqlx::query_scalar("SELECT pg_current_wal_flush_lsn()::text")
        .fetch_one(&mut *connection)
        .await
        .unwrap();

    sqlx::query("BEGIN")
        .execute(&mut *connection)
        .await
        .unwrap();
    // Keep this non-XID WAL event's transaction OPEN: autocommitting it can
    // advertise asyncXactLSN to walwriter even with synchronous_commit=on.
    // Holding it open models idle non-commit WAL without any async GUC change.
    let snapshot_end: String = sqlx::query_scalar("SELECT pg_log_standby_snapshot()::text")
        .fetch_one(&mut *connection)
        .await
        .unwrap();
    let before: serde_json::Value = sqlx::query_scalar(OBSERVE)
        .bind(&proposed)
        .fetch_one(&mut *connection)
        .await
        .unwrap();
    assert_eq!(before["synchronous_commit"], "on");
    assert_eq!(before["fsync"], "on");
    assert_eq!(before["full_page_writes"], "on");
    assert_eq!(before["local_flush_lsn"], committed_cut);
    assert!(
        lsn(&serde_json::json!(snapshot_end)) > lsn(&before["local_flush_lsn"])
            && lsn(&before["insert_lsn"]) > lsn(&before["local_flush_lsn"]),
        "the actual fixture must retain an unflushed non-commit tail: {before}"
    );
    for value in [
        &before["proof"]["prefix_lsn"],
        &before["position"]["wal_lsn"],
        &before["candidate"]["candidate"]["prefix_lsn"],
    ] {
        assert_eq!(value, &before["local_flush_lsn"]);
    }
    store
        .inner
        .durability
        .as_ref()
        .unwrap()
        .check(
            tokio::time::Instant::now() + Duration::from_secs(2),
            CancellationToken::new(),
        )
        .await
        .unwrap();
    let after: serde_json::Value = sqlx::query_scalar(OBSERVE)
        .bind(&proposed)
        .fetch_one(&mut *connection)
        .await
        .unwrap();
    // The initial sample proved the idle tail. Background WAL flushing may
    // advance it while the readonly fence runs; separate volatile observations
    // need only stay within the original cut and a freshly sampled upper cut.
    let after_end_flush: String = sqlx::query_scalar("SELECT pg_current_wal_flush_lsn()::text")
        .fetch_one(&mut *connection)
        .await
        .unwrap();
    let after_end_insert: String = sqlx::query_scalar("SELECT pg_current_wal_insert_lsn()::text")
        .fetch_one(&mut *connection)
        .await
        .unwrap();
    let original_cut = lsn(&before["local_flush_lsn"]);
    let upper_cut = lsn(&serde_json::json!(after_end_flush));
    assert!(upper_cut >= original_cut && upper_cut <= lsn(&serde_json::json!(after_end_insert)));
    assert!(lsn(&after["proof"]["flush_lsn"]) >= original_cut);
    for value in [
        &after["local_flush_lsn"],
        &after["proof"]["prefix_lsn"],
        &after["position"]["wal_lsn"],
        &after["candidate"]["candidate"]["prefix_lsn"],
    ] {
        assert!(
            (original_cut..=upper_cut).contains(&lsn(value)),
            "readonly durable cut outside the observed range: {after}"
        );
    }
    for key in [
        "managed_epoch",
        "blob_binding_sha256",
        "primary_system_identifier",
        "timeline",
        "postmaster_started_at",
        "standby_name",
        "standby_role",
        "standby_addr",
        "standby_client_dn",
        "standby_issuer_dn",
        "standby_client_serial",
        "sender_pid",
        "sender_started_at",
    ] {
        assert_eq!(after["proof"][key], before["proof"][key]);
    }
    // All observations and the real two-second proof preceded this rollback.
    sqlx::query("ROLLBACK")
        .execute(&mut *connection)
        .await
        .unwrap();
    drop(connection);
    store.close().await.unwrap();
    admin.close().await;
}

#[tokio::test]
#[ignore = "requires scripts/test-replication.sh real TLS PG18 pair"]
async fn replication_pair_real_flush_authenticated_serial_epoch_and_least_privilege() {
    let pair = fixture::replication::TestReplication::new().await;
    // One connection makes the post-restart lifetime refusal observable on a
    // demonstrably healthy connection, rather than an arbitrary dead idle one.
    let mut runtime_options = pair.options();
    runtime_options.max_connections = 1;
    let runtime = super::pool(&pair.runtime_url, runtime_options, Duration::from_secs(1))
        .await
        .unwrap();
    let admin = super::pool(&pair.admin_url, pair.options(), Duration::from_secs(1))
        .await
        .unwrap();
    let wrong_seed = crate::replication::PostgresDurabilityFence::open(
        runtime.clone(),
        crate::replication::ReplicationOptions::for_epoch(pair.epoch, "2".repeat(64)),
    )
    .await;
    assert!(matches!(wrong_seed, Err(Error::StorageUnavailable)));
    let fence = crate::replication::PostgresDurabilityFence::open(
        runtime.clone(),
        crate::replication::ReplicationOptions::for_epoch(
            pair.epoch,
            fixture::replication::FIXTURE_BLOB_BINDING_SHA256.into(),
        ),
    )
    .await
    .unwrap();
    fence
        .check(
            tokio::time::Instant::now() + Duration::from_secs(2),
            CancellationToken::new(),
        )
        .await
        .unwrap();
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let (healthy, request_cancelled) = tokio::join!(
        fence.check(
            tokio::time::Instant::now() + Duration::from_secs(2),
            CancellationToken::new()
        ),
        fence.check(
            tokio::time::Instant::now() + Duration::from_secs(2),
            cancelled
        )
    );
    assert_eq!(
        healthy,
        Ok(()),
        "a cancelled public waiter must not invalidate a concurrent healthy proof"
    );
    assert_eq!(request_cancelled, Err(Error::Cancelled));
    let no_broad_stats: bool =
        sqlx::query_scalar("SELECT NOT pg_has_role(current_user,'pg_read_all_stats','MEMBER')")
            .fetch_one(&runtime)
            .await
            .unwrap();
    assert!(no_broad_stats);
    assert!(
        sqlx::query("SELECT * FROM openlegal_replication.control")
            .execute(&runtime)
            .await
            .is_err()
    );
    assert!(
        sqlx::query("DELETE FROM openlegal_replication.control")
            .execute(&runtime)
            .await
            .is_err()
    );
    sqlx::query(
        "UPDATE openlegal_replication.control SET standby_client_serial=standby_client_serial+1",
    )
    .execute(&admin)
    .await
    .unwrap();
    assert_eq!(
        fence
            .check(
                tokio::time::Instant::now() + Duration::from_secs(2),
                CancellationToken::new()
            )
            .await,
        Err(Error::StorageUnavailable)
    );
    sqlx::query(
        "UPDATE openlegal_replication.control SET standby_client_serial=standby_client_serial-1",
    )
    .execute(&admin)
    .await
    .unwrap();
    let before_restart: serde_json::Value =
        sqlx::query_scalar("SELECT openlegal_replication.capture_prefix()")
            .fetch_one(&runtime)
            .await
            .unwrap();
    pair.restart_primary().await;
    let mut restart_diagnostic = serde_json::json!({"stage": "await_new_postmaster"});
    let restarted = tokio::time::timeout(Duration::from_secs(15), async {
        let mut target: Option<serde_json::Value> = None;
        loop {
            let observed = if let Some(target) = &target {
                sqlx::query_scalar::<_, serde_json::Value>(
                    "WITH p AS MATERIALIZED (SELECT openlegal_replication.observe_flush() AS proof)
                     SELECT jsonb_build_object('proof',proof,'caught_up',
                       (proof->>'flush_lsn')::pg_lsn >= $1::pg_lsn) FROM p",
                )
                .bind(target["prefix_lsn"].as_str().unwrap())
                .fetch_one(&runtime)
                .await
            } else {
                sqlx::query_scalar::<_, serde_json::Value>(
                    "SELECT jsonb_build_object('proof',openlegal_replication.capture_prefix(),'caught_up',false)",
                )
                .fetch_one(&runtime)
                .await
            };
            let after = match observed {
                Ok(value) => value,
                Err(error) => {
                    let category = match &error {
                        sqlx::Error::RowNotFound => "row_not_found",
                        sqlx::Error::PoolTimedOut => "pool_timed_out",
                        sqlx::Error::PoolClosed => "pool_closed",
                        sqlx::Error::Io(_) => "io",
                        sqlx::Error::Tls(_) => "tls",
                        sqlx::Error::Protocol(_) => "protocol",
                        _ => "database_or_other",
                    };
                    let sqlstate = error.as_database_error()
                        .and_then(|database| database.code()).map(|code| code.into_owned());
                    restart_diagnostic = serde_json::json!({
                        "stage": if target.is_some() { "await_fixed_prefix_flush" } else { "capture_new_postmaster_target" },
                        "error_category": category, "sqlstate": sqlstate,
                    });
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            };
            let proof = &after["proof"];
            restart_diagnostic = serde_json::json!({
                "stage": if target.is_some() { "await_fixed_prefix_flush" } else { "capture_new_postmaster_target" },
                "system_identifier": proof["primary_system_identifier"],
                "timeline": proof["timeline"], "postmaster_started_at": proof["postmaster_started_at"],
                "sender_pid": proof["sender_pid"], "sender_started_at": proof["sender_started_at"],
                "observed_prefix_lsn": proof["prefix_lsn"], "flush_lsn": proof["flush_lsn"],
                "original_target_lsn": target.as_ref().map(|value| &value["prefix_lsn"]),
            });
            if let Some(target) = &target {
                assert!([
                    "managed_epoch", "blob_binding_sha256", "primary_system_identifier", "timeline",
                    "postmaster_started_at", "standby_name", "standby_role", "standby_addr",
                    "standby_client_dn", "standby_issuer_dn", "standby_client_serial",
                    "sender_pid", "sender_started_at",
                ].iter().all(|key| proof[*key] == target[*key]),
                    "post-restart setup changed its authenticated peer/session: {restart_diagnostic}");
                if after["caught_up"] == true {
                    break proof.clone();
                }
            } else if proof["postmaster_started_at"] != before_restart["postmaster_started_at"] {
                // Pin ONCE. Later WAL may advance while this exact prefix is
                // flushed; a moving current-prefix equality is not readiness.
                target = Some(proof.clone());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("restarted primary fixed-prefix setup exceeded 15s: {restart_diagnostic}"));
    assert_ne!(
        restarted["postmaster_started_at"],
        before_restart["postmaster_started_at"]
    );
    assert_eq!(
        fence
            .check(
                tokio::time::Instant::now() + Duration::from_secs(2),
                CancellationToken::new()
            )
            .await,
        Err(Error::StorageUnavailable),
        "an existing fence never silently adopts a restarted primary"
    );
    // A new process gets a new pool. No failed constructor is retried and the
    // strict two-second constructor/fence budget is unchanged.
    let mut fresh_options = pair.options();
    fresh_options.max_connections = 1;
    let fresh_pool = super::pool(&pair.runtime_url, fresh_options, Duration::from_secs(1))
        .await
        .unwrap();
    let fresh = crate::replication::PostgresDurabilityFence::open(
        fresh_pool.clone(),
        crate::replication::ReplicationOptions::for_epoch(
            pair.epoch,
            fixture::replication::FIXTURE_BLOB_BINDING_SHA256.into(),
        ),
    )
    .await
    .unwrap();
    sqlx::query("UPDATE openlegal_replication.control SET managed_epoch=uuidv7()")
        .execute(&admin)
        .await
        .unwrap();
    assert_eq!(
        fresh
            .check(
                tokio::time::Instant::now() + Duration::from_secs(2),
                CancellationToken::new()
            )
            .await,
        Err(Error::StorageUnavailable)
    );
    runtime.close().await;
    fresh_pool.close().await;
    admin.close().await;
}

#[tokio::test]
#[ignore = "requires scripts/test-replication.sh real TLS PG18 pair"]
async fn replication_pair_cancelled_sync_commit_can_be_visible_but_not_replica_acknowledged() {
    let pair = fixture::replication::TestReplication::new().await;
    let runtime = super::pool(&pair.runtime_url, pair.options(), Duration::from_secs(1))
        .await
        .unwrap();
    let admin = super::pool(&pair.admin_url, pair.options(), Duration::from_secs(1))
        .await
        .unwrap();
    sqlx::query("CREATE TABLE openlegal.replication_fixture(id integer PRIMARY KEY)")
        .execute(&admin)
        .await
        .unwrap();
    let fence = crate::replication::PostgresDurabilityFence::open(
        runtime.clone(),
        crate::replication::ReplicationOptions::for_epoch(
            pair.epoch,
            fixture::replication::FIXTURE_BLOB_BINDING_SHA256.into(),
        ),
    )
    .await
    .unwrap();
    pair.pause_standby().await;
    let mut connection = admin.acquire().await.unwrap().detach();
    let backend: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut connection)
        .await
        .unwrap();
    sqlx::query("SET statement_timeout='100ms'")
        .execute(&mut connection)
        .await
        .unwrap();
    sqlx::query("BEGIN").execute(&mut connection).await.unwrap();
    sqlx::query("INSERT INTO openlegal.replication_fixture VALUES(1)")
        .execute(&mut connection)
        .await
        .unwrap();
    // This is a real server cancellation while waiting for the paused physical
    // standby, not a fabricated successful command tag or mocked observer.
    let committing = tokio::spawn(async move {
        let result = sqlx::query("COMMIT").execute(&mut connection).await;
        (result, connection)
    });
    tokio::time::timeout(Duration::from_secs(2),async {
        loop {
            let waiting: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1 AND wait_event='SyncRep')")
                .bind(backend).fetch_one(&admin).await.unwrap();
            if waiting { break; }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await.expect("owned COMMIT must reach real SyncRep wait");
    let cancelled: bool = sqlx::query_scalar("SELECT pg_cancel_backend($1)")
        .bind(backend)
        .fetch_one(&admin)
        .await
        .unwrap();
    assert!(cancelled);
    let (commit, connection) = tokio::time::timeout(Duration::from_secs(3), committing)
        .await
        .expect("cancelled COMMIT response deadline")
        .unwrap();
    assert!(
        commit.is_ok(),
        "PG18 sync-wait cancellation must exercise the successful local COMMIT-tag boundary"
    );
    let visible: i64 = sqlx::query_scalar("SELECT count(*) FROM openlegal.replication_fixture")
        .fetch_one(&admin)
        .await
        .unwrap();
    assert_eq!(visible, 1);
    assert_eq!(
        fence
            .check(
                tokio::time::Instant::now() + Duration::from_millis(200),
                CancellationToken::new()
            )
            .await,
        Err(Error::StorageUnavailable)
    );
    pair.resume_standby().await;
    sqlx::query("INSERT INTO openlegal.replication_fixture VALUES(2)")
        .execute(&admin)
        .await
        .unwrap();
    fence
        .check(
            tokio::time::Instant::now() + Duration::from_secs(2),
            CancellationToken::new(),
        )
        .await
        .unwrap();
    connection.close().await.unwrap();
    let closed: bool =
        sqlx::query_scalar("SELECT NOT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1)")
            .bind(backend)
            .fetch_one(&admin)
            .await
            .unwrap();
    assert!(
        closed,
        "client close must be independently read back; never infer backend exit from a dropped future"
    );
    runtime.close().await;
    admin.close().await;
}

#[tokio::test]
#[ignore = "requires scripts/test-replication.sh real TLS PG18 pair"]
async fn replication_pair_gc_fences_current_prefix_before_any_physical_unlink() {
    let pair = fixture::replication::TestReplication::new().await;
    let admin = super::pool(&pair.admin_url, pair.options(), Duration::from_secs(1))
        .await
        .unwrap();
    sqlx::query("CREATE TABLE openlegal.replication_fixture(id integer PRIMARY KEY)")
        .execute(&admin)
        .await
        .unwrap();
    let blobs = crate::blob::FsBlobStore::open(&pair.directory.path().join("blobs"))
        .await
        .unwrap();
    let store = PostgresStore::open_with_replication(
        &pair.runtime_url,
        pair.options(),
        blobs,
        RetentionPolicy::default(),
        100,
        StartupMode::Serve,
        Some(crate::replication::ReplicationOptions::for_epoch(
            pair.epoch,
            fixture::replication::FIXTURE_BLOB_BINDING_SHA256.into(),
        )),
    )
    .await
    .unwrap();
    store
        .publish(super::tests::request(&store, 100).await)
        .await
        .unwrap();
    let row =
        sqlx::query("SELECT sha256,generation,size_bytes,storage_key FROM openlegal.blob_object")
            .fetch_one(&admin)
            .await
            .unwrap();
    let location = super::data::location(&row).unwrap();
    let gate = super::tests::gate(&store, TestPoint::BeforeDelete, None);
    let deadline = 100 + RetentionPolicy::default().retention_days * 86_400;
    let task = tokio::spawn(store.maintain(deadline));
    super::tests::reached(&gate).await;
    pair.pause_standby().await;
    let mut connection = admin.acquire().await.unwrap().detach();
    sqlx::query("SET synchronous_commit=local")
        .execute(&mut connection)
        .await
        .unwrap();
    sqlx::query("INSERT INTO openlegal.replication_fixture VALUES(1)")
        .execute(&mut connection)
        .await
        .unwrap();
    gate.resume.add_permits(1);
    assert_eq!(task.await.unwrap().err(), Some(Error::StorageUnavailable));
    assert!(
        store
            .inner
            .blobs
            .get(location, CancellationToken::new())
            .await
            .unwrap()
            .is_some()
    );
    let queued: i64 = sqlx::query_scalar("SELECT count(*) FROM openlegal.blob_deletion")
        .fetch_one(&admin)
        .await
        .unwrap();
    assert_eq!(queued, 1);
    pair.resume_standby().await;
    connection.close().await.unwrap();
    sqlx::query("INSERT INTO openlegal.replication_fixture VALUES(2)")
        .execute(&admin)
        .await
        .unwrap();
    store.close().await.unwrap();
    admin.close().await;
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn replication_requirement_is_public_boolean_without_observer_or_control_access() {
    let fixture = fixture::TestDatabase::new().await;
    let store = fixture.open(100).await;
    let pool = store.pool();
    sqlx::query("CREATE ROLE openlegal_replication_acl_fixture NOLOGIN")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("GRANT USAGE ON SCHEMA openlegal TO openlegal_replication_acl_fixture")
        .execute(&pool)
        .await
        .unwrap();
    let mut connection = pool.acquire().await.unwrap().detach();
    sqlx::query("SET ROLE openlegal_replication_acl_fixture")
        .execute(&mut connection)
        .await
        .unwrap();
    let required: bool = sqlx::query_scalar("SELECT openlegal.replication_required()")
        .fetch_one(&mut connection)
        .await
        .unwrap();
    assert!(!required);
    for forbidden in [
        "SELECT openlegal_replication.capture_prefix()",
        "SELECT openlegal_replication.observe_flush()",
        "SELECT * FROM openlegal_replication.control",
        "DELETE FROM openlegal_replication.control",
        "UPDATE openlegal_replication.control SET timeline=2",
    ] {
        assert!(
            sqlx::query(forbidden)
                .execute(&mut connection)
                .await
                .is_err()
        );
    }
    sqlx::query("RESET ROLE")
        .execute(&mut connection)
        .await
        .unwrap();
    connection.close().await.unwrap();
    sqlx::query("DROP OWNED BY openlegal_replication_acl_fixture")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("DROP ROLE openlegal_replication_acl_fixture")
        .execute(&pool)
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn database_required_policy_refuses_local_constructor_before_startup_accounting() {
    let fixture = fixture::TestDatabase::new().await;
    let store = fixture.open(100).await;
    let pool = store.pool();
    sqlx::query("INSERT INTO openlegal_replication.control VALUES(true,uuidv7(),'1',1,'fixture','postgres','127.0.0.1','fixture-client','fixture-issuer',1,repeat('1',64))")
        .execute(&pool).await.unwrap();
    let required: bool = sqlx::query_scalar("SELECT openlegal.replication_required()")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(required);
    let blobs = crate::blob::FsBlobStore::open(&fixture.directory.path().join("second-blobs"))
        .await
        .unwrap();
    let result = PostgresStore::open(
        &fixture.url,
        fixture::options(),
        blobs,
        RetentionPolicy::default(),
        100,
        StartupMode::Serve,
    )
    .await;
    assert!(matches!(
        result,
        Err(StartupError::Storage(Error::StorageUnavailable))
    ));
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn strict_open_never_accepts_a_primary_without_the_bound_synchronous_peer() {
    let fixture = fixture::TestDatabase::new().await;
    let store = fixture.open(100).await;
    let pool = store.pool();
    let epoch: uuid::Uuid = sqlx::query_scalar("SELECT uuidv7()")
        .fetch_one(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO openlegal_replication.control VALUES(true,$1,'1',1,'fixture','postgres','127.0.0.1','fixture-client','fixture-issuer',1,repeat('1',64))")
        .bind(epoch).execute(&pool).await.unwrap();
    let result = crate::replication::PostgresDurabilityFence::open(
        pool,
        crate::replication::ReplicationOptions::for_epoch(
            epoch,
            fixture::replication::FIXTURE_BLOB_BINDING_SHA256.into(),
        ),
    )
    .await;
    assert!(matches!(result, Err(Error::StorageUnavailable)));
}
