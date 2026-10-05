use super::*;
use openlegal_application::persistence::PersistentStore;
use serde_json::json;

fn recovery_request(snapshot: &ProviderAdminSnapshot, resume: bool) -> ProviderRecoveryRequest {
    ProviderRecoveryRequest {
        actor: "fixture-operator".into(),
        reason: "reviewed lost response fixture".into(),
        quiescence: ProviderQuiescenceEvidence {
            writers_stopped_at: snapshot.observed_at,
            deployment_revision: "fixture-release".into(),
            stopped_writer_ids: vec!["fixture-collector".into()],
            evidence_reference: "fixture-offline-proof".into(),
        },
        resolve_legacy: snapshot.state["budget"]["unresolved_response"] == true,
        owners: snapshot.state["ledger"]
            .as_array()
            .unwrap()
            .iter()
            .map(|a| a["owner"].as_str().unwrap().to_owned())
            .collect(),
        resume,
        waits: vec![],
    }
}
async fn seed_legacy(pool: &PgPool) {
    sqlx::query("UPDATE openlegal.provider_request_budget SET unresolved_response=true,admission_owner=pg_catalog.uuidv7(),daily_used=9,on_demand_used=7,next_allowed_at=0")
        .execute(pool).await.unwrap();
    sqlx::query("UPDATE openlegal_admin.provider_control SET legacy_identity=pg_catalog.uuidv7() WHERE singleton")
        .execute(pool).await.unwrap();
    sqlx::query("INSERT INTO openlegal_admin.uncertainty_observation(identity,kind,owner) SELECT c.legacy_identity,'legacy',b.admission_owner FROM openlegal_admin.provider_control c CROSS JOIN openlegal.provider_request_budget b")
        .execute(pool).await.unwrap();
}
#[tokio::test]
async fn plan_rejects_unreviewed_or_expanded_resume_and_bounds_snapshot_bytes() {
    let snapshot:ProviderAdminSnapshot=serde_json::from_value(json!({"version":1,"observed_at":100,
        "state":{"budget":{"unresolved_response":true},"ledger":[]},"waiting":[],"waiting_truncated":false})).unwrap();
    let store = ProviderAdminStore {
        pool: sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://fixture@localhost/fixture")
            .unwrap(),
    };
    let mut request = recovery_request(&snapshot, false);
    request.waits.push(ReviewedProviderWait {
        kind: ProviderWaitKind::DetailJob,
        id: "00000000-0000-4000-8000-000000000001".into(),
        review_reason: "review".into(),
    });
    assert!(matches!(
        store.plan(snapshot.clone(), request),
        Err(ProviderAdminError::InvalidInput)
    ));
    let mut request = recovery_request(&snapshot, true);
    request.quiescence.writers_stopped_at = 101;
    assert!(matches!(
        store.plan(snapshot.clone(), request),
        Err(ProviderAdminError::InvalidInput)
    ));
    let mut snapshot = snapshot;
    snapshot.state["oversized"] = Value::String("x".repeat(262144));
    assert!(matches!(
        store.plan(snapshot.clone(), recovery_request(&snapshot, false)),
        Err(ProviderAdminError::InvalidInput)
    ));
}
#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn cleanup_is_atomic_held_and_idempotent_then_new_plan_releases_only_hold() {
    let fixture = crate::test_support::TestDatabase::new().await;
    let base = fixture.open(100).await;
    let pool = base.pool();
    seed_legacy(&pool).await;
    let store = ProviderAdminStore { pool: pool.clone() };
    let snapshot = store.inspect().await.unwrap();
    let before = snapshot.state["budget"].clone();
    let plan = store
        .plan(snapshot.clone(), recovery_request(&snapshot, false))
        .unwrap();
    let result = store.apply(&plan).await.unwrap();
    assert!(result.recovery_hold);
    assert!(held(&pool).await.unwrap());
    let post = store.inspect().await.unwrap();
    assert_eq!(post.state["budget"]["daily_used"], before["daily_used"]);
    assert_eq!(
        post.state["budget"]["on_demand_used"],
        before["on_demand_used"]
    );
    assert_eq!(
        post.state["budget"]["next_allowed_at"],
        before["next_allowed_at"]
    );
    assert_eq!(
        post.state["budget"]["operator_suspended"],
        before["operator_suspended"]
    );
    assert_eq!(post.state["budget"]["unresolved_response"], false);
    assert!(post.state["budget"]["admission_owner"].is_null());
    let again = store.apply(&plan).await.unwrap();
    assert_eq!(again.operation_id, result.operation_id);
    assert!(store.readback(&plan.operation_id).await.unwrap().is_some());
    let audit: i64 =
        sqlx::query_scalar("SELECT count(*) FROM openlegal_admin.provider_recovery_audit")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(audit, 1);
    let resume = store
        .plan(post.clone(), recovery_request(&post, true))
        .unwrap();
    let resumed = store.apply(&resume).await.unwrap();
    assert!(!resumed.recovery_hold);
    assert!(!held(&pool).await.unwrap());
    base.close().await.unwrap();
}
#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn selected_unknown_wait_lease_advances_without_resetting_other_waits_or_pause() {
    let fixture = crate::test_support::TestDatabase::new().await;
    let base = fixture.open(100).await;
    let pool = base.pool();
    seed_legacy(&pool).await;
    sqlx::query("UPDATE openlegal.provider_request_budget SET next_allowed_at=floor(extract(epoch FROM clock_timestamp()))::bigint+120")
        .execute(&pool).await.unwrap();
    let ids:Vec<Uuid>=sqlx::query_scalar("INSERT INTO openlegal.collection_request(request_key,canonical_key,payload,status,created_at,expires_at,lease_until) SELECT repeat(n::text,64),repeat(n::text,64),'{}','deferred',floor(extract(epoch FROM clock_timestamp()))::bigint,floor(extract(epoch FROM clock_timestamp()))::bigint+86400,floor(extract(epoch FROM clock_timestamp()))::bigint+3600 FROM generate_series(1,2) n RETURNING id")
        .fetch_all(&pool).await.unwrap();
    let store = ProviderAdminStore { pool: pool.clone() };
    let snapshot = store.inspect().await.unwrap();
    let mut request = recovery_request(&snapshot, true);
    request.waits.push(ReviewedProviderWait {
        kind: ProviderWaitKind::CollectionRequest,
        id: ids[0].to_string(),
        review_reason: "explicit review of legacy NULL cause".into(),
    });
    let before_second: i64 =
        sqlx::query_scalar("SELECT lease_until FROM openlegal.collection_request WHERE id=$1")
            .bind(ids[1])
            .fetch_one(&pool)
            .await
            .unwrap();
    let plan = store.plan(snapshot, request).unwrap();
    let result = store.apply(&plan).await.unwrap();
    assert_eq!(result.advanced_waits.len(), 1);
    let first: (i64, String, Option<String>) = sqlx::query_as(
        "SELECT lease_until,status,reason FROM openlegal.collection_request WHERE id=$1",
    )
    .bind(ids[0])
    .fetch_one(&pool)
    .await
    .unwrap();
    let pause: i64 =
        sqlx::query_scalar("SELECT next_allowed_at FROM openlegal.provider_request_budget")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(first.0, pause);
    assert_eq!(first.1, "deferred");
    assert_eq!(first.2, None);
    let after_second: i64 =
        sqlx::query_scalar("SELECT lease_until FROM openlegal.collection_request WHERE id=$1")
            .bind(ids[1])
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(after_second, before_second);
    base.close().await.unwrap();
}
#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn direct_sql_rejects_missing_null_wrong_types_and_cross_storage_snapshot() {
    let fixture = crate::test_support::TestDatabase::new().await;
    let base = fixture.open(100).await;
    let pool = base.pool();
    seed_legacy(&pool).await;
    let store = ProviderAdminStore { pool: pool.clone() };
    let snapshot = store.inspect().await.unwrap();
    let plan = store
        .plan(snapshot.clone(), recovery_request(&snapshot, false))
        .unwrap();
    let valid = serde_json::to_value(&plan).unwrap();
    for key in ["created_at", "expires_at", "version", "operation_id"] {
        for replacement in [None, Some(Value::Null), Some(json!({"wrong":"type"}))] {
            let mut malformed = valid.clone();
            if let Some(v) = replacement {
                malformed[key] = v;
            } else {
                malformed.as_object_mut().unwrap().remove(key);
            }
            assert!(
                sqlx::query("SELECT openlegal_admin.provider_apply($1)")
                    .bind(malformed)
                    .execute(&pool)
                    .await
                    .is_err(),
                "{key}"
            );
        }
    }
    for path in ["actor", "reason", "quiescence"] {
        let mut malformed = valid.clone();
        malformed["request"].as_object_mut().unwrap().remove(path);
        assert!(
            sqlx::query("SELECT openlegal_admin.provider_apply($1)")
                .bind(malformed)
                .execute(&pool)
                .await
                .is_err()
        );
    }
    let mut crossed = valid.clone();
    crossed["snapshot"]["state"]["storage_id"] = json!("00000000-0000-4000-8000-000000000000");
    assert!(
        sqlx::query("SELECT openlegal_admin.provider_apply($1)")
            .bind(crossed)
            .execute(&pool)
            .await
            .is_err()
    );
    let audits: i64 =
        sqlx::query_scalar("SELECT count(*) FROM openlegal_admin.provider_recovery_audit")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(audits, 0);
    assert!(
        store.inspect().await.unwrap().state["budget"]["unresolved_response"]
            .as_bool()
            .unwrap()
    );
    base.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn first_observation_is_committed_by_refusal_and_public_reads_never_write() {
    use crate::law_go_kr::{LawClient, RequestBudgetMode};
    use tokio_util::sync::CancellationToken;
    let fixture = crate::test_support::TestDatabase::new().await;
    let base = fixture.open(100).await;
    let pool = base.pool();
    seed_legacy(&pool).await;
    let initial = runtime_snapshot(&pool).await.unwrap();
    assert_eq!(initial.first_observed_at, None);
    let mut tx = pool.begin().await.unwrap();
    sqlx::query("SELECT openlegal_admin.observe_uncertainty()")
        .execute(&mut *tx)
        .await
        .unwrap();
    tx.rollback().await.unwrap();
    assert_eq!(
        runtime_snapshot(&pool).await.unwrap().first_observed_at,
        None
    );
    assert_eq!(
        LawClient::reserve_provider_request_budget(
            &pool,
            &RequestBudgetMode::Continuous,
            &CancellationToken::new()
        )
        .await
        .unwrap_err(),
        DatabaseError::BudgetExhausted
    );
    let first = runtime_snapshot(&pool).await.unwrap();
    assert!(first.first_observed_at.is_some());
    assert_eq!(first.blocked_since, None);
    let counts: i64 =
        sqlx::query_scalar("SELECT count(*) FROM openlegal_admin.uncertainty_observation")
            .fetch_one(&pool)
            .await
            .unwrap();
    let again = runtime_snapshot(&pool).await.unwrap();
    assert_eq!(again.first_observed_at, first.first_observed_at);
    let after: i64 =
        sqlx::query_scalar("SELECT count(*) FROM openlegal_admin.uncertainty_observation")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(counts, after);
    base.close().await.unwrap();
}
#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn live_slot_and_changed_policy_reject_apply_without_audit_or_marker_cleanup() {
    use crate::law_go_kr::{LawClient, RequestBudgetMode};
    use tokio_util::sync::CancellationToken;
    let fixture = crate::test_support::TestDatabase::new().await;
    let base = fixture.open(100).await;
    let pool = base.pool();
    let mut guard = LawClient::reserve_provider_request_budget(
        &pool,
        &RequestBudgetMode::Continuous,
        &CancellationToken::new(),
    )
    .await
    .unwrap();
    let store = ProviderAdminStore { pool: pool.clone() };
    let snapshot = store.inspect().await.unwrap();
    let plan = store
        .plan(snapshot.clone(), recovery_request(&snapshot, false))
        .unwrap();
    assert!(matches!(
        store.apply(&plan).await,
        Err(ProviderAdminError::ActiveOwners)
    ));
    guard.complete().await.unwrap();
    seed_legacy(&pool).await;
    let snapshot = store.inspect().await.unwrap();
    let plan = store
        .plan(snapshot.clone(), recovery_request(&snapshot, false))
        .unwrap();
    sqlx::query("UPDATE openlegal.provider_request_budget SET daily_used=daily_used+1")
        .execute(&pool)
        .await
        .unwrap();
    assert!(matches!(
        store.apply(&plan).await,
        Err(ProviderAdminError::StalePlan)
    ));
    assert!(
        store.inspect().await.unwrap().state["budget"]["unresolved_response"]
            .as_bool()
            .unwrap()
    );
    let count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM openlegal_admin.provider_recovery_audit")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(count, 0);
    base.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn selected_inspection_reaches_waits_beyond_first_page_and_cleanup_plan_stays_small() {
    let fixture = crate::test_support::TestDatabase::new().await;
    let base = fixture.open(100).await;
    let pool = base.pool();
    seed_legacy(&pool).await;
    sqlx::query("INSERT INTO openlegal.collection_request(request_key,canonical_key,payload,status,created_at,expires_at,lease_until) SELECT lpad(to_hex(n),64,'0'),lpad(to_hex(n),64,'0'),'{}','deferred',100,200,150 FROM generate_series(1,300) n")
        .execute(&pool).await.unwrap();
    let store = ProviderAdminStore { pool: pool.clone() };
    let first = store.inspect().await.unwrap();
    assert!(first.waiting_truncated);
    assert_eq!(first.waiting.len(), 256);
    let chosen: Uuid =
        sqlx::query_scalar("SELECT id FROM openlegal.collection_request ORDER BY id DESC LIMIT 1")
            .fetch_one(&pool)
            .await
            .unwrap();
    let selected = ReviewedProviderWait {
        kind: ProviderWaitKind::CollectionRequest,
        id: chosen.to_string(),
        review_reason: "individually reviewed beyond inventory page".into(),
    };
    let snapshot = store.inspect_selected(&[selected]).await.unwrap();
    assert_eq!(snapshot.waiting.len(), 1);
    assert_eq!(snapshot.waiting[0].id, chosen.to_string());
    let empty = store.inspect_selected(&[]).await.unwrap();
    assert!(empty.waiting.is_empty());
    assert!(
        store
            .plan(empty.clone(), recovery_request(&empty, false))
            .is_ok()
    );
    base.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn dedicated_function_owner_has_minimal_grants_and_runtime_cannot_apply_or_modify_audit() {
    let fixture = crate::test_support::TestDatabase::new().await;
    let base = fixture.open(100).await;
    let pool = base.pool();
    seed_legacy(&pool).await;
    let suffix: String = sqlx::query_scalar("SELECT replace(pg_catalog.uuidv7()::text,'-','')")
        .fetch_one(&pool)
        .await
        .unwrap();
    let owner = format!("fixture_provider_owner_{suffix}");
    let runtime = format!("fixture_provider_runtime_{suffix}");
    let operator = format!("fixture_provider_operator_{suffix}");
    // Names are generated locally from a UUID; no external string becomes SQL.
    let setup=format!("CREATE ROLE {owner} NOLOGIN; CREATE ROLE {runtime} NOLOGIN; CREATE ROLE {operator} NOLOGIN;
        GRANT USAGE ON SCHEMA openlegal,openlegal_admin TO {owner};
        GRANT SELECT ON public._sqlx_migrations TO {owner};
        GRANT SELECT ON openlegal.cache_storage,openlegal.corpus_control,openlegal.provider_request_budget,
            openlegal.provider_request_admission,openlegal.collection_request,openlegal.corpus_job,openlegal.corpus_object TO {owner};
        GRANT UPDATE(singleton) ON openlegal.corpus_control TO {owner};
        GRANT UPDATE(unresolved_response,admission_owner) ON openlegal.provider_request_budget TO {owner};
        GRANT DELETE ON openlegal.provider_request_admission TO {owner};
        GRANT UPDATE(lease_until) ON openlegal.collection_request,openlegal.corpus_job TO {owner};
        GRANT UPDATE(version) ON openlegal.corpus_object TO {owner};
        GRANT SELECT ON ALL TABLES IN SCHEMA openlegal_admin TO {owner};
        GRANT UPDATE ON openlegal_admin.provider_control TO {owner};
        GRANT INSERT,UPDATE ON openlegal_admin.uncertainty_observation TO {owner};
        GRANT INSERT ON openlegal_admin.provider_recovery_audit TO {owner};
        GRANT CREATE ON SCHEMA openlegal_admin TO {owner};
        DO $$ DECLARE f record; BEGIN FOR f IN SELECT oid::regprocedure::text signature FROM pg_proc WHERE pronamespace='openlegal_admin'::regnamespace LOOP EXECUTE format('ALTER FUNCTION %s OWNER TO {owner}',f.signature); END LOOP; END $$;
        REVOKE CREATE ON SCHEMA openlegal_admin FROM {owner};
        GRANT USAGE ON SCHEMA openlegal_admin TO {runtime},{operator};
        GRANT EXECUTE ON FUNCTION openlegal_admin.provider_diagnostic(),openlegal_admin.provider_held(),openlegal_admin.observe_uncertainty(),openlegal_admin.provider_blocker_fingerprint() TO {runtime};
        GRANT EXECUTE ON FUNCTION openlegal_admin.provider_inspect(),openlegal_admin.provider_inspect_selected(jsonb),openlegal_admin.provider_apply(jsonb),openlegal_admin.provider_readback(uuid) TO {operator};");
    sqlx::raw_sql(sqlx::AssertSqlSafe(setup.as_str()))
        .execute(&pool)
        .await
        .unwrap();
    let mut tx = pool.begin().await.unwrap();
    sqlx::query("SELECT set_config('role',$1,true)")
        .bind(&runtime)
        .execute(&mut *tx)
        .await
        .unwrap();
    let diag: Value = sqlx::query_scalar("SELECT openlegal_admin.provider_diagnostic()")
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    assert!(diag["legacy_uncertain"].as_bool().unwrap());
    sqlx::query("SELECT openlegal_admin.observe_uncertainty()")
        .execute(&mut *tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    for forbidden in [
        "SELECT openlegal_admin.provider_apply('{}')",
        "SELECT * FROM openlegal_admin.provider_recovery_audit",
        "DELETE FROM openlegal_admin.uncertainty_observation",
    ] {
        let mut tx = pool.begin().await.unwrap();
        sqlx::query("SELECT set_config('role',$1,true)")
            .bind(&runtime)
            .execute(&mut *tx)
            .await
            .unwrap();
        let err = sqlx::query(forbidden).execute(&mut *tx).await.unwrap_err();
        assert_eq!(
            err.as_database_error().unwrap().code().as_deref(),
            Some("42501")
        );
        tx.rollback().await.unwrap();
    }
    let mut tx = pool.begin().await.unwrap();
    sqlx::query("SELECT set_config('role',$1,true)")
        .bind(&operator)
        .execute(&mut *tx)
        .await
        .unwrap();
    let value: Value = sqlx::query_scalar("SELECT openlegal_admin.provider_inspect_selected('[]')")
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    let snapshot: ProviderAdminSnapshot = serde_json::from_value(value).unwrap();
    let store = ProviderAdminStore { pool: pool.clone() };
    let plan = store
        .plan(snapshot.clone(), recovery_request(&snapshot, false))
        .unwrap();
    let result: Value = sqlx::query_scalar("SELECT openlegal_admin.provider_apply($1)")
        .bind(serde_json::to_value(plan).unwrap())
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    assert_eq!(result["recovery_hold"], true);
    tx.commit().await.unwrap();
    base.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn selected_receipt_expiring_during_row_lock_wait_rejects_whole_apply() {
    use sqlx::Connection;
    let fixture = crate::test_support::TestDatabase::new().await;
    let base = fixture.open(100).await;
    let pool = base.pool();
    seed_legacy(&pool).await;
    let id: Uuid = sqlx::query_scalar("INSERT INTO openlegal.collection_request(request_key,canonical_key,payload,status,created_at,expires_at,lease_until) VALUES(repeat('a',64),repeat('a',64),'{}','deferred',floor(extract(epoch FROM clock_timestamp()))::bigint,floor(extract(epoch FROM clock_timestamp()))::bigint+3,floor(extract(epoch FROM clock_timestamp()))::bigint+3600) RETURNING id")
        .fetch_one(&pool).await.unwrap();
    let store = ProviderAdminStore { pool: pool.clone() };
    let review = ReviewedProviderWait {
        kind: ProviderWaitKind::CollectionRequest,
        id: id.to_string(),
        review_reason: "offline receipt reviewed before row lock wait".into(),
    };
    let snapshot = store
        .inspect_selected(std::slice::from_ref(&review))
        .await
        .unwrap();
    let mut request = recovery_request(&snapshot, true);
    request.waits.push(review);
    let plan = store.plan(snapshot, request).unwrap();
    let mut blocker = pool.begin().await.unwrap();
    sqlx::query("SELECT id FROM openlegal.collection_request WHERE id=$1 FOR UPDATE")
        .bind(id)
        .fetch_one(&mut *blocker)
        .await
        .unwrap();
    // A direct operator session can choose longer waits than the CLI pool. The
    // definer function must recheck expiration after those waits itself.
    let mut connection = sqlx::PgConnection::connect_with(&pool.connect_options())
        .await
        .unwrap();
    sqlx::raw_sql("SET lock_timeout='0'; SET statement_timeout='0'; SET transaction_timeout='10s'")
        .execute(&mut connection)
        .await
        .unwrap();
    let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut connection)
        .await
        .unwrap();
    let pending = tokio::spawn(async move {
        sqlx::query("SELECT openlegal_admin.provider_apply($1)")
            .bind(serde_json::to_value(plan).unwrap())
            .execute(&mut connection)
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let waiting: bool = sqlx::query_scalar(
                "SELECT COALESCE(wait_event_type='Lock',false) FROM pg_stat_activity WHERE pid=$1",
            )
            .bind(pid)
            .fetch_one(&pool)
            .await
            .unwrap();
            if waiting {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_secs(3)).await;
    blocker.rollback().await.unwrap();
    let error = pending.await.unwrap().unwrap_err();
    assert_eq!(
        error.as_database_error().unwrap().code().as_deref(),
        Some("40001")
    );
    let unchanged: bool =
        sqlx::query_scalar("SELECT unresolved_response FROM openlegal.provider_request_budget")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(unchanged);
    let audit: i64 =
        sqlx::query_scalar("SELECT count(*) FROM openlegal_admin.provider_recovery_audit")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(audit, 0);
    base.close().await.unwrap();
}
