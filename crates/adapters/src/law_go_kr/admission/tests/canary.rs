use super::*;
#[test]
fn submitted_provider_proof_drop_poison_is_not_cleared_by_a_different_success() {
    struct Marker(Arc<AtomicBool>);
    impl openlegal_application::durability::DurabilityFence for Marker {
        fn check(
            &self,
            _: tokio::time::Instant,
            _: CancellationToken,
        ) -> futures::future::BoxFuture<'static, Result<(), openlegal_domain::RetrievalError>>
        {
            Box::pin(async { Ok(()) })
        }
        fn mark_uncertain(&self) {
            self.0.store(true, Ordering::Release);
        }
    }
    let poisoned = Arc::new(AtomicBool::new(false));
    let fence: Arc<dyn openlegal_application::durability::DurabilityFence> =
        Arc::new(Marker(poisoned.clone()));
    drop(CommitProof::new(Some(fence.clone())));
    assert!(!poisoned.load(Ordering::Acquire));
    let failed = CommitProof::new(Some(fence.clone()));
    failed.submit();
    drop(failed);
    assert!(poisoned.load(Ordering::Acquire));
    let mut successful = CommitProof::new(Some(fence));
    successful.submit();
    successful.confirm();
    drop(successful);
    assert!(poisoned.load(Ordering::Acquire));
}

async fn authority(pool: &PgPool) -> (Uuid, Uuid) {
    let (operation, id): (Uuid, Uuid) = sqlx::query_as("SELECT uuidv7(),uuidv7()")
        .fetch_one(pool)
        .await
        .unwrap();
    sqlx::query("UPDATE openlegal_admin.provider_control SET recovery_hold=true WHERE singleton")
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO openlegal_admin.provider_recovery_audit(operation_id,plan,actor,database_actor,actual_writers_stopped_at,applied_at,before_state,result) SELECT $1,'{}','synthetic-fixture',session_user,1,1,'{}',jsonb_build_object('recovery_hold',true,'hold_generation',generation) FROM openlegal_admin.provider_control WHERE singleton")
        .bind(operation).execute(pool).await.unwrap();
    (operation, id)
}
async fn arm(pool: &PgPool) -> (Uuid, Uuid) {
    let (operation, id) = authority(pool).await;
    sqlx::query("SELECT openlegal_admin.canary_init($1,$2)")
        .bind(operation)
        .bind(id)
        .execute(pool)
        .await
        .unwrap();
    (operation, id)
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn canary_functions_work_with_nologin_owner_and_only_named_runtime_operator_grants() {
    use sqlx::postgres::{PgConnectOptions, PgSslMode};
    use std::str::FromStr;
    let fixture = crate::test_support::TestDatabase::new().await;
    let store = fixture.open(100).await;
    let pool = store.pool();
    let (operation, id) = authority(&pool).await;
    sqlx::raw_sql("CREATE ROLE openlegal_canary_fixture_owner NOLOGIN; CREATE ROLE openlegal_canary_fixture_runtime NOLOGIN; CREATE ROLE openlegal_canary_fixture_operator NOLOGIN;
      GRANT USAGE ON SCHEMA public,openlegal,openlegal_admin TO openlegal_canary_fixture_owner;
      GRANT SELECT ON openlegal.provider_request_budget,openlegal.provider_request_admission TO openlegal_canary_fixture_owner;
      GRANT UPDATE(admission_owner) ON openlegal.provider_request_budget TO openlegal_canary_fixture_owner;
      GRANT SELECT ON ALL TABLES IN SCHEMA openlegal_admin TO openlegal_canary_fixture_owner;
      GRANT UPDATE ON openlegal_admin.provider_control,openlegal_admin.provider_canary,openlegal_admin.provider_canary_control TO openlegal_canary_fixture_owner;
      GRANT INSERT ON openlegal_admin.provider_canary,openlegal_admin.provider_canary_audit TO openlegal_canary_fixture_owner;
      GRANT CREATE ON SCHEMA openlegal_admin TO openlegal_canary_fixture_owner;
      ALTER FUNCTION openlegal_admin.canary_init(uuid,uuid) OWNER TO openlegal_canary_fixture_owner;
      ALTER FUNCTION openlegal_admin.canary_consume(uuid) OWNER TO openlegal_canary_fixture_owner;
      ALTER FUNCTION openlegal_admin.canary_close(uuid,uuid) OWNER TO openlegal_canary_fixture_owner;
      REVOKE CREATE ON SCHEMA openlegal_admin FROM openlegal_canary_fixture_owner;
      GRANT USAGE ON SCHEMA openlegal_admin TO openlegal_canary_fixture_runtime,openlegal_canary_fixture_operator;
      GRANT EXECUTE ON FUNCTION openlegal_admin.canary_consume(uuid) TO openlegal_canary_fixture_runtime;
      GRANT EXECUTE ON FUNCTION openlegal_admin.canary_init(uuid,uuid),openlegal_admin.canary_close(uuid,uuid) TO openlegal_canary_fixture_operator;")
      .execute(&pool).await.unwrap();
    let password = Uuid::new_v4().simple().to_string();
    // The database formats one fixed test-only DDL with a bound SQL literal;
    // no caller-controlled identifiers or executable fragments are accepted.
    let create_login: String = sqlx::query_scalar(
        "SELECT format('CREATE ROLE openlegal_canary_fixture_login LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD %L', $1)",
    )
    .bind(&password)
    .fetch_one(&pool)
    .await
    .unwrap();
    sqlx::raw_sql(sqlx::AssertSqlSafe(create_login))
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("GRANT openlegal_canary_fixture_runtime,openlegal_canary_fixture_operator TO openlegal_canary_fixture_login")
        .execute(&pool).await.unwrap();
    // session_user must be the real restricted LOGIN. SET ROLE from a
    // superuser session cannot test denial of owner-role escalation.
    let options = PgConnectOptions::from_str(&fixture.url)
        .unwrap()
        .username("openlegal_canary_fixture_login")
        .password(&password)
        .ssl_mode(PgSslMode::Disable);
    let mut connection = PgConnection::connect_with(&options).await.unwrap();
    let (session, superuser): (String, bool) = sqlx::query_as(
        "SELECT session_user::text,rolsuper FROM pg_roles WHERE rolname=session_user",
    )
    .fetch_one(&mut connection)
    .await
    .unwrap();
    assert_eq!(session, "openlegal_canary_fixture_login");
    assert!(!superuser);
    sqlx::raw_sql("SET statement_timeout='2s'; SET lock_timeout='1s'; SET transaction_timeout='5s'; SET idle_in_transaction_session_timeout='5s'")
        .execute(&mut connection).await.unwrap();
    sqlx::query("SET ROLE openlegal_canary_fixture_operator")
        .execute(&mut connection)
        .await
        .unwrap();
    let initialized: serde_json::Value =
        sqlx::query_scalar("SELECT openlegal_admin.canary_init($1,$2)")
            .bind(operation)
            .bind(id)
            .fetch_one(&mut connection)
            .await
            .unwrap();
    assert_eq!(initialized["consumed"], 0);
    sqlx::query("RESET ROLE")
        .execute(&mut connection)
        .await
        .unwrap();
    sqlx::query("SET ROLE openlegal_canary_fixture_runtime")
        .execute(&mut connection)
        .await
        .unwrap();
    sqlx::query("SELECT openlegal_admin.canary_consume($1)")
        .bind(id)
        .execute(&mut connection)
        .await
        .unwrap();
    for statement in [
        "UPDATE openlegal_admin.provider_canary SET consumed=0",
        "DELETE FROM openlegal_admin.provider_canary_control",
        "SELECT openlegal_admin.canary_init(uuidv7(),uuidv7())",
        "SET ROLE openlegal_canary_fixture_owner",
    ] {
        assert!(
            sqlx::query(statement)
                .execute(&mut connection)
                .await
                .is_err(),
            "{statement}"
        );
    }
    sqlx::query("RESET ROLE")
        .execute(&mut connection)
        .await
        .unwrap();
    sqlx::query("SET ROLE openlegal_canary_fixture_operator")
        .execute(&mut connection)
        .await
        .unwrap();
    let closed: serde_json::Value =
        sqlx::query_scalar("SELECT openlegal_admin.canary_close($1,$2)")
            .bind(operation)
            .bind(id)
            .fetch_one(&mut connection)
            .await
            .unwrap();
    assert_eq!(closed["consumed"], 1);
    assert_eq!(closed["active"], false);
    sqlx::query("RESET ROLE")
        .execute(&mut connection)
        .await
        .unwrap();
    connection.close().await.unwrap();
    // The fixture database and its containing Docker cluster are disposable;
    // role cleanup does not touch any application/production role.
    sqlx::raw_sql("DROP OWNED BY openlegal_canary_fixture_login,openlegal_canary_fixture_runtime,openlegal_canary_fixture_operator,openlegal_canary_fixture_owner; DROP ROLE openlegal_canary_fixture_login,openlegal_canary_fixture_runtime,openlegal_canary_fixture_operator,openlegal_canary_fixture_owner")
        .execute(&pool).await.unwrap();
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn canary_is_global_across_modes_and_never_reset_by_day_restart_or_reinit() {
    let fixture = crate::test_support::TestDatabase::new().await;
    let store = fixture.open(100).await;
    let pool = store.pool();
    unrestricted(&pool).await;
    let (operation, id) = arm(&pool).await;
    // Fixture starts near the fixed cap; the two real admission transactions
    // spend its remaining allowance through different origins/request modes.
    sqlx::query("UPDATE openlegal_admin.provider_canary SET consumed=98 WHERE id=$1")
        .bind(id)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE openlegal_admin.provider_control SET recovery_hold=false WHERE singleton")
        .execute(&pool)
        .await
        .unwrap();
    for mode in [RequestBudgetMode::Continuous, RequestBudgetMode::OnDemand] {
        let events = tokio::sync::OnceCell::new();
        let mut guard = reserve_with_durability(
            &pool,
            mode,
            None,
            None,
            &events,
            None,
            None,
            None,
            Some(id),
            &CancellationToken::new(),
        )
        .await
        .unwrap();
        guard.check_dispatch().unwrap();
        guard.complete().await.unwrap();
    }
    // A new connection/day has no authority to reset this operation.
    sqlx::query("UPDATE openlegal.provider_request_budget SET utc_day=0,daily_used=0,on_demand_used=0 WHERE singleton")
        .execute(&pool).await.unwrap();
    let events = tokio::sync::OnceCell::new();
    let denied = reserve_with_durability(
        &pool,
        RequestBudgetMode::Pilot,
        None,
        None,
        &events,
        None,
        None,
        None,
        Some(id),
        &CancellationToken::new(),
    )
    .await;
    assert!(denied.is_err());
    let consumed: i32 =
        sqlx::query_scalar("SELECT consumed FROM openlegal_admin.provider_canary WHERE id=$1")
            .bind(id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(consumed, 100);
    sqlx::query("UPDATE openlegal_admin.provider_control SET recovery_hold=true WHERE singleton")
        .execute(&pool)
        .await
        .unwrap();
    let same: serde_json::Value = sqlx::query_scalar("SELECT openlegal_admin.canary_init($1,$2)")
        .bind(operation)
        .bind(id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(same["consumed"], 100);
    let closed: serde_json::Value =
        sqlx::query_scalar("SELECT openlegal_admin.canary_close($1,$2)")
            .bind(operation)
            .bind(id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(closed["active"], false);
    assert!(
        sqlx::query("SELECT openlegal_admin.canary_consume($1)")
            .bind(id)
            .execute(&pool)
            .await
            .is_err()
    );
    assert!(
        sqlx::query("SELECT openlegal_admin.canary_consume(NULL)")
            .execute(&pool)
            .await
            .is_err()
    );
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn canary_requires_identity_and_reservation_rollback_does_not_spend() {
    let fixture = crate::test_support::TestDatabase::new().await;
    let store = fixture.open(100).await;
    let pool = store.pool();
    let (_, id) = arm(&pool).await;
    assert!(
        sqlx::query("SELECT openlegal_admin.canary_consume(NULL)")
            .execute(&pool)
            .await
            .is_err()
    );
    assert!(
        sqlx::query("SELECT openlegal_admin.canary_consume(uuidv7())")
            .execute(&pool)
            .await
            .is_err()
    );
    let mut tx = pool.begin().await.unwrap();
    sqlx::query("SELECT openlegal_admin.canary_consume($1)")
        .bind(id)
        .execute(&mut *tx)
        .await
        .unwrap();
    tx.rollback().await.unwrap();
    let used: i32 =
        sqlx::query_scalar("SELECT consumed FROM openlegal_admin.provider_canary WHERE id=$1")
            .bind(id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(used, 0);
    sqlx::query("UPDATE openlegal_admin.provider_canary SET last_observed_at=clock_timestamp()+interval '1 hour' WHERE id=$1")
        .bind(id).execute(&pool).await.unwrap();
    assert!(
        sqlx::query("SELECT openlegal_admin.canary_consume($1)")
            .bind(id)
            .execute(&pool)
            .await
            .is_err()
    );
}

struct RefusingFence;
impl openlegal_application::durability::DurabilityFence for RefusingFence {
    fn check(
        &self,
        _: tokio::time::Instant,
        _: CancellationToken,
    ) -> futures::future::BoxFuture<'static, Result<(), openlegal_domain::RetrievalError>> {
        Box::pin(async { Err(openlegal_domain::RetrievalError::StorageUnavailable) })
    }
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn postcommit_fence_failure_keeps_charged_canary_and_uncertain_owner() {
    let fixture = crate::test_support::TestDatabase::new().await;
    let store = fixture.open(100).await;
    let pool = store.pool();
    unrestricted(&pool).await;
    let (_, id) = arm(&pool).await;
    sqlx::query("UPDATE openlegal_admin.provider_control SET recovery_hold=false WHERE singleton")
        .execute(&pool)
        .await
        .unwrap();
    let events = tokio::sync::OnceCell::new();
    let fence: Arc<dyn openlegal_application::durability::DurabilityFence> =
        Arc::new(RefusingFence);
    let result = reserve_with_durability(
        &pool,
        RequestBudgetMode::Continuous,
        None,
        None,
        &events,
        None,
        None,
        Some(&fence),
        Some(id),
        &CancellationToken::new(),
    )
    .await;
    assert!(matches!(result, Err(DatabaseError::StorageUnavailable)));
    let (used, owners): (i32, i64) = sqlx::query_as("SELECT consumed,(SELECT count(*) FROM openlegal.provider_request_admission) FROM openlegal_admin.provider_canary WHERE id=$1")
        .bind(id).fetch_one(&pool).await.unwrap();
    assert_eq!((used, owners), (1, 1));
}
