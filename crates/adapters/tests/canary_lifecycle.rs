//! Actual disposable TLS pair and LOGIN/ACL checks. The explicitly shifted
//! database timestamps exercise finish logic, never real six-hour acceptance.
#[path = "../../../test-support/replication.rs"]
#[allow(dead_code, reason = "shared pair also exposes other fault controls")]
mod support;

use serde_json::{Value, json};
use sqlx::{
    Connection, PgConnection,
    postgres::{PgConnectOptions, PgSslMode},
};
use std::{str::FromStr, time::Duration};
use uuid::Uuid;

async fn new_id(connection: &mut PgConnection) -> Uuid {
    sqlx::query_scalar("SELECT pg_catalog.uuidv7()")
        .fetch_one(connection)
        .await
        .unwrap()
}

async fn activate(
    connection: &mut PgConnection,
    recovery: Uuid,
    canary: Uuid,
    generation: Uuid,
) -> Result<Value, sqlx::Error> {
    sqlx::query_scalar("SELECT openlegal_checkpoint.activate_canary($1,$2,$3,$4)")
        .bind(recovery)
        .bind(canary)
        .bind("a".repeat(64))
        .bind(generation)
        .fetch_one(connection)
        .await
}

async fn finish(
    connection: &mut PgConnection,
    checkpoint: Uuid,
    canary: Uuid,
) -> Result<Value, sqlx::Error> {
    sqlx::query_scalar("SELECT openlegal_checkpoint.finish_canary($1,$2,$3)")
        .bind(checkpoint)
        .bind("b".repeat(64))
        .bind(canary)
        .fetch_one(connection)
        .await
}

async fn hold(connection: &mut PgConnection, operation: Uuid) -> Value {
    sqlx::query_scalar("SELECT openlegal_checkpoint.hold($1,$2)")
        .bind(operation)
        .bind("b".repeat(64))
        .fetch_one(connection)
        .await
        .unwrap()
}

async fn release(connection: &mut PgConnection, operation: Uuid) -> Result<Value, sqlx::Error> {
    sqlx::query_scalar("SELECT openlegal_checkpoint.release($1,$2)")
        .bind(operation)
        .bind("b".repeat(64))
        .fetch_one(connection)
        .await
}

async fn flush(connection: &mut PgConnection) {
    let before: Value = sqlx::query_scalar("SELECT openlegal_replication.capture_prefix()")
        .fetch_one(&mut *connection)
        .await
        .unwrap();
    let after: Value = sqlx::query_scalar("SELECT openlegal_replication.observe_flush()")
        .fetch_one(connection)
        .await
        .unwrap();
    for field in [
        "managed_epoch",
        "blob_binding_sha256",
        "primary_system_identifier",
        "timeline",
        "postmaster_started_at",
        "sender_pid",
        "sender_started_at",
    ] {
        assert_eq!(before[field], after[field]);
    }
    fn lsn(value: &Value) -> u64 {
        let (a, b) = value.as_str().unwrap().split_once('/').unwrap();
        (u64::from_str_radix(a, 16).unwrap() << 32) | u64::from_str_radix(b, 16).unwrap()
    }
    assert!(lsn(&after["flush_lsn"]) >= lsn(&before["prefix_lsn"]));
}

#[tokio::test]
#[ignore = "requires scripts/test-replication.sh actual isolated PG18 TLS pair"]
async fn canary_lifecycle_actual_login_atomic_activation_and_permanent_final_hold() {
    let pair = support::TestReplication::new().await;
    let options = |url: &str| {
        PgConnectOptions::from_str(url)
            .unwrap()
            .ssl_mode(PgSslMode::VerifyFull)
            .ssl_root_cert(pair.directory.path().join("ca.crt"))
    };
    let mut admin = PgConnection::connect_with(&options(&pair.admin_url))
        .await
        .unwrap();
    sqlx::raw_sql("CREATE ROLE lifecycle_legacy_owner NOLOGIN NOSUPERUSER NOINHERIT NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
      CREATE ROLE lifecycle_actor NOLOGIN NOSUPERUSER NOINHERIT NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
      GRANT lifecycle_actor TO openlegal_fixture_runtime;
      GRANT USAGE ON SCHEMA openlegal,openlegal_admin,openlegal_replication,openlegal_checkpoint TO lifecycle_legacy_owner;
      GRANT SELECT ON openlegal.corpus_control,openlegal.provider_request_budget,openlegal.provider_request_admission,
        openlegal_admin.provider_control,openlegal_admin.provider_recovery_audit,openlegal_admin.provider_canary,
        openlegal_admin.provider_canary_control,openlegal_replication.control,
        openlegal_checkpoint.operation,openlegal_checkpoint.canary_lifecycle TO lifecycle_legacy_owner;
      GRANT UPDATE(singleton) ON openlegal.corpus_control,openlegal.provider_request_budget,
        openlegal_replication.control TO lifecycle_legacy_owner;
      GRANT UPDATE(recovery_hold,generation,hold_started_at) ON openlegal_admin.provider_control TO lifecycle_legacy_owner;
      GRANT UPDATE(ended_at) ON openlegal_admin.provider_canary TO lifecycle_legacy_owner;
      GRANT UPDATE(required_id) ON openlegal_admin.provider_canary_control TO lifecycle_legacy_owner;
      GRANT INSERT ON openlegal_admin.provider_canary,openlegal_admin.provider_canary_audit TO lifecycle_legacy_owner;
      GRANT INSERT,UPDATE ON openlegal_checkpoint.operation,openlegal_checkpoint.canary_lifecycle TO lifecycle_legacy_owner;
      GRANT EXECUTE ON FUNCTION openlegal_replication.capture_prefix() TO lifecycle_legacy_owner;
      GRANT CREATE ON SCHEMA openlegal_admin,openlegal_checkpoint TO lifecycle_legacy_owner;
      ALTER FUNCTION openlegal_admin.canary_init(uuid,uuid) OWNER TO lifecycle_legacy_owner;
      ALTER FUNCTION openlegal_checkpoint.state() OWNER TO lifecycle_legacy_owner;
      ALTER FUNCTION openlegal_checkpoint.hold(uuid,text) OWNER TO lifecycle_legacy_owner;
      ALTER FUNCTION openlegal_checkpoint.release(uuid,text) OWNER TO lifecycle_legacy_owner;
      REVOKE CREATE ON SCHEMA openlegal_admin,openlegal_checkpoint FROM lifecycle_legacy_owner;
      CREATE ROLE lifecycle_owner NOLOGIN NOSUPERUSER NOINHERIT NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
      GRANT USAGE ON SCHEMA openlegal,openlegal_admin,openlegal_replication,openlegal_checkpoint TO lifecycle_owner;
      GRANT SELECT ON openlegal.corpus_control,openlegal.provider_request_budget,openlegal.provider_request_admission,
        openlegal_admin.provider_control,openlegal_admin.provider_recovery_audit,openlegal_admin.provider_canary,
        openlegal_admin.provider_canary_control,openlegal_replication.control,
        openlegal_checkpoint.operation,openlegal_checkpoint.canary_lifecycle TO lifecycle_owner;
      GRANT UPDATE(singleton) ON openlegal.corpus_control,openlegal.provider_request_budget,
        openlegal_replication.control TO lifecycle_owner;
      GRANT UPDATE(recovery_hold,generation,hold_started_at) ON openlegal_admin.provider_control TO lifecycle_owner;
      GRANT UPDATE(ended_at) ON openlegal_admin.provider_canary TO lifecycle_owner;
      GRANT UPDATE(required_id) ON openlegal_admin.provider_canary_control TO lifecycle_owner;
      GRANT UPDATE(operation_id) ON openlegal_checkpoint.operation TO lifecycle_owner;
      GRANT INSERT ON openlegal_admin.provider_canary_audit TO lifecycle_owner;
      GRANT INSERT,UPDATE ON openlegal_checkpoint.canary_lifecycle TO lifecycle_owner;
      GRANT EXECUTE ON FUNCTION openlegal_admin.canary_init(uuid,uuid),
        openlegal_replication.capture_prefix(),openlegal_checkpoint.state() TO lifecycle_owner;
      GRANT CREATE ON SCHEMA openlegal_checkpoint TO lifecycle_owner;
      ALTER FUNCTION openlegal_checkpoint.activate_canary(uuid,uuid,text,uuid) OWNER TO lifecycle_owner;
      ALTER FUNCTION openlegal_checkpoint.finish_canary(uuid,text,uuid) OWNER TO lifecycle_owner;
      ALTER FUNCTION openlegal_checkpoint.canary_readback(uuid) OWNER TO lifecycle_owner;
      REVOKE CREATE ON SCHEMA openlegal_checkpoint FROM lifecycle_owner;
      GRANT USAGE ON SCHEMA openlegal_checkpoint,openlegal_replication TO lifecycle_actor;
      GRANT EXECUTE ON FUNCTION openlegal_checkpoint.activate_canary(uuid,uuid,text,uuid),
        openlegal_checkpoint.finish_canary(uuid,text,uuid),openlegal_checkpoint.canary_readback(uuid),
        openlegal_checkpoint.hold(uuid,text),openlegal_checkpoint.release(uuid,text),
        openlegal_replication.capture_prefix(),openlegal_replication.observe_flush() TO lifecycle_actor;")
        .execute(&mut admin).await.unwrap();
    let recovery = new_id(&mut admin).await;
    let canary = new_id(&mut admin).await;
    let generation: Uuid = sqlx::query_scalar(
        "UPDATE openlegal_admin.provider_control SET recovery_hold=true RETURNING generation",
    )
    .fetch_one(&mut admin)
    .await
    .unwrap();
    // Synthetic audited hold only in this disposable SQL fixture. Production
    // must use the reviewed provider_apply plan and real stopped-writer proof.
    sqlx::query("INSERT INTO openlegal_admin.provider_recovery_audit(operation_id,plan,actor,database_actor,
        actual_writers_stopped_at,applied_at,before_state,result)
        VALUES($1,'{}','fixture',session_user,0,0,'{}',$2)")
        .bind(recovery).bind(json!({"hold_generation":generation,"recovery_hold":true}))
        .execute(&mut admin).await.unwrap();
    let budget: Value =
        sqlx::query_scalar("SELECT to_jsonb(b) FROM openlegal.provider_request_budget b")
            .fetch_one(&mut admin)
            .await
            .unwrap();
    let mut actor = PgConnection::connect_with(&options(&pair.runtime_url))
        .await
        .unwrap();
    sqlx::query("SET ROLE lifecycle_actor")
        .execute(&mut actor)
        .await
        .unwrap();
    for query in [
        "SET ROLE lifecycle_owner",
        "SET ROLE lifecycle_legacy_owner",
        "SELECT * FROM openlegal_checkpoint.canary_lifecycle",
        "UPDATE openlegal_admin.provider_control SET recovery_hold=false",
    ] {
        assert!(
            sqlx::query(query).execute(&mut actor).await.is_err(),
            "{query}"
        );
    }
    let wrong = new_id(&mut admin).await;
    assert!(activate(&mut actor, recovery, canary, wrong).await.is_err());
    let absent: i64 = sqlx::query_scalar("SELECT count(*) FROM openlegal_admin.provider_canary")
        .fetch_one(&mut admin)
        .await
        .unwrap();
    assert_eq!(absent, 0);
    let activated = tokio::time::timeout(Duration::from_secs(2), async {
        let result = activate(&mut actor, recovery, canary, generation)
            .await
            .unwrap();
        flush(&mut actor).await;
        result
    })
    .await
    .unwrap();
    assert_eq!(activated["canary_id"], json!(canary));
    assert_eq!(activated["recovery_hold"], false);
    assert_eq!(activated["consumed"], 0);
    assert!(
        activate(&mut actor, recovery, canary, generation)
            .await
            .is_err()
    );
    let early = new_id(&mut admin).await;
    assert_eq!(hold(&mut actor, early).await["owned"], true);
    assert!(finish(&mut actor, early, canary).await.is_err());
    release(&mut actor, early).await.unwrap();
    // Test-only clock fixture: this is NOT a real 21,600-second observation.
    // Preserve all timestamp relationships and immutable audit correspondence.
    sqlx::query("UPDATE openlegal_admin.provider_canary SET
        started_at=started_at-interval '6 hours 1 second',deadline=deadline-interval '6 hours 1 second',
        last_observed_at=last_observed_at-interval '6 hours 1 second' WHERE id=$1")
        .bind(canary).execute(&mut admin).await.unwrap();
    sqlx::query(
        "UPDATE openlegal_checkpoint.canary_lifecycle l SET activation_result=
        l.activation_result||jsonb_build_object('started_at',k.started_at,'deadline',k.deadline)
        FROM openlegal_admin.provider_canary k WHERE l.canary_id=k.id AND k.id=$1",
    )
    .bind(canary)
    .execute(&mut admin)
    .await
    .unwrap();
    let final_operation = new_id(&mut admin).await;
    hold(&mut actor, final_operation).await;
    let finished = tokio::time::timeout(Duration::from_secs(2), async {
        let result = finish(&mut actor, final_operation, canary).await.unwrap();
        flush(&mut actor).await;
        result
    })
    .await
    .unwrap();
    assert_eq!(finished["recovery_hold"], true);
    assert_eq!(
        finished["final_checkpoint_operation"],
        json!(final_operation)
    );
    assert!(finish(&mut actor, final_operation, canary).await.is_err());
    assert!(
        release(&mut actor, final_operation).await.is_err(),
        "closed canary must not reopen unrestricted traffic"
    );
    let readback: Value = sqlx::query_scalar("SELECT openlegal_checkpoint.canary_readback($1)")
        .bind(canary)
        .fetch_one(&mut actor)
        .await
        .unwrap();
    assert_eq!(readback["finish"], finished);
    let after: Value =
        sqlx::query_scalar("SELECT to_jsonb(b) FROM openlegal.provider_request_budget b")
            .fetch_one(&mut admin)
            .await
            .unwrap();
    assert_eq!(after, budget);
    let required: Uuid =
        sqlx::query_scalar("SELECT required_id FROM openlegal_admin.provider_canary_control")
            .fetch_one(&mut admin)
            .await
            .unwrap();
    assert_eq!(required, canary);
    actor.close().await.unwrap();
    admin.close().await.unwrap();
    drop(pair);
}
