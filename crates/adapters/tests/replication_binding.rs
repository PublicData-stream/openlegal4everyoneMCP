//! Real isolated TLS pair. Metadata sentinels are not production Blob seed proof.
#[path = "../../../test-support/replication.rs"]
#[allow(
    dead_code,
    reason = "shared pair fixture also exports fault controls used by other suites"
)]
mod support;

use serde_json::{Value, json};
use sqlx::{
    Connection, PgConnection,
    postgres::{PgConnectOptions, PgSslMode},
};
use std::{str::FromStr, time::Duration};
use uuid::Uuid;

async fn bind(
    connection: &mut PgConnection,
    operation: Uuid,
    old: Value,
    new: Value,
) -> Result<Value, sqlx::Error> {
    sqlx::query_scalar("SELECT openlegal_checkpoint.bind_replica($1,$2,$3,$4)")
        .bind(operation)
        .bind("a".repeat(64))
        .bind(old)
        .bind(new)
        .fetch_one(connection)
        .await
}

#[tokio::test]
#[ignore = "requires scripts/test-replication.sh isolated real TLS PG18 pair"]
async fn replication_binding_function_only_role_checks_actual_peer_and_never_replays() {
    let pair = support::TestReplication::new().await;
    let options = PgConnectOptions::from_str(&pair.admin_url)
        .unwrap()
        .ssl_mode(PgSslMode::VerifyFull)
        .ssl_root_cert(pair.directory.path().join("ca.crt"));
    let mut admin = PgConnection::connect_with(&options).await.unwrap();
    let original: Value = sqlx::query_scalar(
        "SELECT (to_jsonb(c)-'singleton') || jsonb_build_object(
           'standby_client_serial',standby_client_serial::text)
         FROM openlegal_replication.control c WHERE singleton",
    )
    .fetch_one(&mut admin)
    .await
    .unwrap();
    let operation: Uuid = sqlx::query_scalar("SELECT pg_catalog.uuidv7()")
        .fetch_one(&mut admin)
        .await
        .unwrap();
    let epoch: Uuid = sqlx::query_scalar("SELECT pg_catalog.uuidv7()")
        .fetch_one(&mut admin)
        .await
        .unwrap();
    // Only this isolated fixture clears its synthetic control for a first-seed
    // exercise. The production operator never deletes/replays a binding.
    sqlx::raw_sql("UPDATE openlegal_admin.provider_control SET recovery_hold=true;
        DELETE FROM openlegal_replication.control;
        CREATE ROLE checkpoint_bind_owner NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
        CREATE ROLE checkpoint_bind_actor NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
        GRANT USAGE ON SCHEMA openlegal,openlegal_admin,openlegal_checkpoint,openlegal_replication TO checkpoint_bind_owner;
        GRANT SELECT ON openlegal.corpus_control,openlegal.provider_request_budget,openlegal_admin.provider_control TO checkpoint_bind_owner;
        GRANT UPDATE(singleton) ON openlegal.corpus_control,openlegal.provider_request_budget,openlegal_admin.provider_control TO checkpoint_bind_owner;
        GRANT SELECT,INSERT,UPDATE ON openlegal_replication.control TO checkpoint_bind_owner;
        GRANT EXECUTE ON FUNCTION openlegal_replication.capture_prefix() TO checkpoint_bind_owner;
        GRANT CREATE ON SCHEMA openlegal_checkpoint TO checkpoint_bind_owner;
        ALTER FUNCTION openlegal_checkpoint.bind_replica(uuid,text,jsonb,jsonb) OWNER TO checkpoint_bind_owner;
        REVOKE CREATE ON SCHEMA openlegal_checkpoint FROM checkpoint_bind_owner;
        GRANT USAGE ON SCHEMA openlegal_checkpoint,openlegal_replication TO checkpoint_bind_actor;
        GRANT EXECUTE ON FUNCTION openlegal_checkpoint.bind_replica(uuid,text,jsonb,jsonb),
          openlegal_replication.capture_prefix(),openlegal_replication.observe_flush(),
          openlegal_replication.observe_candidate(jsonb) TO checkpoint_bind_actor;
        GRANT checkpoint_bind_actor TO openlegal_fixture_runtime;
        GRANT CREATE ON SCHEMA openlegal_replication TO openlegal_fixture_observer;
        ALTER FUNCTION openlegal_replication.observe_candidate(jsonb) OWNER TO openlegal_fixture_observer;
        REVOKE CREATE ON SCHEMA openlegal_replication FROM openlegal_fixture_observer;")
        .execute(&mut admin).await.unwrap();
    // A superuser session can SET ROLE to any owner even after SET ROLE actor.
    // Use the actual non-superuser LOGIN to exercise the membership boundary.
    let actor_options = PgConnectOptions::from_str(&pair.runtime_url)
        .unwrap()
        .ssl_mode(PgSslMode::VerifyFull)
        .ssl_root_cert(pair.directory.path().join("ca.crt"));
    let mut actor = PgConnection::connect_with(&actor_options).await.unwrap();
    let mut proposed = original.clone();
    proposed["managed_epoch"] = json!(epoch);
    proposed["blob_binding_sha256"] = json!("a".repeat(64));
    sqlx::query("SET ROLE checkpoint_bind_actor")
        .execute(&mut actor)
        .await
        .unwrap();
    for statement in [
        "SELECT * FROM openlegal_replication.control",
        "UPDATE openlegal_admin.provider_control SET recovery_hold=false",
        "SET ROLE checkpoint_bind_owner",
    ] {
        assert!(sqlx::query(statement).execute(&mut actor).await.is_err());
    }
    let candidate: Value = sqlx::query_scalar("SELECT openlegal_replication.observe_candidate($1)")
        .bind(&proposed)
        .fetch_one(&mut actor)
        .await
        .unwrap();
    assert_eq!(candidate["kind"], "openlegal_replication_candidate_peer");
    assert_eq!(candidate["proposed_binding"], proposed);
    assert!(candidate["candidate"].get("managed_epoch").is_none());
    assert!(candidate["candidate"].get("blob_binding_sha256").is_none());
    let mut wrong_peer = proposed.clone();
    wrong_peer["standby_client_serial"] = json!("1");
    assert!(
        bind(&mut actor, operation, Value::Null, wrong_peer)
            .await
            .is_err()
    );
    let absent: i64 = sqlx::query_scalar("SELECT count(*) FROM openlegal_replication.control")
        .fetch_one(&mut admin)
        .await
        .unwrap();
    assert_eq!(
        absent, 0,
        "failed authenticated-peer check must roll back the inserted control"
    );
    let result = tokio::time::timeout(Duration::from_secs(2), async {
        let result = bind(&mut actor, operation, Value::Null, proposed.clone())
            .await
            .unwrap();
        let before: Value = sqlx::query_scalar("SELECT openlegal_replication.capture_prefix()")
            .fetch_one(&mut actor)
            .await
            .unwrap();
        let after: Value = sqlx::query_scalar("SELECT openlegal_replication.observe_flush()")
            .fetch_one(&mut actor)
            .await
            .unwrap();
        assert_eq!(before["managed_epoch"], json!(epoch));
        assert_eq!(
            before["blob_binding_sha256"],
            proposed["blob_binding_sha256"]
        );
        for key in [
            "primary_system_identifier",
            "timeline",
            "postmaster_started_at",
            "sender_pid",
            "sender_started_at",
        ] {
            assert_eq!(before[key], after[key]);
        }
        fn lsn(value: &Value) -> u64 {
            let (a, b) = value.as_str().unwrap().split_once('/').unwrap();
            (u64::from_str_radix(a, 16).unwrap() << 32) | u64::from_str_radix(b, 16).unwrap()
        }
        assert!(lsn(&after["flush_lsn"]) >= lsn(&before["prefix_lsn"]));
        result
    })
    .await
    .unwrap();
    assert_eq!(result["operation_id"], json!(operation));
    assert!(result["previous_binding"].is_null());
    assert_eq!(result["new_binding"]["managed_epoch"], json!(epoch));
    // A lost/repeated first-seed call cannot overwrite an installed binding.
    assert!(
        bind(&mut actor, operation, Value::Null, proposed.clone())
            .await
            .is_err()
    );
    let mut same_timeline = proposed.clone();
    same_timeline["managed_epoch"] = json!(operation);
    assert!(
        bind(&mut actor, operation, proposed.clone(), same_timeline)
            .await
            .is_err()
    );
    let current: Value = sqlx::query_scalar(
        "SELECT (to_jsonb(c)-'singleton') || jsonb_build_object(
           'standby_client_serial',standby_client_serial::text)
         FROM openlegal_replication.control c WHERE singleton",
    )
    .fetch_one(&mut admin)
    .await
    .unwrap();
    assert_eq!(current, proposed);
    let held: bool = sqlx::query_scalar(
        "SELECT recovery_hold FROM openlegal_admin.provider_control WHERE singleton",
    )
    .fetch_one(&mut admin)
    .await
    .unwrap();
    assert!(held);
    actor.close().await.unwrap();
    admin.close().await.unwrap();
    drop(pair);
}
