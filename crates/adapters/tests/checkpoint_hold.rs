//! Real PostgreSQL ACL/ownership fixtures, with no provider traffic or host effects.
#[path = "../../../test-support/postgres.rs"]
#[allow(
    dead_code,
    reason = "shared fixture also exports Blob helpers used by the other persistence suites"
)]
mod support;

use serde_json::{Value, json};
use sqlx::{Connection, PgConnection};
use std::sync::atomic::{AtomicUsize, Ordering};
use uuid::Uuid;

static NEXT: AtomicUsize = AtomicUsize::new(1);

fn fixture_identifier(value: &str) {
    assert!(
        value.len() <= 63
            && value.starts_with("checkpoint_")
            && value
                .bytes()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'_')
    );
}

async fn set_actor(connection: &mut PgConnection, actor: &str) {
    fixture_identifier(actor);
    sqlx::query("SELECT set_config('role',$1,false)")
        .bind(actor)
        .execute(connection)
        .await
        .unwrap();
}

async fn operator(admin: &mut PgConnection) -> (String, String) {
    let suffix = format!(
        "{}_{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    );
    let owner = format!("checkpoint_owner_{suffix}");
    let actor = format!("checkpoint_actor_{suffix}");
    fixture_identifier(&owner);
    fixture_identifier(&actor);
    // Utility DDL cannot bind identifiers. Only the checked, locally generated
    // fixture roles above enter this SQL; every data value below uses bindings.
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
        "CREATE ROLE {owner} NOLOGIN; CREATE ROLE {actor} NOLOGIN;
         GRANT USAGE ON SCHEMA public,openlegal,openlegal_admin,openlegal_checkpoint TO {owner};
         GRANT USAGE ON SCHEMA openlegal_replication TO {owner};
         GRANT SELECT ON ALL TABLES IN SCHEMA openlegal,openlegal_admin,
           openlegal_replication,openlegal_checkpoint TO {owner};
         GRANT SELECT ON public._sqlx_migrations TO {owner};
         GRANT EXECUTE ON FUNCTION pg_catalog.pg_control_system(),
           pg_catalog.pg_control_checkpoint(),openlegal_admin.slot_locked(integer) TO {owner};
         GRANT pg_read_all_settings TO {owner} WITH INHERIT TRUE, SET FALSE;
         GRANT SELECT ON openlegal.provider_request_budget,openlegal_admin.provider_control,
           openlegal.provider_request_admission,openlegal_admin.provider_canary_control,
           openlegal_admin.provider_canary TO {owner};
         GRANT UPDATE(singleton) ON openlegal.corpus_control,openlegal.provider_request_budget TO {owner};
         GRANT SELECT ON openlegal.corpus_control TO {owner};
         GRANT UPDATE(recovery_hold,generation,hold_started_at) ON openlegal_admin.provider_control TO {owner};
         GRANT SELECT,INSERT,UPDATE ON openlegal_checkpoint.operation TO {owner};
         GRANT CREATE ON SCHEMA openlegal_checkpoint TO {owner};
         ALTER FUNCTION openlegal_checkpoint.state() OWNER TO {owner};
         ALTER FUNCTION openlegal_checkpoint.hold(uuid,text) OWNER TO {owner};
         ALTER FUNCTION openlegal_checkpoint.release(uuid,text) OWNER TO {owner};
         ALTER FUNCTION openlegal_checkpoint.readback(uuid) OWNER TO {owner};
         ALTER FUNCTION openlegal_checkpoint.observe(uuid) OWNER TO {owner};
         ALTER FUNCTION openlegal_checkpoint.retained_tables() OWNER TO {owner};
         ALTER FUNCTION openlegal_checkpoint.retained_references() OWNER TO {owner};
         ALTER FUNCTION openlegal_checkpoint.database_position() OWNER TO {owner};
         ALTER FUNCTION openlegal_checkpoint.database_layout() OWNER TO {owner};
         ALTER FUNCTION openlegal_checkpoint.client_sessions() OWNER TO {owner};
         ALTER FUNCTION openlegal_checkpoint.storage_metadata() OWNER TO {owner};
         ALTER FUNCTION openlegal_checkpoint.canary_observation(uuid) OWNER TO {owner};
         REVOKE CREATE ON SCHEMA openlegal_checkpoint FROM {owner};
         GRANT USAGE ON SCHEMA openlegal_checkpoint TO {actor};
         GRANT EXECUTE ON FUNCTION openlegal_checkpoint.hold(uuid,text),
           openlegal_checkpoint.release(uuid,text),openlegal_checkpoint.readback(uuid),
           openlegal_checkpoint.state(),
           openlegal_checkpoint.observe(uuid),openlegal_checkpoint.retained_tables(),
           openlegal_checkpoint.retained_references(),openlegal_checkpoint.database_position(),
           openlegal_checkpoint.database_layout(),
           openlegal_checkpoint.client_sessions(),openlegal_checkpoint.storage_metadata(),
           openlegal_checkpoint.canary_observation(uuid) TO {actor};"
    ))).execute(&mut *admin).await.unwrap();
    (owner, actor)
}

async fn drop_roles(admin: &mut PgConnection, owner: &str, actor: &str) {
    fixture_identifier(owner);
    fixture_identifier(actor);
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!("RESET ROLE; DROP OWNED BY {actor}; DROP OWNED BY {owner} CASCADE; DROP ROLE {actor}; DROP ROLE {owner};")))
        .execute(admin).await.unwrap();
}

async fn call(
    connection: &mut PgConnection,
    function: &str,
    operation: Uuid,
    intent: &str,
) -> Result<Value, sqlx::Error> {
    let sql = match function {
        "hold" => "SELECT openlegal_checkpoint.hold($1,$2)",
        "release" => "SELECT openlegal_checkpoint.release($1,$2)",
        _ => panic!("invalid fixed fixture operation"),
    };
    sqlx::query_scalar(sql)
        .bind(operation)
        .bind(intent)
        .fetch_one(connection)
        .await
}

async fn new_operation(connection: &mut PgConnection) -> Uuid {
    sqlx::query_scalar("SELECT pg_catalog.uuidv7()")
        .fetch_one(connection)
        .await
        .unwrap()
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn checkpoint_observers_use_function_only_acl_and_actual_slot_lock_provenance() {
    let fixture = support::TestDatabase::new().await;
    let mut admin = PgConnection::connect(&fixture.url).await.unwrap();
    let (owner, actor) = operator(&mut admin).await;
    let operation = new_operation(&mut admin).await;
    let mut holder = PgConnection::connect(&fixture.url).await.unwrap();
    let holder_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut holder)
        .await
        .unwrap();
    let admission = new_operation(&mut admin).await;
    sqlx::query("INSERT INTO openlegal.provider_request_admission(owner,slot,mode,started_at_ms) VALUES($1,1,'continuous',1)")
        .bind(admission).execute(&mut admin).await.unwrap();
    sqlx::query("SELECT pg_advisory_lock(1869376611,1818326865)")
        .execute(&mut holder)
        .await
        .unwrap();
    set_actor(&mut admin, &actor).await;
    let observed: Value = sqlx::query_scalar("SELECT openlegal_checkpoint.observe($1)")
        .bind(operation)
        .fetch_one(&mut admin)
        .await
        .unwrap();
    assert_eq!(
        observed["state"]["admissions"][0]["owner"],
        admission.to_string()
    );
    assert_eq!(
        observed["admission_ownership"]["slots"],
        json!([{"owner":admission,"slot":1,"locked":true}])
    );
    assert_eq!(observed["admission_ownership"]["legacy_locked"], false);
    assert!(observed["checkpoint_owner"].is_null());
    assert!(observed["checkpoint_readback"].is_null());
    for sql in [
        "SELECT * FROM openlegal.provider_request_budget",
        "SELECT * FROM openlegal_admin.provider_control",
        "SELECT * FROM openlegal_checkpoint.operation",
        "SELECT * FROM openlegal.cache_snapshot",
        "UPDATE openlegal.provider_request_budget SET daily_used=0",
        "SELECT openlegal_admin.slot_locked(1)",
    ] {
        assert!(
            sqlx::query(sql).execute(&mut admin).await.is_err(),
            "unexpected privilege: {sql}"
        );
    }
    let tables: Value = sqlx::query_scalar("SELECT openlegal_checkpoint.retained_tables()")
        .fetch_one(&mut admin)
        .await
        .unwrap();
    assert_eq!(tables["openlegal.provider_request_admission"]["rows"], 1);
    assert_eq!(tables["openlegal.cache_snapshot"]["rows"], 0);
    assert_eq!(
        tables["openlegal.cache_snapshot"]["sha256"]
            .as_str()
            .unwrap()
            .len(),
        64
    );
    let references: Value = sqlx::query_scalar("SELECT openlegal_checkpoint.retained_references()")
        .fetch_one(&mut admin)
        .await
        .unwrap();
    assert_eq!(references, json!([]));
    let position: Value = sqlx::query_scalar("SELECT openlegal_checkpoint.database_position()")
        .fetch_one(&mut admin)
        .await
        .unwrap();
    assert_eq!(position["in_recovery"], false);
    assert!(position["system_identifier"].is_string());
    assert!(position["postmaster_started_at"].is_string());
    let layout: Value = sqlx::query_scalar("SELECT openlegal_checkpoint.database_layout()")
        .fetch_one(&mut admin)
        .await
        .unwrap();
    assert_eq!(layout.as_object().unwrap().len(), 1);
    assert!(layout["data_directory"].as_str().unwrap().starts_with('/'));
    assert!(
        sqlx::query("SELECT current_setting('data_directory')")
            .execute(&mut admin)
            .await
            .is_err()
    );
    let metadata: Value = sqlx::query_scalar("SELECT openlegal_checkpoint.storage_metadata()")
        .fetch_one(&mut admin)
        .await
        .unwrap();
    assert_eq!(
        metadata["cache_totals"],
        json!({"snapshots":0,"queries":0,"referenced_bytes":0})
    );
    let sessions: Value = sqlx::query_scalar("SELECT openlegal_checkpoint.client_sessions()")
        .fetch_one(&mut admin)
        .await
        .unwrap();
    assert!(
        sessions["sessions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|row| row["pid"] == holder_pid)
    );
    assert!(
        sqlx::query("SELECT openlegal_checkpoint.observe($1)")
            .bind(Uuid::nil())
            .execute(&mut admin)
            .await
            .is_err()
    );
    sqlx::query("SELECT pg_advisory_unlock_all()")
        .execute(&mut holder)
        .await
        .unwrap();
    let unlocked: Value = sqlx::query_scalar("SELECT openlegal_checkpoint.observe($1)")
        .bind(operation)
        .fetch_one(&mut admin)
        .await
        .unwrap();
    assert_eq!(unlocked["admission_ownership"]["slots"][0]["locked"], false);
    let canary: Value = sqlx::query_scalar("SELECT openlegal_checkpoint.canary_observation($1)")
        .bind(operation)
        .fetch_one(&mut admin)
        .await
        .unwrap();
    assert_eq!(canary["schema_version"], 1);
    assert_eq!(canary["kind"], "openlegal_canary_observation");
    assert!(canary["canary"].is_null());
    assert!(canary["required_id"].is_null());
    assert!(canary["observed_at"].as_str().unwrap().ends_with('Z'));
    sqlx::query("SELECT pg_advisory_lock(1869376611,1818326864)")
        .execute(&mut holder)
        .await
        .unwrap();
    let legacy: Value = sqlx::query_scalar("SELECT openlegal_checkpoint.observe($1)")
        .bind(operation)
        .fetch_one(&mut admin)
        .await
        .unwrap();
    assert_eq!(legacy["admission_ownership"]["legacy_locked"], true);
    holder.close().await.unwrap();
    drop_roles(&mut admin, &owner, &actor).await;
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn checkpoint_hold_clean_ownership_acl_and_historical_receipts() {
    let fixture = support::TestDatabase::new().await;
    let mut admin = PgConnection::connect(&fixture.url).await.unwrap();
    let (owner, actor) = operator(&mut admin).await;
    let operation = new_operation(&mut admin).await;
    sqlx::query(
        "UPDATE openlegal.provider_request_budget SET daily_used=7,on_demand_used=3,pilot_used=2",
    )
    .execute(&mut admin)
    .await
    .unwrap();
    let before: Value =
        sqlx::query_scalar("SELECT to_jsonb(b) FROM openlegal.provider_request_budget b")
            .fetch_one(&mut admin)
            .await
            .unwrap();
    set_actor(&mut admin, &actor).await;
    let intent = "a".repeat(64);
    let held = call(&mut admin, "hold", operation, &intent).await.unwrap();
    assert_eq!(held["owned"], true);
    assert_eq!(held["provider_recovery_hold"], true);
    assert_eq!(
        call(&mut admin, "hold", operation, &intent).await.unwrap(),
        held
    );
    assert!(
        call(&mut admin, "hold", operation, &"b".repeat(64))
            .await
            .is_err()
    );
    assert!(
        sqlx::query("UPDATE openlegal_admin.provider_control SET recovery_hold=false")
            .execute(&mut admin)
            .await
            .is_err()
    );
    assert!(
        sqlx::query("DELETE FROM openlegal_checkpoint.operation")
            .execute(&mut admin)
            .await
            .is_err()
    );
    let state: Value = sqlx::query_scalar("SELECT openlegal_checkpoint.state()")
        .fetch_one(&mut admin)
        .await
        .unwrap();
    assert_eq!(state["budget"]["daily_used"], 7);
    let released = call(&mut admin, "release", operation, &intent)
        .await
        .unwrap();
    assert_eq!(released["released"], true);
    assert_eq!(released["provider_recovery_hold"], false);
    assert_eq!(
        call(&mut admin, "release", operation, &intent)
            .await
            .unwrap(),
        released
    );
    assert!(call(&mut admin, "hold", operation, &intent).await.is_err());
    sqlx::query("RESET ROLE").execute(&mut admin).await.unwrap();
    let after: Value =
        sqlx::query_scalar("SELECT to_jsonb(b) FROM openlegal.provider_request_budget b")
            .fetch_one(&mut admin)
            .await
            .unwrap();
    assert_eq!(before, after);
    sqlx::query("UPDATE openlegal_admin.provider_control SET recovery_hold=true,generation=pg_catalog.uuidv7()")
        .execute(&mut admin).await.unwrap();
    set_actor(&mut admin, &actor).await;
    assert!(
        call(&mut admin, "release", operation, &intent)
            .await
            .is_err()
    );
    let audit: Value = sqlx::query_scalar("SELECT openlegal_checkpoint.readback($1)")
        .bind(operation)
        .fetch_one(&mut admin)
        .await
        .unwrap();
    // Readback is explicitly historical and cannot attest current hold status.
    assert_eq!(audit["release"], released);
    drop_roles(&mut admin, &owner, &actor).await;
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn checkpoint_hold_preserves_existing_uncertainty_and_never_owns_it() {
    let fixture = support::TestDatabase::new().await;
    let mut admin = PgConnection::connect(&fixture.url).await.unwrap();
    let (owner, actor) = operator(&mut admin).await;
    let operation = new_operation(&mut admin).await;
    sqlx::raw_sql(
        "UPDATE openlegal_admin.provider_control SET recovery_hold=true;
        UPDATE openlegal.provider_request_budget SET unresolved_response=true,daily_used=8;
        INSERT INTO openlegal.provider_request_admission(owner,slot,mode,started_at_ms)
          VALUES(pg_catalog.uuidv7(),1,'continuous',1)",
    )
    .execute(&mut admin)
    .await
    .unwrap();
    let before: Value = sqlx::query_scalar("SELECT openlegal_checkpoint.state()")
        .fetch_one(&mut admin)
        .await
        .unwrap();
    set_actor(&mut admin, &actor).await;
    let held = call(&mut admin, "hold", operation, &"c".repeat(64))
        .await
        .unwrap();
    assert_eq!(held["owned"], false);
    assert!(
        call(&mut admin, "release", operation, &"c".repeat(64))
            .await
            .is_err()
    );
    sqlx::query("RESET ROLE").execute(&mut admin).await.unwrap();
    let after: Value = sqlx::query_scalar("SELECT openlegal_checkpoint.state()")
        .fetch_one(&mut admin)
        .await
        .unwrap();
    assert_eq!(before, after);
    let hold: bool =
        sqlx::query_scalar("SELECT recovery_hold FROM openlegal_admin.provider_control")
            .fetch_one(&mut admin)
            .await
            .unwrap();
    assert!(hold);
    drop_roles(&mut admin, &owner, &actor).await;
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn checkpoint_hold_changed_accounting_or_live_owner_refuses_release() {
    let fixture = support::TestDatabase::new().await;
    let mut admin = PgConnection::connect(&fixture.url).await.unwrap();
    let (owner, actor) = operator(&mut admin).await;
    let operation = new_operation(&mut admin).await;
    set_actor(&mut admin, &actor).await;
    call(&mut admin, "hold", operation, &"d".repeat(64))
        .await
        .unwrap();
    sqlx::query("RESET ROLE").execute(&mut admin).await.unwrap();
    let mut owner_session = PgConnection::connect(&fixture.url).await.unwrap();
    let locked: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock(1869376611,1818326864+16)")
        .fetch_one(&mut owner_session)
        .await
        .unwrap();
    assert!(locked);
    set_actor(&mut admin, &actor).await;
    assert!(
        call(&mut admin, "release", operation, &"d".repeat(64))
            .await
            .is_err()
    );
    owner_session.close().await.unwrap();
    sqlx::query("RESET ROLE").execute(&mut admin).await.unwrap();
    sqlx::query("UPDATE openlegal.provider_request_budget SET daily_used=daily_used+1")
        .execute(&mut admin)
        .await
        .unwrap();
    set_actor(&mut admin, &actor).await;
    assert!(
        call(&mut admin, "release", operation, &"d".repeat(64))
            .await
            .is_err()
    );
    sqlx::query("RESET ROLE").execute(&mut admin).await.unwrap();
    let state: (bool, i64) = sqlx::query_as("SELECT recovery_hold,daily_used FROM openlegal_admin.provider_control CROSS JOIN openlegal.provider_request_budget")
        .fetch_one(&mut admin).await.unwrap();
    assert_eq!(state, (true, 1));
    drop_roles(&mut admin, &owner, &actor).await;
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn checkpoint_hold_public_and_unprivileged_runtime_cannot_change_state() {
    let fixture = support::TestDatabase::new().await;
    let mut admin = PgConnection::connect(&fixture.url).await.unwrap();
    let role = format!(
        "checkpoint_denied_{}_{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    );
    fixture_identifier(&role);
    sqlx::query(sqlx::AssertSqlSafe(format!("CREATE ROLE {role} NOLOGIN")))
        .execute(&mut admin)
        .await
        .unwrap();
    set_actor(&mut admin, &role).await;
    for sql in [
        "SELECT openlegal_checkpoint.hold('a1f71db8-bd59-4d33-a3bb-fb77154b571e','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')",
        "SELECT openlegal_checkpoint.readback('a1f71db8-bd59-4d33-a3bb-fb77154b571e')",
        "SELECT * FROM openlegal_checkpoint.operation",
    ] {
        assert!(sqlx::query(sql).execute(&mut admin).await.is_err());
    }
    sqlx::query("RESET ROLE").execute(&mut admin).await.unwrap();
    let unchanged: Value = sqlx::query_scalar("SELECT jsonb_build_object('held',recovery_hold,'operations',(SELECT count(*) FROM openlegal_checkpoint.operation)) FROM openlegal_admin.provider_control")
        .fetch_one(&mut admin).await.unwrap();
    assert_eq!(unchanged, json!({"held":false,"operations":0}));
    sqlx::query(sqlx::AssertSqlSafe(format!("DROP ROLE {role}")))
        .execute(&mut admin)
        .await
        .unwrap();
}
