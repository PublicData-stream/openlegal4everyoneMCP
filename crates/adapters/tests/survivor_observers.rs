//! Actual disposable TLS PG18 standby observers and effective function-only role.
//! Blob keys and bodies are synthetic. No physical seed, provider, public MCP,
//! promotion/reseed, six-hour canary run or recovery/Ready acceptance is implied.
#[path = "../../../test-support/replication.rs"]
#[allow(dead_code, reason = "shared pair fixture exports other fault controls")]
mod support;

use serde_json::{Value, json};
use sqlx::{
    Connection, PgConnection,
    postgres::{PgConnectOptions, PgSslMode},
};
use std::{str::FromStr, time::Duration};

const BODY: &str = "SURVIVOR_BODY_SENTINEL_PRIVATE";
const QUERY: &str = "SURVIVOR_QUERY_SENTINEL_PRIVATE";
const SOURCE: &str = "https://example.test/source?private=SURVIVOR_SOURCE_SENTINEL";

// This is the explicit native grant chain under test. No owner is a LOGIN or
// granted to the actor. The fixture LOGIN retains its normal test role, but SET
// ROLE survivor_reader removes those privileges for EVERY actor assertion.
const GRANTS: &str = r#"
CREATE ROLE survivor_replication_owner NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
CREATE ROLE survivor_checkpoint_owner NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
CREATE ROLE survivor_reader NOLOGIN NOSUPERUSER NOINHERIT NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
GRANT USAGE ON SCHEMA openlegal_replication TO survivor_replication_owner;
GRANT SELECT ON openlegal_replication.control TO survivor_replication_owner;
GRANT EXECUTE ON FUNCTION pg_catalog.pg_control_system(),pg_catalog.pg_control_checkpoint(),
 pg_catalog.pg_is_wal_replay_paused()
 TO survivor_replication_owner;
GRANT pg_read_all_settings TO survivor_replication_owner;
GRANT CREATE ON SCHEMA openlegal_replication TO survivor_replication_owner;
ALTER FUNCTION openlegal_replication.survivor_position() OWNER TO survivor_replication_owner;
REVOKE CREATE ON SCHEMA openlegal_replication FROM survivor_replication_owner;

GRANT USAGE ON SCHEMA openlegal,openlegal_admin,openlegal_replication,openlegal_checkpoint
 TO survivor_checkpoint_owner;
GRANT SELECT ON ALL TABLES IN SCHEMA openlegal,openlegal_admin,openlegal_replication,openlegal_checkpoint
 TO survivor_checkpoint_owner;
GRANT SELECT ON public._sqlx_migrations TO survivor_checkpoint_owner;
GRANT EXECUTE ON FUNCTION openlegal_replication.survivor_position() TO survivor_checkpoint_owner;
GRANT CREATE ON SCHEMA openlegal_checkpoint TO survivor_checkpoint_owner;
ALTER FUNCTION openlegal_checkpoint.state() OWNER TO survivor_checkpoint_owner;
ALTER FUNCTION openlegal_checkpoint.retained_tables() OWNER TO survivor_checkpoint_owner;
ALTER FUNCTION openlegal_checkpoint.retained_references() OWNER TO survivor_checkpoint_owner;
ALTER FUNCTION openlegal_checkpoint.storage_metadata() OWNER TO survivor_checkpoint_owner;
ALTER FUNCTION openlegal_checkpoint.client_sessions() OWNER TO survivor_checkpoint_owner;
ALTER FUNCTION openlegal_checkpoint.provider_survivor_state() OWNER TO survivor_checkpoint_owner;
ALTER FUNCTION openlegal_checkpoint.survivor_snapshot() OWNER TO survivor_checkpoint_owner;
REVOKE CREATE ON SCHEMA openlegal_checkpoint FROM survivor_checkpoint_owner;

GRANT USAGE ON SCHEMA openlegal_replication,openlegal_checkpoint TO survivor_reader;
GRANT EXECUTE ON FUNCTION openlegal_replication.survivor_position(),
 openlegal_checkpoint.provider_survivor_state(),openlegal_checkpoint.survivor_snapshot(),
 openlegal_checkpoint.client_sessions() TO survivor_reader;
GRANT survivor_reader TO openlegal_fixture_runtime;
"#;

fn options(pair: &support::TestReplication, url: &str) -> PgConnectOptions {
    PgConnectOptions::from_str(url)
        .unwrap()
        .ssl_mode(PgSslMode::VerifyFull)
        .ssl_root_cert(pair.directory.path().join("ca.crt"))
}

async fn actor(pair: &support::TestReplication, url: &str) -> PgConnection {
    let mut connection = PgConnection::connect_with(&options(pair, url))
        .await
        .unwrap();
    // Before SET ROLE, the authenticated role owns this backend's statistics.
    // The restricted effective reader deliberately cannot unmask that role.
    let ssl: Option<bool> =
        sqlx::query_scalar("SELECT ssl FROM pg_stat_ssl WHERE pid=pg_backend_pid()")
            .fetch_one(&mut connection)
            .await
            .unwrap();
    assert_eq!(ssl, Some(true));
    sqlx::raw_sql("SET ROLE survivor_reader; SET TimeZone='UTC'; SET DateStyle='ISO,YMD';")
        .execute(&mut connection)
        .await
        .unwrap();
    let identity: Value = sqlx::query_scalar(
        "SELECT jsonb_build_object('session',session_user,'effective',current_user,
         'superuser',r.rolsuper,'create_role',r.rolcreaterole,'create_database',r.rolcreatedb,
         'replication',r.rolreplication,'bypass_rls',r.rolbypassrls,
         'replication_owner_member',pg_has_role(session_user,'survivor_replication_owner','MEMBER'),
         'checkpoint_owner_member',pg_has_role(session_user,'survivor_checkpoint_owner','MEMBER'),
         'settings_member',pg_has_role(session_user,'pg_read_all_settings','MEMBER'),
         'stats_member',pg_has_role(session_user,'pg_read_all_stats','MEMBER'))
         FROM pg_roles r WHERE rolname=session_user",
    )
    .fetch_one(&mut connection)
    .await
    .unwrap();
    assert_eq!(identity["session"], "openlegal_fixture_runtime");
    assert_eq!(identity["effective"], "survivor_reader");
    for key in [
        "superuser",
        "create_role",
        "create_database",
        "replication",
        "bypass_rls",
        "replication_owner_member",
        "checkpoint_owner_member",
        "settings_member",
        "stats_member",
    ] {
        assert_eq!(identity[key], false, "restricted actual LOGIN: {key}");
    }
    connection
}

fn lsn(value: &Value) -> u64 {
    let (high, low) = value
        .as_str()
        .expect("actual nonnull LSN")
        .split_once('/')
        .unwrap();
    (u64::from_str_radix(high, 16).unwrap() << 32) | u64::from_str_radix(low, 16).unwrap()
}

async fn position(connection: &mut PgConnection) -> Value {
    sqlx::query_scalar("SELECT openlegal_replication.survivor_position()")
        .fetch_one(connection)
        .await
        .unwrap()
}

fn fixed_position(observed: &Value, target: &Value) {
    assert_eq!(observed["wal_receiver_count"], 0);
    assert_eq!(observed["receive_lsn"], target["receive_lsn"]);
    for identity in [
        "schema_version",
        "kind",
        "system_identifier",
        "timeline",
        "postmaster_started_at",
        "data_directory",
        "control",
        "in_recovery",
        "replay_paused",
    ] {
        assert_eq!(
            observed[identity], target[identity],
            "fixed survivor: {identity}"
        );
    }
}

async fn refused(connection: &mut PgConnection, query: &'static str, code: &str) {
    let error = sqlx::query(query)
        .execute(connection)
        .await
        .expect_err(query);
    assert_eq!(
        error.as_database_error().and_then(|e| e.code()).as_deref(),
        Some(code)
    );
}

async fn snapshot(connection: &mut PgConnection) -> Value {
    sqlx::raw_sql(
        "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;
         SET LOCAL statement_timeout='30000ms'; SET LOCAL lock_timeout='1000ms';
         SET LOCAL transaction_timeout='35000ms';
         SET LOCAL idle_in_transaction_session_timeout='5000ms';",
    )
    .execute(&mut *connection)
    .await
    .unwrap();
    let value = sqlx::query_scalar("SELECT openlegal_checkpoint.survivor_snapshot()")
        .fetch_one(&mut *connection)
        .await
        .unwrap();
    sqlx::query("ROLLBACK").execute(connection).await.unwrap();
    value
}

#[tokio::test]
#[ignore = "requires scripts/test-replication.sh isolated real TLS PG18 pair"]
async fn survivor_observers_preserve_fixed_standby_prefix_with_function_only_login() {
    let pair = support::TestReplication::new().await;
    let primary_options = options(&pair, &pair.admin_url);
    let mut admin = PgConnection::connect_with(&primary_options).await.unwrap();
    sqlx::raw_sql(GRANTS).execute(&mut admin).await.unwrap();
    // UTC and textual settings match the observer's deterministic row hashes.
    sqlx::raw_sql(r#"SET TimeZone='UTC'; SET DateStyle='ISO,YMD'; SET IntervalStyle='iso_8601';
        SET bytea_output='hex'; SET extra_float_digits=3;
        INSERT INTO openlegal.cache_query(namespace,provider,dataset,query_hash,canonical_identity,query,created_at)
          VALUES('survivor','fixture','fixture',decode(repeat('01',32),'hex'),decode('01','hex'),
          '{"private":"SURVIVOR_QUERY_SENTINEL_PRIVATE"}',1);
        INSERT INTO openlegal.blob_object(sha256,size_bytes,storage_key,ready,created_at)
          VALUES(decode(repeat('02',32),'hex'),3,'02/'||repeat('02',32)||'-'||uuidv7(),true,1);
        INSERT INTO openlegal.cache_snapshot(public_id,query_id,sequence,captured_at,retrieved_at,
          original_validated_at,processor_version,schema_version,raw_blob_sha256,processed_sha256,
          processed_data,source_reference,envelope_sha256)
          SELECT repeat('3',64),id,1,1,1,1,'fixture',1,decode(repeat('02',32),'hex'),
          decode(repeat('04',32),'hex'),'{"body":"SURVIVOR_BODY_SENTINEL_PRIVATE"}',
          'https://example.test/source?private=SURVIVOR_SOURCE_SENTINEL',decode(repeat('05',32),'hex')
          FROM openlegal.cache_query;
        UPDATE openlegal.cache_storage SET snapshots=1,queries=1,referenced_bytes=3;
        INSERT INTO openlegal.corpus_object(object_key,identity)
          VALUES(repeat('b',64),'{"jurisdiction":"kr","provider":"law_go_kr","dataset":"national_statute","id":"synthetic-survivor"}');
        INSERT INTO openlegal.corpus_capture(id,object_key,revision_id,sequence,captured_at,event_sequence,
          payload,payload_sha256,raw_sha256,raw_size,storage_key)
          VALUES(repeat('a',64),repeat('b',64),'fixture-revision',1,1,1,
          '{"record":{"body":"SURVIVOR_BODY_SENTINEL_PRIVATE"}}',decode(repeat('06',32),'hex'),
          decode(repeat('cc',32),'hex'),3,'cc/'||repeat('cc',32)||'-'||uuidv7());
        UPDATE openlegal.corpus_control SET next_event=2,index_ack=1;
        UPDATE openlegal.provider_request_budget SET utc_day=20757,daily_used=664,on_demand_used=17,
          pilot_used=6,unresolved_response=true,admission_owner='00000000-0000-0000-0000-000000000001';
        UPDATE openlegal_admin.provider_control SET recovery_hold=true,hold_started_at=12345,
          legacy_identity='00000000-0000-0000-0000-000000000002',operator_suspended_at=12345;
        INSERT INTO openlegal.provider_request_admission(owner,slot,mode,started_at_ms)
          VALUES('00000000-0000-0000-0000-000000000003',3,'continuous',12345000);
        INSERT INTO openlegal_admin.provider_recovery_audit(operation_id,plan,actor,database_actor,
          actual_writers_stopped_at,applied_at,before_state,result)
          VALUES('00000000-0000-0000-0000-000000000004','{}','fixture',session_user,1,1,'{}','{}');
        INSERT INTO openlegal_admin.provider_canary(id,operation_id,consumed,started_at,deadline,last_observed_at)
          VALUES('00000000-0000-0000-0000-000000000005','00000000-0000-0000-0000-000000000004',
          37,transaction_timestamp(),transaction_timestamp()+interval '6 hours',transaction_timestamp());
        UPDATE openlegal_admin.provider_canary_control
          SET required_id='00000000-0000-0000-0000-000000000005';"#)
        .execute(&mut admin).await.unwrap();

    let owner_contract: bool = sqlx::query_scalar(
        "SELECT count(*)=3 AND bool_and(NOT r.rolcanlogin AND NOT r.rolsuper
          AND NOT r.rolcreaterole AND NOT r.rolcreatedb AND NOT r.rolreplication AND NOT r.rolbypassrls)
          AND bool_and(NOT EXISTS(SELECT 1 FROM aclexplode(COALESCE(p.proacl,acldefault('f',p.proowner))) a
            WHERE a.grantee=0 AND a.privilege_type='EXECUTE'))
         FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace JOIN pg_roles r ON r.oid=p.proowner
         WHERE (n.nspname='openlegal_replication' AND p.proname='survivor_position'
           AND r.rolname='survivor_replication_owner') OR
          (n.nspname='openlegal_checkpoint' AND p.proname IN ('provider_survivor_state','survivor_snapshot')
           AND r.rolname='survivor_checkpoint_owner')",
    )
    .fetch_one(&mut admin)
    .await
    .unwrap();
    assert!(owner_contract);
    let source: Value = sqlx::query_scalar(
        "SELECT jsonb_build_object('retained_tables',openlegal_checkpoint.retained_tables(),
          'retained_references',openlegal_checkpoint.retained_references(),
          'storage_metadata',openlegal_checkpoint.storage_metadata(),
          'provider',jsonb_build_object('state',openlegal_checkpoint.state(),
            'hold_control',(SELECT to_jsonb(c) FROM openlegal_admin.provider_control c WHERE singleton),
            'lock_provenance','primary_advisory_locks_not_observable_on_standby'))",
    )
    .fetch_one(&mut admin)
    .await
    .unwrap();
    assert_eq!(
        source["retained_tables"]["public._sqlx_migrations"]["rows"],
        33
    );
    assert_eq!(source["retained_references"].as_array().unwrap().len(), 2);
    assert_eq!(source["provider"]["state"]["budget"]["daily_used"], 664);
    assert_eq!(
        source["provider"]["state"]["budget"]["unresolved_response"],
        true
    );
    assert_eq!(
        source["provider"]["state"]["admissions"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(source["provider"]["state"]["canaries"][0]["consumed"], 37);

    let mut primary_actor = actor(&pair, &pair.runtime_url).await;
    refused(
        &mut primary_actor,
        "SELECT openlegal_replication.survivor_position()",
        "55000",
    )
    .await;
    refused(
        &mut primary_actor,
        "SELECT openlegal_checkpoint.provider_survivor_state()",
        "55000",
    )
    .await;
    refused(
        &mut primary_actor,
        "SELECT openlegal_checkpoint.survivor_snapshot()",
        "25006",
    )
    .await;
    primary_actor.close().await.unwrap();

    let synchronous_commit: String = sqlx::query_scalar("SHOW synchronous_commit")
        .fetch_one(&mut admin)
        .await
        .unwrap();
    assert_eq!(synchronous_commit, "on");
    let prefix: Value = sqlx::query_scalar("SELECT openlegal_replication.capture_prefix()")
        .fetch_one(&mut admin)
        .await
        .unwrap();
    let acknowledged = lsn(&prefix["prefix_lsn"]);
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let peer: Value = sqlx::query_scalar("SELECT openlegal_replication.observe_flush()")
                .fetch_one(&mut admin)
                .await
                .unwrap();
            if lsn(&peer["flush_lsn"]) >= acknowledged {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("real synchronous peer flush through source prefix");
    let mut standby_admin = PgConnection::connect_with(&options(&pair, &pair.standby_admin_url))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let replay: Option<String> =
                sqlx::query_scalar("SELECT pg_last_wal_replay_lsn()::text")
                    .fetch_one(&mut standby_admin)
                    .await
                    .unwrap();
            if replay
                .as_ref()
                .is_some_and(|s| lsn(&json!(s)) >= acknowledged)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("actual standby replays owner/grant/data source prefix");
    standby_admin.close().await.unwrap();
    let mut reader = actor(&pair, &pair.standby_runtime_url).await;
    let initial = position(&mut reader).await;
    assert_eq!(initial["control"]["managed_epoch"], json!(pair.epoch));
    assert_eq!(initial["in_recovery"], true);
    assert!(initial["data_directory"].as_str().unwrap().starts_with('/'));
    for query in [
        "SELECT payload FROM openlegal.corpus_capture",
        "SELECT * FROM openlegal.provider_request_budget",
        "SELECT * FROM openlegal_replication.control",
        "SELECT * FROM public._sqlx_migrations",
        "UPDATE openlegal_admin.provider_control SET recovery_hold=false",
        "SET ROLE survivor_replication_owner",
        "SET ROLE survivor_checkpoint_owner",
        "SHOW data_directory",
        "SELECT pg_promote(false)",
        "SELECT pg_wal_replay_resume()",
    ] {
        refused(&mut reader, query, "42501").await;
    }
    refused(
        &mut reader,
        "SELECT openlegal_checkpoint.survivor_snapshot()",
        "25006",
    )
    .await;
    sqlx::query("SET statement_timeout='20ms'")
        .execute(&mut reader)
        .await
        .unwrap();
    refused(&mut reader, "SELECT pg_sleep(0.2)", "57014").await;
    sqlx::query("SET statement_timeout=0")
        .execute(&mut reader)
        .await
        .unwrap();

    admin.close().await.unwrap();
    let stopped = pair.stop_primary().await;
    assert_eq!(stopped["running"], false);
    assert_eq!(stopped["pid"], 0);
    assert_eq!(stopped["status"], "exited");
    assert_eq!(stopped["exit_code"], 0);
    let primary_connect = tokio::time::timeout(
        Duration::from_secs(2),
        PgConnection::connect_with(&primary_options),
    )
    .await;
    assert!(
        !matches!(primary_connect, Ok(Ok(_))),
        "stopped owned primary SQL stays unavailable"
    );

    // Select the actual durable receive end ONCE, only after receiver absence.
    // A post-fault restart/null position is a refusal, never synthesized zero.
    let target = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let observed = position(&mut reader).await;
            if observed["wal_receiver_count"] == 0 {
                break observed;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("old streaming channel closes after actual primary stop");
    assert!(lsn(&target["receive_lsn"]) >= acknowledged);
    assert_eq!(target["replay_paused"], false);
    for key in [
        "system_identifier",
        "timeline",
        "postmaster_started_at",
        "data_directory",
        "control",
    ] {
        assert_eq!(
            target[key], initial[key],
            "surviving same-lifetime identity: {key}"
        );
    }
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let observed = position(&mut reader).await;
            fixed_position(&observed, &target);
            if lsn(&observed["replay_lsn"]) >= lsn(&target["receive_lsn"]) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("actual replay through one fixed receive target");

    let sealed = snapshot(&mut reader).await;
    assert_eq!(
        sealed["transaction"],
        json!({"isolation":"repeatable read","read_only":true,
        "statement_timeout_ms":30000,"lock_timeout_ms":1000,"transaction_timeout_ms":35000,
        "idle_in_transaction_session_timeout_ms":5000})
    );
    let repeated = snapshot(&mut reader).await;
    for value in [&sealed, &repeated] {
        for key in ["position_before", "position_after"] {
            fixed_position(&value[key], &target);
            assert!(lsn(&value[key]["replay_lsn"]) >= lsn(&target["receive_lsn"]));
        }
    }
    for key in [
        "retained_tables",
        "retained_references",
        "storage_metadata",
        "provider",
    ] {
        assert_eq!(sealed[key], source[key], "acknowledged source state: {key}");
        assert_eq!(
            repeated[key], sealed[key],
            "same fixed-prefix readonly snapshot: {key}"
        );
    }
    let serialized = serde_json::to_string(&sealed).unwrap();
    let parsed_url = url::Url::parse(&pair.runtime_url).unwrap();
    for secret in [BODY, QUERY, SOURCE, parsed_url.password().unwrap()] {
        assert!(
            !serialized.contains(secret),
            "private body/query/source/credential absent from metadata"
        );
    }
    reader.close().await.unwrap();
    let mut closure_reader = actor(&pair, &pair.standby_runtime_url).await;
    let closure: Value = sqlx::query_scalar("SELECT openlegal_checkpoint.client_sessions()")
        .fetch_one(&mut closure_reader)
        .await
        .unwrap();
    assert_eq!(closure, json!({"sessions":[]}));
    closure_reader.close().await.unwrap();
}
