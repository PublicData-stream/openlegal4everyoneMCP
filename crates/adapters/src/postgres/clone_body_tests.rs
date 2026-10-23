//! Upgrade and immutable-evidence coverage for the stored clone-body projection.
use super::*;
use crate::test_support as fixture;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use sqlx::Connection;

async fn schema_25() -> (fixture::TestDatabase, PgPool) {
    let fixture = fixture::TestDatabase::new().await;
    let pool = pool(&fixture.url, fixture::options(), Duration::from_secs(15))
        .await
        .unwrap();
    // TestDatabase normally installs the current schema. Reinstall the actual
    // preceding catalog in this disposable database rather than fabricating an
    // applied migration or changing any checked-in SQL/checksum.
    sqlx::raw_sql("DROP SCHEMA openlegal_admin CASCADE; DROP SCHEMA openlegal CASCADE; DROP TABLE public._sqlx_migrations")
        .execute(&pool)
        .await
        .unwrap();
    let mut previous = Migrator::with_migrations(
        migrator()
            .iter()
            .filter(|m| m.version <= 25)
            .cloned()
            .collect(),
    );
    previous.dangerous_set_table_name("public._sqlx_migrations");
    previous.run(&pool).await.unwrap();
    assert!(matches!(
        verify_schema(&pool).await,
        Err(Error::StorageCorrupt)
    ));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT max(version) FROM public._sqlx_migrations")
            .fetch_one(&pool)
            .await
            .unwrap(),
        25
    );
    (fixture, pool)
}

async fn insert_capture(pool: &PgPool, sequence: i64, payload: &Value) {
    let digest = Sha256::digest(serde_json::to_vec(payload).unwrap()).to_vec();
    sqlx::query("INSERT INTO openlegal.corpus_capture(id,object_key,revision_id,sequence,captured_at,event_sequence,payload,payload_sha256,raw_sha256,raw_size,storage_key) VALUES($1,repeat('a',64),$2,$3,100,$3,$4,$5,$5,17,$6)")
        .bind(format!("{sequence:064x}"))
        .bind(format!("synthetic-revision-{sequence}"))
        .bind(sequence)
        .bind(payload)
        .bind(digest)
        .bind(format!("synthetic-clone-body:{sequence}"))
        .execute(pool)
        .await
        .unwrap();
}

async fn retained_state(pool: &PgPool) -> Value {
    sqlx::query_scalar("SELECT jsonb_build_object('captures',(SELECT jsonb_agg(to_jsonb(c)-'clone_body_complete' ORDER BY id) FROM openlegal.corpus_capture c),'catalog',(SELECT jsonb_agg(to_jsonb(c) ORDER BY id) FROM openlegal.corpus_capture_catalog c),'revisions',(SELECT jsonb_agg(to_jsonb(r) ORDER BY revision_id) FROM openlegal.corpus_revision r),'outbox',(SELECT jsonb_agg(to_jsonb(o) ORDER BY sequence) FROM openlegal.corpus_outbox o),'control',(SELECT to_jsonb(c) FROM openlegal.corpus_control c WHERE singleton))")
        .fetch_one(pool)
        .await
        .unwrap()
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh isolated PostgreSQL 18"]
async fn clone_body_projection_upgrades_existing_archive_without_changing_evidence() {
    let (fixture, pool) = schema_25().await;
    sqlx::query(
        "INSERT INTO openlegal.corpus_object(object_key,identity) VALUES(repeat('a',64),'{}')",
    )
    .execute(&pool)
    .await
    .unwrap();
    // Synthetic fixtures include malformed/absent containers because the old
    // SQL treats their missing flags permissively. This is not provider evidence.
    let cases = [
        (json!({}), true),
        (json!({"record": null}), true),
        (json!({"record": "scalar"}), true),
        (json!({"record": []}), true),
        (json!({"record": {}}), true),
        (json!({"record": {"metadata": null}}), true),
        (json!({"record": {"metadata": "scalar"}}), true),
        (json!({"record": {"metadata": []}}), true),
        (
            json!({"record": {"metadata": {"attachment_status": null, "body_status": null}}}),
            true,
        ),
        (
            json!({"record": {"metadata": {"attachment_status": "complete", "body_status": "complete"}}}),
            true,
        ),
        (
            json!({"record": {"metadata": {"attachment_status": "unknown", "body_status": "unknown"}}}),
            true,
        ),
        (
            json!({"record": {"metadata": {"attachment_status": false, "body_status": 7}}}),
            true,
        ),
        (
            json!({"record": {"metadata": {"attachment_status": "incomplete"}}}),
            false,
        ),
        (
            json!({"record": {"metadata": {"body_status": "response_identity_unverified_metadata_only"}}}),
            false,
        ),
        (
            json!({"record": {"metadata": {"attachment_status": "incomplete", "body_status": "response_identity_unverified_metadata_only"}}}),
            false,
        ),
        (
            json!({"record": {"metadata": {"attachment_status": "response_identity_unverified_metadata_only", "body_status": "incomplete"}}}),
            true,
        ),
    ];
    for (index, (payload, _)) in cases.iter().enumerate() {
        insert_capture(&pool, index as i64 + 1, payload).await;
    }
    let body: String = (0_u64..2048)
        .flat_map(|n| Sha256::digest(n.to_be_bytes()))
        .map(|byte| format!("{byte:02x}"))
        .collect();
    let large = json!({"record": {"body": body, "metadata": {"attachment_status": "complete", "body_status": "complete"}}});
    insert_capture(&pool, 100, &large).await;
    sqlx::raw_sql("INSERT INTO openlegal.corpus_revision(object_key,revision_id,latest_capture,last_sequence,captured_at) SELECT object_key,revision_id,id,sequence,captured_at FROM openlegal.corpus_capture; INSERT INTO openlegal.corpus_capture_catalog(id,object_key,revision_id,sequence,captured_at,payload,payload_sha256) SELECT id,object_key,revision_id,sequence,captured_at,payload,payload_sha256 FROM openlegal.corpus_capture; INSERT INTO openlegal.corpus_outbox(sequence,object_key,object_version,capture_id,withdrawn,is_head) SELECT event_sequence,object_key,1,id,false,false FROM openlegal.corpus_capture; UPDATE openlegal.corpus_control SET next_event=101,index_ack=100,raw_bytes=289,historical_bytes=289")
        .execute(&pool)
        .await
        .unwrap();
    let before = retained_state(&pool).await;
    PostgresStore::migrate(&fixture.url, fixture::options())
        .await
        .unwrap();
    verify_schema(&pool).await.unwrap();
    assert_eq!(retained_state(&pool).await, before);
    let rows: Vec<(i64, bool, bool)> = sqlx::query_as("SELECT sequence,clone_body_complete,COALESCE(payload->'record'->'metadata'->>'attachment_status','') <> 'incomplete' AND COALESCE(payload->'record'->'metadata'->>'body_status','') <> 'response_identity_unverified_metadata_only' FROM openlegal.corpus_capture ORDER BY sequence")
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(rows.len(), cases.len() + 1);
    for ((_, actual, old_expression), (_, expected)) in rows.iter().zip(&cases) {
        assert_eq!(actual, expected);
        assert_eq!(actual, old_expression);
    }
    assert_eq!(rows.last(), Some(&(100, true, true)));
    // A second migration invocation must be a no-op on the retained archive.
    PostgresStore::migrate(&fixture.url, fixture::options())
        .await
        .unwrap();
    assert_eq!(retained_state(&pool).await, before);

    insert_capture(
        &pool,
        101,
        &json!({"record": {"metadata": {"attachment_status": "incomplete"}}}),
    )
    .await;
    let generated: bool = sqlx::query_scalar(
        "SELECT clone_body_complete FROM openlegal.corpus_capture WHERE sequence=101",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(!generated);
    for (statement, expected_code) in [
        (
            "INSERT INTO openlegal.corpus_capture(id,object_key,revision_id,sequence,captured_at,event_sequence,payload,payload_sha256,raw_sha256,raw_size,storage_key,clone_body_complete) VALUES(repeat('f',64),repeat('a',64),'override',102,100,102,'{}',decode(repeat('00',32),'hex'),decode(repeat('00',32),'hex'),0,'override',true)",
            "428C9",
        ),
        (
            "UPDATE openlegal.corpus_capture SET payload='{}' WHERE sequence=101",
            "23514",
        ),
        (
            "UPDATE openlegal.corpus_capture SET clone_body_complete=DEFAULT WHERE sequence=101",
            "23514",
        ),
    ] {
        let error = sqlx::query(statement).execute(&pool).await.unwrap_err();
        assert_eq!(
            error.as_database_error().and_then(|e| e.code()).as_deref(),
            Some(expected_code)
        );
    }
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM openlegal.corpus_capture")
            .fetch_one(&pool)
            .await
            .unwrap(),
        cases.len() as i64 + 2
    );
    pool.close().await;
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh isolated PostgreSQL 18"]
async fn clone_body_projection_is_automatic_with_restricted_runtime_privileges() {
    let fixture = fixture::TestDatabase::new().await;
    let pool = pool(&fixture.url, fixture::options(), Duration::from_secs(15))
        .await
        .unwrap();
    let mut connection = pool.acquire().await.unwrap();
    // This fixed test-only NOLOGIN role has DML privileges, never schema
    // ownership or DDL permission. The gate destroys its disposable cluster.
    sqlx::raw_sql("CREATE ROLE openlegal_clone_body_runtime NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE; GRANT USAGE ON SCHEMA openlegal TO openlegal_clone_body_runtime; GRANT SELECT,INSERT,UPDATE ON openlegal.corpus_object,openlegal.corpus_capture TO openlegal_clone_body_runtime; SET ROLE openlegal_clone_body_runtime")
        .execute(&mut *connection).await.unwrap();
    sqlx::query(
        "INSERT INTO openlegal.corpus_object(object_key,identity) VALUES(repeat('a',64),'{}')",
    )
    .execute(&mut *connection)
    .await
    .unwrap();
    sqlx::query("INSERT INTO openlegal.corpus_capture(id,object_key,revision_id,sequence,captured_at,event_sequence,payload,payload_sha256,raw_sha256,raw_size,storage_key) VALUES(repeat('b',64),repeat('a',64),'runtime',1,100,1,$1,decode(repeat('00',32),'hex'),decode(repeat('00',32),'hex'),0,'restricted-runtime')")
        .bind(json!({"record": {"metadata": {"body_status": "response_identity_unverified_metadata_only"}}}))
        .execute(&mut *connection).await.unwrap();
    assert!(
        !sqlx::query_scalar::<_, bool>("SELECT clone_body_complete FROM openlegal.corpus_capture")
            .fetch_one(&mut *connection)
            .await
            .unwrap()
    );
    for (statement, expected_code) in [
        (
            "ALTER TABLE openlegal.corpus_capture ADD COLUMN forbidden integer",
            "42501",
        ),
        ("UPDATE openlegal.corpus_capture SET payload='{}'", "23514"),
        (
            "UPDATE openlegal.corpus_capture SET clone_body_complete=true",
            "428C9",
        ),
    ] {
        let error = sqlx::query(statement)
            .execute(&mut *connection)
            .await
            .unwrap_err();
        assert_eq!(
            error.as_database_error().and_then(|e| e.code()).as_deref(),
            Some(expected_code)
        );
    }
    sqlx::raw_sql("RESET ROLE; DROP OWNED BY openlegal_clone_body_runtime; DROP ROLE openlegal_clone_body_runtime")
        .execute(&mut *connection).await.unwrap();
    drop(connection);
    pool.close().await;
}

async fn timeout_settings(connection: &mut PgConnection) -> (String, String, String) {
    sqlx::query_as("SELECT current_setting('statement_timeout'),current_setting('transaction_timeout'),current_setting('lock_timeout')")
        .fetch_one(connection)
        .await
        .unwrap()
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh isolated PostgreSQL 18"]
async fn clone_body_migration_rearms_transaction_deadline_and_restores_session_limits() {
    let fixture = fixture::TestDatabase::new().await;
    let pool = pool(&fixture.url, fixture::options(), Duration::from_secs(15))
        .await
        .unwrap();
    let mut connection = pool.acquire().await.unwrap();
    let expected = ("2s".into(), "5s".into(), "1s".into());
    assert_eq!(timeout_settings(&mut connection).await, expected);
    let migrations = migrator();
    let migration = migrations.iter().find(|m| m.version == 26).unwrap();
    let (prefix, _) = migration.sql.as_str().split_once("\nALTER TABLE ").unwrap();
    for commit in [true, false] {
        let mut tx = connection.begin().await.unwrap();
        // This slice is only the compiled migration's literal preamble. It
        // contains no fixture values, identifiers, credentials, or caller input.
        sqlx::raw_sql(sqlx::AssertSqlSafe(prefix))
            .execute(&mut *tx)
            .await
            .unwrap();
        assert_eq!(
            timeout_settings(&mut tx).await,
            ("30min".into(), "35min".into(), "1s".into())
        );
        // Changing the active timer directly from 5s to 35min does not rearm
        // PostgreSQL 18's original timer. This exceeds that original deadline.
        sqlx::query("SELECT pg_sleep(6)")
            .execute(&mut *tx)
            .await
            .unwrap();
        if commit {
            tx.commit().await.unwrap();
        } else {
            tx.rollback().await.unwrap();
        }
        assert_eq!(timeout_settings(&mut connection).await, expected);
    }
    drop(connection);
    pool.close().await;
}
