//! Client/security boundary checks against the isolated PostgreSQL integration gate.
#[path = "../../../test-support/postgres.rs"]
mod support;

use openlegal_adapters::{
    blob::FsBlobStore,
    postgres::{PostgresOptions, PostgresStore, PostgresTls, StartupError, StartupMode},
};
use openlegal_application::{
    StoredPayload,
    blob::BlobStore,
    persistence::{
        HistoryKey, PersistentKey, PersistentStore, PublicationOutcome, PublicationRequest,
        RetentionPolicy, digest_hex,
    },
};
use openlegal_domain::{Provenance, Query, Record, RetrievalData, RetrievalError};
use sqlx::{Connection, PgConnection, postgres::PgPoolOptions};
use std::{path::Path, process::Stdio, sync::Arc, time::Duration};
use support::{TestDatabase, options};
use tokio_util::sync::CancellationToken;

fn token() -> CancellationToken {
    CancellationToken::new()
}

async fn open_at(
    fixture: &TestDatabase,
    url: &str,
    options: PostgresOptions,
) -> Result<Arc<PostgresStore>, StartupError> {
    let blobs = FsBlobStore::open(&fixture.directory.path().join("security-blobs"))
        .await
        .unwrap();
    let result = PostgresStore::open(
        url,
        options,
        blobs.clone(),
        RetentionPolicy::default(),
        100,
        StartupMode::Serve,
    )
    .await;
    if result.is_err() {
        let _ = blobs.close().await;
    }
    result
}

fn fixture_payload() -> (PersistentKey, Arc<StoredPayload>) {
    let key = PersistentKey {
        history: HistoryKey {
            namespace: "security_fixture".into(),
            provider: "synthetic".into(),
            dataset: "records".into(),
            query: Query::Get {
                source: "fixture".into(),
                id: "001".into(),
            },
        },
        processor_version: "v1".into(),
        schema_version: 1,
    };
    let raw = b"fictional role boundary evidence".to_vec();
    let value = Arc::new(StoredPayload {
        data: RetrievalData::Get(Record {
            source: "fixture".into(),
            id: "001".into(),
            title: "Fiction".into(),
            body: "Synthetic record".into(),
            synthetic: true,
        }),
        provenance: Provenance {
            provider: "synthetic".into(),
            dataset: "records".into(),
            source_reference: "https://example.test/fiction".into(),
            payload_sha256: digest_hex(&raw),
            processor_version: "v1".into(),
            retrieved_at: 100,
            validated_at: 100,
        },
        raw,
        bytes: 2048,
        snapshot: None,
    });
    (key, value)
}

#[tokio::test]
async fn unknown_dsn_parameters_are_rejected_without_exposing_secret_values() {
    for parameter in ["unexpected", "sslmode", "options", "application_name"] {
        let url = format!(
            "postgresql://synthetic_user:SENSITIVE_PASSWORD@127.0.0.1:1/synthetic?{parameter}=SENSITIVE_PARAMETER"
        );
        let error = PostgresStore::migrate(&url, options()).await.unwrap_err();
        assert!(matches!(
            error,
            StartupError::Storage(RetrievalError::InvalidInput)
        ));
        let visible = format!("{error} {error:?}");
        assert!(!visible.contains("SENSITIVE"));
        assert!(!visible.contains("postgresql://"));
    }
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh isolated PostgreSQL 18"]
async fn restricted_migration_and_runtime_roles_keep_ddl_out_of_serving() {
    let fixture = TestDatabase::new().await;
    let mut admin = PgConnection::connect(&fixture.url)
        .await
        .expect("isolated admin connection");
    sqlx::query("DROP SCHEMA openlegal CASCADE")
        .execute(&mut admin)
        .await
        .unwrap();
    sqlx::query("DROP TABLE public._sqlx_migrations")
        .execute(&mut admin)
        .await
        .unwrap();
    let mut secret = [0u8; 24];
    getrandom::fill(&mut secret).unwrap();
    let password: String = secret.iter().map(|b| format!("{b:02x}")).collect();
    // Utility statements cannot bind identifiers/password clauses. The fixed
    // role names and server-side %L quoting keep all credential values bound.
    sqlx::query("CREATE FUNCTION pg_temp.setup_security_roles(secret text) RETURNS void LANGUAGE plpgsql AS $$ BEGIN EXECUTE format('CREATE ROLE openlegal_security_migrator LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD %L', secret); EXECUTE format('CREATE ROLE openlegal_security_runtime LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD %L', secret); EXECUTE format('GRANT CREATE ON DATABASE %I TO openlegal_security_migrator', current_database()); END $$").execute(&mut admin).await.unwrap();
    sqlx::query("SELECT pg_temp.setup_security_roles($1)")
        .bind(&password)
        .execute(&mut admin)
        .await
        .unwrap();
    sqlx::query("GRANT CREATE ON SCHEMA public TO openlegal_security_migrator")
        .execute(&mut admin)
        .await
        .unwrap();
    let mut migration_url = url::Url::parse(&fixture.url).unwrap();
    migration_url
        .set_username("openlegal_security_migrator")
        .unwrap();
    migration_url.set_password(Some(&password)).unwrap();
    PostgresStore::migrate(migration_url.as_str(), options())
        .await
        .expect("restricted migration role");
    sqlx::query("GRANT USAGE ON SCHEMA openlegal TO openlegal_security_runtime")
        .execute(&mut admin)
        .await
        .unwrap();
    sqlx::query("GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA openlegal TO openlegal_security_runtime").execute(&mut admin).await.unwrap();
    sqlx::query("GRANT SELECT ON public._sqlx_migrations TO openlegal_security_runtime")
        .execute(&mut admin)
        .await
        .unwrap();
    let mut runtime_url = migration_url.clone();
    runtime_url
        .set_username("openlegal_security_runtime")
        .unwrap();
    let store = open_at(&fixture, runtime_url.as_str(), options())
        .await
        .expect("restricted runtime role");
    let (key, value) = fixture_payload();
    let expected = store
        .lookup(key.clone(), 100, token())
        .await
        .unwrap()
        .observation;
    let publication = store
        .publish(PublicationRequest {
            key: key.clone(),
            value,
            expected,
            now: 100,
            authorize: Arc::new(|| true),
            cancellation: token(),
        })
        .await
        .unwrap();
    assert!(matches!(publication, PublicationOutcome::Accepted(_)));
    assert!(
        store
            .lookup(key.clone(), 101, token())
            .await
            .unwrap()
            .value
            .is_some()
    );
    assert_eq!(
        store
            .list(key.history, None, 10, 101, token())
            .await
            .unwrap()
            .snapshots
            .len(),
        1
    );
    let mut runtime = PgConnection::connect(runtime_url.as_str())
        .await
        .expect("runtime SQL connection");
    for statement in [
        "CREATE TABLE openlegal.forbidden_ddl(id integer)",
        "ALTER TABLE openlegal.cache_query ADD COLUMN forbidden integer",
        "UPDATE public._sqlx_migrations SET success=false",
        "TRUNCATE openlegal.cache_head",
        "UPDATE openlegal.cache_snapshot SET source_reference='https://example.test/changed'",
    ] {
        assert!(sqlx::query(statement).execute(&mut runtime).await.is_err());
    }
    let privileges: (bool, bool, bool) = sqlx::query_as(
        "SELECT rolsuper,rolcreatedb,rolcreaterole FROM pg_roles WHERE rolname=current_user",
    )
    .fetch_one(&mut runtime)
    .await
    .unwrap();
    assert_eq!(privileges, (false, false, false));
    store.maintain(101).await.unwrap();
    store.close().await.unwrap();
    runtime.close().await.unwrap();
    admin.close().await.unwrap();
    // The gate destroys the complete cluster, including its synthetic test roles.
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh isolated PostgreSQL 18"]
async fn checksum_and_pending_migration_state_fail_startup_without_serving() {
    for checksum in [true, false] {
        let fixture = TestDatabase::new().await;
        let baseline = fixture.open(100).await;
        baseline.close().await.unwrap();
        let database = PgPoolOptions::new()
            .max_connections(1)
            .connect(&fixture.url)
            .await
            .unwrap();
        if checksum {
            sqlx::query("UPDATE public._sqlx_migrations SET checksum=$1")
                .bind(vec![0u8; 48])
                .execute(&database)
                .await
                .unwrap();
        } else {
            sqlx::query("UPDATE public._sqlx_migrations SET success=false")
                .execute(&database)
                .await
                .unwrap();
        }
        let result = open_at(&fixture, &fixture.url, options()).await;
        assert!(matches!(result, Err(StartupError::SchemaMismatch)));
        database.close().await;
    }
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh PostgreSQL 17 rejection fixture"]
async fn postgres_17_is_rejected_with_an_explicit_supported_version_error() {
    let url = std::env::var("OPENLEGAL_TEST_UNSUPPORTED_DATABASE_URL")
        .expect("the integration gate must provide its PostgreSQL 17 rejection fixture");
    let error = PostgresStore::migrate(&url, options()).await.unwrap_err();
    assert!(matches!(error, StartupError::PostgreSQL18Required));
    assert_eq!(
        error.to_string(),
        "persistent storage requires PostgreSQL 18.x"
    );
    assert!(!format!("{error:?}").contains(&url));
}

async fn command(program: &str, args: &[&str], directory: Option<&Path>) {
    let mut command = tokio::process::Command::new(program);
    command
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    if let Some(directory) = directory {
        command.current_dir(directory);
    }
    let output = tokio::time::timeout(Duration::from_secs(20), command.output())
        .await
        .expect("isolated certificate setup deadline")
        .expect("isolated setup process");
    assert!(
        output.status.success(),
        "isolated certificate setup command failed: {program}"
    );
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh isolated PostgreSQL 18, Docker, and OpenSSL"]
async fn tls_verifies_the_configured_ca_and_peer_hostname() {
    let fixture = TestDatabase::new().await;
    let container = std::env::var("OPENLEGAL_TEST_POSTGRES_CONTAINER").unwrap();
    let mut verified_url = url::Url::parse(&fixture.url).unwrap();
    let address = verified_url
        .host_str()
        .unwrap()
        .parse::<std::net::IpAddr>()
        .unwrap();
    let hostname = if address.is_loopback() {
        "localhost"
    } else {
        container.as_str()
    };
    assert!(
        hostname
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-')
    );
    verified_url.set_host(Some(hostname)).unwrap();
    let certificates = tempfile::tempdir().unwrap();
    command(
        "openssl",
        &[
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-days",
            "1",
            "-subj",
            "/CN=OpenLegal synthetic CA",
            "-keyout",
            "ca.key",
            "-out",
            "ca.pem",
        ],
        Some(certificates.path()),
    )
    .await;
    command(
        "openssl",
        &[
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-days",
            "1",
            "-subj",
            "/CN=OpenLegal unrelated CA",
            "-keyout",
            "wrong-ca.key",
            "-out",
            "wrong-ca.pem",
        ],
        Some(certificates.path()),
    )
    .await;
    let subject = format!("/CN={hostname}");
    command(
        "openssl",
        &[
            "req",
            "-new",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-subj",
            &subject,
            "-keyout",
            "server.key",
            "-out",
            "server.csr",
        ],
        Some(certificates.path()),
    )
    .await;
    tokio::fs::write(certificates.path().join("server.ext"), format!("subjectAltName=DNS:{hostname}\nextendedKeyUsage=serverAuth\nbasicConstraints=CA:FALSE\nkeyUsage=digitalSignature,keyEncipherment\n")).await.unwrap();
    command(
        "openssl",
        &[
            "x509",
            "-req",
            "-in",
            "server.csr",
            "-CA",
            "ca.pem",
            "-CAkey",
            "ca.key",
            "-CAcreateserial",
            "-days",
            "1",
            "-extfile",
            "server.ext",
            "-out",
            "server.pem",
        ],
        Some(certificates.path()),
    )
    .await;
    command(
        "docker",
        &[
            "exec",
            &container,
            "mkdir",
            "-p",
            "/tmp/openlegal-security-tls",
        ],
        None,
    )
    .await;
    for name in ["server.pem", "server.key"] {
        let source = certificates.path().join(name);
        let destination = format!("{container}:/tmp/openlegal-security-tls/{name}");
        command(
            "docker",
            &["cp", source.to_str().unwrap(), &destination],
            None,
        )
        .await;
    }
    command(
        "docker",
        &[
            "exec",
            &container,
            "chown",
            "-R",
            "postgres:postgres",
            "/tmp/openlegal-security-tls",
        ],
        None,
    )
    .await;
    command(
        "docker",
        &[
            "exec",
            &container,
            "chmod",
            "600",
            "/tmp/openlegal-security-tls/server.key",
        ],
        None,
    )
    .await;
    let mut admin = PgConnection::connect(&fixture.url).await.unwrap();
    sqlx::query("ALTER SYSTEM SET ssl_cert_file='/tmp/openlegal-security-tls/server.pem'")
        .execute(&mut admin)
        .await
        .unwrap();
    sqlx::query("ALTER SYSTEM SET ssl_key_file='/tmp/openlegal-security-tls/server.key'")
        .execute(&mut admin)
        .await
        .unwrap();
    sqlx::query("ALTER SYSTEM SET ssl='on'")
        .execute(&mut admin)
        .await
        .unwrap();
    sqlx::query("SELECT pg_reload_conf()")
        .execute(&mut admin)
        .await
        .unwrap();
    let tls_options = |file: &str| PostgresOptions {
        max_connections: 4,
        tls: PostgresTls::VerifyFull {
            ca_file: Some(certificates.path().join(file)),
        },
    };
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let store = loop {
        match open_at(&fixture, verified_url.as_str(), tls_options("ca.pem")).await {
            Ok(store) => break store,
            Err(_) if tokio::time::Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(100)).await
            }
            Err(error) => panic!("verified TLS did not become available: {error}"),
        }
    };
    store.health(100).await.unwrap();
    store.close().await.unwrap();
    assert!(
        open_at(&fixture, verified_url.as_str(), tls_options("wrong-ca.pem"))
            .await
            .is_err()
    );
    // Same server, port and CA; the IP address is absent from the certificate SAN.
    assert!(
        open_at(&fixture, &fixture.url, tls_options("ca.pem"))
            .await
            .is_err()
    );
    admin.close().await.unwrap();
}
