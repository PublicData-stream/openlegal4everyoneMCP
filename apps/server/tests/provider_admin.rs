//! Offline administration through the packaged CLI and disposable PostgreSQL.
#[path = "../../../test-support/postgres.rs"]
#[allow(dead_code)] // This CLI fixture intentionally never opens ordinary blob-backed storage.
mod postgres;
use serde_json::{Value, json};
use std::time::Duration;

async fn cli(fixture: &postgres::TestDatabase, config: &std::path::Path, args: &[&str]) -> Value {
    let output = tokio::time::timeout(
        Duration::from_secs(15),
        tokio::process::Command::new(env!("CARGO_BIN_EXE_openlegal-server"))
            .arg("--provider-admin")
            .args(args)
            .arg(config)
            .env(
                "OPENLEGAL_DATABASE_URL",
                "invalid-runtime-url-must-not-be-read",
            )
            .env(
                "OPENLEGAL_MIGRATION_DATABASE_URL",
                "invalid-migration-url-must-not-be-read",
            )
            .env("OPENLEGAL_PROVIDER_ADMIN_DATABASE_URL", &fixture.url)
            .kill_on_drop(true)
            .output(),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(
        output.status.success(),
        "sanitized CLI error: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).expect("stdout contains only bounded JSON")
}
#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh PostgreSQL 18 environment"]
async fn offline_cli_uses_only_admin_secret_and_retains_hold_until_explicit_resume() {
    let fixture = postgres::TestDatabase::new().await;
    let root = fixture.directory.path().join("never-opened-blobs");
    let occupied = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config: toml::Value =
        toml::from_str(include_str!("../../../deploy/demo/server.toml")).unwrap();
    config["http"]["bind"] = toml::Value::String(occupied.local_addr().unwrap().to_string());
    config["cache"]["blob"]["path"] = toml::Value::String(root.to_str().unwrap().into());
    config["demo"]["widget_html"] = toml::Value::String("missing-widget.html".into());
    config["text_diff"]["widget_html"] = toml::Value::String("missing-comparison.html".into());
    let config_path = fixture.directory.path().join("admin.toml");
    tokio::fs::write(&config_path, toml::to_string(&config).unwrap())
        .await
        .unwrap();
    let database = url::Url::parse(&fixture.url)
        .unwrap()
        .path()
        .trim_start_matches('/')
        .to_owned();
    let output = tokio::process::Command::new("docker")
        .args([
            "exec",
            &std::env::var("OPENLEGAL_TEST_POSTGRES_CONTAINER").unwrap(),
            "psql",
            "-U",
            "postgres",
            "-d",
            &database,
            "-v",
            "ON_ERROR_STOP=1",
            "-c",
            "UPDATE openlegal.provider_request_budget SET unresolved_response=true,admission_owner=pg_catalog.uuidv7() WHERE singleton; UPDATE openlegal_admin.provider_control SET legacy_identity=pg_catalog.uuidv7() WHERE singleton; INSERT INTO openlegal_admin.uncertainty_observation(identity,kind,owner) SELECT c.legacy_identity,'legacy',b.admission_owner FROM openlegal_admin.provider_control c CROSS JOIN openlegal.provider_request_budget b WHERE c.singleton AND b.singleton",
        ])
        .output()
        .await
        .unwrap();
    assert!(output.status.success());
    let inspection = cli(&fixture, &config_path, &["inspect"]).await;
    let spec = json!({"actor":"fixture-operator","reason":"synthetic offline review","quiescence":{"writers_stopped_at":inspection["observed_at"],"deployment_revision":"fixture-only","stopped_writer_ids":["fictional-collector"],"evidence_reference":"synthetic fixture; no provider client constructed"},"resolve_legacy":true,"owners":[],"resume":false,"waits":[]});
    let spec_path = fixture.directory.path().join("spec.json");
    let plan_path = fixture.directory.path().join("plan.json");
    tokio::fs::write(&spec_path, serde_json::to_vec(&spec).unwrap())
        .await
        .unwrap();
    let plan = cli(
        &fixture,
        &config_path,
        &["plan", spec_path.to_str().unwrap()],
    )
    .await;
    tokio::fs::write(&plan_path, serde_json::to_vec(&plan).unwrap())
        .await
        .unwrap();
    let applied = cli(
        &fixture,
        &config_path,
        &["apply", plan_path.to_str().unwrap()],
    )
    .await;
    assert_eq!(applied["recovery_hold"], true);
    assert_eq!(
        cli(
            &fixture,
            &config_path,
            &["readback", plan["operation_id"].as_str().unwrap()]
        )
        .await,
        applied
    );
    assert_eq!(
        cli(
            &fixture,
            &config_path,
            &["apply", plan_path.to_str().unwrap()]
        )
        .await,
        applied
    );
    let mut resume_spec = spec;
    resume_spec["resolve_legacy"] = json!(false);
    tokio::fs::write(&spec_path, serde_json::to_vec(&resume_spec).unwrap())
        .await
        .unwrap();
    let resume_plan = cli(
        &fixture,
        &config_path,
        &["plan", spec_path.to_str().unwrap(), "--resume"],
    )
    .await;
    tokio::fs::write(&plan_path, serde_json::to_vec(&resume_plan).unwrap())
        .await
        .unwrap();
    let resumed = cli(
        &fixture,
        &config_path,
        &["apply", plan_path.to_str().unwrap()],
    )
    .await;
    assert_eq!(resumed["recovery_hold"], false);
    assert!(
        !root.exists(),
        "admin commands must not initialize blob storage"
    );
}
