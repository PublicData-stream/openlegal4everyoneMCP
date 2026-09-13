//! Isolated PostgreSQL 18 test databases; invoked only by the explicit Docker gate.
use openlegal_adapters::{
    blob::FsBlobStore,
    postgres::{PostgresOptions, PostgresStore, PostgresTls, StartupMode},
};
use openlegal_application::persistence::RetentionPolicy;
use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

static NEXT_DATABASE: AtomicU64 = AtomicU64::new(1);

pub struct TestDatabase {
    pub url: String,
    pub directory: tempfile::TempDir,
    name: String,
    container: String,
}

pub fn options() -> PostgresOptions {
    PostgresOptions {
        max_connections: 4,
        tls: PostgresTls::Plaintext,
    }
}

impl TestDatabase {
    pub async fn new() -> Self {
        let container = std::env::var("OPENLEGAL_TEST_POSTGRES_CONTAINER")
            .expect("run scripts/test-postgres.sh to provision the PostgreSQL 18 environment");
        let base = std::env::var("OPENLEGAL_TEST_DATABASE_URL")
            .expect("PostgreSQL test URL must be set by the explicit integration gate");
        let name = format!(
            "openlegal_test_{}_{}",
            std::process::id(),
            NEXT_DATABASE.fetch_add(1, Ordering::Relaxed)
        );
        let created = tokio::time::timeout(
            Duration::from_secs(15),
            tokio::process::Command::new("docker")
                .args(["exec", &container, "createdb", "-U", "postgres", &name])
                .kill_on_drop(true)
                .output(),
        )
        .await
        .expect("create test database deadline")
        .expect("create test database process");
        assert!(
            created.status.success(),
            "cannot create isolated test database"
        );
        let mut url = url::Url::parse(&base).expect("test URL syntax");
        url.set_path(&name);
        let fixture = Self {
            url: url.into(),
            directory: tempfile::tempdir().unwrap(),
            name,
            container,
        };
        PostgresStore::migrate(&fixture.url, options())
            .await
            .expect("migrate PostgreSQL 18 test database");
        fixture
    }

    pub async fn open(&self, now: u64) -> Arc<PostgresStore> {
        let blobs = FsBlobStore::open(&self.directory.path().join("blobs"))
            .await
            .unwrap();
        PostgresStore::open(
            &self.url,
            options(),
            blobs,
            RetentionPolicy::default(),
            now,
            StartupMode::Serve,
        )
        .await
        .expect("open PostgreSQL 18 test store")
    }
}

impl Drop for TestDatabase {
    fn drop(&mut self) {
        // Test-only fallback also runs after panic; the gate removes the entire container.
        let _ = std::process::Command::new("timeout")
            .args([
                "15",
                "docker",
                "exec",
                &self.container,
                "dropdb",
                "--force",
                "-U",
                "postgres",
                &self.name,
            ])
            .output();
    }
}
