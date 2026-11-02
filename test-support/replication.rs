//! A separate, real TLS PG18 physical-replication pair for fault tests. Each pair
//! owns its entire two-cluster namespace; global settings never affect the normal
//! integration database or another test. Credentials stay in private /tmp files.
use openlegal_adapters::postgres::{PostgresOptions, PostgresStore, PostgresTls};
use std::{
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    time::Duration,
};
/// Test-only PG fence fixture sentinel. This is not a seeded BlobStore proof.
pub const FIXTURE_BLOB_BINDING_SHA256: &str =
    "1111111111111111111111111111111111111111111111111111111111111111";

pub struct TestReplication {
    pub admin_url: String,
    pub runtime_url: String,
    pub standby_admin_url: String,
    pub standby_runtime_url: String,
    pub epoch: uuid::Uuid,
    pub directory: tempfile::TempDir,
    pub primary: String,
    pub standby: String,
    ca_file: PathBuf,
}
fn script() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../test-support/replication-fixture.py")
}
impl TestReplication {
    pub async fn new() -> Self {
        let directory = tempfile::Builder::new()
            .prefix("openlegal-replication-test-")
            .tempdir_in("/tmp")
            .expect("private replication fixture directory");
        std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700))
            .expect("private replication fixture root mode");
        let mut this = Self {
            admin_url: String::new(),
            runtime_url: String::new(),
            standby_admin_url: String::new(),
            standby_runtime_url: String::new(),
            epoch: uuid::Uuid::nil(),
            primary: String::new(),
            standby: String::new(),
            ca_file: directory.path().join("ca.crt"),
            directory,
        };
        this.command("prepare").await;
        let value: serde_json::Value = serde_json::from_slice(
            &std::fs::read(this.directory.path().join("connection.json"))
                .expect("private connection record"),
        )
        .expect("connection record shape");
        this.admin_url = value["admin_url"]
            .as_str()
            .expect("admin connection field")
            .into();
        this.runtime_url = value["runtime_url"]
            .as_str()
            .expect("runtime connection field")
            .into();
        this.standby_admin_url = value["standby_admin_url"]
            .as_str()
            .expect("standby admin connection field")
            .into();
        this.standby_runtime_url = value["standby_runtime_url"]
            .as_str()
            .expect("standby runtime connection field")
            .into();
        this.primary = value["primary"]
            .as_str()
            .expect("primary identity field")
            .into();
        this.standby = value["standby"]
            .as_str()
            .expect("standby identity field")
            .into();
        PostgresStore::migrate(&this.admin_url, this.options())
            .await
            .expect("migrate disposable replication pair");
        this.command("qualify").await;
        let qualified: serde_json::Value = serde_json::from_slice(
            &std::fs::read(this.directory.path().join("qualified.json")).expect("qualified record"),
        )
        .expect("qualified record shape");
        this.epoch = qualified["epoch"]
            .as_str()
            .expect("qualified epoch")
            .parse()
            .expect("epoch syntax");
        this
    }
    pub fn options(&self) -> PostgresOptions {
        PostgresOptions {
            max_connections: 8,
            tls: PostgresTls::VerifyFull {
                ca_file: Some(self.ca_file.clone()),
            },
        }
    }
    async fn command(&self, phase: &str) {
        let output = tokio::time::timeout(
            Duration::from_secs(240),
            tokio::process::Command::new("python3")
                .arg(script())
                .arg(phase)
                .arg(self.directory.path())
                .kill_on_drop(true)
                .output(),
        )
        .await
        .expect("replication fixture phase deadline")
        .expect("replication fixture process");
        let stderr = String::from_utf8_lossy(&output.stderr);
        if !output.status.success() {
            use std::io::Write;
            let mut record =
                tempfile::NamedTempFile::new_in("/tmp").expect("private fixture failure record");
            record
                .write_all(output.stderr.get(..output.stderr.len().min(65536)).unwrap())
                .expect("fixture diagnostic write");
            let (_, path) = record.keep().expect("retain fixture diagnostic");
            eprintln!(
                "fixture launch diagnostic: {} source: {}",
                path.display(),
                script().display()
            );
            if let Ok(bytes) = std::fs::read(self.directory.path().join("failure-stderr.bin")) {
                let mut child =
                    tempfile::NamedTempFile::new_in("/tmp").expect("private child diagnostic");
                child
                    .write_all(&bytes[..bytes.len().min(65536)])
                    .expect("child diagnostic write");
                let (_, path) = child.keep().expect("retain child diagnostic");
                eprintln!("fixture child diagnostic: {}", path.display());
            }
        }
        let detail = stderr
            .lines()
            .find(|line| {
                line.starts_with("replication fixture failed: ")
                    && line.len() <= 256
                    && line
                        .bytes()
                        .all(|c| c.is_ascii_alphanumeric() || b" :_-/".contains(&c))
            })
            .unwrap_or("unclassified fixture failure");
        assert!(
            output.status.success(),
            "replication fixture phase failed: {phase}: {detail}"
        );
    }
    pub async fn pause_standby(&self) {
        self.docker("pause").await;
    }
    pub async fn resume_standby(&self) {
        self.docker("unpause").await;
    }
    pub async fn restart_primary(&self) {
        let output = tokio::time::timeout(
            Duration::from_secs(20),
            tokio::process::Command::new("docker")
                .args(["restart", "--time", "1", &self.primary])
                .kill_on_drop(true)
                .output(),
        )
        .await
        .expect("owned primary restart deadline")
        .expect("owned primary restart process");
        assert!(output.status.success(), "owned primary restart failed");
    }
    pub async fn stop_primary(&self) -> serde_json::Value {
        self.command("stop_primary").await;
        serde_json::from_slice(
            &std::fs::read(self.directory.path().join("primary-stopped.json"))
                .expect("owned primary stop readback"),
        )
        .expect("owned primary stop record shape")
    }
    async fn docker(&self, operation: &str) {
        let output = tokio::time::timeout(
            Duration::from_secs(15),
            tokio::process::Command::new("docker")
                .args([operation, &self.standby])
                .kill_on_drop(true)
                .output(),
        )
        .await
        .expect("fixture Docker deadline")
        .expect("fixture Docker process");
        assert!(output.status.success(), "fixture Docker operation failed");
    }
}
impl Drop for TestReplication {
    fn drop(&mut self) {
        // Failed startup also owns cleanup from the pre-effect state manifest.
        if self.directory.path().join("state.json").exists() {
            let status = std::process::Command::new("timeout")
                .args(["100", "python3"])
                .arg(script())
                .arg("cleanup")
                .arg(self.directory.path())
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .status();
            if !matches!(status, Ok(status) if status.success()) {
                eprintln!(
                    "replication fixture cleanup incomplete; inspect owned /tmp test namespace"
                );
                assert!(
                    std::thread::panicking(),
                    "owned replication fixture cleanup was not acknowledged and read back"
                );
            }
        }
    }
}
