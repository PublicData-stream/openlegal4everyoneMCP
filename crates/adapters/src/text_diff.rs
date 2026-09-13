//! Isolated Git subprocess mechanics. Supplied text never becomes an argument or path.
use futures::{FutureExt, future::BoxFuture};
use openlegal_application::text_diff::{
    DiffEngine, HandleGenerator, MAX_PATCH_BYTES, MAX_STDERR_BYTES,
};
use openlegal_domain::text_diff::TextDiffError;
use std::{
    path::{Path, PathBuf},
    process::{ExitStatus, Stdio},
    sync::Arc,
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    process::Command,
    time::Instant,
};
use tokio_util::sync::CancellationToken;

pub struct OsHandleGenerator;
impl HandleGenerator for OsHandleGenerator {
    fn generate(&self) -> Result<[u8; 32], TextDiffError> {
        let mut value = [0; 32];
        getrandom::fill(&mut value).map_err(|_| TextDiffError::Unavailable)?;
        Ok(value)
    }
}

#[derive(Clone)]
pub struct GitDiffEngine {
    executable: PathBuf,
}
impl GitDiffEngine {
    /// Validate the operator-selected executable before listeners admit calls.
    pub async fn new(git_path: &Path) -> Result<Self, TextDiffError> {
        if !git_path.is_absolute() {
            return Err(TextDiffError::InvalidInput);
        }
        let executable = tokio::fs::canonicalize(git_path)
            .await
            .map_err(|_| TextDiffError::Unavailable)?;
        if !tokio::fs::metadata(&executable)
            .await
            .map_err(|_| TextDiffError::Unavailable)?
            .is_file()
        {
            return Err(TextDiffError::Unavailable);
        }
        let engine = Self { executable };
        let temp = tempfile::tempdir().map_err(|_| TextDiffError::Unavailable)?;
        let mut command = engine.command(temp.path());
        command.arg("--version");
        let (status, bytes) = execute(
            command,
            CancellationToken::new(),
            Instant::now() + Duration::from_secs(5),
            4096,
        )
        .await?;
        temp.close().map_err(|_| TextDiffError::Internal)?;
        if !status.success() || !bytes.starts_with(b"git version ") {
            return Err(TextDiffError::Unavailable);
        }
        Ok(engine)
    }
    fn command(&self, directory: &Path) -> Command {
        let mut command = Command::new(&self.executable);
        command
            .current_dir(directory)
            .env_clear()
            .env("HOME", directory)
            .env("XDG_CONFIG_HOME", directory)
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_ATTR_NOSYSTEM", "1")
            .env("LC_ALL", "C")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        command
    }
}
impl DiffEngine for GitDiffEngine {
    fn diff(
        &self,
        before: Arc<str>,
        after: Arc<str>,
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> BoxFuture<'static, Result<String, TextDiffError>> {
        let engine = self.clone();
        async move {
            if cancellation.is_cancelled() || Instant::now() >= deadline {
                return Err(TextDiffError::Cancelled);
            }
            let temp = tempfile::tempdir().map_err(|_| TextDiffError::Unavailable)?;
            let operation = async {
                tokio::fs::write(temp.path().join("before"), before.as_bytes())
                    .await
                    .map_err(|_| TextDiffError::Unavailable)?;
                tokio::fs::write(temp.path().join("after"), after.as_bytes())
                    .await
                    .map_err(|_| TextDiffError::Unavailable)?;
                if cancellation.is_cancelled() || Instant::now() >= deadline {
                    return Err(TextDiffError::Cancelled);
                }
                let mut command = engine.command(temp.path());
                command.args([
                    "--no-pager",
                    "-c",
                    "core.attributesFile=/dev/null",
                    "-c",
                    "core.quotePath=false",
                    "diff",
                    "--no-index",
                    "--text",
                    "--no-ext-diff",
                    "--no-textconv",
                    "--no-color",
                    "--no-renames",
                    "--diff-algorithm=myers",
                    "--no-indent-heuristic",
                    "--unified=3",
                    "--no-prefix",
                    "--",
                    "before",
                    "after",
                ]);
                let (status, patch) =
                    execute(command, cancellation, deadline, MAX_PATCH_BYTES).await?;
                if !matches!(status.code(), Some(0 | 1)) {
                    return Err(TextDiffError::Unavailable);
                }
                String::from_utf8(patch).map_err(|_| TextDiffError::Internal)
            }
            .await;
            // Removal is observed on every ordinary failure path after the process is reaped.
            let cleanup = temp.close().map_err(|_| TextDiffError::Internal);
            cleanup?;
            operation
        }
        .boxed()
    }
}

async fn capped_read(reader: impl AsyncRead + Unpin, cap: usize) -> Result<Vec<u8>, TextDiffError> {
    let mut bytes = Vec::new();
    reader
        .take(cap as u64 + 1)
        .read_to_end(&mut bytes)
        .await
        .map_err(|_| TextDiffError::Unavailable)?;
    if bytes.len() > cap {
        Err(TextDiffError::ResourceLimit)
    } else {
        Ok(bytes)
    }
}

async fn execute(
    mut command: Command,
    cancellation: CancellationToken,
    deadline: Instant,
    stdout_limit: usize,
) -> Result<(ExitStatus, Vec<u8>), TextDiffError> {
    let mut child = command.spawn().map_err(|_| TextDiffError::Unavailable)?;
    let stdout = child.stdout.take().ok_or(TextDiffError::Internal)?;
    let stderr = child.stderr.take().ok_or(TextDiffError::Internal)?;
    let result = tokio::select! {
        biased;
        _ = cancellation.cancelled() => Err(TextDiffError::Cancelled),
        _ = tokio::time::sleep_until(deadline) => Err(TextDiffError::Cancelled),
        result = async { tokio::try_join!(capped_read(stdout, stdout_limit), capped_read(stderr, MAX_STDERR_BYTES),
            async { child.wait().await.map_err(|_| TextDiffError::Unavailable) }) } => {
            result.map(|(stdout, _, status)| (status, stdout))
        }
    };
    if result.is_err() {
        let _ = child.start_kill();
        child.wait().await.map_err(|_| TextDiffError::Internal)?;
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use openlegal_application::text_diff::TextDiffService;
    use openlegal_domain::text_diff::{CompareInput, PageRequest, PageView};
    #[tokio::test]
    async fn real_git_preserves_unicode_crlf_and_final_newline() {
        let engine = GitDiffEngine::new(Path::new("/usr/bin/git")).await.unwrap();
        let service = TextDiffService::new(Arc::new(engine), Arc::new(OsHandleGenerator));
        let summary = service
            .compare(
                CompareInput {
                    before: "한글\r\n끝".into(),
                    after: "한글\n끝\n".into(),
                    before_label: None,
                    after_label: None,
                },
                CancellationToken::new(),
                Instant::now() + Duration::from_secs(10),
            )
            .await
            .unwrap();
        assert_eq!(summary.before.crlf, 1);
        assert_eq!(summary.after.lf, 2);
        assert!(!summary.before.final_newline);
        assert!(summary.after.final_newline);
        let page = service
            .page(PageRequest {
                comparison_id: summary.comparison_id.clone(),
                view: PageView::Changes,
                page: 0,
            })
            .unwrap();
        assert!(page.fragments[0].patch.contains("-한글\r\n"));
        assert!(
            page.fragments[0]
                .patch
                .contains("\\ No newline at end of file")
        );
        service.delete(&summary.comparison_id).unwrap();
        assert_eq!(
            service.summary(&summary.comparison_id).unwrap_err(),
            TextDiffError::NotFound
        );
        let stop = CancellationToken::new();
        stop.cancel();
        service.run(stop).await.unwrap();
    }
    #[tokio::test]
    async fn missing_executable_and_randomness_are_checked() {
        assert!(
            GitDiffEngine::new(Path::new("/nonexistent/openlegal-git"))
                .await
                .is_err()
        );
        assert_ne!(
            OsHandleGenerator.generate().unwrap(),
            OsHandleGenerator.generate().unwrap()
        );
    }
    #[tokio::test]
    async fn maximum_texts_and_heavily_escaped_lines_have_bounded_pages() {
        let engine = GitDiffEngine::new(Path::new("/usr/bin/git")).await.unwrap();
        let service = TextDiffService::new(Arc::new(engine), Arc::new(OsHandleGenerator));
        let before = format!("{}\n", "x".repeat(16383)).repeat(64);
        let after = format!("{}\n", "\u{0001}".repeat(16383)).repeat(64);
        assert_eq!(before.len(), 1024 * 1024);
        let summary = service
            .compare(
                CompareInput {
                    before,
                    after: after.clone(),
                    before_label: None,
                    after_label: None,
                },
                CancellationToken::new(),
                Instant::now() + Duration::from_secs(10),
            )
            .await
            .unwrap();
        assert_eq!((summary.additions, summary.deletions), (64, 64));
        assert!(summary.change_pages > 1);
        for page in 0..summary.change_pages {
            let page = service
                .page(PageRequest {
                    comparison_id: summary.comparison_id.clone(),
                    view: PageView::Changes,
                    page,
                })
                .unwrap();
            assert!(serde_json::to_vec(&page).unwrap().len() <= 256 * 1024);
        }
        let mut restored = String::new();
        for page in 0..32 {
            let page = service
                .page(PageRequest {
                    comparison_id: summary.comparison_id.clone(),
                    view: PageView::After,
                    page,
                })
                .unwrap();
            assert_eq!(page.total_pages, 32);
            restored.push_str(page.text.as_deref().unwrap());
        }
        assert_eq!(restored, after);
        let stop = CancellationToken::new();
        stop.cancel();
        service.run(stop).await.unwrap();
    }
    #[cfg(unix)]
    fn fake_git(directory: &Path, command: &str) -> PathBuf {
        use std::os::unix::fs::PermissionsExt;
        let executable = directory.join("git-fixture");
        let script = format!(
            "#!/bin/sh\nif [ \"$1\" = --version ]; then echo 'git version fixture'; exit 0; fi\necho $$ > '{}'/pid\npwd > '{}'/working\nexec {command}\n",
            directory.display(),
            directory.display()
        );
        std::fs::write(&executable, script).unwrap();
        std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o700)).unwrap();
        executable
    }
    #[cfg(unix)]
    #[tokio::test]
    async fn cancellation_reaps_process_and_removes_private_inputs() {
        let fixture = tempfile::tempdir().unwrap();
        let engine = GitDiffEngine::new(&fake_git(fixture.path(), "/bin/sleep 60"))
            .await
            .unwrap();
        let cancellation = CancellationToken::new();
        let token = cancellation.clone();
        let task = tokio::spawn(async move {
            engine
                .diff(
                    Arc::from("old"),
                    Arc::from("new"),
                    token,
                    Instant::now() + Duration::from_secs(10),
                )
                .await
        });
        tokio::time::timeout(Duration::from_secs(5), async {
            while !fixture.path().join("working").exists() {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        let pid = std::fs::read_to_string(fixture.path().join("pid")).unwrap();
        let working = std::fs::read_to_string(fixture.path().join("working")).unwrap();
        cancellation.cancel();
        assert_eq!(task.await.unwrap(), Err(TextDiffError::Cancelled));
        assert!(!Path::new(working.trim()).exists());
        assert!(!Path::new("/proc").join(pid.trim()).exists());
    }
    #[cfg(unix)]
    #[tokio::test]
    async fn stdout_and_stderr_overflow_reap_and_cleanup() {
        for command in ["/usr/bin/yes x", "/usr/bin/yes x >&2"] {
            let fixture = tempfile::tempdir().unwrap();
            let engine = GitDiffEngine::new(&fake_git(fixture.path(), command))
                .await
                .unwrap();
            let result = engine
                .diff(
                    Arc::from("old"),
                    Arc::from("new"),
                    CancellationToken::new(),
                    Instant::now() + Duration::from_secs(10),
                )
                .await;
            assert_eq!(result, Err(TextDiffError::ResourceLimit));
            let pid = std::fs::read_to_string(fixture.path().join("pid")).unwrap();
            let working = std::fs::read_to_string(fixture.path().join("working")).unwrap();
            assert!(!Path::new(working.trim()).exists());
            assert!(!Path::new("/proc").join(pid.trim()).exists());
        }
    }
}
