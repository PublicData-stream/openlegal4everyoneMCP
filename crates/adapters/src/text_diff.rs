//! Killable, in-memory Rust comparison workers. Supplied text travels only in pipes.
use futures::{FutureExt, future::BoxFuture};
use openlegal_application::text_diff::ComputedDiff;
use openlegal_application::text_diff::{DiffEngine, HandleGenerator, MAX_STDERR_BYTES};
use openlegal_domain::text_diff::TextDiffError;
use std::{
    path::{Path, PathBuf},
    process::Stdio,
    sync::Arc,
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    process::Command,
    time::Instant,
};
use tokio_util::sync::CancellationToken;

mod compute;
mod protocol;

/// Run one private worker exchange before initializing the server runtime.
/// Errors are returned without exposing source text or private diagnostics.
pub fn run_worker() -> Result<(), TextDiffError> {
    protocol::run(std::io::stdin().lock(), std::io::stdout().lock())
}

pub struct OsHandleGenerator;
impl HandleGenerator for OsHandleGenerator {
    fn generate(&self) -> Result<[u8; 32], TextDiffError> {
        let mut value = [0; 32];
        getrandom::fill(&mut value).map_err(|_| TextDiffError::Unavailable)?;
        Ok(value)
    }
}

#[derive(Clone)]
pub struct SimilarDiffEngine {
    executable: PathBuf,
}
impl SimilarDiffEngine {
    /// Probe the actual server executable before listeners admit comparisons.
    pub async fn new(path: &Path) -> Result<Self, TextDiffError> {
        if !path.is_absolute() {
            return Err(TextDiffError::InvalidInput);
        }
        let executable = tokio::fs::canonicalize(path)
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
        let result = engine
            .diff(
                Arc::from(""),
                Arc::from(""),
                CancellationToken::new(),
                Instant::now() + Duration::from_secs(5),
            )
            .await?;
        if !result.patch.is_empty() || !result.inline_changes.is_empty() {
            return Err(TextDiffError::Unavailable);
        }
        Ok(engine)
    }
    fn command(&self) -> Command {
        let mut command = Command::new(&self.executable);
        command
            .arg("--text-diff-worker")
            .env_clear()
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        command
    }
}
impl DiffEngine for SimilarDiffEngine {
    fn diff(
        &self,
        before: Arc<str>,
        after: Arc<str>,
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> BoxFuture<'static, Result<ComputedDiff, TextDiffError>> {
        let engine = self.clone();
        async move {
            if cancellation.is_cancelled() || Instant::now() >= deadline {
                return Err(TextDiffError::Cancelled);
            }
            openlegal_application::text_diff::text_info(&before, "Before")?;
            openlegal_application::text_diff::text_info(&after, "After")?;
            execute(engine.command(), before, after, cancellation, deadline).await
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
    before: Arc<str>,
    after: Arc<str>,
    cancellation: CancellationToken,
    deadline: Instant,
) -> Result<ComputedDiff, TextDiffError> {
    let mut child = command.spawn().map_err(|_| TextDiffError::Unavailable)?;
    // All three handles are guaranteed by command construction.
    let mut stdin = child.stdin.take().ok_or(TextDiffError::Internal)?;
    let stdout = child.stdout.take().ok_or(TextDiffError::Internal)?;
    let stderr = child.stderr.take().ok_or(TextDiffError::Internal)?;
    let result = tokio::select! {
        biased;
        _ = cancellation.cancelled() => Err(TextDiffError::Cancelled),
        _ = tokio::time::sleep_until(deadline) => Err(TextDiffError::Cancelled),
        result = async { tokio::try_join!(
            async {
                let header = protocol::request_header(before.len(), after.len())?;
                stdin.write_all(&header).await.map_err(|_| TextDiffError::Unavailable)?;
                stdin.write_all(before.as_bytes()).await.map_err(|_| TextDiffError::Unavailable)?;
                stdin.write_all(after.as_bytes()).await.map_err(|_| TextDiffError::Unavailable)?;
                stdin.shutdown().await.map_err(|_| TextDiffError::Unavailable)?;
                drop(stdin);
                Ok(())
            },
            protocol::read_response(stdout),
            capped_read(stderr, MAX_STDERR_BYTES),
            async { child.wait().await.map_err(|_| TextDiffError::Unavailable) }
        ) } => result.and_then(|(_, result, _, status)| if status.success() { Ok(result) } else { Err(TextDiffError::Unavailable) })
    };
    if result.is_err() {
        let _ = child.start_kill();
        child.wait().await.map_err(|_| TextDiffError::Internal)?;
    }
    result
}

#[cfg(test)]
mod tests;
