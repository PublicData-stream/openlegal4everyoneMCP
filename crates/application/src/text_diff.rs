//! Bounded transient comparisons shared by every transport.
mod paging;

use crate::{Clock, SystemClock};
use futures::future::BoxFuture;
use openlegal_domain::text_diff::*;
use std::{
    collections::HashMap,
    mem::size_of,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{
    sync::{Semaphore, oneshot},
    task::JoinSet,
    time::Instant,
};
use tokio_util::sync::CancellationToken;

pub const MAX_PATCH_BYTES: usize = 8 * 1024 * 1024;
pub const MAX_STDERR_BYTES: usize = 8 * 1024;
pub const RETENTION: Duration = Duration::from_secs(600);
const MAX_ENTRIES: usize = 32;
const MAX_STORE_BYTES: usize = 128 * 1024 * 1024;
const JOB_RESERVATION: usize = 32 * 1024 * 1024;

/// An engine must kill and reap its child before returning on cancellation/deadline.
/// The service owns its future even when the requesting caller disappears.
pub trait DiffEngine: Send + Sync + 'static {
    fn diff(
        &self,
        before: Arc<str>,
        after: Arc<str>,
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> BoxFuture<'static, Result<String, TextDiffError>>;
}
pub trait HandleGenerator: Send + Sync + 'static {
    fn generate(&self) -> Result<[u8; 32], TextDiffError>;
}

struct Stored {
    summary: ComparisonSummary,
    before: Arc<str>,
    after: Arc<str>,
    changes: Vec<Vec<DiffFragment>>,
    expires: Instant,
    caller: CancellationToken,
    bytes: usize,
}
#[derive(Default)]
struct State {
    entries: HashMap<String, Stored>,
    reserved: usize,
    reserved_slots: usize,
    stopping: bool,
}

struct Reservation(Arc<TextDiffService>);
impl Drop for Reservation {
    fn drop(&mut self) {
        if let Ok(mut state) = self.0.state.lock() {
            state.reserved = state.reserved.saturating_sub(JOB_RESERVATION);
            state.reserved_slots = state.reserved_slots.saturating_sub(1);
        }
    }
}

pub struct TextDiffService {
    engine: Arc<dyn DiffEngine>,
    handles: Arc<dyn HandleGenerator>,
    clock: Arc<dyn Clock>,
    state: Mutex<State>,
    jobs: Mutex<JoinSet<()>>,
    admission: Arc<Semaphore>,
    shutdown: CancellationToken,
    completed: AtomicU64,
    failed: AtomicU64,
    duration_micros: AtomicU64,
    worker_failed: AtomicBool,
}

impl TextDiffService {
    pub fn new(engine: Arc<dyn DiffEngine>, handles: Arc<dyn HandleGenerator>) -> Arc<Self> {
        Self::with_clock(engine, handles, Arc::new(SystemClock::default()))
    }
    pub fn with_clock(
        engine: Arc<dyn DiffEngine>,
        handles: Arc<dyn HandleGenerator>,
        clock: Arc<dyn Clock>,
    ) -> Arc<Self> {
        Arc::new(Self {
            engine,
            handles,
            clock,
            state: Mutex::new(State::default()),
            jobs: Mutex::new(JoinSet::new()),
            admission: Arc::new(Semaphore::new(2)),
            shutdown: CancellationToken::new(),
            completed: AtomicU64::new(0),
            failed: AtomicU64::new(0),
            duration_micros: AtomicU64::new(0),
            worker_failed: AtomicBool::new(false),
        })
    }

    pub async fn compare(
        self: &Arc<Self>,
        input: CompareInput,
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> Result<ComparisonSummary, TextDiffError> {
        let before_info = text_info(
            &input.before,
            input.before_label.as_deref().unwrap_or("Before"),
        )?;
        let after_info = text_info(
            &input.after,
            input.after_label.as_deref().unwrap_or("After"),
        )?;
        if cancellation.is_cancelled() || deadline <= Instant::now() {
            return Err(TextDiffError::Cancelled);
        }
        let (receive, guard, deadline) = {
            let permit = self
                .admission
                .clone()
                .try_acquire_owned()
                .map_err(|_| TextDiffError::Busy)?;
            let mut jobs = self.jobs.lock().map_err(|_| TextDiffError::Internal)?;
            while let Some(result) = jobs.try_join_next() {
                if result.is_err() {
                    self.worker_failed.store(true, Ordering::Relaxed);
                    self.shutdown.cancel();
                    return Err(TextDiffError::Internal);
                }
            }
            {
                let mut state = self.state.lock().map_err(|_| TextDiffError::Internal)?;
                expire(&mut state);
                if state.stopping || self.shutdown.is_cancelled() {
                    return Err(TextDiffError::Unavailable);
                }
                let used: usize = state.entries.values().map(|entry| entry.bytes).sum();
                if state.entries.len() + state.reserved_slots >= MAX_ENTRIES
                    || used + state.reserved + JOB_RESERVATION > MAX_STORE_BYTES
                {
                    return Err(TextDiffError::Busy);
                }
                state.reserved += JOB_RESERVATION;
                state.reserved_slots += 1;
            }
            // The completion lease is independent of transport cancellation: transports
            // may retire their request token normally after receiving the summary.
            let token = CancellationToken::new();
            let guard = token.clone().drop_guard();
            let service = self.clone();
            let (send, receive) = oneshot::channel();
            let before: Arc<str> = input.before.into();
            let after: Arc<str> = input.after.into();
            let deadline = deadline.min(Instant::now() + Duration::from_secs(10));
            jobs.spawn(async move {
                let _permit = permit;
                let reservation = Reservation(service.clone());
                let started = Instant::now();
                let engine_token = token.child_token();
                let work = service.engine.diff(
                    before.clone(),
                    after.clone(),
                    engine_token.clone(),
                    deadline,
                );
                tokio::pin!(work);
                let patch = tokio::select! {
                    biased;
                    _ = service.shutdown.cancelled() => { engine_token.cancel(); work.await }
                    result = &mut work => result,
                };
                let result = patch.and_then(|patch| {
                    if token.is_cancelled()
                        || service.shutdown.is_cancelled()
                        || Instant::now() >= deadline
                    {
                        return Err(TextDiffError::Cancelled);
                    }
                    if patch.len() > MAX_PATCH_BYTES {
                        return Err(TextDiffError::ResourceLimit);
                    }
                    let (changes, additions, deletions) = paging::changes(&patch)?;
                    let mut state = service.state.lock().map_err(|_| TextDiffError::Internal)?;
                    if state.stopping || token.is_cancelled() || Instant::now() >= deadline {
                        return Err(TextDiffError::Cancelled);
                    }
                    let mut id = String::new();
                    for _ in 0..4 {
                        use std::fmt::Write;
                        id.clear();
                        for byte in service.handles.generate()? {
                            write!(&mut id, "{byte:02x}").map_err(|_| TextDiffError::Internal)?;
                        }
                        if !state.entries.contains_key(&id) {
                            break;
                        }
                    }
                    if state.entries.contains_key(&id) {
                        return Err(TextDiffError::Internal);
                    }
                    let equal = before == after;
                    if equal != (additions == 0 && deletions == 0) {
                        return Err(TextDiffError::Internal);
                    }
                    let summary = ComparisonSummary {
                        schema_version: 1,
                        comparison_id: id.clone(),
                        expires_at: service.clock.now().saturating_add(600),
                        before: before_info,
                        after: after_info,
                        additions,
                        deletions,
                        equal,
                        change_pages: changes.len(),
                    };
                    let bytes = retained_bytes(&before, &after, &changes)
                        + changes.capacity() * size_of::<Vec<DiffFragment>>()
                        + 4096;
                    if bytes > JOB_RESERVATION {
                        return Err(TextDiffError::ResourceLimit);
                    }
                    state.entries.insert(
                        id,
                        Stored {
                            summary: summary.clone(),
                            before,
                            after,
                            changes,
                            expires: Instant::now() + RETENTION,
                            caller: token.clone(),
                            bytes,
                        },
                    );
                    Ok(summary)
                });
                drop(reservation);
                if result.is_ok() {
                    service.completed.fetch_add(1, Ordering::Relaxed);
                } else {
                    service.failed.fetch_add(1, Ordering::Relaxed);
                }
                service.duration_micros.fetch_add(
                    started.elapsed().as_micros().min(u64::MAX as u128) as u64,
                    Ordering::Relaxed,
                );
                // A disappearing caller must not leave an inaccessible newly published result.
                if let Err(Ok(summary)) = send.send(result) {
                    let _ = service.delete(&summary.comparison_id);
                }
            });
            (receive, guard, deadline)
        };
        let result = tokio::select! {
            biased;
            _ = cancellation.cancelled() => Err(TextDiffError::Cancelled),
            _ = self.shutdown.cancelled() => Err(TextDiffError::Cancelled),
            _ = tokio::time::sleep_until(deadline) => Err(TextDiffError::Cancelled),
            result = receive => result.map_err(|_| TextDiffError::Internal)?,
        };
        if result.is_ok() {
            guard.disarm();
        } else {
            drop(guard);
        }
        result
    }

    pub fn summary(&self, id: &str) -> Result<ComparisonSummary, TextDiffError> {
        valid_handle(id)?;
        let mut state = self.state.lock().map_err(|_| TextDiffError::Internal)?;
        expire(&mut state);
        state
            .entries
            .get(id)
            .map(|entry| entry.summary.clone())
            .ok_or(TextDiffError::NotFound)
    }
    pub fn page(&self, request: PageRequest) -> Result<PageResponse, TextDiffError> {
        valid_handle(&request.comparison_id)?;
        let mut state = self.state.lock().map_err(|_| TextDiffError::Internal)?;
        expire(&mut state);
        let entry = state
            .entries
            .get(&request.comparison_id)
            .ok_or(TextDiffError::NotFound)?;
        let (total_pages, text, fragments) = match request.view {
            PageView::Changes => (
                entry.changes.len(),
                None,
                entry
                    .changes
                    .get(request.page)
                    .cloned()
                    .ok_or(TextDiffError::InvalidInput)?,
            ),
            PageView::Before | PageView::After => {
                let source = if request.view == PageView::Before {
                    &entry.before
                } else {
                    &entry.after
                };
                let chunks = paging::chunks(source);
                (
                    chunks.len(),
                    Some(
                        chunks
                            .get(request.page)
                            .ok_or(TextDiffError::InvalidInput)?
                            .to_string(),
                    ),
                    Vec::new(),
                )
            }
        };
        let response = PageResponse {
            schema_version: 1,
            comparison_id: request.comparison_id,
            view: request.view,
            page: request.page,
            total_pages,
            text,
            fragments,
        };
        if serde_json::to_vec(&response)
            .map_err(|_| TextDiffError::Internal)?
            .len()
            > 256 * 1024
        {
            return Err(TextDiffError::ResourceLimit);
        }
        Ok(response)
    }
    /// Deletion is intentionally idempotent; an absent/expired handle is indistinguishable.
    pub fn delete(&self, id: &str) -> Result<(), TextDiffError> {
        valid_handle(id)?;
        self.state
            .lock()
            .map_err(|_| TextDiffError::Internal)?
            .entries
            .remove(id);
        Ok(())
    }
    /// Fixed aggregate metrics; never include text, labels, handles or executable paths.
    pub fn metrics_prometheus(&self) -> String {
        let (entries, bytes, jobs) = self
            .state
            .lock()
            .map(|mut state| {
                expire(&mut state);
                (
                    state.entries.len(),
                    state
                        .entries
                        .values()
                        .map(|entry| entry.bytes)
                        .sum::<usize>()
                        + state.reserved,
                    state.reserved_slots,
                )
            })
            .unwrap_or_default();
        format!(
            "openlegal_text_diff_entries {entries}\nopenlegal_text_diff_accounted_bytes {bytes}\nopenlegal_text_diff_active_jobs {jobs}\nopenlegal_text_diff_completed_total {}\nopenlegal_text_diff_failed_total {}\nopenlegal_text_diff_job_duration_seconds_sum {}\n",
            self.completed.load(Ordering::Relaxed),
            self.failed.load(Ordering::Relaxed),
            self.duration_micros.load(Ordering::Relaxed) as f64 / 1_000_000.0
        )
    }
    /// Own all comparison jobs through process cleanup, including dropped callers.
    pub async fn run(&self, shutdown: CancellationToken) -> Result<(), TextDiffError> {
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        let mut failed = false;
        loop {
            tokio::select! {
                biased;
                _ = shutdown.cancelled() => break,
                _ = self.shutdown.cancelled() => break,
                _ = interval.tick() => {
                    let mut jobs = self.jobs.lock().map_err(|_| TextDiffError::Internal)?;
                    expire(&mut *self.state.lock().map_err(|_| TextDiffError::Internal)?);
                    while let Some(result) = jobs.try_join_next() { if result.is_err() { failed = true; self.shutdown.cancel(); } }
                }
            }
        }
        self.shutdown.cancel();
        let mut jobs = {
            let mut jobs = self.jobs.lock().map_err(|_| TextDiffError::Internal)?;
            self.state
                .lock()
                .map_err(|_| TextDiffError::Internal)?
                .stopping = true;
            std::mem::take(&mut *jobs)
        };
        while let Some(result) = jobs.join_next().await {
            failed |= result.is_err();
        }
        self.state
            .lock()
            .map_err(|_| TextDiffError::Internal)?
            .entries
            .clear();
        if failed || self.worker_failed.load(Ordering::Relaxed) {
            Err(TextDiffError::Internal)
        } else {
            Ok(())
        }
    }
}

fn expire(state: &mut State) {
    state
        .entries
        .retain(|_, entry| entry.expires > Instant::now() && !entry.caller.is_cancelled());
}
fn valid_handle(id: &str) -> Result<(), TextDiffError> {
    if id.len() == 64
        && id
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        Ok(())
    } else {
        Err(TextDiffError::InvalidInput)
    }
}
fn retained_bytes(before: &str, after: &str, pages: &[Vec<DiffFragment>]) -> usize {
    before.len()
        + after.len()
        + pages
            .iter()
            .map(|page| {
                page.capacity() * size_of::<DiffFragment>()
                    + page
                        .iter()
                        .map(|fragment| fragment.patch.capacity())
                        .sum::<usize>()
            })
            .sum::<usize>()
}

pub fn text_info(text: &str, label: &str) -> Result<TextInfo, TextDiffError> {
    if text.len() > MAX_TEXT_BYTES
        || text.contains('\0')
        || label.is_empty()
        || label.len() > 128
        || label.chars().any(char::is_control)
    {
        return Err(TextDiffError::InvalidInput);
    }
    let lines = text.split_inclusive('\n').count();
    if lines > MAX_LINES
        || text
            .split_inclusive('\n')
            .any(|line| line.strip_suffix('\n').unwrap_or(line).len() > MAX_LINE_BYTES)
    {
        return Err(TextDiffError::InvalidInput);
    }
    let crlf = text.matches("\r\n").count();
    Ok(TextInfo {
        label: label.to_owned(),
        bytes: text.len(),
        lines,
        crlf,
        lf: text.bytes().filter(|b| *b == b'\n').count() - crlf,
        bare_cr: text.bytes().filter(|b| *b == b'\r').count() - crlf,
        bom: text.starts_with('\u{feff}'),
        final_newline: text.ends_with('\n'),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::FutureExt;
    struct Handles(AtomicU64);
    impl HandleGenerator for Handles {
        fn generate(&self) -> Result<[u8; 32], TextDiffError> {
            let mut bytes = [0; 32];
            bytes[..8].copy_from_slice(&self.0.fetch_add(1, Ordering::Relaxed).to_be_bytes());
            Ok(bytes)
        }
    }
    struct Equal;
    impl DiffEngine for Equal {
        fn diff(
            &self,
            _: Arc<str>,
            _: Arc<str>,
            _: CancellationToken,
            _: Instant,
        ) -> BoxFuture<'static, Result<String, TextDiffError>> {
            async { Ok(String::new()) }.boxed()
        }
    }
    fn service() -> Arc<TextDiffService> {
        TextDiffService::new(Arc::new(Equal), Arc::new(Handles(AtomicU64::new(0))))
    }
    fn input() -> CompareInput {
        CompareInput {
            before: "secret\r\n".into(),
            after: "secret\r\n".into(),
            before_label: None,
            after_label: None,
        }
    }
    #[tokio::test(start_paused = true)]
    async fn fixed_expiry_and_completed_request_cancellation_preserve_retention() {
        let service = service();
        let token = CancellationToken::new();
        let summary = service
            .compare(
                input(),
                token.clone(),
                Instant::now() + Duration::from_secs(20),
            )
            .await
            .unwrap();
        token.cancel();
        assert!(service.summary(&summary.comparison_id).is_ok());
        tokio::time::advance(Duration::from_secs(599)).await;
        assert!(service.summary(&summary.comparison_id).is_ok());
        tokio::time::advance(Duration::from_secs(1)).await;
        assert_eq!(
            service.summary(&summary.comparison_id).unwrap_err(),
            TextDiffError::NotFound
        );
        service.delete(&summary.comparison_id).unwrap();
        service.delete(&summary.comparison_id).unwrap();
    }
    #[tokio::test]
    async fn original_pages_preserve_text_and_delete_is_idempotent() {
        let service = service();
        let summary = service
            .compare(
                input(),
                CancellationToken::new(),
                Instant::now() + Duration::from_secs(10),
            )
            .await
            .unwrap();
        assert!(summary.equal);
        assert_eq!(summary.change_pages, 0);
        let page = service
            .page(PageRequest {
                comparison_id: summary.comparison_id.clone(),
                view: PageView::Before,
                page: 0,
            })
            .unwrap();
        assert_eq!(page.text.as_deref(), Some("secret\r\n"));
        service.delete(&summary.comparison_id).unwrap();
        service.delete(&summary.comparison_id).unwrap();
        assert_eq!(
            service.summary(&summary.comparison_id).unwrap_err(),
            TextDiffError::NotFound
        );
        assert!(!service.metrics_prometheus().contains("secret"));
    }
    #[tokio::test]
    async fn entry_capacity_rejects_without_eviction_then_deletion_restores_capacity() {
        let service = service();
        let mut ids = Vec::new();
        for _ in 0..32 {
            ids.push(
                service
                    .compare(
                        input(),
                        CancellationToken::new(),
                        Instant::now() + Duration::from_secs(10),
                    )
                    .await
                    .unwrap()
                    .comparison_id,
            );
        }
        assert_eq!(
            service
                .compare(
                    input(),
                    CancellationToken::new(),
                    Instant::now() + Duration::from_secs(10)
                )
                .await
                .unwrap_err(),
            TextDiffError::Busy
        );
        assert!(service.summary(&ids[0]).is_ok());
        service.delete(&ids[0]).unwrap();
        assert!(
            service
                .compare(
                    input(),
                    CancellationToken::new(),
                    Instant::now() + Duration::from_secs(10)
                )
                .await
                .is_ok()
        );
    }
    struct Waiting {
        started: Arc<Semaphore>,
        finished: Arc<AtomicU64>,
    }
    impl DiffEngine for Waiting {
        fn diff(
            &self,
            _: Arc<str>,
            _: Arc<str>,
            cancellation: CancellationToken,
            deadline: Instant,
        ) -> BoxFuture<'static, Result<String, TextDiffError>> {
            let started = self.started.clone();
            let finished = self.finished.clone();
            async move {
                started.add_permits(1);
                tokio::select! { _ = cancellation.cancelled() => {}, _ = tokio::time::sleep_until(deadline) => {} }
                tokio::task::yield_now().await; // observable owned cleanup after cancellation
                finished.fetch_add(1, Ordering::Relaxed);
                Err(TextDiffError::Cancelled)
            }.boxed()
        }
    }
    #[tokio::test]
    async fn saturation_dropped_callers_and_shutdown_join_owned_jobs() {
        let started = Arc::new(Semaphore::new(0));
        let finished = Arc::new(AtomicU64::new(0));
        let service = TextDiffService::new(
            Arc::new(Waiting {
                started: started.clone(),
                finished: finished.clone(),
            }),
            Arc::new(Handles(AtomicU64::new(0))),
        );
        let mut calls = Vec::new();
        for _ in 0..2 {
            let service = service.clone();
            calls.push(tokio::spawn(async move {
                service
                    .compare(
                        input(),
                        CancellationToken::new(),
                        Instant::now() + Duration::from_secs(10),
                    )
                    .await
            }));
        }
        started.acquire_many(2).await.unwrap().forget();
        assert_eq!(
            service
                .compare(
                    input(),
                    CancellationToken::new(),
                    Instant::now() + Duration::from_secs(10)
                )
                .await
                .unwrap_err(),
            TextDiffError::Busy
        );
        calls[0].abort();
        let _ = (&mut calls[0]).await;
        let stop = CancellationToken::new();
        stop.cancel();
        service.run(stop).await.unwrap();
        assert_eq!(finished.load(Ordering::Relaxed), 2);
        assert!(calls.remove(1).await.unwrap().is_err());
        assert_eq!(service.state.lock().unwrap().reserved, 0);
        assert!(service.state.lock().unwrap().entries.is_empty());
    }
    #[tokio::test(start_paused = true)]
    async fn deadline_cancels_owned_work_without_publishing() {
        let started = Arc::new(Semaphore::new(0));
        let finished = Arc::new(AtomicU64::new(0));
        let service = TextDiffService::new(
            Arc::new(Waiting {
                started: started.clone(),
                finished: finished.clone(),
            }),
            Arc::new(Handles(AtomicU64::new(0))),
        );
        let caller_service = service.clone();
        let call = tokio::spawn(async move {
            caller_service
                .compare(
                    input(),
                    CancellationToken::new(),
                    Instant::now() + Duration::from_secs(1),
                )
                .await
        });
        started.acquire().await.unwrap().forget();
        tokio::time::advance(Duration::from_secs(1)).await;
        assert_eq!(call.await.unwrap().unwrap_err(), TextDiffError::Cancelled);
        let stop = CancellationToken::new();
        stop.cancel();
        service.run(stop).await.unwrap();
        assert_eq!(finished.load(Ordering::Relaxed), 1);
        assert!(service.state.lock().unwrap().entries.is_empty());
        assert_eq!(service.state.lock().unwrap().reserved, 0);
    }
    #[test]
    fn byte_line_and_text_semantics_are_checked_before_work() {
        assert!(text_info(&"x".repeat(MAX_LINE_BYTES), "Before").is_ok());
        assert!(text_info(&"x".repeat(MAX_LINE_BYTES + 1), "Before").is_err());
        assert!(text_info(&"\n".repeat(MAX_LINES + 1), "Before").is_err());
        assert!(text_info("x\0", "Before").is_err());
        let info = text_info("\u{feff}한\r\n\r끝\n", "Before").unwrap();
        assert_eq!(
            (info.lines, info.crlf, info.lf, info.bare_cr, info.bom),
            (2, 1, 1, 1, true)
        );
    }
}
