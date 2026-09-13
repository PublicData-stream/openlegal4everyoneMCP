//! Persistent L2 isolated in a supervised, single-writer child process.
//!
//! Cancellation before COMMIT prevents manifest publication. After authorization,
//! a killed transaction can be found committed during recovery; only the synced
//! COMMITTED reply is a successful write. No filesystem call runs in this adapter's
//! parent runtime. A kernel-stuck child cannot be promised to reap by a deadline:
//! that condition closes admission and never launches a replacement writer.
mod protocol;
mod worker;

use futures::future::BoxFuture;
use openlegal_application::{
    StoredPayload,
    persistence::{
        CommitAuthorization, HistoryKey, PersistentKey, PersistentStore, RetentionPolicy,
        StorageMetrics, StoredResult,
    },
};
use openlegal_domain::{
    RetrievalError as Error,
    history::{SnapshotEnvelope, SnapshotPage},
};
use protocol::{Payload, Reply, Request, Response};
use std::{
    path::{Path, PathBuf},
    process::Stdio,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    process::{Child, ChildStdin, ChildStdout, Command},
    sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot, watch},
    task::JoinHandle,
    time::Instant,
};
use tokio_util::sync::CancellationToken;

const QUEUE: usize = 32;
const REAP: Duration = Duration::from_secs(2);
const STARTUP: Duration = Duration::from_secs(30);
// Unit Engine tests intentionally execute filesystem mechanics in the harness
// process. Exclude process-fixture forks while those tests hold flock/writer
// descriptors; production holds them exclusively in the non-forking cache child.
#[cfg(test)]
pub(crate) static PROCESS_FIXTURE_GATE: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

pub fn run_worker() -> Result<(), Error> {
    worker::run()
}

struct Shared {
    epoch: AtomicU64,
    healthy: AtomicBool,
    available: AtomicBool,
    metrics: Mutex<StorageMetrics>,
    bytes: Arc<Semaphore>,
    finished: watch::Sender<Option<Result<(), Error>>>,
}
struct Call {
    request: Request,
    cancellation: CancellationToken,
    authorize: Option<CommitAuthorization>,
    reply: oneshot::Sender<Result<(Response, u64), Error>>,
    _bytes: OwnedSemaphorePermit,
    deadline: Instant,
}
type OwnedSupervisor = Arc<Mutex<Option<JoinHandle<Result<(), Error>>>>>;
pub struct FsCache {
    sender: mpsc::Sender<Call>,
    shared: Arc<Shared>,
    stop: CancellationToken,
    task: OwnedSupervisor,
    policy: RetentionPolicy,
}
struct Process {
    child: Child,
    input: ChildStdin,
    output: ChildStdout,
    stderr: JoinHandle<()>,
}
impl Process {
    async fn launch(
        executable: &Path,
        root: &Path,
        policy: &RetentionPolicy,
        stop: &CancellationToken,
    ) -> Result<(Self, StorageMetrics), Error> {
        let mut command = Command::new(executable);
        command
            .arg("--cache-worker")
            .env_clear()
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        let mut child = command.spawn().map_err(|error| {
            #[cfg(test)]
            eprintln!(
                "cache fixture spawn failure: kind={:?}, errno={:?}",
                error.kind(),
                error.raw_os_error()
            );
            #[cfg(not(test))]
            let _ = error;
            Error::StorageUnavailable
        })?;
        let input = child.stdin.take().ok_or(Error::Internal)?;
        let output = child.stdout.take().ok_or(Error::Internal)?;
        let stderr = child.stderr.take().ok_or(Error::Internal)?;
        let stderr = tokio::spawn(async move {
            let mut bytes = Vec::new();
            let _ = stderr.take(8193).read_to_end(&mut bytes).await;
        });
        let mut process = Self {
            child,
            input,
            output,
            stderr,
        };
        let open = Request::Open {
            root: root.to_str().ok_or(Error::InvalidInput)?.into(),
            policy: policy.clone(),
        };
        let result = tokio::select! {biased;_=stop.cancelled()=>Err(Error::Shutdown),result=tokio::time::timeout(STARTUP,async{
            protocol::write_async(&mut process.input,&open,&[]).await?;
            let(reply,raw)=protocol::read_async::<Reply>(&mut process.output).await?;
            if !raw.is_empty() || !matches!(reply.response,Response::Ready){return Err(Error::StorageUnavailable);}
            Ok(reply.metrics)
        })=>result.unwrap_or(Err(Error::StorageUnavailable))};
        match result {
            Ok(metrics) => Ok((process, metrics)),
            Err(e) => {
                #[cfg(test)]
                eprintln!(
                    "cache fixture startup exchange failure: error={e:?}, status={:?}",
                    process.child.try_wait()
                );
                process.kill().await?;
                Err(e)
            }
        }
    }
    async fn kill(&mut self) -> Result<(), Error> {
        let _ = self.child.start_kill();
        let result = tokio::time::timeout(REAP, self.child.wait()).await;
        self.stderr.abort();
        let _ = (&mut self.stderr).await;
        match result {
            Ok(Ok(_)) => Ok(()),
            _ => Err(Error::StorageUnavailable),
        }
    }
    async fn exchange(&mut self, call: &Call, shared: &Shared) -> Result<Reply, Error> {
        protocol::write_async(&mut self.input, &call.request, call.request.raw()).await?;
        let (mut reply, raw) = protocol::read_async::<Reply>(&mut self.output).await?;
        reply.attach(raw)?;
        let mut stages = 0;
        while matches!(reply.response, Response::Prepared) {
            stages += 1;
            if stages > 2 {
                return Err(Error::StorageUnavailable);
            }
            let publish =
                matches!(call.request, Request::Publish { .. }) && !reply.invalidates_memory;
            if call.cancellation.is_cancelled()
                || publish && !call.authorize.as_ref().is_some_and(|f| f())
            {
                return Err(Error::Cancelled);
            }
            if reply.invalidates_memory {
                shared.epoch.fetch_add(1, Ordering::AcqRel);
            }
            // Sending this frame is the point after which cancellation is not a
            // rollback promise; the supervisor kills and recovers on uncertainty.
            protocol::write_async(&mut self.input, &Request::Commit, &[]).await?;
            let (next, raw) = protocol::read_async::<Reply>(&mut self.output).await?;
            reply = next;
            reply.attach(raw)?;
        }
        if matches!(reply.response, Response::Prepared | Response::Ready) {
            return Err(Error::StorageUnavailable);
        }
        Ok(reply)
    }
}

impl FsCache {
    pub async fn open(
        executable: &Path,
        root: &Path,
        policy: RetentionPolicy,
    ) -> Result<Arc<Self>, Error> {
        policy.validate()?;
        if !executable.is_absolute() || !root.is_absolute() || root.to_str().is_none() {
            return Err(Error::InvalidInput);
        }
        let stop = CancellationToken::new();
        let (process, metrics) = Process::launch(executable, root, &policy, &stop).await?;
        let shared = Arc::new(Shared {
            epoch: AtomicU64::new(1),
            healthy: AtomicBool::new(true),
            available: AtomicBool::new(true),
            metrics: Mutex::new(metrics),
            bytes: Arc::new(Semaphore::new(8 * 1024 * 1024)),
            finished: watch::channel(None).0,
        });
        let (sender, receiver) = mpsc::channel(QUEUE);
        let cache = Arc::new(Self {
            sender,
            shared: shared.clone(),
            stop: stop.clone(),
            task: Arc::new(Mutex::new(None)),
            policy: policy.clone(),
        });
        let executable = executable.to_owned();
        let root = root.to_owned();
        let completion = shared.clone();
        let task = tokio::spawn(async move {
            let result = supervise(process, receiver, shared, stop, executable, root, policy).await;
            completion.finished.send_replace(Some(result));
            result
        });
        *cache.task.lock().map_err(|_| Error::Internal)? = Some(task);
        Ok(cache)
    }
    fn call(
        &self,
        request: Request,
        cancellation: CancellationToken,
        authorize: Option<CommitAuthorization>,
    ) -> BoxFuture<'static, Result<(Response, u64), Error>> {
        let sender = self.sender.clone();
        let shared = self.shared.clone();
        let stop = self.stop.clone();
        Box::pin(async move {
            let token = cancellation.child_token();
            let _guard = CancelOnDrop(token.clone());
            if stop.is_cancelled() {
                return Err(Error::Shutdown);
            }
            if token.is_cancelled() {
                return Err(Error::Cancelled);
            }
            if !shared.healthy.load(Ordering::Acquire) || !shared.available.load(Ordering::Acquire)
            {
                return Err(Error::StorageUnavailable);
            }
            let bytes = protocol::encode(&request, request.raw())?.len();
            let permit = shared
                .bytes
                .clone()
                .try_acquire_many_owned(bytes as u32)
                .map_err(|_| Error::Busy)?;
            let (tx, rx) = oneshot::channel();
            sender
                .try_send(Call {
                    request,
                    cancellation: token.clone(),
                    authorize,
                    reply: tx,
                    _bytes: permit,
                    deadline: Instant::now() + Duration::from_secs(5),
                })
                .map_err(|e| match e {
                    mpsc::error::TrySendError::Full(_) => {
                        if let Ok(mut m) = shared.metrics.lock() {
                            m.saturation += 1;
                        }
                        Error::Busy
                    }
                    mpsc::error::TrySendError::Closed(_) => Error::StorageUnavailable,
                })?;
            // Once admitted, completion means the owned child operation has
            // reconciled. Dropping this future still signals cancellation.
            rx.await.unwrap_or(Err(Error::StorageUnavailable))
        })
    }
}
struct CancelOnDrop(CancellationToken);
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.cancel();
    }
}
impl Drop for FsCache {
    fn drop(&mut self) {
        self.stop.cancel();
    }
}

async fn supervise(
    mut process: Process,
    mut receiver: mpsc::Receiver<Call>,
    shared: Arc<Shared>,
    stop: CancellationToken,
    executable: PathBuf,
    root: PathBuf,
    policy: RetentionPolicy,
) -> Result<(), Error> {
    struct Terminal(Arc<Shared>);
    impl Drop for Terminal {
        fn drop(&mut self) {
            self.0.healthy.store(false, Ordering::Release);
            self.0.available.store(false, Ordering::Release);
        }
    }
    let _terminal = Terminal(shared.clone());
    let mut restarts = Vec::<Instant>::new();
    let mut metrics_base = StorageMetrics::default();
    loop {
        let call = tokio::select! {biased;_=stop.cancelled()=>break,call=receiver.recv()=>match call{Some(call)=>call,None=>break}};
        if call.cancellation.is_cancelled() {
            let _ = call.reply.send(Err(Error::Cancelled));
            continue;
        }
        if Instant::now() >= call.deadline {
            let _ = call.reply.send(Err(Error::StorageUnavailable));
            continue;
        }
        let (result, cancelled) = {
            let exchange = process.exchange(&call, &shared);
            tokio::pin!(exchange);
            tokio::select! {biased;
                _=stop.cancelled()=>(Err(Error::Shutdown),false),
                _=call.cancellation.cancelled()=>{
                    let until=call.deadline.min(Instant::now()+Duration::from_secs(1));
                    let result=tokio::select!{biased;_=stop.cancelled()=>Err(Error::Shutdown),value=tokio::time::timeout_at(until,&mut exchange)=>value.unwrap_or(Err(Error::Cancelled))};
                    (result,true)
                },
                result=tokio::time::timeout_at(call.deadline,&mut exchange)=>(result.unwrap_or(Err(Error::StorageUnavailable)),false),
            }
        };
        match result {
            Ok(reply) => {
                if let Ok(mut metrics) = shared.metrics.lock() {
                    let saturation = metrics.saturation;
                    *metrics = add_metrics(&metrics_base, reply.metrics);
                    metrics.saturation = saturation;
                }
                let result = if cancelled {
                    Err(Error::Cancelled)
                } else {
                    match reply.response {
                        Response::Error(e) => Err(e.into()),
                        response => Ok((response, shared.epoch.load(Ordering::Acquire))),
                    }
                };
                let _ = call.reply.send(result);
            }
            Err(error) => {
                shared.available.store(false, Ordering::Release);
                shared.epoch.fetch_add(1, Ordering::AcqRel);
                let reaped = process.kill().await;
                if reaped.is_err() {
                    shared.healthy.store(false, Ordering::Release);
                    let _ = call.reply.send(Err(Error::StorageUnavailable));
                    return Err(Error::StorageUnavailable);
                }
                if stop.is_cancelled() {
                    let _ = call.reply.send(Err(error));
                    return Ok(());
                }
                let now = Instant::now();
                restarts.retain(|at| now.duration_since(*at) < Duration::from_secs(60));
                if restarts.len() >= 3 {
                    shared.healthy.store(false, Ordering::Release);
                    let _ = call.reply.send(Err(Error::StorageUnavailable));
                    return Err(Error::StorageUnavailable);
                }
                restarts.push(now);
                if let Ok(metrics) = shared.metrics.lock() {
                    metrics_base = metrics.clone();
                }
                // Recovery owns this task. Cancellation of one client cannot
                // create concurrent restart processes or release root exclusion.
                tokio::select! {biased;_=stop.cancelled()=>{let _=call.reply.send(Err(error));return Ok(());},_=tokio::time::sleep(Duration::from_secs(1))=>{}};
                let launched = Process::launch(&executable, &root, &policy, &stop).await;
                let (next, metrics) = match launched {
                    Ok(v) => v,
                    Err(e) => {
                        shared.healthy.store(false, Ordering::Release);
                        let _ = call.reply.send(Err(e));
                        if stop.is_cancelled() {
                            return Ok(());
                        }
                        return Err(e);
                    }
                };
                process = next;
                if let Ok(mut old) = shared.metrics.lock() {
                    *old = add_metrics(&metrics_base, metrics);
                }
                shared.available.store(true, Ordering::Release);
                let _ = call.reply.send(Err(error));
            }
        }
    }
    shared.healthy.store(false, Ordering::Release);
    shared.epoch.fetch_add(1, Ordering::AcqRel);
    receiver.close();
    while let Ok(call) = receiver.try_recv() {
        let _ = call.reply.send(Err(Error::Shutdown));
    }
    let _ = process.input.shutdown().await;
    process.kill().await
}
fn add_metrics(base: &StorageMetrics, mut current: StorageMetrics) -> StorageMetrics {
    current.hits += base.hits;
    current.misses += base.misses;
    current.writes += base.writes;
    current.evictions += base.evictions;
    current.corruptions += base.corruptions;
    current.recoveries += base.recoveries;
    current.saturation += base.saturation;
    current
}

impl PersistentStore for FsCache {
    fn lookup(
        &self,
        key: PersistentKey,
        now: u64,
        cancellation: CancellationToken,
    ) -> BoxFuture<'static, Result<Option<StoredResult>, Error>> {
        let call = self.call(Request::Lookup { key, now }, cancellation, None);
        Box::pin(async move {
            let (response, epoch) = call.await?;
            match response {
                Response::Payload(value) => Ok(value.map(|p| StoredResult {
                    payload: p.into_stored(),
                    epoch,
                })),
                _ => Err(Error::StorageUnavailable),
            }
        })
    }
    fn publish(
        &self,
        key: PersistentKey,
        value: Arc<StoredPayload>,
        now: u64,
        authorize: CommitAuthorization,
        cancellation: CancellationToken,
    ) -> BoxFuture<'static, Result<StoredResult, Error>> {
        let call = self.call(
            Request::Publish {
                key,
                value: Box::new(Payload::from_stored(&value)),
                now,
            },
            cancellation,
            Some(authorize),
        );
        Box::pin(async move {
            let (response, epoch) = call.await?;
            match response {
                Response::Payload(Some(p)) => Ok(StoredResult {
                    payload: p.into_stored(),
                    epoch,
                }),
                _ => Err(Error::StorageUnavailable),
            }
        })
    }
    fn list(
        &self,
        key: HistoryKey,
        cursor: Option<String>,
        limit: usize,
        now: u64,
        cancellation: CancellationToken,
    ) -> BoxFuture<'static, Result<SnapshotPage, Error>> {
        let call = self.call(
            Request::List {
                key,
                cursor,
                limit,
                now,
            },
            cancellation,
            None,
        );
        Box::pin(async move {
            match call.await?.0 {
                Response::List(v) => Ok(v),
                _ => Err(Error::StorageUnavailable),
            }
        })
    }
    fn get(
        &self,
        key: HistoryKey,
        id: String,
        now: u64,
        cancellation: CancellationToken,
    ) -> BoxFuture<'static, Result<SnapshotEnvelope, Error>> {
        let call = self.call(Request::Get { key, id, now }, cancellation, None);
        Box::pin(async move {
            match call.await?.0 {
                Response::Snapshot(v) => Ok(v),
                _ => Err(Error::StorageUnavailable),
            }
        })
    }
    fn maintain(&self, now: u64) -> BoxFuture<'static, Result<(), Error>> {
        let call = self.call(Request::Maintain { now }, CancellationToken::new(), None);
        Box::pin(async move {
            match call.await?.0 {
                Response::Done => Ok(()),
                _ => Err(Error::StorageUnavailable),
            }
        })
    }
    fn close(&self) -> BoxFuture<'static, Result<(), Error>> {
        self.stop.cancel();
        let task = self.task.clone();
        let mut finished = self.shared.finished.subscribe();
        Box::pin(async move {
            let task = task.lock().map_err(|_| Error::Internal)?.take();
            if let Some(task) = task {
                task.await.map_err(|_| Error::StorageUnavailable)?
            } else {
                loop {
                    if let Some(result) = *finished.borrow_and_update() {
                        break result;
                    }
                    finished
                        .changed()
                        .await
                        .map_err(|_| Error::StorageUnavailable)?;
                }
            }
        })
    }
    fn epoch(&self) -> u64 {
        self.shared.epoch.load(Ordering::Acquire)
    }
    fn healthy(&self) -> bool {
        self.shared.healthy.load(Ordering::Acquire) && !self.stop.is_cancelled()
    }
    fn policy(&self) -> RetentionPolicy {
        self.policy.clone()
    }
    fn metrics(&self) -> StorageMetrics {
        self.shared
            .metrics
            .lock()
            .map(|m| m.clone())
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests;
