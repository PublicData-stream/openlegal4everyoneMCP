//! Cross-process refusal of unclean strict-runtime exits. These control records
//! are NOT legal blob generations, replication ACKs or automatic recovery rights.
//!
//! A private SSD directory is shared by all strict processes. Its directory lock
//! serializes bounded scans and lifecycle changes. A live process holds one file
//! flock; dropping it leaves `active_unclean` evidence which blocks every peer.
//! Only an explicit clean-shutdown assertion may persist `closed_clean`. No Drop
//! path removes or cleans records, and no runtime path removes an unclean record.
use futures::future::BoxFuture;
use openlegal_application::durability::DurabilityFence;
use openlegal_domain::RetrievalError as Error;
use rustix::fs::{self, AtFlags, FlockOperation, Mode, OFlags};
use serde::{Deserialize, Serialize};
use std::{
    fs::File,
    os::unix::{
        ffi::OsStrExt,
        fs::{FileExt, MetadataExt},
    },
    path::{Component, Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::sync::{Notify, Semaphore};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const LEAF: &str = ".openlegal-runtime-guards";
const DIRECTORY_LOCK: &str = ".directory.lock";
const RECORD_LIMIT: u64 = 4096;
const GUARDS: usize = 512;
const BUDGET: Duration = Duration::from_secs(2);
static STARTUPS: Semaphore = Semaphore::const_new(16);

fn unavailable(_: impl std::fmt::Debug) -> Error {
    Error::StorageUnavailable
}
fn within(deadline: Instant, cancellation: &CancellationToken) -> Result<(), Error> {
    if cancellation.is_cancelled() {
        Err(Error::Cancelled)
    } else if Instant::now() >= deadline {
        Err(Error::StorageUnavailable)
    } else {
        Ok(())
    }
}
fn valid_binding(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|v| v.is_ascii_digit() || (b'a'..=b'f').contains(&v))
}
fn valid_key(value: &str) -> bool {
    if matches!(
        value,
        "serve" | "scheduler" | "admin.maintain" | "admin.rebuild"
    ) {
        return true;
    }
    let Some(rest) = value.strip_prefix("collection.") else {
        return false;
    };
    let Some((id, epoch)) = rest.split_once('.') else {
        return false;
    };
    Uuid::parse_str(id).is_ok_and(|v| !v.is_nil() && v.to_string() == id)
        && epoch.parse::<u64>().is_ok_and(|v| v.to_string() == epoch)
}
fn nonce() -> Result<Uuid, Error> {
    let mut bytes = [0; 16];
    getrandom::fill(&mut bytes).map_err(unavailable)?;
    bytes[6] = (bytes[6] & 15) | 64;
    bytes[8] = (bytes[8] & 63) | 128;
    Ok(Uuid::from_bytes(bytes))
}
fn boot_id() -> Result<Uuid, Error> {
    let text = std::fs::read_to_string("/proc/sys/kernel/random/boot_id").map_err(unavailable)?;
    if text.len() > 64 {
        return Err(Error::StorageUnavailable);
    }
    Uuid::parse_str(text.trim()).map_err(unavailable)
}
fn start_ticks() -> Result<u64, Error> {
    let text = std::fs::read_to_string("/proc/self/stat").map_err(unavailable)?;
    if text.len() > 8192 {
        return Err(Error::StorageUnavailable);
    }
    text.rsplit_once(')')
        .and_then(|(_, tail)| tail.split_whitespace().nth(19))
        .ok_or(Error::StorageUnavailable)?
        .parse()
        .map_err(unavailable)
}
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Identity {
    dev: u64,
    ino: u64,
    fsid: u64,
}
fn identity(file: &File) -> Result<Identity, Error> {
    let s = file.metadata().map_err(unavailable)?;
    Ok(Identity {
        dev: s.dev(),
        ino: s.ino(),
        fsid: fs::fstatvfs(file).map_err(unavailable)?.f_fsid,
    })
}
fn same_filesystem(parent: &Identity, child: &Identity) -> bool {
    parent.dev == child.dev && parent.fsid == child.fsid
}
fn private_directory(file: File, uid: u32) -> Result<File, Error> {
    let s = file.metadata().map_err(unavailable)?;
    if !s.is_dir() || s.uid() != uid || s.mode() & 0o7777 != 0o700 {
        return Err(Error::StorageUnavailable);
    }
    Ok(file)
}
fn regular(file: File, uid: u32) -> Result<File, Error> {
    let s = file.metadata().map_err(unavailable)?;
    if !s.is_file()
        || s.uid() != uid
        || s.nlink() != 1
        || s.mode() & 0o7777 != 0o600
        || s.len() > RECORD_LIMIT
    {
        return Err(Error::StorageUnavailable);
    }
    Ok(file)
}
fn open_directory(path: &Path) -> Result<File, Error> {
    if !path.is_absolute()
        || path.as_os_str().as_bytes().len() > 4096
        || path.components().count() > 128
    {
        return Err(Error::InvalidInput);
    }
    let mut directory = File::from(
        fs::open(
            "/",
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(unavailable)?,
    );
    for component in path.components() {
        match component {
            Component::RootDir => {}
            Component::Normal(name) => {
                directory = File::from(
                    fs::openat(
                        &directory,
                        name,
                        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                        Mode::empty(),
                    )
                    .map_err(unavailable)?,
                )
            }
            _ => return Err(Error::InvalidInput),
        }
    }
    Ok(directory)
}
fn open_regular(dir: &File, name: &str, uid: u32, create: bool) -> Result<File, Error> {
    let flags = OFlags::RDWR
        | OFlags::NOFOLLOW
        | OFlags::NONBLOCK
        | OFlags::CLOEXEC
        | if create {
            OFlags::CREATE | OFlags::EXCL
        } else {
            OFlags::empty()
        };
    let file = regular(
        File::from(fs::openat(dir, name, flags, Mode::from_raw_mode(0o600)).map_err(unavailable)?),
        uid,
    )?;
    if !same_filesystem(&identity(dir)?, &identity(&file)?) {
        return Err(Error::StorageUnavailable);
    }
    Ok(file)
}
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum State {
    ActiveUnclean,
    ClosedClean,
}
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Record {
    schema_version: u8,
    managed_epoch: Uuid,
    binding_sha256: String,
    key: String,
    operation_id: Uuid,
    parent: Identity,
    directory: Identity,
    boot_id: Uuid,
    // Diagnostic only: peer Pods may use different PID namespaces. The flock,
    // not another namespace's /proc/<pid>, establishes active ownership.
    pid: u32,
    process_start_ticks: u64,
    state: State,
}
fn read_record(file: &File) -> Result<Record, Error> {
    let before = file.metadata().map_err(unavailable)?;
    if before.len() == 0 || before.len() > RECORD_LIMIT {
        return Err(Error::StorageUnavailable);
    }
    let mut bytes = vec![0; before.len() as usize];
    file.read_exact_at(&mut bytes, 0).map_err(unavailable)?;
    let after = file.metadata().map_err(unavailable)?;
    if (
        before.dev(),
        before.ino(),
        before.len(),
        before.mtime_nsec(),
        before.ctime_nsec(),
        before.mtime(),
        before.ctime(),
    ) != (
        after.dev(),
        after.ino(),
        after.len(),
        after.mtime_nsec(),
        after.ctime_nsec(),
        after.mtime(),
        after.ctime(),
    ) {
        return Err(Error::StorageUnavailable);
    }
    serde_json::from_slice(&bytes).map_err(unavailable)
}
fn write_record(file: &File, record: &Record) -> Result<(), Error> {
    let mut bytes = serde_json::to_vec(record).map_err(unavailable)?;
    bytes.push(b'\n');
    if bytes.len() as u64 > RECORD_LIMIT {
        return Err(Error::InvalidInput);
    }
    file.write_all_at(&bytes, 0).map_err(unavailable)?;
    file.set_len(bytes.len() as u64).map_err(unavailable)?;
    file.sync_all().map_err(unavailable)?;
    if read_record(file)? != *record {
        return Err(Error::StorageUnavailable);
    }
    Ok(())
}
struct Directory {
    path: PathBuf,
    parent_path: PathBuf,
    parent: File,
    dir: File,
    global: File,
    parent_identity: Identity,
    dir_identity: Identity,
    global_identity: (u64, u64),
    uid: u32,
    epoch: Uuid,
    binding: String,
    boot: Uuid,
}
impl Directory {
    fn open(path: &Path, epoch: Uuid, binding: &str) -> Result<Self, Error> {
        if path.file_name().is_none_or(|v| v != LEAF) {
            return Err(Error::InvalidInput);
        }
        let uid = rustix::process::geteuid().as_raw();
        let parent_path = path.parent().ok_or(Error::InvalidInput)?.to_owned();
        let parent = private_directory(open_directory(&parent_path)?, uid)?;
        match fs::mkdirat(&parent, LEAF, Mode::from_raw_mode(0o700)) {
            Ok(()) | Err(rustix::io::Errno::EXIST) => {}
            Err(e) => return Err(unavailable(e)),
        }
        parent.sync_all().map_err(unavailable)?;
        let dir = private_directory(open_directory(path)?, uid)?;
        let parent_identity = identity(&parent)?;
        let dir_identity = identity(&dir)?;
        if !same_filesystem(&parent_identity, &dir_identity) {
            return Err(Error::StorageUnavailable);
        }
        let global = match open_regular(&dir, DIRECTORY_LOCK, uid, true) {
            Ok(file) => file,
            Err(_) => open_regular(&dir, DIRECTORY_LOCK, uid, false)?,
        };
        if global.metadata().map_err(unavailable)?.len() != 0 {
            return Err(Error::StorageUnavailable);
        }
        dir.sync_all().map_err(unavailable)?;
        let g = global.metadata().map_err(unavailable)?;
        Ok(Self {
            path: path.to_owned(),
            parent_path,
            parent_identity,
            dir_identity,
            parent,
            dir,
            global,
            global_identity: (g.dev(), g.ino()),
            uid,
            epoch,
            binding: binding.into(),
            boot: boot_id()?,
        })
    }
    fn validate(&self) -> Result<(), Error> {
        if boot_id()? != self.boot
            || identity(&private_directory(open_directory(&self.path)?, self.uid)?)?
                != self.dir_identity
            || identity(&private_directory(
                open_directory(&self.parent_path)?,
                self.uid,
            )?)? != self.parent_identity
            || identity(&self.dir)? != self.dir_identity
            || identity(&self.parent)? != self.parent_identity
        {
            return Err(Error::StorageUnavailable);
        }
        let current = open_regular(&self.dir, DIRECTORY_LOCK, self.uid, false)?;
        let c = current.metadata().map_err(unavailable)?;
        let g = self.global.metadata().map_err(unavailable)?;
        if (c.dev(), c.ino()) != self.global_identity
            || (g.dev(), g.ino()) != self.global_identity
            || c.len() != 0
            || g.len() != 0
        {
            return Err(Error::StorageUnavailable);
        }
        Ok(())
    }
    fn locked_until<T>(
        &self,
        readonly: bool,
        deadline: Instant,
        cancellation: &CancellationToken,
        action: impl FnOnce() -> Result<T, Error>,
    ) -> Result<T, Error> {
        within(deadline, cancellation)?;
        // Each operation needs its own open-file description: sharing a flock FD
        // would let one reader's unlock release another reader's protection.
        let global = open_regular(&self.dir, DIRECTORY_LOCK, self.uid, false)?;
        let meta = global.metadata().map_err(unavailable)?;
        if (meta.dev(), meta.ino()) != self.global_identity || meta.len() != 0 {
            return Err(Error::StorageUnavailable);
        }
        let operation = if readonly {
            FlockOperation::NonBlockingLockShared
        } else {
            FlockOperation::NonBlockingLockExclusive
        };
        loop {
            within(deadline, cancellation)?;
            match fs::flock(&global, operation) {
                Ok(()) => break,
                Err(rustix::io::Errno::WOULDBLOCK) => {
                    // This is an owned blocking worker, never a Tokio thread.
                    std::thread::sleep(Duration::from_millis(1));
                }
                Err(error) => return Err(unavailable(error)),
            }
        }
        struct Unlock(File);
        impl Drop for Unlock {
            fn drop(&mut self) {
                let _ = fs::flock(&self.0, FlockOperation::NonBlockingUnlock);
            }
        }
        let _unlock = Unlock(global);
        within(deadline, cancellation)?;
        self.validate()?;
        within(deadline, cancellation)?;
        let value = action()?;
        within(deadline, cancellation)?;
        Ok(value)
    }
    #[cfg(test)]
    fn locked<T>(&self, action: impl FnOnce() -> Result<T, Error>) -> Result<T, Error> {
        self.locked_until(
            false,
            Instant::now() + BUDGET,
            &CancellationToken::new(),
            action,
        )
    }
    fn valid_record(&self, name: &str, record: &Record) -> bool {
        record.schema_version == 1
            && record.managed_epoch == self.epoch
            && record.binding_sha256 == self.binding
            && record.parent == self.parent_identity
            && record.directory == self.dir_identity
            && record.boot_id == self.boot
            && valid_key(&record.key)
            && !record.operation_id.is_nil()
            && record.pid > 0
            && record.process_start_ticks > 0
            && name == format!("{}.{}.json", record.key, record.operation_id)
    }
    fn scan(
        &self,
        own: Option<(&str, &File, &Record)>,
        same_key: Option<&str>,
        retire_clean: bool,
    ) -> Result<(), Error> {
        let mut names = Vec::new();
        let mut entries = fs::Dir::read_from(&self.dir).map_err(unavailable)?;
        while let Some(entry) = entries.read() {
            let entry = entry.map_err(unavailable)?;
            let name = entry.file_name().to_str().map_err(unavailable)?;
            if matches!(name, "." | ".." | DIRECTORY_LOCK) {
                continue;
            }
            if name.len() > 128 || names.len() >= GUARDS {
                return Err(Error::StorageUnavailable);
            }
            names.push(name.to_owned());
        }
        names.sort();
        let mut clean = Vec::new();
        let mut active = 0;
        let mut own_seen = own.is_none();
        for name in names {
            let file = open_regular(&self.dir, &name, self.uid, false)?;
            let record = read_record(&file)?;
            if !self.valid_record(&name, &record) {
                return Err(Error::StorageUnavailable);
            }
            if let Some((own_name, own_file, own_record)) = own
                && name == own_name
            {
                let a = file.metadata().map_err(unavailable)?;
                let b = own_file.metadata().map_err(unavailable)?;
                if (a.dev(), a.ino()) != (b.dev(), b.ino())
                    || record != *own_record
                    || record.state != State::ActiveUnclean
                {
                    return Err(Error::StorageUnavailable);
                }
                own_seen = true;
                continue;
            }
            match fs::flock(&file, FlockOperation::NonBlockingLockExclusive) {
                Ok(()) => {
                    if record.state != State::ClosedClean {
                        return Err(Error::StorageUnavailable);
                    }
                    if retire_clean {
                        clean.push(name);
                    }
                }
                Err(rustix::io::Errno::WOULDBLOCK) => {
                    active += 1;
                    if same_key == Some(record.key.as_str()) {
                        return Err(Error::Busy);
                    }
                }
                Err(error) => return Err(unavailable(error)),
            }
        }
        if !own_seen {
            return Err(Error::StorageUnavailable);
        }
        if retire_clean && active >= GUARDS {
            return Err(Error::StorageUnavailable);
        }
        // No stale/invalid file is ever removed. Scan the entire directory first.
        for name in clean {
            fs::unlinkat(&self.dir, name, AtFlags::empty()).map_err(unavailable)?;
        }
        if retire_clean {
            self.dir.sync_all().map_err(unavailable)?;
        }
        Ok(())
    }
}
struct Shared {
    directory: Directory,
    file: File,
    name: String,
    record: Record,
    poisoned: AtomicBool,
    closing: AtomicBool,
    release_attempted: AtomicBool,
    admission: Mutex<()>,
    slots: Arc<Semaphore>,
    waiters: Semaphore,
    active: AtomicU64,
    completed: Notify,
}
impl Shared {
    fn poison(&self) {
        self.poisoned.store(true, Ordering::SeqCst);
        // Stable, never-duplicated FD stays alive for this Shared's lifetime.
        // Revocation cannot wait for SSD writes or a blocked scan mutex.
        let _ = fs::flock(&self.file, FlockOperation::NonBlockingUnlock);
    }
    fn check_sync(&self, deadline: Instant, cancellation: &CancellationToken) -> Result<(), Error> {
        within(deadline, cancellation)?;
        if self.poisoned.load(Ordering::Acquire) || self.closing.load(Ordering::Acquire) {
            return Err(Error::StorageUnavailable);
        }
        self.directory
            .locked_until(true, deadline, cancellation, || {
                self.directory
                    .scan(Some((&self.name, &self.file, &self.record)), None, false)
            })?;
        if self.poisoned.load(Ordering::Acquire) || self.closing.load(Ordering::Acquire) {
            return Err(Error::StorageUnavailable);
        }
        within(deadline, cancellation)
    }
    async fn drain(&self) {
        loop {
            let notified = self.completed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.active.load(Ordering::Acquire) == 0 {
                return;
            }
            notified.await;
        }
    }
}

/// A strict process lifecycle guard. A dirty drop requires separate operator audit.
#[derive(Clone)]
pub struct RuntimeGuard {
    shared: Arc<Shared>,
}
impl RuntimeGuard {
    /// Acquire before opening any mutable blob/PG component. The caller supplies
    /// the reviewed strict epoch and complete primary/replica root-binding SHA.
    pub async fn acquire(
        path: &Path,
        epoch: Uuid,
        key: &str,
        binding_sha256: &str,
    ) -> Result<Arc<Self>, Error> {
        if epoch.is_nil() || !valid_key(key) || !valid_binding(binding_sha256) {
            return Err(Error::InvalidInput);
        }
        let deadline = Instant::now() + BUDGET;
        let cancellation = CancellationToken::new();
        let _cancel_on_drop = cancellation.clone().drop_guard();
        let path = path.to_owned();
        let key = key.to_owned();
        let binding = binding_sha256.to_owned();
        let permit = STARTUPS.try_acquire().map_err(|_| Error::Busy)?;
        let task = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            within(deadline, &cancellation)?;
            let directory = Directory::open(&path, epoch, &binding)?;
            directory
                .locked_until(false, deadline, &cancellation, || {
                    directory.scan(None, Some(&key), true)?;
                    within(deadline, &cancellation)?;
                    let record = Record {
                        schema_version: 1,
                        managed_epoch: epoch,
                        binding_sha256: binding,
                        key,
                        operation_id: nonce()?,
                        parent: directory.parent_identity.clone(),
                        directory: directory.dir_identity.clone(),
                        boot_id: directory.boot,
                        pid: std::process::id(),
                        process_start_ticks: start_ticks()?,
                        state: State::ActiveUnclean,
                    };
                    let name = format!("{}.{}.json", record.key, record.operation_id);
                    let file = open_regular(&directory.dir, &name, directory.uid, true)?;
                    fs::flock(&file, FlockOperation::NonBlockingLockExclusive)
                        .map_err(unavailable)?;
                    write_record(&file, &record)?;
                    directory.dir.sync_all().map_err(unavailable)?;
                    Ok((name, file, record))
                })
                .map(|(name, file, record)| {
                    Arc::new(Self {
                        shared: Arc::new(Shared {
                            directory,
                            file,
                            name,
                            record,
                            poisoned: AtomicBool::new(false),
                            closing: AtomicBool::new(false),
                            release_attempted: AtomicBool::new(false),
                            admission: Mutex::new(()),
                            slots: Arc::new(Semaphore::new(4)),
                            waiters: Semaphore::new(128),
                            active: AtomicU64::new(0),
                            completed: Notify::new(),
                        }),
                    })
                })
        });
        let result = tokio::time::timeout_at(deadline, task)
            .await
            .map_err(|_| Error::StorageUnavailable)?
            .map_err(unavailable)?;
        if Instant::now() >= deadline {
            return Err(Error::StorageUnavailable);
        }
        result
    }
    pub fn fence(self: &Arc<Self>) -> Arc<dyn DurabilityFence> {
        self.clone()
    }
    /// Immediate process-independent revocation, including while SSD I/O stalls.
    pub fn mark_uncertain(&self) {
        self.shared.poison();
    }
    pub fn active_jobs(&self) -> u64 {
        self.shared.active.load(Ordering::Acquire)
    }
    /// Wait for this guard's actual physical reads to finish; does not admit work.
    pub fn drain(&self) -> BoxFuture<'static, Result<(), Error>> {
        let shared = self.shared.clone();
        Box::pin(async move {
            tokio::time::timeout(BUDGET, shared.drain())
                .await
                .map_err(|_| Error::StorageUnavailable)
        })
    }
    /// Irreversible assertion by the root lifecycle owner: ALL application work,
    /// mutation callbacks, stores and blobs have already drained, overall result
    /// is Ok, and no future mutation may use this guard. Never call on failure.
    pub async fn release_clean(&self) -> Result<(), Error> {
        let deadline = Instant::now() + BUDGET;
        let cancellation = CancellationToken::new();
        let _cancel_on_drop = cancellation.clone().drop_guard();
        {
            let _lock = self
                .shared
                .admission
                .lock()
                .map_err(|_| Error::StorageUnavailable)?;
            if self.shared.poisoned.load(Ordering::Acquire)
                || self.shared.release_attempted.swap(true, Ordering::AcqRel)
            {
                return Err(Error::StorageUnavailable);
            }
            self.shared.closing.store(true, Ordering::Release);
            self.shared.slots.close();
        }
        if tokio::time::timeout_at(deadline, self.shared.drain())
            .await
            .is_err()
        {
            self.mark_uncertain();
            return Err(Error::StorageUnavailable);
        }
        let shared = self.shared.clone();
        let permit = match STARTUPS.try_acquire() {
            Ok(permit) => permit,
            Err(_) => {
                self.mark_uncertain();
                return Err(Error::StorageUnavailable);
            }
        };
        let task = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            shared
                .directory
                .locked_until(false, deadline, &cancellation, || {
                    if shared.poisoned.load(Ordering::Acquire) {
                        return Err(Error::StorageUnavailable);
                    }
                    shared.directory.scan(
                        Some((&shared.name, &shared.file, &shared.record)),
                        None,
                        false,
                    )?;
                    if shared.poisoned.load(Ordering::Acquire) {
                        return Err(Error::StorageUnavailable);
                    }
                    within(deadline, &cancellation)?;
                    let mut record = shared.record.clone();
                    record.state = State::ClosedClean;
                    write_record(&shared.file, &record)?;
                    if shared.poisoned.load(Ordering::Acquire) {
                        // This cannot represent genuinely pending mutation callbacks:
                        // the caller's clean assertion forbids that race. Restore
                        // active evidence on observed misuse rather than clean it.
                        let _ = write_record(&shared.file, &shared.record);
                        return Err(Error::StorageUnavailable);
                    }
                    shared.directory.dir.sync_all().map_err(unavailable)?;
                    if shared.poisoned.load(Ordering::Acquire) {
                        let _ = write_record(&shared.file, &shared.record);
                        return Err(Error::StorageUnavailable);
                    }
                    fs::flock(&shared.file, FlockOperation::NonBlockingUnlock)
                        .map_err(unavailable)?;
                    Ok(())
                })
        });
        let result = tokio::time::timeout_at(deadline, task)
            .await
            .map_err(|_| Error::StorageUnavailable)
            .and_then(|r| r.map_err(unavailable))
            .and_then(|r| r);
        if self.shared.poisoned.load(Ordering::Acquire) || Instant::now() >= deadline {
            self.mark_uncertain();
            return Err(Error::StorageUnavailable);
        }
        if result.is_err() {
            self.mark_uncertain();
        }
        result
    }
}
impl DurabilityFence for RuntimeGuard {
    fn mark_uncertain(&self) {
        self.shared.poison();
    }
    fn check(
        &self,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> BoxFuture<'static, Result<(), Error>> {
        let shared = self.shared.clone();
        Box::pin(async move {
            if cancellation.is_cancelled() {
                return Err(Error::Cancelled);
            }
            let deadline = deadline.min(Instant::now() + BUDGET);
            if deadline <= Instant::now() {
                return Err(Error::StorageUnavailable);
            }
            let _waiter = shared.waiters.try_acquire().map_err(|_| Error::Busy)?;
            let permit = tokio::select! { biased;
                _ = cancellation.cancelled() => return Err(Error::Cancelled),
                result = tokio::time::timeout_at(deadline, shared.slots.clone().acquire_owned()) => {
                    result.map_err(|_| Error::StorageUnavailable)?.map_err(unavailable)?
                }
            };
            within(deadline, &cancellation)?;
            {
                let _lock = shared
                    .admission
                    .lock()
                    .map_err(|_| Error::StorageUnavailable)?;
                if shared.poisoned.load(Ordering::Acquire) || shared.closing.load(Ordering::Acquire)
                {
                    return Err(Error::StorageUnavailable);
                }
                shared.active.fetch_add(1, Ordering::AcqRel);
            }
            let worker_cancellation = cancellation.child_token();
            let _cancel_on_drop = worker_cancellation.clone().drop_guard();
            let worker = shared.clone();
            let task = tokio::task::spawn_blocking(move || {
                struct Finished(Arc<Shared>);
                impl Drop for Finished {
                    fn drop(&mut self) {
                        self.0.active.fetch_sub(1, Ordering::AcqRel);
                        self.0.completed.notify_waiters();
                    }
                }
                let _finished = Finished(worker.clone());
                let _permit = permit;
                worker.check_sync(deadline, &worker_cancellation)
            });
            tokio::select! {biased;
                _=cancellation.cancelled()=>Err(Error::Cancelled),
                result=tokio::time::timeout_at(deadline,task)=>{
                    let result=result.map_err(|_|Error::StorageUnavailable).and_then(|v|v.map_err(unavailable)).and_then(|v|v);
                    within(deadline, &cancellation)?;
                    if shared.poisoned.load(Ordering::Acquire)||shared.closing.load(Ordering::Acquire) {Err(Error::StorageUnavailable)}else{result}
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        fs,
        os::unix::fs::{PermissionsExt, symlink},
        sync::{Condvar, mpsc},
    };
    const BINDING: &str = "1111111111111111111111111111111111111111111111111111111111111111";
    fn root() -> tempfile::TempDir {
        let dir = tempfile::Builder::new()
            .prefix("openlegal-runtime-guard-")
            .tempdir_in(std::env::temp_dir())
            .unwrap();
        fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o700)).unwrap();
        dir
    }
    fn epoch() -> Uuid {
        Uuid::from_u128(1)
    }
    async fn acquire(root: &Path, key: &str) -> Arc<RuntimeGuard> {
        RuntimeGuard::acquire(&root.join(LEAF), epoch(), key, BINDING)
            .await
            .unwrap()
    }
    async fn check(guard: &RuntimeGuard) -> Result<(), Error> {
        guard
            .check(Instant::now() + BUDGET, CancellationToken::new())
            .await
    }
    fn record_path(guard: &RuntimeGuard) -> PathBuf {
        guard.shared.directory.path.join(&guard.shared.name)
    }
    struct Release(Arc<(Mutex<bool>, Condvar)>);
    impl Drop for Release {
        fn drop(&mut self) {
            let (lock, ready) = &*self.0;
            *lock.lock().unwrap() = true;
            ready.notify_all();
        }
    }
    #[test]
    fn guard_and_record_media_must_match_the_bound_replica_filesystem() {
        let root = Identity {
            dev: 1,
            ino: 10,
            fsid: 100,
        };
        assert!(same_filesystem(
            &root,
            &Identity {
                dev: 1,
                ino: 11,
                fsid: 100
            }
        ));
        assert!(!same_filesystem(
            &root,
            &Identity {
                dev: 2,
                ino: 11,
                fsid: 100
            }
        ));
        assert!(!same_filesystem(
            &root,
            &Identity {
                dev: 1,
                ino: 11,
                fsid: 200
            }
        ));
    }
    #[test]
    fn keys_are_strict_and_include_administrative_writers() {
        for key in [
            "serve",
            "scheduler",
            "admin.maintain",
            "admin.rebuild",
            "collection.00000000-0000-0000-0000-000000000001.0",
            "collection.00000000-0000-0000-0000-000000000001.123",
        ] {
            assert!(valid_key(key), "{key}");
        }
        for key in [
            "",
            "../serve",
            "serve/1",
            "admin",
            "collection.00000000-0000-0000-0000-000000000000.1",
            "collection.00000000-0000-0000-0000-000000000001.01",
            "collection.00000000-0000-0000-0000-000000000001.-1",
        ] {
            assert!(!valid_key(key), "{key}");
        }
    }
    #[tokio::test]
    async fn active_peers_are_valid_but_a_logical_key_cannot_overlap() {
        let _io = crate::blob::test_io_admission().await;
        let root = root();
        let server = acquire(root.path(), "serve").await;
        let scheduler = acquire(root.path(), "scheduler").await;
        assert_eq!(check(&server).await, Ok(()));
        assert_eq!(check(&scheduler).await, Ok(()));
        assert!(matches!(
            RuntimeGuard::acquire(&root.path().join(LEAF), epoch(), "serve", BINDING).await,
            Err(Error::Busy)
        ));
        server.release_clean().await.unwrap();
        scheduler.release_clean().await.unwrap();
    }
    #[tokio::test]
    async fn dirty_drop_blocks_restart_and_already_running_peer() {
        let _io = crate::blob::test_io_admission().await;
        let root = root();
        let server = acquire(root.path(), "serve").await;
        let scheduler = acquire(root.path(), "scheduler").await;
        let path = record_path(&server);
        drop(server);
        assert_eq!(check(&scheduler).await, Err(Error::StorageUnavailable));
        assert!(matches!(
            RuntimeGuard::acquire(&root.path().join(LEAF), epoch(), "serve", BINDING).await,
            Err(Error::StorageUnavailable)
        ));
        assert!(path.exists());
        assert_eq!(
            read_record(&File::open(path).unwrap()).unwrap().state,
            State::ActiveUnclean
        );
    }
    #[tokio::test]
    async fn clean_release_is_explicit_and_only_clean_records_can_be_retired() {
        let _io = crate::blob::test_io_admission().await;
        let root = root();
        let first = acquire(root.path(), "serve").await;
        let path = record_path(&first);
        first.release_clean().await.unwrap();
        assert!(path.exists());
        assert_eq!(
            read_record(&File::open(&path).unwrap()).unwrap().state,
            State::ClosedClean
        );
        assert_eq!(check(&first).await, Err(Error::StorageUnavailable));
        let second = acquire(root.path(), "serve").await;
        assert!(!path.exists());
        assert_ne!(
            second.shared.record.operation_id,
            first.shared.record.operation_id
        );
        assert_eq!(check(&second).await, Ok(()));
        second.release_clean().await.unwrap();
    }
    #[tokio::test]
    async fn poison_unlocks_without_filesystem_writes_and_can_never_clean() {
        let _io = crate::blob::test_io_admission().await;
        let root = root();
        let guard = acquire(root.path(), "serve").await;
        let path = record_path(&guard);
        let before = fs::read(&path).unwrap();
        guard.mark_uncertain();
        assert_eq!(fs::read(&path).unwrap(), before);
        let probe = open_regular(
            &guard.shared.directory.dir,
            &guard.shared.name,
            guard.shared.directory.uid,
            false,
        )
        .unwrap();
        rustix::fs::flock(&probe, FlockOperation::NonBlockingLockExclusive).unwrap();
        drop(probe);
        assert_eq!(check(&guard).await, Err(Error::StorageUnavailable));
        assert_eq!(guard.release_clean().await, Err(Error::StorageUnavailable));
        assert_eq!(
            read_record(&File::open(path).unwrap()).unwrap().state,
            State::ActiveUnclean
        );
    }
    #[tokio::test]
    async fn poison_is_immediate_while_a_directory_scan_is_blocked() {
        let _io = crate::blob::test_io_admission().await;
        let root = root();
        let guard = acquire(root.path(), "serve").await;
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        let release = Release(gate.clone());
        let worker_gate = gate.clone();
        let shared = guard.shared.clone();
        let (entered, ready) = mpsc::channel();
        let worker = std::thread::spawn(move || {
            shared.directory.locked(|| {
                entered.send(()).unwrap();
                let (lock, wake) = &*worker_gate;
                let mut done = lock.lock().unwrap();
                while !*done {
                    done = wake.wait(done).unwrap();
                }
                Ok(())
            })
        });
        ready.recv_timeout(Duration::from_secs(2)).unwrap();
        guard.mark_uncertain();
        let probe = open_regular(
            &guard.shared.directory.dir,
            &guard.shared.name,
            guard.shared.directory.uid,
            false,
        )
        .unwrap();
        rustix::fs::flock(&probe, FlockOperation::NonBlockingLockExclusive).unwrap();
        drop(probe);
        drop(release);
        worker.join().unwrap().unwrap();
        assert_eq!(check(&guard).await, Err(Error::StorageUnavailable));
    }
    #[tokio::test]
    async fn active_or_clean_records_of_another_epoch_or_binding_are_not_ignored() {
        let _io = crate::blob::test_io_admission().await;
        let root = root();
        let guard = acquire(root.path(), "serve").await;
        assert!(matches!(
            RuntimeGuard::acquire(
                &root.path().join(LEAF),
                Uuid::from_u128(2),
                "scheduler",
                BINDING
            )
            .await,
            Err(Error::StorageUnavailable)
        ));
        assert!(matches!(
            RuntimeGuard::acquire(
                &root.path().join(LEAF),
                epoch(),
                "scheduler",
                &"2".repeat(64)
            )
            .await,
            Err(Error::StorageUnavailable)
        ));
        guard.release_clean().await.unwrap();
        assert!(matches!(
            RuntimeGuard::acquire(
                &root.path().join(LEAF),
                Uuid::from_u128(2),
                "serve",
                BINDING
            )
            .await,
            Err(Error::StorageUnavailable)
        ));
    }
    #[tokio::test]
    async fn malformed_and_linked_records_refuse_without_removal() {
        let _io = crate::blob::test_io_admission().await;
        let root = root();
        let guard = acquire(root.path(), "serve").await;
        let dir = &guard.shared.directory.path;
        let outside = root.path().join("not-a-control-file");
        fs::write(&outside, b"preserved").unwrap();
        symlink(&outside, dir.join("bad.json")).unwrap();
        assert_eq!(check(&guard).await, Err(Error::StorageUnavailable));
        assert_eq!(fs::read(&outside).unwrap(), b"preserved");
        fs::remove_file(dir.join("bad.json")).unwrap();
        fs::write(
            dir.join("bad.json"),
            b"{\"schema_version\":1,\"schema_version\":1}",
        )
        .unwrap();
        fs::set_permissions(dir.join("bad.json"), fs::Permissions::from_mode(0o600)).unwrap();
        assert_eq!(check(&guard).await, Err(Error::StorageUnavailable));
        assert!(dir.join("bad.json").exists());
    }
    #[tokio::test]
    async fn directory_or_global_lock_rebinding_fails_closed() {
        let _io = crate::blob::test_io_admission().await;
        let root = root();
        let guard = acquire(root.path(), "serve").await;
        let global = guard.shared.directory.path.join(DIRECTORY_LOCK);
        fs::rename(&global, guard.shared.directory.path.join("old-lock")).unwrap();
        fs::write(&global, b"").unwrap();
        fs::set_permissions(&global, fs::Permissions::from_mode(0o600)).unwrap();
        assert_eq!(check(&guard).await, Err(Error::StorageUnavailable));
        assert_eq!(guard.release_clean().await, Err(Error::StorageUnavailable));
        assert_eq!(
            read_record(&File::open(record_path(&guard)).unwrap())
                .unwrap()
                .state,
            State::ActiveUnclean
        );
    }
    #[tokio::test]
    async fn own_original_metadata_is_immutable_even_when_the_lock_is_held() {
        let _io = crate::blob::test_io_admission().await;
        let root = root();
        let guard = acquire(root.path(), "serve").await;
        let mut changed = guard.shared.record.clone();
        changed.process_start_ticks += 1;
        write_record(&guard.shared.file, &changed).unwrap();
        assert_eq!(check(&guard).await, Err(Error::StorageUnavailable));
        assert_eq!(guard.release_clean().await, Err(Error::StorageUnavailable));
        assert_eq!(
            read_record(&File::open(record_path(&guard)).unwrap())
                .unwrap()
                .state,
            State::ActiveUnclean
        );
    }
    #[tokio::test]
    async fn oversized_guard_directory_is_bounded_and_preserved() {
        let _io = crate::blob::test_io_admission().await;
        let root = root();
        let guard = acquire(root.path(), "serve").await;
        for n in 0..GUARDS {
            let path = guard.shared.directory.path.join(format!("invalid-{n}"));
            fs::write(&path, b"").unwrap();
        }
        assert_eq!(check(&guard).await, Err(Error::StorageUnavailable));
        assert_eq!(
            fs::read_dir(&guard.shared.directory.path).unwrap().count(),
            GUARDS + 2
        );
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_healthy_checks_queue_and_preserve_active_peers() {
        let _io = crate::blob::test_io_admission().await;
        let root = root();
        let server = acquire(root.path(), "serve").await;
        let scheduler = acquire(root.path(), "scheduler").await;
        let mut requests = Vec::new();
        for index in 0..64 {
            let guard = if index % 2 == 0 {
                server.clone()
            } else {
                scheduler.clone()
            };
            requests.push(tokio::spawn(async move { check(&guard).await }));
        }
        for request in requests {
            assert_eq!(request.await.unwrap(), Ok(()));
        }
        assert!(!server.shared.poisoned.load(Ordering::Acquire));
        assert!(!scheduler.shared.poisoned.load(Ordering::Acquire));
        check(&server).await.unwrap();
        check(&scheduler).await.unwrap();
        server.release_clean().await.unwrap();
        scheduler.release_clean().await.unwrap();
    }
    #[tokio::test]
    async fn healthy_exclusive_contention_waits_instead_of_refusing() {
        let _io = crate::blob::test_io_admission().await;
        let root = root();
        let guard = acquire(root.path(), "serve").await;
        let shared = guard.shared.clone();
        let (entered, ready) = tokio::sync::oneshot::channel();
        let worker = tokio::task::spawn_blocking(move || {
            shared.directory.locked(|| {
                entered.send(()).unwrap();
                std::thread::sleep(Duration::from_millis(40));
                Ok(())
            })
        });
        ready.await.unwrap();
        assert_eq!(check(&guard).await, Ok(()));
        worker.await.unwrap().unwrap();
        guard.release_clean().await.unwrap();
    }
    #[tokio::test]
    async fn physical_scan_never_accepts_a_late_or_cancelled_proof() {
        let _io = crate::blob::test_io_admission().await;
        let root = root();
        let guard = acquire(root.path(), "serve").await;
        let shared = guard.shared.clone();
        tokio::task::spawn_blocking(move || {
            let cancellation = CancellationToken::new();
            let result = shared.directory.locked_until(
                true,
                Instant::now() + Duration::from_millis(10),
                &cancellation,
                || {
                    std::thread::sleep(Duration::from_millis(30));
                    Ok(())
                },
            );
            assert_eq!(result, Err(Error::StorageUnavailable));
            let worker_cancel = cancellation.clone();
            let result =
                shared
                    .directory
                    .locked_until(true, Instant::now() + BUDGET, &cancellation, || {
                        worker_cancel.cancel();
                        Ok(())
                    });
            assert_eq!(result, Err(Error::Cancelled));
        })
        .await
        .unwrap();
        check(&guard).await.unwrap();
        guard.release_clean().await.unwrap();
    }
    #[test]
    fn cancelled_queued_check_retains_ownership_until_actual_worker_completion() {
        let _io = crate::blob::test_io_admission_blocking();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .max_blocking_threads(1)
            .build()
            .unwrap();
        runtime.block_on(async {
            let root = root();
            let guard = acquire(root.path(), "serve").await;
            let gate = Arc::new((Mutex::new(false), Condvar::new()));
            let release = Release(gate.clone());
            let (entered, ready) = tokio::sync::oneshot::channel();
            let blocker = tokio::task::spawn_blocking(move || {
                let _ = entered.send(());
                let (lock, wake) = &*gate;
                let mut done = lock.lock().unwrap();
                while !*done {
                    done = wake.wait(done).unwrap();
                }
            });
            ready.await.unwrap();
            let cancellation = CancellationToken::new();
            let pending = tokio::spawn(guard.check(Instant::now() + BUDGET, cancellation.clone()));
            tokio::time::timeout(BUDGET, async {
                while guard.active_jobs() != 1 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            cancellation.cancel();
            assert_eq!(pending.await.unwrap(), Err(Error::Cancelled));
            assert_eq!(guard.active_jobs(), 1);
            assert_eq!(
                read_record(&File::open(record_path(&guard)).unwrap())
                    .unwrap()
                    .state,
                State::ActiveUnclean
            );
            drop(release);
            blocker.await.unwrap();
            guard.drain().await.unwrap();
            assert_eq!(guard.active_jobs(), 0);
            guard.release_clean().await.unwrap();
        });
    }
    #[test]
    fn subprocess_fixture_unclean_exit() {
        let Ok(path) = std::env::var("OPENLEGAL_RUNTIME_GUARD_SUBPROCESS_FIXTURE") else {
            return;
        };
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let _guard = runtime
            .block_on(RuntimeGuard::acquire(
                Path::new(&path),
                epoch(),
                "serve",
                BINDING,
            ))
            .unwrap();
        // Real process death releases the flock without any Rust Drop cleanup.
        std::process::exit(0);
    }
    #[tokio::test]
    async fn another_process_unclean_exit_is_seen_by_existing_and_new_processes() {
        let _io = crate::blob::test_io_admission().await;
        let root = root();
        let scheduler = acquire(root.path(), "scheduler").await;
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "runtime_guard::tests::subprocess_fixture_unclean_exit",
                "--nocapture",
            ])
            .env(
                "OPENLEGAL_RUNTIME_GUARD_SUBPROCESS_FIXTURE",
                root.path().join(LEAF),
            )
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap();
        let until = std::time::Instant::now() + Duration::from_secs(30);
        let status = loop {
            if let Some(status) = child.try_wait().unwrap() {
                break status;
            }
            if std::time::Instant::now() >= until {
                let _ = child.kill();
                let _ = child.wait();
                panic!("owned guard fixture deadline");
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        };
        assert!(status.success());
        assert_eq!(check(&scheduler).await, Err(Error::StorageUnavailable));
        assert!(matches!(
            RuntimeGuard::acquire(&root.path().join(LEAF), epoch(), "serve", BINDING).await,
            Err(Error::StorageUnavailable)
        ));
    }
}
