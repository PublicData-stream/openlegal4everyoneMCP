//! Filesystem mechanics. This module executes only in the isolated child.
use super::protocol::{self, Payload, Reply, Request, Response};
use openlegal_application::{
    MAX_PROCESSED_BYTES, MAX_RAW_BYTES,
    persistence::{HistoryKey, PersistentKey, RetentionPolicy, StorageMetrics},
};
use openlegal_domain::{
    Query, RetrievalData, RetrievalError as Error,
    history::{
        SnapshotEnvelope, SnapshotPage, SnapshotReference, SnapshotSummary, valid_snapshot_id,
    },
};
use rustix::fs::{self, AtFlags, FlockOperation, Mode, OFlags};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{HashMap, HashSet},
    fs::File,
    io::{Read, Write},
    os::unix::fs::MetadataExt,
    path::{Component, Path},
};

const MAX_KEYS: usize = 4096;
const MAX_SNAPSHOTS: usize = 10_000;
const MAX_FILES: usize = MAX_KEYS + MAX_SNAPSHOTS + 64;
const MAX_QUARANTINE_BYTES: u64 = 16 * 1024 * 1024;
const MAX_QUARANTINE_FILES: usize = 32;
const FORMAT: &[u8] = b"openlegal-filesystem-cache-v1\n";
#[cfg(test)]
thread_local! { static FAIL_AFTER:std::cell::Cell<Option<usize>>=const{std::cell::Cell::new(None)}; }
fn checkpoint() -> Result<(), Error> {
    #[cfg(test)]
    {
        FAIL_AFTER.with(|value| match value.get() {
            Some(0) => Err(Error::StorageUnavailable),
            Some(n) => {
                value.set(Some(n - 1));
                Ok(())
            }
            None => Ok(()),
        })
    }
    #[cfg(not(test))]
    Ok(())
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Entry {
    summary: SnapshotSummary,
    bytes: u64,
    file_sha256: String,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Head {
    id: String,
    processor_version: String,
    schema_version: u32,
    validated_at: u64,
    retrieved_at: u64,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    format: u32,
    key: HistoryKey,
    incarnation: String,
    next_sequence: u64,
    heads: Vec<Head>,
    entries: Vec<Entry>,
}
impl Manifest {
    fn head(&self, key: &PersistentKey) -> Option<&Head> {
        self.heads.iter().find(|h| {
            h.processor_version == key.processor_version && h.schema_version == key.schema_version
        })
    }
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CheckedManifest {
    digest: String,
    manifest: Manifest,
}
fn manifest_bytes(manifest: &Manifest) -> Result<Vec<u8>, Error> {
    let bytes = serde_json::to_vec(manifest).map_err(unavailable)?;
    serde_json::to_vec(&CheckedManifest {
        digest: digest(&bytes),
        manifest: manifest.clone(),
    })
    .map_err(unavailable)
}
fn parse_manifest(bytes: &[u8]) -> Result<Manifest, Error> {
    let checked: CheckedManifest =
        serde_json::from_slice(bytes).map_err(|_| Error::StorageCorrupt)?;
    if digest(&serde_json::to_vec(&checked.manifest).map_err(unavailable)?) != checked.digest {
        return Err(Error::StorageCorrupt);
    }
    Ok(checked.manifest)
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Snapshot {
    key: PersistentKey,
    summary: SnapshotSummary,
    payload: Payload,
}
pub(super) struct Engine {
    root: File,
    _lock: File,
    policy: RetentionPolicy,
    manifests: HashMap<String, Manifest>,
    bad: HashSet<String>,
    metrics: StorageMetrics,
    staged: Option<(String, Manifest, Payload)>,
    staged_maintenance: Option<u64>,
    staged_isolation: Option<(String, Option<String>, bool, u64)>,
    staged_publish: Option<(PersistentKey, Payload, u64)>,
    quarantine: Vec<(u64, String, u64)>,
    orphan_cleanup: bool,
}
fn quarantine_time(name: &str) -> Option<u64> {
    let rest = name.strip_prefix("q-")?.strip_suffix(".quarantine")?;
    let (at, id) = rest.split_once('-')?;
    if at.len() != 20 || !valid_snapshot_id(id) {
        return None;
    }
    at.parse().ok()
}

fn unavailable(_: impl std::fmt::Debug) -> Error {
    Error::StorageUnavailable
}
fn digest(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|v| format!("{v:02x}"))
        .collect()
}
fn key_id(key: &HistoryKey) -> Result<String, Error> {
    Ok(digest(&serde_json::to_vec(key).map_err(unavailable)?))
}
fn regular(file: File) -> Result<File, Error> {
    let meta = file.metadata().map_err(unavailable)?;
    if !meta.is_file()
        || meta.nlink() != 1
        || meta.uid() != rustix::process::geteuid().as_raw()
        || meta.mode() & 0o777 != 0o600
    {
        return Err(Error::StorageCorrupt);
    }
    Ok(file)
}
fn open_file(root: &File, name: &str, create: bool) -> Result<File, Error> {
    let flags = if create {
        OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL
    } else {
        OFlags::RDONLY
    };
    regular(File::from(
        fs::openat(
            root,
            name,
            flags | OFlags::NOFOLLOW | OFlags::CLOEXEC | OFlags::NONBLOCK,
            Mode::from_raw_mode(0o600),
        )
        .map_err(unavailable)?,
    ))
}
fn root_directory(path: &Path) -> Result<File, Error> {
    if !path.is_absolute() || path.components().count() < 2 {
        return Err(Error::InvalidInput);
    }
    let mut root = File::from(
        fs::open(
            "/",
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(unavailable)?,
    );
    let components: Vec<_> = path.components().collect();
    for (index, component) in components.iter().enumerate() {
        match component {
            Component::RootDir => {}
            Component::Normal(name) => {
                if index + 1 == components.len() {
                    match fs::mkdirat(&root, *name, Mode::from_raw_mode(0o700)) {
                        Ok(()) => root.sync_all().map_err(unavailable)?,
                        Err(rustix::io::Errno::EXIST) => {}
                        Err(e) => return Err(unavailable(e)),
                    }
                }
                root = File::from(
                    fs::openat(
                        &root,
                        *name,
                        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                        Mode::empty(),
                    )
                    .map_err(unavailable)?,
                );
            }
            _ => return Err(Error::InvalidInput),
        }
    }
    let metadata = root.metadata().map_err(unavailable)?;
    if metadata.mode() & 0o777 != 0o700 || metadata.uid() != rustix::process::geteuid().as_raw() {
        return Err(Error::InvalidInput);
    }
    Ok(root)
}
fn names(root: &File) -> Result<Vec<String>, Error> {
    let mut out = Vec::new();
    for item in fs::Dir::read_from(root).map_err(unavailable)? {
        let item = item.map_err(unavailable)?;
        let name = item.file_name().to_str().map_err(unavailable)?;
        if name == "." || name == ".." {
            continue;
        }
        if out.len() >= MAX_FILES {
            return Err(Error::StorageCapacity);
        }
        out.push(name.to_owned());
    }
    Ok(out)
}
impl Engine {
    fn open(path: &Path, policy: RetentionPolicy) -> Result<Self, Error> {
        policy.validate()?;
        let root = root_directory(path)?;
        let lock = regular(File::from(
            fs::openat(
                &root,
                ".lock",
                OFlags::RDWR
                    | OFlags::CREATE
                    | OFlags::NOFOLLOW
                    | OFlags::CLOEXEC
                    | OFlags::NONBLOCK,
                Mode::from_raw_mode(0o600),
            )
            .map_err(unavailable)?,
        ))?;
        fs::flock(&lock, FlockOperation::NonBlockingLockExclusive).map_err(unavailable)?;
        let mut engine = Self {
            root,
            _lock: lock,
            policy,
            manifests: HashMap::new(),
            bad: HashSet::new(),
            metrics: StorageMetrics::default(),
            staged: None,
            staged_maintenance: None,
            staged_isolation: None,
            staged_publish: None,
            quarantine: Vec::new(),
            orphan_cleanup: false,
        };
        let entries = names(&engine.root)?;
        // Recovery may delete recognized staging/orphan names. Validate every
        // existing object first so cleanup never silently accepts an unsafe root.
        for name in &entries {
            drop(open_file(&engine.root, name, false)?);
        }
        if !entries.iter().any(|v| v == "FORMAT") {
            if entries
                .iter()
                .any(|v| v != ".lock" && v != ".staged-format")
            {
                return Err(Error::StorageCorrupt);
            }
            if entries.iter().any(|v| v == ".staged-format") {
                drop(open_file(&engine.root, ".staged-format", false)?);
                engine.unlink(".staged-format")?;
            }
            engine.write_new(".staged-format", FORMAT)?;
            fs::renameat(&engine.root, ".staged-format", &engine.root, "FORMAT")
                .map_err(unavailable)?;
            checkpoint()?;
            engine.root.sync_all().map_err(unavailable)?;
            checkpoint()?;
        } else if engine.read_bytes("FORMAT", 128)? != FORMAT {
            return Err(Error::StorageCorrupt);
        }
        for name in &entries {
            if let Some(id) = name.strip_suffix(".manifest") {
                if !valid_snapshot_id(id) || engine.manifests.len() + engine.bad.len() >= MAX_KEYS {
                    return Err(Error::StorageCapacity);
                }
                let result = engine
                    .read_bytes(name, protocol::MAX_FRAME)
                    .and_then(|v| parse_manifest(&v));
                match result {
                    Ok(manifest) if manifest.format != 1 => return Err(Error::StorageUnavailable),
                    Ok(manifest) if engine.valid_manifest(id, &manifest) => {
                        engine.manifests.insert(id.to_owned(), manifest);
                    }
                    _ => {
                        engine.bad.insert(id.to_owned());
                        engine.metrics.corruptions += 1;
                    }
                }
            } else if let Some(at) = quarantine_time(name) {
                let bytes = open_file(&engine.root, name, false)?
                    .metadata()
                    .map_err(unavailable)?
                    .len();
                engine.quarantine.push((at, name.clone(), bytes));
            } else if name != ".lock"
                && name != "FORMAT"
                && name != ".staged-format"
                && name != ".staged-manifest"
                && !(name.ends_with(".snapshot")
                    && valid_snapshot_id(name.trim_end_matches(".snapshot")))
            {
                return Err(Error::StorageCorrupt);
            }
        }
        // Unreferenced files are safe to remove only when every manifest is readable.
        if entries.iter().any(|v| v == ".staged-manifest") {
            engine.unlink(".staged-manifest")?;
        }
        if entries.iter().any(|v| v == ".staged-format") {
            engine.unlink(".staged-format")?;
        }
        if engine.bad.is_empty() {
            let referenced: HashSet<_> = engine
                .manifests
                .values()
                .flat_map(|m| {
                    m.entries
                        .iter()
                        .map(|e| format!("{}.snapshot", e.summary.snapshot_id))
                })
                .collect();
            for name in entries {
                if name.ends_with(".snapshot") && !referenced.contains(&name) {
                    engine.unlink(&name)?;
                }
            }
        }
        engine.root.sync_all().map_err(unavailable)?;
        engine.recount()?;
        engine.trim_quarantine(None)?;
        if engine.metrics.snapshots > MAX_SNAPSHOTS
            || engine
                .metrics
                .bytes
                .saturating_add(protocol::MAX_FRAME as u64)
                > engine.policy.max_bytes
            || engine
                .manifests
                .values()
                .any(|m| m.entries.len() > engine.policy.max_snapshots_per_query)
        {
            return Err(Error::StorageCapacity);
        }
        engine.metrics.recoveries = 1;
        Ok(engine)
    }
    fn valid_manifest(&self, id: &str, m: &Manifest) -> bool {
        m.format == 1
            && valid_snapshot_id(&m.incarnation)
            && key_id(&m.key).is_ok_and(|v| v == id)
            && validate_key(&m.key).is_ok()
            && m.entries.len() <= 1000
            && m.next_sequence > 0
            && {
                let mut ids = HashSet::new();
                let mut seq = 0;
                m.entries.iter().all(|e| {
                    let valid = valid_snapshot_id(&e.summary.snapshot_id)
                        && ids.insert(&e.summary.snapshot_id)
                        && e.summary.sequence > seq
                        && e.summary.sequence < m.next_sequence
                        && e.bytes <= protocol::MAX_FRAME as u64;
                    seq = e.summary.sequence;
                    valid
                })
            }
            && {
                let mut variants = HashSet::new();
                m.heads.len() <= m.entries.len()
                    && m.heads.iter().all(|h| {
                        variants.insert((&h.processor_version, h.schema_version))
                            && m.entries.iter().any(|e| {
                                e.summary.snapshot_id == h.id
                                    && e.summary.processor_version == h.processor_version
                                    && e.summary.schema_version == h.schema_version
                            })
                    })
            }
    }
    fn read_bytes(&self, name: &str, cap: usize) -> Result<Vec<u8>, Error> {
        let file = open_file(&self.root, name, false)?;
        if file.metadata().map_err(unavailable)?.len() > cap as u64 {
            return Err(Error::StorageCorrupt);
        }
        let mut bytes = Vec::new();
        file.take(cap as u64 + 1)
            .read_to_end(&mut bytes)
            .map_err(unavailable)?;
        if bytes.len() > cap {
            return Err(Error::StorageCorrupt);
        }
        Ok(bytes)
    }
    fn write_new(&self, name: &str, bytes: &[u8]) -> Result<(), Error> {
        let mut file = open_file(&self.root, name, true)?;
        checkpoint()?;
        file.write_all(bytes).map_err(unavailable)?;
        checkpoint()?;
        file.sync_all().map_err(unavailable)?;
        checkpoint()
    }
    fn unlink(&self, name: &str) -> Result<(), Error> {
        match fs::unlinkat(&self.root, name, AtFlags::empty()) {
            Ok(()) | Err(rustix::io::Errno::NOENT) => Ok(()),
            Err(e) => Err(unavailable(e)),
        }
    }
    fn replace_manifest(&self, id: &str, m: &Manifest) -> Result<(), Error> {
        let bytes = manifest_bytes(m)?;
        if bytes.len() > protocol::MAX_FRAME {
            return Err(Error::StorageCapacity);
        }
        self.write_new(".staged-manifest", &bytes)?;
        self.install_manifest(id)
    }
    fn install_manifest(&self, id: &str) -> Result<(), Error> {
        fs::renameat(
            &self.root,
            ".staged-manifest",
            &self.root,
            format!("{id}.manifest"),
        )
        .map_err(unavailable)?;
        checkpoint()?;
        self.root.sync_all().map_err(unavailable)?;
        checkpoint()
    }
    fn recount(&mut self) -> Result<(), Error> {
        let mut bytes = 0;
        let mut snapshots = 0;
        for name in names(&self.root)? {
            bytes += open_file(&self.root, &name, false)?
                .metadata()
                .map_err(unavailable)?
                .len();
            if name.ends_with(".snapshot") {
                snapshots += 1;
            }
        }
        self.metrics.bytes = bytes;
        self.metrics.snapshots = snapshots;
        Ok(())
    }
    fn checked_manifest(&self, key: &HistoryKey) -> Result<Option<&Manifest>, Error> {
        validate_key(key)?;
        let id = key_id(key)?;
        if self.bad.contains(&id) {
            return Err(Error::StorageCorrupt);
        }
        match self.manifests.get(&id) {
            Some(m) if &m.key != key => Err(Error::StorageCorrupt),
            value => Ok(value),
        }
    }
    fn snapshot(&self, key: &HistoryKey, entry: &Entry) -> Result<Snapshot, Error> {
        let bytes = self
            .read_bytes(
                &format!("{}.snapshot", entry.summary.snapshot_id),
                protocol::MAX_FRAME,
            )
            .map_err(|_| Error::StorageCorrupt)?;
        if bytes.len() as u64 != entry.bytes || digest(&bytes) != entry.file_sha256 {
            return Err(Error::StorageCorrupt);
        }
        let mut input = bytes.as_slice();
        if bytes.starts_with(b"OLCACHE") && bytes.get(7) != Some(&b'1') {
            return Err(Error::StorageUnavailable);
        }
        let (mut value, raw) =
            protocol::read::<Snapshot>(&mut input).map_err(|_| Error::StorageCorrupt)?;
        value.payload.raw = raw;
        if !input.is_empty()
            || &value.key.history != key
            || value.summary != entry.summary
            || value.payload.snapshot.as_ref()
                != Some(&SnapshotReference {
                    snapshot_id: entry.summary.snapshot_id.clone(),
                    captured_at: entry.summary.captured_at,
                })
            || validate_payload(&value.key, &value.payload).is_err()
        {
            return Err(Error::StorageCorrupt);
        }
        Ok(value)
    }
    fn lookup(&mut self, key: &PersistentKey, now: u64) -> Result<Response, Error> {
        let Some(m) = self.checked_manifest(&key.history)?.cloned() else {
            self.metrics.misses += 1;
            return Ok(Response::Payload(None));
        };
        let Some(head) = m.head(key) else {
            self.metrics.misses += 1;
            return Ok(Response::Payload(None));
        };
        let entry = m
            .entries
            .iter()
            .find(|e| e.summary.snapshot_id == head.id)
            .ok_or(Error::StorageCorrupt)?;
        if !self.policy.retains(entry.summary.captured_at, now) || head.validated_at > now {
            self.metrics.misses += 1;
            return Ok(Response::Payload(None));
        }
        let mut value = self.snapshot(&key.history, entry)?;
        if &value.key != key {
            self.metrics.misses += 1;
            return Ok(Response::Payload(None));
        }
        value.payload.provenance.validated_at = head.validated_at;
        value.payload.provenance.retrieved_at = head.retrieved_at;
        self.metrics.hits += 1;
        Ok(Response::Payload(Some(value.payload)))
    }
    fn stage(
        &mut self,
        key: PersistentKey,
        mut value: Payload,
        now: u64,
    ) -> Result<Response, Error> {
        validate_payload(&key, &value).map_err(|_| Error::InvalidPayload)?;
        self.maintain(now)?;
        let id = key_id(&key.history)?;
        let mut random = [0; 32];
        getrandom::fill(&mut random).map_err(unavailable)?;
        let mut m = self
            .checked_manifest(&key.history)?
            .cloned()
            .unwrap_or(Manifest {
                format: 1,
                key: key.history.clone(),
                incarnation: digest(&random),
                next_sequence: 1,
                heads: Vec::new(),
                entries: Vec::new(),
            });
        if let Some(old) = self.unchanged_head(&key, &value, now)? {
            while self
                .metrics
                .bytes
                .saturating_add(2 * protocol::MAX_FRAME as u64)
                > self.policy.max_bytes
            {
                self.evict_oldest(Some(&id))?;
            }
            value.provenance.retrieved_at = old.provenance.retrieved_at;
            value.snapshot = old.snapshot;
        } else {
            while !self.manifests.contains_key(&id)
                && self.manifests.len() + self.bad.len() >= MAX_KEYS
            {
                self.evict_oldest(None)?;
            }
            while m.entries.len() >= self.policy.max_snapshots_per_query {
                let victim = m.entries[0].summary.snapshot_id.clone();
                self.evict_id(&id, &victim)?;
                m = self
                    .manifests
                    .get(&id)
                    .cloned()
                    .ok_or(Error::StorageCorrupt)?;
            }
            let mut random = [0; 32];
            getrandom::fill(&mut random).map_err(unavailable)?;
            let snapshot_id = digest(&random);
            let summary = SnapshotSummary {
                snapshot_id: snapshot_id.clone(),
                sequence: m.next_sequence,
                captured_at: now,
                processor_version: key.processor_version.clone(),
                schema_version: key.schema_version,
                payload_sha256: value.provenance.payload_sha256.clone(),
            };
            m.next_sequence = m
                .next_sequence
                .checked_add(1)
                .ok_or(Error::StorageCapacity)?;
            value.snapshot = Some(SnapshotReference {
                snapshot_id: snapshot_id.clone(),
                captured_at: now,
            });
            let snapshot = Snapshot {
                key: key.clone(),
                summary: summary.clone(),
                payload: value.clone(),
            };
            let encoded = protocol::encode(&snapshot, &value.raw)?;
            // Reserve two manifest frames as well as the new immutable file.
            let reserve = encoded.len() as u64 + 2 * protocol::MAX_FRAME as u64;
            while self.metrics.bytes.saturating_add(reserve) > self.policy.max_bytes
                || self.metrics.snapshots >= MAX_SNAPSHOTS
            {
                self.evict_oldest(None)?;
            }
            if let Some(retained) = self.manifests.get(&id) {
                m.entries = retained.entries.clone();
                m.heads = retained.heads.clone();
            } else {
                m.entries.clear();
                m.heads.clear();
            }
            self.write_new(&format!("{snapshot_id}.snapshot"), &encoded)?;
            self.root.sync_all().map_err(unavailable)?;
            checkpoint()?;
            m.entries.push(Entry {
                summary,
                bytes: encoded.len() as u64,
                file_sha256: digest(&encoded),
            });
        }
        let snapshot = value.snapshot.as_ref().ok_or(Error::StorageCorrupt)?;
        m.heads.retain(|h| {
            h.processor_version != key.processor_version || h.schema_version != key.schema_version
        });
        m.heads.push(Head {
            id: snapshot.snapshot_id.clone(),
            processor_version: key.processor_version.clone(),
            schema_version: key.schema_version,
            validated_at: value.provenance.validated_at,
            retrieved_at: value.provenance.retrieved_at,
        });
        let bytes = manifest_bytes(&m)?;
        if bytes.len() > protocol::MAX_FRAME {
            return Err(Error::StorageCapacity);
        }
        self.write_new(".staged-manifest", &bytes)?;
        self.staged = Some((id, m, value));
        Ok(Response::Prepared)
    }
    fn prepare_publish(
        &mut self,
        key: PersistentKey,
        value: Payload,
        now: u64,
    ) -> Result<Response, Error> {
        validate_payload(&key, &value).map_err(|_| Error::InvalidPayload)?;
        let id = key_id(&key.history)?;
        let m = self.checked_manifest(&key.history)?;
        let unchanged = self.unchanged_head(&key, &value, now)?.is_some();
        let workspace = if unchanged { 2 } else { 3 } * protocol::MAX_FRAME as u64;
        // This conservative preflight can request an invalidation barrier before
        // a pressure scan that ultimately finds no deletion. Ordinary writes do
        // not invalidate unrelated L1 entries or completed publication results.
        let cleanup = self.orphan_cleanup
            || self.manifests.values().any(|m| {
                m.entries
                    .iter()
                    .any(|e| !self.policy.retains(e.summary.captured_at, now))
            })
            || !unchanged
                && m.is_some_and(|m| m.entries.len() >= self.policy.max_snapshots_per_query)
            || !unchanged
                && !self.manifests.contains_key(&id)
                && self.manifests.len() + self.bad.len() >= MAX_KEYS
            || !unchanged && self.metrics.snapshots >= MAX_SNAPSHOTS
            || self.metrics.bytes.saturating_add(workspace) > self.policy.max_bytes;
        if cleanup {
            self.staged_publish = Some((key, value, now));
            Ok(Response::Prepared)
        } else {
            self.stage(key, value, now)
        }
    }
    /// Share exactly the same eligibility and identity checks between resource
    /// admission and staging, including future-clock and expired observations.
    fn unchanged_head(
        &self,
        key: &PersistentKey,
        value: &Payload,
        now: u64,
    ) -> Result<Option<Payload>, Error> {
        let Some(m) = self.checked_manifest(&key.history)? else {
            return Ok(None);
        };
        let Some(entry) = m
            .head(key)
            .and_then(|h| m.entries.iter().find(|e| e.summary.snapshot_id == h.id))
            .filter(|e| {
                e.summary.captured_at <= now && self.policy.retains(e.summary.captured_at, now)
            })
        else {
            return Ok(None);
        };
        let old = self.snapshot(&key.history, entry)?;
        if old.payload.provenance.retrieved_at <= now
            && old.key == *key
            && old.payload.raw == value.raw
            && old.payload.provenance.source_reference == value.provenance.source_reference
            && serde_json::to_value(&old.payload.data).map_err(unavailable)?
                == serde_json::to_value(&value.data).map_err(unavailable)?
        {
            Ok(Some(old.payload))
        } else {
            Ok(None)
        }
    }
    fn commit(&mut self) -> Result<Response, Error> {
        if let Some((key, value, now)) = self.staged_publish.take() {
            return self.stage(key, value, now);
        }
        if let Some((id, snapshot, lookup, now)) = self.staged_isolation.take() {
            if self.bad.remove(&id) {
                self.quarantine_file(&format!("{id}.manifest"), now)?;
                self.orphan_cleanup = self.bad.is_empty();
                self.reclaim_orphans(32)?;
            } else if let Some(snapshot) = snapshot {
                let mut m = self
                    .manifests
                    .get(&id)
                    .cloned()
                    .ok_or(Error::StorageCorrupt)?;
                m.entries.retain(|e| e.summary.snapshot_id != snapshot);
                m.heads.retain(|h| h.id != snapshot);
                self.replace_manifest(&id, &m)?;
                self.manifests.insert(id, m);
                self.quarantine_file(&format!("{snapshot}.snapshot"), now)?;
            }
            self.recount()?;
            return Ok(if lookup {
                Response::Payload(None)
            } else {
                Response::Error(Error::StorageCorrupt.into())
            });
        }
        if let Some(now) = self.staged_maintenance.take() {
            self.maintain(now)?;
            return Ok(Response::Done);
        }
        let (id, m, value) = self.staged.take().ok_or(Error::InvalidInput)?;
        self.install_manifest(&id)?;
        self.manifests.insert(id, m);
        self.metrics.writes += 1;
        self.recount()?;
        Ok(Response::Payload(Some(value)))
    }
    fn evict_id(&mut self, key: &str, victim: &str) -> Result<(), Error> {
        let mut m = self
            .manifests
            .get(key)
            .cloned()
            .ok_or(Error::StorageCapacity)?;
        let old_manifest = manifest_bytes(&m)?.len() as u64;
        let victim_bytes = m
            .entries
            .iter()
            .find(|e| e.summary.snapshot_id == victim)
            .map_or(0, |e| e.bytes);
        m.entries.retain(|e| e.summary.snapshot_id != victim);
        m.heads.retain(|h| h.id != victim);
        let new_manifest = manifest_bytes(&m)?.len() as u64;
        self.replace_manifest(key, &m)?;
        self.manifests.insert(key.to_owned(), m);
        self.unlink(&format!("{victim}.snapshot"))?;
        self.root.sync_all().map_err(unavailable)?;
        checkpoint()?;
        self.metrics.evictions += 1;
        self.metrics.snapshots = self.metrics.snapshots.saturating_sub(1);
        self.metrics.bytes = self
            .metrics
            .bytes
            .saturating_sub(old_manifest + victim_bytes)
            .saturating_add(new_manifest);
        Ok(())
    }
    fn evict_oldest(&mut self, exclude: Option<&str>) -> Result<(), Error> {
        if self.reclaim_orphans(32)? > 0 {
            return Ok(());
        }
        if !self.quarantine.is_empty() {
            return self.evict_quarantine();
        }
        // Empty key manifests are removed first; this bounds all-key bookkeeping.
        let empty = self
            .manifests
            .iter()
            .find(|(id, m)| m.entries.is_empty() && Some(id.as_str()) != exclude)
            .map(|(id, _)| id.clone());
        if let Some(id) = empty {
            let bytes = self
                .manifests
                .get(&id)
                .map(manifest_bytes)
                .transpose()?
                .map_or(0, |v| v.len() as u64);
            self.unlink(&format!("{id}.manifest"))?;
            self.root.sync_all().map_err(unavailable)?;
            self.manifests.remove(&id);
            self.metrics.bytes = self.metrics.bytes.saturating_sub(bytes);
            return Ok(());
        }
        let candidate = self
            .manifests
            .iter()
            .filter(|(id, _)| Some(id.as_str()) != exclude)
            .flat_map(|(id, m)| {
                m.entries.iter().map(move |e| {
                    let head = m.heads.iter().find(|h| h.id == e.summary.snapshot_id);
                    (
                        head.is_some(),
                        head.map_or(e.summary.captured_at, |h| h.validated_at),
                        e.summary.sequence,
                        id.clone(),
                        e.summary.snapshot_id.clone(),
                    )
                })
            })
            .min();
        let Some((_, _, _, id, victim)) = candidate else {
            return Err(Error::StorageCapacity);
        };
        self.evict_id(&id, &victim)
    }
    fn maintain(&mut self, now: u64) -> Result<(), Error> {
        if self.orphan_cleanup {
            self.reclaim_orphans(32)?;
        }
        self.trim_quarantine(Some(now))?;
        let victims: Vec<_> = self
            .manifests
            .iter()
            .flat_map(|(id, m)| {
                m.entries
                    .iter()
                    .filter(|e| !self.policy.retains(e.summary.captured_at, now))
                    .map(|e| (id.clone(), e.summary.snapshot_id.clone()))
            })
            .take(32)
            .collect();
        for (id, victim) in victims {
            self.evict_id(&id, &victim)?;
        }
        Ok(())
    }
    /// Only complete, currently authoritative manifests establish references.
    /// An unreadable manifest prevents orphan deletion because its references
    /// are unknown. Accounting changes only after unlink and directory sync.
    fn reclaim_orphans(&mut self, limit: usize) -> Result<usize, Error> {
        if !self.bad.is_empty() {
            return Ok(0);
        }
        let referenced: HashSet<_> = self
            .manifests
            .values()
            .flat_map(|m| {
                m.entries
                    .iter()
                    .map(|e| format!("{}.snapshot", e.summary.snapshot_id))
            })
            .collect();
        let victims: Vec<_> = names(&self.root)?
            .into_iter()
            .filter(|name| name.ends_with(".snapshot") && !referenced.contains(name))
            .take(limit + 1)
            .collect();
        self.orphan_cleanup = victims.len() > limit;
        let mut removed = 0;
        for name in victims.into_iter().take(limit) {
            let bytes = open_file(&self.root, &name, false)?
                .metadata()
                .map_err(unavailable)?
                .len();
            self.unlink(&name)?;
            self.root.sync_all().map_err(unavailable)?;
            self.metrics.bytes = self.metrics.bytes.saturating_sub(bytes);
            self.metrics.snapshots = self.metrics.snapshots.saturating_sub(1);
            removed += 1;
        }
        Ok(removed)
    }
    fn quarantine_file(&mut self, name: &str, now: u64) -> Result<(), Error> {
        let file = match fs::openat(
            &self.root,
            name,
            OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
            Mode::empty(),
        ) {
            Ok(file) => regular(File::from(file))?,
            Err(rustix::io::Errno::NOENT) => return Ok(()),
            Err(e) => return Err(unavailable(e)),
        };
        let bytes = file.metadata().map_err(unavailable)?.len();
        let mut random = [0; 32];
        getrandom::fill(&mut random).map_err(unavailable)?;
        let target = format!("q-{now:020}-{}.quarantine", digest(&random));
        fs::renameat(&self.root, name, &self.root, &target).map_err(unavailable)?;
        self.root.sync_all().map_err(unavailable)?;
        self.quarantine.push((now, target, bytes));
        self.trim_quarantine(Some(now))
    }
    fn evict_quarantine(&mut self) -> Result<(), Error> {
        self.quarantine.sort();
        let (_, name, bytes) = self
            .quarantine
            .first()
            .cloned()
            .ok_or(Error::StorageCapacity)?;
        self.unlink(&name)?;
        self.root.sync_all().map_err(unavailable)?;
        self.quarantine.remove(0);
        self.metrics.bytes = self.metrics.bytes.saturating_sub(bytes);
        Ok(())
    }
    fn trim_quarantine(&mut self, now: Option<u64>) -> Result<(), Error> {
        self.quarantine.sort();
        while self.quarantine.len() > MAX_QUARANTINE_FILES
            || self.quarantine.iter().map(|v| v.2).sum::<u64>() > MAX_QUARANTINE_BYTES
            || self
                .quarantine
                .first()
                .is_some_and(|q| now.is_some_and(|now| !self.policy.retains(q.0, now)))
        {
            self.evict_quarantine()?;
        }
        Ok(())
    }
    fn list(
        &self,
        key: &HistoryKey,
        cursor: Option<String>,
        limit: usize,
        now: u64,
    ) -> Result<Response, Error> {
        if !(1..=20).contains(&limit) {
            return Err(Error::InvalidInput);
        }
        let id = key_id(key)?;
        let m = self.checked_manifest(key)?;
        let max = m.map_or(0, |m| m.next_sequence - 1);
        let incarnation = m.map_or("", |m| m.incarnation.as_str());
        let (watermark, before) = if let Some(cursor) = cursor {
            if cursor.len() > 180 {
                return Err(Error::InvalidInput);
            }
            let mut pieces = cursor.split('.');
            if pieces.next() != Some(id.as_str()) {
                return Err(Error::InvalidInput);
            }
            if pieces.next() != Some(incarnation) {
                return Err(Error::InvalidInput);
            }
            let a = pieces
                .next()
                .ok_or(Error::InvalidInput)?
                .parse::<u64>()
                .map_err(|_| Error::InvalidInput)?;
            let b = pieces
                .next()
                .ok_or(Error::InvalidInput)?
                .parse::<u64>()
                .map_err(|_| Error::InvalidInput)?;
            if pieces.next().is_some() || a > max || b == 0 {
                return Err(Error::InvalidInput);
            }
            (a, b)
        } else {
            (max, u64::MAX)
        };
        let mut rows = m
            .into_iter()
            .flat_map(|m| m.entries.iter().rev())
            .filter(|e| {
                e.summary.sequence <= watermark
                    && e.summary.sequence < before
                    && self.policy.retains(e.summary.captured_at, now)
            });
        let snapshots: Vec<_> = rows
            .by_ref()
            .take(limit)
            .map(|e| e.summary.clone())
            .collect();
        let next_cursor = if rows.next().is_some() {
            snapshots
                .last()
                .map(|v| format!("{id}.{incarnation}.{watermark}.{}", v.sequence))
        } else {
            None
        };
        Ok(Response::List(SnapshotPage {
            snapshots,
            next_cursor,
            synthetic: true,
        }))
    }
    fn get(&self, key: &HistoryKey, id: &str, now: u64) -> Result<Response, Error> {
        if !valid_snapshot_id(id) {
            return Err(Error::InvalidInput);
        }
        let m = self
            .checked_manifest(key)?
            .ok_or(Error::SnapshotUnavailable)?;
        let e = m
            .entries
            .iter()
            .find(|e| {
                e.summary.snapshot_id == id && self.policy.retains(e.summary.captured_at, now)
            })
            .ok_or(Error::SnapshotUnavailable)?;
        let value = self.snapshot(key, e)?;
        Ok(Response::Snapshot(SnapshotEnvelope {
            schema_version: 1,
            snapshot: value.summary,
            query: key.query.clone(),
            data: value.payload.data,
            provenance: value.payload.provenance,
            historical: true,
            synthetic: true,
            clock_anomaly: e.summary.captured_at > now,
        }))
    }
}

#[cfg(test)]
mod tests;

fn validate_key(key: &HistoryKey) -> Result<(), Error> {
    key.query.validate()?;
    if [&key.namespace, &key.provider, &key.dataset]
        .iter()
        .any(|v| !openlegal_domain::valid_identifier(v, 64))
    {
        return Err(Error::InvalidInput);
    }
    Ok(())
}
fn validate_payload(key: &PersistentKey, value: &Payload) -> Result<(), Error> {
    validate_key(&key.history)?;
    let p = &value.provenance;
    if key.schema_version != 1
        || key.processor_version.is_empty()
        || key.processor_version.len() > 128
        || p.provider != key.history.provider
        || p.dataset != key.history.dataset
        || p.processor_version != key.processor_version
        || value.raw.len() > MAX_RAW_BYTES
        || p.payload_sha256 != digest(&value.raw)
        || p.source_reference.is_empty()
        || p.source_reference.len() > 2048
        || p.source_reference.contains(['?', '#', '@'])
        || p.source_reference.chars().any(char::is_control)
        || serde_json::to_vec(&value.data).map_err(unavailable)?.len() > MAX_PROCESSED_BYTES
    {
        return Err(Error::StorageCorrupt);
    }
    let record_valid = |r: &openlegal_domain::Record| {
        r.source == key.history.query.source()
            && openlegal_domain::valid_identifier(&r.id, 128)
            && r.synthetic
            && !r.title.is_empty()
            && r.title.len() <= 1024
            && r.body.len() <= 16 * 1024
    };
    let valid = match (&key.history.query, &value.data) {
        (Query::Get { id, .. }, RetrievalData::Get(r)) => &r.id == id && record_valid(r),
        (
            Query::Search {
                page, page_size, ..
            },
            RetrievalData::Search(s),
        ) => {
            let mut ids = HashSet::new();
            s.page == *page
                && s.page_size == *page_size
                && s.total <= 20_000
                && s.records.len() as u64
                    == u64::from(*page_size).min(
                        u64::from(s.total).saturating_sub(u64::from(*page) * u64::from(*page_size)),
                    )
                && s.records
                    .iter()
                    .all(|r| record_valid(r) && ids.insert(&r.id))
        }
        _ => false,
    };
    if !valid {
        return Err(Error::StorageCorrupt);
    }
    Ok(())
}

pub(super) fn run() -> Result<(), Error> {
    let mut reader = std::io::stdin().lock();
    let mut writer = std::io::stdout().lock();
    let (request, raw) = protocol::read::<Request>(&mut reader)?;
    let Request::Open { root, policy } = request else {
        return Err(Error::InvalidInput);
    };
    if !raw.is_empty() {
        return Err(Error::InvalidInput);
    }
    let mut engine = Engine::open(Path::new(&root), policy)?;
    protocol::write(
        &mut writer,
        &Reply {
            response: Response::Ready,
            metrics: engine.metrics.clone(),
            invalidates_memory: false,
        },
        &[],
    )?;
    loop {
        let (mut request, raw) = match protocol::read::<Request>(&mut reader) {
            Ok(value) => value,
            Err(Error::Unavailable) => return Ok(()),
            Err(e) => return Err(e),
        };
        request.attach(raw)?;
        let isolation = match &request {
            Request::Lookup { key, now } | Request::Publish { key, now, .. } => Some((
                key_id(&key.history)?,
                engine
                    .manifests
                    .get(&key_id(&key.history)?)
                    .and_then(|m| m.head(key).map(|h| h.id.clone())),
                matches!(request, Request::Lookup { .. }),
                *now,
            )),
            Request::Get { key, id, now } => Some((key_id(key)?, Some(id.clone()), false, *now)),
            Request::List { key, now, .. } => Some((key_id(key)?, None, false, *now)),
            _ => None,
        };
        let result = match request {
            Request::Lookup { key, now } => engine.lookup(&key, now),
            Request::Publish { key, value, now } => engine.prepare_publish(key, *value, now),
            Request::List {
                key,
                cursor,
                limit,
                now,
            } => engine.list(&key, cursor, limit, now),
            Request::Get { key, id, now } => engine.get(&key, &id, now),
            Request::Maintain { now } => {
                if engine.orphan_cleanup
                    || engine
                        .quarantine
                        .iter()
                        .any(|q| !engine.policy.retains(q.0, now))
                    || engine.manifests.values().any(|m| {
                        m.entries
                            .iter()
                            .any(|e| !engine.policy.retains(e.summary.captured_at, now))
                    })
                {
                    engine.staged_maintenance = Some(now);
                    Ok(Response::Prepared)
                } else {
                    Ok(Response::Done)
                }
            }
            Request::Commit => engine.commit(),
            Request::Close => {
                protocol::write(
                    &mut writer,
                    &Reply {
                        response: Response::Done,
                        metrics: engine.metrics.clone(),
                        invalidates_memory: false,
                    },
                    &[],
                )?;
                return Ok(());
            }
            Request::Open { .. } => Err(Error::InvalidInput),
        };
        if matches!(result, Err(Error::StorageCorrupt)) {
            engine.metrics.corruptions += 1;
        }
        // An I/O failure may leave an installed but unacknowledged manifest. Exit;
        // the parent must reap and reopen before allowing further operations.
        if matches!(result, Err(Error::StorageUnavailable)) {
            return Err(Error::StorageUnavailable);
        }
        let response = if matches!(result, Err(Error::StorageCorrupt)) {
            if let Some(isolation) = isolation {
                engine.staged_isolation = Some(isolation);
                Response::Prepared
            } else {
                Response::Error(Error::StorageCorrupt.into())
            }
        } else {
            result.unwrap_or_else(|e| Response::Error(e.into()))
        };
        let invalidates_memory = engine.staged_publish.is_some()
            || engine.staged_maintenance.is_some()
            || engine.staged_isolation.is_some();
        let reply = Reply {
            response,
            metrics: engine.metrics.clone(),
            invalidates_memory,
        };
        protocol::write(&mut writer, &reply, reply.raw())?;
    }
}
