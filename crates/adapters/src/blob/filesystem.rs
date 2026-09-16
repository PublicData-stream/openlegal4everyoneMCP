use super::{Error, MAX_BATCH, check_cancel};
use openlegal_application::blob::{BlobLocation, BlobPage, BlobPutResult};
use rustix::fs::{self, AtFlags, Mode, OFlags, RenameFlags};
use sha2::{Digest, Sha256};
use std::{
    fs::File,
    io::{Read, Write},
    os::unix::fs::MetadataExt,
    path::{Component, Path},
    sync::Mutex,
};
use tokio_util::sync::CancellationToken;

const STAGING_AGE: u64 = 60;

#[cfg(test)]
thread_local! { pub(super) static FAIL_AFTER: std::cell::Cell<Option<usize>> = const { std::cell::Cell::new(None) }; }
#[cfg(test)]
thread_local! { pub(super) static PARENT_SYNC_FAIL: std::cell::Cell<bool> = const { std::cell::Cell::new(false) }; }

fn sync_parent(directory: &File) -> Result<(), Error> {
    #[cfg(test)]
    if PARENT_SYNC_FAIL.get() {
        return Err(Error::StorageUnavailable);
    }
    directory.sync_all().map_err(unavailable)
}
fn checkpoint() -> Result<(), Error> {
    #[cfg(test)]
    return FAIL_AFTER.with(|point| match point.get() {
        Some(0) => Err(Error::StorageUnavailable),
        Some(left) => {
            point.set(Some(left - 1));
            Ok(())
        }
        None => Ok(()),
    });
    #[cfg(not(test))]
    Ok(())
}

pub(super) struct Filesystem {
    root: File,
    max_object_bytes: usize,
    staging_cursor: Mutex<(u16, i64)>,
}

fn unavailable(_: impl std::fmt::Debug) -> Error {
    Error::StorageUnavailable
}
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|v| format!("{v:02x}")).collect()
}
fn nonce() -> Result<String, Error> {
    let mut bytes = [0u8; 32];
    getrandom::fill(&mut bytes).map_err(unavailable)?;
    Ok(hex(&bytes))
}
fn private_directory(file: File) -> Result<File, Error> {
    let metadata = file.metadata().map_err(unavailable)?;
    if !metadata.is_dir()
        || metadata.uid() != rustix::process::geteuid().as_raw()
        || metadata.mode() & 0o777 != 0o700
    {
        return Err(Error::StorageCorrupt);
    }
    Ok(file)
}
fn regular(file: File) -> Result<File, Error> {
    let metadata = file.metadata().map_err(unavailable)?;
    if !metadata.is_file()
        || metadata.nlink() != 1
        || metadata.uid() != rustix::process::geteuid().as_raw()
        || metadata.mode() & 0o777 != 0o600
    {
        return Err(Error::StorageCorrupt);
    }
    Ok(file)
}
fn open_file(dir: &File, name: &str, create: bool) -> Result<Option<File>, Error> {
    let flags = if create {
        OFlags::RDWR | OFlags::CREATE | OFlags::EXCL
    } else {
        OFlags::RDONLY
    };
    match fs::openat(
        dir,
        name,
        flags | OFlags::NOFOLLOW | OFlags::CLOEXEC | OFlags::NONBLOCK,
        Mode::from_raw_mode(0o600),
    ) {
        Ok(file) => regular(File::from(file)).map(Some),
        Err(rustix::io::Errno::NOENT) if !create => Ok(None),
        Err(rustix::io::Errno::LOOP) => Err(Error::StorageCorrupt),
        Err(error) => Err(unavailable(error)),
    }
}
fn unlink(dir: &File, name: &str) -> Result<bool, Error> {
    let removed = match fs::unlinkat(dir, name, AtFlags::empty()) {
        Ok(()) => true,
        Err(rustix::io::Errno::NOENT) => false,
        Err(error) => return Err(unavailable(error)),
    };
    dir.sync_all().map_err(unavailable)?;
    Ok(removed)
}
fn parse_name(name: &str) -> Option<[u8; 32]> {
    let (digest, generation) = name.split_once('-')?;
    if digest.len() != 64 || generation.len() != 36 {
        return None;
    }
    if !digest
        .bytes()
        .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return None;
    }
    if !generation.bytes().enumerate().all(|(i, b)| {
        if [8, 13, 18, 23].contains(&i) {
            b == b'-'
        } else {
            b.is_ascii_digit() || (b'a'..=b'f').contains(&b)
        }
    }) {
        return None;
    }
    let mut value = [0u8; 32];
    for (i, byte) in value.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&digest[i * 2..i * 2 + 2], 16).ok()?;
    }
    Some(value)
}
fn location_parts(location: &BlobLocation, max_object_bytes: usize) -> Result<(&str, &str), Error> {
    let (prefix, name) = location
        .storage_key
        .split_once('/')
        .ok_or(Error::InvalidInput)?;
    if location.size_bytes > max_object_bytes as u64
        || prefix.len() != 2
        || !name.starts_with(prefix)
        || parse_name(name) != Some(location.digest)
    {
        return Err(Error::InvalidInput);
    }
    Ok((prefix, name))
}
fn staging_name(name: &str) -> bool {
    name.strip_prefix(".stage-")
        .and_then(|rest| rest.rsplit_once('.'))
        .is_some_and(|(target, suffix)| {
            parse_name(target).is_some()
                && suffix.len() == 64
                && suffix
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        })
}

impl Filesystem {
    #[cfg(test)]
    pub(super) fn open(path: &Path) -> Result<Self, Error> {
        Self::open_with_limit(path, openlegal_application::MAX_RAW_BYTES)
    }
    pub(super) fn open_with_limit(path: &Path, max_object_bytes: usize) -> Result<Self, Error> {
        if !path.is_absolute() || path.components().count() < 2 {
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
        let components: Vec<_> = path.components().collect();
        for (index, component) in components.iter().enumerate() {
            match component {
                Component::RootDir => {}
                Component::Normal(name) => {
                    if index + 1 == components.len() {
                        match fs::mkdirat(&directory, *name, Mode::from_raw_mode(0o700)) {
                            Ok(()) | Err(rustix::io::Errno::EXIST) => {}
                            Err(error) => return Err(unavailable(error)),
                        }
                        // Another creator may have stopped after mkdir. This
                        // opener establishes the parent's durability itself.
                        sync_parent(&directory)?;
                    }
                    directory = File::from(
                        fs::openat(
                            &directory,
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
        Ok(Self {
            root: private_directory(directory)?,
            max_object_bytes,
            staging_cursor: Mutex::new((0, 0)),
        })
    }
    fn shard(&self, prefix: &str, create: bool) -> Result<Option<File>, Error> {
        if create {
            match fs::mkdirat(&self.root, prefix, Mode::from_raw_mode(0o700)) {
                Ok(()) | Err(rustix::io::Errno::EXIST) => {}
                Err(error) => return Err(unavailable(error)),
            }
            sync_parent(&self.root)?;
        }
        match fs::openat(
            &self.root,
            prefix,
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        ) {
            Ok(file) => private_directory(File::from(file)).map(Some),
            Err(rustix::io::Errno::NOENT) if !create => Ok(None),
            Err(error) => Err(unavailable(error)),
        }
    }
    fn verified(
        &self,
        dir: &File,
        name: &str,
        location: &BlobLocation,
        durable: bool,
    ) -> Result<Option<Vec<u8>>, Error> {
        let Some(mut file) = open_file(dir, name, false)? else {
            return Ok(None);
        };
        if file.metadata().map_err(unavailable)?.len() != location.size_bytes {
            return Err(Error::StorageCorrupt);
        }
        let mut bytes = Vec::with_capacity(location.size_bytes as usize);
        (&mut file)
            .take(location.size_bytes + 1)
            .read_to_end(&mut bytes)
            .map_err(unavailable)?;
        if bytes.len() as u64 != location.size_bytes
            || Sha256::digest(&bytes).as_slice() != location.digest
        {
            return Err(Error::StorageCorrupt);
        }
        if durable {
            file.sync_all().map_err(unavailable)?;
            dir.sync_all().map_err(unavailable)?;
        }
        Ok(Some(bytes))
    }
    pub(super) fn get(&self, location: &BlobLocation) -> Result<Option<Vec<u8>>, Error> {
        let (prefix, name) = location_parts(location, self.max_object_bytes)?;
        let Some(dir) = self.shard(prefix, false)? else {
            return Ok(None);
        };
        self.verified(&dir, name, location, false)
    }
    pub(super) fn put(
        &self,
        location: &BlobLocation,
        bytes: &[u8],
        token: &CancellationToken,
    ) -> Result<BlobPutResult, Error> {
        let (prefix, name) = location_parts(location, self.max_object_bytes)?;
        if bytes.len() as u64 != location.size_bytes
            || Sha256::digest(bytes).as_slice() != location.digest
        {
            return Err(Error::InvalidInput);
        }
        check_cancel(token)?;
        let dir = self.shard(prefix, true)?.ok_or(Error::StorageUnavailable)?;
        if let Some(existing) = self.verified(&dir, name, location, true)? {
            if existing != bytes {
                return Err(Error::StorageCorrupt);
            }
            return Ok(BlobPutResult::AlreadyPresent);
        }
        let temporary = format!(".stage-{name}.{}", nonce()?);
        let mut file = open_file(&dir, &temporary, true)?.ok_or(Error::StorageUnavailable)?;
        let result = (|| {
            file.write_all(bytes).map_err(unavailable)?;
            file.sync_all().map_err(unavailable)?;
            checkpoint()?;
            check_cancel(token)?;
            let result = match fs::renameat_with(
                &dir,
                temporary.as_str(),
                &dir,
                name,
                RenameFlags::NOREPLACE,
            ) {
                Ok(()) => BlobPutResult::Created,
                Err(rustix::io::Errno::EXIST) => {
                    let existing = self
                        .verified(&dir, name, location, true)?
                        .ok_or(Error::StorageCorrupt)?;
                    if existing != bytes {
                        return Err(Error::StorageCorrupt);
                    }
                    BlobPutResult::AlreadyPresent
                }
                Err(error) => return Err(unavailable(error)),
            };
            checkpoint()?;
            dir.sync_all().map_err(unavailable)?;
            checkpoint()?;
            check_cancel(token)?;
            Ok(result)
        })();
        // A failed cleanup leaves only recognizable staging garbage, never evidence.
        let cleanup = unlink(&dir, &temporary);
        match result {
            Ok(value) => {
                cleanup?;
                Ok(value)
            }
            Err(error) => Err(error),
        }
    }
    pub(super) fn delete(&self, location: &BlobLocation) -> Result<bool, Error> {
        let (prefix, name) = location_parts(location, self.max_object_bytes)?;
        let Some(dir) = self.shard(prefix, false)? else {
            return Ok(false);
        };
        if let Some(file) = open_file(&dir, name, false)? {
            drop(file);
        }
        unlink(&dir, name)
    }
    pub(super) fn enumerate(
        &self,
        cursor: Option<String>,
        limit: usize,
        token: &CancellationToken,
    ) -> Result<BlobPage, Error> {
        if !(1..=MAX_BATCH).contains(&limit) {
            return Err(Error::InvalidInput);
        }
        let (mut shard, mut offset) = if let Some(cursor) = cursor {
            if cursor.len() > 32 {
                return Err(Error::InvalidInput);
            }
            let (a, b) = cursor.split_once(':').ok_or(Error::InvalidInput)?;
            (
                a.parse::<u16>().map_err(|_| Error::InvalidInput)?,
                b.parse::<i64>().map_err(|_| Error::InvalidInput)?,
            )
        } else {
            (0, 0)
        };
        if shard > 255 || offset < 0 {
            return Err(Error::InvalidInput);
        }
        let mut objects = Vec::new();
        let mut scanned = 0;
        while shard < 256 && scanned < limit {
            check_cancel(token)?;
            let prefix = format!("{shard:02x}");
            let Some(dir) = self.shard(&prefix, false)? else {
                shard += 1;
                offset = 0;
                scanned += 1;
                continue;
            };
            let mut entries = fs::Dir::read_from(&dir).map_err(unavailable)?;
            entries.seek(offset).map_err(unavailable)?;
            let mut ended = false;
            while scanned < limit {
                let Some(entry) = entries.read() else {
                    ended = true;
                    break;
                };
                let entry = entry.map_err(unavailable)?;
                offset = entry.offset();
                scanned += 1;
                let Some(name) = entry.file_name().to_str().ok() else {
                    continue;
                };
                let Some(digest) = parse_name(name) else {
                    continue;
                };
                if !name.starts_with(&prefix) {
                    return Err(Error::StorageCorrupt);
                }
                if let Some(file) = open_file(&dir, name, false)? {
                    let size_bytes = file.metadata().map_err(unavailable)?.len();
                    if size_bytes > self.max_object_bytes as u64 {
                        return Err(Error::StorageCorrupt);
                    }
                    objects.push(BlobLocation {
                        digest,
                        size_bytes,
                        storage_key: format!("{prefix}/{name}"),
                    });
                }
            }
            if ended {
                shard += 1;
                offset = 0;
            }
        }
        Ok(BlobPage {
            objects,
            next_cursor: (shard < 256).then(|| format!("{shard}:{offset}")),
        })
    }
    pub(super) fn cleanup_staging(
        &self,
        now: u64,
        limit: usize,
        token: &CancellationToken,
    ) -> Result<usize, Error> {
        if !(1..=MAX_BATCH).contains(&limit) {
            return Err(Error::InvalidInput);
        }
        // This narrow cursor mutex never guards upstream/SQL work. try_lock avoids
        // letting a wedged filesystem cleanup create a blocking-job backlog.
        let mut cursor = self.staging_cursor.try_lock().map_err(|_| Error::Busy)?;
        let (mut shard, mut offset) = *cursor;
        let mut scanned = 0;
        let mut removed = 0;
        while scanned < limit {
            check_cancel(token)?;
            let prefix = format!("{shard:02x}");
            let Some(dir) = self.shard(&prefix, false)? else {
                shard = (shard + 1) % 256;
                offset = 0;
                scanned += 1;
                continue;
            };
            let mut entries = fs::Dir::read_from(&dir).map_err(unavailable)?;
            entries.seek(offset).map_err(unavailable)?;
            let mut ended = false;
            while scanned < limit {
                let Some(entry) = entries.read() else {
                    ended = true;
                    break;
                };
                let entry = entry.map_err(unavailable)?;
                offset = entry.offset();
                scanned += 1;
                let Some(name) = entry.file_name().to_str().ok() else {
                    continue;
                };
                if !staging_name(name) {
                    continue;
                }
                let Some(file) = open_file(&dir, name, false)? else {
                    continue;
                };
                let metadata = file.metadata().map_err(unavailable)?;
                if u64::try_from(metadata.mtime())
                    .ok()
                    .and_then(|at| now.checked_sub(at))
                    .is_some_and(|age| age >= STAGING_AGE)
                {
                    drop(file);
                    if unlink(&dir, name)? {
                        removed += 1;
                    }
                }
            }
            if ended {
                shard = (shard + 1) % 256;
                offset = 0;
            }
        }
        *cursor = (shard, offset);
        Ok(removed)
    }
    pub(super) fn health(&self) -> Result<(), Error> {
        // Probe in the staging namespace: health bytes can never be mistaken for
        // a final source object or require a PostgreSQL snapshot reservation.
        let prefix = "00";
        let dir = self.shard(prefix, true)?.ok_or(Error::StorageUnavailable)?;
        let target = format!("{}-00000000-0000-0000-0000-000000000000", "0".repeat(64));
        let a = format!(".stage-{target}.{}", nonce()?);
        let b = format!(".stage-{target}.{}", nonce()?);
        let result = (|| {
            let mut file = open_file(&dir, &a, true)?.ok_or(Error::StorageUnavailable)?;
            file.write_all(b"blob-health\n").map_err(unavailable)?;
            file.sync_all().map_err(unavailable)?;
            fs::renameat_with(&dir, a.as_str(), &dir, b.as_str(), RenameFlags::NOREPLACE)
                .map_err(unavailable)?;
            dir.sync_all().map_err(unavailable)?;
            let mut check = open_file(&dir, &b, false)?.ok_or(Error::StorageUnavailable)?;
            let mut data = Vec::new();
            (&mut check)
                .take(32)
                .read_to_end(&mut data)
                .map_err(unavailable)?;
            if data != b"blob-health\n" {
                return Err(Error::StorageCorrupt);
            }
            let _second = open_file(&dir, &a, true)?.ok_or(Error::StorageUnavailable)?;
            if fs::renameat_with(&dir, a.as_str(), &dir, b.as_str(), RenameFlags::NOREPLACE)
                != Err(rustix::io::Errno::EXIST)
            {
                return Err(Error::StorageUnavailable);
            }
            Ok(())
        })();
        let cleanup_a = unlink(&dir, &a);
        let cleanup_b = unlink(&dir, &b);
        result?;
        cleanup_a?;
        cleanup_b?;
        Ok(())
    }
}
