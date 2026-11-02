//! Bind an operator-verified Blob seed to the exact currently mounted roots.
//!
//! This module never creates a root, marker, file, role or seed. The operator
//! must verify the full manifest/reference correspondence and the marker's
//! filesystem_id against the actual device/dataset before provisioning the
//! protected PostgreSQL binding. A directory identity is not hardware proof.
//!
//! Producer algorithm (also implementable with Python stdlib on Linux): sort
//! roots by role; use each exact absolute container path, os.fstat directory
//! dev/ino/uid/gid/(mode & 07777), os.fstatvfs(fd).f_fsid as unsigned64, the
//! validated marker filesystem_id and SHA256 of its ORIGINAL bytes. Serialize
//! the document/fields defined below as UTF8 compact JSON with sorted keys and
//! no ASCII escaping, append LF, prepend `openlegal-blob-seed-binding-v1\n`,
//! then SHA256. managed_epoch and boot_id are canonical lowercase UUIDs.
//! Include every configured persistent root: this API cannot infer omitted
//! configuration. Marker bytes/mtime are immutable while a binding is held;
//! ordinary generation/health changes must not change the marker.
//!
//! Metadata reads can block in the kernel. They have no hard physical deadline;
//! async callers must use owned bounded work, never perform these in readiness.
use openlegal_application::blob::{BlobRootIdentity, BlobRootVerifier};
use openlegal_domain::RetrievalError as Error;
use rustix::fs::{self, Mode, OFlags};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeSet,
    fs::File,
    io::Read,
    os::unix::fs::MetadataExt,
    path::{Component, Path, PathBuf},
};
use uuid::Uuid;

const MARKER: &str = ".openlegal-seed-v1.json";
const MAX_MARKER: u64 = 16_384;
const DOMAIN: &[u8] = b"openlegal-blob-seed-binding-v1\n";
const ROLES: [&str; 4] = [
    "cache_primary",
    "cache_replica",
    "corpus_primary",
    "corpus_replica",
];

#[derive(Clone, Debug, PartialEq, Eq)]
struct DirectoryIdentity {
    dev: u64,
    ino: u64,
    uid: u32,
    gid: u32,
    mode: u32,
}
fn directory_identity(file: &File) -> Result<DirectoryIdentity, Error> {
    let m = file.metadata().map_err(unavailable)?;
    if !m.is_dir() {
        return Err(Error::StorageCorrupt);
    }
    Ok(DirectoryIdentity {
        dev: m.dev(),
        ino: m.ino(),
        uid: m.uid(),
        gid: m.gid(),
        mode: m.mode(),
    })
}
struct Directory {
    file: File,
    identity: DirectoryIdentity,
}
#[derive(Debug, PartialEq, Eq)]
struct MarkerIdentity {
    dev: u64,
    ino: u64,
    uid: u32,
    gid: u32,
    mode: u32,
    links: u64,
    size: u64,
    mtime: (i64, i64),
    ctime: (i64, i64),
}
fn marker_identity(file: &File, owner: u32) -> Result<MarkerIdentity, Error> {
    let m = file.metadata().map_err(unavailable)?;
    if !m.is_file()
        || m.uid() != owner
        || m.nlink() != 1
        || !matches!(m.mode() & 0o7777, 0o644 | 0o640)
        || m.len() == 0
        || m.len() > MAX_MARKER
    {
        return Err(Error::StorageCorrupt);
    }
    Ok(MarkerIdentity {
        dev: m.dev(),
        ino: m.ino(),
        uid: m.uid(),
        gid: m.gid(),
        mode: m.mode(),
        links: m.nlink(),
        size: m.len(),
        mtime: (m.mtime(), m.mtime_nsec()),
        ctime: (m.ctime(), m.ctime_nsec()),
    })
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Marker {
    schema_version: u32,
    kind: String,
    managed_epoch: String,
    seed_operation: String,
    role: String,
    manifest_sha256: String,
    correspondence_sha256: String,
    filesystem_id: String,
}
impl Marker {
    fn validate(&self, epoch: Uuid, role: &str) -> Result<(), Error> {
        if self.schema_version != 1
            || self.kind != "openlegal_blob_seed"
            || canonical_uuid(&self.managed_epoch)? != epoch
            || self.role != role
            || !digest(&self.manifest_sha256)
            || !digest(&self.correspondence_sha256)
            || self.filesystem_id.is_empty()
            || self.filesystem_id.len() > 128
            || !self
                .filesystem_id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b':' | b'-' | b'_' | b'.'))
        {
            return Err(Error::StorageCorrupt);
        }
        canonical_uuid(&self.seed_operation)?;
        Ok(())
    }
}

// Field declaration order is lexicographic, independently of serde_json Map's
// optional feature flags. The Python producer uses sort_keys=True.
#[derive(Serialize)]
struct BindingDocument {
    boot_id: String,
    kind: &'static str,
    managed_epoch: String,
    roots: Vec<RootDocument>,
    schema_version: u32,
}
#[derive(Serialize)]
struct RootDocument {
    container_path: String,
    filesystem_id: String,
    kernel_filesystem_id: u64,
    marker_sha256: String,
    role: String,
    root_dev: u64,
    root_gid: u32,
    root_ino: u64,
    root_mode: u32,
    root_uid: u32,
}
struct BoundRoot {
    role: String,
    path: PathBuf,
    directories: Vec<Directory>,
    filesystem_id: u64,
    marker_file: File,
    marker_identity: MarkerIdentity,
    marker_sha256: String,
}

/// Retains every opened ancestry/root descriptor until dropped. This proves
/// configuration/seed identity, not the contents' ongoing immutability or RPO0.
pub struct SeedBinding {
    epoch: Uuid,
    boot_id: String,
    roots: Vec<BoundRoot>,
    sha256: String,
    marker_owner: u32,
}
impl BlobRootVerifier for SeedBinding {
    fn verify(&self, role: &str, actual: BlobRootIdentity) -> Result<(), Error> {
        // Compare only the identities captured from retained descriptors during
        // seed open. Reopening a pathname here could authenticate A -> B -> A
        // while the BlobStore still holds the substituted B descriptor.
        let root = self
            .roots
            .iter()
            .find(|root| root.role == role)
            .ok_or(Error::StorageCorrupt)?;
        let held = root.directories.last().ok_or(Error::StorageCorrupt)?;
        if actual.device != held.identity.dev
            || actual.inode != held.identity.ino
            || actual.filesystem != root.filesystem_id
        {
            return Err(Error::StorageCorrupt);
        }
        Ok(())
    }
}

impl SeedBinding {
    pub fn sha256(&self) -> String {
        self.sha256.clone()
    }

    pub fn revalidate(&self) -> Result<(), Error> {
        if boot_id()? != self.boot_id {
            return Err(Error::StorageUnavailable);
        }
        for root in &self.roots {
            let current = open_directories(&root.path)?;
            if current.len() != root.directories.len() {
                return Err(Error::StorageCorrupt);
            }
            for (held, now) in root.directories.iter().zip(&current) {
                if directory_identity(&held.file)? != held.identity || now.identity != held.identity
                {
                    return Err(Error::StorageCorrupt);
                }
            }
            let dir = current.last().ok_or(Error::InvalidInput)?;
            if kernel_filesystem_id(&dir.file)? != root.filesystem_id
                || marker_identity(&root.marker_file, self.marker_owner)? != root.marker_identity
            {
                return Err(Error::StorageCorrupt);
            }
            let (file, raw, marker) = read_marker(&dir.file, self.marker_owner)?;
            marker.validate(self.epoch, &root.role)?;
            if marker_identity(&file, self.marker_owner)? != root.marker_identity
                || hex(&Sha256::digest(&raw)) != root.marker_sha256
            {
                return Err(Error::StorageCorrupt);
            }
            // Reading the marker must not hide an ancestry/root replacement
            // which occurred after the first descriptor walk.
            let final_path = open_directories(&root.path)?;
            if final_path.len() != root.directories.len()
                || final_path
                    .iter()
                    .zip(&root.directories)
                    .any(|(now, held)| now.identity != held.identity)
            {
                return Err(Error::StorageCorrupt);
            }
        }
        Ok(())
    }
}

/// Open only an existing complete seed. No mode/owner/path is repaired here.
pub fn open(epoch: Uuid, roots: &[(&str, &Path)]) -> Result<SeedBinding, Error> {
    open_with_owner(epoch, roots, 0)
}
fn open_with_owner(
    epoch: Uuid,
    roots: &[(&str, &Path)],
    marker_owner: u32,
) -> Result<SeedBinding, Error> {
    if epoch.is_nil() || !matches!(roots.len(), 2 | 4) {
        return Err(Error::InvalidInput);
    }
    let roles: BTreeSet<_> = roots.iter().map(|(role, _)| *role).collect();
    if roles.len() != roots.len()
        || roles.iter().any(|role| !ROLES.contains(role))
        || roles.contains("cache_primary") != roles.contains("cache_replica")
        || roles.contains("corpus_primary") != roles.contains("corpus_replica")
    {
        return Err(Error::InvalidInput);
    }
    for (i, (_, a)) in roots.iter().enumerate() {
        canonical_path(a)?;
        for (_, b) in roots.iter().skip(i + 1) {
            if a.starts_with(b) || b.starts_with(a) {
                return Err(Error::InvalidInput);
            }
        }
    }
    let boot = boot_id()?;
    let mut sorted = roots.to_vec();
    sorted.sort_by_key(|(role, _)| *role);
    let mut held = Vec::new();
    let mut documents = Vec::new();
    let mut identities = BTreeSet::new();
    let mut seed_operation: Option<String> = None;
    for (role, path) in sorted {
        let directories = open_directories(path)?;
        let root = directories.last().ok_or(Error::InvalidInput)?;
        if root.identity.mode & 0o7777 != 0o700
            || root.identity.uid != rustix::process::geteuid().as_raw()
        {
            return Err(Error::StorageCorrupt);
        }
        if !identities.insert((root.identity.dev, root.identity.ino)) {
            return Err(Error::InvalidInput);
        }
        let filesystem_id = kernel_filesystem_id(&root.file)?;
        let (file, raw, marker) = read_marker(&root.file, marker_owner)?;
        marker.validate(epoch, role)?;
        if seed_operation
            .as_ref()
            .is_some_and(|operation| operation != &marker.seed_operation)
        {
            return Err(Error::StorageCorrupt);
        }
        seed_operation = Some(marker.seed_operation.clone());
        let marker_sha256 = hex(&Sha256::digest(&raw));
        let marker_id = marker_identity(&file, marker_owner)?;
        if marker_id.dev != root.identity.dev {
            // A stale marker overmounted from a config filesystem cannot
            // authenticate replacement/empty data media.
            return Err(Error::StorageCorrupt);
        }
        documents.push(RootDocument {
            container_path: canonical_path(path)?,
            filesystem_id: marker.filesystem_id,
            kernel_filesystem_id: filesystem_id,
            marker_sha256: marker_sha256.clone(),
            role: role.into(),
            root_dev: root.identity.dev,
            root_gid: root.identity.gid,
            root_ino: root.identity.ino,
            root_mode: root.identity.mode & 0o7777,
            root_uid: root.identity.uid,
        });
        held.push(BoundRoot {
            role: role.into(),
            path: path.into(),
            directories,
            filesystem_id,
            marker_identity: marker_id,
            marker_file: file,
            marker_sha256,
        });
    }
    let document = BindingDocument {
        boot_id: boot.clone(),
        kind: "openlegal_blob_seed_binding",
        managed_epoch: epoch.to_string(),
        roots: documents,
        schema_version: 1,
    };
    let binding = SeedBinding {
        epoch,
        boot_id: boot,
        roots: held,
        sha256: document_digest(&document)?,
        marker_owner,
    };
    binding.revalidate()?;
    Ok(binding)
}

fn canonical_path(path: &Path) -> Result<String, Error> {
    let text = path.to_str().ok_or(Error::InvalidInput)?;
    if !path.is_absolute()
        || text.len() > 4096
        || path.components().count() > 32
        || path == Path::new("/")
    {
        return Err(Error::InvalidInput);
    }
    let mut normalized = PathBuf::from("/");
    for part in path.components().skip(1) {
        let Component::Normal(part) = part else {
            return Err(Error::InvalidInput);
        };
        normalized.push(part);
    }
    if normalized.as_os_str() != path.as_os_str() {
        return Err(Error::InvalidInput);
    }
    Ok(text.into())
}
fn open_directories(path: &Path) -> Result<Vec<Directory>, Error> {
    canonical_path(path)?;
    let flags =
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC | OFlags::NONBLOCK;
    let first = File::from(fs::open("/", flags, Mode::empty()).map_err(unavailable)?);
    let mut result = vec![Directory {
        identity: directory_identity(&first)?,
        file: first,
    }];
    let mut current = PathBuf::from("/");
    for part in path.components().skip(1) {
        let Component::Normal(part) = part else {
            return Err(Error::InvalidInput);
        };
        let file = File::from(
            fs::openat(
                &result.last().ok_or(Error::Internal)?.file,
                part,
                flags,
                Mode::empty(),
            )
            .map_err(unavailable)?,
        );
        current.push(part);
        let identity = directory_identity(&file)?;
        let tmp =
            current == Path::new("/tmp") && identity.uid == 0 && identity.mode & 0o1777 == 0o1777;
        if !tmp
            && ((identity.uid != 0 && identity.uid != rustix::process::geteuid().as_raw())
                || identity.mode & 0o002 != 0
                || (identity.mode & 0o020 != 0
                    && identity.gid != rustix::process::getegid().as_raw()))
        {
            return Err(Error::StorageCorrupt);
        }
        result.push(Directory { file, identity });
    }
    Ok(result)
}
fn read_marker(dir: &File, owner: u32) -> Result<(File, Vec<u8>, Marker), Error> {
    let mut file = File::from(
        fs::openat(
            dir,
            MARKER,
            OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(unavailable)?,
    );
    let before = marker_identity(&file, owner)?;
    let mut bytes = Vec::new();
    (&mut file)
        .take(MAX_MARKER + 1)
        .read_to_end(&mut bytes)
        .map_err(unavailable)?;
    if bytes.len() as u64 != before.size || marker_identity(&file, owner)? != before {
        return Err(Error::StorageCorrupt);
    }
    let current = File::from(
        fs::openat(
            dir,
            MARKER,
            OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(unavailable)?,
    );
    if marker_identity(&current, owner)? != before {
        return Err(Error::StorageCorrupt);
    }
    let marker = serde_json::from_slice::<Marker>(&bytes).map_err(|_| Error::StorageCorrupt)?;
    Ok((file, bytes, marker))
}
fn kernel_filesystem_id(file: &File) -> Result<u64, Error> {
    Ok(fs::fstatvfs(file).map_err(unavailable)?.f_fsid)
}
fn boot_id() -> Result<String, Error> {
    let directories = open_directories(Path::new("/proc/sys/kernel/random"))?;
    let mut file = File::from(
        fs::openat(
            &directories.last().ok_or(Error::Internal)?.file,
            "boot_id",
            OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(unavailable)?,
    );
    let mut bytes = Vec::new();
    (&mut file)
        .take(65)
        .read_to_end(&mut bytes)
        .map_err(unavailable)?;
    if bytes.len() != 37 || bytes[36] != b'\n' {
        return Err(Error::StorageCorrupt);
    }
    let text = std::str::from_utf8(&bytes[..36]).map_err(|_| Error::StorageCorrupt)?;
    canonical_uuid(text)?;
    Ok(text.into())
}
fn canonical_uuid(text: &str) -> Result<Uuid, Error> {
    let id = Uuid::parse_str(text).map_err(|_| Error::StorageCorrupt)?;
    if id.is_nil() || id.to_string() != text {
        return Err(Error::StorageCorrupt);
    }
    Ok(id)
}
fn digest(text: &str) -> bool {
    text.len() == 64
        && text
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
fn unavailable(_: impl std::fmt::Debug) -> Error {
    Error::StorageUnavailable
}
fn document_digest(document: &BindingDocument) -> Result<String, Error> {
    let mut bytes = DOMAIN.to_vec();
    bytes.extend(serde_json::to_vec(document).map_err(|_| Error::Internal)?);
    bytes.push(b'\n');
    Ok(hex(&Sha256::digest(&bytes)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};
    use std::{
        fs as stdfs,
        os::unix::fs::{PermissionsExt, symlink},
    };

    struct Fixture {
        _temp: tempfile::TempDir,
        parent: PathBuf,
        a: PathBuf,
        b: PathBuf,
        epoch: Uuid,
    }
    impl Fixture {
        fn new() -> Self {
            let temp = tempfile::Builder::new()
                .prefix("seed-binding-")
                .tempdir_in("/tmp")
                .unwrap();
            stdfs::set_permissions(temp.path(), stdfs::Permissions::from_mode(0o700)).unwrap();
            let parent = temp.path().join("parent");
            stdfs::create_dir(&parent).unwrap();
            stdfs::set_permissions(&parent, stdfs::Permissions::from_mode(0o700)).unwrap();
            let a = parent.join("primary");
            let b = parent.join("replica");
            let epoch = Uuid::parse_str("11111111-1111-4111-8111-111111111111").unwrap();
            let fixture = Self {
                _temp: temp,
                parent,
                a,
                b,
                epoch,
            };
            for (role, path) in fixture.roots() {
                stdfs::create_dir(path).unwrap();
                stdfs::set_permissions(path, stdfs::Permissions::from_mode(0o700)).unwrap();
                fixture.marker(path, fixture.value(role));
            }
            fixture
        }
        fn roots(&self) -> [(&str, &Path); 2] {
            [("cache_primary", &self.a), ("cache_replica", &self.b)]
        }
        fn value(&self, role: &str) -> Value {
            json!({"schema_version":1,"kind":"openlegal_blob_seed","managed_epoch":self.epoch.to_string(),"seed_operation":"44444444-4444-4444-8444-444444444444","role":role,"manifest_sha256":"a".repeat(64),"correspondence_sha256":"b".repeat(64),"filesystem_id":format!("fixture:{role}")})
        }
        fn marker(&self, path: &Path, value: Value) {
            stdfs::write(path.join(MARKER), serde_json::to_vec(&value).unwrap()).unwrap();
            stdfs::set_permissions(path.join(MARKER), stdfs::Permissions::from_mode(0o644))
                .unwrap();
        }
        fn open(&self) -> Result<SeedBinding, Error> {
            open_with_owner(
                self.epoch,
                &self.roots(),
                rustix::process::geteuid().as_raw(),
            )
        }
    }

    #[test]
    fn complete_existing_seed_binds_order_independently_and_retains_descriptors() {
        let f = Fixture::new();
        let binding = f.open().unwrap();
        binding.revalidate().unwrap();
        let reverse = [
            ("cache_replica", f.b.as_path()),
            ("cache_primary", f.a.as_path()),
        ];
        assert_eq!(
            binding.sha256(),
            open_with_owner(f.epoch, &reverse, rustix::process::geteuid().as_raw())
                .unwrap()
                .sha256()
        );
        assert_eq!(binding.sha256().len(), 64);
        // The production function always fixes root0 marker ownership; the
        // internal expected-owner override is never a runtime configuration.
        if rustix::process::geteuid().is_root() {
            assert_eq!(
                open(f.epoch, &f.roots()).unwrap().sha256(),
                binding.sha256()
            );
        } else {
            assert!(open(f.epoch, &f.roots()).is_err());
        }
    }

    #[test]
    fn opened_root_binding_rejects_wrong_role_or_any_identity_component_without_io() {
        let f = Fixture::new();
        let binding = f.open().unwrap();
        let root = &binding.roots[0];
        let identity = &root.directories.last().unwrap().identity;
        let actual = BlobRootIdentity {
            device: identity.dev,
            inode: identity.ino,
            filesystem: root.filesystem_id,
        };
        // Verifier itself does no path/marker I/O; revalidation is a separate
        // startup obligation. This remains valid for the actual retained FD.
        stdfs::remove_file(root.path.join(MARKER)).unwrap();
        binding.verify(&root.role, actual).unwrap();
        for changed in [
            BlobRootIdentity {
                device: actual.device ^ 1,
                ..actual
            },
            BlobRootIdentity {
                inode: actual.inode ^ 1,
                ..actual
            },
            BlobRootIdentity {
                filesystem: actual.filesystem ^ 1,
                ..actual
            },
        ] {
            assert_eq!(
                binding.verify(&root.role, changed),
                Err(Error::StorageCorrupt)
            );
        }
        assert_eq!(
            binding.verify("unknown", actual),
            Err(Error::StorageCorrupt)
        );
        assert_eq!(
            binding.verify("cache_replica", actual),
            Err(Error::StorageCorrupt)
        );
    }

    #[test]
    fn path_restoration_cannot_authenticate_a_substituted_blob_descriptor() {
        let f = Fixture::new();
        let binding = f.open().unwrap();
        let saved = f.parent.join("saved-primary");
        stdfs::rename(&f.a, &saved).unwrap();
        stdfs::create_dir(&f.a).unwrap();
        stdfs::set_permissions(&f.a, stdfs::Permissions::from_mode(0o700)).unwrap();
        let substituted = open_directories(&f.a).unwrap();
        let held = substituted.last().unwrap();
        let actual = BlobRootIdentity {
            device: held.identity.dev,
            inode: held.identity.ino,
            filesystem: kernel_filesystem_id(&held.file).unwrap(),
        };
        stdfs::remove_dir(&f.a).unwrap();
        stdfs::rename(&saved, &f.a).unwrap();
        binding.revalidate().unwrap();
        assert_eq!(
            binding.verify("cache_primary", actual),
            Err(Error::StorageCorrupt)
        );
    }

    #[test]
    fn missing_root_or_marker_never_creates_or_repairs_anything() {
        let f = Fixture::new();
        let missing = f.parent.join("missing");
        assert!(
            open_with_owner(
                f.epoch,
                &[("cache_primary", &f.a), ("cache_replica", &missing)],
                rustix::process::geteuid().as_raw()
            )
            .is_err()
        );
        assert!(!missing.exists());
        stdfs::remove_file(f.a.join(MARKER)).unwrap();
        assert!(f.open().is_err());
        assert!(!f.a.join(MARKER).exists());
    }

    #[test]
    fn marker_identity_epoch_role_hashes_and_uuid_are_strict() {
        let f = Fixture::new();
        for (key, value) in [
            ("schema_version", json!(true)),
            ("schema_version", json!(2)),
            ("kind", json!("other")),
            ("managed_epoch", json!(Uuid::nil().to_string())),
            ("seed_operation", json!("not-a-uuid")),
            ("role", json!("corpus_primary")),
            ("manifest_sha256", json!("A".repeat(64))),
            ("correspondence_sha256", json!("b".repeat(63))),
            ("filesystem_id", json!("untrusted\npath")),
        ] {
            let mut marker = f.value("cache_primary");
            marker[key] = value;
            f.marker(&f.a, marker);
            assert!(f.open().is_err(), "{key}");
        }
        f.marker(&f.a, f.value("cache_primary"));
        assert!(
            open_with_owner(
                f.epoch,
                &f.roots(),
                rustix::process::geteuid().as_raw().wrapping_add(1)
            )
            .is_err()
        );
    }

    #[test]
    fn unknown_duplicate_nonfinite_and_oversized_json_are_refused() {
        let f = Fixture::new();
        let mut value = f.value("cache_primary");
        value["unexpected"] = json!(1);
        f.marker(&f.a, value);
        assert!(f.open().is_err());
        let original = serde_json::to_string(&f.value("cache_primary")).unwrap();
        for raw in [
            original.replacen('{', "{\"schema_version\":1,", 1),
            original.replace("\"schema_version\":1", "\"schema_version\":NaN"),
            "x".repeat(MAX_MARKER as usize + 1),
        ] {
            stdfs::write(f.a.join(MARKER), raw).unwrap();
            assert!(f.open().is_err());
        }
    }

    #[test]
    fn invalid_roles_duplicates_nested_and_noncanonical_paths_are_refused() {
        let f = Fixture::new();
        let uid = rustix::process::geteuid().as_raw();
        for roots in [
            vec![("cache_primary", f.a.as_path())],
            vec![
                ("cache_primary", f.a.as_path()),
                ("cache_primary", f.b.as_path()),
            ],
            vec![("cache_primary", f.a.as_path()), ("other", f.b.as_path())],
            vec![
                ("cache_primary", f.a.as_path()),
                ("cache_replica", f.a.as_path()),
            ],
        ] {
            assert!(open_with_owner(f.epoch, &roots, uid).is_err());
        }
        assert!(
            open_with_owner(
                f.epoch,
                &[
                    ("cache_primary", &f.a),
                    ("cache_replica", &f.a.join("nested"))
                ],
                uid
            )
            .is_err()
        );
        for p in [
            PathBuf::from("relative"),
            PathBuf::from(format!("{}/./primary", f.parent.display())),
            PathBuf::from(format!("{}//primary", f.parent.display())),
        ] {
            assert!(
                open_with_owner(
                    f.epoch,
                    &[("cache_primary", &p), ("cache_replica", &f.b)],
                    uid
                )
                .is_err()
            );
        }
    }

    #[test]
    fn mixing_distinct_seed_operations_under_one_epoch_is_refused() {
        let f = Fixture::new();
        let mut marker = f.value("cache_replica");
        marker["seed_operation"] = json!("55555555-5555-4555-8555-555555555555");
        f.marker(&f.b, marker);
        assert!(f.open().is_err());
    }

    #[test]
    fn symlink_hardlink_fifo_and_marker_permissions_are_refused() {
        let f = Fixture::new();
        let alias = f.parent.join("alias");
        symlink(&f.a, &alias).unwrap();
        assert!(
            open_with_owner(
                f.epoch,
                &[("cache_primary", &alias), ("cache_replica", &f.b)],
                rustix::process::geteuid().as_raw()
            )
            .is_err()
        );
        stdfs::remove_file(f.a.join(MARKER)).unwrap();
        symlink(f.b.join(MARKER), f.a.join(MARKER)).unwrap();
        assert!(f.open().is_err());
        stdfs::remove_file(f.a.join(MARKER)).unwrap();
        stdfs::hard_link(f.b.join(MARKER), f.a.join(MARKER)).unwrap();
        assert!(f.open().is_err());
        stdfs::remove_file(f.a.join(MARKER)).unwrap();
        let dir = File::open(&f.a).unwrap();
        fs::mknodat(&dir, MARKER, fs::FileType::Fifo, Mode::RUSR | Mode::WUSR, 0).unwrap();
        assert!(f.open().is_err());
        stdfs::remove_file(f.a.join(MARKER)).unwrap();
        f.marker(&f.a, f.value("cache_primary"));
        for mode in [0o600, 0o666, 0o664, 0o4644] {
            stdfs::set_permissions(f.a.join(MARKER), stdfs::Permissions::from_mode(mode)).unwrap();
            assert!(f.open().is_err());
        }
    }

    #[test]
    fn replaced_marker_or_root_or_ancestry_cannot_reuse_old_binding() {
        let f = Fixture::new();
        let binding = f.open().unwrap();
        let saved = f.a.join("marker-saved");
        stdfs::rename(f.a.join(MARKER), &saved).unwrap();
        f.marker(&f.a, f.value("cache_primary"));
        assert!(binding.revalidate().is_err());
        drop(binding);
        let binding = f.open().unwrap();
        stdfs::rename(&f.a, f.parent.join("old-primary")).unwrap();
        stdfs::create_dir(&f.a).unwrap();
        stdfs::set_permissions(&f.a, stdfs::Permissions::from_mode(0o700)).unwrap();
        f.marker(&f.a, f.value("cache_primary"));
        assert!(binding.revalidate().is_err());
        drop(binding);
        let binding = f.open().unwrap();
        stdfs::rename(&f.parent, f._temp.path().join("old-parent")).unwrap();
        stdfs::create_dir(&f.parent).unwrap();
        assert!(binding.revalidate().is_err());
    }

    #[test]
    fn generation_and_health_directory_changes_do_not_invalidate_seed_identity() {
        let f = Fixture::new();
        let binding = f.open().unwrap();
        stdfs::create_dir(f.a.join("00")).unwrap();
        stdfs::write(f.a.join("00/transient-health-fixture"), b"fixture").unwrap();
        binding.revalidate().unwrap();
    }

    #[test]
    fn sorted_json_domain_matches_python_stdlib_golden_vector() {
        let document = BindingDocument {
            boot_id: "22222222-2222-4222-8222-222222222222".into(),
            kind: "openlegal_blob_seed_binding",
            managed_epoch: "33333333-3333-4333-8333-333333333333".into(),
            roots: vec![RootDocument {
                container_path: "/var/lib/openlegal/cache-primary".into(),
                filesystem_id: "ext4:11111111-1111-4111-8111-111111111111".into(),
                kernel_filesystem_id: 987654321,
                marker_sha256: "a".repeat(64),
                role: "cache_primary".into(),
                root_dev: 2052,
                root_gid: 10004,
                root_ino: 29,
                root_mode: 448,
                root_uid: 10004,
            }],
            schema_version: 1,
        };
        // Independently generated with hashlib + json.dumps(sort_keys=True,
        // ensure_ascii=False,separators=(',',':')) and the documented domain/LF.
        assert_eq!(
            document_digest(&document).unwrap(),
            "ab8ca6ef742eacf1396e7917e3e4fc5dd287f5840dc8312acb758ef3565afe6f"
        );
    }
}
