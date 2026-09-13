//! Provider-neutral immutable evidence objects. SQL owns references and lifecycle.
use futures::future::BoxFuture;
use openlegal_domain::RetrievalError;
use tokio_util::sync::CancellationToken;

/// Digest identifies content; the opaque storage key identifies one physical generation.
/// Adapters validate keys and verify the exact digest and size on every accepted read.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlobLocation {
    pub digest: [u8; 32],
    pub size_bytes: u64,
    pub storage_key: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BlobPutResult {
    Created,
    AlreadyPresent,
}

#[derive(Clone, Debug, Default)]
pub struct BlobPage {
    pub objects: Vec<BlobLocation>,
    pub next_cursor: Option<String>,
}

#[derive(Clone, Debug, Default)]
pub struct BlobMetrics {
    pub reads: u64,
    pub writes: u64,
    pub deduplicated_puts: u64,
    pub deletions: u64,
    pub failures: u64,
    pub corruptions: u64,
    pub saturation: u64,
    pub active_jobs: u64,
}

/// Operations own bounded work until completion even if their waiting caller leaves.
/// A successful put, including an existing object, establishes durable publication.
/// Enumeration is maintenance-only; normal history lookup never scans object storage.
pub trait BlobStore: Send + Sync + 'static {
    fn put_if_absent(
        &self,
        location: BlobLocation,
        bytes: Vec<u8>,
        cancellation: CancellationToken,
    ) -> BoxFuture<'static, Result<BlobPutResult, RetrievalError>>;
    fn get(
        &self,
        location: BlobLocation,
        cancellation: CancellationToken,
    ) -> BoxFuture<'static, Result<Option<Vec<u8>>, RetrievalError>>;
    fn delete_if_present(
        &self,
        location: BlobLocation,
        cancellation: CancellationToken,
    ) -> BoxFuture<'static, Result<(), RetrievalError>>;
    fn enumerate(
        &self,
        cursor: Option<String>,
        limit: usize,
        cancellation: CancellationToken,
    ) -> BoxFuture<'static, Result<BlobPage, RetrievalError>>;
    fn cleanup_staging(
        &self,
        now: u64,
        limit: usize,
        cancellation: CancellationToken,
    ) -> BoxFuture<'static, Result<usize, RetrievalError>>;
    fn health(
        &self,
        cancellation: CancellationToken,
    ) -> BoxFuture<'static, Result<(), RetrievalError>>;
    fn close(&self) -> BoxFuture<'static, Result<(), RetrievalError>>;
    fn metrics(&self) -> BlobMetrics;
}
