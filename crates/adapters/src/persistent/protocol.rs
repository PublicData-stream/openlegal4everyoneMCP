//! Private bounded protocol shared with the isolated filesystem process.
use openlegal_application::{
    StoredPayload,
    persistence::{HistoryKey, PersistentKey, RetentionPolicy, StorageMetrics},
};
use openlegal_domain::{
    Provenance, RetrievalData, RetrievalError,
    history::{SnapshotEnvelope, SnapshotPage, SnapshotReference},
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::{
    io::{Read, Write},
    sync::Arc,
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub const MAX_FRAME: usize = 2 * 1024 * 1024;
const MAX_METADATA: usize = 256 * 1024;
const MAGIC: &[u8; 8] = b"OLCACHE1";

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Payload {
    pub data: RetrievalData,
    pub provenance: Provenance,
    #[serde(skip)]
    pub raw: Vec<u8>,
    pub snapshot: Option<SnapshotReference>,
}
impl Payload {
    pub fn from_stored(value: &StoredPayload) -> Self {
        Self {
            data: value.data.clone(),
            provenance: value.provenance.clone(),
            raw: value.raw.clone(),
            snapshot: value.snapshot.clone(),
        }
    }
    pub fn into_stored(self) -> Arc<StoredPayload> {
        let bytes = self.raw.len()
            + serde_json::to_vec(&self.data).map_or(0, |v| v.len())
            + self.provenance.source_reference.len()
            + 1024;
        Arc::new(StoredPayload {
            data: self.data,
            provenance: self.provenance,
            raw: self.raw,
            bytes,
            snapshot: self.snapshot,
        })
    }
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "command", deny_unknown_fields)]
pub enum Request {
    Open {
        root: String,
        policy: RetentionPolicy,
    },
    Lookup {
        key: PersistentKey,
        now: u64,
    },
    Publish {
        key: PersistentKey,
        value: Box<Payload>,
        now: u64,
    },
    List {
        key: HistoryKey,
        cursor: Option<String>,
        limit: usize,
        now: u64,
    },
    Get {
        key: HistoryKey,
        id: String,
        now: u64,
    },
    Maintain {
        now: u64,
    },
    Commit,
    Close,
}
impl Request {
    pub fn raw(&self) -> &[u8] {
        if let Self::Publish { value, .. } = self {
            &value.raw
        } else {
            &[]
        }
    }
    pub fn attach(&mut self, raw: Vec<u8>) -> Result<(), RetrievalError> {
        if let Self::Publish { value, .. } = self {
            value.raw = raw;
            Ok(())
        } else if raw.is_empty() {
            Ok(())
        } else {
            Err(RetrievalError::InvalidPayload)
        }
    }
}

#[derive(Serialize, Deserialize)]
pub enum Response {
    Ready,
    Prepared,
    Payload(Option<Payload>),
    List(SnapshotPage),
    Snapshot(SnapshotEnvelope),
    Done,
    Error(ErrorCode),
}
#[derive(Serialize, Deserialize)]
pub enum ErrorCode {
    InvalidInput,
    NotFound,
    Busy,
    ResourceLimit,
    InvalidPayload,
    Unavailable,
    StorageUnavailable,
    StorageCorrupt,
    StorageCapacity,
    SnapshotUnavailable,
}
impl From<RetrievalError> for ErrorCode {
    fn from(e: RetrievalError) -> Self {
        match e {
            RetrievalError::StorageUnavailable => Self::StorageUnavailable,
            RetrievalError::StorageCorrupt => Self::StorageCorrupt,
            RetrievalError::StorageCapacity => Self::StorageCapacity,
            RetrievalError::SnapshotUnavailable => Self::SnapshotUnavailable,
            RetrievalError::InvalidInput => Self::InvalidInput,
            RetrievalError::NotFound => Self::NotFound,
            RetrievalError::Busy => Self::Busy,
            RetrievalError::ResourceLimit => Self::ResourceLimit,
            RetrievalError::InvalidPayload => Self::InvalidPayload,
            _ => Self::StorageUnavailable,
        }
    }
}
impl From<ErrorCode> for RetrievalError {
    fn from(e: ErrorCode) -> Self {
        match e {
            ErrorCode::StorageUnavailable => Self::StorageUnavailable,
            ErrorCode::StorageCorrupt => Self::StorageCorrupt,
            ErrorCode::StorageCapacity => Self::StorageCapacity,
            ErrorCode::SnapshotUnavailable => Self::SnapshotUnavailable,
            ErrorCode::InvalidInput => Self::InvalidInput,
            ErrorCode::NotFound => Self::NotFound,
            ErrorCode::Busy => Self::Busy,
            ErrorCode::ResourceLimit => Self::ResourceLimit,
            ErrorCode::InvalidPayload => Self::InvalidPayload,
            ErrorCode::Unavailable => Self::StorageUnavailable,
        }
    }
}
#[derive(Serialize, Deserialize)]
pub struct Reply {
    pub response: Response,
    pub metrics: StorageMetrics,
    pub invalidates_memory: bool,
}
impl Reply {
    pub fn raw(&self) -> &[u8] {
        if let Response::Payload(Some(p)) = &self.response {
            &p.raw
        } else {
            &[]
        }
    }
    pub fn attach(&mut self, raw: Vec<u8>) -> Result<(), RetrievalError> {
        if let Response::Payload(Some(p)) = &mut self.response {
            p.raw = raw;
            Ok(())
        } else if raw.is_empty() {
            Ok(())
        } else {
            Err(RetrievalError::InvalidPayload)
        }
    }
}
pub fn encode<T: Serialize>(value: &T, raw: &[u8]) -> Result<Vec<u8>, RetrievalError> {
    let metadata = serde_json::to_vec(value).map_err(|_| RetrievalError::InvalidPayload)?;
    if metadata.len() > MAX_METADATA
        || raw.len() > 1024 * 1024
        || metadata.len() + raw.len() + 16 > MAX_FRAME
    {
        return Err(RetrievalError::ResourceLimit);
    }
    let mut out = Vec::with_capacity(16 + metadata.len() + raw.len());
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(&(metadata.len() as u32).to_be_bytes());
    out.extend_from_slice(&(raw.len() as u32).to_be_bytes());
    out.extend_from_slice(&metadata);
    out.extend_from_slice(raw);
    Ok(out)
}
fn lengths(header: &[u8; 16]) -> Result<(usize, usize), RetrievalError> {
    if &header[..8] != MAGIC {
        return Err(RetrievalError::InvalidPayload);
    }
    let a = u32::from_be_bytes(
        header[8..12]
            .try_into()
            .map_err(|_| RetrievalError::InvalidPayload)?,
    ) as usize;
    let b = u32::from_be_bytes(
        header[12..]
            .try_into()
            .map_err(|_| RetrievalError::InvalidPayload)?,
    ) as usize;
    if a > MAX_METADATA || b > 1024 * 1024 || a + b + 16 > MAX_FRAME {
        return Err(RetrievalError::ResourceLimit);
    }
    Ok((a, b))
}
pub fn read<T: DeserializeOwned>(reader: &mut impl Read) -> Result<(T, Vec<u8>), RetrievalError> {
    let mut header = [0; 16];
    reader
        .read_exact(&mut header)
        .map_err(|_| RetrievalError::StorageUnavailable)?;
    let (a, b) = lengths(&header)?;
    let mut metadata = vec![0; a];
    let mut raw = vec![0; b];
    reader
        .read_exact(&mut metadata)
        .and_then(|()| reader.read_exact(&mut raw))
        .map_err(|_| RetrievalError::StorageUnavailable)?;
    Ok((
        serde_json::from_slice(&metadata).map_err(|_| RetrievalError::InvalidPayload)?,
        raw,
    ))
}
pub fn write<T: Serialize>(
    writer: &mut impl Write,
    value: &T,
    raw: &[u8],
) -> Result<(), RetrievalError> {
    writer
        .write_all(&encode(value, raw)?)
        .and_then(|()| writer.flush())
        .map_err(|_| RetrievalError::StorageUnavailable)
}
pub async fn read_async<T: DeserializeOwned>(
    reader: &mut (impl AsyncRead + Unpin),
) -> Result<(T, Vec<u8>), RetrievalError> {
    let mut header = [0; 16];
    reader
        .read_exact(&mut header)
        .await
        .map_err(|_| RetrievalError::StorageUnavailable)?;
    let (a, b) = lengths(&header)?;
    let mut metadata = vec![0; a];
    let mut raw = vec![0; b];
    reader
        .read_exact(&mut metadata)
        .await
        .map_err(|_| RetrievalError::StorageUnavailable)?;
    reader
        .read_exact(&mut raw)
        .await
        .map_err(|_| RetrievalError::StorageUnavailable)?;
    Ok((
        serde_json::from_slice(&metadata).map_err(|_| RetrievalError::InvalidPayload)?,
        raw,
    ))
}
pub async fn write_async<T: Serialize>(
    writer: &mut (impl AsyncWrite + Unpin),
    value: &T,
    raw: &[u8],
) -> Result<(), RetrievalError> {
    writer
        .write_all(&encode(value, raw)?)
        .await
        .map_err(|_| RetrievalError::StorageUnavailable)?;
    writer
        .flush()
        .await
        .map_err(|_| RetrievalError::StorageUnavailable)
}
