//! Pure, versioned identity encoding and retained-evidence validation.
use super::{HistoryKey, PersistentKey};
use crate::{MAX_PROCESSED_BYTES, MAX_RAW_BYTES, StoredPayload};
use openlegal_domain::{
    Query, RetrievalData, RetrievalError, history::valid_snapshot_id, valid_identifier,
};
use sha2::{Digest, Sha256};
use std::{collections::HashSet, io::Write};

fn field(bytes: &mut Vec<u8>, value: &str) {
    bytes.extend_from_slice(&(value.len() as u64).to_be_bytes());
    bytes.extend_from_slice(value.as_bytes());
}

/// Hash input is a version tag, length-delimited UTF-8 fields, operation tag and
/// fixed-width big-endian integers. No trimming, case folding or JSON formatting
/// participates in equivalence. Processor/schema versions belong to head identity.
pub fn canonical_identity(key: &HistoryKey) -> Result<Vec<u8>, RetrievalError> {
    key.query.validate()?;
    if !valid_identifier(&key.namespace, 128)
        || !valid_identifier(&key.provider, 64)
        || !valid_identifier(&key.dataset, 64)
    {
        return Err(RetrievalError::InvalidInput);
    }
    let mut bytes = b"openlegal-query-identity-v1\0".to_vec();
    field(&mut bytes, &key.namespace);
    field(&mut bytes, &key.provider);
    field(&mut bytes, &key.dataset);
    field(&mut bytes, key.query.source());
    match &key.query {
        Query::Get { id, .. } => {
            bytes.push(0);
            field(&mut bytes, id);
        }
        Query::Search {
            query,
            page,
            page_size,
            ..
        } => {
            bytes.push(1);
            field(&mut bytes, query);
            bytes.extend_from_slice(&page.to_be_bytes());
            bytes.extend_from_slice(&page_size.to_be_bytes());
        }
    }
    Ok(bytes)
}

pub fn identity_digest(key: &HistoryKey) -> Result<[u8; 32], RetrievalError> {
    Ok(Sha256::digest(canonical_identity(key)?).into())
}

pub fn digest_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// Stable serialization of the supported typed output, independent of JSONB key
/// order. Bounded construction prevents oversized restored data from allocating
/// an unbounded additional serialization buffer.
pub fn processed_bytes(data: &RetrievalData) -> Result<Vec<u8>, RetrievalError> {
    struct Bounded(Vec<u8>);
    impl Write for Bounded {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            if buf.len() > MAX_PROCESSED_BYTES.saturating_sub(self.0.len()) {
                return Err(std::io::Error::other("processed payload limit"));
            }
            self.0.extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut bytes = Bounded(Vec::new());
    serde_json::to_writer(&mut bytes, data).map_err(|_| RetrievalError::ResourceLimit)?;
    Ok(bytes.0)
}

pub fn processed_digest(data: &RetrievalData) -> Result<[u8; 32], RetrievalError> {
    Ok(Sha256::digest(processed_bytes(data)?).into())
}

/// Revalidate source association, evidence, provenance and supported output shape.
/// Restored corruption is always a storage error, never ordinary cache absence.
pub fn validate_payload(key: &PersistentKey, value: &StoredPayload) -> Result<(), RetrievalError> {
    let corrupt = RetrievalError::StorageCorrupt;
    canonical_identity(&key.history).map_err(|_| corrupt)?;
    let provenance = &value.provenance;
    if key.schema_version != 1
        || key.processor_version.is_empty()
        || key.processor_version.len() > 128
        || provenance.provider != key.history.provider
        || provenance.dataset != key.history.dataset
        || provenance.processor_version != key.processor_version
        || value.raw.len() > MAX_RAW_BYTES
        || provenance.payload_sha256 != digest_hex(&value.raw)
        || provenance.source_reference.is_empty()
        || provenance.source_reference.len() > 2048
        || provenance.source_reference.contains(['?', '#', '@'])
        || provenance.source_reference.chars().any(char::is_control)
        || value
            .snapshot
            .as_ref()
            .is_some_and(|snapshot| !valid_snapshot_id(&snapshot.snapshot_id))
    {
        return Err(corrupt);
    }
    let record_valid = |record: &openlegal_domain::Record| {
        record.source == key.history.query.source()
            && valid_identifier(&record.id, 128)
            && record.synthetic
            && !record.title.is_empty()
            && record.title.len() <= 1024
            && record.body.len() <= 16 * 1024
    };
    let valid = match (&key.history.query, &value.data) {
        (Query::Get { id, .. }, RetrievalData::Get(record)) => {
            &record.id == id && record_valid(record)
        }
        (
            Query::Search {
                page, page_size, ..
            },
            RetrievalData::Search(result),
        ) => {
            let mut ids = HashSet::new();
            result.page == *page
                && result.page_size == *page_size
                && result.total <= 20_000
                && result.records.len() as u64
                    == u64::from(*page_size).min(
                        u64::from(result.total)
                            .saturating_sub(u64::from(*page) * u64::from(*page_size)),
                    )
                && result
                    .records
                    .iter()
                    .all(|record| record_valid(record) && ids.insert(&record.id))
        }
        _ => false,
    };
    if !valid {
        return Err(corrupt);
    }
    processed_bytes(&value.data).map_err(|_| corrupt)?;
    Ok(())
}

/// Corruption fingerprint of immutable capture metadata and its exact content
/// identities. Call with original capture provenance, before overlaying mutable
/// head validation. This detects accidental damage, not privileged tampering.
pub fn immutable_payload_digest(
    key: &PersistentKey,
    value: &StoredPayload,
) -> Result<[u8; 32], RetrievalError> {
    validate_payload(key, value)?;
    let mut bytes = b"openlegal-capture-integrity-v1\0".to_vec();
    bytes.extend_from_slice(&canonical_identity(&key.history)?);
    field(&mut bytes, &key.processor_version);
    bytes.extend_from_slice(&key.schema_version.to_be_bytes());
    bytes.extend_from_slice(&processed_digest(&value.data)?);
    bytes.extend_from_slice(&Sha256::digest(&value.raw));
    bytes.extend_from_slice(&(value.raw.len() as u64).to_be_bytes());
    field(&mut bytes, &value.provenance.source_reference);
    bytes.extend_from_slice(&value.provenance.retrieved_at.to_be_bytes());
    bytes.extend_from_slice(&value.provenance.validated_at.to_be_bytes());
    match &value.snapshot {
        Some(snapshot) => {
            bytes.push(1);
            field(&mut bytes, &snapshot.snapshot_id);
            bytes.extend_from_slice(&snapshot.captured_at.to_be_bytes());
        }
        None => bytes.push(0),
    }
    Ok(Sha256::digest(bytes).into())
}

#[cfg(test)]
mod tests;
