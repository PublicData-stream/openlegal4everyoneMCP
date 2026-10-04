//! Exact original evidence selected exclusively from a capture's authorized
//! resource metadata. Provider URLs and filesystem paths are never inputs.
use super::*;
pub use openlegal_domain::rights::OriginalEvidence;

fn evidence_ordinal(
    metadata: &std::collections::BTreeMap<String, String>,
    source_ordinal: u32,
) -> Result<i32, DatabaseError> {
    let Some(mapping) = metadata.get("attachment_evidence_ordinals") else {
        return i32::try_from(source_ordinal).map_err(|_| DatabaseError::RevisionUnavailable);
    };
    let ordinals: Vec<u32> =
        serde_json::from_str(mapping).map_err(|_| DatabaseError::RevisionUnavailable)?;
    let unique: std::collections::BTreeSet<_> = ordinals.iter().copied().collect();
    if ordinals.len() > 64 || unique.len() != ordinals.len() || unique.contains(&0) {
        return Err(DatabaseError::RevisionUnavailable);
    }
    if source_ordinal == 0 {
        return Ok(0);
    }
    let position = ordinals
        .iter()
        .position(|ordinal| *ordinal == source_ordinal)
        .ok_or(DatabaseError::RevisionUnavailable)?;
    i32::try_from(position + 1).map_err(|_| DatabaseError::RevisionUnavailable)
}

impl PgCorpusStore {
    /// Returns the original response (ordinal zero) or attached evidence without
    /// transforming its bytes. Legacy captures lacking verified resource rights
    /// cannot expose their raw evidence through this interface.
    pub async fn original_evidence(
        &self,
        capture_id: &str,
        ordinal: u32,
        cancel: CancellationToken,
    ) -> Result<OriginalEvidence, DatabaseError> {
        if !openlegal_domain::history::valid_snapshot_id(capture_id) {
            return Err(DatabaseError::InvalidInput);
        }
        // This verifies the complete retained capture and withdrawal status.
        // Permanent archival availability does not depend on a citation lease.
        let capture = self.capture(capture_id, 0, cancel.clone()).await?;
        let resources = openlegal_domain::rights::resources(&capture.record.metadata);
        let mut candidates = resources
            .into_iter()
            .filter(|resource| resource.ordinal == ordinal);
        let resource = candidates
            .next()
            .ok_or(DatabaseError::RevisionUnavailable)?;
        if candidates.next().is_some() || !resource.retained || !resource.rights.can_store() {
            return Err(DatabaseError::RevisionUnavailable);
        }
        // Provider ordinals include skipped files; stored ordinals count only retained blobs.
        let stored_ordinal = evidence_ordinal(&capture.record.metadata, ordinal)?;
        let row = if stored_ordinal == 0 {
            sqlx::query("SELECT storage_key,raw_sha256,raw_size FROM openlegal.corpus_capture WHERE id=$1")
                .bind(capture_id).fetch_optional(&self.pool).await.map_err(db)?
        } else {
            sqlx::query("SELECT storage_key,raw_sha256,raw_size FROM openlegal.corpus_capture_blob WHERE capture_id=$1 AND ordinal=$2")
                .bind(capture_id).bind(stored_ordinal).fetch_optional(&self.pool).await.map_err(db)?
        }.ok_or(DatabaseError::RevisionUnavailable)?;
        let digest: Vec<u8> = row.try_get("raw_sha256").map_err(db)?;
        let size: u64 = row
            .try_get::<i64, _>("raw_size")
            .map_err(db)?
            .try_into()
            .map_err(corrupt)?;
        let location = BlobLocation {
            digest: digest.clone().try_into().map_err(corrupt)?,
            size_bytes: size,
            storage_key: row.try_get("storage_key").map_err(db)?,
        };
        let bytes = self
            .blobs
            .get(location, cancel.clone())
            .await
            .map_err(|error| {
                let error = blob_error(error);
                if error == DatabaseError::StorageCorrupt {
                    self.blocked.store(true, Ordering::Release);
                }
                error
            })?;
        let Some(bytes) = bytes else {
            self.blocked.store(true, Ordering::Release);
            return Err(DatabaseError::StorageCorrupt);
        };
        if bytes.len() as u64 != size || bytes_hash(&bytes) != digest {
            self.blocked.store(true, Ordering::Release);
            return Err(DatabaseError::StorageCorrupt);
        }
        check(&cancel)?;
        if self.state(&capture.record.object).await?.withdrawn {
            return Err(DatabaseError::Withdrawn);
        }
        Ok(OriginalEvidence {
            credentials_redacted: ordinal == 0
                && capture
                    .record
                    .metadata
                    .get("transport_credentials_redacted")
                    .is_some_and(|value| value == "true"),
            bytes,
            media_type: resource.media_type,
            title: resource.title,
            rights: resource.rights,
        })
    }
}

impl PgCorpusStore {
    pub(super) fn original_evidence_future(
        &self,
        capture_id: String,
        ordinal: u32,
        cancel: CancellationToken,
    ) -> BoxFuture<'static, Result<OriginalEvidence, DatabaseError>> {
        let this = self.clone();
        Box::pin(async move { this.original_evidence(&capture_id, ordinal, cancel).await })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn attachment_mapping_preserves_source_ordinals_and_rejects_ambiguous_lists() {
        let metadata = |mapping: &str| {
            std::collections::BTreeMap::from([(
                "attachment_evidence_ordinals".into(),
                mapping.into(),
            )])
        };
        assert_eq!(evidence_ordinal(&metadata("[2,5]"), 0), Ok(0));
        assert_eq!(evidence_ordinal(&metadata("[2,5]"), 2), Ok(1));
        assert_eq!(evidence_ordinal(&metadata("[2,5]"), 5), Ok(2));
        assert_eq!(
            evidence_ordinal(&metadata("[2,5]"), 1),
            Err(DatabaseError::RevisionUnavailable)
        );
        assert_eq!(evidence_ordinal(&Default::default(), 5), Ok(5));
        for mapping in [
            "[0]".to_owned(),
            "[2,2]".into(),
            "not json".into(),
            serde_json::to_string(&(1..=65).collect::<Vec<_>>()).unwrap(),
        ] {
            assert_eq!(
                evidence_ordinal(&metadata(&mapping), 0),
                Err(DatabaseError::RevisionUnavailable)
            );
            assert_eq!(
                evidence_ordinal(&metadata(&mapping), 2),
                Err(DatabaseError::RevisionUnavailable)
            );
        }
    }
}
