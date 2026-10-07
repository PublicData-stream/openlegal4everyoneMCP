//! Permanent byte-exact finite inventory and supplement observations. These do
//! not establish legal-object identity, currentness or corpus-wide completeness.
use super::*;
use openlegal_domain::rights::SourceRights;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

const MAX_OBSERVATION_BYTES: usize = 16 * 1024 * 1024;
const MAX_METADATA_BYTES: usize = 64 * 1024;

#[derive(Clone, Debug)]
pub struct SourceObservationInput {
    /// `law_go_kr:{registered guide}:{bounded non-sensitive context}`.
    pub source_key: String,
    /// None retains metadata/rights status without downloading restricted bytes.
    pub raw: Option<Vec<u8>>,
    pub media_type: String,
    pub rights: SourceRights,
    pub metadata: BTreeMap<String, String>,
    pub observed_at: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceObservation {
    pub observation_id: String,
    pub source_key: String,
    pub raw_sha256: Option<String>,
    pub raw_size: u64,
    pub media_type: String,
    pub rights: SourceRights,
    pub metadata: BTreeMap<String, String>,
    pub observed_at: u64,
    pub validated_at: u64,
}
impl SourceObservation {
    pub fn retained(&self) -> bool {
        self.raw_sha256.is_some()
    }
    fn manifest(&self) -> Result<Vec<u8>, DatabaseError> {
        let mut immutable = self.clone();
        immutable.validated_at = immutable.observed_at;
        serde_json::to_vec(&immutable).map_err(corrupt)
    }
    fn content_id(&self) -> Result<String, DatabaseError> {
        self.content_id_with_policy(true)
    }
    fn content_id_with_policy(&self, include_policy: bool) -> Result<String, DatabaseError> {
        let mut identity = self.source_key.as_bytes().to_vec();
        identity.push(0);
        if let Some(digest) = &self.raw_sha256 {
            identity.extend_from_slice(b"raw:");
            identity.extend_from_slice(digest.as_bytes());
            if include_policy {
                identity.extend_from_slice(b"\0rights-media-v2\0");
                identity.extend_from_slice(
                    &serde_json::to_vec(&(&self.rights, &self.media_type)).map_err(corrupt)?,
                );
            }
        } else {
            identity.extend_from_slice(b"metadata:");
            identity.extend_from_slice(
                &serde_json::to_vec(&(&self.media_type, &self.rights, &self.metadata))
                    .map_err(corrupt)?,
            );
        }
        Ok(hex(&bytes_hash(&identity)))
    }
}

fn valid_source_key(key: &str) -> bool {
    if key.is_empty()
        || key.len() > 1024
        || !key
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.' | b':'))
    {
        return false;
    }
    let mut fields = key.splitn(3, ':');
    fields.next() == Some("law_go_kr")
        && fields.next().is_some_and(|guide| {
            crate::law_go_kr::catalog::GUIDE_ENTRIES
                .iter()
                .any(|entry| entry.guide == guide)
        })
        && fields.next().is_some_and(|context| !context.is_empty())
}

fn validate(input: &SourceObservationInput) -> Result<(), DatabaseError> {
    if !valid_source_key(&input.source_key)
        || input.media_type.is_empty()
        || input.media_type.len() > 128
        || !input
            .media_type
            .bytes()
            .all(|byte| byte.is_ascii_graphic() || byte == b' ')
        || input.metadata.len() > 128
        || input
            .metadata
            .iter()
            .any(|(key, value)| key.is_empty() || key.len() > 128 || value.len() > 4096)
        || serde_json::to_vec(&(&input.rights, &input.metadata))
            .map_err(corrupt)?
            .len()
            > MAX_METADATA_BYTES
        || input
            .raw
            .as_ref()
            .is_some_and(|bytes| bytes.len() > MAX_OBSERVATION_BYTES || !input.rights.can_store())
    {
        return Err(DatabaseError::InvalidInput);
    }
    Ok(())
}

fn decode(row: &PgRow) -> Result<SourceObservation, DatabaseError> {
    let raw: Option<Vec<u8>> = row.try_get("raw_sha256").map_err(db)?;
    let observation = SourceObservation {
        observation_id: row.try_get("id").map_err(db)?,
        source_key: row.try_get("source_key").map_err(db)?,
        raw_sha256: raw.map(|digest| hex(&digest)),
        raw_size: row
            .try_get::<i64, _>("raw_size")
            .map_err(db)?
            .try_into()
            .map_err(corrupt)?,
        media_type: row.try_get("media_type").map_err(db)?,
        rights: serde_json::from_value(row.try_get("rights").map_err(db)?).map_err(corrupt)?,
        metadata: serde_json::from_value(row.try_get("metadata").map_err(db)?).map_err(corrupt)?,
        observed_at: unsigned(row, "observed_at")?,
        validated_at: unsigned(row, "validated_at")?,
    };
    let expected: Vec<u8> = row.try_get("manifest_sha256").map_err(db)?;
    if (observation.content_id()? != observation.observation_id
        && observation.content_id_with_policy(false)? != observation.observation_id)
        || bytes_hash(&observation.manifest()?) != expected
        || !valid_source_key(&observation.source_key)
        || observation
            .raw_sha256
            .as_ref()
            .is_some_and(|digest| !openlegal_domain::history::valid_snapshot_id(digest))
        || observation.validated_at < observation.observed_at
    {
        return Err(DatabaseError::StorageCorrupt);
    }
    Ok(observation)
}

const SELECT_OBSERVATION: &str = "SELECT id,source_key,raw_sha256,raw_size,storage_key,media_type,rights,metadata,observed_at::text,validated_at::text,manifest_sha256 FROM openlegal.corpus_source_observation WHERE id=$1";

impl PgCorpusStore {
    pub async fn source_observation(
        &self,
        id: &str,
        cancel: CancellationToken,
    ) -> Result<SourceObservation, DatabaseError> {
        self.gate().await?;
        check(&cancel)?;
        if !openlegal_domain::history::valid_snapshot_id(id) {
            return Err(DatabaseError::InvalidInput);
        }
        let row = sqlx::query(SELECT_OBSERVATION)
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .map_err(db)?
            .ok_or(DatabaseError::RevisionUnavailable)?;
        let result = decode(&row);
        if result == Err(DatabaseError::StorageCorrupt) {
            self.blocked.store(true, Ordering::Release);
        }
        check(&cancel)?;
        result
    }

    /// Internal archive read; no public MCP route or legal citation is inferred.
    pub async fn source_observation_bytes(
        &self,
        id: &str,
        cancel: CancellationToken,
    ) -> Result<Vec<u8>, DatabaseError> {
        self.gate().await?;
        check(&cancel)?;
        if !openlegal_domain::history::valid_snapshot_id(id) {
            return Err(DatabaseError::InvalidInput);
        }
        let row = sqlx::query(SELECT_OBSERVATION)
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .map_err(db)?
            .ok_or(DatabaseError::RevisionUnavailable)?;
        let observation = decode(&row).inspect_err(|error| {
            if *error == DatabaseError::StorageCorrupt {
                self.blocked.store(true, Ordering::Release);
            }
        })?;
        if !observation.retained() || !observation.rights.can_store() {
            return Err(DatabaseError::RevisionUnavailable);
        }
        let digest: Vec<u8> = row.try_get("raw_sha256").map_err(db)?;
        let location = BlobLocation {
            digest: digest.clone().try_into().map_err(corrupt)?,
            size_bytes: observation.raw_size,
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
        if bytes.len() as u64 != observation.raw_size || bytes_hash(&bytes) != digest {
            self.blocked.store(true, Ordering::Release);
            return Err(DatabaseError::StorageCorrupt);
        }
        check(&cancel)?;
        Ok(bytes)
    }

    pub async fn retain_source_observation(
        &self,
        input: SourceObservationInput,
        cancel: CancellationToken,
    ) -> Result<SourceObservation, DatabaseError> {
        self.gate().await?;
        check(&cancel)?;
        validate(&input)?;
        let mut observation = SourceObservation {
            observation_id: String::new(),
            source_key: input.source_key,
            raw_sha256: input.raw.as_ref().map(|raw| hex(&bytes_hash(raw))),
            raw_size: input.raw.as_ref().map_or(0, |raw| raw.len() as u64),
            media_type: input.media_type,
            rights: input.rights,
            metadata: input.metadata,
            observed_at: input.observed_at,
            validated_at: input.observed_at,
        };
        observation.observation_id = observation.content_id()?;
        if let Some(row) = retry_storage(&cancel, "observation_existing", || async {
            sqlx::query(SELECT_OBSERVATION)
                .bind(&observation.observation_id)
                .fetch_optional(&self.pool)
                .await
                .map_err(db)
        })
        .await?
        {
            let mut prior = decode(&row).inspect_err(|error| {
                if *error == DatabaseError::StorageCorrupt {
                    self.blocked.store(true, Ordering::Release);
                }
            })?;
            if prior.retained() {
                self.source_observation_bytes(&prior.observation_id, cancel.clone())
                    .await?;
            }
            check(&cancel)?;
            retry_storage(&cancel, "observation_revalidate", || async {
                sqlx::query("UPDATE openlegal.corpus_source_observation SET validated_at=GREATEST(validated_at,$2::text::numeric) WHERE id=$1")
                    .bind(&prior.observation_id).bind(input.observed_at.to_string()).execute(&self.pool).await.map_err(db)
            }).await?;
            prior.validated_at = prior.validated_at.max(input.observed_at);
            check(&cancel)?;
            return Ok(prior);
        }
        let digest = input.raw.as_ref().map(|raw| bytes_hash(raw));
        let location = if let Some(digest) = &digest {
            let generation: Uuid = retry_storage(&cancel, "observation_generation", || async {
                sqlx::query_scalar("SELECT pg_catalog.uuidv7()")
                    .fetch_one(&self.pool)
                    .await
                    .map_err(db)
            })
            .await?;
            let hex = hex(digest);
            Some(format!("{}/{}-{generation}", &hex[..2], hex))
        } else {
            None
        };
        if let Some(location) = &location {
            retry_storage(&cancel, "observation_reserve", || async {
            let mut tx = self.pool.begin().await.map_err(db)?;
            let counts = sqlx::query("SELECT raw_bytes,staged_bytes,max_raw_bytes::text,(SELECT count(*) FROM openlegal.corpus_staging) AS stages FROM openlegal.corpus_control WHERE singleton FOR UPDATE")
                .fetch_one(&mut *tx).await.map_err(db)?;
            let staged: i64 = counts.try_get("staged_bytes").map_err(db)?;
            let total = counts
                .try_get::<i64, _>("raw_bytes")
                .map_err(db)?
                .checked_add(staged)
                .and_then(|bytes| bytes.checked_add(observation.raw_size as i64))
                .ok_or(DatabaseError::Capacity)?;
            let cap = counts
                .try_get::<Option<String>, _>("max_raw_bytes")
                .map_err(db)?
                .map(|bytes| bytes.parse::<u64>())
                .transpose()
                .map_err(corrupt)?;
            if cap.is_some_and(|cap| total as u64 > cap)
                || staged + observation.raw_size as i64 > 16_i64 * 1024 * 1024 * 1024
                || counts.try_get::<i64, _>("stages").map_err(db)? >= 128 * 65
            {
                return Err(DatabaseError::Capacity);
            }
            sqlx::query("INSERT INTO openlegal.corpus_staging(storage_key,raw_sha256,raw_size,created_at) VALUES($1,$2,$3,$4::text::numeric)")
                .bind(location).bind(&digest).bind(observation.raw_size as i64).bind(input.observed_at.max(self.publication_clock.now()).to_string()).execute(&mut *tx).await.map_err(db)?;
            sqlx::query("UPDATE openlegal.corpus_control SET staged_bytes=staged_bytes+$1")
                .bind(observation.raw_size as i64)
                .execute(&mut *tx)
                .await
                .map_err(db)?;
            check(&cancel)?;
            tx.commit().await.map_err(db)?;
            Ok(())
            }).await?;
            self.blobs
                .put_if_absent(
                    BlobLocation {
                        digest: digest
                            .clone()
                            .ok_or(DatabaseError::StorageCorrupt)?
                            .try_into()
                            .map_err(corrupt)?,
                        size_bytes: observation.raw_size,
                        storage_key: location.clone(),
                    },
                    input.raw.ok_or(DatabaseError::StorageCorrupt)?,
                    cancel.clone(),
                )
                .await
                .map_err(blob_error)?;
        }
        check(&cancel)?;
        retry_storage(&cancel, "observation_commit", || async {
        let mut tx = self.pool.begin().await.map_err(db)?;
        sqlx::query("SELECT singleton FROM openlegal.corpus_control WHERE singleton FOR UPDATE")
            .fetch_one(&mut *tx)
            .await
            .map_err(db)?;
        // Cancellation/crash leaves a bounded stage for normal temporary cleanup.
        // A stage removed by maintenance may never become a retained reference.
        if let Some(location) = &location {
            let staged: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM openlegal.corpus_staging WHERE storage_key=$1)",
            )
            .bind(location)
            .fetch_one(&mut *tx)
            .await
            .map_err(db)?;
            if !staged {
                return Err(DatabaseError::Conflict);
            }
        }
        let changed = sqlx::query("INSERT INTO openlegal.corpus_source_observation(id,source_key,raw_sha256,raw_size,storage_key,media_type,rights,metadata,observed_at,validated_at,manifest_sha256) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9::text::numeric,$9::text::numeric,$10) ON CONFLICT(id) DO NOTHING")
            .bind(&observation.observation_id).bind(&observation.source_key).bind(&digest).bind(observation.raw_size as i64).bind(&location).bind(&observation.media_type)
            .bind(serde_json::to_value(&observation.rights).map_err(corrupt)?).bind(serde_json::to_value(&observation.metadata).map_err(corrupt)?)
            .bind(input.observed_at.to_string()).bind(bytes_hash(&observation.manifest()?)).execute(&mut *tx).await.map_err(db)?.rows_affected();
        if changed == 0 {
            // Duplicate content never increases retained bytes. Queue the losing
            // physical generation for ordinary deletion and promptly refund staging.
            sqlx::query("UPDATE openlegal.corpus_source_observation SET validated_at=GREATEST(validated_at,$2::text::numeric) WHERE id=$1")
                .bind(&observation.observation_id).bind(input.observed_at.to_string()).execute(&mut *tx).await.map_err(db)?;
            if let Some(location) = &location {
                sqlx::query("INSERT INTO openlegal.corpus_blob_deletion(storage_key,raw_sha256,raw_size) VALUES($1,$2,$3) ON CONFLICT DO NOTHING")
                    .bind(location).bind(&digest).bind(observation.raw_size as i64).execute(&mut *tx).await.map_err(db)?;
                sqlx::query("DELETE FROM openlegal.corpus_staging WHERE storage_key=$1")
                    .bind(location)
                    .execute(&mut *tx)
                    .await
                    .map_err(db)?;
                sqlx::query("UPDATE openlegal.corpus_control SET staged_bytes=staged_bytes-$1")
                    .bind(observation.raw_size as i64)
                    .execute(&mut *tx)
                    .await
                    .map_err(db)?;
            }
        } else if let Some(location) = &location {
            sqlx::query("DELETE FROM openlegal.corpus_staging WHERE storage_key=$1")
                .bind(location)
                .execute(&mut *tx)
                .await
                .map_err(db)?;
            sqlx::query("UPDATE openlegal.corpus_control SET raw_bytes=raw_bytes+$1,staged_bytes=staged_bytes-$1")
                .bind(observation.raw_size as i64).execute(&mut *tx).await.map_err(db)?;
        }
        check(&cancel)?;
        tx.commit().await.map_err(db)?;
        Ok(())
        }).await?;
        // A committed observation is never re-inserted to retry its readback.
        let retained = retry_storage(&cancel, "observation_readback", || {
            self.source_observation(&observation.observation_id, cancel.clone())
        })
        .await?;
        if retained.retained() {
            self.source_observation_bytes(&retained.observation_id, cancel)
                .await?;
        }
        Ok(retained)
    }
}

#[cfg(test)]
mod identity_tests {
    use super::*;
    fn observation() -> SourceObservation {
        SourceObservation {
            observation_id: String::new(),
            source_key: "law_go_kr:lsEfYdInfoGuide:fixture".into(),
            raw_sha256: Some("ab".repeat(32)),
            raw_size: 1,
            media_type: "application/xml".into(),
            rights: SourceRights::legal_information(),
            metadata: BTreeMap::new(),
            observed_at: 100,
            validated_at: 100,
        }
    }
    #[test]
    fn raw_identity_separates_rights_and_media_corrections_without_changing_legacy_ids() {
        let original = observation();
        let mut corrected = original.clone();
        corrected.rights.attribution = "Corrected issuer attribution".into();
        assert_ne!(
            original.content_id().unwrap(),
            corrected.content_id().unwrap()
        );
        assert_eq!(
            original.content_id_with_policy(false).unwrap(),
            corrected.content_id_with_policy(false).unwrap()
        );
        assert_ne!(
            bytes_hash(&original.manifest().unwrap()),
            bytes_hash(&corrected.manifest().unwrap())
        );
        corrected = original.clone();
        corrected.media_type = "text/xml".into();
        assert_ne!(
            original.content_id().unwrap(),
            corrected.content_id().unwrap()
        );
        corrected = original.clone();
        corrected.validated_at = 200;
        assert_eq!(
            original.content_id().unwrap(),
            corrected.content_id().unwrap()
        );
    }
    #[tokio::test]
    #[ignore = "requires scripts/test-postgres.sh"]
    async fn legacy_ids_remain_readable_and_policy_corrected_observations_stay_separate() {
        use openlegal_application::persistence::PersistentStore;
        let fixture = crate::test_support::TestDatabase::new().await;
        let base = fixture.open(100).await;
        let blobs =
            crate::blob::FsBlobStore::open(&fixture.directory.path().join("legacy-source-policy"))
                .await
                .unwrap();
        let store = PgCorpusStore::new(base.pool(), blobs);
        let input = SourceObservationInput {
            source_key: "law_go_kr:lsEfYdInfoGuide:fixture".into(),
            raw: Some(b"unchanged original bytes".to_vec()),
            media_type: "application/xml".into(),
            rights: SourceRights::legal_information(),
            metadata: BTreeMap::new(),
            observed_at: 100,
        };
        let retained = store
            .retain_source_observation(input.clone(), CancellationToken::new())
            .await
            .unwrap();
        let mut legacy = retained.clone();
        legacy.observation_id = legacy.content_id_with_policy(false).unwrap();
        sqlx::query(
            "UPDATE openlegal.corpus_source_observation SET id=$2,manifest_sha256=$3 WHERE id=$1",
        )
        .bind(&retained.observation_id)
        .bind(&legacy.observation_id)
        .bind(bytes_hash(&legacy.manifest().unwrap()))
        .execute(&base.pool())
        .await
        .unwrap();
        assert_eq!(
            store
                .source_observation(&legacy.observation_id, CancellationToken::new())
                .await
                .unwrap(),
            legacy
        );
        assert_eq!(
            store
                .source_observation_bytes(&legacy.observation_id, CancellationToken::new())
                .await
                .unwrap(),
            b"unchanged original bytes"
        );
        let mut corrected = input;
        corrected.observed_at = 200;
        corrected.rights.attribution = "Corrected issuer attribution".into();
        let corrected = store
            .retain_source_observation(corrected, CancellationToken::new())
            .await
            .unwrap();
        assert_ne!(legacy.observation_id, corrected.observation_id);
        assert_eq!(
            store
                .source_observation(&legacy.observation_id, CancellationToken::new())
                .await
                .unwrap()
                .rights,
            legacy.rights
        );
        assert_eq!(
            store
                .source_observation(&corrected.observation_id, CancellationToken::new())
                .await
                .unwrap()
                .rights,
            corrected.rights
        );
        assert_eq!(
            store
                .source_observation_bytes(&corrected.observation_id, CancellationToken::new())
                .await
                .unwrap(),
            b"unchanged original bytes"
        );
        base.close().await.unwrap();
    }
}
