use super::data::{hex, location, verify_query};
use super::*;
use openlegal_application::{
    StoredPayload,
    blob::BlobLocation,
    persistence::{
        StoredResult, canonical_identity, identity_digest, immutable_payload_digest,
        processed_digest, validate_payload,
    },
};
use openlegal_domain::history::SnapshotReference;
use sha2::{Digest, Sha256};
use uuid::Uuid;

impl PostgresStore {
    async fn reserve_blob(
        &self,
        digest: [u8; 32],
        size: usize,
        now: u64,
    ) -> Result<(BlobLocation, Uuid, bool), Error> {
        let mut tx = DbTransaction::begin(&self.inner.pool).await?;
        let row=sqlx::query("SELECT sha256,generation,size_bytes,storage_key,ready FROM openlegal.blob_object WHERE sha256=$1").bind(digest.as_slice()).fetch_optional(tx.conn()?).await.map_err(database_error)?;
        let row = if let Some(row) = row {
            row
        } else {
            let (count,bytes):(i64,i64)=sqlx::query_as("SELECT count(*)::bigint,COALESCE(sum(size_bytes),0)::bigint FROM openlegal.blob_object WHERE NOT ready").fetch_one(tx.conn()?).await.map_err(database_error)?;
            if count >= 128 || bytes.saturating_add(size as i64) > 128 * 1024 * 1024 {
                return Err(Error::StorageCapacity);
            };
            // The database generates every physical incarnation; digest remains identity.
            let generation: Uuid = sqlx::query_scalar("SELECT pg_catalog.uuidv7()")
                .fetch_one(tx.conn()?)
                .await
                .map_err(database_error)?;
            let digest_hex = hex(&digest);
            let key = format!("{}/{}-{}", &digest_hex[..2], digest_hex, generation);
            sqlx::query("INSERT INTO openlegal.blob_object(sha256,generation,size_bytes,storage_key,created_at) VALUES($1,$2,$3,$4,$5::text::numeric) RETURNING sha256,generation,size_bytes,storage_key,ready")
                .bind(digest.as_slice()).bind(generation).bind(size as i64).bind(key).bind(now.to_string()).fetch_one(tx.conn()?).await.map_err(database_error)?
        };
        let location = location(&row)?;
        if location.size_bytes != size as u64 {
            return Err(Error::StorageCorrupt);
        };
        let generation = row.try_get("generation").map_err(database_error)?;
        let ready = row.try_get("ready").map_err(database_error)?;
        tx.commit().await?;
        Ok((location, generation, ready))
    }
    pub(super) async fn publish_inner(
        &self,
        request: PublicationRequest,
    ) -> Result<PublicationOutcome, Error> {
        let PublicationRequest {
            key,
            value,
            expected,
            now,
            authorize,
            cancellation,
        } = request;
        validate_payload(&key, &value)?;
        if cancellation.is_cancelled() || !(authorize)() {
            return Err(Error::Cancelled);
        };
        let start_epoch = self.epoch();
        let previous = self
            .lookup_inner(key.clone(), now, cancellation.clone())
            .await?;
        if previous.observation != expected {
            return Ok(PublicationOutcome::Conflict);
        };
        let previous = previous.value.map(|stored| stored.payload);
        let unchanged = previous.as_ref().is_some_and(|old| {
            old.raw == value.raw
                && old.data == value.data
                && old.provenance.source_reference == value.provenance.source_reference
                && old.provenance.retrieved_at <= now
                && old.snapshot.as_ref().is_some_and(|s| {
                    s.captured_at <= now && self.inner.policy.retains(s.captured_at, now)
                })
        });
        let raw_digest: [u8; 32] = Sha256::digest(&value.raw).into();
        let (blob, generation, ready) = self.reserve_blob(raw_digest, value.raw.len(), now).await?;
        #[cfg(test)]
        self.checkpoint(TestPoint::AfterBlobReservation).await?;
        if ready {
            let bytes = self
                .inner
                .blobs
                .get(blob.clone(), cancellation.clone())
                .await?;
            let Some(bytes) = bytes else {
                // Retention may retire this physical generation while no SQL
                // transaction is held across the read. Only a still-live missing
                // object is corrupt evidence; a retired generation is contention.
                let live: bool = sqlx::query_scalar(
                    "SELECT EXISTS(SELECT 1 FROM openlegal.blob_object WHERE sha256=$1 AND generation=$2)",
                )
                .bind(raw_digest.as_slice())
                .bind(generation)
                .fetch_one(&self.inner.pool)
                .await
                .map_err(database_error)?;
                return Err(if live {
                    Error::StorageCorrupt
                } else {
                    Error::Busy
                });
            };
            if bytes != value.raw {
                return Err(Error::StorageCorrupt);
            };
        } else {
            self.inner
                .blobs
                .put_if_absent(blob.clone(), value.raw.clone(), cancellation.clone())
                .await?;
        }
        #[cfg(test)]
        self.checkpoint(TestPoint::AfterBlob).await?;
        if cancellation.is_cancelled() || !(authorize)() {
            return Err(Error::Cancelled);
        };
        if self.epoch() != start_epoch || !self.healthy() {
            return Err(Error::Busy);
        };
        let mut tx = DbTransaction::begin(&self.inner.pool).await?;
        let hash = identity_digest(&key.history)?;
        let row=sqlx::query("SELECT id,namespace,provider,dataset,query_hash,canonical_identity,query,mutation_revision FROM openlegal.cache_query WHERE query_hash=$1 FOR UPDATE")
            .bind(hash.as_slice()).fetch_optional(tx.conn()?).await.map_err(database_error)?;
        let query_id = if let Some(row) = row {
            let query = verify_query(&row, &key.history)?;
            if expected.query_id.as_deref() != Some(query.id.to_string().as_str())
                || expected.revision != query.revision
            {
                return Ok(PublicationOutcome::Conflict);
            };
            query.id
        } else {
            if expected.query_id.is_some() || expected.revision != 0 {
                return Ok(PublicationOutcome::Conflict);
            };
            sqlx::query_scalar("INSERT INTO openlegal.cache_query(namespace,provider,dataset,query_hash,canonical_identity,query,created_at) VALUES($1,$2,$3,$4,$5,$6,$7::text::numeric) RETURNING id")
                .bind(&key.history.namespace).bind(&key.history.provider).bind(&key.history.dataset).bind(hash.as_slice()).bind(canonical_identity(&key.history)?).bind(serde_json::to_value(&key.history.query).map_err(|_|Error::Internal)?).bind(now.to_string()).fetch_one(tx.conn()?).await.map_err(database_error)?
        };
        let blob_exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM openlegal.blob_object WHERE sha256=$1 AND generation=$2)",
        )
        .bind(raw_digest.as_slice())
        .bind(generation)
        .fetch_one(tx.conn()?)
        .await
        .map_err(database_error)?;
        if !blob_exists {
            return Err(Error::Busy);
        };
        if self.epoch() != start_epoch || !self.healthy() {
            return Err(Error::Busy);
        };
        let (snapshot_id, payload) = if unchanged {
            let old = previous.ok_or(Error::Internal)?;
            let public_id = &old
                .snapshot
                .as_ref()
                .ok_or(Error::StorageCorrupt)?
                .snapshot_id;
            let id: Uuid = sqlx::query_scalar(
                "SELECT id FROM openlegal.cache_snapshot WHERE query_id=$1 AND public_id=$2",
            )
            .bind(query_id)
            .bind(public_id)
            .fetch_one(tx.conn()?)
            .await
            .map_err(database_error)?;
            let mut provenance = old.provenance.clone();
            provenance.validated_at = value.provenance.validated_at;
            (
                id,
                Arc::new(StoredPayload {
                    data: old.data.clone(),
                    raw: old.raw.clone(),
                    bytes: old.bytes,
                    provenance,
                    snapshot: old.snapshot.clone(),
                }),
            )
        } else {
            let mut random = [0u8; 32];
            getrandom::fill(&mut random).map_err(|_| Error::StorageUnavailable)?;
            let public_id = hex(&random);
            let payload = Arc::new(StoredPayload {
                data: value.data.clone(),
                raw: value.raw.clone(),
                bytes: value.bytes,
                provenance: value.provenance.clone(),
                snapshot: Some(SnapshotReference {
                    snapshot_id: public_id.clone(),
                    captured_at: now,
                }),
            });
            let fingerprint = immutable_payload_digest(&key, &payload)?;
            let processed = processed_digest(&payload.data)?;
            let sequence:i64=sqlx::query_scalar("UPDATE openlegal.cache_query SET next_sequence=next_sequence+1 WHERE id=$1 AND next_sequence<9223372036854775807 RETURNING next_sequence-1")
                .bind(query_id).fetch_optional(tx.conn()?).await.map_err(database_error)?.ok_or(Error::StorageCapacity)?;
            sqlx::query(
                "UPDATE openlegal.blob_object SET ready=true WHERE sha256=$1 AND generation=$2",
            )
            .bind(raw_digest.as_slice())
            .bind(generation)
            .execute(tx.conn()?)
            .await
            .map_err(database_error)?;
            let id:Uuid=sqlx::query_scalar("INSERT INTO openlegal.cache_snapshot(public_id,query_id,sequence,captured_at,retrieved_at,original_validated_at,processor_version,schema_version,raw_blob_sha256,processed_sha256,processed_data,source_reference,envelope_sha256) VALUES($1,$2,$3,$4::text::numeric,$5::text::numeric,$6::text::numeric,$7,$8,$9,$10,$11,$12,$13) RETURNING id")
                .bind(&public_id).bind(query_id).bind(sequence).bind(now.to_string()).bind(payload.provenance.retrieved_at.to_string()).bind(payload.provenance.validated_at.to_string()).bind(&key.processor_version).bind(i64::from(key.schema_version)).bind(raw_digest.as_slice()).bind(processed.as_slice()).bind(serde_json::to_value(&payload.data).map_err(|_|Error::Internal)?).bind(&payload.provenance.source_reference).bind(fingerprint.as_slice()).fetch_one(tx.conn()?).await.map_err(database_error)?;
            (id, payload)
        };
        sqlx::query("INSERT INTO openlegal.cache_head(query_id,processor_version,schema_version,snapshot_id,validated_at) VALUES($1,$2,$3,$4,$5::text::numeric) ON CONFLICT(query_id,processor_version,schema_version) DO UPDATE SET snapshot_id=EXCLUDED.snapshot_id,validated_at=EXCLUDED.validated_at,etag=NULL,last_modified=NULL")
            .bind(query_id).bind(&key.processor_version).bind(i64::from(key.schema_version)).bind(snapshot_id).bind(value.provenance.validated_at.to_string()).execute(tx.conn()?).await.map_err(database_error)?;
        Self::advance_revision(&mut tx, query_id).await?;
        let mut barrier = None;
        let evictions = self
            .trim(&mut tx, Some(snapshot_id), now, &mut barrier)
            .await?;
        self.update_accounting(&mut tx, true).await?;
        #[cfg(test)]
        self.checkpoint(TestPoint::BeforeCommit).await?;
        if cancellation.is_cancelled() || !(authorize)() {
            return Err(Error::Cancelled);
        };
        if self.inner.closing.is_cancelled()
            || matches!(
                self.inner.state.load(Ordering::Acquire),
                RECOVERING | CORRUPT | CLOSED
            )
        {
            return Err(Error::StorageUnavailable);
        };
        tx.commit().await?;
        #[cfg(test)]
        self.checkpoint(TestPoint::AfterCommit).await?;
        drop(barrier);
        self.metric(|m| {
            if unchanged {
                m.unchanged_validations += 1;
            } else {
                m.writes += 1;
            }
            m.evictions += evictions;
        });
        Ok(PublicationOutcome::Accepted(StoredResult {
            payload,
            epoch: self.epoch(),
        }))
    }
    pub(super) async fn advance_revision(tx: &mut DbTransaction, id: Uuid) -> Result<(), Error> {
        let changed=sqlx::query("UPDATE openlegal.cache_query SET mutation_revision=mutation_revision+1 WHERE id=$1 AND mutation_revision<9223372036854775807").bind(id).execute(tx.conn()?).await.map_err(database_error)?.rows_affected();
        if changed != 1 {
            return Err(Error::StorageCapacity);
        };
        Ok(())
    }
}
