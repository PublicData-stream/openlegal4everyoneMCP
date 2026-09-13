use super::*;
use openlegal_application::{
    StoredPayload,
    blob::BlobLocation,
    persistence::{
        ObservationToken, StoredResult, canonical_identity, identity_digest,
        immutable_payload_digest, processed_bytes, processed_digest, validate_payload,
    },
};
use openlegal_domain::{
    Provenance, Query, RetrievalData,
    history::{SnapshotReference, SnapshotSummary, valid_snapshot_id},
};
use sqlx::postgres::PgRow;
use uuid::Uuid;

pub(super) fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
pub(super) fn unsigned(row: &PgRow, name: &str) -> Result<u64, Error> {
    row.try_get::<String, _>(name)
        .map_err(database_error)?
        .parse()
        .map_err(|_| Error::StorageCorrupt)
}
pub(super) fn digest(row: &PgRow, name: &str) -> Result<[u8; 32], Error> {
    row.try_get::<Vec<u8>, _>(name)
        .map_err(database_error)?
        .try_into()
        .map_err(|_| Error::StorageCorrupt)
}
pub(super) fn location(row: &PgRow) -> Result<BlobLocation, Error> {
    let digest = digest(row, "sha256")?;
    let size_bytes = u64::try_from(
        row.try_get::<i64, _>("size_bytes")
            .map_err(database_error)?,
    )
    .map_err(|_| Error::StorageCorrupt)?;
    let storage_key: String = row.try_get("storage_key").map_err(database_error)?;
    let generation: Uuid = row.try_get("generation").map_err(database_error)?;
    let encoded = hex(&digest);
    if storage_key != format!("{}/{}-{}", &encoded[..2], encoded, generation)
        || size_bytes > openlegal_application::MAX_RAW_BYTES as u64
    {
        return Err(Error::StorageCorrupt);
    }
    Ok(BlobLocation {
        digest,
        size_bytes,
        storage_key,
    })
}
pub(super) struct QueryRow {
    pub id: Uuid,
    pub revision: u64,
}
pub(super) fn verify_query(row: &PgRow, expected: &HistoryKey) -> Result<QueryRow, Error> {
    let actual = HistoryKey {
        namespace: row.try_get("namespace").map_err(database_error)?,
        provider: row.try_get("provider").map_err(database_error)?,
        dataset: row.try_get("dataset").map_err(database_error)?,
        query: serde_json::from_value(row.try_get("query").map_err(database_error)?)
            .map_err(|_| Error::StorageCorrupt)?,
    };
    if &actual != expected
        || row
            .try_get::<Vec<u8>, _>("canonical_identity")
            .map_err(database_error)?
            != canonical_identity(expected)?
        || digest(row, "query_hash")? != identity_digest(expected)?
    {
        return Err(Error::StorageCorrupt);
    }
    Ok(QueryRow {
        id: row.try_get("id").map_err(database_error)?,
        revision: u64::try_from(
            row.try_get::<i64, _>("mutation_revision")
                .map_err(database_error)?,
        )
        .map_err(|_| Error::StorageCorrupt)?,
    })
}
pub(super) struct Capture {
    pub key: PersistentKey,
    pub summary: SnapshotSummary,
    pub payload: Arc<StoredPayload>,
}
impl PostgresStore {
    pub(super) async fn query_row(&self, key: &HistoryKey) -> Result<Option<QueryRow>, Error> {
        let digest = identity_digest(key)?;
        sqlx::query("SELECT id, namespace, provider, dataset, query_hash, canonical_identity, query, mutation_revision FROM openlegal.cache_query WHERE query_hash=$1")
            .bind(digest.as_slice()).fetch_optional(&self.inner.pool).await.map_err(database_error)?.map(|row|verify_query(&row,key)).transpose()
    }
    pub(super) async fn read_snapshot(
        &self,
        query_id: Option<Uuid>,
        public_id: Option<&str>,
        snapshot_id: Option<Uuid>,
        now: u64,
    ) -> Result<Option<Capture>, Error> {
        let row = sqlx::query("SELECT s.id, s.query_id, s.public_id, s.sequence, s.captured_at::text, s.retrieved_at::text, s.original_validated_at::text, s.processor_version, s.schema_version, s.processed_data, s.processed_sha256, s.source_reference, s.envelope_sha256, s.raw_blob_sha256, q.namespace, q.provider, q.dataset, q.query, q.query_hash, q.canonical_identity, b.sha256, b.generation, b.storage_key, b.size_bytes, b.ready FROM openlegal.cache_snapshot s LEFT JOIN openlegal.cache_query q ON q.id=s.query_id LEFT JOIN openlegal.blob_object b ON b.sha256=s.raw_blob_sha256 WHERE ($1::uuid IS NULL OR s.query_id=$1) AND ($2::text IS NULL OR s.public_id=$2) AND ($3::uuid IS NULL OR s.id=$3) LIMIT 1")
            .bind(query_id).bind(public_id).bind(snapshot_id).fetch_optional(&self.inner.pool).await.map_err(database_error)?;
        let Some(row) = row else { return Ok(None) };
        let captured_at = unsigned(&row, "captured_at")?;
        if !self.inner.policy.retains(captured_at, now) {
            return Ok(None);
        };
        let schema_version = u32::try_from(
            row.try_get::<i64, _>("schema_version")
                .map_err(database_error)?,
        )
        .map_err(|_| Error::StorageCorrupt)?;
        if schema_version != 1 {
            return Err(Error::SnapshotUnavailable);
        };
        let key = PersistentKey {
            history: HistoryKey {
                namespace: row.try_get("namespace").map_err(database_error)?,
                provider: row.try_get("provider").map_err(database_error)?,
                dataset: row.try_get("dataset").map_err(database_error)?,
                query: serde_json::from_value::<Query>(
                    row.try_get("query").map_err(database_error)?,
                )
                .map_err(|_| Error::StorageCorrupt)?,
            },
            processor_version: row.try_get("processor_version").map_err(database_error)?,
            schema_version,
        };
        if row
            .try_get::<Vec<u8>, _>("canonical_identity")
            .map_err(database_error)?
            != canonical_identity(&key.history).map_err(|_| Error::StorageCorrupt)?
            || digest(&row, "query_hash")?
                != identity_digest(&key.history).map_err(|_| Error::StorageCorrupt)?
            || !row.try_get::<bool, _>("ready").map_err(database_error)?
        {
            return Err(Error::StorageCorrupt);
        };
        let id: Uuid = row.try_get("id").map_err(database_error)?;
        let blob = location(&row)?;
        if blob.digest != digest(&row, "raw_blob_sha256")? {
            return Err(Error::StorageCorrupt);
        };
        let raw = self
            .inner
            .blobs
            .get(blob.clone(), self.inner.closing.child_token())
            .await?;
        let Some(raw) = raw else {
            let retained:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM openlegal.cache_snapshot WHERE id=$1 AND captured_at > $2::text::numeric - $3::bigint)")
                .bind(id).bind(now.to_string()).bind((self.inner.policy.retention_days*86400) as i64).fetch_one(&self.inner.pool).await.map_err(database_error)?;
            return if retained {
                Err(Error::StorageCorrupt)
            } else {
                Ok(None)
            };
        };
        let data: RetrievalData =
            serde_json::from_value(row.try_get("processed_data").map_err(database_error)?)
                .map_err(|_| Error::StorageCorrupt)?;
        if processed_digest(&data).map_err(|_| Error::StorageCorrupt)?
            != digest(&row, "processed_sha256")?
        {
            return Err(Error::StorageCorrupt);
        };
        let snapshot_id: String = row.try_get("public_id").map_err(database_error)?;
        let payload = StoredPayload {
            bytes: raw.len()
                + processed_bytes(&data)
                    .map_err(|_| Error::StorageCorrupt)?
                    .len()
                + row
                    .try_get::<String, _>("source_reference")
                    .map_err(database_error)?
                    .len()
                + 1024,
            raw,
            data,
            provenance: Provenance {
                provider: key.history.provider.clone(),
                dataset: key.history.dataset.clone(),
                processor_version: key.processor_version.clone(),
                source_reference: row.try_get("source_reference").map_err(database_error)?,
                payload_sha256: hex(&blob.digest),
                retrieved_at: unsigned(&row, "retrieved_at")?,
                validated_at: unsigned(&row, "original_validated_at")?,
            },
            snapshot: Some(SnapshotReference {
                snapshot_id: snapshot_id.clone(),
                captured_at,
            }),
        };
        validate_payload(&key, &payload)?;
        if immutable_payload_digest(&key, &payload)? != digest(&row, "envelope_sha256")? {
            return Err(Error::StorageCorrupt);
        };
        let summary = SnapshotSummary {
            snapshot_id,
            sequence: u64::try_from(row.try_get::<i64, _>("sequence").map_err(database_error)?)
                .map_err(|_| Error::StorageCorrupt)?,
            captured_at,
            processor_version: key.processor_version.clone(),
            schema_version,
            payload_sha256: hex(&blob.digest),
        };
        Ok(Some(Capture {
            key,
            summary,
            payload: Arc::new(payload),
        }))
    }
    pub(super) async fn lookup_inner(
        &self,
        key: PersistentKey,
        now: u64,
        cancellation: CancellationToken,
    ) -> Result<LookupResult, Error> {
        let epoch = self.epoch();
        let Some(query) = self.query_row(&key.history).await? else {
            self.metric(|m| m.misses += 1);
            return Ok(LookupResult {
                value: None,
                observation: ObservationToken::default(),
            });
        };
        let observation = ObservationToken {
            query_id: Some(query.id.to_string()),
            revision: query.revision,
        };
        let head=sqlx::query("SELECT snapshot_id,validated_at::text FROM openlegal.cache_head WHERE query_id=$1 AND processor_version=$2 AND schema_version=$3")
            .bind(query.id).bind(&key.processor_version).bind(i64::from(key.schema_version)).fetch_optional(&self.inner.pool).await.map_err(database_error)?;
        let Some(head) = head else {
            self.metric(|m| m.misses += 1);
            return Ok(LookupResult {
                value: None,
                observation,
            });
        };
        if cancellation.is_cancelled() {
            return Err(Error::Cancelled);
        };
        let id: Uuid = head.try_get("snapshot_id").map_err(database_error)?;
        let Some(capture) = self
            .read_snapshot(Some(query.id), None, Some(id), now)
            .await?
        else {
            let dangling:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM openlegal.cache_head WHERE snapshot_id=$1) AND NOT EXISTS(SELECT 1 FROM openlegal.cache_snapshot WHERE id=$1)").bind(id).fetch_one(&self.inner.pool).await.map_err(database_error)?;
            if dangling {
                return Err(Error::StorageCorrupt);
            }
            // Only a successful relational lookup (including logical retention) is a miss.
            self.metric(|m| m.misses += 1);
            return Ok(LookupResult {
                value: None,
                observation,
            });
        };
        if capture.key != key {
            return Err(Error::StorageCorrupt);
        };
        let value = &capture.payload;
        let mut provenance = value.provenance.clone();
        provenance.validated_at = unsigned(&head, "validated_at")?;
        let payload = Arc::new(StoredPayload {
            data: value.data.clone(),
            raw: value.raw.clone(),
            bytes: value.bytes,
            provenance,
            snapshot: value.snapshot.clone(),
        });
        self.metric(|m| m.hits += 1);
        Ok(LookupResult {
            value: Some(StoredResult { payload, epoch }),
            observation,
        })
    }
    pub(super) async fn get_inner(
        &self,
        key: HistoryKey,
        id: String,
        now: u64,
    ) -> Result<SnapshotEnvelope, Error> {
        if !valid_snapshot_id(&id) {
            return Err(Error::InvalidInput);
        };
        let query = self
            .query_row(&key)
            .await?
            .ok_or(Error::SnapshotUnavailable)?;
        let capture = self
            .read_snapshot(Some(query.id), Some(&id), None, now)
            .await?
            .ok_or(Error::SnapshotUnavailable)?;
        if capture.key.history != key {
            return Err(Error::StorageCorrupt);
        };
        let value = &capture.payload;
        Ok(SnapshotEnvelope {
            schema_version: 1,
            snapshot: capture.summary.clone(),
            query: key.query,
            data: value.data.clone(),
            provenance: value.provenance.clone(),
            historical: true,
            synthetic: true,
            clock_anomaly: capture.summary.captured_at > now
                || value.provenance.retrieved_at > now
                || value.provenance.validated_at > now,
        })
    }
    pub(super) async fn list_inner(
        &self,
        key: HistoryKey,
        cursor: Option<String>,
        limit: usize,
        now: u64,
    ) -> Result<SnapshotPage, Error> {
        if !(1..=20).contains(&limit) {
            return Err(Error::InvalidInput);
        };
        let query = self.query_row(&key).await?;
        let Some(query) = query else {
            if cursor.is_some() {
                return Err(Error::InvalidInput);
            };
            return Ok(SnapshotPage {
                snapshots: vec![],
                next_cursor: None,
                synthetic: true,
            });
        };
        let current_high: Option<i64> =
            sqlx::query_scalar("SELECT next_sequence-1 FROM openlegal.cache_query WHERE id=$1")
                .bind(query.id)
                .fetch_optional(&self.inner.pool)
                .await
                .map_err(database_error)?;
        let Some(current_high) = current_high else {
            if cursor.is_some() {
                return Err(Error::InvalidInput);
            };
            return Ok(SnapshotPage {
                snapshots: vec![],
                next_cursor: None,
                synthetic: true,
            });
        };
        let (high, before) = if let Some(cursor) = cursor {
            if cursor.len() > 256 {
                return Err(Error::InvalidInput);
            };
            let parts: Vec<_> = cursor.split(':').collect();
            if parts.len() != 4 || parts[0] != "v1" || parts[1] != query.id.to_string() {
                return Err(Error::InvalidInput);
            };
            let high = parts[2].parse::<i64>().map_err(|_| Error::InvalidInput)?;
            let before = parts[3].parse::<i64>().map_err(|_| Error::InvalidInput)?;
            if high < 1 || high > current_high || before < 1 || before > high {
                return Err(Error::InvalidInput);
            };
            (high, before)
        } else {
            let high = current_high;
            (high, high.checked_add(1).ok_or(Error::StorageCapacity)?)
        };
        let rows=sqlx::query("SELECT public_id,sequence,captured_at::text,processor_version,schema_version,raw_blob_sha256 FROM openlegal.cache_snapshot WHERE query_id=$1 AND schema_version=1 AND sequence<=$2 AND sequence<$3 AND captured_at>$4::text::numeric-$5::bigint ORDER BY sequence DESC LIMIT $6")
            .bind(query.id).bind(high).bind(before).bind(now.to_string()).bind((self.inner.policy.retention_days*86400) as i64).bind((limit+1) as i64).fetch_all(&self.inner.pool).await.map_err(database_error)?;
        let has_more = rows.len() > limit;
        let mut snapshots = Vec::new();
        for row in rows.into_iter().take(limit) {
            let snapshot_id: String = row.try_get("public_id").map_err(database_error)?;
            if !valid_snapshot_id(&snapshot_id) {
                return Err(Error::StorageCorrupt);
            };
            snapshots.push(SnapshotSummary {
                snapshot_id,
                sequence: u64::try_from(row.try_get::<i64, _>("sequence").map_err(database_error)?)
                    .map_err(|_| Error::StorageCorrupt)?,
                captured_at: unsigned(&row, "captured_at")?,
                processor_version: row.try_get("processor_version").map_err(database_error)?,
                schema_version: 1,
                payload_sha256: hex(&digest(&row, "raw_blob_sha256")?),
            });
        }
        let next_cursor = if has_more {
            snapshots
                .last()
                .map(|s| format!("v1:{}:{high}:{}", query.id, s.sequence))
        } else {
            None
        };
        Ok(SnapshotPage {
            snapshots,
            next_cursor,
            synthetic: true,
        })
    }
}
