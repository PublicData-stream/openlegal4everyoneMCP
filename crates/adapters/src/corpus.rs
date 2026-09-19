//! PostgreSQL corpus and exact immutable evidence. Use a dedicated BlobStore root;
//! retrieval-cache garbage collection must never enumerate this corpus root.
use futures::future::BoxFuture;
use openlegal_application::{
    blob::{BlobLocation, BlobStore},
    database::*,
};
use openlegal_domain::legal::*;
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Row, postgres::PgRow};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

mod lifecycle;
mod runtime_lease;
pub use runtime_lease::CorpusRuntimeLease;

#[derive(Clone)]
pub struct PgCorpusStore {
    pool: PgPool,
    blobs: Arc<dyn BlobStore>,
    blocked: Arc<AtomicBool>,
    publication_clock: Arc<dyn openlegal_application::Clock>,
}
fn db(_: sqlx::Error) -> DatabaseError {
    DatabaseError::StorageUnavailable
}
fn blob_error(error: openlegal_domain::RetrievalError) -> DatabaseError {
    match error {
        openlegal_domain::RetrievalError::Cancelled => DatabaseError::Cancelled,
        openlegal_domain::RetrievalError::StorageCorrupt => DatabaseError::StorageCorrupt,
        _ => DatabaseError::StorageUnavailable,
    }
}
fn corrupt<T>(_: T) -> DatabaseError {
    DatabaseError::StorageCorrupt
}
fn bytes_hash(v: &[u8]) -> Vec<u8> {
    Sha256::digest(v).to_vec()
}
fn hex(v: &[u8]) -> String {
    v.iter().map(|b| format!("{b:02x}")).collect()
}
fn key(object: &ObjectId) -> Result<String, DatabaseError> {
    object.validate()?;
    Ok(hex(&bytes_hash(
        &serde_json::to_vec(object).map_err(corrupt)?,
    )))
}
fn unsigned(row: &PgRow, name: &str) -> Result<u64, DatabaseError> {
    row.try_get::<String, _>(name)
        .map_err(db)?
        .parse()
        .map_err(corrupt)
}
fn check(cancel: &CancellationToken) -> Result<(), DatabaseError> {
    if cancel.is_cancelled() {
        Err(DatabaseError::Cancelled)
    } else {
        Ok(())
    }
}
impl PgCorpusStore {
    pub fn new(pool: PgPool, blobs: Arc<dyn BlobStore>) -> Self {
        Self::with_publication_clock(
            pool,
            blobs,
            Arc::new(openlegal_application::SystemClock::default()),
        )
    }
    pub fn with_publication_clock(
        pool: PgPool,
        blobs: Arc<dyn BlobStore>,
        publication_clock: Arc<dyn openlegal_application::Clock>,
    ) -> Self {
        Self {
            publication_clock,
            pool,
            blobs,
            blocked: Arc::new(AtomicBool::new(false)),
        }
    }
    pub fn healthy(&self) -> bool {
        !self.blocked.load(Ordering::Acquire) && !self.pool.is_closed()
    }
    pub async fn health(&self) -> Result<(), DatabaseError> {
        if !self.healthy() {
            return Err(DatabaseError::StorageCorrupt);
        }
        sqlx::query("SELECT singleton FROM openlegal.corpus_control")
            .fetch_one(&self.pool)
            .await
            .map_err(db)?;
        self.blobs
            .health(CancellationToken::new())
            .await
            .map_err(blob_error)?;
        Ok(())
    }
    async fn gate(&self) -> Result<(), DatabaseError> {
        if !self.healthy() {
            return Err(DatabaseError::StorageCorrupt);
        }
        Ok(())
    }
    pub async fn state(&self, object: &ObjectId) -> Result<ObjectState, DatabaseError> {
        self.gate().await?;
        let k = key(object)?;
        let row=sqlx::query("SELECT identity,version,catalog_version,head_capture,pending,withdrawn,inventory_complete FROM openlegal.corpus_object WHERE object_key=$1").bind(k).fetch_optional(&self.pool).await.map_err(db)?;
        let Some(row) = row else {
            return Ok(ObjectState {
                catalog_version: 0,
                version: 0,
                head_capture: None,
                pending: false,
                withdrawn: false,
                inventory_complete: false,
            });
        };
        let identity: ObjectId =
            serde_json::from_value(row.try_get("identity").map_err(db)?).map_err(corrupt)?;
        if &identity != object {
            return Err(DatabaseError::StorageCorrupt);
        }
        Ok(ObjectState {
            catalog_version: row
                .try_get::<i64, _>("catalog_version")
                .map_err(db)?
                .try_into()
                .map_err(corrupt)?,
            version: row
                .try_get::<i64, _>("version")
                .map_err(db)?
                .try_into()
                .map_err(corrupt)?,
            head_capture: row.try_get("head_capture").map_err(db)?,
            pending: row.try_get("pending").map_err(db)?,
            withdrawn: row.try_get("withdrawn").map_err(db)?,
            inventory_complete: row.try_get("inventory_complete").map_err(db)?,
        })
    }
    async fn missing_evidence(&self, id: &str) -> Result<DatabaseError, DatabaseError> {
        let retired:bool=sqlx::query_scalar("SELECT NOT EXISTS(SELECT 1 FROM openlegal.corpus_capture WHERE id=$1) AND EXISTS(SELECT 1 FROM openlegal.corpus_outbox WHERE capture_id=$1 AND removed)").bind(id).fetch_one(&self.pool).await.map_err(db)?;
        Ok(if retired {
            DatabaseError::RevisionUnavailable
        } else {
            DatabaseError::StorageCorrupt
        })
    }
    async fn capture(
        &self,
        id: &str,
        now: u64,
        cancel: CancellationToken,
    ) -> Result<Capture, DatabaseError> {
        let result = self.capture_inner(id, now, false, cancel).await;
        if matches!(result, Err(DatabaseError::StorageCorrupt)) {
            self.blocked.store(true, Ordering::Release);
        }
        result
    }
    async fn capture_inner(
        &self,
        id: &str,
        now: u64,
        index_read: bool,
        cancel: CancellationToken,
    ) -> Result<Capture, DatabaseError> {
        self.gate().await?;
        check(&cancel)?;
        let row=sqlx::query("SELECT c.*,c.captured_at::text AS time_text,o.withdrawn FROM openlegal.corpus_capture c JOIN openlegal.corpus_object o USING(object_key) WHERE c.id=$1 AND ($3 OR o.head_capture=c.id OR c.captured_at>$2::text::numeric-2592000 OR EXISTS(SELECT 1 FROM openlegal.corpus_session s WHERE s.generation>=c.event_sequence AND s.expires_at>$2::text::numeric AND NOT s.invalidated))").bind(id).bind(now.to_string()).bind(index_read).fetch_optional(&self.pool).await.map_err(db)?.ok_or(DatabaseError::RevisionUnavailable)?;
        if !index_read && row.try_get::<bool, _>("withdrawn").map_err(db)? {
            return Err(DatabaseError::Withdrawn);
        }
        let value: serde_json::Value = row.try_get("payload").map_err(db)?;
        let encoded = serde_json::to_vec(&value).map_err(corrupt)?;
        let expected: Vec<u8> = row.try_get("payload_sha256").map_err(db)?;
        if bytes_hash(&encoded) != expected {
            self.blocked.store(true, Ordering::Release);
            return Err(DatabaseError::StorageCorrupt);
        }
        let capture: Capture = serde_json::from_value(value).map_err(corrupt)?;
        capture.record.validate().map_err(corrupt)?;
        let digest: Vec<u8> = row.try_get("raw_sha256").map_err(db)?;
        let raw_size: i64 = row.try_get("raw_size").map_err(db)?;
        let location = BlobLocation {
            digest: digest.clone().try_into().map_err(corrupt)?,
            size_bytes: raw_size.try_into().map_err(corrupt)?,
            storage_key: row.try_get("storage_key").map_err(db)?,
        };
        let raw = self
            .blobs
            .get(location, cancel.clone())
            .await
            .map_err(blob_error)?;
        let raw = match raw {
            Some(raw) => raw,
            None => return Err(self.missing_evidence(id).await?),
        };
        if bytes_hash(&raw) != digest
            || raw.len() as i64 != raw_size
            || capture.capture_id != id
            || capture.raw_sha256 != hex(&digest)
            || key(&capture.record.object)? != row.try_get::<String, _>("object_key").map_err(db)?
        {
            self.blocked.store(true, Ordering::Release);
            return Err(DatabaseError::StorageCorrupt);
        }
        let attachments = sqlx::query(
            "SELECT * FROM openlegal.corpus_capture_blob WHERE capture_id=$1 ORDER BY ordinal",
        )
        .bind(id)
        .fetch_all(&self.pool)
        .await
        .map_err(db)?;
        let mut evidence = std::collections::BTreeSet::from([capture.raw_sha256.clone()]);
        for a in attachments {
            let d: Vec<u8> = a.try_get("raw_sha256").map_err(db)?;
            let n: i64 = a.try_get("raw_size").map_err(db)?;
            let data = self
                .blobs
                .get(
                    BlobLocation {
                        digest: d.clone().try_into().map_err(corrupt)?,
                        size_bytes: n.try_into().map_err(corrupt)?,
                        storage_key: a.try_get("storage_key").map_err(db)?,
                    },
                    cancel.clone(),
                )
                .await
                .map_err(blob_error)?;
            let data = match data {
                Some(data) => data,
                None => return Err(self.missing_evidence(id).await?),
            };
            if bytes_hash(&data) != d || data.len() as i64 != n {
                return Err(DatabaseError::StorageCorrupt);
            }
            evidence.insert(hex(&d));
        }
        if capture
            .record
            .sections
            .iter()
            .filter_map(|s| s.source_document_sha256.as_ref())
            .any(|sha| !evidence.contains(sha))
        {
            return Err(self.missing_evidence(id).await?);
        }
        check(&cancel)?;
        let withdrawn:bool=sqlx::query_scalar("SELECT o.withdrawn FROM openlegal.corpus_capture c JOIN openlegal.corpus_object o USING(object_key) WHERE c.id=$1").bind(id).fetch_optional(&self.pool).await.map_err(db)?.ok_or(DatabaseError::RevisionUnavailable)?;
        if withdrawn && !index_read {
            return Err(DatabaseError::Withdrawn);
        }
        Ok(capture)
    }
    async fn resolve_inner(
        &self,
        object: ObjectId,
        selector: RevisionSelector,
        now: u64,
        metadata_only: bool,
        cancel: CancellationToken,
    ) -> Result<Capture, DatabaseError> {
        object.validate()?;
        selector.validate()?;
        let state = self.state(&object).await?;
        if state.withdrawn {
            return Err(DatabaseError::Withdrawn);
        }
        let k = key(&object)?;
        let head = matches!(selector, RevisionSelector::Head);
        let expected_date = match &selector {
            RevisionSelector::PublicationDate { date } => Some((false, date.clone())),
            RevisionSelector::EffectiveDate { date } => Some((true, date.clone())),
            _ => None,
        };
        let date_selector = matches!(
            selector,
            RevisionSelector::PublicationDate { .. } | RevisionSelector::EffectiveDate { .. }
        );
        if object.dataset == Dataset::Precedent
            && matches!(selector, RevisionSelector::Revision { .. })
        {
            return Err(DatabaseError::UnsupportedHistory);
        }
        let id = match selector {
            RevisionSelector::Head => {
                if state.pending {
                    return Err(DatabaseError::ProcessingPending);
                }
                state.head_capture.ok_or(DatabaseError::ProcessingPending)?
            }
            RevisionSelector::Capture { id } => id,
            RevisionSelector::Revision { id } => {
                if metadata_only {
                    sqlx::query_scalar::<_,String>("SELECT id FROM openlegal.corpus_capture_catalog WHERE object_key=$1 AND revision_id=$2 ORDER BY sequence DESC LIMIT 1").bind(&k).bind(id).fetch_optional(&self.pool).await.map_err(db)?.ok_or(DatabaseError::RevisionUnavailable)?
                } else {
                    sqlx::query_scalar::<_,Option<String>>("SELECT latest_capture FROM openlegal.corpus_revision WHERE object_key=$1 AND revision_id=$2").bind(&k).bind(id).fetch_optional(&self.pool).await.map_err(db)?.flatten().ok_or(DatabaseError::RevisionUnavailable)?
                }
            }
            date @ (RevisionSelector::PublicationDate { .. }
            | RevisionSelector::EffectiveDate { .. }) => {
                if object.dataset == Dataset::Precedent {
                    return Err(DatabaseError::UnsupportedHistory);
                }
                if !state.inventory_complete {
                    return Err(DatabaseError::HistoryIncomplete);
                }
                let (query, date) = match date {
                    RevisionSelector::PublicationDate { date } => (
                        "SELECT CASE WHEN $3 THEN (SELECT id FROM openlegal.corpus_capture_catalog c WHERE c.object_key=r.object_key AND c.revision_id=r.revision_id ORDER BY sequence DESC LIMIT 1) ELSE latest_capture END FROM openlegal.corpus_revision r WHERE object_key=$1 AND publication_date=$2 LIMIT 2",
                        date,
                    ),
                    RevisionSelector::EffectiveDate { date } => (
                        "SELECT CASE WHEN $3 THEN (SELECT id FROM openlegal.corpus_capture_catalog c WHERE c.object_key=r.object_key AND c.revision_id=r.revision_id ORDER BY sequence DESC LIMIT 1) ELSE latest_capture END FROM openlegal.corpus_revision r WHERE object_key=$1 AND effective_date=$2 LIMIT 2",
                        date,
                    ),
                    _ => return Err(DatabaseError::InvalidInput),
                };
                let rows = sqlx::query_scalar::<_, Option<String>>(query)
                    .bind(&k)
                    .bind(date)
                    .bind(metadata_only)
                    .fetch_all(&self.pool)
                    .await
                    .map_err(db)?;
                if rows.len() > 1 {
                    return Err(DatabaseError::AmbiguousRevision);
                }
                rows.into_iter()
                    .next()
                    .flatten()
                    .ok_or(DatabaseError::RevisionUnavailable)?
            }
        };
        let mut result = if metadata_only {
            let row=sqlx::query("SELECT payload,payload_sha256 FROM openlegal.corpus_capture_catalog WHERE id=$1 AND object_key=$2").bind(&id).bind(&k).fetch_optional(&self.pool).await.map_err(db)?.ok_or(DatabaseError::RevisionUnavailable)?;
            let value: serde_json::Value = row.try_get("payload").map_err(db)?;
            if bytes_hash(&serde_json::to_vec(&value).map_err(corrupt)?)
                != row.try_get::<Vec<u8>, _>("payload_sha256").map_err(db)?
            {
                self.blocked.store(true, Ordering::Release);
                return Err(DatabaseError::StorageCorrupt);
            }
            let capture: Capture = serde_json::from_value(value).map_err(corrupt)?;
            capture.record.validate().map_err(corrupt)?;
            if !capture.record.body.is_empty()
                || !capture.record.sections.is_empty()
                || capture.capture_id != id
            {
                return Err(DatabaseError::StorageCorrupt);
            }
            check(&cancel)?;
            if self.state(&object).await?.withdrawn {
                return Err(DatabaseError::Withdrawn);
            }
            capture
        } else {
            self.capture(&id, now, cancel).await?
        };
        if let Some((effective, date)) = expected_date
            && (if effective {
                result.record.effective_date.as_ref()
            } else {
                result.record.publication_date.as_ref()
            }) != Some(&date)
        {
            return Err(DatabaseError::RevisionUnavailable);
        }
        if result.record.object != object {
            return Err(DatabaseError::RevisionUnavailable);
        }
        if date_selector {
            let after = self.state(&object).await?;
            if after.version != state.version || after.catalog_version != state.catalog_version {
                return Err(DatabaseError::Conflict);
            }
        }
        if head {
            let row=sqlx::query("SELECT head_capture,validated_at::text,pending,withdrawn FROM openlegal.corpus_object WHERE object_key=$1").bind(k).fetch_one(&self.pool).await.map_err(db)?;
            if row.try_get::<bool, _>("withdrawn").map_err(db)? {
                return Err(DatabaseError::Withdrawn);
            }
            if row.try_get::<bool, _>("pending").map_err(db)?
                || row
                    .try_get::<Option<String>, _>("head_capture")
                    .map_err(db)?
                    .as_deref()
                    != Some(&id)
            {
                return Err(DatabaseError::Conflict);
            }
            result.validated_at = unsigned(&row, "validated_at")?;
        }
        Ok(result)
    }
    pub async fn publish(
        &self,
        mut p: Publication,
        cancel: CancellationToken,
    ) -> Result<Capture, DatabaseError> {
        self.gate().await?;
        check(&cancel)?;
        p.record.validate()?;
        let total_size = p
            .additional_evidence
            .iter()
            .try_fold(p.raw.len(), |n, b| n.checked_add(b.len()))
            .ok_or(DatabaseError::Capacity)?;
        if p.additional_evidence.len() > 64
            || total_size > MAX_SOURCE_BYTES
            || p.processor_version.is_empty()
            || p.processor_version.len() > 128
            || p.retrieved_at > p.now
        {
            return Err(DatabaseError::InvalidInput);
        }
        let evidence: std::collections::BTreeSet<_> = std::iter::once(&p.raw)
            .chain(p.additional_evidence.iter())
            .map(|b| hex(&bytes_hash(b)))
            .collect();
        if p.record
            .sections
            .iter()
            .filter_map(|s| s.source_document_sha256.as_ref())
            .any(|sha| !evidence.contains(sha))
        {
            return Err(DatabaseError::InvalidInput);
        }
        let previous = if p.install_head {
            match self.state(&p.record.object).await?.head_capture {
                Some(id) => Some(self.capture(&id, p.now, cancel.clone()).await?),
                None => None,
            }
        } else {
            None
        };
        let k = key(&p.record.object)?;
        let digest = bytes_hash(&p.raw);
        let size = p.raw.len() as i64;
        let generation: Uuid = sqlx::query_scalar("SELECT pg_catalog.uuidv7()")
            .fetch_one(&self.pool)
            .await
            .map_err(db)?;
        let digest_hex = hex(&digest);
        let storage_key = format!("{}/{}-{generation}", &digest_hex[..2], digest_hex);
        let mut inputs = vec![std::mem::take(&mut p.raw)];
        inputs.append(&mut p.additional_evidence);
        let mut staged_blobs = Vec::new();
        for (ordinal, raw) in inputs.iter().enumerate() {
            let d = bytes_hash(raw);
            let h = hex(&d);
            let location = if ordinal == 0 {
                storage_key.clone()
            } else {
                format!(
                    "{}/{}-{}",
                    &h[..2],
                    h,
                    sqlx::query_scalar::<_, Uuid>("SELECT pg_catalog.uuidv7()")
                        .fetch_one(&self.pool)
                        .await
                        .map_err(db)?
                )
            };
            staged_blobs.push((location, d, raw.len() as i64));
        }
        let mut reserve = self.pool.begin().await.map_err(db)?;
        let counts=sqlx::query("SELECT raw_bytes,staged_bytes,(SELECT count(*) FROM openlegal.corpus_staging) AS stages FROM openlegal.corpus_control WHERE singleton FOR UPDATE").fetch_one(&mut *reserve).await.map_err(db)?;
        if counts.try_get::<i64, _>("raw_bytes").map_err(db)?
            + counts.try_get::<i64, _>("staged_bytes").map_err(db)?
            + total_size as i64
            > 1024_i64 * 1024 * 1024 * 1024
            || counts.try_get::<i64, _>("staged_bytes").map_err(db)? + total_size as i64
                > 16_i64 * 1024 * 1024 * 1024
            || counts.try_get::<i64, _>("stages").map_err(db)? + staged_blobs.len() as i64
                > 128 * 65
        {
            return Err(DatabaseError::Capacity);
        }
        for (location, d, n) in &staged_blobs {
            sqlx::query("INSERT INTO openlegal.corpus_staging VALUES($1,$2,$3,$4::text::numeric)")
                .bind(location)
                .bind(d)
                .bind(n)
                .bind(p.now.to_string())
                .execute(&mut *reserve)
                .await
                .map_err(db)?;
        }
        sqlx::query("UPDATE openlegal.corpus_control SET staged_bytes=staged_bytes+$1")
            .bind(total_size as i64)
            .execute(&mut *reserve)
            .await
            .map_err(db)?;
        reserve.commit().await.map_err(db)?;
        for ((location, d, n), raw) in staged_blobs.iter().zip(inputs) {
            self.blobs
                .put_if_absent(
                    BlobLocation {
                        digest: d.clone().try_into().map_err(corrupt)?,
                        size_bytes: *n as u64,
                        storage_key: location.clone(),
                    },
                    raw,
                    cancel.clone(),
                )
                .await
                .map_err(blob_error)?;
        }
        check(&cancel)?;
        let mut tx = self.pool.begin().await.map_err(db)?;
        // This shared short lock makes event sequence order commit order, avoiding
        // gaps being mistaken for a complete index watermark.
        let event: i64 = sqlx::query_scalar(
            "SELECT next_event FROM openlegal.corpus_control WHERE singleton FOR UPDATE",
        )
        .fetch_one(&mut *tx)
        .await
        .map_err(db)?;
        sqlx::query("INSERT INTO openlegal.corpus_object(object_key,identity) VALUES($1,$2) ON CONFLICT DO NOTHING").bind(&k).bind(serde_json::to_value(&p.record.object).map_err(corrupt)?).execute(&mut *tx).await.map_err(db)?;
        let object=sqlx::query("SELECT identity,version,next_capture,withdrawn,head_capture,validated_at::text FROM openlegal.corpus_object WHERE object_key=$1 FOR UPDATE").bind(&k).fetch_one(&mut *tx).await.map_err(db)?;
        if serde_json::from_value::<ObjectId>(object.try_get("identity").map_err(db)?)
            .map_err(corrupt)?
            != p.record.object
        {
            return Err(DatabaseError::StorageCorrupt);
        }
        if object.try_get::<bool, _>("withdrawn").map_err(db)? {
            return Err(DatabaseError::Withdrawn);
        }
        // Stamp the publication transaction after durable blob staging and lock
        // admission. Returned captures are visible only after this transaction commits.
        p.now = p.now.max(self.publication_clock.now());
        let version: i64 = object.try_get("version").map_err(db)?;
        if p.install_head
            && object
                .try_get::<Option<String>, _>("validated_at")
                .map_err(db)?
                .map(|v| v.parse::<u64>())
                .transpose()
                .map_err(corrupt)?
                .is_some_and(|old| p.now < old)
        {
            return Err(DatabaseError::InvalidInput);
        }

        if let Some(job) = &p.job_id {
            let authorized:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM openlegal.corpus_job WHERE id=$1 AND object_key=$2 AND revision_id=$3 AND expected_version=$4 AND status='running' AND install_head=$5 AND lease_until>$6::text::numeric)").bind(Uuid::parse_str(job).map_err(|_|DatabaseError::InvalidInput)?).bind(&k).bind(&p.record.revision_id).bind(version).bind(p.install_head).bind(p.now.to_string()).fetch_one(&mut *tx).await.map_err(db)?;
            if !authorized {
                return Err(DatabaseError::Conflict);
            }
        }

        if u64::try_from(version).map_err(corrupt)? != p.expected_version {
            return Err(DatabaseError::Conflict);
        }
        for (location, _, _) in &staged_blobs {
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
        let previous_evidence_matches = if let Some(old) = &previous {
            let digests:Vec<Vec<u8>>=sqlx::query_scalar("SELECT raw_sha256 FROM openlegal.corpus_capture_blob WHERE capture_id=$1 ORDER BY ordinal").bind(&old.capture_id).fetch_all(&mut *tx).await.map_err(db)?;
            digests
                .iter()
                .eq(staged_blobs.iter().skip(1).map(|(_, d, _)| d))
        } else {
            false
        };
        if let Some(mut old) = previous
            && previous_evidence_matches
            && old.record == p.record
            && old.processor_version == p.processor_version
            && old.raw_sha256 == hex(&digest)
            && object
                .try_get::<Option<String>, _>("head_capture")
                .map_err(db)?
                .as_deref()
                == Some(old.capture_id.as_str())
        {
            sqlx::query("UPDATE openlegal.corpus_object SET validated_at=$2::text::numeric,pending=false,version=version+1 WHERE object_key=$1").bind(&k).bind(p.now.to_string()).execute(&mut *tx).await.map_err(db)?;
            for (location, d, n) in &staged_blobs {
                sqlx::query("INSERT INTO openlegal.corpus_blob_deletion VALUES($1,$2,$3)")
                    .bind(location)
                    .bind(d)
                    .bind(n)
                    .execute(&mut *tx)
                    .await
                    .map_err(db)?;
                sqlx::query("DELETE FROM openlegal.corpus_staging WHERE storage_key=$1")
                    .bind(location)
                    .execute(&mut *tx)
                    .await
                    .map_err(db)?;
            }
            sqlx::query("UPDATE openlegal.corpus_control SET staged_bytes=staged_bytes-$1")
                .bind(total_size as i64)
                .execute(&mut *tx)
                .await
                .map_err(db)?;
            if let Some(job) = p.job_id {
                sqlx::query("UPDATE openlegal.corpus_job SET status='done',lease_until=NULL WHERE id=$1 AND object_key=$2 AND expected_version=$3").bind(Uuid::parse_str(&job).map_err(|_|DatabaseError::InvalidInput)?).bind(&k).bind(version).execute(&mut *tx).await.map_err(db)?;
            }
            check(&cancel)?;
            tx.commit().await.map_err(db)?;
            old.validated_at = p.now;
            return Ok(old);
        }
        let historical_add = if p.install_head {
            if let Some(old_id) = object
                .try_get::<Option<String>, _>("head_capture")
                .map_err(db)?
            {
                sqlx::query_scalar::<_,i64>("SELECT c.raw_size+COALESCE((SELECT sum(b.raw_size)::bigint FROM openlegal.corpus_capture_blob b WHERE b.capture_id=c.id),0) FROM openlegal.corpus_capture c WHERE c.id=$1").bind(old_id).fetch_one(&mut *tx).await.map_err(db)?
            } else {
                0
            }
        } else {
            total_size as i64
        };
        let history_size: i64 =
            sqlx::query_scalar("SELECT historical_bytes FROM openlegal.corpus_control")
                .fetch_one(&mut *tx)
                .await
                .map_err(db)?;
        if history_size + historical_add > 64_i64 * 1024 * 1024 * 1024 {
            return Err(DatabaseError::Capacity);
        }
        sqlx::query("UPDATE openlegal.corpus_control SET historical_bytes=historical_bytes+$1")
            .bind(historical_add)
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        let sequence: i64 = object.try_get("next_capture").map_err(db)?;
        let capture_id = hex(&bytes_hash(generation.as_bytes()));
        let capture = Capture {
            capture_id: capture_id.clone(),
            sequence: sequence.try_into().map_err(corrupt)?,
            record: p.record,
            retrieved_at: p.retrieved_at,
            captured_at: p.now,
            validated_at: p.now,
            processor_version: p.processor_version,
            raw_sha256: hex(&digest),
        };
        let value = serde_json::to_value(&capture).map_err(corrupt)?;
        let checksum = bytes_hash(&serde_json::to_vec(&value).map_err(corrupt)?);
        sqlx::query("INSERT INTO openlegal.corpus_capture(id,object_key,revision_id,sequence,captured_at,publication_date,effective_date,payload,payload_sha256,raw_sha256,raw_size,storage_key,event_sequence) VALUES($1,$2,$3,$4,$5::text::numeric,$6,$7,$8,$9,$10,$11,$12,$13)").bind(&capture_id).bind(&k).bind(&capture.record.revision_id).bind(sequence).bind(p.now.to_string()).bind(&capture.record.publication_date).bind(&capture.record.effective_date).bind(value).bind(checksum).bind(&digest).bind(size).bind(&storage_key).bind(event).execute(&mut *tx).await.map_err(db)?;
        let mut metadata_capture = capture.clone();
        metadata_capture.record.body.clear();
        metadata_capture.record.sections.clear();
        let metadata = serde_json::to_value(metadata_capture).map_err(corrupt)?;
        let metadata_checksum = bytes_hash(&serde_json::to_vec(&metadata).map_err(corrupt)?);
        sqlx::query("INSERT INTO openlegal.corpus_capture_catalog VALUES($1,$2,$3,$4,$5::text::numeric,$6,$7,$8,$9)").bind(&capture_id).bind(&k).bind(&capture.record.revision_id).bind(sequence).bind(p.now.to_string()).bind(&capture.record.publication_date).bind(&capture.record.effective_date).bind(metadata).bind(metadata_checksum).execute(&mut *tx).await.map_err(db)?;
        for (ordinal, (location, d, n)) in staged_blobs.iter().enumerate().skip(1) {
            sqlx::query("INSERT INTO openlegal.corpus_capture_blob VALUES($1,$2,$3,$4,$5)")
                .bind(&capture_id)
                .bind(ordinal as i32)
                .bind(d)
                .bind(n)
                .bind(location)
                .execute(&mut *tx)
                .await
                .map_err(db)?;
        }
        sqlx::query("INSERT INTO openlegal.corpus_revision VALUES($1,$2,$3,$4,$5,$6,$7::text::numeric) ON CONFLICT(object_key,revision_id) DO UPDATE SET latest_capture=EXCLUDED.latest_capture,publication_date=EXCLUDED.publication_date,effective_date=EXCLUDED.effective_date,last_sequence=EXCLUDED.last_sequence,captured_at=EXCLUDED.captured_at").bind(&k).bind(&capture.record.revision_id).bind(&capture_id).bind(&capture.record.publication_date).bind(&capture.record.effective_date).bind(sequence).bind(p.now.to_string()).execute(&mut *tx).await.map_err(db)?;
        sqlx::query("UPDATE openlegal.corpus_object SET version=version+1,next_capture=next_capture+1,head_capture=CASE WHEN $2 THEN $3 ELSE head_capture END,validated_at=CASE WHEN $2 THEN $4::text::numeric ELSE validated_at END,pending=CASE WHEN $2 THEN false ELSE pending END WHERE object_key=$1").bind(&k).bind(p.install_head).bind(&capture_id).bind(p.now.to_string()).execute(&mut *tx).await.map_err(db)?;
        sqlx::query("INSERT INTO openlegal.corpus_outbox SELECT next_event,$1,$2,$3,false,$4,false FROM openlegal.corpus_control").bind(&k).bind(version+1).bind(&capture_id).bind(p.install_head).execute(&mut *tx).await.map_err(db)?;
        sqlx::query("UPDATE openlegal.corpus_control SET next_event=next_event+1,raw_bytes=raw_bytes+$1,staged_bytes=staged_bytes-$1").bind(total_size as i64).execute(&mut *tx).await.map_err(db)?;
        for (location, _, _) in &staged_blobs {
            sqlx::query("DELETE FROM openlegal.corpus_staging WHERE storage_key=$1")
                .bind(location)
                .execute(&mut *tx)
                .await
                .map_err(db)?;
        }
        if let Some(job) = p.job_id {
            let job = Uuid::parse_str(&job).map_err(|_| DatabaseError::InvalidInput)?;
            sqlx::query("UPDATE openlegal.corpus_job SET status='done',lease_until=NULL WHERE id=$1 AND object_key=$2 AND expected_version=$3").bind(job).bind(&k).bind(version).execute(&mut *tx).await.map_err(db)?;
        }
        check(&cancel)?;
        tx.commit().await.map_err(db)?;
        Ok(capture)
    }
}

impl DatabaseStore for PgCorpusStore {
    fn resolve(
        &self,
        object: ObjectId,
        selector: RevisionSelector,
        now: u64,
        cancel: CancellationToken,
    ) -> BoxFuture<'static, Result<Capture, DatabaseError>> {
        let this = self.clone();
        Box::pin(async move {
            this.resolve_inner(object, selector, now, false, cancel)
                .await
        })
    }
    fn resolve_metadata(
        &self,
        object: ObjectId,
        selector: RevisionSelector,
        now: u64,
        cancel: CancellationToken,
    ) -> BoxFuture<'static, Result<MetadataResult, DatabaseError>> {
        let this = self.clone();
        Box::pin(async move {
            this.resolve_inner(object, selector, now, true, cancel)
                .await
                .map(|capture| {
                    GetResult {
                        capture,
                        freshness: None,
                    }
                    .into()
                })
        })
    }
    fn history(
        &self,
        object: ObjectId,
        kind: HistoryKind,
        cursor: Option<String>,
        limit: usize,
        now: u64,
        cancel: CancellationToken,
    ) -> BoxFuture<'static, Result<HistoryPage, DatabaseError>> {
        let this = self.clone();
        Box::pin(async move {
            this.history_inner(object, kind, cursor, limit, now, cancel)
                .await
        })
    }
}
