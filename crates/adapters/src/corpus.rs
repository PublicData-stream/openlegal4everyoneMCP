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

pub(super) mod storage_retry;
use storage_retry::retry_storage;

#[cfg(test)]
mod contention_regressions;

mod citation;
mod clone_progress;
pub use clone_progress::CloneView;
mod supplement_jobs;
pub use supplement_jobs::{SupplementJob, SupplementJobStatus};
mod source_observations;
pub use source_observations::{SourceObservation, SourceObservationInput};
mod original;
pub use original::OriginalEvidence;
mod collection_gaps;
mod collection_requests;
pub use collection_requests::CollectionLaunch;
mod object_status;
pub use collection_gaps::PageGapObservation;
mod lifecycle;
mod runtime_lease;
pub use runtime_lease::CorpusRuntimeLease;

#[derive(Clone)]
pub struct PgCorpusStore {
    pool: PgPool,
    blobs: Arc<dyn BlobStore>,
    blocked: Arc<AtomicBool>,
    publication_clock: Arc<dyn openlegal_application::Clock>,
    collection_events: Arc<tokio::sync::OnceCell<crate::collection_events::CollectionEvents>>,
}
fn db(error: sqlx::Error) -> DatabaseError {
    // Only a server-reported rejection proves that the SQL statement failed.
    // Connection and pool timeouts can leave the commit outcome uncertain.
    if error.as_database_error().is_some_and(|error| {
        matches!(
            error.code().as_deref(),
            Some("55P03" | "57014" | "40P01" | "40001")
        )
    }) {
        if let Some(code) = error.as_database_error().and_then(|error| error.code()) {
            tracing::warn!(operation = "corpus_sql", sqlstate = %code, "corpus SQL rejected by server");
        }
        DatabaseError::StorageContended
    } else {
        DatabaseError::StorageUnavailable
    }
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
fn json_hash(value: &serde_json::Value) -> Result<Vec<u8>, DatabaseError> {
    struct DigestWriter(Sha256);
    impl std::io::Write for DigestWriter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.update(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut writer = DigestWriter(Sha256::new());
    serde_json::to_writer(&mut writer, value).map_err(corrupt)?;
    Ok(writer.0.finalize().to_vec())
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
    pub async fn collection_events(
        &self,
    ) -> Result<crate::collection_events::CollectionEvents, DatabaseError> {
        self.gate().await?;
        Ok(self
            .collection_events
            .get_or_try_init(|| crate::collection_events::CollectionEvents::open(&self.pool))
            .await?
            .clone())
    }

    pub async fn close_collection_events(&self) {
        if let Some(events) = self.collection_events.get() {
            events.close().await;
        }
    }

    /// Only work claimable by normal detail workers delays another scan. Explicit
    /// outbound readiness is represented by provider tickets, not parsing Jobs.
    pub async fn collection_backlog(&self) -> Result<bool, DatabaseError> {
        self.gate().await?;
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM openlegal.corpus_job j JOIN openlegal.corpus_object o USING(object_key) JOIN openlegal.provider_request_budget b ON b.singleton WHERE NOT o.withdrawn AND (NOT j.install_head OR j.revision_id=o.desired_head_revision) AND j.attempts<b.max_job_attempts AND (j.status='pending' OR (j.status='running' AND j.lease_until<=floor(extract(epoch from clock_timestamp()))::bigint)) AND j.explicit_request_id IS NULL AND COALESCE(j.source_metadata->>'collection_origin','')<>'explicit' AND (b.continuous_daily_limit IS NULL OR b.utc_day<>floor(extract(epoch from clock_timestamp()))::bigint/86400 OR b.daily_used<b.continuous_daily_limit) AND NOT EXISTS(SELECT 1 FROM openlegal.corpus_job active WHERE active.object_key=j.object_key AND active.id<>j.id AND active.status='running' AND active.lease_until>floor(extract(epoch from clock_timestamp()))::bigint))")
            .fetch_one(&self.pool).await.map_err(db)
    }
    /// Metadata-only checkpoint for an observed current-list revision. This
    /// never reads blob bodies and never asserts provider inventory completeness.
    pub async fn head_revision_ready(
        &self,
        object: &ObjectId,
        revision_id: &str,
        now: u64,
    ) -> Result<bool, DatabaseError> {
        self.gate().await?;
        let identity = key(object)?;
        let ready: Option<bool> = sqlx::query_scalar("SELECT NOT o.withdrawn AND NOT o.pending AND o.desired_head_revision=$2 AND c.revision_id=$2 AND COALESCE(c.payload->'record'->'metadata'->>'attachment_status','complete') <> 'incomplete' AND o.validated_at IS NOT NULL AND o.validated_at<=$3::text::numeric AND o.validated_at>$3::text::numeric-3600 FROM openlegal.corpus_object o JOIN openlegal.corpus_capture c ON c.id=o.head_capture WHERE o.object_key=$1")
            .bind(identity).bind(revision_id).bind(now.to_string())
            .fetch_optional(&self.pool).await.map_err(db)?;
        Ok(ready.unwrap_or(false))
    }
    /// Publication progress is independent of the one-hour freshness window.
    /// An older accepted capture still satisfies a list page while the cursor
    /// moves through other objects on that page.
    pub async fn head_revision_published(
        &self,
        object: &ObjectId,
        revision_id: &str,
    ) -> Result<bool, DatabaseError> {
        self.gate().await?;
        let identity = key(object)?;
        let published: Option<bool> = sqlx::query_scalar("SELECT NOT o.withdrawn AND NOT o.pending AND o.desired_head_revision=$2 AND c.revision_id=$2 AND COALESCE(c.payload->'record'->'metadata'->>'attachment_status','complete') <> 'incomplete' FROM openlegal.corpus_object o JOIN openlegal.corpus_capture c ON c.id=o.head_capture WHERE o.object_key=$1")
            .bind(identity).bind(revision_id).fetch_optional(&self.pool).await.map_err(db)?;
        Ok(published.unwrap_or(false))
    }
    /// Internal ingestion lookup for a recently validated retained capture. Public revision
    /// selectors remain unsupported for datasets without provider history.
    pub async fn revision_capture_recent(
        &self,
        object: &ObjectId,
        revision_id: &str,
        now: u64,
    ) -> Result<bool, DatabaseError> {
        self.gate().await?;
        let identity = key(object)?;
        let ready: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM openlegal.corpus_revision r JOIN openlegal.corpus_capture c ON c.id=r.latest_capture JOIN openlegal.corpus_object o ON o.object_key=r.object_key WHERE r.object_key=$1 AND r.revision_id=$2 AND COALESCE(c.payload->'record'->'metadata'->>'attachment_status','complete') <> 'incomplete' AND r.last_validated_at<=$3::text::numeric AND r.last_validated_at>$3::text::numeric-3600 AND NOT o.withdrawn)")
            .bind(identity).bind(revision_id).bind(now.to_string())
            .fetch_one(&self.pool).await.map_err(db)?;
        Ok(ready)
    }
    pub async fn revision_capture_published(
        &self,
        object: &ObjectId,
        revision_id: &str,
        _now: u64,
    ) -> Result<bool, DatabaseError> {
        self.gate().await?;
        let identity = key(object)?;
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM openlegal.corpus_revision r JOIN openlegal.corpus_capture c ON c.id=r.latest_capture JOIN openlegal.corpus_object o ON o.object_key=r.object_key WHERE r.object_key=$1 AND r.revision_id=$2 AND COALESCE(c.payload->'record'->'metadata'->>'attachment_status','complete') <> 'incomplete' AND NOT o.withdrawn)")
            .bind(identity).bind(revision_id)
            .fetch_one(&self.pool).await.map_err(db)
    }
    pub async fn inventory_cursor(
        &self,
        dataset: Dataset,
        historical: bool,
    ) -> Result<u32, DatabaseError> {
        self.gate().await?;
        let name = format!(
            "{}:{historical}",
            serde_json::to_string(&dataset).map_err(corrupt)?
        );
        sqlx::query("INSERT INTO openlegal.provider_inventory_cursor(dataset) VALUES($1) ON CONFLICT DO NOTHING")
            .bind(&name).execute(&self.pool).await.map_err(db)?;
        let page: i32 = sqlx::query_scalar(
            "SELECT next_page FROM openlegal.provider_inventory_cursor WHERE dataset=$1",
        )
        .bind(&name)
        .fetch_one(&self.pool)
        .await
        .map_err(db)?;
        u32::try_from(page).map_err(corrupt)
    }
    pub async fn advance_inventory_cursor(
        &self,
        dataset: Dataset,
        historical: bool,
        observed_page: u32,
        done: bool,
    ) -> Result<(), DatabaseError> {
        self.gate().await?;
        let name = format!(
            "{}:{historical}",
            serde_json::to_string(&dataset).map_err(corrupt)?
        );
        let next = if done {
            1
        } else {
            observed_page
                .checked_add(1)
                .ok_or(DatabaseError::Capacity)?
        };
        let rows = sqlx::query("UPDATE openlegal.provider_inventory_cursor SET next_page=$1,item_offset=0 WHERE dataset=$2 AND next_page=$3")
            .bind(i32::try_from(next).map_err(corrupt)?)
            .bind(&name)
            .bind(i32::try_from(observed_page).map_err(corrupt)?)
            .execute(&self.pool).await.map_err(db)?.rows_affected();
        if rows != 1 {
            return Err(DatabaseError::Conflict);
        }
        Ok(())
    }
    pub async fn inventory_item_offset(
        &self,
        dataset: Dataset,
        historical: bool,
        page: u32,
    ) -> Result<usize, DatabaseError> {
        self.gate().await?;
        let name = format!(
            "{}:{historical}",
            serde_json::to_string(&dataset).map_err(corrupt)?
        );
        let offset: Option<i32> = sqlx::query_scalar("SELECT item_offset FROM openlegal.provider_inventory_cursor WHERE dataset=$1 AND next_page=$2")
            .bind(name).bind(i32::try_from(page).map_err(corrupt)?)
            .fetch_optional(&self.pool).await.map_err(db)?;
        usize::try_from(offset.ok_or(DatabaseError::Conflict)?).map_err(corrupt)
    }
    pub async fn set_inventory_item_offset(
        &self,
        dataset: Dataset,
        historical: bool,
        page: u32,
        offset: usize,
    ) -> Result<(), DatabaseError> {
        self.gate().await?;
        let name = format!(
            "{}:{historical}",
            serde_json::to_string(&dataset).map_err(corrupt)?
        );
        let rows = sqlx::query("UPDATE openlegal.provider_inventory_cursor SET item_offset=$1 WHERE dataset=$2 AND next_page=$3")
            .bind(i32::try_from(offset).map_err(corrupt)?)
            .bind(name).bind(i32::try_from(page).map_err(corrupt)?)
            .execute(&self.pool).await.map_err(db)?.rows_affected();
        if rows != 1 {
            return Err(DatabaseError::Conflict);
        }
        Ok(())
    }
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
            collection_events: Arc::new(tokio::sync::OnceCell::new()),
            publication_clock,
            pool,
            blobs,
            blocked: Arc::new(AtomicBool::new(false)),
        }
    }
    /// Shared archive capacity. `None` admits storage without a configured total
    /// cap; per-publication and staging bounds still apply. Lowering a cap never
    /// removes existing evidence, and takes effect at the next reservation.
    pub async fn configure_archive_capacity(
        &self,
        max_raw_bytes: Option<u64>,
    ) -> Result<(), DatabaseError> {
        if max_raw_bytes == Some(0) {
            return Err(DatabaseError::InvalidInput);
        }
        self.gate().await?;
        sqlx::query(
            "UPDATE openlegal.corpus_control SET max_raw_bytes=$1::text::numeric WHERE singleton",
        )
        .bind(max_raw_bytes.map(|n| n.to_string()))
        .execute(&self.pool)
        .await
        .map_err(db)?;
        Ok(())
    }
    pub fn healthy(&self) -> bool {
        !self.blocked.load(Ordering::Acquire) && !self.pool.is_closed()
    }
    pub async fn health(&self) -> Result<(), DatabaseError> {
        if !self.healthy() {
            return Err(DatabaseError::StorageCorrupt);
        }
        retry_storage(&CancellationToken::new(), "corpus_health", || async {
            sqlx::query("SELECT singleton FROM openlegal.corpus_control")
                .fetch_one(&self.pool)
                .await
                .map_err(db)
        })
        .await?;
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
                observed: false,
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
            observed: true,
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
        _now: u64,
        index_read: bool,
        cancel: CancellationToken,
    ) -> Result<Capture, DatabaseError> {
        self.gate().await?;
        check(&cancel)?;
        let row=sqlx::query("SELECT c.*,c.captured_at::text AS time_text,o.withdrawn FROM openlegal.corpus_capture c JOIN openlegal.corpus_object o USING(object_key) WHERE c.id=$1").bind(id).fetch_optional(&self.pool).await.map_err(db)?.ok_or(DatabaseError::RevisionUnavailable)?;
        if !index_read && row.try_get::<bool, _>("withdrawn").map_err(db)? {
            return Err(DatabaseError::Withdrawn);
        }
        let value: serde_json::Value = row.try_get("payload").map_err(db)?;
        let expected: Vec<u8> = row.try_get("payload_sha256").map_err(db)?;
        if json_hash(&value)? != expected {
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
        let selected_revision = match &selector {
            RevisionSelector::Revision { id } => Some(id.clone()),
            _ => None,
        };
        let expected_date = match &selector {
            RevisionSelector::PublicationDate { date } => Some((false, date.clone())),
            RevisionSelector::EffectiveDate { date } => Some((true, date.clone())),
            _ => None,
        };
        let date_selector = matches!(
            selector,
            RevisionSelector::PublicationDate { .. } | RevisionSelector::EffectiveDate { .. }
        );
        if !object.dataset.has_provider_revisions()
            && matches!(selector, RevisionSelector::Revision { .. })
        {
            return Err(DatabaseError::UnsupportedHistory);
        }
        let id = match selector {
            RevisionSelector::Head => {
                if !state.observed {
                    return Err(DatabaseError::NotObserved);
                }
                if state.pending || state.head_capture.is_none() {
                    return Err(match self.object_status(&object, now).await?.state {
                        ObjectCollectionState::ProcessingPending => {
                            DatabaseError::ProcessingPending
                        }
                        _ => DatabaseError::CollectionIncomplete,
                    });
                }
                state
                    .head_capture
                    .ok_or(DatabaseError::CollectionIncomplete)?
            }
            RevisionSelector::Capture { id } => id,
            RevisionSelector::Revision { id } => {
                if metadata_only {
                    sqlx::query_scalar::<_,Option<String>>("SELECT COALESCE(r.latest_capture,(SELECT id FROM openlegal.corpus_capture_catalog c WHERE c.object_key=r.object_key AND c.revision_id=r.revision_id ORDER BY sequence DESC LIMIT 1)) FROM openlegal.corpus_revision r WHERE r.object_key=$1 AND r.revision_id=$2").bind(&k).bind(id).fetch_optional(&self.pool).await.map_err(db)?.flatten().ok_or(DatabaseError::RevisionUnavailable)?
                } else {
                    sqlx::query_scalar::<_,Option<String>>("SELECT latest_capture FROM openlegal.corpus_revision WHERE object_key=$1 AND revision_id=$2").bind(&k).bind(id).fetch_optional(&self.pool).await.map_err(db)?.flatten().ok_or(DatabaseError::RevisionUnavailable)?
                }
            }
            date @ (RevisionSelector::PublicationDate { .. }
            | RevisionSelector::EffectiveDate { .. }) => {
                if !object.dataset.has_provider_revisions() {
                    return Err(DatabaseError::UnsupportedHistory);
                }
                if !state.inventory_complete {
                    return Err(DatabaseError::HistoryIncomplete);
                }
                let (query, date) = match date {
                    RevisionSelector::PublicationDate { date } => (
                        "SELECT CASE WHEN $3 THEN COALESCE(r.latest_capture,(SELECT id FROM openlegal.corpus_capture_catalog c WHERE c.object_key=r.object_key AND c.revision_id=r.revision_id ORDER BY sequence DESC LIMIT 1)) ELSE latest_capture END FROM openlegal.corpus_revision r WHERE object_key=$1 AND publication_date=$2 LIMIT 2",
                        date,
                    ),
                    RevisionSelector::EffectiveDate { date } => (
                        "SELECT CASE WHEN $3 THEN COALESCE(r.latest_capture,(SELECT id FROM openlegal.corpus_capture_catalog c WHERE c.object_key=r.object_key AND c.revision_id=r.revision_id ORDER BY sequence DESC LIMIT 1)) ELSE latest_capture END FROM openlegal.corpus_revision r WHERE object_key=$1 AND effective_date=$2 LIMIT 2",
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
        if let Some(revision_id) = selected_revision {
            let validation: Option<Option<String>> = sqlx::query_scalar("SELECT last_validated_at::text FROM openlegal.corpus_revision WHERE object_key=$1 AND revision_id=$2 AND latest_capture=$3")
                .bind(&k).bind(&revision_id).bind(&id).fetch_optional(&self.pool).await.map_err(db)?;
            match validation.flatten() {
                Some(value) => result.validated_at = value.parse().map_err(corrupt)?,
                None if !metadata_only => return Err(DatabaseError::Conflict),
                None => {}
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
        let update_job_gap = p.job_id.is_some();
        let attachment_incomplete = p
            .record
            .metadata
            .get("attachment_status")
            .map(String::as_str)
            == Some("incomplete");
        if update_job_gap && attachment_incomplete {
            let object_key = key(&p.record.object)?;
            retry_storage(&cancel, "publication_gap_capacity", || async {
                self.ensure_gap_capacity(&collection_gaps::detail_key(
                    &object_key,
                    &p.record.revision_id,
                ))
                .await
            })
            .await?;
        }
        let k = key(&p.record.object)?;
        let previous_id = if p.install_head {
            retry_storage(&cancel, "publication_previous_head", || {
                self.state(&p.record.object)
            })
            .await?
            .head_capture
        } else {
            retry_storage(&cancel, "publication_previous_revision", || async {
                sqlx::query_scalar::<_, Option<String>>("SELECT latest_capture FROM openlegal.corpus_revision WHERE object_key=$1 AND revision_id=$2")
                    .bind(&k).bind(&p.record.revision_id).fetch_optional(&self.pool).await.map_err(db)
            }).await?.flatten()
        };
        let previous = match previous_id {
            Some(id) => match retry_storage(&cancel, "publication_previous_capture", || {
                self.capture(&id, p.now, cancel.clone())
            })
            .await
            {
                Ok(capture) => Some(capture),
                Err(DatabaseError::RevisionUnavailable) if !p.install_head => None,
                Err(error) => return Err(error),
            },
            None => None,
        };
        let digest = bytes_hash(&p.raw);
        let size = p.raw.len() as i64;
        let generation: Uuid = retry_storage(&cancel, "publication_generation", || async {
            sqlx::query_scalar("SELECT pg_catalog.uuidv7()")
                .fetch_one(&self.pool)
                .await
                .map_err(db)
        })
        .await?;
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
                    retry_storage(&cancel, "publication_evidence_generation", || async {
                        sqlx::query_scalar::<_, Uuid>("SELECT pg_catalog.uuidv7()")
                            .fetch_one(&self.pool)
                            .await
                            .map_err(db)
                    })
                    .await?
                )
            };
            staged_blobs.push((location, d, raw.len() as i64));
        }
        retry_storage(&cancel, "publication_reserve", || async {
        let mut reserve = self.pool.begin().await.map_err(db)?;
        let counts=sqlx::query("SELECT raw_bytes,staged_bytes,max_raw_bytes::text,(SELECT count(*) FROM openlegal.corpus_staging) AS stages FROM openlegal.corpus_control WHERE singleton FOR UPDATE").fetch_one(&mut *reserve).await.map_err(db)?;
        let staged_bytes = counts.try_get::<i64, _>("staged_bytes").map_err(db)?;
        let reserved_bytes = counts
            .try_get::<i64, _>("raw_bytes")
            .map_err(db)?
            .checked_add(staged_bytes)
            .and_then(|n| n.checked_add(total_size as i64))
            .ok_or(DatabaseError::Capacity)?;
        let max_raw_bytes = counts
            .try_get::<Option<String>, _>("max_raw_bytes")
            .map_err(db)?
            .map(|n| n.parse::<u64>())
            .transpose()
            .map_err(corrupt)?;
        if max_raw_bytes.is_some_and(|max| reserved_bytes as u64 > max)
            || staged_bytes + total_size as i64 > 16_i64 * 1024 * 1024 * 1024
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
        check(&cancel)?;
        reserve.commit().await.map_err(db)?;
        Ok(())
        }).await?;
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
        // Retry only the rejected metadata transaction. Prepared evidence and
        // physical generations survive each rollback; HTTP and blobs are not replayed.
        retry_storage(&cancel, "publication_commit", || async {
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
        let now = p.now.max(self.publication_clock.now());
        let version: i64 = object.try_get("version").map_err(db)?;
        if p.install_head
            && object
                .try_get::<Option<String>, _>("validated_at")
                .map_err(db)?
                .map(|v| v.parse::<u64>())
                .transpose()
                .map_err(corrupt)?
                .is_some_and(|old| now < old)
        {
            return Err(DatabaseError::InvalidInput);
        }

        if let Some(job) = &p.job_id {
            let authorized:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM openlegal.corpus_job WHERE id=$1 AND object_key=$2 AND revision_id=$3 AND expected_version=$4 AND status='running' AND error_category IS NULL AND install_head=$5 AND lease_until>$6::text::numeric)").bind(Uuid::parse_str(job).map_err(|_|DatabaseError::InvalidInput)?).bind(&k).bind(&p.record.revision_id).bind(version).bind(p.install_head).bind(now.to_string()).fetch_one(&mut *tx).await.map_err(db)?;
            if !authorized {
                return Err(DatabaseError::Conflict);
            }
        }

        if u64::try_from(version).map_err(corrupt)? != p.expected_version {
            return Err(DatabaseError::Conflict);
        }
        // Preserve the complete HEAD and its validation time while recording
        // this partial observation. The version fence above proves `previous`
        // still describes the current HEAD.
        let preserve_head = p.install_head
            && p.record
                .metadata
                .get("attachment_status")
                .map(String::as_str)
                == Some("incomplete")
            && previous.as_ref().is_some_and(|old| {
                old.record
                    .metadata
                    .get("attachment_status")
                    .map(String::as_str)
                    != Some("incomplete")
            });
        let publish_head = p.install_head && !preserve_head;
        let preserve_revision = preserve_head
            && previous
                .as_ref()
                .is_some_and(|old| old.record.revision_id == p.record.revision_id);
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
        let previous_still_current = if let Some(old) = &previous {
            if p.install_head {
                object
                    .try_get::<Option<String>, _>("head_capture")
                    .map_err(db)?
                    .as_deref()
                    == Some(old.capture_id.as_str())
            } else {
                let latest: Option<String> = sqlx::query_scalar("SELECT latest_capture FROM openlegal.corpus_revision WHERE object_key=$1 AND revision_id=$2")
                    .bind(&k).bind(&p.record.revision_id).fetch_optional(&mut *tx).await.map_err(db)?.flatten();
                latest.as_deref() == Some(old.capture_id.as_str())
            }
        } else {
            false
        };
        if let Some(old) = &previous
            && previous_evidence_matches
            && old.record == p.record
            && old.processor_version == p.processor_version
            && old.raw_sha256 == hex(&digest)
            && previous_still_current
        {
            let mut old = old.clone();
            if p.install_head && !attachment_incomplete {
                sqlx::query("UPDATE openlegal.corpus_object SET validated_at=$2::text::numeric,pending=false,version=version+1,desired_head_revision=$3 WHERE object_key=$1").bind(&k).bind(now.to_string()).bind(&p.record.revision_id).execute(&mut *tx).await.map_err(db)?;
            }
            if !attachment_incomplete {
                sqlx::query("UPDATE openlegal.corpus_revision SET last_validated_at=$3::text::numeric WHERE object_key=$1 AND revision_id=$2 AND latest_capture=$4")
                    .bind(&k).bind(&p.record.revision_id).bind(now.to_string()).bind(&old.capture_id).execute(&mut *tx).await.map_err(db)?;
            }
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
            if let Some(job) = &p.job_id {
                sqlx::query("UPDATE openlegal.corpus_job SET status='done',lease_until=NULL,completed_at=$4::text::numeric WHERE id=$1 AND object_key=$2 AND expected_version=$3").bind(Uuid::parse_str(job).map_err(|_|DatabaseError::InvalidInput)?).bind(&k).bind(version).bind(now.to_string()).execute(&mut *tx).await.map_err(db)?;
            }
            if update_job_gap {
                self.update_published_detail_gap(
                    &mut tx,
                    &p.record.object,
                    &p.record.revision_id,
                    attachment_incomplete,
                    now,
                )
                .await?;
            }
            check(&cancel)?;
            tx.commit().await.map_err(db)?;
            if !attachment_incomplete {
                old.validated_at = now;
            }
            return Ok(old);
        }
        let historical_add = if publish_head {
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
            record: p.record.clone(),
            retrieved_at: p.retrieved_at,
            captured_at: now,
            validated_at: now,
            processor_version: p.processor_version.clone(),
            raw_sha256: hex(&digest),
        };
        let value = serde_json::to_value(&capture).map_err(corrupt)?;
        let checksum = bytes_hash(&serde_json::to_vec(&value).map_err(corrupt)?);
        sqlx::query("INSERT INTO openlegal.corpus_capture(id,object_key,revision_id,sequence,captured_at,publication_date,effective_date,payload,payload_sha256,raw_sha256,raw_size,storage_key,event_sequence) VALUES($1,$2,$3,$4,$5::text::numeric,$6,$7,$8,$9,$10,$11,$12,$13)").bind(&capture_id).bind(&k).bind(&capture.record.revision_id).bind(sequence).bind(now.to_string()).bind(&capture.record.publication_date).bind(&capture.record.effective_date).bind(value).bind(checksum).bind(&digest).bind(size).bind(&storage_key).bind(event).execute(&mut *tx).await.map_err(db)?;
        let mut metadata_capture = capture.clone();
        metadata_capture.record.body.clear();
        metadata_capture.record.sections.clear();
        let metadata = serde_json::to_value(metadata_capture).map_err(corrupt)?;
        let metadata_checksum = bytes_hash(&serde_json::to_vec(&metadata).map_err(corrupt)?);
        sqlx::query("INSERT INTO openlegal.corpus_capture_catalog VALUES($1,$2,$3,$4,$5::text::numeric,$6,$7,$8,$9)").bind(&capture_id).bind(&k).bind(&capture.record.revision_id).bind(sequence).bind(now.to_string()).bind(&capture.record.publication_date).bind(&capture.record.effective_date).bind(metadata).bind(metadata_checksum).execute(&mut *tx).await.map_err(db)?;
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
        if !preserve_revision {
            sqlx::query("INSERT INTO openlegal.corpus_revision(object_key,revision_id,latest_capture,publication_date,effective_date,last_sequence,captured_at,last_validated_at) VALUES($1,$2,$3,$4,$5,$6,$7::text::numeric,$7::text::numeric) ON CONFLICT(object_key,revision_id) DO UPDATE SET latest_capture=EXCLUDED.latest_capture,publication_date=EXCLUDED.publication_date,effective_date=EXCLUDED.effective_date,last_sequence=EXCLUDED.last_sequence,captured_at=EXCLUDED.captured_at,last_validated_at=EXCLUDED.last_validated_at").bind(&k).bind(&capture.record.revision_id).bind(&capture_id).bind(&capture.record.publication_date).bind(&capture.record.effective_date).bind(sequence).bind(now.to_string()).execute(&mut *tx).await.map_err(db)?;
        }
        // A different desired revision keeps HEAD pending, so the old capture
        // cannot be served with a fresh claim after its replacement was seen.
        let clear_pending = publish_head || preserve_revision;
        sqlx::query("UPDATE openlegal.corpus_object SET version=version+1,next_capture=next_capture+1,head_capture=CASE WHEN $2 THEN $3 ELSE head_capture END,validated_at=CASE WHEN $2 THEN $4::text::numeric ELSE validated_at END,pending=CASE WHEN $5 THEN false ELSE pending END,desired_head_revision=CASE WHEN $2 THEN $6 ELSE desired_head_revision END WHERE object_key=$1").bind(&k).bind(publish_head).bind(&capture_id).bind(now.to_string()).bind(clear_pending).bind(&capture.record.revision_id).execute(&mut *tx).await.map_err(db)?;
        sqlx::query("INSERT INTO openlegal.corpus_outbox SELECT next_event,$1,$2,$3,false,$4,false FROM openlegal.corpus_control").bind(&k).bind(version+1).bind(&capture_id).bind(publish_head).execute(&mut *tx).await.map_err(db)?;
        sqlx::query("UPDATE openlegal.corpus_control SET next_event=next_event+1,raw_bytes=raw_bytes+$1,staged_bytes=staged_bytes-$1").bind(total_size as i64).execute(&mut *tx).await.map_err(db)?;
        for (location, _, _) in &staged_blobs {
            sqlx::query("DELETE FROM openlegal.corpus_staging WHERE storage_key=$1")
                .bind(location)
                .execute(&mut *tx)
                .await
                .map_err(db)?;
        }
        if let Some(job) = &p.job_id {
            let job = Uuid::parse_str(job).map_err(|_| DatabaseError::InvalidInput)?;
            sqlx::query("UPDATE openlegal.corpus_job SET status='done',lease_until=NULL,completed_at=$4::text::numeric WHERE id=$1 AND object_key=$2 AND expected_version=$3").bind(job).bind(&k).bind(version).bind(now.to_string()).execute(&mut *tx).await.map_err(db)?;
        }
        if update_job_gap {
            self.update_published_detail_gap(
                &mut tx,
                &capture.record.object,
                &capture.record.revision_id,
                attachment_incomplete,
                now,
            )
            .await?;
        }
        check(&cancel)?;
        tx.commit().await.map_err(db)?;
        Ok(capture)
        }).await
    }
}

impl DatabaseStore for PgCorpusStore {
    fn original_evidence(
        &self,
        capture_id: String,
        ordinal: u32,
        cancel: CancellationToken,
    ) -> BoxFuture<'static, Result<openlegal_domain::rights::OriginalEvidence, DatabaseError>> {
        self.original_evidence_future(capture_id, ordinal, cancel)
    }
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

impl PgCorpusStore {
    /// Read-only sanitized state; never initializes provider clients or records observations.
    pub async fn provider_admission_snapshot(
        &self,
    ) -> Result<openlegal_domain::provider_admin::ProviderAdmissionSnapshot, DatabaseError> {
        self.gate().await?;
        crate::postgres::provider_admin::runtime_snapshot(&self.pool).await
    }
    pub async fn provider_collection_held(&self) -> Result<bool, DatabaseError> {
        self.gate().await?;
        crate::postgres::provider_admin::held(&self.pool).await
    }
}

#[cfg(test)]
mod archive_hash_tests {
    use super::*;
    #[test]
    fn streamed_manifest_hash_preserves_existing_json_identity() {
        let value = serde_json::json!({"escaped": "\\\"\n\u{0000}", "nested": [null, true, 1, "한글"], "empty": {}});
        assert_eq!(
            json_hash(&value).unwrap(),
            bytes_hash(&serde_json::to_vec(&value).unwrap())
        );
    }
}

#[cfg(test)]
mod contention_tests {
    use super::*;
    use openlegal_application::{blob::*, persistence::PersistentStore};
    use openlegal_domain::{RetrievalError, rights::SourceRights};
    use std::sync::atomic::AtomicUsize;

    struct FixedClock;
    impl openlegal_application::Clock for FixedClock {
        fn now(&self) -> u64 {
            100
        }
    }

    /// Block final SQL only after the durable put succeeds. A repeated put would
    /// count and reacquire the lock, so this also detects accidental whole-operation retries.
    struct LockAfterPut {
        inner: Arc<dyn BlobStore>,
        pool: PgPool,
        puts: Arc<AtomicUsize>,
        release: Arc<tokio::sync::Mutex<Option<tokio::task::JoinHandle<()>>>>,
    }
    impl BlobStore for LockAfterPut {
        fn put_if_absent(
            &self,
            location: BlobLocation,
            bytes: Vec<u8>,
            cancel: CancellationToken,
        ) -> BoxFuture<'static, Result<BlobPutResult, RetrievalError>> {
            let inner = self.inner.clone();
            let pool = self.pool.clone();
            let puts = self.puts.clone();
            let release = self.release.clone();
            Box::pin(async move {
                puts.fetch_add(1, Ordering::SeqCst);
                let result = inner.put_if_absent(location, bytes, cancel).await?;
                let mut tx = pool.begin().await.unwrap();
                sqlx::query(
                    "SELECT singleton FROM openlegal.corpus_control WHERE singleton FOR UPDATE",
                )
                .fetch_one(&mut *tx)
                .await
                .unwrap();
                *release.lock().await = Some(tokio::spawn(async move {
                    tokio::time::sleep(std::time::Duration::from_millis(1250)).await;
                    tx.commit().await.unwrap();
                }));
                Ok(result)
            })
        }
        fn get(
            &self,
            location: BlobLocation,
            cancel: CancellationToken,
        ) -> BoxFuture<'static, Result<Option<Vec<u8>>, RetrievalError>> {
            self.inner.get(location, cancel)
        }
        fn delete_if_present(
            &self,
            location: BlobLocation,
            cancel: CancellationToken,
        ) -> BoxFuture<'static, Result<(), RetrievalError>> {
            self.inner.delete_if_present(location, cancel)
        }
        fn enumerate(
            &self,
            cursor: Option<String>,
            limit: usize,
            cancel: CancellationToken,
        ) -> BoxFuture<'static, Result<BlobPage, RetrievalError>> {
            self.inner.enumerate(cursor, limit, cancel)
        }
        fn cleanup_staging(
            &self,
            now: u64,
            limit: usize,
            cancel: CancellationToken,
        ) -> BoxFuture<'static, Result<usize, RetrievalError>> {
            self.inner.cleanup_staging(now, limit, cancel)
        }
        fn health(
            &self,
            cancel: CancellationToken,
        ) -> BoxFuture<'static, Result<(), RetrievalError>> {
            self.inner.health(cancel)
        }
        fn close(&self) -> BoxFuture<'static, Result<(), RetrievalError>> {
            self.inner.close()
        }
        fn metrics(&self) -> BlobMetrics {
            self.inner.metrics()
        }
    }

    #[tokio::test]
    #[ignore = "requires scripts/test-postgres.sh"]
    async fn final_publication_and_observation_contention_preserve_staging_and_put_once() {
        let fixture = crate::test_support::TestDatabase::new().await;
        let base = fixture.open(100).await;
        let inner =
            crate::blob::FsBlobStore::open(&fixture.directory.path().join("sql-phase-retry"))
                .await
                .unwrap();
        let puts = Arc::new(AtomicUsize::new(0));
        let release = Arc::new(tokio::sync::Mutex::new(None));
        let blobs = Arc::new(LockAfterPut {
            inner,
            pool: base.pool(),
            puts: puts.clone(),
            release: release.clone(),
        });
        let store =
            PgCorpusStore::with_publication_clock(base.pool(), blobs.clone(), Arc::new(FixedClock));
        let raw = b"Fictional fixture body".to_vec();
        // Independently reject the reservation phase before any blob is written.
        let mut reservation_lock = base.pool().begin().await.unwrap();
        sqlx::query("SELECT singleton FROM openlegal.corpus_control WHERE singleton FOR UPDATE")
            .fetch_one(&mut *reservation_lock)
            .await
            .unwrap();
        let reservation_release = tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(1250)).await;
            reservation_lock.commit().await.unwrap();
        });
        let capture = store
            .publish(
                Publication {
                    record: LegalRecord {
                        object: ObjectId {
                            jurisdiction: "kr".into(),
                            provider: "fictional_test".into(),
                            dataset: Dataset::NationalStatute,
                            id: "contention".into(),
                        },
                        revision_id: "r1".into(),
                        title: "Fictional contention fixture".into(),
                        body: "Fictional fixture body".into(),
                        metadata: Default::default(),
                        publication_date: None,
                        effective_date: None,
                        source_url: "https://example.test/fixture".into(),
                        representation: "provider_text_v1".into(),
                        sections: vec![],
                    },
                    raw: raw.clone(),
                    additional_evidence: vec![],
                    processor_version: "fixture_v1".into(),
                    retrieved_at: 100,
                    now: 100,
                    expected_version: 0,
                    install_head: true,
                    job_id: None,
                },
                CancellationToken::new(),
            )
            .await
            .unwrap();
        reservation_release.await.unwrap();
        release.lock().await.take().unwrap().await.unwrap();
        assert_eq!(puts.load(Ordering::SeqCst), 1);
        assert_eq!(capture.sequence, 1);
        let input = SourceObservationInput {
            source_key: "law_go_kr:lsEfYdInfoGuide:contention_fixture".into(),
            raw: Some(raw.clone()),
            media_type: "application/xml".into(),
            rights: SourceRights::legal_information(),
            metadata: Default::default(),
            observed_at: 100,
        };
        let observation = store
            .retain_source_observation(input, CancellationToken::new())
            .await
            .unwrap();
        release.lock().await.take().unwrap().await.unwrap();
        assert_eq!(puts.load(Ordering::SeqCst), 2);
        assert_eq!(
            store
                .source_observation_bytes(&observation.observation_id, CancellationToken::new())
                .await
                .unwrap(),
            raw
        );
        let row = sqlx::query("SELECT raw_bytes,staged_bytes,next_event,(SELECT count(*) FROM openlegal.corpus_staging) stages,(SELECT count(*) FROM openlegal.corpus_capture) captures,(SELECT count(*) FROM openlegal.corpus_outbox) events,(SELECT count(*) FROM openlegal.corpus_source_observation) observations FROM openlegal.corpus_control").fetch_one(&base.pool()).await.unwrap();
        assert_eq!(row.get::<i64, _>("raw_bytes"), (raw.len() * 2) as i64);
        assert_eq!(row.get::<i64, _>("staged_bytes"), 0);
        assert_eq!(row.get::<i64, _>("stages"), 0);
        assert_eq!(row.get::<i64, _>("captures"), 1);
        assert_eq!(row.get::<i64, _>("events"), 1);
        assert_eq!(row.get::<i64, _>("observations"), 1);
        assert_eq!(row.get::<i64, _>("next_event"), 2);
        base.close().await.unwrap();
    }
}
