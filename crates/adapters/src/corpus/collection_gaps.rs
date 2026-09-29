use super::{PgCorpusStore, db, key};
use openlegal_application::database::Job;
use openlegal_domain::legal::{CollectionNotice, DatabaseError, Dataset, ObjectId};
use sha2::{Digest, Sha256};
use sqlx::Postgres;
use sqlx::Row;

pub struct PageGapObservation<'a> {
    pub reason: &'a str,
    pub rows: usize,
    pub now: u64,
}

fn dataset_name(dataset: Dataset) -> Result<String, DatabaseError> {
    serde_json::to_value(dataset)
        .map_err(|_| DatabaseError::StorageCorrupt)?
        .as_str()
        .map(str::to_owned)
        .ok_or(DatabaseError::StorageCorrupt)
}
fn page_key(dataset: &str, historical: bool, class: Option<u8>, page: u32) -> String {
    format!("p:{dataset}:{historical}:{}:{page}", class.unwrap_or(0))
}
pub(super) fn detail_key(object_key: &str, revision: &str) -> String {
    let digest = Sha256::digest(format!("{object_key}:{revision}").as_bytes());
    format!(
        "d:{}",
        digest
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>()
    )
}
impl PgCorpusStore {
    /// Bound each dataset's share of the global 128-job queue so a large first
    /// page cannot prevent the other datasets from being observed and queued.
    pub async fn active_jobs_for_dataset(&self, dataset: Dataset) -> Result<u64, DatabaseError> {
        self.gate().await?;
        let dataset = dataset_name(dataset)?;
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM openlegal.corpus_job j JOIN openlegal.corpus_object o USING(object_key) WHERE j.status IN ('pending','running') AND o.identity->>'dataset'=$1")
            .bind(dataset).fetch_one(&self.pool).await.map_err(db)?;
        count.try_into().map_err(|_| DatabaseError::StorageCorrupt)
    }
    pub async fn active_detail_job(
        &self,
        object: &ObjectId,
        revision_id: &str,
        install_head: bool,
    ) -> Result<bool, DatabaseError> {
        self.gate().await?;
        let object_key = key(object)?;
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM openlegal.corpus_job WHERE object_key=$1 AND revision_id=$2 AND status IN ('pending','running') AND (NOT $3 OR install_head))")
            .bind(object_key).bind(revision_id).bind(install_head)
            .fetch_one(&self.pool).await.map_err(db)
    }
    pub(super) async fn update_published_detail_gap(
        &self,
        tx: &mut sqlx::Transaction<'_, Postgres>,
        object: &ObjectId,
        revision: &str,
        incomplete: bool,
        now: u64,
    ) -> Result<(), DatabaseError> {
        let object_key = key(object)?;
        let gap = detail_key(&object_key, revision);
        if incomplete {
            let dataset = dataset_name(object.dataset)?;
            sqlx::query("INSERT INTO openlegal.provider_collection_gap(gap_key,dataset,scope,object_key,revision_id,reason,first_seen_at,last_seen_at,retry_at) VALUES($1,$2,'detail',$3,$4,'attachment_incomplete',$5,$5,$6) ON CONFLICT(gap_key) DO UPDATE SET reason=EXCLUDED.reason,last_seen_at=EXCLUDED.last_seen_at,retry_at=EXCLUDED.retry_at,resolved_at=NULL")
                .bind(gap).bind(dataset).bind(object_key).bind(revision).bind(now as i64).bind(now.saturating_add(3600) as i64)
                .execute(&mut **tx).await.map_err(db)?;
        } else {
            sqlx::query("UPDATE openlegal.provider_collection_gap SET resolved_at=$2 WHERE gap_key=$1 AND resolved_at IS NULL")
                .bind(gap).bind(now as i64).execute(&mut **tx).await.map_err(db)?;
        }
        Ok(())
    }
    pub(super) async fn ensure_gap_capacity(&self, gap: &str) -> Result<(), DatabaseError> {
        let allowed: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM openlegal.provider_collection_gap WHERE gap_key=$1) OR (SELECT count(*) FROM openlegal.provider_collection_gap WHERE resolved_at IS NULL)<100000")
            .bind(gap).fetch_one(&self.pool).await.map_err(db)?;
        if allowed {
            Ok(())
        } else {
            Err(DatabaseError::Capacity)
        }
    }
    pub async fn record_page_gap(
        &self,
        dataset: Dataset,
        historical: bool,
        class: Option<u8>,
        page: u32,
        observation: PageGapObservation<'_>,
    ) -> Result<(), DatabaseError> {
        let dataset = dataset_name(dataset)?;
        let rows = i32::try_from(observation.rows.clamp(1, 100))
            .map_err(|_| DatabaseError::InvalidInput)?;
        let page = i32::try_from(page).map_err(|_| DatabaseError::InvalidInput)?;
        let gap = page_key(&dataset, historical, class, page as u32);
        self.ensure_gap_capacity(&gap).await?;
        sqlx::query("INSERT INTO openlegal.provider_collection_gap(gap_key,dataset,scope,historical,treaty_class,page,reason,affected_rows,first_seen_at,last_seen_at,retry_at) VALUES($1,$2,'page',$3,$4,$5,$6,$7,$8,$8,$9) ON CONFLICT(gap_key) DO UPDATE SET reason=EXCLUDED.reason,affected_rows=EXCLUDED.affected_rows,last_seen_at=EXCLUDED.last_seen_at,retry_at=EXCLUDED.retry_at,resolved_at=NULL")
            .bind(gap).bind(dataset).bind(historical).bind(class.map(i16::from)).bind(page).bind(observation.reason).bind(rows).bind(observation.now as i64).bind(observation.now.saturating_add(3600) as i64)
            .execute(&self.pool).await.map_err(db)?;
        Ok(())
    }
    pub async fn resolve_page_gap(
        &self,
        dataset: Dataset,
        historical: bool,
        class: Option<u8>,
        page: u32,
        now: u64,
    ) -> Result<(), DatabaseError> {
        let dataset = dataset_name(dataset)?;
        sqlx::query("UPDATE openlegal.provider_collection_gap SET resolved_at=$2 WHERE gap_key=$1 AND resolved_at IS NULL")
            .bind(page_key(&dataset,historical,class,page)).bind(now as i64).execute(&self.pool).await.map_err(db)?;
        Ok(())
    }
    pub async fn due_gap_page(
        &self,
        dataset: Dataset,
        historical: bool,
        class: Option<u8>,
        now: u64,
    ) -> Result<Option<u32>, DatabaseError> {
        let dataset = dataset_name(dataset)?;
        let page: Option<i32> = sqlx::query_scalar("SELECT page FROM openlegal.provider_collection_gap WHERE dataset=$1 AND scope='page' AND historical=$2 AND treaty_class IS NOT DISTINCT FROM $3 AND resolved_at IS NULL AND retry_at<=$4 ORDER BY retry_at,page LIMIT 1")
            .bind(dataset).bind(historical).bind(class.map(i16::from)).bind(now as i64).fetch_optional(&self.pool).await.map_err(db)?;
        page.map(|p| u32::try_from(p).map_err(|_| DatabaseError::StorageCorrupt))
            .transpose()
    }
    pub async fn skip_claim(&self, job: &Job, reason: &str, now: u64) -> Result<(), DatabaseError> {
        let object_key = key(&job.object)?;
        let dataset = dataset_name(job.object.dataset)?;
        let gap = detail_key(&object_key, &job.revision_id);
        self.ensure_gap_capacity(&gap).await?;
        let id = uuid::Uuid::parse_str(&job.id).map_err(|_| DatabaseError::InvalidInput)?;
        let mut tx = self.pool.begin().await.map_err(db)?;
        let rows = sqlx::query("UPDATE openlegal.corpus_job SET status='failed',lease_until=NULL,error_category=$2,completed_at=$5::text::numeric WHERE id=$1 AND expected_version=$3 AND attempts=$4 AND status='running'")
            .bind(id).bind(reason).bind(job.expected_version as i64).bind(job.attempts as i32)
            .bind(now.to_string())
            .execute(&mut *tx).await.map_err(db)?.rows_affected();
        if rows != 1 {
            return Err(DatabaseError::Conflict);
        }
        sqlx::query("INSERT INTO openlegal.provider_collection_gap(gap_key,dataset,scope,object_key,revision_id,reason,first_seen_at,last_seen_at,retry_at) VALUES($1,$2,'detail',$3,$4,$5,$6,$6,$7) ON CONFLICT(gap_key) DO UPDATE SET reason=EXCLUDED.reason,last_seen_at=EXCLUDED.last_seen_at,retry_at=EXCLUDED.retry_at,resolved_at=NULL")
            .bind(gap).bind(dataset).bind(object_key).bind(&job.revision_id).bind(reason).bind(now as i64).bind(now.saturating_add(3600) as i64)
            .execute(&mut *tx).await.map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(())
    }
    pub async fn resolve_detail_gap(
        &self,
        object: &ObjectId,
        revision: &str,
        now: u64,
    ) -> Result<(), DatabaseError> {
        let object_key = key(object)?;
        sqlx::query("UPDATE openlegal.provider_collection_gap SET resolved_at=$2 WHERE gap_key=$1 AND resolved_at IS NULL")
            .bind(detail_key(&object_key,revision)).bind(now as i64).execute(&self.pool).await.map_err(db)?;
        Ok(())
    }
    pub async fn detail_gap_active(
        &self,
        object: &ObjectId,
        revision: &str,
    ) -> Result<bool, DatabaseError> {
        let object_key = key(object)?;
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM openlegal.provider_collection_gap WHERE gap_key=$1 AND resolved_at IS NULL)")
            .bind(detail_key(&object_key, revision)).fetch_one(&self.pool).await.map_err(db)
    }
    pub async fn requeue_due_details(&self, now: u64) -> Result<u64, DatabaseError> {
        let mut tx = self.pool.begin().await.map_err(db)?;
        // An isolated request Pod may disappear after enqueue or claim. After
        // its deadline (or the claim lease) the continuous worker may safely
        // adopt the stranded job without issuing a duplicate provider call.
        sqlx::query("UPDATE openlegal.corpus_job SET source_metadata=source_metadata - 'collection_origin' WHERE source_metadata->>'collection_origin'='explicit' AND ((status='pending' AND created_at<=$1::text::numeric-8100) OR (status='running' AND lease_until<=$1::text::numeric))")
            .bind(now.to_string()).execute(&mut *tx).await.map_err(db)?;
        sqlx::query("DELETE FROM openlegal.provider_collection_gap WHERE resolved_at IS NOT NULL AND resolved_at<$1")
            .bind(now.saturating_sub(30*86400) as i64).execute(&mut *tx).await.map_err(db)?;
        let queued: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM openlegal.corpus_job WHERE status IN ('pending','running')",
        )
        .fetch_one(&mut *tx)
        .await
        .map_err(db)?;
        let available = (128i64 - queued).clamp(0, 16);
        let rows = sqlx::query("SELECT g.gap_key,j.id FROM openlegal.provider_collection_gap g JOIN openlegal.corpus_job j ON j.object_key=g.object_key AND j.revision_id=g.revision_id JOIN openlegal.corpus_object o ON o.object_key=j.object_key WHERE g.scope='detail' AND g.resolved_at IS NULL AND g.retry_at<=$1 AND j.status IN ('failed','done') AND (NOT j.install_head OR o.desired_head_revision=j.revision_id) ORDER BY g.retry_at LIMIT $2 FOR UPDATE OF g SKIP LOCKED")
            .bind(now as i64).bind(available).fetch_all(&mut *tx).await.map_err(db)?;
        for row in &rows {
            let id: uuid::Uuid = row.try_get("id").map_err(db)?;
            let gap: String = row.try_get("gap_key").map_err(db)?;
            sqlx::query("UPDATE openlegal.corpus_job SET status='pending',attempts=0,lease_until=NULL,error_category=NULL,started_at=NULL,completed_at=NULL,source_metadata=source_metadata - 'collection_origin' WHERE id=$1")
                .bind(id).execute(&mut *tx).await.map_err(db)?;
            sqlx::query(
                "UPDATE openlegal.provider_collection_gap SET retry_at=$2 WHERE gap_key=$1",
            )
            .bind(gap)
            .bind(now.saturating_add(3600) as i64)
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        }
        tx.commit().await.map_err(db)?;
        Ok(rows.len() as u64)
    }
    pub async fn collection_notices(
        &self,
        datasets: &[Dataset],
        object: Option<&ObjectId>,
    ) -> Result<Vec<CollectionNotice>, DatabaseError> {
        let names = datasets
            .iter()
            .copied()
            .map(dataset_name)
            .collect::<Result<Vec<_>, _>>()?;
        let object_key = object.map(key).transpose()?;
        let rows = sqlx::query("SELECT dataset,scope,reason,sum(affected_rows)::bigint AS affected_count,max(last_seen_at) AS last_seen_at,min(retry_at) AS retry_at FROM openlegal.provider_collection_gap WHERE resolved_at IS NULL AND dataset=ANY($1) AND (scope='page' OR $2::text IS NULL OR object_key=$2) GROUP BY dataset,scope,reason ORDER BY dataset,scope,reason LIMIT 64")
            .bind(names).bind(object_key).fetch_all(&self.pool).await.map_err(db)?;
        rows.into_iter()
            .map(|row| {
                let name: String = row.try_get("dataset").map_err(db)?;
                let dataset: Dataset = serde_json::from_value(serde_json::Value::String(name))
                    .map_err(|_| DatabaseError::StorageCorrupt)?;
                let scope: String = row.try_get("scope").map_err(db)?;
                let code: String = row.try_get("reason").map_err(db)?;
                let count: i64 = row.try_get("affected_count").map_err(db)?;
                let seen: i64 = row.try_get("last_seen_at").map_err(db)?;
                let retry: i64 = row.try_get("retry_at").map_err(db)?;
                Ok(CollectionNotice {
                    dataset,
                    scope,
                    code,
                    affected_count: count
                        .try_into()
                        .map_err(|_| DatabaseError::StorageCorrupt)?,
                    last_seen_at: seen.try_into().map_err(|_| DatabaseError::StorageCorrupt)?,
                    retry_at: retry
                        .try_into()
                        .map_err(|_| DatabaseError::StorageCorrupt)?,
                })
            })
            .collect()
    }
}
