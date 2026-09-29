use super::*;
use openlegal_domain::collection::{CollectionReceipt, CollectionRequest};

impl PgCorpusStore {
    /// Return unsettled request Jobs. A launched Job can remain `launching`
    /// when the scheduler loses its database acknowledgement.
    pub async fn unsettled_collection_jobs(
        &self,
    ) -> Result<Vec<(String, Option<String>)>, DatabaseError> {
        self.gate().await?;
        let rows = sqlx::query("SELECT id,job_name FROM openlegal.collection_request WHERE status IN ('launching','running') ORDER BY created_at,id LIMIT 16")
            .fetch_all(&self.pool).await.map_err(db)?;
        rows.into_iter()
            .map(|row| {
                Ok((
                    row.try_get::<Uuid, _>("id").map_err(db)?.to_string(),
                    row.try_get("job_name").map_err(db)?,
                ))
            })
            .collect()
    }

    /// A terminal failed Job can race with the request Pod's own settlement.
    pub async fn fail_finished_collection_job(
        &self,
        id: &str,
        job_name: &str,
    ) -> Result<(), DatabaseError> {
        let id = Uuid::parse_str(id).map_err(|_| DatabaseError::InvalidInput)?;
        sqlx::query("UPDATE openlegal.collection_request SET status='failed',payload='{}'::jsonb,lease_until=NULL,reason='worker_failed' WHERE id=$1 AND ((status='running' AND job_name=$2) OR (status='launching' AND job_name IS NULL))")
            .bind(id).bind(job_name).execute(&self.pool).await.map_err(db)?;
        Ok(())
    }
    pub async fn heartbeat_collection_scheduler(&self) -> Result<(), DatabaseError> {
        self.gate().await?;
        sqlx::query("UPDATE openlegal.corpus_control SET collection_scheduler_seen_at=floor(extract(epoch from clock_timestamp()))::bigint WHERE singleton")
            .execute(&self.pool).await.map_err(db)?;
        Ok(())
    }
    /// Claim one request only when fewer than sixteen claims have been launched.
    /// A lost launch acknowledgement is left visible for operator reconciliation.
    pub async fn claim_collection_request(
        &self,
    ) -> Result<Option<(String, CollectionRequest)>, DatabaseError> {
        self.gate().await?;
        let mut tx = self.pool.begin().await.map_err(db)?;
        sqlx::query("SELECT singleton FROM openlegal.corpus_control WHERE singleton FOR UPDATE")
            .fetch_one(&mut *tx)
            .await
            .map_err(db)?;
        let active: i64 = sqlx::query_scalar("SELECT count(*) FROM openlegal.collection_request WHERE status IN ('launching','running')")
            .fetch_one(&mut *tx).await.map_err(db)?;
        if active >= 16 {
            return Ok(None);
        }
        let row = sqlx::query("SELECT id,payload FROM openlegal.collection_request WHERE expires_at>floor(extract(epoch from clock_timestamp()))::bigint AND (status='queued' OR (status='deferred' AND lease_until<=floor(extract(epoch from clock_timestamp()))::bigint)) ORDER BY created_at,id LIMIT 1 FOR UPDATE SKIP LOCKED")
            .fetch_optional(&mut *tx).await.map_err(db)?;
        let Some(row) = row else {
            return Ok(None);
        };
        let id: Uuid = row.try_get("id").map_err(db)?;
        let request: CollectionRequest =
            serde_json::from_value(row.try_get("payload").map_err(db)?).map_err(corrupt)?;
        request.validate()?;
        sqlx::query("UPDATE openlegal.collection_request SET status='launching',lease_until=floor(extract(epoch from clock_timestamp()))::bigint+8100 WHERE id=$1")
            .bind(id).execute(&mut *tx).await.map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(Some((id.to_string(), request)))
    }

    pub async fn mark_collection_running(
        &self,
        id: &str,
        job_name: &str,
    ) -> Result<(), DatabaseError> {
        let id = Uuid::parse_str(id).map_err(|_| DatabaseError::InvalidInput)?;
        if job_name.len() > 63
            || !job_name
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        {
            return Err(DatabaseError::InvalidInput);
        }
        let changed = sqlx::query("UPDATE openlegal.collection_request SET status='running',job_name=$2,lease_until=floor(extract(epoch from clock_timestamp()))::bigint+8100 WHERE id=$1 AND status='launching'")
            .bind(id).bind(job_name).execute(&self.pool).await.map_err(db)?.rows_affected();
        if changed != 1 {
            let status: Option<String> =
                sqlx::query_scalar("SELECT status FROM openlegal.collection_request WHERE id=$1")
                    .bind(id)
                    .fetch_optional(&self.pool)
                    .await
                    .map_err(db)?;
            if !status
                .as_deref()
                .is_some_and(|s| matches!(s, "done" | "skipped" | "deferred" | "failed"))
            {
                return Err(DatabaseError::Conflict);
            }
        }
        Ok(())
    }

    pub async fn load_collection_request(
        &self,
        id: &str,
    ) -> Result<CollectionRequest, DatabaseError> {
        self.gate().await?;
        let id = Uuid::parse_str(id).map_err(|_| DatabaseError::InvalidInput)?;
        let payload: serde_json::Value = sqlx::query_scalar("SELECT payload FROM openlegal.collection_request WHERE id=$1 AND status IN ('launching','running')")
            .bind(id).fetch_optional(&self.pool).await.map_err(db)?.ok_or(DatabaseError::NotFound)?;
        let request: CollectionRequest = serde_json::from_value(payload).map_err(corrupt)?;
        request.validate()?;
        Ok(request)
    }

    pub async fn settle_collection_request(
        &self,
        id: &str,
        status: &str,
    ) -> Result<(), DatabaseError> {
        self.settle_collection_request_with_reason(id, status, None)
            .await
    }

    pub async fn settle_collection_request_with_reason(
        &self,
        id: &str,
        status: &str,
        reason: Option<&str>,
    ) -> Result<(), DatabaseError> {
        if !matches!(status, "done" | "skipped" | "deferred" | "failed") {
            return Err(DatabaseError::InvalidInput);
        }
        if reason.is_some_and(|value| {
            !matches!(
                value,
                "ambiguous"
                    | "source_inventory_incomplete"
                    | "not_found"
                    | "source_data_invalid"
                    | "source_unavailable"
                    | "download_failed"
                    | "identity_conflict"
                    | "worker_failed"
                    | "worker_lost"
            )
        }) {
            return Err(DatabaseError::InvalidInput);
        }
        let id = Uuid::parse_str(id).map_err(|_| DatabaseError::InvalidInput)?;
        let changed = sqlx::query("UPDATE openlegal.collection_request SET status=$2,payload=CASE WHEN $2='deferred' THEN payload ELSE '{}'::jsonb END,lease_until=CASE WHEN $2='deferred' THEN floor(extract(epoch from clock_timestamp()))::bigint+3600 ELSE NULL END,reason=$3 WHERE id=$1 AND status IN ('launching','running')")
            .bind(id).bind(status).bind(reason).execute(&self.pool).await.map_err(db)?.rows_affected();
        if changed != 1 {
            return Err(DatabaseError::Conflict);
        }
        Ok(())
    }

    pub async fn request_collection(
        &self,
        request: CollectionRequest,
    ) -> Result<CollectionReceipt, DatabaseError> {
        request.validate()?;
        let request = request.normalized();
        let payload = serde_json::to_value(&request).map_err(corrupt)?;
        let request_key = hex(&bytes_hash(&serde_json::to_vec(&payload).map_err(corrupt)?));
        self.gate().await?;
        let mut tx = self.pool.begin().await.map_err(db)?;
        sqlx::query("SELECT singleton FROM openlegal.corpus_control WHERE singleton FOR UPDATE")
            .fetch_one(&mut *tx)
            .await
            .map_err(db)?;
        let now: i64 =
            sqlx::query_scalar("SELECT floor(extract(epoch from clock_timestamp()))::bigint")
                .fetch_one(&mut *tx)
                .await
                .map_err(db)?;
        let scheduler_seen: i64 = sqlx::query_scalar(
            "SELECT collection_scheduler_seen_at FROM openlegal.corpus_control WHERE singleton FOR UPDATE",
        )
        .fetch_one(&mut *tx)
        .await
        .map_err(db)?;
        if scheduler_seen < now.saturating_sub(30) {
            return Err(DatabaseError::Capacity);
        }
        if let Some(row) = sqlx::query("SELECT id,status,reason,created_at,expires_at FROM openlegal.collection_request WHERE request_key=$1 FOR UPDATE")
            .bind(&request_key).fetch_optional(&mut *tx).await.map_err(db)? {
            let id: Uuid = row.try_get("id").map_err(db)?;
            let status: String = row.try_get("status").map_err(db)?;
            let reason: Option<String> = row.try_get("reason").map_err(db)?;
            let created: i64 = row.try_get("created_at").map_err(db)?;
            let expires: i64 = row.try_get("expires_at").map_err(db)?;
            let terminal_retry_due = matches!(status.as_str(), "failed" | "skipped")
                && created.saturating_add(3600) <= now;
            if (expires > now && !terminal_retry_due)
                || matches!(status.as_str(), "launching" | "running")
            {
                tx.commit().await.map_err(db)?;
                let retry_after_seconds = if status == "deferred" { 3600 } else if matches!(status.as_str(), "queued" | "launching" | "running") { 10 } else { 0 };
                return Ok(CollectionReceipt { request_id: id.to_string(), status, retry_after_seconds, reason });
            }
            if !matches!(status.as_str(), "queued" | "launching" | "running") {
                let queued: i64 = sqlx::query_scalar("SELECT count(*) FROM openlegal.collection_request WHERE status IN ('queued','launching','running')")
                    .fetch_one(&mut *tx).await.map_err(db)?;
                if queued >= 128 {
                    return Err(DatabaseError::Capacity);
                }
            }
            // A terminal receipt remains addressable by its original ID until
            // expiry, even when an equal request is retried after an hour.
            let old_key = hex(&bytes_hash(format!("retired:{request_key}:{id}").as_bytes()));
            sqlx::query("UPDATE openlegal.collection_request SET request_key=$2 WHERE id=$1")
                .bind(id).bind(old_key).execute(&mut *tx).await.map_err(db)?;
            let new_id: Uuid = sqlx::query_scalar("INSERT INTO openlegal.collection_request(request_key,payload,status,created_at,expires_at) VALUES($1,$2,'queued',$3,$4) RETURNING id")
                .bind(&request_key).bind(payload).bind(now).bind(now+86400)
                .fetch_one(&mut *tx).await.map_err(db)?;
            tx.commit().await.map_err(db)?;
            return Ok(CollectionReceipt { request_id: new_id.to_string(), status: "queued".into(), retry_after_seconds: 10, reason: None });
        }
        let queued: i64 = sqlx::query_scalar("SELECT count(*) FROM openlegal.collection_request WHERE status IN ('queued','launching','running')")
            .fetch_one(&mut *tx).await.map_err(db)?;
        if queued >= 128 {
            return Err(DatabaseError::Capacity);
        }
        let id: Uuid = sqlx::query_scalar("INSERT INTO openlegal.collection_request(request_key,payload,status,created_at,expires_at) VALUES($1,$2,'queued',$3,$4) RETURNING id")
            .bind(request_key).bind(payload).bind(now).bind(now+86400)
            .fetch_one(&mut *tx).await.map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(CollectionReceipt {
            request_id: id.to_string(),
            status: "queued".into(),
            retry_after_seconds: 10,
            reason: None,
        })
    }

    pub async fn collection_status(&self, id: &str) -> Result<CollectionReceipt, DatabaseError> {
        self.gate().await?;
        let id = Uuid::parse_str(id).map_err(|_| DatabaseError::InvalidInput)?;
        let row = sqlx::query("SELECT status,reason FROM openlegal.collection_request WHERE id=$1")
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .map_err(db)?
            .ok_or(DatabaseError::NotFound)?;
        let status: String = row.try_get("status").map_err(db)?;
        let reason: Option<String> = row.try_get("reason").map_err(db)?;
        let retry_after_seconds = if status == "deferred" {
            3600
        } else if matches!(status.as_str(), "queued" | "launching" | "running") {
            10
        } else {
            0
        };
        Ok(CollectionReceipt {
            request_id: id.to_string(),
            status,
            retry_after_seconds,
            reason,
        })
    }

    pub async fn prune_collection_requests(&self) -> Result<(), DatabaseError> {
        sqlx::query("DELETE FROM openlegal.collection_request WHERE expires_at < floor(extract(epoch from clock_timestamp()))::bigint AND status NOT IN ('launching','running')")
            .execute(&self.pool).await.map_err(db)?;
        Ok(())
    }
    pub async fn reap_stale_collection_requests(&self) -> Result<(), DatabaseError> {
        self.gate().await?;
        sqlx::query("UPDATE openlegal.collection_request SET status='failed',payload='{}'::jsonb,lease_until=NULL,reason='worker_lost' WHERE status IN ('launching','running') AND lease_until < floor(extract(epoch from clock_timestamp()))::bigint")
            .execute(&self.pool).await.map_err(db)?;
        Ok(())
    }
}
