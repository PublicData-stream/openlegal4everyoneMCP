use super::*;
use openlegal_application::demand_collection::DemandCollectionStore;
use openlegal_application::upstream_policy::RequestLimit;
use openlegal_domain::collection::{
    CollectionReceipt, CollectionRequest, CollectionTarget, DemandCollectionState as DemandState,
    DemandCollectionStatus,
};

/// Private launch policy: never included in public request identity or payload.
#[derive(Clone, Debug)]
pub struct CollectionLaunch {
    pub id: String,
    pub request: CollectionRequest,
    pub timeout_secs: u64,
    pub attempt_limit: RequestLimit,
    pub launched_at: u64,
    pub observed_at: u64,
}
impl CollectionLaunch {
    pub fn job_deadline_secs(&self) -> u64 {
        self.timeout_secs + 300
    }
    pub fn recovery_at(&self) -> u64 {
        self.launched_at + self.timeout_secs + 900
    }
    pub fn remaining_secs(&self, now: u64) -> u64 {
        self.launched_at
            .saturating_add(self.timeout_secs)
            .saturating_sub(now)
    }
}
fn launch_from_row(row: &PgRow) -> Result<CollectionLaunch, DatabaseError> {
    let request: CollectionRequest =
        serde_json::from_value(row.try_get("payload").map_err(db)?).map_err(corrupt)?;
    request.validate()?;
    let limit: Option<i64> = row.try_get("operation_attempt_limit").map_err(db)?;
    let attempt_limit = match limit {
        Some(value) => RequestLimit::Limited(u32::try_from(value).map_err(corrupt)?),
        None => RequestLimit::Unlimited,
    };
    Ok(CollectionLaunch {
        id: row.try_get::<Uuid, _>("id").map_err(db)?.to_string(),
        request,
        timeout_secs: u64::try_from(
            row.try_get::<i64, _>("operation_timeout_secs")
                .map_err(db)?,
        )
        .map_err(corrupt)?,
        attempt_limit,
        observed_at: u64::try_from(row.try_get::<i64, _>("observed_at").map_err(db)?)
            .map_err(corrupt)?,
        launched_at: u64::try_from(row.try_get::<i64, _>("launched_at").map_err(db)?)
            .map_err(corrupt)?,
    })
}

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

    /// Each scheduler observation includes the ownership epoch. A finished Job
    /// from an earlier deferred launch must never settle a newer launch.
    pub async fn unsettled_collection_launches(
        &self,
    ) -> Result<Vec<(String, Option<String>, u64)>, DatabaseError> {
        self.gate().await?;
        let rows = sqlx::query("SELECT id,job_name,launched_at FROM openlegal.collection_request WHERE status IN ('launching','running') ORDER BY created_at,id LIMIT 16")
            .fetch_all(&self.pool).await.map_err(db)?;
        rows.into_iter()
            .map(|row| {
                Ok((
                    row.try_get::<Uuid, _>("id").map_err(db)?.to_string(),
                    row.try_get("job_name").map_err(db)?,
                    u64::try_from(row.try_get::<i64, _>("launched_at").map_err(db)?)
                        .map_err(corrupt)?,
                ))
            })
            .collect()
    }

    pub async fn fail_finished_collection_launch(
        &self,
        id: &str,
        job_name: &str,
        epoch: u64,
    ) -> Result<(), DatabaseError> {
        self.gate().await?;
        let id = Uuid::parse_str(id).map_err(|_| DatabaseError::InvalidInput)?;
        let epoch = i64::try_from(epoch).map_err(|_| DatabaseError::InvalidInput)?;
        sqlx::query("UPDATE openlegal.collection_request SET status='failed',payload='{}'::jsonb,lease_until=NULL,reason='worker_failed' WHERE id=$1 AND launched_at=$3 AND ((status='running' AND job_name=$2) OR (status='launching' AND job_name IS NULL))")
            .bind(id).bind(job_name).bind(epoch).execute(&self.pool).await.map_err(db)?;
        Ok(())
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
        Ok(self
            .claim_collection_request_with_policy(7200, RequestLimit::Limited(32))
            .await?
            .map(|launch| (launch.id, launch.request)))
    }
    /// Snapshot policy atomically with launch; later ConfigMap changes cannot shorten it.
    pub async fn claim_collection_request_with_policy(
        &self,
        timeout_secs: u64,
        attempt_limit: RequestLimit,
    ) -> Result<Option<CollectionLaunch>, DatabaseError> {
        if !(60..=86400).contains(&timeout_secs) {
            return Err(DatabaseError::InvalidInput);
        }
        attempt_limit
            .validate()
            .map_err(|_| DatabaseError::InvalidInput)?;
        self.gate().await?;
        let mut tx = begin_storage(&self.pool, "claim_collection_request_with_policy").await?;
        sqlx::query("SELECT singleton FROM openlegal.corpus_control WHERE singleton FOR UPDATE")
            .fetch_one(&mut *tx)
            .await
            .map_err(db)?;
        let held: bool = sqlx::query_scalar("SELECT openlegal_admin.provider_held()")
            .fetch_one(&mut *tx)
            .await
            .map_err(db)?;
        if held {
            return Ok(None);
        }

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
        let row = sqlx::query("UPDATE openlegal.collection_request SET status='launching',job_name=NULL,launched_at=GREATEST(floor(extract(epoch from clock_timestamp()))::bigint,COALESCE(launched_at,0)+1),operation_timeout_secs=$2,operation_attempt_limit=$3,lease_until=GREATEST(floor(extract(epoch from clock_timestamp()))::bigint,COALESCE(launched_at,0)+1)+$2+900 WHERE id=$1 RETURNING id,payload,launched_at,operation_timeout_secs,operation_attempt_limit,floor(extract(epoch from clock_timestamp()))::bigint AS observed_at")
            .bind(id).bind(timeout_secs as i64).bind(attempt_limit.as_option().map(i64::from))
            .fetch_one(&mut *tx).await.map_err(db)?;
        let launch = launch_from_row(&row)?;
        // Claim COMMIT acknowledgements cannot be retried as pre-admission
        // contention: the new launch epoch may already be durable.
        tx.commit()
            .await
            .map_err(|_| DatabaseError::StorageUnavailable)?;
        Ok(Some(launch))
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
        let changed = sqlx::query("UPDATE openlegal.collection_request SET status='running',job_name=$2 WHERE id=$1 AND status='launching'")
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

    pub async fn mark_collection_launch_running(
        &self,
        launch: &CollectionLaunch,
        job_name: &str,
    ) -> Result<(), DatabaseError> {
        self.gate().await?;
        if job_name.is_empty()
            || job_name.len() > 63
            || !job_name
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        {
            return Err(DatabaseError::InvalidInput);
        }
        let id = Uuid::parse_str(&launch.id).map_err(|_| DatabaseError::InvalidInput)?;
        let epoch = i64::try_from(launch.launched_at).map_err(|_| DatabaseError::InvalidInput)?;
        let changed = sqlx::query("UPDATE openlegal.collection_request SET status='running',job_name=$2 WHERE id=$1 AND status='launching' AND launched_at=$3 AND lease_until>floor(extract(epoch from clock_timestamp()))::bigint")
            .bind(id).bind(job_name).bind(epoch).execute(&self.pool).await.map_err(db)?.rows_affected();
        if changed != 1 {
            let row = sqlx::query("SELECT status,job_name FROM openlegal.collection_request WHERE id=$1 AND launched_at=$2")
                .bind(id).bind(epoch).fetch_optional(&self.pool).await.map_err(db)?;
            let Some(row) = row else {
                return Err(DatabaseError::Conflict);
            };
            let status: String = row.try_get("status").map_err(db)?;
            let name: Option<String> = row.try_get("job_name").map_err(db)?;
            // A fast Pod can finish before Kubernetes creation acknowledgement;
            // a repeated acknowledgement of this same running Job is idempotent.
            if !matches!(status.as_str(), "done" | "skipped" | "deferred" | "failed")
                && !(status == "running" && name.as_deref() == Some(job_name))
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

    pub async fn load_collection_launch(
        &self,
        id: &str,
    ) -> Result<CollectionLaunch, DatabaseError> {
        self.gate().await?;
        let id = Uuid::parse_str(id).map_err(|_| DatabaseError::InvalidInput)?;
        let row = sqlx::query("SELECT id,payload,launched_at,operation_timeout_secs,operation_attempt_limit,floor(extract(epoch from clock_timestamp()))::bigint AS observed_at FROM openlegal.collection_request WHERE id=$1 AND status IN ('launching','running') AND lease_until>floor(extract(epoch from clock_timestamp()))::bigint")
            .bind(id).fetch_optional(&self.pool).await.map_err(db)?.ok_or(DatabaseError::NotFound)?;
        launch_from_row(&row)
    }

    pub async fn load_collection_launch_for_epoch(
        &self,
        id: &str,
        expected: u64,
    ) -> Result<CollectionLaunch, DatabaseError> {
        self.gate().await?;
        let id = Uuid::parse_str(id).map_err(|_| DatabaseError::InvalidInput)?;
        let expected = i64::try_from(expected).map_err(|_| DatabaseError::InvalidInput)?;
        let row = sqlx::query("SELECT id,payload,launched_at,operation_timeout_secs,operation_attempt_limit,floor(extract(epoch from clock_timestamp()))::bigint AS observed_at FROM openlegal.collection_request WHERE id=$1 AND launched_at=$2 AND status IN ('launching','running') AND lease_until>floor(extract(epoch from clock_timestamp()))::bigint")
            .bind(id).bind(expected).fetch_optional(&self.pool).await.map_err(db)?.ok_or(DatabaseError::NotFound)?;
        launch_from_row(&row)
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
        validate_settlement_reason(reason)?;
        let id = Uuid::parse_str(id).map_err(|_| DatabaseError::InvalidInput)?;
        let changed = sqlx::query("UPDATE openlegal.collection_request SET status=$2,payload=CASE WHEN $2='deferred' THEN payload ELSE '{}'::jsonb END,lease_until=CASE WHEN $2='deferred' THEN floor(extract(epoch from clock_timestamp()))::bigint+3600 ELSE NULL END,reason=$3,provider_deferral_fingerprint=NULL WHERE id=$1 AND status IN ('launching','running')")
            .bind(id).bind(status).bind(reason).execute(&self.pool).await.map_err(db)?.rows_affected();
        if changed != 1 {
            return Err(DatabaseError::Conflict);
        }
        Ok(())
    }

    /// A request Pod may settle only its own launch. Healthy contention is
    /// rechecked promptly; quota/pause deferral uses the actual admission time.
    pub async fn settle_collection_launch(
        &self,
        launch: &CollectionLaunch,
        status: &str,
        reason: Option<&str>,
        deferred_until: Option<u64>,
    ) -> Result<(), DatabaseError> {
        self.settle_collection_launch_with_fingerprint(launch, status, reason, deferred_until, None)
            .await
    }
    pub async fn settle_collection_launch_with_fingerprint(
        &self,
        launch: &CollectionLaunch,
        status: &str,
        reason: Option<&str>,
        deferred_until: Option<u64>,
        fingerprint: Option<&openlegal_domain::provider_admin::ProviderBlockerFingerprint>,
    ) -> Result<(), DatabaseError> {
        if !matches!(status, "done" | "skipped" | "deferred" | "failed")
            || (status == "deferred") != deferred_until.is_some()
        {
            return Err(DatabaseError::InvalidInput);
        }
        validate_settlement_reason(reason)?;
        if deferred_until.is_some_and(|value| i64::try_from(value).is_err()) {
            return Err(DatabaseError::InvalidInput);
        }
        let id = Uuid::parse_str(&launch.id).map_err(|_| DatabaseError::InvalidInput)?;
        let changed = sqlx::query("UPDATE openlegal.collection_request SET status=$2,payload=CASE WHEN $2='deferred' THEN payload ELSE '{}'::jsonb END,lease_until=$3::text::bigint,reason=$4,provider_deferral_fingerprint=CASE WHEN $2='deferred' THEN $6::jsonb ELSE NULL END WHERE id=$1 AND status IN ('launching','running') AND launched_at=$5::text::bigint AND lease_until>floor(extract(epoch from clock_timestamp()))::bigint")
            .bind(id).bind(status).bind(deferred_until.map(|value| value.to_string()))
            .bind(reason).bind(launch.launched_at.to_string())
            .bind(fingerprint.map(serde_json::to_value).transpose().map_err(corrupt)?)
            .execute(&self.pool).await.map_err(db)?.rows_affected();
        if changed != 1 {
            return Err(DatabaseError::Conflict);
        }
        Ok(())
    }

    pub async fn request_collection(
        &self,
        request: CollectionRequest,
    ) -> Result<CollectionReceipt, DatabaseError> {
        self.request_collection_shared(request, false, &CancellationToken::new())
            .await?
            .receipt
            .ok_or(DatabaseError::StorageCorrupt)
    }

    async fn request_collection_shared(
        &self,
        request: CollectionRequest,
        demand: bool,
        cancel: &CancellationToken,
    ) -> Result<DemandCollectionStatus, DatabaseError> {
        request.validate()?;
        check(cancel)?;
        let request = request.normalized();
        let payload = serde_json::to_value(&request).map_err(corrupt)?;
        let canonical = hex(&bytes_hash(&serde_json::to_vec(&payload).map_err(corrupt)?));
        self.gate().await?;
        let mut tx = begin_storage(&self.pool, "request_collection_shared").await?;
        sqlx::query("SELECT singleton FROM openlegal.corpus_control WHERE singleton FOR UPDATE")
            .fetch_one(&mut *tx)
            .await
            .map_err(db)?;
        check(cancel)?;
        let now: i64 =
            sqlx::query_scalar("SELECT floor(extract(epoch from clock_timestamp()))::bigint")
                .fetch_one(&mut *tx)
                .await
                .map_err(db)?;
        // Publication can race the local lookup; check authoritative HEAD again
        // under admission before recording redundant work.
        if demand && let CollectionTarget::Object { object } = &request.target {
            let row = sqlx::query("SELECT o.withdrawn,o.pending,NOT o.withdrawn AND NOT o.pending AND c.revision_id=o.desired_head_revision AND COALESCE(c.payload->'record'->'metadata'->>'attachment_status','complete') <> 'incomplete' AND o.validated_at<=$2::text::numeric AND o.validated_at>$2::text::numeric-3600 AS fresh FROM openlegal.corpus_object o LEFT JOIN openlegal.corpus_capture c ON c.id=o.head_capture WHERE o.object_key=$1 FOR SHARE OF o")
                .bind(key(object)?).bind(now.to_string()).fetch_optional(&mut *tx).await.map_err(db)?;
            if let Some(row) = row {
                if row.try_get::<bool, _>("withdrawn").map_err(db)? {
                    return Ok(DemandCollectionStatus::reason(
                        DemandState::Unavailable,
                        "withdrawn",
                    ));
                }
                if row.try_get::<Option<bool>, _>("fresh").map_err(db)? == Some(true) {
                    return Ok(DemandCollectionStatus::new(DemandState::Fresh));
                }
            }
        }
        // Both origins share any active canonical request, including deferred
        // requests. Their old terminal receipts remain addressable for 24 hours.
        let active = sqlx::query("SELECT id,status,reason,lease_until,floor(extract(epoch from clock_timestamp()))::bigint AS observed_at FROM openlegal.collection_request WHERE canonical_key=$1 AND status IN ('queued','launching','running','deferred') AND (expires_at>$2 OR status IN ('launching','running')) ORDER BY created_at DESC,id DESC LIMIT 1 FOR UPDATE")
            .bind(&canonical).bind(now).fetch_optional(&mut *tx).await.map_err(db)?;
        if let Some(row) = active {
            let receipt = receipt_from_row(&row)?;
            if !demand {
                sqlx::query("UPDATE openlegal.collection_request SET explicit_until=COALESCE(explicit_until,$2),expires_at=GREATEST(expires_at,COALESCE(explicit_until,$2)) WHERE id=$1")
                    .bind(Uuid::parse_str(&receipt.request_id).map_err(corrupt)?).bind(now+86400)
                    .execute(&mut *tx).await.map_err(db)?;
            }
            check(cancel)?;
            commit_storage(tx, "request_collection_shared").await?;
            return Ok(DemandCollectionStatus {
                status: DemandState::Pending,
                receipt: Some(receipt),
                reason: None,
            });
        }
        let latest = if demand {
            sqlx::query("SELECT id,status,reason,lease_until,completed_at,created_at,floor(extract(epoch from clock_timestamp()))::bigint AS observed_at FROM openlegal.collection_request WHERE canonical_key=$1 AND status IN ('done','skipped','failed') AND expires_at>$2 ORDER BY completed_at DESC NULLS LAST,created_at DESC,id DESC LIMIT 1 FOR UPDATE")
                .bind(&canonical).bind(now).fetch_optional(&mut *tx).await.map_err(db)?
        } else {
            sqlx::query("SELECT id,status,reason,lease_until,completed_at,created_at,floor(extract(epoch from clock_timestamp()))::bigint AS observed_at FROM openlegal.collection_request WHERE canonical_key=$1 AND explicit_until>$2 AND status IN ('done','skipped','failed') ORDER BY created_at DESC,id DESC LIMIT 1 FOR UPDATE")
                .bind(&canonical).bind(now).fetch_optional(&mut *tx).await.map_err(db)?
        };
        if let Some(row) = latest {
            let receipt = receipt_from_row(&row)?;
            let completed = row
                .try_get::<Option<i64>, _>("completed_at")
                .map_err(db)?
                .unwrap_or(row.try_get::<i64, _>("created_at").map_err(db)?);
            let successful = (receipt.status == "done" && receipt.reason.is_none())
                || (receipt.status == "skipped"
                    && matches!(
                        receipt.reason.as_deref(),
                        Some("no_matches" | "already_fresh")
                    ));
            let retain = if demand {
                // A successful collection receipt is not HEAD validation. For
                // example, already_fresh may finish just before HEAD expires.
                // The authoritative recheck above alone determines freshness;
                // only failure cooldowns may defer another object refresh.
                completed.saturating_add(3600) > now
                    && !(successful && matches!(request.target, CollectionTarget::Object { .. }))
            } else {
                (receipt.status == "done" && receipt.reason.is_none())
                    || completed.saturating_add(3600) > now
            };
            if retain {
                let status = if demand
                    && successful
                    && matches!(request.target, CollectionTarget::Search { .. })
                {
                    DemandState::Fresh
                } else {
                    DemandState::Unavailable
                };
                check(cancel)?;
                commit_storage(tx, "request_collection_shared").await?;
                return Ok(DemandCollectionStatus {
                    status,
                    reason: receipt.reason.clone(),
                    receipt: Some(receipt),
                });
            }
        }
        let scheduler_seen: i64 = sqlx::query_scalar(
            "SELECT collection_scheduler_seen_at FROM openlegal.corpus_control WHERE singleton",
        )
        .fetch_one(&mut *tx)
        .await
        .map_err(db)?;
        if scheduler_seen < now.saturating_sub(30) {
            return Err(DatabaseError::Capacity);
        }
        // Deferred work counts toward the bound; otherwise repeated exhaustion
        // could create an unlimited deferred queue alongside the active queue.
        let queued: i64 = sqlx::query_scalar("SELECT count(*) FROM openlegal.collection_request WHERE status IN ('queued','launching','running','deferred')")
            .fetch_one(&mut *tx).await.map_err(db)?;
        if queued >= 128 {
            return Err(DatabaseError::Capacity);
        }
        if let Some(id) = sqlx::query_scalar::<_, Uuid>(
            "SELECT id FROM openlegal.collection_request WHERE request_key=$1 FOR UPDATE",
        )
        .bind(&canonical)
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?
        {
            let retired = hex(&bytes_hash(format!("retired:{canonical}:{id}").as_bytes()));
            sqlx::query("UPDATE openlegal.collection_request SET request_key=$2 WHERE id=$1")
                .bind(id)
                .bind(retired)
                .execute(&mut *tx)
                .await
                .map_err(db)?;
        }
        check(cancel)?;
        let id: Uuid = sqlx::query_scalar("INSERT INTO openlegal.collection_request(request_key,canonical_key,payload,status,created_at,expires_at,explicit_until) VALUES($1,$1,$2,'queued',$3,$4,$5) RETURNING id")
            .bind(&canonical).bind(payload).bind(now).bind(now+86400)
            .bind((!demand).then_some(now+86400)).fetch_one(&mut *tx).await.map_err(db)?;
        check(cancel)?;
        commit_storage(tx, "request_collection_shared").await?;
        Ok(DemandCollectionStatus {
            status: DemandState::Pending,
            receipt: Some(CollectionReceipt {
                request_id: id.to_string(),
                status: "queued".into(),
                retry_after_seconds: 10,
                reason: None,
            }),
            reason: None,
        })
    }

    pub async fn collection_status(&self, id: &str) -> Result<CollectionReceipt, DatabaseError> {
        self.gate().await?;
        let id = Uuid::parse_str(id).map_err(|_| DatabaseError::InvalidInput)?;
        let row = sqlx::query("SELECT id,status,reason,lease_until,floor(extract(epoch from clock_timestamp()))::bigint AS observed_at FROM openlegal.collection_request WHERE id=$1")
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .map_err(db)?
            .ok_or(DatabaseError::NotFound)?;
        receipt_from_row(&row)
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

fn receipt_from_row(row: &PgRow) -> Result<CollectionReceipt, DatabaseError> {
    let status: String = row.try_get("status").map_err(db)?;
    let retry_after_seconds = receipt_retry_after(
        &status,
        row.try_get("lease_until").map_err(db)?,
        row.try_get("observed_at").map_err(db)?,
    )?;
    Ok(CollectionReceipt {
        request_id: row.try_get::<Uuid, _>("id").map_err(db)?.to_string(),
        status,
        retry_after_seconds,
        reason: row.try_get("reason").map_err(db)?,
    })
}

fn receipt_retry_after(
    status: &str,
    lease_until: Option<i64>,
    observed_at: i64,
) -> Result<u32, DatabaseError> {
    if status == "deferred" {
        let until = lease_until.ok_or(DatabaseError::StorageCorrupt)?;
        Ok(u32::try_from(until.saturating_sub(observed_at).max(0)).unwrap_or(u32::MAX))
    } else if matches!(status, "queued" | "launching" | "running") {
        Ok(10)
    } else {
        Ok(0)
    }
}

fn validate_settlement_reason(reason: Option<&str>) -> Result<(), DatabaseError> {
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
                | "collection_pending"
                | "already_fresh"
                | "collection_already_in_progress"
                | "head_observation_superseded"
                | "publication_superseded"
                | "no_matches"
                | "multiple_skip_reasons"
                | "provider_daily_limit"
                | "provider_suspended"
                | "provider_response_uncertain"
                | "provider_retry_after"
                | "operation_attempt_limit"
                | "provider_admission_wait"
                | "capacity_wait"
                | "provider_recovery_hold"
        )
    }) {
        return Err(DatabaseError::InvalidInput);
    }
    Ok(())
}

impl DemandCollectionStore for PgCorpusStore {
    fn request_demand(
        &self,
        request: CollectionRequest,
        cancel: CancellationToken,
    ) -> BoxFuture<'static, Result<DemandCollectionStatus, DatabaseError>> {
        let store = self.clone();
        Box::pin(async move {
            store
                .request_collection_shared(request, true, &cancel)
                .await
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[ignore = "requires scripts/test-postgres.sh"]
    async fn deferred_status_and_coalesced_receipt_share_reason_and_actual_lease() {
        use openlegal_application::persistence::PersistentStore;
        let fixture = crate::test_support::TestDatabase::new().await;
        let base = fixture.open(100).await;
        let pool = base.pool();
        let blobs =
            crate::blob::FsBlobStore::open(&fixture.directory.path().join("deferral-receipts"))
                .await
                .unwrap();
        let store = PgCorpusStore::new(pool.clone(), blobs);
        store.heartbeat_collection_scheduler().await.unwrap();
        let request = CollectionRequest {
            target: CollectionTarget::Object {
                object: ObjectId {
                    jurisdiction: "kr".into(),
                    provider: "law_go_kr".into(),
                    dataset: Dataset::NationalStatute,
                    id: "001".into(),
                },
            },
        };
        let requested = store.request_collection(request.clone()).await.unwrap();
        let launch = store
            .claim_collection_request_with_policy(7200, RequestLimit::Limited(32))
            .await
            .unwrap()
            .unwrap();
        let until: i64 =
            sqlx::query_scalar("SELECT floor(extract(epoch from clock_timestamp()))::bigint+120")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(
            store
                .settle_collection_launch(
                    &launch,
                    "deferred",
                    Some("untrusted-owner-detail"),
                    Some(until as u64)
                )
                .await,
            Err(DatabaseError::InvalidInput)
        );
        assert_eq!(
            store
                .settle_collection_launch(&launch, "deferred", None, Some(u64::MAX))
                .await,
            Err(DatabaseError::InvalidInput)
        );
        store
            .settle_collection_launch(
                &launch,
                "deferred",
                Some("provider_retry_after"),
                Some(until as u64),
            )
            .await
            .unwrap();
        let status = store
            .collection_status(&requested.request_id)
            .await
            .unwrap();
        assert_eq!(status.reason.as_deref(), Some("provider_retry_after"));
        assert!((115..=120).contains(&status.retry_after_seconds));
        let coalesced = store.request_collection(request).await.unwrap();
        assert_eq!(coalesced.request_id, requested.request_id);
        assert_eq!(coalesced.reason, status.reason);
        assert!((115..=120).contains(&coalesced.retry_after_seconds));
        sqlx::query("UPDATE openlegal.collection_request SET lease_until=floor(extract(epoch from clock_timestamp()))::bigint-1 WHERE id=$1::uuid")
            .bind(&requested.request_id).execute(&pool).await.unwrap();
        assert_eq!(
            store
                .collection_status(&requested.request_id)
                .await
                .unwrap()
                .retry_after_seconds,
            0
        );
        base.close().await.unwrap();
    }

    #[tokio::test]
    #[ignore = "requires scripts/test-postgres.sh"]
    async fn deferral_reason_migration_preserves_legacy_receipts_and_rejects_raw_details() {
        use openlegal_application::persistence::PersistentStore;
        let fixture = crate::test_support::TestDatabase::new().await;
        let base = fixture.open(100).await;
        let pool = base.pool();
        sqlx::raw_sql(include_str!(
            "../../migrations/0011_collection_skip_reasons.sql"
        ))
        .execute(&pool)
        .await
        .unwrap();
        let legacy_null: Uuid = sqlx::query_scalar("INSERT INTO openlegal.collection_request(request_key,canonical_key,payload,status,created_at,expires_at) VALUES(repeat('0',64),repeat('0',64),'{}','done',100,200) RETURNING id")
            .fetch_one(&pool).await.unwrap();
        let legacy_reason: Uuid = sqlx::query_scalar("INSERT INTO openlegal.collection_request(request_key,canonical_key,payload,status,reason,created_at,expires_at) VALUES(repeat('1',64),repeat('1',64),'{}','failed','source_inventory_incomplete',100,200) RETURNING id")
            .fetch_one(&pool).await.unwrap();
        sqlx::raw_sql(include_str!(
            "../../migrations/0022_collection_deferral_reasons.sql"
        ))
        .execute(&pool)
        .await
        .unwrap();
        let null: Option<String> =
            sqlx::query_scalar("SELECT reason FROM openlegal.collection_request WHERE id=$1")
                .bind(legacy_null)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(null, None);
        let reason: Option<String> =
            sqlx::query_scalar("SELECT reason FROM openlegal.collection_request WHERE id=$1")
                .bind(legacy_reason)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(reason.as_deref(), Some("source_inventory_incomplete"));
        for reason in [
            "provider_response_uncertain",
            "provider_suspended",
            "provider_daily_limit",
            "provider_retry_after",
            "operation_attempt_limit",
            "provider_admission_wait",
            "capacity_wait",
        ] {
            sqlx::query("UPDATE openlegal.collection_request SET reason=$2 WHERE id=$1")
                .bind(legacy_null)
                .bind(reason)
                .execute(&pool)
                .await
                .unwrap();
        }
        assert!(
            sqlx::query(
                "UPDATE openlegal.collection_request SET reason='raw-owner-secret' WHERE id=$1"
            )
            .bind(legacy_null)
            .execute(&pool)
            .await
            .is_err()
        );
        base.close().await.unwrap();
    }

    #[test]
    fn deferred_receipt_uses_remaining_lease_and_bounds_old_or_long_leases() {
        assert_eq!(receipt_retry_after("deferred", Some(105), 100), Ok(5));
        assert_eq!(receipt_retry_after("deferred", Some(100), 105), Ok(0));
        assert_eq!(
            receipt_retry_after("deferred", Some(i64::MAX), 100),
            Ok(u32::MAX)
        );
        assert_eq!(
            receipt_retry_after("deferred", None, 100),
            Err(DatabaseError::StorageCorrupt)
        );
        assert_eq!(receipt_retry_after("running", Some(7200), 100), Ok(10));
        assert_eq!(receipt_retry_after("done", None, 100), Ok(0));
    }

    #[test]
    fn settlement_reasons_reject_untrusted_details() {
        for reason in [
            "provider_daily_limit",
            "provider_suspended",
            "provider_response_uncertain",
            "provider_retry_after",
            "operation_attempt_limit",
            "provider_admission_wait",
            "capacity_wait",
        ] {
            assert_eq!(validate_settlement_reason(Some(reason)), Ok(()));
        }
        assert_eq!(
            validate_settlement_reason(Some("internal-owner-secret")),
            Err(DatabaseError::InvalidInput)
        );
    }
}
