use super::*;

impl PgCorpusStore {
    /// Acknowledge only after the durable index generation has incorporated every
    /// event through this sequence. Gaps are not valid acknowledgements.
    pub async fn acknowledge_index(&self, generation: u64) -> Result<(), DatabaseError> {
        let retry_cancel = CancellationToken::new();
        retry_storage(&retry_cancel, "acknowledge_index", || {
            async {

        let generation = i64::try_from(generation).map_err(|_| DatabaseError::InvalidInput)?;
        let count=sqlx::query("UPDATE openlegal.corpus_control SET index_ack=$1 WHERE index_ack<=$1 AND next_event>$1").bind(generation).execute(&self.pool).await.map_err(db)?.rows_affected();
        if count == 0 {
            return Err(DatabaseError::Conflict);
        }
        Ok(())

            }
        }).await
    }
    pub async fn enqueue(
        &self,
        object: ObjectId,
        revision_id: String,
        now: u64,
    ) -> Result<Job, DatabaseError> {
        self.enqueue_job(object, revision_id, None, true, true, now)
            .await
    }
    pub async fn enqueue_with_policy(
        &self,
        object: ObjectId,
        revision_id: String,
        now: u64,
        mark_head_pending: bool,
    ) -> Result<Job, DatabaseError> {
        self.enqueue_job(object, revision_id, None, true, mark_head_pending, now)
            .await
    }
    /// Retains the exact provider request descriptor for restart-safe execution.
    pub async fn enqueue_job(
        &self,
        object: ObjectId,
        revision_id: String,
        effective_date: Option<String>,
        install_head: bool,
        mark_head_pending: bool,
        now: u64,
    ) -> Result<Job, DatabaseError> {
        self.enqueue_job_with_metadata(
            object,
            revision_id,
            effective_date,
            install_head,
            mark_head_pending,
            now,
            Default::default(),
        )
        .await
    }
    #[allow(clippy::too_many_arguments)]
    pub async fn enqueue_job_with_metadata(
        &self,
        object: ObjectId,
        revision_id: String,
        effective_date: Option<String>,
        install_head: bool,
        mark_head_pending: bool,
        now: u64,
        source_metadata: std::collections::BTreeMap<String, String>,
    ) -> Result<Job, DatabaseError> {
        self.enqueue_job_with_metadata_fenced(
            object,
            revision_id,
            effective_date,
            install_head,
            mark_head_pending,
            now,
            source_metadata,
            None,
        )
        .await
    }
    #[allow(clippy::too_many_arguments)]
    pub async fn enqueue_job_with_metadata_fenced(
        &self,
        object: ObjectId,
        revision_id: String,
        effective_date: Option<String>,
        install_head: bool,
        mark_head_pending: bool,
        now: u64,
        source_metadata: std::collections::BTreeMap<String, String>,
        observed_version: Option<u64>,
    ) -> Result<Job, DatabaseError> {
        self.enqueue_job_with_owner(
            object,
            revision_id,
            effective_date,
            install_head,
            mark_head_pending,
            now,
            source_metadata,
            observed_version,
            None,
        )
        .await
    }
    #[allow(clippy::too_many_arguments)]
    pub async fn enqueue_job_for_collection_request(
        &self,
        object: ObjectId,
        revision_id: String,
        effective_date: Option<String>,
        install_head: bool,
        mark_head_pending: bool,
        now: u64,
        source_metadata: std::collections::BTreeMap<String, String>,
        observed_version: Option<u64>,
        launch: &CollectionLaunch,
    ) -> Result<Job, DatabaseError> {
        self.enqueue_job_with_owner(
            object,
            revision_id,
            effective_date,
            install_head,
            mark_head_pending,
            now,
            source_metadata,
            observed_version,
            Some(launch),
        )
        .await
    }
    /// A read-only hint for the exact HEAD job. Adoption rechecks these facts
    /// under the claim lock; this hint alone never authorizes a provider call.
    pub async fn background_budget_wait_available(
        &self,
        object: &ObjectId,
        revision_id: &str,
        effective_date: Option<&str>,
        observed_version: u64,
    ) -> Result<bool, DatabaseError> {
        self.gate().await?;
        let k = key(object)?;
        let version = i64::try_from(observed_version).map_err(|_| DatabaseError::InvalidInput)?;
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM openlegal.corpus_job j JOIN openlegal.corpus_object o USING(object_key) WHERE j.object_key=$1 AND j.revision_id=$2 AND j.effective_date=$3 AND j.install_head AND j.status='running' AND j.error_category='budget_wait' AND j.expected_version=$4 AND o.version=$4 AND o.pending AND NOT o.withdrawn AND o.desired_head_revision=$2 AND j.explicit_request_id IS NULL AND COALESCE(j.source_metadata->>'collection_origin','')<>'explicit' AND j.attempts<(SELECT max_job_attempts FROM openlegal.provider_request_budget WHERE singleton))")
            .bind(k).bind(revision_id).bind(effective_date.unwrap_or(""))
            .bind(version).fetch_one(&self.pool).await.map_err(db)
    }
    /// Move a completed background admission wait to a live explicit request.
    /// Execution attempts are preserved; version advancement fences every late
    /// settlement/publication by the original background claimant.
    #[allow(clippy::too_many_arguments)]
    pub async fn adopt_background_budget_wait(
        &self,
        object: ObjectId,
        revision_id: String,
        effective_date: Option<String>,
        observed_version: u64,
        mut source_metadata: std::collections::BTreeMap<String, String>,
        launch: &CollectionLaunch,
        now: u64,
    ) -> Result<Option<Job>, DatabaseError> {
        source_metadata.insert("collection_origin".into(), "explicit".into());
        if source_metadata.len() > 128
            || source_metadata
                .iter()
                .map(|(k, v)| k.len() + v.len())
                .sum::<usize>()
                > 65536
            || effective_date
                .as_deref()
                .is_some_and(|date| !valid_date(date))
        {
            return Err(DatabaseError::InvalidInput);
        }
        RevisionSelector::Revision {
            id: revision_id.clone(),
        }
        .validate()?;
        self.gate().await?;
        let k = key(&object)?;
        let version = i64::try_from(observed_version).map_err(|_| DatabaseError::InvalidInput)?;
        let next_version = version
            .checked_add(1)
            .ok_or(DatabaseError::StorageCorrupt)?;
        let owner = Uuid::parse_str(&launch.id).map_err(|_| DatabaseError::InvalidInput)?;
        let recovery =
            i64::try_from(launch.recovery_at()).map_err(|_| DatabaseError::InvalidInput)?;
        let launched_at =
            i64::try_from(launch.launched_at).map_err(|_| DatabaseError::InvalidInput)?;
        let mut tx = self.pool.begin().await.map_err(db)?;
        sqlx::query("SELECT singleton FROM openlegal.corpus_control WHERE singleton FOR UPDATE")
            .fetch_one(&mut *tx)
            .await
            .map_err(db)?;
        let current: Option<i64> = sqlx::query_scalar("SELECT version FROM openlegal.corpus_object WHERE object_key=$1 AND pending AND NOT withdrawn AND desired_head_revision=$2 FOR UPDATE")
            .bind(&k).bind(&revision_id).fetch_optional(&mut *tx).await.map_err(db)?;
        if current != Some(version) {
            return Ok(None);
        }
        let candidate = sqlx::query("SELECT j.id,j.attempts FROM openlegal.corpus_job j WHERE j.object_key=$1 AND j.revision_id=$2 AND j.effective_date=$3 AND j.install_head AND j.status='running' AND j.error_category='budget_wait' AND j.expected_version=$4 AND j.explicit_request_id IS NULL AND COALESCE(j.source_metadata->>'collection_origin','')<>'explicit' AND j.attempts<(SELECT max_job_attempts FROM openlegal.provider_request_budget WHERE singleton) AND NOT EXISTS(SELECT 1 FROM openlegal.corpus_job active WHERE active.object_key=j.object_key AND active.id<>j.id AND active.status='running' AND active.lease_until>$5::text::numeric) AND EXISTS(SELECT 1 FROM openlegal.collection_request r WHERE r.id=$6 AND r.launched_at=$7 AND r.status IN ('launching','running') AND r.lease_until>$5::bigint) FOR UPDATE OF j")
            .bind(&k).bind(&revision_id).bind(effective_date.as_deref().unwrap_or(""))
            .bind(version).bind(now.to_string()).bind(owner).bind(launched_at).fetch_optional(&mut *tx).await.map_err(db)?;
        let Some(candidate) = candidate else {
            return Ok(None);
        };
        let id: Uuid = candidate.try_get("id").map_err(db)?;
        let attempts: i32 = candidate.try_get("attempts").map_err(db)?;
        sqlx::query("UPDATE openlegal.corpus_object SET version=$2 WHERE object_key=$1")
            .bind(&k)
            .bind(next_version)
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        sqlx::query("UPDATE openlegal.corpus_job SET status='pending',expected_version=$2,source_metadata=$3,explicit_request_id=$4,explicit_recovery_at=$5,lease_until=NULL,error_category=NULL,started_at=NULL,completed_at=NULL WHERE id=$1")
            .bind(id).bind(next_version).bind(serde_json::to_value(&source_metadata).map_err(corrupt)?)
            .bind(owner).bind(recovery).execute(&mut *tx).await.map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(Some(Job {
            id: id.to_string(),
            object,
            revision_id,
            effective_date,
            install_head: true,
            source_metadata,
            expected_version: next_version.try_into().map_err(corrupt)?,
            attempts: attempts.try_into().map_err(corrupt)?,
        }))
    }
    #[allow(clippy::too_many_arguments)]
    async fn enqueue_job_with_owner(
        &self,
        object: ObjectId,
        revision_id: String,
        effective_date: Option<String>,
        install_head: bool,
        mark_head_pending: bool,
        now: u64,
        source_metadata: std::collections::BTreeMap<String, String>,
        observed_version: Option<u64>,
        launch: Option<&CollectionLaunch>,
    ) -> Result<Job, DatabaseError> {
        let retry_cancel = CancellationToken::new();
        retry_storage(&retry_cancel, "enqueue_job_with_owner", || {
            let object = object.clone();
            let revision_id = revision_id.clone();
            let effective_date = effective_date.clone();
            let source_metadata = source_metadata.clone();
            async move {

        let owner = launch
            .map(|value| Uuid::parse_str(&value.id).map_err(|_| DatabaseError::InvalidInput))
            .transpose()?;
        let recovery_at = launch
            .map(|value| {
                i64::try_from(value.recovery_at()).map_err(|_| DatabaseError::InvalidInput)
            })
            .transpose()?;
        if source_metadata.len() > 128
            || source_metadata
                .iter()
                .map(|(k, v)| k.len() + v.len())
                .sum::<usize>()
                > 65536
        {
            return Err(DatabaseError::InvalidInput);
        }
        self.gate().await?;
        let k = key(&object)?;
        RevisionSelector::Revision {
            id: revision_id.clone(),
        }
        .validate()?;
        if effective_date.as_ref().is_some_and(|d| !valid_date(d)) {
            return Err(DatabaseError::InvalidInput);
        }
        let date = effective_date.as_deref().unwrap_or("");
        let mut tx = self.pool.begin().await.map_err(db)?;
        sqlx::query("SELECT singleton FROM openlegal.corpus_control WHERE singleton FOR UPDATE")
            .fetch_one(&mut *tx)
            .await
            .map_err(db)?;
        if let Some(launch) = launch {
            let authorized: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM openlegal.collection_request WHERE id=$1 AND launched_at=$2 AND status IN ('launching','running') AND lease_until>$3 FOR SHARE)")
                .bind(owner).bind(i64::try_from(launch.launched_at).map_err(|_|DatabaseError::InvalidInput)?)
                .bind(i64::try_from(now).map_err(|_|DatabaseError::InvalidInput)?)
                .fetch_one(&mut *tx).await.map_err(db)?;
            if !authorized {
                return Err(DatabaseError::Cancelled);
            }
        }
        sqlx::query("INSERT INTO openlegal.corpus_object(object_key,identity) VALUES($1,$2) ON CONFLICT DO NOTHING").bind(&k).bind(serde_json::to_value(&object).map_err(corrupt)?).execute(&mut *tx).await.map_err(db)?;
        let row=sqlx::query("SELECT identity,version,withdrawn,desired_head_revision,pending FROM openlegal.corpus_object WHERE object_key=$1 FOR UPDATE").bind(&k).fetch_one(&mut *tx).await.map_err(db)?;
        if serde_json::from_value::<ObjectId>(row.try_get("identity").map_err(db)?)
            .map_err(corrupt)?
            != object
        {
            return Err(DatabaseError::StorageCorrupt);
        }
        if row.try_get::<bool, _>("withdrawn").map_err(db)? {
            return Err(DatabaseError::Withdrawn);
        }
        if observed_version.is_some_and(|expected| {
            row.try_get::<i64, _>("version")
                .ok()
                .and_then(|v| u64::try_from(v).ok())
                != Some(expected)
        }) {
            return Err(DatabaseError::Conflict);
        }
        let head_changed = install_head
            && (row
                .try_get::<Option<String>, _>("desired_head_revision")
                .map_err(db)?
                .as_deref()
                != Some(revision_id.as_str())
                || (mark_head_pending && !row.try_get::<bool, _>("pending").map_err(db)?));
        let version = row.try_get::<i64, _>("version").map_err(db)? + i64::from(head_changed);
        if install_head {
            sqlx::query("UPDATE openlegal.corpus_job SET status='failed',lease_until=NULL,error_category='superseded',completed_at=floor(extract(epoch from clock_timestamp()))::bigint WHERE object_key=$1 AND install_head AND revision_id<>$2 AND status IN ('pending','running')").bind(&k).bind(&revision_id).execute(&mut *tx).await.map_err(db)?;
            sqlx::query("UPDATE openlegal.corpus_object SET desired_head_revision=$2,pending=pending OR $3,version=$4 WHERE object_key=$1").bind(&k).bind(&revision_id).bind(mark_head_pending).bind(version).execute(&mut *tx).await.map_err(db)?;
        }
        if let Some(job)=sqlx::query("SELECT id,expected_version,attempts,install_head,source_metadata FROM openlegal.corpus_job WHERE object_key=$1 AND revision_id=$2 AND effective_date=$3 AND status IN ('pending','running')").bind(&k).bind(&revision_id).bind(date).fetch_optional(&mut *tx).await.map_err(db)? {
            let id:Uuid=job.try_get("id").map_err(db)?;
            let old_head:bool=job.try_get("install_head").map_err(db)?;
            if install_head && !old_head {
                // A claimed manual revision hint may still be using its old
                // metadata. Fence that attempt and queue a fresh list-backed
                // HEAD job with the current observation's metadata.
                sqlx::query("UPDATE openlegal.corpus_job SET install_head=true,source_metadata=CASE WHEN explicit_request_id IS NOT NULL THEN jsonb_set($2::jsonb,'{collection_origin}','\"explicit\"'::jsonb) ELSE $2 END,expected_version=$3,status='pending',attempts=0,created_at=$4::text::numeric,lease_until=NULL,error_category=NULL,started_at=NULL,completed_at=NULL WHERE id=$1")
                    .bind(id).bind(serde_json::to_value(&source_metadata).map_err(corrupt)?).bind(version).bind(now.to_string()).execute(&mut *tx).await.map_err(db)?;
                tx.commit().await.map_err(db)?;
                return Ok(Job{source_metadata,id:id.to_string(),object,revision_id,effective_date,install_head:true,expected_version:version.try_into().map_err(corrupt)?,attempts:0});
            }
            let head=install_head||old_head;
            sqlx::query("UPDATE openlegal.corpus_object SET pending=pending OR $2 WHERE object_key=$1").bind(&k).bind(mark_head_pending).execute(&mut *tx).await.map_err(db)?;
            tx.commit().await.map_err(db)?;
            return Ok(Job{source_metadata:serde_json::from_value(job.try_get("source_metadata").map_err(db)?).map_err(corrupt)?,id:id.to_string(),object,revision_id,effective_date,install_head:head,expected_version:job.try_get::<i64,_>("expected_version").map_err(db)?.try_into().map_err(corrupt)?,attempts:job.try_get::<i32,_>("attempts").map_err(db)?.try_into().map_err(corrupt)?});
        }
        let queued: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM openlegal.corpus_job WHERE status IN ('pending','running')",
        )
        .fetch_one(&mut *tx)
        .await
        .map_err(db)?;
        if queued >= 128 {
            // A saturated work queue cannot erase an already observed replacement.
            // Persist the HEAD fence and supersession even though no job fits.
            tx.commit().await.map_err(db)?;
            return Err(DatabaseError::Capacity);
        }
        let job:Uuid=sqlx::query_scalar("INSERT INTO openlegal.corpus_job(object_key,revision_id,effective_date,install_head,expected_version,status,created_at,source_metadata,explicit_request_id,explicit_recovery_at) VALUES($1,$2,$3,$4,$5,'pending',$6::text::numeric,$7,$8,$9) ON CONFLICT(object_key,revision_id,effective_date) DO UPDATE SET source_metadata=EXCLUDED.source_metadata,expected_version=EXCLUDED.expected_version,install_head=EXCLUDED.install_head,status='pending',attempts=0,created_at=EXCLUDED.created_at,lease_until=NULL,error_category=NULL,started_at=NULL,completed_at=NULL,explicit_request_id=EXCLUDED.explicit_request_id,explicit_recovery_at=EXCLUDED.explicit_recovery_at RETURNING id").bind(&k).bind(&revision_id).bind(date).bind(install_head).bind(version).bind(now.to_string()).bind(serde_json::to_value(&source_metadata).map_err(corrupt)?).bind(owner).bind(recovery_at).fetch_one(&mut *tx).await.map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(Job {
            source_metadata,
            id: job.to_string(),
            object,
            revision_id,
            effective_date,
            install_head,
            expected_version: version.try_into().map_err(corrupt)?,
            attempts: 0,
        })

            }
        }).await
    }
    /// A dead worker is retried at most three times. The lease must cover the
    /// configured detail deadline plus validation and publication.
    pub async fn claim_job(&self, now: u64) -> Result<Option<Job>, DatabaseError> {
        self.claim_job_with_lease(now, SESSION_SECONDS).await
    }
    pub async fn claim_job_with_lease(
        &self,
        now: u64,
        lease_seconds: u64,
    ) -> Result<Option<Job>, DatabaseError> {
        self.claim_job_inner(now, lease_seconds, None, None, None)
            .await
    }
    pub async fn claim_job_with_lease_for_dataset(
        &self,
        now: u64,
        lease_seconds: u64,
        dataset: Dataset,
    ) -> Result<Option<Job>, DatabaseError> {
        self.claim_job_inner(now, lease_seconds, None, Some(dataset), None)
            .await
    }
    pub async fn claim_explicit_job(
        &self,
        id: &str,
        now: u64,
        lease_seconds: u64,
    ) -> Result<Option<Job>, DatabaseError> {
        let id = Uuid::parse_str(id).map_err(|_| DatabaseError::InvalidInput)?;
        self.claim_job_inner(now, lease_seconds, Some(id), None, None)
            .await
    }
    /// Bind an unclaimed job to its original request deadline. Joining work never renews it.
    pub async fn adopt_explicit_job(
        &self,
        id: &str,
        request_id: &str,
        recovery_at: u64,
        now: u64,
    ) -> Result<bool, DatabaseError> {
        let id = Uuid::parse_str(id).map_err(|_| DatabaseError::InvalidInput)?;
        let owner = Uuid::parse_str(request_id).map_err(|_| DatabaseError::InvalidInput)?;
        let recovery_at = i64::try_from(recovery_at).map_err(|_| DatabaseError::InvalidInput)?;
        let mut tx = self.pool.begin().await.map_err(db)?;
        sqlx::query("SELECT singleton FROM openlegal.corpus_control WHERE singleton FOR UPDATE")
            .fetch_one(&mut *tx)
            .await
            .map_err(db)?;
        let changed = sqlx::query("UPDATE openlegal.corpus_job j SET explicit_request_id=$2,explicit_recovery_at=CASE WHEN explicit_request_id=$2 THEN explicit_recovery_at ELSE $3 END,source_metadata=jsonb_set(source_metadata,'{collection_origin}','\"explicit\"'::jsonb) WHERE j.id=$1 AND (j.status='pending' OR (j.status='running' AND j.lease_until<=$4::text::numeric)) AND NOT EXISTS(SELECT 1 FROM openlegal.corpus_job active WHERE active.object_key=j.object_key AND active.id<>j.id AND active.status='running' AND active.lease_until>$4::text::numeric) AND EXISTS(SELECT 1 FROM openlegal.collection_request r WHERE r.id=$2 AND r.status IN ('launching','running') AND r.lease_until>$4::bigint) AND (j.explicit_request_id=$2 OR (j.explicit_request_id IS NULL AND (j.explicit_recovery_at IS NULL OR j.explicit_recovery_at<=$4::bigint)) OR (j.explicit_request_id IS NOT NULL AND j.explicit_recovery_at<=$4::bigint AND NOT EXISTS(SELECT 1 FROM openlegal.collection_request r WHERE r.id=j.explicit_request_id AND r.status IN ('launching','running') AND r.lease_until>$4::bigint)))")
            .bind(id).bind(owner).bind(recovery_at).bind(now.to_string()).execute(&mut *tx).await.map_err(db)?.rows_affected();
        tx.commit().await.map_err(db)?;
        Ok(changed == 1)
    }
    pub async fn claim_explicit_job_for_request(
        &self,
        id: &str,
        request_id: &str,
        now: u64,
        lease_seconds: u64,
    ) -> Result<Option<Job>, DatabaseError> {
        let id = Uuid::parse_str(id).map_err(|_| DatabaseError::InvalidInput)?;
        let owner = Uuid::parse_str(request_id).map_err(|_| DatabaseError::InvalidInput)?;
        self.claim_job_inner(now, lease_seconds, Some(id), None, Some(owner))
            .await
    }
    pub async fn release_unclaimed_explicit_job_for_request(
        &self,
        id: &str,
        request_id: &str,
    ) -> Result<(), DatabaseError> {
        let id = Uuid::parse_str(id).map_err(|_| DatabaseError::InvalidInput)?;
        let owner = Uuid::parse_str(request_id).map_err(|_| DatabaseError::InvalidInput)?;
        sqlx::query("UPDATE openlegal.corpus_job SET source_metadata=source_metadata-'collection_origin',explicit_request_id=NULL,explicit_recovery_at=NULL WHERE id=$1 AND explicit_request_id=$2 AND status='pending'")
            .bind(id).bind(owner).execute(&self.pool).await.map_err(db)?;
        Ok(())
    }
    pub async fn release_unclaimed_explicit_job(&self, id: &str) -> Result<(), DatabaseError> {
        let id = Uuid::parse_str(id).map_err(|_| DatabaseError::InvalidInput)?;
        sqlx::query("UPDATE openlegal.corpus_job SET source_metadata=source_metadata - 'collection_origin' WHERE id=$1 AND explicit_request_id IS NULL AND status='pending' AND source_metadata->>'collection_origin'='explicit'")
            .bind(id).execute(&self.pool).await.map_err(db)?;
        Ok(())
    }
    async fn claim_job_inner(
        &self,
        now: u64,
        lease_seconds: u64,
        explicit_id: Option<Uuid>,
        preferred_dataset: Option<Dataset>,
        explicit_owner: Option<Uuid>,
    ) -> Result<Option<Job>, DatabaseError> {
        let retry_cancel = CancellationToken::new();
        let retry_started = tokio::time::Instant::now();
        retry_storage(&retry_cancel, "claim_job_inner", || {
            async {
                let now = now.saturating_add(retry_started.elapsed().as_secs());

        if !(SESSION_SECONDS..=7320).contains(&lease_seconds) {
            return Err(DatabaseError::InvalidInput);
        }
        self.gate().await?;
        let mut tx = self.pool.begin().await.map_err(db)?;
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

        sqlx::query("UPDATE openlegal.corpus_job SET status='failed',error_category='attempts_exhausted',completed_at=$1::text::numeric WHERE attempts>=(SELECT max_job_attempts FROM openlegal.provider_request_budget WHERE singleton) AND (status='pending' OR (status='running' AND lease_until<=$1::text::numeric))").bind(now.to_string()).execute(&mut *tx).await.map_err(db)?;
        let dataset_name: Option<String> = preferred_dataset
            .map(|dataset| serde_json::to_value(dataset).map_err(corrupt))
            .transpose()?
            .map(|value| {
                value
                    .as_str()
                    .ok_or(DatabaseError::StorageCorrupt)
                    .map(str::to_owned)
            })
            .transpose()?;
        let row=sqlx::query("SELECT j.id,j.revision_id,j.expected_version,j.attempts,j.effective_date,j.install_head,j.source_metadata,o.identity,o.version AS object_version,j.object_key FROM openlegal.corpus_job j JOIN openlegal.corpus_object o USING(object_key) WHERE NOT o.withdrawn AND (NOT j.install_head OR j.revision_id=o.desired_head_revision) AND (($2::uuid IS NULL AND j.explicit_request_id IS NULL AND COALESCE(j.source_metadata->>'collection_origin','')<>'explicit') OR (j.id=$2 AND j.source_metadata->>'collection_origin'='explicit')) AND ($3::text IS NULL OR o.identity->>'dataset'=$3) AND (($4::uuid IS NULL AND j.explicit_request_id IS NULL) OR (j.explicit_request_id=$4 AND EXISTS(SELECT 1 FROM openlegal.collection_request owner WHERE owner.id=$4 AND owner.status IN ('launching','running') AND owner.lease_until>$1::bigint))) AND NOT EXISTS(SELECT 1 FROM openlegal.corpus_job active WHERE active.object_key=j.object_key AND active.id<>j.id AND active.status='running' AND active.lease_until>$1::text::numeric) AND j.attempts<(SELECT max_job_attempts FROM openlegal.provider_request_budget WHERE singleton) AND (j.status='pending' OR (j.status='running' AND j.lease_until<=$1::text::numeric)) ORDER BY j.created_at,j.id LIMIT 1 FOR UPDATE OF j SKIP LOCKED").bind(now.to_string()).bind(explicit_id).bind(dataset_name).bind(explicit_owner).fetch_optional(&mut *tx).await.map_err(db)?;
        let Some(row) = row else {
            tx.commit().await.map_err(db)?;
            return Ok(None);
        };
        let id: Uuid = row.try_get("id").map_err(db)?;
        sqlx::query("UPDATE openlegal.corpus_job SET status='running',attempts=attempts+1,lease_until=$2::text::numeric,expected_version=$3,started_at=$4::text::numeric,completed_at=NULL,error_category=NULL WHERE id=$1").bind(id).bind(now.saturating_add(lease_seconds).to_string()).bind(row.try_get::<i64,_>("object_version").map_err(db)?+1).bind(now.to_string()).execute(&mut *tx).await.map_err(db)?;
        sqlx::query("UPDATE openlegal.corpus_object SET version=version+1 WHERE object_key=$1")
            .bind(row.try_get::<String, _>("object_key").map_err(db)?)
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        let date: String = row.try_get("effective_date").map_err(db)?;
        let job = Job {
            source_metadata: serde_json::from_value(row.try_get("source_metadata").map_err(db)?)
                .map_err(corrupt)?,
            effective_date: if date.is_empty() { None } else { Some(date) },
            install_head: row.try_get("install_head").map_err(db)?,
            id: id.to_string(),
            object: serde_json::from_value(row.try_get("identity").map_err(db)?)
                .map_err(corrupt)?,
            revision_id: row.try_get("revision_id").map_err(db)?,
            expected_version: (row.try_get::<i64, _>("object_version").map_err(db)? + 1)
                .try_into()
                .map_err(corrupt)?,
            attempts: (row.try_get::<i32, _>("attempts").map_err(db)? + 1)
                .try_into()
                .map_err(corrupt)?,
        };
        tx.commit().await.map_err(db)?;
        Ok(Some(job))

            }
        }).await
    }
    pub async fn fail_job(&self, id: &str, retry: bool) -> Result<(), DatabaseError> {
        let id = Uuid::parse_str(id).map_err(|_| DatabaseError::InvalidInput)?;
        sqlx::query("UPDATE openlegal.corpus_job SET status=CASE WHEN $2 AND attempts<(SELECT max_job_attempts FROM openlegal.provider_request_budget WHERE singleton) THEN 'pending' ELSE 'failed' END,lease_until=NULL,error_category='processing_failed',completed_at=CASE WHEN $2 AND attempts<(SELECT max_job_attempts FROM openlegal.provider_request_budget WHERE singleton) THEN NULL ELSE floor(extract(epoch from clock_timestamp()))::bigint END WHERE id=$1 AND status='running'").bind(id).bind(retry).execute(&self.pool).await.map_err(db)?;
        Ok(())
    }
    /// Provider inventory metadata survives body eviction. This does not claim
    /// that a single page is a complete history inventory.
    pub async fn record_revision_catalog(
        &self,
        object: &ObjectId,
        revision_id: &str,
        publication_date: Option<&str>,
        effective_date: Option<&str>,
        _observed_at: u64,
    ) -> Result<(), DatabaseError> {
        let retry_cancel = CancellationToken::new();
        retry_storage(&retry_cancel, "record_revision_catalog", || {
            async {

        self.gate().await?;
        let k = key(object)?;
        RevisionSelector::Revision {
            id: revision_id.into(),
        }
        .validate()?;
        if [publication_date, effective_date]
            .into_iter()
            .flatten()
            .any(|d| !valid_date(d))
        {
            return Err(DatabaseError::InvalidInput);
        }
        if !object.dataset.has_provider_revisions() {
            return Err(DatabaseError::UnsupportedHistory);
        }
        let mut tx = self.pool.begin().await.map_err(db)?;
        sqlx::query("SELECT singleton FROM openlegal.corpus_control WHERE singleton FOR UPDATE")
            .fetch_one(&mut *tx)
            .await
            .map_err(db)?;
        sqlx::query("INSERT INTO openlegal.corpus_object(object_key,identity) VALUES($1,$2) ON CONFLICT DO NOTHING").bind(&k).bind(serde_json::to_value(object).map_err(corrupt)?).execute(&mut *tx).await.map_err(db)?;
        let previous=sqlx::query("SELECT publication_date,effective_date FROM openlegal.corpus_revision WHERE object_key=$1 AND revision_id=$2").bind(&k).bind(revision_id).fetch_optional(&mut *tx).await.map_err(db)?;
        let inserted = previous.is_none();
        let changed = if let Some(previous) = previous {
            let publication: Option<String> = previous.try_get("publication_date").map_err(db)?;
            let effective: Option<String> = previous.try_get("effective_date").map_err(db)?;
            publication_date.is_some_and(|v| Some(v) != publication.as_deref())
                || effective_date.is_some_and(|v| Some(v) != effective.as_deref())
        } else {
            true
        };
        if changed {
            sqlx::query("INSERT INTO openlegal.corpus_revision(object_key,revision_id,publication_date,effective_date,last_sequence,captured_at) SELECT $1,$2,$3,$4,next_capture,NULL FROM openlegal.corpus_object WHERE object_key=$1 ON CONFLICT(object_key,revision_id) DO UPDATE SET publication_date=COALESCE(EXCLUDED.publication_date,openlegal.corpus_revision.publication_date),effective_date=COALESCE(EXCLUDED.effective_date,openlegal.corpus_revision.effective_date)").bind(&k).bind(revision_id).bind(publication_date).bind(effective_date).execute(&mut *tx).await.map_err(db)?;
            sqlx::query("UPDATE openlegal.corpus_object SET catalog_version=catalog_version+1,next_capture=next_capture+$2 WHERE object_key=$1").bind(k).bind(i64::from(inserted)).execute(&mut *tx).await.map_err(db)?;
        }
        tx.commit().await.map_err(db)?;
        Ok(())

            }
        }).await
    }
    /// Replays an exact publication, including expired bytes protected by the
    /// index acknowledgment fence. Missing bytes require durable retirement proof.
    pub async fn index_capture(
        &self,
        event: &OutboxEntry,
        cancel: CancellationToken,
    ) -> Result<Option<Capture>, DatabaseError> {
        if event.withdrawn || event.removed {
            return Ok(None);
        }
        let id = event
            .capture_id
            .as_ref()
            .ok_or(DatabaseError::StorageCorrupt)?;
        let k = key(&event.object)?;
        let exists:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM openlegal.corpus_outbox WHERE sequence=$1 AND object_key=$2 AND capture_id=$3 AND NOT withdrawn AND NOT removed)").bind(i64::try_from(event.sequence).map_err(|_|DatabaseError::InvalidInput)?).bind(k).bind(id).fetch_one(&self.pool).await.map_err(db)?;
        if !exists {
            return Err(DatabaseError::StorageCorrupt);
        }
        match self.capture_inner(id, 0, true, cancel).await {
            Ok(capture) => {
                if capture.record.object != event.object {
                    return Err(DatabaseError::StorageCorrupt);
                }
                Ok(Some(capture))
            }
            Err(DatabaseError::RevisionUnavailable) => {
                let retired:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM openlegal.corpus_outbox WHERE capture_id=$1 AND removed)").bind(id).fetch_one(&self.pool).await.map_err(db)?;
                if retired {
                    Ok(None)
                } else {
                    self.blocked.store(true, Ordering::Release);
                    Err(DatabaseError::StorageCorrupt)
                }
            }
            Err(error) => {
                if error == DatabaseError::StorageCorrupt {
                    self.blocked.store(true, Ordering::Release);
                }
                Err(error)
            }
        }
    }
    /// Only a successfully stabilized complete traversal can set this true.
    /// Complements runtime inventory stability and index-lag checks. Historical
    /// catalog-only objects and superseded jobs do not define current coverage.
    pub async fn current_coverage_ready(&self, now: u64) -> Result<bool, DatabaseError> {
        self.gate().await?;
        sqlx::query_scalar("SELECT NOT EXISTS(SELECT 1 FROM openlegal.corpus_object o LEFT JOIN openlegal.corpus_capture c ON c.id=o.head_capture WHERE o.identity->>'jurisdiction'='kr' AND o.identity->>'provider'='law_go_kr' AND NOT o.withdrawn AND o.desired_head_revision IS NOT NULL AND (o.head_capture IS NULL OR o.pending OR c.revision_id IS DISTINCT FROM o.desired_head_revision OR COALESCE(c.payload->'record'->'metadata'->>'attachment_status','complete') = 'incomplete' OR o.validated_at IS NULL OR o.validated_at>$1::text::numeric OR o.validated_at<=$1::text::numeric-86400 OR EXISTS(SELECT 1 FROM openlegal.corpus_job j WHERE j.object_key=o.object_key AND j.install_head AND j.revision_id=o.desired_head_revision AND j.status IN ('pending','running','failed'))))").bind(now.to_string()).fetch_one(&self.pool).await.map_err(db)
    }
    pub async fn mark_dataset_inventory_complete(
        &self,
        dataset: Dataset,
        complete: bool,
    ) -> Result<(), DatabaseError> {
        let retry_cancel = CancellationToken::new();
        retry_storage(&retry_cancel, "mark_dataset_inventory_complete", || {
            async {

        self.gate().await?;
        if !dataset.has_provider_revisions() {
            return Err(DatabaseError::UnsupportedHistory);
        }
        let dataset = serde_json::to_value(dataset)
            .map_err(corrupt)?
            .as_str()
            .ok_or(DatabaseError::InvalidInput)?
            .to_owned();
        sqlx::query("UPDATE openlegal.corpus_object SET inventory_complete=$1,catalog_version=catalog_version+1 WHERE identity->>'jurisdiction'='kr' AND identity->>'provider'='law_go_kr' AND identity->>'dataset'=$2 AND inventory_complete IS DISTINCT FROM $1").bind(complete).bind(dataset).execute(&self.pool).await.map_err(db)?;
        Ok(())

            }
        }).await
    }
    /// End only this active execution after a proven database rejection. Keep
    /// charged attempts and object ownership intact until the cooldown expires.
    pub async fn defer_storage_claim(&self, job: &Job) -> Result<(), DatabaseError> {
        let id = Uuid::parse_str(&job.id).map_err(|_| DatabaseError::InvalidInput)?;
        let version =
            i64::try_from(job.expected_version).map_err(|_| DatabaseError::InvalidInput)?;
        let attempts = i32::try_from(job.attempts).map_err(|_| DatabaseError::InvalidInput)?;
        let cooldown = 5_i64.pow(job.attempts.min(3));
        retry_storage(&CancellationToken::new(), "detail.storage_cooldown", || async {
            self.gate().await?;
            let mut tx = self.pool.begin().await.map_err(db)?;
            sqlx::query("SELECT singleton FROM openlegal.corpus_control WHERE singleton FOR UPDATE")
                .fetch_one(&mut *tx).await.map_err(db)?;
            let changed = sqlx::query("UPDATE openlegal.corpus_job j SET status=CASE WHEN j.attempts>=b.max_job_attempts THEN 'failed' ELSE 'running' END,lease_until=CASE WHEN j.attempts>=b.max_job_attempts THEN NULL ELSE floor(extract(epoch from clock_timestamp()))::bigint+$4 END,error_category='processing_failed',completed_at=CASE WHEN j.attempts>=b.max_job_attempts THEN floor(extract(epoch from clock_timestamp()))::bigint ELSE NULL END FROM openlegal.provider_request_budget b WHERE b.singleton AND j.id=$1 AND j.expected_version=$2 AND j.attempts=$3 AND j.status='running' AND j.error_category IS NULL AND j.lease_until>floor(extract(epoch from clock_timestamp()))::bigint AND EXISTS(SELECT 1 FROM openlegal.corpus_object o WHERE o.object_key=j.object_key AND o.version=j.expected_version AND NOT o.withdrawn)")
                .bind(id).bind(version).bind(attempts).bind(cooldown)
                .execute(&mut *tx).await.map_err(db)?.rows_affected();
            if changed != 1 {
                return Err(DatabaseError::Conflict);
            }
            tx.commit().await.map_err(db)
        }).await
    }
    pub async fn fail_claim(&self, job: &Job, retry: bool) -> Result<(), DatabaseError> {
        let retry_cancel = CancellationToken::new();
        retry_storage(&retry_cancel, "fail_claim", || {
            async {

        let id = Uuid::parse_str(&job.id).map_err(|_| DatabaseError::InvalidInput)?;
        sqlx::query("UPDATE openlegal.corpus_job SET status=CASE WHEN $2 AND attempts<(SELECT max_job_attempts FROM openlegal.provider_request_budget WHERE singleton) THEN 'pending' ELSE 'failed' END,lease_until=NULL,error_category='processing_failed',completed_at=CASE WHEN $2 AND attempts<(SELECT max_job_attempts FROM openlegal.provider_request_budget WHERE singleton) THEN NULL ELSE floor(extract(epoch from clock_timestamp()))::bigint END WHERE id=$1 AND expected_version=$3 AND attempts=$4 AND status='running'").bind(id).bind(retry).bind(i64::try_from(job.expected_version).map_err(|_|DatabaseError::InvalidInput)?).bind(job.attempts as i32).execute(&self.pool).await.map_err(db)?;
        Ok(())

            }
        }).await
    }
    /// A claim that never reserved an upstream attempt does not spend a
    /// processing execution. A later claim has a new object version, so an old
    /// claimant cannot refund a newly reclaimed job with the same attempt count.
    pub async fn release_admission_wait(&self, job: &Job) -> Result<(), DatabaseError> {
        let retry_cancel = CancellationToken::new();
        retry_storage(&retry_cancel, "release_admission_wait", || {
            async {

        let id = Uuid::parse_str(&job.id).map_err(|_| DatabaseError::InvalidInput)?;
        if job.attempts == 0 {
            return Err(DatabaseError::InvalidInput);
        }
        let changed = sqlx::query("UPDATE openlegal.corpus_job SET status='pending',attempts=attempts-1,lease_until=NULL,error_category=NULL,started_at=NULL,completed_at=NULL WHERE id=$1 AND expected_version=$2 AND attempts=$3 AND status='running'")
            .bind(id).bind(i64::try_from(job.expected_version).map_err(|_|DatabaseError::InvalidInput)?)
            .bind(job.attempts as i32).execute(&self.pool).await.map_err(db)?.rows_affected();
        if changed != 1 {
            return Err(DatabaseError::Conflict);
        }
        Ok(())

            }
        }).await
    }
    /// A daily upstream cap is admission policy, not a failed processing
    /// attempt. Keep the claim leased until the next UTC day across restarts.
    pub async fn defer_budget_claim(&self, job: &Job, resume_at: u64) -> Result<(), DatabaseError> {
        self.defer_budget_claim_inner(job, resume_at, true, None)
            .await
    }
    /// A response with Retry-After has already spent a provider attempt. Keep
    /// that execution charged while preserving the durable next-eligible lease.
    /// An exhausted execution fails immediately; the provider pause is untouched.
    pub async fn defer_reserved_budget_claim(
        &self,
        job: &Job,
        resume_at: u64,
    ) -> Result<(), DatabaseError> {
        self.defer_budget_claim_inner(job, resume_at, false, None)
            .await
    }
    pub async fn defer_budget_claim_with_fingerprint(
        &self,
        job: &Job,
        resume_at: u64,
        fingerprint: Option<&openlegal_domain::provider_admin::ProviderBlockerFingerprint>,
        reserved: bool,
    ) -> Result<(), DatabaseError> {
        self.defer_budget_claim_inner(job, resume_at, !reserved, fingerprint)
            .await
    }
    async fn defer_budget_claim_inner(
        &self,
        job: &Job,
        resume_at: u64,
        refund: bool,
        fingerprint: Option<&openlegal_domain::provider_admin::ProviderBlockerFingerprint>,
    ) -> Result<(), DatabaseError> {
        let retry_cancel = CancellationToken::new();
        retry_storage(&retry_cancel, "defer_budget_claim_inner", || {
            async {

        let id = Uuid::parse_str(&job.id).map_err(|_| DatabaseError::InvalidInput)?;
        if job.attempts == 0 || resume_at == 0 {
            return Err(DatabaseError::InvalidInput);
        }
        let rows = sqlx::query("UPDATE openlegal.corpus_job j SET attempts=j.attempts-CASE WHEN $5 THEN 1 ELSE 0 END,status=CASE WHEN NOT $5 AND j.attempts>=b.max_job_attempts THEN 'failed' ELSE 'running' END,lease_until=CASE WHEN NOT $5 AND j.attempts>=b.max_job_attempts THEN NULL ELSE $1::text::numeric END,error_category=CASE WHEN NOT $5 AND j.attempts>=b.max_job_attempts THEN 'processing_failed' ELSE 'budget_wait' END,provider_deferral_fingerprint=CASE WHEN NOT $5 AND j.attempts>=b.max_job_attempts THEN NULL ELSE $6::jsonb END,completed_at=CASE WHEN NOT $5 AND j.attempts>=b.max_job_attempts THEN floor(extract(epoch from clock_timestamp()))::bigint ELSE NULL END FROM openlegal.provider_request_budget b WHERE b.singleton AND j.id=$2 AND j.expected_version=$3 AND j.attempts=$4 AND j.status='running'")
            .bind(resume_at.to_string()).bind(id)
            .bind(i64::try_from(job.expected_version).map_err(|_|DatabaseError::InvalidInput)?)
            .bind(job.attempts as i32).bind(refund).bind(fingerprint.map(serde_json::to_value).transpose().map_err(corrupt)?).execute(&self.pool).await.map_err(db)?.rows_affected();
        if rows != 1 {
            return Err(DatabaseError::Conflict);
        }
        Ok(())

            }
        }).await
    }
    pub async fn mark_inventory_complete(
        &self,
        object: &ObjectId,
        complete: bool,
    ) -> Result<(), DatabaseError> {
        let retry_cancel = CancellationToken::new();
        retry_storage(&retry_cancel, "mark_inventory_complete", || {
            async {

        self.gate().await?;
        let k = key(object)?;
        sqlx::query("UPDATE openlegal.corpus_object SET inventory_complete=$2,catalog_version=catalog_version+1 WHERE object_key=$1 AND inventory_complete IS DISTINCT FROM $2")
            .bind(k)
            .bind(complete)
            .execute(&self.pool)
            .await
            .map_err(db)?;
        Ok(())

            }
        }).await
    }
    pub async fn revalidate(
        &self,
        object: &ObjectId,
        capture_id: &str,
        expected_version: u64,
        now: u64,
    ) -> Result<(), DatabaseError> {
        self.gate().await?;
        let k = key(object)?;
        let count=sqlx::query("UPDATE openlegal.corpus_object SET validated_at=$4::text::numeric,pending=false,version=version+1 WHERE object_key=$1 AND head_capture=$2 AND version=$3 AND NOT withdrawn AND validated_at<=$4::text::numeric").bind(k).bind(capture_id).bind(i64::try_from(expected_version).map_err(|_|DatabaseError::InvalidInput)?).bind(now.to_string()).execute(&self.pool).await.map_err(db)?.rows_affected();
        if count == 0 {
            return Err(DatabaseError::Conflict);
        }
        Ok(())
    }
    pub async fn withdraw(
        &self,
        object: &ObjectId,
        expected_version: u64,
        _now: u64,
    ) -> Result<(), DatabaseError> {
        self.gate().await?;
        let k = key(object)?;
        let mut tx = self.pool.begin().await.map_err(db)?;
        sqlx::query("SELECT singleton FROM openlegal.corpus_control WHERE singleton FOR UPDATE")
            .fetch_one(&mut *tx)
            .await
            .map_err(db)?;
        let changed=sqlx::query("UPDATE openlegal.corpus_object SET withdrawn=true,pending=false,version=version+1 WHERE object_key=$1 AND version=$2 RETURNING version").bind(&k).bind(i64::try_from(expected_version).map_err(|_|DatabaseError::InvalidInput)?).fetch_optional(&mut *tx).await.map_err(db)?.ok_or(DatabaseError::Conflict)?;
        let version: i64 = changed.try_get("version").map_err(db)?;
        sqlx::query("INSERT INTO openlegal.corpus_outbox SELECT next_event,$1,$2,NULL,true,true,false FROM openlegal.corpus_control").bind(&k).bind(version).execute(&mut *tx).await.map_err(db)?;
        sqlx::query("UPDATE openlegal.corpus_control SET next_event=next_event+1")
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        sqlx::query("UPDATE openlegal.corpus_session SET invalidated=true WHERE NOT invalidated")
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        sqlx::query("DELETE FROM openlegal.corpus_citation_lease l USING openlegal.corpus_capture c WHERE l.capture_id=c.id AND c.object_key=$1")
            .bind(k)
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(())
    }
    pub async fn watermark(&self) -> Result<u64, DatabaseError> {
        self.gate().await?;
        let n: i64 = sqlx::query_scalar("SELECT next_event-1 FROM openlegal.corpus_control")
            .fetch_one(&self.pool)
            .await
            .map_err(db)?;
        n.try_into().map_err(corrupt)
    }
    pub async fn outbox(
        &self,
        after: u64,
        limit: usize,
    ) -> Result<Vec<OutboxEntry>, DatabaseError> {
        self.gate().await?;
        if !(1..=1000).contains(&limit) {
            return Err(DatabaseError::InvalidInput);
        }
        let rows=sqlx::query("SELECT e.*,o.identity FROM openlegal.corpus_outbox e JOIN openlegal.corpus_object o USING(object_key) WHERE e.sequence>$1 ORDER BY e.sequence LIMIT $2").bind(i64::try_from(after).map_err(|_|DatabaseError::InvalidInput)?).bind(limit as i64).fetch_all(&self.pool).await.map_err(db)?;
        rows.into_iter()
            .map(|r| {
                Ok(OutboxEntry {
                    sequence: r
                        .try_get::<i64, _>("sequence")
                        .map_err(db)?
                        .try_into()
                        .map_err(corrupt)?,
                    object: serde_json::from_value(r.try_get("identity").map_err(db)?)
                        .map_err(corrupt)?,
                    capture_id: r.try_get("capture_id").map_err(db)?,
                    object_version: r
                        .try_get::<i64, _>("object_version")
                        .map_err(db)?
                        .try_into()
                        .map_err(corrupt)?,
                    withdrawn: r.try_get("withdrawn").map_err(db)?,
                    install_head: r.try_get("is_head").map_err(db)?,
                    removed: r.try_get("removed").map_err(db)?,
                })
            })
            .collect()
    }
    /// Stable object hash/revision order. Search builders must pin their watermark
    /// before scanning and reject/retry a changed watermark before publication.
    pub async fn scan(
        &self,
        include_history: bool,
        after: Option<String>,
        limit: usize,
        now: u64,
        cancel: CancellationToken,
    ) -> Result<CorpusPage, DatabaseError> {
        self.gate().await?;
        if !(1..=100).contains(&limit) || after.as_ref().is_some_and(|c| c.len() > 1024) {
            return Err(DatabaseError::InvalidInput);
        }
        let rows=sqlx::query("SELECT c.id,c.object_key || ':' || c.revision_id AS position FROM openlegal.corpus_capture c JOIN openlegal.corpus_object o USING(object_key) JOIN openlegal.corpus_revision r ON r.object_key=c.object_key AND r.revision_id=c.revision_id AND r.latest_capture=c.id WHERE NOT o.withdrawn AND NOT EXISTS(SELECT 1 FROM openlegal.corpus_retirement t WHERE t.capture_id=c.id) AND ($1 OR o.head_capture=c.id) AND (c.object_key || ':' || c.revision_id)>$2 ORDER BY c.object_key,c.revision_id LIMIT $3").bind(include_history).bind(after.unwrap_or_default()).bind((limit+1) as i64).fetch_all(&self.pool).await.map_err(db)?;
        let more = rows.len() > limit;
        let mut captures = Vec::new();
        let mut cursor = None;
        let mut total = 0usize;
        for row in rows.into_iter().take(limit) {
            let id: String = row.try_get("id").map_err(db)?;
            let c = self.capture(&id, now, cancel.clone()).await?;
            total = total.saturating_add(
                c.record.body.len()
                    + c.record
                        .sections
                        .iter()
                        .map(|s| s.text.len())
                        .sum::<usize>(),
            );
            if total > 128 * 1024 * 1024 {
                return Err(DatabaseError::Capacity);
            }
            captures.push(c);
            cursor = Some(row.try_get("position").map_err(db)?);
        }
        Ok(CorpusPage {
            captures,
            next_cursor: if more { cursor } else { None },
        })
    }
    pub(super) async fn history_inner(
        &self,
        object: ObjectId,
        kind: HistoryKind,
        cursor: Option<String>,
        limit: usize,
        _now: u64,
        cancel: CancellationToken,
    ) -> Result<HistoryPage, DatabaseError> {
        check(&cancel)?;
        if !(1..=100).contains(&limit) || cursor.as_ref().is_some_and(|c| c.len() > 512) {
            return Err(DatabaseError::InvalidInput);
        }
        let state = self.state(&object).await?;
        if state.withdrawn {
            return Err(DatabaseError::Withdrawn);
        }
        if !object.dataset.has_provider_revisions() && matches!(kind, HistoryKind::Revisions) {
            return Err(DatabaseError::UnsupportedHistory);
        }
        let k = key(&object)?;
        let revisions = matches!(kind, HistoryKind::Revisions);
        let tag = if revisions { "r2" } else { "c" };
        let mut after_revision = None;
        let mut after_date = String::new();
        let mut before = i64::MAX;
        if let Some(cursor) = cursor {
            // The revision ID occupies the terminal field, so embedded colons
            // remain literal UTF-8. Its 256-byte limit keeps r2 cursors below 512
            // bytes without expanding provider IDs into escaped representations.
            let fields = if revisions { 5 } else { 4 };
            let parts: Vec<_> = if revisions {
                cursor.splitn(fields, ':').collect()
            } else {
                cursor.split(':').collect()
            };
            if parts.len() != fields
                || parts[0] != k
                || parts[1] != tag
                || parts[2] != format!("{}.{}", state.version, state.catalog_version)
            {
                return Err(DatabaseError::SnapshotInvalidated);
            }
            if revisions {
                if !parts[3].is_empty() && !valid_date(parts[3]) {
                    return Err(DatabaseError::InvalidInput);
                }
                RevisionSelector::Revision {
                    id: parts[4].into(),
                }
                .validate()?;
                after_date = parts[3].into();
                after_revision = Some(parts[4].to_owned());
            } else {
                before = parts[3]
                    .parse::<i64>()
                    .map_err(|_| DatabaseError::InvalidInput)?;
            }
        }
        let rows = if revisions {
            // Observation sequences can change when old bytes are corrected;
            // chronological presentation follows retained checkpoint dates.
            // Empty dates sort last, and C collation gives a stable ID tie-break.
            if let Some(after_revision) = after_revision {
                // Separate bounded ranges let the index seek past the cursor
                // even for large groups of revisions sharing a checkpoint date.
                let sql = "SELECT * FROM ((SELECT revision_id,latest_capture AS capture_id,last_sequence AS sequence,captured_at::text,publication_date,effective_date,COALESCE(effective_date,publication_date,'') AS sort_date FROM openlegal.corpus_revision WHERE object_key=$1 AND COALESCE(effective_date,publication_date,'')=$2 AND revision_id COLLATE \"C\">$3 COLLATE \"C\" ORDER BY COALESCE(effective_date,publication_date,'') DESC,revision_id COLLATE \"C\" ASC LIMIT $4) UNION ALL (SELECT revision_id,latest_capture AS capture_id,last_sequence AS sequence,captured_at::text,publication_date,effective_date,COALESCE(effective_date,publication_date,'') AS sort_date FROM openlegal.corpus_revision WHERE object_key=$1 AND COALESCE(effective_date,publication_date,'')<$2 ORDER BY COALESCE(effective_date,publication_date,'') DESC,revision_id COLLATE \"C\" ASC LIMIT $4)) remaining ORDER BY sort_date DESC,revision_id COLLATE \"C\" ASC LIMIT $4";
                sqlx::query(sql)
                    .bind(&k)
                    .bind(after_date)
                    .bind(after_revision)
                    .bind((limit + 1) as i64)
                    .fetch_all(&self.pool)
                    .await
                    .map_err(db)?
            } else {
                sqlx::query("SELECT revision_id,latest_capture AS capture_id,last_sequence AS sequence,captured_at::text,publication_date,effective_date FROM openlegal.corpus_revision WHERE object_key=$1 ORDER BY COALESCE(effective_date,publication_date,'') DESC,revision_id COLLATE \"C\" ASC LIMIT $2")
                    .bind(&k)
                    .bind((limit + 1) as i64)
                    .fetch_all(&self.pool)
                    .await
                    .map_err(db)?
            }
        } else {
            sqlx::query("SELECT revision_id,id AS capture_id,sequence,captured_at::text,publication_date,effective_date FROM openlegal.corpus_capture_catalog WHERE object_key=$1 AND sequence<$2 ORDER BY sequence DESC LIMIT $3")
                .bind(&k)
                .bind(before)
                .bind((limit + 1) as i64)
                .fetch_all(&self.pool)
                .await
                .map_err(db)?
        };
        let more = rows.len() > limit;
        let mut entries = Vec::new();
        for r in rows.into_iter().take(limit) {
            entries.push(HistoryEntry {
                revision_id: r.try_get("revision_id").map_err(db)?,
                capture_id: r.try_get("capture_id").map_err(db)?,
                sequence: r
                    .try_get::<i64, _>("sequence")
                    .map_err(db)?
                    .try_into()
                    .map_err(corrupt)?,
                captured_at: r
                    .try_get::<Option<String>, _>("captured_at")
                    .map_err(db)?
                    .map(|v| v.parse::<u64>().map_err(corrupt))
                    .transpose()?,
                publication_date: r.try_get("publication_date").map_err(db)?,
                effective_date: r.try_get("effective_date").map_err(db)?,
            });
        }
        let new_state = self.state(&object).await?;
        if new_state.version != state.version
            || new_state.catalog_version != state.catalog_version
            || new_state.withdrawn
        {
            return Err(DatabaseError::SnapshotInvalidated);
        }
        check(&cancel)?;
        let next_cursor = if more {
            entries.last().map(|e| {
                if revisions {
                    format!(
                        "{k}:{tag}:{}.{}:{}:{}",
                        state.version,
                        state.catalog_version,
                        e.effective_date
                            .as_deref()
                            .or(e.publication_date.as_deref())
                            .unwrap_or(""),
                        e.revision_id,
                    )
                } else {
                    format!(
                        "{k}:{tag}:{}.{}:{}",
                        state.version, state.catalog_version, e.sequence
                    )
                }
            })
        } else {
            None
        };
        Ok(HistoryPage {
            entries,
            next_cursor,
            inventory_complete: state.inventory_complete,
        })
    }
    /// Empty IDs pin the whole index generation, without materializing its corpus.
    pub async fn pin_session(
        &self,
        id: String,
        generation: u64,
        capture_ids: Vec<String>,
        now: u64,
    ) -> Result<(), DatabaseError> {
        self.gate().await?;
        if !openlegal_domain::history::valid_snapshot_id(&id)
            || capture_ids.len() > 1000
            || capture_ids
                .iter()
                .any(|v| !openlegal_domain::history::valid_snapshot_id(v))
        {
            return Err(DatabaseError::InvalidInput);
        }
        let mut tx = self.pool.begin().await.map_err(db)?;
        let watermark: i64 = sqlx::query_scalar(
            "SELECT next_event-1 FROM openlegal.corpus_control WHERE singleton FOR UPDATE",
        )
        .fetch_one(&mut *tx)
        .await
        .map_err(db)?;
        let generation = i64::try_from(generation).map_err(|_| DatabaseError::InvalidInput)?;
        if generation > watermark {
            return Err(DatabaseError::InvalidInput);
        }
        let acknowledged: i64 =
            sqlx::query_scalar("SELECT index_ack FROM openlegal.corpus_control")
                .fetch_one(&mut *tx)
                .await
                .map_err(db)?;
        if capture_ids.is_empty() && generation < acknowledged {
            return Err(DatabaseError::SnapshotInvalidated);
        }
        let count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM openlegal.corpus_session WHERE expires_at>$1::text::numeric",
        )
        .bind(now.to_string())
        .fetch_one(&mut *tx)
        .await
        .map_err(db)?;
        if count >= 128 {
            return Err(DatabaseError::Capacity);
        }
        // A withdrawal after this generation makes it unsuitable for new sessions.
        let revoked: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM openlegal.corpus_outbox WHERE sequence>$1 AND withdrawn)",
        )
        .bind(generation)
        .fetch_one(&mut *tx)
        .await
        .map_err(db)?;
        if revoked {
            return Err(DatabaseError::SnapshotInvalidated);
        }
        // Withdrawal and GC use this same control lock. A newer watermark cannot
        // revive a capture whose object was withdrawn before this pin request.
        for capture in &capture_ids {
            let withdrawn:Option<bool>=sqlx::query_scalar("SELECT o.withdrawn FROM openlegal.corpus_capture c JOIN openlegal.corpus_object o USING(object_key) WHERE c.id=$1").bind(capture).fetch_optional(&mut *tx).await.map_err(db)?;
            match withdrawn {
                Some(false) => {}
                Some(true) => return Err(DatabaseError::SnapshotInvalidated),
                None => return Err(DatabaseError::RevisionUnavailable),
            }
        }
        sqlx::query("INSERT INTO openlegal.corpus_session VALUES($1,$2,$3::text::numeric,false)")
            .bind(&id)
            .bind(generation)
            .bind(now.saturating_add(SESSION_SECONDS).to_string())
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        for capture in capture_ids {
            sqlx::query("INSERT INTO openlegal.corpus_pin VALUES($1,$2)")
                .bind(&id)
                .bind(capture)
                .execute(&mut *tx)
                .await
                .map_err(db)?;
        }
        tx.commit().await.map_err(db)?;
        Ok(())
    }
    pub async fn check_session(&self, id: &str, now: u64) -> Result<(), DatabaseError> {
        self.gate().await?;
        let row = sqlx::query(
            "SELECT invalidated,expires_at::text FROM openlegal.corpus_session WHERE id=$1",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await
        .map_err(db)?
        .ok_or(DatabaseError::SessionExpired)?;
        if row.try_get::<bool, _>("invalidated").map_err(db)? {
            return Err(DatabaseError::SnapshotInvalidated);
        }
        if unsigned(&row, "expires_at")? <= now {
            return Err(DatabaseError::SessionExpired);
        }
        Ok(())
    }
    pub async fn release_session(&self, id: &str) -> Result<(), DatabaseError> {
        sqlx::query("DELETE FROM openlegal.corpus_session WHERE id=$1")
            .bind(id)
            .execute(&self.pool)
            .await
            .map_err(db)?;
        Ok(())
    }
    /// Bounded cleanup of temporary stages, sessions and citation leases. Captures
    /// and attachment evidence are permanent; the legacy cutoff is ignored.
    pub async fn maintain(
        &self,
        now: u64,
        _historical_before: u64,
    ) -> Result<usize, DatabaseError> {
        self.gate().await?;
        retry_storage(&CancellationToken::new(), "maintenance.cleanup", || async {
        let mut tx = self.pool.begin().await.map_err(db)?;
        sqlx::query("SELECT singleton FROM openlegal.corpus_control WHERE singleton FOR UPDATE")
            .fetch_one(&mut *tx)
            .await
            .map_err(db)?;
        sqlx::query("DELETE FROM openlegal.corpus_session WHERE expires_at<=$1::text::numeric OR invalidated").bind(now.to_string()).execute(&mut *tx).await.map_err(db)?;
        sqlx::query("DELETE FROM openlegal.corpus_citation_lease l WHERE l.expires_at<=$1::text::numeric OR EXISTS(SELECT 1 FROM openlegal.corpus_capture c JOIN openlegal.corpus_object o USING(object_key) WHERE c.id=l.capture_id AND o.withdrawn)")
            .bind(now.to_string())
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        // A pending deletion must never remove still-referenced archive bytes,
        // including queues left by a previous release.
        sqlx::query("DELETE FROM openlegal.corpus_blob_deletion d WHERE EXISTS(SELECT 1 FROM openlegal.corpus_capture c WHERE c.storage_key=d.storage_key) OR EXISTS(SELECT 1 FROM openlegal.corpus_capture_blob b WHERE b.storage_key=d.storage_key) OR EXISTS(SELECT 1 FROM openlegal.corpus_source_observation s WHERE s.storage_key=d.storage_key)")
            .execute(&mut *tx).await.map_err(db)?;
        let stages=sqlx::query("SELECT * FROM openlegal.corpus_staging WHERE created_at<$1::text::numeric ORDER BY created_at LIMIT 128 FOR UPDATE").bind(now.saturating_sub(3600).to_string()).fetch_all(&mut *tx).await.map_err(db)?;
        for row in stages {
            let location: String = row.try_get("storage_key").map_err(db)?;
            let size: i64 = row.try_get("raw_size").map_err(db)?;
            sqlx::query("INSERT INTO openlegal.corpus_blob_deletion VALUES($1,$2,$3) ON CONFLICT DO NOTHING").bind(&location).bind(row.try_get::<Vec<u8>,_>("raw_sha256").map_err(db)?).bind(row.try_get::<i64,_>("raw_size").map_err(db)?).execute(&mut *tx).await.map_err(db)?;
            sqlx::query("DELETE FROM openlegal.corpus_staging WHERE storage_key=$1")
                .bind(&location)
                .execute(&mut *tx)
                .await
                .map_err(db)?;
            sqlx::query("UPDATE openlegal.corpus_control SET staged_bytes=staged_bytes-$1")
                .bind(size)
                .execute(&mut *tx)
                .await
                .map_err(db)?;
        }
        tx.commit().await.map_err(db)?;
            Ok(())
        }).await?;
        let deletions = retry_storage(&CancellationToken::new(), "maintenance.deletion_queue", || async {
 sqlx::query("SELECT * FROM openlegal.corpus_blob_deletion d WHERE NOT EXISTS(SELECT 1 FROM openlegal.corpus_capture c WHERE c.storage_key=d.storage_key) AND NOT EXISTS(SELECT 1 FROM openlegal.corpus_capture_blob b WHERE b.storage_key=d.storage_key) AND NOT EXISTS(SELECT 1 FROM openlegal.corpus_source_observation s WHERE s.storage_key=d.storage_key) LIMIT 128")
            .fetch_all(&self.pool)
            .await
            .map_err(db)
        }).await?;
        for row in deletions {
            let location = BlobLocation {
                digest: row
                    .try_get::<Vec<u8>, _>("raw_sha256")
                    .map_err(db)?
                    .try_into()
                    .map_err(corrupt)?,
                size_bytes: row
                    .try_get::<i64, _>("raw_size")
                    .map_err(db)?
                    .try_into()
                    .map_err(corrupt)?,
                storage_key: row.try_get("storage_key").map_err(db)?,
            };
            self.blobs
                .delete_if_present(location.clone(), CancellationToken::new())
                .await
                .map_err(corrupt)?;
            retry_storage(
                &CancellationToken::new(),
                "maintenance.remove_deleted",
                || async {
                    sqlx::query("DELETE FROM openlegal.corpus_blob_deletion WHERE storage_key=$1")
                        .bind(&location.storage_key)
                        .execute(&self.pool)
                        .await
                        .map_err(db)?;
                    Ok(())
                },
            )
            .await?;
        }
        Ok(0)
    }
}
