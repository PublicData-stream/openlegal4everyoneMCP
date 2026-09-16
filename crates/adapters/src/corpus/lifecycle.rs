use super::*;

impl PgCorpusStore {
    /// Acknowledge only after the durable index generation has incorporated every
    /// event through this sequence. Gaps are not valid acknowledgements.
    pub async fn acknowledge_index(&self, generation: u64) -> Result<(), DatabaseError> {
        let generation = i64::try_from(generation).map_err(|_| DatabaseError::InvalidInput)?;
        let count=sqlx::query("UPDATE openlegal.corpus_control SET index_ack=$1 WHERE index_ack<=$1 AND next_event>$1").bind(generation).execute(&self.pool).await.map_err(db)?.rows_affected();
        if count == 0 {
            return Err(DatabaseError::Conflict);
        }
        Ok(())
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
        let head_changed = install_head
            && (row
                .try_get::<Option<String>, _>("desired_head_revision")
                .map_err(db)?
                .as_deref()
                != Some(revision_id.as_str())
                || (mark_head_pending && !row.try_get::<bool, _>("pending").map_err(db)?));
        let version = row.try_get::<i64, _>("version").map_err(db)? + i64::from(head_changed);
        if install_head {
            sqlx::query("UPDATE openlegal.corpus_job SET status='failed',lease_until=NULL,error_category='superseded' WHERE object_key=$1 AND install_head AND revision_id<>$2 AND status IN ('pending','running')").bind(&k).bind(&revision_id).execute(&mut *tx).await.map_err(db)?;
            sqlx::query("UPDATE openlegal.corpus_object SET desired_head_revision=$2,pending=pending OR $3,version=$4 WHERE object_key=$1").bind(&k).bind(&revision_id).bind(mark_head_pending).bind(version).execute(&mut *tx).await.map_err(db)?;
        }
        if let Some(job)=sqlx::query("SELECT id,expected_version,attempts,install_head,source_metadata FROM openlegal.corpus_job WHERE object_key=$1 AND revision_id=$2 AND effective_date=$3 AND status IN ('pending','running')").bind(&k).bind(&revision_id).bind(date).fetch_optional(&mut *tx).await.map_err(db)? {
            let id:Uuid=job.try_get("id").map_err(db)?;
            let head=install_head||job.try_get::<bool,_>("install_head").map_err(db)?;
            sqlx::query("UPDATE openlegal.corpus_job SET install_head=$2 WHERE id=$1").bind(id).bind(head).execute(&mut *tx).await.map_err(db)?;
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
        let job:Uuid=sqlx::query_scalar("INSERT INTO openlegal.corpus_job(object_key,revision_id,effective_date,install_head,expected_version,status,created_at,source_metadata) VALUES($1,$2,$3,$4,$5,'pending',$6::text::numeric,$7) ON CONFLICT(object_key,revision_id,effective_date) DO UPDATE SET source_metadata=EXCLUDED.source_metadata,expected_version=EXCLUDED.expected_version,install_head=EXCLUDED.install_head,status='pending',attempts=0,created_at=EXCLUDED.created_at,lease_until=NULL,error_category=NULL RETURNING id").bind(&k).bind(&revision_id).bind(date).bind(install_head).bind(version).bind(now.to_string()).bind(serde_json::to_value(&source_metadata).map_err(corrupt)?).fetch_one(&mut *tx).await.map_err(db)?;
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
    /// Ten-minute leases are bounded; a dead worker is retried at most three times.
    pub async fn claim_job(&self, now: u64) -> Result<Option<Job>, DatabaseError> {
        self.gate().await?;
        let mut tx = self.pool.begin().await.map_err(db)?;
        sqlx::query("SELECT singleton FROM openlegal.corpus_control WHERE singleton FOR UPDATE")
            .fetch_one(&mut *tx)
            .await
            .map_err(db)?;
        sqlx::query("UPDATE openlegal.corpus_job SET status='failed',error_category='attempts_exhausted' WHERE attempts>=3 AND status='running' AND lease_until<=$1::text::numeric").bind(now.to_string()).execute(&mut *tx).await.map_err(db)?;
        let row=sqlx::query("SELECT j.id,j.revision_id,j.expected_version,j.attempts,j.effective_date,j.install_head,j.source_metadata,o.identity,o.version AS object_version,j.object_key FROM openlegal.corpus_job j JOIN openlegal.corpus_object o USING(object_key) WHERE NOT o.withdrawn AND (NOT j.install_head OR j.revision_id=o.desired_head_revision) AND NOT EXISTS(SELECT 1 FROM openlegal.corpus_job active WHERE active.object_key=j.object_key AND active.id<>j.id AND active.status='running' AND active.lease_until>$1::text::numeric) AND j.attempts<3 AND (j.status='pending' OR (j.status='running' AND j.lease_until<=$1::text::numeric)) ORDER BY j.created_at,j.id LIMIT 1 FOR UPDATE OF j SKIP LOCKED").bind(now.to_string()).fetch_optional(&mut *tx).await.map_err(db)?;
        let Some(row) = row else {
            return Ok(None);
        };
        let id: Uuid = row.try_get("id").map_err(db)?;
        sqlx::query("UPDATE openlegal.corpus_job SET status='running',attempts=attempts+1,lease_until=$2::text::numeric,expected_version=$3 WHERE id=$1").bind(id).bind(now.saturating_add(SESSION_SECONDS).to_string()).bind(row.try_get::<i64,_>("object_version").map_err(db)?+1).execute(&mut *tx).await.map_err(db)?;
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
    pub async fn fail_job(&self, id: &str, retry: bool) -> Result<(), DatabaseError> {
        let id = Uuid::parse_str(id).map_err(|_| DatabaseError::InvalidInput)?;
        sqlx::query("UPDATE openlegal.corpus_job SET status=CASE WHEN $2 AND attempts<3 THEN 'pending' ELSE 'failed' END,lease_until=NULL,error_category='processing_failed' WHERE id=$1 AND status='running'").bind(id).bind(retry).execute(&self.pool).await.map_err(db)?;
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
        if object.dataset == Dataset::Precedent {
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
        sqlx::query_scalar("SELECT NOT EXISTS(SELECT 1 FROM openlegal.corpus_object o LEFT JOIN openlegal.corpus_capture c ON c.id=o.head_capture WHERE o.identity->>'jurisdiction'='kr' AND o.identity->>'provider'='law_go_kr' AND NOT o.withdrawn AND o.desired_head_revision IS NOT NULL AND (o.head_capture IS NULL OR o.pending OR c.revision_id IS DISTINCT FROM o.desired_head_revision OR o.validated_at IS NULL OR o.validated_at>$1::text::numeric OR o.validated_at<=$1::text::numeric-86400 OR EXISTS(SELECT 1 FROM openlegal.corpus_job j WHERE j.object_key=o.object_key AND j.install_head AND j.revision_id=o.desired_head_revision AND j.status IN ('pending','running','failed'))))").bind(now.to_string()).fetch_one(&self.pool).await.map_err(db)
    }
    pub async fn mark_dataset_inventory_complete(
        &self,
        dataset: Dataset,
        complete: bool,
    ) -> Result<(), DatabaseError> {
        self.gate().await?;
        if dataset == Dataset::Precedent {
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
    pub async fn fail_claim(&self, job: &Job, retry: bool) -> Result<(), DatabaseError> {
        let id = Uuid::parse_str(&job.id).map_err(|_| DatabaseError::InvalidInput)?;
        sqlx::query("UPDATE openlegal.corpus_job SET status=CASE WHEN $2 AND attempts<3 THEN 'pending' ELSE 'failed' END,lease_until=NULL,error_category='processing_failed' WHERE id=$1 AND expected_version=$3 AND attempts=$4 AND status='running'").bind(id).bind(retry).bind(i64::try_from(job.expected_version).map_err(|_|DatabaseError::InvalidInput)?).bind(job.attempts as i32).execute(&self.pool).await.map_err(db)?;
        Ok(())
    }
    pub async fn mark_inventory_complete(
        &self,
        object: &ObjectId,
        complete: bool,
    ) -> Result<(), DatabaseError> {
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
        sqlx::query("INSERT INTO openlegal.corpus_outbox SELECT next_event,$1,$2,NULL,true,true,false FROM openlegal.corpus_control").bind(k).bind(version).execute(&mut *tx).await.map_err(db)?;
        sqlx::query("UPDATE openlegal.corpus_control SET next_event=next_event+1")
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        sqlx::query("UPDATE openlegal.corpus_session SET invalidated=true WHERE NOT invalidated")
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
        if !(1..=100).contains(&limit) {
            return Err(DatabaseError::InvalidInput);
        }
        let state = self.state(&object).await?;
        if state.withdrawn {
            return Err(DatabaseError::Withdrawn);
        }
        if object.dataset == Dataset::Precedent && matches!(kind, HistoryKind::Revisions) {
            return Err(DatabaseError::UnsupportedHistory);
        }
        let k = key(&object)?;
        let tag = if matches!(kind, HistoryKind::Revisions) {
            "r"
        } else {
            "c"
        };
        let before = if let Some(cursor) = cursor {
            let parts: Vec<_> = cursor.split(':').collect();
            if parts.len() != 4
                || parts[0] != k
                || parts[1] != tag
                || parts[2] != format!("{}.{}", state.version, state.catalog_version)
            {
                return Err(DatabaseError::SnapshotInvalidated);
            }
            parts[3]
                .parse::<i64>()
                .map_err(|_| DatabaseError::InvalidInput)?
        } else {
            i64::MAX
        };
        let sql = if matches!(kind, HistoryKind::Revisions) {
            "SELECT revision_id,latest_capture AS capture_id,last_sequence AS sequence,captured_at::text,publication_date,effective_date FROM openlegal.corpus_revision WHERE object_key=$1 AND last_sequence<$2 ORDER BY last_sequence DESC LIMIT $3"
        } else {
            "SELECT revision_id,id AS capture_id,sequence,captured_at::text,publication_date,effective_date FROM openlegal.corpus_capture_catalog WHERE object_key=$1 AND sequence<$2 ORDER BY sequence DESC LIMIT $3"
        };
        let rows = sqlx::query(sql)
            .bind(&k)
            .bind(before)
            .bind((limit + 1) as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(db)?;
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
                format!(
                    "{k}:{tag}:{}.{}:{}",
                    state.version, state.catalog_version, e.sequence
                )
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
    /// One bounded pass. Current HEADs and live index/session pins are protected.
    /// Metadata removal is committed before physical blob deletion.
    pub async fn maintain(&self, now: u64, historical_before: u64) -> Result<usize, DatabaseError> {
        self.gate().await?;
        let mut tx = self.pool.begin().await.map_err(db)?;
        sqlx::query("SELECT singleton FROM openlegal.corpus_control WHERE singleton FOR UPDATE")
            .fetch_one(&mut *tx)
            .await
            .map_err(db)?;
        sqlx::query("DELETE FROM openlegal.corpus_session WHERE expires_at<=$1::text::numeric OR invalidated").bind(now.to_string()).execute(&mut *tx).await.map_err(db)?;
        let candidates=sqlx::query("SELECT c.id,c.object_key,o.version FROM openlegal.corpus_capture c JOIN openlegal.corpus_object o USING(object_key) WHERE c.captured_at<$1::text::numeric AND c.id IS DISTINCT FROM o.head_capture AND NOT EXISTS(SELECT 1 FROM openlegal.corpus_retirement t WHERE t.capture_id=c.id) ORDER BY c.captured_at,c.id LIMIT 128").bind(historical_before.to_string()).fetch_all(&mut *tx).await.map_err(db)?;
        for row in candidates {
            let id: String = row.try_get("id").map_err(db)?;
            let event:i64=sqlx::query_scalar("UPDATE openlegal.corpus_control SET next_event=next_event+1 RETURNING next_event-1").fetch_one(&mut *tx).await.map_err(db)?;
            sqlx::query("INSERT INTO openlegal.corpus_retirement VALUES($1,$2)")
                .bind(&id)
                .bind(event)
                .execute(&mut *tx)
                .await
                .map_err(db)?;
            sqlx::query("INSERT INTO openlegal.corpus_outbox VALUES($1,$2,$3,$4,false,false,true)")
                .bind(event)
                .bind(row.try_get::<String, _>("object_key").map_err(db)?)
                .bind(row.try_get::<i64, _>("version").map_err(db)?)
                .bind(id)
                .execute(&mut *tx)
                .await
                .map_err(db)?;
        }
        let rows=sqlx::query("SELECT c.id,c.object_key,c.storage_key,c.raw_sha256,c.raw_size FROM openlegal.corpus_capture c JOIN openlegal.corpus_object o USING(object_key) WHERE c.captured_at<$1::text::numeric AND EXISTS(SELECT 1 FROM openlegal.corpus_retirement t JOIN openlegal.corpus_control x ON true WHERE t.capture_id=c.id AND t.event_sequence<=x.index_ack) AND c.id IS DISTINCT FROM o.head_capture AND NOT EXISTS(SELECT 1 FROM openlegal.corpus_pin p WHERE p.capture_id=c.id) AND NOT EXISTS(SELECT 1 FROM openlegal.corpus_session s WHERE s.generation>=c.event_sequence AND s.expires_at>$2::text::numeric AND NOT s.invalidated) ORDER BY c.captured_at,c.id LIMIT 128 FOR UPDATE OF c").bind(historical_before.to_string()).bind(now.to_string()).fetch_all(&mut *tx).await.map_err(db)?;
        let count = rows.len();
        for row in rows {
            let id: String = row.try_get("id").map_err(db)?;
            let mut size: i64 = row.try_get("raw_size").map_err(db)?;
            let attachments = sqlx::query(
                "SELECT raw_size FROM openlegal.corpus_capture_blob WHERE capture_id=$1",
            )
            .bind(&id)
            .fetch_all(&mut *tx)
            .await
            .map_err(db)?;
            for a in attachments {
                size += a.try_get::<i64, _>("raw_size").map_err(db)?;
            }
            sqlx::query("INSERT INTO openlegal.corpus_blob_deletion SELECT storage_key,raw_sha256,raw_size FROM openlegal.corpus_capture_blob WHERE capture_id=$1 ON CONFLICT DO NOTHING").bind(&id).execute(&mut *tx).await.map_err(db)?;
            sqlx::query("INSERT INTO openlegal.corpus_blob_deletion VALUES($1,$2,$3) ON CONFLICT DO NOTHING").bind(row.try_get::<String,_>("storage_key").map_err(db)?).bind(row.try_get::<Vec<u8>,_>("raw_sha256").map_err(db)?).bind(row.try_get::<i64,_>("raw_size").map_err(db)?).execute(&mut *tx).await.map_err(db)?;
            sqlx::query(
                "UPDATE openlegal.corpus_revision SET latest_capture=NULL WHERE latest_capture=$1",
            )
            .bind(&id)
            .execute(&mut *tx)
            .await
            .map_err(db)?;
            sqlx::query("DELETE FROM openlegal.corpus_retirement WHERE capture_id=$1")
                .bind(&id)
                .execute(&mut *tx)
                .await
                .map_err(db)?;
            sqlx::query("DELETE FROM openlegal.corpus_capture WHERE id=$1")
                .bind(&id)
                .execute(&mut *tx)
                .await
                .map_err(db)?;
            sqlx::query("UPDATE openlegal.corpus_control SET raw_bytes=raw_bytes-$1,historical_bytes=historical_bytes-$1")
                .bind(size)
                .execute(&mut *tx)
                .await
                .map_err(db)?;
            sqlx::query("UPDATE openlegal.corpus_object SET version=version+1 WHERE object_key=$1")
                .bind(row.try_get::<String, _>("object_key").map_err(db)?)
                .execute(&mut *tx)
                .await
                .map_err(db)?;
        }
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
        let deletions = sqlx::query("SELECT * FROM openlegal.corpus_blob_deletion LIMIT 128")
            .fetch_all(&self.pool)
            .await
            .map_err(db)?;
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
            sqlx::query("DELETE FROM openlegal.corpus_blob_deletion WHERE storage_key=$1")
                .bind(location.storage_key)
                .execute(&self.pool)
                .await
                .map_err(db)?;
        }
        Ok(count)
    }
}
