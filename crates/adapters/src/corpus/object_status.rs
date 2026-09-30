use super::*;

impl PgCorpusStore {
    /// Local evidence and job state only. This method never contacts a provider.
    pub async fn object_status(
        &self,
        object: &ObjectId,
        now: u64,
    ) -> Result<ObjectStatus, DatabaseError> {
        let state = self.state(object).await?;
        let mut status = ObjectStatus {
            schema_version: 1,
            object: object.clone(),
            state: ObjectCollectionState::NotObserved,
            head_capture_id: state.head_capture.clone(),
            indexed: None,
            job: None,
            retry_at: None,
            eta: ObjectCompletionEta::Unknown {
                reason: "not_observed".into(),
            },
        };
        if !state.observed {
            return Ok(status);
        }
        let object_key = key(object)?;
        if let Some(head) = &state.head_capture {
            let event: Option<i64> = sqlx::query_scalar(
                "SELECT max(sequence) FROM openlegal.corpus_outbox WHERE object_key=$1 AND capture_id=$2 AND is_head AND NOT removed",
            )
            .bind(&object_key)
            .bind(head)
            .fetch_one(&self.pool)
            .await
            .map_err(db)?;
            let ack: i64 = sqlx::query_scalar(
                "SELECT index_ack FROM openlegal.corpus_control WHERE singleton",
            )
            .fetch_one(&self.pool)
            .await
            .map_err(db)?;
            status.indexed = Some(event.is_some_and(|sequence| sequence <= ack));
        }
        let job = sqlx::query(
            "SELECT j.id::text AS id,j.status,j.attempts,j.created_at::text AS created_at,j.started_at::text AS started_at,j.completed_at::text AS completed_at,j.error_category FROM openlegal.corpus_job j JOIN openlegal.corpus_object o ON o.object_key=j.object_key WHERE j.object_key=$1 AND j.install_head AND j.revision_id=o.desired_head_revision ORDER BY j.created_at DESC,j.id DESC LIMIT 1",
        )
        .bind(&object_key)
        .fetch_optional(&self.pool)
        .await
        .map_err(db)?;
        if let Some(row) = job {
            status.job = Some(ObjectJobStatus {
                id: row.try_get("id").map_err(db)?,
                status: row.try_get("status").map_err(db)?,
                attempts: row
                    .try_get::<i32, _>("attempts")
                    .map_err(db)?
                    .try_into()
                    .map_err(corrupt)?,
                created_at: row
                    .try_get::<String, _>("created_at")
                    .map_err(db)?
                    .parse()
                    .map_err(corrupt)?,
                started_at: row
                    .try_get::<Option<String>, _>("started_at")
                    .map_err(db)?
                    .map(|value| value.parse().map_err(corrupt))
                    .transpose()?,
                completed_at: row
                    .try_get::<Option<String>, _>("completed_at")
                    .map_err(db)?
                    .map(|value| value.parse().map_err(corrupt))
                    .transpose()?,
                error_category: row.try_get("error_category").map_err(db)?,
            });
        }
        status.retry_at = sqlx::query_scalar::<_, String>(
            "SELECT g.retry_at::text FROM openlegal.provider_collection_gap g JOIN openlegal.corpus_object o ON o.object_key=g.object_key WHERE g.scope='detail' AND g.object_key=$1 AND g.revision_id=o.desired_head_revision AND g.resolved_at IS NULL ORDER BY g.retry_at LIMIT 1",
        )
        .bind(&object_key)
        .fetch_optional(&self.pool)
        .await
        .map_err(db)?
        .map(|value| value.parse().map_err(corrupt))
        .transpose()?;

        status.state = if state.withdrawn {
            ObjectCollectionState::Withdrawn
        } else if status
            .job
            .as_ref()
            .is_some_and(|job| matches!(job.status.as_str(), "pending" | "running"))
        {
            ObjectCollectionState::ProcessingPending
        } else if !state.pending && state.head_capture.is_some() {
            ObjectCollectionState::Published
        } else {
            ObjectCollectionState::CollectionIncomplete
        };
        status.eta = match (&status.state, status.job.as_ref()) {
            (ObjectCollectionState::ProcessingPending, Some(job)) if job.status == "running" => {
                if let Some(started_at) = job.started_at {
                    self.running_eta(object.dataset, now, started_at, &job.id)
                        .await?
                } else {
                    ObjectCompletionEta::Unknown {
                        reason: "missing_start_time".into(),
                    }
                }
            }
            (ObjectCollectionState::ProcessingPending, _) => ObjectCompletionEta::Unknown {
                reason: "not_running".into(),
            },
            (ObjectCollectionState::CollectionIncomplete, _) => ObjectCompletionEta::Unknown {
                reason: "collection_incomplete".into(),
            },
            (ObjectCollectionState::Published, _) => ObjectCompletionEta::Unknown {
                reason: "already_published".into(),
            },
            (ObjectCollectionState::Withdrawn, _) => ObjectCompletionEta::Unknown {
                reason: "withdrawn".into(),
            },
            (ObjectCollectionState::NotObserved, _) => ObjectCompletionEta::Unknown {
                reason: "not_observed".into(),
            },
        };
        Ok(status)
    }

    async fn running_eta(
        &self,
        dataset: Dataset,
        now: u64,
        started_at: u64,
        job_id: &str,
    ) -> Result<ObjectCompletionEta, DatabaseError> {
        let job_id = Uuid::parse_str(job_id).map_err(corrupt)?;
        let origin: Option<String> = sqlx::query_scalar(
            "SELECT source_metadata->>'collection_origin' FROM openlegal.corpus_job WHERE id=$1",
        )
        .bind(job_id)
        .fetch_one(&self.pool)
        .await
        .map_err(db)?;
        let budget = sqlx::query(
            "SELECT utc_day,daily_used,on_demand_used,continuous_daily_limit,on_demand_daily_limit,operator_suspended,unresolved_response FROM openlegal.provider_request_budget WHERE singleton",
        )
        .fetch_one(&self.pool)
        .await
        .map_err(db)?;
        if budget
            .try_get::<bool, _>("operator_suspended")
            .map_err(db)?
            || budget
                .try_get::<bool, _>("unresolved_response")
                .map_err(db)?
        {
            return Ok(ObjectCompletionEta::Unknown {
                reason: "provider_paused".into(),
            });
        }
        let used: i32 = budget
            .try_get(if origin.as_deref() == Some("explicit") {
                "on_demand_used"
            } else {
                "daily_used"
            })
            .map_err(db)?;
        let limit: i32 = budget
            .try_get(if origin.as_deref() == Some("explicit") {
                "on_demand_daily_limit"
            } else {
                "continuous_daily_limit"
            })
            .map_err(db)?;
        if budget.try_get::<i64, _>("utc_day").map_err(db)? == (now / 86_400) as i64
            && used >= limit
        {
            return Ok(ObjectCompletionEta::Unknown {
                reason: "provider_budget_exhausted".into(),
            });
        }
        let dataset = serde_json::to_value(dataset).map_err(corrupt)?;
        let dataset = dataset.as_str().ok_or(DatabaseError::StorageCorrupt)?;
        let rows = sqlx::query(
            "SELECT (j.completed_at-j.started_at)::text AS seconds FROM openlegal.corpus_job j JOIN openlegal.corpus_object o ON o.object_key=j.object_key WHERE j.status='done' AND j.started_at IS NOT NULL AND j.completed_at IS NOT NULL AND j.completed_at>=j.started_at AND j.completed_at>=$1::text::numeric AND o.identity->>'dataset'=$2 ORDER BY j.completed_at DESC LIMIT 256",
        )
        .bind(now.saturating_sub(86400).to_string())
        .bind(dataset)
        .fetch_all(&self.pool)
        .await
        .map_err(db)?;
        let mut durations = rows
            .iter()
            .map(|row| {
                row.try_get::<String, _>("seconds")
                    .map_err(db)?
                    .parse::<u64>()
                    .map_err(corrupt)
            })
            .collect::<Result<Vec<_>, DatabaseError>>()?;
        if durations.len() < 20 {
            return Ok(ObjectCompletionEta::Unknown {
                reason: "insufficient_recent_samples".into(),
            });
        }
        durations.sort_unstable();
        let elapsed = now.saturating_sub(started_at);
        let p25 = durations[(durations.len() - 1) / 4];
        let p90 = durations[(durations.len() - 1) * 9 / 10];
        Ok(ObjectCompletionEta::Range {
            earliest_at: now.saturating_add(p25.saturating_sub(elapsed)),
            latest_at: now.saturating_add(p90.saturating_sub(elapsed)),
            sample_size: durations.len() as u32,
        })
    }
}
