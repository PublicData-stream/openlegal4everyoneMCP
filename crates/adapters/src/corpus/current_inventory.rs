//! Durable ambiguity fences bound to stored canonical current-list observations.
//! Dates and row order never select a HEAD. Clearing a hold is an audited operator action.
use super::*;
use crate::law_go_kr::{InventoryPage, catalog};
use sqlx::{Postgres, Transaction};
use std::collections::{BTreeMap, BTreeSet};

/// Caller owns corpus_control, relevant view rows, and this object in that order.
/// Repeated quarantine preserves the version fence and never refunds attempts.
pub(super) async fn quarantine_current_inventory_object(
    tx: &mut Transaction<'_, Postgres>,
    object_key: &str,
    now: u64,
) -> Result<(), DatabaseError> {
    let changed = sqlx::query("UPDATE openlegal.corpus_object SET version=version+CASE WHEN current_inventory_conflict THEN 0 ELSE 1 END,current_inventory_conflict=true,pending=true,desired_head_revision=NULL WHERE object_key=$1")
        .bind(object_key).execute(&mut **tx).await.map_err(db)?.rows_affected();
    if changed != 1 {
        return Err(DatabaseError::NotObserved);
    }
    sqlx::query("UPDATE openlegal.current_inventory_candidate SET conflict_proof=true WHERE object_key=$1 AND NOT conflict_proof")
        .bind(object_key).execute(&mut **tx).await.map_err(db)?;
    sqlx::query("UPDATE openlegal.corpus_job SET status='failed',error_category='source_data_invalid',lease_until=NULL,completed_at=$2::text::numeric WHERE object_key=$1 AND install_head AND status IN ('pending','running')")
        .bind(object_key).bind(now.to_string()).execute(&mut **tx).await.map_err(db)?;
    Ok(())
}

impl PgCorpusStore {
    /// Registers the complete current page before any of its HEAD jobs may enqueue.
    ///
    /// Candidate checkpoints accumulate across the actual active view cycle. An
    /// already held object admits no new candidate rows, preserving bounded proof.
    /// Pilot scans may observe pages without advancing the continuous cursor.
    pub async fn observe_current_inventory(
        &self,
        view: CloneView,
        page: u32,
        result: &InventoryPage,
        now: u64,
    ) -> Result<Vec<ObjectId>, DatabaseError> {
        self.gate().await?;
        if view.historical || !(1..=1_000_000).contains(&page) {
            return Err(DatabaseError::InvalidInput);
        }
        let view_key = view.key()?;
        let expected = result.current_revision_metadata()?;
        if result
            .items
            .iter()
            .any(|item| item.object.dataset != view.dataset)
        {
            return Err(DatabaseError::InvalidInput);
        }
        let supplied = result
            .source_observation
            .as_ref()
            .ok_or(DatabaseError::InvalidInput)?;
        let stored = self
            .source_observation(&supplied.observation_id, CancellationToken::new())
            .await?;
        let mut supplied_immutable = supplied.clone();
        supplied_immutable.validated_at = stored.validated_at;
        let family = catalog::source_family(view.dataset);
        let source_key = format!(
            "law_go_kr:{}:{}:false:{}:{}:current_revisions_v1",
            family.list_guide,
            view.dataset.as_str(),
            view.treaty_class.unwrap_or(0),
            page
        );
        let bound: BTreeMap<_, _> = stored
            .metadata
            .iter()
            .filter(|(name, _)| name.starts_with("current_revision_"))
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect();
        if supplied_immutable != stored
            || supplied.validated_at < supplied.observed_at
            || stored.source_key != source_key
            || stored.metadata.get("dataset").map(String::as_str) != Some(view.dataset.as_str())
            || stored.metadata.get("historical").map(String::as_str) != Some("false")
            || stored.metadata.get("page").map(String::as_str) != Some(page.to_string().as_str())
            || stored.validated_at < supplied.validated_at
            || supplied.validated_at > now
            || bound != expected
        {
            return Err(DatabaseError::InvalidInput);
        }
        let mut objects = BTreeMap::new();
        for item in &result.items {
            let object_key = key(&item.object)?;
            if let Some(previous) = objects.insert(object_key, item.object.clone())
                && previous != item.object
            {
                return Err(DatabaseError::StorageCorrupt);
            }
        }
        // This initialization is an independent committed metadata operation;
        // the following transaction always locks corpus_control before the view.
        self.clone_cursor(view).await?;
        let cancel = CancellationToken::new();
        retry_storage(&cancel, "current_inventory.observe", || async {
            let mut tx = begin_storage(&self.pool, "current_inventory.observe").await?;
            sqlx::query("SELECT singleton FROM openlegal.corpus_control WHERE singleton FOR UPDATE")
                .fetch_one(&mut *tx).await.map_err(db)?;
            let state = sqlx::query("SELECT cycle,dataset,historical,treaty_class FROM openlegal.provider_clone_view WHERE view_key=$1 FOR UPDATE")
                .bind(&view_key).fetch_one(&mut *tx).await.map_err(db)?;
            let cycle: i64 = state.try_get("cycle").map_err(db)?;
            if cycle < 1
                || state.try_get::<String, _>("dataset").map_err(db)? != view.dataset.as_str()
                || state.try_get::<bool, _>("historical").map_err(db)?
                || state.try_get::<Option<i16>, _>("treaty_class").map_err(db)? != view.treaty_class.map(i16::from)
            {
                return Err(DatabaseError::StorageCorrupt);
            }
            let mut held = BTreeSet::new();
            for (object_key, object) in &objects {
                sqlx::query("INSERT INTO openlegal.corpus_object(object_key,identity) VALUES($1,$2) ON CONFLICT DO NOTHING")
                    .bind(object_key).bind(serde_json::to_value(object).map_err(corrupt)?)
                    .execute(&mut *tx).await.map_err(db)?;
                let row = sqlx::query("SELECT identity,current_inventory_conflict FROM openlegal.corpus_object WHERE object_key=$1 FOR UPDATE")
                    .bind(object_key).fetch_one(&mut *tx).await.map_err(db)?;
                let identity: ObjectId = serde_json::from_value(row.try_get("identity").map_err(db)?).map_err(corrupt)?;
                if identity != *object { return Err(DatabaseError::StorageCorrupt); }
                if row.try_get::<bool, _>("current_inventory_conflict").map_err(db)? {
                    held.insert(object_key.clone());
                }
            }
            sqlx::query("DELETE FROM openlegal.current_inventory_candidate c USING openlegal.corpus_object o WHERE c.object_key=o.object_key AND c.view_key=$1 AND c.cycle<$2 AND NOT o.current_inventory_conflict AND NOT c.conflict_proof")
                .bind(&view_key).bind(cycle).execute(&mut *tx).await.map_err(db)?;
            // No per-item enqueue occurs until every checkpoint is registered.
            for item in &result.items {
                let object_key = key(&item.object)?;
                if held.contains(&object_key) { continue; }
                sqlx::query("INSERT INTO openlegal.current_inventory_candidate(view_key,cycle,object_key,revision_id,effective_date,page,observed_at,source_observation_id) VALUES($1,$2,$3,$4,$5,$6,$7::text::numeric,$8) ON CONFLICT(view_key,cycle,object_key,revision_id,effective_date) DO NOTHING")
                    .bind(&view_key).bind(cycle).bind(&object_key).bind(&item.revision_id)
                    .bind(&item.effective_date).bind(i32::try_from(page).map_err(corrupt)?)
                    .bind(supplied.validated_at.to_string()).bind(&stored.observation_id)
                    .execute(&mut *tx).await.map_err(db)?;
            }
            let keys: Vec<_> = objects.keys().cloned().collect();
            let conflicts: Vec<String> = sqlx::query_scalar("SELECT object_key FROM openlegal.current_inventory_candidate WHERE view_key=$1 AND cycle=$2 AND object_key=ANY($3::text[]) GROUP BY object_key HAVING count(*)>1 ORDER BY object_key")
                .bind(&view_key).bind(cycle).bind(&keys).fetch_all(&mut *tx).await.map_err(db)?;
            held.extend(conflicts);
            for object_key in &held {
                quarantine_current_inventory_object(&mut tx, object_key, now).await?;
            }
            commit_storage(tx, "current_inventory.observe").await?;
            let mut returned = BTreeSet::new();
            let mut held_objects = Vec::new();
            for item in &result.items {
                let object_key = key(&item.object)?;
                if held.contains(&object_key) && returned.insert(object_key) {
                    held_objects.push(item.object.clone());
                }
            }
            Ok(held_objects)
        }).await
    }

    /// Current-only search excludes a held HEAD and an audited release still
    /// awaiting a fresh, uniquely observed HEAD. Capture/history reads are separate.
    pub async fn current_inventory_search_blocked(
        &self,
        object: &ObjectId,
    ) -> Result<bool, DatabaseError> {
        self.gate().await?;
        let row = sqlx::query("SELECT identity,current_inventory_conflict OR (pending AND desired_head_revision IS NULL) AS blocked FROM openlegal.corpus_object WHERE object_key=$1")
            .bind(key(object)?).fetch_optional(&self.pool).await.map_err(db)?;
        let Some(row) = row else {
            return Ok(false);
        };
        let identity: ObjectId =
            serde_json::from_value(row.try_get("identity").map_err(db)?).map_err(corrupt)?;
        if identity != *object {
            return Err(DatabaseError::StorageCorrupt);
        }
        row.try_get("blocked").map_err(db)
    }

    /// Unobserved objects are not held; a hold remains sticky until audited release.
    pub async fn head_inventory_conflicted(
        &self,
        object: &ObjectId,
    ) -> Result<bool, DatabaseError> {
        self.gate().await?;
        let row = sqlx::query("SELECT identity,current_inventory_conflict FROM openlegal.corpus_object WHERE object_key=$1")
            .bind(key(object)?).fetch_optional(&self.pool).await.map_err(db)?;
        let Some(row) = row else {
            return Ok(false);
        };
        let identity: ObjectId =
            serde_json::from_value(row.try_get("identity").map_err(db)?).map_err(corrupt)?;
        if identity != *object {
            return Err(DatabaseError::StorageCorrupt);
        }
        row.try_get("current_inventory_conflict").map_err(db)
    }
}
