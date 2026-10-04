//! Durable progress over finite canonical inventory views. This never upgrades
//! application coverage to a claim of an atomic upstream snapshot.
use super::*;
use crate::law_go_kr::{InventoryPage, catalog};
use futures::TryStreamExt;
#[derive(Clone, Copy, Debug)]
pub struct CloneView {
    pub dataset: Dataset,
    pub historical: bool,
    pub treaty_class: Option<u8>,
}
impl CloneView {
    pub fn key(self) -> Result<String, DatabaseError> {
        if (self.dataset == Dataset::Treaty && self.treaty_class.is_none())
            || self
                .treaty_class
                .is_some_and(|c| self.dataset != Dataset::Treaty || !matches!(c, 1 | 2))
        {
            return Err(DatabaseError::InvalidInput);
        }
        if self.historical
            && !matches!(
                catalog::source_family(self.dataset).history_mode,
                catalog::HistoryMode::StatuteEffective | catalog::HistoryMode::CurrentHistory
            )
        {
            return Err(DatabaseError::UnsupportedHistory);
        }
        Ok(format!(
            "{}:{}:{}",
            self.dataset.as_str(),
            self.historical,
            self.treaty_class.unwrap_or(0)
        ))
    }
    pub fn all() -> Vec<Self> {
        let mut views = Vec::new();
        for dataset in Dataset::ALL.iter().copied() {
            if dataset == Dataset::Treaty {
                for class in [1, 2] {
                    views.push(Self {
                        dataset,
                        historical: false,
                        treaty_class: Some(class),
                    });
                }
            } else {
                views.push(Self {
                    dataset,
                    historical: false,
                    treaty_class: None,
                });
            }
            if matches!(
                catalog::source_family(dataset).history_mode,
                catalog::HistoryMode::StatuteEffective | catalog::HistoryMode::CurrentHistory
            ) {
                views.push(Self {
                    dataset,
                    historical: true,
                    treaty_class: None,
                });
            }
        }
        views
    }
}
impl PgCorpusStore {
    pub async fn clone_cursor(&self, view: CloneView) -> Result<(u32, usize), DatabaseError> {
        self.gate().await?;
        let k = view.key()?;
        sqlx::query("INSERT INTO openlegal.provider_clone_view(view_key,dataset,historical,treaty_class) VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING")
            .bind(&k).bind(view.dataset.as_str()).bind(view.historical).bind(view.treaty_class.map(i16::from)).execute(&self.pool).await.map_err(db)?;
        let (page, offset): (i32, i32) = sqlx::query_as(
            "SELECT next_page,item_offset FROM openlegal.provider_clone_view WHERE view_key=$1",
        )
        .bind(k)
        .fetch_one(&self.pool)
        .await
        .map_err(db)?;
        Ok((
            page.try_into().map_err(corrupt)?,
            offset.try_into().map_err(corrupt)?,
        ))
    }
    /// Moving offset pages must restart scheduling if the exact inventory rows
    /// changed since a partial page was saved. A worker never skips new entries.
    pub async fn clone_page_offset(
        &self,
        view: CloneView,
        page: u32,
        result: &InventoryPage,
    ) -> Result<usize, DatabaseError> {
        self.gate().await?;
        let digest = bytes_hash(&serde_json::to_vec(&result.items).map_err(corrupt)?);
        let mut tx = self.pool.begin().await.map_err(db)?;
        let k = view.key()?;
        let row=sqlx::query("SELECT next_page,item_offset,page_digest FROM openlegal.provider_clone_view WHERE view_key=$1 FOR UPDATE").bind(&k).fetch_one(&mut *tx).await.map_err(db)?;
        if row.try_get::<i32, _>("next_page").map_err(db)? as u32 != page {
            return Err(DatabaseError::Conflict);
        }
        let previous: Option<Vec<u8>> = row.try_get("page_digest").map_err(db)?;
        let offset = if previous.as_ref() == Some(&digest) {
            row.try_get::<i32, _>("item_offset").map_err(db)? as usize
        } else {
            0
        };
        sqlx::query("UPDATE openlegal.provider_clone_view SET item_offset=$2,page_digest=$3 WHERE view_key=$1").bind(k).bind(offset as i32).bind(digest).execute(&mut *tx).await.map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(offset)
    }
    pub async fn clone_page_scheduled(
        &self,
        view: CloneView,
        page: u32,
        offset: usize,
        result: &InventoryPage,
        now: u64,
        cancel: &CancellationToken,
    ) -> Result<(), DatabaseError> {
        self.gate().await?;
        if offset > result.items.len()
            || result
                .items
                .iter()
                .any(|i| i.object.dataset != view.dataset)
        {
            return Err(DatabaseError::InvalidInput);
        }
        let k = view.key()?;
        let mut tx = self.pool.begin().await.map_err(db)?;
        let row =
            sqlx::query("SELECT * FROM openlegal.provider_clone_view WHERE view_key=$1 FOR UPDATE")
                .bind(&k)
                .fetch_one(&mut *tx)
                .await
                .map_err(db)?;
        if row.try_get::<i32, _>("next_page").map_err(db)? as u32 != page {
            return Err(DatabaseError::Conflict);
        }
        let saved: i32 = row.try_get("item_offset").map_err(db)?;
        if offset < saved as usize {
            return Err(DatabaseError::Conflict);
        }
        let cycle: i64 = row.try_get("cycle").map_err(db)?;
        let total = result
            .total
            .map(i64::try_from)
            .transpose()
            .map_err(corrupt)?;
        let expected: Option<i64> = row.try_get("expected_total").map_err(db)?;
        let invalid = row.try_get::<bool, _>("cycle_invalid").map_err(db)?
            || result.incomplete
            || result.rejected_rows > 0
            || total.is_none()
            || (page > 1 && expected != total);
        let required = !catalog::source_family(view.dataset).metadata_only;
        for item in &result.items {
            sqlx::query("INSERT INTO openlegal.provider_clone_member VALUES($1,$2,$3,$4,$5) ON CONFLICT(view_key,object_key,revision_id) DO UPDATE SET seen_cycle=EXCLUDED.seen_cycle,required_body=EXCLUDED.required_body")
                .bind(&k).bind(key(&item.object)?).bind(&item.revision_id).bind(cycle).bind(required).execute(&mut *tx).await.map_err(db)?;
        }
        let complete_page = offset == result.items.len();
        if complete_page && result.done {
            let mut members=sqlx::query("SELECT object_key,revision_id FROM openlegal.provider_clone_member WHERE view_key=$1 AND seen_cycle=$2 ORDER BY object_key,revision_id")
                .bind(&k).bind(cycle).fetch(&mut *tx);
            let mut hasher = Sha256::new();
            let mut count = 0i64;
            while let Some(m) = members.try_next().await.map_err(db)? {
                check(cancel)?;
                count = count.checked_add(1).ok_or(DatabaseError::Capacity)?;
                for field in ["object_key", "revision_id"] {
                    let value: String = m.try_get(field).map_err(db)?;
                    hasher.update((value.len() as u64).to_be_bytes());
                    hasher.update(value.as_bytes());
                }
            }
            drop(members);
            let complete = !invalid && total == Some(count);
            let digest = hasher.finalize().to_vec();
            let previous: Option<Vec<u8>> = row.try_get("last_digest").map_err(db)?;
            let stable = if !complete {
                0
            } else if previous.as_ref() == Some(&digest) {
                row.try_get::<i32, _>("stable_cycles")
                    .map_err(db)?
                    .saturating_add(1)
            } else {
                1
            };
            sqlx::query("UPDATE openlegal.provider_clone_view SET next_page=1,item_offset=0,page_digest=NULL,cycle=cycle+1,expected_total=NULL,cycle_invalid=false,last_digest=$2,stable_cycles=$3,last_completed_at=$4,last_observed_at=$4 WHERE view_key=$1")
                .bind(&k).bind(complete.then_some(digest)).bind(stable).bind(i64::try_from(now).map_err(corrupt)?).execute(&mut *tx).await.map_err(db)?;
        } else {
            let next = if complete_page {
                page.checked_add(1).ok_or(DatabaseError::Capacity)?
            } else {
                page
            };
            sqlx::query("UPDATE openlegal.provider_clone_view SET next_page=$2,item_offset=$3,page_digest=CASE WHEN $2<>next_page THEN NULL ELSE page_digest END,expected_total=$4,cycle_invalid=$5,last_observed_at=$6 WHERE view_key=$1")
                .bind(&k).bind(i32::try_from(next).map_err(corrupt)?).bind(if complete_page {0} else {i32::try_from(offset).map_err(corrupt)?}).bind(if page==1 {total} else {expected}).bind(invalid).bind(i64::try_from(now).map_err(corrupt)?).execute(&mut *tx).await.map_err(db)?;
        }
        check(cancel)?;
        tx.commit().await.map_err(db)?;
        Ok(())
    }
    pub async fn clone_progress(&self) -> Result<serde_json::Value, DatabaseError> {
        self.gate().await?;
        let rows=sqlx::query("SELECT v.*, (SELECT count(*) FROM openlegal.provider_clone_member m WHERE m.view_key=v.view_key AND m.seen_cycle>=v.cycle-1) AS observed, (SELECT count(*) FROM openlegal.provider_clone_member m WHERE m.view_key=v.view_key AND m.seen_cycle>=v.cycle-1 AND m.required_body AND (NOT EXISTS(SELECT 1 FROM openlegal.corpus_revision r JOIN openlegal.corpus_capture c ON c.id=r.latest_capture WHERE r.object_key=m.object_key AND r.revision_id=m.revision_id AND COALESCE(c.payload->'record'->'metadata'->>'attachment_status','')<>'incomplete' AND COALESCE(c.payload->'record'->'metadata'->>'body_status','')<>'response_identity_unverified_metadata_only') OR EXISTS(SELECT 1 FROM openlegal.corpus_job j WHERE j.object_key=m.object_key AND j.revision_id=m.revision_id AND j.status='failed'))) AS missing FROM openlegal.provider_clone_view v ORDER BY view_key").fetch_all(&self.pool).await.map_err(db)?;
        let mut views = Vec::new();
        let expected: std::collections::BTreeSet<String> = CloneView::all()
            .into_iter()
            .map(CloneView::key)
            .collect::<Result<_, _>>()?;
        let actual: std::collections::BTreeSet<String> = rows
            .iter()
            .map(|r| r.try_get::<String, _>("view_key").map_err(db))
            .collect::<Result<_, _>>()?;
        let mut complete = expected == actual;
        for row in rows {
            let stable: i32 = row.try_get("stable_cycles").map_err(db)?;
            let missing: i64 = row.try_get("missing").map_err(db)?;
            complete &= stable >= 2
                && missing == 0
                && !row.try_get::<bool, _>("cycle_invalid").map_err(db)?;
            views.push(serde_json::json!({"view":row.try_get::<String,_>("view_key").map_err(db)?,"next_page":row.try_get::<i32,_>("next_page").map_err(db)?,"cycle":row.try_get::<i64,_>("cycle").map_err(db)?,"stable_cycles":stable,"observed":row.try_get::<i64,_>("observed").map_err(db)?,"missing_bodies":missing,"metadata_only":catalog::source_family(Dataset::from_name(&row.try_get::<String,_>("dataset").map_err(db)?).ok_or(DatabaseError::StorageCorrupt)?).metadata_only}));
        }
        let gaps: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM openlegal.provider_collection_gap WHERE resolved_at IS NULL",
        )
        .fetch_one(&self.pool)
        .await
        .map_err(db)?;
        let jobs: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM openlegal.corpus_job WHERE status IN ('pending','running')",
        )
        .fetch_one(&self.pool)
        .await
        .map_err(db)?;
        let index_ready: bool = sqlx::query_scalar(
            "SELECT index_ack=next_event-1 FROM openlegal.corpus_control WHERE singleton",
        )
        .fetch_one(&self.pool)
        .await
        .map_err(db)?;
        complete &= gaps == 0 && jobs == 0 && index_ready;
        let supplementary = self.supplement_progress().await?;
        let guide_coverage = crate::law_go_kr::supplements::guide_coverage();
        let unresolved_guides = guide_coverage
            .iter()
            .filter(|g| {
                matches!(
                    g.status,
                    crate::law_go_kr::supplements::GuideCoverageStatus::NeedsVerification { .. }
                )
            })
            .count();
        Ok(
            serde_json::json!({"initial_canonical_clone_complete":complete,"full_available_clone_complete":complete && unresolved_guides==0 && supplementary["complete"]==true,"atomic_upstream_snapshot":false,"open_gaps":gaps,"active_jobs":jobs,"index_ready":index_ready,"views":views,"supplementary":supplementary,"unresolved_guides":unresolved_guides,"guide_coverage":guide_coverage}),
        )
    }
}
