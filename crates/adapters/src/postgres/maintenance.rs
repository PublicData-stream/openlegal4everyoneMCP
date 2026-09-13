use super::data::location;
use super::*;
use uuid::Uuid;

pub(super) struct EvictionBarrier {
    store: PostgresStore,
}
impl EvictionBarrier {
    fn enter(store: &PostgresStore) -> Result<Self, Error> {
        store
            .inner
            .state
            .compare_exchange(READY, MAINTAINING, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| Error::Busy)?;
        store.inner.epoch.fetch_add(1, Ordering::AcqRel);
        Ok(Self {
            store: store.clone(),
        })
    }
}
impl Drop for EvictionBarrier {
    fn drop(&mut self) {
        self.store.inner.epoch.fetch_add(1, Ordering::AcqRel);
        let _ = self.store.inner.state.compare_exchange(
            MAINTAINING,
            READY,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
    }
}
#[derive(Clone, Copy)]
struct Accounting {
    snapshots: i64,
    queries: i64,
    bytes: i64,
}
impl Accounting {
    fn fits(self, policy: &RetentionPolicy) -> bool {
        self.snapshots <= policy.max_snapshots as i64
            && self.queries <= policy.max_queries as i64
            && self.bytes <= policy.max_blob_bytes as i64
    }
}
impl PostgresStore {
    async fn totals(connection: &mut PgConnection) -> Result<Accounting, Error> {
        let (snapshots,queries,bytes):(i64,i64,i64)=sqlx::query_as("SELECT (SELECT count(*)::bigint FROM openlegal.cache_snapshot),(SELECT count(*)::bigint FROM openlegal.cache_query),COALESCE((SELECT sum(b.size_bytes)::bigint FROM openlegal.blob_object b WHERE EXISTS(SELECT 1 FROM openlegal.cache_snapshot s WHERE s.raw_blob_sha256=b.sha256)),0)")
            .fetch_one(connection).await.map_err(database_error)?;
        Ok(Accounting {
            snapshots,
            queries,
            bytes,
        })
    }
    pub(super) async fn refresh_gauges(&self) -> Result<(), Error> {
        let row=sqlx::query("SELECT snapshots,queries,referenced_bytes,(SELECT COALESCE(sum(size_bytes),0)::bigint FROM openlegal.blob_object WHERE NOT ready) AS staging_bytes,(SELECT count(*)::bigint FROM openlegal.blob_deletion) AS deletion_queue FROM openlegal.cache_storage WHERE singleton")
            .fetch_one(&self.inner.probe_pool).await.map_err(database_error)?;
        let snapshots: i64 = row.try_get("snapshots").map_err(database_error)?;
        let queries: i64 = row.try_get("queries").map_err(database_error)?;
        let bytes: i64 = row.try_get("referenced_bytes").map_err(database_error)?;
        let staging: i64 = row.try_get("staging_bytes").map_err(database_error)?;
        let deletion: i64 = row.try_get("deletion_queue").map_err(database_error)?;
        if [snapshots, queries, bytes, staging, deletion]
            .iter()
            .any(|value| *value < 0)
        {
            return Err(Error::StorageCorrupt);
        };
        self.metric(|m| {
            m.snapshots = snapshots as usize;
            m.queries = queries as usize;
            m.bytes = bytes as u64;
            m.staging_bytes = staging as u64;
            m.deletion_queue = deletion as u64;
        });
        Ok(())
    }
    pub(super) async fn check_accounting(&self, enforce: bool) -> Result<(), Error> {
        // Consistent view under the same short mutation lock; no repair hidden in startup.
        let mut tx = DbTransaction::begin(&self.inner.pool).await?;
        let totals = Self::totals(tx.conn()?).await?;
        let recorded=sqlx::query("SELECT snapshots,queries,referenced_bytes FROM openlegal.cache_storage WHERE singleton").fetch_one(tx.conn()?).await.map_err(database_error)?;
        if recorded
            .try_get::<i64, _>("snapshots")
            .map_err(database_error)?
            != totals.snapshots
            || recorded
                .try_get::<i64, _>("queries")
                .map_err(database_error)?
                != totals.queries
            || recorded
                .try_get::<i64, _>("referenced_bytes")
                .map_err(database_error)?
                != totals.bytes
        {
            return Err(Error::StorageCorrupt);
        };
        if enforce {
            let too_many:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM openlegal.cache_snapshot GROUP BY query_id HAVING count(*)>$1)").bind(self.inner.policy.max_snapshots_per_query as i64).fetch_one(tx.conn()?).await.map_err(database_error)?;
            if !totals.fits(&self.inner.policy) || too_many {
                return Err(Error::StorageCapacity);
            };
        }
        tx.commit().await?;
        self.metric(|m| {
            m.bytes = totals.bytes as u64;
            m.snapshots = totals.snapshots as usize;
            m.queries = totals.queries as usize;
        });
        Ok(())
    }
    pub(super) async fn update_accounting(
        &self,
        tx: &mut DbTransaction,
        enforce: bool,
    ) -> Result<(), Error> {
        let totals = Self::totals(tx.conn()?).await?;
        if enforce {
            let over_query:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM openlegal.cache_snapshot GROUP BY query_id HAVING count(*)>$1)").bind(self.inner.policy.max_snapshots_per_query as i64).fetch_one(tx.conn()?).await.map_err(database_error)?;
            if !totals.fits(&self.inner.policy) || over_query {
                return Err(Error::StorageCapacity);
            };
        }
        sqlx::query("UPDATE openlegal.cache_storage SET snapshots=$1,queries=$2,referenced_bytes=$3 WHERE singleton").bind(totals.snapshots).bind(totals.queries).bind(totals.bytes).execute(tx.conn()?).await.map_err(database_error)?;
        Ok(())
    }
    async fn queue_available(tx: &mut DbTransaction, size: i64) -> Result<(), Error> {
        let (count,bytes):(i64,i64)=sqlx::query_as("SELECT count(*)::bigint,COALESCE(sum(size_bytes),0)::bigint FROM openlegal.blob_deletion").fetch_one(tx.conn()?).await.map_err(database_error)?;
        if count >= 4096 || bytes.saturating_add(size) > 4 * 1024 * 1024 * 1024 {
            return Err(Error::StorageCapacity);
        };
        Ok(())
    }
    async fn retire(
        tx: &mut DbTransaction,
        row: &sqlx::postgres::PgRow,
        now: u64,
    ) -> Result<(), Error> {
        let size: i64 = row.try_get("size_bytes").map_err(database_error)?;
        Self::queue_available(tx, size).await?;
        sqlx::query("INSERT INTO openlegal.blob_deletion(storage_key,sha256,generation,size_bytes,queued_at) VALUES($1,$2,$3,$4,$5::text::numeric) ON CONFLICT(storage_key) DO NOTHING")
            .bind(row.try_get::<String,_>("storage_key").map_err(database_error)?).bind(row.try_get::<Vec<u8>,_>("sha256").map_err(database_error)?).bind(row.try_get::<Uuid,_>("generation").map_err(database_error)?).bind(size).bind(now.to_string()).execute(tx.conn()?).await.map_err(database_error)?;
        sqlx::query("DELETE FROM openlegal.blob_object WHERE sha256=$1")
            .bind(
                row.try_get::<Vec<u8>, _>("sha256")
                    .map_err(database_error)?,
            )
            .execute(tx.conn()?)
            .await
            .map_err(database_error)?;
        Ok(())
    }
    pub(super) async fn trim(
        &self,
        tx: &mut DbTransaction,
        protected: Option<Uuid>,
        now: u64,
        barrier: &mut Option<EvictionBarrier>,
    ) -> Result<u64, Error> {
        let mut evicted = 0;
        for _ in 0..128 {
            // Per-query pressure first keeps the newest occurrences, independently of processor.
            let row=sqlx::query("SELECT s.id,s.query_id FROM openlegal.cache_snapshot s WHERE ($1::uuid IS NULL OR s.id<>$1) AND (s.captured_at<=$2::text::numeric-$3::bigint OR s.query_id IN(SELECT query_id FROM openlegal.cache_snapshot GROUP BY query_id HAVING count(*)>$4)) ORDER BY EXISTS(SELECT 1 FROM openlegal.cache_head h WHERE h.snapshot_id=s.id),s.captured_at,s.sequence,s.id LIMIT 1")
                .bind(protected).bind(now.to_string()).bind((self.inner.policy.retention_days*86400) as i64).bind(self.inner.policy.max_snapshots_per_query as i64).fetch_optional(tx.conn()?).await.map_err(database_error)?;
            let row = if let Some(row) = row {
                Some(row)
            } else {
                let totals = Self::totals(tx.conn()?).await?;
                if totals.fits(&self.inner.policy) {
                    None
                } else {
                    sqlx::query("SELECT s.id,s.query_id FROM openlegal.cache_snapshot s LEFT JOIN openlegal.cache_head h ON h.snapshot_id=s.id WHERE ($1::uuid IS NULL OR s.id<>$1) ORDER BY (h.snapshot_id IS NOT NULL),COALESCE(h.validated_at,s.captured_at),s.sequence,s.id LIMIT 1")
                        .bind(protected).fetch_optional(tx.conn()?).await.map_err(database_error)?
                }
            };
            let Some(row) = row else { break };
            if barrier.is_none() {
                *barrier = Some(EvictionBarrier::enter(self)?);
            }
            let id: Uuid = row.try_get("id").map_err(database_error)?;
            let query_id: Uuid = row.try_get("query_id").map_err(database_error)?;
            sqlx::query("DELETE FROM openlegal.cache_head WHERE snapshot_id=$1")
                .bind(id)
                .execute(tx.conn()?)
                .await
                .map_err(database_error)?;
            sqlx::query("DELETE FROM openlegal.cache_snapshot WHERE id=$1")
                .bind(id)
                .execute(tx.conn()?)
                .await
                .map_err(database_error)?;
            Self::advance_revision(tx, query_id).await?;
            evicted += 1;
            sqlx::query("DELETE FROM openlegal.cache_query q WHERE q.id=$1 AND NOT EXISTS(SELECT 1 FROM openlegal.cache_snapshot s WHERE s.query_id=q.id)").bind(query_id).execute(tx.conn()?).await.map_err(database_error)?;
        }
        // Retire every ready object losing its final reference in this transaction.
        // Evaluate after the complete snapshot delta, so same-digest replacement stays live.
        let orphans=sqlx::query("SELECT sha256,generation,size_bytes,storage_key FROM openlegal.blob_object b WHERE ready AND NOT EXISTS(SELECT 1 FROM openlegal.cache_snapshot s WHERE s.raw_blob_sha256=b.sha256) ORDER BY sha256 LIMIT 129").fetch_all(tx.conn()?).await.map_err(database_error)?;
        if orphans.len() > 128 {
            return Err(Error::StorageCapacity);
        };
        for row in orphans {
            Self::retire(tx, &row, now).await?;
        }
        Ok(evicted)
    }
    pub(super) async fn maintain_inner(&self, now: u64) -> Result<(), Error> {
        self.inner
            .blobs
            .health(self.inner.closing.child_token())
            .await?;
        self.cleanup_queue().await?;
        let mut tx = DbTransaction::begin(&self.inner.pool).await?;
        let mut barrier = None;
        let evicted = self.trim(&mut tx, None, now, &mut barrier).await?;
        let pending=sqlx::query("SELECT sha256,generation,size_bytes,storage_key FROM openlegal.blob_object WHERE NOT ready AND created_at<=$1::text::numeric-60 ORDER BY created_at LIMIT 128").bind(now.to_string()).fetch_all(tx.conn()?).await.map_err(database_error)?;
        for row in pending {
            Self::retire(&mut tx, &row, now).await?;
        }
        self.update_accounting(&mut tx, false).await?;
        tx.commit().await?;
        drop(barrier);
        self.metric(|m| m.evictions += evicted);
        self.cleanup_queue().await?;
        self.enumerate_orphans(now).await?;
        self.inner
            .blobs
            .cleanup_staging(now, 128, self.inner.closing.child_token())
            .await?;
        self.check_accounting(false).await?;
        Ok(())
    }
    async fn cleanup_queue(&self) -> Result<(), Error> {
        let rows=sqlx::query("SELECT sha256,generation,size_bytes,storage_key FROM openlegal.blob_deletion ORDER BY id LIMIT 128").fetch_all(&self.inner.pool).await.map_err(database_error)?;
        for row in rows {
            let location = location(&row)?;
            let live: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM openlegal.blob_object WHERE storage_key=$1)",
            )
            .bind(&location.storage_key)
            .fetch_one(&self.inner.pool)
            .await
            .map_err(database_error)?;
            if live {
                return Err(Error::StorageCorrupt);
            }
            #[cfg(test)]
            self.checkpoint(TestPoint::BeforeDelete).await?;
            self.inner
                .blobs
                .delete_if_present(location.clone(), self.inner.closing.child_token())
                .await?;
            let mut tx = DbTransaction::begin(&self.inner.pool).await?;
            sqlx::query("DELETE FROM openlegal.blob_deletion WHERE storage_key=$1")
                .bind(location.storage_key)
                .execute(tx.conn()?)
                .await
                .map_err(database_error)?;
            tx.commit().await?;
            self.metric(|m| m.orphan_cleanups += 1);
        }
        Ok(())
    }
    async fn enumerate_orphans(&self, now: u64) -> Result<(), Error> {
        let cursor = self
            .inner
            .scan_cursor
            .lock()
            .map_err(|_| Error::Internal)?
            .take();
        let page = self
            .inner
            .blobs
            .enumerate(cursor, 128, self.inner.closing.child_token())
            .await?;
        for blob in page.objects {
            let generation = blob
                .storage_key
                .get(68..)
                .and_then(|s| Uuid::parse_str(s).ok())
                .ok_or(Error::StorageCorrupt)?;
            let mut tx = DbTransaction::begin(&self.inner.pool).await?;
            let live:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM openlegal.blob_object WHERE storage_key=$1) OR EXISTS(SELECT 1 FROM openlegal.blob_deletion WHERE storage_key=$1)").bind(&blob.storage_key).fetch_one(tx.conn()?).await.map_err(database_error)?;
            if !live {
                Self::queue_available(&mut tx, blob.size_bytes as i64).await?;
                sqlx::query("INSERT INTO openlegal.blob_deletion(storage_key,sha256,generation,size_bytes,queued_at) VALUES($1,$2,$3,$4,$5::text::numeric)").bind(&blob.storage_key).bind(blob.digest.as_slice()).bind(generation).bind(blob.size_bytes as i64).bind(now.to_string()).execute(tx.conn()?).await.map_err(database_error)?;
            }
            tx.commit().await?;
        }
        *self.inner.scan_cursor.lock().map_err(|_| Error::Internal)? = page.next_cursor;
        Ok(())
    }
    /// Explicit operator pruning. Existing serving processes must be stopped.
    pub async fn prune(&self, now: u64) -> Result<(), Error> {
        tokio::time::timeout(Duration::from_secs(60),async {
            for _ in 0..8192 {
                self.maintain_inner(now).await?;
                let expired:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM openlegal.cache_snapshot WHERE captured_at<=$1::text::numeric-$2::bigint)").bind(now.to_string()).bind((self.inner.policy.retention_days*86400) as i64).fetch_one(&self.inner.pool).await.map_err(database_error)?;
                match self.check_accounting(true).await {Ok(()) if !expired=>return Ok(()),Ok(())|Err(Error::StorageCapacity)=>{},Err(error)=>return Err(error)}
            }
            Err(Error::StorageCapacity)
        }).await.unwrap_or(Err(Error::StorageUnavailable))
    }
}
