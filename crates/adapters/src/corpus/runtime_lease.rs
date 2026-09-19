//! Process-lifetime exclusion between corpus serving and offline index rebuilding.
use super::{DatabaseError, PgCorpusStore, db};
use sqlx::{Connection, PgConnection};
use tokio::sync::Mutex;

/// Owns a detached connection: dropping this guard closes the session instead of
/// returning a session-level advisory lock to the shared connection pool.
pub struct CorpusRuntimeLease {
    connection: Mutex<Option<PgConnection>>,
}

impl PgCorpusStore {
    /// Durable retention fence; rebuilding must never move this backward.
    pub async fn acknowledged_index(&self) -> Result<u64, DatabaseError> {
        self.gate().await?;
        let value: i64 = sqlx::query_scalar("SELECT index_ack FROM openlegal.corpus_control")
            .fetch_one(&self.pool)
            .await
            .map_err(db)?;
        value.try_into().map_err(|_| DatabaseError::StorageCorrupt)
    }

    pub async fn acquire_runtime_lease(&self) -> Result<CorpusRuntimeLease, DatabaseError> {
        let mut connection = self.pool.acquire().await.map_err(db)?.detach();
        // Two fixed int keys identify this application's corpus runtime within
        // the current database. Cache-only administration uses a separate scope.
        let acquired: bool =
            sqlx::query_scalar("SELECT pg_try_advisory_lock(1869376611, 1919971955)")
                .fetch_one(&mut connection)
                .await
                .map_err(db)?;
        if !acquired {
            return Err(DatabaseError::Conflict);
        }
        Ok(CorpusRuntimeLease {
            connection: Mutex::new(Some(connection)),
        })
    }
}

impl CorpusRuntimeLease {
    /// A broken connection loses exclusion; never reconnect it silently.
    pub async fn check(&self) -> Result<(), DatabaseError> {
        let mut connection = self.connection.lock().await;
        connection
            .as_mut()
            .ok_or(DatabaseError::StorageUnavailable)?
            .ping()
            .await
            .map_err(db)
    }

    pub async fn close(&self) -> Result<(), DatabaseError> {
        if let Some(connection) = self.connection.lock().await.take() {
            connection.close().await.map_err(db)?;
        }
        Ok(())
    }
}
