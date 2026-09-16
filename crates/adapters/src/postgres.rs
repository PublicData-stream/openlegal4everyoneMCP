//! PostgreSQL observation persistence. Retrieval policy remains in the application.
mod data;
mod maintenance;
mod publication;
#[cfg(test)]
mod tests;

use futures::future::BoxFuture;
use openlegal_application::blob::BlobStore;
use openlegal_application::persistence::{
    HistoryKey, LookupResult, PersistentKey, PersistentStore, PublicationOutcome,
    PublicationRequest, RetentionPolicy, StorageMetrics, StorageStatus,
};
use openlegal_domain::{
    RetrievalError as Error,
    history::{SnapshotEnvelope, SnapshotPage},
};
use sqlx::{
    ConnectOptions, PgConnection, PgPool, Postgres, Row, SqlSafeStr,
    migrate::{Migration, MigrationType, Migrator},
    pool::PoolConnection,
    postgres::{PgConnectOptions, PgPoolOptions, PgSslMode},
};
use std::{
    path::PathBuf,
    str::FromStr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU8, AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::sync::{Mutex as AsyncMutex, Semaphore};
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
pub enum PostgresTls {
    VerifyFull { ca_file: Option<PathBuf> },
    Plaintext,
}
#[derive(Clone)]
pub struct PostgresOptions {
    pub max_connections: u32,
    pub tls: PostgresTls,
}
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum StartupMode {
    Serve,
    Maintain,
}

/// Sanitized startup diagnostics, never driver messages or connection URLs.
#[derive(Debug)]
pub enum StartupError {
    PostgreSQL18Required,
    SchemaMismatch,
    Storage(Error),
}
impl std::fmt::Display for StartupError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::PostgreSQL18Required=>f.write_str("persistent storage requires PostgreSQL 18.x"),
            Self::SchemaMismatch=>f.write_str("PostgreSQL persistence schema is incompatible or migrations are pending; run --migrate with the matching server version"),
            Self::Storage(error)=>std::fmt::Display::fmt(error,f),
        }
    }
}
impl std::error::Error for StartupError {}
impl From<Error> for StartupError {
    fn from(error: Error) -> Self {
        Self::Storage(error)
    }
}
async fn startup_version(pool: &PgPool) -> Result<(), StartupError> {
    let version: i32 = sqlx::query_scalar("SELECT current_setting('server_version_num')::integer")
        .fetch_one(pool)
        .await
        .map_err(database_error)?;
    if !(180000..190000).contains(&version) {
        return Err(StartupError::PostgreSQL18Required);
    };
    Ok(())
}

#[derive(Clone)]
pub struct PostgresStore {
    inner: Arc<Inner>,
}
struct Inner {
    pool: PgPool,
    probe_pool: PgPool,
    transition: Mutex<()>,
    blobs: Arc<dyn BlobStore>,
    policy: RetentionPolicy,
    state: AtomicU8,
    epoch: AtomicU64,
    recovery_epoch: AtomicU64,
    scan_cursor: Mutex<Option<String>>,
    metrics: Mutex<StorageMetrics>,
    operations: Arc<Semaphore>,
    maintenance: AsyncMutex<()>,
    closing: CancellationToken,
    #[cfg(test)]
    hooks: Mutex<std::collections::HashMap<TestPoint, Arc<TestGate>>>,
}
#[cfg(test)]
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
enum TestPoint {
    AfterBlobReservation,
    AfterBlob,
    BeforeCommit,
    AfterCommit,
    BeforeDelete,
    BeforeRecoverReady,
}
#[cfg(test)]
struct TestGate {
    reached: Semaphore,
    resume: Semaphore,
    error: Option<Error>,
}
#[cfg(test)]
impl PostgresStore {
    async fn checkpoint(&self, point: TestPoint) -> Result<(), Error> {
        let gate = self
            .inner
            .hooks
            .lock()
            .map_err(|_| Error::Internal)?
            .remove(&point);
        if let Some(gate) = gate {
            gate.reached.add_permits(1);
            gate.resume
                .acquire()
                .await
                .map_err(|_| Error::Internal)?
                .forget();
            if let Some(error) = gate.error {
                return Err(error);
            };
        }
        Ok(())
    }
}
const READY: u8 = 0;
const RECOVERING: u8 = 1;
const CORRUPT: u8 = 2;
const MAINTAINING: u8 = 3;
const CLOSED: u8 = 4;
const SQL: &str = include_str!("../migrations/0001_persistence.sql");

fn migrator() -> Migrator {
    let mut migrator = Migrator::with_migrations(vec![
        Migration::new(
            1,
            "persistence".into(),
            MigrationType::Simple,
            SQL.into_sql_str(),
            false,
        ),
        Migration::new(
            2,
            "legal corpus".into(),
            MigrationType::Simple,
            include_str!("../migrations/0002_legal_corpus.sql").into_sql_str(),
            false,
        ),
    ]);
    migrator.dangerous_set_table_name("public._sqlx_migrations");
    migrator
}
fn database_error(error: sqlx::Error) -> Error {
    match &error {
        sqlx::Error::PoolTimedOut => Error::Busy,
        sqlx::Error::Database(db) => match db.code().as_deref() {
            Some("55P03" | "57014" | "40P01" | "40001") => Error::Busy,
            Some(code)
                if code.starts_with("23") || code.starts_with("22") || code.starts_with("42") =>
            {
                Error::StorageCorrupt
            }
            _ => Error::StorageUnavailable,
        },
        sqlx::Error::ColumnDecode { .. }
        | sqlx::Error::Decode(_)
        | sqlx::Error::ColumnNotFound(_) => Error::StorageCorrupt,
        _ => Error::StorageUnavailable,
    }
}
async fn pool(url: &str, options: PostgresOptions) -> Result<PgPool, Error> {
    if !(1..=64).contains(&options.max_connections) || url.len() > 8192 {
        return Err(Error::InvalidInput);
    }
    let parsed = url::Url::parse(url).map_err(|_| Error::InvalidInput)?;
    if !matches!(parsed.scheme(), "postgres" | "postgresql")
        || parsed.fragment().is_some()
        || parsed.query_pairs().any(|(key, _)| {
            !matches!(
                key.as_ref(),
                "host" | "port" | "dbname" | "user" | "password"
            )
        })
    {
        return Err(Error::InvalidInput);
    }
    let mut connect = PgConnectOptions::from_str(url)
        .map_err(|_| Error::InvalidInput)?
        .disable_statement_logging()
        .application_name("openlegal-persistence")
        .options([
            ("statement_timeout", "2000"),
            ("lock_timeout", "1000"),
            ("idle_in_transaction_session_timeout", "5000"),
            ("search_path", "pg_catalog,public"),
            ("synchronous_commit", "on"),
        ]);
    connect = match options.tls {
        PostgresTls::Plaintext => connect.ssl_mode(PgSslMode::Disable),
        PostgresTls::VerifyFull { ca_file } => {
            let connect = connect.ssl_mode(PgSslMode::VerifyFull);
            if let Some(path) = ca_file {
                connect.ssl_root_cert(path)
            } else {
                connect
            }
        }
    };
    PgPoolOptions::new()
        .max_connections(options.max_connections)
        .min_connections(0)
        .acquire_timeout(Duration::from_secs(1))
        .max_lifetime(Duration::from_secs(1800))
        .after_connect(|connection, _| {
            Box::pin(async move {
                let version: i32 =
                    sqlx::query_scalar("SELECT current_setting('server_version_num')::integer")
                        .fetch_one(&mut *connection)
                        .await?;
                if (180000..190000).contains(&version) {
                    sqlx::query("SET transaction_timeout='5s'")
                        .execute(&mut *connection)
                        .await?;
                }
                Ok(())
            })
        })
        .connect_with(connect)
        .await
        .map_err(database_error)
}
async fn verify_version(pool: &PgPool) -> Result<(), Error> {
    let version: i32 = sqlx::query_scalar("SELECT current_setting('server_version_num')::integer")
        .fetch_one(pool)
        .await
        .map_err(database_error)?;
    if !(180000..190000).contains(&version) {
        return Err(Error::StorageCorrupt);
    }
    let native: bool =
        sqlx::query_scalar("SELECT pg_catalog.uuid_extract_version(pg_catalog.uuidv7()) = 7")
            .fetch_one(pool)
            .await
            .map_err(database_error)?;
    if !native {
        return Err(Error::StorageCorrupt);
    }
    Ok(())
}
async fn verify_schema(pool: &PgPool) -> Result<(), Error> {
    verify_version(pool).await?;
    let rows = sqlx::query(
        "SELECT version, checksum, success FROM public._sqlx_migrations ORDER BY version LIMIT 3",
    )
    .fetch_all(pool)
    .await
    .map_err(database_error)?;
    let migrations = migrator();
    if rows.len() != migrations.migrations.len() {
        return Err(Error::StorageCorrupt);
    }
    for (row, migration) in rows.iter().zip(migrations.iter()) {
        if row.try_get::<i64, _>("version").map_err(database_error)? != migration.version
            || row
                .try_get::<Vec<u8>, _>("checksum")
                .map_err(database_error)?
                .as_slice()
                != migration.checksum.as_ref()
            || !row.try_get::<bool, _>("success").map_err(database_error)?
        {
            return Err(Error::StorageCorrupt);
        }
    }
    Ok(())
}
impl PostgresStore {
    /// Shared validated runtime pool; its lifetime remains owned by this store.
    pub fn pool(&self) -> PgPool {
        self.inner.pool.clone()
    }
    pub async fn migrate(url: &str, options: PostgresOptions) -> Result<(), StartupError> {
        let pool = pool(url, options).await?;
        let result = async {
            startup_version(&pool).await?;
            migrator()
                .run(&pool)
                .await
                .map_err(|_| StartupError::SchemaMismatch)
        }
        .await;
        pool.close().await;
        result
    }
    pub async fn open(
        url: &str,
        options: PostgresOptions,
        blobs: Arc<dyn BlobStore>,
        policy: RetentionPolicy,
        now: u64,
        mode: StartupMode,
    ) -> Result<Arc<Self>, StartupError> {
        policy.validate()?;
        if options.max_connections < 2 {
            return Err(Error::InvalidInput.into());
        }
        let mut probe_options = options.clone();
        probe_options.max_connections = 1;
        let mut data_options = options;
        data_options.max_connections -= 1;
        let pool = pool(url, data_options).await?;
        let probe_pool = match self::pool(url, probe_options).await {
            Ok(pool) => pool,
            Err(error) => {
                pool.close().await;
                return Err(error.into());
            }
        };
        let store = Arc::new(Self {
            inner: Arc::new(Inner {
                pool,
                probe_pool,
                transition: Mutex::new(()),
                blobs,
                policy,
                state: AtomicU8::new(READY),
                epoch: AtomicU64::new(0),
                recovery_epoch: AtomicU64::new(0),
                scan_cursor: Mutex::new(None),
                metrics: Mutex::new(StorageMetrics::default()),
                operations: Arc::new(Semaphore::new(64)),
                maintenance: AsyncMutex::new(()),
                closing: CancellationToken::new(),
                #[cfg(test)]
                hooks: Mutex::new(std::collections::HashMap::new()),
            }),
        });
        let result: Result<(), StartupError> = async {
            startup_version(&store.inner.pool).await?;
            verify_schema(&store.inner.pool)
                .await
                .map_err(|_| StartupError::SchemaMismatch)?;
            store.inner.blobs.health(CancellationToken::new()).await?;
            store.check_accounting(mode == StartupMode::Serve).await?;
            // Probe database-selected evidence, in addition to object-store capability checks.
            if let Some(row) =
                sqlx::query("SELECT public_id FROM openlegal.cache_snapshot ORDER BY id LIMIT 1")
                    .fetch_optional(&store.inner.pool)
                    .await
                    .map_err(database_error)?
            {
                let id: String = row.try_get("public_id").map_err(database_error)?;
                let _ = store.read_snapshot(None, Some(&id), None, now).await?;
            }
            Ok(())
        }
        .await;
        if let Err(error) = result {
            store.inner.pool.close().await;
            store.inner.probe_pool.close().await;
            let _ = store.inner.blobs.close().await;
            return Err(error);
        }
        Ok(store)
    }
    fn record(&self, result: &Result<impl Sized, Error>) {
        if let Err(error) = result {
            if matches!(error, Error::Busy | Error::StorageCapacity) {
                self.metric(|m| m.saturation += 1);
            }
            let state = match error {
                Error::StorageCorrupt => CORRUPT,
                Error::StorageUnavailable => RECOVERING,
                _ => return,
            };
            if let Ok(_guard) = self.inner.transition.lock() {
                let current = self.inner.state.load(Ordering::Acquire);
                if current != CLOSED && current != CORRUPT {
                    self.inner.epoch.fetch_add(1, Ordering::AcqRel);
                    self.inner.recovery_epoch.fetch_add(1, Ordering::AcqRel);
                    self.inner.state.store(state, Ordering::Release);
                }
            }
            self.metric(|m| {
                m.failures += 1;
                if state == CORRUPT {
                    m.corruptions += 1;
                }
            });
        }
    }
    fn metric(&self, update: impl FnOnce(&mut StorageMetrics)) {
        if let Ok(mut metrics) = self.inner.metrics.lock() {
            update(&mut metrics);
        }
    }
    fn own<T: Send + 'static>(
        &self,
        operation: impl FnOnce(Self) -> BoxFuture<'static, Result<T, Error>> + Send + 'static,
    ) -> BoxFuture<'static, Result<T, Error>> {
        let store = self.clone();
        Box::pin(async move {
            if let Some(error) = store.status().error() {
                return Err(error);
            }
            let permit = store
                .inner
                .operations
                .clone()
                .try_acquire_owned()
                .map_err(|_| Error::Busy)?;
            let task = tokio::spawn(async move {
                let _permit = permit;
                let result =
                    tokio::time::timeout(Duration::from_secs(20), operation(store.clone()))
                        .await
                        .unwrap_or(Err(Error::StorageUnavailable));
                store.record(&result);
                result
            });
            task.await.map_err(|_| Error::StorageUnavailable)?
        })
    }
    async fn recover(&self) -> Result<(), Error> {
        if self.inner.state.load(Ordering::Acquire) == CORRUPT {
            return Err(Error::StorageCorrupt);
        }
        if self.inner.closing.is_cancelled() {
            return Err(Error::Shutdown);
        }
        let failure_generation = self.inner.recovery_epoch.load(Ordering::Acquire);
        // All commits held this row. Acquiring it on another connection fences an uncertain commit.
        let transaction = DbTransaction::begin(&self.inner.probe_pool).await?;
        transaction.commit().await?;
        verify_schema(&self.inner.probe_pool).await?;
        self.check_accounting(true).await?;
        self.inner.blobs.health(CancellationToken::new()).await?;
        #[cfg(test)]
        self.checkpoint(TestPoint::BeforeRecoverReady).await?;
        let _guard = self.inner.transition.lock().map_err(|_| Error::Internal)?;
        if self.inner.closing.is_cancelled()
            || self.inner.state.load(Ordering::Acquire) != RECOVERING
            || self.inner.recovery_epoch.load(Ordering::Acquire) != failure_generation
        {
            return Err(Error::StorageUnavailable);
        }
        self.inner.epoch.fetch_add(1, Ordering::AcqRel);
        self.inner.state.store(READY, Ordering::Release);
        self.metric(|m| m.recoveries += 1);
        Ok(())
    }
}
/// Own a checked-out connection until the entire transaction is resolved. An
/// unacknowledged transaction is never returned to the pool, including on timeout.
struct DbTransaction {
    connection: Option<PoolConnection<Postgres>>,
    committed: bool,
}
impl DbTransaction {
    async fn begin(pool: &PgPool) -> Result<Self, Error> {
        let connection = pool.acquire().await.map_err(database_error)?;
        let mut transaction = Self {
            connection: Some(connection),
            committed: false,
        };
        sqlx::query("BEGIN")
            .execute(transaction.conn()?)
            .await
            .map_err(database_error)?;
        sqlx::query("SELECT id FROM openlegal.cache_storage WHERE singleton FOR UPDATE")
            .fetch_one(transaction.conn()?)
            .await
            .map_err(database_error)?;
        Ok(transaction)
    }
    fn conn(&mut self) -> Result<&mut PgConnection, Error> {
        self.connection.as_deref_mut().ok_or(Error::Internal)
    }
    async fn commit(mut self) -> Result<(), Error> {
        sqlx::query("COMMIT")
            .execute(self.conn()?)
            .await
            .map_err(database_error)?;
        self.committed = true;
        Ok(())
    }
}
impl Drop for DbTransaction {
    fn drop(&mut self) {
        if !self.committed
            && let Some(connection) = self.connection.take()
        {
            drop(connection.detach());
        }
    }
}
impl PersistentStore for PostgresStore {
    fn lookup(
        &self,
        key: PersistentKey,
        now: u64,
        cancellation: CancellationToken,
    ) -> BoxFuture<'static, Result<LookupResult, Error>> {
        self.own(move |store| {
            Box::pin(async move {
                if cancellation.is_cancelled() {
                    return Err(Error::Cancelled);
                }
                store.lookup_inner(key, now, cancellation).await
            })
        })
    }
    fn publish(
        &self,
        request: PublicationRequest,
    ) -> BoxFuture<'static, Result<PublicationOutcome, Error>> {
        self.own(move |store| Box::pin(async move { store.publish_inner(request).await }))
    }
    fn list(
        &self,
        key: HistoryKey,
        cursor: Option<String>,
        limit: usize,
        now: u64,
        cancellation: CancellationToken,
    ) -> BoxFuture<'static, Result<SnapshotPage, Error>> {
        self.own(move |store| {
            Box::pin(async move {
                if cancellation.is_cancelled() {
                    return Err(Error::Cancelled);
                }
                store.list_inner(key, cursor, limit, now).await
            })
        })
    }
    fn get(
        &self,
        key: HistoryKey,
        id: String,
        now: u64,
        cancellation: CancellationToken,
    ) -> BoxFuture<'static, Result<SnapshotEnvelope, Error>> {
        self.own(move |store| {
            Box::pin(async move {
                if cancellation.is_cancelled() {
                    return Err(Error::Cancelled);
                }
                store.get_inner(key, id, now).await
            })
        })
    }
    fn health(&self, _now: u64) -> BoxFuture<'static, Result<(), Error>> {
        let store = self.clone();
        Box::pin(async move {
            let _guard = store
                .inner
                .maintenance
                .try_lock()
                .map_err(|_| Error::Busy)?;
            let status = store.inner.state.load(Ordering::Acquire);
            if status == MAINTAINING {
                return Err(Error::Busy);
            };
            let work = async {
                match status {
                    RECOVERING => store.recover().await,
                    READY => {
                        verify_schema(&store.inner.probe_pool).await?;
                        store.refresh_gauges().await?;
                        store
                            .inner
                            .blobs
                            .health(store.inner.closing.child_token())
                            .await
                    }
                    CORRUPT => Err(Error::StorageCorrupt),
                    CLOSED => Err(Error::Shutdown),
                    _ => Err(Error::Busy),
                }
            };
            let result = tokio::time::timeout(Duration::from_secs(5), work)
                .await
                .unwrap_or(Err(Error::StorageUnavailable));
            let result = if matches!(result, Err(Error::Busy)) {
                Err(Error::StorageUnavailable)
            } else {
                result
            };
            store.record(&result);
            result
        })
    }
    fn maintain(&self, now: u64) -> BoxFuture<'static, Result<(), Error>> {
        let store = self.clone();
        Box::pin(async move {
            let _guard = store
                .inner
                .maintenance
                .try_lock()
                .map_err(|_| Error::Busy)?;
            if store.inner.state.load(Ordering::Acquire) == RECOVERING {
                let result = tokio::time::timeout(Duration::from_secs(5), store.recover())
                    .await
                    .unwrap_or(Err(Error::StorageUnavailable));
                store.record(&result);
                return result;
            }
            if !store.healthy() {
                return Err(Error::StorageUnavailable);
            }
            let result = tokio::time::timeout(Duration::from_secs(5), store.maintain_inner(now))
                .await
                .unwrap_or(Err(Error::StorageUnavailable));
            store.record(&result);
            result
        })
    }
    fn close(&self) -> BoxFuture<'static, Result<(), Error>> {
        let store = self.clone();
        Box::pin(async move {
            store.inner.closing.cancel();
            {
                let _guard = store.inner.transition.lock().map_err(|_| Error::Internal)?;
                store.inner.state.store(CLOSED, Ordering::Release);
                store.inner.epoch.fetch_add(1, Ordering::AcqRel);
            }
            let drain = store.inner.operations.acquire_many(64);
            let _permits = tokio::time::timeout(Duration::from_secs(5), drain)
                .await
                .map_err(|_| Error::StorageUnavailable)?
                .map_err(|_| Error::StorageUnavailable)?;
            let blob = store.inner.blobs.close().await;
            tokio::time::timeout(Duration::from_secs(5), store.inner.pool.close())
                .await
                .map_err(|_| Error::StorageUnavailable)?;
            tokio::time::timeout(Duration::from_secs(5), store.inner.probe_pool.close())
                .await
                .map_err(|_| Error::StorageUnavailable)?;
            blob
        })
    }
    fn epoch(&self) -> u64 {
        self.inner.epoch.load(Ordering::Acquire)
    }
    fn recovery_epoch(&self) -> u64 {
        self.inner.recovery_epoch.load(Ordering::Acquire)
    }
    fn healthy(&self) -> bool {
        self.inner.state.load(Ordering::Acquire) == READY
    }
    fn status(&self) -> StorageStatus {
        match self.inner.state.load(Ordering::Acquire) {
            READY => StorageStatus::Ready,
            CORRUPT => StorageStatus::IntegrityBlocked,
            MAINTAINING => StorageStatus::Maintaining,
            CLOSED => StorageStatus::Closed,
            _ => StorageStatus::Recovering,
        }
    }
    fn policy(&self) -> RetentionPolicy {
        self.inner.policy.clone()
    }
    fn metrics(&self) -> StorageMetrics {
        let mut metrics = self
            .inner
            .metrics
            .lock()
            .map(|m| m.clone())
            .unwrap_or_default();
        let blob = self.inner.blobs.metrics();
        metrics.blob_reads = blob.reads;
        metrics.blob_writes = blob.writes;
        metrics.deduplicated_puts = blob.deduplicated_puts;
        metrics.pool_connections = u64::from(self.inner.pool.size() + self.inner.probe_pool.size());
        metrics.pool_idle = (self.inner.pool.num_idle() + self.inner.probe_pool.num_idle()) as u64;
        metrics
    }
}
