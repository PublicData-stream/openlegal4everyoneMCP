//! Cross-Pod LAW HTTP ownership. No database transaction spans a network fetch.
use super::{DatabaseError, PgPool, RequestBudgetMode};
use crate::collection_events::CollectionEvents;
use sqlx::{Connection, PgConnection, Row, postgres::PgRow};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU32, Ordering},
    },
    time::Duration,
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

// Distinct from the corpus runtime and cache maintenance advisory-lock keys.
const PROVIDER_LOCK: (i32, i32) = (1869376611, 1818326864);

fn storage(error: sqlx::Error) -> DatabaseError {
    let sqlstate = error.as_database_error().and_then(|error| error.code());
    let error_kind = if sqlstate.is_some() {
        "database"
    } else if matches!(error, sqlx::Error::PoolTimedOut) {
        "pool_timeout"
    } else {
        "connection_or_protocol"
    };
    tracing::warn!(
        sqlstate = sqlstate.as_deref().unwrap_or(""),
        error_kind,
        "LAW storage operation failed; outcome is not replayable"
    );
    DatabaseError::StorageUnavailable
}

/// No statement or provider request has been submitted on this acquisition.
fn acquire_error(error: sqlx::Error) -> DatabaseError {
    if matches!(error, sqlx::Error::PoolTimedOut) {
        tracing::warn!(
            phase = "pool_acquire",
            error_kind = "pool_timeout",
            commit_started = false,
            "LAW admission yielded before SQL submission"
        );
        DatabaseError::StorageContended
    } else {
        storage(error)
    }
}

async fn commit_budget(
    tx: sqlx::Transaction<'_, sqlx::Postgres>,
    operation: &'static str,
) -> Result<(), DatabaseError> {
    let started = tokio::time::Instant::now();
    tracing::debug!(
        operation,
        phase = "commit",
        commit_started = true,
        "LAW admission COMMIT started"
    );
    tx.commit().await.map_err(|error| {
        tracing::warn!(
            operation,
            phase = "commit",
            commit_started = true,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "LAW admission COMMIT was not acknowledged"
        );
        storage(error)
    })
}

/// Retry only the first budget-row lock, before any admission mutation.
/// PostgreSQL ROLLBACK AND CHAIN acknowledges the entire failed transaction and
/// starts a fresh one on the same session. The SQLx wrapper still owns depth one;
/// its commit/rollback/drop finishes the newly chained transaction. Session-level
/// response ownership survives the chain, while transaction_timeout starts afresh.
/// No reservation effects, settlement mutation or COMMIT are replayed.
async fn lock_budget(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    operation: &'static str,
    cancel: Option<&CancellationToken>,
    before_http: bool,
) -> Result<PgRow, DatabaseError> {
    const DELAYS: [Duration; 3] = [
        Duration::from_millis(100),
        Duration::from_millis(200),
        Duration::from_millis(400),
    ];
    let started = tokio::time::Instant::now();
    let deadline = started + Duration::from_secs(10);
    for (attempt, delay) in DELAYS
        .into_iter()
        .map(Some)
        .chain(std::iter::once(None))
        .enumerate()
    {
        if cancel.is_some_and(CancellationToken::is_cancelled) {
            return Err(DatabaseError::Cancelled);
        }
        let result = tokio::time::timeout_at(deadline, async {
            match sqlx::query("SELECT *,floor(extract(epoch from clock_timestamp())*1000)::bigint AS now_ms FROM openlegal.provider_request_budget WHERE singleton FOR UPDATE")
                .fetch_one(&mut **tx).await
            {
                Ok(row) => Ok(Some(row)),
                Err(error) if error.as_database_error().is_some_and(|error| error.code().as_deref() == Some("55P03")) => {
                    sqlx::query("ROLLBACK AND CHAIN").execute(&mut **tx).await.map_err(|error| {
                        tracing::warn!(operation, phase = "rollback_chain", attempt = attempt + 1,
                            commit_started = false, elapsed_ms = started.elapsed().as_millis() as u64,
                            "LAW first-lock outer rollback was not acknowledged");
                        storage(error)
                    })?;
                    tracing::warn!(operation, phase = "first_budget_lock", attempt = attempt + 1,
                        sqlstate = "55P03", error_kind = "lock_rejected", commit_started = false,
                        elapsed_ms = started.elapsed().as_millis() as u64,
                        "provider budget lock rejected; outer rollback and fresh transaction acknowledged");
                    Ok(None)
                }
                Err(error) => Err(storage(error)),
            }
        }).await.map_err(|_| {
            tracing::warn!(operation, phase = "first_budget_lock", error_kind = "local_deadline",
                commit_started = false, elapsed_ms = started.elapsed().as_millis() as u64,
                "LAW first-lock outcome was interrupted locally");
            DatabaseError::StorageUnavailable
        })??;
        if let Some(row) = result {
            return Ok(row);
        }
        let exhausted = || {
            if before_http {
                DatabaseError::StorageContended
            } else {
                DatabaseError::StorageUnavailable
            }
        };
        let Some(delay) = delay else {
            return Err(exhausted());
        };
        let wake = tokio::time::Instant::now() + delay;
        if wake >= deadline {
            return Err(exhausted());
        }
        if let Some(cancel) = cancel {
            tokio::select! {
                _ = cancel.cancelled() => return Err(DatabaseError::Cancelled),
                _ = tokio::time::sleep_until(wake) => {}
            }
        } else {
            tokio::time::sleep_until(wake).await;
        }
    }
    Err(DatabaseError::StorageUnavailable)
}

/// Owns one charged attempt and its detached advisory-lock session. Dropping an
/// unsettled guard closes the session while retaining the durable uncertain marker.
pub struct ProviderRequestGuard {
    pool: PgPool,
    connection: Option<PgConnection>,
    owner: Uuid,
    slot: i32,
}
impl std::fmt::Debug for ProviderRequestGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProviderRequestGuard")
            .finish_non_exhaustive()
    }
}
impl PartialEq for ProviderRequestGuard {
    fn eq(&self, other: &Self) -> bool {
        self.owner == other.owner
    }
}
impl Eq for ProviderRequestGuard {}

impl ProviderRequestGuard {
    /// Settle only this response; other concurrent owners retain their evidence.
    pub async fn complete(&mut self) -> Result<(), DatabaseError> {
        self.settle(None).await?;
        self.release().await
    }
    /// Publish a shared provider pause and settle only this response atomically.
    pub async fn pause(&mut self, delay: u64) -> Result<(), DatabaseError> {
        self.settle(Some(delay)).await?;
        self.release().await
    }
    #[tracing::instrument(skip_all, fields(operation = "settle_response"))]
    async fn settle(&mut self, delay: Option<u64>) -> Result<(), DatabaseError> {
        let connection = self.connection.as_mut().ok_or(DatabaseError::Conflict)?;
        let mut tx = connection.begin().await.map_err(storage)?;
        lock_budget(&mut tx, "settle_response", None, false).await?;
        let changed =
            sqlx::query("DELETE FROM openlegal.provider_request_admission WHERE owner=$1")
                .bind(self.owner)
                .execute(&mut *tx)
                .await
                .map_err(storage)?
                .rows_affected();
        if changed != 1 {
            return Err(DatabaseError::Conflict);
        }
        if let Some(delay) = delay {
            let delay = i64::try_from(delay)
                .unwrap_or(i64::MAX / 4)
                .min(i64::MAX / 4);
            sqlx::query("UPDATE openlegal.provider_request_budget SET next_allowed_at=GREATEST(next_allowed_at,floor(extract(epoch from clock_timestamp()))::bigint+$1),operator_suspended=operator_suspended OR $2 WHERE singleton")
                .bind(delay).bind(delay > 7 * 86400).execute(&mut *tx).await.map_err(storage)?;
        }
        commit_budget(tx, "settle_response").await
    }
    /// Rejection can be discovered after HTTP settlement, during parsing. Its
    /// original owner token cannot clear another concurrent request's evidence.
    pub async fn suspend(&mut self) -> Result<(), DatabaseError> {
        suspend_owned_response(&self.pool, self.owner).await?;
        self.release().await
    }
    async fn release(&mut self) -> Result<(), DatabaseError> {
        if let Some(mut connection) = self.connection.take() {
            sqlx::query("SELECT pg_advisory_unlock($1,$2)")
                .bind(PROVIDER_LOCK.0)
                .bind(PROVIDER_LOCK.1 + self.slot)
                .execute(&mut connection)
                .await
                .map_err(storage)?;
            // Notify only after the session lock is released; a waiter never
            // observes completion while the old owner still excludes admission.
            sqlx::query("SELECT pg_notify('openlegal_collection','')")
                .execute(&mut connection)
                .await
                .map_err(storage)?;
            connection.close().await.map_err(storage)?;
        }
        Ok(())
    }
    pub(super) fn owner(&self) -> Uuid {
        self.owner
    }
    pub(super) fn is_active(&self) -> bool {
        self.connection.is_some()
    }
}

#[tracing::instrument(skip_all, fields(operation = "suspend_response"))]
pub(super) async fn suspend_owned_response(
    pool: &PgPool,
    owner: Uuid,
) -> Result<(), DatabaseError> {
    let mut tx = pool.begin().await.map_err(storage)?;
    lock_budget(&mut tx, "suspend_response", None, false).await?;
    sqlx::query(
        "UPDATE openlegal.provider_request_budget SET operator_suspended=true WHERE singleton",
    )
    .execute(&mut *tx)
    .await
    .map_err(storage)?;
    sqlx::query("DELETE FROM openlegal.provider_request_admission WHERE owner=$1")
        .bind(owner)
        .execute(&mut *tx)
        .await
        .map_err(storage)?;
    sqlx::query("SELECT pg_notify('openlegal_collection','')")
        .execute(&mut *tx)
        .await
        .map_err(storage)?;
    commit_budget(tx, "suspend_response").await
}

/// Detect every abandoned slot, including slots above a newly lowered limit.
/// A live owner holds its session lock. Acquiring its lock transactionally proves
/// the durable response has lost its owner; never erase or retry this evidence.
async fn live_owner_count(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<Option<usize>, DatabaseError> {
    let slots: Vec<i32> =
        sqlx::query_scalar("SELECT slot FROM openlegal.provider_request_admission ORDER BY slot")
            .fetch_all(&mut **tx)
            .await
            .map_err(storage)?;
    for slot in &slots {
        let abandoned: bool = sqlx::query_scalar("SELECT pg_try_advisory_xact_lock($1,$2)")
            .bind(PROVIDER_LOCK.0)
            .bind(PROVIDER_LOCK.1 + slot)
            .fetch_one(&mut **tx)
            .await
            .map_err(storage)?;
        if abandoned {
            return Ok(None);
        }
    }
    Ok(Some(slots.len()))
}

/// Call only while holding the provider singleton row lock. An uncertain owner
/// also prevents configuration changes from waking budget-only deferred work.
pub(super) async fn uncertain_in_transaction(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<bool, DatabaseError> {
    Ok(live_owner_count(tx).await?.is_none())
}

/// Sanitized admission diagnostics from one locked provider snapshot. A recheck
/// time does not promise recovery from operator suspension or uncertain evidence.
#[derive(Clone, Debug)]
pub struct ProviderDeferral {
    pub reason: &'static str,
    pub recheck_at: u64,
    pub fingerprint: Option<openlegal_domain::provider_admin::ProviderBlockerFingerprint>,
}

#[tracing::instrument(skip_all, fields(operation = "admission_deferral"))]
pub(super) async fn deferral_snapshot(
    pool: &PgPool,
    mode: RequestBudgetMode,
    operation_exhausted: bool,
    locally_suspended: bool,
) -> Result<ProviderDeferral, DatabaseError> {
    let mut connection = pool.acquire().await.map_err(acquire_error)?;
    let mut tx = connection.begin().await.map_err(storage)?;
    let row = lock_budget(&mut tx, "admission_deferral", None, true).await?;
    // Read time after acquiring the row lock. Waiting for settlement must not
    // produce an ETA from a stale clock or pair it with a different policy row.
    let now: i64 =
        sqlx::query_scalar("SELECT floor(extract(epoch from clock_timestamp()))::bigint")
            .fetch_one(&mut *tx)
            .await
            .map_err(storage)?;
    let number = |name| row.try_get::<i64, _>(name).map_err(storage);
    let held: bool = sqlx::query_scalar("SELECT openlegal_admin.provider_held()")
        .fetch_one(&mut *tx)
        .await
        .map_err(storage)?;
    let uncertain = row
        .try_get::<bool, _>("unresolved_response")
        .map_err(storage)?
        || uncertain_in_transaction(&mut tx).await?;
    let suspended = row
        .try_get::<bool, _>("operator_suspended")
        .map_err(storage)?
        || locally_suspended;
    let (used, limit) = if mode == RequestBudgetMode::OnDemand {
        (
            number("on_demand_used")?,
            row.try_get::<Option<i32>, _>("on_demand_daily_limit")
                .map_err(storage)?,
        )
    } else {
        (
            number("daily_used")?,
            row.try_get::<Option<i32>, _>("continuous_daily_limit")
                .map_err(storage)?,
        )
    };
    let day = now / 86400;
    let daily_exhausted =
        number("utc_day")? == day && limit.is_some_and(|limit| used >= i64::from(limit));
    let pause = number("next_allowed_at")?;
    let spacing = number("next_request_at_ms")?;
    let spacing = spacing / 1000 + i64::from(spacing % 1000 != 0);
    let earliest = pause.max(spacing).max(now.saturating_add(10));
    let (reason, recheck) = if held {
        ("provider_recovery_hold", now.saturating_add(3600))
    } else if uncertain {
        ("provider_response_uncertain", now.saturating_add(3600))
    } else if suspended {
        ("provider_suspended", now.saturating_add(3600))
    } else if daily_exhausted {
        ("provider_daily_limit", earliest.max((day + 1) * 86400 + 10))
    } else if operation_exhausted {
        ("operation_attempt_limit", earliest)
    } else if pause > now {
        ("provider_retry_after", earliest)
    } else {
        // The blocker can settle before this snapshot, or a pilot/local limit
        // can be exhausted. Do not invent a provider failure in either case.
        ("provider_admission_wait", earliest)
    };
    let fingerprint = if uncertain || held {
        // This is a collector deferral decision, not the public diagnostic read.
        // Commit first-observation evidence before returning its refusal.
        sqlx::query("SELECT openlegal_admin.observe_uncertainty()")
            .execute(&mut *tx)
            .await
            .map_err(storage)?;
        let value: serde_json::Value =
            sqlx::query_scalar("SELECT openlegal_admin.provider_blocker_fingerprint()")
                .fetch_one(&mut *tx)
                .await
                .map_err(storage)?;
        Some(serde_json::from_value(value).map_err(|_| DatabaseError::StorageCorrupt)?)
    } else {
        None
    };
    commit_budget(tx, "admission_deferral").await?;
    Ok(ProviderDeferral {
        reason,
        fingerprint,
        recheck_at: u64::try_from(recheck).map_err(|_| DatabaseError::StorageCorrupt)?,
    })
}

async fn demand_waiting(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    now: i64,
) -> Result<bool, DatabaseError> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM openlegal.provider_demand_ticket WHERE mode='on_demand' AND lease_until>$1) OR EXISTS(SELECT 1 FROM openlegal.collection_request WHERE expires_at>$1 AND ((status='launching' AND launched_at>=$1-30) OR ((status='queued' OR (status='deferred' AND lease_until<=$1)) AND EXISTS(SELECT 1 FROM openlegal.corpus_control WHERE singleton AND collection_scheduler_seen_at>=$1-30) AND (SELECT count(*) FROM openlegal.collection_request WHERE status IN ('launching','running'))<16)))")
        .bind(now).fetch_one(&mut **tx).await.map_err(storage)
}

#[tracing::instrument(skip_all, fields(operation = "provider_idle"))]
pub(super) async fn idle(pool: &PgPool, mode: RequestBudgetMode) -> Result<bool, DatabaseError> {
    let mut connection = pool.acquire().await.map_err(acquire_error)?;
    let mut tx = connection.begin().await.map_err(storage)?;
    let row = lock_budget(&mut tx, "provider_idle", None, true).await?;
    let now_ms: i64 = row.try_get("now_ms").map_err(storage)?;
    let now = now_ms / 1000;
    let daily_available = |name: &str, used: &str| -> Result<bool, DatabaseError> {
        let limit: Option<i32> = row.try_get(name).map_err(storage)?;
        let current = row.try_get::<i64, _>("utc_day").map_err(storage)? == now / 86400;
        let used: i64 = row.try_get(used).map_err(storage)?;
        Ok(!current || limit.is_none_or(|limit| used < i64::from(limit)))
    };
    let capacity: i32 = row.try_get("max_in_flight").map_err(storage)?;
    let active = live_owner_count(&mut tx).await?;
    let held: bool = sqlx::query_scalar("SELECT openlegal_admin.provider_held()")
        .fetch_one(&mut *tx)
        .await
        .map_err(storage)?;
    if held
        || row
            .try_get::<bool, _>("operator_suspended")
            .map_err(storage)?
        || row
            .try_get::<bool, _>("unresolved_response")
            .map_err(storage)?
        || active.is_none_or(|count| count >= capacity as usize)
        || row
            .try_get::<i64, _>("next_allowed_at")
            .map_err(storage)?
            .saturating_mul(1000)
            .max(
                row.try_get::<i64, _>("next_request_at_ms")
                    .map_err(storage)?,
            )
            > now_ms
        || !daily_available(
            if mode == RequestBudgetMode::OnDemand {
                "on_demand_daily_limit"
            } else {
                "continuous_daily_limit"
            },
            if mode == RequestBudgetMode::OnDemand {
                "on_demand_used"
            } else {
                "daily_used"
            },
        )?
    {
        sqlx::query("SELECT openlegal_admin.observe_uncertainty()")
            .execute(&mut *tx)
            .await
            .map_err(storage)?;
        commit_budget(tx, "provider_idle").await?;
        return Ok(false);
    }
    let last: Option<String> = row.try_get("last_admission_mode").map_err(storage)?;
    if mode != RequestBudgetMode::OnDemand
        && last.as_deref() != Some("on_demand")
        && daily_available("on_demand_daily_limit", "on_demand_used")?
        && demand_waiting(&mut tx, now).await?
    {
        return Ok(false);
    }
    if mode == RequestBudgetMode::OnDemand
        && last.as_deref() == Some("on_demand")
        && daily_available("continuous_daily_limit", "daily_used")?
    {
        let background: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM openlegal.provider_demand_ticket WHERE mode='continuous' AND lease_until>$1)")
            .bind(now).fetch_one(&mut *tx).await.map_err(storage)?;
        if background {
            return Ok(false);
        }
    }
    if mode == RequestBudgetMode::Pilot {
        let cap: Option<i32> = row.try_get("pilot_attempt_limit").map_err(storage)?;
        let used: i64 = row.try_get("pilot_used").map_err(storage)?;
        let started: Option<i64> = row.try_get("pilot_started_at").map_err(storage)?;
        let duration: i64 = row
            .try_get::<Option<i64>, _>("pilot_duration_secs")
            .map_err(storage)?
            .unwrap_or(row.try_get("pilot_timeout_secs").map_err(storage)?);
        if cap.is_some_and(|cap| used >= i64::from(cap))
            || started.is_some_and(|started| now.saturating_sub(started) >= duration)
        {
            return Ok(false);
        }
    }
    Ok(true)
}

/// A bounded foreground ticket starts before the in-process semaphore wait.
/// Its renewer is cancelled on drop; expired tickets are never priority evidence.
pub(super) struct ForegroundTicket {
    pool: PgPool,
    owner: Uuid,
    stop: CancellationToken,
    failed: CancellationToken,
    renewer: tokio::task::JoinHandle<()>,
}
impl ForegroundTicket {
    pub(super) async fn open(pool: &PgPool) -> Result<Self, DatabaseError> {
        Self::open_mode(pool, "on_demand").await
    }
    #[tracing::instrument(skip_all, fields(operation = "open_demand_ticket"))]
    async fn open_mode(pool: &PgPool, mode: &str) -> Result<Self, DatabaseError> {
        let mut connection = pool.acquire().await.map_err(acquire_error)?;
        let mut tx = connection.begin().await.map_err(storage)?;
        lock_budget(&mut tx, "open_demand_ticket", None, true).await?;
        let owner: Uuid = sqlx::query_scalar("SELECT pg_catalog.uuidv7()")
            .fetch_one(&mut *tx)
            .await
            .map_err(storage)?;
        sqlx::query("DELETE FROM openlegal.provider_demand_ticket WHERE lease_until<=floor(extract(epoch from clock_timestamp()))::bigint")
            .execute(&mut *tx).await.map_err(storage)?;
        let count: i64 =
            sqlx::query_scalar("SELECT count(*) FROM openlegal.provider_demand_ticket")
                .fetch_one(&mut *tx)
                .await
                .map_err(storage)?;
        if count >= 128 {
            return Err(DatabaseError::Capacity);
        }
        sqlx::query("INSERT INTO openlegal.provider_demand_ticket(owner,lease_until,mode) VALUES($1,floor(extract(epoch from clock_timestamp()))::bigint+30,$2)")
            .bind(owner).bind(mode).execute(&mut *tx).await.map_err(storage)?;
        sqlx::query("SELECT pg_notify('openlegal_collection','')")
            .execute(&mut *tx)
            .await
            .map_err(storage)?;
        commit_budget(tx, "open_demand_ticket").await?;
        let stop = CancellationToken::new();
        let failed = CancellationToken::new();
        let token = stop.clone();
        let failure = failed.clone();
        let heartbeat_pool = pool.clone();
        let renewer = tokio::spawn(async move {
            loop {
                tokio::select! { _ = token.cancelled() => return, _ = tokio::time::sleep(Duration::from_secs(5)) => {} }
                let result = sqlx::query("UPDATE openlegal.provider_demand_ticket SET lease_until=floor(extract(epoch from clock_timestamp()))::bigint+30 WHERE owner=$1 AND lease_until>floor(extract(epoch from clock_timestamp()))::bigint")
                    .bind(owner).execute(&heartbeat_pool).await;
                if !matches!(result, Ok(ref result) if result.rows_affected() == 1) {
                    failure.cancel();
                    return;
                }
            }
        });
        Ok(Self {
            pool: pool.clone(),
            owner,
            stop,
            failed,
            renewer,
        })
    }
    pub(super) fn failed(&self) -> &CancellationToken {
        &self.failed
    }
}
impl Drop for ForegroundTicket {
    fn drop(&mut self) {
        self.stop.cancel();
        self.renewer.abort();
        // Best-effort prompt cleanup; the durable 30-second lease is the
        // correctness bound if this process or its database connection fails.
        let pool = self.pool.clone();
        let owner = self.owner;
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                let _ = sqlx::query("DELETE FROM openlegal.provider_demand_ticket WHERE owner=$1")
                    .bind(owner)
                    .execute(&pool)
                    .await;
                let _ = sqlx::query("SELECT pg_notify('openlegal_collection','')")
                    .execute(&pool)
                    .await;
            });
        }
    }
}

/// Reserve under the shared budget lock; every denied/waiting path leaves counters intact.
#[allow(clippy::too_many_arguments)]
pub(super) async fn reserve(
    pool: &PgPool,
    mode: RequestBudgetMode,
    local_cap: Option<&Arc<AtomicU32>>,
    ticket: Option<&ForegroundTicket>,
    shared_events: &tokio::sync::OnceCell<CollectionEvents>,
    reservation_observer: Option<&Arc<AtomicBool>>,
    explicit_owner: Option<&(Uuid, u64)>,
    cancel: &CancellationToken,
) -> Result<ProviderRequestGuard, DatabaseError> {
    if cancel.is_cancelled() {
        return Err(DatabaseError::Cancelled);
    }
    let own_ticket = if ticket.is_none() {
        Some(
            ForegroundTicket::open_mode(
                pool,
                if mode == RequestBudgetMode::OnDemand {
                    "on_demand"
                } else {
                    "continuous"
                },
            )
            .await?,
        )
    } else {
        None
    };
    let ticket = ticket.or(own_ticket.as_ref());
    let mut events = None;
    loop {
        if cancel.is_cancelled() {
            return Err(DatabaseError::Cancelled);
        }
        if ticket.is_some_and(|ticket| ticket.failed.is_cancelled()) {
            return Err(DatabaseError::StorageUnavailable);
        }
        let connection = tokio::select! {
            _ = cancel.cancelled() => return Err(DatabaseError::Cancelled),
            result = pool.acquire() => result.map_err(acquire_error)?,
        };
        // Detach before acquiring any session lock: a cancelled query may have
        // acquired its lock, so this connection must never return to the pool.
        let mut connection = connection.detach();
        let decision = reserve_locked(
            &mut connection,
            mode,
            local_cap,
            ticket,
            reservation_observer,
            explicit_owner,
            cancel,
        )
        .await;
        match decision {
            Ok(Decision::Reserved(owner, slot)) => {
                return Ok(ProviderRequestGuard {
                    pool: pool.clone(),
                    connection: Some(connection),
                    owner,
                    slot,
                });
            }
            Ok(Decision::Wait(duration)) => {
                connection.close().await.map_err(storage)?;
                wait_change(pool, &mut events, shared_events, ticket, cancel, duration).await?;
            }
            Err(error) => {
                let _ = connection.close().await;
                return Err(error);
            }
        }
    }
}
enum Decision {
    Reserved(Uuid, i32),
    Wait(Duration),
}
async fn wait_change(
    pool: &PgPool,
    events: &mut Option<CollectionEvents>,
    shared_events: &tokio::sync::OnceCell<CollectionEvents>,
    ticket: Option<&ForegroundTicket>,
    cancel: &CancellationToken,
    maximum: Duration,
) -> Result<(), DatabaseError> {
    if events.is_none() {
        *events = Some(
            shared_events
                .get_or_try_init(|| CollectionEvents::open(pool))
                .await?
                .clone(),
        );
        // LISTEN first, then re-read the ledger to close the subscription race.
        return Ok(());
    }
    let wait = events
        .as_mut()
        .ok_or(DatabaseError::StorageUnavailable)?
        .wait(cancel, maximum.min(Duration::from_secs(5)));
    if let Some(ticket) = ticket {
        tokio::select! { _ = ticket.failed.cancelled() => Err(DatabaseError::StorageUnavailable), result = wait => result }
    } else {
        wait.await
    }
}

#[tracing::instrument(skip_all, fields(operation = "reserve_request"))]
async fn reserve_locked(
    connection: &mut PgConnection,
    mode: RequestBudgetMode,
    local_cap: Option<&Arc<AtomicU32>>,
    ticket: Option<&ForegroundTicket>,
    reservation_observer: Option<&Arc<AtomicBool>>,
    explicit_owner: Option<&(Uuid, u64)>,
    cancel: &CancellationToken,
) -> Result<Decision, DatabaseError> {
    let mut tx = connection.begin().await.map_err(storage)?;
    let row = lock_budget(&mut tx, "reserve_request", Some(cancel), true).await?;
    let number = |name| row.try_get::<i64, _>(name).map_err(storage);
    let now_ms: i64 =
        sqlx::query_scalar("SELECT floor(extract(epoch from clock_timestamp())*1000)::bigint")
            .fetch_one(&mut *tx)
            .await
            .map_err(storage)?;
    let now = now_ms / 1000;
    if let Some((owner, launched_at)) = explicit_owner {
        let launched_at = i64::try_from(*launched_at).map_err(|_| DatabaseError::InvalidInput)?;
        let authorized: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM openlegal.collection_request WHERE id=$1 AND launched_at=$2 AND status IN ('launching','running') AND lease_until>$3 FOR SHARE)")
            .bind(owner).bind(launched_at).bind(now).fetch_one(&mut *tx).await.map_err(storage)?;
        if !authorized {
            return Err(DatabaseError::Cancelled);
        }
    }
    let day = now / 86400;
    let current_day = number("utc_day")? == day;
    let daily = if current_day {
        number("daily_used")?
    } else {
        0
    };
    let on_demand = if current_day {
        number("on_demand_used")?
    } else {
        0
    };
    let continuous_limit: Option<i32> = row.try_get("continuous_daily_limit").map_err(storage)?;
    let demand_limit: Option<i32> = row.try_get("on_demand_daily_limit").map_err(storage)?;
    let (limit, used) = if mode == RequestBudgetMode::OnDemand {
        (demand_limit, on_demand)
    } else {
        (continuous_limit, daily)
    };
    let pilot_limit: Option<i32> = row.try_get("pilot_attempt_limit").map_err(storage)?;
    let pilot_used = number("pilot_used")?;
    let pilot_started: Option<i64> = row.try_get("pilot_started_at").map_err(storage)?;
    let pilot_duration = row
        .try_get::<Option<i64>, _>("pilot_duration_secs")
        .map_err(storage)?
        .unwrap_or(number("pilot_timeout_secs")?);
    let held: bool = sqlx::query_scalar("SELECT openlegal_admin.provider_held()")
        .fetch_one(&mut *tx)
        .await
        .map_err(storage)?;
    if held
        || row
            .try_get::<bool, _>("operator_suspended")
            .map_err(storage)?
        || row
            .try_get::<bool, _>("unresolved_response")
            .map_err(storage)?
        || limit.is_some_and(|limit| used >= i64::from(limit))
        || (mode == RequestBudgetMode::Pilot
            && (pilot_limit.is_some_and(|limit| pilot_used >= i64::from(limit))
                || pilot_started
                    .is_some_and(|started| now.saturating_sub(started) >= pilot_duration)))
    {
        sqlx::query("SELECT openlegal_admin.observe_uncertainty()")
            .execute(&mut *tx)
            .await
            .map_err(storage)?;
        commit_budget(tx, "reserve_request").await?;
        return Err(DatabaseError::BudgetExhausted);
    }
    let capacity: i32 = row.try_get("max_in_flight").map_err(storage)?;
    let Some(active) = live_owner_count(&mut tx).await? else {
        sqlx::query("SELECT openlegal_admin.observe_uncertainty()")
            .execute(&mut *tx)
            .await
            .map_err(storage)?;
        commit_budget(tx, "reserve_request").await?;
        return Err(DatabaseError::BudgetExhausted);
    };
    if active >= capacity as usize {
        return Ok(Decision::Wait(Duration::from_secs(5)));
    }
    sqlx::query("DELETE FROM openlegal.provider_demand_ticket WHERE lease_until<=$1")
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(storage)?;
    let last: Option<String> = row.try_get("last_admission_mode").map_err(storage)?;
    if mode != RequestBudgetMode::OnDemand
        && last.as_deref() != Some("on_demand")
        && demand_limit.is_none_or(|limit| on_demand < i64::from(limit))
        && demand_waiting(&mut tx, now).await?
    {
        return Ok(Decision::Wait(Duration::from_secs(5)));
    }
    if mode == RequestBudgetMode::OnDemand
        && last.as_deref() == Some("on_demand")
        && continuous_limit.is_none_or(|limit| daily < i64::from(limit))
    {
        let pending: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM openlegal.provider_demand_ticket WHERE mode='continuous' AND lease_until>$1)")
            .bind(now).fetch_one(&mut *tx).await.map_err(storage)?;
        if pending {
            return Ok(Decision::Wait(Duration::from_secs(5)));
        }
    }
    let next = number("next_allowed_at")?
        .saturating_mul(1000)
        .max(number("next_request_at_ms")?);
    if next > now_ms {
        if next - now_ms > 30000 {
            return Err(DatabaseError::BudgetExhausted);
        }
        return Ok(Decision::Wait(Duration::from_millis(
            (next - now_ms) as u64,
        )));
    }
    if let Some(ticket) = ticket {
        let live: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM openlegal.provider_demand_ticket WHERE owner=$1 AND lease_until>$2)")
            .bind(ticket.owner).bind(now).fetch_one(&mut *tx).await.map_err(storage)?;
        if !live {
            return Err(DatabaseError::StorageUnavailable);
        }
    }
    if cancel.is_cancelled() {
        return Err(DatabaseError::Cancelled);
    }
    // Session-lock ownership and the durable row are established under the same
    // budget lock as spacing and counters. Slots still settling may remain locked
    // briefly after deletion; skip them without spending an attempt.
    let mut slot = None;
    for candidate in 1..=capacity {
        let acquired: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1,$2)")
            .bind(PROVIDER_LOCK.0)
            .bind(PROVIDER_LOCK.1 + candidate)
            .fetch_one(&mut *tx)
            .await
            .map_err(storage)?;
        if acquired {
            slot = Some(candidate);
            break;
        }
    }
    let Some(slot) = slot else {
        return Ok(Decision::Wait(Duration::from_millis(50)));
    };
    let interval: i32 = row.try_get("interval_ms").map_err(storage)?;
    let owner: Uuid = sqlx::query_scalar("SELECT pg_catalog.uuidv7()")
        .fetch_one(&mut *tx)
        .await
        .map_err(storage)?;
    // Validate arithmetic before spending the operation-local allowance.
    let daily = daily
        .checked_add(i64::from(mode != RequestBudgetMode::OnDemand))
        .ok_or(DatabaseError::StorageCorrupt)?;
    let on_demand = on_demand
        .checked_add(i64::from(mode == RequestBudgetMode::OnDemand))
        .ok_or(DatabaseError::StorageCorrupt)?;
    let pilot_used = pilot_used
        .checked_add(i64::from(mode == RequestBudgetMode::Pilot))
        .ok_or(DatabaseError::StorageCorrupt)?;
    if let Some(cap) = local_cap {
        cap.fetch_update(Ordering::AcqRel, Ordering::Acquire, |left| {
            left.checked_sub(1)
        })
        .map_err(|_| DatabaseError::BudgetExhausted)?;
    }
    let reservation = async {
        let policy_mode = if mode == RequestBudgetMode::OnDemand { "on_demand" } else { "continuous" };
        let request_mode = if mode == RequestBudgetMode::Pilot { "pilot" } else { policy_mode };
        sqlx::query("UPDATE openlegal.provider_request_budget SET utc_day=$1,daily_used=$2,on_demand_used=$3,next_request_at_ms=$4,last_admission_mode=$5,pilot_started_at=CASE WHEN $6 THEN COALESCE(pilot_started_at,$7) ELSE pilot_started_at END,pilot_duration_secs=CASE WHEN $6 THEN COALESCE(pilot_duration_secs,pilot_timeout_secs) ELSE pilot_duration_secs END,pilot_used=$8 WHERE singleton")
            .bind(day).bind(daily).bind(on_demand).bind(now_ms.saturating_add(i64::from(interval))).bind(policy_mode).bind(mode == RequestBudgetMode::Pilot).bind(now).bind(pilot_used)
            .execute(&mut *tx).await.map_err(storage)?;
        sqlx::query("INSERT INTO openlegal.provider_request_admission(owner,slot,mode,started_at_ms,explicit_request_id,explicit_launched_at) VALUES($1,$2,$3,$4,$5,$6)")
            .bind(owner).bind(slot).bind(request_mode).bind(now_ms)
            .bind(explicit_owner.map(|(owner, _)| *owner))
            .bind(explicit_owner.map(|(_, launched_at)| *launched_at as i64))
            .execute(&mut *tx).await.map_err(storage)?;
        if let Some(ticket) = ticket {
            sqlx::query("DELETE FROM openlegal.provider_demand_ticket WHERE owner=$1").bind(ticket.owner).execute(&mut *tx).await.map_err(storage)?;
        }
        sqlx::query("SELECT pg_notify('openlegal_collection','')")
            .execute(&mut *tx).await.map_err(storage)?;
        // Once COMMIT can be transmitted, cancellation cannot prove the
        // reservation was uncharged. Treat an uncertain acknowledgement as an
        // attempted reservation; the durable marker continues to fail closed.
        if let Some(observer) = reservation_observer {
            observer.store(true, Ordering::Release);
        }
        commit_budget(tx, "reserve_request").await
    }.await;
    // A failed acknowledgement is uncertain, so do not refund an attempt
    // that may have committed. Fail-closed durable evidence remains the fence.
    reservation?;
    Ok(Decision::Reserved(owner, slot))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::law_go_kr::{LawClient, ProviderRequestLimits};
    use openlegal_application::{persistence::PersistentStore, upstream_policy::RequestLimit};

    mod lock_retry;

    async fn unrestricted(pool: &PgPool) {
        LawClient::configure_provider_request_limits(
            pool,
            &ProviderRequestLimits {
                continuous_daily_limit: RequestLimit::Unlimited,
                on_demand_daily_limit: RequestLimit::Unlimited,
                pilot_attempt_limit: RequestLimit::Unlimited,
                on_demand_attempt_limit: RequestLimit::Unlimited,
                interval_ms: 1,
                max_in_flight: 4,
                pilot_timeout_secs: 1800,
                on_demand_timeout_secs: 7200,
                max_job_attempts: 3,
            },
        )
        .await
        .unwrap();
    }
    async fn start(pool: &PgPool) -> ProviderRequestGuard {
        LawClient::reserve_provider_request_budget(
            pool,
            &RequestBudgetMode::Continuous,
            &CancellationToken::new(),
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    #[ignore = "requires scripts/test-postgres.sh"]
    async fn public_diagnostic_waits_for_normal_settlement_and_never_reports_deleted_owner_uncertain()
     {
        use openlegal_domain::provider_admin::ProviderAdmissionSnapshot;
        let fixture = crate::test_support::TestDatabase::new().await;
        let store = fixture.open(100).await;
        let pool = store.pool();
        unrestricted(&pool).await;
        let mut guard = start(&pool).await;
        let owner = guard.owner();
        // Exercise the exact DELETE/COMMIT then session-unlock phases of normal
        // guard settlement while another statement has already begun reading.
        let mut settlement = guard.connection.as_mut().unwrap().begin().await.unwrap();
        sqlx::query(
            "SELECT singleton FROM openlegal.provider_request_budget WHERE singleton FOR UPDATE",
        )
        .fetch_one(&mut *settlement)
        .await
        .unwrap();
        sqlx::query("DELETE FROM openlegal.provider_request_admission WHERE owner=$1")
            .bind(owner)
            .execute(&mut *settlement)
            .await
            .unwrap();
        let mut reader = PgConnection::connect_with(&pool.connect_options())
            .await
            .unwrap();
        sqlx::query("SET lock_timeout='0'")
            .execute(&mut reader)
            .await
            .unwrap();
        let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(&mut reader)
            .await
            .unwrap();
        let reading = tokio::spawn(async move {
            sqlx::query_scalar::<_, serde_json::Value>(
                "SELECT openlegal_admin.provider_diagnostic()",
            )
            .fetch_one(&mut reader)
            .await
            .unwrap()
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let blocked: bool = sqlx::query_scalar("SELECT COALESCE(wait_event_type='Lock',false) FROM pg_stat_activity WHERE pid=$1")
                    .bind(pid).fetch_one(&pool).await.unwrap();
                if blocked { break; }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        }).await.unwrap();
        settlement.commit().await.unwrap();
        guard.release().await.unwrap();
        let snapshot: ProviderAdmissionSnapshot =
            serde_json::from_value(reading.await.unwrap()).unwrap();
        assert_eq!(snapshot.active_slots, 0);
        assert_eq!(snapshot.abandoned_slots, 0);
        assert!(!snapshot.legacy_uncertain);
        assert!(!snapshot.continuous.requires_operator_review);
        assert_ne!(snapshot.continuous.reason, "provider_response_uncertain");
        store.close().await.unwrap();
    }

    #[tokio::test]
    #[ignore = "requires scripts/test-postgres.sh"]
    async fn deferral_snapshot_distinguishes_blockers_without_mutating_evidence_or_usage() {
        let fixture = crate::test_support::TestDatabase::new().await;
        let store = fixture.open(100).await;
        let pool = store.pool();
        unrestricted(&pool).await;
        sqlx::query("UPDATE openlegal.provider_request_budget SET utc_day=floor(extract(epoch from clock_timestamp()))::bigint/86400,daily_used=4,on_demand_used=3,on_demand_daily_limit=3")
            .execute(&pool).await.unwrap();
        let daily = deferral_snapshot(&pool, RequestBudgetMode::OnDemand, false, false)
            .await
            .unwrap();
        assert_eq!(daily.reason, "provider_daily_limit");
        let now: i64 =
            sqlx::query_scalar("SELECT floor(extract(epoch from clock_timestamp()))::bigint")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(daily.recheck_at, ((now / 86400 + 1) * 86400 + 10) as u64);
        sqlx::query("UPDATE openlegal.provider_request_budget SET utc_day=utc_day-1,next_allowed_at=floor(extract(epoch from clock_timestamp()))::bigint+120")
            .execute(&pool).await.unwrap();
        let pause = deferral_snapshot(&pool, RequestBudgetMode::OnDemand, false, false)
            .await
            .unwrap();
        assert_eq!(pause.reason, "provider_retry_after");
        assert!(pause.recheck_at >= (now + 120) as u64);
        sqlx::query("UPDATE openlegal.provider_request_budget SET operator_suspended=true")
            .execute(&pool)
            .await
            .unwrap();
        let suspended = deferral_snapshot(&pool, RequestBudgetMode::OnDemand, false, false)
            .await
            .unwrap();
        assert_eq!(suspended.reason, "provider_suspended");
        assert!(suspended.recheck_at >= (now + 3600) as u64);
        sqlx::query("UPDATE openlegal.provider_request_budget SET unresolved_response=true")
            .execute(&pool)
            .await
            .unwrap();
        assert_eq!(
            deferral_snapshot(&pool, RequestBudgetMode::OnDemand, false, false)
                .await
                .unwrap()
                .reason,
            "provider_response_uncertain"
        );
        let evidence: (bool, bool, i64, i64) = sqlx::query_as("SELECT unresolved_response,operator_suspended,daily_used,on_demand_used FROM openlegal.provider_request_budget")
            .fetch_one(&pool).await.unwrap();
        assert_eq!(evidence, (true, true, 4, 3));
        // Only the fixture operator clears fences; the diagnostic never does.
        sqlx::query("UPDATE openlegal.provider_request_budget SET unresolved_response=false,operator_suspended=false,next_allowed_at=0,next_request_at_ms=0")
            .execute(&pool).await.unwrap();
        assert_eq!(
            deferral_snapshot(&pool, RequestBudgetMode::OnDemand, true, false)
                .await
                .unwrap()
                .reason,
            "operation_attempt_limit"
        );
        assert_eq!(
            deferral_snapshot(&pool, RequestBudgetMode::OnDemand, false, false)
                .await
                .unwrap()
                .reason,
            "provider_admission_wait"
        );
        store.close().await.unwrap();
    }

    #[tokio::test]
    #[ignore = "requires scripts/test-postgres.sh"]
    async fn snapshot_distinguishes_live_and_abandoned_slots_including_lowered_capacity() {
        let fixture = crate::test_support::TestDatabase::new().await;
        let store = fixture.open(100).await;
        let pool = store.pool();
        unrestricted(&pool).await;
        let mut first = start(&pool).await;
        let second = start(&pool).await;
        sqlx::query("UPDATE openlegal.provider_request_budget SET max_in_flight=1")
            .execute(&pool)
            .await
            .unwrap();
        assert_eq!(
            deferral_snapshot(&pool, RequestBudgetMode::Continuous, false, false)
                .await
                .unwrap()
                .reason,
            "provider_admission_wait"
        );
        let abandoned_owner = second.owner();
        drop(second);
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let status = deferral_snapshot(&pool, RequestBudgetMode::Continuous, false, false)
                    .await
                    .unwrap();
                if status.reason == "provider_response_uncertain" {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let owners: Vec<Uuid> =
            sqlx::query_scalar("SELECT owner FROM openlegal.provider_request_admission")
                .fetch_all(&pool)
                .await
                .unwrap();
        assert!(owners.contains(&abandoned_owner));
        first.complete().await.unwrap();
        assert_eq!(
            deferral_snapshot(&pool, RequestBudgetMode::Continuous, false, false)
                .await
                .unwrap()
                .reason,
            "provider_response_uncertain"
        );
        store.close().await.unwrap();
    }

    #[tokio::test]
    #[ignore = "requires scripts/test-postgres.sh"]
    async fn healthy_contention_wakes_after_completion_without_spending_while_busy() {
        let fixture = crate::test_support::TestDatabase::new().await;
        let store = fixture.open(100).await;
        let pool = store.pool();
        unrestricted(&pool).await;
        sqlx::query("UPDATE openlegal.provider_request_budget SET max_in_flight=1")
            .execute(&pool)
            .await
            .unwrap();
        let mut first = start(&pool).await;
        let next_pool = pool.clone();
        let mut next = tokio::spawn(async move { start(&next_pool).await });
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut next)
                .await
                .is_err()
        );
        let charged: i64 = sqlx::query_scalar(
            "SELECT daily_used FROM openlegal.provider_request_budget WHERE singleton",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(charged, 1);
        first.complete().await.unwrap();
        let mut second = tokio::time::timeout(Duration::from_secs(2), next)
            .await
            .unwrap()
            .unwrap();
        let charged: i64 = sqlx::query_scalar(
            "SELECT daily_used FROM openlegal.provider_request_budget WHERE singleton",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(charged, 2);
        second.complete().await.unwrap();
        store.close().await.unwrap();
    }

    #[tokio::test]
    #[ignore = "requires scripts/test-postgres.sh"]
    async fn abandoned_owner_retains_uncertain_evidence_and_blocks_next_start() {
        let fixture = crate::test_support::TestDatabase::new().await;
        let store = fixture.open(100).await;
        let pool = store.pool();
        unrestricted(&pool).await;
        let mut first = start(&pool).await;
        let owner = first.owner();
        // Simulate loss of the detached session with a acknowledged close.
        first.connection.take().unwrap().close().await.unwrap();
        drop(first);
        assert_eq!(
            LawClient::reserve_provider_request_budget(
                &pool,
                &RequestBudgetMode::Continuous,
                &CancellationToken::new()
            )
            .await,
            Err(DatabaseError::BudgetExhausted)
        );
        let state: (Uuid, i64) = sqlx::query_as("SELECT owner, daily_used FROM openlegal.provider_request_admission CROSS JOIN openlegal.provider_request_budget WHERE singleton").fetch_one(&pool).await.unwrap();
        assert_eq!(state, (owner, 1));
        store.close().await.unwrap();
    }

    #[tokio::test]
    #[ignore = "requires scripts/test-postgres.sh"]
    async fn late_rejection_suspends_without_clearing_newer_owner() {
        let fixture = crate::test_support::TestDatabase::new().await;
        let store = fixture.open(100).await;
        let pool = store.pool();
        unrestricted(&pool).await;
        let mut first = start(&pool).await;
        first.complete().await.unwrap();
        let mut second = start(&pool).await;
        assert_eq!(first.pause(60).await, Err(DatabaseError::Conflict));
        first.suspend().await.unwrap();
        let state: (bool, Uuid) = sqlx::query_as("SELECT operator_suspended,owner FROM openlegal.provider_request_budget CROSS JOIN openlegal.provider_request_admission WHERE singleton").fetch_one(&pool).await.unwrap();
        assert_eq!(state, (true, second.owner()));
        second.complete().await.unwrap();
        let suspended: bool = sqlx::query_scalar(
            "SELECT operator_suspended FROM openlegal.provider_request_budget WHERE singleton",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(suspended);
        store.close().await.unwrap();
    }

    #[tokio::test]
    #[ignore = "requires scripts/test-postgres.sh"]
    async fn waiting_foreground_ticket_yields_background_then_cleanup_wakes_it() {
        let fixture = crate::test_support::TestDatabase::new().await;
        let store = fixture.open(100).await;
        let pool = store.pool();
        unrestricted(&pool).await;
        let ticket = ForegroundTicket::open(&pool).await.unwrap();
        let next_pool = pool.clone();
        let mut next = tokio::spawn(async move { start(&next_pool).await });
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut next)
                .await
                .is_err()
        );
        let charged: i64 = sqlx::query_scalar(
            "SELECT daily_used FROM openlegal.provider_request_budget WHERE singleton",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(charged, 0);
        drop(ticket);
        let mut guard = tokio::time::timeout(Duration::from_secs(2), next)
            .await
            .unwrap()
            .unwrap();
        guard.complete().await.unwrap();
        store.close().await.unwrap();
    }

    #[tokio::test]
    #[ignore = "requires scripts/test-postgres.sh"]
    async fn quota_blocked_and_expired_foreground_tickets_do_not_block_background() {
        let fixture = crate::test_support::TestDatabase::new().await;
        let store = fixture.open(100).await;
        let pool = store.pool();
        unrestricted(&pool).await;
        sqlx::query("UPDATE openlegal.provider_request_budget SET utc_day=floor(extract(epoch from clock_timestamp()))::bigint/86400,on_demand_daily_limit=1,on_demand_used=1")
            .execute(&pool).await.unwrap();
        let ticket = ForegroundTicket::open(&pool).await.unwrap();
        let mut guard = tokio::time::timeout(Duration::from_secs(2), start(&pool))
            .await
            .unwrap();
        guard.complete().await.unwrap();
        sqlx::query("UPDATE openlegal.provider_request_budget SET on_demand_daily_limit=NULL")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("UPDATE openlegal.provider_demand_ticket SET lease_until=0 WHERE owner=$1")
            .bind(ticket.owner)
            .execute(&pool)
            .await
            .unwrap();
        let mut guard = tokio::time::timeout(Duration::from_secs(2), start(&pool))
            .await
            .unwrap();
        let count: i64 =
            sqlx::query_scalar("SELECT count(*) FROM openlegal.provider_demand_ticket")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(count, 0);
        guard.complete().await.unwrap();
        drop(ticket);
        store.close().await.unwrap();
    }

    #[tokio::test]
    #[ignore = "requires scripts/test-postgres.sh"]
    async fn healthy_wait_cancellation_preserves_local_and_durable_attempts() {
        let fixture = crate::test_support::TestDatabase::new().await;
        let store = fixture.open(100).await;
        let pool = store.pool();
        unrestricted(&pool).await;
        sqlx::query("UPDATE openlegal.provider_request_budget SET max_in_flight=1")
            .execute(&pool)
            .await
            .unwrap();
        let mut guard = start(&pool).await;
        let cap = Arc::new(AtomicU32::new(1));
        let cancel = CancellationToken::new();
        let events = tokio::sync::OnceCell::new();
        let observer = Arc::new(AtomicBool::new(false));
        let pending = reserve(
            &pool,
            RequestBudgetMode::Continuous,
            Some(&cap),
            None,
            &events,
            Some(&observer),
            None,
            &cancel,
        );
        tokio::pin!(pending);
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut pending)
                .await
                .is_err()
        );
        cancel.cancel();
        assert_eq!(pending.await, Err(DatabaseError::Cancelled));
        assert!(!observer.load(Ordering::Acquire));
        assert_eq!(cap.load(Ordering::Acquire), 1);
        let charged: i64 = sqlx::query_scalar(
            "SELECT daily_used FROM openlegal.provider_request_budget WHERE singleton",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(charged, 1);
        guard.complete().await.unwrap();
        store.close().await.unwrap();
    }

    #[tokio::test]
    #[ignore = "requires scripts/test-postgres.sh"]
    async fn receipt_priority_requires_live_scheduler_or_bounded_launch_window() {
        let fixture = crate::test_support::TestDatabase::new().await;
        let store = fixture.open(100).await;
        let pool = store.pool();
        unrestricted(&pool).await;
        let request: Uuid = sqlx::query_scalar("INSERT INTO openlegal.collection_request(request_key,canonical_key,payload,status,created_at,expires_at) VALUES(repeat('0',64),repeat('0',64),'{}','queued',floor(extract(epoch from clock_timestamp()))::bigint,floor(extract(epoch from clock_timestamp()))::bigint+86400) RETURNING id")
            .fetch_one(&pool).await.unwrap();
        // A dead scheduler cannot turn a retained receipt into a 24-hour pause.
        assert!(idle(&pool, RequestBudgetMode::Continuous).await.unwrap());
        let mut guard = tokio::time::timeout(Duration::from_secs(2), start(&pool))
            .await
            .unwrap();
        guard.complete().await.unwrap();
        sqlx::query("UPDATE openlegal.corpus_control SET collection_scheduler_seen_at=floor(extract(epoch from clock_timestamp()))::bigint WHERE singleton")
            .execute(&pool).await.unwrap();
        assert!(!idle(&pool, RequestBudgetMode::Continuous).await.unwrap());
        let next_pool = pool.clone();
        let mut pending = tokio::spawn(async move { start(&next_pool).await });
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut pending)
                .await
                .is_err()
        );
        // A launch that never acquired a foreground ticket relinquishes priority.
        sqlx::query("UPDATE openlegal.collection_request SET status='launching',launched_at=floor(extract(epoch from clock_timestamp()))::bigint-31 WHERE id=$1")
            .bind(request).execute(&pool).await.unwrap();
        sqlx::query("SELECT pg_notify('openlegal_collection','')")
            .execute(&pool)
            .await
            .unwrap();
        let mut guard = tokio::time::timeout(Duration::from_secs(2), pending)
            .await
            .unwrap()
            .unwrap();
        guard.complete().await.unwrap();
        assert!(idle(&pool, RequestBudgetMode::Continuous).await.unwrap());
        // Document-processing Jobs occupy launch slots, but no HTTP capacity.
        sqlx::query("UPDATE openlegal.collection_request SET status='queued' WHERE id=$1")
            .bind(request)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO openlegal.collection_request(request_key,canonical_key,payload,status,created_at,expires_at,launched_at) SELECT lpad(i::text,64,'0'),lpad(i::text,64,'0'),'{}','running',floor(extract(epoch from clock_timestamp()))::bigint,floor(extract(epoch from clock_timestamp()))::bigint+86400,floor(extract(epoch from clock_timestamp()))::bigint-31 FROM generate_series(1,16) i")
            .execute(&pool).await.unwrap();
        assert!(idle(&pool, RequestBudgetMode::Continuous).await.unwrap());
        let mut guard = tokio::time::timeout(Duration::from_secs(2), start(&pool))
            .await
            .unwrap();
        guard.complete().await.unwrap();
        store.close().await.unwrap();
    }
    #[test]
    fn default_policy_and_parallel_bounds_are_explicit() {
        let policy = ProviderRequestLimits::default();
        assert_eq!(policy.interval_ms, 200);
        assert_eq!(policy.max_in_flight, 4);
        assert_eq!(policy.continuous_daily_limit, RequestLimit::Unlimited);
        assert_eq!(policy.on_demand_daily_limit, RequestLimit::Limited(1000));
        for capacity in [0, 17] {
            assert_eq!(
                ProviderRequestLimits {
                    max_in_flight: capacity,
                    ..policy
                }
                .validated(),
                Err(DatabaseError::InvalidInput)
            );
        }
        for capacity in [1, 4, 16] {
            assert!(
                ProviderRequestLimits {
                    max_in_flight: capacity,
                    ..policy
                }
                .validated()
                .is_ok()
            );
        }
    }

    #[tokio::test]
    #[ignore = "requires scripts/test-postgres.sh"]
    async fn four_live_owners_share_spacing_and_fifth_waits_without_spending() {
        let fixture = crate::test_support::TestDatabase::new().await;
        let store = fixture.open(100).await;
        let pool = store.pool();
        let policy = ProviderRequestLimits {
            on_demand_daily_limit: RequestLimit::Unlimited,
            ..ProviderRequestLimits::default()
        };
        LawClient::configure_provider_request_limits(&pool, &policy)
            .await
            .unwrap();
        let mut guards = Vec::new();
        for _ in 0..4 {
            guards.push(start(&pool).await);
        }
        let starts: Vec<i64> = sqlx::query_scalar(
            "SELECT started_at_ms FROM openlegal.provider_request_admission ORDER BY started_at_ms",
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(starts.len(), 4);
        assert!(starts.windows(2).all(|pair| pair[1] - pair[0] >= 200));
        let waiting_pool = pool.clone();
        let mut waiting = tokio::spawn(async move { start(&waiting_pool).await });
        assert!(
            tokio::time::timeout(Duration::from_millis(250), &mut waiting)
                .await
                .is_err()
        );
        let charged: i64 = sqlx::query_scalar(
            "SELECT daily_used FROM openlegal.provider_request_budget WHERE singleton",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(charged, 4);
        guards[0].complete().await.unwrap();
        let mut fifth = tokio::time::timeout(Duration::from_secs(2), waiting)
            .await
            .unwrap()
            .unwrap();
        let count: i64 =
            sqlx::query_scalar("SELECT count(*) FROM openlegal.provider_request_admission")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(count, 4);
        for guard in &mut guards[1..] {
            guard.complete().await.unwrap();
        }
        fifth.complete().await.unwrap();
        store.close().await.unwrap();
    }

    #[tokio::test]
    #[ignore = "requires scripts/test-postgres.sh"]
    async fn one_abandoned_parallel_owner_fences_all_slots_and_survives_other_completion() {
        let fixture = crate::test_support::TestDatabase::new().await;
        let store = fixture.open(100).await;
        let pool = store.pool();
        unrestricted(&pool).await;
        let mut survivor = start(&pool).await;
        let mut abandoned = start(&pool).await;
        let owner = abandoned.owner();
        abandoned.connection.take().unwrap().close().await.unwrap();
        drop(abandoned);
        assert!(!idle(&pool, RequestBudgetMode::Continuous).await.unwrap());
        assert_eq!(
            deferral_snapshot(&pool, RequestBudgetMode::Continuous, false, false)
                .await
                .unwrap()
                .reason,
            "provider_response_uncertain"
        );
        assert_eq!(
            LawClient::reserve_provider_request_budget(
                &pool,
                &RequestBudgetMode::Continuous,
                &CancellationToken::new()
            )
            .await,
            Err(DatabaseError::BudgetExhausted)
        );
        survivor.complete().await.unwrap();
        let pending: Vec<Uuid> =
            sqlx::query_scalar("SELECT owner FROM openlegal.provider_request_admission")
                .fetch_all(&pool)
                .await
                .unwrap();
        assert_eq!(pending, vec![owner]);
        assert_eq!(
            LawClient::reserve_provider_request_budget(
                &pool,
                &RequestBudgetMode::OnDemand,
                &CancellationToken::new()
            )
            .await,
            Err(DatabaseError::BudgetExhausted)
        );
        let charged: i64 = sqlx::query_scalar(
            "SELECT daily_used FROM openlegal.provider_request_budget WHERE singleton",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(charged, 2);
        store.close().await.unwrap();
    }

    #[tokio::test]
    #[ignore = "requires scripts/test-postgres.sh"]
    async fn retry_after_settles_only_its_owner_and_preserves_shared_pause() {
        let fixture = crate::test_support::TestDatabase::new().await;
        let store = fixture.open(100).await;
        let pool = store.pool();
        unrestricted(&pool).await;
        let mut first = start(&pool).await;
        let mut second = start(&pool).await;
        first.pause(120).await.unwrap();
        let owners: Vec<Uuid> =
            sqlx::query_scalar("SELECT owner FROM openlegal.provider_request_admission")
                .fetch_all(&pool)
                .await
                .unwrap();
        assert_eq!(owners, vec![second.owner()]);
        assert_eq!(
            LawClient::reserve_provider_request_budget(
                &pool,
                &RequestBudgetMode::Continuous,
                &CancellationToken::new()
            )
            .await,
            Err(DatabaseError::BudgetExhausted)
        );
        let before: i64 = sqlx::query_scalar(
            "SELECT next_allowed_at FROM openlegal.provider_request_budget WHERE singleton",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        second.complete().await.unwrap();
        let after: i64 = sqlx::query_scalar(
            "SELECT next_allowed_at FROM openlegal.provider_request_budget WHERE singleton",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(before, after);
        store.close().await.unwrap();
    }

    #[tokio::test]
    #[ignore = "requires scripts/test-postgres.sh"]
    async fn both_waiting_modes_alternate_without_foreground_starving_inventory() {
        let fixture = crate::test_support::TestDatabase::new().await;
        let store = fixture.open(100).await;
        let pool = store.pool();
        unrestricted(&pool).await;
        let background = ForegroundTicket::open_mode(&pool, "continuous")
            .await
            .unwrap();
        let foreground = ForegroundTicket::open(&pool).await.unwrap();
        let events = tokio::sync::OnceCell::new();
        let cancel = CancellationToken::new();
        let mut demand = reserve(
            &pool,
            RequestBudgetMode::OnDemand,
            None,
            Some(&foreground),
            &events,
            None,
            None,
            &cancel,
        )
        .await
        .unwrap();
        demand.complete().await.unwrap();
        let next_foreground = ForegroundTicket::open(&pool).await.unwrap();
        // The next continuous request wins even with another foreground waiter.
        let mut continuous = reserve(
            &pool,
            RequestBudgetMode::Continuous,
            None,
            Some(&background),
            &events,
            None,
            None,
            &cancel,
        )
        .await
        .unwrap();
        continuous.complete().await.unwrap();
        let next_background = ForegroundTicket::open_mode(&pool, "continuous")
            .await
            .unwrap();
        let mut demand = reserve(
            &pool,
            RequestBudgetMode::OnDemand,
            None,
            Some(&next_foreground),
            &events,
            None,
            None,
            &cancel,
        )
        .await
        .unwrap();
        demand.complete().await.unwrap();
        let charged: (i64, i64) = sqlx::query_as("SELECT daily_used,on_demand_used FROM openlegal.provider_request_budget WHERE singleton")
            .fetch_one(&pool).await.unwrap();
        assert_eq!(charged, (1, 2));
        drop(next_background);
        drop(next_foreground);
        drop(background);
        drop(foreground);
        store.close().await.unwrap();
    }
}
