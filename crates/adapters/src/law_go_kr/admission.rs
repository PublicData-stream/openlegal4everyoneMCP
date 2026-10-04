//! Cross-Pod LAW HTTP ownership. No database transaction spans a network fetch.
use super::{DatabaseError, PgPool, RequestBudgetMode};
use crate::collection_events::CollectionEvents;
use sqlx::{Connection, PgConnection, Row};
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

fn storage(_: sqlx::Error) -> DatabaseError {
    DatabaseError::StorageUnavailable
}

/// Owns one charged attempt and its detached advisory-lock session. Dropping an
/// unsettled guard closes the session while retaining the durable uncertain marker.
pub struct ProviderRequestGuard {
    pool: PgPool,
    connection: Option<PgConnection>,
    owner: Uuid,
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
    /// Settle complete local response evidence and release HTTP admission before parsing.
    pub async fn complete(&mut self) -> Result<(), DatabaseError> {
        let connection = self.connection.as_mut().ok_or(DatabaseError::Conflict)?;
        let changed = sqlx::query("UPDATE openlegal.provider_request_budget SET unresolved_response=false,admission_owner=NULL WHERE singleton AND admission_owner=$1 AND unresolved_response")
            .bind(self.owner).execute(&mut *connection).await.map_err(storage)?.rows_affected();
        if changed != 1 {
            return Err(DatabaseError::Conflict);
        }
        self.release().await
    }
    /// Honor a response's pause without clearing a newer attempt's evidence.
    pub async fn pause(&mut self, delay: u64) -> Result<(), DatabaseError> {
        let connection = self.connection.as_mut().ok_or(DatabaseError::Conflict)?;
        let delay = i64::try_from(delay)
            .unwrap_or(i64::MAX / 4)
            .min(i64::MAX / 4);
        let changed = sqlx::query("UPDATE openlegal.provider_request_budget SET next_allowed_at=GREATEST(next_allowed_at,floor(extract(epoch from clock_timestamp()))::bigint+$2),operator_suspended=operator_suspended OR $3,unresolved_response=false,admission_owner=NULL WHERE singleton AND admission_owner=$1 AND unresolved_response")
            .bind(self.owner).bind(delay).bind(delay > 7 * 86400)
            .execute(&mut *connection).await.map_err(storage)?.rows_affected();
        if changed != 1 {
            return Err(DatabaseError::Conflict);
        }
        self.release().await
    }
    /// Rejection may become known during parsing, after another HTTP call starts.
    /// Suspend globally, but settle the marker only if this response still owns it.
    pub async fn suspend(&mut self) -> Result<(), DatabaseError> {
        suspend_owned_response(&self.pool, self.owner).await?;
        self.release().await
    }
    async fn release(&mut self) -> Result<(), DatabaseError> {
        if let Some(mut connection) = self.connection.take() {
            sqlx::query("SELECT pg_advisory_unlock($1,$2)")
                .bind(PROVIDER_LOCK.0)
                .bind(PROVIDER_LOCK.1)
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

pub(super) async fn suspend_owned_response(
    pool: &PgPool,
    owner: Uuid,
) -> Result<(), DatabaseError> {
    let changed = sqlx::query("UPDATE openlegal.provider_request_budget SET operator_suspended=true,unresolved_response=CASE WHEN admission_owner=$1 THEN false ELSE unresolved_response END,admission_owner=CASE WHEN admission_owner=$1 THEN NULL ELSE admission_owner END WHERE singleton")
        .bind(owner).execute(pool).await.map_err(storage)?.rows_affected();
    if changed != 1 {
        return Err(DatabaseError::StorageUnavailable);
    }
    sqlx::query("SELECT pg_notify('openlegal_collection','')")
        .execute(pool)
        .await
        .map_err(storage)?;
    Ok(())
}

pub(super) async fn idle(pool: &PgPool, mode: RequestBudgetMode) -> Result<bool, DatabaseError> {
    let mut tx = pool.begin().await.map_err(storage)?;
    let acquired: bool = sqlx::query_scalar("SELECT pg_try_advisory_xact_lock($1,$2)")
        .bind(PROVIDER_LOCK.0)
        .bind(PROVIDER_LOCK.1)
        .fetch_one(&mut *tx)
        .await
        .map_err(storage)?;
    if !acquired {
        return Ok(false);
    }
    let ready: bool = sqlx::query_scalar(
        "SELECT NOT operator_suspended AND NOT unresolved_response
        AND GREATEST(next_allowed_at::numeric*1000,next_request_at_ms::numeric)<=floor(extract(epoch from clock_timestamp())*1000)::bigint
        AND ($1::bool AND (on_demand_daily_limit IS NULL OR utc_day<>floor(extract(epoch from clock_timestamp()))::bigint/86400 OR on_demand_used<on_demand_daily_limit)
          OR NOT $1::bool AND (continuous_daily_limit IS NULL OR utc_day<>floor(extract(epoch from clock_timestamp()))::bigint/86400 OR daily_used<continuous_daily_limit))
        AND ($1::bool OR NOT (on_demand_daily_limit IS NULL OR utc_day<>floor(extract(epoch from clock_timestamp()))::bigint/86400 OR on_demand_used<on_demand_daily_limit)
          OR (NOT EXISTS(SELECT 1 FROM openlegal.provider_demand_ticket WHERE lease_until>floor(extract(epoch from clock_timestamp()))::bigint)
            AND NOT EXISTS(SELECT 1 FROM openlegal.collection_request WHERE expires_at>floor(extract(epoch from clock_timestamp()))::bigint AND ((status='launching' AND launched_at>=floor(extract(epoch from clock_timestamp()))::bigint-30) OR ((status='queued' OR (status='deferred' AND lease_until<=floor(extract(epoch from clock_timestamp()))::bigint)) AND EXISTS(SELECT 1 FROM openlegal.corpus_control WHERE singleton AND collection_scheduler_seen_at>=floor(extract(epoch from clock_timestamp()))::bigint-30) AND (SELECT count(*) FROM openlegal.collection_request WHERE status IN ('launching','running'))<16)))))
        AND (NOT $2::bool OR ((pilot_attempt_limit IS NULL OR pilot_used<pilot_attempt_limit)
          AND (pilot_started_at IS NULL OR floor(extract(epoch from clock_timestamp()))::bigint-pilot_started_at<COALESCE(pilot_duration_secs,pilot_timeout_secs))))
        FROM openlegal.provider_request_budget WHERE singleton"
    ).bind(mode == RequestBudgetMode::OnDemand).bind(mode == RequestBudgetMode::Pilot)
        .fetch_one(&mut *tx).await.map_err(storage)?;
    Ok(ready)
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
        let mut tx = pool.begin().await.map_err(storage)?;
        let owner: Uuid = sqlx::query_scalar("SELECT pg_catalog.uuidv7()")
            .fetch_one(&mut *tx)
            .await
            .map_err(storage)?;
        sqlx::query(
            "SELECT singleton FROM openlegal.provider_request_budget WHERE singleton FOR UPDATE",
        )
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
        sqlx::query("INSERT INTO openlegal.provider_demand_ticket(owner,lease_until) VALUES($1,floor(extract(epoch from clock_timestamp()))::bigint+30)")
            .bind(owner).execute(&mut *tx).await.map_err(storage)?;
        sqlx::query("SELECT pg_notify('openlegal_collection','')")
            .execute(&mut *tx)
            .await
            .map_err(storage)?;
        tx.commit().await.map_err(storage)?;
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

/// Reserve under the session lock; every denied/waiting path leaves counters intact.
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
            result = pool.acquire() => result.map_err(storage)?,
        };
        // Detach before the first session-lock query: cancellation while its
        // response is in flight must close this session, never pool a lock that
        // PostgreSQL may already have acquired.
        let mut connection = connection.detach();
        let acquired: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1,$2)")
            .bind(PROVIDER_LOCK.0)
            .bind(PROVIDER_LOCK.1)
            .fetch_one(&mut connection)
            .await
            .map_err(storage)?;
        if !acquired {
            connection.close().await.map_err(storage)?;
            wait_change(
                pool,
                &mut events,
                shared_events,
                ticket,
                cancel,
                Duration::from_secs(5),
            )
            .await?;
            continue;
        }
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
            Ok(Decision::Reserved(owner)) => {
                return Ok(ProviderRequestGuard {
                    pool: pool.clone(),
                    connection: Some(connection),
                    owner,
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
    Reserved(Uuid),
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
    let row = sqlx::query("SELECT *,floor(extract(epoch from clock_timestamp())*1000)::bigint AS now_ms FROM openlegal.provider_request_budget WHERE singleton FOR UPDATE")
        .fetch_one(&mut *tx).await.map_err(storage)?;
    let number = |name| row.try_get::<i64, _>(name).map_err(storage);
    let now_ms = number("now_ms")?;
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
    if row
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
        return Err(DatabaseError::BudgetExhausted);
    }
    sqlx::query("DELETE FROM openlegal.provider_demand_ticket WHERE lease_until<=$1")
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(storage)?;
    if mode != RequestBudgetMode::OnDemand
        && demand_limit.is_none_or(|limit| on_demand < i64::from(limit))
    {
        let pending: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM openlegal.provider_demand_ticket WHERE lease_until>$1) OR EXISTS(SELECT 1 FROM openlegal.collection_request WHERE expires_at>$1 AND ((status='launching' AND launched_at>=$1-30) OR ((status='queued' OR (status='deferred' AND lease_until<=$1)) AND EXISTS(SELECT 1 FROM openlegal.corpus_control WHERE singleton AND collection_scheduler_seen_at>=$1-30) AND (SELECT count(*) FROM openlegal.collection_request WHERE status IN ('launching','running'))<16)))")
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
        sqlx::query("UPDATE openlegal.provider_request_budget SET utc_day=$1,daily_used=$2,on_demand_used=$3,next_request_at_ms=$4,unresolved_response=true,admission_owner=$5,pilot_started_at=CASE WHEN $6 THEN COALESCE(pilot_started_at,$7) ELSE pilot_started_at END,pilot_duration_secs=CASE WHEN $6 THEN COALESCE(pilot_duration_secs,pilot_timeout_secs) ELSE pilot_duration_secs END,pilot_used=$8 WHERE singleton")
            .bind(day).bind(daily).bind(on_demand).bind(now_ms.saturating_add(i64::from(interval))).bind(owner).bind(mode == RequestBudgetMode::Pilot).bind(now).bind(pilot_used)
            .execute(&mut *tx).await.map_err(storage)?;
        if let Some(ticket) = ticket {
            sqlx::query("DELETE FROM openlegal.provider_demand_ticket WHERE owner=$1").bind(ticket.owner).execute(&mut *tx).await.map_err(storage)?;
        }
        // Once COMMIT can be transmitted, cancellation cannot prove the
        // reservation was uncharged. Treat an uncertain acknowledgement as an
        // attempted reservation; the durable marker continues to fail closed.
        if let Some(observer) = reservation_observer {
            observer.store(true, Ordering::Release);
        }
        tx.commit().await.map_err(storage)
    }.await;
    // A failed acknowledgement is uncertain, so do not refund an attempt
    // that may have committed. Fail-closed durable evidence remains the fence.
    reservation?;
    Ok(Decision::Reserved(owner))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::law_go_kr::{LawClient, ProviderRequestLimits};
    use openlegal_application::{persistence::PersistentStore, upstream_policy::RequestLimit};

    async fn unrestricted(pool: &PgPool) {
        LawClient::configure_provider_request_limits(
            pool,
            &ProviderRequestLimits {
                continuous_daily_limit: RequestLimit::Unlimited,
                on_demand_daily_limit: RequestLimit::Unlimited,
                pilot_attempt_limit: RequestLimit::Unlimited,
                on_demand_attempt_limit: RequestLimit::Unlimited,
                interval_ms: 1,
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
    async fn healthy_contention_wakes_after_completion_without_spending_while_busy() {
        let fixture = crate::test_support::TestDatabase::new().await;
        let store = fixture.open(100).await;
        let pool = store.pool();
        unrestricted(&pool).await;
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
        let first = start(&pool).await;
        let owner = first.owner();
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
        let state: (bool,Option<Uuid>,i64) = sqlx::query_as("SELECT unresolved_response,admission_owner,daily_used FROM openlegal.provider_request_budget WHERE singleton").fetch_one(&pool).await.unwrap();
        assert_eq!(state, (true, Some(owner), 1));
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
        let state: (bool,bool,Option<Uuid>) = sqlx::query_as("SELECT operator_suspended,unresolved_response,admission_owner FROM openlegal.provider_request_budget WHERE singleton").fetch_one(&pool).await.unwrap();
        assert_eq!(state, (true, true, Some(second.owner())));
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
}
