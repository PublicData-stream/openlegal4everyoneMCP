//! Real PostgreSQL rejection barriers for the first admission row lock.
use super::*;
use sqlx::postgres::PgPoolOptions;
use std::sync::atomic::{AtomicU64, Ordering};
use tracing::{Subscriber, instrument::WithSubscriber};

#[derive(Clone)]
struct Rejections {
    attempts: tokio::sync::watch::Sender<usize>,
    span_id: Arc<AtomicU64>,
}
impl Rejections {
    fn new() -> Self {
        Self {
            attempts: tokio::sync::watch::channel(0).0,
            span_id: Arc::new(AtomicU64::new(1)),
        }
    }
    fn count(&self) -> usize {
        *self.attempts.borrow()
    }
    async fn wait(&self, count: usize) {
        let mut receiver = self.attempts.subscribe();
        let reached = tokio::time::timeout(Duration::from_secs(10), async {
            while *receiver.borrow_and_update() < count {
                receiver.changed().await.unwrap();
            }
        })
        .await;
        assert!(
            reached.is_ok(),
            "expected {count} acknowledged rejections, observed {}",
            self.count()
        );
    }
}
impl Subscriber for Rejections {
    fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
        true
    }
    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(self.span_id.fetch_add(1, Ordering::Relaxed))
    }
    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
    fn enter(&self, _: &tracing::span::Id) {}
    fn exit(&self, _: &tracing::span::Id) {}
    fn event(&self, event: &tracing::Event<'_>) {
        #[derive(Default)]
        struct Fields {
            attempt: Option<usize>,
            lock_rejected: bool,
        }
        impl tracing::field::Visit for Fields {
            fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
                if field.name() == "attempt" {
                    self.attempt = Some(value as usize);
                }
            }
            fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
                if field.name() == "sqlstate" && value == "55P03" {
                    self.lock_rejected = true;
                }
            }
            fn record_debug(&mut self, _: &tracing::field::Field, _: &dyn std::fmt::Debug) {}
        }
        let mut fields = Fields::default();
        event.record(&mut fields);
        if fields.lock_rejected
            && let Some(attempt) = fields.attempt
        {
            self.attempts.send_replace(attempt);
        }
    }
}

async fn retry_pool(pool: &PgPool) -> PgPool {
    PgPoolOptions::new()
        .max_connections(4)
        .after_connect(|connection, _| {
            Box::pin(async move {
                sqlx::raw_sql("SET lock_timeout='1s'; SET statement_timeout='2s'; SET transaction_timeout='5s'")
                    .execute(connection)
                    .await?;
                Ok(())
            })
        })
        .connect_with((*pool.connect_options()).clone())
        .await
        .unwrap()
}

async fn block_budget(pool: &PgPool) -> sqlx::Transaction<'static, sqlx::Postgres> {
    let mut connection = pool.acquire().await.unwrap();
    // The blocker idles while the subject retries, exceeding both the fixture's
    // five-second transaction and idle-in-transaction deadlines. Disable ONLY
    // these barrier-session deadlines before BEGIN and verify both settings.
    // Close the session afterwards so zero never escapes into the runtime pool.
    connection.close_on_drop();
    sqlx::raw_sql("SET transaction_timeout='0'; SET idle_in_transaction_session_timeout='0'")
        .execute(&mut *connection)
        .await
        .unwrap();
    let configured: (String, String) = sqlx::query_as("SELECT current_setting('transaction_timeout'),current_setting('idle_in_transaction_session_timeout')")
        .fetch_one(&mut *connection)
        .await
        .unwrap();
    assert_eq!(configured, ("0".into(), "0".into()));
    let mut tx = sqlx::Transaction::begin(connection, None).await.unwrap();
    sqlx::query(
        "SELECT singleton FROM openlegal.provider_request_budget WHERE singleton FOR UPDATE",
    )
    .fetch_one(&mut *tx)
    .await
    .unwrap();
    tx
}

async fn usage(pool: &PgPool) -> (i64, i64) {
    sqlx::query_as(
        "SELECT daily_used,on_demand_used FROM openlegal.provider_request_budget WHERE singleton",
    )
    .fetch_one(pool)
    .await
    .unwrap()
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn settlement_lock_rejection_retries_only_the_owned_response() {
    let fixture = crate::test_support::TestDatabase::new().await;
    let store = fixture.open(100).await;
    let pool = store.pool();
    unrestricted(&pool).await;
    let retry_pool = retry_pool(&pool).await;
    let mut first = start(&retry_pool).await;
    let mut sibling = start(&retry_pool).await;
    let owner = first.owner();
    let slot = first.slot;
    let before = usage(&pool).await;
    let blocker = block_budget(&pool).await;
    let rejections = Rejections::new();
    let seen = rejections.clone();
    let settling = tokio::spawn(
        async move {
            let result = first.complete().await;
            (first, result)
        }
        .with_subscriber(seen),
    );
    rejections.wait(2).await;
    let still_owned: bool = sqlx::query_scalar("SELECT NOT pg_try_advisory_xact_lock($1,$2)")
        .bind(PROVIDER_LOCK.0)
        .bind(PROVIDER_LOCK.1 + slot)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(
        still_owned,
        "retry must retain the original response session"
    );
    assert_eq!(usage(&pool).await, before);
    blocker.rollback().await.unwrap();
    let (first, result) = settling.await.unwrap();
    assert_eq!(result, Ok(()));
    assert_eq!(rejections.count(), 2);
    assert!(first.connection.is_none());
    let owners: Vec<Uuid> =
        sqlx::query_scalar("SELECT owner FROM openlegal.provider_request_admission")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(owners, vec![sibling.owner()]);
    assert!(!owners.contains(&owner));
    assert_eq!(usage(&pool).await, before);
    sibling.complete().await.unwrap();
    retry_pool.close().await;
    store.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn settlement_lock_retry_exhaustion_is_fatal_and_retains_charged_owner() {
    let fixture = crate::test_support::TestDatabase::new().await;
    let store = fixture.open(100).await;
    let pool = store.pool();
    unrestricted(&pool).await;
    let retry_pool = retry_pool(&pool).await;
    let mut guard = start(&retry_pool).await;
    let before = usage(&pool).await;
    let blocker = block_budget(&pool).await;
    let rejections = Rejections::new();
    assert_eq!(
        guard.complete().with_subscriber(rejections.clone()).await,
        Err(DatabaseError::StorageUnavailable)
    );
    assert_eq!(rejections.count(), 4);
    assert!(guard.connection.is_some());
    let owners: Vec<Uuid> =
        sqlx::query_scalar("SELECT owner FROM openlegal.provider_request_admission")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(owners, vec![guard.owner()]);
    assert_eq!(usage(&pool).await, before);
    blocker.rollback().await.unwrap();
    guard.complete().await.unwrap();
    retry_pool.close().await;
    store.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn reservation_lock_retry_charges_once_and_exhaustion_has_no_effects() {
    let fixture = crate::test_support::TestDatabase::new().await;
    let store = fixture.open(100).await;
    let pool = store.pool();
    unrestricted(&pool).await;
    let retry_pool = retry_pool(&pool).await;
    let cap = Arc::new(AtomicU32::new(2));
    let reserved = Arc::new(AtomicBool::new(false));
    let mut connection = retry_pool.acquire().await.unwrap().detach();
    let blocker = block_budget(&pool).await;
    let before = usage(&pool).await;
    let rejections = Rejections::new();
    let result = reserve_locked(
        &mut connection,
        RequestBudgetMode::Continuous,
        Some(&cap),
        None,
        Some(&reserved),
        None,
        &CancellationToken::new(),
    )
    .with_subscriber(rejections.clone())
    .await;
    assert!(matches!(result, Err(DatabaseError::StorageContended)));
    assert_eq!(rejections.count(), 4);
    assert_eq!(cap.load(Ordering::Acquire), 2);
    assert!(!reserved.load(Ordering::Acquire));
    assert_eq!(usage(&pool).await, before);
    let count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM openlegal.provider_request_admission")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(count, 0);
    connection.close().await.unwrap();

    let mut connection = retry_pool.acquire().await.unwrap().detach();
    let next_cap = cap.clone();
    let next_reserved = reserved.clone();
    let rejections = Rejections::new();
    let seen = rejections.clone();
    let reserving = tokio::spawn(
        async move {
            let decision = reserve_locked(
                &mut connection,
                RequestBudgetMode::Continuous,
                Some(&next_cap),
                None,
                Some(&next_reserved),
                None,
                &CancellationToken::new(),
            )
            .await;
            (connection, decision)
        }
        .with_subscriber(seen),
    );
    rejections.wait(2).await;
    assert_eq!(cap.load(Ordering::Acquire), 2);
    assert!(!reserved.load(Ordering::Acquire));
    blocker.rollback().await.unwrap();
    let (connection, decision) = reserving.await.unwrap();
    let Ok(Decision::Reserved(owner, slot)) = decision else {
        panic!("reservation must succeed after the known blocker releases");
    };
    assert_eq!(rejections.count(), 2);
    assert_eq!(cap.load(Ordering::Acquire), 1);
    assert!(reserved.load(Ordering::Acquire));
    assert_eq!(usage(&pool).await, (before.0 + 1, before.1));
    let mut guard = ProviderRequestGuard {
        pool: retry_pool.clone(),
        connection: Some(connection),
        owner,
        slot,
    };
    guard.complete().await.unwrap();
    retry_pool.close().await;
    store.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn reservation_lock_retry_cancellation_stops_before_admission_effects() {
    let fixture = crate::test_support::TestDatabase::new().await;
    let store = fixture.open(100).await;
    let pool = store.pool();
    unrestricted(&pool).await;
    let retry_pool = retry_pool(&pool).await;
    let mut connection = retry_pool.acquire().await.unwrap().detach();
    let blocker = block_budget(&pool).await;
    let cap = Arc::new(AtomicU32::new(2));
    let reserved = Arc::new(AtomicBool::new(false));
    let cancel = CancellationToken::new();
    let worker_cancel = cancel.clone();
    let worker_cap = cap.clone();
    let worker_reserved = reserved.clone();
    let rejections = Rejections::new();
    let seen = rejections.clone();
    let reserving = tokio::spawn(
        async move {
            let result = reserve_locked(
                &mut connection,
                RequestBudgetMode::Continuous,
                Some(&worker_cap),
                None,
                Some(&worker_reserved),
                None,
                &worker_cancel,
            )
            .await;
            (connection, result)
        }
        .with_subscriber(seen),
    );
    rejections.wait(1).await;
    cancel.cancel();
    let (connection, result) = reserving.await.unwrap();
    assert!(matches!(result, Err(DatabaseError::Cancelled)));
    assert_eq!(rejections.count(), 1);
    assert_eq!(cap.load(Ordering::Acquire), 2);
    assert!(!reserved.load(Ordering::Acquire));
    assert_eq!(usage(&pool).await, (0, 0));
    connection.close().await.unwrap();
    blocker.rollback().await.unwrap();
    retry_pool.close().await;
    store.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn settlement_rejection_after_budget_lock_is_not_replayed() {
    let fixture = crate::test_support::TestDatabase::new().await;
    let store = fixture.open(100).await;
    let pool = store.pool();
    unrestricted(&pool).await;
    let mut guard = start(&pool).await;
    let before = usage(&pool).await;
    sqlx::raw_sql("CREATE SEQUENCE public.settlement_attempt; CREATE FUNCTION public.reject_settlement() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM nextval('public.settlement_attempt'); RAISE EXCEPTION 'fixture rejection after admission lock' USING ERRCODE='55P03'; END $$; CREATE TRIGGER reject_settlement BEFORE DELETE ON openlegal.provider_request_admission FOR EACH ROW EXECUTE FUNCTION public.reject_settlement()")
        .execute(&pool).await.unwrap();
    let rejections = Rejections::new();
    assert_eq!(
        guard.complete().with_subscriber(rejections.clone()).await,
        Err(DatabaseError::StorageUnavailable)
    );
    assert_eq!(rejections.count(), 0);
    let attempts: (i64, bool) =
        sqlx::query_as("SELECT last_value,is_called FROM public.settlement_attempt")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        attempts,
        (1, true),
        "a rejected DELETE is outside the SELECT retry"
    );
    assert_eq!(usage(&pool).await, before);
    sqlx::query("DROP TRIGGER reject_settlement ON openlegal.provider_request_admission")
        .execute(&pool)
        .await
        .unwrap();
    guard.complete().await.unwrap();
    store.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn settlement_transport_loss_is_fatal_without_lock_retry() {
    let fixture = crate::test_support::TestDatabase::new().await;
    let store = fixture.open(100).await;
    let pool = store.pool();
    unrestricted(&pool).await;
    let mut guard = start(&pool).await;
    let before = usage(&pool).await;
    let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(guard.connection.as_mut().unwrap())
        .await
        .unwrap();
    let killed: bool = sqlx::query_scalar("SELECT pg_terminate_backend($1)")
        .bind(pid)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(killed);
    let rejections = Rejections::new();
    assert_eq!(
        guard.complete().with_subscriber(rejections.clone()).await,
        Err(DatabaseError::StorageUnavailable)
    );
    assert_eq!(rejections.count(), 0);
    assert_eq!(usage(&pool).await, before);
    let owners: Vec<Uuid> =
        sqlx::query_scalar("SELECT owner FROM openlegal.provider_request_admission")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(owners, vec![guard.owner()]);
    drop(guard);
    store.close().await.unwrap();
}

/// Spend three seconds in the original transaction before the known first-lock
/// rejections. Without a fresh outer transaction the five-second server timer
/// terminates this session before attempt three/four can succeed.
#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn chained_first_lock_resets_outer_transaction_and_preserves_sqlx_lifecycle() {
    let fixture = crate::test_support::TestDatabase::new().await;
    let store = fixture.open(100).await;
    let pool = store.pool();
    unrestricted(&pool).await;
    let retry_pool = retry_pool(&pool).await;
    for rejected_attempts in [2, 3] {
        let mut connection = retry_pool.acquire().await.unwrap().detach();
        let settings: (String, String, String) = sqlx::query_as("SELECT current_setting('lock_timeout'),current_setting('statement_timeout'),current_setting('transaction_timeout')")
            .fetch_one(&mut connection).await.unwrap();
        assert_eq!(settings, ("1s".into(), "2s".into(), "5s".into()));
        let session: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(&mut connection)
            .await
            .unwrap();
        sqlx::query("SELECT pg_advisory_lock($1,$2)")
            .bind(PROVIDER_LOCK.0)
            .bind(PROVIDER_LOCK.1 + 200)
            .execute(&mut connection)
            .await
            .unwrap();
        let blocker = block_budget(&pool).await;
        let rejections = Rejections::new();
        let seen = rejections.clone();
        let worker = tokio::spawn(async move {
            let mut tx = connection.begin().await.unwrap();
            let original: (i64, String) = sqlx::query_as("SELECT floor(extract(epoch from transaction_timestamp())*1000)::bigint,pg_current_xact_id()::text")
                .fetch_one(&mut *tx).await.unwrap();
            tokio::time::sleep(Duration::from_secs(3)).await;
            let row = lock_budget(&mut tx, "fresh_outer_fixture", None, false).await.unwrap();
            assert!(row.try_get::<bool, _>("singleton").unwrap());
            let current: (i64, String, i32) = sqlx::query_as("SELECT floor(extract(epoch from transaction_timestamp())*1000)::bigint,pg_current_xact_id()::text,pg_backend_pid()")
                .fetch_one(&mut *tx).await.unwrap();
            assert!(current.0 >= original.0 + 3000);
            assert_ne!(current.1, original.1);
            assert_eq!(current.2, session);
            tx.commit().await.unwrap();
            // A fresh SQLx transaction after COMMIT must be top level: nesting
            // would keep this INSERT invisible to another database connection.
            let mut next = connection.begin().await.unwrap();
            sqlx::query("INSERT INTO openlegal.provider_demand_ticket(owner,lease_until,mode) VALUES(pg_catalog.uuidv7(),1,'continuous')")
                .execute(&mut *next).await.unwrap();
            next.commit().await.unwrap();
            connection
        }.with_subscriber(seen));
        rejections.wait(rejected_attempts).await;
        let owned: bool = sqlx::query_scalar("SELECT NOT pg_try_advisory_xact_lock($1,$2)")
            .bind(PROVIDER_LOCK.0)
            .bind(PROVIDER_LOCK.1 + 200)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert!(
            owned,
            "outer rollback must retain session advisory ownership"
        );
        blocker.rollback().await.unwrap();
        let connection = worker.await.unwrap();
        assert_eq!(rejections.count(), rejected_attempts);
        let visible: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM openlegal.provider_demand_ticket WHERE lease_until=1",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(visible, rejected_attempts as i64 - 1);
        connection.close().await.unwrap();
    }
    retry_pool.close().await;
    store.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn chained_retry_cancellation_and_drop_leave_pool_connection_reusable() {
    let fixture = crate::test_support::TestDatabase::new().await;
    let store = fixture.open(100).await;
    let pool = store.pool();
    unrestricted(&pool).await;
    let retry_pool = retry_pool(&pool).await;
    let blocker = block_budget(&pool).await;
    let cancel = CancellationToken::new();
    let next_cancel = cancel.clone();
    let rejections = Rejections::new();
    let seen = rejections.clone();
    let worker_pool = retry_pool.clone();
    let worker = tokio::spawn(async move {
        let mut connection = worker_pool.acquire().await.unwrap();
        let mut tx = connection.begin().await.unwrap();
        assert!(matches!(lock_budget(&mut tx, "cancel_chain_fixture", Some(&next_cancel), true).await,
            Err(DatabaseError::Cancelled)));
        // Drop queues the rollback for the newly chained outer transaction.
        drop(tx);
        let mut next = connection.begin().await.unwrap();
        sqlx::query("INSERT INTO openlegal.provider_demand_ticket(owner,lease_until,mode) VALUES(pg_catalog.uuidv7(),2,'continuous')")
            .execute(&mut *next).await.unwrap();
        next.commit().await.unwrap();
    }.with_subscriber(seen));
    rejections.wait(1).await;
    cancel.cancel();
    worker.await.unwrap();
    blocker.rollback().await.unwrap();
    let visible: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM openlegal.provider_demand_ticket WHERE lease_until=2",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(visible, 1);
    let mut explicit = retry_pool.begin().await.unwrap();
    lock_budget(&mut explicit, "explicit_rollback_fixture", None, true)
        .await
        .unwrap();
    explicit.rollback().await.unwrap();
    assert!(
        idle(&retry_pool, RequestBudgetMode::Continuous)
            .await
            .unwrap()
    );
    retry_pool.close().await;
    store.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn pool_exhaustion_before_sql_yields_but_response_suspension_remains_fatal() {
    let fixture = crate::test_support::TestDatabase::new().await;
    let store = fixture.open(100).await;
    let pool = store.pool();
    unrestricted(&pool).await;
    let exhausted = PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(Duration::from_millis(100))
        .connect_with((*pool.connect_options()).clone())
        .await
        .unwrap();
    let held = exhausted.acquire().await.unwrap();
    assert_eq!(
        idle(&exhausted, RequestBudgetMode::Continuous).await,
        Err(DatabaseError::StorageContended)
    );
    assert!(matches!(
        ForegroundTicket::open(&exhausted).await,
        Err(DatabaseError::StorageContended)
    ));
    assert_eq!(
        suspend_owned_response(&exhausted, Uuid::nil()).await,
        Err(DatabaseError::StorageUnavailable)
    );
    assert_eq!(usage(&pool).await, (0, 0));
    drop(held);
    exhausted.close().await;
    store.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn settlement_commit_connection_loss_preserves_owner_and_never_replays_delete() {
    let fixture = crate::test_support::TestDatabase::new().await;
    let store = fixture.open(100).await;
    let pool = store.pool();
    unrestricted(&pool).await;
    let mut guard = start(&pool).await;
    let before = usage(&pool).await;
    sqlx::raw_sql("CREATE SEQUENCE public.commit_attempt; CREATE FUNCTION public.lose_settlement_commit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM nextval('public.commit_attempt'); PERFORM pg_terminate_backend(pg_backend_pid()); RETURN OLD; END $$; CREATE CONSTRAINT TRIGGER lose_settlement_commit AFTER DELETE ON openlegal.provider_request_admission DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION public.lose_settlement_commit()")
        .execute(&pool).await.unwrap();
    let rejections = Rejections::new();
    assert_eq!(
        guard.complete().with_subscriber(rejections.clone()).await,
        Err(DatabaseError::StorageUnavailable)
    );
    assert_eq!(rejections.count(), 0);
    let attempts: (i64, bool) =
        sqlx::query_as("SELECT last_value,is_called FROM public.commit_attempt")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(attempts, (1, true));
    assert_eq!(usage(&pool).await, before);
    let owners: Vec<Uuid> =
        sqlx::query_scalar("SELECT owner FROM openlegal.provider_request_admission")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(owners, vec![guard.owner()]);
    drop(guard);
    store.close().await.unwrap();
}
