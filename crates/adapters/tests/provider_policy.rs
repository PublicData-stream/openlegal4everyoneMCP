//! Mock-free database admission tests: reserve attempts without provider traffic.
#[path = "../../../test-support/postgres.rs"]
mod support;
use openlegal_adapters::law_go_kr::{LawClient, ProviderRequestLimits, RequestBudgetMode};
use openlegal_application::{
    document::{DocumentError, DocumentInput, DocumentOutput, DocumentProcessor},
    persistence::PersistentStore,
    upstream_policy::RequestLimit,
};
use openlegal_domain::legal::DatabaseError;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

struct UnusedProcessor;
impl DocumentProcessor for UnusedProcessor {
    fn process(
        &self,
        _: DocumentInput,
        _: CancellationToken,
    ) -> futures::future::BoxFuture<'static, Result<DocumentOutput, DocumentError>> {
        Box::pin(async { Err(DocumentError::SandboxUnavailable) })
    }
}
fn unlimited() -> ProviderRequestLimits {
    ProviderRequestLimits {
        continuous_daily_limit: RequestLimit::Unlimited,
        on_demand_daily_limit: RequestLimit::Unlimited,
        pilot_attempt_limit: RequestLimit::Unlimited,
        on_demand_attempt_limit: RequestLimit::Unlimited,
        interval_ms: 1,
        max_in_flight: 4,
        pilot_timeout_secs: 1800,
        on_demand_timeout_secs: 7200,
        max_job_attempts: 3,
    }
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn unlimited_keeps_bigint_accounting_pacing_and_safety_fences() {
    let fixture = support::TestDatabase::new().await;
    let base = fixture.open(100).await;
    let pool = base.pool();
    LawClient::configure_provider_request_limits(&pool, &unlimited())
        .await
        .unwrap();
    sqlx::query("UPDATE openlegal.provider_request_budget SET utc_day=floor(extract(epoch from clock_timestamp()))::bigint/86400,daily_used=2147483647,on_demand_used=1000000,pilot_used=100,next_request_at_ms=0")
        .execute(&pool).await.unwrap();
    let cancel = CancellationToken::new();
    let mut pilot =
        LawClient::reserve_provider_request_budget(&pool, &RequestBudgetMode::Pilot, &cancel)
            .await
            .unwrap();
    let first: (i64,i64,i64,i64) = sqlx::query_as("SELECT daily_used,on_demand_used,pilot_used,next_request_at_ms FROM openlegal.provider_request_budget WHERE singleton").fetch_one(&pool).await.unwrap();
    assert_eq!((first.0, first.1, first.2), (2147483648, 1000000, 101));
    // A healthy response owner leaves capacity for another mode; explicit
    // settlement, rather than clearing a singleton flag, releases its evidence.
    let mut demand =
        LawClient::reserve_provider_request_budget(&pool, &RequestBudgetMode::OnDemand, &cancel)
            .await
            .unwrap();
    pilot.complete().await.unwrap();
    demand.complete().await.unwrap();
    let second: (i64,i64,i64) = sqlx::query_as("SELECT daily_used,on_demand_used,next_request_at_ms FROM openlegal.provider_request_budget WHERE singleton").fetch_one(&pool).await.unwrap();
    assert_eq!((second.0, second.1), (2147483648, 1000001));
    assert!(second.2 > first.3);
    let limited = ProviderRequestLimits::new(1000, 1000, 1).unwrap();
    LawClient::configure_provider_request_limits(&pool, &limited)
        .await
        .unwrap();
    sqlx::query("UPDATE openlegal.provider_request_budget SET unresolved_response=false,next_request_at_ms=0").execute(&pool).await.unwrap();
    assert_eq!(
        LawClient::reserve_provider_request_budget(&pool, &RequestBudgetMode::Continuous, &cancel)
            .await,
        Err(DatabaseError::BudgetExhausted)
    );
    LawClient::configure_provider_request_limits(&pool, &unlimited())
        .await
        .unwrap();
    sqlx::query("UPDATE openlegal.provider_request_budget SET operator_suspended=true")
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(
        LawClient::reserve_provider_request_budget(&pool, &RequestBudgetMode::Continuous, &cancel)
            .await,
        Err(DatabaseError::BudgetExhausted)
    );
    sqlx::query("UPDATE openlegal.provider_request_budget SET operator_suspended=false,next_allowed_at=floor(extract(epoch from clock_timestamp()))::bigint+120").execute(&pool).await.unwrap();
    assert_eq!(
        LawClient::reserve_provider_request_budget(&pool, &RequestBudgetMode::Continuous, &cancel)
            .await,
        Err(DatabaseError::BudgetExhausted)
    );
    cancel.cancel();
    assert_eq!(
        LawClient::reserve_provider_request_budget(&pool, &RequestBudgetMode::Continuous, &cancel)
            .await,
        Err(DatabaseError::Cancelled)
    );
    sqlx::query("UPDATE openlegal.provider_request_budget SET utc_day=-1,next_allowed_at=0")
        .execute(&pool)
        .await
        .unwrap();
    let mut final_request = LawClient::reserve_provider_request_budget(
        &pool,
        &RequestBudgetMode::Continuous,
        &CancellationToken::new(),
    )
    .await
    .unwrap();
    let reset: (i64,i64,i64) = sqlx::query_as("SELECT daily_used,on_demand_used,pilot_used FROM openlegal.provider_request_budget WHERE singleton").fetch_one(&pool).await.unwrap();
    assert_eq!(reset, (1, 0, 101));
    final_request.complete().await.unwrap();
    base.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn pilot_window_is_snapshotted_and_survives_policy_changes() {
    let fixture = support::TestDatabase::new().await;
    let base = fixture.open(100).await;
    let pool = base.pool();
    let mut policy = unlimited();
    policy.pilot_timeout_secs = 60;
    LawClient::configure_provider_request_limits(&pool, &policy)
        .await
        .unwrap();
    let client = LawClient::new("fictional".into(), Arc::new(UnusedProcessor))
        .unwrap()
        .with_request_budget(pool.clone(), RequestBudgetMode::Pilot);
    assert!(client.begin_pilot().await.unwrap().as_secs() <= 60);
    policy.pilot_timeout_secs = 86400;
    LawClient::configure_provider_request_limits(&pool, &policy)
        .await
        .unwrap();
    let snapshot: i64 = sqlx::query_scalar(
        "SELECT pilot_duration_secs FROM openlegal.provider_request_budget WHERE singleton",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(snapshot, 60);
    sqlx::query("UPDATE openlegal.provider_request_budget SET pilot_started_at=floor(extract(epoch from clock_timestamp()))::bigint-61").execute(&pool).await.unwrap();
    assert!(client.begin_pilot().await.unwrap().is_zero());
    assert_eq!(
        LawClient::reserve_provider_request_budget(
            &pool,
            &RequestBudgetMode::Pilot,
            &CancellationToken::new()
        )
        .await,
        Err(DatabaseError::BudgetExhausted)
    );
    base.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn unlimited_wakes_exhausted_jobs_without_resetting_evidence_or_misreporting_eta() {
    use openlegal_adapters::{blob::FsBlobStore, corpus::PgCorpusStore};
    use openlegal_domain::legal::{Dataset, ObjectCompletionEta, ObjectId};
    let fixture = support::TestDatabase::new().await;
    let base = fixture.open(100).await;
    let pool = base.pool();
    let store = PgCorpusStore::new(
        pool.clone(),
        FsBlobStore::open(&fixture.directory.path().join("wake"))
            .await
            .unwrap(),
    );
    let now: i64 =
        sqlx::query_scalar("SELECT floor(extract(epoch from clock_timestamp()))::bigint")
            .fetch_one(&pool)
            .await
            .unwrap();
    let object = ObjectId {
        jurisdiction: "kr".into(),
        provider: "fictional_test".into(),
        dataset: Dataset::NationalStatute,
        id: "policy-wake".into(),
    };
    store
        .enqueue_job(object.clone(), "r1".into(), None, true, true, now as u64)
        .await
        .unwrap();
    let claimed = store.claim_job(now as u64).await.unwrap().unwrap();
    store
        .defer_budget_claim(&claimed, (now + 86400) as u64)
        .await
        .unwrap();
    sqlx::query("UPDATE openlegal.provider_request_budget SET utc_day=$1,daily_used=1000,on_demand_used=1000,next_allowed_at=$2")
        .bind(now/86400).bind(now+120).execute(&pool).await.unwrap();
    assert!(
        matches!(store.object_status(&object, now as u64).await.unwrap().eta,
        ObjectCompletionEta::Unknown { reason } if reason == "provider_budget_exhausted")
    );
    let mut policy = ProviderRequestLimits::new(1000, 1000, 1).unwrap();
    policy.continuous_daily_limit = RequestLimit::Unlimited;
    assert_eq!(
        LawClient::configure_provider_request_limits(&pool, &policy)
            .await
            .unwrap(),
        1
    );
    let lease: String =
        sqlx::query_scalar("SELECT lease_until::text FROM openlegal.corpus_job WHERE id=$1::uuid")
            .bind(&claimed.id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(lease.parse::<i64>().unwrap(), now + 120);
    assert_eq!(
        LawClient::configure_provider_request_limits(&pool, &policy)
            .await
            .unwrap(),
        0
    );
    let ledger: (i64,i64,Option<i32>) = sqlx::query_as("SELECT daily_used,on_demand_used,on_demand_daily_limit FROM openlegal.provider_request_budget WHERE singleton").fetch_one(&pool).await.unwrap();
    assert_eq!(ledger, (1000, 1000, Some(1000)));
    assert!(
        matches!(store.object_status(&object, now as u64).await.unwrap().eta,
        ObjectCompletionEta::Unknown { reason } if reason == "insufficient_recent_samples")
    );
    let client = LawClient::new("fictional".into(), Arc::new(UnusedProcessor))
        .unwrap()
        .with_request_budget(pool.clone(), RequestBudgetMode::Continuous);
    assert_eq!(
        client.next_admissible_epoch().await.unwrap(),
        (now + 120) as u64
    );
    base.close().await.unwrap();
}
