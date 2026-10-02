//! Fictional provider admission fixtures; never contacts a legal provider.
#[path = "../../../test-support/postgres.rs"]
mod support;
use openlegal_application::{
    persistence::PersistentStore,
    upstream_policy::{DailyBudgetStore, RequestLimit},
};
use openlegal_domain::RetrievalError;
use tokio_util::sync::CancellationToken;

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn daily_accounting_is_durable_scoped_and_preserved_when_policy_changes() {
    let fixture = support::TestDatabase::new().await;
    let store = fixture.open(100).await;
    let namespace = "fictional_origin".to_string();
    let provider = "synthetic".to_string();
    store
        .configure(namespace.clone(), provider.clone(), RequestLimit::Unlimited)
        .await
        .unwrap();
    store
        .reserve(
            namespace.clone(),
            provider.clone(),
            CancellationToken::new(),
        )
        .await
        .unwrap();
    store
        .reserve(
            namespace.clone(),
            provider.clone(),
            CancellationToken::new(),
        )
        .await
        .unwrap();
    store
        .configure(
            namespace.clone(),
            provider.clone(),
            RequestLimit::Limited(2),
        )
        .await
        .unwrap();
    assert_eq!(
        store
            .reserve(
                namespace.clone(),
                provider.clone(),
                CancellationToken::new()
            )
            .await,
        Err(RetrievalError::Busy)
    );
    store
        .configure(
            "another_origin".into(),
            provider.clone(),
            RequestLimit::Limited(1),
        )
        .await
        .unwrap();
    store
        .reserve(
            "another_origin".into(),
            provider.clone(),
            CancellationToken::new(),
        )
        .await
        .unwrap();
    store
        .configure(
            namespace.clone(),
            "other_provider".into(),
            RequestLimit::Limited(1),
        )
        .await
        .unwrap();
    store
        .reserve(
            namespace.clone(),
            "other_provider".into(),
            CancellationToken::new(),
        )
        .await
        .unwrap();
    store.close().await.unwrap();
    let reopened = fixture.open(100).await;
    assert_eq!(
        reopened
            .reserve(
                namespace.clone(),
                provider.clone(),
                CancellationToken::new()
            )
            .await,
        Err(RetrievalError::Busy)
    );
    reopened
        .configure(namespace.clone(), provider.clone(), RequestLimit::Unlimited)
        .await
        .unwrap();
    reopened
        .reserve(
            namespace.clone(),
            provider.clone(),
            CancellationToken::new(),
        )
        .await
        .unwrap();
    let used: i64 = sqlx::query_scalar(
        "SELECT used FROM openlegal.upstream_daily_budget WHERE namespace=$1 AND provider=$2",
    )
    .bind(&namespace)
    .bind(&provider)
    .fetch_one(&reopened.pool())
    .await
    .unwrap();
    assert_eq!(used, 3);
    sqlx::query("UPDATE openlegal.upstream_daily_budget SET utc_day=utc_day-1 WHERE namespace=$1 AND provider=$2")
        .bind(&namespace).bind(&provider).execute(&reopened.pool()).await.unwrap();
    reopened
        .configure(
            namespace.clone(),
            provider.clone(),
            RequestLimit::Limited(1),
        )
        .await
        .unwrap();
    reopened
        .reserve(
            namespace.clone(),
            provider.clone(),
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(
        reopened
            .reserve(
                namespace.clone(),
                provider.clone(),
                CancellationToken::new()
            )
            .await,
        Err(RetrievalError::Busy)
    );
    reopened.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn concurrent_reservations_never_exceed_finite_quota_and_cancelled_calls_do_not_charge() {
    let fixture = support::TestDatabase::new().await;
    let store = fixture.open(100).await;
    store
        .configure(
            "origin".into(),
            "synthetic".into(),
            RequestLimit::Limited(1),
        )
        .await
        .unwrap();
    let cancel = CancellationToken::new();
    cancel.cancel();
    assert_eq!(
        store
            .reserve("origin".into(), "synthetic".into(), cancel)
            .await,
        Err(RetrievalError::Cancelled)
    );
    let (a, b) = tokio::join!(
        store.reserve(
            "origin".into(),
            "synthetic".into(),
            CancellationToken::new()
        ),
        store.reserve(
            "origin".into(),
            "synthetic".into(),
            CancellationToken::new()
        )
    );
    assert_eq!(usize::from(a.is_ok()) + usize::from(b.is_ok()), 1);
    assert!(a == Err(RetrievalError::Busy) || b == Err(RetrievalError::Busy));
    store.close().await.unwrap();
}
