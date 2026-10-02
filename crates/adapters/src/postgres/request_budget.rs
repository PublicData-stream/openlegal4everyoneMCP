//! Provider/origin-scoped daily accounting, independent of retained query rows.
use super::*;
use openlegal_application::upstream_policy::{DailyBudgetStore, RequestLimit};
use openlegal_domain::valid_identifier;

fn validate_identity(namespace: &str, provider: &str) -> Result<(), Error> {
    if !valid_identifier(namespace, 128) || !valid_identifier(provider, 64) {
        return Err(Error::InvalidInput);
    }
    Ok(())
}

impl DailyBudgetStore for PostgresStore {
    fn configure(
        &self,
        namespace: String,
        provider: String,
        limit: RequestLimit,
    ) -> BoxFuture<'static, Result<(), Error>> {
        self.own(move |store| Box::pin(async move {
            validate_identity(&namespace, &provider)?;
            limit.validate()?;
            let mut tx = DbTransaction::begin(&store.inner.pool).await?;
            sqlx::query("INSERT INTO openlegal.upstream_daily_budget(namespace,provider,daily_limit) VALUES($1,$2,$3) ON CONFLICT(namespace,provider) DO UPDATE SET daily_limit=EXCLUDED.daily_limit")
                .bind(namespace).bind(provider).bind(limit.as_option().map(i64::from))
                .execute(tx.conn()?).await.map_err(database_error)?;
            if store.inner.closing.is_cancelled() || store.status().error().is_some() {
                return Err(Error::StorageUnavailable);
            }
            tx.commit().await
        }))
    }

    fn reserve(
        &self,
        namespace: String,
        provider: String,
        cancellation: CancellationToken,
    ) -> BoxFuture<'static, Result<(), Error>> {
        self.own(move |store| Box::pin(async move {
            validate_identity(&namespace, &provider)?;
            if cancellation.is_cancelled() { return Err(Error::Cancelled); }
            let mut tx = DbTransaction::begin(&store.inner.pool).await?;
            let row = sqlx::query("SELECT utc_day,used,daily_limit,floor(extract(epoch from clock_timestamp())/86400)::bigint AS current_day FROM openlegal.upstream_daily_budget WHERE namespace=$1 AND provider=$2 FOR UPDATE")
                .bind(&namespace).bind(&provider).fetch_optional(tx.conn()?).await
                .map_err(database_error)?.ok_or(Error::StorageCorrupt)?;
            let day: i64 = row.try_get("current_day").map_err(database_error)?;
            let previous_day: i64 = row.try_get("utc_day").map_err(database_error)?;
            let used: i64 = if day == previous_day { row.try_get("used").map_err(database_error)? } else { 0 };
            let limit: Option<i64> = row.try_get("daily_limit").map_err(database_error)?;
            if used < 0 || limit.is_some_and(|value| !(1..=1_000_000).contains(&value)) {
                return Err(Error::StorageCorrupt);
            }
            if limit.is_some_and(|limit| used >= limit) { return Err(Error::Busy); }
            let charged = used.checked_add(1).ok_or(Error::StorageCapacity)?;
            sqlx::query("UPDATE openlegal.upstream_daily_budget SET utc_day=$3,used=$4 WHERE namespace=$1 AND provider=$2")
                .bind(namespace).bind(provider).bind(day).bind(charged)
                .execute(tx.conn()?).await.map_err(database_error)?;
            if cancellation.is_cancelled() { return Err(Error::Cancelled); }
            if store.inner.closing.is_cancelled() || store.status().error().is_some() {
                return Err(Error::StorageUnavailable);
            }
            // The store owns this commit even if its caller disappears. An
            // uncertain outcome never refunds a possibly committed reservation.
            tx.commit().await
        }))
    }
}
