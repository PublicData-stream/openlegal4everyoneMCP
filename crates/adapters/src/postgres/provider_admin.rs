//! Operator-only SQL administration. Opens no provider, blobs or serving services.
use super::{PostgresOptions, pool, startup_version, verify_schema};
use openlegal_domain::{legal::DatabaseError, provider_admin::*};
use serde_json::Value;
use sqlx::PgPool;
use std::time::Duration;
use uuid::Uuid;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProviderAdminError {
    InvalidInput,
    StalePlan,
    ActiveOwners,
    PermissionDenied,
    NotFound,
    StorageUnavailable,
    StorageCorrupt,
    CommitUncertain,
}
impl std::fmt::Display for ProviderAdminError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::InvalidInput => "invalid or expired provider recovery plan",
            Self::StalePlan => "provider recovery snapshot changed; inspect and plan again",
            Self::ActiveOwners => "provider ownership or database contention prevents recovery",
            Self::PermissionDenied => "provider administration requires dedicated database grants",
            Self::NotFound => "provider recovery operation was not found",
            Self::StorageUnavailable => "provider administration database unavailable",
            Self::StorageCorrupt => "provider administration database contract is incompatible",
            Self::CommitUncertain => "recovery commit acknowledgement is uncertain; read back the operation ID before taking another action",
        })
    }
}
impl std::error::Error for ProviderAdminError {}
fn db(error: sqlx::Error) -> ProviderAdminError {
    match error.as_database_error().and_then(|e| e.code()).as_deref() {
        Some("22023" | "22P02" | "22003") => ProviderAdminError::InvalidInput,
        Some("40001") => ProviderAdminError::StalePlan,
        Some("55P03" | "57014" | "40P01") => ProviderAdminError::ActiveOwners,
        Some("42501") => ProviderAdminError::PermissionDenied,
        _ => ProviderAdminError::StorageUnavailable,
    }
}
fn text_valid(s: &str, max: usize) -> bool {
    !s.is_empty() && s.len() <= max && s.trim() == s && !s.chars().any(char::is_control)
}
fn id_valid(s: &str) -> bool {
    Uuid::parse_str(s).is_ok_and(|id| id.to_string() == s)
}

#[derive(Clone)]
pub struct ProviderAdminStore {
    pool: PgPool,
}
impl ProviderAdminStore {
    pub async fn open(url: &str, options: PostgresOptions) -> Result<Self, ProviderAdminError> {
        let pool = pool(url, options, Duration::from_secs(1))
            .await
            .map_err(|_| ProviderAdminError::StorageUnavailable)?;
        if startup_version(&pool).await.is_err() || verify_schema(&pool).await.is_err() {
            pool.close().await;
            return Err(ProviderAdminError::StorageCorrupt);
        }
        Ok(Self { pool })
    }
    pub async fn inspect(&self) -> Result<ProviderAdminSnapshot, ProviderAdminError> {
        let value: Value = sqlx::query_scalar("SELECT openlegal_admin.provider_inspect()")
            .fetch_one(&self.pool)
            .await
            .map_err(db)?;
        serde_json::from_value(value).map_err(|_| ProviderAdminError::StorageCorrupt)
    }
    /// Private exact snapshots for explicitly reviewed IDs, independent of the
    /// bounded first page returned by inspect. An empty list creates a small
    /// cleanup/hold-only planning snapshot.
    pub async fn inspect_selected(
        &self,
        waits: &[ReviewedProviderWait],
    ) -> Result<ProviderAdminSnapshot, ProviderAdminError> {
        if waits.len() > 256 {
            return Err(ProviderAdminError::InvalidInput);
        }
        let selected: Vec<Value> = waits
            .iter()
            .map(|w| serde_json::json!({"kind":w.kind,"id":w.id}))
            .collect();
        let value: Value =
            sqlx::query_scalar("SELECT openlegal_admin.provider_inspect_selected($1)")
                .bind(serde_json::to_value(selected).map_err(|_| ProviderAdminError::InvalidInput)?)
                .fetch_one(&self.pool)
                .await
                .map_err(db)?;
        serde_json::from_value(value).map_err(|_| ProviderAdminError::StorageCorrupt)
    }
    pub fn plan(
        &self,
        snapshot: ProviderAdminSnapshot,
        request: ProviderRecoveryRequest,
    ) -> Result<ProviderRecoveryPlan, ProviderAdminError> {
        let mut bytes = [0; 16];
        getrandom::fill(&mut bytes).map_err(|_| ProviderAdminError::StorageUnavailable)?;
        bytes[6] = (bytes[6] & 15) | 64;
        bytes[8] = (bytes[8] & 63) | 128;
        let plan = ProviderRecoveryPlan {
            version: 1,
            operation_id: Uuid::from_bytes(bytes).to_string(),
            created_at: snapshot.observed_at,
            expires_at: snapshot
                .observed_at
                .checked_add(900)
                .ok_or(ProviderAdminError::InvalidInput)?,
            snapshot,
            request,
        };
        validate_plan(&plan)?;
        Ok(plan)
    }
    pub async fn apply(
        &self,
        plan: &ProviderRecoveryPlan,
    ) -> Result<ProviderRecoveryResult, ProviderAdminError> {
        validate_plan(plan)?;
        let value = serde_json::to_value(plan).map_err(|_| ProviderAdminError::InvalidInput)?;
        let mut tx = self.pool.begin().await.map_err(db)?;
        let result: Value = sqlx::query_scalar("SELECT openlegal_admin.provider_apply($1)")
            .bind(value)
            .fetch_one(&mut *tx)
            .await
            .map_err(db)?;
        let result =
            serde_json::from_value(result).map_err(|_| ProviderAdminError::StorageCorrupt)?;
        // Never retry a transmitted COMMIT as a rejected admission statement.
        tx.commit()
            .await
            .map_err(|_| ProviderAdminError::CommitUncertain)?;
        Ok(result)
    }
    pub async fn readback(
        &self,
        operation_id: &str,
    ) -> Result<Option<ProviderRecoveryResult>, ProviderAdminError> {
        let id = Uuid::parse_str(operation_id).map_err(|_| ProviderAdminError::InvalidInput)?;
        let value: Option<Value> =
            sqlx::query_scalar("SELECT openlegal_admin.provider_readback($1)")
                .bind(id)
                .fetch_one(&self.pool)
                .await
                .map_err(db)?;
        value
            .map(|v| serde_json::from_value(v).map_err(|_| ProviderAdminError::StorageCorrupt))
            .transpose()
    }
    pub async fn close(&self) {
        self.pool.close().await;
    }
}

pub fn validate_plan(plan: &ProviderRecoveryPlan) -> Result<(), ProviderAdminError> {
    let request = &plan.request;
    if plan.version != 1
        || plan.snapshot.version != 1
        || !id_valid(&plan.operation_id)
        || plan.created_at != plan.snapshot.observed_at
        || plan.created_at.checked_add(900) != Some(plan.expires_at)
        || request.quiescence.writers_stopped_at > plan.created_at
        || !text_valid(&request.actor, 200)
        || !text_valid(&request.reason, 1000)
        || !text_valid(&request.quiescence.deployment_revision, 200)
        || !text_valid(&request.quiescence.evidence_reference, 1000)
        || !(1..=64).contains(&request.quiescence.stopped_writer_ids.len())
        || request
            .quiescence
            .stopped_writer_ids
            .iter()
            .any(|id| !text_valid(id, 200))
        || request.owners.len() > 16
        || request.waits.len() > 256
        || (!request.resume && !request.waits.is_empty())
        || request.owners.iter().any(|id| !id_valid(id))
        || request
            .owners
            .iter()
            .enumerate()
            .any(|(i, id)| request.owners[..i].contains(id))
        || request
            .waits
            .iter()
            .any(|w| !id_valid(&w.id) || !text_valid(&w.review_reason, 1000))
        || request.waits.iter().enumerate().any(|(i, w)| {
            request.waits[..i]
                .iter()
                .any(|old| old.id == w.id && old.kind == w.kind)
        })
        || request.waits.iter().any(|w| {
            !plan
                .snapshot
                .waiting
                .iter()
                .any(|old| old.kind == w.kind && old.id == w.id)
        })
        || serde_json::to_vec(plan)
            .map_err(|_| ProviderAdminError::InvalidInput)?
            .len()
            > 256 * 1024
    {
        return Err(ProviderAdminError::InvalidInput);
    }
    Ok(())
}

pub(crate) async fn runtime_snapshot(
    pool: &PgPool,
) -> Result<ProviderAdmissionSnapshot, DatabaseError> {
    let value: Value = sqlx::query_scalar("SELECT openlegal_admin.provider_diagnostic()")
        .fetch_one(pool)
        .await
        .map_err(|_| DatabaseError::StorageUnavailable)?;
    serde_json::from_value(value).map_err(|_| DatabaseError::StorageCorrupt)
}
pub(crate) async fn held(pool: &PgPool) -> Result<bool, DatabaseError> {
    sqlx::query_scalar("SELECT openlegal_admin.provider_held()")
        .fetch_one(pool)
        .await
        .map_err(|_| DatabaseError::StorageUnavailable)
}

#[cfg(test)]
mod commit_ack_tests;
#[cfg(test)]
mod tests;
