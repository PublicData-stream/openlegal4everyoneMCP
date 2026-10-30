//! Offline, audited object quarantine. Never opens providers, blobs or serving.
use crate::postgres::{PostgresOptions, pool, verify_schema};
use openlegal_domain::legal::ObjectId;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Postgres, Row, Transaction};
use std::time::Duration;
use uuid::Uuid;

const MAX_CANDIDATES: i64 = 100;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CurrentInventoryAdminError {
    InvalidInput,
    PermissionDenied,
    MaintenanceRequired,
    NotFound,
    StaleState,
    OperationMismatch,
    AmbiguousCurrentInventory,
    InvalidTransition,
    StorageUnavailable,
    StorageCorrupt,
    CommitUncertain,
}
impl std::fmt::Display for CurrentInventoryAdminError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::InvalidInput => "invalid current inventory administrator input",
            Self::PermissionDenied => "current inventory administration requires runtime-only database privileges",
            Self::MaintenanceRequired => "current inventory administration requires global recovery hold and zero other writers or provider owners",
            Self::NotFound => "current inventory object or operation was not found",
            Self::StaleState => "current inventory object version or capture changed",
            Self::OperationMismatch => "current inventory operation ID already identifies another manifest",
            Self::AmbiguousCurrentInventory => "active current inventory remains ambiguous; preserve the hold and evidence",
            Self::InvalidTransition => "current inventory hold does not match the requested action",
            Self::StorageUnavailable => "current inventory administration database unavailable",
            Self::StorageCorrupt => "current inventory administration database contract is incompatible",
            Self::CommitUncertain => "current inventory commit acknowledgement is uncertain; read back the operation ID before any further action",
        })
    }
}
impl std::error::Error for CurrentInventoryAdminError {}
type Error = CurrentInventoryAdminError;
fn db(error: sqlx::Error) -> Error {
    match error.as_database_error().and_then(|e| e.code()).as_deref() {
        Some("42501") => Error::PermissionDenied,
        Some("55P03" | "57014" | "40P01" | "40001") => Error::MaintenanceRequired,
        _ => Error::StorageUnavailable,
    }
}
fn text_valid(value: &str, maximum: usize) -> bool {
    !value.is_empty()
        && value.len() <= maximum
        && value.trim() == value
        && !value.chars().any(char::is_control)
}
fn hex256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
fn operation(value: &str) -> Result<Uuid, Error> {
    Uuid::parse_str(value)
        .ok()
        .filter(|id| id.to_string() == value)
        .ok_or(Error::InvalidInput)
}
fn required_capture<'de, D: serde::Deserializer<'de>>(
    input: D,
) -> Result<Option<String>, D::Error> {
    Option::<String>::deserialize(input)
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CurrentInventoryAction {
    Quarantine,
    ReleaseHold,
}
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CurrentInventoryManifest {
    pub schema_version: u32,
    pub operation_id: String,
    pub object: ObjectId,
    pub expected_version: u64,
    #[serde(deserialize_with = "required_capture")]
    pub expected_head_capture: Option<String>,
    pub action: CurrentInventoryAction,
    pub actor: String,
    pub reason: String,
    pub evidence_ref: String,
    pub evidence_sha256: String,
}
impl CurrentInventoryManifest {
    pub fn validate(&self) -> Result<(), Error> {
        operation(&self.operation_id)?;
        self.object.validate().map_err(|_| Error::InvalidInput)?;
        if self.schema_version != 1
            || self.expected_version > i64::MAX as u64
            || self
                .expected_head_capture
                .as_deref()
                .is_some_and(|s| !hex256(s))
            || !text_valid(&self.actor, 200)
            || !text_valid(&self.reason, 1000)
            || !text_valid(&self.evidence_ref, 1000)
            || !hex256(&self.evidence_sha256)
        {
            return Err(Error::InvalidInput);
        }
        Ok(())
    }
    fn canonical(&self) -> Result<(Value, Vec<u8>), Error> {
        self.validate()?;
        let value = serde_json::to_value(self).map_err(|_| Error::InvalidInput)?;
        let bytes = serde_json::to_vec(&value).map_err(|_| Error::InvalidInput)?;
        if bytes.len() > 256 * 1024 {
            return Err(Error::InvalidInput);
        }
        Ok((value, Sha256::digest(bytes).to_vec()))
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CurrentInventoryState {
    pub version: u64,
    pub head_capture: Option<String>,
    pub validated_at: Option<u64>,
    pub pending: bool,
    pub withdrawn: bool,
    pub current_inventory_conflict: bool,
    pub desired_head_revision: Option<String>,
}
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CurrentInventoryReceipt {
    pub schema_version: u32,
    pub operation_id: String,
    pub object: ObjectId,
    pub action: CurrentInventoryAction,
    pub manifest_sha256: String,
    pub applied_at: u64,
    pub before: CurrentInventoryState,
    pub after: CurrentInventoryState,
}
#[derive(Clone, Debug, Serialize)]
pub struct CurrentInventoryInspection {
    pub schema_version: u32,
    pub object: ObjectId,
    pub state: CurrentInventoryState,
    pub candidates: Vec<Value>,
    pub candidates_truncated: bool,
    pub active_inventory_ambiguous: bool,
    pub recovery_hold: bool,
}

#[derive(Clone)]
pub struct CurrentInventoryAdminStore {
    pool: PgPool,
}
impl CurrentInventoryAdminStore {
    pub async fn open(url: &str, mut options: PostgresOptions) -> Result<Self, Error> {
        options.max_connections = 1;
        let pool = pool(url, options, Duration::from_secs(15))
            .await
            .map_err(|_| Error::StorageUnavailable)?;
        let checked = async {
            verify_schema(&pool).await.map_err(|_| Error::StorageCorrupt)?;
            // MEMBER includes NOINHERIT memberships: SET ROLE must not bypass
            // this runtime-only boundary. Check ownership/privileges, not only
            // the deployment's conventional migration/operator role names.
            let allowed: bool = sqlx::query_scalar(
                r#"WITH reachable AS (
                    SELECT r.* FROM pg_roles r
                    WHERE r.rolname=current_user OR pg_has_role(current_user,r.oid,'MEMBER')
                ), app_schemas AS (
                    SELECT oid,nspowner FROM pg_namespace
                    WHERE nspname IN ('openlegal','openlegal_admin')
                ), owners AS (
                    SELECT nspowner AS oid FROM app_schemas
                    UNION SELECT d.datdba FROM pg_database d
                        WHERE d.datname=current_database()
                    UNION SELECT c.relowner FROM pg_class c
                        WHERE c.relnamespace IN (SELECT oid FROM app_schemas)
                           OR (c.relnamespace='public'::regnamespace AND c.relname='_sqlx_migrations')
                    UNION SELECT p.proowner FROM pg_proc p
                        WHERE p.pronamespace IN (SELECT oid FROM app_schemas)
                )
                SELECT NOT EXISTS (
                    SELECT 1 FROM reachable r
                    WHERE r.rolsuper OR r.rolcreaterole OR r.rolcreatedb
                       OR r.rolreplication OR r.rolbypassrls
                       OR r.rolname IN ('openlegal_migration','openlegal_provider_operator','openlegal_provider_function_owner',
                                        'pg_read_server_files','pg_write_server_files','pg_execute_server_program')
                       OR r.oid IN (SELECT oid FROM owners)
                       OR EXISTS (SELECT 1 FROM app_schemas s
                                  WHERE has_schema_privilege(r.oid,s.oid,'CREATE'))
                )"#,
            )
            .fetch_one(&pool).await.map_err(db)?;
            if !allowed { return Err(Error::PermissionDenied); }
            Ok(())
        }.await;
        if let Err(error) = checked {
            pool.close().await;
            return Err(error);
        }
        Ok(Self { pool })
    }
    pub async fn close(&self) {
        self.pool.close().await;
    }

    pub async fn inspect(&self, object: &ObjectId) -> Result<CurrentInventoryInspection, Error> {
        let key = super::key(object).map_err(|_| Error::InvalidInput)?;
        let mut tx = self.pool.begin().await.map_err(db)?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        let state = object_state(&mut tx, &key, object, false).await?;
        let candidates: Vec<Value> = sqlx::query_scalar("SELECT jsonb_build_object('view_key',c.view_key,'cycle',c.cycle,'revision_id',c.revision_id,'effective_date',c.effective_date,'page',c.page,'observed_at',c.observed_at,'source_observation_id',c.source_observation_id,'active',v.cycle=c.cycle) FROM openlegal.current_inventory_candidate c JOIN openlegal.provider_clone_view v USING(view_key) WHERE c.object_key=$1 ORDER BY c.observed_at DESC,c.view_key,c.cycle,c.revision_id,c.effective_date NULLS LAST LIMIT $2")
            .bind(&key).bind(MAX_CANDIDATES + 1).fetch_all(&mut *tx).await.map_err(db)?;
        let ambiguous = active_ambiguity(&mut tx, &key).await?;
        let held: bool = sqlx::query_scalar("SELECT openlegal_admin.provider_held()")
            .fetch_one(&mut *tx)
            .await
            .map_err(db)?;
        tx.rollback().await.map_err(db)?;
        Ok(CurrentInventoryInspection {
            schema_version: 1,
            object: object.clone(),
            state,
            candidates_truncated: candidates.len() > MAX_CANDIDATES as usize,
            candidates: candidates
                .into_iter()
                .take(MAX_CANDIDATES as usize)
                .collect(),
            active_inventory_ambiguous: ambiguous,
            recovery_hold: held,
        })
    }

    pub async fn readback(&self, id: &str) -> Result<Option<CurrentInventoryReceipt>, Error> {
        let id = operation(id)?;
        let row = sqlx::query("SELECT manifest,manifest_sha256,applied_at::text,before_state,after_state FROM openlegal.current_inventory_audit WHERE operation_id=$1")
            .bind(id).fetch_optional(&self.pool).await.map_err(db)?;
        row.map(|row| receipt(id, &row)).transpose()
    }

    pub async fn apply(
        &self,
        manifest: &CurrentInventoryManifest,
    ) -> Result<CurrentInventoryReceipt, Error> {
        let (value, digest) = manifest.canonical()?;
        let id = operation(&manifest.operation_id)?;
        let key = super::key(&manifest.object).map_err(|_| Error::InvalidInput)?;
        // No retry owner surrounds this transaction or its COMMIT.
        let mut tx = self.pool.begin().await.map_err(db)?;
        sqlx::query("SELECT singleton FROM openlegal.corpus_control WHERE singleton FOR UPDATE")
            .fetch_one(&mut *tx)
            .await
            .map_err(db)?;
        if let Some(row) = sqlx::query("SELECT manifest,manifest_sha256,applied_at::text,before_state,after_state FROM openlegal.current_inventory_audit WHERE operation_id=$1")
            .bind(id).fetch_optional(&mut *tx).await.map_err(db)? {
            if row.try_get::<Value,_>("manifest").map_err(db)? != value || row.try_get::<Vec<u8>,_>("manifest_sha256").map_err(db)? != digest { return Err(Error::OperationMismatch); }
            let result = receipt(id, &row)?;
            tx.rollback().await.map_err(db)?;
            return Ok(result);
        }
        sqlx::query("SELECT view_key FROM openlegal.provider_clone_view WHERE dataset=$1 AND NOT historical ORDER BY view_key FOR UPDATE")
            .bind(manifest.object.dataset.as_str()).fetch_all(&mut *tx).await.map_err(db)?;
        let before = object_state(&mut tx, &key, &manifest.object, true).await?;
        validate_transition(manifest, &before)?;
        let budget = maintenance_guard(&mut tx).await?;
        if manifest.action == CurrentInventoryAction::ReleaseHold
            && active_ambiguity(&mut tx, &key).await?
        {
            return Err(Error::AmbiguousCurrentInventory);
        }
        let now: i64 =
            sqlx::query_scalar("SELECT floor(extract(epoch from clock_timestamp()))::bigint")
                .fetch_one(&mut *tx)
                .await
                .map_err(db)?;
        match manifest.action {
            CurrentInventoryAction::Quarantine => {
                super::current_inventory::quarantine_current_inventory_object(
                    &mut tx,
                    &key,
                    now.try_into().map_err(|_| Error::StorageCorrupt)?,
                )
                .await
                .map_err(|_| Error::StorageUnavailable)?
            }
            CurrentInventoryAction::ReleaseHold => {
                sqlx::query("UPDATE openlegal.corpus_object SET current_inventory_conflict=false,pending=true,desired_head_revision=NULL,version=version+1 WHERE object_key=$1")
                .bind(&key).execute(&mut *tx).await.map_err(db)?;
            }
        }
        let after = object_state(&mut tx, &key, &manifest.object, false).await?;
        validate_after(&before, &after, manifest.action)?;
        if maintenance_guard(&mut tx).await? != budget {
            return Err(Error::MaintenanceRequired);
        }
        let before_json = serde_json::to_value(&before).map_err(|_| Error::StorageCorrupt)?;
        let after_json = serde_json::to_value(&after).map_err(|_| Error::StorageCorrupt)?;
        sqlx::query("INSERT INTO openlegal.current_inventory_audit(operation_id,manifest_sha256,manifest,applied_at,before_state,after_state) VALUES($1,$2,$3,$4::text::numeric,$5,$6)")
            .bind(id).bind(&digest).bind(value).bind(now.to_string()).bind(before_json).bind(after_json).execute(&mut *tx).await.map_err(db)?;
        let result = CurrentInventoryReceipt {
            schema_version: 1,
            operation_id: id.to_string(),
            object: manifest.object.clone(),
            action: manifest.action,
            manifest_sha256: super::hex(&digest),
            applied_at: now.try_into().map_err(|_| Error::StorageCorrupt)?,
            before,
            after,
        };
        tx.commit().await.map_err(|_| Error::CommitUncertain)?;
        Ok(result)
    }
}

fn validate_transition(
    manifest: &CurrentInventoryManifest,
    state: &CurrentInventoryState,
) -> Result<(), Error> {
    if manifest.expected_version != state.version
        || manifest.expected_head_capture != state.head_capture
    {
        return Err(Error::StaleState);
    }
    if (manifest.action == CurrentInventoryAction::Quarantine) == state.current_inventory_conflict {
        return Err(Error::InvalidTransition);
    }
    if state.version == i64::MAX as u64 {
        return Err(Error::InvalidInput);
    }
    Ok(())
}
fn validate_after(
    before: &CurrentInventoryState,
    after: &CurrentInventoryState,
    action: CurrentInventoryAction,
) -> Result<(), Error> {
    if before.version.checked_add(1) != Some(after.version)
        || after.head_capture != before.head_capture
        || after.validated_at != before.validated_at
        || after.withdrawn != before.withdrawn
        || !after.pending
        || after.desired_head_revision.is_some()
        || after.current_inventory_conflict != (action == CurrentInventoryAction::Quarantine)
    {
        return Err(Error::StorageCorrupt);
    }
    Ok(())
}
async fn object_state(
    tx: &mut Transaction<'_, Postgres>,
    key: &str,
    object: &ObjectId,
    locked: bool,
) -> Result<CurrentInventoryState, Error> {
    let query = if locked {
        "SELECT identity,version,head_capture,validated_at::text,pending,withdrawn,current_inventory_conflict,desired_head_revision FROM openlegal.corpus_object WHERE object_key=$1 FOR UPDATE"
    } else {
        "SELECT identity,version,head_capture,validated_at::text,pending,withdrawn,current_inventory_conflict,desired_head_revision FROM openlegal.corpus_object WHERE object_key=$1"
    };
    let row = sqlx::query(query)
        .bind(key)
        .fetch_optional(&mut **tx)
        .await
        .map_err(db)?
        .ok_or(Error::NotFound)?;
    if serde_json::from_value::<ObjectId>(row.try_get("identity").map_err(db)?)
        .map_err(|_| Error::StorageCorrupt)?
        != *object
    {
        return Err(Error::StorageCorrupt);
    }
    Ok(CurrentInventoryState {
        version: row
            .try_get::<i64, _>("version")
            .map_err(db)?
            .try_into()
            .map_err(|_| Error::StorageCorrupt)?,
        head_capture: row.try_get("head_capture").map_err(db)?,
        validated_at: row
            .try_get::<Option<String>, _>("validated_at")
            .map_err(db)?
            .map(|s| s.parse().map_err(|_| Error::StorageCorrupt))
            .transpose()?,
        pending: row.try_get("pending").map_err(db)?,
        withdrawn: row.try_get("withdrawn").map_err(db)?,
        current_inventory_conflict: row.try_get("current_inventory_conflict").map_err(db)?,
        desired_head_revision: row.try_get("desired_head_revision").map_err(db)?,
    })
}
async fn active_ambiguity(tx: &mut Transaction<'_, Postgres>, key: &str) -> Result<bool, Error> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM openlegal.current_inventory_candidate c JOIN openlegal.provider_clone_view v ON v.view_key=c.view_key AND v.cycle=c.cycle WHERE c.object_key=$1 AND NOT v.historical GROUP BY c.view_key,c.cycle HAVING count(DISTINCT (c.revision_id,c.effective_date))>1)")
        .bind(key).fetch_one(&mut **tx).await.map_err(db)
}
fn validate_maintenance(
    hold: bool,
    unresolved: bool,
    owner_clear: bool,
    admissions: i64,
    others: i64,
) -> Result<(), Error> {
    if !hold || unresolved || !owner_clear || admissions != 0 || others != 0 {
        return Err(Error::MaintenanceRequired);
    }
    Ok(())
}
async fn maintenance_guard(tx: &mut Transaction<'_, Postgres>) -> Result<Value, Error> {
    // PostgreSQL can cache activity information until transaction end. Refresh
    // before every read so the final guard sees sessions opened after the first.
    sqlx::query("SELECT pg_stat_clear_snapshot()")
        .execute(&mut **tx)
        .await
        .map_err(db)?;
    let row=sqlx::query("SELECT openlegal_admin.provider_held() AS held,unresolved_response,admission_owner IS NULL AS owner_clear,to_jsonb(b) AS budget,(SELECT count(*) FROM openlegal.provider_request_admission) AS admissions,(SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid()) AS other_sessions FROM openlegal.provider_request_budget b WHERE singleton FOR SHARE")
        .fetch_one(&mut **tx).await.map_err(db)?;
    validate_maintenance(
        row.try_get("held").map_err(db)?,
        row.try_get("unresolved_response").map_err(db)?,
        row.try_get("owner_clear").map_err(db)?,
        row.try_get("admissions").map_err(db)?,
        row.try_get("other_sessions").map_err(db)?,
    )?;
    for slot in 0_i32..=16 {
        let locked: bool =
            sqlx::query_scalar("SELECT pg_try_advisory_xact_lock(1869376611,1818326864+$1)")
                .bind(slot)
                .fetch_one(&mut **tx)
                .await
                .map_err(db)?;
        if !locked {
            return Err(Error::MaintenanceRequired);
        }
    }
    row.try_get("budget").map_err(db)
}
fn receipt(id: Uuid, row: &sqlx::postgres::PgRow) -> Result<CurrentInventoryReceipt, Error> {
    let manifest: CurrentInventoryManifest =
        serde_json::from_value(row.try_get("manifest").map_err(db)?)
            .map_err(|_| Error::StorageCorrupt)?;
    let (_, expected) = manifest.canonical().map_err(|_| Error::StorageCorrupt)?;
    let digest: Vec<u8> = row.try_get("manifest_sha256").map_err(db)?;
    if digest != expected || manifest.operation_id != id.to_string() {
        return Err(Error::StorageCorrupt);
    }
    let before: CurrentInventoryState =
        serde_json::from_value(row.try_get("before_state").map_err(db)?)
            .map_err(|_| Error::StorageCorrupt)?;
    let after: CurrentInventoryState =
        serde_json::from_value(row.try_get("after_state").map_err(db)?)
            .map_err(|_| Error::StorageCorrupt)?;
    validate_transition(&manifest, &before).map_err(|_| Error::StorageCorrupt)?;
    validate_after(&before, &after, manifest.action)?;
    Ok(CurrentInventoryReceipt {
        schema_version: 1,
        operation_id: id.to_string(),
        object: manifest.object,
        action: manifest.action,
        manifest_sha256: super::hex(&digest),
        applied_at: row
            .try_get::<String, _>("applied_at")
            .map_err(db)?
            .parse()
            .map_err(|_| Error::StorageCorrupt)?,
        before,
        after,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use openlegal_domain::legal::Dataset;
    use serde_json::json;

    fn manifest() -> CurrentInventoryManifest {
        CurrentInventoryManifest {
            schema_version: 1,
            operation_id: "a46663dc-e20a-4aa4-b0ef-e09570c58c80".into(),
            object: ObjectId {
                jurisdiction: "kr".into(),
                provider: "fictional_test".into(),
                dataset: Dataset::SchoolRule,
                id: "fixture-school".into(),
            },
            expected_version: 7,
            expected_head_capture: Some("a".repeat(64)),
            action: CurrentInventoryAction::Quarantine,
            actor: "fixture operator".into(),
            reason: "conflicting current inventory identity".into(),
            evidence_ref: "fixture:bounded-evidence".into(),
            evidence_sha256: "b".repeat(64),
        }
    }
    fn state() -> CurrentInventoryState {
        CurrentInventoryState {
            version: 7,
            head_capture: Some("a".repeat(64)),
            validated_at: Some(123),
            pending: true,
            withdrawn: false,
            current_inventory_conflict: false,
            desired_head_revision: Some("r-new".into()),
        }
    }
    #[test]
    fn manifest_is_exact_bounded_and_capture_expectation_is_required() {
        let value = serde_json::to_value(manifest()).unwrap();
        let decoded: CurrentInventoryManifest = serde_json::from_value(value.clone()).unwrap();
        decoded.validate().unwrap();
        let mut missing = value.clone();
        missing
            .as_object_mut()
            .unwrap()
            .remove("expected_head_capture");
        assert!(serde_json::from_value::<CurrentInventoryManifest>(missing).is_err());
        let mut no_capture = value.clone();
        no_capture["expected_head_capture"] = Value::Null;
        serde_json::from_value::<CurrentInventoryManifest>(no_capture)
            .unwrap()
            .validate()
            .unwrap();
        for field in ["selected_revision", "force", "provider_url"] {
            let mut unknown = value.clone();
            unknown[field] = json!("private-value");
            assert!(serde_json::from_value::<CurrentInventoryManifest>(unknown).is_err());
        }
        for (field, bad) in [
            ("schema_version", json!(2)),
            ("expected_version", json!(u64::MAX)),
            (
                "operation_id",
                json!("A46663DC-E20A-4AA4-B0EF-E09570C58C80"),
            ),
            ("evidence_sha256", json!("B".repeat(64))),
            ("expected_head_capture", json!("a".repeat(63))),
            ("actor", json!(" leading")),
            ("reason", json!("unsafe\nvalue")),
            ("evidence_ref", json!("x".repeat(1001))),
        ] {
            let mut invalid = value.clone();
            invalid[field] = bad;
            assert_eq!(
                serde_json::from_value::<CurrentInventoryManifest>(invalid)
                    .unwrap()
                    .validate(),
                Err(Error::InvalidInput),
                "{field}"
            );
        }
        let (canonical, digest) = decoded.canonical().unwrap();
        let mut reordered = serde_json::Map::new();
        for (key, value) in canonical.as_object().unwrap().iter().rev() {
            reordered.insert(key.clone(), value.clone());
        }
        let reordered: CurrentInventoryManifest =
            serde_json::from_value(Value::Object(reordered)).unwrap();
        assert_eq!(reordered.canonical().unwrap().1, digest);
        let mut changed = decoded;
        changed.reason.push('!');
        assert_ne!(changed.canonical().unwrap().1, digest);
    }
    #[test]
    fn transition_requires_exact_capture_version_and_hold_state() {
        let mut input = manifest();
        let mut before = state();
        validate_transition(&input, &before).unwrap();
        input.expected_version += 1;
        assert_eq!(validate_transition(&input, &before), Err(Error::StaleState));
        input.expected_version -= 1;
        input.expected_head_capture = None;
        assert_eq!(validate_transition(&input, &before), Err(Error::StaleState));
        input.expected_head_capture = before.head_capture.clone();
        before.current_inventory_conflict = true;
        assert_eq!(
            validate_transition(&input, &before),
            Err(Error::InvalidTransition)
        );
        input.action = CurrentInventoryAction::ReleaseHold;
        validate_transition(&input, &before).unwrap();
        before.version = i64::MAX as u64;
        input.expected_version = before.version;
        assert_eq!(
            validate_transition(&input, &before),
            Err(Error::InvalidInput)
        );
    }
    #[test]
    fn release_cannot_refresh_head_or_make_it_resolvable() {
        let mut before = state();
        before.current_inventory_conflict = true;
        before.desired_head_revision = None;
        let mut after = before.clone();
        after.version += 1;
        after.current_inventory_conflict = false;
        validate_after(&before, &after, CurrentInventoryAction::ReleaseHold).unwrap();
        let correct = after.clone();
        after.pending = false;
        assert_eq!(
            validate_after(&before, &after, CurrentInventoryAction::ReleaseHold),
            Err(Error::StorageCorrupt)
        );
        after = correct.clone();
        after.validated_at = Some(999);
        assert!(validate_after(&before, &after, CurrentInventoryAction::ReleaseHold).is_err());
        after = correct.clone();
        after.head_capture = None;
        assert!(validate_after(&before, &after, CurrentInventoryAction::ReleaseHold).is_err());
        after = correct;
        after.desired_head_revision = Some("guessed-winner".into());
        assert!(validate_after(&before, &after, CurrentInventoryAction::ReleaseHold).is_err());
    }
    #[test]
    fn every_maintenance_fence_is_mandatory() {
        validate_maintenance(true, false, true, 0, 0).unwrap();
        for args in [
            (false, false, true, 0, 0),
            (true, true, true, 0, 0),
            (true, false, false, 0, 0),
            (true, false, true, 1, 0),
            (true, false, true, 0, 1),
        ] {
            assert_eq!(
                validate_maintenance(args.0, args.1, args.2, args.3, args.4),
                Err(Error::MaintenanceRequired)
            );
        }
        assert!(!Error::CommitUncertain.to_string().contains("postgres://"));
        assert!(Error::CommitUncertain.to_string().contains("read back"));
    }

    async fn admin_pool(fixture: &crate::test_support::TestDatabase) -> PgPool {
        pool(
            &fixture.url,
            crate::test_support::options(),
            Duration::from_secs(15),
        )
        .await
        .unwrap()
    }
    async fn sql(fixture: &crate::test_support::TestDatabase, statement: &'static str) {
        let pool = admin_pool(fixture).await;
        sqlx::raw_sql(statement).execute(&pool).await.unwrap();
        pool.close().await;
    }
    async fn runtime_role(setup: &PgPool, base_url: &str) -> (String, String) {
        let role = format!("inventory_fixture_{}", Uuid::new_v4().simple());
        // Ephemeral local-only credential for this disposable database.
        let password = Uuid::new_v4().simple().to_string();
        let mut connection = setup.acquire().await.unwrap();
        // Bound values and PostgreSQL %I/%L keep identifiers/literals quoted.
        sqlx::query("CREATE OR REPLACE FUNCTION pg_temp.inventory_fixture_roles(role_name text, secret text) RETURNS void LANGUAGE plpgsql AS $$ BEGIN EXECUTE format('CREATE ROLE %I LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD %L',role_name,secret); EXECUTE format('GRANT USAGE ON SCHEMA public,openlegal,openlegal_admin TO %I',role_name); EXECUTE format('GRANT SELECT ON public._sqlx_migrations TO %I',role_name); EXECUTE format('GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA openlegal TO %I',role_name); EXECUTE format('GRANT EXECUTE ON FUNCTION openlegal_admin.provider_held() TO %I',role_name); END $$")
            .execute(&mut *connection).await.unwrap();
        sqlx::query("SELECT pg_temp.inventory_fixture_roles($1,$2)")
            .bind(&role)
            .bind(&password)
            .execute(&mut *connection)
            .await
            .unwrap();
        let mut url = url::Url::parse(base_url).unwrap();
        url.set_username(&role).unwrap();
        url.set_password(Some(&password)).unwrap();
        (role, url.into())
    }
    async fn drop_role(setup: &PgPool, role: &str) {
        let mut connection = setup.acquire().await.unwrap();
        sqlx::query("CREATE OR REPLACE FUNCTION pg_temp.inventory_fixture_drop_role(role_name text) RETURNS void LANGUAGE plpgsql AS $$ BEGIN EXECUTE format('DROP OWNED BY %I',role_name); EXECUTE format('DROP ROLE %I',role_name); END $$")
            .execute(&mut *connection).await.unwrap();
        sqlx::query("SELECT pg_temp.inventory_fixture_drop_role($1)")
            .bind(role)
            .execute(&mut *connection)
            .await
            .unwrap();
    }
    async fn assert_runtime_denied(url: &str) {
        assert!(matches!(
            CurrentInventoryAdminStore::open(url, crate::test_support::options()).await,
            Err(Error::PermissionDenied)
        ));
    }

    #[tokio::test]
    #[ignore = "requires scripts/test-postgres.sh"]
    async fn runtime_boundary_rejects_semantic_authority_and_noinherit_membership() {
        let fixture = crate::test_support::TestDatabase::new().await;
        let setup = admin_pool(&fixture).await;
        let (role, url) = runtime_role(&setup, &fixture.url).await;
        let owner = format!("inventory_owner_{}", Uuid::new_v4().simple());
        let mut connection = setup.acquire().await.unwrap();
        sqlx::query(
            r#"CREATE FUNCTION pg_temp.inventory_fixture_authority(runtime_name text, owner_name text, action text)
            RETURNS void LANGUAGE plpgsql AS $$ BEGIN
                CASE action
                    WHEN 'setup' THEN
                        EXECUTE format('CREATE ROLE %I NOLOGIN',owner_name);
                        CREATE TABLE openlegal.inventory_authority_fixture(id integer);
                        CREATE FUNCTION openlegal_admin.inventory_authority_fixture() RETURNS integer LANGUAGE sql AS 'SELECT 1';
                    WHEN 'table_owner' THEN
                        EXECUTE format('ALTER TABLE openlegal.inventory_authority_fixture OWNER TO %I',runtime_name);
                    WHEN 'routine_owner' THEN
                        EXECUTE format('ALTER FUNCTION openlegal_admin.inventory_authority_fixture() OWNER TO %I',runtime_name);
                    WHEN 'ledger_owner' THEN
                        EXECUTE format('ALTER TABLE public._sqlx_migrations OWNER TO %I',runtime_name);
                    WHEN 'schema_owner' THEN
                        EXECUTE format('ALTER SCHEMA openlegal OWNER TO %I',runtime_name);
                    WHEN 'admin_schema_owner' THEN
                        EXECUTE format('ALTER SCHEMA openlegal_admin OWNER TO %I',runtime_name);
                    WHEN 'database_owner' THEN
                        EXECUTE format('ALTER DATABASE %I OWNER TO %I',current_database(),runtime_name);
                    WHEN 'server_read' THEN EXECUTE format('GRANT pg_read_server_files TO %I',runtime_name);
                    WHEN 'server_write' THEN EXECUTE format('GRANT pg_write_server_files TO %I',runtime_name);
                    WHEN 'server_program' THEN EXECUTE format('GRANT pg_execute_server_program TO %I',runtime_name);
                    WHEN 'create' THEN
                        EXECUTE format('GRANT CREATE ON SCHEMA openlegal TO %I',runtime_name);
                    WHEN 'admin_create' THEN
                        EXECUTE format('GRANT CREATE ON SCHEMA openlegal_admin TO %I',runtime_name);
                    WHEN 'reachable_owner' THEN
                        EXECUTE format('ALTER TABLE openlegal.inventory_authority_fixture OWNER TO %I',owner_name);
                        EXECUTE format('ALTER ROLE %I NOINHERIT',runtime_name);
                        EXECUTE format('GRANT %I TO %I',owner_name,runtime_name);
                    WHEN 'reachable_super' THEN EXECUTE format('ALTER ROLE %I SUPERUSER',owner_name);
                    WHEN 'reachable_createrole' THEN EXECUTE format('ALTER ROLE %I CREATEROLE',owner_name);
                    WHEN 'reachable_createdb' THEN EXECUTE format('ALTER ROLE %I CREATEDB',owner_name);
                    WHEN 'reachable_replication' THEN EXECUTE format('ALTER ROLE %I REPLICATION',owner_name);
                    WHEN 'reachable_bypassrls' THEN EXECUTE format('ALTER ROLE %I BYPASSRLS',owner_name);
                    WHEN 'reset' THEN
                        EXECUTE format('ALTER DATABASE %I OWNER TO %I',current_database(),current_user);
                        EXECUTE format('REVOKE pg_read_server_files,pg_write_server_files,pg_execute_server_program FROM %I',runtime_name);
                        ALTER TABLE openlegal.inventory_authority_fixture OWNER TO CURRENT_USER;
                        ALTER FUNCTION openlegal_admin.inventory_authority_fixture() OWNER TO CURRENT_USER;
                        ALTER TABLE public._sqlx_migrations OWNER TO CURRENT_USER;
                        ALTER SCHEMA openlegal OWNER TO CURRENT_USER;
                        ALTER SCHEMA openlegal_admin OWNER TO CURRENT_USER;
                        EXECUTE format('REVOKE CREATE ON SCHEMA openlegal,openlegal_admin FROM %I',runtime_name);
                        EXECUTE format('GRANT USAGE ON SCHEMA openlegal,openlegal_admin TO %I',runtime_name);
                        EXECUTE format('GRANT SELECT ON public._sqlx_migrations TO %I',runtime_name);
                        EXECUTE format('ALTER ROLE %I NOSUPERUSER NOCREATEROLE NOCREATEDB NOREPLICATION NOBYPASSRLS',owner_name);
                    ELSE RAISE EXCEPTION 'unknown fixture action';
                END CASE;
            END $$"#,
        ).execute(&mut *connection).await.unwrap();
        sqlx::query("SELECT pg_temp.inventory_fixture_authority($1,$2,'setup')")
            .bind(&role)
            .bind(&owner)
            .execute(&mut *connection)
            .await
            .unwrap();
        // Ordinary runtime DML/USAGE, ledger SELECT, provider_held EXECUTE and
        // the database's default TEMP privilege do not confer admin authority.
        let allowed = CurrentInventoryAdminStore::open(&url, crate::test_support::options())
            .await
            .unwrap();
        let ordinary: bool = sqlx::query_scalar("SELECT has_schema_privilege(current_user,'openlegal','USAGE') AND has_schema_privilege(current_user,'openlegal_admin','USAGE') AND has_table_privilege(current_user,'openlegal.corpus_object','SELECT,INSERT,UPDATE,DELETE') AND has_table_privilege(current_user,'public._sqlx_migrations','SELECT') AND has_function_privilege(current_user,'openlegal_admin.provider_held()','EXECUTE') AND has_database_privilege(current_user,current_database(),'TEMP')")
            .fetch_one(&allowed.pool).await.unwrap();
        assert!(ordinary);
        allowed.close().await;
        for action in [
            "database_owner",
            "server_read",
            "server_write",
            "server_program",
            "table_owner",
            "routine_owner",
            "ledger_owner",
            "schema_owner",
            "admin_schema_owner",
            "create",
            "admin_create",
            "reachable_owner",
            "reachable_super",
            "reachable_createrole",
            "reachable_createdb",
            "reachable_replication",
            "reachable_bypassrls",
        ] {
            sqlx::query("SELECT pg_temp.inventory_fixture_authority($1,$2,$3)")
                .bind(&role)
                .bind(&owner)
                .bind(action)
                .execute(&mut *connection)
                .await
                .unwrap();
            if action == "reachable_owner" {
                let (member, inherited, can_set): (bool, bool, bool) = sqlx::query_as("SELECT pg_has_role($1::name,$2::name,'MEMBER'),pg_has_role($1::name,$2::name,'USAGE'),pg_has_role($1::name,$2::name,'SET')")
                    .bind(&role).bind(&owner).fetch_one(&mut *connection).await.unwrap();
                assert!(member && can_set && !inherited);
            }
            assert_runtime_denied(&url).await;
            sqlx::query("SELECT pg_temp.inventory_fixture_authority($1,$2,'reset')")
                .bind(&role)
                .bind(&owner)
                .execute(&mut *connection)
                .await
                .unwrap();
        }
        let allowed = CurrentInventoryAdminStore::open(&url, crate::test_support::options())
            .await
            .unwrap();
        allowed.close().await;
        drop(connection);
        drop_role(&setup, &role).await;
        drop_role(&setup, &owner).await;
        setup.close().await;
    }

    #[tokio::test]
    #[ignore = "requires scripts/test-postgres.sh"]
    async fn maintenance_guard_observes_session_opened_after_first_check() {
        let fixture = crate::test_support::TestDatabase::new().await;
        let setup = admin_pool(&fixture).await;
        let (role, url) = runtime_role(&setup, &fixture.url).await;
        sqlx::query("UPDATE openlegal_admin.provider_control SET recovery_hold=true")
            .execute(&setup)
            .await
            .unwrap();
        setup.close().await;
        let store = CurrentInventoryAdminStore::open(&url, crate::test_support::options())
            .await
            .unwrap();
        let mut tx = store.pool.begin().await.unwrap();
        maintenance_guard(&mut tx).await.unwrap();
        // Keep the same transaction (and its statistics snapshot) open while
        // an independently connected session appears between the two guards.
        let late = admin_pool(&fixture).await;
        let _: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(&late)
            .await
            .unwrap();
        assert_eq!(
            maintenance_guard(&mut tx).await,
            Err(Error::MaintenanceRequired)
        );
        tx.rollback().await.unwrap();
        store.close().await;
        drop_role(&late, &role).await;
        late.close().await;
    }

    #[tokio::test]
    #[ignore = "requires scripts/test-postgres.sh"]
    async fn offline_runtime_admin_is_audited_fenced_and_preserves_history_and_budget() {
        let fixture = crate::test_support::TestDatabase::new().await;
        assert!(matches!(
            CurrentInventoryAdminStore::open(&fixture.url, crate::test_support::options()).await,
            Err(Error::PermissionDenied)
        ));
        let setup = admin_pool(&fixture).await;
        let input = manifest();
        let key = super::super::key(&input.object).unwrap();
        sqlx::query("INSERT INTO openlegal.corpus_object(object_key,identity,version,pending,desired_head_revision) VALUES($1,$2,7,true,'r-new')").bind(&key).bind(serde_json::to_value(&input.object).unwrap()).execute(&setup).await.unwrap();
        let capture = "a".repeat(64);
        sqlx::query("INSERT INTO openlegal.corpus_capture(id,object_key,revision_id,sequence,captured_at,event_sequence,payload,payload_sha256,raw_sha256,raw_size,storage_key) VALUES($1,$2,'retained-r1',1,100,1,'{}',$3,$3,7,'fixture-retained')")
            .bind(&capture).bind(&key).bind(vec![0_u8;32]).execute(&setup).await.unwrap();
        sqlx::query("UPDATE openlegal.corpus_object SET head_capture=$1,validated_at=123 WHERE object_key=$2").bind(&capture).bind(&key).execute(&setup).await.unwrap();
        sqlx::query("INSERT INTO openlegal.corpus_revision(object_key,revision_id,latest_capture,last_sequence,captured_at) VALUES($1,'retained-r1',$2,1,100)").bind(&key).bind(&capture).execute(&setup).await.unwrap();
        for (revision, head, status, attempts) in [
            ("r-new", true, "running", 2),
            ("r-next", true, "pending", 1),
            ("r-history", false, "pending", 0),
        ] {
            sqlx::query("INSERT INTO openlegal.corpus_job(object_key,revision_id,install_head,expected_version,status,attempts,created_at,lease_until) VALUES($1,$2,$3,7,$4,$5,100,999)").bind(&key).bind(revision).bind(head).bind(status).bind(attempts).execute(&setup).await.unwrap();
        }
        sqlx::query("UPDATE openlegal.provider_request_budget SET daily_used=19,on_demand_used=3,pilot_used=2").execute(&setup).await.unwrap();
        let retained:Value=sqlx::query_scalar("SELECT jsonb_build_object('capture',(SELECT jsonb_agg(to_jsonb(c)) FROM openlegal.corpus_capture c),'revision',(SELECT jsonb_agg(to_jsonb(r)) FROM openlegal.corpus_revision r),'budget',(SELECT to_jsonb(b) FROM openlegal.provider_request_budget b))").fetch_one(&setup).await.unwrap();
        let (role, url) = runtime_role(&setup, &fixture.url).await;
        let store = CurrentInventoryAdminStore::open(&url, crate::test_support::options())
            .await
            .unwrap();
        // This other session independently fences writes, even with a global hold.
        sqlx::query("UPDATE openlegal_admin.provider_control SET recovery_hold=true")
            .execute(&setup)
            .await
            .unwrap();
        assert_eq!(
            store.apply(&input).await.unwrap_err(),
            Error::MaintenanceRequired
        );
        setup.close().await;
        sql(
            &fixture,
            "UPDATE openlegal_admin.provider_control SET recovery_hold=false",
        )
        .await;
        assert_eq!(
            store.apply(&input).await.unwrap_err(),
            Error::MaintenanceRequired
        );
        sql(&fixture,"UPDATE openlegal_admin.provider_control SET recovery_hold=true; UPDATE openlegal.provider_request_budget SET unresolved_response=true").await;
        assert_eq!(
            store.apply(&input).await.unwrap_err(),
            Error::MaintenanceRequired
        );
        sql(&fixture,"UPDATE openlegal.provider_request_budget SET unresolved_response=false; INSERT INTO openlegal.provider_request_admission(owner,slot,mode,started_at_ms) VALUES('64c38a14-d5bb-4de3-9a2b-ce467e7b4ed4',1,'continuous',100)").await;
        assert_eq!(
            store.apply(&input).await.unwrap_err(),
            Error::MaintenanceRequired
        );
        sql(&fixture, "DELETE FROM openlegal.provider_request_admission").await;
        let quarantined = store.apply(&input).await.unwrap();
        assert_eq!(quarantined.after.version, 8);
        assert!(quarantined.after.current_inventory_conflict);
        assert_eq!(store.apply(&input).await.unwrap(), quarantined);
        assert_eq!(
            store.readback(&input.operation_id).await.unwrap(),
            Some(quarantined.clone())
        );
        assert!(store.readback("not-a-uuid").await.is_err());
        assert!(
            store
                .readback("a46663dc-e20a-4aa4-b0ef-e09570c58c81")
                .await
                .unwrap()
                .is_none()
        );
        let mut different = input.clone();
        different.reason.push('!');
        assert_eq!(
            store.apply(&different).await.unwrap_err(),
            Error::OperationMismatch
        );
        let mut release = input.clone();
        release.operation_id = Uuid::new_v4().to_string();
        release.action = CurrentInventoryAction::ReleaseHold;
        assert_eq!(store.apply(&release).await.unwrap_err(), Error::StaleState);
        release.expected_version = 8;
        let setup = admin_pool(&fixture).await;
        sqlx::query("INSERT INTO openlegal.provider_clone_view(view_key,dataset,historical) VALUES('fixture-current','school_rule',false)").execute(&setup).await.unwrap();
        sqlx::query("INSERT INTO openlegal.corpus_source_observation(id,source_key,raw_size,media_type,rights,metadata,observed_at,validated_at,manifest_sha256) VALUES($1,'fixture-source',0,'application/xml','{}','{}',100,100,$2)").bind("c".repeat(64)).bind(vec![0_u8;32]).execute(&setup).await.unwrap();
        for revision in ["r-current-a", "r-current-b"] {
            sqlx::query("INSERT INTO openlegal.current_inventory_candidate(view_key,cycle,object_key,revision_id,effective_date,page,observed_at,source_observation_id) VALUES('fixture-current',1,$1,$2,NULL,1,100,$3)").bind(&key).bind(revision).bind("c".repeat(64)).execute(&setup).await.unwrap();
        }
        setup.close().await;
        assert!(
            store
                .inspect(&input.object)
                .await
                .unwrap()
                .active_inventory_ambiguous
        );
        assert_eq!(
            store.apply(&release).await.unwrap_err(),
            Error::AmbiguousCurrentInventory
        );
        sql(
            &fixture,
            "UPDATE openlegal.provider_clone_view SET cycle=2 WHERE view_key='fixture-current'",
        )
        .await;
        let released = store.apply(&release).await.unwrap();
        assert_eq!(released.after.version, 9);
        assert!(!released.after.current_inventory_conflict);
        assert!(released.after.pending);
        assert!(released.after.desired_head_revision.is_none());
        assert_eq!(released.after.head_capture, Some(capture));
        assert_eq!(released.after.validated_at, Some(123));
        assert_eq!(store.apply(&release).await.unwrap(), released);
        let inspection = store.inspect(&input.object).await.unwrap();
        assert_eq!(inspection.candidates.len(), 2);
        assert!(!inspection.active_inventory_ambiguous);
        let setup = admin_pool(&fixture).await;
        let preserved:Value=sqlx::query_scalar("SELECT jsonb_build_object('capture',(SELECT jsonb_agg(to_jsonb(c)) FROM openlegal.corpus_capture c),'revision',(SELECT jsonb_agg(to_jsonb(r)) FROM openlegal.corpus_revision r),'budget',(SELECT to_jsonb(b) FROM openlegal.provider_request_budget b))").fetch_one(&setup).await.unwrap();
        assert_eq!(preserved, retained);
        let jobs:Vec<(String,String,i32,Option<String>)>=sqlx::query_as("SELECT revision_id,status,attempts,error_category FROM openlegal.corpus_job ORDER BY revision_id").fetch_all(&setup).await.unwrap();
        assert_eq!(
            jobs,
            vec![
                ("r-history".into(), "pending".into(), 0, None),
                (
                    "r-new".into(),
                    "failed".into(),
                    2,
                    Some("source_data_invalid".into())
                ),
                (
                    "r-next".into(),
                    "failed".into(),
                    1,
                    Some("source_data_invalid".into())
                )
            ]
        );
        let audits: i64 =
            sqlx::query_scalar("SELECT count(*) FROM openlegal.current_inventory_audit")
                .fetch_one(&setup)
                .await
                .unwrap();
        assert_eq!(audits, 2);
        assert!(
            sqlx::query("UPDATE openlegal.current_inventory_audit SET after_state='{}'")
                .execute(&setup)
                .await
                .is_err()
        );
        assert!(
            sqlx::query("DELETE FROM openlegal.current_inventory_audit")
                .execute(&setup)
                .await
                .is_err()
        );
        let stored: Value = sqlx::query_scalar(
            "SELECT manifest FROM openlegal.current_inventory_audit WHERE operation_id=$1",
        )
        .bind(operation(&input.operation_id).unwrap())
        .fetch_one(&setup)
        .await
        .unwrap();
        assert_eq!(stored, input.canonical().unwrap().0);
        store.close().await;
        drop_role(&setup, &role).await;
        setup.close().await;
    }
}
