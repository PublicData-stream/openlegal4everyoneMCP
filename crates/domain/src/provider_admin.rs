//! Versioned provider admission diagnostics and operator recovery plans.
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ProviderModeDiagnostic {
    pub reason: String,
    pub ready: bool,
    pub recheck_at: Option<u64>,
    pub requires_operator_review: bool,
}
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ProviderAdmissionSnapshot {
    pub version: u32,
    pub observed_at: u64,
    pub recovery_hold: bool,
    pub hold_generation: Option<String>,
    pub hold_started_at: Option<u64>,
    pub operator_suspended: bool,
    pub operator_suspended_at: Option<u64>,
    pub legacy_uncertain: bool,
    pub abandoned_slots: u32,
    pub active_slots: u32,
    pub first_observed_at: Option<u64>,
    /// Actual onset is unknown for migrated uncertainty and suspension.
    pub blocked_since: Option<u64>,
    pub continuous: ProviderModeDiagnostic,
    pub on_demand: ProviderModeDiagnostic,
}
#[derive(Clone, Copy, Debug, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProviderWaitKind {
    CollectionRequest,
    DetailJob,
}
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ProviderWaitSnapshot {
    pub kind: ProviderWaitKind,
    pub id: String,
    /// Private exact state used to fence one lease adjustment, never public metadata.
    pub state: Value,
}
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ProviderAdminSnapshot {
    pub version: u32,
    pub observed_at: u64,
    /// Private database policy, fence identities and lock observations.
    pub state: Value,
    pub waiting: Vec<ProviderWaitSnapshot>,
    pub waiting_truncated: bool,
}
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ProviderQuiescenceEvidence {
    pub writers_stopped_at: u64,
    pub deployment_revision: String,
    pub stopped_writer_ids: Vec<String>,
    pub evidence_reference: String,
}
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ReviewedProviderWait {
    pub kind: ProviderWaitKind,
    pub id: String,
    pub review_reason: String,
}
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ProviderRecoveryRequest {
    pub actor: String,
    pub reason: String,
    pub quiescence: ProviderQuiescenceEvidence,
    pub resolve_legacy: bool,
    pub owners: Vec<String>,
    /// Releases admission globally. Only selected waits gain earlier leases;
    /// all other work follows its normal scheduling rules.
    #[serde(default)]
    pub resume: bool,
    pub waits: Vec<ReviewedProviderWait>,
}
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ProviderRecoveryPlan {
    pub version: u32,
    pub operation_id: String,
    pub created_at: u64,
    pub expires_at: u64,
    pub snapshot: ProviderAdminSnapshot,
    pub request: ProviderRecoveryRequest,
}
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ProviderRecoveryResult {
    pub version: u32,
    pub operation_id: String,
    pub applied_at: u64,
    pub recovery_hold: bool,
    pub hold_generation: Option<String>,
    pub resolved_legacy: bool,
    pub resolved_owners: Vec<String>,
    pub advanced_waits: Vec<ReviewedProviderWait>,
    pub unchanged_waits: Vec<ProviderWaitOutcome>,
}

/// Private identity of the exact admission fences observed by a collector.
/// Existing waits remain NULL; no public receipt exposes these identities.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ProviderBlockerFingerprint {
    pub legacy_identity: Option<String>,
    pub slot_owners: Vec<String>,
    pub hold_generation: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ProviderWaitOutcome {
    pub kind: ProviderWaitKind,
    pub id: String,
    pub reason: String,
}
