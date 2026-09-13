//! Local immutable observations, deliberately distinct from provider/legal revisions.
use crate::{Provenance, Query, RetrievalData};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SnapshotReference {
    pub snapshot_id: String,
    pub captured_at: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SnapshotSummary {
    pub snapshot_id: String,
    pub sequence: u64,
    pub captured_at: u64,
    pub processor_version: String,
    pub schema_version: u32,
    pub payload_sha256: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SnapshotEnvelope {
    pub schema_version: u32,
    pub snapshot: SnapshotSummary,
    pub query: Query,
    pub data: RetrievalData,
    pub provenance: Provenance,
    pub historical: bool,
    pub synthetic: bool,
    pub clock_anomaly: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SnapshotPage {
    pub snapshots: Vec<SnapshotSummary>,
    pub next_cursor: Option<String>,
    pub synthetic: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SnapshotComparisonOrigin {
    pub source: String,
    pub record_id: String,
    pub before: SnapshotSummary,
    pub after: SnapshotSummary,
    /// Exactly title + two LF bytes + body; no normalization.
    pub projection: String,
}

pub fn valid_snapshot_id(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
