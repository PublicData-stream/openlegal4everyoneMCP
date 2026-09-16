//! Transport-independent contracts for explicitly synthetic retrieval demonstrations.
//! These types do not establish legal identifiers, date semantics, or citations.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::fmt;

pub mod history;
pub mod legal;
pub mod search_query;
pub mod text_diff;

/// A versioned operation identity. Freshness preferences are deliberately separate.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Hash, JsonSchema)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
pub enum Query {
    Search {
        source: String,
        query: String,
        page: u32,
        page_size: u32,
    },
    Get {
        source: String,
        id: String,
    },
}

impl Query {
    pub fn source(&self) -> &str {
        match self {
            Self::Search { source, .. } | Self::Get { source, .. } => source,
        }
    }

    /// Validate before cache allocation, registration lookup or upstream work.
    pub fn validate(&self) -> Result<(), RetrievalError> {
        if !valid_identifier(self.source(), 64) {
            return Err(RetrievalError::InvalidInput);
        }
        match self {
            Self::Search {
                query,
                page,
                page_size,
                ..
            } => {
                if query.len() > 256
                    || query.chars().any(char::is_control)
                    || *page > 1000
                    || !(1..=20).contains(page_size)
                {
                    return Err(RetrievalError::InvalidInput);
                }
            }
            Self::Get { id, .. } if !valid_identifier(id, 128) => {
                return Err(RetrievalError::InvalidInput);
            }
            Self::Get { .. } => {}
        }
        Ok(())
    }
}

pub fn valid_identifier(value: &str, max: usize) -> bool {
    !value.is_empty()
        && value.len() <= max
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"_-".contains(&b))
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum FreshnessRequirement {
    #[default]
    AllowStale,
    FreshOnly,
}

/// Source-owned synthetic identity; never a real-law applicability assertion.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Record {
    pub source: String,
    pub id: String,
    pub title: String,
    pub body: String,
    pub synthetic: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SearchPage {
    pub records: Vec<Record>,
    pub page: u32,
    pub page_size: u32,
    pub total: u32,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, JsonSchema)]
#[serde(untagged)]
pub enum RetrievalData {
    Search(SearchPage),
    Get(Record),
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, JsonSchema)]
pub struct Provenance {
    pub provider: String,
    pub dataset: String,
    /// Sanitized configured reference; credentials and caller query values are excluded.
    pub source_reference: String,
    pub payload_sha256: String,
    pub processor_version: String,
    /// Unix seconds from the explicitly supplied clock, not a legal date.
    pub retrieved_at: u64,
    pub validated_at: u64,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum FreshnessState {
    Fresh,
    Stale,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, JsonSchema)]
pub struct Freshness {
    pub state: FreshnessState,
    pub age_seconds: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, JsonSchema)]
pub struct RetrievalEnvelope<T> {
    pub data: T,
    pub provenance: Provenance,
    pub freshness: Freshness,
    pub synthetic: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot: Option<history::SnapshotReference>,
}

/// Monotonic, finite progress stages; no payloads, URLs or user input are carried.
#[derive(
    Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum ProgressStage {
    Accepted,
    Refreshing,
    Validating,
    Complete,
}

/// Sanitized outcomes. Temporary failure never establishes source absence.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RetrievalError {
    StorageUnavailable,
    StorageCorrupt,
    StorageCapacity,
    SnapshotUnavailable,
    InvalidInput,
    UnknownSource,
    NotFound,
    Ambiguous,
    Busy,
    Unavailable,
    FreshnessUnavailable,
    NormalizationFailed,
    InvalidPayload,
    ResourceLimit,
    Cancelled,
    Shutdown,
    Internal,
    /// Retry-After is seconds, already bounded by the response parser.
    Throttled {
        retry_after_secs: Option<u64>,
    },
}

impl RetrievalError {
    pub fn is_transient(self) -> bool {
        matches!(self, Self::Unavailable | Self::Throttled { .. })
    }
}

impl fmt::Display for RetrievalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::StorageUnavailable => "persistent storage unavailable",
            Self::StorageCorrupt => "retained data failed integrity checks",
            Self::StorageCapacity => "persistent storage capacity exhausted",
            Self::SnapshotUnavailable => "snapshot is not retained",
            Self::InvalidInput => "invalid retrieval input",
            Self::UnknownSource => "unknown source",
            Self::NotFound => "record not found",
            Self::Ambiguous => "ambiguous result",
            Self::Busy => "retrieval capacity exhausted",
            Self::Unavailable => "upstream unavailable",
            Self::FreshnessUnavailable => "fresh data unavailable",
            Self::NormalizationFailed => "payload normalization failed",
            Self::InvalidPayload => "invalid upstream payload",
            Self::ResourceLimit => "retrieval resource limit exceeded",
            Self::Cancelled => "retrieval cancelled",
            Self::Shutdown => "retrieval service stopped",
            Self::Internal => "retrieval internal error",
            Self::Throttled { .. } => "upstream throttled",
        })
    }
}
impl std::error::Error for RetrievalError {}

pub mod legal_search;
