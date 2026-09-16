//! Search contracts: literal evidence and analyzed matching are different views.
use crate::legal::{Dataset, ObjectId};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Filters {
    #[serde(default)]
    pub datasets: Vec<Dataset>,
    pub authority: Option<String>,
    pub object_id: Option<String>,
    pub document_type: Option<String>,
    pub date_kind: Option<DateKind>,
    pub date_from: Option<String>,
    pub date_to: Option<String>,
}
#[derive(Clone, Copy, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DateKind {
    Publication,
    Effective,
    Judgment,
}
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SearchRequest {
    #[serde(default)]
    pub query: String,
    #[serde(default)]
    pub filters: Filters,
    #[serde(default)]
    pub include_history: bool,
    #[serde(default)]
    pub include_ocr: bool,
    #[serde(default)]
    pub sections: Vec<String>,
    #[serde(default = "default_limit")]
    pub limit: usize,
    pub cursor: Option<String>,
    #[serde(default)]
    pub literal: bool,
    #[serde(default)]
    pub ignore_case: bool,
    #[serde(default)]
    pub context_lines: u8,
}
fn default_limit() -> usize {
    20
}
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct SearchHit {
    pub match_scope: String,
    pub excerpt_section: String,
    pub includes_ocr: bool,
    pub object: ObjectId,
    pub revision_id: String,
    pub capture_id: String,
    pub title: String,
    pub section: String,
    pub line: u64,
    pub text: String,
    pub byte_start: usize,
    pub byte_end: usize,
    pub derived_ocr: bool,
}
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct SearchPage {
    pub schema_version: u32,
    pub hits: Vec<SearchHit>,
    pub next_cursor: Option<String>,
    pub generation: u64,
    pub corpus_complete: bool,
    pub scanned_bytes: u64,
    pub analyzer_version: String,
    pub index_lag: u64,
}
