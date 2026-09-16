//! Exact supplied-text comparison contracts. These do not assert legal equivalence.
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

pub const MAX_TEXT_BYTES: usize = 1024 * 1024;
pub const MAX_LINE_BYTES: usize = 16 * 1024;
pub const MAX_LINES: usize = 100_000;
pub const MAX_INLINE_RANGES: usize = 65_536;
pub const MAX_PAGE_INLINE_RANGES: usize = 4_096;
pub const MAX_PATCH_INPUT_BYTES: usize = 8 * 1024 * 1024;
pub const MAX_ATTACHMENT_CHUNK_BYTES: usize = 32 * 1024;

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(untagged)]
pub enum TextSource {
    Inline(String),
    Attachment(AttachmentHandle),
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AttachmentHandle {
    #[schemars(length(min = 64, max = 64), regex(pattern = "^[0-9a-f]{64}$"))]
    pub attachment_id: String,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AttachmentKind {
    Text,
    Patch,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AttachmentUpload {
    pub attachment_id: Option<String>,
    pub kind: Option<AttachmentKind>,
    pub total_bytes: Option<usize>,
    #[serde(default)]
    pub offset: usize,
    #[schemars(length(max = 32768))]
    pub chunk: String,
    #[serde(rename = "final")]
    pub complete: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct AttachmentSummary {
    pub schema_version: u32,
    pub attachment_id: String,
    pub kind: AttachmentKind,
    pub total_bytes: usize,
    pub committed_bytes: usize,
    pub sealed: bool,
    pub expires_at: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AttachmentRead {
    pub attachment_id: String,
    #[serde(default)]
    pub offset: usize,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct AttachmentPage {
    pub schema_version: u32,
    pub attachment: AttachmentSummary,
    pub offset: usize,
    pub next_offset: usize,
    pub complete: bool,
    pub text: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DiffInput {
    pub before: TextSource,
    pub after: TextSource,
    pub before_label: Option<String>,
    pub after_label: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct DiffResult {
    pub schema_version: u32,
    pub comparison: ComparisonSummary,
    pub patch: AttachmentSummary,
    pub explanation: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ApplyPatchInput {
    pub target: TextSource,
    pub patch: TextSource,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct ApplyPatchResult {
    pub schema_version: u32,
    pub result: AttachmentSummary,
    pub info: TextInfo,
}

/// Half-open Unicode scalar offsets in the original line, including CR/LF.
pub type ScalarRange = [u32; 2];

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct InlineChange {
    /// Zero-based ordinal among all fragment data rows, excluding patch headers
    /// and missing-final-newline markers. Only added/deleted rows have entries.
    pub row_index: u32,
    pub ranges: Vec<ScalarRange>,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CompareInput {
    #[schemars(length(max = 1048576))]
    pub before: String,
    #[schemars(length(max = 1048576))]
    pub after: String,
    #[schemars(length(max = 128))]
    pub before_label: Option<String>,
    #[schemars(length(max = 128))]
    pub after_label: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct TextInfo {
    pub label: String,
    pub bytes: usize,
    pub lines: usize,
    pub crlf: usize,
    pub lf: usize,
    pub bare_cr: usize,
    pub bom: bool,
    pub final_newline: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct ComparisonSummary {
    pub schema_version: u32,
    pub comparison_id: String,
    pub expires_at: u64,
    pub before: TextInfo,
    pub after: TextInfo,
    pub additions: usize,
    pub deletions: usize,
    pub equal: bool,
    pub change_pages: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin: Option<crate::history::SnapshotComparisonOrigin>,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PageView {
    #[default]
    Changes,
    Before,
    After,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PageRequest {
    #[schemars(length(min = 64, max = 64), regex(pattern = "^[0-9a-f]{64}$"))]
    pub comparison_id: String,
    #[serde(default)]
    pub view: PageView,
    pub page: usize,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct DiffFragment {
    pub patch: String,
    pub before_start: usize,
    pub before_count: usize,
    pub after_start: usize,
    pub after_count: usize,
    pub inline_changes: Vec<InlineChange>,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct PageResponse {
    pub schema_version: u32,
    pub comparison_id: String,
    pub view: PageView,
    pub page: usize,
    pub total_pages: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    pub fragments: Vec<DiffFragment>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TextDiffError {
    InvalidInput,
    NotFound,
    Busy,
    ResourceLimit,
    Unavailable,
    Cancelled,
    Internal,
}
impl std::fmt::Display for TextDiffError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "text comparison failed: {self:?}")
    }
}
impl std::error::Error for TextDiffError {}
