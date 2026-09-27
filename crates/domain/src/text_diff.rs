//! Exact supplied-text comparison contracts. These do not assert legal equivalence.
use schemars::{JsonSchema, Schema, SchemaGenerator, json_schema};
use serde::{Deserialize, Deserializer, Serialize, de};
use std::borrow::Cow;

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
    Attachment(AttachmentReference),
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AttachmentHandle {
    #[schemars(length(min = 64, max = 64), regex(pattern = "^[0-9a-f]{64}$"))]
    pub attachment_id: String,
}

/// A bearer handle, optionally accompanied by the complete published metadata.
/// Metadata never replaces the stored attachment as the source of truth.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(untagged)]
pub enum AttachmentReference {
    Handle(AttachmentHandle),
    Summary(AttachmentSummary),
}

impl AttachmentReference {
    pub fn attachment_id(&self) -> &str {
        match self {
            Self::Handle(handle) => &handle.attachment_id,
            Self::Summary(summary) => &summary.attachment_id,
        }
    }

    pub fn declared_kind(&self) -> Option<AttachmentKind> {
        match self {
            Self::Handle(_) => None,
            Self::Summary(summary) => Some(summary.kind),
        }
    }
}

impl From<AttachmentHandle> for AttachmentReference {
    fn from(handle: AttachmentHandle) -> Self {
        Self::Handle(handle)
    }
}

// Tool inputs must have an object at the root; the branches are both strict objects.
impl JsonSchema for AttachmentReference {
    fn schema_name() -> Cow<'static, str> {
        "AttachmentReference".into()
    }

    fn json_schema(generator: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "type": "object",
            "anyOf": [
                generator.subschema_for::<AttachmentHandle>(),
                generator.subschema_for::<AttachmentSummary>()
            ]
        })
    }
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

#[derive(Clone, Copy, Debug)]
pub struct ResumeTrue;

impl<'de> Deserialize<'de> for ResumeTrue {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        if bool::deserialize(deserializer)? {
            Ok(Self)
        } else {
            Err(de::Error::custom("resume must be true"))
        }
    }
}

impl JsonSchema for ResumeTrue {
    fn schema_name() -> Cow<'static, str> {
        "ResumeTrue".into()
    }

    fn json_schema(_: &mut SchemaGenerator) -> Schema {
        json_schema!({"const": true})
    }
}

/// An upload continuation carrying the complete metadata from an earlier response.
#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AttachmentUploadResume {
    pub schema_version: u32,
    #[schemars(length(min = 64, max = 64), regex(pattern = "^[0-9a-f]{64}$"))]
    pub attachment_id: String,
    pub kind: AttachmentKind,
    pub total_bytes: usize,
    pub committed_bytes: usize,
    pub sealed: bool,
    pub expires_at: u64,
    #[schemars(length(max = 32768))]
    pub chunk: String,
    pub offset: usize,
    #[serde(rename = "final")]
    pub complete: bool,
    pub resume: ResumeTrue,
}

impl AttachmentUploadResume {
    pub fn into_request(self) -> (AttachmentUpload, AttachmentKind) {
        (
            AttachmentUpload {
                attachment_id: Some(self.attachment_id),
                kind: None,
                total_bytes: None,
                offset: self.offset,
                chunk: self.chunk,
                complete: self.complete,
            },
            self.kind,
        )
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(untagged)]
pub enum AttachmentUploadInput {
    Resume(AttachmentUploadResume),
    Existing(AttachmentUpload),
}

impl JsonSchema for AttachmentUploadInput {
    fn schema_name() -> Cow<'static, str> {
        "AttachmentUploadInput".into()
    }

    fn json_schema(generator: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "type": "object",
            "anyOf": [
                generator.subschema_for::<AttachmentUploadResume>(),
                generator.subschema_for::<AttachmentUpload>()
            ]
        })
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AttachmentSummary {
    pub schema_version: u32,
    #[schemars(length(min = 64, max = 64), regex(pattern = "^[0-9a-f]{64}$"))]
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
    #[schemars(length(min = 64, max = 64), regex(pattern = "^[0-9a-f]{64}$"))]
    pub attachment_id: String,
    #[serde(default)]
    pub offset: usize,
}

#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AttachmentReadFull {
    pub schema_version: u32,
    #[schemars(length(min = 64, max = 64), regex(pattern = "^[0-9a-f]{64}$"))]
    pub attachment_id: String,
    pub kind: AttachmentKind,
    pub total_bytes: usize,
    pub committed_bytes: usize,
    pub sealed: bool,
    pub expires_at: u64,
    #[serde(default)]
    pub offset: usize,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(untagged)]
pub enum AttachmentReadInput {
    Minimal(AttachmentRead),
    Full(AttachmentReadFull),
}

impl AttachmentReadInput {
    pub fn into_request(self) -> (AttachmentRead, Option<AttachmentKind>) {
        match self {
            Self::Minimal(request) => (request, None),
            Self::Full(full) => (
                AttachmentRead {
                    attachment_id: full.attachment_id,
                    offset: full.offset,
                },
                Some(full.kind),
            ),
        }
    }
}

impl JsonSchema for AttachmentReadInput {
    fn schema_name() -> Cow<'static, str> {
        "AttachmentReadInput".into()
    }

    fn json_schema(generator: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "type": "object",
            "anyOf": [
                generator.subschema_for::<AttachmentRead>(),
                generator.subschema_for::<AttachmentReadFull>()
            ]
        })
    }
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
    InvalidUtf8Boundary { offset: usize },
    PatchConflict,
    AttachmentKindMismatch,
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
