//! One-document processing port. Implementations execute provider parsers in the
//! configured disposable sandbox; callers must not substitute in-process parsing.
use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

pub const MAX_DOCUMENT_BYTES: usize = 100 * 1024 * 1024;
pub const MAX_DOCUMENT_OUTPUT_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_DOCUMENT_HEADER_BYTES: usize = 4096;
pub const MAX_DOCUMENT_NODES: usize = 100_000;
pub const MAX_DOCUMENT_DEPTH: usize = 64;
pub const MAX_DOCUMENT_PAGES: usize = 500;

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DocumentFormat {
    Xml,
    Html,
    Pdf,
    Hwp5,
    Hwpx,
}

/// All identity context is supplied by trusted application code. A digest is
/// checked on both sides of the worker boundary before publication.
#[derive(Clone, Debug)]
pub struct DocumentInput {
    pub format: DocumentFormat,
    pub raw: Vec<u8>,
    pub source_sha256: String,
    pub ocr: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DocumentHeader {
    pub format: DocumentFormat,
    pub source_sha256: String,
    pub ocr: bool,
    pub bytes_len: usize,
}

/// Ordered children retain repeated fields, mixed text and source attributes.
/// Namespace-expanded element names use `{namespace}local`; no legal meaning is
/// inferred by this generic extraction boundary.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum DocumentNode {
    Element {
        name: String,
        attributes: Vec<(String, String)>,
        children: Vec<DocumentNode>,
    },
    Text {
        value: String,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct DocumentPage {
    /// One-based physical page for paginated formats.
    pub page: usize,
    pub text: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DocumentOutput {
    pub source_sha256: String,
    pub processor_version: String,
    pub format: DocumentFormat,
    pub tree: Option<DocumentNode>,
    pub text: String,
    pub pages: Vec<DocumentPage>,
    /// OCR never overwrites the provider or deterministic extraction projection.
    pub ocr_pages: Vec<DocumentPage>,
    /// Stable diagnostic codes only; parser messages may contain document data.
    pub diagnostics: Vec<String>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DocumentError {
    InvalidInput,
    InvalidDocument,
    ResourceLimit,
    UnsupportedFormat,
    ProcessingFailed,
    SandboxUnavailable,
    Cancelled,
    TimedOut,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "status", content = "value", rename_all = "snake_case")]
pub enum DocumentResponse {
    Success(Box<DocumentOutput>),
    Error(DocumentError),
}

pub trait DocumentProcessor: Send + Sync + 'static {
    fn process(
        &self,
        input: DocumentInput,
        cancellation: CancellationToken,
    ) -> BoxFuture<'static, Result<DocumentOutput, DocumentError>>;
}

/// Validate untrusted worker output independently of the parser process.
pub fn validate_output(
    output: &DocumentOutput,
    input: &DocumentInput,
) -> Result<(), DocumentError> {
    if output.source_sha256 != input.source_sha256
        || output.format != input.format
        || output.processor_version.is_empty()
        || output.processor_version.len() > 256
        || output.diagnostics.len() > 64
        || output.diagnostics.iter().any(|v| v.len() > 128)
        || (!input.ocr && !output.ocr_pages.is_empty())
    {
        return Err(DocumentError::InvalidDocument);
    }
    let tree_required = matches!(input.format, DocumentFormat::Xml | DocumentFormat::Html);
    if output.tree.is_some() != tree_required {
        return Err(DocumentError::InvalidDocument);
    }
    let mut count = 0usize;
    if let Some(tree) = &output.tree {
        let mut stack = vec![(tree, 1)];
        while let Some((node, depth)) = stack.pop() {
            count += 1;
            if count > MAX_DOCUMENT_NODES || depth > MAX_DOCUMENT_DEPTH {
                return Err(DocumentError::ResourceLimit);
            }
            if let DocumentNode::Element {
                name,
                attributes,
                children,
            } = node
            {
                if name.is_empty() || name.len() > 1024 || attributes.len() > 128 {
                    return Err(DocumentError::InvalidDocument);
                }
                stack.extend(children.iter().map(|child| (child, depth + 1)));
            }
        }
    }
    for pages in [&output.pages, &output.ocr_pages] {
        if pages.len() > MAX_DOCUMENT_PAGES
            || pages
                .iter()
                .enumerate()
                .any(|(index, page)| page.page != index + 1)
        {
            return Err(DocumentError::InvalidDocument);
        }
    }
    if serde_json::to_vec(output)
        .map_err(|_| DocumentError::InvalidDocument)?
        .len()
        > MAX_DOCUMENT_OUTPUT_BYTES - 128
    {
        return Err(DocumentError::ResourceLimit);
    }
    Ok(())
}
