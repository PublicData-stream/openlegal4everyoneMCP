//! Loading of trusted, immutable MCP App assets; never driven by caller paths.
use crate::{ServerError, config::SourceOffer, resources::ResourceRegistry};
use rmcp::model::{MetaObject, Resource, ResourceContents};
use std::path::Path;
use tokio::io::AsyncReadExt;

#[derive(Clone, Copy)]
pub(crate) enum WidgetKind {
    Records,
    TextDiff,
}

impl WidgetKind {
    fn descriptor(self) -> (&'static str, &'static str, usize) {
        match self {
            Self::Records => (
                crate::demo::WIDGET_URI,
                "Synthetic record browser",
                1024 * 1024,
            ),
            Self::TextDiff => (
                crate::text_diff::WIDGET_URI,
                "Text comparison",
                3 * 1024 * 1024,
            ),
        }
    }
}

pub(crate) async fn load_widget(
    path: &Path,
    source: &SourceOffer,
    kind: WidgetKind,
) -> Result<ResourceRegistry, ServerError> {
    let (_, _, limit) = kind.descriptor();
    let file = tokio::fs::File::open(path).await?;
    if !file.metadata().await?.is_file() {
        return Err("widget asset must be a regular file".into());
    }
    let mut bytes = Vec::new();
    file.take(limit as u64 + 1).read_to_end(&mut bytes).await?;
    if bytes.len() > limit {
        return Err("widget asset exceeds its configured resource bound".into());
    }
    widget_resources(String::from_utf8(bytes)?, source, kind)
}

pub(crate) fn widget_resources(
    html: String,
    source: &SourceOffer,
    kind: WidgetKind,
) -> Result<ResourceRegistry, ServerError> {
    const MARKER: &str = "__OPENLEGAL_SOURCE_URL__";
    const METADATA: &str =
        "<meta name=\"openlegal-source-url\" content=\"__OPENLEGAL_SOURCE_URL__\">";
    if html.matches(MARKER).count() != 1 || html.matches(METADATA).count() != 1 {
        return Err("widget must contain exactly one source URL metadata placeholder".into());
    }
    let escaped = source
        .url()
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;");
    let html = html.replacen(MARKER, &escaped, 1);
    let (uri, title, _) = kind.descriptor();
    let mime = crate::demo::WIDGET_MIME;
    let metadata = MetaObject(serde_json::from_value(serde_json::json!({
        "ui": {"prefersBorder": true, "csp": {"connectDomains": [], "resourceDomains": [], "frameDomains": []}}
    }))?);
    let definition = Resource::new(uri, title)
        .with_mime_type(mime)
        .with_meta(metadata.clone());
    let content = ResourceContents::text(html, uri)
        .with_mime_type(mime)
        .with_meta(metadata);
    let mut resources = ResourceRegistry::new();
    match kind {
        WidgetKind::Records => resources.register(definition, content)?,
        WidgetKind::TextDiff => resources.register_comparison_widget(definition, content)?,
    }
    Ok(resources)
}
