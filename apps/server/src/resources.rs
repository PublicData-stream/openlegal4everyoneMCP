//! Immutable, explicitly registered text resources. URIs never trigger I/O.

use crate::{ServerError, registry::ensure_serialized_limit};
use rmcp::model::{CacheScope, ReadResourceResult, Resource, ResourceContents};
use std::collections::BTreeMap;

pub(crate) struct StaticResource {
    pub definition: Resource,
    pub result: ReadResourceResult,
}

#[derive(Default)]
pub struct ResourceRegistry {
    pub(crate) resources: BTreeMap<String, StaticResource>,
}

impl ResourceRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register an exact static resource URI. Only UTF-8 text is supported in v1.
    /// Content and metadata are trusted application assets, never caller-selected paths.
    pub fn register(
        &mut self,
        definition: Resource,
        content: ResourceContents,
    ) -> Result<(), ServerError> {
        let uri = &definition.uri;
        let parsed = url::Url::parse(uri).map_err(|_| "invalid resource URI")?;
        if uri.len() > 512
            || parsed.scheme() != "ui"
            || parsed.host_str().is_none()
            || !parsed.username().is_empty()
            || parsed.password().is_some()
            || parsed.query().is_some()
            || parsed.fragment().is_some()
            || definition.name.is_empty()
            || definition.name.len() > 128
            || self.resources.len() >= 32
            || self.resources.contains_key(uri)
        {
            return Err("invalid, duplicate or excessive static resource registration".into());
        }
        let ResourceContents::TextResourceContents {
            uri: content_uri,
            mime_type,
            text,
            ..
        } = &content
        else {
            return Err("only static text resources are supported".into());
        };
        if content_uri != uri
            || mime_type
                .as_ref()
                .is_none_or(|mime| mime.is_empty() || mime.len() > 128)
            || mime_type != &definition.mime_type
            || text.len() > 1024 * 1024
        {
            return Err("resource content URI and MIME must match its descriptor".into());
        }
        ensure_serialized_limit(&definition, 16 * 1024)?;
        let result = ReadResourceResult::new(vec![content])
            .with_ttl_ms(0)
            .with_cache_scope(CacheScope::Public);
        ensure_serialized_limit(&result, 2 * 1024 * 1024)?;
        self.resources
            .insert(uri.clone(), StaticResource { definition, result });
        Ok(())
    }

    pub(crate) fn validate_limits(&self, max_message_bytes: usize) -> Result<(), ServerError> {
        let definitions: Vec<_> = self.resources.values().map(|r| &r.definition).collect();
        ensure_serialized_limit(&definitions, max_message_bytes / 2)?;
        for resource in self.resources.values() {
            ensure_serialized_limit(&resource.result, max_message_bytes / 2)?;
        }
        Ok(())
    }
}
