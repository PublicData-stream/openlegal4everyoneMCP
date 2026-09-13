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
        self.register_bounded(definition, content, 1024 * 1024)
    }

    pub(crate) fn register_comparison_widget(
        &mut self,
        definition: Resource,
        content: ResourceContents,
    ) -> Result<(), ServerError> {
        if definition.uri != crate::text_diff::WIDGET_URI {
            return Err("larger resource allowance is reserved for the comparison widget".into());
        }
        self.register_bounded(definition, content, 3 * 1024 * 1024)
    }

    /// Merge startup registries atomically; duplicate URIs remain an error.
    pub fn extend(&mut self, other: Self) -> Result<(), ServerError> {
        if self.resources.len() + other.resources.len() > 32
            || other
                .resources
                .keys()
                .any(|uri| self.resources.contains_key(uri))
        {
            return Err("duplicate or excessive static resource registration".into());
        }
        self.resources.extend(other.resources);
        Ok(())
    }

    fn register_bounded(
        &mut self,
        definition: Resource,
        content: ResourceContents,
        max_bytes: usize,
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
            || text.len() > max_bytes
        {
            return Err("resource content URI and MIME must match its descriptor".into());
        }
        ensure_serialized_limit(&definition, 16 * 1024)?;
        let result = ReadResourceResult::new(vec![content])
            .with_ttl_ms(0)
            .with_cache_scope(CacheScope::Public);
        ensure_serialized_limit(&result, max_bytes * 2)?;
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
