//! Bounded in-memory storage mechanics. The application supplies retention cutoffs.
use openlegal_application::{
    CacheKey, CacheStore, MAX_CACHE_BYTES, MAX_CACHE_ENTRIES, StoredPayload,
};
use std::{collections::HashMap, sync::Arc};

struct Entry {
    value: Arc<StoredPayload>,
    touched: u64,
}

/// Process-local LRU storage; restart discards raw evidence and normalized records.
#[derive(Default)]
pub struct MemoryCache {
    entries: HashMap<CacheKey, Entry>,
    bytes: usize,
    access: u64,
}
impl MemoryCache {
    pub fn new() -> Self {
        Self::default()
    }
}

impl CacheStore for MemoryCache {
    fn get(&mut self, key: &CacheKey) -> Option<Arc<StoredPayload>> {
        self.access = self.access.wrapping_add(1);
        self.entries.get_mut(key).map(|entry| {
            entry.touched = self.access;
            entry.value.clone()
        })
    }
    fn publish(&mut self, key: CacheKey, value: Arc<StoredPayload>) {
        // The application rejects oversized candidates. Keep the storage boundary
        // independently bounded for other trusted callers of this adapter.
        if value.bytes > MAX_CACHE_BYTES {
            return;
        }
        if let Some(old) = self.entries.remove(&key) {
            self.bytes -= old.value.bytes;
        }
        while self.entries.len() >= MAX_CACHE_ENTRIES || self.bytes + value.bytes > MAX_CACHE_BYTES
        {
            let oldest = self
                .entries
                .iter()
                .min_by_key(|(_, entry)| entry.touched)
                .map(|(key, _)| key.clone());
            let Some(oldest) = oldest else { break };
            if let Some(entry) = self.entries.remove(&oldest) {
                self.bytes -= entry.value.bytes;
            }
        }
        self.access = self.access.wrapping_add(1);
        self.bytes += value.bytes;
        self.entries.insert(
            key,
            Entry {
                value,
                touched: self.access,
            },
        );
    }
    fn expire_before(&mut self, validated_before: u64) {
        self.entries.retain(|_, entry| {
            if entry.value.provenance.validated_at < validated_before {
                self.bytes -= entry.value.bytes;
                false
            } else {
                true
            }
        });
    }
    fn clear(&mut self) {
        self.entries.clear();
        self.bytes = 0;
    }
    fn stats(&self) -> (usize, usize) {
        (self.entries.len(), self.bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::future::BoxFuture;
    use openlegal_application::{FetchedPayload, Source, Upstream};
    use openlegal_domain::{Provenance, Query, Record, RetrievalData, RetrievalError};
    use tokio_util::sync::CancellationToken;

    struct Unused;
    impl Upstream for Unused {
        fn fetch(
            &self,
            _query: Query,
            _cancellation: CancellationToken,
        ) -> BoxFuture<'static, Result<FetchedPayload, RetrievalError>> {
            Box::pin(async { Err(RetrievalError::Unavailable) })
        }
    }
    fn key(id: usize) -> CacheKey {
        let source = Source {
            id: "layout_a".into(),
            provider: "synthetic".into(),
            dataset: "records".into(),
            processor_version: "v1".into(),
            upstream: Arc::new(Unused),
        };
        CacheKey::for_source(
            &source,
            Query::Get {
                source: source.id.clone(),
                id: id.to_string(),
            },
        )
        .unwrap()
    }
    fn payload(bytes: usize, at: u64) -> Arc<StoredPayload> {
        Arc::new(StoredPayload {
            snapshot: None,
            data: RetrievalData::Get(Record {
                source: "layout_a".into(),
                id: "001".into(),
                title: "Synthetic".into(),
                body: "Example".into(),
                synthetic: true,
            }),
            provenance: Provenance {
                provider: "synthetic".into(),
                dataset: "records".into(),
                source_reference: "https://example.test/synthetic".into(),
                payload_sha256: "digest".into(),
                processor_version: "v1".into(),
                retrieved_at: at,
                validated_at: at,
            },
            raw: vec![0; bytes.saturating_sub(1024)],
            bytes,
        })
    }

    #[test]
    fn entry_pressure_evicts_least_recently_used_envelope() {
        let mut cache = MemoryCache::new();
        for id in 0..MAX_CACHE_ENTRIES {
            cache.publish(key(id), payload(2048, 0));
        }
        assert!(cache.get(&key(0)).is_some());
        cache.publish(key(MAX_CACHE_ENTRIES), payload(2048, 0));
        assert!(cache.get(&key(0)).is_some());
        assert!(cache.get(&key(1)).is_none());
        assert_eq!(cache.stats(), (MAX_CACHE_ENTRIES, MAX_CACHE_ENTRIES * 2048));
    }

    #[test]
    fn raw_evidence_byte_pressure_and_application_cutoff_are_atomic() {
        let mut cache = MemoryCache::new();
        let original = payload(1024 * 1024, 10);
        let evidence = Arc::downgrade(&original);
        cache.publish(key(0), original);
        for id in 1..=32 {
            cache.publish(key(id), payload(1024 * 1024, 10));
        }
        assert!(cache.get(&key(0)).is_none());
        assert!(evidence.upgrade().is_none());
        assert_eq!(cache.stats(), (32, MAX_CACHE_BYTES));
        cache.expire_before(10);
        assert_eq!(cache.stats().0, 32);
        cache.expire_before(11);
        assert_eq!(cache.stats(), (0, 0));
    }

    #[test]
    fn replacement_and_clear_release_old_evidence_and_correct_accounting() {
        let mut cache = MemoryCache::new();
        let original = payload(2048, 0);
        let weak = Arc::downgrade(&original);
        cache.publish(key(0), original);
        cache.publish(key(0), payload(4096, 1));
        assert!(weak.upgrade().is_none());
        assert_eq!(cache.stats(), (1, 4096));
        cache.clear();
        assert_eq!(cache.stats(), (0, 0));
    }
}
