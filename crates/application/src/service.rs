use crate::persistence::{PersistentStore, StorageMetrics};
use crate::{
    Clock, FRESH_SECONDS, FetchedPayload, MAX_PROCESSED_BYTES, MAX_RAW_BYTES, RETENTION_SECONDS,
    Source, SystemClock,
};
use openlegal_domain::{
    Freshness, FreshnessRequirement, FreshnessState, ProgressStage, Provenance, Query, Record,
    RetrievalData, RetrievalEnvelope, RetrievalError, valid_identifier,
};
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{
    sync::{Semaphore, watch},
    task::JoinSet,
    time::Instant,
};
use tokio_util::sync::CancellationToken;

#[path = "service/persistent.rs"]
mod persistent;

const MAX_IN_FLIGHT: usize = 32;
const MAX_WAITERS_PER_KEY: usize = 16;
const MAX_WAITERS: usize = 64;
const REFRESH_DEADLINE: Duration = Duration::from_secs(10);
const ATTEMPT_DEADLINE: Duration = Duration::from_secs(5);
const FILESYSTEM_MAINTENANCE_INTERVAL: Duration = Duration::from_secs(60);

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct CacheKey {
    provider: String,
    dataset: String,
    processor_version: String,
    schema_version: u32,
    query: Query,
}

impl CacheKey {
    /// Construct a namespaced/versioned key for an already registered source.
    pub fn for_source(source: &Source, query: Query) -> Result<Self, RetrievalError> {
        query.validate()?;
        if query.source() != source.id {
            return Err(RetrievalError::InvalidInput);
        }
        Ok(Self {
            provider: source.provider.clone(),
            dataset: source.dataset.clone(),
            processor_version: source.processor_version.clone(),
            schema_version: 1,
            query,
        })
    }
}

pub struct StoredPayload {
    pub data: RetrievalData,
    pub provenance: Provenance,
    // Raw evidence lives and is evicted atomically with this result.
    pub raw: Vec<u8>,
    pub bytes: usize,
    pub snapshot: Option<openlegal_domain::history::SnapshotReference>,
}
/// Storage mechanics only; freshness and retention cutoffs are supplied by the service.
/// Implementations publish/evict the entire evidence/result envelope atomically,
/// bound bytes and entries, and never decide whether callers may receive stale data.
pub trait CacheStore: Send + 'static {
    fn get(&mut self, key: &CacheKey) -> Option<Arc<StoredPayload>>;
    fn publish(&mut self, key: CacheKey, value: Arc<StoredPayload>);
    fn expire_before(&mut self, validated_before: u64);
    fn clear(&mut self);
    fn stats(&self) -> (usize, usize);
}
type Outcome = Result<Arc<StoredPayload>, RetrievalError>;
type PublishedOutcome = Result<(Arc<StoredPayload>, Option<u64>), RetrievalError>;
struct Flight {
    generation: u64,
    waiters: usize,
    cancellation: CancellationToken,
    outcome: watch::Sender<Option<PublishedOutcome>>,
    progress: watch::Sender<ProgressStage>,
}
struct State {
    cache: Box<dyn CacheStore>,
    flights: HashMap<CacheKey, Flight>,
    waiters: usize,
    generation: u64,
    stopping: bool,
    storage_epoch: u64,
}
struct Rate {
    tokens: f64,
    at: Instant,
    cooldown: Option<Instant>,
    // Unrepresentable provider guidance must never become an immediate retry.
    // This fixed state remains paused until the service is reconstructed.
    paused: Option<RetrievalError>,
}
struct Provider {
    active: Arc<Semaphore>,
    rate: Mutex<Rate>,
}

#[derive(Default)]
struct Counters {
    hits: AtomicU64,
    misses: AtomicU64,
    requests: AtomicU64,
    coalesced: AtomicU64,
    failures: AtomicU64,
    stale: AtomicU64,
    retries: AtomicU64,
    throttled: AtomicU64,
    saturated: AtomicU64,
}

/// Fixed-label operational counters and bounded-state gauges, without query data.
#[derive(Clone, Debug, Default)]
pub struct MetricsSnapshot {
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub upstream_requests: u64,
    pub coalesced: u64,
    pub refresh_failures: u64,
    pub stale_responses: u64,
    pub retries: u64,
    pub throttled: u64,
    pub saturation: u64,
    pub cache_entries: usize,
    pub cache_bytes: usize,
    pub in_flight: usize,
    pub waiters: usize,
    pub filesystem: StorageMetrics,
}

pub struct RetrievalService {
    sources: HashMap<String, Arc<Source>>,
    providers: HashMap<String, Arc<Provider>>,
    clock: Arc<dyn Clock>,
    state: Mutex<State>,
    jobs: Mutex<JoinSet<()>>,
    shutdown: CancellationToken,
    failed: AtomicBool,
    counters: Counters,
    persistence: Option<Arc<dyn PersistentStore>>,
    namespace: String,
}

impl RetrievalService {
    /// Bounded startup registration metadata for capability descriptions.
    pub fn source_processor_versions(&self) -> std::collections::BTreeMap<String, String> {
        self.sources
            .iter()
            .map(|(id, source)| (id.clone(), source.processor_version.clone()))
            .collect()
    }

    pub fn new(
        sources: Vec<Source>,
        cache: Box<dyn CacheStore>,
    ) -> Result<Arc<Self>, RetrievalError> {
        Self::with_clock(sources, Arc::new(SystemClock::default()), cache)
    }

    pub fn with_clock(
        sources: Vec<Source>,
        clock: Arc<dyn Clock>,
        cache: Box<dyn CacheStore>,
    ) -> Result<Arc<Self>, RetrievalError> {
        Self::build(sources, clock, cache, None, String::new())
    }

    pub fn with_persistence(
        sources: Vec<Source>,
        clock: Arc<dyn Clock>,
        cache: Box<dyn CacheStore>,
        store: Arc<dyn PersistentStore>,
        namespace: String,
    ) -> Result<Arc<Self>, RetrievalError> {
        if !valid_identifier(&namespace, 128) || !store.healthy() {
            return Err(RetrievalError::InvalidInput);
        }
        store.policy().validate()?;
        Self::build(sources, clock, cache, Some(store), namespace)
    }

    fn build(
        sources: Vec<Source>,
        clock: Arc<dyn Clock>,
        cache: Box<dyn CacheStore>,
        persistence: Option<Arc<dyn PersistentStore>>,
        namespace: String,
    ) -> Result<Arc<Self>, RetrievalError> {
        if sources.is_empty() || sources.len() > 32 {
            return Err(RetrievalError::InvalidInput);
        }
        tokio::runtime::Handle::try_current().map_err(|_| RetrievalError::Internal)?;
        let mut registered = HashMap::new();
        let mut providers = HashMap::new();
        for source in sources {
            if !valid_identifier(&source.id, 64)
                || !valid_identifier(&source.provider, 64)
                || !valid_identifier(&source.dataset, 64)
                || source.processor_version.is_empty()
                || source.processor_version.len() > 128
                || registered.contains_key(&source.id)
            {
                return Err(RetrievalError::InvalidInput);
            }
            providers.entry(source.provider.clone()).or_insert_with(|| {
                Arc::new(Provider {
                    active: Arc::new(Semaphore::new(2)),
                    rate: Mutex::new(Rate {
                        tokens: 2.0,
                        at: Instant::now(),
                        cooldown: None,
                        paused: None,
                    }),
                })
            });
            registered.insert(source.id.clone(), Arc::new(source));
        }
        let service = Arc::new(Self {
            sources: registered,
            providers,
            clock,
            state: Mutex::new(State {
                cache,
                flights: HashMap::new(),
                waiters: 0,
                generation: 0,
                stopping: false,
                storage_epoch: persistence.as_ref().map_or(0, |store| store.epoch()),
            }),
            jobs: Mutex::new(JoinSet::new()),
            shutdown: CancellationToken::new(),
            failed: AtomicBool::new(false),
            counters: Counters::default(),
            persistence,
            namespace,
        });
        let weak = Arc::downgrade(&service);
        let token = service.shutdown.clone();
        service.jobs.lock().map_err(|_| RetrievalError::Internal)?.spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            let mut filesystem_maintenance_at = Instant::now() + FILESYSTEM_MAINTENANCE_INTERVAL;
            loop {
                tokio::select! {
                    biased;
                    _ = token.cancelled() => break,
                    _ = interval.tick() => {
                        let Some(service) = weak.upgrade() else { break };
                        if let Some(store) = &service.persistence
                            && Instant::now() >= filesystem_maintenance_at {
                            filesystem_maintenance_at = Instant::now() + FILESYSTEM_MAINTENANCE_INTERVAL;
                            // The storage port owns bounded cleanup; never drop its active future.
                            let result = store.maintain(service.clock.now()).await;
                            if result.is_err() && !store.healthy() { service.fail_storage(); }
                        }
                        if let Ok(mut state) = service.state.lock() { service.sync_epoch(&mut state); expire(&mut state, service.clock.now()); }
                        if let Ok(mut jobs) = service.jobs.lock() {
                            while let Some(joined) = jobs.try_join_next() {
                                if joined.is_err() { service.failed.store(true, Ordering::Relaxed); service.shutdown.cancel(); }
                            }
                        }
                    }
                }
            }
        });
        Ok(service)
    }

    /// All calls share representation work; each waiter's stale permission and
    /// cancellation apply only to that waiter. No user-controlled URL is accepted.
    pub async fn retrieve(
        self: &Arc<Self>,
        query: Query,
        freshness: FreshnessRequirement,
        cancellation: CancellationToken,
        progress: Option<watch::Sender<ProgressStage>>,
    ) -> Result<RetrievalEnvelope<RetrievalData>, RetrievalError> {
        query.validate()?;
        if cancellation.is_cancelled() {
            return Err(RetrievalError::Cancelled);
        }
        if let Some(sender) = &progress {
            sender.send_replace(ProgressStage::Accepted);
        }
        let source = self
            .sources
            .get(query.source())
            .ok_or(RetrievalError::UnknownSource)?
            .clone();
        let key = CacheKey::for_source(&source, query.clone())?;
        let (mut result_rx, mut progress_rx, generation) = {
            let mut state = self.state.lock().map_err(|_| RetrievalError::Internal)?;
            if state.stopping || self.shutdown.is_cancelled() {
                return Err(RetrievalError::Shutdown);
            }
            self.sync_epoch(&mut state);
            if !self.storage_healthy() {
                return Err(RetrievalError::StorageUnavailable);
            }
            expire(&mut state, self.clock.now());
            let cached = state.cache.get(&key);
            if let Some(value) = &cached
                && self
                    .usable_age(value)
                    .is_some_and(|age| age < FRESH_SECONDS)
            {
                self.counters.hits.fetch_add(1, Ordering::Relaxed);
                if let Some(sender) = &progress {
                    sender.send_replace(ProgressStage::Complete);
                }
                return Ok(self.envelope(value, FreshnessState::Fresh));
            }
            self.counters.misses.fetch_add(1, Ordering::Relaxed);
            if state.waiters >= MAX_WAITERS {
                return Err(self.busy());
            }
            let existing = state.flights.get_mut(&key);
            let (rx, stage, generation) = if let Some(flight) = existing {
                if flight.cancellation.is_cancelled() {
                    return Err(self.busy());
                }
                if flight.waiters >= MAX_WAITERS_PER_KEY {
                    return Err(self.busy());
                }
                flight.waiters += 1;
                self.counters.coalesced.fetch_add(1, Ordering::Relaxed);
                (
                    flight.outcome.subscribe(),
                    flight.progress.subscribe(),
                    flight.generation,
                )
            } else {
                if state.flights.len() >= MAX_IN_FLIGHT {
                    return Err(self.busy());
                }
                let provider = self
                    .providers
                    .get(&source.provider)
                    .ok_or(RetrievalError::Internal)?
                    .clone();
                // Memory-only admission remains unchanged. L2 gets its own bounded
                // flight before acquiring scarce upstream concurrency.
                let permit = if self.persistence.is_none() {
                    Some(
                        provider
                            .active
                            .clone()
                            .try_acquire_owned()
                            .map_err(|_| self.busy())?,
                    )
                } else {
                    None
                };
                let (outcome, rx) = watch::channel(None);
                let initial_stage = if self.persistence.is_some() {
                    ProgressStage::Accepted
                } else {
                    ProgressStage::Refreshing
                };
                let (stage_sender, stage) = watch::channel(initial_stage);
                let token = self.shutdown.child_token();
                state.generation = state.generation.wrapping_add(1);
                let generation = state.generation;
                state.flights.insert(
                    key.clone(),
                    Flight {
                        generation,
                        waiters: 1,
                        cancellation: token.clone(),
                        outcome,
                        progress: stage_sender,
                    },
                );
                let service = self.clone();
                let refresh_key = key.clone();
                let mut jobs = self.jobs.lock().map_err(|_| RetrievalError::Internal)?;
                // Reap completed jobs at admission; the periodic job is separately bounded.
                while let Some(joined) = jobs.try_join_next() {
                    if joined.is_err() {
                        self.failed.store(true, Ordering::Relaxed);
                        self.shutdown.cancel();
                        state.stopping = true;
                        return Err(RetrievalError::Internal);
                    }
                }
                jobs.spawn(async move {
                    let _permit = permit;
                    if service.persistence.is_some() {
                        let result = service
                            .resolve_persistent(
                                &source,
                                &provider,
                                refresh_key.clone(),
                                query,
                                &token,
                                generation,
                            )
                            .await;
                        service.publish(refresh_key, generation, result);
                        return;
                    }
                    let result = tokio::select! {
                        biased;
                        _ = token.cancelled() => Err(RetrievalError::Cancelled),
                        result = tokio::time::timeout(REFRESH_DEADLINE,
                            service.refresh(&source, &provider, query, &token, generation)) => {
                            result.unwrap_or(Err(RetrievalError::Unavailable))
                        }
                    };
                    service.publish(refresh_key, generation, result.map(|value| (value, None)));
                });
                (rx, stage, generation)
            };
            state.waiters += 1;
            (rx, stage, generation)
        };
        let _waiter = Waiter {
            service: Arc::downgrade(self),
            key: key.clone(),
            generation,
        };
        let outcome = loop {
            if cancellation.is_cancelled() {
                return Err(RetrievalError::Cancelled);
            }
            if self.shutdown.is_cancelled() {
                return Err(RetrievalError::Shutdown);
            }
            let completed = result_rx.borrow_and_update().clone();
            if let Some(result) = completed {
                break result;
            }
            if let Some(sender) = &progress {
                sender.send_replace(*progress_rx.borrow_and_update());
            }
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => return Err(RetrievalError::Cancelled),
                _ = self.shutdown.cancelled() => return Err(RetrievalError::Shutdown),
                changed = result_rx.changed() => { if changed.is_err() { return Err(RetrievalError::Internal); } },
                changed = progress_rx.changed() => {
                    if changed.is_err() { /* Completion channel owns the outcome. */ }
                }
            }
        };
        let value = match outcome {
            Ok((value, epoch)) => {
                if let Some(expected) = epoch {
                    let mut state = self.state.lock().map_err(|_| RetrievalError::Internal)?;
                    self.sync_epoch(&mut state);
                    if state.storage_epoch != expected || !self.storage_healthy() {
                        return Err(RetrievalError::StorageUnavailable);
                    }
                }
                if !self
                    .usable_age(&value)
                    .is_some_and(|age| age < FRESH_SECONDS)
                {
                    return Err(RetrievalError::FreshnessUnavailable);
                }
                self.envelope(&value, FreshnessState::Fresh)
            }
            Err(error) if error.is_transient() && freshness == FreshnessRequirement::AllowStale => {
                let stale = {
                    let mut state = self.state.lock().map_err(|_| RetrievalError::Internal)?;
                    self.sync_epoch(&mut state);
                    if !self.storage_healthy() {
                        return Err(RetrievalError::StorageUnavailable);
                    }
                    expire(&mut state, self.clock.now());
                    state.cache.get(&key)
                };
                if let Some(value) = stale
                    && self
                        .usable_age(&value)
                        .is_some_and(|age| age <= RETENTION_SECONDS)
                {
                    let state = if self
                        .clock
                        .now()
                        .saturating_sub(value.provenance.validated_at)
                        < FRESH_SECONDS
                    {
                        FreshnessState::Fresh
                    } else {
                        self.counters.stale.fetch_add(1, Ordering::Relaxed);
                        FreshnessState::Stale
                    };
                    self.envelope(&value, state)
                } else {
                    return Err(error);
                }
            }
            Err(error) if error.is_transient() && freshness == FreshnessRequirement::FreshOnly => {
                return Err(RetrievalError::FreshnessUnavailable);
            }
            Err(error) => return Err(error),
        };
        if let Some(sender) = &progress {
            sender.send_replace(ProgressStage::Complete);
        }
        Ok(value)
    }

    fn busy(&self) -> RetrievalError {
        self.counters.saturated.fetch_add(1, Ordering::Relaxed);
        RetrievalError::Busy
    }

    fn envelope(
        &self,
        value: &StoredPayload,
        state: FreshnessState,
    ) -> RetrievalEnvelope<RetrievalData> {
        RetrievalEnvelope {
            data: value.data.clone(),
            provenance: value.provenance.clone(),
            snapshot: value.snapshot.clone(),
            synthetic: true,
            freshness: Freshness {
                state,
                age_seconds: self
                    .clock
                    .now()
                    .saturating_sub(value.provenance.validated_at),
            },
        }
    }

    async fn refresh(
        &self,
        source: &Source,
        provider: &Provider,
        query: Query,
        cancellation: &CancellationToken,
        generation: u64,
    ) -> Outcome {
        for attempt in 0..2 {
            self.acquire_rate(provider, cancellation).await?;
            if attempt != 0 {
                self.counters.retries.fetch_add(1, Ordering::Relaxed);
            }
            self.counters.requests.fetch_add(1, Ordering::Relaxed);
            let result = tokio::time::timeout(
                ATTEMPT_DEADLINE,
                source
                    .upstream
                    .fetch(query.clone(), cancellation.child_token()),
            )
            .await
            .unwrap_or(Err(RetrievalError::Unavailable));
            match result {
                Ok(payload) => {
                    if cancellation.is_cancelled() {
                        return Err(RetrievalError::Cancelled);
                    }
                    self.set_stage(&query, generation, ProgressStage::Validating);
                    return self.validate(source, &query, payload);
                }
                Err(error) if error.is_transient() => {
                    if matches!(error, RetrievalError::Throttled { .. }) {
                        self.counters.throttled.fetch_add(1, Ordering::Relaxed);
                    }
                    // One fixed cooldown slot per registered provider, never a per-query failure cache.
                    let seconds = match error {
                        RetrievalError::Throttled {
                            retry_after_secs: Some(seconds),
                        } => seconds,
                        _ => 1,
                    };
                    // Keep the full provider delay; overflow is conservatively unavailable.
                    // Jitter extends the floor, never retries before Retry-After.
                    let jitter = Duration::from_millis(100 + (generation.wrapping_mul(73) % 151));
                    let delay = Duration::from_secs(seconds).saturating_add(jitter);
                    let Some(until) = Instant::now().checked_add(delay) else {
                        provider
                            .rate
                            .lock()
                            .map_err(|_| RetrievalError::Internal)?
                            .paused = Some(error);
                        return Err(error);
                    };
                    {
                        let mut rate =
                            provider.rate.lock().map_err(|_| RetrievalError::Internal)?;
                        rate.cooldown =
                            Some(rate.cooldown.map_or(until, |existing| existing.max(until)));
                    }
                    if attempt == 1 {
                        return Err(error);
                    }
                }
                Err(error) => return Err(error),
            }
        }
        Err(RetrievalError::Unavailable)
    }

    async fn acquire_rate(
        &self,
        provider: &Provider,
        cancellation: &CancellationToken,
    ) -> Result<(), RetrievalError> {
        loop {
            let delay = {
                let mut rate = provider.rate.lock().map_err(|_| RetrievalError::Internal)?;
                if let Some(error) = rate.paused {
                    return Err(error);
                }
                let now = Instant::now();
                rate.tokens =
                    (rate.tokens + now.duration_since(rate.at).as_secs_f64() * 2.0).min(2.0);
                rate.at = now;
                if let Some(until) = rate.cooldown
                    && until > now
                {
                    until - now
                } else if rate.tokens >= 1.0 {
                    rate.cooldown = None;
                    rate.tokens -= 1.0;
                    return Ok(());
                } else {
                    Duration::from_secs_f64((1.0 - rate.tokens) / 2.0)
                }
            };
            tokio::select! {
                _ = cancellation.cancelled() => return Err(RetrievalError::Cancelled),
                _ = tokio::time::sleep(delay) => {}
            }
        }
    }

    fn set_stage(&self, query: &Query, generation: u64, stage: ProgressStage) {
        if let Ok(state) = self.state.lock() {
            for (key, flight) in &state.flights {
                if &key.query == query && flight.generation == generation {
                    flight.progress.send_replace(stage);
                    break;
                }
            }
        }
    }

    fn validate(&self, source: &Source, query: &Query, payload: FetchedPayload) -> Outcome {
        if payload.raw.len() > MAX_RAW_BYTES
            || payload.source_reference.is_empty()
            || payload.source_reference.len() > 2048
        {
            return Err(RetrievalError::ResourceLimit);
        }
        // The reference contract is deliberately narrow: the adapter supplies a
        // configured source path, never query strings, fragments or credentials.
        if payload.source_reference.chars().any(char::is_control)
            || payload.source_reference.contains(['?', '#', '@'])
        {
            return Err(RetrievalError::InvalidPayload);
        }
        let valid = match (&payload.data, query) {
            (RetrievalData::Get(record), Query::Get { id, .. }) => {
                valid_record(record, query) && &record.id == id
            }
            (
                RetrievalData::Search(page),
                Query::Search {
                    page: requested,
                    page_size,
                    ..
                },
            ) => {
                let mut ids = std::collections::HashSet::new();
                page.page == *requested
                    && page.page_size == *page_size
                    && page.records.len() <= *page_size as usize
                    && page.total <= 20_000
                    && page.records.len() as u64
                        == u64::from(page.page_size).min(
                            u64::from(page.total)
                                .saturating_sub(u64::from(page.page) * u64::from(page.page_size)),
                        )
                    && page
                        .records
                        .iter()
                        .all(|record| valid_record(record, query) && ids.insert(&record.id))
            }
            _ => false,
        };
        if !valid {
            return Err(RetrievalError::InvalidPayload);
        }
        let encoded = bounded_size(&payload.data, MAX_PROCESSED_BYTES)?;
        let now = self.clock.now();
        let bytes = payload.raw.len() + encoded + payload.source_reference.len() + 1024;
        let digest: String = Sha256::digest(&payload.raw)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        Ok(Arc::new(StoredPayload {
            data: payload.data,
            provenance: Provenance {
                provider: source.provider.clone(),
                dataset: source.dataset.clone(),
                source_reference: payload.source_reference,
                payload_sha256: digest,
                processor_version: source.processor_version.clone(),
                retrieved_at: now,
                validated_at: now,
            },
            raw: payload.raw,
            bytes,
            snapshot: None,
        }))
    }

    fn publish(&self, key: CacheKey, generation: u64, mut outcome: PublishedOutcome) {
        let Ok(mut state) = self.state.lock() else {
            self.shutdown.cancel();
            return;
        };
        let Some(flight) = state.flights.get(&key) else {
            return;
        };
        if flight.generation != generation {
            return;
        }
        if flight.cancellation.is_cancelled() || state.stopping {
            if let Some(flight) = state.flights.remove(&key) {
                state.waiters -= flight.waiters;
                flight
                    .outcome
                    .send_replace(Some(Err(RetrievalError::Cancelled)));
            }
            return;
        }
        self.sync_epoch(&mut state);
        if let Ok((_, Some(epoch))) = &outcome
            && (*epoch != state.storage_epoch || !self.storage_healthy())
        {
            outcome = Err(RetrievalError::StorageUnavailable);
        }
        if let Ok((value, _)) = &outcome {
            expire(&mut state, self.clock.now());
            state.cache.publish(key.clone(), value.clone());
        } else {
            self.counters.failures.fetch_add(1, Ordering::Relaxed);
        }
        if let Some(flight) = state.flights.remove(&key) {
            state.waiters -= flight.waiters;
            flight.outcome.send_replace(Some(outcome));
        }
    }

    /// Cancel admission and all refreshes, then join every owned worker. Join
    /// errors are returned; shutdown never represents an unobserved panic as success.
    pub async fn shutdown(&self) -> Result<(), RetrievalError> {
        {
            let mut state = self.state.lock().map_err(|_| RetrievalError::Internal)?;
            state.stopping = true;
            self.shutdown.cancel();
            for (_, flight) in state.flights.drain() {
                flight.cancellation.cancel();
                flight
                    .outcome
                    .send_replace(Some(Err(RetrievalError::Shutdown)));
            }
            state.waiters = 0;
            state.cache.clear();
        }
        let mut jobs = {
            let mut jobs = self.jobs.lock().map_err(|_| RetrievalError::Internal)?;
            std::mem::take(&mut *jobs)
        };
        let mut failed = false;
        // Closing admission also interrupts maintenance/recovery before joining
        // owned jobs; shutdown must not wait for a full recovery deadline.
        if let Some(store) = &self.persistence {
            failed |= store.close().await.is_err();
        }
        while let Some(result) = jobs.join_next().await {
            failed |= result.is_err();
        }
        if failed || self.failed.load(Ordering::Relaxed) {
            Err(RetrievalError::Internal)
        } else {
            Ok(())
        }
    }

    /// Supervised host worker. Internal failures stop endpoint readiness; normal
    /// host shutdown cancels and joins the same owned refresh/maintenance jobs.
    pub async fn run(&self, host_shutdown: CancellationToken) -> Result<(), RetrievalError> {
        tokio::select! {
            biased;
            _ = self.shutdown.cancelled() => {},
            _ = host_shutdown.cancelled() => {},
        }
        self.shutdown().await
    }

    pub fn metrics(&self) -> MetricsSnapshot {
        let counters = &self.counters;
        let mut snapshot = MetricsSnapshot {
            cache_hits: counters.hits.load(Ordering::Relaxed),
            cache_misses: counters.misses.load(Ordering::Relaxed),
            upstream_requests: counters.requests.load(Ordering::Relaxed),
            coalesced: counters.coalesced.load(Ordering::Relaxed),
            refresh_failures: counters.failures.load(Ordering::Relaxed),
            stale_responses: counters.stale.load(Ordering::Relaxed),
            retries: counters.retries.load(Ordering::Relaxed),
            throttled: counters.throttled.load(Ordering::Relaxed),
            saturation: counters.saturated.load(Ordering::Relaxed),
            filesystem: self
                .persistence
                .as_ref()
                .map_or_else(StorageMetrics::default, |store| store.metrics()),
            ..Default::default()
        };
        if let Ok(state) = self.state.lock() {
            let (entries, bytes) = state.cache.stats();
            snapshot.cache_entries = entries;
            snapshot.cache_bytes = bytes;
            snapshot.in_flight = state.flights.len();
            snapshot.waiters = state.waiters;
        }
        snapshot
    }

    pub fn metrics_prometheus(&self) -> String {
        let m = self.metrics();
        let mut output = format!(
            "openlegal_cache_hits_total {}\nopenlegal_cache_misses_total {}\nopenlegal_upstream_requests_total {}\nopenlegal_coalesced_total {}\nopenlegal_refresh_failures_total {}\nopenlegal_stale_responses_total {}\nopenlegal_retries_total {}\nopenlegal_throttled_total {}\nopenlegal_retrieval_saturation_total {}\nopenlegal_cache_entries {}\nopenlegal_cache_bytes {}\nopenlegal_refreshes {}\nopenlegal_waiters {}\n",
            m.cache_hits,
            m.cache_misses,
            m.upstream_requests,
            m.coalesced,
            m.refresh_failures,
            m.stale_responses,
            m.retries,
            m.throttled,
            m.saturation,
            m.cache_entries,
            m.cache_bytes,
            m.in_flight,
            m.waiters
        );
        if self.persistence.is_some() {
            let fs = m.filesystem;
            output.push_str(&format!("openlegal_filesystem_hits_total {}\nopenlegal_filesystem_misses_total {}\nopenlegal_filesystem_writes_total {}\nopenlegal_filesystem_evictions_total {}\nopenlegal_filesystem_corruptions_total {}\nopenlegal_filesystem_recoveries_total {}\nopenlegal_filesystem_saturation_total {}\nopenlegal_filesystem_bytes {}\nopenlegal_filesystem_snapshots {}\n", fs.hits, fs.misses, fs.writes, fs.evictions, fs.corruptions, fs.recoveries, fs.saturation, fs.bytes, fs.snapshots));
        }
        output
    }
}

fn valid_record(record: &Record, query: &Query) -> bool {
    record.synthetic
        && record.source == query.source()
        && valid_identifier(&record.id, 128)
        && !record.title.is_empty()
        && record.title.len() <= 1024
        && record.body.len() <= 16 * 1024
}
fn bounded_size(value: &RetrievalData, limit: usize) -> Result<usize, RetrievalError> {
    struct Counter {
        used: usize,
        limit: usize,
    }
    impl std::io::Write for Counter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if bytes.len() > self.limit - self.used {
                return Err(std::io::Error::other("output limit"));
            }
            self.used += bytes.len();
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut counter = Counter { used: 0, limit };
    serde_json::to_writer(&mut counter, value).map_err(|_| RetrievalError::ResourceLimit)?;
    Ok(counter.used)
}
fn expire(state: &mut State, now: u64) {
    state
        .cache
        .expire_before(now.saturating_sub(RETENTION_SECONDS));
}

struct Waiter {
    service: Weak<RetrievalService>,
    key: CacheKey,
    generation: u64,
}
impl Drop for Waiter {
    fn drop(&mut self) {
        let Some(service) = self.service.upgrade() else {
            return;
        };
        let Ok(mut state) = service.state.lock() else {
            service.shutdown.cancel();
            return;
        };
        if let Some(flight) = state.flights.get_mut(&self.key)
            && flight.generation == self.generation
        {
            flight.waiters -= 1;
            let last = flight.waiters == 0;
            state.waiters -= 1;
            if last {
                if service.persistence.is_some() {
                    // An active storage transaction must reconcile before the key
                    // can admit a replacement generation.
                    if let Some(flight) = state.flights.get(&self.key) {
                        flight.cancellation.cancel();
                    }
                } else if let Some(flight) = state.flights.remove(&self.key) {
                    flight.cancellation.cancel();
                }
            }
        }
    }
}

impl Drop for RetrievalService {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Upstream;
    use futures::future::BoxFuture;
    use openlegal_domain::SearchPage;
    use std::sync::atomic::AtomicUsize;

    #[derive(Default)]
    struct TestCache(HashMap<CacheKey, Arc<StoredPayload>>);
    impl CacheStore for TestCache {
        fn get(&mut self, key: &CacheKey) -> Option<Arc<StoredPayload>> {
            self.0.get(key).cloned()
        }
        fn publish(&mut self, key: CacheKey, value: Arc<StoredPayload>) {
            self.0.insert(key, value);
        }
        fn expire_before(&mut self, before: u64) {
            self.0
                .retain(|_, value| value.provenance.validated_at >= before);
        }
        fn clear(&mut self) {
            self.0.clear();
        }
        fn stats(&self) -> (usize, usize) {
            (self.0.len(), self.0.values().map(|v| v.bytes).sum())
        }
    }
    #[derive(Default)]
    struct TestClock(AtomicU64);
    impl Clock for TestClock {
        fn now(&self) -> u64 {
            self.0.load(Ordering::Relaxed)
        }
    }
    struct Mock {
        calls: Arc<AtomicUsize>,
        behavior: Arc<AtomicUsize>,
        delay: Duration,
    }
    impl Upstream for Mock {
        fn fetch(
            &self,
            query: Query,
            _cancellation: CancellationToken,
        ) -> BoxFuture<'static, Result<FetchedPayload, RetrievalError>> {
            let calls = self.calls.clone();
            let behavior = self.behavior.clone();
            let delay = self.delay;
            Box::pin(async move {
                calls.fetch_add(1, Ordering::Relaxed);
                let mode = behavior.load(Ordering::Relaxed);
                tokio::time::sleep(delay).await;
                match mode {
                    1 => return Err(RetrievalError::Unavailable),
                    2 => return Err(RetrievalError::NormalizationFailed),
                    4 => {
                        return Err(RetrievalError::Throttled {
                            retry_after_secs: Some(30),
                        });
                    }
                    5 => panic!("synthetic worker panic"),
                    8 => {
                        return Err(RetrievalError::Throttled {
                            retry_after_secs: Some(u64::MAX),
                        });
                    }
                    _ => {}
                }
                let id = match &query {
                    Query::Get { id, .. } => id.clone(),
                    _ => "001".into(),
                };
                let record = Record {
                    source: query.source().into(),
                    id: if mode == 3 { "wrong-id".into() } else { id },
                    title: "Synthetic record".into(),
                    body: "Not legal information".into(),
                    synthetic: true,
                };
                let data = match query {
                    Query::Get { .. } => RetrievalData::Get(record),
                    Query::Search {
                        page, page_size, ..
                    } => RetrievalData::Search(SearchPage {
                        records: if mode == 7 { vec![] } else { vec![record] },
                        page,
                        page_size,
                        total: match mode {
                            6 => 0,
                            7 => 10,
                            _ => page * page_size + 1,
                        },
                    }),
                };
                Ok(FetchedPayload {
                    raw: b"synthetic-payload".to_vec(),
                    data,
                    source_reference: "http://127.0.0.1:8081/synthetic".into(),
                })
            })
        }
    }
    fn query(id: &str) -> Query {
        Query::Get {
            source: "layout_a".into(),
            id: id.into(),
        }
    }
    fn fixture() -> (
        Arc<RetrievalService>,
        Arc<TestClock>,
        Arc<AtomicUsize>,
        Arc<AtomicUsize>,
    ) {
        let calls = Arc::new(AtomicUsize::new(0));
        let behavior = Arc::new(AtomicUsize::new(0));
        let clock = Arc::new(TestClock::default());
        let upstream = Arc::new(Mock {
            calls: calls.clone(),
            behavior: behavior.clone(),
            delay: Duration::from_millis(100),
        });
        let source = Source {
            id: "layout_a".into(),
            provider: "synthetic".into(),
            dataset: "records".into(),
            processor_version: "v1".into(),
            upstream,
        };
        let service =
            RetrievalService::with_clock(vec![source], clock.clone(), Box::<TestCache>::default())
                .unwrap();
        (service, clock, calls, behavior)
    }
    async fn retrieve(
        service: &Arc<RetrievalService>,
        id: &str,
    ) -> Result<RetrievalEnvelope<RetrievalData>, RetrievalError> {
        service
            .retrieve(
                query(id),
                FreshnessRequirement::AllowStale,
                CancellationToken::new(),
                None,
            )
            .await
    }
    async fn wait_for_waiters(service: &RetrievalService, waiters: usize) {
        for _ in 0..100 {
            if service.metrics().waiters == waiters {
                return;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(service.metrics().waiters, waiters);
    }

    #[tokio::test(start_paused = true)]
    async fn fresh_hit_preserves_original_evidence_and_idle_expiry_removes_it() {
        let (service, clock, calls, _) = fixture();
        let first = retrieve(&service, "001").await.unwrap();
        clock.0.store(59, Ordering::Relaxed);
        let cached = retrieve(&service, "001").await.unwrap();
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        assert_eq!(cached.freshness.age_seconds, 59);
        assert_eq!(cached.provenance, first.provenance);
        assert_eq!(cached.provenance.payload_sha256.len(), 64);
        assert!(service.metrics().cache_bytes > b"synthetic-payload".len());
        clock.0.store(301, Ordering::Relaxed);
        tokio::time::advance(Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
        assert_eq!(service.metrics().cache_entries, 0);
        assert_eq!(service.metrics().cache_bytes, 0);
        service.shutdown().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn concurrent_waiters_share_one_refresh_and_enforce_per_key_limit() {
        let (service, _, calls, _) = fixture();
        let mut handles = Vec::new();
        for _ in 0..16 {
            let service = service.clone();
            handles.push(tokio::spawn(async move { retrieve(&service, "001").await }));
        }
        wait_for_waiters(&service, 16).await;
        assert_eq!(retrieve(&service, "001").await, Err(RetrievalError::Busy));
        for handle in handles {
            handle.await.unwrap().unwrap();
        }
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        assert_eq!(service.metrics().coalesced, 15);
        assert_eq!(service.metrics().waiters, 0);
        service.shutdown().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn mixed_freshness_waiters_share_failed_refresh_but_choose_own_fallback() {
        let (service, clock, calls, behavior) = fixture();
        retrieve(&service, "001").await.unwrap();
        clock.0.store(60, Ordering::Relaxed);
        behavior.store(1, Ordering::Relaxed);
        let stale_service = service.clone();
        let stale = tokio::spawn(async move { retrieve(&stale_service, "001").await });
        let fresh_service = service.clone();
        let fresh = tokio::spawn(async move {
            fresh_service
                .retrieve(
                    query("001"),
                    FreshnessRequirement::FreshOnly,
                    CancellationToken::new(),
                    None,
                )
                .await
        });
        wait_for_waiters(&service, 2).await;
        let stale = stale.await.unwrap().unwrap();
        assert_eq!(stale.freshness.state, FreshnessState::Stale);
        assert_eq!(stale.freshness.age_seconds, 60);
        assert_eq!(
            fresh.await.unwrap(),
            Err(RetrievalError::FreshnessUnavailable)
        );
        assert_eq!(calls.load(Ordering::Relaxed), 3);
        assert_eq!(service.metrics().retries, 1);
        service.shutdown().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn stale_fallback_includes_exact_boundary_but_never_crosses_it_during_refresh() {
        let (service, clock, _, behavior) = fixture();
        retrieve(&service, "001").await.unwrap();
        clock.0.store(300, Ordering::Relaxed);
        behavior.store(1, Ordering::Relaxed);
        let stale = retrieve(&service, "001").await.unwrap();
        assert_eq!(stale.freshness.state, FreshnessState::Stale);
        assert_eq!(stale.freshness.age_seconds, 300);
        let refreshing_service = service.clone();
        let refreshing = tokio::spawn(async move { retrieve(&refreshing_service, "001").await });
        wait_for_waiters(&service, 1).await;
        clock.0.store(301, Ordering::Relaxed);
        assert_eq!(refreshing.await.unwrap(), Err(RetrievalError::Unavailable));
        assert_eq!(service.metrics().cache_bytes, 0);
        service.shutdown().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn invalid_refresh_cannot_publish_or_trigger_stale_fallback() {
        let (service, clock, calls, behavior) = fixture();
        let first = retrieve(&service, "001").await.unwrap();
        clock.0.store(61, Ordering::Relaxed);
        for (mode, error) in [
            (2, RetrievalError::NormalizationFailed),
            (3, RetrievalError::InvalidPayload),
        ] {
            behavior.store(mode, Ordering::Relaxed);
            assert_eq!(retrieve(&service, "001").await, Err(error));
        }
        assert_eq!(calls.load(Ordering::Relaxed), 3);
        assert_eq!(service.metrics().cache_entries, 1);
        {
            let mut state = service.state.lock().unwrap();
            let source = service.sources.get("layout_a").unwrap();
            let key = CacheKey::for_source(source, query("001")).unwrap();
            assert_eq!(state.cache.get(&key).unwrap().provenance, first.provenance);
        }
        service.shutdown().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn cancelling_one_waiter_preserves_other_and_last_waiter_releases_generation() {
        let (service, _, calls, _) = fixture();
        let cancellation = CancellationToken::new();
        let a_service = service.clone();
        let a_token = cancellation.clone();
        let a = tokio::spawn(async move {
            a_service
                .retrieve(
                    query("001"),
                    FreshnessRequirement::AllowStale,
                    a_token,
                    None,
                )
                .await
        });
        let b_service = service.clone();
        let b = tokio::spawn(async move { retrieve(&b_service, "001").await });
        wait_for_waiters(&service, 2).await;
        cancellation.cancel();
        assert_eq!(a.await.unwrap(), Err(RetrievalError::Cancelled));
        b.await.unwrap().unwrap();
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        let token = CancellationToken::new();
        let single_service = service.clone();
        let single_token = token.clone();
        let single = tokio::spawn(async move {
            single_service
                .retrieve(
                    query("002"),
                    FreshnessRequirement::AllowStale,
                    single_token,
                    None,
                )
                .await
        });
        wait_for_waiters(&service, 1).await;
        token.cancel();
        assert_eq!(single.await.unwrap(), Err(RetrievalError::Cancelled));
        tokio::task::yield_now().await;
        assert_eq!(service.metrics().in_flight, 0);
        assert_eq!(service.metrics().cache_entries, 1);
        retrieve(&service, "002").await.unwrap();
        assert_eq!(service.metrics().cache_entries, 2);
        service.shutdown().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn provider_admission_rejects_backlog_and_retry_after_never_retries_early() {
        let (service, _, calls, behavior) = fixture();
        behavior.store(4, Ordering::Relaxed);
        let a_service = service.clone();
        let a = tokio::spawn(async move { retrieve(&a_service, "001").await });
        let b_service = service.clone();
        let b = tokio::spawn(async move { retrieve(&b_service, "002").await });
        wait_for_waiters(&service, 2).await;
        assert_eq!(retrieve(&service, "003").await, Err(RetrievalError::Busy));
        assert_eq!(a.await.unwrap(), Err(RetrievalError::Unavailable));
        assert_eq!(b.await.unwrap(), Err(RetrievalError::Unavailable));
        assert_eq!(calls.load(Ordering::Relaxed), 2);
        assert_eq!(service.metrics().retries, 0);
        service.shutdown().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn source_and_selectors_are_distinct_and_candidate_total_is_validated() {
        let (service, _, calls, behavior) = fixture();
        retrieve(&service, "001").await.unwrap();
        retrieve(&service, "002").await.unwrap();
        assert_eq!(calls.load(Ordering::Relaxed), 2);
        let query = Query::Search {
            source: "layout_a".into(),
            query: "synthetic".into(),
            page: 0,
            page_size: 5,
        };
        for mode in [6, 7] {
            behavior.store(mode, Ordering::Relaxed);
            assert_eq!(
                service
                    .retrieve(
                        query.clone(),
                        FreshnessRequirement::AllowStale,
                        CancellationToken::new(),
                        None
                    )
                    .await,
                Err(RetrievalError::InvalidPayload)
            );
        }
        let source = service.sources.get("layout_a").unwrap();
        let key = CacheKey::for_source(source, super::tests::query("001")).unwrap();
        let mut next_version = key.clone();
        next_version.processor_version = "v2".into();
        assert_ne!(key, next_version);
        service.shutdown().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn worker_panic_fails_host_and_rejects_previously_fresh_hits() {
        let (service, _, _, behavior) = fixture();
        retrieve(&service, "001").await.unwrap();
        behavior.store(5, Ordering::Relaxed);
        let failing_service = service.clone();
        let failing = tokio::spawn(async move { retrieve(&failing_service, "002").await });
        let monitor_service = service.clone();
        let monitor =
            tokio::spawn(async move { monitor_service.run(CancellationToken::new()).await });
        assert_eq!(monitor.await.unwrap(), Err(RetrievalError::Internal));
        assert_eq!(failing.await.unwrap(), Err(RetrievalError::Shutdown));
        assert_eq!(
            retrieve(&service, "001").await,
            Err(RetrievalError::Shutdown)
        );
    }
    fn multi_source(count: usize) -> (Arc<RetrievalService>, Arc<AtomicUsize>) {
        let calls = Arc::new(AtomicUsize::new(0));
        let upstream = Arc::new(Mock {
            calls: calls.clone(),
            behavior: Arc::new(AtomicUsize::new(0)),
            delay: Duration::from_millis(100),
        });
        let sources = (0..count)
            .map(|index| Source {
                id: format!("layout_{index}"),
                provider: format!("provider_{index}"),
                dataset: "records".into(),
                processor_version: "v1".into(),
                upstream: upstream.clone(),
            })
            .collect();
        (
            RetrievalService::with_clock(
                sources,
                Arc::<TestClock>::default(),
                Box::<TestCache>::default(),
            )
            .unwrap(),
            calls,
        )
    }
    fn source_query(index: usize, id: &str) -> Query {
        Query::Get {
            source: format!("layout_{index}"),
            id: id.into(),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn global_waiter_and_unique_key_caps_bound_cross_source_work() {
        let (service, calls) = multi_source(32);
        let mut handles = Vec::new();
        for source in 0..4 {
            for _ in 0..16 {
                let service = service.clone();
                handles.push(tokio::spawn(async move {
                    service
                        .retrieve(
                            source_query(source, "001"),
                            FreshnessRequirement::AllowStale,
                            CancellationToken::new(),
                            None,
                        )
                        .await
                }));
            }
        }
        wait_for_waiters(&service, 64).await;
        assert_eq!(
            service
                .retrieve(
                    source_query(4, "001"),
                    FreshnessRequirement::AllowStale,
                    CancellationToken::new(),
                    None
                )
                .await,
            Err(RetrievalError::Busy)
        );
        for handle in handles {
            handle.await.unwrap().unwrap();
        }
        assert_eq!(calls.load(Ordering::Relaxed), 4);
        assert_eq!(service.metrics().cache_entries, 4);
        let mut handles = Vec::new();
        for source in 0..32 {
            let service = service.clone();
            handles.push(tokio::spawn(async move {
                service
                    .retrieve(
                        source_query(source, "002"),
                        FreshnessRequirement::AllowStale,
                        CancellationToken::new(),
                        None,
                    )
                    .await
            }));
        }
        wait_for_waiters(&service, 32).await;
        assert_eq!(service.metrics().in_flight, 32);
        assert_eq!(
            service
                .retrieve(
                    source_query(0, "003"),
                    FreshnessRequirement::AllowStale,
                    CancellationToken::new(),
                    None
                )
                .await,
            Err(RetrievalError::Busy)
        );
        for handle in handles {
            handle.await.unwrap().unwrap();
        }
        assert_eq!(calls.load(Ordering::Relaxed), 36);
        assert_eq!(service.metrics().cache_entries, 36);
        service.shutdown().await.unwrap();
    }
    #[tokio::test(start_paused = true)]
    async fn unrepresentable_retry_after_pauses_provider_until_reconstruction() {
        let (service, _, calls, behavior) = fixture();
        behavior.store(8, Ordering::Relaxed);
        let expected = Err(RetrievalError::Throttled {
            retry_after_secs: Some(u64::MAX),
        });
        assert_eq!(retrieve(&service, "001").await, expected);
        assert_eq!(retrieve(&service, "002").await, expected);
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        assert_eq!(service.metrics().in_flight, 0);
        service.shutdown().await.unwrap();
    }
}
