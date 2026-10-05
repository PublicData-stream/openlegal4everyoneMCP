//! LAW OPEN DATA transport and evidence-backed field projection. Document parsing
//! is exclusively delegated to the configured disposable document processor.
use crate::{literal_ip, public_address};
use openlegal_application::document::{
    DocumentError, DocumentFormat, DocumentInput, DocumentNode, DocumentOutput, DocumentProcessor,
};
use openlegal_application::upstream_policy::RequestLimit;
use openlegal_domain::legal::*;
use openlegal_domain::rights::{OriginalResource, SourceRights};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Row};
use std::{
    collections::BTreeMap,
    net::SocketAddr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU32, Ordering},
    },
    time::Duration,
};
use tokio::{sync::Semaphore, time::Instant};
use tokio_util::sync::CancellationToken;
use url::Url;
mod admission;
pub mod catalog;
pub mod supplements;
pub use admission::{ProviderDeferral, ProviderRequestGuard};
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct InventoryItem {
    pub object: ObjectId,
    pub revision_id: String,
    pub effective_date: Option<String>,
    pub publication_date: Option<String>,
    pub title: String,
    pub data_source: Option<String>,
    pub case_number: Option<String>,
    pub treaty_class_code: Option<String>,
    /// Provider `제개정구분명` of this revision, such as `일부개정` or `타법폐지`.
    #[serde(default)]
    pub amendment_type: Option<String>,
}
impl InventoryItem {
    /// Validate an operator-supplied identity hint before it can queue a live
    /// detail request. Publication still requires exact provider response checks.
    pub fn validate_for_detail(&self) -> Result<(), DatabaseError> {
        self.object.validate()?;
        revision_parts(self)?;
        if self
            .effective_date
            .as_deref()
            .is_some_and(|d| !valid_date(d))
            || self
                .publication_date
                .as_deref()
                .is_some_and(|d| !valid_date(d))
            || self.title.len() > 512
            || self.data_source.as_deref().is_some_and(|s| s.len() > 512)
            || self.case_number.as_deref().is_some_and(|s| s.len() > 512)
            || self.amendment_type.as_deref().is_some_and(|s| s.len() > 64)
        {
            return Err(DatabaseError::InvalidInput);
        }
        Ok(())
    }
}
pub struct ProviderDetail {
    pub source_observations: Vec<crate::corpus::SourceObservationInput>,
    pub retrieved_at: u64,
    pub record: LegalRecord,
    pub raw: Vec<u8>,
    pub additional_evidence: Vec<Vec<u8>>,
    pub processor_version: String,
}
#[derive(Clone, Debug)]
pub struct InventoryPage {
    pub source_evidence: Option<InventoryEvidence>,
    pub items: Vec<InventoryItem>,
    pub done: bool,
    pub total: Option<u64>,
    pub rejected_rows: usize,
    pub incomplete: bool,
}

#[derive(Clone)]
pub struct InventoryEvidence {
    pub raw: Vec<u8>,
    pub retrieved_at: u64,
    pub credentials_redacted: bool,
}
impl std::fmt::Debug for InventoryEvidence {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InventoryEvidence")
            .field("bytes", &self.raw.len())
            .field("retrieved_at", &self.retrieved_at)
            .finish()
    }
}

enum FetchedDocument<T> {
    Processed(T),
    Original {
        raw: Vec<u8>,
        retrieved_at: u64,
    },
    UnexpectedAttachment {
        raw: Vec<u8>,
        html: bool,
        reason: &'static str,
    },
}

#[derive(Serialize)]
struct MissingAttachment {
    ordinal: usize,
    expected_format: DocumentFormat,
    response_sha256: Option<String>,
    reason: &'static str,
}
#[derive(Clone)]
pub struct LawClient {
    credential: Arc<String>,
    processor: Arc<dyn DocumentProcessor>,
    resolver: hickory_resolver::TokioResolver,
    admission: Arc<Semaphore>,
    next_request: Arc<Mutex<Option<Instant>>>,
    operator_suspended: Arc<AtomicBool>,
    clock: Arc<dyn openlegal_application::Clock>,
    budget: Option<(PgPool, RequestBudgetMode)>,
    local_cap: Option<Arc<AtomicU32>>,
    reservation_observer: Option<Arc<AtomicBool>>,
    explicit_owner: Option<(uuid::Uuid, u64)>,
    collection_events: Arc<tokio::sync::OnceCell<crate::collection_events::CollectionEvents>>,
    proxy: Option<crate::upstream_proxy::Socks5Proxy>,
    source_archive: Option<Arc<crate::corpus::PgCorpusStore>>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RequestBudgetMode {
    Pilot,
    Continuous,
    OnDemand,
}
/// Operator policy persisted once for every client using the same provider ledger.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProviderRequestLimits {
    pub continuous_daily_limit: RequestLimit,
    pub on_demand_daily_limit: RequestLimit,
    pub pilot_attempt_limit: RequestLimit,
    pub on_demand_attempt_limit: RequestLimit,
    pub interval_ms: u32,
    /// Global HTTP attempts permitted across all clients sharing this ledger.
    pub max_in_flight: u32,
    pub pilot_timeout_secs: u64,
    pub on_demand_timeout_secs: u64,
    /// Total processing executions, including the initial attempt.
    pub max_job_attempts: u32,
}
impl Default for ProviderRequestLimits {
    fn default() -> Self {
        Self {
            continuous_daily_limit: RequestLimit::Unlimited,
            on_demand_daily_limit: RequestLimit::Limited(1000),
            pilot_attempt_limit: RequestLimit::Limited(100),
            on_demand_attempt_limit: RequestLimit::Limited(32),
            interval_ms: 200,
            max_in_flight: 4,
            pilot_timeout_secs: 1800,
            on_demand_timeout_secs: 7200,
            max_job_attempts: 3,
        }
    }
}
impl ProviderRequestLimits {
    /// Compatibility constructor for the legacy whole-second spacing policy.
    pub fn new(
        continuous_daily_limit: u32,
        on_demand_daily_limit: u32,
        min_interval_secs: u32,
    ) -> Result<Self, DatabaseError> {
        if !(1..=3600).contains(&min_interval_secs) {
            return Err(DatabaseError::InvalidInput);
        }
        Self {
            continuous_daily_limit: RequestLimit::Limited(continuous_daily_limit),
            on_demand_daily_limit: RequestLimit::Limited(on_demand_daily_limit),
            pilot_attempt_limit: RequestLimit::Limited(100),
            on_demand_attempt_limit: RequestLimit::Limited(32),
            interval_ms: min_interval_secs * 1000 + 1,
            max_in_flight: 4,
            pilot_timeout_secs: 1800,
            on_demand_timeout_secs: 7200,
            max_job_attempts: 3,
        }
        .validated()
    }
    pub fn validated(self) -> Result<Self, DatabaseError> {
        for limit in [
            self.continuous_daily_limit,
            self.on_demand_daily_limit,
            self.pilot_attempt_limit,
            self.on_demand_attempt_limit,
        ] {
            limit.validate().map_err(|_| DatabaseError::InvalidInput)?;
        }
        if !(1..=3_600_001).contains(&self.interval_ms)
            || !(1..=16).contains(&self.max_in_flight)
            || !(60..=86400).contains(&self.pilot_timeout_secs)
            || !(60..=86400).contains(&self.on_demand_timeout_secs)
            || !(1..=10).contains(&self.max_job_attempts)
        {
            return Err(DatabaseError::InvalidInput);
        }
        Ok(self)
    }
}
impl LawClient {
    /// Change admission policy without resetting attempts or clearing provider evidence.
    /// Only exhausted-budget waits gain an earlier lease when their budget increases.
    pub async fn configure_provider_request_limits(
        pool: &PgPool,
        limits: &ProviderRequestLimits,
    ) -> Result<u64, DatabaseError> {
        let mut tx = pool
            .begin()
            .await
            .map_err(|_| DatabaseError::StorageUnavailable)?;
        // Match the corpus claim lock order; a newly claimed job clears budget_wait.
        sqlx::query("SELECT singleton FROM openlegal.corpus_control WHERE singleton FOR UPDATE")
            .fetch_one(&mut *tx)
            .await
            .map_err(|_| DatabaseError::StorageUnavailable)?;
        let row = sqlx::query("SELECT utc_day,daily_used,on_demand_used,continuous_daily_limit,on_demand_daily_limit,interval_ms,next_allowed_at,next_request_at_ms,operator_suspended,unresolved_response,floor(extract(epoch from clock_timestamp()))::bigint AS now FROM openlegal.provider_request_budget WHERE singleton FOR UPDATE")
            .fetch_one(&mut *tx).await.map_err(|_| DatabaseError::StorageUnavailable)?;
        let get = |name| {
            row.try_get::<i64, _>(name)
                .map_err(|_| DatabaseError::StorageUnavailable)
        };
        let get_count = |name| {
            row.try_get::<i64, _>(name)
                .map_err(|_| DatabaseError::StorageUnavailable)
        };
        let now = get("now")?;
        let current_day = get("utc_day")? == now / 86_400;
        let old_continuous: Option<i32> = row
            .try_get("continuous_daily_limit")
            .map_err(|_| DatabaseError::StorageUnavailable)?;
        let old_on_demand: Option<i32> = row
            .try_get("on_demand_daily_limit")
            .map_err(|_| DatabaseError::StorageUnavailable)?;
        let continuous_used = if current_day {
            get_count("daily_used")?
        } else {
            0
        };
        let on_demand_used = if current_day {
            get_count("on_demand_used")?
        } else {
            0
        };
        let continuous = limits
            .continuous_daily_limit
            .as_option()
            .map(|value| value as i32);
        let on_demand = limits
            .on_demand_daily_limit
            .as_option()
            .map(|value| value as i32);
        let newly_available = |old: Option<i32>, new: Option<i32>, used: i64| {
            old.is_some_and(|old| used >= i64::from(old))
                && new.is_none_or(|new| used < i64::from(new))
        };
        let wake_continuous = newly_available(old_continuous, continuous, continuous_used);
        let wake_on_demand = newly_available(old_on_demand, on_demand, on_demand_used);
        let paused = row
            .try_get::<bool, _>("operator_suspended")
            .map_err(|_| DatabaseError::StorageUnavailable)?
            || row
                .try_get::<bool, _>("unresolved_response")
                .map_err(|_| DatabaseError::StorageUnavailable)?
            || admission::uncertain_in_transaction(&mut tx).await?;
        let limits = limits.validated()?;
        sqlx::query("UPDATE openlegal.provider_request_budget SET continuous_daily_limit=$1,on_demand_daily_limit=$2,interval_ms=$3,pilot_attempt_limit=$4,on_demand_attempt_limit=$5,pilot_timeout_secs=$6,on_demand_timeout_secs=$7,max_job_attempts=$8,max_in_flight=$9 WHERE singleton")
            .bind(continuous).bind(on_demand).bind(limits.interval_ms as i32)
            .bind(limits.pilot_attempt_limit.as_option().map(|value| value as i32))
            .bind(limits.on_demand_attempt_limit.as_option().map(|value| value as i32))
            .bind(limits.pilot_timeout_secs as i64).bind(limits.on_demand_timeout_secs as i64)
            .bind(limits.max_job_attempts as i32).bind(limits.max_in_flight as i32)
            .execute(&mut *tx).await.map_err(|_| DatabaseError::StorageUnavailable)?;
        // Lowering the retry policy must not strand exhausted pending jobs.
        // Active claims retain their fences until completion or expiry.
        sqlx::query("UPDATE openlegal.corpus_job SET status='failed',error_category='attempts_exhausted',completed_at=$1::text::numeric WHERE status='pending' AND attempts >= $2")
            .bind(now.to_string()).bind(limits.max_job_attempts as i32)
            .execute(&mut *tx).await.map_err(|_| DatabaseError::StorageUnavailable)?;
        let mut woken = 0;
        if !paused && (wake_continuous || wake_on_demand) {
            let spacing = get("next_request_at_ms")?;
            let resume_at = now
                .saturating_add(1)
                .max(get("next_allowed_at")?)
                .max(spacing / 1000 + i64::from(spacing % 1000 != 0));
            woken = sqlx::query("UPDATE openlegal.corpus_job SET lease_until=$1::text::numeric WHERE status='running' AND error_category='budget_wait' AND lease_until>$1::text::numeric AND ((source_metadata->>'collection_origin'='explicit' AND $2) OR (source_metadata->>'collection_origin' IS DISTINCT FROM 'explicit' AND $3))")
                .bind(resume_at.to_string()).bind(wake_on_demand).bind(wake_continuous)
                .execute(&mut *tx).await.map_err(|_| DatabaseError::StorageUnavailable)?.rows_affected();
        }
        tx.commit()
            .await
            .map_err(|_| DatabaseError::StorageUnavailable)?;
        Ok(woken)
    }
    pub fn new(
        credential: String,
        processor: Arc<dyn DocumentProcessor>,
    ) -> Result<Self, DatabaseError> {
        if credential.is_empty()
            || credential.len() > 256
            || credential.chars().any(char::is_control)
        {
            return Err(DatabaseError::InvalidInput);
        }
        let mut builder = hickory_resolver::TokioResolver::builder_tokio()
            .map_err(|_| DatabaseError::StorageUnavailable)?;
        let options = builder.options_mut();
        options.timeout = Duration::from_secs(2);
        options.attempts = 1;
        options.num_concurrent_reqs = 1;
        options.max_active_requests = 2;
        options.cache_size = 16;
        options.use_hosts_file = hickory_resolver::config::ResolveHosts::Never;
        Ok(Self {
            credential: Arc::new(credential),
            processor,
            resolver: builder
                .build()
                .map_err(|_| DatabaseError::StorageUnavailable)?,
            admission: Arc::new(Semaphore::new(1)),
            clock: Arc::new(openlegal_application::SystemClock::default()),
            next_request: Arc::new(Mutex::new(Some(Instant::now()))),
            operator_suspended: Arc::new(AtomicBool::new(false)),
            budget: None,
            local_cap: None,
            reservation_observer: None,
            explicit_owner: None,
            collection_events: Arc::new(tokio::sync::OnceCell::new()),
            proxy: None,
            source_archive: None,
        })
    }
    /// Covers inventory, details, NTS HTML and linked attachments, including clones
    /// used by explicit collection. Provider admission and destination checks remain.
    pub fn with_socks5_proxy(mut self, proxy: crate::upstream_proxy::Socks5Proxy) -> Self {
        self.proxy = Some(proxy);
        self
    }
    /// Retain generic primary response evidence before any parser or legal
    /// identity projection. This never publishes an unverified legal record.
    pub fn with_source_archive(mut self, archive: Arc<crate::corpus::PgCorpusStore>) -> Self {
        self.source_archive = Some(archive);
        self
    }
    /// The database migration must be applied before an enabled client starts.
    /// Every outbound attempt reserves its allowance before DNS resolution.
    pub fn with_request_budget(mut self, pool: PgPool, mode: RequestBudgetMode) -> Self {
        // The shared database ledger owns the selected limit (1..=16). The
        // local ceiling bounds waiting fetches without serializing network I/O.
        self.admission = Arc::new(Semaphore::new(16));
        self.budget = Some((pool, mode));
        self
    }
    pub fn with_local_cap(mut self, attempts: u32) -> Self {
        self.local_cap = Some(Arc::new(AtomicU32::new(attempts)));
        self
    }
    /// Records actual attempt reservation for an operation's timeout handling.
    /// Ticket, semaphore, pacing and healthy contention waits leave it false.
    pub fn with_reservation_observer(mut self, observer: Arc<AtomicBool>) -> Self {
        self.reservation_observer = Some(observer);
        self
    }
    /// Fence a request Pod to the exact launch it loaded. Reusing the receipt
    /// UUID for a later launch never authorizes this earlier client's calls.
    pub fn with_collection_launch(
        mut self,
        launch: &crate::corpus::CollectionLaunch,
    ) -> Result<Self, DatabaseError> {
        let owner = uuid::Uuid::parse_str(&launch.id).map_err(|_| DatabaseError::InvalidInput)?;
        i64::try_from(launch.launched_at).map_err(|_| DatabaseError::InvalidInput)?;
        self.explicit_owner = Some((owner, launch.launched_at));
        Ok(self)
    }
    pub fn on_demand_client(&self) -> Result<Self, DatabaseError> {
        self.on_demand_client_with_limit(RequestLimit::Limited(32))
    }
    pub fn on_demand_client_with_limit(&self, limit: RequestLimit) -> Result<Self, DatabaseError> {
        limit.validate().map_err(|_| DatabaseError::InvalidInput)?;
        let (pool, _) = self.budget.as_ref().ok_or(DatabaseError::InvalidInput)?;
        let mut client = self
            .clone()
            .with_request_budget(pool.clone(), RequestBudgetMode::OnDemand);
        client.local_cap = limit
            .as_option()
            .map(|attempts| Arc::new(AtomicU32::new(attempts)));
        Ok(client)
    }
    /// Start one durable pilot window, or resume the original window after restart.
    /// Changing configured duration cannot extend an already started pilot.
    pub async fn begin_pilot(&self) -> Result<Duration, DatabaseError> {
        let (pool, mode) = self.budget.as_ref().ok_or(DatabaseError::InvalidInput)?;
        if *mode != RequestBudgetMode::Pilot {
            return Err(DatabaseError::InvalidInput);
        }
        let mut tx = pool
            .begin()
            .await
            .map_err(|_| DatabaseError::StorageUnavailable)?;
        let row = sqlx::query("SELECT pilot_started_at,pilot_duration_secs,pilot_timeout_secs,floor(extract(epoch from clock_timestamp()))::bigint AS now FROM openlegal.provider_request_budget WHERE singleton FOR UPDATE")
            .fetch_one(&mut *tx).await.map_err(|_| DatabaseError::StorageUnavailable)?;
        let now: i64 = row
            .try_get("now")
            .map_err(|_| DatabaseError::StorageUnavailable)?;
        let started: Option<i64> = row
            .try_get("pilot_started_at")
            .map_err(|_| DatabaseError::StorageUnavailable)?;
        let configured: i64 = row
            .try_get("pilot_timeout_secs")
            .map_err(|_| DatabaseError::StorageUnavailable)?;
        let duration: Option<i64> = row
            .try_get("pilot_duration_secs")
            .map_err(|_| DatabaseError::StorageUnavailable)?;
        let started = started.unwrap_or(now);
        let duration = duration.unwrap_or(configured);
        sqlx::query("UPDATE openlegal.provider_request_budget SET pilot_started_at=$1,pilot_duration_secs=$2 WHERE singleton")
            .bind(started).bind(duration).execute(&mut *tx).await.map_err(|_| DatabaseError::StorageUnavailable)?;
        tx.commit()
            .await
            .map_err(|_| DatabaseError::StorageUnavailable)?;
        let remaining = started.saturating_add(duration).saturating_sub(now).max(0);
        Ok(Duration::from_secs(remaining as u64))
    }
    #[cfg(test)]
    async fn reserve_request(
        &self,
        cancel: &CancellationToken,
    ) -> Result<Option<ProviderRequestGuard>, DatabaseError> {
        let ticket = self.foreground_ticket().await?;
        self.reserve_request_with_ticket(cancel, ticket.as_ref())
            .await
    }
    async fn foreground_ticket(
        &self,
    ) -> Result<Option<admission::ForegroundTicket>, DatabaseError> {
        match &self.budget {
            Some((pool, RequestBudgetMode::OnDemand)) => {
                admission::ForegroundTicket::open(pool).await.map(Some)
            }
            _ => Ok(None),
        }
    }
    async fn reserve_request_with_ticket(
        &self,
        cancel: &CancellationToken,
        ticket: Option<&admission::ForegroundTicket>,
    ) -> Result<Option<ProviderRequestGuard>, DatabaseError> {
        if cancel.is_cancelled() {
            return Err(DatabaseError::Cancelled);
        }
        if self.operator_suspended.load(Ordering::Acquire) {
            return Err(DatabaseError::BudgetExhausted);
        }
        if let Some((pool, mode)) = &self.budget {
            let guard = admission::reserve(
                pool,
                *mode,
                self.local_cap.as_ref(),
                ticket,
                &self.collection_events,
                self.reservation_observer.as_ref(),
                self.explicit_owner.as_ref(),
                cancel,
            )
            .await?;
            if let Some(observer) = &self.reservation_observer {
                observer.store(true, Ordering::Release);
            }
            return Ok(Some(guard));
        }
        if let Some(cap) = &self.local_cap {
            cap.fetch_update(Ordering::AcqRel, Ordering::Acquire, |left| {
                left.checked_sub(1)
            })
            .map_err(|_| DatabaseError::BudgetExhausted)?;
        }
        if let Some(observer) = &self.reservation_observer {
            observer.store(true, Ordering::Release);
        }
        Ok(None)
    }
    async fn pause_provider_requests(
        &self,
        delay: u64,
        guard: Option<&mut ProviderRequestGuard>,
    ) -> Result<(), DatabaseError> {
        let suspended = delay > 7 * 86_400;
        self.operator_suspended.store(true, Ordering::Release);
        *self
            .next_request
            .lock()
            .map_err(|_| DatabaseError::StorageUnavailable)? =
            Instant::now().checked_add(Duration::from_secs(delay.min(7 * 86_400)));
        if let Some(guard) = guard {
            guard.pause(delay).await?;
        }
        self.operator_suspended.store(suspended, Ordering::Release);
        Ok(())
    }
    /// The owner token comes from this response's reservation, even when parsing
    /// finishes after the HTTP guard has been released for another request.
    async fn suspend_owned_response(&self, owner: Option<uuid::Uuid>) -> Result<(), DatabaseError> {
        self.operator_suspended.store(true, Ordering::Release);
        if let Some((pool, _)) = &self.budget {
            admission::suspend_owned_response(pool, owner.ok_or(DatabaseError::Conflict)?).await?;
        }
        Ok(())
    }
    async fn settle_fetch<T>(
        &self,
        fetched: Result<T, DatabaseError>,
        mut guard: Option<&mut ProviderRequestGuard>,
    ) -> Result<T, DatabaseError> {
        match fetched {
            Ok(value) => {
                if let Some(guard) = &mut guard {
                    guard.complete().await?;
                }
                Ok(value)
            }
            Err(error @ (DatabaseError::SourceRejected | DatabaseError::SourceUnauthorized)) => {
                self.operator_suspended.store(true, Ordering::Release);
                if let Some(guard) = &mut guard {
                    guard.suspend().await?;
                }
                Err(error)
            }
            Err(error @ (DatabaseError::Cancelled | DatabaseError::StorageUnavailable)) => {
                Err(error)
            }
            Err(error) => {
                if !self.operator_suspended.load(Ordering::Acquire)
                    && let Some(guard) = &mut guard
                    && guard.is_active()
                {
                    // Retry-After may already have settled and released it.
                    guard.complete().await?;
                }
                Err(error)
            }
        }
    }
    /// Reuse the process's continuously drained collection listener.
    pub fn with_collection_events(
        mut self,
        events: crate::collection_events::CollectionEvents,
    ) -> Self {
        self.collection_events = Arc::new(tokio::sync::OnceCell::new_with(Some(events)));
        self
    }
    /// Read shared readiness without reserving an attempt or changing counters.
    pub async fn provider_idle(&self) -> Result<bool, DatabaseError> {
        let Some((pool, mode)) = &self.budget else {
            return Ok(self.admission.available_permits() > 0);
        };
        admission::idle(pool, *mode).await
    }
    /// Read one consistent provider snapshot without spending an attempt. The
    /// returned time is a recheck time; suspended/uncertain evidence still needs
    /// operator review and is never cleared by this observation.
    pub async fn admission_deferral(&self) -> Result<ProviderDeferral, DatabaseError> {
        let (pool, mode) = self.budget.as_ref().ok_or(DatabaseError::InvalidInput)?;
        admission::deferral_snapshot(
            pool,
            *mode,
            self.local_cap
                .as_ref()
                .is_some_and(|cap| cap.load(Ordering::Acquire) == 0),
            self.operator_suspended.load(Ordering::Acquire),
        )
        .await
    }
    /// A transient pause is durable for configured ingestion; the job worker
    /// uses this timestamp without burning another attempt while it waits.
    pub async fn next_admissible_epoch(&self) -> Result<u64, DatabaseError> {
        Ok(self.admission_deferral().await?.recheck_at)
    }
    /// Reserve a provider attempt without transmitting it. Exposed for the
    /// explicit PostgreSQL integration gate; callers must not split one
    /// outbound attempt into multiple reservations.
    pub async fn reserve_provider_request_budget(
        pool: &PgPool,
        mode: &RequestBudgetMode,
        cancel: &CancellationToken,
    ) -> Result<ProviderRequestGuard, DatabaseError> {
        let ticket = if *mode == RequestBudgetMode::OnDemand {
            Some(admission::ForegroundTicket::open(pool).await?)
        } else {
            None
        };
        let events = tokio::sync::OnceCell::new();
        admission::reserve(
            pool,
            *mode,
            None,
            ticket.as_ref(),
            &events,
            None,
            None,
            cancel,
        )
        .await
    }
    pub async fn inventory(
        &self,
        dataset: Dataset,
        page: u32,
        cancel: CancellationToken,
    ) -> Result<(Vec<InventoryItem>, bool), DatabaseError> {
        self.inventory_page(dataset, page, false, None, cancel)
            .await
            .map(|page| (page.items, page.done))
    }
    pub async fn historical_inventory(
        &self,
        dataset: Dataset,
        page: u32,
        object_id: Option<&str>,
        cancel: CancellationToken,
    ) -> Result<(Vec<InventoryItem>, bool), DatabaseError> {
        self.inventory_page(dataset, page, true, object_id, cancel)
            .await
            .map(|page| (page.items, page.done))
    }
    pub async fn inventory_page(
        &self,
        dataset: Dataset,
        page: u32,
        historical: bool,
        object_id: Option<&str>,
        cancel: CancellationToken,
    ) -> Result<InventoryPage, DatabaseError> {
        self.inventory_page_class(dataset, page, historical, object_id, None, cancel)
            .await
    }
    /// The provider's treaty list exposes one `trty` target with a documented
    /// bilateral/multilateral filter. It remains one stored dataset.
    pub async fn inventory_page_class(
        &self,
        dataset: Dataset,
        page: u32,
        historical: bool,
        object_id: Option<&str>,
        treaty_class: Option<u8>,
        cancel: CancellationToken,
    ) -> Result<InventoryPage, DatabaseError> {
        self.inventory_page_class_filtered(
            dataset,
            page,
            historical,
            object_id,
            treaty_class,
            None,
            cancel,
        )
        .await
    }
    pub async fn inventory_search_page_class(
        &self,
        dataset: Dataset,
        page: u32,
        term: &str,
        literal: bool,
        treaty_class: Option<u8>,
        cancel: CancellationToken,
    ) -> Result<InventoryPage, DatabaseError> {
        if term.is_empty()
            || term.len() > 128
            || term
                .chars()
                .any(|ch| !(ch.is_alphanumeric() || ch == ' ' || ch == '-'))
        {
            return Err(DatabaseError::InvalidInput);
        }
        self.inventory_page_class_filtered(
            dataset,
            page,
            false,
            None,
            treaty_class,
            Some((term, literal)),
            cancel,
        )
        .await
    }
    /// The precedent list documents `nb` as its case-number filter. Keep this
    /// separate from `query`, which searches the case title by default.
    pub async fn inventory_precedent_case_page(
        &self,
        case_number: &str,
        page: u32,
        cancel: CancellationToken,
    ) -> Result<InventoryPage, DatabaseError> {
        let url = self.precedent_case_search_url(case_number, page)?;
        let (parsed, _) = self
            .fetch_parse(url, DocumentFormat::Xml, false, cancel)
            .await?;
        let tree = parsed
            .tree
            .as_ref()
            .ok_or(DatabaseError::SourceDataInvalid)?;
        parse_inventory_tree(tree, Dataset::Precedent, page)
    }
    fn precedent_case_search_url(
        &self,
        case_number: &str,
        page: u32,
    ) -> Result<Url, DatabaseError> {
        if !(1..=3).contains(&page)
            || case_number.is_empty()
            || case_number.len() > 64
            || !case_number
                .chars()
                .all(|ch| ch.is_alphanumeric() || ch == '-')
        {
            return Err(DatabaseError::InvalidInput);
        }
        let mut url = self.api("lawSearch.do", "prec")?;
        url.query_pairs_mut()
            .append_pair("display", "100")
            .append_pair("page", &page.to_string())
            .append_pair("nb", case_number);
        Ok(url)
    }
    /// Construct only registered inventory routes. Does not issue a request.
    #[allow(clippy::too_many_arguments)]
    pub fn inventory_url(
        &self,
        dataset: Dataset,
        page: u32,
        historical: bool,
        object_id: Option<&str>,
        treaty_class: Option<u8>,
        search: Option<(&str, bool)>,
    ) -> Result<Url, DatabaseError> {
        if treaty_class.is_some_and(|c| dataset != Dataset::Treaty || !matches!(c, 1 | 2)) {
            return Err(DatabaseError::InvalidInput);
        }
        if search.is_some_and(|(term, _)| {
            term.is_empty()
                || term.len() > 128
                || term
                    .chars()
                    .any(|ch| !(ch.is_alphanumeric() || ch == ' ' || ch == '-'))
        }) {
            return Err(DatabaseError::InvalidInput);
        }
        if page == 0
            || page > 1_000_000
            || object_id.is_some_and(|id| !openlegal_domain::valid_identifier(id, 128))
        {
            return Err(DatabaseError::InvalidInput);
        }
        if historical
            && !matches!(
                catalog::source_family(dataset).history_mode,
                catalog::HistoryMode::StatuteEffective | catalog::HistoryMode::CurrentHistory
            )
        {
            return Err(DatabaseError::UnsupportedHistory);
        }
        if object_id.is_some() && dataset != Dataset::NationalStatute {
            return Err(DatabaseError::UnsupportedHistory);
        }
        let target = target(dataset);
        let mut url = self.api("lawSearch.do", target)?;
        url.query_pairs_mut()
            .append_pair("display", "100")
            .append_pair("page", &page.to_string());
        if let Some((term, literal)) = search {
            let query = if literal {
                format!("\"{term}\"")
            } else {
                term.to_owned()
            };
            url.query_pairs_mut().append_pair("query", &query);
        }
        if let Some(class) = treaty_class {
            url.query_pairs_mut().append_pair("cls", &class.to_string());
        }
        match dataset {
            Dataset::NationalStatute => {
                url.query_pairs_mut()
                    .append_pair("nw", if historical { "1,3" } else { "3" });
                if let Some(id) = object_id {
                    url.query_pairs_mut().append_pair("LID", id);
                }
            }
            Dataset::Ordinance
            | Dataset::AdministrativeRule
            | Dataset::SchoolRule
            | Dataset::LocalPublicCorporationRule
            | Dataset::PublicInstitutionRule => {
                url.query_pairs_mut()
                    .append_pair("nw", if historical { "2" } else { "1" });
            }
            _ => {}
        }
        Ok(url)
    }
    #[allow(clippy::too_many_arguments)]
    async fn inventory_page_class_filtered(
        &self,
        dataset: Dataset,
        page: u32,
        historical: bool,
        object_id: Option<&str>,
        treaty_class: Option<u8>,
        search: Option<(&str, bool)>,
        cancel: CancellationToken,
    ) -> Result<InventoryPage, DatabaseError> {
        let url = self.inventory_url(dataset, page, historical, object_id, treaty_class, search)?;
        let (parsed, raw, retrieved_at) = self
            .fetch_parse_timed(url, DocumentFormat::Xml, false, cancel)
            .await?;
        let tree = parsed
            .tree
            .as_ref()
            .ok_or(DatabaseError::SourceDataInvalid)?;
        let mut result = parse_inventory_tree(tree, dataset, page).inspect_err(|_| {
            eprintln!("law provider: response rejected at inventory_projection");
        })?;
        result.source_evidence = Some(InventoryEvidence {
            credentials_redacted: has_credential_redaction(&raw),
            raw,
            retrieved_at,
        });
        Ok(result)
    }
    /// Construct a verified detail route. No URI is accepted from the record.
    pub fn detail_url(&self, item: &InventoryItem) -> Result<Url, DatabaseError> {
        item.object.validate()?;
        let (master, effective) = revision_parts(item)?;
        let target = target(item.object.dataset);
        let mut url = self.api("lawService.do", target)?;
        let family = catalog::source_family(item.object.dataset);
        if family.detail_mode == catalog::DetailMode::ListOnly {
            return Err(DatabaseError::SourceUnavailable);
        }
        let value = if family.detail_mode == catalog::DetailMode::TermName {
            if item.title.is_empty()
                || item.title.len() > 512
                || item.title.chars().any(char::is_control)
            {
                return Err(DatabaseError::InvalidInput);
            }
            item.title.as_str()
        } else {
            master.as_str()
        };
        let parameter = if item.object.dataset == Dataset::Ordinance {
            "MST"
        } else {
            family.detail_parameter()
        };
        url.query_pairs_mut().append_pair(parameter, value);
        if let Some(date) = effective {
            url.query_pairs_mut()
                .append_pair("efYd", &date)
                .append_pair("chrClsCd", "010201");
        }
        if item.object.dataset == Dataset::Treaty {
            url.query_pairs_mut().append_pair("chrClsCd", "010202");
        }
        let html = precedent_html(item);
        if html {
            let pairs: Vec<(String, String)> = url
                .query_pairs()
                .filter(|(k, _)| k != "type")
                .map(|(k, v)| (k.into_owned(), v.into_owned()))
                .collect();
            url.set_query(None);
            url.query_pairs_mut()
                .extend_pairs(pairs)
                .append_pair("type", "HTML");
        }
        Ok(url)
    }
    pub async fn detail(
        &self,
        item: &InventoryItem,
        cancel: CancellationToken,
    ) -> Result<ProviderDetail, DatabaseError> {
        let family = catalog::source_family(item.object.dataset);
        if family.metadata_only || family.detail_mode == catalog::DetailMode::ListOnly {
            // Unknown rights never authorize a detail/attachment download. This
            // capture is an inventory observation, not a retained provider body.
            return metadata_detail(item, Vec::new(), self.clock.now(), false);
        }
        if item.object.dataset == Dataset::EnglishStatute {
            let url = self.detail_url(item)?;
            let (raw, retrieved_at) = self
                .fetch_original(url, DocumentFormat::Xml, None, cancel)
                .await?;
            let mut detail = metadata_detail(item, Vec::new(), retrieved_at, false)?;
            let mut metadata = BTreeMap::new();
            metadata.insert("status".into(), "response_identity_unverified".into());
            metadata.insert("requested_revision".into(), item.revision_id.clone());
            if has_credential_redaction(&raw) {
                metadata.insert("credentials_redacted".into(), "true".into());
                detail
                    .record
                    .metadata
                    .insert("transport_credentials_redacted".into(), "true".into());
            }
            // The runtime archive hook already retained this transport response.
            if self.source_archive.is_none() {
                detail
                    .source_observations
                    .push(crate::corpus::SourceObservationInput {
                        source_key: format!(
                            "law_go_kr:{}:{}",
                            family
                                .detail_guide
                                .ok_or(DatabaseError::SourceDataInvalid)?,
                            item.revision_id
                        ),
                        raw: Some(raw),
                        media_type: "application/xml".into(),
                        rights: SourceRights::legal_information(),
                        metadata,
                        observed_at: retrieved_at,
                    });
            }
            detail.record.metadata.insert(
                "body_status".into(),
                "response_identity_unverified_metadata_only".into(),
            );
            return Ok(detail);
        }
        let url = self.detail_url(item)?;
        let html = precedent_html(item);
        let (mut record, links, raw, retrieved_at, processor_version) = self
            .fetch_parse_timed_checked(
                url,
                if html {
                    DocumentFormat::Html
                } else {
                    DocumentFormat::Xml
                },
                false,
                cancel.clone(),
                |output, raw, retrieved_at| {
                    let mut record = project(item, &output)?;
                    if item.object.dataset == Dataset::NationalStatute {
                        let provisions = provision_numbers_checked(
                            output.tree.as_ref().ok_or(DatabaseError::StorageCorrupt)?,
                        )?;
                        record.metadata.insert(
                            "provider_provisions_json".into(),
                            serde_json::to_string(&provisions)
                                .map_err(|_| DatabaseError::StorageCorrupt)?,
                        );
                    }
                    if output
                        .tree
                        .as_ref()
                        .is_some_and(contains_supplementary_subtree)
                    {
                        record.metadata.insert(
                            "supplementary_projection".into(),
                            "separate_rights_required".into(),
                        );
                    }
                    record
                        .validate()
                        .map_err(|_| DatabaseError::StorageCorrupt)?;
                    let links = attachment_links(
                        output.tree.as_ref().ok_or(DatabaseError::StorageCorrupt)?,
                    )?;
                    Ok((record, links, raw, retrieved_at, output.processor_version))
                },
            )
            .await?;
        let mut resources = vec![OriginalResource {
            ordinal: 0,
            title: record.title.clone(),
            media_type: if html { "text/html" } else { "application/xml" }.into(),
            source_url: record.source_url.clone(),
            retained: !record.metadata.contains_key("supplementary_projection"),
            rights: SourceRights::legal_information(),
        }];
        let mut additional_evidence = Vec::new();
        let mut evidence_ordinals = Vec::new();
        let mut total = raw.len();
        let mut extracted = 0usize;
        let expected_count = links.len();
        let mut missing: Vec<MissingAttachment> = Vec::new();
        for (ordinal, link) in links.into_iter().enumerate() {
            let resource_index = resources.len();
            resources.push(OriginalResource {
                ordinal: (ordinal + 1) as u32,
                title: link.title.clone(),
                media_type: media_type(link.format).into(),
                source_url: link.url.as_str().into(),
                retained: false,
                rights: link.rights.clone(),
            });
            if !link.rights.can_store() {
                continue;
            }
            if !link.rights.can_process() {
                let (bytes, _) = match self
                    .fetch_original(
                        link.url.clone(),
                        link.format,
                        Some((100usize * 1024 * 1024).saturating_sub(total)),
                        cancel.clone(),
                    )
                    .await
                {
                    Ok(value) => value,
                    Err(
                        DatabaseError::SourceDataInvalid
                        | DatabaseError::SourceUnavailable
                        | DatabaseError::SourceDownloadFailed,
                    ) => {
                        missing.push(MissingAttachment {
                            ordinal: ordinal + 1,
                            expected_format: link.format,
                            response_sha256: None,
                            reason: "original_download_invalid",
                        });
                        continue;
                    }
                    Err(error) => return Err(error),
                };
                total = total
                    .checked_add(bytes.len())
                    .ok_or(DatabaseError::SourceDataInvalid)?;
                if total > 100 * 1024 * 1024 {
                    return Err(DatabaseError::SourceDataInvalid);
                }
                resources[resource_index].retained =
                    !missing.iter().any(|failure| failure.ordinal == ordinal + 1);
                if has_credential_redaction(&bytes) {
                    record
                        .metadata
                        .insert("transport_credentials_redacted".into(), "true".into());
                }
                additional_evidence.push(bytes);
                evidence_ordinals.push(ordinal + 1);
                continue;
            }
            let section_checkpoint = record.sections.len();
            let total_checkpoint = total;
            let extracted_checkpoint = extracted;
            let mut retries = 0;
            let bytes = loop {
                let result = self
                    .fetch_attachment_timed_checked(
                        link.url.clone(),
                        link.format,
                        (100usize * 1024 * 1024).saturating_sub(total),
                        cancel.clone(),
                        |attachment, bytes, _| {
                            total = total
                                .checked_add(bytes.len())
                                .ok_or(DatabaseError::SourceDataInvalid)?;
                            if total > 100 * 1024 * 1024 {
                                return Err(DatabaseError::SourceDataInvalid);
                            }
                            let digest = attachment.source_sha256.clone();
                            let pages = if attachment.pages.is_empty() {
                                vec![openlegal_application::document::DocumentPage {
                                    page: 1,
                                    text: attachment.text,
                                }]
                            } else {
                                attachment.pages
                            };
                            for (kind, pages) in [
                                (SectionKind::Extracted, pages),
                                (SectionKind::Ocr, attachment.ocr_pages),
                            ] {
                                for page in pages {
                                    extracted = extracted
                                        .checked_add(page.text.len())
                                        .ok_or(DatabaseError::SourceDataInvalid)?;
                                    if extracted > 16 * 1024 * 1024 {
                                        return Err(DatabaseError::SourceDataInvalid);
                                    }
                                    let label = if kind == SectionKind::Ocr {
                                        "ocr"
                                    } else {
                                        "extracted"
                                    };
                                    record.sections.push(LegalSection {
                                        id: format!(
                                            "attachment:{}:{label}:{}",
                                            ordinal + 1,
                                            page.page
                                        ),
                                        title: link.title.clone(),
                                        text: page.text,
                                        kind: kind.clone(),
                                        source_document_sha256: Some(digest.clone()),
                                        page: Some(
                                            page.page
                                                .try_into()
                                                .map_err(|_| DatabaseError::SourceDataInvalid)?,
                                        ),
                                    });
                                }
                            }
                            record
                                .validate()
                                .map_err(|_| DatabaseError::SourceDataInvalid)?;
                            Ok(bytes)
                        },
                    )
                    .await;
                let result = match result {
                    Ok(result) => result,
                    Err(
                        error @ (DatabaseError::SourceUnavailable
                        | DatabaseError::SourceDownloadFailed),
                    ) => {
                        record.sections.truncate(section_checkpoint);
                        total = total_checkpoint;
                        extracted = extracted_checkpoint;
                        missing.push(MissingAttachment {
                            ordinal: ordinal + 1,
                            expected_format: link.format,
                            response_sha256: None,
                            reason: if error == DatabaseError::SourceUnavailable {
                                "source_unavailable"
                            } else {
                                "download_failed"
                            },
                        });
                        break Vec::new();
                    }
                    Err(DatabaseError::SourceDataInvalid) => {
                        record.sections.truncate(section_checkpoint);
                        total = total_checkpoint;
                        extracted = extracted_checkpoint;
                        missing.push(MissingAttachment {
                            ordinal: ordinal + 1,
                            expected_format: link.format,
                            response_sha256: None,
                            reason: "download_invalid",
                        });
                        break Vec::new();
                    }
                    Err(error) => return Err(error),
                };
                match result {
                    FetchedDocument::Processed(bytes) => break bytes,
                    FetchedDocument::Original { .. } => return Err(DatabaseError::StorageCorrupt),
                    FetchedDocument::UnexpectedAttachment {
                        raw, html: true, ..
                    } if retries < 2 => {
                        retries += 1;
                        // Each attempt passes through the same durable request
                        // admission and spacing policy as the first download.
                        drop(raw);
                    }
                    FetchedDocument::UnexpectedAttachment { raw, reason, .. } => {
                        record.sections.truncate(section_checkpoint);
                        total = total_checkpoint;
                        extracted = extracted_checkpoint;
                        total = total
                            .checked_add(raw.len())
                            .ok_or(DatabaseError::SourceDataInvalid)?;
                        if total > 100 * 1024 * 1024 {
                            return Err(DatabaseError::SourceDataInvalid);
                        }
                        missing.push(MissingAttachment {
                            ordinal: ordinal + 1,
                            expected_format: link.format,
                            response_sha256: Some(
                                Sha256::digest(&raw)
                                    .iter()
                                    .map(|b| format!("{b:02x}"))
                                    .collect(),
                            ),
                            reason,
                        });
                        break raw;
                    }
                }
            };
            if !bytes.is_empty() {
                resources[resource_index].retained =
                    !missing.iter().any(|failure| failure.ordinal == ordinal + 1);
                if has_credential_redaction(&bytes) {
                    record
                        .metadata
                        .insert("transport_credentials_redacted".into(), "true".into());
                }
                additional_evidence.push(bytes);
                evidence_ordinals.push(ordinal + 1);
            }
        }
        if !missing.is_empty() {
            record
                .metadata
                .insert("attachment_status".into(), "incomplete".into());
            record.metadata.insert(
                "attachment_expected_count".into(),
                expected_count.to_string(),
            );
            record.metadata.insert(
                "attachment_available_count".into(),
                (expected_count - missing.len()).to_string(),
            );
            record.metadata.insert(
                "attachment_failures".into(),
                serde_json::to_string(&missing).map_err(|_| DatabaseError::StorageCorrupt)?,
            );
            record.metadata.insert(
                "attachment_evidence_ordinals".into(),
                serde_json::to_string(&evidence_ordinals)
                    .map_err(|_| DatabaseError::StorageCorrupt)?,
            );
        }
        record.metadata.insert(
            "attachment_evidence_ordinals".into(),
            serde_json::to_string(&evidence_ordinals).map_err(|_| DatabaseError::StorageCorrupt)?,
        );
        record.metadata.insert(
            "original_resources".into(),
            serde_json::to_string(&resources).map_err(|_| DatabaseError::StorageCorrupt)?,
        );
        let warnings = openlegal_domain::rights::warnings(&record.metadata);
        record.metadata.insert(
            "rights_warnings".into(),
            serde_json::to_string(&warnings).map_err(|_| DatabaseError::StorageCorrupt)?,
        );
        record
            .validate()
            .map_err(|_| DatabaseError::SourceDataInvalid)?;
        Ok(ProviderDetail {
            source_observations: Vec::new(),
            retrieved_at,
            record,
            raw,
            additional_evidence,
            processor_version,
        })
    }
    fn api(&self, path: &str, target: &str) -> Result<Url, DatabaseError> {
        let mut url = Url::parse(&format!("https://www.law.go.kr/DRF/{path}"))
            .map_err(|_| DatabaseError::InvalidInput)?;
        url.query_pairs_mut()
            .append_pair("OC", &self.credential)
            .append_pair("target", target)
            .append_pair("type", "XML");
        Ok(url)
    }
    pub async fn fetch_parse(
        &self,
        url: Url,
        format: DocumentFormat,
        ocr: bool,
        cancel: CancellationToken,
    ) -> Result<(DocumentOutput, Vec<u8>), DatabaseError> {
        self.fetch_parse_timed(url, format, ocr, cancel)
            .await
            .map(|(output, raw, _)| (output, raw))
    }
    async fn fetch_parse_timed(
        &self,
        url: Url,
        format: DocumentFormat,
        ocr: bool,
        cancel: CancellationToken,
    ) -> Result<(DocumentOutput, Vec<u8>, u64), DatabaseError> {
        self.fetch_parse_timed_checked(url, format, ocr, cancel, |output, raw, retrieved_at| {
            Ok((output, raw, retrieved_at))
        })
        .await
    }
    /// Finalize HTTP response evidence and release admission before running the
    /// document processor and source-dependent checks. Late rejection retains
    /// the response owner token and cannot clear another HTTP attempt's marker.
    /// Fetch unmodified bytes through the same durable admission and bounded
    /// transport without invoking extraction, OCR or document processing.
    pub async fn fetch_original(
        &self,
        url: Url,
        format: DocumentFormat,
        remaining_bytes: Option<usize>,
        cancel: CancellationToken,
    ) -> Result<(Vec<u8>, u64), DatabaseError> {
        match self
            .fetch_parse_timed_checked_inner(
                url,
                format,
                false,
                cancel,
                remaining_bytes,
                false,
                |_, _, _| Ok(()),
            )
            .await?
        {
            FetchedDocument::Original { raw, retrieved_at } => Ok((raw, retrieved_at)),
            _ => Err(DatabaseError::StorageCorrupt),
        }
    }
    async fn fetch_parse_timed_checked<T, F>(
        &self,
        url: Url,
        format: DocumentFormat,
        ocr: bool,
        cancel: CancellationToken,
        check: F,
    ) -> Result<T, DatabaseError>
    where
        F: FnOnce(DocumentOutput, Vec<u8>, u64) -> Result<T, DatabaseError>,
    {
        match self
            .fetch_parse_timed_checked_inner(url, format, ocr, cancel, None, true, check)
            .await?
        {
            FetchedDocument::Processed(value) => Ok(value),
            FetchedDocument::UnexpectedAttachment { .. } => Err(DatabaseError::StorageCorrupt),
            FetchedDocument::Original { .. } => Err(DatabaseError::StorageCorrupt),
        }
    }
    async fn fetch_attachment_timed_checked<T, F>(
        &self,
        url: Url,
        format: DocumentFormat,
        remaining_bytes: usize,
        cancel: CancellationToken,
        check: F,
    ) -> Result<FetchedDocument<T>, DatabaseError>
    where
        F: FnOnce(DocumentOutput, Vec<u8>, u64) -> Result<T, DatabaseError>,
    {
        self.fetch_parse_timed_checked_inner(
            url,
            format,
            true,
            cancel,
            Some(remaining_bytes),
            true,
            check,
        )
        .await
    }
    #[allow(clippy::too_many_arguments)]
    async fn fetch_parse_timed_checked_inner<T, F>(
        &self,
        url: Url,
        format: DocumentFormat,
        ocr: bool,
        cancel: CancellationToken,
        attachment_remaining_bytes: Option<usize>,
        process: bool,
        check: F,
    ) -> Result<FetchedDocument<T>, DatabaseError>
    where
        F: FnOnce(DocumentOutput, Vec<u8>, u64) -> Result<T, DatabaseError>,
    {
        if url.scheme() != "https"
            || url.port_or_known_default() != Some(443)
            || !matches!(url.host_str(), Some("www.law.go.kr" | "law.go.kr"))
            || !url.username().is_empty()
            || url.password().is_some()
            || url.fragment().is_some()
            || !matches!(
                url.path(),
                "/DRF/lawService.do" | "/DRF/lawSearch.do" | "/LSW/flDownload.do"
            )
        {
            return Err(DatabaseError::InvalidInput);
        }
        if literal_ip(&url).is_some() {
            return Err(DatabaseError::InvalidInput);
        }
        if cancel.is_cancelled() {
            return Err(DatabaseError::Cancelled);
        }
        let primary_archive_url = self.source_archive.as_ref().map(|_| url.clone());
        let ticket = self.foreground_ticket().await?;
        let ticket_failure = ticket
            .as_ref()
            .map(|ticket| ticket.failed().clone())
            .unwrap_or_default();
        let permit = tokio::select! {
            _ = cancel.cancelled() => return Err(DatabaseError::Cancelled),
            _ = ticket_failure.cancelled() => return Err(DatabaseError::StorageUnavailable),
            permit = self.admission.clone().acquire_owned() => permit.map_err(|_|DatabaseError::Capacity)?,
        };
        let next = (*self
            .next_request
            .lock()
            .map_err(|_| DatabaseError::StorageUnavailable)?)
        .ok_or(DatabaseError::Capacity)?;
        if next.saturating_duration_since(Instant::now()) > Duration::from_secs(30) {
            return Err(if self.budget.is_some() {
                DatabaseError::BudgetExhausted
            } else {
                DatabaseError::Capacity
            });
        }
        if next > Instant::now() {
            tokio::select! {_=cancel.cancelled()=>return Err(DatabaseError::Cancelled),_=tokio::time::sleep_until(next)=>{}}
        }
        *self
            .next_request
            .lock()
            .map_err(|_| DatabaseError::StorageUnavailable)? =
            Instant::now().checked_add(Duration::from_secs(u64::from(self.budget.is_none())));
        let mut guard = self
            .reserve_request_with_ticket(&cancel, ticket.as_ref())
            .await?;
        let response_owner = guard.as_ref().map(|guard| guard.owner());
        drop(ticket);
        let fetched = async {
        let host = url.host_str().ok_or(DatabaseError::InvalidInput)?;
        let ips = tokio::select! {_=cancel.cancelled()=>return Err(DatabaseError::Cancelled),r=self.resolver.lookup_ip(format!("{host}."))=>r.map_err(|_|download_failed("dns"))?};
        let mut addresses = Vec::new();
        for ip in ips.iter() {
            if !public_address(ip) || addresses.len() >= 16 {
                return Err(DatabaseError::InvalidInput);
            }
            addresses.push(SocketAddr::new(ip, 443));
        }
        if addresses.is_empty() {
            return Err(download_failed("dns_empty"));
        }
        let builder = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(30))
            .resolve_to_addrs(host, &addresses);
        let builder = match &self.proxy {
            Some(proxy) => proxy.apply(builder),
            None => builder,
        };
        let client = builder
            .build()
            .map_err(|_| DatabaseError::StorageUnavailable)?;
        let fetch = async {
            let mut response = client
                .get(url)
                .header("accept-encoding", "identity")
                .send()
                .await
                .map_err(|error| {
                    download_failed(if error.is_timeout() {
                        "request_timeout"
                    } else if error.is_connect() {
                        "connect_tls"
                    } else {
                        "response"
                    })
                })?;
            if matches!(
                response.status(),
                reqwest::StatusCode::TOO_MANY_REQUESTS | reqwest::StatusCode::SERVICE_UNAVAILABLE
            ) {
                let delay = response
                    .headers()
                    .get("retry-after")
                    .and_then(|v| v.to_str().ok())
                    .and_then(crate::retry_after)
                    .unwrap_or(60)
                    .max(1);
                self.pause_provider_requests(delay, guard.as_mut()).await?;
                return Err(if self.budget.is_some() {
                    DatabaseError::BudgetExhausted
                } else {
                    DatabaseError::Capacity
                });
            }
            if let Some(error) = http_status_error(response.status()) {
                return Err(error);
            }
            if response
                .headers()
                .get("content-encoding")
                .is_some_and(|v| v != "identity")
            {
                return Err(DatabaseError::SourceDataInvalid);
            }
            let html_content_type = response
                .headers()
                .get(reqwest::header::CONTENT_TYPE)
                .and_then(|value| value.to_str().ok())
                .is_some_and(|value| value.to_ascii_lowercase().starts_with("text/html"));
            let max = if matches!(format, DocumentFormat::Xml | DocumentFormat::Html)
                || (attachment_remaining_bytes.is_some() && html_content_type)
            {
                16 * 1024 * 1024
            } else {
                100 * 1024 * 1024
            };
            let max = max.min(attachment_remaining_bytes.unwrap_or(usize::MAX));
            if response.content_length().is_some_and(|n| n > max as u64) {
                return Err(DatabaseError::SourceDataInvalid);
            }
            let mut raw = Vec::new();
            while let Some(chunk) = response
                .chunk()
                .await
                .map_err(|error| {
                    download_failed(if error.is_timeout() {
                        "body_timeout"
                    } else {
                        "body"
                    })
                })?
            {
                if raw.len().saturating_add(chunk.len()) > max {
                    return Err(DatabaseError::SourceDataInvalid);
                }
                raw.extend_from_slice(&chunk);
            }
            Ok((raw, html_content_type))
        };
        tokio::select! {_ = cancel.cancelled()=>Err(DatabaseError::Cancelled),result=fetch=>result}
        }.await;
        let settled = self.settle_fetch(fetched, guard.as_mut()).await;
        drop(guard);
        drop(permit);
        let (raw, html_content_type) = settled?;
        let retrieved_at = self.clock.now();
        // API errors commonly use HTTP 200. They never become original legal
        // evidence, and reflected credentials never enter the permanent archive.
        if let Some(error) = provider_response_error(&raw) {
            if error == DatabaseError::SourceUnauthorized {
                self.suspend_owned_response(response_owner).await?;
            }
            return Err(error);
        }
        // Normal list links may repeat OC. Remove only credential material;
        // archived/processed hashes describe these explicitly marked bytes.
        let actual_format = if html_content_type || looks_like_html(&raw) {
            DocumentFormat::Html
        } else {
            format
        };
        let (raw, credentials_redacted) =
            redact_transport_credentials(raw, &self.credential, actual_format).inspect_err(
                |_| {
                    // Static stage only: never log provider bodies, URLs or secrets.
                    eprintln!("law provider: response withheld at credential_redaction");
                },
            )?;
        if let (Some(archive), Some(url)) = (&self.source_archive, primary_archive_url)
            && attachment_remaining_bytes.is_none()
            && let Some(observation) = primary_source_observation(
                &url,
                if html_content_type {
                    DocumentFormat::Html
                } else {
                    format
                },
                &raw,
                retrieved_at,
            )
        {
            archive
                .retain_source_observation(observation, cancel.clone())
                .await?;
        }
        if !process {
            if attachment_remaining_bytes.is_some() && !expected_document_magic(&raw, format) {
                return Err(DatabaseError::SourceDataInvalid);
            }
            return Ok(FetchedDocument::Original { raw, retrieved_at });
        }
        if attachment_remaining_bytes.is_some() && !expected_document_magic(&raw, format) {
            return Ok(FetchedDocument::UnexpectedAttachment {
                html: html_content_type || looks_like_html(&raw),
                raw,
                reason: "unexpected_attachment_format",
            });
        }
        let digest = Sha256::digest(&raw)
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
        let processed = self
            .processor
            .process(
                DocumentInput {
                    format,
                    raw: raw.clone(),
                    source_sha256: digest,
                    ocr,
                },
                cancel,
            )
            .await
            .inspect_err(|_| {
                eprintln!("law provider: response rejected at document_processor");
            })
            .map_err(document_error);
        let output = processed
            .and_then(|mut output| {
                if credentials_redacted {
                    output.diagnostics.truncate(63);
                    output
                        .diagnostics
                        .push("provider_credential_redacted".into());
                }
                check(output, raw.clone(), retrieved_at).map_err(|error| {
                    // Only bounded error categories identify the rejection stage;
                    // legal content, source URLs and credentials stay private.
                    eprintln!("law provider: response rejected at response_validation ({error:?})");
                    if error == DatabaseError::StorageCorrupt {
                        DatabaseError::SourceDataInvalid
                    } else {
                        error
                    }
                })
            })
            .map(FetchedDocument::Processed);
        match &output {
            Err(DatabaseError::SourceDataInvalid) if attachment_remaining_bytes.is_some() => {
                return Ok(FetchedDocument::UnexpectedAttachment {
                    raw,
                    html: false,
                    reason: "document_invalid",
                });
            }
            Err(DatabaseError::SourceRejected | DatabaseError::SourceUnauthorized) => {
                self.suspend_owned_response(response_owner).await?;
                return output;
            }
            Err(DatabaseError::Cancelled) => return Err(DatabaseError::Cancelled),
            _ => {}
        }
        output
    }
}
fn parse_inventory_tree(
    tree: &DocumentNode,
    dataset: Dataset,
    page: u32,
) -> Result<InventoryPage, DatabaseError> {
    let family = catalog::source_family(dataset);
    let mut nodes = Vec::new();
    for name in family.row_tags {
        elements(tree, name, &mut nodes);
        if !nodes.is_empty() {
            break;
        }
    }
    if nodes.is_empty() {
        identity_rows(tree, family.id_fields, &mut nodes);
    }
    let observed_rows = nodes.len();
    let mut items = Vec::new();
    let mut rejected_rows = 0usize;
    for node in nodes {
        let parsed = (|| -> Result<Option<InventoryItem>, DatabaseError> {
            let id = first_of(node, family.id_fields).ok_or(DatabaseError::StorageCorrupt)?;
            let master =
                first_of(node, family.revision_fields).ok_or(DatabaseError::StorageCorrupt)?;
            if dataset == Dataset::AdministrativeAppeal && (master == "0" || id == "0") {
                // This list includes placeholder rows without a usable detail ID.
                return Ok(None);
            }
            if !numeric_id(&master) || !numeric_id(&id) || master == "0" || id == "0" {
                return Err(DatabaseError::StorageCorrupt);
            }
            let title = first_of(node, family.title_fields).unwrap_or_default();
            let object = ObjectId {
                jurisdiction: "kr".into(),
                provider: "law_go_kr".into(),
                dataset,
                id,
            };
            object.validate()?;
            let effective_date = match dataset {
                Dataset::NationalStatute
                | Dataset::AdministrativeRule
                | Dataset::Ordinance
                | Dataset::EnglishStatute
                | Dataset::SchoolRule
                | Dataset::LocalPublicCorporationRule
                | Dataset::PublicInstitutionRule => date(first(node, "시행일자"))?,
                Dataset::Treaty => date(first(node, "발효일자"))?,
                _ => None,
            };
            let revision_id = if dataset == Dataset::NationalStatute {
                format!(
                    "{master}:{}",
                    effective_date
                        .as_deref()
                        .ok_or(DatabaseError::StorageCorrupt)?
                )
            } else {
                master
            };
            Ok(Some(InventoryItem {
                publication_date: if matches!(
                    dataset,
                    Dataset::NationalStatute | Dataset::Ordinance | Dataset::EnglishStatute
                ) {
                    date(first(node, "공포일자"))?
                } else {
                    None
                },
                object,
                revision_id,
                effective_date,
                title,
                data_source: first(node, "데이터출처명"),
                case_number: first(node, "사건번호"),
                treaty_class_code: if dataset == Dataset::Treaty {
                    first(node, "조약구분코드")
                } else {
                    None
                },
                amendment_type: if dataset.has_provider_revisions() {
                    first(node, "제개정구분명").filter(|v| !v.is_empty() && v.len() <= 64)
                } else {
                    None
                },
            }))
        })();
        match parsed {
            Ok(Some(item)) => items.push(item),
            Ok(None) => {}
            Err(_) => rejected_rows += 1,
        }
    }
    let total = first(tree, "totalCnt").and_then(|v| v.parse::<u64>().ok());
    let incomplete = rejected_rows > 0
        || total.is_none()
        || observed_rows > 100
        || total.is_some_and(|total| {
            total < u64::from(page.saturating_sub(1)) * 100 + observed_rows as u64
        })
        || total.is_some_and(|total| {
            dataset != Dataset::AdministrativeAppeal
                && items.is_empty()
                && u64::from(page.saturating_sub(1)) * 100 < total
        });
    if items.len() > 100 {
        items.truncate(100);
    }
    Ok(InventoryPage {
        source_evidence: None,
        items,
        done: total.is_some_and(|total| (page as u64) * 100 >= total),
        total,
        rejected_rows,
        incomplete,
    })
}
fn download_failed(stage: &'static str) -> DatabaseError {
    eprintln!("law provider: download failed at {stage}");
    DatabaseError::SourceDownloadFailed
}
const CREDENTIAL_REDACTION_MARKER: &str = "[openlegal-credential-redacted]";
fn credential_representations(credential: &str) -> Vec<String> {
    if credential.is_empty() {
        return Vec::new();
    }
    let encoded = url::form_urlencoded::byte_serialize(credential.as_bytes()).collect();
    let escaped = credential
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;");
    let mut representations = vec![credential.to_string(), encoded, escaped];
    representations.sort_by(|a, b| b.len().cmp(&a.len()).then_with(|| a.cmp(b)));
    representations.dedup();
    representations
}
fn reflected_credential(raw: &[u8], credential: &str) -> bool {
    credential_representations(credential).iter().any(|value| {
        raw.windows(value.len())
            .any(|part| part == value.as_bytes())
    })
}
fn has_credential_redaction(raw: &[u8]) -> bool {
    raw.windows(CREDENTIAL_REDACTION_MARKER.len())
        .any(|part| part == CREDENTIAL_REDACTION_MARKER.as_bytes())
}
/// Diagnostic categories are fixed call-site strings; provider values never
/// enter this message. Classification preserves the original fail-closed guard.
fn credential_redaction_failure(reason: &'static str) -> DatabaseError {
    eprintln!("law provider: credential redaction rejected at {reason}");
    DatabaseError::SourceDataInvalid
}
/// Identify only documented XML link-field text. This bounded lexical guard
/// grants no legal projection; the disposable processor still validates XML.
fn credential_link_ranges(
    xml: &str,
    credential: &str,
) -> Result<Vec<(usize, usize)>, DatabaseError> {
    let invalid = || credential_redaction_failure("xml_lexical_structure");
    let allowed_fields: std::collections::BTreeSet<&str> = catalog::SOURCE_FAMILIES
        .iter()
        .flat_map(|family| family.list_fields.iter().chain(family.detail_fields.iter()))
        .chain(
            catalog::GUIDE_ENTRIES
                .iter()
                .flat_map(|guide| guide.response_fields.iter()),
        )
        .copied()
        .filter(|field| field.ends_with("링크") || field.ends_with("URL"))
        .collect();
    let body_fields: std::collections::BTreeSet<&str> = catalog::SOURCE_FAMILIES
        .iter()
        .flat_map(|family| family.body_fields.iter())
        .copied()
        .collect();
    let blocked_ancestor = |name: &str| {
        content_field(local(name))
            || body_fields.contains(local(name))
            || matches!(
                local(name),
                "조문"
                    | "조문단위"
                    | "항"
                    | "호"
                    | "목"
                    | "별표"
                    | "별표단위"
                    | "첨부파일"
                    | "attachment"
                    | "본문"
                    | "법령본문"
                    | "body"
                    | "annotation"
                    | "translation"
                    | "commentary"
            )
            || local(name).contains("주석")
            || local(name).contains("해설")
            || local(name).contains("번역")
    };
    let mut stack: Vec<(&str, usize)> = Vec::new();
    let mut patches = Vec::new();
    let mut cursor = 0;
    let mut tags = 0;
    while let Some(relative) = xml[cursor..].find('<') {
        let start = cursor + relative;
        let rest = &xml[start..];
        if rest.starts_with("<!--") || rest.starts_with("<?") || rest.starts_with("<![CDATA[") {
            let (prefix, end) = if rest.starts_with("<!--") {
                (4, "-->")
            } else if rest.starts_with("<?") {
                (2, "?>")
            } else {
                (9, "]]>")
            };
            cursor = start + prefix + rest[prefix..].find(end).ok_or_else(invalid)? + end.len();
            continue;
        }
        if rest.starts_with("<!") {
            return Err(invalid());
        }
        // Find '>' outside quoted attributes; credential-bearing attributes
        // are never writable and will fail the final residual check.
        let mut quote = None;
        let mut end = None;
        for (offset, byte) in rest.bytes().enumerate().skip(1) {
            if let Some(active) = quote {
                if byte == active {
                    quote = None;
                }
            } else if matches!(byte, b'\'' | b'"') {
                quote = Some(byte);
            } else if byte == b'>' {
                end = Some(start + offset);
                break;
            }
        }
        let end = end.ok_or_else(invalid)?;
        let content = &xml[start + 1..end];
        let closing = content.starts_with('/');
        let name = content
            .trim_start_matches('/')
            .split(|character: char| character.is_ascii_whitespace() || character == '/')
            .next()
            .ok_or_else(invalid)?;
        if name.is_empty()
            || name.chars().any(|character| {
                !(character.is_alphanumeric() || matches!(character, '_' | ':' | '-' | '.'))
            })
        {
            return Err(invalid());
        }
        tags += 1;
        if tags > 100000 {
            return Err(invalid());
        }
        if closing {
            let (opened, text_start) = stack
                .pop()
                .ok_or_else(|| credential_redaction_failure("xml_closing_without_open"))?;
            // XML permits S (space, tab, CR, LF) between an end-tag name
            // and '>'. Keep those provider bytes unchanged in the archive.
            if opened != name
                || !content
                    .strip_prefix('/')
                    .is_some_and(|tag| tag.trim_end_matches([' ', '\t', '\r', '\n']) == name)
            {
                return Err(credential_redaction_failure("xml_closing_tag_mismatch"));
            }
            {
                let original = &xml[text_start..start];
                let text = original.trim();
                let trim_offset = original.len() - original.trim_start().len();
                let (value, offset, cdata) =
                    if text.starts_with("<![CDATA[") && text.ends_with("]]>") {
                        (&text[9..text.len() - 3], text_start + trim_offset + 9, true)
                    } else {
                        (text, text_start + trim_offset, false)
                    };
                if !value.contains('<') {
                    let mut candidate = Vec::new();
                    credential_url_ranges(value, offset, credential, cdata, &mut candidate)?;
                    if !candidate.is_empty() {
                        if name.contains(':') {
                            return Err(credential_redaction_failure("xml_link_namespace"));
                        }
                        if !allowed_fields.contains(name) {
                            return Err(credential_redaction_failure("xml_link_field_not_allowed"));
                        }
                        if stack.iter().any(|(ancestor, _)| blocked_ancestor(ancestor)) {
                            return Err(credential_redaction_failure("xml_link_body_ancestor"));
                        }
                    }
                    patches.extend(candidate);
                }
            }
        } else if !content.trim_end().ends_with('/') {
            stack.push((name, end + 1));
            if stack.len() > 64 {
                return Err(invalid());
            }
        }
        cursor = end + 1;
    }
    if !stack.is_empty() {
        return Err(credential_redaction_failure("xml_unclosed_element"));
    }
    patches.sort_unstable();
    if patches.len() > 10000 || patches.windows(2).any(|ranges| ranges[0].1 > ranges[1].0) {
        return Err(invalid());
    }
    Ok(patches)
}
fn credential_url_ranges(
    value: &str,
    offset: usize,
    credential: &str,
    cdata: bool,
    patches: &mut Vec<(usize, usize)>,
) -> Result<(), DatabaseError> {
    if value.len() > 16384 || value.chars().any(char::is_whitespace) {
        if reflected_credential(value.as_bytes(), credential) {
            eprintln!("law provider: credential link value outside lexical bounds");
        }
        return Ok(());
    }
    let common_entities = [
        ("&amp;", "&"),
        ("&lt;", "<"),
        ("&gt;", ">"),
        ("&quot;", "\""),
        ("&apos;", "'"),
    ];
    let decode_xml = |value: &str| {
        if cdata {
            return value.to_string();
        }
        // Decode ampersand last to avoid interpreting double-escaped entities.
        let mut decoded = value.to_string();
        for (entity, replacement) in common_entities
            .iter()
            .skip(1)
            .chain(common_entities.iter().take(1))
        {
            decoded = decoded.replace(entity, replacement);
        }
        decoded
    };
    // Ordinary XML text may contain numeric entities. Only URL query
    // candidates enter the conservative transport-link entity checks.
    let Some(question) = value.find('?') else {
        return Ok(());
    };
    let decoded = decode_xml(value);
    let base = Url::parse("https://www.law.go.kr").map_err(|_| DatabaseError::SourceDataInvalid)?;
    let Ok(url) = base.join(&decoded) else {
        return Ok(());
    };
    if !matches!(url.scheme(), "http" | "https")
        || !matches!(url.host_str(), Some("www.law.go.kr" | "law.go.kr"))
        || !url.username().is_empty()
        || url.password().is_some()
    {
        return Ok(());
    }
    if !cdata {
        // Numeric/custom entities are withheld rather than interpreted by the
        // server. General XML entity handling remains in the sandbox worker.
        if value.contains("&#") {
            return Err(credential_redaction_failure("xml_link_numeric_entity"));
        }
        for (index, _) in value.match_indices('&') {
            let rest = &value[index..];
            if rest
                .as_bytes()
                .iter()
                .take(17)
                .position(|byte| *byte == b';')
                .is_some_and(|end| !rest[..end].contains('=') && !rest[1..end].contains('&'))
                && !common_entities
                    .iter()
                    .any(|(entity, _)| rest.starts_with(entity))
            {
                return Err(credential_redaction_failure("xml_link_custom_entity"));
            }
        }
    }
    if url.fragment().is_some() {
        return Err(credential_redaction_failure("xml_link_fragment"));
    }
    let separator_len = |position: usize| {
        if !cdata && value[position..].starts_with("&amp;") {
            5
        } else {
            1
        }
    };
    let next_separator = |start: usize| {
        value[start..].match_indices('&').find_map(|(relative, _)| {
            let position = start + relative;
            (cdata
                || !common_entities
                    .iter()
                    .skip(1)
                    .any(|(entity, _)| value[position..].starts_with(entity)))
            .then_some(position)
        })
    };
    let mut cursor = question + 1;
    while cursor < value.len() {
        let end = next_separator(cursor).unwrap_or(value.len());
        if let Some(equals) = value[cursor..end].find('=') {
            let start = cursor + equals + 1;
            let segment = decode_xml(&value[cursor..end]);
            let mut parsed = url::form_urlencoded::parse(segment.as_bytes());
            if let Some((key, active)) = parsed.next()
                && key.eq_ignore_ascii_case("OC")
                && active == credential
                && parsed.next().is_none()
            {
                patches.push((offset + start, offset + end));
            }
        }
        if end == value.len() {
            break;
        }
        cursor = end + separator_len(end);
    }
    Ok(())
}

fn credential_privacy_view(value: &str) -> String {
    let mut decoded = String::with_capacity(value.len());
    let mut remaining = value;
    while let Some(start) = remaining.find('&') {
        decoded.push_str(&remaining[..start]);
        let entity = &remaining[start + 1..];
        let matched = entity
            .as_bytes()
            .iter()
            .take(17)
            .position(|byte| *byte == b';')
            .and_then(|end| {
                let name = &entity[..end];
                let character = match name {
                    "amp" => Some('&'),
                    "lt" => Some('<'),
                    "gt" => Some('>'),
                    "quot" => Some('"'),
                    "apos" => Some('\''),
                    _ => name
                        .strip_prefix("#x")
                        .or_else(|| name.strip_prefix("#X"))
                        .and_then(|hex| u32::from_str_radix(hex, 16).ok())
                        .or_else(|| {
                            name.strip_prefix('#')
                                .and_then(|decimal| decimal.parse::<u32>().ok())
                        })
                        .and_then(char::from_u32),
                }?;
                Some((end, character))
            });
        if let Some((end, character)) = matched {
            decoded.push(character);
            remaining = &entity[end + 1..];
        } else {
            decoded.push('&');
            remaining = entity;
        }
    }
    decoded.push_str(remaining);
    decoded
}
fn redact_transport_credentials(
    raw: Vec<u8>,
    credential: &str,
    format: DocumentFormat,
) -> Result<(Vec<u8>, bool), DatabaseError> {
    // Opaque signed/ND originals and HTML never undergo byte rewriting.
    if format != DocumentFormat::Xml {
        let html_auth_query = format == DocumentFormat::Html
            && std::str::from_utf8(&raw).is_ok_and(|text| {
                let inspected = credential_privacy_view(text).to_ascii_lowercase();
                ["oc=", "%4f%43=", "o%43=", "%4fc="]
                    .iter()
                    .any(|key| inspected.contains(key))
            });
        return if reflected_credential(&raw, credential) || html_auth_query {
            Err(DatabaseError::SourceDataInvalid)
        } else {
            Ok((raw, false))
        };
    }
    let text = std::str::from_utf8(&raw).map_err(|_| DatabaseError::SourceDataInvalid)?;
    let ranges = credential_link_ranges(text, credential).inspect_err(|_| {
        eprintln!("law provider: credential redaction failed in xml_link_scan");
    })?;
    let changed = !ranges.is_empty();
    let mut output = Vec::with_capacity(raw.len());
    let mut cursor = 0;
    for (start, end) in ranges {
        if output
            .len()
            .saturating_add(start - cursor)
            .saturating_add(CREDENTIAL_REDACTION_MARKER.len())
            > openlegal_application::document::MAX_DOCUMENT_BYTES
        {
            return Err(DatabaseError::SourceDataInvalid);
        }
        output.extend_from_slice(&raw[cursor..start]);
        output.extend_from_slice(CREDENTIAL_REDACTION_MARKER.as_bytes());
        cursor = end;
    }
    if output.len().saturating_add(raw.len() - cursor)
        > openlegal_application::document::MAX_DOCUMENT_BYTES
    {
        return Err(DatabaseError::SourceDataInvalid);
    }
    output.extend_from_slice(&raw[cursor..]);
    if reflected_credential(&output, credential) {
        return Err(credential_redaction_failure("residual_credential_literal"));
    }
    if std::str::from_utf8(&output).is_ok_and(|text| {
        reflected_credential(credential_privacy_view(text).as_bytes(), credential)
    }) {
        return Err(credential_redaction_failure(
            "residual_credential_xml_entity",
        ));
    }
    Ok((output, changed))
}

fn mark_redacted_projection(output: &DocumentOutput, metadata: &mut BTreeMap<String, String>) {
    if output
        .diagnostics
        .iter()
        .any(|diagnostic| diagnostic == "provider_credential_redacted")
    {
        metadata.insert("transport_credentials_redacted".into(), "true".into());
    }
}
/// Inspect only the XML root envelope, without parsing provider documents in
/// the server. Leading declarations, comments and PIs cannot conceal an API
/// error envelope from the raw archive guard.
fn provider_error_response(raw: &[u8]) -> bool {
    let mut rest = raw.strip_prefix(&[0xef, 0xbb, 0xbf]).unwrap_or(raw);
    loop {
        rest = rest.trim_ascii_start();
        let terminator = if rest.starts_with(b"<?") {
            b"?>".as_slice()
        } else if rest.starts_with(b"<!--") {
            b"-->".as_slice()
        } else {
            break;
        };
        let Some(end) = rest
            .windows(terminator.len())
            .position(|part| part == terminator)
        else {
            return false;
        };
        rest = &rest[end + terminator.len()..];
    }
    let Some(rest) = rest.strip_prefix(b"<") else {
        return false;
    };
    let end = rest
        .iter()
        .position(|byte| byte.is_ascii_whitespace() || matches!(byte, b'>' | b'/'))
        .unwrap_or(rest.len());
    rest[..end].rsplit(|byte| *byte == b':').next() == Some(b"Response".as_slice())
}

fn provider_response_error(raw: &[u8]) -> Option<DatabaseError> {
    if !provider_error_response(raw) {
        return None;
    }
    let Ok(body) = std::str::from_utf8(raw) else {
        return Some(DatabaseError::SourceDataInvalid);
    };
    let message = body.match_indices('<').find_map(|(start, _)| {
        let tag = &body[start + 1..];
        let end = tag.find('>')?;
        let name = tag[..end].split_ascii_whitespace().next()?;
        if name.rsplit(':').next() != Some("msg") {
            return None;
        }
        let content = &tag[end + 1..];
        let close = format!("</{name}>");
        let end = content.find(&close)?;
        (end <= 8192).then(|| content[..end].to_ascii_lowercase())
    });
    let unauthorized = message.is_some_and(|message| {
        [
            "unauthorized",
            "authentication",
            "permission",
            "access denied",
            "ip address",
            "권한",
            "인증",
            "사용자 이메일",
            "ip주소",
            "ip 주소",
            "등록된ip",
            "등록된 ip",
            "등록되지 않은 ip",
            "ip가 등록",
            "ip를 등록",
            "허용된 ip",
        ]
        .iter()
        .any(|marker| message.contains(marker))
    });
    Some(if unauthorized {
        DatabaseError::SourceUnauthorized
    } else {
        DatabaseError::SourceDataInvalid
    })
}

fn primary_source_observation(
    url: &Url,
    format: DocumentFormat,
    raw: &[u8],
    retrieved_at: u64,
) -> Option<crate::corpus::SourceObservationInput> {
    if !matches!(format, DocumentFormat::Xml | DocumentFormat::Html)
        || url.scheme() != "https"
        || url.port_or_known_default() != Some(443)
        || !matches!(url.host_str(), Some("www.law.go.kr" | "law.go.kr"))
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
    {
        return None;
    }
    let targets: Vec<_> = url
        .query_pairs()
        .filter(|(key, _)| key == "target")
        .map(|(_, value)| value.into_owned())
        .collect();
    let [target] = targets.as_slice() else {
        return None;
    };
    let family = catalog::SOURCE_FAMILIES.iter().find(|family| {
        family.target == target
            && (url.path() == family.list_path || family.detail_path == Some(url.path()))
    })?;
    let guide = if url.path() == family.list_path {
        family.list_guide
    } else {
        family.detail_guide?
    };
    let mut parameters: Vec<(String, String)> = url
        .query_pairs()
        .filter(|(key, _)| {
            !matches!(
                key.to_ascii_lowercase().as_str(),
                "oc" | "key" | "apikey" | "api_key" | "token" | "servicekey" | "password"
            )
        })
        .map(|(key, value)| (key.into_owned(), value.into_owned()))
        .collect();
    parameters.sort();
    let mut public = url.clone();
    public.set_query(None);
    public.query_pairs_mut().extend_pairs(parameters);
    if !openlegal_domain::rights::public_source_url(public.as_str()) || public.as_str().len() > 4096
    {
        return None;
    }
    let digest: String = Sha256::digest(public.as_str().as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    let mut observation = crate::corpus::SourceObservationInput {
        source_key: format!("law_go_kr:{guide}:{digest}"),
        raw: (!family.metadata_only).then(|| raw.to_vec()),
        media_type: if format == DocumentFormat::Html {
            "text/html"
        } else {
            "application/xml"
        }
        .into(),
        rights: if family.metadata_only {
            SourceRights::default()
        } else {
            SourceRights::legal_information()
        },
        metadata: BTreeMap::from([
            ("source_url".into(), public.to_string()),
            ("guide".into(), guide.into()),
            (
                "status".into(),
                if family.metadata_only {
                    "rights_unverified_metadata_only"
                } else {
                    "response_identity_unverified"
                }
                .into(),
            ),
        ]),
        observed_at: retrieved_at,
    };
    if has_credential_redaction(raw) {
        observation
            .metadata
            .insert("credentials_redacted".into(), "true".into());
    }
    Some(observation)
}

/// Derive JO solely from explicit direct numeric fields of original article
/// units. Both fields must be explicit. Article keys, attachments and prose
/// never supply identifiers. More than 4096 unique numbers returns no seeds;
/// publication uses the checked form and rejects that oversized response.
pub fn provision_numbers(tree: &DocumentNode) -> Vec<String> {
    provision_numbers_checked(tree).unwrap_or_default()
}
fn provision_numbers_checked(tree: &DocumentNode) -> Result<Vec<String>, DatabaseError> {
    fn direct_number(children: &[DocumentNode], field: &str) -> Result<Option<u32>, ()> {
        let mut fields = children.iter().filter_map(|child| match child {
            DocumentNode::Element { name, children, .. } if local(name) == field => Some(children),
            _ => None,
        });
        let Some(content) = fields.next() else {
            return Ok(None);
        };
        if fields.next().is_some()
            || content
                .iter()
                .any(|node| !matches!(node, DocumentNode::Text { .. }))
        {
            return Err(());
        }
        let mut value = String::new();
        for node in content {
            text(node, &mut value);
        }
        let value = value.trim();
        if value.is_empty() || value.len() > 8 || !value.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(());
        }
        value.parse::<u32>().map(Some).map_err(|_| ())
    }
    let mut found = std::collections::BTreeSet::new();
    let mut stack = vec![tree];
    while let Some(node) = stack.pop() {
        let DocumentNode::Element { name, children, .. } = node else {
            continue;
        };
        if matches!(local(name), "별표" | "첨부파일" | "attachment") {
            continue;
        }
        if local(name) == "조문단위"
            && let (Ok(Some(number)), Ok(Some(branch))) = (
                direct_number(children, "조문번호"),
                direct_number(children, "조문가지번호"),
            )
            && (1..=9999).contains(&number)
            && branch <= 99
        {
            found.insert(format!("{number:04}{branch:02}"));
            if found.len() > 4096 {
                return Err(DatabaseError::SourceDataInvalid);
            }
        }
        stack.extend(children.iter().rev());
    }
    Ok(found.into_iter().collect())
}

fn http_status_error(status: reqwest::StatusCode) -> Option<DatabaseError> {
    if status.is_success() {
        None
    } else if matches!(
        status,
        reqwest::StatusCode::UNAUTHORIZED | reqwest::StatusCode::FORBIDDEN
    ) {
        Some(DatabaseError::SourceUnauthorized)
    } else if matches!(
        status,
        reqwest::StatusCode::NOT_FOUND | reqwest::StatusCode::GONE
    ) {
        Some(DatabaseError::SourceUnavailable)
    } else if matches!(
        status,
        reqwest::StatusCode::REQUEST_TIMEOUT
            | reqwest::StatusCode::INTERNAL_SERVER_ERROR
            | reqwest::StatusCode::BAD_GATEWAY
            | reqwest::StatusCode::GATEWAY_TIMEOUT
    ) {
        eprintln!(
            "law provider: download failed at http_status_{}",
            status.as_u16()
        );
        Some(DatabaseError::SourceDownloadFailed)
    } else if status.is_server_error() {
        Some(DatabaseError::SourceTransient)
    } else {
        Some(DatabaseError::SourceRejected)
    }
}
fn document_error(error: DocumentError) -> DatabaseError {
    match error {
        DocumentError::Cancelled => DatabaseError::Cancelled,
        DocumentError::SandboxUnavailable | DocumentError::TimedOut => {
            DatabaseError::ProcessingPending
        }
        DocumentError::InvalidDocument
        | DocumentError::UnsupportedFormat
        | DocumentError::ResourceLimit => DatabaseError::SourceDataInvalid,
        DocumentError::InvalidInput | DocumentError::ProcessingFailed => {
            DatabaseError::SourceRejected
        }
    }
}
fn expected_document_magic(raw: &[u8], format: DocumentFormat) -> bool {
    match format {
        DocumentFormat::Pdf => raw.starts_with(b"%PDF-"),
        DocumentFormat::Hwp5 => raw.starts_with(&[0xd0, 0xcf, 0x11, 0xe0, 0xa1, 0xb1, 0x1a, 0xe1]),
        DocumentFormat::Hwpx => raw.starts_with(b"PK\x03\x04"),
        DocumentFormat::Xml | DocumentFormat::Html => false,
    }
}
fn looks_like_html(raw: &[u8]) -> bool {
    let prefix = raw
        .strip_prefix(&[0xef, 0xbb, 0xbf])
        .unwrap_or(raw)
        .iter()
        .copied()
        .skip_while(u8::is_ascii_whitespace)
        .take(16)
        .collect::<Vec<_>>();
    let lower = prefix.to_ascii_lowercase();
    lower.starts_with(b"<!doctype html") || lower.starts_with(b"<html")
}
fn media_type(format: DocumentFormat) -> &'static str {
    match format {
        DocumentFormat::Pdf => "application/pdf",
        DocumentFormat::Hwp5 => "application/x-hwp",
        DocumentFormat::Hwpx => "application/vnd.hancom.hwpx",
        DocumentFormat::Xml => "application/xml",
        DocumentFormat::Html => "text/html",
    }
}
fn metadata_detail(
    item: &InventoryItem,
    raw: Vec<u8>,
    retrieved_at: u64,
    retained: bool,
) -> Result<ProviderDetail, DatabaseError> {
    item.validate_for_detail()?;
    let family = catalog::source_family(item.object.dataset);
    let rights = if retained {
        SourceRights::legal_information()
    } else {
        SourceRights::default()
    };
    let resource = OriginalResource {
        ordinal: 0,
        title: item.title.clone(),
        media_type: "application/xml".into(),
        source_url: family.guide_url(),
        retained,
        rights,
    };
    let mut metadata = BTreeMap::new();
    metadata.insert(
        "original_resources".into(),
        serde_json::to_string(&vec![resource]).map_err(|_| DatabaseError::StorageCorrupt)?,
    );
    metadata.insert(
        "body_status".into(),
        if retained {
            "provider_original_only"
        } else {
            "rights_unverified_metadata_only"
        }
        .into(),
    );
    if retained {
        metadata.insert(
            "identity_verification".into(),
            "verified request identity; response fields undocumented".into(),
        );
    }
    let record = LegalRecord {
        object: item.object.clone(),
        revision_id: item.revision_id.clone(),
        title: item.title.clone(),
        body: String::new(),
        metadata,
        publication_date: item.publication_date.clone(),
        effective_date: item.effective_date.clone(),
        source_url: family.guide_url(),
        representation: if retained {
            "provider-original"
        } else {
            "metadata-only"
        }
        .into(),
        sections: Vec::new(),
    };
    record.validate()?;
    let raw = if retained {
        raw
    } else {
        serde_json::to_vec(item).map_err(|_| DatabaseError::StorageCorrupt)?
    };
    Ok(ProviderDetail {
        source_observations: Vec::new(),
        retrieved_at,
        record,
        raw,
        additional_evidence: Vec::new(),
        processor_version: "law-original-v1".into(),
    })
}
/// A licence belongs only to the attachment's own containing element. A
/// licence on the containing legal record or another attachment is not inherited.
fn attachment_rights(tree: &DocumentNode, link: &DocumentNode) -> SourceRights {
    fn parent<'a>(node: &'a DocumentNode, target: &DocumentNode) -> Option<&'a DocumentNode> {
        if let DocumentNode::Element { children, .. } = node {
            if children.iter().any(|child| std::ptr::eq(child, target)) {
                return Some(node);
            }
            for child in children {
                if let Some(found) = parent(child, target) {
                    return Some(found);
                }
            }
        }
        None
    }
    let Some(DocumentNode::Element { name, children, .. }) = parent(tree, link) else {
        return SourceRights::default();
    };
    if !matches!(local(name), "별표" | "첨부파일" | "attachment") {
        return SourceRights::default();
    }
    let direct = |field: &str| {
        children.iter().find_map(|child| match child {
            DocumentNode::Element { name, .. } if local(name) == field => {
                let mut value = String::new();
                text(child, &mut value);
                Some(value)
            }
            _ => None,
        })
    };
    let Some(kind) = direct("공공누리유형").and_then(|value| value.parse::<u8>().ok()) else {
        return SourceRights::default();
    };
    let Some(evidence) = direct("이용허락근거URL").filter(|value| {
        Url::parse(value).is_ok_and(|url| {
            url.scheme() == "https"
                && url.username().is_empty()
                && url.password().is_none()
                && !url.query_pairs().any(|(key, _)| {
                    matches!(
                        key.to_ascii_lowercase().as_str(),
                        "oc" | "token" | "apikey" | "api_key" | "servicekey" | "password"
                    )
                })
                && matches!(
                    url.host_str(),
                    Some("www.law.go.kr" | "law.go.kr" | "www.kogl.or.kr")
                )
        })
    }) else {
        return SourceRights::default();
    };
    let Some(attribution) = direct("출처표시").filter(|value| !value.trim().is_empty()) else {
        return SourceRights::default();
    };
    SourceRights::kogl(kind, evidence, attribution)
}
struct AttachmentLink {
    url: Url,
    format: DocumentFormat,
    title: String,
    rights: SourceRights,
}
/// Only documented attachment URL fields are consumed, with no synthesized IDs
/// or URL extraction from arbitrary prose. HTTP links are upgraded on the same host.
fn attachment_links(tree: &DocumentNode) -> Result<Vec<AttachmentLink>, DatabaseError> {
    let mut found = Vec::new();
    let mut seen = std::collections::BTreeSet::new();
    for (field, format) in [
        ("별표서식PDF파일링크", DocumentFormat::Pdf),
        ("별표서식파일링크", DocumentFormat::Hwp5),
    ] {
        let mut nodes = Vec::new();
        elements(tree, field, &mut nodes);
        for node in nodes {
            let mut value = String::new();
            text(node, &mut value);
            if value.is_empty() {
                continue;
            }
            let base =
                Url::parse("https://www.law.go.kr").map_err(|_| DatabaseError::InvalidInput)?;
            let mut url = base.join(&value).map_err(|_| DatabaseError::InvalidInput)?;
            if url.scheme() == "http" {
                url.set_scheme("https")
                    .map_err(|_| DatabaseError::InvalidInput)?;
            }
            if !matches!(url.host_str(), Some("www.law.go.kr" | "law.go.kr"))
                || url.path() != "/LSW/flDownload.do"
                || url.port_or_known_default() != Some(443)
                || !url.username().is_empty()
                || url.password().is_some()
                || url.fragment().is_some()
                || url.query_pairs().any(|(key, _)| {
                    matches!(
                        key.to_ascii_lowercase().as_str(),
                        "oc" | "apikey" | "api_key" | "token" | "servicekey" | "password"
                    )
                })
            {
                return Err(DatabaseError::InvalidInput);
            }
            if seen.insert(url.as_str().to_owned()) {
                let format = if format == DocumentFormat::Hwp5
                    && url
                        .query_pairs()
                        .any(|(_, v)| v.to_ascii_lowercase().ends_with(".hwpx"))
                {
                    DocumentFormat::Hwpx
                } else {
                    format
                };
                found.push(AttachmentLink {
                    url,
                    format,
                    title: field.into(),
                    rights: attachment_rights(tree, node),
                });
                if found.len() > 64 {
                    return Err(DatabaseError::SourceDataInvalid);
                }
            }
        }
    }
    Ok(found)
}
fn local(name: &str) -> &str {
    name.rsplit('}').next().unwrap_or(name)
}
fn elements<'a>(node: &'a DocumentNode, name: &str, out: &mut Vec<&'a DocumentNode>) {
    if let DocumentNode::Element {
        name: n, children, ..
    } = node
    {
        if local(n) == name {
            out.push(node);
        }
        for child in children {
            elements(child, name, out);
        }
    }
}
fn text(node: &DocumentNode, out: &mut String) {
    match node {
        DocumentNode::Text { value } => out.push_str(value),
        DocumentNode::Element { children, .. } => {
            for child in children {
                text(child, out)
            }
        }
    }
}
pub fn first(node: &DocumentNode, name: &str) -> Option<String> {
    let mut nodes = Vec::new();
    elements(node, name, &mut nodes);
    nodes.first().map(|n| {
        let mut value = String::new();
        text(n, &mut value);
        value
    })
}
fn date(value: Option<String>) -> Result<Option<String>, DatabaseError> {
    match value {
        Some(v) if !v.is_empty() => {
            if valid_date(&v) {
                Ok(Some(v))
            } else {
                Err(DatabaseError::StorageCorrupt)
            }
        }
        _ => Ok(None),
    }
}
fn numeric_id(value: &str) -> bool {
    !value.is_empty() && value.len() <= 128 && value.bytes().all(|b| b.is_ascii_digit())
}
fn target(dataset: Dataset) -> &'static str {
    catalog::source_family(dataset).target
}
fn precedent_html(item: &InventoryItem) -> bool {
    item.object.dataset == Dataset::Precedent
        && item
            .data_source
            .as_deref()
            .is_some_and(|source| source == "국세법령정보시스템" || source == "국세청")
}
/// Search fields in declared priority order, preserving their source values.
fn first_of(node: &DocumentNode, fields: &[&str]) -> Option<String> {
    fields.iter().find_map(|field| first(node, field))
}
/// Only an element with a direct identity child is a row. This handles the
/// ministry guides that document serial fields without documenting row tags.
fn identity_rows<'a>(node: &'a DocumentNode, fields: &[&str], out: &mut Vec<&'a DocumentNode>) {
    if let DocumentNode::Element { children, .. } = node {
        if children.iter().any(|child| {
            matches!(child,
            DocumentNode::Element { name, .. } if fields.contains(&local(name)))
        }) {
            out.push(node);
        } else {
            for child in children {
                identity_rows(child, fields, out);
            }
        }
    }
}
fn revision_parts(item: &InventoryItem) -> Result<(String, Option<String>), DatabaseError> {
    if item.object.provider != "law_go_kr"
        || item.object.jurisdiction != "kr"
        || !numeric_id(&item.object.id)
    {
        return Err(DatabaseError::InvalidInput);
    }
    if item.object.dataset == Dataset::NationalStatute {
        let (master, effective) = item
            .revision_id
            .split_once(':')
            .ok_or(DatabaseError::InvalidInput)?;
        if !numeric_id(master)
            || !valid_date(effective)
            || item.effective_date.as_deref() != Some(effective)
        {
            return Err(DatabaseError::InvalidInput);
        }
        Ok((master.into(), Some(effective.into())))
    } else if numeric_id(&item.revision_id)
        && (item.object.dataset.has_provider_revisions() || item.revision_id == item.object.id)
    {
        Ok((item.revision_id.clone(), None))
    } else {
        Err(DatabaseError::InvalidInput)
    }
}
fn content_field(name: &str) -> bool {
    matches!(
        name,
        "조문내용"
            | "항내용"
            | "호내용"
            | "목내용"
            | "조내용"
            | "부칙내용"
            | "개정문내용"
            | "제개정이유내용"
            | "판시사항"
            | "판결요지"
            | "참조조문"
            | "참조판례"
            | "판례내용"
            | "조문참고자료"
            | "조약내용"
            | "결정요지"
            | "전문"
            | "심판대상조문"
            | "질의요지"
            | "회답"
            | "이유"
            | "주문"
            | "청구취지"
            | "재결요지"
    )
}
fn content_parts(node: &DocumentNode, out: &mut Vec<String>) {
    if let DocumentNode::Element { name, children, .. } = node {
        if content_field(local(name)) {
            let mut value = String::new();
            text(node, &mut value);
            if !value.is_empty() {
                out.push(value);
            }
            return;
        }
        for child in children {
            content_parts(child, out);
        }
    }
}
fn sections(node: &DocumentNode, out: &mut Vec<LegalSection>) {
    if let DocumentNode::Element {
        name,
        attributes,
        children,
    } = node
    {
        let name = local(name);
        if name == "조문단위" || content_field(name) {
            let mut parts = Vec::new();
            content_parts(node, &mut parts);
            if parts.is_empty() {
                return;
            }
            let key = if name == "조문단위" {
                attributes
                    .iter()
                    .find(|(k, _)| local(k) == "조문키")
                    .map(|(_, v)| v.clone())
            } else {
                None
            };
            let id = key
                .map(|k| format!("article:{k}"))
                .unwrap_or_else(|| format!("source_ordinal:{}", out.len() + 1));
            out.push(LegalSection {
                id,
                title: if name == "조문단위" {
                    first(node, "조문제목").unwrap_or_default()
                } else {
                    name.into()
                },
                text: parts.join("\n"),
                kind: SectionKind::ProviderText,
                source_document_sha256: None,
                page: None,
            });
            return;
        }
        for child in children {
            sections(child, out);
        }
    }
}
fn catalog_content_parts(node: &DocumentNode, fields: &[&str], out: &mut Vec<String>) {
    if let DocumentNode::Element { name, children, .. } = node {
        if fields.contains(&local(name)) {
            let mut value = String::new();
            text(node, &mut value);
            if !value.is_empty() {
                out.push(value);
            }
        } else {
            for child in children {
                catalog_content_parts(child, fields, out);
            }
        }
    }
}
fn catalog_sections(node: &DocumentNode, fields: &[&str], out: &mut Vec<LegalSection>) {
    if let DocumentNode::Element {
        name,
        attributes,
        children,
    } = node
    {
        if local(name) == "조문단위" || fields.contains(&local(name)) {
            let mut parts = Vec::new();
            catalog_content_parts(node, fields, &mut parts);
            if parts.is_empty() {
                return;
            }
            let key = if local(name) == "조문단위" {
                attributes
                    .iter()
                    .find(|(key, _)| local(key) == "조문키")
                    .map(|(_, value)| value)
            } else {
                None
            };
            out.push(LegalSection {
                id: key
                    .map(|value| format!("article:{value}"))
                    .unwrap_or_else(|| format!("source_ordinal:{}", out.len() + 1)),
                title: if local(name) == "조문단위" {
                    first(node, "조문제목").unwrap_or_default()
                } else {
                    local(name).into()
                },
                text: parts.join("\n"),
                kind: SectionKind::ProviderText,
                source_document_sha256: None,
                page: None,
            });
        } else {
            for child in children {
                catalog_sections(child, fields, out);
            }
        }
    }
}
fn html_identity(node: &DocumentNode, id: &str) -> bool {
    if let DocumentNode::Element {
        name,
        attributes,
        children,
    } = node
    {
        if local(name).eq_ignore_ascii_case("input") {
            let name = attributes
                .iter()
                .find(|(k, _)| k.eq_ignore_ascii_case("name"))
                .map(|(_, v)| v.as_str());
            let value = attributes
                .iter()
                .find(|(k, _)| k.eq_ignore_ascii_case("value"))
                .map(|(_, v)| v.as_str());
            if matches!(name, Some("ID" | "precSeq" | "precId")) && value == Some(id) {
                return true;
            }
        }
        return children.iter().any(|n| html_identity(n, id));
    }
    false
}
fn strip_supplementary_subtrees(node: &mut DocumentNode) {
    if let DocumentNode::Element { children, .. } = node {
        children.retain(|child| !matches!(child,DocumentNode::Element{name,..} if matches!(local(name),"별표"|"첨부파일"|"attachment")));
        for child in children {
            strip_supplementary_subtrees(child);
        }
    }
}
fn contains_supplementary_subtree(node: &DocumentNode) -> bool {
    match node {
        DocumentNode::Element { name, children, .. } => {
            matches!(local(name), "별표" | "첨부파일" | "attachment")
                || children.iter().any(contains_supplementary_subtree)
        }
        _ => false,
    }
}
pub fn project(
    item: &InventoryItem,
    output: &DocumentOutput,
) -> Result<LegalRecord, DatabaseError> {
    // HTML text is flattened before projection; pruning its tree cannot remove
    // an unlicensed annex from that text. Refuse that projection entirely.
    if output.format == DocumentFormat::Html
        && output
            .tree
            .as_ref()
            .is_some_and(contains_supplementary_subtree)
    {
        return Err(DatabaseError::SourceDataInvalid);
    }
    let mut output = output.clone();
    if let Some(tree) = output.tree.as_mut() {
        strip_supplementary_subtrees(tree);
    }
    let output = &output;

    if !matches!(
        item.object.dataset,
        Dataset::NationalStatute | Dataset::Ordinance | Dataset::Precedent
    ) {
        return project_additional(item, output);
    }
    let tree = output.tree.as_ref().ok_or(DatabaseError::StorageCorrupt)?;
    let (master, effective) = revision_parts(item)?;
    let (idfield, titlefield) = match item.object.dataset {
        Dataset::NationalStatute => ("법령ID", "법령명_한글"),
        Dataset::Ordinance => ("자치법규ID", "자치법규명"),
        Dataset::Precedent => ("판례정보일련번호", "사건명"),
        _ => unreachable!("additional provider projection is handled above"),
    };
    let html = output.format == DocumentFormat::Html;
    let mut metadata = BTreeMap::new();
    mark_redacted_projection(output, &mut metadata);
    let title;
    let mut source_sections = Vec::new();
    if html {
        if item.object.dataset != Dataset::Precedent
            || !html_identity(tree, &item.object.id)
            || item.title.is_empty()
            || !output.text.contains(&item.title)
            || item
                .case_number
                .as_deref()
                .is_some_and(|v| !output.text.contains(v))
        {
            return Err(DatabaseError::StorageCorrupt);
        }
        title = item.title.clone();
        metadata.insert(
            "html_identity_rule".into(),
            "explicit_hidden_record_id_and_inventory_title_v1".into(),
        );
        source_sections.push(LegalSection {
            id: "html_document".into(),
            title: title.clone(),
            text: output.text.clone(),
            kind: SectionKind::ProviderText,
            source_document_sha256: None,
            page: None,
        });
    } else {
        let id = first(tree, idfield)
            .or_else(|| {
                if item.object.dataset == Dataset::Precedent {
                    first(tree, "판례일련번호")
                } else {
                    None
                }
            })
            .ok_or(DatabaseError::StorageCorrupt)?;
        if id != item.object.id {
            return Err(DatabaseError::StorageCorrupt);
        }
        if item.object.dataset == Dataset::Precedent
            && item
                .case_number
                .as_deref()
                .is_some_and(|expected| first(tree, "사건번호").as_deref() != Some(expected))
        {
            return Err(DatabaseError::StorageCorrupt);
        }
        if let Some(returned) = first(
            tree,
            match item.object.dataset {
                Dataset::NationalStatute => "법령일련번호",
                Dataset::Ordinance => "자치법규일련번호",
                Dataset::Precedent => "판례정보일련번호",
                _ => unreachable!("additional provider projection is handled above"),
            },
        ) && returned != master
        {
            return Err(DatabaseError::StorageCorrupt);
        }
        title = first(tree, titlefield)
            .filter(|v| !v.is_empty())
            .ok_or(DatabaseError::StorageCorrupt)?;
        sections(tree, &mut source_sections);
    }
    if source_sections.is_empty() {
        return Err(DatabaseError::StorageCorrupt);
    }
    let body = source_sections
        .iter()
        .map(|s| s.text.as_str())
        .collect::<Vec<_>>()
        .join("\n");
    for (original, key) in [
        ("소관부처", "authority"),
        ("지자체기관명", "authority"),
        ("법원명", "authority"),
        ("법종구분", "document_type"),
        ("자치법규종류", "document_type"),
        ("선고일자", "judgment_date_raw"),
        ("사건번호", "case_number"),
        ("조문시행일자문자열", "provision_effective_dates"),
        ("별표시행일자문자열", "annex_effective_dates"),
    ] {
        if let Some(value) = first(tree, original) {
            metadata.insert(key.into(), value);
        }
    }
    if let Some(value) = metadata
        .get("judgment_date_raw")
        .filter(|v| valid_date(v))
        .cloned()
    {
        metadata.insert("judgment_date".into(), value);
    }
    if let Some(value) = &item.data_source {
        metadata.insert("data_source".into(), value.clone());
    }
    if let Some(value) = &item.amendment_type {
        metadata.insert("amendment_type".into(), value.clone());
    }
    if let Some(value) = &item.case_number {
        metadata
            .entry("case_number".into())
            .or_insert(value.clone());
    }
    metadata.insert("projection_version".into(), "law_go_kr_text_v2".into());
    metadata.insert("provider_record_number".into(), master.clone());
    metadata.insert(
        "section_locator_semantics".into(),
        "source_article_key_or_source_ordinal".into(),
    );
    if let Some(value) = &effective {
        metadata.insert("requested_efYd".into(), value.clone());
        metadata.insert("character_view".into(), "010201".into());
    }
    let mut source = Url::parse("https://www.law.go.kr/DRF/lawService.do")
        .map_err(|_| DatabaseError::InvalidInput)?;
    source
        .query_pairs_mut()
        .append_pair("target", target(item.object.dataset))
        .append_pair("type", if html { "HTML" } else { "XML" })
        .append_pair(
            if item.object.dataset == Dataset::Precedent {
                "ID"
            } else {
                "MST"
            },
            &master,
        );
    if let Some(value) = &effective {
        source
            .query_pairs_mut()
            .append_pair("efYd", value)
            .append_pair("chrClsCd", "010201");
    }
    let returned_effective = if html {
        None
    } else {
        date(first(tree, "시행일자"))?
    };
    if effective.is_some() && returned_effective != effective {
        return Err(DatabaseError::StorageCorrupt);
    }
    let record = LegalRecord {
        object: item.object.clone(),
        revision_id: item.revision_id.clone(),
        title,
        body,
        sections: source_sections,
        metadata,
        publication_date: if html {
            None
        } else {
            date(first(tree, "공포일자"))?
        },
        effective_date: returned_effective,
        source_url: source.into(),
        representation: match item.object.dataset {
            Dataset::NationalStatute => "provider_effective_original",
            Dataset::Ordinance => "provider_current",
            Dataset::Precedent => "provider_record",
            _ => unreachable!("additional provider projection is handled above"),
        }
        .into(),
    };
    record
        .validate()
        .map_err(|_| DatabaseError::StorageCorrupt)?;
    Ok(record)
}

/// These API families expose a provider record number, not a documented
/// revision history. Only administrative rules additionally expose a stable ID.
fn project_additional(
    item: &InventoryItem,
    output: &DocumentOutput,
) -> Result<LegalRecord, DatabaseError> {
    if output.format != DocumentFormat::Xml {
        return Err(DatabaseError::StorageCorrupt);
    }
    let tree = output.tree.as_ref().ok_or(DatabaseError::StorageCorrupt)?;
    let (number, _) = revision_parts(item)?;
    let family = catalog::source_family(item.object.dataset);
    if matches!(item.object.dataset, Dataset::EnglishStatute)
        || family.detail_mode == catalog::DetailMode::ListOnly
    {
        return Err(DatabaseError::SourceDataInvalid);
    }
    let number_field = match item.object.dataset {
        Dataset::AdministrativeAppeal => "행정심판례일련번호",
        Dataset::LegalTerm => "법령용어일련번호",
        _ => family
            .revision_fields
            .first()
            .copied()
            .ok_or(DatabaseError::StorageCorrupt)?,
    };
    let title_fields: &[&str] = match item.object.dataset {
        Dataset::Treaty => &["조약명_한글"],
        Dataset::LegalTerm => &["법령용어명_한글"],
        _ => family.title_fields,
    };
    let mut serials = Vec::new();
    elements(tree, number_field, &mut serials);
    if serials.len() != 1 || first(tree, number_field).as_deref() != Some(number.as_str()) {
        return Err(DatabaseError::StorageCorrupt);
    }
    if matches!(
        item.object.dataset,
        Dataset::AdministrativeRule
            | Dataset::SchoolRule
            | Dataset::LocalPublicCorporationRule
            | Dataset::PublicInstitutionRule
    ) {
        let mut ids = Vec::new();
        elements(tree, "행정규칙ID", &mut ids);
        if ids.len() != 1 || first(tree, "행정규칙ID").as_deref() != Some(item.object.id.as_str())
        {
            return Err(DatabaseError::StorageCorrupt);
        }
    }
    let title = first_of(tree, title_fields)
        .filter(|s| !s.is_empty())
        .ok_or(DatabaseError::StorageCorrupt)?;
    if item.object.dataset == Dataset::LegalTerm && title != item.title {
        return Err(DatabaseError::StorageCorrupt);
    }
    let mut source_sections = Vec::new();
    if matches!(
        item.object.dataset,
        Dataset::AdministrativeRule
            | Dataset::Treaty
            | Dataset::ConstitutionalDecision
            | Dataset::LegalInterpretation
            | Dataset::AdministrativeAppeal
    ) {
        sections(tree, &mut source_sections);
    } else {
        catalog_sections(tree, family.body_fields, &mut source_sections);
    }
    if source_sections.is_empty() {
        return Err(DatabaseError::StorageCorrupt);
    }
    let mut metadata = BTreeMap::new();
    mark_redacted_projection(output, &mut metadata);
    metadata.insert(
        "projection_version".into(),
        "law_go_kr_additional_v1".into(),
    );
    metadata.insert("provider_record_number".into(), number.clone());
    metadata.insert("section_locator_semantics".into(), "source_ordinal".into());
    if let Some(value) = &item.amendment_type {
        metadata.insert("amendment_type".into(), value.clone());
    }
    for (original, key) in [
        ("행정규칙종류", "document_type"),
        ("소관부처명", "authority"),
        ("조약구분코드", "document_type_code"),
        ("사건번호", "case_number"),
        ("해석기관명", "authority"),
        ("재결청", "authority"),
    ] {
        if let Some(value) = first(tree, original).filter(|s| !s.is_empty()) {
            metadata.insert(key.into(), value);
        }
    }
    if item.object.dataset == Dataset::Treaty {
        if let Some(expected) = &item.treaty_class_code
            && metadata.get("document_type_code") != Some(expected)
        {
            return Err(DatabaseError::StorageCorrupt);
        }
        let kind = match metadata.get("document_type_code").map(String::as_str) {
            Some("440101") => "bilateral_treaty",
            Some("440102") => "multilateral_treaty",
            _ => return Err(DatabaseError::StorageCorrupt),
        };
        metadata.insert("document_type".into(), kind.into());
        metadata.insert("character_view".into(), "010202".into());
    }
    if item.object.dataset == Dataset::ConstitutionalDecision
        && let Some(raw) = first(tree, "종국일자").filter(|v| !v.is_empty())
    {
        metadata.insert("final_disposition_date_raw".into(), raw.clone());
        if valid_date(&raw) {
            metadata.insert("final_disposition_date".into(), raw);
        }
    }
    for (original, key) in [
        ("해석일자", "interpretation_date"),
        ("의결일자", "decision_date"),
        ("발령일자", "issuance_date"),
    ] {
        if let Some(raw) = first(tree, original).filter(|v| !v.is_empty()) {
            metadata.insert(format!("{key}_raw"), raw.clone());
            if valid_date(&raw) {
                metadata.insert(key.into(), raw);
            }
        }
    }
    let effective_date = match item.object.dataset {
        Dataset::AdministrativeRule
        | Dataset::SchoolRule
        | Dataset::LocalPublicCorporationRule
        | Dataset::PublicInstitutionRule => date(first(tree, "시행일자"))?,
        Dataset::Treaty => date(first(tree, "발효일자"))?,
        _ => None,
    };
    if item.effective_date.is_some() && effective_date != item.effective_date {
        return Err(DatabaseError::StorageCorrupt);
    }
    let mut source = Url::parse("https://www.law.go.kr/DRF/lawService.do")
        .map_err(|_| DatabaseError::InvalidInput)?;
    source
        .query_pairs_mut()
        .append_pair("target", target(item.object.dataset))
        .append_pair("type", "XML")
        .append_pair(
            family.detail_parameter(),
            if family.detail_mode == catalog::DetailMode::TermName {
                &item.title
            } else {
                &number
            },
        );
    if item.object.dataset == Dataset::Treaty {
        source.query_pairs_mut().append_pair("chrClsCd", "010202");
    }
    let body = source_sections
        .iter()
        .map(|s| s.text.as_str())
        .collect::<Vec<_>>()
        .join("\n");
    let record = LegalRecord {
        object: item.object.clone(),
        revision_id: item.revision_id.clone(),
        title,
        body,
        sections: source_sections,
        metadata,
        publication_date: None,
        effective_date,
        source_url: source.into(),
        representation: "provider_record".into(),
    };
    record
        .validate()
        .map_err(|_| DatabaseError::StorageCorrupt)?;
    Ok(record)
}

#[cfg(test)]
mod tests {
    use super::*;
    use openlegal_application::persistence::PersistentStore;

    struct UnusedProcessor;
    impl DocumentProcessor for UnusedProcessor {
        fn process(
            &self,
            _input: DocumentInput,
            _cancellation: CancellationToken,
        ) -> futures::future::BoxFuture<'static, Result<DocumentOutput, DocumentError>> {
            Box::pin(async { Err(DocumentError::InvalidInput) })
        }
    }

    #[test]
    fn primary_archive_descriptor_preserves_unparsed_bytes_and_omits_credentials() {
        let url = Url::parse("https://www.law.go.kr/DRF/lawSearch.do?OC=fixture-only&target=eflaw&type=XML&page=1&display=100").unwrap();
        let input =
            primary_source_observation(&url, DocumentFormat::Xml, b"malformed original XML", 123)
                .unwrap();
        assert_eq!(
            input.raw.as_deref(),
            Some(b"malformed original XML".as_slice())
        );
        assert_eq!(input.observed_at, 123);
        assert_eq!(input.metadata["status"], "response_identity_unverified");
        assert_eq!(input.metadata["guide"], "lsEfYdListGuide");
        assert!(input.rights.can_store());
        assert!(!input.metadata["source_url"].contains("fixture-only"));
        assert!(!input.metadata["source_url"].contains("OC="));
        let changed = Url::parse("https://www.law.go.kr/DRF/lawSearch.do?display=100&page=1&type=XML&target=eflaw&oc=another-fixture&TOKEN=fixture-token&api_key=fixture-key").unwrap();
        let same = primary_source_observation(
            &changed,
            DocumentFormat::Xml,
            b"malformed original XML",
            124,
        )
        .unwrap();
        assert_eq!(same.source_key, input.source_key);
        assert_eq!(same.metadata["source_url"], input.metadata["source_url"]);
        let detail = primary_source_observation(
            &Url::parse("https://www.law.go.kr/DRF/lawService.do?target=eflaw&type=XML&MST=123")
                .unwrap(),
            DocumentFormat::Xml,
            b"unverified identity",
            125,
        )
        .unwrap();
        assert_eq!(detail.metadata["guide"], "lsEfYdInfoGuide");
        assert_ne!(detail.source_key, input.source_key);
    }
    #[test]
    fn primary_archive_retains_only_metadata_for_unknown_rights_and_excludes_other_routes() {
        let family = catalog::SOURCE_FAMILIES
            .iter()
            .find(|family| family.metadata_only)
            .unwrap();
        let url = Url::parse(&format!(
            "https://www.law.go.kr{}?target={}&type=XML",
            family.list_path, family.target
        ))
        .unwrap();
        let input = primary_source_observation(
            &url,
            DocumentFormat::Xml,
            b"unverified copyrighted original",
            123,
        )
        .unwrap();
        assert!(input.raw.is_none());
        assert!(!input.rights.can_store());
        assert_eq!(input.metadata["status"], "rights_unverified_metadata_only");
        for value in [
            "https://www.law.go.kr/LSW/flDownload.do?target=eflaw&flSeq=123",
            "https://www.law.go.kr/DRF/lawSearch.do?target=lsByl&type=XML",
            "https://www.law.go.kr/DRF/lawSearch.do?target=eflaw&target=eflaw",
            "https://www.law.go.kr/DRF/lawSearch.do",
            "https://example.test/DRF/lawSearch.do?target=eflaw",
        ] {
            assert!(
                primary_source_observation(
                    &Url::parse(value).unwrap(),
                    DocumentFormat::Xml,
                    b"raw",
                    123
                )
                .is_none()
            );
        }
    }
    #[test]
    fn provider_error_envelope_cannot_hide_behind_xml_comments_or_processing_instructions() {
        for raw in [
            "<Response><message>error</message></Response>",
            "\u{feff} <?xml version=\"1.0\"?> <!--comment--> <?provider error?> <Response code=\"failure\"/>",
            "<!--prefix--><p:Response xmlns:p=\"urn:fixture\"><message>error</message></p:Response>",
            "<?xml version=\"1.0\"?><?xml-stylesheet href=\"ignored\"?><!--first--><!--second--><ns:Response/>",
        ] {
            assert!(provider_error_response(raw.as_bytes()), "{raw}");
        }
        let long_prefix = format!("<!--{}--><api:Response/>", "x".repeat(8192));
        assert!(provider_error_response(long_prefix.as_bytes()));
        for raw in [
            "<?xml version=\"1.0\"?><law><Response>quoted text</Response></law>",
            "<!--unfinished",
            "<?unfinished",
            "<ResponseCount>1</ResponseCount>",
            "<law/>",
        ] {
            assert!(!provider_error_response(raw.as_bytes()), "{raw}");
        }
    }
    #[test]
    fn scoped_transport_redaction_handles_exact_auth_values_and_preserves_legal_body() {
        let credential = "fixture@example.test\"auth";
        for representation in [
            "fixture@example.test\"auth",
            "fixture%40example.test%22auth",
            "fixture@example.test&quot;auth",
        ] {
            let wire = format!(
                "<LawSearch><법령상세링크>/DRF/lawService.do?OC={representation}&amp;target=eflaw&amp;MST=100</법령상세링크><조문내용>한글 법률 본문 그대로</조문내용></LawSearch>"
            );
            let (redacted, changed) =
                redact_transport_credentials(wire.into_bytes(), credential, DocumentFormat::Xml)
                    .unwrap();
            assert!(changed);
            assert!(!reflected_credential(&redacted, credential));
            let expected = format!(
                "<LawSearch><법령상세링크>/DRF/lawService.do?OC={CREDENTIAL_REDACTION_MARKER}&amp;target=eflaw&amp;MST=100</법령상세링크><조문내용>한글 법률 본문 그대로</조문내용></LawSearch>"
            );
            assert_eq!(redacted, expected.as_bytes());
            let input = primary_source_observation(
                &Url::parse("https://www.law.go.kr/DRF/lawSearch.do?target=eflaw").unwrap(),
                DocumentFormat::Xml,
                &redacted,
                100,
            )
            .unwrap();
            assert_eq!(input.metadata["credentials_redacted"], "true");
            assert_eq!(input.raw.as_deref(), Some(redacted.as_slice()));
        }
        let encoded = "<LawSearch><법령상세링크>/DRF/lawService.do?OC=%66ixture&amp;target=eflaw</법령상세링크></LawSearch>";
        assert!(
            redact_transport_credentials(
                encoded.as_bytes().to_vec(),
                "fixture",
                DocumentFormat::Xml
            )
            .unwrap()
            .1
        );
        let wire = "<LawSearch><법령상세링크><![CDATA[/DRF/lawService.do?oc=fixture&target=eflaw]]></법령상세링크></LawSearch>".as_bytes();
        let (redacted, changed) =
            redact_transport_credentials(wire.to_vec(), "fixture", DocumentFormat::Xml).unwrap();
        assert!(changed);
        assert!(
            String::from_utf8(redacted)
                .unwrap()
                .contains("?oc=[openlegal-credential-redacted]&target=eflaw")
        );
    }
    #[test]
    fn decision_inventory_transport_links_use_the_documented_relative_url_shape() {
        for (target, field) in [
            ("decc", "행정심판례상세링크"),
            ("ppc", "결정문상세링크"),
            ("ftc", "결정문상세링크"),
            ("acr", "결정문상세링크"),
            ("nhrck", "결정문상세링크"),
        ] {
            let wire = format!(
                "<{target}Search><{target}><{field}>/DRF/lawService.do?OC=fixture&amp;target={target}&amp;ID=12345&amp;type=XML</{field}><사건명>사건 본문 그대로</사건명></{target}></{target}Search>"
            );
            let expected = wire.replace("OC=fixture", &format!("OC={CREDENTIAL_REDACTION_MARKER}"));
            let (redacted, changed) =
                redact_transport_credentials(wire.into_bytes(), "fixture", DocumentFormat::Xml)
                    .unwrap();
            assert!(changed, "target {target}");
            assert_eq!(redacted, expected.as_bytes(), "target {target}");
        }
    }
    #[test]
    fn transport_link_redaction_preserves_legal_xml_end_tag_whitespace() {
        let wire = "<DeccSearch>\n<decc><행정심판례상세링크>/DRF/lawService.do?OC=fixture&amp;target=decc&amp;ID=12345&amp;type=XML</행정심판례상세링크 \t>\n<사건명>변형 없는 법률 본문</사건명\n>\n</decc \r\n>\n</DeccSearch >";
        let expected = wire.replace("OC=fixture", &format!("OC={CREDENTIAL_REDACTION_MARKER}"));
        let (redacted, changed) =
            redact_transport_credentials(wire.as_bytes().to_vec(), "fixture", DocumentFormat::Xml)
                .unwrap();
        assert!(changed);
        assert_eq!(redacted, expected.as_bytes());
        for invalid_closing in ["법령상세링크 extra", "법령상세링크/", "wrong "] {
            let invalid = format!(
                "<LawSearch><법령상세링크>/DRF/lawService.do?OC=fixture</{invalid_closing}></LawSearch>"
            );
            assert_eq!(
                redact_transport_credentials(invalid.into_bytes(), "fixture", DocumentFormat::Xml)
                    .err(),
                Some(DatabaseError::SourceDataInvalid)
            );
        }
    }
    #[test]
    fn reflected_non_link_text_and_opaque_originals_are_withheld_without_rewriting() {
        for wire in [
            "<law><조문내용>fixture 법률 본문</조문내용></law>",
            "<law><법령상세링크>/DRF/lawService.do?ID=fixture</법령상세링크></law>",
            "<law><!--<법령상세링크>/DRF/lawService.do?OC=fixture</법령상세링크>--></law>",
            "<law><조문내용><![CDATA[<법령상세링크>/DRF/lawService.do?OC=fixture</법령상세링크>]]></조문내용></law>",
            "<law><법령상세링크>/DRF/lawService.do?OC=fixture</법령상세링크><조문내용>fixture 법률 본문</조문내용></law>",
            "<law><임의링크>/DRF/lawService.do?OC=fixture</임의링크></law>",
            "<law><임의링크>/DRF/lawService.do?OC=%66ixture</임의링크></law>",
            "<law><법령상세링크>/DRF/lawService.do?OC=%66ixture#section</법령상세링크></law>",
            "<law><조문내용><법령상세링크>/DRF/lawService.do?OC=%66ixture</법령상세링크></조문내용></law>",
            "<law><주석><법령상세링크>/DRF/lawService.do?OC=fixture</법령상세링크></주석></law>",
            "<law><별표><법령상세링크>/DRF/lawService.do?OC=fixture</법령상세링크></별표></law>",
            "<law><법령상세링크>/DRF/lawService.do?OC=fixt&#117;re&amp;target=eflaw</법령상세링크></law>",
            "<law><조문내용>fixt&#117;re 법률 본문</조문내용></law>",
        ] {
            assert_eq!(
                redact_transport_credentials(
                    wire.as_bytes().to_vec(),
                    "fixture",
                    DocumentFormat::Xml
                )
                .err(),
                Some(DatabaseError::SourceDataInvalid)
            );
        }
        for format in [
            DocumentFormat::Pdf,
            DocumentFormat::Hwp5,
            DocumentFormat::Hwpx,
            DocumentFormat::Html,
        ] {
            assert_eq!(
                redact_transport_credentials(
                    b"opaque signed fixture original".to_vec(),
                    "fixture",
                    format
                )
                .err(),
                Some(DatabaseError::SourceDataInvalid)
            );
        }
        assert_eq!(
            redact_transport_credentials(
                b"<html><a href=\"/DRF/lawService.do?OC=%66ixture\">link</a></html>".to_vec(),
                "fixture",
                DocumentFormat::Html
            )
            .err(),
            Some(DatabaseError::SourceDataInvalid)
        );
        let wire = "<law><법령명_한글>법률&#32;명</법령명_한글><조문내용>조문&#x20;본문 &amp; 원문</조문내용></law>".as_bytes();
        let (same, changed) =
            redact_transport_credentials(wire.to_vec(), "fixture", DocumentFormat::Xml).unwrap();
        assert!(!changed);
        assert_eq!(same, wire);
    }
    #[test]
    fn redacted_projection_metadata_carries_the_transport_exception_for_every_family() {
        let mut original = output(branch(
            "법령",
            vec![
                field("법령ID", "1"),
                field("법령명_한글", "Fictional statute"),
                field("시행일자", "20260101"),
                field("조문내용", "한글 법률 본문 그대로"),
            ],
        ));
        original
            .diagnostics
            .push("provider_credential_redacted".into());
        let record = project(&item(), &original).unwrap();
        assert_eq!(record.body, "한글 법률 본문 그대로");
        assert_eq!(record.metadata["transport_credentials_redacted"], "true");
        let mut additional = item();
        additional.object.dataset = Dataset::MoelInterpretation;
        additional.object.id = "100".into();
        additional.revision_id = "100".into();
        additional.effective_date = None;
        let mut data = output(branch(
            "Service",
            vec![
                field("법령해석일련번호", "100"),
                field("안건명", "Fictional additional"),
                field("회답", "한글 법률 본문 그대로"),
            ],
        ));
        data.diagnostics.push("provider_credential_redacted".into());
        let record = project(&additional, &data).unwrap();
        assert_eq!(record.body, "한글 법률 본문 그대로");
        assert_eq!(record.metadata["transport_credentials_redacted"], "true");
    }
    #[test]
    fn provider_error_envelopes_fence_only_permission_or_auth_failures() {
        for value in [
            "<Response><msg>사용자 이메일 주소를 확인해주세요.</msg></Response>",
            "<!--prefix--><ns:Response><ns:msg>등록된IP가 아닙니다.</ns:msg></ns:Response>",
            "<Response><msg>권한이 없습니다.</msg></Response>",
            "<Response><msg>Authentication failed</msg></Response>",
        ] {
            assert_eq!(
                provider_response_error(value.as_bytes()),
                Some(DatabaseError::SourceUnauthorized)
            );
        }
        for value in [
            "<Response><msg>잘못된 파라미터 입니다.</msg></Response>",
            "<Response><msg>지원하지 않는 API 입니다.</msg></Response>",
            "<Response><msg>invalid parameter</msg><diagnostic>permission</diagnostic></Response>",
        ] {
            assert_eq!(
                provider_response_error(value.as_bytes()),
                Some(DatabaseError::SourceDataInvalid)
            );
        }
        assert_eq!(
            provider_response_error(b"<law><msg>permission</msg></law>"),
            None
        );
        assert!(reflected_credential(
            b"<law>fixture%40example.test</law>",
            "fixture@example.test"
        ));
        assert!(reflected_credential(
            b"<law>fixture&amp;credential</law>",
            "fixture&credential"
        ));
        assert!(!reflected_credential(
            b"<law>unrelated legal information</law>",
            "fixture@example.test"
        ));
    }
    #[test]
    fn provision_numbers_use_only_direct_numeric_fields_and_never_article_keys() {
        let unit = |children| branch("조문단위", children);
        let tree = branch(
            "법령",
            vec![
                unit(vec![field("조문번호", "1"), field("조문가지번호", "0")]),
                unit(vec![field("조문번호", "7")]),
                unit(vec![field("조문번호", "12"), field("조문가지번호", "3")]),
                unit(vec![field("조문번호", "9999"), field("조문가지번호", "99")]),
                unit(vec![field("조문번호", "0012"), field("조문가지번호", "03")]),
                unit(vec![
                    field("조문키", "0042000"),
                    field("조문내용", "제42조"),
                ]),
                unit(vec![branch("nested", vec![field("조문번호", "43")])]),
                unit(vec![field("조문번호", "0")]),
                unit(vec![field("조문번호", "10000")]),
                unit(vec![field("조문번호", "2"), field("조문가지번호", "100")]),
                unit(vec![
                    field("조문번호", "3"),
                    field("조문가지번호", "invalid"),
                ]),
                unit(vec![field("조문번호", "4"), field("조문번호", "5")]),
                branch("별표", vec![unit(vec![field("조문번호", "6")])]),
            ],
        );
        assert_eq!(provision_numbers(&tree), vec!["000100", "001203", "999999"]);
        let many = branch(
            "법령",
            (1..=5000)
                .map(|number| {
                    unit(vec![
                        field("조문번호", &number.to_string()),
                        field("조문가지번호", "0"),
                    ])
                })
                .collect(),
        );
        let values = provision_numbers(&many);
        assert!(values.is_empty());
        assert_eq!(
            provision_numbers_checked(&many).err(),
            Some(DatabaseError::SourceDataInvalid)
        );
        let exact = branch(
            "법령",
            (1..=4096)
                .map(|number| {
                    unit(vec![
                        field("조문번호", &number.to_string()),
                        field("조문가지번호", "0"),
                    ])
                })
                .collect(),
        );
        assert_eq!(provision_numbers(&exact).len(), 4096);
    }

    #[tokio::test]
    async fn local_attempt_cap_is_shared_and_unlimited_is_explicit() {
        let client = LawClient::new("fixture".into(), Arc::new(UnusedProcessor))
            .unwrap()
            .with_local_cap(2);
        let clone = client.clone();
        client
            .reserve_request(&CancellationToken::new())
            .await
            .unwrap();
        clone
            .reserve_request(&CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(
            client.reserve_request(&CancellationToken::new()).await,
            Err(DatabaseError::BudgetExhausted)
        );
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgresql://fictional:fictional@127.0.0.1/unused")
            .unwrap();
        let budgeted = client.with_request_budget(pool, RequestBudgetMode::Continuous);
        let unlimited = budgeted
            .on_demand_client_with_limit(RequestLimit::Unlimited)
            .unwrap();
        assert!(unlimited.local_cap.is_none());
        let capped = budgeted
            .on_demand_client_with_limit(RequestLimit::Limited(17))
            .unwrap();
        assert_eq!(
            capped.local_cap.as_ref().unwrap().load(Ordering::Acquire),
            17
        );
        assert_eq!(
            budgeted
                .on_demand_client_with_limit(RequestLimit::Limited(0))
                .err(),
            Some(DatabaseError::InvalidInput)
        );
    }

    #[tokio::test]
    async fn cancellation_before_reservation_does_not_spend_local_attempts() {
        let observer = Arc::new(AtomicBool::new(false));
        let client = LawClient::new("fictional".into(), Arc::new(UnusedProcessor))
            .unwrap()
            .with_local_cap(1)
            .with_reservation_observer(observer.clone());
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        assert_eq!(
            client.reserve_request(&cancellation).await,
            Err(DatabaseError::Cancelled)
        );
        assert_eq!(
            client.local_cap.as_ref().unwrap().load(Ordering::Acquire),
            1
        );
        assert!(!observer.load(Ordering::Acquire));
        client
            .reserve_request(&CancellationToken::new())
            .await
            .unwrap();
        assert!(observer.load(Ordering::Acquire));
    }

    #[tokio::test]
    #[ignore = "requires scripts/test-postgres.sh"]
    async fn old_collection_launch_cannot_reserve_under_relaunched_receipt_uuid() {
        let fixture = crate::test_support::TestDatabase::new().await;
        let storage = fixture.open(100).await;
        let pool = storage.pool();
        let blobs = crate::blob::FsBlobStore::open(
            &fixture.directory.path().join("launch-fenced-admission"),
        )
        .await
        .unwrap();
        let corpus = crate::corpus::PgCorpusStore::new(pool.clone(), blobs);
        corpus.heartbeat_collection_scheduler().await.unwrap();
        corpus
            .request_collection(openlegal_domain::collection::CollectionRequest {
                target: openlegal_domain::collection::CollectionTarget::Object {
                    object: ObjectId {
                        jurisdiction: "kr".into(),
                        provider: "law_go_kr".into(),
                        dataset: Dataset::NationalStatute,
                        id: "001".into(),
                    },
                },
            })
            .await
            .unwrap();
        let old = corpus
            .claim_collection_request_with_policy(60, RequestLimit::Limited(1))
            .await
            .unwrap()
            .unwrap();
        let observer = Arc::new(AtomicBool::new(false));
        let client = LawClient::new("fictional".into(), Arc::new(UnusedProcessor))
            .unwrap()
            .with_request_budget(pool.clone(), RequestBudgetMode::OnDemand)
            .with_local_cap(1)
            .with_collection_launch(&old)
            .unwrap()
            .with_reservation_observer(observer.clone());
        let mut malformed = old.clone();
        malformed.id = "invalid-owner".into();
        assert_eq!(
            client.clone().with_collection_launch(&malformed).err(),
            Some(DatabaseError::InvalidInput)
        );
        sqlx::query("UPDATE openlegal.collection_request SET launched_at=launched_at+1,lease_until=lease_until+1 WHERE id=$1::uuid")
            .bind(&old.id).execute(&pool).await.unwrap();
        assert_eq!(
            client.reserve_request(&CancellationToken::new()).await,
            Err(DatabaseError::Cancelled)
        );
        assert!(!observer.load(Ordering::Acquire));
        assert_eq!(
            client.local_cap.as_ref().unwrap().load(Ordering::Acquire),
            1
        );
        let state: (i64,bool) = sqlx::query_as("SELECT on_demand_used,unresolved_response FROM openlegal.provider_request_budget WHERE singleton")
            .fetch_one(&pool).await.unwrap();
        assert_eq!(state, (0, false));
        let current = corpus.load_collection_launch(&old.id).await.unwrap();
        let fresh = client.clone().with_collection_launch(&current).unwrap();
        let mut guard = fresh
            .reserve_request(&CancellationToken::new())
            .await
            .unwrap()
            .unwrap();
        assert!(observer.load(Ordering::Acquire));
        guard.complete().await.unwrap();
        let charged: i64 = sqlx::query_scalar(
            "SELECT on_demand_used FROM openlegal.provider_request_budget WHERE singleton",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(charged, 1);
        storage.close().await.unwrap();
    }

    #[tokio::test]
    #[ignore = "requires scripts/test-postgres.sh"]
    async fn settled_download_failure_allows_next_attempt_but_cancellation_blocks_it() {
        let fixture = crate::test_support::TestDatabase::new().await;
        let store = fixture.open(100).await;
        let pool = store.pool();
        let client = LawClient::new("fixture-credential".into(), Arc::new(UnusedProcessor))
            .unwrap()
            .with_request_budget(pool.clone(), RequestBudgetMode::Pilot);
        let mut reservation = LawClient::reserve_provider_request_budget(
            &pool,
            &RequestBudgetMode::Pilot,
            &CancellationToken::new(),
        )
        .await
        .unwrap();
        assert_eq!(
            client
                .settle_fetch::<()>(
                    Err(DatabaseError::SourceDownloadFailed),
                    Some(&mut reservation)
                )
                .await,
            Err(DatabaseError::SourceDownloadFailed)
        );
        let flags: (i64, bool) = sqlx::query_as(
            "SELECT pilot_used,unresolved_response FROM openlegal.provider_request_budget WHERE singleton",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(flags, (1, false));
        sqlx::query("UPDATE openlegal.provider_request_budget SET next_allowed_at=0")
            .execute(&pool)
            .await
            .unwrap();
        let mut reservation = LawClient::reserve_provider_request_budget(
            &pool,
            &RequestBudgetMode::Pilot,
            &CancellationToken::new(),
        )
        .await
        .unwrap();
        assert_eq!(
            client.settle_fetch(Ok(()), Some(&mut reservation)).await,
            Ok(())
        );
        let flags: (i64, bool) = sqlx::query_as(
            "SELECT pilot_used,unresolved_response FROM openlegal.provider_request_budget WHERE singleton",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(flags, (2, false));
        sqlx::query("UPDATE openlegal.provider_request_budget SET next_allowed_at=0")
            .execute(&pool)
            .await
            .unwrap();
        let mut reservation = LawClient::reserve_provider_request_budget(
            &pool,
            &RequestBudgetMode::Pilot,
            &CancellationToken::new(),
        )
        .await
        .unwrap();
        assert_eq!(
            client
                .settle_fetch::<()>(Err(DatabaseError::Cancelled), Some(&mut reservation))
                .await,
            Err(DatabaseError::Cancelled)
        );
        let owner = reservation.owner();
        drop(reservation);
        let flags: (i64, bool) = sqlx::query_as(
            "SELECT pilot_used,unresolved_response FROM openlegal.provider_request_budget WHERE singleton",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(flags, (3, false));
        // New parallel reservations retain uncertain evidence per owner, while
        // the singleton flag continues to describe legacy/operator fences.
        let unresolved: Vec<uuid::Uuid> =
            sqlx::query_scalar("SELECT owner FROM openlegal.provider_request_admission")
                .fetch_all(&pool)
                .await
                .unwrap();
        assert_eq!(unresolved, vec![owner]);
        assert_eq!(
            LawClient::reserve_provider_request_budget(
                &pool,
                &RequestBudgetMode::Pilot,
                &CancellationToken::new(),
            )
            .await,
            Err(DatabaseError::BudgetExhausted),
        );
        store.close().await.unwrap();
    }
    #[test]
    fn revisioned_inventory_rows_keep_the_provider_amendment_type() {
        let row = |amendment: &str| {
            branch(
                "law",
                vec![
                    field("법령ID", "1"),
                    field("법령일련번호", "100"),
                    field("법령명한글", "Fictional statute"),
                    field("시행일자", "20260101"),
                    field("제개정구분명", amendment),
                ],
            )
        };
        let tree = branch(
            "root",
            vec![field("totalCnt", "2"), row("타법폐지"), row("")],
        );
        let page = parse_inventory_tree(&tree, Dataset::NationalStatute, 1).unwrap();
        assert_eq!(page.items[0].amendment_type.as_deref(), Some("타법폐지"));
        assert_eq!(page.items[1].amendment_type, None);
        let precedent = branch(
            "root",
            vec![
                field("totalCnt", "1"),
                branch(
                    "prec",
                    vec![field("판례일련번호", "100"), field("제개정구분명", "폐지")],
                ),
            ],
        );
        let page = parse_inventory_tree(&precedent, Dataset::Precedent, 1).unwrap();
        assert_eq!(page.items[0].amendment_type, None);
        let mut hint = item();
        hint.amendment_type = Some("폐".repeat(30));
        assert!(hint.validate_for_detail().is_err());
    }
    #[test]
    fn mixed_inventory_keeps_valid_rows_and_marks_page_incomplete() {
        let tree = branch(
            "root",
            vec![
                field("totalCnt", "2"),
                branch(
                    "prec",
                    vec![
                        field("판례일련번호", "100"),
                        field("사건명", "Fictional case"),
                    ],
                ),
                branch(
                    "prec",
                    vec![
                        field("판례일련번호", "invalid"),
                        field("사건명", "Broken case"),
                    ],
                ),
            ],
        );
        let page = parse_inventory_tree(&tree, Dataset::Precedent, 1).unwrap();
        assert_eq!(page.items.len(), 1);
        assert_eq!(page.items[0].object.id, "100");
        assert_eq!(page.rejected_rows, 1);
        assert!(page.incomplete);
        let unknown_total = branch(
            "root",
            vec![
                field("totalCnt", "broken"),
                branch("prec", vec![field("판례일련번호", "100")]),
            ],
        );
        let page = parse_inventory_tree(&unknown_total, Dataset::Precedent, 1).unwrap();
        assert_eq!(page.items.len(), 1);
        assert!(page.incomplete);
        assert!(!page.done);
        let contradictory_total = branch(
            "root",
            vec![
                field("totalCnt", "0"),
                branch("prec", vec![field("판례일련번호", "100")]),
            ],
        );
        let page = parse_inventory_tree(&contradictory_total, Dataset::Precedent, 1).unwrap();
        assert_eq!(page.items.len(), 1);
        assert!(page.incomplete);
    }
    #[tokio::test]
    async fn precedent_case_list_uses_documented_nb_filter_without_title_query() {
        let client =
            LawClient::new("fixture-credential".into(), Arc::new(UnusedProcessor)).unwrap();
        let url = client.precedent_case_search_url("2018도14262", 1).unwrap();
        let pairs: std::collections::BTreeMap<_, _> = url.query_pairs().into_owned().collect();
        assert_eq!(url.path(), "/DRF/lawSearch.do");
        assert_eq!(pairs.get("target").map(String::as_str), Some("prec"));
        assert_eq!(pairs.get("nb").map(String::as_str), Some("2018도14262"));
        assert_eq!(pairs.get("display").map(String::as_str), Some("100"));
        assert!(!pairs.contains_key("query"));
        assert!(
            client
                .precedent_case_search_url("2018도14262,2019도1", 1)
                .is_err()
        );
        assert!(client.precedent_case_search_url("2018도14262", 4).is_err());
    }

    #[test]
    fn precedent_case_list_row_keeps_number_and_serial_for_exact_match() {
        let tree = branch(
            "PrecSearch",
            vec![
                field("totalCnt", "1"),
                branch(
                    "prec",
                    vec![
                        field("판례일련번호", "204234"),
                        field("사건명", "Fictional case"),
                        field("사건번호", "2018도14262"),
                    ],
                ),
            ],
        );
        let page = parse_inventory_tree(&tree, Dataset::Precedent, 1).unwrap();
        assert!(page.done);
        assert!(!page.incomplete);
        assert_eq!(page.items[0].object.id, "204234");
        assert_eq!(page.items[0].revision_id, "204234");
        assert_eq!(page.items[0].case_number.as_deref(), Some("2018도14262"));
    }

    #[test]
    fn precedent_detail_must_match_case_number_from_list() {
        let mut item = item();
        item.object.dataset = Dataset::Precedent;
        item.object.id = "204234".into();
        item.revision_id = "204234".into();
        item.effective_date = None;
        item.case_number = Some("2018도14262".into());
        let detail = |case_number: &str| {
            output(branch(
                "PrecService",
                vec![
                    field("판례정보일련번호", "204234"),
                    field("사건명", "Fictional case"),
                    field("사건번호", case_number),
                    field("판례내용", "Fictional body"),
                ],
            ))
        };
        assert!(project(&item, &detail("2018도14262")).is_ok());
        assert_eq!(
            project(&item, &detail("2018도14263")).unwrap_err(),
            DatabaseError::StorageCorrupt
        );
    }
    #[test]
    fn provider_response_classification_preserves_auth_and_pause_boundaries() {
        use reqwest::StatusCode;
        assert_eq!(
            http_status_error(StatusCode::NOT_FOUND),
            Some(DatabaseError::SourceUnavailable)
        );
        assert_eq!(
            http_status_error(StatusCode::GONE),
            Some(DatabaseError::SourceUnavailable)
        );
        assert_eq!(
            http_status_error(StatusCode::UNAUTHORIZED),
            Some(DatabaseError::SourceUnauthorized)
        );
        assert_eq!(
            http_status_error(StatusCode::FORBIDDEN),
            Some(DatabaseError::SourceUnauthorized)
        );
        assert_eq!(
            http_status_error(StatusCode::INTERNAL_SERVER_ERROR),
            Some(DatabaseError::SourceDownloadFailed)
        );
        for status in [
            StatusCode::REQUEST_TIMEOUT,
            StatusCode::BAD_GATEWAY,
            StatusCode::GATEWAY_TIMEOUT,
        ] {
            assert_eq!(
                http_status_error(status),
                Some(DatabaseError::SourceDownloadFailed)
            );
        }
        assert_eq!(
            http_status_error(StatusCode::SERVICE_UNAVAILABLE),
            Some(DatabaseError::SourceTransient)
        );
        assert_eq!(
            http_status_error(StatusCode::NOT_IMPLEMENTED),
            Some(DatabaseError::SourceTransient)
        );
        assert_eq!(
            document_error(DocumentError::InvalidDocument),
            DatabaseError::SourceDataInvalid
        );
        assert_eq!(
            document_error(DocumentError::SandboxUnavailable),
            DatabaseError::ProcessingPending
        );
    }
    fn field(name: &str, value: &str) -> DocumentNode {
        DocumentNode::Element {
            name: name.into(),
            attributes: vec![],
            children: vec![DocumentNode::Text {
                value: value.into(),
            }],
        }
    }
    fn branch(name: &str, children: Vec<DocumentNode>) -> DocumentNode {
        DocumentNode::Element {
            name: name.into(),
            attributes: vec![],
            children,
        }
    }
    fn item() -> InventoryItem {
        InventoryItem {
            object: ObjectId {
                jurisdiction: "kr".into(),
                provider: "law_go_kr".into(),
                dataset: Dataset::NationalStatute,
                id: "1".into(),
            },
            revision_id: "100:20260101".into(),
            effective_date: Some("20260101".into()),
            publication_date: None,
            title: "Fictional statute".into(),
            data_source: None,
            case_number: None,
            treaty_class_code: None,
            amendment_type: None,
        }
    }
    fn output(tree: DocumentNode) -> DocumentOutput {
        DocumentOutput {
            source_sha256: "a".repeat(64),
            processor_version: "fictional_test_v1".into(),
            format: DocumentFormat::Xml,
            tree: Some(tree),
            text: String::new(),
            pages: vec![],
            ocr_pages: vec![],
            diagnostics: vec![],
        }
    }
    #[test]
    fn undocumented_ministry_row_names_use_direct_serial_fields() {
        let tree = branch(
            "Result",
            vec![
                field("totalCnt", "2"),
                branch(
                    "Results",
                    vec![
                        branch(
                            "provider_row",
                            vec![
                                field("법령해석일련번호", "00100"),
                                field("안건명", "Fictional one"),
                            ],
                        ),
                        branch(
                            "provider_row",
                            vec![
                                field("법령해석일련번호", "00101"),
                                field("안건명", "Fictional two"),
                            ],
                        ),
                    ],
                ),
            ],
        );
        let page = parse_inventory_tree(&tree, Dataset::MoelInterpretation, 1).unwrap();
        assert!(!page.incomplete);
        assert!(page.done);
        assert_eq!(page.items.len(), 2);
        assert_eq!(page.items[0].object.id, "00100");
        assert_eq!(page.items[1].revision_id, "00101");
        assert!(
            page.items
                .iter()
                .all(|item| item.object.dataset == Dataset::MoelInterpretation)
        );
    }
    #[test]
    fn new_families_project_only_documented_fields_and_exact_serials() {
        for (dataset, serial_field, title_field, body_field) in [
            (
                Dataset::MoelInterpretation,
                "법령해석일련번호",
                "안건명",
                "회답",
            ),
            (Dataset::FscDecision, "결정문일련번호", "안건명", "조치내용"),
            (Dataset::OcltDecision, "결정문일련번호", "제목", "판단"),
            (
                Dataset::TtSpecialAppeal,
                "특별행정심판재결례일련번호",
                "사건명",
                "재결요지",
            ),
            (
                Dataset::AuditConsultation,
                "감사원사전컨설팅의견서일련번호",
                "의견서명",
                "종합의견",
            ),
        ] {
            let mut candidate = item();
            candidate.object.dataset = dataset;
            candidate.object.id = "00100".into();
            candidate.revision_id = "00100".into();
            candidate.effective_date = None;
            let data = output(branch(
                "Response",
                vec![
                    field(serial_field, "00100"),
                    field(title_field, "Fictional source"),
                    field(body_field, "Fictional body"),
                    field("unregistered_body", "Must not project"),
                ],
            ));
            let record = project(&candidate, &data).unwrap();
            assert_eq!(record.body, "Fictional body");
            assert_eq!(record.object, candidate.object);
            candidate.revision_id = "00101".into();
            candidate.object.id = "00101".into();
            assert!(matches!(
                project(&candidate, &data),
                Err(DatabaseError::StorageCorrupt)
            ));
        }
    }
    #[test]
    fn institution_inventory_preserves_stable_id_separate_from_revision() {
        let tree = branch(
            "Result",
            vec![
                field("totalCnt", "1"),
                branch(
                    "admrul",
                    vec![
                        field("행정규칙ID", "0007"),
                        field("행정규칙일련번호", "00100"),
                        field("행정규칙명", "Fictional institutional rule"),
                        field("시행일자", "20260101"),
                    ],
                ),
            ],
        );
        let page = parse_inventory_tree(&tree, Dataset::PublicInstitutionRule, 1).unwrap();
        assert!(!page.incomplete);
        assert_eq!(page.items[0].object.id, "0007");
        assert_eq!(page.items[0].revision_id, "00100");
        assert_eq!(page.items[0].effective_date.as_deref(), Some("20260101"));
    }
    #[tokio::test]
    async fn term_detail_uses_name_query_and_rejects_ambiguous_response() {
        let client = LawClient::new("fixture".into(), Arc::new(UnusedProcessor)).unwrap();
        let mut candidate = item();
        candidate.object.dataset = Dataset::LegalTerm;
        candidate.object.id = "100".into();
        candidate.revision_id = "100".into();
        candidate.effective_date = None;
        candidate.title = "가상 용어".into();
        let url = client.detail_url(&candidate).unwrap();
        let pairs: BTreeMap<_, _> = url.query_pairs().into_owned().collect();
        assert_eq!(pairs.get("query").map(String::as_str), Some("가상 용어"));
        assert!(!pairs.contains_key("ID"));
        let mut rows = vec![
            field("법령용어일련번호", "100"),
            field("법령용어명_한글", "가상 용어"),
            field("법령용어정의", "Fictional definition"),
        ];
        assert_eq!(
            project(&candidate, &output(branch("Result", rows.clone())))
                .unwrap()
                .body,
            "Fictional definition"
        );
        rows.push(field("법령용어일련번호", "101"));
        assert!(matches!(
            project(&candidate, &output(branch("Result", rows))),
            Err(DatabaseError::StorageCorrupt)
        ));
    }
    #[tokio::test]
    async fn closed_routes_preserve_history_and_unknown_detail_contracts() {
        let client = LawClient::new("fixture".into(), Arc::new(UnusedProcessor)).unwrap();
        let url = client
            .inventory_url(Dataset::SchoolRule, 2, true, None, None, None)
            .unwrap();
        let pairs: BTreeMap<_, _> = url.query_pairs().into_owned().collect();
        assert_eq!(pairs.get("target").map(String::as_str), Some("school"));
        assert_eq!(pairs.get("nw").map(String::as_str), Some("2"));
        assert_eq!(
            client.inventory_url(Dataset::EnglishStatute, 1, true, None, None, None),
            Err(DatabaseError::UnsupportedHistory)
        );
        let mut candidate = item();
        candidate.object.dataset = Dataset::MoefInterpretation;
        candidate.object.id = "100".into();
        candidate.revision_id = "100".into();
        candidate.effective_date = None;
        assert_eq!(
            client.detail_url(&candidate),
            Err(DatabaseError::SourceUnavailable)
        );
        candidate.object.dataset = Dataset::EnglishStatute;
        candidate.object.id = "1".into();
        let url = client.detail_url(&candidate).unwrap();
        let pairs: BTreeMap<_, _> = url.query_pairs().into_owned().collect();
        assert_eq!(pairs.get("target").map(String::as_str), Some("elaw"));
        assert_eq!(pairs.get("MST").map(String::as_str), Some("100"));
        assert!(matches!(
            project(
                &candidate,
                &output(branch(
                    "Result",
                    vec![
                        field("법령ID", "1"),
                        field("조문내용", "Cannot infer a mapping")
                    ]
                ))
            ),
            Err(DatabaseError::SourceDataInvalid)
        ));
    }
    #[test]
    fn additional_provider_records_keep_exact_identity_and_classification() {
        let cases = [
            (
                Dataset::AdministrativeRule,
                "행정규칙일련번호",
                "행정규칙명",
                "조문내용",
            ),
            (Dataset::Treaty, "조약일련번호", "조약명_한글", "조약내용"),
            (
                Dataset::ConstitutionalDecision,
                "헌재결정례일련번호",
                "사건명",
                "전문",
            ),
            (
                Dataset::LegalInterpretation,
                "법령해석례일련번호",
                "안건명",
                "회답",
            ),
            (
                Dataset::AdministrativeAppeal,
                "행정심판례일련번호",
                "사건명",
                "주문",
            ),
        ];
        for (dataset, number_field, title_field, body_field) in cases {
            let mut i = item();
            i.object.dataset = dataset;
            i.object.id = if dataset == Dataset::AdministrativeRule {
                "1"
            } else {
                "100"
            }
            .into();
            i.revision_id = "100".into();
            i.effective_date = None;
            let mut fields = vec![
                field(number_field, "100"),
                field(title_field, "Fictional record"),
                field(body_field, "Fictional body"),
            ];
            if dataset == Dataset::AdministrativeRule {
                fields.push(field("행정규칙ID", "1"));
            }
            if dataset == Dataset::Treaty {
                fields.push(field("조약구분코드", "440102"));
                i.treaty_class_code = Some("440102".into());
            }
            if dataset == Dataset::ConstitutionalDecision {
                fields.push(field("종국일자", "20260101"));
            }
            if dataset == Dataset::LegalInterpretation {
                fields.push(field("해석일자", "2026"));
            }
            let data = output(branch("Service", fields));
            let record = project(&i, &data).unwrap();
            assert_eq!(record.body, "Fictional body");
            assert!(!record.source_url.contains("OC="));
            assert!(record.source_url.contains("ID=100"));
            if dataset == Dataset::Treaty {
                assert_eq!(record.metadata["document_type"], "multilateral_treaty");
                i.treaty_class_code = Some("440101".into());
                assert!(project(&i, &data).is_err());
            }
            if dataset == Dataset::ConstitutionalDecision {
                assert_eq!(record.metadata["final_disposition_date"], "20260101");
                assert!(!record.metadata.contains_key("judgment_date"));
            }
            if dataset == Dataset::LegalInterpretation {
                assert_eq!(record.metadata["interpretation_date_raw"], "2026");
                assert!(!record.metadata.contains_key("interpretation_date"));
            }
            i.revision_id = "101".into();
            assert!(project(&i, &data).is_err());
        }
    }
    #[test]
    fn additional_detail_rejects_multiple_records_in_one_document() {
        let mut i = item();
        i.object.dataset = Dataset::Treaty;
        i.object.id = "100".into();
        i.revision_id = "100".into();
        i.effective_date = None;
        let data = output(branch(
            "Service",
            vec![
                branch(
                    "Record",
                    vec![
                        field("조약일련번호", "100"),
                        field("조약명_한글", "First"),
                        field("조약내용", "First body"),
                    ],
                ),
                branch(
                    "Record",
                    vec![
                        field("조약일련번호", "101"),
                        field("조약명_한글", "Second"),
                        field("조약내용", "Second body"),
                    ],
                ),
            ],
        ));
        assert_eq!(project(&i, &data), Err(DatabaseError::StorageCorrupt));
    }
    #[test]
    fn national_preserves_ordered_paragraph_subparagraph_and_supplementary_text() {
        let tree = branch(
            "법령",
            vec![
                field("법령ID", "1"),
                field("법령명_한글", "Fictional statute"),
                field("시행일자", "20260101"),
                branch(
                    "조문단위",
                    vec![
                        field("조문내용", "article"),
                        branch(
                            "항",
                            vec![
                                field("항내용", "paragraph"),
                                branch(
                                    "호",
                                    vec![field("호내용", "subparagraph"), field("목내용", "item")],
                                ),
                            ],
                        ),
                    ],
                ),
                field("부칙내용", "supplementary"),
            ],
        );
        let data = output(tree);
        let record = project(&item(), &data).unwrap();
        assert_eq!(
            record.body,
            "article\nparagraph\nsubparagraph\nitem\nsupplementary"
        );
        assert_eq!(record.sections.len(), 2);
        assert!(!record.source_url.contains("OC="));
        assert_eq!(record.metadata["requested_efYd"], "20260101");
        assert!(!record.metadata.contains_key("amendment_type"));
        let mut repealed = item();
        repealed.amendment_type = Some("타법폐지".into());
        let record = project(&repealed, &data).unwrap();
        assert_eq!(record.metadata["amendment_type"], "타법폐지");
        let mut wrong = item();
        wrong.effective_date = Some("20260102".into());
        assert!(project(&wrong, &data).is_err());
        wrong = item();
        wrong.object.id = "2".into();
        assert!(project(&wrong, &data).is_err());
    }
    #[test]
    fn exact_composite_revision_and_safe_documented_attachment_links() {
        let mut i = item();
        assert_eq!(revision_parts(&i).unwrap().0, "100");
        i.revision_id = "100".into();
        assert!(revision_parts(&i).is_err());
        let tree = field(
            "별표서식PDF파일링크",
            "http://www.law.go.kr/LSW/flDownload.do?flSeq=123",
        );
        let links = attachment_links(&tree).unwrap();
        assert_eq!(links[0].url.scheme(), "https");
        assert_eq!(links[0].format, DocumentFormat::Pdf);
        assert!(
            attachment_links(&field(
                "별표서식PDF파일링크",
                "https://evil.invalid/LSW/flDownload.do?flSeq=123"
            ))
            .is_err()
        );
        assert!(
            attachment_links(&field(
                "별표서식PDF파일링크",
                "https://www.law.go.kr/other?flSeq=123"
            ))
            .is_err()
        );
    }
    #[test]
    fn attachment_signatures_distinguish_html_error_from_document_bytes() {
        let busy = b"\xef\xbb\xbf  <!DOCTYPE html><html>fictional busy page</html>";
        assert!(looks_like_html(busy));
        for format in [
            DocumentFormat::Pdf,
            DocumentFormat::Hwp5,
            DocumentFormat::Hwpx,
        ] {
            assert!(!expected_document_magic(busy, format));
        }
        assert!(expected_document_magic(b"%PDF-1.7", DocumentFormat::Pdf));
        assert!(expected_document_magic(
            &[0xd0, 0xcf, 0x11, 0xe0, 0xa1, 0xb1, 0x1a, 0xe1],
            DocumentFormat::Hwp5
        ));
        assert!(expected_document_magic(b"PK\x03\x04", DocumentFormat::Hwpx));
        assert!(!looks_like_html(b"%PDF-1.7"));
    }
    #[test]
    fn terminal_provider_failures_and_typed_judgment_dates() {
        for (status, expected) in [
            (401, DatabaseError::SourceUnauthorized),
            (403, DatabaseError::SourceUnauthorized),
            (404, DatabaseError::SourceUnavailable),
            (302, DatabaseError::SourceRejected),
        ] {
            assert_eq!(
                http_status_error(reqwest::StatusCode::from_u16(status).unwrap()),
                Some(expected)
            );
        }
        assert_eq!(
            http_status_error(reqwest::StatusCode::INTERNAL_SERVER_ERROR),
            Some(DatabaseError::SourceDownloadFailed)
        );
        assert_eq!(
            document_error(DocumentError::Cancelled),
            DatabaseError::Cancelled
        );
        assert_eq!(
            document_error(DocumentError::TimedOut),
            DatabaseError::ProcessingPending
        );
        assert_eq!(
            document_error(DocumentError::UnsupportedFormat),
            DatabaseError::SourceDataInvalid
        );
        let mut i = item();
        i.object.dataset = Dataset::Precedent;
        i.revision_id = "1".into();
        i.effective_date = None;
        for value in ["20260201", "20260230", "2026.02.01"] {
            let tree = branch(
                "PrecService",
                vec![
                    field("판례정보일련번호", "1"),
                    field("사건명", "Fictional"),
                    field("판례내용", "Fictional text"),
                    field("선고일자", value),
                ],
            );
            let r = project(&i, &output(tree)).unwrap();
            assert_eq!(r.metadata["judgment_date_raw"], value);
            assert_eq!(
                r.metadata.contains_key("judgment_date"),
                value == "20260201"
            );
            assert!(r.source_url.contains("type=XML"));
        }
    }
    #[test]
    fn observed_audit_inventory_serial_alias_is_bounded_and_detail_stays_unverified() {
        let tree = branch(
            "BaiPvcsSearch",
            vec![
                field("totalCnt", "1"),
                branch(
                    "baiPvcs",
                    vec![
                        field("감사원사전컨설팅일련번호", "777"),
                        field("의견서명", "Fictional audit consultation"),
                    ],
                ),
            ],
        );
        let page = parse_inventory_tree(&tree, Dataset::AuditConsultation, 1).unwrap();
        assert_eq!(page.items.len(), 1);
        assert_eq!(page.rejected_rows, 0);
        assert!(!page.incomplete);
        assert_eq!(page.items[0].object.id, "777");
        assert_eq!(page.items[0].revision_id, "777");
        let detail = output(branch(
            "BaiPvcsService",
            vec![
                field("감사원사전컨설팅일련번호", "777"),
                field("의견서명", "Fictional audit consultation"),
                field("종합의견", "Fictional original conclusion"),
            ],
        ));
        assert!(project(&page.items[0], &detail).is_err());
    }
    #[test]
    fn ftc_first_documented_decision_form_projects_its_original_text() {
        let mut i = item();
        i.object.dataset = Dataset::FtcDecision;
        i.object.id = "777".into();
        i.revision_id = "777".into();
        i.effective_date = None;
        let out = output(branch(
            "FtcService",
            vec![
                field("결정문일련번호", "777"),
                field("사건명", "Fictional FTC decision"),
                field("주문", "Fictional original order"),
                field("신청취지", "Fictional original request"),
                field("이유", "Fictional original reasons"),
            ],
        ));
        let record = project(&i, &out).unwrap();
        assert_eq!(
            record.body,
            "Fictional original order\nFictional original request\nFictional original reasons"
        );
    }
    #[test]
    fn fictional_html_identity_is_required_and_not_inferred_from_title() {
        let mut i = item();
        i.object.dataset = Dataset::Precedent;
        i.revision_id = "1".into();
        i.effective_date = None;
        i.case_number = Some("fictional-case".into());
        let hidden = DocumentNode::Element {
            name: "input".into(),
            attributes: vec![
                ("name".into(), "precSeq".into()),
                ("value".into(), "1".into()),
            ],
            children: vec![],
        };
        let mut out = output(branch("html", vec![hidden]));
        out.format = DocumentFormat::Html;
        out.text = "Fictional statute fictional-case provider text".into();
        assert!(project(&i, &out).unwrap().source_url.contains("type=HTML"));
        out.tree = Some(branch("html", vec![]));
        assert!(project(&i, &out).is_err());
    }
}
