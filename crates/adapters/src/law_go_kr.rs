//! LAW OPEN DATA transport and evidence-backed field projection. Document parsing
//! is exclusively delegated to the configured disposable document processor.
use crate::{literal_ip, public_address};
use openlegal_application::document::{
    DocumentError, DocumentFormat, DocumentInput, DocumentNode, DocumentOutput, DocumentProcessor,
};
use openlegal_domain::legal::*;
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
        {
            return Err(DatabaseError::InvalidInput);
        }
        Ok(())
    }
}
pub struct ProviderDetail {
    pub retrieved_at: u64,
    pub record: LegalRecord,
    pub raw: Vec<u8>,
    pub additional_evidence: Vec<Vec<u8>>,
    pub processor_version: String,
}
#[derive(Clone, Debug)]
pub struct InventoryPage {
    pub items: Vec<InventoryItem>,
    pub done: bool,
    pub total: Option<u64>,
    pub rejected_rows: usize,
    pub incomplete: bool,
}

enum FetchedDocument<T> {
    Processed(T),
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
    proxy: Option<crate::upstream_proxy::Socks5Proxy>,
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
    continuous_daily_limit: u32,
    on_demand_daily_limit: u32,
    min_interval_secs: u32,
}
impl ProviderRequestLimits {
    pub fn new(
        continuous_daily_limit: u32,
        on_demand_daily_limit: u32,
        min_interval_secs: u32,
    ) -> Result<Self, DatabaseError> {
        if !(1..=1_000_000).contains(&continuous_daily_limit)
            || !(1..=1_000_000).contains(&on_demand_daily_limit)
            || !(1..=3600).contains(&min_interval_secs)
        {
            return Err(DatabaseError::InvalidInput);
        }
        Ok(Self {
            continuous_daily_limit,
            on_demand_daily_limit,
            min_interval_secs,
        })
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
        let row = sqlx::query("SELECT utc_day,daily_used,on_demand_used,continuous_daily_limit,on_demand_daily_limit,min_interval_secs,next_allowed_at,next_request_at_ms,operator_suspended,unresolved_response,floor(extract(epoch from clock_timestamp()))::bigint AS now FROM openlegal.provider_request_budget WHERE singleton FOR UPDATE")
            .fetch_one(&mut *tx).await.map_err(|_| DatabaseError::StorageUnavailable)?;
        let get = |name| {
            row.try_get::<i64, _>(name)
                .map_err(|_| DatabaseError::StorageUnavailable)
        };
        let get_count = |name| {
            row.try_get::<i32, _>(name)
                .map_err(|_| DatabaseError::StorageUnavailable)
        };
        let now = get("now")?;
        let current_day = get("utc_day")? == now / 86_400;
        let old_continuous = get_count("continuous_daily_limit")?;
        let old_on_demand = get_count("on_demand_daily_limit")?;
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
        let continuous = i32::try_from(limits.continuous_daily_limit)
            .map_err(|_| DatabaseError::InvalidInput)?;
        let on_demand =
            i32::try_from(limits.on_demand_daily_limit).map_err(|_| DatabaseError::InvalidInput)?;
        let wake_continuous = continuous > old_continuous
            && continuous_used >= old_continuous
            && continuous_used < continuous;
        let wake_on_demand = on_demand > old_on_demand
            && on_demand_used >= old_on_demand
            && on_demand_used < on_demand;
        let paused = row
            .try_get::<bool, _>("operator_suspended")
            .map_err(|_| DatabaseError::StorageUnavailable)?
            || row
                .try_get::<bool, _>("unresolved_response")
                .map_err(|_| DatabaseError::StorageUnavailable)?;
        sqlx::query("UPDATE openlegal.provider_request_budget SET continuous_daily_limit=$1,on_demand_daily_limit=$2,min_interval_secs=$3 WHERE singleton")
            .bind(continuous).bind(on_demand).bind(limits.min_interval_secs as i32)
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
            proxy: None,
        })
    }
    /// Covers inventory, details, NTS HTML and linked attachments, including clones
    /// used by explicit collection. Provider admission and destination checks remain.
    pub fn with_socks5_proxy(mut self, proxy: crate::upstream_proxy::Socks5Proxy) -> Self {
        self.proxy = Some(proxy);
        self
    }
    /// The database migration must be applied before an enabled client starts.
    /// Every outbound attempt reserves its allowance before DNS resolution.
    pub fn with_request_budget(mut self, pool: PgPool, mode: RequestBudgetMode) -> Self {
        self.budget = Some((pool, mode));
        self
    }
    pub fn with_local_cap(mut self, attempts: u32) -> Self {
        self.local_cap = Some(Arc::new(AtomicU32::new(attempts)));
        self
    }
    pub fn on_demand_client(&self) -> Result<Self, DatabaseError> {
        let (pool, _) = self.budget.as_ref().ok_or(DatabaseError::InvalidInput)?;
        Ok(self
            .clone()
            .with_request_budget(pool.clone(), RequestBudgetMode::OnDemand)
            .with_local_cap(32))
    }
    async fn reserve_request(&self, cancel: &CancellationToken) -> Result<(), DatabaseError> {
        if self.operator_suspended.load(Ordering::Acquire) {
            return Err(DatabaseError::BudgetExhausted);
        }
        if let Some(cap) = &self.local_cap {
            cap.fetch_update(Ordering::AcqRel, Ordering::Acquire, |left| {
                left.checked_sub(1)
            })
            .map_err(|_| DatabaseError::BudgetExhausted)?;
        }
        let Some((pool, mode)) = &self.budget else {
            return Ok(());
        };
        Self::reserve_provider_request_budget(pool, mode, cancel).await
    }
    async fn pause_provider_requests(&self, delay: u64) -> Result<(), DatabaseError> {
        let suspended = delay > 7 * 86_400;
        self.operator_suspended.store(true, Ordering::Release);
        *self
            .next_request
            .lock()
            .map_err(|_| DatabaseError::StorageUnavailable)? =
            Instant::now().checked_add(Duration::from_secs(delay.min(7 * 86_400)));
        if let Some((pool, _)) = &self.budget {
            let durable_delay = i64::try_from(delay)
                .unwrap_or(i64::MAX / 4)
                .min(i64::MAX / 4);
            sqlx::query("UPDATE openlegal.provider_request_budget SET next_allowed_at=GREATEST(next_allowed_at, floor(extract(epoch from clock_timestamp()))::bigint + $1),operator_suspended=operator_suspended OR $2,unresolved_response=false WHERE singleton")
                .bind(durable_delay).bind(suspended)
                .execute(pool).await.map_err(|_| DatabaseError::StorageUnavailable)?;
        }
        self.operator_suspended.store(suspended, Ordering::Release);
        Ok(())
    }
    /// A deterministic rejection may also be an invalid or revoked credential.
    /// A pilot must not repeat it after a process restart without operator review.
    pub async fn suspend_after_source_rejection(&self) -> Result<(), DatabaseError> {
        self.operator_suspended.store(true, Ordering::Release);
        if let Some((pool, _)) = &self.budget {
            let updated = sqlx::query(
                "UPDATE openlegal.provider_request_budget SET operator_suspended=true,unresolved_response=false WHERE singleton",
            )
            .execute(pool)
            .await
            .map_err(|_| DatabaseError::StorageUnavailable)?;
            if updated.rows_affected() != 1 {
                return Err(DatabaseError::StorageUnavailable);
            }
        }
        Ok(())
    }
    async fn complete_request(&self) -> Result<(), DatabaseError> {
        if let Some((pool, _)) = &self.budget
            && sqlx::query("UPDATE openlegal.provider_request_budget SET unresolved_response=false WHERE singleton")
                .execute(pool).await.is_err() {
            self.operator_suspended.store(true, Ordering::Release);
            return Err(DatabaseError::StorageUnavailable);
        }
        Ok(())
    }
    async fn settle_fetch<T>(&self, fetched: Result<T, DatabaseError>) -> Result<T, DatabaseError> {
        match fetched {
            Ok(value) => {
                // The complete response is now local evidence. Document work
                // may time out independently of provider admission.
                self.complete_request().await?;
                Ok(value)
            }
            Err(error @ (DatabaseError::SourceRejected | DatabaseError::SourceUnauthorized)) => {
                self.suspend_after_source_rejection().await?;
                Err(error)
            }
            Err(error @ (DatabaseError::Cancelled | DatabaseError::StorageUnavailable)) => {
                // A reserved attempt might have been sent. Cancellation and
                // storage failure retain the marker for operator review.
                Err(error)
            }
            Err(error) => {
                // A bounded download failure spends its reserved GET attempt.
                // The caller records an incomplete page, detail, or attachment.
                if !self.operator_suspended.load(Ordering::Acquire) {
                    self.complete_request().await?;
                }
                Err(error)
            }
        }
    }
    /// A transient pause is durable for configured ingestion; the job worker
    /// uses this timestamp without burning another attempt while it waits.
    pub async fn next_admissible_epoch(&self) -> Result<u64, DatabaseError> {
        let Some((pool, mode)) = &self.budget else {
            return Err(DatabaseError::InvalidInput);
        };
        let row = sqlx::query("SELECT utc_day,daily_used,on_demand_used,continuous_daily_limit,on_demand_daily_limit,next_allowed_at,next_request_at_ms,operator_suspended,unresolved_response,floor(extract(epoch from clock_timestamp()))::bigint AS now FROM openlegal.provider_request_budget WHERE singleton")
            .fetch_one(pool).await.map_err(|_| DatabaseError::StorageUnavailable)?;
        let now: i64 = row
            .try_get("now")
            .map_err(|_| DatabaseError::StorageUnavailable)?;
        let day = now / 86_400;
        let used: i32 = row
            .try_get(if *mode == RequestBudgetMode::OnDemand {
                "on_demand_used"
            } else {
                "daily_used"
            })
            .map_err(|_| DatabaseError::StorageUnavailable)?;
        let stored_day: i64 = row
            .try_get("utc_day")
            .map_err(|_| DatabaseError::StorageUnavailable)?;
        let next: i64 = row
            .try_get("next_allowed_at")
            .map_err(|_| DatabaseError::StorageUnavailable)?;
        if row
            .try_get::<bool, _>("operator_suspended")
            .map_err(|_| DatabaseError::StorageUnavailable)?
            || row
                .try_get::<bool, _>("unresolved_response")
                .map_err(|_| DatabaseError::StorageUnavailable)?
        {
            return u64::try_from(now.saturating_add(3600))
                .map_err(|_| DatabaseError::StorageUnavailable);
        }
        let limit: i32 = row
            .try_get(if *mode == RequestBudgetMode::OnDemand {
                "on_demand_daily_limit"
            } else {
                "continuous_daily_limit"
            })
            .map_err(|_| DatabaseError::StorageUnavailable)?;
        let spacing: i64 = row
            .try_get("next_request_at_ms")
            .map_err(|_| DatabaseError::StorageUnavailable)?;
        let next = next.max(spacing / 1000 + i64::from(spacing % 1000 != 0));
        let daily = if stored_day == day && used >= limit {
            (day + 1) * 86_400 + 10
        } else {
            now + 10
        };
        u64::try_from(next.max(daily)).map_err(|_| DatabaseError::StorageUnavailable)
    }
    /// Reserve a provider attempt without transmitting it. Exposed for the
    /// explicit PostgreSQL integration gate; callers must not split one
    /// outbound attempt into multiple reservations.
    pub async fn reserve_provider_request_budget(
        pool: &PgPool,
        mode: &RequestBudgetMode,
        cancel: &CancellationToken,
    ) -> Result<(), DatabaseError> {
        loop {
            if cancel.is_cancelled() {
                return Err(DatabaseError::Cancelled);
            }
            let mut tx = pool
                .begin()
                .await
                .map_err(|_| DatabaseError::StorageUnavailable)?;
            let row = sqlx::query("SELECT utc_day,daily_used,on_demand_used,continuous_daily_limit,on_demand_daily_limit,min_interval_secs,next_allowed_at,next_request_at_ms,operator_suspended,unresolved_response,pilot_started_at,pilot_used,floor(extract(epoch from clock_timestamp())*1000)::bigint AS now_ms FROM openlegal.provider_request_budget WHERE singleton FOR UPDATE")
                .fetch_one(&mut *tx).await.map_err(|_| DatabaseError::StorageUnavailable)?;
            let now_ms: i64 = row
                .try_get("now_ms")
                .map_err(|_| DatabaseError::StorageUnavailable)?;
            let now = now_ms / 1000;
            let day = now / 86_400;
            let previous_day: i64 = row
                .try_get("utc_day")
                .map_err(|_| DatabaseError::StorageUnavailable)?;
            let daily_used: i32 = if previous_day == day {
                row.try_get("daily_used")
                    .map_err(|_| DatabaseError::StorageUnavailable)?
            } else {
                0
            };
            let on_demand_used: i32 = if previous_day == day {
                row.try_get("on_demand_used")
                    .map_err(|_| DatabaseError::StorageUnavailable)?
            } else {
                0
            };
            let next: i64 = row
                .try_get("next_allowed_at")
                .map_err(|_| DatabaseError::StorageUnavailable)?;
            let spacing: i64 = row
                .try_get("next_request_at_ms")
                .map_err(|_| DatabaseError::StorageUnavailable)?;
            let next = next.saturating_mul(1000).max(spacing);
            let interval: i32 = row
                .try_get("min_interval_secs")
                .map_err(|_| DatabaseError::StorageUnavailable)?;
            let limit: i32 = row
                .try_get(if *mode == RequestBudgetMode::OnDemand {
                    "on_demand_daily_limit"
                } else {
                    "continuous_daily_limit"
                })
                .map_err(|_| DatabaseError::StorageUnavailable)?;
            let pilot_started: Option<i64> = row
                .try_get("pilot_started_at")
                .map_err(|_| DatabaseError::StorageUnavailable)?;
            let pilot_used: i32 = row
                .try_get("pilot_used")
                .map_err(|_| DatabaseError::StorageUnavailable)?;
            if row
                .try_get::<bool, _>("operator_suspended")
                .map_err(|_| DatabaseError::StorageUnavailable)?
                || row
                    .try_get::<bool, _>("unresolved_response")
                    .map_err(|_| DatabaseError::StorageUnavailable)?
                || (if *mode == RequestBudgetMode::OnDemand {
                    on_demand_used
                } else {
                    daily_used
                }) >= limit
                || (*mode == RequestBudgetMode::Pilot
                    && (pilot_used >= 100
                        || pilot_started
                            .is_some_and(|started| now.saturating_sub(started) >= 1800)))
            {
                return Err(DatabaseError::BudgetExhausted);
            }
            if next > now_ms {
                drop(tx);
                if next - now_ms > 30_000 {
                    return Err(DatabaseError::BudgetExhausted);
                }
                let delay = (next - now_ms).min(30_000) as u64;
                tokio::select! {_ = cancel.cancelled() => return Err(DatabaseError::Cancelled), _ = tokio::time::sleep(Duration::from_millis(delay)) => {}}
                continue;
            }
            sqlx::query("UPDATE openlegal.provider_request_budget SET utc_day=$1,daily_used=$2,on_demand_used=$3,next_request_at_ms=$4,unresolved_response=true,pilot_started_at=CASE WHEN $5 THEN COALESCE(pilot_started_at,$6) ELSE pilot_started_at END,pilot_used=pilot_used+CASE WHEN $5 THEN 1 ELSE 0 END WHERE singleton")
                .bind(day)
                .bind(daily_used + i32::from(*mode != RequestBudgetMode::OnDemand))
                .bind(on_demand_used + i32::from(*mode == RequestBudgetMode::OnDemand))
                .bind(now_ms.saturating_add(i64::from(interval) * 1000 + 1))
                .bind(*mode == RequestBudgetMode::Pilot).bind(now)
                .execute(&mut *tx).await.map_err(|_| DatabaseError::StorageUnavailable)?;
            tx.commit()
                .await
                .map_err(|_| DatabaseError::StorageUnavailable)?;
            return Ok(());
        }
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
        if treaty_class.is_some_and(|c| dataset != Dataset::Treaty || !matches!(c, 1 | 2)) {
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
                dataset,
                Dataset::NationalStatute | Dataset::Ordinance | Dataset::AdministrativeRule
            )
        {
            return Err(DatabaseError::UnsupportedHistory);
        }
        if object_id.is_some() && dataset != Dataset::NationalStatute {
            return Err(DatabaseError::UnsupportedHistory);
        }
        let target = match dataset {
            Dataset::NationalStatute => "eflaw",
            Dataset::AdministrativeRule => "admrul",
            Dataset::Ordinance => "ordin",
            Dataset::Treaty => "trty",
            Dataset::Precedent => "prec",
            Dataset::ConstitutionalDecision => "detc",
            Dataset::LegalInterpretation => "expc",
            Dataset::AdministrativeAppeal => "decc",
        };
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
            Dataset::Ordinance | Dataset::AdministrativeRule => {
                url.query_pairs_mut()
                    .append_pair("nw", if historical { "2" } else { "1" });
            }
            _ => {}
        }
        let (parsed, _) = self
            .fetch_parse(url, DocumentFormat::Xml, false, cancel)
            .await?;
        let tree = parsed
            .tree
            .as_ref()
            .ok_or(DatabaseError::SourceDataInvalid)?;
        parse_inventory_tree(tree, dataset, page)
    }
    pub async fn detail(
        &self,
        item: &InventoryItem,
        cancel: CancellationToken,
    ) -> Result<ProviderDetail, DatabaseError> {
        item.object.validate()?;
        let (master, effective) = revision_parts(item)?;
        let target = target(item.object.dataset);
        let mut url = self.api("lawService.do", target)?;
        if matches!(
            item.object.dataset,
            Dataset::Precedent
                | Dataset::Treaty
                | Dataset::ConstitutionalDecision
                | Dataset::LegalInterpretation
                | Dataset::AdministrativeAppeal
                | Dataset::AdministrativeRule
        ) {
            url.query_pairs_mut().append_pair("ID", &master);
        } else {
            url.query_pairs_mut().append_pair("MST", &master);
        }
        if let Some(date) = effective {
            url.query_pairs_mut()
                .append_pair("efYd", &date)
                .append_pair("chrClsCd", "010201");
        }
        if item.object.dataset == Dataset::Treaty {
            url.query_pairs_mut().append_pair("chrClsCd", "010202");
        }
        let html = item.object.dataset == Dataset::Precedent
            && item
                .data_source
                .as_deref()
                .is_some_and(|s| s == "국세법령정보시스템" || s == "국세청");
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
                    let record = project(item, &output)?;
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
        let mut additional_evidence = Vec::new();
        let mut evidence_ordinals = Vec::new();
        let mut total = raw.len();
        let mut extracted = 0usize;
        let expected_count = links.len();
        let mut missing = Vec::new();
        for (ordinal, link) in links.into_iter().enumerate() {
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
        record
            .validate()
            .map_err(|_| DatabaseError::SourceDataInvalid)?;
        Ok(ProviderDetail {
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
    /// Run source-dependent checks before releasing the one-request admission
    /// permit and finalizing the durable response state.
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
            .fetch_parse_timed_checked_inner(url, format, ocr, cancel, None, check)
            .await?
        {
            FetchedDocument::Processed(value) => Ok(value),
            FetchedDocument::UnexpectedAttachment { .. } => Err(DatabaseError::StorageCorrupt),
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
            check,
        )
        .await
    }
    async fn fetch_parse_timed_checked_inner<T, F>(
        &self,
        url: Url,
        format: DocumentFormat,
        ocr: bool,
        cancel: CancellationToken,
        attachment_remaining_bytes: Option<usize>,
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
        let permit = tokio::select! {_=cancel.cancelled()=>return Err(DatabaseError::Cancelled),p=tokio::time::timeout(Duration::from_secs(30),self.admission.clone().acquire_owned())=>p.map_err(|_|DatabaseError::Capacity)?.map_err(|_|DatabaseError::Capacity)?};
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
        self.reserve_request(&cancel).await?;
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
                self.pause_provider_requests(delay).await?;
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
        let (raw, html_content_type) = self.settle_fetch(fetched).await?;
        let retrieved_at = self.clock.now();
        if attachment_remaining_bytes.is_some() && !expected_document_magic(&raw, format) {
            drop(permit);
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
            .map_err(document_error);
        let output = processed
            .and_then(|output| {
                check(output, raw.clone(), retrieved_at).map_err(|error| {
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
                self.suspend_after_source_rejection().await?;
                return output;
            }
            Err(DatabaseError::Cancelled) => return Err(DatabaseError::Cancelled),
            _ => {}
        }
        drop(permit);
        output
    }
}
fn parse_inventory_tree(
    tree: &DocumentNode,
    dataset: Dataset,
    page: u32,
) -> Result<InventoryPage, DatabaseError> {
    let item_name = match dataset {
        Dataset::NationalStatute => "law",
        Dataset::AdministrativeRule => "admrul",
        Dataset::Ordinance => "law",
        Dataset::Treaty => "trty",
        Dataset::Precedent => "prec",
        Dataset::ConstitutionalDecision => "detc",
        Dataset::LegalInterpretation => "expc",
        Dataset::AdministrativeAppeal => "decc",
    };
    let mut nodes = Vec::new();
    elements(tree, item_name, &mut nodes);
    // Ordinance feeds use either `law` or `ordin` record elements; exact ID fields remain mandatory.
    if nodes.is_empty() && dataset == Dataset::Ordinance {
        elements(tree, "ordin", &mut nodes);
    }
    let observed_rows = nodes.len();
    let mut items = Vec::new();
    let mut rejected_rows = 0usize;
    for node in nodes {
        let parsed = (|| -> Result<Option<InventoryItem>, DatabaseError> {
            let idfield = match dataset {
                Dataset::NationalStatute => "법령ID",
                Dataset::AdministrativeRule => "행정규칙ID",
                Dataset::Ordinance => "자치법규ID",
                Dataset::Treaty => "조약일련번호",
                Dataset::Precedent => "판례일련번호",
                Dataset::ConstitutionalDecision => "헌재결정례일련번호",
                Dataset::LegalInterpretation => "법령해석례일련번호",
                Dataset::AdministrativeAppeal => "행정심판재결례일련번호",
            };
            let revfield = match dataset {
                Dataset::NationalStatute => "법령일련번호",
                Dataset::AdministrativeRule => "행정규칙일련번호",
                Dataset::Ordinance => "자치법규일련번호",
                Dataset::Treaty => "조약일련번호",
                Dataset::Precedent => "판례일련번호",
                Dataset::ConstitutionalDecision => "헌재결정례일련번호",
                Dataset::LegalInterpretation => "법령해석례일련번호",
                Dataset::AdministrativeAppeal => "행정심판재결례일련번호",
            };
            let id = first(node, idfield).ok_or(DatabaseError::StorageCorrupt)?;
            let master = first(node, revfield).ok_or(DatabaseError::StorageCorrupt)?;
            if dataset == Dataset::AdministrativeAppeal && (master == "0" || id == "0") {
                // This list includes placeholder rows without a usable detail ID.
                return Ok(None);
            }
            if !numeric_id(&master) || !numeric_id(&id) || master == "0" || id == "0" {
                return Err(DatabaseError::StorageCorrupt);
            }
            let title = first(
                node,
                match dataset {
                    Dataset::NationalStatute => "법령명한글",
                    Dataset::AdministrativeRule => "행정규칙명",
                    Dataset::Ordinance => "자치법규명",
                    Dataset::Treaty => "조약명",
                    Dataset::Precedent => "사건명",
                    Dataset::ConstitutionalDecision => "사건명",
                    Dataset::LegalInterpretation => "안건명",
                    Dataset::AdministrativeAppeal => "사건명",
                },
            )
            .unwrap_or_default();
            let object = ObjectId {
                jurisdiction: "kr".into(),
                provider: "law_go_kr".into(),
                dataset,
                id,
            };
            object.validate()?;
            let effective_date = match dataset {
                Dataset::NationalStatute | Dataset::AdministrativeRule | Dataset::Ordinance => {
                    date(first(node, "시행일자"))?
                }
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
                    Dataset::NationalStatute | Dataset::Ordinance
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
struct AttachmentLink {
    url: Url,
    format: DocumentFormat,
    title: String,
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
    match dataset {
        Dataset::NationalStatute => "eflaw",
        Dataset::AdministrativeRule => "admrul",
        Dataset::Ordinance => "ordin",
        Dataset::Treaty => "trty",
        Dataset::Precedent => "prec",
        Dataset::ConstitutionalDecision => "detc",
        Dataset::LegalInterpretation => "expc",
        Dataset::AdministrativeAppeal => "decc",
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
            | "별표내용"
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
pub fn project(
    item: &InventoryItem,
    output: &DocumentOutput,
) -> Result<LegalRecord, DatabaseError> {
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
    let (number_field, title_field) = match item.object.dataset {
        Dataset::AdministrativeRule => ("행정규칙일련번호", "행정규칙명"),
        Dataset::Treaty => ("조약일련번호", "조약명_한글"),
        Dataset::ConstitutionalDecision => ("헌재결정례일련번호", "사건명"),
        Dataset::LegalInterpretation => ("법령해석례일련번호", "안건명"),
        Dataset::AdministrativeAppeal => ("행정심판례일련번호", "사건명"),
        _ => return Err(DatabaseError::InvalidInput),
    };
    let mut serials = Vec::new();
    elements(tree, number_field, &mut serials);
    if serials.len() != 1 || first(tree, number_field).as_deref() != Some(number.as_str()) {
        return Err(DatabaseError::StorageCorrupt);
    }
    if item.object.dataset == Dataset::AdministrativeRule {
        let mut ids = Vec::new();
        elements(tree, "행정규칙ID", &mut ids);
        if ids.len() != 1 || first(tree, "행정규칙ID").as_deref() != Some(item.object.id.as_str())
        {
            return Err(DatabaseError::StorageCorrupt);
        }
    }
    let title = first(tree, title_field)
        .filter(|s| !s.is_empty())
        .ok_or(DatabaseError::StorageCorrupt)?;
    let mut source_sections = Vec::new();
    sections(tree, &mut source_sections);
    if source_sections.is_empty() {
        return Err(DatabaseError::StorageCorrupt);
    }
    let mut metadata = BTreeMap::new();
    metadata.insert(
        "projection_version".into(),
        "law_go_kr_additional_v1".into(),
    );
    metadata.insert("provider_record_number".into(), number.clone());
    metadata.insert("section_locator_semantics".into(), "source_ordinal".into());
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
        Dataset::AdministrativeRule => date(first(tree, "시행일자"))?,
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
        .append_pair("ID", &number);
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

    #[tokio::test]
    #[ignore = "requires scripts/test-postgres.sh"]
    async fn settled_download_failure_allows_next_attempt_but_cancellation_blocks_it() {
        let fixture = crate::test_support::TestDatabase::new().await;
        let store = fixture.open(100).await;
        let pool = store.pool();
        let client = LawClient::new("fixture-credential".into(), Arc::new(UnusedProcessor))
            .unwrap()
            .with_request_budget(pool.clone(), RequestBudgetMode::Pilot);
        LawClient::reserve_provider_request_budget(
            &pool,
            &RequestBudgetMode::Pilot,
            &CancellationToken::new(),
        )
        .await
        .unwrap();
        assert_eq!(
            client
                .settle_fetch::<()>(Err(DatabaseError::SourceDownloadFailed))
                .await,
            Err(DatabaseError::SourceDownloadFailed)
        );
        let flags: (i32, bool) = sqlx::query_as(
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
        LawClient::reserve_provider_request_budget(
            &pool,
            &RequestBudgetMode::Pilot,
            &CancellationToken::new(),
        )
        .await
        .unwrap();
        assert_eq!(client.settle_fetch(Ok(())).await, Ok(()));
        let flags: (i32, bool) = sqlx::query_as(
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
        LawClient::reserve_provider_request_budget(
            &pool,
            &RequestBudgetMode::Pilot,
            &CancellationToken::new(),
        )
        .await
        .unwrap();
        assert_eq!(
            client
                .settle_fetch::<()>(Err(DatabaseError::Cancelled))
                .await,
            Err(DatabaseError::Cancelled)
        );
        let flags: (i32, bool) = sqlx::query_as(
            "SELECT pilot_used,unresolved_response FROM openlegal.provider_request_budget WHERE singleton",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(flags, (3, true));
        store.close().await.unwrap();
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
