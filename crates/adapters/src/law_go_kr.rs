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
        atomic::{AtomicBool, Ordering},
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
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RequestBudgetMode {
    Pilot,
    Continuous,
}
impl LawClient {
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
        })
    }
    /// The database migration must be applied before an enabled client starts.
    /// Every outbound attempt reserves its allowance before DNS resolution.
    pub fn with_request_budget(mut self, pool: PgPool, mode: RequestBudgetMode) -> Self {
        self.budget = Some((pool, mode));
        self
    }
    async fn reserve_request(&self, cancel: &CancellationToken) -> Result<(), DatabaseError> {
        if self.operator_suspended.load(Ordering::Acquire) {
            return Err(DatabaseError::BudgetExhausted);
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
    /// A transient pause is durable for configured ingestion; the job worker
    /// uses this timestamp without burning another attempt while it waits.
    pub async fn next_admissible_epoch(&self) -> Result<u64, DatabaseError> {
        let Some((pool, _)) = &self.budget else {
            return Err(DatabaseError::InvalidInput);
        };
        let row = sqlx::query("SELECT utc_day,daily_used,next_allowed_at,operator_suspended,unresolved_response,floor(extract(epoch from clock_timestamp()))::bigint AS now FROM openlegal.provider_request_budget WHERE singleton")
            .fetch_one(pool).await.map_err(|_| DatabaseError::StorageUnavailable)?;
        let now: i64 = row
            .try_get("now")
            .map_err(|_| DatabaseError::StorageUnavailable)?;
        let day = now / 86_400;
        let used: i32 = row
            .try_get("daily_used")
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
        let daily = if stored_day == day && used >= 1000 {
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
            let row = sqlx::query("SELECT utc_day,daily_used,next_allowed_at,operator_suspended,unresolved_response,pilot_started_at,pilot_used,floor(extract(epoch from clock_timestamp()))::bigint AS now FROM openlegal.provider_request_budget WHERE singleton FOR UPDATE")
                .fetch_one(&mut *tx).await.map_err(|_| DatabaseError::StorageUnavailable)?;
            let now: i64 = row
                .try_get("now")
                .map_err(|_| DatabaseError::StorageUnavailable)?;
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
            let next: i64 = row
                .try_get("next_allowed_at")
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
                || daily_used >= 1000
                || (*mode == RequestBudgetMode::Pilot
                    && (pilot_used >= 100
                        || pilot_started
                            .is_some_and(|started| now.saturating_sub(started) >= 1800)))
            {
                return Err(DatabaseError::BudgetExhausted);
            }
            if next > now {
                drop(tx);
                if next - now > 30 {
                    return Err(DatabaseError::BudgetExhausted);
                }
                let delay = (next - now).min(30) as u64;
                tokio::select! {_ = cancel.cancelled() => return Err(DatabaseError::Cancelled), _ = tokio::time::sleep(Duration::from_secs(delay)) => {}}
                continue;
            }
            sqlx::query("UPDATE openlegal.provider_request_budget SET utc_day=$1,daily_used=$2,next_allowed_at=$3,unresolved_response=true,pilot_started_at=CASE WHEN $4 THEN COALESCE(pilot_started_at,$5) ELSE pilot_started_at END,pilot_used=pilot_used+CASE WHEN $4 THEN 1 ELSE 0 END WHERE singleton")
                .bind(day).bind(daily_used + 1).bind(now.saturating_add(6))
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
            .map(|(items, done, _)| (items, done))
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
            .map(|(items, done, _)| (items, done))
    }
    pub async fn inventory_page(
        &self,
        dataset: Dataset,
        page: u32,
        historical: bool,
        object_id: Option<&str>,
        cancel: CancellationToken,
    ) -> Result<(Vec<InventoryItem>, bool, u64), DatabaseError> {
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
    ) -> Result<(Vec<InventoryItem>, bool, u64), DatabaseError> {
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
        let tree = parsed.tree.as_ref().ok_or(DatabaseError::StorageCorrupt)?;
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
        let mut items = Vec::new();
        for node in nodes {
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
                continue;
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
            items.push(InventoryItem {
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
            });
        }
        let total = first(tree, "totalCnt")
            .and_then(|v| v.parse::<u64>().ok())
            .ok_or(DatabaseError::StorageCorrupt)?;
        if items.len() > 100
            || (dataset != Dataset::AdministrativeAppeal
                && items.is_empty()
                && (page as u64 - 1) * 100 < total)
        {
            return Err(DatabaseError::StorageCorrupt);
        }
        Ok((items, (page as u64) * 100 >= total, total))
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
                    record.validate()?;
                    let links = attachment_links(
                        output.tree.as_ref().ok_or(DatabaseError::StorageCorrupt)?,
                    )?;
                    Ok((record, links, raw, retrieved_at, output.processor_version))
                },
            )
            .await?;
        let mut additional_evidence = Vec::new();
        let mut total = raw.len();
        let mut extracted = 0usize;
        for (ordinal, link) in links.into_iter().enumerate() {
            let bytes = self
                .fetch_parse_timed_checked(
                    link.url,
                    link.format,
                    true,
                    cancel.clone(),
                    |attachment, bytes, _| {
                        total = total
                            .checked_add(bytes.len())
                            .ok_or(DatabaseError::SourceRejected)?;
                        if total > 100 * 1024 * 1024 {
                            return Err(DatabaseError::SourceRejected);
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
                                    .ok_or(DatabaseError::SourceRejected)?;
                                if extracted > 16 * 1024 * 1024 {
                                    return Err(DatabaseError::SourceRejected);
                                }
                                let label = if kind == SectionKind::Ocr {
                                    "ocr"
                                } else {
                                    "extracted"
                                };
                                record.sections.push(LegalSection {
                                    id: format!("attachment:{}:{label}:{}", ordinal + 1, page.page),
                                    title: link.title.clone(),
                                    text: page.text,
                                    kind: kind.clone(),
                                    source_document_sha256: Some(digest.clone()),
                                    page: Some(
                                        page.page
                                            .try_into()
                                            .map_err(|_| DatabaseError::SourceRejected)?,
                                    ),
                                });
                            }
                        }
                        record.validate()?;
                        Ok(bytes)
                    },
                )
                .await?;
            additional_evidence.push(bytes);
        }
        record.validate()?;
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
            Instant::now().checked_add(Duration::from_secs(if self.budget.is_some() {
                5
            } else {
                1
            }));
        self.reserve_request(&cancel).await?;
        let fetched = async {
        let host = url.host_str().ok_or(DatabaseError::InvalidInput)?;
        let ips = tokio::select! {_=cancel.cancelled()=>return Err(DatabaseError::Cancelled),r=self.resolver.lookup_ip(format!("{host}."))=>r.map_err(|_|DatabaseError::StorageUnavailable)?};
        let mut addresses = Vec::new();
        for ip in ips.iter() {
            if !public_address(ip) || addresses.len() >= 16 {
                return Err(DatabaseError::InvalidInput);
            }
            addresses.push(SocketAddr::new(ip, 443));
        }
        if addresses.is_empty() {
            return Err(DatabaseError::StorageUnavailable);
        }
        let client = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(30))
            .resolve_to_addrs(host, &addresses)
            .build()
            .map_err(|_| DatabaseError::StorageUnavailable)?;
        let fetch = async {
            let mut response = client
                .get(url)
                .header("accept-encoding", "identity")
                .send()
                .await
                .map_err(|_| DatabaseError::StorageUnavailable)?;
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
                return Err(DatabaseError::SourceRejected);
            }
            let max = if matches!(format, DocumentFormat::Xml | DocumentFormat::Html) {
                16 * 1024 * 1024
            } else {
                100 * 1024 * 1024
            };
            if response.content_length().is_some_and(|n| n > max as u64) {
                return Err(DatabaseError::SourceRejected);
            }
            let mut raw = Vec::new();
            while let Some(chunk) = response
                .chunk()
                .await
                .map_err(|_| DatabaseError::StorageUnavailable)?
            {
                if raw.len().saturating_add(chunk.len()) > max {
                    return Err(DatabaseError::SourceRejected);
                }
                raw.extend_from_slice(&chunk);
            }
            Ok(raw)
        };
        tokio::select! {_ = cancel.cancelled()=>Err(DatabaseError::Cancelled),result=fetch=>result}
        }.await;
        let pilot = self
            .budget
            .as_ref()
            .is_some_and(|(_, mode)| *mode == RequestBudgetMode::Pilot);
        let raw = match fetched {
            Ok(raw) => raw,
            Err(DatabaseError::SourceRejected) if pilot => {
                // Keep the admission permit until this failure is fenced in
                // both the process and durable request ledger.
                self.suspend_after_source_rejection().await?;
                return Err(DatabaseError::SourceRejected);
            }
            Err(error @ (DatabaseError::Cancelled | DatabaseError::StorageUnavailable))
                if pilot =>
            {
                // A reserved request may have been sent; preserve the
                // unresolved marker for operator review after a restart.
                return Err(error);
            }
            Err(DatabaseError::Cancelled) => return Err(DatabaseError::Cancelled),
            Err(error) => {
                if !self.operator_suspended.load(Ordering::Acquire) {
                    self.complete_request().await?;
                }
                return Err(error);
            }
        };
        let retrieved_at = self.clock.now();
        let digest = Sha256::digest(&raw)
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
        let output = self
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
            .map_err(document_error)
            .and_then(|output| check(output, raw, retrieved_at));
        match output {
            Err(DatabaseError::SourceRejected) if pilot => {
                self.suspend_after_source_rejection().await?;
                return Err(DatabaseError::SourceRejected);
            }
            Err(DatabaseError::Cancelled) => return Err(DatabaseError::Cancelled),
            _ => {}
        }
        if !self.operator_suspended.load(Ordering::Acquire) {
            self.complete_request().await?;
        }
        drop(permit);
        output
    }
}
fn http_status_error(status: reqwest::StatusCode) -> Option<DatabaseError> {
    if status.is_success() {
        None
    } else if status.is_server_error() || status == reqwest::StatusCode::TOO_MANY_REQUESTS {
        Some(DatabaseError::StorageUnavailable)
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
        DocumentError::InvalidInput
        | DocumentError::InvalidDocument
        | DocumentError::UnsupportedFormat
        | DocumentError::ProcessingFailed
        | DocumentError::ResourceLimit => DatabaseError::SourceRejected,
    }
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
                    return Err(DatabaseError::SourceRejected);
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
    record.validate()?;
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
    record.validate()?;
    Ok(record)
}

#[cfg(test)]
mod tests {
    use super::*;
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
    fn terminal_provider_failures_and_typed_judgment_dates() {
        for status in [401, 403, 404, 302] {
            assert_eq!(
                http_status_error(reqwest::StatusCode::from_u16(status).unwrap()),
                Some(DatabaseError::SourceRejected)
            );
        }
        assert_eq!(
            http_status_error(reqwest::StatusCode::INTERNAL_SERVER_ERROR),
            Some(DatabaseError::StorageUnavailable)
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
            DatabaseError::SourceRejected
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
