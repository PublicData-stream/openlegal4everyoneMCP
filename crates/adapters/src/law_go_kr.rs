//! LAW OPEN DATA transport and evidence-backed field projection. Document parsing
//! is exclusively delegated to the configured disposable document processor.
use crate::{literal_ip, public_address};
use openlegal_application::document::{
    DocumentError, DocumentFormat, DocumentInput, DocumentNode, DocumentOutput, DocumentProcessor,
};
use openlegal_domain::legal::*;
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{sync::Semaphore, time::Instant};
use tokio_util::sync::CancellationToken;
use url::Url;
#[derive(Clone, Debug)]
pub struct InventoryItem {
    pub object: ObjectId,
    pub revision_id: String,
    pub effective_date: Option<String>,
    pub publication_date: Option<String>,
    pub title: String,
    pub data_source: Option<String>,
    pub case_number: Option<String>,
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
    clock: Arc<dyn openlegal_application::Clock>,
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
        })
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
        if page == 0
            || page > 1_000_000
            || object_id.is_some_and(|id| !openlegal_domain::valid_identifier(id, 128))
        {
            return Err(DatabaseError::InvalidInput);
        }
        if historical && dataset == Dataset::Precedent {
            return Err(DatabaseError::UnsupportedHistory);
        }
        if object_id.is_some() && dataset != Dataset::NationalStatute {
            return Err(DatabaseError::UnsupportedHistory);
        }
        let target = match dataset {
            Dataset::NationalStatute => "eflaw",
            Dataset::Ordinance => "ordin",
            Dataset::Precedent => "prec",
        };
        let mut url = self.api("lawSearch.do", target)?;
        url.query_pairs_mut()
            .append_pair("display", "100")
            .append_pair("page", &page.to_string());
        match dataset {
            Dataset::NationalStatute => {
                url.query_pairs_mut()
                    .append_pair("nw", if historical { "1,3" } else { "3" });
                if let Some(id) = object_id {
                    url.query_pairs_mut().append_pair("LID", id);
                }
            }
            Dataset::Ordinance => {
                url.query_pairs_mut()
                    .append_pair("nw", if historical { "2" } else { "1" });
            }
            Dataset::Precedent => {}
        }
        let (parsed, _) = self
            .fetch_parse(url, DocumentFormat::Xml, false, cancel)
            .await?;
        let tree = parsed.tree.as_ref().ok_or(DatabaseError::StorageCorrupt)?;
        let item_name = match dataset {
            Dataset::NationalStatute => "law",
            Dataset::Ordinance => "law",
            Dataset::Precedent => "prec",
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
                Dataset::Ordinance => "자치법규ID",
                Dataset::Precedent => "판례일련번호",
            };
            let revfield = match dataset {
                Dataset::NationalStatute => "법령일련번호",
                Dataset::Ordinance => "자치법규일련번호",
                Dataset::Precedent => "판례일련번호",
            };
            let id = first(node, idfield).ok_or(DatabaseError::StorageCorrupt)?;
            let master = first(node, revfield).ok_or(DatabaseError::StorageCorrupt)?;
            if !numeric_id(&master) {
                return Err(DatabaseError::StorageCorrupt);
            }
            let title = first(
                node,
                match dataset {
                    Dataset::NationalStatute => "법령명한글",
                    Dataset::Ordinance => "자치법규명",
                    Dataset::Precedent => "사건명",
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
            let effective_date = date(first(node, "시행일자"))?;
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
                publication_date: date(first(node, "공포일자"))?,
                object,
                revision_id,
                effective_date,
                title,
                data_source: first(node, "데이터출처명"),
                case_number: first(node, "사건번호"),
            });
        }
        let total = first(tree, "totalCnt")
            .and_then(|v| v.parse::<u64>().ok())
            .ok_or(DatabaseError::StorageCorrupt)?;
        if items.len() > 100 || (items.is_empty() && (page as u64 - 1) * 100 < total) {
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
        if item.object.dataset == Dataset::Precedent {
            url.query_pairs_mut().append_pair("ID", &item.object.id);
        } else {
            url.query_pairs_mut().append_pair("MST", &master);
        }
        if let Some(date) = effective {
            url.query_pairs_mut()
                .append_pair("efYd", &date)
                .append_pair("chrClsCd", "010201");
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
        let (output, raw, retrieved_at) = self
            .fetch_parse_timed(
                url,
                if html {
                    DocumentFormat::Html
                } else {
                    DocumentFormat::Xml
                },
                false,
                cancel.clone(),
            )
            .await?;
        let mut record = project(item, &output)?;
        let links = attachment_links(output.tree.as_ref().ok_or(DatabaseError::StorageCorrupt)?)?;
        let mut additional_evidence = Vec::new();
        let mut total = raw.len();
        let mut extracted = 0usize;
        for (ordinal, link) in links.into_iter().enumerate() {
            let (attachment, bytes) = self
                .fetch_parse(link.url, link.format, true, cancel.clone())
                .await?;
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
            additional_evidence.push(bytes);
        }
        record.validate()?;
        Ok(ProviderDetail {
            retrieved_at,
            record,
            raw,
            additional_evidence,
            processor_version: output.processor_version,
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
            return Err(DatabaseError::Capacity);
        }
        if next > Instant::now() {
            tokio::select! {_=cancel.cancelled()=>return Err(DatabaseError::Cancelled),_=tokio::time::sleep_until(next)=>{}}
        }
        *self
            .next_request
            .lock()
            .map_err(|_| DatabaseError::StorageUnavailable)? =
            Instant::now().checked_add(Duration::from_secs(1));
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
                *self
                    .next_request
                    .lock()
                    .map_err(|_| DatabaseError::StorageUnavailable)? =
                    Instant::now().checked_add(Duration::from_secs(delay));
                return Err(DatabaseError::Capacity);
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
        let raw = tokio::select! {_ = cancel.cancelled()=>return Err(DatabaseError::Cancelled),result=fetch=>result?};
        let retrieved_at = self.clock.now();
        drop(permit);
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
            .map_err(document_error)?;
        Ok((output, raw, retrieved_at))
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
        Dataset::Ordinance => "ordin",
        Dataset::Precedent => "prec",
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
    } else if numeric_id(&item.revision_id) {
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
    let tree = output.tree.as_ref().ok_or(DatabaseError::StorageCorrupt)?;
    let (master, effective) = revision_parts(item)?;
    let (idfield, titlefield) = match item.object.dataset {
        Dataset::NationalStatute => ("법령ID", "법령명_한글"),
        Dataset::Ordinance => ("자치법규ID", "자치법규명"),
        Dataset::Precedent => ("판례정보일련번호", "사건명"),
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
        }
        .into(),
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
