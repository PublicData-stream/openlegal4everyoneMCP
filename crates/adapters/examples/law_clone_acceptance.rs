//! Explicit, bounded live LAW acceptance against a disposable loopback PostgreSQL 18 DB.
//! Never automatically run by tests. Credentials and provider documents stay off stdout/stderr.
//! Usage: law_clone_acceptance --token-file PATH --worker-image IMAGE --seccomp-path PATH
//!        [--max-attempts 390] [--dataset DATASET]
//! OPENLEGAL_LAW_ACCEPTANCE_DATABASE_URL must name openlegal_law_acceptance_* on loopback.
//! The database must not already contain the openlegal schema. This is an acceptance
//! probe, not a corpus publication or complete traversal. Admission timestamps are
//! recorded in this isolated database; they are not measured socket-send timestamps.
use futures::{FutureExt, future::BoxFuture};
use openlegal_adapters::{
    law_go_kr::{
        LawClient, ProviderRequestLimits, RequestBudgetMode,
        catalog::{DetailMode, HistoryMode, SOURCE_FAMILIES},
        supplements::{
            SupplementOutcome, SupplementRequest, SupplementSeed, global_requests, record_seed,
            seeded_requests,
        },
    },
    postgres::{PostgresOptions, PostgresStore, PostgresTls},
};
use openlegal_application::{
    document::{
        DocumentError, DocumentFormat, DocumentHeader, DocumentInput, DocumentNode, DocumentOutput,
        DocumentProcessor, DocumentResponse, MAX_DOCUMENT_BYTES, MAX_DOCUMENT_HEADER_BYTES,
        MAX_DOCUMENT_OUTPUT_BYTES, validate_output,
    },
    upstream_policy::RequestLimit,
};
use openlegal_domain::legal::{DatabaseError, Dataset};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Row, postgres::PgPoolOptions};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    process::Stdio,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    process::Command,
};
use tokio_util::sync::CancellationToken;
use url::Url;

const MAX_ATTEMPTS: u32 = 390;
const WORKER_PREFIX: &str = "ghcr.io/publicdata-stream/openlegal-document-worker";
const SECCOMP: &[u8] = include_bytes!("../../../deploy/document-sandbox/seccomp.json");
type SetupResult<T> = Result<T, &'static str>;

struct Settings {
    token_file: PathBuf,
    worker_image: String,
    seccomp_path: PathBuf,
    max_attempts: u32,
    dataset: Option<Dataset>,
}
impl Settings {
    fn parse() -> SetupResult<Self> {
        let mut args = std::env::args_os().skip(1);
        let mut values = BTreeMap::new();
        while let Some(key) = args.next() {
            let key = key.into_string().map_err(|_| "invalid_arguments")?;
            if !matches!(
                key.as_str(),
                "--token-file"
                    | "--worker-image"
                    | "--seccomp-path"
                    | "--max-attempts"
                    | "--dataset"
            ) || values.contains_key(&key)
            {
                return Err("invalid_arguments");
            }
            values.insert(key, args.next().ok_or("invalid_arguments")?);
        }
        let token_file = PathBuf::from(values.remove("--token-file").ok_or("invalid_arguments")?);
        let seccomp_path =
            PathBuf::from(values.remove("--seccomp-path").ok_or("invalid_arguments")?);
        let worker_image = values
            .remove("--worker-image")
            .ok_or("invalid_arguments")?
            .into_string()
            .map_err(|_| "invalid_arguments")?;
        let max_attempts = match values.remove("--max-attempts") {
            Some(value) => value
                .to_str()
                .and_then(|v| v.parse().ok())
                .ok_or("invalid_arguments")?,
            None => MAX_ATTEMPTS,
        };
        if !(1..=MAX_ATTEMPTS).contains(&max_attempts) || !valid_image(&worker_image) {
            return Err("invalid_arguments");
        }
        let dataset = values
            .remove("--dataset")
            .map(|value| {
                Dataset::ALL
                    .iter()
                    .copied()
                    .find(|dataset| Some(dataset.as_str()) == value.to_str())
                    .ok_or("invalid_arguments")
            })
            .transpose()?;
        Ok(Self {
            token_file,
            worker_image,
            seccomp_path,
            max_attempts,
            dataset,
        })
    }
}

fn valid_image(image: &str) -> bool {
    if image == "openlegal-document-worker:local" {
        return true;
    }
    if let Some(digest) = image.strip_prefix(&format!("{WORKER_PREFIX}@sha256:")) {
        return digest.len() == 64 && digest.bytes().all(|b| b.is_ascii_hexdigit());
    }
    image
        .strip_prefix(&format!("{WORKER_PREFIX}:"))
        .is_some_and(|tag| {
            !tag.is_empty()
                && tag.len() <= 128
                && tag
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"_.-".contains(&b))
                && tag.as_bytes()[0] != b'-'
                && tag.as_bytes()[0] != b'.'
        })
}
fn digest(raw: &[u8]) -> String {
    Sha256::digest(raw)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

#[derive(Clone)]
struct DockerProcessor {
    image: String,
    seccomp: PathBuf,
    sequence: Arc<AtomicU64>,
    formats: Arc<Mutex<BTreeMap<&'static str, u64>>>,
    provisions: Arc<Mutex<Vec<String>>>,
}
impl DockerProcessor {
    async fn new(settings: &Settings) -> SetupResult<Self> {
        let seccomp = tokio::fs::canonicalize(&settings.seccomp_path)
            .await
            .map_err(|_| "sandbox_configuration")?;
        if !seccomp.is_absolute()
            || tokio::fs::read(&seccomp)
                .await
                .map_err(|_| "sandbox_configuration")?
                != SECCOMP
        {
            return Err("sandbox_configuration");
        }
        let status = tokio::time::timeout(
            Duration::from_secs(15),
            Self::command()
                .args(["image", "inspect", &settings.worker_image])
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .status(),
        )
        .await;
        if !matches!(status, Ok(Ok(status)) if status.success()) {
            return Err("sandbox_image_unavailable");
        }
        Ok(Self {
            image: settings.worker_image.clone(),
            seccomp,
            sequence: Arc::new(AtomicU64::new(0)),
            formats: Arc::new(Mutex::new(BTreeMap::new())),
            provisions: Arc::new(Mutex::new(Vec::new())),
        })
    }
    fn command() -> Command {
        let mut command = Command::new("/usr/bin/docker");
        command
            .args(["--host", "unix:///var/run/docker.sock"])
            .env_remove("DOCKER_CONTEXT")
            .env_remove("DOCKER_HOST")
            .env_remove("DOCKER_TLS_VERIFY")
            .env_remove("DOCKER_CERT_PATH")
            .stderr(Stdio::null())
            .kill_on_drop(true);
        command
    }
    async fn framed(
        &self,
        input: &DocumentInput,
        name: &str,
    ) -> Result<DocumentOutput, DocumentError> {
        // Recheck the caller-selected file before every spawn; the profile content is fixed.
        if tokio::fs::read(&self.seccomp)
            .await
            .map_err(|_| DocumentError::SandboxUnavailable)?
            != SECCOMP
        {
            return Err(DocumentError::SandboxUnavailable);
        }
        let header = serde_json::to_vec(&DocumentHeader {
            format: input.format,
            source_sha256: input.source_sha256.clone(),
            ocr: input.ocr,
            bytes_len: input.raw.len(),
        })
        .map_err(|_| DocumentError::InvalidInput)?;
        if header.len() > MAX_DOCUMENT_HEADER_BYTES {
            return Err(DocumentError::InvalidInput);
        }
        let mut child = Self::command()
            .args([
                "run",
                "--pull",
                "never",
                "-i",
                "--rm",
                "--name",
                name,
                "--network",
                "none",
                "--read-only",
                "--cap-drop",
                "ALL",
                "--user",
                "65532:65532",
                "--security-opt",
                "no-new-privileges",
                "--security-opt",
            ])
            .arg(format!("seccomp={}", self.seccomp.display()))
            .args([
                "--memory",
                "4g",
                "--memory-swap",
                "4g",
                "--cpus",
                "2",
                "--pids-limit",
                "128",
                "--tmpfs",
                "/scratch:rw,size=2g,uid=65532,gid=65532",
                "--log-driver",
                "none",
                "--entrypoint",
                "/usr/local/bin/openlegal-document-worker",
                &self.image,
                "--process",
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .map_err(|_| DocumentError::SandboxUnavailable)?;
        let mut stdin = child
            .stdin
            .take()
            .ok_or(DocumentError::SandboxUnavailable)?;
        let mut stdout = child
            .stdout
            .take()
            .ok_or(DocumentError::SandboxUnavailable)?;
        let write = async {
            stdin
                .write_all(&(header.len() as u32).to_be_bytes())
                .await?;
            stdin.write_all(&header).await?;
            stdin.write_all(&input.raw).await?;
            stdin.shutdown().await?;
            drop(stdin);
            Ok::<(), std::io::Error>(())
        };
        let read = async {
            let length = stdout
                .read_u32()
                .await
                .map_err(|_| DocumentError::ProcessingFailed)? as usize;
            if length == 0 || length > MAX_DOCUMENT_OUTPUT_BYTES {
                return Err(DocumentError::ResourceLimit);
            }
            let mut bytes = vec![0; length];
            stdout
                .read_exact(&mut bytes)
                .await
                .map_err(|_| DocumentError::ProcessingFailed)?;
            let mut trailing = [0; 1];
            if stdout
                .read(&mut trailing)
                .await
                .map_err(|_| DocumentError::ProcessingFailed)?
                != 0
            {
                return Err(DocumentError::InvalidDocument);
            }
            serde_json::from_slice::<DocumentResponse>(&bytes)
                .map_err(|_| DocumentError::InvalidDocument)
        };
        let (write, read) = tokio::join!(write, read);
        write.map_err(|_| DocumentError::ProcessingFailed)?;
        if !child
            .wait()
            .await
            .map_err(|_| DocumentError::SandboxUnavailable)?
            .success()
        {
            return Err(DocumentError::ProcessingFailed);
        }
        let output = match read? {
            DocumentResponse::Success(output) => *output,
            DocumentResponse::Error(error) => return Err(error),
        };
        validate_output(&output, input)?;
        Ok(output)
    }
}
impl DocumentProcessor for DockerProcessor {
    fn process(
        &self,
        input: DocumentInput,
        cancel: CancellationToken,
    ) -> BoxFuture<'static, Result<DocumentOutput, DocumentError>> {
        let this = self.clone();
        async move {
            if input.raw.is_empty() || input.raw.len() > MAX_DOCUMENT_BYTES || input.source_sha256 != digest(&input.raw) {
                return Err(DocumentError::InvalidInput);
            }
            let name = format!("openlegal-law-acceptance-{}-{}", std::process::id(), this.sequence.fetch_add(1, Ordering::Relaxed));
            let result = tokio::select! {
                _ = cancel.cancelled() => Err(DocumentError::Cancelled),
                result = tokio::time::timeout(Duration::from_secs(120), this.framed(&input, &name)) =>
                    result.unwrap_or(Err(DocumentError::TimedOut)),
            };
            // Killing the Docker CLI alone does not stop a container. Always reap its fixed name.
            let _ = tokio::time::timeout(Duration::from_secs(15), Self::command()
                .args(["rm", "-f", &name]).stdin(Stdio::null()).stdout(Stdio::null()).status()).await;
            if let Ok(output) = &result {
                let format = match input.format { DocumentFormat::Xml => "xml", DocumentFormat::Html => "html",
                    DocumentFormat::Pdf => "pdf", DocumentFormat::Hwp5 => "hwp5", DocumentFormat::Hwpx => "hwpx" };
                *this.formats.lock().map_err(|_| DocumentError::ProcessingFailed)?.entry(format).or_default() += 1;
                if let Some(number) = output.tree.as_ref().and_then(first_provision) {
                    this.provisions.lock().map_err(|_| DocumentError::ProcessingFailed)?.push(number);
                }
            }
            result
        }.boxed()
    }
}

fn first_provision(node: &DocumentNode) -> Option<String> {
    if let DocumentNode::Element { name, children, .. } = node {
        if name == "조문단위" {
            let number = openlegal_adapters::law_go_kr::first(node, "조문번호")?;
            // A missing branch field is uncertainty, not evidence of branch zero.
            let branch = openlegal_adapters::law_go_kr::first(node, "조문가지번호")?;
            if !number.bytes().all(|b| b.is_ascii_digit())
                || !branch.bytes().all(|b| b.is_ascii_digit())
            {
                return None;
            }
            let number: u32 = number.parse().ok()?;
            let branch: u32 = branch.parse().ok()?;
            if number == 0 || number > 9999 || branch > 99 {
                return None;
            }
            return Some(format!("{number:04}{branch:02}"));
        }
        return children.iter().find_map(first_provision);
    }
    None
}

fn english_response_shape(tree: &DocumentNode, object_id: &str, revision: &str) -> Value {
    let mut names = BTreeSet::new();
    let mut stack = vec![tree];
    let mut truncated = false;
    while let Some(node) = stack.pop() {
        if let DocumentNode::Element { name, children, .. } = node {
            // Namespace URIs and attributes never enter the summary. Only bounded XML local names.
            let name = name.rsplit('}').next().unwrap_or(name);
            let sensitive = ["token", "password", "secret", "credential", "authorization"]
                .iter()
                .any(|key| name.to_ascii_lowercase().contains(key));
            if !sensitive
                && name.len() <= 128
                && name
                    .chars()
                    .all(|ch| ch.is_alphanumeric() || "_-.".contains(ch))
            {
                if names.len() < 512 {
                    names.insert(name.to_owned());
                } else {
                    truncated = true;
                }
            }
            stack.extend(children);
        }
    }
    let matches = |field: &str, expected: &str| -> Option<bool> {
        let actual = openlegal_adapters::law_go_kr::first(tree, field)?;
        if actual.is_empty() || actual.len() > 64 || !actual.bytes().all(|b| b.is_ascii_digit()) {
            return Some(false);
        }
        Some(actual.trim_start_matches('0') == expected.trim_start_matches('0'))
    };
    json!({"tag_names":names,"tag_names_truncated":truncated,
        "numeric_object_matches":matches("법령ID",object_id),
        "numeric_revision_matches":matches("법령일련번호",revision.split(':').next().unwrap_or(revision)),
        "canonical_identity_established":false})
}

struct Harness {
    pool: PgPool,
    client: LawClient,
    processor: DockerProcessor,
    results: Vec<Value>,
    errors: BTreeMap<String, u64>,
    stop: Option<String>,
    max_attempts: u32,
    last_operation: Option<tokio::time::Instant>,
    seeds: Vec<SupplementSeed>,
}
impl Harness {
    async fn ready(&mut self) -> bool {
        if self.stop.is_some() {
            return false;
        }
        if let Some(last) = self.last_operation {
            tokio::time::sleep_until(last + Duration::from_millis(200)).await;
        }
        self.last_operation = Some(tokio::time::Instant::now());
        match sqlx::query("SELECT on_demand_used,operator_suspended,unresolved_response,next_allowed_at,floor(extract(epoch from clock_timestamp()))::bigint AS now FROM openlegal.provider_request_budget WHERE singleton")
            .fetch_one(&self.pool).await {
            Ok(row) => {
                let used: i64 = row.get("on_demand_used");
                if row.get::<bool, _>("operator_suspended") || row.get::<bool, _>("unresolved_response") {
                    self.stop = Some("provider_admission_fenced".into());
                } else if row.get::<i64, _>("next_allowed_at") > row.get::<i64, _>("now") {
                    self.stop = Some("provider_retry_after_pause".into());
                } else if used >= i64::from(self.max_attempts) {
                    self.stop = Some("attempt_cap_reached".into());
                }
            }
            Err(_) => self.stop = Some("storage_unavailable".into()),
        }
        self.stop.is_none()
    }
    fn error(
        &mut self,
        error: DatabaseError,
        stage: &'static str,
        dataset: Option<Dataset>,
        guide: Option<&str>,
    ) {
        let category = serde_json::to_value(error)
            .ok()
            .and_then(|v| v.as_str().map(str::to_owned))
            .unwrap_or_else(|| "internal".into());
        *self.errors.entry(category.clone()).or_default() += 1;
        self.results.push(json!({"stage":stage,"dataset":dataset,"guide":guide,"status":"error","error":category}));
        if matches!(
            error,
            DatabaseError::SourceRejected
                | DatabaseError::SourceUnauthorized
                | DatabaseError::StorageUnavailable
                | DatabaseError::StorageCorrupt
                | DatabaseError::Cancelled
                | DatabaseError::BudgetExhausted
                | DatabaseError::Conflict
        ) {
            self.stop = Some(category);
        }
    }
    fn seed(&mut self, seed: SupplementSeed) {
        // One representative per type/dataset; the harness never recursively expands observations.
        let key = seed_key(&seed);
        if !self.seeds.iter().any(|old| seed_key(old) == key) {
            self.seeds.push(seed);
        }
    }
    async fn family(&mut self, dataset: Dataset, historical: bool) {
        if !self.ready().await {
            return;
        }
        let stage = if historical {
            "historical_inventory"
        } else {
            "current_inventory"
        };
        let family = openlegal_adapters::law_go_kr::catalog::source_family(dataset);
        let page = match self
            .client
            .inventory_page(dataset, 1, historical, None, CancellationToken::new())
            .await
        {
            Ok(page) => page,
            Err(error) => {
                self.error(error, stage, Some(dataset), Some(family.list_guide));
                return;
            }
        };
        self.results.push(json!({"stage":stage,"dataset":dataset,"guide":family.list_guide,"status":"observed",
            "rows":page.items.len(),"total":page.total,"done":page.done,"incomplete":page.incomplete,
            "rejected_rows":page.rejected_rows,"raw_bytes":page.source_evidence.as_ref().map(|e|e.raw.len())}));
        if page.items.len() > 100 {
            self.error(
                DatabaseError::SourceDataInvalid,
                stage,
                Some(dataset),
                Some(family.list_guide),
            );
            return;
        }
        if page.incomplete || page.rejected_rows != 0 {
            self.error(
                DatabaseError::SourceInventoryIncomplete,
                stage,
                Some(dataset),
                Some(family.list_guide),
            );
        }
        // A scope error may have set a persistent fence while returning an observation.
        if historical || !self.ready().await {
            return;
        }
        let Some(item) = page.items.first() else {
            return;
        };
        if matches!(
            dataset,
            Dataset::NationalStatute | Dataset::AdministrativeRule
        ) && let Ok(seed) = record_seed(item)
        {
            self.seed(seed);
        }
        if dataset == Dataset::LegalTerm {
            self.seed(SupplementSeed::LegalTerm {
                name: item.title.clone(),
            });
        }
        if family.metadata_only || family.detail_mode == DetailMode::ListOnly {
            self.results.push(json!({"stage":"detail","dataset":dataset,"status":"skipped_metadata_only","requests":0}));
            return;
        }
        if let Ok(mut values) = self.processor.provisions.lock() {
            values.clear();
        }
        match self.client.detail(item, CancellationToken::new()).await {
            Ok(detail) => {
                if detail.record.validate().is_err()
                    || detail.record.object != item.object
                    || detail.record.revision_id != item.revision_id
                {
                    self.error(
                        DatabaseError::SourceDataInvalid,
                        "detail_identity",
                        Some(dataset),
                        family.detail_guide,
                    );
                    return;
                }
                let resources = openlegal_domain::rights::resources(&detail.record.metadata);
                let processed = !detail.record.body.is_empty();
                let raw_only = dataset == Dataset::EnglishStatute;
                self.results.push(json!({"stage":"detail","dataset":dataset,"guide":family.detail_guide,
                    "status":if raw_only {"original_only_identity_unverified"}else if processed {"projected"}else{"metadata_only"},
                    "raw_bytes":detail.raw.len(),"additional_evidence":detail.additional_evidence.len(),
                    "source_observations":detail.source_observations.len(),"resources":resources.len(),
                    "restricted_resources_skipped":resources.iter().filter(|r|!r.rights.can_store()&&!r.retained).count(),
                    "retained_no_derivatives":resources.iter().filter(|r|r.retained&&!r.rights.can_process()).count(),
                    "sections":detail.record.sections.len()}));
                if dataset == Dataset::EnglishStatute
                    && let Some(raw) = detail
                        .source_observations
                        .iter()
                        .find_map(|observation| observation.raw.as_ref())
                {
                    match self
                        .processor
                        .process(
                            DocumentInput {
                                format: DocumentFormat::Xml,
                                raw: raw.clone(),
                                source_sha256: digest(raw),
                                ocr: false,
                            },
                            CancellationToken::new(),
                        )
                        .await
                    {
                        Ok(output) => {
                            if let Some(tree) = output.tree.as_ref() {
                                self.results.push(json!({"stage":"english_response_shape","dataset":dataset,
                                        "shape":english_response_shape(tree,&item.object.id,&item.revision_id)}));
                            }
                        }
                        Err(_) => self.error(
                            DatabaseError::SourceDataInvalid,
                            "english_response_shape",
                            Some(dataset),
                            family.detail_guide,
                        ),
                    }
                }
                if dataset == Dataset::NationalStatute {
                    let number = self
                        .processor
                        .provisions
                        .lock()
                        .ok()
                        .and_then(|values| values.first().cloned());
                    if let Some(number) = number {
                        self.seed(SupplementSeed::Provision {
                            object: item.object.clone(),
                            number,
                        });
                    }
                }
            }
            Err(error) => self.error(error, "detail", Some(dataset), family.detail_guide),
        }
    }
    async fn supplement(&mut self, request: SupplementRequest, seeds: bool) {
        if !self.ready().await {
            return;
        }
        match self
            .client
            .fetch_supplement(&request, 0, CancellationToken::new())
            .await
        {
            Ok(SupplementOutcome::Deferred { .. }) => self
                .results
                .push(json!({"stage":"supplement","guide":request.guide(),
                "source":request.source(),"status":"skipped_metadata_only","requests":0})),
            Ok(SupplementOutcome::Captured(capture)) => {
                if let Some(error) = capture.processing_error {
                    self.error(error, "supplement_processing", None, Some(request.guide()));
                }
                self.results.push(json!({"stage":"supplement","guide":request.guide(),"source":request.source(),
                    "status":"observed","raw_bytes":capture.raw.len(),"rows":capture.page.observed_rows,
                    "total":capture.page.total,"done":capture.page.done,"incomplete":capture.page.incomplete}));
                if seeds {
                    for seed in capture.page.seeds {
                        self.seed(seed);
                    }
                }
            }
            Err(error) => self.error(error, "supplement", None, Some(request.guide())),
        }
    }
}
fn seed_key(seed: &SupplementSeed) -> String {
    match seed {
        SupplementSeed::Global => "global".into(),
        SupplementSeed::Record { object, .. } => format!("record:{}", object.dataset.as_str()),
        SupplementSeed::Provision { .. } => "provision".into(),
        SupplementSeed::LegalTerm { .. } => "legal_term".into(),
        SupplementSeed::EverydayTerm { .. } => "everyday_term".into(),
        SupplementSeed::ChangeDay { .. } => "change_day".into(),
        SupplementSeed::ChangeWindow { .. } => "change_window".into(),
    }
}

async fn acceptance(settings: Settings) -> SetupResult<Value> {
    let database = std::env::var("OPENLEGAL_LAW_ACCEPTANCE_DATABASE_URL")
        .map_err(|_| "database_configuration")?;
    let parsed = Url::parse(&database).map_err(|_| "database_configuration")?;
    if !matches!(parsed.scheme(), "postgres" | "postgresql")
        || !matches!(
            parsed.host_str(),
            Some("127.0.0.1" | "[::1]" | "::1" | "localhost")
        )
        || !parsed.path().starts_with("/openlegal_law_acceptance_")
        || parsed.query().is_some()
        || parsed.fragment().is_some()
    {
        return Err("database_not_isolated_loopback");
    }
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .acquire_timeout(Duration::from_secs(10))
        .connect(&database)
        .await
        .map_err(|_| "database_unavailable")?;
    let exists: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname='openlegal')")
            .fetch_one(&pool)
            .await
            .map_err(|_| "database_unavailable")?;
    if exists {
        return Err("database_not_pristine");
    }
    PostgresStore::migrate(
        &database,
        PostgresOptions {
            max_connections: 4,
            tls: PostgresTls::Plaintext,
        },
    )
    .await
    .map_err(|_| "database_migration_failed")?;
    LawClient::configure_provider_request_limits(
        &pool,
        &ProviderRequestLimits {
            continuous_daily_limit: RequestLimit::Limited(settings.max_attempts),
            on_demand_daily_limit: RequestLimit::Limited(settings.max_attempts),
            on_demand_attempt_limit: RequestLimit::Limited(settings.max_attempts),
            pilot_attempt_limit: RequestLimit::Limited(settings.max_attempts),
            interval_ms: 200,
            max_in_flight: 4,
            ..ProviderRequestLimits::default()
        },
    )
    .await
    .map_err(|_| "database_configuration")?;
    // Recorder belongs only to this throwaway acceptance DB. It stores no URLs or documents.
    sqlx::raw_sql("CREATE TABLE public.law_acceptance_admission (sequence bigserial PRIMARY KEY, started_at_ms bigint NOT NULL, slot integer NOT NULL, active_count integer NOT NULL); CREATE FUNCTION public.law_acceptance_record() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN INSERT INTO public.law_acceptance_admission(started_at_ms,slot,active_count) SELECT NEW.started_at_ms,NEW.slot,count(*) FROM openlegal.provider_request_admission; RETURN NEW; END $$; CREATE TRIGGER law_acceptance_record AFTER INSERT ON openlegal.provider_request_admission FOR EACH ROW EXECUTE FUNCTION public.law_acceptance_record();")
        .execute(&pool).await.map_err(|_| "database_recorder_failed")?;
    let processor = DockerProcessor::new(&settings).await?;
    // Verify framing/digest/tree validation in the actual cached sandbox before using API scope.
    for (format, raw) in [
        (
            DocumentFormat::Xml,
            b"<root><item>acceptance</item></root>".as_slice(),
        ),
        (
            DocumentFormat::Html,
            b"<html><body><p>acceptance</p></body></html>".as_slice(),
        ),
    ] {
        processor
            .process(
                DocumentInput {
                    format,
                    raw: raw.to_vec(),
                    source_sha256: digest(raw),
                    ocr: false,
                },
                CancellationToken::new(),
            )
            .await
            .map_err(|_| "sandbox_preflight_failed")?;
    }
    // Only this explicitly invoked executable reads the credential. Never print file errors or content.
    let metadata = tokio::fs::metadata(&settings.token_file)
        .await
        .map_err(|_| "credential_unavailable")?;
    if !metadata.is_file() || metadata.len() > 1024 {
        return Err("credential_invalid");
    }
    let token = tokio::fs::read_to_string(&settings.token_file)
        .await
        .map_err(|_| "credential_unavailable")?;
    let client = LawClient::new(token.trim().to_owned(), Arc::new(processor.clone()))
        .map_err(|_| "credential_invalid")?
        .with_request_budget(pool.clone(), RequestBudgetMode::OnDemand)
        .with_local_cap(settings.max_attempts);
    let mut harness = Harness {
        pool: pool.clone(),
        client,
        processor,
        results: Vec::new(),
        errors: BTreeMap::new(),
        stop: None,
        max_attempts: settings.max_attempts,
        last_operation: None,
        seeds: Vec::new(),
    };
    for family in SOURCE_FAMILIES {
        if settings
            .dataset
            .is_some_and(|dataset| dataset != family.dataset)
        {
            continue;
        }
        harness.family(family.dataset, false).await;
        if matches!(
            family.history_mode,
            HistoryMode::StatuteEffective | HistoryMode::CurrentHistory
        ) {
            harness.family(family.dataset, true).await;
        }
        if harness.stop.is_some() {
            break;
        }
    }
    if harness.stop.is_none() && settings.dataset.is_none() {
        for request in global_requests(1).map_err(|_| "registry_invalid")? {
            harness.supplement(request, true).await;
            if harness.stop.is_some() {
                break;
            }
        }
    }
    if harness.stop.is_none() && settings.dataset.is_none() {
        // Date-filter APIs have no documented complete date universe and stay closed.
        let seeds = std::mem::take(&mut harness.seeds);
        let mut seen = BTreeSet::new();
        for seed in seeds {
            for request in seeded_requests(seed, 1).map_err(|_| "registry_invalid")? {
                // Dedupe canonical request hashes, not only guide IDs (three-column has two views).
                let key = request.observation_key().map_err(|_| "registry_invalid")?;
                if seen.insert(key) {
                    harness.supplement(request, false).await;
                }
                if harness.stop.is_some() {
                    break;
                }
            }
            if harness.stop.is_some() {
                break;
            }
        }
    }
    let budget = sqlx::query("SELECT on_demand_used,interval_ms,max_in_flight,operator_suspended,unresolved_response FROM openlegal.provider_request_budget WHERE singleton")
        .fetch_one(&pool).await.map_err(|_| "database_unavailable")?;
    let evidence = sqlx::query("SELECT count(*)::bigint AS reservations,max(active_count) AS max_active,min(delta) AS min_interval FROM (SELECT active_count,started_at_ms-lag(started_at_ms) OVER(ORDER BY sequence) AS delta FROM public.law_acceptance_admission) a")
        .fetch_one(&pool).await.map_err(|_| "database_unavailable")?;
    let used: i64 = budget.get("on_demand_used");
    if harness.stop.is_none()
        && (budget.get::<bool, _>("operator_suspended")
            || budget.get::<bool, _>("unresolved_response"))
    {
        harness.stop = Some("provider_admission_fenced".into());
    }
    let reservations: i64 = evidence.get("reservations");
    let min_interval: Option<i64> = evidence.get("min_interval");
    let max_active: Option<i32> = evidence.get("max_active");
    let rate_verified = used == reservations
        && used <= i64::from(settings.max_attempts)
        && min_interval.is_none_or(|v| v >= 200)
        && max_active.is_none_or(|v| v <= 4);
    let formats = harness
        .processor
        .formats
        .lock()
        .map_err(|_| "sandbox_statistics_failed")?
        .clone();
    let output = json!({"status":if harness.stop.is_some(){"stopped"}else if harness.errors.is_empty() && rate_verified {"completed"}else{"completed_with_errors"},
        "stop_reason":harness.stop,"scope":if settings.dataset.is_some(){"selected_family_first_page_and_representative"}else{"first_page_and_one_representative_per_family"},
        "selected_dataset":settings.dataset,
        "catalog_families":SOURCE_FAMILIES.len(),"max_attempts":settings.max_attempts,"attempts":used,
        "admission":{"configured_interval_ms":budget.get::<i32,_>("interval_ms"),"configured_max_in_flight":budget.get::<i32,_>("max_in_flight"),
            "recorded_reservations":reservations,"min_admission_interval_ms":min_interval,"max_active_admissions":max_active,
            "verified":rate_verified,"measurement":"database_reservation_not_socket_send",
            "operator_suspended":budget.get::<bool,_>("operator_suspended"),"unresolved_response":budget.get::<bool,_>("unresolved_response")},
        "sandbox_processed_formats":formats,"sandbox_preflight_documents":2,"errors":harness.errors,"results":harness.results,
        "archive_persistence_exercised":false,"full_traversal_completed":false});
    pool.close().await;
    Ok(output)
}

#[tokio::main]
async fn main() {
    // Suppress panic diagnostics because library messages can contain provider document data.
    std::panic::set_hook(Box::new(|_| {}));
    if std::env::args_os().skip(1).any(|a| a == "--help") {
        println!(
            "law_clone_acceptance --token-file PATH --worker-image IMAGE --seccomp-path PATH [--max-attempts 1..390] [--dataset DATASET]\nSet OPENLEGAL_LAW_ACCEPTANCE_DATABASE_URL to a pristine loopback PostgreSQL 18 database named openlegal_law_acceptance_*."
        );
        return;
    }
    match Settings::parse() {
        Ok(settings) => match acceptance(settings).await {
            Ok(summary) => {
                let succeeded = summary["status"] == "completed";
                println!("{summary}");
                if !succeeded {
                    std::process::exit(1);
                }
            }
            Err(error) => {
                println!("{}", json!({"status":"startup_failed","error":error}));
                std::process::exit(2);
            }
        },
        Err(error) => {
            println!("{}", json!({"status":"startup_failed","error":error}));
            std::process::exit(2);
        }
    }
}
