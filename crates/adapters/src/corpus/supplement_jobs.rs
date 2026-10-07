//! Persistent closed-source supplemental work. Only source/seed/page descriptors
//! survive restart; URLs, parameters and credentials cannot be supplied by a job.
use super::*;
use crate::law_go_kr::supplements::{self, SupplementRequest, SupplementSeed, SupplementSource};
use serde_json::{Value, json};
use sqlx::{Postgres, Transaction};

pub struct SupplementJob {
    pub key: String,
    pub request: SupplementRequest,
    pub observed_before: u64,
    pub observation_id: Option<String>,
    pub lease_owner: String,
    pub lease_until: u64,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SupplementJobStatus {
    Pending,
    Done,
    Deferred,
    Incomplete,
}
impl SupplementJobStatus {
    fn name(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Done => "done",
            Self::Deferred => "deferred",
            Self::Incomplete => "incomplete",
        }
    }
}
struct Descriptor {
    key: String,
    source: String,
    value: Value,
    predecessor: Option<String>,
    page: i32,
    global: bool,
}
fn descriptor(request: &SupplementRequest) -> Result<Descriptor, DatabaseError> {
    let rebuilt = supplements::request(request.source(), request.seed().clone(), request.page())?;
    let key = rebuilt.observation_key()?;
    if key != request.observation_key()? {
        return Err(DatabaseError::InvalidInput);
    }
    let source = serde_json::to_value(request.source())
        .map_err(corrupt)?
        .as_str()
        .ok_or(DatabaseError::InvalidInput)?
        .to_string();
    let value = serde_json::to_value((request.source(), request.seed(), request.page()))
        .map_err(corrupt)?;
    if serde_json::to_vec(&value).map_err(corrupt)?.len() > 4096 {
        return Err(DatabaseError::InvalidInput);
    }
    let predecessor = if request.page() == 1 {
        None
    } else {
        Some(
            supplements::request(request.source(), request.seed().clone(), request.page() - 1)?
                .observation_key()?,
        )
    };
    Ok(Descriptor {
        key,
        source,
        value,
        predecessor,
        page: request.page().try_into().map_err(corrupt)?,
        global: matches!(request.seed(), SupplementSeed::Global),
    })
}
fn decode(row: &PgRow) -> Result<SupplementRequest, DatabaseError> {
    let (source, seed, page): (SupplementSource, SupplementSeed, u32) =
        serde_json::from_value(row.try_get("descriptor").map_err(db)?).map_err(corrupt)?;
    let request = supplements::request(source, seed, page).map_err(corrupt)?;
    let d = descriptor(&request).map_err(corrupt)?;
    if d.key != row.try_get::<String, _>("job_key").map_err(db)?
        || d.source != row.try_get::<String, _>("source").map_err(db)?
        || d.page != row.try_get::<i32, _>("page").map_err(db)?
        || d.predecessor
            != row
                .try_get::<Option<String>, _>("predecessor_key")
                .map_err(db)?
        || d.global != row.try_get::<bool, _>("is_global").map_err(db)?
    {
        return Err(DatabaseError::StorageCorrupt);
    }
    Ok(request)
}
async fn enqueue(
    tx: &mut Transaction<'_, Postgres>,
    request: &SupplementRequest,
    now: u64,
) -> Result<bool, DatabaseError> {
    let d = descriptor(request)?;
    let changed=sqlx::query("INSERT INTO openlegal.provider_supplement_job(job_key,source,descriptor,page,predecessor_key,is_global,created_at,requested_at) VALUES($1,$2,$3,$4,$5,$6,$7::text::numeric,$7::text::numeric) ON CONFLICT(job_key) DO UPDATE SET status='pending',observation_id=NULL,observed_rows=0,requested_at=EXCLUDED.requested_at,completed_at=NULL,retry_at=NULL WHERE openlegal.provider_supplement_job.status='done' AND openlegal.provider_supplement_job.completed_at<=EXCLUDED.requested_at-86400")
        .bind(&d.key).bind(&d.source).bind(&d.value).bind(d.page).bind(&d.predecessor).bind(d.global).bind(now.to_string()).execute(&mut **tx).await.map_err(db)?.rows_affected();
    {
        let row=sqlx::query("SELECT job_key,source,descriptor,page,predecessor_key,is_global FROM openlegal.provider_supplement_job WHERE job_key=$1")
            .bind(&d.key).fetch_one(&mut **tx).await.map_err(db)?;
        let existing = decode(&row)?;
        if !existing.same_work_as(request) {
            return Err(DatabaseError::StorageCorrupt);
        }
    }
    Ok(changed > 0)
}
impl PgCorpusStore {
    pub async fn enqueue_supplement(
        &self,
        request: &SupplementRequest,
        now: u64,
    ) -> Result<bool, DatabaseError> {
        self.gate().await?;
        let mut tx = self.pool.begin().await.map_err(db)?;
        // Keep the same control-before-job lock order as claim and settlement.
        // A daily parent refresh cannot race a child's predecessor admission.
        sqlx::query("SELECT singleton FROM openlegal.provider_supplement_control WHERE singleton FOR UPDATE")
            .execute(&mut *tx).await.map_err(db)?;
        let changed = enqueue(&mut tx, request, now).await?;
        tx.commit().await.map_err(db)?;
        Ok(changed)
    }
    /// One execution lease; global work and identified seeds alternate, then
    /// source names rotate. Expired workers are fenced by a fresh UUID owner.
    pub async fn claim_supplement(&self, now: u64) -> Result<Option<SupplementJob>, DatabaseError> {
        self.gate().await?;
        let until = now.checked_add(600).ok_or(DatabaseError::Capacity)?;
        let mut tx = self.pool.begin().await.map_err(db)?;
        sqlx::query("SELECT singleton FROM openlegal.corpus_control WHERE singleton FOR UPDATE")
            .fetch_one(&mut *tx)
            .await
            .map_err(db)?;
        let held: bool = sqlx::query_scalar("SELECT openlegal_admin.provider_held()")
            .fetch_one(&mut *tx)
            .await
            .map_err(db)?;
        if held {
            return Ok(None);
        }
        let control=sqlx::query("SELECT last_global_source,last_seed_source,last_global FROM openlegal.provider_supplement_control WHERE singleton FOR UPDATE").fetch_one(&mut *tx).await.map_err(db)?;
        let row=sqlx::query("SELECT j.*,j.requested_at::text AS requested_at_text FROM openlegal.provider_supplement_job j WHERE (j.status='pending' OR (j.status='incomplete' AND j.retry_at<=$1::text::numeric) OR (j.status='running' AND j.lease_until<=$1::text::numeric)) AND (j.predecessor_key IS NULL OR EXISTS(SELECT 1 FROM openlegal.provider_supplement_job predecessor WHERE predecessor.job_key=j.predecessor_key AND predecessor.status='done')) ORDER BY (j.is_global<>$2) DESC,(j.source>CASE WHEN j.is_global THEN $3 ELSE $4 END) DESC,j.source,j.created_at,j.job_key LIMIT 1 FOR UPDATE OF j SKIP LOCKED")
            .bind(now.to_string()).bind(control.try_get::<bool,_>("last_global").map_err(db)?).bind(control.try_get::<String,_>("last_global_source").map_err(db)?).bind(control.try_get::<String,_>("last_seed_source").map_err(db)?).fetch_optional(&mut *tx).await.map_err(db)?;
        let Some(row) = row else {
            return Ok(None);
        };
        let request = decode(&row)?;
        let key: String = row.try_get("job_key").map_err(db)?;
        let previous: Option<String> = row.try_get("predecessor_key").map_err(db)?;
        let observed_before = if let Some(previous) = previous {
            let row=sqlx::query("SELECT observed_before::text,observed_rows::text FROM openlegal.provider_supplement_job WHERE job_key=$1 AND status='done'")
                .bind(previous).fetch_one(&mut *tx).await.map_err(db)?;
            unsigned(&row, "observed_before")?
                .checked_add(unsigned(&row, "observed_rows")?)
                .ok_or(DatabaseError::Capacity)?
        } else {
            0
        };
        let refresh_incomplete = row.try_get::<String, _>("status").map_err(db)? == "incomplete"
            && now.saturating_sub(unsigned(&row, "requested_at_text")?) >= 86400;
        // Parser retries reuse retained bytes for one day. Then discard only the
        // job's reference and advance its request epoch so corrected upstream
        // bytes can be downloaded; the original observation remains permanent.
        // Other claims recover a crash between byte retention and settlement.
        let observation_id: Option<String> = if refresh_incomplete {
            None
        } else {
            sqlx::query_scalar("SELECT COALESCE(j.observation_id,(SELECT o.id FROM openlegal.corpus_source_observation o WHERE o.source_key=j.job_key AND o.raw_sha256 IS NOT NULL AND o.validated_at>=j.requested_at ORDER BY o.validated_at DESC,o.id DESC LIMIT 1)) FROM openlegal.provider_supplement_job j WHERE j.job_key=$1")
                .bind(&key).fetch_one(&mut *tx).await.map_err(db)?
        };
        let owner: Uuid = sqlx::query_scalar("SELECT pg_catalog.uuidv7()")
            .fetch_one(&mut *tx)
            .await
            .map_err(db)?;
        sqlx::query("UPDATE openlegal.provider_supplement_job SET status='running',owner=$2,lease_until=$3::text::numeric,observed_before=$4::text::numeric,observation_id=$5,retry_at=NULL,requested_at=CASE WHEN $6 THEN $7::text::numeric ELSE requested_at END WHERE job_key=$1")
            .bind(&key).bind(owner).bind(until.to_string()).bind(observed_before.to_string()).bind(&observation_id).bind(refresh_incomplete).bind(now.to_string()).execute(&mut *tx).await.map_err(db)?;
        sqlx::query("UPDATE openlegal.provider_supplement_control SET last_global_source=CASE WHEN $2 THEN $1 ELSE last_global_source END,last_seed_source=CASE WHEN $2 THEN last_seed_source ELSE $1 END,last_global=$2 WHERE singleton")
            .bind(row.try_get::<String,_>("source").map_err(db)?).bind(row.try_get::<bool,_>("is_global").map_err(db)?).execute(&mut *tx).await.map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(Some(SupplementJob {
            key,
            request,
            observed_before,
            observation_id,
            lease_owner: owner.to_string(),
            lease_until: until,
        }))
    }
    pub async fn settle_supplement(
        &self,
        job: &SupplementJob,
        status: SupplementJobStatus,
        observation_id: Option<&str>,
        observed_rows: u64,
        now: u64,
    ) -> Result<(), DatabaseError> {
        self.settle_supplement_with_successor(job, status, observation_id, observed_rows, now, None)
            .await
    }
    /// A processed nonterminal page and its successor commit together. Callers
    /// must enqueue child seeds before settlement or supply them in a later API.
    #[allow(clippy::too_many_arguments)]
    pub async fn settle_supplement_with_successor(
        &self,
        job: &SupplementJob,
        status: SupplementJobStatus,
        observation_id: Option<&str>,
        observed_rows: u64,
        now: u64,
        successor: Option<&SupplementRequest>,
    ) -> Result<(), DatabaseError> {
        self.gate().await?;
        let owner = Uuid::parse_str(&job.lease_owner).map_err(|_| DatabaseError::InvalidInput)?;
        if descriptor(&job.request)?.key != job.key {
            return Err(DatabaseError::InvalidInput);
        }
        if let Some(next) = successor
            && (status != SupplementJobStatus::Done
                || job.request.next_page()?.observation_key()? != next.observation_key()?)
        {
            return Err(DatabaseError::InvalidInput);
        }
        if status == SupplementJobStatus::Done
            && (observation_id.is_none() || job.request.metadata_only())
        {
            return Err(DatabaseError::InvalidInput);
        }
        let retry = if status == SupplementJobStatus::Incomplete {
            Some(
                now.checked_add(3600)
                    .ok_or(DatabaseError::Capacity)?
                    .to_string(),
            )
        } else {
            None
        };
        let completed = matches!(
            status,
            SupplementJobStatus::Done | SupplementJobStatus::Deferred
        )
        .then(|| now.to_string());
        let mut tx = self.pool.begin().await.map_err(db)?;
        sqlx::query("SELECT singleton FROM openlegal.provider_supplement_control WHERE singleton FOR UPDATE")
            .execute(&mut *tx).await.map_err(db)?;
        let row=sqlx::query("SELECT * FROM openlegal.provider_supplement_job WHERE job_key=$1 AND status='running' AND owner=$2 AND lease_until>$3::text::numeric FOR UPDATE")
            .bind(&job.key).bind(owner).bind(now.to_string()).fetch_optional(&mut *tx).await.map_err(db)?.ok_or(DatabaseError::Conflict)?;
        decode(&row)?;
        if let Some(id) = observation_id {
            if !openlegal_domain::history::valid_snapshot_id(id) {
                return Err(DatabaseError::InvalidInput);
            }
            let eligible:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM openlegal.corpus_source_observation o JOIN openlegal.provider_supplement_job j ON j.job_key=o.source_key WHERE o.id=$1 AND o.source_key=$2 AND ($3 OR (o.raw_sha256 IS NOT NULL AND o.validated_at>=j.requested_at)))")
                .bind(id).bind(&job.key).bind(status!=SupplementJobStatus::Done).fetch_one(&mut *tx).await.map_err(db)?;
            if !eligible {
                return Err(DatabaseError::InvalidInput);
            }
        }
        sqlx::query("UPDATE openlegal.provider_supplement_job SET status=$2,owner=NULL,lease_until=NULL,observation_id=COALESCE($3,observation_id),observed_rows=$4::text::numeric,completed_at=$5::text::numeric,retry_at=$6::text::numeric WHERE job_key=$1")
            .bind(&job.key).bind(status.name()).bind(observation_id).bind(observed_rows.to_string()).bind(completed).bind(retry).execute(&mut *tx).await.map_err(db)?;
        if let Some(next) = successor {
            enqueue(&mut tx, next, now).await?;
        }
        tx.commit().await.map_err(db)
    }
    pub async fn supplement_progress(&self) -> Result<Value, DatabaseError> {
        self.gate().await?;
        let required_keys: Vec<String> = supplements::global_requests(1)?
            .iter()
            .map(SupplementRequest::observation_key)
            .collect::<Result<_, _>>()?;
        let rows=sqlx::query("SELECT source,status,count(*) AS jobs,count(*) FILTER(WHERE job_key=ANY($1) AND status='done') AS completed_global_roots FROM openlegal.provider_supplement_job GROUP BY source,status ORDER BY source,status")
            .bind(&required_keys).fetch_all(&self.pool).await.map_err(db)?;
        let mut sources = Vec::new();
        let mut pending = 0_u64;
        let mut done = 0_u64;
        let mut deferred = 0_u64;
        let mut incomplete = 0_u64;
        let mut completed_global_roots = 0_u64;
        for row in rows {
            completed_global_roots = completed_global_roots
                .checked_add(
                    row.try_get::<i64, _>("completed_global_roots")
                        .map_err(db)?
                        .try_into()
                        .map_err(corrupt)?,
                )
                .ok_or(DatabaseError::Capacity)?;
            let count: u64 = row
                .try_get::<i64, _>("jobs")
                .map_err(db)?
                .try_into()
                .map_err(corrupt)?;
            let status: String = row.try_get("status").map_err(db)?;
            match status.as_str() {
                "pending" | "running" => {
                    pending = pending.checked_add(count).ok_or(DatabaseError::Capacity)?
                }
                "done" => done = done.checked_add(count).ok_or(DatabaseError::Capacity)?,
                "deferred" => {
                    deferred = deferred.checked_add(count).ok_or(DatabaseError::Capacity)?
                }
                "incomplete" => {
                    incomplete = incomplete
                        .checked_add(count)
                        .ok_or(DatabaseError::Capacity)?
                }
                _ => return Err(DatabaseError::StorageCorrupt),
            }
            sources.push(json!({"source":row.try_get::<String,_>("source").map_err(db)?,"status":status,"jobs":count}));
        }
        let complete = completed_global_roots == required_keys.len() as u64
            && pending == 0
            && deferred == 0
            && incomplete == 0;
        Ok(
            json!({"complete":complete,"completed_global_roots":completed_global_roots,"required_global_roots":required_keys.len(),"pending_or_running":pending,"done":done,"deferred":deferred,"incomplete":incomplete,"sources":sources}),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blob::FsBlobStore;
    use openlegal_application::{document::DocumentNode, persistence::PersistentStore};
    use openlegal_domain::rights::SourceRights;
    use std::collections::BTreeMap;

    fn record(source: SupplementSource, object_id: &str, record_number: &str) -> SupplementRequest {
        supplements::request(
            source,
            SupplementSeed::Record {
                object: ObjectId {
                    jurisdiction: "kr".into(),
                    provider: "law_go_kr".into(),
                    dataset: Dataset::NationalStatute,
                    id: object_id.into(),
                },
                record_number: record_number.into(),
            },
            1,
        )
        .unwrap()
    }
    async fn setup() -> (
        crate::test_support::TestDatabase,
        Arc<crate::postgres::PostgresStore>,
        PgCorpusStore,
    ) {
        let fixture = crate::test_support::TestDatabase::new().await;
        let base = fixture.open(100).await;
        let blobs = FsBlobStore::open(&fixture.directory.path().join("supplement-regression"))
            .await
            .unwrap();
        let store = PgCorpusStore::new(base.pool(), blobs);
        (fixture, base, store)
    }
    async fn descriptor_value(pool: &PgPool, key: &str) -> Value {
        sqlx::query_scalar(
            "SELECT descriptor FROM openlegal.provider_supplement_job WHERE job_key=$1",
        )
        .bind(key)
        .fetch_one(pool)
        .await
        .unwrap()
    }
    async fn retain(store: &PgCorpusStore, request: &SupplementRequest, at: u64) -> String {
        store
            .retain_source_observation(
                SourceObservationInput {
                    source_key: request.observation_key().unwrap(),
                    raw: Some(b"<list>unchanged exact evidence</list>".to_vec()),
                    media_type: "application/xml".into(),
                    rights: SourceRights::legal_information(),
                    metadata: BTreeMap::new(),
                    observed_at: at,
                },
                CancellationToken::new(),
            )
            .await
            .unwrap()
            .observation_id
    }

    #[tokio::test]
    #[ignore = "requires scripts/test-postgres.sh"]
    async fn related_statutes_alias_preserves_legacy_descriptor_claim_retry_and_daily_refresh() {
        let (fixture, base, store) = setup().await;
        let pool = base.pool();
        let budget: Value = sqlx::query_scalar(
            "SELECT to_jsonb(b) FROM openlegal.provider_request_budget b WHERE singleton",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        let old = record(SupplementSource::RelatedStatutes, "123", "100");
        let new = record(SupplementSource::RelatedStatutes, "123", "101");
        let key = old.observation_key().unwrap();
        assert!(store.enqueue_supplement(&old, 100).await.unwrap());
        let legacy = descriptor_value(&pool, &key).await;
        let claim = store.claim_supplement(100).await.unwrap().unwrap();
        assert!(!store.enqueue_supplement(&new, 101).await.unwrap());
        let unchanged_owner: String = sqlx::query_scalar(
            "SELECT owner::text FROM openlegal.provider_supplement_job WHERE job_key=$1",
        )
        .bind(&key)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(unchanged_owner, claim.lease_owner);
        assert_eq!(descriptor_value(&pool, &key).await, legacy);
        let id = retain(&store, &old, 102).await;
        let observation = store
            .source_observation(&id, CancellationToken::new())
            .await
            .unwrap();
        store
            .settle_supplement(&claim, SupplementJobStatus::Incomplete, Some(&id), 0, 200)
            .await
            .unwrap();

        // Reopen the persistent queue and bytes rather than retaining an in-memory claim.
        let blobs = FsBlobStore::open(&fixture.directory.path().join("supplement-regression"))
            .await
            .unwrap();
        let restarted = PgCorpusStore::new(pool.clone(), blobs);
        let retry = restarted.claim_supplement(3800).await.unwrap().unwrap();
        assert_eq!(retry.observation_id.as_deref(), Some(id.as_str()));
        assert_eq!(retry.request.seed(), old.seed());
        assert_eq!(
            restarted
                .settle_supplement(&claim, SupplementJobStatus::Done, Some(&id), 1, 3801)
                .await,
            Err(DatabaseError::Conflict)
        );
        restarted
            .settle_supplement(&retry, SupplementJobStatus::Done, Some(&id), 1, 3801)
            .await
            .unwrap();
        assert!(!restarted.enqueue_supplement(&new, 90200).await.unwrap());
        assert!(restarted.enqueue_supplement(&new, 90201).await.unwrap());
        assert_eq!(descriptor_value(&pool, &key).await, legacy);
        let fresh = restarted.claim_supplement(90201).await.unwrap().unwrap();
        assert!(fresh.observation_id.is_none());
        assert_eq!(fresh.request.seed(), old.seed());
        assert_eq!(
            restarted
                .settle_supplement(&fresh, SupplementJobStatus::Done, Some(&id), 1, 90202)
                .await,
            Err(DatabaseError::InvalidInput)
        );
        assert_eq!(retain(&restarted, &new, 90202).await, id);
        let reclaimed = restarted.claim_supplement(90801).await.unwrap().unwrap();
        assert_eq!(reclaimed.observation_id.as_deref(), Some(id.as_str()));
        assert_eq!(
            restarted
                .settle_supplement(&fresh, SupplementJobStatus::Done, Some(&id), 1, 90802)
                .await,
            Err(DatabaseError::Conflict)
        );
        restarted
            .settle_supplement(&reclaimed, SupplementJobStatus::Done, Some(&id), 1, 90802)
            .await
            .unwrap();
        let revalidated = restarted
            .source_observation(&id, CancellationToken::new())
            .await
            .unwrap();
        let mut expected_observation = observation;
        expected_observation.validated_at = 90202;
        assert_eq!(revalidated, expected_observation);
        assert_eq!(
            restarted
                .source_observation_bytes(&id, CancellationToken::new())
                .await
                .unwrap(),
            b"<list>unchanged exact evidence</list>"
        );
        assert_eq!(descriptor_value(&pool, &key).await, legacy);
        let counts: (i64, i64, i64) = sqlx::query_as("SELECT raw_bytes,staged_bytes,(SELECT count(*) FROM openlegal.corpus_source_observation) FROM openlegal.corpus_control WHERE singleton")
            .fetch_one(&pool).await.unwrap();
        assert_eq!(
            counts,
            (b"<list>unchanged exact evidence</list>".len() as i64, 0, 1)
        );
        let after_budget: Value = sqlx::query_scalar(
            "SELECT to_jsonb(b) FROM openlegal.provider_request_budget b WHERE singleton",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(after_budget, budget);
        let admissions: i64 =
            sqlx::query_scalar("SELECT count(*) FROM openlegal.provider_request_admission")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(admissions, 0);
        base.close().await.unwrap();
    }

    #[tokio::test]
    #[ignore = "requires scripts/test-postgres.sh"]
    async fn equal_mst_wire_identity_cannot_merge_distinct_parent_objects() {
        let (_fixture, base, store) = setup().await;
        let first = record(SupplementSource::StatuteHierarchy, "123", "100");
        let other = record(SupplementSource::StatuteHierarchy, "124", "100");
        assert_eq!(
            first.observation_key().unwrap(),
            other.observation_key().unwrap()
        );
        store.enqueue_supplement(&first, 100).await.unwrap();
        let before = descriptor_value(&base.pool(), &first.observation_key().unwrap()).await;
        assert_eq!(
            store.enqueue_supplement(&other, 101).await,
            Err(DatabaseError::StorageCorrupt)
        );
        assert_eq!(
            descriptor_value(&base.pool(), &first.observation_key().unwrap()).await,
            before
        );
        base.close().await.unwrap();
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
    #[tokio::test]
    #[ignore = "requires scripts/test-postgres.sh"]
    async fn hierarchy_page_seeds_with_multiple_record_versions_enqueue_and_retry_without_corruption()
     {
        let (_fixture, base, store) = setup().await;
        let request = supplements::request(
            SupplementSource::StatuteHierarchyInventory,
            SupplementSeed::Global,
            1,
        )
        .unwrap();
        let tree = DocumentNode::Element {
            name: "LawSearch".into(),
            attributes: vec![],
            children: vec![
                field("totalCnt", "2"),
                DocumentNode::Element {
                    name: "law".into(),
                    attributes: vec![],
                    children: vec![field("법령ID", "123"), field("법령일련번호", "100")],
                },
                DocumentNode::Element {
                    name: "law".into(),
                    attributes: vec![],
                    children: vec![field("법령ID", "123"), field("법령일련번호", "101")],
                },
            ],
        };
        let page = supplements::inspect_page(&request, &tree, 0).unwrap();
        assert_eq!(page.seeds.len(), 2);
        assert!(!page.incomplete);
        assert_eq!(page.done, Some(true));
        for at in [100, 101] {
            for seed in &page.seeds {
                for next in supplements::seeded_requests(seed.clone(), 1).unwrap() {
                    store.enqueue_supplement(&next, at).await.unwrap();
                }
            }
        }
        let related_count: i64 = sqlx::query_scalar("SELECT count(*) FROM openlegal.provider_supplement_job WHERE source='related_statutes'")
            .fetch_one(&base.pool()).await.unwrap();
        assert_eq!(related_count, 1);
        let jobs: i64 =
            sqlx::query_scalar("SELECT count(*) FROM openlegal.provider_supplement_job")
                .fetch_one(&base.pool())
                .await
                .unwrap();
        assert_eq!(jobs, 13);
        base.close().await.unwrap();
    }
}
