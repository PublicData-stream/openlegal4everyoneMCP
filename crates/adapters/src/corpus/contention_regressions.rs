//! Fixture-only PostgreSQL wire fault: commit succeeds but its acknowledgement
//! never reaches the operator connection. No payloads or credentials are logged.
use super::*;
use crate::law_go_kr::{
    InventoryItem, InventoryPage,
    supplements::{self, SupplementSeed, SupplementSource},
};
use openlegal_application::persistence::PersistentStore;
use openlegal_domain::rights::SourceRights;
use std::str::FromStr;
use std::time::Duration;
use std::{
    io,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::oneshot,
    task::{JoinHandle, JoinSet},
};
use tokio_util::sync::CancellationToken;

const MAX_PACKET: usize = 1024 * 1024;
const IO_DEADLINE: Duration = Duration::from_secs(10);

async fn packet_body<R: AsyncRead + Unpin>(reader: &mut R) -> io::Result<Vec<u8>> {
    let length = reader.read_u32().await? as usize;
    if !(4..=MAX_PACKET).contains(&length) {
        return Err(io::Error::other("invalid fixture protocol packet length"));
    }
    let mut bytes = vec![0; length - 4];
    reader.read_exact(&mut bytes).await?;
    Ok(bytes)
}

async fn write_packet<W: AsyncWrite + Unpin>(
    writer: &mut W,
    kind: Option<u8>,
    body: &[u8],
) -> io::Result<()> {
    if body.len() > MAX_PACKET - 4 {
        return Err(io::Error::other("oversized fixture protocol packet"));
    }
    if let Some(kind) = kind {
        writer.write_u8(kind).await?;
    }
    writer.write_u32((body.len() + 4) as u32).await?;
    writer.write_all(body).await
}

type CommitSignal = Arc<Mutex<Option<oneshot::Sender<()>>>>;

async fn relay_connection(
    mut client: TcpStream,
    mut backend: TcpStream,
    armed: Arc<AtomicBool>,
    forwarded_commits: Arc<AtomicUsize>,
    committed: CommitSignal,
) -> io::Result<()> {
    // Plaintext PgConnectOptions never sends SSLRequest; the untagged initial
    // packet must be protocol 3 StartupMessage, before tagged authentication.
    let startup = tokio::time::timeout(IO_DEADLINE, packet_body(&mut client))
        .await
        .map_err(|_| io::Error::other("fixture startup deadline"))??;
    if startup.get(..4) != Some(&196608_u32.to_be_bytes()) {
        return Err(io::Error::other("unsupported fixture startup protocol"));
    }
    write_packet(&mut backend, None, &startup).await?;
    let (mut client_read, mut client_write) = client.into_split();
    let (mut backend_read, mut backend_write) = backend.into_split();
    let suppress_ack = AtomicBool::new(false);
    let upstream = async {
        loop {
            let kind = match client_read.read_u8().await {
                Ok(kind) => kind,
                Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => return Ok(()),
                Err(error) => return Err(error),
            };
            let body = packet_body(&mut client_read).await?;
            // SQLx 0.9 PgTransactionManager::commit executes COMMIT without
            // arguments; the executor emits a SimpleQuery ('Q') packet.
            if kind == b'Q' && body == b"COMMIT\0" {
                forwarded_commits.fetch_add(1, Ordering::SeqCst);
                if armed.swap(false, Ordering::SeqCst) {
                    suppress_ack.store(true, Ordering::SeqCst);
                }
            }
            write_packet(&mut backend_write, Some(kind), &body).await?;
        }
    };
    let downstream = async {
        loop {
            let kind = match backend_read.read_u8().await {
                Ok(kind) => kind,
                Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => return Ok(()),
                Err(error) => return Err(error),
            };
            let body = packet_body(&mut backend_read).await?;
            if suppress_ack.load(Ordering::SeqCst) && kind == b'C' && body == b"COMMIT\0" {
                // PostgreSQL already completed COMMIT. Suppress its
                // CommandComplete and the following ReadyForQuery. Dropping
                // both relays closes the client's connection without an ack.
                if let Some(signal) = committed
                    .lock()
                    .map_err(|_| io::Error::other("fixture signal lock unavailable"))?
                    .take()
                {
                    let _ = signal.send(());
                }
                return Ok(());
            }
            write_packet(&mut client_write, Some(kind), &body).await?;
        }
    };
    tokio::select! { result = upstream => result, result = downstream => result }
}

struct CommitAckProxy {
    url: String,
    armed: Arc<AtomicBool>,
    forwarded_commits: Arc<AtomicUsize>,
    committed: oneshot::Receiver<()>,
    cancel: CancellationToken,
    task: JoinHandle<()>,
}
impl CommitAckProxy {
    async fn open(url: &str) -> Self {
        let mut parsed = url::Url::parse(url).expect("fixture database URL syntax");
        assert!(
            !parsed
                .query_pairs()
                .any(|(name, _)| matches!(name.as_ref(), "host" | "port"))
        );
        let upstream_host = parsed.host_str().expect("fixture database host").to_owned();
        let upstream_port = parsed.port().unwrap_or(5432);
        let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        parsed.set_host(Some("127.0.0.1")).unwrap();
        parsed
            .set_port(Some(listener.local_addr().unwrap().port()))
            .unwrap();
        let armed = Arc::new(AtomicBool::new(false));
        let forwarded_commits = Arc::new(AtomicUsize::new(0));
        let (signal, committed) = oneshot::channel();
        let signal = Arc::new(Mutex::new(Some(signal)));
        let cancel = CancellationToken::new();
        let token = cancel.clone();
        let arm = armed.clone();
        let commits = forwarded_commits.clone();
        let task = tokio::spawn(async move {
            let mut relays = JoinSet::new();
            loop {
                tokio::select! {
                    _ = token.cancelled() => break,
                    accepted = listener.accept() => {
                        let Ok((client, _)) = accepted else { break };
                        let host = upstream_host.clone();
                        let arm = arm.clone();
                        let commits = commits.clone();
                        let signal = signal.clone();
                        relays.spawn(async move {
                            let _ = tokio::time::timeout(Duration::from_secs(30), async {
                                let backend = TcpStream::connect((host.as_str(), upstream_port)).await?;
                                relay_connection(client, backend, arm, commits, signal).await
                            }).await;
                        });
                    }
                    _ = relays.join_next(), if !relays.is_empty() => {}
                }
            }
            relays.abort_all();
            while relays.join_next().await.is_some() {}
        });
        Self {
            url: parsed.into(),
            armed,
            forwarded_commits,
            committed,
            cancel,
            task,
        }
    }
}
impl Drop for CommitAckProxy {
    fn drop(&mut self) {
        self.cancel.cancel();
        self.task.abort();
    }
}

async fn setup() -> (
    crate::test_support::TestDatabase,
    Arc<crate::postgres::PostgresStore>,
    PgCorpusStore,
) {
    let fixture = crate::test_support::TestDatabase::new().await;
    let base = fixture.open(100).await;
    let blobs =
        crate::blob::FsBlobStore::open(&fixture.directory.path().join("contention-regressions"))
            .await
            .unwrap();
    let store = PgCorpusStore::new(base.pool(), blobs);
    (fixture, base, store)
}

fn object(id: &str) -> ObjectId {
    ObjectId {
        jurisdiction: "kr".into(),
        provider: "fictional_test".into(),
        dataset: Dataset::NationalStatute,
        id: id.into(),
    }
}
fn publication(object: ObjectId, version: u64, job: Option<String>, now: u64) -> Publication {
    Publication {
        record: LegalRecord {
            object,
            revision_id: "r1".into(),
            title: "Fictional regression".into(),
            body: "Fixture body".into(),
            metadata: Default::default(),
            publication_date: None,
            effective_date: None,
            source_url: "https://example.test/fixture".into(),
            representation: "provider_text_v1".into(),
            sections: vec![],
        },
        raw: b"Fixture body".to_vec(),
        additional_evidence: vec![],
        processor_version: "fixture_v1".into(),
        retrieved_at: now,
        now,
        expected_version: version,
        install_head: true,
        job_id: job,
    }
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn lost_observation_commit_ack_is_not_retried_and_direct_readback_proves_one_commit() {
    let (fixture, base, _store) = setup().await;
    let mut proxy = CommitAckProxy::open(&fixture.url).await;
    let options = sqlx::postgres::PgConnectOptions::from_str(&proxy.url)
        .unwrap()
        .ssl_mode(sqlx::postgres::PgSslMode::Disable);
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .connect_with(options)
        .await
        .unwrap();
    let blobs =
        crate::blob::FsBlobStore::open(&fixture.directory.path().join("uncertain-observation"))
            .await
            .unwrap();
    let store = PgCorpusStore::new(pool.clone(), blobs);
    proxy.armed.store(true, Ordering::SeqCst);
    let result = tokio::time::timeout(
        IO_DEADLINE,
        store.retain_source_observation(
            SourceObservationInput {
                source_key: "law_go_kr:lsEfYdInfoGuide:lost_commit_fixture".into(),
                raw: None,
                media_type: "application/xml".into(),
                rights: SourceRights::legal_information(),
                metadata: Default::default(),
                observed_at: 100,
            },
            CancellationToken::new(),
        ),
    )
    .await
    .unwrap();
    assert_eq!(result, Err(DatabaseError::StorageUnavailable));
    tokio::time::timeout(IO_DEADLINE, &mut proxy.committed)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(proxy.forwarded_commits.load(Ordering::SeqCst), 1);
    let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM openlegal.corpus_source_observation WHERE source_key='law_go_kr:lsEfYdInfoGuide:lost_commit_fixture'").fetch_one(&base.pool()).await.unwrap();
    assert_eq!(
        rows, 1,
        "server committed despite lost acknowledgement; no retry is safe"
    );
    pool.close().await;
    drop(proxy);
    base.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn inventory_partial_checkpoint_and_completed_cycle_each_commit_once_after_rejection() {
    let (_fixture, base, store) = setup().await;
    let view = CloneView {
        dataset: Dataset::NationalStatute,
        historical: false,
        treaty_class: None,
    };
    assert_eq!(store.clone_cursor(view).await.unwrap(), (1, 0));
    let page = InventoryPage {
        source_evidence: None,
        items: ["first", "second"]
            .map(|id| InventoryItem {
                object: object(id),
                revision_id: "r1".into(),
                effective_date: None,
                publication_date: None,
                title: "Fictional inventory".into(),
                data_source: None,
                case_number: None,
                treaty_class_code: None,
                amendment_type: None,
            })
            .to_vec(),
        done: true,
        total: Some(2),
        rejected_rows: 0,
        incomplete: false,
    };
    assert_eq!(store.clone_page_offset(view, 1, &page).await.unwrap(), 0);
    // Sequence increments survive transaction rollback, proving both attempts
    // reached the real database instead of testing a mocked error enum.
    sqlx::raw_sql("CREATE SEQUENCE public.inventory_rejections; CREATE FUNCTION public.reject_inventory_once_per_step() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF nextval('public.inventory_rejections') IN (1,3) THEN RAISE EXCEPTION USING ERRCODE='55P03', MESSAGE='fixture inventory rejection'; END IF; RETURN NEW; END $$; CREATE TRIGGER reject_inventory BEFORE UPDATE ON openlegal.provider_clone_view FOR EACH ROW EXECUTE FUNCTION public.reject_inventory_once_per_step();").execute(&base.pool()).await.unwrap();
    store
        .clone_page_scheduled(view, 1, 1, &page, 100, &CancellationToken::new())
        .await
        .unwrap();
    let partial: (i32, i32, i64) =
        sqlx::query_as("SELECT next_page,item_offset,cycle FROM openlegal.provider_clone_view")
            .fetch_one(&base.pool())
            .await
            .unwrap();
    assert_eq!(partial, (1, 1, 1));
    store
        .clone_page_scheduled(view, 1, 2, &page, 101, &CancellationToken::new())
        .await
        .unwrap();
    let complete: (i32, i32, i64, i32) = sqlx::query_as(
        "SELECT next_page,item_offset,cycle,stable_cycles FROM openlegal.provider_clone_view",
    )
    .fetch_one(&base.pool())
    .await
    .unwrap();
    assert_eq!(complete, (1, 0, 2, 1));
    let attempts: i64 = sqlx::query_scalar("SELECT last_value FROM public.inventory_rejections")
        .fetch_one(&base.pool())
        .await
        .unwrap();
    assert_eq!(attempts, 4);
    let members: i64 = sqlx::query_scalar("SELECT count(*) FROM openlegal.provider_clone_member")
        .fetch_one(&base.pool())
        .await
        .unwrap();
    assert_eq!(members, 2);
    base.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn supplement_owner_and_successor_settle_atomically_after_rejected_insert() {
    let (_fixture, base, store) = setup().await;
    let request = supplements::request(
        SupplementSource::StatuteHierarchyInventory,
        SupplementSeed::Global,
        1,
    )
    .unwrap();
    assert!(store.enqueue_supplement(&request, 100).await.unwrap());
    let claim = store.claim_supplement(100).await.unwrap().unwrap();
    let owner = claim.lease_owner.clone();
    let observation = store
        .retain_source_observation(
            SourceObservationInput {
                source_key: claim.key.clone(),
                raw: Some(b"<list>fixture</list>".to_vec()),
                media_type: "application/xml".into(),
                rights: SourceRights::legal_information(),
                metadata: Default::default(),
                observed_at: 100,
            },
            CancellationToken::new(),
        )
        .await
        .unwrap();
    sqlx::raw_sql("CREATE SEQUENCE public.supplement_rejections; CREATE FUNCTION public.reject_successor_once() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.page=2 AND nextval('public.supplement_rejections')=1 THEN RAISE EXCEPTION USING ERRCODE='55P03', MESSAGE='fixture successor rejection'; END IF; RETURN NEW; END $$; CREATE TRIGGER reject_successor BEFORE INSERT ON openlegal.provider_supplement_job FOR EACH ROW EXECUTE FUNCTION public.reject_successor_once();").execute(&base.pool()).await.unwrap();
    let successor = request.next_page().unwrap();
    store
        .settle_supplement_with_successor(
            &claim,
            SupplementJobStatus::Done,
            Some(&observation.observation_id),
            100,
            100,
            Some(&successor),
        )
        .await
        .unwrap();
    let parent: (String,Option<Uuid>,String,i64) = sqlx::query_as("SELECT status,owner,observation_id,observed_rows::bigint FROM openlegal.provider_supplement_job WHERE job_key=$1").bind(&claim.key).fetch_one(&base.pool()).await.unwrap();
    assert_eq!(
        parent,
        ("done".into(), None, observation.observation_id, 100)
    );
    let next = store.claim_supplement(101).await.unwrap().unwrap();
    assert_eq!(next.key, successor.observation_key().unwrap());
    assert_ne!(next.lease_owner, owner);
    assert_eq!(next.observed_before, 100);
    assert_eq!(
        store
            .settle_supplement(&claim, SupplementJobStatus::Incomplete, None, 0, 101)
            .await,
        Err(DatabaseError::Conflict)
    );
    let counts: (i64,i64) = sqlx::query_as("SELECT (SELECT count(*) FROM openlegal.provider_supplement_job),(SELECT last_value FROM public.supplement_rejections)").fetch_one(&base.pool()).await.unwrap();
    assert_eq!(counts, (2, 2));
    base.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn detail_storage_cooldown_preserves_charges_and_fences_old_publications_before_and_after_reclaim()
 {
    let (_fixture, base, store) = setup().await;
    let now: i64 =
        sqlx::query_scalar("SELECT floor(extract(epoch from clock_timestamp()))::bigint")
            .fetch_one(&base.pool())
            .await
            .unwrap();
    let now = now as u64;
    sqlx::query("UPDATE openlegal.provider_request_budget SET daily_used=9,on_demand_used=7")
        .execute(&base.pool())
        .await
        .unwrap();
    let identity = object("cooldown");
    store
        .enqueue_job(identity.clone(), "r1".into(), None, true, true, now)
        .await
        .unwrap();
    let claim = store.claim_job(now).await.unwrap().unwrap();
    store.defer_storage_claim(&claim).await.unwrap();
    let state: (String,Option<String>,i64,i32,i64,i64) = sqlx::query_as("SELECT j.status,j.error_category,j.lease_until::bigint,j.attempts,b.daily_used,b.on_demand_used FROM openlegal.corpus_job j CROSS JOIN openlegal.provider_request_budget b").fetch_one(&base.pool()).await.unwrap();
    assert_eq!(state.0, "running");
    assert_eq!(state.1.as_deref(), Some("processing_failed"));
    assert!(state.2 > now as i64);
    assert_eq!(state.3, claim.attempts as i32);
    assert_eq!((state.4, state.5), (9, 7));
    assert!(matches!(
        store
            .publish(
                publication(
                    identity.clone(),
                    claim.expected_version,
                    Some(claim.id.clone()),
                    now
                ),
                CancellationToken::new()
            )
            .await,
        Err(DatabaseError::Conflict)
    ));
    assert!(store.claim_job(now).await.unwrap().is_none());
    sqlx::query("UPDATE openlegal.corpus_job SET lease_until=$1::text::numeric")
        .bind(now.saturating_sub(1).to_string())
        .execute(&base.pool())
        .await
        .unwrap();
    let next = store.claim_job(now).await.unwrap().unwrap();
    assert_eq!(next.id, claim.id);
    assert!(next.expected_version > claim.expected_version);
    assert_eq!(next.attempts, claim.attempts + 1);
    assert!(matches!(
        store
            .publish(
                publication(identity, claim.expected_version, Some(claim.id), now),
                CancellationToken::new()
            )
            .await,
        Err(DatabaseError::Conflict)
    ));
    let counts: (i64,i64,i64,i64) = sqlx::query_as("SELECT (SELECT count(*) FROM openlegal.corpus_capture),(SELECT count(*) FROM openlegal.corpus_outbox),daily_used,on_demand_used FROM openlegal.provider_request_budget").fetch_one(&base.pool()).await.unwrap();
    assert_eq!(counts, (0, 0, 9, 7));
    base.close().await.unwrap();
}

use openlegal_application::blob::*;
use openlegal_domain::RetrievalError;
struct LockAfterDelete {
    inner: Arc<dyn BlobStore>,
    pool: PgPool,
    deletes: Arc<AtomicUsize>,
    arm_after_put: Option<Arc<AtomicBool>>,
    release: Arc<tokio::sync::Mutex<Option<tokio::task::JoinHandle<()>>>>,
}
impl BlobStore for LockAfterDelete {
    fn put_if_absent(
        &self,
        location: BlobLocation,
        bytes: Vec<u8>,
        cancel: CancellationToken,
    ) -> BoxFuture<'static, Result<BlobPutResult, RetrievalError>> {
        let inner = self.inner.clone();
        let arm = self.arm_after_put.clone();
        Box::pin(async move {
            let result = inner.put_if_absent(location, bytes, cancel).await?;
            if let Some(arm) = arm {
                arm.store(true, Ordering::SeqCst);
            }
            Ok(result)
        })
    }
    fn delete_if_present(
        &self,
        location: BlobLocation,
        cancel: CancellationToken,
    ) -> BoxFuture<'static, Result<(), RetrievalError>> {
        let inner = self.inner.clone();
        let pool = self.pool.clone();
        let deletes = self.deletes.clone();
        let release = self.release.clone();
        Box::pin(async move {
            deletes.fetch_add(1, Ordering::SeqCst);
            inner.delete_if_present(location.clone(), cancel).await?;
            let mut tx = pool.begin().await.unwrap();
            sqlx::query(
                    "SELECT storage_key FROM openlegal.corpus_blob_deletion WHERE storage_key=$1 FOR UPDATE",
                )
                .bind(&location.storage_key)
                    .fetch_one(&mut *tx)
                .await
                .unwrap();
            *release.lock().await = Some(tokio::spawn(async move {
                tokio::time::sleep(std::time::Duration::from_millis(1250)).await;
                tx.commit().await.unwrap();
            }));
            Ok(())
        })
    }
    fn get(
        &self,
        location: BlobLocation,
        cancel: CancellationToken,
    ) -> BoxFuture<'static, Result<Option<Vec<u8>>, RetrievalError>> {
        self.inner.get(location, cancel)
    }
    fn enumerate(
        &self,
        cursor: Option<String>,
        limit: usize,
        cancel: CancellationToken,
    ) -> BoxFuture<'static, Result<BlobPage, RetrievalError>> {
        self.inner.enumerate(cursor, limit, cancel)
    }
    fn cleanup_staging(
        &self,
        now: u64,
        limit: usize,
        cancel: CancellationToken,
    ) -> BoxFuture<'static, Result<usize, RetrievalError>> {
        self.inner.cleanup_staging(now, limit, cancel)
    }
    fn health(&self, cancel: CancellationToken) -> BoxFuture<'static, Result<(), RetrievalError>> {
        self.inner.health(cancel)
    }
    fn close(&self) -> BoxFuture<'static, Result<(), RetrievalError>> {
        self.inner.close()
    }
    fn metrics(&self) -> BlobMetrics {
        self.inner.metrics()
    }
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn maintenance_deletes_blob_once_when_queue_sql_times_out_then_recovers() {
    let (fixture, base, _store) = setup().await;
    let inner = crate::blob::FsBlobStore::open(&fixture.directory.path().join("delete-retry"))
        .await
        .unwrap();
    let raw = b"unreferenced fixture bytes".to_vec();
    let location = BlobLocation {
        digest: bytes_hash(&raw).try_into().unwrap(),
        size_bytes: raw.len() as u64,
        storage_key: format!(
            "{}/{}-01990000-0000-7000-8000-000000000001",
            &hex(&bytes_hash(&raw))[..2],
            hex(&bytes_hash(&raw))
        ),
    };
    inner
        .put_if_absent(location.clone(), raw, CancellationToken::new())
        .await
        .unwrap();
    sqlx::query("INSERT INTO openlegal.corpus_blob_deletion VALUES($1,$2,$3)")
        .bind(&location.storage_key)
        .bind(location.digest.to_vec())
        .bind(location.size_bytes as i64)
        .execute(&base.pool())
        .await
        .unwrap();
    let deletes = Arc::new(AtomicUsize::new(0));
    let release = Arc::new(tokio::sync::Mutex::new(None));
    let blobs = Arc::new(LockAfterDelete {
        inner: inner.clone(),
        pool: base.pool(),
        deletes: deletes.clone(),
        arm_after_put: None,
        release: release.clone(),
    });
    let store = PgCorpusStore::new(base.pool(), blobs);
    store.maintain(100, 0).await.unwrap();
    release.lock().await.take().unwrap().await.unwrap();
    assert_eq!(deletes.load(Ordering::SeqCst), 1);
    assert!(
        inner
            .get(location, CancellationToken::new())
            .await
            .unwrap()
            .is_none()
    );
    let queue: i64 = sqlx::query_scalar("SELECT count(*) FROM openlegal.corpus_blob_deletion")
        .fetch_one(&base.pool())
        .await
        .unwrap();
    assert_eq!(queue, 0);
    base.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn publication_reserve_final_and_dedup_commit_ack_loss_never_replays_transactions() {
    for phase in ["reserve", "final", "dedup"] {
        let (fixture, base, direct) = setup().await;
        let identity = object(phase);
        let initial_version = if phase == "dedup" {
            direct
                .publish(
                    publication(identity.clone(), 0, None, 100),
                    CancellationToken::new(),
                )
                .await
                .unwrap();
            direct.state(&identity).await.unwrap().version
        } else {
            0
        };
        let mut proxy = CommitAckProxy::open(&fixture.url).await;
        let options = sqlx::postgres::PgConnectOptions::from_str(&proxy.url)
            .unwrap()
            .ssl_mode(sqlx::postgres::PgSslMode::Disable);
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(2)
            .connect_with(options)
            .await
            .unwrap();
        // Use the same physical root as the initial direct publication so that
        // the dedup path verifies original evidence before its second COMMIT.
        let inner = crate::blob::FsBlobStore::open(
            &fixture.directory.path().join("contention-regressions"),
        )
        .await
        .unwrap();
        let blobs = Arc::new(LockAfterDelete {
            inner: inner.clone(),
            pool: base.pool(),
            deletes: Arc::new(AtomicUsize::new(0)),
            arm_after_put: (phase != "reserve").then(|| proxy.armed.clone()),
            release: Arc::new(tokio::sync::Mutex::new(None)),
        });
        let store = PgCorpusStore::new(pool.clone(), blobs);
        if phase == "reserve" {
            proxy.armed.store(true, Ordering::SeqCst);
        }
        let result = tokio::time::timeout(
            IO_DEADLINE,
            store.publish(
                publication(identity, initial_version, None, 100),
                CancellationToken::new(),
            ),
        )
        .await
        .unwrap();
        assert!(
            matches!(result, Err(DatabaseError::StorageUnavailable)),
            "phase={phase}"
        );
        tokio::time::timeout(IO_DEADLINE, &mut proxy.committed)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            proxy.forwarded_commits.load(Ordering::SeqCst),
            if phase == "reserve" { 1 } else { 2 },
            "phase={phase}"
        );
        let state: (i64,i64,i64,i64,i64) = sqlx::query_as("SELECT raw_bytes,staged_bytes,(SELECT count(*) FROM openlegal.corpus_capture),(SELECT count(*) FROM openlegal.corpus_outbox),(SELECT count(*) FROM openlegal.corpus_blob_deletion) FROM openlegal.corpus_control").fetch_one(&base.pool()).await.unwrap();
        match phase {
            "reserve" => {
                assert_eq!(state, (0, 12, 0, 0, 0));
                assert_eq!(inner.metrics().writes, 0);
            }
            "final" => assert_eq!(state, (12, 0, 1, 1, 0)),
            "dedup" => assert_eq!(state, (12, 0, 1, 1, 1)),
            _ => unreachable!("fixture phase"),
        }
        pool.close().await;
        drop(proxy);
        base.close().await.unwrap();
    }
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn retained_observation_commit_ack_loss_preserves_one_blob_and_one_observation() {
    let (fixture, base, _store) = setup().await;
    let mut proxy = CommitAckProxy::open(&fixture.url).await;
    let options = sqlx::postgres::PgConnectOptions::from_str(&proxy.url)
        .unwrap()
        .ssl_mode(sqlx::postgres::PgSslMode::Disable);
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .connect_with(options)
        .await
        .unwrap();
    let inner =
        crate::blob::FsBlobStore::open(&fixture.directory.path().join("retained-commit-loss"))
            .await
            .unwrap();
    let blobs = Arc::new(LockAfterDelete {
        inner: inner.clone(),
        pool: base.pool(),
        deletes: Arc::new(AtomicUsize::new(0)),
        arm_after_put: Some(proxy.armed.clone()),
        release: Arc::new(tokio::sync::Mutex::new(None)),
    });
    let store = PgCorpusStore::new(pool.clone(), blobs);
    let raw = b"Observation fixture".to_vec();
    let result = tokio::time::timeout(
        IO_DEADLINE,
        store.retain_source_observation(
            SourceObservationInput {
                source_key: "law_go_kr:lsEfYdInfoGuide:retained_commit_fixture".into(),
                raw: Some(raw.clone()),
                media_type: "application/xml".into(),
                rights: SourceRights::legal_information(),
                metadata: Default::default(),
                observed_at: 100,
            },
            CancellationToken::new(),
        ),
    )
    .await
    .unwrap();
    assert_eq!(result, Err(DatabaseError::StorageUnavailable));
    tokio::time::timeout(IO_DEADLINE, &mut proxy.committed)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(proxy.forwarded_commits.load(Ordering::SeqCst), 2);
    assert_eq!(inner.metrics().writes, 1);
    let state:(i64,i64,i64)=sqlx::query_as("SELECT raw_bytes,staged_bytes,(SELECT count(*) FROM openlegal.corpus_source_observation) FROM openlegal.corpus_control")
        .fetch_one(&base.pool()).await.unwrap();
    assert_eq!(state, (raw.len() as i64, 0, 1));
    pool.close().await;
    drop(proxy);
    base.close().await.unwrap();
}
