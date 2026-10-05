//! Fixture-only PostgreSQL wire fault: commit succeeds but its acknowledgement
//! never reaches the operator connection. No payloads or credentials are logged.
use super::*;
use openlegal_application::persistence::PersistentStore;
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

#[tokio::test]
async fn fixture_packet_decoder_rejects_unbounded_or_truncated_frames() {
    for length in [0_u32, 3, MAX_PACKET as u32 + 1, u32::MAX] {
        let bytes = length.to_be_bytes();
        assert!(packet_body(&mut bytes.as_slice()).await.is_err());
    }
    assert!(packet_body(&mut [0, 0, 0, 8, 1].as_slice()).await.is_err());
    let mut bytes = Vec::new();
    write_packet(&mut bytes, Some(b'Q'), b"COMMIT\0")
        .await
        .unwrap();
    let mut reader = bytes.as_slice();
    assert_eq!(reader.read_u8().await.unwrap(), b'Q');
    assert_eq!(packet_body(&mut reader).await.unwrap(), b"COMMIT\0");
    assert!(reader.is_empty());
}

#[tokio::test]
#[ignore = "requires scripts/test-postgres.sh"]
async fn lost_commit_ack_returns_uncertain_and_readback_proves_one_held_recovery() {
    let fixture = crate::test_support::TestDatabase::new().await;
    let base = fixture.open(100).await;
    let pool = base.pool();
    sqlx::query("UPDATE openlegal.provider_request_budget SET unresolved_response=true,admission_owner=pg_catalog.uuidv7(),daily_used=9,on_demand_used=7,next_allowed_at=0")
        .execute(&pool).await.unwrap();
    sqlx::query("UPDATE openlegal_admin.provider_control SET legacy_identity=pg_catalog.uuidv7() WHERE singleton")
        .execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO openlegal_admin.uncertainty_observation(identity,kind,owner) SELECT c.legacy_identity,'legacy',b.admission_owner FROM openlegal_admin.provider_control c CROSS JOIN openlegal.provider_request_budget b")
        .execute(&pool).await.unwrap();
    let options = super::super::PostgresOptions {
        max_connections: 1,
        tls: super::super::PostgresTls::Plaintext,
    };
    let direct = ProviderAdminStore::open(&fixture.url, options.clone())
        .await
        .unwrap();
    let snapshot = direct.inspect_selected(&[]).await.unwrap();
    let request = ProviderRecoveryRequest {
        actor: "fixture-operator".into(),
        reason: "offline review before lost commit acknowledgement".into(),
        quiescence: ProviderQuiescenceEvidence {
            writers_stopped_at: snapshot.observed_at,
            deployment_revision: "fixture-release".into(),
            stopped_writer_ids: vec!["fixture-collector".into()],
            evidence_reference: "fixture-offline-proof".into(),
        },
        resolve_legacy: true,
        owners: vec![],
        resume: false,
        waits: vec![],
    };
    let plan = direct.plan(snapshot, request).unwrap();
    let mut proxy = CommitAckProxy::open(&fixture.url).await;
    let proxied = ProviderAdminStore::open(&proxy.url, options).await.unwrap();
    proxy.armed.store(true, Ordering::SeqCst);
    let outcome = tokio::time::timeout(IO_DEADLINE, proxied.apply(&plan))
        .await
        .unwrap();
    assert!(matches!(outcome, Err(ProviderAdminError::CommitUncertain)));
    tokio::time::timeout(IO_DEADLINE, &mut proxy.committed)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        proxy.forwarded_commits.load(Ordering::SeqCst),
        1,
        "apply must not retry COMMIT after its acknowledgement is lost"
    );
    let receipt = direct
        .readback(&plan.operation_id)
        .await
        .unwrap()
        .expect("committed recovery receipt");
    assert!(receipt.recovery_hold);
    assert!(receipt.resolved_legacy);
    assert!(held(&pool).await.unwrap());
    let budget: (bool, Option<Uuid>, i64, i64) = sqlx::query_as("SELECT unresolved_response,admission_owner,daily_used,on_demand_used FROM openlegal.provider_request_budget WHERE singleton")
        .fetch_one(&pool).await.unwrap();
    assert_eq!(budget, (false, None, 9, 7));
    let repeated = direct.apply(&plan).await.unwrap();
    assert_eq!(
        serde_json::to_value(repeated).unwrap(),
        serde_json::to_value(&receipt).unwrap()
    );
    let mut conflicting = plan.clone();
    conflicting.request.reason = "a different plan cannot reuse this operation identity".into();
    assert!(matches!(
        direct.apply(&conflicting).await,
        Err(ProviderAdminError::StalePlan)
    ));
    let audits: i64 =
        sqlx::query_scalar("SELECT count(*) FROM openlegal_admin.provider_recovery_audit")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(audits, 1);
    let observed: Option<i64> = sqlx::query_scalar(
        "SELECT first_observed_at FROM openlegal_admin.uncertainty_observation WHERE kind='legacy'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        observed, None,
        "operator recovery must preserve unknown collector observation time"
    );
    proxied.close().await;
    direct.close().await;
    drop(proxy);
    base.close().await.unwrap();
}
