//! Fresh, identity-bound PostgreSQL replica durability observations. No promotion,
//! credential provisioning, retry of mutations or asynchronous fallback lives here.
use futures::future::BoxFuture;
use openlegal_application::durability::DurabilityFence;
use openlegal_domain::RetrievalError as Error;
use serde::Deserialize;
use sqlx::PgPool;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::time::Duration;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

#[derive(Clone)]
pub struct ReplicationOptions {
    pub managed_epoch: Uuid,
    /// Operator-approved seed proof plus the actual four mirror-root identities.
    /// The server computes this binding before any storage recovery/GC.
    pub expected_blob_binding_sha256: String,
    /// Filesystem-only SSD process admission authority; never a PostgreSQL fence.
    /// Production composition supplies this before touching persistence.
    pub admission_guard: Option<Arc<dyn DurabilityFence>>,
    /// Cached comparison against the operator's held seed-root descriptors.
    pub root_verifier: Option<Arc<dyn openlegal_application::blob::BlobRootVerifier>>,
}
impl std::fmt::Debug for ReplicationOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReplicationOptions")
            .field("managed_epoch", &self.managed_epoch)
            .field(
                "expected_blob_binding_sha256",
                &self.expected_blob_binding_sha256,
            )
            .field(
                "admission_guard_configured",
                &self.admission_guard.is_some(),
            )
            .field("root_verifier_configured", &self.root_verifier.is_some())
            .finish()
    }
}
impl ReplicationOptions {
    pub fn for_epoch(managed_epoch: Uuid, expected_blob_binding_sha256: String) -> Self {
        Self {
            managed_epoch,
            expected_blob_binding_sha256,
            admission_guard: None,
            root_verifier: None,
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Receipt {
    managed_epoch: Uuid,
    blob_binding_sha256: String,
    primary_system_identifier: String,
    timeline: i64,
    postmaster_started_at: String,
    standby_name: String,
    standby_role: String,
    standby_addr: String,
    standby_client_dn: String,
    standby_issuer_dn: String,
    standby_client_serial: String,
    sender_pid: i32,
    sender_started_at: String,
    prefix_lsn: String,
    flush_lsn: String,
}
impl Receipt {
    fn valid(&self) -> bool {
        !self.managed_epoch.is_nil()
            && valid_digest(&self.blob_binding_sha256)
            && self.timeline > 0
            && self.sender_pid > 0
            && !self.primary_system_identifier.is_empty()
            && self.primary_system_identifier.len() <= 20
            && !self.standby_client_serial.is_empty()
            && self.standby_client_serial.len() <= 60
            && self
                .standby_client_serial
                .bytes()
                .all(|c| c.is_ascii_digit())
            && self
                .primary_system_identifier
                .bytes()
                .all(|c| c.is_ascii_digit())
            && [
                &self.postmaster_started_at,
                &self.standby_name,
                &self.standby_role,
                &self.standby_addr,
                &self.standby_client_dn,
                &self.standby_issuer_dn,
                &self.sender_started_at,
            ]
            .iter()
            .all(|s| !s.is_empty() && s.len() <= 1024)
            && lsn(&self.prefix_lsn).is_some()
            && lsn(&self.flush_lsn).is_some()
    }
    fn same_binding(&self, other: &Self) -> bool {
        self.managed_epoch == other.managed_epoch
            && self.blob_binding_sha256 == other.blob_binding_sha256
            && self.primary_system_identifier == other.primary_system_identifier
            && self.timeline == other.timeline
            && self.postmaster_started_at == other.postmaster_started_at
            && self.standby_name == other.standby_name
            && self.standby_role == other.standby_role
            && self.standby_addr == other.standby_addr
            && self.standby_client_dn == other.standby_client_dn
            && self.standby_issuer_dn == other.standby_issuer_dn
            && self.standby_client_serial == other.standby_client_serial
    }
    fn same_session(&self, other: &Self) -> bool {
        self.same_binding(other)
            && self.sender_pid == other.sender_pid
            && self.sender_started_at == other.sender_started_at
    }
}
fn valid_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
}
fn lsn(value: &str) -> Option<u64> {
    let (high, low) = value.split_once('/')?;
    if high.is_empty()
        || low.is_empty()
        || high.len() > 8
        || low.len() > 8
        || !high
            .bytes()
            .chain(low.bytes())
            .all(|c| c.is_ascii_hexdigit())
    {
        return None;
    }
    Some(
        (u64::from(u32::from_str_radix(high, 16).ok()?) << 32)
            | u64::from(u32::from_str_radix(low, 16).ok()?),
    )
}

pub struct PostgresDurabilityFence {
    pool: PgPool,
    binding: Receipt,
    failure_generation: Arc<AtomicU64>,
    admission_guard: Option<Arc<dyn DurabilityFence>>,
}
impl PostgresDurabilityFence {
    /// Called before any recovery/GC. The protected control row and current
    /// authenticated sender establish the binding; a different epoch is refused.
    pub async fn open(pool: PgPool, options: ReplicationOptions) -> Result<Arc<Self>, Error> {
        if options.managed_epoch.is_nil() || !valid_digest(&options.expected_blob_binding_sha256) {
            return Err(Error::InvalidInput);
        }
        if let Some(guard) = &options.admission_guard {
            guard
                .check(
                    Instant::now() + Duration::from_secs(2),
                    CancellationToken::new(),
                )
                .await?;
        }
        let binding = tokio::time::timeout(Duration::from_secs(2), observe(&pool, true))
            .await
            .map_err(|_| Error::StorageUnavailable)??;
        if binding.managed_epoch != options.managed_epoch
            || binding.blob_binding_sha256 != options.expected_blob_binding_sha256
        {
            return Err(Error::StorageUnavailable);
        }
        let this = Arc::new(Self {
            pool,
            binding,
            failure_generation: Arc::new(AtomicU64::new(0)),
            admission_guard: options.admission_guard,
        });
        this.check(
            Instant::now() + Duration::from_secs(2),
            CancellationToken::new(),
        )
        .await?;
        Ok(this)
    }
}
async fn observe(pool: &PgPool, prefix: bool) -> Result<Receipt, Error> {
    let value: serde_json::Value = sqlx::query_scalar(if prefix {
        "SELECT openlegal_replication.capture_prefix()"
    } else {
        "SELECT openlegal_replication.observe_flush()"
    })
    .fetch_one(pool)
    .await
    .map_err(|_| Error::StorageUnavailable)?;
    let receipt: Receipt = serde_json::from_value(value).map_err(|_| Error::StorageUnavailable)?;
    if !receipt.valid() {
        return Err(Error::StorageUnavailable);
    }
    Ok(receipt)
}
impl DurabilityFence for PostgresDurabilityFence {
    fn mark_uncertain(&self) {
        self.failure_generation.fetch_add(1, Ordering::AcqRel);
        if let Some(guard) = &self.admission_guard {
            guard.mark_uncertain();
        }
    }
    fn check(
        &self,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> BoxFuture<'static, Result<(), Error>> {
        let pool = self.pool.clone();
        let binding = self.binding.clone();
        let failure = self.failure_generation.clone();
        let admission_guard = self.admission_guard.clone();
        Box::pin(async move {
            let generation = failure.load(Ordering::Acquire);
            let limit = Instant::now() + Duration::from_secs(2);
            let caller_limited = deadline < limit;
            let deadline = deadline.min(limit);
            let work = async {
                if let Some(guard) = &admission_guard {
                    guard.check(deadline, cancellation.clone()).await?;
                }
                let prefix = observe(&pool, true).await?;
                if !prefix.same_binding(&binding) {
                    return Err(Error::StorageUnavailable);
                }
                let target = lsn(&prefix.prefix_lsn).ok_or(Error::StorageUnavailable)?;
                loop {
                    let receipt = observe(&pool, false).await?;
                    if !prefix.same_session(&receipt) {
                        return Err(Error::StorageUnavailable);
                    }
                    if lsn(&receipt.flush_lsn).ok_or(Error::StorageUnavailable)? >= target {
                        // Re-read identity/epoch after the flush observation. The
                        // new prefix need not be caught: it is not the target.
                        let final_receipt = observe(&pool, true).await?;
                        if !prefix.same_session(&final_receipt)
                            || failure.load(Ordering::Acquire) != generation
                        {
                            return Err(Error::StorageUnavailable);
                        }
                        if let Some(guard) = &admission_guard {
                            guard.check(deadline, cancellation.clone()).await?;
                        }
                        if failure.load(Ordering::Acquire) != generation {
                            return Err(Error::StorageUnavailable);
                        }
                        return Ok(());
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            };
            let (result, dependency_failed) = if deadline <= Instant::now() {
                (Err(Error::StorageUnavailable), false)
            } else {
                tokio::select! {
                    biased;
                    () = cancellation.cancelled() => (Err(Error::Cancelled),false),
                    result = tokio::time::timeout_at(deadline, work) => match result {
                        Ok(result) => {
                            let failed=matches!(result,Err(error) if !matches!(error,Error::Cancelled|Error::Busy));
                            (result,failed)
                        },
                        Err(_) => (Err(Error::StorageUnavailable),!caller_limited),
                    },
                }
            };
            if dependency_failed {
                failure.fetch_add(1, Ordering::AcqRel);
            }
            result
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    struct ClosedAdmission;
    impl DurabilityFence for ClosedAdmission {
        fn check(&self, _: Instant, _: CancellationToken) -> BoxFuture<'static, Result<(), Error>> {
            Box::pin(async { Err(Error::StorageUnavailable) })
        }
    }
    struct BusyAdmission;
    impl DurabilityFence for BusyAdmission {
        fn check(&self, _: Instant, _: CancellationToken) -> BoxFuture<'static, Result<(), Error>> {
            Box::pin(async { Err(Error::Busy) })
        }
    }
    fn receipt() -> Receipt {
        Receipt {
            managed_epoch: Uuid::from_u128(1),
            blob_binding_sha256: "1".repeat(64),
            primary_system_identifier: "123".into(),
            timeline: 1,
            postmaster_started_at: "2026-11-01 00:00:00+00".into(),
            standby_name: "openlegal_ssd".into(),
            standby_role: "replicator".into(),
            standby_addr: "127.0.0.1".into(),
            standby_client_dn: "CN=ssd".into(),
            standby_issuer_dn: "CN=private-ca".into(),
            standby_client_serial: "1234".into(),
            sender_pid: 123,
            sender_started_at: "2026-11-01 00:00:01+00".into(),
            prefix_lsn: "1/100".into(),
            flush_lsn: "1/100".into(),
        }
    }
    #[test]
    fn lsn_parsing_is_exact_and_bounded() {
        assert_eq!(lsn("1/FFFFFFFF"), Some(0x1ffffffff));
        for bad in [
            "",
            "1",
            "/1",
            "1/",
            "100000000/0",
            "0/100000000",
            "-1/0",
            "1/0/0",
            " 1/0",
        ] {
            assert_eq!(lsn(bad), None, "{bad}");
        }
    }
    #[test]
    fn receipt_never_compares_flush_across_primary_epoch_or_authenticated_sender_changes() {
        let original = receipt();
        assert!(original.valid());
        for mutate in [
            |r: &mut Receipt| r.managed_epoch = Uuid::from_u128(2),
            |r: &mut Receipt| r.blob_binding_sha256 = "2".repeat(64),
            |r: &mut Receipt| r.primary_system_identifier = "456".into(),
            |r: &mut Receipt| r.timeline = 2,
            |r: &mut Receipt| r.postmaster_started_at = "restarted".into(),
            |r: &mut Receipt| r.standby_name = "other".into(),
            |r: &mut Receipt| r.standby_role = "other".into(),
            |r: &mut Receipt| r.standby_addr = "127.0.0.2".into(),
            |r: &mut Receipt| r.standby_client_dn = "CN=other".into(),
            |r: &mut Receipt| r.standby_issuer_dn = "CN=other".into(),
            |r: &mut Receipt| r.standby_client_serial = "1235".into(),
            |r: &mut Receipt| r.sender_pid = 124,
            |r: &mut Receipt| r.sender_started_at = "reconnected".into(),
        ] {
            let mut changed = original.clone();
            mutate(&mut changed);
            assert!(!original.same_session(&changed));
        }
        let mut later = original.clone();
        later.prefix_lsn = "1/200".into();
        later.flush_lsn = "1/200".into();
        assert!(original.same_session(&later));
        let mut restarted = original.clone();
        restarted.postmaster_started_at = "another-primary-incarnation".into();
        assert!(!original.same_binding(&restarted));
    }
    #[tokio::test]
    async fn expired_deadline_and_cancelled_call_do_not_open_a_database_connection() {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgresql://127.0.0.1:1/no-server")
            .unwrap();
        let fence = PostgresDurabilityFence {
            pool,
            binding: receipt(),
            failure_generation: Arc::new(AtomicU64::new(0)),
            admission_guard: None,
        };
        assert_eq!(
            fence.check(Instant::now(), CancellationToken::new()).await,
            Err(Error::StorageUnavailable)
        );
        let token = CancellationToken::new();
        token.cancel();
        assert_eq!(
            fence
                .check(Instant::now() + Duration::from_secs(60), token)
                .await,
            Err(Error::Cancelled)
        );
        assert_eq!(
            fence.failure_generation.load(Ordering::Acquire),
            0,
            "request-local cancellation/expired budget is not dependency failure evidence"
        );
    }
    #[tokio::test]
    async fn stale_process_admission_refuses_before_reading_postgres() {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgresql://127.0.0.1:1/no-server")
            .unwrap();
        let fence = PostgresDurabilityFence {
            pool,
            binding: receipt(),
            failure_generation: Arc::new(AtomicU64::new(0)),
            admission_guard: Some(Arc::new(ClosedAdmission)),
        };
        assert_eq!(
            fence
                .check(
                    Instant::now() + Duration::from_secs(2),
                    CancellationToken::new()
                )
                .await,
            Err(Error::StorageUnavailable)
        );
        assert_eq!(fence.pool.size(), 0);
    }
    #[tokio::test]
    async fn guard_capacity_is_request_local_not_a_dependency_failure_epoch() {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgresql://127.0.0.1:1/no-server")
            .unwrap();
        let fence = PostgresDurabilityFence {
            pool,
            binding: receipt(),
            failure_generation: Arc::new(AtomicU64::new(0)),
            admission_guard: Some(Arc::new(BusyAdmission)),
        };
        assert_eq!(
            fence
                .check(
                    Instant::now() + Duration::from_secs(2),
                    CancellationToken::new()
                )
                .await,
            Err(Error::Busy)
        );
        assert_eq!(fence.failure_generation.load(Ordering::Acquire), 0);
        assert_eq!(fence.pool.size(), 0);
    }
}
