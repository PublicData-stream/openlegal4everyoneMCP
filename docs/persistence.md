# PostgreSQL 18 and immutable blob persistence

Persistent mode combines PostgreSQL 18.x with a provider-neutral `BlobStore` beneath
the process-local memory cache. This is the only supported persistent architecture.
The initial blob adapter uses a dedicated filesystem directory; it is an immutable
object store, not a filesystem database. Explicit memory mode is non-persistent.
Retrieval-cache captures are synthetic observations. A cache observation is not
an authoritative legal revision, a legal applicability decision, or a permanent
archive. The [legal corpus](database.md) uses the same PostgreSQL migrations with
its own tables, blob directory, search index and permanent archive.

## Configuration and startup

Every registered retrieval source requires an explicit mode. For persistent mode:

```toml
[cache]
mode = "persistent"
retention_days = 30
max_snapshots_per_query = 100
max_snapshots = 10000
max_queries = 4096
max_blob_bytes = 1073741824

[cache.postgres]
url_env = "OPENLEGAL_DATABASE_URL"
migration_url_env = "OPENLEGAL_MIGRATION_DATABASE_URL"
max_connections = 16
tls_mode = "verify-full"
# ca_file = "/etc/openlegal/postgres-ca.pem"

[cache.blob]
kind = "filesystem"
path = "/var/lib/openlegal/blobs"
```

Store connection URLs in operator-provided environment variables, not TOML, source
control, MCP inputs, or metrics. Runtime and migration variable names must differ.
Ordinary serving never reads the migration secret. Verified TLS is the default;
the optional CA file supplies the PostgreSQL trust root. `plaintext` must be an
explicit deployment choice for an isolated demonstration or a separately protected
local connection. Opportunistic TLS downgrade is not supported.

Use a PostgreSQL role with schema-creation and migration ownership for migrations,
and a restricted runtime role with the grants described below. After preparing the
database and secret environment, run:

```sh
openlegal-server --migrate CONFIG.toml
openlegal-server CONFIG.toml
```

The migration command executes checked-in SQL migrations using the migration
credential, then exits. It does not initialize blob storage, load widgets, start
comparison workers, contact upstreams, or bind listeners. Serving startup verifies
PostgreSQL 18.x, native `uuidv7()`, the complete successful migration/checksum state,
retention accounting, and blob capabilities before admission. Pending, missing,
modified or incompatible migration state fails clearly. Compilation does not need
a live database, `DATABASE_URL`, or generated SQL query metadata.

The configured connection limit (2–64, default 16) includes one reserved health
connection. Foreground saturation cannot consume this probe connection. Native
certificate roots and optional operator CA files supply verified TLS trust.
Connection URLs accept only identity/address query parameters (`host`, `port`,
`dbname`, `user`, `password`); configure TLS through TOML. Unknown URL parameters
are rejected before driver parsing. Pool acquisition and lock waits are limited
to one second, statements to two seconds, transactions to five seconds, and a
persistent operation to twenty seconds. Blob operations have a five-second caller
deadline by default. These retrieval-cache bounds remain independent of the
optional corpus setting `[database].blob_operation_timeout_secs`: whole seconds
from 1 through 60, default five. The corpus setting covers its blob startup,
individual operations, shared durability probes and close/drain in serving,
background collection and offline index rebuilding. It does not change the cache
adapter, SQL transaction limits or outer MCP/document-worker budgets. A longer
corpus blob allowance does not extend an enclosing caller deadline.

These deadlines do not interrupt a blocked filesystem syscall. A caller timeout
or cancellation leaves the owned watchdog and physical job responsible for its
admission permit until actual completion. Successful health recovery still
requires the matching failure generation and older jobs to have drained;
publication still waits for durable blob acknowledgment before its final SQL
commit. SQL outcome uncertainty and deletion durability are unchanged.

The initial migration uses the `openlegal` schema and SQLx's migration ledger in
`public`. The runtime role needs `USAGE` on `openlegal`, `SELECT` on
`public._sqlx_migrations`, and `SELECT`, `INSERT`, `UPDATE`, `DELETE` on the storage
tables. It does not require `CREATE` on the database/schema or migration ownership.
Database administrators own role provisioning and grants; migrations do not create
passwords or roles. Keep both schemas free of untrusted object creators. The adapter
uses schema-qualified storage SQL and parameterized values.

For Kubernetes, the [first-deployment sequence](deployment-kubernetes.md#first-deployment)
orders role provisioning, migration completion and runtime grants before serving.
Use the [rollback and recovery procedure](deployment-kubernetes.md#rollback-and-recovery)
to coordinate database, blob and index recovery against these persistence contracts.

For an isolated non-persistent demonstration, replace the whole cache configuration
with:

```toml
[cache]
mode = "memory"
```

Memory mode rejects persistent subsections and retention options. There is no
fallback persistent backend. Old filesystem cache configuration fails parsing.
There is intentionally no migration utility for the disposable pre-production
filesystem cache; operators may discard its old directory after replacing the
configuration. Do not point the blob adapter at an old cache directory.

## Data model and identity

| Relation | Responsibility |
| --- | --- |
| `cache_query` | Logical identity, canonical identity bytes and structured query, race-free next sequence, publication mutation revision |
| `blob_object` | Exact source SHA-256, byte size, immutable physical storage key/generation and publication state |
| `cache_snapshot` | Immutable captured occurrence, query sequence, processed JSONB and digest, source reference and original technical provenance |
| `cache_head` | Current compatible snapshot and mutable successful validation state, separated by query/processor/schema |
| `cache_storage` | Bounded global snapshot/query/reference-byte accounting |
| `blob_deletion` | Durable queue of retired physical blob generations awaiting idempotent deletion |
| `upstream_daily_budget` | Provider/origin daily reservations, optional finite caps and retained charged counts |

Relational entity primary keys default to PostgreSQL 18's native `uuidv7()`.
Snapshot UUIDs identify occurrences; source SHA-256 identifies content. The public
MCP snapshot ID remains a separate unique opaque 64-character lowercase hexadecimal
value. Existing clients and widgets need no snapshot-ID representation change.
Security-sensitive comparison handles retain cryptographically random 256-bit values.

UUID timestamps never supply capture, retrieval, validation, promulgation, effective,
or legal revision dates. Explicit application-supplied Unix seconds are stored
losslessly in a constrained `numeric(20,0)` domain covering Rust `u64`. Capture,
retrieval, original validation, and current successful validation remain distinct.
No `CURRENT_TIMESTAMP` replaces the controllable application clock. Clock anomalies
remain explicit; historical sequence order is not a legal timeline.

Trusted application code encodes a versioned, length-delimited identity and hashes
it with SHA-256. Identity includes namespace, provider, dataset, source, operation,
exact selectors/query text, and pagination. No trimming, case folding or JSON text
formatting changes equivalence. The demo namespace identifies its configured source
origin. PostgreSQL retains canonical bytes and structured fields and verifies both
on hash matches. A digest collision fails integrity validation instead of merging
semantically different identities. Processor/schema versions belong to compatible
head identity, while history retains observations from different processors.

An unchanged compatible refresh reuses its occurrence and advances head validation
state only. Original retrieval/capture provenance remains immutable. A → B → A
creates three occurrences, even when occurrences one and three share a raw blob.
Identical bytes across distinct queries can share a blob but never a query or
occurrence identity. Expired captures cannot live indefinitely through validation;
a later successful capture creates a new occurrence.

## Publication, concurrency and failure

The application resolves L1, then persistent storage, then upstream. It owns
freshness, bounded stale fallback, equivalent-request coalescing, cancellation,
provider budgets and generation authorization. Persistent hits consume no upstream
permit and do not advance validation time. Storage failures never become cache
misses, upstream fallback or eligible stale-response errors.

Upstream HTTP, validation/processing, blob I/O and text comparison occur outside
PostgreSQL publication transactions and outside broad application locks. Publication
uses a durable pending blob identity/generation, puts and synchronizes the exact
source bytes, then enters a short metadata transaction. The transaction verifies
query identity and the expected pre-fetch mutation revision, checks the live
application generation/cancellation authorization, and locks only relevant metadata
plus bounded global accounting. It either updates unchanged validation state or
atomically allocates `next_sequence`, inserts an immutable occurrence, and installs
the compatible head. No `SELECT max(sequence) + 1` allocation is used.

A concurrent publication or retention mutation makes an obsolete expected revision
conflict rather than overwrite accepted state. PostgreSQL constraints and mutation
checks establish publication correctness across independent compute processes;
process-local coalescing alone is insufficient. Concurrent upstream work can still
occur across processes because distributed refresh leasing is deliberately deferred.

Persistent success and L1 promotion occur only after acknowledged durable commit.
Failed or uncertain commit never produces an L1 success. Cancellation before the
final commit authorization prevents publication. A cancellation after authorization
can leave a valid committed occurrence, but the cancelled caller receives no success.
An acknowledged blob write followed by cancellation or failed SQL publication can
leave safe garbage. The system prefers such garbage over a committed snapshot
referencing bytes that were never durably published.

Runtime availability failure makes persistence unavailable, invalidates L1 and
fences delayed work. `/ready` returns 503; persistent current/history tools reject
without upstream traffic. Bounded probes can restore readiness after both dependencies
and schema compatibility recover. `/live`, metrics, supplied-text comparison and
existing transient comparison handles remain available while the process is healthy.
An actual failed endpoint or crashed owned worker still triggers shared shutdown.
Known corruption and missing referenced bytes remain integrity faults; connectivity
health alone does not clear them. Repair the affected infrastructure/evidence and
restart with startup validation. Errors and metrics expose bounded categories,
never credential-bearing URLs, SQL values, raw queries or source payloads.

## Blob publication and garbage collection

A blob is identified by SHA-256 of its exact stored bytes. PostgreSQL supplies the
expected digest, size and storage key for every read. The initial filesystem layout
is `<digest[0:2]>/<full-digest>-<physical-generation-uuid>`; the generation distinguishes
physical deletion lifecycles without changing content identity. Normal current and
history reads never traverse directories to discover metadata.

Durable puts are immutable and idempotent. Existing objects are verified before a
put is accepted as deduplicated. Every evidence read verifies expected size and
SHA-256. PostgreSQL also verifies query association, processed-data digest and
bounds, processor/schema compatibility, envelope integrity and provenance. Missing
referenced blobs, mismatched bytes, invalid metadata or broken relational references
produce explicit integrity/storage errors. They are never isolated away as permission
to silently refetch. Digests detect accidental corruption; they do not authenticate a
legal source or protect against an attacker with the same service/storage privileges.

The filesystem adapter validates operator paths, directory ownership and private
permissions, uses descriptor-relative nofollow operations, rejects unsafe object
shapes and hard links, and durably publishes regular immutable files. Blocking jobs
have bounded admission and retain their slots until actual filesystem completion,
even after caller cancellation or deadline. A blocked mount cannot create an
unbounded detached-work backlog. Sixteen ordinary filesystem jobs and one reserved
health probe may run concurrently. Up to sixteen concurrent health callers share
the in-progress probe and its configured watchdog (five seconds by default);
completed results are not
cached for later calls. Cancelling or dropping one caller does not cancel the
probe for others. Only a still-waiting, uncancelled caller may reopen health after
checking shutdown, the failure generation and recovery drainage; an abandoned or
late probe cannot restore readiness. Healthy probes can overlap ordinary work;
recovery requires older failed or blocked jobs to drain. The adapter is not an
index, history manifest, transaction journal, or exclusive cache-root database
owner.

Keep filesystem blob roots owned by the serving UID with mode `0700`, and blob
files private (`0600`). When Kubernetes mounts a volume, use a separately prepared
parent volume root and configure the private `data` child as the blob path.
`fsGroup` must not recursively relax retained blob permissions. The
[Kubernetes preparation contract](deployment-kubernetes.md#storage-and-permissions)
uses a matching `root:10004` parent, mode `2770`, and `OnRootMismatch`; verify the
actual storage driver/kubelet behavior before admitting retained evidence. Never
weaken blob validation or recursively repair retained data at serving startup.

Retention first commits reference removal and enqueues retired physical generations
in PostgreSQL, then deletes their blob objects. A failed deletion leaves reclaimable
garbage. Generation-specific keys keep a delayed deletion from deleting a later
publication of the same digest. Maintenance uses bounded batches; enumeration is
only for orphan/staging discovery and never answers current/history requests.
Referenced generations are excluded from reclamation. Interrupted publication and
maintenance resume from database state; there is no distributed transaction with
the blob store.

`max_blob_bytes` counts unique **referenced raw payload bytes**. It does not guess
PostgreSQL page, index, WAL or TOAST size, and transient orphans/deletion backlog can
consume additional filesystem space. Snapshot/query count and age limits bound
metadata independently. Monitor database physical growth and actual blob-volume
free space operationally. Global accounting requires a short shared metadata lock;
this is an intentional single-node tradeoff, never a lock across upstream or blob I/O.

Read paths enforce the logical retention deadline even between maintenance sweeps.
Retention removes expired observations, then older history and cold heads when
required by configured count/byte limits. Lowering limits below existing retained
totals causes ordinary startup to fail without silent pruning. Explicitly run:

```sh
openlegal-server --maintain CONFIG.toml
```

This command uses the runtime credential, allows opening over-budget storage, runs
bounded pruning/cleanup, and exits with failure if it cannot complete its contract.
It does not start serving or fetch upstream data. PostgreSQL capacity and storage
integrity errors remain distinct from missing retained snapshots.
Stop serving processes before running this offline command with changed limits.
Each maintenance pass considers at most 128 candidates per phase. Pending puts are
bounded to 128 objects/128 MiB and become reclaimable after sixty application-clock
seconds; retired deletion metadata is bounded to 4096 objects/4 GiB. Hitting these
bounds rejects admission until cleanup makes room. Online maintenance has a
five-second deadline; offline pruning has a sixty-second deadline and can be rerun.
Health probes run every five seconds and maintenance every sixty seconds. Metrics
include aggregate hit/miss/publication/validation counts, blob operations, integrity
and availability failures, retention/orphan cleanup, retained and pending budgets,
and pool size/idle counts; health probes refresh database accounting gauges.

## History and comparisons

`demo_list_snapshots` takes the exact query, optional opaque cursor and page limit
(default 10, maximum 20). Results descend by per-query occurrence sequence. Cursors
bind the query UUID and a fixed high-water sequence; concurrent appends do not shift
later pages, while retention may leave gaps. Query removal/recreation invalidates
old cursors. No per-client server state or upstream request is created.

`demo_get_snapshot` takes that exact query and opaque public snapshot ID. Historical
responses preserve original provenance and the historical marker, without a current
freshness claim. Unavailable snapshots never substitute current data. Raw evidence
is internal. Older processors are labeled; every search page is an independently
captured membership result. Opening a row shows its embedded historical record;
an explicit current-record action makes a normal retrieval request.

With text comparison enabled, `demo_compare_record_snapshots` resolves two exact
retained Get snapshots for the same source/record. The projection remains
`title + "\n\n" + body` (`title_lf_lf_body_v1`) without normalization. Search-page
snapshots cannot be used as record snapshots. Server-derived origins remain attached
to the transient comparison; supplied-text recomparison cannot assert them. Comparison
size/line/NUL limits, paging, cancellation, deletion and ten-minute random-handle
lifetime are unchanged. The transient comparison can outlive source retention;
that does not claim continued availability of the original source evidence.

## Request-policy schema upgrade

Migration 0013 adds nullable unlimited LAW caps, bigint charged usage, millisecond
pacing, operation snapshots and configurable finite Job attempts, plus the
synthetic provider/origin daily ledger. Migrations 0001..0012 remain immutable.
Apply the matching binary's migration with ingestion stopped, then upgrade serving,
scheduler and request Jobs together. Mixed old/new binaries are unsupported.
Preserve usage, pilot start, pauses, unresolved responses and active claim evidence;
changing policy never resets them. Rollback requires the coordinated pre-migration
snapshot and its matching binaries, not running an older executable against the
new schema. This migration does not alter retained legal identity or rebuild the
search index.

## Offline provider recovery

The administrator CLI opens PostgreSQL only. It does not initialize blob stores,
provider clients, dictionaries, widgets, serving endpoints or collection Jobs.
`[cache.postgres].provider_admin_url_env` defaults to
`OPENLEGAL_PROVIDER_ADMIN_DATABASE_URL`, distinct from runtime and migration
variables. Only administrator commands resolve this secret. TLS and URL validation
use the existing PostgreSQL policy. No ordinary Pod should receive this secret.

```sh
openlegal-server --provider-admin inspect CONFIG.toml > inspection.json
openlegal-server --provider-admin plan reviewed-spec.json CONFIG.toml > plan.json
openlegal-server --provider-admin apply plan.json CONFIG.toml
openlegal-server --provider-admin readback OPERATION_ID CONFIG.toml
```

`inspect` and `plan` are read-only. Supply an actor, review reason, exact legacy
and parallel owner targets, stopped-writer identities, stop time, deployed
revision and evidence reference in the specification. All request initiators and
late response callbacks must be stopped and drained; the evidence is the
operator's attestation, not proof furnished by the database or CLI.

A synthetic specification (replace it with reviewed actual observations):

```json
{
  "actor": "operator-label",
  "reason": "reviewed uncertain response after all writers drained",
  "quiescence": {
    "writers_stopped_at": 1791200000,
    "deployment_revision": "matching-deployed-revision",
    "stopped_writer_ids": ["scheduler", "request-and-detail-collectors"],
    "evidence_reference": "operator-maintenance-record"
  },
  "resolve_legacy": true,
  "owners": [],
  "resume": false,
  "waits": []
}
```

Version 1 plans bind storage identity, schema and the complete provider state to a
unique operation ID and expire after 15 minutes. JSON input is limited to 256 KiB;
plans select at most 16 parallel owners and 256 explicitly reviewed waits. Inspect
reports whether its bounded waiting inventory was truncated. Plan reads only the
explicitly selected IDs, including IDs outside that first inventory window; a
cleanup-only plan does not retain unrelated waiting entries. Offline cleanup
requires the exact full owner ledger; it cannot force-clean a live owner or
silently ignore unreviewed uncertain owners. Plan files contain private operational
identities and review evidence; keep them in operator-controlled storage.

Default apply resolves reviewed uncertainty and atomically creates a durable
`provider_recovery_hold`. Restarting a current collector does not release it.
To resume globally, make a new plan with the explicit `--resume` option. This also
supports a hold-only plan after cleanup (`resolve_legacy=false`, `owners=[]`).
Selected waits require exact `kind`, `id` and nonempty `review_reason`; legacy NULL
reasons are not guessed or backfilled. Only their eligible leases are advanced.
Unselected due work can run on its original schedule after global hold release.
Quotas, authentication suspension, Retry-After, epochs, attempts and permanent
captures remain authoritative. A reviewed selection blocked by another policy
may remain unadvanced; inspect the returned `advanced_waits` rather than assuming
that resumption launched every selected item.

Apply rechecks state and leases under corpus/provider row locks and the legacy
plus all 16 slot advisory locks. Changed state or active ownership rejects the
whole transaction. Audit insertion, fence cleanup, hold and lease updates commit
atomically. Identical reapplication returns the recorded result; reuse of an
operation ID with a different plan is rejected. On a lost COMMIT acknowledgement,
do not automatically apply again: use `readback` and reconcile that operation.
Charged usage is never refunded. Audit and observation history are retained.

For collection contention incidents, deploy the matching storage/runtime fix
before resuming admission. Do not clear an abandoned slot merely because the
serving endpoint is healthy: correlate runtime failure, provider diagnostics and
the reviewed owner ledger. Stop and drain scheduler, request/detail collectors and
late callbacks, preserve a coordinated database/archive snapshot, then use the
inspection, plan, apply and readback sequence above. Cleanup retains the recovery
hold; global resume is a separate reviewed operation. Never replay an ambiguous
apply in place of reading its operation result.

After resumption, observe serving and scheduler restart counts, abandoned slots,
publication progress and index acknowledgement continuously for at least two
hours. Record the actual elapsed interval and any failures; a short passing window
does not establish longer stability. Verify the requested statute by exact object
identity, published capture and capture-fixed body retrieval, rather than a related
title hit or a completed job alone. Full corpus completion and native ChatGPT UI
acceptance remain separate checks.

### Administrator role provisioning

DBAs provision roles and credentials outside migrations. The migration creates
`openlegal_admin`, revokes PUBLIC table/function access and exposes named checked
functions. Ordinary broad grants on `openlegal` do not grant access to this schema.
Use a separate NOLOGIN function-owner role and a LOGIN operator role; do not use
the migration credential for routine recovery. The operator has no direct admin
DML or audit insertion rights. These controls protect ordinary roles, not a DBA,
migration owner or superuser capable of changing objects and grants.

After migration, a DBA can provision the following role structure, with LOGIN
credentials supplied by the site's secret-management process:

```sql
CREATE ROLE openlegal_provider_function_owner NOLOGIN;
CREATE ROLE openlegal_provider_operator LOGIN;
-- Required even when a hardened deployment revokes PUBLIC schema usage.
-- This permits schema lookup, not CREATE or access to unrelated tables.
GRANT USAGE ON SCHEMA public
  TO openlegal_provider_function_owner, openlegal_provider_operator;
GRANT USAGE ON SCHEMA openlegal, openlegal_admin TO openlegal_provider_function_owner;
GRANT SELECT ON public._sqlx_migrations TO openlegal_provider_function_owner;
GRANT SELECT ON openlegal.cache_storage, openlegal.corpus_control,
  openlegal.provider_request_budget, openlegal.provider_request_admission,
  openlegal.collection_request, openlegal.corpus_job, openlegal.corpus_object
  TO openlegal_provider_function_owner;
GRANT UPDATE (singleton) ON openlegal.corpus_control TO openlegal_provider_function_owner;
GRANT UPDATE (unresolved_response, admission_owner) ON openlegal.provider_request_budget
  TO openlegal_provider_function_owner;
GRANT DELETE ON openlegal.provider_request_admission TO openlegal_provider_function_owner;
GRANT UPDATE (lease_until) ON openlegal.collection_request, openlegal.corpus_job
  TO openlegal_provider_function_owner;
GRANT UPDATE (version) ON openlegal.corpus_object TO openlegal_provider_function_owner;
GRANT SELECT ON ALL TABLES IN SCHEMA openlegal_admin TO openlegal_provider_function_owner;
GRANT UPDATE ON openlegal_admin.provider_control TO openlegal_provider_function_owner;
GRANT INSERT, UPDATE ON openlegal_admin.uncertainty_observation TO openlegal_provider_function_owner;
GRANT INSERT ON openlegal_admin.provider_recovery_audit TO openlegal_provider_function_owner;
-- Schema 28: these SECURITY DEFINER functions share this existing owner.
-- FOR UPDATE of the singleton requires UPDATE privilege even without changing it.
GRANT INSERT, UPDATE ON openlegal_admin.provider_canary,
  openlegal_admin.provider_canary_control TO openlegal_provider_function_owner;
GRANT INSERT ON openlegal_admin.provider_canary_audit TO openlegal_provider_function_owner;
-- Temporarily permit function ownership transfer, then remove CREATE.
GRANT CREATE ON SCHEMA openlegal_admin TO openlegal_provider_function_owner;
DO $$ DECLARE f record; BEGIN
  FOR f IN SELECT p.oid::regprocedure AS signature FROM pg_proc p
    JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='openlegal_admin'
  LOOP EXECUTE format('ALTER FUNCTION %s OWNER TO openlegal_provider_function_owner',f.signature); END LOOP;
END $$;
REVOKE CREATE ON SCHEMA openlegal_admin FROM openlegal_provider_function_owner;
GRANT USAGE ON SCHEMA openlegal_admin TO runtime, openlegal_provider_operator;
GRANT SELECT ON public._sqlx_migrations TO openlegal_provider_operator;
GRANT EXECUTE ON FUNCTION openlegal_admin.provider_inspect(),
  openlegal_admin.provider_inspect_selected(jsonb),
  openlegal_admin.provider_apply(jsonb), openlegal_admin.provider_readback(uuid)
  TO openlegal_provider_operator;
GRANT EXECUTE ON FUNCTION openlegal_admin.canary_init(uuid,uuid),
  openlegal_admin.canary_close(uuid,uuid) TO openlegal_provider_operator;
GRANT EXECUTE ON FUNCTION openlegal_admin.provider_diagnostic(),
  openlegal_admin.provider_held(), openlegal_admin.observe_uncertainty(),
  openlegal_admin.provider_blocker_fingerprint(),
  openlegal_admin.canary_consume(uuid) TO runtime;
```

Replace `runtime` with the deployment's actual runtime role. Verify that PUBLIC
and runtime cannot invoke apply/readback/inspect, operator cannot invoke collector
observation functions, and neither can update/delete/truncate audit or release
hold via direct DML. Do not grant operator membership in the function-owner or
migration role. Missing hold-read grants fail closed at admission; diagnostic
reads report unknown. Upgrade all restartable collectors before applying recovery;
old binaries do not recognize the hold. Follow the coordinated restore procedure
for rollback rather than removing new schema or retained audit records.

## Offline current inventory administration

Migration 27 adds an explicit `corpus_object.current_inventory_conflict` hold,
bounded active-cycle candidate checkpoints and immutable
`current_inventory_audit` decisions. Existing captures and blob identities are
unchanged. Held checkpoint proof survives audited release and pruning of obsolete
unconflicted cycles. Apply the migration and ordinary runtime table grants before
using the matching binary; a schema-26 executable cannot open schema 27. Rollback
requires a compatible executable or coordinated recovery, not deletion of the
migration/audit rows.

The native offline command uses only the configured runtime PostgreSQL credential
and verified connection policy. It rejects database/application-object ownership,
administrative role attributes and reachable server-file/program privileges.
It opens no blob store, provider, dictionary,
widget, worker or listener:

```sh
openlegal-server --current-inventory-admin inspect OBJECT.json CONFIG.toml
openlegal-server --current-inventory-admin apply MANIFEST.json CONFIG.toml
openlegal-server --current-inventory-admin readback OPERATION_ID CONFIG.toml
```

`OBJECT.json` is a typed object identifier. An apply manifest has
`schema_version: 1`, a canonical UUID `operation_id`, `object`, exact
`expected_version`, required nullable `expected_head_capture`, `action`
(`quarantine` or `release_hold`), bounded `actor`, `reason`, `evidence_ref` and
the evidence's hexadecimal SHA-256 `evidence_sha256`. These operator assertions
must refer to reviewed evidence; the CLI does not prove legal applicability from
the reference or digest. Keep operator inputs private and exclude credentials
and raw legal text.

Apply requires the explicit global provider recovery hold, drained writers and
zero other database sessions or provider owners. Merely reaching a request quota
is insufficient. Quarantine fences HEAD jobs and preserves attempts, charged
usage, captures, historical jobs and prior validation time. Release refuses a
still-ambiguous active-cycle candidate set. It clears only the conflict flag,
increments the object version and leaves HEAD pending with no desired revision.
It neither chooses a revision nor makes the previous HEAD fresh or searchable as
current. A subsequent unambiguous current observation must schedule normal work.
Historical and exact-capture access remains subject to the existing rights and
withdrawal rules.

The immutable audit binds the full canonical manifest, before/after state and
operation UUID. Reusing a UUID with another manifest is rejected. Preserve the
manifest and transmission marker before apply; an uncertain acknowledgment
requires `readback` of that UUID rather than another apply attempt. Provider owner
cleanup and global resumption remain separate audited operations described above.
Inspect output is bounded and reports truncation; a display limit is not evidence
that an undisplayed candidate does not exist.

## Strict two-copy durability

Replication is optional. Existing configurations keep local durability and the
existing cache recovery lifecycle. Schema 28 introduces the strict policy;
install the complete migration set matching the executable (including schema
29 for checked daily holds). A complete configuration includes these additions:

```toml
[cache.postgres]
replication_mode = "required"
replication_epoch = "a1f71db8-bd59-4d33-a3bb-fb77154b571e" # operator-issued example
tls_mode = "verify-full"

[cache.blob]
kind = "filesystem_mirror"
path = "/var/lib/openlegal/cache-blobs"
replica_path = "/var/lib/openlegal/cache-blobs-replica"

[database]
blob_path = "/var/lib/openlegal/corpus-blobs"
blob_replica_path = "/var/lib/openlegal/corpus-blobs-replica"
storage_failure_policy = "recover"
```

These are additions to a complete configuration, not a standalone configuration.
Required mode rejects plaintext PostgreSQL, a missing or nil epoch, single-copy
cache/corpus storage and overlapping roots. A DBA seeds all retained references
and physical generations before binding the protected database control to that
epoch and the exact authenticated Blob seed/root proof. Startup refuses a
missing, replaced or mismatched seed; merely creating an empty directory is not
seeding. Operator qualification separately proves independent HDD/SSD backing
devices, PV/PVC identities and matching configuration in serving, scheduler and
every collection template. Distinct paths alone prove no hardware independence.

Both Blob writes use the same immutable physical key and generation. Success
requires file and parent-directory durability on both media. Ready-object
deduplication verifies and synchronizes the existing object on both copies; it
does not repair a missing copy. Reads and enumeration use the selected primary
only. There is no automatic fallback, asynchronous mirror or single-copy ACK.
One logical timeout covers both legs; timeout/cancellation does not interrupt
kernel I/O. Aggregate admission remains held until both physical jobs drain.

PostgreSQL 18 requires `fsync=on`, `full_page_writes=on`,
`synchronous_commit=on` for every authoritative writer, and the exact named
synchronous peer. After the operation, the observer samples the primary's local
durable WAL flush position; the authenticated peer must durably flush through
that fixed position. Completed synchronous commits are covered by this cut.
Unflushed background WAL in a partial page does not advance it. The private
`prefix_lsn` and checkpoint `wal_lsn` fields both identify this primary durable
position; physical restore diagnostics retain their separate raw insertion
positions. The fresh proof binds the managed epoch, primary system identifier/current
timeline, postmaster start, sender session, dedicated replication role/address
and TLS client certificate DN, issuer and serial. A COMMIT command tag alone is
insufficient: cancelling synchronous wait may leave a transaction committed
locally before standby confirmation. The observer checks its binding again after
the flush observation and does not reuse a readiness bit or cached receipt.
Each proof uses at most two seconds or the caller's remaining deadline.

The fence covers cache/corpus publication, ready-object deduplication, provider
admission before DNS/HTTP, persistent public results and retained-source bytes,
and reference absence/generation authorization before physical garbage collection.
Uncertain COMMIT or post-COMMIT proof failure is sticky in that process and stops
supervised work. A later health success cannot authorize replay. Confirmed
health-probe availability recovery remains a separate, narrower lifecycle.

The guarantee is RPO 0 for acknowledged database state and its referenced Blob
bytes on the two configured media, including resumed collection. It does not
cover an external response not yet durably retained, host loss, loss of both
media, operator deletion, logical corruption or unresolved reuse rights. Peer
loss blocks new write/provider admission; already admitted work receives bounded
best-effort cancellation and keeps charged usage and uncertain ownership. Manual
recovery physically fences all old writers, verifies the latest standby WAL and
index, and durably seeds a second copy before strict service resumes. An advisory
lock on the former primary cannot fence a promoted database.

The observer schema and control are private. Runtime roles receive only the
named fixed observer functions; a separate NOLOGIN function owner holds catalog
privileges. The sole PUBLIC exception is the inputless nonsecret boolean
`openlegal.replication_required()`: a legacy local client uses it to refuse
opening a database already configured as strict, before accounting or GC.
PUBLIC receives no observer, control-table or catalog access through it.

Schemas 28 through 31 are additive; previous migration bytes remain unchanged. After a strict
ACK, rollback must retain the latest database and both Blob generations with an
executable compatible with the complete installed migration set, or fix forward. Restoring an older checkpoint
over acknowledged writes is not a rollback method for this policy.

### Checkpoint operator privileges

Schema 29 provides fixed, private operator functions. Provision their separate
NOLOGIN owner and the named LOGIN operator before qualifying the workflow.
The migration creates neither role nor a password. The LOGIN operator must not
inherit the function owner, receive direct table privileges, or use a superuser
fallback. Its authentication and certificate/peer identity are a separate,
reviewed deployment input.

The following grant block is a template for a DBA, not an executed provisioning
step. Create the two roles with `NOSUPERUSER NOCREATEDB NOCREATEROLE
NOREPLICATION NOBYPASSRLS NOINHERIT`; the owner also has `NOLOGIN`. Do not grant
either role to the application runtime. Replace the names consistently after
reviewing the exact deployment authority.

```sql
GRANT USAGE ON SCHEMA public,openlegal,openlegal_admin,
  openlegal_replication,openlegal_checkpoint TO openlegal_checkpoint_function_owner;
GRANT SELECT ON ALL TABLES IN SCHEMA openlegal,openlegal_admin,
  openlegal_replication,openlegal_checkpoint TO openlegal_checkpoint_function_owner;
GRANT SELECT ON public._sqlx_migrations TO openlegal_checkpoint_function_owner;
GRANT UPDATE(singleton) ON openlegal.corpus_control,
  openlegal.provider_request_budget TO openlegal_checkpoint_function_owner;
GRANT UPDATE(recovery_hold,generation,hold_started_at)
  ON openlegal_admin.provider_control TO openlegal_checkpoint_function_owner;
GRANT SELECT,INSERT,UPDATE ON openlegal_checkpoint.operation
  TO openlegal_checkpoint_function_owner;
GRANT EXECUTE ON FUNCTION pg_catalog.pg_control_system(),
  pg_catalog.pg_control_checkpoint(),openlegal_admin.slot_locked(integer)
  TO openlegal_checkpoint_function_owner;
GRANT pg_read_all_settings TO openlegal_checkpoint_function_owner
  WITH INHERIT TRUE, SET FALSE;
GRANT CREATE ON SCHEMA openlegal_checkpoint TO openlegal_checkpoint_function_owner;
ALTER FUNCTION openlegal_checkpoint.state() OWNER TO openlegal_checkpoint_function_owner;
ALTER FUNCTION openlegal_checkpoint.hold(uuid,text) OWNER TO openlegal_checkpoint_function_owner;
ALTER FUNCTION openlegal_checkpoint.release(uuid,text) OWNER TO openlegal_checkpoint_function_owner;
ALTER FUNCTION openlegal_checkpoint.readback(uuid) OWNER TO openlegal_checkpoint_function_owner;
ALTER FUNCTION openlegal_checkpoint.observe(uuid) OWNER TO openlegal_checkpoint_function_owner;
ALTER FUNCTION openlegal_checkpoint.retained_tables() OWNER TO openlegal_checkpoint_function_owner;
ALTER FUNCTION openlegal_checkpoint.retained_references() OWNER TO openlegal_checkpoint_function_owner;
ALTER FUNCTION openlegal_checkpoint.database_position() OWNER TO openlegal_checkpoint_function_owner;
ALTER FUNCTION openlegal_checkpoint.database_layout() OWNER TO openlegal_checkpoint_function_owner;
ALTER FUNCTION openlegal_checkpoint.client_sessions() OWNER TO openlegal_checkpoint_function_owner;
ALTER FUNCTION openlegal_checkpoint.storage_metadata() OWNER TO openlegal_checkpoint_function_owner;
ALTER FUNCTION openlegal_checkpoint.canary_observation(uuid) OWNER TO openlegal_checkpoint_function_owner;
REVOKE CREATE ON SCHEMA openlegal_checkpoint FROM openlegal_checkpoint_function_owner;
GRANT USAGE ON SCHEMA openlegal_checkpoint TO openlegal_storage_operator;
GRANT EXECUTE ON FUNCTION openlegal_checkpoint.state(),
  openlegal_checkpoint.hold(uuid,text),openlegal_checkpoint.release(uuid,text),
  openlegal_checkpoint.readback(uuid),openlegal_checkpoint.observe(uuid),
  openlegal_checkpoint.retained_tables(),openlegal_checkpoint.retained_references(),
  openlegal_checkpoint.database_position(),openlegal_checkpoint.database_layout(),
  openlegal_checkpoint.client_sessions(),
  openlegal_checkpoint.storage_metadata(),openlegal_checkpoint.canary_observation(uuid)
  TO openlegal_storage_operator;
```

The LOGIN also needs the fixed replication observers under their separate
NOLOGIN owner. Those return only authenticated peer/epoch/WAL metadata.
`observe(uuid)` takes budget then provider-control SHARE locks and returns one
fresh state, slot-lock provenance and protected checkpoint-owner/readback
observation. `state()` omits the three mutable hold bookkeeping fields so a
checked hold change cannot mask a usage or policy change. The other functions
return row counts/hashes, exact Blob references, database/index positions and
bounded storage/session metadata; they return no legal bodies, query text,
credentials or connection strings. Hash/reference limits and timeout/physical
closure checks remain the caller's responsibility.
`database_layout()` exposes only the effective server data-directory path. The
operator maps it through the exact inspected container's bind mounts, checks the
actual `pg_wal` directory or reviewed symlink, and requires correspondence with
each side's selected backup source. Environment variables are not evidence of
the effective directory. The LOGIN does not inherit `pg_read_all_settings`.
The session observer counts every PID in the database, including background
sessions, and excludes only its own PID. It does not filter on PostgreSQL's
privilege-masked `backend_type`, which could hide another role's client. An
unrelated database background session can therefore conservatively block drain;
the function owner does not receive `pg_read_all_stats`.

`hold` and `release` expose a generic named operator surface; SQL grants do not
restrict them to one approved operation UUID. Source/inventory/intent pins,
immutable authority, a shared operation lock and dual-media journals enforce the
workflow's narrower scope. A foreign or uncertain hold is preserved; only a clean
hold created by the current verified operation may be released. There is no
owner cleanup, usage refund, credential creation or automatic resume on a failed
or unknown result. The real PostgreSQL checkpoint fixture tests this function-only
effective role privileges and actual advisory-lock provenance using `SET ROLE`;
it does not establish end-to-end production LOGIN/TLS authentication. Offline JSON tests do not
establish that production roles have these privileges.

### Explicit replica binding

Schema 30 adds a separate recovery-only `bind_replica(uuid,text,jsonb,jsonb)`
function. Ordinary checkpoint operators do not receive it. Provision its
NOLOGIN owner with the provider-control locks used above, SELECT/INSERT/UPDATE
on `openlegal_replication.control`, and named EXECUTE on `capture_prefix()`;
grant the separately approved recovery LOGIN only schema USAGE and named
EXECUTE. Never grant the owner role to that LOGIN. The migration creates no
credentials or grants to an existing runtime account.

The caller records an exact two-media seed intent before invoking it once. An
initial binding requires an empty control row. A manual replacement compares
all previous typed fields and requires a new epoch, the same system identifier
and a strictly newer promoted timeline. A held, drained provider, all admission
locks and the actual authenticated synchronous peer are required. A failed peer
check rolls back the proposed binding. The caller must separately fence old
physical writers, prove both Blob copies, and verify a fresh post-COMMIT WAL
prefix and peer flush within the original acknowledgement deadline. An unknown
result is preserved for investigation; it does not authorize replay.

`observe_candidate(jsonb)` is a read-only startup observer under the existing
NOLOGIN replication owner. Give the recovery LOGIN only named EXECUTE. Its
response separates the proposed epoch/Blob intent from actual primary/peer/WAL
facts; it does not claim the proposed control is already installed. Proposed
binding inputs and fixed peer observers encode certificate serials as decimal
strings. `bind_replica` returns typed control-row JSON; the operator parses its
numeric serial without floating-point conversion and normalizes it to decimal
text before comparison. The isolated TLS pair gate uses
an actual non-superuser LOGIN to test owner-membership denial and initial bind
rollback/success. It is not a production seed or promoted-pair acceptance test.

### Bounded collection canary

An operator can bind `[database.ingestion].canary_operation` to a protected
six-hour operation. Schema 28 consumes at most **100 additional provider HTTP
attempts total**, in the same locked reservation transaction as ordinary budgets,
across modes, origins, restarts and UTC days. Attempts include attachments and
failed DNS/TLS; usage is never refunded or reset by a retry or phase change.
Existing stricter quotas, rights and uncertainty fences still apply. The original
deadline bounds request connection, send and body work; it is not renewed per
request. Automatic redirects and transport retries remain disabled.

Initialization/closure require the separately audited held and drained provider
operation. A closed canary remains required and cannot enable an unlimited
fallback. This mechanism bounds traffic; it does not establish successful
publication. Acceptance requires actual 21,600-second observation and at least
one eligible acknowledged publication, with fresh transport and preservation
checks. Exhausting the cap without sufficient evidence is incomplete acceptance.

Schema 31 adds recovery-only `activate_canary(uuid,uuid,text,uuid)`,
`finish_canary(uuid,text,uuid)` and `canary_readback(uuid)`. They are separate
from daily checkpoint privileges. Before serving starts, the operator must
already have a legitimate audited provider hold from the existing reviewed
provider-recovery procedure. Activation compares that exact held generation,
requires a clean ledger and actual strict replica, and atomically initializes
the original six-hour canary and releases that hold. It changes no request
charges, suspension, retry state, waits or policy. The serving process need not
restart between the watch's before/after observations.

After the full observation, the operator physically stops and drains every
known writer and creates its own clean schema-29 checkpoint hold. Finish
requires that exact current hold/intent, unchanged epoch/Blob binding and
original elapsed database deadline. It closes the canary while preserving both
the final hold and required canary ID. It records the original recovery audit
and final checkpoint operation together. Replays are refused; use the readback
function to investigate a lost acknowledgement. These database functions do not
prove physical closure, preservation, publication or public transport success.

Provision a separate NOLOGIN canary function owner and an explicitly approved
operator LOGIN with no owner membership. Neither receives superuser, role
creation, database creation, replication or RLS-bypass privileges. The LOGIN
receives only the needed named functions, including separately reviewed
checkpoint/replica observations. A DBA grant template for the function owner is:

```sql
GRANT USAGE ON SCHEMA openlegal,openlegal_admin,openlegal_replication,
  openlegal_checkpoint TO openlegal_canary_function_owner;
GRANT SELECT ON openlegal.corpus_control,openlegal.provider_request_budget,
  openlegal.provider_request_admission,openlegal_admin.provider_control,
  openlegal_admin.provider_recovery_audit,openlegal_admin.provider_canary,
  openlegal_admin.provider_canary_control,openlegal_replication.control,
  openlegal_checkpoint.operation,openlegal_checkpoint.canary_lifecycle
  TO openlegal_canary_function_owner;
GRANT UPDATE(singleton) ON openlegal.corpus_control,
  openlegal.provider_request_budget,openlegal_replication.control
  TO openlegal_canary_function_owner;
GRANT UPDATE(recovery_hold,generation,hold_started_at)
  ON openlegal_admin.provider_control TO openlegal_canary_function_owner;
GRANT UPDATE(ended_at) ON openlegal_admin.provider_canary
  TO openlegal_canary_function_owner;
GRANT UPDATE(required_id) ON openlegal_admin.provider_canary_control
  TO openlegal_canary_function_owner;
GRANT UPDATE(operation_id) ON openlegal_checkpoint.operation
  TO openlegal_canary_function_owner;
GRANT INSERT ON openlegal_admin.provider_canary_audit
  TO openlegal_canary_function_owner;
GRANT INSERT,UPDATE ON openlegal_checkpoint.canary_lifecycle
  TO openlegal_canary_function_owner;
GRANT EXECUTE ON FUNCTION openlegal_admin.canary_init(uuid,uuid),
  openlegal_replication.capture_prefix(),openlegal_checkpoint.state()
  TO openlegal_canary_function_owner;
GRANT CREATE ON SCHEMA openlegal_checkpoint TO openlegal_canary_function_owner;
ALTER FUNCTION openlegal_checkpoint.activate_canary(uuid,uuid,text,uuid)
  OWNER TO openlegal_canary_function_owner;
ALTER FUNCTION openlegal_checkpoint.finish_canary(uuid,text,uuid)
  OWNER TO openlegal_canary_function_owner;
ALTER FUNCTION openlegal_checkpoint.canary_readback(uuid)
  OWNER TO openlegal_canary_function_owner;
REVOKE CREATE ON SCHEMA openlegal_checkpoint FROM openlegal_canary_function_owner;
GRANT USAGE ON SCHEMA openlegal_checkpoint TO openlegal_canary_operator;
GRANT EXECUTE ON FUNCTION openlegal_checkpoint.activate_canary(uuid,uuid,text,uuid),
  openlegal_checkpoint.finish_canary(uuid,text,uuid),
  openlegal_checkpoint.canary_readback(uuid) TO openlegal_canary_operator;
```

The existing `canary_init` owner retains its separately provisioned table
privileges; the new owner calls that function without inheriting its role.
The isolated TLS/LOGIN lifecycle test shifts timestamps only to exercise SQL
finish conditions. It cannot establish actual six-hour production observation.

### Preservation and standby observations

Schema 32 adds recovery-only `preservation_page(text,jsonb,integer)` and
`publication_page(bigint,text,integer)` functions. Both use fixed projections,
at most 128 rows per page and a 256 KiB response bound. They return metadata and
digests, without legal bodies or arbitrary SQL. PUBLIC receives no execution
privilege. Provision named observation functions separately from lifecycle
mutation privileges.

The operator captures all fifteen preservation relations and every permanent
generation in the four BlobStore roots while writers are physically drained.
Final comparison runs under its owned checkpoint hold before finishing the
canary. Every baseline key and immutable digest must remain; permitted counters
may only advance. This includes ready cache blobs that are currently unreferenced:
ordinary cache GC that removes a baseline generation fails this acceptance gate.
Additions do not excuse missing history. Both journals and the original absolute
deadline are checked through final acknowledgement. These proofs alone do not
establish a publicly acknowledged publication, producer OS exit or restoration.

Schema 33 adds standby-only `survivor_position()`,
`provider_survivor_state()` and `survivor_snapshot()` in the replication and
checkpoint schemas. The snapshot requires a repeatable-read, read-only
transaction. A recovery observer seals one receive position only after the old
WAL receiver has disconnected, then requires replay through that fixed position
and no reconnect. Primary advisory locks are not replicated, so a standby's
lock-free observation cannot justify clearing ownership or uncertainty.
Provision function owners as separate NOLOGIN roles and grant named callers
only the required execution privileges. These observers neither promote a standby nor
authorize serving before a second durable copy is established.

## Deployment boundaries and validation

The supported operating shape is one service process and PostgreSQL 18, with
local storage by default or the opt-in two-copy policy above. Native SQL constraints, per-query sequences, explicit provenance and
stable snapshot UUIDs permit later compute replicas without a history-format
migration. Multi-instance upstream rate coordination, distributed refresh leases,
regional L1 invalidation, automatic failover, NFS deployment
and S3 SDKs are deferred. The separate [legal corpus](database.md) tables and
search index do not make cache observations authoritative legal revisions.

A dedicated NFS mount can later replace the local blob mount only after verifying
its durability, exclusive publication, ownership and failure behavior against the
same adapter contract. No NFS orchestration is included. A future S3-compatible
adapter can implement the same immutable location contract and preserve the existing
storage keys while copying bytes; neither PostgreSQL history nor `RetrievalService`
needs redesign. Backend replacement is an operator data-copy/verification procedure,
not permission to serve missing objects during a move.

The mandatory `scripts/test-postgres.sh` gate provisions real PostgreSQL 18 with
isolated databases and synthetic fixtures. Ordinary workspace tests cover pure
identity and application policy without a database. The OxiBelt gate independently
provisions PostgreSQL and a blob volume and exercises both transports, both MCP
revisions, exact history and comparison origins. Widget unit/browser tests preserve
client contract validation. See [contributor checks](../CONTRIBUTING.md#testing-and-ci)
and [review evidence](persistence-review.md). No legal provider or live ChatGPT
acceptance is implied by these synthetic checks.

`scripts/test-replication.sh` separately provisions a disposable TLS-authenticated
PostgreSQL primary/physical standby pair. It exercises real flush confirmation,
identity/epoch/ACL rejection, cancelled synchronous wait and the physical-delete
barrier. This synthetic pair is not a production seed or failover receipt.
