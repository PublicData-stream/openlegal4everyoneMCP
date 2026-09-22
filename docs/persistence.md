# PostgreSQL 18 and immutable blob persistence

Persistent mode combines PostgreSQL 18.x with a provider-neutral `BlobStore` beneath
the process-local memory cache. This is the only supported persistent architecture.
The initial blob adapter uses a dedicated filesystem directory; it is an immutable
object store, not a filesystem database. Explicit memory mode is non-persistent.
All current captures are synthetic observations. A cache observation is not an
authoritative legal revision, a legal applicability decision, or a permanent archive.

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
deadline. These deadlines do not claim to interrupt a blocked filesystem syscall.

The initial migration uses the `openlegal` schema and SQLx's migration ledger in
`public`. The runtime role needs `USAGE` on `openlegal`, `SELECT` on
`public._sqlx_migrations`, and `SELECT`, `INSERT`, `UPDATE`, `DELETE` on the storage
tables. It does not require `CREATE` on the database/schema or migration ownership.
Database administrators own role provisioning and grants; migrations do not create
passwords or roles. Keep both schemas free of untrusted object creators. The adapter
uses schema-qualified storage SQL and parameterized values.

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
the in-progress probe and its five-second watchdog; completed results are not
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

## Deployment boundaries and validation

The supported operating shape is one service process, PostgreSQL 18 and one dedicated
blob store. Native SQL constraints, per-query sequences, explicit provenance and
stable snapshot UUIDs permit later compute replicas without a history-format
migration. Multi-instance upstream rate coordination, distributed refresh leases,
regional L1 invalidation, edge routing, replication orchestration, NFS deployment,
S3 SDKs, legal search/PGroonga and authoritative legal-domain tables are deferred.
Future legal/search tables can reference snapshot UUIDs and provenance without
making cache observations authoritative legal revisions.

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
