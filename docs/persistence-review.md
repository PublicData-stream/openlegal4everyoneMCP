# PostgreSQL and BlobStore implementation review

This records the replacement of the disposable filesystem database with PostgreSQL
18 and immutable blobs. The review baseline is
`fd5479f55325f2348d199ebdf70921c721040ec8`. Public snapshot IDs and MCP contracts remain
unchanged; there is intentionally no old-format migration.

## Independent review

The following are separate agent reviews, not human GitHub approvals. The parent
implemented SQL persistence and integrated the patch; each critical implementation
received review by an agent other than its implementer.

| Reviewer | Independent scope |
| --- | --- |
| `storage_design` | PostgreSQL migration, query identity, transactions, sequence allocation, retention/accounting, uncertain commits, SQL/credential boundaries, startup and server wiring |
| `retrieval_semantics` | Filesystem blob publication, integrity, interrupted jobs and GC; final PostgreSQL concurrency/recovery and retention review |
| `history_integration` | Application persistence port, freshness/coalescing/cancellation, L1 admission, history provenance and MCP/widget contract implications |
| Parent | Integration, security test independence, server/transport callers and final validation; does not self-approve its SQL implementation |

Reviewed production paths include `crates/adapters/migrations`,
`crates/adapters/src/postgres.rs`, `crates/adapters/src/postgres`,
`crates/adapters/src/blob.rs`, `crates/adapters/src/blob`,
`crates/application/src/persistence.rs`, `crates/application/src/persistence`,
`crates/application/src/blob.rs`, `crates/application/src/service.rs`,
`crates/application/src/service/persistent.rs`, and server configuration, startup,
readiness, history/tool callers and transport tests. Reviews also inspected the
canonical upstream/legal-data policies and preserved observation/date semantics.

Material findings corrected and subjected to follow-up review:

- GC location metadata now verifies digest, size and exact physical generation
  before deletion. Late deletion cannot remove a republished generation.
- Both existing-directory publication paths synchronize parent directories;
  existing object deduplication verifies complete immutable bytes.
- Recovery uses a failure generation and serialized state transition. A late
  successful probe cannot erase a newer availability/integrity failure or shutdown.
  An uncertain commit closes its connection and is fenced through the quota row
  on a separate connection before readiness returns.
- Foreground PostgreSQL saturation cannot consume the reserved health connection.
  Blob probes also have separate bounded admission; healthy probes overlap normal
  work, while recovery waits for older jobs to drain.
- Retention's bounded deletion pass is followed by explicit per-query/global budget
  enforcement. Read-time expiry is enforced independently of maintenance; a history
  page expiring during its await returns a bounded error instead of an invalid
  empty continuation.
- Publication rereads database generation state before calling a missing reserved
  blob corruption: legitimate concurrent retirement yields contention. A missing
  still-referenced object remains an integrity failure.
- Migration ledger lookup is explicitly qualified. PostgreSQL version detection
  precedes PostgreSQL-18-only session configuration, and startup failures are
  sanitized and distinguish unsupported versions from incompatible schema state.
- Unknown DSN parameters are rejected before driver parsing; SQLx diagnostic targets
  are suppressed even under verbose logging. TLS policy is controlled explicitly.
- Decoded metadata violations are integrity errors, not payload-limit misses;
  history cursors validate query incarnation and high-water bounds.

All three independent reviewers completed renewed review with no unresolved
material findings in their stated scopes. The parent checked their reviewed
production file hashes against the staged files. `storage_design` independently
approved server configuration/composition/readiness; `history_integration`'s tests
of those files are validation evidence, not self-approval.

The final implementation patch, excluding this evidence document, has SHA-256
`4e1777c9f2a7c0b81956e8437ee7004d6b93d1ae507ae9c3b3ac9f4cfd18bf11`.
Reproduce against the implementation commit (substitute its revision for `HEAD`
if inspecting a later checkout):

```sh
git diff --no-ext-diff --no-renames --binary \
  fd5479f55325f2348d199ebdf70921c721040ec8 HEAD \
  -- . ':!docs/persistence-review.md' | sha256sum
```

Selected independently verified SHA-256 anchors:

| Artifact | SHA-256 |
| --- | --- |
| PostgreSQL migration | `a220e9fb09b5252a38bcab9bc1b9ab69c6be01127f7507fee795e23c360b8aad` |
| `crates/adapters/src/postgres.rs` | `fb278540220fc04bd58ac90ff6031167be24b71f03d5d2e5b94608badb9116cb` |
| `crates/adapters/src/blob.rs` | `644f4a4673fd5c9821a9a970eef78fa4644bf6cc387338bb3b28643033c2bc0e` |
| `crates/application/src/service/persistent.rs` | `2c62c6202c4511a11f584e5a1038bf2b5140c6645977c294a57d52d4ff8c1fec` |
| `apps/server/src/main.rs` | `6a3014a2acdf21dc1cfb9abfcdde5e9b38e59dfcf8db43c6dacff925322213e8` |

## Validation evidence

All fixtures are synthetic and all databases are isolated disposable containers.
The mandatory PostgreSQL gate runs real PostgreSQL 18.6, with PostgreSQL 17.11 as a
negative startup fixture. It explicitly runs the database tests marked ignored by
the ordinary workspace suite; missing infrastructure is a failure.

The following checks passed on 2026-09-13:

| Command | Result |
| --- | --- |
| `cargo fmt --all -- --check` | Passed |
| `cargo clippy --workspace --all-targets --locked -- -D warnings` | Passed |
| `cargo test --workspace --locked` | 148 passed; 32 database tests explicitly reserved for the next gate; doctest targets ran |
| `scripts/test-postgres.sh` | All 32 database tests passed, none skipped: 9 deterministic adapter, 13 storage/retrieval, 4 security/version/TLS, 6 server/transport |
| `cargo build --workspace --bins --examples --locked` | Passed after final runtime corrections |
| `cargo audit` | Passed with current RustSec data, 1243 advisories and 369 locked dependencies |
| `cargo deny check` | Passed; informational duplicate-version warnings remain, no new exception |
| `pnpm --dir apps/widget install --frozen-lockfile` | Passed |
| `pnpm --dir apps/widget typecheck` | Passed |
| `pnpm --dir apps/widget test` | 24 passed |
| `pnpm --dir apps/widget build` | Passed; records HTML 898555 bytes, text comparison HTML 902501 bytes |
| `pnpm --dir apps/widget exec playwright install --with-deps chromium` | Passed |
| `pnpm --dir apps/widget test:browser` | 34 passed |
| `pnpm --dir apps/widget check:dependencies` | Passed; no known vulnerabilities, 30 installed licenses admitted |
| `scripts/test-oxibelt.sh` with both widget artifacts | Passed, repeated after final runtime corrections |
| `bash -n scripts/test-postgres.sh scripts/test-oxibelt.sh` | Passed |
| `git diff --check` and staged equivalent | Passed |

The OxiBelt gate uses the clean pinned revision
`72564d165dfd05cb29a64aeebd19fccd7944ea6f`, both MCP revisions over HTTP/WebTransport,
and real PostgreSQL/blob persistence. It includes positive bounded progress,
resources/widgets, retained history, exact comparison origins and rejection cases.
The frontend sources/assets were unchanged by the final runtime race corrections.

The integration coverage includes cold publication and restart without upstream,
L1/persistent hits, unchanged validation, A → B → A occurrences, cross-query blob
deduplication, compatible processor/schema heads, exact lookup, stable pagination,
full unsigned timestamps, read-time/maintenance retention, count/byte caps,
concurrent independent and equivalent queries, race-free sequence allocation,
publication cancellation, lost commit acknowledgement, outage recovery, missing or
corrupt evidence, safe orphan cleanup, late generation deletion, native `uuidv7()`
defaults, relational constraints, restricted roles, TLS peer validation, migration
checksum state, and HTTP/WebTransport history/comparison behavior.

Failure seams pause specific publication, recovery and deletion boundaries; they
are compiled only for tests. Existing application tests separately assert failed
publication never promotes L1 and storage errors never become upstream misses or
stale fallback. Blob tests exercise immutable concurrent puts, unsafe filesystem
objects, partial publication, bounded jobs, cancellation and health fencing.

## Operational limits

This establishes the supported single-process deployment with PostgreSQL 18 and a
dedicated local blob directory. The shared accounting lock deliberately serializes
short metadata mutations. Database physical size and extra orphan/deletion space
remain operational concerns. Kernel-blocked filesystem calls retain bounded job
slots until actual completion; caller deadlines cannot interrupt kernel I/O.

NFS durability, cloud object storage, HA deployment, distributed refresh budgets,
replication, regional invalidation, live legal providers and live ChatGPT acceptance
were not implemented or tested. Cache observations remain distinct from legal
revisions. See [persistence operations](persistence.md) for explicit migrations,
secrets, startup checks and offline pruning.
