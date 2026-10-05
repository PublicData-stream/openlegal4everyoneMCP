# Collection deferral and scheduler contention validation

Validation date: 2026-10-05. This records the code and disposable test gates for
the collection recovery changes. Production queue recovery and provider
acceptance are separate operations.

## Behavior and safety boundaries

Collection receipts now distinguish uncertain provider responses, suspension,
daily limits, retry windows, operation limits, admission waits and capacity waits.
Deferred status and coalesced deferred receipts report the remaining stored lease
duration; other active states retain the ten-second hint and terminal states zero.
Migration `0022` preserves legacy NULL and terminal reasons while admitting only
bounded diagnostic codes. Reading these reasons does not spend provider attempts,
clear unresolved-response evidence or change collection ownership.

The scheduler retries selected database operations only after PostgreSQL reports
a known rejection: SQLSTATE `55P03`, `57014`, `40P01` or `40001`. There are at most
four attempts with cancellation-aware 100, 200 and 400 ms waits. Unknown storage
outcomes and claim COMMIT acknowledgement failures stay fail-closed; Kubernetes
Job creation is never repeated by this retry helper.

The real-database regression holds `corpus_control` to exercise lock and statement
timeouts, verifies that a rejected claim leaves its request queued, then checks
that releasing the lock permits exactly one claim. An occupied single-connection
pool separately verifies that a pool admission timeout remains
`StorageUnavailable`. The initial closed-pool assertion was replaced because a
closed pool fails the existing health gate before reaching the SQL error mapper.
This fixture does not reproduce a lost COMMIT acknowledgement.

Affected implementation paths:

- `apps/server/src/corpus_runtime.rs`,
  `crates/adapters/src/corpus/collection_requests.rs`,
  `crates/adapters/src/law_go_kr.rs` and
  `crates/adapters/src/law_go_kr/admission.rs`: bounded deferral reasons, lease
  polling hints and rejection-category logging.
- `crates/adapters/migrations/0022_collection_deferral_reasons.sql` and
  `crates/adapters/src/postgres.rs`: compatible reason migration registration.
- `apps/server/src/collection_scheduler_retry.rs`, `apps/server/src/main.rs`,
  `apps/server/src/database.rs`, `crates/adapters/src/corpus.rs` and
  `crates/domain/src/legal.rs`: contention classification and scheduler retry.
- `crates/adapters/tests/scheduler_contention.rs`: real PostgreSQL regression.
  Deferral regression cases also live beside the changed adapter code.
- `docs/server.md` and `docs/upstream-policy.md`: public hints and retry contract.

## Reproducible checks

Local GNU checks used the pinned toolchain and verified dictionary cache, with
Cargo/Rustdoc flag overrides cleared so repository CPU flags apply:

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked -- --test-threads=1
cargo audit
cargo deny check
```

The workspace passed 424 tests with 135 database/dictionary cases explicitly
ignored. Audit retained the existing reviewed `lru` advisory
`RUSTSEC-2026-0253`; dependency admission passed with existing duplicate warnings.
After the test-only pool correction, focused compilation, clippy and formatting
passed again.
An initial sandboxed mock-socket run could not bind its fixture sockets. A later
parallel WebTransport attempt encountered a temporary-port collision; the final
GNU workspace run outside those socket restrictions used `--test-threads=1` and
passed. This establishes the serialized local result, rather than parallel
hosted-CI stability.

The authorized SSH-MCP `dev` host received source and stripped native GNU test
executables over SFTP. Archive SHA-256 and each of the 35 executable hashes were
checked before execution. Compile-time fixture paths and the companion server
path matched the source mirror; the execution host supports x86-64-v3. These
PostgreSQL tests were compiled locally and executed remotely, rather than built
by Cargo on the remote host.

The first PostgreSQL run failed the closed-pool expectation described above.
After correcting that test and transferring its replacement executable, all
135 database/dictionary tests passed across 35 targets, with zero failures and
zero ignored cases in the explicit ignored-test run. The corrected executable
SHA-256, checked by the remote manifest runner, is
`cd1f3d344351edeba2067c39993a0b4cbfe3feaa8ffb545a9f5f2d7d56666556`.
Both remote OxiBelt profiles passed HTTP and native WebTransport checks.

```sh
scripts/test-postgres.sh --prebuilt-directory "$TEST_ARTIFACT_DIRECTORY"
scripts/test-oxibelt.sh --profile fixture
scripts/test-oxibelt.sh --profile kubernetes
scripts/test-server-image.sh --platform linux/amd64 --target runtime
scripts/test-server-image.sh --platform linux/amd64 --target runtime-ingestion
scripts/test-server-image.sh --platform linux/arm64 --target runtime
scripts/test-server-image.sh --platform linux/arm64 --target runtime-ingestion
```

Final remote outcomes:

| Gate | Result |
| --- | --- |
| PostgreSQL 18 native executable run | 135 passed, zero failed or ignored; exit 0 |
| OxiBelt fixture profile | HTTP and native WebTransport passed; exit 0 |
| OxiBelt Kubernetes profile | HTTP and native WebTransport passed; exit 0 |
| AMD64 runtime image | Native Alpine/musl gate passed; exit 0 |
| AMD64 runtime-ingestion image | Native Alpine/musl gate passed; exit 0 |
| ARM64 runtime image | QEMU Alpine/musl gate passed; exit 0 |
| ARM64 runtime-ingestion image | QEMU Alpine/musl gate passed; exit 0 |

All four standard image invocations passed text-only and retained-corpus checks,
including exact citation search/fetch, history, packaged widgets, restart,
interrupted-index recovery and expected startup rejection for missing credentials,
untrusted PostgreSQL certificates, invalid blob permissions/ownership and missing
or corrupt dictionaries. Both ingestion targets additionally passed the packaged
kubectl synthetic TLS API fixture, including token replacement, Pod create/delete
and 401/403 handling. Temporary fixture containers, volumes and networks are
removed by the gates. The extracted native executables were removed after their
successful gates, preserving transfer archives, the corrected manifest and logs.

The gates use disposable PostgreSQL, verified full dictionary assets, fictional
records and synthetic upstream/API endpoints. Server images build the source in
Alpine/musl. AMD64 executes natively on `dev`; ARM64 uses QEMU emulation and does
not establish native ARM64 CI acceptance. The Kubernetes OxiBelt profile checks a
synthetic NodePort handoff, rather than real-cluster routing or policy enforcement.
Image validation uses the isolated clean checkout at `e17ed97`; the subsequent
`cbacb20` change only corrects the PostgreSQL test and leaves runtime code intact.
The first ARM64 build was cancelled before a verdict to increase the configurable
`BUILD_JOBS` from two to four after checking available memory. The four-job cache
prewarm was then cancelled before completion to use eight jobs, after sampled
host memory use was about 2.2 GiB on the 6.9 GiB execution host. An ARM64
runtime build with `--build-arg BUILD_JOBS=8` warms the Cargo cache; the standard image
gate commands above then run with their original defaults and assertions. This
environment adjustment does not alter the source or count as a passed gate.

## Independent review and remaining limits

Reviewer `/root/independent_review` inspected MCP/API Boundary and Upstream/Cache
Correctness at the integrated source snapshot
`d1512ca6bc362b5f4236b46b008d46111578ecd9d426a9ad621763e16072ceed`
without actionable findings. The test-only pool correction was reviewed again
against `e17ed97`, with file SHA-256
`0deffe33e389d392d09bb87cb41851f4bc857ae7ccc51aac1fb4e125c40991da`,
without actionable findings.

These changes do not prove that the deployed search corpus is complete or that
collection has resumed. Existing uncertain-response markers require a separate
ownership investigation before any operational recovery. Source-validation
rejections now expose only a bounded rejection stage/category in logs; no parser
acceptance rule was broadened without failed-response evidence. No live LAW
requests, image publication, source push or deployment form part of these tests.
