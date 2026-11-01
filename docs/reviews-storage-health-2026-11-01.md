# Storage health diagnostics review, 2026-11-01

This record covers failure diagnostics, not a storage repair or production
acceptance. The [recovery checkpoint](recovery-2026-10-29.md) records the observed
serving and collection failures. Current procedures belong to the
[operator runbook](deployment-kubernetes.md) and [persistence contract](persistence.md).

## Reviewed scope

The base is signed revision
[`21be7a77138ce0acf83fb0dd18b3c95709372cb7`](https://github.com/PublicData-stream/openlegal4everyoneMCP/commit/21be7a77138ce0acf83fb0dd18b3c95709372cb7),
whose runtime source matches `c93f52e00806afe7fbfd7828346ac8ffd4dc2d88`.
The reviewed three-file patch has SHA-256
`6ae8ccf0c4531bfce4105b0887b5f36f36844b892596ea90c459c3147fcbe32e`:

- [`corpus.rs`](../crates/adapters/src/corpus.rs): distinguish SQL and corpus blob
  health failures before the existing error mapping.
- [`blob.rs`](../crates/adapters/src/blob.rs): distinguish deadline expiry and
  blocking-worker join failure in the existing owned-job watchdog.
- [`blob tests`](../crates/adapters/src/blob/tests.rs): exercise both watchdog
  outcomes for ordinary jobs and health probes.

A separate Codex agent reviewed the actual patch and affected callers under
**Upstream/Cache Correctness Review** and **Security Review**. The final review
resolved a test reliability finding: the fixture's lock holder now has a finite
20-second transaction and idle allowance. PostgreSQL 18's transaction timer is
restarted inside that test-only transaction before lengthening it. Rollback
restores its settings; the tested health pool and production retain their
one-second lock, two-second statement and five-second transaction limits.
This bounded review is not a full security audit or a human GitHub approval.

## Resulting diagnostics and preserved behavior

| Event | Recorded fields |
| --- | --- |
| Corpus health dependency failure | `operation=corpus_health`, `health_phase=sql` or `blob`, bounded `elapsed_ms`, closed error label |
| Owned blob job deadline or join failure | `operation=blob_health` or `blob_job`, `error_category=local_deadline` or `worker_join`, bounded `elapsed_ms`, `operation_timeout_ms`, `probe` |

Events contain static classifications and numeric or boolean metadata. They add
no raw error formatting, SQL text, paths, source bodies, identifiers, credentials
or connection URLs. The blob label retains the pre-mapping variant, including
`Shutdown` or `Busy`, while omitting a throttle payload.

SQL retries, returned errors, cancellation, physical permit ownership, failure
epochs, shutdown and durable acknowledgement remain unchanged. The watchdog
still has one timeout and retains ownership after the caller leaves. Late
completion cannot restore health. Default and cache blob operations retain five
seconds; the separately configured production corpus allowance remains 30 seconds.
No API, configuration, migration, dependency or legal-data policy changes occur.

Elapsed time covers the observed call, including applicable queueing, scheduling
and SQL retries. It is not a measurement of fsync or time in a particular wait.
The patch does not identify a filesystem syscall or errno, prove the historical
failure cause, or prevent an unavailable storage path from failing again.

## Validation at this record's cutoff

The initial focused parallel blob run failed: 22 passed and eight existing
startup or recovery assertions returned `StorageUnavailable`. Both new telemetry
tests passed. An unchanged serial diagnostic run passed all 30 tests. Those
results do not explain or erase the initial failures. Later local I/O pressure
and sampled WAL synchronization waits were observed separately; they do not
establish the earlier failure's cause or production causality.

Before the test-only lock-holder refinement, the complete local baseline passed:
formatting, workspace clippy, workspace tests, audit and dependency admission.
Workspace tests reported 525 passed, zero failed and 204 explicitly ignored.
The dedicated `scripts/test-postgres.sh` gate then executed 204 tests with zero
failures or ignored tests, including the new health-origin test. Ignored tests
were not counted as ordinary workspace passes.

After the test-only refinement, formatting and workspace clippy passed. A new
default parallel workspace attempt stopped in the adapters library with 166
passed, two failed and 72 ignored. The failures were the existing overlapping
health-check startup and ordinary-waiter recovery assertions, both returning
`StorageUnavailable`; later workspace packages were not executed in that attempt.
This failure remains unresolved and is not replaced by the earlier passing run.

The final serial workspace invocation then passed 525 tests with zero failures
and 204 ignored. Its explicit `--test-threads=1` controls independent fixture
load; intentional concurrency inside tests remains exercised. The refined
PostgreSQL fixture also ran successfully: one passed, zero failed or ignored,
239 filtered out. This focused check is distinct from the earlier full 204-test
gate. Commands:

```sh
cargo test --workspace --offline --locked -- --test-threads=1
scripts/test-postgres.sh -p openlegal-adapters --lib health_failure_telemetry_distinguishes_sql_and_blob_without_changing_errors
```

Hosted CI, native release images, registry publication, rollout and live diagnostic
acceptance remain separate gates. Exact-commit default parallel CI must pass
before a diagnostic image is adopted; local serial success does not establish
that result. No new provider recovery credential, native
cleanup or collection resumption is authorized by this review. Collection remains
quarantined; successful diagnostics alone do not establish two-hour stability,
full corpus completion or a storage repair.
