# Collection scheduler cancellation-drain review

> **Point-in-time review record, 2026-10-31.** Base revision:
> `671c36937770481b89601f5513319a04982e0e3b`. The reviewed patch is identified by
> the exact source hashes below; it was not yet committed or deployed at this
> review cutoff. Current contracts: [optional ingestion
> integration](deployment-kubernetes.md#optional-ingestion-integration),
> [persistence](persistence.md), and [review gates](../CONTRIBUTING.md#review-gates).
> Later changes belong in this header rather than rewriting the recorded review.

## Result and reviewed scope

Each review was performed by an **independent Codex reviewer**, separate from the
implementation under review. Both applied **Upstream/Cache Correctness Review** to
their respective scope: the scheduler lifecycle change or the composed PostgreSQL
regression fixtures. Neither final source review found a blocking issue within
its stated scope. This is bounded correctness review, not a repository-wide
security assessment, human GitHub approval, or operational acceptance.

| Reviewed source | SHA-256 |
| --- | --- |
| `apps/server/src/main.rs` | `2487cbf32ec2df4c6fb010df6f2c85ebc34496f6d317936068952cbecd4176d3` |
| `apps/server/src/collection_scheduler_lifecycle.rs` | `86013d4cc5c475673232a22397ccedeaa2bdada35a675f32b69ee304fd792174` |
| `crates/adapters/src/law_go_kr/admission/tests/lock_retry.rs` | `72a6c4d8acdd8e9781728bb7733f38a909464144b04abf3a4baca66dbadab044` |

The independent reports were checked against their actual bytes. Their portable
artifact identities are:

| Independent report | Review time (UTC epoch seconds) | SHA-256 |
| --- | --- | --- |
| Scheduler lifecycle first-poll review | `1793431407` | `209a9cd7cb3fdedc8a1bd4c73ea4ed61ba4f4492809ff0d7a6a75069e3ecd926` |
| Heartbeat/settlement PostgreSQL fixture review | `1793432798` | `4006a432d1e591e15fafcb8a731f48fdb3052fed4dd33e50cb37de16c3c9097b` |

## Lifecycle behavior established by source and focused tests

The scheduler previously selected between its dispatch/heartbeat supervisor and
the corpus background runtime, cancelling the shared child token when either
returned. Dropping the losing future could drop the runtime's `JoinSet` before
already-started response settlement drained.

The new `run_and_drain` helper pins both futures once and selects through mutable
references. When either completes, it cancels the shared token and awaits the
other future before returning. Main reaches its existing runtime/storage close
calls only afterward. The helper preserves the primary error; a secondary typed
`DatabaseError::Cancelled` is treated as sibling shutdown, while other secondary
errors propagate after primary success or are logged without replacing a primary
error. A primary cancellation is not silently reclassified as successful shutdown.

Each wrapper checks cancellation once, on its first poll. If the token is already
cancelled at that check, it does not enter the original future. This prevents
drainage from starting a previously unpolled sibling's startup writes, including
pilot initialization. Once the original future has started, later cancellation
does not bypass its drainage. The check does not make arbitrary side effects
globally atomic with external cancellation.

The focused lifecycle tests cover a manually released settlement task in a
`JoinSet`, drainage in both completion directions, primary-error priority, typed
secondary cancellation, cold siblings, initial parent cancellation, and exactly
one entry/completion for both already-started futures.

## PostgreSQL fixture scope

The two new ignored tests in `lock_retry.rs` are explicitly **composed SQL and
lifecycle models**, not production scheduler or HTTP end-to-end tests:

- `heartbeat_fatal_model_dropping_settlement_leaves_charged_abandoned_owner`
- `heartbeat_fatal_model_draining_settlement_preserves_charges_and_other_owner`

Their source uses the existing disposable PostgreSQL admission helpers to reserve
two charged guards without constructing an HTTP client. A barrier owns both the
budget and corpus-control rows before either fault subject starts. The actual
heartbeat API must return four known contention failures, each accompanied by a
server `55P03`; the model retains the existing 100/200/400 ms retry delays.

After the third heartbeat rejection, the response guard enters `complete()`.
After its first budget-lock rejection and acknowledged `ROLLBACK AND CHAIN`, an
explicit oneshot barrier holds the pinned settlement future unpolled. This
deliberate scheduling avoids settlement exhaustion racing the fatal model; it
does not reproduce the incident's exact timing.

The drop branch waits for actual loss of the detached session's advisory lock and
requires the charged owner row to remain. Its resume sender stays alive until
that lock-loss check, excluding a closed-channel panic as an alternative cause.
The drain branch releases the blocker, resumes and joins settlement, and requires
only that response's owner to disappear. Both branches check unchanged
daily/on-demand/pilot charges and preservation of the other owner's row and lock.
Only the fixture's existing barrier session disables its transaction/idle
deadlines; subject sessions retain the 1-second lock, 2-second statement and
5-second transaction settings.

These are source-reviewed assertions. Execution of the two ignored PostgreSQL
tests was **pending at the independent review cutoff**. The source review and
SQLSTATE observer unit test do not establish that those PostgreSQL assertions
passed.

## Checks recorded by the independent reviews

Commands are repository-relative. These outcomes apply to the reviewed source
above, not to a later integrated patch or published image.

| Check | Recorded outcome |
| --- | --- |
| `cargo test -p openlegal-server --bin openlegal-server collection_scheduler_ --offline --locked` | 16 passed: 10 lifecycle and 6 unchanged retry tests; no failures or ignored tests in this selection |
| `cargo +1.98.1 test -p openlegal-adapters --lib heartbeat_rejection_capture_requires_the_actual_lock_sqlstate --offline --locked` | 1 passed; distinguishes display-form `55P03` from `57014` and pool-timeout-only events |
| `cargo fmt --all -- --check` | Passed in the lifecycle review |
| `cargo +1.98.1 fmt --all -- --check` | Passed in the PostgreSQL fixture review |
| `git diff --check -- apps/server/src/main.rs apps/server/src/collection_scheduler_lifecycle.rs` | Passed in the lifecycle review; the new module was also read and hashed directly |
| Scoped `git diff --check` for the PostgreSQL fixture | Passed |

## Remaining limits

The production incident's root cause remains unconfirmed. The change protects
settlement already in progress from losing its supervisor future during sibling
shutdown. It does not guarantee settlement of a request interrupted while provider
HTTP is still pending; existing cancellation and uncertain-owner retention remain
unchanged.

Heartbeat failure remains fatal, with the same retry count and delays. Runtime
lease failures remain fatal. The patch does not change SQL timeout settings,
provider HTTP retries, charged accounting, uncertainty cleanup, schema, or
provider admission policy.

Final integrated workspace and PostgreSQL gate results were not part of these
source-review reports and are not claimed here. Hosted CI, image publication,
deployment, live-provider acceptance, a completed production clone and native UI
acceptance are separate evidence boundaries. No deployment of this runtime
change is established by this record.
