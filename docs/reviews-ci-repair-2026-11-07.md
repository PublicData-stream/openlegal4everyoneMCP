# Clock and maintained document parser review

> **Historical record.** Recording commit: the commit introducing this file
> (`git log --diff-filter=A -1 --format=%H -- docs/reviews-ci-repair-2026-11-07.md`).
> Base revision: `15c1e4ac2c64153a21410c4584410f29cb6df4e6`.
> Current behavior: [dependency admission](dependencies.md),
> [document processing](document-sandbox.md), [provider provenance](providers/kr-law-go-kr.md),
> and [persistence](persistence.md). Index: [review records](records.md).
> Not updated after recording.

Date: 2026-11-07.

## Problem and resulting behavior

The [PostgreSQL job](https://github.com/PublicData-stream/openlegal4everyoneMCP/actions/runs/42345001008/job/128320732120)
failed the first HEAD `database.get` in the real corpus/MCP scenario with
`freshness_unavailable`. Default clocks independently truncated their Unix anchor
and elapsed time. Clocks created at 100.100 and 100.700 seconds could report 101
and 100 at real time 101.200. Publication raises its timestamp to its own clock;
a read through the other clock could therefore reject a freshly published capture.
The job did not retain the internal clock values; the exact skew in that run was
not measured. The deterministic arithmetic regression establishes the defect.

Default clocks now share one process-local standard monotonic anchor with full
Unix-duration precision. Addition precedes truncation. Injected clocks, strict
future-time refusal, freshness thresholds, Tokio deadlines, history and legal
dates retain their contracts. A process restart can still expose a genuine wall
clock anomaly; stored timestamps are not clamped or rewritten.

The [document-worker job](https://github.com/PublicData-stream/openlegal4everyoneMCP/actions/runs/42345001008/job/128324039225)
failed admission for the newly issued maintenance advisory
[RUSTSEC-2026-0444](https://rustsec.org/advisories/RUSTSEC-2026-0444.html).
The standalone graph changes only converter 3.18.6 to 3.20.0, astral-tl 0.8.0 to
astral-html 0.0.4, and the converter's existing-smallvec dependency edge.
The [tagged converter release](https://github.com/xberg-io/html-to-markdown/releases/tag/v3.20.0)
requires Rust 1.99. Repository manifests, CI/release commands and three Rust
builders therefore use Rust 1.99.1. The verified Alpine 3.24 multi-platform index is
`sha256:44819b27be4c97b0e4d7203bd2e02c4820f97700211b6a37fb4c246a2de59383`.
Architecture flags, dynamic musl linkage, parser features and existing maintenance
exceptions are unchanged. Three atomic operations use the new `try_update` name;
Rust's deprecated `fetch_update` forwards to that same implementation with the
same arguments.

Native PDF text can reach Xberg's HTML cleanup. The fictional PDF regression
proves literal markup survives native extraction before checking the new
converter's output, Korean text, source digest, page and processor identity.
The binary worker identity includes converter/tokenizer versions. Successfully
projected LAW attachments also retain `attachment_processors` metadata binding
original ordinal, source SHA-256 and exact processor version. Invalid admission
restores the previous mapping; skipped and original-only resources add none.
Metadata participates in record equality and retained checksums, distinguishing
same-text projections produced by different processors. Existing captures stay
unchanged. The root XML/HTML identity and public schemas remain intact.

The fixture target now includes both ordinary and ignored binary-format tests.
Existing malformed-PDF cases use timer-only runtimes, matching the production
worker and retaining the canonical no-socket sandbox. All 13 native/OCR, malformed
PDF and archive cases therefore execute in the confined image.

The image retains astral-html's embedded WHATWG BSD-3-Clause entity-table notice
alongside its MIT/Apache crate notices. Notice extraction uses the same locked
registry cache as compilation; image checks require the packaged notice.

The WebTransport rate fixture also needed deterministic timing. Separate Docker
client launches could outlast its one-second refill period. The corrected fixture
runs fresh client processes in one hardened container using a generated 6r/m,
burst-one policy. It requires `SessionRejected` within a measured ten-second
window, then verifies recovery after 10.2 seconds. Committed edge rates and the
pinned upstream are unchanged.

## Independent review

The **Independent Codex clock/compiler reviewer** reviewed the clock, compiler,
atomic operations, temporary-directory fixtures and edge-harness scopes. The
**Independent Codex parser/provenance reviewer** reviewed the worker, attachment
version bindings, notices and expanded format-fixture scope. Both inspected the
actual source, affected callers and upstream implementations. Reviewers did not implement the scope they reviewed.
No source-level blocking findings remained after the cache-mount and retained
attachment-version corrections. This is agent review, not human GitHub approval
or a repository-wide security assessment.

| Scope | Categories | Exact binary-diff SHA-256 from the base |
| --- | --- | --- |
| Application clock and freshness tests | Upstream/Cache Correctness Review; MCP/API Boundary Review | `e532777ce8b4e4d360eec09800f73ad8d571cb193b6e26465f8e1a5c19fd2d3a` |
| Nine parser, provenance, notice and owning-document files | Security Review; Legal Data Integrity Review; MCP/API Boundary Review | `72cebfc6d75961de1ed421e76839c60f5a8fda3ff2c918341986ad51a62d9fa0` |
| Two TMPDIR-aware rejection-test fixtures | Security Review (test-fixture scope) | `c1dec6274fd45a9cca8c5da49e01703dcfe6feaabfcd768b603a8739cf90d321` |
| Measured WebTransport rate fixture | Security Review; MCP/API Boundary Review | `f2067104e05158465f76fc07ebd6ade40ee93c72bfbd87dedd4c660ba0141216` |

Compiler/image review applied **Security Review** and **MCP/API Boundary Review**;
the atomic-name changes also passed **Upstream/Cache Correctness Review**.
Signed commit `a9f8572` records that compiler baseline and compatibility delta.
Compiler/image review independently verified the official Rust image index and
[tagged atomic forwarding implementation](https://github.com/rust-lang/rust/blob/1.99.1/library/core/src/sync/atomic.rs),
cache version/architecture separation, build paths, release metadata commands,
CPU/musl flags and retained notice extraction. Its final review also covered the
three atomic renames and the worker notice checks.

The nine-file parser scope is the worker lockfile, binary processor implementation
and fixture tests, worker Dockerfile and gate, LAW adapter, and provider/dependency/
sandbox documentation. Focused mock processor tests validate actual projection
metadata and rollback; they do not execute provider HTTP retries.

## Verification

Rust checks use 1.99.1 and the existing x86-64-v3 baseline. Cargo commands use
`CARGO_TARGET_DIR=/tmp/rust/target`, two build jobs, and the privileged local command
channel. Fixture data, deployment tools and temporary adapters stay under `/tmp`.
Local ARM64 server builds use the existing explicit `BUILD_JOBS=4` argument;
committed builder defaults stay at two jobs. These ARM64 checks use QEMU.
Advisory tools are cargo-audit 0.22.2 and cargo-deny 0.20.2. Fresh disposable
advisory repositories were used after pre-existing cache ownership/object errors;
only cache locations changed in temporary deny configurations.

| Check | Result |
| --- | --- |
| Workspace/worker formatting; actionlint; changed-shell ShellCheck; diff whitespace | Passed |
| Final workspace Clippy, all targets, locked, warnings denied | Passed |
| Clock, freshness and unchanged comparison-retention focused tests | 3 + 3 + 1 passed |
| Attachment processor metadata regressions | 2 passed |
| Workspace tests including doctests, serialized, disposable tmpfs | 610 passed, 236 explicitly ignored |
| Root and standalone worker audit/deny with current advisory data | Passed; three existing worker maintenance notices remain |
| Release contract and CI-selection tests | 16 passed |
| Full pinned Korean dictionary gate | Passed, source manifest/checksums verified |
| WebTransport serving-smoke example tests | 8 passed |
| Deployment tools and serving-template gate | Passed, including 81 fixture checks |
| Full PostgreSQL integration, volatile ordinary fixture data | 236 passed, including the original two-protocol MCP scenario |
| Authenticated replication gate, standard disposable TLS primary/standby | 7 passed |
| Confined worker image gate | Passed: 3 library tests, all 13 native/malformed/archive fixtures, framing/PID/empty-log checks |
| Both OxiBelt profiles | Passed; measured handshake rejection at 0.173s / 0.177s, then refill recovery |
| Server runtime image, amd64 | Passed complete production-image acceptance |
| Server ingestion image, amd64 | Passed, including synthetic kubectl TLS/token/quota/create/delete/401/403 checks |
| Server runtime image, arm64 | Passed complete local-QEMU acceptance on an identical retry; first restart exceeded the storage budget (below) |
| Server ingestion image, arm64 | Passed complete local-QEMU acceptance, including synthetic kubectl TLS/token/quota/create/delete/401/403 checks |

The initial ordinary-disk workspace run failed and stalled a blob health test.
Independent isolated diagnosis measured physical filesystem work beyond the
unchanged five-second watchdog. A direct single-test retry passed, while a blob
suite under load reproduced several unrelated open/put deadline failures. A
serialized ordinary-disk run and database creation also exceeded storage deadlines.
Own competing builds were stopped. Functional workspace verification then used a
disposable tmpfs below `/tmp`; two tests were changed to honor TMPDIR instead of
hardcoding `/tmp`. Their privacy, assertions and production deadlines are unchanged.

The first ARM64 runtime-image attempt passed its initial retained HTTP cycle,
but failed readiness after restart. Sanitized blob-bootstrap telemetry recorded
`logical_deadline` in `health`: 1,035 microseconds queued, 1,030,998 opening,
and 3,992,829 in health work, physically completing after 5,025,127 microseconds
against the unchanged 5,000 ms budget. The identical gate was rerun using its
completed image-build cache; storage configuration, limits and assertions were
unchanged. This failed attempt is retained separately from the final result.

The PostgreSQL tmpfs adapter changes only this gate's named disposable database
containers. PostgreSQL 18 uses a 4 GiB volatile data filesystem and a 6 GiB memory
limit to accommodate the existing 16,000-body status fixture; the first 512 MiB
trial exhausted fixture space. Images, CPU allowance, database configuration,
queries, row counts and assertions are unchanged. The legacy-version fixture
uses its own legacy data path. These are functional checks on volatile storage,
not normal-disk performance, power-loss durability or capacity acceptance.

## Reproduction and limits

Run the required commands in [Contributing](../CONTRIBUTING.md#testing-and-ci).
Use the unchanged image/transport gates; configure `OPENLEGAL_DEPLOY_TOOLS` under
`/tmp`, and point OxiBelt binary/target overrides at the requested Cargo target.
For a volatile test mount, select a private TMPDIR below `/tmp`; retain the exact
PostgreSQL images and test assertions when choosing fixture storage capacity.

The following are the executed gate forms. `fixture_tmp` names the private 4 GiB
tmpfs below `/tmp`; `fixture_bin` contains the temporary Docker adapter described
above (also adding explicit `BUILD_JOBS=4` only for ARM64 server builds). The full
verified dictionary and deployment tools are selected explicitly under `/tmp`.
The OxiBelt binary was built from the pinned clean source by the fixture gate,
then reused after its embedded source identity was checked.

```sh
# validation_tmp is the prepared private fixture root below /tmp.
: "${validation_tmp:?set the prepared fixture root}"
fixture_tmp="$validation_tmp/io"
fixture_bin="$validation_tmp/bin"
export DOCKER_HOST=unix:///var/run/docker.sock
unset DOCKER_CONTEXT DOCKER_TLS DOCKER_TLS_VERIFY DOCKER_CERT_PATH
unset RUSTFLAGS CARGO_ENCODED_RUSTFLAGS RUSTDOCFLAGS CARGO_ENCODED_RUSTDOCFLAGS
export CARGO_TARGET_DIR=/tmp/rust/target CARGO_BUILD_JOBS=2
export OPENLEGAL_DEPLOY_TOOLS="$validation_tmp/deployment-tools"
export OPENLEGAL_TEST_MECAB_DICTIONARY="$validation_tmp/dictionary"
export SERVER_BINARY="$CARGO_TARGET_DIR/debug/openlegal-server"
export WT_CLIENT_BINARY="$CARGO_TARGET_DIR/debug/examples/wt_client"
export MOCK_UPSTREAM_BINARY="$CARGO_TARGET_DIR/debug/examples/mock_upstream"
export OXIBELT_TARGET_DIR="$CARGO_TARGET_DIR"
export OXIBELT_BINARY="$CARGO_TARGET_DIR/debug/oxibelt"

cargo fmt --all -- --check
cargo fmt --manifest-path apps/document-worker/Cargo.toml -- --check
TMPDIR="$fixture_tmp" cargo clippy --workspace --all-targets --locked -j 2 -- -D warnings
TMPDIR="$fixture_tmp" cargo test --workspace --locked -j 2 -- --test-threads=1
PATH="$fixture_bin:$PATH" TMPDIR="$fixture_tmp" scripts/test-postgres.sh
scripts/test-replication.sh
cargo test --locked -p openlegal-server --example wt_client serving_smoke -j 2
python3 -m unittest scripts.tests.test_release_image_contract scripts.tests.test_select_release_ci
scripts/test-korean-tokenization.sh
scripts/test-kubernetes-serving.sh
scripts/test-document-worker.sh
TMPDIR="$fixture_tmp" scripts/test-oxibelt.sh --profile fixture
TMPDIR="$fixture_tmp" scripts/test-oxibelt.sh --profile kubernetes
for platform in linux/amd64 linux/arm64; do
  for target in runtime runtime-ingestion; do
    PATH="$fixture_bin:$PATH" TMPDIR="$fixture_tmp" \
      scripts/test-server-image.sh --platform "$platform" --target "$target"
  done
done
actionlint
shellcheck scripts/test-document-worker.sh scripts/test-oxibelt.sh
git diff --check
```

Dependency admission used `cargo audit --db "$validation_tmp/advisory-db"` for
both lockfiles (adding `--file apps/document-worker/Cargo.lock` for the worker).
`cargo deny check` and its standalone-worker invocation used temporary copies of
the committed policies changing only `[advisories].db-path` to a fresh directory
below `validation_tmp`; all advisory/license/source rules remained identical.

The implementation diff, excluding this record and its index, has SHA-256
`0c2de4cc1a07e6adcda43bab85a4b760d8b101de8e90a1ef3d8a3d076e7f69e8`.
Reproduce from the recording commit:

```sh
base=15c1e4ac2c64153a21410c4584410f29cb6df4e6
git diff --binary "$base" HEAD -- . \
  ':(exclude)docs/reviews-ci-repair-2026-11-07.md' \
  ':(exclude)docs/records.md' | sha256sum
```

No provider traffic, production migration, publication or deployment was performed.
No GitHub push or fresh hosted workflow result is claimed. Both local ARM64 image gates used QEMU;
these results are separate from native ARM64 CI. Minikube/hardened-cluster and
production storage acceptance remain separate operations.
