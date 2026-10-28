# MeCab zstd security maintenance review

> **Historical record.** Recording commit: the commit introducing this file
> (`git log --diff-filter=A -1 --format=%H -- docs/zstd-review.md`). Base revision:
> `610e5f6ed2b83ec105f0ce89574a57c8a6257462`. Not updated after recording.
> Current behavior: [dependency admission](dependencies.md). Index:
> [review and validation records](records.md).

Date: 2026-10-28. Review category: **Security Review**.

## Change and compatibility

The [dependencies job](https://github.com/PublicData-stream/openlegal4everyoneMCP/actions/runs/40505424194/job/122193386899)
reported four vulnerabilities and ten unsoundness warnings in the MeCab zstd
graph; its following `cargo deny check` was skipped. Published MeCab dictionary
and builder 0.7.2 require `zstd ^0.13`, which cannot select the fixed 0.14 release.

The local patches retain the published Rust source, features, versions and
licenses. Only the normalized manifests' zstd requirements change to `0.14.3`;
published development lockfiles are omitted and local provenance notices added.
Verified archive URLs, SHA-256 values and upstream revision are retained in
[vendor provenance](../vendor/README.md). The root lockfile selects
`zstd 0.14.3`, `zstd-safe 8.2.0` and `zstd-sys 2.1.2+zstd.1.5.7`, with no advisory
exception or unrelated dependency refresh.

Both production server and retained-fixture Docker contexts include the patched
sources. Successful Cargo artifacts still determine the production notice
inventory; built third-party path dependencies now retain their licenses and
patch notices. The standalone document-worker workspace and lockfile are unchanged.

The unchanged baseline revision and patched implementation independently produced
byte-identical dictionary manifests and all five referenced files, with 816,283
entries. Both passed the full-dictionary dual-engine test. Canonical dictionary
identity: `48b1d12acea6aefebd0a58cca2b8368a353bdc2c1dfa22645fe69dff6e64636d`.
Dictionary format, analyzer identity, public interfaces and runtime configuration
remain unchanged; this patch requires no index rebuild or persistence migration.

Large synthetic status fixtures also needed bounded INSERT batches to fit the
existing two-second statement deadline on the local test environment. All row
ranges, filtering, payloads and assertions remain; production deadlines are unchanged.

## Verification

Commands run from the repository root with Rust 1.98.1, the x86-64-v3 baseline,
an external Cargo target cache and disposable host files under `/tmp`. Host and
native image builds used two Cargo jobs; local ARM64 server builds used the
existing Dockerfile `BUILD_JOBS=4` argument through a temporary CLI wrapper.
Compiler flags and gate assertions were unchanged. Dependency tools were
cargo-audit 0.22.2 and cargo-deny 0.20.2.

| Check | Result |
| --- | --- |
| `cargo fmt --all -- --check` | Passed |
| `cargo clippy --workspace --all-targets --locked -- -D warnings` | Passed |
| `cargo test --workspace --locked` | 485 passed, 187 explicitly ignored |
| `cargo test --locked -p openlegal-server --example wt_client serving_smoke` | 8 passed |
| `cargo audit`; `cargo deny check` | Passed with current advisory data; duplicate-version warnings remain |
| `python3 apps/server/tests/test_collect_notices.py` | 25 passed, also passed inside both architecture builds |
| `python3 -m unittest scripts.tests.test_release_image_contract scripts.tests.test_select_release_ci` | 16 passed |
| `git archive --format=tar HEAD vendor` inspection | All 50 vendor files, both license sets and patch notices included |
| `scripts/test-korean-tokenization.sh` with baseline and new dictionaries | Passed both; all five artifact checksums identical |
| `scripts/test-postgres.sh -p openlegal-adapters --test corpus_status_snapshot` | 6 passed after bounded fixture loading |
| `scripts/test-postgres.sh` | 187 passed in the complete serialized run |
| `scripts/test-server-image.sh --platform linux/amd64 --target runtime` | Passed complete acceptance |
| `scripts/test-server-image.sh --platform linux/amd64 --target runtime-ingestion` | Passed, including packaged kubectl TLS/token fixture |
| `scripts/test-server-image.sh --platform linux/arm64 --target runtime` | Passed; local QEMU execution |
| `scripts/test-server-image.sh --platform linux/arm64 --target runtime-ingestion` | Passed, including packaged kubectl TLS/token fixture; local QEMU execution |
| `git diff --check` | Passed |

Configured rootless/rootful Docker overlay mounts initially failed. Local validation
uses a disposable classic-overlay daemon and dedicated containerd in a private
mount namespace with ext4-backed `/tmp`; configured daemons and global `/tmp`
remain unchanged. CRI plugins are disabled in the dedicated runtime. Temporary
dictionary/test files use a task-local tmpfs under `/tmp` to avoid virtiofs stalls.
An initial database run had four fixture-initialization failures correlated with
Docker ENFILE errors; relocating runtime state allowed all 37 corpus cases to pass.
Subsequent status-fixture INSERT timeouts were reproduced in isolation and corrected
by batching setup without changing the read deadline. Initial native image serving,
restart and citation checks passed, but a controlled rebuild interruption failed
its required `137:false` assertion. Final native serving acceptance passed that
unchanged assertion, incomplete-index rejection, rebuild, negative startup cases
and recovered serving. Native ingestion acceptance also passed its TLS/controller
fixture. Both final emulated ARM64 gates passed the same interruption, recovered
serving and startup-rejection assertions; ARM64 ingestion also passed its
packaged kubectl TLS/token fixture. No gate assertion was relaxed.

## Independent review and limits

A separate Codex agent independently reviewed the actual patch, published archives,
dependency paths, dictionary callers, workspace membership, notice selection,
Docker inputs and release/source handling. Renewed review covered the retained
helper context and exact data equivalence of the fixture batching. No blocking
finding remained. The reviewer separately verified the Rust/admission, PostgreSQL
and image logs and byte-identical dictionary evidence; the parent implementer owns
execution of the checks. This is agent review, not human GitHub approval or a
whole-repository security assessment.

Reviewed implementation SHA-256:
`c00a518213896a84f3d0f26d32e28274ef49723ee0859645855c56fce3786625`.
It covers the binary Git diff from the base for the root Cargo manifest/lockfile,
server Dockerfile/context, notice collector/tests, dictionary compression test,
dependency documentation, image gate, retained-helper Dockerfile/context and status
fixture test. Append each of the 50 sorted regular `vendor/` files as repository
path, NUL, file bytes, NUL before finalizing SHA-256. This record and its index
are excluded, so the digest remains reproducible after committing the record.

Reproduce at the recording commit:

```python
from pathlib import Path
import hashlib
import subprocess

base = "610e5f6ed2b83ec105f0ce89574a57c8a6257462"
paths = [".", ":(exclude)vendor/**", ":(exclude)docs/records.md",
         ":(exclude)docs/zstd-review.md"]
digest = hashlib.sha256(subprocess.check_output([
    "git", "diff", "--binary", "--no-ext-diff", base, "--", *paths,
]))
for path in sorted(p for p in Path("vendor").rglob("*") if p.is_file()):
    digest.update(path.as_posix().encode() + b"\0" + path.read_bytes() + b"\0")
print(digest.hexdigest())
```

Local ARM64 emulation does not establish native hosted ARM64 acceptance. Hosted CI,
GHCR publication, production deployment, live legal-provider acceptance and native
ChatGPT UI acceptance are separate evidence boundaries.
