# Filesystem L2 implementation review

Review date: 2026-09-13. Base revision:
`461b6901224a39050fc7a6c430599881c0b8c453`.

This change adds optional durable retrieval storage, bounded immutable captures,
exact history tools and snapshot comparison. All provider evidence in tests is
synthetic. Captures do not establish legal revision identity or applicability.

## Independent review

Codex agent `plan_review` reviewed the adapter, server boundary, domain contracts,
comparison integration and widget patch. It did not implement those paths.
Codex agent `history_ui` independently reviewed the application persistence flow
and related documentation; it implemented the widget and did not review its own
widget work. The integration owner inspected the combined changes and tests.
Both reviewers renewed review after material corrections; no unresolved findings
remain in their respective scopes.

The applicable categories are Upstream/Cache Correctness, MCP/API Boundary,
Security, and capture/provenance aspects of Legal Data Integrity. The review covers
local capture identity, not real-provider mapping. These are focused agent reviews,
not human GitHub approvals or a repository-wide security audit.

Material findings prompted renewed review and regression evidence:

- Independent processor/schema heads and original retrieval timestamps preserve
  capture identity. Unchanged validation does not rewrite a capture; A→B→A and
  clock rollback create distinct eligible occurrences.
- L2 operations retain ownership through cancellation, child reaping and recovery.
  Startup failures also reap the worker and release the exclusive root.
- Epoch changes fence cleanup and recovery without invalidating unrelated L1
  entries on ordinary publication or unchanged revalidation at capacity.
- Capture expiry is enforced on the requested head even when a bounded global
  maintenance batch has not reached it.
- Filesystem checks enforce owner and private modes. Quarantine and orphan cleanup
  remain accounted, including corruption repair near the byte limit. Lowered
  configuration limits preserve retained history and require cleanup workspace.
- Empty-store initialization stages its format marker before atomic installation,
  so interruption cannot leave an authoritative partial marker.
- History parsers enforce complete search pages, bounded strings and exact query
  association. Supplied-text results cannot assert snapshot origins; editing
  clears the association and handle cleanup remains retryable.
- Documentation separates current freshness, capture retention, execution
  deadlines, owned reconciliation and optional feature combinations.

## Exact scope and validation

The reviewed scope contains 59 modified/new files, excluding this document.
Content SHA-256:
`ad375e2a993901ba53b403604b44dcbc11ed3bdf4f6da33398358769b08f00a5`.
Reproduce it from the commit containing this review (use that revision instead
of `HEAD` when reading this document after later changes):

```sh
python3 - <<'PY'
import hashlib
import subprocess

base = "461b6901224a39050fc7a6c430599881c0b8c453"
revision = "HEAD"
names = subprocess.check_output([
    "git", "diff", "--name-only", "--diff-filter=ACMR", "-z", base, revision,
]).decode().split("\0")
paths = sorted(set(names) - {"", "docs/filesystem-cache-review.md"})
digest = hashlib.sha256()
for name in paths:
    content = subprocess.check_output(["git", "show", f"{revision}:{name}"])
    digest.update(name.encode() + b"\0" + content + b"\0")
print(len(paths), digest.hexdigest())
PY
```

The integration owner ran these checks successfully on the final Rust changes:

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
cargo build --workspace --bins --examples --locked
cargo audit
cargo deny check
git diff --check
```

The final rootless Docker integration gate also passed:

```sh
DEMO_WIDGET_HTML=apps/widget/dist/index.html \
TEXT_DIFF_WIDGET_HTML=apps/widget/dist/text-diff.html \
OXIBELT_BINARY="$PWD/target/oxibelt/debug/oxibelt" \
SERVER_BINARY="$PWD/target/debug/openlegal-server" \
WT_CLIENT_BINARY="$PWD/target/debug/examples/wt_client" \
MOCK_UPSTREAM_BINARY="$PWD/target/debug/examples/mock_upstream" \
scripts/test-oxibelt.sh
```

The harness verified the pinned clean OxiBelt revision
`72564d165dfd05cb29a64aeebd19fccd7944ea6f`, both supported MCP revisions on HTTP
and native WebTransport, exact history and comparison origins, positive progress,
widget resources, maximum comparison inputs, deletion, reconnection and expected
Origin/path/certificate rejection. No assertions were skipped.

The workspace suite passed 147 tests and all doctest targets. This includes 43
adapter tests, 34 application tests, real-child restart/cancellation/concurrency
regressions, and both native transport suites. Advisory checks used current data;
cargo-deny retained informational duplicate-version warnings. Changed Markdown
relative links and shell/Python syntax were also checked.

The widget implementer ran all seven
[required widget commands](../CONTRIBUTING.md#react-mcp-apps-widget), including
frozen installation, typechecking, 24 model tests, production builds, Chromium
setup, browser cases and current advisory/license checks. The integration owner
reran the final 34 browser cases successfully. The record asset is 898,555 bytes
and comparison asset 902,501 bytes, within their 1 MiB and 3 MiB allowances.

Reviewers inspected regression assertions and actual code; the full check results
above were reported by their runners, not independently repeated by reviewers.

An intermittent test-fixture startup failure was reproduced with Linux
`ExecutableFileBusy` (`ETXTBSY`, errno 26). Concurrent fork/exec tests could briefly
inherit descriptors for freshly written scripts or in-process cache locks from
other tests. The fixture isolation and deterministic launch-failure regression
address that interference; production launch failures remain explicit errors.
The production cache lock belongs to the isolated worker, which does not fork.

## Remaining limits

This store supports one owning process on a dedicated local Linux filesystem.
It is a bounded cache, not a permanent archive. The first persisted format has no
automatic migration or reprocessing. Retention can remove snapshots while
transient comparison handles still exist.

Failure injection and child termination exercise process-crash recovery, not
physical power-loss behavior on every filesystem. Digests detect accidental
corruption, not malicious changes by another process with the same privileges.
Kernel I/O that cannot be interrupted can prevent confirmed reaping; admission
then remains closed and no replacement writer starts.

Browser tests use a local MCP Apps host. Live ChatGPT compatibility, real legal
providers and deployment remain untested by this change.
