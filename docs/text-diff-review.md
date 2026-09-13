# Text comparison implementation review

Review date: 2026-09-13. Contract: [supplied-text comparison](text-diff.md).

Independent reviewer: Codex agent `independent_diff_review`, separate from the
implementers. Categories: MCP/API Boundary and Security, including shared
retention, resource accounting, cancellation and publication behavior. The reviewer
inspected the actual integrated patch and relevant surrounding contracts, then
renewed review after material fixes. No blocking findings remain in this scope.
This records agent review, not a human GitHub approval or a repository-wide audit.

## Exact scope

Base revision: `22ead58972a5121249003faa4bb9c635342306eb`.
The reviewed scope contains 50 modified/new files, excluding this evidence document.
SHA-256: `3f680e4870b4e6e8fb74804d9cebfa51564ddb4bed7d047af0ae8ba7a9e1aa3b`.

At the final implementation revision, reproduce the digest from the repository root:

```sh
python3 - <<'PY'
from pathlib import Path
import hashlib
import subprocess

base = "22ead58972a5121249003faa4bb9c635342306eb"
names = subprocess.check_output([
    "git", "diff", "--name-only", "--diff-filter=ACMR", "-z", base, "HEAD",
]).decode().split("\0")
paths = sorted(set(names) - {"", "docs/text-diff-review.md"})
digest = hashlib.sha256()
for name in paths:
    digest.update(name.encode() + b"\0" + Path(name).read_bytes() + b"\0")
print(len(paths), digest.hexdigest())
PY
```

## Findings addressed

- Complete MCP page responses now avoid duplicating the structured page in text
  content. HTTP tests measure the complete serialized response against 256 KiB.
- Widget cancellation keeps creation pending until its outcome is known; late
  created handles are deleted, and failed deletion remains retryable. Clear and
  page navigation observe the corresponding operation barriers.
- Maximum escaped inputs exceeded the edge's inherited 10 MiB request allowance.
  The fixture now explicitly permits 16 MiB only on `/mcp`.
- The first full-size native WebTransport run exposed a stream-buffering failure.
  Smaller flow-control windows passed the renewed gate while preserving message
  limits, application budgets, deadlines, certificate checks and stream restrictions.
  Reference-client phase diagnostics omit texts, handles and complete responses.

## Validation

The integration owner ran these checks successfully on the final runtime changes:

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
cargo audit
cargo deny check
DEMO_WIDGET_HTML=apps/widget/dist/index.html \
TEXT_DIFF_WIDGET_HTML=apps/widget/dist/text-diff.html \
OXIBELT_BINARY=target/oxibelt/debug/oxibelt scripts/test-oxibelt.sh
git diff --check
```

The workspace suite passed 92 tests and its doctest targets. Advisory checks used
current data; cargo-deny retained informational duplicate-version warnings.
The Docker gate verified the pinned OxiBelt revision
`72564d165dfd05cb29a64aeebd19fccd7944ea6f` and passed HTTP and WebTransport for both
supported MCP revisions, maximum-size comparisons, widget resources, deletion and
expected rejection checks. Earlier failed gate runs are described above; the final
gate passed without skipped assertions.

The frontend implementer ran all seven [required widget commands](../CONTRIBUTING.md#react-mcp-apps-widget),
including frozen installation, typechecking, 11 model tests, build, Chromium setup,
19 browser cases and current dependency admission checks. The comparison asset is
2,164,630 bytes, within its scoped 3 MiB raw-resource allowance.

The independent reviewer additionally executed eight focused application tests,
five focused adapter tests and a TOML placement check. The complete workspace,
frontend and Docker results above were reported to that reviewer by their runners;
they were not separately rerun by the reviewer.

## Remaining limits

Browser evidence uses a local MCP Apps host; live ChatGPT compatibility is untested.
High-latency network performance is unmeasured. Accounting limits do not guarantee
process RSS. A lost response or closed widget can leave retained results until
their fixed expiry; temporary-file cleanup does not promise secure erasure after
a host crash. This work does not integrate legal providers or deploy the service.
