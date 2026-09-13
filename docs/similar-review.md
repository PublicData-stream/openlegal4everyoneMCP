# Rust line and character comparison migration

The migration replaces the Git CLI with `similar` 3.2.0 in a killable mode of the
server executable. Both MCP revisions retain version 1 patches and add Unicode
scalar highlight ranges. React renders those ranges without calculating a second
diff. The contract and configuration migration are in [text comparison](text-diff.md).

## Independent review

Review date: 2026-09-13. Independent reviewer: Codex agent `worker_plan_review`,
separate from the implementation owners. Categories: MCP/API Boundary and Security,
including subprocess lifecycle, worker framing, exact source/range mapping,
resource accounting, publication and browser rendering. This is agent review,
not human GitHub approval or a repository-wide audit.

The reviewer renewed review after the corrections below and independently
reproduced the final content digest. No unresolved findings remain in this scope.
The reviewer performed static review; test execution was owned by the integration
owner and widget implementer.

Review corrections and evidence:

- Private engine result types live with the application interface. Domain owns
  public scalar ranges and fragment annotations.
- Tests exercise a live worker declaring an oversized patch or metadata section;
  both errors kill and reap the process. Metadata overflow returns a typed error
  frame instead of a partially successful result.
- Application validation requires the ordered fixed patch headers, change-bearing
  hunks, exact source correspondence and complete annotation consumption. Equal
  originals require an empty patch, preserving the summary/page invariant.
- Browser parsing checks cumulative rows, ranges and bytes before processing a
  complete page and stops at the first overflowing row/fragment. Regression tests
  prove later data is not processed after a budget failure.

## Exact scope

Base revision: `290078409573f7b6f52d571d8be9912f9f6b1a1f`.
The reviewed scope contains 39 modified/new files, excluding this evidence
document. SHA-256:
`75a03fcc1bbc4bd0d761fa2d3b8d7e9ba7148bf2c1fea706f8c59d72737b41fd`.

After committing, reproduce the digest from the repository root:

```sh
python3 - <<'PY'
from pathlib import Path
import hashlib
import subprocess
base = "290078409573f7b6f52d571d8be9912f9f6b1a1f"
names = subprocess.check_output([
    "git", "diff", "--name-only", "--diff-filter=ACMR", "-z", base, "HEAD",
]).decode().split("\0")
paths = sorted(set(names) - {"", "docs/similar-review.md"})
digest = hashlib.sha256()
for name in paths:
    digest.update(name.encode() + b"\0" + Path(name).read_bytes() + b"\0")
print(len(paths), digest.hexdigest())
PY
```

## Validation

The integration owner and widget implementer ran these contributor checks:

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
cargo audit
cargo deny check
pnpm --dir apps/widget install --frozen-lockfile
pnpm --dir apps/widget typecheck
pnpm --dir apps/widget test
pnpm --dir apps/widget build
pnpm --dir apps/widget exec playwright install --with-deps chromium
pnpm --dir apps/widget test:browser
pnpm --dir apps/widget check:dependencies
```

All commands above passed. The final Rust suite passed 107 tests and completed
all workspace doctest targets; the widget passed 19 model tests and 21 Chromium
browser tests. Built resource sizes were 866,890 bytes for the synthetic browser
and 898,601 bytes for comparison. Current Rust/npm advisory checks found no vulnerabilities. Cargo dependency checks
passed with the existing duplicate-version warnings. `cargo tree --locked -p
similar -e normal` confirmed no enabled runtime dependencies below `similar`.
Widget license admission checked the installed frozen graph.

The rootless Docker gate uses the pinned, verified clean OxiBelt revision and both
complete built widget assets:

```sh
OXIBELT_BINARY=target/oxibelt/debug/oxibelt \
DEMO_WIDGET_HTML=apps/widget/dist/index.html \
TEXT_DIFF_WIDGET_HTML=apps/widget/dist/text-diff.html scripts/test-oxibelt.sh
```

HTTP and native WebTransport exercise revisions `2026-07-28` and `2025-11-25`,
scalar annotations, 1 MiB inputs, source reconstruction, paging, resource loading
and deletion. The runtime image no longer installs Git. Expected negative
TLS/origin/path cases remain part of the gate. The gate passed again after both
review fixes and the final widget rebuild.

Focused evidence covers ASCII/CJK/astral/combining characters, BOM, LF/CRLF/lone CR,
missing final LF, multiple highlights and unequal replacement blocks; exact range
limits, encoded page limits and retained allocation accounting; malformed frames,
process failure/cancellation/reaping and startup dispatch before logs/config; and
browser rendering that follows supplied ranges and keeps HTML-like input inert.

## Compatibility and limitations

Operators must remove `text_diff.git_path` and rebuild the widget. Existing patch
fields and tool/resource identities remain unchanged; new widgets require the
additive range metadata. Character units are scalars, not grapheme clusters.
Successful diffs preserve exact text, but alignment/counts may differ from Git.

Complete character computation can reach the existing deadline or resource limits;
the comparison fails without publishing a line-only or truncated result. Two
killable workers and accounted retention bounds do not constitute an RSS sandbox.
Whitespace checks, shell/Python syntax checks, 63 relative Markdown file links
and the reproducibility of the historical Git review digest were also checked.
The test fixtures are synthetic. No live legal provider, ChatGPT host, deployment
or publication was tested by this migration.
