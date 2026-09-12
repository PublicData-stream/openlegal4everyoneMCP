# Retrieval and MCP Apps validation

Validation date: 2026-09-12. Base revision:
`ff98d3802193657bfc9f28347688d04425c3135c`.

## Implemented scope

The change adds trusted compiled processing modules, a shared bounded retrieval
service, replaceable HTTP/cache adapters, two synthetic JSON layouts, typed MCP
results, optional progress, registered UI resources, and an opt-in React browser.
Streamable HTTP and native WebTransport use the same service. The
[retrieval contract](retrieval.md), [server contract](server.md), and
[local demo guide](demo.md) describe the resulting behavior and limits.

No real legal-data source, deployment, live ChatGPT connection, public DNS/TLS
integration, or provider quota is established by this work. Browser acceptance
uses a local simulated host with the real MCP Apps SDK bridge. Production source
admission and legal semantics still require their own evidence and review.

## Independent review

Separate Codex agents reviewed code they did not implement:

| Reviewer | Exact scope and applicable gates | Conclusion |
| --- | --- | --- |
| `/root/widget` | `crates/domain`, `crates/normalization`, `crates/application`, and `crates/adapters/src/cache.rs`; shared retrieval and architecture documentation. Upstream/Cache Correctness and parser/input Security, including synthetic identity and provenance. | No unresolved findings after corrections and re-review. Independently ran application, normalization, and cache tests; checked the final SHA-256 representation against four independent fixed vectors. |
| `/root/upstream_contract` | Server registry, handler, endpoint supervision, progress, resources, WebTransport, and their regression tests; final server contract, CI, and OxiBelt harness. MCP/API Boundary and Security. | No unresolved findings after corrections and re-review. Independently ran the focused MCP/transport regression suite. |
| `/root/server_extension` | HTTP adapter and resolver, mock upstream, demo composition/configuration/metrics, cross-transport demo test, and widget source/build/dependencies/tests. MCP/API Boundary, outbound/parser/UI Security, and dependency admission. | No unresolved code findings after corrections and re-review. Independently ran HTTP adapter and local DNS cancellation tests; reviewed integration documentation and smoke coverage. |

The parent reviewed the integrated patch and the final smoke-test corrections.
Corrections included expired-evidence maintenance, canceled-generation publication,
fatal worker propagation, full provider cooldowns, bounded asynchronous DNS,
absolute DNS names, widget identity/query validation, and freshness wording that
explicitly describes a returned snapshot. Smoke assertions require actual progress
for successful token-bearing demo calls and enforce the five-event limit.

These are agent technical reviews, not human GitHub approvals, an exhaustive
security scan, or an audit of every transitive dependency. Real legal identity,
date interpretation, applicability, and citation mappings remain unimplemented;
the fictional fixtures cannot establish those properties.

## Validation evidence

The final Rust checks passed with the repository's Rust 1.98.1 toolchain:

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
cargo build --workspace --bins --examples --locked
cargo audit
cargo deny check --hide-inclusion-graph
```

All 65 Rust tests passed. Five crate doctest targets ran with zero doctests.
Tests include local HTTP payload failures, controlled-clock cache/coalescing and
cancellation cases, bounded resources/progress, supervised workers, and shared
HTTP/WebTransport synthetic retrieval across both supported protocol revisions.
The DNS test uses a local silent resolver, not an external DNS service.
Cargo audit scanned 326 dependencies against 1,243 RustSec advisories without a
vulnerability report. Cargo deny passed advisories, bans, licenses, and sources;
only the documented existing duplicate-version warnings remain.

With Node 24.21.0 and pnpm 12.3.4, widget type checking, three model tests, four
Chromium bridge tests, the bounded single-file build, and dependency checks passed:

```sh
pnpm --dir apps/widget typecheck
pnpm --dir apps/widget test
pnpm --dir apps/widget build
pnpm --dir apps/widget test:browser
pnpm --dir apps/widget check:dependencies
```

The dependency check reported no known npm advisories and admitted the licenses
of all 30 installed packages. The generated HTML was 829,781 bytes, below 1 MiB;
generated assets and browser output are ignored rather than committed.

Workflow lint, shell syntax/lint, Python syntax, TOML parsing, Markdown fences and
relative file links, and `git diff --check` passed. Hosted Actions were not watched.

The isolated [OxiBelt gate](oxibelt.md) used the actual widget bundle and clean
edge revision `72564d165dfd05cb29a64aeebd19fccd7944ea6f`. Both revisions
(`2026-07-28` and `2025-11-25`) were exercised over HTTP and WebTransport, including
all three demo tools, bounded progress, and resource retrieval. Existing origin,
path, authority, certificate-rejection and reconnect checks remain included.

```sh
DEMO_WIDGET_HTML=apps/widget/dist/index.html scripts/test-oxibelt.sh
```

The final assertion-only rerun reused the same identity-checked local edge binary
via `OXIBELT_BINARY=target/oxibelt/debug/oxibelt`. Disposable containers, network,
certificate volume, and harness image were cleaned up. Finite local tests do not
establish production load capacity or hosted-client behavior.

## Snapshot identification

Final source snapshot SHA-256: `961177cee09f766da198b8e2626789954fb691f7f33375d7bede9130e21edcca`.
This digest identifies repository content; the reviewed scopes are listed above.
Reproduce it from the completed checkout, excluding this document to avoid
self-reference and excluding ignored generated artifacts:

```python
from pathlib import Path
import hashlib
import subprocess

paths = sorted(set(subprocess.check_output(
    ["git", "ls-files", "--cached", "--others", "--exclude-standard"],
    text=True,
).splitlines()) - {"docs/retrieval-review.md"})
digest = hashlib.sha256()
for name in paths:
    digest.update(name.encode() + b"\0" + Path(name).read_bytes() + b"\0")
print(digest.hexdigest())
```
