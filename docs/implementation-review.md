# Server foundation validation and independent review

Validation date: 2026-09-11. Base revision:
`f4e69de533c352d6918e605effa6ba1643786586`.

## Result and evidence

The foundation implements startup tool/endpoint registration, a shared bounded
MCP handler, Streamable HTTP and native WebTransport for revisions `2026-07-28`
and `2025-11-25`. Both data transports are required by the binary. Configuration,
protocol limitations and extension responsibilities are in [the server contract](server.md).
No legal-data provider, public deployment, browser test or live ChatGPT connection
is claimed.

The following commands passed on Rust 1.98.1, Linux x86_64 GNU:

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
cargo build --workspace --examples --bins --locked
cargo audit
cargo deny check --hide-inclusion-graph
actionlint .github/workflows/ci.yml
bash -n scripts/test-oxibelt.sh
shellcheck scripts/test-oxibelt.sh
git diff --check
```

All 28 tests passed: five library tests, one binary logging test, six extension
tests, four HTTP tests and twelve native WebTransport tests. Cargo also executed
the doctest target, which currently contains zero doctests. TOML parsing, Python
AST parsing, Markdown fences and relative file links were checked successfully.

Cargo audit fetched the RustSec database and reported no vulnerable dependency.
Cargo deny passed advisories, bans, licenses and sources. Its only warnings were
duplicate transitive versions of base64, getrandom, syn and winnow, explained in
[dependency admission](dependencies.md); there are no advisory suppression entries.

The final isolated Docker gate passed through clean OxiBelt source revision
`72564d165dfd05cb29a64aeebd19fccd7944ea6f`. It exercised both revisions over both
transports, native tool calls, Origin/path/authority rejection, certificate trust
failures and reconnect after restoring trust. The [portable harness](oxibelt.md)
documents source pinning, setup and cleanup. All disposable run containers,
network, certificate volume and image were removed. Hosted GitHub Actions runs
were not watched or represented as completed validation.

Final tested native executable SHA-256 values (local build evidence, not release artifacts):

- Server: `9c5c7e87497f538341310951efc0524b178bb409ac2306367b701930213470f8`
- Reference client: `31ad3703ffcd39d5f53fbac45c0adfe4dbcc62ee4beec9db4a6df3ece328794a`
- Pinned OxiBelt: `5f21c91157ac96a60da9f8045174f48284c1547318fcd3fe9f9d39befd4be95e`

## Independent review

A separate Codex agent, assigned exclusively to read-only independent review,
reviewed the actual implementation and surrounding contracts for MCP/API Boundary
and Security, including dependency admission, CI and the OxiBelt harness. The
reviewer independently ran all 28 Rust tests. It did not rerun Docker or advisory
checks; those outcomes above are implementation-team evidence.

Review corrections addressed HTTP producer versus socket deadlines, independent
response deadlines during flow-control stalls, joined WebTransport teardown,
typed sanitized tool errors, output preflight, startup access checks, continued
supervisor draining after errors, bounded unsupported-version negotiation, and
mixed-lifecycle rejection. Focused re-review found no unresolved findings in scope.
This is agent technical review, not a human GitHub approval or a comprehensive
transitive dependency audit.

The final reviewed content digest is
`362e514889950d5f70e94dd2142b45eb28e7aad14dbd87a363dff7acb3936d8a`.
Reproduce it from the repository root; this evidence document is deliberately
outside the digest to avoid a self-reference:

```python
from pathlib import Path
import hashlib

paths = sorted([
    *Path("apps/server").rglob("*.rs"),
    Path("apps/server/Cargo.toml"), Path("Cargo.toml"), Path("Cargo.lock"),
    Path("deny.toml"), Path("rust-toolchain.toml"),
    Path(".github/workflows/ci.yml"), Path("docs/dependencies.md"),
    Path("docs/server.md"), Path("docs/architecture.md"), Path("AGENTS.md"),
    Path("CONTRIBUTING.md"), Path("README.md"), Path("SECURITY.md"),
    Path("docs/oxibelt.md"), *Path("deploy/oxibelt").glob("*"),
    Path("scripts/test-oxibelt.sh"), Path("scripts/http_smoke.py"),
], key=str)
digest = hashlib.sha256()
for path in paths:
    digest.update(str(path).encode() + b"\0" + path.read_bytes() + b"\0")
print(digest.hexdigest())
```

Trusted modules must still bound their own allocations and avoid detached work.
Forced shutdown returns failure rather than claiming successful cleanup of SDK
internals. Native interoperability and these finite regression tests do not
establish browser support, load capacity, service availability or operational
production readiness. The security-reporting channel remains explicitly pending
verification in [SECURITY.md](../SECURITY.md).
