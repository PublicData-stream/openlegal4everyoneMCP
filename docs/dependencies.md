# Rust dependency admission

Baseline: Rust 1.98.1, edition 2024, Linux x86_64 GNU. There is one supported
production feature configuration: both transports are compiled together with
the features in `apps/server/Cargo.toml`. No optional first-party features exist.
Commit `Cargo.lock` and use locked resolution for builds and tests. Dependencies
come from crates.io; unreviewed Git dependencies and registries fail `cargo deny`.

## Direct dependencies and alternatives

| Dependency | Purpose, choice and boundary considerations |
| --- | --- |
| rmcp 3.3.0 | Official MCP Rust SDK; handles both chosen revisions and HTTP protocol translation. Prefer its maintained protocol implementation over a new JSON-RPC stack. Public input reaches this code; host-level admission, strict Origin checks and bounded framing supplement its defaults. Disable its payload logging in the binary. Apache-2.0. |
| wtransport 0.7.2 | Native WebTransport endpoint over Quinn/Rustls. Enables `ring` and `quinn` for TLS and explicit QUIC bounds; default self-signed and dangerous-configuration features remain off. Alternative lower-level H3/Quinn wiring would duplicate session mechanics. Upstream still cautions about production readiness; the pinned OxiBelt interoperability gate and independent boundary review are required, and broader deployment validation remains operator work. MIT OR Apache-2.0. |
| Axum, Hyper, hyper-util | HTTP routing and serving. Hyper's connection APIs are used explicitly for header, stream and connection limits; the socket wrapper adds write deadlines. Axum-only defaults do not provide all required limits. MIT. |
| Tokio, tokio-util, futures | Shared asynchronous runtime, cancellation, supervised tasks and stream adapters; mixing another executor would complicate task lifetimes. The text-diff adapter enables Tokio process spawning with bounded pipes and supervised cancellation/reaping. MIT / Apache-2.0 alternatives in their manifests. |
| Serde, serde_json, Schemars | Typed parameters, bounded serialization and schemas; handwritten schema copies risk drift. Derive macros execute only at build time. JSON nesting uses the parser's finite default recursion limit. MIT OR Apache-2.0. |
| jsonschema 0.56 | Validate complete input constraints beyond Rust field types. HTTP/file reference resolution and TLS features are disabled; schemas are compiled once from trusted modules, never fetched from callers. Invalid/unresolved schemas fail startup. A limited handwritten validator would misrepresent JSON Schema support. MIT. |
| http | HTTP vocabulary shared with the SDK stack; avoids incompatible representations and extra protocol conversions. MIT. |
| TOML, url | Strict operator configuration and origin parsing. The URL parser normalizes origin tuples; raw string/prefix matching would be incorrect. MIT OR Apache-2.0. |
| tracing, tracing-subscriber | Operational events and filtering; SDK payload logs are disabled regardless of the normal runtime log setting. MIT. |
| rcgen, tempfile, reqwest test features | Isolated TLS fixtures, cleanup and native HTTP tests. rcgen and tempfile remain development-only. Certificate generation uses ring; reqwest disables default features and uses Rustls. Production reqwest admission is described below. No production test certificates or live provider traffic. MIT / Apache-2.0 alternatives. |

The listed versions are baseline anchors; exact versions and checksums for all
dependencies live in the lockfile. Source review covered relevant installed SDK
HTTP/lifecycle/framing code and wtransport's TLS/QUIC APIs. Updating a boundary
dependency requires renewed relevant review and transport tests, not only a build.

## Retrieval and widget additions

- **reqwest 0.13** is now also a production adapter dependency with only `rustls`
  and `stream`. It supplies bounded streamed HTTP bodies instead of a second
  handwritten HTTP client. Automatic retries, redirects, environment proxies and
  compression are disabled. Configured HTTPS hosts are pinned to previously checked
  addresses, retaining TLS hostname/certificate verification. Source: crates.io;
  MIT OR Apache-2.0. Existing native tests retain their development client features.
- **Hickory resolver 0.26.2**, with locked net/proto 0.26.3, replaces blocking
  libc DNS work that would outlive an aborted lookup. Only Tokio and system-config
  features are enabled; encrypted DNS, DNSSEC and mDNS are not requested. The async
  resolver reads operator DNS configuration at startup; request timeout, attempts,
  active queries and cache size are bounded. Every returned IP is checked before
  HTTP pinning. Alternative OS lookup could leave uninterruptible blocking work.
  Sources: crates.io and the project's [resolver documentation](https://docs.rs/hickory-resolver/0.26.2/hickory_resolver/).
  MIT OR Apache-2.0; new transitive cache/platform dependencies receive current
  license/advisory admission checks. Isolated silent-resolver cancellation/deadline
  tests supplement destination-policy tests for this security-critical boundary.
- **sha2 0.11** computes evidence digests with RustCrypto, reusing the version
  already required by WebTransport instead of introducing another cryptographic
  implementation. Digests provide linkage, not legal authenticity. Default CPU
  dispatch may select platform implementations; first-party unsafe remains denied.
  MIT OR Apache-2.0, crates.io.
- **httpdate 1** parses HTTP-date `Retry-After` guidance. Integer and date delays
  are preserved without shortening the provider floor; unrepresentable cooldowns
  pause the provider. It already existed transitively. MIT OR Apache-2.0, crates.io.

Workspace path dependencies specify version `0.1.0`; no wildcard-policy exception
is needed. React, TypeScript, esbuild, the MCP Apps SDK and Playwright use exact npm
versions and a committed pnpm lockfile. The [widget admission rationale](../apps/widget/README.md#dependency-admission)
documents alternatives, licenses, enabled build scripts and boundary implications.
Only esbuild's installation script is enabled. The production resource bundles
dependencies locally; it loads no CDN assets. Node 24.21.0/pnpm 12.3.4 are the
declared frontend baseline, with browser and current advisory/license checks in CI.

## License and build policy

The initial resolved graph uses MIT, Apache-2.0, BSD-2-Clause, BSD-3-Clause, ISC,
MIT-0, Unicode-3.0 and Zlib licenses (or expressions satisfiable by these choices).
These remain the explicit third-party allowlist in `deny.toml`; no advisory
exceptions are installed. The five first-party workspace packages use
`AGPL-3.0-only`, admitted through exact package/version exceptions rather than a
global AGPL allowance. Owner: PiQuark6046; review by 2026-12-12 and on package
version changes. The exceptions implement the project's licensing choice and do
not authorize new third-party AGPL dependencies. Alternative-license expressions
do not require accepting every offered license. Preserve license notices when
distributing dependencies; the widget build retains inline dependency legal
comments and embeds the full first-party license.

Each Rust package has a relative `LICENSE` symlink to the central license text.
Cargo includes the referenced bytes as a regular file in source packages, so the
license is delivered without maintaining divergent copies. Preserve these links
when adding or moving workspace members.

First-party code denies unsafe Rust. Dependencies include unsafe implementation
code, especially Tokio/socket layers and ring. Ring builds native C/assembly via
its build script and needs a C compiler; this is an admitted cryptographic backend,
not an exception permitting first-party unsafe. Procedural derives and ordinary
platform/probing build scripts execute during Cargo builds. This is a package
admission review, not a claim that every transitive line has been audited.

Multiple transitive major versions are warnings, not blanket hidden exceptions:
the initial graph includes base64 (SDK versus TLS helpers), getrandom (ring,
schema hashing and current runtime dependencies), syn (derive ecosystems), and
winnow (TOML parser dependencies). Check their callers when updates change the
graph; do not force semver-incompatible replacements merely to erase a warning.

## Required check tooling

```sh
cargo install cargo-audit --version 0.22.2 --locked
cargo install cargo-deny --version 0.20.2 --locked
cargo audit
cargo deny check
```

Both advisory checks require access to the current RustSec advisory database.
Unavailable or stale advisory data is not a passing current-advisory check.
CI performs these on pushes, pull requests and a weekly schedule. Check commands
and readiness requirements remain owned by [Contributing](../CONTRIBUTING.md).

## Text comparison additions

`similar` 3.2.0 (Apache-2.0, crates.io) replaces the system Git subprocess as the
line and character comparison implementation. It is pinned exactly with default
features disabled and only `std` and `text` enabled: no extra runtime dependencies,
build script, Unicode segmentation, inline word refinement or WASM integration are
requested. Its MSRV is Rust 1.85, below this workspace baseline. The maintained
upstream release was published on 2026-08-17. Source and API:
[similar 3.2.0](https://docs.rs/similar/3.2.0/similar/).

Git requires an external installation and temporary inputs; libgit2 would add a
native library, while handwritten diff algorithms would duplicate a maintained
implementation. The Rust worker uses LF-delimited slices and scalar `diff_chars`
to preserve this product's exactness contract. Parent process deadlines kill and
reap the worker rather than using the library's deadline approximation. Pinned
source review, Unicode fixtures, resource tests and independent MCP/API/Security
review supplement license and current advisory checks for this boundary dependency.

`getrandom` supplies operating-system randomness for 256-bit comparison handles;
failure prevents publication. Tokio's `process`, `fs` and `io-util` features support
executable validation, bounded pipes and process reaping. `tempfile` is now used
only in development fixtures. No first-party unsafe exception is introduced.

The comparison widget uses a local React text-node renderer of Rust-computed
scalar ranges. The previous `@git-diff-view/react`, syntax-highlighting graph and
associated exact BSD license exceptions are removed. Remaining third-party
notices are bundled locally; the source-offer marker and existing asset ceilings
remain enforced. The frozen graph receives current advisory/license checks, and
only esbuild's installation script remains enabled.
