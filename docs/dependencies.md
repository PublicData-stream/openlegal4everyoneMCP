# Rust dependency admission

Baseline: Rust 1.98.1, edition 2024, Linux GNU on x86_64 (x86-64-v3) and ARM64
(generic CPU). The separate document-worker graph remains x86_64-only.
There is one supported
production feature configuration: both transports are compiled together with
the features in `apps/server/Cargo.toml`. No optional first-party features exist.
Commit `Cargo.lock` and use locked resolution for builds and tests. Dependencies
come from crates.io; unreviewed Git dependencies and registries fail `cargo deny`.

The architecture-scoped compiler and rustdoc flags in `.cargo/config.toml` select
x86-64-v3 for all repository x86_64 Rust builds, including the server image and
document worker. ARM64 receives no x86-specific flag. `deny.toml` checks both
supported server target graphs; the worker retains its separate admission policy.
See [the contributor baseline](../CONTRIBUTING.md#rust-baseline) for host support
and Cargo environment-variable precedence.

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

## Rustls security maintenance (2026-09-15)

The workspace lockfile updates the single shared Rustls package from 0.23.44 to
0.23.45 for [RUSTSEC-2026-0285](https://rustsec.org/advisories/RUSTSEC-2026-0285).
The affected version could accept a complete plaintext handshake message following
a key-changing message within the same TLS record. The upstream patch corrects
handshake alignment checks. The advisory does not establish handshake forgery or
a certificate-verification bypass.

This is the smallest patched release accepted by existing dependency constraints;
no other package version or feature changes. Source remains crates.io, MSRV remains
Rust 1.71, and the license remains Apache-2.0 OR ISC OR MIT. Manifests, enabled
features, dependencies, and build scripts are otherwise unchanged. The release
also zeroizes consumed private-key DER and tightens HelloRetryRequest validation;
the existing verified TLS and QUIC workflows require compatibility checks.

The package is shared by wtransport/Quinn, reqwest, and SQLx. The workspace enables
both ring and AWS-LC; the retained in-memory TLS regression exercises both
providers, a complete and a fragmented malicious handshake, and a valid handshake
with application data. Existing native transport, OxiBelt, and PostgreSQL TLS gates
cover their integration. The separately pinned OxiBelt edge build has its own
dependency graph; this lockfile update does not upgrade that project. See the
[maintenance review](rustls-review.md) for reproduction and validation evidence.

## License and build policy

The initial resolved graph uses MIT, Apache-2.0, BSD-2-Clause, BSD-3-Clause, ISC,
MIT-0, Unicode-3.0 and Zlib licenses (or expressions satisfiable by these choices).
These remain the default third-party allowlist in `deny.toml`; the exact corpus
search exceptions are documented below. The five first-party workspace packages use
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

## PostgreSQL and blob persistence additions

`sqlx` 0.9.0 supplies one async PostgreSQL stack: Tokio runtime, Rustls with
ring and native certificate roots, PostgreSQL, UUID, JSON, and migrations; default features are disabled.
Runtime parameterized queries avoid a build-time database and query-macro metadata.
Checked-in SQL is embedded with `include_str!`; migrations use SQLx's migration
ledger/checksums and locking. The alternative `tokio-postgres` plus a separate
pool/migration stack would add composition without a present capability benefit.
SQLx is MIT OR Apache-2.0, from crates.io. Its PostgreSQL framing, authentication,
TLS, pooling and cancellation are security boundaries. Native certificate roots reuse the existing workspace trust-store stack rather
than introducing the differently licensed bundled webpki root dataset. Explicit
operator CA files remain supported. Exact transitive versions are in Cargo.lock; inactive optional SQLx drivers may appear in the lockfile but
are not compiled into the admitted PostgreSQL feature graph. No MySQL or SQLite
persistent backend is implemented.

`uuid` 1 is used for typed database UUID decoding/encoding only (std, no generator
features). PostgreSQL 18 generates relational IDs through native `uuidv7()`;
cryptographic public aliases/comparison handles continue to use `getrandom`.
The UUID crate is MIT OR Apache-2.0, from crates.io.

`rustix` 1.1.4 remains the safe Linux filesystem/process boundary for immutable
blob directory-relative nofollow operations, ownership checks and no-replace
publication. Its MIT OR Apache-2.0 syscall implementation may contain dependency
unsafe code; first-party unsafe remains denied. File and parent-directory sync,
including concurrent deduplication, receive independent review and deterministic
failure tests. There is no filesystem database, worker protocol, manifest index,
or format migration remaining. Blocking object jobs retain bounded admission
through actual completion; SQL owns references and retention.

Admission evidence must include current cargo-audit and cargo-deny results,
features/source review, real PostgreSQL 18 tests, and focused independent reviews.
See [persistence](persistence.md) for credentials, verified TLS, bounded pooling,
and the explicit distinction between digest integrity and source authentication.

## Corpus search admission (2026-09-16)

- **Tantivy 0.26.2** provides immutable local index readers, atomic commits and
  rebuildable storage. Only mmap and LZ4 compression features are enabled; no
  external search service or caller-provided index path is introduced. Corpus
  publication remains PostgreSQL-owned. Source: crates.io, MIT. Writer buffers,
  document serialization, scans and retained reader sessions have separate bounds;
  a writer buffer is not a total process-memory limit.
- **Lindera 5.0.1**, with locked dictionary/ko-dic 5.3.0, provides Korean morphology.
  Only the embedded Korean dictionary feature is requested. Search surfaces use
  NFC and ASCII lowercase without stopwords; legal bodies remain unchanged.
  Source: crates.io, MIT. Its build helper fetches the named
  `mecab-ko-dic-2.1.1-20180720.tar.gz` artifact and verifies the upstream-configured
  MD5 `b996764e91c96bc89dc32ea208514a96`. This is a build-time network dependency,
  not a runtime dictionary download. Dictionary redistribution needs the original
  Apache-2.0 notices. Dictionary/version changes require a new analyzer version
  and complete index rebuild.
- **grep-regex 0.1.14 / grep-matcher 0.1.8** are ripgrep's Rust regex/matcher
  libraries, allowing typed, bounded matching without interpreting command-line
  switches or spawning shell commands. Source: crates.io, MIT OR Unlicense;
  select MIT. Regex automata and per-call scans have independent limits.
- **unicode-normalization 0.1.25** implements NFC for analyzed search surfaces;
  exact quoted matching and original text use the unmodified representation.
  Source: crates.io, MIT OR Apache-2.0. Hand-written Unicode composition is not an
  appropriate alternative.

### MeCab-Ko alongside Lindera (2026-09-19)

The Rust [hephaex/mecab-ko](https://github.com/hephaex/mecab-ko) engine is admitted
alongside Lindera, using the exact crates.io `mecab-ko =0.7.2` release and
`mecab-ko-dict-builder =0.7.2` for provisioning. Runtime `mecab-ko` and
`mecab-ko-dict` disable default features; the builder is a development dependency
used by the provisioning example, outside the serving dependency graph.
These published release sources
must not be confused with later repository commits carrying the same version.
Source: crates.io; MIT OR Apache-2.0. This is the Rust reimplementation, not C++
MeCab FFI. Retaining Lindera preserves a separate analysis stream; neither engine
is treated as a semantic authority for legal text.

The builder consumes Lindera's exact `mecab-ko-dic-2.1.1-20180720.tar.gz` archive,
SHA-256 `702ced21c6167e9d9aebc674ab5ee54af58d4443975f2940d37d0567c020591a`.
Redistribution preserves its Apache-2.0 notices. Provisioning validates source
encoding and CSV records, entry counts, trie mappings, matrix dimensions and
context IDs, and emits release-compatible uncompressed artifacts and a manifest.
Runtime accepts only a verified full dictionary at the configured explicit path,
with eager loading. Default discovery, miniature fallback and empty-entry behavior
are unsuitable for corpus search. No runtime downloads are introduced.

A four-slot pool bounds mutable tokenizers, with one owned dictionary per slot.
This costs four dictionary copies; compare memory and throughput with Lindera using
`scripts/test-korean-tokenization.sh`. In addition to the existing byte, aggregate-token and serialized-index bounds,
normalized lines admit at most 4096 Unicode scalars and whitespace-delimited runs
at most 128 scalars. The pinned engine generates all unknown-word prefix candidates
for some character classes; byte limits alone do not bound that expansion adequately.
Oversized inputs fail capacity admission without changing their segmentation.
Synthetic measurements cover accepted input sizes; they are not an absolute RSS
or wall-clock guarantee for in-process native allocation. Dictionary parsers, lattice allocation,
blocking-work cancellation and admission require focused independent review.
Passing synthetic fixtures does not establish improved legal-search accuracy.
Analyzer/dictionary changes invalidate index compatibility and require the
[offline rebuild](database.md#offline-index-rebuild).

### Narrow dependency exceptions

Tantivy 0.26.2 depends on lru 0.16.4, affected by
[RUSTSEC-2026-0253](https://rustsec.org/advisories/RUSTSEC-2026-0253.html).
The advisory requires `pop` to unwind through a panicking key destructor. Source
and reverse-dependency review found one consumer: Tantivy's
`store/reader.rs::BlockCache`, an `LruCache<usize, Block>` using new/get/put/len.
It does not call `pop`; `usize` has no destructor. This is an unreachable
precondition in the selected graph, not a fix to lru itself. `deny.toml` records the
specific advisory exception. Owner: PiQuark6046; review by **2026-10-16**, on any
Tantivy/lru update, or if another consumer appears. Independent review:
`text_review`, actual dependency graph and source, 2026-09-16. Prefer an upstream
Tantivy release using fixed lru >=0.18.2 when available.

`webpki-roots 1.0.9`, used by Lindera's build/download support, adds
CDLA-Permissive-2.0 licensed root-certificate data. The exception is scoped to that
exact package/version; preserve its accompanying license when redistributing the
data. Owner/review deadline: PiQuark6046, 2026-10-16. Other licenses retain their
existing admission policy. No blanket license or advisory suppression is added.


`notify 6.1.1` is an unconditional crates.io dependency of `mecab-ko-dict 0.7.2`,
even with default features disabled. Its CC0-1.0 license is admitted only for this
exact package/version in `deny.toml`; preserve the upstream license and attribution
material when redistributing. The serving adapter does not construct file watchers
or invoke hot-reload APIs. Removing the dependency requires an upstream graph change,
not enabling a different first-party feature. Owner: PiQuark6046; review by
**2026-12-19**, or on MeCab-Ko/notify changes. This is a narrow license admission,
not a general CC0 allowlist or an advisory exception.
