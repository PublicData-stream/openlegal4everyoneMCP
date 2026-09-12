# AGPL migration validation

Validation date: 2026-09-12. Base revision:
`adef9b6cb994bdaf47b2fc57b286b2bbe26d4517`.

## Result and compatibility

First-party code and documentation now use `AGPL-3.0-only`. The complete root
license was downloaded from the [GNU AGPLv3 text](https://www.gnu.org/licenses/agpl-3.0.txt)
and retained byte-for-byte. SHA-256:
`0d96a4ff68ad6d4b6f1f30f713b18d5184912ba8dd389f86aa7710db079abcb0`.
The README supplies the explicit version-3-only grant and project copyright;
the license appendix remains an unmodified example, not a later-version grant.
Each Cargo package includes the same license through a relative symlink. The
widget embeds its complete text and retains inline dependency legal comments.

Every server configuration now requires `[source].url`. It advertises the same
validated corresponding-source URL in legacy initialization, modern discovery,
the existing `server_info` tool, and the rendered widget. Library constructors
require an explicit `SourceOffer`. Rebuild the widget when upgrading because
startup requires the new inert source metadata placeholder. See the
[migration contract](server.md#source-offers-and-migration).

The maintainer confirmed authority to relicense all first-party content, including
OxiBelt-adapted documentation. Read-only GitHub API requests and the GitHub connector
corroborated repository control, seven signed project commits by `piquark6046`,
one listed contributor, and the PiQuark6046 copyright in the
[referenced OxiBelt license](https://github.com/OxiBelt/OxiBelt/blob/72564d165dfd05cb29a64aeebd19fccd7944ea6f/LICENSE).
These are authorship/provenance records, not independent proof of legal ownership.
Earlier Apache-2.0 grants, third-party licenses and legal-data reuse conditions
are not replaced by this migration.

## Independent review

An independent Codex reviewer, a separate agent that did not implement the patch,
completed the MCP/API Boundary and Security review. Scope: every changed tracked
file against the base revision, the two new widget source/test files, and the five
package license symlinks. No unresolved findings remained in the reviewed code.

The review covered required startup validation and output budgets, shared
discovery/tool metadata, inert HTML insertion and resource bounds, host-mediated
navigation and fallback, package licensing, dependency admission and provenance.
It identified a server/widget backslash-validation mismatch; server rejection and
regression cases resolved it. Startup now also checks the source-bearing tool
result against the configured budget. The reviewer personally inspected the code,
ran whitespace and license-hash checks, and checked ordinary URL parsing behavior;
the implementation agents and parent ran the suites below.

Reviewed patch identity before adding this evidence document:

- `git diff --binary HEAD | sha256sum`:
  `41689ef88a68e918c37128b96145442a0c02e120336c24c02b56e12f005f4d56`.
- `apps/widget/src/source-offer.ts` SHA-256:
  `3865d438994ec2ba18a2bf7b35b54a400de483b093311c65f931be4fce7b02e2`.
- `apps/widget/tests/source-offer.test.ts` SHA-256:
  `fe311f34598c827935eb8f858e02a2067d8e15025ce823d636e4748f99110625`.
- The five new package `LICENSE` symlinks target `../../LICENSE`; their resolved
  bytes have the root license digest above.

This is independent technical review, not a human GitHub approval, a full security
scan, or a legal opinion. The source URL is public operator metadata. Validation
does not fetch it or prove source availability, completeness or correspondence.

## Checks

Passed with the repository's pinned toolchains and privileged Cargo/pnpm access:

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
cargo build --workspace --bins --examples --locked
cargo audit
cargo deny check
cargo deny check --hide-inclusion-graph
pnpm --dir apps/widget install --frozen-lockfile
pnpm --dir apps/widget typecheck
pnpm --dir apps/widget test
pnpm --dir apps/widget build
pnpm --dir apps/widget exec playwright install --with-deps chromium
pnpm --dir apps/widget test:browser
pnpm --dir apps/widget check:dependencies
git diff --check
bash -n scripts/test-oxibelt.sh
```

All 71 Rust tests, five crate doctest targets (zero doctests), five widget unit
tests and 11 Chromium tests passed. Source-offer cases include invalid/missing
configuration, normalization and byte bounds, incompatible response budgets,
missing/duplicate/misplaced HTML markers, escaping and post-insertion limits,
both revisions/transports, visible full license text, no automatic navigation,
and successful/unsupported/disconnected/refused/failed host navigation.

The widget was 866,827 bytes before operator URL substitution, contained exactly
one source marker and retained dependency MIT comments. Current Rust advisory
checks scanned 326 dependencies against 1,243 advisories without vulnerabilities;
cargo-deny passed with the existing duplicate-version warnings. The widget audit
found no known vulnerabilities, and its license gate accepted 30 dependencies.
Neither lockfile changed.

`cargo metadata --no-deps --locked --format-version 1` confirmed all five packages
inherit `AGPL-3.0-only`. `cargo package --list --allow-dirty --locked -p PACKAGE`
confirmed each includes `LICENSE`; their resolved files match the root text.
`cargo package -p openlegal-domain --no-verify --allow-dirty --locked` additionally
confirmed its archive contains the full license as a regular file, not a dangling
symlink. No package was published. Python parsed the HTTP smoke script without
writing bytecode, changed TOML files parsed, and 42 relative documentation links
and anchors passed checks.

The final OxiBelt gate passed using rootless Docker and the built widget:

```sh
OXIBELT_BINARY=target/oxibelt/debug/oxibelt \
  DEMO_WIDGET_HTML=apps/widget/dist/index.html scripts/test-oxibelt.sh
```

The harness checked the cached edge binary's reported clean pinned build identity
`72564d165dfd05cb29a64aeebd19fccd7944ea6f`. Both MCP revisions passed over HTTP and
WebTransport, including discovery/source metadata, synthetic tool calls, progress,
widget resource delivery, Origin/authority/path checks and TLS rejection cases.
HTTP smoke assertions also require the injected fixture source URL and absence of
the unresolved marker. The native reference client checks source metadata agrees
with discovery/initialization instructions. Disposable Docker resources were
removed on successful completion. Omit `OXIBELT_BINARY` to have the harness build
the pinned source when no cached binary is available. The build-identity check
reads the binary's version report; it is not a cryptographic build attestation.

No source archive hosting, deployment, live ChatGPT connection, GitHub publication
or live legal-provider test is performed. Operators must replace example source
URLs and maintain free corresponding source for their actual server and widget,
including modifications and required build/install/run material.
