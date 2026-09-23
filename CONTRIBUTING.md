# Contributing to openlegal4everyoneMCP

This repository develops **openlegal4everyone.stream**, open legal-information
infrastructure with a Rust MCP server foundation. Legal-data integrity and responsible
upstream access are engineering requirements, not just documentation concerns.

## Policy ownership

This document is authoritative for contributor workflow, testing, review,
documentation, commit messages, and secure-development requirements. The linked
documents own their technical contracts; follow them when the change affects that
area. Agent summaries and templates must point to these rules rather than create
alternate versions.

| Canonical document | Owns |
| --- | --- |
| This document | Contributor duties, required checks, secure development, review gates, and readiness |
| [Architecture](docs/architecture.md) | Responsibilities, dependency direction, and side-effect boundaries |
| [Legal-data policy](docs/legal-data-policy.md) | Identity, dates, normalization, provenance, citations, and evidence |
| [Upstream policy](docs/upstream-policy.md) | Cache semantics, freshness, refresh, coalescing, and request budgets |
| [Provider profiles](docs/providers/kr-law-go-kr.md) | Evidence-backed provider-specific mappings, capabilities, and unresolved items |
| [Security policy](SECURITY.md) | Private reporting, disclosure, and development-stage security support |
| [AGENTS.md](AGENTS.md) | Agent orientation and navigation |
| [Delegation skill](.agents/skills/subagent-delegation/SKILL.md) | Assignment, supervision, escalation, and synthesis of delegated work |

Resolve conflicts by correcting the canonical owner and its references. If the
intended technical behavior is uncertain, surface it for a maintainer decision;
do not silently pick whichever policy permits the change. Repository policies do
not override higher-priority instructions, user scope, or execution permissions.

## Contribution workflow

1. Inspect the checkout and identify affected responsibilities and contracts.
2. Make the smallest coherent change that achieves the intended behavior. Explain
   architectural or public-contract changes before spreading them across layers.
3. Add meaningful tests for changed behavior and preserve source evidence for
   legal-data changes. Update relevant documentation and examples.
4. Run applicable checks and report their exact commands and outcomes, including
   unavailable, failed, or skipped checks and why.
5. Obtain required independent review and address findings before marking the
   change ready. Inspect the integrated diff, including new files and fixtures.

The Rust server foundation now lives in `apps/server`, with a committed workspace,
lockfile, tests and CI. Retrieval/cache services now exercise synthetic sources;
the legal corpus and LAW OPEN DATA adapter are opt-in and tested with offline fixtures.
Do not make live provider requests merely because an adapter exists. Live acceptance
and hardened document-worker deployment remain separate explicit gates.

Use repository-relative paths in durable descriptions. Commands run from the
repository root unless a different directory is explicitly stated. Keep local
session aliases, absolute host paths, credentials, and disposable artifacts out
of commit messages and PR evidence.

First-party contributions intentionally submitted for inclusion are licensed under
the repository's [GNU AGPL version 3 only](LICENSE) (`AGPL-3.0-only`). Submit only
material you have authority to contribute under these terms. Preserve separately
licensed third-party notices and record meaningful provenance.

## Commit messages

Use lightweight Conventional Commits:

```text
<type>(optional-scope): <concise subject>
```

Use `feat`, `fix`, `docs`, `test`, `refactor`, `perf`, `build`, `ci`, `chore`, or
`revert`. A scope, when useful, names the affected responsibility. Describe a
security fix using the appropriate type without disclosing private findings.

```text
docs(governance): define legal-data review requirements
fix(normalization): preserve unknown effective dates
test(cache): cover concurrent refresh cancellation
```

Subjects should explain the change clearly. No special tense policing or mandatory
backticks around every identifier are required. Explain important behavior and
compatibility context in the body or PR, using portable evidence.

## Rust engineering

When Rust is introduced, follow the [architecture](docs/architecture.md).
Use responsibility-focused modules; do not put unrelated helpers in `utils` or
`common` merely because they are shared. Review responsibility and dependencies
when a module grows rather than enforcing an arbitrary line-count limit.

Document public contracts and non-obvious invariants. Use typed errors and explicit
handling for invalid input, absent data, ambiguity, upstream failure, and partial
results. Avoid input-triggered `unwrap`, `expect`, `panic!`, `todo!`, or unjustified
`unreachable!` on externally reachable paths. Do not suppress errors to make a
result appear complete.

Choose the Rust toolchain/MSRV, dependency versions, supported feature combinations,
and lint configuration during backend bootstrap and track them in the repository.
Workspace lints must be inherited by member packages; declare equivalent policy
for any later standalone test/probe packages. Commit the application workspace
lockfile and use locked dependency resolution in checks.

New or materially changed dependencies need a short explanation of need,
alternatives, source, relevant features/build scripts, maintenance, and license
considerations. Explain security implications for boundary-critical dependencies.
Run admission checks for manifest, lockfile, or dependency-policy changes.
Exceptions must identify the exact affected dependency/advisory or rule, rationale,
owner, and review deadline; avoid blanket or indefinite suppression. Introduce a
reviewed `deny.toml` with the dependency baseline, not an invented allowlist now.

## Testing and CI

### Documentation-only changes

Check Markdown rendering, relative links and anchors, examples, terminology,
current/planned labels, policy ownership, and template usability. Validate skill
frontmatter and instructions when a skill changes. Run:

```sh
git diff --check
git status --short
```

`git diff` does not include untracked file contents: inspect newly added documents
as well. Do not create Rust tests that merely assert policy wording or headings.

### Rust baseline

The development baseline is Rust 1.98.1 on Linux GNU, with x86-64-v3 on x86_64 and the
generic Rust CPU baseline on ARM64. Both transports compile together with no
optional first-party features. Native CI runs the Rust baseline on both
`ubuntu-26.04` and `ubuntu-26.04-arm`. The separate document worker remains
x86_64-only and also requires x86-64-v3. Production images use Alpine 3.24
and dynamically linked musl builds with the same CPU baselines; image-local
linker flags must preserve these architecture flags. Dependency admission covers
GNU development and musl production targets. Required checks are:

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
cargo audit
cargo deny check
```

The repository's `.cargo/config.toml` applies `target-cpu=x86-64-v3` to x86_64
Rust builds and rustdoc/doctests without changing native artifact paths. Check
the build and execution host's CPU support before running x86_64 binaries; on
GNU Linux, `/lib64/ld-linux-x86-64.so.2 --help` must list
`x86-64-v3 (supported, searched)`. An incompatible host is unsupported; compilation
success alone does not establish execution compatibility. Cargo environment
overrides such as `RUSTFLAGS`, `CARGO_ENCODED_RUSTFLAGS`, `RUSTDOCFLAGS` and
`CARGO_ENCODED_RUSTDOCFLAGS` take precedence over the repository flags, including
when set to an empty value. Clear these overrides for baseline checks, or retain
the CPU flag when adding flags. See [Cargo configuration precedence](https://doc.rust-lang.org/cargo/reference/config.html#buildrustflags).

Install `cargo-audit` 0.22.2 and `cargo-deny` 0.20.2 using the locked commands in
[dependency admission](docs/dependencies.md). Both require current advisory-data
access; unavailable/stale advisory data is not a successful current check.
The separate `scripts/test-oxibelt.sh` Docker gate verifies both transports through
the pinned edge and runs in CI. Edge, NodePort handoff, and OxiBelt harness changes
require both `scripts/test-oxibelt.sh --profile fixture` and
`scripts/test-oxibelt.sh --profile kubernetes`; the default remains `fixture`.
The Kubernetes profile exercises the committed edge example with a Docker backend
listening on the NodePort numbers; it does not validate Kubernetes routing or
firewall enforcement. CI runs both profiles. `scripts/test-postgres.sh` provisions
the pinned PostgreSQL 18 image and explicitly executes the ignored real-database persistence,
history and transport tests. Both gates are required for persistence changes and
run in CI. An unavailable Docker/database environment is an incomplete gate, not a
passed or silently skipped check. Ordinary Rust tests and compilation require
neither Docker nor `DATABASE_URL`; tests requiring PostgreSQL are clearly marked
and run by the dedicated gate.

Korean corpus analysis, search semantics, dictionary provisioning and analyzer/index
compatibility changes also require `scripts/test-korean-tokenization.sh`. This gate
builds and validates the full pinned standard dictionary and runs the dual-engine
integration cases and synthetic resource measurements. It runs in CI. Both it and
the PostgreSQL gate provision a disposable dictionary unless
`OPENLEGAL_TEST_MECAB_DICTIONARY` names an existing provisioned artifact. To reuse
a downloaded source archive, set `MECAB_SOURCE_ARCHIVE`; its digest is still checked.
The explicit provisioning step may download the pinned dictionary source; tests do
not contact legal providers. Ordinary unit tests need no external MeCab-Ko artifact.
A missing full dictionary or failed provisioning is an incomplete gate, never a
silently skipped or successful check. See [corpus setup](docs/database.md) for
artifact compatibility, index rebuild and rollback requirements.

The PostgreSQL gate uses a disposable bridge with dynamically allocated
localhost-only database ports on host runners; those fixture containers can make
outbound connections. Inside a Docker development container, it instead joins the
daemon-visible runner to a disposable internal network and uses database IPs without
publishing ports, preserving the runner's existing network connections. Both modes
remove their fixture containers, volumes and network on exit. A devcontainer run
does not by itself validate the host runner's port-publication path.

Use focused tests while developing, then run the applicable workspace baseline
before a Rust change is ready. Bootstrap must enumerate supported feature
combinations and add explicit checks for them. Use `--all-features` only when all
features are intended to coexist. Preserve doctest coverage; selecting all targets
alone is not a substitute for running documentation tests.

Changes to document processing also require `scripts/test-document-worker.sh`.
This Docker gate validates the standalone worker's locked native dependency graph,
formatting, linting and tests, then exercises fictional XML/HTML/PDF/HWP5/HWPX/OCR
fixtures with the worker's seccomp and resource limits. It runs in CI, including
the scheduled advisory checks. It does not establish Kubernetes, gVisor or
AppArmor acceptance; the separately configured real-cluster gate and its deployment
prerequisites are documented in [document processing](docs/document-sandbox.md).
The dedicated hosted worker job removes unused preinstalled Android/.NET SDKs and
requires at least 20 GiB free build space; local runs must provision that space
without deleting unrelated host data.

Changes to Alpine package installation also require
`scripts/test-alpine-packages.sh`. Its isolated signed HTTPS repository tests
bounded retries, timeouts, strict metadata handling and integrity failures with
the base image's APK. CI requires this gate before the worker and server image jobs;
see [dependency admission](docs/dependencies.md) for prerequisites and limits.

Changes to the production server image, its build inputs or packaged widgets
also require `scripts/test-server-image.sh --platform linux/amd64` and
`scripts/test-server-image.sh --platform linux/arm64`. Each invocation builds and
tests one architecture; omitting `--platform` selects the host architecture.
Native image jobs run on both hosted runners. Local ARM64 emulation is useful
evidence but does not replace native ARM64 CI acceptance. The gate uses disposable
rendered text-only and retained-corpus configurations, disposable certificates and
an internal Docker network to verify image contents, read-only execution, health,
MCP text comparison, retained search, graceful restart and storage identity. It
provisions disposable PostgreSQL 18 with verified TLS, separate migration/runtime
roles, isolated volumes and the full pinned dictionary. It requires no production
or provider credentials and publishes no host ports. Dictionary provisioning may
download the pinned source; `MECAB_SOURCE_ARCHIVE` or
`OPENLEGAL_TEST_MECAB_DICTIONARY` permits explicit verified reuse. Missing dictionary,
startup timeout and OOM are failures, not successful negative cases. Allow space
for native build caches and image layers; the hosted job reports available disk space. Its
clean-build peak has not been measured on hosted ARM64, so the worker's separate
20 GiB requirement is not imposed on the server job.
Unavailable Docker, architecture support or build inputs leave this gate incomplete.
Image builds do not publish images. See [server deployment](docs/deployment-kubernetes.md).

The optional ingestion runtime additionally requires the same two image gates with
`--target runtime-ingestion`; the default `--target runtime` stays minimal. The
ingestion gate checks the packaged pinned kubectl against an isolated synthetic TLS
API, including explicit credentials, token-file replacement, discovery and Pod
creation. It never starts enabled ingestion against a legal provider. This fixture
does not establish Kubernetes authorization, projected-token delivery, CNI or
sandbox enforcement. Controller authentication/environment or ingestion deployment
changes also require `scripts/test-document-worker.sh` and independent Security
Review. Real-cluster acceptance remains a separate explicitly configured gate.

Serving-manifest and deployment-validation changes require explicit tool provisioning
with `scripts/setup-deployment-tools.sh`, then `scripts/test-kubernetes-serving.sh`.
The gate uses pinned kubectl/Kustomize, hash-locked PyYAML with Python 3.11–3.14,
and checksum-verified kubeconform with local Kubernetes 1.36.0 and 1.37.0 schemas.
It checks inventoried source files before rendering, rendered project invariants,
and both strict schema versions without cluster access or validation-time downloads.
Missing, modified or incomplete schema assets fail; rerun explicit setup after
removing only the invalid schema-validation bundle. Schema checks do not establish
API-server admission or cluster enforcement. A dedicated CI job runs the same gate;
all four native image jobs provision these tools and consume both rendered profiles.
Every invocation validates retained and text-only profiles; `--profile` selects
which configuration is exported by `--config-output`, defaulting to retained.
Every invocation also validates all three suspended
administrative Job roots, shared configuration/image identity, and separately
applied Local PV/PVC examples, including fresh rebuild storage. Network
checks cover the namespace default deny, each standalone allow example,
all supported database/DNS/monitoring combinations and actual workload selectors.
Every invocation additionally renders the opt-in ingestion overlay, its separate
cross-namespace RoleBinding root and API/provider allow examples, and rejects
credential leakage into retained serving/admin, broad RBAC, token subPath mounts,
configuration drift and unscoped egress. The gate also checks the unchanged document namespace denial, quota and prepared-node
runtime invariants. These are committed-template checks, not admission of arbitrary
operator overlays or proof of CNI enforcement. Administrative
command or mount changes require the PostgreSQL gate and both image platforms;
the image gate exercises cache maintenance, fresh index rebuild and interruption
recovery with disposable storage. Run both image platforms when changing the
serving configuration, container contract or image smoke harness. Retained deployment/storage changes
also require the Rust baseline, PostgreSQL, Korean tokenization and OxiBelt gates.
Source inventory additions must identify their validation owner and local render
coverage; new resource kinds need reviewed schemas for both versions. Raw source
checks reject high-confidence credential patterns, including in comments, but do
not constitute a general secret scanner. Keep failures free of source excerpts and
credential values. Deployment-tool updates require the four image target/platform
checks and independent Security Review of the affected validation boundary.
Run `shellcheck` on changed shell scripts and `actionlint` when the workflow changes. Do not describe these checks as real-cluster acceptance.

Serving smoke client changes require
`python3 -m unittest discover -s scripts/tests -p 'test_serving_smoke.py'`,
`cargo test --locked -p openlegal-server --example wt_client serving_smoke`,
and both OxiBelt profiles. Disposable fixture preparation changes also require
`target/deployment-tools/bin/python -m unittest discover -s test-support/kubernetes-acceptance -p 'test_*.py'`.
The OxiBelt gate
executes the bounded deployed-endpoint profile against its synthetic fixture.
The Python checks also run in the deployment CI job. Explicit-endpoint smoke
is an operator action, not a default CI connection to a deployed service.
Disposable Kubernetes serving acceptance is separately opt-in; its setup, evidence
requirements and cleanup are in [deployment acceptance](docs/deployment-kubernetes.md#phase-9-serving-acceptance).
Credential files, raw workload dumps and unsanitized test output are never
acceptance evidence for publication. Apply independent Security and MCP/API
Boundary review to changes in smoke endpoint, parser and reporting boundaries.

CI introduced with Rust must match these documented checks and test supported
feature combinations on a declared toolchain/platform baseline. Run advisory checks
on a schedule as well as relevant changes. Failures, cancellations, and unexpected
skips must not be reported as successful validation. Make checks reproducible from
a clean checkout and keep untrusted PR jobs free of production credentials and
privileged publication actions. CI or check-policy changes update this section.

### React MCP Apps widget

Node 24.21.0 and pnpm 12.3.4 are the frontend baseline. For widget changes, run:

```sh
pnpm --dir apps/widget install --frozen-lockfile
pnpm --dir apps/widget typecheck
pnpm --dir apps/widget test
pnpm --dir apps/widget build
pnpm --dir apps/widget exec playwright install --with-deps chromium
pnpm --dir apps/widget test:browser
pnpm --dir apps/widget check:dependencies
```

The last command requires current npm advisory data and checks dependency licenses.
Build output and browser artifacts are ignored. The local browser harness exercises
the real MCP Apps bridge against synthetic responses; it is not a live ChatGPT test.
For integrated widget/transport changes, set `DEMO_WIDGET_HTML=apps/widget/dist/index.html`
and `TEXT_DIFF_WIDGET_HTML=apps/widget/dist/text-diff.html` when running the
OxiBelt gate. Ordinary Rust tests remain independent of Node.

### Behavior and integration tests

Test changed behavior and failure modes, not just implementation structure.
Legal-data changes need expected results grounded in provider evidence or clearly
labeled synthetic scenarios. Cache changes need observable upstream request counts,
key separation, controlled time, and failure/cancellation behavior.

Default tests use deterministic fixtures, mock upstreams, isolated temporary
storage, and controlled clocks. They must not contact live legal-data APIs.
Live integration checks require explicit opt-in, appropriate approved access,
bounded requests, and documented setup; keep them outside routine PR gates.
Do not use public upstreams for load testing.

Place tests according to [Architecture](docs/architecture.md#tests-and-fixtures).
Generate disposable data in temporary directories and clean it up, including on
failure where practical. Do not commit caches, credentials, logs, build outputs,
or bulk legal-data downloads. Commit only intentionally curated fixtures under the
[fixture policy](docs/legal-data-policy.md#retained-evidence-and-fixtures).

Do not remove tests or weaken assertions merely to obtain a passing run. Explain
intentional test retirement or changed expectations. Treat a flaky failure as
unresolved until evidence establishes its cause or bounded impact.

## Documentation and compatibility

Update README for setup, status, and discoverability; put detailed contracts in
their owning policy or technical document. Keep commands and examples consistent
with the implementation. Documentation-only work should not claim planned behavior
is implemented or deployed.

Changes to MCP tools, future HTTP responses, identifiers, citations, normalization,
cache schemas, or configuration assess callers and persisted data. Explain
invalidation/reprocessing, migration or rollback needs, and any compatibility
limitations. No release ledger or deployment workflow is required before those
capabilities exist.

## Secure development

The implemented HTTP and WebTransport boundaries accept untrusted input. Apply
these requirements to their changes and to future providers; authentication, tenancy,
and live deployment remain unimplemented unless explicitly introduced.

| Boundary | Required consideration |
| --- | --- |
| MCP parameters and future public HTTP input | Validate type, shape, length, selectors, pagination, and allowed operations before expensive work; return bounded results/errors. |
| Outbound HTTP | Construct requests from approved provider configuration and typed selectors. Do not accept arbitrary caller-supplied fetch URLs. Restrict schemes, origins, ports, paths, and redirects as appropriate to the provider. |
| SSRF and source-supplied links | Apply destination policy to every followed link/redirect and connection, including DNS resolution and private/local destinations. Treat returned URLs as untrusted. Use explicit isolated configuration for local mock servers. |
| Transport integrity | Preserve certificate validation and avoid silent downgrade to insecure transport. An upstream guide's HTTP sample is not permission to disable security checks. |
| Parsers and decompression | Bound compressed/decompressed bytes, nesting, expansion, work, pagination, and elapsed time. Disable external entity/resource loading for applicable formats; do not execute embedded content. |
| Cache publication and concurrency | Follow the canonical key, identity, validation, coalescing, and resource-accounting contracts in the upstream policy; assess poisoning and stale/partial-write races. |
| Secrets and logs | Keep credentials out of source, fixtures, errors, citations, metrics, and logs. Redact query-string authentication values as well as headers. Avoid routine raw payload or legal-search logging. |
| Future authentication/authorization | Define access checks and cache/coalescing isolation with the feature; public data does not make admin actions or credentials public. |

For a security-sensitive change, identify the affected boundary, attacker-controlled
inputs, failure behavior, and compatibility impact. Add focused regression evidence
and document residual uncertainty. Fail safely on security-boundary validation or
authorization failures. The explicitly bounded stale-data policy is an availability
decision, not permission to bypass identity or security validation.

Suspected vulnerabilities follow [SECURITY.md](SECURITY.md), including the pending
private-channel setup requirement. Review affected callers and related operations
as needed; a full-repository scan is not an automatic gate for every change.

### Unsafe Rust

First-party Rust denies unsafe code by default. At Rust bootstrap, set workspace
`unsafe_code = "deny"` and `unsafe_op_in_unsafe_fn = "deny"`, with lint inheritance
for member packages. Require safety documentation for public unsafe contracts and
operation-specific `SAFETY` comments for any admitted unsafe blocks. First-party
policy is distinct from unsafe inside dependencies, which needs proportionate
dependency review.

An exception requires a demonstrated need that an appropriate safe implementation
cannot meet, narrow module-level scope, and an independently reviewed safe wrapper.
Document caller obligations, ownership/lifetimes, layout or platform assumptions,
concurrency, and failure behavior where relevant. Keep exceptions explicitly
enumerated here when first introduced; there are currently none. Do not silently
lower lints elsewhere or expose an unchecked unsafe operation through a safe API.

Include focused tests and appropriate Miri, sanitizer, fuzz, or platform evidence
for the actual safety model. Record unavailable or inapplicable tooling and residual
uncertainty; ordinary tests are not proof of freedom from undefined behavior.
Exception changes and changes to admitted unsafe code require independent review.

## Review gates

High-risk changes require a reviewer other than the implementer to inspect the
actual patch, relevant surrounding contracts, and evidence before readiness.
The reviewer may be a separate human or agent. Record identity, reviewed revision
or exact patch scope, conclusions, and unresolved items in the PR. Material changes
after review require renewed review of the affected behavior.

Apply the categories that fit the actual change:

| Review category | High-risk triggers |
| --- | --- |
| Legal Data Integrity Review | Changes to identity/equivalence, legal date meaning, revision selection, substantive normalization, or historical/source citation association |
| Upstream/Cache Correctness Review | Key identity/isolation, freshness classification, coalescing/cancellation, shared budgets, refresh publication, or persistent format changes |
| MCP/API Boundary Review | Changes that cross input/trust boundaries, alter public identity/freshness/error semantics, or bypass shared application policy |
| Security Review | Outbound destination controls, untrusted parser/resource boundaries, credentials, later access control, unsafe Rust, or security-critical dependency behavior |

One independent reviewer may cover several categories when capable; four separate
reviews are not required. A cosmetic edit mentioning one of these topics is not
automatically high-risk. Conversely, a small edit changing an invariant can be.
Changes to these gates or their technical invariants need the same relevant review.

If required review is unavailable, leave the change awaiting review and say so.
Passing tests or self-review does not waive the requirement. Agent evidence is not
a fabricated human GitHub approval, and review does not authorize merging,
publishing, deployment, or live-service actions outside the task's scope.

## Pull request readiness

Use the [PR template](.github/pull_request_template.md), removing inapplicable
sections. A ready change explains the problem/result, records applicable checks,
updates owning documentation, and includes required independent review. Identify
unresolved failures and risks rather than presenting them as completed checks.

For legal-data investigation, distinguish code-established fact, authoritative
upstream evidence, inference, and unresolved uncertainty. Include source locations
and revision/access context sufficient to verify consequential claims. A successful
compile, passing tests, or provider documentation alone does not prove legal
correctness or a successful live integration.

## OxiBelt provenance

This project's maintainer also develops OxiBelt. The governance here adapts its
orientation/requirements split, responsibility boundaries, and bounded delegation
principles, with project-specific text and legal-data contracts.

Reference snapshot: `72564d165dfd05cb29a64aeebd19fccd7944ea6f`:

- [OxiBelt AGENTS.md](https://github.com/OxiBelt/OxiBelt/blob/72564d165dfd05cb29a64aeebd19fccd7944ea6f/AGENTS.md)
- [OxiBelt CONTRIBUTING.md](https://github.com/OxiBelt/OxiBelt/blob/72564d165dfd05cb29a64aeebd19fccd7944ea6f/CONTRIBUTING.md)
- [OxiBelt delegation skill](https://github.com/OxiBelt/OxiBelt/blob/72564d165dfd05cb29a64aeebd19fccd7944ea6f/.agents/skills/subagent-delegation/SKILL.md)

This is an adaptation, not a synchronized import. OxiBelt's proxy/WAF, TLS
termination, HTTP/3, Person Proof, deployment, release, directory, and model-alias
policies do not apply here. Later reuse should record meaningful provenance without
copying unrelated obligations. The maintainer authorized relicensing the adapted
first-party documentation under the repository's [GNU AGPL version 3 only](LICENSE)
as part of the September 12, 2026 migration. This does not change OxiBelt's license
or the referenced historical snapshot; upstream data reuse is handled in provider
profiles.
