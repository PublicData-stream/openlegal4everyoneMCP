# Contributing to openlegal4everyoneMCP

This repository develops **openlegal4everyone.stream**, open legal-information
infrastructure with a planned Rust backend. Legal-data integrity and responsible
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

At this bootstrap there is no Cargo workspace, Rust source, test suite, or CI.
Use the documentation checks below now; Rust commands are future requirements.
Do not add scaffolding, dependencies, live API requests, or repository settings
merely because they appear in the proposed architecture.

Use repository-relative paths in durable descriptions. Commands run from the
repository root unless a different directory is explicitly stated. Keep local
session aliases, absolute host paths, credentials, and disposable artifacts out
of commit messages and PR evidence.

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

### Future Rust baseline

After the workspace, lockfile, toolchain, and dependency policy exist, the baseline
checks are:

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
cargo audit
cargo deny check
```

These are not runnable project checks at this governance bootstrap. `cargo audit`
and `cargo deny` are separately installed tools; bootstrap must document their
versions/setup and advisory-data access. Distinguish an unavailable/stale advisory
database from a successful current advisory check.

Use focused tests while developing, then run the applicable workspace baseline
before a Rust change is ready. Bootstrap must enumerate supported feature
combinations and add explicit checks for them. Use `--all-features` only when all
features are intended to coexist. Preserve doctest coverage; selecting all targets
alone is not a substitute for running documentation tests.

CI introduced with Rust must match these documented checks and test supported
feature combinations on a declared toolchain/platform baseline. Run advisory checks
on a schedule as well as relevant changes. Failures, cancellations, and unexpected
skips must not be reported as successful validation. Make checks reproducible from
a clean checkout and keep untrusted PR jobs free of production credentials and
privileged publication actions. CI or check-policy changes update this section.

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

The current repository has no runtime attack surface. Apply these requirements
when implementing or changing the planned boundaries; do not describe hypothetical
authentication, tenancy, or deployments as existing features.

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
copying unrelated obligations. The repository's [Apache-2.0 license](LICENSE)
remains unchanged; upstream data reuse is handled in provider profiles.
