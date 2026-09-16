# MCP corpus and text endpoint review

This review covers the implementation following base commit
`e6f7125cb9a624cf2b4dd726f87c53a282f662e7`: the text attachment/patch lifecycle,
legal domain and migration `0002_legal_corpus.sql`, corpus persistence and provider
adapter, retained search/read services, server integration, widgets, and isolated
document processing. The enclosing commit identifies the reviewed patch.

## Result and compatibility

The server exposes the eight requested canonical tools through its existing MCP
transports. Existing text-comparison names remain available. Text attachments are
temporary bearer resources; patch application returns a new result and cannot
write caller-selected filesystem paths. Database tools are explicitly configured
and operate on a managed PostgreSQL/blob corpus. They distinguish provider
revisions, retained captures, current freshness and unavailable history.

The additive corpus migration shares the existing migration ledger, with separate
corpus tables and evidence directories. Apply migrations with the migration
credential before enabling the module. Disabling `[database]` disables serving and
ingestion of the corpus; it does not erase retained evidence. No down migration or
conversion of historical legal captures into the synthetic cache is supplied.
An older binary can reject a migration ledger containing newer migrations, so
rollback requires a compatible binary or an operator-controlled database restore.

Search uses bounded traversal of stored Tantivy documents and Lindera tokens;
this implementation does not claim inverted-index query acceleration. Generation
sessions, cursors and scan budgets bound each request. Observed provider coverage
requires matching inventory traversals, available desired HEADs and an aligned
index; it is not an upstream atomic-snapshot guarantee.

## Independent review

Separate Codex agents inspected actual changes and affected callers under the
repository delegation skill. These are engineering reviews, not human GitHub
approvals or deployment authorization.

- `text_review` reviewed the text MCP/worker boundary, strict patch handling,
  result accounting, cancellation and attachment delivery. A completed worker
  result could survive an abandoned receiver; delivery now has an ownership guard
  that reclaims undelivered attachments, with a cancellation/drop regression test.
- `text_tools` independently reviewed corpus identity/history, persistence,
  provider mappings, search and serving integration. Follow-up review covered
  queue-capacity publication fencing, separate catalog versions, stable inventory
  checks, raw/staging accounting, withdrawal/pin races, source retry classification,
  bounded index admission and conservative coverage claims. Reported findings were
  corrected and reviewed again.
- `text_review` reviewed document controller framing and cleanup, quota/RBAC,
  manifests and syscall restrictions. Network and root-write probes now require
  actual policy-denial errors; PID probes require positive work before exhaustion.
  The worker uses a timer-only Tokio runtime so startup needs no socket exception.
  Native fixture failures exposed absent generic-font fallbacks and Xberg's
  dictionary-filtered Markdown OCR default. Explicit pinned-font mappings and
  derived plaintext OCR bypass dictionary/table rewriting; OCR remains a separate
  representation with provenance rather than authoritative legal text.
  Independent source review also covered the exact dependency exceptions recorded
  in [dependency admission](dependencies.md) and
  [document processing](document-sandbox.md).

The legal provider tests use fictional offline fixtures. Provider documentation
supports the chosen mappings but is not evidence of a successful authenticated
live request, full provider coverage, or legal equivalence.

## Verification

Executed locally on Rust 1.98.1 with locked dependencies:

- `cargo fmt --all -- --check`
- `cargo clippy --workspace --all-targets --locked -- -D warnings`
- `cargo test --workspace --locked`, including doctests
- `scripts/test-postgres.sh`: 42 real PostgreSQL tests passed, including nine
  corpus tests and the database MCP integration fixture.
- `DEMO_WIDGET_HTML=apps/widget/dist/index.html TEXT_DIFF_WIDGET_HTML=apps/widget/dist/text-diff.html scripts/test-oxibelt.sh`:
  both MCP revisions over HTTP and WebTransport passed.
- Widget frozen installation, typecheck, build, 30 model tests, 39 browser tests,
  and current dependency advisory/license checks passed.
- `cargo audit` and `cargo deny check`: admission passed with the documented
  exact, expiring `lru` advisory exception; no claim of a warning-free graph.
- `actionlint .github/workflows/ci.yml` and `git diff --check` passed.
- `scripts/test-document-worker.sh`: fresh standalone admission, formatting,
  all-target native Clippy, three XML/HTML unit tests, release-image build and seven
  confined native-format/OCR fixtures passed. Final worker framing/digest and
  local confinement probes also passed.

Native worker fixture details and exact dependency exceptions are recorded in
[document processing](document-sandbox.md).

No live legal-provider calls, hosted workflow observation, image publication or
deployment were performed. The real Kubernetes/gVisor/AppArmor acceptance gate
requires an explicitly configured cluster and remains an operator prerequisite.
