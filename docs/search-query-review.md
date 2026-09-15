# Search-query processor implementation review

Review date: 2026-09-15. Base revision:
`313fc03a4052189b7f2a355d5adca3a08c4b7dfe`.

Independent reviewer: Codex agent `parser_security_review`, separate from the
parser implementer and the `query_integration` test implementer. Category:
Security Review of untrusted parsing, resource limits, source spans, diagnostics,
and preservation of existing query/history boundaries. No blocking security or
correctness findings remained in the reviewed scope.

This is agent review, not a human GitHub approval or a repository-wide audit.
The implementation and its standalone boundary are documented in the
[syntax contract](search-query.md).

## Reviewed scope

The reviewer inspected the following exact contents and their relevant surrounding
contracts. This evidence document is excluded from its own source fingerprint.
Reproduce individual hashes with `sha256sum` on these repository-relative paths:

```text
4d6e7b79271bd1194d0755aa15932cc801365e572052d532f29a320314f04019  crates/domain/src/search_query.rs
212c9c23aa962a1db882a31244e801401c87f96b7d7d2b2f4233482459db06cc  crates/domain/src/lib.rs
320692ee8f74fc8bdda040b016510c93d8fd331e4b581d6f747e37fc3b1aa2c2  crates/normalization/src/search_query.rs
e6aa51175043e800dbef0e1c2422625b2416b3fc76a53b4ab7efd173509245a6  crates/normalization/src/lib.rs
c11f7847d2e08e37659812ad6aeb6081809310890cda61e47d0ea5841aba6c03  crates/normalization/tests/search_query.rs
4a8358221fdc38495f161003a57f166c662e01b2605a4a726d0b2d0a01cf2c03  docs/search-query.md
5450ab7612284279927c5879e57aa15a9223b945c97cc87158b00b2b03c34a1f  docs/architecture.md
b56a6fa69047dd863d080e0ada660c61d9232258e695773af7aa78f294db455c  README.md
```

## Review conclusions

- Input bytes are capped before allocation. Scanner operations advance over valid
  UTF-8 boundaries or return an error, and controls are rejected globally.
- Recursive descent checks nesting before entry; associative Boolean chains use
  iteration. Every successful expression tree obeys the node cap.
- A single-quoted token may temporarily hold more words than the expression-node
  limit before admission; that temporary storage remains bounded by input bytes.
- Field configuration is bounded and validated before copying. Nested scope state
  propagates through negation, groups, and Boolean expressions.
- Error formatting contains category and offsets only. Original source is retained
  in successful results without normalization.
- Existing `Query::validate`, cache identity, and historical callers are unchanged.

## Validation

The parent ran these checks successfully with the final Rustls 0.23.45 lockfile:

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
cargo audit
cargo deny check
git diff --check
```

The workspace run passed 171 tests including doctests, with 32 explicitly ignored
PostgreSQL tests. The separate PostgreSQL gate executed all 32 successfully as part
of the [Rustls maintenance validation](rustls-review.md). Dependency admission
passed advisories, bans, licenses, and sources; existing duplicate-version warnings
remain warnings. Initial advisory checks failed on pre-existing Rustls 0.23.44;
the separately authorized maintenance patch corrected that dependency before final
validation. No advisory exception was added.

The test implementer, parent, and independent reviewer also ran focused checks:

```sh
cargo test -p openlegal-normalization --test search_query --locked
cargo test -p openlegal-normalization --lib --locked
cargo test -p openlegal-normalization --doc --locked
```

All 19 query integration tests pass. They include independent expected expression
shapes, quote/field/negation semantics, Unicode source spans, escaping, malformed
syntax, configuration bounds, and exact input/node/nesting limits. Generated tests
include 280 Unicode cases, 48 nested valid expressions, sampled escaped Unicode
scalars, and 160 mixed-syntax inputs. One intermediate generated test expectation
was corrected to use explicit parentheses, preserving the intended grouping rather
than assuming a nested shape for a flat OR chain. No parser behavior was weakened.

## Remaining limits

Generated regression cases are not exhaustive fuzzing. The processor has no search
executor, provider integration, wire schema, or widget. Future execution requires
explicit analyzer and cache/history compatibility decisions. Native fixture checks
do not establish deployed behavior or live ChatGPT compatibility.
