# Korean tokenization implementation review

## Result and evidence boundary

Reviewed on 2026-09-19 against base `47dd725aa38275e533b7fafb26cecf4eaac01cc0`.
The core changes are recorded in signed commits `15edfe5` and `cd7c8ba`.
The corpus query executor now retains separate Lindera and MeCab-Ko analyses,
combines positive results within each engine, and shares exclusions across engines.
Exact quoted matching still reads the original text. The operator provisions a
full dictionary and rebuilds incompatible derived indexes offline. The behavior
and operator procedures are owned by [the corpus contract](database.md).

The published crates.io MeCab-Ko 0.7.2 sources, builder, and full dictionary were
inspected and exercised locally. No legal-provider calls, production deployment,
live ChatGPT acceptance, or legal-search accuracy evaluation was performed.

## Independent review

Reviewer `docs_gates` was a separate agent, independent of the core implementation.
It inspected the actual patch and surrounding code for MCP/API behavior,
dictionary/resource security, and derived-index/retention correctness, following
the repository [delegation skill](../.agents/skills/subagent-delegation/SKILL.md).
This is agent review evidence, not a human GitHub approval or repository-wide scan.

Two findings were fixed and independently re-reviewed:

- The upstream tokenizer constructs all prefixes of some unknown-character runs
  before returning. The previous 64 KiB line bound alone permitted excessive
  lattice allocation. Pre-analysis admission now also limits normalized lines to
  4096 Unicode scalars and whitespace-delimited runs to 128 scalars. These bounds
  use the same whitespace boundaries tracked by the pinned engine, precede both
  engines, reject rather than split input, and are included in analyzer identity.
- Index publication and rebuild analysis previously created fresh per-field
  deadlines/cancellation tokens. They now pass one caller deadline and token
  through both fields, old/new indexed documents, and the precommit check.

The final review found no blocking findings. It covered paired Boolean evaluation,
field/prefix/source-exact matching, compiled query session ownership, blocking-work
leases, full-dictionary admission, exact entry counts, metadata compatibility,
monotonic acknowledgments, durable retirement replay, and process exclusion. The
reviewer also checked the exact-version `notify` license admission independently.

The reviewed source fingerprint is
`48fea0298517223c4f9bf9f8b73db6964122025e221e9af6a3e6e1337579538a`.
It is SHA-256 of each repository-relative UTF-8 path, NUL, file bytes, NUL,
concatenated in this order:

```text
crates/adapters/src/korean_analysis.rs
crates/adapters/src/korean_dictionary.rs
crates/adapters/src/korean_query.rs
crates/adapters/src/search_index.rs
crates/adapters/src/corpus_search.rs
crates/adapters/src/corpus/runtime_lease.rs
apps/server/src/corpus_runtime.rs
apps/server/src/main.rs
crates/adapters/examples/provision_korean_dictionary.rs
scripts/prepare-korean-dictionary.sh
deny.toml
```

## Local validation

All commands ran from the repository root. Cargo used the declared Rust 1.98.1
baseline and locked dependency resolution where applicable.

| Check | Result |
| --- | --- |
| `cargo fmt --all -- --check` | Passed |
| `cargo clippy --workspace --all-targets --locked -- -D warnings` | Passed |
| `cargo test --workspace --locked` | 202 passed; 45 explicit integration tests ignored |
| `cargo audit` | Passed with the pre-existing documented lru advisory exception |
| `cargo deny check` | Advisory, license, source and dependency policies passed; duplicate-version warnings remain |
| `scripts/prepare-korean-dictionary.sh NEW_DIRECTORY` | Full source build and admission passed, 816283 entries |
| `scripts/test-korean-tokenization.sh` with provisioned artifact | Full-engine test passed, including segmentation disagreement and maximum-run regression |
| `scripts/test-postgres.sh` with provisioned artifact | 45 passed, none ignored; both new rebuild tests passed |
| `DEMO_WIDGET_HTML=apps/widget/dist/index.html TEXT_DIFF_WIDGET_HTML=apps/widget/dist/text-diff.html scripts/test-oxibelt.sh` | HTTP and native WebTransport gates passed for both protocol revisions |
| `actionlint .github/workflows/ci.yml`, changed shell scripts' `bash -n`, `git diff --check` | Passed |

Set `OPENLEGAL_TEST_MECAB_DICTIONARY` to the provisioned directory to reuse it in
the full-dictionary and PostgreSQL gates. Omit the variable to have those scripts
provision disposable artifacts. The source SHA-256 is
`702ced21c6167e9d9aebc674ab5ee54af58d4443975f2940d37d0567c020591a`;
the built canonical manifest digest is
`48b1d12acea6aefebd0a58cca2b8368a353bdc2c1dfa22645fe69dff6e64636d`.

The final debug fixture measurement processed 1000 fictional lines in 84.3 ms for
Lindera alone and 198.6 ms for combined analysis. Four-slot startup took 10.05 s.
The process reported about 1015 MiB resident memory and 1079 MiB peak memory after
combined analysis, versus about 20 MiB for the initial Lindera baseline. Two
128-character unknown runs separated by whitespace took 244 ms. These are single
local measurements in a test process, not performance thresholds or accuracy claims.

## Remaining limits

Engine calls run in-process and cannot be preempted mid-call. Admission bounds and
finite slots constrain work; they are not an OS-enforced RSS limit. Each tokenizer
owns an eager dictionary copy. The dictionary directory is trusted operator input
and must remain immutable while loading; hashes detect accidental changes, not
the authority of dictionary content.

Old binaries do not participate in the new database advisory lease and must be
stopped before rebuilding. Lease health checks are not transaction-level fencing
against an administrator forcibly terminating its database connection. Corpus
evidence is retained; rollback availability depends on index/acknowledgment
compatibility as documented in the corpus contract. Hosted CI was not watched.
