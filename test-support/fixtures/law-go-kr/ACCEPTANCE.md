# Permanent LAW clone validation

Validation date: 2026-10-04. This document records implementation validation,
bounded provider observations and the remaining deployment gates separately.
It does not assert that an operational corpus has finished cloning.

## Implemented behavior

- LAW defaults to five evenly spaced admission attempts per second and four
  simultaneous attempts. PostgreSQL coordinates the rate, fair admission and
  attempt ownership across processes; every retry is charged. The operator can
  change `database.ingestion.provider_requests.requests_per_second` and
  `max_in_flight`. Continuous daily collection defaults to `"unlimited"`.
- Canonical inventories cover 69 source families and documented history views.
  The registry accounts for 195 public guides, including aliases, supplements,
  query-only endpoints and explicitly unresolved contracts.
- Original observations, captures, historical revisions and correction captures
  are permanent. The optional `database.max_raw_bytes` cap defaults to
  `"unlimited"`; capacity exhaustion stops new writes without evicting evidence.
  Temporary storage, individual responses, decoded records and worker limits
  remain bounded. Physically deleted evidence from an earlier deployment cannot
  be recovered by the migration.
- Supplemental work has durable descriptors, fenced leases, resumable pagination
  and daily revalidation. Corpus status separates canonical and supplemental
  completion and keeps unresolved identities, missing bodies and guide contracts
  visible as incomplete coverage.
- Unknown source/attachment rights permit metadata only. Verified ND resources
  retain their original bytes without extraction. NC conditions are disclosed.
  Bundled primary transport XML is private evidence when embedded supplementary
  material has unresolved rights.
- Documented XML link fields can echo the active `OC` credential. Only that
  authentication query value is redacted, with explicit evidence metadata; body
  and opaque original bytes are never rewritten. Valid XML end-tag whitespace
  is preserved. Credential reflection elsewhere is withheld.

## Offline and native tests

The GNU Rust workspace is compiled locally; Docker and native PostgreSQL
execution run on the separate authorized dev host to avoid local network-range
conflicts. The dev source directory matches compile-time fixture paths. Transfer
manifests verify every test executable SHA-256 before execution. This is separate
compilation and native execution evidence, not a dev Cargo build or a production
musl image gate. The execution host was checked for GNU runtime and x86-64-v3
compatibility.

Commands run from the repository root:

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked --offline -- -D warnings
cargo test --workspace --locked --offline
cargo audit --no-fetch
cargo deny check
npm --prefix apps/widget test
npm --prefix apps/widget run typecheck
npm --prefix apps/widget run build
npm --prefix apps/widget run test:browser -- tests/database.spec.ts
scripts/test-postgres.sh --prebuilt-directory "$TEST_ARTIFACT_DIRECTORY"
scripts/test-oxibelt.sh --profile fixture
scripts/test-oxibelt.sh --profile kubernetes
scripts/test-kubernetes-serving.sh
scripts/test-document-worker.sh
```

Prebuilt PostgreSQL execution requires the full verified dictionary through
`OPENLEGAL_TEST_MECAB_DICTIONARY`, the companion server executable at its original
compile-time `CARGO_BIN_EXE_openlegal-server` path, and matching fixture sources.
OxiBelt execution uses the separately compiled server/client/mock binaries and
the verified pinned clean OxiBelt 0.10.0 binary. Neither gate uses a LAW token.

The final Rust workspace has 420 passing tests and 130 explicitly ignored
database/dictionary cases. Fmt, clippy and dependency admission passed; audit
retains the already reviewed baseline `RUSTSEC-2026-0253` warning for `lru`.
All 130 ignored PostgreSQL/dictionary cases then passed on dev, including object
progress registration, permanent history rebuild, parallel admission and both
database transports. Widget unit tests, type checking, build and all four database browser tests passed.
Both dev OxiBelt profiles passed HTTP and native WebTransport checks. Offline
Kubernetes validation passed 81 tests and all 54 resources against each of
Kubernetes 1.36.0 and 1.37.0. This is template validation, not cluster admission.

The standalone document-worker gate also passed on dev. Unlike the separately
compiled GNU workspace tests, this gate built and tested the current source in
the native Alpine/musl Docker toolchain. Worker formatting, dependency admission
and clippy passed, followed by three library tests and seven document-format
tests (two cases filtered by the existing fixture target). The development worker
image passed framed XML/HTML processing, inert-script handling, PID confinement
and empty-log checks. The transient test service exited successfully with status
zero and left no running test containers. This proves the dev worker image gate;
it does not establish a published release or production activation.

## Authorized provider probe

The explicitly invoked `law_clone_acceptance` example reads the credential file
at runtime, uses a pristine loopback PostgreSQL database and the hardened cached
document-worker image, and prints only bounded status/shape summaries. An optional
closed `--dataset` selector permits one-family diagnosis without repeating the
whole matrix. First-page inventories and one representative detail per family
do not establish full traversal, attachment availability or corpus publication.
The probe reports `archive_persistence_exercised = false`; real-database fixtures
exercise archival persistence independently.

The dev IP registration produced normal authenticated XML. A 147-attempt matrix
observed many first-page inventories and representative bodies. Its minimum
recorded admission spacing was 543 ms. Initial DNS failures were isolated to the
systemd stub resolver; a later 35-attempt probe using the upstream resolver in a
temporary service mount had no DNS failures and a minimum spacing of 553 ms.
These measure database reservation times, not socket-send timestamps. The serial
probe does not prove four-way concurrency; the PostgreSQL tests exercise that
contract.

Safe response-shape probes identified the audit list's documented/observed ID
alias and valid whitespace in 100 of 1106 administrative-appeal closing tags.
Regression fixtures use fictional values. No authenticated raw response, request
URL, token or provider body is committed here.

After those corrections, the final matrix used 163 admission attempts, observed
the first page of all 69 canonical families and projected 58 representative
details, including audit consultation. It had no DNS or authentication failures;
its minimum recorded admission spacing was 559 ms. Thirteen invalid-response
errors and one inventory identity gap remained explicit. No whole-corpus or
supplemental traversal was completed. Including earlier probes and conservative
allowance for an uncertain diagnostic launch, dev stayed within 384 upstream
attempts, below its authorized 400-attempt cap. The local IP-rejected probe used
three attempts. Both test credential copies were removed; the original token
file was preserved and its contents were never printed.

## Remaining limits

- Legal-term lists return some non-decimal composite IDs, up to 157 characters
  in the observed first page. No documented decomposition or stable canonical
  mapping was established; these rows remain rejected identity gaps.
- English detail identities/revisions and a complete English history selector
  remain unverified. Their private transport observations do not establish a
  publishable canonical body.
- Date-filtered change APIs lack a verified complete date universe. Those closed
  descriptors stay `NeedsVerification`; no invented starting date is used.
- The final sample verified one audit detail against its documented serial;
  broader detail forms, attachments and individual reuse conditions still
  require additional acceptance. Permanent retention does
  not mean unknown third-party rights have been verified.
- Full clone completion, hosted CI, production image builds, hardened-cluster
  acceptance and production activation have not been established by these checks.
  Mixed old/new collector generations must not overlap during activation.

See [the source catalog](CATALOG.md), [the provider profile](../../../docs/providers/kr-law-go-kr.md),
[database contract](../../../docs/database.md) and [upstream policy](../../../docs/upstream-policy.md).
