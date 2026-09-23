# openlegal4everyone.stream

`openlegal4everyoneMCP` is the repository for **openlegal4everyone.stream**, an
open legal-information infrastructure project with a Rust MCP server foundation.

The intended backend retrieves authoritative upstream legal data, normalizes it
without inventing legal facts, preserves source references, and shares cached
results across hosted MCP requests. A public HTTP API and administrative tooling
may follow. The architecture supports multiple jurisdictions; the first provider adapter covers Korean national legislation, ordinances and precedents
through LAW OPEN DATA, with live acceptance and deployment still separate gates.

## Development status

Production server, ingestion and document-worker images use Alpine Linux 3.24
with native musl builds. Base images are digest-pinned; signed native packages
follow the v3.24 repositories. See [image build inputs](docs/dependencies.md#production-image-build-inputs)
for package inventories and the rebuild/rollback boundary, and the
[Alpine acceptance record](docs/alpine-image-acceptance.md) for executed checks and
remaining qualification limits.

The Rust server exposes pluggable read-only tools through Streamable HTTP and
WebTransport, with `server_info` as its initial diagnostic tool. Both transports
support MCP `2026-07-28` and `2025-11-25`. Tools and endpoint adapters register as
Rust modules at startup. WebTransport uses a documented custom binding and a native
Rust reference client; ChatGPT uses the HTTPS Streamable HTTP endpoint.

The workspace includes tests, dependency checks, CI, and a pinned OxiBelt integration
harness. A shared retrieval/cache framework, two synthetic JSON processors, and
an opt-in React MCP Apps browser now demonstrate upstream extensions. A separate
[Text comparison widget](docs/text-diff.md) compares supplied text with bounded
paging, managed text-file attachments and strict atomic patch application. The optional
[legal corpus tools](docs/database.md) provide indexed query/regex search, exact
checkpoint retrieval, metadata, history and comparisons with provenance and HEAD TTL.
The LAW OPEN DATA adapter and disposable document worker have offline fixtures;
live provider acceptance, hardened document-worker cluster acceptance and user accounts remain pending. Local browser tests use a simulated
host; the product domain does not imply deployment or verified live ChatGPT access.

A [GHCR release pipeline](CONTRIBUTING.md#ghcr-image-publication) publishes
versioned server and ingestion images for amd64 and arm64, plus an amd64 document
worker, after full CI and image acceptance. Deployments still select recorded
digests and require separate [operator qualification](docs/deployment-kubernetes.md#ghcr-release-images).

A standalone [search-query processor](docs/search-query.md) parses Boolean
expressions, field scopes, exact phrases, word groups, and prefixes. The corpus search evaluator combines Lindera and MeCab-Ko Korean morphology with
exact quoted substrings;
synthetic search still uses literal text.

## Run and extend

Install the pinned Rust toolchain, prepare the configuration and TLS certificate
paths from the [server guide](docs/server.md), then run:

```sh
cargo run --locked -p openlegal-server -- server.toml
cargo test --workspace --locked
```

The binary requires both data transports and a private health listener. For a
complete local edge/CA fixture, run `scripts/test-oxibelt.sh` with Docker; use
`--profile kubernetes` to exercise the committed NodePort handoff example with
disposable Docker endpoints. See [OxiBelt setup](docs/oxibelt.md). Contributor
validation and independent review requirements are in [Contributing](CONTRIBUTING.md).

For the runnable fictional provider and interactive widget, follow the
[synthetic demo guide](docs/demo.md). Ordinary server startup does not require
Node or a widget build. Enabling the legal corpus requires a provisioned MeCab-Ko
dictionary; see [corpus setup and index upgrades](docs/database.md#operator-configuration).

For hosting, start with the [Kubernetes operator runbook](docs/deployment-kubernetes.md#first-deployment)
and its [operator handoff](docs/deployment-kubernetes.md#operator-handoff).
The [recorded serving fixture](docs/deployment-acceptance.md#failures-and-remaining-boundaries)
left intermittent OxiBelt HTTP failures unresolved; production traffic and seamless
backend-only upgrades remain unaccepted.

## Documentation

- [Contributing](CONTRIBUTING.md): workflow, checks, security engineering, and review.
- [Agent orientation](AGENTS.md): where coding agents should begin.
- [Architecture](docs/architecture.md): server, legal-data and document-processing responsibilities.
- [Server contract](docs/server.md): configuration, extension API and transport behavior.
- [Kubernetes operator runbook](docs/deployment-kubernetes.md): topology, prerequisites, first deployment, upgrades, storage and index administration, rollback, optional ingestion and operator handoff.
- [GHCR image releases](CONTRIBUTING.md#ghcr-image-publication): tag and release commands, publication gates, and first-publication checks.
- [Serving acceptance](docs/deployment-kubernetes.md#phase-9-serving-acceptance): bounded explicit-endpoint smoke, a disposable Kubernetes fixture, and [executed evidence and remaining limits](docs/deployment-acceptance.md).
- [Dependencies](docs/dependencies.md): admission rationale and check tooling.
- [Retrieval framework](docs/retrieval.md): processors, caching, evidence and resource limits.
- [Search-query syntax](docs/search-query.md): standalone parser, operators, diagnostics, and bounds.
- [Legal corpus](docs/database.md): search, checkpoint retrieval, history, freshness and ingestion.
- [Document sandbox](docs/document-sandbox.md): isolated XML/HTML/PDF/HWP/OCR processing and acceptance.
- [Text comparison](docs/text-diff.md): Rust line/character engine, input limits, paging, retention and editable widget.
- [Synthetic browser](docs/demo.md): local setup, tools, progress and ChatGPT manual checks.
- [Retrieval validation](docs/retrieval-review.md): independent reviews, checks and remaining limits.
- [Legal-data policy](docs/legal-data-policy.md): identity, dates, normalization, and citations.
- [Upstream policy](docs/upstream-policy.md): caching, refresh, and service protection.
- [Korean provider profile](docs/providers/kr-law-go-kr.md): sources, scope, and unresolved capabilities.
- [Security policy draft](SECURITY.md): private reporting and its publication prerequisite.

## License

Copyright 2026 PiQuark6046 as a contributor of openlegal4everyoneMCP.

First-party code and documentation are licensed under the
[GNU Affero General Public License, version 3 only](LICENSE)
(`AGPL-3.0-only`). You may redistribute and modify this work under version 3 of
that license. It is provided without warranty, including implied warranties of
merchantability or fitness for a particular purpose. The license's appendix is
illustrative; this project's grant does not include later versions.

The September 12, 2026 migration applies from its commit onward. Earlier versions
remain available under their original Apache-2.0 terms; previously granted rights
are not revoked. Third-party dependencies retain their licenses and notices.
Upstream legal data has its own provenance and applicable reuse conditions; the
software license does not replace them. Documentation adaptation from the
maintainer's OxiBelt project is recorded in
[Contributing](CONTRIBUTING.md#oxibelt-provenance).

Every server configuration requires a public HTTPS corresponding-source URL for
the running server and widget. MCP clients and widget users can discover this
offer; operators must maintain the matching source and its availability. See
[source offers and migration](docs/server.md#source-offers-and-migration).

Optional [PostgreSQL 18 + BlobStore persistence and snapshot history](docs/persistence.md)
adds restart reuse, bounded captured history, MCP history tools, and record-widget
snapshot comparison. The [combined synthetic demo](docs/demo.md) enables L2 and
comparison together; all source data remains fictional.
