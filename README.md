# openlegal4everyone.stream

`openlegal4everyoneMCP` is the repository for **openlegal4everyone.stream**, an
open legal-information infrastructure project with a Rust MCP server foundation.

The intended backend retrieves authoritative upstream legal data, normalizes it
without inventing legal facts, preserves source references, and shares cached
results across hosted MCP requests. A public HTTP API and administrative tooling
may follow. The architecture supports multiple jurisdictions; the first planned
integration covers Korean national legislation and history through LAW OPEN DATA.

## Development status

The Rust server exposes pluggable read-only tools through Streamable HTTP and
WebTransport, with `server_info` as its initial diagnostic tool. Both transports
support MCP `2026-07-28` and `2025-11-25`. Tools and endpoint adapters register as
Rust modules at startup. WebTransport uses a documented custom binding and a native
Rust reference client; ChatGPT uses the HTTPS Streamable HTTP endpoint.

The workspace includes tests, dependency checks, CI, and a pinned OxiBelt integration
harness. Legal-data retrieval, caches, providers and user accounts remain planned.
The product domain does not imply a deployed service, and browser/ChatGPT platform
integration has not been verified.

## Run and extend

Install the pinned Rust toolchain, prepare the configuration and TLS certificate
paths from the [server guide](docs/server.md), then run:

```sh
cargo run --locked -p openlegal-server -- server.toml
cargo test --workspace --locked
```

The binary requires both data transports and a private health listener. For a
complete local edge/CA fixture, run `scripts/test-oxibelt.sh` with Docker; see
[OxiBelt setup](docs/oxibelt.md). Contributor validation and independent review
requirements are in [Contributing](CONTRIBUTING.md).

## Documentation

- [Contributing](CONTRIBUTING.md): workflow, checks, security engineering, and review.
- [Agent orientation](AGENTS.md): where coding agents should begin.
- [Architecture](docs/architecture.md): implemented server and planned legal-data layers.
- [Server contract](docs/server.md): configuration, extension API and transport behavior.
- [Dependencies](docs/dependencies.md): admission rationale and check tooling.
- [Legal-data policy](docs/legal-data-policy.md): identity, dates, normalization, and citations.
- [Upstream policy](docs/upstream-policy.md): caching, refresh, and service protection.
- [Korean provider profile](docs/providers/kr-law-go-kr.md): sources, scope, and unresolved capabilities.
- [Security policy draft](SECURITY.md): private reporting and its publication prerequisite.

## License

Project contributions are covered by [Apache-2.0](LICENSE). Upstream legal data
has its own provenance and applicable reuse conditions; the software license does
not replace them. Documentation adaptation from the maintainer's OxiBelt project
is recorded in [Contributing](CONTRIBUTING.md#oxibelt-provenance).
