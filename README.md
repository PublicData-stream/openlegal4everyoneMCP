# openlegal4everyone.stream

`openlegal4everyoneMCP` is the repository for **openlegal4everyone.stream**, an
open legal-information infrastructure project with a planned Rust backend.

The intended backend retrieves authoritative upstream legal data, normalizes it
without inventing legal facts, preserves source references, and shares cached
results across hosted MCP requests. A public HTTP API and administrative tooling
may follow. The architecture supports multiple jurisdictions; the first planned
integration covers Korean national legislation and history through LAW OPEN DATA.

## Development status

This repository currently contains its Apache-2.0 license, contributor and agent
guidance, architecture and data policies, and contribution templates. There is no
Rust workspace, running MCP server, public API, test suite, or CI workflow yet.
The product name is not a claim that a service is deployed at that domain.

The initial hosted design uses one backend instance. Repeated equivalent requests
should reuse cached data or shared refresh work instead of independently querying
the upstream provider. Responses must preserve provenance and make relevant
freshness limitations explicit.

## Documentation

- [Contributing](CONTRIBUTING.md): workflow, checks, security engineering, and review.
- [Agent orientation](AGENTS.md): where coding agents should begin.
- [Architecture](docs/architecture.md): planned components and dependency direction.
- [Legal-data policy](docs/legal-data-policy.md): identity, dates, normalization, and citations.
- [Upstream policy](docs/upstream-policy.md): caching, refresh, and service protection.
- [Korean provider profile](docs/providers/kr-law-go-kr.md): sources, scope, and unresolved capabilities.
- [Security policy draft](SECURITY.md): private reporting and its publication prerequisite.

## License

Project contributions are covered by [Apache-2.0](LICENSE). Upstream legal data
has its own provenance and applicable reuse conditions; the software license does
not replace them. Documentation adaptation from the maintainer's OxiBelt project
is recorded in [Contributing](CONTRIBUTING.md#oxibelt-provenance).
