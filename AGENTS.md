# Agent orientation

## Project and current state

`openlegal4everyoneMCP` is the repository; `openlegal4everyone.stream` is the
product. It is intended to provide open legal-information infrastructure through
a Rust backend, hosted MCP, and potentially a public HTTP API.

The repository implements a Rust server in `apps/server`, shared retrieval/cache
services and compiled payload processors in `crates`, and React MCP Apps for synthetic
records, Rust text comparison and the optional legal corpus in `apps/widget`.
Streamable HTTP and native WebTransport share tools and progress. The opt-in LAW
OPEN DATA adapter and disposable document worker have offline fixtures; live-provider
acceptance, hardened-cluster acceptance and production deployment remain pending.
Verify the checkout before acting. The [architecture](docs/architecture.md) separates
serving, legal-data and document-processing responsibilities; the
[server contract](docs/server.md) documents extensions, configuration and transports.

## Authoritative guidance

[CONTRIBUTING.md](CONTRIBUTING.md) is the source of truth for contributor workflow,
testing, review, documentation, commit messages, and secure-development duties.
Its [policy ownership table](CONTRIBUTING.md#policy-ownership) identifies the
canonical technical policies and private reporting policy.

This file summarizes navigation, not competing requirements. If a workflow
summary here diverges from CONTRIBUTING.md, follow CONTRIBUTING.md and correct
the summary. Do not resolve contradictory technical policies by silently choosing
the more convenient behavior; identify the conflict and seek a maintainer decision.
Repository guidance does not override higher-priority instructions, user scope,
or execution permissions.

## Task routing

| When working on | Read |
| --- | --- |
| Crates, modules, interfaces, or side effects | [Architecture](docs/architecture.md) |
| Identifiers, names, dates, revisions, normalization, or citations | [Legal-data policy](docs/legal-data-policy.md) and the relevant provider profile |
| Retrieval, caching, refresh, concurrency, or retries | [Upstream policy](docs/upstream-policy.md) |
| Synthetic provider, payload extensions, or widget | [Retrieval contract](docs/retrieval.md) and [demo guide](docs/demo.md) |
| Supplied-text comparison, Rust comparison workers, or comparison widget | [Text comparison](docs/text-diff.md) and [server contract](docs/server.md) |
| Korean legal data | [Korean provider profile](docs/providers/kr-law-go-kr.md) |
| Legal corpus tools, collection, search, or query syntax | [Legal corpus tools](docs/database.md) and [search-query syntax](docs/search-query.md) |
| PostgreSQL, blob storage, migrations, or snapshot history | [Persistence](docs/persistence.md) |
| Law-name resolution, legal analysis, or retained-source citations | [Legal reference tools](docs/legal-reference.md), [legal analysis tools](docs/legal-analysis.md) and [retained-source citations](docs/citations.md) |
| Kubernetes deployment, production images, or dependency admission | [Kubernetes operator runbook](docs/deployment-kubernetes.md) and [dependency admission](docs/dependencies.md) |
| Document worker or document sandbox | [Disposable document processing](docs/document-sandbox.md) |
| OxiBelt edge configuration or its integration harness | [OxiBelt edge integration](docs/oxibelt.md) |
| Dated review or acceptance records | [Records index](docs/records.md) |
| MCP/API input, outbound requests, parsers, secrets, or unsafe Rust | [Secure development](CONTRIBUTING.md#secure-development) and [review gates](CONTRIBUTING.md#review-gates) |
| A suspected vulnerability | [Security policy](SECURITY.md), including its reporting-channel status |
| End-user skills or Claude/ChatGPT plugin manifests | [openlegal4everyone plugin](plugins/openlegal4everyone/README.md); keep them separate from `.agents/skills` |
| Work that benefits from bounded delegation | [Subagent delegation skill](.agents/skills/subagent-delegation/SKILL.md) |

## Agent handoff

Report the resulting behavior, affected files, evidence, checks actually run, and
remaining limitations. Distinguish code-established facts, authoritative upstream
evidence, inference, and unresolved uncertainty when investigating legal data.
Do not treat a provider's documentation as a successful live API test.

Use portable repository paths and reproducible commands in durable handoffs.
For changes awaiting independent review, say so explicitly; do not equate an
implementation summary with completed review. Delegation does not transfer final
responsibility away from the parent agent or authorize publication, deployment,
live upstream traffic, or work outside the user's scope.

The orientation/requirements division is adapted from OxiBelt; see
[provenance](CONTRIBUTING.md#oxibelt-provenance).
