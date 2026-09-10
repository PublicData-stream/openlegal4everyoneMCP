# Agent orientation

## Project and current state

`openlegal4everyoneMCP` is the repository; `openlegal4everyone.stream` is the
product. It is intended to provide open legal-information infrastructure through
a Rust backend, hosted MCP, and potentially a public HTTP API.

At this governance bootstrap, the repository contains documentation and templates,
but no Cargo workspace, runtime, tests, or CI. Verify the checkout before acting;
do not report proposed components or future validation commands as implemented.
The [architecture](docs/architecture.md) contains the planned responsibility map.
Update this orientation when the backend is introduced.

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
| Korean legal data | [Korean provider profile](docs/providers/kr-law-go-kr.md) |
| MCP/API input, outbound requests, parsers, secrets, or unsafe Rust | [Secure development](CONTRIBUTING.md#secure-development) and [review gates](CONTRIBUTING.md#review-gates) |
| A suspected vulnerability | [Security policy](SECURITY.md), including its reporting-channel status |
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
