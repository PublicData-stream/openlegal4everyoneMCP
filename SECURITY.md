# Security policy

## Publication status: private reporting setup pending

This reporting policy is a **draft**. Before relying on these instructions as an
operational reporting channel, the maintainer must enable GitHub private
vulnerability reporting for `PublicData-stream/openlegal4everyoneMCP` and verify
that the private report form is available. Record that verification in the PR that
updates this policy and replace this status section with the verified reporting status.

The intended private route is
[GitHub private vulnerability reporting](https://github.com/PublicData-stream/openlegal4everyoneMCP/security/advisories/new).
Its availability has not been verified. Do not submit a test vulnerability to check
it. If the route is unavailable, withhold vulnerability details and ask the
maintainer to enable private reporting through a non-sensitive setup request.
Do not substitute a public issue, PR, discussion, or revealing commit.

## Development-stage support scope

The repository implements an HTTP/WebTransport MCP server without public caller
authentication; optional backend mTLS authenticates only the configured edge. It
also contains the opt-in legal corpus and LAW OPEN DATA ingestion, the disposable
document worker, Kubernetes deployment templates, dependency checks and an isolated
OxiBelt integration harness. A [GHCR publication workflow](CONTRIBUTING.md#ghcr-image-publication)
is configured to publish versioned container images. No verified hosted production service is
established, and this policy defines no support status for published images.
Security concerns in code, defaults, deployment templates, dependencies as used by
this project, and documentation are relevant.

Update this scope when implementation, releases or deployment status change. Do not
infer release support windows, response-time guarantees, backports, or coverage of
third-party systems from this development policy.

## Reporting a vulnerability

Once the private route above is verified, use it for undisclosed vulnerabilities.
Include the affected commit/component, relevant configuration, attacker-controlled
inputs or prerequisites, observed impact, source locations, and a minimal safe
description or local test demonstrating the problem when available. Include
sanitized diagnostics and any known mitigation or uncertainty.

Do not include credentials, unnecessary personal data, or bulk legal-data payloads.
Ordinary discrepancies without a suspected security impact can use the
[legal-data discrepancy template](.github/ISSUE_TEMPLATE/data-integrity.md). Keep suspected
cache poisoning, unauthorized access, or other security-sensitive reports private.

## Disclosure handling

Maintainers assess reports and coordinate remediation and disclosure privately.
Keep undisclosed vulnerability details out of public channels until a supported fix
or actionable mitigation has been verified and maintainers have authorized
disclosure. Do not automatically publish an advisory or request a CVE as a side
effect of analysis. A user-authorized write must preserve its intended visibility;
if tooling cannot do that, report the limitation without changing channels.

This policy does not authorize testing against live third-party upstream services.
Use bounded local evidence and respect the project's
[upstream policy](docs/upstream-policy.md).

## Secure development

[CONTRIBUTING.md](CONTRIBUTING.md#secure-development) owns secure-development and
unsafe-Rust requirements; its [review gates](CONTRIBUTING.md#review-gates) govern
independent review. Technical cache and data invariants live in their linked
policies rather than being duplicated here.
