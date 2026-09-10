# Korean provider: LAW OPEN DATA

## Status and scope

Planned provider: the Republic of Korea's Ministry of Government Legislation
(법제처), through 국가법령정보 공동활용 / LAW OPEN DATA and 국가법령정보센터.
Initial coverage is national legislation and history. Other Korean dataset families
and other jurisdictions require their own scoped integration work; the common
domain must remain capable of distinguishing them.

This profile records official documentation inspected on **2026-09-10**. No
authenticated data endpoint, account approval, quota, validator, or live response
contract has been tested for this project. Documentation evidence is not a
statement of successful integration or a complete identifier mapping.

## Authoritative sources

| Source | Use |
| --- | --- |
| [API guide index](https://open.law.go.kr/LSO/openApi/guideList.do) | Dataset/view families, including legislation and history |
| [API usage manual](https://open.law.go.kr/LSO/openApi/openApiManual.do) | List/detail flow and linked resources |
| [Service usage guidance](https://open.law.go.kr/LSO/information/guide.do) | Application/approval, attribution, and service-use constraints |
| [Effective-date detail guide](https://open.law.go.kr/LSO/openApi/guideResult.do?htmlName=lsEfYdInfoGuide) | `target=eflaw`, `ID`, `MST`, `efYd`, and response fields |
| [Promulgation-based detail guide](https://open.law.go.kr/LSO/openApi/guideResult.do?htmlName=lsNwInfoGuide) | `target=law` and promulgation-based fields |

Recheck the relevant documentation when implementing a mapping. Capture the exact
endpoint/view and source section rather than citing only the index page.

## Documented endpoint and field distinctions

The guide index distinguishes promulgation-based and effective-date-based views,
along with history. They are not interchangeable meanings of "current law."

The effective-date detail guide documents `ID` as a law identifier and `MST` as a
master number associated with `lsi_seq`. For that endpoint, `ID` selects current
text and the guide says not to supply `efYd` with `ID`; master-based selection uses
the effective date. Do not infer that `ID`, `MST`, and date selectors can be merged
into one generic numeric ID or substituted without changing the requested view.

Both detail guides distinguish promulgation and effective dates. Provider mappings
must also inspect provision-level date fields and revision metadata before claiming
one date describes all provisions. Keep original Korean field names in mapping
evidence where an English shorthand could hide a distinction.

## Implications for this integration

These are engineering conclusions from the documented distinctions, not additional
upstream guarantees:

- Preserve view/target and supplied identifier/date selectors in request identity.
- Separate instrument identity from a selected representation/revision.
- Do not merge same-name records or assume a law identifier alone pins historical text.
- Maintain distinct promulgation and effective-date mappings and test their use.
- Validate response identity against the requested view before cache publication.

Exact uniqueness, persistence across revisions, correction behavior, and complete
historical-selection semantics remain to be established with endpoint-specific
evidence. Do not assert undocumented permanent identifiers or immutable revisions.

## Access, attribution, and outbound references

The service guidance describes application/approval before data use, source
attribution, and possible restriction for excessive traffic or other service-use
violations. Verify the applicable dataset conditions and approved access before
live integration; this profile does not claim approval has been obtained.

Detail guides document the `OC` authentication parameter. Keep the project's real
authentication value out of source, fixtures, logs, errors, and citation URLs.
Documentation sample credentials are not proof of permission for automated testing.

The usage manual describes linked attachment resources. Treat those links as
untrusted input, with explicit destination and resource limits before any fetch.
API documentation origins and request/resource origins need separate consideration:
the integration's concrete origin/path/redirect allowlist has not yet been validated.
Do not copy an HTTP example into an insecure default; verify the intended secure
transport without disabling certificate validation.

## Capabilities to establish before implementation

| Item | Current evidence/status | Required next step |
| --- | --- | --- |
| Project access/credentials | Not verified | Obtain/confirm appropriate approved access outside ordinary PR tests |
| Exact legislation/history endpoints and mappings | Detail-view distinctions documented; complete mapping not established | Record selected endpoints, selectors, response identity and errors |
| Numeric quotas and concurrency | Not established by the inspected pages | Confirm published/account constraints and choose documented conservative local budgets |
| Conditional requests/validators | Unverified | Establish support per representation; otherwise use bounded refresh |
| Incremental changes and deletion | Guide index lists related services; suitability/completeness unverified | Establish permissions, semantics, cursors and correction handling before relying on them |
| Absence and transient errors | Unverified | Establish response/error classification before negative caching |
| Freshness, stale limits and retention | Not chosen | Define per-class policy with evidence and operational rationale |
| Secure origins, redirects and linked resources | Not validated for this integration | Define and test outbound restrictions using approved access and mocks |
| Formats, encodings and resource bounds | Complete response behavior unverified | Collect minimal permitted fixtures; establish parser/decompression limits |

An unavailable capability has the explicit fallback described in the
[upstream policy](../upstream-policy.md); it is not silently assumed supported.

## Fixture and mapping evidence

Before merging a provider implementation, include curated list/detail and history
fixtures as applicable, with source/capture context and sanitized credentials.
Test distinct view/selector combinations, same-name records, missing fields,
promulgation versus effective dates, provision references, revisions, and error
responses. State which cases are authoritative captures and which are synthetic.

Record each consequential mapping as an upstream field/selector, normalized meaning,
source evidence, uncertainty, and expected test outcome. Follow the shared
[legal-data policy](../legal-data-policy.md); do not fabricate records to fill gaps
in provider coverage.
