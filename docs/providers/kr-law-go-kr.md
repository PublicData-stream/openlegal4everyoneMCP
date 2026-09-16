# Korean provider: LAW OPEN DATA

## Implementation and evidence status

The repository implements an opt-in adapter for the Republic of Korea's Ministry
of Government Legislation (법제처), through 국가법령정보 공동활용 / LAW OPEN DATA.
Configured ingestion covers national statutes (`eflaw`), every ordinance type
exposed by `ordin`, and precedents (`prec`), including an HTML processing path for
National Tax Service records. Provider integration is disabled without explicit
configuration and a document sandbox.

Official documentation was inspected during the September 2026 implementation.
The checked-in mapping tests use **fictional fixtures**, not captured legal records.
No authenticated endpoint, account approval, quota, live response contract, or
production deployment has been verified. Implemented request construction and
local tests do not establish successful live integration. In particular, actual
NTS HTML identity markers remain unverified; unrecognized markup fails closed.

## Authoritative sources

| Source | Mapping evidence |
| --- | --- |
| [API guide index](https://open.law.go.kr/LSO/openApi/guideList.do) | Dataset and view families |
| [API usage manual](https://open.law.go.kr/LSO/openApi/openApiManual.do) | List/detail flow and linked resources |
| [Service guidance](https://open.law.go.kr/LSO/information/guide.do) | Access approval, attribution and use constraints |
| [Effective-date list](https://open.law.go.kr/LSO/openApi/guideResult.do?htmlName=lsEfYdListGuide) | `eflaw` inventory, `nw`, `LID`, revision and date fields |
| [Effective-date detail](https://open.law.go.kr/LSO/openApi/guideResult.do?htmlName=lsEfYdInfoGuide) | `ID`, `MST`, `efYd`, character view, nested text and attachments |
| [Ordinance list](https://open.law.go.kr/LSO/openApi/guideResult.do?htmlName=ordinListGuide) | Current/history inventory and ordinance identities |
| [Ordinance detail](https://open.law.go.kr/LSO/openApi/guideResult.do?htmlName=ordinInfoGuide) | Identifier fields, ordinance types and content |
| [Precedent list](https://open.law.go.kr/LSO/openApi/guideResult.do?htmlName=precListGuide) | Record identity, source, case number and judgment date |
| [Precedent detail](https://open.law.go.kr/LSO/openApi/guideResult.do?htmlName=precInfoGuide) | Detail fields and the NTS HTML-only restriction |

## Identity, views and dates

Application identity is `(jurisdiction=kr, provider=law_go_kr, dataset, provider ID)`.
Names never establish identity. National `법령ID` and ordinance `자치법규ID` are
separate from the provider master/serial identifying a selected revision.

National current inventory explicitly requests `nw=3`; historical inventory uses
`nw=1,3`, optionally restricted by the documented `LID`. A national checkpoint
is the composite `MST:efYd`, because an effective-date detail needs both values.
Requests select the original-character view with `chrClsCd=010201`. Selecting
current text by `ID` would ignore the supplied effective date, so it is not used
as a historical substitute. The normalized response must match the requested
object and effective date. Provider revision identifiers are not assumed immutable:
corrected bytes create another capture under the same official revision.

Ordinance inventories request `nw=1` for current and `nw=2` for history. All
provider ordinance types remain in scope, including rules, instructions, notices
and council rules. History enumeration is global because the inspected guide
does not establish a stable-object-ID list filter. Its current view retains the
provider's classification; the adapter does not infer national effective-view
semantics for ordinances.

Precedents use the provider record serial. Judgment dates describe judgments,
not revisions. `judgment_date_raw` preserves the supplied field; `judgment_date`
is exposed to typed search only for a valid Gregorian `YYYYMMDD` value. Missing
or malformed dates do not become guessed dates. Same-decision official revision
history remains unsupported pending a documented provider; local capture history
is available separately and is not described as official precedent history.

Publication and effective dates remain separate. Date-only checkpoint resolution
requires a complete observed inventory and exactly one matching revision; it
refuses ambiguity, incomplete history and missing historical bodies. Inventory
pagination is not a provider-guaranteed atomic snapshot. Runtime completeness must
only be asserted after its stabilized full-traversal checks succeed. Catalog
changes invalidate history/date-resolution views without invalidating a running
normalization job; job publication has its own version fence.

## Text, evidence and references

Ordered XML fields retain article, paragraph, subparagraph, item and supplementary
text. Explicit sections distinguish provider text, extracted attachment text and
OCR. Source article keys or clearly named source ordinals identify sections;
ordinals are not legal citations. Embedded HTML within XML text remains literal
provider-field text. OCR never replaces provider text.

The adapter downloads documented national PDF/HWP attachment-link fields through
an exact host/path allowlist. Ordinance attachment filenames alone do not justify
inventing a download URL. Primary and attachment bytes are immutable evidence;
extracted sections carry their source digest and physical page. XML, HTML, PDF,
HWP5, HWPX and OCR parsing use the configured no-network document sandbox.

Source references preserve dataset, selected identifier, `efYd`, character view
and the actual `type=XML` or `type=HTML`. The real `OC` value never appears in
references, fixtures, logs or returned errors. Requests use HTTPS with certificate
verification, bounded DNS resolution, no proxy or redirect following, and an
explicit destination allowlist. These code-established restrictions have not
been validated against live provider responses.

## Local freshness and resource policy

These are application choices, not claimed upstream service guarantees:

- HEAD is fresh for one hour; disclosed stale serving ends at 24 hours after
  validation. An observed replacement immediately makes HEAD processing-pending,
  including when the durable job queue is full.
- Current bodies remain durable. Historical bodies have a 30-day retention policy;
  independent revision and capture metadata catalogs survive body eviction.
  Active sessions and index acknowledgment protect bytes during retention.
- Daily bounded inventory revisits and durable jobs are coordinated by the runtime.
  Identical catalog observations do not advance publication fences or sequences.
- Admission allows one fetch at a time and starts at most one request per second.
  HTTP 429/503 honors bounded admission pauses from `Retry-After`, including HTTP
  dates. Permanent HTTP client rejection and deterministic parser/format failure
  return `source_rejected`; cancellation remains cancellation. Transient sandbox
  unavailability and timeouts remain retryable processing states.
- The queue holds at most 128 active jobs, with at most three attempts and fenced
  claims. A publication accepts at most 100 MiB combined source bytes and 64
  attachments; extracted/OCR text is limited to 16 MiB. Raw corpus, historical
  and staging accounting have separate limits of 1 TiB, 64 GiB and 16 GiB.

`retrieved_at` records completion of the primary download before sandbox parsing.
`captured_at`/`cached_at` records the publication transaction timestamp after blob
staging and lock admission; results become visible only after commit. It is not
claimed to be the database's exact commit instant. Unchanged evidence updates
validation timing without inventing a new capture.

## Remaining external validation

Approved credentials, actual quotas, conditional validators, upstream error bodies,
correction/deletion semantics, attachment availability and representative provider
fixtures still require authorized verification. Preserve this distinction in
release notes and coverage reporting. Ordinary tests must not make live provider
requests. See the [legal-data policy](../legal-data-policy.md),
[upstream policy](../upstream-policy.md) and [database contract](../database.md).
