# Korean provider: LAW OPEN DATA

## Implementation and evidence status

The repository implements an opt-in adapter for the Republic of Korea's Ministry
of Government Legislation (법제처), through 국가법령정보 공동활용 / LAW OPEN DATA.
Configured ingestion covers national statutes (`eflaw`), administrative rules
(`admrul`), ordinances (`ordin`), treaties (`trty`), precedents (`prec`),
constitutional decisions (`detc`), legal interpretations (`expc`) and
administrative appeals (`decc`). The treaty record keeps the provider's
bilateral/multilateral code as a `document_type` search facet; these are nine
public categories across eight stored datasets. The existing NTS precedent HTML
path remains. Provider integration is disabled without explicit configuration
and a document sandbox.

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
| [Administrative-rule list](https://open.law.go.kr/LSO/openApi/guideResult.do?htmlName=admrulListGuide) and [detail](https://open.law.go.kr/LSO/openApi/guideResult.do?htmlName=admrulInfoGuide) | Stable `행정규칙ID`, record serial and body |
| [Treaty list](https://open.law.go.kr/LSO/openApi/guideResult.do?htmlName=trtyListGuide) and [detail](https://open.law.go.kr/LSO/openApi/guideResult.do?htmlName=trtyInfoGuide) | `trty`, `cls=1/2`, record serial and Korean view `010202` |
| [Constitutional-decision list](https://open.law.go.kr/LSO/openApi/guideResult.do?htmlName=detcListGuide) and [detail](https://open.law.go.kr/LSO/openApi/guideResult.do?htmlName=detcInfoGuide) | Decision serial and text fields |
| [Interpretation list](https://open.law.go.kr/LSO/openApi/guideResult.do?htmlName=expcListGuide) and [detail](https://open.law.go.kr/LSO/openApi/guideResult.do?htmlName=expcInfoGuide) | Interpretation serial and response/reason fields |
| [Administrative-appeal list](https://open.law.go.kr/LSO/openApi/guideResult.do?htmlName=deccListGuide) and [detail](https://open.law.go.kr/LSO/openApi/guideResult.do?htmlName=deccInfoGuide) | Appeal serial, order, claim and reasons |

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

Administrative rules have a stable `행정규칙ID` and a distinct serial checkpoint.
Treaties, precedents and decisions use their provider record serial. Treaties
preserve `440101` (bilateral) or `440102` (multilateral) and request Korean text.
Appeal list rows with serial `0` are skipped because no detail can be verified
from that value. Judgment dates describe judgments, not revisions.
`judgment_date_raw` preserves the precedent field; `judgment_date` is exposed
to typed search only for a valid Gregorian `YYYYMMDD` value. Missing or
malformed dates do not become guessed dates. Official revision history for
treaties, precedents and decisions remains unsupported; local capture history
is available separately.
Constitutional `종국일자` is retained as `final_disposition_date_raw` and, when
valid, `final_disposition_date`, separate from precedent `judgment_date`; it
does not satisfy the judgment-date search facet. Interpretation, decision and
issuance dates likewise retain their nonempty `_raw` fields, with typed dates
only for valid values.

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
- `mode = "pilot"` queues up to two revision-only manual candidates and two
  live current-list HEAD candidates for each of the nine public categories,
  scanning at most five live list pages per category. It stops upstream work
  after 30 minutes. The durable PostgreSQL ledger admits at most 100 attempts
  in this pilot, including list, detail and attachment requests. Pilot state
  is intentionally not reset on restart.
  Manual candidates contribute identity hints only; descriptive metadata for
  a public HEAD must come from a fresh live current-list observation.
- `mode = "continuous"` uses durable per-dataset page cursors and queues every
  record on one current page per dataset and one historical page for datasets
  with provider revisions each hour. The 128-job queue applies backpressure,
  and a current-page cursor advances only after all listed HEAD revisions have
  published. It alternates a front-page refresh with one-page overlap to reduce
  moving-offset omissions. This is incremental
  collection, not a stabilized full inventory. It never marks date-selector
  catalogs or corpus-wide coverage complete. Historical details are revalidated
  when their previous validation is over one hour old; unchanged bytes and
  records keep the existing capture, while corrected content creates a new
  capture. A body past its 30-day public retention age must be recaptured.
  Identical observations do not advance publication fences or sequences.
- Admission allows one fetch at a time, reserves an attempt before DNS, spaces
  attempts by at least five seconds across restarts and caps all requests at
  1,000 per UTC day in PostgreSQL. The next admissible time is conservatively
  rounded to a whole second. These limits are operator policy, not a provider
  quota assertion.
  HTTP 429/503 persists admission pauses from `Retry-After` across restarts,
  including HTTP dates. Guidance over seven days sets durable
  `operator_suspended=true` and blocks further attempts until the operator
  verifies the provider state, clears the suspension and restarts ingestion;
  the recorded `next_allowed_at` still prevents an early retry.
  Each reserved request remains marked `unresolved_response` until its response
  is handled. A crash or failed pause write leaves ingestion stopped after
  restart; the operator must inspect the provider state before clearing it.
  Deferred claims do not spend another job attempt while
  the pause holds. Permanent HTTP client rejection and deterministic parser/format failure
  return `source_rejected`; cancellation remains cancellation. Transient sandbox
  unavailability and timeouts remain retryable processing states.
- The queue holds at most 128 active jobs, with at most three attempts and fenced
  claims. A publication accepts at most 100 MiB combined source bytes and 64
  attachments; extracted/OCR text is limited to 16 MiB. Raw corpus, historical
  and staging accounting have separate limits of 480 GiB, 64 GiB and 16 GiB.

Manual list XML exports can select pilot candidates, but are not provider
detail evidence or proof of complete inventories. The September 2026 appeal
export contains `0` serials, and the constitutional-decision export has
malformed XML; neither establishes coverage or authorizes publication.
Run `scripts/prepare-law-pilot-candidates.py` against operator-held exports to
create the bounded JSON identity manifest. The current administrative-rule
export's `행정규칙ID`/`행정규칙LID` relationship has not been independently verified,
so the generator omits it. Missing or malformed categories use the live list
during a pilot. Manual candidates are queued as revision-only evidence. Even a
matching live detail cannot make them current HEAD; only a fresh provider
current-list observation can do that.

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
