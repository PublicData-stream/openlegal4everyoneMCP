# Korean provider: LAW OPEN DATA

## Implementation and evidence status

The repository implements an opt-in adapter for the Republic of Korea's Ministry
of Government Legislation (법제처), through 국가법령정보 공동활용 / LAW OPEN DATA.
The closed source registry contains **69 original families**, preserving the
existing statute (`eflaw`), administrative-rule (`admrul`), ordinance (`ordin`),
treaty (`trty`), precedent (`prec`), constitutional-decision (`detc`), interpretation
(`expc`) and appeal (`decc`) identities. Institution and ministry/commission
namespaces remain distinct. Mobile, customized-subset and provision views do not
create new legal objects. List-only families do not acquire an invented detail
endpoint; knowledge-base result ordinals do not become stable legal-object IDs.
The existing NTS precedent HTML path remains. Ingestion requires explicit
configuration and a document sandbox.

Public documentation was captured on **2026-10-04**. The guide index displays 191
items, while its fetched HTML contains **195 unique guide links**. The catalog
preserves all 195 without assuming an explanation for this discrepancy; it groups
them into original families, finite metadata, identified supplements and query-only
features. Query-only services have no finite query-space traversal. These counts
are documentation evidence, not an account-scope inventory or proof that all
families have been downloaded. The [catalog evidence and unresolved contracts](../../test-support/fixtures/law-go-kr/CATALOG.md) distinguish those limits.

The date-filtered [statute change inventory](https://open.law.go.kr/LSO/openApi/guideResult.do?htmlName=lsChgListGuide)
and [daily provision change inventory](https://open.law.go.kr/LSO/openApi/guideResult.do?htmlName=lsDayJoRvsListGuide)
remain `NeedsVerification`, with requests disabled. Their date filters do not
establish an earliest supported date or a complete change-date universe. The
daily provision endpoint documents next-day availability. Neither sample dates,
promulgation/effective dates nor an empty response for today establish full coverage.

Mapping tests use fictional records and public guide-field fixtures, rather than
an authenticated full-corpus capture. Successful local tests do not establish
account approval, actual provider limits, all live response contracts or production
rollout. Actual NTS HTML identity markers remain unverified; unrecognized markup
fails closed. Existing eight-dataset live observations, when separately recorded,
do not establish successful access to newly registered families.

Finite inventory and identified-supplement responses can also be retained as
permanent source observations that preserve the retained evidence bytes. LAW
authentication echoed in documented link fields is redacted before storage; this
exception is recorded rather than presented as untouched HTTP evidence. Their
content-derived observation IDs
are separate from legal-record captures and citations; a retained list or relation
response does not establish the identity, currentness or completeness of a legal
record. Duplicate bytes for the same registered source key update validation time
without additional retained-byte accounting. Corrected responses preserve both
observations. Unverified materials remain metadata-only.

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

## Commercial use and application

The project's operator policy permits commercial use of the configured LAW OPEN
DATA legal-information APIs. Operators must state a commercial purpose, when
applicable, in their API application and obtain approval for the selected data
and actual egress IP. The application-purpose requirement is maintainer-supplied
confirmation recorded on 2026-10-01; this repository has not inspected the
account-specific application or approval screen.

The public [service guidance](https://open.law.go.kr/LSO/information/guide.do),
checked on 2026-10-01, expressly includes commercial use in its legal-information
reuse policy. Its generic restriction paragraph does not enumerate noncommercial
DRF APIs. Do not characterize the configured `eflaw`, `admrul`, `ordin`, `trty`,
`prec`, `detc`, `expc` or `decc` routes as commercially prohibited on that basis.
The separately licensed file-data conversion APIs on `data.go.kr` are different
services; their licenses do not establish restrictions on these DRF routes.
Preserve required source attribution and approved access conditions.

Reuse permission is recorded per material, with evidence URL and attribution.
The generic legal-information guidance does not establish third-party dictionary,
institutional-document, linked-content or attachment rights. Verified noncommercial
materials preserve `noncommercial_only`; their warning does not confer additional
permission. Verified no-derivatives materials preserve original bytes and provider
metadata only, skipping OCR, extraction, body indexing, excerpts, comparisons and
conversion. They expose `no_derivatives_original_only`. Unverified materials retain
metadata, official links and `rights_unverified` while original download and public
content remain withheld. Permission for a primary legal record is never inherited
by commentary or attachments automatically.

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

Statute, administrative-rule and ordinance list rows keep a nonempty
`제개정구분명` of at most 64 bytes as the capture metadata `amendment_type`,
verbatim. The [effective-date list guide](https://open.law.go.kr/LSO/openApi/guideResult.do?htmlName=lsEfYdListGuide)
documents the field and its amendment classes, including `폐지`, `폐지제정`,
`일괄폐지` and `타법폐지`. The value travels with the list observation through
the job queue; manual pilot hints do not supply it. A missing field is recorded
as absent, never inferred. The legal analysis tools derive repeal status from it
and nothing else; the observation-time `현행연혁코드` is not retained because it
changes as revisions take effect. Live list rows have not been checked for the
field.

## Text, evidence and references

Ordered XML fields retain article, paragraph, subparagraph, item and supplementary
text. Explicit sections distinguish provider text, extracted attachment text and
OCR. Source article keys or clearly named source ordinals identify sections;
ordinals are not legal citations. When a source article key repeats among the
projected sections, every occurrence uses
`article:{key}:source_ordinal:{position}` with its one-based projected position.
Unique source keys keep `article:{key}`. This suffix distinguishes source units
within a capture; it does not infer an article number, legal identity or revision.
Text, titles and order are preserved, including repeated headings. Empty units and
excluded supplementary subtrees do not create projected-key collisions.

Duplicate-key projections record `duplicate_source_article_keys` and bounded group
and section counts in metadata. Their provenance uses `law_go_kr_text_v3` or
`law_go_kr_additional_v2`; unaffected projections keep their prior version.
Generated locator collisions and oversized locators still fail validation.
Previously retained captures, sections and capture-fixed citations remain
immutable; changed future projections produce a new capture through normal
publication. Embedded HTML within XML text remains literal provider-field text.
OCR never replaces provider text.

The adapter downloads documented national PDF/HWP attachment-link fields through
an exact host/path allowlist. Ordinance attachment filenames alone do not justify
inventing a download URL. Retained primary and attachment bytes are immutable
evidence;
extracted sections carry their source digest and physical page. XML, HTML, PDF,
HWP5, HWPX and OCR parsing use the configured no-network document sandbox.

LAW may echo the request's authentication query in documented XML detail-link
fields. Only the exact active credential in a law.go.kr link's `OC` query value
is redacted before sandbox processing, permanent retention and public metadata.
This is a narrow exception to preserving upstream response bytes: legal wording
and non-authentication fields
remain unchanged. Retained digests, captures and original downloads identify the
authentication-redacted artifact, not the untouched HTTP response. Provenance and
processing diagnostics report the transformation through `credentials_redacted`,
`transport_credentials_redacted` and `provider_credential_redacted`. A response
that still contains the active credential is withheld; neither the credential nor
that response enters the archive. HTML and opaque PDF/HWP/HWPX responses containing
the credential are withheld without alteration. This exception does not authorize
body rewriting or weaken material-specific reuse restrictions.

An attachment endpoint can return HTTP success with bytes in the wrong document
format. An HTML response gets at most two additional requests through the same
durable admission ledger and spacing policy. If the response still is not the
advertised PDF/HWP format, the file is corrupt, or that attachment alone returns
404/410, an identified primary body may be published with
`attachment_status=incomplete`, expected/available counts, and a bounded list of
failed link ordinals, expected formats and optional response digests. A digest is
absent when no response bytes were received. Actual unexpected bytes are retained
as private evidence for the terminal failed attempt, not exposed as legal text; earlier HTML retry responses are discarded. `attachment_evidence_ordinals`
maps stored attachment evidence to advertised link ordinals. Successful
attachments retain their source sections. A valid PDF containing only `삭제` is
preserved as PDF evidence and text without inferring a legal withdrawal.

An incomplete result never replaces an existing complete HEAD or advances that
HEAD's validation time. An incomplete HEAD does not establish corpus coverage or
suppress later collection. Budget exhaustion, cancellation, authentication or
rate-limit rejection, destination violations, and document-worker infrastructure
failure do not qualify for partial publication. A corrupt primary body is never
published. The previously observed 200
HTML attachment page reported service congestion; its underlying cause and
subsequent availability remain unverified outside that diagnostic response.

Source references preserve dataset, selected identifier, `efYd`, character view
and the actual `type=XML` or `type=HTML`. The real `OC` value never appears in
references, fixtures, logs or returned errors. Requests use HTTPS with certificate
verification, bounded DNS resolution, no redirect following, and an explicit
destination allowlist. An optional operator-configured SOCKS5 next hop covers
lists, details, NTS HTML and attachments; its credentials are loaded only from
the environment variable named by `[database.ingestion.proxy].url_env`.
Destination DNS remains local, with every address checked and pinned before
SOCKS5 CONNECT; `socks5h` is unsupported. A proxy failure never triggers direct
fallback. Register the proxy's actual upstream-facing egress IP for approved
access, rather than assuming the collector host's IP is still used. The reported
European-host rejection, including after IP registration, is an operator
observation; geographic filtering has not been established as its cause.
These code-established restrictions have not
been validated against live provider responses.

## Local freshness and resource policy

These are application choices, not claimed upstream service guarantees:

- HEAD is fresh for one hour; disclosed stale serving ends at 24 hours after
  validation. An observed replacement immediately makes HEAD processing-pending,
  including when the durable job queue is full.
- Permitted current and historical bodies, attachments and corrected captures
  remain permanently archived, including multiple captures of one official
  revision. Withdrawal still blocks public access; archived evidence does not
  become current merely because it remains stored. Reader/session leases retain
  their coordination role. Previously deleted bytes must be recollected as new
  captures and cannot be restored under an old capture identity.
- `mode = "pilot"` uses bounded manual candidates for the legacy categories and
  live list candidates, scanning at most five live list pages per category.
  Extended families remain subject to the same finite pilot request allowance;
  a pilot is not a complete source inventory. It stops upstream work
  after 30 minutes by default. The durable PostgreSQL ledger defaults to at most
  100 attempts in this pilot, including list, detail and attachment requests.
  `pilot_attempt_limit` accepts a positive number or `"unlimited"`, and
  `pilot_timeout_secs` accepts 60..86400 seconds. The pilot start/window snapshot
  is retained, and expiration cancels inventory and detail work together. Pilot state
  is intentionally not reset on restart.
  Manual candidates contribute identity hints only; descriptive metadata for
  a public HEAD must come from a fresh live current-list observation.
  A structurally invalid list row is skipped while valid rows are used; a fully
  invalid page is recorded as a gap and the bounded scan continues. An individual
  404/410, bounded download failure (DNS, connection/TLS, request or body timeout,
  body read, HTTP 408/500/502/504), or corrupt downloaded detail is recorded as
  a distinct gap. A failed attachment leaves an explicitly incomplete record.
  TLS verification remains enabled. Authentication and other unsafe client
  rejection still suspend provider requests until operator review. Explicit
  cancellation, a dropped request, and failed budget writes remain unresolved.
  The pilot logs each rejected family and a summary when the scan finishes;
  queued candidates and prior valid pages do not establish publication or
  inventory completeness.
  Unclassified scan, job worker, index, lease and storage errors remain fatal
  to serving. A rejected list page is handled at its provider boundary.
- `mode = "continuous"` uses durable per-dataset page cursors and queues every
  record on one current page per dataset and one historical page for datasets
  with provider revisions per incremental scan pass. The delay between completed
  passes is `database.ingestion.scan_interval_secs` (default 3,600 seconds,
  accepted range 60–86,400); the active collection template uses 300 seconds.
  A pass can take longer while provider admission or processing is busy.
  The 128-job queue applies backpressure,
  and a current-page cursor advances only after each listed HEAD revision has
  published or has a durable, explicitly incomplete gap. Due page and detail gaps
  keep their existing retry eligibility and are revisited in a later scan under
  the same request budget. It
  alternates a front-page refresh with one-page overlap to reduce
  moving-offset omissions. This is incremental
  collection, not a stabilized full inventory. It never marks date-selector
  catalogs or corpus-wide coverage complete. Historical details are revalidated
  when their previous validation is over one hour old; unchanged bytes and
  records keep the existing capture, while corrected content creates a new
  capture without removing the previous one. Identical observations do not
  advance publication fences or sequences. Historical access has no 30-day
  expiry; successful revalidation changes validation timing, not capture time.
- Admission reserves an attempt before DNS, with four simultaneous HTTP
  attempts by default across all clients sharing the PostgreSQL LAW ledger.
  `[database.ingestion.provider_requests]` configures independent
  `continuous_daily_limit` and `on_demand_daily_limit` budgets, shared spacing
  and `max_in_flight` (1..16). Select `requests_per_second` (1..1000, evenly
  paced and rounded up to milliseconds) or legacy `min_interval_secs` (1..3600);
  selecting both is invalid. Omission defaults to five starts per second, no
  burst, unlimited continuous attempts and 1,000 explicit attempts per UTC day.
  Daily limits accept 1..1,000,000 or `"unlimited"`, and unlimited attempts remain
  charged. The committed collection template selects these same defaults.
  Daily limits use UTC calendar days, not rolling windows. Lists, details,
  attachments and retries all share admission. The rate is operator policy,
  not a provider quota assertion or a throughput guarantee. Both waiting modes
  alternate admission; expired waiting tickets relinquish priority. Configuration
  preserves charged counts, pauses, uncertain evidence and suspension, including
  when changing concurrency. The next admissible time rounds to whole seconds.
  HTTP 429/503 persists admission pauses from `Retry-After` across restarts,
  including HTTP dates. Guidance over seven days sets durable
  `operator_suspended=true` and blocks further attempts until the operator
  verifies the provider state, clears the suspension and restarts ingestion;
  the recorded `next_allowed_at` still prevents an early retry.
  Each reservation has an owner/slot in `provider_request_admission` and a
  detached advisory-lock session. A live owner permits other slots to proceed;
  loss of any owner's session leaves durable uncertain evidence and globally
  stops new requests. A bounded classified failure can settle only its own
  reservation. Cancellation before outcome handling keeps that evidence, even
  if the request may not have been sent. Late rejection or another owner's
  completion cannot erase it. Legacy singleton `unresolved_response` remains
  a separate global fence. Operator review of provider state is required before
  clearing either form of uncertain evidence. A failed pause or suspension write
  also preserves the unresolved reservation.
  Deferred claims do not spend another job attempt while
  the pause holds. Individual 404/410, bounded download failures, and corrupt
  downloaded bytes are classified separately from authentication and unsafe
  client rejection. The former can be skipped and reported; the latter suspend
  provider requests. HTTP 429/503 retain their durable pause. Cancellation
  remains cancellation. Transient sandbox
  unavailability and timeouts remain retryable processing states.
- The queue holds at most 128 active jobs, with three total attempts by default
  (`max_job_attempts` accepts 1..10, including the first execution) and fenced
  claims. A publication accepts at most 100 MiB combined source bytes and 64
  attachments; extracted/OCR text is limited to 16 MiB. Raw corpus accounting
  defaults to `"unlimited"`; `[database].max_raw_bytes` optionally accepts a
  positive byte count. Reaching it blocks new publication without removing old
  evidence. There is no separate 64 GiB history cap. Staging retains a 16 GiB cap;
  filesystem exhaustion also blocks new writes until capacity becomes available.

On-demand operations default to 32 admission attempts and 7200 seconds.
`on_demand_attempt_limit` accepts 1..1,000,000 or `"unlimited"` and
`on_demand_timeout_secs` accepts 60..86400. Limits are snapshotted at launch;
client clones share the finite local counter, while unrelated operations do not.
A finite local debit retains the existing conservative ordering before durable
reservation. Uncertain reservation or cancellation does not automatically refund
it. Increasing a processing retry limit never silently revives terminal failures;
scheduled collection-gap cycles remain distinct from inline HTTP retries.

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

The [2026-10-04 acceptance record](../../test-support/fixtures/law-go-kr/ACCEPTANCE.md)
separates offline tests, native dev execution and a bounded authenticated probe.
The probe observed first-page inventories for all 69 families; it did not complete
an operational clone or establish every detail/attachment contract.

Approved credentials, actual quotas, conditional validators, upstream error bodies,
correction/deletion semantics, attachment availability and representative provider
fixtures still require authorized verification. Preserve this distinction in
release notes and coverage reporting. Ordinary tests must not make live provider
requests. See the [legal-data policy](../legal-data-policy.md),
[upstream policy](../upstream-policy.md) and [database contract](../database.md).

### Transport evidence and separately licensed material

Approved primary legal-information API responses are retained privately as
transport evidence before parsing, including when parsing fails. The only permitted
transport redaction replaces only the exact active credential in a law.go.kr
link's `OC` query value in documented XML fields. Retained evidence is exact after
that disclosed transformation. This implements
the requested permanent audit archive under the service's
[legal-information reuse guidance](https://open.law.go.kr/LSO/information/guide.do)
and [copyright policy](https://www.law.go.kr/lawPetitionForm.do?menuId=13&subMenuId=79).
Treating the API envelope as transport evidence is an implementation interpretation;
it does not verify reuse rights for every embedded third-party work. Unknown
inline supplementary content is excluded from body projection, and its containing
original response is not exported through source-file. Unknown separate attachments
are metadata-only and never trigger an extra body download. Unknown source families
also retain metadata only. Primary transport evidence is never automatically promoted
to a public resource when supplementary rights remain unresolved.

Verified primary article units seed provision-history and term relations from
explicit numeric article and branch fields (JO: four plus two digits). Article
keys and missing branch fields never supply inferred JO identifiers.
