# Upstream and cache policy

## Purpose and status

The backend is a responsible client of public legal-data services. Repeated
equivalent MCP/API requests must reuse cached results or shared in-flight work;
they must not independently trigger an equivalent number of upstream requests.
The synthetic framework implements these controls as documented in the
[retrieval contract](retrieval.md). Real providers still require separate onboarding.

## Provider onboarding

Before integrating a provider, maintain a profile containing authoritative sources
and access dates; jurisdiction/dataset scope; identifier and date semantics;
approved access and credentials; permitted origins and redirects; reuse/attribution
conditions; and expected response/error formats.

The implementation must also settle and document:

- Cache freshness classes, maximum stale age, negative-cache lifetime, and retention.
- Per-provider concurrency/rate limits, queue bounds, request deadlines, retry
  attempt/elapsed-time limits, and response/decompression/work limits.
- Conditional request support, incremental feeds/cursors, deletion semantics,
  and fallback refresh behavior.
- Credential-sensitive response variation, absence semantics, and test fixtures.

Use documented provider constraints where available and record conservative local
limits where the provider does not publish numbers. Unknown capability is not
evidence of support. Do not discover safe quotas by flooding a public service.
The [Korean profile](providers/kr-law-go-kr.md) is the initial example.

## Cache identity and storage

Retrieval uses a process-local L1 cache and an explicit storage mode. The only
persistent architecture is [PostgreSQL 18 plus BlobStore](persistence.md). PostgreSQL
owns query identity, compatible current heads, immutable observed occurrences and
retention metadata. Blob storage owns content-addressed source bytes. Explicit
memory mode is non-persistent and is suitable for isolated demonstrations/tests.
Cold starts and persistent misses remain within the same upstream request budget.
A retained capture is an observation, not an authoritative legal revision or a
promise of permanent archival storage.

Keys must distinguish all dimensions affecting the result: provider, dataset,
record or query identity, revision/date selector, language/representation,
pagination, sorting, filters, and normalization/schema compatibility. Do not
collapse semantically different requests merely by sorting or removing parameters.
In particular, current-version lookups and explicit historical-version lookups
must not accidentally share an identity.

If credentials or authorization affect results, partition cache and coalescing
identity by the relevant access context. Never expose raw credentials in keys,
logs, citations, or responses. Search-result membership and detailed records are
different cache objects even when they reference the same instrument.

Validate source identity, expected format, completeness, and provenance before
publishing a successful entry. Publish payload/record metadata consistently; a
partial or failed refresh must not overwrite good data as a successful result.
Prevent an older concurrent refresh from replacing a newer accepted result.

Raw evidence retention follows the
[legal-data policy](legal-data-policy.md#retained-evidence-and-fixtures).
Bound both cache storage and bookkeeping such as in-flight and negative entries.

## Freshness and failure behavior

A fresh cache hit makes no upstream request. Record original retrieval time and
last successful validation time separately; neither is a legal effective date.
Freshness is an age/validation property, while cache use is a retrieval mechanism.
A cached response may be fresh; a successful conditional revalidation does not
mean the body was newly downloaded.

An expired entry triggers a bounded refresh according to the provider policy.
Permit stale fallback only within the documented limit, with an explicit stale
indicator and relevant validation context visible through MCP/API results.
For a fresh-only operation, return an explicit unavailable/freshness error if
freshness cannot be established. Do not silently downgrade that request.

Do not serve known poisoned, invalid, or authoritatively withdrawn data under a
stale exception. Distinguish a provider's deletion/withdrawal signal from an
unavailable endpoint or an incomplete scan. Historical access, if permitted, must
preserve withdrawal status rather than claiming the record is current.

Separate not-found, ambiguous, invalid-input, throttled, upstream-unavailable, and
normalization-failed outcomes. Serving layers may translate errors but must retain
their meaning. A successfully retrieved payload does not prove that all upstream
legal data is complete or current.

## Refresh and negative caching

Use validators such as ETag or Last-Modified only where supported. Bind validators
to the correct representation; a not-modified response requires the corresponding
usable cached payload. Apply documented update/deletion feeds or incremental
cursors when available, advancing checkpoints only after the required updates
have succeeded. Account for overlap, corrections, pagination, and partial failures.

If these capabilities are unavailable, use bounded scheduled or demand-driven
refresh. Do not perform full-corpus refresh for each user request or repeatedly
download details already available under a valid cache identity.

Negative-cache confirmed absence or valid empty searches only when the provider's
meaning is understood and a bounded lifetime is defined. Never turn authentication
errors, throttling, timeouts, transport failures, or parser failures into absence.
An HTTP status alone may not establish dataset-level absence. Keep temporary
failure backoff distinct from a negative legal-data result.

## Coordination and request budgets

Equivalent concurrent misses or refreshes share one in-flight refresh operation.
That operation may contain bounded retries or required pagination, all charged to
the shared budget; coalescing is not a promise that every operation needs exactly
one HTTP request.

Define ownership, waiter limits, timeouts, cancellation, cleanup, and failure
propagation. One caller's cancellation must not unexpectedly cancel work needed
by remaining callers. Release entries/permits on completion and failure. Avoid
holding broad locks across network waits and prevent unbounded waiter queues.

Per-provider rate, concurrency, and queue bounds cover demand requests, retries,
scheduled work, and administrative refresh together. A caller cannot bypass those
bounds through a force-refresh flag. Admission/overload failure must be bounded
and explicit rather than producing an unlimited backlog.

The initial deployment uses one backend instance and shared in-process policy.
Before enabling additional replicas or independent refresh processes, document
deployment-wide request budgeting, refresh coordination, failure recovery, and
the guarantee's scope. A shared database alone does not provide coalescing or
aggregate request-rate control. See the
[admin boundary](architecture.md#transport-and-administration-boundaries).

## Retry and outbound behavior

Retry only appropriate transient failures for safe operations, using bounded
attempts, an elapsed-time/deadline budget, exponential backoff, and jitter. Honor
applicable Retry-After guidance; if its delay exceeds the operation deadline,
defer or fail rather than retrying early. Non-retryable input/access/schema
failures must not enter blind retry loops.

There must be one policy owner for retries, not multiplicative loops in the MCP
handler, application, HTTP client, and scheduler. Outbound destination, TLS,
redirect, parser, and resource controls are defined in
[secure development](../CONTRIBUTING.md#secure-development).

## Operational evidence

Implement bounded, redacted metrics for cache hits/misses, upstream requests,
refresh failures, coalesced work, stale responses, throttling, retries, and queue
saturation. Avoid raw queries, identifiers, credentials, or URLs as unbounded
metric labels. Measurements should demonstrate that repeated requests reuse work.

Test fresh hits, concurrent misses, key separation, failed/partial refresh,
negative-cache classification, conditional validation, stale limits, cancellation,
and budget saturation with mock upstreams and controlled time. Live checks follow
[Contributing](../CONTRIBUTING.md#testing-and-ci).
