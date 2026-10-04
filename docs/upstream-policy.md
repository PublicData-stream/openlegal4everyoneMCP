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
An identified primary body may be published with missing linked attachment
evidence only when its provider profile defines an explicit incomplete status,
retains the terminal failed response bytes privately when available, and prevents that capture from replacing
a complete HEAD or satisfying full-coverage checks. Other validation failures
remain failures.

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

For the Korean corpus, `[database].auto_collection` defaults to `true`. Eligible
HEAD lookups and local searches may enqueue bounded demand collection; they
return local data and a receipt rather than waiting for provider results.
`database.request_collection` stores an explicit coalesced request for the same
dedicated collection Job path; `database.collection_status` only reads its state.
Disable automatic collection to retain local-only lookup/search behavior.
The scheduler and request Jobs share provider spacing and
single-call admission. Operator configuration selects independent continuous and
explicit daily caps, each accepting a positive limit or `"unlimited"`, and shared
request pacing. These are UTC calendar-day caps, not rolling 24-hour windows. Defaults are 1,000 reserved
attempts per UTC day for each mode and a five-second minimum interval. The
ingestion template selects 50,000 continuous and 1,000 explicit attempts with a
one-second minimum interval and a five-minute busy recheck interval. This is operator
policy, not a claim about provider quotas or achieved throughput. All clients use
the same durable policy and charged counters, including after restarts. Increasing
an exhausted budget may advance only budget-wait leases; it must preserve existing
Retry-After pauses, suspension and unresolved-response evidence. Lowering a cap
retains already charged attempts and blocks further admission until allowance
exists. Unlimited admission continues recording attempts, so returning to a finite
cap cannot discard prior usage. The scan interval is independent of legal-data freshness and gap retries.

`[database.ingestion].adaptive_polling` defaults to `true`. An idle LAW provider
starts its next inventory cycle immediately, including without downstream demand.
Actual HTTP admission, a claimable detail backlog or eligible foreground demand
delays background collection. `scan_interval_secs` bounds busy rechecks; shared
PostgreSQL notifications wake waiting collectors and the request scheduler earlier.
A five-second readiness fallback re-reads state without initiating another scan
while still busy. Setting `adaptive_polling = false` restores fixed waits after
inventory cycles. This policy does not change synthetic retrieval.

Eligible demand takes the next HTTP slot after the current call. Daily-exhausted
demand and document parsing alone do not hold that slot. Session advisory locks
and owner tokens fence durable attempts across processes. An abandoned response
marker remains fail-closed; a waiter behind a live owner spends no request attempt.
Processing claims released before any durable reservation are refunded with an
owner fence. Retry-After, failure retries and finite operation deadlines still apply.

Automatic national-statute HEAD collection rechecks authoritative one-hour
freshness before enqueueing. Successful discovery is reused for one hour per
normalized term, datasets and mode; failed/partial attempts cool down for one hour.
Active automatic and explicit requests share canonical targets. Explicit successful
requests and old receipt identities remain available for 24 hours. Capture,
revision, historical, continuation and source-URL reads never enqueue collection.
See [legal corpus](database.md) for eligible query inputs and response contracts.
The scheduler marks an explicit request failed when its Kubernetes Job reports a
terminal failure, including failure before the request Pod can open storage.
Transient storage admission contention is retried only during collection Pod
startup, before any provider request is made. An uncertain Job creation outcome
remains fenced until it can be reconciled or its lease expires.

LAW accepts `requests_per_second = 1..1000` instead of an explicitly selected
`min_interval_secs`; specifying both is invalid. Requests remain single-flight,
with start spacing rounded up to milliseconds. This is an upper bound, not a
throughput guarantee. Daily exhaustion never clears a provider pause.

Synthetic retrieval has its own `[demo.provider_requests]` policy. Its daily cap
defaults to `"unlimited"` and its rate defaults to two starts per second with burst
two and concurrency two. Layouts belonging to the same provider share this policy;
unrelated providers and origins do not. Persistent mode reserves daily attempts
in a PostgreSQL ledger keyed by provider and origin namespace, including attempts
under an unlimited cap. Memory mode counts only within the process and resets on
reconstruction. A cache hit does not reserve an attempt; every fetch and retry
reserves before DNS. Admission storage failure prevents the fetch and never falls
back to memory accounting. Quota exhaustion returns a non-retryable local admission
error through the existing MCP `rate_limited` contract.

Optional attempt caps do not disable finite deadlines or retry budgets. LAW pilot
and explicit-operation caps default to 100 and 32 respectively; each also accepts
`"unlimited"`. Pilot and explicit timeouts default to 1800 and 7200 seconds, with
60..86400 supported. LAW `max_job_attempts` defaults to three total processing
executions, including the first; its HTTP transport does not add retries. Demo
`max_attempts` defaults to two total fetches. Both maxima accept 1..10. Scheduled
collection-gap cycles remain distinct future work, not inline retry loops.


Negative-cache confirmed absence or valid empty searches only when the provider's
meaning is understood and a bounded lifetime is defined. Never turn authentication
errors, throttling, timeouts, transport failures, or parser failures into absence.
An explicit incomplete collection gap may record one failed item for a later
bounded retry without claiming that the legal item is absent.
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

An upstream may use its own explicit operator-selected SOCKS5 next hop. Configure
`[database.ingestion.proxy].url_env` for LAW OPEN DATA and `[demo.proxy].url_env`
for the synthetic upstream; the referenced environment variable contains the
proxy URL, including optional credentials. Omission selects direct access;
configuration with a missing, empty or invalid value fails startup. Ambient
`HTTP_PROXY`, `HTTPS_PROXY`, `ALL_PROXY` and `NO_PROXY` do not select or override
these routes. A failed proxy request must never fall back to a direct request.
Proxy routing applies to every request under that provider, including linked
attachments and on-demand collection, without resetting or splitting its ledger.

Only `socks5://[username:password@]host:port` is supported, with an explicit
nonzero port; proxy DNS names must use ASCII/Punycode form. Destination DNS remains
local: validate all returned addresses and
pin them before CONNECT, retaining HTTPS hostname and certificate verification.
`socks5h` is deliberately unsupported because remote DNS would bypass that address
validation. A proxy is a trusted operator-controlled next hop and may be on a
private network; this exception applies only to the proxy, not fetched URLs.
Unlike fetched-target DNS, a proxy hostname uses the HTTP client's OS resolver;
an already started blocking lookup may continue after timeout or cancellation.
Prefer a literal proxy IP when strict next-hop cancellation/resource accounting
is needed. Request deadlines still bound how long the caller waits.
SOCKS5 does not encrypt its own handshake or authentication. Protect that hop
using a private network or an SSH/VPN tunnel when necessary. Keep proxy URLs out
of committed TOML, diagnostics and source evidence. Proxy selection alone does
not prove a provider accepts the egress IP, and does not alter cache identity.

## Operational evidence

Implement bounded, redacted metrics for cache hits/misses, upstream requests,
refresh failures, coalesced work, stale responses, throttling, retries, and queue
saturation. Avoid raw queries, identifiers, credentials, or URLs as unbounded
metric labels. Measurements should demonstrate that repeated requests reuse work.

Test fresh hits, concurrent misses, key separation, failed/partial refresh,
negative-cache classification, conditional validation, stale limits, cancellation,
and budget saturation with mock upstreams and controlled time. Live checks follow
[Contributing](../CONTRIBUTING.md#testing-and-ci).
