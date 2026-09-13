# Upstream retrieval and synthetic processing

This implementation is an extension framework with fictional demonstration data.
It does not retrieve Korean law or establish legal identity, dates, applicability,
or citations. The [upstream policy](upstream-policy.md) remains the owner of shared
retrieval invariants; real providers require their own evidence-backed onboarding.

## Extension and responsibility contracts

Register trusted compiled Rust processing modules at startup. A `PayloadProcessor`
accepts supplied bytes and an explicit query/context; it performs no network,
filesystem, environment, or clock access. Two JSON layouts demonstrate the same
typed output from different source schemas. Source identity is namespaced and
never inferred from a title. The raw source and processor version remain linked
to each accepted result by its payload digest.

`RetrievalService` owns cache decisions, shared refreshes, retries, provider limits,
and publication. `Upstream` supplies a narrow fetch operation. Concrete HTTP
adapters implement that operation and invoke pure processing. `CacheStore` owns
L1 mechanics without deciding freshness. The asynchronous `PersistentStore` port
provides optional filesystem L2 and immutable captured history; see the
[filesystem contract](filesystem-cache.md). Source registrations bind the
source, provider, dataset, processor version, and adapter before serving.

MCP tools invoke one shared service across HTTP and WebTransport. Widget requests
use those same tools through the MCP Apps host. No transport owns another cache
or retry loop. Processing modules are trusted code and must honor work limits;
a Rust trait or asynchronous timeout does not isolate a malicious module.

## Initial local policy

These numbers are conservative demonstration policy, not documented provider
quotas. A real source must choose and review its own policy before onboarding.

| Property | Limit |
| --- | --- |
| Fresh age | Less than 60 seconds since successful validation |
| Stale fallback | Only after transient refresh failure, at most 300 seconds since validation |
| Memory cache | 256 entries or 32 MiB, including retained source bodies |
| L1 evidence retention | Evict source and processed data together after 300 seconds or LRU pressure; optional L2 retention is separate |
| Provider concurrency | Two requests |
| Request starts | Two per second, burst two; all retries included |
| Refresh deadline | Ten seconds total; five seconds per attempt; at most two attempts |
| Shared work | 32 distinct refreshes, 16 waiters per key, 64 waiters overall |
| Upstream body | 1 MiB; compression and redirects unsupported |
| Processed output | 64 KiB, with construction-time parser/work bounds |
| Search | Zero-based pages, page size 1–20 (default five), query at most 256 UTF-8 bytes |

Admission overflow fails promptly. An admitted refresh may wait for its provider
rate token only within its deadline. A bounded provider cooldown honors
`Retry-After`; it is not a cached claim of absence. There is no background crawler,
negative cache, conditional revalidation, incremental feed, or attachment fetch.
An unrepresentable `Retry-After` pauses that provider until reconstruction instead
of retrying early. L1 maintenance sweeps expired evidence once per second; ordinary
current retrieval never serves beyond the 300-second validation-age limit. Optional
L2 maintenance runs every 60 seconds, with capture retention enforced on reads;
explicit historical retrieval can return captures older than 300 seconds. In-flight payloads have separate bounded
request lifetimes and are not part of the cache-capacity gauge.

Keys distinguish source/provider, dataset, operation, every selector and page,
processor version, and schema compatibility. Fresh-only and stale-permitted
callers share refresh work but evaluate the result independently. A failed refresh
cannot overwrite a good entry or advance its validation time. A stale result is
explicitly marked, and a fresh-only call never silently accepts it.

Each waiter owns its cancellation and deadline. One waiter leaving does not cancel
others; the last waiter cancels the operation. A refresh has its own ten-second
deadline. Generation checks prevent obsolete work from publishing. With L2, cancellation
before commit authorization prevents publication; an authorized disk transaction
may survive a cancelled caller and be reconciled during recovery.
Shutdown stops admission and joins owned jobs before completing.

The memory cache is process-local and empty after restart. Optional L2 lazily
repopulates it from validated retained captures; its separate retention and worker
lifecycle are documented in the [filesystem contract](filesystem-cache.md). In memory-only mode raw evidence expires
with its entry; a digest is not a substitute for retained bytes or a permanent
archive. Multiple processes must not be enabled without the deployment-wide
coordination required by the upstream policy.

## Outbound and parser boundaries

Production HTTP adapters accept only configured HTTPS destinations. DNS results
must pass public-address policy and remain pinned to the validated connection.
Hickory performs asynchronous DNS using startup-loaded system configuration, with
two-second requests, one attempt, one parallel query per lookup, two active requests
per multiplexed connection, and 16 cached responses. Hosts-file lookup is disabled
and names are absolute. No blocking libc resolver job survives caller cancellation.
TLS certificate verification remains enabled. Redirects, implicit environment
proxies, compression, caller URLs, and source-supplied link following are disabled.
The isolated demonstration exception permits only a configured loopback HTTP
address and port; it does not enable arbitrary private-network access.

JSON processing bounds body size, nesting, collections, strings, and work before
publishing an output. Malformed payloads, identity conflicts, and unsupported
formats remain failures. Synthetic fixtures are deliberately fictional and may
not be cited as authoritative legal evidence.

## Operational evidence

The private health listener exports fixed aggregate metrics for cache reuse,
coalescing, upstream attempts, stale responses, failures, retries, and saturation.
Labels do not contain queries, identifiers, URLs, or credentials. Routine tests
use controlled clocks and local mocks; no live legal-data requests are required.
