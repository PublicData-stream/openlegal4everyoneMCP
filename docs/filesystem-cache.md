# Filesystem cache and captured history

This optional L2 stores synthetic source observations beneath the process-local
memory cache. Captures are not provider revision identifiers or assertions about
legal applicability. Source corrections remain possible; immutable local captures
preserve the observed differences within bounded retention.

## Configuration and identity

Enable the single-owner local Linux store explicitly:

```toml
[cache.filesystem]
path = "target/demo/cache"
retention_days = 30
max_bytes = 1073741824
max_snapshots_per_query = 100
```

The configured directory must be dedicated to this service. The child owns its
exclusive lifetime lock; a second server cannot use the same root. Network/shared
filesystems and multiple independent retrieval processes are unsupported. Existing
configurations without this section remain memory-only. A cache configuration
without a registered retrieval source fails startup. Paths are operator inputs,
never MCP arguments. Keep persistent production data outside disposable build
output; the example path is only for the local fictional demonstration.

Logical history keys include the configured source namespace, provider, dataset,
and exact operation/selectors/pagination. The demo namespace hashes its normalized
configured upstream origin, so changing that origin cannot reuse old captures as
the new source. Current lookup additionally requires matching processor and output
schema versions. Listing history may include older processors when the output
schema remains supported. No automatic reprocessing or format migration occurs.

Each immutable occurrence stores source bytes, processed data, capture provenance,
source/processing identity, and digests. A mutable per-query manifest records
ordered history and validation metadata. An unchanged compatible current response
updates validation without rewriting the occurrence. A→B→A creates three captures;
identical content does not establish identity across different query keys. Capture
time and validation time are technical observations, not legal dates.

## Retrieval and publication

The shared application resolves memory, then disk, then upstream. Equivalent
callers share the same resolution flight; disk hits do not acquire provider
concurrency or rate tokens. Existing provider retry/rate limits remain in force.
Freshness remains less than 60 seconds, with stale fallback at most 300 seconds
since validation and only after an eligible transient upstream failure. Disk reads
do not advance these timestamps. A future persisted validation time is never
silently converted to age zero.

A new upstream result succeeds only after durable publication when L2 is enabled.
Disk infrastructure failure is not a cache miss and never triggers an unbudgeted
upstream retry or stale fallback. Storage unavailable, corruption, capacity, and
snapshot-not-retained outcomes are distinct from provider failures.

The isolated `--cache-worker` mode runs in the configured server executable before
server runtime initialization. It uses bounded private pipes, a cleared environment,
and no public listener. Raw evidence is carried as a bounded binary section, not
an expanding JSON byte array. The parent authorizes commit only after staging and
checking the live resolution generation, cancellation and waiters. Publication
synchronizes the new snapshot, atomically installs its manifest, and synchronizes
the containing directory. L1 promotion and successful replies follow the durable
acknowledgement.

Cancellation before commit authorization prevents publication. After authorization,
a cancelled call can leave a valid committed capture: terminating a process cannot
undo an already installed manifest. Such a caller receives no successful result.
Lost acknowledgements and uncertain commits require recovery before admission
resumes; recovery never promotes an uninstalled staged manifest.

The parent owns and supervises the child even when callers disappear. Queued
cancellation is removed locally; active operations get a bounded abort/drain
opportunity before termination. Storage execution has a five-second deadline;
resolution cancellation starts at 20 seconds, including the existing ten-second
upstream budget. Owned reaping/reconciliation may outlive those execution deadlines;
MCP callers remain subject to the independent transport call deadline. Recovery
permits one attempt per incident, at most three restarts
per rolling minute with a one-second cooldown, and a 30-second recovery deadline.
Shutdown interrupts recovery and uses a two-second reap bound. An unreaped child
keeps its lock; no replacement writer starts. Uninterruptible kernel I/O may prevent
confirmed termination, in which case shutdown/readiness reports failure.

## Retention and integrity

Defaults retain captures for 30 days, at most 100 per logical query, within 1 GiB
of accounted file bytes. Fixed safety bounds also limit logical keys to 4,096 and
snapshots to 10,000. File-byte accounting includes metadata, replacement files,
staging, quarantine and orphans until deletion succeeds; directory/filesystem
allocation overhead is additional. Transactions reserve peak space before staging.
Startup also requires 2 MiB of free space within the configured byte budget for
atomic metadata replacement; usable retained capacity is therefore smaller than
the configured ceiling. These are cache retention limits, not a promise of
permanent archival storage.

Retention first removes expired captures and oldest non-head history, then cold
query heads when capacity requires it. An expired head cannot be kept indefinitely
by validation: a later successful fetch creates a new occurrence even if unchanged.
A full store that cannot reclaim enough space rejects publication without updating
validation or L1. Reference removals become durable before files are unlinked.
Interrupted cleanup may leave garbage, never intentionally dangling live references.

An epoch fences L1 invalidation against delayed disk results. Cleanup, isolation
and recovery invalidate memory entries, and results from earlier epochs cannot
repopulate them. Historical reads and response construction enforce retention even
between maintenance sweeps. Normal shutdown clears L1 while preserving L2.

Read bounded files and verify format, full identity, source digest, envelope
integrity and processed-data constraints. Directory-relative nofollow operations,
regular-file and ownership checks, private permissions and hard-link rejection
protect the filesystem boundary. Corrupt objects are isolated within bounded
accounting; a corrupt manifest isolates its query. Successful isolation permits a
normal budgeted refetch for current retrieval. Other history remains available.
Unsupported root formats fail without automatic conversion or deletion of unknown
content. Lowering byte or count limits below already retained totals fails startup
without discarding history; this includes the metadata replacement allowance.
Restore the prior limits until retention frees space.
Digests detect corruption; they do not protect against malicious writers
with the same service privileges or establish legal source authority.

## History and comparisons

`demo_list_snapshots` accepts the complete `query`, optional `cursor`, and `limit`
(default 10, maximum 20). It lists descending occurrence summaries. Cursors bind
query identity and a high-water sequence: appends do not shift later pages and
eviction can leave gaps. No per-client pagination state or upstream request is
created. `demo_get_snapshot` requires that same query plus an exact 64-character
snapshot ID; unavailable captures never substitute current data. Raw evidence is
retained internally, not exposed by these tools.

Historical envelopes carry capture provenance and an explicit historical marker,
not ordinary fresh/stale classification. Older processors are labeled. Each search
page is an independent capture; opening a row displays the record embedded in that
page. An explicit current-record action is required to make a normal Get request.

With both L2 and `[text_diff]`, `demo_compare_record_snapshots` resolves two exact
record snapshots for the same source/record identity on the server. It compares
`title + "\n\n" + body` byte-for-byte as UTF-8 (`title_lf_lf_body_v1`) through the
existing bounded Rust comparison service. Existing NUL, line and size restrictions
still apply. A comparison failure never edits its source captures. Search-page
snapshots are browsable but cannot be passed as record snapshots.

Comparison summaries retain server-derived snapshot origins and processor versions.
Supplied-text inputs cannot assert these origins; editing creates a new supplied-
text result without that association. Comparison originals and their metadata have
the existing ten-minute transient lifecycle, independent of L2 eviction; this does
not imply the original raw source evidence remains available. The record widget
embeds a shared read-only viewer, preserving paging, expiry and handle deletion.

The private metrics endpoint reports bounded aggregate cache/storage reuse,
publication, corruption, eviction, capacity and worker recovery evidence. It does
not use raw queries, identifiers, credentials or source URLs as metric labels.
