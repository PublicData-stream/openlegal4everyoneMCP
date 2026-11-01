# Diagnostic lookup deployment, 2026-11-01

Later change: [corpus Blob SSD lookup cutover and two-hour observation](deployment-ssd-lookup-2026-11-01.md).
The earlier checkpoint below is preserved.

This record captures the lookup acceptance completed at **07:51:14 UTC** against
signed source [`eef8c514d065d062ad6aaedebbfd1596f135597f`](https://github.com/PublicData-stream/openlegal4everyoneMCP/commit/eef8c514d065d062ad6aaedebbfd1596f135597f),
version `0.1.0-build.eef8c514`. Current procedures belong to the
[operator runbook](deployment-kubernetes.md) and [persistence contract](persistence.md).
The [diagnostics review](reviews-storage-health-2026-11-01.md) covers the runtime
change; the [earlier recovery record](recovery-2026-10-29.md) preserves prior
failures. This is a diagnostic serving deployment, with collection quarantined.
The read-only storage follow-up below was captured at **08:05 UTC**.

## Release and runtime evidence

| Gate | Result at this checkpoint |
| --- | --- |
| Exact-source CI | [Run 41262133382, attempt 1](https://github.com/PublicData-stream/openlegal4everyoneMCP/actions/runs/41262133382/attempts/1): all 16 jobs succeeded, including native AMD64/ARM64 checks; PostgreSQL executed 204 tests with zero failures or ignored tests. |
| Signed candidate publication | [Run 41265427024, attempt 1](https://github.com/PublicData-stream/openlegal4everyoneMCP/actions/runs/41265427024/attempts/1): publication for signed tag `0.1.0-build.eef8c514` succeeded, reusing the successful exact-source CI. |
| Image retrieval | Five platform-image Docker pulls completed anonymously. Cached layers could be reused; manifest/config/source/platform checks were separately verified. |
| Actual serving runtime | Linux/AMD64 node, one ready serving Pod, one server restart and zero init-container restarts. CRI selected config/rootfs metadata matched the published AMD64 image for both the running server and completed `network-settle` init container. The accepted lookup checks followed the restart; they do not establish stability. |
| Private and public identity | Internal `/live` and `/ready` returned 200. Both public hosts and both protocol revisions returned the EEF source identity; the connected MCP `server_info` also returned EEF. |

Published server index:
`sha256:2eee53981cc8a8a043dec9ad53b4ecac59535f9299b75f93afc779bf50b065da`.
Its verified AMD64 leaf was
`sha256:9943ce441bcf842130523228394db9b3aa688196e017b9aa6355d800b810e052`,
and the actual selected config was
`sha256:0052bc29a7f303d3e2715d7e4d276bff90a4e00274f10bc8307d3d4d75927255`.
The index digest alone was not used as native execution evidence.

## Preservation and restore

The authorized maintenance stopped serving and kept collection stopped. A
coordinated snapshot covered PostgreSQL, cache blobs, corpus blobs, corpus index,
MeCab-Ko data, configuration and HTTP assets. All seven source snapshots were
retained after the drill.

An isolated PostgreSQL 18 restore used seven disposable clones, no network or
published ports, and the existing pinned database image. Verification checked
schema 27 and its compiled migration checksums, the original storage identity,
all 28 retained-table fingerprints, 18,740 captures, all 50,103 physical
generations and permanent references, index metadata and the ancillary snapshot
trees. The restored source observations were covered by the retained-table
fingerprints. Separately, current-preservation checks examined source-observation
representation and provenance; they were not a global audit of legal meaning.

The verifier's actual OS exit was zero, all owned command processes and tagged
SQL work were closed, and the disposable container and seven clones were
removed. Journals and the empty restore parent were retained. Separately, a
stopped-source reopen after the snapshot matched its complete current manifests,
retained-table fingerprints and source-observation results.

Normal SQL limits and default/cache blob allowances remained unchanged. The
operational corpus allowance remained 30 seconds. Separate read-only operator
audits used finite 30-second statements and 35-second transactions; they did not
change product connection settings, durability or acknowledgement rules.

## Lookup acceptance

A new external phase and matrix verified all **16** combinations of two hosts,
two protocol revisions, IPv4/IPv6 and Streamable HTTP/native WebTransport. HTTP
cases also checked the retained fixed-capture body hash. The external runner was
separate from the production SSH host; actual peer addresses and raw result
hashes were checked. No provider collection was initiated.

The operating consumer accepted these actual bytes within its unchanged arrival
bound, then completed a fresh 120-second safety assessment. Its decision was
`preserve_serving`, with collection quarantined and provider review required.
The final checks again matched all retained-table fingerprints and provider
state, closed SQL work, and verified actual target resources and native runtime.
The verifier's actual OS exit was zero; all 59 files in its closed journal mirror
were checked against their original bytes.

The accepted completion receipt SHA-256 was
`1babc0400adb19b59de8ad60724fb4166d803d9c717cdb0e1a45e160c8d17316`;
the actual external raw manifest SHA-256 was
`ca5c57d7d9a047d8b6915baa5670e84d42a5d8ab9b97fbd4e05e2cc3267155b8`.
These identify the final attempt, not an earlier matrix.

## Earlier failures and remaining gates

At 07:33:19 UTC, the new diagnostics recorded `blob_health` exceeding its
30,000-ms allowance (`local_deadline`, elapsed 30,001 ms), followed by
`corpus_health` failing in the blob phase and the corpus runtime failing its
health stage with `StorageUnavailable`. The server exited with status 1 at
07:33:31 and restarted at 07:33:32; the watcher observed Ready again at
07:33:45. This identifies the failing dependency, not the underlying syscall or
device cause. Measurement starts in the deadline supervisor after the blocking
job is spawned; it may include remaining queueing, execution, completion
observation and the failure-publication lock delay. It is neither the full job
duration nor an fsync-duration measurement.

At 08:05 UTC, a read-only sample fenced to the current serving container, Pod
identity and process start ticks found a blocked worker in syscall 74 (`fsync`),
with the kernel stack passing through `zfs_fsync` and `zil_commit`. This is an
observation of the restarted process, not a trace of the earlier failed probe.
ZFS reported the mirror online with no known data errors and zero read, write
or checksum errors. Recent host I/O pressure was elevated; the sampled cache,
corpus and PostgreSQL datasets all retained `sync=standard` and
`logbias=latency`. These observations support investigating ZFS commit latency
and competing I/O, but do not establish hardware failure or attribute the
earlier deadline to a particular syscall. No storage properties were changed.

Failed operator attempts remained failed. Corrections addressed effective
read-only ZFS snapshot detection, finite bulk-audit time, this client's
`--patch-file=/dev/stdin` convention, an ambient Docker setting rejected by the
existing host guard, and distinct hashes for the static descriptor and its
canonical Journal serialization. Source/input checks and independent reviews
covered the separate corrected executors. The final lookup executor passed 39
offline tests; the proof installer included eight regressions exercising the
actual Journal serialization. These fixtures are separate from live acceptance.

An earlier external matrix passed all 16 cases, but delivery to its operating
consumer failed. Its expired phase and timestamps were preserved. The accepted
attempt used a new phase, new raw results and the original time bounds. The
original cache launcher's missing OS status remains unknown, with its cache
postcondition separately qualified. The earlier failed observation and strict
167-reference comparison remain failed; exact historical column attribution
remains unresolved. Current preservation checks do not replace those failures.

Serving was restored with the scheduler at zero. Existing usage and the unlocked
continuous-owner uncertainty were preserved. No additional recovery role,
credential, Secret or collection Job was created, and collection was not resumed.
The new image supplies failure-origin diagnostics; the storage failure's root
cause and a durable storage repair remain unestablished. Two-hour stability,
complete provider cloning and reuse-rights resolution, native ChatGPT UI,
reboot persistence, hardened-cluster/document-worker and load/capacity acceptance
remain separate outstanding gates.

## Record review

An independent Codex reviewer checked the actual acceptance receipts, their
hashes and sizes, native image identity, valid guard at completion, relative
links and documentation scope. The review identified the server restart and
separated restored-table equality from current source-observation checks; this
record includes those corrections. Separate anonymous-pull and hosted
PostgreSQL job evidence supports the corresponding release rows.

Documentation validation used `git diff --check`, `git status --short`, manual
inspection of the newly added record, and a relative-link check. No Rust checks
were rerun for this documentation-only change; the exact-source hosted checks
above describe the deployed runtime source.
