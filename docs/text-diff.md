# Supplied-text comparison

This opt-in feature compares two supplied UTF-8 texts with `similar` in Rust and
renders bounded line and character differences in a React MCP App. It does not
retrieve legal records, browse repositories, determine legal equivalence, or interpret amendments.

## Run locally

Install the pinned Rust/Node/pnpm toolchains. Build both widget assets:

```sh
pnpm --dir apps/widget install --frozen-lockfile
pnpm --dir apps/widget build
```

Prepare the disposable TLS files described in the [demo setup](demo.md#local-setup),
without starting its mock upstream. Replace the corresponding-source placeholder
in `deploy/text-diff/server.toml`, then run:

```sh
cargo run --locked -p openlegal-server -- deploy/text-diff/server.toml
```

The additional configuration is:

```toml
[limits]
max_message_bytes = 16777216
max_buffer_bytes = 268435456

[text_diff]
widget_html = "apps/widget/dist/text-diff.html"
```

The feature requires these message/buffer allowances because two 1 MiB inputs can
expand to approximately 12 MiB when JSON-escaped, and HTTP reserves four message
allowances per active request. This profile does not change ordinary-server
defaults. The 256 MiB transport reservation budget and 128 MiB comparison budget
are separate accounting bounds, not a process RSS guarantee. Allow additional
runtime overhead; the integration container uses 1 GiB. Both data transports and
the private health listener remain required.

The edge must admit the same request size: the pinned OxiBelt fixture sets
`routes.limits.max_request_body_bytes = 16777216` only for `/mcp`. Its inherited
10 MiB default is insufficient for the largest JSON-escaped input pair.

The server probes its own executable's internal comparison-worker mode before
listeners bind. No system Git is needed for runtime comparisons. Remove the old
`text_diff.git_path` setting when upgrading: strict configuration parsing rejects
it. Use an immutable executable/container while serving; an incompatible worker
protocol fails closed if an executable is replaced in place.
The comparison HTML must fit 3 MiB after source-offer substitution and its
serialized resource must fit both 6 MiB and half the configured message allowance.
The synthetic browser retains its 1 MiB raw allowance. Both assets bundle script,
styles and license locally, with no external origins allowed by their MCP CSP.

## MCP contract

All four tools share one application service across HTTP and native WebTransport.
Object outputs carry `schema_version: 1`.

| Tool | Input and output |
| --- | --- |
| `compare_texts` | Required `before` and `after` strings; optional `before_label` and `after_label`. Returns a summary with `comparison_id`, expiry, byte/line/newline information, additions/deletions, equality and change-page count. |
| `show_text_diff` | Either `{}`, a before/after pair with optional labels, or `comparison_id` alone. Returns `{schema_version, comparison}`; null comparison opens an empty editor. Only this tool advertises the UI resource. |
| `get_text_diff_page` | `comparison_id`, zero-based `page`, and `view` (`changes`, `before`, `after`). Returns numbered changes or original text chunks, with `total_pages`. |
| `delete_text_diff` | `comparison_id`. Returns `{schema_version: 1, deleted: true}` even when the well-formed handle is already absent. |

The resource URI is `ui://openlegal/text-diff-v1.html`, MIME
`text/html;profile=mcp-app`. Missing/expired handles have the same sanitized read
error. Invalid arguments, unavailable execution, saturation and resource limits
remain distinct existing MCP errors. Worker stderr, private paths and supplied text
are not diagnostics. A summary is not a complete patch: retrieve every changes
page when completeness matters.

## Exactness and paging

Each text permits at most 1 MiB UTF-8, 100,000 LF-delimited lines and 16 KiB per
line. NUL is rejected. Display labels are bounded to 128 UTF-8 bytes and never
become file paths. Empty strings are valid. Whitespace, BOMs, CRLF/LF, lone CR and
final-newline differences are preserved; there is no Unicode normalization or
ignore-whitespace mode.

`similar` 3.2.0 computes Myers line differences over LF-inclusive slices with three
context lines. The adapter emits Git-style unified patches with fixed `before` and
`after` names. Lone CR remains content, and a missing final LF has the standard
`\ No newline at end of file` marker. Different valid alignments, hunk boundaries
and line counts from Git are permitted; byte-identical Git output is not promised.

Rust then computes character differences over each complete replacement block,
before page splitting. Units are Unicode scalar values (`char`), covering ASCII,
CJK and other valid UTF-8. Combining marks, decomposed Hangul and emoji components
can be highlighted separately; no normalization or grapheme clustering occurs.
Pure additions/deletions highlight the entire original line, including CR/LF.
There is no similarity threshold or line-only fallback. A time or resource limit
fails the whole comparison, so successful results include all character metadata.

Each `DiffFragment` adds `inline_changes: [{row_index, ranges: [[start, end], ...]}]`
to the existing version 1 output. `row_index` counts all fragment data rows,
including context, from zero; headers and final-newline markers are excluded.
Every `+`/`-` row has one entry, and context rows have none. Each ordered,
nonoverlapping range is half-open in Unicode scalars of the original line after
its diff prefix, including any CR/LF. Empty range arrays are valid when a changed
line's characters match across a replacement block. The missing-newline marker
distinguishes source LF from the formatting LF added by unified patches.

The widget validates these annotations and renders text nodes using scalar
indices. It does not run a second diff. Changed CR/LF have visible markers;
missing-final-newline indicators remain attached to their data rows. Split view
pairs adjacent removed/added rows in order for presentation only. Older clients
can continue using the unchanged patch fields; the updated widget requires the
new metadata.

Change pages contain at most 400 diff-content lines, 4,096 character ranges and
256 KiB of serialized output including metadata. There are at most 65,536 ranges
per comparison. A single row exceeding the range/page allowance fails the whole
comparison. Page assembly targets 240 KiB to reserve the response envelope. Large
hunks are split between lines; each fragment retains original before/after starts and counts, and final-newline markers stay
attached to their content line. The widget renders bounded fragments with local gutter numbers and labels their
original source ranges.
It does not regenerate differences or expand unchanged source context. Original
text chunks contain at most 32 KiB, split at UTF-8 boundaries; concatenate pages
in order to reconstruct the exact source.

Local UTF-8 files are read with strict decoding and BOM preservation. They are sent
only on Compare. Raw inputs remain separate from textarea display values. Editing
CR-containing input requires an explicit LF-edit transition, so the conversion
becomes an intentional input change. An existing handle can be opened without
recomputation; original text pages are loaded only when editing is requested.

## Retention, deletion and lifecycle

A handle contains 256 random bits and is a bearer capability: anyone receiving it
can read or delete the result. There is no account isolation or list-results API.
Do not place handles in logs, URLs or persistent browser storage. Sharing a handle
also shares deletion authority.

Results expire ten minutes after publication; reads never extend the deadline.
At most 32 comparisons and 128 MiB of accounted originals, patches, indexes and
reservations are admitted. Unexpired entries are not evicted to admit new work.
At most two Rust workers run at once, with a ten-second deadline shortened by the
caller deadline. Capacity is reserved before spawning. The parent kills and reaps
the child on cancellation, deadline, shutdown or protocol failure before releasing
the job permit and its 32 MiB reservation. Process isolation provides killability,
not an operating-system memory sandbox or a peak-RSS guarantee.

Supervised work survives a dropped requesting future only long enough to stop and
reap its worker. Failed/cancelled work does not publish. Inputs travel through pipes
and remain in memory; the comparison feature does not create temporary input files.
Memory-only storage is not a secure-erasure guarantee against host access, swap or
crash dumps.

The private `--text-diff-worker` mode runs synchronously before server logging,
configuration and runtime initialization. Its versioned length-prefixed protocol
uses raw UTF-8 inputs (at most 1 MiB each), an 8 MiB raw patch section and an 8 MiB
JSON annotation section; stderr is capped at 8 KiB and never exposed as MCP
errors. Lengths, EOF, process status, source correspondence and annotation bounds
are checked. Both pipe input/output and process completion are supervised to avoid
pipe deadlocks. The worker does not spawn descendants.

Clear is disabled while creation is unresolved. It invalidates page requests and
deletes the current result before reporting success; deletion failure leaves a
retry action. Recomparison deletes the previous widget result before creating its
replacement. Deletion prevents subsequent reads but cannot retract content already
returned to another client. Closing a widget is not a reliable deletion signal;
fixed expiry remains the fallback.

`delete_text_diff` and its `text.diff.delete` alias are built-in mutation exceptions
alongside the temporary attachment upload/deletion tools described below. Their annotations declare
read-only false, destructive true, idempotent true and open-world false. Generic
extension registration still requires read-only behavior. This exception does not
authorize repository changes, provider writes, or general administration.

## Validation and limitations

The [contributor checks](../CONTRIBUTING.md#testing-and-ci) apply.
See the [Rust worker migration review](similar-review.md) for implementation evidence. Run the complete
edge gate with both built assets:

```sh
DEMO_WIDGET_HTML=apps/widget/dist/index.html \
TEXT_DIFF_WIDGET_HTML=apps/widget/dist/text-diff.html scripts/test-oxibelt.sh
```

Fixtures are synthetic or supplied text; routine tests never contact legal providers.
Browser tests use a local MCP Apps host. Passing these gates does not establish live
ChatGPT rendering or a particular host's support for maximum-size tool arguments.
No deployment or publication is included.

## Comparisons from retained snapshots

With PostgreSQL + BlobStore persistence enabled, the record history tool can resolve two retained
snapshots of the same record and call the existing comparison service. The exact
projection is title, two LF bytes, then body. See the [history contract](persistence.md#history-and-comparisons).
The optional summary `origin` is server-derived and retained with the transient
comparison. `compare_texts` does not accept this field. Editing creates a supplied-
text comparison without a verified historical association. Original source payloads
remain governed by L2 retention; comparison text and metadata expire after ten
minutes even if the source snapshots are already evicted.

## Canonical text tools and temporary attachments

The canonical API adds `text.diff`, `text.apply_patch`, `text.diff.show`,
`text.diff.page`, and `text.diff.delete`. The four original tool names remain
available with their original input/output shapes; the namespaced show/page/delete
helpers have the same contracts. `text.diff` accepts `before` and `after` as UTF-8
strings or `{ "attachment_id": "<handle>" }`, plus optional display labels. Its
result contains `comparison`, a sealed `patch` attachment, and a deterministic
`explanation` of the line/scalar algorithm described above. The patch attachment
contains one complete unified patch; concatenating display fragments is not a
substitute. Equal texts export an empty no-op patch. Comparison and patch handles
are independently deletable and expire ten minutes after their publication.

`text.apply_patch` accepts `target` and `patch`, each either inline or an attachment
reference of the corresponding kind. It returns `result` (a sealed text attachment)
and `info` (text byte/line information). Read the result through attachment pages;
its maximum escaped representation need not fit in one tool response. Applying a
patch never writes a file, changes a database object, or modifies an input
attachment. A separate comparison can visualize the target/result difference.

Patch application accepts a single ordinary unified patch with `---`/`+++` headers,
optional `diff --git` and `index` preamble, ordered hunks and standard missing-final-
newline markers. Header names are ignored as paths. Every context/deletion line
must match the target at exactly the declared location, including CR and LF bytes.
There is no fuzz, offset search, whitespace repair, reverse mode or partial success.
Mode changes, rename metadata, binary/combined/multi-file patches and malformed
counts are rejected. Empty patches preserve the target. Invalid syntax and target
mismatches return invalid-input errors; size/work limits remain resource errors.
The target and result retain the existing 1 MiB/line-count/line-size text limits;
patch inputs permit up to 8 MiB. This parser is a pure transformation in normalization;
the same killable two-worker pool enforces the ten-second/caller deadline.

Three built-in tools manage memory-only attachments:

| Tool | Contract |
| --- | --- |
| `text.attachment.upload` | First call: `kind` (`text` or `patch`), `total_bytes`, `chunk`, `final`, and optional zero `offset`. Later calls: `attachment_id`, byte `offset`, `chunk`, `final`; omit kind/total. |
| `text.attachment.read` | `attachment_id`, optional zero `offset`; returns `attachment` metadata, `text`, `next_offset` and `complete`. Only sealed attachments are readable. |
| `text.attachment.delete` | `attachment_id`; repeated deletion of a well-formed absent handle succeeds. |

Chunks contain at most 32 KiB of UTF-8 and offsets must be scalar boundaries.
Upload sequentially. Exact replays of committed bytes succeed; gaps and conflicting
replays fail. `final: true` seals only when the declared length has been reached;
sealed bytes cannot change. Empty attachments are an empty final first upload.
Text sealing checks the comparison text limits. Patch sealing checks UTF-8/NUL and
byte limits; the application validates patch syntax when it is used.

There are at most 64 attachment handles, including unfinished uploads. Their reserved
capacity and any input leases share the 128 MiB comparison accounting allowance;
comparison/job slots remain capped at 32. Declared upload capacity is reserved before
acceptance. An attachment expires ten minutes from the initial upload; append,
seal and read do not extend expiry. Deletion/expiry prevents new reads, while an
operation that already acquired the bytes may finish; pinned bytes stay accounted
until released. Restart discards all attachments. An initial response lost before
its handle reaches the caller can leave an unreachable upload until fixed expiry.

Handles authorize both read and deletion, without user/account isolation. Never log
handles or source contents. Upload and deletion are narrow built-in mutation
exceptions; ordinary extension registration stays read-only. Upload is marked
non-idempotent because retrying initial creation may allocate another handle.

The widget's patch panel loads local UTF-8 target/patch files with strict decoding,
keeps CR/BOM bytes, uploads only on Apply patch, reads the complete result in bounded
chunks and deletes its temporary handles. Clear patch retries any failed cleanup.
The local result preview remains until cleared. A closed host may prevent cleanup;
fixed expiry remains the fallback. No live ChatGPT compatibility is established.
