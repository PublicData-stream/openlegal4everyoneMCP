# MCP Apps widgets

An optional React MCP Apps resource for the synthetic upstream demo. Search and
detail actions call the server through the host bridge; the widget makes no direct
network requests. Source text is rendered as text, with a synthetic label and each
record's freshness snapshot when returned, never a continuously updated current
freshness claim. Detail views expose bounded source reference, payload digest,
processor version, and retrieval/validation timestamps in a collapsed section. Tool errors are sanitized and malformed results rejected.

First-party widget code is licensed under **AGPL-3.0-only**. Persistent notices
state copyright, redistribution terms, and absence of warranty; an accessible
collapsed panel includes the complete root license. Dependency notices remain
in the bundled JavaScript.

## Build and checks

Use Node 24.x (verified with 24.21.0) and the package-manager pin in `package.json`.
From the repository root:

```sh
pnpm --dir apps/widget install --frozen-lockfile
pnpm --dir apps/widget typecheck
pnpm --dir apps/widget test
pnpm --dir apps/widget build
pnpm --dir apps/widget exec playwright install --with-deps chromium
pnpm --dir apps/widget test:browser
pnpm --dir apps/widget check:dependencies
```

The build emits self-contained `apps/widget/dist/index.html`, capped at 1 MiB, and
`apps/widget/dist/text-diff.html`, capped at 3 MiB.
Build output, dependencies and browser artifacts are ignored. The server reads
this asset once during explicitly configured demo startup; ordinary Rust builds
do not invoke frontend tools. See the repository demo configuration for its asset
path. Do not publish the offline test harness as the widget resource.

Rebuild the widget when adopting source offers. The build contains exactly one
inert `openlegal-source-url` metadata placeholder. Server startup substitutes its
required `[source] url` using HTML attribute escaping and enforces the resource
size limit after substitution. Old builds without the placeholder fail startup.
Operators must keep that HTTPS URL available without charge and supply the
corresponding source for the running server and widget, including modifications
and necessary build instructions. The widget does not host or fetch source archives.

`pnpm --dir apps/widget harness` serves a deterministic, local-only simulated host
at `http://127.0.0.1:4173`. Build first. It uses the real SDK `AppBridge`, a sandboxed
iframe, and in-memory synthetic responses, with no MCP server or credentials.
Browser tests cover bridge requests, source selection, pagination, keyboard detail
navigation, text rendering, freshness, malformed data, errors, loading, licensing,
and source offers with successful, unsupported, disconnected, and refusing hosts.
The harness inserts a fictional HTTPS source URL and records navigation requests
without following them.
The harness does not establish live ChatGPT compatibility.

## Contract

Search calls `demo_search_records` with `source`, literal `query` (at most 256
UTF-8 bytes), zero-based `page`, `page_size: 5`, and `fresh_only`. Detail calls
`demo_get_record` with the selected record's source and ID, preserving namespaced
identity. Both return `structuredContent` envelopes containing `data`, `synthetic:
true`, and `freshness: {state, age_seconds}`. Search data contains `records`, `page`,
`page_size`, and `total`; detail data is a record. Every record includes `source`
(`layout_a` or `layout_b`), `id`, `title`, `body`, and `synthetic: true`.

Initial host results from `demo_show_records` contain
`{records: [recordEnvelope, ...], synthetic: true}` so mixed-source records retain
individual freshness. Only this render tool advertises the versioned `ui://`
resource through `_meta.ui.resourceUri`; the MIME type is
`text/html;profile=mcp-app`. CSP resource and connection allowlists stay empty.
The source button uses `App.openLink` only after a user click and only when the
host advertises `openLinks`. The widget revalidates the HTTPS source metadata
before display or navigation and always provides a selectable URL, including
when the bridge is unavailable or navigation is refused. No direct navigation
or network request is performed by the widget.
See [official OpenAI UI documentation](https://developers.openai.com/plugins/build/chatgpt-ui)
for the MCP Apps bridge contract and the separate manual host acceptance step.

## Dependency admission

All direct dependencies use exact versions and the pnpm lockfile records registry
integrity. The runtime is React/React DOM (MIT), chosen for the approved React UI,
and the official `@modelcontextprotocol/ext-apps` SDK (MIT), chosen instead of a
custom postMessage protocol. The SDK handles the host handshake and RPC boundary;
the widget additionally validates bounded synthetic result shapes and invokes only
the two read-only demo data tools. The SDK is bundled locally without runtime CDN
loading. Its protocol dependencies are also MIT licensed.

TypeScript and Playwright (Apache-2.0) provide static checks and a real Chromium
bridge test; esbuild (MIT) replaces a larger development server framework and
produces bounded HTML assets. Type declarations are MIT. Transitive tools also
include ISC packages. Upstream maintained npm releases are pinned; updates require
review and rerunning the checks above. Only esbuild's platform-binary installation
script is enabled in `pnpm-workspace.yaml`. Playwright's browser download is an
explicit setup command. No runtime dependency install scripts are enabled.

`check:dependencies` checks current npm advisories for the entire installed graph
and enforces the reviewed license set. Advisory-network failure fails the check;
it is not interpreted as a clean audit. The lockfile is the reproducible baseline,
not evidence that future advisories cannot arise.


## Supplied-text comparison

`text-diff.html` is an independent MCP App and does not require the synthetic
upstream demo. It accepts pasted text or UTF-8 files, optional before/after labels,
and compares through the host-mediated `compare_texts` tool. Each source is bounded
at 1 MiB UTF-8, 100,000 LF-delimited lines, and 16 KiB per line excluding LF but
including any preceding CR. Labels require 1–128 UTF-8 bytes without controls. Files
are decoded with fatal UTF-8 validation, preserving a leading BOM; NUL and invalid
Unicode are rejected. Supplied text is not trimmed or normalized.

Browser textarea editing normalizes CR line endings. The widget keeps the original
string separately and makes CR-containing previews read-only. **Edit before/after
as LF** is the explicit conversion step; comparing without that step submits the
original string. A UTF-8 file can replace either source. Clipboard text is captured
explicitly, without an implicit newline conversion; pasting into a CR-containing
preview requires the explicit LF edit step first. Server metadata reports byte and
line counts, CRLF/LF/bare-CR counts, BOM, and final-newline status.

`show_text_diff` opens a blank editor, a supplied pair, or an existing handle.
Version 1 show results contain `{schema_version: 1, comparison: summary | null}`;
`compare_texts` returns the versioned summary directly. Summaries contain the opaque
comparison handle, expiry, source metadata, added/deleted line counts, equality,
and change-page count. Existing handles can load their exact before/after source
chunks for editing. The widget reassembles them in order and checks source size
before exposing the editor.

`get_text_diff_page` takes `{comparison_id, view, page}`, where view is `changes`,
`before`, or `after`, and pages are zero-based. Results include `schema_version: 1`,
the same identity/view/page, `total_pages`, a `fragments` array, and source `text`
only for source pages. Change fragments contain a standalone Git-style hunk and its
original before/after starting lines and counts. Each fragment includes
`inline_changes: [{row_index, ranges: [[start, end], ...]}]`, supplied by Rust.
Row indices count all data rows (context, removed, added), excluding headers and
final-newline markers. Each changed row has exactly one entry, which may contain
no ranges. Ranges are half-open Unicode scalar offsets in its original source line,
including any CR/LF and excluding the patch prefix. The widget rejects missing,
invalid, duplicate or out-of-bounds metadata, more than 4,096 ranges per page,
and pages exceeding 400 rows or 256 KiB serialized output.

A focused React renderer applies these authoritative ranges using `Array.from`;
it never recalculates line or character differences in JavaScript. Each fragment
uses small local gutter numbers with its original source ranges displayed above.
Split view pairs adjacent removed/added lines in source order for presentation
only. CR characters and changed LF characters use visible symbols; Git
final-newline markers are retained. All supplied text is rendered as React text
nodes, without HTML or syntax highlighting. Wide screens default to split view,
narrow screens to unified; the layout selector overrides either. Source tabs expose
exact bounded chunks, which can divide a line. This is a text comparison, not a
claim about legal equivalence or revisions.

Comparisons are retained server-side for up to ten minutes. The visible notice
explains that the handle grants access to the retained text. **Clear** awaits
`delete_text_diff` and keeps a failed deletion retryable. Recomparison deletes the
previous handle before submitting the next pair. Clear is disabled during a
comparison, including after cancellation until the actual RPC settles. A late
successful handle is deleted before Clear is re-enabled; cleanup failure retains
the handle for a retry of Clear. Closing the widget or losing the host response
can prevent cleanup, leaving server expiry as the fallback. Pending page results
cannot restore content after Clear. Input edits
leave the last result labelled **Previous comparison** until recomparison succeeds.
The widget does not persist sources or handles in browser storage. Host
conversation retention is outside the widget's server-deletion operation.

The offline comparison harness is at `/diff`. It exercises the actual React diff
renderer through the SDK bridge, with synthetic paginated patches, source chunks,
errors, expired handles, and deletion failures. Tests cover local gutter allocation
near source line 100,000, exact Unicode/CR/BOM handling, original source loading,
responsive layouts, inert HTML-like content, cancellation, and late-page rejection.
This is not a live ChatGPT integration test.

The comparison renderer is first-party React code. Removing
`@git-diff-view/react` also removes its browser diff engines, highlighting graph,
and version-specific BSD license exceptions. Rust `similar` owns both line and
Unicode scalar character comparison. The resource retains its separate 3 MiB
ceiling and the existing demo's 1 MiB cap, with no CDN or Worker loading and empty
external-origin CSP metadata. [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md)
retains notices for the current runtime packages; bundled JavaScript also retains
license comments. Updates must rerun model/browser tests, resource-size checks,
current npm advisories, and installed license checks. No new package install
scripts are enabled.
