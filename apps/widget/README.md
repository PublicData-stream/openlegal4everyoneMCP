# MCP Apps widgets

Three optional React MCP Apps resources: the synthetic record browser for the
upstream demo, the [supplied-text comparison](#supplied-text-comparison), and the
[legal corpus browser](#legal-corpus-browser). Each calls the server only through
the host bridge and makes no direct network requests. In the record browser,
search and detail actions call the synthetic demo tools. Source text is rendered as text, with a synthetic label and each
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

The build emits three self-contained assets, each checked against a build-time cap
in `scripts/build.mjs`:

| Asset | Widget | Build cap | Configured by |
| --- | --- | --- | --- |
| `apps/widget/dist/index.html` | Synthetic record browser | 1 MiB | `[demo].widget_html` |
| `apps/widget/dist/text-diff.html` | Text comparison | 3 MiB | `[text_diff].widget_html` |
| `apps/widget/dist/database.html` | Legal corpus browser | 3 MiB | `[database].widget_html` |

The server separately enforces a raw limit on the configured file and again after
source-offer substitution (`apps/server/src/widget.rs`): 1 MiB for the record and
legal corpus browsers and 3 MiB for the text comparison. A database asset between those two sizes passes
the build but fails server startup.
Build output, dependencies and browser artifacts are ignored. The server reads
each configured asset once during startup; ordinary Rust builds do not invoke
frontend tools. See the repository configurations for asset paths. Do not publish
the offline test harness as a widget resource.

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
without following them. It also serves the comparison host at `/diff` and the
legal corpus host at `/database`, both with fictional fixtures.
The harness does not establish live ChatGPT compatibility.

## Contract

This section describes the synthetic record browser. Search calls `demo_search_records` with `source`, literal `query` (at most 256
UTF-8 bytes), zero-based `page`, `page_size: 5`, and `fresh_only`. Detail calls
`demo_get_record` with the selected record's source and ID, preserving namespaced
identity. Both return `structuredContent` envelopes containing `data`, `synthetic:
true`, and `freshness: {state, age_seconds}`. Search data contains `records`, `page`,
`page_size`, and `total`; detail data is a record. Every record includes `source`
(`layout_a` or `layout_b`), `id`, `title`, `body`, and `synthetic: true`.

Initial host results from `demo_show_records` contain
`{records: [recordEnvelope, ...], synthetic: true}` so mixed-source records retain
individual freshness. Only this render tool advertises the versioned
`ui://openlegal-demo/records-v1.html` resource through `_meta.ui.resourceUri`; the MIME type is
`text/html;profile=mcp-app`. CSP resource and connection allowlists stay empty.
The source button uses `App.openLink` only after a user click and only when the
host advertises `openLinks`. The widget revalidates the HTTPS source metadata
before display or navigation and always provides a selectable URL, including
when the bridge is unavailable or navigation is refused. No direct navigation
or network request is performed by the widget.
With persistent history enabled, the record browser also calls
`demo_list_snapshots`, `demo_get_snapshot` and `demo_compare_record_snapshots`,
then pages and deletes the resulting comparison with `text.diff.page` and
`text.diff.delete`; see the [persistence contract](../../docs/persistence.md#history-and-comparisons).
See [official OpenAI UI documentation](https://developers.openai.com/plugins/build/chatgpt-ui)
for the MCP Apps bridge contract and the separate manual host acceptance step.

## Dependency admission

All direct dependencies use exact versions and the pnpm lockfile records registry
integrity. The runtime is React/React DOM (MIT), chosen for the approved React UI,
and the official `@modelcontextprotocol/ext-apps` SDK (MIT), chosen instead of a
custom postMessage protocol. The SDK handles the host handshake and RPC boundary;
each widget additionally validates bounded result shapes and invokes only the
tools named in its contract section. The SDK is bundled locally without runtime CDN
loading. Its client/core protocol dependencies declare Apache-2.0 and retain the
upstream license-transition notice, including legacy MIT terms.

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

The 2026-10-23 maintenance update pins the Apps SDK to 2.0.3 and its `client`
and `core` peers to 2.4.0. Range-scoped pnpm overrides also pin Zod to 4.6.7,
covering [GHSA-mrhx-c7xc-m3w5](https://github.com/advisories/GHSA-mrhx-c7xc-m3w5)
and the client's
[GHSA-6prh-2h8m-c8cw](https://github.com/modelcontextprotocol/typescript-sdk/security/advisories/GHSA-6prh-2h8m-c8cw).
These compatible stable releases retain the existing React and build-tool versions.
Exact release-age exclusions admit the two SDK 2.4.0 packages published on the
maintenance date so the graph uses the latest stable SDK, including the patched
client; other packages retain pnpm's release-age checks. Owner: PiQuark6046; review
the two exact exclusions by 2026-10-30.
Sources remain the official npm SDK and Zod packages. The Apps SDK and Zod declare
MIT; client/core declare Apache-2.0. `THIRD_PARTY_NOTICES.md` preserves the complete
upstream SDK license and transition text. No new install scripts are enabled.
The widget uses the SDK's host-mediated postMessage protocol and does not configure
OAuth providers, HTTP authorization, or stored OAuth credentials. SDK 2.4's optional
OAuth-refresh and skills APIs are not enabled by this update. Existing type, model,
build, browser, advisory and license checks cover admission of the changed protocol
packages.

## Supplied-text comparison

`text-diff.html` is an independent MCP App and does not require the synthetic
upstream demo. It accepts pasted text or UTF-8 files, optional before/after labels,
and compares through the host-mediated `text.diff` tool. Each source is bounded
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

`text.diff.show` opens a blank editor, a supplied pair, or an existing handle.
Version 1 show results contain `{schema_version: 1, comparison: summary | null}`;
`text.diff` returns `{schema_version: 1, comparison, patch, explanation}`. The
widget validates this envelope and its sealed patch attachment, then reads the
nested comparison summary. Snapshot comparison results still contain the summary
directly. Summaries contain the opaque
comparison handle, expiry, source metadata, added/deleted line counts, equality,
and change-page count. Existing handles can load their exact before/after source
chunks for editing. The widget reassembles them in order and checks source size
before exposing the editor.

`text.diff.page` takes `{comparison_id, view, page}`, where view is `changes`,
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
explains that the handle grants access to the retained text. The widget promptly
deletes `text.diff`'s unused patch through `text.attachment.delete`; deleting the
comparison does not delete its patch attachment. Failed patch cleanup keeps the
comparison usable and retains the attachment for retry of **Clear**. **Clear**
awaits `text.diff.delete` and outstanding attachment deletions before reporting
success, records individual successful deletions, and keeps failed deletions
retryable. Recomparison resolves outstanding cleanup and deletes the previous
comparison handle before submitting the next pair. Clear is disabled during a
comparison, including after cancellation until the actual RPC settles. A late
successful comparison and patch handles are deleted before Clear is re-enabled;
cleanup failure retains each failed handle for a retry of Clear. Stale successful
responses and rejected historical association metadata also trigger cleanup.
Closing the widget or losing the host response can prevent cleanup, leaving server expiry as the fallback. Pending page results
cannot restore content after Clear. Input edits
leave the last result labelled **Previous comparison** until recomparison succeeds.
The widget does not persist sources or handles in browser storage. Host
conversation retention is outside the widget's server-deletion operation.

The offline comparison harness is at `/diff`. It exercises the actual React diff
renderer through the SDK bridge, with synthetic paginated patches, source chunks,
errors, expired handles, and deletion failures. Tests cover local gutter allocation
near source line 100,000, exact Unicode/CR/BOM handling, original source loading,
responsive layouts, inert HTML-like content, cancellation, and late-page rejection.
This is not a live ChatGPT integration test. The former `compare_texts`,
`show_text_diff`, `get_text_diff_page`, and `delete_text_diff` names are removed;
there are no compatibility wrappers. See the [migration mapping](../../docs/text-diff.md#canonical-text-tools-and-temporary-attachments).

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

## Legal corpus browser

`database.html` is the legal corpus MCP App, registered as
`ui://openlegal/database-v1.html` with MIME `text/html;profile=mcp-app` when
`[database]` is configured. Only `database.show` advertises it. The browser searches
through `database.query` or `database.rg`, reads paged content with
`database.get`, lists checkpoints with `database.history`, compares two
checkpoints with `database.diff`, and pages and deletes that comparison with
`text.diff.page` and `text.diff.delete`. It displays freshness, collection notices
and the provider admission diagnostics described in the
[server contract](../../docs/server.md#current-provider-admission-diagnostics);
it does not poll automatically. Tool semantics are in the
[legal corpus contract](../../docs/database.md). The offline `/database` harness
uses fictional records and does not establish live provider or ChatGPT behavior.
