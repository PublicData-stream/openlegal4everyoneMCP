# Synthetic record browser

An optional React MCP Apps resource for the synthetic upstream demo. Search and
detail actions call the server through the host bridge; the widget makes no direct
network requests. Source text is rendered as text, with a synthetic label and each
record's freshness snapshot when returned, never a continuously updated current
freshness claim. Detail views expose bounded source reference, payload digest,
processor version, and retrieval/validation timestamps in a collapsed section. Tool errors are sanitized and malformed results rejected.

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

The build emits one self-contained `apps/widget/dist/index.html`, capped at 1 MiB.
Build output, dependencies and browser artifacts are ignored. The server reads
this asset once during explicitly configured demo startup; ordinary Rust builds
do not invoke frontend tools. See the repository demo configuration for its asset
path. Do not publish the offline test harness as the widget resource.

`pnpm --dir apps/widget harness` serves a deterministic, local-only simulated host
at `http://127.0.0.1:4173`. Build first. It uses the real SDK `AppBridge`, a sandboxed
iframe, and in-memory synthetic responses, with no MCP server or credentials.
Browser tests cover bridge requests, source selection, pagination, keyboard detail
navigation, text rendering, freshness, malformed data, errors, and loading.
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
produces one bounded HTML asset. Type declarations are MIT. Transitive tools also
include ISC packages. Upstream maintained npm releases are pinned; updates require
review and rerunning the checks above. Only esbuild's platform-binary installation
script is enabled in `pnpm-workspace.yaml`. Playwright's browser download is an
explicit setup command. No runtime dependency install scripts are enabled.

`check:dependencies` checks current npm advisories for the entire installed graph
and enforces the reviewed license set. Advisory-network failure fails the check;
it is not interpreted as a clean audit. The lockfile is the reproducible baseline,
not evidence that future advisories cannot arise.
