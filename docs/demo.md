# Synthetic record browser

This opt-in demonstration exercises the upstream extension framework, both MCP
transports, and an interactive React MCP Apps resource. All records are fictional.
It does not connect a legal-data provider or supply legal citations.

## Local setup

Use the repository Rust toolchain, Node 24, and the pnpm version pinned in
`apps/widget/package.json`. From the repository root:

```sh
pnpm --dir apps/widget install --frozen-lockfile
pnpm --dir apps/widget build
mkdir -p target/demo
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -days 2 \
  -subj /CN=localhost -addext 'subjectAltName=DNS:localhost,IP:127.0.0.1' \
  -keyout target/demo/key.pem -out target/demo/cert.pem
cargo run --locked -p openlegal-adapters --example mock_upstream -- 127.0.0.1:8081
```

Leave the mock running, then in another terminal:

```sh
cargo run --locked -p openlegal-server -- deploy/demo/server.toml
```

Every configuration requires `[source].url`; replace the example placeholder with
the actual source offer before hosting. See [source offers](server.md#source-offers-and-migration).
The server configuration without `[demo]` still exposes only its
registered foundation tools. The demo additionally requires its bounded local
widget asset before listeners bind. Paths in the example are relative to the
repository root. Generated HTML, keys, and certificates are disposable and ignored.

## Tools and display

- `demo_search_records`: choose `layout_a` or `layout_b`, a literal query,
  zero-based page, page size (default five, maximum 20), and optional `fresh_only`.
- `demo_get_record`: pass the source and exact record ID from a search result.
- `demo_show_records`: pass `records: [{source, id}, ...]` to open the browser;
  an empty list opens its initial search interface.

Search and detail results include structured data, source provenance, processor
version, payload digest, retrieval and validation times (Unix seconds), and explicit
freshness. The render tool preserves those values separately for every record.
Source IDs are distinct across the two layouts even when display titles match.

The widget uses host-mediated MCP tool calls; it does not fetch source URLs. It
supports search, pagination, details, and empty/error states. Source bodies are
rendered as text. Ordinary clients can use the data tools without rendering UI.

The widget resource is `ui://openlegal-demo/records-v1.html`, with MIME type
`text/html;profile=mcp-app`. Only the render tool advertises `_meta.ui.resourceUri`.
The server lists and reads immutable registered resources; resource URIs never
become filesystem paths or outbound requests. The resource includes its JavaScript
and styles and declares no external network/resource/frame origins.

Its persistent license notice identifies AGPL-3.0-only, copyright, redistribution
terms and absence of warranty. An accessible collapsed panel contains the full
license text. The source button requests that the MCP Apps host open the configured
corresponding-source URL only when clicked; there is no automatic navigation or
direct source fetch. A selectable URL remains available if the host is disconnected,
lacks navigation support, or rejects the request. Source availability must cover
both the server and the widget actually served, including modifications.

After upgrading from the Apache-2.0 version, rebuild the widget as well as adding
`[source]` to server configuration. Startup requires the new HTML source metadata
placeholder and inserts the escaped operator URL before validating resource bounds.

## Progress and lifecycle

Clients supplying `_meta.progressToken` receive bounded, monotonic stages before
the final result. Stages describe work, not partial legal records. Missing tokens
disable notifications. Slow readers cannot stall a shared refresh on behalf of
other callers. HTTP remains stateless: close the original response stream to cancel
its call. WebTransport also supports cancellation notifications. Progress does not
extend deadlines, and updates stop after the invocation ends.

The [retrieval contract](retrieval.md) defines cache retention, freshness, provider
budgets, cancellation, and unsupported source capabilities. Private health metrics
show cache reuse and upstream work without recording queries or source payloads.

## ChatGPT manual verification

Current OpenAI guidance uses Streamable HTTP for the remote MCP connection and the
MCP Apps bridge for embedded UI. See the official
[connection guide](https://developers.openai.com/plugins/deploy/connect-chatgpt)
and [UI contract](https://developers.openai.com/plugins/build/chatgpt-ui).

When an operator supplies a public HTTPS endpoint or an appropriate development
tunnel, connect `/mcp` in ChatGPT developer mode, refresh tools, search the synthetic
records, and ask to display selected IDs using `demo_show_records`. Check search,
details, freshness, empty/error states, and model-readable results without UI.
Record the client/version and observed behavior; protocol tests alone do not prove
that a particular ChatGPT client renders progress or widgets.

This change does not deploy a service, configure account authentication, submit a
plugin, or run a live ChatGPT connection. Local browser tests exercise a simulated
MCP Apps host. The pinned OxiBelt gate exercises both native transports, new tools,
progress, and resource retrieval using isolated synthetic fixtures.
