# Server and extension contract

The implemented foundation is one `apps/server` library/binary package. It serves
anonymous MCP tools over Streamable HTTP and WebTransport. Extension tools are
read-only; the built-in text comparison feature additionally permits deletion of
a temporary result using its bearer handle. Both adapters
are required by the production binary. An explicitly configured synthetic provider,
shared memory retrieval cache, progress notifications, and React MCP Apps widget
extend this foundation. No real legal provider, user account, or deployment is implemented.

## Configure and run

The binary accepts one TOML configuration path. All tables below are required
except `limits`; unknown fields are rejected. Generate certificates only for local
fixtures, or supply operator-managed certificates for hosting.

The optional `[text_diff]` table enables [Rust text comparison](text-diff.md) without
a synthetic upstream. It requires its separately built widget,
16 MiB messages and at least 256 MiB transport buffering.

The optional `[demo]` table is documented in the [demo guide](demo.md). It requires
a configured loopback mock and bounded local widget HTML; absence preserves ordinary
server behavior. The example demo uses a 4 MiB message budget for its UI resource.

```toml
[source]
# Placeholder: replace with free corresponding source for the running version.
url = "https://example.org/openlegal/source"

[http]
bind = "127.0.0.1:8080"
allowed_hosts = ["127.0.0.1:8080"]
allowed_origins = []

[webtransport]
bind = "127.0.0.1:4433"
certificate = "certs/server.pem"
private_key = "certs/server-key.pem"
allowed_hosts = ["127.0.0.1:4433"]
allowed_origins = []

[health]
bind = "127.0.0.1:9090"
```

```sh
cargo run --locked -p openlegal-server -- server.toml
```

HTTP `/mcp` is private plaintext behind the TLS edge. WebTransport `/mcp-wt/v1`
always uses TLS/QUIC. Both must bind successfully before readiness becomes true.
Keep health `/live`, `/ready`, and `/metrics` private; they have no authentication.
`/metrics` exposes aggregate tool call/failure counts and, when configured, bounded
application retrieval metrics, not request payloads.
SIGINT/SIGTERM stop admission and drain the server. An unexpected required endpoint
failure also stops the server. Shutdown deadline exhaustion is an error.

The actual proxied configuration and reproducible native-client acceptance gate
are in [OxiBelt integration](oxibelt.md). A simulated browser host is tested; live
ChatGPT behavior remains unverified. To test ChatGPT manually, configure a developer-mode connection to a
deployed HTTPS `/mcp` endpoint, inspect the listed `server_info` tool, and call it.
The result must identify the server foundation without claiming legal retrieval.
No plugin directory submission or deployment is automated here. The
[demo guide](demo.md) adds the synthetic workflow and widget acceptance procedure.

## Source offers and migration

The September 12, 2026 AGPL-3.0-only migration requires `[source].url` in every
configuration, including servers without the demo. Old configurations must add
this table before upgrading; there is no implicit link to upstream `main`.
The example URLs are isolated placeholders, not working source offers.

`SourceOffer` validates an absolute HTTPS URL with a hostname, no credentials,
leading/trailing whitespace, control characters or backslashes, and at most 2,048 bytes before and
after normalization. The shared handler also requires its source metadata to fit
the configured tool-result budget; an incompatible small budget fails startup.
Validation happens before listeners bind. The URL is public metadata: do not put
tokens or other secrets in it. The server does not fetch the URL, inspect an
archive, or establish source availability by validating its syntax.

Provide a public page or download giving free access to the corresponding source
for the exact running server and widget, including local modifications and the
source and scripts needed to generate, install, run and modify them. An immutable
revision/archive is preferable to a moving branch. A repository link is sufficient
only when it actually provides that corresponding source. Operators must maintain
availability and include necessary dependency source as required by the license;
the built-in link is not a completeness or compliance certification. See
[AGPLv3 sections 1, 6 and 13](https://www.gnu.org/licenses/agpl-3.0.html).

The existing `server_info` tool retains its identity fields and adds `license`
(`AGPL-3.0-only`), `licenseUrl` (the GNU license text), and `sourceUrl` (the validated
operator URL). Legacy initialization and modern `server/discover` instructions
advertise the same offer through the shared handler on both transports. This adds
public metadata without adding a source-download endpoint.

Pass the same `SourceOffer` to `server_info_registry`, `ServerBuilder`, and widget
loading. Direct `McpHandler` constructors also require it. Rebuild the widget when
upgrading: its HTML must contain exactly one `__OPENLEGAL_SOURCE_URL__` placeholder
in the `openlegal-source-url` metadata attribute. Startup replaces it with an
attribute-escaped URL and checks the resulting resource size. Missing or duplicate
markers fail startup. The widget embeds the full license and offers host-mediated
source navigation with a selectable URL fallback; see the [demo guide](demo.md).

## Rust extension API

Construct `ToolRegistry`, register modules implementing `ToolModule`, and pass it
to `ServerBuilder::new(registry, limits, source)`, where `source` is a validated
`SourceOffer`. Modules call
`ToolRegistry::register::<Input, _, _>(name, description, handler)` where `Input`
implements Serde deserialization and Schemars JSON Schema. The asynchronous handler
receives typed input and `ToolContext`, returning `Result<Value, ToolError>`.
`register_with_annotations` supplies accurate MCP annotations; public registration
accepts only read-only modules. A crate-private registration path permits only the
built-in `delete_text_diff` operation, marked read-only false, destructive true,
idempotent true and open-world false. Possession of a valid comparison handle
authorizes reading and deletion; this exception does not enable arbitrary writes. Annotations are descriptive,
not a sandbox or authorization mechanism.

Registration validates object input schemas, names, descriptions, duplicate names,
and registry size. Remote/file schema resolution is disabled. Each invocation
validates its arguments against the registered schema before deserializing or
calling a handler. Input errors remain protocol errors; not-found, unavailable,
rate-limited, and internal execution failures are bounded structured tool errors
with distinct codes. Raw implementation diagnostics never become tool errors.

Use `ServerBuilder::register_endpoint` with an implementation of `Endpoint` to
add a transport. Its binding declaration is checked before startup; `bind` returns
a `BoundEndpoint` that owns its listeners and serving future. Dropping a bound
endpoint must release its listeners. `EndpointContext` supplies the shared MCP
handler, shutdown token, global admission semaphores and limits. Adapters must use
these budgets before spawning request work, validate their boundary, and join
their owned tasks during shutdown. The generic API does not require boxing an
SDK transport trait object. Tests in `apps/server/tests/extensions.rs` exercise
downstream endpoint implementations, conflicts and startup rollback.

The registry is immutable once the server starts; no tool-list change events are
advertised. Registered static resources enable discovery/read; prompts, tasks,
subscriptions, sampling and elicitation remain unsupported. New Rust modules require rebuilding. Modules are
trusted code: they must bound result construction, cooperate with cancellation,
avoid detached tasks, and use shared application services for future legal-data
operations. They cannot be sandboxed by a Rust trait or a serialized-byte limit.

### Typed results, resources and workers

`register_typed<I, O, _, _>` adds a typed output schema, `ToolOptions` descriptor
metadata, and `ToolOutput<O>` structured data with optional text and metadata.
Both schemas are validated without remote/file resolution. Successful output must
match its declared object schema. Existing `Value` registration remains supported.
Combined result text/metadata/structured content remain centrally bounded; `_meta`
is client-visible information, never a place for secrets.

Typed handlers receive `ToolExecutionContext`, containing the original cancellation
context, absolute deadline, and a restricted `ProgressReporter`. Progress is
optional, monotonic, capped at five coarse stages and one-quarter of the message
budget including framing allowance. Slow sends can drop updates after 100 ms.
The final result remains authoritative; progress never extends a call deadline.

`ResourceRegistry` accepts up to 32 trusted text resources with exact `ui://` URIs.
The default raw text limit is 1 MiB and serialized resource limit 2 MiB. Only the
built-in comparison widget has a 3 MiB raw / 6 MiB serialized allowance, additionally bounded
by half the configured message allowance. Duplicate registrations, bad MIME/URI
matches, dangling tool UI references, and excessive discovery/results fail startup.
`with_resources` enables static `resources/list` and `resources/read`; no caller URI
causes filesystem or network I/O. Registered resources use modern public cache scope
with zero TTL so clients revalidate rather than reuse a stale UI template.

`register_worker` adds a required application worker to the endpoint supervisor.
The server starts registered worker futures after successful listener binding;
unexpected completion fails readiness and triggers shared shutdown/drain. The
retrieval service owns its maintenance/refresh tasks and its `run` monitor propagates
internal failure to this worker. No request handler detaches refresh work.

## Protocol compatibility

Both adapters support `2026-07-28` and `2025-11-25`. Modern requests carry protocol
version and client metadata per request; legacy clients initialize. HTTP uses SDK
Streamable HTTP routing and JSON/SSE error/result translation. HTTP legacy mode
is stateless: initialization works without issuing a persistent session ID. No
standalone legacy HTTP+SSE listener, persistent event store or GET event stream is
provided. HTTP protocol headers must be consistent with the body; missing or
unsupported version headers are rejected except for legacy initialization.

WebTransport binding v1 uses exactly one client-opened reliable bidirectional
application stream per connection, UTF-8 JSON-RPC messages separated by LF (CRLF
also accepted). Framing only is borrowed from stdio; it does not start a subprocess.
An initialized legacy connection and modern per-request metadata are distinct
lifecycles. Mixed-era traffic closes the connection without downgrade. The path
versions the binding; it does not select the MCP revision. Application datagrams
and additional application streams are rejected; HTTP/3 control streams remain
available to the transport library.

Request IDs cancel work through MCP cancellation notifications. Disconnect closes
all connection work; cancellation suppresses late serialized responses. String IDs
are limited to 128 bytes. Canceled IDs remain byte-accounted tombstones until
reconnect, with at most `max_in_flight` tombstones, preventing late response/ID reuse
ambiguity. Malformed framing, duplicate active/canceled IDs, overload, extra streams
and exhausted tombstone capacity close the connection. Reconnect after closure;
automatic request replay is not implemented.

WebTransport progress tokens have the same 128-byte string bound as request IDs.
Active or canceled token reuse is rejected; canceled token tombstones remain
bounded with request admission. Queued progress is suppressed after cancellation
or completion. HTTP progress uses the original request's SSE stream; stateless
HTTP cancellation closes that response rather than correlating a separate POST.

```sh
cargo run --locked -p openlegal-server --example wt_client -- \
  https://localhost:4433/mcp-wt/v1 certs/ca.pem 2026-07-28
```

An optional final argument supplies Origin. The reference client verifies TLS,
lists tools and calls `server_info`; never disable certificate verification.

## Resource and privacy defaults

| Configuration under `[limits]` | Default |
| --- | ---: |
| `max_message_bytes` | 1,048,576 |
| `max_buffer_bytes` | 67,108,864 |
| `max_in_flight` | 64 |
| `max_connections` | 128 |
| `max_calls_per_connection` | 8 |
| `io_timeout_secs` | 10 |
| `call_timeout_secs` | 30 |
| `idle_timeout_secs` | 60 |
| `shutdown_timeout_secs` | 15 |

Budgets are process-local and shared by both data transports. The buffering budget
is distinct from QUIC flow control: stream receive credit is at most 256 KiB and
connection receive/send windows are twice that (smaller frame limits reduce both).
Large JSON frames stream through these windows into the separately charged frame
allocation, preventing a larger message allowance from creating a large QUIC burst.
The buffering budget accounts for admitted application frames/bodies, with
conservative HTTP copy reservations; it is not a bound on process RSS or arbitrary plugin allocations.
TLS/QUIC, HTTP/2 and kernel buffers have separate finite limits. Registry discovery
must fit in half a message. Tool output is preflighted before SDK serialization;
structured values must fit one-eighth of a message, leaving room for its text copy,
escaping and protocol metadata. Final serialized responses remain bounded.

HTTP limits header parsing and socket write progress separately from tool work.
An independent response watchdog closes the TCP connection after call plus I/O
deadlines even if HTTP/2 flow control prevents body polling; other requests on that
connection are also interrupted. Idle connections are retired. WebTransport bounds
handshakes, partial frames, writes and connection idleness. Overload fails promptly
without an unbounded waiting queue. Transport retirement is not proof that a
misbehaving plugin detached no external work; that remains a module contract.

Host allowlists are explicit. Native clients may omit Origin; an empty Origin
allowlist rejects every present Origin. Forwarded headers do not establish identity.
The server suppresses SDK payload logs, including when `RUST_LOG` is configured,
and logs only bounded operational events. Database credentials belong only in operator-provided environment variables;
public read-only MCP inputs never accept them.

## Explicit persistence mode

A configured retrieval source requires `[cache].mode = "memory"` or `"persistent"`.
Memory mode is explicitly non-persistent. Persistent mode requires PostgreSQL 18.x
and an immutable BlobStore; there is no alternate persistent backend. The
[persistence contract](persistence.md) defines configuration, environment secrets,
SQL migrations, retention and the storage failure/recovery lifecycle.

Run `openlegal-server --migrate CONFIG.toml` with the separately configured migration
credential before serving. Ordinary startup uses only the runtime credential and
rejects unsupported PostgreSQL versions or missing, pending or changed migrations.
Run `openlegal-server --maintain CONFIG.toml` for explicit bounded retention pruning,
including after lowering limits below retained totals. Both commands finish without
opening listeners or initializing retrieval, widgets or text-comparison workers.
There is intentionally no conversion of old filesystem cache data.

Startup verifies PostgreSQL and blob health before listener admission. A runtime
storage outage makes `/ready` return 503 and rejects persistent retrieval, including
L1 hits, and history tools without upstream fallback. Bounded background probes
allow availability recovery. `/live`, metrics, supplied-text comparison and existing
transient comparison handles remain available while the process is healthy.
Actual endpoint/worker failure still triggers shared shutdown and drain. Database
and blob errors are translated to sanitized `storage_unavailable`, `storage_corrupt`,
`storage_capacity`, and `snapshot_unavailable` outcomes.

Persistent mode enables `demo_list_snapshots` and `demo_get_snapshot`; adding `[text_diff]` also
enables `demo_compare_record_snapshots`. Current result envelopes optionally include
`snapshot`; historical responses use a separate envelope. Demo render results
advertise history/comparison capability flags and source processor versions.
Comparison summaries optionally carry server-derived `origin` metadata. These
additions apply equally to both transports. Existing widget resources and source
metadata requirements remain unchanged.
