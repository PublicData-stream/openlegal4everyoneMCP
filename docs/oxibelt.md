# OxiBelt edge integration

The server has two private listeners behind OxiBelt: `/mcp` uses Streamable
HTTP, and `/mcp-wt/v1` uses the project's custom WebTransport binding. ChatGPT
connects to the public HTTPS `/mcp` URL. Browser WebTransport interoperability
and a live ChatGPT connection require separate platform testing.

The integration configuration targets OxiBelt source revision
[`72564d165dfd05cb29a64aeebd19fccd7944ea6f`](https://github.com/OxiBelt/OxiBelt/tree/72564d165dfd05cb29a64aeebd19fccd7944ea6f).
It follows that revision's [WebTransport forwarding implementation](https://github.com/OxiBelt/OxiBelt/blob/72564d165dfd05cb29a64aeebd19fccd7944ea6f/source/src/proxy/http/webtransport.rs).
The reference repository is not modified by the harness.

## Run the isolated integration test

From a Linux checkout with the pinned Rust toolchain, Git, OpenSSL, Python 3,
and a working rootless Docker daemon:

```sh
scripts/test-oxibelt.sh
```

The first run downloads the pinned OxiBelt source and Docker runtime image and
builds the server, native client, and OxiBelt with locked dependencies. The
harness gives the unoptimized OxiBelt executable a 64 MiB process-stack limit;
its main future overflows the default stack in a debug build. OxiBelt's
build also needs its native dependency toolchain, including a C/C++ compiler,
CMake, Perl, and pkg-config. Builds can use several GiB of disk and memory.
Run Cargo through the privileged command channel when using a shared local cache.
The harness uses an Ubuntu 26.04 runtime because its binaries are built on the
host; host architecture must match Docker's architecture and its glibc must be
compatible with that runtime. It is an integration image, not a release image.

A local OxiBelt checkout avoids fetching source:

```sh
OXIBELT_SOURCE=../OxiBelt scripts/test-oxibelt.sh
```

The script archives the pinned Git object, ignoring working-tree modifications.
`OXIBELT_TARGET_DIR` selects an optional build cache; its default is
`target/oxibelt`. `OXIBELT_BUILD_JOBS` defaults to two. `OXIBELT_BINARY` can reuse
an already built, trusted binary only if its emitted build identity reports the
exact clean revision. This identity check prevents accidental stale builds; it
is not cryptographic provenance. `SERVER_BINARY`, `WT_CLIENT_BINARY`, and
`MOCK_UPSTREAM_BINARY` select prebuilt executables; set all three to skip their build.
The mock runs on loopback inside the backend container. By default the resource
is a small synthetic HTML fixture; set `DEMO_WIDGET_HTML=apps/widget/dist/index.html`
and `TEXT_DIFF_WIDGET_HTML=apps/widget/dist/text-diff.html` after the widget build
to test both complete bundled resources, as CI does. The text-comparison fixture
installs Git, enables 16 MiB messages and 256 MiB transport buffering, and gives
the backend a 1 GiB container limit. Both revisions/transports exercise exact
1 MiB inputs, paged results, widget resources and bearer-authorized deletion.
The `/mcp` route explicitly allows a 16 MiB request body to match the backend;
the edge's inherited 10 MiB default cannot carry the largest escaped text pair.

All service containers share an internal Docker network. No host port is
published and no legal-data provider is contacted. The test generates a
short-lived synthetic CA and TLS leaves with `edge` and `backend` DNS SANs.
Configuration and disposable leaf keys are streamed into a temporary Docker
volume, mounted read-only by serving containers. Keys are excluded from the
Docker build context and cache. The script removes its containers, network,
fixture volume, temporary image, and local key directory on completion or
failure. CA private keys remain only in the temporary host directory.

The test fails unless all of these checks pass:

- Pinned OxiBelt identity and its native `--check` configuration validation.
- HTTP discovery/initialization, `tools/list`, and `server_info` for both supported
  MCP revisions; the client handles bounded JSON or SSE responses.
- The same native WebTransport tool calls for both revisions, including a fresh
  connection and explicit allowed Origin.
- Rejection of wrong routes, HTTP authority, disallowed Origin, and untrusted
  downstream certificates.
- Rejection when only OxiBelt's backend CA trust changes, followed by a successful
  reconnect after restoring trust.

A successful run prints `OxiBelt HTTP and WebTransport integration checks passed.`
This is an opt-in integration gate; ordinary Rust tests remain deterministic and
independent of Docker. These checks establish native client interoperability for
this pinned edge configuration. They do not establish browser or ChatGPT platform
integration, streaming performance, or production readiness.

## Adapt the configuration for hosting

Use [the edge example](../deploy/oxibelt/oxibelt.toml) and
[the backend example](../deploy/oxibelt/backend.toml) as the responsibility map.
OxiBelt expects certificate filenames relative to a `cert/` directory beside its
`config/` directory. The backend accepts its configured certificate paths.

| Traffic | Edge listener/path | Private destination |
| --- | --- | --- |
| ChatGPT and other MCP HTTP clients | TCP TLS, `/mcp` | `http://backend:8080` |
| Native WebTransport clients | UDP QUIC/HTTP/3, CONNECT `/mcp-wt/v1` | `https://backend:4433` |
| Backend health | Private administration network | Port `9090` |

Replace the synthetic `edge` route hostname with the real public hostname and
install a publicly trusted edge certificate. Expose the selected edge TLS port
on both TCP and UDP (normally 443). Keep backend TCP 8080, UDP 4433, and health
9090 private. Retain `webtransport = true` and upstream HTTP/3 on the WebTransport
route. Both upstream URLs must be origins without a base path, so paths survive
forwarding unchanged.

Retain `preserve_host = false`; backend allowlists contain backend authorities,
including ports. Set backend `allowed_origins` to the actual browser/client
origins allowed by the operator; this value is an Origin, without a path, and is
independent of the public server URL. Native callers may omit Origin. OxiBelt
must preserve caller Origin so the backend can enforce this policy.

Trust the backend's issuing CA in OxiBelt and issue its certificate with a SAN
matching the backend DNS name. Never disable certificate verification to fix
routing. The HTTP route disables response buffering, caching, and compression
so MCP streaming can pass through. Treat proxy timeouts, connection budgets,
backend request deadlines, and shutdown grace as one deployment policy.
