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
scripts/test-oxibelt.sh --profile fixture
scripts/test-oxibelt.sh --profile kubernetes
```

The default profile is `fixture`, using the original synthetic edge/backend
configuration. The `kubernetes` profile reads the committed
[NodePort handoff example](../deploy/oxibelt/kubernetes-upstream.example.toml),
substitutes disposable hostnames/certificates, and uses a backend listening directly
on TCP 30080 and UDP 30433. It retains those upstream ports from the example.
Both profiles run the same protocol and rejection checks, and both run in CI.
Neither profile creates a cluster or verifies Kubernetes NodePort translation,
node reachability, firewall enforcement or production certificates.

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
runs the Rust comparison worker without Git, enables 16 MiB messages and 256 MiB
transport buffering, and gives the backend a 1 GiB container limit. Both revisions/transports exercise exact
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
- Rejection of wrong routes, public `/live`, `/ready` and `/metrics`, HTTP
  authority, disallowed Origin, and untrusted downstream certificates.
- Rejection when only OxiBelt's backend CA trust changes, followed by a successful
  reconnect after restoring trust.
- Rejection of a backend certificate signed by the trusted CA but issued for the
  wrong DNS name, followed by successful recovery with the matching certificate.

A successful run reports that the OxiBelt HTTP and WebTransport integration checks
passed for the selected profile.
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

## Kubernetes NodePort handoff

Start with the complete
[`kubernetes-upstream.example.toml`](../deploy/oxibelt/kubernetes-upstream.example.toml)
for the pinned revision above. It routes the public host `openlegal4everyone.stream`
on exact `/mcp` and CONNECT `/mcp-wt/v1` paths. The container listens on 8443;
the operator's host Compose must publish `443:8443/tcp` and `443:8443/udp`.
Remove the existing `openlegal4everyone` nginx service from that operator-owned
Compose configuration and retain OxiBelt and lego. No host Compose file is shipped
in this repository, and no additional proxy is needed between OxiBelt and the
Kubernetes Service.

Replace every `replace-with-private-node.invalid` occurrence with a private node
DNS name reachable from the OxiBelt container. The reserved placeholder will not
work unchanged. Use the same hostname for both upstreams and configure these
identities together:

| Setting | Operator value, where `NODE_DNS` is the private node hostname |
| --- | --- |
| OxiBelt HTTP origin | `http://NODE_DNS:30080` |
| Backend `[http].allowed_hosts` | `["NODE_DNS:30080"]` |
| OxiBelt WebTransport origin | `https://NODE_DNS:30433` |
| Backend `[webtransport].allowed_hosts` | `["NODE_DNS:30433"]` |
| Backend certificate DNS SAN | `NODE_DNS` without a port |
| Both backend `allowed_origins` | `["https://openlegal4everyone.stream"]`, extended only for intended callers |

The backend still listens on 8080/TCP and 4433/UDP. Kubernetes translates the
NodePorts; it does not rewrite HTTP Host or WebTransport authority. Because
`preserve_host = false`, the backend receives the upstream hostname and NodePort,
not its Pod listener port. Replace both existing authority placeholders in the
[serving configuration](../deploy/kubernetes/config/server.toml) accordingly.
Caller Origin must pass through unchanged. Retain HTTP/1 upstream forwarding,
HTTP/3 WebTransport, the 16 MiB HTTP request limit, streaming request/response
bodies, disabled caching/compression and the example's connection/request/idle
timeouts. Upstream URLs remain origins without base paths.

Public edge TLS and private backend TLS have separate ownership. Mount the
lego/ACME edge certificate and key as `cert/edge.pem` and `cert/edge-key.pem`
beside OxiBelt's `config/` directory. Mount the backend issuing CA certificate as
`cert/backend-ca.pem`, as selected by `proxy.trusted_ca_certs`. Put only the
issued backend certificate and private key into Kubernetes Secret
`openlegal-backend-tls` (`tls.crt` and `tls.key`). Keep the CA private key outside
both OxiBelt and Kubernetes. Do not disable chain or hostname verification, and
do not reuse the public edge certificate identity as the backend identity by
assumption. The example disables hot reload; coordinate edge restart and backend
rollout when replacing certificates, allowing for `Recreate` downtime and
preserving current trust.

Private node DNS is the primary path. When OxiBelt and Kubernetes share a physical
host, an operator may instead use a stable hostname explicitly mapped to the host
gateway in Compose, for example:

```yaml
services:
  oxibelt:
    extra_hosts:
      - "openlegal-node.internal:host-gateway"
```

This is a fragment for the operator's existing service, not a complete Compose
file. Use `openlegal-node.internal` consistently as `NODE_DNS` in the table above
and issue the matching backend certificate. Do not hard-code a Docker bridge IP.
Verify what `host-gateway` resolves to in the selected daemon/network setup and
whether both NodePorts are reachable from the actual OxiBelt container; a rootless
daemon does not by itself establish that route. See
[Compose host mappings](https://docs.docker.com/reference/compose-file/services/#extra_hosts).

Complete the [NodePort firewall prerequisites](deployment-kubernetes.md#service-and-private-network-handoff)
before applying the Service. Port 9090 and `/live`, `/ready`, `/metrics` have no
Service or public edge route. Pod-network isolation, real TCP/UDP routing, source
translation, firewall enforcement, public edge access and browser/ChatGPT platform
acceptance require their own operator checks; the isolated harness supplies no
real-cluster acceptance evidence.
