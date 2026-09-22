# Kubernetes deployment design

## Status and baseline

Phase 0 records the selected deployment design and existing server contracts.
Phase 1 adds the production server image, local image acceptance and native
amd64/ARM64 CI jobs. Kubernetes serving manifests and complete operator runbooks
remain planned. This document does not establish a running deployment or
successful real-cluster, live-provider, browser WebTransport or ChatGPT acceptance.

The inventory was checked on `main` at
`79a8a852916d6fb18f306e5d7541115e1bab87d8`, with a clean working tree before these
documentation changes. The earlier design baseline,
`f4bf5899caa1cc04491b68ddeb7c516b58f93387`, differs only by the commit ignoring
`.agents/temp/`; runtime code is unchanged. The configured `GitHub` remote is
`https://github.com/PublicData-stream/openlegal4everyoneMCP` for fetch and push;
this records local configuration, not a remote synchronization check. Recheck with:

```sh
git rev-parse HEAD
git status --short
git remote -v
```

The [architecture](architecture.md), [server contract](server.md),
[corpus contract](database.md), [persistence contract](persistence.md) and
[document sandbox](document-sandbox.md) remain authoritative for their behavior.
Phase 0 changed documentation only. Phase 1 changes packaging and the Rust CPU
baseline; public MCP schemas, configuration types and legal-data semantics remain
unchanged.

## Production server image

Build from the repository root with Docker BuildKit. The production platforms are
Linux GNU x86_64 with **x86-64-v3 required**, and generic AArch64. An amd64 manifest
does not itself communicate the stronger CPU requirement: check deployment nodes
as described in the [contributor baseline](../CONTRIBUTING.md#rust-baseline).
The same x86 CPU requirement applies to local Rust builds and the separate
document-worker image; ARM64 support here covers the server, not that worker.

```sh
docker build --platform linux/amd64 -f apps/server/Dockerfile \
  --build-arg REVISION="$(git rev-parse HEAD)" \
  --build-arg VERSION=development \
  -t openlegal-server:local-amd64 .
docker build --platform linux/arm64 -f apps/server/Dockerfile \
  --build-arg REVISION="$(git rev-parse HEAD)" \
  --build-arg VERSION=development \
  -t openlegal-server:local-arm64 .
```

Use a native builder for each architecture or a builder with explicitly configured
emulation. No builder installation, registry login or image push is performed by
these commands. The Rust build defaults to two jobs; `--build-arg BUILD_JOBS=N`
can adjust build resource use. Provision space for native build caches and image
layers; hosted jobs report available disk space, and their clean-build peak still
requires measurement. Empty BuildKit
caches are supported; compiled caches are separated by architecture. All base
images and the frontend are digest-pinned, native build/runtime packages use a
dated Debian snapshot, and application graphs use their committed lockfiles.
This is pinned-input reproducibility, not a byte-identical-image guarantee.

OCI labels record the source repository, revision, version, license and CPU
baseline. Defaults are `unknown` revision and `development` version; set truthful
values when producing an operator artifact. Build from a clean committed tree
before identifying an image as that revision. Local tags above are disposable
build handles; deployments must use the digest of the published immutable image.
Publishing an image remains a separate operator action.

The image runs `/usr/local/bin/openlegal-server` directly as `10004:10004` and
defaults to `/etc/openlegal/server.toml`. Supply that file and WebTransport TLS
material through read-only mounts; no default configuration or keys are baked in.
The existing administrative command arguments can replace the default config
argument, for example `--migrate /etc/openlegal/server.toml`. Ordinary startup
does not migrate the database. Configure only the credentials each mode needs.

| Configuration | Immutable image path |
| --- | --- |
| `[text_diff].widget_html` | `/opt/openlegal/widgets/text-diff.html` |
| `[database].widget_html` | `/opt/openlegal/widgets/database.html` |
| `[demo].widget_html` | `/opt/openlegal/widgets/index.html` |

Shipping a widget does not enable its feature. Retained-corpus serving still needs
the separately provisioned read-only MeCab dictionary, PostgreSQL and separate
writable blob/index mounts. The Lindera dictionary is already embedded at build
time; the image never downloads a dictionary at startup. Ingestion stays opt-in
and requires a later ingestion-capable image: this image contains no `kubectl`.

The image contains the CA trust bundle and runtime GNU libraries, but no compiler,
Cargo, Node, pnpm, Git or build cache. Application files are root-owned and the
image creates no writable application directory for UID 10004. Run with read-only
root, dropped capabilities, no privilege escalation, the default seccomp policy
and resource limits. Mount temporary storage and configured persistent directories
explicitly; Docker image metadata cannot enforce these runtime controls itself.
SIGTERM reaches the server directly. Keep its shutdown grace longer than the
configured drain timeout.

Licenses and dependency notices reside under `/opt/openlegal/notices/`, including
a dependency inventory, supplemental source evidence and Rust standard-library
notices. The build rejects missing or mismatched supplemental notices. The widget
files retain their source-offer marker until the server replaces it with the
operator's `[source].url`. That URL must provide corresponding source for the exact
running server/widget, including modifications and necessary build material; OCI
labels do not replace the [source-offer contract](server.md#source-offers-and-migration).

### Image acceptance

```sh
scripts/test-server-image.sh --platform linux/amd64
scripts/test-server-image.sh --platform linux/arm64
```

Omit `--platform` to select the Docker host architecture. The gate builds the final
image and checks its identity, libraries, widgets, notices, absent build tools,
non-root/read-only operation, invalid configuration, health, MCP text comparison
and SIGTERM shutdown. It generates disposable certificates outside the build
context and transfers them through named volumes, supporting development containers
whose Docker daemon runs on the host. The client and server use an internal network
without published host ports; no provider, database or Kubernetes credentials are
needed. Requests, polling, resource use and cleanup are bounded.

The CI image jobs use native `ubuntu-24.04` and `ubuntu-24.04-arm` runners, with no
publication credentials. Label local ARM emulation evidence as emulated and native
CI evidence separately. Neither the image smoke nor a committed workflow proves
real-cluster isolation, retained-corpus startup, live provider or public transport
acceptance. Existing PostgreSQL, Korean analyzer, OxiBelt and document-worker gates
remain separate. Record actual check outcomes with the implementation handoff.

## Selected topology

```text
Internet -- TCP/443 + UDP/443 --> Host Docker Compose
                                  lego (edge certificates)
                                  OxiBelt
                                    | TCP /mcp       | UDP /mcp-wt/v1
                                    v                v
                              Private Kubernetes NodePorts
                                    |                |
Kubernetes                          v                v
  openlegal-serving             HTTP :8080       QUIC/TLS :4433
    openlegal-server Deployment: one replica, Recreate
      |-- private health :9090 (no NodePort or public edge route)
      |-- PostgreSQL 18 (operator-provisioned endpoint)
      |-- separate persistent blob/index mounts
      |-- read-only provisioned dictionary
      `-- optional ingestion controller, disabled by default
               | Kubernetes API, separately scoped controller identity
               v
  openlegal-documents
    disposable document-worker Pods, RuntimeClass/openlegal-document
    deny-all networking, no provider credentials or controller tokens
```

OxiBelt and lego stay outside Kubernetes to retain host ownership of public
routing and certificate lifecycle. OxiBelt already provides the required edge;
this design adds no intermediary reverse proxy. The NodePort handoff carries
private plaintext HTTP and separately verified WebTransport TLS. Follow the
[OxiBelt hosting contract](oxibelt.md#adapt-the-configuration-for-hosting): preserve
caller Origin, retain `preserve_host = false`, allow the actual backend authorities,
and issue a backend certificate matching the authority trusted by OxiBelt.
NodePorts alone do not establish privacy; the operator must restrict access to
the intended host/private path and validate the effective network controls.

The serving Deployment will have exactly one replica and use `Recreate`.
Application budgets and upstream coordination assume one backend, and concurrent
processes must not share the corpus index. Preserve the existing database advisory
lease between corpus serving and rebuilding; deployment settings do not replace
it or the operator's responsibility to stop serving for offline work. Multi-replica
serving, distributed indexing and automatic failover are outside this design.

Serving uses the normal hardened container runtime, with a non-root user,
read-only root filesystem, dropped capabilities, no privilege escalation and
RuntimeDefault seccomp. Document parsing uses its existing separate gVisor domain
because it processes untrusted XML/HTML and binary documents with native parsers.
The [existing sandbox artifacts](../deploy/document-sandbox/) remain canonical;
only document-worker Pods use `RuntimeClass/openlegal-document`. Its quota, RBAC,
network denial and node prerequisites remain governed by the
[sandbox acceptance contract](document-sandbox.md#cluster-preparation-and-acceptance).

## Existing runtime inventory

The [command dispatcher](../apps/server/src/main.rs) implements these modes;
`CONFIG.toml` denotes an operator-supplied configuration file.

| Invocation | Existing behavior and deployment consequence |
| --- | --- |
| `openlegal-server CONFIG.toml` | Validates serving configuration, opens configured runtime storage/services, and requires HTTP, WebTransport and private health listeners. Ordinary startup does not migrate the schema. |
| `openlegal-server --migrate CONFIG.toml` | Parses the full configuration and uses only the migration database credential. Does not open blobs, dictionary, widgets, workers or listeners. Required source/HTTP/WebTransport/health sections must still parse; referenced serving files need not be mounted. |
| `openlegal-server --maintain CONFIG.toml` | Uses the runtime credential and cache blob store for bounded cache pruning, then exits without serving or upstream fetching. This is not a general corpus-maintenance command; follow the offline procedure in the persistence contract. |
| `openlegal-server --rebuild-corpus-index CONFIG.toml` | Uses runtime storage, retained corpus evidence, the provisioned dictionary and a fresh index destination under the corpus lease. No provider credential or listeners are needed. Follow the corpus contract's stop/rebuild/restart procedure and rollback constraints. |
| `openlegal-server --text-diff-worker` | Internal pipe-based comparison worker; takes no further arguments and bypasses configuration and listener startup. This is distinct from the document-worker image. |

The [configuration implementation](../apps/server/src/config.rs) makes listener
addresses explicit; `8080/TCP`, `4433/UDP` and `9090/TCP` above are the selected
deployment ports, not hard-coded server defaults. WebTransport requires explicit
certificate/key paths. The corpus requires PostgreSQL persistence, text comparison,
widget paths and a provisioned MeCab-Ko dictionary. Preserve the text-comparison
profile of 16 MiB messages and at least 256 MiB transport buffering.

Runtime and migration environment names are configurable and must differ. Keep
the existing defaults, `OPENLEGAL_DATABASE_URL` and
`OPENLEGAL_MIGRATION_DATABASE_URL`, in future templates. The serving workload
receives only the runtime credential; the separate migration Job receives only
the migration credential. Preserve verified PostgreSQL TLS and operator CA inputs
as described in the [persistence configuration](persistence.md#configuration-and-startup).

The [health handlers](../apps/server/src/http.rs) expose unauthenticated
`GET /live`, `GET /ready` and `GET /metrics` on the private health listener.
Readiness combines lifecycle state with already observed storage health without
new I/O in the request. It is not a fresh connectivity test, complete-corpus claim
or index-catch-up gate. Metrics expose bounded operational counters; none of these
routes will be published through OxiBelt or a NodePort.

Retained corpus serving requires no provider credential or document-controller
access. Disabling ingestion does not make storage read-only: the
[corpus runtime](../apps/server/src/corpus_runtime.rs) still processes index events,
checks health and performs retention maintenance. Cache blobs, corpus blobs and
the index require separate, non-nested writable directories. Provision the
dictionary before startup, mount it read-only, and follow the
[dictionary and index contracts](database.md#operator-configuration) for changes;
there is no runtime download or ambient dictionary discovery.

## Template and operator ownership

The following division guides later implementation; it does not imply these
production templates already exist.

| Repository templates and contracts | Operator-supplied deployment values and actions |
| --- | --- |
| Image build, immutable image references, widget locations and source-offer field | Published image digests and a public corresponding-source URL for the exact running server/widget |
| Serving namespace, one replica, Recreate, security settings and probe definitions | Target cluster/runtime, measured resource sizing and real-cluster acceptance |
| TCP/UDP Service structure and OxiBelt handoff example | Concrete NodePorts, private addresses, backend authority, allowed origins, firewall rules and host Compose/certificate configuration |
| Separate storage mounts and generic Local PV/PVC examples | ZFS datasets, host paths, node affinity, capacity, ownership/permissions and provisioned dictionary |
| Secret references and separate serving/migration commands | PostgreSQL endpoint, roles/grants, credentials, CA material and backend TLS certificate/key |
| Explicit opt-in ingestion overlay and namespace-scoped controller access | Provider credential, digest-pinned worker image and separately configured controller identity after sandbox acceptance |

Commit no credentials, private keys, production kubeconfig, database URL, real
node identifier or host-specific ZFS path. Exact network/storage values, resource
limits and later ingestion authentication integration remain future-phase work.
The current controller requires explicit kubeconfig and context; any in-cluster
authentication alternative needs a deliberate contract change and Security Review.

## Validation boundary

Phase 0 acceptance consists of source/document inspection, Markdown/link checks
and independent review of the documentation patch under
[CONTRIBUTING.md](../CONTRIBUTING.md#documentation-only-changes). It supplies no
new runtime, image, manifest or CI validation evidence.

Image acceptance is available in Phase 1; Kubernetes deployment validation remains
later work. Real-cluster
networking, storage, shutdown and sandbox enforcement require operator acceptance;
live LAW OPEN DATA access and public transport/platform acceptance are separate
gates. Existing offline fixtures and local integration evidence do not satisfy
those gates. Deployment, publication and live provider requests are not part of
Phase 0.
