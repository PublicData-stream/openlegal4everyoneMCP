# Kubernetes deployment design

## Status and baseline

Phase 0 records the selected deployment design and existing server contracts.
Production serving images, Kubernetes serving manifests, deployment CI and operator
runbooks are planned. This document does not establish a running deployment or
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
Phase 0 changes documentation only; public MCP schemas, configuration types and
legal-data semantics are unchanged.

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

Image builds and deployment validation remain later repository phases. Real-cluster
networking, storage, shutdown and sandbox enforcement require operator acceptance;
live LAW OPEN DATA access and public transport/platform acceptance are separate
gates. Existing offline fixtures and local integration evidence do not satisfy
those gates. Deployment, publication and live provider requests are not part of
Phase 0.
