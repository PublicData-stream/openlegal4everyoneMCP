# Kubernetes operator runbook

<a id="kubernetes-deployment-design"></a>

## Status and baseline

Use this guide to prepare and operate the single-replica retained-corpus deployment
behind a host-managed OxiBelt edge. Image builds, serving/storage/network templates,
suspended administrative Jobs, opt-in ingestion and deterministic validation are
implemented. Production deployment and acceptance remain pending.

**Production acceptance blocker:** the [Phase 9 execution record](deployment-acceptance.md)
reports intermittent public HTTP failures through the pinned OxiBelt, including
after backend replacement. Restarting the edge restored a full smoke run, but the
cause and lasting fix remain unresolved. Public HTTP stability and seamless
backend-only upgrades are not accepted. Reassess this issue before accepting
production traffic or upgrades; backend readiness alone cannot clear it.

| Evidence | Established boundary |
| --- | --- |
| Repository and Docker checks | Implemented gates and recorded local outcomes; configured CI jobs do not establish observed hosted CI success |
| Disposable Kubernetes serving acceptance | The recorded single-node synthetic topology only; see each passed, failed and unexecuted case in the execution record |
| Production ZFS, firewall and routing | Operator qualification pending; disposable ext4 paths and node firewall rules do not qualify production infrastructure |
| Document sandbox and live provider | Same-host `runc` synthetic sandbox gate passed on 2026-09-26; independent security review and live-provider acceptance remain pending; default serving enables neither |
| Production traffic, browser WebTransport and ChatGPT | Not established by native clients or fixture results |

The [architecture](architecture.md), [server contract](server.md),
[corpus contract](database.md), [persistence contract](persistence.md),
[OxiBelt contract](oxibelt.md) and [document sandbox](document-sandbox.md) own their
technical behavior. This guide owns deployment sequencing and artifact handoff.
[Implementation history](#implementation-history) records the development phases.

Start with [operator inputs](#operator-handoff), then choose
[first deployment](#first-deployment), [upgrade/rotation](#upgrade-and-secret-rotation),
[administration](#administrative-jobs), [index rebuild](#fresh-index-destination-and-recovery)
or [rollback](#rollback-and-recovery). Finish with
[observability and acceptance](#observability-and-acceptance).

## Selected topology

```text
Internet -- TCP/443 + UDP/443 --> Host Docker Compose
                                  lego (edge certificates)
                                  OxiBelt
                                    | TCP /mcp       | UDP /mcp-wt/v1
                                    v                v
                              Kubernetes NodePorts TCP :30080 / UDP :30433
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

The serving Deployment has exactly one desired replica and uses `Recreate`.
Application budgets and upstream coordination assume one backend, and concurrent
processes must not share the corpus index. Preserve the existing database advisory
lease between corpus serving and rebuilding; deployment settings do not replace
it or the operator's responsibility to stop serving for offline work. Multi-replica
serving, distributed indexing and automatic failover are outside this design.

Serving uses the normal hardened container runtime, with a non-root user,
read-only root filesystem, dropped capabilities, no privilege escalation and
RuntimeDefault seccomp. Document parsing requires a separately qualified runtime domain
because it processes untrusted XML/HTML and binary documents with native parsers.
The [existing sandbox artifacts](../deploy/document-sandbox/) remain canonical;
only document-worker Pods use `RuntimeClass/openlegal-document`. Its quota, RBAC,
network denial and node prerequisites remain governed by the
[sandbox acceptance contract](document-sandbox.md#cluster-preparation-and-acceptance).

## Template and operator ownership

The following division covers the full deployment design. The templates are implemented;
the operator supplies and qualifies each target deployment.

| Repository templates and contracts | Operator-supplied deployment values and actions |
| --- | --- |
| GHCR image publication, immutable image references, widget locations and source-offer field | Selected published image digests and a public corresponding-source URL for the exact running server/widget |
| Serving namespace, one replica, Recreate, security settings and probe definitions | Target cluster/runtime, measured resource sizing and real-cluster acceptance |
| Fixed TCP 30080 / UDP 30433 NodePorts and OxiBelt handoff example | NodePort availability, private node DNS, backend authorities, allowed origins, firewall rules and host Compose/certificate configuration |
| Namespace-wide default deny and independently selected allow templates | Enforcing CNI, exact edge/database/DNS/monitoring peers, firewall controls and network acceptance |
| Separate storage mounts and generic Local PV/PVC examples | ZFS datasets, host paths, node affinity, capacity, ownership/permissions and provisioned dictionary |
| Secret references and separate serving/migration commands | PostgreSQL endpoint, roles/grants, credentials, CA material and backend TLS certificate/key |
| Explicit opt-in ingestion overlay and namespace-scoped controller access | Provider credential, digest-pinned worker image and separately configured controller identity after sandbox acceptance |

Commit no credentials, private keys, production kubeconfig, database URL, real
node identifier or host-specific ZFS path. Private network/storage values and
production resource sizing remain operator inputs. The ingestion overlay preserves
explicit kubeconfig/context authentication with a projected rotating token; no
ambient in-cluster authentication mode is introduced.

## Operator handoff

Before starting, assemble an operator-owned release directory outside Git. Preserve
the `config`, `serving`, `admin`, `network`, `storage` and optional `ingestion`
directory relationships when copying templates. The
[shared server configuration](../deploy/kubernetes/config/server.toml) is the
retained deployment example; replace its placeholders and render every selected
root before applying. Do not copy the text-only or fictional corpus fixtures into
production configuration.

| Required input | Preparation and acceptance checkpoint |
| --- | --- |
| Release | Clean source revision, truthful build version, published image digest for each selected platform/target, and matching public corresponding-source URL; follow [image preparation](#production-server-image) |
| Cluster | Explicit kubeconfig/context, qualified node/CPU, admitted resource kinds, measured resource budget and enforcing CNI; schema tool versions are not a supported-cluster matrix |
| Database and Secrets | PostgreSQL 18 endpoint, separate DBA-provisioned migration/runtime roles, verified CA, backend TLS identity, and the [Secret names and keys](#operator-prerequisites); keep values private |
| Storage | Prepared Local PV roots, capacities, node affinity, dictionary identity and recovery material; follow [storage preparation](#storage-and-permissions) |
| Edge and network | Actual edge path, backend DNS/authorities, allowed Origin, firewall restrictions and selected database/DNS/monitoring policies; follow [NodePort handoff](oxibelt.md#kubernetes-nodeport-handoff) |
| Maintenance and evidence | Named operator, maintenance window, prior compatible release artifacts, private recovery location, and per-gate acceptance outcomes |

Use a registry from which the workload can pull the published digest. The GHCR
release pipeline publishes the supported images; any alternate registry and
required pull credentials belong to the operator. Replace the complete
non-pullable image sentinel in serving and each selected administrative Job.
The optional ingestion image differs from the minimal administrative image;
follow its dedicated procedure.

All cluster commands below operate on deliberately tailored copies. Set an explicit
context in the shell used for each procedure; no ambient context is assumed:

```bash
kube=(kubectl --kubeconfig /absolute/operator/kubeconfig --context OPERATOR_CONTEXT)
```

These are staged operator actions, not a script to run uninterrupted. Stop at each
checkpoint until its required observation is recorded. Keep credentials, Secret
values, raw workload dumps and private host details out of shared evidence.

## First deployment

1. Select the intended [published immutable image](#ghcr-release-images).
   Verify its platforms and matching source offer. Prepare the release directory,
   node and PostgreSQL roles before applying workloads.
2. Establish the [NodePort firewall restrictions](#service-and-private-network-handoff).
   Create the namespace by itself, then apply default deny and the tailored allow
   policies through [network bootstrap](#bootstrap-and-policy-changes). Verify the
   selected paths and enforcement before starting administrative or serving Pods.

   ```bash
   "${kube[@]}" apply -f /absolute/operator/serving/namespace.yaml
   ```

3. Provision the [operator Secrets](#operator-prerequisites) in that namespace.
   Prepare ZFS/Local PV roots and the dictionary using the
   [storage procedure](#storage-and-permissions), then apply the tailored objects:

   ```bash
   "${kube[@]}" apply -f /absolute/operator/storage/storage-class.example.yaml
   "${kube[@]}" apply -f /absolute/operator/storage/local-pv.example.yaml
   "${kube[@]}" apply -f /absolute/operator/storage/local-pvc.example.yaml
   ```

   Check claim reservations, node affinity, capacity and private directory ownership.
   The dictionary must be complete and validated before serving starts. Migration
   itself needs only its credential and PostgreSQL CA, not these volumes or backend TLS.
4. Follow [prepare, stop and execute](#prepare-stop-and-execute) to render and apply
   the migration Job suspended, inspect its image/configuration/credential set, and
   explicitly activate it. Require successful completion, exit and termination.
   Have the DBA apply the [runtime grants](persistence.md#configuration-and-startup)
   after the tables exist. Failed or ambiguous migration keeps serving stopped.
5. Render retained serving and verify ingestion is absent, only the runtime DB
   credential is injected, and there is one replica with `Recreate`. Compare its
   image/configuration with the migration release. Only after the preceding checks:

   ```bash
   "${kube[@]}" kustomize /absolute/operator/serving > /absolute/operator/serving-rendered.yaml
   # Inspect the rendered resources before this apply.
   "${kube[@]}" apply -f /absolute/operator/serving-rendered.yaml
   "${kube[@]}" -n openlegal-serving rollout status deployment/openlegal-server --timeout=360s
   ```

   This apply creates both the running Deployment and NodePort Service. Readiness
   failure is a stop condition; do not relax network or storage validation to bypass it.
6. Verify private health, then TCP and native UDP transport from the actual OxiBelt
   container through the trusted NodePorts. Configure the edge using the
   [authority and TLS mapping](oxibelt.md#kubernetes-nodeport-handoff), then complete
   [public smoke and acceptance](#observability-and-acceptance), including rejection
   cases. Apply the public HTTP blocker above to the acceptance decision.
7. With a new empty database, expect empty retained results; readiness does not
   imply corpus coverage. For existing retained data, compare known capture identities
   and bounded search results. Fictional fixture seeding is not a production step.
8. Leave ingestion disabled for retained serving. If ingestion is required, follow
   [optional activation](#network-preparation-activation-and-rollback): complete
   sandbox acceptance and independent review, prepare identity/RBAC/networking,
   then use `mode = "pilot"` for the bounded first live pass. Enabling the overlay
   immediately initiates provider traffic. The durable pilot ledger allows 100
   attempts over 30 minutes and never resets automatically. Verify an actual
   provider detail, source and search result for every public category before
   selecting `mode = "continuous"`; continuous collection remains incomplete.

## Upgrade and Secret rotation

Use a maintenance window; `Recreate` entails downtime. Keep the
[public HTTP acceptance blocker](#status-and-baseline) in the release decision.

1. Retain the previous release's image/configuration, required Secret recovery
   material, dictionary and storage recovery references privately. Compare target
   schema and analyzer/index compatibility using the canonical
   [persistence](persistence.md#configuration-and-startup) and
   [rebuild](database.md#offline-index-rebuild) contracts.
2. Keep the operator serving configuration at zero replicas and prevent automation
   from restoring it. Follow the [shutdown procedure](#prepare-stop-and-execute),
   including actual termination of serving, ingestion, retention, prior Jobs and
   any external/legacy writers. Pod absence alone is insufficient.
3. Apply required policy/configuration changes while stopped. Run migration only
   if the release requires it; apply any necessary runtime grants afterward.
   Run cache maintenance only when deliberately selected, and rebuild only when
   required for the new analyzer/index. Execute one administrative attempt at a
   time and require success before proceeding.
4. Apply the target image, configuration and any successful rebuilt index claim
   while the operator configuration still specifies zero replicas. Render and
   inspect it, then set the operator configuration to one replica and apply it.
   Require readiness, retained capture/search checks and both public transports.
   Failure keeps the release unaccepted; follow [recovery](#rollback-and-recovery).

Configuration hashes cause replacement, but mounted certificates and environment
credentials do not hot-reload. For a Secret-only change that needs no schema or
storage administration, coordinate database credential validity or backend CA/SAN
trust first, update the operator-managed Secret, then explicitly restart:

```bash
"${kube[@]}" -n openlegal-serving rollout restart deployment/openlegal-server
"${kube[@]}" -n openlegal-serving rollout status deployment/openlegal-server --timeout=360s
```

Recheck private health, retained data and both public transports. Backend certificate
or CA changes also require the coordinated edge trust/restart procedure in the
[OxiBelt contract](oxibelt.md#kubernetes-nodeport-handoff). Retain required old trust
and credential recovery material until the transition is accepted. Do not print
Secret values to verify rotation.

## Rollback and recovery

Keep serving stopped when migration/rebuild outcomes or storage compatibility are
uncertain. A prior image alone does not reverse SQL migrations or index acknowledgment.
Use the compatibility and coordinated-restore requirements in
[fresh index recovery](#fresh-index-destination-and-recovery) and the
[canonical rebuild contract](database.md#offline-index-rebuild). Never rewind
acknowledgment or delete retained evidence to make an older release start.

Retain failed administrative destinations for investigation. A retry needs a new
Job attempt and, for rebuild, another fresh destination after actual termination
of the old process. Resume one backend only when the intended release and storage
are known compatible; repeat the same retained-data and public transport checks.

When private/NodePort serving succeeds but public HTTP fails, preserve the failed
result and consult the [observed edge recovery](#acceptance-checklist-and-evidence-boundaries).
The recorded disposable edge restart is a workaround, not a proven diagnosis or
stable production fix. Production recovery belongs to an explicitly authorized
maintenance window and requires renewed acceptance.

## Observability and acceptance

The private health listener provides `/live`, `/ready` and aggregate `/metrics`;
the [server contract](server.md#configure-and-run) owns their semantics. Use an
already authorized operational network and the selected monitoring policy. There
is no health Service/NodePort or public edge route. Readiness observes existing
state; separately verify retained identities and index catch-up when applicable.

Run the [bounded explicit-endpoint smoke](#phase-9-serving-acceptance) and record
its selected checks individually. Complete the wider
[acceptance checklist](#acceptance-checklist-and-evidence-boundaries) for networking,
backend trust, credential placement, storage and lifecycle. Disruptive negatives
belong in the disposable fixture or a separately authorized maintenance window.

For each release handoff, record revision/image identities, selected configuration
and storage generations, check commands and outcomes, independent review scope,
failed/not-run cases and the operator actions still required. Keep a new dated
operator acceptance record; do not overwrite the historical
[Phase 9 record](deployment-acceptance.md). The
[Alpine migration record](alpine-image-acceptance.md) records the later image and
disposable-cluster checks, including failed qualification. Distinguish local Docker, PostgreSQL,
disposable Kubernetes, observed hosted CI and target-production results. Document
sandbox, provider and platform acceptance separately.

## Production server image

### GHCR release images

The [release pipeline](../CONTRIBUTING.md#ghcr-image-publication) publishes
`ghcr.io/publicdata-stream/openlegal-server` and
`ghcr.io/publicdata-stream/openlegal-server-ingestion` for amd64 and arm64, and
`ghcr.io/publicdata-stream/openlegal-document-worker` for amd64 only. Stable
`X.Y.Z` and beta `X.Y.Z-beta.N` tags follow publication of a GitHub Release;
`X.Y.Z-build.<8-hex-commit-prefix>` tags follow an annotated tag push. There
are no `latest` or major-version aliases. Each architecture is smoke-tested
before its tested image is pushed; a final server or ingestion multi-platform
reference is assembled only after both native gates pass. The workflow uses
a complete recent main-push CI attempt for the exact release commit, or runs
full CI when that evidence is unavailable; both check the committed `0.0.0`
Rust placeholder. It then stamps the validated full tag into each image job's
Rust build inputs before locked compilation and smoke testing. The tagged
source includes the stamping script needed to reproduce those build inputs.
The workflow attests platform and final digests, and uploads
`ghcr-release-digests` with exact digest references and the matching public
corresponding-source URL.

Use the digest from that release artifact and verify the registry manifest before
replacing the non-pullable image sentinel in an operator copy. For example:

```text
ghcr.io/publicdata-stream/openlegal-server@sha256:<published-manifest-digest>
ghcr.io/publicdata-stream/openlegal-server-ingestion@sha256:<published-manifest-digest>
ghcr.io/publicdata-stream/openlegal-document-worker@sha256:<published-image-digest>
```

The worker has no arm64 image. Qualify amd64 nodes for x86-64-v3 before running
server or worker there; the manifest's amd64 label alone does not prove CPU
compatibility. Before the first public release is accepted, a package admin must
make all three GHCR packages public and verify anonymous pulls for each supported
platform. Keep Kubernetes templates as digest placeholders until the operator
selects an accepted release. GHCR publication does not deploy, qualify a target
cluster, enable ingestion, or authorize live provider traffic.

### Local image builds

Build from the repository root with Docker BuildKit. The production platforms are
Alpine 3.24 Linux musl x86_64 with **x86-64-v3 required**, and generic AArch64. An amd64 manifest
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
caches are supported; compiled caches are separated by architecture, libc and
toolchain. All production stages use digest-pinned Alpine 3.24 variants; the
frontend is also digest-pinned. Native build/runtime packages come from signed,
moving v3.24 repositories, and application graphs use committed lockfiles.
Package selection can change between builds. Preserve the installed APK inventory
and accepted image digest; rebuilding old source is not an image rollback.

OCI labels record the source repository, revision, version, license and CPU
baseline. Defaults are `unknown` revision and `development` version; set truthful
values when producing an operator artifact. Build from a clean committed tree
before identifying an image as that revision. Local tags above are disposable
build handles; deployments must use the digest of the published immutable image.
Custom-registry publication remains a separate operator action.

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
and requires the explicit `runtime-ingestion` image target; the default image
contains no `kubectl`.

The image contains the CA trust bundle, musl runtime libraries and standard Alpine
BusyBox/APK utilities, but no compiler,
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
image and exercises both the text-only fixture and retained-corpus profile. It
checks image identity, libraries, widgets, notices, absent build tools, non-root
read-only operation, invalid configuration, health, MCP text comparison and
SIGTERM shutdown. Disposable certificates and fixture data travel through named
volumes, supporting development containers whose Docker daemon runs on the host.
An internal network connects the clients, server and disposable PostgreSQL 18;
there are no published host ports or production/provider credentials.

Retained acceptance provisions the complete pinned dictionary, verified PostgreSQL
TLS and separate migration/runtime roles. A separate fixture command migrates the
database; a test-only helper publishes fictional corpus evidence through existing
publication APIs and exits before serving starts. The production image contains no
seed helper. The gate checks retained search and capture identity, then gracefully
stops and restarts against the same database and storage. It requires unchanged
capture identity and private blob permissions, and normal startup failure for
missing credentials, invalid database trust, unsafe blob permissions and a missing
or corrupt dictionary. Timeouts and OOM kills are failures, not accepted negative
results. Startup and memory measurements are diagnostic evidence for the provisional
resource budget below. Requests, polling and cleanup remain bounded.

Dictionary provisioning may download the pinned source archive. Set
`MECAB_SOURCE_ARCHIVE` to reuse a checksum-verified archive, or
`OPENLEGAL_TEST_MECAB_DICTIONARY` to reuse a complete provisioned dictionary.
Ordinary tests never contact legal providers. An absent dictionary is an incomplete
gate, not a skipped success.

The ordinary CI image jobs use native `ubuntu-26.04` and `ubuntu-26.04-arm` runners
with read-only permissions. The separate release jobs publish the same images
after full CI succeeds. Label local ARM emulation and native CI evidence separately.
Docker acceptance does not establish kubelet ownership behavior, real-cluster
isolation, live provider or public transport acceptance. Existing PostgreSQL,
Korean analyzer, OxiBelt and document-worker gates remain separate. Record actual
check outcomes with the implementation handoff.

## Retained-corpus serving template

The [serving Kustomization](../deploy/kubernetes/serving/) generates a Namespace,
Deployment, content-hashed ConfigMap, NodePort Service and default-deny NetworkPolicy.
Required allow policies are applied separately before administration or startup.
It enables supplied-text
comparison and retained-corpus serving with persistent PostgreSQL, two blob stores and a corpus
index. Ingestion is omitted. There is no provider credential, controller identity
or document-worker access. Retained serving still writes index events and performs
retention maintenance. The former text-only configuration is now a
[test fixture](../test-support/deployment/text-only/), selected explicitly in checks.

### Operator prerequisites

- Select the matching [published server digest](#ghcr-release-images). Replace the entire
  `registry.example/openlegal-server@sha256:000...000` reference with its real
  registry and immutable SHA-256 digest. The all-zero digest is an intentionally
  non-pullable sentinel; a validated reference does not prove an image exists.
- Replace `[source].url`'s release revision with free corresponding source for the
  exact running server and widgets, including modifications and build material.
- Replace both backend authority placeholders with the authorities sent by
  OxiBelt: `NODE_DNS:30080` for HTTP and `NODE_DNS:30433` for WebTransport,
  where `NODE_DNS` is the selected private node hostname. The intended browser
  Origins are `https://openlegal4everyone.mcp.publicdata.stream` and
  `https://openlegal4everyone.api.publicdata.stream`; retain explicit Origin validation. Follow
  the [OxiBelt hosting contract](oxibelt.md#adapt-the-configuration-for-hosting).
- Establish and verify the [NodePort firewall restrictions](#service-and-private-network-handoff)
  before applying the serving Kustomization, which creates the Service. Apply the
  [network baseline and tailored allows](#network-and-namespace-boundaries) before
  resuming any administrative Job or starting serving.
- Qualify Linux amd64 or ARM64 nodes before applying `openlegal.server/ready=true`.
  Verify the [x86-64-v3 baseline](../CONTRIBUTING.md#rust-baseline) on amd64, resource
  availability, Restricted Pod Security and storage suitability. ARM64 uses the
  generic CPU baseline. The label is an operator assertion, not auto-detection.
- Provision PostgreSQL 18, distinct migration/runtime roles, grants and verified
  TLS according to [persistence](persistence.md#configuration-and-startup). Ordinary
  startup never migrates. Complete the [migration Job](#administrative-jobs) before
  serving. The serving configuration names `OPENLEGAL_MIGRATION_DATABASE_URL`,
  but the serving Deployment must never inject its credential.
- Create namespace `openlegal-serving` and the following operator-managed Secrets.
  Keep values and private keys outside Git, images, command histories and logs.

| Secret | Required keys | Serving access |
| --- | --- | --- |
| `openlegal-runtime-db` | `OPENLEGAL_DATABASE_URL` | Individual `secretKeyRef` into the runtime environment |
| `openlegal-migration-db` | `OPENLEGAL_MIGRATION_DATABASE_URL` | None; migration Job only |
| `openlegal-backend-tls` | `tls.crt`, `tls.key` | Read-only `/run/secrets/backend-tls`, mode `0440` |
| `openlegal-postgres-ca` | `ca.crt` | Read-only `/run/secrets/postgres-ca/ca.crt`, mode `0440` |

TLS volumes expose only the listed keys. Group `10004` supplies read access;
verify effective permissions on the target cluster. Issue the backend certificate
for the actual verified WebTransport authority. Preserve PostgreSQL `verify-full`,
including hostname verification. Do not put connection URLs in TOML or weaken TLS
to bypass certificate errors.

### Storage and permissions

Four separately provisioned claims mount volume roots; configured paths use a
private `data` child so Kubernetes volume-root permissions do not relax the blob
adapter's private-directory requirement.

| Claim | Container mount | Configured path | Serving access |
| --- | --- | --- | --- |
| `openlegal-cache-blobs` | `/var/lib/openlegal/cache-blobs` | Mount + `/data` | Writable |
| `openlegal-corpus-blobs` | `/var/lib/openlegal/corpus-blobs` | Mount + `/data` | Writable |
| `openlegal-corpus-index` | `/var/lib/openlegal/corpus-index` | Mount + `/data` | Writable |
| `openlegal-mecab-dictionary` | `/var/lib/openlegal/mecab-ko-dictionary` | Mount + `/data` | Read-only |

Use distinct, non-nested host roots and no shared claims. The separately applied
[storage examples](../deploy/kubernetes/storage/) include a non-default StorageClass
with `kubernetes.io/no-provisioner` and `WaitForFirstConsumer`, four Local PVs with
`Retain`, common explicit node affinity and static claim reservations, and four
matching filesystem `ReadWriteOnce` PVCs. Replace every host-path, node and capacity
placeholder in an operator-owned copy. PV and PVC capacities must match, and all
four PVs must be available on the selected qualified node. These examples are not
included in the serving Kustomization. Local PVs do not provide automatic failover;
`ReadWriteOnce` does not fence concurrent processes on one node. See the
[Kubernetes Local PV guidance](https://kubernetes.io/docs/concepts/storage/volumes/#local).

For ZFS-backed Local PVs, the operator owns dataset creation, mountpoints, mount
availability before scheduling, quotas and recovery. A declared PV/PVC capacity
is not an enforced filesystem quota, and the application's
[corpus/staging ledger limits](database.md#operator-configuration) do not account
for PostgreSQL, WAL, indexes, simultaneous rebuild generations, snapshots or
filesystem overhead. Budget and monitor that additional space within the selected
storage ceiling; a full pool or dataset can fail writes even when an application
ledger has room. No ZFS provisioning, tuning, replication or backup job is supplied.

Record which datasets/volumes and database belong to each recovery set. Retain
compatible release/configuration/dictionary artifacts, and verify an operator-owned
coordinated database/blob restore procedure on disposable storage before relying
on it. Independent dataset snapshots alone do not demonstrate a coherent database
and blob recovery point. Do not roll back a live dataset beneath a running process;
use the [stopped recovery procedure](#rollback-and-recovery). Local PV `Retain`
preserves reclaim ownership, not backups or automatic recovery.

On a freshly provisioned volume, prepare its root as `root:10004`, mode `2770`.
Prepare each writable `data` child as `10004:10004`, mode `0700`; blob files must
remain `0600`. The Pod uses `fsGroup: 10004` and `fsGroupChangePolicy: OnRootMismatch`.
A matching root allows Kubernetes to skip recursive permission changes, preserving
private children. A mismatched root can trigger recursive changes that break blob
admission. Storage drivers may handle ownership differently; verify preservation
on the actual cluster before retaining evidence. There is no repair init container,
and strict blob validation remains unchanged. See the
[Kubernetes ownership policy](https://kubernetes.io/docs/tasks/configure-pod-container/security-context/#configure-volume-permission-and-ownership-change-policy-for-pods).

The following Bash example is for **new empty operator-provisioned directories**
on the selected node. Replace the paths first; run privileged preparation through
the operator's normal host administration mechanism. Do not run recursive ownership
or permission repair against existing retained data.

```bash
set -euo pipefail
cache_volume=/REPLACE_WITH_CACHE_VOLUME_ROOT
corpus_volume=/REPLACE_WITH_CORPUS_VOLUME_ROOT
index_volume=/REPLACE_WITH_INDEX_VOLUME_ROOT
dictionary_volume=/REPLACE_WITH_DICTIONARY_VOLUME_ROOT
for volume_root in "$cache_volume" "$corpus_volume" "$index_volume" "$dictionary_volume"; do
  case "$volume_root" in /REPLACE_*|/|"") exit 1 ;; esac
  test ! -e "$volume_root/data" || exit 1
  sudo install -d -o 0 -g 10004 -m 2770 -- "$volume_root"
done
for volume_root in "$cache_volume" "$corpus_volume" "$index_volume"; do
  sudo install -d -o 10004 -g 10004 -m 0700 -- "$volume_root/data"
done
```

Provision the dictionary using the existing helper in a new staging directory,
then copy only that newly built artifact to the empty dictionary volume. The
helper needs the pinned Rust toolchain and may download the checksum-pinned source;
`MECAB_SOURCE_ARCHIVE` permits reuse. These commands do not modify retained blobs.

```bash
dictionary_stage=$(mktemp -d)
scripts/prepare-korean-dictionary.sh "$dictionary_stage/data"
sudo cp -a -- "$dictionary_stage/data" "$dictionary_volume/data"
sudo chown -R 0:10004 -- "$dictionary_volume/data"
sudo find "$dictionary_volume/data" -type d -exec chmod 0550 {} +
sudo find "$dictionary_volume/data" -type f -exec chmod 0440 {} +
rm -rf -- "$dictionary_stage"
```

Keep the dictionary mounted read-only, including the PVC reference. The server
never downloads, discovers or replaces it. Dictionary replacement and offline index
rebuild follow the [corpus contract](database.md#operator-configuration). Stop all
serving processes before maintenance and preserve the database corpus lease.
Neither Kubernetes scheduling nor `Recreate` substitutes for that lease. Rollback
requires a compatible binary, dictionary and index; retain prior artifacts and
follow the documented monotonic acknowledgment/rebuild constraints.

### Apply and lifecycle contract

Use an operator-owned copy/overlay, render it before applying, and select an explicit
kubeconfig/context for every cluster command. Create the namespace, Secrets and
prepared storage first, establish the NodePort firewall restrictions below, apply
the default-deny baseline and selected allow policies, and require migration success
before starting the Deployment. On first installation,
create the namespace from `serving/namespace.yaml` separately; applying the serving
Kustomization creates a one-replica Deployment immediately. Follow the
[administration sequence](#administrative-jobs) before the example below. On an
upgrade, keep the operator serving overlay at zero replicas until administration
has succeeded. For example, after all placeholders and prerequisites have been
resolved and the selected administrative Jobs have completed:

```sh
kubectl --kubeconfig /absolute/operator/kubeconfig --context OPERATOR_CONTEXT \
  apply -k /absolute/operator/serving
kubectl --kubeconfig /absolute/operator/kubeconfig --context OPERATOR_CONTEXT \
  -n openlegal-serving rollout status deployment/openlegal-server --timeout=360s
```

`WaitForFirstConsumer` supports scheduling-aware binding; these explicitly reserved
PV/PVC pairs can bind before a consumer because `volumeName` fixes the selection.
The scheduler still enforces the Local PV node affinity. Namespace labels use the cluster's `latest` Restricted policy; review admission on Kubernetes
upgrades. Validation-tool versions are not a cluster compatibility matrix.

The retained template exposes only the data transports through the Service
described below. Health port 9090 has no Service port or public route. There is no
Ingress resource. The default-deny baseline and selected allow policies restrict
Pod traffic when the CNI enforces them; enforcement requires acceptance on the
selected cluster. The [disposable record](deployment-acceptance.md) covers only its
tested topology. Monitoring access to health is optional and explicit.
`kubectl port-forward` cannot validate UDP/WebTransport; a forwarded HTTP client
must still send an allowed Host.

The Pod uses UID/GID/fsGroup `10004`, dropped capabilities, no privilege escalation,
RuntimeDefault seccomp, a read-only root and read-only configuration/Secret mounts.
There is no writable `/tmp` or service-account token. Document-worker isolation
remains separate. Requests are **1 CPU / 2 GiB** and limits are **2 CPUs / 4 GiB**,
a provisional retained-corpus envelope that must be measured with the complete
four-copy MeCab dictionary, Lindera, index readers and concurrent work. The 256 MiB
transport buffer is not an RSS ceiling. Exceeding the envelope fails acceptance
and requires investigation, rather than silently raising limits or skipping tests.

The `/live` startup probe allows 60 attempts at five-second intervals with a
two-second timeout (nominally 300 seconds). Readiness and liveness start only after
startup succeeds. Existing `/live` and `/ready` probes retain their settings.
Readiness observes existing health state; it is neither a fresh database check nor
an index-catch-up gate. The image receives SIGTERM directly; its configured drain
is 15 seconds and termination grace is 30 seconds, with no sleeping preStop hook.
Graceful-exit evidence covers an already-ready server; early initialization precedes
registration of the normal serving shutdown handler.

Exactly one desired replica and `Recreate` order Deployment upgrades. Kubernetes
[does not guarantee non-overlap for manual Pod deletion](https://kubernetes.io/docs/concepts/workloads/controllers/deployment/#recreate-deployment).
Do not add replicas, share the corpus index or treat the access mode as fencing.
Kustomize's ConfigMap hash triggers replacement when configuration changes; retain
the suffix. Configuration, certificates and runtime credentials do not hot-reload.
Secret rotation requires an explicit Deployment rollout restart and readiness
verification. `Recreate` entails downtime. Retain compatible previous image/config,
Secrets, dictionary and storage recovery material; `apply -k` does not automatically
garbage-collect old generated ConfigMaps.

### Service and private network handoff

[`Service/openlegal-server`](../deploy/kubernetes/serving/service.yaml) in
`openlegal-serving` selects the existing single-replica Deployment. Its two ports
are fixed and named; the health listener is excluded.

| Port name | Protocol | NodePort | Service / Pod port |
| --- | --- | --- | --- |
| `mcp-http` | TCP | `30080` | `8080` |
| `mcp-webtransport` | UDP | `30433` | `4433` |

The Service uses `externalTrafficPolicy: Cluster`, allowing a reachable node to
forward to the serving Pod even when it runs elsewhere. This does not add replicas
or storage failover. The cluster must admit these fixed NodePorts without a port
collision. Use the selected private node DNS name in both upstream URLs in the
[OxiBelt handoff example](../deploy/oxibelt/kubernetes-upstream.example.toml).
The [hosting instructions](oxibelt.md#kubernetes-nodeport-handoff) cover the matching
backend authorities, certificate identity and same-host gateway alternative.

**Before applying the Service**, restrict TCP 30080 and UDP 30433 to the intended
OxiBelt host/private path on every node/interface where the Service is reachable.
A private DNS record does not limit NodePort exposure. Inspect the cluster's actual
service-proxy implementation and effective address selection; where kube-proxy is
used, review `nodePortAddresses` and its proxy-mode behavior. Do not assume a
loopback or private-only binding. Cluster routing and Docker networking may change
the source address, so verify the effective firewall path and do not treat the
backend-observed source IP as client authentication. See the
[Kubernetes NodePort guidance](https://kubernetes.io/docs/concepts/services-networking/service/#type-nodeport).

After applying, operator acceptance must demonstrate HTTP through TCP 30080 and
WebTransport through UDP 30433 from the OxiBelt container, and denial from unintended
networks across all exposed nodes/interfaces. Verify backend certificate trust and
hostname validation, allowed Host/Origin behavior, and absence of public health
routes. Run these checks again after service-proxy, CNI, Docker, firewall or node
changes. Offline rendering and Docker fixtures do not establish these properties
on a real cluster. The [network policies](#network-and-namespace-boundaries)
complement these host restrictions; no intermediary proxy is introduced.

### Network and namespace boundaries

The [network roots](../deploy/kubernetes/network/) each render one NetworkPolicy
in `openlegal-serving`. The retained serving root imports only `network/base`;
allow policies are deliberate, separate operator selections. Applying serving alone
can block its database access and prevent startup. The text-only fixture has no
network policy and does not establish namespace isolation.

| Root | Workloads / direction | Permitted peer and ports |
| --- | --- | --- |
| `base` | Every Pod / ingress and egress | Nothing; namespace default deny |
| `edge` | Serving / ingress | Explicit edge source host IPs; TCP 8080 and UDP 4433 |
| `postgres-in-cluster` | Serving and all administrative modes / egress | Combined database namespace and Pod selectors; TCP 5432 |
| `postgres-external` | Serving and all administrative modes / egress | Explicit database host IPs; TCP 5432 |
| `dns-cluster` | Serving and all administrative modes / egress | Combined cluster DNS namespace and Pod selectors; TCP and UDP 53 |
| `dns-fixed` | Serving and all administrative modes / egress | Explicit resolver host IPs; TCP and UDP 53 |
| `monitoring` | Serving / ingress | Combined monitoring namespace and Pod selectors; TCP 9090 |

Select exactly one PostgreSQL variant, zero or one DNS variant, and monitoring only
when needed. The alternatives use the same resource names (`openlegal-allow-postgres`
and `openlegal-allow-dns`) so changing variant replaces the existing rule. Do not
combine both variants in one Kustomization or apply them concurrently. Default deny
selects every Pod; allow rules select `app.kubernetes.io/name: openlegal-server`
and, for database/DNS egress only, `openlegal-admin`. Administrative Pods receive no
ingress allowance.

Tailor an operator-owned copy outside Git. Replace reserved `192.0.2.*` addresses
and every `replace-with-*` selector; verify the example cluster DNS labels against
the actual resolver. Use individual `/32` IPv4 or `/128` IPv6 host addresses,
including each intended dual-stack destination, and the actual database port if it
differs from 5432. Keep namespace and Pod selectors together in one peer to require
both. Do not use unrestricted CIDRs, empty allow peers, namespace-wide database
allows or broad node exceptions. Track address drift and coordinate policy updates
with destination changes. Keep database `verify-full` and its verified hostname;
an IP allow rule does not require changing the connection hostname or disabling TLS.
DNS can be omitted only when the selected application addressing needs no lookup.
DNS permission does not restrict queried domain names or authorize connections to
the returned addresses. No provider HTTPS or Kubernetes API egress is included.

NetworkPolicy requires an enforcing CNI. Allows are additive: another matching
policy can broaden access despite default deny. For Pod-to-Pod traffic, both source
egress and destination ingress must allow it. Inventory all policies selecting these
workloads and inspect unknown policies before acceptance. Node-originating traffic
has exemptions, and address translation relative to policy enforcement varies by
network implementation. With `externalTrafficPolicy: Cluster`, do not assume the
edge IP remains visible; determine the policy-visible source along the actual path.
The required node firewall remains part of the boundary. See the
[Kubernetes NetworkPolicy guidance](https://kubernetes.io/docs/concepts/services-networking/network-policies/).

For NodeLocal DNS, select and verify the actual local resolver address in
`dns-fixed`. A host-network resolver or node-local path may not be controlled as an
ordinary DNS Pod by the CNI; do not infer enforcement from this rule's presence.
Verify both TCP and UDP, Service translation and host-network handling. See
[NodeLocal DNSCache](https://kubernetes.io/docs/tasks/administer-cluster/nodelocaldns/).

Verify kubelet probes on the target cluster without adding a broad kubelet/node
allow. A CNI-specific probe exception, if actually required, needs a separately
reviewed narrowly scoped operator rule. The optional monitoring rule opens the
entire unauthenticated health listener: `/live`, `/ready` and `/metrics`. It cannot
restrict an HTTP path. Port 9090 remains absent from the Service and public edge.

#### Bootstrap and policy changes

For first installation, prepare the firewall, then create the Namespace, apply
default deny and the tailored allow policies, verify enforcement, and only then
resume migration. Prepare Secrets and storage as required by each mode; complete
migration before starting serving. For an existing installation, introduce or
change these restrictions during the [administrative maintenance window](#prepare-stop-and-execute),
with serving stopped and administrative Jobs suspended. Do not briefly delete
default deny to diagnose a blocked dependency.

The following operator example selects an in-cluster database, cluster DNS and no
monitoring. Replace paths, context, peers and ports before use; the repository
examples are intentionally not ready-to-apply production policies.

```bash
kube=(kubectl --kubeconfig /absolute/operator/kubeconfig --context OPERATOR_CONTEXT)
"${kube[@]}" apply -f /absolute/operator/serving/namespace.yaml
"${kube[@]}" apply -k /absolute/operator/network/base
"${kube[@]}" apply -k /absolute/operator/network/edge
"${kube[@]}" apply -k /absolute/operator/network/postgres-in-cluster
"${kube[@]}" apply -k /absolute/operator/network/dns-cluster
"${kube[@]}" -n openlegal-serving get networkpolicies -o yaml
# Verify enforcement and the chosen destinations before resuming migration.
# Complete administration before applying serving with one replica.
```

Use `postgres-external` instead when appropriate, and omit DNS or substitute
`dns-fixed` as selected. Apply `monitoring` separately if required. NetworkPolicy
updates may converge asynchronously; inspect the CNI's enforcement state and test
new connections before proceeding.

A later apply does not remove policies omitted from that apply. When disabling
monitoring, explicitly delete only `networkpolicy/openlegal-allow-monitoring`; when
removing DNS, delete only `networkpolicy/openlegal-allow-dns`, after confirming all
selected workloads can operate without it. Use the explicit context above, for
example:

```bash
"${kube[@]}" -n openlegal-serving delete networkpolicy/openlegal-allow-monitoring
# Only when DNS is also deliberately disabled:
"${kube[@]}" -n openlegal-serving delete networkpolicy/openlegal-allow-dns
```

Inventory policies again after each change. Review unknown or differently named
legacy allows and remove them only after establishing ownership and intended use;
never delete all policies as cleanup. On full workload decommission, remove the
repository-owned `openlegal-allow-edge`, `openlegal-allow-postgres`,
`openlegal-allow-dns` and `openlegal-allow-monitoring` by exact name as applicable.
Keep `openlegal-default-deny` while the namespace exists with any workloads.

#### Target-cluster network acceptance

Phase 6 templates alone provide no cluster evidence. The separately provisioned
Phase 9 fixture records only its own observed boundaries. Record cluster/CNI/service-proxy versions,
node interfaces, selected policy manifests, firewall rules and policy-visible peers.
Use operator-controlled synthetic endpoints and bounded connection attempts. For
each denied path, demonstrate that the endpoint is listening and reachable from an
appropriate permitted control, then correlate the rejected attempt with CNI/firewall
denial evidence. A timeout, authentication failure, TLS rejection or missing listener
alone does not prove network denial. Use fresh connections after policy convergence.

- From the actual OxiBelt container, verify HTTP and native WebTransport through
  the two NodePorts, retaining certificate, Host and Origin checks. From unintended
  networks, verify rejection on every exposed node/interface and both protocols;
  include direct Pod paths when routable. Node-exempt paths require firewall evidence.
- Verify PostgreSQL with hostname-validated TLS for serving and each of migration,
  maintenance and rebuild under the appropriate credential and storage contracts.
  Exercise administrative operations against disposable synthetic data and fresh
  rebuild storage; do not run destructive acceptance against retained production data.
- Verify selected DNS over UDP and TCP, including actual Service/NodeLocal routing.
  Reject unselected resolver destinations using synthetic reachable controls. With
  DNS omitted, demonstrate that serving and every administrative mode still work.
- Verify startup, liveness and readiness probes. If monitoring is selected, verify
  access to all three health routes only from the selected namespace-and-Pod pair;
  reject matching Pods in another namespace and unrelated Pods in that namespace.
  With monitoring absent, ordinary Pod access to TCP 9090 must be rejected.
- Reject unrelated Pod ingress to serving and administrative Pods, and unrelated
  egress from each selected workload label. For administrative ingress, use a
  disposable restricted synthetic listener with matching labels; closed ports on
  the real Jobs are not denial evidence. Do not add test listeners to production Pods.
- Verify no unintended API/provider egress rule exists and demonstrate rejection
  of representative TCP HTTPS paths with operator-controlled reachable synthetic
  endpoints. Make no authenticated API operations or live legal-provider requests;
  synthetic denial checks alone do not establish reachability of every real endpoint.
  Record CNI/firewall evidence for any node-address exemptions.

The [document namespace](document-sandbox.md) remains a separate trust boundary:
its deny-all ingress/egress, two-Pod quota and qualified `runc` RuntimeClass
remain required. No serving policy grants parser access or a fetch role. Its existing
real-cluster sandbox acceptance remains independently pending. Offline manifests,
Docker OxiBelt tests and synthetic controls do not establish completed production,
live-provider, browser or ChatGPT acceptance.

### Local and CI checks

```sh
scripts/setup-deployment-tools.sh
scripts/test-kubernetes-serving.sh --profile retained
scripts/test-kubernetes-serving.sh --profile text-only
scripts/test-oxibelt.sh --profile fixture
scripts/test-oxibelt.sh --profile kubernetes
scripts/test-server-image.sh --platform linux/amd64
scripts/test-server-image.sh --platform linux/arm64
```

Every manifest invocation validates both profiles and both Kubernetes schema versions.
`--profile` defaults to `retained` and selects which TOML `--config-output PATH`
exports. Provisioning requires Python 3.11–3.14 with venv/ensurepip,
curl and SHA-256 utilities. It downloads pinned verification tools explicitly;
validation is offline with no kubeconfig, discovery or client dry-run.
`OPENLEGAL_DEPLOY_TOOLS` selects the provisioned directory (default
`target/deployment-tools`). See [tool provenance](dependencies.md#deployment-validation-tools).

Before rendering, the gate checks an explicit inventory of all Kubernetes template
and text-only fixture files, plus the document namespace/controller Role and OxiBelt
handoff sources. Missing, unexpected and symlinked inputs fail. Every file has a
validation owner; local Kustomize references must resolve within that inventory.
Remote references, unreviewed generators, plugins and secret generators are rejected
before kubectl runs. Adding a source requires extending its reviewed validation
coverage, not merely listing its filename.

Raw source checks reject high-confidence credential patterns, including private-key
markers and credential-bearing URLs in comments, and forbidden inline credentials.
Secret references, token-file paths and documented placeholders remain supported.
These checks are scoped guards, not a general secret-scanning guarantee. Failures
report rules and locations without source excerpts or credential values.

Explicit setup provisions kubeconform and the 28 required schema files with committed
checksums. Validation verifies the local bundle and uses only its strict schemas;
missing schemas, changed assets, external schema references and skipped resources
fail without a network fallback. To repair a corrupt installation, remove only
`schema-validation/` inside the selected deployment-tools directory and rerun setup.
Do not remove unrelated caches or data.

The current 47 resource documents cover 14 Kubernetes resource types. Storage
examples first pass their existing exact placeholder checks. Only temporary schema
copies replace their five PV/PVC capacity pairs with `1Gi`, five local paths with
distinct `/var/lib/openlegal/schema-fixture/` paths and node affinity with
`schema-fixture-node`. Each replacement requires the exact expected field and old
value. These values are synthetic, are never exported as operator configuration and
do not recommend production sizing. Embedded TOML/kubeconfig and Kustomize inputs
retain their dedicated semantic checks rather than being treated as workload APIs.

The fast gate validates rendered YAML/TOML relationships, profile-specific security,
lifecycle and storage invariants, and rejection of unsafe mutations. It validates
Local PV/PVC examples without applying them, requires five objects for retained
serving and three for text-only, and checks the Service port/selector contract
against the OxiBelt example. It rejects serving migration/provider
credentials, inline connection URLs, insecure TLS, overlapping storage paths,
reused claims, writable dictionaries, missing ownership policy and mutable images.
Every invocation also renders the three independent admin roots and checks their
image/configuration identity, credentials, mounts, suspended lifecycle and exclusion
from the serving Service selector. Separate fresh-rebuild PV/PVC examples are checked
without applying them. `--admin-output-dir DIR` exports validated Job/ConfigMap
YAML for fixture use; this is template validation, not production-value admission.
Rust configuration tests exercise the rendered TOML through existing validation.
It also validates each independent network root, all supported database/DNS/monitoring
combinations, workload selectors and the unchanged document-sandbox network, quota
and prepared-node RuntimeClass invariants. Duplicate, missing or unexpected resources
and broadened peers/ports fail validation. This gate checks committed templates,
not arbitrary operator overlays or production values. These are repository checks,
not API-server admission or enforcement proof. Schema validation is additive to
the project-specific invariants; it does not establish scheduling, authorization,
CNI enforcement, storage availability or correctness of operator substitutions.

The image gate consumes rendered configurations with the same runtime paths,
using disposable values for authorities, corresponding source and certificates.
It exercises both HTTP MCP revisions, Host/Origin denial, packaged widgets and
text workers, then retained search/restart and startup failures described above.
Fixtures use named Docker volume subdirectories; no host ports are published.
All four native CI image jobs provision the required tools and full dictionary.

The two OxiBelt profiles exercise the pinned edge with disposable identities.
The Kubernetes profile consumes the committed handoff example and connects to a
fixture backend listening directly on 30080/30433. It does not create a Kubernetes
Service or test NodePort translation, firewalls or real-cluster routing. See the
[OxiBelt gate](oxibelt.md#run-the-isolated-integration-test) for covered failures.

Changes to this storage/image boundary also require the Rust baseline, PostgreSQL,
Korean tokenization and OxiBelt gates in [CONTRIBUTING](../CONTRIBUTING.md#testing-and-ci),
plus shellcheck for changed shell scripts and actionlint for workflow changes.
Local ARM64 emulation does not establish native CI success. Real-cluster admission,
Local PV binding, kubelet permission preservation across restart, Secret permissions,
probe timing, rollout ordering and network enforcement remain operator gates.

## Administrative Jobs

The independent [admin roots](../deploy/kubernetes/admin/) each render one
`batch/v1` Job and the shared ConfigMap, without a Deployment, Service, Secret,
namespace or PVC. They require **Kubernetes 1.34 or later** for
`podReplacementPolicy: Failed`. Serving does not include them. Use an operator-owned
copy preserving the `config`, `serving`, `admin`, `network` and `storage` directory
relationships.
Replace image references in serving and every selected Job with the same immutable
image digest for the intended release. Render and compare the images and generated
configuration before applying. Do not use a ConfigMap from a different release.

All commands receive the full TOML and read-only PostgreSQL CA. The full TOML must
parse even when it references serving files that the command does not open.

| Root / Job | Arguments | Additional access |
| --- | --- | --- |
| `admin/migrate` / `openlegal-migrate` | `--migrate /etc/openlegal/server.toml` | Migration credential only; no PVCs |
| `admin/maintain` / `openlegal-maintain` | `--maintain /etc/openlegal/server.toml` | Runtime credential and writable cache blob PVC only |
| `admin/rebuild` / `openlegal-rebuild` | `--rebuild-corpus-index /etc/openlegal/server.toml` | Runtime credential, writable cache/corpus blobs and fresh index PVC, read-only dictionary |

No Job receives backend TLS keys, provider credentials, a service-account token or
cluster-control permissions. Both blob stores need writable mounts during rebuild
because initialization/health performs filesystem canary writes; retained evidence
is not rewritten. The old index is not mounted in the rebuild Pod.

Jobs start suspended, with one completion, parallelism one, `restartPolicy: Never`,
zero retries, and replacement delayed until the previous Pod is terminal/failed.
These settings limit accidental repetition but do **not** guarantee exactly-once
execution. Database migration locking and the corpus rebuild lease retain their
existing roles. A duplicate rebuild may fail on an occupied lease or populated
destination and requires investigation. See the
[Kubernetes Job lifecycle](https://kubernetes.io/docs/concepts/workloads/controllers/job/).

| Operation | Deadline after activation | CPU / memory request | CPU / memory limit |
| --- | --- | --- | --- |
| Migration | 600 seconds | 100m / 128 MiB | 1 / 512 MiB |
| Cache maintenance | 300 seconds | 100m / 128 MiB | 1 / 512 MiB |
| Index rebuild | 86400 seconds | 1 / 2 GiB | 2 / 4 GiB |

These are provisional budgets, not full-corpus sizing evidence. The cache command
also retains its internal sixty-second pruning deadline. Investigate OOM, deadline
expiry or insufficient capacity before preparing a new attempt with deliberately
revised limits. Do not silently increase resources. Jobs retain the serving node
qualification and container hardening, including a 30-second termination grace;
admin modes do not promise serving's graceful SIGTERM handling.

### Prepare, stop and execute

1. On first installation, apply the standalone namespace manifest and establish
   the firewall and [network policies](#network-and-namespace-boundaries). Provision
   external PostgreSQL roles/credentials and the CA, then create the migration Job.
   Migration does not need storage or backend TLS. Runtime grants must be applied
   by the database administrator after the relevant tables exist; migration does
   not provision roles, passwords or grants.
2. Before an upgrade or any cache maintenance/rebuild, stop all serving, ingestion,
   retention and other publishing processes using this database/storage. Keep the
   operator serving overlay at `replicas: 0`, scale the existing Deployment to zero,
   and prevent deployment automation from restoring replicas during the window.
   Verify actual termination of every relevant Pod/process, including prior admin
   attempts and legacy/out-of-cluster processes. Zero ready replicas, a missing Pod
   object, `ReadWriteOnce`, and the corpus lease alone are not proof of shutdown.
3. Render the selected admin root, verify its exact image/config, credential and
   mount set, and apply it suspended. Run only one selected operation at a time.
   Use migration first when the intended binary requires a new schema; apply needed
   runtime grants before maintenance or rebuild. Neither operation is mandatory on
   every upgrade.
4. Resume only after prerequisites, including verified network enforcement and
   database/DNS access for administrative Pods, hold. Require `Complete=True`, successful process
   exit and actual termination before continuing. Failure or an ambiguous result
   keeps serving stopped. Inspect bounded operational diagnostics; keep credentials,
   SQL payloads and raw evidence out of shared logs or tickets.
5. After successful administration, apply the verified serving configuration/image
   and, if rebuilt, replacement index claim **while replicas remain zero**. Then
   set the operator overlay back to one replica, apply it, check rollout/readiness
   and perform bounded retained-search/capture checks. Restore exactly one backend.

The following examples are operator actions, not a repository deployment script.
Substitute all paths and context names first. For a new installation, complete
[first-deployment prerequisites](#first-deployment) and skip this shutdown block.
For an existing Deployment, keep the operator overlay at zero replicas before:

```bash
kube=(kubectl --kubeconfig /absolute/operator/kubeconfig --context OPERATOR_CONTEXT)
"${kube[@]}" -n openlegal-serving scale deployment/openlegal-server --replicas=0
"${kube[@]}" -n openlegal-serving wait --for=delete pod \
  -l app.kubernetes.io/name=openlegal-server --timeout=360s
"${kube[@]}" -n openlegal-serving get pods,jobs
```

After network/bootstrap prerequisites and, for existing installations, verified
termination of every relevant process, render the selected Job. This example uses
the first migration attempt's default name; use a new name for later attempts as
described below. Inspect the rendered file before applying it:

```bash
kube=(kubectl --kubeconfig /absolute/operator/kubeconfig --context OPERATOR_CONTEXT)
"${kube[@]}" kustomize /absolute/operator/admin/migrate > /absolute/operator/migrate-rendered.yaml
"${kube[@]}" apply -f /absolute/operator/migrate-rendered.yaml
"${kube[@]}" -n openlegal-serving patch job/openlegal-migrate \
  --type=merge -p '{"spec":{"suspend":false}}'
"${kube[@]}" -n openlegal-serving wait --for=condition=complete \
  job/openlegal-migrate --timeout=660s
```

Use the corresponding root and Job name for maintenance/rebuild; bounded waits may
allow 360 seconds for maintenance and 86460 seconds for rebuild. A wait timeout is
not permission to start another attempt or serving. Check Job conditions and all
associated Pods. There is no automatic restart of serving after an administrative
failure. Do not reapply a suspended manifest over an active Job: that can terminate
its Pods. Jobs are outside the ordinary serving apply/reconciliation path.

A changed image, ConfigMap reference or claim requires a **new Job**, because Job
Pod templates are immutable. An operator overlay may patch only the Job
`metadata.name` to a unique attempt name; use that rendered name in commands.
Avoid a root-wide `nameSuffix`, which would also rename the shared ConfigMap. Retain old Job/Pod diagnostics
until investigated, then explicitly clean up terminated attempts and unreferenced
ConfigMaps. There is no TTL controller configuration or automatic volume deletion.

### Cache maintenance window

`--maintain` performs bounded **cache** pruning and cleanup, not corpus-history
pruning, and does not promise to exhaust all orphan/deletion queues. It opens only
the cache blob root. It acquires no corpus runtime lease; its process-local cache
invalidation cannot fence a separate serving process. Keep serving and all writers
stopped, including when changing retention limits in the shared config. Use the
same limits for the administrative attempt and subsequent serving. The
[persistence retention contract](persistence.md#blob-publication-and-garbage-collection)
owns pruning behavior. Corpus retention remains part of the ordinary corpus runtime.
No CronJob or arbitrary schedule is supplied.

### Fresh index destination and recovery

Copy the separate
[fresh PV](../deploy/kubernetes/storage/local-rebuild-pv.example.yaml) and
[fresh PVC](../deploy/kubernetes/storage/local-rebuild-pvc.example.yaml) examples.
Their default name is `openlegal-corpus-index-rebuild`; assign a unique PV/PVC pair
and host directory per attempt, and change the Job claim reference to match.
Capacity is operator-supplied. Use the existing `openlegal-local` StorageClass,
`Retain`, and the same qualified storage node as the retained blob volumes. Verify
physical host paths are distinct and non-nested, not aliases of the old index,
blobs or dictionary. Reserve enough space to retain both old and new indexes.

Prepare the new empty volume root and private `data` child using the
[storage ownership procedure](#storage-and-permissions). Mount the new claim at
`/var/lib/openlegal/corpus-index`; the shared TOML still points to its `data` child.
The old claim stays preserved and unmounted. The CLI rejects a populated destination.
After success, change only the serving index volume's claim to the successful new
claim while replicas remain zero; also verify the intended dictionary and image.
Retain prior image/configuration/dictionary/index recovery material.

Suspending or deleting a running Job, eviction and deadline expiration may abruptly
terminate rebuild. The CLI's interrupt handler does not establish graceful SIGTERM
cancellation. Interruption can leave an incomplete index, a complete index awaiting
acknowledgment, or an ambiguous outcome after acknowledgment. A nonempty directory
or completion metadata alone is not command success. Keep the failed destination
for investigation and retry with another fresh one only after the old process has
actually terminated. Do not resume an interrupted Job into its used destination.

The [canonical rebuild contract](database.md#offline-index-rebuild) governs replay,
completion and monotonic acknowledgment. Never rewind acknowledgment. A preserved
old index may be behind the database after rebuilding and cannot simply be selected
for rollback. Require a compatible binary, dictionary and complete index; otherwise
repair forward or use appropriate version-specific rebuild tooling. Schema migration
rollback likewise needs an individually assessed compatible binary/schema or a
coordinated restore of database and retained storage from operator recovery material;
reapplying an old image does not undo migrations. Keep serving stopped while resolving
uncertainty, and never discard retained legal evidence to make a rollback start.

Repository checks establish rendered relationships and disposable fixture behavior.
Target-cluster Job admission, scheduling, Secret permissions, storage binding,
shutdown exclusion, failure recovery and resource sizing remain operator acceptance
gates. These templates do not perform a deployment or live-provider acceptance.

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
`OPENLEGAL_MIGRATION_DATABASE_URL`, in the serving template. The serving workload
receives only the runtime credential; the operator migration command receives only
the migration credential. The [separate Job](#administrative-jobs) enforces this
mount/environment separation. Preserve
verified PostgreSQL TLS and operator CA inputs as described in the [persistence configuration](persistence.md#configuration-and-startup).

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

## Optional ingestion integration

Retained serving is the default. The [ingestion overlay](../deploy/kubernetes/ingestion/)
enables managed background LAW OPEN DATA traffic as soon as its server starts.
Applying it is an operator decision requiring separate live-provider authorization;
rendering it offline does not authorize traffic. It uses the same one-replica
Deployment and storage, not a second independently budgeted crawler.

### Ingestion image and configuration

Build the explicit target for each server architecture:

```sh
docker build --platform linux/amd64 --target runtime-ingestion \
  -f apps/server/Dockerfile --build-arg REVISION="$(git rev-parse HEAD)" \
  --build-arg VERSION=development -t openlegal-server-ingestion:local-amd64 .
docker build --platform linux/arm64 --target runtime-ingestion \
  -f apps/server/Dockerfile --build-arg REVISION="$(git rev-parse HEAD)" \
  --build-arg VERSION=development -t openlegal-server-ingestion:local-arm64 .
```

The final/default Docker target remains minimal `runtime`. Both targets preserve
UID/GID 10004, hardening, widgets and source metadata. Only ingestion includes the
checksum-verified kubectl v1.37.0 at `/usr/local/bin/kubectl` and its redistribution
notices. Initial API-server targets are Kubernetes 1.36–1.37, within the upstream
[version-skew policy](https://kubernetes.io/releases/version-skew-policy/).
The document worker remains separately built for amd64/x86-64-v3: qualify only
compatible worker nodes with `openlegal.document-sandbox/ready=true`. ARM64 server
support does not imply ARM64 document-worker support.

The overlay replaces the generated server ConfigMap with retained configuration
plus `[database.ingestion]`. Its fixed context is `openlegal-document-controller`,
its namespace is `openlegal-documents`, and history-body ingestion remains disabled.
The same ConfigMap mounts `pilot-candidates.json` at the explicit
`manual_candidates_path`. Its committed default is empty. To use operator-held
list exports, run:

```bash
python3 scripts/prepare-law-pilot-candidates.py \
  --input-dir /absolute/operator/list-exports \
  --output /absolute/operator/ingestion/pilot-candidates.json \
  --allow-incomplete
```

Inspect the reported skips and file hashes. The
generator selects at most two untrusted IDs per category; a missing category
falls back to a bounded live list query. Replace only the JSON in the tailored
ingestion overlay. It does not seed public records or imply inventory coverage;
the running adapter fetches and checks live detail before revision-only
publication. A fresh live current-list observation is required before a
manual candidate can become public HEAD.
Replace both non-pullable server/worker digest sentinels with the accepted
`openlegal-server-ingestion` and `openlegal-document-worker` digests from the
[release artifact](#ghcr-release-images), and set the corresponding-source
revision in an operator copy. Keep all retained configuration fields consistent
with the shared serving/admin base when changing endpoints or storage.
Administrative Jobs keep their original minimal image and ingestion-free ConfigMap;
the ingestion ConfigMap has a different content hash by design.

Create `Secret/openlegal-law-provider` outside Git in `openlegal-serving`, with key
`OPENLEGAL_LAW_PROVIDER_CREDENTIAL`. Only the serving container references it. Never
put its value in TOML, kubeconfig, image layers, command arguments or test artifacts.
The server continues to receive only runtime PostgreSQL credentials.

### Explicit projected identity

The overlay creates `ServiceAccount/openlegal-document-controller` with automatic
token mounting disabled, also disabled on the Pod. A dedicated directory projection
supplies a token requested for 3,600 seconds and `kube-root-ca.crt`; omitting audience
uses the API server default. The actual token expiry is determined by the API server.
UID/GID 10004 can read the 0440 projection through the Pod's fsGroup. Do not use
`subPath`: directory projection must receive token rotation updates.

The nonsecret kubeconfig at
`/run/secrets/document-controller/config/kubeconfig` contains exactly one cluster,
user and context. It references absolute CA/token paths under the sibling
`/run/secrets/document-controller/identity` mount and defaults to
`https://kubernetes.default.svc:443`. There is no inline token, authentication plugin,
proxy URL or insecure TLS setting. See Kubernetes' [token projection guidance](https://kubernetes.io/docs/tasks/configure-pod-container/configure-service-account/#serviceaccount-token-volume-projection).

Apply the existing document namespace and `Role/document-controller` as sandbox
prerequisites. The separate [RoleBinding root](../deploy/kubernetes/ingestion/rbac/)
binds that unchanged Role in `openlegal-documents` to the ServiceAccount in
`openlegal-serving`; do not import it under a serving namespace transformer.
The controller has no Secret/log access or cluster-wide binding. Sandbox acceptance
uses a separate operator identity with the additional inspection permissions it
needs; do not broaden the controller Role to run that harness.

Every kubectl subprocess receives only fixed PATH/HOME/TMPDIR values and explicit
kubeconfig/context/namespace/cache arguments. Server credentials, ambient Kubernetes
configuration and proxy variables are not inherited. The executable remains trusted
code in the serving container, not a separate security sandbox. Existing external
controller setups relying on inherited proxy/plugin variables must be adapted.

Only ingestion mounts a writable 64 MiB disk-backed `emptyDir` at `/tmp`, with
ephemeral-storage request/limit of 64/128 MiB. This provisional budget covers kubectl
discovery/schema caches and logs and must be measured on the target cluster.
Kubernetes accounting/eviction is not a synchronous filesystem quota. The root
filesystem stays read-only; no new persistent application storage is introduced.

### Network preparation, activation and rollback

The overlay imports only the existing default-deny policy. Independently tailor and
apply [API egress](../deploy/kubernetes/network/ingestion-api/) and
[provider egress](../deploy/kubernetes/network/ingestion-provider/) examples. Both
select the serving application plus `openlegal.ingestion/enabled=true`; neither
selects administrative Jobs, retained-only Pods or document workers. The examples
use documentation-only /32 addresses and TCP 443. Replace these with verified
API/provider host addresses and actual API ports; never replace them with an
unrestricted internet or namespace-wide allow.

Apply an existing DNS allow policy for `kubernetes.default.svc` and `www.law.go.kr`.
Determine whether the CNI evaluates API Service traffic before or after DNAT, then
allow only the necessary Service/backend IPs and ports. Maintain provider address
changes explicitly, including /128 entries if using IPv6. Stale lists fail closed;
there is no automatic address update or broader-network fallback. NetworkPolicy
does not enforce provider hostnames; the existing HTTPS/destination policy remains
the application boundary. Parser Pods retain deny-all networking and receive no
controller token or provider credential.

Operator sequence (commands refer to an explicitly tailored copy):

1. Complete retained-serving acceptance and stop the sole backend during the
   activation window. Do not run an additional ingestion process against its index.
2. Prepare compatible document nodes and complete the separately configured
   [sandbox acceptance gate](document-sandbox.md#cluster-preparation-and-acceptance).
3. Provision immutable images, provider Secret, controller identity/RoleBinding and
   explicit DNS/API/provider policies. Inspect rendered namespace references before
   applying anything. Record real-cluster token rotation, RBAC denial outside the
   intended namespace, CNI enforcement and worker credential isolation.
4. Obtain separate authorization for bounded live-provider acceptance. Only then
   apply the ingestion overlay and complete that acceptance; its startup immediately
   enables managed upstream requests. Keep production acceptance pending until
   the required evidence is recorded.
5. Verify the single Pod becomes Ready, provider/sandbox failures remain bounded,
   and corpus freshness/coverage is reported honestly. Private readiness is not
   proof of successful ingestion or corpus completeness.

To disable ingestion, replace the Deployment/configuration with the retained root
and wait for the ingestion Pod to terminate. Explicitly delete its two allow
policies, RoleBinding and ServiceAccount once no ingestion Pod uses them; remove
the unused provider Secret/controller ConfigMap according to operator policy.
Reconcile leftover worker Pods with the operator identity. Applying a different
Kustomize root alone does not prune these separately applied objects. Do not delete
the shared document Role/namespace, retained storage or base network policies.

### Offline acceptance

```sh
scripts/setup-deployment-tools.sh
scripts/test-kubernetes-serving.sh
scripts/test-kubernetes-serving.sh --profile text-only
scripts/test-server-image.sh --platform linux/amd64 --target runtime-ingestion
scripts/test-server-image.sh --platform linux/arm64 --target runtime-ingestion
```

The manifest gate checks ingestion/configuration parity, cross-namespace RBAC,
projection/Secret boundaries and scoped egress along with retained/admin invariants.
The image gate exercises retained serving with ingestion disabled, then invokes
the packaged kubectl against a synthetic TLS API on an internal Docker network.
It checks quota/Pod command compatibility, token-file replacement and authentication
failures without legal-provider traffic. Tests use disposable credentials and do
not implement or prove Kubernetes RBAC, projected-token delivery, Pod admission,
runc user namespaces, AppArmor or CNI enforcement. Native ARM64 CI remains distinct from local emulation.
Real-cluster ingestion, live-provider and production traffic acceptance remain pending.

## Phase 9 serving acceptance

The routine smoke command performs fixed, bounded serving operations against
explicit endpoints. It neither deploys workloads nor runs migration, restarts,
certificate replacements or provider ingestion. Tiny fictional text comparisons
create transient comparison and patch handles; the client deletes only handles
returned to its own run. No legal corpus operation is included.

Build the native client explicitly before running smoke:

```sh
cargo build --locked -p openlegal-server --example wt_client
scripts/smoke-kubernetes-serving.sh \
  --http-url https://openlegal4everyone.mcp.publicdata.stream/mcp \
  --webtransport-url https://openlegal4everyone.mcp.publicdata.stream/mcp-wt/v1 \
  --origin https://openlegal4everyone.mcp.publicdata.stream \
  --ca-file /absolute/operator/edge-ca-bundle.pem \
  --wt-client "$PWD/target/debug/examples/wt_client" \
  --report /absolute/operator/new-smoke-report.json
```

Run only against an explicitly selected deployment. Supply an existing PEM
certificate bundle that trusts the **edge**; the client's edge trust does not
prove the edge's separate backend trust. Endpoints require verified HTTPS, exact
MCP paths and no credentials, query or fragment. The positive Origin requires
HTTPS and must be configured as allowed. Reserve `https://smoke-denied.invalid`
outside all allowlists. There is no proxy, redirect, ambient endpoint discovery,
runtime download or insecure TLS option. The Python driver requires Linux/POSIX,
Python 3.11 or later and a compatible prebuilt native client.

The client exercises both MCP revisions: legacy initialize/initialized and
modern discovery, tool listing, server/source information, canonical `text.diff`,
comparison and patch assertions, cleanup, HTTP SSE progress, and native
WebTransport. Invalid Host/Origin and public health-route checks require explicit
HTTP rejection responses. A connection failure never passes a denial check.
The native SDK exposes WebTransport session rejection without its HTTP status;
native evidence records `transport_session_rejected`, while HTTP independently
requires 403 for the invalid Origin.

Optional `--live-url http://PRIVATE_POD:9090/live` and
`--ready-url http://PRIVATE_POD:9090/ready` checks run only from an already
authorized private network. Do not introduce port forwarding or public routes to
make them pass. Optional Pod readiness requires **all** of `--kubectl`,
`--kubeconfig`, `--context`, `--namespace` and `--pod`. It requests only selected
Pod identity/readiness fields; a stable UID, Running phase, Ready condition and
absence of deletion are required. Neither health nor Pod readiness establishes
fresh database access, index catch-up, singleton exclusion or firewall enforcement.

Operations have ten-second absolute deadlines, including slow HTTP/SSE responses.
The transport phase is bounded to 180 seconds, optional Pod readiness to 360
seconds, and response bodies/frames to 16 MiB. Calls are sequential and not
automatically retried. Failures include cleanup failures; interrupted connections
can leave the run's transient objects to expire under the normal store TTL.

The command exits nonzero for a selected failed check. Its optional version-1 JSON
report uses `passed`, `failed` and `not_run`, fixed check/reason identifiers and
timings. It creates a new report file and refuses to overwrite one. Reports omit
URLs, response bodies, handles, credentials and raw subprocess diagnostics.
“All selected smoke checks passed” means exactly that, not complete acceptance.

### Acceptance checklist and evidence boundaries

Execute disruptive cases only against the prepared disposable fixture or within
a separately authorized operator maintenance window. The
[fixture guide](../test-support/kubernetes-acceptance/README.md) owns pinned
tool preparation and cluster cleanup; its [operations appendix](../test-support/kubernetes-acceptance/OPERATIONS.md)
provides manual disruptive checks and restoration commands. Build and load images before running the
single-node cluster; keep the whole dev workload within 7 GiB and run expensive
checks sequentially. The server retains its canonical 4 GiB container limit.
Use the fictional retained seed, full pinned dictionary and distinct migration
and runtime credentials from the existing image fixture. Never copy a production
database or enable ingestion for these checks.

| Gate | Required observation | Evidence limit |
| --- | --- | --- |
| Pod and private health | Running, non-terminating Ready Pod; `/live` and `/ready` return 200 from permitted path | Not index catch-up or a fresh DB query |
| TCP/UDP NodePorts | Actual edge container reaches HTTP and native WebTransport; unintended fixture clients are denied | Positive controls plus observed CNI/firewall counters; timeouts alone fail evidence requirements |
| Public MCP | Both revisions, both transports, source metadata, tiny diff and HTTP SSE progress pass | Native clients, not browser or ChatGPT acceptance |
| Host/Origin and health exclusion | Invalid HTTP Host 404, invalid Origin 403, public health paths 404; native invalid Origin is session-rejected | Keep known-good TLS/endpoint controls |
| Backend trust and identity | Fresh WT connection fails under unrelated backend CA and under trusted wrong-SAN certificate; HTTP/private-health controls remain good; restoring trust/identity recovers WT | Change only disposable edge trust/backend certificate; client-side CA failure is a different boundary |
| PostgreSQL credentials | Migration Job completes before serving; runtime role receives insufficient-privilege SQLSTATE for disposable DDL; serving has no migration credential reference or mount | Never print environment values, Secret data, SQL credentials or full workload dumps |
| Storage | Separate PV/mount paths, expected ownership, writable private data paths and read-only dictionary | Local directories inside kind are not production ZFS qualification or disk quotas |
| Graceful lifecycle | Stop an already-ready Deployment, observe clean server termination within grace, then start it and verify retained capture/search identity | Do not force-delete Pods; early initialization has a different shutdown boundary |
| Singleton | Observe `Recreate` transition and no overlapping serving processes; competing runtime lease is rejected using a separate disposable index | Replica count and volume access mode alone are not fencing |

For network denial evidence, establish a reachable positive control before each
negative and inspect the corresponding CNI/node firewall counters or denial
events. Place fixture firewall rules only in the disposable node network namespace;
do not change the host's firewall. Check public NodePorts from distinct allowed
and denied fixture containers, and health from permitted monitoring and unrelated
Pod identities. A nested single-node fixture does not establish cross-node
routing, physical-node interfaces, actual production source NAT, or the production
host's firewall.

For lifecycle evidence, record Pod UIDs and selected status fields, stop serving
with `scale --replicas=0`, wait for termination, then restore one replica and wait
for readiness. Compare the same known synthetic capture IDs and query results
before and after. A separate `rollout restart` checks the Recreate upgrade path.
Watch termination through completion so a fast replacement does not erase exit
evidence. Never start the competing-instance test against the live index: give it
a separate empty index path while preserving the same runtime-lease database.

The Phase 9 dev run observed HTTP failures with `channel closed` in the pinned
OxiBelt after backend replacement and again without another backend replacement.
The cause remains unresolved. Do not treat backend readiness as restored
public transport acceptance. Verify retained data through the trusted NodePort,
restart the disposable edge to discard stale upstream connections, and rerun
both public transports. Record the original failed public request as well as
recovery; this is an operator workaround, not a proxy fix or seamless-upgrade
claim. Reassess HTTP stability before accepting production traffic or backend upgrades.

Record every checklist row individually in a new dated operator acceptance record,
including exact commands, version/digest identities and failed or unexecuted cases.
Use the [Phase 9 evidence record](deployment-acceptance.md) as a historical example;
do not overwrite it with later results. Keep raw private run artifacts outside Git. Distinguish implemented, executed in
Docker, executed against PostgreSQL, executed on the disposable Kubernetes cluster,
and observed CI results. Document sandbox, production ZFS/firewall, live LAW OPEN
DATA, browser/ChatGPT and production traffic remain independently pending until
their own gates are performed. Do not infer CI success from a configured job.

## Validation boundary

[Contributor requirements](../CONTRIBUTING.md#testing-and-ci) own the required
checks for changes to images, deployment templates, validation tools and runtime
behavior. The [local and CI checks](#local-and-ci-checks) describe deterministic
rendering and offline schema coverage. These checks do not establish target-cluster
admission, enforcement, scheduling, resource sizing or production acceptance.

The [Phase 9 record](deployment-acceptance.md) reports separately executed disposable
serving-cluster observations and their unresolved public HTTP failure. It does not
qualify production or the document sandbox. Phase 10 reorganizes documentation and
operator sequencing; it adds no runtime, image, manifest, hosted CI, cluster or live
provider validation evidence. Publication, deployment and live provider calls remain
separate operator actions.

## Implementation history

Phase 0 records the selected deployment design and existing server contracts.
Phase 1 adds the production server image, local image acceptance and native
amd64/ARM64 CI jobs. Phase 2 established hardened text-only serving and offline
manifest checks. Phase 3 makes retained-corpus serving the default template, adds
operator-managed storage examples and extends image acceptance to retained data.
Phase 4 adds the TCP/UDP NodePort Service and a tested OxiBelt handoff example.
Phase 5 adds suspended migration, cache-maintenance and offline index-rebuild Jobs
with separate credentials and an operator-controlled maintenance window. Phase 6
adds namespace-wide default deny, separately selected allow policies and a network
acceptance runbook. The text-only profile remains a separate test fixture without
a Service or NetworkPolicy. Phase 7 adds an opt-in ingestion image, projected
controller identity, separate RBAC/egress templates and offline controller checks.
Phase 8 adds source-inventory checks and pinned offline Kubernetes 1.36.0/1.37.0
schema validation to the existing deployment and four native image CI jobs.
Phase 9 adds a bounded explicit-endpoint smoke client and separately provisioned
disposable Kubernetes serving acceptance. See the [checklist](#phase-9-serving-acceptance)
and its [execution record](deployment-acceptance.md) for scoped disposable-cluster
outcomes and unresolved HTTP stability. Complete production acceptance remains
pending; no production deployment, live-provider, browser WebTransport or ChatGPT
acceptance is established.

The original Phase 0 inventory was recorded on `main` at
`79a8a852916d6fb18f306e5d7541115e1bab87d8`. The earlier design baseline,
`f4bf5899caa1cc04491b68ddeb7c516b58f93387`, differs only by the commit ignoring
`.agents/temp/`; those two revisions have identical runtime code. Phase 3 builds
on the clean Phase 2 revision `f18751caadf2eec490841980a0ee94ba2d9cbaac`.
The configured `GitHub` remote is
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
unchanged. Phase 2 also rejects empty certificate chains and mismatched TLS keys
as normal startup errors instead of panicking in the TLS dependency. Valid TLS
configuration retains ring, TLS 1.3, WebTransport ALPN and existing QUIC limits.
Phase 3 preserves Rust configuration types and MCP schemas; its default template
now requires migrated PostgreSQL, prepared storage and a provisioned dictionary.
Phase 5 preserves production Rust behavior and the configuration contents; serving
and administration import the same content-hashed ConfigMap from the
[shared configuration base](../deploy/kubernetes/config/).
