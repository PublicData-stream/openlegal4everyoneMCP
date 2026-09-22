# Kubernetes deployment design

## Status and baseline

Phase 0 records the selected deployment design and existing server contracts.
Phase 1 adds the production server image, local image acceptance and native
amd64/ARM64 CI jobs. Phase 2 established hardened text-only serving and offline
manifest checks. Phase 3 makes retained-corpus serving the default template, adds
operator-managed storage examples and extends image acceptance to retained data.
Phase 4 adds the TCP/UDP NodePort Service and a tested OxiBelt handoff example.
Phase 5 adds suspended migration, cache-maintenance and offline index-rebuild Jobs
with separate credentials and an operator-controlled maintenance window. The
text-only profile remains a separate test fixture without a Service. NetworkPolicy,
ingestion integration and complete production acceptance remain planned. This document does not establish a running deployment
or successful real-cluster, live-provider, browser WebTransport or ChatGPT acceptance.

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

The CI image jobs use native `ubuntu-24.04` and `ubuntu-24.04-arm` runners, with no
publication credentials. Label local ARM emulation and native CI evidence separately.
Docker acceptance does not establish kubelet ownership behavior, real-cluster
isolation, live provider or public transport acceptance. Existing PostgreSQL,
Korean analyzer, OxiBelt and document-worker gates remain separate. Record actual
check outcomes with the implementation handoff.

## Retained-corpus serving template

The [serving Kustomization](../deploy/kubernetes/serving/) generates a Namespace,
Deployment, content-hashed ConfigMap and NodePort Service. It enables supplied-text
comparison and retained-corpus serving with persistent PostgreSQL, two blob stores and a corpus
index. Ingestion is omitted. There is no provider credential, controller identity
or document-worker access. Retained serving still writes index events and performs
retention maintenance. The former text-only configuration is now a
[test fixture](../test-support/deployment/text-only/), selected explicitly in checks.

### Operator prerequisites

- Publish the matching server image separately. Replace the entire
  `registry.example/openlegal-server@sha256:000...000` reference with its real
  registry and immutable SHA-256 digest. The all-zero digest is an intentionally
  non-pullable sentinel; a validated reference does not prove an image exists.
- Replace `[source].url`'s release revision with free corresponding source for the
  exact running server and widgets, including modifications and build material.
- Replace both backend authority placeholders with the authorities sent by
  OxiBelt: `NODE_DNS:30080` for HTTP and `NODE_DNS:30433` for WebTransport,
  where `NODE_DNS` is the selected private node hostname. The intended browser
  Origin is
  `https://openlegal4everyone.stream`; retain explicit Origin validation. Follow
  the [OxiBelt hosting contract](oxibelt.md#adapt-the-configuration-for-hosting).
- Establish and verify the [NodePort firewall restrictions](#service-and-private-network-handoff)
  before applying the serving Kustomization, which creates the Service.
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
prepared storage first, establish the NodePort firewall restrictions below, and
require migration success before starting the Deployment. On first installation,
create the namespace from `serving/namespace.yaml` separately; applying the serving
Kustomization creates a one-replica Deployment immediately. Follow the
[administration sequence](#administrative-jobs) before the example below. On an
upgrade, keep the operator serving overlay at zero replicas until administration
has succeeded. For example, after all placeholders and prerequisites have been
resolved and the selected administrative Jobs have completed:

```sh
kubectl --kubeconfig /absolute/operator/kubeconfig --context OPERATOR_CONTEXT \
  apply -f /absolute/operator/storage/storage-class.example.yaml
kubectl --kubeconfig /absolute/operator/kubeconfig --context OPERATOR_CONTEXT \
  apply -f /absolute/operator/storage/local-pv.example.yaml
kubectl --kubeconfig /absolute/operator/kubeconfig --context OPERATOR_CONTEXT \
  apply -f /absolute/operator/storage/local-pvc.example.yaml
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
Ingress or NetworkPolicy in this phase; cluster peers may still reach Pod IPs,
including health. NetworkPolicy and its real enforcement remain Phase 6 gates.
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
on a real cluster. This handoff adds no intermediary proxy or serving NetworkPolicy.

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

The manifest gate defaults to `--profile retained`; `--config-output PATH` still
exports rendered TOML. Provisioning requires Python 3.11–3.14 with venv/ensurepip,
curl and SHA-256 utilities. It downloads pinned verification tools explicitly;
validation is offline with no kubeconfig, discovery or client dry-run.
`OPENLEGAL_DEPLOY_TOOLS` selects the provisioned directory (default
`target/deployment-tools`). See [tool provenance](dependencies.md#deployment-validation-tools).

The fast gate validates rendered YAML/TOML relationships, profile-specific security,
lifecycle and storage invariants, and rejection of unsafe mutations. It validates
Local PV/PVC examples without applying them, requires four objects for retained
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
These are repository checks, not API schema admission or enforcement proof.

The image gate consumes rendered configurations with the same runtime paths,
using disposable values for authorities, corresponding source and certificates.
It exercises both HTTP MCP revisions, Host/Origin denial, packaged widgets and
text workers, then retained search/restart and startup failures described above.
Fixtures use named Docker volume subdirectories; no host ports are published.
Both native CI image jobs provision the required tools and full dictionary.

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
copy preserving the `config`, `serving`, `admin` and `storage` directory relationships.
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

1. On first installation, apply only the standalone namespace manifest, provision
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
4. Resume only after prerequisites hold. Require `Complete=True`, successful process
   exit and actual termination before continuing. Failure or an ambiguous result
   keeps serving stopped. Inspect bounded operational diagnostics; keep credentials,
   SQL payloads and raw evidence out of shared logs or tickets.
5. After successful administration, apply the verified serving configuration/image
   and, if rebuilt, replacement index claim **while replicas remain zero**. Then
   set the operator overlay back to one replica, apply it, check rollout/readiness
   and perform bounded retained-search/capture checks. Restore exactly one backend.

The following examples are operator actions, not a repository deployment script.
Substitute all paths and context names first. The first command is for a new
installation; the shutdown commands require an existing Deployment.

```bash
kube=(kubectl --kubeconfig /absolute/operator/kubeconfig --context OPERATOR_CONTEXT)
"${kube[@]}" apply -f /absolute/operator/serving/namespace.yaml
# Existing deployment only; also keep the operator overlay at replicas: 0.
"${kube[@]}" -n openlegal-serving scale deployment/openlegal-server --replicas=0
"${kube[@]}" -n openlegal-serving wait --for=delete pod \
  -l app.kubernetes.io/name=openlegal-server --timeout=360s
"${kube[@]}" -n openlegal-serving get pods,jobs
# Confirm all relevant processes stopped, then render and inspect this file.
kubectl kustomize /absolute/operator/admin/migrate > /absolute/operator/migrate-rendered.yaml
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

A changed image, ConfigMap reference or claim requires a **new Job**, because Job
Pod templates are immutable. An operator overlay may patch only the Job
`metadata.name` to a unique attempt name; use that rendered name in commands.
Avoid a root-wide `nameSuffix`, which would also rename the shared ConfigMap. Retain old Job/Pod diagnostics
until investigated, then explicitly clean up terminated attempts and unreferenced
ConfigMaps. There is no TTL controller configuration or automatic volume deletion.

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

## Template and operator ownership

The following division covers the full deployment design. Retained-corpus serving
and separately applied storage examples exist; later-phase production integrations
remain planned.

| Repository templates and contracts | Operator-supplied deployment values and actions |
| --- | --- |
| Image build, immutable image references, widget locations and source-offer field | Published image digests and a public corresponding-source URL for the exact running server/widget |
| Serving namespace, one replica, Recreate, security settings and probe definitions | Target cluster/runtime, measured resource sizing and real-cluster acceptance |
| Fixed TCP 30080 / UDP 30433 NodePorts and OxiBelt handoff example | NodePort availability, private node DNS, backend authorities, allowed origins, firewall rules and host Compose/certificate configuration |
| Separate storage mounts and generic Local PV/PVC examples | ZFS datasets, host paths, node affinity, capacity, ownership/permissions and provisioned dictionary |
| Secret references and separate serving/migration commands | PostgreSQL endpoint, roles/grants, credentials, CA material and backend TLS certificate/key |
| Explicit opt-in ingestion overlay and namespace-scoped controller access | Provider credential, digest-pinned worker image and separately configured controller identity after sandbox acceptance |

Commit no credentials, private keys, production kubeconfig, database URL, real
node identifier or host-specific ZFS path. Private network/storage values and
production resource sizing remain operator inputs; later ingestion authentication integration remains future-phase work.
The current controller requires explicit kubeconfig and context; any in-cluster
authentication alternative needs a deliberate contract change and Security Review.

## Validation boundary

Phase 0 acceptance consists of source/document inspection, Markdown/link checks
and independent review of the documentation patch under
[CONTRIBUTING.md](../CONTRIBUTING.md#documentation-only-changes). It supplies no
new runtime, image, manifest or CI validation evidence.

Image acceptance began in Phase 1, rendering/invariant checks in Phase 2, and
retained configuration/storage/restart checks in Phase 3. Phase 4 adds offline
Service/example consistency checks and a Docker OxiBelt handoff profile. Phase 5
adds offline admin-manifest checks and disposable database/image administration
scenarios. These do not validate Job admission, scheduling or termination on a
real cluster. Real-cluster networking, storage, shutdown and sandbox enforcement require operator acceptance;
live LAW OPEN DATA access and public transport/platform acceptance are separate
gates. Existing offline fixtures and local integration evidence do not satisfy
those gates. Deployment, publication and live provider requests are not part of
Phase 0.
