# Disposable serving acceptance fixture

This fixture prepares one real Kubernetes node with an enforcing Calico CNI.
It is a development fixture within the 7 GiB `dev` host budget. It does not
qualify production storage, cross-node routing, document-worker isolation or live
legal providers. The serving checklist belongs to
[deployment acceptance](../../docs/deployment-kubernetes.md).

`cluster.sh` creates or deletes only its owned cluster/network. It does not build
images, apply serving workloads, generate production credentials, alter the host
firewall, or run the disruptive acceptance checks. Use a dedicated `dev` session;
run builds and acceptance sequentially. The Docker daemon must support kind's
privileged node container, cgroup delegation and the native host architecture.
Use rootless Docker where supported by the configured host; lack of those kernel
features is a failed prerequisite, not permission to disable isolation.

## Dependency admission

`assets.lock.json` records kind 0.33.0 binary checksums, the release-provided
Kubernetes 1.36.4 node digest, Calico 3.32.2 manifest SHA-256 and registry-resolved
multiarchitecture image digests. Admission was checked on 2026-09-22 using the
[kind release API](https://api.github.com/repos/kubernetes-sigs/kind/releases/tags/v0.33.0),
[Calico manifest](https://raw.githubusercontent.com/projectcalico/calico/v3.32.2/manifests/calico.yaml)
and `docker buildx imagetools inspect quay.io/calico/{cni,node,kube-controllers}:v3.32.2`.
Kubernetes 1.36 is in the
[Calico tested versions](https://docs.tigera.io/calico/latest/getting-started/kubernetes/requirements).
The existing pinned kubectl 1.37 client is within one minor of this API server.

kind and Calico are maintained upstream Apache-2.0 projects. These are explicit
development dependencies, not application dependencies. kind gives reproducible
node images without permanent VM installation; Calico supplies NetworkPolicy
enforcement missing from kind's default CNI. Both introduce privileged node/CNI
code, so this fixture belongs only on the disposable development environment.
The upstream manifest is downloaded outside tracked state, retains its upstream
content, and is verified before the image fields and pull policies are changed.
No upstream source or binary is vendored here. Preserve upstream image notices
and [kind license](https://github.com/kubernetes-sigs/kind/blob/v0.33.0/LICENSE) and
[Calico license](https://github.com/projectcalico/calico/blob/v3.32.2/LICENSE.md)
when redistributing the downloaded artifacts. The immutable node image also pins
its embedded control-plane, DNS and storage-provisioner dependencies; inspect
its images if changing the node release.

## Explicit preparation

Run these from the repository root on the Docker host. Set `accept_tools` and
`accept_run` to private development locations; `accept_run` must not yet exist.
The explicit provisioning step needs outbound artifact access. Cluster creation
needs only these preloaded images. Its dedicated private Docker bridge permits
outbound connections: kind requires a default gateway for its DNS setup. No
application ports are published; committed CNI policies and node-local firewall
tests establish the serving boundaries separately.

```sh
accept_tools="$PWD/target/deployment-tools"
accept_run="$PWD/target/serving-acceptance-run"
scripts/setup-deployment-tools.sh "$accept_tools"
scripts/setup-serving-acceptance-tools.sh "$accept_tools"
accept_images=$(mktemp)
trap 'rm -f -- "$accept_images"' EXIT
python3 - <<'PY' > "$accept_images"
import json
from pathlib import Path
lock = json.loads(Path('test-support/kubernetes-acceptance/assets.lock.json').read_text())
print(lock['node_image'])
print(*lock['calico_images'].values(), sep='\n')
PY
while IFS= read -r image; do docker pull "$image"; done < "$accept_images"
test-support/kubernetes-acceptance/cluster.sh create "$accept_tools" "$accept_run"
read -r accept_name < "$accept_run/owner"
accept_kubectl=("$accept_tools/bin/kubectl" --kubeconfig "$accept_run/kubeconfig" --context "kind-$accept_name")
python3 test-support/kubernetes-acceptance/prepare-storage.py \
    --node "$accept_name-control-plane" --output "$accept_run/storage.yaml"
```

The fixed pod CIDR is `10.244.0.0/16` and service CIDR is `10.96.0.0/16`;
Calico's initial IPv4 pool is set explicitly to the pod CIDR. Creation rejects
specific host routes and existing Docker subnets overlapping either range.
Install `iproute2` for this prerequisite; default routes are excluded from the
conflict test. Choose another isolated host if the fixed ranges conflict.
Calico images are loaded using temporary run-owned Docker tags, then assigned
the admitted digest reference inside node containerd; the fixture verifies CRI
can resolve each reference before applying the `Never` pull-policy manifest.

The API binds only to loopback. No application NodePort is published on the host.
Edge and test clients join the named private Docker network explicitly. After
creation the node has a 6 GiB cgroup ceiling; bootstrap relies on the enclosing
dev VM's 7 GiB physical RAM limit. This script does not enforce an aggregate
cgroup ceiling. Record VM memory and peak usage, bound external edge/client
containers, run sequentially, and do not raise the approved host budget to force
a passing result.
Serving retains its canonical 4 GiB limit. The Local PV directories are separate
paths inside the owned node and are deleted with it. Their 4 GiB PV capacities
are scheduler metadata, not storage quotas or evidence of ZFS separation.

## Workload inputs and storage

Build the production `runtime` image and the fictional retained-data helper from
`test-support/retained-image/Dockerfile` before starting acceptance. Record the
exact source revision and immutable image references. The preparer requires
`repository@sha256:...` references for both; a local tag or Docker image ID alone
is insufficient. PostgreSQL uses the existing pinned PostgreSQL 18 digest from
`scripts/test-retained-server-image.sh`. No images are published by this fixture.

Preload every workload image into the owned node for its native `linux/amd64`
or `linux/arm64` platform. Preserve the admitted digest reference when importing
into containerd's `k8s.io` namespace and verify it with `crictl inspecti` before
applying workloads. The native-platform import procedure in `cluster.sh` avoids
requesting missing foreign-platform blobs from a locally partial multiarchitecture
index. If a temporary run-owned tag is needed during transfer, restore the
immutable reference inside containerd and remove only that temporary Docker tag.
All prepared workloads use `imagePullPolicy: Never`; missing images fail startup.
Do not switch to mutable tags or runtime registry pulls to bypass this check.

The full dictionary must be admitted by `scripts/prepare-korean-dictionary.sh`
or the existing complete dictionary gate before deployment. Set
`accept_dictionary` to that provisioned artifact directory; a manifest's presence
alone does not establish admission. Copy it only into the owned node:

```sh
: "${accept_dictionary:?Set the admitted full dictionary directory}"
tar -C "$accept_dictionary" -cf - . | docker exec -i "$accept_name-control-plane" \
    tar -xf - -C /var/local/openlegal/mecab-ko-dictionary/data
docker exec "$accept_name-control-plane" sh -ec '
    chown -R 0:10004 /var/local/openlegal/mecab-ko-dictionary/data
    find /var/local/openlegal/mecab-ko-dictionary/data -type d -exec chmod 550 {} +
    find /var/local/openlegal/mecab-ko-dictionary/data -type f -exec chmod 440 {} +
'
```

`cluster.sh` prepares the volume roots as root:10004 2770 and private writable
`data` directories as 10004:10004 with permission bits 0700 (the setgid bit may
be inherited from the volume root; the dev run observed 2700). Preserve those root permissions when
copying the dictionary; serving mounts it read-only. The separate
`competing-index/data` directory is for the competing-lease check; never point
the competing process at the live index. These directories reside inside the
disposable node and are removed with that node.

## Render and apply serving

Reserve the run-owned edge container's private network address before rendering.
Set `accept_edge_ip` and `accept_node_ip` to the observed fixture IPv4 addresses;
set `accept_source_revision` to the 40-character source revision recorded before
source transfer. The archive need not contain `.git`. Run the preparer with the
hash-locked deployment Python and provisioned kubectl. `accept_workloads` must
not exist; preparation creates it mode 0700, with mode-0600 files.

```sh
: "${accept_server_image:?Set the admitted production runtime image digest reference}"
: "${accept_seed_image:?Set the admitted fictional seed image digest reference}"
: "${accept_edge_ip:?Set the reserved private edge IPv4 address}"
: "${accept_node_ip:?Set the owned node private IPv4 address}"
: "${accept_source_revision:?Set the exact 40-character source revision}"
accept_workloads="$accept_run/workloads"
"$accept_tools/bin/python" test-support/kubernetes-acceptance/prepare-workloads.py \
    --output "$accept_workloads" --kubectl "$accept_tools/bin/kubectl" \
    --node "$accept_name-control-plane" --source-revision "$accept_source_revision" \
    --server-image "$accept_server_image" --seed-image "$accept_seed_image" \
    --postgres-image postgres@sha256:ae6c78831cbc35fa3a4aaf4d763ddacf6183d6004774cc2dc28b3920410d1d1a \
    --edge-ip "$accept_edge_ip" --node-ip "$accept_node_ip"
```

The helper performs no cluster operations. It renders committed serving,
migration and network templates, generates disposable database roles and TLS
material, and keeps ingestion disabled. Certificates expire after two days.
Its trusted wrong-hostname certificate and unrelated CA are solely for explicit
negative tests; restore valid material and repeat positive controls afterwards.
Secrets, generated SQL, private keys and workload files remain private run inputs.

Apply each stage explicitly and stop at the first failed command or timeout.
Never apply the directory recursively or run these stages concurrently. The
fresh fixture has no serving process during migration and publication:

```sh
"${accept_kubectl[@]}" apply -f "$accept_workloads/00-namespaces.yaml"
"${accept_kubectl[@]}" apply -f "$accept_run/storage.yaml"
"${accept_kubectl[@]}" apply -f "$accept_workloads/10-serving-inputs.yaml"
"${accept_kubectl[@]}" apply -f "$accept_workloads/15-network.yaml"
"${accept_kubectl[@]}" apply -f "$accept_workloads/20-postgres.yaml"
"${accept_kubectl[@]}" -n openlegal-accept-postgres wait deployment/postgres \
    --for=condition=Available --timeout=120s
"${accept_kubectl[@]}" apply -f "$accept_workloads/30-bootstrap.yaml"
"${accept_kubectl[@]}" -n openlegal-accept-postgres wait job/postgres-bootstrap \
    --for=condition=Complete --timeout=100s
"${accept_kubectl[@]}" apply -f "$accept_workloads/40-migrate.yaml"
"${accept_kubectl[@]}" -n openlegal-serving wait job/openlegal-migrate \
    --for=condition=Complete --timeout=610s
"${accept_kubectl[@]}" apply -f "$accept_workloads/50-grants.yaml"
"${accept_kubectl[@]}" -n openlegal-accept-postgres wait job/postgres-grants \
    --for=condition=Complete --timeout=100s
"${accept_kubectl[@]}" apply -f "$accept_workloads/60-seed.yaml"
"${accept_kubectl[@]}" -n openlegal-serving wait job/openlegal-seed \
    --for=condition=Complete --timeout=190s
"${accept_kubectl[@]}" apply -f "$accept_workloads/70-serving.yaml"
"${accept_kubectl[@]}" -n openlegal-serving scale deployment/openlegal-server --replicas=1
"${accept_kubectl[@]}" -n openlegal-serving wait deployment/openlegal-server \
    --for=condition=Available --timeout=360s
```

The migration Job receives only the migration URL; serving and the seed receive
only the runtime URL. PostgreSQL bootstrap/grants Jobs run in their own namespace
with separate credentials. Keep any diagnostic Job output private. PostgreSQL
uses disposable `emptyDir` storage: restarting serving can test retained capture
identity, but this fixture does not test database Pod replacement durability.
`70-serving.yaml` deliberately declares zero replicas, so reapplying it stops
serving; scaling to one is an explicit operator step after prerequisites pass.

The monitor and denied-client namespaces are created, but client Pods and their
explicit probes remain operator steps. The network render permits the selected
edge address, serving/admin database traffic, selected cluster DNS and a monitor
label in its separate namespace. Confirm the observed CNI enforcement addresses
and node-local firewall behavior before interpreting connectivity results.

## Edge inputs and acceptance

OxiBelt resolves certificate filenames through a `cert/` directory beside its
`config/` directory. Stage only `oxibelt.toml` in `config/`, and only `edge.pem`,
`edge-key.pem` and `backend-ca.pem` in `cert/`. A flat `/fixture/oxibelt.toml`
layout resolves the certificate directory incorrectly. Set `accept_edge_image`
to the tested OxiBelt harness image, then create a separate run-owned volume:

```sh
: "${accept_edge_image:?Set the tested OxiBelt harness image}"
umask 077
accept_edge_root="$accept_run/edge-inputs"
accept_edge_volume="$accept_name-edge-inputs"
mkdir -m 700 "$accept_edge_root" "$accept_edge_root/config" "$accept_edge_root/cert"
cp "$accept_workloads/oxibelt.toml" "$accept_edge_root/config/oxibelt.toml"
cp "$accept_workloads/edge.pem" "$accept_workloads/edge-key.pem" \
    "$accept_workloads/backend-ca.pem" "$accept_edge_root/cert/"
docker volume create --label "openlegal.acceptance.run=$accept_run" "$accept_edge_volume"
tar -C "$accept_edge_root" -cf - . | docker run --rm -i --network none --user 0:0 \
    --mount "type=volume,source=$accept_edge_volume,target=/fixture" \
    --entrypoint sh "$accept_edge_image" -ec '
        tar -xf - -C /fixture
        chown -R 65532:65532 /fixture
        find /fixture -type d -exec chmod 500 {} +
        find /fixture -type f -exec chmod 400 {} +
    '
docker run --rm --network none --user 65532:65532 --read-only --cap-drop ALL \
    --security-opt no-new-privileges --memory 256m --cpus 1 --pids-limit 64 \
    --mount "type=volume,source=$accept_edge_volume,target=/fixture,readonly" \
    --entrypoint /usr/local/bin/oxibelt "$accept_edge_image" \
    --config /fixture/config/oxibelt.toml --check
```

The initializer changes ownership only inside the disposable volume. Keep its
directories mode 0500 and files mode 0400, owned by harness UID/GID 65532. Mount
the volume read-only at `/fixture` when launching the edge, with entrypoint
`/usr/local/bin/oxibelt` and arguments `--config /fixture/config/oxibelt.toml`.
Never mount or copy the entire workloads directory: it also contains the CA
signing key, PostgreSQL credentials and private test material. Keep the curated
staging directory and volume private because the edge key remains sensitive.
Remove only this run's edge volume after stopping its edge/client containers.

Run the external edge on the reserved private IP with explicit memory/CPU limits
and no published host ports. Client containers can use
`--add-host "openlegal4everyone.stream:$accept_edge_ip"` and the public fixture
URLs `https://openlegal4everyone.stream:8443/mcp` and
`https://openlegal4everyone.stream:8443/mcp-wt/v1`. Supply only the CA certificate
to clients; retain certificate-chain and hostname verification. The allowed
Origin is `https://openlegal4everyone.stream`. Backend authorities are the node
IP with ports 30080 and 30433, and the backend certificate contains that IP SAN.

Follow the [Phase 9 serving checklist](../../docs/deployment-kubernetes.md#phase-9-serving-acceptance)
for transport assertions, runtime-role DDL rejection, private-health isolation,
TLS negatives and recovery, storage permission checks, SIGTERM/restart,
`Recreate` non-overlap and the separate-index competing lease. A Ready node,
successful Job or available Deployment alone does not pass those checks.
The [operations appendix](OPERATIONS.md) supplies exact manual probe and disruptive
check commands using the private inputs prepared here.

## Evidence and cleanup

Record selected readiness/resource states and sanitized client outcomes. Keep the
private kubeconfig, generated Secrets, SQL and raw logs under the mode-0700 run
directory. Do not commit them. Evidence must distinguish a Ready node, successful
policy installation, observed traffic enforcement and complete serving checks.
Network timeout alone is insufficient evidence of denial.

Stop and remove run-owned edge/client containers before cleanup, then:

```sh
test-support/kubernetes-acceptance/cluster.sh delete "$accept_tools" "$accept_run"
```

Deletion validates the random cluster name, any remaining node label, and any
remaining network ownership label and
retains the private run directory for deliberate operator disposal. A failed
create leaves the ownership record for the same cleanup command; inspect any
failure before retrying with a fresh run directory. Cleanup also accepts an
absent network after a partially failed create or prior deletion attempt. Never use global Docker or
Kubernetes cleanup commands. Do not remove an unrelated context or network.

Offline checks:

```sh
shellcheck scripts/setup-serving-acceptance-tools.sh test-support/kubernetes-acceptance/cluster.sh
PYTHONDONTWRITEBYTECODE=1 target/deployment-tools/bin/python -m unittest discover -s test-support/kubernetes-acceptance -p 'test_*.py'
```
