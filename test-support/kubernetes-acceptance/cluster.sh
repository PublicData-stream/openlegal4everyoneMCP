#!/usr/bin/env bash
# Create/delete only the named disposable fixture. Serving acceptance is separate.
set -euo pipefail
repo=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
if [[ $# != 3 || ! $1 =~ ^(create|delete)$ ]]; then
    echo "usage: $0 create|delete TOOLS_DIR RUN_DIR" >&2; exit 2
fi
operation=$1
tools_dir=$(realpath -e -- "$2")
run_dir=$(realpath -m -- "$3")
[[ $run_dir != / && $run_dir != "$repo" && ! -L $3 ]] || exit 2
kind=$tools_dir/bin/kind
kubectl=$tools_dir/bin/kubectl
lock=$repo/test-support/kubernetes-acceptance/assets.lock.json
for command in docker python3 timeout ip; do command -v "$command" >/dev/null; done
[[ -x $kind && -x $kubectl ]] || { echo 'Provision deployment and acceptance tools first.' >&2; exit 1; }
# Do not trust arbitrary programs left in a tools directory after provisioning.
case "$(uname -m)" in x86_64) arch=amd64 ;; aarch64|arm64) arch=arm64 ;; *) exit 1 ;; esac
mapfile -t pins < <(python3 - "$lock" "$arch" <<'PY'
import json, sys
lock = json.load(open(sys.argv[1]))
print(lock['kind_sha256'][sys.argv[2]])
print(lock['node_image'])
print(*lock['calico_images'].values(), sep='\n')
PY
)
(( ${#pins[@]} == 5 )) || exit 1
printf '%s  %s\n' "${pins[0]}" "$kind" | sha256sum --check --status
kubectl_checksum=$(awk -v arch="$arch" '$2 == arch {print $1}' "$repo/scripts/deployment-kubectl.sha256")
printf '%s  %s\n' "$kubectl_checksum" "$kubectl" | sha256sum --check --status
if [[ $operation == delete ]]; then
    [[ -f $run_dir/owner && ! -L $run_dir/owner ]] || { echo 'Missing run ownership record.' >&2; exit 1; }
    read -r name < "$run_dir/owner"
    [[ $name =~ ^openlegal-accept-[0-9a-f]{16}$ ]] || exit 1
    # Require the exact run path recorded in the network label before deleting.
    network_present=false
    if docker network inspect "$name" > /dev/null 2>&1; then
        network_present=true
        network_owner=$(docker network inspect "$name" --format '{{index .Labels "openlegal.acceptance.run"}}')
        [[ $network_owner == "$run_dir" ]] || { echo 'Network ownership mismatch.' >&2; exit 1; }
    fi
    if docker container inspect "$name-control-plane" >/dev/null 2>&1; then
        node_owner=$(docker inspect "$name-control-plane" --format '{{index .Config.Labels "io.x-k8s.kind.cluster"}}')
        [[ $node_owner == "$name" ]] || exit 1
    fi
    KIND_EXPERIMENTAL_DOCKER_NETWORK=$name "$kind" delete cluster --name "$name" --kubeconfig "$run_dir/kubeconfig"
    if $network_present; then docker network rm "$name" >/dev/null; fi
    # Leave private evidence/artifacts for deliberate operator disposal.
    mv "$run_dir/owner" "$run_dir/deleted-owner"
    echo 'Owned cluster/network deleted; run directory retained.'
    exit 0
fi
[[ ! -e $run_dir ]] || { echo 'RUN_DIR must not exist; each run gets a fresh directory.' >&2; exit 1; }
for image in "${pins[@]:1}"; do
    docker image inspect "$image" >/dev/null 2>&1 || { echo 'A locked image is missing; preload before creation.' >&2; exit 1; }
done
python3 "$repo/test-support/kubernetes-acceptance/assets.py" \
    "$tools_dir/acceptance/calico.upstream.yaml" /dev/null
# Ignore only default routes: any specific host/Docker route must not overlap
# the fixed pod/service CIDRs. A failed inventory is a failed prerequisite.
route_inventory=$(ip -j -4 route show table all)
network_inventory=$(docker network ls -q | xargs -r docker network inspect)
python3 "$repo/test-support/kubernetes-acceptance/networks.py" "$route_inventory" "${network_inventory:-[]}"
umask 077
mkdir -p -- "$run_dir"
name=openlegal-accept-$(python3 -c 'import secrets; print(secrets.token_hex(8))')
printf '%s\n' "$name" > "$run_dir/owner"
# Recreate the manifest from verified source, ignoring any edited rendered copy.
python3 "$repo/test-support/kubernetes-acceptance/assets.py" \
    "$tools_dir/acceptance/calico.upstream.yaml" "$run_dir/calico.yaml"
cat > "$run_dir/kind.yaml" <<YAML
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
networking:
  apiServerAddress: "127.0.0.1"
  disableDefaultCNI: true
  podSubnet: "10.244.0.0/16"
  serviceSubnet: "10.96.0.0/16"
nodes:
  - role: control-plane
    labels:
      openlegal.server/ready: "true"
YAML
# kind's node entrypoint requires a default gateway for Docker DNS rewriting.
# A dedicated bridge supplies it; no application port is published on the host.
# This bridge permits outbound traffic. Serving isolation is enforced separately
# by the committed CNI policies and the fixture's node firewall checks.
docker network create --label "openlegal.acceptance.run=$run_dir" "$name" >/dev/null
python3 "$repo/test-support/kubernetes-acceptance/networks.py" \
    "$(ip -j -4 route show)" "$(docker network inspect "$name")"
printf 'Creating owned fixture %s; failure leaves explicit cleanup state.\n' "$name"
KIND_EXPERIMENTAL_DOCKER_NETWORK=$name timeout 360 "$kind" create cluster \
    --name "$name" --image "${pins[1]}" --config "$run_dir/kind.yaml" \
    --kubeconfig "$run_dir/kubeconfig" --retain --wait 0s
# Bootstrap is bounded by the enclosing dev VM (7 GiB physical RAM); kind has
# no supported per-node memory option. This cap applies after node creation.
# Record VM memory/peak separately; this is not an aggregate cgroup guarantee.
docker update --memory 6g --memory-swap 6g "$name-control-plane" >/dev/null
for image in "${pins[@]:2}"; do
    # docker save cannot portably export a repo@digest reference. Give the
    # already inspected immutable source a run-owned tag, load it, and register
    # its exact admitted digest reference in the node's containerd image store.
    component=${image##*/}
    component=${component%%:*}
    local_tag="$name/$component:verified"
    docker tag "$image" "$local_tag"
    # Docker's containerd image store may retain a multiarchitecture index with
    # only native blobs. kind's --all-platforms import then requests absent blobs.
    if ! timeout 120 docker image save "$local_tag" \
        | timeout 120 docker exec -i "$name-control-plane" ctr --namespace k8s.io \
            images import --platform "linux/$arch" --digests - >/dev/null; then
        docker image rm "$local_tag" >/dev/null
        exit 1
    fi
    docker image rm "$local_tag" >/dev/null
    # CRI canonicalizes tag@digest to repository@digest. Preserve the original
    # multiarch index in the archive while importing only native blobs.
    digest_ref="${image%%:*}@${image##*@}"
    docker exec "$name-control-plane" ctr --namespace k8s.io images tag \
        "docker.io/$local_tag" "$digest_ref" >/dev/null
    docker exec "$name-control-plane" crictl inspecti "$image" >/dev/null
done
# All directories live in this run's disposable node; no production host bind.
docker exec "$name-control-plane" sh -ec '
    for part in cache-blobs corpus-blobs corpus-index competing-index mecab-ko-dictionary; do
        install -d -o 0 -g 10004 -m 2770 "/var/local/openlegal/$part"
        install -d -o 10004 -g 10004 -m 0700 "/var/local/openlegal/$part/data"
    done
'
"$kubectl" --kubeconfig "$run_dir/kubeconfig" --context "kind-$name" \
    --request-timeout=60s apply --server-side -f "$run_dir/calico.yaml"
"$kubectl" --kubeconfig "$run_dir/kubeconfig" --context "kind-$name" \
    --request-timeout=370s wait --for=condition=Ready "node/$name-control-plane" --timeout=360s
"$kubectl" --kubeconfig "$run_dir/kubeconfig" --context "kind-$name" -n kube-system \
    --request-timeout=190s rollout status daemonset/calico-node --timeout=180s
printf 'Disposable CNI-enabled cluster prepared; serving acceptance has not run. Run state: %s\n' "$run_dir"
