#!/usr/bin/env bash
# Exercise packaged kubectl against a synthetic TLS API on an internal Docker network.
# No cluster/provider access, host ports, or daemon-visible host binds are used.
set -euo pipefail
repo=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
if [[ $# != 4 || $1 != --image || $3 != --platform ]]; then
    echo "Usage: $0 --image IMAGE --platform linux/amd64|linux/arm64" >&2; exit 2
fi
image=$2
platform=$4
case $platform in linux/amd64|linux/arm64) ;; *) exit 2 ;; esac
for command in docker openssl tar python3; do
    command -v "$command" >/dev/null || { echo "Missing command: $command" >&2; exit 1; }
done
case $(docker info --format '{{.Architecture}}') in
    x86_64|amd64) native_platform=linux/amd64 ;;
    aarch64|arm64) native_platform=linux/arm64 ;;
    *) echo 'Unsupported host architecture' >&2; exit 1 ;;
esac
scratch=$(mktemp -d)
run_id="openlegal-ingestion-$(basename "$scratch" | tr '[:upper:]' '[:lower:]')-$$"
network=$run_id
fixture=$run_id-fixture
api=$run_id-api
client=$run_id-client
initializer=$run_id-initialize
node_image=node:24.21.0-trixie-slim@sha256:8ec5d7557396cfe32d21c3f9c13072355ceab22b584578ca4bb28af31120cffe
cleanup() {
    status=$?
    if (( status != 0 )); then
        docker logs "$api" >&2 2>/dev/null || true
        docker exec "$api" cat /tmp/events.jsonl >&2 2>/dev/null || true
        docker inspect --format '{{.Name}} exit={{.State.ExitCode}} oom={{.State.OOMKilled}}' "$api" "$client" >&2 2>/dev/null || true
    fi
    docker rm -fv "$api" "$client" "$initializer" >/dev/null 2>&1 || true
    docker network rm "$network" >/dev/null 2>&1 || true
    docker volume rm "$fixture" >/dev/null 2>&1 || true
    rm -rf -- "$scratch"
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
mkdir "$scratch/fixture"
cp "$repo/test-support/ingestion-image/api.mjs" "$scratch/fixture/"
# Disposable self-signed trust anchors. No production credentials are used.
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -days 2 \
    -subj /CN=api -addext subjectAltName=DNS:api \
    -keyout "$scratch/fixture/tls.key" -out "$scratch/fixture/tls.crt" >/dev/null 2>&1
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -days 2 \
    -subj /CN=untrusted-fixture -keyout "$scratch/untrusted.key" \
    -out "$scratch/fixture/untrusted.crt" >/dev/null 2>&1
printf '%s\n' synthetic-first > "$scratch/fixture/token"
printf '%s\n' synthetic-first > "$scratch/fixture/stale-token"
printf '%s\n' synthetic-forbidden > "$scratch/fixture/forbidden-token"
python3 - "$scratch/fixture" <<'PY'
import json, pathlib, sys
root = pathlib.Path(sys.argv[1])
config = {
    'apiVersion': 'v1', 'kind': 'Config',
    'clusters': [{'name': 'fixture', 'cluster': {'server': 'https://api:8443', 'certificate-authority': '/fixture/tls.crt'}}],
    'users': [{'name': 'controller', 'user': {'tokenFile': '/fixture/token'}}],
    'contexts': [{'name': 'openlegal-document-controller', 'context': {'cluster': 'fixture', 'user': 'controller', 'namespace': 'openlegal-documents'}}],
    'current-context': 'openlegal-document-controller',
}
(root / 'kubeconfig').write_text(json.dumps(config))
for name, token in [('missing-token', 'absent'), ('stale', 'stale-token'), ('forbidden', 'forbidden-token')]:
    config['users'][0]['user']['tokenFile'] = f'/fixture/{token}'
    (root / name).write_text(json.dumps(config))
config['users'][0]['user']['tokenFile'] = '/fixture/token'
config['clusters'][0]['cluster']['certificate-authority'] = '/fixture/untrusted.crt'
(root / 'bad-ca').write_text(json.dumps(config))
config['clusters'][0]['cluster']['server'] = 'https://127.0.0.1:1'
(root / 'conflicting-config').write_text(json.dumps(config))
pod = {'apiVersion': 'v1', 'kind': 'Pod', 'metadata': {'name': 'document-image-fixture', 'namespace': 'openlegal-documents'},
       'spec': {'automountServiceAccountToken': False, 'restartPolicy': 'Never',
                'containers': [{'name': 'worker', 'image': 'synthetic.invalid/worker@sha256:' + '0' * 64}]}}
(root / 'pod.json').write_text(json.dumps(pod))
pod['spec']['containers'][0]['name'] = 'invalid-fixture-worker'
(root / 'invalid-pod.json').write_text(json.dumps(pod))
PY
chmod 444 "$scratch/fixture/"*
docker pull --platform "$native_platform" "$node_image" >/dev/null
docker network create --internal "$network" >/dev/null
docker volume create "$fixture" >/dev/null
tar -c -C "$scratch/fixture" . | docker run --rm -i --name "$initializer" \
    --platform "$native_platform" --network none --user 0:0 \
    --mount "type=volume,source=$fixture,target=/fixture" \
    --entrypoint tar "$node_image" -x -C /fixture
hardening=(--read-only --cap-drop ALL --security-opt no-new-privileges --user 10004:10004 \
    --memory 256m --cpus 1 --pids-limit 64 \
    --mount "type=volume,source=$fixture,target=/fixture,readonly")
# Docker tmpfs is bounded at 64 MiB; deployed emptyDir is disk-backed and its
# ephemeral-storage enforcement requires separate Kubernetes acceptance.
docker run -d --name "$api" --platform "$native_platform" --network "$network" --network-alias api \
    "${hardening[@]}" --tmpfs '/tmp:rw,noexec,nosuid,nodev,size=16m,mode=1777' \
    --entrypoint node "$node_image" /fixture/api.mjs >/dev/null
for attempt in {1..30}; do
    docker logs "$api" 2>/dev/null | grep -q 'Synthetic TLS API ready' && break
    if (( attempt == 30 )); then echo 'Synthetic API failed to start' >&2; exit 1; fi
    sleep 1
done
docker run -d --name "$client" --platform "$platform" --network "$network" \
    "${hardening[@]}" --tmpfs '/tmp:rw,noexec,nosuid,nodev,size=64m,mode=1777' \
    --env KUBECONFIG=/fixture/conflicting-config --env HTTPS_PROXY=http://127.0.0.1:1 \
    --entrypoint /bin/sh "$image" -c 'sleep 600' >/dev/null
fixed_env=(/usr/bin/env -i PATH=/usr/local/bin:/usr/bin:/bin HOME=/tmp TMPDIR=/tmp)
base=(--context openlegal-document-controller --namespace openlegal-documents \
    --request-timeout=30s --cache-dir=/tmp/openlegal-kubectl-cache)
kubectl() {
    local config=$1
    shift
    docker exec -i "$client" "${fixed_env[@]}" /usr/local/bin/kubectl \
        --kubeconfig "/fixture/$config" "${base[@]}" "$@"
}
expect_failure() {
    local label=$1 pattern=$2
    shift 2
    if "$@" > "$scratch/failure" 2>&1; then echo "$label unexpectedly succeeded" >&2; exit 1; fi
    if ! grep -Eqi -- "$pattern" "$scratch/failure"; then
        echo "$label failed for an unexpected reason" >&2
        cat "$scratch/failure" >&2; exit 1
    fi
}
kubectl kubeconfig get resourcequota document-budget -o json > "$scratch/quota.json"
python3 - "$scratch/quota.json" <<'PY'
import json, sys
with open(sys.argv[1], encoding='utf-8') as source:
    assert json.load(source)['spec']['hard']['pods'] == '2'
PY
# Explicit configuration also wins if KUBECONFIG reaches kubectl directly.
docker exec "$client" "${fixed_env[@]}" KUBECONFIG=/fixture/conflicting-config \
    /usr/local/bin/kubectl --kubeconfig /fixture/kubeconfig "${base[@]}" \
    get resourcequota document-budget -o json >/dev/null
expect_failure 'Missing kubeconfig' 'no such file|does not exist' kubectl absent get resourcequota document-budget
expect_failure 'Missing context' 'context.*does not exist|no context exists' \
    kubectl kubeconfig --context missing get resourcequota document-budget
expect_failure 'Missing token file' 'no such file|unable to read' kubectl missing-token get resourcequota document-budget
expect_failure 'Untrusted CA' 'unknown authority|certificate signed' kubectl bad-ca get resourcequota document-budget
expect_failure 'API forbidden' 'Forbidden' kubectl forbidden get resourcequota document-budget
expect_failure 'Strict synthetic admission' 'Invalid' kubectl kubeconfig create -f - < "$scratch/fixture/invalid-pod.json"
kubectl kubeconfig create -f - < "$scratch/fixture/pod.json"
kubectl kubeconfig delete pod document-image-fixture --ignore-not-found --wait=true --timeout=30s
# Replace the token atomically, as a projection refresh does. This exercises client
# rereading between commands; it does not prove kubelet projection or API RBAC.
docker run --rm --name "$initializer" --platform "$native_platform" --network none --user 0:0 \
    --mount "type=volume,source=$fixture,target=/fixture" --entrypoint /bin/sh "$node_image" -ec \
    'printf "%s\n" synthetic-second > /fixture/token-next; chmod 444 /fixture/token-next; mv /fixture/token-next /fixture/token'
kubectl kubeconfig get resourcequota document-budget -o json >/dev/null
expect_failure 'Expired token' 'Unauthorized|logged in|provide credentials' kubectl stale get resourcequota document-budget
# BusyBox du reports allocated KiB; compare the same bounded tmpfs consumption.
cache_kib=$(docker exec "$client" du -sk /tmp/openlegal-kubectl-cache | cut -f1)
[[ $cache_kib =~ ^[0-9]+$ ]] && (( cache_kib < 64 * 1024 ))
docker exec "$api" cat /tmp/events.jsonl > "$scratch/events.jsonl"
python3 "$repo/test-support/ingestion-image/assert-events.py" "$scratch/events.jsonl"
for container in "$api" "$client"; do
    [[ $(docker inspect --format '{{.State.OOMKilled}}' "$container") == false ]]
    [[ $(docker inspect --format '{{.State.Running}}' "$container") == true ]]
done
printf 'Packaged kubectl TLS fixture passed for %s; cache allocation %s KiB (synthetic data only).\n' "$platform" "$cache_kib"
