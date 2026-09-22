#!/usr/bin/env bash
# Synthetic, signed HTTPS APT fixture. Never contacts a legal-data provider.
set -euo pipefail
cd "$(dirname "$0")/.."
rust_image='rust:1.98.1-slim-trixie@sha256:a2de23e559fd8afd260d22beb00f3987073ea0dcc2ba2646cccdaeda6a62a095'
node_image='node:24.21.0-trixie-slim@sha256:8ec5d7557396cfe32d21c3f9c13072355ceab22b584578ca4bb28af31120cffe'
# No arguments runs the complete gate; explicit names permit focused diagnosis.
scenarios=(baseline recovery persistent stall stale tls signature hash deadline)
if (( $# > 0 )); then scenarios=("$@"); fi
for scenario in "${scenarios[@]}"; do
    case "$scenario" in
        baseline|recovery|persistent|stall|stale|tls|signature|hash|deadline) ;;
        *) echo "Unknown snapshot fixture scenario: $scenario" >&2; exit 2 ;;
    esac
done
for command in docker python3 openssl gpg gpgconf dpkg-deb tar; do
    command -v "$command" >/dev/null || { echo "Missing prerequisite: $command" >&2; exit 1; }
done
fixture=$(mktemp -d)
suffix="$$-$RANDOM"
network="openlegal-snapshot-$suffix"
volume="openlegal-snapshot-$suffix"
server="openlegal-snapshot-origin-$suffix"
client="openlegal-snapshot-client-$suffix"
cleanup() {
    docker rm -f "$client" "$server" >/dev/null 2>&1 || true
    docker volume rm "$volume" >/dev/null 2>&1 || true
    docker network rm "$network" >/dev/null 2>&1 || true
    rm -rf "$fixture"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

# Pull first: all subsequent container traffic is confined to the internal bridge.
docker pull "$rust_image" >/dev/null
docker pull "$node_image" >/dev/null
python3 test-support/snapshot-packages/prepare.py "$fixture" > "$fixture/preparation.log" 2>&1 || {
    cat "$fixture/preparation.log"; exit 1;
}
cp test-support/snapshot-packages/{server.mjs,client.sh} scripts/install-snapshot-packages.sh "$fixture/"
docker network create --internal "$network" >/dev/null
docker volume create "$volume" >/dev/null
# Stream only runtime inputs; remote/rootless daemons need no host bind mounts.
# Deliberately use a nonroot archive owner so root-run tests also cover CI ownership.
tar --owner=1001 --group=1001 --numeric-owner -C "$fixture" -cf - \
    repository ca.crt server.key fixture.gpg server.mjs client.sh install-snapshot-packages.sh |
    docker run --pull never --rm -i --user 0:0 --network none \
        --mount "type=volume,source=$volume,target=/fixture" \
        --entrypoint sh "$node_image" -ec '
            tar --no-same-owner -xf - -C /fixture
            test "$(stat -c "%u:%g:%a" /fixture/server.key)" = 0:0:600
        '

origin_failed() {
    echo "Fixture origin failed to start: scenario=$scenario: $1" >&2
    docker inspect --format 'status={{.State.Status}} exit={{.State.ExitCode}} oom={{.State.OOMKilled}}' \
        "$server" >&2 || true
    docker logs "$server" >&2 || true
    exit 1
}

for scenario in "${scenarios[@]}"; do
    echo "Testing snapshot packages: $scenario"
    docker run --pull never -d --user 0:0 --name "$server" --network "$network" \
        --network-alias snapshot.debian.org --read-only --cap-drop ALL --cap-add NET_BIND_SERVICE \
        --security-opt no-new-privileges --memory 256m --pids-limit 64 --tmpfs /tmp:rw,size=16m \
        --mount "type=volume,source=$volume,target=/fixture,readonly" --env "SCENARIO=$scenario" \
        --entrypoint node "$node_image" /fixture/server.mjs >/dev/null
    ready=false
    for ((attempt = 0; attempt < 30; attempt++)); do
        if docker exec "$server" test -f /tmp/ready 2>/dev/null; then ready=true; break; fi
        if ! running=$(docker inspect --format '{{.State.Running}}' "$server"); then
            origin_failed 'container state unavailable'
        fi
        if [[ "$running" != true ]]; then origin_failed 'container stopped'; fi
        sleep 1
    done
    if [[ "$ready" != true ]]; then origin_failed 'readiness timeout'; fi
    if ! docker run --pull never --rm --name "$client" --network "$network" \
        --security-opt no-new-privileges --memory 512m --pids-limit 128 \
        --mount "type=volume,source=$volume,target=/fixture,readonly" \
        --entrypoint /bin/sh "$rust_image" /fixture/client.sh "$scenario" > "$fixture/$scenario.log" 2>&1; then
        cat "$fixture/$scenario.log"
        docker logs "$server"
        exit 1
    fi
    docker exec "$server" cat /tmp/stats.json > "$fixture/$scenario.json"
    python3 - "$scenario" "$fixture/$scenario.json" <<'PY'
import json
import sys

scenario = sys.argv[1]
with open(sys.argv[2]) as stream:
    stats = json.load(stream)
expected_requests = {"baseline": 4, "recovery": 5, "persistent": 6, "stall": 6}
if scenario in expected_requests:
    assert stats["packageRequests"] == expected_requests[scenario], (scenario, stats)
if scenario in ("tls", "signature", "stale", "deadline"):
    assert stats["packageRequests"] == 0, (scenario, stats)
if scenario == "stale":
    assert stats["metadataRequests"] >= 2 and stats["failures"] > 0, stats
if scenario == "hash":
    assert stats["packageRequests"] >= 1, stats
if scenario == "stall":
    assert stats["stalls"] == 6, stats
if scenario == "deadline":
    assert stats["stalls"] >= 1, stats
print(f"PASS {scenario}: {stats}")
PY
    tail -n 1 "$fixture/$scenario.log"
    docker rm -f "$server" >/dev/null
done
printf 'Snapshot package fixtures passed: %s\n' "${scenarios[*]}"
