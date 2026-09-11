#!/usr/bin/env bash
# Local, opt-in integration. Requires Linux, Cargo, Git, OpenSSL, Python, rootless Docker.
set -euo pipefail
repo=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
revision=72564d165dfd05cb29a64aeebd19fccd7944ea6f
scratch=$(mktemp -d)
run_id="openlegal-edge-$$"
image="$run_id:local"
network="$run_id"
fixture_volume="$run_id-fixture"
backend="$run_id-backend"
edge="$run_id-edge"
cleanup() {
    status=$?
    if (( status != 0 )); then
        docker inspect --format '{{json .State}}' "$backend" "$edge" >&2 2>/dev/null || true
        docker logs "$backend" >&2 2>/dev/null || true
        docker logs "$edge" >&2 2>/dev/null || true
    fi
    docker rm -f "$edge" "$backend" >/dev/null 2>&1 || true
    docker network rm "$network" >/dev/null 2>&1 || true
    docker volume rm "$fixture_volume" >/dev/null 2>&1 || true
    docker image rm "$image" >/dev/null 2>&1 || true
    rm -rf -- "$scratch"
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
for command in git cargo openssl python3 docker; do
    command -v "$command" >/dev/null || { echo "Missing command: $command" >&2; exit 1; }
done
cd "$repo"
if [[ -z ${SERVER_BINARY:-} || -z ${WT_CLIENT_BINARY:-} ]]; then
    cargo build --workspace --bins --examples --locked
fi
server_binary=${SERVER_BINARY:-"$repo/target/debug/openlegal-server"}
client_binary=${WT_CLIENT_BINARY:-"$repo/target/debug/examples/wt_client"}
if [[ -n ${OXIBELT_BINARY:-} ]]; then
    oxibelt_binary=$OXIBELT_BINARY
else
    source_checkout=${OXIBELT_SOURCE:-"$scratch/source-git"}
    if [[ -z ${OXIBELT_SOURCE:-} ]]; then
        git init -q "$source_checkout"
        git -C "$source_checkout" fetch --depth 1 https://github.com/OxiBelt/OxiBelt.git "$revision"
    fi
    mkdir "$scratch/source"
    # Archive the object, never build a possibly modified working tree.
    git -C "$source_checkout" archive "$revision" | tar -x -C "$scratch/source"
    target_dir=${OXIBELT_TARGET_DIR:-"$repo/target/oxibelt"}
    target_dir=$(realpath -m -- "$target_dir")
    OXIBELT_BUILD_VERSION=0.0.0-dev.g72564d16 \
    OXIBELT_BUILD_REVISION="$revision" OXIBELT_BUILD_REF=unknown \
    OXIBELT_BUILD_DIRTY=clean OXIBELT_BUILD_KIND=git_development \
    CARGO_TARGET_DIR="$target_dir" CARGO_PROFILE_DEV_DEBUG=0 \
        cargo build --manifest-path "$scratch/source/Cargo.toml" \
        -p oxibelt --bin oxibelt --locked -j "${OXIBELT_BUILD_JOBS:-2}"
    oxibelt_binary="$target_dir/debug/oxibelt"
fi
"$oxibelt_binary" --version | python3 -c '
import json, sys
lines = sys.stdin.read().splitlines()
identity = next((json.loads(line.split("=", 1)[1]) for line in lines if line.startswith("OXIBELT_BUILD_IDENTITY_V1=")), None)
assert identity and identity.get("revision") == sys.argv[1] and identity.get("dirty") == "clean", "OxiBelt binary must report the pinned clean revision"
print("Verified OxiBelt revision:", identity["revision"])
' "$revision"
mkdir -p "$scratch/bin" "$scratch/fixture/config" "$scratch/fixture/cert"
cp "$server_binary" "$scratch/bin/openlegal-server"
cp "$client_binary" "$scratch/bin/wt_client"
cp "$oxibelt_binary" "$scratch/bin/oxibelt"
cp deploy/oxibelt/backend.toml scripts/http_smoke.py "$scratch/fixture/"
cp deploy/oxibelt/oxibelt.toml "$scratch/fixture/config/"
cp deploy/oxibelt/Dockerfile.harness "$scratch/Dockerfile"
printf '*\n!Dockerfile\n!bin/\n!bin/**\n' > "$scratch/.dockerignore"
# Synthetic two-day certificates are generated solely for this disposable network.
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -days 2 \
    -addext basicConstraints=critical,CA:TRUE -addext keyUsage=critical,keyCertSign,cRLSign \
    -subj /CN=OpenLegal-Integration-CA -keyout "$scratch/ca-key.pem" \
    -out "$scratch/fixture/cert/ca.pem" >/dev/null 2>&1
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -days 2 \
    -addext basicConstraints=critical,CA:TRUE -addext keyUsage=critical,keyCertSign,cRLSign \
    -subj /CN=OpenLegal-Untrusted-CA -keyout "$scratch/wrong-ca-key.pem" \
    -out "$scratch/fixture/cert/wrong-ca.pem" >/dev/null 2>&1
for name in edge backend; do
    openssl req -new -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
        -subj "/CN=$name" -keyout "$scratch/fixture/cert/$name-key.pem" \
        -out "$scratch/$name.csr" >/dev/null 2>&1
    printf 'subjectAltName=DNS:%s\nbasicConstraints=CA:FALSE\nkeyUsage=digitalSignature\nextendedKeyUsage=serverAuth\n' "$name" > "$scratch/$name.ext"
    openssl x509 -req -in "$scratch/$name.csr" -CA "$scratch/fixture/cert/ca.pem" \
        -CAkey "$scratch/ca-key.pem" -CAcreateserial -days 2 \
        -extfile "$scratch/$name.ext" -out "$scratch/fixture/cert/$name.pem" >/dev/null 2>&1
done
# Container's unprivileged user must read these disposable keys.
chmod 444 "$scratch/fixture/cert/"*.pem
python3 - "$scratch/fixture/config/oxibelt.toml" <<'PY'
import pathlib, sys
source = pathlib.Path(sys.argv[1])
(source.parent / "oxibelt-untrusted.toml").write_text(source.read_text().replace(
    'trusted_ca_certs = ["ca.pem"]', 'trusted_ca_certs = ["wrong-ca.pem"]'))
PY
docker build -t "$image" "$scratch"
docker network create --internal "$network" >/dev/null
docker volume create "$fixture_volume" >/dev/null
# Stream fixtures into a volume; Docker never receives private keys as build input.
# The initializer is container-root on the rootless daemon; serving mounts are read-only.
tar -c -C "$scratch/fixture" . | docker run --rm -i --network none --user 0:0 \
    --mount "type=volume,source=$fixture_volume,target=/fixture" \
    --entrypoint tar "$image" -x -C /fixture
hardening=(--mount "type=volume,source=$fixture_volume,target=/fixture,readonly" --network "$network" --read-only --cap-drop ALL --security-opt no-new-privileges --tmpfs "/tmp:rw,noexec,nosuid,size=16m")
docker run --rm "${hardening[@]}" --entrypoint /usr/local/bin/oxibelt "$image" --config /fixture/config/oxibelt.toml --check
docker run -d --name "$backend" --network-alias backend "${hardening[@]}" --memory 512m "$image" >/dev/null
start_edge() {
    docker run -d --name "$edge" --network-alias edge "${hardening[@]}" --memory 1g --ulimit stack=67108864:67108864 \
        --entrypoint /usr/local/bin/oxibelt "$image" --config "$1" >/dev/null
    docker run --rm "${hardening[@]}" --entrypoint python3 "$image" /fixture/http_smoke.py /fixture/cert/ca.pem --ready
}
client() {
    docker run --rm "${hardening[@]}" --entrypoint /usr/local/bin/wt_client "$image" "$@"
}
reject_client() {
    if client "$@"; then
        echo "Unexpected WebTransport success: $1" >&2
        exit 1
    fi
}
start_edge /fixture/config/oxibelt.toml
docker run --rm "${hardening[@]}" --entrypoint python3 "$image" /fixture/http_smoke.py /fixture/cert/ca.pem
for protocol in 2026-07-28 2025-11-25; do
    client https://edge:8443/mcp-wt/v1 /fixture/cert/ca.pem "$protocol"
    # A fresh process creates a fresh QUIC connection, checking reconnection too.
    client https://edge:8443/mcp-wt/v1 /fixture/cert/ca.pem "$protocol" https://example.test
done
reject_client https://edge:8443/mcp-wt/v1 /fixture/cert/ca.pem 2026-07-28 https://rejected.test
reject_client https://edge:8443/mcp-wt/wrong /fixture/cert/ca.pem 2026-07-28
reject_client https://edge:8443/mcp-wt/v1 /fixture/cert/wrong-ca.pem 2026-07-28
# Change only upstream trust, then prove that HTTP still works but QUIC forwarding fails.
docker rm -f "$edge" >/dev/null
start_edge /fixture/config/oxibelt-untrusted.toml
reject_client https://edge:8443/mcp-wt/v1 /fixture/cert/ca.pem 2026-07-28
# Restore trust and reconnect to rule out persistent transport/startup failures.
docker rm -f "$edge" >/dev/null
start_edge /fixture/config/oxibelt.toml
client https://edge:8443/mcp-wt/v1 /fixture/cert/ca.pem 2026-07-28
printf '%s\n' 'OxiBelt HTTP and WebTransport integration checks passed.'
