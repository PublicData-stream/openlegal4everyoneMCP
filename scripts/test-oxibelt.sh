#!/usr/bin/env bash
# Local, opt-in integration. Requires Linux, Cargo, Git, OpenSSL, Python, rootless Docker.
set -euo pipefail
profile=fixture
while (( $# )); do
    case "$1" in
        --profile)
            [[ $# -ge 2 ]] || { echo '--profile requires fixture or kubernetes' >&2; exit 2; }
            profile=$2
            shift 2
            ;;
        --help|-h)
            echo 'Usage: scripts/test-oxibelt.sh [--profile fixture|kubernetes]'
            echo 'The kubernetes profile tests the production OxiBelt example in Docker, not Kubernetes routing.'
            exit 0
            ;;
        *) echo "Unknown argument: $1" >&2; exit 2 ;;
    esac
done
case "$profile" in
    fixture|kubernetes) ;;
    *) echo "Unknown profile: $profile" >&2; exit 2 ;;
esac
repo=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
revision=e879ac7e263b0af9a86280cd8799de44cbffc05c
scratch=$(mktemp -d)
run_id="openlegal-edge-$$"
image="$run_id:local"
network="$run_id"
fixture_volume="$run_id-fixture"
blob_volume="$run_id-blobs"
postgres="$run_id-postgres"
postgres_image=postgres@sha256:ae6c78831cbc35fa3a4aaf4d763ddacf6183d6004774cc2dc28b3920410d1d1a
backend="$run_id-backend"
edge="$run_id-edge"
cleanup() {
    status=$?
    if (( status != 0 )); then
        docker inspect --format '{{json .State}}' "$backend" "$edge" >&2 2>/dev/null || true
        docker logs "$backend" >&2 2>/dev/null || true
        docker logs "$edge" >&2 2>/dev/null || true
    fi
    docker rm -fv "$edge" "$backend" "$postgres" >/dev/null 2>&1 || true
    docker network rm "$network" >/dev/null 2>&1 || true
    docker volume rm "$fixture_volume" "$blob_volume" >/dev/null 2>&1 || true
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
if [[ -z ${SERVER_BINARY:-} || -z ${WT_CLIENT_BINARY:-} || -z ${MOCK_UPSTREAM_BINARY:-} ]]; then
    cargo build --workspace --bins --examples --locked
fi
server_binary=${SERVER_BINARY:-"$repo/target/debug/openlegal-server"}
client_binary=${WT_CLIENT_BINARY:-"$repo/target/debug/examples/wt_client"}
mock_binary=${MOCK_UPSTREAM_BINARY:-"$repo/target/debug/examples/mock_upstream"}
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
    OXIBELT_BUILD_VERSION=0.10.0 \
    OXIBELT_BUILD_REVISION="$revision" OXIBELT_BUILD_REF=refs/tags/0.10.0 \
    OXIBELT_BUILD_DIRTY=clean OXIBELT_BUILD_KIND=tagged_development \
    CARGO_TARGET_DIR="$target_dir" CARGO_PROFILE_DEV_DEBUG=0 \
        cargo build --manifest-path "$scratch/source/Cargo.toml" \
        -p oxibelt --bin oxibelt --locked -j "${OXIBELT_BUILD_JOBS:-2}"
    oxibelt_binary="$target_dir/debug/oxibelt"
fi
"$oxibelt_binary" --version | python3 -c '
import json, sys
lines = sys.stdin.read().splitlines()
identity = next((json.loads(line.split("=", 1)[1]) for line in lines if line.startswith("OXIBELT_BUILD_IDENTITY_V1=")), None)
assert identity and identity.get("revision") == sys.argv[1] and identity.get("dirty") == "clean" and identity.get("version") == "0.10.0" and identity.get("source_ref") == "refs/tags/0.10.0" and identity.get("kind") == "tagged_development", "OxiBelt binary must report the pinned clean 0.10.0 release source"
print("Verified OxiBelt revision:", identity["revision"])
' "$revision"
mkdir -p "$scratch/bin" "$scratch/fixture/config" "$scratch/fixture/cert"
cp "$server_binary" "$scratch/bin/openlegal-server"
cp "$client_binary" "$scratch/bin/wt_client"
cp "$mock_binary" "$scratch/bin/mock_upstream"
cp "$oxibelt_binary" "$scratch/bin/oxibelt"
cp deploy/oxibelt/backend.toml scripts/http_smoke.py scripts/serving_smoke.py "$scratch/fixture/"
if [[ -n ${DEMO_WIDGET_HTML:-} ]]; then
    cp "$DEMO_WIDGET_HTML" "$scratch/fixture/widget.html"
else
    printf '%s\n' '<html><head><meta name="openlegal-source-url" content="__OPENLEGAL_SOURCE_URL__"></head><body>Synthetic transport resource fixture</body></html>' > "$scratch/fixture/widget.html"
fi
if [[ -n ${TEXT_DIFF_WIDGET_HTML:-} ]]; then
    cp "$TEXT_DIFF_WIDGET_HTML" "$scratch/fixture/text-diff.html"
else
    printf '%s\n' '<html><head><meta name="openlegal-source-url" content="__OPENLEGAL_SOURCE_URL__"></head><body>Text comparison transport resource fixture</body></html>' > "$scratch/fixture/text-diff.html"
fi
# Both supplied artifacts and transport-only fixtures obey their individual caps.
python3 - "$scratch/fixture/widget.html" "$scratch/fixture/text-diff.html" <<'PYBOUND'
import pathlib, sys
for name, maximum in zip(sys.argv[1:], (1024 * 1024, 3 * 1024 * 1024), strict=True):
    assert pathlib.Path(name).stat().st_size <= maximum, f"widget exceeds raw HTML limit: {name}"
PYBOUND
cat >> "$scratch/fixture/backend.toml" <<'TOML'

[http.tls]
certificate = "/fixture/cert/backend.pem"
private_key = "/fixture/cert/backend-key.pem"

[limits]
max_message_bytes = 16777216
max_buffer_bytes = 268435456

[limits.rate_limit]
calls_per_second = 1
burst = 1

[limits.rate_limit.verified_tunnel]
calls_per_second = 1000
burst = 1000

[edge_mtls]
client_ca_file = "/fixture/cert/edge-client-ca.pem"
required_client_dns_san = "oxibelt.openlegal.internal"

[demo]
upstream = "http://127.0.0.1:8081"
widget_html = "/fixture/widget.html"

[text_diff]
widget_html = "/fixture/text-diff.html"

[cache]
mode = "persistent"

[cache.postgres]
tls_mode = "plaintext"

[cache.blob]
kind = "filesystem"
path = "/blobs"
TOML
if [[ $profile == kubernetes ]]; then
    # Exercise the committed handoff with disposable identities. The backend
    # listens on NodePort numbers directly; this does not test Service translation.
    python3 - "$scratch/fixture" <<'PYPROFILE'
import pathlib, sys
root = pathlib.Path(sys.argv[1])
source = pathlib.Path("deploy/oxibelt/kubernetes-upstream.example.toml").read_text()
for before, after in (
    ("replace-with-private-node.invalid", "backend"),
    ("openlegal4everyone.mcp.publicdata.stream", "edge"),
    ("openlegal4everyone.reference.publicdata.stream", "reference-edge"),
    ('["backend-ca.pem"]', '["ca.pem"]'),
):
    assert before in source, f"missing handoff substitution: {before}"
    source = source.replace(before, after)
(root / "config/oxibelt.toml").write_text(source)
backend = root / "backend.toml"
backend.write_text(backend.read_text().replace(":8080", ":30080").replace(":4433", ":30433"))
PYPROFILE
else
    cp deploy/oxibelt/oxibelt.toml "$scratch/fixture/config/"
fi
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
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -days 2 \
    -addext basicConstraints=critical,CA:TRUE -addext keyUsage=critical,keyCertSign,cRLSign \
    -subj /CN=OpenLegal-Edge-Client-CA -keyout "$scratch/edge-client-ca-key.pem" \
    -out "$scratch/fixture/cert/edge-client-ca.pem" >/dev/null 2>&1
for name in edge backend wrong-backend; do
    openssl req -new -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
        -subj "/CN=$name" -keyout "$scratch/fixture/cert/$name-key.pem" \
        -out "$scratch/$name.csr" >/dev/null 2>&1
    certificate_names="DNS:$name"
    if [[ $name == edge ]]; then certificate_names+=",DNS:reference-edge"; fi
    printf 'subjectAltName=%s\nbasicConstraints=CA:FALSE\nkeyUsage=digitalSignature\nextendedKeyUsage=serverAuth\n' "$certificate_names" > "$scratch/$name.ext"
    openssl x509 -req -in "$scratch/$name.csr" -CA "$scratch/fixture/cert/ca.pem" \
        -CAkey "$scratch/ca-key.pem" -CAcreateserial -days 2 \
        -extfile "$scratch/$name.ext" -out "$scratch/fixture/cert/$name.pem" >/dev/null 2>&1
done
openssl req -new -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
    -subj /CN=oxibelt.openlegal.internal \
    -keyout "$scratch/fixture/cert/openlegal-client-key.pem" \
    -out "$scratch/edge-client.csr" >/dev/null 2>&1
printf 'subjectAltName=DNS:oxibelt.openlegal.internal\nbasicConstraints=CA:FALSE\nkeyUsage=digitalSignature\nextendedKeyUsage=clientAuth\n' > "$scratch/edge-client.ext"
openssl x509 -req -in "$scratch/edge-client.csr" \
    -CA "$scratch/fixture/cert/edge-client-ca.pem" \
    -CAkey "$scratch/edge-client-ca-key.pem" -CAcreateserial -days 2 \
    -extfile "$scratch/edge-client.ext" \
    -out "$scratch/fixture/cert/openlegal-client.pem" >/dev/null 2>&1
openssl verify -CAfile "$scratch/fixture/cert/edge-client-ca.pem" \
    -purpose sslclient -verify_hostname oxibelt.openlegal.internal \
    "$scratch/fixture/cert/openlegal-client.pem"
# Verify the negative fixture isolates identity from trust before using it.
openssl verify -CAfile "$scratch/fixture/cert/ca.pem" "$scratch/fixture/cert/wrong-backend.pem"
if openssl verify -CAfile "$scratch/fixture/cert/ca.pem" -verify_hostname backend \
    "$scratch/fixture/cert/wrong-backend.pem" >"$scratch/wrong-san-verification.log" 2>&1; then
    echo 'Wrong-SAN fixture unexpectedly identifies backend' >&2
    exit 1
fi
# OxiBelt requires the mTLS client key to be unreadable by group and world.
# The serving UID owns that key inside the disposable fixture volume.
chmod 444 "$scratch/fixture/cert/"*.pem
chmod 400 "$scratch/fixture/cert/openlegal-client-key.pem"
python3 - "$scratch/fixture/config/oxibelt.toml" "$scratch/fixture/backend.toml" <<'PY'
import pathlib, sys
source = pathlib.Path(sys.argv[1])
config = source.read_text()
(source.parent / "oxibelt-untrusted.toml").write_text(config.replace(
    'trusted_ca_certs = ["ca.pem"]', 'trusted_ca_certs = ["wrong-ca.pem"]'))
client_identity = ('[upstreams.tls.client_identity]\n'
                   'cert_chain = "openlegal-client.pem"\n'
                   'private_key = "openlegal-client-key.pem"\n\n')
assert config.count(client_identity) == 2, "both upstreams must present the edge client identity"
(source.parent / "oxibelt-no-client.toml").write_text(config.replace(client_identity, ""))
assert config.count('rate = "1000r/s"\nburst = 1000') == 1
(source.parent / "oxibelt-http-throttled.toml").write_text(config.replace(
    'rate = "1000r/s"\nburst = 1000', 'rate = "1r/s"\nburst = 1'))
# The first 100r/s policy is the CONNECT handshake bucket; the reference
# route has a later independent bucket with the same rate.
assert config.count('rate = "100r/s"\nburst = 100') in (1, 2)
(source.parent / "oxibelt-wt-throttled.toml").write_text(config.replace(
    'rate = "100r/s"\nburst = 100', 'rate = "1r/s"\nburst = 1', 1))
backend = pathlib.Path(sys.argv[2])
(backend.parent / "backend-wrong-san.toml").write_text(backend.read_text().replace(
    '/cert/backend.pem', '/cert/wrong-backend.pem').replace(
    '/cert/backend-key.pem', '/cert/wrong-backend-key.pem'))
PY
docker build -t "$image" "$scratch"
docker network create --internal "$network" >/dev/null
docker volume create "$fixture_volume" >/dev/null
docker volume create "$blob_volume" >/dev/null
# Stream fixtures into a volume; Docker never receives private keys as build input.
# The initializer is container-root on the rootless daemon; serving mounts are read-only.
tar -c -C "$scratch/fixture" . | docker run --rm -i --network none --user 0:0 \
    --mount "type=volume,source=$fixture_volume,target=/fixture" \
    --entrypoint tar "$image" -x -C /fixture
docker run --rm --network none --user 0:0 \
    --mount "type=volume,source=$fixture_volume,target=/fixture" \
    --entrypoint /bin/sh "$image" -c 'chown 65532:65532 /fixture/cert/openlegal-client-key.pem && chmod 400 /fixture/cert/openlegal-client-key.pem'
hardening=(--mount "type=volume,source=$fixture_volume,target=/fixture,readonly" --network "$network" --read-only --cap-drop ALL --security-opt no-new-privileges --tmpfs "/tmp:rw,noexec,nosuid,size=16m")
docker run --rm "${hardening[@]}" --entrypoint /usr/local/bin/oxibelt "$image" --config /fixture/config/oxibelt.toml --check
# The database and immutable blobs are separate disposable persistence components.
password=$(python3 -c 'import secrets; print(secrets.token_hex(24))')
docker run -d --name "$postgres" --network "$network" --network-alias postgres \
    --memory 512m -e POSTGRES_PASSWORD="$password" -e POSTGRES_DB=openlegal \
    "$postgres_image" -c log_statement=none -c log_min_error_statement=panic >/dev/null
for attempt in {1..60}; do
    if docker exec "$postgres" pg_isready -U postgres -d openlegal >/dev/null 2>&1; then break; fi
    if (( attempt == 60 )); then echo 'PostgreSQL 18 did not become ready' >&2; exit 1; fi
    sleep 1
done
database_url="postgresql://postgres:$password@postgres:5432/openlegal"
docker run --rm "${hardening[@]}" -e OPENLEGAL_MIGRATION_DATABASE_URL="$database_url" \
    "$image" --migrate /fixture/backend.toml
# Initializer is root only within the rootless daemon; serving owns a private blob root.
docker run --rm --network none --user 0:0 --mount "type=volume,source=$blob_volume,target=/blobs" \
    --entrypoint /bin/sh "$image" -c 'chown 65532:65532 /blobs && chmod 700 /blobs'
start_backend() {
    docker run -d --name "$backend" --network-alias backend "${hardening[@]}" --memory 1g \
        --mount "type=volume,source=$blob_volume,target=/blobs" \
        -e OPENLEGAL_DATABASE_URL="$database_url" "$image" "$1" >/dev/null
    docker exec -d "$backend" /usr/local/bin/mock_upstream 127.0.0.1:8081
}
start_backend /fixture/backend.toml
start_edge() {
    docker run -d --name "$edge" --network-alias edge --network-alias reference-edge "${hardening[@]}" --memory 1g --ulimit stack=67108864:67108864 \
        --entrypoint /usr/local/bin/oxibelt "$image" --config "$1" >/dev/null
    if [[ ${2:-ready} == ready ]]; then
        docker run --rm "${hardening[@]}" --entrypoint python3 "$image" /fixture/http_smoke.py /fixture/cert/ca.pem --ready
    fi
}
reject_http_upstream() {
    docker run --rm "${hardening[@]}" --entrypoint python3 "$image" -c '
import sys, time
sys.path.insert(0, "/fixture")
from http_smoke import MODERN, exchange, request
deadline = time.monotonic() + 10
while True:
    try:
        status, _, _ = exchange(request("tools/list", MODERN), timeout=2)
        break
    except OSError:
        if time.monotonic() >= deadline:
            raise
        time.sleep(0.1)
assert status in (502, 503, 504), f"expected upstream TLS rejection, got HTTP {status}"
'
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
# The deployed-endpoint profile must work without demo/provider operations. This
# remains Docker fixture evidence, including when the profile is kubernetes.
docker run --rm "${hardening[@]}" --entrypoint python3 "$image" /fixture/serving_smoke.py \
    --http-url https://edge:8443/mcp --webtransport-url https://edge:8443/mcp-wt/v1 \
    --origin https://example.test --ca-file /fixture/cert/ca.pem --wt-client /usr/local/bin/wt_client
if [[ $profile == kubernetes ]]; then
    docker run --rm "${hardening[@]}" --entrypoint python3 "$image" -c '
import http.client, ssl
context = ssl.create_default_context(cafile="/fixture/cert/ca.pem")
for method, path in (("GET", "/source/unknown"), ("HEAD", "/source/unknown"), ("POST", "/source/unknown"), ("GET", "/ready"), ("POST", "/mcp")):
    connection = http.client.HTTPSConnection("reference-edge", 8443, context=context, timeout=10)
    connection.request(method, path, headers={"Host": "reference-edge"})
    response = connection.getresponse()
    body = response.read()
    assert response.status in (404, 405), (method, path, response.status)
    if method in ("GET", "HEAD") and path.startswith("/source/"):
        assert response.getheader("x-accel-buffering") == "no", (method, path, response.status, "GET/HEAD did not reach the bounded backend")
        assert response.getheader("cache-control") == "no-store"
    if method == "HEAD": assert not body
    connection.close()
print("Reference host: GET/HEAD reach bounded backend; other methods and operational paths excluded")
'
    # The edge stays up while the backend is replaced. Repeated fresh MCP
    # sessions exercise the stale-H1-connection failure seen in acceptance.
    docker rm -f "$backend" >/dev/null
    start_backend /fixture/backend.toml
    for attempt in {1..60}; do
        if docker exec "$backend" python3 -c 'import urllib.request; urllib.request.urlopen("http://127.0.0.1:9090/ready", timeout=2)' >/dev/null 2>&1; then break; fi
        if (( attempt == 60 )); then echo 'Replacement backend did not become Ready' >&2; exit 1; fi
        sleep 1
    done
    for attempt in 1 2 3; do
        docker run --rm "${hardening[@]}" --entrypoint python3 "$image" /fixture/serving_smoke.py \
            --http-url https://edge:8443/mcp --webtransport-url https://edge:8443/mcp-wt/v1 \
            --origin https://example.test --ca-file /fixture/cert/ca.pem --wt-client /usr/local/bin/wt_client
    done
fi
for protocol in 2026-07-28 2025-11-25; do
    client https://edge:8443/mcp-wt/v1 /fixture/cert/ca.pem "$protocol" --demo --text-diff
    # A fresh process creates a fresh QUIC connection, checking reconnection too.
    client https://edge:8443/mcp-wt/v1 /fixture/cert/ca.pem "$protocol" https://example.test
done
reject_client https://edge:8443/mcp-wt/v1 /fixture/cert/ca.pem 2026-07-28 https://rejected.test
reject_client https://edge:8443/mcp-wt/wrong /fixture/cert/ca.pem 2026-07-28
reject_client https://edge:8443/mcp-wt/v1 /fixture/cert/wrong-ca.pem 2026-07-28
# Exercise the route's own request bucket with a temporary one-token policy.
docker rm -f "$edge" >/dev/null
start_edge /fixture/config/oxibelt-http-throttled.toml skip-ready
docker run --rm "${hardening[@]}" --entrypoint python3 "$image" -c '
import sys, time
sys.path.insert(0, "/fixture")
from http_smoke import MODERN, exchange, request
deadline = time.monotonic() + 10
while True:
    try:
        first, _, _ = exchange(request("tools/list", MODERN), timeout=2)
        break
    except OSError:
        if time.monotonic() >= deadline:
            raise
        time.sleep(0.1)
assert first == 200, first
second, _, _ = exchange(request("tools/list", MODERN))
assert second == 429, second
time.sleep(1.2)
recovered, _, _ = exchange(request("tools/list", MODERN))
assert recovered == 200, recovered
print("HTTP edge rate limit: 429 and refill recovery", flush=True)
'
# A WebTransport rate limit counts CONNECT handshakes, not calls in a session.
docker rm -f "$edge" >/dev/null
start_edge /fixture/config/oxibelt-wt-throttled.toml
client https://edge:8443/mcp-wt/v1 /fixture/cert/ca.pem 2026-07-28
reject_client https://edge:8443/mcp-wt/v1 /fixture/cert/ca.pem 2026-07-28
sleep 1.2
client https://edge:8443/mcp-wt/v1 /fixture/cert/ca.pem 2026-07-28
# Change only upstream trust and prove both private TLS handshakes fail.
docker rm -f "$edge" >/dev/null
start_edge /fixture/config/oxibelt-untrusted.toml skip-ready
reject_http_upstream
reject_client https://edge:8443/mcp-wt/v1 /fixture/cert/ca.pem 2026-07-28
# Restore trust and reconnect to rule out persistent transport/startup failures.
docker rm -f "$edge" >/dev/null
start_edge /fixture/config/oxibelt.toml
client https://edge:8443/mcp-wt/v1 /fixture/cert/ca.pem 2026-07-28
# OxiBelt cannot use either backend transport without a client certificate.
docker rm -f "$edge" >/dev/null
start_edge /fixture/config/oxibelt-no-client.toml skip-ready
reject_http_upstream
reject_client https://edge:8443/mcp-wt/v1 /fixture/cert/ca.pem 2026-07-28
# Restart both endpoints to rule out reuse of an already authenticated QUIC
# connection, then change only the backend certificate identity (same trusted CA).
docker rm -f "$edge" "$backend" >/dev/null
start_backend /fixture/backend-wrong-san.toml
start_edge /fixture/config/oxibelt.toml skip-ready
reject_http_upstream
reject_client https://edge:8443/mcp-wt/v1 /fixture/cert/ca.pem 2026-07-28
# Restore the valid identity and prove both HTTP readiness and QUIC recovery.
docker rm -f "$edge" "$backend" >/dev/null
start_backend /fixture/backend.toml
start_edge /fixture/config/oxibelt.toml
client https://edge:8443/mcp-wt/v1 /fixture/cert/ca.pem 2026-07-28
printf 'OxiBelt HTTP and WebTransport integration checks passed (profile: %s).\n' "$profile"
