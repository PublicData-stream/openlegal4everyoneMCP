#!/usr/bin/env bash
# Build and accept one production platform. Provision deployment tools first.
# Requires Docker, Git, OpenSSL, tar and scripts/setup-deployment-tools.sh inputs.
# Docker may use a host rootless daemon: fixtures are streamed into a named volume.
set -euo pipefail
repo=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
for command in docker git openssl tar; do
    command -v "$command" >/dev/null || { echo "Missing command: $command" >&2; exit 1; }
done
case $(docker info --format '{{.Architecture}}') in
    x86_64|amd64) native_platform=linux/amd64 ;;
    aarch64|arm64) native_platform=linux/arm64 ;;
    *) echo 'Unsupported host architecture' >&2; exit 1 ;;
esac
platform=$native_platform
image_target=runtime
output_image=
release_version=
while (( $# )); do
    case "$1" in
        --platform) [[ $# -ge 2 ]] || exit 2; platform=$2; shift 2 ;;
        --target) [[ $# -ge 2 ]] || exit 2; image_target=$2; shift 2 ;;
        --output-image) [[ $# -ge 2 ]] || exit 2; output_image=$2; shift 2 ;;
        --version) [[ $# -ge 2 ]] || exit 2; release_version=$2; shift 2 ;;
        *) echo "Usage: $0 [--platform linux/amd64|linux/arm64] [--target runtime|runtime-ingestion] [--output-image IMAGE --version VERSION]" >&2; exit 2 ;;
    esac
done
case "$image_target" in runtime|runtime-ingestion) ;; *) echo 'Invalid image target' >&2; exit 2 ;; esac
if [[ -n $output_image || -n $release_version ]]; then
    [[ -n $output_image && -n $release_version ]] || { echo 'Output image and version must be supplied together' >&2; exit 2; }
    [[ $output_image =~ ^ghcr\.io/publicdata-stream/openlegal-server(-ingestion)?:[a-zA-Z0-9_.-]+$ ]] || { echo 'Invalid release image name' >&2; exit 2; }
    [[ $release_version =~ ^[0-9]+\.[0-9]+\.[0-9]+(-beta\.[0-9]+|-build\.[0-9a-f]{8})?$ ]] || { echo 'Invalid release version' >&2; exit 2; }
fi
case $platform in
    linux/amd64) cpu_baseline=x86-64-v3 ;;
    linux/arm64) cpu_baseline=generic-arm64 ;;
    *) echo 'Platform must be linux/amd64 or linux/arm64' >&2; exit 2 ;;
esac
if [[ -n $output_image ]]; then
    arch=${platform#linux/}
    case "$image_target:$output_image" in
        "runtime:ghcr.io/publicdata-stream/openlegal-server:$release_version-$arch"|\
        "runtime-ingestion:ghcr.io/publicdata-stream/openlegal-server-ingestion:$release_version-$arch") ;;
        *) echo 'Output image does not match target, version and architecture' >&2; exit 2 ;;
    esac
fi
scratch=$(mktemp -d)
run_id="openlegal-image-$(basename "$scratch" | tr '[:upper:]' '[:lower:]')-$$"
image="$run_id:local"
if [[ -n $output_image ]]; then image=$output_image; fi
network="$run_id"
fixture_volume="$run_id-fixture"
server="$run_id-server"
client="$run_id-client"
initializer="$run_id-initialize"
probe="$run_id-probe"
invalid="$run_id-invalid"
node_image=node:24.21.0-trixie-slim@sha256:8ec5d7557396cfe32d21c3f9c13072355ceab22b584578ca4bb28af31120cffe
cleanup() {
    status=$?
    if (( status != 0 )); then
        docker inspect --format '{{json .State}}' "$server" "$invalid" >&2 2>/dev/null || true
        docker logs "$server" >&2 2>/dev/null || true
        docker logs "$invalid" >&2 2>/dev/null || true
    fi
    docker rm -fv "$server" "$client" "$initializer" "$probe" "$invalid" >/dev/null 2>&1 || true
    docker network rm "$network" >/dev/null 2>&1 || true
    docker volume rm "$fixture_volume" >/dev/null 2>&1 || true
    if (( status != 0 )) || [[ -z $output_image ]]; then
        docker image rm "$image" >/dev/null 2>&1 || true
    fi
    rm -rf -- "$scratch"
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
cd "$repo"
mkdir -p "$scratch/fixture/config" "$scratch/fixture/tls" "$scratch/fixture/invalid-tls"
scripts/test-kubernetes-serving.sh --profile text-only --config-output "$scratch/fixture/config/server.toml"
deploy_tools=${OPENLEGAL_DEPLOY_TOOLS:-$repo/target/deployment-tools}
"$deploy_tools/bin/python" - "$scratch/fixture" <<'PYTHON'
import json
from pathlib import Path
import sys
import tomllib
fixture = Path(sys.argv[1])
config = tomllib.loads((fixture / "config/server.toml").read_text())
(fixture / "client.json").write_text(json.dumps({
    "source": config["source"]["url"],
    "authority": config["http"]["allowed_hosts"][0],
}))
PYTHON
revision=$(git rev-parse HEAD)
version="image-smoke-${revision:0:12}"
if [[ -n $release_version ]]; then version=$release_version; fi
# Keep architecture tags as single manifests; the release workflow attests pushed digests.
docker build --provenance=false --platform "$platform" --file apps/server/Dockerfile \
    --target "$image_target" \
    --build-arg "REVISION=$revision" --build-arg "VERSION=$version" --tag "$image" .
# Pull while network access is available; execution below is isolated.
docker pull --platform "$native_platform" "$node_image" >/dev/null
expect_image() {
    actual=$(docker image inspect --format "$1" "$image")
    [[ $actual == "$2" ]] || { echo "Image property mismatch: $1 = $actual (expected $2)" >&2; exit 1; }
}
expect_image '{{.Os}}/{{.Architecture}}' "$platform"
expect_image '{{.Config.User}}' '10004:10004'
expect_image '{{json .Config.Entrypoint}}' '["/usr/local/bin/openlegal-server"]'
expect_image '{{json .Config.Cmd}}' '["/etc/openlegal/server.toml"]'
expect_image '{{.Config.StopSignal}}' SIGTERM
expect_image '{{index .Config.Labels "org.opencontainers.image.source"}}' 'https://github.com/PublicData-stream/openlegal4everyoneMCP'
expect_image '{{index .Config.Labels "org.opencontainers.image.revision"}}' "$revision"
expect_image '{{index .Config.Labels "org.opencontainers.image.version"}}' "$version"
expect_image '{{index .Config.Labels "org.opencontainers.image.licenses"}}' AGPL-3.0-only
expect_image '{{index .Config.Labels "org.openlegal.cpu-baseline"}}' "$cpu_baseline"
hardening=(--read-only --cap-drop ALL --security-opt no-new-privileges \
    --memory 2g --cpus 2 --pids-limit 128)
docker run --rm --name "$probe" --platform "$platform" --network none "${hardening[@]}" \
    --tmpfs "/tmp:rw,noexec,nosuid,nodev,size=16m,mode=1777" \
    --env "OPENLEGAL_IMAGE_TARGET=$image_target" --entrypoint /bin/sh "$image" -exc '
    test "$(id -u):$(id -g)" = 10004:10004
    . /etc/os-release
    test "$ID" = alpine
    case "$VERSION_ID" in 3.24|3.24.*) ;; *) exit 1 ;; esac
    for package in gcompat libc6-compat; do
      if apk info -e "$package"; then echo "glibc compatibility package present: $package" >&2; exit 1; fi
    done
    test -s /opt/openlegal/notices/alpine-packages.txt
    apk info -v | LC_ALL=C sort > /tmp/alpine-packages.txt
    cmp /tmp/alpine-packages.txt /opt/openlegal/notices/alpine-packages.txt
    test -x /usr/local/bin/openlegal-server
    test -s /etc/ssl/certs/ca-certificates.crt
    test "$(stat -c %u:%g /usr/local/bin/openlegal-server)" = 0:0
    ldd /usr/local/bin/openlegal-server > /tmp/ldd 2>&1
    cat /tmp/ldd
    grep -q "ld-musl-" /tmp/ldd
    if grep -Eq "not found|Error loading|Error relocating|libc\.so\.6" /tmp/ldd; then
        echo "Unresolved runtime library" >&2; exit 1
    fi
    for tool in cargo rustc cc gcc clang node npm pnpm git; do
        if command -v "$tool"; then
            echo "Build tool present: $tool" >&2; exit 1
        fi
    done
    if test "$OPENLEGAL_IMAGE_TARGET" = runtime-ingestion; then
        test -x /usr/local/bin/kubectl
        test "$(stat -c %u:%g /usr/local/bin/kubectl)" = 0:0
        /usr/local/bin/kubectl version --client -o json > /tmp/kubectl-version
        grep -q '\''"gitVersion": "v1.37.0"'\'' /tmp/kubectl-version
        test -s /opt/openlegal/notices/kubectl/LICENSE
        test -s /opt/openlegal/notices/kubectl/provenance.md
    else
        ! command -v kubectl
    fi
    for widget in index text-diff database; do
        file="/opt/openlegal/widgets/$widget.html"
        test -s "$file"
        test "$(stat -c %u:%g "$file")" = 0:0
        test "$(grep -o __OPENLEGAL_SOURCE_URL__ "$file" | wc -l)" -eq 1
        grep -q openlegal-source-url "$file"
    done
    test -s /opt/openlegal/notices/OPENLEGAL-LICENSE
    test -s /opt/openlegal/notices/WIDGET-NOTICES.md
    test -s /opt/openlegal/notices/dependencies/inventory.json
    test -s /opt/openlegal/notices/dependencies/supplemental-sources.json
    test -s /opt/openlegal/notices/dependencies/rust-standard-library/COPYRIGHT-library.html
    test -n "$(find /opt/openlegal/notices/dependencies -path "*/lindera-ko-dic-*/NOTICE.txt" -type f -print -quit)"
    test -z "$(find /opt/openlegal /usr/local/bin/openlegal-server \( -perm -020 -o -perm -002 \) -print)"
    for directory in / /opt/openlegal /opt/openlegal/widgets /usr/local/bin /etc; do
        if touch "$directory/image-smoke-write" 2>/dev/null; then
            echo "Unexpected write access to $directory" >&2; exit 1
        fi
    done
    touch /tmp/allowed-write
    '
cp scripts/server-image-smoke.mjs "$scratch/fixture/"
# Synthetic certificate for the required WT listener, never production trust.
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -days 2 \
    -subj /CN=localhost -addext subjectAltName=DNS:localhost,IP:127.0.0.1 \
    -keyout "$scratch/fixture/tls/tls.key" -out "$scratch/fixture/tls/tls.crt" >/dev/null 2>&1
printf '%s\n' 'not valid TOML [' > "$scratch/fixture/invalid.toml"
printf '%s\n' 'not a certificate' > "$scratch/fixture/invalid-tls/tls.crt"
cp "$scratch/fixture/tls/tls.key" "$scratch/fixture/invalid-tls/tls.key"
chmod 444 "$scratch/fixture/"*.* "$scratch/fixture/config/server.toml"
docker network create --internal "$network" >/dev/null
docker volume create "$fixture_volume" >/dev/null
tar -c -C "$scratch/fixture" . | docker run --rm -i --name "$initializer" \
    --platform "$native_platform" --network none --user 0:0 \
    --mount "type=volume,source=$fixture_volume,target=/fixture" \
    --entrypoint tar "$node_image" -x -C /fixture
# Capture the exact packaged widget for client-side response comparison.
docker run --rm --name "$initializer" --platform "$platform" --network none --user 0:0 \
    --mount "type=volume,source=$fixture_volume,target=/fixture" \
    --entrypoint /bin/sh "$image" -ec '
    cp /opt/openlegal/widgets/text-diff.html /fixture/text-diff.html
    chmod 444 /fixture/text-diff.html
    chown -R 0:10004 /fixture/tls /fixture/invalid-tls
    chmod 750 /fixture/tls /fixture/invalid-tls
    chmod 440 /fixture/tls/* /fixture/invalid-tls/*
    '
fixture=(--mount "type=volume,source=$fixture_volume,target=/fixture,readonly")
config_mount=(--mount "type=volume,source=$fixture_volume,target=/etc/openlegal,volume-subpath=config,readonly")
tls_mount=(--mount "type=volume,source=$fixture_volume,target=/run/secrets/backend-tls,volume-subpath=tls,readonly")
expect_startup_failure() {
    local label=$1
    shift
    docker run -d --name "$invalid" --platform "$platform" --network none \
        "${hardening[@]}" "$@" >/dev/null
    for _attempt in {1..20}; do
        [[ $(docker inspect --format '{{.State.Running}}' "$invalid") == false ]] && break
        sleep 1
    done
    [[ $(docker inspect --format '{{.State.Running}}' "$invalid") == false ]] || { echo "$label did not exit" >&2; exit 1; }
    local invalid_code
    invalid_code=$(docker inspect --format '{{.State.ExitCode}}' "$invalid")
    [[ $invalid_code == 1 ]] || { echo "$label exited $invalid_code, expected 1" >&2; exit 1; }
    [[ $(docker inspect --format '{{.State.OOMKilled}}' "$invalid") == false ]]
    docker rm "$invalid" >/dev/null
}
expect_startup_failure 'Missing configuration' "$image"
expect_startup_failure 'Malformed configuration' "${fixture[@]}" "$image" /fixture/invalid.toml
expect_startup_failure 'Missing TLS' "${config_mount[@]}" "$image"
expect_startup_failure 'Invalid TLS' "${config_mount[@]}" \
    --mount "type=volume,source=$fixture_volume,target=/run/secrets/backend-tls,volume-subpath=invalid-tls,readonly" "$image"
# Use the default entrypoint/arguments and exact committed mount paths; no /tmp.
docker run -d --name "$server" --platform "$platform" --network "$network" --network-alias server \
    "${hardening[@]}" "${config_mount[@]}" "${tls_mount[@]}" "$image" >/dev/null
docker exec "$server" /bin/sh -ec '
    test "$(id -u):$(id -g)" = 10004:10004
    grep -Eq "^CapEff:[[:space:]]+0+$" /proc/1/status
    grep -Eq "^NoNewPrivs:[[:space:]]+1$" /proc/1/status
    grep -Eq "^Seccomp:[[:space:]]+2$" /proc/1/status
    test -r /run/secrets/backend-tls/tls.key
    test ! -e /var/run/secrets/kubernetes.io/serviceaccount/token
    for directory in /tmp /etc/openlegal /run/secrets/backend-tls; do
        if touch "$directory/image-smoke-write" 2>/dev/null; then
            echo "Unexpected write access to $directory" >&2; exit 1
        fi
    done
    '
docker run --rm --name "$client" --platform "$native_platform" --network "$network" \
    --user 10004:10004 "${hardening[@]}" "${fixture[@]}" \
    --env "EXPECTED_SERVER_VERSION=${release_version:-0.0.0}" \
    --entrypoint node "$node_image" /fixture/server-image-smoke.mjs
shutdown_started=$SECONDS
docker stop --signal SIGTERM --timeout 30 "$server" >/dev/null
(( SECONDS - shutdown_started < 30 )) || { echo 'Server exceeded termination grace' >&2; exit 1; }
[[ $(docker inspect --format '{{.State.ExitCode}}' "$server") == 0 ]] || { echo 'Server failed graceful SIGTERM exit' >&2; exit 1; }
[[ $(docker inspect --format '{{.State.OOMKilled}}' "$server") == false ]]
OPENLEGAL_EXPECTED_SERVER_VERSION="${release_version:-0.0.0}" \
    scripts/test-retained-server-image.sh --image "$image" --platform "$platform"
if [[ $image_target == runtime-ingestion ]]; then
    scripts/test-ingestion-controller-image.sh --image "$image" --platform "$platform"
fi
printf 'Production image acceptance passed for %s (%s, %s).\n' "$platform" "$cpu_baseline" "$image_target"
