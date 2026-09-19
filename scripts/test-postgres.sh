#!/usr/bin/env bash
# Explicit PostgreSQL 18 integration gate. Ordinary cargo tests require no database.
set -euo pipefail
repo=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
postgres_image=postgres@sha256:ae6c78831cbc35fa3a4aaf4d763ddacf6183d6004774cc2dc28b3920410d1d1a
container="openlegal-postgres-test-$$"
unsupported_container="${container}-unsupported"
unsupported_image=postgres@sha256:67f41722b7a8cbdb868a44a4995c846eddfdc2973bccb291ce937dce88ad5675
network="openlegal-postgres-network-$$"
runner_container=""
dictionary_fixture=""
cleanup() {
    status=$?
    if [[ -n $dictionary_fixture ]]; then rm -rf -- "$dictionary_fixture"; fi
    docker rm -fv "$container" "$unsupported_container" >/dev/null 2>&1 || true
    if [[ -n $runner_container ]]; then docker network disconnect "$network" "$runner_container" >/dev/null 2>&1 || true; fi
    docker network rm "$network" >/dev/null 2>&1 || true
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
for command in docker cargo python3 timeout openssl; do
    command -v "$command" >/dev/null || { echo "Missing command: $command" >&2; exit 1; }
done
# Corpus integration tests use the complete explicitly provisioned dictionary.
# Keep the parent directory disposable; provisioning requires a fresh destination.
if [[ -z ${OPENLEGAL_TEST_MECAB_DICTIONARY:-} ]]; then
    dictionary_fixture=$(mktemp -d)
    "$repo/scripts/prepare-korean-dictionary.sh" "$dictionary_fixture/dictionary"
    export OPENLEGAL_TEST_MECAB_DICTIONARY="$dictionary_fixture/dictionary"
fi
network_options=()
publish=(-p 127.0.0.1::5432)
# A host-rootless daemon's published localhost is not the devcontainer's localhost.
# Join only this disposable internal network; retain the existing default route.
if [[ -e /.dockerenv ]]; then
    candidate=$(hostname)
    if ! docker inspect --type container "$candidate" >/dev/null 2>&1; then
        echo 'The development container must be visible to the configured Docker daemon.' >&2
        exit 1
    fi
    network_options=(--internal)
    publish=()
fi
docker network create "${network_options[@]}" "$network" >/dev/null
if [[ ${#publish[@]} == 0 ]]; then
    docker network connect "$network" "$candidate"
    runner_container=$candidate
fi

# Inspect only endpoint metadata: a full inspection would include the password.
database_endpoint() {
    local database=$1 endpoint
    if [[ -n $runner_container ]]; then
        if ! endpoint=$(docker inspect --format "{{with index .NetworkSettings.Networks \"$network\"}}{{.IPAddress}}{{end}}" "$database" 2>/dev/null); then
            echo "Cannot inspect database endpoint for $database" >&2
            return 1
        fi
        if ! python3 -c 'import ipaddress, sys; ipaddress.IPv4Address(sys.argv[1])' "$endpoint" 2>/dev/null; then
            echo "Missing or invalid database address on the test network for $database" >&2
            return 1
        fi
        printf '%s:5432\n' "$endpoint"
    else
        if ! endpoint=$(docker port "$database" 5432/tcp 2>/dev/null); then
            echo "Cannot discover published database port for $database" >&2
            return 1
        fi
        if [[ ! $endpoint =~ ^127\.0\.0\.1:([0-9]{1,5})$ ]] ||
            (( 10#${BASH_REMATCH[1]} < 1 || 10#${BASH_REMATCH[1]} > 65535 )); then
            echo "Missing or invalid localhost database port for $database" >&2
            return 1
        fi
        printf '%s\n' "$endpoint"
    fi
}
password=$(python3 -c 'import secrets; print(secrets.token_hex(24))')
docker run -d --name "$container" --memory 512m --cpus 2 \
    --network "$network" "${publish[@]}" -e POSTGRES_PASSWORD="$password" \
    "$postgres_image" -c log_statement=none -c log_min_error_statement=panic >/dev/null
for attempt in {1..60}; do
    if docker exec "$container" pg_isready -U postgres -d postgres >/dev/null 2>&1; then break; fi
    if (( attempt == 60 )); then echo 'PostgreSQL 18 did not become ready within 60 seconds' >&2; exit 1; fi
    sleep 1
done
endpoint=$(database_endpoint "$container")
docker run -d --name "$unsupported_container" --memory 512m --cpus 2 \
    --network "$network" "${publish[@]}" -e POSTGRES_PASSWORD="$password" \
    "$unsupported_image" -c log_statement=none -c log_min_error_statement=panic >/dev/null
for attempt in {1..60}; do
    if docker exec "$unsupported_container" pg_isready -U postgres -d postgres >/dev/null 2>&1; then break; fi
    if (( attempt == 60 )); then echo 'Unsupported-version test database did not become ready' >&2; exit 1; fi
    sleep 1
done
unsupported_endpoint=$(database_endpoint "$unsupported_container")
export OPENLEGAL_TEST_UNSUPPORTED_DATABASE_URL="postgresql://postgres:$password@$unsupported_endpoint/postgres"
export OPENLEGAL_TEST_POSTGRES_CONTAINER="$container"
export OPENLEGAL_TEST_DATABASE_URL="postgresql://postgres:$password@$endpoint/postgres"
cd "$repo"
# All ignored tests are explicitly invoked here, never silently skipped for missing fixtures.
if (( $# )); then
    cargo test --locked "$@" -- --ignored --test-threads=1
else
    cargo test --workspace --locked -- --ignored --test-threads=1
fi
printf '%s\n' 'PostgreSQL 18 integration checks passed.'
