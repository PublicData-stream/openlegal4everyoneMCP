#!/usr/bin/env bash
# Disposable retained-corpus image acceptance, with no published ports or providers.
# Invoked with the already-built image by test-server-image.sh. Cargo dictionary
# provisioning uses the caller's configured toolchain/cache; provision beforehand
# or run this gate through the privileged command channel in managed workspaces.
set -euo pipefail
repo=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
if [[ $# != 4 || $1 != --image || $3 != --platform ]]; then
    echo "Usage: $0 --image IMAGE --platform linux/amd64|linux/arm64" >&2; exit 2
fi
image=$2
platform=$4
case $platform in linux/amd64|linux/arm64) ;; *) exit 2 ;; esac
for command in docker openssl tar python3 cargo; do
    command -v "$command" >/dev/null || { echo "Missing command: $command" >&2; exit 1; }
done
case $(docker info --format '{{.Architecture}}') in
    x86_64|amd64) native_platform=linux/amd64 ;;
    aarch64|arm64) native_platform=linux/arm64 ;;
    *) echo 'Unsupported host architecture' >&2; exit 1 ;;
esac
scratch=$(mktemp -d)
run_id="openlegal-retained-$(basename "$scratch" | tr '[:upper:]' '[:lower:]')-$$"
network=$run_id
server=$run_id-server
postgres=$run_id-postgres
client=$run_id-client
initializer=$run_id-initialize
migration=$run_id-migrate
maintenance=$run_id-maintain
interrupted=$run_id-interrupted
rebuild=$run_id-rebuild
seed=$run_id-seed
fixture=$run_id-fixture
helper_image=$run_id-helper:local
volumes=("$fixture" "$run_id-cache" "$run_id-corpus" "$run_id-index" "$run_id-dictionary" "$run_id-interrupted-index" "$run_id-rebuilt-index")
node_image=node:24.21.0-trixie-slim@sha256:8ec5d7557396cfe32d21c3f9c13072355ceab22b584578ca4bb28af31120cffe
postgres_image=postgres@sha256:ae6c78831cbc35fa3a4aaf4d763ddacf6183d6004774cc2dc28b3920410d1d1a
cleanup() {
    status=$?
    if (( status != 0 )); then
        # Never inspect Config.Env, SQL input, or print raw database/driver logs.
        docker inspect --format '{{.Name}} exit={{.State.ExitCode}} oom={{.State.OOMKilled}} running={{.State.Running}}' \
            "$server" "$migration" "$maintenance" "$interrupted" "$rebuild" "$seed" "$postgres" >&2 2>/dev/null || true
    fi
    docker rm -fv "$server" "$client" "$initializer" "$migration" "$maintenance" "$interrupted" "$rebuild" "$seed" "$postgres" >/dev/null 2>&1 || true
    docker network rm "$network" >/dev/null 2>&1 || true
    docker volume rm "${volumes[@]}" >/dev/null 2>&1 || true
    docker image rm "$helper_image" >/dev/null 2>&1 || true
    rm -rf -- "$scratch"
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
cd "$repo"
if [[ -z ${OPENLEGAL_TEST_MECAB_DICTIONARY:-} ]]; then
    scripts/prepare-korean-dictionary.sh "$scratch/dictionary"
    dictionary=$scratch/dictionary
else
    dictionary=$(realpath -- "$OPENLEGAL_TEST_MECAB_DICTIONARY")
fi
[[ -s $dictionary/manifest.json ]] || { echo 'Missing provisioned dictionary manifest' >&2; exit 1; }
# The helper has a compatible libc and the daemon's native architecture, including
# when the production image is tested through emulation. Reuse release build caches.
docker build --platform "$native_platform" --file test-support/retained-image/Dockerfile --tag "$helper_image" .
docker pull --platform "$native_platform" "$node_image" >/dev/null
docker pull --platform "$native_platform" "$postgres_image" >/dev/null
mkdir -p "$scratch/fixture/config" "$scratch/fixture/tls" "$scratch/fixture/postgres-ca" "$scratch/fixture/bad-ca" "$scratch/postgres-tls"
scripts/test-kubernetes-serving.sh --profile retained --config-output "$scratch/fixture/config/server.toml" \
    --admin-output-dir "$scratch/admin"
deploy_tools=${OPENLEGAL_DEPLOY_TOOLS:-$repo/target/deployment-tools}
"$deploy_tools/bin/python" - "$scratch/admin" <<'PYTHON'
import pathlib, sys, yaml
root = pathlib.Path(sys.argv[1])
for operation in ('migrate', 'maintain', 'rebuild'):
    docs = list(yaml.safe_load_all((root / f'{operation}.yaml').read_text()))
    job, = [doc for doc in docs if doc and doc['kind'] == 'Job']
    container, = job['spec']['template']['spec']['containers']
    (root / f'{operation}.args').write_text('\n'.join(container['args']) + '\n')
PYTHON
mapfile -t migrate_args < "$scratch/admin/migrate.args"
mapfile -t maintain_args < "$scratch/admin/maintain.args"
mapfile -t rebuild_args < "$scratch/admin/rebuild.args"
OPENLEGAL_RENDERED_CONFIG="$scratch/fixture/config/server.toml" \
    cargo test --locked -p openlegal-server --test deployment_config
cp scripts/server-image-smoke.mjs "$scratch/fixture/"
python3 - "$scratch/fixture" <<'PY'
import json, pathlib, sys, tomllib
root = pathlib.Path(sys.argv[1])
c = tomllib.loads((root / 'config/server.toml').read_text())
(root / 'client.json').write_text(json.dumps({'source': c['source']['url'], 'authority': c['http']['allowed_hosts'][0], 'retained': True}))
PY
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -days 2 \
    -subj /CN=localhost -addext subjectAltName=DNS:localhost \
    -keyout "$scratch/fixture/tls/tls.key" -out "$scratch/fixture/tls/tls.crt" >/dev/null 2>&1
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -days 2 \
    -subj /CN=retained-image-fixture-ca -addext basicConstraints=critical,CA:TRUE \
    -addext keyUsage=critical,keyCertSign,cRLSign \
    -keyout "$scratch/postgres-ca.key" -out "$scratch/fixture/postgres-ca/ca.crt" >/dev/null 2>&1
openssl req -new -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
    -subj /CN=postgres -keyout "$scratch/postgres-tls/server.key" -out "$scratch/postgres.csr" >/dev/null 2>&1
printf '%s\n' 'subjectAltName=DNS:postgres' 'basicConstraints=critical,CA:FALSE' \
    'keyUsage=critical,digitalSignature' 'extendedKeyUsage=serverAuth' > "$scratch/postgres.ext"
openssl x509 -req -days 2 -in "$scratch/postgres.csr" -CA "$scratch/fixture/postgres-ca/ca.crt" \
    -CAkey "$scratch/postgres-ca.key" -set_serial 1 -extfile "$scratch/postgres.ext" \
    -out "$scratch/postgres-tls/server.crt" >/dev/null 2>&1
openssl verify -purpose sslserver -verify_hostname postgres -CAfile "$scratch/fixture/postgres-ca/ca.crt" \
    "$scratch/postgres-tls/server.crt" >/dev/null
# A valid but unrelated CA ensures failure is trust verification, not PEM parsing.
cp "$scratch/fixture/tls/tls.crt" "$scratch/fixture/bad-ca/ca.crt"
python3 - "$scratch" <<'PY'
import pathlib, secrets, sys
root = pathlib.Path(sys.argv[1])
passwords = {role: secrets.token_hex(24) for role in ('postgres', 'migration', 'runtime')}
(root / 'postgres.env').write_text('POSTGRES_PASSWORD=' + passwords['postgres'] + '\n')
(root / 'probe.env').write_text('PGHOST=postgres\nPGDATABASE=openlegal\nPGUSER=migration\n'
    + 'PGPASSWORD=' + passwords['migration'] + '\nPGSSLMODE=verify-full\n'
    + 'PGSSLROOTCERT=/run/secrets/postgres-ca/ca.crt\nPGCONNECT_TIMEOUT=2\n')
for role, variable in [('runtime', 'OPENLEGAL_DATABASE_URL'), ('migration', 'OPENLEGAL_MIGRATION_DATABASE_URL')]:
    (root / (role + '.env')).write_text(f'{variable}=postgresql://{role}:{passwords[role]}@postgres:5432/openlegal\n')
(root / 'roles.sql').write_text(f"CREATE ROLE migration LOGIN PASSWORD '{passwords['migration']}';\nCREATE ROLE runtime LOGIN PASSWORD '{passwords['runtime']}';\nCREATE DATABASE openlegal OWNER migration;\n")
for name in ('postgres.env', 'runtime.env', 'migration.env', 'probe.env', 'roles.sql'):
    (root / name).chmod(0o600)
PY
docker network create --internal "$network" >/dev/null
for volume in "${volumes[@]}"; do docker volume create "$volume" >/dev/null; done
cache_storage=(--mount "type=volume,source=$run_id-cache,target=/var/lib/openlegal/cache-blobs")
select_index() {
    storage=("${cache_storage[@]}" \
        --mount "type=volume,source=$run_id-corpus,target=/var/lib/openlegal/corpus-blobs" \
        --mount "type=volume,source=$1,target=/var/lib/openlegal/corpus-index")
}
select_index "$run_id-index"
fixture_rw=(--mount "type=volume,source=$fixture,target=/fixture")
fixture_ro=(--mount "type=volume,source=$fixture,target=/fixture,readonly")
dictionary_rw=(--mount "type=volume,source=$run_id-dictionary,target=/var/lib/openlegal/mecab-ko-dictionary")
dictionary_ro=(--mount "type=volume,source=$run_id-dictionary,target=/var/lib/openlegal/mecab-ko-dictionary,readonly")
# Stream fixtures to the daemon instead of relying on host bind path visibility.
tar -c -C "$scratch/fixture" . | docker run --rm -i --name "$initializer" --platform "$native_platform" \
    --network none --user 0:0 "${fixture_rw[@]}" --entrypoint tar "$node_image" -x -C /fixture
# Preserve the volume-root ownership/mode that OnRootMismatch requires on cluster.
docker run --rm --name "$initializer" --platform "$native_platform" --network none --user 0:0 \
    "${storage[@]}" "${dictionary_rw[@]}" "${fixture_rw[@]}" --entrypoint sh "$node_image" -ec '
    for name in cache-blobs corpus-blobs corpus-index mecab-ko-dictionary; do
        root=/var/lib/openlegal/$name
        chown 0:10004 "$root"; chmod 2770 "$root"
        # GNU chmod preserves inherited directory setgid with a three/four-digit
        # mode. Five digits explicitly clear it, matching operator install -m.
        mkdir "$root/data"; chown 10004:10004 "$root/data"; chmod 00700 "$root/data"
    done
    chown -R 0:10004 /fixture/tls /fixture/postgres-ca /fixture/bad-ca
    chmod 750 /fixture/tls /fixture/postgres-ca /fixture/bad-ca
    chmod 440 /fixture/tls/* /fixture/postgres-ca/* /fixture/bad-ca/*
    '
# Fresh rebuild destinations retain the same volume-root/private-data contract.
for volume in "$run_id-interrupted-index" "$run_id-rebuilt-index"; do
    docker run --rm --name "$initializer" --platform "$native_platform" --network none --user 0:0 \
        --mount "type=volume,source=$volume,target=/fresh" --entrypoint sh "$node_image" -ec '
        chown 0:10004 /fresh; chmod 2770 /fresh
        mkdir /fresh/data; chown 10004:10004 /fresh/data; chmod 00700 /fresh/data
        '
done
tar -c -C "$dictionary" . | docker run --rm -i --name "$initializer" --platform "$native_platform" \
    --network none --user 0:0 "${dictionary_rw[@]}" --entrypoint tar "$node_image" -x -C /var/lib/openlegal/mecab-ko-dictionary/data
docker run --rm --name "$initializer" --platform "$native_platform" --network none --user 0:0 \
    "${dictionary_rw[@]}" --entrypoint sh "$node_image" -ec '
    chown -R 0:10004 /var/lib/openlegal/mecab-ko-dictionary/data
    find /var/lib/openlegal/mecab-ko-dictionary/data -type d -exec chmod 550 {} +
    find /var/lib/openlegal/mecab-ko-dictionary/data -type f -exec chmod 440 {} +
    '
docker run --rm --name "$initializer" --platform "$platform" --network none --user 0:0 \
    "${fixture_rw[@]}" --entrypoint sh "$image" -ec '
    cp /opt/openlegal/widgets/text-diff.html /opt/openlegal/widgets/database.html /fixture/
    chmod 444 /fixture/*.html
    '
# Bootstrap SQL only uses a local socket; all application connections require TLS.
docker run -d --name "$postgres" --platform "$native_platform" --network "$network" --network-alias postgres \
    --memory 512m --cpus 2 --env-file "$scratch/postgres.env" "$postgres_image" \
    -c log_statement=none -c log_min_error_statement=panic >/dev/null
for attempt in {1..60}; do
    # The entrypoint's temporary initialization server only accepts Unix sockets.
    if docker exec "$postgres" pg_isready -h 127.0.0.1 -U postgres -d postgres >/dev/null 2>&1; then break; fi
    (( attempt < 60 )) || { echo 'PostgreSQL startup timed out' >&2; exit 1; }
    sleep 1
done
tar -c -C "$scratch/postgres-tls" . | docker exec -i --user 0 "$postgres" sh -ec '
    mkdir /postgres-tls; tar -x -C /postgres-tls
    chown -R postgres:postgres /postgres-tls; chmod 700 /postgres-tls; chmod 600 /postgres-tls/*
    '
docker exec -i "$postgres" psql -U postgres -v ON_ERROR_STOP=1 >/dev/null < "$scratch/roles.sql"
printf '%s\n' "ALTER SYSTEM SET ssl = 'on';" "ALTER SYSTEM SET ssl_cert_file = '/postgres-tls/server.crt';" \
    "ALTER SYSTEM SET ssl_key_file = '/postgres-tls/server.key';" 'SELECT pg_reload_conf();' | \
    docker exec -i "$postgres" psql -U postgres -v ON_ERROR_STOP=1 >/dev/null
# Require encryption on every TCP connection; health and bootstrap use sockets.
docker exec "$postgres" sh -ec 'printf "local all all trust\nhostssl all all all scram-sha-256\nhostnossl all all all reject\n" > "$PGDATA/pg_hba.conf"'
docker exec "$postgres" psql -U postgres -v ON_ERROR_STOP=1 -c 'SELECT pg_reload_conf()' >/dev/null
hardening=(--read-only --cap-drop ALL --security-opt no-new-privileges --memory 4g --cpus 2 --pids-limit 128)
config=(--mount "type=volume,source=$fixture,target=/etc/openlegal,volume-subpath=config,readonly")
tls=(--mount "type=volume,source=$fixture,target=/run/secrets/backend-tls,volume-subpath=tls,readonly")
ca=(--mount "type=volume,source=$fixture,target=/run/secrets/postgres-ca,volume-subpath=postgres-ca,readonly")
# Diagnostics are mapped to a fixed allowlist, never echoed from driver output.
report_failure_category() {
    python3 - "$1" "$2" <<'PY'
import pathlib, sys
label, path = sys.argv[1:]
text = pathlib.Path(path).read_text(errors='replace')
categories = [
    ('Error: SchemaMismatch', 'schema-mismatch'),
    ('Error: PostgreSQL18Required', 'postgres-version'),
    ('Error: Storage(StorageUnavailable)', 'database-unavailable'),
    ('Error: Storage(Busy)', 'database-busy'),
    ('Error: StorageCorrupt', 'storage-integrity'),
    ('Error: StorageUnavailable', 'storage-unavailable'),
    ('Error: "retained image fixture seeding failed"', 'fixture-publication'),
    ('certificate verify failed', 'tls-verification'),
    ('password authentication failed', 'database-authentication'),
    ('server does not support SSL', 'tls-unavailable'),
    ('Connection refused', 'connection-refused'),
]
category = next((category for pattern, category in categories if pattern in text), 'unexpected-private-diagnostic')
print(f'{label} failed: {category}', file=sys.stderr)
PY
}
# pg_reload_conf is asynchronous. Require an actual verify-full TCP connection
# with the migration role before invoking the Rust migration command.
for attempt in {1..10}; do
    if docker run --rm --name "$client" --platform "$native_platform" --network "$network" \
        --user 10004:10004 "${hardening[@]}" "${ca[@]}" --env-file "$scratch/probe.env" \
        --entrypoint timeout "$postgres_image" --kill-after=1s 5s \
        psql -X -v ON_ERROR_STOP=1 -Atc 'SELECT 1' \
        > "$scratch/probe.log" 2>&1; then
        break
    fi
    if (( attempt == 10 )); then report_failure_category 'Verified PostgreSQL readiness' "$scratch/probe.log"; exit 1; fi
    sleep 1
done
# Migration receives its distinct credential and no writable storage or runtime URL.
if ! docker run --name "$migration" --platform "$platform" --network "$network" "${hardening[@]}" \
    --memory 512m --cpus 1 "${config[@]}" "${ca[@]}" --env-file "$scratch/migration.env" "$image" \
    "${migrate_args[@]}" > "$scratch/migrate.log" 2>&1; then
    report_failure_category Migration "$scratch/migrate.log"; exit 1
fi
printf '%s\n' 'REVOKE CREATE ON SCHEMA public FROM PUBLIC;' 'GRANT USAGE ON SCHEMA openlegal TO runtime;' \
    'GRANT SELECT ON public._sqlx_migrations TO runtime;' \
    'GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA openlegal TO runtime;' | \
    docker exec -i "$postgres" psql -U postgres -d openlegal -v ON_ERROR_STOP=1 >/dev/null
# Publication uses the runtime role; the helper releases every lease before serve.
if ! docker run --name "$seed" --platform "$native_platform" --network "$network" "${hardening[@]}" \
    "${storage[@]}" "${ca[@]}" --env-file "$scratch/runtime.env" "$helper_image" \
    /var/lib/openlegal/cache-blobs/data /var/lib/openlegal/corpus-blobs/data /run/secrets/postgres-ca/ca.crt \
    > "$scratch/fixture/expected.json" 2> "$scratch/seed.log"; then
    report_failure_category Publication "$scratch/seed.log"; exit 1
fi
tar -c -C "$scratch/fixture" expected.json | docker run --rm -i --name "$initializer" --platform "$native_platform" \
    --network none --user 0:0 "${fixture_rw[@]}" --entrypoint tar "$node_image" -x -C /fixture
fixture_sql() {
    docker exec "$postgres" psql -X -U postgres -d openlegal -v ON_ERROR_STOP=1 -Atc "$1"
}
[[ $(fixture_sql 'SELECT count(*) FROM openlegal.cache_snapshot') == 1 ]]
# Maintenance must succeed with only cache blobs; corpus/index/dictionary, backend
# TLS and migration credentials are absent even though retained config names them.
if ! docker run --name "$maintenance" --platform "$platform" --network "$network" "${hardening[@]}" \
    --memory 512m --cpus 1 "${config[@]}" "${ca[@]}" "${cache_storage[@]}" \
    --env-file "$scratch/runtime.env" "$image" "${maintain_args[@]}" > "$scratch/maintain.log" 2>&1; then
    report_failure_category Maintenance "$scratch/maintain.log"; exit 1
fi
[[ $(fixture_sql 'SELECT count(*) FROM openlegal.cache_snapshot') == 0 ]]
[[ $(fixture_sql 'SELECT snapshots FROM openlegal.cache_storage WHERE singleton') == 0 ]]
[[ $(fixture_sql 'SELECT count(*) FROM openlegal.corpus_capture') == 2 ]]
start_server() {
    docker run -d --name "$server" --platform "$platform" --network "$network" --network-alias server \
        "${hardening[@]}" "${config[@]}" "${tls[@]}" "${ca[@]}" "${storage[@]}" "${dictionary_ro[@]}" \
        "$@" "$image" >/dev/null
}
stop_server() {
    local start=$SECONDS
    docker stop --signal SIGTERM --timeout 30 "$server" >/dev/null
    (( SECONDS - start < 30 )) || { echo 'Retained server exceeded shutdown grace' >&2; exit 1; }
    [[ $(docker inspect --format '{{.State.ExitCode}}:{{.State.OOMKilled}}' "$server") == 0:false ]]
    docker rm "$server" >/dev/null
}
check_storage() {
    docker run --rm --name "$initializer" --platform "$native_platform" --network none --user 0:0 \
        "${storage[@]}" --entrypoint sh "$node_image" -ec '
        for name in cache-blobs corpus-blobs corpus-index; do
            root=/var/lib/openlegal/$name
            actual=$(stat -c %u:%g:%a "$root")
            test "$actual" = 0:10004:2770 || { echo "$name volume ownership/mode mismatch: $actual" >&2; exit 1; }
            actual=$(stat -c %u:%g:%a "$root/data")
            test "$actual" = 10004:10004:700 || { echo "$name private root ownership/mode mismatch: $actual" >&2; exit 1; }
        done
        for name in cache-blobs corpus-blobs; do
            test -z "$(find /var/lib/openlegal/$name/data -type f ! -perm 0600 -print -quit)"
            test -z "$(find /var/lib/openlegal/$name/data ! -uid 10004 -print -quit)"
        done
        test -n "$(find /var/lib/openlegal/corpus-blobs/data -type f -print -quit)"
        '
}
acceptance_cycle() {
    local cycle=$1
    start_server --env-file "$scratch/runtime.env"
    docker run --rm --name "$client" --platform "$native_platform" --network "$network" \
        --user 10004:10004 "${hardening[@]}" "${fixture_ro[@]}" \
        --env "EXPECTED_SERVER_VERSION=${OPENLEGAL_EXPECTED_SERVER_VERSION:-0.0.0}" \
        --entrypoint node "$node_image" /fixture/server-image-smoke.mjs
    docker exec "$server" sh -ec '
        test "$(id -u):$(id -g)" = 10004:10004
        test -z "${OPENLEGAL_MIGRATION_DATABASE_URL+x}"
        grep -Eq "^CapEff:[[:space:]]+0+$" /proc/1/status
        grep -Eq "^NoNewPrivs:[[:space:]]+1$" /proc/1/status
        grep -Eq "^Seccomp:[[:space:]]+2$" /proc/1/status
        for root in /tmp /etc/openlegal /run/secrets/backend-tls /run/secrets/postgres-ca /var/lib/openlegal/mecab-ko-dictionary /var/lib/openlegal/mecab-ko-dictionary/data; do
            if touch "$root/forbidden-write" 2>/dev/null; then exit 1; fi
        done
        '
    printf 'Retained %s resource sample: ' "$cycle"
    docker stats --no-stream --format 'memory={{.MemUsage}} cpu={{.CPUPerc}}' "$server"
    docker exec "$server" sh -ec 'if test -r /sys/fs/cgroup/memory.peak; then printf "cgroup peak bytes: "; cat /sys/fs/cgroup/memory.peak; fi'
    stop_server
    check_storage
}
acceptance_cycle first
acceptance_cycle restart
expect_failure() {
    local label=$1 expected_error=$2
    shift 2
    start_server "$@"
    for attempt in {1..300}; do
        [[ $(docker inspect --format '{{.State.Running}}' "$server") == false ]] && break
        (( attempt < 300 )) || { echo "$label timed out; not an accepted failure" >&2; exit 1; }
        sleep 1
    done
    [[ $(docker inspect --format '{{.State.ExitCode}}:{{.State.OOMKilled}}' "$server") == 1:false ]] || {
        echo "$label failed abnormally (signal/OOM); not accepted" >&2; exit 1;
    }
    # Capture privately and compare only a fixed sanitized terminal category.
    # Never print logs on failure: upstream diagnostics may contain credentials.
    docker logs "$server" > "$scratch/negative.log" 2>&1
    chmod 600 "$scratch/negative.log"
    if ! grep -Fxq -- "$expected_error" "$scratch/negative.log"; then
        echo "$label returned an unexpected sanitized error category" >&2
        # Only these fixed, credential-free categories may leave the scratch log.
        for category in 'Error: Storage(StorageUnavailable)' 'Error: Storage(Busy)' \
            'Error: StorageCorrupt' 'Error: StorageUnavailable'; do
            if grep -Fxq -- "$category" "$scratch/negative.log"; then
                printf 'Observed category: %s\n' "$category" >&2
            fi
        done
        exit 1
    fi
    rm "$scratch/negative.log"
    docker rm "$server" >/dev/null
    printf 'Expected startup rejection: %s\n' "$label"
}
# Pause replay at a real database lock, after create_rebuild has committed its
# incomplete metadata. This fixture changes no production code and does not rely
# on a large corpus or a guessed sleep to interrupt the correct phase.
old_index_digest=$(docker run --rm --name "$initializer" --platform "$native_platform" --network none \
    --user 10004:10004 --mount "type=volume,source=$run_id-index,target=/old,readonly" \
    --entrypoint sha256sum "$node_image" /old/data/meta.json)
ack_before=$(fixture_sql 'SELECT index_ack FROM openlegal.corpus_control')
docker exec -d --env PGAPPNAME=openlegal-rebuild-barrier "$postgres" \
    psql -X -U postgres -d openlegal -v ON_ERROR_STOP=1 -c \
    'BEGIN; LOCK TABLE openlegal.corpus_outbox IN ACCESS EXCLUSIVE MODE; SELECT pg_sleep(600); ROLLBACK;'
for attempt in {1..100}; do
    [[ $(fixture_sql "SELECT count(*) FROM pg_locks l JOIN pg_stat_activity a ON a.pid=l.pid WHERE a.application_name='openlegal-rebuild-barrier' AND l.relation='openlegal.corpus_outbox'::regclass AND l.granted") == 1 ]] && break
    (( attempt < 100 )) || { echo 'Rebuild barrier was not acquired' >&2; exit 1; }
    sleep 0.1
done
select_index "$run_id-interrupted-index"
docker run -d --name "$interrupted" --platform "$platform" --network "$network" "${hardening[@]}" \
    "${config[@]}" "${ca[@]}" "${storage[@]}" "${dictionary_ro[@]}" \
    --env-file "$scratch/runtime.env" "$image" "${rebuild_args[@]}" >/dev/null
for attempt in {1..3000}; do
    if [[ $(fixture_sql "SELECT count(*) FROM pg_locks l JOIN pg_stat_activity a ON a.pid=l.pid WHERE a.usename='runtime' AND l.relation='openlegal.corpus_outbox'::regclass AND NOT l.granted") == 1 ]]; then
        docker kill --signal SIGKILL "$interrupted" >/dev/null
        break
    fi
    [[ $(docker inspect --format '{{.State.Running}}' "$interrupted") == true ]] || {
        echo 'Rebuild exited before controlled interruption' >&2; exit 1;
    }
    (( attempt < 3000 )) || { echo 'Rebuild never reached replay barrier' >&2; exit 1; }
    sleep 0.1
done
docker wait "$interrupted" >/dev/null
[[ $(docker inspect --format '{{.State.ExitCode}}:{{.State.OOMKilled}}' "$interrupted") == 137:false ]]
fixture_sql "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name='openlegal-rebuild-barrier'" >/dev/null
[[ $(fixture_sql 'SELECT index_ack FROM openlegal.corpus_control') == "$ack_before" ]]
expect_failure 'interrupted incomplete index' 'Error: StorageCorrupt' --env-file "$scratch/runtime.env"
select_index "$run_id-rebuilt-index"
if ! docker run --name "$rebuild" --platform "$platform" --network "$network" "${hardening[@]}" \
    "${config[@]}" "${ca[@]}" "${storage[@]}" "${dictionary_ro[@]}" \
    --env-file "$scratch/runtime.env" "$image" "${rebuild_args[@]}" > "$scratch/rebuild.log" 2>&1; then
    report_failure_category Rebuild "$scratch/rebuild.log"; exit 1
fi
[[ $(fixture_sql 'SELECT index_ack = next_event - 1 FROM openlegal.corpus_control') == t ]]
[[ $(docker run --rm --name "$initializer" --platform "$native_platform" --network none \
    --user 10004:10004 --mount "type=volume,source=$run_id-index,target=/old,readonly" \
    --entrypoint sha256sum "$node_image" /old/data/meta.json) == "$old_index_digest" ]]
# Serving now consumes the fresh completed destination, with identical capture
# and query assertions. The previous index remains preserved on its own volume.
acceptance_cycle rebuilt
expect_failure 'missing runtime credential' 'Error: "required PostgreSQL connection environment value is missing or invalid"'
ca=(--mount "type=volume,source=$fixture,target=/run/secrets/postgres-ca,volume-subpath=bad-ca,readonly")
expect_failure 'untrusted PostgreSQL certificate' 'Error: Storage(StorageUnavailable)' --env-file "$scratch/runtime.env"
ca=(--mount "type=volume,source=$fixture,target=/run/secrets/postgres-ca,volume-subpath=postgres-ca,readonly")
docker run --rm --name "$initializer" --platform "$native_platform" --network none --user 0:0 \
    "${storage[@]}" --entrypoint chmod "$node_image" 750 /var/lib/openlegal/cache-blobs/data
expect_failure 'invalid blob permissions' 'Error: StorageCorrupt' --env-file "$scratch/runtime.env"
docker run --rm --name "$initializer" --platform "$native_platform" --network none --user 0:0 \
    "${storage[@]}" --entrypoint sh "$node_image" -ec 'chmod 700 /var/lib/openlegal/cache-blobs/data; chown 0:10004 /var/lib/openlegal/cache-blobs/data'
expect_failure 'invalid blob ownership' 'Error: StorageUnavailable' --env-file "$scratch/runtime.env"
docker run --rm --name "$initializer" --platform "$native_platform" --network none --user 0:0 \
    "${storage[@]}" --entrypoint chown "$node_image" 10004:10004 /var/lib/openlegal/cache-blobs/data
docker run --rm --name "$initializer" --platform "$native_platform" --network none --user 0:0 \
    "${dictionary_rw[@]}" --entrypoint mv "$node_image" /var/lib/openlegal/mecab-ko-dictionary/data /var/lib/openlegal/mecab-ko-dictionary/saved
expect_failure 'missing dictionary' 'Error: StorageUnavailable' --env-file "$scratch/runtime.env"
docker run --rm --name "$initializer" --platform "$native_platform" --network none --user 0:0 \
    "${dictionary_rw[@]}" --entrypoint sh "$node_image" -ec '
    mv /var/lib/openlegal/mecab-ko-dictionary/saved /var/lib/openlegal/mecab-ko-dictionary/data
    cp -p /var/lib/openlegal/mecab-ko-dictionary/data/manifest.json /var/lib/openlegal/mecab-ko-dictionary/manifest.saved
    printf corrupt > /var/lib/openlegal/mecab-ko-dictionary/data/manifest.json
    '
expect_failure 'corrupt dictionary' 'Error: StorageCorrupt' --env-file "$scratch/runtime.env"
docker run --rm --name "$initializer" --platform "$native_platform" --network none --user 0:0 \
    "${dictionary_rw[@]}" --entrypoint sh "$node_image" -ec '
    mv /var/lib/openlegal/mecab-ko-dictionary/manifest.saved /var/lib/openlegal/mecab-ko-dictionary/data/manifest.json
    test "$(stat -c %u:%g:%a /var/lib/openlegal/mecab-ko-dictionary/data/manifest.json)" = 0:10004:440
    '
# A known-good startup and the same capture/query assertions after all negative
# injections prevent an unrelated persistent failure from earning a passing gate.
acceptance_cycle recovered
printf 'Retained image acceptance passed for %s (fictional data, Docker only).\n' "$platform"
