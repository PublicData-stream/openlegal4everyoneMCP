#!/usr/bin/env bash
# Explicit real PG18/TLS synchronous replication gate; all data/certs are /tmp.
set -euo pipefail
repo=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
for command in docker python3 openssl cargo timeout; do
    command -v "$command" >/dev/null || { echo "Missing command: $command" >&2; exit 1; }
done
export TMPDIR=/tmp
export CARGO_TARGET_DIR=${CARGO_TARGET_DIR:-/buildcache/rust/target}
cd "$repo"
cargo test --locked --lib -p openlegal-adapters replication_pair -- --ignored --test-threads=1
cargo test --locked -p openlegal-adapters --test replication_binding -- --ignored --test-threads=1
cargo test --locked -p openlegal-adapters --test canary_lifecycle -- --ignored --test-threads=1
cargo test --locked -p openlegal-adapters --test survivor_observers -- --ignored --test-threads=1
