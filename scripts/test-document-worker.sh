#!/usr/bin/env bash
# Local/CI native parser acceptance; no Kubernetes or provider credentials.
set -euo pipefail
cd "$(dirname "$0")/.."
image="openlegal-document-fixture-tests:local"
docker build --platform linux/amd64 --target fixture-tests \
  --build-arg "ADMISSION_REFRESH=$(date -u +%Y%m%dT%H%M%SZ)" \
  -f apps/document-worker/Dockerfile -t "$image" .
docker run --pull never --rm --network none --read-only --cap-drop ALL \
  --security-opt no-new-privileges \
  --security-opt "seccomp=$PWD/deploy/document-sandbox/seccomp.json" \
  --pids-limit 128 --memory 4g --cpus 2 \
  --tmpfs /scratch:rw,size=2g,uid=65532,gid=65532 \
  "$image"
