#!/usr/bin/env bash
# Local/CI native parser acceptance; no Kubernetes or provider credentials.
set -euo pipefail
cd "$(dirname "$0")/.."
image="openlegal-document-fixture-tests:local"
production_image="openlegal-document-worker:local"
release_version=
while (( $# )); do
  case "$1" in
    --output-image) [[ $# -ge 2 ]] || exit 2; production_image=$2; shift 2 ;;
    --version) [[ $# -ge 2 ]] || exit 2; release_version=$2; shift 2 ;;
    *) echo "Usage: $0 [--output-image IMAGE --version VERSION]" >&2; exit 2 ;;
  esac
done
if [[ -n $release_version || $production_image != openlegal-document-worker:local ]]; then
  [[ -n $release_version && $production_image =~ ^ghcr\.io/publicdata-stream/openlegal-document-worker:[a-zA-Z0-9_.-]+$ ]] || { echo 'Invalid release image arguments' >&2; exit 2; }
  [[ $release_version =~ ^[0-9]+\.[0-9]+\.[0-9]+(-beta\.[0-9]+|-build\.[0-9a-f]{8})?$ ]] || { echo 'Invalid release version' >&2; exit 2; }
  [[ $production_image == "ghcr.io/publicdata-stream/openlegal-document-worker:$release_version-amd64" ]] || { echo 'Output image does not match version and architecture' >&2; exit 2; }
fi
admission_refresh=$(date -u +%Y%m%dT%H%M%SZ)
revision=$(git rev-parse HEAD)
container="openlegal-document-smoke-$$"
trap 'docker rm -f "$container" >/dev/null 2>&1 || true' EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
docker build --platform linux/amd64 --target fixture-tests \
  --build-arg "ADMISSION_REFRESH=$admission_refresh" \
  -f apps/document-worker/Dockerfile -t "$image" .
# Inspect image contents with Docker's default seccomp: BusyBox shell process
# creation is outside the worker allowlist. Actual worker execution
# below retains the canonical seccomp profile and all confinement limits.
docker run --pull never --rm --network none --read-only --cap-drop ALL \
  --security-opt no-new-privileges \
  --pids-limit 128 --memory 4g --cpus 2 \
  --tmpfs /scratch:rw,size=2g,uid=65532,gid=65532 \
  --entrypoint /bin/sh "$image" -ec '
    test "$(id -u):$(id -g)" = 65532:65532
    . /etc/os-release
    test "$ID" = alpine
    case "$VERSION_ID" in 3.24|3.24.*) ;; *) exit 1 ;; esac
    for package in gcompat libc6-compat; do
      if apk info -e "$package"; then echo "glibc compatibility package present: $package" >&2; exit 1; fi
    done
    test -s /opt/notices/alpine-packages.txt
    apk info -v | LC_ALL=C sort > /scratch/alpine-packages.txt
    cmp /scratch/alpine-packages.txt /opt/notices/alpine-packages.txt
    for executable in openlegal-document-worker document-format-tests; do
      test "$(stat -c %u:%g /usr/local/bin/$executable)" = 0:0
      ldd /usr/local/bin/$executable > /scratch/ldd 2>&1
      cat /scratch/ldd
      grep -q ld-musl- /scratch/ldd
      if grep -Eq "not found|Error loading|Error relocating|libc\.so\.6" /scratch/ldd; then exit 1; fi
    done
    for tool in cargo rustc cc gcc clang node npm pnpm git; do
      if command -v "$tool"; then echo "Build tool present: $tool" >&2; exit 1; fi
    done
    test -s /opt/tessdata/eng.traineddata
    test -s /opt/tessdata/kor.traineddata
    test -s /opt/fonts/NotoSansCJKkr-Regular.otf
    test -s /opt/notices/OPENLEGAL-LICENSE
  '
docker run --pull never --rm --network none --read-only --cap-drop ALL \
  --security-opt no-new-privileges \
  --security-opt "seccomp=$PWD/deploy/document-sandbox/seccomp.json" \
  --pids-limit 128 --memory 4g --cpus 2 \
  --tmpfs /scratch:rw,size=2g,uid=65532,gid=65532 \
  "$image"
# Build the shipped target from the same admitted layers; never ship the test binary.
# Keep the release architecture tag as a single manifest for registry verification.
docker build --provenance=false --platform linux/amd64 --target worker \
  --build-arg "ADMISSION_REFRESH=$admission_refresh" \
  --build-arg "REVISION=$revision" --build-arg "VERSION=${release_version:-development}" \
  -f apps/document-worker/Dockerfile -t "$production_image" .
for label in org.opencontainers.image.source org.opencontainers.image.revision org.opencontainers.image.version org.opencontainers.image.licenses org.openlegal.cpu-baseline; do
  actual=$(docker image inspect --format "{{index .Config.Labels \"$label\"}}" "$production_image")
  case "$label" in
    org.opencontainers.image.source) expected=https://github.com/PublicData-stream/openlegal4everyoneMCP ;;
    org.opencontainers.image.revision) expected=$revision ;;
    org.opencontainers.image.version) expected=${release_version:-development} ;;
    org.opencontainers.image.licenses) expected=AGPL-3.0-only ;;
    org.openlegal.cpu-baseline) expected=x86-64-v3 ;;
  esac
  [[ $actual == "$expected" ]] || { echo "Worker image label $label was $actual, expected $expected" >&2; exit 1; }
done
docker run --pull never -d --name "$container" --network none --read-only --cap-drop ALL \
  --security-opt no-new-privileges \
  --security-opt "seccomp=$PWD/deploy/document-sandbox/seccomp.json" \
  --pids-limit 128 --memory 4g --cpus 2 \
  --tmpfs /scratch:rw,size=2g,uid=65532,gid=65532 \
  "$production_image" >/dev/null
docker exec "$container" test ! -e /usr/local/bin/document-format-tests
python3 - "$container" <<'PY'
import hashlib
import json
from pathlib import Path
import struct
import subprocess
import sys

container = sys.argv[1]
for filename, format_, expected in (
    ("law.xml", "xml", "가상 조문"),
    ("precedent.html", "html", "가상 판결"),
):
    raw = (Path("apps/document-worker/tests/fixtures") / filename).read_bytes()
    digest = hashlib.sha256(raw).hexdigest()
    header = json.dumps(dict(format=format_, source_sha256=digest,
                             ocr=False, bytes_len=len(raw))).encode()
    result = subprocess.run(
        ["docker", "exec", "-i", container, "/usr/local/bin/openlegal-document-worker", "--process"],
        input=struct.pack(">I", len(header)) + header + raw,
        capture_output=True, check=True, timeout=30,
    )
    assert not result.stderr, "unexpected processing stderr"
    assert len(result.stdout) >= 4, "missing response frame"
    length = struct.unpack(">I", result.stdout[:4])[0]
    assert length <= 16 * 1024**2 and len(result.stdout) == length + 4
    response = json.loads(result.stdout[4:])
    assert response["status"] == "success"
    value = response["value"]
    assert value["source_sha256"] == digest and value["format"] == format_
    assert expected in value["text"] and not value["ocr_pages"]
    assert "untrusted.invalid" not in value["text"], "HTML script was not inert"
probe = json.loads(subprocess.check_output(
    ["docker", "exec", container, "/usr/local/bin/openlegal-document-worker", "--probe-exhaust-pids"],
    timeout=30,
))
assert probe["denied"] and 0 < probe["created"] < 128
logs = subprocess.run(["docker", "logs", container], capture_output=True, check=True, timeout=10)
assert not logs.stdout and not logs.stderr, "document content reached container logs"
print("Production worker: framed XML/HTML, inert scripts, PID limit and empty logs passed")
PY
