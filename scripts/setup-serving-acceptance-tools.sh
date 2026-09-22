#!/usr/bin/env bash
# Explicit provisioning only. No cluster creation, builds, or legal provider calls.
set -euo pipefail
repo=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
if (( $# != 1 )); then
    echo "usage: $0 DEST (also run setup-deployment-tools.sh DEST)" >&2; exit 2
fi
tools_dir=$1
case "$(uname -s)/$(uname -m)" in
    Linux/x86_64) arch=amd64 ;;
    Linux/aarch64|Linux/arm64) arch=arm64 ;;
    *) echo 'Acceptance tools require native Linux amd64 or arm64.' >&2; exit 1 ;;
esac
mkdir -p "$tools_dir/bin" "$tools_dir/acceptance"
scratch=$(mktemp -d)
trap 'rm -rf -- "$scratch"' EXIT
mapfile -t pins < <(python3 - "$repo/test-support/kubernetes-acceptance/assets.lock.json" "$arch" <<'PY'
import json, sys
lock = json.load(open(sys.argv[1]))
print(lock['kind_version'])
print(lock['kind_sha256'][sys.argv[2]])
print(lock['calico_url'])
print(lock['calico_sha256'])
PY
)
(( ${#pins[@]} == 4 )) || exit 1
fetch() {
    curl --fail --show-error --silent --location --proto '=https' --proto-redir '=https' \
        --tlsv1.2 --connect-timeout 15 --max-time 300 "$1" -o "$2"
    printf '%s  %s\n' "$3" "$2" | sha256sum --check --status
}
fetch "https://github.com/kubernetes-sigs/kind/releases/download/v${pins[0]}/kind-linux-$arch" \
    "$scratch/kind" "${pins[1]}"
fetch "${pins[2]}" "$scratch/calico.upstream.yaml" "${pins[3]}"
python3 "$repo/test-support/kubernetes-acceptance/assets.py" "$scratch/calico.upstream.yaml" "$scratch/calico.yaml"
install -m 0755 "$scratch/kind" "$tools_dir/bin/kind"
install -m 0644 "$scratch/calico.upstream.yaml" "$tools_dir/acceptance/calico.upstream.yaml"
install -m 0644 "$scratch/calico.yaml" "$tools_dir/acceptance/calico.yaml"
printf 'Verified kind and Calico assets provisioned in %s\n' "$tools_dir"
printf 'Preload the locked images as described in test-support/kubernetes-acceptance/README.md.\n'
