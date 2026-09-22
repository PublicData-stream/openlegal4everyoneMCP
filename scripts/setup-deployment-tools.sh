#!/usr/bin/env bash
# Explicit provisioning only; the serving gate never downloads tools.
set -euo pipefail
repo=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
if (( $# > 1 )); then
    echo "usage: $0 [DEST]" >&2
    exit 2
fi
tools_dir=${1:-${OPENLEGAL_DEPLOY_TOOLS:-$repo/target/deployment-tools}}
case "$(uname -s)/$(uname -m)" in
    Linux/x86_64) arch=amd64 ;;
    Linux/aarch64|Linux/arm64) arch=arm64 ;;
    *) echo 'Deployment tools require Linux amd64 or arm64.' >&2; exit 1 ;;
esac
python3 -c 'import sys; assert (3, 11) <= sys.version_info[:2] <= (3, 14), "Python 3.11-3.14 required"'
if ! python3 -c 'import ensurepip' 2>/dev/null; then
    echo 'Python venv/ensurepip is required; install python3-venv on Debian/Ubuntu.' >&2
    exit 1
fi
mkdir -p "$tools_dir/bin"
scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT
checksum=$(awk -v arch="$arch" '$2 == arch {print $1}' "$repo/scripts/deployment-kubectl.sha256")
if [[ ! -f "$tools_dir/bin/kubectl" ]] || ! printf '%s  %s\n' "$checksum" "$tools_dir/bin/kubectl" | sha256sum --check --status 2>/dev/null; then
    curl --fail --show-error --silent --location --proto '=https' --tlsv1.2 \
        --connect-timeout 15 --max-time 300 --retry 3 \
        "https://dl.k8s.io/release/v1.37.0/bin/linux/$arch/kubectl" -o "$scratch/kubectl"
    printf '%s  %s\n' "$checksum" "$scratch/kubectl" | sha256sum --check --status
    install -m 0755 "$scratch/kubectl" "$tools_dir/bin/kubectl"
fi
python3 -m venv "$tools_dir"
"$tools_dir/bin/python" -m pip --isolated --disable-pip-version-check install \
    --index-url https://pypi.org/simple --require-hashes --only-binary=:all: --no-deps \
    -r "$repo/scripts/deployment-requirements.txt"
printf 'Deployment tools provisioned in %s\n' "$tools_dir"
