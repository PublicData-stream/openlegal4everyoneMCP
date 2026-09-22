#!/usr/bin/env bash
# Offline template invariants, not Kubernetes API admission or cluster acceptance.
set -euo pipefail
repo=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
config_args=()
if (( $# )); then
    if [[ $# != 2 || $1 != --config-output || -z $2 ]]; then
        echo "usage: $0 [--config-output FILE]" >&2
        exit 2
    fi
    config_args=(--config-output "$2")
fi
tools_dir=${OPENLEGAL_DEPLOY_TOOLS:-$repo/target/deployment-tools}
case "$(uname -s)/$(uname -m)" in
    Linux/x86_64) arch=amd64 ;;
    Linux/aarch64|Linux/arm64) arch=arm64 ;;
    *) echo 'Deployment tools require Linux amd64 or arm64.' >&2; exit 1 ;;
esac
checksum=$(awk -v arch="$arch" '$2 == arch {print $1}' "$repo/scripts/deployment-kubectl.sha256")
if [[ ! -f "$tools_dir/bin/kubectl" ]] || ! printf '%s  %s\n' "$checksum" "$tools_dir/bin/kubectl" | sha256sum --check --status 2>/dev/null \
    || [[ ! -x "$tools_dir/bin/python" ]]; then
    echo 'Missing or mismatched tools; run scripts/setup-deployment-tools.sh first.' >&2
    exit 1
fi
"$tools_dir/bin/python" -c 'import yaml; assert yaml.__version__ == "6.0.3", "Run scripts/setup-deployment-tools.sh"'
scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT
# Kustomize build is local: no cluster, credentials, discovery or API schema fetch.
"$tools_dir/bin/kubectl" kustomize "$repo/deploy/kubernetes/serving" > "$scratch/serving.yaml"
PYTHONDONTWRITEBYTECODE=1 "$tools_dir/bin/python" "$repo/scripts/deployment_validation.py" \
    "$scratch/serving.yaml" "${config_args[@]}"
OPENLEGAL_RENDERED_SERVING="$scratch/serving.yaml" PYTHONDONTWRITEBYTECODE=1 \
    "$tools_dir/bin/python" -m unittest discover -s "$repo/scripts/tests" -p test_deployment_validation.py
