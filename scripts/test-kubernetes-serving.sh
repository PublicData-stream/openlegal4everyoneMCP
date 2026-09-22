#!/usr/bin/env bash
# Offline template invariants, not Kubernetes API admission or cluster acceptance.
set -euo pipefail
repo=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
config_args=()
profile=retained
while (( $# )); do
    case "$1" in
        --profile)
            [[ $# -ge 2 ]] || { echo '--profile needs retained or text-only' >&2; exit 2; }
            profile=$2
            shift 2 ;;
        --config-output)
            [[ $# -ge 2 && -n $2 ]] || { echo '--config-output needs FILE' >&2; exit 2; }
            config_args=(--config-output "$2")
            shift 2 ;;
        *) echo "usage: $0 [--profile retained|text-only] [--config-output FILE]" >&2; exit 2 ;;
    esac
done
case "$profile" in retained|text-only) ;; *) echo 'Invalid profile' >&2; exit 2 ;; esac
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
"$tools_dir/bin/kubectl" kustomize "$repo/deploy/kubernetes/serving" > "$scratch/retained.yaml"
"$tools_dir/bin/kubectl" kustomize "$repo/test-support/deployment/text-only" > "$scratch/text-only.yaml"
# Storage examples intentionally contain operator capacity placeholders and are
# parsed directly, not passed to Kubernetes schema/admission or attached to serving.
for example in storage-class local-pv local-pvc; do
    printf '%s\n' '---' >> "$scratch/storage.yaml"
    cat "$repo/deploy/kubernetes/storage/$example.example.yaml" >> "$scratch/storage.yaml"
done
for checked_profile in retained text-only; do
    output_args=()
    if [[ $checked_profile == "$profile" ]]; then
        output_args=("${config_args[@]}")
    fi
    PYTHONDONTWRITEBYTECODE=1 "$tools_dir/bin/python" "$repo/scripts/deployment_validation.py" \
        "$scratch/$checked_profile.yaml" --profile "$checked_profile" \
        --storage-manifest "$scratch/storage.yaml" "${output_args[@]}"
done
OPENLEGAL_RENDERED_SERVING="$scratch/retained.yaml" \
    OPENLEGAL_RENDERED_TEXT_ONLY="$scratch/text-only.yaml" \
    OPENLEGAL_STORAGE_EXAMPLES="$scratch/storage.yaml" PYTHONDONTWRITEBYTECODE=1 \
    "$tools_dir/bin/python" -m unittest discover -s "$repo/scripts/tests" -p test_deployment_validation.py
