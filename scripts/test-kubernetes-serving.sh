#!/usr/bin/env bash
# Offline source, invariant and schema checks; not API admission or cluster acceptance.
set -euo pipefail
export PYTHONDONTWRITEBYTECODE=1
repo=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
config_args=()
admin_output_dir=
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
        --admin-output-dir)
            [[ $# -ge 2 && -n $2 ]] || { echo '--admin-output-dir needs DIR' >&2; exit 2; }
            admin_output_dir=$2
            shift 2 ;;
        *) echo "usage: $0 [--profile retained|text-only] [--config-output FILE] [--admin-output-dir DIR]" >&2; exit 2 ;;
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
"$tools_dir/bin/python" "$repo/scripts/deployment_sources.py" "$repo"
"$tools_dir/bin/python" "$repo/scripts/deployment_schemas.py" verify --tools-dir "$tools_dir"
scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT
# Inputs were checked before any renderer runs. Do not forward renderer errors:
# they can include credential-bearing snippets from malformed source files.
render() {
    if ! "$tools_dir/bin/kubectl" kustomize "$repo/$1" > "$2" 2> "$scratch/render-error"; then
        echo "Deployment rendering failed: $1 (renderer diagnostics suppressed)." >&2
        exit 1
    fi
}
render deploy/kubernetes/serving "$scratch/retained.yaml"
render test-support/deployment/text-only "$scratch/text-only.yaml"
mkdir "$scratch/network"
for variant in base edge postgres-in-cluster postgres-external dns-cluster dns-fixed monitoring ingestion-api ingestion-provider; do
    render "deploy/kubernetes/network/$variant" "$scratch/network/$variant.yaml"
done
render deploy/kubernetes/ingestion "$scratch/ingestion.yaml"
render deploy/kubernetes/ingestion/rbac "$scratch/ingestion-rbac.yaml"
admin_args=()
for operation in migrate maintain rebuild; do
    render "deploy/kubernetes/admin/$operation" "$scratch/$operation.yaml"
    admin_args+=(--admin-manifest "$operation=$scratch/$operation.yaml")
done
# Validate untouched operator examples first. Schema validation uses separate
# temporary copies with exact, synthetic storage substitutions only.
for example in storage-class local-pv local-pvc local-rebuild-pv local-rebuild-pvc; do
    printf '%s\n' '---' >> "$scratch/storage.yaml"
    cat "$repo/deploy/kubernetes/storage/$example.example.yaml" >> "$scratch/storage.yaml"
done
for checked_profile in retained text-only; do
    output_args=()
    edge_args=()
    operation_args=()
    if [[ $checked_profile == retained ]]; then
        edge_args=(--oxibelt-config "$repo/deploy/oxibelt/kubernetes-upstream.example.toml")
        operation_args=("${admin_args[@]}" --ingestion-manifest "$scratch/ingestion.yaml" \
            --ingestion-rbac "$scratch/ingestion-rbac.yaml")
    fi
    if [[ $checked_profile == "$profile" ]]; then
        output_args=("${config_args[@]}")
    fi
    PYTHONDONTWRITEBYTECODE=1 "$tools_dir/bin/python" "$repo/scripts/deployment_validation.py" \
        "$scratch/$checked_profile.yaml" --profile "$checked_profile" \
        --network-dir "$scratch/network" \
        --document-boundary "$repo/deploy/document-sandbox/namespace.yaml" \
        --document-controller-role "$repo/deploy/document-sandbox/controller-role.yaml" \
        --storage-manifest "$scratch/storage.yaml" "${edge_args[@]}" "${output_args[@]}" "${operation_args[@]}"
done
"$tools_dir/bin/python" "$repo/scripts/deployment_schemas.py" validate \
    --tools-dir "$tools_dir" --rendered-dir "$scratch" --repo "$repo"
OPENLEGAL_DEPLOY_TOOLS="$tools_dir" OPENLEGAL_SCHEMA_RENDERED_DIR="$scratch" \
OPENLEGAL_RENDERED_INGESTION="$scratch/ingestion.yaml" \
    OPENLEGAL_RENDERED_INGESTION_RBAC="$scratch/ingestion-rbac.yaml" \
    OPENLEGAL_RENDERED_SERVING="$scratch/retained.yaml" \
    OPENLEGAL_RENDERED_TEXT_ONLY="$scratch/text-only.yaml" \
    OPENLEGAL_STORAGE_EXAMPLES="$scratch/storage.yaml" PYTHONDONTWRITEBYTECODE=1 \
    OPENLEGAL_OXIBELT_EXAMPLE="$repo/deploy/oxibelt/kubernetes-upstream.example.toml" \
    OPENLEGAL_RENDERED_ADMIN_DIR="$scratch" \
    OPENLEGAL_RENDERED_NETWORK_DIR="$scratch/network" \
    OPENLEGAL_DOCUMENT_BOUNDARY="$repo/deploy/document-sandbox/namespace.yaml" \
    OPENLEGAL_DOCUMENT_CONTROLLER_ROLE="$repo/deploy/document-sandbox/controller-role.yaml" \
    "$tools_dir/bin/python" -m unittest discover -s "$repo/scripts/tests" -p 'test_deployment*.py'
if [[ -n $admin_output_dir ]]; then
    mkdir -p "$admin_output_dir"
    for operation in migrate maintain rebuild; do
        cp "$scratch/$operation.yaml" "$admin_output_dir/$operation.yaml"
    done
fi
