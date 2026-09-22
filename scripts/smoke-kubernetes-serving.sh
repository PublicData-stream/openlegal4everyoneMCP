#!/usr/bin/env bash
# Explicit configured-endpoint acceptance; never provisions or builds dependencies.
set -euo pipefail
script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
exec python3 "$script_dir/serving_smoke.py" "$@"
