#!/usr/bin/env bash
set -euo pipefail
if [[ ${RUN_DOCUMENT_SANDBOX_TESTS:-} != 1 ]]; then
  echo 'Opt in with RUN_DOCUMENT_SANDBOX_TESTS=1; this gate creates and deletes disposable Pods.' >&2
  exit 2
fi
: "${DOCUMENT_KUBECONFIG:?explicit kubeconfig required}"
: "${DOCUMENT_CONTEXT:?explicit Kubernetes context required}"
: "${DOCUMENT_IMAGE:?immutable worker image digest required}"
python3 deploy/document-sandbox/acceptance.py
