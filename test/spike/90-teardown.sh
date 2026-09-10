#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
for f in .state/pf-*.pid; do [[ -f "$f" ]] && kill "$(cat "$f")" 2>/dev/null || true; done
KIND_EXPERIMENTAL_PROVIDER="${KIND_EXPERIMENTAL_PROVIDER:-podman}" kind delete cluster --name lh-spike
rm -rf .state
echo "OK: spike ortamı silindi"
