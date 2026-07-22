#!/usr/bin/env bash
# manual-install/render.sh — chart'tan tek-dosya manuel kurulum manifesti ÜRETİR.
#
# `chart/` tek doğruluk kaynağıdır; bu script yalnızca `helm template` render'ını
# bir dosyaya yazan bir kolaylıktır (air-gapped/manuel/Helm'siz senaryolar için).
# Üretilen dosya ELLE DÜZENLENMEZ — chart değişince bu script yeniden çalıştırılıp
# çıktı yeniden commit edilir.
#
# Usage: manual-install/render.sh [namespace] [values-file]
#   default namespace:    example
#   default values-file:  chart/values-prod.example.yaml
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CHART_DIR="$ROOT/chart"

NS="${1:-example}"
VALUES_FILE="${2:-$CHART_DIR/values-prod.example.yaml}"
OUT_DIR="$ROOT/manual-install/manifests"
OUT_FILE="$OUT_DIR/lakehouse-${NS}.yaml"

mkdir -p "$OUT_DIR"

echo "== helm template (chart/ -n $NS -f $VALUES_FILE) -> $OUT_FILE =="

{
  echo "# GENERATED from chart/ — do not edit; edit the chart and re-run"
  echo "# manual-install/render.sh instead. chart/ is the single source of truth;"
  echo "# this file is a rendered convenience for air-gapped/manual/no-helm"
  echo "# install scenarios (see manual-install/README.md)."
  echo "#"
  echo "# Command used to generate this file:"
  echo "#   manual-install/render.sh ${NS} ${VALUES_FILE#"$ROOT"/}"
  echo "#"
  helm template lakehouse "$CHART_DIR" -f "$VALUES_FILE" -n "$NS"
} > "$OUT_FILE"

echo "OK: $OUT_FILE"
