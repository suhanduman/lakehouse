#!/usr/bin/env bash
# chart/tests/quota-fits.sh — per-tier ResourceQuota fit gate (audit 🟠).
# For each sizing tier, renders the chart and asserts (via quota-check.py)
# that the namespace ResourceQuota >= the aggregate resource demand of every
# pod-producing kind, no container limit exceeds LimitRange.max, and the pod
# count fits. See docs/superpowers/specs/2026-08-30-tier-quota-fit-design.md
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
CHART="$(cd "$HERE/.." && pwd)"
CHECK="$CHART/scripts/quota-check.py"
declare -a TIERS=("values.yaml:small" "values-medium.yaml:medium" "values-large.yaml:large")
fail=0
for entry in "${TIERS[@]}"; do
  vf="${entry%%:*}"; label="${entry##*:}"
  if ! helm template lakehouse "$CHART" -f "$CHART/$vf" 2>/dev/null | python3 "$CHECK" --tier "$label"; then
    fail=1
  fi
done
if [ "$fail" -ne 0 ]; then echo "QUOTA-FITS: FAIL"; exit 1; fi
echo "QUOTA-FITS: all tiers OK"
