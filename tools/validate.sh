#!/usr/bin/env bash
# Static manifest/script validation. Requirements: yamllint, python3, shellcheck.
# For --helm mode, also: helm (v3/v4).
set -uo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
fail=0

# -------------------------------------------------------------------------
# --helm mode: helm lint + helm template (render) -> helm-check.py
#   (namespace-awareness + no-{{}} + service-reference closure check)
# Usage: validate.sh --helm [namespace] [values-file]
#   default namespace:    strayprobe
#   default values-file:  chart/values-prod.example.yaml
#
# Probe-namespace token is "strayprobe", NOT "example" (mirrors the same
# chart/tests/integrity.sh fix): the neutral placeholder domain
# (chart/values-prod.example.yaml's `global.domain: lakehouse.example.com`,
# post customer-independence genericization) legitimately contains the
# substring "example" in every public hostname / S3 endpoint, which would
# otherwise trip `--no-stray example` with false positives regardless of
# actual namespace-hardcoding bugs. "strayprobe" does not collide with any
# legitimate chart output.
# -------------------------------------------------------------------------
if [ "${1:-}" = "--helm" ]; then
  shift
  NS="${1:-strayprobe}"
  CHART_DIR="$(cd "$ROOT/../chart" && pwd)"
  VALUES_FILE="${2:-$CHART_DIR/values-prod.example.yaml}"

  echo "== helm lint =="
  helm lint "$CHART_DIR" || fail=1

  echo "== helm template (-n $NS, -f $VALUES_FILE) + helm-check.py =="
  if ! helm template "$CHART_DIR" -f "$VALUES_FILE" -n "$NS" \
      | python3 "$CHART_DIR/scripts/helm-check.py" - \
          --release-namespace "$NS" --no-stray strayprobe --service-closure; then
    fail=1
  fi

  [ "$fail" = 0 ] && echo "VALIDATE: PASS" || echo "VALIDATE: FAIL"
  exit $fail
fi

TARGET="${1:-$ROOT}"

echo "== yamllint =="
if python3 -m yamllint --version >/dev/null 2>&1; then
  python3 -m yamllint -c "$ROOT/.yamllint" "$TARGET" || fail=1
elif command -v yamllint >/dev/null; then
  yamllint -c "$ROOT/.yamllint" "$TARGET" || fail=1
else echo "SKIP: yamllint yok (pip install --user yamllint)"; fi

echo "== YAML parse (multi-doc) =="
while IFS= read -r -d '' f; do
  python3 -c "import sys,yaml; list(yaml.safe_load_all(open(sys.argv[1])))" "$f" \
    || { echo "PARSE FAIL: $f"; fail=1; }
done < <(find "$TARGET" -type f \( -name '*.yaml' -o -name '*.yml' \) -print0)

echo "== shellcheck =="
if command -v shellcheck >/dev/null; then
  while IFS= read -r -d '' s; do shellcheck "$s" || fail=1; done \
    < <(find "$TARGET" -type f -name '*.sh' -print0)
else echo "SKIP: shellcheck yok (brew install shellcheck)"; fi

[ "$fail" = 0 ] && echo "VALIDATE: PASS" || echo "VALIDATE: FAIL"
exit $fail
