#!/usr/bin/env bash
# Polaris katalog/rol/principal kurulumu (kurulumdan sonra bir kez; idempotent).
#   runbooks/scripts/polaris-setup.sh [--setup platform/polaris/setup.yaml] [--ns lakehouse]
# Gereksinim: pip install apache-polaris ; root credential Secret'ı (polaris-root: clientId/clientSecret) kümede.
set -euo pipefail
SETUP=platform/polaris/setup.yaml; NS=lakehouse
while [[ $# -gt 0 ]]; do case "$1" in --setup) SETUP="$2"; shift 2;; --ns) NS="$2"; shift 2;; *) echo "bilinmeyen argüman: $1"; exit 2;; esac; done
command -v polaris >/dev/null || { echo "polaris CLI yok: pip install apache-polaris"; exit 1; }
CLIENT_ID=$(kubectl -n "$NS" get secret polaris-root -o jsonpath='{.data.clientId}' | base64 -d)
CLIENT_SECRET=$(kubectl -n "$NS" get secret polaris-root -o jsonpath='{.data.clientSecret}' | base64 -d)
export CLIENT_ID CLIENT_SECRET
kubectl -n "$NS" port-forward svc/polaris 8181:8181 >/dev/null 2>&1 & PF=$!; trap 'kill $PF 2>/dev/null' EXIT; sleep 3
LOG=$(mktemp)
polaris setup apply "$SETUP" 2>&1 | tee "$LOG"
# setup apply yeni principal'lar için {"clientId","clientSecret"} basar (yaratma sırasıyla); root rotate EDEMEZ -> hemen Secret'a yaz
python3 - "$LOG" "$NS" <<'PY'
import sys, re, json, subprocess
log = open(sys.argv[1]).read(); ns = sys.argv[2]
names = re.findall(r"Creating principal: ([A-Za-z0-9_-]+)", log)
creds = [json.loads(l) for l in log.splitlines() if l.startswith('{"clientId"')]
assert len(names) == len(creds), f"principal/credential sayısı uyuşmuyor: {names} vs {len(creds)}"
for n, c in zip(names, creds):
    manifest = subprocess.run(["kubectl", "-n", ns, "create", "secret", "generic", f"polaris-{n}",
                               f"--from-literal=credential={c['clientId']}:{c['clientSecret']}", "--dry-run=client", "-o", "yaml"],
                              check=True, capture_output=True, text=True).stdout
    subprocess.run(["kubectl", "apply", "-f", "-"], input=manifest, check=True, text=True)
    print("Secret yazıldı:", f"polaris-{n}")
if not names: print("Yeni principal yok (idempotent çalıştırma).")
PY
