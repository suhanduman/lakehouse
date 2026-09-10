#!/usr/bin/env bash
# S1: Polaris 1.7 + CNPG + MinIO. Idempotent. Port-forward'lar arka planda açılır (.state/pf-*.pid).
set -euo pipefail
SELF="$(cd "$(dirname "$0")" && pwd)/$(basename "$0")"
cd "$(dirname "$0")"
STATE=.state; mkdir -p "$STATE"
PY=../../.venv/bin/python; POLARIS=../../.venv/bin/polaris

pf() { # pf <ns> <svc> <local:remote> <pidfile>
  if [[ -f "$STATE/$4" ]] && kill -0 "$(cat "$STATE/$4")" 2>/dev/null; then return; fi
  kubectl -n "$1" port-forward "svc/$2" "$3" >/dev/null 2>&1 & echo $! > "$STATE/$4"; sleep 2
}

smoke_job() { # smoke_job <mode: vended|static>  -- smoke.py'yi KÜME İÇİNDE koşturur (sunucu s3.endpoint'i küme-içi ad döndürür)
  local m="$1" extra=""; [[ "$m" == "static" ]] && extra="--static-keys"
  kubectl -n polaris delete job "polaris-smoke-$m" --ignore-not-found >/dev/null
  sed -e "s/MODE/$m/" -e "s/ EXTRA_ARG/${extra:+ $extra}/" 20-polaris/smoke-job.yaml | kubectl apply -f - >/dev/null
  kubectl -n polaris wait --for=condition=complete "job/polaris-smoke-$m" --timeout=420s >/dev/null
  kubectl -n polaris logs "job/polaris-smoke-$m" --tail=3 | grep "^OK"
}

if [[ "${1:-}" == "--check" ]]; then
  kubectl -n polaris rollout status deploy/polaris --timeout=30s >/dev/null
  set -a; source "$STATE/polaris-connect.env"; set +a
  kubectl -n polaris create configmap polaris-smoke --from-file=20-polaris/smoke.py --dry-run=client -o yaml | kubectl apply -f - >/dev/null
  kubectl -n polaris create secret generic polaris-smoke-cred --from-literal=CLIENT_ID="$CLIENT_ID" --from-literal=CLIENT_SECRET="$CLIENT_SECRET" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
  smoke_job vended
  smoke_job static
  kubectl -n lakehouse get secret polaris-connect >/dev/null && kubectl -n spark get secret polaris-spark >/dev/null
  echo "OK: S1 Polaris hazır"; exit 0
fi

helm repo add polaris https://downloads.apache.org/polaris/helm-chart >/dev/null 2>&1 || true
helm repo update polaris >/dev/null
kubectl -n polaris apply -f 20-polaris/bootstrap-job.yaml
kubectl -n polaris wait --for=condition=complete job/polaris-bootstrap --timeout=300s
helm upgrade --install polaris polaris/polaris --version 1.7.0 -n polaris -f 20-polaris/values.yaml --wait --timeout 10m
pf polaris polaris 8181:8181 pf-polaris.pid
pf minio minio 9000:9000 pf-minio.pid
kubectl -n polaris rollout status deploy/polaris --timeout=600s   # health 8182 (management) portunda; API 8181
until curl -s -o /dev/null -w "%{http_code}" http://localhost:8181/api/catalog/v1/config | grep -qE "^(200|400|401)$"; do sleep 3; done

export CLIENT_ID=root CLIENT_SECRET=s3cr3t
$POLARIS setup apply 20-polaris/setup.yaml 2>&1 | tee -a "$STATE/setup-apply.log"
# setup apply, yarattığı her principal için stdout'a {"clientId","clientSecret"} basar (yaratma sırasıyla).
# Root principal ROTATE_CREDENTIALS yetkisine sahip DEĞİL (canlı bulgu) -> credential'lar yalnız bu log'dan alınır.
mapfile -t NAMES < <(grep -oE "Creating principal: [a-zA-Z0-9_-]+" "$STATE/setup-apply.log" | awk '{print $3}')
mapfile -t CREDS < <(grep -E '^\{"clientId"' "$STATE/setup-apply.log")
for i in "${!NAMES[@]}"; do
  p="${NAMES[$i]}"; [[ -f "$STATE/polaris-$p.env" ]] && continue
  id=$(echo "${CREDS[$i]}" | $PY -c 'import sys,json;print(json.load(sys.stdin)["clientId"])')
  sec=$(echo "${CREDS[$i]}" | $PY -c 'import sys,json;print(json.load(sys.stdin)["clientSecret"])')
  printf 'CLIENT_ID=%s\nCLIENT_SECRET=%s\n' "$id" "$sec" > "$STATE/polaris-$p.env"
done
for p in connect spark trino; do
  [[ -f "$STATE/polaris-$p.env" ]] || { echo "HATA: $p credential yok (setup-apply.log'da JSON bulunamadı); principal'ı silip setup apply'ı tekrar çalıştır"; exit 1; }
  set -a; source "$STATE/polaris-$p.env"; set +a
  code=$(curl -s -o /dev/null -w "%{http_code}" -u "$CLIENT_ID:$CLIENT_SECRET" -d "grant_type=client_credentials&scope=PRINCIPAL_ROLE:ALL" http://localhost:8181/api/catalog/v1/oauth/tokens)
  [[ "$code" == "200" ]] || { echo "HATA: $p için token alınamadı (HTTP $code)"; exit 1; }
  echo "credential doğrulandı: $p"
done
set -a; source "$STATE/polaris-connect.env"; set +a
kubectl -n lakehouse create secret generic polaris-connect --from-literal=credential="$CLIENT_ID:$CLIENT_SECRET" --dry-run=client -o yaml | kubectl apply -f -
set -a; source "$STATE/polaris-spark.env"; set +a
kubectl -n spark create secret generic polaris-spark --from-literal=credential="$CLIENT_ID:$CLIENT_SECRET" --dry-run=client -o yaml | kubectl apply -f -
"$SELF" --check
