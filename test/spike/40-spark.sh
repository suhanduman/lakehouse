#!/usr/bin/env bash
# S4: spark-operator 2.5.2 + Spark 4.1 (resmi imaj, Maven packages) -> MoR MERGE spike.
set -euo pipefail
SELF="$(cd "$(dirname "$0")" && pwd)/$(basename "$0")"
cd "$(dirname "$0")"; STATE=.state; mkdir -p "$STATE"

if [[ "${1:-}" == "--check" ]]; then
  st=$(kubectl -n spark get sparkapplication merge-spike -o jsonpath='{.status.applicationState.state}')
  [[ "$st" == "COMPLETED" ]] || { echo "durum: $st"; kubectl -n spark logs merge-spike-driver --tail=60 2>&1 | tail -60; exit 1; }
  kubectl -n spark logs merge-spike-driver | grep -E "BRONZE_OPS|INC|SILVER|POSITION_DELETE|TIME_TRAVEL|ANSI_MODE|S4_OK" | tee "$STATE/s4-results.txt"
  grep -q S4_OK "$STATE/s4-results.txt"; echo "OK: S4"; exit 0
fi

helm repo add spark-operator https://kubeflow.github.io/spark-operator >/dev/null 2>&1 || true
helm repo update spark-operator >/dev/null
helm upgrade --install spark-operator spark-operator/spark-operator --version 2.5.2 -n spark \
  --set 'spark.jobNamespaces={spark}' --wait --timeout 5m
SA=$(kubectl -n spark get sa -o name | grep -E "spark-operator-spark|^serviceaccount/spark$" | head -1 | cut -d/ -f2)
echo "Spark job SA: $SA"
# Polaris credential'ını sparkConf'a enjekte et (Secret -> conf); prod'da glue chart aynı işi values ile yapar.
CRED=$(kubectl -n spark get secret polaris-spark -o jsonpath='{.data.credential}' | base64 -d)
kubectl -n spark create configmap merge-spike-job --from-file=40-spark/merge_spike.py --dry-run=client -o yaml | kubectl apply -f -
kubectl -n spark delete sparkapplication merge-spike --ignore-not-found
sed -e "s#spark.sql.catalog.lakehouse.scope: PRINCIPAL_ROLE:ALL#spark.sql.catalog.lakehouse.scope: PRINCIPAL_ROLE:ALL\n    spark.sql.catalog.lakehouse.credential: ${CRED}#" \
    -e "s#serviceAccount: spark-operator-spark#serviceAccount: ${SA}#" 40-spark/sparkapp.yaml | kubectl apply -f -
T0=$(date +%s)
for i in $(seq 1 150); do
  st=$(kubectl -n spark get sparkapplication merge-spike -o jsonpath='{.status.applicationState.state}' 2>/dev/null || true)
  [[ "$st" == "COMPLETED" || "$st" == "FAILED" ]] && break; sleep 10
done
echo "S4 çalışma süresi (packages çözümü dahil): $(( $(date +%s) - T0 )) s" | tee "$STATE/s4-runtime.txt"
"$SELF" --check
