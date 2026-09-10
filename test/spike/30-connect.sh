#!/usr/bin/env bash
# S2/S3: Strimzi 1.2 + Kafka + KafkaConnect(build) + Debezium pg -> Iceberg sink -> Bronze (Polaris/MinIO).
set -euo pipefail
SELF="$(cd "$(dirname "$0")" && pwd)/$(basename "$0")"
cd "$(dirname "$0")"
STATE=.state; mkdir -p "$STATE"

verify() { # verify <ns> <table> <min_rows>  -- verify.py'yi KÜME İÇİNDE koşturur (S1 bulgusu: sunucu s3.endpoint'i küme-içi ad döndürür)
  kubectl -n polaris create configmap bronze-verify --from-file=30-connect/verify.py --dry-run=client -o yaml | kubectl apply -f - >/dev/null
  kubectl -n polaris delete job bronze-verify --ignore-not-found >/dev/null
  sed -e "s/NS/$1/" -e "s/TBL/$2/" -e "s/MINROWS/$3/" 30-connect/verify-job.yaml | kubectl apply -f - >/dev/null
  kubectl -n polaris wait --for=condition=complete job/bronze-verify --timeout=420s >/dev/null || { kubectl -n polaris logs job/bronze-verify --tail=15; return 1; }
  kubectl -n polaris logs job/bronze-verify | grep -E "^(schema|partition spec|rows=|örnek|OK)"
}

if [[ "${1:-}" == "--check" ]]; then
  kubectl -n lakehouse wait kafkaconnector/dbz-shop --for=condition=Ready --timeout=10s >/dev/null
  kubectl -n lakehouse wait kafkaconnector/iceberg-shop --for=condition=Ready --timeout=10s >/dev/null
  verify shop_raw orders "${2:-3}"
  echo "OK: S2 Bronze dolu"; exit 0
fi
if [[ "${1:-}" == "--verify" ]]; then verify "$2" "$3" "$4"; exit $?; fi

helm upgrade --install strimzi oci://quay.io/strimzi-helm/strimzi-kafka-operator --version 1.2.0 \
  -n lakehouse --set watchNamespaces="{lakehouse}" --wait --timeout 5m
kubectl apply -f 30-connect/kafka.yaml
kubectl -n lakehouse wait kafka/lakehouse --for=condition=Ready --timeout=600s
kubectl apply -f 30-connect/rbac.yaml

[[ -f "$STATE/connect-image" ]] || echo "ttl.sh/lakehouse-connect-$(uuidgen | tr 'A-Z' 'a-z'):24h" > "$STATE/connect-image"
IMG=$(cat "$STATE/connect-image")
sed "s#ttl.sh/lakehouse-connect-spike-CHANGEME:24h#${IMG}#" 30-connect/connect.yaml | kubectl apply -f -
echo "build başladı: $(date +%T)"; T0=$(date +%s)
kubectl -n lakehouse wait kafkaconnect/connect --for=condition=Ready --timeout=1800s
echo "build+ready süresi: $(( $(date +%s) - T0 )) s" | tee "$STATE/s2-build-time.txt"

kubectl apply -f 30-connect/pg-source.yaml -f 30-connect/iceberg-sink.yaml
# yeniden build sonrasında FAILED kalmış task'ları Strimzi'nin kendi mekanizmasıyla yeniden başlat
for c in dbz-shop iceberg-shop; do kubectl -n lakehouse annotate kafkaconnector "$c" strimzi.io/restart="true" --overwrite >/dev/null; done
sleep 20
kubectl -n lakehouse wait kafkaconnector/dbz-shop --for=condition=Ready --timeout=300s
kubectl -n lakehouse wait kafkaconnector/iceberg-shop --for=condition=Ready --timeout=300s
echo "commit bekleniyor (30 s aralık)…"; sleep 75
"$SELF" --check
