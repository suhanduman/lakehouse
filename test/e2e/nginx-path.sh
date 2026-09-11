#!/usr/bin/env bash
# e2e F3 nginx yolu: dış listener var -> Fluent Bit (küme içi, aynı SASL_SSL/PEM config) -> nginx.access -> sink -> nginx_raw.access_log
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"; NS=lakehouse
# shellcheck source=/dev/null
source "$ROOT/test/e2e/lib.sh"
echo "== dış listener"
kubectl -n "$NS" get svc lakehouse-kafka-external-bootstrap -o jsonpath='{.spec.type} {.spec.ports[0].nodePort}{"\n"}' | grep -E "^NodePort [0-9]+"
kubectl -n "$NS" wait kafkatopic/nginx.access --for=condition=Ready --timeout=300s
kubectl -n "$NS" wait kafkaconnector/sink-nginx --for=condition=Ready --timeout=600s
echo "== fluent-bit"
kubectl -n "$NS" create configmap fluent-bit-config --from-file="$ROOT/agents/fluent-bit/fluent-bit.conf" --from-file="$ROOT/agents/fluent-bit/parsers.conf" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
kubectl apply -f "$ROOT/test/e2e/nginx-fixture.yaml" >/dev/null
kubectl -n "$NS" rollout status deploy/fluent-bit --timeout=300s
echo "== Bronze nginx_raw.access_log"
verify nginx_raw.access_log 3 --exact --wait 600 'remote=10.0.0.1:code=200' 'code=404' '~ts=2026-09-11 13:52:24'
echo "E2E F3 NGINX OK"
