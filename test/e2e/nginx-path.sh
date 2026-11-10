#!/usr/bin/env bash
# e2e F3 nginx yolu: dış listener var -> Fluent Bit (küme içi, aynı SASL_SSL/PEM config) -> nginx.access -> sink -> nginx_raw.access_log
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"; NS=lakehouse
# shellcheck source=/dev/null
source "$ROOT/test/e2e/lib.sh"
echo "== dış listener"
# OpenShift: Route (passthrough, 443); kind/vanilla: NodePort (glue kafka.yaml listener type)
if kubectl -n "$NS" get route lakehouse-kafka-bootstrap >/dev/null 2>&1; then
  kubectl -n "$NS" get route lakehouse-kafka-bootstrap -o jsonpath='Route {.spec.host} {.spec.tls.termination}{"\n"}' | grep -E "^Route [^ ]+ passthrough$"
else
  kubectl -n "$NS" get svc lakehouse-kafka-external-bootstrap -o jsonpath='{.spec.type} {.spec.ports[0].nodePort}{"\n"}' | grep -E "^NodePort [0-9]+"
fi
kubectl -n "$NS" wait kafkatopic/nginx.access --for=condition=Ready --timeout=300s
kubectl -n "$NS" wait kafkaconnector/sink-nginx --for=condition=Ready --timeout=600s
echo "== fluent-bit"
kubectl -n "$NS" create configmap fluent-bit-config --from-file="$ROOT/agents/fluent-bit/fluent-bit.conf" --from-file="$ROOT/agents/fluent-bit/parsers.conf" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
kubectl apply -f "$ROOT/test/e2e/nginx-fixture.yaml" >/dev/null
kubectl -n "$NS" rollout status deploy/fluent-bit --timeout=300s
echo "== Bronze nginx_raw.access_log"
# 4. satır nginx'in varsayılan "main" biçimi (sonda x_forwarded_for): ayrıştırılmalı, remote/path dolu gelmeli
verify nginx_raw.access_log 4 --exact --wait "$(sink_wait sink-nginx)" 'remote=10.0.0.1:code=200:server=e2e-nginx-1' 'code=404' '~ts=2026-09-11 13:52:24' 'remote=10.0.0.4:path=/main-format'
echo "E2E F3 NGINX OK"
