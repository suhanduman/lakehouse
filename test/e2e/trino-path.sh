#!/usr/bin/env bash
# e2e F4 Trino yolu: pg/mongo/nginx yollarından SONRA (Silver: shop.orders 3 [id1 shipped, id3 new, id4 new] -> öğrenci 2; crm.customers 3).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"; NS=lakehouse
# shellcheck source=/dev/null
source "$ROOT/test/e2e/lib.sh"
echo "== Trino hazır (polaris-trino Secret'ı polaris-setup ile geldi)"
kubectl -n "$NS" rollout status deploy/trino-coordinator --timeout=900s
echo "== servis hesabı + OIDC (analyst1/student1) + rules.json + sandbox"
run_check_job "$ROOT/test/e2e/trino-check" trino-check check.py 3 3 2
echo "E2E F4 TRINO OK"
