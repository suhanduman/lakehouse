#!/usr/bin/env bash
# e2e F2 pg yolu (spec §9): fixture -> connector Ready -> Bronze -> MERGE -> UPDATE/DELETE -> MERGE -> bakım. run.sh sonunda çağrılır.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"; NS=lakehouse
PSQL() { kubectl -n "$NS" exec demo-pg-1 -c postgres -- psql -U postgres -d shop -Atc "$1"; }

verify() {  # verify <ns.table> <min_rows> [ek argümanlar…]  -> küme içi pyiceberg Job
  local name; name="verify-$(date +%s)-$RANDOM"
  kubectl -n "$NS" create configmap lakehouse-verify --from-file="$ROOT/test/e2e/verify/verify.py" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
  sed -e "s#JOBNAME#$name#" -e "s#VERIFY_ARGS#$*#" "$ROOT/test/e2e/verify/job.yaml" | kubectl apply -f - >/dev/null
  kubectl -n "$NS" wait --for=condition=complete "job/$name" --timeout=900s >/dev/null || { kubectl -n "$NS" logs "job/$name" --tail=40; return 1; }
  kubectl -n "$NS" logs "job/$name" | grep "^OK"
}

run_spark_once() {  # run_spark_once <ScheduledSparkApplication adı> -> template'ten tek seferlik SparkApplication (plan P9)
  local ssa="$1" app="e2e-$1" st=""
  kubectl -n "$NS" get scheduledsparkapplication "$ssa" -o json \
    | jq --arg n "$app" '{apiVersion:"sparkoperator.k8s.io/v1beta2",kind:"SparkApplication",metadata:{name:$n,namespace:.metadata.namespace},spec:.spec.template}' \
    | kubectl apply -f - >/dev/null
  for _ in $(seq 1 120); do
    st=$(kubectl -n "$NS" get sparkapplication "$app" -o jsonpath='{.status.applicationState.state}' 2>/dev/null || true)
    [[ "$st" == "COMPLETED" || "$st" == "FAILED" ]] && break; sleep 10
  done
  kubectl -n "$NS" logs "$app-driver" --tail=200 2>/dev/null | grep -E "MERGE_OK|MAINT_OK|HATA|Exception|->" | tail -15 || true
  [[ "$st" == "COMPLETED" ]] || { echo "SparkApplication $app: $st"; kubectl -n "$NS" describe sparkapplication "$app" | tail -20; return 1; }
  kubectl -n "$NS" delete sparkapplication "$app" --ignore-not-found >/dev/null
}

echo "== fixture (CNPG demo-pg)"
kubectl apply -f "$ROOT/test/e2e/pg-fixture.yaml" >/dev/null
kubectl -n "$NS" wait --for=condition=Ready cluster/demo-pg --timeout=600s
[[ "$(PSQL 'select count(*) from public.orders')" == "3" ]]

echo "== connector'lar"
kubectl -n "$NS" wait kafkaconnector/dbz-shop --for=condition=Ready --timeout=600s
kubectl -n "$NS" wait kafkaconnector/sink-shop --for=condition=Ready --timeout=600s

echo "== Bronze (ilk snapshot: 3 satır op=I)"
verify shop_raw.orders 3 --wait 600 'id=1:status=new' 'id=2:status=paid'

echo "== MERGE #1"
run_spark_once silver-merge
verify shop.orders 3 --wait 60 'id=1:status=new' 'id=2:status=paid' 'id=3:status=new'

echo "== kaynakta UPDATE/DELETE"
PSQL "update public.orders set status='shipped' where id=1; delete from public.orders where id=2;" >/dev/null
verify shop_raw.orders 5 --wait 600 'id=1:status=shipped'

echo "== MERGE #2 (artımlı)"
run_spark_once silver-merge
verify shop.orders 2 --wait 60 'id=1:status=shipped' '!id=2' 'id=3:status=new'

echo "== bakım"
run_spark_once maint-position-deletes
run_spark_once maint-compact
run_spark_once maint-expire-orphan-ttl
verify shop.orders 2 'id=1:status=shipped' '!id=2'
echo "E2E F2 OK"
