#!/usr/bin/env bash
# e2e ortak yardımcılar (pg/mongo/nginx yolları). Çağıran script ROOT ve NS tanımlar, sonra: source "$ROOT/test/e2e/lib.sh"

verify() {  # verify <ns.table> <min_rows> [ek argümanlar…]  -> küme içi pyiceberg Job
  local name; name="verify-$(date +%s)-$RANDOM"
  kubectl -n "$NS" create configmap lakehouse-verify --from-file="$ROOT/test/e2e/verify/verify.py" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
  # her argüman tek tırnakla: Job komutu /bin/sh -c ile çalışır, boşluklu koşul ('~ts=2026-09-11 13:52:24') bölünmesin (F3 canlı)
  # koşullar repo-içi sabitlerdir; ' ya da # içeren koşul desteklenmez
  local q="" a; for a in "$@"; do q+="'$a' "; done
  sed -e "s#JOBNAME#$name#" -e "s#VERIFY_ARGS#$q#" "$ROOT/test/e2e/verify/job.yaml" | kubectl apply -f - >/dev/null
  kubectl -n "$NS" wait --for=condition=complete "job/$name" --timeout=900s >/dev/null || { kubectl -n "$NS" logs "job/$name" --tail=40; return 1; }
  kubectl -n "$NS" logs "job/$name" | grep "^OK"
}

run_spark_once() {  # run_spark_once <ScheduledSparkApplication adı> -> template'ten tek seferlik SparkApplication (plan P9)
  local ssa="$1" app="e2e-$1" st=""
  # e2e tek seferlik koşusu dev cron'la (:00/:30) çakışmasın — mongo-bronze append idempotent değil (F3 notu 4)
  kubectl -n "$NS" patch scheduledsparkapplication "$ssa" --type=merge -p '{"spec":{"suspend":true}}' >/dev/null
  # önceki koşudan kalan (FAILED) aynı adlı SparkApplication yeniden koşmaz: önce sil
  kubectl -n "$NS" delete sparkapplication "$app" --ignore-not-found --wait=true >/dev/null 2>&1 || true
  kubectl -n "$NS" get scheduledsparkapplication "$ssa" -o json \
    | jq --arg n "$app" '{apiVersion:"sparkoperator.k8s.io/v1beta2",kind:"SparkApplication",metadata:{name:$n,namespace:.metadata.namespace},spec:.spec.template}' \
    | kubectl apply -f - >/dev/null
  for _ in $(seq 1 120); do
    st=$(kubectl -n "$NS" get sparkapplication "$app" -o jsonpath='{.status.applicationState.state}' 2>/dev/null || true)
    [[ "$st" == "COMPLETED" || "$st" == "FAILED" ]] && break; sleep 10
  done
  # merge modu da log'a düşsün: incremental / FALLBACK bounded / FALLBACK full (artımlı yolun gerçekten koştuğunun kanıtı).
  # 'full' sözcük sınırıyla: Spark'ın "…successfully…" satırları tail -15'i doldurup asıl satırları kaydırmasın.
  # tail=2000: birden çok pipeline'da (shop + crm) ilk pipeline'ın satırı 200 satırlık Spark INFO'nun dışına kayıyordu (F3 canlı)
  kubectl -n "$NS" logs "$app-driver" --tail=2000 2>/dev/null | grep -E "MERGE_OK|MAINT_OK|MONGO_OK|HATA|Exception|->|incremental|FALLBACK|[^[:alpha:]]full[^[:alpha:]]" | tail -15 || true
  [[ "$st" == "COMPLETED" ]] || { echo "SparkApplication $app: $st"; kubectl -n "$NS" describe sparkapplication "$app" | tail -20; return 1; }
  kubectl -n "$NS" delete sparkapplication "$app" --ignore-not-found >/dev/null
}
