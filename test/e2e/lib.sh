#!/usr/bin/env bash
# e2e ortak yardımcılar (pg/mongo/nginx yolları). Çağıran script ROOT ve NS tanımlar, sonra: source "$ROOT/test/e2e/lib.sh"

# --- Ortam: geliştirme (kind, CI) varsayılanları; üretim/OpenShift kabulünde scripts/acceptance.sh ezer ---
# ArgoCD ad alanı: kind'da argocd, OpenShift GitOps'ta openshift-gitops.
ARGOCD_NS="${ARGOCD_NS:-argocd}"
# Kimlik: kind'da Keycloak geliştirme kullanıcıları + parola akışı; üretimde AD test hesapları + tarayıcı akışı
# (authcode — Keycloak giriş formu doldurulur, üretimde parola akışı bilerek kapalı). Hesaplar AD'de şu gruplarda
# olmalıdır: analyst -> lakehouse-analysts, user -> lakehouse-users, nogroup -> üç lakehouse grubunun hiçbiri.
ACC_FLOW="${ACC_FLOW:-password}"
ACC_ANALYST_USER="${ACC_ANALYST_USER:-analyst1}"; ACC_ANALYST_PASS="${ACC_ANALYST_PASS:-analyst1-dev}"
ACC_USER_USER="${ACC_USER_USER:-user1}";          ACC_USER_PASS="${ACC_USER_PASS:-user1-dev}"
ACC_NOGROUP_USER="${ACC_NOGROUP_USER:-nogroup1}"; ACC_NOGROUP_PASS="${ACC_NOGROUP_PASS:-nogroup1-dev}"
# Trino servis hesabı (trino-service-accounts Secret'ındaki anahtar): kind'da e2e; üretimde e2e YOK, aynı yetkili
# zeppelin kullanılır (rules.json: superset|zeppelin|e2e aynı satırlarda).
ACC_SA="${ACC_SA:-e2e}"
export ARGOCD_NS ACC_FLOW ACC_ANALYST_USER ACC_ANALYST_PASS ACC_USER_USER ACC_USER_PASS ACC_NOGROUP_USER ACC_NOGROUP_PASS ACC_SA

acc_env() {  # kimlik değerlerini küme içi kontrol Job'larının okuyacağı Secret'a yazar (Job tanımına düz metin girmesin)
  # TRINO_REDIRECT: tarayıcı akışında Keycloak'a verilen geri dönüş adresi (realm'de trino istemcisine kayıtlı olan)
  local host; host=$(kubectl -n "$NS" get route trino -o jsonpath='{.spec.host}' 2>/dev/null || true)
  kubectl -n "$NS" create secret generic e2e-acceptance \
    --from-literal=TRINO_REDIRECT="https://${host:-trino-$NS.local}/oauth2/callback" \
    --from-literal=ACC_FLOW="$ACC_FLOW" --from-literal=ACC_SA="$ACC_SA" \
    --from-literal=ACC_ANALYST_USER="$ACC_ANALYST_USER" --from-literal=ACC_ANALYST_PASS="$ACC_ANALYST_PASS" \
    --from-literal=ACC_USER_USER="$ACC_USER_USER" --from-literal=ACC_USER_PASS="$ACC_USER_PASS" \
    --dry-run=client -o yaml | kubectl apply -f - >/dev/null
}

s3_endpoint() {  # istemcilerin kullandığı S3 adresi — kümeden okunur (kind: MinIO, üretim: müşteri S3'ü); tek kaynak glue s3.endpoint
  kubectl -n "$NS" get scheduledsparkapplication silver-merge \
    -o jsonpath='{.spec.template.sparkConf.spark\.sql\.catalog\.lakehouse\.s3\.endpoint}'
}

run_check_job() {  # run_check_job <dizin> <ad> <pyfile> [argümanlar…] -> dizin/pyfile ConfigMap lakehouse-<ad>, dizin/job.yaml (JOBNAME, CHECK_ARGS, S3_ENDPOINT_VALUE) Job; "^OK" satırları
  local dir="$1" base="$2" pyfile="$3"; shift 3; local q="" a name s3; name="$base-$(date +%s)-$RANDOM"
  kubectl -n "$NS" create configmap "lakehouse-$base" --from-file="$dir/$pyfile" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
  acc_env
  s3=$(s3_endpoint); [[ -n "$s3" ]] || { echo "HATA s3 adresi okunamadı (scheduledsparkapplication/silver-merge yok: demo kaynaklar açık mı?)"; return 1; }
  # her argüman tek tırnakla: Job komutu /bin/sh -c ile çalışır, boşluklu koşul ('~ts=2026-09-11 13:52:24') bölünmesin (F3 canlı)
  # koşullar repo-içi sabitlerdir; ' ya da # içeren koşul desteklenmez
  for a in "$@"; do q+="'$a' "; done
  sed -e "s#JOBNAME#$name#" -e "s#CHECK_ARGS#$q#" -e "s#S3_ENDPOINT_VALUE#$s3#" "$dir/job.yaml" | kubectl apply -f - >/dev/null
  kubectl -n "$NS" wait --for=condition=complete "job/$name" --timeout=900s >/dev/null || { kubectl -n "$NS" logs "job/$name" --tail=60; return 1; }
  kubectl -n "$NS" logs "job/$name" | grep "^OK"
}

verify() {  # verify <ns.table> <min_rows> [ek argümanlar…]  -> küme içi pyiceberg Job (run_check_job'ın ince sarmalayıcısı, DRY)
  run_check_job "$ROOT/test/e2e/verify" verify verify.py "$@"
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
  kubectl -n "$NS" logs "$app-driver" --tail=2000 2>/dev/null | grep -E "MERGE_OK|MAINT_OK|MONGO_OK|HATA|Exception|->|incremental|FALLBACK|rewrite_manifests|[^[:alpha:]]full[^[:alpha:]]" | tail -15 || true
  [[ "$st" == "COMPLETED" ]] || { echo "SparkApplication $app: $st"; kubectl -n "$NS" describe sparkapplication "$app" | tail -20; return 1; }
  kubectl -n "$NS" delete sparkapplication "$app" --ignore-not-found >/dev/null
}
