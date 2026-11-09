#!/usr/bin/env bash
# Kabul testi orkestratörü (şartname kabul maddeleri — docs/90-referans/kabul-testleri.md).
#   scripts/acceptance.sh [--ns lakehouse] [--mon-ns monitoring] [--velero-ns velero] [--argocd-ns argocd] [--skip dr]
#
# ZATEN KURULU bir kümede koşar: kind/bootstrap adımı YOKTUR (onlar test/e2e/run.sh'ta).
# Yaptığı tek şey: kaynak DB fixture'larını uygula -> polaris-setup.sh (idempotent) -> dokuz e2e yolunu
# SIRAYLA koştur -> "E2E … OK" satırlarından KABUL özeti bas. Yeni bir kontrol/iddia EKLEMEZ; kanıtı üreten
# kod e2e yol script'lerinin kendisidir.
#
# Ön koşullar: kubectl (küme erişimi), jq, python3, `polaris` CLI (pip install 'apache-polaris==1.7.0') ve
# glue'da demo kaynakların açık olması (`sources`/`pipelines`/`nginx.enabled` — platform/values/glue-dev.yaml
# kalıbı). Kaynaklar kapalıysa pg/mongo/nginx yolları bekledikleri connector'ları bulamaz.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
NS=lakehouse
MON_NS="${MON_NS:-monitoring}"
VELERO_NS="${VELERO_NS:-velero}"
ARGOCD_NS="${ARGOCD_NS:-argocd}"
SKIP=""

usage() {
  cat <<'EOF'
Kullanım: scripts/acceptance.sh [seçenekler]

  --ns <ad>          lakehouse namespace'i (varsayılan: lakehouse)
  --mon-ns <ad>      Prometheus/Grafana namespace'i (varsayılan: monitoring; monitoring-path.sh MON_NS ile okur)
  --velero-ns <ad>   Velero/OADP namespace'i (varsayılan: velero; OpenShift: openshift-adp; dr-path.sh VELERO_NS ile okur)
  --argocd-ns <ad>   ArgoCD namespace'i (varsayılan: argocd; OpenShift GitOps: openshift-gitops)
  --skip <yol>       bir yolu atla (tekrarlanabilir; ör. --skip dr: OADP kurulu değilse). Özet atlananları yazar.
  -h, --help         bu yardım

Ortam değişkeni dikişleri (test/e2e/monitoring-path.sh çocuk sürece kalıtımla geçer, forward gerekmez):
  PROM_STS, PROM_SVC, GRAFANA_SVC, GRAFANA_SECRET, GRAFANA_SKIP=1 (OpenShift UWM: GRAFANA_SKIP=1 + UWM
  StatefulSet/Service adları).

Kimlik (üretim/OpenShift — ayrıntı docs/90-referans/kabul-testleri.md §1):
  ACC_FLOW=authcode   Keycloak tarayıcı akışı (üretimde parola akışı kapalıdır); kind'da varsayılan password
  ACC_ANALYST_USER/PASS  lakehouse-analysts üyesi AD test hesabı
  ACC_USER_USER/PASS     lakehouse-users üyesi AD test hesabı
  ACC_NOGROUP_USER/PASS  hiçbir lakehouse grubunda olmayan AD test hesabı
  ACC_SA=zeppelin        Trino servis hesabı (kind'da e2e)
  ACC_CA_FILE            router sertifikasının kökü (tarayıcı girişleri için; yoksa sistem kökleri)

Sıra: pg-fixture + mongo-fixture -> polaris-setup.sh -> pg, mongo, nginx, trino, superset, jupyterhub,
      zeppelin, monitoring, dr yolları -> KABUL özeti.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --ns) NS="$2"; shift 2;;
    --mon-ns) MON_NS="$2"; shift 2;;
    --velero-ns) VELERO_NS="$2"; shift 2;;
    --argocd-ns) ARGOCD_NS="$2"; shift 2;;
    --skip) SKIP+=" ${2%-path.sh}-path.sh"; shift 2;;
    -h|--help) usage; exit 0;;
    *) echo "bilinmeyen argüman: $1"; usage; exit 2;;
  esac
done
# monitoring-path.sh ${MON_NS:-monitoring}, dr-path.sh ${VELERO_NS:-velero} okur -> alt süreçlere aktar
export MON_NS VELERO_NS ARGOCD_NS

# e2e yol script'leri ve fixture manifest'leri `lakehouse` ns'ini SABİT kullanır (NS=lakehouse / metadata.namespace).
# --ns yalnız bu script'in kendi adımları (ön kontrol, polaris-setup) içindir; farklıysa yüksek sesle uyar.
if [[ "$NS" != "lakehouse" ]]; then
  echo "UYARI --ns=$NS: e2e yol script'leri ve fixture'lar 'lakehouse' ns'ine sabitlidir (test/e2e/*.sh, *-fixture.yaml)."
  echo "UYARI Farklı bir namespace için o dosyalar da uyarlanmalıdır; bu koşu büyük olasılıkla başarısız olur."
fi

LOG="$(mktemp)"
FAILED=""
PATHS="pg-path.sh mongo-path.sh nginx-path.sh trino-path.sh superset-path.sh jupyterhub-path.sh zeppelin-path.sh monitoring-path.sh dr-path.sh"
RUN=""; for p in $PATHS; do [[ " $SKIP " == *" $p "* ]] || RUN+=" $p"; done
TOTAL=$(wc -w <<<"$RUN" | tr -d ' ')
resume_ssa() {  # run_spark_once (test/e2e/lib.sh) ürünün zamanlanmış işlerini askıya alır; koşu NASIL biterse bitsin geri aç.
  # Unutulursa Silver güncellenmez, bakım durur (eski sürümde kitap bunu kurulumcuya bırakıyordu).
  local s
  for s in $(kubectl -n "$NS" get scheduledsparkapplication -o jsonpath='{.items[*].metadata.name}' 2>/dev/null); do
    [[ "$s" == e2e-* ]] && continue
    kubectl -n "$NS" patch scheduledsparkapplication "$s" --type=merge -p '{"spec":{"suspend":false}}' >/dev/null 2>&1 || true
  done
  echo "zamanlanmış Spark işleri geri açıldı: $(kubectl -n "$NS" get scheduledsparkapplication \
    -o jsonpath='{range .items[*]}{.metadata.name}={.spec.suspend} {end}' 2>/dev/null)"
}
summary() {
  local rc=$? n
  resume_ssa
  echo
  echo "=== KABUL ÖZETİ ==="
  grep -E '^E2E .* OK$' "$LOG" || echo "(hiç 'E2E … OK' satırı yok)"
  n=$(grep -cE '^E2E .* OK$' "$LOG" || true)
  [[ -n "$SKIP" ]] && echo "ATLANAN:$SKIP"
  if [[ -n "$FAILED" ]]; then
    echo "KABUL BAŞARISIZ: $FAILED (tamamlanan yol: $n/$TOTAL)"
  elif [[ "$n" -ge "$TOTAL" ]]; then
    echo "KABUL: $TOTAL/$TOTAL yol geçti (ns=$NS, mon-ns=$MON_NS, velero-ns=$VELERO_NS, argocd-ns=$ARGOCD_NS)"
  else
    echo "KABUL EKSİK: $n/$TOTAL yol (ns=$NS, mon-ns=$MON_NS, velero-ns=$VELERO_NS)"
    [[ "$rc" == 0 ]] && rc=1
  fi
  rm -f "$LOG"
  exit "$rc"
}
trap summary EXIT

echo "== ön kontrol"
kubectl get namespace "$NS" >/dev/null
command -v polaris >/dev/null || { echo "polaris CLI yok: pip install 'apache-polaris==1.7.0'"; exit 1; }
# Demo kaynaklar (sources/pipelines/nginx) site değerlerinde açık mı? Kapalıysa pg/mongo/nginx yolları bağlayıcı
# bulamaz ve dakikalarca bekledikten sonra düşer — baştan, ne yapılacağını söyleyerek dur.
kubectl -n "$NS" get kafkaconnector dbz-shop >/dev/null 2>&1 || {
  echo "HATA demo kaynaklar açık değil (kafkaconnector/dbz-shop yok)."
  echo "     platform/values/site/glue.yaml'a kabul bloğunu ekleyip itin: docs/90-referans/kabul-testleri.md §1."
  exit 1; }

echo "== kaynak DB fixture'ları (pg + mongo)"
kubectl apply -f "$ROOT/test/e2e/pg-fixture.yaml"
kubectl apply -f "$ROOT/test/e2e/mongo-fixture.yaml"

echo "== polaris-setup (idempotent)"
"$ROOT/scripts/polaris-setup.sh" --setup "$ROOT/platform/polaris/setup.yaml" --ns "$NS"
# Doğrulama Job'ları (test/e2e/verify) tabloları connect principal'ının kimliğiyle okur (run.sh ile aynı)
CRED=$(kubectl -n "$NS" get secret polaris-connect -o jsonpath='{.data.credential}' | base64 -d)
kubectl -n "$NS" create secret generic polaris-smoke-cred --from-literal=CLIENT_ID="${CRED%%:*}" \
  --from-literal=CLIENT_SECRET="${CRED#*:}" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
unset CRED

for p in $RUN; do
  echo
  echo "=== $p"
  if ! "$ROOT/test/e2e/$p" 2>&1 | tee -a "$LOG"; then FAILED="$p"; exit 1; fi
done
