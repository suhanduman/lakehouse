#!/usr/bin/env bash
# Kabul testi orkestratörü (şartname kabul maddeleri — runbooks/acceptance-tests.md).
#   runbooks/scripts/acceptance.sh [--ns lakehouse] [--mon-ns monitoring] [--velero-ns velero]
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
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
NS=lakehouse
MON_NS="${MON_NS:-monitoring}"
VELERO_NS="${VELERO_NS:-velero}"

usage() {
  cat <<'EOF'
Kullanım: runbooks/scripts/acceptance.sh [seçenekler]

  --ns <ad>          lakehouse namespace'i (varsayılan: lakehouse)
  --mon-ns <ad>      Prometheus/Grafana namespace'i (varsayılan: monitoring; monitoring-path.sh MON_NS ile okur)
  --velero-ns <ad>   Velero/OADP namespace'i (varsayılan: velero; OpenShift: openshift-adp; dr-path.sh VELERO_NS ile okur)
  -h, --help         bu yardım

Sıra: pg-fixture + mongo-fixture -> polaris-setup.sh -> pg, mongo, nginx, trino, superset, jupyterhub,
      zeppelin, monitoring, dr yolları -> KABUL özeti.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --ns) NS="$2"; shift 2;;
    --mon-ns) MON_NS="$2"; shift 2;;
    --velero-ns) VELERO_NS="$2"; shift 2;;
    -h|--help) usage; exit 0;;
    *) echo "bilinmeyen argüman: $1"; usage; exit 2;;
  esac
done
# monitoring-path.sh ${MON_NS:-monitoring}, dr-path.sh ${VELERO_NS:-velero} okur -> alt süreçlere aktar
export MON_NS VELERO_NS

# e2e yol script'leri ve fixture manifest'leri `lakehouse` ns'ini SABİT kullanır (NS=lakehouse / metadata.namespace).
# --ns yalnız bu script'in kendi adımları (ön kontrol, polaris-setup) içindir; farklıysa yüksek sesle uyar.
if [[ "$NS" != "lakehouse" ]]; then
  echo "UYARI --ns=$NS: e2e yol script'leri ve fixture'lar 'lakehouse' ns'ine sabitlidir (test/e2e/*.sh, *-fixture.yaml)."
  echo "UYARI Farklı bir namespace için o dosyalar da uyarlanmalıdır; bu koşu büyük olasılıkla başarısız olur."
fi

LOG="$(mktemp)"
FAILED=""
summary() {
  local rc=$? n
  echo
  echo "=== KABUL ÖZETİ ==="
  grep -E '^E2E .* OK$' "$LOG" || echo "(hiç 'E2E … OK' satırı yok)"
  n=$(grep -cE '^E2E .* OK$' "$LOG" || true)
  if [[ -n "$FAILED" ]]; then
    echo "KABUL BAŞARISIZ: $FAILED (tamamlanan yol: $n/9)"
  elif [[ "$n" -ge 9 ]]; then
    echo "KABUL: 9/9 yol geçti (ns=$NS, mon-ns=$MON_NS, velero-ns=$VELERO_NS)"
  else
    echo "KABUL EKSİK: $n/9 yol (ns=$NS, mon-ns=$MON_NS, velero-ns=$VELERO_NS)"
    [[ "$rc" == 0 ]] && rc=1
  fi
  rm -f "$LOG"
  exit "$rc"
}
trap summary EXIT

echo "== ön kontrol"
kubectl get namespace "$NS" >/dev/null
command -v polaris >/dev/null || { echo "polaris CLI yok: pip install 'apache-polaris==1.7.0'"; exit 1; }

echo "== kaynak DB fixture'ları (pg + mongo)"
kubectl apply -f "$ROOT/test/e2e/pg-fixture.yaml"
kubectl apply -f "$ROOT/test/e2e/mongo-fixture.yaml"

echo "== polaris-setup (idempotent)"
"$ROOT/runbooks/scripts/polaris-setup.sh" --setup "$ROOT/platform/polaris/setup.yaml" --ns "$NS"

for p in pg-path.sh mongo-path.sh nginx-path.sh trino-path.sh superset-path.sh \
         jupyterhub-path.sh zeppelin-path.sh monitoring-path.sh dr-path.sh; do
  echo
  echo "=== $p"
  if ! "$ROOT/test/e2e/$p" 2>&1 | tee -a "$LOG"; then FAILED="$p"; exit 1; fi
done
