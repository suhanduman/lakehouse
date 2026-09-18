#!/usr/bin/env bash
# e2e: kind -> bootstrap -> Application'lar Healthy -> polaris-setup -> smoke -> pg (F2) -> mongo + nginx (F3) -> trino + superset (F4) -> monitoring + DR (F5) yolları. CI ve lokal aynı.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"; MODE=argocd; REVISION="${REVISION:-v2}"; REPO="${REPO:-https://github.com/suhanduman/lakehouse.git}"
while [[ $# -gt 0 ]]; do case "$1" in --mode) MODE="$2"; shift 2;; --revision) REVISION="$2"; shift 2;; --repo) REPO="$2"; shift 2;; *) echo "bilinmeyen argüman: $1"; exit 2;; esac; done
"$ROOT/test/e2e/kind.sh"
"$ROOT/bootstrap/bootstrap.sh" --env dev --mode "$MODE" --repo "$REPO" --revision "$REVISION"
# Kaynak DB fixture'ı (Secret shop-db + CNPG demo-pg) connector'lardan ÖNCE: dbz-shop Secret'ı bulamazsa glue Degraded kalır (F2 e2e)
# CRD + operatör webhook'u hazır olmadan apply reddedilir (CI: "failed calling webhook mcluster.cnpg.io") -> sınırlı yeniden deneme
for _ in $(seq 1 60); do kubectl apply -f "$ROOT/test/e2e/pg-fixture.yaml" >/dev/null 2>&1 && break; sleep 5; done
kubectl -n lakehouse get cluster/demo-pg >/dev/null
# Mongo fixture'ı da connector'lardan önce (dbz-crm Secret crm-db'yi bekler — aksi glue Degraded, F2 notu 8); idempotent
# namespace/CRD beklemesini üstteki pg döngüsünden miras alır
kubectl apply -f "$ROOT/test/e2e/mongo-fixture.yaml" >/dev/null
# ArgoCD Application'ı bekle: wait_app <ad> <revizyon_kontrolü 0|1> [zaman aşımı]. Revizyon kontrolü $EXPECT'i kullanır.
wait_app() {
  local app="$1" chk="$2" to="${3:-1800s}" seen="" rev=""
  echo "bekleniyor: application/$app"
  for _ in $(seq 1 60); do kubectl -n argocd get application/"$app" >/dev/null 2>&1 && break; sleep 5; done
  # Yeniden koşuda eski revizyonun "Synced" durumu yanıltır: git kaynaklı uygulamalar yeni revizyonu görmüş olsun
  if [[ "$chk" == 1 ]]; then
    for _ in $(seq 1 120); do
      rev=$(kubectl -n argocd get application/"$app" -o jsonpath='{.status.sync.revision}{.status.sync.revisions}' 2>/dev/null)
      [[ "$rev" == *"$EXPECT"* ]] && { seen=1; break; }; sleep 5
    done
    # sessizce düşmek eski revizyonla test etmek demekti: zaman aşımında yüksek sesle başarısız ol
    [[ -n "$seen" ]] || { echo "application/$app 10 dk içinde $EXPECT revizyonuna gelmedi (görülen: ${rev:-yok})"; exit 1; }
  fi
  kubectl -n argocd wait application/"$app" --for=jsonpath='{.status.health.status}'=Healthy --timeout="$to"
  # glue'nun sync işlemi dalga (wave) bekler: Connect build bitmeden Synced olmaz -> Healthy ile aynı bütçe
  kubectl -n argocd wait application/"$app" --for=jsonpath='{.status.sync.status}'=Synced --timeout="$to"
}

if [[ "$MODE" == "argocd" ]]; then
  # Alt Application'ları kök üretir (ilk sync repo klonu + kustomize): önce kök Synced, sonra çocuk var olsun
  kubectl -n argocd wait application/lakehouse-root --for=jsonpath='{.status.sync.status}'=Synced --timeout=600s
  # ArgoCD status.sync.revision her zaman commit SHA'sıdır: dal/etiket adıyla karşılaştırmak (eski "v2" Synced'ı)
  # yanıltır -> beklenen SHA'yı uzaktan çöz. (head SIGPIPE'ı pipefail'i tetiklemesin diye || true; boşsa fail-loud.)
  if [[ "$REVISION" =~ ^[0-9a-f]{7,40}$ ]]; then EXPECT="$REVISION"
  else EXPECT=$(git ls-remote "$REPO" "refs/heads/$REVISION" "refs/tags/$REVISION" | head -1 | cut -f1 || true); fi
  [[ -n "$EXPECT" ]] || { echo "revizyon çözülemedi: $REVISION ($REPO) — dal/etiket var mı?"; exit 1; }
  echo "beklenen revizyon: $EXPECT"
  # monitoring ve velero YALNIZ dev overlay'inde var (platform/apps/dev); e2e her zaman --env dev ile bootstrap eder
  for app in cert-manager strimzi cnpg cnpg-barman keycloak-operator spark-operator superset-operator monitoring velero glue polaris jupyterhub; do
    # Revizyon kontrolü GIT KAYNAKLI her uygulama için: glue/polaris/jupyterhub/trino/keycloak-operator
    # $values ref'i ile değer dosyası çeker; monitoring ve velero de öyle ($values/platform/values/
    # {monitoring,velero}-dev.yaml — bootstrap.sh DEV_ONLY_PATCH'i o kaynağı repo/revizyona sabitler).
    # Kontrolsüz bırakılırlarsa var olan bir kümede yeniden koşu ESKİ revizyonun değerlerini test edip
    # yeşil raporlayabilirdi. Salt-chart olanlar (cert-manager, strimzi, cnpg, spark-operator, …) 0 alır.
    # glue en ağır uygulama (Connect build + CNPG x3 + Keycloak + Zeppelin/Superset imajları): varsayılan 1800s yetmedi (CI 35267164775)
    case "$app" in glue) wait_app "$app" 1 2700s;; keycloak-operator|polaris|jupyterhub) wait_app "$app" 1;; monitoring|velero) wait_app "$app" 1;; *) wait_app "$app" 0;; esac
  done
fi
kubectl -n lakehouse wait kafka/lakehouse --for=condition=Ready --timeout=900s
kubectl -n lakehouse wait kafkaconnect/connect --for=condition=Ready --timeout=1800s
kubectl -n lakehouse rollout status deploy/polaris --timeout=600s
kubectl -n lakehouse wait keycloak/keycloak --for=condition=Ready --timeout=900s
kubectl -n lakehouse wait keycloakrealmimport/lakehouse-realm --for=condition=Done --timeout=600s
python3 -m venv "$ROOT/.venv" >/dev/null 2>&1 || true
[[ -x "$ROOT/.venv/bin/pip" ]] || { echo "venv yok: python3 -m venv $ROOT/.venv başarısız"; exit 1; }
"$ROOT/.venv/bin/pip" install -q 'apache-polaris==1.7.0'
PATH="$ROOT/.venv/bin:$PATH" "$ROOT/runbooks/scripts/polaris-setup.sh" --setup "$ROOT/platform/polaris/setup.yaml"
# Trino app döngüde DEĞİL: pod polaris-trino Secret'ını bekler -> ancak polaris-setup'tan sonra Healthy olabilir.
# Değerleri aynı git kaynağından geldiği için revizyon kontrolü diğer git kaynaklı uygulamalarla aynı (eski rules.json'a karşı test etmemek için).
[[ "$MODE" == "argocd" ]] && wait_app trino 1 900s
# smoke: connect principal'ının credential'ıyla küme içinden yaz/oku
CRED=$(kubectl -n lakehouse get secret polaris-connect -o jsonpath='{.data.credential}' | base64 -d)
kubectl -n lakehouse create secret generic polaris-smoke-cred --from-literal=CLIENT_ID="${CRED%%:*}" --from-literal=CLIENT_SECRET="${CRED#*:}" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
kubectl -n lakehouse create configmap polaris-smoke --from-file="$ROOT/test/e2e/polaris-smoke/smoke.py" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
kubectl -n lakehouse delete job polaris-smoke --ignore-not-found >/dev/null
kubectl apply -f "$ROOT/test/e2e/polaris-smoke/job.yaml"
kubectl -n lakehouse wait --for=condition=complete job/polaris-smoke --timeout=600s || { kubectl -n lakehouse logs job/polaris-smoke --tail=30; exit 1; }
kubectl -n lakehouse logs job/polaris-smoke | grep "^OK"
echo "E2E F1 OK"
"$ROOT/test/e2e/pg-path.sh"
"$ROOT/test/e2e/mongo-path.sh"
"$ROOT/test/e2e/nginx-path.sh"
"$ROOT/test/e2e/trino-path.sh"
"$ROOT/test/e2e/superset-path.sh"
"$ROOT/test/e2e/jupyterhub-path.sh"
"$ROOT/test/e2e/zeppelin-path.sh"
"$ROOT/test/e2e/monitoring-path.sh"
"$ROOT/test/e2e/dr-path.sh"
