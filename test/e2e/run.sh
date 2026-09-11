#!/usr/bin/env bash
# e2e: kind -> bootstrap -> Application'lar Healthy -> polaris-setup -> smoke -> pg (F2) -> mongo + nginx (F3) yolları. CI ve lokal aynı.
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
if [[ "$MODE" == "argocd" ]]; then
  # Alt Application'ları kök üretir (ilk sync repo klonu + kustomize): önce kök Synced, sonra çocuk var olsun
  kubectl -n argocd wait application/lakehouse-root --for=jsonpath='{.status.sync.status}'=Synced --timeout=600s
  # ArgoCD status.sync.revision her zaman commit SHA'sıdır: dal/etiket adıyla karşılaştırmak (eski "v2" Synced'ı)
  # yanıltır -> beklenen SHA'yı uzaktan çöz. (head SIGPIPE'ı pipefail'i tetiklemesin diye || true; boşsa fail-loud.)
  if [[ "$REVISION" =~ ^[0-9a-f]{7,40}$ ]]; then EXPECT="$REVISION"
  else EXPECT=$(git ls-remote "$REPO" "refs/heads/$REVISION" "refs/tags/$REVISION" | head -1 | cut -f1 || true); fi
  [[ -n "$EXPECT" ]] || { echo "revizyon çözülemedi: $REVISION ($REPO) — dal/etiket var mı?"; exit 1; }
  echo "beklenen revizyon: $EXPECT"
  for app in strimzi cnpg keycloak-operator spark-operator glue polaris; do
    echo "bekleniyor: application/$app"
    for _ in $(seq 1 60); do kubectl -n argocd get application/"$app" >/dev/null 2>&1 && break; sleep 5; done
    # Yeniden koşuda eski revizyonun "Synced" durumu yanıltır: git kaynaklı uygulamalar yeni revizyonu görmüş olsun
    case "$app" in keycloak-operator|glue|polaris)
      seen="" rev=""
      for _ in $(seq 1 120); do
        rev=$(kubectl -n argocd get application/"$app" -o jsonpath='{.status.sync.revision}{.status.sync.revisions}' 2>/dev/null)
        [[ "$rev" == *"$EXPECT"* ]] && { seen=1; break; }; sleep 5
      done
      # sessizce düşmek eski revizyonla test etmek demekti: zaman aşımında yüksek sesle başarısız ol
      [[ -n "$seen" ]] || { echo "application/$app 10 dk içinde $EXPECT revizyonuna gelmedi (görülen: ${rev:-yok})"; exit 1; };;
    esac
    kubectl -n argocd wait application/"$app" --for=jsonpath='{.status.health.status}'=Healthy --timeout=1800s
    # glue'nun sync işlemi dalga (wave) bekler: Connect build bitmeden Synced olmaz -> Healthy ile aynı bütçe
    kubectl -n argocd wait application/"$app" --for=jsonpath='{.status.sync.status}'=Synced --timeout=1800s
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
