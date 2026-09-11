#!/usr/bin/env bash
# e2e (F1): kind -> bootstrap -> Application'lar Healthy -> polaris-setup -> smoke Job. CI ve lokal aynı.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"; MODE=argocd; REVISION="${REVISION:-v2}"; REPO="${REPO:-https://github.com/suhanduman/lakehouse.git}"
while [[ $# -gt 0 ]]; do case "$1" in --mode) MODE="$2"; shift 2;; --revision) REVISION="$2"; shift 2;; --repo) REPO="$2"; shift 2;; *) echo "bilinmeyen argüman: $1"; exit 2;; esac; done
"$ROOT/test/e2e/kind.sh"
"$ROOT/bootstrap/bootstrap.sh" --env dev --mode "$MODE" --repo "$REPO" --revision "$REVISION"
if [[ "$MODE" == "argocd" ]]; then
  # Alt Application'ları kök üretir (ilk sync repo klonu + kustomize): önce kök Synced, sonra çocuk var olsun
  kubectl -n argocd wait application/lakehouse-root --for=jsonpath='{.status.sync.status}'=Synced --timeout=600s
  for app in strimzi cnpg keycloak-operator spark-operator glue polaris; do
    echo "bekleniyor: application/$app"
    for _ in $(seq 1 60); do kubectl -n argocd get application/"$app" >/dev/null 2>&1 && break; sleep 5; done
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
