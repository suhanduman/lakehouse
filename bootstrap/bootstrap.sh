#!/usr/bin/env bash
# Lakehouse v2 bootstrap: ArgoCD'yi kur ve app-of-apps kök Application'ını uygula. Idempotent.
#   bootstrap/bootstrap.sh --env dev|prod [--repo URL] [--revision REF] [--mode argocd|helm]
# --mode helm : ArgoCD'siz lokal döngü (aynı chart'lar helm ile; ArgoCD yolu CI'da doğrulanır)
set -euo pipefail
ARGOCD_VERSION=v3.5.2
ENV=dev; REPO=https://github.com/suhanduman/lakehouse.git; REVISION=v2; MODE=argocd
while [[ $# -gt 0 ]]; do case "$1" in
  --env) ENV="$2"; shift 2;; --repo) REPO="$2"; shift 2;; --revision) REVISION="$2"; shift 2;; --mode) MODE="$2"; shift 2;;
  *) echo "bilinmeyen argüman: $1"; exit 2;; esac; done
ROOT="$(cd "$(dirname "$0")/.." && pwd)"

if [[ "$MODE" == "helm" ]]; then
  helm upgrade --install strimzi oci://quay.io/strimzi-helm/strimzi-kafka-operator --version 1.2.0 -n lakehouse --create-namespace --set watchNamespaces="{lakehouse}" --wait
  helm repo add cnpg https://cloudnative-pg.github.io/charts >/dev/null 2>&1 || true; helm repo update cnpg >/dev/null
  helm upgrade --install cnpg cnpg/cloudnative-pg --version 0.29.0 -n cnpg-system --create-namespace --wait
  kubectl apply -k "$ROOT/platform/keycloak-operator"
  helm upgrade --install glue "$ROOT/glue" -n lakehouse -f "$ROOT/platform/values/glue-${ENV}.yaml" --wait --timeout 25m
  kubectl -n lakehouse wait --for=condition=Ready cluster/polaris-db --timeout=600s
  kubectl -n lakehouse wait --for=condition=complete job/polaris-bootstrap --timeout=600s
  helm repo add polaris https://downloads.apache.org/polaris/helm-chart >/dev/null 2>&1 || true; helm repo update polaris >/dev/null
  helm upgrade --install polaris polaris/polaris --version 1.7.0 -n lakehouse -f "$ROOT/platform/values/polaris.yaml" --wait --timeout 10m
  echo "OK: helm modunda kuruldu (env=$ENV)"; exit 0
fi

kubectl create ns argocd --dry-run=client -o yaml | kubectl apply -f - >/dev/null
kubectl apply -n argocd --server-side -f "https://raw.githubusercontent.com/argoproj/argo-cd/${ARGOCD_VERSION}/manifests/install.yaml"
kubectl -n argocd rollout status deploy/argocd-server --timeout=300s
kubectl -n argocd rollout status deploy/argocd-repo-server --timeout=300s
kubectl -n argocd rollout status statefulset/argocd-application-controller --timeout=300s
# ArgoCD, OCI Helm chart'ları (strimzi) sadece kayıtlı bir repository Secret'ından çeker. Anonim, kimlik bilgisi yok.
kubectl apply -f - <<'YAML'
apiVersion: v1
kind: Secret
metadata:
  name: strimzi-helm
  namespace: argocd
  labels: {argocd.argoproj.io/secret-type: repository}
stringData:
  name: strimzi-helm
  url: quay.io/strimzi-helm
  type: helm
  enableOCI: "true"
YAML
# Kök Application: env yolu + repo/revizyon (fork/PR için). Alt Application'lar da aynı repo/revizyona baksın diye
# kustomize çıktısı bir kez doğrudan uygulanır; sonraki senkronları kök Application yönetir.
sed -e "s#path: platform/envs/dev#path: platform/envs/${ENV}#" \
    -e "s#repoURL: https://github.com/suhanduman/lakehouse.git#repoURL: ${REPO}#" \
    -e "s#targetRevision: v2#targetRevision: ${REVISION}#" "$ROOT/platform/root-app.yaml" | kubectl apply -f -
kubectl kustomize "$ROOT/platform/envs/${ENV}" \
  | sed -e "s#repoURL: https://github.com/suhanduman/lakehouse.git#repoURL: ${REPO}#" -e "s#targetRevision: v2#targetRevision: ${REVISION}#" \
  | kubectl apply -f -
echo "OK: ArgoCD ${ARGOCD_VERSION} + lakehouse-root (env=${ENV}, repo=${REPO}@${REVISION}) uygulandı"
echo "İzle: kubectl -n argocd get applications"
