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
case "$ENV" in dev|prod) ;; *) echo "bilinmeyen --env: $ENV (dev|prod olmalı)"; exit 2;; esac

if [[ "$MODE" == "helm" ]]; then
  # ArgoCD'nin varsayılanı: prod glue.yaml kullanır (10-glue.yaml), sadece dev overlay'i glue-dev.yaml'a değiştirir.
  # Job polaris-bootstrap düz Job: değerleri değişirse `kubectl delete job` gerekir (immutable template).
  GLUE_VALUES="$ROOT/platform/values/glue.yaml"; [[ "$ENV" == "dev" ]] && GLUE_VALUES="$ROOT/platform/values/glue-dev.yaml"
  helm upgrade --install cert-manager oci://quay.io/jetstack/charts/cert-manager --version v1.21.2 -n cert-manager --create-namespace --set crds.enabled=true --wait --timeout 5m
  helm upgrade --install strimzi oci://quay.io/strimzi-helm/strimzi-kafka-operator --version 1.2.0 -n lakehouse --create-namespace --set watchNamespaces="{lakehouse}" --wait
  helm repo add cnpg https://cloudnative-pg.github.io/charts >/dev/null 2>&1 || true; helm repo update cnpg >/dev/null
  helm upgrade --install cnpg cnpg/cloudnative-pg --version 0.29.0 -n cnpg-system --create-namespace --wait
  kubectl apply -k "$ROOT/platform/keycloak-operator"
  helm repo add spark-operator https://kubeflow.github.io/spark-operator >/dev/null 2>&1 || true; helm repo update spark-operator >/dev/null
  helm upgrade --install spark-operator spark-operator/spark-operator --version 2.5.2 -n lakehouse --set 'spark.jobNamespaces={lakehouse}' --wait --timeout 5m
  helm upgrade --install glue "$ROOT/glue" -n lakehouse -f "$GLUE_VALUES" --wait --timeout 25m
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
kubectl apply -f - <<'YAML'
apiVersion: v1
kind: Secret
metadata:
  name: jetstack-helm
  namespace: argocd
  labels: {argocd.argoproj.io/secret-type: repository}
stringData:
  name: jetstack-helm
  url: quay.io/jetstack/charts
  type: helm
  enableOCI: "true"
YAML
kubectl apply -f - <<'YAML'
apiVersion: v1
kind: Secret
metadata:
  name: superset-operator-helm
  namespace: argocd
  labels: {argocd.argoproj.io/secret-type: repository}
stringData:
  name: superset-operator-helm
  url: ghcr.io/apache/superset-kubernetes-operator/charts
  type: helm
  enableOCI: "true"
YAML
# Kök Application (platform/root-app.yaml'ın --env/--repo/--revision ile parametrelenmiş hâli). YALNIZ kök uygulanır:
# alt Application'lar kökün kustomize patch'leriyle her reconcile'da aynı repo/revizyona sabitlenir -> PR'da bootstrap
# edilen revizyon test edilir (tek seferlik `kubectl apply` yerine kalıcı, self-heal'e dayanıklı).
kubectl apply -f - <<YAML
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata: {name: lakehouse-root, namespace: argocd, finalizers: [resources-finalizer.argocd.argoproj.io]}
spec:
  project: default
  source:
    repoURL: "${REPO}"
    targetRevision: "${REVISION}"
    path: platform/envs/${ENV}
    kustomize:
      patches:
      - target: {kind: Application, name: glue}
        patch: |-
          - {op: replace, path: /spec/source/repoURL, value: "${REPO}"}
          - {op: replace, path: /spec/source/targetRevision, value: "${REVISION}"}
      - target: {kind: Application, name: keycloak-operator}
        patch: |-
          - {op: replace, path: /spec/source/repoURL, value: "${REPO}"}
          - {op: replace, path: /spec/source/targetRevision, value: "${REVISION}"}
      - target: {kind: Application, name: polaris}
        patch: |-
          - {op: replace, path: /spec/sources/1/repoURL, value: "${REPO}"}
          - {op: replace, path: /spec/sources/1/targetRevision, value: "${REVISION}"}
  destination: {server: https://kubernetes.default.svc, namespace: argocd}
  syncPolicy:
    automated: {prune: true, selfHeal: true}
YAML
echo "OK: ArgoCD ${ARGOCD_VERSION} + lakehouse-root (env=${ENV}, repo=${REPO}@${REVISION}) uygulandı"
echo "İzle: kubectl -n argocd get applications"
