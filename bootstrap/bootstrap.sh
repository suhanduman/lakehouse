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
  # İzleme yığını YALNIZ dev'de ve HER ŞEYDEN ÖNCE: PodMonitor/ServiceMonitor/PrometheusRule CRD'leri
  # spark-operator (podMonitor.create) ve glue (monitoring.yaml) uygulanmadan var olmalı.
  # Prod OpenShift'te CRD'ler platformdan gelir; bu chart KURULMAZ.
  if [[ "$ENV" == "dev" ]]; then
    helm repo add prometheus-community https://prometheus-community.github.io/helm-charts >/dev/null 2>&1 || true; helm repo update prometheus-community >/dev/null
    helm upgrade --install monitoring prometheus-community/kube-prometheus-stack --version 91.4.1 -n monitoring --create-namespace -f "$ROOT/platform/values/monitoring-dev.yaml" --wait --timeout 10m
  fi
  helm upgrade --install cert-manager oci://quay.io/jetstack/charts/cert-manager --version v1.21.2 -n cert-manager --create-namespace --set crds.enabled=true --wait --timeout 5m
  helm upgrade --install strimzi oci://quay.io/strimzi-helm/strimzi-kafka-operator --version 1.2.0 -n lakehouse --create-namespace --set watchNamespaces="{lakehouse}" --wait
  helm repo add cnpg https://cloudnative-pg.github.io/charts >/dev/null 2>&1 || true; helm repo update cnpg >/dev/null
  helm upgrade --install cnpg cnpg/cloudnative-pg --version 0.29.0 -n cnpg-system --create-namespace --wait
  # Barman Cloud eklentisi (CNPG-I): PITR yedekleri (in-tree barmanObjectStore 1.31'de kalkıyor). chart 0.8.0 = plugin v0.15.0; cert-manager gerekir.
  helm upgrade --install plugin-barman-cloud cnpg/plugin-barman-cloud --version 0.8.0 -n cnpg-system --create-namespace --set resources.requests.cpu=10m --set resources.requests.memory=64Mi --wait --timeout 5m
  kubectl apply -k "$ROOT/platform/keycloak-operator"
  helm repo add spark-operator https://kubeflow.github.io/spark-operator >/dev/null 2>&1 || true; helm repo update spark-operator >/dev/null
  # podMonitor: ArgoCD yolunda platform/apps/00-spark-operator.yaml values'ından gelir; helm modunda --set ile
  helm upgrade --install spark-operator spark-operator/spark-operator --version 2.5.2 -n lakehouse --set 'spark.jobNamespaces={lakehouse}' --set prometheus.metrics.enable=true --set prometheus.podMonitor.create=true --wait --timeout 5m
  helm upgrade --install superset-operator oci://ghcr.io/apache/superset-kubernetes-operator/charts/superset-operator --version 0.2.0 -n lakehouse --wait --timeout 5m
  # 40m: taze düğümde imaj çekimi (Zeppelin 2,7 GB, pyspark-notebook ~2 GB) + Connect build + CNPG initdb (2026-09-17 canlı)
  helm upgrade --install glue "$ROOT/glue" -n lakehouse -f "$GLUE_VALUES" --wait --timeout 40m
  kubectl -n lakehouse wait --for=condition=Ready cluster/polaris-db --timeout=600s
  kubectl -n lakehouse wait --for=condition=complete job/polaris-bootstrap --timeout=900s
  helm repo add polaris https://downloads.apache.org/polaris/helm-chart >/dev/null 2>&1 || true; helm repo update polaris >/dev/null
  helm upgrade --install polaris polaris/polaris --version 1.7.0 -n lakehouse -f "$ROOT/platform/values/polaris.yaml" --wait --timeout 10m
  helm repo add trino https://trinodb.github.io/charts >/dev/null 2>&1 || true; helm repo update trino >/dev/null
  # dev: trino-dev.yaml (dosya tabanlı gruplar); prod: trino-ldap.yaml (AD group provider) — ikisi birbirini dışlar
  TRINO_VALUES=(-f "$ROOT/platform/values/trino.yaml")
  if [[ "$ENV" == "dev" ]]; then TRINO_VALUES+=(-f "$ROOT/platform/values/trino-dev.yaml"); else TRINO_VALUES+=(-f "$ROOT/platform/values/trino-ldap.yaml"); fi
  # --wait YOK: pod polaris-trino Secret'ını bekler (polaris-setup.sh sonrası hazır olur)
  helm upgrade --install trino trino/trino --version 1.42.2 -n lakehouse "${TRINO_VALUES[@]}"
  helm repo add jupyterhub https://hub.jupyter.org/helm-chart/ >/dev/null 2>&1 || true; helm repo update jupyterhub >/dev/null
  JH_VALUES=(-f "$ROOT/platform/values/jupyterhub.yaml"); [[ "$ENV" == "dev" ]] && JH_VALUES+=(-f "$ROOT/platform/values/jupyterhub-dev.yaml")
  helm upgrade --install jupyterhub jupyterhub/jupyterhub --version 4.4.2 -n lakehouse "${JH_VALUES[@]}" --wait --timeout 10m
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
# monitoring Application'ı YALNIZ dev overlay'inde var: prod'da eşleşmeyen bir kustomize patch'i hata verir.
MONITORING_PATCH=""
if [[ "$ENV" == "dev" ]]; then MONITORING_PATCH=$(cat <<EOS
      - target: {kind: Application, name: monitoring}
        patch: |-
          - {op: replace, path: /spec/sources/1/repoURL, value: "${REPO}"}
          - {op: replace, path: /spec/sources/1/targetRevision, value: "${REVISION}"}
EOS
); fi
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
      - target: {kind: Application, name: trino}
        patch: |-
          - {op: replace, path: /spec/sources/1/repoURL, value: "${REPO}"}
          - {op: replace, path: /spec/sources/1/targetRevision, value: "${REVISION}"}
      - target: {kind: Application, name: jupyterhub}
        patch: |-
          - {op: replace, path: /spec/sources/1/repoURL, value: "${REPO}"}
          - {op: replace, path: /spec/sources/1/targetRevision, value: "${REVISION}"}
${MONITORING_PATCH}
  destination: {server: https://kubernetes.default.svc, namespace: argocd}
  syncPolicy:
    automated: {prune: true, selfHeal: true}
YAML
echo "OK: ArgoCD ${ARGOCD_VERSION} + lakehouse-root (env=${ENV}, repo=${REPO}@${REVISION}) uygulandı"
echo "İzle: kubectl -n argocd get applications"
