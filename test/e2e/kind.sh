#!/usr/bin/env bash
# kind cluster (Podman veya Docker). Idempotent. Podman'da Spark için pids limiti yükseltilir (F0 S0d).
set -euo pipefail
CLUSTER="${KIND_CLUSTER:-lakehouse}"
PROVIDER="${KIND_EXPERIMENTAL_PROVIDER:-}"
# Taze kind düğümünde imaj önbelleği boştur ve kubelet varsayılanı imajları SIRAYLA çeker: 2,7 GB'lık Zeppelin
# kuyruğun önüne geçip CNPG postgres imajını 11 dk beklet(ti), glue kurulumu zaman aşımına uğradı (2026-09-17 canlı).
if ! kind get clusters 2>/dev/null | grep -qx "$CLUSTER"; then
  kind create cluster --name "$CLUSTER" --wait 120s --config - <<'YAML'
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
- role: control-plane
  kubeadmConfigPatches:
  - |
    kind: KubeletConfiguration
    serializeImagePulls: false
    maxParallelImagePulls: 4
YAML
fi
kubectl config use-context "kind-$CLUSTER" >/dev/null
if [[ "$PROVIDER" == "podman" ]]; then
  podman update --pids-limit 8192 "${CLUSTER}-control-plane" >/dev/null 2>&1 || true
  podman exec "${CLUSTER}-control-plane" sh -c 'mkdir -p /etc/systemd/system.conf.d && printf "[Manager]\nDefaultTasksMax=infinity\n" > /etc/systemd/system.conf.d/tasksmax.conf && systemctl daemon-reexec' >/dev/null 2>&1 || true
fi
kubectl create ns lakehouse --dry-run=client -o yaml | kubectl apply -f - >/dev/null
# dev Secret'ları (Git'e girmez; kind için sentetik)
kubectl -n lakehouse create secret generic polaris-root --from-literal=clientId=root --from-literal=clientSecret=s3cr3t --dry-run=client -o yaml | kubectl apply -f - >/dev/null
kubectl -n lakehouse create secret generic keycloak-admin --from-literal=username=admin --from-literal=password=admin --dry-run=client -o yaml | kubectl apply -f - >/dev/null
echo "OK: kind-$CLUSTER hazır"
