#!/usr/bin/env bash
# kind (podman) cluster + namespaces + MinIO. Idempotent.
set -euo pipefail
SELF="$(cd "$(dirname "$0")" && pwd)/$(basename "$0")"
cd "$(dirname "$0")"
export KIND_EXPERIMENTAL_PROVIDER="${KIND_EXPERIMENTAL_PROVIDER:-podman}"
CLUSTER=lh-spike
# Node imajı: varsayılan kind'ın kendi eşleştirdiği imaj (kind 0.33 -> k8s 1.36; Polaris chart >=1.33 ister).
# Farklı sürüm gerekirse KIND_NODE_IMAGE=kindest/node:vX.Y.Z@sha256:... ver.
NODE_IMAGE_ARG=(); [[ -n "${KIND_NODE_IMAGE:-}" ]] && NODE_IMAGE_ARG=(--image "$KIND_NODE_IMAGE")

if [[ "${1:-}" == "--check" ]]; then
  kubectl get ns minio polaris lakehouse spark >/dev/null
  kubectl -n minio rollout status deploy/minio --timeout=60s >/dev/null
  kubectl -n minio wait --for=condition=complete job/minio-bucket-init --timeout=120s >/dev/null
  for ns in polaris lakehouse spark; do kubectl -n "$ns" get secret minio-creds >/dev/null; done
  echo "OK: cluster+minio hazır"; exit 0
fi

if ! kind get clusters | grep -qx "$CLUSTER"; then
  kind create cluster --name "$CLUSTER" "${NODE_IMAGE_ARG[@]}" --wait 120s
fi
kubectl config use-context "kind-$CLUSTER" >/dev/null
# Podman: düğüm konteyneri pids_limit=2048 -> pod başına systemd DefaultTasksMax %15 = 307 thread -> Spark driver
# "unable to create native thread" (canlı bulgu S0d). Düğüm limitini yükselt + systemd drop-in ile pod TasksMax'ı kaldır
# (kubelet podPidsLimit pod cgroup'una yansımadı). Docker'da gereksiz; komutlar hata verirse sessizce geçer.
if [[ "$KIND_EXPERIMENTAL_PROVIDER" == "podman" ]]; then
  podman update --pids-limit 8192 "${CLUSTER}-control-plane" >/dev/null 2>&1 || true
  podman exec "${CLUSTER}-control-plane" sh -c 'mkdir -p /etc/systemd/system.conf.d && printf "[Manager]\nDefaultTasksMax=infinity\n" > /etc/systemd/system.conf.d/tasksmax.conf && systemctl daemon-reexec' >/dev/null 2>&1 || true
fi
for ns in minio polaris lakehouse spark; do kubectl create ns "$ns" --dry-run=client -o yaml | kubectl apply -f -; done

kubectl -n minio apply -f - <<'YAML'
apiVersion: apps/v1
kind: Deployment
metadata: {name: minio}
spec:
  replicas: 1
  selector: {matchLabels: {app: minio}}
  template:
    metadata: {labels: {app: minio}}
    spec:
      containers:
      - name: minio
        image: quay.io/minio/minio:latest
        args: ["server", "/data", "--console-address", ":9001"]
        env:
        - {name: MINIO_ROOT_USER, value: minioadmin}
        - {name: MINIO_ROOT_PASSWORD, value: minioadmin}
        ports: [{containerPort: 9000}, {containerPort: 9001}]
        volumeMounts: [{name: data, mountPath: /data}]
      volumes: [{name: data, emptyDir: {}}]
---
apiVersion: v1
kind: Service
metadata: {name: minio}
spec:
  selector: {app: minio}
  ports: [{name: api, port: 9000}, {name: console, port: 9001}]
---
apiVersion: batch/v1
kind: Job
metadata: {name: minio-bucket-init}
spec:
  backoffLimit: 10
  template:
    spec:
      restartPolicy: OnFailure
      containers:
      - name: mc
        image: quay.io/minio/mc:latest
        command: ["/bin/sh","-c"]
        args:
        - |
          until mc alias set m http://minio.minio.svc:9000 minioadmin minioadmin; do sleep 3; done
          mc mb -p m/lakehouse && mc ls m
YAML

for ns in polaris lakehouse spark; do
  kubectl -n "$ns" create secret generic minio-creds \
    --from-literal=AWS_ACCESS_KEY_ID=minioadmin --from-literal=AWS_SECRET_ACCESS_KEY=minioadmin \
    --dry-run=client -o yaml | kubectl apply -f -
done
kubectl -n minio rollout status deploy/minio --timeout=180s
kubectl -n minio wait --for=condition=complete job/minio-bucket-init --timeout=300s
"$SELF" --check
