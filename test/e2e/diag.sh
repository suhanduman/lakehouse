#!/usr/bin/env bash
# test/e2e/diag.sh — failure diagnostics dump for the kind e2e. Called by
# .github/workflows/e2e.yaml on failure (if: failure()) so `gh run view
# --log-failed` carries enough cluster state to debug remotely without
# re-running. Read-only; never fails the job itself (best-effort dumps).
set -uo pipefail

E2E_NS="${E2E_NS:-lakehouse}"

section() { printf '\n########## %s ##########\n' "$*"; }

section "nodes + all pods"
kubectl get nodes -o wide 2>&1
kubectl get pods -A -o wide 2>&1

section "node allocated resources"
kubectl describe nodes 2>&1 | sed -n '/Allocated resources/,/Events/p'

section "events (${E2E_NS})"
kubectl -n "$E2E_NS" get events --sort-by=.lastTimestamp 2>&1 | tail -80

section "helm releases"
helm list -A 2>&1

section "namespace objects"
kubectl -n "$E2E_NS" get deploy,sts,job,pvc,svc 2>&1

section "CNPG clusters"
kubectl -n "$E2E_NS" get clusters.postgresql.cnpg.io -o wide 2>&1

section "Strimzi CR status"
for kind in kafka kafkanodepool kafkaconnect kafkaconnector kafkatopic kafkauser; do
  kubectl -n "$E2E_NS" get "$kind" 2>/dev/null
done
kubectl -n "$E2E_NS" get kafka kafka -o jsonpath='{.status.conditions}' 2>/dev/null; echo
kubectl -n "$E2E_NS" get kafkaconnect connect -o jsonpath='{.status.conditions}' 2>/dev/null; echo
for kc in iceberg-sink-cdc iceberg-sink-mongo e2e-pg-customers; do
  echo "--- kafkaconnector/${kc} status:"
  kubectl -n "$E2E_NS" get kafkaconnector "$kc" -o jsonpath='{.status}' 2>/dev/null; echo
done

section "service endpoints (${E2E_NS})"
kubectl -n "$E2E_NS" get endpoints 2>&1

section "connectivity matrix (from e2e-tools)"
TRINO_POD_IP="$(kubectl -n "$E2E_NS" get pod -l app=trino,role=coordinator -o jsonpath='{.items[0].status.podIP}' 2>/dev/null)"
kubectl -n "$E2E_NS" exec e2e-tools -- python -c "
import socket
def t(host, port):
    try:
        s = socket.create_connection((host, int(port)), 5); s.close()
        print('OK  ', host, port)
    except Exception as e:
        print('FAIL', host, port, e)
t('trino-coordinator', 8080)
t('${TRINO_POD_IP:-127.0.0.1}', 8080)
t('nessie', 19120)
t('minio', 9000)
t('pg-nessie-rw', 5432)
t('1.1.1.1', 443)
" 2>&1

section "kindnet / network-policy agent logs"
kubectl -n kube-system logs -l app=kindnet --tail=100 2>&1
kubectl -n kube-system logs ds/kube-network-policies --tail=100 2>&1

section "not-ready pod describes (${E2E_NS})"
for pod in $(kubectl -n "$E2E_NS" get pods --no-headers 2>/dev/null \
    | awk '$3 != "Running" && $3 != "Completed" {print $1}'); do
  echo "--- describe pod/${pod}:"
  kubectl -n "$E2E_NS" describe pod "$pod" 2>&1 | tail -40
done

section "key pod logs (tails)"
for selector in "app=minio" "app=trino,role=coordinator" "app=e2e-tools"; do
  echo "--- logs (-l ${selector}):"
  kubectl -n "$E2E_NS" logs -l "$selector" --tail=50 2>&1
done
for deploy in nessie apicurio-registry; do
  echo "--- logs deploy/${deploy}:"
  kubectl -n "$E2E_NS" logs "deploy/${deploy}" --tail=50 2>&1
done
echo "--- logs connect-connect-0:"
kubectl -n "$E2E_NS" logs connect-connect-0 --tail=150 2>&1

section "e2e job logs"
for job in e2e-silver-merge e2e-maintenance; do
  echo "--- logs job/${job}:"
  kubectl -n "$E2E_NS" logs "job/${job}" --tail=150 2>&1
done

section "operator namespaces"
for ns in strimzi cnpg-system cert-manager spark-operator keycloak-system; do
  echo "--- pods in ${ns}:"
  kubectl get pods -n "$ns" 2>&1
done

exit 0
