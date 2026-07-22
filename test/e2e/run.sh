#!/usr/bin/env bash
# test/e2e/run.sh — kind e2e orchestrator (Product Foundation Phase 5 /
# Task 14). Runs the SAME way locally (kind/kubectl/helm/docker installed)
# and in .github/workflows/e2e.yaml (which just calls this script).
#
#   Stage 1 (install smoke): kind cluster -> bootstrap.sh --platform vanilla
#     --skip-argocd -> helm install (values-dev + values-ci, Connect and
#     connectors OFF — the Connect image only exists in stage 2) -> assert.sh
#     --stage 1 (everything Ready + Trino SELECT 1).
#
#   Stage 2 (data path): + pre-built Connect image kind-loaded (never pushed
#     to a registry) -> source-DB Secrets -> full install (Connect + Iceberg
#     sinks) -> pg-src fixture seeded -> Bronze/Silver tables PRE-CREATED
#     (pyiceberg, tools/create_iceberg_table.py) -> Debezium source connector
#     -> Bronze rows land -> one-shot local-mode silver-merge (merge_cdc.py)
#     -> maintenance chain (iceberg_maintenance.py) -> assert.sh --stage 2
#     (Silver count == seeded count, maintenance clean, no S3 residue).
#
# Usage: run.sh --stage 1|2 [--namespace lakehouse] [--cluster lakehouse-e2e]
# The release namespace default (lakehouse) is COUPLED to the endpoints in
# test/e2e/values-ci.yaml — see that file's header before changing it.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
E2E_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

STAGE=""
KIND_CLUSTER="${KIND_CLUSTER:-lakehouse-e2e}"
CONNECT_IMAGE="${CONNECT_IMAGE:-lakehouse-connect:e2e}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --stage) STAGE="${2:?--stage requires a value}"; shift 2 ;;
    --namespace) E2E_NS="${2:?--namespace requires a value}"; shift 2 ;;
    --cluster) KIND_CLUSTER="${2:?--cluster requires a value}"; shift 2 ;;
    *) echo "run.sh: unknown argument: $1" >&2; exit 2 ;;
  esac
done
case "$STAGE" in 1|2) ;; *) echo "run.sh: --stage 1|2 is required" >&2; exit 2 ;; esac

# shellcheck source=lib.sh disable=SC1091
source "$E2E_ROOT/test/e2e/lib.sh"

for bin in kind kubectl helm; do
  command -v "$bin" >/dev/null || { echo "run.sh: missing dependency: $bin" >&2; exit 2; }
done

# --------------------------------------------------------------------------
# 1. kind cluster (created here when run locally; the workflow pre-creates it
#    via helm/kind-action and this becomes a no-op).
# --------------------------------------------------------------------------
e2e::log "kind cluster '${KIND_CLUSTER}'"
if ! kind get clusters 2>/dev/null | grep -qx "$KIND_CLUSTER"; then
  kind create cluster --name "$KIND_CLUSTER" --wait 120s
fi
kubectl config use-context "kind-${KIND_CLUSTER}"
# Safety: everything below mutates the current context — refuse to continue
# unless it is the kind cluster we expect.
current_ctx="$(kubectl config current-context)"
if [[ "$current_ctx" != "kind-${KIND_CLUSTER}" ]]; then
  echo "run.sh: refusing to run against context '${current_ctx}'" >&2
  exit 2
fi

# --------------------------------------------------------------------------
# 2. Stage 2 only: the pre-built Connect image (built by the workflow, or
#    here as a local fallback) loaded straight into the kind node — NO
#    registry publishing anywhere.
# --------------------------------------------------------------------------
if [[ "$STAGE" == "2" ]]; then
  command -v docker >/dev/null || { echo "run.sh: stage 2 needs docker" >&2; exit 2; }
  e2e::log "Connect image ${CONNECT_IMAGE}"
  if ! docker image inspect "$CONNECT_IMAGE" >/dev/null 2>&1; then
    echo "  image absent locally — building (first build compiles the Iceberg sink from source, ~20 min)"
    docker build -f "$E2E_ROOT/images/connect/Dockerfile" -t "$CONNECT_IMAGE" "$E2E_ROOT"
  fi
  kind load docker-image "$CONNECT_IMAGE" --name "$KIND_CLUSTER"
fi

# --------------------------------------------------------------------------
# 3. Layer 0: bootstrap (operators + namespace + generated secrets).
#    --skip-argocd: this e2e installs the chart directly with helm; ArgoCD
#    would only burn the runner's CPU without being exercised.
# --------------------------------------------------------------------------
e2e::log "bootstrap.sh --platform vanilla --skip-argocd --namespace ${E2E_NS}"
bash "$E2E_ROOT/bootstrap/bootstrap.sh" --platform vanilla --skip-argocd --namespace "$E2E_NS"

# --------------------------------------------------------------------------
# 3b. CI resource relief (live kind-e2e finding, 2026-07-22): a PRIVATE
#     repo's ubuntu-latest runner is only 2 vCPU / 7GB (node allocatable
#     2000m; the 4-vCPU spec is public-repo-only) and kube-system alone
#     reserves ~0.95 CPU. The operators' stock CPU requests (Keycloak 300m,
#     Strimzi 200m, ...) would leave nothing schedulable, so their REQUESTS
#     (scheduling reservations only — limits untouched) are patched down and
#     the second coredns replica is dropped. CI-only relief, deliberately
#     NOT baked into bootstrap: real installs keep the operators' own
#     defaults.
# --------------------------------------------------------------------------
e2e::log "CI resource relief (2-vCPU runner: shrink operator CPU requests)"
kubectl -n kube-system scale deployment coredns --replicas=1
kubectl -n keycloak-system set resources deployment keycloak-operator --requests=cpu=20m
kubectl -n strimzi set resources deployment strimzi-cluster-operator --requests=cpu=50m
kubectl -n cnpg-system set resources deployment cnpg-cloudnative-pg --requests=cpu=25m
kubectl -n spark-operator set resources deployment spark-operator-controller spark-operator-webhook --requests=cpu=25m
# The patches roll the operator pods — wait until each is back (CNPG's
# validating webhook otherwise rejects the chart's Cluster CRs with
# "connection refused" mid-restart).
kubectl -n keycloak-system rollout status deployment keycloak-operator --timeout=180s
kubectl -n strimzi rollout status deployment strimzi-cluster-operator --timeout=180s
kubectl -n cnpg-system rollout status deployment cnpg-cloudnative-pg --timeout=180s
kubectl -n spark-operator rollout status deployment spark-operator-controller spark-operator-webhook --timeout=180s

# The chart renders the Namespace object itself while bootstrap has already
# created it — stamp helm ownership metadata on it so `helm install` adopts
# it instead of failing with "exists and cannot be imported".
kubectl label ns "$E2E_NS" app.kubernetes.io/managed-by=Helm --overwrite
kubectl annotate ns "$E2E_NS" \
  meta.helm.sh/release-name=lakehouse \
  "meta.helm.sh/release-namespace=${E2E_NS}" --overwrite

# --------------------------------------------------------------------------
# 4. Stage 2 only: source-database Secrets. Deliberately NOT bootstrap's job
#    (customer DB credentials are created at source-onboarding time) — but
#    Connect's pod template mounts all three volumes, so they must exist
#    before the Connect pod can start. pg is real (matches the pg-src
#    fixture); mssql/mongo are placeholder mounts (no such sources here).
# --------------------------------------------------------------------------
if [[ "$STAGE" == "2" ]]; then
  e2e::log "source-DB secrets (pg real, mssql/mongo placeholder mounts)"
  e2e::k create secret generic pg \
    --from-literal=user=postgres --from-literal=pass=e2e-pgpass \
    --dry-run=client -o yaml | e2e::k apply -f -
  for s in mssql mongo; do
    e2e::k create secret generic "$s" \
      --from-literal=user=unused --from-literal=pass=unused \
      --dry-run=client -o yaml | e2e::k apply -f -
  done
fi

# --------------------------------------------------------------------------
# 5. helm install — THE product install command under test.
#    Stage 1 turns Connect + connectors off (their prebuilt image is a
#    stage-2 artifact); stage 2 installs the full ingest plane.
# --------------------------------------------------------------------------
declare -a stage_sets=()
if [[ "$STAGE" == "1" ]]; then
  stage_sets=(--set components.kafkaConnect=false --set components.connectors.enabled=false)
fi
# Fresh tools pod every run (picks up current scripts); created BEFORE the
# chart install so it is minutes old — not seconds — by the time assert.sh
# probes through it (avoids racing the CNI's view of a brand-new pod).
e2e::k delete pod "$E2E_TOOLS_POD" --ignore-not-found
e2e::k delete configmap e2e-scripts --ignore-not-found
e2e::ensure_tools_pod

e2e::log "helm upgrade --install lakehouse (stage ${STAGE})"
helm upgrade --install lakehouse "$E2E_ROOT/chart" \
  -f "$E2E_ROOT/chart/values-dev.yaml" \
  -f "$E2E_ROOT/test/e2e/values-ci.yaml" \
  -n "$E2E_NS" --timeout 15m "${stage_sets[@]:+${stage_sets[@]}}"

if [[ "$STAGE" == "1" ]]; then
  bash "$E2E_ROOT/test/e2e/assert.sh" --stage 1 --namespace "$E2E_NS"
  e2e::log "stage 1 COMPLETE"
  exit 0
fi

# ==========================================================================
# Stage 2 data path
# ==========================================================================

e2e::log "waiting for the ingest plane (Kafka + Connect) before seeding"
e2e::wait_cr "kafka.kafka.strimzi.io/kafka" Ready 900
e2e::wait_cr "kafkaconnect.kafka.strimzi.io/connect" Ready 900
# Assert the sink connectors here, WHILE the full ingest plane is up — the
# stage-2 assert runs later against a deliberately trimmed stack (see the
# "phase the workload" note below), so connector readiness is proven now.
for kc in iceberg-sink-cdc iceberg-sink-mongo; do
  e2e::wait_cr "kafkaconnector.kafka.strimzi.io/${kc}" Ready 300
done

# --------------------------------------------------------------------------
# PHASE THE WORKLOAD (live kind-e2e finding, 2026-07-22): the FULL stack +
# a concurrent Spark merge job does not fit the 2-vCPU / 7GB runner — actual
# RAM exceeds the node and the kubelet OOM-cascades (Nessie/Connect/CNPG go
# "connection refused"). The medallion phases have DISJOINT hot sets, so we
# run them one at a time and free the idle tier between phases (the same
# "shut down idle pods to free resources" lesson from the F multipass VM):
#   ingest  -> Kafka + Connect + Apicurio + Nessie + MinIO   (Trino down)
#   merge   -> Nessie + MinIO + Spark                        (ingest down)
#   assert  -> Trino + Nessie + MinIO                         (Spark gone)
#   maint   -> Nessie + MinIO + Spark                        (ingest down)
# merge/maintenance read Bronze from Nessie+MinIO, NOT Kafka.
# --------------------------------------------------------------------------

# e2e::scale_deploy REPLICAS TIMEOUT DEPLOY... — scale + (on scale-up) wait.
e2e::scale_deploy() {
  local replicas="$1" timeout="$2"; shift 2
  local d
  for d in "$@"; do
    e2e::k scale deployment "$d" --replicas="$replicas" 2>/dev/null || true
  done
  if [[ "$replicas" != "0" ]]; then
    for d in "$@"; do
      e2e::k rollout status deployment "$d" --timeout="${timeout}s"
    done
  fi
}

# Trino is not needed until the assert phase — scale its coordinator to 0
# now to free ~1GB for the heaviest (ingest) phase.
e2e::log "phase=ingest — scaling Trino coordinator down (freed until assert)"
e2e::scale_deploy 0 60 trino-coordinator

e2e::log "pg-src fixture + seed data"
e2e::k apply -f "$E2E_DIR/fixtures/pg-source.yaml"
e2e::k rollout status deploy/pg-src --timeout=300s
e2e::k exec deploy/pg-src -- psql -U postgres -d postgres -v ON_ERROR_STOP=1 -c "
  CREATE TABLE IF NOT EXISTS public.customers (
    id int PRIMARY KEY, name text NOT NULL, email text);
  INSERT INTO public.customers VALUES
    (1, 'Ada',      'ada@example.com'),
    (2, 'Grace',    'grace@example.com'),
    (3, 'Alan',     'alan@example.com'),
    (4, 'Barbara',  'barbara@example.com'),
    (5, 'Margaret', 'margaret@example.com')
  ON CONFLICT (id) DO NOTHING;"

e2e::log "pre-creating Bronze/Silver Iceberg tables (pyiceberg — MUST precede data flow)"
e2e::ensure_tools_pod
e2e::tools_pip
e2e::tools_exec python /e2e/create_iceberg_table.py --descriptor /e2e/bronze-customers.yaml
e2e::tools_exec python /e2e/create_iceberg_table.py --descriptor /e2e/silver-customers.yaml

e2e::log "Debezium source connector (topic CR + KafkaConnector)"
e2e::k apply -f "$E2E_DIR/fixtures/pg-cdc-connector.yaml"
if ! e2e::wait_cr "kafkaconnector.kafka.strimzi.io/e2e-pg-customers" Ready 300; then
  # One task-restart attempt: a task that raced a not-yet-reachable
  # dependency (e.g. Apicurio) stays FAILED until restarted.
  echo "  connector not Ready — requesting a Strimzi restart and retrying once"
  e2e::k annotate kafkaconnector e2e-pg-customers strimzi.io/restart=true --overwrite
  e2e::wait_cr "kafkaconnector.kafka.strimzi.io/e2e-pg-customers" Ready 300
fi

e2e::log "waiting for Bronze rows (snapshot -> Kafka -> Iceberg sink commit)"
# 900s: on the 2-vCPU runner the sink's control consumer + first 2-phase
# commit take ~10 min end to end after the connector is applied (observed
# live: the commit landed 4s AFTER a 600s deadline).
e2e::tools_exec python /e2e/iceberg_wait.py \
  --table depo_raw.customers --warehouse rawdata \
  --min-rows "$E2E_EXPECTED_ROWS" --timeout 900

# Bronze is committed and confirmed — the ingest tier's job is done. Free it
# before Spark: pause+scale Connect to 0 and Apicurio to 0 (merge/maintenance
# read Bronze from Nessie+MinIO, never Kafka/Apicurio). Kafka stays up (idle)
# — scaling KRaft nodepools to 0 fights Strimzi reconciliation for little
# gain; Connect (~1GB) + Apicurio (~0.4GB) + the already-down Trino (~1GB)
# free enough headroom for the Spark driver.
e2e::log "phase=merge — freeing the ingest tier (Connect + Apicurio down)"
# Pause reconciliation first so Strimzi does not immediately scale Connect
# back up to its CR replicas.
e2e::k annotate kafkaconnect connect strimzi.io/pause-reconciliation=true --overwrite
e2e::k scale kafkaconnect connect --replicas=0 2>/dev/null || true
e2e::scale_deploy 0 60 apicurio-registry
# Give the kubelet a moment to actually reclaim the freed pods' memory.
e2e::k wait --for=delete pod -l strimzi.io/cluster=connect --timeout=120s 2>/dev/null || true

e2e::log "silver-merge (one-shot local-mode Spark, ConfigMap-mounted merge_cdc.py)"
e2e::k create configmap e2e-spark-jobs \
  --from-file="$E2E_ROOT/tools/jobs/merge_cdc.py" \
  --from-file="$E2E_ROOT/tools/jobs/merge_lib.py" \
  --from-file="$E2E_ROOT/tools/jobs/iceberg_maintenance.py" \
  --from-file="$E2E_ROOT/tools/jobs/maintenance_lib.py" \
  --dry-run=client -o yaml | e2e::k apply -f -

run_spark_job() {
  local name="$1" main="$2"
  e2e::k delete job "$name" --ignore-not-found
  sed -e "s/__NAME__/${name}/g" -e "s/__MAIN__/${main}/g" -e "s/__NS__/${E2E_NS}/g" \
    "$E2E_DIR/manifests/spark-local-job.yaml" | e2e::k apply -f -
  e2e::wait_job "$name" 1500
}

run_spark_job e2e-silver-merge merge_cdc.py
if e2e::k logs job/e2e-silver-merge --tail=-1 | grep -F "[FAIL]"; then
  echo "run.sh: FATAL: silver-merge logged per-table [FAIL] lines (above)" >&2
  exit 1
fi
# The merge Job's pod lingers Completed and still counts against node memory
# accounting on some kubelets — delete it before the next Spark phase.
e2e::k delete job e2e-silver-merge --ignore-not-found

e2e::log "phase=maint — maintenance chain (rewrite/expire/orphan-cleanup + Bronze TTL)"
run_spark_job e2e-maintenance iceberg_maintenance.py

# Assert phase: Spark is gone; bring Trino back for the query asserts
# (Nessie + MinIO stayed up throughout). assert.sh --stage 2 probes only
# this trimmed stack — connector/Kafka/CNPG readiness was already asserted
# above while the full plane was up.
e2e::log "phase=assert — scaling Trino coordinator back up"
e2e::scale_deploy 1 300 trino-coordinator

bash "$E2E_ROOT/test/e2e/assert.sh" --stage 2 --namespace "$E2E_NS" \
  --expected-rows "$E2E_EXPECTED_ROWS"
e2e::log "stage 2 COMPLETE"
