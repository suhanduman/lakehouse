#!/usr/bin/env bash
# test/e2e/assert.sh — the EXECUTABLE ACCEPTANCE SPEC for the kind e2e
# (Product Foundation Phase 5). Pure checks, no mutations (except creating
# the read-only in-cluster tools pod it probes through) — test/e2e/run.sh
# performs all the actions and then calls this.
#
# Stage 1 (install smoke): a single bootstrap + helm install yields a
# platform where
#   - every CNPG Cluster is Ready (pg-nessie, pg-apicurio),
#   - the Strimzi Kafka cluster is Ready,
#   - every Deployment in the release namespace is Available,
#   - Trino answers `SELECT 1`.
#
# Stage 2 (data path) additionally proves the medallion CDC pipeline:
#   - KafkaConnect + both Iceberg fan-out sink connectors are Ready,
#   - the Silver table `lakehouse.depo.customers` holds EXACTLY the seeded
#     row count (Debezium snapshot -> Kafka -> Bronze append -> silver-merge
#     MERGE -> Trino),
#   - the maintenance chain ran clean (job/e2e-maintenance succeeded with
#     no per-table "[error]" lines),
#   - NO S3 residue: every object in the Iceberg buckets is tracked by
#     current Iceberg metadata (test/e2e/py/s3_residue_check.py).
#
# Usage: assert.sh [--stage 1|2] [--namespace NS] [--expected-rows N]
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
E2E_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

STAGE=1
while [[ $# -gt 0 ]]; do
  case "$1" in
    --stage) STAGE="${2:?--stage requires a value}"; shift 2 ;;
    --namespace) E2E_NS="${2:?--namespace requires a value}"; shift 2 ;;
    --expected-rows) E2E_EXPECTED_ROWS="${2:?--expected-rows requires a value}"; shift 2 ;;
    *) echo "assert.sh: unknown argument: $1" >&2; exit 2 ;;
  esac
done
case "$STAGE" in 1|2) ;; *) echo "assert.sh: --stage must be 1 or 2" >&2; exit 2 ;; esac

# shellcheck source=lib.sh disable=SC1091
source "$E2E_ROOT/test/e2e/lib.sh"

e2e::log "ASSERT stage ${STAGE} (namespace=${E2E_NS})"

# Stage 1 (install smoke) verifies the FULL stack is Ready: every CNPG
# Cluster, the Kafka cluster, every Deployment Available, Trino SELECT 1.
# Stage 2 (data path) deliberately runs against a TRIMMED stack (Trino +
# Nessie + MinIO) — test/e2e/run.sh phases the medallion workload to fit the
# CI runner, freeing the ingest tier (Connect/Apicurio) and scaling Trino
# down/up around the Spark jobs, so the KafkaConnect/connector/CNPG/Kafka
# readiness for stage 2 is asserted THERE while the full plane is up. Stage 2
# here asserts only the data-path outcomes.
if [[ "$STAGE" == "1" ]]; then
  e2e::log "CNPG Postgres clusters Ready"
  for c in pg-nessie pg-apicurio; do
    e2e::wait_cr "cluster.postgresql.cnpg.io/${c}" Ready 600
    echo "  ready: cluster/${c}"
  done

  e2e::log "Strimzi Kafka cluster Ready"
  e2e::wait_cr "kafka.kafka.strimzi.io/kafka" Ready 900
  echo "  ready: kafka/kafka"

  e2e::log "All Deployments in ${E2E_NS} Available"
  e2e::k wait deploy --all --for=condition=Available --timeout=900s
fi

e2e::log "Trino answers SELECT 1"
e2e::ensure_tools_pod
e2e::tools_exec python /e2e/trino_query.py --query "SELECT 1" --expect 1 --timeout 300

if [[ "$STAGE" == "2" ]]; then
  e2e::log "Silver row count == ${E2E_EXPECTED_ROWS} (Trino, lakehouse.depo.customers)"
  e2e::tools_exec python /e2e/trino_query.py \
    --query "SELECT count(*) FROM lakehouse.depo.customers" \
    --expect "$E2E_EXPECTED_ROWS" --timeout 300

  e2e::log "Maintenance chain ran clean (job/e2e-maintenance)"
  if [[ "$(e2e::k get job e2e-maintenance -o jsonpath='{.status.succeeded}' 2>/dev/null)" != "1" ]]; then
    echo "assert.sh: FATAL: job/e2e-maintenance did not succeed" >&2
    exit 1
  fi
  # iceberg_maintenance.py isolates per-table failures as "[error]" log
  # lines and still exits 0 — a clean run must have none.
  if e2e::k logs job/e2e-maintenance --tail=-1 | grep -F "[error]"; then
    echo "assert.sh: FATAL: maintenance logged per-table [error] lines (above)" >&2
    exit 1
  fi
  echo "  maintenance: succeeded, no [error] lines"

  e2e::log "No S3 residue (MinIO object list is a subset of Iceberg-tracked files)"
  e2e::tools_pip
  e2e::tools_exec python /e2e/s3_residue_check.py
fi

e2e::log "ASSERT stage ${STAGE}: OK"
