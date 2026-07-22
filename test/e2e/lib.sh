# test/e2e/lib.sh — shared helpers for test/e2e/run.sh + test/e2e/assert.sh.
# Sourced, never executed. Callers must set E2E_ROOT (repo root) before
# sourcing and may pre-set E2E_NS / E2E_EXPECTED_ROWS.
# shellcheck shell=bash

E2E_NS="${E2E_NS:-lakehouse}"
E2E_DIR="${E2E_ROOT:?E2E_ROOT (repo root) must be set before sourcing lib.sh}/test/e2e"
E2E_TOOLS_POD="e2e-tools"
E2E_EXPECTED_ROWS="${E2E_EXPECTED_ROWS:-5}"

e2e::log() { printf '\n== [e2e] %s\n' "$*"; }

# All kubectl calls scoped to the e2e namespace.
e2e::k() { kubectl -n "$E2E_NS" "$@"; }

# e2e::retry TIMEOUT_S INTERVAL_S CMD... — poll CMD (silenced) until it
# succeeds or TIMEOUT_S elapses.
e2e::retry() {
  local timeout="$1" interval="$2"
  shift 2
  local waited=0
  until "$@" >/dev/null 2>&1; do
    if (( waited >= timeout )); then
      echo "e2e: timed out after ${timeout}s waiting for: $*" >&2
      return 1
    fi
    sleep "$interval"
    waited=$((waited + interval))
  done
}

# e2e::wait_cr REF CONDITION TIMEOUT_S — wait for a (possibly not yet
# created) namespaced resource to reach CONDITION. Unlike a bare
# `kubectl wait`, tolerates the resource not existing yet (helm applies the
# CR before its operator has necessarily created anything).
e2e::wait_cr() {
  local ref="$1" condition="$2" timeout="$3"
  e2e::retry "$timeout" 5 e2e::k get "$ref"
  e2e::k wait --for="condition=${condition}" "$ref" --timeout="${timeout}s"
}

# e2e::wait_job NAME TIMEOUT_S — wait for a batch Job to succeed; on failure
# or timeout dump its logs and return non-zero.
e2e::wait_job() {
  local name="$1" timeout="$2"
  local waited=0
  while true; do
    if [[ "$(e2e::k get job "$name" -o jsonpath='{.status.succeeded}' 2>/dev/null)" == "1" ]]; then
      return 0
    fi
    if e2e::k get job "$name" -o jsonpath='{.status.conditions[?(@.type=="Failed")].status}' 2>/dev/null | grep -q True; then
      echo "e2e: job/${name} FAILED — logs follow" >&2
      e2e::k logs "job/${name}" --tail=200 >&2 || true
      return 1
    fi
    if (( waited >= timeout )); then
      echo "e2e: job/${name} did not complete within ${timeout}s — logs follow" >&2
      e2e::k logs "job/${name}" --tail=200 >&2 || true
      return 1
    fi
    sleep 10
    waited=$((waited + 10))
  done
}

# e2e::ensure_tools_pod — in-cluster python toolbox pod (python:3.11-slim +
# the e2e python scripts/descriptors ConfigMap-mounted at /e2e). Runs
# IN-CLUSTER on purpose: Nessie's Iceberg REST table-config responses carry
# in-cluster S3 endpoints, so pyiceberg/Trino access only resolves cleanly
# from inside the cluster (no port-forward juggling). Idempotent — creates
# only when absent; run.sh deletes it up-front so every run gets fresh
# scripts.
e2e::ensure_tools_pod() {
  if ! e2e::k get pod "$E2E_TOOLS_POD" >/dev/null 2>&1; then
    e2e::k create configmap e2e-scripts \
      --from-file="${E2E_DIR}/py/trino_query.py" \
      --from-file="${E2E_DIR}/py/iceberg_wait.py" \
      --from-file="${E2E_DIR}/py/s3_residue_check.py" \
      --from-file="${E2E_ROOT}/tools/create_iceberg_table.py" \
      --from-file="${E2E_DIR}/descriptors/bronze-customers.yaml" \
      --from-file="${E2E_DIR}/descriptors/silver-customers.yaml" \
      --dry-run=client -o yaml | e2e::k apply -f -
    e2e::k apply -f "${E2E_DIR}/manifests/tools-pod.yaml"
  fi
  e2e::k wait --for=condition=Ready "pod/${E2E_TOOLS_POD}" --timeout=180s
}

e2e::tools_exec() { e2e::k exec "$E2E_TOOLS_POD" -- "$@"; }

# pyiceberg is needed only for table pre-create / Bronze polling / the
# residue check (NOT for the stage-1 Trino probe, which is stdlib-only) —
# installed lazily, once per pod. Same pin as console/backend.
e2e::tools_pip() {
  e2e::tools_exec sh -c \
    'python -m pip show pyiceberg >/dev/null 2>&1 || python -m pip install --quiet "pyiceberg[s3fs]==0.10.*" pyyaml'
}
