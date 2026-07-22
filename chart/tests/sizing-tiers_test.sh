#!/usr/bin/env bash
# chart/tests/sizing-tiers_test.sh — Product Foundation Phase 3, Task 9
# ("sizing tiers") verification gate.
#
# This chart ships three supported sizing-tier `values` overlays, each
# layered OVER `chart/values.yaml` (the chart defaults, which are already
# "medium-ish HA" by design — see values-medium.yaml's own header comment):
#   - values-dev.yaml    — single-node/laptop/PoC/e2e (HA off, RF=1)
#   - values-medium.yaml — typical production (documents/asserts the
#                          chart-default shape; nearly-empty by design)
#   - values-large.yaml  — high-volume production (scaled up)
#
# It also ships `values-example.yaml` (Product Foundation Phase 4, Task
# 12) — a small, anonymized reference overlay a new customer copies as a
# starting point (meant to be layered on top of one of the three tiers
# above, but rendered standalone here since it must also stand on its own).
#
# Three checks, in order (each gates the next — no point content-asserting a
# render that didn't even produce output, and no point running the
# knob-existence check against overlays that don't parse):
#   1. Render matrix: every tier x platform (vanilla, openshift) renders
#      clean (`helm template` exits 0, produces non-empty output) — PLUS
#      `values-example.yaml` standalone x platform.
#   2. Content assertions: dev/large/medium each render the sizing signals
#      the tier promises (kafka broker replicas, MinIO presence, CNPG
#      instances) — parsed from the rendered manifests (not grep, so
#      indentation-level false positives can't sneak by).
#   3. Knob-existence check: every key path used in all three tier overlays
#      (plus `values-example.yaml`) must exist somewhere in
#      `chart/values.yaml`'s own key structure. This
#      catches typo'd/renamed overlay keys that Helm would otherwise
#      silently ignore (an overlay setting `kafka.nodePools.broker.replica`
#      — missing the `s` — renders with the chart DEFAULT of 3, no error,
#      no warning; this check is the only thing that catches that).
#
# Exit code is non-zero the moment any stage fails.
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
CHART="$ROOT/chart"

if ! command -v helm >/dev/null 2>&1; then
  echo "sizing-tiers_test.sh: FAIL — helm not found on PATH" >&2
  exit 1
fi
if ! command -v python3 >/dev/null 2>&1; then
  echo "sizing-tiers_test.sh: FAIL — python3 not found on PATH" >&2
  exit 1
fi
if ! python3 -c "import yaml" >/dev/null 2>&1; then
  echo "sizing-tiers_test.sh: FAIL — python3 'yaml' (pyyaml) module not available" >&2
  exit 1
fi

TIERS=(dev medium large)
PLATFORMS=(vanilla openshift)

WORKDIR="$(mktemp -d)"
trap 'rm -rf "$WORKDIR"' EXIT

overall_fail=0

# ---------------------------------------------------------------------------
# Stage 1: render matrix — every tier x platform renders clean.
# ---------------------------------------------------------------------------
stage1_fail=0
for tier in "${TIERS[@]}"; do
  values_file="$CHART/values-${tier}.yaml"
  if [ ! -f "$values_file" ]; then
    echo "FAIL: missing $values_file" >&2
    stage1_fail=1
    continue
  fi
  for platform in "${PLATFORMS[@]}"; do
    out="$WORKDIR/${tier}-${platform}.yaml"
    err="$WORKDIR/${tier}-${platform}.err"
    if helm template lakehouse "$CHART" -f "$values_file" --set "platform=${platform}" \
        >"$out" 2>"$err"; then
      if [ -s "$out" ]; then
        echo "PASS: render tier=${tier} platform=${platform}"
      else
        echo "FAIL: render tier=${tier} platform=${platform} — produced EMPTY output" >&2
        stage1_fail=1
      fi
    else
      echo "FAIL: render tier=${tier} platform=${platform} — helm template exited non-zero:" >&2
      sed 's/^/    /' "$err" >&2
      stage1_fail=1
    fi
  done
done

# `values-example.yaml` (Product Foundation Phase 4, Task 12) — the small,
# anonymized "start here" onboarding overlay a new customer copies. Rendered
# standalone (not layered over a tier) here, same as the three TIERS above,
# on both platforms — it must be a self-sufficient, renderable values file
# on its own, since a fresh customer's very first `helm template`/`helm
# install` may well run it without a tier overlay yet.
example_values_file="$CHART/values-example.yaml"
if [ ! -f "$example_values_file" ]; then
  echo "FAIL: missing $example_values_file" >&2
  stage1_fail=1
else
  for platform in "${PLATFORMS[@]}"; do
    out="$WORKDIR/example-${platform}.yaml"
    err="$WORKDIR/example-${platform}.err"
    if helm template lakehouse "$CHART" -f "$example_values_file" --set "platform=${platform}" \
        >"$out" 2>"$err"; then
      if [ -s "$out" ]; then
        echo "PASS: render tier=example platform=${platform}"
      else
        echo "FAIL: render tier=example platform=${platform} — produced EMPTY output" >&2
        stage1_fail=1
      fi
    else
      echo "FAIL: render tier=example platform=${platform} — helm template exited non-zero:" >&2
      sed 's/^/    /' "$err" >&2
      stage1_fail=1
    fi
  done
fi

if [ "$stage1_fail" -ne 0 ]; then
  echo "sizing-tiers_test.sh: FAIL at render stage — aborting further checks" >&2
  exit 1
fi
overall_fail=$((overall_fail + stage1_fail))

# ---------------------------------------------------------------------------
# Stage 2: tier-specific content assertions (vanilla render of each tier).
# ---------------------------------------------------------------------------
python3 - "$WORKDIR/dev-vanilla.yaml" "$WORKDIR/medium-vanilla.yaml" "$WORKDIR/large-vanilla.yaml" <<'PY'
import sys
import yaml

dev_path, medium_path, large_path = sys.argv[1:4]


def load_docs(path):
    with open(path) as f:
        return [d for d in yaml.safe_load_all(f) if d]


def find(docs, kind, name):
    for d in docs:
        if d.get("kind") == kind and d.get("metadata", {}).get("name") == name:
            return d
    return None


fail = False


def check(label, cond):
    global fail
    if cond:
        print(f"PASS: {label}")
    else:
        print(f"FAIL: {label}", file=sys.stderr)
        fail = True


dev = load_docs(dev_path)
medium = load_docs(medium_path)
large = load_docs(large_path)

# --- dev: HA off, RF=1, MinIO present ---
broker = find(dev, "KafkaNodePool", "broker")
check("dev: kafka broker KafkaNodePool replicas == 1",
      broker is not None and broker.get("spec", {}).get("replicas") == 1)

controller = find(dev, "KafkaNodePool", "controller")
check("dev: kafka controller KafkaNodePool replicas == 1",
      controller is not None and controller.get("spec", {}).get("replicas") == 1)

minio = find(dev, "Deployment", "minio")
check("dev: MinIO Deployment present (components.minio: true)", minio is not None)

for cluster_name in ("pg-nessie", "pg-apicurio", "pg-keycloak"):
    pg = find(dev, "Cluster", cluster_name)
    check(f"dev: {cluster_name} CNPG instances == 1",
          pg is not None and pg.get("spec", {}).get("instances") == 1)

# --- medium: defaults (chart defaults ARE the medium shape) ---
broker_m = find(medium, "KafkaNodePool", "broker")
check("medium: kafka broker KafkaNodePool replicas == 3 (chart defaults)",
      broker_m is not None and broker_m.get("spec", {}).get("replicas") == 3)

for cluster_name in ("pg-nessie", "pg-apicurio", "pg-keycloak"):
    pg = find(medium, "Cluster", cluster_name)
    check(f"medium: {cluster_name} CNPG instances == 3 (chart defaults)",
          pg is not None and pg.get("spec", {}).get("instances") == 3)

minio_m = find(medium, "Deployment", "minio")
check("medium: no MinIO Deployment (components.minio stays false)", minio_m is None)

# --- large: scaled up, no MinIO ---
broker_l = find(large, "KafkaNodePool", "broker")
check("large: kafka broker KafkaNodePool replicas == 5",
      broker_l is not None and broker_l.get("spec", {}).get("replicas") == 5)

minio_l = find(large, "Deployment", "minio")
check("large: no MinIO Deployment (dev/PoC-only component)", minio_l is None)

sys.exit(1 if fail else 0)
PY
stage2_rc=$?
if [ "$stage2_rc" -ne 0 ]; then
  echo "sizing-tiers_test.sh: FAIL at content-assertion stage" >&2
fi
overall_fail=$((overall_fail + stage2_rc))

# ---------------------------------------------------------------------------
# Stage 3: knob-existence check — every overlay key path must exist in
# chart/values.yaml's own key structure (catches typo'd overlay keys that
# Helm silently ignores instead of erroring on).
# ---------------------------------------------------------------------------
python3 - "$CHART/values.yaml" "$CHART/values-dev.yaml" "$CHART/values-medium.yaml" "$CHART/values-large.yaml" "$CHART/values-example.yaml" <<'PY'
import sys
import yaml

base_path = sys.argv[1]
overlay_paths = sys.argv[2:]

with open(base_path) as f:
    base = yaml.safe_load(f)

fail = False


def check_keys(overlay_node, base_node, path, file_label):
    global fail
    if not isinstance(overlay_node, dict):
        # Scalar/list leaf — existence of this key itself was already
        # verified by the caller before recursing in. Nothing further to
        # check (we do not descend into list contents).
        return
    if not isinstance(base_node, dict):
        print(f"FAIL: {file_label}: '{path}' is a mapping in the overlay but "
              f"not in chart/values.yaml (type mismatch)", file=sys.stderr)
        fail = True
        return
    for key, value in overlay_node.items():
        new_path = f"{path}.{key}" if path else str(key)
        if key not in base_node:
            print(f"FAIL: {file_label}: unknown key path '{new_path}' — no "
                  f"such key in chart/values.yaml", file=sys.stderr)
            fail = True
            continue
        check_keys(value, base_node[key], new_path, file_label)


for overlay_path in overlay_paths:
    with open(overlay_path) as f:
        overlay = yaml.safe_load(f) or {}
    check_keys(overlay, base, "", overlay_path)
    if not fail:
        print(f"PASS: knob-existence check clean — {overlay_path}")

sys.exit(1 if fail else 0)
PY
stage3_rc=$?
if [ "$stage3_rc" -ne 0 ]; then
  echo "sizing-tiers_test.sh: FAIL at knob-existence stage" >&2
fi
overall_fail=$((overall_fail + stage3_rc))

# ---------------------------------------------------------------------------
# Stage 4: dev-tier REQUESTS-SUM budget — the Phase-5 CI e2e suite installs
# `values-dev.yaml` on a GitHub Actions runner (~4 vCPU / 16GB total, shared
# with the kind/k3d control plane + every operator Pod already running
# there). This sums every STEADY-STATE resource request the dev render
# produces and fails if the total exceeds the laptop/CI budget documented in
# values-dev.yaml's own header comment: < 3.5 vCPU / < 8Gi.
#
# Covered (steady-state, always-resident workloads):
#   - Deployment / StatefulSet: sum of container `resources.requests`,
#     multiplied by `spec.replicas` (default 1).
#   - Strimzi `KafkaNodePool` / `KafkaConnect`: `spec.resources.requests`,
#     multiplied by `spec.replicas`.
#   - CNPG `Cluster` (postgresql.cnpg.io): `spec.resources.requests`,
#     multiplied by `spec.instances`.
#   - Keycloak Operator `Keycloak` CR: `spec.resources.requests`,
#     multiplied by `spec.instances`.
#   - Continuous `SparkApplication` (NOT `ScheduledSparkApplication` — see
#     excluded list below): driver (1 pod) + executor (`spec.executor.
#     instances`, default 1) `cores`/`memory`.
#
# Deliberately EXCLUDED (documented, not silently skipped):
#   - `ScheduledSparkApplication` (cron-triggered Spark jobs — e.g.
#     iceberg-maintenance hourly, silver-merge every 15min with dynamic
#     allocation floor 0): bounded-duration bursts, not steady-state
#     residents — a laptop/CI runner needs headroom for the ALWAYS-ON set,
#     not for a job that finishes and scales back to zero.
#   - one-shot `Job`/`CronJob` (e.g. the S3 bucket-init Job): install-time
#     transients, same reasoning.
# If future chart changes move real steady-state workloads onto one of the
# excluded kinds, this stage will silently under-count — that's a known,
# accepted limitation of "implement pragmatically", not a hidden bug.
python3 - "$WORKDIR/dev-vanilla.yaml" <<'PY'
import re
import sys
import yaml

CPU_LIMIT_M = 3500
MEM_LIMIT_MI = 8192  # 8Gi

path = sys.argv[1]
with open(path) as f:
    docs = [d for d in yaml.safe_load_all(f) if d]


def parse_cpu_millicores(v):
    if v is None:
        return 0
    s = str(v)
    if s.endswith("m"):
        return int(s[:-1])
    return int(float(s) * 1000)


def parse_mem_mebibytes(v):
    if v is None:
        return 0
    s = str(v)
    m = re.match(r"^([0-9.]+)([A-Za-z]*)$", s)
    if not m:
        return 0
    num, unit = float(m.group(1)), m.group(2)
    # Treat decimal (M/G) and binary (Mi/Gi) suffixes the same (approximate,
    # consistent with how this budget was originally sized) — good enough
    # for a "well under the ceiling" gate, not a billing-grade calculation.
    mult = {
        "": 1 / (1024 * 1024),
        "Ki": 1 / 1024, "K": 1 / 1024,
        "Mi": 1, "M": 1,
        "Gi": 1024, "G": 1024,
        "Ti": 1024 * 1024,
    }
    return num * mult.get(unit, 1)


total_cpu_m = 0
total_mem_mi = 0
breakdown = []

for d in docs:
    kind = d.get("kind")
    api = str(d.get("apiVersion", ""))
    name = d.get("metadata", {}).get("name")
    cpu_m = mem_mi = 0

    if kind in ("Deployment", "StatefulSet"):
        replicas = d.get("spec", {}).get("replicas", 1) or 1
        containers = d.get("spec", {}).get("template", {}).get("spec", {}).get("containers", [])
        for c in containers:
            req = (c.get("resources") or {}).get("requests", {})
            cpu_m += parse_cpu_millicores(req.get("cpu")) * replicas
            mem_mi += parse_mem_mebibytes(req.get("memory")) * replicas
    elif kind in ("KafkaNodePool", "KafkaConnect"):
        replicas = d.get("spec", {}).get("replicas", 1) or 1
        req = (d.get("spec", {}).get("resources") or {}).get("requests", {})
        cpu_m += parse_cpu_millicores(req.get("cpu")) * replicas
        mem_mi += parse_mem_mebibytes(req.get("memory")) * replicas
    elif kind == "Cluster" and "postgresql" in api:
        instances = d.get("spec", {}).get("instances", 1) or 1
        req = (d.get("spec", {}).get("resources") or {}).get("requests", {})
        cpu_m += parse_cpu_millicores(req.get("cpu")) * instances
        mem_mi += parse_mem_mebibytes(req.get("memory")) * instances
    elif kind == "Keycloak":
        instances = d.get("spec", {}).get("instances", 1) or 1
        req = (d.get("spec", {}).get("resources") or {}).get("requests", {})
        cpu_m += parse_cpu_millicores(req.get("cpu")) * instances
        mem_mi += parse_mem_mebibytes(req.get("memory")) * instances
    elif kind == "SparkApplication":  # continuous only — NOT ScheduledSparkApplication
        driver = d.get("spec", {}).get("driver", {})
        executor = d.get("spec", {}).get("executor", {})
        cpu_m += (driver.get("cores") or 0) * 1000
        mem_mi += parse_mem_mebibytes(driver.get("memory"))
        executor_instances = executor.get("instances", 1) or 1
        cpu_m += (executor.get("cores") or 0) * 1000 * executor_instances
        mem_mi += parse_mem_mebibytes(executor.get("memory")) * executor_instances
    else:
        continue

    if cpu_m or mem_mi:
        breakdown.append((kind, name, cpu_m, mem_mi))
    total_cpu_m += cpu_m
    total_mem_mi += mem_mi

for kind, name, cpu_m, mem_mi in sorted(breakdown, key=lambda x: -x[2]):
    print(f"    {kind:22s} {name:25s} cpu={cpu_m:5d}m  mem={mem_mi:7.1f}Mi")

print(f"dev-tier REQUESTS-SUM: cpu={total_cpu_m}m (limit {CPU_LIMIT_M}m), "
      f"mem={total_mem_mi:.1f}Mi (limit {MEM_LIMIT_MI}Mi)")

fail = False
if total_cpu_m > CPU_LIMIT_M:
    print(f"FAIL: dev-tier requests-sum: total CPU requests {total_cpu_m}m "
          f"exceeds {CPU_LIMIT_M}m budget", file=sys.stderr)
    fail = True
else:
    print(f"PASS: dev-tier requests-sum: total CPU requests {total_cpu_m}m "
          f"<= {CPU_LIMIT_M}m budget")

if total_mem_mi > MEM_LIMIT_MI:
    print(f"FAIL: dev-tier requests-sum: total memory requests {total_mem_mi:.1f}Mi "
          f"exceeds {MEM_LIMIT_MI}Mi budget", file=sys.stderr)
    fail = True
else:
    print(f"PASS: dev-tier requests-sum: total memory requests {total_mem_mi:.1f}Mi "
          f"<= {MEM_LIMIT_MI}Mi budget")

sys.exit(1 if fail else 0)
PY
stage4_rc=$?
if [ "$stage4_rc" -ne 0 ]; then
  echo "sizing-tiers_test.sh: FAIL at dev-tier requests-sum stage" >&2
fi
overall_fail=$((overall_fail + stage4_rc))

if [ "$overall_fail" -ne 0 ]; then
  echo "sizing-tiers_test.sh: FAIL" >&2
  exit 1
fi

echo "sizing-tiers_test.sh: OK (render matrix + content assertions + knob-existence + dev requests-sum all clean)"
