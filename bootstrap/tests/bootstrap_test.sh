#!/usr/bin/env bash
# bootstrap/tests/bootstrap_test.sh — plain-bash test for bootstrap/bootstrap.sh
# + bootstrap/lib/secrets.sh (bats is NOT installed in this repo; follows the
# chart/tests/*.sh style: set -euo pipefail, echo OK/FAIL, non-zero exit on
# failure).
#
# Uses a PATH-shim technique so the tests never touch a real cluster: a temp
# dir is prepended to PATH with fake `kubectl`/`helm` executables that
# record every invocation (one line per call) to $CALL_LOG and return
# canned/tunable results, letting bootstrap.sh's real control-flow run
# end-to-end against fully scripted responses.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BOOTSTRAP="$ROOT/bootstrap/bootstrap.sh"
SECRETS_LIB="$ROOT/bootstrap/lib/secrets.sh"

FAIL=0

pass() { echo "OK: $1"; }
fail() {
  echo "FAIL: $1" >&2
  FAIL=1
}

TMPROOT="$(mktemp -d)"
trap 'rm -rf "$TMPROOT"' EXIT

# make_shim_dir DIR — populates DIR with fake kubectl/helm that log their
# full argument list to $CALL_LOG (one call per line) and exit/print
# according to a handful of tunable env vars (all optional):
#   SHIM_KUBECTL_GET_SECRET_EXIT   exit code for `kubectl get secret ...`
#                                  (0 = "exists", non-zero = "absent";
#                                  default: absent)
#   SHIM_KUBECTL_GET_NS_EXIT       exit code for `kubectl get ns ...`
#                                  (default: absent, i.e. non-zero)
#   SHIM_KUBECTL_API_RESOURCES_OUT stdout for `kubectl api-resources ...`
#                                  (default: empty — vanilla platform)
# Every other kubectl/helm subcommand (apply/create/upgrade/wait/rollout/...)
# just logs and exits 0 — sufficient for these tests, which only assert on
# what was (or wasn't) *called*, never on cluster state.
make_shim_dir() {
  local dir="$1"
  mkdir -p "$dir"

  cat > "$dir/kubectl" <<'SHIM'
#!/usr/bin/env bash
echo "kubectl $*" >> "${CALL_LOG:?CALL_LOG not set}"
if [[ "${1:-}" == "get" && "${2:-}" == "secret" ]]; then
  exit "${SHIM_KUBECTL_GET_SECRET_EXIT:-1}"
fi
if [[ "${1:-}" == "get" && "${2:-}" == "ns" ]]; then
  exit "${SHIM_KUBECTL_GET_NS_EXIT:-1}"
fi
if [[ "${1:-}" == "api-resources" ]]; then
  printf '%s\n' "${SHIM_KUBECTL_API_RESOURCES_OUT:-}"
  exit 0
fi
if [[ "${1:-}" == "wait" ]]; then
  exit "${SHIM_KUBECTL_WAIT_EXIT:-0}"
fi
exit 0
SHIM
  chmod +x "$dir/kubectl"

  cat > "$dir/helm" <<'SHIM'
#!/usr/bin/env bash
echo "helm $*" >> "${CALL_LOG:?CALL_LOG not set}"
exit 0
SHIM
  chmod +x "$dir/helm"
}

# --------------------------------------------------------------------------
# 1. --help exits 0 and mentions every flag.
# --------------------------------------------------------------------------
if help_out=$("$BOOTSTRAP" --help 2>&1); then
  pass "--help exits 0"
else
  fail "--help exited non-zero"
  help_out=""
fi

for flag in --platform --namespace --secrets-mode --s3-endpoint --s3-secret-from-env --skip-argocd --dry-run --help; do
  if grep -qF -- "$flag" <<<"$help_out"; then
    pass "--help mentions ${flag}"
  else
    fail "--help does not mention ${flag}"
  fi
done

# --------------------------------------------------------------------------
# 2. --dry-run --platform vanilla exits 0 with NO kubectl/helm apply/install
#    calls (explicit --platform means no cluster probing either).
# --------------------------------------------------------------------------
SHIM1="$TMPROOT/shim1"
make_shim_dir "$SHIM1"
CALL_LOG1="$TMPROOT/calls1.log"
: > "$CALL_LOG1"

if dry_out=$(PATH="$SHIM1:$PATH" CALL_LOG="$CALL_LOG1" "$BOOTSTRAP" --dry-run --platform vanilla 2>&1); then
  pass "--dry-run --platform vanilla exits 0"
else
  fail "--dry-run --platform vanilla exited non-zero: $dry_out"
  dry_out=""
fi

if grep -qiE 'upgrade --install|apply -f|create secret|create ns|create namespace' "$CALL_LOG1"; then
  fail "--dry-run invoked a mutating kubectl/helm call: $(cat "$CALL_LOG1")"
else
  pass "--dry-run made no mutating kubectl/helm calls"
fi

if grep -qi 'get secret' "$CALL_LOG1"; then
  fail "--dry-run invoked a live 'kubectl get secret' read: $(cat "$CALL_LOG1")"
else
  pass "--dry-run made no 'kubectl get secret' calls (secrets-mode=generated is the default)"
fi

if [[ -s "$CALL_LOG1" ]]; then
  fail "--dry-run --platform vanilla recorded unexpected kubectl/helm invocation(s): $(cat "$CALL_LOG1")"
else
  pass "--dry-run --platform vanilla made ZERO kubectl/helm calls (fully live-cluster-free)"
fi

# --------------------------------------------------------------------------
# 3. Dry-run plan content: openshift plan mentions operators/ apply + gitops;
#    vanilla plan mentions the helm installs from operators.env pins.
# --------------------------------------------------------------------------
# shellcheck disable=SC1091
source "$ROOT/bootstrap/operators.env"

# KEYCLOAK_OPERATOR_CRD_URLS is a deliberately space-separated URL list
# (kubernetes.yml has no CRDs; they must be applied separately — live
# kind-e2e finding).
# shellcheck disable=SC2086
for token in "$STRIMZI_CHART" "$STRIMZI_VERSION" "$CNPG_CHART" "$CNPG_VERSION" \
             "$CERT_MANAGER_CHART" "$SPARK_OPERATOR_CHART" "$ARGOCD_CHART" "$ARGOCD_VERSION" \
             "$KEYCLOAK_OPERATOR_MANIFEST_URL" $KEYCLOAK_OPERATOR_CRD_URLS; do
  if grep -qF -- "$token" <<<"$dry_out"; then
    pass "vanilla dry-run plan mentions ${token}"
  else
    fail "vanilla dry-run plan missing ${token}"
  fi
done

SHIM2="$TMPROOT/shim2"
make_shim_dir "$SHIM2"
CALL_LOG2="$TMPROOT/calls2.log"
: > "$CALL_LOG2"

if os_out=$(PATH="$SHIM2:$PATH" CALL_LOG="$CALL_LOG2" "$BOOTSTRAP" --dry-run --platform openshift 2>&1); then
  pass "--dry-run --platform openshift exits 0"
else
  fail "--dry-run --platform openshift exited non-zero: $os_out"
  os_out=""
fi

if grep -qiE 'upgrade --install|apply -f|create secret|create ns|create namespace' "$CALL_LOG2"; then
  fail "openshift --dry-run invoked a mutating kubectl/helm call: $(cat "$CALL_LOG2")"
else
  pass "openshift --dry-run made no mutating kubectl/helm calls"
fi

if grep -qi 'get secret' "$CALL_LOG2"; then
  fail "openshift --dry-run invoked a live 'kubectl get secret' read: $(cat "$CALL_LOG2")"
else
  pass "openshift --dry-run made no 'kubectl get secret' calls"
fi

if [[ -s "$CALL_LOG2" ]]; then
  fail "--dry-run --platform openshift recorded unexpected kubectl/helm invocation(s): $(cat "$CALL_LOG2")"
else
  pass "--dry-run --platform openshift made ZERO kubectl/helm calls (fully live-cluster-free)"
fi

if grep -qF "operators/00-cert-manager.yaml" <<<"$os_out" && grep -qF "operators/06-openshift-gitops.yaml" <<<"$os_out"; then
  pass "openshift dry-run plan includes the operators/ file list (incl. 06-openshift-gitops.yaml)"
else
  fail "openshift dry-run plan missing operators/ file(s): $os_out"
fi

if grep -qiF "gitops" <<<"$os_out"; then
  pass "openshift dry-run plan mentions gitops"
else
  fail "openshift dry-run plan does not mention gitops"
fi

# --------------------------------------------------------------------------
# 3b. Vanilla ArgoCD install pins --set fullnameOverride=argocd (without it,
#     release "argocd" + chart "argo-cd" would produce fullname
#     "argocd-argo-cd", i.e. Deployment "argocd-argo-cd-server", NOT
#     "argocd-server" — the wait target bootstrap.sh actually waits on).
#     Verified against the REAL (non-dry-run) recorded helm call, not just
#     the dry-run plan text, since that's what the reviewer asked for.
# --------------------------------------------------------------------------
SHIM4="$TMPROOT/shim4"
make_shim_dir "$SHIM4"
CALL_LOG5="$TMPROOT/calls5.log"
: > "$CALL_LOG5"

ARGOCD_RUN_LOG="$TMPROOT/argocd_run.log"
# shellcheck disable=SC2030,SC2031
if ! (
  export PATH="$SHIM4:$PATH"
  export CALL_LOG="$CALL_LOG5"
  export SHIM_KUBECTL_GET_SECRET_EXIT=1
  export SHIM_KUBECTL_GET_NS_EXIT=1
  "$BOOTSTRAP" --platform vanilla --namespace bootstrap-test-ns
) >"$ARGOCD_RUN_LOG" 2>&1; then
  fail "real vanilla run (for fullnameOverride check) exited non-zero: $(cat "$ARGOCD_RUN_LOG")"
fi

if grep -qiE 'helm .*upgrade --install argocd .*--set fullnameOverride=argocd' "$CALL_LOG5"; then
  pass "vanilla ArgoCD helm install recorded --set fullnameOverride=argocd"
else
  fail "vanilla ArgoCD helm install missing --set fullnameOverride=argocd: $(cat "$CALL_LOG5")"
fi

# --------------------------------------------------------------------------
# 3b2. --skip-argocd: the vanilla dry-run plan must NOT mention the ArgoCD
#      chart/install at all (only a "skipped" note), while still planning
#      every operator install; a REAL (shimmed) vanilla run must record no
#      `helm upgrade --install argocd` call either.
# --------------------------------------------------------------------------
SHIM6="$TMPROOT/shim6"
make_shim_dir "$SHIM6"
CALL_LOG7="$TMPROOT/calls7.log"
: > "$CALL_LOG7"

# shellcheck disable=SC2031
if skip_out=$(PATH="$SHIM6:$PATH" CALL_LOG="$CALL_LOG7" "$BOOTSTRAP" --dry-run --platform vanilla --skip-argocd 2>&1); then
  pass "--dry-run --platform vanilla --skip-argocd exits 0"
else
  fail "--dry-run --platform vanilla --skip-argocd exited non-zero: $skip_out"
  skip_out=""
fi

if grep -qF -- "$ARGOCD_CHART" <<<"$skip_out"; then
  fail "--skip-argocd dry-run plan still mentions ${ARGOCD_CHART}"
else
  pass "--skip-argocd dry-run plan does not mention ${ARGOCD_CHART}"
fi

if grep -qiF "skipped — --skip-argocd" <<<"$skip_out"; then
  pass "--skip-argocd dry-run plan prints the skip note"
else
  fail "--skip-argocd dry-run plan missing the skip note: $skip_out"
fi

if grep -qF -- "$STRIMZI_CHART" <<<"$skip_out" && grep -qF -- "$SPARK_OPERATOR_CHART" <<<"$skip_out"; then
  pass "--skip-argocd dry-run plan still includes the operator installs"
else
  fail "--skip-argocd dry-run plan lost the operator installs: $skip_out"
fi

SKIP_RUN_LOG="$TMPROOT/skip_argocd_run.log"
CALL_LOG8="$TMPROOT/calls8.log"
: > "$CALL_LOG8"
# shellcheck disable=SC2030,SC2031
if ! (
  export PATH="$SHIM6:$PATH"
  export CALL_LOG="$CALL_LOG8"
  export SHIM_KUBECTL_GET_SECRET_EXIT=1
  export SHIM_KUBECTL_GET_NS_EXIT=1
  "$BOOTSTRAP" --platform vanilla --skip-argocd --namespace bootstrap-test-ns
) >"$SKIP_RUN_LOG" 2>&1; then
  fail "real vanilla --skip-argocd run exited non-zero: $(cat "$SKIP_RUN_LOG")"
fi

if grep -qiE 'helm .*upgrade --install argocd' "$CALL_LOG8"; then
  fail "--skip-argocd real run still invoked the ArgoCD helm install: $(cat "$CALL_LOG8")"
else
  pass "--skip-argocd real run made no ArgoCD helm install call"
fi

if grep -qiE 'helm .*upgrade --install strimzi' "$CALL_LOG8"; then
  pass "--skip-argocd real run still installed the operators (strimzi recorded)"
else
  fail "--skip-argocd real run lost the operator installs: $(cat "$CALL_LOG8")"
fi

# --------------------------------------------------------------------------
# 3c. --dry-run with NO --platform must not probe the live cluster either —
#     it should warn and default to vanilla instead of running
#     `kubectl api-resources` (or anything else).
# --------------------------------------------------------------------------
SHIM5="$TMPROOT/shim5"
make_shim_dir "$SHIM5"
CALL_LOG6="$TMPROOT/calls6.log"
: > "$CALL_LOG6"

# shellcheck disable=SC2031
if noplatform_out=$(PATH="$SHIM5:$PATH" CALL_LOG="$CALL_LOG6" "$BOOTSTRAP" --dry-run 2>&1); then
  pass "--dry-run with no --platform exits 0"
else
  fail "--dry-run with no --platform exited non-zero: $noplatform_out"
  noplatform_out=""
fi

if [[ -s "$CALL_LOG6" ]]; then
  fail "--dry-run with no --platform recorded unexpected kubectl/helm invocation(s) (should skip auto-detect entirely): $(cat "$CALL_LOG6")"
else
  pass "--dry-run with no --platform made ZERO kubectl/helm calls (auto-detect probe skipped)"
fi

if grep -qiF "skipping the live platform auto-detection" <<<"$noplatform_out"; then
  pass "--dry-run with no --platform prints a warning instead of probing"
else
  fail "--dry-run with no --platform did not print the expected warning: $noplatform_out"
fi

# --------------------------------------------------------------------------
# 4. secrets.sh idempotency unit: fake kubectl `get secret` succeeds -> skip
#    (no create call recorded); fails -> create called exactly once.
# --------------------------------------------------------------------------
SHIM3="$TMPROOT/shim3"
make_shim_dir "$SHIM3"

CALL_LOG3="$TMPROOT/calls3.log"
: > "$CALL_LOG3"
# The env exports below are deliberately scoped to this subshell (isolates
# the two idempotency cases from each other and from the rest of the test).
# shellcheck disable=SC2030,SC2031
(
  export PATH="$SHIM3:$PATH"
  export CALL_LOG="$CALL_LOG3"
  export SHIM_KUBECTL_GET_SECRET_EXIT=0
  # shellcheck disable=SC1090
  source "$SECRETS_LIB"
  bootstrap::create_secret_if_absent test-secret lakehouse false --from-literal=foo=bar
)
if grep -q "create secret" "$CALL_LOG3"; then
  fail "secret_exists=true: create_secret_if_absent still called 'create secret'"
else
  pass "secret_exists=true: create_secret_if_absent skipped (no create call)"
fi

CALL_LOG4="$TMPROOT/calls4.log"
: > "$CALL_LOG4"
# shellcheck disable=SC2030,SC2031
(
  export PATH="$SHIM3:$PATH"
  export CALL_LOG="$CALL_LOG4"
  export SHIM_KUBECTL_GET_SECRET_EXIT=1
  # shellcheck disable=SC1090
  source "$SECRETS_LIB"
  bootstrap::create_secret_if_absent test-secret lakehouse false --from-literal=foo=bar
)
create_calls="$(grep -c "create secret" "$CALL_LOG4" || true)"
if [[ "$create_calls" -eq 1 ]]; then
  pass "secret_exists=false: create_secret_if_absent called create exactly once"
else
  fail "secret_exists=false: expected exactly 1 create call, got ${create_calls}"
fi

# --------------------------------------------------------------------------
# 5. Invalid --platform / --secrets-mode exit non-zero with a clear message.
# --------------------------------------------------------------------------
if bogus_platform_out=$("$BOOTSTRAP" --dry-run --platform bogus 2>&1); then
  fail "--platform bogus should have exited non-zero"
else
  if grep -qiF "invalid --platform" <<<"$bogus_platform_out"; then
    pass "--platform bogus exits non-zero with a clear message"
  else
    fail "--platform bogus error message unclear: $bogus_platform_out"
  fi
fi

if bogus_mode_out=$("$BOOTSTRAP" --dry-run --platform vanilla --secrets-mode bogus 2>&1); then
  fail "--secrets-mode bogus should have exited non-zero"
else
  if grep -qiF "invalid --secrets-mode" <<<"$bogus_mode_out"; then
    pass "--secrets-mode bogus exits non-zero with a clear message"
  else
    fail "--secrets-mode bogus error message unclear: $bogus_mode_out"
  fi
fi

# --------------------------------------------------------------------------
if [[ "$FAIL" -ne 0 ]]; then
  echo "bootstrap_test.sh: FAIL" >&2
  exit 1
fi
echo "bootstrap_test.sh: OK"
