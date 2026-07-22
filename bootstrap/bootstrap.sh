#!/usr/bin/env bash
# bootstrap/bootstrap.sh — Layer 0: takes a clean cluster to "ready for the
# ArgoCD app-of-apps" (namespace/RBAC via operators, operator set, ArgoCD,
# and all `generated`-mode Secrets). One idempotent, platform-detected
# script — zero manual steps for a fresh product install.
#
# Layer 1 (the umbrella chart / app-of-apps itself, Task 8) is OUT of
# scope here: this script stops the moment ArgoCD is up and the required
# Secrets exist, and prints the next step.
#
# Usage: bootstrap/bootstrap.sh --help
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# shellcheck source=operators.env disable=SC1091
source "$SCRIPT_DIR/operators.env"
# shellcheck source=lib/secrets.sh disable=SC1091
source "$SCRIPT_DIR/lib/secrets.sh"

# Cluster-scoped OLM Subscriptions applied on OpenShift (step 1). Kept as an
# explicit array (not a bare `kubectl apply -f operators/` directory glob)
# so 06-openshift-gitops.yaml is guaranteed included and the list is visible
# in --dry-run output.
OPENSHIFT_OPERATOR_FILES=(
  operators/00-cert-manager.yaml
  operators/01-external-secrets.yaml
  operators/02-cloudnativepg.yaml
  operators/03-strimzi.yaml
  operators/04-spark-operator.yaml
  operators/05-keycloak-operator.yaml
  operators/06-openshift-gitops.yaml
)

BOOTSTRAP_CRD_TIMEOUT="${BOOTSTRAP_CRD_TIMEOUT:-300}"

PLATFORM=""
NAMESPACE="lakehouse"
SECRETS_MODE="generated"
S3_ENDPOINT=""
S3_SECRET_FROM_ENV="false"
SKIP_ARGOCD="false"
DRY_RUN="false"

usage() {
  cat <<'EOF'
Usage: bootstrap.sh [--platform openshift|vanilla] [--namespace lakehouse]
                     [--secrets-mode generated|external|manual]
                     [--s3-endpoint URL] [--s3-secret-from-env]
                     [--skip-argocd] [--dry-run] [--help]

Takes a clean cluster to "ready for ArgoCD app-of-apps": namespace/RBAC,
operator set, ArgoCD, and all `generated`-mode Secrets. Stops after ArgoCD
is up — installing the app-of-apps itself is a separate step (Task 8 / see
bootstrap/README.md).

Flags:
  --platform openshift|vanilla   Target platform. Omit to auto-detect via
                                  `kubectl api-resources --api-group=route.openshift.io`.
  --namespace NAME                Target namespace for the product install
                                  (created if absent). Default: lakehouse.
  --secrets-mode MODE             generated (default) | external | manual.
                                  See bootstrap/README.md for what each mode
                                  does and does not create.
  --s3-endpoint URL                S3-compatible endpoint URL for the
                                  s3-credentials Secret. Omit to default to
                                  the in-cluster dev MinIO service.
  --s3-secret-from-env             Read S3_ACCESS_KEY_ID/S3_SECRET_ACCESS_KEY
                                  from the environment for s3-credentials
                                  instead of generating random values (never
                                  taken as CLI args, never logged).
  --skip-argocd                    Skip step 2 (ArgoCD install/wait) entirely.
                                  For environments that install the chart
                                  directly with helm instead of the ArgoCD
                                  app-of-apps — e.g. the CI kind e2e
                                  (test/e2e/run.sh), where ArgoCD would only
                                  burn runner CPU without being exercised.
                                  Operators, namespace and secrets still run.
  --dry-run                        Print the ordered action plan (operators
                                  -> argocd -> namespace -> secrets) with the
                                  exact commands; apply nothing; never touches
                                  a live cluster, not even to auto-detect
                                  --platform (see below).
  --help                           Show this help and exit.

Note: --dry-run + no --platform does NOT run the live auto-detection probe
(that would require a live cluster, defeating the point of --dry-run) — it
prints a warning and plans for 'vanilla' instead. Pass --platform explicitly
to see the openshift plan.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --platform)
      PLATFORM="${2:?--platform requires a value}"
      shift 2
      ;;
    --namespace)
      NAMESPACE="${2:?--namespace requires a value}"
      shift 2
      ;;
    --secrets-mode)
      SECRETS_MODE="${2:?--secrets-mode requires a value}"
      shift 2
      ;;
    --s3-endpoint)
      S3_ENDPOINT="${2:?--s3-endpoint requires a value}"
      shift 2
      ;;
    --s3-secret-from-env)
      S3_SECRET_FROM_ENV="true"
      shift
      ;;
    --skip-argocd)
      SKIP_ARGOCD="true"
      shift
      ;;
    --dry-run)
      DRY_RUN="true"
      shift
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      echo "bootstrap.sh: unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

case "$PLATFORM" in
  ""|openshift|vanilla) ;;
  *)
    echo "bootstrap.sh: invalid --platform '${PLATFORM}' (must be 'openshift' or 'vanilla')" >&2
    exit 2
    ;;
esac

case "$SECRETS_MODE" in
  generated|external|manual) ;;
  *)
    echo "bootstrap.sh: invalid --secrets-mode '${SECRETS_MODE}' (must be 'generated', 'external', or 'manual')" >&2
    exit 2
    ;;
esac

if [[ -z "$PLATFORM" ]]; then
  if [[ "$DRY_RUN" == "true" ]]; then
    # Auto-detection needs a live cluster (`kubectl api-resources`) — under
    # --dry-run that would defeat "does not require a live cluster", so skip
    # the probe entirely and plan for the safe default instead.
    PLATFORM="vanilla"
    echo "bootstrap.sh: WARNING: --dry-run without --platform — skipping the live platform auto-detection probe (it would require a live cluster); planning for 'vanilla'. Pass --platform explicitly to see the openshift plan." >&2
  else
    if kubectl api-resources --api-group=route.openshift.io 2>/dev/null | grep -q route; then
      PLATFORM="openshift"
    else
      PLATFORM="vanilla"
    fi
    echo "bootstrap.sh: auto-detected platform: ${PLATFORM}"
  fi
fi

# run_or_echo CMD... — the single code path shared by dry-run planning and
# real execution: dry-run just prints what would run, real mode prints then
# runs it. This is what guarantees the printed plan and the real behavior
# never drift apart.
run_or_echo() {
  if [[ "$DRY_RUN" == "true" ]]; then
    printf '  [dry-run] %s\n' "$*"
  else
    printf '  -> %s\n' "$*"
    "$@"
  fi
}

# wait_for_resource REF NAMESPACE CONDITION [TIMEOUT]
#   REF: e.g. "crd/kafkas.kafka.strimzi.io" or "deployment/argocd-server"
#   NAMESPACE: "" for cluster-scoped resources (no -n flag added)
#   Retries `kubectl wait` itself so a not-yet-created resource (operator
#   still reconciling) is tolerated up to TIMEOUT, not just a not-yet-ready
#   one.
wait_for_resource() {
  local ref="$1" namespace="$2" condition="$3" timeout="${4:-$BOOTSTRAP_CRD_TIMEOUT}"
  local -a ns_flag=()
  [[ -n "$namespace" ]] && ns_flag=(-n "$namespace")

  if [[ "$DRY_RUN" == "true" ]]; then
    printf '  [dry-run] would wait up to %ss for %s%s to be %s: kubectl %swait --for=condition=%s %s --timeout=%ss\n' \
      "$timeout" "$ref" "${namespace:+ (ns=$namespace)}" "$condition" \
      "${namespace:+-n $namespace }" "$condition" "$ref" "$timeout"
    return 0
  fi

  echo "  waiting up to ${timeout}s for ${ref}${namespace:+ (ns=$namespace)} to be ${condition}"
  local waited=0 interval=5
  while ! kubectl "${ns_flag[@]}" wait --for="condition=${condition}" "$ref" --timeout=1s >/dev/null 2>&1; do
    if (( waited >= timeout )); then
      echo "bootstrap.sh: timed out waiting for ${ref} to be ${condition}" >&2
      exit 1
    fi
    sleep "$interval"
    waited=$((waited + interval))
  done
  echo "  ready: ${ref}"
}

wait_for_required_crds() {
  local required=(
    kafkas.kafka.strimzi.io
    clusters.postgresql.cnpg.io
    certificates.cert-manager.io
    sparkapplications.sparkoperator.k8s.io
    keycloaks.k8s.keycloak.org
  )
  # ExternalSecrets is only installed (and thus only waited on) on OpenShift
  # (operators/ always includes it) or vanilla + secrets-mode=external.
  if [[ "$PLATFORM" == "openshift" || "$SECRETS_MODE" == "external" ]]; then
    required+=(externalsecrets.external-secrets.io)
  fi
  local crd
  for crd in "${required[@]}"; do
    wait_for_resource "crd/${crd}" "" Established
  done
}

step_operators() {
  echo "== [1/4] Operators (platform=${PLATFORM}) =="
  if [[ "$PLATFORM" == "openshift" ]]; then
    local f
    for f in "${OPENSHIFT_OPERATOR_FILES[@]}"; do
      run_or_echo kubectl apply -f "${REPO_ROOT}/${f}"
    done
  else
    run_or_echo helm repo add "$STRIMZI_CHART_REPO_NAME" "$STRIMZI_CHART_REPO_URL"
    run_or_echo helm repo add "$CNPG_CHART_REPO_NAME" "$CNPG_CHART_REPO_URL"
    run_or_echo helm repo add "$CERT_MANAGER_CHART_REPO_NAME" "$CERT_MANAGER_CHART_REPO_URL"
    run_or_echo helm repo add "$SPARK_OPERATOR_CHART_REPO_NAME" "$SPARK_OPERATOR_CHART_REPO_URL"
    if [[ "$SECRETS_MODE" == "external" ]]; then
      run_or_echo helm repo add "$EXTERNAL_SECRETS_CHART_REPO_NAME" "$EXTERNAL_SECRETS_CHART_REPO_URL"
    fi
    run_or_echo helm repo update

    run_or_echo helm upgrade --install strimzi "$STRIMZI_CHART" --version "$STRIMZI_VERSION" \
      -n "$STRIMZI_NAMESPACE" --create-namespace --set watchAnyNamespace=true

    run_or_echo helm upgrade --install cnpg "$CNPG_CHART" --version "$CNPG_VERSION" \
      -n "$CNPG_NAMESPACE" --create-namespace

    run_or_echo helm upgrade --install cert-manager "$CERT_MANAGER_CHART" --version "$CERT_MANAGER_VERSION" \
      -n "$CERT_MANAGER_NAMESPACE" --create-namespace --set crds.enabled=true

    if [[ "$SECRETS_MODE" == "external" ]]; then
      run_or_echo helm upgrade --install external-secrets "$EXTERNAL_SECRETS_CHART" --version "$EXTERNAL_SECRETS_VERSION" \
        -n "$EXTERNAL_SECRETS_NAMESPACE" --create-namespace
    else
      echo "  (external-secrets operator skipped — secrets-mode=${SECRETS_MODE}, only needed for secrets-mode=external)"
    fi

    run_or_echo helm upgrade --install spark-operator "$SPARK_OPERATOR_CHART" --version "$SPARK_OPERATOR_VERSION" \
      -n "$SPARK_OPERATOR_NAMESPACE" --create-namespace

    # Asymmetry (documented in operators.env): no official Keycloak Operator
    # Helm chart, so this one is `kubectl apply -f <pinned manifest URL>`.
    # Namespace creation goes through the same idempotent
    # get-or-create helper as every other namespace in this script (a plain
    # `kubectl create namespace` fails with AlreadyExists on re-run, which
    # `set -e` turns into a hard abort — see bootstrap::ensure_namespace).
    bootstrap::ensure_namespace "$KEYCLOAK_OPERATOR_NAMESPACE" "$DRY_RUN"
    # CRDs FIRST: keycloak-k8s-resources' kubernetes.yml carries only the
    # operator (SA/RBAC/Deployment) — its two CRDs are separate files
    # (live-verified on the kind e2e; without these the
    # keycloaks.k8s.keycloak.org wait below times out on a fresh cluster).
    local crd_url
    # KEYCLOAK_OPERATOR_CRD_URLS is a deliberately space-separated URL list
    # (operators.env stays a flat KEY=VALUE env file; no arrays).
    # shellcheck disable=SC2086
    for crd_url in $KEYCLOAK_OPERATOR_CRD_URLS; do
      run_or_echo kubectl apply -f "$crd_url"
    done
    run_or_echo kubectl apply -n "$KEYCLOAK_OPERATOR_NAMESPACE" -f "$KEYCLOAK_OPERATOR_MANIFEST_URL"
  fi
  wait_for_required_crds
}

step_argocd() {
  echo "== [2/4] ArgoCD =="
  if [[ "$SKIP_ARGOCD" == "true" ]]; then
    echo "  (skipped — --skip-argocd; install the chart directly with helm, or re-run without the flag for the ArgoCD app-of-apps path)"
    return 0
  fi
  if [[ "$PLATFORM" == "openshift" ]]; then
    echo "  (installed via operators/06-openshift-gitops.yaml applied in step 1 — OLM-managed, creates a default ArgoCD instance in openshift-gitops)"
    wait_for_resource "deployment/openshift-gitops-server" "openshift-gitops" Available
  else
    run_or_echo helm repo add "$ARGOCD_CHART_REPO_NAME" "$ARGOCD_CHART_REPO_URL"
    run_or_echo helm repo update
    # --set fullnameOverride=argocd pins the argo-cd chart's resource names to
    # "argocd-*" regardless of chart version. Without it, release name
    # "argocd" + chart "argo-cd" produces fullname "argocd-argo-cd" (the
    # chart's own <release>-<chart-name> template), so the server Deployment
    # would actually be named "argocd-argo-cd-server", NOT "argocd-server" —
    # the wait below would time out on every real vanilla install. Pinning
    # fullnameOverride makes "argocd-server" a deterministic, version-stable
    # target instead of depending on the chart's internal naming template.
    run_or_echo helm upgrade --install argocd "$ARGOCD_CHART" --version "$ARGOCD_VERSION" \
      -n "$ARGOCD_NAMESPACE" --create-namespace --set fullnameOverride=argocd
    wait_for_resource "deployment/argocd-server" "$ARGOCD_NAMESPACE" Available
  fi
}

step_namespace() {
  echo "== [3/4] Namespace =="
  bootstrap::ensure_namespace "$NAMESPACE" "$DRY_RUN"
}

step_secrets() {
  echo "== [4/4] Secrets (mode=${SECRETS_MODE}) =="
  case "$SECRETS_MODE" in
    generated)
      bootstrap::seed_generated_secrets "$NAMESPACE" "$S3_ENDPOINT" "$S3_SECRET_FROM_ENV" "$DRY_RUN"
      ;;
    external)
      bootstrap::print_external_inventory
      ;;
    manual)
      bootstrap::print_manual_inventory "$NAMESPACE"
      ;;
  esac
}

main() {
  step_operators
  step_argocd
  step_namespace
  step_secrets

  echo "=========================================================================="
  if [[ "$DRY_RUN" == "true" ]]; then
    echo " DRY RUN complete — nothing was applied."
  else
    echo " Bootstrap complete: namespace \"${NAMESPACE}\" ready on platform \"${PLATFORM}\"."
  fi
  if [[ "$SKIP_ARGOCD" == "true" ]]; then
    echo " NEXT STEP (--skip-argocd): install the chart directly, e.g."
    echo "   helm install lakehouse chart/ -f chart/values-dev.yaml -n \"${NAMESPACE}\""
  else
    echo " NEXT STEP: apply gitops/apps/app-of-apps.yaml (Task 8) — see bootstrap/README.md."
  fi
  echo "=========================================================================="
}

main
