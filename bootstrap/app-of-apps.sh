#!/usr/bin/env bash
# bootstrap/app-of-apps.sh — Layer 1: fills the two repoURL placeholders +
# the ArgoCD namespace in gitops/apps/app-of-apps.yaml, applies the
# argocd-cm health-check patch (gitops/argocd/argocd-cm-health.yaml), then
# applies the substituted root Application (lakehouse-root) so ArgoCD can
# take over (platform -> pipelines, sync-wave ordered).
#
# Layer 0 (bootstrap.sh) stops once ArgoCD is up and the required Secrets
# exist; this script is the "next step" it points to. See bootstrap/README.md
# "Layer 1" section and gitops/README.md.
#
# The committed gitops/apps/app-of-apps.yaml is NEVER mutated: render()
# always streams a substituted COPY (sed to stdout), whether that copy is
# piped into `kubectl apply -f -` or just printed under --dry-run.
#
# Usage: bootstrap/app-of-apps.sh --help
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

PLATFORM_REPO=""
PIPELINE_REPO=""
ARGOCD_NS="argocd"
DRY_RUN="false"

usage() {
  cat <<'EOF'
Usage: app-of-apps.sh --platform-repo URL --pipeline-repo URL
                       [--argocd-namespace argocd|openshift-gitops]
                       [--dry-run] [--help]

Layer 1: fills the <PLATFORM_REPO_URL> / <PIPELINE_REPO_URL> placeholders
and the ArgoCD namespace in a RENDERED COPY of gitops/apps/app-of-apps.yaml
(the committed file itself is never mutated), applies the argocd-cm health
patch (gitops/argocd/argocd-cm-health.yaml), then applies the substituted
root Application.

Flags:
  --platform-repo URL     Git URL for the platform repo (holds gitops/apps
                           and chart/). Required.
  --pipeline-repo URL      Git URL for the pipeline repo (holds pipelines/,
                           produced by the Console). Required.
  --argocd-namespace NAME  Namespace ArgoCD runs in. Default: argocd
                           (vanilla Helm install, fullnameOverride=argocd).
                           Use openshift-gitops on OpenShift (OLM-managed).
  --dry-run                Print the substituted plan (both repoURLs filled,
                            no placeholder left); apply NOTHING; never
                            touches a live cluster.
  --help                   Show this help and exit.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --platform-repo)
      PLATFORM_REPO="${2:?--platform-repo requires a value}"
      shift 2
      ;;
    --pipeline-repo)
      PIPELINE_REPO="${2:?--pipeline-repo requires a value}"
      shift 2
      ;;
    --argocd-namespace)
      ARGOCD_NS="${2:?--argocd-namespace requires a value}"
      shift 2
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
      echo "app-of-apps.sh: unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ -z "$PLATFORM_REPO" ]]; then
  echo "app-of-apps.sh: --platform-repo is required (and must not be empty)" >&2
  usage >&2
  exit 2
fi

if [[ -z "$PIPELINE_REPO" ]]; then
  echo "app-of-apps.sh: --pipeline-repo is required (and must not be empty)" >&2
  usage >&2
  exit 2
fi

# run_or_echo CMD... — the single code path shared by dry-run planning and
# real execution: dry-run just prints what would run, real mode prints then
# runs it. This is what guarantees the printed plan and the real behavior
# never drift apart (same helper as bootstrap.sh).
run_or_echo() {
  if [[ "$DRY_RUN" == "true" ]]; then
    printf '  [dry-run] %s\n' "$*"
  else
    printf '  -> %s\n' "$*"
    "$@"
  fi
}

# render — stream a substituted COPY of gitops/apps/app-of-apps.yaml to
# stdout. Never edits the committed placeholder file in place.
render() {
  sed -e "s|<PLATFORM_REPO_URL>|$PLATFORM_REPO|g" \
      -e "s|<PIPELINE_REPO_URL>|$PIPELINE_REPO|g" \
      -e "s|namespace: openshift-gitops|namespace: $ARGOCD_NS|g" \
      "$REPO_ROOT/gitops/apps/app-of-apps.yaml"
}

main() {
  echo "== [1/2] argocd-cm health patch (namespace=${ARGOCD_NS}) =="
  run_or_echo kubectl -n "$ARGOCD_NS" apply -f "$REPO_ROOT/gitops/argocd/argocd-cm-health.yaml"

  echo "== [2/2] root Application (lakehouse-root, namespace=${ARGOCD_NS}) =="
  if [[ "$DRY_RUN" == "true" ]]; then
    render | sed 's/^/  would-apply: /'
  else
    printf '  -> %s\n' "kubectl apply -f - (rendered gitops/apps/app-of-apps.yaml)"
    render | kubectl apply -f -
  fi

  # Pick up the argocd-cm health patch applied above (repo-server caches
  # resource.customizations.health.* on startup).
  run_or_echo kubectl -n "$ARGOCD_NS" rollout restart deploy/argocd-repo-server

  echo "=========================================================================="
  if [[ "$DRY_RUN" == "true" ]]; then
    echo " DRY RUN complete — nothing was applied."
  else
    echo " app-of-apps applied: ArgoCD (namespace \"${ARGOCD_NS}\") now owns lakehouse-root."
  fi
  echo "=========================================================================="
}

main
