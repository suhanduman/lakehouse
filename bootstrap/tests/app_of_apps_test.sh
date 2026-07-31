#!/usr/bin/env bash
# bootstrap/tests/app_of_apps_test.sh — plain-bash test for
# bootstrap/app-of-apps.sh (Layer 1). Follows bootstrap_test.sh's style and
# PATH-shim technique: a temp dir is prepended to PATH with a fake `kubectl`
# that records every invocation (one line per call) to $CALL_LOG and exits 0,
# letting app-of-apps.sh's real control-flow run end-to-end without ever
# touching a live cluster.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
APP_OF_APPS="$ROOT/bootstrap/app-of-apps.sh"

FAIL=0

pass() { echo "OK: $1"; }
fail() {
  echo "FAIL: $1" >&2
  FAIL=1
}

TMPROOT="$(mktemp -d)"
trap 'rm -rf "$TMPROOT"' EXIT

# make_shim_dir DIR — populates DIR with a fake kubectl that logs its full
# argument list to $CALL_LOG (one call per line) and exits 0 for every
# subcommand. Sufficient here: these tests only assert on what was (or
# wasn't) *called*, never on cluster state.
make_shim_dir() {
  local dir="$1"
  mkdir -p "$dir"

  cat > "$dir/kubectl" <<'SHIM'
#!/usr/bin/env bash
echo "kubectl $*" >> "${CALL_LOG:?CALL_LOG not set}"
exit 0
SHIM
  chmod +x "$dir/kubectl"
}

# --------------------------------------------------------------------------
# 1. --help exits 0 and mentions every flag.
# --------------------------------------------------------------------------
if help_out=$("$APP_OF_APPS" --help 2>&1); then
  pass "--help exits 0"
else
  fail "--help exited non-zero"
  help_out=""
fi

for flag in --platform-repo --pipeline-repo --argocd-namespace --dry-run --help; do
  if grep -qF -- "$flag" <<<"$help_out"; then
    pass "--help mentions ${flag}"
  else
    fail "--help does not mention ${flag}"
  fi
done

# --------------------------------------------------------------------------
# 2. Missing --platform-repo / --pipeline-repo fail loud (non-zero exit,
#    clear message), and never touch kubectl.
# --------------------------------------------------------------------------
SHIM_MISSING="$TMPROOT/shim_missing"
make_shim_dir "$SHIM_MISSING"
CALL_LOG_MISSING="$TMPROOT/calls_missing.log"
: > "$CALL_LOG_MISSING"

if missing_platform_out=$(PATH="$SHIM_MISSING:$PATH" CALL_LOG="$CALL_LOG_MISSING" "$APP_OF_APPS" --dry-run --pipeline-repo ssh://x/pipe.git 2>&1); then
  fail "missing --platform-repo should have exited non-zero"
else
  if grep -qiF -- "--platform-repo is required" <<<"$missing_platform_out"; then
    pass "missing --platform-repo exits non-zero with a clear message"
  else
    fail "missing --platform-repo error message unclear: $missing_platform_out"
  fi
fi

if missing_pipeline_out=$(PATH="$SHIM_MISSING:$PATH" CALL_LOG="$CALL_LOG_MISSING" "$APP_OF_APPS" --dry-run --platform-repo ssh://x/plat.git 2>&1); then
  fail "missing --pipeline-repo should have exited non-zero"
else
  if grep -qiF -- "--pipeline-repo is required" <<<"$missing_pipeline_out"; then
    pass "missing --pipeline-repo exits non-zero with a clear message"
  else
    fail "missing --pipeline-repo error message unclear: $missing_pipeline_out"
  fi
fi

# Explicitly-empty values must also fail loud (":?" guard on "$2").
if empty_platform_out=$(PATH="$SHIM_MISSING:$PATH" CALL_LOG="$CALL_LOG_MISSING" "$APP_OF_APPS" --dry-run --platform-repo "" --pipeline-repo ssh://x/pipe.git 2>&1); then
  fail "empty --platform-repo value should have exited non-zero: $empty_platform_out"
else
  pass "empty --platform-repo value exits non-zero"
fi

if [[ -s "$CALL_LOG_MISSING" ]]; then
  fail "missing/empty required-arg cases invoked kubectl unexpectedly: $(cat "$CALL_LOG_MISSING")"
else
  pass "missing/empty required-arg cases made ZERO kubectl calls"
fi

# --------------------------------------------------------------------------
# 3. --dry-run (default namespace: argocd): substituted plan has BOTH
#    repoURLs filled, NO placeholder left, targets the argocd namespace,
#    and applies nothing live (zero kubectl calls).
# --------------------------------------------------------------------------
SHIM1="$TMPROOT/shim1"
make_shim_dir "$SHIM1"
CALL_LOG1="$TMPROOT/calls1.log"
: > "$CALL_LOG1"

if dry_out=$(PATH="$SHIM1:$PATH" CALL_LOG="$CALL_LOG1" "$APP_OF_APPS" --dry-run --platform-repo ssh://x/plat.git --pipeline-repo ssh://x/pipe.git 2>&1); then
  pass "--dry-run exits 0"
else
  fail "--dry-run exited non-zero: $dry_out"
  dry_out=""
fi

if grep -qF -- "ssh://x/plat.git" <<<"$dry_out" && grep -qF -- "ssh://x/pipe.git" <<<"$dry_out"; then
  pass "--dry-run plan has both repoURLs substituted"
else
  fail "--dry-run plan missing a substituted repoURL: $dry_out"
fi

if grep -qE '<PLATFORM_REPO_URL>|<PIPELINE_REPO_URL>' <<<"$dry_out"; then
  fail "--dry-run plan still contains an unsubstituted placeholder: $dry_out"
else
  pass "--dry-run plan has NO unsubstituted placeholder left"
fi

if grep -qF -- "namespace: argocd" <<<"$dry_out"; then
  pass "--dry-run plan targets the argocd namespace (default)"
else
  fail "--dry-run plan does not target the argocd namespace: $dry_out"
fi

if grep -qF -- "namespace: openshift-gitops" <<<"$dry_out"; then
  fail "--dry-run plan still mentions the openshift-gitops placeholder namespace (default should have overridden it): $dry_out"
else
  pass "--dry-run plan does not leak the openshift-gitops placeholder namespace"
fi

if [[ -s "$CALL_LOG1" ]]; then
  fail "--dry-run recorded unexpected kubectl invocation(s) (should apply NOTHING live): $(cat "$CALL_LOG1")"
else
  pass "--dry-run applied nothing live (ZERO kubectl calls)"
fi

# --------------------------------------------------------------------------
# 4. --argocd-namespace openshift-gitops overrides the default correctly.
# --------------------------------------------------------------------------
SHIM2="$TMPROOT/shim2"
make_shim_dir "$SHIM2"
CALL_LOG2="$TMPROOT/calls2.log"
: > "$CALL_LOG2"

if osgo_out=$(PATH="$SHIM2:$PATH" CALL_LOG="$CALL_LOG2" "$APP_OF_APPS" --dry-run --platform-repo ssh://x/plat.git --pipeline-repo ssh://x/pipe.git --argocd-namespace openshift-gitops 2>&1); then
  pass "--dry-run --argocd-namespace openshift-gitops exits 0"
else
  fail "--dry-run --argocd-namespace openshift-gitops exited non-zero: $osgo_out"
  osgo_out=""
fi

if grep -qF -- "namespace: openshift-gitops" <<<"$osgo_out"; then
  pass "--argocd-namespace openshift-gitops plan targets openshift-gitops"
else
  fail "--argocd-namespace openshift-gitops plan does not target openshift-gitops: $osgo_out"
fi

if [[ -s "$CALL_LOG2" ]]; then
  fail "--dry-run --argocd-namespace openshift-gitops recorded unexpected kubectl invocation(s): $(cat "$CALL_LOG2")"
else
  pass "--dry-run --argocd-namespace openshift-gitops applied nothing live"
fi

# --------------------------------------------------------------------------
# 5. Real (non-dry-run) run: applies the argocd-cm health patch and the
#    rendered root Application via `kubectl apply -f -` (piped stdin, never
#    a path to the committed placeholder file), scoped to the given
#    namespace, and restarts argocd-repo-server to pick up the health patch.
#    Also verifies the committed gitops/apps/app-of-apps.yaml is untouched.
# --------------------------------------------------------------------------
SHIM3="$TMPROOT/shim3"
make_shim_dir "$SHIM3"
CALL_LOG3="$TMPROOT/calls3.log"
: > "$CALL_LOG3"

COMMITTED_YAML="$ROOT/gitops/apps/app-of-apps.yaml"
BEFORE_HASH="$(shasum -a 256 "$COMMITTED_YAML" | awk '{print $1}')"

if real_out=$(PATH="$SHIM3:$PATH" CALL_LOG="$CALL_LOG3" "$APP_OF_APPS" --platform-repo ssh://x/plat.git --pipeline-repo ssh://x/pipe.git 2>&1); then
  pass "real (non-dry-run) run exits 0"
else
  fail "real run exited non-zero: $real_out"
fi

AFTER_HASH="$(shasum -a 256 "$COMMITTED_YAML" | awk '{print $1}')"
if [[ "$BEFORE_HASH" == "$AFTER_HASH" ]]; then
  pass "committed gitops/apps/app-of-apps.yaml is unchanged after a real run"
else
  fail "committed gitops/apps/app-of-apps.yaml was MUTATED by a real run"
fi

if grep -qF -- "apply -f $ROOT/gitops/argocd/argocd-cm-health.yaml" "$CALL_LOG3"; then
  pass "real run applies the argocd-cm health patch"
else
  fail "real run did not apply the argocd-cm health patch: $(cat "$CALL_LOG3")"
fi

if grep -qE '^kubectl apply -f -$' "$CALL_LOG3"; then
  pass "real run applies the rendered root Application via piped stdin (-f -), not the raw placeholder file"
else
  fail "real run did not apply via piped stdin as expected: $(cat "$CALL_LOG3")"
fi

if grep -qF -- "app-of-apps.yaml" "$CALL_LOG3"; then
  fail "real run passed the committed placeholder file path directly to kubectl (should always pipe a rendered copy): $(cat "$CALL_LOG3")"
else
  pass "real run never passes the committed placeholder file path directly to kubectl"
fi

if grep -qF -- "rollout restart deploy/argocd-repo-server" "$CALL_LOG3"; then
  pass "real run restarts argocd-repo-server to pick up the health patch"
else
  fail "real run did not restart argocd-repo-server: $(cat "$CALL_LOG3")"
fi

# --------------------------------------------------------------------------
# 6. The committed argocd-cm health fragment declares the
#    `app.kubernetes.io/part-of: argocd` label. That ConfigMap is named
#    `argocd-cm`, so applying it (here via `kubectl apply`) creates or
#    overwrites argocd-cm; WITHOUT the label, ArgoCD's configmap informer —
#    which filters on `app.kubernetes.io/part-of=argocd` — never sees it,
#    and the application-controller CrashLoopBackOffs with
#    `error loading cache settings: configmap "argocd-cm" not found` even
#    though the CM exists. (Live-verify finding, GitOps Slice B-II 2026-07-31.)
# --------------------------------------------------------------------------
HEALTH_YAML="$ROOT/gitops/argocd/argocd-cm-health.yaml"
if grep -qE '^[[:space:]]+app\.kubernetes\.io/part-of:[[:space:]]*argocd[[:space:]]*$' "$HEALTH_YAML"; then
  pass "argocd-cm health fragment declares the app.kubernetes.io/part-of=argocd label"
else
  fail "argocd-cm health fragment is MISSING the app.kubernetes.io/part-of=argocd label (ArgoCD's configmap informer won't see argocd-cm)"
fi

# --------------------------------------------------------------------------
if [[ "$FAIL" -ne 0 ]]; then
  echo "app_of_apps_test.sh: FAIL" >&2
  exit 1
fi
echo "app_of_apps_test.sh: OK"
