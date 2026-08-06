#!/usr/bin/env bash
# chart/tests/integrity.sh — full-chart namespace + service-reference-closure
# integrity gate (spec §3.1/§8's KEY acceptance criterion).
#
# Renders the FULL chart into TWO release namespaces (`strayprobe`,
# `lakehouse-test`) under a handful of toggle combinations, and for each
# render pipes `helm template` straight into
# `chart/scripts/helm-check.py --release-namespace <ns> --no-stray strayprobe
# --service-closure`, which asserts:
#   (a) every doc's metadata.namespace == the release namespace passed to -n
#   (b) no stray hardcoded "strayprobe" literal leaks into a differently-named
#       release namespace (--no-stray is a no-op when ns IS strayprobe).
#       NOTE: the probe token is deliberately NOT "example" — the product's
#       neutral placeholder domain (chart/values-prod.example.yaml's
#       `global.domain: lakehouse.example.com`, per the customer-independence
#       genericization) legitimately contains the substring "example" in
#       every public hostname / S3 endpoint, which would otherwise trip this
#       check with false positives on every single render regardless of
#       actual namespace-hardcoding bugs. "strayprobe" does not collide with
#       any legitimate chart output.
#   (c) the service-reference graph is closed: every consumer-side reference
#       (Kafka bootstrap servers, *_URL hosts, svc.cluster.local FQDNs,
#       Route .spec.to.name, NetworkPolicy peer podSelectors) resolves to a
#       Service/pod-label-set this SAME render (or a recognized
#       operator-managed CR) actually produces — produced ⊇ consumed
#   (d) no leftover unrendered `{{ }}` template syntax
#
# Exit code is non-zero the moment ANY render+check combination fails, so
# this can be wired straight into CI (see tools/validate.sh --helm
# for the single-namespace/single-toggle-set sibling check this test
# generalizes).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
CHART="$ROOT/chart"
CHECK="$CHART/scripts/helm-check.py"
VALUES="$CHART/values-prod.example.yaml"

if ! command -v helm >/dev/null 2>&1; then
  echo "integrity.sh: FAIL — helm not found on PATH" >&2
  exit 1
fi

NAMESPACES=(strayprobe lakehouse-test)

# Toggle sets: label -> extra `helm template --set ...` args (may be empty).
# - defaults               : values-prod.example.yaml as-is (components.ai=
#                             false — the shipped production defaults).
#
# NOTE: components.ai / .superset are STILL DELIBERATELY not exercised here:
# each renders a Route whose backing Service this chart does not (yet)
# produce, so turning either on is — by design — an unsupported install
# shape that helm-check.py's strengthened Route-target closure will
# (correctly) reject. Their gating (default false, see chart/templates/
# 08-routes-tls.yaml) is what keeps the DEFAULT prod render
# Route-reference-closed; the test-the-test demonstration (see
# task-12-report.md) covers the "a dangling Route IS caught" direction
# directly. `components.jupyter` graduated OUT of this exclusion (Task 6,
# jupyterhub-per-user-sandbox slice): its backing `jupyterhub-proxy` Service
# now exists (chart/templates/23-jupyterhub.yaml) and a render-time `fail`
# guard in that same file makes `components.jupyter=true` +
# `auth.oidc.enabled=false` a hard error instead of a silently-renderable
# dangling Route — so `components.jupyter=true` alone (with the chart's
# `auth.oidc.enabled` default of `true`) is now a SUPPORTED, closure-clean
# install shape, exercised below as `jupyter-on-openshift`.
# - monitoring-oidc-on     : forces components.monitoring=true AND
#                            monitoring.grafana.oidc.enabled=true, exercising
#                            the full İzleme (D1) render — Grafana Deployment
#                            + non-expiring SA-token Secret + Thanos datasource
#                            + per-target ServiceMonitors/PodMonitors +
#                            PrometheusRule + the Keycloak grafana confidential
#                            client (10-keycloak.yaml) — through the same
#                            namespace/closure check. (monitoring is already
#                            on in values-prod.example.yaml; this toggle adds
#                            the OIDC-on Grafana + Keycloak grafana-client path
#                            on top so both render shapes are covered.)
#
# - sandbox-on-openshift   : Consumption slice c2 per-department analyst
#                            sandboxes (chart/templates/09c-zeppelin-sandbox.
#                            yaml + the per-dept Route in 08-routes-tls.yaml)
#                            — `sandbox.enabled=true` + one department,
#                            platform stays `openshift` (values-prod.example.
#                            yaml default). Proves the per-dept
#                            `zeppelin-sandbox-<name>` Route's `spec.to.name`
#                            resolves to the per-dept Service THIS SAME
#                            render produces (no dangling backend) — the
#                            08-routes-tls.yaml `zeppelin` entry's sandbox
#                            equivalent.
# - sandbox-on-vanilla     : SAME sandbox toggle as above, but
#                            `platform=vanilla` — exercises the mirrored
#                            per-dept Ingress in 08b-ingress-tls.yaml through
#                            the SAME closure check (Ingress
#                            `backend.service.name` resolution). `platform:
#                            vanilla` is otherwise UNEXERCISED by this script
#                            (values-prod.example.yaml ships `platform:
#                            openshift`), so this is also the only place the
#                            vanilla Ingress path (08b-ingress-tls.yaml) is
#                            closure-checked at all.
# - jupyter-on-openshift   : JupyterHub per-user sandbox (chart/templates/
#                            23-jupyterhub.yaml + the pre-existing `jupyter`
#                            Route in 08-routes-tls.yaml) — `components.
#                            jupyter=true` (values-prod.example.yaml's
#                            `auth.oidc.enabled` is already `true` by
#                            inherited default from chart/values.yaml, so no
#                            override needed there — this is exactly the
#                            coherent combo Task 6's render-time guard
#                            requires). Proves the `jupyter` Route's
#                            `spec.to.name: jupyterhub-proxy` resolves to the
#                            Service this SAME render produces (no dangling
#                            backend) — the follow-up-component exclusion
#                            note above no longer applies to `components.
#                            jupyter` now that its Route/Service can never
#                            disagree (Task 6 coherence guard).
SANDBOX_DEPT_JSON='[{"name":"veri","adGroup":"CN=sandbox-veri,OU=Groups,DC=example,DC=com","s3SecretName":"s3-sandbox-veri","oidcClientSecret":"devz"}]'
TOGGLE_LABELS=(
  "defaults"
  "monitoring-oidc-on"
  "sandbox-on-openshift"
  "sandbox-on-vanilla"
  "jupyter-on-openshift"
)
TOGGLE_ARGS=(
  ""
  "--set components.monitoring=true --set monitoring.grafana.oidc.enabled=true"
  "--set sandbox.enabled=true --set-json sandbox.departments=${SANDBOX_DEPT_JSON}"
  "--set platform=vanilla --set sandbox.enabled=true --set-json sandbox.departments=${SANDBOX_DEPT_JSON}"
  "--set components.jupyter=true --set jupyter.oidcClientSecret=devz --set jupyter.proxyToken=devz"
)

total=0
fail_count=0

for ns in "${NAMESPACES[@]}"; do
  for i in "${!TOGGLE_LABELS[@]}"; do
    label="${TOGGLE_LABELS[$i]}"
    args="${TOGGLE_ARGS[$i]}"
    total=$((total + 1))
    echo "=== render ns=${ns} toggles=${label} ==="
    # $args is an intentionally word-split `--set ...` argument list (or empty).
    # shellcheck disable=SC2086
    if helm template lakehouse "$CHART" -f "$VALUES" -n "$ns" $args \
        | python3 "$CHECK" - --release-namespace "$ns" --no-stray strayprobe --service-closure; then
      echo "PASS: ns=${ns} toggles=${label}"
    else
      echo "FAIL: ns=${ns} toggles=${label}" >&2
      fail_count=$((fail_count + 1))
    fi
    echo
  done
done

if [ "$fail_count" -ne 0 ]; then
  echo "integrity.sh: FAIL (${fail_count}/${total} render+check combination(s) failed)" >&2
  exit 1
fi

echo "integrity.sh: OK (${total}/${total} render+check combination(s) passed)"
