{{/*
Lakehouse — chart prerequisite CRD check. Verifies the
cluster-scoped OLM operator prerequisites
documented in `operators/README.md` (Strimzi, CloudNativePG, Spark Operator,
Keycloak Operator, ExternalSecrets Operator, cert-manager — see
`operators/*.yaml`) are actually installed BEFORE this chart submits CRs that
depend on their CRDs (`Kafka`, CNPG `Cluster`, `SparkApplication`/
`ScheduledSparkApplication`, `Keycloak`, `ExternalSecret`, `Certificate`).

Two independent mechanisms exist, both gated by the SAME `prereqCheck.enabled`
value (see chart/values.yaml):

  1. THIS file's `lakehouse.prereqCheck` template — a template-time
     `.Capabilities.APIVersions.Has` check, `include`d once from
     chart/templates/00-namespace.yaml (that file always renders, so the
     check always runs when enabled). This is the fast, in-process check:
     during a REAL `helm install`/`helm upgrade`, Helm is always actually
     connected to the target cluster, so `.Capabilities.APIVersions`
     correctly reflects the live set of installed API groups/CRDs.

  2. chart/templates/prereq-check-job.yaml — a `pre-install`/`pre-upgrade`
     hook Job that independently re-verifies the same CRDs from inside the
     cluster via `kubectl get crd`. More robust (no dependency on Helm's
     Capabilities semantics), and gives a clear, inspectable
     (`oc logs job/...`) failure if the template-time check were ever
     bypassed (e.g. `helm upgrade --disable-openapi-validation`-style flows).

WHY `prereqCheck.enabled` DEFAULTS TO `false` (chart/values.yaml AND
chart/values-prod.example.yaml): `.Capabilities.APIVersions.Has` can NEVER
distinguish "not connected to any cluster at all" (e.g. `helm template`,
this chart's own CI harness — prod-manifests/validate.sh --helm) from
"connected to a real cluster, CRD genuinely missing" — both cases report
`Has == false` identically to the template engine (verified empirically:
offline `helm template` always returns `false` for any non-builtin
CRD group, regardless of what a real cluster would say). A default of
`true` would therefore hard-fail `helm template` in CI on EVERY run, even
against a correctly-provisioned cluster — there's no template-native way to
tell the two situations apart (a well known Helm limitation, not something
this chart can work around). So this check ships OFF by default; a real
deployer turns it on explicitly, once `operators/` has been applied and
every CSV shows `Succeeded` (see operators/README.md "Doğrulama"):

  helm install lakehouse chart/ -f values-<env>.yaml -n example \
    --set prereqCheck.enabled=true

The six CRD/APIVersion strings checked below are the exact `apiVersion`
values this chart's OWN templates already emit for the corresponding CRs
(grep-verified against chart/templates/*.yaml, not guessed):
  - kafka.strimzi.io/<.Values.versions.strimziApi>  (parametrized, default
    `v1` — 03-kafka-strimzi.yaml, 12-kafka-connect.yaml, 13-connectors.yaml,
    14-nginx-ingest.yaml, console/kafka-ui.yaml all emit
    `apiVersion: kafka.strimzi.io/{{ .Values.versions.strimziApi }}`; this
    check reads the SAME value so it never drifts from what the chart
    actually renders — see values.yaml's "COHERENT STRIMZI VERSION TRIPLE"
    comment)
  - postgresql.cnpg.io/v1           (02-postgres-ha.yaml, 10-keycloak.yaml, 11-apicurio-registry.yaml)
  - sparkoperator.k8s.io/v1beta2    (05-spark-operator.yaml, 14-nginx-ingest.yaml)
  - k8s.keycloak.org/v2alpha1       (10-keycloak.yaml — NOTE: the modern
    Keycloak Operator's API group, NOT the legacy `keycloak.org/v1alpha1`
    Operator SDK v1 CRDs; see operators/05-keycloak-operator.yaml POC-DOĞRULA
    note on picking a package that ships this exact group)
  - cert-manager.io/v1              (10-keycloak.yaml Certificate CR)
  - external-secrets.io/v1beta1      (chart/templates/15-external-secrets.yaml
    renders `ExternalSecret` CRs ONLY when `.Values.secrets.mode ==
    "external"` — the other two modes, `generated` (default, the
    zero-manual-step / zero-ESO product install path) and `manual`, never
    touch this CRD group at all. Unlike the six unconditional checks above,
    THIS check is itself gated on `secrets.mode == "external"` (same
    conditional-check pattern as the `monitoring.coreos.com/v1` check below,
    which is gated on `components.monitoring`) — a `generated`-mode install
    must NOT be hard-blocked by a prereq for an operator it doesn't need.
    See operators/README.md's "Ön koşullar" table for the ExternalSecrets
    Operator prerequisite, which only applies when a release actually runs
    `secrets.mode: external`.
*/}}

{{- define "lakehouse.prereqCheck" -}}
{{- if .Values.prereqCheck.enabled -}}
{{- $ctx := . -}}
{{- $missing := list -}}
{{- $strimziApiVersion := printf "kafka.strimzi.io/%s" $ctx.Values.versions.strimziApi -}}
{{- if not ($ctx.Capabilities.APIVersions.Has $strimziApiVersion) -}}
{{- $missing = append $missing (printf "Strimzi (Kafka) -> %s" $strimziApiVersion) -}}
{{- end -}}
{{- if not ($ctx.Capabilities.APIVersions.Has "postgresql.cnpg.io/v1") -}}
{{- $missing = append $missing "CloudNativePG -> postgresql.cnpg.io/v1" -}}
{{- end -}}
{{- if not ($ctx.Capabilities.APIVersions.Has "sparkoperator.k8s.io/v1beta2") -}}
{{- $missing = append $missing "Spark Operator -> sparkoperator.k8s.io/v1beta2" -}}
{{- end -}}
{{- if not ($ctx.Capabilities.APIVersions.Has "k8s.keycloak.org/v2alpha1") -}}
{{- $missing = append $missing "Keycloak Operator -> k8s.keycloak.org/v2alpha1" -}}
{{- end -}}
{{- if not ($ctx.Capabilities.APIVersions.Has "cert-manager.io/v1") -}}
{{- $missing = append $missing "cert-manager -> cert-manager.io/v1" -}}
{{- end -}}
{{/*
  ExternalSecrets Operator: ONLY required when `secrets.mode: external`
  (chart/templates/15-external-secrets.yaml is the only template that emits
  `ExternalSecret` CRs, and only in that mode) — mirrors the SAME
  conditional-check pattern as the `monitoring.coreos.com/v1` check just
  below (gated on `components.monitoring`). A `generated`-mode install (the
  zero-manual-step default) must not be hard-blocked by a prereq for an
  operator it never touches.
*/}}
{{- if eq (include "lakehouse.secretsMode" $ctx) "external" -}}
{{- if not ($ctx.Capabilities.APIVersions.Has "external-secrets.io/v1beta1") -}}
{{- $missing = append $missing "ExternalSecrets Operator -> external-secrets.io/v1beta1" -}}
{{- end -}}
{{- end -}}
{{/*
  İzleme (D1): ONLY when components.monitoring is on does this chart
  emit ServiceMonitor/PodMonitor/PrometheusRule CRs (monitoring.coreos.com/v1),
  which are supplied by OpenShift user-workload-monitoring (UWM) /
  Prometheus-Operator — NOT by operators/. So this CRD is checked
  conditionally (unlike the six always-on operator CRDs above), mirroring the
  same `.Capabilities.APIVersions.Has` pattern. See operators/README.md's UWM
  prerequisite (enableUserWorkloadMonitoring in cluster-monitoring-config).
*/}}
{{- if $ctx.Values.components.monitoring -}}
{{- if not ($ctx.Capabilities.APIVersions.Has "monitoring.coreos.com/v1") -}}
{{- $missing = append $missing "UWM/Prometheus-Operator -> monitoring.coreos.com/v1 (ServiceMonitor/PodMonitor/PrometheusRule); enable OpenShift user-workload-monitoring first — see operators/README.md (enableUserWorkloadMonitoring)" -}}
{{- end -}}
{{- end -}}
{{- if $missing -}}
{{ fail (printf "lakehouse prereq-check: missing required operator CRD(s): %s. Install the cluster-scoped operator prerequisites FIRST — see operators/README.md (oc apply -f operators/, then wait for `oc get csv -A` to show Succeeded for each), and only then retry this helm install/upgrade. If you are intentionally rendering without a live cluster (helm template / CI), set --set prereqCheck.enabled=false (the bundled default)." (join ", " $missing)) }}
{{- end -}}
{{- end -}}
{{- end -}}
