{{/*
Lakehouse — shared template helpers.

This file is the SINGLE SOURCE OF TRUTH for every in-cluster Service DNS
name used by this chart. Both the template that OWNS a Service (i.e. the
one that renders `kind: Service, metadata.name: <x>`) and every template
that CONSUMES that service (bootstrap servers, JDBC/HTTP URLs, catalog
URIs, etc.) MUST resolve the name through the helpers below — never by
retyping a literal string. This is what makes the service-reference graph
closed and namespace-portable (see chart/scripts/helm-check.py
--service-closure, and plan spec §3.1/§8).

Canonical names are verified against `prod-manifests/` (main branch):
  - kafka-kafka-bootstrap : Strimzi `Kafka` CR name is `kafka`
                            (03-kafka-strimzi.yaml) -> Strimzi convention
                            `<cluster-name>-kafka-bootstrap`.
  - nessie                : `Service` metadata.name (04-nessie-ha.yaml).
  - apicurio-registry     : `Service` metadata.name (11-apicurio-registry.yaml).
  - trino-coordinator     : `Service` metadata.name (06-trino-ha.yaml).
  - connect-connect-api   : Strimzi `KafkaConnect` CR name is `connect`
                            (12-kafka-connect.yaml) -> Strimzi convention
                            `<connect-name>-connect-api`.
  - console-backend       : `Service` metadata.name (15-lakehouse-console.yaml).
  - lakehouse-console      : Frontend `Service` metadata.name (Route target,
                            15-lakehouse-console.yaml).
  - kafka-ui              : `Service` metadata.name (16-kafka-ui.yaml).
  - pg-nessie-rw          : CNPG `Cluster` name is `pg-nessie`
                            (02-postgres-ha.yaml) -> CNPG convention
                            `<cluster-name>-rw`.
  - pg-apicurio-rw        : CNPG `Cluster` name is `pg-apicurio`
                            (11-apicurio-registry.yaml) -> CNPG convention.
  - pg-keycloak-rw        : CNPG `Cluster` name is `pg-keycloak`
                            (10-keycloak.yaml) -> CNPG convention.
  - keycloak-service      : Keycloak Operator (`k8s.keycloak.org/v2alpha1`)
                            `Keycloak` CR named `keycloak` (10-keycloak.yaml)
                            -> Operator convention `<cr-name>-service`
                            (verified against prod-manifests/10-keycloak.yaml's
                            own Route, which targets `keycloak-service`).
  - minio                 : `Service` metadata.name (chart/templates/
                            17-minio.yaml, Product Foundation Task 8). NOT
                            part of the prod-manifests/ baseline (this
                            component is DEV/PoC/e2e-only, gated by
                            `components.minio`, default false) — the name
                            `minio` is chosen to match bootstrap's own
                            default `s3-credentials.endpoint` convention
                            (`http://minio.<namespace>.svc:9000`, see
                            bootstrap/lib/secrets.sh), not an operator/CRD
                            naming convention like the others above.
*/}}

{{/*
lakehouse.name - base chart name.
*/}}
{{- define "lakehouse.name" -}}
{{- .Chart.Name -}}
{{- end -}}

{{/*
lakehouse.namespace - the ONE place templates get the release namespace.
No template should ever hardcode a namespace literal; always:
  namespace: {{ include "lakehouse.namespace" . }}
*/}}
{{- define "lakehouse.namespace" -}}
{{- .Release.Namespace -}}
{{- end -}}

{{/*
lakehouse.labels - common labels applied to every resource this chart renders.
*/}}
{{- define "lakehouse.labels" -}}
app.kubernetes.io/part-of: lakehouse
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/instance: {{ .Release.Name }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
{{- end -}}

{{/*
lakehouse.fqdn - fully-qualified in-cluster DNS name for a short service name.
Usage:
  {{ include "lakehouse.fqdn" (dict "svc" (include "lakehouse.svc.nessie" .) "context" .) }}
  -> "nessie.<release-namespace>.svc.cluster.local"
*/}}
{{- define "lakehouse.fqdn" -}}
{{- $svc := .svc -}}
{{- $ctx := .context -}}
{{- printf "%s.%s.svc.cluster.local" $svc (include "lakehouse.namespace" $ctx) -}}
{{- end -}}

{{/*
Canonical Service names. Every one of these MUST be used both by the
template that owns the matching `kind: Service` (or the operator-managed
CR whose reconciler creates that Service, e.g. Strimzi/CNPG) and by every
consumer template that needs to reach it. Do not inline these strings
anywhere else.
*/}}
{{- define "lakehouse.svc.kafkaBootstrap" -}}kafka-kafka-bootstrap{{- end -}}
{{- define "lakehouse.svc.nessie" -}}nessie{{- end -}}
{{- define "lakehouse.svc.apicurio" -}}apicurio-registry{{- end -}}
{{- define "lakehouse.svc.trino" -}}trino-coordinator{{- end -}}
{{- define "lakehouse.svc.connect" -}}connect-connect-api{{- end -}}
{{- define "lakehouse.svc.consoleBackend" -}}console-backend{{- end -}}
{{- define "lakehouse.svc.consoleFrontend" -}}lakehouse-console{{- end -}}
{{- define "lakehouse.svc.kafkaUi" -}}kafka-ui{{- end -}}
{{- define "lakehouse.svc.pgNessieRw" -}}pg-nessie-rw{{- end -}}
{{- define "lakehouse.svc.pgApicurioRw" -}}pg-apicurio-rw{{- end -}}
{{- define "lakehouse.svc.pgKeycloakRw" -}}pg-keycloak-rw{{- end -}}
{{- define "lakehouse.svc.keycloak" -}}keycloak-service{{- end -}}
{{- define "lakehouse.svc.minio" -}}minio{{- end -}}

{{/*
lakehouse.routeHost - public route hostname for a route's logical short name
(e.g. "trino", "bi", "auth"), derived from `.Values.global.domain`. This is
the SINGLE SOURCE for BOTH a Route's own `spec.host` (chart/templates/
08-routes-tls.yaml, and the Keycloak Route in 10-keycloak.yaml) AND every
Keycloak OIDC client's `redirectUris`/`webOrigins` (10-keycloak.yaml
KeycloakRealmImport) that must reach the SAME public endpoint — so a Route's
host and its corresponding OIDC client's redirect host can never drift apart
(task brief: "route host == redirect URI host"). The logical short name is
NOT necessarily the in-cluster Service name (e.g. "bi" for the `superset`
Service, "auth" for Keycloak) — it is the public-facing subdomain label used
verbatim in prod-manifests/08-routes-tls.yaml + 10-keycloak.yaml.
Usage:
  {{ include "lakehouse.routeHost" (dict "name" "trino" "context" .) }}
  -> "trino.<global.domain>"
*/}}
{{- define "lakehouse.routeHost" -}}
{{- $name := .name -}}
{{- $ctx := .context -}}
{{- printf "%s.%s" $name $ctx.Values.global.domain -}}
{{- end -}}

{{/*
lakehouse.isOpenShift - "true" iff `.Values.platform` == "openshift", empty
(falsy) otherwise. Templates use:
  {{- if include "lakehouse.isOpenShift" . }}
to gate every `kind: Route` (OpenShift-only) alongside its Ingress
equivalent in chart/templates/08b-ingress-tls.yaml, which gates on
`{{- if not (include "lakehouse.isOpenShift" .) }}` — the SAME helper,
never a second literal `eq .Values.platform ...` check, so the two paths can
never both render (or both stay empty) for the same logical endpoint. See
`.Values.platform`'s own doc comment (values.yaml) for the two valid values.
Fails loud (matching _prereq-check.tpl's style) on any other value: an
unrecognized `platform` must never silently render neither a Route nor an
Ingress for the same endpoint.
*/}}
{{- define "lakehouse.isOpenShift" -}}
{{- if eq .Values.platform "openshift" -}}
true
{{- else if ne .Values.platform "vanilla" -}}
{{ fail (printf "lakehouse: invalid .Values.platform %q — must be \"openshift\" or \"vanilla\" (see chart/values.yaml's `platform` doc comment)." .Values.platform) }}
{{- end -}}
{{- end -}}

{{/*
lakehouse.secretsMode - passthrough validation for `.Values.secrets.mode`,
same fail-loud style as `lakehouse.isOpenShift` above. Returns the mode
string unchanged when it is one of the three valid values ("generated",
"external", "manual" — see `.Values.secrets.mode`'s own doc comment,
values.yaml); `fail`s the render loudly on anything else, so a typo/bogus
mode can never silently fall through to "chart renders nothing" (which
looks identical to a correctly-configured `generated`/`manual` mode until an
operator notices missing secrets at runtime). Templates that branch on
`.Values.secrets.mode` (chart/templates/15-external-secrets.yaml,
NOTES.txt) call this FIRST, unconditionally, so the check runs on every
render regardless of which components/modes are otherwise gated off.
Usage:
  {{ include "lakehouse.secretsMode" . }}
*/}}
{{- define "lakehouse.secretsMode" -}}
{{- $mode := .Values.secrets.mode -}}
{{- if has $mode (list "generated" "external" "manual") -}}
{{- $mode -}}
{{- else -}}
{{ fail (printf "lakehouse: invalid .Values.secrets.mode %q — must be \"generated\", \"external\", or \"manual\" (see chart/values.yaml's `secrets.mode` doc comment)." $mode) }}
{{- end -}}
{{- end -}}

{{/*
lakehouse.podSecurityContext / lakehouse.containerSecurityContext —
OpenShift restricted-v2 SCC + PSA-restricted uyumlu securityContext. Yalnızca
`.Values.global.security.restrictedSecurityContext` true iken çıktı üretir
(aksi halde BOŞ → şablon değişmez, microk8s'te ve mevcut testlerde etkisiz).
runAsUser BİLİNÇLİ olarak SET EDİLMEZ — OpenShift SCC rastgele (namespace'e
atanmış) UID verir; sabit UID SCC ihlali olur. Kullanım (pod spec):
  {{- $psc := include "lakehouse.podSecurityContext" . | trim }}
  {{- if $psc }}
  securityContext:
    {{- $psc | nindent <n> }}
  {{- end }}
*/}}
{{- define "lakehouse.podSecurityContext" -}}
{{- if .Values.global.security.restrictedSecurityContext -}}
runAsNonRoot: true
seccompProfile:
  type: RuntimeDefault
{{- end -}}
{{- end -}}

{{- define "lakehouse.containerSecurityContext" -}}
{{- if .Values.global.security.restrictedSecurityContext -}}
allowPrivilegeEscalation: false
runAsNonRoot: true
capabilities:
  drop: ["ALL"]
seccompProfile:
  type: RuntimeDefault
{{- end -}}
{{- end -}}

{{/*
S3 internal-CA truststore wiring (gated: .Values.global.security.s3TrustBundle
.enabled). S3 endpoint kurum Internal-CA ile imzalıysa JVM S3
istemcileri SSLHandshakeException verir; bu helper'lar sistem cacerts'in bir
kopyasına CA'yı ekleyip (init-container + keytool) JVM'i o truststore'a
yönlendirir. Trino referans implementasyonudur; aynı 4 helper Nessie/Connect/
Spark'a da uygulanabilir (bkz. gitops/README runbook).
  - s3TrustVolumes       : ConfigMap (CA) + emptyDir (built store) — pod volumes
  - s3TrustStoreMount    : emptyDir mount (ana container /work/s3-trust)
  - s3TrustInitContainer : dict {context, image} — cacerts kopyala + CA import
  - s3TrustJvmProps      : -Djavax.net.ssl.trustStore=... (JVM opts'a eklenir)
*/}}
{{- define "lakehouse.s3TrustVolumes" -}}
{{- if .Values.global.security.s3TrustBundle.enabled }}
- name: s3-trust-ca
  configMap:
    name: {{ .Values.global.security.s3TrustBundle.configMapName }}
- name: s3-trust-store
  emptyDir: {}
{{- end }}
{{- end -}}

{{- define "lakehouse.s3TrustStoreMount" -}}
{{- if .Values.global.security.s3TrustBundle.enabled }}
- name: s3-trust-store
  mountPath: /work/s3-trust
{{- end }}
{{- end -}}

{{- define "lakehouse.s3TrustInitContainer" -}}
{{- $ctx := .context -}}
{{- if $ctx.Values.global.security.s3TrustBundle.enabled }}
- name: s3-trust-init
  image: {{ .image }}
  command: ["sh", "-c"]
  args:
    - |
      set -e
      SRC="${JAVA_HOME:-/usr/lib/jvm/jre}/lib/security/cacerts"
      [ -f "$SRC" ] || SRC="$(find / -path '*/security/cacerts' 2>/dev/null | head -1)"
      cp "$SRC" /work/s3-trust/cacerts
      keytool -importcert -noprompt -alias s3-endpoint \
        -file /mnt/s3-ca/{{ $ctx.Values.global.security.s3TrustBundle.key }} \
        -keystore /work/s3-trust/cacerts -storepass changeit
  volumeMounts:
    - {name: s3-trust-ca, mountPath: /mnt/s3-ca, readOnly: true}
    - {name: s3-trust-store, mountPath: /work/s3-trust}
  {{- $csc := include "lakehouse.containerSecurityContext" $ctx | trim }}
  {{- if $csc }}
  securityContext:
    {{- $csc | nindent 4 }}
  {{- end }}
{{- end }}
{{- end -}}

{{- define "lakehouse.s3TrustJvmProps" -}}
{{- if .Values.global.security.s3TrustBundle.enabled -}}
-Djavax.net.ssl.trustStore=/work/s3-trust/cacerts
-Djavax.net.ssl.trustStorePassword=changeit
{{- end -}}
{{- end -}}

{{/*
lakehouse.s3BucketFromUri - extract the bucket name from an `s3://bucket/key...`
value (e.g. "s3://depo/warehouse" -> "depo"). Used by chart/templates/
18-bucket-init.yaml to COMPUTE its default bucket list directly from the
`s3://` locations already embedded elsewhere in this file
(`nessie.catalog.warehouse.location` / `nessie.catalog.rawdata.location` /
`postgres.*.backup.destinationPath`) instead of duplicating those bucket
names a second time as a hand-maintained static list — one source, so the
two can never silently drift apart (see that template's header comment).
Usage:
  {{ include "lakehouse.s3BucketFromUri" "s3://depo/warehouse" }} -> "depo"
*/}}
{{- define "lakehouse.s3BucketFromUri" -}}
{{- . | trimPrefix "s3://" | splitList "/" | first -}}
{{- end -}}

{{- /* lakehouse.s3.s3aEndpoint - resolve the fs.s3a.endpoint value Spark
       (and the Console-rendered batch/s3 job) should use:
         1. `storage.s3.endpoint` if set explicitly - always wins.
         2. else, when the bundled MinIO is enabled (`components.minio`),
            auto-derive its in-cluster endpoint
            (http://<lakehouse.svc.minio>.<namespace>.svc:9000) - so a
            MinIO-bundling install (values.storage.s3.endpoint left empty)
            doesn't regress: without this, `mainApplicationFile: s3a://...`
            would resolve against hadoop-aws's default (AWS) endpoint
            instead of the in-cluster MinIO and every Spark job would fail
            before its code even runs.
         3. else empty (external S3 with no endpoint override needed -
            hadoop-aws's own AWS-endpoint default is correct there).
       Usage: {{ include "lakehouse.s3.s3aEndpoint" . }} */ -}}
{{- define "lakehouse.s3.s3aEndpoint" -}}
{{- if .Values.storage.s3.endpoint -}}
{{- .Values.storage.s3.endpoint -}}
{{- else if .Values.components.minio -}}
{{- printf "http://%s.%s.svc:9000" (include "lakehouse.svc.minio" .) (include "lakehouse.namespace" .) -}}
{{- end -}}
{{- end -}}

{{- /* Base Spark config for all first-party spark jobs (was spark-defaults.conf,
       retired: spark-operator v2.x owns /opt/spark/conf). Emit under sparkConf. */ -}}
{{- define "lakehouse.spark.baseConf" -}}
spark.sql.extensions: "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
spark.jars.ivy: "/tmp/.ivy2"
spark.hadoop.fs.s3a.path.style.access: "true"
{{- $ep := include "lakehouse.s3.s3aEndpoint" . }}
{{- with $ep }}
spark.hadoop.fs.s3a.endpoint: {{ . | quote }}
{{- end }}
spark.hadoop.fs.s3a.aws.credentials.provider: "com.amazonaws.auth.EnvironmentVariableCredentialsProvider"
spark.sql.catalog.lakehouse: "org.apache.iceberg.spark.SparkCatalog"
spark.sql.catalog.lakehouse.catalog-impl: "org.apache.iceberg.rest.RESTCatalog"
spark.sql.catalog.lakehouse.uri: "http://{{ include "lakehouse.svc.nessie" . }}:19120/iceberg/"
spark.sql.catalog.lakehouse.warehouse: "{{ .Values.nessie.catalog.warehouse.location }}"
spark.sql.catalog.lakehouse.io-impl: "org.apache.iceberg.aws.s3.S3FileIO"
spark.sql.catalog.rawlake: "org.apache.iceberg.spark.SparkCatalog"
spark.sql.catalog.rawlake.catalog-impl: "org.apache.iceberg.rest.RESTCatalog"
spark.sql.catalog.rawlake.uri: "http://{{ include "lakehouse.svc.nessie" . }}:19120/iceberg/"
spark.sql.catalog.rawlake.warehouse: "rawdata"
spark.sql.catalog.rawlake.io-impl: "org.apache.iceberg.aws.s3.S3FileIO"
{{- end -}}
{{- /* AWS creds env: map the S3 secret's dash-keys to AWS SDK env names +HOME. */ -}}
{{- define "lakehouse.spark.awsEnv" -}}
- {name: AWS_ACCESS_KEY_ID,     valueFrom: {secretKeyRef: {name: {{ .Values.storage.s3.secretName }}, key: access-key-id}}}
- {name: AWS_SECRET_ACCESS_KEY, valueFrom: {secretKeyRef: {name: {{ .Values.storage.s3.secretName }}, key: secret-access-key}}}
- {name: AWS_ENDPOINT_URL_S3,   valueFrom: {secretKeyRef: {name: {{ .Values.storage.s3.secretName }}, key: endpoint}}}
- {name: AWS_REGION,            valueFrom: {secretKeyRef: {name: {{ .Values.storage.s3.secretName }}, key: region}}}
- {name: HOME, value: /tmp}
{{- end -}}
