{{/*
lakehouse.zeppelin.instance — reusable Zeppelin instance body.

Extracted from chart/templates/09-zeppelin.yaml (pure refactor, byte-identical
for the shared instance) as a named template. Sandbox v2 (Task 5, 2026-08-07-
sandbox-v2) retired the per-department sandbox Zeppelin instances that used
to also call this define with a `sandbox: true` arm (chart/templates/09c-
zeppelin-sandbox.yaml, now deleted) — there is only ONE Zeppelin instance
left (the shared one, called from 09-zeppelin.yaml), and its %pyspark
interpreter now carries the UNIFIED sandbox write catalog directly, gated on
`.Values.sandbox.enabled`, instead of a per-instance sandbox arm.

Call convention: `.` inside this define is the ARGUMENT DICT passed to
`include`, NOT the chart root — every `.Values`/`.Release`/`.Chart` reference
and every nested `include "helper" .` MUST go through `$ctx` (rebound below
from the dict's `ctx` key, which callers set to `.` / `$` at the call site).

Dict keys:
  ctx          - chart root context (`.` at the call site)
  name         - instance name; becomes the PVC/Deployment/Service/PDB/
                 spark-defaults/Shiro/trino-interpreter ConfigMap name prefix
                 AND the trino-oidc-credentials Secret name prefix (the
                 shared instance passes "zeppelin", preserving today's
                 literal names).
  s3SecretName - name of the Secret holding S3 access-key-id/secret-access-
                 key/endpoint/region (today: .Values.storage.s3.secretName)
  oauth2ClientId, oidcSecretName - the READ-ONLY Nessie machine-auth identity
                 (Task 6) for the `lakehouse`/`rawlake` catalogs: the shared
                 instance passes `"svc-zeppelin-nessie"` /
                 `"nessie-machine-auth-credentials"`.
  oidcSecretKey - key inside `oidcSecretName` holding the OAuth2 client
                 secret. Defaults to `client-secret` when omitted; the
                 shared instance passes `"zeppelin"` — its secret material
                 lives in the shared `nessie-machine-auth-credentials`
                 Secret (chart/templates/22-nessie-machine-auth.yaml), which
                 keys all 4 machine-auth identities by engine name, not by
                 the literal `client-secret`.

  Nessie machine-auth (Task 6): when `$ctx.Values.auth.oidc.enabled` is true,
  the instance gets an OAuth2 block on its `lakehouse`/`rawlake` catalogs —
  a sed→tmpfs initContainer resolves the OAuth2 client-secret + OIDC
  issuer-url placeholders at pod start into a tmpfs that `SPARK_CONF_DIR`
  points at — with the READ-ONLY `svc-zeppelin-nessie` identity (passed as
  `oauth2ClientId`). Read-only-ness itself is enforced server-side by
  Nessie's CEL authorization rule (Task 3); this wiring only makes the
  Zeppelin's %spark interpreter AUTHENTICATE for reads.

  Sandbox v2 (Task 5): when `$ctx.Values.sandbox.enabled` is ALSO true, the
  SAME sed→tmpfs initContainer additionally resolves a second OAuth2
  client-secret placeholder and the rendered spark-defaults.conf gets a
  THIRD catalog (`.Values.sandbox.namespace`, e.g. "sandbox") — a WRITE
  catalog, unified `svc-sandbox` identity, unified `sandbox-oidc-
  credentials` Secret (chart/templates/23-jupyterhub.yaml). This requires
  `auth.oidc.enabled` (the sandbox catalog's OAuth2 wiring depends on the
  SAME tmpfs mechanism/OIDC issuer as the read-only catalogs above) — a
  render-time `fail` guard below enforces this instead of letting a
  `sandbox.enabled=true` + `auth.oidc.enabled=false` install crash-loop at
  runtime on a missing Secret.

  Warehouse value (whole-branch review C1 fix, 2026-08-07-sandbox-v2 final
  review — supersedes commit 73704c4's `s3a://<bucket>/` choice): the
  sandbox catalog's `warehouse` is the REGISTERED NAME
  (`.Values.sandbox.namespace`, e.g. "sandbox" — the Nessie
  NESSIE_CATALOG_WAREHOUSES_SANDBOX_LOCATION registry key,
  chart/templates/04-nessie-ha.yaml), the SAME NAME Trino's sandbox.
  properties (`iceberg.rest-catalog.warehouse=sandbox`, 06-trino-ha.yaml)
  and the PyIceberg helper (images/jupyter/lakehouse_nb.py, `warehouse=
  NESSIE_SANDBOX_CATALOG`) already use — mirrored byte-for-byte in that same
  file's `_spark_conf()`. An `s3a://<bucket>/` LOCATION string here matches
  neither the registered NAME nor the exact registered LOCATION
  (`s3://sandbox/warehouse`, note the extra `/warehouse` suffix and
  `s3://` vs `s3a://` scheme), so Nessie's REST catalog falls back to the
  DEFAULT warehouse (`s3://depo/warehouse`) — Spark sandbox writes would
  silently land in the PROD `depo` bucket (isolation breach).
*/}}
{{- define "lakehouse.zeppelin.instance" -}}
{{- $ctx := .ctx -}}
{{- $name := .name -}}
{{- $s3SecretName := .s3SecretName -}}
{{- $nessieOAuth := $ctx.Values.auth.oidc.enabled -}}
{{- $sandboxWrite := $ctx.Values.sandbox.enabled -}}
{{- if and $sandboxWrite (not $ctx.Values.auth.oidc.enabled) }}
{{- fail "components.zeppelin + sandbox.enabled requires auth.oidc.enabled — the shared Zeppelin's unified sandbox catalog authenticates to Nessie via OAuth2 (svc-sandbox)" }}
{{- end }}
{{- $oidcSecretKey := default "client-secret" .oidcSecretKey -}}
{{- $groupRolesMap := printf "%s:admin,\\\n      %s:analyst,\\\n      %s:student\"" $ctx.Values.zeppelin.ad.groups.admin $ctx.Values.zeppelin.ad.groups.analyst $ctx.Values.zeppelin.ad.groups.student -}}
# No separate cache PVC needed since it ships baked into the Spark binary image.
# Shared home for notebooks — RWX so it works across multiple replicas.
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: {{ $name }}-notebooks-pvc
  namespace: {{ include "lakehouse.namespace" $ctx }}
spec:
  accessModes: [ReadWriteMany]
  resources:
    requests: {storage: {{ $ctx.Values.zeppelin.notebooks.storage | quote }}}
  # storageClassName empty (default, `zeppelin.storageClassName`) = the
  # cluster's default StorageClass — no longer coupled to the opt-in
  # `storage.classes.fileRwx` SC-creation block / `components.storage`
  # (storage-neutrality convention, Product Foundation task 6.5).
{{- with $ctx.Values.zeppelin.storageClassName }}
  storageClassName: {{ . }}
{{- end }}
---
{{- if $ctx.Values.zeppelin.ad.bindPassword }}
# AD bind (service account) secret template -- rendered ONLY when a real
# `zeppelin.ad.bindPassword` is explicitly supplied (dev/test convenience).
# Default is empty: in that case this Secret is NOT rendered at all, and the
# SAME name/keys (referenced by the Deployment's `secretKeyRef`s below
# regardless of which path created it) must be provisioned out-of-band
# (ExternalSecret/Vault or `oc create secret generic ad-bind-credentials
# --from-literal=...`) per `.Values.secrets.mode`, BEFORE install.
apiVersion: v1
kind: Secret
metadata:
  name: ad-bind-credentials
  namespace: {{ include "lakehouse.namespace" $ctx }}
type: Opaque
stringData:
  # Read-only AD service account:
  #   CN=svc-lakehouse-ad,OU=Service Accounts,DC=example,DC=com
  bind-dn: {{ $ctx.Values.zeppelin.ad.bindDn | quote }}
  bind-password: {{ $ctx.Values.zeppelin.ad.bindPassword | quote }}
  user-search-base: {{ $ctx.Values.zeppelin.ad.userSearchBase | quote }}   # OU=Users,DC=example,DC=com
  group-search-base: {{ $ctx.Values.zeppelin.ad.groupSearchBase | quote }} # OU=Groups,DC=example,DC=com
  ad-url: {{ $ctx.Values.zeppelin.ad.url | quote }}
{{- end }}
---
{{- /*
Task 5 fix: this Secret's name is per-instance (`<name>-trino-oidc-
credentials`) — NOT the bare literal `zeppelin-trino-oidc-credentials` it
had before — because it renders once per `lakehouse.zeppelin.instance` call
regardless of `sandbox` (unlike `ad-bind-credentials` above, which is
shared-gated instead). A literal name would collide (duplicate kind/name)
once any sandbox department is added alongside the shared c1 instance. For
the shared instance (`name: "zeppelin"`) this is byte-identical to the old
literal name. Go-template comment (stripped before render) so it documents
the fix without touching the shared instance's rendered bytes.
*/}}
{{- if and $ctx.Values.components.zeppelin $ctx.Values.zeppelin.trino.oidc.clientSecret }}
# svc-zeppelin-trino confidential client secret — single source with the
# Keycloak client (10-keycloak.yaml) via `.Values.zeppelin.trino.oidc.
# clientSecret`. Same dev-convenience contract as ad-bind-credentials/
# superset-trino-oidc-credentials above: rendered ONLY when a real secret is
# supplied; otherwise provision this Secret's name/keys out-of-band before
# install. Read-only %trino interpreter identity (c1).
#
# rest-password (Task 5): the Zeppelin-admin AD service-account password the
# token-refresher sidecar uses for `POST /api/login` (Shiro form-login).
# Follows the SAME dev-convenience/out-of-band contract as `client-secret`
# above and `ad-bind-credentials` — if `zeppelin.trino.refresher.restPassword`
# is left empty while `clientSecret` is set (so this Secret still renders),
# the key is rendered empty and the real value must be supplied out-of-band
# under this SAME name/key before install.
apiVersion: v1
kind: Secret
metadata:
  name: {{ $name }}-trino-oidc-credentials
  namespace: {{ include "lakehouse.namespace" $ctx }}
type: Opaque
stringData:
  client-secret: {{ $ctx.Values.zeppelin.trino.oidc.clientSecret | quote }}
  rest-password: {{ $ctx.Values.zeppelin.trino.refresher.restPassword | quote }}
{{- end }}
{{- if and $ctx.Values.components.zeppelin $ctx.Values.auth.oidc.enabled }}
{{- if not $ctx.Values.superset.trino.tls.enabled }}
{{- fail "components.zeppelin + auth.oidc.enabled requires superset.trino.tls.enabled=true (the %trino interpreter connects over Trino's :8443 HTTPS listener, gated by that SAME flag — see chart/templates/06-trino-ha.yaml)" }}
{{- end }}
---
# %trino read-only JDBC interpreter seed (Consumption slice c1). Spike-
# confirmed mechanism (docs/superpowers/spikes/2026-08-04-c1-zeppelin-trino-
# interp.md §1): a direct ConfigMap mount AT conf/interpreter.json crashes
# Zeppelin (it rewrites that file at runtime; a read-only volume can't be
# rewritten). Instead this ConfigMap is mounted at a SEPARATE path (/seed)
# and copied into the writable conf/ dir by the container command override
# below, before the real entrypoint starts. trino-jdbc driver is baked into
# the image (images/zeppelin, Task 3) — `dependencies: []`, no runtime Maven
# download. `default.accessToken` starts as a placeholder; the Task-5
# sidecar PUTs the real svc-zeppelin-trino JWT via the Zeppelin REST API
# (the JDBC interpreter connects lazily on first paragraph, so the
# placeholder itself never reaches Trino).
{{- /*
Task 5 fix: this ConfigMap's name is per-instance (`<name>-trino-
interpreter`) — NOT the bare literal `zeppelin-trino-interpreter` it had
before — same duplicate-name rationale as the trino-oidc-credentials
Secret above (renders once per instance call regardless of `sandbox`).
Byte-identical for the shared instance. Go-template comment (stripped
before render) so it documents the fix without touching the shared
instance's rendered bytes.
*/}}
apiVersion: v1
kind: ConfigMap
metadata:
  name: {{ $name }}-trino-interpreter
  namespace: {{ include "lakehouse.namespace" $ctx }}
data:
  interpreter.json: |
    {
      "interpreterSettings": {
        "trino": {
          "id": "trino",
          "name": "trino",
          "group": "jdbc",
          "properties": {
            "default.url": {"name": "default.url", "value": "jdbc:trino://{{ include "lakehouse.svc.trino" $ctx }}:8443/{{ $ctx.Values.zeppelin.trino.catalog }}?SSL=true&SSLTrustStorePath=/mnt/trino-tls/ca.crt", "type": "string"},
            "default.user": {"name": "default.user", "value": "svc-zeppelin-trino", "type": "string"},
            "default.password": {"name": "default.password", "value": "", "type": "password"},
            "default.driver": {"name": "default.driver", "value": "io.trino.jdbc.TrinoDriver", "type": "string"},
            "default.accessToken": {"name": "default.accessToken", "value": "PLACEHOLDER_REPLACED_BY_REFRESHER", "type": "password"}
          },
          "status": "READY",
          "interpreterGroup": [
            {"name": "sql", "class": "org.apache.zeppelin.jdbc.JDBCInterpreter", "defaultInterpreter": false,
             "editor": {"language": "sql", "editOnDblClick": false, "completionSupport": true}}
          ],
          "dependencies": [],
          "option": {"remote": true, "port": -1, "isExistingProcess": false, "setPermission": false, "owners": [], "isUserImpersonate": false}
        }
      }
    }
{{- end }}
---
# Apache Shiro — ActiveDirectoryGroupRealm
apiVersion: v1
kind: ConfigMap
metadata:
  name: {{ $name }}-shiro
  namespace: {{ include "lakehouse.namespace" $ctx }}
data:
  shiro.ini: |
    [main]
    # AD realm — LDAPS (port 636) + TLS; AD certificate is in Zeppelin's truststore
    activeDirectoryRealm = org.apache.zeppelin.realm.ActiveDirectoryGroupRealm
    activeDirectoryRealm.systemUsername = ${AD_BIND_DN}
    activeDirectoryRealm.systemPassword = ${AD_BIND_PASSWORD}
    activeDirectoryRealm.searchBase = ${AD_USER_SEARCH_BASE}
    activeDirectoryRealm.url = ${AD_URL}
    activeDirectoryRealm.principalSuffix = {{ $ctx.Values.zeppelin.ad.principalSuffix }}

    # AD group DN → Zeppelin role mapping
    activeDirectoryRealm.groupRolesMap = "\
      {{ $groupRolesMap }}

    cacheManager = org.apache.shiro.cache.MemoryConstrainedCacheManager
    securityManager.cacheManager = $cacheManager
    sessionManager = org.apache.shiro.web.session.mgt.DefaultWebSessionManager
    sessionManager.globalSessionTimeout = {{ $ctx.Values.zeppelin.ad.sessionTimeoutMs | int64 }}   # {{ div $ctx.Values.zeppelin.ad.sessionTimeoutMs 3600000 }} hours
    securityManager.sessionManager = $sessionManager
    securityManager.realms = $activeDirectoryRealm

    shiro.loginUrl = /api/login

    [roles]
    admin = *
    analyst = *
    student = *

    [urls]
    /api/version = anon
    /api/interpreter/** = authc, roles[admin]
    /api/configurations/** = authc, roles[admin]
    /api/credential/** = authc
    /** = authc
---
# Spark interpreter config — same two catalogs + hadoop-aws as spark.yaml.
# DELIBERATELY SEPARATE from the shared `spark-defaults` ConfigMap in
# 05-spark-operator.yaml (this one does not include the Kafka connector
# package — Zeppelin is an interactive notebook, not a streaming job; the
# difference from the source manifest is preserved).
apiVersion: v1
kind: ConfigMap
metadata:
  name: {{ $name }}-spark-defaults
  namespace: {{ include "lakehouse.namespace" $ctx }}
data:
  spark-defaults.conf: |
    spark.jars.ivy=/tmp/ivy2
    spark.jars.packages=org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:{{ $ctx.Values.versions.icebergSparkRuntime }},org.apache.iceberg:iceberg-aws-bundle:{{ $ctx.Values.versions.icebergSparkRuntime }},org.apache.hadoop:hadoop-aws:{{ $ctx.Values.versions.hadoopAws }}
    spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions

    spark.hadoop.fs.s3a.path.style.access=true
    spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.EnvironmentVariableCredentialsProvider

    spark.sql.catalog.lakehouse=org.apache.iceberg.spark.SparkCatalog
    spark.sql.catalog.lakehouse.catalog-impl=org.apache.iceberg.rest.RESTCatalog
    spark.sql.catalog.lakehouse.uri=http://{{ include "lakehouse.svc.nessie" $ctx }}:19120/iceberg/
    spark.sql.catalog.lakehouse.warehouse={{ $ctx.Values.nessie.catalog.warehouse.location }}
    spark.sql.catalog.lakehouse.io-impl=org.apache.iceberg.aws.s3.S3FileIO
{{- if $nessieOAuth }}
    spark.sql.catalog.lakehouse.authentication.type=OAUTH2
    spark.sql.catalog.lakehouse.authentication.oauth2.client-id={{ .oauth2ClientId }}
    spark.sql.catalog.lakehouse.authentication.oauth2.client-secret=${OAUTH_CLIENT_SECRET}
    spark.sql.catalog.lakehouse.authentication.oauth2.issuer-url=${OIDC_ISSUER_URL}
{{- end }}

    spark.sql.catalog.rawlake=org.apache.iceberg.spark.SparkCatalog
    spark.sql.catalog.rawlake.catalog-impl=org.apache.iceberg.rest.RESTCatalog
    spark.sql.catalog.rawlake.uri=http://{{ include "lakehouse.svc.nessie" $ctx }}:19120/iceberg/
    spark.sql.catalog.rawlake.warehouse=rawdata
    spark.sql.catalog.rawlake.io-impl=org.apache.iceberg.aws.s3.S3FileIO
{{- if $nessieOAuth }}
    spark.sql.catalog.rawlake.authentication.type=OAUTH2
    spark.sql.catalog.rawlake.authentication.oauth2.client-id={{ .oauth2ClientId }}
    spark.sql.catalog.rawlake.authentication.oauth2.client-secret=${OAUTH_CLIENT_SECRET}
    spark.sql.catalog.rawlake.authentication.oauth2.issuer-url=${OIDC_ISSUER_URL}
{{- end }}
{{- if $sandboxWrite }}

    # Sandbox v2 (Task 5) — unified WRITE catalog. Same Nessie REST
    # endpoint, single shared `.Values.sandbox.namespace` Nessie namespace/
    # Trino catalog name + `.Values.sandbox.bucket` S3 bucket, unified
    # `svc-sandbox` Keycloak service-account identity (chart/templates/
    # 10-keycloak.yaml) whose secret lives in the unified `sandbox-oidc-
    # credentials` Secret (chart/templates/23-jupyterhub.yaml) — the SAME
    # identity/Secret images/jupyter/lakehouse_nb.py's spark()/iceberg_
    # sandbox() helpers and Trino's sandbox.properties (chart/templates/
    # 06-trino-ha.yaml) authenticate as. `warehouse` is the REGISTERED NAME
    # (see this define's header comment) -- mirrors lakehouse_nb.py's
    # `_spark_conf()` byte-for-byte.
    spark.sql.catalog.{{ $ctx.Values.sandbox.namespace }}=org.apache.iceberg.spark.SparkCatalog
    spark.sql.catalog.{{ $ctx.Values.sandbox.namespace }}.catalog-impl=org.apache.iceberg.rest.RESTCatalog
    spark.sql.catalog.{{ $ctx.Values.sandbox.namespace }}.uri=http://{{ include "lakehouse.svc.nessie" $ctx }}:19120/iceberg/
    spark.sql.catalog.{{ $ctx.Values.sandbox.namespace }}.warehouse={{ $ctx.Values.sandbox.namespace }}
    spark.sql.catalog.{{ $ctx.Values.sandbox.namespace }}.io-impl=org.apache.iceberg.aws.s3.S3FileIO
    spark.sql.catalog.{{ $ctx.Values.sandbox.namespace }}.authentication.type=OAUTH2
    spark.sql.catalog.{{ $ctx.Values.sandbox.namespace }}.authentication.oauth2.client-id=svc-sandbox
    spark.sql.catalog.{{ $ctx.Values.sandbox.namespace }}.authentication.oauth2.client-secret=${SANDBOX_OAUTH_CLIENT_SECRET}
    spark.sql.catalog.{{ $ctx.Values.sandbox.namespace }}.authentication.oauth2.issuer-url=${OIDC_ISSUER_URL}
{{- end }}
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: {{ $name }}
  namespace: {{ include "lakehouse.namespace" $ctx }}
  labels: {app: {{ $name }}}
spec:
  replicas: {{ $ctx.Values.zeppelin.replicas }}
  strategy: {type: Recreate}
  selector:
    matchLabels: {app: {{ $name }}}
  template:
    metadata:
      labels: {app: {{ $name }}}
    spec:
      {{- $psc := include "lakehouse.podSecurityContext" $ctx | trim }}
      {{- if $psc }}
      securityContext:
        {{- $psc | nindent 8 }}
      {{- end }}
      # No initContainers needed since Spark ships baked into the image.
{{- if $nessieOAuth }}
      # Shared instance with Nessie machine-auth on (Task 6; ALSO carries the
      # unified sandbox catalog's OAuth2 secret when `sandboxWrite`, Task 5):
      # the spark-defaults ConfigMap above holds a TEMPLATE — the OAuth2
      # client-secret(s) and OIDC issuer-url can only be literal `${...}`
      # placeholders there (never a resolved value in a ConfigMap; the
      # issuer-url itself also only ever lives in the `oidc.secretName`
      # Secret, never in a plain Helm value — see `_zeppelin.tpl`'s c1
      # refresher sidecar / chart-wide OIDC_ISSUER convention). This
      # initContainer resolves the placeholder(s) at pod start into a tmpfs
      # (`emptyDir{medium: Memory}` — never disk, never logged) that the
      # Spark interpreter's `SPARK_CONF_DIR` points at below. Substitution
      # mechanism: `envsubst` is NOT present in the Zeppelin image (verified:
      # apache/zeppelin:0.11.2, Ubuntu 20.04 base, `command -v envsubst` →
      # absent), so this uses a POSIX `sed` substitution instead (present, no
      # new package added) — replacement text is escaped for sed's `\`/`&`/
      # delimiter metacharacters so an arbitrary secret value round-trips
      # literally.
      initContainers:
      - name: spark-conf-render
        image: {{ printf "%s/zeppelin:%s" $ctx.Values.global.imageRegistry $ctx.Values.versions.zeppelinImageTag | quote }}
        {{- $icsc := include "lakehouse.containerSecurityContext" $ctx | trim }}
        {{- if $icsc }}
        securityContext:
          {{- $icsc | nindent 10 }}
        {{- end }}
        command: ["/bin/sh", "-c"]
        args:
          - |
            set -eu
            esc_secret=$(printf '%s' "$OAUTH_CLIENT_SECRET" | sed -e 's/[\\&|]/\\&/g')
            esc_issuer=$(printf '%s' "$OIDC_ISSUER_URL" | sed -e 's/[\\&|]/\\&/g')
            {{- if $sandboxWrite }}
            esc_sandbox_secret=$(printf '%s' "$SANDBOX_OAUTH_CLIENT_SECRET" | sed -e 's/[\\&|]/\\&/g')
            {{- end }}
            sed -e "s|\${OAUTH_CLIENT_SECRET}|$esc_secret|g" -e "s|\${OIDC_ISSUER_URL}|$esc_issuer|g"{{ if $sandboxWrite }} -e "s|\${SANDBOX_OAUTH_CLIENT_SECRET}|$esc_sandbox_secret|g"{{ end }} \
              /conf-tpl/spark-defaults.conf > /spark-conf-rendered/spark-defaults.conf
        env:
        - {name: OAUTH_CLIENT_SECRET, valueFrom: {secretKeyRef: {name: {{ .oidcSecretName }}, key: {{ $oidcSecretKey }}}}}
        - {name: OIDC_ISSUER_URL,     valueFrom: {secretKeyRef: {name: {{ $ctx.Values.oidc.secretName }}, key: issuer-url}}}
        {{- if $sandboxWrite }}
        - {name: SANDBOX_OAUTH_CLIENT_SECRET, valueFrom: {secretKeyRef: {name: sandbox-oidc-credentials, key: client-secret}}}
        {{- end }}
        volumeMounts:
        - {name: spark-conf,          mountPath: /conf-tpl,           readOnly: true}
        - {name: spark-conf-rendered, mountPath: /spark-conf-rendered}
        resources:
          requests: {cpu: "10m", memory: "16Mi"}
          limits:   {cpu: "100m", memory: "32Mi"}
{{- end }}
      containers:
      - name: {{ $name }}
        image: {{ printf "%s/zeppelin:%s" $ctx.Values.global.imageRegistry $ctx.Values.versions.zeppelinImageTag | quote }}
        {{- $csc := include "lakehouse.containerSecurityContext" $ctx | trim }}
        {{- if $csc }}
        securityContext:
          {{- $csc | nindent 10 }}
        {{- end }}
        {{- if and $ctx.Values.components.zeppelin $ctx.Values.auth.oidc.enabled }}
        # Seed the %trino interpreter (c1) before Zeppelin starts: copy the
        # ConfigMap-provided seed JSON into the live (writable) conf/ dir,
        # then exec the stock entrypoint. Mirrors the image's own
        # ENTRYPOINT ["/usr/bin/tini","--"] + CMD ["bin/zeppelin.sh"]
        # (WORKDIR $ZEPPELIN_HOME) — using $ZEPPELIN_CONF_DIR/$ZEPPELIN_HOME
        # keeps this layout-agnostic across the platform's /zeppelin base
        # and a stock /opt/zeppelin stand-in.
        command: ["/bin/sh", "-c"]
        args:
          # set -e: if the seed copy fails (e.g. an unexpected base layout
          # leaves the conf path unwritable), crash loudly instead of exec'ing
          # Zeppelin UNSEEDED — an unseeded start silently drops the %trino
          # interpreter and users hit "interpreter not found" only at first use.
          - |
            set -e
            cp /seed/interpreter.json "${ZEPPELIN_CONF_DIR:-$ZEPPELIN_HOME/conf}/interpreter.json"
            exec /usr/bin/tini -- bin/zeppelin.sh
        {{- end }}
        ports: [{containerPort: 8080}]
        env:
        - {name: SPARK_HOME,            value: "/opt/spark"}
        - {name: ZEPPELIN_NOTEBOOK_DIR, value: "/zeppelin/notebook"}
        - {name: SPARK_CONF_DIR,        value: {{ if $nessieOAuth }}"/spark-conf-rendered"{{ else }}"/spark-conf"{{ end }}}
        - {name: ZEPPELIN_SHIRO_CONF,   value: "/zeppelin/conf/shiro.ini"}
        - {name: AD_BIND_DN,          valueFrom: {secretKeyRef: {name: ad-bind-credentials, key: bind-dn}}}
        - {name: AD_BIND_PASSWORD,    valueFrom: {secretKeyRef: {name: ad-bind-credentials, key: bind-password}}}
        - {name: AD_USER_SEARCH_BASE, valueFrom: {secretKeyRef: {name: ad-bind-credentials, key: user-search-base}}}
        - {name: AD_URL,              valueFrom: {secretKeyRef: {name: ad-bind-credentials, key: ad-url}}}
        - {name: AWS_ACCESS_KEY_ID,     valueFrom: {secretKeyRef: {name: {{ $s3SecretName }}, key: access-key-id}}}
        - {name: AWS_SECRET_ACCESS_KEY, valueFrom: {secretKeyRef: {name: {{ $s3SecretName }}, key: secret-access-key}}}
        - {name: AWS_ENDPOINT_URL_S3,   valueFrom: {secretKeyRef: {name: {{ $s3SecretName }}, key: endpoint}}}
        - {name: AWS_REGION,            valueFrom: {secretKeyRef: {name: {{ $s3SecretName }}, key: region}}}
        volumeMounts:
        - {name: {{ $name }}-notebooks, mountPath: /zeppelin/notebook}
        - {name: spark-conf,         mountPath: /spark-conf}
        - {name: shiro-conf,         mountPath: /zeppelin/conf/shiro.ini,   subPath: shiro.ini}
        {{- if $nessieOAuth }}
        - {name: spark-conf-rendered, mountPath: /spark-conf-rendered}
        {{- end }}
        {{- if and $ctx.Values.components.zeppelin $ctx.Values.auth.oidc.enabled }}
        - {name: trino-interp, mountPath: /seed,          readOnly: true}
        - {name: trino-tls,    mountPath: /mnt/trino-tls, readOnly: true}
        {{- end }}
        resources:
          requests: {memory: {{ $ctx.Values.zeppelin.resources.requests.memory | quote }}, cpu: {{ $ctx.Values.zeppelin.resources.requests.cpu | quote }}}
          limits:   {memory: {{ $ctx.Values.zeppelin.resources.limits.memory | quote }},   cpu: {{ $ctx.Values.zeppelin.resources.limits.cpu | quote }}}
        readinessProbe:
          httpGet: {path: /api/version, port: 8080}
          initialDelaySeconds: 60
          periodSeconds: 15
        livenessProbe:
          httpGet: {path: /api/version, port: 8080}
          initialDelaySeconds: 120
          periodSeconds: 30
      {{- if and $ctx.Values.components.zeppelin $ctx.Values.auth.oidc.enabled }}
      # Trino JWT refresher sidecar (c1 Task 5). Keeps the %trino interpreter's
      # `default.accessToken` fresh, mirroring the b1 Superset↔Trino sidecar
      # (19-superset.yaml `trino-token-refresher`) but with a different delivery
      # mechanism, per the spike (docs/superpowers/spikes/2026-08-04-c1-
      # zeppelin-trino-interp.md §4, CONFIRMED): Zeppelin has no shared-file
      # token slot like Superset's DB_CONNECTION_MUTATOR — the JDBC interpreter
      # only re-reads `default.accessToken` from its in-memory settings object,
      # so the sidecar must (1) fetch a fresh svc-zeppelin-trino JWT from
      # Keycloak, (2) log into Zeppelin's own REST API as a Zeppelin ADMIN
      # (shiro.ini gates /api/interpreter/** to `roles[admin]` — the
      # ActiveDirectoryGroupRealm + `shiro.loginUrl=/api/login` above already
      # handle this via POST /api/login form-login, the SAME mechanism real UI
      # users use; the REST user MUST be a member of `zeppelin.ad.groups.admin`),
      # (3) GET the current trino interpreter setting, (4) patch
      # `properties["default.accessToken"].value`, (5) PUT the whole object
      # back. PUT ALONE recycles the interpreter — Zeppelin tears down and
      # rebuilds the interpreter process as part of applying new settings, so
      # NO separate restart call is needed (spike-confirmed). This recycle
      # interrupts any in-flight %trino paragraph on every refresh — a real,
      # documented cost, not an artifact — which is why
      # `zeppelin.trino.tokenRefreshSeconds` defaults deliberately long (3000s)
      # to keep refreshes rare. The exact GET/PUT JSON envelope is
      # UAT-confirmable per the spike, but GET→patch→PUT-the-same-object is
      # the flow the spike verified returns 200.
      - name: zeppelin-trino-refresher
        image: {{ printf "%s/zeppelin:%s" $ctx.Values.global.imageRegistry $ctx.Values.versions.zeppelinImageTag | quote }}
        {{- $csc := include "lakehouse.containerSecurityContext" $ctx | trim }}
        {{- if $csc }}
        securityContext:
          {{- $csc | nindent 10 }}
        {{- end }}
        command: ["/bin/bash", "-c"]
        args:
          - |
            set -uo pipefail
            BACKOFF=5
            while true; do
              if python3 - <<'PY'
            import os, sys, json, urllib.request, urllib.parse, http.cookiejar

            def fail(msg):
                sys.stderr.write("WARN: %s; keeping interpreter as-is\n" % msg)
                sys.exit(1)

            # 1) fetch a fresh svc-zeppelin-trino JWT (client_credentials)
            issuer = os.environ["OIDC_ISSUER"].rstrip("/")
            token_req = urllib.parse.urlencode({
                "grant_type": "client_credentials",
                "client_id": "svc-zeppelin-trino",
                "client_secret": os.environ["SVC_CLIENT_SECRET"],
            }).encode()
            try:
                r = urllib.request.urlopen(issuer + "/protocol/openid-connect/token", data=token_req, timeout=10)
                token = json.load(r)["access_token"]
            except Exception as e:
                fail("token fetch failed: %s" % type(e).__name__)

            # 2) log into Zeppelin's REST API as the admin AD service account
            #    (Shiro form-login: POST /api/login, form userName/password)
            jar = http.cookiejar.CookieJar()
            opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(jar))
            login_req = urllib.parse.urlencode({
                "userName": os.environ["ZEPPELIN_REST_USER"],
                "password": os.environ["ZEPPELIN_REST_PASSWORD"],
            }).encode()
            try:
                resp = opener.open("http://localhost:8080/api/login", data=login_req, timeout=10)
                if resp.status != 200:
                    fail("zeppelin login returned HTTP %s" % resp.status)
            except Exception as e:
                fail("zeppelin login failed: %s" % type(e).__name__)

            # 3) GET the current trino interpreter setting
            try:
                resp = opener.open("http://localhost:8080/api/interpreter/setting/trino", timeout=10)
                setting = json.load(resp)["body"]
            except Exception as e:
                fail("GET interpreter setting failed: %s" % type(e).__name__)

            # 4) patch default.accessToken, PUT the whole object back — PUT
            #    alone recycles the interpreter, no separate restart call.
            try:
                setting["properties"]["default.accessToken"]["value"] = token
                put_req = urllib.request.Request(
                    "http://localhost:8080/api/interpreter/setting/trino",
                    data=json.dumps(setting).encode(),
                    method="PUT",
                    headers={"Content-Type": "application/json"},
                )
                resp = opener.open(put_req, timeout=15)
                if resp.status != 200:
                    fail("PUT interpreter setting returned HTTP %s" % resp.status)
            except Exception as e:
                fail("PUT interpreter setting failed: %s" % type(e).__name__)

            sys.exit(0)
            PY
              then
                BACKOFF=5
                sleep {{ $ctx.Values.zeppelin.trino.tokenRefreshSeconds }}
              else
                echo "WARN: trino token refresh cycle failed; retry in ${BACKOFF}s" >&2
                sleep "$BACKOFF"; BACKOFF=$(( BACKOFF*2 > 60 ? 60 : BACKOFF*2 ))
              fi
            done
        env:
        - {name: OIDC_ISSUER,           valueFrom: {secretKeyRef: {name: {{ $ctx.Values.oidc.secretName }}, key: issuer-url}}}
        - {name: SVC_CLIENT_SECRET,     valueFrom: {secretKeyRef: {name: {{ $name }}-trino-oidc-credentials, key: client-secret}}}
        - {name: ZEPPELIN_REST_USER,    value: {{ $ctx.Values.zeppelin.trino.refresher.restUsername | quote }}}
        - {name: ZEPPELIN_REST_PASSWORD, valueFrom: {secretKeyRef: {name: {{ $name }}-trino-oidc-credentials, key: rest-password}}}
        resources:
          requests: {cpu: "50m", memory: "64Mi"}
          limits:   {cpu: "200m", memory: "128Mi"}
      {{- end }}
      volumes:
      - name: {{ $name }}-notebooks
        persistentVolumeClaim: {claimName: {{ $name }}-notebooks-pvc}
      - name: spark-conf
        configMap: {name: {{ $name }}-spark-defaults}
      - name: shiro-conf
        configMap: {name: {{ $name }}-shiro}
      {{- if $nessieOAuth }}
      - name: spark-conf-rendered
        emptyDir: {medium: Memory}
      {{- end }}
      {{- if and $ctx.Values.components.zeppelin $ctx.Values.auth.oidc.enabled }}
      - name: trino-interp
        configMap: {name: {{ $name }}-trino-interpreter}
      - name: trino-tls
        secret: {secretName: trino-internal-tls}
      {{- end }}
---
apiVersion: v1
kind: Service
metadata:
  name: {{ $name }}
  namespace: {{ include "lakehouse.namespace" $ctx }}
spec:
  selector: {app: {{ $name }}}
  ports: [{name: http, port: 8080, targetPort: 8080}]
---
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: {{ $name }}-pdb
  namespace: {{ include "lakehouse.namespace" $ctx }}
spec:
  minAvailable: 0
  selector:
    matchLabels: {app: {{ $name }}}
{{- end -}}
