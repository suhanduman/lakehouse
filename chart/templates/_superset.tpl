{{/*
Superset Alerts & Reports — shared env for every pod that loads superset_config.py
(superset, init-Job, worker, beat). Emits column-0 `- name: ...` list items; the
caller places them with `nindent <N>` under an existing `env:` at the right indent.
Only used inside `{{- if .Values.superset.alertsReports.enabled }}` blocks.
REDIS_HOST = the in-namespace superset-redis Service (fqdn helper). Redis/SMTP
passwords come from Secrets (optional: true → open relay / password-less redis dev
won't block pod start; the Secret is referenced by name, rendered inline as
dev-convenience or provisioned out-of-band).
*/}}
{{- define "lakehouse.superset.reportsEnv" -}}
- name: REDIS_HOST
  value: {{ include "lakehouse.fqdn" (dict "svc" "superset-redis" "context" .) | quote }}
- name: REDIS_PASSWORD
  valueFrom: {secretKeyRef: {name: superset-redis-auth, key: password, optional: true}}
- name: SMTP_HOST
  value: {{ .Values.superset.smtp.host | quote }}
- name: SMTP_PORT
  value: {{ .Values.superset.smtp.port | quote }}
- name: SMTP_STARTTLS
  value: {{ .Values.superset.smtp.startTLS | quote }}
- name: SMTP_SSL
  value: {{ .Values.superset.smtp.ssl | quote }}
- name: SMTP_USER
  value: {{ .Values.superset.smtp.user | quote }}
- name: SMTP_MAIL_FROM
  value: {{ .Values.superset.smtp.mailFrom | quote }}
- name: SMTP_PASSWORD
  valueFrom: {secretKeyRef: {name: superset-smtp, key: password, optional: true}}
{{- end -}}

{{/*
Trino machine-auth (b1) sidecar + volumes, shared by the superset Deployment and
the alerts&reports worker. Both run report/interactive queries against Trino and
need the same rotating svc-superset-trino JWT. Emits column-0 list items; callers
place with `nindent <N>` and gate on `eq .Values.superset.trino.authMode "jwt"`.
*/}}
{{- define "lakehouse.superset.trinoRefresherContainer" -}}
- name: trino-token-refresher
  image: {{ printf "%s/superset:%s" .Values.global.imageRegistry .Values.versions.supersetImageTag | quote }}
  {{- $csc := include "lakehouse.containerSecurityContext" . | trim }}
  {{- if $csc }}
  securityContext:
    {{- $csc | nindent 4 }}
  {{- end }}
  command: ["/bin/bash","-c"]
  args:
  - |
    set -uo pipefail
    DEST=/var/run/trino-token/token
    BACKOFF=5
    while true; do
      TOK=$(python3 - <<'PY'
    import os,sys,json,urllib.parse,urllib.request
    data=urllib.parse.urlencode({"grant_type":"client_credentials",
      "client_id":"svc-superset-trino",
      "client_secret":os.environ["SVC_CLIENT_SECRET"]}).encode()
    url=os.environ["OIDC_ISSUER"].rstrip("/")+"/protocol/openid-connect/token"
    try:
      r=urllib.request.urlopen(url,data=data,timeout=10)
      print(json.load(r)["access_token"])
    except Exception as e:
      sys.stderr.write("token fetch failed: %s\n" % type(e).__name__); sys.exit(1)
    PY
    )
      if [ -n "$TOK" ]; then
        printf '%s' "$TOK" > /var/run/trino-token/token.tmp && mv /var/run/trino-token/token.tmp "$DEST"
        BACKOFF=5
        sleep {{ .Values.superset.trino.tokenRefreshSeconds }}
      else
        echo "WARN: keeping last-good token; retry in ${BACKOFF}s"
        sleep "$BACKOFF"; BACKOFF=$(( BACKOFF*2 > 60 ? 60 : BACKOFF*2 ))
      fi
    done
  env:
  - {name: OIDC_ISSUER, valueFrom: {secretKeyRef: {name: {{ .Values.oidc.secretName }}, key: issuer-url}}}
  - {name: SVC_CLIENT_SECRET, valueFrom: {secretKeyRef: {name: superset-trino-oidc-credentials, key: client-secret}}}
  volumeMounts:
  - {name: trino-token, mountPath: /var/run/trino-token}
  resources:
    requests: {cpu: "50m", memory: "64Mi"}
    limits:   {cpu: "200m", memory: "128Mi"}
{{- end -}}

{{- define "lakehouse.superset.trinoVolumes" -}}
- name: trino-token
  emptyDir: {}
- name: trino-ca
  secret:
    secretName: trino-internal-tls
    items:
    - {key: ca.crt, path: ca.crt}
{{- end -}}

{{- define "lakehouse.superset.trinoTokenMounts" -}}
- {name: trino-token, mountPath: /var/run/trino-token}
# Trino :8443 internal CA — mounted on the `superset` container ONLY
# (NOT the refresher sidecar below, which talks to Keycloak, not
# Trino). Consumed by DB_CONNECTION_MUTATOR's `verify` (superset_config.py above).
- {name: trino-ca, mountPath: /etc/trino-ca/ca.crt, subPath: ca.crt, readOnly: true}
{{- end -}}
