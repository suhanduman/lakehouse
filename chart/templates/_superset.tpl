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
