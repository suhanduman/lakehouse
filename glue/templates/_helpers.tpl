{{- define "glue.ns" -}}{{ .Values.namespace }}{{- end -}}
{{- define "glue.isOpenShift" -}}{{ eq .Values.platform "openshift" }}{{- end -}}
{{- define "glue.host" -}}{{ regexReplaceAll "^https?://([^/:]+).*$" . "${1}" }}{{- end -}}
