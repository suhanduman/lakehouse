{{- define "glue.ns" -}}{{ .Values.namespace }}{{- end -}}
{{- define "glue.isOpenShift" -}}{{ eq .Values.platform "openshift" }}{{- end -}}
{{- /* Superset "lakehouse" veritabanının SABİT uuid'si — TEK KAYNAK. Superset import'u nesneleri uuid ile
       eşleştirir (superset/models/helpers.py import_from_dict). Hem ürün datasource'u (trino.yaml) hem asset
       bundle'ındaki databases/lakehouse.yaml ve datasets/.../database_uuid bu değeri kullanır; böylece taze
       kurulumda legacy-import'un yarattığı satırı bundle import'u bulur, ikinci satır açılmaz. */ -}}
{{- define "glue.supersetDbUuid" -}}7b68c652-4b36-41e8-a5f8-1324d7d577cf{{- end -}}
{{- define "glue.host" -}}{{ regexReplaceAll "^https?://([^/:]+).*$" . "${1}" }}{{- end -}}
