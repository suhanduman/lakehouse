{{- define "glue.ns" -}}{{ .Values.namespace }}{{- end -}}
{{- define "glue.isOpenShift" -}}{{ eq .Values.platform "openshift" }}{{- end -}}
{{- /* Superset "lakehouse" veritabanının SABİT uuid'si — TEK KAYNAK. Superset import'u nesneleri uuid ile
       eşleştirir (superset/models/helpers.py import_from_dict). Hem ürün datasource'u (trino.yaml) hem asset
       bundle'ındaki databases/lakehouse.yaml ve datasets/.../database_uuid bu değeri kullanır; böylece taze
       kurulumda legacy-import'un yarattığı satırı bundle import'u bulur, ikinci satır açılmaz. */ -}}
{{- define "glue.supersetDbUuid" -}}7b68c652-4b36-41e8-a5f8-1324d7d577cf{{- end -}}
{{- define "glue.host" -}}{{ regexReplaceAll "^https?://([^/:]+).*$" . "${1}" }}{{- end -}}
{{- /* glue.hostFor: bileşen hostname'i TEK KAYNAKTAN. Açık `<bileşen>.hostname` varsa (TAM URL ise yalnız host
       kısmı) o; yoksa `<bileşen>-<ns>.<appsDomain>` türetilir; ikisi de yoksa render HATA verir (sessiz yanlış
       host yerine gürültülü hata). Kullanım: include "glue.hostFor" (dict "name" "trino" "ctx" $) */ -}}
{{- define "glue.hostFor" -}}
{{- $v := index .ctx.Values .name -}}
{{- if and $v $v.hostname }}{{ include "glue.host" $v.hostname }}
{{- else if .ctx.Values.appsDomain }}{{ printf "%s-%s.%s" .name (include "glue.ns" .ctx) .ctx.Values.appsDomain }}
{{- else }}{{ fail (printf "appsDomain ya da %s.hostname verilmeli (platform/values/site/glue.yaml)" .name) }}{{ end -}}
{{- end -}}
{{- /* glue.keycloakUrl: Keycloak TAM URL'i (token iss = bu değer). keycloak.hostname verilmişse ŞEMA ZORUNLU
       (dev'de küme içi http://keycloak-service...:8080); çıplak host sessizce geçerse Keycloak CR'ı ve Superset
       KEYCLOAK_URL'i şemasız kalır ve OIDC akışı bozulur -> render HATA verir. Boşsa https://keycloak-<ns>.<appsDomain>. */ -}}
{{- define "glue.keycloakUrl" -}}
{{- $h := .Values.keycloak.hostname -}}
{{- if $h -}}
{{- if not (regexMatch "^https?://" $h) }}{{ fail (printf "keycloak.hostname TAM URL olmalı (şema dâhil, ör. https://keycloak-lakehouse.apps.ocp.example.net); verilen: %s" $h) }}{{ end -}}
{{ $h }}
{{- else -}}
{{ printf "https://%s" (include "glue.hostFor" (dict "name" "keycloak" "ctx" .)) }}
{{- end -}}
{{- end -}}
