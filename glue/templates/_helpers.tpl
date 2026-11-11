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
{{- /* JSON akışları (values.streams): Bronze tablo adı, Polaris ad alanı ve doğrulama. */ -}}
{{- define "glue.streamTable" -}}{{ .table | default (printf "%s_raw.events" .name) }}{{- end -}}
{{- define "glue.validateStreams" -}}
{{- $taken := list "nginx" }}
{{- range .Values.sources }}{{ $taken = append $taken (.topicPrefix | default .name) }}{{ end }}
{{- $seen := list }}
{{- range .Values.streams }}
{{- if not (regexMatch "^[a-z][a-z0-9_]*$" (.name | default "")) }}{{ fail (printf "streams[].name %q: küçük harfle başlamalı, yalnız a-z 0-9 _ içermeli" (.name | default "")) }}{{ end }}
{{- if has .name $taken }}{{ fail (printf "streams[%s]: ad bir kaynağın konu önekiyle ya da 'nginx' ile çakışıyor" .name) }}{{ end }}
{{- if has .name $seen }}{{ fail (printf "streams[%s]: aynı ad iki kez verilmiş" .name) }}{{ end }}
{{- $seen = append $seen .name }}
{{- end }}
{{- end -}}
{{- /* JSON gövdeli bir Kafka konusundan Iceberg'e yazan sink'in ortak ayarları (nginx ve streams). Çağıran fromJson ile
   dict'e çevirip konuya özel dönüşümleri ekler. Girdi: dict ctx topic table partitionBy dlq. */ -}}
{{- define "glue.jsonSinkConfig" -}}
{{- $c := .ctx }}{{ $ns := include "glue.ns" $c }}
{{- $cfg := dict
  "topics" .topic
  "key.converter" "org.apache.kafka.connect.storage.StringConverter"
  "value.converter" "org.apache.kafka.connect.json.JsonConverter"
  "value.converter.schemas.enable" "false"
  "iceberg.tables" .table
  "iceberg.tables.auto-create-enabled" "true"
  "iceberg.tables.evolve-schema-enabled" "true"
  "iceberg.tables.default-partition-by" .partitionBy
  "iceberg.tables.auto-create-props.write.metadata.delete-after-commit.enabled" "true"
  "iceberg.catalog.type" "rest"
  "iceberg.catalog.uri" $c.Values.iceberg.catalogUri
  "iceberg.catalog.warehouse" $c.Values.iceberg.warehouse
  "iceberg.catalog.credential" (printf "${secrets:%s/polaris-connect:credential}" $ns)
  "iceberg.catalog.scope" "PRINCIPAL_ROLE:ALL"
  "iceberg.catalog.io-impl" "org.apache.iceberg.aws.s3.S3FileIO"
  "iceberg.catalog.s3.endpoint" $c.Values.s3.endpoint
  "iceberg.catalog.s3.path-style-access" $c.Values.s3.pathStyleAccess
  "iceberg.catalog.client.region" $c.Values.s3.region
  "iceberg.kafka.session.timeout.ms" "120000"
  "iceberg.kafka.heartbeat.interval.ms" "15000"
  "iceberg.kafka.max.poll.interval.ms" "300000"
  "iceberg.kafka.request.timeout.ms" "130000"
  "iceberg.control.commit.timeout-ms" "120000"
  "errors.tolerance" "all"
  "errors.log.enable" "true"
  "errors.deadletterqueue.topic.name" .dlq
  "errors.deadletterqueue.topic.replication.factor" (index $c.Values.kafka.config "default.replication.factor") }}
{{- if $c.Values.s3.vendedCredentials }}
{{- $_ := set $cfg "iceberg.catalog.header.X-Iceberg-Access-Delegation" "vended-credentials" }}
{{- else }}
{{- $_ := set $cfg "iceberg.catalog.header.X-Iceberg-Access-Delegation" "none" }}
{{- $_ := set $cfg "iceberg.catalog.s3.access-key-id" (printf "${secrets:%s/s3-creds:AWS_ACCESS_KEY_ID}" $ns) }}
{{- $_ := set $cfg "iceberg.catalog.s3.secret-access-key" (printf "${secrets:%s/s3-creds:AWS_SECRET_ACCESS_KEY}" $ns) }}
{{- end }}
{{- with $c.Values.connect.sinkCommitIntervalMs }}
{{- $_ := set $cfg "iceberg.control.commit.interval-ms" . }}
{{- end }}
{{- toJson $cfg }}
{{- end -}}
