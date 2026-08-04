#!/usr/bin/env bash
# Modüler "yeni kaynak ekle": bucket + KafkaTopic + connector CR + Iceberg namespace üretir.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DRY=0; declare -A A
while [ $# -gt 0 ]; do
  case "$1" in
    --dry-run) DRY=1; shift;;
    --*) A["${1#--}"]="$2"; shift 2;;
    *) echo "bilinmeyen arg: $1" >&2; exit 2;;
  esac
done
req() { [ -n "${A[$1]:-}" ] || { echo "eksik --$1" >&2; exit 2; }; }
for k in source kind type db table target-ns target-table; do req "$k"; done

SRC="${A[source]}"; KIND="${A[kind]}"; TYPE="${A[type]}"
BUCKET="src-${A[target-ns]//_/-}"
TOPIC_PREFIX_CDC="cdc.${SRC}"
# shellcheck disable=SC2018,SC2019 # DB adları ASCII (mssql/pg identifier kısıtı); locale-class'a gerek yok.
TOPIC_PREFIX_JDBC="jdbc.${SRC}.$(echo "${A[db]}" | tr 'A-Z' 'a-z')."
POLL="${A[poll-ms]:-3600000}"; INC="${A[incrementing-col]:-id}"; TS="${A[timestamp-col]:-updated_at}"
# NOT: default'lar ayrı değişkenlere alınıyor çünkü "${A[k]:-{{X}}}" gibi
# gövdesinde literal çift-parantez taşıyan bir ${...:-...} ifadesi, bash'te
# (5.3'te doğrulandı) A[k] SET iken kapanış parantez sayımını şaşırıp
# değerin sonuna sahte "}}" ekliyor (yalnızca A[k] UNSET iken doğru
# davranıyor) — dolaylı değişken referansı bu ayrıştırma tuzağını by-pass eder.
# JDBC_URL/MONGO_URI de aynı tuzağa düşüyordu (--jdbc-url/--mongo-uri verilince
# connection.url/mongodb.connection.string sonuna sahte "}}" ekleniyordu).
DB_HOST_DEFAULT='{{DB_HOST}}'
JDBC_URL_DEFAULT='{{JDBC_URL}}'
MONGO_URI_DEFAULT='{{MONGO_URI}}'
# Apicurio schema auto-register — prod-safe default "false" (uncontrolled
# schema evolution is an anti-pattern in prod; see chart/values.yaml
# `connectors.schemaAutoRegister` / render_service.py `SCHEMA_AUTO_REGISTER`
# for the same default+rationale in this project's other two connector-
# rendering paths). No literal "}}" in this default, so (unlike DB_HOST/
# JDBC_URL/MONGO_URI above) the indirect-variable workaround isn't required
# here — inlined directly in the sed rule below.
# Postgres slot/publication adları yalnızca [a-z0-9_] kabul eder; render_service
# _safe_ident ile sanitize ediyor — CLI tarafında da SRC'yi aynı şekilde
# temizleyip {{SLOT_SRC}} olarak veriyoruz (ör. pg-src1 -> pg_src1).
# shellcheck disable=SC2018,SC2019 # ASCII identifier; locale-class'a gerek yok.
SLOT_SRC="$(printf '%s' "$SRC" | tr 'A-Z' 'a-z' | tr -c 'a-z0-9_' '_')"

pick_template() {
  case "${KIND}:${TYPE}" in
    cdc:mssql) echo "source-cdc-relational.yaml";;
    cdc:pg) echo "source-cdc-pg.yaml";;
    scheduled:mssql|scheduled:pg) echo "source-scheduled-jdbc.yaml";;
    cdc:mongo) echo "source-cdc-mongo.yaml";;
    *) echo "desteklenmeyen kind/type: ${KIND}/${TYPE}" >&2; exit 2;;
  esac
}
prefix() { [ "$KIND" = cdc ] && echo "$TOPIC_PREFIX_CDC" || echo "$TOPIC_PREFIX_JDBC"; }
render() { # $1 = template dosyası
  sed -e "s#{{SOURCE}}#${SRC}#g" \
      -e "s#{{DB}}#${A[db]}#g" \
      -e "s#{{TABLE}}#${A[table]}#g" \
      -e "s#{{TARGET_NS}}#${A[target-ns]}#g" \
      -e "s#{{TARGET_TABLE}}#${A[target-table]}#g" \
      -e "s#{{BUCKET}}#${BUCKET}#g" \
      -e "s#{{TOPIC_PREFIX}}#$(prefix)#g" \
      -e "s#{{INCREMENTING_COL}}#${INC}#g" \
      -e "s#{{TIMESTAMP_COL}}#${TS}#g" \
      -e "s#{{POLL_MS}}#${POLL}#g" \
      -e "s#{{DB_HOST}}#${A[db-host]:-$DB_HOST_DEFAULT}#g" \
      -e "s#{{AUTO_REGISTER}}#${A[auto-register]:-false}#g" \
      -e "s#{{SLOT_SRC}}#${SLOT_SRC}#g" \
      -e "s#{{JDBC_URL}}#${A[jdbc-url]:-$JDBC_URL_DEFAULT}#g" \
      -e "s#{{MONGO_URI}}#${A[mongo-uri]:-$MONGO_URI_DEFAULT}#g" \
      -e "s#{{CRON}}#${A[cron]:-0 0 2 * * *}#g" \
      "$1"
}

TOPIC_NAME="$([ "$KIND" = cdc ] && echo "$(prefix).${A[table]}" || echo "$(prefix)${A[table]}")"
TOPIC_YAML="$(sed "s#{{TOPIC_NAME}}#${TOPIC_NAME}#g" "$ROOT/kafkatopic.yaml")"
# NOT: pick_template'in "exit 2"si $(render "$ROOT/$(pick_template)") gibi iç içe komut
# ikamesinde eriyip gider (errexit yayılmaz) — desteklenmeyen kind/type sessizce boş/bozuk
# connector üretirdi. Bu yüzden şablon adı önce ayrı bir atamayla çözülüyor.
TEMPLATE="$(pick_template)"
CONN_YAML="$(render "$ROOT/${TEMPLATE}")"
# Iceberg tablo PRE-CREATE komutu — Trino DDL DEĞİL. Trino, Iceberg identifier
# field'ını (equality-delete anahtarı) set edemez; sink'in auto-create'i de
# dinamik modda identifier koymuyor → upsert sessizce append-only'e düşüyor ve
# her CDC UPDATE duplicate satır bırakıyor. Bu yüzden tablo, veri akmadan önce
# create_iceberg_table.py (pyiceberg → Nessie REST) ile identifier field set
# edilerek yaratılmalıdır. İlişkisel PK = --incrementing-col (INC); mongo
# daima _id (STRING). mssql introspeksiyonu henüz yok → --descriptor.
PRECREATE_PY="python3 \"$ROOT/../create_iceberg_table.py\""
# BRONZE pre-create (medallion CDC): mirrors the console orchestrator's
# two-layer pre-create — Bronze (<ns>_raw, day(__ts_ms) partition, rawdata
# warehouse, no identifier) is created BEFORE Silver. --layer bronze makes
# create_iceberg_table.py append the fixed CDC metadata cols + set the
# partition/warehouse itself; --discover's PK/identifier is irrelevant for
# bronze (forced to [] internally) so it's simply omitted here.
case "$TYPE" in
  pg)    PRECREATE_BRONZE="$PRECREATE_PY --discover --source-type pg --dsn 'postgresql://<user>:<pass>@${A[db-host]:-DB_HOST}:5432/${A[db]}' --source-table '${A[table]}' --namespace '${A[target-ns]}_raw' --table '${A[target-table]}' --layer bronze"
         PRECREATE="$PRECREATE_PY --discover --source-type pg --dsn 'postgresql://<user>:<pass>@${A[db-host]:-DB_HOST}:5432/${A[db]}' --source-table '${A[table]}' --namespace '${A[target-ns]}' --table '${A[target-table]}' --identifier-columns '${INC}'";;
  mongo) PRECREATE_BRONZE="$PRECREATE_PY --discover --source-type mongo --mongo-uri '<mongodb-uri>' --db '${A[db]}' --source-table '${A[table]}' --namespace '${A[target-ns]}_raw' --table '${A[target-table]}' --layer bronze"
         PRECREATE="$PRECREATE_PY --discover --source-type mongo --mongo-uri '<mongodb-uri>' --db '${A[db]}' --source-table '${A[table]}' --namespace '${A[target-ns]}' --table '${A[target-table]}'";;
  *)     PRECREATE_BRONZE="$PRECREATE_PY --descriptor '<${A[target-table]}-bronze-descriptor.yaml>' --layer bronze  # ${TYPE}: introspeksiyonu yok — descriptor (columns) verin; layer=bronze metadata kolonlarını + rawdata warehouse'u otomatik ekler"
         PRECREATE="$PRECREATE_PY --descriptor '<${A[target-table]}-descriptor.yaml>'  # ${TYPE}: create_iceberg_table.py introspeksiyonu yok — descriptor (columns + identifier) verin";;
esac

if [ "$DRY" = 1 ]; then
  echo "### BUCKET: aws s3 mb s3://${BUCKET}"
  echo "### ICEBERG PRE-CREATE — BRONZE (önce; ${A[target-ns]}_raw, connector'DAN ÖNCE):"; echo "$PRECREATE_BRONZE"
  echo "### ICEBERG PRE-CREATE — SILVER (pyiceberg → Nessie; connector'DAN ÖNCE):"; echo "$PRECREATE"
  echo "### KAFKATOPIC:"; echo "$TOPIC_YAML"
  echo "### CONNECTOR (tablo hazır OLDUKTAN sonra apply edin):"; echo "$CONN_YAML"
  exit 0
fi

echo ">> bucket oluştur (yetki yoksa manuel — README)"; aws s3 mb "s3://${BUCKET}" || echo "  (atlandı; manuel oluşturun)"
# SIRA ÖNEMLİ: tablo identifier field'ı ile hazır OLMADAN connector apply
# edilmemeli (yoksa append-only bug). create_iceberg_table.py, Nessie/S3 (ve
# discover için kaynak DB) erişimi olan bir ortamda çalıştırılmalı; o yüzden
# burada OTOMATİK apply EDİLMEZ — komut basılır, connector'ı bundan SONRA uygula.
echo ">> 1) ÖNCE Bronze Iceberg tabloyu pre-create edin (day(__ts_ms) partition, rawdata warehouse):"; echo "   $PRECREATE_BRONZE"
echo ">> 2) SONRA Silver Iceberg tabloyu pre-create edin (identifier field ile):"; echo "   $PRECREATE"
echo ">> 3) KafkaTopic uygula:"; echo "$TOPIC_YAML" | oc apply -f -
echo ">> 4) Her iki tablo da hazır OLDUKTAN sonra connector manifestini apply edin:"; echo "$CONN_YAML"
