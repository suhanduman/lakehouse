#!/usr/bin/env bash
# Modular "add new source": produces a bucket + KafkaTopic + connector CR + Iceberg namespace.
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
# shellcheck disable=SC2018,SC2019 # DB names are ASCII (mssql/pg identifier constraint); no need for a locale class.
TOPIC_PREFIX_JDBC="jdbc.${SRC}.$(echo "${A[db]}" | tr 'A-Z' 'a-z')."
POLL="${A[poll-ms]:-3600000}"; INC="${A[incrementing-col]:-id}"; TS="${A[timestamp-col]:-updated_at}"
# NOTE: defaults are captured in separate variables because a ${...:-...}
# expression whose body carries a literal double-brace like "${A[k]:-{{X}}}"
# confuses bash's (verified on 5.3) closing-brace counting when A[k] IS set,
# appending a bogus "}}" to the end of the value (it only behaves correctly
# when A[k] is UNSET) — an indirect variable reference bypasses this parsing
# trap.
# JDBC_URL/MONGO_URI fell into the same trap (passing --jdbc-url/--mongo-uri
# would append a bogus "}}" to the end of connection.url/mongodb.connection.string).
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
# Postgres slot/publication names only accept [a-z0-9_]; render_service
# sanitizes them via _safe_ident — on the CLI side we clean up SRC the same
# way and pass it in as {{SLOT_SRC}} (e.g. pg-src1 -> pg_src1).
# shellcheck disable=SC2018,SC2019 # ASCII identifier; no need for a locale class.
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
render() { # $1 = template file
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
# NOTE: pick_template's "exit 2" gets swallowed inside a nested command
# substitution like $(render "$ROOT/$(pick_template)") (errexit doesn't propagate)
# — an unsupported kind/type would silently produce an empty/broken connector.
# That's why the template name is resolved first via a separate assignment.
TEMPLATE="$(pick_template)"
CONN_YAML="$(render "$ROOT/${TEMPLATE}")"
# Iceberg table PRE-CREATE command — NOT Trino DDL. Trino cannot set the
# Iceberg identifier field (the equality-delete key); the sink's auto-create
# also doesn't set an identifier in dynamic mode → upsert silently falls back
# to append-only and every CDC UPDATE leaves a duplicate row behind. That's
# why the table must be created before data starts flowing, with the
# identifier field set via create_iceberg_table.py (pyiceberg → Nessie REST).
# Relational PK = --incrementing-col (INC); mongo is always _id (STRING).
# mssql introspection doesn't exist yet → use --descriptor.
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
# ORDER MATTERS: the connector must NOT be applied before the table is ready
# with its identifier field (otherwise the append-only bug). create_iceberg_
# table.py must be run in an environment with Nessie/S3 access (and access to
# the source DB for discover); that's why it is NOT applied AUTOMATICALLY
# here — the command is printed, apply the connector AFTER this.
echo ">> 1) ÖNCE Bronze Iceberg tabloyu pre-create edin (day(__ts_ms) partition, rawdata warehouse):"; echo "   $PRECREATE_BRONZE"
echo ">> 2) SONRA Silver Iceberg tabloyu pre-create edin (identifier field ile):"; echo "   $PRECREATE"
echo ">> 3) KafkaTopic uygula:"; echo "$TOPIC_YAML" | oc apply -f -
echo ">> 4) Her iki tablo da hazır OLDUKTAN sonra connector manifestini apply edin:"; echo "$CONN_YAML"
