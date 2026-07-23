#!/usr/bin/env python3
"""create_iceberg_table.py — pre-creates the SILVER current-state Iceberg table
(the Bronze→Silver MERGE target) WITH its identifier field, BEFORE the pipeline
runs. Silver lives in the Nessie default warehouse (`warehouse`); the append-only
sink writes the raw change-log to Bronze (`<ns>_raw`, rawdata warehouse) and the
`silver-merge` Spark job MERGEs Bronze→Silver keyed on this identifier.

NEDEN / WHY (medalyon CDC tasarımı):
  Silver tablosu, Bronze değişiklik-log'undan MERGE INTO ile beslenen
  current-state hedeftir; MERGE'in eşleşme koşulu (`ON silver.pk =
  bronze.pk`) identifier field'ın Silver şemasında baştan var olmasını
  gerektirir. identifier field YALNIZCA CREATE anında set edilebiliyor:
  Nessie REST üzerinden ALTER / set-identifier-fields HTTP 400 döndürüyor;
  Trino ise identifier field'ı hiç desteklemiyor. Dolayısıyla Silver tablosu
  pyiceberg ile pre-create EDİLMELİDİR (console/backend/app/services/
  iceberg_service.py bu script'in Console tarafındaki karşılığıdır).

İKİ GİRDİ MODU / TWO INPUT MODES:
  --descriptor FILE   YAML/JSON şema tanımı — B-v2 ArgoCD PreSync hook Job'ının
                      ConfigMap'ten mount ederek kullandığı deklaratif girdi.
                      {namespace, table, identifier: [...],
                       columns: [{name, type, required}]}
  --discover          Kaynak DB'ye bağlanıp (pg/mongo) şema + PK introspeksiyonu
                      yapar — B-v1 Console backend'i ve scaffold-source.sh
                      tarafından kullanılır.

IDEMPOTENCY / FAIL-LOUD SÖZLEŞMESİ:
  Tablo zaten varsa, identifier field'ları istenenle AYNI olmalıdır — aksi
  halde `exit 1` ile SESLİCE patlar (asla sessizce devam etmez; append-only
  bug'ı tam olarak böyle sızmıştı). Sütun kayması (drift) bu script'in kapsamı
  DIŞINDADIR — sink'in evolve-schema-enabled ayarı yeni sütunları yazma anında
  ekler. Bir PreSync hook'ta bu davranış, tablo/anahtar hazır olmadan
  KafkaConnector'ın apply edilmesini kilitler.

Bağımlılıklar / deps:  pip install "pyiceberg[s3fs]"
  (discover modu için ayrıca: psycopg[binary] (pg) veya pymongo (mongo);
   YAML descriptor için: pyyaml)
"""
from __future__ import annotations

import argparse
import json
import os
import sys

from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import DayTransform
from pyiceberg.types import (
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    TimestampType,
    NestedField,
)

# Kanonik Iceberg tip adları → pyiceberg tip nesnesi.
# Storage defaults applied at CREATE time via pyiceberg `properties=` (kept in
# lockstep with console/backend/app/services/iceberg_service.py — same
# constant, same CoW trio). 128 MiB bounds small-file growth for maintenance
# compaction (ALL layers).
_TARGET_FILE_SIZE_BYTES = "134217728"  # 128 MiB — bounds small-file growth; see spec storage note.

_ICEBERG_TYPES = {
    "int": IntegerType(),
    "long": LongType(),
    "string": StringType(),
    "boolean": BooleanType(),
    "double": DoubleType(),
    "float": FloatType(),
    "date": DateType(),
    "timestamp": TimestampType(),
    "binary": BinaryType(),
}


# Fixed CDC metadata columns appended to a BRONZE changelog table's business
# schema. Types match exactly what the source SMTs + append sink write:
# __ts_ms is TIMESTAMP (Connect Timestamp via the `tsconv` TimestampConverter
# SMT), __deleted is STRING ("true"/"false" from ExtractNewRecordState
# rewrite mode), __op string, __lsn long. __ts_ms (Debezium envelope/
# processing time, coerced to timestamp) serves BOTH MERGE ordering AND the
# Bronze partition key (day(__ts_ms)).
BRONZE_METADATA_COLS = [
    {"name": "__op", "type": "string"},
    {"name": "__ts_ms", "type": "timestamp"},
    {"name": "__deleted", "type": "string"},
    {"name": "__lsn", "type": "long"},
    # Deterministic dedup tie-break: Kafka offset+partition of each record,
    # inserted by the Iceberg sink's InsertField SMT (chart 13-connectors.yaml).
    {"name": "__kafka_offset", "type": "long"},
    {"name": "__kafka_partition", "type": "int"},
]


def _iceberg_type(name: str):
    n = name.strip().lower()
    if n.startswith("decimal"):
        inside = n[n.find("(") + 1 : n.find(")")] if "(" in n else "38,10"
        p, s = (int(x) for x in inside.split(","))
        return DecimalType(p, s)
    if n in _ICEBERG_TYPES:
        return _ICEBERG_TYPES[n]
    # Kanonik Iceberg adı değilse, SQL/JDBC tip adı olarak ele al (bigint→long,
    # varchar→string, ...) — descriptor hem kanonik hem SQL tipini kabul etsin
    # (console IcebergService ve discover modu ile tutarlı). decimal(p,s) zaten
    # yukarıda ele alındı; _map_sql_type "decimal(38,10)" gibi bir string
    # dönerse onu da çöz.
    mapped = _map_sql_type(name)
    if mapped.startswith("decimal"):
        return _iceberg_type(mapped)
    if mapped in _ICEBERG_TYPES:
        return _ICEBERG_TYPES[mapped]
    raise SystemExit(f"FATAL: bilinmeyen tip '{name}' (descriptor'da düzeltin)")


# JDBC/SQL tip aileleri → kanonik Iceberg tipi (statik eşleme; kenar tipleri
# descriptor ile override edilebilir).
_JDBC_TO_ICEBERG = {
    "int": "int", "integer": "int", "smallint": "int", "tinyint": "int", "serial": "int",
    "bigint": "long", "bigserial": "long",
    "bool": "boolean", "boolean": "boolean", "bit": "boolean",
    "real": "float",
    "double": "double", "double precision": "double", "float": "double", "float8": "double",
    "numeric": "decimal(38,10)", "decimal": "decimal(38,10)", "money": "decimal(38,4)",
    "date": "date",
    "timestamp": "timestamp", "timestamptz": "timestamp",
    "timestamp with time zone": "timestamp", "timestamp without time zone": "timestamp",
    "datetime": "timestamp", "datetime2": "timestamp", "smalldatetime": "timestamp",
    "bytea": "binary", "varbinary": "binary", "binary": "binary", "image": "binary",
    # kalan her şey (text/varchar/char/uuid/json/jsonb/nvarchar/...) → string
}


def _map_sql_type(sql_type: str) -> str:
    t = sql_type.strip().lower()
    for key, iceberg in _JDBC_TO_ICEBERG.items():
        if t == key or t.startswith(key + "("):
            return iceberg
    return "string"


def _build_schema(columns: list[dict], identifier: list[str]) -> Schema:
    """columns=[{name,type,required}], identifier=[col,...] → pyiceberg Schema.
    identifier kolonları Iceberg kuralı gereği REQUIRED olmalıdır; burada zorla
    required=True yapılır (kaynakta nullable görünse bile PK non-null'dır)."""
    id_names = set(identifier)
    fields, name_to_id = [], {}
    for i, col in enumerate(columns, start=1):
        name = col["name"]
        required = bool(col.get("required", False)) or name in id_names
        fields.append(NestedField(i, name, _iceberg_type(col["type"]), required=required))
        name_to_id[name] = i
    missing = id_names - set(name_to_id)
    if missing:
        raise SystemExit(f"FATAL: identifier kolonu şemada yok: {sorted(missing)}")
    return Schema(*fields, identifier_field_ids=[name_to_id[n] for n in identifier])


def _build_partition_spec(schema: Schema, column_name: str | None) -> PartitionSpec:
    """day(<column_name>) PartitionSpec for the given schema, or unpartitioned when
    column_name is None. Used for Bronze (day(__ts_ms)); Silver stays
    unpartitioned. The source column must be a timestamp/date (day() requires it)."""
    if not column_name:
        return PartitionSpec()  # unpartitioned
    src = schema.find_field(column_name)
    return PartitionSpec(
        PartitionField(source_id=src.field_id, field_id=1000,
                       transform=DayTransform(), name=f"{column_name}_day")
    )


def _load_catalog(args) -> RestCatalog:
    props = {
        "uri": args.catalog_uri,
        "s3.endpoint": args.s3_endpoint,
        "s3.access-key-id": args.s3_access_key,
        "s3.secret-access-key": args.s3_secret_key,
        "s3.path-style-access": "true",
    }
    if getattr(args, "warehouse", None):
        props["warehouse"] = args.warehouse
    return RestCatalog(args.catalog_name, **{k: v for k, v in props.items() if v})


# ---------------------------------------------------------------------------
# Discover modu: kaynak DB'den şema + PK introspeksiyonu (B-v1 / scaffold)
# ---------------------------------------------------------------------------
def _discover_pg(dsn: str, schema_table: str) -> tuple[list[dict], list[str]]:
    import psycopg  # pip install "psycopg[binary]"

    schema, _, table = schema_table.partition(".")
    if not table:
        schema, table = "public", schema
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT column_name, data_type, is_nullable FROM information_schema.columns "
            "WHERE table_schema=%s AND table_name=%s ORDER BY ordinal_position",
            (schema, table),
        )
        cols = [
            {"name": r[0], "type": _map_sql_type(r[1]), "required": r[2] == "NO"}
            for r in cur.fetchall()
        ]
        cur.execute(
            "SELECT a.attname FROM pg_index i "
            "JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=ANY(i.indkey) "
            "WHERE i.indrelid=%s::regclass AND i.indisprimary",
            (f"{schema}.{table}",),
        )
        pk = [r[0] for r in cur.fetchall()]
    if not cols:
        raise SystemExit(f"FATAL: pg tablosu bulunamadı/boş: {schema}.{table}")
    if not pk:
        raise SystemExit(f"FATAL: {schema}.{table} PRIMARY KEY yok — upsert/delete için PK zorunlu")
    return cols, pk


def _discover_mongo(uri: str, db: str, collection: str) -> tuple[list[dict], list[str]]:
    # Mongo şemasızdır: _id daima STRING identifier olarak açılır (garanti anahtar).
    # Alan çıkarımı örnek dokümandan best-effort'tur; yeni alanlar sink'in
    # evolve-schema-enabled'ı ile sonradan eklenir.
    import pymongo  # pip install pymongo

    client = pymongo.MongoClient(uri)
    doc = client[db][collection].find_one()
    cols = [{"name": "_id", "type": "string", "required": True}]
    seen = {"_id"}
    for k, v in (doc or {}).items():
        if k in seen:
            continue
        seen.add(k)
        if isinstance(v, bool):
            t = "boolean"
        elif isinstance(v, int):
            t = "long"
        elif isinstance(v, float):
            t = "double"
        else:
            t = "string"
        cols.append({"name": k, "type": t, "required": False})
    return cols, ["_id"]


# ---------------------------------------------------------------------------
# Create + fail-loud verify
# ---------------------------------------------------------------------------
def _ns_exists(cat: RestCatalog, namespace: str) -> bool:
    try:
        existing = {n[0] if isinstance(n, tuple) else n for n in cat.list_namespaces()}
        return namespace in existing
    except Exception:
        return True


def _ensure_table(cat: RestCatalog, namespace: str, table: str, schema: Schema,
                   partition_spec: PartitionSpec, properties: dict) -> int:
    """Namespace + tabloyu (identifier field'lı) yaratır. Tablo varsa identifier
    field'ları doğrular; uyuşmazsa 1 döner (fail-loud). Dönüş kodları:
    0=yaratıldı, 2=zaten uyumlu (no-op), 1=FATAL uyumsuzluk.

    `properties`: CREATE anında set edilen Iceberg tablo property'leri (128MB
    target-file-size ALL layers; Silver'a ek olarak CoW trio) — bkz. main()."""
    fq = f"{namespace}.{table}"
    want = set(schema.identifier_field_names())
    try:
        cat.create_namespace(namespace)
    except Exception:
        pass  # zaten var / eşzamanlı yaratım
    existing = {t[1] for t in cat.list_tables(namespace)} if _ns_exists(cat, namespace) else set()
    if table in existing:
        have = set(cat.load_table(fq).schema().identifier_field_names())
        if have != want:
            print(
                f"FATAL: '{fq}' zaten var ama identifier field'ları uyuşmuyor "
                f"(mevcut={sorted(have) or 'YOK'}, beklenen={sorted(want)}). "
                f"Nessie REST identifier field'ı ALTER edemez — tabloyu düşürüp "
                f"yeniden yaratmanız gerekir. KafkaConnector apply EDİLMEMELİ.",
                file=sys.stderr,
            )
            return 1
        print(f"OK (no-op): '{fq}' zaten doğru identifier ile mevcut: {sorted(have)}")
        return 2
    cat.create_table(fq, schema=schema, partition_spec=partition_spec, properties=properties)
    got = sorted(cat.load_table(fq).schema().identifier_field_names())
    if set(got) != want:
        print(f"FATAL: '{fq}' yaratıldı ama identifier set edilemedi (got={got})", file=sys.stderr)
        return 1
    print(f"CREATED '{fq}' identifier={got}")
    return 0


def main() -> int:
    p = argparse.ArgumentParser(description="Pre-create Iceberg CDC tables with identifier fields (Nessie REST).")
    p.add_argument("--catalog-name", default="nessie")
    p.add_argument("--catalog-uri", default=os.environ.get("NESSIE_URI", "http://nessie:19120/iceberg/"))
    p.add_argument("--s3-endpoint", default=os.environ.get("S3_ENDPOINT"))
    p.add_argument("--s3-access-key", default=os.environ.get("S3_ACCESS_KEY"))
    p.add_argument("--s3-secret-key", default=os.environ.get("S3_SECRET_KEY"))
    # descriptor modu
    p.add_argument("--descriptor", help="YAML/JSON: {namespace, table, identifier[], columns[]}")
    # discover modu
    p.add_argument("--discover", action="store_true")
    p.add_argument("--source-type", choices=["pg", "mongo"], help="discover için kaynak tipi")
    p.add_argument("--dsn", help="pg: libpq DSN/URI (ör. postgresql://user:pass@host:5432/db)")
    p.add_argument("--mongo-uri", help="mongo: connection string")
    p.add_argument("--db", help="mongo: kaynak DB adı")
    p.add_argument("--source-table", help="discover: kaynak tablo/collection (pg: schema.table)")
    p.add_argument("--namespace", help="hedef Iceberg namespace (ör. depo)")
    p.add_argument("--table", help="hedef Iceberg tablo adı")
    p.add_argument("--identifier-columns", help="discover override: virgülle ayrık PK kolonları")
    p.add_argument("--dry-run", action="store_true", help="şemayı yazdır, yaratma")
    p.add_argument("--layer", choices=["bronze", "silver"], default="silver",
                   help="bronze: append changelog (rawdata, day(__ts_ms) partition, "
                        "no identifier) + appends fixed CDC metadata cols; "
                        "silver: curated current-state (default warehouse, identifier).")
    p.add_argument("--warehouse", default=None,
                   help="Nessie warehouse override; default bronze=rawdata, silver=default.")
    args = p.parse_args()

    if args.descriptor:
        with open(args.descriptor) as f:
            raw = f.read()
        try:
            d = json.loads(raw)
        except json.JSONDecodeError:
            import yaml  # pip install pyyaml
            d = yaml.safe_load(raw)
        namespace, table = d["namespace"], d["table"]
        columns, identifier = d["columns"], d["identifier"]
        # Descriptor'daki `layer`/`warehouse` alanları CLI bayraklarının önüne
        # geçer — descriptor tek gerçek kaynak (IaC) olduğundan.
        if d.get("layer"):
            args.layer = d["layer"]
        if d.get("warehouse") and not args.warehouse:
            args.warehouse = d["warehouse"]
    elif args.discover:
        if not (args.namespace and args.table and args.source_table):
            p.error("--discover için --namespace --table --source-table zorunlu")
        if args.source_type == "pg":
            columns, identifier = _discover_pg(args.dsn, args.source_table)
        elif args.source_type == "mongo":
            columns, identifier = _discover_mongo(args.mongo_uri, args.db, args.source_table)
        else:
            p.error("--discover için --source-type pg|mongo zorunlu")
        if args.identifier_columns:
            identifier = [c.strip() for c in args.identifier_columns.split(",")]
        namespace, table = args.namespace, args.table
    else:
        p.error("--descriptor VEYA --discover verin")

    partition_col = None
    if args.layer == "bronze":
        existing = {c["name"] for c in columns}
        columns = columns + [c for c in BRONZE_METADATA_COLS if c["name"] not in existing]
        identifier = []            # append log has no identifier
        partition_col = "__ts_ms"
        if args.warehouse is None:
            args.warehouse = "rawdata"

    schema = _build_schema(columns, identifier)
    partition_spec = _build_partition_spec(schema, partition_col)
    # Storage defaults (kept in lockstep with console/backend's
    # IcebergService.create_table — same constant, same CoW trio).
    properties = {"write.target-file-size-bytes": _TARGET_FILE_SIZE_BYTES}
    if args.layer == "silver":
        # entity/upsert Silver: read-heavy (Trino) + MERGE-written -> copy-on-write.
        properties.update({
            "write.merge.mode": "copy-on-write",
            "write.update.mode": "copy-on-write",
            "write.delete.mode": "copy-on-write",
        })
    if args.dry_run:
        print(f"layer={args.layer} warehouse={args.warehouse or 'default'} "
              f"namespace={namespace} table={table} identifier={identifier}")
        print(f"partition={[f.name for f in partition_spec.fields] or 'unpartitioned'}")
        print(f"properties={properties}")
        print(schema)
        return 0
    cat = _load_catalog(args)
    rc = _ensure_table(cat, namespace, table, schema, partition_spec, properties)
    return 0 if rc in (0, 2) else 1


if __name__ == "__main__":
    sys.exit(main())
