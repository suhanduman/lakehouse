"""IcebergService — CDC hedef Iceberg tablolarını *identifier field*
(equality-delete anahtarı / PK) ile pyiceberg → Nessie REST üzerinden
pre-create eder.

NEDEN: Iceberg sink'in dinamik-routing auto-create'i tabloyu
`identifier-field-ids` OLMADAN yaratır → `upsert-enabled` sessizce
append-only'e düşer ve her CDC UPDATE duplicate satır bırakır. Trino
identifier field'ı DDL ile SET EDEMEZ; Nessie REST ALTER'ı reddediyor (400)
— identifier YALNIZCA CREATE anında verilebiliyor. Bu yüzden Console tablo
yaratımı Trino DDL değil, pyiceberg üzerinden yapılır.

Çekirdek mantık (tip eşleme + şema + fail-loud) tools/create_iceberg_table.py
ile AYNIDIR — ikisi senkron tutulmalı (biri değişince diğeri de).

pyiceberg LAZY import edilir: bu modül pyiceberg kurulu olmadan da import
edilebilir (unit testler `get_iceberg`'i fake IcebergService ile override eder,
gerçek pyiceberg yoluna hiç girmez)."""
from __future__ import annotations

from typing import Any, Callable, Dict, List


class IdentifierMismatch(Exception):
    """Tablo zaten var ama identifier field'ları isteneni karşılamıyor. Router
    bunu 409'a çevirir — SESSİZCE devam edilmez (append-only bug'ının kaynağı)."""


# medallion CDC (tools/create_iceberg_table.py — kept verbatim in sync here;
# that module is not importable from this backend's venv, see pytest.ini
# `pythonpath = .` scoped to console/backend). Fixed CDC metadata columns
# appended to a BRONZE changelog table's business schema. Types match
# exactly what the source SMTs + append sink write: __ts_ms is TIMESTAMP,
# __deleted is STRING ("true"/"false" from ExtractNewRecordState rewrite
# mode), __op string, __lsn long. Do NOT change these types (__deleted is
# NOT boolean, __ts_ms is NOT long) without updating
# tools/create_iceberg_table.py in lockstep.
#
# __ts_ms serves BOTH MERGE ordering AND the Bronze partition key.
BRONZE_METADATA_COLS = [
    {"name": "__op", "type": "string"},
    {"name": "__ts_ms", "type": "timestamp"},
    {"name": "__deleted", "type": "string"},
    {"name": "__lsn", "type": "long"},
    # Deterministic dedup tie-break: Kafka offset+partition of each record,
    # inserted by the Console-rendered dedicated Iceberg sink's InsertField SMT
    # (render_service._iceberg_sink_config / _cdc_dedicated_sink_config).
    {"name": "__kafka_offset", "type": "long"},
    {"name": "__kafka_partition", "type": "int"},
]

# Bronze changelog partitioning: day(__ts_ms). Field name mirrors
# tools/create_iceberg_table.py's _build_partition_spec (f"{column}_day").
_BRONZE_PARTITION_COL = "__ts_ms"
_BRONZE_WAREHOUSE = "rawdata"

# Storage defaults applied at CREATE time via pyiceberg `properties=` (kept in
# lockstep with tools/create_iceberg_table.py — same constant, same CoW trio).
# 128 MiB bounds small-file growth for maintenance compaction (ALL layers).
_TARGET_FILE_SIZE_BYTES = "134217728"  # 128 MiB — bounds small-file growth; see spec storage note.


# JDBC/SQL (Trino tip adları dahil) → kanonik Iceberg tipi. Kalan her şey → string.
_SQL_TO_ICEBERG = {
    "int": "int", "integer": "int", "smallint": "int", "tinyint": "int", "serial": "int",
    "bigint": "long", "bigserial": "long", "long": "long",
    "bool": "boolean", "boolean": "boolean", "bit": "boolean",
    "real": "float", "float": "double", "float8": "double",
    "double": "double", "double precision": "double",
    "numeric": "decimal(38,10)", "decimal": "decimal(38,10)", "money": "decimal(38,4)",
    "date": "date",
    "timestamp": "timestamp", "timestamptz": "timestamp",
    "timestamp with time zone": "timestamp", "timestamp without time zone": "timestamp",
    "datetime": "timestamp", "datetime2": "timestamp", "smalldatetime": "timestamp",
    "bytea": "binary", "varbinary": "binary", "binary": "binary", "image": "binary",
}


def _map_sql_type(sql_type: str) -> str:
    t = sql_type.strip().lower()
    for key, iceberg in _SQL_TO_ICEBERG.items():
        if t == key or t.startswith(key + "("):
            return iceberg
    return "string"


class IcebergService:
    """`catalog_factory(warehouse=None)` çağrıldığında bir pyiceberg RestCatalog
    döndürür (deps.py gerçeğini kurar; testler bu servisi tümden fake'ler).
    `warehouse` verildiğinde (bronze → "rawdata") catalog o Nessie warehouse'a
    bağlanır — bu, warehouse'un (ve dolayısıyla S3 konumunun) tek geçerli
    resolve mekanizmasıdır (bkz. tools/create_iceberg_table.py _load_catalog);
    namespace property olarak "warehouse" stamplemek Nessie tarafından yok
    sayılır. Her warehouse için lazy + per-warehouse cache'lenir ki authz
    403'te (sağlayıcı hiç inşa edilmeden) ağ/creds gerekmesin —
    TrinoService'in conn_factory deseniyle aynı."""

    def __init__(self, catalog_factory: Callable[..., Any]) -> None:
        self._catalog_factory = catalog_factory
        self._catalogs: Dict[Any, Any] = {}

    def _catalog_(self, warehouse: str | None = None) -> Any:
        if warehouse not in self._catalogs:
            self._catalogs[warehouse] = self._catalog_factory(warehouse=warehouse)
        return self._catalogs[warehouse]

    def _build_schema(self, columns: List[Dict[str, Any]], identifier: List[str]):
        from pyiceberg.schema import Schema
        from pyiceberg.types import (
            BinaryType, BooleanType, DateType, DecimalType, DoubleType, FloatType,
            IntegerType, LongType, NestedField, StringType, TimestampType,
        )

        prim = {
            "int": IntegerType(), "long": LongType(), "string": StringType(),
            "boolean": BooleanType(), "double": DoubleType(), "float": FloatType(),
            "date": DateType(), "timestamp": TimestampType(), "binary": BinaryType(),
        }

        def _to_type(name: str):
            n = name.strip().lower()
            if n.startswith("decimal"):
                inside = n[n.find("(") + 1 : n.find(")")] if "(" in n else "38,10"
                p, s = (int(x) for x in inside.split(","))
                return DecimalType(p, s)
            return prim.get(n) or prim[_map_sql_type(name)]

        id_names = set(identifier)
        fields, name_to_id = [], {}
        for i, col in enumerate(columns, start=1):
            nm = col["name"]
            # identifier kolonları Iceberg kuralı gereği REQUIRED olmalı.
            required = bool(col.get("required", False)) or nm in id_names
            fields.append(NestedField(i, nm, _to_type(col["type"]), required=required))
            name_to_id[nm] = i
        missing = id_names - set(name_to_id)
        if missing:
            raise ValueError(f"identifier kolonu şemada yok: {sorted(missing)}")
        return Schema(*fields, identifier_field_ids=[name_to_id[n] for n in identifier])

    @staticmethod
    def _bronze_partition_spec(schema) -> Any:
        """day(__ts_ms) PartitionSpec — mirrors tools/create_iceberg_table.py's
        _build_partition_spec (field name f"{col}_day", i.e. __ts_ms_day)."""
        from pyiceberg.partitioning import PartitionField, PartitionSpec
        from pyiceberg.transforms import DayTransform

        src = schema.find_field(_BRONZE_PARTITION_COL)
        return PartitionSpec(
            PartitionField(
                source_id=src.field_id,
                field_id=1000,
                transform=DayTransform(),
                name=f"{_BRONZE_PARTITION_COL}_day",
            )
        )

    def create_table(
        self,
        namespace: str,
        table: str,
        columns: List[Dict[str, Any]],
        identifier: List[str],
        location: str | None = None,
        layer: str = "silver",
    ) -> str:
        """Namespace + tabloyu yaratır (idempotent).

        layer="silver" (default): mevcut davranış — identifier field (PK)
        ZORUNLU (boşsa fail-loud ValueError), default warehouse, unpartitioned.
        Tablo VARSA identifier'ı doğrular; uyuşmazsa IdentifierMismatch fırlatır
        (fail-loud). `location` verilirse namespace bu S3 konumuyla yaratılır
        (kaynak-başına bucket modeli — Model X).

        layer="bronze": medallion CDC changelog tablosu — identifier YOK
        (append-only log; MERGE hedefi Silver'dır), sabit CDC metadata
        kolonları (BRONZE_METADATA_COLS) şemaya eklenir, `rawdata` Nessie
        warehouse'una (catalog construction property — `_catalog_factory(
        warehouse="rawdata")`, S3 konumunu bu belirler) ve day(__ts_ms)
        partition spec'ine yaratılır. Bkz. tools/create_iceberg_table.py — bu
        metod onun Console tarafındaki pre-create karşılığıdır.

        Dönüş: fully-qualified name."""
        if layer == "silver" and not identifier:
            raise ValueError("identifier (PK) boş olamaz — upsert/delete için zorunlu")

        partition_spec = None
        ns_properties: Dict[str, Any] = {"location": location} if location else {}
        warehouse = None
        if layer == "bronze":
            identifier = []  # changelog — no identifier, ever (fail-loud rule is silver-only)
            existing_names = {c["name"] for c in columns}
            columns = list(columns) + [
                c for c in BRONZE_METADATA_COLS if c["name"] not in existing_names
            ]
            warehouse = _BRONZE_WAREHOUSE

        cat = self._catalog_(warehouse=warehouse)
        schema = self._build_schema(columns, identifier)
        want = set(schema.identifier_field_names())
        if layer == "bronze":
            partition_spec = self._bronze_partition_spec(schema)
        try:
            cat.create_namespace(namespace, ns_properties)
        except Exception:
            pass  # zaten var / eşzamanlı
        fq = f"{namespace}.{table}"
        try:
            existing = {t[1] for t in cat.list_tables(namespace)}
        except Exception:
            existing = set()
        if table in existing:
            have = set(cat.load_table(fq).schema().identifier_field_names())
            if have != want:
                raise IdentifierMismatch(
                    f"'{fq}' zaten var, identifier uyuşmuyor (mevcut={sorted(have) or 'YOK'}, "
                    f"beklenen={sorted(want)}). Nessie REST identifier ALTER edemez; "
                    f"tabloyu düşürüp yeniden yaratın."
                )
            return fq
        properties = {"write.target-file-size-bytes": _TARGET_FILE_SIZE_BYTES}
        if layer == "silver":
            # entity/upsert Silver: read-heavy (Trino) + MERGE-written -> copy-on-write.
            properties.update({
                "write.merge.mode": "copy-on-write",
                "write.update.mode": "copy-on-write",
                "write.delete.mode": "copy-on-write",
            })
        if partition_spec is not None:
            cat.create_table(fq, schema=schema, partition_spec=partition_spec, properties=properties)
        else:
            cat.create_table(fq, schema=schema, properties=properties)
        return fq

    def table_stats(self, namespace: str, table: str, layer: str = "silver") -> Dict[str, int] | None:
        """{'records','data_files'} from the current snapshot summary (metadata
        read, no scan), or None if the table is absent / has no snapshot."""
        warehouse = _BRONZE_WAREHOUSE if layer == "bronze" else None
        try:
            tbl = self._catalog_(warehouse=warehouse).load_table(f"{namespace}.{table}")
            snap = tbl.current_snapshot()
        except Exception:
            return None
        if snap is None:
            return None
        summary = snap.summary or {}
        return {
            "records": int(summary.get("total-records", 0)),
            "data_files": int(summary.get("total-data-files", 0)),
        }

    def namespace_tables(self, namespace: str, layer: str = "bronze") -> set:
        """Table names in `namespace` (empty set if the namespace is absent).
        Read-only — used by the orchestrator uniqueness check."""
        warehouse = _BRONZE_WAREHOUSE if layer == "bronze" else None
        cat = self._catalog_(warehouse=warehouse)
        try:
            return {t[1] for t in cat.list_tables(namespace)}
        except Exception:
            return set()
