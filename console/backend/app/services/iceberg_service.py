"""IcebergService — pre-creates CDC target Iceberg tables with the
*identifier field* (equality-delete key / PK) via pyiceberg -> Nessie REST.

WHY: the Iceberg sink's dynamic-routing auto-create creates the table
WITHOUT `identifier-field-ids` -> `upsert-enabled` silently degrades to
append-only and every CDC UPDATE leaves a duplicate row. Trino CANNOT SET
the identifier field via DDL; Nessie REST rejects the ALTER (400) --
identifier can ONLY be given at CREATE time. So the Console creates tables
via pyiceberg, not Trino DDL.

The core logic (type mapping + schema + fail-loud) is IDENTICAL to
tools/create_iceberg_table.py -- the two must be kept in sync (a change to
one means a change to the other).

pyiceberg is imported LAZILY: this module can be imported even without
pyiceberg installed (unit tests override `get_iceberg` with a fake
IcebergService and never touch the real pyiceberg path)."""
from __future__ import annotations

from typing import Any, Callable, Dict, List


class IdentifierMismatch(Exception):
    """The table already exists but its identifier fields don't match what was
    requested. The router turns this into a 409 -- it never proceeds SILENTLY
    (that's the source of the append-only bug)."""


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
# 256 MiB bounds small-file growth for maintenance compaction (ALL layers).
_TARGET_FILE_SIZE_BYTES = "268435456"  # 256 MiB — bounds small-file growth; see spec storage note.


# JDBC/SQL (including Trino type names) -> canonical Iceberg type. Everything else -> string.
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
    """`catalog_factory(warehouse=None)` returns a pyiceberg RestCatalog when
    called (deps.py wires up the real one; tests fake this service entirely).
    When `warehouse` is given (bronze -> "rawdata"), the catalog connects to
    that Nessie warehouse -- this is the ONLY valid mechanism for resolving
    the warehouse (and hence the S3 location) (see tools/create_iceberg_table.py
    _load_catalog); stamping "warehouse" as a namespace property is ignored
    by Nessie. Lazy + cached per warehouse, so that a 403 (before any provider
    is even constructed) doesn't need network/creds -- same pattern as
    TrinoService's conn_factory."""

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
            # identifier columns must be REQUIRED per Iceberg's rules.
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

    @staticmethod
    def _silver_partition_spec(schema, identifier, bucket_count) -> Any:
        """bucket(N, id) per identifier column — aligns the MERGE join key with
        the partition transform so MERGE prunes to the increment's buckets."""
        from pyiceberg.partitioning import PartitionField, PartitionSpec
        from pyiceberg.transforms import BucketTransform

        fields = []
        for i, col in enumerate(identifier):
            src = schema.find_field(col)
            fields.append(
                PartitionField(
                    source_id=src.field_id,
                    field_id=1000 + i,
                    transform=BucketTransform(bucket_count),
                    name=f"{col}_bucket",
                )
            )
        return PartitionSpec(*fields)

    @staticmethod
    def _silver_sort_order(schema, identifier) -> Any:
        """Sort by the identifier column(s) — bundles equality-delete/upsert
        keys together within each data file, cutting file scans on MERGE."""
        from pyiceberg.table.sorting import SortField, SortOrder
        from pyiceberg.transforms import IdentityTransform

        return SortOrder(
            *[
                SortField(source_id=schema.find_field(c).field_id, transform=IdentityTransform())
                for c in identifier
            ]
        )

    def create_table(
        self,
        namespace: str,
        table: str,
        columns: List[Dict[str, Any]],
        identifier: List[str],
        location: str | None = None,
        layer: str = "silver",
        bucket_count: int = 16,
        write_mode: str = "copy-on-write",
    ) -> str:
        """Creates the namespace + table (idempotent).

        layer="silver" (default): the existing behavior -- identifier field
        (PK) is REQUIRED (fail-loud ValueError if empty), default warehouse,
        bucket(N, id) per identifier column (`bucket_count`, default 16) +
        identifier sort order + write.distribution-mode=hash -- a scale-ready
        layout so MERGE prunes to the increment's buckets instead of scanning
        the whole table. `write_mode` (default "copy-on-write") sets the
        write.merge/update/delete.mode trio ("merge-on-read" also
        supported). If the table already EXISTS, its identifier is validated;
        a mismatch raises IdentifierMismatch (fail-loud). If `location` is
        given, the namespace is created with that S3 location (the
        per-source bucket model -- Model X).

        layer="bronze": the medallion CDC changelog table -- NO identifier
        (append-only log; Silver is the MERGE target), the fixed CDC metadata
        columns (BRONZE_METADATA_COLS) are appended to the schema, and it's
        created in the `rawdata` Nessie warehouse (catalog construction
        property -- `_catalog_factory(warehouse="rawdata")`, which determines
        the S3 location) with a day(__ts_ms) partition spec. See
        tools/create_iceberg_table.py -- this method is its Console-side
        pre-create counterpart.

        Returns: fully-qualified name."""
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
            pass  # already exists / concurrent creation
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
        sort_order = None
        properties = {"write.target-file-size-bytes": _TARGET_FILE_SIZE_BYTES}
        if layer == "silver":
            # entity/upsert Silver: bucket(N, id) partition + identifier sort
            # order + hash distribution -> MERGE prunes to the increment's
            # buckets. write_mode (default copy-on-write) drives the
            # merge/update/delete trio; "merge-on-read" also supported.
            partition_spec = self._silver_partition_spec(schema, identifier, bucket_count)
            sort_order = self._silver_sort_order(schema, identifier)
            properties["write.distribution-mode"] = "hash"
            properties.update({
                "write.merge.mode": write_mode,
                "write.update.mode": write_mode,
                "write.delete.mode": write_mode,
            })
        kwargs: Dict[str, Any] = {"schema": schema, "properties": properties}
        if partition_spec is not None:
            kwargs["partition_spec"] = partition_spec
        if sort_order is not None:
            kwargs["sort_order"] = sort_order
        cat.create_table(fq, **kwargs)
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
