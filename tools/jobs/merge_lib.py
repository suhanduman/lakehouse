"""Pure, Spark-free helpers for the Bronze->Silver CDC MERGE job (merge_cdc.py).
No pyspark import here so the merge logic is unit-testable without a Spark
runtime.
"""
from __future__ import annotations

from dataclasses import dataclass, field

# CDC metadata columns present in Bronze but NOT in Silver (Silver holds business
# columns exclusively). __op/__ts_ms/__lsn come from the Debezium
# ExtractNewRecordState `add.fields`; __deleted from delete.handling.mode=rewrite.
# `_target_table` is the InsertField route field every source sets for the sink's
# dynamic routing — the Apache sink reads it for routing but ALSO writes it as a
# data column (it does not strip it), so it leaks into Bronze and must be
# excluded from Silver's business columns.
# `__ts_ms` (Debezium envelope/processing timestamp, coerced to a Connect
# Timestamp via the `tsconv` TimestampConverter SMT) is BOTH the MERGE ordering
# key AND the Bronze partition key `day(__ts_ms)` — Bronze-only, excluded from
# Silver like the other metadata columns.
# `__kafka_offset`/`__kafka_partition` are stamped by the Iceberg sink's
# InsertField SMT (chart/templates/13-connectors.yaml) from each record's
# Kafka offset/partition. __lsn is null for Mongo/JDBC sources, so on a
# __ts_ms tie the ordering was non-deterministic; Debezium keys by PK -> same
# key -> same Kafka partition -> offset is monotonic per key, making
# __kafka_offset a deterministic final tie-break in latest_per_key_sql's
# ORDER BY. Bronze-only, excluded from Silver like the other metadata columns.
METADATA_COLS = ("__op", "__ts_ms", "__deleted", "__lsn", "_target_table",
                 "__kafka_offset", "__kafka_partition")

# Iceberg-native safe (widening) numeric promotions, in the Spark type spellings
# a DESCRIBE returns. Decimal precision-widen (same scale) handled separately.
_SAFE_PROMOTIONS = {("int", "long"), ("int", "bigint"), ("float", "double")}


def has_cdc_metadata(schema: dict) -> bool:
    """True iff the Bronze table carries the CDC metadata contract the MERGE
    needs (__op, __ts_ms, __deleted). Non-relational sources (Mongo raw-JSON,
    scheduled-JDBC) route to Bronze without these; silver-merge skips them
    until their metadata SMT is implemented."""
    required = {"__op", "__ts_ms", "__deleted"}
    return required.issubset(set(schema))


def silver_ns_from_bronze(bronze_ns: str, suffix: str = "_raw") -> str:
    """'depo_raw' -> 'depo'. Fail-loud if the bronze namespace lacks the suffix."""
    if not bronze_ns.endswith(suffix):
        raise ValueError(f"bronze namespace {bronze_ns!r} does not end with {suffix!r}")
    return bronze_ns[: -len(suffix)]


def _decimal_parts(t: str) -> tuple[int, int]:
    inside = t[t.find("(") + 1 : t.find(")")]
    p, s = (int(x) for x in inside.split(","))
    return p, s


def is_safe_promotion(from_type: str, to_type: str) -> bool:
    """True iff to_type is an Iceberg-native widening of from_type."""
    f, t = from_type.strip().lower(), to_type.strip().lower()
    if f == t:
        return True
    if (f, t) in _SAFE_PROMOTIONS:
        return True
    if f.startswith("decimal(") and t.startswith("decimal("):
        (fp, fs), (tp, ts) = _decimal_parts(f), _decimal_parts(t)
        return fs == ts and tp >= fp
    return False


@dataclass
class ReconcilePlan:
    adds: list = field(default_factory=list)          # [(name, type)]
    promotions: list = field(default_factory=list)    # [(name, from, to)]
    conflicts: list = field(default_factory=list)     # [(name, from, to)]
    ignored_drops: list = field(default_factory=list) # [name]

    @property
    def ok(self) -> bool:
        return not self.conflicts


def reconcile_plan(bronze: dict, silver: dict) -> ReconcilePlan:
    """bronze/silver: {col_name: type_str}. Plan to bring Silver forward:
    ADD (bronze-only) / safe TYPE promotion / unsafe TYPE conflict; DROP
    (silver-only) is ignored (retain history). Bronze metadata cols excluded."""
    plan = ReconcilePlan()
    b = {k: v for k, v in bronze.items() if k not in METADATA_COLS}
    for name, btype in b.items():
        if name not in silver:
            plan.adds.append((name, btype))
        elif btype.strip().lower() != silver[name].strip().lower():
            if is_safe_promotion(silver[name], btype):
                plan.promotions.append((name, silver[name], btype))
            else:
                plan.conflicts.append((name, silver[name], btype))
    for name in silver:
        if name not in b:
            plan.ignored_drops.append(name)
    return plan


def latest_per_key_sql(bronze_fqn: str, id_cols: list, business_cols: list) -> str:
    """Dedup the Bronze increment to the newest row per key (ORDER BY __ts_ms,
    __lsn NULLS LAST, __kafka_offset — __kafka_offset is the deterministic
    final tie-break, since __lsn is null for Mongo/JDBC sources)."""
    part = ", ".join(id_cols)
    cols = ", ".join(business_cols + ["__deleted"])
    return (
        f"SELECT {cols} FROM ("
        f"SELECT {cols}, ROW_NUMBER() OVER ("
        f"PARTITION BY {part} ORDER BY __ts_ms DESC, __lsn DESC NULLS LAST, "
        f"__kafka_offset DESC) AS __rn "
        f"FROM {bronze_fqn}) WHERE __rn = 1"
    )


def merge_sql(silver_fqn: str, src_view: str, id_cols: list, business_cols: list) -> str:
    on = " AND ".join(f"t.{c} = s.{c}" for c in id_cols)
    set_cols = [c for c in business_cols if c not in id_cols]
    insert_cols = ", ".join(business_cols)
    insert_vals = ", ".join(f"s.{c}" for c in business_cols)
    # __deleted arrives as a STRING "true"/"false" from Debezium ExtractNewRecordState
    # (delete.handling.mode=rewrite), not a boolean — a bare `s.__deleted` in a boolean
    # context is a Spark type-mismatch error. CAST makes the delete flag robust whether
    # a source emits a string or a real boolean.
    deleted = "CAST(s.__deleted AS BOOLEAN)"
    parts = [
        f"MERGE INTO {silver_fqn} t USING {src_view} s ON {on}",
        f"WHEN MATCHED AND {deleted} THEN DELETE",
    ]
    if set_cols:
        set_clause = ", ".join(f"{c} = s.{c}" for c in set_cols)
        parts.append(f"WHEN MATCHED THEN UPDATE SET {set_clause}")
    parts.append(
        f"WHEN NOT MATCHED AND NOT {deleted} THEN INSERT ({insert_cols}) VALUES ({insert_vals})"
    )
    return " ".join(parts)
