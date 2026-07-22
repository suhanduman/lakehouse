"""Pure, Spark-free helpers for iceberg_maintenance.py (unit-testable without Spark)."""
from __future__ import annotations


def is_bronze_namespace(ns: str, suffix: str = "_raw") -> bool:
    """True for Bronze changelog namespaces (subject to retention TTL)."""
    return ns.endswith(suffix)


def bronze_ttl_delete_sql(catalog: str, ns: str, table: str, retention_days: int) -> str:
    """Partition-aligned metadata DELETE for Bronze retention. day(__ts_ms)
    partitioning makes a day-aligned predicate a metadata-only delete (no row scan,
    no positional deletes); expire_snapshots then physically reclaims the files.

    `__ts_ms` (Debezium envelope/processing time) is both the Bronze
    partition key and this TTL predicate's column."""
    return (f"DELETE FROM {catalog}.{ns}.{table} "
            f"WHERE __ts_ms < date_sub(current_date(), {int(retention_days)})")
