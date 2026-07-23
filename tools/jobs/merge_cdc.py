#!/usr/bin/env python3
"""Bronze -> Silver incremental CDC MERGE (ScheduledSparkApplication `silver-merge`).

Dynamic discovery (like iceberg_maintenance.py): finds every Bronze changelog
table `rawlake.<ns>_raw.<table>`, maps it to Silver `lakehouse.<ns>.<table>`,
reads only the new Bronze increment (Iceberg start-snapshot-id watermark stored
as the Silver table property `app.bronze-merged-snapshot`), dedups to
latest-per-key, and MERGEs into Silver. Resilient per-table loop; on any table
failure the driver exits non-zero. Recovery: --reset-watermark / --restate.
"""
from __future__ import annotations

import argparse
import sys
import threading
import time

try:
    from tools.jobs import merge_lib as ml          # repo-root / pytest layout
except ImportError:                                  # flat /opt/spark/jobs/ layout
    # The spark-py image bakes merge_cdc.py and merge_lib.py as flat siblings
    # into /opt/spark/jobs/ (no `tools` package at runtime).
    import merge_lib as ml

WATERMARK_PROP = "app.bronze-merged-snapshot"
AUDIT_TABLE = "lakehouse.ops.merge_runs"
METADATA_COLS = set(ml.METADATA_COLS)


class SkipTable(Exception):
    """Raised for a Bronze table that lacks the CDC metadata contract the MERGE
    needs; caught in run()'s per-table loop and treated as a SKIP, not a failure."""
    pass


def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Bronze->Silver incremental CDC MERGE.")
    p.add_argument("--bronze-catalog", default="rawlake")
    p.add_argument("--silver-catalog", default="lakehouse")
    p.add_argument("--bronze-suffix", default="_raw")
    # recovery / DR
    p.add_argument("--reset-watermark", metavar="NS.TABLE",
                   help="Silver identifier; clear/set its watermark, then exit.")
    p.add_argument("--from-snapshot", type=int, default=None,
                   help="with --reset-watermark: set watermark to this Bronze snapshot id "
                        "(omit to clear -> next run re-reads from the beginning).")
    p.add_argument("--restate", metavar="NS.TABLE",
                   help="Silver identifier; truncate Silver + clear watermark, then exit.")
    p.add_argument("--max-parallel", type=int, default=4,
                   help="max tables MERGEd concurrently (ThreadPoolExecutor over a "
                        "FAIR-scheduled shared Spark session).")
    return p.parse_args(argv)


# ---- Spark helpers (pyspark imported lazily so parse_args/tests need no Spark) ----

def _describe(spark, fqn: str) -> dict:
    """{col: spark_type} for a table, stopping at the DESCRIBE footer."""
    rows = spark.sql(f"DESCRIBE TABLE {fqn}").collect()
    out = {}
    for r in rows:
        name = (r["col_name"] or "").strip()
        if not name or name.startswith("#"):
            break
        out[name] = (r["data_type"] or "").strip()
    return out


def _get_prop(spark, fqn: str, key: str):
    try:
        rows = spark.sql(f"SHOW TBLPROPERTIES {fqn} ('{key}')").collect()
    except Exception:
        return None
    if rows:
        v = rows[0]["value"]
        if v and not v.startswith("Table ") :
            return v
    return None


def _set_prop(spark, fqn: str, key: str, value) -> None:
    spark.sql(f"ALTER TABLE {fqn} SET TBLPROPERTIES ('{key}'='{value}')")


def _current_snapshot(spark, fqn: str):
    rows = spark.sql(
        f"SELECT snapshot_id FROM {fqn}.snapshots ORDER BY committed_at DESC LIMIT 1"
    ).collect()
    return rows[0]["snapshot_id"] if rows else None


def _ensure_audit(spark) -> None:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.ops")
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {AUDIT_TABLE} ("
        "table_name STRING, start_snapshot BIGINT, end_snapshot BIGINT, "
        "increment_rows BIGINT, deleted_rows BIGINT, status STRING, error STRING, "
        "duration_s DOUBLE, run_ts TIMESTAMP) USING iceberg"
    )


def _q(v):
    return "NULL" if v is None else "'" + str(v).replace("'", "''") + "'"


def _n(v):
    return "NULL" if v is None else str(int(v))


def _audit(spark, **row) -> None:
    spark.sql(
        f"INSERT INTO {AUDIT_TABLE} VALUES ("
        f"{_q(row['table_name'])}, {_n(row['start_snapshot'])}, {_n(row['end_snapshot'])}, "
        f"{_n(row['increment_rows'])}, {_n(row['deleted_rows'])}, {_q(row['status'])}, "
        f"{_q(row['error'])}, {row['duration_s']}, current_timestamp())"
    )


def _apply_reconcile(spark, silver_fqn: str, plan: ml.ReconcilePlan) -> None:
    for name, btype in plan.adds:
        spark.sql(f"ALTER TABLE {silver_fqn} ADD COLUMN {name} {btype}")
        print(f"[reconcile] {silver_fqn}: ADD COLUMN {name} {btype}")
    for name, frm, to in plan.promotions:
        spark.sql(f"ALTER TABLE {silver_fqn} ALTER COLUMN {name} TYPE {to}")
        print(f"[reconcile] {silver_fqn}: ALTER COLUMN {name} {frm}->{to}")
    for name in plan.ignored_drops:
        print(f"[reconcile] {silver_fqn}: IGNORE dropped source column {name} (retain history)")


def _identifier_fields(spark, silver_fqn: str) -> list:
    """Silver's declared identifier (equality) field names — the MERGE key.
    Read from the Iceberg table's schema.identifierFieldNames() via the Spark
    Iceberg catalog's Java handle (Spark SQL DESCRIBE does not expose it)."""
    jvm = spark._jvm
    gateway = spark.sparkContext._gateway
    catalog_name, ns, table = silver_fqn.split(".")
    spark_catalog = spark._jsparkSession.sessionState().catalogManager().catalog(catalog_name)
    # Build a REAL Java String[] for the namespace levels: PythonUtils.toArray
    # returns Object[], which Identifier.of(String[], String) reflection rejects
    # ("Method of([Ljava.lang.Object;,String]) does not exist").
    levels = ns.split(".")
    ns_arr = gateway.new_array(jvm.java.lang.String, len(levels))
    for i, lvl in enumerate(levels):
        ns_arr[i] = lvl
    ident = jvm.org.apache.spark.sql.connector.catalog.Identifier.of(ns_arr, table)
    itable = spark_catalog.loadTable(ident)  # org.apache.iceberg.spark.source.SparkTable
    names = itable.table().schema().identifierFieldNames()
    ids = [str(n) for n in names]
    if not ids:
        raise RuntimeError(
            f"{silver_fqn} has no identifier fields — pre-create it with "
            f"tools/create_iceberg_table.py (identifier required for the MERGE key)"
        )
    return ids


def _read_increment(spark, bronze_fqn: str, start):
    """Bronze append-scan since the watermark snapshot. Falls back to a FULL read
    when `start` is not a valid ancestor of the current snapshot — which happens
    routinely once Bronze TTL/expire_snapshots prunes it, or on any lineage
    discontinuity. The MERGE is idempotent (upsert by identifier), so a full
    re-read is always correct; the watermark is an optimization, not a
    correctness input. This is the failure mode the fallback guards against:
    e.g. the watermark snapshot vanishing after a sink re-write → "not a
    parent ancestor" hard failure."""
    if not start:
        return spark.table(bronze_fqn)
    try:
        df = spark.read.format("iceberg").option("start-snapshot-id", start).load(bronze_fqn)
        df.count()  # force snapshot-ancestry validation now (the scan is otherwise lazy)
        return df
    except Exception as e:  # noqa: BLE001 - any incremental failure => safe full re-read
        print(f"[warn] {bronze_fqn}: incremental from snapshot {start} invalid "
              f"({str(e)[:100]}); full re-read (idempotent MERGE)")
        return spark.table(bronze_fqn)


def _merge_one(spark, bronze_fqn: str, silver_fqn: str) -> dict:
    """Reconcile + incremental read + dedup + MERGE + advance watermark for one
    table. Returns an audit dict. Raises on conflict / SQL error (caught by loop)."""
    t0 = time.time()
    # Spark temp views live in the shared session catalog, NOT thread-local --
    # two concurrent _merge_one calls on the same SparkSession would otherwise
    # clobber each other's fixed-name views. Suffix by thread identity so each
    # concurrent invocation gets its own view names.
    _sfx = f"{threading.get_ident()}"
    bronze_view = f"bronze_inc_{_sfx}"
    src_view = f"src_latest_{_sfx}"

    bronze_schema = _describe(spark, bronze_fqn)
    if not ml.has_cdc_metadata(bronze_schema):
        raise SkipTable(
            f"{bronze_fqn} lacks CDC metadata columns (__op/__ts_ms/__deleted) — "
            f"non-relational source; silver-merge supports relational CDC only for now"
        )
    silver_schema = _describe(spark, silver_fqn)
    plan = ml.reconcile_plan(bronze_schema, silver_schema)
    if not plan.ok:
        raise RuntimeError(f"unsafe schema change(s): {plan.conflicts} — resolve Silver manually")
    _apply_reconcile(spark, silver_fqn, plan)

    id_cols = _identifier_fields(spark, silver_fqn)
    business_cols = [c for c in bronze_schema if c not in METADATA_COLS]

    start = _get_prop(spark, silver_fqn, WATERMARK_PROP)
    end = _current_snapshot(spark, bronze_fqn)
    inc = _read_increment(spark, bronze_fqn, start)
    inc.createOrReplaceTempView(bronze_view)

    spark.sql(ml.latest_per_key_sql(bronze_view, id_cols, business_cols)) \
        .createOrReplaceTempView(src_view)
    increment_rows = spark.table(src_view).count()
    deleted_rows = spark.sql(
        f"SELECT count(*) c FROM {src_view} WHERE CAST(__deleted AS BOOLEAN)"
    ).collect()[0]["c"]

    if increment_rows:
        spark.sql(ml.merge_sql(silver_fqn, src_view, id_cols, business_cols))
    if end is not None:
        _set_prop(spark, silver_fqn, WATERMARK_PROP, end)

    return {
        "table_name": silver_fqn, "start_snapshot": start, "end_snapshot": end,
        "increment_rows": increment_rows, "deleted_rows": deleted_rows,
        "status": "ok", "error": None, "duration_s": round(time.time() - t0, 3),
    }


def _discover_bronze(spark, args) -> list:
    """[(bronze_fqn, silver_fqn)] for every rawlake namespace ending in the suffix."""
    pairs = []
    nss = [r.namespace for r in spark.sql(f"SHOW NAMESPACES IN {args.bronze_catalog}").collect()]
    for ns in nss:
        if not ns.endswith(args.bronze_suffix):
            continue
        silver_ns = ml.silver_ns_from_bronze(ns, args.bronze_suffix)
        for r in spark.sql(f"SHOW TABLES IN {args.bronze_catalog}.{ns}").collect():
            t = r.tableName
            pairs.append((f"{args.bronze_catalog}.{ns}.{t}",
                          f"{args.silver_catalog}.{silver_ns}.{t}"))
    return pairs


def _silver_exists(spark, silver_fqn: str) -> bool:
    catalog, ns, t = silver_fqn.split(".")
    try:
        return any(r.tableName == t for r in spark.sql(f"SHOW TABLES IN {catalog}.{ns}").collect())
    except Exception:
        return False


def _log_skip_missing(bronze_fqn: str, silver_fqn: str) -> None:
    print(f"[skip] {bronze_fqn}: Silver {silver_fqn} not pre-created yet")


def run(spark, args) -> int:
    _ensure_audit(spark)

    # ---- recovery / DR subcommands (operate on ONE Silver table, then exit) ----
    if args.reset_watermark:
        ns, t = args.reset_watermark.split(".")
        fqn = f"{args.silver_catalog}.{ns}.{t}"
        if args.from_snapshot is not None:
            _set_prop(spark, fqn, WATERMARK_PROP, args.from_snapshot)
            print(f"[recovery] {fqn}: watermark set to {args.from_snapshot}")
        else:
            spark.sql(f"ALTER TABLE {fqn} UNSET TBLPROPERTIES IF EXISTS ('{WATERMARK_PROP}')")
            print(f"[recovery] {fqn}: watermark cleared (next run re-reads from beginning)")
        return 0
    if args.restate:
        ns, t = args.restate.split(".")
        fqn = f"{args.silver_catalog}.{ns}.{t}"
        spark.sql(f"DELETE FROM {fqn}")  # truncate current state
        spark.sql(f"ALTER TABLE {fqn} UNSET TBLPROPERTIES IF EXISTS ('{WATERMARK_PROP}')")
        print(f"[recovery] {fqn}: restated (Silver emptied, watermark cleared)")
        return 0

    # ---- normal tick: parallel per-table MERGE, aggregate non-zero exit ----
    from concurrent.futures import ThreadPoolExecutor
    import threading

    pairs = []
    for bronze_fqn, silver_fqn in _discover_bronze(spark, args):
        if _silver_exists(spark, silver_fqn):
            pairs.append((bronze_fqn, silver_fqn))
        else:
            _log_skip_missing(bronze_fqn, silver_fqn)

    lock = threading.Lock()
    failures = {"n": 0}

    def _safe_audit(**row):
        # An audit write must never abort the batch: a Nessie commit conflict
        # on the shared lakehouse.ops.merge_runs table is exactly what retry
        # exists for, but if it ultimately still fails, log and move on rather
        # than propagate out of _do (which would cancel/abort remaining tables).
        try:
            ml.run_with_retry(lambda: _audit(spark, **row),
                              attempts=5, is_retryable=ml.is_commit_conflict, sleep=time.sleep)
        except Exception as e:  # noqa: BLE001 - an audit write must never abort the batch
            print(f"[warn] audit write failed for {row.get('table_name')}: {e}", file=sys.stderr)

    def _do(pair):
        bronze_fqn, silver_fqn = pair
        # FAIR scheduler pool per table -> concurrent MERGEs share resources fairly.
        spark.sparkContext.setLocalProperty("spark.scheduler.pool", f"merge-{silver_fqn}")
        try:
            row = ml.run_with_retry(
                lambda: _merge_one(spark, bronze_fqn, silver_fqn),
                attempts=5, is_retryable=ml.is_commit_conflict, sleep=time.sleep)
            _safe_audit(**row)
            print(f"[ok] {silver_fqn}: +{row['increment_rows']} (del {row['deleted_rows']}) "
                  f"{row['duration_s']}s")
        except SkipTable as e:  # not a failure — must not affect the aggregate exit code
            _safe_audit(table_name=silver_fqn, start_snapshot=None, end_snapshot=None,
                        increment_rows=None, deleted_rows=None, status="skipped",
                        error=str(e)[:2000], duration_s=0.0)
            print(f"[skip] {silver_fqn}: {e}")
        except Exception as e:  # noqa: BLE001 - per-table isolation; watermark NOT advanced
            with lock:
                failures["n"] += 1
            _safe_audit(table_name=silver_fqn, start_snapshot=None, end_snapshot=None,
                        increment_rows=None, deleted_rows=None, status="failed",
                        error=str(e)[:2000], duration_s=0.0)
            print(f"[FAIL] {silver_fqn}: {e}", file=sys.stderr)

    with ThreadPoolExecutor(max_workers=max(1, args.max_parallel)) as pool:
        list(pool.map(_do, pairs))
    return 1 if failures["n"] else 0


def main() -> int:
    from pyspark.sql import SparkSession
    args = parse_args()
    spark = SparkSession.builder.appName("SilverMergeCDC").getOrCreate()
    try:
        return run(spark, args)
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
