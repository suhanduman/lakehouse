# Iceberg bakım job'u — DİNAMİK tarama: hardcoded tablo listesi YOK.
# lakehouse ve rawlake kataloglarındaki tüm namespace/tablo çiftleri
# SHOW NAMESPACES / SHOW TABLES ile keşfedilir; her tablo için
# rewrite_data_files (compaction) + expire_snapshots + remove_orphan_files
# çağırılır. Yeni bir tablo eklendiğinde bu script veya manifest değişmez.
#
# Tablolar arasında paralel çalışır (ThreadPoolExecutor, MAINTENANCE_MAX_PARALLEL
# env, varsayılan 4) -- bir tablonun hatası diğerlerini durdurmaz.
#
# ScheduledSparkApplication `iceberg-maintenance` tarafından saatlik
# tetiklenir (bkz. chart/templates/05-spark-operator.yaml).
import os
from concurrent.futures import ThreadPoolExecutor

try:
    from tools.jobs import maintenance_lib as mlib          # repo-root / pytest layout
except ImportError:                                          # flat /opt/spark/jobs/ layout
    # The spark-py image bakes iceberg_maintenance.py and maintenance_lib.py as flat
    # siblings into /opt/spark/jobs/ (no `tools` package at runtime) — see merge_cdc.py.
    import maintenance_lib as mlib

RETENTION_DAYS = int(os.environ.get("BRONZE_RETENTION_DAYS", "30"))

CATALOGS = ["lakehouse", "rawlake"]


def discover_tables(spark, catalogs=CATALOGS):
    """[(catalog, ns, table), ...] for every table found via SHOW NAMESPACES /
    SHOW TABLES across `catalogs`, skipping `information_schema` and degrading
    per-catalog / per-namespace on discovery errors (logs `[skip] ...`)."""
    triples = []
    for catalog in catalogs:
        try:
            namespaces = [r.namespace for r in spark.sql(f"SHOW NAMESPACES IN {catalog}").collect()]
        except Exception as e:
            print(f"[skip] {catalog}: {e}")
            continue
        for ns in namespaces:
            if ns == "information_schema":
                continue
            try:
                tables = [r.tableName for r in spark.sql(f"SHOW TABLES IN {catalog}.{ns}").collect()]
            except Exception as e:
                print(f"[skip] {catalog}.{ns}: {e}")
                continue
            for t in tables:
                triples.append((catalog, ns, t))
    return triples


def maintain_table(spark, catalog, ns, table, retention_days):
    """Per-table maintenance chain (gc.enabled + bronze-ttl + the 4 CALLs).
    Isolated try/except: one table's failure never aborts the others, whether
    run sequentially or (as in main()) concurrently across a ThreadPoolExecutor."""
    fq = f"{catalog}.{ns}.{table}"
    print(f"[maintain] {fq}")
    try:
        # Nessie REST serves every table with `gc.enabled=false`
        # (multi-branch safety: physical file deletion could corrupt
        # OTHER refs sharing those files), which makes
        # expire_snapshots/remove_orphan_files hard-fail with
        # "Cannot expire snapshots: GC is disabled" (live kind-e2e
        # finding, 2026-07-22). This platform only ever writes the
        # default `main` branch — no branch/tag workflows — so
        # table-level GC is safe and this maintenance chain IS the
        # designed cleanup path. A deployment that adopts Nessie
        # branches/tags must drop this override and switch to
        # Nessie's own GC tool instead.
        spark.sql(f"ALTER TABLE {fq} SET TBLPROPERTIES ('gc.enabled'='true')")
        if catalog == "rawlake" and mlib.is_bronze_namespace(ns):
            spark.sql(mlib.bronze_ttl_delete_sql(catalog, ns, table, retention_days))
        # Silver (MERGE-on-read) accumulates equality/position delete
        # files; fold them back so Trino stays fast. delete-file-threshold
        # forces rewrite of data files carrying deletes.
        spark.sql(
            f"CALL {catalog}.system.rewrite_data_files("
            f"table => '{ns}.{table}', "
            f"options => map('delete-file-threshold','1'))"
        )
        spark.sql(f"CALL {catalog}.system.rewrite_position_delete_files(table => '{ns}.{table}')")
        spark.sql(f"CALL {catalog}.system.expire_snapshots(table => '{ns}.{table}')")
        spark.sql(f"CALL {catalog}.system.remove_orphan_files(table => '{ns}.{table}')")
    except Exception as e:
        print(f"[error] {catalog}.{ns}.{table}: {e}")


def main(spark):
    """Discover every (catalog, ns, table) triple, then run maintain_table for
    each concurrently over a ThreadPoolExecutor bounded by
    MAINTENANCE_MAX_PARALLEL (default 4) so compaction scales with pipeline
    count rather than running fully sequentially."""
    triples = discover_tables(spark)
    max_workers = int(os.environ.get("MAINTENANCE_MAX_PARALLEL", "4"))
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = [
            pool.submit(maintain_table, spark, catalog, ns, table, RETENTION_DAYS)
            for catalog, ns, table in triples
        ]
        for f in futures:
            f.result()
    spark.stop()


if __name__ == "__main__":
    from pyspark.sql import SparkSession  # lazily imported: tests need no Spark (see merge_cdc.py)

    spark = SparkSession.builder.appName("IcebergMaintenance").getOrCreate()
    main(spark)
