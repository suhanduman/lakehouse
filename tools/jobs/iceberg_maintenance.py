# Iceberg bakım job'u — DİNAMİK tarama: hardcoded tablo listesi YOK.
# lakehouse ve rawlake kataloglarındaki tüm namespace/tablo çiftleri
# SHOW NAMESPACES / SHOW TABLES ile keşfedilir; her tablo için sırayla
# rewrite_data_files (compaction) + expire_snapshots + remove_orphan_files
# çağırılır. Yeni bir tablo eklendiğinde bu script veya manifest değişmez.
#
# ScheduledSparkApplication `iceberg-maintenance` tarafından saatlik
# tetiklenir (bkz. chart/templates/05-spark-operator.yaml).
import os

from pyspark.sql import SparkSession

try:
    from tools.jobs import maintenance_lib as mlib          # repo-root / pytest layout
except ImportError:                                          # flat /opt/spark/jobs/ layout
    # The spark-py image bakes iceberg_maintenance.py and maintenance_lib.py as flat
    # siblings into /opt/spark/jobs/ (no `tools` package at runtime) — see merge_cdc.py.
    import maintenance_lib as mlib

spark = SparkSession.builder.appName("IcebergMaintenance").getOrCreate()

RETENTION_DAYS = int(os.environ.get("BRONZE_RETENTION_DAYS", "30"))

CATALOGS = ["lakehouse", "rawlake"]

for catalog in CATALOGS:
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
            fq = f"{catalog}.{ns}.{t}"
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
                    spark.sql(mlib.bronze_ttl_delete_sql(catalog, ns, t, RETENTION_DAYS))
                # Silver (MERGE-on-read) accumulates equality/position delete
                # files; fold them back so Trino stays fast. delete-file-threshold
                # forces rewrite of data files carrying deletes.
                spark.sql(
                    f"CALL {catalog}.system.rewrite_data_files("
                    f"table => '{ns}.{t}', "
                    f"options => map('delete-file-threshold','1'))"
                )
                spark.sql(f"CALL {catalog}.system.rewrite_position_delete_files(table => '{ns}.{t}')")
                spark.sql(f"CALL {catalog}.system.expire_snapshots(table => '{ns}.{t}')")
                spark.sql(f"CALL {catalog}.system.remove_orphan_files(table => '{ns}.{t}')")
            except Exception as e:
                print(f"[error] {fq}: {e}")

spark.stop()
