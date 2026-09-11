"""iceberg_maintenance — spec §6 bakım (D(f) deklaratif): 3 ScheduledSparkApplication aynı dosyayı farklı --mode ile koşturur.
  --mode position-deletes   Silver: rewrite_position_delete_files (MoR delete dosyalarını katla)         saatlik
  --mode compact            Silver+Bronze: rewrite_data_files(delete-file-threshold=5, remove-dangling-deletes, partial-progress)  6 saat
  --mode expire-orphan-ttl  Silver+Bronze: expire_snapshots(--snapshot-days) + remove_orphan_files(--orphan-days); Bronze: DELETE _cdc.ts < now-ttl  günlük
Tablolar pipelines.json'dan (bronze + türetilen silver). Var olmayan tablo atlanır (henüz veri gelmemiş olabilir)."""
import argparse
import json
import os
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import merge_lib as ml  # noqa: E402
from merge_cdc import CATALOG, PIPELINES, session, table_exists  # noqa: E402


def ts_days_ago(days: int) -> str:
    return (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S+00:00")


def call(spark, proc: str, table: str, extra: str = "") -> None:
    sql = f"CALL {CATALOG}.system.{proc}(table => '{table}'{extra})"
    print(sql)
    for r in spark.sql(sql).collect():
        print("  ", r.asDict())


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", required=True, choices=["position-deletes", "compact", "expire-orphan-ttl"])
    ap.add_argument("--snapshot-days", type=int, default=7)
    ap.add_argument("--orphan-days", type=int, default=3)
    ap.add_argument("--bronze-ttl-days", type=int, default=30)
    a = ap.parse_args()
    with open(PIPELINES, encoding="utf-8") as f:
        pipes = ml.parse_pipelines(json.load(f))
    silver = [p.silver or ml.silver_name(p.bronze) for p in pipes]
    bronze = [p.bronze for p in pipes]
    spark = session(f"maint-{a.mode}")
    failed = []
    for t in (silver if a.mode == "position-deletes" else silver + bronze):
        if not table_exists(spark, f"{CATALOG}.{t}"):
            print(f"[{t}] yok — atlandı")
            continue
        try:
            if a.mode == "position-deletes":
                call(spark, "rewrite_position_delete_files", t)
            elif a.mode == "compact":
                call(spark, "rewrite_data_files", t,
                     ", options => map('delete-file-threshold','5','remove-dangling-deletes','true','partial-progress.enabled','true')")
            else:
                call(spark, "expire_snapshots", t, f", older_than => TIMESTAMP '{ts_days_ago(a.snapshot_days)}', retain_last => 1")
                # prefix_listing: Hadoop FS yerine FileIO (S3FileIO) ile listeler -> resmi Spark imajında s3a yok (F2 e2e: "No FileSystem for scheme s3")
                call(spark, "remove_orphan_files", t, f", older_than => TIMESTAMP '{ts_days_ago(a.orphan_days)}', prefix_listing => true")
                if t in bronze:
                    sql = f"DELETE FROM {CATALOG}.{t} WHERE _cdc.ts < current_timestamp() - INTERVAL {a.bronze_ttl_days} DAYS"
                    print(sql)
                    spark.sql(sql)
        except Exception as e:  # noqa: BLE001
            failed.append(t)
            print(f"[{t}] HATA {type(e).__name__}: {e}", file=sys.stderr)
    spark.stop()
    if failed:
        print(f"BAŞARISIZ: {failed}", file=sys.stderr)
        sys.exit(1)
    print("MAINT_OK")


if __name__ == "__main__":
    main()
