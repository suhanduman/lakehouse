"""merge_cdc — Bronze(_cdc) -> Silver MERGE (ScheduledSparkApplication silver-merge). Spark 4.1 / Iceberg 1.11 / Polaris REST.
Watermark: Silver TBLPROPERTIES 'lakehouse.bronze.snapshot-id' -> artımlı okuma (yalnız append snapshot'ları); yoksa/okunamazsa
tam okuma (snapshot-id=cur). MERGE idempotent (anahtar başına son durum) -> yeniden işleme güvenli. Bronze yoksa (henüz veri gelmedi) atlanır."""
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import merge_lib as ml  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql.utils import AnalysisException  # noqa: E402

WM_PROP = "lakehouse.bronze.snapshot-id"
CATALOG = os.environ.get("LAKEHOUSE_CATALOG", "lakehouse")
PIPELINES = os.environ.get("PIPELINES_FILE", "/opt/job/pipelines.json")


def session(app: str) -> SparkSession:
    b = SparkSession.builder.appName(app)
    cred = os.environ.get("POLARIS_CREDENTIAL")          # Secret -> env; YAML'a girmez (plan P4)
    if cred:
        b = b.config(f"spark.sql.catalog.{CATALOG}.credential", cred)
    return b.getOrCreate()


def fields(spark, table):
    return [(f.name, f.dataType.simpleString()) for f in spark.table(table).schema.fields]


def table_exists(spark, table) -> bool:
    try:
        spark.table(table)
        return True
    except AnalysisException:
        return False


def current_snapshot(spark, table):
    rows = spark.sql(f"SELECT snapshot_id FROM {table}.snapshots ORDER BY committed_at DESC LIMIT 1").collect()
    return int(rows[0][0]) if rows else None


def watermark(spark, silver):
    rows = spark.sql(f"SHOW TBLPROPERTIES {silver} ('{WM_PROP}')").collect()
    val = rows[0]["value"] if rows else None
    return int(val) if val and str(val).isdigit() else None


def load_bronze(spark, bronze, wm, cur):
    """(df, mod). Artımlı: (wm, cur] aralığındaki append snapshot'ları; delete/replace snapshot'ları Iceberg atlar.
    Herhangi bir hata (snapshot expire edilmiş, overwrite vb.) -> cur snapshot'ının tam okuması."""
    if wm is not None:
        try:
            df = spark.read.format("iceberg").option("start-snapshot-id", wm).option("end-snapshot-id", cur).load(bronze)
            df.schema  # plan tetikle (hata varsa burada çıkar)
            return df, "incremental"
        except Exception as e:  # noqa: BLE001
            print(f"[{bronze}] artımlı okuma başarısız ({type(e).__name__}: {str(e)[:160]}) -> tam okuma")
    return spark.read.format("iceberg").option("snapshot-id", cur).load(bronze), "full"


def with_commit_retry(fn, tries=3):
    for i in range(tries):
        try:
            return fn()
        except Exception as e:  # noqa: BLE001
            msg = f"{type(e).__name__}: {e}".lower()
            if i == tries - 1 or not ("commit" in msg or "conflict" in msg):
                raise
            print(f"commit çakışması, {5 * (i + 1)} s sonra tekrar ({i + 1}/{tries})")
            time.sleep(5 * (i + 1))


def run_pipeline(spark, p: ml.Pipeline) -> None:
    bronze = f"{CATALOG}.{p.bronze}"
    silver = f"{CATALOG}.{p.silver or ml.silver_name(p.bronze)}"
    if not table_exists(spark, bronze):
        print(f"[{p.bronze}] Bronze yok (henüz veri gelmedi) — atlandı")
        return
    cur = current_snapshot(spark, bronze)
    if cur is None:
        print(f"[{p.bronze}] Bronze'da snapshot yok — atlandı")
        return
    cols = ml.silver_columns(fields(spark, bronze), p.casts)
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {silver.rsplit('.', 1)[0]}")
    if not table_exists(spark, silver):
        ddl = ml.create_silver_sql(silver, cols, p.keys, p.write_mode, p.bucket_count)
        print(ddl)
        spark.sql(ddl)
        wm = None
    else:
        for ddl in ml.plan_schema_changes(silver, ml.business_columns(fields(spark, silver)), cols):
            print(ddl)
            spark.sql(ddl)
        wm = watermark(spark, silver)
    if wm == cur:
        print(f"[{p.bronze}] yeni snapshot yok (watermark {cur})")
        return
    df, mode = load_bronze(spark, bronze, wm, cur)
    df.createOrReplaceTempView("bronze_inc")
    spark.sql(ml.dedup_select_sql("bronze_inc", cols, p.keys, p.casts)).createOrReplaceTempView("inc")
    n = spark.table("inc").count()
    with_commit_retry(lambda: spark.sql(ml.merge_sql(silver, "inc", cols, p.keys)))
    spark.sql(f"ALTER TABLE {silver} SET TBLPROPERTIES ('{WM_PROP}'='{cur}')")
    print(f"[{p.bronze}] -> {silver}: {mode}, {n} anahtar, snapshot {wm} -> {cur}")


def main() -> None:
    with open(PIPELINES, encoding="utf-8") as f:
        pipes = ml.parse_pipelines(json.load(f))
    spark = session("silver-merge")
    failed = []
    for p in pipes:
        try:
            run_pipeline(spark, p)
        except Exception as e:  # noqa: BLE001 — bir pipeline diğerlerini engellemesin; sonunda fail-loud
            failed.append(p.bronze)
            print(f"[{p.bronze}] HATA {type(e).__name__}: {e}", file=sys.stderr)
    spark.stop()
    if failed:
        print(f"BAŞARISIZ pipeline'lar: {failed}", file=sys.stderr)
        sys.exit(1)
    print("MERGE_OK")


if __name__ == "__main__":
    main()
