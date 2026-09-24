"""s3_register_example — tek seferlik: S3'teki düz CSV/Parquet dosyalarını doğrudan bir Iceberg tablosuna kaydeder
(spec dışı backlog örneği — kaynak sistemi olmayan tek seferlik toplu yükleme). e2e'ye eklenmez; docs/50-isletme/yeni-spark-uygulamasi.md
Ek A bunu bir SparkApplication'a nasıl çevireceğini anlatır (mevcut bir ScheduledSparkApplication şablonundan türetme).
--source s3://bucket/prefix/ --format csv|parquet --table ns.tbl [--header true|false]"""
import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from merge_cdc import CATALOG, session  # noqa: E402


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", required=True)
    ap.add_argument("--format", required=True, choices=["csv", "parquet"])
    ap.add_argument("--table", required=True)
    ap.add_argument("--header", default="true")
    a = ap.parse_args()
    spark = session(f"s3-register-{a.table}")
    df = spark.read.format(a.format).option("header", a.header).load(a.source)
    ns = a.table.rsplit(".", 1)[0]
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{ns}")
    df.writeTo(f"{CATALOG}.{a.table}").using("iceberg").createOrReplace()
    n = spark.table(f"{CATALOG}.{a.table}").count()
    spark.stop()
    print(f"S3_REGISTER_OK {a.source} -> {CATALOG}.{a.table} ({n} satır)")


if __name__ == "__main__":
    main()
