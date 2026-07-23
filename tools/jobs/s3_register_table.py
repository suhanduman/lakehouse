#!/usr/bin/env python3
"""S3 file-set -> Iceberg table (full-refresh). Reads files already in the
platform object store at s3a://<bucket>/<prefix> and (re)derives an Iceberg
table <target> in the rawlake warehouse via CREATE OR REPLACE TABLE ... AS
SELECT * FROM <format>.`s3a://<bucket>/<prefix>`. Idempotent; re-run picks up
new files under the prefix. Invoked by the Console-rendered SparkApplication
(render_service._render_s3_register)."""
from __future__ import annotations

import argparse

VALID_FORMATS = ("parquet", "json", "avro")


def build_namespace_sql(target: str) -> str:
    catalog, ns, _table = target.split(".", 2)
    return f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{ns}"


def build_ctas_sql(target: str, fmt: str, bucket: str, prefix: str) -> str:
    if fmt not in VALID_FORMATS:
        raise ValueError(f"unsupported format {fmt!r}; expected one of {VALID_FORMATS}")
    return (f"CREATE OR REPLACE TABLE {target} USING iceberg AS "
            f"SELECT * FROM {fmt}.`s3a://{bucket}/{prefix}`")


def parse_args(argv=None):
    p = argparse.ArgumentParser(description="Register S3 files as an Iceberg table (full-refresh).")
    for a in ("bucket", "prefix", "format", "target"):
        p.add_argument(f"--{a}", required=True)
    return p.parse_args(argv)


def main() -> int:
    from pyspark.sql import SparkSession
    args = parse_args()
    spark = SparkSession.builder.appName(f"s3-register-{args.target}").getOrCreate()
    try:
        spark.sql(build_namespace_sql(args.target))
        spark.sql(build_ctas_sql(args.target, args.format, args.bucket, args.prefix))
        n = spark.sql(f"SELECT count(*) c FROM {args.target}").collect()[0]["c"]
        print(f"[ok] {args.target}: {n} rows from s3a://{args.bucket}/{args.prefix} ({args.format})")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    import sys
    sys.exit(main())
