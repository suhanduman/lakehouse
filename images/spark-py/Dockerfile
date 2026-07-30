# images/spark-py/Dockerfile
# First-party Spark image for the platform's SparkApplications
# (iceberg-maintenance, silver-merge, s3-register-table). Stock Spark base +
# job code baked via local:// (unchanged) + all Iceberg/AWS/Kafka/Avro jars
# BAKED at build time.
#
# WHY: chart spark-defaults previously set `spark.jars.packages`, which makes
# Spark resolve these jars from Maven via Ivy on EVERY job start — a runtime
# network dependency (fails air-gapped, slow cold-start, a day-2 risk). Baking
# them makes jobs reproducible + offline-by-default. The coordinate set below
# started IDENTICAL to the old spark.jars.packages (runtime behavior
# unchanged); spark-avro was added on top for s3_register_table.py's
# `SELECT * FROM avro.\`...\`` full-refresh CTAS path (parquet/json are
# built into Spark, Avro is not).
#
# AWS SDK v2 is baked via `org.apache.iceberg:iceberg-aws-bundle`
# (version-matched to iceberg-spark-runtime), NOT a standalone
# `software.amazon.awssdk:bundle` coordinate. A standalone AWS SDK bundle
# resolves its own (newer) SDK v2 version, which skews against the SDK v2
# version iceberg-aws was compiled against and breaks S3FileIO writes at
# runtime with `NoSuchMethodError: ...IoUtils.closeQuietly(...)`.
# iceberg-aws-bundle ships the exact SDK v2 classes iceberg-aws expects.
#
# All versions MUST match chart .Values.versions.* (single source of truth).
# Build (from repo root):
#   docker build -f images/spark-py/Dockerfile \
#     --build-arg SPARK_VERSION=3.5.1 \
#     --build-arg ICEBERG_SPARK_RUNTIME=1.9.0 \
#     --build-arg HADOOP_AWS=<versions.hadoopAws> \
#     -t <registry>/spark-py:1.0 .
ARG SPARK_VERSION=3.5.1
FROM apache/spark:${SPARK_VERSION}-scala2.12-java17-python3-ubuntu
ARG SPARK_VERSION
ARG ICEBERG_SPARK_RUNTIME=1.9.0
ARG HADOOP_AWS=3.3.4
USER root

# Build-time Ivy resolution -> copy every resolved jar into $SPARK_HOME/jars,
# so at runtime the classpath is complete with NO --packages / Ivy download.
# We submit a real (no-op) job so spark-submit actually reaches --packages
# resolution: `--version` would exit during arg parsing BEFORE resolution
# ever runs, silently baking zero jars. A loud post-check below fails the
# build if resolution didn't actually produce jars, so that regression can
# never again pass silently.
RUN set -eux; \
    PKGS="org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:${ICEBERG_SPARK_RUNTIME},\
org.apache.iceberg:iceberg-aws-bundle:${ICEBERG_SPARK_RUNTIME},\
org.apache.spark:spark-sql-kafka-0-10_2.12:${SPARK_VERSION},\
org.apache.spark:spark-avro_2.12:${SPARK_VERSION},\
org.apache.hadoop:hadoop-aws:${HADOOP_AWS}"; \
    echo "print('spark-py: baking dependency jars')" > /tmp/bake.py; \
    "$SPARK_HOME/bin/spark-submit" --packages "$PKGS" \
        --conf spark.jars.ivy=/tmp/.ivy /tmp/bake.py; \
    find /tmp/.ivy -name '*.jar' -exec cp -n {} "$SPARK_HOME/jars/" \; ; \
    test -n "$(find "$SPARK_HOME/jars" -name 'iceberg-spark-runtime*')" \
      || { echo "ERROR: dependency bake resolved no jars" >&2; exit 1; }; \
    test -n "$(find "$SPARK_HOME/jars" -name 'iceberg-aws-bundle*')" \
      || { echo "ERROR: iceberg-aws-bundle not baked" >&2; exit 1; }; \
    rm -rf /tmp/.ivy /tmp/bake.py

# Job code baked (local:// path the chart references). Kept in the image (not a
# ConfigMap) — it is already baked & working, and sourcing it from the chart
# would create a drift-prone second copy of merge_cdc.py/merge_lib.py.
# Only top-level job scripts are copied (not tests/ or .DS_Store).
COPY tools/jobs/*.py /opt/spark/jobs/
RUN chmod -R a+r /opt/spark/jobs
USER 1001
