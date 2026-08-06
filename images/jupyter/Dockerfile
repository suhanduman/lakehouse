# Internally-built single-user notebook image (hand-rolled JupyterHub
# per-user sandbox — the image KubeSpawner launches per user;
# chart/templates/23-jupyterhub.yaml, NOT the Hub itself — see
# images/jupyterhub/Dockerfile for that). Batteries-included base
# (quay.io/jupyter/pyspark-notebook: pandas/numpy/scipy/scikit-learn/
# matplotlib + PySpark) + this platform's lakehouse client stack
# (pyiceberg, trino) + the Spark-on-k8s Iceberg/AWS jars so notebook Spark
# sessions can read/write Nessie-cataloged Iceberg tables on S3 with NO
# runtime Maven/Ivy download (air-gap-safe, same rationale as
# images/spark-py/Dockerfile).
#
# VERSION MATRIX (single source of truth: chart .Values.versions.* — keep
# this file's ARGs in sync with `versions.sparkVersion` /
# `versions.icebergSparkRuntime` there):
#   quay.io/jupyter/pyspark-notebook:spark-3.5.1
#     -- pinned to the `spark-X.Y.Z` build tag matching this platform's
#        Spark minor/patch (`versions.sparkVersion` = 3.5.1); multi-arch
#        manifest list (amd64 + arm64), so no --platform needed even on an
#        arm64 build host. Ships PySpark 3.5.1 + pandas/numpy/scipy/
#        scikit-learn/matplotlib/JupyterLab out of the box.
#   pyiceberg[s3fs,pyarrow]  0.10.0  -- Python Iceberg client (REST/Nessie
#        catalog + S3 + Arrow reads). Same 0.10.x line as
#        images/iceberg-tools/Dockerfile's pyiceberg pin (single Python-side
#        Iceberg client version across the platform).
#   trino[sqlalchemy]        0.338.0 -- Trino DB-API + SQLAlchemy dialect,
#        same exact pin as images/superset/Dockerfile (one Trino client
#        version everywhere; matches trinoImage trinodb/trino:476).
#   iceberg-spark-runtime-3.5_2.12  1.9.0  -- Spark<->Iceberg (Nessie REST
#        catalog client bundled in this artifact)
#   iceberg-aws-bundle              1.9.0  -- version-matched AWS SDK v2
#        S3FileIO (must be the SAME version as iceberg-spark-runtime above —
#        see images/spark-py/Dockerfile's comment on the
#        NoSuchMethodError/IoUtils.closeQuietly class-skew failure mode this
#        avoids). Matches `versions.icebergSparkRuntime` = 1.9.0 (Iceberg
#        1.9.0 / Nessie 0.104.3 / Spark 3.5.1 — same matrix as spark-py).
#
# Build (from repo root):
#   podman build -t <registry>/jupyter:<tag> images/jupyter
FROM quay.io/jupyter/pyspark-notebook:spark-3.5.1

ARG PYICEBERG_VERSION=0.10.0
ARG TRINO_VERSION=0.338.0
ARG ICEBERG_SPARK_RUNTIME=1.9.0

# pyiceberg/trino installed as $NB_USER (jovyan) into the base's conda env —
# /opt/conda/lib/python3.11/site-packages is jovyan-owned/group-writable in
# this base image, no root needed.
RUN pip install --no-cache-dir \
      "pyiceberg[s3fs,pyarrow]==${PYICEBERG_VERSION}" \
      "trino[sqlalchemy]==${TRINO_VERSION}"

# $SPARK_HOME/jars is root-owned (0755) in the base image -> root needed
# ONLY for this step, then drop back to $NB_USER (the base's own non-root
# convention; matches jovyan's existing UID/GID, not a new user).
USER root
RUN set -eux; \
    curl -fL -o "$SPARK_HOME/jars/iceberg-spark-runtime-3.5_2.12-${ICEBERG_SPARK_RUNTIME}.jar" \
      "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_SPARK_RUNTIME}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_SPARK_RUNTIME}.jar"; \
    curl -fL -o "$SPARK_HOME/jars/iceberg-aws-bundle-${ICEBERG_SPARK_RUNTIME}.jar" \
      "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_SPARK_RUNTIME}/iceberg-aws-bundle-${ICEBERG_SPARK_RUNTIME}.jar"; \
    chmod 0644 "$SPARK_HOME/jars/iceberg-spark-runtime-3.5_2.12-${ICEBERG_SPARK_RUNTIME}.jar" \
      "$SPARK_HOME/jars/iceberg-aws-bundle-${ICEBERG_SPARK_RUNTIME}.jar"

# Back to the base's own non-root user (jovyan, $NB_UID=1000) for runtime.
# $HOME (/home/jovyan) is already jovyan-owned/writable in the base image —
# `pip install --user` persistence (e.g. into a mounted PVC at $HOME) works
# unmodified; NOT re-chowned here so a later per-user PVC mount's ownership
# is authoritative, not baked-image ownership.
USER ${NB_UID}
WORKDIR ${HOME}

# Lakehouse helper module + starter notebook, baked at a fixed path (NOT
# $HOME -- a per-user PVC mount at $HOME shadows anything baked there, see
# Task-5 postStart hook which copies starter.ipynb into $HOME at pod start).
# PYTHONPATH makes `import lakehouse_nb` work from any notebook/kernel.
COPY lakehouse_nb.py /opt/lakehouse/lakehouse_nb.py
COPY starter.ipynb   /opt/lakehouse/starter.ipynb
ENV PYTHONPATH=/opt/lakehouse:${PYTHONPATH}
