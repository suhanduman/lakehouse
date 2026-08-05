# Internally-built Zeppelin image: a THIN LAYER over the platform's own
# Zeppelin image, adding ONLY the Trino JDBC driver onto the JDBC interpreter
# classpath so the %trino interpreter needs NO runtime Maven download
# (air-gap-safe; avoids the re-download + non-READY churn that Zeppelin
# triggers on every interpreter-settings PUT — which the c1 token-refresher
# performs on each rotation). Trino JDBC version is pinned to match the Trino
# server (trinodb/trino:439).
#
# WHY a thin layer (not FROM stock apache/zeppelin): the platform base bakes
# customizations this chart depends on and that can't be reconstructed here —
# notably the corporate AD CA cert in the JVM truststore (LDAPS login) and the
# /zeppelin filesystem layout the Deployment mounts assume. Rebasing onto
# stock apache/zeppelin would silently drop the AD truststore and break login.
# ${ZEPPELIN_HOME}/interpreter/jdbc resolves correctly whatever layout the base
# uses. If the platform base already ships trino-jdbc this simply overwrites it
# with the version-matched jar (harmless).
#
# ZEPPELIN_BASE MUST be the platform's Zeppelin image. Build in CI, which can
# reach both the platform registry and Maven Central (the running cluster
# cannot):
#   docker build --build-arg ZEPPELIN_BASE=<registry>/zeppelin:<tag> images/zeppelin
ARG ZEPPELIN_BASE=registry.apps.ocp.example.com/lakehouse/zeppelin:1.0
FROM ${ZEPPELIN_BASE}
ARG TRINO_JDBC_VERSION=439
USER root
RUN curl -fsSL -o "${ZEPPELIN_HOME}/interpreter/jdbc/trino-jdbc-${TRINO_JDBC_VERSION}.jar" \
      "https://repo1.maven.org/maven2/io/trino/trino-jdbc/${TRINO_JDBC_VERSION}/trino-jdbc-${TRINO_JDBC_VERSION}.jar" \
    && chmod 0644 "${ZEPPELIN_HOME}/interpreter/jdbc/trino-jdbc-${TRINO_JDBC_VERSION}.jar"
USER 1000
