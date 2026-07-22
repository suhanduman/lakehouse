# Lakehouse — pre-built Kafka Connect image (prod-grade sink).
#
# WHY THIS EXISTS:
# Apache Iceberg does NOT publish a downloadable kafka-connect-runtime binary
# (no Maven Central jar/zip, no GitHub release asset — it is built from source
# via gradle), and the archived Tabular/Databricks fork that used to publish a
# zip is unmaintained. So Strimzi `spec.build` (which downloads plugin URLs at
# cluster-build time) cannot obtain the Iceberg sink. The reproducible,
# air-gap-friendly pattern is a pre-built versioned image consumed via
# `KafkaConnect.spec.image` — built ONCE here, pushed to the registry, pinned
# by version.
#
# Build (from repo root):
#   docker build -f images/connect/Dockerfile \
#     --build-arg ICEBERG_VERSION=1.9.0 \
#     -t <registry>/connect-lakehouse:1.0 .
# All plugin versions MUST match chart `.Values.versions.*`.

# ---------------------------------------------------------------------------
# Stage 1 — build the Apache Iceberg kafka-connect-runtime ZIP from source.
# Self-contained: uses a JDK17 toolchain image, so the build host needs no JDK.
# ---------------------------------------------------------------------------
# NOTE: the kafka-connect-runtime DISTRIBUTION module (the bundle we need) only
# exists from Iceberg 1.7.x (1.6.x shipped the connector but no runtime dist
# module). The Apache sink is append-only: it lands the raw CDC change-log in
# Bronze and current-state upsert happens downstream in the silver-merge Spark
# job, so no in-sink CDC transform module is needed. Pinned to 1.9.0,
# independent of the query-side Iceberg version.
ARG ICEBERG_VERSION=1.9.0
FROM eclipse-temurin:17-jdk AS iceberg-build
ARG ICEBERG_VERSION
RUN apt-get update && apt-get install -y --no-install-recommends git unzip ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /src
# Pinned to the exact Apache release tag (refs/tags/apache-iceberg-1.9.0).
RUN git clone --depth 1 --branch "apache-iceberg-${ICEBERG_VERSION}" https://github.com/apache/iceberg.git .
# Build the Kafka Connect runtime distribution (skip tests). settings.gradle
# renames the module to `iceberg-kafka-connect-runtime` under parent
# `iceberg-kafka-connect`; `build` produces the distribution zip under
# kafka-connect/kafka-connect-runtime/build/distributions/.
RUN ./gradlew --no-daemon -x test -x integrationTest \
      :iceberg-kafka-connect:iceberg-kafka-connect-runtime:build
# Unpack the NON-hive runtime zip into a flat plugin dir (jars under lib/).
RUN set -eux; \
    zip="$(find . -path '*/build/distributions/iceberg-kafka-connect-runtime-*.zip' ! -name '*hive*' | head -1)"; \
    test -n "$zip"; \
    mkdir -p /plugins/iceberg; \
    unzip -o "$zip" -d /tmp/ice; \
    find /tmp/ice -name '*.jar' -exec cp {} /plugins/iceberg/ \;

# ---------------------------------------------------------------------------
# Stage 2 — assemble the Connect image: Strimzi Kafka base + all plugins baked.
# Base MUST match the running Strimzi operator's Kafka image
# (Strimzi 0.51.0 / Kafka 4.2.0). Other plugins fetched from Maven/GitHub at
# IMAGE-build time (not cluster-build time) — pinned to chart versions.
# ---------------------------------------------------------------------------
FROM quay.io/strimzi/kafka:0.51.0-kafka-4.2.0
ARG DEBEZIUM_VERSION=2.7.3
ARG AIVEN_JDBC_VERSION=6.10.0
ARG MSSQL_JDBC_VERSION=12.8.1.jre11
ARG PG_JDBC_VERSION=42.7.4
ARG APICURIO_VERSION=2.6.5
USER root
RUN set -eux; \
    P=/opt/kafka/plugins; \
    mkdir -p "$P/debezium-sqlserver" "$P/debezium-postgres" "$P/debezium-mongodb" "$P/aiven-jdbc" "$P/apicurio-avro-converter" "$P/iceberg"; \
    cd "$P/debezium-sqlserver"; \
    curl -fL -o d.zip "https://repo1.maven.org/maven2/io/debezium/debezium-connector-sqlserver/${DEBEZIUM_VERSION}.Final/debezium-connector-sqlserver-${DEBEZIUM_VERSION}.Final-plugin.zip"; unzip -o d.zip; rm d.zip; \
    cd "$P/debezium-postgres"; \
    curl -fL -o d.zip "https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/${DEBEZIUM_VERSION}.Final/debezium-connector-postgres-${DEBEZIUM_VERSION}.Final-plugin.zip"; unzip -o d.zip; rm d.zip; \
    cd "$P/debezium-mongodb"; \
    curl -fL -o d.zip "https://repo1.maven.org/maven2/io/debezium/debezium-connector-mongodb/${DEBEZIUM_VERSION}.Final/debezium-connector-mongodb-${DEBEZIUM_VERSION}.Final-plugin.zip"; unzip -o d.zip; rm d.zip; \
    cd "$P/aiven-jdbc"; \
    curl -fL -o a.zip "https://github.com/Aiven-Open/jdbc-connector-for-apache-kafka/releases/download/v${AIVEN_JDBC_VERSION}/jdbc-connector-for-apache-kafka-${AIVEN_JDBC_VERSION}.zip"; unzip -o a.zip; rm a.zip; \
    curl -fL -o "mssql-jdbc.jar" "https://repo1.maven.org/maven2/com/microsoft/sqlserver/mssql-jdbc/${MSSQL_JDBC_VERSION}/mssql-jdbc-${MSSQL_JDBC_VERSION}.jar"; \
    curl -fL -o "postgresql.jar" "https://repo1.maven.org/maven2/org/postgresql/postgresql/${PG_JDBC_VERSION}/postgresql-${PG_JDBC_VERSION}.jar"; \
    cd "$P/apicurio-avro-converter"; \
    curl -fL -o c.zip "https://repo1.maven.org/maven2/io/apicurio/apicurio-registry-distro-connect-converter/${APICURIO_VERSION}.Final/apicurio-registry-distro-connect-converter-${APICURIO_VERSION}.Final.zip"; unzip -o c.zip; rm c.zip
# Iceberg runtime built in stage 1.
COPY --from=iceberg-build /plugins/iceberg /opt/kafka/plugins/iceberg

# (Opsiyonel) S3 endpoint'in Internal-CA'sını imaj JVM truststore'una göm.
# Strimzi KafkaConnect init-container DESTEKLEMEDİĞİNDEN (Trino/Nessie'deki
# runtime keytool init-container yaklaşımı Connect'e uygulanamaz), S3 endpoint
# CA'sı BUILD anında eklenir. Boşsa no-op. Prod hardening — bkz. gitops/README
# "S3 internal-CA truststore" runbook. Kullanım (CA env'e özgü):
#   docker build --build-arg S3_CA_CERT_B64="$(base64 -w0 s3-root-ca.crt)" \
#     -f images/connect/Dockerfile -t <registry>/connect-lakehouse:<tag> images/connect
ARG S3_CA_CERT_B64=""
RUN set -eux; \
    if [ -n "$S3_CA_CERT_B64" ]; then \
      echo "$S3_CA_CERT_B64" | base64 -d > /tmp/s3-ca.crt; \
      CACERTS="${JAVA_HOME:-/opt/java/openjdk}/lib/security/cacerts"; \
      keytool -importcert -noprompt -alias s3-endpoint -file /tmp/s3-ca.crt \
        -keystore "$CACERTS" -storepass changeit; \
      rm -f /tmp/s3-ca.crt; \
    fi
USER 1001
