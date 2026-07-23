"""form -> CR: pure functions turning a `SourceSpec` into the same
KafkaConnector/KafkaTopic dicts (+ namespace DDL) that sub-project A's
`tools/templates/source-*.yaml` templates render by hand.

No k8s/network calls here — every function is a pure dict/str builder so it
can be unit-tested without a cluster. The API layer (later task) is
responsible for actually `oc apply`-ing what these functions return.

Template parity (sub-project A, read for this task):
  - tools/templates/source-cdc-relational.yaml   (+ dbz-mssql-students.yaml)
  - tools/templates/source-scheduled-jdbc.yaml    (+ jdbc-mssql-scheduled.yaml)
  - tools/templates/source-cdc-mongo.yaml         (+ dbz-mongo-lms.yaml)
  - tools/templates/kafkatopic.yaml
"""

from __future__ import annotations

import re
from typing import Optional, Tuple

from app.models import SourceSpec
from app import source_types

APICURIO_URL = "http://apicurio-registry:8080/apis/registry/v2"
NESSIE_URI = "http://nessie:19120/iceberg/"   # in-cluster Nessie Iceberg REST (svc `nessie`)
SCHEMA_HISTORY_BOOTSTRAP = "kafka-kafka-bootstrap:9093"
DEFAULT_POLL_MS = "3600000"
# Apicurio schema auto-register for every rendered Avro connector. Prod-safe
# default: "false" — uncontrolled schema auto-registration is an anti-pattern
# in prod (an unreviewed schema change would silently register a new/
# incompatible version and can break downstream consumers); prod schemas are
# deployed via CI/CD with backward-compat checks run against Apicurio BEFORE
# rollout, so an unknown/unregistered schema at connector-start time should
# fail fast instead. This module has no chart-values access (unlike
# chart/templates/13-connectors.yaml, which reads
# `.Values.connectors.schemaAutoRegister`), so this constant IS the contract
# here — a dev/test deployment that wants auto-register=true overrides the
# generated connector config (e.g. via the Console's edit-before-apply step)
# after render_connector() returns it.
SCHEMA_AUTO_REGISTER = "false"
# DLQ topic replication factor for connectors that render one (mirrors
# chart/templates/13-connectors.yaml's `errors.deadletterqueue.topic.
# replication.factor`). Prod default: "3" (3+ broker cluster). A single-
# broker/dev deployment (e.g. microk8s-ingest) must override the generated
# config down to "1", same as `kafkaConnect.internalTopicReplicationFactor`
# does chart-side — Connect auto-creates the DLQ topic with this RF, and
# RF=3 fails on a <3-broker cluster ("Could not initialize dead letter
# queue").
DLQ_REPLICATION_FACTOR = "3"


# --------------------------------------------------------------------------
# Small pure helpers
# --------------------------------------------------------------------------

def bucket_name(target_ns: str) -> str:
    """`src-<ns>` with `_` -> `-` (e.g. mssql_ogrenci -> src-mssql-ogrenci)."""
    return f"src-{target_ns.replace('_', '-')}"


def _cred(source: str, key: str) -> str:
    """DirectoryConfigProvider placeholder, matching sub-project A's actual
    templates (colon form: directory path, then `:`, then the filename==secret key):
    `${directory:/mnt/external-configuration/<source>:<key>}`.
    See tools/templates/source-cdc-relational.yaml.
    """
    return f"${{directory:/mnt/external-configuration/{source}:{key}}}"


def _safe_ident(value: str) -> str:
    """Sanitize a source name for use inside a Postgres slot/publication name
    (Debezium/Postgres identifiers only allow [a-z0-9_])."""
    return re.sub(r"[^0-9a-zA-Z]+", "_", value).lower()


def _split_schema_table(table: str) -> Tuple[Optional[str], str]:
    """'dbo.students' -> ('dbo', 'students'); 'enrollments' -> (None, 'enrollments')."""
    if "." in table:
        schema, name = table.split(".", 1)
        return schema, name
    return None, table


def _jdbc_topic_parts(spec: SourceSpec) -> Tuple[str, str]:
    """(topic.prefix, table.whitelist) for the scheduled-JDBC connector, matching
    A's jdbc-mssql-scheduled.yaml: topic.prefix="jdbc.mssql1.dbo.", table.whitelist="courses".
    """
    schema, name = _split_schema_table(spec.table)
    prefix = f"jdbc.{spec.source}." + (f"{schema}." if schema else "")
    return prefix, name


BRONZE_NAMESPACE_SUFFIX = "_raw"


def _route_transform(target_ns: str, target_table: str) -> dict:
    """`_target_table` InsertField$Value route SMT. Routes to the BRONZE namespace
    (<ns>_raw) — the append-only sink lands the CDC change-log there; the
    silver-merge Spark job MERGEs Bronze -> Silver (<ns>)."""
    return {
        "transforms.route.type": "org.apache.kafka.connect.transforms.InsertField$Value",
        "transforms.route.static.field": "_target_table",
        "transforms.route.static.value": f"{target_ns}{BRONZE_NAMESPACE_SUFFIX}.{target_table}",
    }


def _tsconv() -> dict:
    """Common SMT (every source): coerce __ts_ms to a Connect Timestamp so the
    sink writes an Iceberg timestamp column (day() partition needs
    timestamp) -- __ts_ms is both the MERGE ordering key and the Bronze
    partition key (day(__ts_ms)). `__ts_ms` is the Debezium envelope/
    processing time (≈ ingestion time), populated for every source."""
    return {
        "transforms.tsconv.type": "org.apache.kafka.connect.transforms.TimestampConverter$Value",
        "transforms.tsconv.field": "__ts_ms",
        "transforms.tsconv.target.type": "Timestamp",
    }


def _connector(name: str, klass: str, config: dict) -> dict:
    # NB: no `metadata.namespace` here on purpose — the API layer
    # (`app.services.k8s_service.K8sService._apply`) always applies this body
    # via `create/patch_namespaced_custom_object(namespace=self.namespace, ...)`,
    # i.e. the release/target namespace is supplied by the API call itself,
    # not by the body. Stamping a hardcoded namespace into the body here
    # would cause an API-side namespace mismatch (the body's namespace vs.
    # the URL path's namespace) on any install whose namespace isn't the
    # dev/POC default.
    return {
        "apiVersion": "kafka.strimzi.io/v1beta2",
        "kind": "KafkaConnector",
        "metadata": {
            "name": name,
            "labels": {"strimzi.io/cluster": "connect"},
        },
        "spec": {
            "class": klass,
            "tasksMax": 1,
            "config": config,
        },
    }


# --------------------------------------------------------------------------
# topic_name / render_kafka_topic
# --------------------------------------------------------------------------

def _topic_cdc_relational(spec: SourceSpec) -> str:
    # dbz-mssql-students.yaml: topic.prefix "cdc.<source>" + table.include.list
    # "dbo.students" -> "cdc.<source>.dbo.students".
    return f"cdc.{spec.source}.{spec.table}"


def _topic_cdc_mongo(spec: SourceSpec) -> str:
    return f"mongo.{spec.db}.{spec.table}"


def _topic_scheduled_jdbc(spec: SourceSpec) -> str:
    prefix, name = _jdbc_topic_parts(spec)
    return f"{prefix}{name}"


def _topic_cdc_mysql(spec: SourceSpec) -> str:
    # Debezium MySQL topic: <topic.prefix>.<database>.<table>.
    return f"cdc.{spec.source}.{spec.db}.{spec.table}"


_TOPICS = {
    "cdc-relational": _topic_cdc_relational,
    "cdc-mongo": _topic_cdc_mongo,
    "scheduled-jdbc": _topic_scheduled_jdbc,
    "cdc-mysql": _topic_cdc_mysql,
}


def topic_name(spec: SourceSpec) -> str:
    """The Kafka topic a given source's data lands on, matching each connector
    type's own topic-naming convention in A's concrete examples."""
    descriptor = source_types.get(spec.kind, spec.type)
    fn = _TOPICS.get(descriptor.topic_key)
    if fn is None:
        raise NotImplementedError(
            f"topic_name: no Kafka topic for {descriptor.id} "
            f"(lane={descriptor.lane}); the spark-batch lane bypasses Kafka"
        )
    return fn(spec)


def render_kafka_topic(topic_name: str) -> dict:
    """Matches tools/templates/kafkatopic.yaml field-for-field.

    NB: no `metadata.namespace` here on purpose — see `_connector`'s
    docstring/comment above; the API layer (`K8sService._apply`) supplies the
    release/target namespace at apply time, not this rendered body.
    """
    return {
        "apiVersion": "kafka.strimzi.io/v1beta2",
        "kind": "KafkaTopic",
        "metadata": {
            "name": topic_name,
            "labels": {"strimzi.io/cluster": "kafka"},
        },
        "spec": {
            "partitions": 6,
            "replicas": 3,
            "config": {"retention.ms": "604800000", "cleanup.policy": "delete"},
        },
    }


# --------------------------------------------------------------------------
# render_namespace_ddl
# --------------------------------------------------------------------------

def render_namespace_ddl(target_ns: str, bucket: str) -> str:
    return (
        f"CREATE NAMESPACE IF NOT EXISTS lakehouse.{target_ns} "
        f"WITH (location='s3://{bucket}/warehouse')"
    )


# --------------------------------------------------------------------------
# render_connector — dispatch + per-(kind,type) builders
# --------------------------------------------------------------------------

def _schema_history_client_config(role: str) -> dict:
    """producer/consumer SASL_SSL+SCRAM-SHA-512 config for Debezium's schema-history
    internal client, matching source-cdc-relational.yaml / dbz-mssql-students.yaml
    (only needed by SQL Server CDC — Postgres/Mongo don't use this mechanism)."""
    prefix = f"schema.history.internal.{role}"
    jaas_password = _cred("debezium-src", "password")
    return {
        f"{prefix}.security.protocol": "SASL_SSL",
        f"{prefix}.sasl.mechanism": "SCRAM-SHA-512",
        f"{prefix}.sasl.jaas.config": (
            "org.apache.kafka.common.security.scram.ScramLoginModule required "
            f'username="debezium-src" password="{jaas_password}";'
        ),
        f"{prefix}.ssl.truststore.location": "/mnt/external-configuration/kafka-ca/ca.p12",
        f"{prefix}.ssl.truststore.password": _cred("kafka-ca", "ca.password"),
        f"{prefix}.ssl.truststore.type": "PKCS12",
    }


def _render_cdc_relational(spec: SourceSpec) -> dict:
    """cdc + mssql/pg -> Debezium source connector. Mirrors
    tools/templates/source-cdc-relational.yaml (mssql branch is a direct
    field-for-field match; pg branch follows the same shape using the equivalent
    Debezium PostgreSQL connector config keys — A has no pg-specific template).
    The pg branch maps to a wal_level=logical + replication slot + publication
    prerequisite on the Postgres side.
    """
    tname = topic_name(spec)
    prefix = f"cdc.{spec.source}"

    config: dict = {
        "database.hostname": spec.db_host,
        "database.user": _cred(spec.source, "user"),
        "database.password": _cred(spec.source, "pass"),
        "topic.prefix": prefix,
        "table.include.list": spec.table,
    }

    if spec.type == "mssql":
        klass = "io.debezium.connector.sqlserver.SqlServerConnector"
        config["database.port"] = "1433"
        config["database.names"] = spec.db
        config["database.encrypt"] = "true"

        # Schema-history block — matches source-cdc-relational.yaml
        # field-for-field. This is a Debezium SqlServer/MySQL/Oracle-only
        # mechanism (it tracks DDL history in a separate internal Kafka
        # topic); the Debezium Postgres connector has no equivalent — it
        # tracks position via its logical replication slot instead (see the
        # `slot.name`/`publication.name` config in the pg branch below), so
        # these keys must NOT be emitted for pg.
        config["schema.history.internal.kafka.bootstrap.servers"] = SCHEMA_HISTORY_BOOTSTRAP
        config["schema.history.internal.kafka.topic"] = f"schema-history.{spec.source}"
        config.update(_schema_history_client_config("producer"))
        config.update(_schema_history_client_config("consumer"))
    else:  # pg
        klass = "io.debezium.connector.postgresql.PostgresConnector"
        config["database.port"] = "5432"
        config["database.dbname"] = spec.db
        slot = _safe_ident(spec.source)
        config["plugin.name"] = "pgoutput"
        config["slot.name"] = f"debezium_{slot}"
        config["publication.name"] = f"dbz_{slot}_pub"

    config.update({
        "key.converter": "io.apicurio.registry.utils.converter.AvroConverter",
        "value.converter": "io.apicurio.registry.utils.converter.AvroConverter",
        "key.converter.apicurio.registry.url": APICURIO_URL,
        "value.converter.apicurio.registry.url": APICURIO_URL,
        "key.converter.apicurio.registry.auto-register": SCHEMA_AUTO_REGISTER,
        "value.converter.apicurio.registry.auto-register": SCHEMA_AUTO_REGISTER,
        "transforms": "unwrap,route,tsconv",
        "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
        "transforms.unwrap.delete.handling.mode": "rewrite",
        "transforms.unwrap.drop.tombstones": "false",
        # CDC metadata for the downstream silver-merge job: __op,__ts_ms,__lsn
        # (+__deleted from delete.handling.mode=rewrite). pg exposes source.lsn;
        # SQL Server exposes source.change_lsn.
        "transforms.unwrap.add.fields": (
            "op,ts_ms,source.change_lsn:lsn" if spec.type == "mssql"
            else "op,ts_ms,source.lsn:lsn"
        ),
    })
    config.update(_route_transform(spec.target_ns, spec.target_table))
    config.update(_tsconv())
    config.update({
        "errors.tolerance": "all",
        "errors.deadletterqueue.topic.name": f"{tname}.dlq",
        "errors.deadletterqueue.context.headers.enable": "true",
    })

    _, table_name = _split_schema_table(spec.table)
    name = f"dbz-{spec.source}-{table_name}"
    return _connector(name, klass, config)


def _mysql_server_id(source: str) -> str:
    # server.id must be cluster-unique and numeric. Derive a stable value from
    # the source name (zlib.crc32 is deterministic) so re-rendering a source is
    # idempotent and two DIFFERENT sources on one MySQL cluster stay distinct.
    import zlib
    return str(1000 + (zlib.crc32(source.encode()) % 4_000_000))


def _render_cdc_mysql(spec: SourceSpec) -> dict:
    """cdc + mysql (MySQL/MariaDB) -> Debezium MySqlConnector. Same schema-history
    mechanism as SQL Server; the MySQL connector also serves MariaDB."""
    tname = topic_name(spec)
    prefix = f"cdc.{spec.source}"

    config: dict = {
        "database.hostname": spec.db_host,
        "database.port": "3306",
        "database.user": _cred(spec.source, "user"),
        "database.password": _cred(spec.source, "pass"),
        "database.server.id": _mysql_server_id(spec.source),
        "database.include.list": spec.db,
        "topic.prefix": prefix,
        "table.include.list": f"{spec.db}.{spec.table}",
        "schema.history.internal.kafka.bootstrap.servers": SCHEMA_HISTORY_BOOTSTRAP,
        "schema.history.internal.kafka.topic": f"schema-history.{spec.source}",
    }
    config.update(_schema_history_client_config("producer"))
    config.update(_schema_history_client_config("consumer"))
    config.update({
        "key.converter": "io.apicurio.registry.utils.converter.AvroConverter",
        "value.converter": "io.apicurio.registry.utils.converter.AvroConverter",
        "key.converter.apicurio.registry.url": APICURIO_URL,
        "value.converter.apicurio.registry.url": APICURIO_URL,
        "key.converter.apicurio.registry.auto-register": SCHEMA_AUTO_REGISTER,
        "value.converter.apicurio.registry.auto-register": SCHEMA_AUTO_REGISTER,
        "transforms": "unwrap,route,tsconv",
        "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
        "transforms.unwrap.delete.handling.mode": "rewrite",
        "transforms.unwrap.drop.tombstones": "false",
        # MySQL LSN analogue: binlog position (source.pos, a long). Best-effort;
        # the deterministic dedup tie-break is __kafka_offset (silver-merge), not __lsn.
        "transforms.unwrap.add.fields": "op,ts_ms,source.pos:lsn",
    })
    config.update(_route_transform(spec.target_ns, spec.target_table))
    config.update(_tsconv())
    config.update({
        "errors.tolerance": "all",
        "errors.deadletterqueue.topic.name": f"{tname}.dlq",
        "errors.deadletterqueue.context.headers.enable": "true",
    })

    _, table_name = _split_schema_table(spec.table)
    name = f"dbz-{spec.source}-{table_name}"
    return _connector(name, "io.debezium.connector.mysql.MySqlConnector", config)


def _render_cdc_mongo(spec: SourceSpec) -> dict:
    """cdc + mongo -> Debezium MongoDB connector. Mirrors
    tools/templates/source-cdc-mongo.yaml / dbz-mongo-lms.yaml."""
    tname = topic_name(spec)
    prefix = f"mongo.{spec.db}"

    config = {
        "mongodb.connection.string": spec.mongo_uri,
        "mongodb.user": _cred(spec.source, "user"),
        "mongodb.password": _cred(spec.source, "pass"),
        "topic.prefix": prefix,
        "collection.include.list": f"{spec.db}.{spec.table}",
        "capture.mode": "change_streams_update_full",
        "key.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable": "false",
        "value.converter.schemas.enable": "false",
        "transforms": "unwrap,route,tsconv",
        # Full CDC via Debezium's Mongo-specific unwrap SMT (the relational
        # ExtractNewRecordState has no Mongo equivalent — change-stream events
        # have a different envelope shape). Mirrors the relational unwrap's
        # delete.handling.mode=rewrite/drop.tombstones=false so deletes survive
        # as a tombstone-free __deleted=true record, and add.fields=op,ts_ms
        # for the same downstream silver-merge CDC metadata contract.
        "transforms.unwrap.type": "io.debezium.connector.mongodb.transforms.ExtractNewDocumentState",
        "transforms.unwrap.delete.handling.mode": "rewrite",
        "transforms.unwrap.drop.tombstones": "false",
        "transforms.unwrap.add.fields": "op,ts_ms",
    }
    config.update(_route_transform(spec.target_ns, spec.target_table))
    config.update(_tsconv())
    config.update({
        "errors.tolerance": "all",
        "errors.deadletterqueue.topic.name": f"{tname}.dlq",
        "errors.deadletterqueue.context.headers.enable": "true",
    })

    name = f"dbz-{spec.source}-{spec.table}"
    return _connector(name, "io.debezium.connector.mongodb.MongoDbConnector", config)


def _render_scheduled_jdbc(spec: SourceSpec) -> dict:
    """scheduled + mssql/pg -> Aiven JDBC source connector. Mirrors
    tools/templates/source-scheduled-jdbc.yaml / jdbc-mssql-scheduled.yaml,
    plus errors.tolerance/DLQ (mirroring the relational connector's DLQ
    shape, see _render_cdc_relational above)."""
    prefix, table_name = _jdbc_topic_parts(spec)
    tname = topic_name(spec)

    config: dict = {
        "connection.url": spec.jdbc_url,
        "connection.user": _cred(spec.source, "user"),
        "connection.password": _cred(spec.source, "pass"),
        # Can only run true delta ("timestamp+incrementing") when a timestamp
        # column was supplied; otherwise fall back to incrementing-only.
        "mode": "timestamp+incrementing" if spec.timestamp_col else "incrementing",
        "incrementing.column.name": spec.incrementing_col,
        "table.whitelist": table_name,
        "topic.prefix": prefix,
        "poll.interval.ms": str(spec.poll_ms) if spec.poll_ms else DEFAULT_POLL_MS,
        "value.converter": "io.apicurio.registry.utils.converter.AvroConverter",
        "value.converter.apicurio.registry.url": APICURIO_URL,
        "value.converter.apicurio.registry.auto-register": SCHEMA_AUTO_REGISTER,
        # JDBC has no CDC envelope (no __op/__deleted/__lsn) — this is a plain
        # upsert-only poller, so __op/__deleted are synthesized as constants
        # ("u"/"false": every polled row is an upsert of current state; this
        # source can never observe a delete, see the scheduled+mongo Spark
        # path for the one source kind that reconciles deletes out-of-band).
        "transforms.setop.type": "org.apache.kafka.connect.transforms.InsertField$Value",
        "transforms.setop.static.field": "__op",
        "transforms.setop.static.value": "u",
        "transforms.setdel.type": "org.apache.kafka.connect.transforms.InsertField$Value",
        "transforms.setdel.static.field": "__deleted",
        "transforms.setdel.static.value": "false",
    }
    # Aiven's JDBC source connector (TimestampIncrementingTableQuerier) builds
    # every SourceRecord via the 5-arg constructor `new SourceRecord(partition,
    # offset, topic, schema, value)` -- it never passes a timestamp, so
    # `SourceRecord.timestamp()` is always null. JDBC records therefore carry
    # no source timestamp of their own.
    #
    # When running in timestamp+incrementing mode (a timestamp.column.name was
    # supplied), __ts_ms is instead derived from that column -- the only
    # JDBC-native source of event time. `tsfield` (ReplaceField$Value) renames
    # <timestamp_col> -> __ts_ms in place (keeping its Timestamp value; the
    # column becomes __ts_ms); `tsconv` then normalizes it to a Connect
    # Timestamp (idempotent if already Timestamp, safe if it's date/other
    # temporal).
    if spec.timestamp_col:
        config["timestamp.column.name"] = spec.timestamp_col
        config["transforms"] = "route,setop,setdel,tsfield,tsconv"
        config["transforms.tsfield.type"] = "org.apache.kafka.connect.transforms.ReplaceField$Value"
        config["transforms.tsfield.renames"] = f"{spec.timestamp_col}:__ts_ms"
        config.update(_tsconv())
    else:
        # incrementing-only mode (no timestamp column supplied): there is no
        # JDBC-native event-time source at all, so __ts_ms is intentionally
        # left unset -- do NOT add tsfield (nothing to rename from) or tsconv
        # (it would throw on a missing __ts_ms field). This DEGRADES the
        # Bronze day(__ts_ms) partition/TTL and the MERGE latest-per-key
        # ordering for this source (both end up operating on a null
        # __ts_ms); a timestamp column should be supplied for correct
        # medallion behavior on JDBC sources.
        config["transforms"] = "route,setop,setdel"
    config.update(_route_transform(spec.target_ns, spec.target_table))
    config.update({
        "errors.tolerance": "all",
        "errors.deadletterqueue.topic.name": f"{tname}.dlq",
        "errors.deadletterqueue.context.headers.enable": "true",
        "errors.deadletterqueue.topic.replication.factor": DLQ_REPLICATION_FACTOR,
    })

    name = f"jdbc-{spec.source}-{table_name}"
    return _connector(name, "io.aiven.connect.jdbc.JdbcSourceConnector", config)


def _kafka_consumer_override(spec: SourceSpec) -> dict:
    """External-Kafka consumer overrides. A Kafka Connect SINK normally reads
    from the worker's own cluster; `consumer.override.*` (allowed by the
    default connector.client.config.override.policy=All on Kafka 3.0+, set
    explicitly in 12-kafka-connect.yaml) reroutes THIS sink's input consumer to
    the customer's external brokers. Empty when spec.kafka_bootstrap is None
    (reads the in-cluster Kafka)."""
    if not spec.kafka_bootstrap:
        return {}
    user = _cred(spec.source, "user")
    password = _cred(spec.source, "pass")
    return {
        "consumer.override.bootstrap.servers": spec.kafka_bootstrap,
        "consumer.override.security.protocol": "SASL_SSL",
        "consumer.override.sasl.mechanism": "SCRAM-SHA-512",
        "consumer.override.sasl.jaas.config": (
            "org.apache.kafka.common.security.scram.ScramLoginModule required "
            f'username="{user}" password="{password}";'
        ),
        # TLS truststore for the external cluster's CA — mounted like kafka-ca;
        # see Task 5 (external-Kafka TLS is a documented hardening prerequisite).
        "consumer.override.ssl.truststore.location": "/mnt/external-configuration/ext-kafka-ca/ca.p12",
        "consumer.override.ssl.truststore.password": _cred("ext-kafka-ca", "ca.password"),
        "consumer.override.ssl.truststore.type": "PKCS12",
    }


def _render_kafka_ingest(spec: SourceSpec) -> dict:
    """stream+kafka (event/append-only) -> a DEDICATED Iceberg sink KafkaConnector
    that reads an existing topic (in-cluster or external) directly and lands an
    append-only change-log in its own Bronze table. Separate from the shared
    CDC/JDBC sinks; no message duplication. Bronze is pre-created as a metadata
    skeleton by the orchestrator; evolve-schema adds business columns."""
    name = f"kafka-ingest-{spec.source}-{spec.target_table}"
    config: dict = {
        "topics": spec.table,
        "key.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable": "false",
        "value.converter.schemas.enable": "false",
        # --- Nessie REST catalog + S3 (all deployment values via colon-form) ---
        "iceberg.catalog.type": "rest",
        "iceberg.catalog.uri": NESSIE_URI,
        "iceberg.catalog.warehouse": "rawdata",
        "iceberg.catalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        "iceberg.catalog.s3.path-style-access": "true",
        "iceberg.catalog.s3.endpoint": _cred("s3", "endpoint"),
        "iceberg.catalog.s3.access-key-id": _cred("s3", "access-key-id"),
        "iceberg.catalog.s3.secret-access-key": _cred("s3", "secret-access-key"),
        # --- fan-out routing + auto-create/evolve (no upfront business schema) ---
        "iceberg.tables.dynamic-enabled": "true",
        "iceberg.tables.route-field": "_target_table",
        "iceberg.tables.auto-create-enabled": "true",
        "iceberg.tables.evolve-schema-enabled": "true",
        "iceberg.control.commit.interval-ms": "60000",
        # --- SMTs: route + synthesize medallion metadata (event: __op=u/__deleted=false) ---
        "transforms": "route,setop,setdel,tsms,tsconv,kafkameta",
        "transforms.setop.type": "org.apache.kafka.connect.transforms.InsertField$Value",
        "transforms.setop.static.field": "__op",
        "transforms.setop.static.value": "u",
        "transforms.setdel.type": "org.apache.kafka.connect.transforms.InsertField$Value",
        "transforms.setdel.static.field": "__deleted",
        "transforms.setdel.static.value": "false",
        # __ts_ms from the Kafka record timestamp (there is no source event-time),
        # then normalize to a Connect Timestamp for the day(__ts_ms) Bronze partition.
        "transforms.tsms.type": "org.apache.kafka.connect.transforms.InsertField$Value",
        "transforms.tsms.timestamp.field": "__ts_ms",
        "transforms.tsconv.type": "org.apache.kafka.connect.transforms.TimestampConverter$Value",
        "transforms.tsconv.field": "__ts_ms",
        "transforms.tsconv.target.type": "Timestamp",
        # deterministic dedup tie-break metadata (parity with the Plan A sinks)
        "transforms.kafkameta.type": "org.apache.kafka.connect.transforms.InsertField$Value",
        "transforms.kafkameta.offset.field": "__kafka_offset",
        "transforms.kafkameta.partition.field": "__kafka_partition",
        # --- errors / DLQ ---
        "errors.tolerance": "all",
        "errors.deadletterqueue.topic.name": f"{name}.dlq",
        "errors.deadletterqueue.topic.replication.factor": DLQ_REPLICATION_FACTOR,
        "errors.deadletterqueue.context.headers.enable": "true",
    }
    config.update(_route_transform(spec.target_ns, spec.target_table))
    config.update(_kafka_consumer_override(spec))
    return _connector(name, "org.apache.iceberg.connect.IcebergSinkConnector", config)


_RENDERERS = {
    "cdc-relational": _render_cdc_relational,
    "cdc-mongo": _render_cdc_mongo,
    "scheduled-jdbc": _render_scheduled_jdbc,
    "cdc-mysql": _render_cdc_mysql,
    "kafka-ingest": _render_kafka_ingest,
}


def render_connector(spec: SourceSpec) -> dict:
    """Dispatch on the source-type registry -> KafkaConnector dict.

    The spark-batch lane (e.g. scheduled+mongo) has no render_key and is
    intentionally not a KafkaConnector — it raises NotImplementedError, and
    the orchestrator rejects it up front (see orchestrator module docstring)."""
    descriptor = source_types.get(spec.kind, spec.type)
    fn = _RENDERERS.get(descriptor.render_key)
    if fn is None:
        raise NotImplementedError(
            f"render_connector: no KafkaConnector rendering for {descriptor.id} "
            f"(lane={descriptor.lane}); the spark-batch lane uses a Spark CronJob"
        )
    return fn(spec)
