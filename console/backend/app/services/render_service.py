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
from urllib.parse import urlsplit

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
# Annotation namespace stamped on every rendered CR (KafkaConnector,
# dedicated sink, ScheduledSparkApplication) so Console/GitOps callers can
# round-trip the logical `spec.source` that produced a given CR from the CR
# object alone -- the CR's own `metadata.name` is a composite
# (`_k8s_name(prefix, spec.source, table)`), never the bare source id, so
# there is no other reliable way to recover it (see gitops delete routing in
# routers.sources.delete_source, which fails loud without this annotation).
SPARK_ANNOTATION_PREFIX = "lakehouse.solus.dev/"


# --------------------------------------------------------------------------
# Small pure helpers
# --------------------------------------------------------------------------

def bucket_name(target_ns: str) -> str:
    """`src-<ns>`, RFC1123-safe (e.g. mssql_ogrenci -> src-mssql-ogrenci)."""
    return _k8s_name("src", target_ns)


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


def _k8s_name(*parts: str) -> str:
    """RFC1123-label-safe k8s object name: join parts with '-', lowercase,
    replace any run of invalid chars with a single '-', trim leading/trailing
    '-', cap at 63. Clean lowercase-alnum-'-' inputs pass through unchanged."""
    joined = "-".join(p for p in parts if p)
    s = re.sub(r"[^a-z0-9-]+", "-", joined.lower())
    s = re.sub(r"-{2,}", "-", s).strip("-")
    return s[:63].strip("-")


def _k8s_topic_name(topic: str) -> str:
    """RFC1123-subdomain-safe KafkaTopic CR name: keep '.' (valid + meaningful
    in Kafka topic names), lowercase, invalid runs -> '-', trim. Used as the CR
    metadata.name; the real (possibly '_'/uppercase) topic goes in spec.topicName
    when it differs (see render_kafka_topic)."""
    s = re.sub(r"[^a-z0-9.-]+", "-", topic.lower())
    s = re.sub(r"-{2,}", "-", s).strip("-.")
    return s[:253]


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


def _connector(name: str, klass: str, config: dict, source: str) -> dict:
    # NB: no `metadata.namespace` here on purpose — the API layer
    # (`app.services.k8s_service.K8sService._apply`) always applies this body
    # via `create/patch_namespaced_custom_object(namespace=self.namespace, ...)`,
    # i.e. the release/target namespace is supplied by the API call itself,
    # not by the body. Stamping a hardcoded namespace into the body here
    # would cause an API-side namespace mismatch (the body's namespace vs.
    # the URL path's namespace) on any install whose namespace isn't the
    # dev/POC default.
    #
    # `source` -- the logical spec.source that produced this CR -- IS stamped
    # as an annotation (unlike namespace): `name` is a composite
    # (`_k8s_name(prefix, source, table)`), never the bare source id, so
    # without this annotation there is no way to recover `spec.source` from
    # the CR alone (needed by e.g. gitops delete routing).
    return {
        "apiVersion": "kafka.strimzi.io/v1beta2",
        "kind": "KafkaConnector",
        "metadata": {
            "name": name,
            "labels": {"strimzi.io/cluster": "connect"},
            "annotations": {f"{SPARK_ANNOTATION_PREFIX}source": source},
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


def _topic_camel_http(spec: SourceSpec) -> str:
    return f"http.{spec.source}.{spec.target_table}"


def _topic_camel_mqtt(spec: SourceSpec) -> str:
    return f"mqtt.{spec.source}.{spec.target_table}"


def _topic_camel_rabbitmq(spec: SourceSpec) -> str:
    # RabbitMQ/AMQP topic prefix is `amqp` (protocol), not `rabbitmq`.
    return f"amqp.{spec.source}.{spec.target_table}"


_TOPICS = {
    "cdc-relational": _topic_cdc_relational,
    "cdc-mongo": _topic_cdc_mongo,
    "scheduled-jdbc": _topic_scheduled_jdbc,
    "cdc-mysql": _topic_cdc_mysql,
    "camel-http": _topic_camel_http,
    "camel-mqtt": _topic_camel_mqtt,
    "camel-rabbitmq": _topic_camel_rabbitmq,
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

    `metadata.name` is sanitized to an RFC1123-subdomain-safe CR name via
    `_k8s_topic_name`; when that differs from the real Kafka topic (e.g. it
    contains `_` or uppercase), the real topic is preserved in `spec.topicName`
    (Strimzi's KafkaTopic honors `spec.topicName` when it differs from
    `metadata.name`).
    """
    cr_name = _k8s_topic_name(topic_name)
    spec: dict = {
        "partitions": 6,
        "replicas": 3,
        "config": {"retention.ms": "604800000", "cleanup.policy": "delete"},
    }
    if cr_name != topic_name:
        spec["topicName"] = topic_name
    return {
        "apiVersion": "kafka.strimzi.io/v1beta2",
        "kind": "KafkaTopic",
        "metadata": {
            "name": cr_name,
            "labels": {"strimzi.io/cluster": "kafka"},
        },
        "spec": spec,
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
    name = _k8s_name("dbz", spec.source, table_name)
    return _connector(name, klass, config, spec.source)


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
    name = _k8s_name("dbz", spec.source, table_name)
    return _connector(name, "io.debezium.connector.mysql.MySqlConnector", config, spec.source)


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

    name = _k8s_name("dbz", spec.source, spec.table)
    return _connector(name, "io.debezium.connector.mongodb.MongoDbConnector", config, spec.source)


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

    name = _k8s_name("jdbc", spec.source, table_name)
    return _connector(name, "io.aiven.connect.jdbc.JdbcSourceConnector", config, spec.source)


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


def _iceberg_catalog_io() -> dict:
    """Nessie REST catalog + S3 (rawdata Bronze warehouse) + dynamic-routing
    knobs shared by every Console-rendered Iceberg sink. Routing reads the
    source-stamped `_target_table` field."""
    return {
        "iceberg.catalog.type": "rest",
        "iceberg.catalog.uri": NESSIE_URI,
        "iceberg.catalog.warehouse": "rawdata",
        "iceberg.catalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        "iceberg.catalog.s3.path-style-access": "true",
        "iceberg.catalog.s3.endpoint": _cred("s3", "endpoint"),
        "iceberg.catalog.s3.access-key-id": _cred("s3", "access-key-id"),
        "iceberg.catalog.s3.secret-access-key": _cred("s3", "secret-access-key"),
        "iceberg.tables.dynamic-enabled": "true",
        "iceberg.tables.route-field": "_target_table",
        "iceberg.tables.auto-create-enabled": "true",
        "iceberg.tables.evolve-schema-enabled": "true",
    }


def _iceberg_control_tuning(name: str) -> dict:
    """Per-connector control topic (`control-<name>`) + commit windows +
    Kafka client tuning. A per-connector control topic keeps two concurrent
    dedicated sinks from cross-reading each other's commit-request/response
    control events (which silently commits 0 rows). The long session/commit
    windows keep the `cg-control-*` consumer from flapping on a resource-
    constrained/single-worker Connect. The chart's `control` ACL prefix covers
    `control-*`, so no ACL change is needed."""
    return {
        "iceberg.control.topic": _k8s_topic_name(f"control-{name}"),
        "iceberg.control.commit.interval-ms": "60000",
        "iceberg.control.commit.timeout-ms": "120000",
        "iceberg.kafka.session.timeout.ms": "120000",
        "iceberg.kafka.heartbeat.interval.ms": "15000",
        "iceberg.kafka.request.timeout.ms": "130000",
        "iceberg.kafka.max.poll.interval.ms": "300000",
    }


def _iceberg_sink_config(name: str, topics: str, target_ns: str, target_table: str, dlq_name: str) -> dict:
    """Shared dedicated-Iceberg-sink config for RAW-BODY lanes (kafka-ingest,
    Camel): JSON converters + shared catalog/S3/routing (`_iceberg_catalog_io`)
    + per-connector control tuning (`_iceberg_control_tuning`) + the medallion
    SMT chain (route/setop/setdel/tsms/tsconv/kafkameta) + DLQ. value.converter
    deserializes JSON to a Map BEFORE the SMTs, so InsertField$Value works on the
    Map (unlike a Camel source's byte[] value)."""
    config: dict = {
        "topics": topics,
        "key.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable": "false",
        "value.converter.schemas.enable": "false",
        **_iceberg_catalog_io(),
        **_iceberg_control_tuning(name),
        "transforms": "route,setop,setdel,tsms,tsconv,kafkameta",
        "transforms.setop.type": "org.apache.kafka.connect.transforms.InsertField$Value",
        "transforms.setop.static.field": "__op",
        "transforms.setop.static.value": "u",
        "transforms.setdel.type": "org.apache.kafka.connect.transforms.InsertField$Value",
        "transforms.setdel.static.field": "__deleted",
        "transforms.setdel.static.value": "false",
        "transforms.tsms.type": "org.apache.kafka.connect.transforms.InsertField$Value",
        "transforms.tsms.timestamp.field": "__ts_ms",
        "transforms.tsconv.type": "org.apache.kafka.connect.transforms.TimestampConverter$Value",
        "transforms.tsconv.field": "__ts_ms",
        "transforms.tsconv.target.type": "Timestamp",
        "transforms.kafkameta.type": "org.apache.kafka.connect.transforms.InsertField$Value",
        "transforms.kafkameta.offset.field": "__kafka_offset",
        "transforms.kafkameta.partition.field": "__kafka_partition",
        "errors.tolerance": "all",
        "errors.deadletterqueue.topic.name": dlq_name,
        "errors.deadletterqueue.topic.replication.factor": DLQ_REPLICATION_FACTOR,
        "errors.deadletterqueue.context.headers.enable": "true",
    }
    config.update(_route_transform(target_ns, target_table))
    return config


def _render_kafka_ingest(spec: SourceSpec) -> dict:
    """stream+kafka -> DEDICATED Iceberg sink reading an existing topic.

    event (default): __deleted is a static "false" -- append-only, no upsert.
    entity + delete_field: rename the upstream BOOLEAN delete flag to __deleted
    so merge_cdc's `WHEN MATCHED AND __deleted THEN DELETE` fires. The flag must
    be a boolean present on EVERY record (else __deleted is NULL and merge_sql's
    NOT-MATCHED insert is skipped by three-valued logic). Value-equality deletes
    (op=="d") are v2 (need a custom SMT). Localized here on purpose: the shared
    _iceberg_sink_config and the Camel sinks are untouched.
    """
    name = _k8s_name("kafka-ingest", spec.source, spec.target_table)
    config = _iceberg_sink_config(name, spec.table, spec.target_ns, spec.target_table, f"{name}.dlq")
    config.update(_kafka_consumer_override(spec))
    if spec.effective_disposition() == "entity" and spec.delete_field:
        config.pop("transforms.setdel.static.field", None)
        config.pop("transforms.setdel.static.value", None)
        config["transforms.setdel.type"] = "org.apache.kafka.connect.transforms.ReplaceField$Value"
        config["transforms.setdel.renames"] = f"{spec.delete_field}:__deleted"
    return _connector(name, "org.apache.iceberg.connect.IcebergSinkConnector", config, spec.source)


# --------------------------------------------------------------------------
# Camel Kafka SOURCE connectors — stream+http/mqtt/rabbitmq (Plan B3)
# --------------------------------------------------------------------------
# Apache Camel Kamelet-based Kafka *source* connectors (baked into the prebuilt
# Connect image, Task 3). Each polls/subscribes its protocol and PRODUCES THE
# RAW JSON BODY (byte[], ByteArrayConverter, NO SMTs) to its own topic
# (http.*/mqtt.*/amqp.*). The medallion-metadata SMT chain does NOT run here —
# a Camel source's value is a byte[], and InsertField$Value silently drops
# every record against a byte[] (live-confirmed, see
# 2026-07-30-b3-camel-sink-side-transform). The transform instead runs on a
# DEDICATED Iceberg sink (render_camel_sink, below): JsonConverter deserializes
# the byte[] to a Map BEFORE the SMTs apply. Kamelet config keys verified
# against camel.apache.org/camel-kamelets/latest (http-source / mqtt-source /
# spring-rabbitmq-source).
#
# KNOWN LIVE-VERIFY RISKS (Task 6, not unit-testable here):
#   (1) `topics` as the destination-topic key for a Camel *source* connector,
#       and spring-rabbitmq-source `exchangeName` (defaulted to the queue name
#       for pure queue-consume with autoDeclare=false) both need live confirm.

_BYTE_CONVERTERS = {
    "key.converter": "org.apache.kafka.connect.converters.ByteArrayConverter",
    "value.converter": "org.apache.kafka.connect.converters.ByteArrayConverter",
}


def _render_camel_http(spec: SourceSpec) -> dict:
    """stream+http -> Camel http-source producing the RAW JSON body (byte[]) to
    http.<source>.<table>. NO SMTs here — the medallion transform runs on the
    dedicated sink (render_camel_sink), where JsonConverter has parsed the body
    to a Map. See spec 2026-07-30-b3-camel-sink-side-transform."""
    name = _k8s_name("http", spec.source, spec.target_table)
    config = {"topics": topic_name(spec), "camel.kamelet.http-source.url": spec.http_url, **_BYTE_CONVERTERS}
    if spec.poll_ms is not None:
        config["camel.kamelet.http-source.period"] = str(spec.poll_ms)
    return _connector(
        name, "org.apache.camel.kafkaconnector.httpsource.CamelHttpsourceSourceConnector", config, spec.source
    )


def _render_camel_mqtt(spec: SourceSpec) -> dict:
    """stream+mqtt -> Camel `mqtt-source` Kamelet KC source. Subscribes
    spec.mqtt_topic on spec.mqtt_broker and produces the RAW JSON body (byte[])
    to mqtt.<source>.<table>. NO SMTs here — see _render_camel_http's docstring;
    the medallion transform runs on the dedicated sink (render_camel_sink).
    Broker creds via DirectoryConfigProvider colon-form (mounted secret <source>).

    v1: broker username/password are emitted unconditionally (DirectoryConfigProvider);
    the orchestrator's secret step must run (creds supplied) or these refs dangle at
    startup. Anonymous-broker support (omit creds) is a follow-up (live-verify)."""
    name = _k8s_name("mqtt", spec.source, spec.target_table)
    config = {
        "topics": topic_name(spec),
        "camel.kamelet.mqtt-source.brokerUrl": spec.mqtt_broker,
        "camel.kamelet.mqtt-source.topic": spec.mqtt_topic,
        "camel.kamelet.mqtt-source.username": _cred(spec.source, "user"),
        "camel.kamelet.mqtt-source.password": _cred(spec.source, "pass"),
    }
    config.update(_BYTE_CONVERTERS)
    return _connector(
        name,
        "org.apache.camel.kafkaconnector.mqttsource.CamelMqttsourceSourceConnector",
        config,
        spec.source,
    )


def _render_camel_rabbitmq(spec: SourceSpec) -> dict:
    """stream+rabbitmq -> Camel `spring-rabbitmq-source` Kamelet KC source.
    Consumes spec.rabbitmq_queue on the broker parsed from spec.rabbitmq_uri and
    produces the RAW JSON body (byte[]) to amqp.<source>.<table>. NO SMTs here —
    see _render_camel_http's docstring; the medallion transform runs on the
    dedicated sink (render_camel_sink). The Kamelet requires exchangeName; for a
    pure queue-consumer we pass the queue name and leave autoDeclare at its
    default (false) — queue/exchange must pre-exist (live-verify, Task 6).

    v1: broker username/password are emitted unconditionally (DirectoryConfigProvider);
    the orchestrator's secret step must run (creds supplied) or these refs dangle at
    startup. Anonymous-broker support (omit creds) is a follow-up (live-verify)."""
    name = _k8s_name("rabbitmq", spec.source, spec.target_table)
    parts = urlsplit(spec.rabbitmq_uri)
    config = {
        "topics": topic_name(spec),
        "camel.kamelet.spring-rabbitmq-source.host": parts.hostname or "",
        "camel.kamelet.spring-rabbitmq-source.port": str(parts.port) if parts.port else "5672",
        "camel.kamelet.spring-rabbitmq-source.queues": spec.rabbitmq_queue,
        "camel.kamelet.spring-rabbitmq-source.exchangeName": spec.rabbitmq_queue,
        "camel.kamelet.spring-rabbitmq-source.username": _cred(spec.source, "user"),
        "camel.kamelet.spring-rabbitmq-source.password": _cred(spec.source, "pass"),
    }
    config.update(_BYTE_CONVERTERS)
    return _connector(
        name,
        "org.apache.camel.kafkaconnector.springrabbitmqsource.CamelSpringrabbitmqsourceSourceConnector",
        config,
        spec.source,
    )


def _cdc_dedicated_sink_config(name: str, topic: str, avro: bool, dlq_name: str) -> dict:
    """Leaner dedicated Iceberg sink for CDC/JDBC/mongo lanes. The Debezium/Aiven
    SOURCE already stamped `_target_table` + medallion metadata (op/ts_ms/deleted)
    and converted `__ts_ms`, so this sink does NOT re-run the medallion SMTs and
    does NOT call `_route_transform` — it only decodes, stamps the Kafka
    offset/partition (sink-side Connect record metadata, the silver-merge dedup
    tie-break), and writes to Bronze via dynamic routing on the source-stamped
    `_target_table`.

    `avro`: relational/jdbc/mysql produce Avro via Apicurio (URL only — the sink
    deserializes, it does not register schemas); mongo produces JSON."""
    if avro:
        converter = {
            "key.converter": "io.apicurio.registry.utils.converter.AvroConverter",
            "value.converter": "io.apicurio.registry.utils.converter.AvroConverter",
            "key.converter.apicurio.registry.url": APICURIO_URL,
            "value.converter.apicurio.registry.url": APICURIO_URL,
        }
    else:
        converter = {
            "key.converter": "org.apache.kafka.connect.json.JsonConverter",
            "value.converter": "org.apache.kafka.connect.json.JsonConverter",
            "key.converter.schemas.enable": "false",
            "value.converter.schemas.enable": "false",
        }
    return {
        "topics": topic,
        **converter,
        **_iceberg_catalog_io(),
        **_iceberg_control_tuning(name),
        "transforms": "kafkameta",
        "transforms.kafkameta.type": "org.apache.kafka.connect.transforms.InsertField$Value",
        "transforms.kafkameta.offset.field": "__kafka_offset",
        "transforms.kafkameta.partition.field": "__kafka_partition",
        "errors.tolerance": "all",
        "errors.deadletterqueue.topic.name": dlq_name,
        "errors.deadletterqueue.topic.replication.factor": DLQ_REPLICATION_FACTOR,
        "errors.deadletterqueue.context.headers.enable": "true",
    }


def render_cdc_sink(spec: SourceSpec) -> dict:
    """Dedicated Iceberg sink for a CDC/JDBC/mongo lane. Mirrors render_camel_sink:
    reads the source's topic, writes Bronze, DLQ under the source topic prefix so
    the connect KafkaUser ACL covers it. Avro for relational/jdbc/mysql, JSON for
    mongo (the only JSON CDC lane)."""
    descriptor = source_types.get(spec.kind, spec.type)
    avro = descriptor.render_key != "cdc-mongo"
    topic = topic_name(spec)
    src_name = render_connector(spec)["metadata"]["name"]
    name = _k8s_name(src_name, "sink")
    config = _cdc_dedicated_sink_config(name, topic, avro, f"{topic}.dlq")
    return _connector(name, "org.apache.iceberg.connect.IcebergSinkConnector", config, spec.source)


def render_camel_sink(spec: SourceSpec) -> dict:
    """Dedicated Iceberg sink for a Camel lane: reads the source's topic, JSON-
    deserializes to a Map, applies the medallion SMTs, routes to <ns>_raw.<tbl>.
    DLQ under the source's topic prefix so the connect KafkaUser ACL covers it."""
    topic = topic_name(spec)
    src_name = render_connector(spec)["metadata"]["name"]
    name = _k8s_name(src_name, "sink")
    config = _iceberg_sink_config(name, topic, spec.target_ns, spec.target_table, f"{topic}.dlq")
    return _connector(name, "org.apache.iceberg.connect.IcebergSinkConnector", config, spec.source)


_RENDERERS = {
    "cdc-relational": _render_cdc_relational,
    "cdc-mongo": _render_cdc_mongo,
    "scheduled-jdbc": _render_scheduled_jdbc,
    "cdc-mysql": _render_cdc_mysql,
    "kafka-ingest": _render_kafka_ingest,
    "camel-http": _render_camel_http,
    "camel-mqtt": _render_camel_mqtt,
    "camel-rabbitmq": _render_camel_rabbitmq,
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


_SINK_RENDERERS = {
    "cdc-relational": render_cdc_sink,
    "cdc-mysql": render_cdc_sink,
    "scheduled-jdbc": render_cdc_sink,
    "cdc-mongo": render_cdc_sink,
    "camel-http": render_camel_sink,
    "camel-mqtt": render_camel_sink,
    "camel-rabbitmq": render_camel_sink,
}


def has_dedicated_sink(spec: SourceSpec) -> bool:
    return source_types.get(spec.kind, spec.type).render_key in _SINK_RENDERERS


def render_sink(spec: SourceSpec) -> dict:
    descriptor = source_types.get(spec.kind, spec.type)
    fn = _SINK_RENDERERS.get(descriptor.render_key)
    if fn is None:
        raise NotImplementedError(f"no dedicated sink for {descriptor.id}")
    return fn(spec)


# --------------------------------------------------------------------------
# render_spark_job — dispatch + per-render_key builders (spark-batch lane)
# --------------------------------------------------------------------------

SPARK_VERSION = "3.5.1"   # matches chart versions.sparkVersion (spark-py image)
# SPARK_ANNOTATION_PREFIX (used below for the round-trip annotations on
# rendered Spark CRs) is defined once, near the top of this module, and
# shared with `_connector` (KafkaConnector/sink CRs) -- see that constant's
# docstring.


def _render_s3_register(spec: SourceSpec, spark_image: str, s3_secret_name: str) -> dict:
    """batch+s3 -> a ScheduledSparkApplication that, on spec.cron, full-refresh
    CTAS's the files under s3a://<bucket>/<prefix> into rawlake.<ns>.<table>
    (Iceberg). Register once; each tick re-derives the table (CREATE OR
    REPLACE) so newly-arrived files stay query-visible. Mirrors the chart's
    ScheduledSparkApplication shape (05-spark-operator.yaml)."""
    name = _k8s_name("s3-register", spec.source, spec.target_table)
    target = f"rawlake.{spec.target_ns}.{spec.target_table}"
    aws_keys = [("AWS_ACCESS_KEY_ID", "access-key-id"),
                ("AWS_SECRET_ACCESS_KEY", "secret-access-key"),
                ("AWS_ENDPOINT_URL_S3", "endpoint"),
                ("AWS_REGION", "region")]
    env = [{"name": n, "valueFrom": {"secretKeyRef": {"name": s3_secret_name, "key": k}}}
           for n, k in aws_keys] + [{"name": "HOME", "value": "/tmp"}]
    spark_conf = {
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "spark.sql.catalog.rawlake": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.rawlake.catalog-impl": "org.apache.iceberg.rest.RESTCatalog",
        "spark.sql.catalog.rawlake.uri": NESSIE_URI,
        "spark.sql.catalog.rawlake.warehouse": "rawdata",
        "spark.sql.catalog.rawlake.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.aws.credentials.provider":
            "com.amazonaws.auth.EnvironmentVariableCredentialsProvider",
        "spark.jars.ivy": "/tmp/.ivy2",
    }
    return {
        "apiVersion": "sparkoperator.k8s.io/v1beta2",
        "kind": "ScheduledSparkApplication",
        "metadata": {
            "name": name,
            "annotations": {
                f"{SPARK_ANNOTATION_PREFIX}source": spec.source,
                f"{SPARK_ANNOTATION_PREFIX}target-ns": spec.target_ns,
                f"{SPARK_ANNOTATION_PREFIX}target-table": spec.target_table,
                f"{SPARK_ANNOTATION_PREFIX}s3-bucket": spec.s3_bucket,
                f"{SPARK_ANNOTATION_PREFIX}s3-prefix": spec.s3_prefix,
                f"{SPARK_ANNOTATION_PREFIX}file-format": spec.file_format,
                f"{SPARK_ANNOTATION_PREFIX}cron": spec.cron,
            },
        },   # namespace supplied by the API layer (see _connector)
        "spec": {
            "schedule": spec.cron,
            "concurrencyPolicy": "Forbid",
            "template": {
                "type": "Python",
                "mode": "cluster",
                "image": spark_image,
                "sparkVersion": SPARK_VERSION,
                "mainApplicationFile": "local:///opt/spark/jobs/s3_register_table.py",
                "arguments": ["--bucket", spec.s3_bucket, "--prefix", spec.s3_prefix,
                              "--format", spec.file_format, "--target", target],
                "restartPolicy": {"type": "Never"},
                "sparkConf": spark_conf,
                "driver": {"cores": 1, "coreLimit": "1200m", "memory": "1g",
                           "serviceAccount": "spark-driver", "env": env},
                "executor": {"cores": 1, "coreLimit": "1200m", "instances": 1,
                             "memory": "1g", "env": env},
            },
        },
    }


_SPARK_RENDERERS = {"s3-register": _render_s3_register}


def render_spark_job(spec: SourceSpec, spark_image: str, s3_secret_name: str) -> dict:
    """Dispatch on the source-type registry -> ScheduledSparkApplication dict
    (spark-batch lane). A spark-batch source with no spark renderer wired yet
    (e.g. scheduled-mongo, render_key="") raises NotImplementedError."""
    descriptor = source_types.get(spec.kind, spec.type)
    fn = _SPARK_RENDERERS.get(descriptor.render_key)
    if fn is None:
        raise NotImplementedError(
            f"render_spark_job: no Spark-batch renderer for {descriptor.id} (lane={descriptor.lane})"
        )
    return fn(spec, spark_image, s3_secret_name)
