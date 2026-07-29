from __future__ import annotations

import pytest

from app.models import SourceSpec
from app.services import render_service as r


# --------------------------------------------------------------------------
# Fixtures (SourceSpec builders per kind/type combo)
# --------------------------------------------------------------------------

def _cdc_mssql():
    return SourceSpec(source="mssql1", kind="cdc", type="mssql", db="OgrenciDB",
        table="dbo.students", target_ns="mssql_ogrenci", target_table="students",
        db_host="10.0.0.5")


def _cdc_pg():
    return SourceSpec(source="pg1", kind="cdc", type="pg", db="analytics",
        table="public.orders", target_ns="pg_analytics", target_table="orders",
        db_host="10.0.0.9")


def _cdc_mongo():
    return SourceSpec(source="lms", kind="cdc", type="mongo", db="lms",
        table="enrollments", target_ns="mongo_lms", target_table="enrollments",
        mongo_uri="mongodb://h:27017/?replicaSet=rs0")


def _scheduled_mssql():
    return SourceSpec(source="mssql1", kind="scheduled", type="mssql", db="OgrenciDB",
        table="dbo.courses", target_ns="mssql_ogrenci", target_table="courses",
        jdbc_url="jdbc:sqlserver://10.0.0.5:1433;databaseName=OgrenciDB",
        incrementing_col="course_id", timestamp_col="updated_at")


def _scheduled_pg():
    return SourceSpec(source="pg1", kind="scheduled", type="pg", db="analytics",
        table="public.customers", target_ns="pg_analytics", target_table="customers",
        jdbc_url="jdbc:postgresql://10.0.0.9:5432/analytics",
        incrementing_col="customer_id")


def _mysql():
    return SourceSpec(source="my1", kind="cdc", type="mysql", db="shop",
                      table="orders", target_ns="depo", target_table="orders",
                      db_host="mysqlhost")


# --------------------------------------------------------------------------
# bucket_name / render_namespace_ddl
# --------------------------------------------------------------------------

def test_bucket_name():
    assert r.bucket_name("mssql_ogrenci") == "src-mssql-ogrenci"


def test_bucket_name_multiple_underscores():
    assert r.bucket_name("mongo_lms_prod") == "src-mongo-lms-prod"


def test_namespace_ddl():
    ddl = r.render_namespace_ddl("mssql_ogrenci", "src-mssql-ogrenci")
    assert "CREATE NAMESPACE IF NOT EXISTS lakehouse.mssql_ogrenci" in ddl
    assert "s3://src-mssql-ogrenci/warehouse" in ddl


# --------------------------------------------------------------------------
# topic_name / render_kafka_topic
# --------------------------------------------------------------------------

def test_topic_name_cdc_mssql():
    assert r.topic_name(_cdc_mssql()) == "cdc.mssql1.dbo.students"


def test_topic_name_cdc_mongo():
    assert r.topic_name(_cdc_mongo()) == "mongo.lms.enrollments"


def test_topic_name_scheduled_jdbc():
    assert r.topic_name(_scheduled_mssql()) == "jdbc.mssql1.dbo.courses"


def test_render_kafka_topic_shape():
    topic = r.render_kafka_topic("cdc.mssql1.dbo.students")
    assert topic["kind"] == "KafkaTopic"
    assert topic["metadata"]["name"] == "cdc.mssql1.dbo.students"
    # No `metadata.namespace` in the rendered body -- the API layer
    # (K8sService._apply) supplies the release namespace at apply time, so a
    # hardcoded namespace here would leak into non-default-namespace installs.
    assert "namespace" not in topic["metadata"]
    assert topic["metadata"]["labels"] == {"strimzi.io/cluster": "kafka"}
    assert topic["spec"]["partitions"] == 6
    assert topic["spec"]["replicas"] == 3


# --------------------------------------------------------------------------
# render_connector — CDC mssql (A: source-cdc-relational.yaml / dbz-mssql-students.yaml)
# --------------------------------------------------------------------------

def test_cdc_mssql_connector():
    c = r.render_connector(_cdc_mssql())
    assert c["kind"] == "KafkaConnector"
    # No `metadata.namespace` in the rendered body -- see
    # test_render_kafka_topic_shape's comment; the API layer
    # (K8sService._apply) supplies the release namespace at apply time.
    assert "namespace" not in c["metadata"]
    assert c["spec"]["class"] == "io.debezium.connector.sqlserver.SqlServerConnector"
    cfg = c["spec"]["config"]
    assert cfg["transforms.route.static.value"] == "mssql_ogrenci_raw.students"
    assert cfg["value.converter"] == "io.apicurio.registry.utils.converter.AvroConverter"
    # DirectoryConfigProvider colon form, matching A's real templates
    # (tools/templates/source-cdc-relational.yaml).
    assert "${directory:/mnt/external-configuration/mssql1:pass}" in cfg["database.password"]
    assert "${directory:/mnt/external-configuration/mssql1:user}" in cfg["database.user"]
    assert cfg["errors.deadletterqueue.topic.name"].endswith(".dlq")


def test_cdc_mssql_schema_auto_register_prod_safe_default():
    # Prod must NOT auto-register schemas (uncontrolled schema evolution) --
    # render_service has no chart-values access, so SCHEMA_AUTO_REGISTER is
    # the documented prod-safe ("false") contract constant (see
    # chart/values.yaml `connectors.schemaAutoRegister` for the equivalent
    # chart-side default).
    cfg = r.render_connector(_cdc_mssql())["spec"]["config"]
    assert cfg["key.converter.apicurio.registry.auto-register"] == "false"
    assert cfg["value.converter.apicurio.registry.auto-register"] == "false"
    assert r.SCHEMA_AUTO_REGISTER == "false"


def test_cdc_mssql_has_unwrap_before_route():
    cfg = r.render_connector(_cdc_mssql())["spec"]["config"]
    # unwrap,route,tsconv — see _tsconv (Bronze partition key + Iceberg-
    # timestamp coercion, all sources).
    assert cfg["transforms"] == "unwrap,route,tsconv"
    assert "ingestts" not in cfg["transforms"]
    assert cfg["transforms.unwrap.type"] == "io.debezium.transforms.ExtractNewRecordState"
    assert cfg["transforms.unwrap.delete.handling.mode"] == "rewrite"
    assert cfg["transforms.unwrap.drop.tombstones"] == "false"


def test_cdc_mssql_schema_history_present():
    cfg = r.render_connector(_cdc_mssql())["spec"]["config"]
    assert cfg["schema.history.internal.kafka.topic"] == "schema-history.mssql1"
    assert cfg["database.names"] == "OgrenciDB"
    assert cfg["database.encrypt"] == "true"


def test_cdc_mssql_schema_history_auth():
    # SASL_SSL + SCRAM-SHA-512 producer/consumer clients, mirroring A's
    # source-cdc-relational.yaml schema-history block field-for-field.
    cfg = r.render_connector(_cdc_mssql())["spec"]["config"]
    assert cfg["schema.history.internal.kafka.bootstrap.servers"] == "kafka-kafka-bootstrap:9093"
    assert cfg["schema.history.internal.producer.security.protocol"] == "SASL_SSL"
    assert cfg["schema.history.internal.producer.sasl.mechanism"] == "SCRAM-SHA-512"
    assert cfg["schema.history.internal.consumer.security.protocol"] == "SASL_SSL"
    assert "debezium-src" in cfg["schema.history.internal.consumer.sasl.jaas.config"]
    # colon-form directory refs inside the block
    assert ("${directory:/mnt/external-configuration/debezium-src:password}"
            in cfg["schema.history.internal.producer.sasl.jaas.config"])
    assert (cfg["schema.history.internal.consumer.ssl.truststore.password"]
            == "${directory:/mnt/external-configuration/kafka-ca:ca.password}")
    assert cfg["schema.history.internal.producer.ssl.truststore.type"] == "PKCS12"


def test_cdc_mssql_dlq_full_value():
    cfg = r.render_connector(_cdc_mssql())["spec"]["config"]
    assert cfg["errors.deadletterqueue.topic.name"] == "cdc.mssql1.dbo.students.dlq"
    assert cfg["errors.tolerance"] == "all"


def test_mssql_cdc_still_has_schema_history():
    # Regression guard for the pg schema-history removal (see
    # test_pg_cdc_has_no_schema_history below): mssql must keep carrying the
    # schema-history block (Debezium SqlServer connector requires it) — byte-
    # parity with tools/templates/source-cdc-relational.yaml.
    c = r.render_connector(_cdc_mssql())
    cfg = c["spec"]["config"]
    assert c["spec"]["class"] == "io.debezium.connector.sqlserver.SqlServerConnector"
    assert cfg["schema.history.internal.kafka.bootstrap.servers"] == "kafka-kafka-bootstrap:9093"
    assert cfg["schema.history.internal.producer.sasl.mechanism"] == "SCRAM-SHA-512"


# --------------------------------------------------------------------------
# render_connector — CDC pg (Debezium Postgres — no exact A template; conventions)
# --------------------------------------------------------------------------

def test_cdc_pg_connector_class_and_dbname():
    c = r.render_connector(_cdc_pg())
    assert c["spec"]["class"] == "io.debezium.connector.postgresql.PostgresConnector"
    cfg = c["spec"]["config"]
    assert cfg["database.dbname"] == "analytics"
    assert cfg["database.hostname"] == "10.0.0.9"
    assert cfg["plugin.name"] == "pgoutput"


def test_cdc_pg_connector_uses_avro_and_route_and_dlq():
    cfg = r.render_connector(_cdc_pg())["spec"]["config"]
    assert cfg["value.converter"] == "io.apicurio.registry.utils.converter.AvroConverter"
    assert cfg["transforms.route.static.value"] == "pg_analytics_raw.orders"
    assert cfg["errors.deadletterqueue.topic.name"].endswith(".dlq")
    # unwrap,route,tsconv — see test_cdc_mssql_has_unwrap_before_route.
    assert cfg["transforms"] == "unwrap,route,tsconv"
    assert cfg["value.converter.apicurio.registry.auto-register"] == "false"


def test_pg_cdc_has_no_schema_history():
    # schema-history is a Debezium SqlServer/MySQL/Oracle mechanism; Postgres
    # tracks position via its replication slot instead, so the pg connector
    # must carry none of these keys (see test_mssql_cdc_still_has_schema_history
    # for the mssql-side regression guard).
    c = r.render_connector(_cdc_pg())
    cfg = c["spec"]["config"]
    assert c["spec"]["class"] == "io.debezium.connector.postgresql.PostgresConnector"
    assert cfg["database.dbname"] == "analytics"
    assert cfg["plugin.name"] == "pgoutput"
    assert not any(k.startswith("schema.history.internal") for k in cfg), (
        f"pg CDC should carry no schema-history keys: "
        f"{[k for k in cfg if k.startswith('schema.history')]}"
    )


# --------------------------------------------------------------------------
# render_connector — CDC mongo (A: source-cdc-mongo.yaml / dbz-mongo-lms.yaml)
# --------------------------------------------------------------------------

def test_mongo_cdc_uses_json_converter():
    cfg = r.render_connector(_cdc_mongo())["spec"]["config"]
    assert cfg["value.converter"] == "org.apache.kafka.connect.json.JsonConverter"
    assert cfg["value.converter.schemas.enable"] == "false"


def test_mongo_cdc_class_and_collection_and_dlq():
    c = r.render_connector(_cdc_mongo())
    assert c["spec"]["class"] == "io.debezium.connector.mongodb.MongoDbConnector"
    cfg = c["spec"]["config"]
    assert cfg["collection.include.list"] == "lms.enrollments"
    assert cfg["transforms.route.static.value"] == "mongo_lms_raw.enrollments"
    assert cfg["errors.deadletterqueue.topic.name"] == "mongo.lms.enrollments.dlq"
    assert cfg["capture.mode"] == "change_streams_update_full"


# --------------------------------------------------------------------------
# render_connector — scheduled JDBC mssql/pg (A: source-scheduled-jdbc.yaml)
# --------------------------------------------------------------------------

def test_scheduled_mssql_connector_class_and_mode():
    c = r.render_connector(_scheduled_mssql())
    assert c["spec"]["class"] == "io.aiven.connect.jdbc.JdbcSourceConnector"
    cfg = c["spec"]["config"]
    assert cfg["mode"] == "timestamp+incrementing"
    assert cfg["incrementing.column.name"] == "course_id"
    assert cfg["timestamp.column.name"] == "updated_at"
    assert cfg["table.whitelist"] == "courses"
    assert cfg["topic.prefix"] == "jdbc.mssql1.dbo."


def test_scheduled_mssql_uses_avro_no_key_converter():
    cfg = r.render_connector(_scheduled_mssql())["spec"]["config"]
    assert cfg["value.converter"] == "io.apicurio.registry.utils.converter.AvroConverter"
    assert "key.converter" not in cfg
    assert cfg["value.converter.apicurio.registry.auto-register"] == "false"


def test_scheduled_jdbc_has_dlq():
    # The JDBC scheduled connector carries errors.tolerance/DLQ, mirroring
    # the relational connector's DLQ shape plus an explicit
    # replication.factor (see _render_scheduled_jdbc/DLQ_REPLICATION_FACTOR).
    cfg = r.render_connector(_scheduled_mssql())["spec"]["config"]
    assert cfg["errors.tolerance"] == "all"
    assert cfg["errors.deadletterqueue.topic.name"] == "jdbc.mssql1.dbo.courses.dlq"
    assert cfg["errors.deadletterqueue.context.headers.enable"] == "true"
    assert cfg["errors.deadletterqueue.topic.replication.factor"] == r.DLQ_REPLICATION_FACTOR
    assert r.DLQ_REPLICATION_FACTOR == "3"


def test_scheduled_jdbc_route_transform():
    cfg = r.render_connector(_scheduled_mssql())["spec"]["config"]
    # Aiven's JDBC SourceRecord carries no timestamp (5-arg constructor), so
    # __ts_ms is derived from the timestamp column via `tsfield`
    # (ReplaceField$Value renames <timestamp_col>:__ts_ms), then `tsconv`
    # normalizes it. Chain: route,setop,setdel,tsfield,tsconv.
    assert cfg["transforms"] == "route,setop,setdel,tsfield,tsconv"
    assert cfg["transforms.route.type"] == "org.apache.kafka.connect.transforms.InsertField$Value"
    assert cfg["transforms.route.static.field"] == "_target_table"
    assert cfg["transforms.route.static.value"] == "mssql_ogrenci_raw.courses"
    assert cfg["transforms.tsfield.type"] == "org.apache.kafka.connect.transforms.ReplaceField$Value"
    assert cfg["transforms.tsfield.renames"] == "updated_at:__ts_ms"
    assert cfg["transforms.tsconv.field"] == "__ts_ms"
    assert "transforms.setts.timestamp.field" not in cfg


# --------------------------------------------------------------------------
# render_connector — CDC mysql/mariadb (Debezium MySqlConnector, Task 4)
# --------------------------------------------------------------------------

def test_mysql_topic_name():
    assert r.topic_name(_mysql()) == "cdc.my1.shop.orders"


def test_mysql_connector_shape():
    body = r.render_connector(_mysql())
    c = body["spec"]["config"]
    assert body["spec"]["class"] == "io.debezium.connector.mysql.MySqlConnector"
    assert c["database.port"] == "3306"
    assert c["database.include.list"] == "shop"
    assert c["table.include.list"] == "shop.orders"
    assert c["database.server.id"].isdigit()
    assert c["transforms.route.static.value"] == "depo_raw.orders"
    assert c["transforms.unwrap.add.fields"] == "op,ts_ms,source.pos:lsn"
    assert c["schema.history.internal.kafka.topic"] == "schema-history.my1"
    assert body["metadata"]["name"] == "dbz-my1-orders"


def test_scheduled_pg_connector_mode_without_timestamp_col():
    # No timestamp_col supplied -> can't use timestamp+incrementing mode.
    cfg = r.render_connector(_scheduled_pg())["spec"]["config"]
    assert cfg["mode"] == "incrementing"
    assert "timestamp.column.name" not in cfg
    assert cfg["connection.url"] == "jdbc:postgresql://10.0.0.9:5432/analytics"
    # Incrementing-only mode has no JDBC-native event-time source, so
    # __ts_ms is intentionally left unset: no tsfield (nothing to rename
    # from) and no tsconv (would throw on a missing __ts_ms field). Degraded
    # Bronze partition/TTL + MERGE ordering for this source.
    assert cfg["transforms"] == "route,setop,setdel"
    assert "transforms.tsfield.type" not in cfg
    assert "transforms.tsfield.renames" not in cfg
    assert "transforms.setts.timestamp.field" not in cfg
    assert "transforms.tsconv.field" not in cfg


# --------------------------------------------------------------------------
# Unsupported combo (scheduled+mongo -> Spark batch, not a KafkaConnector)
# --------------------------------------------------------------------------

def test_scheduled_mongo_not_a_kafka_connector():
    s = SourceSpec(source="lms", kind="scheduled", type="mongo", db="lms",
        table="enrollments", target_ns="mongo_lms", target_table="enrollments",
        cron="0 * * * *")
    with pytest.raises(NotImplementedError):
        r.render_connector(s)


# --------------------------------------------------------------------------
# Bronze routing + CDC metadata (medallion CDC: Bronze changelog -> Spark
# MERGE -> Silver)
# --------------------------------------------------------------------------

def test_relational_routes_to_bronze_and_adds_cdc_metadata():
    from app.models import SourceSpec
    from app.services import render_service as rs
    spec = SourceSpec(source="pgdemo", kind="cdc", type="pg", db="appdb",
                      table="public.customers", target_ns="depo",
                      target_table="customers", db_host="pgdemo")
    cfg = rs.render_connector(spec)["spec"]["config"]
    assert cfg["transforms.route.static.value"] == "depo_raw.customers"
    assert cfg["transforms.unwrap.add.fields"] == "op,ts_ms,source.lsn:lsn"


def test_mssql_uses_change_lsn():
    from app.models import SourceSpec
    from app.services import render_service as rs
    spec = SourceSpec(source="mss", kind="cdc", type="mssql", db="OgrenciDB",
                      table="dbo.students", target_ns="mssql_ogrenci",
                      target_table="students", db_host="mss")
    cfg = rs.render_connector(spec)["spec"]["config"]
    assert cfg["transforms.unwrap.add.fields"] == "op,ts_ms,source.change_lsn:lsn"
    assert cfg["transforms.route.static.value"] == "mssql_ogrenci_raw.students"


def test_mongo_and_jdbc_route_to_bronze():
    from app.models import SourceSpec
    from app.services import render_service as rs
    mongo = SourceSpec(source="m", kind="cdc", type="mongo", db="lms", table="enrollments",
                       target_ns="mongo_lms", target_table="enrollments",
                       mongo_uri="mongodb://h")
    assert rs.render_connector(mongo)["spec"]["config"]["transforms.route.static.value"] \
        == "mongo_lms_raw.enrollments"


# --------------------------------------------------------------------------
# Source-SMT contract: tsconv only (all sources). Mongo full-CDC
# ExtractNewDocumentState, JDBC synthesized upsert-only fields.
# --------------------------------------------------------------------------

def test_relational_adds_tsconv_and_no_ingestts():
    from app.models import SourceSpec
    from app.services import render_service as rs
    spec = SourceSpec(source="pgd", kind="cdc", type="pg", db="d", table="public.customers",
                      target_ns="depo", target_table="customers", db_host="h")
    cfg = rs.render_connector(spec)["spec"]["config"]
    assert "tsconv" in cfg["transforms"]
    assert "ingestts" not in cfg["transforms"]
    assert "transforms.ingestts.timestamp.field" not in cfg
    assert cfg["transforms.tsconv.field"] == "__ts_ms"
    assert cfg["transforms.tsconv.target.type"] == "Timestamp"

def test_mongo_uses_extract_new_document_state_full_cdc():
    from app.models import SourceSpec
    from app.services import render_service as rs
    m = SourceSpec(source="m", kind="cdc", type="mongo", db="lms", table="enrollments",
                   target_ns="mongo_lms", target_table="enrollments", mongo_uri="mongodb://h")
    cfg = rs.render_connector(m)["spec"]["config"]
    assert cfg["transforms.unwrap.type"] == "io.debezium.connector.mongodb.transforms.ExtractNewDocumentState"
    assert cfg["transforms.unwrap.delete.handling.mode"] == "rewrite"
    assert cfg["transforms.route.static.value"] == "mongo_lms_raw.enrollments"
    assert "ingestts" not in cfg["transforms"]
    assert cfg["transforms.tsconv.field"] == "__ts_ms"

def test_jdbc_synthesizes_op_and_deleted_upsert_only():
    from app.models import SourceSpec
    from app.services import render_service as rs
    j = SourceSpec(source="mss", kind="scheduled", type="mssql", db="OgrenciDB", table="courses",
                   target_ns="mssql_ogrenci", target_table="courses",
                   jdbc_url="jdbc:sqlserver://h", incrementing_col="course_id", timestamp_col="updated_at")
    cfg = rs.render_connector(j)["spec"]["config"]
    assert cfg["transforms.setop.static.field"] == "__op" and cfg["transforms.setop.static.value"] == "u"
    assert cfg["transforms.setdel.static.field"] == "__deleted" and cfg["transforms.setdel.static.value"] == "false"
    # Aiven JDBC SourceRecord carries no timestamp, so __ts_ms is derived
    # from the timestamp column via tsfield (ReplaceField$Value) instead.
    assert cfg["transforms.tsfield.renames"] == "updated_at:__ts_ms"
    assert "transforms.setts.timestamp.field" not in cfg
    assert "ingestts" not in cfg["transforms"]


# --------------------------------------------------------------------------
# Registry-driven dispatch (render_service reads app.source_types instead of
# hard-coded kind/type if/elif chains)
# --------------------------------------------------------------------------

def _pg():
    return SourceSpec(source="pg1", kind="cdc", type="pg", db="appdb",
                      table="public.orders", target_ns="depo", target_table="orders",
                      db_host="h")


def test_topic_name_dispatches_via_registry():
    assert r.topic_name(_pg()) == "cdc.pg1.public.orders"


def test_render_connector_dispatches_via_registry():
    body = r.render_connector(_pg())
    assert body["spec"]["class"] == "io.debezium.connector.postgresql.PostgresConnector"
    assert body["metadata"]["name"] == "dbz-pg1-orders"


def test_renderers_cover_every_connector_source_type():
    from app import source_types as st
    # Every registered render_key must be wired into EITHER the KafkaConnector
    # dispatch table (_RENDERERS) or the Spark-batch dispatch table
    # (_SPARK_RENDERERS) -- e.g. batch-s3's "s3-register" is a Spark renderer
    # (ScheduledSparkApplication, render_spark_job), not a KafkaConnector.
    for d in st.all_types():
        if d.render_key:
            assert d.render_key in r._RENDERERS or d.render_key in r._SPARK_RENDERERS
        if d.topic_key:
            assert d.topic_key in r._TOPICS


# --------------------------------------------------------------------------
# stream+kafka (Plan B1) -> dedicated Iceberg sink KafkaConnector
# --------------------------------------------------------------------------

def _kafka(**over):
    from app.models import SourceSpec
    d = dict(source="k1", kind="stream", type="kafka", db="ext", table="orders-topic",
             target_ns="events", target_table="orders")
    d.update(over)
    return SourceSpec(**d)


def test_kafka_ingest_internal_no_consumer_override():
    body = r.render_connector(_kafka())
    c = body["spec"]["config"]
    assert body["spec"]["class"] == "org.apache.iceberg.connect.IcebergSinkConnector"
    assert c["topics"] == "orders-topic"
    assert c["iceberg.catalog.uri"] == "http://nessie:19120/iceberg/"
    assert c["iceberg.catalog.warehouse"] == "rawdata"
    assert c["iceberg.catalog.s3.endpoint"] == "${directory:/mnt/external-configuration/s3:endpoint}"
    assert c["iceberg.tables.route-field"] == "_target_table"
    assert c["iceberg.tables.evolve-schema-enabled"] == "true"
    assert c["transforms.route.static.value"] == "events_raw.orders"
    assert c["transforms.setop.static.value"] == "u"
    assert c["transforms.kafkameta.offset.field"] == "__kafka_offset"
    assert not any(k.startswith("consumer.override.") for k in c)   # internal => no override
    assert body["metadata"]["name"] == "kafka-ingest-k1-orders"


def test_kafka_ingest_external_has_consumer_override_and_creds():
    body = r.render_connector(_kafka(kafka_bootstrap="broker.customer:9093"))
    c = body["spec"]["config"]
    assert c["consumer.override.bootstrap.servers"] == "broker.customer:9093"
    assert c["consumer.override.security.protocol"] == "SASL_SSL"
    assert "${directory:/mnt/external-configuration/k1:pass}" in c["consumer.override.sasl.jaas.config"]


def test_kafka_ingest_registered():
    assert "kafka-ingest" in r._RENDERERS


def test_kafka_ingest_has_control_group_overrides():
    # Console-rendered dedicated sink must carry the iceberg.kafka.* control-group
    # timeout overrides (Plan B1 Task 7: without them the cg-control consumer flaps
    # on a constrained Connect and commits report "0 table(s)" — rows never land).
    c = r.render_connector(_kafka())["spec"]["config"]
    assert c["iceberg.control.commit.timeout-ms"] == "120000"
    assert c["iceberg.kafka.session.timeout.ms"] == "120000"
    assert c["iceberg.kafka.heartbeat.interval.ms"] == "15000"
    assert c["iceberg.kafka.request.timeout.ms"] == "130000"
    assert c["iceberg.kafka.max.poll.interval.ms"] == "300000"


# --------------------------------------------------------------------------
# render_spark_job / _render_s3_register (Plan B2 Task 2: spark-batch lane,
# separate ScheduledSparkApplication dispatch from the KafkaConnector one)
# --------------------------------------------------------------------------

def _s3_spec():
    return SourceSpec(source="ds1", kind="batch", type="s3", db="-", table="-",
                      target_ns="nyc", target_table="trips",
                      s3_bucket="ham-veri", s3_prefix="raw/", file_format="parquet",
                      cron="0 * * * *")


def test_render_spark_job_s3_register():
    body = r.render_spark_job(_s3_spec(), spark_image="reg/spark-py:9", s3_secret_name="s3-credentials")
    assert body["apiVersion"] == "sparkoperator.k8s.io/v1beta2"
    assert body["kind"] == "ScheduledSparkApplication"
    assert body["spec"]["schedule"] == "0 * * * *"
    tmpl = body["spec"]["template"]
    assert tmpl["image"] == "reg/spark-py:9"
    assert tmpl["mainApplicationFile"] == "local:///opt/spark/jobs/s3_register_table.py"
    assert tmpl["arguments"] == ["--bucket", "ham-veri", "--prefix", "raw/",
                                 "--format", "parquet", "--target", "rawlake.nyc.trips"]
    assert tmpl["driver"]["envFrom"][0]["secretRef"]["name"] == "s3-credentials"
    assert body["metadata"]["name"] == "s3-register-ds1-trips"


def test_render_spark_job_rejects_non_spark_renderer():
    spec = SourceSpec(source="m1", kind="scheduled", type="mongo", db="lms",
                      table="enr", target_ns="depo", target_table="enr", cron="0 * * * *")
    with pytest.raises(NotImplementedError):
        r.render_spark_job(spec, spark_image="x", s3_secret_name="y")


# --------------------------------------------------------------------------
# stream+http/mqtt/rabbitmq (Plan B3 Task 2) -> Apache Camel Kafka SOURCE
# connectors producing JSON to their own Kafka topic + synthesized medallion
# metadata, landing in Bronze via the shared JSON Iceberg sink.
# --------------------------------------------------------------------------

def _http():
    from app.models import SourceSpec
    return SourceSpec(source="h1", kind="stream", type="http", db="-", table="-",
                      target_ns="ext", target_table="prices", http_url="https://api.x/p", poll_ms=60000)


def _mqtt():
    from app.models import SourceSpec
    return SourceSpec(source="m1", kind="stream", type="mqtt", db="-", table="-",
                      target_ns="iot", target_table="sensors",
                      mqtt_broker="tcp://mosquitto:1883", mqtt_topic="sensors/#")


def _rabbitmq():
    from app.models import SourceSpec
    return SourceSpec(source="r1", kind="stream", type="rabbitmq", db="-", table="-",
                      target_ns="orders_ext", target_table="orders",
                      rabbitmq_uri="amqp://rabbitmq.default:5672/", rabbitmq_queue="orders-q")


def test_camel_http_render():
    body = r.render_connector(_http())
    c = body["spec"]["config"]
    assert body["spec"]["class"].startswith("org.apache.camel.kafkaconnector.httpsource")
    assert c["value.converter"] == "org.apache.kafka.connect.json.JsonConverter"
    assert c["transforms.route.static.value"] == "ext_raw.prices"
    assert c["transforms.kafkameta.offset.field"] == "__kafka_offset"


def test_camel_http_topic_and_endpoint_config():
    body = r.render_connector(_http())
    c = body["spec"]["config"]
    assert body["metadata"]["name"] == "http-h1-prices"
    assert c["topics"] == "http.h1.prices"
    assert c["camel.kamelet.http-source.url"] == "https://api.x/p"
    assert c["camel.kamelet.http-source.period"] == "60000"
    assert r.topic_name(_http()) == "http.h1.prices"


def test_camel_mqtt_and_rabbitmq_registered():
    assert "camel-mqtt" in r._RENDERERS and "camel-rabbitmq" in r._RENDERERS


def test_camel_mqtt_render():
    body = r.render_connector(_mqtt())
    c = body["spec"]["config"]
    assert body["spec"]["class"] == "org.apache.camel.kafkaconnector.mqttsource.CamelMqttsourceSourceConnector"
    assert body["metadata"]["name"] == "mqtt-m1-sensors"
    assert c["topics"] == "mqtt.m1.sensors"
    assert c["camel.kamelet.mqtt-source.brokerUrl"] == "tcp://mosquitto:1883"
    assert c["camel.kamelet.mqtt-source.topic"] == "sensors/#"
    assert c["camel.kamelet.mqtt-source.username"] == "${directory:/mnt/external-configuration/m1:user}"
    assert c["transforms.route.static.value"] == "iot_raw.sensors"
    assert r.topic_name(_mqtt()) == "mqtt.m1.sensors"


def test_camel_rabbitmq_render():
    body = r.render_connector(_rabbitmq())
    c = body["spec"]["config"]
    assert body["spec"]["class"] == \
        "org.apache.camel.kafkaconnector.springrabbitmqsource.CamelSpringrabbitmqsourceSourceConnector"
    assert body["metadata"]["name"] == "rabbitmq-r1-orders"
    assert c["topics"] == "amqp.r1.orders"
    assert c["camel.kamelet.spring-rabbitmq-source.host"] == "rabbitmq.default"
    assert c["camel.kamelet.spring-rabbitmq-source.port"] == "5672"
    assert c["camel.kamelet.spring-rabbitmq-source.queues"] == "orders-q"
    assert c["transforms.route.static.value"] == "orders_ext_raw.orders"
    assert r.topic_name(_rabbitmq()) == "amqp.r1.orders"


def test_camel_sources_have_dlq_and_kafkameta():
    for spec in (_http(), _mqtt(), _rabbitmq()):
        c = r.render_connector(spec)["spec"]["config"]
        assert c["errors.tolerance"] == "all"
        assert c["errors.deadletterqueue.topic.replication.factor"] == r.DLQ_REPLICATION_FACTOR
        assert c["transforms.kafkameta.partition.field"] == "__kafka_partition"
        assert c["transforms.setop.static.value"] == "u"
        assert c["transforms.setdel.static.value"] == "false"
