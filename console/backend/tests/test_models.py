from __future__ import annotations

import pytest
from pydantic import ValidationError

from app.models import DeleteMode, SourceCredentials, SourceSpec


def test_valid_cdc_mssql():
    s = SourceSpec(source="mssql1", kind="cdc", type="mssql", db="OgrenciDB",
                   table="dbo.students", target_ns="mssql_ogrenci",
                   target_table="students", db_host="10.0.0.5")
    assert s.target_ns == "mssql_ogrenci"


def test_scheduled_jdbc_requires_incrementing():
    with pytest.raises(ValidationError):
        SourceSpec(source="mssql1", kind="scheduled", type="mssql", db="OgrenciDB",
                   table="courses", target_ns="mssql_ogrenci", target_table="courses",
                   jdbc_url="jdbc:sqlserver://h:1433")  # incrementing_col eksik


def test_valid_scheduled_jdbc_with_incrementing_passes():
    s = SourceSpec(source="pg1", kind="scheduled", type="pg", db="analytics",
                   table="public.orders", target_ns="pg_analytics",
                   target_table="orders", jdbc_url="jdbc:postgresql://h:5432/analytics",
                   incrementing_col="order_id")
    assert s.jdbc_url == "jdbc:postgresql://h:5432/analytics"
    assert s.incrementing_col == "order_id"


def test_cdc_mongo_without_mongo_uri_fails():
    with pytest.raises(ValidationError):
        SourceSpec(source="mongo1", kind="cdc", type="mongo", db="ogrenciDb",
                   table="students", target_ns="mongo_ogrenci",
                   target_table="students")


def test_valid_cdc_mongo_with_mongo_uri_passes():
    s = SourceSpec(source="mongo1", kind="cdc", type="mongo", db="ogrenciDb",
                   table="students", target_ns="mongo_ogrenci",
                   target_table="students", mongo_uri="mongodb://h:27017")
    assert s.mongo_uri == "mongodb://h:27017"


def test_cdc_mssql_requires_db_host():
    with pytest.raises(ValidationError):
        SourceSpec(source="mssql1", kind="cdc", type="mssql", db="OgrenciDB",
                   table="dbo.students", target_ns="mssql_ogrenci",
                   target_table="students")


def test_cdc_pg_requires_db_host():
    with pytest.raises(ValidationError):
        SourceSpec(source="pg1", kind="cdc", type="pg", db="analytics",
                   table="public.orders", target_ns="pg_analytics",
                   target_table="orders")


def test_source_credentials_roundtrip():
    creds = SourceCredentials(user="svc_reader", password="s3cr3t")
    assert creds.user == "svc_reader"
    assert creds.password == "s3cr3t"


def test_delete_mode_literal_values():
    assert DeleteMode.__args__ == ("pipeline_only", "with_data")


def _base(**over):
    d = dict(source="s1", kind="cdc", type="pg", db="appdb", table="public.t",
             target_ns="depo", target_table="t", db_host="h")
    d.update(over)
    return d


def test_unknown_kind_type_is_rejected():
    # A (kind,type) not in the registry must be rejected before field checks.
    # type Literal blocks truly-unknown types at parse time; this asserts the
    # registry path raises for a missing required field with the new message.
    with pytest.raises(ValidationError, match="requires"):
        SourceSpec(**_base(db_host=None))


def test_valid_spec_ok():
    s = SourceSpec(**_base())
    assert s.type == "pg"


def test_cdc_mysql_requires_db_host():
    with pytest.raises(ValidationError, match="db_host"):
        SourceSpec(**_base(type="mysql", db_host=None))


def test_cdc_mysql_valid():
    s = SourceSpec(**_base(type="mysql"))
    assert s.type == "mysql"


def _kafka(**over):
    d = dict(source="k1", kind="stream", type="kafka", db="ext", table="orders-topic",
             target_ns="events", target_table="orders")
    d.update(over)
    return d


def test_stream_kafka_valid_defaults_event():
    s = SourceSpec(**_kafka())
    assert s.effective_disposition() == "event"
    assert s.kafka_bootstrap is None


def test_stream_kafka_external_bootstrap():
    s = SourceSpec(**_kafka(kafka_bootstrap="broker.customer:9093"))
    assert s.kafka_bootstrap == "broker.customer:9093"


def test_disposition_entity_rejected_for_event_only_type():
    # stream-kafka now allows entity (upsert-from-Kafka); stream-mqtt remains
    # event-only, so it is the type that still exercises this rejection path.
    with pytest.raises(ValidationError, match="disposition"):
        SourceSpec(source="m1", kind="stream", type="mqtt", db="-", table="-",
                   target_ns="iot", target_table="sensors",
                   mqtt_broker="b", mqtt_topic="t", disposition="entity")


def test_cdc_pg_still_entity():
    s = SourceSpec(**_base())          # from Plan A's test helper
    assert s.effective_disposition() == "entity"


def test_stream_non_kafka_type_is_unregistered():
    # Extending the `type` Literal to include "kafka" makes stream+mssql a
    # valid Literal combo, but it isn't registered -> registry KeyError path.
    with pytest.raises(ValidationError, match="unknown source"):
        SourceSpec(**_kafka(type="mssql"))


def test_stream_kafka_entity_with_columns_identifier_and_delete_field():
    from app.models import ColumnSpec
    s = SourceSpec(**_kafka(
        disposition="entity",
        columns=[ColumnSpec(name="id", type="bigint"), ColumnSpec(name="name", type="varchar")],
        identifier=["id"],
        delete_field="_deleted",
    ))
    assert s.effective_disposition() == "entity"
    assert s.delete_field == "_deleted"
    assert s.identifier == ["id"]


def _http(**o):
    d = dict(source="h1", kind="stream", type="http", db="-", table="-",
             target_ns="ext", target_table="prices", http_url="https://api.example/x")
    d.update(o)
    return d


def test_stream_http_requires_url():
    with pytest.raises(ValidationError, match="http_url"):
        SourceSpec(**_http(http_url=None))


def test_stream_mqtt_requires_broker_topic():
    with pytest.raises(ValidationError, match="mqtt_broker"):
        SourceSpec(source="m1", kind="stream", type="mqtt", db="-", table="-",
                   target_ns="iot", target_table="sensors", mqtt_topic="t")


def test_stream_rabbitmq_valid():
    s = SourceSpec(source="r1", kind="stream", type="rabbitmq", db="-", table="-",
                   target_ns="q", target_table="orders", rabbitmq_uri="amqp://h", rabbitmq_queue="orders")
    assert s.rabbitmq_queue == "orders"


# --------------------------------------------------------------------------
# `source` RFC1123-label validator (Task 1)
# --------------------------------------------------------------------------

def _base_source(**over):
    d = dict(source="s1", kind="cdc", type="mssql", db="school", table="dbo.students",
             target_ns="ns", target_table="t", db_host="h")
    d.update(over)
    return d


def test_source_rejects_non_rfc1123():
    for bad in ("My_Source", "src_1", "UPPER", "-lead", "trail-", "has space"):
        with pytest.raises(ValidationError):
            SourceSpec(**_base_source(source=bad))


def test_source_accepts_rfc1123():
    for ok in ("mssql1", "k1", "src-1", "a", "a1-b2"):
        assert SourceSpec(**_base_source(source=ok)).source == ok


# --------------------------------------------------------------------------
# `target_ns` canonical validator (B-v2 safety fix): target_ns doubles as
# the Silver Iceberg namespace AND the seed for the per-pipeline Bronze/
# Silver bucket names -- render_service's bucket-name sanitization is LOSSY
# (lowercases + collapses any RUN of non-[a-z0-9-] chars, including '_',
# into a SINGLE '-'). A bare "[a-z0-9_]+" check is NOT enough: doubled or
# leading/trailing underscores still collapse together ("a_b"/"a__b"/
# "a___b" all -> "bronze-a-b"; "_ab"/"ab" and "ab_"/"ab" both -> "bronze-ab"),
# so two namespace-DISTINCT target_ns values could sanitize-collide onto the
# SAME bucket, passing the orchestrator's namespace-uniqueness check yet
# sharing storage -- reintroducing the shared-bucket/data-loss class B-v2
# exists to eliminate. The validator requires lowercase alnum segments
# joined by exactly one '_' each (no leading/trailing/doubled '_') so '_' ->
# '-' is the ONLY transformation ever applied -- genuinely injective, not
# just "less lossy".
# --------------------------------------------------------------------------

def test_target_ns_rejects_non_canonical():
    for bad in ("MyPipe", "a.b", "", "has space", "a/b", "a$b",
                "a__b", "a___b", "_ab", "ab_", "_", "__"):
        with pytest.raises(ValidationError):
            SourceSpec(**_base_source(target_ns=bad))


def test_target_ns_accepts_canonical():
    for ok in ("mssql1_students", "ns", "a_b", "depo2", "mssql_ogrenci", "q"):
        assert SourceSpec(**_base_source(target_ns=ok)).target_ns == ok


def test_target_ns_distinct_canonical_names_map_to_distinct_buckets():
    # Direct injectivity check (not just "the validator rejects known-bad
    # inputs"): for every pair of DISTINCT canonical names accepted above,
    # their Bronze bucket names must also be distinct -- this is the actual
    # property the validator exists to guarantee, proven against the real
    # render_service.bronze_bucket_name, not re-derived logic.
    from app.services import render_service

    names = ["mssql1_students", "ns", "a_b", "depo2", "mssql_ogrenci", "q"]
    buckets = [render_service.bronze_bucket_name(n) for n in names]
    assert len(set(buckets)) == len(names)


# --------------------------------------------------------------------------
# Topic provisioning (Task 1)
# --------------------------------------------------------------------------

def test_source_spec_topic_provisioning_defaults():
    s = SourceSpec(source="nginx", kind="stream", type="kafka", db="-", table="nginx-logs",
                   target_ns="logs", target_table="nginx_logs")
    assert s.create_topic is False
    assert s.topic_partitions == 6
    assert s.topic_replication_factor == 3


def test_source_spec_topic_provisioning_overrides():
    s = SourceSpec(source="nginx", kind="stream", type="kafka", db="-", table="nginx-logs",
                   target_ns="logs", target_table="nginx_logs",
                   create_topic=True, topic_partitions=1, topic_replication_factor=1)
    assert s.create_topic is True
    assert (s.topic_partitions, s.topic_replication_factor) == (1, 1)


# --------------------------------------------------------------------------
# snapshot_mode (Task 5)
# --------------------------------------------------------------------------

def test_snapshot_mode_accepts_allowed_and_rejects_junk():
    assert SourceSpec(source="x", kind="cdc", type="pg", db="d", table="t",
                      target_ns="x", target_table="t", db_host="h", snapshot_mode="no_data").snapshot_mode == "no_data"
    with pytest.raises(ValidationError):
        SourceSpec(source="x", kind="cdc", type="pg", db="d", table="t",
                   target_ns="x", target_table="t", db_host="h", snapshot_mode="bogus")


# --------------------------------------------------------------------------
# signal_data_collection / create_stopped (Task 2, snapshot-lifecycle)
# --------------------------------------------------------------------------

def test_signal_data_collection_and_create_stopped_defaults():
    s = SourceSpec(source="x", kind="cdc", type="pg", db="d", table="t",
                   target_ns="x", target_table="t", db_host="h")
    assert s.signal_data_collection is None
    assert s.create_stopped is False


def test_signal_data_collection_and_create_stopped_overrides():
    s = SourceSpec(source="x", kind="cdc", type="pg", db="d", table="t",
                   target_ns="x", target_table="t", db_host="h",
                   signal_data_collection="public.debezium_signal", create_stopped=True)
    assert s.signal_data_collection == "public.debezium_signal"
    assert s.create_stopped is True


# --------------------------------------------------------------------------
# silver_bucket_count / silver_write_mode (Task 1, Silver-tuning)
# --------------------------------------------------------------------------

def test_silver_write_mode_validation():
    base = dict(source="s", kind="cdc", type="pg", db="d", table="t",
                target_ns="s", target_table="t", db_host="h:5432",
                identifier=["id"], columns=[{"name":"id","type":"int"}])
    SourceSpec(**base, silver_write_mode="merge-on-read")   # ok
    SourceSpec(**base, silver_write_mode="copy-on-write")   # ok
    with pytest.raises(ValidationError):
        SourceSpec(**base, silver_write_mode="weird")
    with pytest.raises(ValidationError):
        SourceSpec(**base, silver_bucket_count=0)
