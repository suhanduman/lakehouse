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


def test_scheduled_mongo_requires_cron():
    with pytest.raises(ValidationError):
        SourceSpec(source="mongo1", kind="scheduled", type="mongo", db="ogrenciDb",
                   table="students", target_ns="mongo_ogrenci", target_table="students")


def test_valid_scheduled_mongo_with_cron_passes():
    s = SourceSpec(source="mongo1", kind="scheduled", type="mongo", db="ogrenciDb",
                   table="students", target_ns="mongo_ogrenci",
                   target_table="students", cron="0 * * * *")
    assert s.cron == "0 * * * *"


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
    with pytest.raises(ValidationError, match="disposition"):
        SourceSpec(**_kafka(disposition="entity"))   # stream-kafka allows only event in B1


def test_cdc_pg_still_entity():
    s = SourceSpec(**_base())          # from Plan A's test helper
    assert s.effective_disposition() == "entity"


def test_stream_non_kafka_type_is_unregistered():
    # Extending the `type` Literal to include "kafka" makes stream+mssql a
    # valid Literal combo, but it isn't registered -> registry KeyError path.
    with pytest.raises(ValidationError, match="unknown source"):
        SourceSpec(**_kafka(type="mssql"))
