import pytest
from app import source_types as st


def test_existing_pairs_registered():
    # The five source kinds that exist today must be present with correct lanes.
    expected = {
        ("cdc", "mssql"): ("debezium-cdc", "entity", "cdc-relational"),
        ("cdc", "pg"): ("debezium-cdc", "entity", "cdc-relational"),
        ("cdc", "mongo"): ("debezium-cdc", "entity", "cdc-mongo"),
        ("scheduled", "mssql"): ("kafka-connect-source", "entity", "scheduled-jdbc"),
        ("scheduled", "pg"): ("kafka-connect-source", "entity", "scheduled-jdbc"),
        ("scheduled", "mongo"): ("spark-batch", "entity", ""),
    }
    for (kind, typ), (lane, disp, render_key) in expected.items():
        d = st.get(kind, typ)
        assert d.lane == lane
        assert d.disposition == disp
        assert d.render_key == render_key


def test_required_fields_match_todays_validation():
    assert st.get("cdc", "mssql").required_fields == ("db_host",)
    assert st.get("cdc", "pg").required_fields == ("db_host",)
    assert st.get("cdc", "mongo").required_fields == ("mongo_uri",)
    assert st.get("scheduled", "mssql").required_fields == ("jdbc_url", "incrementing_col")
    assert st.get("scheduled", "mongo").required_fields == ("cron",)


def test_every_lane_and_disposition_is_valid():
    for d in st.all_types():
        assert d.lane in st.LANES
        assert d.disposition in st.DISPOSITIONS


def test_type_names_are_distinct_and_cover_existing():
    names = st.type_names()
    assert set(names) >= {"mssql", "pg", "mongo"}
    assert len(names) == len(set(names))


def test_get_unknown_raises():
    with pytest.raises(KeyError):
        st.get("cdc", "nope")


def test_stream_kafka_allows_event_and_entity():
    d = st.get("stream", "kafka")
    assert d.disposition == "event"                       # default when unset
    assert st.allowed_dispositions(d) == ("event", "entity")


def test_allowed_dispositions_defaults_to_single():
    d = st.get("cdc", "pg")
    assert st.allowed_dispositions(d) == ("entity",)   # from .disposition, no .dispositions


def test_batch_s3_registered_spark_batch():
    d = st.get("batch", "s3")
    assert d.lane == "spark-batch"
    assert d.render_key == "s3-register"
    assert d.topic_key == ""
    assert d.required_fields == ("s3_bucket", "s3_prefix", "file_format", "cron")


def test_b3_lanes_registered():
    for typ, disp in [("http", ("event", "entity")), ("mqtt", ("event",)), ("rabbitmq", ("event",))]:
        d = st.get("stream", typ)
        assert d.lane == "kafka-connect-source"
        assert d.render_key and d.topic_key           # they create a topic + a connector
        assert st.allowed_dispositions(d) == disp
