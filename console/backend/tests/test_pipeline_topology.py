"""Tests for `app.services.pipeline_topology.assemble_pipelines` -- the pure,
injectable per-pipeline topology builder behind `GET /api/pipelines`.

One test per lane (entity CDC, event kafka-ingest, batch spark, event camel,
entity scheduled-jdbc) plus an unparseable-CR guard. Every CR fixture below
mirrors the real rendered shape from `render_service`/`test_sources_router.py`
(`transforms.route.static.value` = "<ns>_raw.<table>", the
`lakehouse.solus.dev/source` grouping annotation, `spec.class` for the
IcebergSinkConnector sink).

No k8s/service singletons anywhere here -- `assemble_pipelines` takes plain
lists/dicts plus two status callables, so these fakes are trivial functions,
never a FastAPI TestClient/dependency override.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from app.services import pipeline_topology as pt
from app.services import render_service

_SINK_CLASS = "org.apache.iceberg.connect.IcebergSinkConnector"
_SRC_ANN = f"{render_service.SPARK_ANNOTATION_PREFIX}source"


def _running(name: str) -> Dict[str, Any]:
    return {"connector": {"state": "RUNNING"}}


def _ok(name: str) -> Dict[str, Any]:
    return {"scheduleState": "Scheduled"}


def _ann(source: str) -> Dict[str, str]:
    return {_SRC_ANN: source}


def _route_value(ns: str, table: str) -> str:
    return f"{ns}{render_service.BRONZE_NAMESPACE_SUFFIX}.{table}"


# --------------------------------------------------------------------------
# CR fixture builders -- shaped like the real render_service output (see
# test_sources_router.py's CONNECTOR_CR/SPARK_CR/CAMEL_SINK_CR).
# --------------------------------------------------------------------------

def _cdc_pg_cr(source: str, ns: str, table: str) -> Dict[str, Any]:
    return {
        "metadata": {"name": f"dbz-{source}-{table}", "annotations": _ann(source)},
        "spec": {
            "class": "io.debezium.connector.postgresql.PostgresConnector",
            "config": {
                "topic.prefix": f"cdc.{source}",
                "table.include.list": f"public.{table}",
                "transforms.route.static.value": _route_value(ns, table),
            },
        },
        "status": {"connectorStatus": {"connector": {"state": "RUNNING"}}},
    }


def _sink_cr(source: str, name: str, ns: Optional[str] = None,
             table: Optional[str] = None) -> Dict[str, Any]:
    """A dedicated CDC/JDBC-family sink (render_cdc_sink's shape): carries
    `topics` (the topic it consumes) but -- per `_cdc_dedicated_sink_config`
    -- NEVER `transforms.route.static.value` (the route SMT runs on the
    SOURCE connector for this family, not the sink); ns/table are optional
    here on purpose so tests can build a "sink with no route info" CR."""
    config: Dict[str, Any] = {}
    if ns is not None and table is not None:
        config["topics"] = f"cdc.{source}.public.{table}"
    return {
        "metadata": {"name": name, "annotations": _ann(source)},
        "spec": {"class": _SINK_CLASS, "config": config},
        "status": {"connectorStatus": {"connector": {"state": "RUNNING"}}},
    }


def _kafka_ingest_cr(source: str, ns: str, table: str) -> Dict[str, Any]:
    """kafka-ingest: ONLY the sink CR exists (no dedicated source connector)
    -- it reads a pre-existing customer topic straight off `config["topics"]`
    and writes Bronze via the medallion SMT chain (`_iceberg_sink_config`),
    which DOES stamp `transforms.route.static.value` (mirrors
    render_service._render_kafka_ingest)."""
    return {
        "metadata": {"name": f"kafka-ingest-{source}-{table}", "annotations": _ann(source)},
        "spec": {
            "class": _SINK_CLASS,
            "config": {
                "topics": f"{table}-topic",
                "transforms.route.static.value": _route_value(ns, table),
            },
        },
        "status": {"connectorStatus": {"connector": {"state": "RUNNING"}}},
    }


def _spark_cr(source: str, ns: str, table: str, bucket: str, prefix: str) -> Dict[str, Any]:
    return {
        "kind": "ScheduledSparkApplication",
        "metadata": {
            "name": f"s3-register-{source}-{table}",
            "annotations": {
                f"{render_service.SPARK_ANNOTATION_PREFIX}source": source,
                f"{render_service.SPARK_ANNOTATION_PREFIX}target-ns": ns,
                f"{render_service.SPARK_ANNOTATION_PREFIX}target-table": table,
                f"{render_service.SPARK_ANNOTATION_PREFIX}s3-bucket": bucket,
                f"{render_service.SPARK_ANNOTATION_PREFIX}s3-prefix": prefix,
                f"{render_service.SPARK_ANNOTATION_PREFIX}file-format": "parquet",
                f"{render_service.SPARK_ANNOTATION_PREFIX}cron": "0 * * * *",
            },
        },
        "spec": {
            "suspend": False,
            "template": {
                "arguments": ["--bucket", bucket, "--prefix", prefix, "--format", "parquet",
                              "--target", f"rawlake.{ns}.{table}"],
            },
        },
        "status": {"scheduleState": "Scheduled"},
    }


def _camel_source_cr(source: str, table: str, topic: str) -> Dict[str, Any]:
    """Camel http-source (render_service._render_camel_http's shape): raw
    byte[] body -> its own topic, via a plain `topics` key (NOT the
    topic.prefix/table.include.list pattern `_topic_from_config` parses --
    Camel sources have no CDC-style topic.prefix at all)."""
    return {
        "metadata": {"name": f"http-{source}-{table}", "annotations": _ann(source)},
        "spec": {
            "class": "org.apache.camel.kafkaconnector.httpsource.CamelHttpsourceSourceConnector",
            "config": {"topics": topic, "camel.kamelet.http-source.url": "http://x"},
        },
        "status": {"connectorStatus": {"connector": {"state": "RUNNING"}}},
    }


def _camel_sink_cr(source: str, name: str, ns: str, table: str, topic: str) -> Dict[str, Any]:
    """Camel dedicated sink (render_camel_sink's shape): same IcebergSinkConnector
    class + `_iceberg_sink_config`, so it DOES carry `transforms.route.static.value`
    (unlike the CDC-family sink)."""
    return {
        "metadata": {"name": name, "annotations": _ann(source)},
        "spec": {
            "class": _SINK_CLASS,
            "config": {
                "topics": topic,
                "transforms.route.static.value": _route_value(ns, table),
            },
        },
        "status": {"connectorStatus": {"connector": {"state": "RUNNING"}}},
    }


def _jdbc_cr(source: str, ns: str, table: str) -> Dict[str, Any]:
    """Aiven scheduled-JDBC source connector (render_service._render_scheduled_jdbc's
    shape): DOES carry transforms.route.static.value (the route SMT runs here,
    same family as CDC)."""
    return {
        "metadata": {"name": f"jdbc-{source}-{table}", "annotations": _ann(source)},
        "spec": {
            "class": "io.aiven.connect.jdbc.JdbcSourceConnector",
            "config": {
                "topic.prefix": f"jdbc.{source}.",
                "table.whitelist": table,
                "transforms.route.static.value": _route_value(ns, table),
            },
        },
        "status": {"connectorStatus": {"connector": {"state": "RUNNING"}}},
    }


# --------------------------------------------------------------------------
# Lane tests
# --------------------------------------------------------------------------

def test_entity_cdc_pipeline_full_chain():
    src = _cdc_pg_cr(source="pgdemo", ns="pgdemo", table="customers")
    sink = _sink_cr(source="pgdemo", name="dbz-pgdemo-customers-sink")
    ns = {"pgdemo": {"customers"}}  # Silver table exists -> entity

    [p] = pt.assemble_pipelines([src, sink], ns, get_status=_running, get_spark_status=_ok)

    assert p["disposition"] == "entity"
    assert p["authoritative"] == {"fqn": "lakehouse.pgdemo.customers", "layer": "silver"}
    types = [n["type"] for n in p["nodes"]]
    assert types == ["source", "connector", "topic", "sink", "bronze", "merge", "silver", "buckets"]
    assert p["owned_tables"] == ["rawlake.pgdemo_raw.customers", "lakehouse.pgdemo.customers"]
    assert p["owned_buckets"] == [
        render_service.bronze_bucket_name("pgdemo"),
        render_service.silver_bucket_name("pgdemo"),
    ]
    assert "error" not in p


def test_event_kafka_ingest_no_merge_no_silver():
    sink = _kafka_ingest_cr(source="nginxlog", ns="nginxlog", table="logs")
    ns = {"nginxlog_raw": {"logs"}}  # no Silver namespace "nginxlog" -> event

    [p] = pt.assemble_pipelines([sink], ns, get_status=_running, get_spark_status=_ok)

    assert p["disposition"] == "event"
    assert p["authoritative"] == {"fqn": "rawlake.nginxlog_raw.logs", "layer": "bronze"}
    types = [n["type"] for n in p["nodes"]]
    assert "merge" not in types and "silver" not in types
    assert p["owned_buckets"] == [render_service.bronze_bucket_name("nginxlog")]
    assert p["owned_tables"] == ["rawlake.nginxlog_raw.logs"]
    # the sink IS the connector for kafka-ingest -- no separate connector CR.
    assert "connector" not in types
    assert "topic" in types and "sink" in types


def test_batch_spark_short_chain():
    cr = _spark_cr(source="batchdemo", ns="batchdemo", table="sensors", bucket="raw", prefix="in/")

    [p] = pt.assemble_pipelines([cr], {}, get_status=_running, get_spark_status=_ok)

    assert p["disposition"] == "batch"
    assert p["authoritative"] == {"fqn": "rawlake.batchdemo.sensors", "layer": "rawlake"}
    types = [n["type"] for n in p["nodes"]]
    assert types == ["source", "connector", "bronze", "buckets"]  # no topic/sink/merge/silver
    assert p["owned_tables"] == ["rawlake.batchdemo.sensors"]
    assert "error" not in p


def test_camel_event_pipeline():
    src = _camel_source_cr(source="h1", table="prices", topic="http.h1.prices")
    sink = _camel_sink_cr(source="h1", name="http-h1-prices-sink", ns="h1", table="prices",
                          topic="http.h1.prices")
    ns: Dict[str, Any] = {}  # no Silver namespace -> event

    [p] = pt.assemble_pipelines([src, sink], ns, get_status=_running, get_spark_status=_ok)

    assert p["disposition"] == "event"
    assert p["authoritative"] == {"fqn": "rawlake.h1_raw.prices", "layer": "bronze"}
    types = [n["type"] for n in p["nodes"]]
    assert types == ["source", "connector", "topic", "sink", "bronze", "buckets"]
    # topic is recovered even though Camel stores it under a plain "topics"
    # key rather than the topic.prefix/table.include.list pattern.
    topic_node = next(n for n in p["nodes"] if n["type"] == "topic")
    assert topic_node["name"] == "http.h1.prices"
    assert p["owned_buckets"] == [render_service.bronze_bucket_name("h1")]


def test_scheduled_jdbc_entity_pipeline():
    src = _jdbc_cr(source="jdbcx", ns="jdbcx", table="orders")
    sink = _sink_cr(source="jdbcx", name="jdbc-jdbcx-orders-sink")
    ns = {"jdbcx": {"orders"}}  # Silver table exists -> entity

    [p] = pt.assemble_pipelines([src, sink], ns, get_status=_running, get_spark_status=_ok)

    assert p["disposition"] == "entity"
    assert p["authoritative"] == {"fqn": "lakehouse.jdbcx.orders", "layer": "silver"}
    types = [n["type"] for n in p["nodes"]]
    assert types == ["source", "connector", "topic", "sink", "bronze", "merge", "silver", "buckets"]
    assert p["owned_tables"] == ["rawlake.jdbcx_raw.orders", "lakehouse.jdbcx.orders"]


def test_unparseable_cr_yields_error_not_raise():
    [p] = pt.assemble_pipelines(
        [{"kind": "KafkaConnector", "metadata": {"name": "x", "annotations": {}}, "spec": {}}],
        {}, _running, _ok,
    )
    assert "error" in p
    assert p["nodes"] == []
    assert p["name"] == "x"
    assert p["cr_kind"] == "KafkaConnector"


def test_status_lookup_failure_yields_unknown_not_raise():
    # get_status/get_spark_status must never propagate -- a broken status
    # call degrades to "unknown", it never turns the whole list into a 500.
    def _boom(name: str) -> Dict[str, Any]:
        raise RuntimeError("connect API down")

    src = _cdc_pg_cr(source="pgdemo", ns="pgdemo", table="customers")
    sink = _sink_cr(source="pgdemo", name="dbz-pgdemo-customers-sink")
    ns = {"pgdemo": {"customers"}}

    [p] = pt.assemble_pipelines([src, sink], ns, get_status=_boom, get_spark_status=_boom)

    assert "error" not in p
    connector_node = next(n for n in p["nodes"] if n["type"] == "connector")
    assert connector_node["state"] == "unknown"
    merge_node = next(n for n in p["nodes"] if n["type"] == "merge")
    assert merge_node["state"] == "unknown"


def test_multiple_pipelines_grouped_independently():
    entity_src = _cdc_pg_cr(source="pgdemo", ns="pgdemo", table="customers")
    entity_sink = _sink_cr(source="pgdemo", name="dbz-pgdemo-customers-sink")
    batch_cr = _spark_cr(source="batchdemo", ns="batchdemo", table="sensors", bucket="raw", prefix="in/")

    pipelines = pt.assemble_pipelines(
        [entity_src, entity_sink, batch_cr], {"pgdemo": {"customers"}},
        get_status=_running, get_spark_status=_ok,
    )

    assert {p["name"] for p in pipelines} == {"pgdemo", "batchdemo"}
    assert len(pipelines) == 2
