"""B-v2 GitOps — render_pipeline_fileset (lane-aware, delegating to
render_service)."""
from __future__ import annotations

import pytest

from app.models import ColumnSpec, SourceSpec
from app.services import gitops_render


# --------------------------------------------------------------------------
# Fixtures — one SourceSpec per lane/disposition, mirroring test_render_service.py
# --------------------------------------------------------------------------

@pytest.fixture
def cdc_entity_spec():
    # cdc/pg entity: columns + identifier (PK) -> Bronze+Silver medallion.
    return SourceSpec(
        source="pgdemo", kind="cdc", type="pg", db="appdb", table="public.customers",
        target_ns="depo", target_table="customers", db_host="pgdemo",
        columns=[ColumnSpec(name="id", type="bigint"), ColumnSpec(name="name", type="varchar")],
        identifier=["id"],
    )


@pytest.fixture
def cdc_entity_spec_no_id():
    # cdc/pg entity without identifier and no fallback (pg has none) -> fail-loud.
    return SourceSpec(
        source="pgdemo2", kind="cdc", type="pg", db="appdb", table="public.orders",
        target_ns="depo2", target_table="orders", db_host="pgdemo",
        columns=[ColumnSpec(name="id", type="bigint")],
    )


@pytest.fixture
def event_spec():
    # stream/kafka: event-only disposition (dispositions=("event",)), no topic_key
    # (consumes an existing topic, creates none) -> Bronze only, no Silver.
    return SourceSpec(
        source="k1", kind="stream", type="kafka", db="ext", table="orders-topic",
        target_ns="events", target_table="orders",
    )


@pytest.fixture
def camel_http_spec():
    # stream/http: Camel Kafka SOURCE connector + its OWN Kafka topic + a
    # dedicated Iceberg sink connector (render_service.has_dedicated_sink).
    return SourceSpec(
        source="h1", kind="stream", type="http", db="-", table="-",
        target_ns="ext", target_table="prices", http_url="https://api.x/p", poll_ms=60000,
    )


def _files(spec):
    return gitops_render.render_pipeline_fileset(
        spec, spark_image="reg/spark-py:9", s3_secret_name="s3-credentials", namespace="lakehouse")


# --------------------------------------------------------------------------
# render_pipeline_fileset
# --------------------------------------------------------------------------

def test_cdc_entity_fileset_has_presync_precreate_topic_connector(cdc_entity_spec):
    files = _files(cdc_entity_spec)
    kinds = [m["kind"] for m in files.values()]
    assert kinds.count("ConfigMap") == 2 and kinds.count("Job") == 2   # Bronze+Silver PreSync
    assert "KafkaTopic" in kinds and "KafkaConnector" in kinds
    # PreSync + sync-wave stamped
    cms = [m for m in files.values() if m["kind"] == "ConfigMap"]
    assert all(m["metadata"]["annotations"]["argocd.argoproj.io/hook"] == "PreSync" for m in cms)
    # every manifest namespaced + paths under <source>/
    assert all(p.startswith(cdc_entity_spec.source + "/") for p in files)
    assert all(m["metadata"]["namespace"] == "lakehouse" for m in files.values())


def test_event_fileset_bronze_only_no_silver(event_spec):
    files = _files(event_spec)
    assert [m["kind"] for m in files.values()].count("ConfigMap") == 1   # Bronze only, no Silver


def test_camel_fileset_includes_dedicated_sink(camel_http_spec):
    files = _files(camel_http_spec)
    connectors = [m for m in files.values() if m["kind"] == "KafkaConnector"]
    assert len(connectors) == 2   # source + dedicated sink


def test_entity_fileset_fail_loud_without_identifier(cdc_entity_spec_no_id):
    with pytest.raises(ValueError):
        _files(cdc_entity_spec_no_id)
