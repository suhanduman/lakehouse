"""Tests for `GET /api/sources/{name}/ingest-config`: filled-in collector/
producer snippets + the producer credential for a kafka-ingest
(existing-Kafka) source.

`disposition`/`authoritative_fqn` are resolved the same way
`GET /api/sources/{name}/connectors` does -- by reusing `assemble_pipelines`
(the topology builder behind `GET /api/pipelines`), NOT by reading an
identifier/PK off the connector CR (the kafka-ingest connector config
carries no such thing -- see `app.routers.sources.ingest_config`'s
docstring). So `pk`/`expected_json` are best-effort `None` throughout; no
test here asserts a key-field hint appears in a snippet.

Fakes mirror `tests/test_sources_connectors.py` (`FakeK8s`/`FakeTrino` for
`assemble_pipelines`) and `tests/test_pipeline_topology.py`'s
`_kafka_ingest_cr` fixture shape (sink-only CR, `spec.config["topics"]` +
`transforms.route.static.value`, `lakehouse.solus.dev/source` annotation).
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Set

import pytest
from fastapi.testclient import TestClient

from app.deps import get_k8s, get_roles, get_trino
from app.main import app
from app.services.authz import Role

_SRC_ANN = "lakehouse.solus.dev/source"
_SINK_CLASS = "org.apache.iceberg.connect.IcebergSinkConnector"


def _kafka_ingest_cr(source: str, ns: str, table: str) -> Dict[str, Any]:
    """kafka-ingest: only the sink CR exists (mirrors
    test_pipeline_topology.py's `_kafka_ingest_cr` / test_sources_router.py's
    `KAFKA_INGEST_CR`) -- `metadata.name` MUST start with "kafka-ingest-" for
    `_kafka_ingest_topic`'s gate, and the `{_SRC_ANN}` annotation is what
    `_kafka_ingest_producer` recovers the bare source id from (NOT
    `metadata.name`, which is the composite CR name)."""
    return {
        "metadata": {"name": f"kafka-ingest-{source}-{table}", "annotations": {_SRC_ANN: source}},
        "spec": {
            "class": _SINK_CLASS,
            "config": {
                "topics": f"{table}-topic",
                "transforms.route.static.value": f"{ns}_raw.{table}",
            },
        },
        "status": {"connectorStatus": {"connector": {"state": "RUNNING"}}},
    }


# A non-kafka-ingest (CDC) source, so `_kafka_ingest_topic` returns None.
CDC_CR: Dict[str, Any] = {
    "metadata": {"name": "some-cdc-source", "annotations": {_SRC_ANN: "some-cdc-source"}},
    "spec": {
        "class": "io.debezium.connector.postgresql.PostgresConnector",
        "config": {
            "topic.prefix": "cdc.pg1",
            "table.include.list": "public.students",
            "transforms.route.static.value": "pg1_raw.students",
        },
    },
    "status": {"connectorStatus": {"connector": {"state": "RUNNING"}}},
}

# event kafka-ingest source: no Silver table for "nginx" -> disposition "event".
NGINX_CR = _kafka_ingest_cr(source="nginx", ns="nginx", table="logs")

# entity kafka-ingest source: Silver table "orders.orders" exists -> "entity".
ORDERS_CR = _kafka_ingest_cr(source="orders", ns="orders", table="orders")


class FakeK8s:
    """Minimal `K8sService` double: `list_sources()` + the two status
    callables `assemble_pipelines` needs, plus `read_secret` for the
    producer credential -- mirrors `test_sources_connectors.py`'s FakeK8s."""

    def __init__(self, sources: List[Dict[str, Any]], secret: Optional[Dict[str, str]] = None):
        self._sources = sources
        self._secret = secret

    def list_sources(self) -> List[Dict[str, Any]]:
        return self._sources

    def get_status(self, name: str) -> Dict[str, Any]:
        return {"connector": {"state": "RUNNING"}}

    def get_spark_status(self, name: str) -> Dict[str, Any]:
        return {"scheduleState": "Scheduled"}

    def read_secret(self, name: str) -> Optional[Dict[str, str]]:
        return self._secret


class FakeTrino:
    """`namespaces` source for `assemble_pipelines` -- `silver` here maps
    Silver namespace -> its table set, exactly what `GET /api/pipelines`'s
    real wiring builds from `list_namespaces`/`list_tables`."""

    def __init__(self, silver: Optional[Dict[str, Set[str]]] = None):
        self._silver = silver or {}

    def list_namespaces(self, catalog: str) -> List[str]:
        return list(self._silver.keys())

    def list_tables(self, catalog: str, ns: str) -> List[str]:
        return list(self._silver.get(ns, set()))


def _client(sources, silver=None, secret=None) -> TestClient:
    app.dependency_overrides[get_roles] = lambda: {Role.ADMIN}
    app.dependency_overrides[get_k8s] = lambda: FakeK8s(sources, secret=secret)
    app.dependency_overrides[get_trino] = lambda: FakeTrino(silver)
    return TestClient(app)


@pytest.fixture(autouse=True)
def _clear_overrides():
    yield
    app.dependency_overrides.clear()


def test_ingest_config_event_filled():
    client = _client([NGINX_CR], silver={}, secret={"password": "SECRET123"})

    resp = client.get(f"/api/sources/{NGINX_CR['metadata']['name']}/ingest-config")

    assert resp.status_code == 200
    body = resp.json()
    assert body["topic"] == "logs-topic"
    assert body["disposition"] == "event"
    assert body["authoritative_fqn"] == "rawlake.nginx_raw.logs"
    assert body["producer"]["user"] == "nginx-producer"
    assert body["producer"]["user"].endswith("producer")
    assert body["producer"]["mechanism"] == "SCRAM-SHA-512"
    assert body["producer"]["password"] == "SECRET123"
    assert body["producer"]["secret_ref"] == "nginx-producer"
    assert body["expected_json"] is None
    assert set(body["snippets"]) == {"fluentbit", "vector", "logstash", "generic"}
    assert "SECRET123" in body["snippets"]["fluentbit"]


def test_ingest_config_password_null_when_secret_absent():
    client = _client([NGINX_CR], silver={}, secret=None)

    resp = client.get(f"/api/sources/{NGINX_CR['metadata']['name']}/ingest-config")

    assert resp.status_code == 200
    body = resp.json()
    assert body["producer"]["password"] is None
    assert "<password" in body["snippets"]["fluentbit"]


def test_ingest_config_entity_marks_disposition():
    client = _client([ORDERS_CR], silver={"orders": {"orders"}}, secret={"password": "x"})

    resp = client.get(f"/api/sources/{ORDERS_CR['metadata']['name']}/ingest-config")

    assert resp.status_code == 200
    body = resp.json()
    assert body["disposition"] == "entity"
    assert body["authoritative_fqn"] == "lakehouse.orders.orders"


def test_ingest_config_400_for_non_kafka_ingest():
    client = _client([CDC_CR], silver={}, secret=None)

    resp = client.get(f"/api/sources/{CDC_CR['metadata']['name']}/ingest-config")

    assert resp.status_code == 400


def test_ingest_config_resolves_by_bare_source_id_same_as_composite_name():
    # FINAL-REVIEW Fix 1 regression guard: SourceDetail calls this route with
    # the connector's composite CR name (`kafka-ingest-k1-orders`), which
    # already worked. The Add-Source wizard instead calls
    # `getIngestConfig(form.source)` with the BARE source id ("k1") right
    # after create -- `_find_source`'s exact `metadata.name` match 404s on
    # that, so the on-create ingestion panel never rendered. Both paths must
    # now resolve the SAME kafka-ingest CR and return the same producer/topic.
    cr = _kafka_ingest_cr(source="k1", ns="orders", table="orders")
    client = _client([cr], silver={}, secret={"password": "SECRET123"})

    by_composite = client.get(f"/api/sources/{cr['metadata']['name']}/ingest-config")
    by_bare_id = client.get("/api/sources/k1/ingest-config")

    assert by_composite.status_code == 200
    assert by_bare_id.status_code == 200
    composite_body, bare_body = by_composite.json(), by_bare_id.json()
    assert composite_body["topic"] == bare_body["topic"] == "orders-topic"
    assert composite_body["producer"]["user"] == bare_body["producer"]["user"] == "k1-producer"
    assert composite_body["producer"]["password"] == bare_body["producer"]["password"] == "SECRET123"


def test_ingest_config_404_for_missing_source():
    client = _client([], silver={}, secret=None)

    resp = client.get("/api/sources/does-not-exist/ingest-config")

    assert resp.status_code == 404


def test_ingest_config_never_logs_password(caplog):
    """Regression guard: the producer password must never hit the app logs,
    even at DEBUG level, only ever the JSON response body."""
    client = _client([NGINX_CR], silver={}, secret={"password": "SUPERSECRET"})

    with caplog.at_level("DEBUG"):
        resp = client.get(f"/api/sources/{NGINX_CR['metadata']['name']}/ingest-config")

    assert resp.status_code == 200
    assert "SUPERSECRET" not in caplog.text
