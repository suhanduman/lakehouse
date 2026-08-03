"""Tests for `GET /api/connectors/{name}/dlq`: resolves the DLQ topic from
the connector's config (`errors.deadletterqueue.topic.name`, sinks only) and
returns a count (via `KafkaConsumerService.topic_record_count`) + last-N
dead-letter records (via `KafkaConsumerService.read_last`).

Mirrors `test_connectors_router.py`'s pattern: a real FastAPI `TestClient`,
with `get_roles`/`get_connect`/`get_kafka_consumer` swapped out via
`app.dependency_overrides` -- no real OIDC token, no live Connect worker, no
live Kafka broker. `get_roles` returns a `Set[Role]` (see
`app.services.authz`), NOT a list of role-name strings.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

import pytest
from fastapi.testclient import TestClient

from app.deps import get_connect, get_kafka_consumer, get_roles
from app.main import app
from app.services.authz import Role


class FakeConnect:
    def __init__(self, config: Dict[str, Any]) -> None:
        self._config = config

    def connector_config(self, name: str) -> Dict[str, Any]:
        return self._config


class FakeKafka:
    def __init__(self, count: Optional[int], records: List[Dict[str, Any]]) -> None:
        self._count = count
        self._records = records
        self.read_last_called = False

    def topic_record_count(self, topic: str) -> Optional[int]:
        return self._count

    def read_last(self, topic: str, limit: int) -> List[Dict[str, Any]]:
        self.read_last_called = True
        return self._records


def _client(connect: FakeConnect, kafka: FakeKafka) -> TestClient:
    app.dependency_overrides[get_connect] = lambda: connect
    app.dependency_overrides[get_kafka_consumer] = lambda: kafka
    app.dependency_overrides[get_roles] = lambda: {Role.ADMIN}  # full-access role
    return TestClient(app)


@pytest.fixture(autouse=True)
def _clear_overrides():
    yield
    app.dependency_overrides.clear()


def test_dlq_returns_count_and_records_for_sink():
    connect = FakeConnect({"errors.deadletterqueue.topic.name": "s.dlq"})
    record = {
        "ts": 123, "error_class": "org.apache.kafka.connect.errors.DataException",
        "error_message": "boom", "source_topic": "s", "source_partition": 0,
        "source_offset": 42, "value_preview": "{}",
    }
    kafka = FakeKafka(count=3, records=[record])
    client = _client(connect, kafka)

    resp = client.get("/api/connectors/s/dlq?limit=50")
    assert resp.status_code == 200
    body = resp.json()
    assert body["has_dlq"] is True
    assert body["topic"] == "s.dlq"
    assert body["count"] == 3
    assert body["returned"] == 1
    assert body["records"][0]["error_class"]
    assert kafka.read_last_called is True


def test_dlq_limit_zero_is_count_only():
    connect = FakeConnect({"errors.deadletterqueue.topic.name": "s.dlq"})
    kafka = FakeKafka(count=3, records=[{"error_class": "x"}])
    client = _client(connect, kafka)

    resp = client.get("/api/connectors/s/dlq?limit=0")
    assert resp.status_code == 200
    body = resp.json()
    assert body["count"] == 3
    assert body["records"] == []
    assert body["returned"] == 0
    assert kafka.read_last_called is False


def test_dlq_no_dlq_connector_returns_hint():
    connect = FakeConnect({"connector.class": "io.debezium.connector.sqlserver.SqlServerConnector"})
    kafka = FakeKafka(count=None, records=[])
    client = _client(connect, kafka)

    resp = client.get("/api/connectors/src/dlq")
    assert resp.status_code == 200
    body = resp.json()
    assert body["has_dlq"] is False
    assert "sink" in body["hint"].lower()
    assert kafka.read_last_called is False


def test_dlq_degrades_when_kafka_down():
    connect = FakeConnect({"errors.deadletterqueue.topic.name": "s.dlq"})
    kafka = FakeKafka(count=None, records=[])
    client = _client(connect, kafka)

    resp = client.get("/api/connectors/s/dlq")
    assert resp.status_code == 200
    body = resp.json()
    assert body["count"] is None
    assert body["records"] == []


def test_dlq_requires_read_role():
    connect = FakeConnect({"errors.deadletterqueue.topic.name": "s.dlq"})
    kafka = FakeKafka(count=3, records=[])
    app.dependency_overrides[get_connect] = lambda: connect
    app.dependency_overrides[get_kafka_consumer] = lambda: kafka
    app.dependency_overrides[get_roles] = lambda: set()  # no roles -> no READ
    client = TestClient(app, raise_server_exceptions=False)

    resp = client.get("/api/connectors/s/dlq")
    assert resp.status_code == 403
