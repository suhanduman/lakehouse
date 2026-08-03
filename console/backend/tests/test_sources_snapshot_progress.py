"""Tests for `GET /api/sources/{name}/snapshot-progress`: reads recent
Debezium sink-channel notifications off `settings.debezium_notification_topic`
(via `KafkaConsumerService.read_last_values` -- NOT `read_last`, which is
DLQ-specific and truncates the value) and returns this connector's Snapshot
progress, filtered by `topic.prefix`/name.

Mirrors `test_connectors_dlq.py`'s pattern: a real FastAPI `TestClient`, with
`get_roles`/`get_connect`/`get_kafka_consumer` swapped out via
`app.dependency_overrides` -- no real OIDC token, no live Connect worker, no
live Kafka broker.
"""

from __future__ import annotations

import json
from typing import Any, Dict, List

import pytest
from fastapi.testclient import TestClient

from app.deps import get_connect, get_kafka_consumer, get_roles
from app.main import app
from app.services.authz import Role

SOURCE_NAME = "cdc-mssql1"
TOPIC_PREFIX = "cdc.pgd"


class FakeConnect:
    def __init__(self, config: Dict[str, Any]) -> None:
        self._config = config

    def connector_config(self, name: str) -> Dict[str, Any]:
        return self._config


class FakeKafka:
    def __init__(self, values: List[Dict[str, Any]]) -> None:
        self._values = values
        self.read_last_values_called = False

    def read_last_values(self, topic: str, limit: int) -> List[Dict[str, Any]]:
        self.read_last_values_called = True
        return self._values


def _client(connect: FakeConnect, kafka: FakeKafka) -> TestClient:
    app.dependency_overrides[get_connect] = lambda: connect
    app.dependency_overrides[get_kafka_consumer] = lambda: kafka
    app.dependency_overrides[get_roles] = lambda: {Role.ADMIN}  # full-access role
    return TestClient(app)


@pytest.fixture(autouse=True)
def _clear_overrides():
    yield
    app.dependency_overrides.clear()


def _rec(value: Any, ts: int = 111) -> Dict[str, Any]:
    return {"ts": ts, "key": None, "value": json.dumps(value) if not isinstance(value, str) else value}


def test_snapshot_progress_filters_to_this_connector_and_snapshot_notifications():
    connect = FakeConnect({"topic.prefix": TOPIC_PREFIX})
    matching = _rec({
        "id": "1",
        "aggregate_type": "Snapshot",
        "type": "IN_PROGRESS",
        "additional_data": {
            "connector_name": TOPIC_PREFIX,
            "data_collections": "dbo.students",
            "scanned_rows": "500",
            "total_rows_scanned": "1000",
        },
        "timestamp": 999,
    }, ts=999)
    other_connector = _rec({
        "id": "2",
        "aggregate_type": "Snapshot",
        "type": "STARTED",
        "additional_data": {"connector_name": "some.other.connector"},
        "timestamp": 1000,
    })
    non_snapshot = _rec({
        "id": "3",
        "aggregate_type": "Initial Load",
        "type": "COMPLETED",
        "additional_data": {"connector_name": TOPIC_PREFIX},
        "timestamp": 1001,
    })
    unparseable = {"ts": 1002, "key": None, "value": "not-json{{{"}

    kafka = FakeKafka(values=[matching, other_connector, non_snapshot, unparseable])
    client = _client(connect, kafka)

    resp = client.get(f"/api/sources/{SOURCE_NAME}/snapshot-progress")
    assert resp.status_code == 200
    body = resp.json()
    assert kafka.read_last_values_called is True
    assert len(body["notifications"]) == 1
    n = body["notifications"][0]
    assert n == {"kind": "in_progress", "table": "dbo.students", "progress": "500/1000", "ts": 999}


def test_snapshot_progress_no_connector_name_is_lenient_and_included():
    connect = FakeConnect({"topic.prefix": TOPIC_PREFIX})
    no_connector_name = _rec({
        "id": "4",
        "aggregate_type": "Snapshot",
        "type": "COMPLETED",
        "additional_data": {"data_collections": "dbo.orders"},
        "timestamp": 2000,
    })
    kafka = FakeKafka(values=[no_connector_name])
    client = _client(connect, kafka)

    resp = client.get(f"/api/sources/{SOURCE_NAME}/snapshot-progress")
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["notifications"]) == 1
    assert body["notifications"][0]["kind"] == "completed"
    assert body["notifications"][0]["table"] == "dbo.orders"


def test_snapshot_progress_degrades_when_kafka_down():
    connect = FakeConnect({"topic.prefix": TOPIC_PREFIX})
    kafka = FakeKafka(values=[])  # simulates read_last_values() -> [] on Kafka-down
    client = _client(connect, kafka)

    resp = client.get(f"/api/sources/{SOURCE_NAME}/snapshot-progress")
    assert resp.status_code == 200
    assert resp.json() == {"notifications": []}


def test_snapshot_progress_degrades_when_connect_down():
    class BoomConnect:
        def connector_config(self, name: str) -> Dict[str, Any]:
            raise RuntimeError("connect unreachable")

    kafka = FakeKafka(values=[_rec({
        "aggregate_type": "Snapshot",
        "type": "STARTED",
        "additional_data": {},
        "timestamp": 1,
    })])
    client = _client(BoomConnect(), kafka)

    resp = client.get(f"/api/sources/{SOURCE_NAME}/snapshot-progress")
    assert resp.status_code == 200
    # topic.prefix unknown -> no connector_name to compare against, but the
    # record itself has no connector_name either -> lenient inclusion
    assert len(resp.json()["notifications"]) == 1


def test_snapshot_progress_requires_read_role():
    connect = FakeConnect({"topic.prefix": TOPIC_PREFIX})
    kafka = FakeKafka(values=[])
    app.dependency_overrides[get_connect] = lambda: connect
    app.dependency_overrides[get_kafka_consumer] = lambda: kafka
    app.dependency_overrides[get_roles] = lambda: set()  # no roles -> no READ
    client = TestClient(app, raise_server_exceptions=False)

    resp = client.get(f"/api/sources/{SOURCE_NAME}/snapshot-progress")
    assert resp.status_code == 403
