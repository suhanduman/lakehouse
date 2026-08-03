"""Tests for Task 5: `POST /api/sources/{name}/snapshot` (+ `/snapshot/stop`)
-- produces a Debezium `execute-snapshot`/`stop-snapshot` Kafka signal. The
Console never touches the source DB itself; it only produces a signal via
`KafkaProducerService.send`.

Wiring mirrors `test_sources_enable_snapshots.py`: `TestClient(app)` with
`app.dependency_overrides` swapping `get_roles`/`get_connect`/
`get_kafka_producer` -- `get_connect`'s fake returns a live-connector-config
dict, `get_kafka_producer`'s fake records `.sent` (topic, key, value) tuples.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

import pytest
from fastapi.testclient import TestClient

from app.deps import get_connect, get_kafka_producer, get_roles
from app.main import app
from app.services.authz import Role


class FakeConnect:
    def __init__(self, config: Optional[Dict[str, Any]] = None, raises: bool = False):
        self._config = config if config is not None else {}
        self._raises = raises

    def connector_config(self, name: str) -> Dict[str, Any]:
        if self._raises:
            raise RuntimeError("connector not found")
        return self._config


class FakeProducer:
    def __init__(self, send_result: bool = True):
        self.sent: List[tuple] = []
        self._send_result = send_result

    def send(self, topic: str, key: str, value: Dict[str, Any]) -> bool:
        self.sent.append((topic, key, value))
        return self._send_result


PGD_CONFIG = {
    "topic.prefix": "cdc.pgd",
    "table.include.list": "public.customers",
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
}

PGD_CONFIG_WITH_SIGNAL_TABLE = {
    **PGD_CONFIG,
    "signal.data.collection": "public.debezium_signal",
}


def _client(connect: FakeConnect, producer: FakeProducer) -> TestClient:
    app.dependency_overrides[get_roles] = lambda: {Role.ADMIN}
    app.dependency_overrides[get_connect] = lambda: connect
    app.dependency_overrides[get_kafka_producer] = lambda: producer
    return TestClient(app)


@pytest.fixture(autouse=True)
def _clear_overrides():
    yield
    app.dependency_overrides.clear()


def test_snapshot_incremental_produces_signal_with_prefix_key():
    fake_producer = FakeProducer()
    client = _client(FakeConnect(PGD_CONFIG_WITH_SIGNAL_TABLE), fake_producer)

    resp = client.post(
        "/api/sources/dbz-pgd-customers/snapshot",
        json={"tables": ["public.customers"], "type": "incremental"},
    )

    assert resp.status_code == 200 and resp.json()["ok"] is True
    topic, key, value = fake_producer.sent[0]
    assert topic == "debezium-signals" and key == "cdc.pgd"
    assert value["type"] == "execute-snapshot"
    assert value["data"]["type"] == "INCREMENTAL" and value["data"]["data-collections"] == ["public.customers"]


def test_snapshot_incremental_without_signal_table_returns_needs_table():
    # connector_config has NO signal.data.collection
    client = _client(FakeConnect(PGD_CONFIG), FakeProducer())

    body = client.post("/api/sources/x/snapshot", json={"type": "incremental"}).json()

    assert body["needs_signal_table"] is True and "CREATE TABLE" in body["dml"]


def test_snapshot_blocking_ok_without_signal_table():
    fake_producer = FakeProducer()
    client = _client(FakeConnect(PGD_CONFIG), fake_producer)

    body = client.post("/api/sources/x/snapshot", json={"type": "blocking"}).json()

    assert body["ok"] is True and fake_producer.sent[0][2]["data"]["type"] == "BLOCKING"


def test_snapshot_degrades_when_produce_fails():
    # producer.send -> False
    client = _client(FakeConnect(PGD_CONFIG), FakeProducer(send_result=False))

    assert client.post("/api/sources/x/snapshot", json={"type": "blocking"}).json()["ok"] is False


def test_snapshot_requires_source_edit():
    app.dependency_overrides[get_roles] = lambda: {Role.STUDENT}
    app.dependency_overrides[get_connect] = lambda: FakeConnect(PGD_CONFIG)
    app.dependency_overrides[get_kafka_producer] = lambda: FakeProducer()
    client = TestClient(app)

    resp = client.post("/api/sources/x/snapshot", json={"type": "blocking"})

    assert resp.status_code == 403


# --------------------------------------------------------------------------
# Degrade-never-500 edge cases beyond the brief's 5 transcribed tests.
# --------------------------------------------------------------------------

def test_snapshot_degrades_when_connector_config_raises():
    client = _client(FakeConnect(raises=True), FakeProducer())

    resp = client.post("/api/sources/x/snapshot", json={"type": "blocking"})

    assert resp.status_code == 200 and resp.json()["ok"] is False


def test_snapshot_degrades_when_topic_prefix_missing():
    client = _client(FakeConnect({"table.include.list": "public.customers"}), FakeProducer())

    resp = client.post("/api/sources/x/snapshot", json={"type": "blocking"})

    assert resp.status_code == 200 and resp.json()["ok"] is False


# --------------------------------------------------------------------------
# /snapshot/stop -- same resolution, stop-snapshot signal.
# --------------------------------------------------------------------------

def test_snapshot_stop_produces_stop_signal_with_prefix_key():
    fake_producer = FakeProducer()
    client = _client(FakeConnect(PGD_CONFIG), fake_producer)

    resp = client.post(
        "/api/sources/dbz-pgd-customers/snapshot/stop",
        json={"tables": ["public.customers"], "type": "incremental"},
    )

    assert resp.status_code == 200 and resp.json()["ok"] is True
    topic, key, value = fake_producer.sent[0]
    assert topic == "debezium-signals" and key == "cdc.pgd"
    assert value["type"] == "stop-snapshot"
    assert value["data"]["data-collections"] == ["public.customers"]


def test_snapshot_stop_requires_source_edit():
    app.dependency_overrides[get_roles] = lambda: {Role.STUDENT}
    app.dependency_overrides[get_connect] = lambda: FakeConnect(PGD_CONFIG)
    app.dependency_overrides[get_kafka_producer] = lambda: FakeProducer()
    client = TestClient(app)

    resp = client.post("/api/sources/x/snapshot/stop", json={"type": "blocking"})

    assert resp.status_code == 403
