"""Tests for `POST /api/sources/test-connection`: renders a minimal
connection-test config (`render_service.render_connection_test`) and asks
Kafka Connect to validate it (`ConnectService.validate_config`) -- the
Console itself never opens a DB connection. Mirrors
`test_sources_router.py`'s auth + fake-dependency-override pattern (real
`TestClient`, `app.dependency_overrides`, `get_roles` -> `Set[Role]`)."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

import pytest
from fastapi.testclient import TestClient

from app.deps import get_connect, get_roles
from app.main import app
from app.services.authz import Role

# cdc-pg: a connector-lane spec (render_connection_test returns non-None for
# it via the "cdc-relational" render_key).
PG_SPEC = {
    "source": "pgdemo",
    "kind": "cdc",
    "type": "pg",
    "db": "school",
    "table": "public.students",
    "target_ns": "pgdemo_ogrenci",
    "target_table": "students",
    "db_host": "pgdemo.internal",
}
PG_CREDENTIALS = {"user": "u", "password": "s3cret-p455word"}

# stream+kafka (kafka-ingest): consumes an already-existing topic -- no
# external DB/connector to test, so render_connection_test returns None (see
# its docstring). The only other None-returning lane, spark-batch, no longer
# has a registered source type.
KAFKA_SPEC = {
    "source": "k1",
    "kind": "stream",
    "type": "kafka",
    "db": "ext",
    "table": "orders-topic",
    "target_ns": "events",
    "target_table": "orders",
}


class FakeConnect:
    def __init__(self, result: Any = None, exc: Optional[Exception] = None):
        self.result = result if result is not None else {"error_count": 0, "configs": []}
        self.exc = exc
        self.calls: List[tuple] = []

    def validate_config(self, connector_class: str, config: Dict[str, Any]) -> Dict[str, Any]:
        self.calls.append((connector_class, config))
        if self.exc is not None:
            raise self.exc
        return self.result


def _client_as(roles, connect=None) -> TestClient:
    app.dependency_overrides[get_roles] = lambda: roles
    app.dependency_overrides[get_connect] = lambda: (connect if connect is not None else FakeConnect())
    return TestClient(app)


@pytest.fixture(autouse=True)
def _clear_overrides():
    yield
    app.dependency_overrides.clear()


def test_test_connection_ok():
    fake = FakeConnect(result={"error_count": 0, "configs": []})
    client = _client_as({Role.ADMIN}, connect=fake)

    r = client.post(
        "/api/sources/test-connection",
        json={"spec": PG_SPEC, "credentials": PG_CREDENTIALS},
    )

    assert r.status_code == 200
    body = r.json()
    assert body["applicable"] is True
    assert body["ok"] is True
    assert body["errors"] == []
    # creds actually got rendered into the config Connect validated.
    assert len(fake.calls) == 1
    _, config = fake.calls[0]
    assert config["database.user"] == "u"
    assert config["database.password"] == PG_CREDENTIALS["password"]


def test_test_connection_maps_errors():
    fake = FakeConnect(result={
        "error_count": 1,
        "configs": [{"value": {"name": "database.hostname", "errors": ["cannot connect"]}}],
    })
    client = _client_as({Role.ADMIN}, connect=fake)

    r = client.post(
        "/api/sources/test-connection",
        json={"spec": PG_SPEC, "credentials": PG_CREDENTIALS},
    )

    assert r.status_code == 200
    body = r.json()
    assert body["applicable"] is True
    assert body["ok"] is False
    assert body["errors"][0]["field"] == "database.hostname"
    assert "cannot connect" in body["errors"][0]["message"]


def test_test_connection_not_applicable_for_kafka_ingest():
    client = _client_as({Role.ADMIN})

    r = client.post(
        "/api/sources/test-connection",
        json={"spec": KAFKA_SPEC, "credentials": {"user": "", "password": ""}},
    )

    assert r.status_code == 200
    body = r.json()
    assert body["applicable"] is False
    assert "reason" in body


def test_test_connection_connect_down_is_200_not_500():
    fake = FakeConnect(exc=RuntimeError("connection refused"))
    client = _client_as({Role.ADMIN}, connect=fake)

    r = client.post(
        "/api/sources/test-connection",
        json={"spec": PG_SPEC, "credentials": PG_CREDENTIALS},
    )

    assert r.status_code == 200
    body = r.json()
    assert body["applicable"] is True
    assert body["ok"] is False
    assert body["errors"]
    # never logged/leaked creds in the degrade path either.
    assert PG_CREDENTIALS["password"] not in r.text


def test_test_connection_requires_source_edit():
    client = _client_as({Role.STUDENT})

    r = client.post(
        "/api/sources/test-connection",
        json={"spec": PG_SPEC, "credentials": PG_CREDENTIALS},
    )

    assert r.status_code == 403
