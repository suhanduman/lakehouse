"""Tests for `POST /api/sources/{name}/credentials` -- rotates a source's
DB/broker password by upserting its k8s Secret + restarting the connector.

The Console never opens a DB/broker connection here: the credential Secret
name is the source id (the CR's `lakehouse.solus.dev/source` round-trip
annotation), NOT the CR's own `metadata.name` -- same recovery `delete_source`
uses in `app.routers.sources` -- and Kafka Connect (already holding the
plugin + a DirectoryConfigProvider) is the one that actually re-reads the
new creds on restart.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

import pytest
from fastapi.testclient import TestClient

from app.deps import get_connect, get_k8s, get_roles
from app.main import app
from app.services.authz import Role

# A KafkaConnector CR carrying the `lakehouse.solus.dev/source` round-trip
# annotation render_service._connector stamps on every rendered connector --
# the credential Secret is named after THIS value ("pgdemo"), never the CR's
# own composite `metadata.name`.
PGDEMO_CR = {
    "metadata": {
        "name": "dbz-pgdemo-customers",
        "annotations": {"lakehouse.solus.dev/source": "pgdemo"},
    },
    "spec": {
        "class": "io.debezium.connector.postgresql.PostgresConnector",
        "config": {},
    },
    "status": {"connectorStatus": {"connector": {"state": "RUNNING"}}},
}

# A CR with no `lakehouse.solus.dev/source` annotation at all (e.g. applied
# by hand, or pre-dating the annotation) -- rotate_credentials must refuse
# with 400 rather than guess/derive a secret name from `metadata.name`.
NGINX_CR = {
    "metadata": {"name": "nginx"},
    "spec": {"class": "some.Connector", "config": {}},
    "status": {"connectorStatus": {"connector": {"state": "RUNNING"}}},
}


class FakeK8s:
    def __init__(self, sources: Optional[List[Dict[str, Any]]] = None):
        self._sources = sources if sources is not None else [PGDEMO_CR]
        self.created_secret: Optional[tuple] = None

    def list_sources(self):
        return self._sources

    def create_secret(self, name: str, data: Dict[str, str]) -> Dict[str, Any]:
        self.created_secret = (name, data)
        return {"ok": True}


class FakeConnect:
    def __init__(self, raise_on_restart: bool = False):
        self.raise_on_restart = raise_on_restart
        self.restarted_called = False
        self.restarted_with: Optional[str] = None

    def restart_connector(self, name: str, include_tasks: bool = True, only_failed: bool = False) -> None:
        self.restarted_called = True
        self.restarted_with = name
        if self.raise_on_restart:
            raise RuntimeError("connect unreachable")


def _client(roles, k8s: FakeK8s, connect: FakeConnect) -> TestClient:
    app.dependency_overrides[get_roles] = lambda: roles
    app.dependency_overrides[get_k8s] = lambda: k8s
    app.dependency_overrides[get_connect] = lambda: connect
    return TestClient(app)


@pytest.fixture(autouse=True)
def _clear_overrides():
    yield
    app.dependency_overrides.clear()


def test_rotate_upserts_secret_and_restarts():
    fake_k8s = FakeK8s([PGDEMO_CR])
    fake_connect = FakeConnect()
    client = _client({Role.ANALYST}, fake_k8s, fake_connect)

    resp = client.post(
        "/api/sources/dbz-pgdemo-customers/credentials",
        json={"user": "u2", "password": "p2"},
    )

    assert resp.status_code == 200
    body = resp.json()
    assert body["ok"] is True
    assert body["name"] == "dbz-pgdemo-customers"
    assert body["restarted"] is True
    assert body["note"] is None
    assert fake_k8s.created_secret == ("pgdemo", {"user": "u2", "pass": "p2"})
    assert fake_connect.restarted_called
    assert fake_connect.restarted_with == "dbz-pgdemo-customers"


def test_rotate_no_source_annotation_400():
    fake_k8s = FakeK8s([NGINX_CR])
    fake_connect = FakeConnect()
    client = _client({Role.ANALYST}, fake_k8s, fake_connect)

    resp = client.post("/api/sources/nginx/credentials", json={"user": "u", "password": "p"})

    assert resp.status_code == 400
    assert fake_k8s.created_secret is None
    assert not fake_connect.restarted_called


def test_rotate_restart_failure_reports_partial_success():
    """The Secret is still upserted even if Connect's restart call fails --
    partial success, never a 500, with a `note` telling the operator to
    restart manually."""
    fake_k8s = FakeK8s([PGDEMO_CR])
    fake_connect = FakeConnect(raise_on_restart=True)
    client = _client({Role.ANALYST}, fake_k8s, fake_connect)

    resp = client.post(
        "/api/sources/dbz-pgdemo-customers/credentials",
        json={"user": "u2", "password": "p2"},
    )

    assert resp.status_code == 200
    body = resp.json()
    assert body["ok"] is True
    assert body["restarted"] is False
    assert body["note"]
    assert fake_k8s.created_secret == ("pgdemo", {"user": "u2", "pass": "p2"})


def test_rotate_password_not_logged(caplog):
    fake_k8s = FakeK8s([PGDEMO_CR])
    fake_connect = FakeConnect()
    client = _client({Role.ANALYST}, fake_k8s, fake_connect)

    with caplog.at_level("DEBUG"):
        resp = client.post(
            "/api/sources/dbz-pgdemo-customers/credentials",
            json={"user": "u2", "password": "SUPERSECRETPW"},
        )

    assert resp.status_code == 200
    assert "SUPERSECRETPW" not in caplog.text


def test_rotate_requires_source_edit():
    fake_k8s = FakeK8s([PGDEMO_CR])
    fake_connect = FakeConnect()
    client = _client({Role.STUDENT}, fake_k8s, fake_connect)

    resp = client.post(
        "/api/sources/dbz-pgdemo-customers/credentials",
        json={"user": "u2", "password": "p2"},
    )

    assert resp.status_code == 403
    assert fake_k8s.created_secret is None
    assert not fake_connect.restarted_called
