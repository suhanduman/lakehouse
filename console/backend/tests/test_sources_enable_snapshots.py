"""Tests for Task 7: `POST /api/sources/{name}/enable-snapshots` -- retrofits
an EXISTING CDC connector with the Kafka signal channel + notification sink
config (direct mode), or returns a structured gitops remediation (409).

Wiring mirrors `test_sources_pause_remediation.py`: `TestClient(app)` with
`app.dependency_overrides` swapping `get_roles`/`get_k8s`/`get_s3`/
`get_trino`/`get_orchestrator`, and `monkeypatch.setattr(settings,
"deploy_mode", ...)` to flip gitops/direct mode.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

import pytest
from fastapi.testclient import TestClient

from app.config import settings
from app.deps import get_k8s, get_orchestrator, get_roles, get_s3, get_trino
from app.main import app
from app.orchestrator import AddSourceResult, StepResult
from app.services.authz import Role


class FakeK8s:
    def __init__(self, sources: Optional[List[Dict[str, Any]]] = None):
        self._sources = sources if sources is not None else []
        self.patched: List[tuple] = []

    def list_sources(self):
        return self._sources

    def patch_connector(self, name, config):
        self.patched.append((name, config))


class FakeOrchestrator:
    def add_source(self, spec, creds):
        return AddSourceResult(steps=[StepResult(name="secret", ok=True)], ok=True)

    def edit_spark_source(self, spec):
        return {"ok": True}


def _connector_cr(name: str) -> Dict[str, Any]:
    return {
        "metadata": {"name": name},
        "spec": {"class": "io.debezium.connector.postgresql.PostgresConnector", "config": {}},
        "status": {"connectorStatus": {"connector": {"state": "RUNNING"}}},
    }


def _spark_cr(name: str) -> Dict[str, Any]:
    return {
        "kind": "ScheduledSparkApplication",
        "metadata": {"name": name, "annotations": {}},
        "spec": {"suspend": False, "template": {"arguments": []}},
        "status": {"scheduleState": "Scheduled"},
    }


def _client(k8s: FakeK8s) -> TestClient:
    app.dependency_overrides[get_roles] = lambda: {Role.ADMIN}
    app.dependency_overrides[get_k8s] = lambda: k8s
    app.dependency_overrides[get_s3] = lambda: object()
    app.dependency_overrides[get_trino] = lambda: object()
    app.dependency_overrides[get_orchestrator] = lambda: FakeOrchestrator()
    return TestClient(app)


@pytest.fixture(autouse=True)
def _clear_overrides():
    yield
    app.dependency_overrides.clear()


def _direct_client(name: str) -> TestClient:
    # settings.deploy_mode defaults to "direct" (no monkeypatch needed).
    return _client(FakeK8s(sources=[_connector_cr(name)]))


def _gitops_client(monkeypatch, name: str) -> TestClient:
    monkeypatch.setattr(settings, "deploy_mode", "gitops")
    return _client(FakeK8s(sources=[_connector_cr(name)]))


# --------------------------------------------------------------------------
# direct mode -- patches the connector config in place.
# --------------------------------------------------------------------------

def test_enable_snapshots_direct_mode_patches_signal_config():
    k8s = FakeK8s(sources=[_connector_cr("students")])
    client = _client(k8s)

    resp = client.post("/api/sources/students/enable-snapshots")

    assert resp.status_code == 200
    body = resp.json()
    assert body == {"ok": True, "name": "students", "snapshots_enabled": True}
    assert len(k8s.patched) == 1
    patched_name, patched_config = k8s.patched[0]
    assert patched_name == "students"
    assert "kafka" in patched_config["signal.enabled.channels"]
    assert patched_config["signal.kafka.topic"] == "debezium-signals"
    assert patched_config["notification.enabled.channels"] == "sink"
    assert patched_config["notification.sink.topic.name"] == "debezium-notifications"
    # signal.data.collection is intentionally NOT part of this patch -- the
    # operator sets the signaling table separately (render_signal_table_dml).
    assert "signal.data.collection" not in patched_config


def test_enable_snapshots_direct_mode_does_not_leak_credentials_in_response():
    client = _direct_client(name="students")

    resp = client.post("/api/sources/students/enable-snapshots")

    assert resp.status_code == 200
    # The response body is the small {"ok", "name", "snapshots_enabled"}
    # shape -- never the patch config (which carries sasl.jaas.config).
    assert set(resp.json().keys()) == {"ok", "name", "snapshots_enabled"}


# --------------------------------------------------------------------------
# gitops mode -- structured remediation, no direct write.
# --------------------------------------------------------------------------

def test_enable_snapshots_in_gitops_returns_structured_remediation(monkeypatch):
    client = _gitops_client(monkeypatch, name="students")

    resp = client.post("/api/sources/students/enable-snapshots")

    assert resp.status_code == 409
    detail = resp.json()["detail"]
    assert "enable-snapshots not supported from the Console in gitops mode" in detail["message"]
    rem = detail["remediation"]
    assert rem["where"] == "gitops"
    assert rem["field"] == "spec.state"
    assert rem["value"] == "running"
    assert isinstance(rem["steps"], list) and rem["steps"]


def test_enable_snapshots_gitops_mode_never_calls_direct_k8s_write(monkeypatch):
    k8s = FakeK8s(sources=[_connector_cr("students")])
    monkeypatch.setattr(settings, "deploy_mode", "gitops")
    client = _client(k8s)

    client.post("/api/sources/students/enable-snapshots")

    assert k8s.patched == []


# --------------------------------------------------------------------------
# spark sources -- not a CDC connector, 400 regardless of deploy_mode.
# --------------------------------------------------------------------------

def test_enable_snapshots_on_spark_source_is_400():
    k8s = FakeK8s(sources=[_spark_cr("nightly-batch")])
    client = _client(k8s)

    resp = client.post("/api/sources/nightly-batch/enable-snapshots")

    assert resp.status_code == 400


def test_enable_snapshots_on_missing_source_is_404():
    client = _direct_client(name="students")

    resp = client.post("/api/sources/does-not-exist/enable-snapshots")

    assert resp.status_code == 404
