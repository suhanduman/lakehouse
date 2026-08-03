"""Tests for Task 8: connector start/stop lifecycle (`POST /{name}/start`,
`POST /{name}/stop`) -- generalizes pause/resume's `set_paused` into
`K8sService.set_state` and adds a full stop (spec.state="stopped", distinct
from "paused") alongside pause/resume.

Mirrors `test_sources_pause_remediation.py` exactly: `TestClient(app)` with
`app.dependency_overrides` swapping `get_roles`/`get_k8s`/`get_s3`/
`get_trino`/`get_orchestrator`, and `monkeypatch.setattr(settings,
"deploy_mode", ...)` to flip gitops/direct mode. Same gitops fail-loud 409 +
structured `remediation` guard as pause/resume -- neither CR kind has gitops
routing for start/stop, so a direct `k8s.set_state`/`set_spark_suspended`
write in gitops mode would be silently reverted by ArgoCD selfHeal on its
next sync.
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


# --------------------------------------------------------------------------
# Fakes -- same shape as test_sources_pause_remediation.py's FakeK8s
# (extended with set_state recording).
# --------------------------------------------------------------------------

class FakeK8s:
    def __init__(self, sources: Optional[List[Dict[str, Any]]] = None):
        self._sources = sources if sources is not None else []
        self.state_calls: List[tuple] = []
        self.spark_suspended_calls: List[tuple] = []

    def list_sources(self):
        return self._sources

    def set_state(self, name, state):
        self.state_calls.append((name, state))

    def set_spark_suspended(self, name, suspended):
        self.spark_suspended_calls.append((name, suspended))


class FakeOrchestrator:
    def add_source(self, spec, creds):
        return AddSourceResult(steps=[StepResult(name="secret", ok=True)], ok=True)

    def edit_spark_source(self, spec):
        return {"ok": True}


# A KafkaConnector CR (start/stop don't care about most fields).
def _connector_cr(name: str) -> Dict[str, Any]:
    return {
        "metadata": {"name": name},
        "spec": {"class": "io.debezium.connector.postgresql.PostgresConnector", "config": {}},
        "status": {"connectorStatus": {"connector": {"state": "RUNNING"}}},
    }


# A ScheduledSparkApplication CR.
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


def _gitops_client(monkeypatch, kind: str, name: str) -> TestClient:
    monkeypatch.setattr(settings, "deploy_mode", "gitops")
    cr = _spark_cr(name) if kind == "ScheduledSparkApplication" else _connector_cr(name)
    return _client(FakeK8s(sources=[cr]))


def _direct_client(name: str) -> TestClient:
    # settings.deploy_mode defaults to "direct" (no monkeypatch needed).
    return _client(FakeK8s(sources=[_connector_cr(name)]))


# --------------------------------------------------------------------------
# direct mode -- performs the real k8s write.
# --------------------------------------------------------------------------

def test_stop_direct_mode_calls_set_state_stopped():
    client = _direct_client(name="students")

    resp = client.post("/api/sources/students/stop")

    assert resp.status_code == 200
    assert resp.json() == {"ok": True, "name": "students", "stopped": True}


def test_start_direct_mode_calls_set_state_running():
    client = _direct_client(name="students")

    resp = client.post("/api/sources/students/start")

    assert resp.status_code == 200
    assert resp.json() == {"ok": True, "name": "students", "stopped": False}


def test_stop_direct_mode_records_expected_k8s_call():
    k8s = FakeK8s(sources=[_connector_cr("students")])
    client = _client(k8s)

    client.post("/api/sources/students/stop")

    assert k8s.state_calls == [("students", "stopped")]
    assert k8s.spark_suspended_calls == []


def test_start_direct_mode_records_expected_k8s_call():
    k8s = FakeK8s(sources=[_connector_cr("students")])
    client = _client(k8s)

    client.post("/api/sources/students/start")

    assert k8s.state_calls == [("students", "running")]
    assert k8s.spark_suspended_calls == []


# --------------------------------------------------------------------------
# spark CR -- routes through set_spark_suspended, not set_state.
# --------------------------------------------------------------------------

def test_stop_spark_source_uses_set_spark_suspended_true():
    k8s = FakeK8s(sources=[_spark_cr("nightly-batch")])
    client = _client(k8s)

    resp = client.post("/api/sources/nightly-batch/stop")

    assert resp.status_code == 200
    assert resp.json() == {"ok": True, "name": "nightly-batch", "stopped": True}
    assert k8s.spark_suspended_calls == [("nightly-batch", True)]
    assert k8s.state_calls == []


def test_start_spark_source_uses_set_spark_suspended_false():
    k8s = FakeK8s(sources=[_spark_cr("nightly-batch")])
    client = _client(k8s)

    resp = client.post("/api/sources/nightly-batch/start")

    assert resp.status_code == 200
    assert resp.json() == {"ok": True, "name": "nightly-batch", "stopped": False}
    assert k8s.spark_suspended_calls == [("nightly-batch", False)]
    assert k8s.state_calls == []


# --------------------------------------------------------------------------
# gitops mode -- fails loud with a structured remediation, never calls k8s.
# --------------------------------------------------------------------------

def test_stop_in_gitops_returns_409_with_remediation(monkeypatch):
    client = _gitops_client(monkeypatch, kind="KafkaConnector", name="students")

    resp = client.post("/api/sources/students/stop")

    assert resp.status_code == 409
    detail = resp.json()["detail"]
    assert detail["message"] == "stop not supported from the Console in gitops mode"
    rem = detail["remediation"]
    assert rem["where"] == "gitops"
    assert isinstance(rem["steps"], list) and rem["steps"]


def test_start_in_gitops_returns_409_with_remediation(monkeypatch):
    client = _gitops_client(monkeypatch, kind="KafkaConnector", name="students")

    resp = client.post("/api/sources/students/start")

    assert resp.status_code == 409
    detail = resp.json()["detail"]
    assert detail["message"] == "start not supported from the Console in gitops mode"
    rem = detail["remediation"]
    assert rem["where"] == "gitops"
    assert isinstance(rem["steps"], list) and rem["steps"]


def test_gitops_mode_never_calls_direct_k8s_write_for_start_stop(monkeypatch):
    monkeypatch.setattr(settings, "deploy_mode", "gitops")
    k8s = FakeK8s(sources=[_connector_cr("students")])
    client = _client(k8s)

    client.post("/api/sources/students/stop")
    client.post("/api/sources/students/start")

    assert k8s.state_calls == []
    assert k8s.spark_suspended_calls == []


# --------------------------------------------------------------------------
# authz -- SOURCE_EDIT required, same as pause/resume. Role.STUDENT is the
# READ-only role (see app/services/authz.py's _MATRIX) -- no SOURCE_EDIT.
# --------------------------------------------------------------------------

def _readonly_client(name: str) -> TestClient:
    app.dependency_overrides[get_roles] = lambda: {Role.STUDENT}
    app.dependency_overrides[get_k8s] = lambda: FakeK8s(sources=[_connector_cr(name)])
    app.dependency_overrides[get_s3] = lambda: object()
    app.dependency_overrides[get_trino] = lambda: object()
    app.dependency_overrides[get_orchestrator] = lambda: FakeOrchestrator()
    return TestClient(app)


def test_stop_without_source_edit_role_is_403():
    client = _readonly_client(name="students")

    resp = client.post("/api/sources/students/stop")

    assert resp.status_code == 403


def test_start_without_source_edit_role_is_403():
    client = _readonly_client(name="students")

    resp = client.post("/api/sources/students/start")

    assert resp.status_code == 403
