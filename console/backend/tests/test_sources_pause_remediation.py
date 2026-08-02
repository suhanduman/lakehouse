"""Tests for Task 5: gitops-mode pause/resume 409 carries a STRUCTURED
`remediation` recipe (not a bare string) in `detail`, so the frontend can
render concrete "do this in the pipeline repo / ArgoCD" steps.

No git write-path is added here -- gitops mode still fails loud with a 409;
only the shape of `detail` changes. Direct mode is unchanged (still performs
the real `k8s.set_paused`/`set_spark_suspended` write and returns 200).

Wiring mirrors `test_sources_router.py` exactly: `TestClient(app)` with
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


# --------------------------------------------------------------------------
# Fakes -- same shape as test_sources_router.py's FakeK8s/FakeOrchestrator
# (only the pieces pause/resume actually touch).
# --------------------------------------------------------------------------

class FakeK8s:
    def __init__(self, sources: Optional[List[Dict[str, Any]]] = None):
        self._sources = sources if sources is not None else []
        self.paused_calls: List[tuple] = []
        self.spark_suspended_calls: List[tuple] = []

    def list_sources(self):
        return self._sources

    def set_paused(self, name, paused):
        self.paused_calls.append((name, paused))

    def set_spark_suspended(self, name, suspended):
        self.spark_suspended_calls.append((name, suspended))


class FakeOrchestrator:
    def add_source(self, spec, creds):
        return AddSourceResult(steps=[StepResult(name="secret", ok=True)], ok=True)

    def edit_spark_source(self, spec):
        return {"ok": True}


# A KafkaConnector CR (pause/resume don't care about most fields).
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
# gitops mode -- structured remediation.
# --------------------------------------------------------------------------

def test_pause_in_gitops_returns_structured_remediation(monkeypatch):
    client = _gitops_client(monkeypatch, kind="KafkaConnector", name="students")

    resp = client.post("/api/sources/students/pause")

    assert resp.status_code == 409
    detail = resp.json()["detail"]
    assert detail["message"] == "pause not supported from the Console in gitops mode"
    rem = detail["remediation"]
    assert rem["where"] == "gitops"
    assert rem["field"] == "spec.state"
    assert rem["value"] == "paused"
    assert isinstance(rem["steps"], list) and rem["steps"]


def test_resume_in_gitops_returns_structured_remediation(monkeypatch):
    client = _gitops_client(monkeypatch, kind="KafkaConnector", name="students")

    resp = client.post("/api/sources/students/resume")

    assert resp.status_code == 409
    detail = resp.json()["detail"]
    assert detail["message"] == "resume not supported from the Console in gitops mode"
    rem = detail["remediation"]
    assert rem["where"] == "gitops"
    assert rem["field"] == "spec.state"
    assert rem["value"] == "running"
    assert isinstance(rem["steps"], list) and rem["steps"]


def test_pause_spark_remediation_uses_suspend(monkeypatch):
    client = _gitops_client(monkeypatch, kind="ScheduledSparkApplication", name="nightly-batch")

    resp = client.post("/api/sources/nightly-batch/pause")

    assert resp.status_code == 409
    rem = resp.json()["detail"]["remediation"]
    assert rem["field"] == "spec.suspend"
    assert rem["value"] is True


def test_resume_spark_remediation_uses_suspend_false(monkeypatch):
    client = _gitops_client(monkeypatch, kind="ScheduledSparkApplication", name="nightly-batch")

    resp = client.post("/api/sources/nightly-batch/resume")

    assert resp.status_code == 409
    rem = resp.json()["detail"]["remediation"]
    assert rem["field"] == "spec.suspend"
    assert rem["value"] is False


def test_remediation_repo_path_derived_from_gitops_settings(monkeypatch):
    monkeypatch.setattr(settings, "gitops_repo_url", "ssh://git@example/pipelines.git")
    monkeypatch.setattr(settings, "gitops_branch", "main")
    monkeypatch.setattr(settings, "gitops_path", "pipelines")
    client = _gitops_client(monkeypatch, kind="KafkaConnector", name="students")

    resp = client.post("/api/sources/students/pause")

    rem = resp.json()["detail"]["remediation"]
    assert rem["repo"] == "ssh://git@example/pipelines.git@main"
    assert rem["path"] == "pipelines/students"


def test_gitops_mode_never_calls_direct_k8s_write(monkeypatch):
    monkeypatch.setattr(settings, "deploy_mode", "gitops")
    k8s = FakeK8s(sources=[_connector_cr("students")])
    client = _client(k8s)

    client.post("/api/sources/students/pause")
    client.post("/api/sources/students/resume")

    assert k8s.paused_calls == []
    assert k8s.spark_suspended_calls == []


# --------------------------------------------------------------------------
# direct mode -- unchanged behavior (still performs the real k8s write).
# --------------------------------------------------------------------------

def test_pause_direct_mode_still_acts():
    client = _direct_client(name="students")

    resp = client.post("/api/sources/students/pause")

    assert resp.status_code == 200
    assert resp.json()["paused"] is True


def test_resume_direct_mode_still_acts():
    client = _direct_client(name="students")

    resp = client.post("/api/sources/students/resume")

    assert resp.status_code == 200
    assert resp.json()["paused"] is False
