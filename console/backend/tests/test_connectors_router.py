"""Tests for the `/api/connectors` router: `GET /{name}/debug` (surfaces the
per-task failure `trace` + a `build_logs_hint` recipe, degrading gracefully
when Connect is unreachable) and `POST /{name}/restart` (maps the request
body onto `ConnectService.restart_connector`'s REST call).

Mirrors `test_other_routers.py`/`test_sources_router.py`'s real pattern: a
real FastAPI `TestClient`, with `get_roles`/`get_connect` swapped out via
`app.dependency_overrides` -- no real OIDC token, no live Connect worker.
`get_roles` returns a `Set[Role]` (see `app.services.authz`), NOT a list of
role-name strings -- `Role.ADMIN` grants both `Action.READ` (debug) and
`Action.SOURCE_EDIT` (restart) per the `_MATRIX` in `app/services/authz.py`.
"""

from __future__ import annotations

import httpx
import pytest
from fastapi.testclient import TestClient

from app.deps import get_connect, get_roles
from app.main import app
from app.services.authz import Role
from app.services.connect_service import ConnectService


def _client(handler) -> TestClient:
    connect = ConnectService("http://c", httpx.Client(transport=httpx.MockTransport(handler)))
    app.dependency_overrides[get_connect] = lambda: connect
    app.dependency_overrides[get_roles] = lambda: {Role.ADMIN}  # full-access role
    return TestClient(app)


@pytest.fixture(autouse=True)
def _clear_overrides():
    yield
    app.dependency_overrides.clear()


def test_debug_surfaces_trace_and_logs_hint():
    def handler(request):
        assert request.url.path == "/connectors/dbz-students/status"
        return httpx.Response(200, json={
            "connector": {"state": "FAILED"},
            "tasks": [{"id": 0, "state": "FAILED", "worker_id": "10.0.0.1:8083",
                       "trace": "org.apache.kafka.connect.errors.ConnectException: boom"}],
        })

    client = _client(handler)
    resp = client.get("/api/connectors/dbz-students/debug")
    assert resp.status_code == 200
    body = resp.json()
    assert body["state"] == "FAILED"
    assert body["tasks"][0]["trace"].startswith("org.apache.kafka")
    assert body["tasks"][0]["worker_id"] == "10.0.0.1:8083"
    assert "dbz-students" in body["logs_hint"]["search_terms"]
    assert "task-0" in body["logs_hint"]["search_terms"]


def test_debug_degrades_when_connect_unreachable():
    def handler(request):
        raise httpx.ConnectError("connection refused")

    client = _client(handler)
    resp = client.get("/api/connectors/dbz-students/debug")
    assert resp.status_code == 200
    body = resp.json()
    assert body["state"] == "unknown"
    assert body["tasks"] == []
    # static recipe still present
    assert body["logs_hint"]["oc_command"].startswith("oc logs")


def test_restart_maps_body_to_service():
    seen = {}

    def handler(request):
        seen["path"] = request.url.path
        seen["query"] = dict(request.url.params)
        return httpx.Response(204)

    client = _client(handler)
    resp = client.post("/api/connectors/dbz-students/restart",
                        json={"only_failed": True})
    assert resp.status_code == 200
    assert resp.json()["ok"] is True
    assert seen["path"] == "/connectors/dbz-students/restart"
    assert seen["query"]["onlyFailed"] == "true"
    assert seen["query"]["includeTasks"] == "true"


def test_debug_requires_read_role():
    def handler(request):
        raise AssertionError("must not reach Connect -- authz gate should short-circuit")

    connect = ConnectService("http://c", httpx.Client(transport=httpx.MockTransport(handler)))
    app.dependency_overrides[get_connect] = lambda: connect
    app.dependency_overrides[get_roles] = lambda: set()  # no roles -> no READ
    client = TestClient(app, raise_server_exceptions=False)
    resp = client.get("/api/connectors/dbz-students/debug")
    assert resp.status_code == 403


def test_restart_requires_source_edit_role():
    def handler(request):
        raise AssertionError("must not reach Connect -- authz gate should short-circuit")

    connect = ConnectService("http://c", httpx.Client(transport=httpx.MockTransport(handler)))
    app.dependency_overrides[get_connect] = lambda: connect
    app.dependency_overrides[get_roles] = lambda: {Role.STUDENT}  # READ only, not SOURCE_EDIT
    client = TestClient(app, raise_server_exceptions=False)
    resp = client.post("/api/connectors/dbz-students/restart", json={})
    assert resp.status_code == 403
