from fastapi.testclient import TestClient
from app.main import app
from app import deps
from app.config import settings


class _FakeK8s:
    def get_application_status(self, name, ns):
        return {"sync": {"status": "Synced"}, "health": {"status": "Healthy"},
                "resources": [
                    {"kind": "KafkaConnector", "name": "dbz-pgdemo-customers", "status": "Synced", "health": {"status": "Healthy"}},
                    {"kind": "KafkaConnector", "name": "dbz-x-y", "status": "OutOfSync", "health": {"status": "Missing"}}]}
    def list_sources(self):
        return [{"kind": "KafkaConnector", "metadata": {"name": "dbz-pgdemo-customers",
                 "annotations": {"lakehouse.solus.dev/source": "pgdemo"}}}]


def test_gitops_status_direct_mode(monkeypatch):
    monkeypatch.setattr(settings, "deploy_mode", "direct")
    app.dependency_overrides[deps.get_k8s] = lambda: object()
    try:
        assert TestClient(app).get("/gitops/status").json() == {"mode": "direct"}
    finally:
        app.dependency_overrides.clear()


def test_gitops_status_gitops_groups_per_source(monkeypatch):
    monkeypatch.setattr(settings, "deploy_mode", "gitops")
    app.dependency_overrides[deps.get_k8s] = lambda: _FakeK8s()
    try:
        body = TestClient(app).get("/gitops/status").json()
        assert body["mode"] == "gitops"
        assert body["application"]["sync"] == "Synced"
        srcs = {s["source"]: s for s in body["sources"]}
        assert "pgdemo" in srcs and srcs["pgdemo"]["health"] == "Healthy"
        assert any(r["name"] == "dbz-x-y" for r in body["outOfSync"])
    finally:
        app.dependency_overrides.clear()
