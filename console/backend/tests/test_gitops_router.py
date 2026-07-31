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


class _FakeK8sNoApplication:
    """get_application_status returns None -- ArgoCD Application CR not
    present yet (best-effort gitops-status, see app/routers/gitops.py)."""
    def get_application_status(self, name, ns):
        return None
    def list_sources(self):
        raise AssertionError("list_sources should not be called when the Application is None")


class _FakeK8sAllHealthy:
    """A single source (multiple resources) that is entirely Healthy+Synced
    -- exercises the per-source rollup's "all healthy" branch (`health ==
    {"Healthy"}` -> "Healthy", no OutOfSync -> "Synced") with more than one
    resource, rather than trivially via a single-resource source."""
    def get_application_status(self, name, ns):
        return {"sync": {"status": "Synced"}, "health": {"status": "Healthy"},
                "resources": [
                    {"kind": "KafkaConnector", "name": "dbz-pgdemo-customers", "status": "Synced", "health": {"status": "Healthy"}},
                    {"kind": "KafkaConnector", "name": "dbz-pgdemo-orders", "status": "Synced", "health": {"status": "Healthy"}}]}
    def list_sources(self):
        return [{"kind": "KafkaConnector", "metadata": {"name": "dbz-pgdemo-customers",
                 "annotations": {"lakehouse.solus.dev/source": "pgdemo"}}},
                {"kind": "KafkaConnector", "metadata": {"name": "dbz-pgdemo-orders",
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


def test_gitops_status_gitops_application_none(monkeypatch):
    monkeypatch.setattr(settings, "deploy_mode", "gitops")
    app.dependency_overrides[deps.get_k8s] = lambda: _FakeK8sNoApplication()
    try:
        assert TestClient(app).get("/gitops/status").json() == {
            "mode": "gitops", "application": None, "sources": [], "outOfSync": []}
    finally:
        app.dependency_overrides.clear()


def test_gitops_status_gitops_source_all_healthy_synced(monkeypatch):
    monkeypatch.setattr(settings, "deploy_mode", "gitops")
    app.dependency_overrides[deps.get_k8s] = lambda: _FakeK8sAllHealthy()
    try:
        body = TestClient(app).get("/gitops/status").json()
        srcs = {s["source"]: s for s in body["sources"]}
        assert srcs["pgdemo"]["sync"] == "Synced"
        assert srcs["pgdemo"]["health"] == "Healthy"
        assert body["outOfSync"] == []
    finally:
        app.dependency_overrides.clear()
