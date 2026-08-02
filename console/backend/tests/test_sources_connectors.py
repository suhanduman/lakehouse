"""Tests for `GET /api/sources/{name}/connectors`: resolves a pipeline's
source + dedicated-sink connectors by reusing `assemble_pipelines` (the same
topology builder behind `GET /api/pipelines`) -- see
`app.routers.pipelines.list_pipelines` for the reference wiring this route
mirrors (`get_k8s` + `get_trino`, NOT `get_iceberg`), and
`tests/test_pipeline_topology.py`/`tests/test_sources_router.py` for the CR
fixture shapes/authz-override pattern this file reuses.

No real cluster/Trino: `K8sService`/`TrinoService` are swapped via
`app.dependency_overrides`, exactly like `test_sources_router.py`.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

import pytest
from fastapi.testclient import TestClient

from app.deps import get_k8s, get_roles, get_trino
from app.main import app
from app.services.authz import Role

# A CDC pipeline "students": a source KafkaConnector (metadata.name
# "students") + a dedicated Iceberg sink KafkaConnector (metadata.name
# "students-sink"), both carrying the `lakehouse.solus.dev/source` grouping
# annotation -- mirrors the CR shapes in test_pipeline_topology.py's
# _cdc_pg_cr/_sink_cr (render_service.render_connector's real output shape).
_SRC_ANN = "lakehouse.solus.dev/source"
_SINK_CLASS = "org.apache.iceberg.connect.IcebergSinkConnector"

SOURCE_CR: Dict[str, Any] = {
    "metadata": {"name": "students", "annotations": {_SRC_ANN: "students"}},
    "spec": {
        "class": "io.debezium.connector.postgresql.PostgresConnector",
        "config": {
            "topic.prefix": "cdc.students",
            "table.include.list": "public.students",
            "transforms.route.static.value": "students_raw.students",
        },
    },
    "status": {"connectorStatus": {"connector": {"state": "RUNNING"}}},
}

SINK_CR: Dict[str, Any] = {
    "metadata": {"name": "students-sink", "annotations": {_SRC_ANN: "students"}},
    "spec": {
        "class": _SINK_CLASS,
        "config": {"topics": "cdc.students.public.students"},
    },
    "status": {"connectorStatus": {"connector": {"state": "RUNNING"}}},
}


class FakeK8s:
    """Minimal `K8sService` double -- `assemble_pipelines` needs
    `list_sources()` plus the two status callables (`get_status`/
    `get_spark_status`), mirroring `test_smoke_all_routes.py`'s FakeK8s."""

    def __init__(self, sources: List[Dict[str, Any]]):
        self._sources = sources

    def list_sources(self) -> List[Dict[str, Any]]:
        return self._sources

    def get_status(self, name: str) -> Dict[str, Any]:
        return {"connector": {"state": "RUNNING"}}

    def get_spark_status(self, name: str) -> Dict[str, Any]:
        return {"scheduleState": "Scheduled"}


class FakeTrino:
    """`namespaces` source `/api/pipelines` (and now this route) uses --
    empty inventory is fine here since the connectors route never inspects
    `disposition`/`authoritative`, only the node list."""

    def list_namespaces(self, catalog: str) -> List[str]:
        return []

    def list_tables(self, catalog: str, ns: str) -> List[str]:
        return []


def _client_with_students_pipeline(with_sink: bool) -> TestClient:
    sources = [SOURCE_CR] + ([SINK_CR] if with_sink else [])
    app.dependency_overrides[get_roles] = lambda: {Role.ADMIN}
    app.dependency_overrides[get_k8s] = lambda: FakeK8s(sources)
    app.dependency_overrides[get_trino] = lambda: FakeTrino()
    return TestClient(app)


@pytest.fixture(autouse=True)
def _clear_overrides():
    yield
    app.dependency_overrides.clear()


def test_source_connectors_returns_source_and_sink():
    client = _client_with_students_pipeline(with_sink=True)
    resp = client.get("/api/sources/students/connectors")
    assert resp.status_code == 200
    roles = {c["role"]: c for c in resp.json()["connectors"]}
    assert set(roles) == {"source", "sink"}
    assert roles["source"]["name"] == "students"
    assert roles["sink"]["name"] == "students-sink"
    assert "state" in roles["source"]


def test_source_connectors_no_sink_role_when_absent():
    client = _client_with_students_pipeline(with_sink=False)
    resp = client.get("/api/sources/students/connectors")
    assert resp.status_code == 200
    roles = {c["role"] for c in resp.json()["connectors"]}
    assert "sink" not in roles


def test_source_connectors_404_for_unknown():
    client = _client_with_students_pipeline(with_sink=True)
    resp = client.get("/api/sources/nope/connectors")
    assert resp.status_code == 404


def test_source_connectors_resolves_by_sink_node_name_too():
    """`name` may be any node's name, not just the top-level pipeline name --
    e.g. looking the pipeline up by its dedicated sink's own CR name."""
    client = _client_with_students_pipeline(with_sink=True)
    resp = client.get("/api/sources/students-sink/connectors")
    assert resp.status_code == 200
    roles = {c["role"] for c in resp.json()["connectors"]}
    assert roles == {"source", "sink"}


def test_source_connectors_requires_read_action():
    app.dependency_overrides[get_roles] = lambda: set()
    app.dependency_overrides[get_k8s] = lambda: FakeK8s([SOURCE_CR, SINK_CR])
    app.dependency_overrides[get_trino] = lambda: FakeTrino()
    client = TestClient(app)
    resp = client.get("/api/sources/students/connectors")
    assert resp.status_code == 403
