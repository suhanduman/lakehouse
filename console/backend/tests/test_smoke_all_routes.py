"""Smoke test: a real `TestClient` hits every registered app route
(healthz + every `/api/...` endpoint) with an authorized (ADMIN) `get_roles`
override plus fakes for every provider dependency
(`get_k8s`/`get_orchestrator`/`get_trino`/`get_s3`/`get_connect`/
`get_apicurio`), asserting none of them 500s.

This is deliberately *not* a behavioral/authz test (that's the job of
`test_sources_router.py` / `test_other_routers.py`) -- it exists purely to
catch wiring gaps: a route whose handler references an import that doesn't
exist, a provider override signature mismatch, a response model that can't
serialize the fake's return shape, etc. An authorized, minimally-valid
request against every route should never produce a 500.

`ENDPOINTS` is an explicit table (method, route *template*, concrete request)
rather than a blind walk of `app.routes`, so each entry is a deliberate,
readable fixture. `test_every_app_route_is_covered_by_the_smoke_list` is the
safety net that keeps the table honest: it walks `app.routes` itself and
fails if a future router adds an endpoint that isn't represented here.
"""

from __future__ import annotations

from typing import Any, Dict, List

import pytest
from fastapi.testclient import TestClient

from app.deps import (
    get_apicurio,
    get_connect,
    get_k8s,
    get_orchestrator,
    get_roles,
    get_s3,
    get_trino,
)
from app.main import app
from app.orchestrator import AddSourceResult, StepResult
from app.services.authz import Role

SOURCE_NAME = "dbz-mssql1-students"

# A KafkaConnector CR shaped like render_service.render_connector's output
# (mirrors test_sources_router.py's CONNECTOR_CR) so GET/PATCH/pause/resume/
# DELETE `/api/sources/{name}` all resolve against a source that "exists".
CONNECTOR_CR = {
    "metadata": {"name": SOURCE_NAME},
    "spec": {
        "class": "io.debezium.connector.sqlserver.SqlServerConnector",
        "config": {
            "topic.prefix": "cdc.mssql1",
            "table.include.list": "dbo.students",
            "transforms.route.static.value": "mssql_ogrenci_raw.students",
        },
    },
    "status": {"connectorStatus": {"connector": {"state": "RUNNING"}}},
}

VALID_SOURCE_PAYLOAD = {
    "spec": {
        "source": "mssql1",
        "kind": "cdc",
        "type": "mssql",
        "db": "school",
        "table": "dbo.students",
        "target_ns": "mssql_ogrenci",
        "target_table": "students",
        "db_host": "mssql1.internal",
    },
    "credentials": {"user": "sa", "password": "s3cret"},
}


# --------------------------------------------------------------------------
# Fakes -- hand-rolled doubles for every provider, minimal enough to make
# every route's happy path return without touching a real cluster/DB/broker.
# --------------------------------------------------------------------------

class FakeK8s:
    def list_sources(self) -> List[Dict[str, Any]]:
        return [CONNECTOR_CR]

    def patch_connector(self, name: str, config: Dict[str, Any]) -> None:
        pass

    def set_paused(self, name: str, paused: bool) -> None:
        pass

    def delete_connector(self, name: str) -> None:
        pass

    def delete_topic(self, name: str) -> None:
        pass


class FakeS3:
    def list_buckets(self) -> List[str]:
        return ["src-mssql-ogrenci"]

    def create_bucket(self, name: str) -> bool:
        return True

    def empty_bucket(self, name: str) -> None:
        pass


class FakeTrino:
    def list_namespaces(self, catalog: str) -> List[str]:
        return ["mssql_ogrenci"]

    def list_tables(self, catalog: str, ns: str) -> List[str]:
        return ["students"]

    def run_ddl(self, sql: str) -> None:
        pass

    def drop_table(self, fqn: str) -> None:
        pass


class FakeOrchestrator:
    def add_source(self, spec, creds) -> AddSourceResult:
        return AddSourceResult(steps=[StepResult(name="secret", ok=True)], ok=True)


class FakeConnect:
    def list_connectors(self) -> List[str]:
        return [SOURCE_NAME]

    def connector_status(self, name: str) -> Dict[str, Any]:
        return {"connector": {"state": "RUNNING"}, "tasks": [{"state": "RUNNING"}]}


class FakeApicurio:
    def list_schemas(self) -> List[Dict[str, Any]]:
        return [{"id": "cdc.mssql1.dbo.students-value"}]


@pytest.fixture
def admin_client() -> TestClient:
    """A single authorized (ADMIN, i.e. every Action granted) client with
    every provider dependency swapped for a fake -- no real IdP, cluster,
    S3/MinIO, Trino, Connect, or Apicurio required."""
    app.dependency_overrides[get_roles] = lambda: {Role.ADMIN}
    app.dependency_overrides[get_k8s] = lambda: FakeK8s()
    app.dependency_overrides[get_s3] = lambda: FakeS3()
    app.dependency_overrides[get_trino] = lambda: FakeTrino()
    app.dependency_overrides[get_orchestrator] = lambda: FakeOrchestrator()
    app.dependency_overrides[get_connect] = lambda: FakeConnect()
    app.dependency_overrides[get_apicurio] = lambda: FakeApicurio()
    try:
        yield TestClient(app)
    finally:
        app.dependency_overrides.clear()


# --------------------------------------------------------------------------
# One entry per registered route. `template` is the route's registered path
# (matches `route.path` from `app.routes`, used by the coverage check below);
# `path` is the concrete URL actually requested.
# --------------------------------------------------------------------------

ENDPOINTS: List[Dict[str, Any]] = [
    {"method": "GET", "template": "/healthz", "path": "/healthz"},
    {"method": "GET", "template": "/api/sources", "path": "/api/sources"},
    {
        "method": "POST",
        "template": "/api/sources",
        "path": "/api/sources",
        "json": VALID_SOURCE_PAYLOAD,
    },
    {
        "method": "POST",
        "template": "/api/sources/preview",
        "path": "/api/sources/preview",
        "json": {"spec": VALID_SOURCE_PAYLOAD["spec"]},
    },
    {
        "method": "GET",
        "template": "/api/sources/{name}",
        "path": f"/api/sources/{SOURCE_NAME}",
    },
    {
        "method": "PATCH",
        "template": "/api/sources/{name}",
        "path": f"/api/sources/{SOURCE_NAME}",
        "json": {"config": {"poll.interval.ms": "1000"}},
    },
    {
        "method": "POST",
        "template": "/api/sources/{name}/pause",
        "path": f"/api/sources/{SOURCE_NAME}/pause",
    },
    {
        "method": "POST",
        "template": "/api/sources/{name}/resume",
        "path": f"/api/sources/{SOURCE_NAME}/resume",
    },
    {
        "method": "DELETE",
        "template": "/api/sources/{name}",
        "path": f"/api/sources/{SOURCE_NAME}",
        "params": {"mode": "pipeline_only"},
    },
    {"method": "GET", "template": "/api/tables", "path": "/api/tables"},
    {
        "method": "POST",
        "template": "/api/tables",
        "path": "/api/tables",
        "json": {
            "namespace": "mssql_ogrenci",
            "table": "students",
            "columns": [{"name": "id", "type": "bigint"}],
        },
    },
    {
        "method": "DELETE",
        "template": "/api/tables/{fqn}",
        "path": "/api/tables/lakehouse.mssql_ogrenci.students",
    },
    {"method": "GET", "template": "/api/buckets", "path": "/api/buckets"},
    {
        "method": "POST",
        "template": "/api/buckets",
        "path": "/api/buckets",
        "json": {"name": "src-new"},
    },
    {"method": "GET", "template": "/api/schemas", "path": "/api/schemas"},
    {"method": "GET", "template": "/api/status", "path": "/api/status"},
]


def _endpoint_id(endpoint: Dict[str, Any]) -> str:
    return f"{endpoint['method']} {endpoint['template']}"


@pytest.mark.parametrize("endpoint", ENDPOINTS, ids=[_endpoint_id(e) for e in ENDPOINTS])
def test_route_never_500s(admin_client: TestClient, endpoint: Dict[str, Any]) -> None:
    kwargs = {k: v for k, v in endpoint.items() if k not in ("method", "template", "path")}
    response = admin_client.request(endpoint["method"], endpoint["path"], **kwargs)
    assert response.status_code != 500, (
        f"{endpoint['method']} {endpoint['path']} -> 500: {response.text}"
    )


# Routes FastAPI (or an instrumentation library) auto-generates -- docs UI,
# OpenAPI schema, Prometheus /metrics -- not app-specific business routes, so
# not interesting wiring-gap targets and excluded from the coverage
# requirement below. `/metrics` is registered by prometheus-fastapi-
# instrumentator with `include_in_schema=False`.
_GENERATED_PATHS = {"/openapi.json", "/docs", "/docs/oauth2-redirect", "/redoc", "/metrics"}


def test_every_app_route_is_covered_by_the_smoke_list() -> None:
    """Safety net: fails if a router adds a new endpoint that isn't added to
    `ENDPOINTS` above, so smoke coverage can't silently rot as routers grow."""
    covered = {(e["method"], e["template"]) for e in ENDPOINTS}
    missing = []
    for route in app.routes:
        path = getattr(route, "path", None)
        methods = getattr(route, "methods", None)
        if path is None or methods is None or path in _GENERATED_PATHS:
            continue
        for method in methods:
            if method == "HEAD":  # implied by GET, not separately routed
                continue
            if (method, path) not in covered:
                missing.append((method, path))
    assert not missing, f"routes not covered by the smoke test: {missing}"
