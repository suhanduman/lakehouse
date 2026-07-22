"""Tests for the `/api/sources` router: authz gating (student / analyst /
admin) exercised through a real FastAPI `TestClient`, with
`get_roles`/`get_k8s`/`get_s3`/`get_trino`/`get_orchestrator` swapped out via
`app.dependency_overrides` -- no real OIDC token, no live cluster, no real
orchestrator.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

import pytest
from fastapi.testclient import TestClient

from app.deps import (
    get_k8s,
    get_orchestrator,
    get_s3,
    get_trino,
    get_roles,
)
from app.main import app
from app.orchestrator import AddSourceResult, StepResult
from app.services.authz import Role

VALID_SPEC = {
    "source": "mssql1",
    "kind": "cdc",
    "type": "mssql",
    "db": "school",
    "table": "dbo.students",
    "target_ns": "mssql_ogrenci",
    "target_table": "students",
    "db_host": "mssql1.internal",
}
VALID_PAYLOAD = {
    "spec": VALID_SPEC,
    "credentials": {"user": "sa", "password": "s3cret"},
}

# A KafkaConnector CR shaped like render_service.render_connector's output:
# metadata.name + spec.class + spec.config (carrying the route static value
# used to recover target ns/table, and the topic-shape keys), + status.
CONNECTOR_CR = {
    "metadata": {"name": "dbz-mssql1-students"},
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


# --------------------------------------------------------------------------
# Fakes -- hand-rolled doubles for K8sService / S3Service / TrinoService /
# AddSourceOrchestrator.
# --------------------------------------------------------------------------

class FakeK8s:
    def __init__(self, sources: Optional[List[Dict[str, Any]]] = None):
        self._sources = sources if sources is not None else [CONNECTOR_CR]
        self.patched: List[tuple] = []
        self.paused_calls: List[tuple] = []
        self.deleted_connectors: List[str] = []
        self.deleted_topics: List[str] = []

    def list_sources(self):
        return self._sources

    def patch_connector(self, name, config):
        self.patched.append((name, config))

    def set_paused(self, name, paused):
        self.paused_calls.append((name, paused))

    def delete_connector(self, name):
        self.deleted_connectors.append(name)

    def delete_topic(self, name):
        self.deleted_topics.append(name)


class FakeS3:
    def __init__(self):
        self.emptied: List[str] = []

    def empty_bucket(self, name):
        self.emptied.append(name)


class FakeTrino:
    def __init__(self):
        self.dropped: List[str] = []

    def drop_table(self, fqn):
        self.dropped.append(fqn)


class FakeOrchestrator:
    def __init__(self, ok: bool = True):
        self.ok = ok
        self.calls: List[tuple] = []

    def add_source(self, spec, creds):
        self.calls.append((spec, creds))
        return AddSourceResult(
            steps=[StepResult(name="secret", ok=True), StepResult(name="verify", ok=self.ok)],
            ok=self.ok,
        )


class ExplodingProvider:
    """A provider override that raises the moment FastAPI tries to construct
    it -- used to prove an unauthorized request is short-circuited by the
    authz gate BEFORE any provider is built (would surface as 500 otherwise)."""

    def __call__(self):
        raise RuntimeError("provider was constructed before authz -- SECURITY BUG")


# --------------------------------------------------------------------------
# Client helper -- swaps every dependency via app.dependency_overrides.
# --------------------------------------------------------------------------

def _client_as(roles, k8s=None, s3=None, trino=None, orchestrator=None) -> TestClient:
    app.dependency_overrides[get_roles] = lambda: roles
    app.dependency_overrides[get_k8s] = lambda: (k8s if k8s is not None else FakeK8s())
    app.dependency_overrides[get_s3] = lambda: (s3 if s3 is not None else FakeS3())
    app.dependency_overrides[get_trino] = lambda: (trino if trino is not None else FakeTrino())
    app.dependency_overrides[get_orchestrator] = lambda: (
        orchestrator if orchestrator is not None else FakeOrchestrator()
    )
    return TestClient(app)


@pytest.fixture(autouse=True)
def _clear_overrides():
    yield
    app.dependency_overrides.clear()


@pytest.fixture
def fake_k8s() -> FakeK8s:
    return FakeK8s()


@pytest.fixture
def fake_orchestrator() -> FakeOrchestrator:
    return FakeOrchestrator(ok=True)


@pytest.fixture
def client_as_student() -> TestClient:
    return _client_as({Role.STUDENT})


@pytest.fixture
def client_as_analyst() -> TestClient:
    return _client_as({Role.ANALYST})


@pytest.fixture
def client_as_admin() -> TestClient:
    return _client_as({Role.ADMIN})


@pytest.fixture
def client_as_analyst_with_fake_orch(fake_orchestrator) -> TestClient:
    return _client_as({Role.ANALYST}, orchestrator=fake_orchestrator)


# --------------------------------------------------------------------------
# Create (POST /api/sources) -- SOURCE_CREATE
# --------------------------------------------------------------------------

def test_create_source_requires_role(client_as_student):
    r = client_as_student.post("/api/sources", json=VALID_PAYLOAD)
    assert r.status_code == 403


def test_create_source_ok(client_as_analyst_with_fake_orch, fake_orchestrator):
    r = client_as_analyst_with_fake_orch.post("/api/sources", json=VALID_PAYLOAD)
    assert r.status_code == 201
    body = r.json()
    assert body["ok"] is True
    assert [s["name"] for s in body["steps"]] == ["secret", "verify"]
    assert len(fake_orchestrator.calls) == 1
    called_spec, called_creds = fake_orchestrator.calls[0]
    assert called_spec.source == "mssql1"
    assert called_creds.user == "sa"


def test_create_source_admin_also_allowed(client_as_admin):
    r = client_as_admin.post("/api/sources", json=VALID_PAYLOAD)
    assert r.status_code == 201


def test_create_source_orchestrator_failure_is_207_not_201(fake_k8s):
    """An in-band pipeline failure (rollback ran, or unsupported
    scheduled+mongo) must NOT be reported as a plain 201 Created -- that
    would tell callers who only check for a 2xx that the source was
    created when it was not. 207 Multi-Status carries the same
    ok:false/steps body so callers that do inspect it still get the detail
    without a second round-trip."""
    client = _client_as({Role.ANALYST}, k8s=fake_k8s, orchestrator=FakeOrchestrator(ok=False))
    r = client.post("/api/sources", json=VALID_PAYLOAD)
    assert r.status_code == 207
    assert r.json()["ok"] is False


def test_create_source_ok_true_is_201(client_as_analyst_with_fake_orch):
    r = client_as_analyst_with_fake_orch.post("/api/sources", json=VALID_PAYLOAD)
    assert r.status_code == 201
    assert r.json()["ok"] is True


# --------------------------------------------------------------------------
# Preview (POST /api/sources/preview) -- SOURCE_CREATE, render-only.
#
# No k8s/s3/trino/orchestrator dependency at all: proven below not merely by
# an unset call-tracking list (which would also pass if the route just
# forgot to invoke a fake) but by installing `ExplodingProvider` -- a
# provider that raises the instant FastAPI tries to *construct* it. A 200
# response is only possible if none of those providers were ever built.
# --------------------------------------------------------------------------

def test_preview_source_requires_role(client_as_student):
    r = client_as_student.post("/api/sources/preview", json={"spec": VALID_SPEC})
    assert r.status_code == 403


def test_preview_source_renders_crs_without_touching_any_provider():
    app.dependency_overrides[get_roles] = lambda: {Role.ANALYST}
    app.dependency_overrides[get_k8s] = ExplodingProvider()
    app.dependency_overrides[get_s3] = ExplodingProvider()
    app.dependency_overrides[get_trino] = ExplodingProvider()
    app.dependency_overrides[get_orchestrator] = ExplodingProvider()
    client = TestClient(app, raise_server_exceptions=False)

    r = client.post("/api/sources/preview", json={"spec": VALID_SPEC})

    assert r.status_code == 200
    body = r.json()
    assert body["connector"]["spec"]["class"] == "io.debezium.connector.sqlserver.SqlServerConnector"
    assert (
        body["connector"]["spec"]["config"]["transforms.route.static.value"]
        == "mssql_ogrenci_raw.students"
    )
    assert body["kafka_topic"]["metadata"]["name"] == "cdc.mssql1.dbo.students"
    assert body["bucket"] == "src-mssql-ogrenci"
    assert "CREATE NAMESPACE IF NOT EXISTS lakehouse.mssql_ogrenci" in body["namespace_ddl"]


def test_preview_source_scheduled_mongo_has_no_connector_but_still_200():
    # scheduled+mongo is a Spark batch CronJob, not a KafkaConnector (see
    # render_service.render_connector's NotImplementedError) -- preview must
    # degrade gracefully (connector/kafka_topic null), never 500.
    spec = {
        "source": "mongo1",
        "kind": "scheduled",
        "type": "mongo",
        "db": "lms",
        "table": "enrollments",
        "target_ns": "lms_kayit",
        "target_table": "enrollments",
        "cron": "0 * * * *",
    }
    app.dependency_overrides[get_roles] = lambda: {Role.ANALYST}
    app.dependency_overrides[get_k8s] = ExplodingProvider()
    app.dependency_overrides[get_s3] = ExplodingProvider()
    app.dependency_overrides[get_trino] = ExplodingProvider()
    app.dependency_overrides[get_orchestrator] = ExplodingProvider()
    client = TestClient(app, raise_server_exceptions=False)

    r = client.post("/api/sources/preview", json={"spec": spec})

    assert r.status_code == 200
    body = r.json()
    assert body["connector"] is None
    assert body["kafka_topic"] is None
    assert body["bucket"] == "src-lms-kayit"


# --------------------------------------------------------------------------
# Imp3 -- authz runs BEFORE any provider is constructed.
# --------------------------------------------------------------------------

def test_unauthorized_create_does_not_build_provider():
    # student lacks SOURCE_CREATE; the orchestrator provider raises if built.
    # A 403 (not 500) proves the authz gate short-circuited before construction.
    app.dependency_overrides[get_roles] = lambda: {Role.STUDENT}
    app.dependency_overrides[get_orchestrator] = ExplodingProvider()
    client = TestClient(app, raise_server_exceptions=False)
    r = client.post("/api/sources", json=VALID_PAYLOAD)
    assert r.status_code == 403


def test_unauthorized_delete_with_data_does_not_build_providers():
    # analyst lacks SOURCE_DELETE_WITH_DATA; all providers raise if built.
    app.dependency_overrides[get_roles] = lambda: {Role.ANALYST}
    app.dependency_overrides[get_k8s] = ExplodingProvider()
    app.dependency_overrides[get_s3] = ExplodingProvider()
    app.dependency_overrides[get_trino] = ExplodingProvider()
    client = TestClient(app, raise_server_exceptions=False)
    r = client.delete("/api/sources/dbz-x?mode=with_data")
    assert r.status_code == 403


# --------------------------------------------------------------------------
# List / get -- READ
# --------------------------------------------------------------------------

def test_list_sources_readable_by_student(client_as_student):
    r = client_as_student.get("/api/sources")
    assert r.status_code == 200
    names = [s["name"] for s in r.json()["sources"]]
    assert "dbz-mssql1-students" in names


def test_get_source_readable_by_analyst(client_as_analyst):
    r = client_as_analyst.get("/api/sources/dbz-mssql1-students")
    assert r.status_code == 200
    body = r.json()
    assert body["name"] == "dbz-mssql1-students"
    assert body["state"] == "RUNNING"


def test_get_source_not_found_is_404(client_as_analyst):
    r = client_as_analyst.get("/api/sources/does-not-exist")
    assert r.status_code == 404


# --------------------------------------------------------------------------
# Edit / pause / resume -- SOURCE_EDIT
# --------------------------------------------------------------------------

def test_patch_source_requires_edit_role(client_as_student):
    r = client_as_student.patch(
        "/api/sources/dbz-mssql1-students", json={"config": {"a": "b"}}
    )
    assert r.status_code == 403


def test_patch_source_ok_for_analyst(fake_k8s):
    client = _client_as({Role.ANALYST}, k8s=fake_k8s)
    r = client.patch(
        "/api/sources/dbz-mssql1-students",
        json={"config": {"poll.interval.ms": "1000"}},
    )
    assert r.status_code == 200
    assert r.json()["ok"] is True
    assert fake_k8s.patched == [("dbz-mssql1-students", {"poll.interval.ms": "1000"})]


def test_pause_then_resume_for_analyst(fake_k8s):
    client = _client_as({Role.ANALYST}, k8s=fake_k8s)

    r1 = client.post("/api/sources/dbz-mssql1-students/pause")
    assert r1.status_code == 200
    assert r1.json()["paused"] is True

    r2 = client.post("/api/sources/dbz-mssql1-students/resume")
    assert r2.status_code == 200
    assert r2.json()["paused"] is False

    assert fake_k8s.paused_calls == [
        ("dbz-mssql1-students", True),
        ("dbz-mssql1-students", False),
    ]


def test_pause_forbidden_for_student(client_as_student):
    r = client_as_student.post("/api/sources/dbz-mssql1-students/pause")
    assert r.status_code == 403


# --------------------------------------------------------------------------
# Delete -- per-mode authz gate + with_data teardown (Imp4) + 404 (Minor7)
# --------------------------------------------------------------------------

def test_delete_pipeline_only_ok_for_analyst():
    k8s, s3, trino = FakeK8s(), FakeS3(), FakeTrino()
    client = _client_as({Role.ANALYST}, k8s=k8s, s3=s3, trino=trino)
    r = client.delete("/api/sources/dbz-mssql1-students?mode=pipeline_only")
    assert r.status_code == 200
    assert r.json() == {"ok": True, "name": "dbz-mssql1-students", "mode": "pipeline_only"}
    assert k8s.deleted_connectors == ["dbz-mssql1-students"]
    # pipeline_only must NOT touch data: no drop_table, no empty_bucket, no topic delete.
    assert trino.dropped == []
    assert s3.emptied == []
    assert k8s.deleted_topics == []


def test_delete_with_data_forbidden_for_analyst(client_as_analyst):
    r = client_as_analyst.delete("/api/sources/dbz-x?mode=with_data")
    assert r.status_code == 403


def test_delete_with_data_ok_for_admin_does_full_teardown():
    k8s, s3, trino = FakeK8s(), FakeS3(), FakeTrino()
    client = _client_as({Role.ADMIN}, k8s=k8s, s3=s3, trino=trino)
    r = client.delete("/api/sources/dbz-mssql1-students?mode=with_data")
    assert r.status_code == 200
    body = r.json()
    assert body["ok"] is True
    assert body["mode"] == "with_data"
    # response is DISTINCT from pipeline_only -- surfaces what was destroyed.
    assert body["dropped_table"] == "lakehouse.mssql_ogrenci.students"
    assert body["emptied_bucket"] == "src-mssql-ogrenci"
    assert body["deleted_topic"] == "cdc.mssql1.dbo.students"
    # and the destructive calls actually happened, against the parsed targets.
    assert k8s.deleted_connectors == ["dbz-mssql1-students"]
    assert k8s.deleted_topics == ["cdc.mssql1.dbo.students"]
    assert trino.dropped == ["lakehouse.mssql_ogrenci.students"]
    assert s3.emptied == ["src-mssql-ogrenci"]


def test_delete_default_mode_is_pipeline_only():
    k8s, s3, trino = FakeK8s(), FakeS3(), FakeTrino()
    client = _client_as({Role.ANALYST}, k8s=k8s, s3=s3, trino=trino)
    r = client.delete("/api/sources/dbz-mssql1-students")
    assert r.status_code == 200
    assert r.json()["mode"] == "pipeline_only"
    assert trino.dropped == [] and s3.emptied == []


def test_delete_pipeline_only_forbidden_for_student(client_as_student):
    r = client_as_student.delete("/api/sources/dbz-mssql1-students?mode=pipeline_only")
    assert r.status_code == 403


def test_delete_invalid_mode_is_422(client_as_admin):
    r = client_as_admin.delete("/api/sources/dbz-x?mode=nonsense")
    assert r.status_code == 422


def test_delete_missing_source_is_404_for_admin():
    # Minor7: 404 before any teardown, consistent with GET.
    k8s, s3, trino = FakeK8s(sources=[]), FakeS3(), FakeTrino()
    client = _client_as({Role.ADMIN}, k8s=k8s, s3=s3, trino=trino)
    r = client.delete("/api/sources/nope?mode=with_data")
    assert r.status_code == 404
    assert k8s.deleted_connectors == []
    assert trino.dropped == [] and s3.emptied == []


# --------------------------------------------------------------------------
# Minor5 -- credential redaction in 422 validation errors.
# --------------------------------------------------------------------------

def test_validation_error_redacts_credentials(client_as_analyst):
    # `spec.source` is required; omitting it triggers a 422 whose default
    # echo would include the whole body -- including credentials.password.
    bad = {
        "spec": {k: v for k, v in VALID_SPEC.items() if k != "source"},
        "credentials": {"user": "sa", "password": "TOPSECRET-do-not-leak"},
    }
    r = client_as_analyst.post("/api/sources", json=bad)
    assert r.status_code == 422
    assert "TOPSECRET-do-not-leak" not in r.text


# --------------------------------------------------------------------------
# get_roles fails closed when nothing overrides it.
# --------------------------------------------------------------------------

def test_no_override_no_token_is_401():
    app.dependency_overrides.clear()
    r = TestClient(app).get("/api/sources")
    assert r.status_code == 401


# --------------------------------------------------------------------------
# Imp1/Imp2 -- OIDC verification path: any JOSEError subclass (JWKError,
# JWSError, JWTError, ...) becomes a 401, never a 500; and the token's own
# `alg` header is not trusted (explicit RS256 allowlist rejects alg:none).
# JWKS fetch is monkeypatched so no network/IdP is involved.
# --------------------------------------------------------------------------

import base64
import json

from fastapi import HTTPException

from app import deps


def _unsigned_none_jwt() -> str:
    def b64(obj):
        return base64.urlsafe_b64encode(json.dumps(obj).encode()).rstrip(b"=").decode()

    return f"{b64({'alg': 'none', 'typ': 'JWT'})}.{b64({'sub': 'attacker'})}."


def test_garbage_token_is_401_not_500(monkeypatch):
    monkeypatch.setattr(deps, "_fetch_jwks", lambda issuer, force=False: {"keys": []})
    with pytest.raises(HTTPException) as excinfo:
        deps._verify_and_decode("this-is-not-a-jwt")
    assert excinfo.value.status_code == 401


def test_alg_none_token_rejected_401(monkeypatch):
    # Explicit RS256 allowlist means an unsigned alg:none token is refused
    # (would be a forgery bypass otherwise), surfaced as 401 not 500.
    monkeypatch.setattr(deps, "_fetch_jwks", lambda issuer, force=False: {"keys": []})
    with pytest.raises(HTTPException) as excinfo:
        deps._verify_and_decode(_unsigned_none_jwt())
    assert excinfo.value.status_code == 401


def test_jwks_cache_reused_within_ttl(monkeypatch):
    # discovery/JWKS fetched once, then served from cache on the next call.
    calls = {"n": 0}

    def fake_get(url, timeout=5.0):
        calls["n"] += 1

        class _R:
            def raise_for_status(self):
                pass

            def json(self):
                return {"jwks_uri": "https://idp.example/jwks"} if "well-known" in url else {"keys": []}

        return _R()

    deps._JWKS_CACHE.clear()
    monkeypatch.setattr(deps.httpx, "get", fake_get)
    first = deps._fetch_jwks("https://idp.example")
    second = deps._fetch_jwks("https://idp.example")
    assert first == second == {"keys": []}
    # 2 requests for the first fetch (discovery + jwks), 0 for the cached second.
    assert calls["n"] == 2
    deps._JWKS_CACHE.clear()
