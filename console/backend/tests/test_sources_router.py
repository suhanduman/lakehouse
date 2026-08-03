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
from kubernetes.client.exceptions import ApiException

from app.deps import (
    get_k8s,
    get_orchestrator,
    get_s3,
    get_trino,
    get_roles,
)
from app.config import settings
from app.main import app
from app.orchestrator import AddSourceResult, StepResult
from app.services import render_service
from app.services.authz import Role
from app.services.git_writer import CommitResult
from app.services.k8s_service import K8sService

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

# A ScheduledSparkApplication CR shaped like render_service.render_spark_job's
# output for a batch+s3 source (`_render_s3_register`): stable name
# `s3-register-<source>-<target_table>`, round-trip annotations under
# render_service.SPARK_ANNOTATION_PREFIX, plus spec.suspend/spec.template.
SPARK_CR = {
    "kind": "ScheduledSparkApplication",
    "metadata": {
        "name": "s3-register-s1-orders",
        "annotations": {
            "lakehouse.solus.dev/source": "s1",
            "lakehouse.solus.dev/target-ns": "ext",
            "lakehouse.solus.dev/target-table": "orders",
            "lakehouse.solus.dev/s3-bucket": "b",
            "lakehouse.solus.dev/s3-prefix": "p/",
            "lakehouse.solus.dev/file-format": "parquet",
            "lakehouse.solus.dev/cron": "0 * * * *",
        },
    },
    "spec": {
        "suspend": False,
        "template": {
            "arguments": ["--bucket", "b", "--prefix", "p/", "--format", "parquet",
                          "--target", "rawlake.ext.orders"],
        },
    },
    "status": {"scheduleState": "Scheduled"},
}


# --------------------------------------------------------------------------
# _target_ns_table -- prefers the round-trip target-ns/target-table
# annotations (stamped by render_service._connector) over re-parsing
# transforms.route.static.value, falling back to the route-parse for CRs
# pre-dating the annotation (or applied by hand).
# --------------------------------------------------------------------------

def test_target_ns_table_prefers_annotations_over_route_parse():
    from app.routers.sources import _target_ns_table
    cr = {
        "metadata": {"annotations": {
            "lakehouse.solus.dev/target-ns": "annNs",
            "lakehouse.solus.dev/target-table": "annTbl",
        }},
        "spec": {"config": {"transforms.route.static.value": "wrongNs_raw.wrongTbl"}},
    }
    assert _target_ns_table(cr) == ("annNs", "annTbl")


def test_target_ns_table_falls_back_to_route_parse_when_no_annotation():
    from app.routers.sources import _target_ns_table
    cr = {
        "metadata": {"annotations": {}},
        "spec": {"config": {"transforms.route.static.value": "pgd_raw.customers"}},
    }
    assert _target_ns_table(cr) == ("pgd", "customers")


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
        self.spark_suspended_calls: List[tuple] = []
        self.deleted_spark_jobs: List[str] = []
        self.removed_acls: List[tuple] = []
        self.deleted_users: List[str] = []
        self.deleted_secrets: List[str] = []

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

    def remove_user_acl(self, user, acl):
        self.removed_acls.append((user, acl))

    def delete_user(self, name):
        self.deleted_users.append(name)

    def delete_secret(self, name):
        self.deleted_secrets.append(name)

    def set_spark_suspended(self, name, suspended):
        self.spark_suspended_calls.append((name, suspended))

    def delete_spark_job(self, name):
        self.deleted_spark_jobs.append(name)


class FakeS3:
    def __init__(self):
        self.emptied: List[str] = []
        self.deleted_buckets: List[str] = []
        self.calls: List[tuple] = []

    def empty_bucket(self, name):
        self.emptied.append(name)
        self.calls.append(("empty_bucket", name))

    def delete_bucket(self, name):
        self.deleted_buckets.append(name)
        self.calls.append(("delete_bucket", name))


class FakeTrino:
    """`list_tables` defaults to always-empty so existing delete tests (which
    don't care about namespace cleanup) don't have to know about it -- the
    new `_drop_ns_if_empty` call in delete_source's with_data teardown will
    see an empty namespace and call `drop_namespace` every time by default.
    Tests exercising the "namespace still has siblings" case pass
    `tables_by_ns` to override per-namespace.
    """

    def __init__(self, tables_by_ns: Dict[tuple, List[str]] | None = None):
        self.dropped: List[str] = []
        self.dropped_namespaces: List[str] = []
        self.calls: List[tuple] = []
        self._tables_by_ns = tables_by_ns or {}

    def drop_table(self, fqn):
        self.dropped.append(fqn)
        self.calls.append(("drop_table", fqn))

    def list_tables(self, catalog, ns):
        tables = self._tables_by_ns.get((catalog, ns), [])
        self.calls.append(("list_tables", catalog, ns))
        return tables

    def drop_namespace(self, fqn):
        self.dropped_namespaces.append(fqn)
        self.calls.append(("drop_namespace", fqn))


class FakeOrchestrator:
    def __init__(self, ok: bool = True, git_writer: Any = None):
        self.ok = ok
        self.calls: List[tuple] = []
        self.edit_spark_calls: List[Any] = []
        self.git_writer = git_writer

    def add_source(self, spec, creds):
        self.calls.append((spec, creds))
        return AddSourceResult(
            steps=[StepResult(name="secret", ok=True), StepResult(name="verify", ok=self.ok)],
            ok=self.ok,
        )

    def edit_spark_source(self, spec):
        self.edit_spark_calls.append(spec)
        return {"ok": True}


class _FakeGitWriter:
    """Stand-in for app.services.git_writer.GitWriter -- records the `source`
    argument remove_source was actually called with (gitops delete routing,
    see test_delete_gitops_* below)."""

    def __init__(self):
        self.removed: List[str] = []

    def remove_source(self, source):
        self.removed.append(source)
        return CommitResult(committed=True, ref="cafef00d")


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


@pytest.fixture
def client() -> TestClient:
    # ANALYST covers both READ (/types) and SOURCE_CREATE (/preview) so the
    # registry-API + preview-registry-routing tests below can share one
    # fixture without tripping the authz gate on either route.
    return _client_as({Role.ANALYST})


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
    assert body["bronze_bucket"] == render_service.bronze_bucket_name("mssql_ogrenci")
    assert body["silver_bucket"] == render_service.silver_bucket_name("mssql_ogrenci")
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
    assert body["bronze_bucket"] == render_service.bronze_bucket_name("lms_kayit")
    assert body["silver_bucket"] == render_service.silver_bucket_name("lms_kayit")


# --------------------------------------------------------------------------
# Registry API (GET /api/sources/types) + preview registry-routing --
# Plan B1 Task 4: the wizard renders itself from the registry, and preview's
# spark-batch guard reads `source_types.get(...).lane` instead of a
# hard-coded (kind, type) check.
# --------------------------------------------------------------------------

def test_get_source_types_lists_registry(client):
    body = client.get("/api/sources/types").json()
    ids = {t["id"] for t in body["types"]}
    assert {"cdc-pg", "cdc-mysql", "stream-kafka"} <= ids
    kafka = next(t for t in body["types"] if t["id"] == "stream-kafka")
    assert kafka["lane"] == "kafka-connect-source"
    assert kafka["dispositions"] == ["event", "entity"]
    assert kafka["needs_bootstrap"] is True


def test_preview_spark_batch_returns_no_connector(client):
    spec = {"source": "m1", "kind": "scheduled", "type": "mongo", "db": "lms",
            "table": "enr", "target_ns": "depo", "target_table": "enr", "cron": "0 * * * *"}
    body = client.post("/api/sources/preview", json={"spec": spec}).json()
    assert body["connector"] is None


def test_preview_kafka_ingest_renders_connector(client):
    spec = {"source": "k1", "kind": "stream", "type": "kafka", "db": "ext",
            "table": "orders-topic", "target_ns": "events", "target_table": "orders"}
    body = client.post("/api/sources/preview", json={"spec": spec}).json()
    assert body["connector"]["spec"]["class"] == "org.apache.iceberg.connect.IcebergSinkConnector"


def test_preview_event_disposition_omits_silver(client):
    # stream-kafka event source (disposition="event" => no Silver table)
    spec = {"source": "ev", "kind": "stream", "type": "kafka", "db": "",
            "table": "t", "target_ns": "ev", "target_table": "t", "disposition": "event"}
    body = client.post("/api/sources/preview", json={"spec": spec}).json()
    assert body["silver_bucket"] is None
    assert body["namespace_ddl"] is None or "lakehouse.ev" not in body["namespace_ddl"]
    assert body["bronze_bucket"] is not None


def test_preview_entity_disposition_keeps_silver(client):
    # cdc/pg entity source (disposition not set => defaults to entity => Silver table included)
    spec = {"source": "en", "kind": "cdc", "type": "pg", "db": "app",
            "table": "c", "target_ns": "en", "target_table": "c", "db_host": "h:5432",
            "identifier": ["id"], "columns": [{"name": "id", "type": "int"}]}
    body = client.post("/api/sources/preview", json={"spec": spec}).json()
    assert body["silver_bucket"] is not None
    assert "lakehouse.en" in body["namespace_ddl"]


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


def test_summary_spark_shape():
    from app.routers.sources import _summary
    item = {"kind": "ScheduledSparkApplication",
            "metadata": {"name": "s3-register-s1-orders",
                         "annotations": {"lakehouse.solus.dev/source": "s1",
                                         "lakehouse.solus.dev/s3-bucket": "b",
                                         "lakehouse.solus.dev/target-ns": "ext",
                                         "lakehouse.solus.dev/target-table": "orders",
                                         "lakehouse.solus.dev/s3-prefix": "p/",
                                         "lakehouse.solus.dev/file-format": "parquet",
                                         "lakehouse.solus.dev/cron": "0 * * * *"}},
            "spec": {"suspend": True},
            "status": {"scheduleState": "Scheduled"}}
    s = _summary(item)
    assert s["cr_kind"] == "ScheduledSparkApplication"
    assert s["paused"] is True
    assert s["state"] == "Scheduled"
    assert s["spark"]["s3_bucket"] == "b" and s["spark"]["cron"] == "0 * * * *"


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
# gitops mode: connector-edit / pause / resume must fail loud (409), never
# silently do a direct k8s write that ArgoCD selfHeal would just revert.
# The spark edit path is exempt -- it already gitops-routes via
# orchestrator.edit_spark_source (see test_orchestrator_gitops.py).
# --------------------------------------------------------------------------

def test_edit_connector_gitops_mode_is_409(monkeypatch):
    monkeypatch.setattr(settings, "deploy_mode", "gitops")
    k8s = FakeK8s()
    client = _client_as({Role.ANALYST}, k8s=k8s)

    r = client.patch("/api/sources/dbz-mssql1-students", json={"config": {"a": "b"}})

    assert r.status_code == 409
    assert "gitops mode" in r.json()["detail"]
    assert k8s.patched == []  # never reaches the direct k8s write


def test_edit_connector_direct_mode_unchanged(fake_k8s):
    # settings.deploy_mode is "direct" by default -- confirm this guard
    # doesn't regress the pre-existing direct-mode patch behavior.
    client = _client_as({Role.ANALYST}, k8s=fake_k8s)
    r = client.patch(
        "/api/sources/dbz-mssql1-students", json={"config": {"poll.interval.ms": "1000"}}
    )
    assert r.status_code == 200
    assert fake_k8s.patched == [("dbz-mssql1-students", {"poll.interval.ms": "1000"})]


def test_pause_gitops_mode_is_409(monkeypatch):
    monkeypatch.setattr(settings, "deploy_mode", "gitops")
    k8s = FakeK8s()
    client = _client_as({Role.ANALYST}, k8s=k8s)

    r = client.post("/api/sources/dbz-mssql1-students/pause")

    assert r.status_code == 409
    # Task 5: detail is now a structured {"message", "remediation"} dict, not
    # a bare string -- see test_sources_pause_remediation.py for full
    # remediation-shape coverage.
    assert "gitops mode" in r.json()["detail"]["message"]
    assert k8s.paused_calls == []
    assert k8s.spark_suspended_calls == []


def test_resume_gitops_mode_is_409(monkeypatch):
    monkeypatch.setattr(settings, "deploy_mode", "gitops")
    k8s = FakeK8s()
    client = _client_as({Role.ANALYST}, k8s=k8s)

    r = client.post("/api/sources/dbz-mssql1-students/resume")

    assert r.status_code == 409
    # Task 5: detail is now a structured {"message", "remediation"} dict, not
    # a bare string -- see test_sources_pause_remediation.py for full
    # remediation-shape coverage.
    assert "gitops mode" in r.json()["detail"]["message"]
    assert k8s.paused_calls == []
    assert k8s.spark_suspended_calls == []


def test_pause_resume_direct_mode_unchanged(fake_k8s):
    # settings.deploy_mode is "direct" by default -- confirm this guard
    # doesn't regress the pre-existing direct-mode pause/resume behavior.
    client = _client_as({Role.ANALYST}, k8s=fake_k8s)

    r1 = client.post("/api/sources/dbz-mssql1-students/pause")
    assert r1.status_code == 200
    r2 = client.post("/api/sources/dbz-mssql1-students/resume")
    assert r2.status_code == 200

    assert fake_k8s.paused_calls == [
        ("dbz-mssql1-students", True),
        ("dbz-mssql1-students", False),
    ]


def test_pause_gitops_mode_also_409_for_spark_cr(monkeypatch):
    # pause/resume have no gitops routing for EITHER CR kind -- the guard
    # must reject a spark source too, not just KafkaConnector ones.
    monkeypatch.setattr(settings, "deploy_mode", "gitops")
    k8s = FakeK8s(sources=[SPARK_CR])
    client = _client_as({Role.ANALYST}, k8s=k8s)

    r = client.post("/api/sources/s3-register-s1-orders/pause")

    assert r.status_code == 409
    assert k8s.spark_suspended_calls == []


# --------------------------------------------------------------------------
# Kind-aware pause/edit/delete -- ScheduledSparkApplication (batch/s3) source
# gets parity with KafkaConnector sources, dispatched on `cr["kind"]`.
# --------------------------------------------------------------------------

def test_pause_spark_uses_suspend():
    k8s = FakeK8s(sources=[SPARK_CR])
    client = _client_as({Role.ANALYST}, k8s=k8s)

    r = client.post("/api/sources/s3-register-s1-orders/pause")

    assert r.status_code == 200
    assert r.json()["paused"] is True
    assert k8s.spark_suspended_calls == [("s3-register-s1-orders", True)]
    assert k8s.paused_calls == []  # connector-specific call NOT fired


def test_edit_spark_rerenders():
    orch = FakeOrchestrator()
    k8s = FakeK8s(sources=[SPARK_CR])
    client = _client_as({Role.ANALYST}, k8s=k8s, orchestrator=orch)
    spec_payload = {
        "source": "s1", "kind": "batch", "type": "s3", "db": "-", "table": "-",
        "target_ns": "ext", "target_table": "orders", "s3_bucket": "b2",
        "s3_prefix": "p2/", "file_format": "parquet", "cron": "5 * * * *",
    }

    r = client.patch("/api/sources/s3-register-s1-orders", json={"spec": spec_payload})

    assert r.status_code == 200
    assert r.json() == {"ok": True, "name": "s3-register-s1-orders"}
    assert len(orch.edit_spark_calls) == 1
    assert orch.edit_spark_calls[0].source == "s1"
    assert k8s.patched == []  # connector-specific call NOT fired


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
    bronze_fqn = "rawlake.mssql_ogrenci_raw.students"
    silver_fqn = "lakehouse.mssql_ogrenci.students"
    bronze_bucket = render_service.bronze_bucket_name("mssql_ogrenci")
    silver_bucket = render_service.silver_bucket_name("mssql_ogrenci")
    # response is DISTINCT from pipeline_only -- surfaces what was destroyed.
    # B-v2: per-pipeline teardown drops BOTH tables and deletes BOTH buckets
    # (never the cross-pipeline empty_bucket the old Silver-only path used).
    assert body["dropped_tables"] == [bronze_fqn, silver_fqn]
    assert body["deleted_buckets"] == [bronze_bucket, silver_bucket]
    assert body["deleted_topic"] == "cdc.mssql1.dbo.students"
    # and the destructive calls actually happened, against the parsed targets.
    assert k8s.deleted_connectors == ["dbz-mssql1-students"]
    assert k8s.deleted_topics == ["cdc.mssql1.dbo.students"]
    assert trino.dropped == [bronze_fqn, silver_fqn]
    assert s3.emptied == []
    assert s3.deleted_buckets == [bronze_bucket, silver_bucket]


# --------------------------------------------------------------------------
# Delete -- best-effort empty-namespace cleanup (13b)
# --------------------------------------------------------------------------

def test_delete_with_data_drops_empty_bronze_namespace():
    # list_tables (default FakeTrino) always reports empty post-drop -> both
    # the Bronze and Silver namespaces get dropped.
    k8s, s3, trino = FakeK8s(), FakeS3(), FakeTrino()
    client = _client_as({Role.ADMIN}, k8s=k8s, s3=s3, trino=trino)
    r = client.delete("/api/sources/dbz-mssql1-students?mode=with_data")
    assert r.status_code == 200
    assert r.json()["ok"] is True
    assert "rawlake.mssql_ogrenci_raw" in trino.dropped_namespaces
    assert "lakehouse.mssql_ogrenci" in trino.dropped_namespaces


def test_delete_with_data_keeps_shared_namespace():
    # A sibling table still lives in the Bronze namespace -> list_tables
    # reports it non-empty -> drop_namespace must NOT be called for it.
    k8s, s3 = FakeK8s(), FakeS3()
    trino = FakeTrino(tables_by_ns={("rawlake", "mssql_ogrenci_raw"): ["other_table"]})
    client = _client_as({Role.ADMIN}, k8s=k8s, s3=s3, trino=trino)
    r = client.delete("/api/sources/dbz-mssql1-students?mode=with_data")
    assert r.status_code == 200
    assert "rawlake.mssql_ogrenci_raw" not in trino.dropped_namespaces
    # Silver namespace is unaffected by the Bronze override -- still empty/dropped.
    assert "lakehouse.mssql_ogrenci" in trino.dropped_namespaces


class _RaisingTrino(FakeTrino):
    """Simulates a Trino error during the best-effort namespace cleanup --
    drop_table still succeeds, but list_tables/drop_namespace blow up. The
    delete must still return ok (degrade, never 500)."""

    def list_tables(self, catalog, ns):
        raise RuntimeError("trino coordinator unreachable")

    def drop_namespace(self, fqn):
        raise RuntimeError("should never be reached -- list_tables raised first")


def test_delete_namespace_drop_error_does_not_500():
    k8s, s3, trino = FakeK8s(), FakeS3(), _RaisingTrino()
    client = _client_as({Role.ADMIN}, k8s=k8s, s3=s3, trino=trino)
    r = client.delete("/api/sources/dbz-mssql1-students?mode=with_data")
    assert r.status_code == 200
    assert r.json()["ok"] is True
    # the drop_table calls (the real teardown) still happened despite the
    # namespace-cleanup helper failing.
    assert trino.dropped == [
        "rawlake.mssql_ogrenci_raw.students", "lakehouse.mssql_ogrenci.students"
    ]
    assert trino.dropped_namespaces == []


def test_delete_entity_drops_both_tables_and_both_buckets():
    # B-v2: with_data teardown of an entity-lane CR must drop BOTH the Bronze
    # changelog table and the Silver merge target, and delete BOTH per-pipeline
    # buckets -- never the old cross-pipeline empty_bucket call.
    p, table = "acct_pipeline", "accounts"
    cr = {
        "metadata": {"name": "dbz-acct-accounts"},
        "spec": {
            "class": "io.debezium.connector.postgresql.PostgresConnector",
            "config": {
                "topic.prefix": "cdc.acct",
                "table.include.list": "public.accounts",
                "transforms.route.static.value": f"{p}{render_service.BRONZE_NAMESPACE_SUFFIX}.{table}",
            },
        },
        "status": {"connectorStatus": {"connector": {"state": "RUNNING"}}},
    }
    k8s, s3, trino = FakeK8s(sources=[cr]), FakeS3(), FakeTrino()
    client = _client_as({Role.ADMIN}, k8s=k8s, s3=s3, trino=trino)

    r = client.delete("/api/sources/dbz-acct-accounts?mode=with_data")

    assert r.status_code == 200
    assert ("drop_table", f"rawlake.{p}_raw.{table}") in trino.calls
    assert ("drop_table", f"lakehouse.{p}.{table}") in trino.calls
    assert render_service.bronze_bucket_name(p) in s3.deleted_buckets
    assert render_service.silver_bucket_name(p) in s3.deleted_buckets
    assert not any(c[0] == "empty_bucket" for c in s3.calls)  # cross-pipeline bug gone


def test_delete_tolerates_absent_silver_event():
    # An event-lane CR has no real Silver table/bucket, but the teardown still
    # calls drop_table/delete_bucket for Silver -- both are idempotent no-ops
    # (Task 2/3), so this must succeed rather than raise.
    p, table = "events", "orders"
    k8s, s3, trino = FakeK8s(sources=[KAFKA_INGEST_CR]), FakeS3(), FakeTrino()
    client = _client_as({Role.ADMIN}, k8s=k8s, s3=s3, trino=trino)

    r = client.delete("/api/sources/kafka-ingest-k1-orders?mode=with_data")

    assert r.status_code == 200
    assert render_service.bronze_bucket_name(p) in s3.deleted_buckets
    # silver delete_bucket + silver drop_table are still CALLED (idempotent
    # no-ops), must not raise.
    assert ("drop_table", f"lakehouse.{p}.{table}") in trino.calls
    assert render_service.silver_bucket_name(p) in s3.deleted_buckets


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


def test_delete_spark_pipeline_only():
    k8s, s3, trino = FakeK8s(sources=[SPARK_CR]), FakeS3(), FakeTrino()
    client = _client_as({Role.ANALYST}, k8s=k8s, s3=s3, trino=trino)

    r = client.delete("/api/sources/s3-register-s1-orders?mode=pipeline_only")

    assert r.status_code == 200
    assert r.json() == {"ok": True, "name": "s3-register-s1-orders", "mode": "pipeline_only"}
    assert k8s.deleted_spark_jobs == ["s3-register-s1-orders"]
    assert k8s.deleted_connectors == []
    # pipeline_only must NOT touch data: no drop_table/empty_bucket/delete_topic.
    assert trino.dropped == []
    assert s3.emptied == []
    assert k8s.deleted_topics == []


def test_delete_spark_with_data_drops_rawlake():
    k8s, s3, trino = FakeK8s(sources=[SPARK_CR]), FakeS3(), FakeTrino()
    client = _client_as({Role.ADMIN}, k8s=k8s, s3=s3, trino=trino)

    r = client.delete("/api/sources/s3-register-s1-orders?mode=with_data")

    assert r.status_code == 200
    body = r.json()
    assert body["dropped_table"] == "rawlake.ext.orders"
    assert body["emptied_bucket"] == render_service.bucket_name("ext")
    assert body["deleted_topic"] is None  # spark-batch: no topic at all
    assert trino.dropped == ["rawlake.ext.orders"]
    assert s3.emptied == [render_service.bucket_name("ext")]
    assert k8s.deleted_topics == []  # no delete_topic call for a spark source
    assert k8s.deleted_spark_jobs == ["s3-register-s1-orders"]


def test_delete_connector_topic_uses_sanitized_cr_name():
    # Regression: a connector's data topic containing '_' must be deleted by
    # its sanitized KafkaTopic CR name (Task 1's _k8s_topic_name), not the raw
    # topic string -- Strimzi's KafkaTopic CR name must be RFC1123-safe.
    cr = {
        "metadata": {"name": "dbz-s1-orders"},
        "spec": {
            "class": "io.debezium.connector.sqlserver.SqlServerConnector",
            "config": {
                "topic.prefix": "cdc.s1",
                "table.include.list": "order_items",
                "transforms.route.static.value": "s1_raw.order_items",
            },
        },
        "status": {"connectorStatus": {"connector": {"state": "RUNNING"}}},
    }
    k8s, s3, trino = FakeK8s(sources=[cr]), FakeS3(), FakeTrino()
    client = _client_as({Role.ADMIN}, k8s=k8s, s3=s3, trino=trino)

    r = client.delete("/api/sources/dbz-s1-orders?mode=with_data")

    assert r.status_code == 200
    body = r.json()
    assert body["deleted_topic"] == "cdc.s1.order_items"  # real topic in the response
    assert k8s.deleted_topics == ["cdc.s1.order-items"]   # deleted by sanitized CR name


def test_delete_missing_source_is_404_for_admin():
    # Minor7: 404 before any teardown, consistent with GET.
    k8s, s3, trino = FakeK8s(sources=[]), FakeS3(), FakeTrino()
    client = _client_as({Role.ADMIN}, k8s=k8s, s3=s3, trino=trino)
    r = client.delete("/api/sources/nope?mode=with_data")
    assert r.status_code == 404
    assert k8s.deleted_connectors == []
    assert trino.dropped == [] and s3.emptied == []


# --------------------------------------------------------------------------
# Existing-Kafka ACL revocation (Task 2): deleting a stream/kafka source must
# revoke the scoped `connect` topic ACL its add_source granted. The customer
# topic is recovered from the CR's own `spec.config["topics"]` GATED on
# `metadata.name` starting with "kafka-ingest-" (see
# `app.routers.sources._kafka_ingest_topic`) -- that name-prefix check is the
# thing that actually does the discrimination; a CDC connector's CR shape has
# no `topics` key at all (belt-and-suspenders, doesn't exercise the gate), but
# a Camel dedicated sink's CR DOES have a `topics` key (same
# IcebergSinkConnector class + `_iceberg_sink_config` helper as kafka-ingest)
# and is named `<source>-sink`, not `kafka-ingest-*` -- CAMEL_SINK_CR below
# exercises exactly that discrimination.
# --------------------------------------------------------------------------

KAFKA_INGEST_CR = {
    # `annotations` mirrors what render_service._connector always stamps in
    # production (Task 5: recovered by `_kafka_ingest_producer` to build the
    # producer-KafkaUser teardown name -- NOT from `metadata.name`, which is
    # the composite "kafka-ingest-k1-orders", not the bare source "k1").
    "metadata": {"name": "kafka-ingest-k1-orders",
                 "annotations": {"lakehouse.solus.dev/source": "k1"}},
    "spec": {
        "class": "org.apache.iceberg.connect.IcebergSinkConnector",
        "config": {
            "topics": "orders-topic",
            "transforms.route.static.value": "events_raw.orders",
        },
    },
    "status": {"connectorStatus": {"connector": {"state": "RUNNING"}}},
}

# A Camel lane's DEDICATED SINK CR (render_service.render_camel_sink's output
# shape): same class + a real `topics` key as KAFKA_INGEST_CR above, but named
# `<source-connector-name>-sink` (never `kafka-ingest-*`). Proves
# `_kafka_ingest_topic`'s name-prefix gate -- not the coincidental absence of
# a `topics` key -- is what excludes non-kafka-ingest sources from ACL
# revocation (see test_delete_camel_sink_does_not_revoke_acl_despite_topics_key).
CAMEL_SINK_CR = {
    "metadata": {"name": "http-h1-prices-sink"},
    "spec": {
        "class": "org.apache.iceberg.connect.IcebergSinkConnector",
        "config": {
            "topics": "http.h1.prices",
            "transforms.route.static.value": "ext_raw.prices",
        },
    },
    "status": {"connectorStatus": {"connector": {"state": "RUNNING"}}},
}


def test_delete_stream_kafka_revokes_connect_topic_acl():
    k8s, s3, trino = FakeK8s(sources=[KAFKA_INGEST_CR]), FakeS3(), FakeTrino()
    client = _client_as({Role.ANALYST}, k8s=k8s, s3=s3, trino=trino)

    r = client.delete("/api/sources/kafka-ingest-k1-orders?mode=pipeline_only")

    assert r.status_code == 200
    assert k8s.removed_acls == [
        ("connect", {"resource": {"type": "topic", "name": "orders-topic", "patternType": "literal"},
                     "operations": ["Read", "Describe"]})
    ]
    assert k8s.deleted_connectors == ["kafka-ingest-k1-orders"]


def test_delete_cdc_source_does_not_revoke_any_acl():
    k8s, s3, trino = FakeK8s(), FakeS3(), FakeTrino()  # default sources=[CONNECTOR_CR] (cdc-mssql)
    client = _client_as({Role.ANALYST}, k8s=k8s, s3=s3, trino=trino)

    r = client.delete("/api/sources/dbz-mssql1-students?mode=pipeline_only")

    assert r.status_code == 200
    assert k8s.removed_acls == []


def test_delete_camel_sink_does_not_revoke_acl_despite_topics_key():
    # Regression guard: CAMEL_SINK_CR has the SAME class + a real `topics`
    # config key as KAFKA_INGEST_CR (both go through
    # render_service._iceberg_sink_config) -- only its CR name differs
    # ("http-h1-prices-sink", not "kafka-ingest-*"). If `_kafka_ingest_topic`'s
    # name-prefix gate were ever loosened/removed, this would start wrongly
    # calling remove_user_acl for a Camel dedicated sink's own topic.
    k8s, s3, trino = FakeK8s(sources=[CAMEL_SINK_CR]), FakeS3(), FakeTrino()
    client = _client_as({Role.ANALYST}, k8s=k8s, s3=s3, trino=trino)

    r = client.delete("/api/sources/http-h1-prices-sink?mode=pipeline_only")

    assert r.status_code == 200
    assert k8s.removed_acls == []
    assert k8s.deleted_connectors == ["http-h1-prices-sink"]


def test_delete_kafka_ingest_tears_down_producer():
    # Task 5: deleting a kafka-ingest source must also tear down the
    # per-pipeline producer KafkaUser + its Secret that add_source
    # provisioned -- name recovered from the CR's round-trip
    # `{_SPARK_ANN}source` annotation (see KAFKA_INGEST_CR), NOT from the
    # route's `name` path param (the composite CR name).
    k8s, s3, trino = FakeK8s(sources=[KAFKA_INGEST_CR]), FakeS3(), FakeTrino()
    client = _client_as({Role.ANALYST}, k8s=k8s, s3=s3, trino=trino)

    r = client.delete("/api/sources/kafka-ingest-k1-orders?mode=pipeline_only")

    assert r.status_code == 200
    assert k8s.deleted_users == ["k1-producer"]
    assert k8s.deleted_secrets == ["k1-producer"]


class _RawCustomApi:
    """Low-level `custom_api` double wired straight into a REAL `K8sService`
    (not `FakeK8s`) -- so this test exercises the actual Fix 2 code path
    (`K8sService.delete_user`/`delete_connector`), not a hand-rolled router
    fake that could silently diverge from it.

    `list_namespaced_custom_object` returns `[KAFKA_INGEST_CR]` (connectors)
    / `[]` (spark CRD absent, matching every other test's `FakeK8s`
    default). `get_namespaced_custom_object` (used by `remove_user_acl`'s
    read-modify-write) returns an empty-ACL KafkaUser so the ACL-revocation
    step no-ops cleanly regardless of which KafkaUser name it's asked for.
    `delete_namespaced_custom_object` raises 404 for `kafkausers` -- the
    producer KafkaUser this kafka-ingest source's CR predates (Fix 2's
    scenario) -- but succeeds (and is recorded) for `kafkaconnectors`, so the
    test can assert `delete_connector` still ran.
    """

    def __init__(self):
        self.deleted: List[Dict[str, Any]] = []

    def list_namespaced_custom_object(self, **kw):
        if kw["plural"] == "scheduledsparkapplications":
            return {"items": []}
        return {"items": [KAFKA_INGEST_CR]}

    def get_namespaced_custom_object(self, **kw):
        return {"metadata": {"resourceVersion": "1"}, "spec": {"authorization": {"acls": []}}}

    def delete_namespaced_custom_object(self, **kw):
        if kw["plural"] == "kafkausers":
            raise ApiException(status=404, reason="NotFound")
        self.deleted.append(kw)
        return {}

    def patch_namespaced_custom_object(self, **kw):
        raise AssertionError("no ACL patch expected -- empty-ACL KafkaUser is already a no-op")


class _RawCoreApi:
    """Low-level `core_api` double: the producer Secret doesn't exist either
    (Fix 2's other half) -- `delete_namespaced_secret` always 404s."""

    def delete_namespaced_secret(self, **kw):
        raise ApiException(status=404, reason="NotFound")


def test_delete_kafka_ingest_survives_missing_producer_user_and_secret():
    # FINAL-REVIEW Fix 2 regression guard: a kafka-ingest source created
    # BEFORE per-pipeline producer provisioning existed has the
    # `{_SPARK_ANN}source` annotation (long pre-existing, see
    # KAFKA_INGEST_CR) but no producer KafkaUser/Secret -- deleting it must
    # NOT 500 (both delete_user and delete_secret would previously raise the
    # K8s 404 straight through, aborting before delete_connector ran), and
    # delete_connector must still run so the source is actually removed.
    real_k8s = K8sService(custom_api=_RawCustomApi(), core_api=_RawCoreApi(), namespace="lakehouse")
    s3, trino = FakeS3(), FakeTrino()
    client = _client_as({Role.ANALYST}, k8s=real_k8s, s3=s3, trino=trino)

    r = client.delete("/api/sources/kafka-ingest-k1-orders?mode=pipeline_only")

    assert r.status_code == 200
    assert r.json()["ok"] is True
    assert {"plural": "kafkaconnectors", "name": "kafka-ingest-k1-orders",
            "group": "kafka.strimzi.io", "version": "v1beta2", "namespace": "lakehouse"} \
        in real_k8s.custom_api.deleted


def test_delete_camel_sink_does_not_tear_down_any_producer():
    # Regression guard, mirroring test_delete_camel_sink_does_not_revoke_acl_
    # despite_topics_key: a Camel dedicated sink is never kafka-ingest, so its
    # delete must not touch any producer KafkaUser/Secret.
    k8s, s3, trino = FakeK8s(sources=[CAMEL_SINK_CR]), FakeS3(), FakeTrino()
    client = _client_as({Role.ANALYST}, k8s=k8s, s3=s3, trino=trino)

    r = client.delete("/api/sources/http-h1-prices-sink?mode=pipeline_only")

    assert r.status_code == 200
    assert k8s.deleted_users == []
    assert k8s.deleted_secrets == []


def test_delete_gitops_mode_stream_kafka_revokes_connect_topic_acl(monkeypatch):
    # ACL revocation is a direct K8s API mutation (Task 1's `remove_user_acl`),
    # not GitOps-managed, so it must run even in gitops mode -- same rationale
    # as with_data's drop_table/empty_bucket (see delete_source's docstring).
    monkeypatch.setattr(settings, "deploy_mode", "gitops")
    cr = {
        "metadata": {
            "name": "kafka-ingest-k1-orders",
            "annotations": {"lakehouse.solus.dev/source": "k1"},
        },
        "spec": {
            "class": "org.apache.iceberg.connect.IcebergSinkConnector",
            "config": {"topics": "orders-topic"},
        },
    }
    k8s, s3, trino = FakeK8s(sources=[cr]), FakeS3(), FakeTrino()
    git_writer = _FakeGitWriter()
    orch = FakeOrchestrator(git_writer=git_writer)
    client = _client_as({Role.ANALYST}, k8s=k8s, s3=s3, trino=trino, orchestrator=orch)

    r = client.delete("/api/sources/kafka-ingest-k1-orders?mode=pipeline_only")

    assert r.status_code == 200
    assert k8s.removed_acls == [
        ("connect", {"resource": {"type": "topic", "name": "orders-topic", "patternType": "literal"},
                     "operations": ["Read", "Describe"]})
    ]
    # Task 5: producer teardown is a direct K8s mutation too (not GitOps
    # managed), so it must fire in gitops mode exactly like the ACL revocation.
    assert k8s.deleted_users == ["k1-producer"]
    assert k8s.deleted_secrets == ["k1-producer"]
    assert git_writer.removed == ["k1"]


# --------------------------------------------------------------------------
# gitops delete_source (Task 4 fix): remove_source must be called with the
# BARE spec.source recovered from the CR's `lakehouse.solus.dev/source`
# round-trip annotation -- NEVER the composite CR name -- and must fail loud
# (409), not silently no-op, if that annotation is missing.
# --------------------------------------------------------------------------

def test_delete_gitops_mode_commits_removal_of_bare_source(monkeypatch):
    monkeypatch.setattr(settings, "deploy_mode", "gitops")
    cr = {
        "metadata": {
            "name": "dbz-pgdemo-customers",
            "annotations": {"lakehouse.solus.dev/source": "pgdemo"},
        },
        "spec": {"class": "io.debezium.connector.postgresql.PostgresConnector", "config": {}},
    }
    k8s, s3, trino = FakeK8s(sources=[cr]), FakeS3(), FakeTrino()
    git_writer = _FakeGitWriter()
    orch = FakeOrchestrator(git_writer=git_writer)
    client = _client_as({Role.ANALYST}, k8s=k8s, s3=s3, trino=trino, orchestrator=orch)

    r = client.delete("/api/sources/dbz-pgdemo-customers?mode=pipeline_only")

    assert r.status_code == 200
    body = r.json()
    assert body == {"ok": True, "name": "dbz-pgdemo-customers", "mode": "pipeline_only", "ref": "cafef00d"}
    # the BARE source id ("pgdemo"), never the composite CR name, must be
    # what GitWriter.remove_source deletes from the pipelines repo.
    assert git_writer.removed == ["pgdemo"]
    # gitops mode never calls the direct connector/topic teardown.
    assert k8s.deleted_connectors == []
    assert k8s.deleted_topics == []
    assert trino.dropped == [] and s3.emptied == []


def test_delete_gitops_mode_missing_annotation_fails_loud(monkeypatch):
    monkeypatch.setattr(settings, "deploy_mode", "gitops")
    cr = {  # no `lakehouse.solus.dev/source` annotation -- e.g. hand-applied or legacy CR
        "metadata": {"name": "dbz-legacy-orders"},
        "spec": {"class": "io.debezium.connector.postgresql.PostgresConnector", "config": {}},
    }
    k8s, s3, trino = FakeK8s(sources=[cr]), FakeS3(), FakeTrino()
    git_writer = _FakeGitWriter()
    orch = FakeOrchestrator(git_writer=git_writer)
    client = _client_as({Role.ANALYST}, k8s=k8s, s3=s3, trino=trino, orchestrator=orch)

    r = client.delete("/api/sources/dbz-legacy-orders?mode=pipeline_only")

    assert r.status_code == 409
    assert "annotation" in r.json()["detail"]
    # fail-loud, not a silent no-op: remove_source must never have been called.
    assert git_writer.removed == []


def test_delete_gitops_mode_with_data_still_drops_table_and_bucket(monkeypatch):
    # with_data (drop table/bucket) is a data-plane action, separate from the
    # GitOps-managed connector/topic teardown -- it must still run in gitops
    # mode, exactly as it does in direct mode.
    monkeypatch.setattr(settings, "deploy_mode", "gitops")
    cr = {
        "metadata": {
            "name": "dbz-pgdemo-customers",
            "annotations": {"lakehouse.solus.dev/source": "pgdemo"},
        },
        "spec": {
            "class": "io.debezium.connector.postgresql.PostgresConnector",
            "config": {
                "topic.prefix": "cdc.pgdemo",
                "table.include.list": "public.customers",
                "transforms.route.static.value": "depo_raw.customers",
            },
        },
    }
    k8s, s3, trino = FakeK8s(sources=[cr]), FakeS3(), FakeTrino()
    git_writer = _FakeGitWriter()
    orch = FakeOrchestrator(git_writer=git_writer)
    client = _client_as({Role.ADMIN}, k8s=k8s, s3=s3, trino=trino, orchestrator=orch)

    r = client.delete("/api/sources/dbz-pgdemo-customers?mode=with_data")

    assert r.status_code == 200
    body = r.json()
    bronze_fqn = "rawlake.depo_raw.customers"
    silver_fqn = "lakehouse.depo.customers"
    bronze_bucket = render_service.bronze_bucket_name("depo")
    silver_bucket = render_service.silver_bucket_name("depo")
    assert body["ref"] == "cafef00d"
    assert body["dropped_tables"] == [bronze_fqn, silver_fqn]
    assert body["deleted_buckets"] == [bronze_bucket, silver_bucket]
    assert body["deleted_topic"] == "cdc.pgdemo.public.customers"
    assert git_writer.removed == ["pgdemo"]
    assert trino.dropped == [bronze_fqn, silver_fqn]
    assert s3.emptied == []
    assert s3.deleted_buckets == [bronze_bucket, silver_bucket]
    assert k8s.deleted_connectors == []   # no direct connector delete in gitops mode
    assert k8s.deleted_topics == []       # no direct topic delete in gitops mode


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
