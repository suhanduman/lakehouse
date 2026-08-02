"""Tests for the `/api/tables`, `/api/buckets`, `/api/schemas`, `/api/status`
routers: authz gating (student / analyst / admin) exercised through a real
FastAPI `TestClient`, with `get_roles`/`get_trino`/`get_s3`/
`get_apicurio`/`get_connect` swapped out via `app.dependency_overrides` --
no real Trino coordinator, S3/MinIO, Apicurio registry, or Connect worker.
Mirrors `test_sources_router.py`'s fake/override pattern.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

import pytest
from botocore.exceptions import ClientError
from fastapi.testclient import TestClient

from app.deps import get_apicurio, get_connect, get_iceberg, get_k8s, get_roles, get_s3, get_trino
from app.main import app
from app.services.authz import Role
from app.services.iceberg_service import IdentifierMismatch

# A single-CR CDC pipeline (mirrors CONNECTOR_CR in test_sources_router.py +
# the `lakehouse.solus.dev/source` grouping annotation `assemble_pipelines`
# groups by, see test_pipeline_topology.py) used by the tables/buckets
# used-vs-orphan tests below: route value "pgdemo_customers_raw.customers" ->
# ns="pgdemo_customers", table="customers"; entity disposition (Silver
# already exists per FakeTrino's tables) -> authoritative = the Silver FQN.
PGDEMO_CR = {
    "metadata": {
        "name": "dbz-pgdemo-customers",
        "annotations": {"lakehouse.solus.dev/source": "pgdemo_customers"},
    },
    "spec": {
        "class": "io.debezium.connector.postgresql.PostgresConnector",
        "config": {
            "topic.prefix": "cdc.pgdemo",
            "table.include.list": "public.customers",
            "transforms.route.static.value": "pgdemo_customers_raw.customers",
        },
    },
    "status": {"connectorStatus": {"connector": {"state": "RUNNING"}}},
}


# --------------------------------------------------------------------------
# Fakes -- hand-rolled doubles for TrinoService / S3Service / ApicurioClient
# / ConnectService / K8sService.
# --------------------------------------------------------------------------

class FakeTrino:
    def __init__(
        self,
        namespaces: Optional[List[str]] = None,
        tables: Optional[Dict[str, List[str]]] = None,
    ):
        self._namespaces = namespaces if namespaces is not None else ["mssql_ogrenci"]
        self._tables = tables if tables is not None else {"mssql_ogrenci": ["students"]}
        self.ddl: List[str] = []
        self.dropped: List[str] = []

    def list_namespaces(self, catalog):
        return self._namespaces

    def list_tables(self, catalog, ns):
        return self._tables.get(ns, [])

    def run_ddl(self, sql):
        self.ddl.append(sql)

    def drop_table(self, fqn):
        self.dropped.append(fqn)


class FakeIceberg:
    """IcebergService double — tablo pre-create'i pyiceberg'e gitmeden kaydeder.
    `mismatch=True` ise create_table IdentifierMismatch fırlatır (409 testi).
    `stats` seeds `table_stats`'s return value, keyed by (namespace, table,
    layer); every call is recorded in `stats_calls` so tests can assert
    table_stats was actually invoked (not just that a fake default leaked
    through)."""

    def __init__(
        self,
        mismatch: bool = False,
        stats: Optional[Dict[Tuple[str, str, str], Dict[str, int]]] = None,
    ):
        self.created: List[Dict[str, Any]] = []
        self._mismatch = mismatch
        self._stats = stats or {}
        self.stats_calls: List[Tuple[str, str, str]] = []

    def create_table(self, namespace, table, columns, identifier, location=None):
        # gerçek IcebergService sözleşmesini yansıt: identifier zorunlu.
        if not identifier:
            raise ValueError("identifier (PK) boş olamaz")
        if self._mismatch:
            raise IdentifierMismatch(f"{namespace}.{table} identifier uyuşmuyor")
        self.created.append(
            {
                "namespace": namespace,
                "table": table,
                "columns": columns,
                "identifier": identifier,
                "location": location,
            }
        )
        return f"{namespace}.{table}"

    def table_stats(self, namespace, table, layer="silver"):
        self.stats_calls.append((namespace, table, layer))
        return self._stats.get((namespace, table, layer))


class FakeK8s:
    """K8sService double for assemble_pipelines' three inputs -- `sources`
    defaults to empty (no active pipeline), so every table/bucket in the
    default FakeTrino/FakeS3 fixtures is an orphan unless a test passes its
    own `sources`."""

    def __init__(self, sources: Optional[List[Dict[str, Any]]] = None):
        self._sources = sources if sources is not None else []

    def list_sources(self):
        return self._sources

    def get_status(self, name):
        return {"connector": {"state": "RUNNING"}}

    def get_spark_status(self, name):
        return {"scheduleState": "Scheduled"}


class FakeS3:
    def __init__(
        self,
        buckets: Optional[List[str]] = None,
        objects: Optional[Dict[str, Dict[str, Any]]] = None,
        raise_for: Optional[List[str]] = None,
    ):
        self._buckets = buckets if buckets is not None else ["src-mssql-ogrenci"]
        self.created: List[str] = []
        self._objects = objects or {}
        # Bucket names whose object_count call should raise a non-
        # NoSuchBucket ClientError (e.g. AccessDenied) -- mirrors a real S3
        # permission/timeout failure on just ONE bucket in the listing.
        self._raise_for = set(raise_for or [])
        self.object_count_calls: List[str] = []

    def list_buckets(self):
        return self._buckets

    def create_bucket(self, name):
        self.created.append(name)
        return True

    def object_count(self, name):
        self.object_count_calls.append(name)
        if name in self._raise_for:
            raise ClientError({"Error": {"Code": "AccessDenied"}}, "ListObjectsV2")
        return self._objects.get(name, {"count": 0, "capped": False})


class FakeApicurio:
    def __init__(self, schemas: Optional[List[Dict[str, Any]]] = None):
        self._schemas = (
            schemas if schemas is not None else [{"id": "cdc.mssql1.dbo.students-value"}]
        )

    def list_schemas(self):
        return self._schemas


class FakeConnect:
    def __init__(
        self,
        connectors: Optional[List[str]] = None,
        statuses: Optional[Dict[str, Dict[str, Any]]] = None,
        raise_on_list: bool = False,
    ):
        self._connectors = connectors if connectors is not None else ["dbz-mssql1-students"]
        self._statuses = statuses if statuses is not None else {
            "dbz-mssql1-students": {
                "connector": {"state": "RUNNING"},
                "tasks": [{"id": 0, "state": "RUNNING"}],
            }
        }
        self.raise_on_list = raise_on_list

    def list_connectors(self):
        if self.raise_on_list:
            raise RuntimeError("connect worker unreachable")
        return self._connectors

    def connector_status(self, name):
        if name not in self._statuses:
            raise RuntimeError(f"{name} unreachable")
        return self._statuses[name]


class ExplodingProvider:
    """A provider override that raises the moment FastAPI tries to construct
    it -- proves an unauthorized request is short-circuited by the authz
    gate BEFORE any provider is built (see test_sources_router.py)."""

    def __call__(self):
        raise RuntimeError("provider was constructed before authz -- SECURITY BUG")


# --------------------------------------------------------------------------
# Client helper -- swaps every dependency via app.dependency_overrides.
# --------------------------------------------------------------------------

def _client_as(
    roles, trino=None, s3=None, apicurio=None, connect=None, iceberg=None, k8s=None
) -> TestClient:
    app.dependency_overrides[get_roles] = lambda: roles
    app.dependency_overrides[get_trino] = lambda: (trino if trino is not None else FakeTrino())
    app.dependency_overrides[get_iceberg] = lambda: (iceberg if iceberg is not None else FakeIceberg())
    app.dependency_overrides[get_k8s] = lambda: (k8s if k8s is not None else FakeK8s())
    app.dependency_overrides[get_s3] = lambda: (s3 if s3 is not None else FakeS3())
    app.dependency_overrides[get_apicurio] = lambda: (
        apicurio if apicurio is not None else FakeApicurio()
    )
    app.dependency_overrides[get_connect] = lambda: (
        connect if connect is not None else FakeConnect()
    )
    return TestClient(app)


@pytest.fixture(autouse=True)
def _clear_overrides():
    yield
    app.dependency_overrides.clear()


@pytest.fixture
def client_as_student() -> TestClient:
    return _client_as({Role.STUDENT})


@pytest.fixture
def client_as_analyst() -> TestClient:
    return _client_as({Role.ANALYST})


@pytest.fixture
def client_as_admin() -> TestClient:
    return _client_as({Role.ADMIN})


# --------------------------------------------------------------------------
# /api/tables -- GET (READ), POST (TABLE_CREATE), DELETE (SOURCE_DELETE_WITH_DATA)
# --------------------------------------------------------------------------

def test_list_tables_readable_by_student(client_as_student):
    r = client_as_student.get("/api/tables?catalog=lakehouse")
    assert r.status_code == 200
    body = r.json()
    assert body["catalog"] == "lakehouse"
    assert len(body["namespaces"]) == 1
    ns = body["namespaces"][0]
    assert ns["name"] == "mssql_ogrenci"
    assert len(ns["tables"]) == 1
    table = ns["tables"][0]
    # no active pipeline owns it (default FakeK8s has no sources) -> orphan.
    assert table["table"] == "students"
    assert table["used"] is False
    assert "leftover" in table["hint"].lower()
    assert table["records"] is None


def test_tables_labels_used_and_orphan_with_counts():
    # pipeline owns lakehouse.pgdemo_customers.customers (authoritative,
    # since Silver already exists per FakeTrino's tables -> entity
    # disposition) + an orphan lakehouse.old_ns.gone exists alongside it.
    trino = FakeTrino(
        namespaces=["pgdemo_customers", "old_ns"],
        tables={"pgdemo_customers": ["customers"], "old_ns": ["gone"]},
    )
    iceberg = FakeIceberg(
        stats={("pgdemo_customers", "customers", "silver"): {"records": 42, "data_files": 3}}
    )
    k8s = FakeK8s(sources=[PGDEMO_CR])
    client = _client_as({Role.STUDENT}, trino=trino, iceberg=iceberg, k8s=k8s)
    r = client.get("/api/tables?catalog=lakehouse")
    assert r.status_code == 200
    body = r.json()

    def _find(ns_name, table_name):
        ns = next(n for n in body["namespaces"] if n["name"] == ns_name)
        return next(t for t in ns["tables"] if t["table"] == table_name)

    used = _find("pgdemo_customers", "customers")
    assert used["used"] is True
    assert used["role"] == "authoritative"
    assert used["pipeline"] == "pgdemo_customers"
    assert used["records"] == 42

    orphan = _find("old_ns", "gone")
    assert orphan["used"] is False
    assert "leftover" in orphan["hint"].lower()
    assert orphan["records"] is None

    # table_stats was actually called (both namespaces are non-"_raw" -> silver layer).
    assert ("pgdemo_customers", "customers", "silver") in iceberg.stats_calls
    assert ("old_ns", "gone", "silver") in iceberg.stats_calls


def test_create_table_forbidden_for_student(client_as_student):
    r = client_as_student.post(
        "/api/tables",
        json={
            "namespace": "mssql_ogrenci",
            "table": "students",
            "columns": [{"name": "id", "type": "bigint"}],
            "identifier": ["id"],
        },
    )
    assert r.status_code == 403


def test_create_table_ok_for_analyst():
    # Tablo yaratımı Trino DDL değil, IcebergService (pyiceberg) üzerinden
    # yapılır — identifier field ile (aksi halde upsert append-only'e düşer).
    iceberg = FakeIceberg()
    client = _client_as({Role.ANALYST}, iceberg=iceberg)
    r = client.post(
        "/api/tables",
        json={
            "namespace": "mssql_ogrenci",
            "table": "students",
            "columns": [{"name": "id", "type": "bigint"}, {"name": "name", "type": "varchar"}],
            "identifier": ["id"],
        },
    )
    assert r.status_code == 201
    assert r.json() == {"ok": True, "fqn": "lakehouse.mssql_ogrenci.students"}
    # pyiceberg'e identifier + kaynak-başına bucket konumu ile gitti
    assert len(iceberg.created) == 1
    created = iceberg.created[0]
    assert created["namespace"] == "mssql_ogrenci"
    assert created["table"] == "students"
    assert created["identifier"] == ["id"]
    assert created["location"] == "s3://src-mssql-ogrenci/warehouse"


def test_create_table_identifier_mismatch_returns_409():
    # Tablo farklı identifier ile zaten varsa fail-loud (409) — sessiz append-only YOK.
    client = _client_as({Role.ANALYST}, iceberg=FakeIceberg(mismatch=True))
    r = client.post(
        "/api/tables",
        json={
            "namespace": "depo",
            "table": "customers",
            "columns": [{"name": "id", "type": "bigint"}],
            "identifier": ["id"],
        },
    )
    assert r.status_code == 409


def test_create_table_empty_identifier_returns_400():
    # identifier (PK) zorunlu; boş liste → 400 (IcebergService ValueError).
    client = _client_as({Role.ANALYST})
    r = client.post(
        "/api/tables",
        json={
            "namespace": "depo",
            "table": "customers",
            "columns": [{"name": "id", "type": "bigint"}],
            "identifier": [],
        },
    )
    assert r.status_code == 400


def test_delete_table_forbidden_for_analyst(client_as_analyst):
    # dropping a table is admin-only (SOURCE_DELETE_WITH_DATA), like sources.py's with_data delete.
    r = client_as_analyst.delete("/api/tables/lakehouse.mssql_ogrenci.students")
    assert r.status_code == 403


def test_delete_table_ok_for_admin():
    trino = FakeTrino()
    client = _client_as({Role.ADMIN}, trino=trino)
    r = client.delete("/api/tables/lakehouse.mssql_ogrenci.students")
    assert r.status_code == 200
    assert r.json() == {"ok": True, "fqn": "lakehouse.mssql_ogrenci.students"}
    assert trino.dropped == ["lakehouse.mssql_ogrenci.students"]


def test_unauthorized_create_table_does_not_build_provider():
    # student lacks TABLE_CREATE; the iceberg provider raises if constructed
    # (create_table now depends on get_iceberg, not get_trino). authz gate must
    # short-circuit with 403 BEFORE the provider is built.
    app.dependency_overrides[get_roles] = lambda: {Role.STUDENT}
    app.dependency_overrides[get_iceberg] = ExplodingProvider()
    client = TestClient(app, raise_server_exceptions=False)
    r = client.post(
        "/api/tables",
        json={
            "namespace": "x",
            "table": "y",
            "columns": [{"name": "id", "type": "bigint"}],
            "identifier": ["id"],
        },
    )
    assert r.status_code == 403


def test_unauthorized_delete_table_does_not_build_provider():
    # analyst lacks SOURCE_DELETE_WITH_DATA; the trino provider raises if constructed.
    app.dependency_overrides[get_roles] = lambda: {Role.ANALYST}
    app.dependency_overrides[get_trino] = ExplodingProvider()
    client = TestClient(app, raise_server_exceptions=False)
    r = client.delete("/api/tables/lakehouse.mssql_ogrenci.students")
    assert r.status_code == 403


# --------------------------------------------------------------------------
# /api/buckets -- GET (READ), POST (TABLE_CREATE)
# --------------------------------------------------------------------------

def test_list_buckets_readable_by_student(client_as_student):
    r = client_as_student.get("/api/buckets")
    assert r.status_code == 200
    body = r.json()
    assert len(body["buckets"]) == 1
    bucket = body["buckets"][0]
    # no active pipeline owns it (default FakeK8s has no sources) -> orphan.
    assert bucket["name"] == "src-mssql-ogrenci"
    assert bucket["used"] is False
    assert "leftover" in bucket["hint"].lower()
    assert bucket["objects"] == {"count": 0, "capped": False}


def test_buckets_labels_used_and_orphan_with_counts():
    # pipeline owns bronze-pgdemo_customers (role=bronze) + silver-pgdemo_customers
    # (role=silver, since Silver already exists -> entity disposition) + an
    # orphan src-orphan bucket exists alongside them.
    from app.services import render_service

    bronze_bucket = render_service.bronze_bucket_name("pgdemo_customers")
    silver_bucket = render_service.silver_bucket_name("pgdemo_customers")
    s3 = FakeS3(
        buckets=[bronze_bucket, silver_bucket, "src-orphan"],
        objects={silver_bucket: {"count": 7, "capped": False}},
    )
    k8s = FakeK8s(sources=[PGDEMO_CR])
    trino = FakeTrino(
        namespaces=["pgdemo_customers"], tables={"pgdemo_customers": ["customers"]}
    )
    client = _client_as({Role.STUDENT}, s3=s3, k8s=k8s, trino=trino)
    r = client.get("/api/buckets")
    assert r.status_code == 200
    body = r.json()

    def _find(name):
        return next(b for b in body["buckets"] if b["name"] == name)

    bronze = _find(bronze_bucket)
    assert bronze["used"] is True
    assert bronze["role"] == "bronze"
    assert bronze["pipeline"] == "pgdemo_customers"
    assert bronze["objects"] == {"count": 0, "capped": False}

    silver = _find(silver_bucket)
    assert silver["used"] is True
    assert silver["role"] == "silver"
    assert silver["pipeline"] == "pgdemo_customers"
    assert silver["objects"] == {"count": 7, "capped": False}

    orphan = _find("src-orphan")
    assert orphan["used"] is False
    assert "leftover" in orphan["hint"].lower()
    assert orphan["objects"] == {"count": 0, "capped": False}

    # object_count was actually called for every bucket, not just used ones.
    assert set(s3.object_count_calls) == {bronze_bucket, silver_bucket, "src-orphan"}


def test_buckets_degrades_gracefully_when_object_count_fails_for_one_bucket():
    # One bucket's object_count raises a non-NoSuchBucket ClientError (e.g.
    # AccessDenied, a transient timeout) -- S3Service.object_count itself
    # re-raises that (fail-loud, by design -- see test_s3_service.py's
    # test_object_count_reraises_other_client_errors), so list_buckets must
    # catch it AT THE CALL SITE and degrade that one bucket's `objects` to
    # None rather than 500ing the whole catalog page; every other bucket
    # must still come back intact.
    s3 = FakeS3(
        buckets=["ok-bucket", "broken-bucket"],
        objects={"ok-bucket": {"count": 3, "capped": False}},
        raise_for=["broken-bucket"],
    )
    client = _client_as({Role.STUDENT}, s3=s3)
    r = client.get("/api/buckets")
    assert r.status_code == 200
    body = r.json()

    def _find(name):
        return next(b for b in body["buckets"] if b["name"] == name)

    ok = _find("ok-bucket")
    assert ok["objects"] == {"count": 3, "capped": False}

    broken = _find("broken-bucket")
    assert broken["objects"] is None
    # used/orphan labeling still resolves normally -- only the count degrades.
    assert broken["used"] is False
    assert "leftover" in broken["hint"].lower()

    assert set(s3.object_count_calls) == {"ok-bucket", "broken-bucket"}


def test_create_bucket_forbidden_for_student(client_as_student):
    r = client_as_student.post("/api/buckets", json={"name": "src-new"})
    assert r.status_code == 403


def test_create_bucket_ok_for_analyst():
    s3 = FakeS3()
    client = _client_as({Role.ANALYST}, s3=s3)
    r = client.post("/api/buckets", json={"name": "src-new"})
    assert r.status_code == 201
    assert r.json() == {"ok": True, "name": "src-new"}
    assert s3.created == ["src-new"]


def test_unauthorized_create_bucket_does_not_build_provider():
    app.dependency_overrides[get_roles] = lambda: {Role.STUDENT}
    app.dependency_overrides[get_s3] = ExplodingProvider()
    client = TestClient(app, raise_server_exceptions=False)
    r = client.post("/api/buckets", json={"name": "src-new"})
    assert r.status_code == 403


# --------------------------------------------------------------------------
# /api/schemas -- GET (READ)
# --------------------------------------------------------------------------

def test_list_schemas_readable_by_student(client_as_student):
    r = client_as_student.get("/api/schemas")
    assert r.status_code == 200
    assert r.json() == {"schemas": [{"id": "cdc.mssql1.dbo.students-value"}]}


def test_list_schemas_readable_by_admin_too(client_as_admin):
    r = client_as_admin.get("/api/schemas")
    assert r.status_code == 200


# --------------------------------------------------------------------------
# /api/status -- GET (READ); unified connector states + lag + dlq +
# maintenance, degrading gracefully on per-connector or full unreachability.
# --------------------------------------------------------------------------

def test_status_readable_by_student(client_as_student):
    r = client_as_student.get("/api/status")
    assert r.status_code == 200
    body = r.json()
    assert body["reachable"] is True
    assert body["connectors"] == [
        {
            "name": "dbz-mssql1-students",
            "state": "RUNNING",
            "tasks": ["RUNNING"],
            "maintenance": False,
            "dlq": False,
            "lag": None,
            "reachable": True,
        }
    ]


def test_status_reports_maintenance_when_connector_paused():
    connect = FakeConnect(
        connectors=["jdbc-paused"],
        statuses={
            "jdbc-paused": {"connector": {"state": "PAUSED"}, "tasks": [{"state": "PAUSED"}]}
        },
    )
    client = _client_as({Role.STUDENT}, connect=connect)
    r = client.get("/api/status")
    entry = r.json()["connectors"][0]
    assert entry["maintenance"] is True
    assert entry["dlq"] is False


def test_status_reports_dlq_when_task_failed():
    connect = FakeConnect(
        connectors=["dbz-failing"],
        statuses={
            "dbz-failing": {
                "connector": {"state": "RUNNING"},
                "tasks": [{"state": "FAILED"}],
            }
        },
    )
    client = _client_as({Role.STUDENT}, connect=connect)
    r = client.get("/api/status")
    entry = r.json()["connectors"][0]
    assert entry["dlq"] is True


def test_status_degrades_gracefully_for_one_unreachable_connector():
    # connector_status() raises for "ghost" -- must not 500 the whole endpoint.
    connect = FakeConnect(connectors=["dbz-mssql1-students", "ghost"])
    client = _client_as({Role.STUDENT}, connect=connect)
    r = client.get("/api/status")
    assert r.status_code == 200
    by_name = {c["name"]: c for c in r.json()["connectors"]}
    assert by_name["dbz-mssql1-students"]["reachable"] is True
    assert by_name["ghost"]["reachable"] is False
    assert by_name["ghost"]["state"] == "unknown"
    assert "error" in by_name["ghost"]


def test_status_degrades_gracefully_when_connect_worker_fully_unreachable():
    connect = FakeConnect(raise_on_list=True)
    client = _client_as({Role.STUDENT}, connect=connect)
    r = client.get("/api/status")
    assert r.status_code == 200
    body = r.json()
    assert body["reachable"] is False
    assert body["connectors"] == []
    assert "error" in body


def test_status_requires_read_role():
    # an empty role set grants nothing, including READ.
    client = _client_as(set())
    r = client.get("/api/status")
    assert r.status_code == 403


def test_unauthorized_status_does_not_build_provider():
    app.dependency_overrides[get_roles] = lambda: set()
    app.dependency_overrides[get_connect] = ExplodingProvider()
    client = TestClient(app, raise_server_exceptions=False)
    r = client.get("/api/status")
    assert r.status_code == 403
