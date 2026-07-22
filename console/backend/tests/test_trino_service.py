"""TrinoService: thin wrapper over a `trino.dbapi`-shaped connection. No
live Trino coordinator — every test injects a fake `conn_factory` returning
a hand-rolled fake connection/cursor that records the SQL it was asked to
execute, so the wrapper's statement-shaping is exercised without a live
cluster.
"""

from __future__ import annotations

from app.services.trino_service import TrinoService


# --------------------------------------------------------------------------
# Fake trino.dbapi connection/cursor
# --------------------------------------------------------------------------

class FakeCursor:
    def __init__(self, rows=None):
        self.executed = []
        self._rows = rows or []
        self.closed = False

    def execute(self, sql):
        self.executed.append(sql)

    def fetchall(self):
        return self._rows

    def close(self):
        self.closed = True


class FakeConnection:
    def __init__(self, rows=None):
        self.cursors = []
        self._rows = rows or []
        self.closed = False

    def cursor(self):
        cur = FakeCursor(self._rows)
        self.cursors.append(cur)
        return cur

    def close(self):
        self.closed = True


def make_factory(rows=None):
    conn = FakeConnection(rows)

    def factory():
        return conn

    return factory, conn


# --------------------------------------------------------------------------
# run_ddl
# --------------------------------------------------------------------------

def test_run_ddl_executes_sql():
    factory, conn = make_factory()
    svc = TrinoService(factory)
    svc.run_ddl("CREATE NAMESPACE IF NOT EXISTS lakehouse.mssql1 WITH (location='s3://src-mssql1/warehouse')")

    assert len(conn.cursors) == 1
    assert conn.cursors[0].executed == [
        "CREATE NAMESPACE IF NOT EXISTS lakehouse.mssql1 WITH (location='s3://src-mssql1/warehouse')"
    ]


def test_run_ddl_closes_cursor():
    factory, conn = make_factory()
    svc = TrinoService(factory)
    svc.run_ddl("DROP NAMESPACE lakehouse.mssql1")
    assert conn.cursors[0].closed is True


# --------------------------------------------------------------------------
# list_namespaces
# --------------------------------------------------------------------------

def test_list_namespaces_returns_names():
    factory, conn = make_factory(rows=[("mssql1",), ("mongo_lms",)])
    svc = TrinoService(factory)
    result = svc.list_namespaces("lakehouse")

    assert result == ["mssql1", "mongo_lms"]
    assert conn.cursors[0].executed == ["SHOW SCHEMAS FROM lakehouse"]


def test_list_namespaces_empty():
    factory, conn = make_factory(rows=[])
    svc = TrinoService(factory)
    assert svc.list_namespaces("lakehouse") == []


# --------------------------------------------------------------------------
# list_tables
# --------------------------------------------------------------------------

def test_list_tables_returns_names():
    factory, conn = make_factory(rows=[("students",), ("courses",)])
    svc = TrinoService(factory)
    result = svc.list_tables("lakehouse", "mssql1")

    assert result == ["students", "courses"]
    assert conn.cursors[0].executed == ["SHOW TABLES FROM lakehouse.mssql1"]


# --------------------------------------------------------------------------
# drop_table
# --------------------------------------------------------------------------

def test_drop_table_executes_drop_statement():
    factory, conn = make_factory()
    svc = TrinoService(factory)
    svc.drop_table("lakehouse.mssql1.students")

    assert conn.cursors[0].executed == [
        "DROP TABLE IF EXISTS lakehouse.mssql1.students"
    ]
