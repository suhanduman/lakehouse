"""TrinoService: thin wrapper over a `trino.dbapi`-shaped connection.

No coordinator/network calls of its own — the constructor takes a
`conn_factory` callable (e.g. `lambda: trino.dbapi.connect(host=..., ...)`,
or a hand-rolled fake in tests) so the whole thing is unit-testable without
a live Trino cluster (see tests/test_trino_service.py). A fresh
connection+cursor is opened per call and closed afterwards, mirroring the
trino-python-client's recommended usage.
"""

from __future__ import annotations

from typing import Any, Callable, List


class TrinoService:
    """DDL/DQL wrapper. `conn_factory` is an injected zero-arg callable
    returning a `trino.dbapi.Connection` (or hand-rolled fake in tests) —
    see class docstring."""

    def __init__(self, conn_factory: Callable[[], Any]) -> None:
        self.conn_factory = conn_factory

    def _execute(self, sql: str) -> List[Any]:
        conn = self.conn_factory()
        cur = conn.cursor()
        try:
            cur.execute(sql)
            try:
                return cur.fetchall()
            except Exception:
                return []
        finally:
            cur.close()

    # ----------------------------------------------------------------
    # run_ddl — CREATE/DROP NAMESPACE etc., no rows expected back
    # ----------------------------------------------------------------

    def run_ddl(self, sql: str) -> None:
        self._execute(sql)

    # ----------------------------------------------------------------
    # list_namespaces / list_tables — SHOW SCHEMAS/TABLES, first column
    # ----------------------------------------------------------------

    def list_namespaces(self, catalog: str) -> List[str]:
        rows = self._execute(f"SHOW SCHEMAS FROM {catalog}")
        return [row[0] for row in rows]

    def list_tables(self, catalog: str, ns: str) -> List[str]:
        rows = self._execute(f"SHOW TABLES FROM {catalog}.{ns}")
        return [row[0] for row in rows]

    # ----------------------------------------------------------------
    # drop_table — fqn is already fully-qualified (catalog.ns.table)
    # ----------------------------------------------------------------

    def drop_table(self, fqn: str) -> None:
        self._execute(f"DROP TABLE IF EXISTS {fqn}")
