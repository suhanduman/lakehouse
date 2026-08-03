"""Spark-free tests for iceberg_maintenance.py: the per-table maintain_table()
chain, its per-table error isolation, and main()'s ThreadPoolExecutor dispatch
(MAINTENANCE_MAX_PARALLEL). Importing the module must not start Spark --
pyspark is imported lazily only under the `if __name__ == "__main__":` guard.
"""
from tools.jobs import iceberg_maintenance as im


class FakeSpark:
    """Records every SQL string passed to .sql(); optionally raises when a
    given fully-qualified table name appears together with a marker string
    (used to fail one specific CALL for one specific table)."""

    def __init__(self, fail_on=None):
        self.calls = []
        self.fail_on = fail_on or {}  # {fq: marker_substring}
        self.stopped = False

    def sql(self, s):
        self.calls.append(s)
        for fq, marker in self.fail_on.items():
            if f"'{fq}'" in s and marker in s:
                raise RuntimeError(f"boom: {fq} {marker}")
        return _EmptyResult()

    def stop(self):
        self.stopped = True


class _EmptyResult:
    def collect(self):
        return []


def test_maintain_table_runs_full_chain_for_normal_table():
    spark = FakeSpark()
    im.maintain_table(spark, "lakehouse", "depo", "customers", 30)

    joined = "\n".join(spark.calls)
    assert "ALTER TABLE lakehouse.depo.customers SET TBLPROPERTIES ('gc.enabled'='true')" in joined
    assert "CALL lakehouse.system.rewrite_data_files(table => 'depo.customers'" in joined
    assert "CALL lakehouse.system.rewrite_position_delete_files(table => 'depo.customers')" in joined
    assert "CALL lakehouse.system.expire_snapshots(table => 'depo.customers')" in joined
    assert "CALL lakehouse.system.remove_orphan_files(table => 'depo.customers')" in joined


def test_maintain_table_bronze_ttl_branch_for_rawlake_bronze_namespace():
    spark = FakeSpark()
    im.maintain_table(spark, "rawlake", "depo_raw", "customers", 30)

    joined = "\n".join(spark.calls)
    assert "DELETE FROM rawlake.depo_raw.customers WHERE __ts_ms < date_sub(current_date(), 30)" in joined


def test_maintain_table_error_is_caught_and_logged_not_raised(capsys):
    spark = FakeSpark(fail_on={"depo.customers": "rewrite_data_files"})

    im.maintain_table(spark, "lakehouse", "depo", "customers", 30)  # must not raise

    out = capsys.readouterr().out
    assert "[error] lakehouse.depo.customers:" in out
    # the CALLs after the raising one never ran for this table
    joined = "\n".join(spark.calls)
    assert "expire_snapshots(table => 'depo.customers')" not in joined


def test_main_dispatches_all_tables_one_raising_does_not_stop_others(monkeypatch):
    monkeypatch.setattr(im, "discover_tables", lambda spark: [
        ("lakehouse", "a", "t"),
        ("lakehouse", "b", "t"),
        ("lakehouse", "c", "t"),
    ])
    spark = FakeSpark(fail_on={"b.t": "rewrite_data_files"})

    im.main(spark)  # must not raise

    joined = "\n".join(spark.calls)
    # the failing table logged [error] and stopped mid-chain...
    assert "expire_snapshots(table => 'b.t')" not in joined
    # ...but the other two tables ran their FULL chain regardless
    for fq in ("a.t", "c.t"):
        assert f"rewrite_data_files(table => '{fq}'" in joined
        assert f"expire_snapshots(table => '{fq}')" in joined
        assert f"remove_orphan_files(table => '{fq}')" in joined
    assert spark.stopped is True


def test_main_reads_max_parallel_env_var(monkeypatch):
    captured = {}

    class FakeFuture:
        def result(self):
            return None

    class FakeExecutor:
        def __init__(self, max_workers=None):
            captured["max_workers"] = max_workers

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def submit(self, fn, *args, **kwargs):
            fn(*args, **kwargs)
            return FakeFuture()

    monkeypatch.setattr(im, "ThreadPoolExecutor", FakeExecutor)
    monkeypatch.setattr(im, "discover_tables", lambda spark: [])
    monkeypatch.setenv("MAINTENANCE_MAX_PARALLEL", "7")

    im.main(FakeSpark())

    assert captured["max_workers"] == 7


def test_main_defaults_max_parallel_to_four(monkeypatch):
    captured = {}

    class FakeFuture:
        def result(self):
            return None

    class FakeExecutor:
        def __init__(self, max_workers=None):
            captured["max_workers"] = max_workers

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def submit(self, fn, *args, **kwargs):
            fn(*args, **kwargs)
            return FakeFuture()

    monkeypatch.setattr(im, "ThreadPoolExecutor", FakeExecutor)
    monkeypatch.setattr(im, "discover_tables", lambda spark: [])
    monkeypatch.delenv("MAINTENANCE_MAX_PARALLEL", raising=False)

    im.main(FakeSpark())

    assert captured["max_workers"] == 4


def test_discover_tables_skips_information_schema_and_degrades_on_error():
    class Row:
        def __init__(self, **kw):
            self.__dict__.update(kw)

    class _Rows:
        def __init__(self, rows):
            self._rows = rows

        def collect(self):
            return self._rows

    class DiscoverySpark:
        def sql(self, s):
            if s == "SHOW NAMESPACES IN lakehouse":
                return _Rows([Row(namespace="depo"), Row(namespace="information_schema")])
            if s == "SHOW TABLES IN lakehouse.depo":
                return _Rows([Row(tableName="customers")])
            if s == "SHOW NAMESPACES IN rawlake":
                raise RuntimeError("catalog unreachable")
            raise AssertionError(f"unexpected sql: {s}")

    triples = im.discover_tables(DiscoverySpark(), catalogs=["lakehouse", "rawlake"])
    assert triples == [("lakehouse", "depo", "customers")]
