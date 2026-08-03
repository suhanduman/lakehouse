"""Spark-free tests for merge_cdc.run()'s per-table loop: isolation, aggregate
exit code, SkipTable semantics, and (this task) the parallel ThreadPoolExecutor
rewrite. All Spark-calling functions (_discover_bronze, _silver_exists,
_merge_one, _audit, _ensure_audit) are monkeypatched, so a FakeSpark only needs
to satisfy `spark.sparkContext.setLocalProperty(...)` (the FAIR-pool call each
worker thread makes) — `sql`/`table` are never invoked in these tests.
"""
import threading

from tools.jobs import merge_cdc as mc


class FakeSparkContext:
    def setLocalProperty(self, key, value):
        pass


class FakeSpark:
    def __init__(self):
        self.sparkContext = FakeSparkContext()

    def sql(self, *args, **kwargs):
        raise NotImplementedError("sql() unused: Spark calls are monkeypatched in this test")

    def table(self, *args, **kwargs):
        raise NotImplementedError("table() unused: Spark calls are monkeypatched in this test")


def test_run_processes_all_tables_and_aggregates_failures(monkeypatch):
    processed = []

    monkeypatch.setattr(mc, "_discover_bronze", lambda spark, args: [
        ("rawlake.a_raw.t", "lakehouse.a.t"),
        ("rawlake.b_raw.t", "lakehouse.b.t"),
        ("rawlake.c_raw.t", "lakehouse.c.t")])
    monkeypatch.setattr(mc, "_silver_exists", lambda spark, fqn: True)
    monkeypatch.setattr(mc, "_audit", lambda spark, **row: None)
    monkeypatch.setattr(mc, "_ensure_audit", lambda spark: None)

    def fake_merge_one(spark, bronze, silver):
        processed.append(silver)
        if silver == "lakehouse.b.t":
            raise RuntimeError("boom")
        return {"table_name": silver, "start_snapshot": None, "end_snapshot": 1,
                "increment_rows": 0, "deleted_rows": 0, "status": "ok",
                "error": None, "duration_s": 0.0}
    monkeypatch.setattr(mc, "_merge_one", fake_merge_one)

    class Args:
        reset_watermark = None
        restate = None
        max_parallel = 3
        silver_catalog = "lakehouse"
        bronze_catalog = "rawlake"
        bronze_suffix = "_raw"

    rc = mc.run(FakeSpark(), Args())
    assert rc == 1
    assert set(processed) == {"lakehouse.a.t", "lakehouse.b.t", "lakehouse.c.t"}


def test_run_all_succeed_returns_zero(monkeypatch):
    monkeypatch.setattr(mc, "_discover_bronze", lambda spark, args: [
        ("rawlake.a_raw.t", "lakehouse.a.t"),
        ("rawlake.b_raw.t", "lakehouse.b.t")])
    monkeypatch.setattr(mc, "_silver_exists", lambda spark, fqn: True)
    monkeypatch.setattr(mc, "_audit", lambda spark, **row: None)
    monkeypatch.setattr(mc, "_ensure_audit", lambda spark: None)

    def fake_merge_one(spark, bronze, silver):
        return {"table_name": silver, "start_snapshot": None, "end_snapshot": 1,
                "increment_rows": 0, "deleted_rows": 0, "status": "ok",
                "error": None, "duration_s": 0.0}
    monkeypatch.setattr(mc, "_merge_one", fake_merge_one)

    class Args:
        reset_watermark = None
        restate = None
        max_parallel = 4
        silver_catalog = "lakehouse"
        bronze_catalog = "rawlake"
        bronze_suffix = "_raw"

    assert mc.run(FakeSpark(), Args()) == 0


def test_run_skip_table_is_not_a_failure(monkeypatch):
    audited = []

    monkeypatch.setattr(mc, "_discover_bronze", lambda spark, args: [
        ("rawlake.a_raw.t", "lakehouse.a.t")])
    monkeypatch.setattr(mc, "_silver_exists", lambda spark, fqn: True)
    monkeypatch.setattr(mc, "_audit", lambda spark, **row: audited.append(row))
    monkeypatch.setattr(mc, "_ensure_audit", lambda spark: None)

    def fake_merge_one(spark, bronze, silver):
        raise mc.SkipTable("no CDC metadata")
    monkeypatch.setattr(mc, "_merge_one", fake_merge_one)

    class Args:
        reset_watermark = None
        restate = None
        max_parallel = 2
        silver_catalog = "lakehouse"
        bronze_catalog = "rawlake"
        bronze_suffix = "_raw"

    rc = mc.run(FakeSpark(), Args())
    assert rc == 0  # SkipTable must NOT count as a failure
    assert audited[0]["status"] == "skipped"


def test_run_silver_not_pre_created_is_skipped_not_processed(monkeypatch):
    processed = []

    monkeypatch.setattr(mc, "_discover_bronze", lambda spark, args: [
        ("rawlake.a_raw.t", "lakehouse.a.t")])
    monkeypatch.setattr(mc, "_silver_exists", lambda spark, fqn: False)
    monkeypatch.setattr(mc, "_ensure_audit", lambda spark: None)

    def fake_merge_one(spark, bronze, silver):
        processed.append(silver)
        return {}
    monkeypatch.setattr(mc, "_merge_one", fake_merge_one)

    class Args:
        reset_watermark = None
        restate = None
        max_parallel = 2
        silver_catalog = "lakehouse"
        bronze_catalog = "rawlake"
        bronze_suffix = "_raw"

    rc = mc.run(FakeSpark(), Args())
    assert rc == 0
    assert processed == []  # never MERGEd — Silver doesn't exist yet


def test_run_sets_fair_scheduler_pool_per_table(monkeypatch):
    pools_set = []

    class RecordingSparkContext(FakeSparkContext):
        def setLocalProperty(self, key, value):
            pools_set.append((key, value))

    class RecordingSpark(FakeSpark):
        def __init__(self):
            self.sparkContext = RecordingSparkContext()

    monkeypatch.setattr(mc, "_discover_bronze", lambda spark, args: [
        ("rawlake.a_raw.t", "lakehouse.a.t")])
    monkeypatch.setattr(mc, "_silver_exists", lambda spark, fqn: True)
    monkeypatch.setattr(mc, "_audit", lambda spark, **row: None)
    monkeypatch.setattr(mc, "_ensure_audit", lambda spark: None)

    def fake_merge_one(spark, bronze, silver):
        return {"table_name": silver, "start_snapshot": None, "end_snapshot": 1,
                "increment_rows": 0, "deleted_rows": 0, "status": "ok",
                "error": None, "duration_s": 0.0}
    monkeypatch.setattr(mc, "_merge_one", fake_merge_one)

    class Args:
        reset_watermark = None
        restate = None
        max_parallel = 2
        silver_catalog = "lakehouse"
        bronze_catalog = "rawlake"
        bronze_suffix = "_raw"

    mc.run(RecordingSpark(), Args())
    assert pools_set == [("spark.scheduler.pool", "merge-lakehouse.a.t")]


def test_merge_one_uses_per_thread_unique_view_names(monkeypatch):
    """Finding 1: bronze_inc/src_latest must not be fixed session-global names
    -- two concurrent _merge_one calls on the SAME SparkSession must register
    DISTINCT temp view names (suffixed by thread identity), never the bare
    literals "bronze_inc"/"src_latest"."""
    recorded = []
    lock = threading.Lock()

    class FakeResult:
        def createOrReplaceTempView(self, name):
            with lock:
                recorded.append((threading.get_ident(), name))

        def count(self):
            return 0

        def collect(self):
            return [{"c": 0}]

    class FakeSparkForMergeOne:
        def __init__(self):
            self.sparkContext = FakeSparkContext()

        def sql(self, *args, **kwargs):
            return FakeResult()

        def table(self, *args, **kwargs):
            return FakeResult()

    def fake_describe(spark, fqn):
        if fqn.startswith("rawlake"):
            return {"id": "bigint", "name": "string", "__op": "string",
                    "__ts_ms": "bigint", "__deleted": "boolean", "__lsn": "bigint"}
        return {"id": "bigint", "name": "string"}

    monkeypatch.setattr(mc, "_describe", fake_describe)
    monkeypatch.setattr(mc, "_identifier_fields", lambda spark, fqn: ["id"])
    monkeypatch.setattr(mc, "_get_prop", lambda spark, fqn, key: None)
    monkeypatch.setattr(mc, "_current_snapshot", lambda spark, fqn: 1)
    monkeypatch.setattr(mc, "_read_increment", lambda spark, fqn, start: FakeResult())
    monkeypatch.setattr(mc, "_set_prop", lambda spark, fqn, key, value: None)

    spark = FakeSparkForMergeOne()
    threads = [
        threading.Thread(target=mc._merge_one, args=(spark, "rawlake.a_raw.t", "lakehouse.a.t")),
        threading.Thread(target=mc._merge_one, args=(spark, "rawlake.b_raw.t", "lakehouse.b.t")),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    names = {name for _, name in recorded}
    assert "bronze_inc" not in names          # never the bare session-global literal
    assert "src_latest" not in names          # never the bare session-global literal

    by_thread = {}
    for tid, name in recorded:
        by_thread.setdefault(tid, set()).add(name)
    assert len(by_thread) == 2                # both threads actually registered views
    thread_view_sets = list(by_thread.values())
    assert thread_view_sets[0].isdisjoint(thread_view_sets[1])  # no cross-thread collision
    for tid, view_names in by_thread.items():
        assert all(str(tid) in n for n in view_names)  # suffixed by that thread's identity


def test_run_survives_audit_failure_on_failure_path(monkeypatch):
    """Finding 2: an audit write must never abort the batch. Here _merge_one
    raises for one table AND the failure-branch _audit call ALSO raises a
    conflict-shaped exception; run() must still process every table and return
    the aggregate failure code without the audit exception propagating out."""
    monkeypatch.setattr(mc.time, "sleep", lambda s: None)  # keep retries instant
    processed = []

    monkeypatch.setattr(mc, "_discover_bronze", lambda spark, args: [
        ("rawlake.a_raw.t", "lakehouse.a.t"),
        ("rawlake.b_raw.t", "lakehouse.b.t"),
        ("rawlake.c_raw.t", "lakehouse.c.t")])
    monkeypatch.setattr(mc, "_silver_exists", lambda spark, fqn: True)
    monkeypatch.setattr(mc, "_ensure_audit", lambda spark: None)

    def flaky_audit(spark, **row):
        if row.get("status") == "failed":
            raise RuntimeError("commit conflicts with existing state")
    monkeypatch.setattr(mc, "_audit", flaky_audit)

    def fake_merge_one(spark, bronze, silver):
        processed.append(silver)
        if silver == "lakehouse.b.t":
            raise RuntimeError("boom")
        return {"table_name": silver, "start_snapshot": None, "end_snapshot": 1,
                "increment_rows": 0, "deleted_rows": 0, "status": "ok",
                "error": None, "duration_s": 0.0}
    monkeypatch.setattr(mc, "_merge_one", fake_merge_one)

    class Args:
        reset_watermark = None
        restate = None
        max_parallel = 3
        silver_catalog = "lakehouse"
        bronze_catalog = "rawlake"
        bronze_suffix = "_raw"

    rc = mc.run(FakeSpark(), Args())  # must NOT raise -- audit failure is swallowed
    assert rc == 1                    # aggregate failure still reported
    assert set(processed) == {"lakehouse.a.t", "lakehouse.b.t", "lakehouse.c.t"}  # no cancellation


def test_spj_conf_enables_bucketed_merge_target_pruning():
    """Test that SPJ_CONF contains the two flags required for bucketed-MERGE
    target pruning (storage-partitioned join)."""
    assert hasattr(mc, "SPJ_CONF"), "SPJ_CONF must be defined as a module-level constant"
    assert isinstance(mc.SPJ_CONF, dict), "SPJ_CONF must be a dictionary"
    assert mc.SPJ_CONF.get("spark.sql.sources.v2.bucketing.enabled") == "true"
    assert mc.SPJ_CONF.get("spark.sql.iceberg.planning.preserve-data-grouping") == "true"
