"""IcebergService unit testleri — GERÇEK pyiceberg şema kurulumunu (tip eşleme +
identifier field) ve idempotent/fail-loud create mantığını, sahte (fake) bir
pyiceberg catalog ile test eder. Nessie/S3'e ihtiyaç yok; sadece pyiceberg
kütüphanesi gerekir (requirements.txt).

Kapsanan: tablonun identifier-field-ids ile yaratıldığını doğrular — bu
olmadan Iceberg sink upsert'ü append-only'e düşer."""
from __future__ import annotations

import pytest

from app.services.iceberg_service import IcebergService, IdentifierMismatch


class FakeSnapshot:
    """Mirrors pyiceberg's Snapshot enough for table_stats: a `.summary` dict
    (string-valued, as the real Iceberg snapshot summary is)."""

    def __init__(self, summary):
        self.summary = summary


class FakeTable:
    def __init__(self, identifier_names, snapshot_summary=None):
        self._ids = list(identifier_names)
        self._snapshot_summary = snapshot_summary

    def schema(self):
        table = self

        class _S:
            def identifier_field_names(_self):
                return table._ids

        return _S()

    def current_snapshot(self):
        """Mirrors pyiceberg's Table.current_snapshot() -> Optional[Snapshot]:
        None when no snapshot has been seeded (fresh/empty table)."""
        if self._snapshot_summary is None:
            return None
        return FakeSnapshot(self._snapshot_summary)


class FakeCatalog:
    """existing: {table_name: [identifier_col, ...]} — önceden var olan tablolar."""

    def __init__(self, existing=None):
        self.existing = existing or {}
        self.created = []          # [(fq, schema)]
        self.partition_specs = []  # [(fq, partition_spec)] — bronze day(__ts_ms)
        self.namespaces = []       # [(ns, properties)]
        self.table_properties = {}  # {fq: properties} — Iceberg CREATE-time table properties
        self.tables_by_ns = {}     # {namespace: {table, ...}} — populated by create_table
        self.snapshots = {}       # {fq: summary_dict} — populated by seed_snapshot

    def create_namespace(self, namespace, properties=None):
        self.namespaces.append((namespace, properties))

    def list_tables(self, namespace):
        # Backward-compatible global `existing` (ignores namespace, as before
        # -- tests that pre-seed `existing=` always query a single namespace)
        # UNION namespace-scoped tables actually created via create_table
        # (tracked below) -- this is what lets namespace_tables() tell two
        # DIFFERENT namespaces apart (e.g. "ns1" vs "absent").
        names = set(self.existing) | self.tables_by_ns.get(namespace, set())
        return [(namespace, t) for t in names]

    def load_table(self, fq):
        # `.get(..., [])` (not a bare index) so table_stats can load_table()
        # for a table that was only seeded via seed_snapshot (never listed in
        # `existing`) -- identifier-mismatch call sites only reach load_table
        # after confirming `table in existing`, so this is additive.
        ids = self.existing.get(fq.split(".")[-1], [])
        return FakeTable(ids, snapshot_summary=self.snapshots.get(fq))

    def seed_snapshot(self, fq, summary_dict):
        """Test helper: makes a subsequent `load_table(fq).current_snapshot()`
        return a FakeSnapshot wrapping `summary_dict` — used by table_stats
        tests."""
        self.snapshots[fq] = summary_dict

    def create_table(self, fq, schema=None, partition_spec=None, properties=None):
        self.created.append((fq, schema))
        self.partition_specs.append((fq, partition_spec))
        self.table_properties[fq] = properties or {}
        namespace, table = fq.rsplit(".", 1)
        # NOTE: intentionally does NOT also write into `self.existing` --
        # `existing` is the backward-compatible "pre-seeded at construction"
        # dict (global, ignores namespace); `tables_by_ns` is the accurate,
        # namespace-scoped record of what THIS create_table call actually
        # created, which is what makes namespace_tables() correctly return
        # set() for a namespace nothing was ever created in.
        self.tables_by_ns.setdefault(namespace, set()).add(table)
        return FakeTable(schema.identifier_field_names())


def _svc(catalog):
    """Fixed-catalog double: factory tolerates the `warehouse=` kwarg
    (IcebergService._catalog_ always passes it, positionally None for
    silver / "rawdata" for bronze) but always returns the same `catalog`."""
    return IcebergService(catalog_factory=lambda warehouse=None: catalog)


class RecordingCatalogFactory:
    """Records every `warehouse` the factory was called with (per-warehouse
    caching means each distinct warehouse value is recorded once) and hands
    back a fresh `FakeCatalog` per warehouse -- lets tests assert Bronze was
    routed to the `rawdata`-warehouse catalog and Silver to the default
    (warehouse=None) one."""

    def __init__(self):
        self.calls = []
        self.catalogs_by_warehouse = {}

    def __call__(self, warehouse=None):
        self.calls.append(warehouse)
        if warehouse not in self.catalogs_by_warehouse:
            self.catalogs_by_warehouse[warehouse] = FakeCatalog()
        return self.catalogs_by_warehouse[warehouse]


def test_create_table_sets_identifier_and_maps_types():
    cat = FakeCatalog()
    fq = _svc(cat).create_table(
        "depo",
        "customers",
        [
            {"name": "id", "type": "bigint"},
            {"name": "name", "type": "varchar"},
            {"name": "created", "type": "timestamp"},
        ],
        identifier=["id"],
        location="s3://src-depo/warehouse",
    )
    assert fq == "depo.customers"
    assert cat.namespaces == [("depo", {"location": "s3://src-depo/warehouse"})]
    assert len(cat.created) == 1
    _, schema = cat.created[0]
    # identifier field set edildi
    assert list(schema.identifier_field_names()) == ["id"]
    # tip eşleme: bigint->long, varchar->string, timestamp->timestamp
    by_name = {f.name: f for f in schema.fields}
    assert str(by_name["id"].field_type) == "long"
    assert str(by_name["name"].field_type) == "string"
    assert str(by_name["created"].field_type) == "timestamp"
    # identifier kolonu REQUIRED'e zorlandı
    assert by_name["id"].required is True


def test_mongo_id_is_string_identifier():
    cat = FakeCatalog()
    _svc(cat).create_table(
        "mongo_lms", "events",
        [{"name": "_id", "type": "string"}, {"name": "payload", "type": "varchar"}],
        identifier=["_id"],
    )
    _, schema = cat.created[0]
    by_name = {f.name: f for f in schema.fields}
    assert str(by_name["_id"].field_type) == "string"
    assert by_name["_id"].required is True
    assert list(schema.identifier_field_names()) == ["_id"]


def test_empty_identifier_raises_valueerror():
    with pytest.raises(ValueError):
        _svc(FakeCatalog()).create_table("depo", "t", [{"name": "id", "type": "int"}], identifier=[])


def test_existing_table_matching_identifier_is_noop():
    cat = FakeCatalog(existing={"customers": ["id"]})
    fq = _svc(cat).create_table(
        "depo", "customers", [{"name": "id", "type": "int"}], identifier=["id"]
    )
    assert fq == "depo.customers"
    assert cat.created == []  # zaten var, yeniden yaratılmadı


def test_existing_table_mismatched_identifier_fails_loud():
    cat = FakeCatalog(existing={"customers": ["wrongcol"]})
    with pytest.raises(IdentifierMismatch):
        _svc(cat).create_table(
            "depo", "customers", [{"name": "id", "type": "int"}], identifier=["id"]
        )


# --------------------------------------------------------------------------
# layer="bronze" — changelog table, no identifier, partitioned, rawdata
# warehouse. Mirrors tools/create_iceberg_table.py's Bronze path.
# --------------------------------------------------------------------------

def test_create_table_bronze_layer_partitioned_no_identifier():
    # layer="bronze" -> underlying catalog.create_table called with a
    # day(__ts_ms) partition spec, empty identifier, and routed to a catalog
    # bound to the "rawdata" Nessie warehouse (a
    # construction-time catalog_factory(warehouse=...) kwarg, NOT an inert
    # namespace property, which Nessie ignores for storage routing).
    factory = RecordingCatalogFactory()
    fq = IcebergService(catalog_factory=factory).create_table(
        "depo_raw",
        "customers",
        [{"name": "id", "type": "bigint"}, {"name": "name", "type": "varchar"}],
        identifier=[],
        layer="bronze",
    )
    assert fq == "depo_raw.customers"
    # factory was asked for the "rawdata" warehouse's catalog
    assert factory.calls == ["rawdata"]
    cat = factory.catalogs_by_warehouse["rawdata"]
    assert len(cat.created) == 1
    _, schema = cat.created[0]
    # no identifier field at all
    assert list(schema.identifier_field_ids) == []
    by_name = {f.name for f in schema.fields}
    # fixed CDC metadata columns present alongside the business columns
    assert {"__op", "__ts_ms", "__deleted", "__lsn"} <= by_name
    assert {"id", "name"} <= by_name
    # captured partition spec: single day(__ts_ms) field
    assert len(cat.partition_specs) == 1
    _, pspec = cat.partition_specs[0]
    assert [f.name for f in pspec.fields] == ["__ts_ms_day"]
    # no more inert "warehouse" namespace property (Nessie ignores it for
    # storage routing -- the catalog_factory(warehouse="rawdata") kwarg
    # above is the real fix) -- namespace create still runs, sans location.
    assert cat.namespaces[-1] == ("depo_raw", {})


def test_create_table_silver_layer_uses_default_warehouse_catalog():
    # layer="silver" (default) -> factory called with warehouse=None, i.e.
    # the Nessie *default* warehouse's catalog -- never "rawdata".
    factory = RecordingCatalogFactory()
    fq = IcebergService(catalog_factory=factory).create_table(
        "depo", "customers", [{"name": "id", "type": "int"}], identifier=["id"]
    )
    assert fq == "depo.customers"
    assert factory.calls == [None]
    cat = factory.catalogs_by_warehouse[None]
    assert len(cat.created) == 1


def test_create_table_bronze_layer_empty_identifier_does_not_raise():
    # The silver-only fail-loud rule must NOT fire for bronze — bronze
    # legitimately passes/forces identifier=[] (append-only changelog).
    cat = FakeCatalog()
    _svc(cat).create_table(
        "depo_raw", "t", [{"name": "id", "type": "int"}], identifier=[], layer="bronze"
    )  # must not raise


# --------------------------------------------------------------------------
# Storage defaults: 128MiB target-file-size (all layers) + Silver
# copy-on-write trio (entity/upsert, read-heavy + MERGE-written). Mirrors
# tools/create_iceberg_table.py's properties dict in lockstep.
# --------------------------------------------------------------------------

def test_silver_table_created_with_cow_and_target_file_size():
    cat = FakeCatalog()
    fq = _svc(cat).create_table(
        "depo", "orders",
        [{"name": "id", "type": "int"}, {"name": "n", "type": "string"}],
        identifier=["id"],
    )
    props = cat.table_properties[fq]
    assert props["write.merge.mode"] == "copy-on-write"
    assert props["write.update.mode"] == "copy-on-write"
    assert props["write.delete.mode"] == "copy-on-write"
    assert props["write.target-file-size-bytes"] == "134217728"


def test_bronze_table_created_with_target_file_size():
    cat = FakeCatalog()
    fq = _svc(cat).create_table(
        "depo_raw", "orders", [{"name": "id", "type": "int"}], identifier=[], layer="bronze"
    )
    props = cat.table_properties[fq]
    assert props["write.target-file-size-bytes"] == "134217728"
    assert "write.merge.mode" not in props  # Bronze is append-only, no merge mode


# --------------------------------------------------------------------------
# namespace_tables — read-only helper, orchestrator uniqueness check (F-spike
# B-v2 Task 4). Does NOT touch create_table's location handling.
# --------------------------------------------------------------------------

COLS = [{"name": "id", "type": "int"}]


@pytest.fixture
def fake_catalog_iceberg():
    cat = FakeCatalog()
    return _svc(cat), cat


def test_namespace_tables_returns_names(fake_catalog_iceberg):
    svc, cat = fake_catalog_iceberg
    svc.create_table("ns1", "t1", COLS, ["id"], location="s3://x/warehouse")
    assert svc.namespace_tables("ns1") == {"t1"}
    assert svc.namespace_tables("absent") == set()


# --------------------------------------------------------------------------
# table_stats — current-snapshot summary read (metadata only, no scan). Pipeline
# Map + catalog enrichment (2026-08-02 spike), Task 1.
# --------------------------------------------------------------------------

def test_table_stats_reads_snapshot_summary(fake_catalog_iceberg):
    svc, cat = fake_catalog_iceberg
    cat.seed_snapshot("depo.customers", {"total-records": "42", "total-data-files": "7"})
    assert svc.table_stats("depo", "customers", layer="silver") == {"records": 42, "data_files": 7}


def test_table_stats_none_when_absent(fake_catalog_iceberg):
    svc, _ = fake_catalog_iceberg
    assert svc.table_stats("nope", "missing") is None
