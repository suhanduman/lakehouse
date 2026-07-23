import pytest
from tools.jobs import merge_lib as m


def test_has_cdc_metadata_true_when_all_present():
    schema = {"id": "bigint", "name": "string",
              "__op": "string", "__ts_ms": "bigint", "__deleted": "boolean", "__lsn": "bigint"}
    assert m.has_cdc_metadata(schema) is True


def test_has_cdc_metadata_false_when_missing_one():
    schema = {"id": "bigint", "name": "string", "__op": "string", "__ts_ms": "bigint"}
    assert m.has_cdc_metadata(schema) is False  # __deleted missing


def test_silver_ns_strips_suffix():
    assert m.silver_ns_from_bronze("depo_raw") == "depo"
    with pytest.raises(ValueError):
        m.silver_ns_from_bronze("depo")  # no suffix -> loud


def test_safe_promotion():
    assert m.is_safe_promotion("int", "long")
    assert m.is_safe_promotion("int", "bigint")
    assert m.is_safe_promotion("float", "double")
    assert m.is_safe_promotion("decimal(10,2)", "decimal(20,2)")
    assert m.is_safe_promotion("string", "string")
    assert not m.is_safe_promotion("string", "int")
    assert not m.is_safe_promotion("long", "int")            # narrowing
    assert not m.is_safe_promotion("decimal(10,2)", "decimal(10,4)")  # scale change


def test_reconcile_excludes_target_table_route_field():
    # The sink writes the _target_table route field as a data column; it must be
    # treated as metadata (excluded from Silver), not a business ADD.
    bronze = {"id": "bigint", "name": "string", "_target_table": "string",
              "__op": "string", "__ts_ms": "timestamp", "__deleted": "boolean", "__lsn": "bigint"}
    silver = {"id": "bigint", "name": "string"}
    plan = m.reconcile_plan(bronze, silver)
    assert plan.adds == []                       # _target_table NOT added to Silver
    assert "_target_table" in m.METADATA_COLS


def test_reconcile_add_drop_promote_conflict():
    bronze = {"id": "bigint", "name": "string", "age": "bigint",
              "__op": "string", "__ts_ms": "bigint", "__deleted": "boolean", "__lsn": "bigint"}
    silver = {"id": "int", "name": "string", "city": "string"}
    plan = m.reconcile_plan(bronze, silver)
    assert plan.adds == [("age", "bigint")]                  # in bronze, not silver
    assert plan.promotions == [("id", "int", "bigint")]      # safe widen
    assert plan.ignored_drops == ["city"]                    # in silver, not bronze
    assert plan.conflicts == []
    assert plan.ok is True
    # metadata cols never appear anywhere
    flat = {c for c, _ in plan.adds} | {c for c, _, _ in plan.promotions}
    assert not (set(m.METADATA_COLS) & flat)


def test_reconcile_conflict_is_not_ok():
    bronze = {"id": "bigint", "age": "string"}
    silver = {"id": "bigint", "age": "int"}
    plan = m.reconcile_plan(bronze, silver)
    assert plan.conflicts == [("age", "int", "string")]
    assert plan.ok is False


def test_latest_per_key_sql():
    sql = m.latest_per_key_sql("rawlake.depo_raw.customers", ["id"], ["id", "name"])
    assert "PARTITION BY id" in sql
    assert "ORDER BY __ts_ms DESC, __lsn DESC" in sql
    assert "__rn = 1" in sql
    assert "__deleted" in sql
    assert "rawlake.depo_raw.customers" in sql


def test_merge_sql_shape():
    sql = m.merge_sql("lakehouse.depo.customers", "src_v", ["id"], ["id", "name"])
    assert "MERGE INTO lakehouse.depo.customers t USING src_v s ON t.id = s.id" in sql
    assert "WHEN MATCHED AND CAST(s.__deleted AS BOOLEAN) THEN DELETE" in sql
    assert "WHEN MATCHED THEN UPDATE SET name = s.name" in sql   # id excluded from SET
    assert "WHEN NOT MATCHED AND NOT CAST(s.__deleted AS BOOLEAN) THEN INSERT (id, name) VALUES (s.id, s.name)" in sql


def test_merge_sql_all_key_table_omits_update():
    sql = m.merge_sql("lakehouse.d.t", "src", ["id"], ["id"])
    assert "WHEN MATCHED THEN UPDATE" not in sql   # nothing to set
    assert "WHEN MATCHED AND CAST(s.__deleted AS BOOLEAN) THEN DELETE" in sql
    assert "WHEN NOT MATCHED AND NOT CAST(s.__deleted AS BOOLEAN) THEN INSERT (id) VALUES (s.id)" in sql


def test_metadata_cols_include_kafka_offset():
    assert "__kafka_offset" in m.METADATA_COLS
    assert "__kafka_partition" in m.METADATA_COLS


def test_latest_per_key_orders_by_offset_last():
    sql = m.latest_per_key_sql("bronze_inc", ["id"], ["id", "name"])
    assert "ORDER BY __ts_ms DESC, __lsn DESC NULLS LAST, __kafka_offset DESC" in sql


def test_kafka_offset_not_a_business_column():
    plan = m.reconcile_plan(
        {"id": "int", "name": "string", "__op": "string", "__ts_ms": "timestamp",
         "__deleted": "string", "__lsn": "bigint", "__kafka_offset": "bigint",
         "__kafka_partition": "int", "_target_table": "string"},
        {"id": "int", "name": "string"},
    )
    assert plan.adds == []
    assert plan.ok


def test_ts_ms_excluded_from_silver_but_not_required_beyond_cdc_contract():
    # __ts_ms does double duty: MERGE ordering key AND (day(__ts_ms)) the
    # Bronze partition key -- but it's still just a Bronze metadata column,
    # excluded from Silver's business columns like the rest of METADATA_COLS.
    assert "__ts_ms" in m.METADATA_COLS
    bronze = {"id": "bigint", "name": "string",
              "__op": "string", "__ts_ms": "timestamp", "__deleted": "string", "__lsn": "bigint"}
    silver = {"id": "bigint", "name": "string"}
    plan = m.reconcile_plan(bronze, silver)
    assert plan.adds == []                      # __ts_ms is NOT a business ADD
    # __ts_ms is part of the required CDC op-contract:
    assert m.has_cdc_metadata({"__op": "x", "__ts_ms": "y", "__deleted": "z"}) is True


def test_is_commit_conflict_matches_nessie_and_iceberg():
    assert m.is_commit_conflict(Exception("Requested commit conflicts with existing state"))
    assert m.is_commit_conflict(Exception("org.apache.iceberg.exceptions.CommitFailedException: ..."))
    assert m.is_commit_conflict(Exception("NessieReferenceConflictException"))
    assert not m.is_commit_conflict(Exception("table not found"))


def test_run_with_retry_retries_then_succeeds():
    calls = {"n": 0}
    slept = []

    def fn():
        calls["n"] += 1
        if calls["n"] < 3:
            raise Exception("commit conflicts with existing state")
        return "ok"

    out = m.run_with_retry(fn, attempts=5, is_retryable=m.is_commit_conflict,
                            sleep=slept.append, base_delay=0.1)
    assert out == "ok"
    assert calls["n"] == 3
    assert len(slept) == 2


def test_run_with_retry_gives_up_and_raises_last():
    def fn():
        raise Exception("commit conflicts with existing state")
    with pytest.raises(Exception, match="conflicts"):
        m.run_with_retry(fn, attempts=3, is_retryable=m.is_commit_conflict,
                          sleep=lambda s: None, base_delay=0.0)


def test_run_with_retry_non_retryable_raises_immediately():
    calls = {"n": 0}

    def fn():
        calls["n"] += 1
        raise Exception("table not found")
    with pytest.raises(Exception, match="not found"):
        m.run_with_retry(fn, attempts=5, is_retryable=m.is_commit_conflict,
                          sleep=lambda s: None)
    assert calls["n"] == 1
