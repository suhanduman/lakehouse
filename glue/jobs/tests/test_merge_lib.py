import pytest
import merge_lib as ml

B = [("id", "bigint"), ("status", "string"), ("amount", "decimal(38,2)"), ("updated_at", "string"),
     ("_cdc", "struct<op:string,ts:timestamp,offset:bigint,source:string,target:string,key:struct<id:bigint>>")]
CASTS = {"updated_at": "timestamp"}


def test_parse_pipelines_defaults_and_validation():
    ps = ml.parse_pipelines({"pipelines": [{"bronze": "shop_raw.orders", "keys": ["id"]}]})
    assert ps[0].write_mode == "merge-on-read" and ps[0].bucket_count == 16 and ps[0].casts == {}
    with pytest.raises(ValueError):
        ml.parse_pipelines({"pipelines": [{"bronze": "x_raw.t"}]})            # keys yok
    with pytest.raises(ValueError):
        ml.parse_pipelines({"pipelines": [{"bronze": "x_raw.t", "keys": ["a"], "write_mode": "cow"}]})
    assert ml.parse_pipelines({}) == []
    # pipelines.json'da fazladan anahtarlar var (bronze_namespaces — bakım işi kullanır): parse etkilenmez
    extra = ml.parse_pipelines({"pipelines": [{"bronze": "shop_raw.orders", "keys": ["id"]}], "bronze_namespaces": ["shop_raw"]})
    assert [p.bronze for p in extra] == ["shop_raw.orders"]


def test_silver_name_strips_raw_suffix():
    assert ml.silver_name("shop_raw.orders") == "shop.orders"
    with pytest.raises(ValueError):
        ml.silver_name("shop.orders")


def test_silver_name_requires_namespace():
    with pytest.raises(ValueError, match="namespace"):
        ml.silver_name("orders")


def test_create_silver_sql_requires_keys():
    with pytest.raises(ValueError, match="keys"):
        ml.create_silver_sql("lakehouse.shop.orders", ml.silver_columns(B, {}), [], "merge-on-read", 4)


def test_silver_columns_drop_cdc_and_apply_casts():
    cols = ml.silver_columns(B, CASTS)
    assert cols == [("id", "bigint"), ("status", "string"), ("amount", "decimal(38,2)"), ("updated_at", "timestamp")]
    with pytest.raises(ValueError):
        ml.silver_columns(B, {"nope": "int"})


def test_create_silver_sql():
    sql = ml.create_silver_sql("lakehouse.shop.orders", ml.silver_columns(B, CASTS), ["id"], "merge-on-read", 4)
    assert sql.startswith("CREATE TABLE IF NOT EXISTS lakehouse.shop.orders (`id` bigint, `status` string, `amount` decimal(38,2), `updated_at` timestamp) USING iceberg")
    assert "PARTITIONED BY (bucket(4, `id`))" in sql
    for prop in ("'format-version'='2'", "'write.merge.mode'='merge-on-read'", "'write.update.mode'='merge-on-read'",
                 "'write.delete.mode'='merge-on-read'", "'write.distribution-mode'='hash'",
                 "'write.metadata.delete-after-commit.enabled'='true'"):
        assert prop in sql
    with pytest.raises(ValueError):
        ml.create_silver_sql("s", ml.silver_columns(B, {}), ["missing"], "merge-on-read", 4)


def test_dedup_select_sql_latest_per_key_with_cast():
    sql = ml.dedup_select_sql("bronze_inc", ml.silver_columns(B, CASTS), ["id"], CASTS)
    assert "CAST(`updated_at` AS timestamp) AS `updated_at`" in sql
    assert "row_number() OVER (PARTITION BY `id` ORDER BY _cdc.ts DESC, _cdc.offset DESC) AS __rn" in sql
    assert sql.rstrip().endswith("WHERE __rn = 1") and "_cdc.op AS __op" in sql


def test_merge_sql_delete_update_insert():
    sql = ml.merge_sql("lakehouse.shop.orders", "inc", ml.silver_columns(B, CASTS), ["id"])
    assert sql.startswith("MERGE INTO lakehouse.shop.orders t USING inc s ON t.`id` = s.`id`")
    assert "WHEN MATCHED AND s.__op = 'D' THEN DELETE" in sql
    assert "WHEN MATCHED THEN UPDATE SET `status` = s.`status`, `amount` = s.`amount`, `updated_at` = s.`updated_at`" in sql
    assert "WHEN NOT MATCHED AND s.__op <> 'D' THEN INSERT (`id`, `status`, `amount`, `updated_at`) VALUES (s.`id`, s.`status`, s.`amount`, s.`updated_at`)" in sql


def test_merge_sql_composite_key():
    sql = ml.merge_sql("s", "inc", [("a", "int"), ("b", "int"), ("v", "string")], ["a", "b"])
    assert "ON t.`a` = s.`a` AND t.`b` = s.`b`" in sql and "UPDATE SET `v` = s.`v`" in sql


def test_can_widen_safe_map():
    assert ml.can_widen("int", "bigint") and ml.can_widen("float", "double") and ml.can_widen("smallint", "int")
    assert ml.can_widen("decimal(10,2)", "decimal(38,2)")
    assert not ml.can_widen("bigint", "int")
    assert not ml.can_widen("decimal(10,2)", "decimal(10,3)")
    assert not ml.can_widen("string", "timestamp")
    assert ml.can_widen("string", "string")


def test_plan_schema_changes_add_widen_conflict():
    silver = [("id", "bigint"), ("status", "string"), ("amount", "decimal(10,2)")]
    target = [("id", "bigint"), ("status", "string"), ("amount", "decimal(38,2)"), ("note", "string")]
    ddl = ml.plan_schema_changes("lakehouse.shop.orders", silver, target)
    assert ddl == ["ALTER TABLE lakehouse.shop.orders ALTER COLUMN `amount` TYPE decimal(38,2)",
                   "ALTER TABLE lakehouse.shop.orders ADD COLUMN `note` string"]
    with pytest.raises(ml.SchemaConflict):
        ml.plan_schema_changes("s", [("id", "bigint")], [("id", "string")])
    # Silver'da olup Bronze'da olmayan kolon dokunulmaz
    assert ml.plan_schema_changes("s", [("id", "bigint"), ("legacy", "string")], [("id", "bigint")]) == []
