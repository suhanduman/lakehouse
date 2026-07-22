from tools.jobs import maintenance_lib as ml


def test_bronze_ttl_delete_sql_day_aligned():
    sql = ml.bronze_ttl_delete_sql("rawlake", "depo_raw", "customers", 30)
    assert sql == ("DELETE FROM rawlake.depo_raw.customers "
                   "WHERE __ts_ms < date_sub(current_date(), 30)")


def test_is_bronze_namespace():
    assert ml.is_bronze_namespace("depo_raw") is True
    assert ml.is_bronze_namespace("depo") is False
    assert ml.is_bronze_namespace("ops") is False
