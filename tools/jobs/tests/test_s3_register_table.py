import pytest
from tools.jobs.s3_register_table import build_ctas_sql, build_namespace_sql, VALID_FORMATS


def test_build_ctas_sql_parquet():
    assert build_ctas_sql("rawlake.nyc.trips", "parquet", "ham-veri", "raw/") == (
        "CREATE OR REPLACE TABLE rawlake.nyc.trips USING iceberg AS "
        "SELECT * FROM parquet.`s3a://ham-veri/raw/`")


def test_build_namespace_sql():
    assert build_namespace_sql("rawlake.nyc.trips") == "CREATE NAMESPACE IF NOT EXISTS rawlake.nyc"


def test_valid_formats():
    assert set(VALID_FORMATS) == {"parquet", "json", "avro"}


@pytest.mark.parametrize("bad", ["", "csv", "orc"])
def test_bad_format_rejected(bad):
    with pytest.raises(ValueError):
        build_ctas_sql("rawlake.a.b", bad, "buck", "p/")
