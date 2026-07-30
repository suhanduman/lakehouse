import pytest
from tools.jobs.s3_register_table import build_ctas_sql, build_namespace_sql, VALID_FORMATS
from tools.jobs.s3_register_table import s3a_conf_from_env


def test_s3a_conf_from_env_sets_endpoint_and_path_style():
    assert s3a_conf_from_env({"AWS_ENDPOINT_URL_S3": "http://minio:9000"}) == {
        "fs.s3a.endpoint": "http://minio:9000",
        "fs.s3a.path.style.access": "true",
    }


def test_s3a_conf_from_env_empty_when_no_endpoint():
    assert s3a_conf_from_env({}) == {}


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
