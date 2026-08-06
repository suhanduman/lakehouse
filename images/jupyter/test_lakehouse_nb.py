import importlib

BASE = {
    "DEPT": "veri",
    "NESSIE_URI": "http://nessie:19120/iceberg/",
    "NESSIE_SANDBOX_CATALOG": "sandbox_veri",
    "NESSIE_CLIENT_ID": "svc-sandbox-veri",
    "NESSIE_OAUTH_SECRET": "sek",
    "AWS_ENDPOINT_URL_S3": "https://flashblade.kc:443",
    "AWS_REGION": "us-east-1",
    "AWS_ACCESS_KEY_ID": "ak",
    "AWS_SECRET_ACCESS_KEY": "sk",
    "NESSIE_WAREHOUSE": "s3://depo/warehouse",
    "TRINO_HOST": "trino-coordinator.lakehouse.svc",
    "TRINO_PORT": "8443",
    "SVC_JUPYTER_TRINO_SECRET": "jt",
    "OIDC_ISSUER": "https://kc/realms/lakehouse",
}


def _load(monkeypatch):
    for k, v in BASE.items():
        monkeypatch.setenv(k, v)
    return importlib.reload(importlib.import_module("lakehouse_nb"))


def test_iceberg_props_use_env_endpoint(monkeypatch):
    p = _load(monkeypatch)._iceberg_props()
    assert p["s3.endpoint"] == "https://flashblade.kc:443" and p["uri"] == "http://nessie:19120/iceberg/"
    assert "minio" not in str(p).lower()               # HARDCODE YOK


def test_spark_conf_has_catalogs(monkeypatch):
    c = _load(monkeypatch)._spark_conf()
    assert c["spark.sql.catalog.sandbox_veri.uri"] == "http://nessie:19120/iceberg/"
    assert c["spark.hadoop.fs.s3a.endpoint"] == "https://flashblade.kc:443"


def test_spark_conf_warehouse_values_match_zeppelin_reference(monkeypatch):
    # Final-review fix wave: chart/templates/_zeppelin.tpl is the working
    # reference for these SAME Nessie REST catalogs -- `lakehouse` gets the
    # FULL warehouse location (NESSIE_WAREHOUSE, unmodified), `rawlake`
    # stays the bare "rawdata" name, and the sandbox catalog gets the full
    # "s3a://sandbox-<dept>/" form (never a bare bucket name).
    c = _load(monkeypatch)._spark_conf()
    assert c["spark.sql.catalog.lakehouse.warehouse"] == "s3://depo/warehouse"
    assert c["spark.sql.catalog.rawlake.warehouse"] == "rawdata"
    assert c["spark.sql.catalog.sandbox_veri.warehouse"] == "s3a://sandbox-veri/"


def test_trino_connect_args(monkeypatch):
    a = _load(monkeypatch)._trino_connect_args(token="JWT")
    assert a["http_scheme"] == "https" and a["host"].startswith("trino-coordinator") and a["verify"] == "/etc/trino-ca/ca.crt"


def test_iceberg_props_write_capability_invariant(monkeypatch):
    p = _load(monkeypatch)._iceberg_props()
    # Task-1 spike: PyIceberg 0.10.0 REST + OAuth2 client-credentials is
    # write-capable -- credential/oauth2-server-uri must stay; scope must
    # stay OMITTED (an explicit scope="" was accepted today but a future
    # Keycloak may need a real scope, so don't lock one in).
    assert "credential" in p
    assert "oauth2-server-uri" in p
    assert "scope" not in p


def test_spark_conf_catalog_invariant(monkeypatch):
    c = _load(monkeypatch)._spark_conf()
    assert "spark.sql.catalog.lakehouse" in c
    assert "spark.sql.catalog.rawlake" in c
    assert c["spark.sql.catalog.lakehouse.rest.auth.type"] == "oauth2"
    assert "spark.sql.catalog.lakehouse.rest.credential" in c
