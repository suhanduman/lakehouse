import importlib

# Sandbox v2 (2026-08-07-sandbox-v2 Task 4): BASE now mirrors the UNIFIED
# sandbox env contract chart/templates/23-jupyterhub.yaml's pre_spawn_hook
# injects -- NESSIE_SANDBOX_CATALOG is the fixed "sandbox" literal
# (.Values.sandbox.namespace) instead of a per-dept "sandbox_<dept>";
# NESSIE_CLIENT_ID is the single "svc-sandbox" instead of
# "svc-sandbox-<dept>"; NESSIE_SANDBOX_SCHEMA is the NEW per-user schema
# ("sandbox.testuser", as if spawner.user.name == "testuser"). DEPT is no
# longer set by the chart at all (dept concept retired) -- dropped here too.
BASE = {
    "NESSIE_URI": "http://nessie:19120/iceberg/",
    "NESSIE_SANDBOX_CATALOG": "sandbox",
    "NESSIE_SANDBOX_SCHEMA": "sandbox.testuser",
    "NESSIE_CLIENT_ID": "svc-sandbox",
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
    assert "warehouse" not in p                        # plain iceberg() catalog: no warehouse key (spike-verified default)


def test_iceberg_props_warehouse_param(monkeypatch):
    # Sandbox v2 (Task 4): iceberg_sandbox() passes warehouse=<catalog> so
    # the REST catalog protocol resolves the NAMED "sandbox" warehouse
    # (same mechanism Trino's sandbox.properties uses).
    p = _load(monkeypatch)._iceberg_props(warehouse="sandbox")
    assert p["warehouse"] == "sandbox"


def test_spark_conf_has_catalogs(monkeypatch):
    c = _load(monkeypatch)._spark_conf()
    assert c["spark.sql.catalog.sandbox.uri"] == "http://nessie:19120/iceberg/"
    assert c["spark.hadoop.fs.s3a.endpoint"] == "https://flashblade.kc:443"


def test_spark_conf_warehouse_values_match_zeppelin_reference(monkeypatch):
    # Final-review fix wave: chart/templates/_zeppelin.tpl is the working
    # reference for these SAME Nessie REST catalogs -- `lakehouse` gets the
    # FULL warehouse location (NESSIE_WAREHOUSE, unmodified), `rawlake`
    # stays the bare "rawdata" name.
    #
    # Whole-branch review C1 fix (2026-08-07-sandbox-v2 final review): the
    # unified sandbox catalog's warehouse must be the REGISTERED NAME
    # ("sandbox", i.e. NESSIE_SANDBOX_CATALOG's value) -- the SAME NAME
    # Trino's sandbox.properties (`iceberg.rest-catalog.warehouse=sandbox`)
    # and this module's own iceberg_sandbox()/_iceberg_props(warehouse=...)
    # already use -- NOT an "s3a://sandbox/" location string, which matches
    # neither the registered NAME nor the exact registered LOCATION
    # ("s3://sandbox/warehouse") and would silently fall back to the
    # DEFAULT warehouse (prod `depo` bucket).
    c = _load(monkeypatch)._spark_conf()
    assert c["spark.sql.catalog.lakehouse.warehouse"] == "s3://depo/warehouse"
    assert c["spark.sql.catalog.rawlake.warehouse"] == "rawdata"
    assert c["spark.sql.catalog.sandbox.warehouse"] == "sandbox"


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


# --- Sandbox v2 (2026-08-07-sandbox-v2 Task 4): per-user sandbox schema ---


def test_sandbox_schema_uses_nessie_sandbox_schema(monkeypatch):
    nb = _load(monkeypatch)
    assert nb.sandbox_schema() == "sandbox.testuser"


def test_sandbox_schema_falls_back_to_sandbox_catalog_when_schema_unset(monkeypatch):
    for k, v in BASE.items():
        if k != "NESSIE_SANDBOX_SCHEMA":
            monkeypatch.setenv(k, v)
    monkeypatch.delenv("NESSIE_SANDBOX_SCHEMA", raising=False)
    nb = importlib.reload(importlib.import_module("lakehouse_nb"))
    assert nb.sandbox_schema() == "sandbox"           # falls back to NESSIE_SANDBOX_CATALOG


def test_ensure_schema_sql_defaults_to_sandbox_schema(monkeypatch):
    nb = _load(monkeypatch)
    assert nb._ensure_schema_sql() == "CREATE SCHEMA IF NOT EXISTS sandbox.testuser"


def test_ensure_schema_sql_explicit_schema_overrides_default(monkeypatch):
    nb = _load(monkeypatch)
    assert nb._ensure_schema_sql("lakehouse.gold") == "CREATE SCHEMA IF NOT EXISTS lakehouse.gold"


class _FakeIcebergCatalog:
    """Minimal PyIceberg-catalog double for _ensure_namespace() -- just
    enough of list_namespaces()/create_namespace() to prove the
    create-if-missing logic without a live Nessie REST catalog."""

    def __init__(self, existing=()):
        self._namespaces = list(existing)
        self.created = []

    def list_namespaces(self):
        return list(self._namespaces)

    def create_namespace(self, ns):
        self._namespaces.append(ns)
        self.created.append(ns)


def test_ensure_namespace_creates_missing_sandbox_namespace(monkeypatch):
    nb = _load(monkeypatch)
    cat = _FakeIcebergCatalog(existing=[])
    ns = nb._ensure_namespace(cat)
    assert ns == ("sandbox", "testuser")
    assert cat.created == [("sandbox", "testuser")]


def test_ensure_namespace_is_a_noop_when_namespace_already_exists(monkeypatch):
    nb = _load(monkeypatch)
    cat = _FakeIcebergCatalog(existing=[("sandbox", "testuser")])
    ns = nb._ensure_namespace(cat)
    assert ns == ("sandbox", "testuser")
    assert cat.created == []          # no duplicate create_namespace call
