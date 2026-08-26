"""KÇ lakehouse notebook helper — spark()/trino()/iceberg() env'den. Sırlar env'den; argv'ye girmez (ps-safe)."""
import os, json, urllib.parse, urllib.request


def _e(k):
    return os.environ[k]


def _oauth_uri():
    return _e("OIDC_ISSUER").rstrip("/") + "/protocol/openid-connect/token"


def sandbox_schema():
    """Sandbox v2 (2026-08-07-sandbox-v2 Task 4): the per-user sandbox
    schema every spark()/trino()/iceberg_sandbox() write defaults to --
    "sandbox.<user>" (NESSIE_SANDBOX_SCHEMA, set by chart/templates/
    23-jupyterhub.yaml's pre_spawn_hook from spawner.user.name). Falls back
    to the bare NESSIE_SANDBOX_CATALOG (e.g. "sandbox") for a caller with no
    per-user identity set at all (e.g. a shared/non-Jupyter caller) -- still
    a valid Nessie namespace, just not personally scoped. This is a CLIENT
    convention only (soft isolation, NOT enforced by Nessie/Trino
    authorization -- see the design doc's known limitation): callers should
    use this value (or a sub-namespace of it) for their own sandbox writes.
    """
    return os.environ.get("NESSIE_SANDBOX_SCHEMA") or _e("NESSIE_SANDBOX_CATALOG")


def _ensure_schema_sql(schema=None):
    """CREATE SCHEMA IF NOT EXISTS <schema> -- the same DDL text works
    against both Spark SQL and Trino for an Iceberg/Nessie-backed schema.
    Defaults to sandbox_schema() when no explicit schema is given."""
    return "CREATE SCHEMA IF NOT EXISTS %s" % (schema or sandbox_schema())


def _iceberg_props(warehouse=None):
    p = {"type": "rest", "uri": _e("NESSIE_URI"),
         "s3.endpoint": _e("AWS_ENDPOINT_URL_S3"), "s3.region": os.environ.get("AWS_REGION", "us-east-1"),
         "s3.access-key-id": _e("AWS_ACCESS_KEY_ID"), "s3.secret-access-key": _e("AWS_SECRET_ACCESS_KEY")}
    # Task-1 spike: PyIceberg 0.10.0 REST catalog + OAuth2 client-credentials
    # was live-verified WRITE-CAPABLE -> keep credential/oauth2-server-uri.
    # Do NOT add scope="" here (spike found an empty scope is accepted today
    # but a future Keycloak may need a real one) -- omit scope entirely so
    # pyiceberg falls back to its default "catalog" scope.
    p["credential"] = "%s:%s" % (_e("NESSIE_CLIENT_ID"), _e("NESSIE_OAUTH_SECRET"))
    p["oauth2-server-uri"] = _oauth_uri()
    # Sandbox v2 (Task 4): the REST catalog protocol resolves a NAMED
    # warehouse (Nessie's NESSIE_CATALOG_WAREHOUSES_<NAME>_LOCATION registry,
    # chart/templates/04-nessie-ha.yaml) via a `warehouse` config key -- same
    # mechanism Trino's sandbox.properties uses
    # (`iceberg.rest-catalog.warehouse=sandbox`, chart/templates/
    # 06-trino-ha.yaml). Only set when explicitly requested (iceberg_sandbox()
    # below) -- the plain iceberg()/lakehouse catalog below is unchanged
    # (spike-verified live behavior, no `warehouse` key, relies on Nessie's
    # own default-warehouse fallback).
    if warehouse:
        p["warehouse"] = warehouse
    return p


def iceberg():
    from pyiceberg.catalog import load_catalog
    return load_catalog("lakehouse", **_iceberg_props())


def _ensure_namespace(catalog, schema=None):
    """PyIceberg equivalent of _ensure_schema_sql() -- CREATE-namespace-if-
    missing (no native "IF NOT EXISTS" verb in the PyIceberg catalog API,
    so check list_namespaces() first). Returns the namespace as a tuple
    (PyIceberg's own identifier form, e.g. ("sandbox", "veri"))."""
    ns = tuple((schema or sandbox_schema()).split("."))
    if ns not in catalog.list_namespaces():
        catalog.create_namespace(ns)
    return ns


def iceberg_sandbox():
    """PyIceberg catalog scoped to the unified sandbox warehouse
    (NESSIE_SANDBOX_CATALOG, e.g. "sandbox") -- the per-user schema
    (sandbox_schema()) is ensured to exist before this returns, so a
    caller's first `catalog.create_table("%s.<table>" % sandbox_schema(),
    ...)` never fails on a missing namespace."""
    from pyiceberg.catalog import load_catalog
    wh = _e("NESSIE_SANDBOX_CATALOG")
    cat = load_catalog(wh, **_iceberg_props(warehouse=wh))
    _ensure_namespace(cat)
    return cat


def _spark_conf():
    # Final-review fix wave: `lakehouse.warehouse` mirrors the working
    # Zeppelin reference (chart/templates/_zeppelin.tpl) byte-for-byte for
    # the SAME Nessie REST catalog -- it is the FULL warehouse location
    # (NESSIE_WAREHOUSE carries e.g. "s3://depo/warehouse", set by
    # chart/templates/23-jupyterhub.yaml's `pre_spawn_hook` from
    # `.Values.nessie.catalog.warehouse.location`, NOT a bucket-only name).
    # `rawlake.warehouse` stays the bare registered NAME "rawdata" (same
    # pattern the REST catalog resolves that warehouse by everywhere else).
    #
    # Whole-branch review C1 fix (2026-08-07-sandbox-v2 final review): the
    # sandbox catalog's `warehouse` must be the REGISTERED NAME
    # (NESSIE_SANDBOX_CATALOG, e.g. "sandbox" -- the Nessie
    # NESSIE_CATALOG_WAREHOUSES_SANDBOX_LOCATION registry key,
    # chart/templates/04-nessie-ha.yaml), NOT an `s3a://<bucket>/` location
    # string -- the SAME NAME Trino's `iceberg.rest-catalog.warehouse=
    # sandbox` (chart/templates/06-trino-ha.yaml) and this file's own
    # `iceberg_sandbox()`/`_iceberg_props(warehouse=...)` already use. A
    # location-string `warehouse` value matches neither the registered NAME
    # nor the exact registered LOCATION (`s3://sandbox/warehouse`, note the
    # extra `/warehouse` suffix and `s3://` vs `s3a://` scheme), so Nessie
    # falls back to the DEFAULT warehouse (`s3://depo/warehouse`) -- Spark
    # sandbox writes would silently land in the PROD `depo` bucket.
    nu = _e("NESSIE_URI"); sb = _e("NESSIE_SANDBOX_CATALOG")
    wh = os.environ.get("NESSIE_WAREHOUSE", "s3://depo/warehouse")
    sb_warehouse = sb

    def cat(name, warehouse):
        return {f"spark.sql.catalog.{name}": "org.apache.iceberg.spark.SparkCatalog",
                f"spark.sql.catalog.{name}.catalog-impl": "org.apache.iceberg.rest.RESTCatalog",
                f"spark.sql.catalog.{name}.uri": nu, f"spark.sql.catalog.{name}.warehouse": warehouse,
                f"spark.sql.catalog.{name}.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
                f"spark.sql.catalog.{name}.rest.auth.type": "oauth2",
                f"spark.sql.catalog.{name}.rest.oauth2-server-uri": _oauth_uri(),
                f"spark.sql.catalog.{name}.rest.credential": "%s:%s" % (_e("NESSIE_CLIENT_ID"), _e("NESSIE_OAUTH_SECRET"))}

    c = {"spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
         "spark.hadoop.fs.s3a.path.style.access": "true", "spark.hadoop.fs.s3a.endpoint": _e("AWS_ENDPOINT_URL_S3")}
    c.update(cat("lakehouse", wh)); c.update(cat("rawlake", "rawdata")); c.update(cat(sb, sb_warehouse))
    return c


def spark(app="notebook"):
    from pyspark.sql import SparkSession
    b = SparkSession.builder.appName(app)
    for k, v in _spark_conf().items():
        b = b.config(k, v)     # secret .config'e (argv değil)
    s = b.getOrCreate()
    # Sandbox v2 (Task 4): default unqualified writes to the per-user
    # sandbox schema -- CREATE SCHEMA IF NOT EXISTS'd on first use, then
    # Spark's multi-part `USE catalog.namespace` sets BOTH the current
    # catalog and current database, so a bare `CREATE TABLE t AS ...`/`df.
    # write...saveAsTable("t")` lands in sandbox_schema() by default.
    # Fully-qualified references (e.g. "lakehouse.silver.foo") are
    # unaffected -- this only changes what an UNQUALIFIED name resolves to.
    s.sql(_ensure_schema_sql())
    s.sql("USE %s" % sandbox_schema())
    return s


def _trino_jwt():
    d = urllib.parse.urlencode({"grant_type": "client_credentials", "client_id": "svc-jupyter-trino",
                                 "client_secret": _e("SVC_JUPYTER_TRINO_SECRET")}).encode()
    return json.load(urllib.request.urlopen(_oauth_uri(), data=d, timeout=15))["access_token"]


def _trino_connect_args(token):
    from trino.auth import JWTAuthentication
    return {"host": _e("TRINO_HOST"), "port": int(os.environ.get("TRINO_PORT", "8443")), "http_scheme": "https",
            "auth": JWTAuthentication(token), "catalog": "lakehouse", "verify": "/etc/trino-ca/ca.crt"}


def trino():
    import trino as _t
    conn = _t.dbapi.connect(**_trino_connect_args(_trino_jwt()))
    # Sandbox v2 (Task 4): CREATE SCHEMA IF NOT EXISTS the per-user sandbox
    # schema before returning the connection -- so a caller's first CTAS
    # (`CREATE TABLE %s.<table> AS SELECT ...` % sandbox_schema()) never
    # fails on a missing schema. The connection's own default `catalog`
    # (above, "lakehouse", read-only) is untouched -- sandbox writes use a
    # fully-qualified `sandbox.<user>.<table>` reference regardless of the
    # session's default catalog, same as any other cross-catalog Trino SQL.
    cur = conn.cursor()
    cur.execute(_ensure_schema_sql())
    cur.fetchall()
    cur.close()
    return conn
