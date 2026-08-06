"""KÇ lakehouse notebook helper — spark()/trino()/iceberg() env'den. Sırlar env'den; argv'ye girmez (ps-safe)."""
import os, json, urllib.parse, urllib.request


def _e(k):
    return os.environ[k]


def _oauth_uri():
    return _e("OIDC_ISSUER").rstrip("/") + "/protocol/openid-connect/token"


def _iceberg_props():
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
    return p


def iceberg():
    from pyiceberg.catalog import load_catalog
    return load_catalog("lakehouse", **_iceberg_props())


def _spark_conf():
    nu = _e("NESSIE_URI"); sb = _e("NESSIE_SANDBOX_CATALOG"); wh = os.environ.get("NESSIE_WAREHOUSE", "depo")

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
    c.update(cat("lakehouse", wh)); c.update(cat("rawlake", "rawdata")); c.update(cat(sb, sb.replace("sandbox_", "sandbox-")))
    return c


def spark(app="notebook"):
    from pyspark.sql import SparkSession
    b = SparkSession.builder.appName(app)
    for k, v in _spark_conf().items():
        b = b.config(k, v)     # secret .config'e (argv değil)
    return b.getOrCreate()


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
    return _t.dbapi.connect(**_trino_connect_args(_trino_jwt()))
