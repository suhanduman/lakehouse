from typing import List

from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    namespace: str = "example"
    s3_endpoint: str = ""
    # Iceberg tablo pre-create (IcebergService → pyiceberg → Nessie REST). CDC
    # upsert'ün identifier field'ı için tablolar Trino DDL ile DEĞİL pyiceberg
    # ile yaratılır (bkz. tools/create_iceberg_table.py). S3 creds pyiceberg
    # FileIO içindir.
    nessie_uri: str = "http://nessie:19120/iceberg/"
    s3_access_key: str = ""
    s3_secret_key: str = ""
    trino_host: str = "trino-coordinator"
    trino_port: int = 8080
    trino_user: str = "lakehouse-console"
    connect_url: str = "http://connect-connect-api:8083"
    apicurio_url: str = "http://apicurio-registry:8080/apis/registry/v2"
    oidc_issuer: str = ""
    oidc_audience: str = "lakehouse-console"
    # Explicit JWT signature-algorithm allowlist. NEVER trust the token's own
    # `alg` header -- an attacker can set `alg: none` or swap to a weaker/HMAC
    # algorithm. jose only accepts a signature matching one of these.
    oidc_algorithms: List[str] = ["RS256"]
    # Spark-batch lane (render_service.render_spark_job / ScheduledSparkApplication).
    # Neutral in-cluster defaults consistent with the chart's spark-operator
    # image/secret naming; override per-deployment via env (SPARK_IMAGE /
    # S3_SECRET_NAME).
    spark_image: str = "image-registry.openshift-image-registry.svc:5000/lakehouse/spark-py:1.0"
    s3_secret_name: str = "s3-credentials"

settings = Settings()
