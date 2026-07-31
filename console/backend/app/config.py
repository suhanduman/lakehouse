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
    # s3a job-code delivery (2026-07-31-s3a-job-code-delivery Task 4): the
    # rendered ScheduledSparkApplication's `mainApplicationFile` points at
    # s3a://<jobs_bucket>/<jobs_prefix>/s3_register_table.py (mirrors the
    # chart's own jobs-seed bucket/prefix, chart/values.yaml
    # storage.jobsBucket/jobsPrefix -- see JOBS_BUCKET/JOBS_PREFIX below).
    jobs_bucket: str = "spark-jobs"
    jobs_prefix: str = "jobs"
    # Hadoop **fs.s3a** endpoint for that spark-submit bootstrap fetch --
    # mirrors chart/templates/_helpers.tpl's `lakehouse.spark.baseConf`
    # (`.Values.storage.s3.endpoint`), NOT the same thing as `s3_endpoint`
    # above: that field is sourced from the S3 CREDENTIALS SECRET's
    # `endpoint` key (see this Deployment's env: block in console.yaml) and
    # drives the console's own boto3 S3Service / AWS_ENDPOINT_URL_S3 data
    # path. A same-named field here would collide -- k8s `env:` entries
    # override `envFrom` ConfigMap values for the same var name, so an
    # `S3_ENDPOINT` ConfigMap key would silently lose to the Secret-sourced
    # one. Kept deliberately separate (SPARK_S3A_ENDPOINT below).
    spark_s3a_endpoint: str = ""

    # GitOps write-path (deploy_mode=gitops): Console commits rendered pipeline
    # manifests to a git repo ArgoCD watches, instead of applying imperatively.
    # deploy_mode default "direct" preserves the current B-v1 behavior.
    deploy_mode: str = "direct"                 # "direct" | "gitops"
    gitops_repo_url: str = ""                   # ssh://... or https://... pipeline repo
    gitops_branch: str = "main"
    gitops_path: str = "pipelines"              # subdir the pipelines Application watches
    gitops_credential_secret: str = "gitops-credential"  # k8s Secret; SSH key or https token
    argocd_namespace: str = "argocd"          # openshift-gitops on OpenShift
    gitops_app_name: str = "lakehouse-pipelines"

settings = Settings()
