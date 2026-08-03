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
    silver_default_bucket_count: int = 16   # Silver bucket(N, id) partition default; large tables override per-pipeline
    connect_url: str = "http://connect-connect-api:8083"
    connect_cluster_name: str = "connect"    # Strimzi KafkaConnect cluster name (pod label selector)
    kafka_external_bootstrap: str = ""   # external listener a cluster-outside producer uses (ingest-config)
    kafka_cluster_name: str = "kafka"   # strimzi.io/cluster label on provisioned KafkaUsers
    kafka_internal_bootstrap: str = "kafka-kafka-bootstrap:9093"  # internal TLS+SCRAM listener
    # Debezium Kafka signal channel + notification sink topics (snapshot-lifecycle
    # feature: incremental/ad-hoc snapshot signals + connector notifications),
    # shared by every CDC connector (see render_service._render_cdc_*).
    debezium_signal_topic: str = "debezium-signals"
    debezium_notification_topic: str = "debezium-notifications"
    kafka_consumer_user: str = "connect"          # SASL identity the Console reads DLQ topics as
    kafka_cluster_ca_secret: str = "kafka-cluster-ca-cert"  # Strimzi cluster CA (TLS trust), key ca.crt
    # Optional deep-link into an external logging UI (OpenShift logging / Kibana /
    # Grafana). Placeholders {namespace} {connector} {connect_cluster} are filled;
    # empty -> no link is produced (the oc-command recipe is always present).
    external_logging_url_template: str = ""
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
