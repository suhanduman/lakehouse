from __future__ import annotations

from typing import List, Optional

from pydantic import BaseModel, field_validator, model_validator
from typing_extensions import Literal

from app import source_types

DeleteMode = Literal["pipeline_only", "with_data"]


class ColumnSpec(BaseModel):
    """Iceberg tablo pre-create için kolon tanımı (SourceSpec.columns). `type`
    kaynak/SQL tip adıdır (IcebergService SQL→Iceberg eşlemesi yapar). identifier
    (PK) kolonları IcebergService tarafından zaten REQUIRED'e zorlanır."""

    name: str
    type: str
    required: bool = False


class SourceSpec(BaseModel):
    """Domain model for a data source registered with the lakehouse console.

    Validation (enforced in `_validate_kind_type_requirements`):
      - kind="scheduled", type="mongo"            -> requires `cron`
      - kind="scheduled", type in ("mssql", "pg") -> requires `jdbc_url` and `incrementing_col`
      - kind="cdc",       type="mongo"            -> requires `mongo_uri`
      - kind="cdc",       type in ("mssql", "pg", "mysql") -> requires `db_host`
    """

    source: str
    kind: Literal["cdc", "scheduled", "stream", "batch"]
    type: Literal["mssql", "pg", "mongo", "mysql", "kafka", "s3", "http", "mqtt", "rabbitmq"]
    db: str
    table: str
    # Unique pipeline namespace (Sub-project B-v2): each source owns its own
    # target_ns (used to derive that pipeline's own Bronze/Silver buckets and
    # Iceberg namespaces) -- NOT a shared group label multiple sources land into.
    target_ns: str
    target_table: str

    db_host: Optional[str] = None
    jdbc_url: Optional[str] = None
    mongo_uri: Optional[str] = None
    incrementing_col: Optional[str] = None
    timestamp_col: Optional[str] = None
    poll_ms: Optional[int] = None
    cron: Optional[str] = None
    disposition: Optional[Literal["entity", "event"]] = None
    # existing-Kafka entity/upsert: name of an upstream BOOLEAN field whose
    # value is the per-record delete flag. When set (entity only), the sink
    # renames it to __deleted so the Bronze->Silver MERGE can DELETE. Must be
    # present on every record and NOT listed in `columns` (it is consumed as
    # the __deleted metadata column). Unset => upsert-only (no deletes).
    delete_field: Optional[str] = None
    kafka_bootstrap: Optional[str] = None   # existing-Kafka: external brokers (None => in-cluster)

    # batch/s3: platform's own S3 file-set registered as an Iceberg table via a
    # scheduled Spark job (spark-batch lane). Schedule reuses `cron` above.
    s3_bucket: Optional[str] = None
    s3_prefix: Optional[str] = None
    file_format: Optional[Literal["parquet", "json", "avro"]] = None

    # stream/http, stream/mqtt, stream/rabbitmq: Apache Camel Kafka SOURCE
    # connectors (Plan B3), kafka-connect-source lane. poll_ms above is
    # reused for HTTP's poll period.
    http_url: Optional[str] = None
    mqtt_broker: Optional[str] = None
    mqtt_topic: Optional[str] = None
    rabbitmq_uri: Optional[str] = None
    rabbitmq_queue: Optional[str] = None

    # Iceberg tablo pre-create için hedef şema + PK: sink auto-create identifier
    # koymadığından tablo, connector'dan önce identifier field ile yaratılmalı —
    # bkz. AddSourceOrchestrator "table" adımı + IcebergService. Console
    # formu/discovery doldurur; identifier verilmezse orchestrator
    # `incrementing_col` (scheduled) veya `_id` (mongo) fallback'ini dener.
    columns: Optional[List[ColumnSpec]] = None
    identifier: Optional[List[str]] = None

    @field_validator("source")
    @classmethod
    def _source_is_rfc1123_label(cls, v: str) -> str:
        import re
        if not re.fullmatch(r"[a-z0-9]([-a-z0-9]*[a-z0-9])?", v):
            raise ValueError(
                "source must be an RFC1123 label (lowercase letters, digits, '-'; "
                f"must start/end alphanumeric) — got {v!r}"
            )
        return v

    @field_validator("target_ns")
    @classmethod
    def _target_ns_is_canonical(cls, v: str) -> str:
        """target_ns doubles as this pipeline's Silver Iceberg namespace AND the
        seed for its per-pipeline Bronze/Silver bucket names (B-v2, see
        render_service._pipeline_bucket / _k8s_name). _k8s_name's bucket-name
        sanitization is LOSSY (lowercases + maps any non-[a-z0-9-] run to a
        single '-'), so two namespace-DISTINCT values (e.g. "MyPipe" vs
        "mypipe", or "a.b" vs "a_b") can sanitize-collide onto the SAME
        bucket even though they pass the orchestrator's namespace-uniqueness
        check -- reintroducing the shared-bucket/data-loss class B-v2 exists
        to eliminate (deleting one pipeline would wipe the other's data).
        Requiring target_ns to already be canonical lowercase [a-z0-9_]
        up front makes the namespace -> bucket mapping injective (only
        '_' -> '-' is applied, no case-fold/dot-collapse)."""
        import re
        if not re.fullmatch(r"[a-z0-9_]+", v):
            raise ValueError(
                "target_ns is the pipeline name / Silver Iceberg namespace and must "
                f"be lowercase [a-z0-9_] (non-empty) — got {v!r}"
            )
        return v

    @model_validator(mode="after")
    def _validate_kind_type_requirements(self) -> "SourceSpec":
        try:
            descriptor = source_types.get(self.kind, self.type)
        except KeyError as e:
            raise ValueError(str(e)) from e  # -> "unknown source (kind=..., type=...)"
        missing = [f for f in descriptor.required_fields if not getattr(self, f)]
        if missing:
            raise ValueError(
                f"{self.kind}+{self.type} requires {list(descriptor.required_fields)} "
                f"(missing: {', '.join(missing)})"
            )
        if self.disposition is not None:
            allowed = source_types.allowed_dispositions(descriptor)
            if self.disposition not in allowed:
                raise ValueError(
                    f"{self.kind}+{self.type} disposition must be one of {list(allowed)} "
                    f"(got {self.disposition!r})"
                )
        return self

    def effective_disposition(self) -> str:
        return self.disposition or source_types.get(self.kind, self.type).disposition


class SourceCredentials(BaseModel):
    user: str
    password: str
