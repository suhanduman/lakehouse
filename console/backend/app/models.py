from __future__ import annotations

from typing import List, Optional

from pydantic import BaseModel, model_validator
from typing_extensions import Literal

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
      - kind="cdc",       type in ("mssql", "pg") -> requires `db_host`
    """

    source: str
    kind: Literal["cdc", "scheduled"]
    type: Literal["mssql", "pg", "mongo"]
    db: str
    table: str
    target_ns: str
    target_table: str

    db_host: Optional[str] = None
    jdbc_url: Optional[str] = None
    mongo_uri: Optional[str] = None
    incrementing_col: Optional[str] = None
    timestamp_col: Optional[str] = None
    poll_ms: Optional[int] = None
    cron: Optional[str] = None

    # Iceberg tablo pre-create için hedef şema + PK: sink auto-create identifier
    # koymadığından tablo, connector'dan önce identifier field ile yaratılmalı —
    # bkz. AddSourceOrchestrator "table" adımı + IcebergService. Console
    # formu/discovery doldurur; identifier verilmezse orchestrator
    # `incrementing_col` (scheduled) veya `_id` (mongo) fallback'ini dener.
    columns: Optional[List[ColumnSpec]] = None
    identifier: Optional[List[str]] = None

    @model_validator(mode="after")
    def _validate_kind_type_requirements(self) -> "SourceSpec":
        if self.kind == "scheduled" and self.type == "mongo":
            if not self.cron:
                raise ValueError("scheduled+mongo requires 'cron'")
        elif self.kind == "scheduled" and self.type in ("mssql", "pg"):
            missing = [f for f in ("jdbc_url", "incrementing_col") if not getattr(self, f)]
            if missing:
                raise ValueError(
                    f"scheduled+{self.type} requires 'jdbc_url' and 'incrementing_col' "
                    f"(missing: {', '.join(missing)})"
                )
        elif self.kind == "cdc" and self.type == "mongo":
            if not self.mongo_uri:
                raise ValueError("cdc+mongo requires 'mongo_uri'")
        elif self.kind == "cdc" and self.type in ("mssql", "pg"):
            if not self.db_host:
                raise ValueError(f"cdc+{self.type} requires 'db_host'")
        return self


class SourceCredentials(BaseModel):
    user: str
    password: str
