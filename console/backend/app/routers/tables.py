"""`/api/tables` router: Trino/Iceberg namespace+table listing, creation, and
destructive drop, gated by `app.services.authz`.

Mirrors `sources.py`'s pattern: every route carries its authz check
as a route-decorator `dependencies=[...]` gate (`deps.require_action(...)`),
which FastAPI resolves BEFORE any body-parameter provider dependency, so an
unauthorized caller gets 403 without a Trino connection ever being opened.

`Action.TABLE_CREATE` gates both bringing a namespace into existence (via
`render_service.render_namespace_ddl`) and the table DDL itself. The DELETE
is gated by `Action.SOURCE_DELETE_WITH_DATA` (the same admin-only action
`sources.py`'s `with_data` delete uses) since dropping an Iceberg table is
equally destructive/data-losing.
"""

from __future__ import annotations

from typing import Any, Dict, List

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel

from app.deps import get_iceberg, get_trino, require_action
from app.services import render_service
from app.services.authz import Action
from app.services.iceberg_service import IcebergService, IdentifierMismatch
from app.services.trino_service import TrinoService

router = APIRouter(prefix="/api/tables", tags=["tables"])


class ColumnSpec(BaseModel):
    name: str
    type: str
    # identifier (PK) kolonları Iceberg kuralı gereği REQUIRED olmak zorunda;
    # IcebergService bunu identifier için zaten zorlar, burada ipucu/override.
    required: bool = False


class CreateTableRequest(BaseModel):
    catalog: str = "lakehouse"
    namespace: str
    table: str
    columns: List[ColumnSpec]
    # CDC upsert/delete için identifier (PK) ZORUNLU — bu olmadan Iceberg sink
    # equality-delete yazamaz ve UPDATE'ler duplicate satır bırakır.
    identifier: List[str]


# --------------------------------------------------------------------------
# List -- READ
# --------------------------------------------------------------------------

@router.get("", dependencies=[Depends(require_action(Action.READ))])
def list_tables(
    catalog: str = "lakehouse",
    trino: TrinoService = Depends(get_trino),
) -> Dict[str, Any]:
    namespaces = [
        {"name": ns, "tables": trino.list_tables(catalog, ns)}
        for ns in trino.list_namespaces(catalog)
    ]
    return {"catalog": catalog, "namespaces": namespaces}


# --------------------------------------------------------------------------
# Create -- TABLE_CREATE. Tabloyu identifier field (PK) ile pyiceberg → Nessie
# REST üzerinden yaratır (Trino DDL DEĞİL — Trino identifier set edemez; bkz.
# IcebergService). Namespace kaynak-başına bucket konumuyla (Model X)
# idempotent yaratılır. Tablo zaten farklı identifier ile varsa 409
# (fail-loud) — sessiz append-only'e düşmez.
# --------------------------------------------------------------------------

@router.post(
    "",
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(require_action(Action.TABLE_CREATE))],
)
def create_table(
    payload: CreateTableRequest,
    iceberg: IcebergService = Depends(get_iceberg),
) -> Dict[str, Any]:
    location = f"s3://{render_service.bucket_name(payload.namespace)}/warehouse"
    try:
        table_id = iceberg.create_table(
            namespace=payload.namespace,
            table=payload.table,
            columns=[c.model_dump() for c in payload.columns],
            identifier=payload.identifier,
            location=location,
        )
    except IdentifierMismatch as e:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    return {"ok": True, "fqn": f"{payload.catalog}.{table_id}"}


# --------------------------------------------------------------------------
# Delete -- admin-only (SOURCE_DELETE_WITH_DATA): destructive DROP TABLE.
# --------------------------------------------------------------------------

@router.delete(
    "/{fqn}",
    dependencies=[Depends(require_action(Action.SOURCE_DELETE_WITH_DATA))],
)
def delete_table(
    fqn: str,
    trino: TrinoService = Depends(get_trino),
) -> Dict[str, Any]:
    trino.drop_table(fqn)
    return {"ok": True, "fqn": fqn}
