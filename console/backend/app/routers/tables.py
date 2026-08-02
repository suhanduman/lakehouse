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

from typing import Any, Dict, List, Tuple

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel

from app.deps import get_iceberg, get_k8s, get_trino, require_action
from app.services import render_service
from app.services.authz import Action
from app.services.iceberg_service import IcebergService, IdentifierMismatch
from app.services.k8s_service import K8sService
from app.services.pipeline_topology import assemble_pipelines
from app.services.trino_service import TrinoService

router = APIRouter(prefix="/api/tables", tags=["tables"])

# Surfaced on any table not owned by an active pipeline's owned_tables set
# (see `_owned_map` below) -- either a deleted pipeline's leftover, or a
# table someone created by hand outside the console.
_ORPHAN_HINT = "no active pipeline — leftover from a deleted pipeline or hand-created"


def _owned_map(pipelines: List[Dict[str, Any]], key: str) -> Dict[str, Tuple[str, str]]:
    """`fqn -> (pipeline_name, role)` for every item in every pipeline's
    `owned_tables`/`owned_buckets` list (`key`). `role` is `authoritative`
    when the item equals that pipeline's `authoritative.fqn`; otherwise
    `bronze` for the first owned slot, `silver` for the second (mirrors
    `assemble_pipelines`: `owned_tables[0]`/`owned_buckets[0]` is always the
    Bronze side, `[1]` -- present only for the `entity` disposition -- is
    Silver). A pipeline that failed to assemble (`error` key, no owned_tables/
    owned_buckets) contributes nothing rather than raising."""
    out: Dict[str, Tuple[str, str]] = {}
    for pipeline in pipelines:
        owned = pipeline.get(key) or []
        authoritative_fqn = (pipeline.get("authoritative") or {}).get("fqn")
        for idx, item in enumerate(owned):
            role = "authoritative" if item == authoritative_fqn else ("bronze" if idx == 0 else "silver")
            out[item] = (pipeline["name"], role)
    return out


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
    k8s: K8sService = Depends(get_k8s),
    iceberg: IcebergService = Depends(get_iceberg),
) -> Dict[str, Any]:
    # Silver-existence inventory (ALWAYS the "lakehouse" catalog, regardless
    # of `catalog` -- mirrors app.routers.pipelines.list_pipelines) so
    # assemble_pipelines's entity/event disposition call is correct even when
    # this endpoint is queried for a different catalog (e.g. "rawlake").
    lakehouse_namespaces = {
        ns: set(trino.list_tables("lakehouse", ns)) for ns in trino.list_namespaces("lakehouse")
    }
    pipelines = assemble_pipelines(
        k8s.list_sources(), lakehouse_namespaces, k8s.get_status, k8s.get_spark_status
    )
    used_tables = _owned_map(pipelines, "owned_tables")

    namespaces = []
    for ns in trino.list_namespaces(catalog):
        layer = "bronze" if ns.endswith(render_service.BRONZE_NAMESPACE_SUFFIX) else "silver"
        tables = []
        for table in trino.list_tables(catalog, ns):
            fqn = f"{catalog}.{ns}.{table}"
            stats = iceberg.table_stats(ns, table, layer)
            entry: Dict[str, Any] = {
                "table": table,
                "used": fqn in used_tables,
                "records": stats["records"] if stats else None,
            }
            if fqn in used_tables:
                pipeline_name, role = used_tables[fqn]
                entry["pipeline"] = pipeline_name
                entry["role"] = role
            else:
                entry["hint"] = _ORPHAN_HINT
            tables.append(entry)
        namespaces.append({"name": ns, "tables": tables})
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
