"""`/api/buckets` router: S3 bucket listing + creation, gated by
`app.services.authz`.

Mirrors `sources.py`'s pattern: the authz check is a route-decorator
`dependencies=[...]` gate resolved BEFORE the `S3Service` provider is built,
so an unauthorized caller never causes a boto3 client to be constructed.
Bucket creation uses `Action.TABLE_CREATE` (same action `tables.py`'s create
route uses) since provisioning a bucket is part of the same "bring lakehouse
storage into existence" capability, granted to ADMIN/ANALYST, not STUDENT.
"""

from __future__ import annotations

from typing import Any, Dict, List, Tuple

from fastapi import APIRouter, Depends, status
from pydantic import BaseModel

from app.deps import get_k8s, get_s3, get_trino, require_action
from app.services.authz import Action
from app.services.k8s_service import K8sService
from app.services.pipeline_topology import assemble_pipelines
from app.services.s3_service import S3Service
from app.services.trino_service import TrinoService

router = APIRouter(prefix="/api/buckets", tags=["buckets"])

# Same literal as app.routers.tables._ORPHAN_HINT -- kept as an independent
# constant (not a cross-router import) since each router is a self-contained
# thin wiring layer, matching this codebase's existing tables.py/buckets.py
# split.
_ORPHAN_HINT = "no active pipeline — leftover from a deleted pipeline or hand-created"


def _owned_map(pipelines: List[Dict[str, Any]], key: str) -> Dict[str, Tuple[str, str]]:
    """`name -> (pipeline_name, role)` for every item in every pipeline's
    `owned_tables`/`owned_buckets` list (`key`). See
    `app.routers.tables._owned_map`'s docstring -- identical logic, kept
    local rather than shared to keep each router self-contained. For
    buckets specifically, `authoritative.fqn` is always a *table* FQN, so it
    never matches a bucket name -- role is always `bronze`/`silver` by owned
    slot (`owned_buckets[0]` is Bronze, `[1]`, entity-only, is Silver)."""
    out: Dict[str, Tuple[str, str]] = {}
    for pipeline in pipelines:
        owned = pipeline.get(key) or []
        authoritative_fqn = (pipeline.get("authoritative") or {}).get("fqn")
        for idx, item in enumerate(owned):
            role = "authoritative" if item == authoritative_fqn else ("bronze" if idx == 0 else "silver")
            out[item] = (pipeline["name"], role)
    return out


class CreateBucketRequest(BaseModel):
    name: str


@router.get("", dependencies=[Depends(require_action(Action.READ))])
def list_buckets(
    s3: S3Service = Depends(get_s3),
    k8s: K8sService = Depends(get_k8s),
    trino: TrinoService = Depends(get_trino),
) -> Dict[str, Any]:
    namespaces = {
        ns: set(trino.list_tables("lakehouse", ns)) for ns in trino.list_namespaces("lakehouse")
    }
    pipelines = assemble_pipelines(
        k8s.list_sources(), namespaces, k8s.get_status, k8s.get_spark_status
    )
    used_buckets = _owned_map(pipelines, "owned_buckets")

    buckets = []
    for name in s3.list_buckets():
        entry: Dict[str, Any] = {
            "name": name,
            "used": name in used_buckets,
            "objects": s3.object_count(name),
        }
        if name in used_buckets:
            pipeline_name, role = used_buckets[name]
            entry["pipeline"] = pipeline_name
            entry["role"] = role
        else:
            entry["hint"] = _ORPHAN_HINT
        buckets.append(entry)
    return {"buckets": buckets}


@router.post(
    "",
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(require_action(Action.TABLE_CREATE))],
)
def create_bucket(
    payload: CreateBucketRequest,
    s3: S3Service = Depends(get_s3),
) -> Dict[str, Any]:
    s3.create_bucket(payload.name)
    return {"ok": True, "name": payload.name}
