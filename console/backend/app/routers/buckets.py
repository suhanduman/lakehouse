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

from typing import Any, Dict, List

from fastapi import APIRouter, Depends, status
from pydantic import BaseModel

from app.deps import get_s3, require_action
from app.services.authz import Action
from app.services.s3_service import S3Service

router = APIRouter(prefix="/api/buckets", tags=["buckets"])


class CreateBucketRequest(BaseModel):
    name: str


@router.get("", dependencies=[Depends(require_action(Action.READ))])
def list_buckets(s3: S3Service = Depends(get_s3)) -> Dict[str, List[str]]:
    return {"buckets": s3.list_buckets()}


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
