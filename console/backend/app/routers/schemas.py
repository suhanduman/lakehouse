"""`/api/schemas` router: Apicurio Registry schema listing (read-only),
gated by `app.services.authz`.

Mirrors `sources.py`'s pattern: the authz check is a route-decorator
`dependencies=[...]` gate resolved BEFORE the `ApicurioClient` provider is
built. There is no mutating route here -- schema registration happens as a
side effect of the Kafka Connect converter (Debezium/JDBC connectors), not
through this console -- so `Action.READ` is the only gate needed.
"""

from __future__ import annotations

from typing import Any, Dict, List

from fastapi import APIRouter, Depends

from app.deps import get_apicurio, require_action
from app.services.authz import Action
from app.services.connect_service import ApicurioClient

router = APIRouter(prefix="/api/schemas", tags=["schemas"])


@router.get("", dependencies=[Depends(require_action(Action.READ))])
def list_schemas(
    apicurio: ApicurioClient = Depends(get_apicurio),
) -> Dict[str, List[Dict[str, Any]]]:
    return {"schemas": apicurio.list_schemas()}
