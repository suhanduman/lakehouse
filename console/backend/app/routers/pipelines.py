"""`GET /api/pipelines` router: deterministic per-pipeline topology +
authoritative table, for the Console Pipeline Map.

Thin wiring layer only -- exactly like `app.routers.gitops`/`sources`, all
the actual topology logic lives in the pure, injectable
`app.services.pipeline_topology.assemble_pipelines`. This router's only job
is gathering the three real inputs that function needs (`k8s.list_sources()`,
the Trino namespace/table inventory, and the two status callables) and
handing them off -- no topology decisions are made here.
"""

from __future__ import annotations

from typing import Any, Dict

from fastapi import APIRouter, Depends

from app.deps import get_k8s, get_trino, require_action
from app.services.authz import Action
from app.services.k8s_service import K8sService
from app.services.pipeline_topology import assemble_pipelines
from app.services.trino_service import TrinoService

router = APIRouter(prefix="/api/pipelines", tags=["pipelines"])


@router.get("", dependencies=[Depends(require_action(Action.READ))])
def list_pipelines(
    k8s: K8sService = Depends(get_k8s),
    trino: TrinoService = Depends(get_trino),
) -> Dict[str, Any]:
    sources = k8s.list_sources()
    namespaces = {
        ns: set(trino.list_tables("lakehouse", ns)) for ns in trino.list_namespaces("lakehouse")
    }
    pipelines = assemble_pipelines(sources, namespaces, k8s.get_status, k8s.get_spark_status)
    return {"pipelines": pipelines}
