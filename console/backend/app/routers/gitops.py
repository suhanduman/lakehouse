"""GET /gitops/status -- ArgoCD Application sync/health, grouped per source.

`deploy_mode=direct` (default): `{"mode": "direct"}`, no k8s call at all.

`deploy_mode=gitops`: reads the ArgoCD Application CR's `.status` (via
`K8sService.get_application_status`) and its `.status.resources[]`, then
joins each resource `(kind, name)` against `K8sService.list_sources()`'s
`lakehouse.solus.dev/source` annotation to roll sync/health up per source.
Best-effort: if the Application isn't present yet (`get_application_status`
returns None), responds with empty sources/outOfSync rather than erroring.
"""

from fastapi import APIRouter, Depends

from app.config import settings
from app.deps import get_k8s
from app.services.k8s_service import K8sService

router = APIRouter(prefix="/gitops", tags=["gitops"])
_SRC_ANN = "lakehouse.solus.dev/source"


@router.get("/status")
def gitops_status(k8s: K8sService = Depends(get_k8s)):
    if settings.deploy_mode != "gitops":
        return {"mode": "direct"}
    status = k8s.get_application_status(settings.gitops_app_name, settings.argocd_namespace)
    if status is None:
        return {"mode": "gitops", "application": None, "sources": [], "outOfSync": []}
    name_to_source = {}
    for item in k8s.list_sources():
        md = item.get("metadata") or {}
        src = (md.get("annotations") or {}).get(_SRC_ANN)
        if src:
            name_to_source[(item.get("kind"), md.get("name"))] = src
    per_source: dict = {}
    out_of_sync = []
    for res in status.get("resources", []) or []:
        entry = {"kind": res.get("kind"), "name": res.get("name"),
                 "status": res.get("status"), "health": (res.get("health") or {}).get("status")}
        if res.get("status") == "OutOfSync":
            out_of_sync.append(entry)
        src = name_to_source.get((res.get("kind"), res.get("name")))
        if src:
            per_source.setdefault(src, []).append(entry)
    sources = []
    for src, entries in per_source.items():
        healths = {e["health"] for e in entries if e["health"]}
        syncs = {e["status"] for e in entries if e["status"]}
        sources.append({"source": src, "resources": entries,
                        "sync": "OutOfSync" if "OutOfSync" in syncs else ("Synced" if syncs else "Unknown"),
                        "health": "Degraded" if ("Degraded" in healths or "Missing" in healths)
                                  else ("Healthy" if healths == {"Healthy"} else "Progressing")})
    return {"mode": "gitops",
            "application": {"sync": (status.get("sync") or {}).get("status"),
                            "health": (status.get("health") or {}).get("status")},
            "sources": sources, "outOfSync": out_of_sync}
