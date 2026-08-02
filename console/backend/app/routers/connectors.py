"""`/api/connectors` router: runtime operability for a single connector
(source or dedicated sink), addressed by name.

- GET /{name}/debug   -- full status incl. the per-task `trace`
  (which /api/status fetches but discards) + a build_logs_hint recipe.
- POST /{name}/restart -- Kafka Connect REST restart (no CR spec mutation;
  works in direct + gitops deploy modes).

Degrades like app/routers/status.py: Connect unreachable never 500s.
"""

from __future__ import annotations

from typing import Any, Dict, List

from fastapi import APIRouter, Depends
from pydantic import BaseModel

from app.config import settings
from app.deps import get_connect, require_action
from app.services.authz import Action
from app.services.connect_service import ConnectService
from app.services.logs_hint import build_logs_hint

router = APIRouter(prefix="/api/connectors", tags=["connectors"])


class RestartRequest(BaseModel):
    include_tasks: bool = True
    only_failed: bool = False


def _is_cdc(status_dict: Dict[str, Any]) -> bool:
    # Debezium connectors surface a "type": "source"; the logger hint is
    # best-effort -- io.debezium simply won't match non-Debezium logs.
    return (status_dict.get("type") == "source")


@router.get("/{name}/debug", dependencies=[Depends(require_action(Action.READ))])
def connector_debug(
    name: str, connect: ConnectService = Depends(get_connect)
) -> Dict[str, Any]:
    try:
        status_dict = connect.connector_status(name)
        state = (status_dict.get("connector") or {}).get("state") or "unknown"
        tasks: List[Dict[str, Any]] = [
            {"id": t.get("id"), "state": t.get("state"),
             "worker_id": t.get("worker_id"), "trace": t.get("trace")}
            for t in (status_dict.get("tasks") or [])
        ]
        failed = [t["id"] for t in tasks if t["state"] == "FAILED" and t["id"] is not None]
        is_cdc = _is_cdc(status_dict)
    except Exception:  # noqa: BLE001 -- degrade, never 500 (mirrors status.py)
        state, tasks, failed, is_cdc = "unknown", [], [], False
    return {
        "name": name,
        "state": state,
        "tasks": tasks,
        "logs_hint": build_logs_hint(
            connector=name,
            namespace=settings.namespace,
            connect_cluster=settings.connect_cluster_name,
            url_template=settings.external_logging_url_template,
            failed_task_ids=failed,
            is_cdc=is_cdc,
        ),
    }


@router.post("/{name}/restart", dependencies=[Depends(require_action(Action.SOURCE_EDIT))])
def connector_restart(
    name: str,
    payload: RestartRequest = RestartRequest(),
    connect: ConnectService = Depends(get_connect),
) -> Dict[str, Any]:
    connect.restart_connector(
        name, include_tasks=payload.include_tasks, only_failed=payload.only_failed
    )
    return {
        "ok": True,
        "name": name,
        "restarted": {
            "include_tasks": payload.include_tasks,
            "only_failed": payload.only_failed,
        },
    }
