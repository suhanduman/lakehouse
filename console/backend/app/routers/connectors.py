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

import httpx
from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel

from app.config import settings
from app.deps import get_connect, get_kafka_consumer, require_action
from app.services.authz import Action
from app.services.connect_service import ConnectService
from app.services.kafka_consumer_service import KafkaConsumerService
from app.services.logs_hint import build_logs_hint

router = APIRouter(prefix="/api/connectors", tags=["connectors"])

_MAX_DLQ = 500


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
    try:
        connect.restart_connector(
            name, include_tasks=payload.include_tasks, only_failed=payload.only_failed
        )
    except httpx.HTTPStatusError as exc:
        # surface the upstream Connect status verbatim (esp. 409
        # rebalance-in-progress -> retryable, not a hard failure)
        raise HTTPException(
            status_code=exc.response.status_code,
            detail=f"connector restart failed: {exc.response.text}",
        ) from exc
    except httpx.RequestError as exc:
        # Connect worker unreachable -> upstream failure, not a client error
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"kafka connect unreachable: {exc}",
        ) from exc
    return {
        "ok": True,
        "name": name,
        "restarted": {
            "include_tasks": payload.include_tasks,
            "only_failed": payload.only_failed,
        },
    }


@router.get("/{name}/dlq", dependencies=[Depends(require_action(Action.READ))])
def connector_dlq(
    name: str,
    limit: int = 50,
    connect: ConnectService = Depends(get_connect),
    kafka: KafkaConsumerService = Depends(get_kafka_consumer),
) -> Dict[str, Any]:
    """Resolve the connector's DLQ topic (`errors.deadletterqueue.topic.name`
    -- sinks only) and return a count + last-N dead-letter records. Degrades
    like /debug: Connect/Kafka unreachable never 500s (count:null,
    records:[]). `limit` is capped at `_MAX_DLQ`; `limit=0` skips the
    (heavier) `read_last` call entirely -- count-only.
    """
    limit = max(0, min(limit, _MAX_DLQ))
    try:
        cfg = connect.connector_config(name)
    except Exception:  # noqa: BLE001 -- connector missing / Connect down -> treat as no dlq info
        cfg = {}
    topic = cfg.get("errors.deadletterqueue.topic.name")
    if not topic:
        return {"has_dlq": False,
                "hint": "no DLQ on this connector -- dropped records for the pipeline "
                        "land in the sink's DLQ"}
    count = kafka.topic_record_count(topic)
    records = kafka.read_last(topic, limit) if limit > 0 else []
    return {"has_dlq": True, "topic": topic, "count": count,
            "returned": len(records), "records": records}
