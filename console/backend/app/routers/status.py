"""`/api/status` router: unified connector health view (state + a
best-effort maintenance/dlq signal derived from Kafka Connect's own status
endpoint), gated by `app.services.authz`.

Degrades gracefully: a single unreachable connector's `connector_status`
call failing never 500s the whole endpoint -- it's surfaced per-entry as
`reachable: False` + `error`; if `list_connectors` itself fails (Connect
worker unreachable), the whole response degrades to
`{"connectors": [], "reachable": False, "error": ...}` rather than raising.

Field provenance (all four signals, from what `ConnectService` actually
exposes):
  - `state`       -- `connector_status()["connector"]["state"]` verbatim.
  - `maintenance` -- derived: Kafka Connect's own state machine reports
                     `PAUSED` when a connector has been intentionally taken
                     out of service, which is exactly a "maintenance" signal.
  - `dlq`         -- derived: any task in `FAILED` state on a connector
                     configured with `errors.deadletterqueue.topic.name`
                     (see `render_service`'s `_render_cdc_*`) means records
                     are landing in its dead-letter topic.
  - `lag`         -- Kafka Connect's REST API does not expose consumer-group
                     lag (that lives in `__consumer_offsets` / would need a
                     JMX or AdminClient integration outside `ConnectService`'s
                     scope); left as an explicit `None` placeholder rather
                     than silently omitted, so API consumers can distinguish
                     "not yet wired up" from "zero lag".
"""

from __future__ import annotations

from typing import Any, Dict, List

from fastapi import APIRouter, Depends

from app.deps import get_connect, require_action
from app.services.authz import Action
from app.services.connect_service import ConnectService

router = APIRouter(prefix="/api/status", tags=["status"])


def _connector_entry(connect: ConnectService, name: str) -> Dict[str, Any]:
    try:
        status_dict = connect.connector_status(name)
    except Exception as exc:  # noqa: BLE001 -- degrade per-connector, never 500
        return {
            "name": name,
            "state": "unknown",
            "maintenance": False,
            "dlq": None,
            "lag": None,
            "reachable": False,
            "error": str(exc),
        }
    connector_state = (status_dict.get("connector") or {}).get("state")
    task_states: List[Any] = [t.get("state") for t in status_dict.get("tasks") or []]
    return {
        "name": name,
        "state": connector_state,
        "tasks": task_states,
        "maintenance": connector_state == "PAUSED",
        "dlq": any(s == "FAILED" for s in task_states),
        "lag": None,
        "reachable": True,
    }


@router.get("", dependencies=[Depends(require_action(Action.READ))])
def get_status(connect: ConnectService = Depends(get_connect)) -> Dict[str, Any]:
    try:
        names = connect.list_connectors()
    except Exception as exc:  # noqa: BLE001 -- Connect worker unreachable
        return {"connectors": [], "reachable": False, "error": str(exc)}
    return {
        "connectors": [_connector_entry(connect, name) for name in names],
        "reachable": True,
    }
