"""`/api/sources` router: source CRUD, gated by `app.services.authz`.

Every mutating/reading route carries its authz check as a route-decorator
`dependencies=[...]` gate (`deps.require_action(...)` / `require_delete_mode`).
Because FastAPI resolves decorator dependencies BEFORE body-parameter
provider dependencies, an unauthorized caller gets 403 *without* any
K8s/S3/Trino client ever being constructed -- authz precedes provider
construction (verified by `test_unauthorized_create_does_not_build_provider`).

`{name}` throughout is the KafkaConnector CR name (the identity `K8sService`
keys its per-connector operations on), so this router is a thin authz +
shaping layer over `K8sService`/`AddSourceOrchestrator`, not a second source
of truth.
"""

from __future__ import annotations

from dataclasses import asdict
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, Depends, HTTPException, Response, status
from pydantic import BaseModel

from app.deps import (
    get_k8s,
    get_orchestrator,
    get_s3,
    get_trino,
    require_action,
    require_delete_mode,
)
from app.models import DeleteMode, SourceCredentials, SourceSpec
from app.orchestrator import AddSourceOrchestrator
from app.services import render_service
from app.services.authz import Action
from app.services.k8s_service import K8sService
from app.services.render_service import BRONZE_NAMESPACE_SUFFIX
from app.services.s3_service import S3Service
from app.services.trino_service import TrinoService

router = APIRouter(prefix="/api/sources", tags=["sources"])


class CreateSourceRequest(BaseModel):
    spec: SourceSpec
    credentials: SourceCredentials


class PreviewSourceRequest(BaseModel):
    """Preview needs no credentials -- it never touches a real system, so the
    DirectoryConfigProvider placeholder strings render_service already emits
    (`${directory:...}`) are all a preview response ever shows in place of a
    secret."""

    spec: SourceSpec


class PatchSourceRequest(BaseModel):
    config: Dict[str, Any]


def _summary(item: Dict[str, Any]) -> Dict[str, Any]:
    """KafkaConnector CR dict -> the small shape the console UI needs (name,
    connector class, paused flag, reconciled state)."""
    metadata = item.get("metadata") or {}
    spec = item.get("spec") or {}
    connector_status = (item.get("status") or {}).get("connectorStatus") or {}
    return {
        "name": metadata.get("name"),
        "class": spec.get("class"),
        "paused": spec.get("state") == "paused",
        "state": (connector_status.get("connector") or {}).get("state"),
    }


def _find_source(k8s: K8sService, name: str) -> Dict[str, Any]:
    for item in k8s.list_sources():
        if (item.get("metadata") or {}).get("name") == name:
            return item
    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"source not found: {name}")


def _target_ns_table(cr: Dict[str, Any]) -> Tuple[str, str]:
    """Recover (target_ns, target_table) -- the SILVER identifier -- from a
    KafkaConnector CR by parsing the `transforms.route.static.value` config
    key. That value carries the BRONZE identifier
    (`"<target_ns>_raw.<target_table>"`, see render_service._route_transform):
    the sink appends the CDC change-log to Bronze, not Silver. The Silver
    table (the `silver-merge` MERGE target this teardown must actually drop)
    is derived by stripping the `_raw` (BRONZE_NAMESPACE_SUFFIX) suffix back
    off the namespace half.

    NB: this only derives the Silver drop target -- teardown of the Bronze
    changelog table itself (`<ns>_raw.<table>`) is out of scope here (follow-up)."""
    config = (cr.get("spec") or {}).get("config") or {}
    route_value = config.get("transforms.route.static.value")
    if not route_value or "." not in route_value:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=(
                "cannot resolve target namespace/table from connector config "
                "(missing transforms.route.static.value) -- refusing with_data teardown"
            ),
        )
    ns, table = route_value.split(".", 1)
    if ns.endswith(BRONZE_NAMESPACE_SUFFIX):
        ns = ns[: -len(BRONZE_NAMESPACE_SUFFIX)]
    return ns, table


def _topic_from_config(config: Dict[str, Any]) -> Optional[str]:
    """Best-effort reconstruction of the data topic a connector produces to,
    from its config -- mirroring render_service.topic_name's conventions:
      - CDC relational: topic.prefix + "." + table.include.list
      - CDC mongo:      topic.prefix + "." + collection (db stripped)
      - scheduled JDBC: topic.prefix (ends with ".") + table.whitelist
    Returns None if the shape isn't recognized (delete_topic then skipped)."""
    prefix = config.get("topic.prefix")
    if not prefix:
        return None
    if "table.include.list" in config:
        return f"{prefix}.{config['table.include.list']}"
    if "collection.include.list" in config:
        collection = config["collection.include.list"]
        _, _, tail = collection.partition(".")
        return f"{prefix}.{tail or collection}"
    if "table.whitelist" in config:
        return f"{prefix}{config['table.whitelist']}"
    return None


# --------------------------------------------------------------------------
# Create
# --------------------------------------------------------------------------

@router.post(
    "",
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(require_action(Action.SOURCE_CREATE))],
)
def create_source(
    payload: CreateSourceRequest,
    response: Response,
    orchestrator: AddSourceOrchestrator = Depends(get_orchestrator),
) -> Dict[str, Any]:
    """Runs the add-source pipeline and shapes its result.

    The orchestrator can complete its HTTP round-trip successfully while the
    *pipeline itself* failed in-band (rollback ran, or scheduled+mongo is
    unsupported) -- `AddSourceResult(ok=False)`. That must not look like a
    plain 201 Created to callers (the wizard, or any other API consumer)
    that reasonably treat 2xx as "the resource now exists". So: 201 only
    when `result.ok`; otherwise 207 Multi-Status, same body shape
    (`asdict(result)`, still carrying `ok`/`steps` for the caller to
    inspect), so a client that actually checks `ok` never needs a second
    round-trip to learn the operation failed.
    """
    result = orchestrator.add_source(payload.spec, payload.credentials)
    if not result.ok:
        response.status_code = status.HTTP_207_MULTI_STATUS
    return asdict(result)


# --------------------------------------------------------------------------
# Preview -- render the CRs the wizard is about to submit, apply nothing.
#
# Deliberately takes no K8sService/S3Service/TrinoService/AddSourceOrchestrator
# dependency: every value below comes from the pure `render_service` builders,
# so this route can never reach a cluster/bucket/warehouse no matter what
# the caller sends -- there is simply no client wired in to call.
# --------------------------------------------------------------------------

@router.post(
    "/preview",
    dependencies=[Depends(require_action(Action.SOURCE_CREATE))],
)
def preview_source(payload: PreviewSourceRequest) -> Dict[str, Any]:
    spec = payload.spec
    bucket = render_service.bucket_name(spec.target_ns)
    preview: Dict[str, Any] = {
        "bucket": bucket,
        "namespace_ddl": render_service.render_namespace_ddl(spec.target_ns, bucket),
        "connector": None,
        "kafka_topic": None,
    }
    if spec.kind == "scheduled" and spec.type == "mongo":
        # Spark SparkApplication+CronJob path -- no KafkaConnector/KafkaTopic
        # to render (see render_service.render_connector's docstring).
        # connector/kafka_topic stay None rather than raising/500ing.
        return preview
    preview["connector"] = render_service.render_connector(spec)
    preview["kafka_topic"] = render_service.render_kafka_topic(render_service.topic_name(spec))
    return preview


# --------------------------------------------------------------------------
# List / get
# --------------------------------------------------------------------------

@router.get("", dependencies=[Depends(require_action(Action.READ))])
def list_sources(
    k8s: K8sService = Depends(get_k8s),
) -> Dict[str, List[Dict[str, Any]]]:
    return {"sources": [_summary(item) for item in k8s.list_sources()]}


@router.get("/{name}", dependencies=[Depends(require_action(Action.READ))])
def get_source(
    name: str,
    k8s: K8sService = Depends(get_k8s),
) -> Dict[str, Any]:
    return _summary(_find_source(k8s, name))


# --------------------------------------------------------------------------
# Edit / pause / resume
# --------------------------------------------------------------------------

@router.patch("/{name}", dependencies=[Depends(require_action(Action.SOURCE_EDIT))])
def edit_source(
    name: str,
    payload: PatchSourceRequest,
    k8s: K8sService = Depends(get_k8s),
) -> Dict[str, Any]:
    k8s.patch_connector(name, payload.config)
    return {"ok": True, "name": name}


@router.post("/{name}/pause", dependencies=[Depends(require_action(Action.SOURCE_EDIT))])
def pause_source(
    name: str,
    k8s: K8sService = Depends(get_k8s),
) -> Dict[str, Any]:
    k8s.set_paused(name, True)
    return {"ok": True, "name": name, "paused": True}


@router.post("/{name}/resume", dependencies=[Depends(require_action(Action.SOURCE_EDIT))])
def resume_source(
    name: str,
    k8s: K8sService = Depends(get_k8s),
) -> Dict[str, Any]:
    k8s.set_paused(name, False)
    return {"ok": True, "name": name, "paused": False}


# --------------------------------------------------------------------------
# Delete -- per-mode authz gate runs (via require_delete_mode) before any
# provider is built; with_data performs the full data teardown.
# --------------------------------------------------------------------------

@router.delete("/{name}", dependencies=[Depends(require_delete_mode)])
def delete_source(
    name: str,
    mode: DeleteMode = "pipeline_only",
    k8s: K8sService = Depends(get_k8s),
    s3: S3Service = Depends(get_s3),
    trino: TrinoService = Depends(get_trino),
) -> Dict[str, Any]:
    """`pipeline_only` tears down only the KafkaConnector CR (data already
    landed in the lakehouse is untouched). `with_data` (admin-only, gated by
    `require_delete_mode`) additionally deletes the KafkaTopic, drops the
    Iceberg table (`lakehouse.<ns>.<table>`) and empties the source bucket --
    the destructive, data-loss path. `<ns>`/`<table>` are recovered from the
    connector's own `transforms.route.static.value` config, and the bucket
    from `render_service.bucket_name(ns)`.
    """
    cr = _find_source(k8s, name)  # 404 before any teardown, consistent with GET
    k8s.delete_connector(name)
    if mode != "with_data":
        return {"ok": True, "name": name, "mode": mode}

    ns, table = _target_ns_table(cr)
    fqn = f"lakehouse.{ns}.{table}"
    bucket = render_service.bucket_name(ns)
    topic = _topic_from_config((cr.get("spec") or {}).get("config") or {})
    if topic:
        k8s.delete_topic(topic)
    trino.drop_table(fqn)
    s3.empty_bucket(bucket)
    return {
        "ok": True,
        "name": name,
        "mode": mode,
        "dropped_table": fqn,
        "emptied_bucket": bucket,
        "deleted_topic": topic,
    }
