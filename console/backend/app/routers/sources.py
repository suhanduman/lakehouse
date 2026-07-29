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

from app import source_types
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
from app.services.render_service import BRONZE_NAMESPACE_SUFFIX, _k8s_topic_name
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
    config: Optional[Dict[str, Any]] = None
    spec: Optional[SourceSpec] = None


_SPARK_ANN = "lakehouse.solus.dev/"


def _summary(item: Dict[str, Any]) -> Dict[str, Any]:
    """CR dict (KafkaConnector or ScheduledSparkApplication) -> the small
    shape the console UI needs (name, class, paused flag, reconciled state,
    `cr_kind`, and -- for spark sources -- the round-tripped `spark`
    annotation fields; `None` for connectors)."""
    kind = item.get("kind") or "KafkaConnector"
    metadata = item.get("metadata") or {}
    spec = item.get("spec") or {}
    if kind == "ScheduledSparkApplication":
        ann = metadata.get("annotations") or {}
        return {
            "name": metadata.get("name"),
            "class": "ScheduledSparkApplication",
            "paused": bool(spec.get("suspend")),
            "state": (item.get("status") or {}).get("scheduleState"),
            "cr_kind": kind,
            "spark": {
                "source": ann.get(f"{_SPARK_ANN}source"),
                "target_ns": ann.get(f"{_SPARK_ANN}target-ns"),
                "target_table": ann.get(f"{_SPARK_ANN}target-table"),
                "s3_bucket": ann.get(f"{_SPARK_ANN}s3-bucket"),
                "s3_prefix": ann.get(f"{_SPARK_ANN}s3-prefix"),
                "file_format": ann.get(f"{_SPARK_ANN}file-format"),
                "cron": ann.get(f"{_SPARK_ANN}cron"),
            },
        }
    connector_status = (item.get("status") or {}).get("connectorStatus") or {}
    return {
        "name": metadata.get("name"),
        "class": spec.get("class"),
        "paused": spec.get("state") == "paused",
        "state": (connector_status.get("connector") or {}).get("state"),
        "cr_kind": "KafkaConnector",
        "spark": None,
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


def _kind_of(cr: Dict[str, Any]) -> str:
    return cr.get("kind") or "KafkaConnector"


def _spark_target(cr: Dict[str, Any]) -> Tuple[str, str]:
    """(target_ns, target_table) for a spark source, from its round-trip
    annotations, falling back to parsing `--target rawlake.<ns>.<tbl>` from the
    CR's spark arguments."""
    ann = (cr.get("metadata") or {}).get("annotations") or {}
    ns, tbl = ann.get(f"{_SPARK_ANN}target-ns"), ann.get(f"{_SPARK_ANN}target-table")
    if ns and tbl:
        return ns, tbl
    args = ((cr.get("spec") or {}).get("template") or {}).get("arguments") or []
    if "--target" in args:
        _, ns, tbl = args[args.index("--target") + 1].split(".", 2)   # rawlake.<ns>.<tbl>
        return ns, tbl
    raise HTTPException(status_code=status.HTTP_409_CONFLICT,
                        detail="cannot resolve spark target ns/table -- refusing with_data teardown")


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
    descriptor = source_types.get(spec.kind, spec.type)
    if descriptor.lane == "spark-batch":
        # Spark SparkApplication+CronJob path -- no KafkaConnector/KafkaTopic
        # to render (see render_service.render_connector's docstring).
        # connector/kafka_topic stay None rather than raising/500ing.
        # Registry-driven (not hard-coded to scheduled+mongo) so any future
        # spark-batch source type is handled the same way automatically.
        return preview
    preview["connector"] = render_service.render_connector(spec)
    if descriptor.topic_key:
        # Kafka-ingest (existing-Kafka) sources consume a pre-existing topic
        # rather than owning one (descriptor.topic_key == ""), so there is no
        # KafkaTopic CR to render for them -- kafka_topic stays None.
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


# NB: registered BEFORE the `/{name}` GET below -- FastAPI/Starlette matches
# routes in registration order, so if this came after `/{name}` a request for
# `/api/sources/types` would be captured there instead (name="types" ->
# 404 "source not found: types") rather than reaching this route.
@router.get("/types", dependencies=[Depends(require_action(Action.READ))])
def list_source_types() -> Dict[str, List[Dict[str, Any]]]:
    """The source-type registry, for the add-source wizard to render itself
    (types, required fields, lane/disposition) instead of hard-coding a
    source-type list client-side."""
    out = []
    for d in source_types.all_types():
        out.append({
            "id": d.id, "kind": d.kind, "type": d.type, "lane": d.lane,
            "disposition": d.disposition,
            "dispositions": list(source_types.allowed_dispositions(d)),
            "required_fields": list(d.required_fields),
            "needs_bootstrap": d.type == "kafka",
        })
    return {"types": out}


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
    orchestrator: AddSourceOrchestrator = Depends(get_orchestrator),
) -> Dict[str, Any]:
    cr = _find_source(k8s, name)
    if _kind_of(cr) == "ScheduledSparkApplication":
        if payload.spec is None:
            raise HTTPException(status_code=400, detail="spark source edit requires `spec`")
        orchestrator.edit_spark_source(payload.spec)
    else:
        if payload.config is None:
            raise HTTPException(status_code=400, detail="connector edit requires `config`")
        k8s.patch_connector(name, payload.config)
    return {"ok": True, "name": name}


@router.post("/{name}/pause", dependencies=[Depends(require_action(Action.SOURCE_EDIT))])
def pause_source(
    name: str,
    k8s: K8sService = Depends(get_k8s),
) -> Dict[str, Any]:
    cr = _find_source(k8s, name)
    if _kind_of(cr) == "ScheduledSparkApplication":
        k8s.set_spark_suspended(name, True)
    else:
        k8s.set_paused(name, True)
    return {"ok": True, "name": name, "paused": True}


@router.post("/{name}/resume", dependencies=[Depends(require_action(Action.SOURCE_EDIT))])
def resume_source(
    name: str,
    k8s: K8sService = Depends(get_k8s),
) -> Dict[str, Any]:
    cr = _find_source(k8s, name)
    if _kind_of(cr) == "ScheduledSparkApplication":
        k8s.set_spark_suspended(name, False)
    else:
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
    if _kind_of(cr) == "ScheduledSparkApplication":
        k8s.delete_spark_job(name)
        if mode != "with_data":
            return {"ok": True, "name": name, "mode": mode}
        ns, table = _spark_target(cr)
        fqn = f"rawlake.{ns}.{table}"          # batch lane -> rawlake, no Silver
        bucket = render_service.bucket_name(ns)
        trino.drop_table(fqn)
        s3.empty_bucket(bucket)
        return {"ok": True, "name": name, "mode": mode, "dropped_table": fqn,
                "emptied_bucket": bucket, "deleted_topic": None}   # spark-batch: no topic

    k8s.delete_connector(name)
    if mode != "with_data":
        return {"ok": True, "name": name, "mode": mode}

    ns, table = _target_ns_table(cr)
    fqn = f"lakehouse.{ns}.{table}"
    bucket = render_service.bucket_name(ns)
    topic = _topic_from_config((cr.get("spec") or {}).get("config") or {})
    if topic:
        k8s.delete_topic(_k8s_topic_name(topic))   # delete by CR name (Task 1 sanitized it)
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
