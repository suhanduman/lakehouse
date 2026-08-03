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

import json
import types
from dataclasses import asdict
from typing import Any, Dict, List, Literal, Optional, Tuple

from fastapi import APIRouter, Depends, HTTPException, Response, status
from pydantic import BaseModel

from app import source_types
from app.config import settings
from app.deps import (
    get_connect,
    get_k8s,
    get_kafka_consumer,
    get_kafka_producer,
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
from app.services.connect_service import ConnectService
from app.services.k8s_service import K8sService
from app.services.kafka_consumer_service import KafkaConsumerService
from app.services.kafka_producer_service import KafkaProducerService
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


class RotateCredentialsRequest(BaseModel):
    user: str
    password: str


class SnapshotRequest(BaseModel):
    """Body for `POST /{name}/snapshot` and `/{name}/snapshot/stop` -- a
    Debezium `execute-snapshot`/`stop-snapshot` Kafka signal. `tables`
    defaults to the connector's own `table.include.list`/
    `collection.include.list` (the SOURCE table/collection, not the Kafka
    topic) when omitted."""

    tables: Optional[List[str]] = None
    type: Literal["incremental", "blocking"]


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


def _source_of(cr: Dict[str, Any]) -> Optional[str]:
    """The logical source id (e.g. "pgdemo") a CR's `{_SPARK_ANN}source`
    round-trip annotation carries -- render_service stamps this on every
    rendered connector/sink/spark CR (see `render_service._connector`/
    `render_spark_job`), so it is available uniformly across CR kinds. This
    is the SAME recovery `delete_source`'s gitops branch and
    `_kafka_ingest_producer` already do inline; `rotate_credentials` reuses
    it (rather than the CR's own `metadata.name`, a composite like
    "dbz-pgdemo-customers") because the credential Secret is provisioned
    under the bare source id, not the connector's CR name. Returns `None`
    when the annotation is missing (e.g. a CR pre-dating this annotation, or
    applied by hand) -- callers must fail loud (never guess a Secret name)."""
    return ((cr.get("metadata") or {}).get("annotations") or {}).get(f"{_SPARK_ANN}source")


def _resolve_kafka_ingest_cr(k8s: K8sService, name: str) -> Dict[str, Any]:
    """Resolve `name` to a source CR, tolerating BOTH identities a
    kafka-ingest source is addressed by:

    - the connector's own composite CR name (`kafka-ingest-<source>-
      <target_table>`) -- what `SourceDetail` passes, and what `_find_source`
      already resolves directly via `metadata.name`;
    - the BARE source id (e.g. "kafka1", i.e. `spec.source`) -- what the
      Add-Source wizard passes (`getIngestConfig(form.source)`) right after
      create, which `_find_source` 404s on since no CR is ever named just
      "kafka1".

    Tries `_find_source` first (covers the composite-name path unchanged,
    for ANY source kind). Only on its 404 does this fall back to scanning
    `k8s.list_sources()` for the kafka-ingest connector (`metadata.name`
    startswith "kafka-ingest-", see `_kafka_ingest_topic`'s docstring) whose
    `{_SPARK_ANN}source` round-trip annotation equals `name` -- the same
    annotation `_kafka_ingest_producer` reads the producer username from, so
    a hit here guarantees `_kafka_ingest_producer` resolves the SAME CR
    either way. Re-raises the original 404 if neither lookup finds a CR."""
    try:
        return _find_source(k8s, name)
    except HTTPException as exc:
        if exc.status_code != status.HTTP_404_NOT_FOUND:
            raise
        for item in k8s.list_sources():
            metadata = item.get("metadata") or {}
            cr_name = metadata.get("name") or ""
            if not cr_name.startswith("kafka-ingest-"):
                continue
            annotations = metadata.get("annotations") or {}
            if annotations.get(f"{_SPARK_ANN}source") == name:
                return item
        raise


def _target_ns_table(cr: Dict[str, Any]) -> Tuple[str, str]:
    """Recover (target_ns, target_table) -- `target_ns` being the unique
    per-pipeline namespace (Sub-project B-v2), NOT a shared group label --
    from a KafkaConnector CR by parsing the `transforms.route.static.value`
    config key. That value carries the BRONZE identifier
    (`"<target_ns>_raw.<target_table>"`, see render_service._route_transform):
    the sink appends the CDC change-log to Bronze, not Silver. The pipeline
    namespace half is recovered by stripping the `_raw` (BRONZE_NAMESPACE_SUFFIX)
    suffix back off.

    Callers (`delete_source`'s `with_data` teardown) reconstruct BOTH the
    Bronze changelog table (`rawlake.<ns>_raw.<table>`) and the Silver merge
    target (`lakehouse.<ns>.<table>`) from this same (ns, table) pair -- this
    helper is not Silver-only despite its historical name."""
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


def _gitops_remediation(cr: Dict[str, Any], state: str) -> Dict[str, Any]:
    """Concrete 'do this in the pipeline repo / ArgoCD' recipe -- the Console
    never writes to git itself (see the operational-controls spec).

    `state` is the target KafkaConnector lifecycle state
    ("paused"/"running"/"stopped" -- see `K8sService.set_state`). For a
    ScheduledSparkApplication there's no analogous 3-way CR field, only
    `spec.suspend` -- both "paused" and "stopped" map to `suspend: true`
    (spark has no distinct stopped state of its own), "running" maps to
    `suspend: false`."""
    if _kind_of(cr) == "ScheduledSparkApplication":
        field, value = "spec.suspend", (state != "running")
    else:  # KafkaConnector
        field, value = "spec.state", state
    name = (cr.get("metadata") or {}).get("name")
    return {
        "reason": "pause/resume changes the CR spec and is reconciled from git",
        "where": "gitops",
        "repo": f"{settings.gitops_repo_url}@{settings.gitops_branch}",
        "path": f"{settings.gitops_path}/{name}",
        "field": field,
        "value": value,
        "steps": [
            "edit the source's manifest in the pipeline repo",
            f"set {field} = {value}",
            "commit & push",
            "ArgoCD syncs the change to the cluster",
        ],
    }


def _gitops_config_add_remediation(cr: Dict[str, Any], config: Dict[str, Any]) -> Dict[str, Any]:
    """Remediation for a CONFIG-ADD mutation (enable-snapshots) in gitops
    mode: the operator must merge these keys into the connector's
    `spec.config` in the pipeline repo (the Console never writes git).

    Distinct from `_gitops_remediation`, which is a lifecycle-state recipe
    (spec.state/spec.suspend) -- following THAT recipe for enable-snapshots
    would set the connector running and never add the signal/notification
    config, so snapshots would stay un-enabled while the recipe claimed to
    fix it.

    `config` (from `render_service._signal_and_notification_config()`) is
    redacted before being embedded: any `signal.consumer.*` key is dropped,
    since that block carries the Kafka signal channel's SASL/truststore
    credentials (`signal.consumer.sasl.jaas.config` embeds a plaintext
    password) -- this API must never echo those back to a client. The
    recipe instead tells the operator those consumer-security properties
    resolve via the existing config-provider/credential mechanism, same as
    every other connector's config."""
    name = (cr.get("metadata") or {}).get("name")
    safe_config = {k: v for k, v in config.items() if not k.startswith("signal.consumer.")}
    return {
        "reason": "enable-snapshots adds config keys and is reconciled from git",
        "where": "gitops",
        "repo": f"{settings.gitops_repo_url}@{settings.gitops_branch}",
        "path": f"{settings.gitops_path}/{name}",
        "field": "spec.config",
        "value": safe_config,
        "steps": [
            "edit the connector's manifest in the pipeline repo",
            "merge these keys into spec.config",
            "the signal.consumer.* Kafka client security properties (SASL/truststore) "
            "resolve via the existing config-provider mechanism -- do not hardcode credentials",
            "commit & push",
            "ArgoCD syncs the change to the cluster",
        ],
    }


# --------------------------------------------------------------------------
# Snapshot signal helpers (snapshot-lifecycle) -- the Console never touches
# the source DB itself; it only PRODUCES a Debezium execute-snapshot/
# stop-snapshot Kafka signal, keyed on the connector's own `topic.prefix`.
# --------------------------------------------------------------------------

# connector.class -> render_signal_table_dml dialect. Only used to build a
# minimal spec-like object (types.SimpleNamespace) for the `needs_signal_table`
# branch below -- render_signal_table_dml reads ONLY `.type`/
# `.signal_data_collection`, so this deliberately avoids constructing/
# validating a full SourceSpec from a live connector config.
_CONNECTOR_CLASS_DIALECT = {
    "io.debezium.connector.postgresql.PostgresConnector": "pg",
    "io.debezium.connector.sqlserver.SqlServerConnector": "mssql",
    "io.debezium.connector.mysql.MySqlConnector": "mysql",
    "io.debezium.connector.mongodb.MongoDbConnector": "mongo",
}


def _snapshot_data_collections(tables: Optional[List[str]], config: Dict[str, Any]) -> List[str]:
    """`payload.tables` if the caller supplied any; otherwise the connector's
    own SOURCE table/collection list (`table.include.list` for relational,
    `collection.include.list` for mongo), split on comma. This is what
    Debezium's `execute-snapshot`/`stop-snapshot` signal `data-collections`
    expects -- the source-side table name, NOT the Kafka topic
    (`_topic_from_config` is deliberately not used here)."""
    if tables:
        return tables
    derived = config.get("table.include.list") or config.get("collection.include.list") or ""
    return [t.strip() for t in derived.split(",") if t.strip()]


def _needs_signal_table_response(config: Dict[str, Any]) -> Dict[str, Any]:
    dialect = _CONNECTOR_CLASS_DIALECT.get(config.get("connector.class"), "pg")
    spec_like = types.SimpleNamespace(
        type=dialect, signal_data_collection=config.get("signal.data.collection")
    )
    return {
        "ok": False,
        "needs_signal_table": True,
        "dml": render_service.render_signal_table_dml(spec_like),
    }


def _connect_topic_acl(topic: str) -> Dict[str, Any]:
    """The scoped literal READ/DESCRIBE ACL AddSourceOrchestrator.add_source
    grants `connect` for a stream/kafka (kafka-ingest) source's customer
    topic (Task 2) -- shape must match the orchestrator's `topic_acl` exactly
    so `remove_user_acl`'s (type,name,patternType) key match finds it."""
    return {"resource": {"type": "topic", "name": topic, "patternType": "literal"},
            "operations": ["Read", "Describe"]}


def _kafka_ingest_topic(cr: Dict[str, Any]) -> Optional[str]:
    """Best-effort recovery of the customer topic a stream/kafka source's
    dedicated sink reads from, straight off its own KafkaConnector CR --
    needed so `delete_source` can revoke the `connect` ACL Task 2's add_source
    granted (this router has no SourceSpec on delete, only the CR, so the
    lane identity must be recovered structurally, the same way
    `_target_ns_table`/`_topic_from_config` above recover other per-source
    facts from a CR alone rather than from a spec).

    `render_service._render_kafka_ingest` is the ONLY renderer that names its
    connector via `_k8s_name("kafka-ingest", spec.source, spec.target_table)`
    (i.e. `metadata.name` always starts with "kafka-ingest-") and sets
    `spec.config["topics"] = spec.table` (see `_iceberg_sink_config`'s
    `topics` param) -- both a stream/kafka source's OWN CR (kafka-ingest IS
    the sink, no separate dedicated-sink CR) and its Camel-sink counterparts
    (named `<source-connector-name>-sink`) also use IcebergSinkConnector + a
    `topics` key, so the name-prefix check (not the class) is what scopes
    this to kafka-ingest specifically.

    Returns None (skip, best-effort -- matching this module's existing
    best-effort teardown tone, e.g. `_topic_from_config`) when the CR isn't a
    kafka-ingest connector or its config is missing the `topics` key."""
    name = (cr.get("metadata") or {}).get("name") or ""
    if not name.startswith("kafka-ingest-"):
        return None
    return (cr.get("spec") or {}).get("config", {}).get("topics") or None


def _kafka_ingest_producer(cr: Dict[str, Any]) -> Optional[str]:
    """The per-pipeline producer KafkaUser name (Task 5) a kafka-ingest
    source's `add_source` provisions -- `render_service._k8s_name(spec.source,
    "producer")` -- recovered from the CR's own `{_SPARK_ANN}source`
    round-trip annotation (stamped on every rendered connector by
    `render_service._connector`), NOT from this route's `name` path param:
    `name` is the CR's own composite `metadata.name`
    (`kafka-ingest-<source>-<target_table>`, see `_kafka_ingest_topic`'s
    docstring), never the bare `spec.source` the producer username is keyed
    on -- so `_k8s_name(name, "producer")` would build the WRONG username and
    silently orphan the real producer KafkaUser/Secret on delete.

    Returns None (skip, best-effort -- matching `_kafka_ingest_topic`'s tone)
    when the annotation is missing (e.g. a CR pre-dating this feature or
    applied by hand)."""
    ann = (cr.get("metadata") or {}).get("annotations") or {}
    source = ann.get(f"{_SPARK_ANN}source")
    if not source:
        return None
    return render_service._k8s_name(source, "producer")


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
    bronze_bucket = render_service.bronze_bucket_name(spec.target_ns)
    silver_bucket = render_service.silver_bucket_name(spec.target_ns)
    preview: Dict[str, Any] = {
        "bronze_bucket": bronze_bucket,
        "silver_bucket": silver_bucket,
        "namespace_ddl": render_service.render_namespace_ddl(spec.target_ns, silver_bucket),
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
# Test connection -- render a minimal per-lane config
# (render_service.render_connection_test) and ask Kafka Connect itself to
# validate it (ConnectService.validate_config). The Console never opens a DB
# connection: Connect already has every connector plugin loaded, so handing
# it the same connection keys the real renderers use and reading back the
# validation response is enough to prove reachability/credentials before the
# wizard actually creates the source.
# --------------------------------------------------------------------------

@router.post("/test-connection", dependencies=[Depends(require_action(Action.SOURCE_EDIT))])
def test_connection(
    payload: CreateSourceRequest,
    connect: ConnectService = Depends(get_connect),
) -> Dict[str, Any]:
    """Degrades never-500: a lane with nothing to test (kafka-ingest,
    spark-batch -- `render_connection_test` returns None) reports
    `applicable: False` with a `reason`; Connect being unreachable (any
    exception from `validate_config`) reports `applicable: True, ok: False`
    with a reachability message in `errors`, still 200 -- never surfaces as a
    500. Credentials are only ever sent to Connect's validate endpoint, never
    logged (the reachability message below carries only `str(exc)`, never the
    rendered config)."""
    built = render_service.render_connection_test(payload.spec, payload.credentials)
    if built is None:
        return {
            "applicable": False,
            "reason": "no external connection to test for this source type",
        }
    connector_class, config = built
    try:
        result = connect.validate_config(connector_class, config)
    except Exception as exc:  # noqa: BLE001 -- degrade, never 500; never log creds
        return {
            "applicable": True,
            "ok": False,
            "errors": [{"field": None, "message": f"could not reach Kafka Connect: {exc}"}],
        }
    errors = [
        {
            "field": (c.get("value") or {}).get("name"),
            "message": "; ".join((c.get("value") or {}).get("errors") or []),
        }
        for c in (result.get("configs") or [])
        if (c.get("value") or {}).get("errors")
    ]
    return {"applicable": True, "ok": result.get("error_count", 0) == 0, "errors": errors}


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


# NB: `app.services.pipeline_topology` imports several helpers (
# `_kafka_ingest_topic`/`_spark_target`/`_target_ns_table`/`_topic_from_config`)
# FROM this module, so a module-level `from app.services.pipeline_topology
# import assemble_pipelines` here would be circular (this module's own
# top-level import of pipeline_topology would run before those helper
# functions are defined, since pipeline_topology's own top-level import of
# THIS module -- triggered first, e.g. via `app.routers.pipelines` -- would
# see this module only partially initialized). Importing inside the handler
# defers it until call time, by which point both modules are fully loaded.
@router.get("/{name}/connectors", dependencies=[Depends(require_action(Action.READ))])
def source_connectors(
    name: str,
    k8s: K8sService = Depends(get_k8s),
    trino: TrinoService = Depends(get_trino),
) -> Dict[str, Any]:
    """Resolve `name` (either the pipeline's own name or any of its node
    names -- e.g. a dedicated sink's CR name) to its source + dedicated-sink
    connectors, reusing the same `assemble_pipelines` topology `/api/pipelines`
    builds (no second topology/grouping implementation)."""
    from app.services.pipeline_topology import assemble_pipelines

    sources = k8s.list_sources()
    namespaces = {
        ns: set(trino.list_tables("lakehouse", ns)) for ns in trino.list_namespaces("lakehouse")
    }
    pipelines = assemble_pipelines(sources, namespaces, k8s.get_status, k8s.get_spark_status)
    pipe = next(
        (p for p in pipelines
         if p.get("name") == name
         or any(n.get("name") == name for n in p.get("nodes", []))),
        None,
    )
    if pipe is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"no pipeline for '{name}'")
    role_by_type = {"connector": "source", "sink": "sink"}
    connectors = [
        {"name": n.get("name"), "role": role_by_type[n["type"]],
         "kind": n.get("kind"), "state": n.get("state")}
        for n in pipe.get("nodes", [])
        if n.get("type") in role_by_type
        # A ScheduledSparkApplication is emitted as a type:"connector" node
        # (see pipeline_topology._batch_pipeline) but it is NOT a Kafka
        # Connect connector -- a restart POST against it 404s. The spec's
        # non-goal is explicit: "Spark job 'restart' -- connectors only."
        and n.get("kind") != "ScheduledSparkApplication"
    ]
    return {"connectors": connectors}


# NB: same circular-import rationale as `source_connectors` above --
# `assemble_pipelines` is imported inside the handler, not at module level.
@router.get("/{name}/ingest-config", dependencies=[Depends(require_action(Action.READ))])
def ingest_config(
    name: str,
    k8s: K8sService = Depends(get_k8s),
    trino: TrinoService = Depends(get_trino),
) -> Dict[str, Any]:
    """Filled-in collector/producer snippets + the producer credential for a
    kafka-ingest (existing-Kafka) source, so a customer team can wire their
    own log shipper (Fluent Bit / Vector / Logstash / any generic producer)
    straight from the Console instead of hand-assembling bootstrap/topic/
    SASL config.

    `disposition`/`authoritative_fqn` are NOT read off the connector CR --
    the kafka-ingest connector config carries no identifier/PK (that lives in
    the Iceberg table, not `spec.config`; `_iceberg_sink_config` only sets a
    `transforms.setdel` chain, which can't tell entity-without-delete_field
    apart from event). Instead this reuses `assemble_pipelines` (the same
    topology builder `/api/pipelines` and `source_connectors` above use),
    which already resolves both fields authoritatively by checking whether
    the Silver table exists.

    For the same reason, `pk`/`expected_json` are best-effort `None` -- the
    rendered snippet simply omits the key-field hint. This is a deliberate,
    documented degradation, not an oversight.
    """
    from app.services.pipeline_topology import assemble_pipelines

    cr = _resolve_kafka_ingest_cr(k8s, name)
    topic = _kafka_ingest_topic(cr)
    if topic is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="ingest-config is only available for kafka-ingest sources",
        )

    sources = k8s.list_sources()
    namespaces = {
        ns: set(trino.list_tables("lakehouse", ns)) for ns in trino.list_namespaces("lakehouse")
    }
    pipelines = assemble_pipelines(sources, namespaces, k8s.get_status, k8s.get_spark_status)
    pipe = next(
        (p for p in pipelines
         if p.get("name") == name
         or any(n.get("name") == name for n in p.get("nodes", []))),
        None,
    )
    disposition = "event"
    authoritative_fqn: Optional[str] = None
    if pipe and "error" not in pipe:
        disposition = pipe.get("disposition") or "event"
        authoritative_fqn = (pipe.get("authoritative") or {}).get("fqn")

    # Producer username MUST match what `add_source`/`delete_source` use --
    # recovered from the CR's own round-trip annotation (Task 5), never
    # rebuilt from `name` (the composite CR name, not the bare source id).
    producer = _kafka_ingest_producer(cr) or ""
    secret = k8s.read_secret(producer) if producer else None
    password = (secret or {}).get("password")   # never logged

    snippets = render_service.render_ingest_snippets(
        bootstrap=settings.kafka_external_bootstrap,
        topic=topic,
        user=producer,
        password=password or "",
        disposition=disposition,
        pk=None,
    )

    return {
        "external_bootstrap": settings.kafka_external_bootstrap,
        "topic": topic,
        "disposition": disposition,
        "authoritative_fqn": authoritative_fqn,
        "producer": {
            "user": producer,
            "mechanism": "SCRAM-SHA-512",
            "password": password,
            "secret_ref": producer,
        },
        "expected_json": None,
        "snippets": snippets,
    }


# --------------------------------------------------------------------------
# Credential rotation -- upserts the source's credential Secret and restarts
# the connector so Kafka Connect (which owns the DirectoryConfigProvider
# reading this Secret) picks up the new value. The Console itself NEVER
# opens a DB/broker connection anywhere in this route: `k8s.create_secret`
# is an upsert (create, replace on 409 -- see its docstring), and
# `connect.restart_connector` only reloads Connect's own config. Works
# identically in both deploy modes -- the credential Secret is a
# Console-managed, non-GitOps object (like the kafka-ingest producer
# Secret/ACL in `delete_source`), never routed through GitWriter.
# --------------------------------------------------------------------------

@router.post("/{name}/credentials", dependencies=[Depends(require_action(Action.SOURCE_EDIT))])
def rotate_credentials(
    name: str,
    payload: RotateCredentialsRequest,
    k8s: K8sService = Depends(get_k8s),
    connect: ConnectService = Depends(get_connect),
) -> Dict[str, Any]:
    """Rotates the DB/broker password backing `name`'s connector.

    The credential Secret is named after the source id (`_source_of`'s
    `{_SPARK_ANN}source` annotation recovery), NOT the CR's own composite
    `metadata.name` -- the same identity `delete_source` resolves its
    GitWriter-facing `source` from. A CR missing that annotation (in-cluster
    source with no external creds, or a CR pre-dating the annotation) fails
    loud with 400 rather than guessing/deriving a Secret name that could
    silently write credentials nothing reads.

    The Secret write and the restart are two separate calls: if the restart
    fails (Connect unreachable, rebalance in progress, ...) the credentials
    are still durably rotated in the Secret -- reported as a partial success
    (`restarted: false` + a `note` telling the operator to restart manually)
    rather than a 500, since the operator-visible remediation (retry the
    restart) is the same either way and the rotation itself already
    succeeded. The password is never logged and never echoed back in the
    response."""
    cr = _find_source(k8s, name)
    source = _source_of(cr)
    if not source:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="no credential secret for this source (in-cluster / no external creds)",
        )
    k8s.create_secret(source, {"user": payload.user, "pass": payload.password})  # upsert
    restarted = True
    try:
        connect.restart_connector(name)  # reload creds via DirectoryConfigProvider
    except Exception:  # noqa: BLE001 -- secret already updated; report partial success
        restarted = False
    return {
        "ok": True,
        "name": name,
        "restarted": restarted,
        "note": None if restarted else "credentials updated; restart failed — restart the connector manually",
    }


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
    """Connector-edit note (gitops): the SPARK branch below already routes
    through `orchestrator.edit_spark_source`, which itself commits via
    GitWriter when `settings.deploy_mode == "gitops"` (Task 4). The
    non-spark (KafkaConnector) branch has no such routing yet -- a direct
    `k8s.patch_connector` in gitops mode would be silently reverted by
    ArgoCD selfHeal on its next sync (a misleading no-op), so it fails loud
    (409) instead until connector-edit gitops routing exists (follow-up)."""
    cr = _find_source(k8s, name)
    if _kind_of(cr) == "ScheduledSparkApplication":
        if payload.spec is None:
            raise HTTPException(status_code=400, detail="spark source edit requires `spec`")
        orchestrator.edit_spark_source(payload.spec)
    else:
        if settings.deploy_mode == "gitops":
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=(
                    "connector edit not supported in gitops mode -- edit the source in the "
                    "pipeline repo (git); the cluster is reconciled from git"
                ),
            )
        if payload.config is None:
            raise HTTPException(status_code=400, detail="connector edit requires `config`")
        k8s.patch_connector(name, payload.config)
    return {"ok": True, "name": name}


@router.post("/{name}/pause", dependencies=[Depends(require_action(Action.SOURCE_EDIT))])
def pause_source(
    name: str,
    k8s: K8sService = Depends(get_k8s),
) -> Dict[str, Any]:
    """Neither CR kind has gitops routing for pause -- a direct
    `k8s.set_paused`/`set_spark_suspended` write in gitops mode would be
    silently reverted by ArgoCD selfHeal on its next sync (a misleading
    no-op), so this fails loud (409) in gitops mode instead. The 409's
    `detail` carries a structured `remediation` recipe (see
    `_gitops_remediation`) so the frontend can render concrete "do this in
    the pipeline repo / ArgoCD" steps -- the Console still never writes to
    git itself here (no GitWriter/ArgoCD client involved)."""
    cr = _find_source(k8s, name)
    if settings.deploy_mode == "gitops":
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "message": "pause not supported from the Console in gitops mode",
                "remediation": _gitops_remediation(cr, "paused"),
            },
        )
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
    """See pause_source's docstring -- same gitops fail-loud guard, same
    structured `remediation` recipe in the 409 `detail`."""
    cr = _find_source(k8s, name)
    if settings.deploy_mode == "gitops":
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "message": "resume not supported from the Console in gitops mode",
                "remediation": _gitops_remediation(cr, "running"),
            },
        )
    if _kind_of(cr) == "ScheduledSparkApplication":
        k8s.set_spark_suspended(name, False)
    else:
        k8s.set_paused(name, False)
    return {"ok": True, "name": name, "paused": False}


@router.post("/{name}/stop", dependencies=[Depends(require_action(Action.SOURCE_EDIT))])
def stop_source(
    name: str,
    k8s: K8sService = Depends(get_k8s),
) -> Dict[str, Any]:
    """Full stop (distinct from pause -- see `set_state`'s
    "running"/"paused"/"stopped" lifecycle): the connector task is torn down
    entirely rather than paused-in-place. Same gitops fail-loud guard as
    `pause_source`/`resume_source` -- neither CR kind has gitops routing for
    stop, so a direct `k8s.set_state`/`set_spark_suspended` write in gitops
    mode would be silently reverted by ArgoCD selfHeal on its next sync (a
    misleading no-op); the 409's `detail` carries the same structured
    `remediation` recipe (`_gitops_remediation`) so the frontend can render
    concrete "do this in the pipeline repo / ArgoCD" steps -- the Console
    still never writes to git itself here."""
    cr = _find_source(k8s, name)
    if settings.deploy_mode == "gitops":
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "message": "stop not supported from the Console in gitops mode",
                "remediation": _gitops_remediation(cr, "stopped"),
            },
        )
    if _kind_of(cr) == "ScheduledSparkApplication":
        k8s.set_spark_suspended(name, True)
    else:
        k8s.set_state(name, "stopped")
    return {"ok": True, "name": name, "stopped": True}


@router.post("/{name}/start", dependencies=[Depends(require_action(Action.SOURCE_EDIT))])
def start_source(
    name: str,
    k8s: K8sService = Depends(get_k8s),
) -> Dict[str, Any]:
    """See `stop_source`'s docstring -- same gitops fail-loud guard, same
    structured `remediation` recipe in the 409 `detail`. Starts a source that
    was stopped (or created stopped via `create_stopped` -- see
    `AddSourceOrchestrator.add_source`)."""
    cr = _find_source(k8s, name)
    if settings.deploy_mode == "gitops":
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "message": "start not supported from the Console in gitops mode",
                "remediation": _gitops_remediation(cr, "running"),
            },
        )
    if _kind_of(cr) == "ScheduledSparkApplication":
        k8s.set_spark_suspended(name, False)
    else:
        k8s.set_state(name, "running")
    return {"ok": True, "name": name, "stopped": False}


@router.post("/{name}/enable-snapshots", dependencies=[Depends(require_action(Action.SOURCE_EDIT))])
def enable_snapshots(
    name: str,
    k8s: K8sService = Depends(get_k8s),
) -> Dict[str, Any]:
    """Retrofit an EXISTING CDC connector with the Kafka signal channel +
    notification sink config (snapshot-lifecycle) -- for a connector that
    predates Task 2's render-time signal/notification block, or one whose
    config drifted. Same gitops fail-loud guard as pause_source/resume_source
    (a direct `patch_connector` write in gitops mode would be silently
    reverted by ArgoCD selfHeal on its next sync): 409 with a structured
    `remediation` recipe instead.

    Only applies to CDC connectors (KafkaConnector CRs) -- a
    ScheduledSparkApplication (spark-batch lane) has no Debezium signal
    channel to enable, so that's a 400, not a gitops/direct branch.

    Deliberately omits `signal.data.collection` from the patch: the operator
    configures the signaling table/collection separately (see
    `render_signal_table_dml`); this route's job is only to turn the channel
    on, not to assert where signals are read from."""
    cr = _find_source(k8s, name)
    if _kind_of(cr) == "ScheduledSparkApplication":
        raise HTTPException(
            status_code=400,
            detail="enable-snapshots applies only to CDC connectors",
        )
    patch = render_service._signal_and_notification_config()
    if settings.deploy_mode == "gitops":
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "message": (
                    "enable-snapshots not supported from the Console in gitops mode -- "
                    "add the signal-channel config to the connector in the pipeline repo (git)"
                ),
                "remediation": _gitops_config_add_remediation(cr, patch),
            },
        )
    k8s.patch_connector(name, patch)
    return {"ok": True, "name": name, "snapshots_enabled": True}


# --------------------------------------------------------------------------
# Snapshot signal (snapshot-lifecycle) -- PRODUCES a Debezium
# execute-snapshot/stop-snapshot Kafka signal; the Console never opens a
# connection to the source DB itself. Both routes degrade never-500: a
# missing/unreachable connector, a config missing `topic.prefix`, or a failed
# produce all come back as HTTP 200 `{"ok": False, ...}`, never a 500.
# --------------------------------------------------------------------------

@router.post("/{name}/snapshot", dependencies=[Depends(require_action(Action.SOURCE_EDIT))])
def snapshot_source(
    name: str,
    payload: SnapshotRequest,
    connect: ConnectService = Depends(get_connect),
    producer: KafkaProducerService = Depends(get_kafka_producer),
) -> Dict[str, Any]:
    """Produces an `execute-snapshot` signal for an incremental or blocking
    (ad-hoc) snapshot of `payload.tables` (or, if omitted, the connector's own
    configured table(s)/collection(s)). An `incremental` snapshot needs a
    signaling table/collection configured on the connector
    (`signal.data.collection`) -- if that's missing, this returns a
    `needs_signal_table` recipe (via `render_service.render_signal_table_dml`)
    instead of producing a signal Debezium could never act on."""
    try:
        config = connect.connector_config(name)
    except Exception as exc:  # noqa: BLE001 -- degrade, never 500
        return {"ok": False, "name": name, "reason": f"could not read connector config: {exc}"}

    topic_prefix = config.get("topic.prefix")
    if not topic_prefix:
        return {"ok": False, "name": name, "reason": "connector config missing topic.prefix"}

    if payload.type == "incremental" and "signal.data.collection" not in config:
        return _needs_signal_table_response(config)

    data_collections = _snapshot_data_collections(payload.tables, config)
    value = {
        "type": "execute-snapshot",
        "data": {
            "data-collections": data_collections,
            "type": "INCREMENTAL" if payload.type == "incremental" else "BLOCKING",
        },
    }
    ok = producer.send(settings.debezium_signal_topic, topic_prefix, value)
    return {"ok": ok, "name": name, "type": payload.type, "data_collections": data_collections}


@router.post("/{name}/snapshot/stop", dependencies=[Depends(require_action(Action.SOURCE_EDIT))])
def snapshot_stop(
    name: str,
    payload: SnapshotRequest,
    connect: ConnectService = Depends(get_connect),
    producer: KafkaProducerService = Depends(get_kafka_producer),
) -> Dict[str, Any]:
    """Produces a `stop-snapshot` signal -- halts an in-progress incremental
    snapshot for `payload.tables` (or, if omitted, the connector's own
    configured table(s)/collection(s)). No signaling-table check here: a
    stop-snapshot signal is only meaningful once an incremental snapshot has
    already been armed via `/snapshot`, which is what enforces that
    prerequisite."""
    try:
        config = connect.connector_config(name)
    except Exception as exc:  # noqa: BLE001 -- degrade, never 500
        return {"ok": False, "name": name, "reason": f"could not read connector config: {exc}"}

    topic_prefix = config.get("topic.prefix")
    if not topic_prefix:
        return {"ok": False, "name": name, "reason": "connector config missing topic.prefix"}

    data_collections = _snapshot_data_collections(payload.tables, config)
    value = {"type": "stop-snapshot", "data": {"data-collections": data_collections}}
    ok = producer.send(settings.debezium_signal_topic, topic_prefix, value)
    return {"ok": ok, "name": name, "data_collections": data_collections}


_SNAPSHOT_NOTIFICATION_LIMIT = 200

_SNAPSHOT_KIND_MAP = {
    "started": "started",
    "in_progress": "in_progress",
    "table_scan_completed": "in_progress",
    "completed": "completed",
    "aborted": "aborted",
    "skipped": "skipped",
}


def _normalize_snapshot_kind(raw_type: Optional[str]) -> str:
    lowered = (raw_type or "").strip().lower()
    return _SNAPSHOT_KIND_MAP.get(lowered, lowered)


def _snapshot_progress_str(additional_data: Dict[str, Any]) -> str:
    scanned = additional_data.get("scanned_rows")
    total = additional_data.get("total_rows_scanned")
    if scanned is not None and total is not None:
        return f"{scanned}/{total}"
    return ""


@router.get("/{name}/snapshot-progress", dependencies=[Depends(require_action(Action.READ))])
def snapshot_progress(
    name: str,
    connect: ConnectService = Depends(get_connect),
    consumer: KafkaConsumerService = Depends(get_kafka_consumer),
) -> Dict[str, Any]:
    """Reads the last N records off `settings.debezium_notification_topic`
    (Debezium's sink-channel notifications -- see
    `render_service._signal_and_notification_config`) and returns this
    connector's Snapshot progress. Uses `KafkaConsumerService.read_last_values`
    (NOT `read_last`, which is DLQ-specific and truncates the value to
    ~512 chars -- a notification JSON payload can exceed that). Degrades to
    `{"notifications": []}` on ANY failure (Connect down, Kafka down,
    unparseable records) -- never 500."""
    try:
        try:
            config = connect.connector_config(name)
        except Exception:  # noqa: BLE001 -- connector missing / Connect down -> unknown prefix
            config = {}
        topic_prefix = config.get("topic.prefix")

        records = consumer.read_last_values(
            settings.debezium_notification_topic, _SNAPSHOT_NOTIFICATION_LIMIT
        )

        notifications: List[Dict[str, Any]] = []
        for record in records:
            try:
                payload = json.loads(record.get("value") or "")
            except Exception:  # noqa: BLE001 -- unparseable value -> skip
                continue
            if not isinstance(payload, dict):
                continue

            aggregate_type = str(payload.get("aggregate_type") or "")
            if "snapshot" not in aggregate_type.lower():
                continue

            additional_data = payload.get("additional_data") or {}
            connector_name = additional_data.get("connector_name")
            if connector_name is not None and connector_name not in (topic_prefix, name):
                continue

            notifications.append({
                "kind": _normalize_snapshot_kind(payload.get("type")),
                "table": (additional_data.get("data_collections")
                          or additional_data.get("current_collection_in_progress")),
                "progress": _snapshot_progress_str(additional_data),
                "ts": payload.get("timestamp") if payload.get("timestamp") is not None else record.get("ts"),
            })
        return {"notifications": notifications}
    except Exception:  # noqa: BLE001 -- degrade, never 500
        return {"notifications": []}


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
    orchestrator: AddSourceOrchestrator = Depends(get_orchestrator),
) -> Dict[str, Any]:
    """`pipeline_only` tears down only the KafkaConnector CR (data already
    landed in the lakehouse is untouched). `with_data` (admin-only, gated by
    `require_delete_mode`) additionally deletes the KafkaTopic and performs a
    per-pipeline data teardown (Sub-project B-v2): drops BOTH the Bronze
    changelog table (`rawlake.<ns>_raw.<table>`) and the Silver merge target
    (`lakehouse.<ns>.<table>`, IF EXISTS -- idempotent no-op for an event-lane
    source with no Silver table), and deletes BOTH the per-pipeline Bronze and
    Silver buckets (`render_service.bronze_bucket_name(ns)` /
    `silver_bucket_name(ns)`, also idempotent no-ops) -- the destructive,
    data-loss path. `<ns>`/`<table>` are recovered from the connector's own
    `transforms.route.static.value` config; `<ns>` here IS the unique
    per-pipeline namespace/bucket-suffix, not a shared group label.

    gitops mode (`settings.deploy_mode == "gitops"`): the direct
    connector/topic (or spark-job) teardown above is replaced by a git commit
    (`orchestrator.git_writer.remove_source`) that removes the source's
    manifests from the pipelines repo -- ArgoCD prunes the live CRs on sync,
    Console itself never calls `k8s.delete_connector`/`delete_topic`/
    `delete_spark_job` in this mode. `GitWriter.remove_source` keys off the
    BARE logical `spec.source` (the git directory name, e.g. "pgdemo") --
    NEVER the CR's `metadata.name` (a composite, e.g. "dbz-pgdemo-customers",
    via `render_service._k8s_name(prefix, source, table)`), so `spec.source`
    is recovered from the CR's own `{_SPARK_ANN}source` round-trip annotation
    (stamped on every rendered connector/sink/spark CR by render_service --
    see `render_service._connector`/`render_spark_job`). Missing annotation
    (e.g. a CR pre-dating this annotation, or applied by hand) -> fail loud
    (409), never a silent no-op that looks like a successful delete while
    leaving the source's manifests untouched in git.
    `with_data` (dropping the Iceberg table / emptying the bucket) is a
    data-plane action, not a GitOps-managed one, so it stays a separate,
    explicit destructive call regardless of deploy_mode -- it still runs here
    exactly as in direct mode, against the CR's round-tripped ns/table (the CR
    itself is expected to already be present in the cluster via ArgoCD's
    earlier sync from git).

    Existing-Kafka ACL revocation (Task 2): a stream/kafka source's
    add_source grants `connect` a scoped literal topic READ/DESCRIBE (see
    `AddSourceOrchestrator.add_source`'s "acl" step). That grant is a direct
    K8s API mutation on the `connect` KafkaUser, not a chart-templated/GitOps
    -managed resource, so -- like `with_data`'s drop_table/delete_bucket --
    it is revoked here directly regardless of `deploy_mode`/`mode`, via
    `_kafka_ingest_topic` recovering the source's topic straight off its own
    CR (best-effort: a CR that isn't a kafka-ingest connector, or is missing
    the expected topic config, is silently skipped -- never fails the
    delete).

    Producer KafkaUser teardown (Task 5): a kafka-ingest source's add_source
    also provisions a per-pipeline producer KafkaUser (`<source>-producer`,
    see `AddSourceOrchestrator.add_source`'s "producer" step). Its Strimzi
    identity/Secret are direct K8s API objects too (not GitOps-managed), so
    they are deleted here alongside the ACL revocation above -- same
    best-effort posture (`_kafka_ingest_producer` returns None, silently
    skipping, if the CR predates this feature and lacks the round-trip
    annotation the username is recovered from).
    """
    if settings.deploy_mode == "gitops":
        cr = _find_source(k8s, name)  # 404 before any teardown, consistent with GET
        source = _source_of(cr)
        if not source:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=(
                    f"cannot resolve logical source id for {name!r} -- CR is missing the "
                    f"'{_SPARK_ANN}source' annotation; refusing gitops delete (would silently "
                    "remove nothing from the pipelines repo)"
                ),
            )
        kafka_ingest_topic = _kafka_ingest_topic(cr)
        if kafka_ingest_topic:
            k8s.remove_user_acl("connect", _connect_topic_acl(kafka_ingest_topic))
            producer = render_service._k8s_name(source, "producer")
            k8s.delete_user(producer)
            k8s.delete_secret(producer)
        commit = orchestrator.git_writer.remove_source(source)
        result: Dict[str, Any] = {"ok": commit.committed, "name": name, "mode": mode, "ref": commit.ref}
        if mode != "with_data":
            return result
        if _kind_of(cr) == "ScheduledSparkApplication":
            ns, table = _spark_target(cr)
            fqn = f"rawlake.{ns}.{table}"
            bucket = render_service.bucket_name(ns)
            trino.drop_table(fqn)
            s3.empty_bucket(bucket)
            result.update(dropped_table=fqn, emptied_bucket=bucket, deleted_topic=None)
            return result
        ns, table = _target_ns_table(cr)   # ns is the pipeline name
        bronze_fqn = f"rawlake.{ns}{render_service.BRONZE_NAMESPACE_SUFFIX}.{table}"
        silver_fqn = f"lakehouse.{ns}.{table}"
        bronze_bucket = render_service.bronze_bucket_name(ns)
        silver_bucket = render_service.silver_bucket_name(ns)
        topic = _topic_from_config((cr.get("spec") or {}).get("config") or {})
        trino.drop_table(bronze_fqn)      # DROP TABLE IF EXISTS
        trino.drop_table(silver_fqn)      # no-op for event
        s3.delete_bucket(bronze_bucket)
        s3.delete_bucket(silver_bucket)   # no-op for event
        result.update(dropped_tables=[bronze_fqn, silver_fqn],
                      deleted_buckets=[bronze_bucket, silver_bucket], deleted_topic=topic)
        return result

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

    kafka_ingest_topic = _kafka_ingest_topic(cr)
    if kafka_ingest_topic:
        k8s.remove_user_acl("connect", _connect_topic_acl(kafka_ingest_topic))
        producer = _kafka_ingest_producer(cr)
        if producer:
            k8s.delete_user(producer)
            k8s.delete_secret(producer)

    k8s.delete_connector(name)
    if mode != "with_data":
        return {"ok": True, "name": name, "mode": mode}

    ns, table = _target_ns_table(cr)   # ns is the pipeline name
    bronze_fqn = f"rawlake.{ns}{render_service.BRONZE_NAMESPACE_SUFFIX}.{table}"
    silver_fqn = f"lakehouse.{ns}.{table}"
    bronze_bucket = render_service.bronze_bucket_name(ns)
    silver_bucket = render_service.silver_bucket_name(ns)
    topic = _topic_from_config((cr.get("spec") or {}).get("config") or {})
    if topic:
        k8s.delete_topic(_k8s_topic_name(topic))   # delete by CR name (Task 1 sanitized it)
    trino.drop_table(bronze_fqn)      # DROP TABLE IF EXISTS
    trino.drop_table(silver_fqn)      # no-op for event
    s3.delete_bucket(bronze_bucket)
    s3.delete_bucket(silver_bucket)   # no-op for event
    return {
        "ok": True,
        "name": name,
        "mode": mode,
        "dropped_tables": [bronze_fqn, silver_fqn],
        "deleted_buckets": [bronze_bucket, silver_bucket],
        "deleted_topic": topic,
    }
