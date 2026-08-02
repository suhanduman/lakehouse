"""Deterministic per-pipeline topology assembly for GET /api/pipelines.

Pure/injectable: no k8s or service singletons inside -- the router injects
`K8sService.list_sources()`'s output, the Trino namespace/table inventory,
and the two status callables (`K8sService.get_status`/`get_spark_status`).
This makes `assemble_pipelines` fully unit-testable with plain fakes (see
`tests/test_pipeline_topology.py`), the same "pure builder, thin router"
split `render_service`/`app.routers.sources` already use.

Grouping key: the `lakehouse.solus.dev/source` annotation (`_SRC_ANN`,
`SPARK_ANNOTATION_PREFIX + "source"`) -- exactly the annotation
`app.routers.gitops`'s `/gitops/status` (`_SRC_ANN` there too) already groups
CRs by. A "pipeline" is every CR sharing one such annotation value.

CR classification within a group:
  - a `ScheduledSparkApplication` CR -> the batch lane (short chain, no
    Kafka/Iceberg-sink hop at all).
  - otherwise, the CR whose `spec.class` is the IcebergSinkConnector class
    is the dedicated sink; the other KafkaConnector (Debezium/Aiven-JDBC/
    Camel) is the source connector.
  - a kafka-ingest group has ONLY the sink CR (`_render_kafka_ingest`'s
    output) -- it reads a pre-existing topic, so there is no separate
    source-connector CR to pair it with.

Disposition/authoritative (per the plan): batch -> `rawlake.<ns>.<t>`;
else entity (if the Silver table `<table>` already exists in
`namespaces[ns]`) -> `lakehouse.<ns>.<t>`; else event -> `rawlake.<ns>_raw.<t>`.

Route-value quirk this module resolves (NOT a naive "prefer the sink" pick):
`transforms.route.static.value` lives on whichever CR actually carries the
route SMT -- the SOURCE connector for the CDC/JDBC family
(`_cdc_dedicated_sink_config`'s dedicated sink deliberately has NO route SMT,
it dynamic-routes off the `_target_table` field the source already stamped),
but the SINK for kafka-ingest/Camel (`_iceberg_sink_config` calls
`_route_transform` itself, since there is no upstream SMT chain to have
stamped it). `_route_cr` below picks whichever CR in the pair actually has
the key, rather than assuming one side always does.
"""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional, Set

from app.routers.sources import (
    _kafka_ingest_topic,
    _spark_target,
    _target_ns_table,
    _topic_from_config,
)
from app.services import render_service

_SINK_CLASS = "org.apache.iceberg.connect.IcebergSinkConnector"
_MERGE_JOB = "silver-merge"
_SRC_ANN = f"{render_service.SPARK_ANNOTATION_PREFIX}source"


def _safe_status(get_status: Callable[[str], Any], name: Optional[str]) -> str:
    """Wrap a `get_status`/`get_spark_status` call so ANY failure (bad name,
    unreachable API, unexpected shape) degrades to "unknown" rather than
    propagating -- status is a nice-to-have here (mirrors the existing
    SourceDetail "status is best-effort" tone elsewhere in this codebase),
    never allowed to turn the whole pipeline list into a 500.

    Handles both callables' return shapes transparently: `get_status`
    returns `{"connector": {"state": ...}}` (K8sService.get_status unwraps
    `.status.connectorStatus` already); `get_spark_status` returns
    `{"scheduleState": ...}` (K8sService.get_spark_status unwraps `.status`
    already)."""
    if not name:
        return "unknown"
    try:
        result = get_status(name)
    except Exception:
        return "unknown"
    if not isinstance(result, dict):
        return "unknown"
    connector = result.get("connector")
    if isinstance(connector, dict) and connector.get("state"):
        return str(connector["state"])
    schedule_state = result.get("scheduleState")
    if schedule_state:
        return str(schedule_state)
    return "unknown"


def _kind(cr: Optional[Dict[str, Any]]) -> Optional[str]:
    if cr is None:
        return None
    return cr.get("kind") or "KafkaConnector"


def _route_cr(sink: Optional[Dict[str, Any]],
               source_conn: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """Whichever of (sink, source_conn) actually carries
    `transforms.route.static.value` -- see module docstring for why this
    isn't a fixed "sink, else source_conn" preference."""
    for cr in (sink, source_conn):
        if cr is None:
            continue
        config = (cr.get("spec") or {}).get("config") or {}
        if config.get("transforms.route.static.value"):
            return cr
    raise ValueError(
        "cannot resolve target namespace/table: neither CR in this pipeline "
        "carries transforms.route.static.value"
    )


def _pipeline_topic(source_conn: Optional[Dict[str, Any]],
                     sink: Optional[Dict[str, Any]]) -> Optional[str]:
    """Best-effort topic recovery for the "topic" node. Prefers the source
    connector's own config (`_topic_from_config`'s topic.prefix-based
    patterns, falling back to a plain `topics` key for Camel sources, which
    have no topic.prefix at all). With no source connector (kafka-ingest),
    falls back to `_kafka_ingest_topic` (gated on the sink CR's own
    "kafka-ingest-" name prefix, matching `app.routers.sources`), then a
    plain `topics` key as a last resort."""
    if source_conn is not None:
        config = (source_conn.get("spec") or {}).get("config") or {}
        return _topic_from_config(config) or config.get("topics")
    if sink is not None:
        config = (sink.get("spec") or {}).get("config") or {}
        return _kafka_ingest_topic(sink) or config.get("topics")
    return None


def assemble_pipelines(
    sources: List[Dict[str, Any]],
    namespaces: Dict[str, Set[str]],
    get_status: Callable[[str], Dict[str, Any]],
    get_spark_status: Callable[[str], Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Group `sources` (KafkaConnector + ScheduledSparkApplication CR dicts,
    `K8sService.list_sources()`'s shape) by their `lakehouse.solus.dev/source`
    annotation and assemble one topology dict per group. Never raises: a
    per-pipeline parse failure is caught and reported as
    `{"name":..., "cr_kind":..., "error": str, "nodes": []}` instead of
    aborting the whole list."""
    groups: Dict[str, List[Dict[str, Any]]] = {}
    for cr in sources:
        ann = (cr.get("metadata") or {}).get("annotations") or {}
        key = ann.get(_SRC_ANN) or (cr.get("metadata") or {}).get("name")
        groups.setdefault(key, []).append(cr)
    return [
        _assemble_one(name, crs, namespaces, get_status, get_spark_status)
        for name, crs in groups.items()
    ]


def _assemble_one(
    name: str,
    crs: List[Dict[str, Any]],
    namespaces: Dict[str, Set[str]],
    get_status: Callable[[str], Dict[str, Any]],
    get_spark_status: Callable[[str], Dict[str, Any]],
) -> Dict[str, Any]:
    try:
        spark = next((c for c in crs if c.get("kind") == "ScheduledSparkApplication"), None)
        if spark is not None:
            return _batch_pipeline(name, spark, get_spark_status)

        sink = next((c for c in crs if ((c.get("spec") or {}).get("class")) == _SINK_CLASS), None)
        source_conn = next((c for c in crs if c is not sink), None)

        route_cr = _route_cr(sink, source_conn)
        ns, table = _target_ns_table(route_cr)
        silver_exists = table in (namespaces.get(ns) or set())
        disposition = "entity" if silver_exists else "event"

        nodes: List[Dict[str, Any]] = [{"type": "source", "name": name}]

        if source_conn is not None:
            conn_name = (source_conn.get("metadata") or {}).get("name")
            nodes.append({
                "type": "connector",
                "name": conn_name,
                "kind": (source_conn.get("spec") or {}).get("class"),
                "state": _safe_status(get_status, conn_name),
            })

        topic = _pipeline_topic(source_conn, sink)
        if topic:
            nodes.append({"type": "topic", "name": topic})

        if sink is not None:
            sink_name = (sink.get("metadata") or {}).get("name")
            nodes.append({
                "type": "sink",
                "name": sink_name,
                "state": _safe_status(get_status, sink_name),
            })

        bronze_fqn = f"rawlake.{ns}{render_service.BRONZE_NAMESPACE_SUFFIX}.{table}"
        nodes.append({"type": "bronze", "fqn": bronze_fqn})
        owned_tables = [bronze_fqn]
        owned_buckets = [render_service.bronze_bucket_name(ns)]

        if disposition == "entity":
            nodes.append({
                "type": "merge",
                "name": _MERGE_JOB,
                "state": _safe_status(get_spark_status, _MERGE_JOB),
            })
            silver_fqn = f"lakehouse.{ns}.{table}"
            nodes.append({"type": "silver", "fqn": silver_fqn})
            owned_tables.append(silver_fqn)
            owned_buckets.append(render_service.silver_bucket_name(ns))
            authoritative = {"fqn": silver_fqn, "layer": "silver"}
        else:
            authoritative = {"fqn": bronze_fqn, "layer": "bronze"}

        nodes.append({"type": "buckets", "buckets": list(owned_buckets)})

        return {
            "name": name,
            "cr_kind": _kind(source_conn or sink),
            "disposition": disposition,
            "authoritative": authoritative,
            "nodes": nodes,
            "owned_tables": owned_tables,
            "owned_buckets": owned_buckets,
        }
    except Exception as exc:
        return {
            "name": name,
            "cr_kind": (crs[0].get("kind") if crs else None),
            "error": str(exc),
            "nodes": [],
        }


def _batch_pipeline(
    name: str,
    spark_cr: Dict[str, Any],
    get_spark_status: Callable[[str], Dict[str, Any]],
) -> Dict[str, Any]:
    """batch lane: short chain, no Kafka/Iceberg-sink/merge/Silver hop at
    all -- ns/table via `_spark_target` (round-trip annotations, falling
    back to `--target rawlake.<ns>.<tbl>` in the spark arguments -- see
    `app.routers.sources._spark_target`'s docstring)."""
    ns, table = _spark_target(spark_cr)
    fqn = f"rawlake.{ns}.{table}"
    bucket = render_service.bucket_name(ns)
    spark_name = (spark_cr.get("metadata") or {}).get("name")
    nodes = [
        {"type": "source", "name": name},
        {
            "type": "connector",
            "name": spark_name,
            "kind": "ScheduledSparkApplication",
            "state": _safe_status(get_spark_status, spark_name),
        },
        {"type": "bronze", "fqn": fqn},
        {"type": "buckets", "buckets": [bucket]},
    ]
    return {
        "name": name,
        "cr_kind": "ScheduledSparkApplication",
        "disposition": "batch",
        "authoritative": {"fqn": fqn, "layer": "rawlake"},
        "nodes": nodes,
        "owned_tables": [fqn],
        "owned_buckets": [bucket],
    }
