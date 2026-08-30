"""B-v2 GitOps — PRODUCES the manifest SET the Console opens as a PR against
the pipeline repo (declarative files INSTEAD OF direct-apply/orchestrator B-v1).

`render_pipeline_fileset` produces a lane-aware manifest SET — it DELEGATES
the render_key/lane/disposition logic to render_service (never re-implements
it here), and adds the GitOps envelope (PreSync pre-create, namespace stamp,
file layout):
  - spark-batch (e.g. batch/s3)      → a single ScheduledSparkApplication
    (writes its own rawlake table itself via full-refresh CTAS; NO
    topic/connector/precreate)
  - cdc / scheduled / stream         → Bronze PreSync pre-create (ConfigMap +
    Job), and for entity disposition also Silver PreSync pre-create +
    (if any) KafkaTopic + KafkaConnector + (for a lane like Camel) the
    dedicated sink's own KafkaConnector, if there is one

Sink auto-create doesn't set an identifier → upsert degrades to append-only;
the table(s) must therefore be created before the connector (PreSync) — both
Bronze and Silver, both before the connector. Secret MATERIAL NEVER enters
the PR — the connector CR references the secret by name (the secret is
provisioned out-of-band / via ExternalSecret; the existing secrets.mode model).
"""
from __future__ import annotations

from typing import Any, Dict, List

import yaml

from app.models import ColumnSpec, SourceSpec
from app.services import render_service
from app.services.iceberg_service import BRONZE_METADATA_COLS

# images/iceberg-tools — the PreSync Job image. Overridden in prod with
# <registry>/lakehouse/... (VERIFY FOR POC).
ICEBERG_TOOLS_IMAGE = (
    "image-registry.openshift-image-registry.svc:5000/lakehouse/iceberg-tools:latest"
)
NESSIE_URI = "http://nessie:19120/iceberg/"
S3_SECRET = "s3-credentials"


def _cm_name(ice_namespace: str, target_table: str) -> str:
    return f"icetbl-{ice_namespace}-{target_table}".replace("_", "-")


def resolve_identifier(spec: SourceSpec) -> List[str]:
    """Identifier (PK) for the Iceberg pre-create step. Same priority as
    AddSourceOrchestrator._resolve_identifier: spec.identifier -> mongo `_id`
    -> scheduled incrementing_col."""
    if spec.identifier:
        return list(spec.identifier)
    if spec.type == "mongo":
        return ["_id"]
    if spec.incrementing_col:
        return [spec.incrementing_col]
    return []


def render_descriptor_configmap(
    spec: SourceSpec,
    columns: List[ColumnSpec],
    identifier: List[str],
    namespace: str,
    *,
    ice_namespace: str | None = None,
    layer: str = "silver",
) -> Dict[str, Any]:
    """`ice_namespace`/`layer` default to Silver (spec.target_ns, "silver") for
    backward compatibility. Bronze callers pass `<ns>_raw` + "bronze" — the
    fixed CDC metadata columns (BRONZE_METADATA_COLS) are appended for bronze
    only (mirrors IcebergService.create_table(layer="bronze"))."""
    ice_namespace = ice_namespace or spec.target_ns
    col_dicts = [c.model_dump() for c in columns]
    if layer == "bronze":
        existing_names = {c["name"] for c in col_dicts}
        col_dicts = col_dicts + [
            c for c in BRONZE_METADATA_COLS if c["name"] not in existing_names
        ]
    descriptor = {
        "namespace": ice_namespace,
        "table": spec.target_table,
        "layer": layer,
        "identifier": identifier,
        "columns": col_dicts,
    }
    return {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": _cm_name(ice_namespace, spec.target_table),
            "namespace": namespace,
            "annotations": {
                "argocd.argoproj.io/hook": "PreSync",
                "argocd.argoproj.io/sync-wave": "0",
                "argocd.argoproj.io/hook-delete-policy": "BeforeHookCreation",
            },
        },
        # sort_keys=False: keep layer/identifier/columns in a readable order (PR diff).
        "data": {"descriptor.yaml": yaml.safe_dump(descriptor, sort_keys=False)},
    }


def render_precreate_job(
    spec: SourceSpec, namespace: str, *, ice_namespace: str | None = None,
    layer: str = "silver",
) -> Dict[str, Any]:
    """`ice_namespace` defaults to Silver (spec.target_ns) for backward
    compatibility; Bronze callers pass `<ns>_raw` so the Job name + mounted
    ConfigMap name stay distinct from the Silver Job/ConfigMap.

    `layer="bronze"` also appends `--layer bronze` to the container args —
    belt-and-suspenders alongside the descriptor's own `layer: bronze` key
    (create_iceberg_table.py's --descriptor branch honors both), so the
    Bronze pre-create is correct even if only one side is read."""
    ice_namespace = ice_namespace or spec.target_ns
    args = ["--descriptor", "/config/descriptor.yaml"]
    if layer == "bronze":
        args += ["--layer", "bronze"]
    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": f"precreate-{ice_namespace}-{spec.target_table}".replace("_", "-"),
            "namespace": namespace,
            "annotations": {
                "argocd.argoproj.io/hook": "PreSync",
                "argocd.argoproj.io/sync-wave": "1",
                "argocd.argoproj.io/hook-delete-policy": "HookSucceeded",
            },
        },
        "spec": {
            "backoffLimit": 1,
            "template": {
                "spec": {
                    "restartPolicy": "Never",
                    "containers": [
                        {
                            "name": "precreate",
                            "image": ICEBERG_TOOLS_IMAGE,
                            "args": args,
                            "env": [
                                {"name": "NESSIE_URI", "value": NESSIE_URI},
                                {"name": "S3_ENDPOINT", "valueFrom": {"secretKeyRef": {"name": S3_SECRET, "key": "endpoint"}}},
                                {"name": "S3_ACCESS_KEY", "valueFrom": {"secretKeyRef": {"name": S3_SECRET, "key": "access-key-id"}}},
                                {"name": "S3_SECRET_KEY", "valueFrom": {"secretKeyRef": {"name": S3_SECRET, "key": "secret-access-key"}}},
                            ],
                            "volumeMounts": [
                                {"name": "descriptor", "mountPath": "/config", "readOnly": True}
                            ],
                        }
                    ],
                    "volumes": [
                        {
                            "name": "descriptor",
                            "configMap": {"name": _cm_name(ice_namespace, spec.target_table)},
                        }
                    ],
                }
            },
        },
    }


def _with_namespace(manifest: Dict[str, Any], namespace: str) -> Dict[str, Any]:
    # render_service's CRs pick up the namespace at apply time (no
    # metadata.namespace); GitOps files must be self-sufficient -> it's set
    # here instead.
    manifest.setdefault("metadata", {})["namespace"] = namespace
    return manifest


def render_pipeline_fileset(
    spec: SourceSpec, *, spark_image: str, s3_secret_name: str, namespace: str
) -> Dict[str, Dict[str, Any]]:
    """Full GitOps manifest set for a source as {relative_path: manifest}.
    DELEGATES to render_service (never re-implements lane/topic/disposition
    logic) so it stays current; adds the GitOps envelope (PreSync pre-create,
    namespace stamp, file layout). File layout: <source>/<NN>-<kind>-<name>.yaml."""
    from app import source_types

    files: Dict[str, Dict[str, Any]] = {}
    src = spec.source

    def _add(idx: int, kind: str, manifest: Dict[str, Any]) -> None:
        m = _with_namespace(manifest, namespace)
        name = m["metadata"]["name"]
        files[f"{src}/{idx:02d}-{kind}-{name}.yaml"] = m

    descriptor = source_types.get(spec.kind, spec.type)

    # spark-batch: the ScheduledSparkApplication owns writing its own rawlake
    # table (full-refresh CTAS) -- no topic/connector/precreate.
    if descriptor.lane == "spark-batch":
        _add(0, "scheduledsparkapplication",
             render_service.render_spark_job(spec, spark_image, s3_secret_name))
        return files

    # cdc / scheduled / stream: PreSync pre-create + topic + connector [+ sink].
    bronze_ns = f"{spec.target_ns}{render_service.BRONZE_NAMESPACE_SUFFIX}"
    _add(0, "configmap", render_descriptor_configmap(
        spec, spec.columns or [], [], namespace, ice_namespace=bronze_ns, layer="bronze"))
    _add(1, "job", render_precreate_job(
        spec, namespace, ice_namespace=bronze_ns, layer="bronze"))
    if spec.effective_disposition() == "entity":
        identifier = resolve_identifier(spec)
        if not identifier:
            raise ValueError(
                f"{spec.kind}+{spec.type}: identifier (PK) gerekli — sink auto-create "
                "identifier koymaz → upsert append-only'e düşer"
            )
        if not spec.columns:
            raise ValueError("spec.columns gerekli (Silver şeması)")
        _add(2, "configmap", render_descriptor_configmap(
            spec, spec.columns, identifier, namespace, layer="silver"))
        _add(3, "job", render_precreate_job(spec, namespace, layer="silver"))
    if descriptor.topic_key:
        _add(4, "kafkatopic",
             render_service.render_kafka_topic(render_service.topic_name(spec)))
    _add(5, "kafkaconnector", render_service.render_connector(spec))
    if render_service.has_dedicated_sink(spec):
        _add(6, "kafkaconnector", render_service.render_sink(spec))
    return files
