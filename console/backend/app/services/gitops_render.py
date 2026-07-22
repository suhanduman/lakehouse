"""B-v2 GitOps — Console'un pipeline repo'suna PR olarak açtığı manifest SETini
ÜRETİR (doğrudan-apply/orchestrator B-v1 YERİNE deklaratif dosyalar).

Her kaynak = 6 manifest (medallion CDC: Console BOTH Bronze ve Silver'ı
pre-create eder), ArgoCD PreSync sırası:
  1) Bronze descriptor ConfigMap  (PreSync wave 0) — `<ns>_raw`, `layer:
     bronze`, identifier YOK, sabit CDC metadata kolonları eklenir
  2) Bronze pre-create Job        (PreSync wave 1) — Bronze changelog
     tablosunu (partitioned, no identifier, rawdata warehouse) yaratır
  3) Silver descriptor ConfigMap  (PreSync wave 0) — `<ns>`, `layer: silver`,
     şema + identifier (PK)
  4) Silver pre-create Job        (PreSync wave 1) — current-state tabloyu
     identifier field ile yaratır (images/iceberg-tools); fail-loud →
     uyuşmazsa connector sync olmaz
  5) KafkaTopic                   (render_service.render_kafka_topic)
  6) KafkaConnector                (render_service.render_connector)

Sink auto-create identifier koymaz → upsert append-only'e düşer; tablo(lar)
bu yüzden connector'dan önce (PreSync) yaratılmalı — hem Bronze hem
Silver, ikisi de connector'dan önce. Secret MATERYALİ PR'a ASLA girmez —
connector CR secret'a ada göre referans verir (secret out-of-band /
ExternalSecret ile sağlanır; mevcut secrets.mode modeli).
"""
from __future__ import annotations

from typing import Any, Dict, List

import yaml

from app.models import ColumnSpec, SourceSpec
from app.services import render_service
from app.services.iceberg_service import BRONZE_METADATA_COLS

# images/iceberg-tools — PreSync Job imajı. Prod'da <registry>/lakehouse/... ile
# override edilir (POC-DOĞRULA).
ICEBERG_TOOLS_IMAGE = (
    "image-registry.openshift-image-registry.svc:5000/lakehouse/iceberg-tools:latest"
)
NESSIE_URI = "http://nessie:19120/iceberg/"
S3_SECRET = "s3-credentials"
DEFAULT_NAMESPACE = "lakehouse"


def _cm_name(ice_namespace: str, target_table: str) -> str:
    return f"icetbl-{ice_namespace}-{target_table}".replace("_", "-")


def resolve_identifier(spec: SourceSpec) -> List[str]:
    """Iceberg pre-create identifier (PK). AddSourceOrchestrator._resolve_identifier
    ile aynı öncelik: spec.identifier → mongo `_id` → scheduled incrementing_col."""
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
        # sort_keys=False: layer/identifier/columns okunası sırada kalsın (PR diff).
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
    # render_service CR'ları namespace'i apply anında alır (metadata.namespace
    # yok); GitOps dosyaları kendi-yeterli olmalı → burada set edilir.
    manifest.setdefault("metadata", {})["namespace"] = namespace
    return manifest


def render_gitops_pipeline(
    spec: SourceSpec,
    columns: List[ColumnSpec],
    identifier: List[str] | None = None,
    namespace: str = DEFAULT_NAMESPACE,
) -> List[Dict[str, Any]]:
    """Bir kaynağın tam pipeline manifest SETi (dosya olarak yazılacak / PR'a
    girecek). identifier verilmezse resolve_identifier fallback'i denenir; şema
    (columns) veya identifier yoksa fail-loud (append-only bug'ı engellenir)."""
    ident = list(identifier) if identifier else resolve_identifier(spec)
    if not ident:
        raise ValueError(
            f"{spec.kind}+{spec.type}: identifier (PK) gerekli — spec.identifier verin "
            "(sink auto-create identifier koymaz → upsert append-only)"
        )
    if not columns:
        raise ValueError("columns (tablo şeması) gerekli — Console discovery ile doldurun")
    bronze_ns = f"{spec.target_ns}{render_service.BRONZE_NAMESPACE_SUFFIX}"
    return [
        # Bronze — changelog, no identifier, metadata cols (before Silver/connector).
        render_descriptor_configmap(
            spec, columns, [], namespace, ice_namespace=bronze_ns, layer="bronze"
        ),
        render_precreate_job(spec, namespace, ice_namespace=bronze_ns, layer="bronze"),
        # Silver — current-state, identifier (PK) (before connector).
        render_descriptor_configmap(
            spec, columns, ident, namespace, ice_namespace=spec.target_ns, layer="silver"
        ),
        render_precreate_job(spec, namespace, ice_namespace=spec.target_ns, layer="silver"),
        _with_namespace(render_service.render_kafka_topic(render_service.topic_name(spec)), namespace),
        _with_namespace(render_service.render_connector(spec), namespace),
    ]
