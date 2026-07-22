"""B-v2 GitOps manifest üretimi + Git-provider seam testleri (SCM-agnostik)."""
from __future__ import annotations

import yaml
import pytest

from app.models import ColumnSpec, SourceSpec
from app.services import gitops_render
from app.services.git_provider import (
    GithubGitProvider,
    GitlabGitProvider,
    LocalDirGitProvider,
    manifests_to_yaml,
)

PG_SPEC = SourceSpec(
    source="pgdemo", kind="cdc", type="pg", db="appdb", table="public.customers",
    target_ns="depo", target_table="customers", db_host="pgdemo",
    columns=[ColumnSpec(name="id", type="bigint"), ColumnSpec(name="name", type="varchar")],
    identifier=["id"],
)


def cm_name():
    return "icetbl-depo-customers"


def _pipeline(spec=PG_SPEC):
    return gitops_render.render_gitops_pipeline(spec, spec.columns, spec.identifier)


def test_pipeline_has_four_manifests_in_presync_order():
    # medallion CDC: 6 manifests — Bronze CM+Job, then Silver CM+Job, then
    # KafkaTopic+KafkaConnector (both table layers pre-created before the
    # connector).
    kinds = [m["kind"] for m in _pipeline()]
    assert kinds == ["ConfigMap", "Job", "ConfigMap", "Job", "KafkaTopic", "KafkaConnector"]
    assert all(m["metadata"]["namespace"] == "lakehouse" for m in _pipeline())


def test_pipeline_precreates_bronze_and_silver():
    manifests = _pipeline()
    kinds = [m["kind"] for m in manifests]
    assert kinds.count("ConfigMap") == 2      # bronze + silver descriptors
    assert kinds.count("Job") == 2            # bronze + silver pre-create jobs
    descs = [yaml.safe_load(m["data"]["descriptor.yaml"]) for m in manifests if m["kind"] == "ConfigMap"]
    by_ns = {d["namespace"]: d for d in descs}
    assert "depo_raw" in by_ns and "depo" in by_ns
    bronze = by_ns["depo_raw"]
    assert bronze["layer"] == "bronze" and bronze["identifier"] == []
    assert {"__op", "__ts_ms", "__deleted", "__lsn"} <= {c["name"] for c in bronze["columns"]}
    silver = by_ns["depo"]
    assert silver["layer"] == "silver" and silver["identifier"] == ["id"]
    assert "__op" not in {c["name"] for c in silver["columns"]}


def test_descriptor_configmap_presync_wave0_and_roundtrips():
    # index 2 = Silver ConfigMap (0=Bronze CM, 1=Bronze Job, 2=Silver CM, ...).
    cm = _pipeline()[2]
    ann = cm["metadata"]["annotations"]
    assert ann["argocd.argoproj.io/hook"] == "PreSync"
    assert ann["argocd.argoproj.io/sync-wave"] == "0"
    desc = yaml.safe_load(cm["data"]["descriptor.yaml"])
    assert desc["namespace"] == "depo"
    assert desc["table"] == "customers"
    assert desc["layer"] == "silver"
    assert desc["identifier"] == ["id"]
    assert {c["name"] for c in desc["columns"]} == {"id", "name"}


def test_precreate_job_presync_wave1_uses_tools_image_and_descriptor():
    # index 3 = Silver pre-create Job (0=Bronze CM, 1=Bronze Job, 2=Silver CM, 3=Silver Job).
    job = _pipeline()[3]
    ann = job["metadata"]["annotations"]
    assert ann["argocd.argoproj.io/hook"] == "PreSync"
    assert ann["argocd.argoproj.io/sync-wave"] == "1"
    c = job["spec"]["template"]["spec"]["containers"][0]
    assert "iceberg-tools" in c["image"]
    # Silver is the tool's default layer — descriptor-only args (no --layer flag).
    assert c["args"] == ["--descriptor", "/config/descriptor.yaml"]
    vol = job["spec"]["template"]["spec"]["volumes"][0]
    assert vol["configMap"]["name"] == cm_name()


def test_bronze_precreate_job_args_include_layer_bronze_flag():
    # Belt-and-suspenders (final review, Fix 1b): the Bronze pre-create Job's
    # container args must carry --layer bronze explicitly, not rely solely on
    # the mounted descriptor's `layer: bronze` key (create_iceberg_table.py's
    # --descriptor branch honors that key too, but this is the second guard).
    # index 1 = Bronze pre-create Job (0=Bronze CM, 1=Bronze Job, ...).
    job = _pipeline()[1]
    c = job["spec"]["template"]["spec"]["containers"][0]
    assert c["args"] == ["--descriptor", "/config/descriptor.yaml", "--layer", "bronze"]


def test_identifier_fallback_mongo_id():
    spec = SourceSpec(
        source="m", kind="cdc", type="mongo", db="d", table="c", target_ns="mns",
        target_table="c", mongo_uri="mongodb://h",
        columns=[ColumnSpec(name="_id", type="string")],
    )
    assert gitops_render.resolve_identifier(spec) == ["_id"]
    # index 2 = Silver ConfigMap — only Silver carries the resolved identifier.
    desc = yaml.safe_load(
        gitops_render.render_gitops_pipeline(spec, spec.columns)[2]["data"]["descriptor.yaml"]
    )
    assert desc["identifier"] == ["_id"]


def test_identifier_fallback_scheduled_incrementing_col():
    spec = SourceSpec(
        source="pg", kind="scheduled", type="pg", db="d", table="t", target_ns="pns",
        target_table="t", jdbc_url="jdbc:postgresql://h/d", incrementing_col="pk",
        columns=[ColumnSpec(name="pk", type="int")],
    )
    assert gitops_render.resolve_identifier(spec) == ["pk"]


def test_fail_loud_without_columns():
    spec = PG_SPEC.model_copy(update={"columns": []})
    with pytest.raises(ValueError):
        gitops_render.render_gitops_pipeline(spec, [], spec.identifier)


def test_fail_loud_without_identifier_and_no_fallback():
    spec = PG_SPEC.model_copy(update={"identifier": None})
    with pytest.raises(ValueError):
        gitops_render.render_gitops_pipeline(spec, spec.columns, None)


def test_local_git_provider_writes_multidoc_yaml(tmp_path):
    provider = LocalDirGitProvider(root=str(tmp_path))
    res = provider.open_pipeline_pr(
        source_name="pgdemo", manifests=_pipeline(), title="add pgdemo", body="..."
    )
    assert res["provider"] == "local" and res["manifests"] == 6
    docs = list(yaml.safe_load_all(open(res["path"])))
    assert [d["kind"] for d in docs] == [
        "ConfigMap", "Job", "ConfigMap", "Job", "KafkaTopic", "KafkaConnector",
    ]


def test_unconfigured_providers_raise():
    for p in (GitlabGitProvider(), GithubGitProvider()):
        with pytest.raises(NotImplementedError):
            p.open_pipeline_pr(source_name="x", manifests=[], title="t", body="b")


def test_manifests_to_yaml_is_multidoc():
    out = manifests_to_yaml(_pipeline())
    assert out.count("---") >= 3


def test_precreate_descriptor_targets_silver_not_bronze():
    # The Console pre-creates BOTH layers, so the pipeline carries a
    # `depo_raw` (Bronze) descriptor too — but the SILVER descriptor (the one
    # carrying the resolved identifier/PK) must still target the plain
    # `depo` namespace, never `depo_raw`.
    manifests = _pipeline()
    descs = [
        yaml.safe_load(m["data"]["descriptor.yaml"]) for m in manifests if m["kind"] == "ConfigMap"
    ]
    silver = next(d for d in descs if d["layer"] == "silver")
    assert silver["namespace"] == "depo"          # NOT depo_raw
    assert not silver["namespace"].endswith("_raw")
    bronze = next(d for d in descs if d["layer"] == "bronze")
    assert bronze["namespace"] == "depo_raw"      # Bronze DOES carry the suffix
