"""AddSourceOrchestrator gitops routing (Task 4): in deploy_mode="gitops",
add_source/edit_spark_source commit to git via the injected GitWriter instead
of applying to the cluster. direct mode (the default) is untouched -- see
test_orchestrator.py / test_sources_router.py for that coverage."""
from __future__ import annotations

import app.orchestrator as orch_mod
import pytest
from app.models import ColumnSpec, SourceCredentials, SourceSpec
from app.orchestrator import AddSourceOrchestrator
from app.services.git_writer import CommitResult


@pytest.fixture
def cdc_entity_spec():
    # cdc/pg entity: columns + identifier (PK) -> Bronze+Silver medallion.
    return SourceSpec(
        source="pgdemo", kind="cdc", type="pg", db="appdb", table="public.customers",
        target_ns="depo", target_table="customers", db_host="pgdemo",
        columns=[ColumnSpec(name="id", type="bigint"), ColumnSpec(name="name", type="varchar")],
        identifier=["id"],
    )


class _FakeWriter:
    def __init__(self):
        self.calls = []

    def write_source(self, source, fileset):
        self.calls.append(("write", source, len(fileset)))
        return CommitResult(committed=True, ref="deadbeef", files=list(fileset))

    def remove_source(self, source):
        self.calls.append(("remove", source, 0))
        return CommitResult(committed=True, ref="cafef00d")


def test_gitops_add_source_commits_and_skips_direct_apply(monkeypatch, cdc_entity_spec):
    # stub the render so we don't need real render_service wiring here
    monkeypatch.setattr(
        orch_mod, "render_pipeline_fileset",
        lambda spec, **k: {f"{spec.source}/00-x-a.yaml": {"kind": "X", "metadata": {"name": "a"}}},
    )
    w = _FakeWriter()
    orch = AddSourceOrchestrator(
        k8s=object(), s3=object(), trino=object(), render=object(),
        iceberg=None, deploy_mode="gitops", git_writer=w,
        spark_image="img:1", s3_secret_name="s3-credentials",
    )
    res = orch.add_source(cdc_entity_spec, SourceCredentials(user="", password=""))
    assert res.ok
    assert w.calls[0][0] == "write"
    assert any("deadbeef" in s.detail for s in res.steps)


def test_gitops_edit_spark_source_commits_instead_of_applying(monkeypatch, cdc_entity_spec):
    monkeypatch.setattr(
        orch_mod, "render_pipeline_fileset",
        lambda spec, **k: {f"{spec.source}/00-x-a.yaml": {"kind": "X", "metadata": {"name": "a"}}},
    )
    w = _FakeWriter()
    orch = AddSourceOrchestrator(
        k8s=object(), s3=object(), trino=object(), render=object(),
        iceberg=None, deploy_mode="gitops", git_writer=w,
        spark_image="img:1", s3_secret_name="s3-credentials",
    )
    res = orch.edit_spark_source(cdc_entity_spec)
    assert res.ok
    assert w.calls[0][0] == "write"
    assert any("deadbeef" in s.detail for s in res.steps)
