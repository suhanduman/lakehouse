"""AddSourceOrchestrator: sequential add-source pipeline (secret -> bucket ->
namespace -> topic -> connector -> verify) with reverse rollback on failure.

No live cluster/S3/Trino — the happy-path/rollback tests inject hand-rolled
fakes recording call order for k8s/s3/trino, using the *real*
app.services.render_service module (already unit-tested in
test_render_service.py) as the renderer. The idempotency test goes one layer
deeper and drives the real K8sService/S3Service/TrinoService wrappers over
hand-rolled low-level API fakes that raise 409/BucketAlreadyOwnedByYou on a
second create, to prove idempotency holds end-to-end through the
orchestrator, not just inside each service.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

import pytest
from botocore.exceptions import ClientError
from kubernetes.client.exceptions import ApiException

from app.models import ColumnSpec, SourceCredentials, SourceSpec
from app.orchestrator import AddSourceOrchestrator
from app.services import render_service
from app.services.k8s_service import K8sService
from app.services.s3_service import S3Service
from app.services.trino_service import TrinoService

CDC_MSSQL_SPEC = SourceSpec(
    source="mssql1",
    kind="cdc",
    type="mssql",
    db="school",
    table="dbo.students",
    target_ns="mssql_ogrenci",
    target_table="students",
    db_host="mssql1.internal",
    # F: CDC hedef tablosu identifier field ile pre-create edilir; mssql'de PK
    # fallback yok, açık verilir. columns Iceberg şemasını kurar.
    columns=[ColumnSpec(name="id", type="bigint"), ColumnSpec(name="name", type="varchar")],
    identifier=["id"],
)
CREDS = SourceCredentials(user="sa", password="s3cret")


# --------------------------------------------------------------------------
# Fakes (call-order-recording doubles for k8s_service/s3_service/trino_service)
# --------------------------------------------------------------------------

class FakeK8s:
    """Records every call; `fail_on` names a step whose k8s call should raise.

    `calls` is a single unified, ordered log of *every* mutating call
    ((op, arg) tuples) so tests can assert the exact interleaving of
    apply/create vs. rollback delete calls — in particular that undos run in
    reverse (connector -> topic -> secret). `status_states` is the sequence
    of connector `state` values get_status returns on successive polls
    (None => status not yet populated / pending); the last entry repeats if
    polled more times than the sequence length.
    """

    def __init__(self, fail_on: Optional[str] = None, status_states=None):
        self.fail_on = fail_on
        self.calls: List[tuple] = []
        self.created_secrets: List[Any] = []
        self.deleted_secrets: List[str] = []
        self.applied_topics: List[Dict[str, Any]] = []
        self.deleted_topics: List[str] = []
        self.applied_connectors: List[Dict[str, Any]] = []
        self.deleted_connectors: List[str] = []
        self.applied_spark_jobs: List[Dict[str, Any]] = []
        self.status_calls: List[str] = []
        self.status_states = list(status_states) if status_states is not None else ["RUNNING"]

    @staticmethod
    def _wrap(state):
        return {"connector": {"state": state}} if state is not None else {}

    def create_secret(self, name, data):
        if self.fail_on == "secret":
            raise RuntimeError("secret boom")
        self.calls.append(("create_secret", name))
        self.created_secrets.append((name, data))

    def delete_secret(self, name):
        self.calls.append(("delete_secret", name))
        self.deleted_secrets.append(name)

    def apply_topic(self, body):
        if self.fail_on == "topic":
            raise RuntimeError("topic boom")
        self.calls.append(("apply_topic", body["metadata"]["name"]))
        self.applied_topics.append(body)

    def delete_topic(self, name):
        self.calls.append(("delete_topic", name))
        self.deleted_topics.append(name)

    def apply_connector(self, body):
        if self.fail_on == "connector":
            raise RuntimeError("connector boom")
        self.calls.append(("apply_connector", body["metadata"]["name"]))
        self.applied_connectors.append(body)

    def delete_connector(self, name):
        self.calls.append(("delete_connector", name))
        self.deleted_connectors.append(name)

    def apply_spark_job(self, body):
        if self.fail_on == "spark-job":
            raise RuntimeError("spark-job boom")
        self.calls.append(("apply_spark_job", body["metadata"]["name"]))
        self.applied_spark_jobs.append(body)

    def get_status(self, name):
        if self.fail_on == "verify":
            raise RuntimeError("verify boom")
        idx = min(len(self.status_calls), len(self.status_states) - 1)
        self.status_calls.append(name)
        return self._wrap(self.status_states[idx])


class FakeS3:
    def __init__(self, fail_on: Optional[str] = None):
        self.fail_on = fail_on
        self.created_buckets: List[str] = []

    def create_bucket(self, name):
        if self.fail_on == "bucket":
            raise RuntimeError("bucket boom")
        self.created_buckets.append(name)
        return True


class FakeTrino:
    def __init__(self, fail_on: Optional[str] = None):
        self.fail_on = fail_on
        self.ddls: List[str] = []

    def run_ddl(self, sql):
        if self.fail_on == "namespace":
            raise RuntimeError("namespace boom")
        self.ddls.append(sql)


class FakeIcebergOrch:
    """IcebergService double for the orchestrator "table" pre-create step.
    `fail_on='table'` makes create_table raise (rollback testi). The "table"
    step calls create_table TWICE (bronze then silver) — `created` records
    BOTH calls, in order, as (namespace, table, identifier, layer)."""

    def __init__(self, fail_on: Optional[str] = None):
        self.fail_on = fail_on
        self.created: List[tuple] = []

    def create_table(self, namespace, table, columns, identifier, location=None, layer="silver"):
        if self.fail_on == "table":
            raise RuntimeError("table boom")
        self.created.append((namespace, table, tuple(identifier), layer))
        return f"{namespace}.{table}"

    @property
    def created_layers(self) -> List[str]:
        """Convenience view over `created` for lane/disposition tests that only
        care which layers were pre-created, not the full (ns, table, identifier,
        layer) tuple — `layer` was already captured above, this just projects it."""
        return [layer for (_ns, _table, _identifier, layer) in self.created]


def _orch(fail_on: Optional[str] = None, status_states=None, verify_attempts: int = 5) -> tuple:
    k8s = FakeK8s(fail_on=fail_on, status_states=status_states)
    s3 = FakeS3(fail_on=fail_on)
    trino = FakeTrino(fail_on=fail_on)
    # sleep is a no-op so the bounded-poll verify runs at zero delay in tests.
    # iceberg fake is attached to orch (orch.iceberg) — testler oradan okur.
    orch = AddSourceOrchestrator(
        k8s, s3, trino, render_service, iceberg=FakeIcebergOrch(fail_on=fail_on),
        verify_attempts=verify_attempts, verify_delay=0.0, sleep=lambda _: None,
    )
    return orch, k8s, s3, trino


def _orch_fakes(fail_on: Optional[str] = None, status_states=None, verify_attempts: int = 5) -> tuple:
    """Same as `_orch` but returns (orch, fakes-dict) — the shape the
    lane/disposition tests below want (`fakes["iceberg"]`, `fakes["k8s"]`, ...).
    No pytest fixture exists in this file (no conftest.py), so this is a plain
    helper, not an `orchestrator_fakes` fixture (adapting the brief's snippet,
    per the task instructions)."""
    orch, k8s, s3, trino = _orch(fail_on=fail_on, status_states=status_states, verify_attempts=verify_attempts)
    return orch, {"k8s": k8s, "s3": s3, "trino": trino, "iceberg": orch.iceberg}


# --------------------------------------------------------------------------
# Happy path — exact step order, all ok
# --------------------------------------------------------------------------

def test_add_source_happy_path_step_order_and_ok():
    orch, k8s, s3, trino = _orch()

    res = orch.add_source(CDC_MSSQL_SPEC, CREDS)

    assert [s.name for s in res.steps] == [
        "secret", "bucket", "namespace", "table", "topic", "connector", "verify",
    ]
    assert all(s.ok for s in res.steps)
    assert res.ok is True


def test_add_source_happy_path_calls_services_with_expected_data():
    orch, k8s, s3, trino = _orch()

    orch.add_source(CDC_MSSQL_SPEC, CREDS)

    assert k8s.created_secrets == [("mssql1", {"user": "sa", "pass": "s3cret"})]
    assert s3.created_buckets == [render_service.bucket_name("mssql_ogrenci")]
    assert trino.ddls == [
        render_service.render_namespace_ddl(
            "mssql_ogrenci", render_service.bucket_name("mssql_ogrenci")
        )
    ]
    expected_topic = render_service.topic_name(CDC_MSSQL_SPEC)
    assert [t["metadata"]["name"] for t in k8s.applied_topics] == [expected_topic]
    expected_connector = render_service.render_connector(CDC_MSSQL_SPEC)
    assert [c["metadata"]["name"] for c in k8s.applied_connectors] == [
        expected_connector["metadata"]["name"]
    ]
    assert k8s.status_calls == [expected_connector["metadata"]["name"]]
    # nothing rolled back on a successful run
    assert k8s.deleted_secrets == []
    assert k8s.deleted_topics == []
    assert k8s.deleted_connectors == []


# --------------------------------------------------------------------------
# Rollback on connector-step failure
# --------------------------------------------------------------------------

def test_rollback_on_connector_failure_deletes_topic_and_secret():
    orch, k8s, s3, trino = _orch(fail_on="connector")

    res = orch.add_source(CDC_MSSQL_SPEC, CREDS)

    assert res.ok is False
    assert [s.name for s in res.steps] == [
        "secret", "bucket", "namespace", "table", "topic", "connector",
    ]
    assert res.steps[-1].ok is False
    assert "connector boom" in res.steps[-1].detail

    # rolled back, in reverse: topic then secret
    assert k8s.deleted_topics == [render_service.topic_name(CDC_MSSQL_SPEC)]
    assert k8s.deleted_secrets == ["mssql1"]
    # the connector itself never successfully applied -> nothing to delete
    assert k8s.deleted_connectors == []

    # unified call log proves the exact interleaving + reverse undo order:
    # forward applies happen first, then undos in reverse (topic before secret).
    topic = render_service.topic_name(CDC_MSSQL_SPEC)
    assert k8s.calls == [
        ("create_secret", "mssql1"),
        ("apply_topic", topic),
        ("delete_topic", topic),
        ("delete_secret", "mssql1"),
    ]

    # bucket/namespace are deliberately NOT rolled back (see orchestrator
    # module docstring "Rollback scope") -- they're cheap+idempotent to
    # leave behind and may already hold state worth keeping.
    assert s3.created_buckets == [render_service.bucket_name("mssql_ogrenci")]
    assert len(trino.ddls) == 1


def test_rollback_deletes_topic_by_sanitized_cr_name_not_raw_name():
    # Regression guard: render_kafka_topic sanitizes metadata.name (RFC1123-safe
    # CR name, e.g. '_' -> '-') while the real Kafka topic keeps its raw form.
    # The topic step's rollback undo must delete the CR by that same sanitized
    # name -- deleting by the raw topic name would 404 against a real cluster
    # and leave the KafkaTopic CR orphaned after a failed-add rollback.
    spec = CDC_MSSQL_SPEC.model_copy(update={"table": "dbo.order_items", "target_table": "order_items"})
    raw_topic = render_service.topic_name(spec)
    sanitized_cr_name = render_service.render_kafka_topic(raw_topic)["metadata"]["name"]
    assert "_" in raw_topic
    assert sanitized_cr_name != raw_topic
    assert sanitized_cr_name.endswith("order-items")

    orch, k8s, s3, trino = _orch(fail_on="connector")

    res = orch.add_source(spec, CREDS)

    assert res.ok is False
    # topic step itself succeeded (applied under the sanitized CR name); the
    # LATER connector step is what fails and triggers rollback.
    assert [s.name for s in res.steps] == [
        "secret", "bucket", "namespace", "table", "topic", "connector",
    ]
    assert res.steps[-2].name == "topic" and res.steps[-2].ok is True

    assert k8s.deleted_topics == [sanitized_cr_name]


def test_rollback_on_verify_failed_state_deletes_connector_topic_secret():
    # get_status reports FAILED -> verify step fails -> full rollback.
    orch, k8s, s3, trino = _orch(status_states=["FAILED"])

    res = orch.add_source(CDC_MSSQL_SPEC, CREDS)

    assert res.ok is False
    assert [s.name for s in res.steps] == [
        "secret", "bucket", "namespace", "table", "topic", "connector", "verify",
    ]
    assert res.steps[-1].ok is False
    assert "FAILED" in res.steps[-1].detail

    connector = render_service.render_connector(CDC_MSSQL_SPEC)["metadata"]["name"]
    topic = render_service.topic_name(CDC_MSSQL_SPEC)
    assert k8s.deleted_connectors == [connector]
    assert k8s.deleted_topics == [topic]
    assert k8s.deleted_secrets == ["mssql1"]
    # reverse undo order asserted via the unified log tail: connector, topic, secret.
    assert k8s.calls[-3:] == [
        ("delete_connector", connector),
        ("delete_topic", topic),
        ("delete_secret", "mssql1"),
    ]


def test_rollback_on_get_status_raising_also_deletes_connector():
    orch, k8s, s3, trino = _orch(fail_on="verify")

    res = orch.add_source(CDC_MSSQL_SPEC, CREDS)

    assert res.ok is False
    connector = render_service.render_connector(CDC_MSSQL_SPEC)["metadata"]["name"]
    assert k8s.deleted_connectors == [connector]
    assert k8s.deleted_topics == [render_service.topic_name(CDC_MSSQL_SPEC)]
    assert k8s.deleted_secrets == ["mssql1"]


def test_rollback_on_secret_failure_has_nothing_to_undo():
    orch, k8s, s3, trino = _orch(fail_on="secret")

    res = orch.add_source(CDC_MSSQL_SPEC, CREDS)

    assert res.ok is False
    assert [s.name for s in res.steps] == ["secret"]
    assert k8s.deleted_secrets == []
    assert k8s.deleted_topics == []
    assert k8s.deleted_connectors == []
    assert s3.created_buckets == []


def test_rollback_on_bucket_failure_undoes_only_secret_no_bucket_undo():
    # bucket-step failure: only the secret (already applied) is rolled back;
    # the bucket step never pushed an undo (no-undo-for-bucket design), so
    # no s3 delete is attempted at all.
    orch, k8s, s3, trino = _orch(fail_on="bucket")

    res = orch.add_source(CDC_MSSQL_SPEC, CREDS)

    assert res.ok is False
    assert [s.name for s in res.steps] == ["secret", "bucket"]
    assert res.steps[-1].ok is False
    # secret rolled back
    assert k8s.deleted_secrets == ["mssql1"]
    assert k8s.calls == [
        ("create_secret", "mssql1"),
        ("delete_secret", "mssql1"),
    ]
    # bucket never created (it raised), and no bucket-undo exists to call
    assert s3.created_buckets == []
    # namespace/topic/connector never reached
    assert trino.ddls == []
    assert k8s.applied_topics == []
    assert k8s.applied_connectors == []


# --------------------------------------------------------------------------
# verify — bounded poll for RUNNING (spec §4 step 6)
# --------------------------------------------------------------------------

def test_verify_running_is_ok_no_rollback():
    orch, k8s, s3, trino = _orch(status_states=["RUNNING"])

    res = orch.add_source(CDC_MSSQL_SPEC, CREDS)

    assert res.ok is True
    verify = res.steps[-1]
    assert verify.name == "verify" and verify.ok is True
    assert verify.detail == ""
    assert k8s.deleted_connectors == []


def test_verify_pending_then_running_polls_until_ok():
    # status starts empty (pending) then becomes RUNNING -> ok, connector kept.
    orch, k8s, s3, trino = _orch(status_states=[None, None, "RUNNING"], verify_attempts=5)

    res = orch.add_source(CDC_MSSQL_SPEC, CREDS)

    assert res.ok is True
    assert res.steps[-1].ok is True
    assert res.steps[-1].detail == ""
    assert len(k8s.status_calls) == 3  # polled twice pending, third RUNNING
    assert k8s.deleted_connectors == []


def test_verify_still_pending_after_all_attempts_is_ok_with_detail_no_rollback():
    from app.orchestrator import VERIFY_PENDING_DETAIL

    # never reaches RUNNING/FAILED -> ok=True but flagged pending; do NOT
    # roll back a healthy-but-not-yet-reconciled connector.
    orch, k8s, s3, trino = _orch(status_states=[None], verify_attempts=3)

    res = orch.add_source(CDC_MSSQL_SPEC, CREDS)

    assert res.ok is True
    verify = res.steps[-1]
    assert verify.name == "verify" and verify.ok is True
    assert verify.detail == VERIFY_PENDING_DETAIL
    assert len(k8s.status_calls) == 3  # exhausted all attempts
    assert k8s.deleted_connectors == []
    assert k8s.deleted_topics == []
    assert k8s.deleted_secrets == []


# --------------------------------------------------------------------------
# scheduled+mongo — out of scope for this orchestrator, must not crash
# --------------------------------------------------------------------------

def test_scheduled_mongo_returns_clear_failure_without_crashing():
    orch, k8s, s3, trino = _orch()
    spec = SourceSpec(
        source="lms1",
        kind="scheduled",
        type="mongo",
        db="lms",
        table="enrollments",
        target_ns="lms_ogrenci",
        target_table="enrollments",
        cron="0 * * * *",
    )

    res = orch.add_source(spec, CREDS)

    assert res.ok is False
    assert len(res.steps) == 1
    assert res.steps[0].ok is False
    assert "Spark batch path" in res.steps[0].detail
    # no side effects attempted at all
    assert k8s.created_secrets == []
    assert s3.created_buckets == []


# --------------------------------------------------------------------------
# Idempotent create — full stack (real service wrappers) over low-level
# fakes that raise 409 / BucketAlreadyOwnedByYou on a second create.
# --------------------------------------------------------------------------

class _LowFakeCustomApi:
    def __init__(self):
        self._created = set()

    def create_namespaced_custom_object(self, group, version, namespace, plural, body):
        key = (plural, body["metadata"]["name"])
        if key in self._created:
            raise ApiException(status=409, reason="Conflict")
        self._created.add(key)
        return body

    def patch_namespaced_custom_object(self, group, version, namespace, plural, name, body):
        return body

    def delete_namespaced_custom_object(self, **kw):
        return {}

    def get_namespaced_custom_object(self, **kw):
        return {"status": {"connectorStatus": {"connector": {"state": "RUNNING"}}}}


class _LowFakeCoreApi:
    def __init__(self):
        self._secrets = {}

    def create_namespaced_secret(self, namespace, body):
        name = body["metadata"]["name"]
        if name in self._secrets:
            raise ApiException(status=409, reason="Conflict")
        self._secrets[name] = body
        return body

    def replace_namespaced_secret(self, name, namespace, body):
        self._secrets[name] = body
        return body

    def delete_namespaced_secret(self, name, namespace):
        return {}


class _LowFakeS3Client:
    def __init__(self):
        self._buckets = set()

    def create_bucket(self, Bucket):
        if Bucket in self._buckets:
            raise ClientError(
                {"Error": {"Code": "BucketAlreadyOwnedByYou", "Message": "mine"}},
                "CreateBucket",
            )
        self._buckets.add(Bucket)
        return {}


class _LowFakeCursor:
    def execute(self, sql):
        pass

    def fetchall(self):
        return []

    def close(self):
        pass


class _LowFakeTrinoConn:
    def cursor(self):
        return _LowFakeCursor()


def test_add_source_is_idempotent_across_two_full_runs():
    k8s = K8sService(custom_api=_LowFakeCustomApi(), core_api=_LowFakeCoreApi(), namespace="example")
    s3 = S3Service(client=_LowFakeS3Client())
    trino = TrinoService(conn_factory=lambda: _LowFakeTrinoConn())
    orch = AddSourceOrchestrator(k8s, s3, trino, render_service, iceberg=FakeIcebergOrch())

    first = orch.add_source(CDC_MSSQL_SPEC, CREDS)
    second = orch.add_source(CDC_MSSQL_SPEC, CREDS)

    assert first.ok is True
    assert second.ok is True
    assert [s.name for s in second.steps] == [
        "secret", "bucket", "namespace", "table", "topic", "connector", "verify",
    ]


# --------------------------------------------------------------------------
# Table pre-create step (identifier field ile, connector'dan önce)
# --------------------------------------------------------------------------

def test_table_step_precreates_with_identifier_before_connector():
    orch, k8s, s3, trino = _orch()
    res = orch.add_source(CDC_MSSQL_SPEC, CREDS)
    assert res.ok is True
    # BOTH layers pre-created — Bronze (<ns>_raw, no identifier) first, then
    # Silver (<ns>, identifier).
    assert orch.iceberg.created == [
        ("mssql_ogrenci_raw", "students", (), "bronze"),
        ("mssql_ogrenci", "students", ("id",), "silver"),
    ]
    # "table" adımı connector'dan (ve topic'ten) ÖNCE
    names = [s.name for s in res.steps]
    assert names.index("table") < names.index("topic") < names.index("connector")


def test_table_identifier_fallback_scheduled_incrementing_col():
    spec = SourceSpec(
        source="pg1", kind="scheduled", type="pg", db="d", table="t",
        target_ns="pg_ns", target_table="t", jdbc_url="jdbc:postgresql://h/d",
        incrementing_col="pk", columns=[ColumnSpec(name="pk", type="int")],
    )
    orch, *_ = _orch()
    res = orch.add_source(spec, CREDS)
    assert res.ok is True
    assert orch.iceberg.created == [
        ("pg_ns_raw", "t", (), "bronze"),
        ("pg_ns", "t", ("pk",), "silver"),
    ]


def test_table_identifier_fallback_mongo_id():
    spec = SourceSpec(
        source="mongo1", kind="cdc", type="mongo", db="d", table="c",
        target_ns="m_ns", target_table="c", mongo_uri="mongodb://h",
        columns=[ColumnSpec(name="_id", type="string")],
    )
    orch, *_ = _orch()
    res = orch.add_source(spec, CREDS)
    assert res.ok is True
    assert orch.iceberg.created == [
        ("m_ns_raw", "c", (), "bronze"),
        ("m_ns", "c", ("_id",), "silver"),
    ]


def test_table_step_fails_loud_without_columns():
    spec = CDC_MSSQL_SPEC.model_copy(update={"columns": None})
    orch, k8s, s3, trino = _orch()
    res = orch.add_source(spec, CREDS)
    assert res.ok is False
    assert res.steps[-1].name == "table" and res.steps[-1].ok is False
    # connector/topic hiç uygulanmadı; secret geri alındı
    assert k8s.applied_connectors == [] and k8s.applied_topics == []
    assert k8s.deleted_secrets == ["mssql1"]


def test_table_step_fails_loud_without_identifier_and_no_fallback():
    # cdc+mssql, identifier YOK ve fallback yok (incrementing_col yok) → fail
    spec = CDC_MSSQL_SPEC.model_copy(update={"identifier": None})
    orch, *_ = _orch()
    res = orch.add_source(spec, CREDS)
    assert res.ok is False
    assert res.steps[-1].name == "table" and res.steps[-1].ok is False


def test_table_step_entity_missing_requirements_creates_nothing_in_iceberg():
    # Regression guard for the Critical fix: an entity source (cdc+mssql) with
    # no columns/identifier must fail BEFORE any Iceberg write -- not create a
    # metadata-only Bronze that later silently diverges from Silver on retry
    # (create_table's bronze idempotency check compares identifier only, which
    # is always []==[], so it would never notice missing business columns).
    spec = CDC_MSSQL_SPEC.model_copy(update={"columns": None, "identifier": None})
    orch, fakes = _orch_fakes()
    res = orch.add_source(spec, CREDS)
    assert res.ok is False
    assert res.steps[-1].name == "table" and res.steps[-1].ok is False
    assert fakes["iceberg"].created_layers == []  # nothing created -- Bronze included
    assert fakes["k8s"].applied_connectors == [] and fakes["k8s"].applied_topics == []


def test_rollback_on_table_failure_undoes_only_secret():
    orch, k8s, s3, trino = _orch(fail_on="table")
    res = orch.add_source(CDC_MSSQL_SPEC, CREDS)
    assert res.ok is False
    assert [s.name for s in res.steps] == ["secret", "bucket", "namespace", "table"]
    # secret geri alındı; topic/connector hiç uygulanmadı (table connector'dan önce)
    assert k8s.deleted_secrets == ["mssql1"]
    assert k8s.applied_topics == [] and k8s.applied_connectors == []


# --------------------------------------------------------------------------
# Lane/disposition-aware orchestrator (Plan B1 Task 3): registry-driven
# spark-batch reject + secret/topic skip + entity-only Silver.
# --------------------------------------------------------------------------

def _kafka_spec(**over) -> SourceSpec:
    d = dict(
        source="k1", kind="stream", type="kafka", db="ext", table="orders-topic",
        target_ns="events", target_table="orders",
    )
    d.update(over)
    return SourceSpec(**d)


def test_kafka_event_skips_secret_topic_and_silver():
    orch, fakes = _orch_fakes()

    res = orch.add_source(_kafka_spec(), SourceCredentials(user="", password=""))

    assert res.ok is True
    names = [s.name for s in res.steps]
    assert "secret" not in names   # in-cluster: no creds -> no secret step
    assert "topic" not in names    # existing topic: descriptor.topic_key == "" -> none created
    assert names == ["bucket", "namespace", "table", "connector", "verify"]
    # Bronze only (event disposition) -- no Silver create_table call at all.
    assert fakes["iceberg"].created_layers == ["bronze"]


def test_kafka_external_creds_creates_secret():
    orch, fakes = _orch_fakes()

    res = orch.add_source(
        _kafka_spec(kafka_bootstrap="b:9093"), SourceCredentials(user="u", password="p")
    )

    assert res.ok is True
    assert "secret" in [s.name for s in res.steps]
    assert fakes["k8s"].created_secrets == [("k1", {"user": "u", "pass": "p"})]
    # still no topic (kafka-ingest consumes an existing topic) and still Bronze-only.
    assert "topic" not in [s.name for s in res.steps]
    assert fakes["iceberg"].created_layers == ["bronze"]


def test_spark_batch_rejected_via_registry_not_hardcoded_detail():
    orch, _fakes = _orch_fakes()
    spec = SourceSpec(
        source="m1", kind="scheduled", type="mongo", db="lms",
        table="enrollments", target_ns="depo", target_table="enr",
        cron="0 * * * *",
    )

    res = orch.add_source(spec, SourceCredentials(user="u", password="p"))

    assert res.ok is False
    assert res.steps[0].name == "validate"
    # registry-driven detail names the descriptor id, not a hard-coded string.
    assert "scheduled-mongo requires the Spark batch path" in res.steps[0].detail


def _s3_spec() -> SourceSpec:
    return SourceSpec(
        source="ds1", kind="batch", type="s3", db="-", table="-",
        target_ns="nyc", target_table="trips",
        s3_bucket="ham-veri", s3_prefix="raw/", file_format="parquet",
        cron="0 * * * *",
    )


def test_batch_s3_onboards_via_spark_job():
    # batch+s3 (registry-driven: spark-batch lane WITH a spark renderer,
    # render_key="s3-register") -> minimal spark-job-only pipeline, no
    # secret/bucket/namespace/table/topic/connector/verify at all.
    orch, fakes = _orch_fakes()

    res = orch.add_source(_s3_spec(), SourceCredentials(user="", password=""))

    assert res.ok is True
    assert [s.name for s in res.steps] == ["spark-job"]
    assert fakes["iceberg"].created_layers == []
    assert fakes["k8s"].applied_spark_jobs


def test_batch_s3_spark_job_failure_reports_failed_step_without_crashing():
    # spark-job step raises (e.g. apply_spark_job k8s call blows up) -> the
    # orchestrator's try/except must convert it to an ok=False StepResult
    # named "spark-job", not propagate the exception.
    orch, fakes = _orch_fakes(fail_on="spark-job")

    res = orch.add_source(_s3_spec(), SourceCredentials(user="", password=""))

    assert res.ok is False
    assert [s.name for s in res.steps] == ["spark-job"]
    assert res.steps[-1].ok is False
    assert "spark-job boom" in res.steps[-1].detail
    assert fakes["k8s"].applied_spark_jobs == []


def test_scheduled_mongo_still_rejected():
    # spark-batch lane WITHOUT a spark renderer (render_key == "") is still
    # rejected up front, exactly as before Plan B2.
    orch, _fakes = _orch_fakes()
    spec = SourceSpec(
        source="m1", kind="scheduled", type="mongo", db="lms",
        table="enr", target_ns="depo", target_table="enr", cron="0 * * * *",
    )

    res = orch.add_source(spec, SourceCredentials(user="u", password="p"))

    assert res.ok is False
    assert res.steps[0].name == "validate"


def test_cdc_mssql_still_creates_secret_topic_and_silver_unchanged():
    # Guard against regressing the entity/CDC/JDBC path while adding the
    # kafka-ingest skips above: secret+topic+Bronze+Silver must all still run.
    orch, fakes = _orch_fakes()

    res = orch.add_source(CDC_MSSQL_SPEC, CREDS)

    assert res.ok is True
    assert [s.name for s in res.steps] == [
        "secret", "bucket", "namespace", "table", "topic", "connector", "verify",
    ]
    assert fakes["iceberg"].created_layers == ["bronze", "silver"]


def _mqtt_spec(**over) -> SourceSpec:
    d = dict(
        source="m1", kind="stream", type="mqtt", db="-", table="-",
        target_ns="iot", target_table="sensors",
        mqtt_broker="tcp://mosquitto:1883", mqtt_topic="sensors/#",
    )
    d.update(over)
    return SourceSpec(**d)


def test_mqtt_event_creates_topic_and_bronze_no_secret_no_silver():
    orch, fakes = _orch_fakes()

    res = orch.add_source(_mqtt_spec(), SourceCredentials(user="", password=""))

    assert res.ok is True
    names = [s.name for s in res.steps]
    assert "secret" not in names   # no creds supplied -> no secret step
    # Camel source DOES create its own topic (topic_key set) -- unlike stream/kafka.
    # Camel lane also gets its dedicated Iceberg sink applied (Task 2).
    assert names == ["bucket", "namespace", "table", "topic", "connector", "sink", "verify"]
    assert fakes["iceberg"].created_layers == ["bronze"]   # event -> Bronze only, no Silver


def _http_spec(**over) -> SourceSpec:
    d = dict(
        source="h1", kind="stream", type="http", db="-", table="-",
        target_ns="ext", target_table="prices", http_url="https://api.x/p",
    )
    d.update(over)
    return SourceSpec(**d)


def test_edit_spark_source_rerenders_and_applies():
    orch, fakes = _orch_fakes()
    spec = SourceSpec(source="s1", kind="batch", type="s3", db="-", table="-",
        target_ns="ext", target_table="orders", s3_bucket="b2", s3_prefix="p2/",
        file_format="parquet", cron="5 * * * *")
    orch.edit_spark_source(spec)
    applied = fakes["k8s"].applied_spark_jobs[-1]
    assert applied["metadata"]["name"] == "s3-register-s1-orders"
    assert "--bucket" in applied["spec"]["template"]["arguments"]


def test_camel_http_applies_source_and_dedicated_sink():
    orch, fakes = _orch_fakes()
    spec = _http_spec()

    res = orch.add_source(spec, SourceCredentials(user="", password=""))

    assert res.ok is True
    names = [s.name for s in res.steps]
    assert "connector" in names and "sink" in names
    applied = [b["metadata"]["name"] for b in fakes["k8s"].applied_connectors]
    assert "http-h1-prices" in applied and "http-h1-prices-sink" in applied


def test_kafka_ingest_has_no_separate_sink_step():
    orch, fakes = _orch_fakes()

    res = orch.add_source(_kafka_spec(), SourceCredentials(user="", password=""))

    assert res.ok is True
    assert "sink" not in [s.name for s in res.steps]


def test_http_entity_creates_topic_bronze_and_silver():
    orch, fakes = _orch_fakes()
    spec = _http_spec(
        disposition="entity",   # stream-http allows event OR entity
        columns=[ColumnSpec(name="id", type="bigint"), ColumnSpec(name="price", type="double")],
        identifier=["id"],
    )

    res = orch.add_source(spec, SourceCredentials(user="", password=""))

    assert res.ok is True
    names = [s.name for s in res.steps]
    # Camel lane also gets its dedicated Iceberg sink applied (Task 2).
    assert names == ["bucket", "namespace", "table", "topic", "connector", "sink", "verify"]
    assert fakes["iceberg"].created_layers == ["bronze", "silver"]   # entity -> Bronze + Silver
