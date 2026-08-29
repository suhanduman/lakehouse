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

# Task 2 (existing-Kafka runtime-owned connect ACLs): the scoped literal
# topic READ/DESCRIBE the orchestrator grants `connect` for a stream/kafka
# source consuming `table="orders-topic"` (see _kafka_spec below).
CONNECT_TOPIC_ACL = {"resource": {"type": "topic", "name": "orders-topic", "patternType": "literal"},
                     "operations": ["Read", "Describe"]}


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

    `status_states_by_name` (optional) gives a PER-NAME state sequence,
    keyed by connector name — needed for Finding-2-style tests where the
    source and the dedicated sink must report DIFFERENT states (e.g. source
    RUNNING, sink FAILED). A name present in this dict polls its own
    independent sequence/index; any other name falls back to the shared
    `status_states` sequence (indexed by total `status_calls` length, as
    before).
    """

    def __init__(self, fail_on: Optional[str] = None, status_states=None,
                 status_states_by_name: Optional[Dict[str, List[Any]]] = None):
        self.fail_on = fail_on
        self.calls: List[tuple] = []
        self.created_secrets: List[Any] = []
        self.deleted_secrets: List[str] = []
        self.applied_topics: List[Dict[str, Any]] = []
        self.deleted_topics: List[str] = []
        self.applied_connectors: List[Dict[str, Any]] = []
        self.deleted_connectors: List[str] = []
        self.applied_spark_jobs: List[Dict[str, Any]] = []
        self.ensured_acls: List[tuple] = []
        self.removed_acls: List[tuple] = []
        self.applied_users: List[Dict[str, Any]] = []
        self.deleted_users: List[str] = []
        self.read_secrets: List[str] = []
        self.status_calls: List[str] = []
        self.status_states = list(status_states) if status_states is not None else ["RUNNING"]
        self.status_states_by_name = (
            {k: list(v) for k, v in status_states_by_name.items()} if status_states_by_name else {}
        )
        self._status_idx_by_name: Dict[str, int] = {}

    @staticmethod
    def _wrap(state):
        return {"connector": {"state": state}} if state is not None else {}

    def create_secret(self, name, data, labels=None):
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

    def ensure_user_acl(self, user, acl):
        if self.fail_on == "acl":
            raise RuntimeError("acl boom")
        self.calls.append(("ensure_user_acl", user))
        self.ensured_acls.append((user, acl))

    def remove_user_acl(self, user, acl):
        self.calls.append(("remove_user_acl", user))
        self.removed_acls.append((user, acl))

    def apply_user(self, body):
        if self.fail_on == "producer":
            raise RuntimeError("producer boom")
        self.calls.append(("apply_user", body["metadata"]["name"]))
        self.applied_users.append(body)

    def delete_user(self, name):
        self.calls.append(("delete_user", name))
        self.deleted_users.append(name)

    def read_secret(self, name):
        self.calls.append(("read_secret", name))
        self.read_secrets.append(name)
        return None

    def get_status(self, name):
        if self.fail_on == "verify":
            raise RuntimeError("verify boom")
        if name in self.status_states_by_name:
            states = self.status_states_by_name[name]
            idx = min(self._status_idx_by_name.get(name, 0), len(states) - 1)
            self._status_idx_by_name[name] = idx + 1
            self.status_calls.append(name)
            return self._wrap(states[idx])
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
    """IcebergService double for the orchestrator "uniqueness"/"table" steps.
    `fail_on='table'` makes create_table raise (rollback testi). The "table"
    step calls create_table TWICE (bronze then silver) — `created` records
    BOTH calls, in order, as (namespace, table, identifier, layer, location) --
    `location` is captured (B-v2 Task 4) so per-pipeline-bucket-location
    assertions are real, not just structural.

    `_tables_by_ns` backs `namespace_tables` (the uniqueness-check read) and is
    updated both by `seed_table` (pre-seeding a collision for the uniqueness
    test) and by `create_table` itself (so a second add_source run against the
    same pipeline sees its own previously-created table and does NOT
    self-collide -- mirrors the real IcebergService/Nessie behavior)."""

    def __init__(self, fail_on: Optional[str] = None):
        self.fail_on = fail_on
        self.created: List[tuple] = []
        # Task 6 (PK-only entity pre-create): the `columns` payload passed to
        # each create_table call, in the same order as `created` -- kept as a
        # SEPARATE list (rather than widening the `created` tuple) so the
        # many existing `orch.iceberg.created == [...]` exact-equality
        # assertions above/below stay untouched.
        self.created_columns: List[list] = []
        # Task 4: per-pipeline Silver tuning — record bucket_count/write_mode
        # kwargs passed to create_table, keyed by layer ("silver"/"bronze")
        self.create_table_kwargs: Dict[str, Dict[str, Any]] = {}
        self._tables_by_ns: Dict[str, set] = {}

    def seed_table(self, namespace: str, table: str) -> None:
        """Pre-seed `namespace` as already holding `table` -- for the
        uniqueness-collision test (no create_table call involved)."""
        self._tables_by_ns.setdefault(namespace, set()).add(table)

    def namespace_tables(self, namespace: str, layer: str = "bronze") -> set:
        return set(self._tables_by_ns.get(namespace, set()))

    def location_for(self, layer: str):
        """The `location=` passed to the create_table call for `layer`
        ("bronze"/"silver"), or None if that layer was never created."""
        for (_ns, _table, _identifier, l, location) in self.created:
            if l == layer:
                return location
        return None

    def create_table(self, namespace, table, columns, identifier, location=None, layer="silver", **kwargs):
        if self.fail_on == "table":
            raise RuntimeError("table boom")
        self.created.append((namespace, table, tuple(identifier), layer, location))
        self.created_columns.append(list(columns))
        # Task 4: record bucket_count/write_mode kwargs per layer
        if kwargs:
            self.create_table_kwargs[layer] = kwargs
        self._tables_by_ns.setdefault(namespace, set()).add(table)
        return f"{namespace}.{table}"

    @property
    def created_layers(self) -> List[str]:
        """Convenience view over `created` for lane/disposition tests that only
        care which layers were pre-created, not the full (ns, table, identifier,
        layer, location) tuple — `layer` was already captured above, this just
        projects it."""
        return [layer for (_ns, _table, _identifier, layer, _location) in self.created]


def _orch(fail_on: Optional[str] = None, status_states=None, verify_attempts: int = 5,
          status_states_by_name=None) -> tuple:
    k8s = FakeK8s(fail_on=fail_on, status_states=status_states, status_states_by_name=status_states_by_name)
    s3 = FakeS3(fail_on=fail_on)
    trino = FakeTrino(fail_on=fail_on)
    # sleep is a no-op so the bounded-poll verify runs at zero delay in tests.
    # iceberg fake is attached to orch (orch.iceberg) — testler oradan okur.
    orch = AddSourceOrchestrator(
        k8s, s3, trino, render_service, iceberg=FakeIcebergOrch(fail_on=fail_on),
        verify_attempts=verify_attempts, verify_delay=0.0, sleep=lambda _: None,
    )
    return orch, k8s, s3, trino


def _orch_fakes(fail_on: Optional[str] = None, status_states=None, verify_attempts: int = 5,
                status_states_by_name=None) -> tuple:
    """Same as `_orch` but returns (orch, fakes-dict) — the shape the
    lane/disposition tests below want (`fakes["iceberg"]`, `fakes["k8s"]`, ...).
    No pytest fixture exists in this file (no conftest.py), so this is a plain
    helper, not an `orchestrator_fakes` fixture (adapting the brief's snippet,
    per the task instructions)."""
    orch, k8s, s3, trino = _orch(fail_on=fail_on, status_states=status_states, verify_attempts=verify_attempts,
                                  status_states_by_name=status_states_by_name)
    return orch, {"k8s": k8s, "s3": s3, "trino": trino, "iceberg": orch.iceberg}


# --------------------------------------------------------------------------
# Happy path — exact step order, all ok
# --------------------------------------------------------------------------

def test_add_source_happy_path_step_order_and_ok():
    orch, k8s, s3, trino = _orch()

    res = orch.add_source(CDC_MSSQL_SPEC, CREDS)

    # CDC lanes now render a dedicated Iceberg sink (Task 2) -- the "sink"
    # step lights up between "connector" and "verify", exactly as it already
    # does for Camel lanes (see test_camel_http_applies_source_and_dedicated_sink).
    assert [s.name for s in res.steps] == [
        "secret", "uniqueness", "bucket", "namespace", "table", "topic", "connector", "sink", "verify",
    ]
    assert all(s.ok for s in res.steps)
    assert res.ok is True


def test_add_source_happy_path_calls_services_with_expected_data():
    orch, k8s, s3, trino = _orch()

    orch.add_source(CDC_MSSQL_SPEC, CREDS)

    p = "mssql_ogrenci"
    assert k8s.created_secrets == [("mssql1", {"user": "sa", "pass": "s3cret"})]
    # B-v2: per-pipeline Bronze+Silver buckets (entity disposition -> both).
    assert s3.created_buckets == [
        render_service.bronze_bucket_name(p), render_service.silver_bucket_name(p),
    ]
    assert trino.ddls == [
        render_service.render_namespace_ddl(p, render_service.silver_bucket_name(p))
    ]
    expected_topic = render_service.topic_name(CDC_MSSQL_SPEC)
    assert [t["metadata"]["name"] for t in k8s.applied_topics] == [expected_topic]
    # source connector AND its dedicated sink are both applied, source first.
    expected_connector = render_service.render_connector(CDC_MSSQL_SPEC)
    expected_sink = render_service.render_sink(CDC_MSSQL_SPEC)
    assert [c["metadata"]["name"] for c in k8s.applied_connectors] == [
        expected_connector["metadata"]["name"], expected_sink["metadata"]["name"],
    ]
    # verify polls both the source AND the dedicated sink (Finding 2: the sink
    # does the Bronze write, not the source).
    assert k8s.status_calls == [
        expected_connector["metadata"]["name"], expected_sink["metadata"]["name"],
    ]
    # nothing rolled back on a successful run
    assert k8s.deleted_secrets == []
    assert k8s.deleted_topics == []
    assert k8s.deleted_connectors == []


# --------------------------------------------------------------------------
# Task 4: Silver per-pipeline tuning — bucket_count + write_mode
# --------------------------------------------------------------------------

def test_silver_create_table_passes_tuning_params_when_set():
    """Entity spec with silver_bucket_count=8, silver_write_mode="merge-on-read"
    → the silver create_table receives bucket_count=8, write_mode="merge-on-read".
    """
    orch, k8s, s3, trino = _orch()
    spec = CDC_MSSQL_SPEC.model_copy(
        update={"silver_bucket_count": 8, "silver_write_mode": "merge-on-read"}
    )

    res = orch.add_source(spec, CREDS)

    assert res.ok is True
    # Verify the silver create_table call received the tuning kwargs
    assert orch.iceberg.create_table_kwargs.get("silver") == {
        "bucket_count": 8,
        "write_mode": "merge-on-read",
    }


def test_silver_create_table_defaults_when_tuning_params_unset():
    """Entity spec with unset silver_bucket_count/write_mode (None)
    → the silver create_table receives bucket_count=16 (settings default),
    write_mode="copy-on-write" (hardcoded default).
    """
    orch, k8s, s3, trino = _orch()
    # CDC_MSSQL_SPEC has silver_bucket_count=None, silver_write_mode=None by default
    res = orch.add_source(CDC_MSSQL_SPEC, CREDS)

    assert res.ok is True
    # Verify the silver create_table call received the defaults
    from app.config import settings as _settings
    assert orch.iceberg.create_table_kwargs.get("silver") == {
        "bucket_count": _settings.silver_default_bucket_count,
        "write_mode": "copy-on-write",
    }


def test_bronze_create_table_does_not_receive_silver_tuning_params():
    """Bronze create_table (entity path) must NOT receive bucket_count/write_mode
    — the tuning is Silver-only.
    """
    orch, k8s, s3, trino = _orch()
    spec = CDC_MSSQL_SPEC.model_copy(
        update={"silver_bucket_count": 8, "silver_write_mode": "merge-on-read"}
    )

    res = orch.add_source(spec, CREDS)

    assert res.ok is True
    # Bronze must not have these tuning kwargs
    assert "bronze" not in orch.iceberg.create_table_kwargs


# --------------------------------------------------------------------------
# create_stopped (snapshot-lifecycle Task 8, "define but don't start"): the
# connector CR is applied with spec.state:"stopped" so an operator can arm a
# snapshot signal before any data flows. Only the SOURCE connector body is
# affected, never the dedicated sink -- the sink should still come up running
# so it's ready to consume once the source is started.
# --------------------------------------------------------------------------

def test_create_stopped_true_applies_connector_with_stopped_state():
    orch, k8s, s3, trino = _orch()
    spec = CDC_MSSQL_SPEC.model_copy(update={"create_stopped": True})

    res = orch.add_source(spec, CREDS)

    assert res.ok is True
    source_body, sink_body = k8s.applied_connectors
    assert source_body["spec"]["state"] == "stopped"
    # the dedicated sink is unaffected -- no state forced on it.
    assert "state" not in sink_body["spec"]


def test_create_stopped_false_does_not_force_connector_state():
    orch, k8s, s3, trino = _orch()

    orch.add_source(CDC_MSSQL_SPEC, CREDS)   # create_stopped defaults to False

    source_body, sink_body = k8s.applied_connectors
    assert "state" not in source_body["spec"]
    assert "state" not in sink_body["spec"]


# --------------------------------------------------------------------------
# Rollback on connector-step failure
# --------------------------------------------------------------------------

def test_rollback_on_connector_failure_deletes_topic_and_secret():
    orch, k8s, s3, trino = _orch(fail_on="connector")

    res = orch.add_source(CDC_MSSQL_SPEC, CREDS)

    assert res.ok is False
    assert [s.name for s in res.steps] == [
        "secret", "uniqueness", "bucket", "namespace", "table", "topic", "connector",
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
    p = "mssql_ogrenci"
    assert s3.created_buckets == [
        render_service.bronze_bucket_name(p), render_service.silver_bucket_name(p),
    ]
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
        "secret", "uniqueness", "bucket", "namespace", "table", "topic", "connector",
    ]
    assert res.steps[-2].name == "topic" and res.steps[-2].ok is True

    assert k8s.deleted_topics == [sanitized_cr_name]


def test_rollback_on_verify_failed_state_deletes_connector_topic_secret():
    # get_status reports FAILED -> verify step fails -> full rollback. The
    # source is polled first and raises immediately (shared status_states),
    # so the sink's own state never matters here -- but the sink WAS applied
    # (the "sink" step ran and succeeded before "verify"), so its undo is on
    # the rollback stack too and must fire, same as a Camel lane (see
    # test_camel_sink_failed_state_fails_add_source_not_ok).
    orch, k8s, s3, trino = _orch(status_states=["FAILED"])

    res = orch.add_source(CDC_MSSQL_SPEC, CREDS)

    assert res.ok is False
    assert [s.name for s in res.steps] == [
        "secret", "uniqueness", "bucket", "namespace", "table", "topic", "connector", "sink", "verify",
    ]
    assert res.steps[-1].ok is False
    assert "FAILED" in res.steps[-1].detail

    connector = render_service.render_connector(CDC_MSSQL_SPEC)["metadata"]["name"]
    sink = render_service.render_sink(CDC_MSSQL_SPEC)["metadata"]["name"]
    topic = render_service.topic_name(CDC_MSSQL_SPEC)
    assert k8s.deleted_connectors == [sink, connector]
    assert k8s.deleted_topics == [topic]
    assert k8s.deleted_secrets == ["mssql1"]
    # reverse undo order asserted via the unified log tail: sink, connector, topic, secret.
    assert k8s.calls[-4:] == [
        ("delete_connector", sink),
        ("delete_connector", connector),
        ("delete_topic", topic),
        ("delete_secret", "mssql1"),
    ]


def test_rollback_on_get_status_raising_also_deletes_connector():
    orch, k8s, s3, trino = _orch(fail_on="verify")

    res = orch.add_source(CDC_MSSQL_SPEC, CREDS)

    assert res.ok is False
    connector = render_service.render_connector(CDC_MSSQL_SPEC)["metadata"]["name"]
    sink = render_service.render_sink(CDC_MSSQL_SPEC)["metadata"]["name"]
    # sink applied (before verify raised) -> its undo also fires, reverse order.
    assert k8s.deleted_connectors == [sink, connector]
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
    assert [s.name for s in res.steps] == ["secret", "uniqueness", "bucket"]
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
    # Dedicated sink also gets polled (verify checks source AND sink, per
    # Finding 2) -- status_states_by_name gives each its own sequence so both
    # polling loops are positively exercised, mirroring
    # test_camel_sink_failed_state_fails_add_source_not_ok's per-name control.
    connector = render_service.render_connector(CDC_MSSQL_SPEC)["metadata"]["name"]
    sink = render_service.render_sink(CDC_MSSQL_SPEC)["metadata"]["name"]
    orch, k8s, s3, trino = _orch(
        status_states_by_name={
            connector: [None, None, "RUNNING"],
            sink: [None, "RUNNING"],
        },
        verify_attempts=5,
    )

    res = orch.add_source(CDC_MSSQL_SPEC, CREDS)

    assert res.ok is True
    assert res.steps[-1].ok is True
    assert res.steps[-1].detail == ""
    assert k8s.status_calls.count(connector) == 3  # polled twice pending, third RUNNING
    assert k8s.status_calls.count(sink) == 2       # polled once pending, second RUNNING
    assert k8s.deleted_connectors == []


def test_verify_still_pending_after_all_attempts_is_ok_with_detail_no_rollback():
    from app.orchestrator import VERIFY_PENDING_DETAIL

    # never reaches RUNNING/FAILED -> ok=True but flagged pending; do NOT
    # roll back a healthy-but-not-yet-reconciled connector OR sink.
    orch, k8s, s3, trino = _orch(status_states=[None], verify_attempts=3)

    res = orch.add_source(CDC_MSSQL_SPEC, CREDS)

    assert res.ok is True
    verify = res.steps[-1]
    assert verify.name == "verify" and verify.ok is True
    assert verify.detail == VERIFY_PENDING_DETAIL
    # source exhausts its 3 attempts pending, then the dedicated sink is
    # polled too and also exhausts its 3 attempts pending -- 6 calls total.
    assert len(k8s.status_calls) == 6
    assert k8s.deleted_connectors == []
    assert k8s.deleted_topics == []
    assert k8s.deleted_secrets == []


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

    def read_namespaced_secret(self, name, namespace):
        # Metadata-only GET create_secret's 409 path uses to decide whether
        # to replace (console-labeled) or raise SecretNameConflict.
        body = self._secrets.get(name, {})
        labels = body.get("metadata", {}).get("labels", {})

        class _Meta:
            pass

        class _Secret:
            pass

        meta = _Meta()
        meta.labels = labels
        secret = _Secret()
        secret.metadata = meta
        return secret


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
        "secret", "uniqueness", "bucket", "namespace", "table", "topic", "connector", "sink", "verify",
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
    p = "mssql_ogrenci"
    assert orch.iceberg.created == [
        ("mssql_ogrenci_raw", "students", (), "bronze",
         f"s3://{render_service.bronze_bucket_name(p)}/warehouse"),
        ("mssql_ogrenci", "students", ("id",), "silver",
         f"s3://{render_service.silver_bucket_name(p)}/warehouse"),
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
    p = "pg_ns"
    assert orch.iceberg.created == [
        ("pg_ns_raw", "t", (), "bronze", f"s3://{render_service.bronze_bucket_name(p)}/warehouse"),
        ("pg_ns", "t", ("pk",), "silver", f"s3://{render_service.silver_bucket_name(p)}/warehouse"),
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
    p = "m_ns"
    assert orch.iceberg.created == [
        ("m_ns_raw", "c", (), "bronze", f"s3://{render_service.bronze_bucket_name(p)}/warehouse"),
        ("m_ns", "c", ("_id",), "silver", f"s3://{render_service.silver_bucket_name(p)}/warehouse"),
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
    assert [s.name for s in res.steps] == ["secret", "uniqueness", "bucket", "namespace", "table"]
    # secret geri alındı; topic/connector hiç uygulanmadı (table connector'dan önce)
    assert k8s.deleted_secrets == ["mssql1"]
    assert k8s.applied_topics == [] and k8s.applied_connectors == []


# --------------------------------------------------------------------------
# Lane/disposition-aware orchestrator (Plan B1 Task 3): registry-driven
# secret/topic skip + entity-only Silver.
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
    # Task 2: "acl" grants `connect` a scoped literal READ on the customer
    # topic (kafka-ingest lane only) -- runs before "connector". Task 5:
    # "producer" always runs for kafka-ingest (create_topic=False here ->
    # no "ingest-topic" step).
    assert names == ["uniqueness", "bucket", "namespace", "table", "acl", "producer", "connector", "verify"]
    assert ("connect", CONNECT_TOPIC_ACL) in fakes["k8s"].ensured_acls
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


# --------------------------------------------------------------------------
# Task 2 (existing-Kafka runtime-owned connect ACLs): the "acl" step grants
# `connect` a scoped literal topic READ/DESCRIBE for stream/kafka sources
# ONLY (descriptor.render_key == "kafka-ingest") -- CDC/JDBC/Camel topics
# live under our own prefixes and need no per-source grant.
# --------------------------------------------------------------------------

def test_stream_kafka_add_grants_connect_topic_acl():
    orch, k8s, s3, trino = _orch()

    res = orch.add_source(_kafka_spec(), SourceCredentials(user="", password=""))

    assert res.ok
    assert ("connect", CONNECT_TOPIC_ACL) in k8s.ensured_acls
    assert "acl" in [s.name for s in res.steps]


def test_cdc_source_does_not_grant_connect_topic_acl():
    orch, k8s, s3, trino = _orch()

    orch.add_source(CDC_MSSQL_SPEC, CREDS)

    assert k8s.ensured_acls == []


def test_cdc_mssql_still_creates_secret_topic_and_silver_unchanged():
    # Guard against regressing the entity/CDC/JDBC path while adding the
    # kafka-ingest skips above: secret+topic+Bronze+Silver must all still run.
    # CDC lanes now also get their dedicated Iceberg sink applied (Task 2),
    # same as Camel already does.
    orch, fakes = _orch_fakes()

    res = orch.add_source(CDC_MSSQL_SPEC, CREDS)

    assert res.ok is True
    assert [s.name for s in res.steps] == [
        "secret", "uniqueness", "bucket", "namespace", "table", "topic", "connector", "sink", "verify",
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
    assert names == ["uniqueness", "bucket", "namespace", "table", "topic", "connector", "sink", "verify"]
    assert fakes["iceberg"].created_layers == ["bronze"]   # event -> Bronze only, no Silver


def _http_spec(**over) -> SourceSpec:
    d = dict(
        source="h1", kind="stream", type="http", db="-", table="-",
        target_ns="ext", target_table="prices", http_url="https://api.x/p",
    )
    d.update(over)
    return SourceSpec(**d)


def test_camel_http_applies_source_and_dedicated_sink():
    orch, fakes = _orch_fakes()
    spec = _http_spec()

    res = orch.add_source(spec, SourceCredentials(user="", password=""))

    assert res.ok is True
    names = [s.name for s in res.steps]
    assert "connector" in names and "sink" in names
    applied = [b["metadata"]["name"] for b in fakes["k8s"].applied_connectors]
    assert "http-h1-prices" in applied and "http-h1-prices-sink" in applied


def test_camel_sink_failed_state_fails_add_source_not_ok():
    # Finding 2: for a Camel lane the dedicated SINK does the Bronze write,
    # not the source. If verify only polled the source, a source that reaches
    # RUNNING while its sink is stuck/FAILED would report ok=True with no data
    # ever landing. Here the source is RUNNING (default fallback sequence) but
    # the sink reports FAILED -- add_source must fail, not ok=True, and the
    # sink (+ source + topic) must be rolled back.
    sink_name = "http-h1-prices-sink"
    orch, fakes = _orch_fakes(status_states_by_name={sink_name: ["FAILED"]})

    res = orch.add_source(_http_spec(), SourceCredentials(user="", password=""))

    assert res.ok is False
    verify_step = res.steps[-1]
    assert verify_step.name == "verify" and verify_step.ok is False
    assert sink_name in verify_step.detail and "FAILED" in verify_step.detail
    # rollback: sink + source connector + topic all deleted (no creds -> no secret step).
    assert sink_name in fakes["k8s"].deleted_connectors
    assert "http-h1-prices" in fakes["k8s"].deleted_connectors
    assert fakes["k8s"].deleted_topics


def test_camel_lane_without_sink_stuck_behaves_as_before():
    # Sanity check for the "keep existing behavior for lanes without a sink"
    # requirement: a non-Camel lane (no dedicated sink -> ctx has no
    # "sink_name") with the source itself FAILED still fails exactly as
    # before this fix -- verify polls only the source.
    orch, k8s, s3, trino = _orch(status_states=["FAILED"])

    res = orch.add_source(CDC_MSSQL_SPEC, CREDS)

    assert res.ok is False
    assert res.steps[-1].name == "verify" and res.steps[-1].ok is False
    assert "FAILED" in res.steps[-1].detail


def test_kafka_ingest_has_no_separate_sink_step():
    orch, fakes = _orch_fakes()

    res = orch.add_source(_kafka_spec(), SourceCredentials(user="", password=""))

    assert res.ok is True
    assert "sink" not in [s.name for s in res.steps]


def test_kafka_ingest_returns_composite_connector_name():
    # Residual fix: add_source must surface the CREATED connector's composite
    # name (kafka-ingest-<source>-<target_table>) so the wizard can fetch
    # ingestion config unambiguously -- the bare source id ("k1") is ambiguous
    # when one source id owns multiple kafka-ingest target tables.
    orch, fakes = _orch_fakes()

    res = orch.add_source(_kafka_spec(), SourceCredentials(user="", password=""))

    assert res.ok is True
    assert res.connector_name == "kafka-ingest-k1-orders"


# --------------------------------------------------------------------------
# Task 5: kafka-ingest producer provisioning (optional topic pre-create +
# per-pipeline producer KafkaUser), with rollback.
# --------------------------------------------------------------------------

def test_kafka_ingest_provisions_producer_user():
    orch, fakes = _orch_fakes()

    res = orch.add_source(_kafka_spec(), SourceCredentials(user="", password=""))  # create_topic=False (default)

    assert res.ok is True
    assert "producer" in [s.name for s in res.steps]
    assert "ingest-topic" not in [s.name for s in res.steps]   # no topic pre-create requested
    applied = fakes["k8s"].applied_users[-1]
    assert applied["metadata"]["name"] == "k1-producer"
    assert applied["metadata"]["labels"]["strimzi.io/cluster"] == "kafka"   # settings.kafka_cluster_name default
    acls = applied["spec"]["authorization"]["acls"]
    ops = acls[0]["operations"]
    assert "Write" in ops
    # create_topic=False -> the producer may need to lazily create the topic itself.
    assert "Create" in ops


def test_kafka_ingest_creates_topic_only_when_requested():
    orch, fakes = _orch_fakes()

    res = orch.add_source(_kafka_spec(create_topic=True), SourceCredentials(user="", password=""))

    assert res.ok is True
    assert "ingest-topic" in [s.name for s in res.steps]
    assert fakes["k8s"].applied_topics
    assert fakes["k8s"].applied_topics[-1]["metadata"]["name"] == "orders-topic"
    # topic is pre-created here -> producer must NOT also get Create (least-privilege).
    ops = fakes["k8s"].applied_users[-1]["spec"]["authorization"]["acls"][0]["operations"]
    assert "Create" not in ops

    orch2, fakes2 = _orch_fakes()
    res2 = orch2.add_source(_kafka_spec(), SourceCredentials(user="", password=""))   # create_topic=False

    assert res2.ok is True
    assert "ingest-topic" not in [s.name for s in res2.steps]
    assert fakes2["k8s"].applied_topics == []


def test_kafka_ingest_rolls_back_producer_on_later_failure():
    # A step AFTER "producer" (here: "connector") raises -> full rollback must
    # undo the producer KafkaUser (and, since create_topic=True here, the
    # pre-created ingest topic) via delete_user/delete_topic.
    orch, fakes = _orch_fakes(fail_on="connector")

    res = orch.add_source(_kafka_spec(create_topic=True), SourceCredentials(user="", password=""))

    assert res.ok is False
    assert fakes["k8s"].deleted_users == ["k1-producer"]
    assert fakes["k8s"].deleted_topics == ["orders-topic"]


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
    assert names == ["uniqueness", "bucket", "namespace", "table", "topic", "connector", "sink", "verify"]
    assert fakes["iceberg"].created_layers == ["bronze", "silver"]   # entity -> Bronze + Silver


def test_stream_kafka_entity_precreates_bronze_and_silver():
    # Task 3 (entity/upsert-from-Kafka): a stream+kafka source with
    # disposition="entity" must pre-create BOTH Bronze and Silver, exactly
    # like CDC/JDBC and the Camel-lane entity test above. The orchestrator's
    # "table" step branches purely on `spec.effective_disposition()` (no
    # hardcoded kind=="stream"/type=="kafka" special-case) -- this locks in
    # that the generic entity branch already covers kafka-ingest, no
    # orchestrator change needed.
    orch, fakes = _orch_fakes()
    spec = _kafka_spec(
        disposition="entity",
        columns=[ColumnSpec(name="id", type="bigint"), ColumnSpec(name="name", type="varchar")],
        identifier=["id"],
    )

    res = orch.add_source(spec, SourceCredentials(user="", password=""))

    assert res.ok is True
    names = [s.name for s in res.steps]
    # kafka-ingest still has no topic step (consumes an existing topic) and
    # no dedicated sink (it IS the sink) -- disposition doesn't change that.
    # Task 5: "producer" always runs for kafka-ingest.
    assert names == ["uniqueness", "bucket", "namespace", "table", "acl", "producer", "connector", "verify"]
    assert fakes["iceberg"].created_layers == ["bronze", "silver"]   # entity -> Bronze + Silver


def test_entity_precreate_accepts_pk_only_columns():
    # Task 6: entity pre-create's guard (`if not spec.columns: raise` in
    # _precreate_table) only requires `spec.columns` to be non-empty -- it
    # does NOT require the full table schema. The wizard may send ONLY the
    # identifier/PK column(s) (columns == [pk column]); the rest of the
    # schema evolves at runtime via the Iceberg sink's evolve-schema/
    # auto-create (Bronze) and silver-merge._apply_reconcile's ALTER TABLE
    # ADD COLUMN (Silver). This mirrors
    # test_stream_kafka_entity_precreates_bronze_and_silver above but with
    # ONLY the PK column in `columns` -- no business columns at all -- to
    # lock in that PK-only already satisfies the guard, with no orchestrator
    # relaxation needed.
    orch, fakes = _orch_fakes()
    spec = _kafka_spec(
        disposition="entity",
        columns=[ColumnSpec(name="id", type="int")],   # PK-only: no business columns
        identifier=["id"],
    )

    res = orch.add_source(spec, SourceCredentials(user="", password=""))

    assert res.ok is True
    assert fakes["iceberg"].created_layers == ["bronze", "silver"]   # entity -> both, PK-only is enough
    # both create_table calls (Bronze then Silver) got ONLY the PK column --
    # no business columns were required to satisfy the guard.
    pk_only_cols = [{"name": "id", "type": "int", "required": False}]
    assert fakes["iceberg"].created_columns == [pk_only_cols, pk_only_cols]
    # ... and the Silver call's identifier is the PK column.
    assert fakes["iceberg"].created[-1][2] == ("id",)


# --------------------------------------------------------------------------
# B-v2 Task 4: uniqueness step + per-pipeline Bronze/Silver buckets + Nessie
# namespace-level locations (create_table's location mechanism itself is
# UNCHANGED -- see IcebergService.create_table docstring; only the orchestrator
# now passes per-pipeline bucket locations into it instead of location=None).
# --------------------------------------------------------------------------

def test_uniqueness_rejects_namespace_collision():
    orch, fakes = _orch_fakes()
    # pre-seed the Bronze namespace with a DIFFERENT table -- simulates two
    # distinct pipelines colliding on the same target_ns (would force them to
    # share a bucket).
    bronze_ns = f"{CDC_MSSQL_SPEC.target_ns}{render_service.BRONZE_NAMESPACE_SUFFIX}"
    fakes["iceberg"].seed_table(bronze_ns, "other_table")

    res = orch.add_source(CDC_MSSQL_SPEC, CREDS)

    assert res.ok is False
    assert any(s.name == "uniqueness" and not s.ok for s in res.steps)
    # fails BEFORE any bucket/namespace/table side effect
    assert fakes["s3"].created_buckets == []
    assert fakes["trino"].ddls == []
    assert fakes["iceberg"].created == []
    # only the secret (already applied) is rolled back
    assert fakes["k8s"].deleted_secrets == ["mssql1"]


def test_uniqueness_allows_resubmit_of_the_same_pipeline():
    # A pipeline re-adding ITSELF (the table it already pre-created) must NOT
    # be treated as a collision -- this is what makes add_source idempotent
    # across retries (see test_add_source_is_idempotent_across_two_full_runs).
    orch, fakes = _orch_fakes()
    bronze_ns = f"{CDC_MSSQL_SPEC.target_ns}{render_service.BRONZE_NAMESPACE_SUFFIX}"
    fakes["iceberg"].seed_table(bronze_ns, CDC_MSSQL_SPEC.target_table)

    res = orch.add_source(CDC_MSSQL_SPEC, CREDS)

    assert res.ok is True
    assert all(s.ok for s in res.steps)


def test_entity_add_creates_two_buckets_and_per_pipeline_locations():
    orch, fakes = _orch_fakes()
    p = CDC_MSSQL_SPEC.target_ns

    res = orch.add_source(CDC_MSSQL_SPEC, CREDS)

    assert res.ok is True
    assert render_service.bronze_bucket_name(p) in fakes["s3"].created_buckets
    assert render_service.silver_bucket_name(p) in fakes["s3"].created_buckets
    # Bronze table created with the bronze-bucket location (moves it off ham-veri)
    assert fakes["iceberg"].location_for("bronze") == f"s3://{render_service.bronze_bucket_name(p)}/warehouse"
    assert fakes["iceberg"].location_for("silver") == f"s3://{render_service.silver_bucket_name(p)}/warehouse"
    # Silver namespace DDL used the silver bucket
    assert any(
        f"s3://{render_service.silver_bucket_name(p)}/warehouse" in ddl for ddl in fakes["trino"].ddls
    )


def test_event_add_only_bronze_bucket_and_no_silver_ns():
    orch, fakes = _orch_fakes()
    spec = _kafka_spec()
    p = spec.target_ns

    res = orch.add_source(spec, SourceCredentials(user="", password=""))

    assert res.ok is True
    assert render_service.bronze_bucket_name(p) in fakes["s3"].created_buckets
    assert render_service.silver_bucket_name(p) not in fakes["s3"].created_buckets
    assert not fakes["trino"].ddls   # no Silver namespace DDL for event
    assert fakes["iceberg"].location_for("bronze") == f"s3://{render_service.bronze_bucket_name(p)}/warehouse"
    assert fakes["iceberg"].location_for("silver") is None   # no Silver table for event
