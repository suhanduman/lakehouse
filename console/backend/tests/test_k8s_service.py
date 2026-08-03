"""K8sService: thin CR CRUD wrapper over CustomObjectsApi (KafkaConnector/
KafkaTopic, group kafka.strimzi.io/v1beta2) + CoreV1Api (Secret). No live
cluster — every test injects a hand-rolled fake recording the calls it
received, so the wrapper's argument-shaping is exercised without a real
kubernetes.client.
"""

from __future__ import annotations

import base64

import pytest
from kubernetes.client.exceptions import ApiException

from app.services.k8s_service import K8sService

NAMESPACE = "example"


# --------------------------------------------------------------------------
# Fakes
# --------------------------------------------------------------------------

class FakeCustom:
    """Records every call; create_namespaced_custom_object can be told to
    raise a 409 (already-exists) once, to exercise the apply->patch fallback.

    `connectors`/`sparks` seed the items returned by
    list_namespaced_custom_object, keyed on the `plural` arg (kafkaconnectors
    vs scheduledsparkapplications); `spark_crd_absent` makes the spark list
    call raise a 404 instead, to exercise list_sources' CRD-not-installed
    tolerance. `last_patch` records the most recent
    patch_namespaced_custom_object call for assertions.
    """

    def __init__(
        self,
        conflict_on_create: bool = False,
        connectors: list | None = None,
        sparks: list | None = None,
        spark_crd_absent: bool = False,
        conflict_on_patch_once: bool = False,
        not_found_on_delete: bool = False,
    ):
        self.conflict_on_create = conflict_on_create
        self.connectors = connectors
        self.sparks = sparks
        self.spark_crd_absent = spark_crd_absent
        # When True, delete_namespaced_custom_object raises a 404 instead of
        # succeeding -- exercises delete_user's 404-tolerance (a KafkaUser
        # that never existed / was already deleted).
        self.not_found_on_delete = not_found_on_delete
        # When True, the *first* patch_namespaced_custom_object call raises a
        # 409 (simulating a concurrent modification losing the optimistic-
        # concurrency race); every call after that succeeds normally. Used to
        # exercise ensure_user_acl/remove_user_acl's retry-once-on-409 path.
        self.conflict_on_patch_once = conflict_on_patch_once
        self._patch_conflict_used = False
        self.created = []
        self.patched = []
        self.patch_attempts = 0
        self.deleted = []
        self.list_calls = []
        self.get_calls = []
        self.last_patch = None
        self.list_response = {"items": [{"metadata": {"name": "dbz-x"}}]}
        self.get_response = {
            "status": {"connectorStatus": {"connector": {"state": "RUNNING"}}}
        }

    def create_namespaced_custom_object(self, **kw):
        if self.conflict_on_create:
            raise ApiException(status=409, reason="Conflict")
        self.created.append(kw)
        return kw["body"]

    def patch_namespaced_custom_object(self, **kw):
        self.patch_attempts += 1
        if self.conflict_on_patch_once and not self._patch_conflict_used:
            self._patch_conflict_used = True
            raise ApiException(status=409, reason="Conflict")
        self.patched.append(kw)
        self.last_patch = kw
        return kw["body"]

    def delete_namespaced_custom_object(self, **kw):
        if self.not_found_on_delete:
            raise ApiException(status=404, reason="NotFound")
        self.deleted.append(kw)
        return {}

    def list_namespaced_custom_object(self, **kw):
        plural = kw["plural"]
        if plural == "scheduledsparkapplications":
            if self.spark_crd_absent:
                raise ApiException(status=404, reason="NotFound")
            self.list_calls.append(kw)
            return {"items": self.sparks if self.sparks is not None else []}
        self.list_calls.append(kw)
        if self.connectors is not None:
            return {"items": self.connectors}
        return self.list_response

    def get_namespaced_custom_object(self, **kw):
        self.get_calls.append(kw)
        return self.get_response


class FakeCore:
    def __init__(
        self,
        conflict_on_create: bool = False,
        secret_data: dict | None = None,
        secret_not_found: bool = False,
        not_found_on_delete: bool = False,
    ):
        self.conflict_on_create = conflict_on_create
        self.created_secrets = []
        self.replaced_secrets = []
        self.deleted_secrets = []
        self.secret_data = secret_data
        self.secret_not_found = secret_not_found
        # When True, delete_namespaced_secret raises a 404 instead of
        # succeeding -- exercises delete_secret's 404-tolerance (a producer
        # Secret that never materialized / was already deleted).
        self.not_found_on_delete = not_found_on_delete

    def create_namespaced_secret(self, **kw):
        if self.conflict_on_create:
            raise ApiException(status=409, reason="Conflict")
        self.created_secrets.append(kw)
        return kw["body"]

    def replace_namespaced_secret(self, **kw):
        self.replaced_secrets.append(kw)
        return kw["body"]

    def delete_namespaced_secret(self, **kw):
        if self.not_found_on_delete:
            raise ApiException(status=404, reason="NotFound")
        self.deleted_secrets.append(kw)
        return {}

    def read_namespaced_secret(self, **kw):
        if self.secret_not_found:
            raise ApiException(status=404, reason="NotFound")

        class FakeSecret:
            def __init__(self, data):
                self.data = data

        return FakeSecret(self.secret_data or {})


# --------------------------------------------------------------------------
# apply_connector — create path
# --------------------------------------------------------------------------

def test_apply_connector_calls_custom_api():
    fake = FakeCustom()
    svc = K8sService(custom_api=fake, core_api=None, namespace=NAMESPACE)
    svc.apply_connector({"metadata": {"name": "dbz-x"}, "spec": {}})

    assert len(fake.created) == 1
    call = fake.created[0]
    assert call["plural"] == "kafkaconnectors"
    assert call["namespace"] == "example"
    assert call["group"] == "kafka.strimzi.io"
    assert call["version"] == "v1beta2"
    assert call["body"] == {"metadata": {"name": "dbz-x"}, "spec": {}}
    assert fake.patched == []


def test_apply_topic_calls_custom_api_with_topic_plural():
    fake = FakeCustom()
    svc = K8sService(custom_api=fake, core_api=None, namespace=NAMESPACE)
    svc.apply_topic({"metadata": {"name": "cdc.mssql1.dbo.students"}, "spec": {}})

    assert len(fake.created) == 1
    assert fake.created[0]["plural"] == "kafkatopics"
    assert fake.created[0]["namespace"] == "example"


# --------------------------------------------------------------------------
# apply_* — 409 -> patch fallback (idempotent apply)
# --------------------------------------------------------------------------

def test_apply_connector_falls_back_to_patch_on_409():
    fake = FakeCustom(conflict_on_create=True)
    svc = K8sService(custom_api=fake, core_api=None, namespace=NAMESPACE)
    body = {"metadata": {"name": "dbz-x"}, "spec": {"class": "Foo"}}

    svc.apply_connector(body)

    assert fake.created == []
    assert len(fake.patched) == 1
    patch_call = fake.patched[0]
    assert patch_call["name"] == "dbz-x"
    assert patch_call["plural"] == "kafkaconnectors"
    assert patch_call["namespace"] == "example"
    assert patch_call["body"] == body


def test_apply_topic_falls_back_to_patch_on_409():
    fake = FakeCustom(conflict_on_create=True)
    svc = K8sService(custom_api=fake, core_api=None, namespace=NAMESPACE)
    body = {"metadata": {"name": "cdc.mssql1.dbo.students"}, "spec": {}}

    svc.apply_topic(body)

    assert fake.created == []
    assert len(fake.patched) == 1
    assert fake.patched[0]["plural"] == "kafkatopics"
    assert fake.patched[0]["name"] == "cdc.mssql1.dbo.students"


def test_apply_connector_reraises_non_409_errors():
    class ExplodingCustom(FakeCustom):
        def create_namespaced_custom_object(self, **kw):
            raise ApiException(status=500, reason="Boom")

    svc = K8sService(custom_api=ExplodingCustom(), core_api=None, namespace=NAMESPACE)
    with pytest.raises(ApiException) as exc_info:
        svc.apply_connector({"metadata": {"name": "dbz-x"}, "spec": {}})
    assert exc_info.value.status == 500


# --------------------------------------------------------------------------
# delete_connector / delete_topic
# --------------------------------------------------------------------------

def test_delete_connector():
    fake = FakeCustom()
    svc = K8sService(custom_api=fake, core_api=None, namespace=NAMESPACE)
    svc.delete_connector("dbz-x")

    assert len(fake.deleted) == 1
    assert fake.deleted[0] == {
        "group": "kafka.strimzi.io",
        "version": "v1beta2",
        "namespace": "example",
        "plural": "kafkaconnectors",
        "name": "dbz-x",
    }


def test_delete_topic():
    fake = FakeCustom()
    svc = K8sService(custom_api=fake, core_api=None, namespace=NAMESPACE)
    svc.delete_topic("cdc.mssql1.dbo.students")

    assert len(fake.deleted) == 1
    assert fake.deleted[0]["plural"] == "kafkatopics"
    assert fake.deleted[0]["name"] == "cdc.mssql1.dbo.students"


# --------------------------------------------------------------------------
# apply_spark_job / delete_spark_job — ScheduledSparkApplication CR
# (group/version differ from the Strimzi ones used above)
# --------------------------------------------------------------------------

@pytest.fixture
def fake_k8s():
    fake = FakeCustom()
    svc = K8sService(custom_api=fake, core_api=None, namespace=NAMESPACE)
    return svc, fake


def test_apply_spark_job_uses_spark_group(fake_k8s):
    svc, api = fake_k8s
    svc.apply_spark_job({"metadata": {"name": "s3-register-ds1-trips"}, "spec": {}})
    call = api.created[-1]
    assert call["group"] == "sparkoperator.k8s.io"
    assert call["version"] == "v1beta2"
    assert call["plural"] == "scheduledsparkapplications"
    assert call["namespace"] == "example"
    assert call["body"] == {"metadata": {"name": "s3-register-ds1-trips"}, "spec": {}}


def test_apply_spark_job_falls_back_to_patch_on_409():
    fake = FakeCustom(conflict_on_create=True)
    svc = K8sService(custom_api=fake, core_api=None, namespace=NAMESPACE)
    body = {"metadata": {"name": "s3-register-ds1-trips"}, "spec": {}}

    svc.apply_spark_job(body)

    assert fake.created == []
    assert len(fake.patched) == 1
    patch_call = fake.patched[0]
    assert patch_call["group"] == "sparkoperator.k8s.io"
    assert patch_call["version"] == "v1beta2"
    assert patch_call["plural"] == "scheduledsparkapplications"
    assert patch_call["name"] == "s3-register-ds1-trips"
    assert patch_call["body"] == body


def test_delete_spark_job(fake_k8s):
    svc, api = fake_k8s
    svc.delete_spark_job("s3-register-ds1-trips")

    assert len(api.deleted) == 1
    assert api.deleted[0] == {
        "group": "sparkoperator.k8s.io",
        "version": "v1beta2",
        "namespace": "example",
        "plural": "scheduledsparkapplications",
        "name": "s3-register-ds1-trips",
    }


def test_delete_spark_job_tolerates_404():
    class NotFoundCustom(FakeCustom):
        def delete_namespaced_custom_object(self, **kw):
            raise ApiException(status=404, reason="NotFound")

    svc = K8sService(custom_api=NotFoundCustom(), core_api=None, namespace=NAMESPACE)
    assert svc.delete_spark_job("missing") is None


def test_delete_spark_job_reraises_non_404_errors():
    class ExplodingCustom(FakeCustom):
        def delete_namespaced_custom_object(self, **kw):
            raise ApiException(status=500, reason="Boom")

    svc = K8sService(custom_api=ExplodingCustom(), core_api=None, namespace=NAMESPACE)
    with pytest.raises(ApiException) as exc_info:
        svc.delete_spark_job("s3-register-ds1-trips")
    assert exc_info.value.status == 500


# --------------------------------------------------------------------------
# create_secret — base64 encodes values
# --------------------------------------------------------------------------

def test_create_secret_base64_encodes_data():
    fake_core = FakeCore()
    svc = K8sService(custom_api=None, core_api=fake_core, namespace=NAMESPACE)
    svc.create_secret("mssql1", {"user": "sa", "pass": "s3cret"})

    assert len(fake_core.created_secrets) == 1
    call = fake_core.created_secrets[0]
    assert call["namespace"] == "example"
    body = call["body"]
    assert body["metadata"]["name"] == "mssql1"
    assert body["metadata"]["namespace"] == "example"
    assert body["type"] == "Opaque"
    assert body["data"]["user"] == base64.b64encode(b"sa").decode("utf-8")
    assert body["data"]["pass"] == base64.b64encode(b"s3cret").decode("utf-8")


def test_create_secret_on_409_replaces_to_converge_data():
    fake_core = FakeCore(conflict_on_create=True)
    svc = K8sService(custom_api=None, core_api=fake_core, namespace=NAMESPACE)

    result = svc.create_secret("mssql1", {"user": "sa2", "pass": "rotated"})

    # create was attempted, 409'd, then replace converged the existing Secret
    assert fake_core.created_secrets == []
    assert len(fake_core.replaced_secrets) == 1
    replace_call = fake_core.replaced_secrets[0]
    assert replace_call["name"] == "mssql1"
    assert replace_call["namespace"] == "example"
    body = replace_call["body"]
    # the NEW (rotated) credentials are what get written, not silently dropped
    assert body["data"]["user"] == base64.b64encode(b"sa2").decode("utf-8")
    assert body["data"]["pass"] == base64.b64encode(b"rotated").decode("utf-8")
    assert result["data"]["pass"] == base64.b64encode(b"rotated").decode("utf-8")


def test_delete_secret():
    fake_core = FakeCore()
    svc = K8sService(custom_api=None, core_api=fake_core, namespace=NAMESPACE)

    svc.delete_secret("mssql1")

    assert fake_core.deleted_secrets == [{"name": "mssql1", "namespace": "example"}]


def test_delete_secret_swallows_404():
    # FINAL-REVIEW Fix 2: a kafka-ingest source predating per-pipeline
    # producer provisioning (or one caught in the window before Strimzi
    # materializes the Secret) has no producer Secret at all -- deleting it
    # must be a no-op, not a raised ApiException that would abort
    # `delete_source`'s teardown before `delete_connector` runs.
    fake_core = FakeCore(not_found_on_delete=True)
    svc = K8sService(custom_api=None, core_api=fake_core, namespace=NAMESPACE)

    result = svc.delete_secret("k1-producer")

    assert result is None
    assert fake_core.deleted_secrets == []


def test_delete_secret_reraises_non_404():
    class RaisingCore(FakeCore):
        def delete_namespaced_secret(self, **kw):
            raise ApiException(status=500, reason="Boom")

    svc = K8sService(custom_api=None, core_api=RaisingCore(), namespace=NAMESPACE)

    with pytest.raises(ApiException):
        svc.delete_secret("k1-producer")


# --------------------------------------------------------------------------
# patch_connector — edit flow
# --------------------------------------------------------------------------

def test_patch_connector_merges_config():
    fake = FakeCustom()
    svc = K8sService(custom_api=fake, core_api=None, namespace=NAMESPACE)
    svc.patch_connector("dbz-x", {"table.include.list": "dbo.students2"})

    assert len(fake.patched) == 1
    call = fake.patched[0]
    assert call["name"] == "dbz-x"
    assert call["plural"] == "kafkaconnectors"
    assert call["namespace"] == "example"
    assert call["body"] == {"spec": {"config": {"table.include.list": "dbo.students2"}}}


# --------------------------------------------------------------------------
# set_state — patch spec.state (running/paused/stopped lifecycle);
# set_paused delegates to it (Task 8).
# --------------------------------------------------------------------------

def test_set_state_running_patches_state():
    fake = FakeCustom()
    svc = K8sService(custom_api=fake, core_api=None, namespace=NAMESPACE)
    svc.set_state("dbz-x", "running")

    assert len(fake.patched) == 1
    assert fake.patched[0]["body"] == {"spec": {"state": "running"}}
    assert fake.patched[0]["name"] == "dbz-x"
    assert fake.patched[0]["plural"] == "kafkaconnectors"
    assert fake.patched[0]["group"] == "kafka.strimzi.io"
    assert fake.patched[0]["version"] == "v1beta2"
    assert fake.patched[0]["namespace"] == "example"


def test_set_state_paused_patches_state():
    fake = FakeCustom()
    svc = K8sService(custom_api=fake, core_api=None, namespace=NAMESPACE)
    svc.set_state("dbz-x", "paused")

    assert fake.patched[0]["body"] == {"spec": {"state": "paused"}}


def test_set_state_stopped_patches_state():
    fake = FakeCustom()
    svc = K8sService(custom_api=fake, core_api=None, namespace=NAMESPACE)
    svc.set_state("dbz-x", "stopped")

    assert fake.patched[0]["body"] == {"spec": {"state": "stopped"}}


def test_set_state_rejects_invalid_state():
    fake = FakeCustom()
    svc = K8sService(custom_api=fake, core_api=None, namespace=NAMESPACE)
    with pytest.raises(ValueError):
        svc.set_state("dbz-x", "bogus")
    assert fake.patched == []   # rejected before any API call


def test_set_paused_true_patches_state_paused():
    fake = FakeCustom()
    svc = K8sService(custom_api=fake, core_api=None, namespace=NAMESPACE)
    svc.set_paused("dbz-x", True)

    assert len(fake.patched) == 1
    assert fake.patched[0]["body"] == {"spec": {"state": "paused"}}
    assert fake.patched[0]["name"] == "dbz-x"
    assert fake.patched[0]["plural"] == "kafkaconnectors"


def test_set_paused_false_patches_state_running():
    fake = FakeCustom()
    svc = K8sService(custom_api=fake, core_api=None, namespace=NAMESPACE)
    svc.set_paused("dbz-x", False)

    assert fake.patched[0]["body"] == {"spec": {"state": "running"}}


# --------------------------------------------------------------------------
# list_sources
# --------------------------------------------------------------------------

def test_list_sources_returns_items():
    fake = FakeCustom()
    svc = K8sService(custom_api=fake, core_api=None, namespace=NAMESPACE)
    result = svc.list_sources()

    assert result == [{"metadata": {"name": "dbz-x"}, "kind": "KafkaConnector"}]
    assert fake.list_calls[0]["plural"] == "kafkaconnectors"
    assert fake.list_calls[0]["namespace"] == "example"


def test_list_sources_empty_when_no_items():
    fake = FakeCustom()
    fake.list_response = {}
    svc = K8sService(custom_api=fake, core_api=None, namespace=NAMESPACE)
    assert svc.list_sources() == []


def test_list_sources_merges_and_tags_both_kinds():
    fake = FakeCustom(connectors=[{"metadata": {"name": "dbz-a"}}],
                       sparks=[{"metadata": {"name": "s3-register-b"}}])
    items = K8sService(fake, FakeCore(), "ns").list_sources()
    kinds = {i["metadata"]["name"]: i["kind"] for i in items}
    assert kinds == {"dbz-a": "KafkaConnector", "s3-register-b": "ScheduledSparkApplication"}


def test_list_sources_tolerates_missing_spark_crd():
    fake = FakeCustom(connectors=[{"metadata": {"name": "dbz-a"}}], spark_crd_absent=True)
    assert [i["metadata"]["name"] for i in K8sService(fake, FakeCore(), "ns").list_sources()] == ["dbz-a"]


def test_set_spark_suspended_patches_suspend():
    fake = FakeCustom()
    K8sService(fake, FakeCore(), "ns").set_spark_suspended("s3-register-b", True)
    assert fake.last_patch["body"] == {"spec": {"suspend": True}}
    assert fake.last_patch["plural"] == "scheduledsparkapplications"


# --------------------------------------------------------------------------
# get_status
# --------------------------------------------------------------------------

def test_get_status_reads_connector_status():
    fake = FakeCustom()
    svc = K8sService(custom_api=fake, core_api=None, namespace=NAMESPACE)
    status = svc.get_status("dbz-x")

    assert status == {"connector": {"state": "RUNNING"}}
    assert len(fake.get_calls) == 1
    assert fake.get_calls[0]["name"] == "dbz-x"
    assert fake.get_calls[0]["plural"] == "kafkaconnectors"
    assert fake.get_calls[0]["namespace"] == "example"


def test_get_status_missing_status_returns_empty_dict():
    fake = FakeCustom()
    fake.get_response = {"metadata": {"name": "dbz-x"}}
    svc = K8sService(custom_api=fake, core_api=None, namespace=NAMESPACE)
    assert svc.get_status("dbz-x") == {}


# --------------------------------------------------------------------------
# get_application_status — ArgoCD Application CR .status (best-effort; None
# on 404 so gitops-status never becomes a hard dependency on ArgoCD being
# installed).
# --------------------------------------------------------------------------

class _FakeCustom:
    def __init__(self, obj=None, raise_status=None):
        self._obj = obj
        self._raise = raise_status

    def get_namespaced_custom_object(self, **kw):
        if self._raise:
            raise ApiException(status=self._raise)
        return self._obj


def test_get_application_status_returns_status():
    svc = K8sService.__new__(K8sService)
    svc.custom_api = _FakeCustom({"status": {"sync": {"status": "Synced"}}})
    assert svc.get_application_status("lakehouse-pipelines", "argocd") == {"sync": {"status": "Synced"}}


def test_get_application_status_404_returns_none():
    svc = K8sService.__new__(K8sService)
    svc.custom_api = _FakeCustom(raise_status=404)
    assert svc.get_application_status("lakehouse-pipelines", "argocd") is None


# --------------------------------------------------------------------------
# ensure_user_acl / remove_user_acl — read-modify-write on a KafkaUser CR's
# spec.authorization.acls (connect-user ACL grant/revoke). Reuses FakeCustom
# above (already supports a configurable get_response + records
# patched/last_patch) rather than a separate fake.
# --------------------------------------------------------------------------

ACL = {"resource": {"type": "topic", "name": "orders", "patternType": "literal"},
       "operations": ["Read", "Describe"]}


def test_ensure_user_acl_adds_when_absent():
    fake = FakeCustom()
    fake.get_response = {"metadata": {"resourceVersion": "10"}, "spec": {"authorization": {"acls": [
        {"resource": {"type": "topic", "name": "cdc.", "patternType": "prefix"},
         "operations": ["Read"]}
    ]}}}
    svc = K8sService(custom_api=fake, core_api=None, namespace=NAMESPACE)
    svc.ensure_user_acl("connect", ACL)

    patched = fake.last_patch["body"]["spec"]["authorization"]["acls"]
    assert ACL in patched
    assert len(patched) == 2   # base preserved + new one
    # optimistic-concurrency precondition carried on the patch
    assert fake.last_patch["body"]["metadata"]["resourceVersion"] == "10"


def test_ensure_user_acl_idempotent_when_present():
    fake = FakeCustom()
    fake.get_response = {"metadata": {"resourceVersion": "10"}, "spec": {"authorization": {"acls": [ACL]}}}
    svc = K8sService(custom_api=fake, core_api=None, namespace=NAMESPACE)
    svc.ensure_user_acl("connect", ACL)

    assert len(fake.patched) == 0   # already present -> no patch


def test_ensure_user_acl_retries_once_on_patch_conflict():
    """A real concurrent modification (e.g. a second add_source call racing
    on the same connect KafkaUser) surfaces as a 409 on patch because the
    body carries the read's resourceVersion as a precondition. The method
    must re-read the fresh object (2nd get) and re-patch (2nd patch attempt,
    which succeeds) rather than losing the update."""
    fake = FakeCustom(conflict_on_patch_once=True)
    fake.get_response = {"metadata": {"resourceVersion": "10"}, "spec": {"authorization": {"acls": [
        {"resource": {"type": "topic", "name": "cdc.", "patternType": "prefix"},
         "operations": ["Read"]}
    ]}}}
    svc = K8sService(custom_api=fake, core_api=None, namespace=NAMESPACE)
    svc.ensure_user_acl("connect", ACL)

    assert len(fake.get_calls) == 2       # re-read after the 409
    assert fake.patch_attempts == 2       # first attempt 409'd, second succeeded
    assert len(fake.patched) == 1         # only the successful patch is recorded
    patched = fake.last_patch["body"]["spec"]["authorization"]["acls"]
    assert ACL in patched
    assert len(patched) == 2


def test_remove_user_acl_removes_when_present():
    fake = FakeCustom()
    fake.get_response = {"metadata": {"resourceVersion": "10"}, "spec": {"authorization": {"acls": [
        ACL,
        {"resource": {"type": "topic", "name": "cdc.", "patternType": "prefix"},
         "operations": ["Read"]},
    ]}}}
    svc = K8sService(custom_api=fake, core_api=None, namespace=NAMESPACE)
    svc.remove_user_acl("connect", ACL)

    patched = fake.last_patch["body"]["spec"]["authorization"]["acls"]
    assert ACL not in patched
    assert len(patched) == 1
    assert fake.last_patch["body"]["metadata"]["resourceVersion"] == "10"


def test_remove_user_acl_idempotent_when_absent():
    fake = FakeCustom()
    fake.get_response = {"metadata": {"resourceVersion": "10"}, "spec": {"authorization": {"acls": []}}}
    svc = K8sService(custom_api=fake, core_api=None, namespace=NAMESPACE)
    svc.remove_user_acl("connect", ACL)

    assert len(fake.patched) == 0


def test_remove_user_acl_retries_once_on_patch_conflict():
    """Symmetric to test_ensure_user_acl_retries_once_on_patch_conflict: a
    409 on the first patch attempt (stale resourceVersion) must trigger a
    re-read + re-patch rather than a lost update."""
    fake = FakeCustom(conflict_on_patch_once=True)
    fake.get_response = {"metadata": {"resourceVersion": "10"}, "spec": {"authorization": {"acls": [
        ACL,
        {"resource": {"type": "topic", "name": "cdc.", "patternType": "prefix"},
         "operations": ["Read"]},
    ]}}}
    svc = K8sService(custom_api=fake, core_api=None, namespace=NAMESPACE)
    svc.remove_user_acl("connect", ACL)

    assert len(fake.get_calls) == 2       # re-read after the 409
    assert fake.patch_attempts == 2       # first attempt 409'd, second succeeded
    assert len(fake.patched) == 1         # only the successful patch is recorded
    patched = fake.last_patch["body"]["spec"]["authorization"]["acls"]
    assert ACL not in patched
    assert len(patched) == 1


# --------------------------------------------------------------------------
# apply_user / delete_user / read_secret
# --------------------------------------------------------------------------

def test_apply_user_hits_kafkausers_plural():
    fake = FakeCustom()
    svc = K8sService(custom_api=fake, core_api=None, namespace=NAMESPACE)
    svc.apply_user({"metadata": {"name": "nginx-producer"}, "spec": {}})

    assert len(fake.created) == 1
    call = fake.created[0]
    assert call["plural"] == "kafkausers"
    assert call["group"] == "kafka.strimzi.io"
    assert call["version"] == "v1beta2"
    assert call["namespace"] == "example"
    assert call["body"] == {"metadata": {"name": "nginx-producer"}, "spec": {}}


def test_apply_user_falls_back_to_patch_on_409():
    fake = FakeCustom(conflict_on_create=True)
    svc = K8sService(custom_api=fake, core_api=None, namespace=NAMESPACE)
    body = {"metadata": {"name": "nginx-producer"}, "spec": {}}

    svc.apply_user(body)

    assert fake.created == []
    assert len(fake.patched) == 1
    patch_call = fake.patched[0]
    assert patch_call["group"] == "kafka.strimzi.io"
    assert patch_call["version"] == "v1beta2"
    assert patch_call["plural"] == "kafkausers"
    assert patch_call["namespace"] == "example"
    assert patch_call["name"] == "nginx-producer"
    assert patch_call["body"] == body


def test_delete_user_hits_kafkausers_plural():
    fake = FakeCustom()
    svc = K8sService(custom_api=fake, core_api=None, namespace=NAMESPACE)
    svc.delete_user("nginx-producer")

    assert len(fake.deleted) == 1
    assert fake.deleted[0] == {
        "group": "kafka.strimzi.io",
        "version": "v1beta2",
        "namespace": "example",
        "plural": "kafkausers",
        "name": "nginx-producer",
    }


def test_delete_user_swallows_404():
    # FINAL-REVIEW Fix 2: a kafka-ingest source predating per-pipeline
    # producer provisioning has no producer KafkaUser at all -- deleting it
    # must be a no-op, not a raised ApiException that would abort
    # `delete_source`'s teardown before `delete_connector` runs.
    fake = FakeCustom(not_found_on_delete=True)
    svc = K8sService(custom_api=fake, core_api=None, namespace=NAMESPACE)

    result = svc.delete_user("k1-producer")

    assert result is None
    assert fake.deleted == []


def test_delete_user_reraises_non_404():
    class RaisingCustom(FakeCustom):
        def delete_namespaced_custom_object(self, **kw):
            raise ApiException(status=500, reason="Boom")

    svc = K8sService(custom_api=RaisingCustom(), core_api=None, namespace=NAMESPACE)

    with pytest.raises(ApiException):
        svc.delete_user("k1-producer")


def test_read_secret_decodes_base64_data():
    fake_core = FakeCore(secret_data={"password": base64.b64encode(b"mysecretpw").decode("utf-8")})
    svc = K8sService(custom_api=None, core_api=fake_core, namespace=NAMESPACE)

    result = svc.read_secret("nginx-producer")

    assert result == {"password": "mysecretpw"}


def test_read_secret_handles_multiple_keys():
    fake_core = FakeCore(secret_data={
        "user": base64.b64encode(b"admin").decode("utf-8"),
        "password": base64.b64encode(b"secret123").decode("utf-8"),
    })
    svc = K8sService(custom_api=None, core_api=fake_core, namespace=NAMESPACE)

    result = svc.read_secret("nginx-producer")

    assert result == {"user": "admin", "password": "secret123"}


def test_read_secret_returns_none_on_404():
    fake_core = FakeCore(secret_not_found=True)
    svc = K8sService(custom_api=None, core_api=fake_core, namespace=NAMESPACE)

    result = svc.read_secret("missing")

    assert result is None


def test_read_secret_reraises_non_404_errors():
    class ExplodingCore(FakeCore):
        def read_namespaced_secret(self, **kw):
            raise ApiException(status=500, reason="Boom")

    svc = K8sService(custom_api=None, core_api=ExplodingCore(), namespace=NAMESPACE)
    with pytest.raises(ApiException) as exc_info:
        svc.read_secret("nginx-producer")
    assert exc_info.value.status == 500
