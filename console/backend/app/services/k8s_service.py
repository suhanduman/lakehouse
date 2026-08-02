"""K8sService: thin CR CRUD wrapper over `kubernetes.client.CustomObjectsApi`
(KafkaConnector/KafkaTopic — group `kafka.strimzi.io`, version `v1beta2`;
ScheduledSparkApplication — group `sparkoperator.k8s.io`, version `v1beta2`)
and `kubernetes.client.CoreV1Api` (Secrets).

No cluster/network calls of its own — the constructor takes already-built api
client objects, so the whole thing is unit-testable with a hand-rolled fake
recording calls (see tests/test_k8s_service.py) without a live cluster.

`apply_*` implements "create, or patch if it already exists" (idempotent
apply) by attempting create and falling back to patch on a 409 (Conflict)
`ApiException`; any other status is re-raised.
"""

from __future__ import annotations

import base64
from typing import Any, Dict, List, Optional

from kubernetes.client.exceptions import ApiException

GROUP = "kafka.strimzi.io"
VERSION = "v1beta2"
CONNECTOR_PLURAL = "kafkaconnectors"
TOPIC_PLURAL = "kafkatopics"

SPARK_GROUP = "sparkoperator.k8s.io"
SPARK_API_VERSION = "v1beta2"
SPARK_PLURAL = "scheduledsparkapplications"

USER_PLURAL = "kafkausers"

HTTP_CONFLICT = 409
HTTP_NOT_FOUND = 404


def _acl_key(acl: Dict[str, Any]) -> tuple:
    """Identity of a Strimzi ACL entry for dedup/removal purposes: compares by
    resource (type+name+patternType) only, so differing `operations` on the
    same resource don't create duplicate entries."""
    r = acl.get("resource", {})
    return (r.get("type"), r.get("name"), r.get("patternType"))


class K8sService:
    """CRUD wrapper. `custom_api`/`core_api` are injected kubernetes client
    instances (or hand-rolled fakes in tests) — see class docstring."""

    def __init__(self, custom_api: Any, core_api: Any, namespace: str) -> None:
        self.custom_api = custom_api
        self.core_api = core_api
        self.namespace = namespace

    # ----------------------------------------------------------------
    # apply_* — create, fall back to patch on 409 (idempotent)
    # ----------------------------------------------------------------

    def apply_connector(self, body: Dict[str, Any]) -> Dict[str, Any]:
        return self._apply(GROUP, VERSION, CONNECTOR_PLURAL, body)

    def apply_topic(self, body: Dict[str, Any]) -> Dict[str, Any]:
        return self._apply(GROUP, VERSION, TOPIC_PLURAL, body)

    def apply_spark_job(self, body: Dict[str, Any]) -> Dict[str, Any]:
        return self._apply(SPARK_GROUP, SPARK_API_VERSION, SPARK_PLURAL, body)

    def _apply(self, group: str, version: str, plural: str, body: Dict[str, Any]) -> Dict[str, Any]:
        try:
            return self.custom_api.create_namespaced_custom_object(
                group=group,
                version=version,
                namespace=self.namespace,
                plural=plural,
                body=body,
            )
        except ApiException as exc:
            if exc.status != HTTP_CONFLICT:
                raise
            name = body["metadata"]["name"]
            return self.custom_api.patch_namespaced_custom_object(
                group=group,
                version=version,
                namespace=self.namespace,
                plural=plural,
                name=name,
                body=body,
            )

    # ----------------------------------------------------------------
    # delete_*
    # ----------------------------------------------------------------

    def delete_connector(self, name: str) -> Dict[str, Any]:
        return self._delete(GROUP, VERSION, CONNECTOR_PLURAL, name)

    def delete_topic(self, name: str) -> Dict[str, Any]:
        return self._delete(GROUP, VERSION, TOPIC_PLURAL, name)

    def delete_spark_job(self, name: str) -> Optional[Dict[str, Any]]:
        try:
            return self._delete(SPARK_GROUP, SPARK_API_VERSION, SPARK_PLURAL, name)
        except ApiException as exc:
            if exc.status != HTTP_NOT_FOUND:
                raise
            return None

    def _delete(self, group: str, version: str, plural: str, name: str) -> Dict[str, Any]:
        return self.custom_api.delete_namespaced_custom_object(
            group=group,
            version=version,
            namespace=self.namespace,
            plural=plural,
            name=name,
        )

    # ----------------------------------------------------------------
    # create_secret — base64-encodes values (v1.Secret.data is base64 text).
    # Idempotent like apply_* (create, fall back on 409): a 409 (already
    # exists) triggers a replace so the existing Secret converges to the
    # desired data, rather than being left with stale credentials. This
    # matters for credential rotation and for the orchestrator re-running
    # create_secret on an already-provisioned source — an
    # idempotent retry must reflect the new data, not silently keep the old.
    # ----------------------------------------------------------------

    def create_secret(self, name: str, data: Dict[str, str]) -> Dict[str, Any]:
        encoded = {
            key: base64.b64encode(value.encode("utf-8")).decode("utf-8")
            for key, value in data.items()
        }
        body = {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {"name": name, "namespace": self.namespace},
            "type": "Opaque",
            "data": encoded,
        }
        try:
            return self.core_api.create_namespaced_secret(namespace=self.namespace, body=body)
        except ApiException as exc:
            if exc.status != HTTP_CONFLICT:
                raise
            return self.core_api.replace_namespaced_secret(
                name=name, namespace=self.namespace, body=body
            )

    # ----------------------------------------------------------------
    # delete_secret — best-effort counterpart used by the add-source
    # orchestrator's rollback, AND by `delete_source`'s kafka-ingest producer
    # teardown. 404-tolerant (mirrors `delete_spark_job` above): a producer
    # Secret may never have materialized yet (Strimzi creates it
    # asynchronously after the KafkaUser, see `read_secret`'s docstring), or
    # may already be gone -- either way that must be a no-op, not a raised
    # ApiException that aborts the caller's teardown before later steps
    # (e.g. `delete_connector`) run. Any OTHER status still propagates.
    # ----------------------------------------------------------------

    def delete_secret(self, name: str) -> Optional[Dict[str, Any]]:
        try:
            return self.core_api.delete_namespaced_secret(name=name, namespace=self.namespace)
        except ApiException as exc:
            if exc.status != HTTP_NOT_FOUND:
                raise
            return None

    # ----------------------------------------------------------------
    # patch_connector — merge new connector `spec.config` (edit flow)
    # ----------------------------------------------------------------

    def patch_connector(self, name: str, config: Dict[str, Any]) -> Dict[str, Any]:
        return self.custom_api.patch_namespaced_custom_object(
            group=GROUP,
            version=VERSION,
            namespace=self.namespace,
            plural=CONNECTOR_PLURAL,
            name=name,
            body={"spec": {"config": config}},
        )

    # ----------------------------------------------------------------
    # set_paused — patch KafkaConnector spec.state (Strimzi pause/resume)
    # ----------------------------------------------------------------

    def set_paused(self, name: str, paused: bool) -> Dict[str, Any]:
        state = "paused" if paused else "running"
        return self.custom_api.patch_namespaced_custom_object(
            group=GROUP,
            version=VERSION,
            namespace=self.namespace,
            plural=CONNECTOR_PLURAL,
            name=name,
            body={"spec": {"state": state}},
        )

    # ----------------------------------------------------------------
    # list_sources — KafkaConnector CRs + ScheduledSparkApplication CRs in
    # the namespace, each tagged with its own "kind" so callers can tell
    # them apart. Tolerates the spark-operator CRD being absent from the
    # cluster (returns just connectors in that case).
    # ----------------------------------------------------------------

    def _list(self, group: str, version: str, plural: str) -> List[Dict[str, Any]]:
        resp = self.custom_api.list_namespaced_custom_object(
            group=group, version=version, namespace=self.namespace, plural=plural,
        )
        return resp.get("items", [])

    def list_sources(self) -> List[Dict[str, Any]]:
        connectors = self._list(GROUP, VERSION, CONNECTOR_PLURAL)
        for c in connectors:
            c["kind"] = "KafkaConnector"
        try:
            sparks = self._list(SPARK_GROUP, SPARK_API_VERSION, SPARK_PLURAL)
        except ApiException as exc:
            if exc.status != HTTP_NOT_FOUND:
                raise
            sparks = []   # spark-operator CRD not installed on this cluster
        for s in sparks:
            s["kind"] = "ScheduledSparkApplication"
        return connectors + sparks

    # ----------------------------------------------------------------
    # get_status — `.status.connectorStatus` of one KafkaConnector CR
    # ----------------------------------------------------------------

    def get_status(self, name: str) -> Dict[str, Any]:
        obj = self.custom_api.get_namespaced_custom_object(
            group=GROUP,
            version=VERSION,
            namespace=self.namespace,
            plural=CONNECTOR_PLURAL,
            name=name,
        )
        return obj.get("status", {}).get("connectorStatus", {})

    # ----------------------------------------------------------------
    # get_spark_status / set_spark_suspended — status + suspend toggle for
    # one ScheduledSparkApplication CR
    # ----------------------------------------------------------------

    def get_spark_status(self, name: str) -> Dict[str, Any]:
        obj = self.custom_api.get_namespaced_custom_object(
            group=SPARK_GROUP, version=SPARK_API_VERSION, namespace=self.namespace,
            plural=SPARK_PLURAL, name=name,
        )
        return obj.get("status", {})

    def set_spark_suspended(self, name: str, suspended: bool) -> Dict[str, Any]:
        return self.custom_api.patch_namespaced_custom_object(
            group=SPARK_GROUP, version=SPARK_API_VERSION, namespace=self.namespace,
            plural=SPARK_PLURAL, name=name, body={"spec": {"suspend": suspended}},
        )

    # ----------------------------------------------------------------
    # get_application_status — ArgoCD Application CR .status (sync/health/
    # resources) via the k8s API. None when ArgoCD/the Application isn't
    # present (404) -- gitops-status is best-effort, never a hard dependency.
    # ----------------------------------------------------------------

    def get_application_status(self, name, namespace):
        """ArgoCD Application CR .status (sync/health/resources) via the k8s API.
        None when ArgoCD/the Application isn't present (404) -- gitops-status is
        best-effort, never a hard dependency."""
        try:
            obj = self.custom_api.get_namespaced_custom_object(
                group="argoproj.io", version="v1alpha1", namespace=namespace,
                plural="applications", name=name)
        except ApiException as exc:
            if exc.status == HTTP_NOT_FOUND:
                return None
            raise
        return (obj or {}).get("status")

    # ----------------------------------------------------------------
    # apply_user / delete_user — KafkaUser CR lifecycle (producer/consumer identity)
    # ----------------------------------------------------------------

    def apply_user(self, body: Dict[str, Any]) -> Dict[str, Any]:
        return self._apply(GROUP, VERSION, USER_PLURAL, body)

    def delete_user(self, name: str) -> Optional[Dict[str, Any]]:
        """404-tolerant (mirrors `delete_spark_job`/`delete_secret`): a
        kafka-ingest source created before per-pipeline producer
        provisioning existed has no producer KafkaUser at all, so
        `delete_source`'s teardown must not abort (and skip
        `delete_connector`) just because there is nothing here to delete.
        Any OTHER status still propagates."""
        try:
            return self._delete(GROUP, VERSION, USER_PLURAL, name)
        except ApiException as exc:
            if exc.status != HTTP_NOT_FOUND:
                raise
            return None

    # ----------------------------------------------------------------
    # read_secret — read a Secret and decode its base64 data. Used to
    # retrieve KafkaUser SCRAM password Secret (materializes asynchronously
    # with the same name as the user); returns None on 404 (not yet present).
    # ----------------------------------------------------------------

    def read_secret(self, name: str) -> Optional[Dict[str, str]]:
        """Base64-decoded `.data` of a Secret, or None if it isn't there yet
        (Strimzi materializes a KafkaUser's SCRAM password Secret asynchronously,
        named exactly like the user)."""
        try:
            obj = self.core_api.read_namespaced_secret(name=name, namespace=self.namespace)
        except ApiException as exc:
            if exc.status == HTTP_NOT_FOUND:
                return None
            raise
        data = getattr(obj, "data", None) or {}
        return {k: base64.b64decode(v).decode("utf-8") for k, v in data.items()}

    # ----------------------------------------------------------------
    # ensure_user_acl / remove_user_acl — read-modify-write a KafkaUser CR's
    # spec.authorization.acls. Used by the add-source/remove-source
    # orchestrator to grant/revoke a scoped topic READ on the `connect` user
    # per stream/kafka source. Both are idempotent (no-op if already in the
    # desired state) and retry once on a 409 (concurrent modification of the
    # KafkaUser) before giving up.
    # ----------------------------------------------------------------

    def ensure_user_acl(self, user: str, acl: Dict[str, Any]) -> None:
        """Idempotently add an ACL entry to a KafkaUser's spec.authorization.acls
        (read-modify-write, retry once on 409). No-op if an entry with the same
        (type,name,patternType) is already present.

        The patch body carries the read's `metadata.resourceVersion` as an
        optimistic-concurrency precondition: the apiserver rejects a stale
        write with 409 (rather than silently last-writer-wins), so a real
        concurrent modification surfaces the 409 this retry loop handles
        instead of two callers both reading-then-clobbering each other's
        appended ACL entry.
        """
        for attempt in range(2):
            obj = self.custom_api.get_namespaced_custom_object(
                group=GROUP, version=VERSION, namespace=self.namespace,
                plural=USER_PLURAL, name=user)
            acls = obj.get("spec", {}).get("authorization", {}).get("acls", []) or []
            if any(_acl_key(a) == _acl_key(acl) for a in acls):
                return
            new_acls = acls + [acl]
            rv = obj.get("metadata", {}).get("resourceVersion")
            try:
                self.custom_api.patch_namespaced_custom_object(
                    group=GROUP, version=VERSION, namespace=self.namespace,
                    plural=USER_PLURAL, name=user,
                    body={"metadata": {"resourceVersion": rv},
                          "spec": {"authorization": {"acls": new_acls}}})
                return
            except ApiException as exc:
                if exc.status == HTTP_CONFLICT and attempt == 0:
                    continue
                raise

    def remove_user_acl(self, user: str, acl: Dict[str, Any]) -> None:
        """Idempotently remove the ACL entry matching (type,name,patternType)
        from a KafkaUser (read-modify-write, retry once on 409). No-op if
        absent. Same resourceVersion optimistic-concurrency precondition as
        ensure_user_acl (see its docstring) so a genuine concurrent
        modification surfaces a 409 instead of a lost update."""
        for attempt in range(2):
            obj = self.custom_api.get_namespaced_custom_object(
                group=GROUP, version=VERSION, namespace=self.namespace,
                plural=USER_PLURAL, name=user)
            acls = obj.get("spec", {}).get("authorization", {}).get("acls", []) or []
            new_acls = [a for a in acls if _acl_key(a) != _acl_key(acl)]
            if len(new_acls) == len(acls):
                return
            rv = obj.get("metadata", {}).get("resourceVersion")
            try:
                self.custom_api.patch_namespaced_custom_object(
                    group=GROUP, version=VERSION, namespace=self.namespace,
                    plural=USER_PLURAL, name=user,
                    body={"metadata": {"resourceVersion": rv},
                          "spec": {"authorization": {"acls": new_acls}}})
                return
            except ApiException as exc:
                if exc.status == HTTP_CONFLICT and attempt == 0:
                    continue
                raise
