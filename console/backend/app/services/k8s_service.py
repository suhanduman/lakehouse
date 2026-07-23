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

HTTP_CONFLICT = 409
HTTP_NOT_FOUND = 404


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
    # orchestrator's rollback.
    # ----------------------------------------------------------------

    def delete_secret(self, name: str) -> Dict[str, Any]:
        return self.core_api.delete_namespaced_secret(name=name, namespace=self.namespace)

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
    # list_sources — all KafkaConnector CRs in the namespace
    # ----------------------------------------------------------------

    def list_sources(self) -> List[Dict[str, Any]]:
        response = self.custom_api.list_namespaced_custom_object(
            group=GROUP,
            version=VERSION,
            namespace=self.namespace,
            plural=CONNECTOR_PLURAL,
        )
        return response.get("items", [])

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
