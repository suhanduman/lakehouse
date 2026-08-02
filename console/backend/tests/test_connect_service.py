"""ConnectService + ApicurioClient: thin, read-only wrappers over Kafka
Connect's REST API and the Apicurio Registry v2 REST API. No live
services — every test injects an `httpx.Client` built on
`httpx.MockTransport`, so the wrapper's URL-shaping and response-parsing is
exercised without a real Connect worker or registry.
"""

from __future__ import annotations

import httpx

from app.services.connect_service import ApicurioClient, ConnectService


# --------------------------------------------------------------------------
# ConnectService.connector_status
# --------------------------------------------------------------------------

def test_connector_status():
    def handler(request):
        assert request.url.path == "/connectors/dbz-x/status"
        return httpx.Response(200, json={"connector": {"state": "RUNNING"}})

    c = ConnectService(
        "http://c", httpx.Client(transport=httpx.MockTransport(handler))
    )
    assert c.connector_status("dbz-x")["connector"]["state"] == "RUNNING"


def test_connector_status_uses_get_method():
    seen = {}

    def handler(request):
        seen["method"] = request.method
        return httpx.Response(200, json={"connector": {"state": "PAUSED"}})

    c = ConnectService(
        "http://connect-connect-api:8083",
        httpx.Client(transport=httpx.MockTransport(handler)),
    )
    c.connector_status("jdbc-mssql1-courses")
    assert seen["method"] == "GET"


# --------------------------------------------------------------------------
# ConnectService.list_connectors
# --------------------------------------------------------------------------

def test_list_connectors_returns_names():
    def handler(request):
        assert request.url.path == "/connectors"
        return httpx.Response(200, json=["dbz-x", "jdbc-y"])

    c = ConnectService("http://c", httpx.Client(transport=httpx.MockTransport(handler)))
    assert c.list_connectors() == ["dbz-x", "jdbc-y"]


def test_list_connectors_empty():
    def handler(request):
        return httpx.Response(200, json=[])

    c = ConnectService("http://c", httpx.Client(transport=httpx.MockTransport(handler)))
    assert c.list_connectors() == []


# --------------------------------------------------------------------------
# ApicurioClient.list_schemas
# --------------------------------------------------------------------------

def test_apicurio_list_schemas():
    def handler(request):
        assert request.url.path == "/apis/registry/v2/search/artifacts"
        return httpx.Response(
            200,
            json={
                "artifacts": [
                    {"id": "cdc.mssql1.dbo.students-value"},
                    {"id": "mongo.lms.enrollments-value"},
                ],
                "count": 2,
            },
        )

    client = ApicurioClient(
        "http://apicurio-registry:8080/apis/registry/v2",
        httpx.Client(transport=httpx.MockTransport(handler)),
    )
    schemas = client.list_schemas()

    assert schemas == [
        {"id": "cdc.mssql1.dbo.students-value"},
        {"id": "mongo.lms.enrollments-value"},
    ]


def test_apicurio_list_schemas_empty():
    def handler(request):
        return httpx.Response(200, json={"artifacts": [], "count": 0})

    client = ApicurioClient(
        "http://apicurio-registry:8080/apis/registry/v2",
        httpx.Client(transport=httpx.MockTransport(handler)),
    )
    assert client.list_schemas() == []


# --------------------------------------------------------------------------
# ConnectService.restart_connector
# --------------------------------------------------------------------------

def test_restart_connector_posts_with_query_params():
    seen = {}

    def handler(request):
        seen["method"] = request.method
        seen["path"] = request.url.path
        seen["query"] = dict(request.url.params)
        return httpx.Response(204)

    c = ConnectService(
        "http://connect-connect-api:8083",
        httpx.Client(transport=httpx.MockTransport(handler)),
    )
    assert c.restart_connector("dbz-x") is None
    assert seen["method"] == "POST"
    assert seen["path"] == "/connectors/dbz-x/restart"
    assert seen["query"] == {"includeTasks": "true", "onlyFailed": "false"}


def test_restart_connector_only_failed_tasks():
    seen = {}

    def handler(request):
        seen["query"] = dict(request.url.params)
        return httpx.Response(202)

    c = ConnectService("http://c", httpx.Client(transport=httpx.MockTransport(handler)))
    c.restart_connector("dbz-x", include_tasks=True, only_failed=True)
    assert seen["query"] == {"includeTasks": "true", "onlyFailed": "true"}


def test_restart_connector_raises_on_error():
    def handler(request):
        return httpx.Response(409, json={"message": "rebalance in progress"})

    c = ConnectService("http://c", httpx.Client(transport=httpx.MockTransport(handler)))
    try:
        c.restart_connector("dbz-x")
        assert False, "expected HTTPStatusError"
    except httpx.HTTPStatusError as exc:
        assert exc.response.status_code == 409
