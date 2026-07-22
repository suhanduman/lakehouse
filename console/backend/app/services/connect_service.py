"""ConnectService + ApicurioClient: thin, read-only wrappers over the
Kafka Connect REST API and the Apicurio Registry v2 REST API.

No network calls of their own — both constructors take an injected
`httpx.Client` (or a client built on `httpx.MockTransport` in tests), so
they're unit-testable without a live Connect worker or registry (see
tests/test_connect_service.py).
"""

from __future__ import annotations

from typing import Any, Dict, List


class ConnectService:
    """Read-only wrapper over Kafka Connect's REST API. `http` is an
    injected `httpx.Client` (or hand-rolled fake in tests) — see class
    docstring."""

    def __init__(self, base_url: str, http: Any) -> None:
        self.base_url = base_url.rstrip("/")
        self.http = http

    def connector_status(self, name: str) -> Dict[str, Any]:
        response = self.http.get(f"{self.base_url}/connectors/{name}/status")
        response.raise_for_status()
        return response.json()

    def list_connectors(self) -> List[str]:
        response = self.http.get(f"{self.base_url}/connectors")
        response.raise_for_status()
        return response.json()


class ApicurioClient:
    """Read-only wrapper over the Apicurio Registry v2 REST API. `http` is
    an injected `httpx.Client` (or hand-rolled fake in tests) — see class
    docstring."""

    def __init__(self, base_url: str, http: Any) -> None:
        self.base_url = base_url.rstrip("/")
        self.http = http

    def list_schemas(self) -> List[Dict[str, Any]]:
        response = self.http.get(f"{self.base_url}/search/artifacts")
        response.raise_for_status()
        return response.json().get("artifacts", [])
