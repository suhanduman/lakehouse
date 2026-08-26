"""Unit tests for tools/lakehouse-cli (device-flow token helper for BYO notebooks).

Loaded via importlib since the script has no .py extension (it's a chmod +x CLI entrypoint).
No live Keycloak calls: urllib.request.urlopen is monkeypatched, and the mocks assert on the
*actual* request URLs/bodies (not just the return value) so a wrong issuer hostname or a
malformed token-exchange body fails the test instead of slipping through.
"""
import importlib.machinery
import importlib.util
import io
import json
import os
import sys
import urllib.parse

_SCRIPT_PATH = os.path.join(os.path.dirname(__file__), "lakehouse-cli")
# lakehouse-cli has no .py suffix, so importlib can't infer a loader from the
# extension via spec_from_file_location() alone (it returns None) — supply the
# SourceFileLoader explicitly.
_loader = importlib.machinery.SourceFileLoader("lhc", _SCRIPT_PATH)
spec = importlib.util.spec_from_loader("lhc", _loader)
lhc = importlib.util.module_from_spec(spec)
_loader.exec_module(lhc)


class _Resp(io.BytesIO):
    def __init__(self, d):
        super().__init__(json.dumps(d).encode())


_DEVICE_RESP = {
    "device_code": "dc",
    "user_code": "AB-CD",
    "verification_uri": "https://kc/device",
    "interval": 0,
    "expires_in": 60,
}


def test_device_login_returns_token(monkeypatch):
    """Asserts device_login hits the right endpoints with the right body, not just
    that *some* mocked call returns a token."""
    requests = []

    def fake_urlopen(req, timeout=0):
        requests.append(req)
        if len(requests) == 1:
            return _Resp(_DEVICE_RESP)
        return _Resp({"access_token": "JWT123"})

    monkeypatch.setattr(lhc.urllib.request, "urlopen", fake_urlopen)
    assert lhc.device_login("https://keycloak.d/realms/lakehouse") == "JWT123"

    assert len(requests) == 2
    assert requests[0].full_url.endswith("/protocol/openid-connect/auth/device")
    assert requests[1].full_url.endswith("/protocol/openid-connect/token")

    body = urllib.parse.parse_qs(requests[1].data.decode())
    assert body["grant_type"] == ["urn:ietf:params:oauth:grant-type:device_code"]
    assert body["client_id"] == ["lakehouse-token"]
    assert body["device_code"] == ["dc"]


def test_main_prints_env_and_uses_auth_host(monkeypatch, capsys):
    """Critical regression test: locks the issuer hostname to auth.<domain> (the chart's
    real external Keycloak route — see chart/templates/08-routes-tls.yaml / 08b-ingress-tls.yaml
    and 10-keycloak.yaml's routeHost name=auth), NOT keycloak.<domain> (which doesn't exist).
    Mocks urlopen directly (not device_login) so a wrong issuer in main() cannot hide behind a
    device_login mock."""
    requests = []

    def fake_urlopen(req, timeout=0):
        requests.append(req)
        if len(requests) == 1:
            return _Resp(_DEVICE_RESP)
        return _Resp({"access_token": "JWT123"})

    monkeypatch.setattr(lhc.urllib.request, "urlopen", fake_urlopen)
    monkeypatch.setattr(sys, "argv", ["lakehouse-cli", "login", "--domain", "kurum.edu.tr"])
    lhc.main()
    out = capsys.readouterr().out

    assert requests[0].full_url == "https://auth.kurum.edu.tr/realms/lakehouse/protocol/openid-connect/auth/device"

    assert "export LAKEHOUSE_TOKEN=JWT123" in out
    assert "TRINO_HOST=trino.kurum.edu.tr" in out
    assert "NESSIE_URI=https://nessie.kurum.edu.tr/iceberg/" in out
