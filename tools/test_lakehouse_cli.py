"""Unit tests for tools/lakehouse-cli (device-flow token helper for BYO notebooks).

Loaded via importlib since the script has no .py extension (it's a chmod +x CLI entrypoint).
No live Keycloak calls: urllib.request.urlopen is monkeypatched.
"""
import importlib.machinery
import importlib.util
import io
import json
import os
import sys

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


def test_device_login_returns_token(monkeypatch):
    calls = {"n": 0}

    def fake_urlopen(req, timeout=0):
        calls["n"] += 1
        if calls["n"] == 1:
            return _Resp({
                "device_code": "dc",
                "user_code": "AB-CD",
                "verification_uri": "https://kc/device",
                "interval": 0,
                "expires_in": 60,
            })
        return _Resp({"access_token": "JWT123"})

    monkeypatch.setattr(lhc.urllib.request, "urlopen", fake_urlopen)
    assert lhc.device_login("https://keycloak.d/realms/lakehouse") == "JWT123"


def test_main_prints_env(monkeypatch, capsys):
    monkeypatch.setattr(lhc, "device_login", lambda *a, **k: "JWT123")
    monkeypatch.setattr(sys, "argv", ["lakehouse-cli", "login", "--domain", "kurum.edu.tr"])
    lhc.main()
    out = capsys.readouterr().out
    assert "export LAKEHOUSE_TOKEN=JWT123" in out
    assert "TRINO_HOST=trino.kurum.edu.tr" in out
    assert "NESSIE_URI=https://nessie.kurum.edu.tr/iceberg/" in out
