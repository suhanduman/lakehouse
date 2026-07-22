from fastapi.testclient import TestClient
from app.main import app

def test_healthz_ok():
    r = TestClient(app).get("/healthz")
    assert r.status_code == 200
    assert r.json() == {"status": "ok"}


def test_metrics_endpoint_exposes_prometheus():
    resp = TestClient(app).get("/metrics")
    assert resp.status_code == 200
    body = resp.text
    # prometheus text exposition format: HELP/TYPE comment lines + request metric
    assert "# HELP" in body
    assert "http_request" in body
