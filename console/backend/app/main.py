from __future__ import annotations

from typing import Any

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from prometheus_fastapi_instrumentator import Instrumentator

HTTP_422_UNPROCESSABLE = 422

from app.routers import buckets, gitops, pipelines, schemas, sources, tables
from app.routers import status as status_router

app = FastAPI(title="Lakehouse Console")
app.include_router(sources.router)
app.include_router(tables.router)
app.include_router(buckets.router)
app.include_router(schemas.router)
app.include_router(status_router.router)
app.include_router(gitops.router)
app.include_router(pipelines.router)

# Prometheus /metrics (monitoring D1). ServiceMonitor `console-backend` scrapes
# this Service. Adds an HTTP request counter + latency histogram plus default
# process metrics.
Instrumentator().instrument(app).expose(app, endpoint="/metrics", include_in_schema=False)

# Field names whose *values* must never be echoed back to the client (nor end
# up in logs via the default error echo). Source creation carries DB
# credentials in its body, and FastAPI/pydantic's default 422 response echoes
# the offending `input` verbatim -- which would leak the password on any
# validation error. This handler redacts them before responding.
_SENSITIVE_KEYS = {"password", "credentials"}


def _redact(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            key: ("***" if key in _SENSITIVE_KEYS else _redact(val))
            for key, val in value.items()
        }
    if isinstance(value, (list, tuple)):
        return [_redact(item) for item in value]
    return value


@app.exception_handler(RequestValidationError)
async def _redacting_validation_handler(
    request: Request, exc: RequestValidationError
) -> JSONResponse:
    errors = []
    for error in exc.errors():
        error = dict(error)
        loc = error.get("loc", ())
        # Drop the echoed input for errors located on a sensitive field, and
        # deep-redact any sensitive keys inside a broader echoed input dict.
        if any(str(part) in _SENSITIVE_KEYS for part in loc):
            error.pop("input", None)
        elif "input" in error:
            error["input"] = _redact(error["input"])
        errors.append(error)
    return JSONResponse(
        status_code=HTTP_422_UNPROCESSABLE,
        content={"detail": errors},
    )


@app.get("/healthz")
def healthz():
    return {"status": "ok"}
