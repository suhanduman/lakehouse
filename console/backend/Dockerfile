# Lakehouse Console backend -- deploy image.
#
# Runtime target only: the venv used for local development/tests is Python
# 3.9 (see console/backend/.venv), but the deploy target is python:3.11-slim
# per docs/superpowers/sdd/task-10-brief.md. Not built in this environment --
# validated via the pytest suite (console/backend/tests), including the
# full-route smoke test (tests/test_smoke_all_routes.py).
FROM python:3.11-slim

WORKDIR /srv/app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

EXPOSE 8080

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080"]
