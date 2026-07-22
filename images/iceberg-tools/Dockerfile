# iceberg-tools — CDC hedef Iceberg tablolarını identifier field ile pre-create
# eden küçük yardımcı imaj. tools/create_iceberg_table.py (pyiceberg → Nessie
# REST) + bağımlılıklarını barındırır. İKİ tüketici:
#   1) B-v2 ArgoCD PreSync hook Job'ı (gitops/pipeline-template/*-precreate-job)
#      — KafkaConnector sync'inden ÖNCE tabloyu identifier field ile yaratır.
#   2) CI (gitops/ci/validate-pipeline.sh) — `--dry-run` ile PR'daki descriptor'ı
#      Nessie/S3'e dokunmadan doğrular.
#
# Build (context = repo kökü):
#   docker build -f images/iceberg-tools/Dockerfile -t <registry>/iceberg-tools:<tag> .
#
# NOT (F bulgusu): Trino identifier field set edemediği ve Nessie REST ALTER'ı
# reddettiği için tablo yaratımı pyiceberg ile ve YALNIZCA CREATE anında yapılır
# — bkz. docs/superpowers/specs/2026-07-19-console-gitops-b-v2-design.md.
FROM python:3.12-slim

# pyiceberg[s3fs] — console/backend/requirements.txt ile aynı sürüm çizgisi
# (senkron tutulmalı: iki yer de aynı pyiceberg davranışına dayanır).
RUN pip install --no-cache-dir "pyiceberg[s3fs]==0.10.*" "pyyaml==6.*"

# Discover modu (pg/mongo introspeksiyonu) opsiyonel sürücüler — descriptor
# modu (PreSync hook'un kullandığı) bunlara ihtiyaç duymaz, ama CLI discover
# için gerekli. Slim tutmak istenirse kaldırılabilir (descriptor modu çalışır).
RUN pip install --no-cache-dir "psycopg[binary]" "pymongo"

COPY tools/create_iceberg_table.py /opt/tools/create_iceberg_table.py

# OpenShift SCC: rastgele (non-root) UID ile çalışır; /opt/tools zaten world-
# readable. Ek yazma gerektirmez (descriptor read-only mount edilir).
WORKDIR /opt/tools
ENTRYPOINT ["python3", "/opt/tools/create_iceberg_table.py"]
