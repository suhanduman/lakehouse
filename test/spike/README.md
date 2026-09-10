# F0 spike'ları

Sıra: `00-cluster.sh` → `10-cnpg.sh` → `20-polaris.sh` → `30-connect.sh` → (S3 için `30-connect/iceberg-sink-partitioned.yaml`) → `40-spark.sh` → `90-teardown.sh`.
Her script idempotent; `--check` yalnız doğrular. Doğrulamalar (pyiceberg smoke/verify) **küme içinde** Job olarak koşar — Polaris istemciye katalogdaki küme-içi S3 endpoint'ini döndürür, laptop'tan çözülmez.
Python araçları (CLI için): `python3 -m venv .venv && .venv/bin/pip install -r test/spike/requirements.txt` (repo kökünde).
Bulgular: `docs/plans/2026-09-10-f0-findings.md`. Gereksinim: Podman machine ≥ 6 CPU / 10 GB, kind ≥ 0.33 (Podman 6 uyumu).
