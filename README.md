# lakehouse (v2)

Deklaratif, minimal-kod açık kaynak data lakehouse: Debezium (Strimzi Kafka Connect) → Apache Iceberg (Polaris REST katalog) → Spark MERGE → Trino/Superset/notebook.

- Tasarım: `docs/specs/2026-09-10-lakehouse-v2-design.md`
- Neden v2: `docs/reviews/2026-09-10-architecture-reassessment/00-KARAR.md`
- Şu an: **F4 kullanıcı yüzü** (cert-manager TLS zinciri; Keycloak realm + gruplar; Trino 483 `OAUTH2,PASSWORD` + `rules.json` satır filtresi/kolon maskesi + `sandbox`; Superset Operator 0.2.0; JupyterHub z2jh; Zeppelin) — plan `docs/plans/2026-09-17-lakehouse-v2-f4-user-facing.md`; bulgular `docs/plans/2026-09-10-f0-findings.md`
- Runbook'lar: kurulum `runbooks/install.md` · kullanıcı yüzü `runbooks/user-facing.md` · yetkilendirme `runbooks/access-control.md` · kaynak ekleme `runbooks/add-source.md`
