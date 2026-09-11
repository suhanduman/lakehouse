# lakehouse (v2)

Deklaratif, minimal-kod açık kaynak data lakehouse: Debezium (Strimzi Kafka Connect) → Apache Iceberg (Polaris REST katalog) → Spark MERGE → Trino/Superset/notebook.

- Tasarım: `docs/specs/2026-09-10-lakehouse-v2-design.md`
- Neden v2: `docs/reviews/2026-09-10-architecture-reassessment/00-KARAR.md`
- Şu an: F1 platform (bootstrap + ArgoCD app-of-apps + glue) — plan `docs/plans/2026-09-11-lakehouse-v2-f1-platform.md`; F0 bulguları `docs/plans/2026-09-10-f0-findings.md`
