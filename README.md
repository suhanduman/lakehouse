# lakehouse (v2)

Deklaratif, minimal-kod açık kaynak data lakehouse: Debezium (Strimzi Kafka Connect) → Apache Iceberg (Polaris REST katalog) → Spark MERGE → Trino/Superset/notebook.

- Tasarım: `docs/specs/2026-09-10-lakehouse-v2-design.md`
- Neden v2: `docs/reviews/2026-09-10-architecture-reassessment/00-KARAR.md`
- Şu an: F0 spike'ları (`test/spike/`, plan `docs/plans/2026-09-10-lakehouse-v2-f0-spikes.md`)
