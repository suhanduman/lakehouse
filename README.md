# lakehouse (v2)

Deklaratif, minimal-kod açık kaynak data lakehouse: Debezium (Strimzi Kafka Connect) → Apache Iceberg (Polaris REST katalog) → Spark MERGE → Trino/Superset/notebook.

- Tasarım: `docs/specs/2026-09-10-lakehouse-v2-design.md`
- Neden v2: `docs/reviews/2026-09-10-architecture-reassessment/00-KARAR.md`
- Şu an: F3 mongo + nginx (Debezium mongodb ham envelope → Spark `mongo-bronze` → Bronze/Silver `(_id,_doc)`; Fluent Bit → Kafka dış listener → Iceberg sink `nginx_raw.access_log`) — plan `docs/plans/2026-09-11-lakehouse-v2-f3-mongo-nginx.md`; bulgular `docs/plans/2026-09-10-f0-findings.md`
