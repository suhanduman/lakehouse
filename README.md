# lakehouse (v2)

Deklaratif, minimal-kod açık kaynak data lakehouse: Debezium (Strimzi Kafka Connect) → Apache Iceberg (Polaris REST katalog) → Spark MERGE → Trino/Superset/notebook.

- Tasarım: `docs/specs/2026-09-10-lakehouse-v2-design.md`
- Neden v2: `docs/reviews/2026-09-10-architecture-reassessment/00-KARAR.md`
- Şu an: **F6 tamam — kabul/pre-ship** (`runbooks/preship-openshift.md`). Cutover yapıldı: geliştirme dalı `v2` yeniden adlandırılarak **`main`** oldu (eski `main` = v1, emekliye ayrıldı); CI ve tüm `targetRevision`'lar `main`'i izler. F6 notu: `docs/plans/2026-09-10-f0-findings.md` (plan `docs/plans/2026-09-18-lakehouse-v2-f6-cutover.md`).
- Runbook'lar: kurulum `runbooks/install.md` · kullanıcı yüzü `runbooks/user-facing.md` · yetkilendirme `runbooks/access-control.md` · kaynak ekleme `runbooks/add-source.md` · tablo ekleme `runbooks/add-table.md` · nginx ajanı `runbooks/nginx-agent.md` · S3 kaydı `runbooks/s3-register.md`
- F5 runbook'ları: sürüm/lisans `runbooks/versions.md` · yükseltme `runbooks/upgrade.md` · yedek/DR `runbooks/dr.md` · sorun giderme `runbooks/troubleshooting.md` · kabul testleri `runbooks/acceptance-tests.md` (+ `runbooks/scripts/acceptance.sh`) · veri metrikleri `runbooks/data-metrics.md`
- Pre-ship: OpenShift canlı doğrulama kontrol listesi `runbooks/preship-openshift.md` (F1–F5'ten devreden tüm maddeler + PDB/MM2 kararları)
- dbt referans örneği: `runbooks/dbt/` — resmi `dbt-trino` imajı olmadığı için örnek CronJob resmi `python:3.13-slim` imajını kullanır ve `dbt-trino==1.10.4`'ü çalışma anında `pip install` eder (özel imaj yok kuralı; PyPI erişimi gerekir)
