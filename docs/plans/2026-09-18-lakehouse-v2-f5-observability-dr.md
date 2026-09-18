# Lakehouse v2 — F5 İzleme + DR + Runbook'lar + versions.md + Kabul Testi Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Kurulum sonunda (1) Kafka/Connect, Spark işleri, Trino, Superset, JupyterHub, Zeppelin metrikleri Prometheus'a aksın ve 4 PrometheusRule (Connect task FAILED, sink stalled, silver-merge son başarı yaşı > 45 dk, herhangi bir zamanlanmış Spark koşusu FAILED) canlı metrik adlarıyla doğrulanmış olsun; (2) Polaris/Keycloak/Superset DB'leri Barman Cloud plugin ile farklı bucket'a PITR yedeklensin ve restore provası e2e'de kanıtlansın; (3) Velero namespace yedeği (not defteri PVC'leri dahil) alınıp bir kaynağın restore'u e2e'de kanıtlansın; (4) `runbooks/{versions,upgrade,dr,troubleshooting,acceptance-tests}.md` + dbt/S3 referans örnekleri + kabul betiği yazılsın; (5) F4'ten devreden küçük borçlar kapansın; lokal + CI taze küme e2e yeşil.

**Architecture:** F1–F4 çizgisi: upstream chart/operator + values, CR'lar `glue`'da. İzleme: kind/vanilla'da minimal **kube-prometheus-stack** (operator + küçük Prometheus + kube-state-metrics + küçük Grafana; Alertmanager/node-exporter/kube-* scrape'leri kapalı) `monitoring` ns'inde **yalnız dev overlay**'de; OpenShift'te platformun user-workload monitoring'i aynı `monitoring.coreos.com/v1` nesnelerini (PodMonitor/ServiceMonitor/PrometheusRule, `lakehouse` ns) alır. Metrik kaynakları: Strimzi `metricsConfig` (upstream örnek ConfigMap'leri repo'ya dosya olarak), Strimzi `kafka-resources-metrics` PodMonitor, spark-operator `podMonitor`, **kube-state-metrics `customResourceState`** ile `SparkApplication`/`ScheduledSparkApplication` durumları (zaman damgalı gauge → yaş kuralı), Trino chart `jmx.exporter` + `serviceMonitor`, Superset CR `spec.monitoring.serviceMonitor`, z2jh hub `/hub/metrics`, Zeppelin `zeppelin.metric.enable.prometheus`. DR: CNPG **Barman Cloud plugin 0.15.0** (in-tree `barmanObjectStore` 1.26'dan beri deprecated, 1.31'de kalkıyor) — `ObjectStore` CR + `ScheduledBackup` ×3 + WAL arşivi; Velero chart (dev/vanilla) ya da OADP (OpenShift) + `Schedule` CR (`velero.io/v1`, namespace, `defaultVolumesToFsBackup`); Kafka/Iceberg verisi yedeklenmez (Kafka: yeniden akıtılabilir/MM2; Iceberg: S3 çoğaltması) — belgelenir.

**Tech Stack:** kube-prometheus-stack **91.4.1** (operator v0.94.0) · kube-state-metrics `customResourceState` · Strimzi 1.2 `examples/metrics` ConfigMap'leri · spark-operator 2.5.2 podMonitor · Trino chart 1.42.2 `jmx.exporter` (varsayılan `bitnamilegacy/jmx-exporter:1.4.0` — F6 aynası) · CNPG `plugin-barman-cloud` eklenti **v0.15.0** = chart `cnpg/plugin-barman-cloud` **0.8.0** (Task 3 canlı düzeltmesi: 0.15.0 appVersion'dır, chart sürümü DEĞİL) · Velero chart **12.1.0** (Velero 1.18.1) + `velero-plugin-for-aws` (sürüm kurulumda `gh release view` ile sabitlenir) · dbt-trino **1.10.4** (resmi imaj YOK → `python:3.13-slim` CronJob + pip, referans örnek) · Grafana dashboard JSON'ları: Strimzi `strimzi-kafka.json`, `strimzi-kafka-connect.json` (repo dosyası), Trino community 20208 (indirilir, dosya)

**Spec:** `docs/specs/2026-09-10-lakehouse-v2-design.md` §4 (runbook listesi), §5.5 (S3 örneği), §8 (izleme/DR/güvenlik), §9 (test), §12–14 · **Bulgular:** `docs/plans/2026-09-10-f0-findings.md` F2/F3/F4 notları (Maven egress, dev MinIO PVC'siz, vended cred 403, CI CPU bütçesi: yeşil koşuda cpu 2610m/4000m Spark öncesi, Spark driver+executor 500m+500m → ~390m marj; 4 vCPU/16 GB runner) · **Backlog (F4 notu "Açık kalanlar")**: rewrite_manifests, dbt-trino runbook/örnek, dev MinIO PVC, tpch/tpcds, `run_check_job`≈`verify`, dev netpol açık + e2e kanıtı, jupyterhub e2e 409/servis adı, Superset imaj sabitleme (digest alanı YOK → tag + ayna), Superset migrate DB kapısı (belgeli, F6), PDB (pre-ship).

## Global Constraints

- Özel imaj YOK, hack YOK, runtime mutasyon YOK. Kod: `glue/jobs/iceberg_maintenance.py` (rewrite_manifests), `glue/jobs/s3_register_example.py` (referans), e2e script'leri, `runbooks/scripts/acceptance.sh`. Diğer her şey upstream chart/operator + values ya da `glue` şablonu/dosyası.
- Sürümler sabit: kube-prometheus-stack `91.4.1`; plugin-barman-cloud chart `0.8.0` (eklenti v0.15.0); Velero chart `12.1.0`; velero-plugin-for-aws → implementer `gh release view --repo vmware-tanzu/velero-plugin-for-aws` ile Velero 1.18 uyumlu son sürümü sabitler ve F5 notuna yazar; dbt-trino `1.10.4`; Grafana dashboard dosyaları Strimzi `1.2.0` etiketinden.
- Namespace'ler: izleme `monitoring` (dev), Velero `velero` (dev); CNPG plugin `cnpg-system`. `lakehouse` NetworkPolicy `allow-platform-namespaces` `monitoring`'e izin veriyor; `velero` eklenir.
- **CPU bütçesi (CI 4 vCPU):** yeni dev bileşenlerin toplam CPU isteği ≤ 200m: prometheus-operator 10m, Prometheus 50m, kube-state-metrics 10m, Grafana 20m, Velero 20m, node-agent 20m, barman plugin 10m, geçici restore CNPG 100m (e2e sonunda silinir). Her Task'ın canlı adımı `kubectl describe node | sed -n '/Allocated/,/Events/p'` ile CPU isteklerini raporlar; Spark koşusu öncesi > 3400m ise istekler düşürülür (Grafana dev'de kapatılabilir).
- Prometheus nesneleri `monitoring.coreos.com/v1` (`PodMonitor`, `ServiceMonitor`, `PrometheusRule`), Velero `velero.io/v1` (`Schedule`), Barman `barmancloud.cnpg.io/v1` (`ObjectStore`), CNPG `postgresql.cnpg.io/v1` (`ScheduledBackup`) — hepsi `glue` şablonları, `monitoring.enabled` / `backup.enabled` / `velero.enabled` bayraklarıyla (varsayılan true; unittest'te render kontrolü).
- kube-prometheus-stack dev values: `prometheus.prometheusSpec.serviceMonitorSelectorNilUsesHelmValues: false`, `podMonitorSelectorNilUsesHelmValues: false`, `ruleSelectorNilUsesHelmValues: false` (aksi hâlde `lakehouse` ns'indeki nesneler görülmez — doğrulanmış tuzak).
- Yedek hedefi **farklı bucket**: dev MinIO `backups` bucket'ı (aynı endpoint; prod farklı endpoint values'tan), Secret `backup-s3-creds` (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`; dev'de MinIO şablonu üretir, prod kurulumdan önce).
- e2e: yeni yollar `monitoring-path.sh`, `dr-path.sh`; `run.sh` sırası: F1 smoke → pg → mongo → nginx → trino → superset → jupyterhub → zeppelin → **monitoring → dr**. Loud assert biçimi (`|| { echo HATA; exit 1; }`). CI `timeout-minutes: 90` kalır (hedef ≤ 60 dk).
- Commit sonu: `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`. Dal `v2`. Shell komutlarında `--verify` / `--no-verify` metni kullanılmaz (hook). Türkçe yorum/belge.

## Spec'e göre kararlar (plan yazarının ruling'leri — upstream 2026-09-18 doğrulamalarına dayanır)

| # | Karar | Gerekçe |
|---|---|---|
| R1 | Dev izleme yığını: kube-prometheus-stack minimal (Grafana açık, küçük; Alertmanager/node-exporter/kube-* scrape kapalı), yalnız dev overlay'de; prod: OpenShift user-workload monitoring | Spec §8 "platformun"; PodMonitor/ServiceMonitor/PrometheusRule her iki tarafta aynı API. Grafana dev'de açık: dashboard ConfigMap'lerinin yüklendiği e2e'de kanıtlanır (`/api/search`) |
| R2 | Spark durum metrikleri **kube-state-metrics customResourceState** (SparkApplication `status.applicationState.state` StateSet + `status.terminationTime` gauge; SSA `status.lastRun`), spark-operator'ün kendi sayaçları ek | Operator metrikleri monoton sayaç (yaş ifade edemez); KSM CR durumu doğrudan zaman damgası verir → "son başarı yaşı > 45 dk" kuralı |
| R3 | Sink "stalled" kuralı Kafka Connect'in genel `sink-task-metrics`'inden (`sink_record_active_count > 0` ve 15 dk'da `offset_commit_completion_total` artışı 0); Iceberg'e özgü JMX metriği varsayılmaz | Iceberg sink 1.11 kendi JMX'i doğrulanamadı; Strimzi stok connect ConfigMap kuralı `sink-task-metrics`'i zaten dışa verir. Canlı metrik adı doğrulanır (F0 KARAR §5) |
| R4 | CNPG yedek: **Barman Cloud plugin** (ObjectStore CR), in-tree `barmanObjectStore` değil | In-tree 1.26'dan beri deprecated, 1.31'de kalkıyor; şimdi plugin = bir sürüm sonra zorunlu göç yok. Ek bileşen: plugin Deployment (cnpg-system) |
| R5 | DR provası e2e'de gerçek: `ScheduledBackup` (`immediate: true`) → `Backup` completed → `polaris-db-restore` Cluster `bootstrap.recovery` (ObjectStore'dan) → psql ile tablo sayısı > 0 → restore cluster silinir | Şartname G.6 kabul-kritik; PITR yolu (WAL arşivi + recovery) tek sefer canlı kanıtlanmalı |
| R6 | Velero: dev/vanilla chart `velero` ns, MinIO BSL (`backups` bucket, `velero/` prefix), node-agent + `defaultVolumesToFsBackup: true`; Kafka ve CNPG pod'ları hacim yedeğinden **dışlanır** (Strimzi `template.pod.metadata.annotations` / CNPG `inheritedMetadata.annotations` → `backup.velero.io/backup-volumes-excludes`); OpenShift: OADP `DataProtectionApplication` runbook; `Schedule` CR glue'da (aynı API) | Kafka verisi yeniden akıtılabilir/MM2 (Strimzi'nin resmi yedek hikâyesi yok), CNPG kendi PITR'ı; Velero namespace tanımları + not defteri PVC'leri (zeppelin-data, hub-db-dir, jupyter kullanıcı PVC'leri) için |
| R7 | Velero e2e provası: `Backup` CR (Schedule şablonundan) Completed + yedek öncesi yaratılan glue-dışı `e2e-dr-marker` ConfigMap silinip `Restore` ile geri gelmesi; PVC fs-restore provası runbook'ta (zaman) | 5 dk bütçe; restore hattı kanıtlanır; glue-yönetimli nesne seçilmez (ArgoCD selfHeal yarışı) |
| R8 | dbt: resmi dbt-trino imajı YOK → referans örnek `runbooks/dbt/` (dbt_project.yml, profiles.yml, 1 Gold model, CronJob manifesti `python:3.13-slim` + `pip install dbt-trino==1.10.4`); chart bileşeni değil (spec §14) | Özel imaj yasak; şartname "referans örnek" ister; pip-at-start bir CronJob örneğinde kabul edilebilir, belgelenir |
| R9 | Maven/PyPI egress: `spark.ivySettingsXml` opsiyonel values (ConfigMap → `spark.jars.ivySettings`) + runbook (iç Maven aynası); PVC ısıtma yolu doğrulanamadı → uygulanmaz | Offline `--packages` davranışı Spark'ta belgeli değil; ivysettings tek doğrulanmış yol |
| R10 | Polaris vended-cred 403: kod değişikliği yok; `troubleshooting.md`'de "VM uyku/saat sıçraması → Polaris önbelleği (1800 s) vs STS süresi (3600 s)" + çözüm: `STORAGE_CREDENTIAL_CACHE_DURATION_SECONDS` düşürme ya da `s3.vendedCredentials=false` | Dev'e özgü; prod STS'siz modda yok |
| R11 | Superset imajı: operator `spec.image` yalnız `repository:tag` (digest alanı yok) → `versions.md`'de "tag + müşteri aynası (registry'de tag immutability)" | Kaynak kodda `fmt.Sprintf("%s:%s")` doğrulandı |
| R12 | Dev NetworkPolicy açılır (`networkPolicy.enabled: true`) ve tüm e2e yolları politika altında geçer; `allow-platform-namespaces`'a `velero` eklenir | kindnetd enforce ediyor (F4 bulgu 10); F1'den beri hiç canlı doğrulanmamıştı |
| R13 | Trino JMX exporter chart varsayılan imajı `bitnamilegacy/jmx-exporter:1.4.0` ile açılır; `versions.md`'de "bitnamilegacy — F6/pre-ship aynası" işaretlenir | Sorgu hata/çalışan sorgu metrikleri değerli; chart'ın tek yolu |
| R14 | Kabul betiği `runbooks/scripts/acceptance.sh` = mevcut kümeye fixture'ları uygulayıp e2e yol script'lerini sırayla koşturur (kind/bootstrap adımları yok); `acceptance-tests.md` şartname maddesi ↔ kanıt komutu tablosu | Yeni kod yerine kanıtlanmış e2e yolları; F6 kabul demosunun v2 uyarlaması bunun üstüne kurulur |
| R15 | `troubleshooting.md` yeni dosya: `install.md`'deki "Sorun giderme" ve F4 troubleshooting bölümleri buraya taşınır (install.md'de bağlantı kalır) | Spec §4 runbook listesi; tek yer |
| R16 | Spec §8 metni bu kararlarla güncellenir (KSM, Barman plugin, Velero dışlamalar) | Spec otoriter |

## Kapsam değişikliği (kullanıcı kararı, 2026-09-18 — Task 2 sonrası)
İzleme yalnız **boru hattı sağlığı**: Kafka/Connect (Strimzi JMX + **kafkaExporter** consumer lag), Spark işleri (spark-operator süre/sayaç + KSM durum), Polaris mgmt. Gösterim bileşenlerinin uygulama metriği (Trino JMX exporter, JupyterHub hub, Zeppelin, Superset) **toplanmaz** — Task 1/2'de eklenenler Task 2b'de kaldırılır; `LakehouseSinkStalled` lag tabanlı olur; `LakehouseSparkRunTooLong` eklenir (5 kural). Tablo düzeyi veri metrikleri için upstream exporter yok → Trino Iceberg metadata tabloları + Superset dashboard (F6 kabul demosu); prod depolama (FlashBlade) metrikleri platform ekibinin exporter'ı. Ledger: `.superpowers/sdd/…/task-2b-brief.md`.

---

## Dosya yapısı

```
platform/apps/40-monitoring.yaml              Application: kube-prometheus-stack 91.4.1 (ns monitoring) — YALNIZ platform/envs/dev/kustomization.yaml resources'ında
platform/apps/40-velero.yaml                  Application: velero 12.1.0 (ns velero) — yalnız dev overlay
platform/apps/00-cnpg-barman.yaml             Application: cnpg/plugin-barman-cloud chart 0.8.0 = eklenti v0.15.0 (ns cnpg-system) — base (prod dahil), wave 0
platform/values/{monitoring-dev,velero-dev}.yaml
platform/envs/dev/kustomization.yaml          + ../../apps/40-monitoring.yaml, ../../apps/40-velero.yaml (resources) — base'e girmez
bootstrap/bootstrap.sh                        helm modu: plugin-barman-cloud, (dev) kube-prometheus-stack, velero; kök patch'ler monitoring/velero
glue/files/metrics/{kafka-metrics.yml,kafka-connect-metrics.yml}    Strimzi 1.2.0 examples/metrics ConfigMap içerikleri (dosya)
glue/files/dashboards/{README.md,strimzi-kafka.json,strimzi-kafka-connect.json,trino-20208.json}
glue/files/ksm-custom-resource-state.yaml     kube-state-metrics CustomResourceStateMetrics (SparkApplication, ScheduledSparkApplication)
glue/templates/monitoring.yaml                ConfigMap'ler (metrics rules), PodMonitor kafka-resources, ServiceMonitor'ler (zeppelin, hub), PrometheusRule lakehouse (4 kural), Grafana dashboard ConfigMap'leri (grafana_dashboard=1)
glue/templates/kafka.yaml, kafka-connect.yaml metricsConfig + Velero dışlama annotation'ı
glue/templates/cnpg.yaml                      3 Cluster'a plugins (barman-cloud, isWALArchiver) + inheritedMetadata annotation; ScheduledBackup ×3; ObjectStore lakehouse-backups
glue/templates/backup.yaml                    Velero Schedule (namespace, daily, fs-backup)
glue/templates/minio.yaml                     dev: PVC (5Gi) + backups bucket + backup-s3-creds Secret; dev-secrets.yaml: velero-s3-creds (ns velero)
glue/templates/zeppelin.yaml                  ZEPPELIN metrics env; superset.yaml spec.monitoring.serviceMonitor
glue/templates/networkpolicy.yaml             velero ns
glue/templates/spark-jobs.yaml                opsiyonel ivySettings ConfigMap mount
glue/jobs/iceberg_maintenance.py              compact modunda rewrite_manifests
glue/jobs/s3_register_example.py              S3 prefix → Iceberg CTAS referans örneği
glue/values.yaml                              monitoring.*, backup.*, velero.*, spark.ivySettingsXml, minio.persistence
platform/values/glue.yaml, glue-dev.yaml      prod/dev (dev: networkPolicy.enabled true)
platform/values/trino.yaml, trino-dev.yaml    jmx.enabled/exporter/serviceMonitor; catalogs tpch/tpcds null
platform/values/jupyterhub*.yaml              hub metrics, e2e servis adı e2e-admin
glue/tests/{monitoring_test,backup_test,...}.yaml
test/e2e/{monitoring-path.sh,dr-path.sh,cnpg-restore.yaml,velero-backup.yaml,velero-restore.yaml}, lib.sh (verify → run_check_job üstüne), jupyterhub-path.sh (409/e2e-admin), run.sh
runbooks/{versions.md,upgrade.md,dr.md,troubleshooting.md,acceptance-tests.md,s3-register.md}, runbooks/dbt/{README.md,dbt_project.yml,profiles.yml,models/gold/orders_daily.sql,cronjob.yaml}, runbooks/scripts/acceptance.sh
docs/specs/… §8, README, docs/plans/2026-09-10-f0-findings.md "F5 notu"
.github/workflows/e2e.yaml                    teşhis: prometheus targets/rules, backups, velero; py_compile; dev render'lar
```

---

### Task 1: İzleme yığını (dev) + metrik kaynakları + PodMonitor/ServiceMonitor'ler + e2e monitoring-path (hedefler)

**Files:**
- Create: `platform/apps/40-monitoring.yaml`, `platform/values/monitoring-dev.yaml`, `glue/files/metrics/kafka-metrics.yml`, `glue/files/metrics/kafka-connect-metrics.yml`, `glue/files/ksm-custom-resource-state.yaml`, `glue/templates/monitoring.yaml`, `glue/tests/monitoring_test.yaml`, `test/e2e/monitoring-path.sh`
- Modify: `platform/envs/dev/kustomization.yaml`, `bootstrap/bootstrap.sh`, `glue/templates/kafka.yaml`, `glue/templates/kafka-connect.yaml`, `glue/templates/zeppelin.yaml`, `glue/templates/superset.yaml`, `glue/values.yaml`, `platform/values/glue-dev.yaml`, `platform/values/trino.yaml`, `platform/values/trino-dev.yaml`, `platform/values/jupyterhub.yaml`, `platform/apps/00-spark-operator.yaml`, `test/e2e/run.sh`

**Interfaces:**
- Produces: Prometheus `http://monitoring-kube-prometheus-prometheus.monitoring.svc:9090` (release adı `monitoring`; Service adı canlı doğrulanır), Grafana `monitoring-grafana` (Secret `monitoring-grafana`, key `admin-password`); metrikler: `kafka_connect_connector_task_status{connector,task,status}`, `kafka_connect_sink_task_*` (sanitize edilmiş adlar canlı yazılır), `spark_application_*` (operator), KSM: `kube_customresource_sparkapp_state{name,state}`, `kube_customresource_sparkapp_termination_time{name}`, `kube_customresource_ssa_last_run{name}`; Trino `trino_*` (jmx exporter kurallarından), Superset ServiceMonitor, hub `/hub/metrics`, Zeppelin `/metrics`.

- [ ] **Step 1: kube-prometheus-stack Application (dev overlay) + values**

`platform/apps/40-monitoring.yaml` (kalıp: 30-trino, chart + `$values`):
```yaml
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata: {name: monitoring, namespace: argocd, annotations: {argocd.argoproj.io/sync-wave: "0"}}
spec:
  project: default
  sources:
  - repoURL: https://prometheus-community.github.io/helm-charts
    chart: kube-prometheus-stack
    targetRevision: 91.4.1
    helm:
      valueFiles: [$values/platform/values/monitoring-dev.yaml]
  - repoURL: https://github.com/suhanduman/lakehouse.git
    targetRevision: v2
    ref: values
  destination: {server: https://kubernetes.default.svc, namespace: monitoring}
  syncPolicy:
    automated: {prune: true, selfHeal: true}
    syncOptions: [CreateNamespace=true, ServerSideApply=true]
```
`platform/envs/dev/kustomization.yaml` `resources: [../../apps, ../../apps/40-monitoring.yaml]` — `platform/apps/kustomization.yaml` base listesine **eklenmez** (prod'da kurulmaz). `bootstrap.sh` kök patch'ine `monitoring` (`/spec/sources/1/repoURL`, `targetRevision`).
`platform/values/monitoring-dev.yaml`:
```yaml
# DEV/vanilla izleme: yalnız operator + küçük Prometheus + kube-state-metrics (customResourceState) + küçük Grafana.
# OpenShift'te KURULMAZ: user-workload monitoring lakehouse ns'indeki PodMonitor/ServiceMonitor/PrometheusRule'ları alır.
alertmanager: {enabled: false}
prometheus-node-exporter: {enabled: false}
nodeExporter: {enabled: false}
kubeApiServer: {enabled: false}
kubelet: {enabled: false}
kubeControllerManager: {enabled: false}
coreDns: {enabled: false}
kubeEtcd: {enabled: false}
kubeScheduler: {enabled: false}
kubeProxy: {enabled: false}
kubeStateMetrics: {enabled: true}
kube-state-metrics:
  resources: {requests: {cpu: 10m, memory: 64Mi}, limits: {memory: 256Mi}}
  rbac:
    extraRules:
    - {apiGroups: [sparkoperator.k8s.io], resources: [sparkapplications, scheduledsparkapplications], verbs: [list, watch]}
  # customResourceState: kube-state-metrics chart'ının `customResourceState: {enabled: true, config: {...}}` anahtarı varsa
  # (helm show values prometheus-community/kube-state-metrics ile doğrula) glue/files/ksm-custom-resource-state.yaml içeriği buraya gömülür;
  # yoksa extraArgs + ConfigMap mount (glue monitoring.yaml ns monitoring'e ConfigMap render eder).
prometheusOperator:
  resources: {requests: {cpu: 10m, memory: 64Mi}, limits: {memory: 256Mi}}
prometheus:
  prometheusSpec:
    retention: 24h
    resources: {requests: {cpu: 50m, memory: 400Mi}, limits: {memory: 1Gi}}
    serviceMonitorSelectorNilUsesHelmValues: false     # lakehouse ns'indeki ServiceMonitor'ler görülsün (tuzak!)
    podMonitorSelectorNilUsesHelmValues: false
    ruleSelectorNilUsesHelmValues: false
    serviceMonitorNamespaceSelector: {}
    podMonitorNamespaceSelector: {}
    ruleNamespaceSelector: {}
    podMetadata: {labels: {hub.jupyter.org/network-access-hub: "true"}}   # z2jh hub netpol: Prometheus hub'a erişebilsin
grafana:
  enabled: true
  resources: {requests: {cpu: 20m, memory: 128Mi}, limits: {memory: 384Mi}}
  sidecar: {dashboards: {enabled: true, searchNamespace: ALL, label: grafana_dashboard}}
  defaultDashboardsEnabled: false
```
`bootstrap.sh` helm modu (yalnız `ENV == dev`, glue'dan ÖNCE — CRD'ler): `helm repo add prometheus-community https://prometheus-community.github.io/helm-charts`; `helm upgrade --install monitoring prometheus-community/kube-prometheus-stack --version 91.4.1 -n monitoring --create-namespace -f platform/values/monitoring-dev.yaml --wait --timeout 10m`.

- [ ] **Step 2: Strimzi metrikleri + PodMonitor**

`glue/files/metrics/kafka-metrics.yml` ve `kafka-connect-metrics.yml`: Strimzi `1.2.0` etiketinden `examples/metrics/kafka-metrics.yaml` (`data.kafka-metrics-config.yml`) ve `examples/metrics/kafka-connect-metrics.yaml` (`data.metrics-config.yml`) ConfigMap içerikleri (yalnız kural gövdesi; dosya başında kaynak URL + etiket yorumu). `glue/templates/monitoring.yaml`:
```yaml
{{- if .Values.monitoring.enabled }}
{{- $ns := include "glue.ns" . }}
apiVersion: v1
kind: ConfigMap
metadata: {name: kafka-metrics, namespace: {{ $ns }}}
data:
  kafka-metrics-config.yml: |
{{ .Files.Get "files/metrics/kafka-metrics.yml" | indent 4 }}
---
apiVersion: v1
kind: ConfigMap
metadata: {name: connect-metrics, namespace: {{ $ns }}}
data:
  metrics-config.yml: |
{{ .Files.Get "files/metrics/kafka-connect-metrics.yml" | indent 4 }}
---
# Strimzi examples/metrics/prometheus-install/pod-monitors/kafka-resources-metrics.yaml (Kafka + KafkaConnect tek PodMonitor)
apiVersion: monitoring.coreos.com/v1
kind: PodMonitor
metadata: {name: kafka-resources-metrics, namespace: {{ $ns }}, labels: {app: strimzi}}
spec:
  selector: {matchExpressions: [{key: strimzi.io/kind, operator: In, values: [Kafka, KafkaConnect]}]}
  namespaceSelector: {matchNames: [{{ $ns }}]}
  podMetricsEndpoints:
  - path: /metrics
    port: tcp-prometheus
    relabelings:
    - {separator: ";", regex: __meta_kubernetes_pod_label_(strimzi_io_.+), replacement: $1, action: labelmap}
    - {sourceLabels: [__meta_kubernetes_namespace], separator: ";", regex: (.*), targetLabel: namespace, replacement: $1, action: replace}
    - {sourceLabels: [__meta_kubernetes_pod_name], separator: ";", regex: (.*), targetLabel: kubernetes_pod_name, replacement: $1, action: replace}
{{- end }}
```
`kafka.yaml` `spec.kafka` altına (`monitoring.enabled` koşullu): `metricsConfig: {type: jmxPrometheusExporter, valueFrom: {configMapKeyRef: {name: kafka-metrics, key: kafka-metrics-config.yml}}}`; `kafka-connect.yaml` `spec` altına aynı kalıp (`connect-metrics`, `metrics-config.yml`).

- [ ] **Step 3: spark-operator podMonitor + kube-state-metrics customResourceState**

`platform/apps/00-spark-operator.yaml` values: `prometheus: {metrics: {enable: true, port: 8080}, podMonitor: {create: true}}`; bootstrap helm modu `--set prometheus.podMonitor.create=true` (yalnız dev; PodMonitor CRD spark-operator'den önce → monitoring kurulumu daha önce; prod OpenShift'te CRD var, prod values `podMonitor.create: true` kalır).
`glue/files/ksm-custom-resource-state.yaml` (alan yolları `kubectl explain sparkapplication.status --recursive` / `scheduledsparkapplication.status` ile canlı doğrulanır):
```yaml
kind: CustomResourceStateMetrics
spec:
  resources:
  - groupVersionKind: {group: sparkoperator.k8s.io, version: v1beta2, kind: SparkApplication}
    metricNamePrefix: kube_customresource_sparkapp
    labelsFromPath: {name: [metadata, name], namespace: [metadata, namespace]}
    metrics:
    - name: state
      help: SparkApplication applicationState.state
      each: {type: StateSet, stateSet: {labelName: state, path: [status, applicationState, state], list: [SUBMITTED, RUNNING, COMPLETED, FAILED, SUBMISSION_FAILED, PENDING_RERUN, INVALIDATING, SUCCEEDING, FAILING, UNKNOWN]}}
    - name: termination_time
      help: SparkApplication terminationTime (epoch s)
      each: {type: Gauge, gauge: {path: [status, terminationTime]}}
  - groupVersionKind: {group: sparkoperator.k8s.io, version: v1beta2, kind: ScheduledSparkApplication}
    metricNamePrefix: kube_customresource_ssa
    labelsFromPath: {name: [metadata, name], namespace: [metadata, namespace]}
    metrics:
    - name: last_run
      help: ScheduledSparkApplication lastRun (epoch s)
      each: {type: Gauge, gauge: {path: [status, lastRun]}}
```
(KSM `gauge.path` RFC3339 zaman damgalarını epoch saniyeye çevirir — canlı doğrula: `kube_customresource_sparkapp_termination_time` değeri `time()`'a yakın olmalı.)

- [ ] **Step 4: Trino / Superset / hub / Zeppelin metrikleri**

`platform/values/trino.yaml`: `jmx: {enabled: true, exporter: {enabled: true, configProperties: |  … }}` — jmx_exporter YAML `rules`: `trino.execution:name=QueryManager` (`RunningQueries`, `QueuedQueries`, `FailedQueries.OneMinute.Count`, `CompletedQueries.OneMinute.Count`) → `trino_execution_QueryManager_<attr>`; `trino.memory:name=ClusterMemoryManager` (`ClusterMemoryBytes`); `serviceMonitor: {enabled: true}`. Superset CR: `spec.monitoring: {serviceMonitor: {interval: 30s}}` (`monitoring.enabled` koşullu). z2jh `platform/values/jupyterhub.yaml`: `hub.config.JupyterHub.authenticate_prometheus: false`; prod için `hub.networkPolicy.ingress: [{from: [{namespaceSelector: {matchLabels: {kubernetes.io/metadata.name: openshift-user-workload-monitoring}}}]}]` (dev: Prometheus pod label'ı Step 1). glue `monitoring.yaml`'a ServiceMonitor `hub` (Service `hub`, port adı chart'tan — `kubectl get svc hub -o yaml`; path `/hub/metrics`) ve `zeppelin` (port `http`, path `/metrics`); `zeppelin.yaml` env `ZEPPELIN_METRIC_ENABLE_PROMETHEUS: "true"` (env↔property eşlemesi canlı doğrulanır: `curl zeppelin:8080/metrics`; çalışmıyorsa `zeppelin-site.xml` ConfigMap subPath ile `zeppelin.metric.enable.prometheus=true`).

- [ ] **Step 5: values + unittest**

`glue/values.yaml`:
```yaml
monitoring:
  enabled: true                 # PodMonitor/ServiceMonitor/PrometheusRule/Grafana ConfigMap'leri (CRD'ler: dev kube-prometheus-stack, OpenShift platform)
  namespace: monitoring         # dev Prometheus/Grafana ns'i
  silverMergeStaleSeconds: 2700 # LakehouseSilverMergeStale eşiği (prod 15 dk cron -> 45 dk); dev gecelik cron -> glue-dev 172800
```
`glue/tests/monitoring_test.yaml`: PodMonitor selector `[Kafka, KafkaConnect]`; Kafka `spec.kafka.metricsConfig.valueFrom.configMapKeyRef.name == kafka-metrics`; KafkaConnect `metricsConfig` key `metrics-config.yml`; `monitoring.enabled=false` → monitoring.yaml 0 doküman ve `metricsConfig` yok; ServiceMonitor `hub` path `/hub/metrics`; Superset CR `spec.monitoring.serviceMonitor.interval == 30s`.

- [ ] **Step 6: e2e monitoring-path.sh (hedefler)**

```bash
#!/usr/bin/env bash
# e2e F5 izleme yolu (1/2): Prometheus hedefleri up, metrik adları mevcut. Kurallar/dashboard'lar Task 2'de eklenir.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"; NS=lakehouse; MON="${MON_NS:-monitoring}"
kubectl -n "$MON" rollout status statefulset/prometheus-monitoring-kube-prometheus-prometheus --timeout=600s   # ad canlı doğrulanır
kubectl -n "$MON" port-forward svc/monitoring-kube-prometheus-prometheus 19090:9090 >/dev/null 2>&1 & PF=$!; trap 'kill $PF 2>/dev/null' EXIT; sleep 3
Q() { curl -sS "localhost:19090/api/v1/query" --data-urlencode "query=$1" | jq -r '.data.result | length'; }
UP() { local job="$1" n; for _ in $(seq 1 30); do n=$(Q "up{job=~\"$job\"} == 1"); [[ "$n" -ge 1 ]] && { echo "OK up $job ($n)"; return; }; sleep 10; done; echo "HATA up $job"; exit 1; }
UP ".*kafka-resources-metrics.*"
UP ".*spark-operator.*"
UP ".*trino.*"
UP ".*superset.*"
UP ".*hub.*"
UP ".*zeppelin.*"
UP ".*kube-state-metrics.*"
M() { local q="$1" n; n=$(Q "$q"); [[ "$n" -ge 1 ]] || { echo "HATA metrik yok: $q"; exit 1; }; echo "OK metrik $q ($n seri)"; }
M 'kafka_connect_connector_task_status{status="running"}'
M 'kafka_connect_sink_task_offset_commit_completion_total'      # sanitize edilmiş ad canlı doğrulanır; farklıysa script + F5 notu güncellenir
M 'kube_customresource_sparkapp_state{state="COMPLETED"}'
M 'kube_customresource_ssa_last_run'
M 'spark_application_success_count'
M 'trino_execution_QueryManager_RunningQueries'
echo "E2E F5 MONITORING OK"
```
`run.sh`: zeppelin-path'ten sonra `monitoring-path.sh`; ArgoCD app döngüsüne `monitoring` (`wait_app "$app" 0`; yalnız dev overlay'de var — helm modunda döngü koşmaz).

- [ ] **Step 7: Canlı doğrulama** — mevcut kind kümesi (F4 sonundan): `bootstrap.sh --env dev --mode helm` (monitoring kurulur, glue upgrade — Kafka/Connect metricsConfig rolling restart), `kubectl -n monitoring get pods`, `monitoring-path.sh`. Her metrik adı için `curl localhost:19090/api/v1/label/__name__/values | jq -r '.data[]' | grep -E 'kafka_connect_sink|kube_customresource|trino_'` çıktısından gerçek adı al; script'i düzelt; F5 notu tablosu için kaydet. CPU: `kubectl describe node | sed -n '/Allocated/,/Events/p'`.

- [ ] **Step 8: Commit** — `feat(monitoring): kube-prometheus-stack (dev) + Strimzi/spark-operator/Trino/Superset/hub/Zeppelin metrics, KSM Spark CR state; e2e monitoring-path (targets)`.

---

### Task 2: PrometheusRule (4 kural) + Grafana dashboard ConfigMap'leri + e2e kural/dashboard doğrulaması

**Files:**
- Create: `glue/files/dashboards/README.md`, `glue/files/dashboards/strimzi-kafka.json`, `glue/files/dashboards/strimzi-kafka-connect.json`, `glue/files/dashboards/trino-20208.json`
- Modify: `glue/templates/monitoring.yaml`, `glue/tests/monitoring_test.yaml`, `test/e2e/monitoring-path.sh`, `glue/values.yaml`, `platform/values/glue-dev.yaml`

**Interfaces:**
- Consumes: Task 1 metrik adları (canlı doğrulanmış hâlleri).
- Produces: `PrometheusRule/lakehouse` (group `lakehouse`): `LakehouseConnectTaskFailed`, `LakehouseSinkStalled`, `LakehouseSilverMergeStale`, `LakehouseSparkScheduledRunFailed`; ConfigMap'ler `grafana-dashboard-{strimzi-kafka,strimzi-kafka-connect,trino}` (`grafana_dashboard: "1"`).

- [ ] **Step 1: PrometheusRule** (`monitoring.yaml`'a; Helm içinde `{{ $labels }}` kaçışı `{{ "{{ $labels.x }}" }}`)

```yaml
---
apiVersion: monitoring.coreos.com/v1
kind: PrometheusRule
metadata: {name: lakehouse, namespace: {{ $ns }}}
spec:
  groups:
  - name: lakehouse
    rules:
    - alert: LakehouseConnectTaskFailed
      expr: kafka_connect_connector_task_status{status="failed"} == 1
      for: 5m
      labels: {severity: critical}
      annotations: {summary: "Connect task FAILED: {{ "{{ $labels.connector }}" }}/{{ "{{ $labels.task }}" }}", runbook: runbooks/troubleshooting.md#connect}
    - alert: LakehouseSinkStalled
      # aktif kayıt var ama 15 dk'da offset commit yok (Iceberg sink upstream 300 s commit aralığı)
      expr: sum by (connector) (kafka_connect_sink_task_sink_record_active_count) > 0 and sum by (connector) (increase(kafka_connect_sink_task_offset_commit_completion_total[15m])) == 0
      for: 5m
      labels: {severity: warning}
      annotations: {summary: "Iceberg sink commit yok: {{ "{{ $labels.connector }}" }}", runbook: runbooks/troubleshooting.md#sink}
    - alert: LakehouseSilverMergeStale
      # en son COMPLETED silver-merge koşusundan bu yana > eşik (prod 45 dk; dev gecelik cron -> 2 gün)
      expr: time() - max(kube_customresource_sparkapp_termination_time{name=~"silver-merge-.*"} and on (name) kube_customresource_sparkapp_state{state="COMPLETED"} == 1) > {{ .Values.monitoring.silverMergeStaleSeconds }}
      for: 10m
      labels: {severity: warning}
      annotations: {summary: "silver-merge son başarı yaşı eşiği aştı", runbook: runbooks/troubleshooting.md#silver-merge}
    - alert: LakehouseSparkScheduledRunFailed
      expr: kube_customresource_sparkapp_state{state="FAILED", name=~"(silver-merge|maint-.*|mongo-bronze)-[0-9]+"} == 1
      for: 1m
      labels: {severity: warning}
      annotations: {summary: "Zamanlanmış Spark koşusu FAILED: {{ "{{ $labels.name }}" }}", runbook: runbooks/troubleshooting.md#spark}
```
`platform/values/glue-dev.yaml`: `monitoring: {silverMergeStaleSeconds: 172800}`.

- [ ] **Step 2: Dashboard dosyaları** — Strimzi `1.2.0` etiketinden `examples/metrics/grafana-dashboards/strimzi-kafka.json` ve `strimzi-kafka-connect.json` (curl; dosya olarak commit); Trino: grafana.com 20208 (`https://grafana.com/api/dashboards/20208/revisions/latest/download`); `glue/files/dashboards/README.md`: kaynak/etiket/lisans tablosu + `${DS_PROMETHEUS}` datasource notu (sidecar `datasource` girdisi). `monitoring.yaml`: 3 ConfigMap `grafana-dashboard-*` (`labels: {grafana_dashboard: "1"}`, `data: {<ad>.json: {{ .Files.Get … | quote }}}`; boyut < 1 MiB).

- [ ] **Step 3: unittest** — PrometheusRule 4 kural adı; dev değerle `> 172800` render; dashboard ConfigMap'lerinde label ve `.json` anahtarı; `monitoring.enabled=false` → yok.

- [ ] **Step 4: e2e (monitoring-path.sh'a ekle)**

```bash
echo "== kurallar"
n=$(curl -sS localhost:19090/api/v1/rules | jq '[.data.groups[] | select(.name=="lakehouse") | .rules[]] | length'); [[ "$n" == "4" ]] || { echo "HATA kural sayısı $n"; exit 1; }; echo "OK 4 kural yüklü"
for a in LakehouseConnectTaskFailed LakehouseSinkStalled LakehouseSilverMergeStale LakehouseSparkScheduledRunFailed; do
  st=$(curl -sS localhost:19090/api/v1/rules | jq -r ".data.groups[].rules[] | select(.name==\"$a\") | .health"); [[ "$st" == "ok" ]] || { echo "HATA $a health=$st"; exit 1; }; echo "OK $a health ok"
done
firing=$(curl -sS localhost:19090/api/v1/alerts | jq '[.data.alerts[] | select(.labels.alertname | startswith("Lakehouse")) | select(.state=="firing")] | length')
[[ "$firing" == "0" ]] || { curl -sS localhost:19090/api/v1/alerts | jq '.data.alerts'; echo "HATA ateşlenen Lakehouse alarmı"; exit 1; }; echo "OK ateşlenen alarm yok"
echo "== grafana dashboard'ları"
kubectl -n "$MON" port-forward svc/monitoring-grafana 13000:80 >/dev/null 2>&1 & PF2=$!; sleep 3
GP=$(kubectl -n "$MON" get secret monitoring-grafana -o jsonpath='{.data.admin-password}' | base64 -d)
titles=$(curl -sS -u "admin:$GP" 'localhost:13000/api/search?type=dash-db' | jq -r '.[].title'); kill $PF2
for t in "Strimzi Kafka" "Strimzi Kafka Connect" "Trino"; do grep -qi "$t" <<<"$titles" || { echo "HATA dashboard yok: $t ($titles)"; exit 1; }; echo "OK dashboard $t"; done
```

- [ ] **Step 5: Canlı doğrulama** — glue upgrade; kuralların `health: ok` olması metrik adlarının doğruluğunu (yoksa hata) dolaylı kanıtlar; mevcut kümede önceki FAILED cron koşuları varsa `LakehouseSparkScheduledRunFailed` ateşlenir → bu kümede "0 firing" assert'i kırmızı olabilir: alarm listesini F5 notuna yaz (pozitif kanıt), assert taze kümede (Task 7) koşar.

- [ ] **Step 6: Commit** — `feat(monitoring): 4 PrometheusRule + Strimzi/Trino Grafana dashboards; e2e rules+dashboards`.

---

### Task 3: CNPG PITR yedekleri — Barman Cloud plugin, ObjectStore, ScheduledBackup ×3, restore provası (e2e dr-path 1/2)

**Files:**
- Create: `platform/apps/00-cnpg-barman.yaml`, `test/e2e/dr-path.sh`, `test/e2e/cnpg-restore.yaml`, `glue/tests/backup_test.yaml`
- Modify: `bootstrap/bootstrap.sh`, `platform/apps/kustomization.yaml`, `glue/templates/cnpg.yaml`, `glue/templates/minio.yaml`, `glue/values.yaml`, `platform/values/glue.yaml`, `platform/values/glue-dev.yaml`, `test/e2e/run.sh`, `runbooks/install.md` (Secret listesi)

**Interfaces:**
- Produces: `ObjectStore/lakehouse-backups` (`destinationPath: s3://<bucket>/cnpg/`, `endpointURL`, `s3Credentials` ← Secret `backup-s3-creds`), 3 Cluster `spec.plugins: [{name: barman-cloud.cloudnative-pg.io, isWALArchiver: true, parameters: {barmanObjectName: lakehouse-backups}}]`, `ScheduledBackup/{polaris,keycloak,superset}-db-daily` (`schedule: "0 0 2 * * *"` 6 alan; `method: plugin`, `pluginConfiguration.name: barman-cloud.cloudnative-pg.io`, `immediate: true`, `backupOwnerReference: self`), dev MinIO bucket `backups` + Secret `backup-s3-creds`.

- [ ] **Step 1: Plugin Application + bootstrap** — `platform/apps/00-cnpg-barman.yaml`: chart `plugin-barman-cloud` repo `https://cloudnative-pg.github.io/charts` `0.8.0` (appVersion v0.15.0), ns `cnpg-system`, wave 0, base kustomization (prod dahil; cert-manager gerekir ✓). Helm modu: `helm upgrade --install plugin-barman-cloud cnpg/plugin-barman-cloud --version 0.8.0 -n cnpg-system --wait --timeout 5m` (cnpg'den sonra).
- [ ] **Step 2: values + MinIO** — `glue/values.yaml`:
```yaml
backup:
  enabled: true
  s3:
    endpoint: http://minio.lakehouse.svc:9000     # prod: FARKLI endpoint/bucket (spec §8) — kurulumda doldur
    bucket: backups
    region: us-east-1
    secret: backup-s3-creds                        # keys AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY (dev: MinIO şablonu üretir; prod kurulumdan önce)
  schedule: "0 0 2 * * *"                          # CNPG ScheduledBackup: 6 alan (saniye dahil) -> her gün 02:00
  retentionPolicy: "30d"
```
`minio.yaml`: `minio.buckets: [lakehouse, backups]`, Secret `backup-s3-creds` (root creds, DEV-ONLY), **PVC** `minio.persistence: {enabled: true, size: 5Gi}` (F2/F3 "dev MinIO PVC'siz" kapanır; Deployment `Recreate`).
- [ ] **Step 3: cnpg.yaml** — 3 Cluster'a `{{- if .Values.backup.enabled }}plugins: …{{- end }}` + `inheritedMetadata: {annotations: {backup.velero.io/backup-volumes-excludes: pgdata}}` (Task 4 için; annotation adı Velero belgesinden, CNPG PVC volume adı `pgdata` canlı doğrulanır); `ObjectStore` + 3 `ScheduledBackup` (ObjectStore `retentionPolicy` values'tan).
- [ ] **Step 4: unittest** — `backup_test.yaml`: ObjectStore endpoint/bucket; 3 ScheduledBackup `spec.schedule` 6 alan ve `method: plugin`; Cluster `plugins[0].isWALArchiver == true`; `backup.enabled=false` → yok; MinIO PVC render.
- [ ] **Step 5: e2e dr-path.sh (1/2) + cnpg-restore.yaml**

```bash
#!/usr/bin/env bash
# e2e F5 DR yolu: (1) CNPG Barman plugin: ScheduledBackup immediate -> Backup completed -> restore Cluster -> psql sayım; (2) Velero (Task 4)
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"; NS=lakehouse
echo "== CNPG yedek"
b=""; for _ in $(seq 1 60); do b=$(kubectl -n "$NS" get backup -l cnpg.io/scheduled-backup=polaris-db-daily -o jsonpath='{.items[0].status.phase}' 2>/dev/null || true); [[ "$b" == "completed" ]] && break; sleep 10; done
[[ "$b" == "completed" ]] || { kubectl -n "$NS" get backup -o wide; kubectl -n "$NS" describe backup | tail -30; echo "HATA polaris-db yedeği tamamlanmadı"; exit 1; }; echo "OK polaris-db Backup completed"
c=$(kubectl -n "$NS" get backup -o jsonpath='{range .items[*]}{.status.phase}{"\n"}{end}' | grep -c completed); [[ "$c" -ge 3 ]] || { echo "HATA 3 DB yedeği tamamlanmadı ($c)"; exit 1; }; echo "OK $c yedek completed"
echo "== restore provası (polaris-db-restore)"
kubectl apply -f "$ROOT/test/e2e/cnpg-restore.yaml"
kubectl -n "$NS" wait --for=condition=Ready cluster/polaris-db-restore --timeout=600s
n=$(kubectl -n "$NS" exec polaris-db-restore-1 -c postgres -- psql -U postgres -d polaris -Atc "select count(*) from information_schema.tables where table_schema='public'")
[[ "$n" -gt 0 ]] || { echo "HATA restore DB boş"; exit 1; }; echo "OK restore: polaris şemasında $n tablo"
kubectl -n "$NS" delete cluster polaris-db-restore --wait=false
```
`cnpg-restore.yaml`: Cluster `polaris-db-restore` (instances 1, storage 2Gi, `bootstrap: {recovery: {source: polaris-db}}`, `externalClusters: [{name: polaris-db, plugin: {name: barman-cloud.cloudnative-pg.io, parameters: {barmanObjectName: lakehouse-backups, serverName: polaris-db}}}]`; `plugins` YOK). `run.sh`: monitoring-path'ten sonra `dr-path.sh`; `install.md` Secret listesine `backup-s3-creds`; ArgoCD döngüsüne `cnpg-barman` (chart-only).
- [ ] **Step 6: Canlı doğrulama** — bootstrap helm (plugin) + glue upgrade (Cluster'lara plugin → rolling restart, kabul); `kubectl -n lakehouse get objectstore,scheduledbackup,backup`; `kubectl -n lakehouse get cluster polaris-db -o jsonpath='{.status.conditions[?(@.type=="ContinuousArchiving")].status}'` == True; `dr-path.sh` (CNPG kısmı). Süreleri F5 notuna.
- [ ] **Step 7: Commit** — `feat(dr): CNPG Barman Cloud plugin — ObjectStore (ayrı bucket), WAL arşivi, 3 ScheduledBackup, e2e restore provası; dev MinIO PVC + backups bucket`.

---

### Task 4: Velero — dev chart + Schedule (namespace, fs-backup) + dışlamalar + restore provası (e2e dr-path 2/2)

**Files:**
- Create: `platform/apps/40-velero.yaml`, `platform/values/velero-dev.yaml`, `glue/templates/backup.yaml`, `test/e2e/velero-backup.yaml`, `test/e2e/velero-restore.yaml`
- Modify: `platform/envs/dev/kustomization.yaml`, `bootstrap/bootstrap.sh`, `glue/templates/dev-secrets.yaml`, `glue/templates/kafka.yaml`, `glue/templates/networkpolicy.yaml`, `glue/values.yaml`, `glue/tests/backup_test.yaml`, `test/e2e/dr-path.sh`, `test/e2e/run.sh`

**Interfaces:**
- Produces: `Schedule/lakehouse-daily` (`velero.io/v1`; `schedule: "0 3 * * *"` 5 alan; `template: {includedNamespaces: [lakehouse], defaultVolumesToFsBackup: true, ttl: 720h, excludedResources: [pods, replicasets, events, backups.postgresql.cnpg.io]}`); Strimzi pod annotation `backup.velero.io/backup-volumes-excludes: <kafka volume adı — canlı doğrulanır>`.

- [ ] **Step 1: Velero Application (dev) + values** — chart `velero` repo `https://vmware-tanzu.github.io/helm-charts` `12.1.0`, ns `velero`; `velero-dev.yaml`: `initContainers: [{name: velero-plugin-for-aws, image: velero/velero-plugin-for-aws:<sabit>, volumeMounts: [{mountPath: /target, name: plugins}]}]`, `configuration: {backupStorageLocation: [{name: default, provider: aws, bucket: backups, prefix: velero, config: {region: us-east-1, s3ForcePathStyle: "true", s3Url: http://minio.lakehouse.svc:9000}}], volumeSnapshotLocation: []}`, `snapshotsEnabled: false`, `deployNodeAgent: true`, `credentials: {useSecret: true, existingSecret: velero-s3-creds}`, `resources` 20m/128Mi, `nodeAgent.resources` 20m/128Mi. Secret `velero-s3-creds` (`cloud` anahtarı: `[default]\naws_access_key_id=…\naws_secret_access_key=…`) dev'de `glue/templates/dev-secrets.yaml`'a `namespace: velero` ile (bootstrap helm modu ve ArgoCD `CreateNamespace` için `velero` ns'i kurulumdan önce: bootstrap `kubectl create ns velero`). Dev kustomization `resources` += `../../apps/40-velero.yaml`; helm modu (dev) `helm upgrade --install velero vmware-tanzu/velero --version 12.1.0 -n velero --create-namespace -f platform/values/velero-dev.yaml --wait --timeout 10m` (glue'dan sonra — Secret); kök patch `velero` (sources/1).
- [ ] **Step 2: glue backup.yaml + dışlamalar** — `Schedule` (velero.enabled koşullu; `velero.schedule`, `velero.ttl` values); `kafka.yaml` Kafka/KafkaNodePool `template.pod.metadata.annotations` Velero excludes (Strimzi 1.x: `KafkaNodePool.spec.template.pod.metadata.annotations`); `networkpolicy.yaml` `velero` ns (yorum: node-agent hostPath ile erişir, ağ gerekmez).
- [ ] **Step 3: e2e dr-path.sh (2/2)**

```bash
echo "== Velero yedek"
kubectl -n "$NS" create configmap e2e-dr-marker --from-literal=k=v --dry-run=client -o yaml | kubectl apply -f - >/dev/null   # glue-dışı işaret: ArgoCD selfHeal yarışı yok
kubectl apply -f "$ROOT/test/e2e/velero-backup.yaml"   # Backup e2e-lakehouse (Schedule şablonuyla aynı alanlar)
p=""; for _ in $(seq 1 60); do p=$(kubectl -n velero get backup e2e-lakehouse -o jsonpath='{.status.phase}' 2>/dev/null || true); [[ "$p" =~ ^(Completed|PartiallyFailed|Failed)$ ]] && break; sleep 10; done
[[ "$p" == "Completed" ]] || { kubectl -n velero describe backup e2e-lakehouse | tail -40; echo "HATA Velero backup $p"; exit 1; }; echo "OK Velero backup Completed"
pv=$(kubectl -n velero get podvolumebackup -l velero.io/backup-name=e2e-lakehouse -o jsonpath='{range .items[*]}{.spec.volume}={.status.phase} {end}'); echo "PodVolumeBackups: $pv"
grep -q 'Completed' <<<"$pv" || { echo "HATA fs-backup yok (zeppelin-data / hub-db-dir bekleniyor)"; exit 1; }
grep -qE 'pgdata=|data-0=' <<<"$pv" && { echo "HATA Kafka/CNPG hacmi yedeğe girdi"; exit 1; }; echo "OK Kafka/CNPG hacimleri dışlandı"
echo "== Velero restore provası (e2e-dr-marker)"
kubectl -n "$NS" delete configmap e2e-dr-marker
kubectl apply -f "$ROOT/test/e2e/velero-restore.yaml"   # Restore e2e-marker: backupName e2e-lakehouse, includedResources [configmaps], labelSelector/ad filtresi e2e-dr-marker
r=""; for _ in $(seq 1 30); do r=$(kubectl -n velero get restore e2e-marker -o jsonpath='{.status.phase}' 2>/dev/null || true); [[ "$r" =~ ^(Completed|PartiallyFailed|Failed)$ ]] && break; sleep 5; done
[[ "$r" == "Completed" ]] || { kubectl -n velero describe restore e2e-marker | tail -30; echo "HATA restore $r"; exit 1; }
kubectl -n "$NS" get configmap e2e-dr-marker -o name >/dev/null || { echo "HATA marker geri gelmedi"; exit 1; }; echo "OK Velero restore: e2e-dr-marker geri geldi"
echo "E2E F5 DR OK"
```
- [ ] **Step 4: unittest** — Schedule alanları; Strimzi annotation; `velero.enabled=false` → yok.
- [ ] **Step 5: Canlı doğrulama** — helm modu kurulum; `dr-path.sh` tam; CPU fotoğrafı; velero-plugin-for-aws sürümü sabitlendi (F5 notu).
- [ ] **Step 6: Commit** — `feat(dr): Velero (dev chart, MinIO BSL, node-agent fs-backup) + glue Schedule; Kafka/CNPG volumes excluded; e2e backup+restore proof`.

---

### Task 5: Bakım/backlog kodu — rewrite_manifests, s3_register_example, ivySettings knob, tpch/tpcds, e2e DRY/409/e2e-admin, dev NetworkPolicy açık

**Files:**
- Create: `glue/jobs/s3_register_example.py`, `runbooks/s3-register.md`
- Modify: `glue/jobs/iceberg_maintenance.py`, `glue/templates/spark-jobs.yaml`, `glue/values.yaml`, `glue/tests/spark_test.yaml`, `platform/values/trino.yaml`, `platform/values/glue-dev.yaml`, `platform/values/jupyterhub-dev.yaml`, `test/e2e/lib.sh`, `test/e2e/pg-path.sh` (yalnız `run_spark_once` grep deseni), `test/e2e/trino-path.sh` (run_check_job imzası), `test/e2e/jupyterhub-path.sh`, `glue/templates/networkpolicy.yaml`, `.github/workflows/e2e.yaml` (py_compile)

- [ ] **Step 1: rewrite_manifests** — `iceberg_maintenance.py` `compact` modunda `rewrite_data_files`'tan sonra `call(spark, "rewrite_manifests", t)`; `lib.sh` `run_spark_once` grep desenine `rewrite_manifests` ekle (log kanıtı).
- [ ] **Step 2: s3_register_example.py** — argümanlar `--source s3://bucket/prefix/ --format csv|parquet --table ns.tbl [--header true]`; `spark.read.format(fmt).option("header", header).load(source)` → `df.writeTo(f"{CATALOG}.{table}").using("iceberg").createOrReplace()`; ~40 satır, `session()`/`CATALOG` kalıbı `iceberg_maintenance.py`'den import (tekrar yok). `runbooks/s3-register.md`: tek seferlik `SparkApplication` = `kubectl -n lakehouse get scheduledsparkapplication silver-merge -o json | jq '.spec.template'` şablonundan `mainApplicationFile: local:///opt/job/s3_register_example.py` + `arguments` ile türetme (run_spark_once mantığı). e2e'ye eklenmez; CI `helm-unittest` job'ına `python3 -m py_compile glue/jobs/*.py`.
- [ ] **Step 3: ivySettings knob** — `glue/values.yaml` `spark.ivySettingsXml: ""` (boş = Maven Central); doluysa ConfigMap `spark-ivysettings` + tüm SSA'lara mount `/opt/ivy/ivysettings.xml` + `sparkConf` `spark.jars.ivySettings`; unittest (boş → yok; dolu → var); `troubleshooting.md`/`install.md` (Task 6): iç Maven aynası örneği (`<ibiblio m2compatible="true" root="…"/>`).
- [ ] **Step 4: tpch/tpcds** — `platform/values/trino.yaml` `catalogs: {tpch: null, tpcds: null, lakehouse: …}`; render'da `tpch.properties`/`tpcds.properties` yok (kontrol: `helm template … | grep -c tpch` → 0).
- [ ] **Step 5: e2e DRY + 409 + e2e-admin** — `lib.sh`: `run_check_job <dir> <name> <pyfile> [args…]` (dosya adı parametre), `verify()` → `run_check_job "$ROOT/test/e2e/verify" verify verify.py "$@"` üstüne (verify/job.yaml `VERIFY_ARGS` → `CHECK_ARGS`); `trino-path.sh` çağrısı güncellenir; `jupyterhub-path.sh`: user create 201|409 kabul, delete 204 assert; `platform/values/jupyterhub-dev.yaml` servis adı `e2e-admin` (services/loadRoles) + script.
- [ ] **Step 6: dev NetworkPolicy açık** — `platform/values/glue-dev.yaml` `networkPolicy: {enabled: true}`; `networkpolicy.yaml` `allow-platform-namespaces` += `velero`; kind'da apiserver→webhook (spark-operator, cnpg webhook'ları lakehouse/cnpg-system'de; CNPG webhook cnpg-system'de — politika lakehouse ns'inde; spark-operator webhook `lakehouse` ns'inde → apiserver host ağından gelir) → gerekirse `allow-apiserver` (ipBlock apiserver IP/32 — dev-only, `networkPolicy.apiserverCidr` values) — canlı bulguya göre; F5 notuna. **Canlı**: glue upgrade sonrası `trino-path.sh`, `superset-path.sh`, `zeppelin-path.sh`, `monitoring-path.sh`, `dr-path.sh` politika altında yeniden; taze küme kanıtı Task 7.
- [ ] **Step 7: Commit(ler)** — `feat(maint): rewrite_manifests in compact; s3_register_example + runbook; spark.ivySettingsXml knob`, `fix(e2e,dev): verify via run_check_job, jupyterhub 409/e2e-admin, trino tpch/tpcds removed, dev NetworkPolicy enabled (+velero ns)`.

---

### Task 6: Runbook'lar — versions.md, upgrade.md, dr.md, troubleshooting.md, acceptance-tests.md + acceptance.sh, dbt örneği, Loki; spec §8; README

**Files:**
- Create: `runbooks/versions.md`, `runbooks/upgrade.md`, `runbooks/dr.md`, `runbooks/troubleshooting.md`, `runbooks/acceptance-tests.md`, `runbooks/scripts/acceptance.sh`, `runbooks/dbt/{README.md,dbt_project.yml,profiles.yml,models/gold/orders_daily.sql,cronjob.yaml}`
- Modify: `runbooks/install.md`, `runbooks/user-facing.md`, `docs/specs/2026-09-10-lakehouse-v2-design.md` §8, `README.md`

- [ ] **Step 1: versions.md** — tablo: Bileşen · Sürüm · Lisans · Kaynak (chart/imaj) · Kurulum yolu · Not. Satırlar: Strimzi 1.2.0 / Kafka 4.3.1 (Apache-2.0), Debezium 3.6.2.Final (Apache-2.0), Iceberg Kafka Connect 1.11.0 (Apache-2.0), Polaris 1.7.0 (Apache-2.0), CNPG operator 1.30.0 chart 0.29.0 + plugin-barman-cloud chart 0.8.0 / eklenti v0.15.0 (Apache-2.0; PostgreSQL imajı — PostgreSQL License), Spark 4.1.0 (Apache-2.0), spark-operator 2.5.2 (Apache-2.0), Trino 483 chart 1.42.2 (Apache-2.0; jmx-exporter `bitnamilegacy/jmx-exporter:1.4.0` Apache-2.0 — F6 aynası), Superset 6.1.0 (`-dev` etiketi, Apache-2.0) + operator 0.2.0 (Apache-2.0; API v1alpha1 → sürüm takibi; digest alanı yok → tag + ayna), JupyterHub z2jh 4.4.2 / 5.5.2 (BSD-3) + pyspark-notebook spark-4.1.2 (BSD-3), Zeppelin 0.12.1 (Apache-2.0), Keycloak 26.7.3 (Apache-2.0), cert-manager v1.21.2 (Apache-2.0), ArgoCD v3.5.2 (Apache-2.0), kube-prometheus-stack 91.4.1 (Apache-2.0; Grafana AGPL-3.0 — dev/vanilla), Velero 1.18.1 chart 12.1.0 (Apache-2.0) / OADP (OpenShift), Fluent Bit 5.1.2 (Apache-2.0), MinIO RELEASE.2025-09-07 (AGPL-3.0, DEV-ONLY), MongoDB 8.0 fixture (SSPL, DEV-ONLY), dbt-trino 1.10.4 (Apache-2.0, referans), pyiceberg 0.10 (Apache-2.0), trino-python-client 0.339 (Apache-2.0). Şartname a/J.2.1/J.3.2 atfı.
- [ ] **Step 2: upgrade.md** — sıra: operatörler (Strimzi → Kafka `version`/`metadataVersion` iki adım; CNPG minor + plugin; spark-operator; keycloak-operator + Keycloak imaj; cert-manager; superset-operator), sonra uygulamalar (Connect `buildImage` tag → yeniden build; Trino `image.tag`; Superset `imageTag`; z2jh chart; Zeppelin imaj; Polaris chart + admin-tool). ArgoCD `targetRevision` bump + `Synced/Healthy`; realm import güncellenmez uyarısı; helm-unittest + CI e2e gate; geri alma (git revert → ArgoCD).
- [ ] **Step 3: dr.md** — kapsam tablosu (CNPG PITR ✓ — Polaris DB = katalog metadata'sı, Keycloak, Superset; Velero namespace + not defteri PVC ✓; Kafka ✗ (yeniden akıtma/MM2); Iceberg S3 ✗ (S3 çoğaltması, metadata dahil)), zamanlama/saklama, `backup-s3-creds`, restore provaları: CNPG PITR (`cnpg-restore.yaml` + `recoveryTarget.targetTime`), Velero `Restore` (namespace tam / seçili / PVC fs-restore), Keycloak realm restore notu, OpenShift OADP `DataProtectionApplication` örneği; prova takvimi (çeyrek).
- [ ] **Step 4: troubleshooting.md** — `install.md` "Sorun giderme" taşınır (bağlantı kalır); F4/F5 maddeleri: Polaris vended-cred 403 (R10), superset-migrate yetim pod'ları, Zeppelin `DOWNLOADING_DEPENDENCIES`, 4 alarm için `#connect`, `#sink`, `#silver-merge`, `#spark` bölümleri (ne bak, ne yap), Velero `PartiallyFailed`, CNPG `ContinuousArchiving` False, ArgoCD PVC wave kilidi, imaj çekim süreleri, Maven aynası (`spark.ivySettingsXml`). Loki bölümü: OpenShift Logging LokiStack (belge bağlantısı), vanilla `grafana/loki` + `grafana/alloy` (promtail EOL), sorgu örnekleri.
- [ ] **Step 5: acceptance-tests.md + acceptance.sh** — tablo: şartname maddesi (A.6, C.2.1(e), C.3, D(e) MERGE INTO, D(f), B.3.1 time travel, F.1, G.1.1, G.5, G.6, H.3.1) ↔ kanıt (yol/komut/beklenen). `acceptance.sh`: `--ns lakehouse [--mon-ns monitoring] [--velero-ns velero]`; mevcut kümede fixture'ları uygular (demo `sources` values'ta açık olmalı — not), `polaris-setup.sh` idempotent, sonra `pg-path mongo-path nginx-path trino-path superset-path jupyterhub-path zeppelin-path monitoring-path dr-path` sırayla; `E2E … OK` → "KABUL" özeti. `dr-path`/`monitoring-path` `MON_NS`/`VELERO_NS` env'lerini okur (Task 1/4 script'lerinde `${VELERO_NS:-velero}`).
- [ ] **Step 6: dbt referans örneği** — `runbooks/dbt/README.md` (Trino servis hesabı `dbt` password.db'ye eklenir, `gold` şeması Polaris'te; CronJob `python:3.13-slim` + `pip install dbt-trino==1.10.4` + `dbt run`; PyPI egress; özel imaj yasağı nedeniyle resmi imaj olmadığı not), `dbt_project.yml`, `profiles.yml` (`type: trino, method: ldap, user/password env, http_scheme: https, cert: /etc/lakehouse-ca/tls.crt, host: trino.lakehouse.svc, port: 8443, database: lakehouse, schema: gold`), `models/gold/orders_daily.sql` (`shop.orders` günlük toplam; `materialized='table'`), `cronjob.yaml` (Secret `dbt-trino`, CA mount, proje ConfigMap). e2e'ye girmez.
- [ ] **Step 7: spec §8 + README + install.md** — §8 metni: KSM customResourceState, Barman plugin, Velero dışlamalar, dev izleme yığını; README "Şu an: F5 …"; install.md adım 2 yeni Application'lar (cnpg-barman base; dev overlay monitoring/velero), OpenShift'te UWM etkinleştirme + OADP.
- [ ] **Step 8: Commit** — `docs(f5): versions/upgrade/dr/troubleshooting/acceptance runbooks, acceptance.sh, dbt reference, Loki, spec §8`.

---

### Task 7: CI + tam e2e (taze küme lokal + CI) + F5 notu

**Files:**
- Modify: `.github/workflows/e2e.yaml`, `docs/plans/2026-09-10-f0-findings.md`

- [ ] **Step 1: CI** — teşhis: `kubectl -n monitoring get pods,prometheus,servicemonitor,podmonitor`, `kubectl -n lakehouse get prometheusrule,podmonitor,servicemonitor,objectstore,scheduledbackup,backup`, `kubectl -n velero get backup,restore,podvolumebackup`, Prometheus targets özeti; helm-unittest job'ına dev render'lar (`helm template monitoring prometheus-community/kube-prometheus-stack --version 91.4.1 -f platform/values/monitoring-dev.yaml >/dev/null`, velero eşdeğeri) — `timeout-minutes` 90 kalır.
- [ ] **Step 2: Lokal taze e2e** — `kind delete cluster --name lakehouse`; `nohup test/e2e/run.sh --mode helm > /tmp/e2e-f5.log 2>&1 &`; sınırlı poll (≤10 dk/çağrı; Monitor ile boşta bekleme YOK); beklenen 10 işaret (F1, F2, F3 MONGO, F3 NGINX, F4 TRINO, SUPERSET, JUPYTERHUB, ZEPPELIN, **F5 MONITORING, F5 DR**); süre + `Allocated resources`; NetworkPolicy açık → politika kanıtı.
- [ ] **Step 3: Push + CI** — ≤3 deneme; kırmızıysa kök neden + declarative fix; CPU: Spark öncesi istekler > 3400m ise dev Grafana kapatılır (F5 notu).
- [ ] **Step 4: F5 notu** — `## F5 notu (tarih)`: canlı metrik adları tablosu (Connect/sink/KSM/Trino), kural sağlığı + varsa ateşlenen alarm kanıtı, dashboard yükleme, CNPG backup/restore süreleri, Velero backup/restore + dışlama kanıtı, NetworkPolicy altında geçen yollar (+ apiserver→webhook bulgusu), CI süre/kaynak, açık kalanlar (OpenShift UWM/OADP canlı, Alertmanager yönlendirme (platform), Loki canlı, Superset migrate DB kapısı, PDB, Kafka MM2, F6 cutover).
- [ ] **Step 5: Commit + push** — `docs(f5): F5 notu — canlı metrik adları, DR provaları, CI`.

---

## Self-review (plan yazarı)

- **Spec kapsamı:** §8 metrikler (T1), kurallar 3→4 (stalled ayrı; T2), Grafana ConfigMap (T2), Loki runbook (T6), DR Velero + CNPG PITR farklı bucket + restore provası (T3, T4, T6), güvenlik NetworkPolicy canlı (T5); §4 runbook listesi: upgrade/dr/troubleshooting/versions/acceptance (T6); §5.5 S3 örneği (T5); §13 "kabul-testi betiği" (T6); §14 dbt (T6). F4 backlog: rewrite_manifests, dev MinIO PVC, tpch/tpcds, DRY, 409/e2e-admin, dev netpol, ivySettings, vended-cred (belge), Superset imaj (belge) — T3/T5/T6. Ertelenen: Superset migrate DB kapısı (F6), PDB (pre-ship), Kafka MM2 (belge).
- **Placeholder taraması:** "canlı doğrula" noktaları (release'e bağlı Service/Secret adları, KSM alan yolları/zaman damgası dönüşümü, sanitize metrik adları, Zeppelin env→property, Velero annotation/volume adları, kube-state-metrics chart `customResourceState` anahtarı, apiserver→webhook netpol, velero-plugin-for-aws sürümü) kodsuz bilinemeyen upstream davranışları; her biri için beklenen sonuç ve sapmada yapılacak yazılı.
- **Ad tutarlılığı:** release `monitoring` → `monitoring-kube-prometheus-prometheus`/`monitoring-grafana` (T1 ↔ T2 ↔ T6 acceptance MON_NS); Secret `backup-s3-creds` (T3 values ↔ MinIO ↔ install.md ↔ dr.md); ObjectStore `lakehouse-backups` (T3 cnpg.yaml ↔ cnpg-restore.yaml ↔ dr.md); Velero ns `velero`, Backup `e2e-lakehouse`, Restore `e2e-marker`, ConfigMap `e2e-dr-marker` (T4 script ↔ manifestler); alarm adları (T2 ↔ T6 troubleshooting bölümleri); `run_check_job <dir> <name> <pyfile>` (T5 lib.sh ↔ trino-path ↔ verify).

## Yürütme ruling'leri (SDD ledger'dan, 2026-09-18 — workspace silinmeden önce kopyalandı)

Kapanış: origin/v2 == 26a16b5 + bu commit; CI ArgoCD taze runner 10/10 marker ×3 (35364856165, 35371704385, 35379357635); lokal taze tam koşu kapanmadı (F5 notu 25. satır).

- Ruling: worktree yok, `v2` dalında main checkout (F1–F4 düzeni; helm-mode e2e ve kind kümesi bu checkout'tan) — yanlışsa maliyet: kirli çalışma ağacı (git ile geri alınır).
- Ruling (kullanıcı kararı 2026-09-18, izleme kapsamı): izleme = boru hattı sağlığı. KALIR: Strimzi Kafka+Connect JMX, spark-operator, KSM Spark CR durumu, Polaris mgmt. EKLENİR: Strimzi Kafka.spec.kafkaExporter (consumer-group lag), LakehouseSinkStalled lag tabanlı (kafka_consumergroup_lag connect-sink-*), yeni kural LakehouseSparkRunTooLong (spark_application_success_execution_time_seconds > 1800). KALKAR: Trino jmx exporter+ServiceMonitor+dashboard, hub ServiceMonitor + authenticate_prometheus + netpol kuralları + Prometheus podMetadata label, Zeppelin metrics env + ServiceMonitor. Tablo düzeyi veri metrikleri: Prometheus exporter yok → Trino Iceberg metadata tabloları + Superset (F6). Uygulama: Task 2b (Task 3'ten önce). — yanlışsa maliyet: Trino sorgu hata metriği kaybı (F6'da Trino native OpenMetrics /metrics ile geri alınabilir)
- Ruling: CI (ArgoCD, taze runner) F5 için de otoriter taze-küme kapısı — F4 emsali; lokal tam koşu kapanmadığı için lokal bileşen-yolu kanıtları (T1–T6) + CI birlikte kabul edilir — yanlışsa maliyet: lokal-özel bir regresyon (helm modu) fark edilmez; helm modu CI'da koşmuyor (F6'da değerlendir).
- Ruling: kontrolör canlı ölçümü (task-7-addendum.md) reviewer'ın "deneme 1–2 bellek kanıtı" okumasını da geçersiz kılar — dmesg'de host OOM YOK, tüm kill'ler cgroup (grafana 13×, limit 256Mi ↔ idle 235 MB), polaris 137 = liveness kill, PSI cpu 5735 s ≫ mem 561 s → lokal düşmelerin nedeni Podman VM'de yük tepelerinde probe zaman aşımı; anlatı bu gerçeğe göre yazılır, Grafana limiti 768Mi'ye çıkar (d56fa48 kısmen geri) — yanlışsa maliyet: dev bellek bütçesi 512Mi artar, CI %97 limit taahhüdü (bağlayıcı değil).
- Ruling: Polaris mgmt metriği — kullanıcı kapsam kararında Polaris KALIR; Polaris chart serviceMonitor/metrics anahtarı canlı doğrulanırsa eklenir (+ netpol 8182 + e2e UP), yoksa spec/troubleshooting ifadesi çıkarılır — yanlışsa maliyet: bir ServiceMonitor ya da iki cümle.
- Ruling: cert-manager sync-wave "-1" (monitoring ile aynı dalga; ikisi bağımsız) — CI'da iki kez yeşil geçmesi selfHeal sayesinde; prod'da emniyet ağı yok — yanlışsa maliyet: yok (yalnız sıra).
- Ruling: parked minors bu dalgaya alınır: ksm dosyası sil (values tek kaynak), spark-operator/values yorumları, dr-path `|| true`, s3-register dry-run satırı, Grafana request 256Mi, dbt README grant hedge doğrulanır; ATLANIR: velero-dev CPU limit (dev'de hiçbir bileşende yok, tasarım), dr.md §5.3 yoğunluk (stil), run_spark_once un-suspend (belgelendi, F3 mirası), Schedule↔velero-backup.yaml (yorum satırı ile yeterli).
