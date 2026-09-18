# Sürümler ve lisanslar (şartname a, J.2.1, J.3.2)

Şartname **a** maddesi tüm bileşenlerin açık kaynak olmasını, **J.2.1** kullanılan ürünlerin sürüm/lisans listesini,
**J.3.2** ise bu listenin güncel tutulmasını ister. Aşağıdaki tablolar tek doğrudur: "Kurulum yolu" sütunu sürümün
repoda **hangi dosyada sabitlendiğini** gösterir — yükseltme o dosyada yapılır (`runbooks/upgrade.md`).

**Özel imaj sıfırdır.** Tüm imajlar upstream'in resmi imajlarıdır; Kafka Connect imajı Strimzi'nin `spec.build`'i
ile kümede üretilir (Dockerfile yok). Hızlı denetim:
`grep -rn "targetRevision\|imageTag\|image:" platform/apps platform/values glue/values.yaml`

## Platform / operatörler

| Bileşen | Sürüm | Lisans | Kaynak (chart/imaj) | Kurulum yolu | Not |
|---|---|---|---|---|---|
| ArgoCD | v3.5.2 | Apache-2.0 | `argoproj/argo-cd` manifest (`v3.5.2/manifests/install.yaml`) | `bootstrap/bootstrap.sh` → `ARGOCD_VERSION` | OpenShift'te platformun GitOps operatörü de kullanılabilir; kök Application aynıdır |
| cert-manager | v1.21.2 | Apache-2.0 | OCI chart `quay.io/jetstack/charts/cert-manager` | `platform/apps/00-cert-manager.yaml` | `crds.enabled=true`; `Issuer/lakehouse-ca` + `Certificate/trino-tls` glue'da. Barman Cloud eklentisi de cert-manager ister |
| Strimzi Kafka operator | 1.2.0 | Apache-2.0 | OCI chart `quay.io/strimzi-helm/strimzi-kafka-operator` | `platform/apps/00-strimzi.yaml` | `watchNamespaces={lakehouse}`; JMX exporter kuralları ve Grafana dashboard'ları Strimzi 1.2.0 örneklerinden birebir (`glue/files/metrics/`, `glue/files/dashboards/`) |
| Apache Kafka | 4.3.1 (`metadataVersion 4.3-IV0`) | Apache-2.0 | Strimzi imajı (`Kafka` CR) | `glue/values.yaml` → `versions.kafka`, `versions.kafkaMetadata` | KRaft; yükseltme **iki adımlıdır** (`runbooks/upgrade.md`) |
| CloudNativePG operator | chart 0.29.0 (operatör **1.30.0**) | Apache-2.0 | `https://cloudnative-pg.github.io/charts` | `platform/apps/00-cnpg.yaml` | PostgreSQL imajı upstream CNPG imajıdır → **PostgreSQL License** (operatör Apache-2.0) |
| CNPG Barman Cloud eklentisi | chart **0.8.0** = eklenti **v0.15.0** | Apache-2.0 | `cnpg/plugin-barman-cloud` | `platform/apps/00-cnpg-barman.yaml` | Chart sürümü ≠ eklenti sürümü (`--version 0.15.0` diye bir chart YOK). In-tree `barmanObjectStore` CNPG 1.31'de kalkıyor |
| Kubeflow spark-operator | 2.5.2 | Apache-2.0 | `https://kubeflow.github.io/spark-operator` | `platform/apps/00-spark-operator.yaml` | `prometheus.podMonitor.create=true`; exporter **per-app etiket yaymaz** (`runbooks/troubleshooting.md#spark-duration`) |
| Keycloak operator + Keycloak | 26.7.3 | Apache-2.0 | `github.com/keycloak/keycloak-k8s-resources//kubernetes?ref=26.7.3` | `platform/keycloak-operator/kustomization.yaml` | Helm chart'ı yok (kustomize uzak kaynak = config, imaj değil). Operatör ve sunucu aynı sürümde ilerler |
| Apache Superset Kubernetes Operator | 0.2.0 | Apache-2.0 | OCI chart `ghcr.io/apache/superset-kubernetes-operator/charts/superset-operator` | `platform/apps/00-superset-operator.yaml` | **API `v1alpha1`** → kırıcı değişiklik riski yüksek, sürüm takibi şart. Resmi Superset Helm chart'ı **deprecated** |

## Veri düzlemi

| Bileşen | Sürüm | Lisans | Kaynak (chart/imaj) | Kurulum yolu | Not |
|---|---|---|---|---|---|
| Debezium (postgres / sqlserver / mongodb) | 3.6.2.Final | Apache-2.0 | Maven Central zip artefaktı (`KafkaConnect.spec.build`) | `glue/values.yaml` → `versions.debezium` | Değişince Connect imajı **yeniden build** edilir (~10 dk) |
| Apache Iceberg Kafka Connect sink | 1.11.0 | Apache-2.0 | `spec.build` maven artefaktları (`iceberg-kafka-connect`, `-transforms`, `iceberg-parquet`, `-orc`, `-aws`, `-aws-bundle`) | `glue/values.yaml` → `versions.iceberg` | Aynı değer Spark `spark.jars.packages`'ini de üretir (tek kaynak) |
| Hadoop client (Connect build) | 3.4.3 | Apache-2.0 | `org.apache.hadoop:hadoop-client-api` / `-runtime` | `glue/values.yaml` → `versions.hadoopClient` | Iceberg sink'in gereksinimi (F0 S2) |
| Apache Polaris (REST katalog) | 1.7.0 | Apache-2.0 | chart `https://downloads.apache.org/polaris/helm-chart` + imaj `apache/polaris:1.7.0` | `platform/apps/20-polaris.yaml`, `platform/values/polaris.yaml` | `persistence.type=relational-jdbc` → CNPG `polaris-db`; admin-tool bootstrap Job glue'da (`versions.polarisAdminTool`) |
| `apache-polaris` CLI | 1.7.0 | Apache-2.0 | PyPI (`pip install 'apache-polaris==1.7.0'`) | `runbooks/scripts/polaris-setup.sh`, `test/e2e/run.sh` | Katalog/namespace/rol/principal kurulumu |
| Apache Spark | 4.1.0 | Apache-2.0 | `apache/spark:4.1.0-java21-python3` | `glue/values.yaml` → `spark.image`, `spark.version` | Iceberg runtime **her koşuda** Maven'den çözülür; iç ayna: `spark.ivySettingsXml` (`runbooks/troubleshooting.md#maven`) |
| Trino | **483** (chart 1.42.2; chart appVersion `480`) | Apache-2.0 | `https://trinodb.github.io/charts` | `platform/apps/30-trino.yaml` + `platform/values/trino.yaml` (`image.tag: "483"`) | Chart varsayılan etiketi 480; values 483'e sabitler. **jmx-exporter sidecar'ı kullanılmıyor** (izleme yalnız boru hattı sağlığı) → `bitnamilegacy/jmx-exporter` imajına bağımlılık YOK |
| Apache Superset | 6.1.0 — imaj etiketi `apache/superset:6.1.0-dev` | Apache-2.0 | resmi imaj + `Superset` CR | `glue/values.yaml` → `superset.imageTag` | `-dev` etiketi `psycopg2`/`trino`/`authlib` sürücülerini İÇERİR (düz `6.1.0` içermez). Operator `spec.image` **digest kabul etmez** (`repository:tag`) → müşteri aynasında *tag immutability* ile sabitlenir; yükseltmede sürücülerin hâlâ imajda olduğu doğrulanır |
| JupyterHub (z2jh) | chart 4.4.2 → JupyterHub **5.5.2** | BSD-3-Clause | `https://hub.jupyter.org/helm-chart/` | `platform/apps/30-jupyterhub.yaml` + `platform/values/jupyterhub.yaml` | |
| Notebook imajı | `quay.io/jupyter/pyspark-notebook:spark-4.1.2` | BSD-3-Clause | quay.io (Jupyter Docker Stacks) | `platform/values/jupyterhub.yaml` → `singleuser.image` | Soğuk çekim ~4,5 dk (`startTimeout: 1200`); `postStart` ile pyiceberg + trino kurulur (PyPI erişimi) |
| Apache Zeppelin | 0.12.1 | Apache-2.0 | `apache/zeppelin:0.12.1` | `glue/values.yaml` → `zeppelin.image` | Chart yok: Deployment + PVC (glue) |
| Trino JDBC (Zeppelin interpreter) | 483 | Apache-2.0 | Maven Central `io.trino:trino-jdbc` | `glue/values.yaml` → `zeppelin.trinoJdbcVersion` | Trino sunucusuyla birlikte yükseltilir |
| Fluent Bit | 5.1.2 | Apache-2.0 | müşteri sunucusunda paket (e2e'de `fluent/fluent-bit:5.1.2`) | `agents/fluent-bit/`, `runbooks/nginx-agent.md` | Küme bileşeni değil, ajan |

## İzleme / DR (F5)

| Bileşen | Sürüm | Lisans | Kaynak (chart/imaj) | Kurulum yolu | Not |
|---|---|---|---|---|---|
| kube-prometheus-stack | 91.4.1 (prometheus-operator **v0.94.0**) | Apache-2.0 | `https://prometheus-community.github.io/helm-charts` | `platform/apps/dev/40-monitoring.yaml` + `platform/values/monitoring-dev.yaml` | **DEV-ONLY** (sync-wave `-1`). Prod'da OpenShift user-workload monitoring; `lakehouse` ns'indeki PodMonitor/PrometheusRule otomatik alınır |
| Grafana (kube-prometheus-stack alt chart'ı) | chart ile gelen | **AGPL-3.0** | aynı chart | aynı | Yalnız dev/vanilla. Dashboard'lar: Strimzi Kafka + Strimzi Kafka Connect (`glue/files/dashboards/`) — Trino dashboard'u kapsam dışı |
| kube-state-metrics (alt chart) | 8.5.0 | Apache-2.0 | aynı chart | `platform/values/monitoring-dev.yaml` → `kube-state-metrics.customResourceState` | `kube_customresource_sparkapp_state` / `_termination_time` / `_ssa_last_run` |
| Velero | chart 12.1.0 (Velero **1.18.1**) | Apache-2.0 | `https://vmware-tanzu.github.io/helm-charts` | `platform/apps/dev/40-velero.yaml` + `platform/values/velero-dev.yaml` | **DEV-ONLY**. Prod'da **OADP** (`openshift-adp`) aynı `velero.io/v1` CRD'lerini sağlar — `runbooks/dr.md` |
| `velero-plugin-for-aws` | v1.14.2 | Apache-2.0 | `velero/velero-plugin-for-aws:v1.14.2` | `platform/values/velero-dev.yaml` (initContainer) | Uyumluluk: plugin v1.14.x ↔ Velero v1.18.x |
| Kopia (node-agent uploader) | Velero ile gelir | Apache-2.0 | Velero imajı | `deployNodeAgent: true` | fs-backup varsayılan uploader'ı |

## İstemci kütüphaneleri (referans; küme bileşeni değil)

| Bileşen | Sürüm | Lisans | Kaynak | Kurulum yolu | Not |
|---|---|---|---|---|---|
| PyIceberg | `>=0.10,<0.11` | Apache-2.0 | PyPI `pyiceberg[s3fs,pyarrow]` | `platform/values/jupyterhub.yaml` (`postStart`), `test/e2e/*/job.yaml` | Not defterleri + e2e doğrulama Job'ları |
| trino (Python client) | `>=0.339,<0.340` | Apache-2.0 | PyPI `trino` | `platform/values/jupyterhub.yaml` (`postStart`) | |
| dbt-trino | 1.10.4 | Apache-2.0 | PyPI | `runbooks/dbt/cronjob.yaml` (referans örnek) | **Resmi dbt-trino imajı YOK** → örnek CronJob `python:3.13-slim` + `pip install dbt-trino==1.10.4` kullanır (özel imaj yasağı); çalışma zamanında PyPI erişimi gerekir |

## DEV-ONLY bileşenler (prod'da KURULMAZ)

| Bileşen | Sürüm | Lisans | Kaynak | Kurulum yolu | Not |
|---|---|---|---|---|---|
| MinIO | `RELEASE.2025-09-07T16-13-09Z` | **AGPL-3.0** | `quay.io/minio/minio` | `glue/templates/minio.yaml` (`components.minio: true`) | Yalnız kind/dev S3'ü (veri + `backups` bucket'ı). Prod'da müşterinin S3'ü |
| MinIO `mc` | `RELEASE.2025-08-13T08-35-41Z` | AGPL-3.0 | `quay.io/minio/mc` | aynı (bucket-init Job) | |
| MongoDB (e2e fixture) | 8.0 | **SSPL** | `mongo:8.0` | `test/e2e/mongo-fixture.yaml` | Yalnız test fixture'ı; müşteri MongoDB'si kaynak sistemdir |
| kind | ≥ 0.33 | Apache-2.0 | kind.sigs.k8s.io | `test/e2e/kind.sh`, CI | Podman 6 uyumu için ≥ 0.33 |
| Helm | 4.2.4 (CI) | Apache-2.0 | `azure/setup-helm` | `.github/workflows/e2e.yaml` | Yerelde ≥ 3.14 yeterli |
| helm-unittest | 1.1.2 | MIT | GitHub plugin | `.github/workflows/e2e.yaml` | `glue/tests/` |

## Replika sayıları ve PodDisruptionBudget (karar: PDB YOK)

Kurulum hiçbir bileşen için `PodDisruptionBudget` üretmez. Gerekçe ve "ne zaman yeniden ele alınır" koşulu:
`runbooks/preship-openshift.md` §5.1.

| Bileşen | Prod replika | Kurulum yolu | PDB |
|---|---|---|---|
| Kafka (broker/controller) | 3 | `glue/values.yaml` → `kafka.replicas` (`min.insync.replicas: 2`) | **Strimzi yönetir** (elle eklenmez) |
| CNPG `polaris-db` / `keycloak-db` / `superset-db` | 2 | `platform/values/glue.yaml` → `cnpg.*.instances` | **CNPG yönetir** (elle eklenmez) |
| Kafka Connect | 1 | `glue/values.yaml` → `connect.replicas` | yok — tek replika, PDB drain'i kilitler |
| Trino coordinator / worker | 1 / 2 | `platform/values/trino.yaml` → `server.workers` | yok — coordinator tekildir (chart), worker durumsuz |
| Polaris | 1 | `platform/apps/20-polaris.yaml` + `platform/values/polaris.yaml` | yok — tek replika |
| Superset | 1 | `glue/values.yaml` → `superset.replicas` | yok — tek replika |
| JupyterHub hub / Zeppelin | 1 / 1 | `platform/values/jupyterhub.yaml`, `glue/templates/zeppelin.yaml` | yok — tek replika |

Tek replikalı bir iş yüküne `minAvailable: 1` vermek düğüm boşaltmayı (`oc adm drain`) kalıcı kilitler;
`maxUnavailable: 1` ise zaten mevcut davranıştır. HA gerekirse **önce replika sayısı** artırılır, PDB kararı o
değişiklikle birlikte verilir.

## Lisans özeti (teslimat kapsamı)

- Prod'a giden küme bileşenlerinin tamamı **Apache-2.0 / BSD-3-Clause / PostgreSQL License** (izin verici lisanslar).
- **AGPL-3.0** yalnız iki yerde: Grafana (dev izleme yığını — OpenShift'te platformun izlemesi kullanılır) ve MinIO
  (yalnız kind/dev). İkisi de prod kurulumunda yer almaz.
- **SSPL** yalnız e2e MongoDB fixture'ındadır (test verisi üreticisi).
- Kaynak kod olarak teslim edilen her şey (glue chart'ı, Spark işleri, runbook'lar) bu repodadır; üçüncü taraf
  imajlarda değişiklik yapılmaz (özel imaj yok) → türev eser yükümlülüğü doğmaz.

## Sürüm listesini güncel tutma (J.3.2)

1. Yükseltme `runbooks/upgrade.md`'deki sırayla yapılır; aynı PR'da **bu dosyadaki satır** güncellenir.
2. PR CI kapısı: `helm unittest glue` + prod/dev render + kind e2e (`.github/workflows/e2e.yaml`).
3. Kurulum sonrası fiili sürümler kümeden doğrulanır:
   ```bash
   kubectl -n argocd get applications -o custom-columns='APP:.metadata.name,CHART:.spec.source.chart,REV:.spec.source.targetRevision'
   kubectl -n lakehouse get kafka lakehouse -o jsonpath='{.spec.kafka.version}{"\n"}'
   kubectl -n lakehouse get pods -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{range .spec.containers[*]}{.image}{" "}{end}{"\n"}{end}'
   ```
