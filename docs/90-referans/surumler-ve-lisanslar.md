# 90 — Sürümler ve lisanslar

**Bu bölümde:** kurulumda yer alan **bütün** bileşenlerin sürümü, lisansı, nereden geldiği
ve sürümün repoda hangi dosyada sabitlendiği; replika sayıları ve `PodDisruptionBudget`
kararı; lisans özeti; listenin güncel tutulması.
**Süre:** okuma 15 dakika.
**Gereken yetki:** kümedeki fiilî sürümleri doğrulamak için `$ARGOCD_NS` ve
`$LAKEHOUSE_NS` ad alanlarında okuma.
**Nerede çalıştırılır:** `[bastion]`.

Şartname **a** maddesi tüm bileşenlerin açık kaynak olmasını, **J.2.1** kullanılan
ürünlerin sürüm/lisans listesini, **J.3.2** ise bu listenin güncel tutulmasını ister.
Aşağıdaki tablolar tek doğrudur: **Kurulum yolu** sütunu sürümün repoda hangi dosyada
sabitlendiğini gösterir — yükseltme o dosyada yapılır.

**Özel imaj sıfırdır.** Bütün imajlar upstream'in resmi imajlarıdır; Kafka Connect imajı
Strimzi'nin `spec.build` mekanizmasıyla kümede üretilir (Dockerfile yoktur). Hızlı denetim:

`[bastion]`

```bash
grep -rn "targetRevision\|imageTag\|image:" platform/apps platform/values glue/values.yaml
```

**Beklenen çıktı:** basılan her satır aşağıdaki tablolardan birinde geçen bir sürüm pinidir;
tabloda karşılığı olmayan bir satır, güncellenmemiş liste demektir.

**Ters giderse:** komut hiçbir şey basmıyorsa depo kökünde değilsinizdir (`cd ~/lakehouse`).

---

## 1. Platform ve operatörler

| Bileşen | Sürüm | Lisans | Kaynak (chart/imaj) | Kurulum yolu | Not |
|---|---|---|---|---|---|
| ArgoCD | v3.5.2 | Apache-2.0 | `argoproj/argo-cd` deposunun v3.5.2 etiketli kurulum manifest'i | `bootstrap/bootstrap.sh` → `ARGOCD_VERSION` | OpenShift'te platformun GitOps operatörü de kullanılabilir; kök Application aynıdır |
| cert-manager | v1.21.2 | Apache-2.0 | OCI chart `quay.io/jetstack/charts/cert-manager` | `platform/apps/00-cert-manager.yaml` | `crds.enabled=true`; `Issuer/lakehouse-ca` ve `Certificate/trino-tls` glue'dadır. Barman Cloud eklentisi de cert-manager ister |
| Strimzi Kafka operator | 1.2.0 | Apache-2.0 | OCI chart `quay.io/strimzi-helm/strimzi-kafka-operator` | `platform/apps/00-strimzi.yaml` | `watchNamespaces={lakehouse}`; JMX exporter kuralları ve Grafana dashboard'ları Strimzi 1.2.0 örneklerinden birebir (`glue/files/metrics/`, `glue/files/dashboards/`) |
| Apache Kafka | 4.3.1 (`metadataVersion 4.3-IV0`) | Apache-2.0 | Strimzi imajı (`Kafka` CR) | `glue/values.yaml` → `versions.kafka`, `versions.kafkaMetadata` | KRaft; yükseltme **iki adımlıdır** |
| CloudNativePG operator | chart 0.29.0 (operatör **1.30.0**) | Apache-2.0 | `https://cloudnative-pg.github.io/charts` | `platform/apps/00-cnpg.yaml` | PostgreSQL imajı upstream CNPG imajıdır → **PostgreSQL License** (operatör Apache-2.0) |
| CNPG Barman Cloud eklentisi | chart **0.8.0** = eklenti **v0.15.0** | Apache-2.0 | `cnpg/plugin-barman-cloud` | `platform/apps/00-cnpg-barman.yaml` | Chart sürümü ≠ eklenti sürümü (`--version 0.15.0` diye bir chart **yoktur**). In-tree `barmanObjectStore` CNPG 1.31'de kalkıyor |
| Kubeflow spark-operator | 2.5.2 | Apache-2.0 | `https://kubeflow.github.io/spark-operator` | `platform/apps/00-spark-operator.yaml` | `prometheus.podMonitor.create=true`; exporter **uygulama başına etiket yaymaz** |
| Keycloak operator + Keycloak | 26.7.3 | Apache-2.0 | `github.com/keycloak/keycloak-k8s-resources//kubernetes?ref=26.7.3` | `platform/keycloak-operator/kustomization.yaml` | Helm chart'ı yoktur (kustomize uzak kaynağı = yapılandırma, imaj değil). Operatör ve sunucu aynı sürümde ilerler |
| Apache Superset Kubernetes Operator | 0.2.0 | Apache-2.0 | OCI chart `ghcr.io/apache/superset-kubernetes-operator/charts/superset-operator` | `platform/apps/00-superset-operator.yaml` | **API `v1alpha1`** → kırıcı değişiklik riski yüksektir, sürüm takibi şarttır. Resmi Superset Helm chart'ı **deprecated** |

---

## 2. Veri düzlemi

| Bileşen | Sürüm | Lisans | Kaynak (chart/imaj) | Kurulum yolu | Not |
|---|---|---|---|---|---|
| Debezium (postgres / sqlserver / mongodb) | 3.6.2.Final | Apache-2.0 | Maven Central zip artefaktı (`KafkaConnect.spec.build`) | `glue/values.yaml` → `versions.debezium` | Değişince Connect imajı **yeniden üretilir** (~10 dk) |
| Apache Iceberg Kafka Connect sink | 1.11.0 | Apache-2.0 | `spec.build` maven artefaktları (`iceberg-kafka-connect`, `-transforms`, `iceberg-parquet`, `-orc`, `-aws`, `-aws-bundle`) | `glue/values.yaml` → `versions.iceberg` | Aynı değer Spark `spark.jars.packages` listesini de üretir (tek kaynak) |
| Hadoop client (Connect build'i) | 3.4.3 | Apache-2.0 | `org.apache.hadoop:hadoop-client-api` / `-runtime` | `glue/values.yaml` → `versions.hadoopClient` | Iceberg sink'in gereksinimidir |
| Apache Polaris (REST katalog) | 1.7.0 | Apache-2.0 | chart `https://downloads.apache.org/polaris/helm-chart` + imaj `apache/polaris:1.7.0` | `platform/apps/20-polaris.yaml`, `platform/values/polaris.yaml` | `persistence.type=relational-jdbc` → CNPG `polaris-db`; admin-tool bootstrap Job'ı glue'dadır (`versions.polarisAdminTool`) |
| `apache-polaris` CLI | 1.7.0 | Apache-2.0 | PyPI (`pip install 'apache-polaris==1.7.0'`) | `scripts/polaris-setup.sh`, `test/e2e/run.sh` | Katalog/namespace/rol/principal kurulumu ([40-kurulum-sonrasi](../40-kurulum-sonrasi.md) §2) |
| Apache Spark | 4.1.0 | Apache-2.0 | `apache/spark:4.1.0-java21-python3` | `glue/values.yaml` → `spark.image` | Iceberg runtime **her koşuda** Maven'den çözülür; iç ayna için `spark.ivySettingsXml` |
| Trino | **483** (chart 1.42.2; chart appVersion `480`) | Apache-2.0 | `https://trinodb.github.io/charts` | `platform/apps/30-trino.yaml` + `platform/values/trino.yaml` (`image.tag: "483"`) | Chart varsayılan etiketi 480'dir; values 483'e sabitler. **jmx-exporter sidecar'ı kullanılmaz** (izleme kapsamı yalnız boru hattı sağlığıdır) |
| Apache Superset | 6.1.0 — imaj etiketi `apache/superset:6.1.0-dev` | Apache-2.0 | resmi imaj + `Superset` CR | `glue/values.yaml` → `superset.imageTag` | `-dev` etiketi `psycopg2`/`trino`/`authlib` sürücülerini **içerir** (düz `6.1.0` içermez). Operatör `spec.image` alanında **digest kabul etmez** (`repository:tag`) → müşteri aynasında etiket değişmezliğiyle sabitlenir |
| JupyterHub (z2jh) | chart 4.4.2 → JupyterHub **5.5.2** | BSD-3-Clause | `https://hub.jupyter.org/helm-chart/` | `platform/apps/30-jupyterhub.yaml` + `platform/values/jupyterhub.yaml` | |
| Not defteri imajı | `quay.io/jupyter/pyspark-notebook:spark-4.1.2` | BSD-3-Clause | quay.io (Jupyter Docker Stacks) | `platform/values/jupyterhub.yaml` → `singleuser.image` | Soğuk çekim ~4,5 dk (`startTimeout: 1200`); `postStart` ile pyiceberg ve trino kurulur (PyPI erişimi gerekir) |
| Apache Zeppelin | 0.12.1 | Apache-2.0 | `apache/zeppelin:0.12.1` | `glue/values.yaml` → `zeppelin.image` | Chart yoktur: Deployment + PVC (glue) |
| Trino JDBC (Zeppelin interpreter'ı) | 483 | Apache-2.0 | Maven Central `io.trino:trino-jdbc` | `glue/values.yaml` → `zeppelin.trinoJdbcVersion` | Trino sunucusuyla birlikte yükseltilir |
| Fluent Bit | 5.1.2 | Apache-2.0 | müşteri sunucusunda paket (e2e'de `fluent/fluent-bit:5.1.2`) | `agents/fluent-bit/` | Küme bileşeni değil, kaynak sunucudaki ajandır |

---

## 3. İzleme ve yedekleme

| Bileşen | Sürüm | Lisans | Kaynak (chart/imaj) | Kurulum yolu | Not |
|---|---|---|---|---|---|
| kube-prometheus-stack | 91.4.1 (prometheus-operator **v0.94.0**) | Apache-2.0 | `https://prometheus-community.github.io/helm-charts` | `platform/apps/dev/40-monitoring.yaml` + `platform/values/monitoring-dev.yaml` | **DEV-ONLY** (sync-wave `-1`). Üretimde OpenShift user-workload monitoring; `$LAKEHOUSE_NS` ad alanındaki PodMonitor/PrometheusRule otomatik alınır |
| Grafana (kube-prometheus-stack alt chart'ı) | chart ile gelen | **AGPL-3.0** | aynı chart | aynı | Yalnız dev/vanilla. Dashboard'lar: Strimzi Kafka ve Strimzi Kafka Connect (`glue/files/dashboards/`) |
| kube-state-metrics (alt chart) | kube-prometheus-stack 91.4.1 ile gelen | Apache-2.0 | aynı chart | `platform/values/monitoring-dev.yaml` → `kube-state-metrics.customResourceState` | `kube_customresource_sparkapp_state` / `_termination_time` / `_ssa_last_run` |
| Velero | chart 12.1.0 (Velero **1.18.1**) | Apache-2.0 | `https://vmware-tanzu.github.io/helm-charts` | `platform/apps/dev/40-velero.yaml` + `platform/values/velero-dev.yaml` | **DEV-ONLY**. Üretimde **OADP** (`openshift-adp`) aynı `velero.io/v1` CRD'lerini sağlar |
| `velero-plugin-for-aws` | v1.14.2 | Apache-2.0 | `velero/velero-plugin-for-aws:v1.14.2` | `platform/values/velero-dev.yaml` (initContainer) | Uyumluluk: plugin v1.14.x ↔ Velero v1.18.x |
| Kopia (node-agent yükleyicisi) | Velero ile gelir | Apache-2.0 | Velero imajı | `deployNodeAgent: true` | fs-backup'ın varsayılan yükleyicisidir |

---

## 4. İstemci kütüphaneleri (referans; küme bileşeni değil)

| Bileşen | Sürüm | Lisans | Kaynak | Kurulum yolu | Not |
|---|---|---|---|---|---|
| PyIceberg | `>=0.10,<0.11` | Apache-2.0 | PyPI `pyiceberg[s3fs,pyarrow]` | `platform/values/jupyterhub.yaml` (`postStart`) | Not defterleri ve e2e doğrulama Job'ları |
| trino (Python istemcisi) | `>=0.339,<0.340` | Apache-2.0 | PyPI `trino` | `platform/values/jupyterhub.yaml` (`postStart`) | |
| dbt-trino | 1.10.4 | Apache-2.0 | PyPI | `examples/dbt/cronjob.yaml` (referans örnek) | **Resmi dbt-trino imajı yoktur** → örnek CronJob `python:3.13-slim` kullanır ve paketi çalışma anında kurar (PyPI erişimi gerekir) |

---

## 5. DEV-ONLY bileşenler (üretimde kurulmaz)

| Bileşen | Sürüm | Lisans | Kaynak | Kurulum yolu | Not |
|---|---|---|---|---|---|
| MinIO | `RELEASE.2025-09-07T16-13-09Z` | **AGPL-3.0** | `quay.io/minio/minio` | `glue/templates/minio.yaml` (`components.minio: true`) | Yalnız kind/dev S3'ü (veri ve yedek bucket'ları). Üretimde müşterinin S3'ü |
| MinIO `mc` | `RELEASE.2025-08-13T08-35-41Z` | AGPL-3.0 | `quay.io/minio/mc` | aynı (bucket-init Job'ı) | |
| MongoDB (e2e fixture'ı) | 8.0 | **SSPL** | `mongo:8.0` | `test/e2e/mongo-fixture.yaml` | Yalnız test fixture'ı; müşteri MongoDB'si kaynak sistemdir |
| kind | ≥ 0.33 (CI: v0.33.0) | Apache-2.0 | kind.sigs.k8s.io | `test/e2e/kind.sh`, `.github/workflows/e2e.yaml` | Podman 6 uyumu için ≥ 0.33 |
| Helm | v4.2.4 (CI) | Apache-2.0 | `azure/setup-helm` | `.github/workflows/e2e.yaml` | Yerelde ≥ 3.14 yeterlidir |
| helm-unittest | 1.1.2 | MIT | GitHub eklentisi | `.github/workflows/e2e.yaml` | `glue/tests/` |

---

## 6. Replika sayıları ve PodDisruptionBudget (karar: PDB yok)

Kurulum hiçbir bileşen için `PodDisruptionBudget` üretmez.

| Bileşen | Üretim replikası | Kurulum yolu | PDB |
|---|---|---|---|
| Kafka (broker/controller) | 3 | `glue/values.yaml` → `kafka.replicas` (`min.insync.replicas: 2`) | **Strimzi yönetir** (elle eklenmez) |
| CNPG `polaris-db` / `keycloak-db` / `superset-db` | 2 | `platform/values/glue.yaml` → `cnpg.*.instances` | **CNPG yönetir** (elle eklenmez) |
| Kafka Connect | 1 | `glue/values.yaml` → `connect.replicas` | yok — tek replika, PDB düğüm boşaltmayı kilitler |
| Trino coordinator / worker | 1 / 2 | `platform/values/trino.yaml` → `server.workers` | yok — coordinator tekildir (chart), worker durumsuzdur |
| Polaris | 1 | `platform/apps/20-polaris.yaml` + `platform/values/polaris.yaml` | yok — tek replika |
| Superset | 1 | `glue/values.yaml` → `superset.replicas` | yok — tek replika |
| JupyterHub hub / Zeppelin | 1 / 1 | `platform/values/jupyterhub.yaml`, `glue/templates/zeppelin.yaml` | yok — tek replika |

**Gerekçe:** tek replikalı bir iş yüküne `minAvailable: 1` vermek düğüm boşaltmayı
(`oc adm drain`) kalıcı olarak kilitler; `maxUnavailable: 1` ise zaten mevcut davranıştır.
Yüksek erişilebilirlik gerekirse **önce replika sayısı** artırılır, PDB kararı o
değişiklikle birlikte verilir. Kararın yeniden ele alınma koşulu
[pre-ship-kontrol-listesi.md](pre-ship-kontrol-listesi.md) §5.1'dedir.

---

## 7. Lisans özeti (teslimat kapsamı)

- Üretime giden küme bileşenlerinin tamamı **Apache-2.0 / BSD-3-Clause / PostgreSQL
  License** (izin verici lisanslar).
- **AGPL-3.0** yalnız iki yerdedir: Grafana (geliştirme izleme yığını — OpenShift'te
  platformun kendi izlemesi kullanılır) ve MinIO (yalnız kind/dev). İkisi de üretim
  kurulumunda yer almaz.
- **SSPL** yalnız e2e MongoDB fixture'ındadır (test verisi üreticisi).
- Kaynak kod olarak teslim edilen her şey (glue chart'ı, Spark işleri, kılavuzlar) bu
  depodadır; üçüncü taraf imajlarda değişiklik yapılmaz (özel imaj yoktur) → türev eser
  yükümlülüğü doğmaz.

---

## 8. Listeyi güncel tutma (J.3.2)

1. Yükseltme, [50-isletme/yukseltme.md](../50-isletme/yukseltme.md) §3'teki sırayla
   yapılır; **aynı PR'da bu sayfadaki satır güncellenir**.
2. PR'ın CI kapısı: `helm unittest glue` + üretim/geliştirme render'ı + kind e2e
   (`.github/workflows/e2e.yaml`).
3. Kurulumdan sonra fiilî sürümler kümeden doğrulanır:

`[bastion]`

```bash
oc -n "$ARGOCD_NS" get applications \
  -o custom-columns='APP:.metadata.name,CHART:.spec.source.chart,REV:.spec.source.targetRevision'
oc -n "$LAKEHOUSE_NS" get kafka lakehouse -o jsonpath='{.spec.kafka.version}{"\n"}'
oc -n "$LAKEHOUSE_NS" get pods \
  -o custom-columns='POD:.metadata.name,IMAJ:.spec.containers[*].image'
```

**Beklenen çıktı** (örnek — ilk komutun chart kaynaklı satırları):

```text
APP            CHART            REV
cert-manager   cert-manager     v1.21.2
cnpg           cloudnative-pg   0.29.0
polaris        polaris          1.7.0
trino          trino            1.42.2
```

Git kaynaklı uygulamaların (`glue`, `custom`, `keycloak-operator`) `CHART` hücresi
doldurulmaz; onların "sürümü" `REV` sütunundaki Git revizyonudur.

**Ters giderse:** `REV` sütunundaki değer bu sayfadaki sürümle uyuşmuyorsa ya küme eski bir
commit'ten sync olmuştur ya da bu sayfa güncellenmemiştir — ikisi de J.3.2 açısından
bulgudur. Kümenin hangi commit'te olduğunu
`oc -n "$ARGOCD_NS" get application glue -o jsonpath='{.status.sync.revision}{"\n"}'` gösterir.

---

## Kontrol listesi

- [ ] Tablolardaki sürümler kümedeki fiilî sürümlerle birebir aynı (§8).
- [ ] Üretim kurulumunda AGPL-3.0 ve SSPL lisanslı hiçbir bileşen yok (§5, §7).
- [ ] Yükseltme PR'ında bu sayfadaki ilgili satır da güncellendi.
- [ ] Hiçbir bileşen için özel imaj üretilmedi (baştaki `grep` denetimi).

## Sonraki bölüm

[kabul-testleri.md](kabul-testleri.md) — bu tablonun şartname maddesi karşılığı ve kabul
kanıtı. Portlar ve servisler: [port-ve-servisler.md](port-ve-servisler.md).
