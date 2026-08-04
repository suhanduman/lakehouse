# Lakehouse — Açık Kaynak Medallion CDC Lakehouse (Kubernetes/OpenShift)

Bu depo, açık kaynak bir lakehouse platformunun **tek versiyonlu,
Helm ile paketlenmiş** kurulumunu barındırır: Kafka/Strimzi, Nessie, Trino,
Iceberg, CNPG Postgres, Apicurio Registry, Keycloak (OIDC + AD federation),
Kafka Connect + CDC/scheduled connector'lar, nginx-access ingest (Spark
Structured Streaming), Zeppelin, self-servis **Lakehouse Console** +
**Kafka UI** ve BI/dashboard katmanı olarak **Apache Superset**.

**Tek doğruluk kaynağı `chart/`'tır.** Kurulum, upgrade ve rollback tek
komutla yapılır; air-gapped/manuel/Helm'siz senaryolar için `manual-install/`
altında chart'tan **üretilen** (generated, elle düzenlenmez) tek-dosya bir
render mevcuttur (bkz. aşağıda "Depo yapısı").

---

## HELM İLE KURULUM

### 1. Ön koşullar

**a) Cluster-scoped operatörler (OLM/OperatorHub) — chart'tan ÖNCE, bir kez:**

```bash
oc apply -f operators/
oc get csv -A | grep -E "cert-manager|external-secrets|cloudnative-pg|strimzi|spark-operator|keycloak"
# Her satır PHASE = Succeeded olana kadar bekleyin (bkz. operators/README.md).
```

Kapsanan 6 operatör: cert-manager, ExternalSecrets Operator, CloudNativePG,
Strimzi, Spark Operator, Keycloak Operator. (NVIDIA GPU Operator, OpenShift
Logging/Loki, Prometheus/Grafana bu chart'ın kapsamı dışıdır — ayrı
alt-projeler.)

**b) Secret'lar — chart'a/values'a hiçbir secret materyali girmez.**
`values.secrets.mode` üç mod sunar: `generated` (varsayılan) — `bootstrap`
script'i kurulumdan önce her Secret'ı güçlü rastgele değerlerle out-of-band
üretir, chart hiçbir Secret/ExternalSecret render etmez (sıfır-manuel-adım
kurulum yolu); `external` — ExternalSecrets Operator + Vault/AD'den
`ExternalSecret` CR'ları (`chart/templates/15-external-secrets.yaml`) ile
otomatik materyalize edilir; `manual` — kurulumdan önce elle oluşturun,
örnek:
  ```bash
  oc -n example create secret generic s3-credentials \
    --from-literal=access-key-id=... --from-literal=secret-access-key=... \
    --from-literal=endpoint=... --from-literal=region=...
  oc -n example create secret generic oidc-credentials --from-literal=issuer-url=... \
    --from-literal=trino-client-id=... --from-literal=trino-client-secret=...
  # mssql / mongo / debezium-src (yalnızca connector'lar enabled ise)
  ```
  Tam liste ve anahtar isimleri: `helm install` sonrası basılan `NOTES.txt`
  çıktısında (bkz. aşağıda "Doğrulama").

CNPG (`pg-*-app`) ve Strimzi (`connect`, `kafka-ui`, `spark-nginx`,
`kafka-cluster-ca-cert`) Secret'ları kendi operatörleri tarafından otomatik
üretilir — bunları elle oluşturmanıza gerek yoktur.

### 2. Kur

```bash
helm install lakehouse ./chart -n example -f chart/values-prod.example.yaml \
  --create-namespace
```

(`chart/values-prod.example.yaml`'ı kopyalayıp kendi ortamınıza göre
düzenleyin — domain, image registry, replica sayıları, secret modu vb.
`values.yaml`'daki varsayılanları geçersiz kılar.) Chart'ı birden çok ortam
için (`values-prod.yaml`, `values-staging.yaml`, …) tekrar tekrar
kullanabilirsiniz; ortam farkı yalnızca `values` dosyasındadır, template'ler
ortam-bağımsızdır. Bu dolu, her-anahtar-görünür OpenShift üretim örneği
yerine minimal bir başlangıç noktası arıyorsanız (yeni müşteri/ortam için
sadece gün-birinde dokunulması gereken anahtarlar), `chart/values-example.yaml`'a
bakın.

Helm tüm kaynakları tek geçişte uygular; namespace/quota → CNPG/Kafka/
Apicurio → Nessie/Trino/Connect/Spark/Keycloak → connector örnekleri/
Console/Kafka UI/Route'lar sırası operatör-reconcile + pod-readiness ile
kendiliğinden oturur (ilk dakikalarda geçici `CrashLoopBackOff` BEKLENİR —
bkz. `operators/README.md` "Kurulum sıralaması").

### 3. Doğrula

```bash
# Bu release'in ürettiği tam manifest:
helm get manifest lakehouse -n example

# Render + namespace/servis-bütünlük statik kontrolü (küme gerekmez):
helm get manifest lakehouse -n example \
  | python3 chart/scripts/helm-check.py - --release-namespace example --service-closure

# Yakınsamayı izle:
watch oc -n example get pods,cluster,kafka,kafkaconnect
```

`helm install` çıktısındaki **NOTES** bölümü (`chart/templates/NOTES.txt`) —
route URL'leri, önkoşul kontrol listesi ve `[KÜME]` doğrulama komutlarının
tamamını basar; örnekler:

```bash
# [KÜME] Trino — Iceberg/Nessie katalog smoke test:
oc -n example exec deploy/trino-coordinator -- \
  trino --execute "SHOW CATALOGS; SHOW SCHEMAS FROM lakehouse;"

# [KÜME] Console — backend health + UI:
oc -n example exec deploy/console-backend -- curl -sf localhost:8080/healthz
open https://console.<global.domain>

# [KÜME] Kafka UI:
oc -n example rollout status deploy/kafka-ui
open https://kafka-ui.<global.domain>
```

A (ingest omurgası) ve B (Console) alt-projelerinin `[KÜME]` doğrulama
komutları (`docs/egitim/ingest-omurgasi-egitim.md`,
`docs/egitim/console-egitim-eki.md`) chart'ın render ettiği isimlerle
(bootstrap, servis, secret adları) birebir tutarlıdır — `_helpers.tpl`
üreten ve tüketen tarafın aynı kanonik adı kullanmasını garanti eder.

### 4. Upgrade / Rollback

```bash
helm upgrade lakehouse ./chart -f chart/values-prod.example.yaml -n example
helm history lakehouse -n example
helm rollback lakehouse <revision> -n example
```

**Stateful uyarı:** CNPG (Postgres) ve Kafka (Strimzi) major-version
upgrade'leri operatör-yönetimlidir — chart yalnızca CR spec'ini değiştirir,
veri migration'ı ilgili operatörün sorumluluğundadır. Operatör/CRD
upgrade'i OLM tarafında yapılır (chart kapsamı dışı).

---

## İzleme (Monitoring — D1)

Chart, `components.monitoring: true` iken (varsayılan `values-prod.example.yaml`'da
açık) platform metriklerini toplayıp görselleştiren İzleme katmanını ekler.
Metrik toplama motoru **OpenShift user-workload-monitoring (UWM)**'dir — bu chart
Prometheus/Thanos KURMAZ; yalnızca `ServiceMonitor`/`PodMonitor`/`PrometheusRule`
CR'larını (`monitoring.coreos.com/v1`) ve **Grafana**'yı (OSS + provisioning)
ekler. Kapsanan hedefler: Kafka broker (JMX), Kafka Connect, Trino, Nessie,
Apicurio, Console, Spark, CNPG Postgres.

### 1. UWM'i aç (ön koşul — chart DIŞI, bir kez)

UWM CRD'leri (`monitoring.coreos.com/v1`) operatör CRD'lerinden ayrıdır ve
`operators/` ile GELMEZ; küme yöneticisi UWM'i açmalıdır:

```bash
# openshift-monitoring/cluster-monitoring-config ConfigMap'inde:
#   enableUserWorkloadMonitoring: true
# (ayrıntı: operators/README.md → UWM ön koşulu)
oc -n openshift-user-workload-monitoring get pods   # prometheus-user-workload* Running olmalı
```

`--set prereqCheck.enabled=true` ile kurarsanız, monitoring açıkken bu CRD'nin
varlığı hem template-zamanı (`_prereq-check.tpl`) hem de pre-install Job'da
(`prereq-check-job.yaml`) kontrol edilir; yoksa net bir hatayla kurulum durur.

### 2. Chart'ı kur (monitoring açık)

```bash
helm install lakehouse ./chart -f chart/values-prod.example.yaml -n example
# monitoring zaten açık; kapatmak için: --set components.monitoring=false
```

Grafana, UWM'in tenancy-aware Thanos querier'ından (`:9091`) kendi
ServiceAccount bearer token'ı ile okur — token **süresi dolmayan**
`kubernetes.io/service-account-token` tipli `grafana-sa-token` Secret'ından gelir
(read-only `cluster-monitoring-view` binding; rotasyon-kaynaklı 401 yok).

### 3. Grafana'ya OIDC ile gir

`monitoring.grafana.oidc.enabled: true` (prod varsayılanı) iken Grafana'ya
Keycloak OIDC ile girilir:

```
https://grafana.<global.domain>
```

Confidential `grafana` Keycloak client secret'ı ile Grafana'nın mount ettiği
`grafana-oidc` Secret'ı TEK kaynaktan gelir (`monitoring.grafana.oidc.clientSecret`
— materyal git'e girmez; `secrets.mode` ile ExternalSecret/Vault ya da elle
`oc create secret`). OIDC kapalıysa (`enabled: false`) `grafana-admin`
Secret'ındaki admin kullanıcı/parola ile girilir.

### 4. Dashboard'lar ve alert'ler

- **Dashboard'lar** (dosya-provisioning, `monitoring.dashboards.*`): Kafka, CNPG,
  Trino, Iceberg, Console, Platform — her biri minimal-geçerli panel(ler)le gelir.
- **Alert'ler** (`PrometheusRule`, `monitoring.alerts.*`): connector FAILED, Kafka
  consumer lag, DLQ, CNPG, Trino ve pod-sağlık uyarıları. Alertmanager
  yönlendirmesi UWM tarafında yapılandırılır.

Metrik seri adları/PromQL'ler sürüm-bağımlı olabilir; doğrulama runbook'u için
`docs/POC-DOGRULA-cluster-checklist.md`'ye bakın.

---

## Superset (BI)

`components.superset: true` iken (medium/large tier'larda açık —
`values-medium.yaml`/`values-large.yaml`; dev tier'da kapalı) chart, self-servis
BI/dashboard katmanı olarak **Apache Superset**'i ekler: Deployment + Service +
PDB + init-Job + ConfigMap, ayrıca metadata veritabanı için kendi CNPG
cluster'ı — **`pg-superset`**.

Girişte confidential Keycloak `superset` OIDC client'ı kullanılır — secret'ı
diğer client'lardaki desenle aynı şekilde `secrets.mode` üzerinden tek
kaynaktan gelir.

**Bilinen sınırlama (v1):** "v1 registers the Trino connection with
`superset.trino.authMode: none` (works when `auth.oidc.enabled: false`).
Under OIDC-enabled Trino the connection renders but queries require the
JWT-authenticator follow-up (next Consumption slice) — see
docs/superpowers/spikes/2026-08-04-superset-trino-oidc.md."

---

## İngest — bilinen tradeoff'lar ve gerekçeler

Aşağıdaki üç nokta, mevcut ingest mimarisinin bilinçli tradeoff'ları ve
gerekçeleridir:

- **Console şu an CR'ları doğrudan k8s API'ye apply ediyor.** Bu bilinçli bir
  tradeoff'tur (self-servis hız), ancak **prod önerisi: GitOps-PR modeli**
  (Console formu → values.yaml/manifest repo'suna PR → ArgoCD/Flux senkronize
  eder) — çoklu-kullanıcı drift'ini ve config'in gerçek küme durumuyla
  split-brain olmasını önler.
- **Kafka Connect imajı önceden-build'lidir** (`images/connect/Dockerfile`,
  `KafkaConnect.spec.image`) — Strimzi'nin in-cluster `spec.build`'i yerine.
  **Neden:** Apache Iceberg `kafka-connect-runtime`'ı Maven Central'da veya
  GitHub release olarak yayınlamıyor (yalnızca kaynaktan gradle ile derleniyor);
  eskiden zip yayınlayan Tabular/Databricks fork'u arşivlendi. Bu yüzden imaj
  kaynaktan-derlenen Iceberg 1.7.1 runtime'ı bake eder ve versiyonlanmış bir
  registry image'ı olarak tüketilir.
- **Mongo scheduled/batch (HWM) yol, hard-delete'leri yakalayamaz** —
  yüksek-su-işareti tabanlı periyodik okuma yalnızca yeni/güncellenen
  dokümanları görür; kaynaktan fiziksel olarak silinen bir doküman batch'te
  hiç fark edilmez. **Delete fidelity gerekiyorsa Mongo CDC (Debezium) yolu
  kullanılmalı**, scheduled/batch değil.

---

## Depo yapısı

```
chart/                # TEK DOĞRULUK KAYNAĞI — Helm chart (kurulum/upgrade/rollback buradan)
  templates/            component template'leri + _helpers.tpl + NOTES.txt
  values.yaml           varsayılanlar (tüm bileşen config + enabled flag'leri)
  values-prod.example.yaml   dolu OpenShift üretim örneği (kopyala/uyarla)
  values-example.yaml        minimal onboarding başlangıç noktası (yeni müşteri/ortam)
  scripts/helm-check.py      render → namespace/servis-bütünlük statik kontrolü
  tests/                helm unittest + integrity.sh (tam ns+servis-kapalılık testi)
operators/            # cluster-scoped OLM operatör bootstrap (helm install ÖNCESİ)
tools/                # chart-DIŞI operasyonel araçlar (self-servis scaffold CLI + spark job'lar + manifest validator):
  templates/            scaffold-source.sh (CLI self-servis yeni-kaynak sihirbazı) + connector/kafkatopic/iceberg şablonları
  jobs/                 Spark job script'leri (spark-py image'ına gömülü, local:// ile referans)
  validate.sh           statik doğrulama (+ `--helm` modu: helm lint + template + helm-check.py)
  README.md             tools/ kapsamı + hâlâ geçerli runbook'lar (AD stratejisi, image build, DLQ vb.)
manual-install/       # chart'tan ÜRETİLEN (generated) tek-dosya manuel kurulum — air-gapped/Helm'siz senaryolar
  render.sh             `helm template chart/ ...` -> manifests/lakehouse-<ns>.yaml üretir
  manifests/            üretilen (commit'li) çıktı — elle düzenlenmez, chart değişince yeniden üretilir
console/              # Lakehouse Console (backend FastAPI + frontend React/Vite) kaynak kodu
docs/
  egitim/                eğitim ekleri (ingest omurgası, console, helm-kurulum)
  superpowers/specs,plans/  tasarım belgeleri + implementasyon planları (tarihsel kayıt)
```

**`tools/templates/scaffold-source.sh`** ve **`tools/jobs/`** chart-dışı,
hâlâ canlı akışlardır: `scaffold-source.sh` CLI'dan self-servis yeni-kaynak
ekleme aracıdır (Console'un GUI karşılığıyla birlikte var olur, onun yerini
almaz); `jobs/*.py` Spark image'larına gömülü olup `local://`-path ile
referans verilir — chart template'i değildir, yalnızca image build girdisidir.

---

## Eğitim dokümanları

- `docs/egitim/ingest-omurgasi-egitim.md` — Debezium CDC + Aiven JDBC
  scheduled + Spark + nginx ingest, hands-on kılavuz.
- `docs/egitim/console-egitim-eki.md` — Self-Service Console + Kafka UI eki.
- `docs/egitim/helm-kurulum-egitim-eki.md` — **Helm kurulum eğitim eki**:
  chart yapısı, values, install/upgrade/rollback, sıralama mantığı
  (operatör-reconcile), namespace-farkındalık + servis-bütünlük (helm-check).

## Diğer

- PoC ortamı: `poc-manifests/` (tek-pod, MinIO — bkz. o klasörün kendi
  belgeleri; bu README yalnızca üretim/Helm kurulumunu kapsar).
- Mimari karar kaydı: `Mimari_Karar_Belgesi.md`.
- Tasarım/implementasyon geçmişi: `docs/superpowers/specs/`,
  `docs/superpowers/plans/` (tarihsel — güncel davranış için chart'a bakın).
