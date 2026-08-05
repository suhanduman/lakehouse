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

### Trino machine-auth (b1) — Superset prod-serving

`auth.oidc.enabled: true` iken (medium/large tier'larda varsayılan) Trino
`OAUTH2,JWT` zincirli bir authenticator ile çalışır: **insanlar** tarayıcı SSO
(`oauth2`) ile, **makine client'ları** (Superset, ve b2'deki dbt) Keycloak'ın
verdiği Bearer JWT (`jwt`) ile kimlik doğrular. `jwt` girişi Keycloak JWKS'e
karşı doğrulanır; `oauth2` insan yolu değişmeden kalır (byte-for-byte).

- **Keycloak service-account client — `svc-superset-trino`:** `client_credentials`
  grant'lı, confidential bir client; access token'ına zorunlu bir **`aud: trino`**
  audience mapper'ı eklenir (mapper olmadan Trino her isteği 401'ler). Secret'ı
  `superset.trino.oidc.clientSecret` üzerinden diğer OIDC client secret'larıyla
  aynı konvansiyonla (`secrets.mode`) tek kaynaktan sağlanır.
- **Token akışı — sidecar + `DB_CONNECTION_MUTATOR` (token Postgres'te asla
  saklanmaz):** Superset pod'una eklenen bir refresher sidecar, her
  `superset.trino.tokenRefreshSeconds` (varsayılan 600s) saniyede Keycloak'tan
  yeni bir `client_credentials` token'ı çekip paylaşılan bir `emptyDir`
  (`trino-token`) dosyasına **atomik** yazar (`token.tmp` yaz → `mv` ile
  rename — yarım/boş token'a karşı). `superset_config.py`'deki
  `DB_CONNECTION_MUTATOR`, her Trino engine kurulumunda bu dosyayı okuyup
  `connect_args["auth"]`'a enjekte eder. Superset'in kayıtlı Trino bağlantısı
  **token içermeyen** (tokenless) bir temel URI'dır — dönen token yalnızca pod
  belleğinde/emptyDir'de yaşar, init-Job'ın yazdığı metadata veritabanına asla
  yazılmaz.
- **`pool_recycle` < token ömrü ilişkisi:** mutator token'ı engine kurulumunda
  enjekte eder, ama Superset DBAPI bağlantılarını pool'lar — token'ından daha
  uzun yaşayan eski bir pool'lu bağlantı Trino'da 401 alır. Bu yüzden init-Job,
  Trino bağlantısını `extra.engine_params.pool_recycle`
  (`.Values.superset.trino.poolRecycleSeconds`, varsayılan 800s) ile Keycloak
  token ömrünün (900s, `10-keycloak.yaml`) **altında** kaydeder — bağlantılar
  token'ları expire olmadan recycle edilir.
- **Trino coordinator internal TLS (`:8443`, cert-manager, hedefli — blanket
  mTLS değil):** JWT/OAuth2 auth güvenli bir transport gerektirdiği için
  coordinator'a cert-manager `Certificate`'ı ile internal TLS eklendi
  (`trino-coordinator.<ns>.svc`, PKCS12 keystore, `http-server.https.enabled=true`
  `:8443`'te) — Keycloak'ın kendi internal TLS'ini sonlandırma hassasına aynı
  hedefli yaklaşım; `allow-insecure-over-http` bilinçli olarak reddedilir.
  Platformun mevcut internal güvenlik duruşu NetworkPolicy segmentasyonu +
  edge TLS'tir; cluster-genelinde internal mTLS (service mesh) ayrı bir Day-2
  kalemidir.
- **Özel Superset image'ı (Trino driver):** Superset'in Trino'ya bağlanabilmesi
  için `trino[sqlalchemy]` driver'ı gömülü özel bir image kullanılır
  (`.Values.superset.supersetImageTag`) — upstream `apache/superset` image'ı
  bu driver'ı içermez.

**Bilinen sınırlama:** B1 machine-auth (Superset↔Trino JWT + internal TLS) is
helm-unittest + docker-spike verified (superset 4.1.1 mutator signature, trino
439 HTTPS:8443 boot, trino[sqlalchemy]==0.338.0 driver); the full
Keycloak→Trino-over-TLS→Superset end-to-end + token-rotation-across-pool_recycle
is an OpenShift UAT deliverable (Keycloak is not deployed and Trino is
scaled-0 in the microk8s dev cluster). The custom Superset image must be
built+pushed (arm64 for microk8s) before deploy.

Medium/large tier'larda (`components.superset: true`) üç Superset secret'ı —
`superset.secretKey`, `superset.adminPassword`, `superset.oidc.clientSecret` —
ilk boot'tan ÖNCE `secrets.mode`'a göre out-of-band provision edilmelidir
(keycloak-admin-credentials ile AYNI konvansiyon); aksi halde superset ve
superset-init pod'ları `CreateContainerConfigError`'da kalır.

---

## dbt-trino Certified Gold (b2)

`components.dbt: true` iken (medium/large tier'larda açık — dev tier'da
kapalı, bkz. `values-medium.yaml`/`values-large.yaml`) chart, Silver üzerine
**dbt-trino** ile transform edilen, iş-hazır (business-ready) bir
**Certified Gold** katmanı ekler: `dbt-build` CronJob + `dbt-profiles`
ConfigMap + (sağlanmışsa) `dbt-trino-oidc-credentials`/`dbt-repo-deploy-key`
Secret'ları (`chart/templates/20-dbt.yaml`).

- **`svc-dbt-trino` — tek-yazar (sole-writer) modeli:** Superset'teki (b1)
  aynı desenle, confidential bir Keycloak service-account client'ı
  (`client_credentials` grant, zorunlu `aud: trino` mapper'ı ile —
  mapper olmadan Trino her isteği 401'ler) dbt'nin Trino kimliğidir
  (`10-keycloak.yaml`). `lakehouse.gold` şemasının (Nessie/Iceberg-REST
  `lakehouse` katalogunun sertifikalı, iş-hazır bölgesi) TEK yazarı budur —
  başka hiçbir principal oraya yazmaz.
- **Yazma-sınırlama (write-confinement) ACL'i — dbt Depo'yu bozamaz:**
  `06-trino-ha.yaml`'daki paylaşılan `rules.json`, `components.dbt` ile
  gated üç ek kural katmanıyla `svc-dbt-trino`'yu daraltır: şema düzeyinde
  yalnızca `lakehouse.gold`'un owner'ı; tablo düzeyinde `lakehouse.gold`
  üzerinde tam DML (SELECT/INSERT/DELETE/UPDATE/OWNERSHIP) YALNIZCA,
  `lakehouse` katalogunun GERİ KALANI (Silver dahil) üzerinde ise yalnızca
  SELECT. Yani dbt Silver'ı okuyabilir ama asla yazamaz — Gold'un dışına
  hiçbir DDL/DML sızmaz, depo (Silver) yanlışlıkla dbt tarafından
  bozulamaz.
- **`dbt-build` CronJob (clone → per-run JWT → dbt build):**
  `.Values.dbt.schedule` (varsayılan `0 3 * * *`) ile çalışan CronJob her
  run'da (1) `dbt.repo.url`'i salt-okunur bir SSH deploy key
  (`dbt-repo-deploy-key`) ile klonlar, (2) Keycloak'tan `client_credentials`
  grant'ıyla TEK SEFERLİK (per-run) bir Bearer JWT çeker ve
  `DBT_TRINO_JWT` env var'ına yazar (Postgres/disk'te asla kalıcı
  saklanmaz — Superset mutator'ının "token hiçbir yerde saklanmaz"
  ilkesiyle aynı), (3) `dbt-profiles` ConfigMap'inden gelen `profiles.yml`'i
  repo'ya kopyalayıp `dbt build` çalıştırır. `profiles.yml`'deki
  `jwt_token: "{{ env_var('DBT_TRINO_JWT') }}"` alanı dbt'nin KENDİ
  Jinja'sıdır (Helm'in değil) — host/port/catalog/schema Helm tarafından
  baked-in gelir, dbt yalnızca JWT'yi runtime'da okur.
- **Git tek-doğruluk-kaynağı + `examples/dbt-gold/`:** dbt projesinin
  kendisi bu chart'ın DIŞINDA, müşterinin kendi git deposunda yaşar
  (`dbt.repo.url`+`dbt.repo.branch`) — chart yalnızca runner'ı taşır,
  modelleri değil. `examples/dbt-gold/` (Silver `customers` üzerinde bir
  model + test) mekanizmanın çalıştığını göstermek için bir BAŞLANGIÇ
  NOKTASIDIR; customer kendi deposunu sağlar. (In-cluster bir
  Gitea-seed out-of-box demo'su bilinçli olarak bir sonraki iterasyona
  ertelendi — v1 mekanizma + `examples/dbt-gold/` starter + `dbt.repo.url`
  clone ile gelir.)
- **Özel dbt image'ı:** Upstream `dbt-core` image'ı Trino adaptörünü
  içermediği için özel bir image kullanılır (`images/dbt/Dockerfile`,
  `dbt-trino==1.10.3`, `.Values.versions.dbtImageTag`) — Superset'in özel
  Trino-driver image'ıyla aynı gerekçe.
- **Internal-CA trust:** CronJob, b1'in `trino-internal-tls` Secret'ının
  `ca.crt`'ini `/etc/trino-ca/ca.crt`'a mount eder; `profiles.yml`'in
  `cert:` alanı buna işaret eder — dbt, Trino coordinator'ın internal
  TLS'ini (aynı cert-manager `Certificate`, `:8443`) doğrular.

**Bilinen sınırlama:** b2 dbt-Gold is helm-unittest + docker-spike verified
(dbt-trino image builds + parses the example; Trino Iceberg-REST write
capability + the svc-dbt-trino principal confirmed); the full
Keycloak→dbt→Trino→lakehouse.gold end-to-end (clone, per-run JWT,
materialize, and the write-to-depo-denied check) is an OpenShift UAT
deliverable. The custom dbt image must be built+pushed (arm64 for
microk8s), the customer must set dbt.repo.url + provide a read-only deploy
key, and routes.certIssuer must issue for *.svc.cluster.local (same
assumption as keycloak-tls).

Medium/large tier'larda (`components.dbt: true`) iki dbt secret'ı —
`dbt.oidc.clientSecret`, `dbt.repo.deployKey` — ilk `dbt-build` run'ından
ÖNCE `secrets.mode`'a göre out-of-band provision edilmelidir
(keycloak-admin-credentials ile AYNI konvansiyon); aksi halde `dbt-build`
pod'u `CreateContainerConfigError`'da kalır.

---

## Spark History Server (Consumption slice d)

`components.sparkHistory: true` iken (medium/large tier'larda açık —
`values-medium.yaml`/`values-large.yaml`; dev tier'da kapalı) chart, batch ve
streaming Spark job'ları için gerçek bir **Spark debug UI** ekler: stage'ler,
SQL planları, executor metrikleri ve job zaman çizelgesi görünür hale gelir
(`chart/templates/21-spark-history.yaml` — Deployment + Service + PDB,
`spark-py` image'ı yeniden kullanılır).

- **Etkinleştirme:** `components.sparkHistory: true` + `sparkHistory.s3Endpoint`
  ayarı — bu, kullanılan S3 endpoint'iyle (MinIO/non-AWS) EŞLEŞMEK
  ZORUNDADIR; `s3-credentials` Secret'ının `endpoint`'iyle aynı değer
  olmalıdır (hadoop-aws bunu explicit ister). Dev tier'da boş bırakılır
  (sparkHistory zaten kapalı orada).
- **`spark-events` bucket + rolling event log'lar:** History Server'ın
  okuduğu event log dizini `s3a://spark-events/`
  (`sparkHistory.eventLogDir`) — bucket, `bucket-init` Job'ı tarafından
  diğer bucket'larla birlikte otomatik oluşturulur
  (`chart/templates/18-bucket-init.yaml`). Event log'lar
  `spark.eventLog.rolling.enabled: true` +
  `spark.eventLog.rolling.maxFileSize: 128m` ile ROLLING yazılır — sürekli
  çalışan streaming job için tek-dosya event log'un sınırsız büyümesi
  yerine, güvenli/parçalı bir yazım modeli.
- **Job'lar nasıl opt-in olur — paylaşılan `baseConf`:** eventLog konfigürasyonu
  chart'ın PAYLAŞILAN `lakehouse.spark.baseConf` helper'ına (`_helpers.tpl`)
  eklenmiştir, `components.sparkHistory` ile gated — yani hem mevcut batch
  hem de streaming job'lar (05-spark-operator.yaml) OTOMATİK olarak eventLog
  yazmaya başlar, ayrı bir per-job konfigürasyon gerekmez. `components.sparkHistory:
  false` iken bu bloklar hiç render edilmez (`05-spark-operator.yaml`'ın
  render'ı byte-for-byte değişmez). Gelecekteki/müşteri job'ları da AYNI
  `baseConf` helper'ını kullandığı sürece hiçbir ek iş yapmadan History
  Server'a event log göndermeye başlar.
- **UI:** `spark-history.<global.domain>` adresinden erişilir (platforma göre
  Route veya Ingress — diğer bileşenlerle aynı `lakehouse.routeHost` deseni).

**Bilinen sınırlama:** Spark History is helm-unittest + docker-spike verified (spark-py eventLog->s3a->HistoryServer loop against MinIO); the full in-cluster loop (real jobs write event logs, the server lists them, UI renders stages/SQL) is an OpenShift UAT deliverable. AUTH: the History UI is EDGE-ONLY (TLS Route, no OIDC) — it exposes job internals (SQL/config) to anyone who can reach the host; front it with an oauth2-proxy (Keycloak) before exposing it beyond a trusted network — tracked hardening follow-up. sparkHistory.s3Endpoint must match the s3-credentials endpoint; the spark-py image must be built+pushed (arm64).

---

## Zeppelin `%trino` interpreter (Consumption slice c1)

`auth.oidc.enabled: true` iken (medium/large tier'larda açık; dev'de kapalı)
Zeppelin notebook'larına **read-only bir `%trino` JDBC interpreter'ı** eklenir —
böylece analistler AYNI notebook içinde hem `%spark`/`%pyspark` (hesaplama) hem
de `%trino` (hızlı interaktif MPP SQL, Superset + dbt ile aynı Trino motoru)
çalıştırır. Trino'ya **b1 machine-auth'u yeniden kullanan** salt-okunur bir
servis kimliğiyle (`svc-zeppelin-trino`, `client_credentials` + `aud:trino`)
bağlanır; Trino'nun `.*` read-only kuralına düşer, YAZMA yetkisi yoktur
(gold-write yalnızca `svc-dbt-trino`'dadır).

- **İmaj — ince katman (thin layer):** `apache/zeppelin` JDBC sürücüsü
  içermez. `images/zeppelin/Dockerfile`, **platformun kendi Zeppelin imajının
  ÜZERİNE** (`ARG ZEPPELIN_BASE`, build-arg ile override edilir)
  `trino-jdbc:439`'u (Trino sunucusuyla — `trinodb/trino:439` — sürüm-eşli)
  `${ZEPPELIN_HOME}/interpreter/jdbc`'ye bake eder. Stock apache/zeppelin'e
  REBASE ETMEZ — çünkü platform imajı LDAPS için kurumsal AD CA sertifikasını
  JVM truststore'una ve `/zeppelin` dizin düzenini gömer; rebase bunları
  sessizce düşürüp AD login'i kırardı. Sürücü **maven `dependencies` yerine
  BAKE edilir**: air-gap kümeler Maven Central'a erişemez, VE her
  interpreter-ayar PUT'u (token-refresher her rotasyonda bir tane yapar) aksi
  halde yeniden indirme + non-READY penceresi tetiklerdi. İç build
  `versions.zeppelinImageTag` + `global.imageRegistry` (superset/dbt gibi).
- **Interpreter provisioning:** `zeppelin-trino-interpreter` ConfigMap'i
  `interpreter.json`'ı taşır (`dependencies: []` — sürücü baked; URL
  `jdbc:trino://<trino-coordinator>:8443/<catalog>?SSL=true&SSLTrustStorePath=/mnt/trino-tls/ca.crt`).
  Container command'i, bu seed dosyasını başlangıçta Zeppelin'in conf
  dizinine KOPYALAYIP normal entrypoint'i exec eder (interpreter.json'ı
  doğrudan bind-mount etmek Zeppelin'in çalışma-anı yeniden-yazımını "Device
  or resource busy" ile çökertir — spike bulgusu). TLS: PEM `ca.crt`
  DOĞRUDAN `SSLTrustStorePath` ile çalışır, JKS/PKCS12 dönüşümü gerekmez
  (trino-client `loadTrustStore()` önce PEM dener).
- **Token enjeksiyonu — refresher sidecar:** Zeppelin'in JDBC interpreter'ı
  STATİK bir `accessToken` okur (Superset'in per-connection mutator'ı gibi bir
  hook yok). Bir sidecar döngüsü: `svc-zeppelin-trino` JWT'sini çeker →
  Zeppelin'e admin olarak login olur → `GET`/patch/`PUT
  /api/interpreter/setting/trino` (PUT interpreter'ı yeniden yükler, ayrı
  restart gerekmez). REST auth için, `shiro.ini`'ye dokunmak yerine
  **Zeppelin admin AD grubunun üyesi, operatör-tarafından-sağlanan bir AD
  servis hesabı** kullanılır (`zeppelin.trino.refresher.restUsername` +
  `restPassword`). PUT çalışan `%trino` paragraflarını KESTİĞİNDEN,
  `zeppelin.trino.tokenRefreshSeconds` bilerek uzundur (3000s) → kesinti
  seyrek. Hata halinde exponential backoff (5→60s), son-iyi durumu korur,
  token/parola loglanmaz.
- **Gating:** hepsi `components.zeppelin` && `auth.oidc.enabled` (+ `:8443`
  dinleyicisini yöneten mevcut `superset.trino.tls.enabled` coherence guard'ı)
  ile gated. Dev'de (`auth.oidc.enabled: false`) Zeppelin bugünküyle
  BYTE-FOR-BYTE aynı render edilir — yalnızca Spark-read, Trino interpreter yok.
- **Operatör gereksinimleri:** `images/zeppelin`'i platform base ile build
  et+push et; `zeppelin.trino.oidc.clientSecret`'i sağla (Keycloak
  `svc-zeppelin-trino` client'ıyla tek-kaynak); refresher AD hesabını
  (admin grubunda) + `zeppelin.trino.refresher.restPassword`'ü sağla.

**Bilinen sınırlama:** c1 helm-unittest + docker-spike doğrulanmıştır (interpreter
seeding, PEM TLS trust, ve `default.accessToken` → trino-jdbc plumbing hepsi
canlı bir `apache/zeppelin` + `trinodb/trino:439` konteynerine karşı doğrulandı;
thin-layer imaj mekaniği stock-base stand-in build ile). Tam
Keycloak → Zeppelin (sidecar login) → Trino (gerçek JWKS-doğrulamalı JWT) E2E'si
bir **OpenShift UAT** teslimatıdır (burada Keycloak yok + Trino scaled-0). UAT'ta
netleşecek: platform `zeppelin:1.0` imajının trino-jdbc'yi zaten içerip
içermediği (thin-layer her iki durumda güvenli — üzerine yazar); GET/PUT REST
zarfının tam şekli; ve platform Zeppelin'inin chart'ın shiro'sunu (OIDC ile
override etmeyip) kullandığının doğrulanması. Interpreter identity SALT-OKUNUR;
per-user Trino kimliği (Zeppelin Shiro/AD ile auth eder, OIDC değil) Day-2
per-user-identity slice'ıdır; sandbox WRITE (`%pyspark` → `sandbox_<dept>`)
slice (c2)'dir.

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
