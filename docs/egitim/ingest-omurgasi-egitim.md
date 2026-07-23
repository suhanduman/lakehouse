# İngest Omurgası — Eğitmen & Hands-On Kılavuzu

**Kapsam:** `prod-manifests/11-apicurio-registry.yaml` → `12-kafka-connect.yaml` →
`13-connectors/*` → `14-nginx-ingest/*` (+ `03-kafka-strimzi.yaml` revizyonu,
`05-spark-operator.yaml` bakım CronJob'u). Bu doküman hem **kümede ilk
kurulum/prova hazırlığı** hem de **canlı hands-on eğitim** verirken
izlenecek **tek kaynaktır**. Spec: `docs/superpowers/specs/2026-07-17-ingest-omurgasi-design.md`
(özellikle §13). Runbook detayları: `tools/README.md` ("İNGEST
OMURGASI" bölümü) — buradaki her komut o runbook'la birebir tutarlıdır.

> **Not:** Bu dokümanda örnek olarak geçen `prod-manifests/0X-*.yaml` dosya
> yolları, kurulu chart'ta `chart/templates/0X-*.yaml` karşılığında
> parametrelenmiş halde bulunur (davranış/isim/namespace korunur, yalnızca
> `.Values.*` ile parametrelenir); ham (Helm'siz) hali `legacy-manifests/`'te
> referans olarak durur. Aşağıdaki komutlar ve CR/servis isimleri (bootstrap,
> topic, connector, secret adları) chart'ın render ettiğiyle birebir aynıdır.
> Kurulum `oc apply -f <NN>.yaml` yerine `helm install/upgrade lakehouse
> ./chart` ile yapılır (bkz. kök `README.md` → "HELM İLE KURULUM",
> `docs/egitim/helm-kurulum-egitim-eki.md`).

Namespace tüm komutlarda `example` (`NS=example` kısayolu README ile aynı).

---

## 1. Büyük resim

```
KAYNAKLAR (OpenShift DIŞI VM'ler)     İNGEST DÜZLEMİ (OpenShift/example)          LAKEHOUSE
───────────────────────────────       ─────────────────────────────            ─────────
                    ┌─ CDC:      Debezium (txn-log) ──┐
MSSQL (OgrenciDB) ──┤                                  ├→ Kafka topic (Avro) ──┐
                    └─ SCHEDULED: Aiven JDBC ─────────┘                        │
                                                                                ├→ Kafka Connect
                    ┌─ CDC: Debezium (change stream) → Kafka topic (JSON) ─────┤   (spec.build custom imaj)
MongoDB (lms) ──────┤                                                          │       │
                    └─ (bu POC'te scheduled yol örneklenmedi)                  │       ▼
                                                                                │   Iceberg Sink (fan-out,
nginx VM'leri ── FluentBit (edge ajan) ──[external listener 9094]→ Kafka topic─┘   _target_table routing)
  (access.log)                                    (nginx-access, ham satır)            │
                                                          │                            ▼
                                                          └→ Spark Structured    Nessie REST katalog
                                                             Streaming (parse +  → S3
                                                             IP hash/KVKK)       (kaynak-başına bucket)
                                                                     │                 │
                                                                     └────────────────→┘
                                                                                        │
ŞEMA: Apicurio Registry (Avro, CNPG'de saklı)                                          ▼
BAKIM: Spark CronJob (rewrite_data_files + expire_snapshots + remove_orphan_files)   Trino (tek katalog,
                                                                                       cross-namespace JOIN)
                                                                                       → Superset/Jupyter/Zeppelin
```

Her bileşenin **"neden var"** tek cümlesi:

| Bileşen | Neden var (tek cümle) |
|---|---|
| **Kafka** (internal 9093 TLS+SCRAM, external 9094 route+SCRAM) | Kaynakları tüketicilerden ayıran, dayanıklı (RF=3) bir tampon — kaynak yavaşlasa/dursa da olaylar kaybolmaz. |
| **Apicurio Registry** | İlişkisel (Avro) kaynakların şemasını tek yerde tutar; kaynak-tarafı şema değişimi (BACKWARD uyumluluk) pipeline'ı kırmadan geçer. |
| **Kafka Connect** (`spec.build`) | Tüm connector'ları (source + sink) barındıran tek ingest platformu; custom plugin imajı deklaratif ve tekrarlanabilir derlenir — elle Docker build/`pip install` yok. |
| **Debezium (CDC)** | Kaynağın transaction log'unu (MSSQL) veya change stream'ini (Mongo) okuyarak insert/update/**hard-delete**'i near-realtime yakalar — tek yol delete'i görebilen. |
| **Aiven JDBC (scheduled)** | CDC altyapısı gerekmeyen, periyodik/bilinen-SQL kaynaklar için basit, düşük-maliyetli alternatif (delete umursanmıyorsa). |
| **Iceberg Sink** (fan-out) | Tüm ingest topic'lerini (`cdc.*`, `jdbc.*`, `mongo.*`) **tek connector** üzerinden, kaynağa özgü Iceberg tablosuna (`_target_table` alanına göre) yönlendirir — Model X'in (kaynak-başına bucket/namespace) uygulayıcısı. |
| **nginx: FluentBit + Spark** | nginx bir veritabanı değil, dosya bazlı log kaynağıdır; edge ajan (FluentBit) log'u Kafka'ya taşır, Spark Structured Streaming ayrıştırıp KVKK gereği IP'yi hash'ler ve Iceberg'e yazar (sink'i değil Spark'ı kullanır). |
| **Iceberg bakım (Spark CronJob)** | Upsert/equality-delete (Merge-on-Read) zamanla küçük dosya ve delete-dosyası biriktirir; bakım olmadan Trino okuma performansı bozulur — opsiyonel değil, mekanik bir gerekliliktir. |
| **Nessie + Trino** | Kaynak-başına ayrı bucket/namespace olsa da **tek katalog** üzerinden sunulur → analist tek SQL sorgusunda kaynaklar arası JOIN yapar. |

---

## 2. Neden bu sıra (bağımlılık zinciri)

Kurulum sırası (README "İngest omurgası → Kurulum sırası" ile birebir):
**Apicurio → Kafka (revize) → Kafka Connect (build) → bucket/namespace
bootstrap → Iceberg Sink → source connector'lar → nginx → bakım.**

Her adım bir öncekine **neden** muhtaç, somut olarak:

1. **Kafka (`03-kafka-strimzi.yaml`, revize) her şeyden önce sağlıklı olmalı** —
   Connect'in kendi iç durumunu (`connect-cluster-configs/offsets/status`,
   RF=3, compact) sakladığı yer burasıdır; bu topic'ler yoksa Connect cluster'ı
   hiç ayağa kalkmaz. Ayrıca `connect`/`debezium-src`/`fluentbit` `KafkaUser`
   ACL'leri burada tanımlanır — sonraki her adım bu kimliklere güvenir.
2. **Apicurio, Connect'ten önce ayakta olmalı** — Connect'in custom imajı
   `apicurio-avro-converter` plugin'ini içerir ve her Avro-yolu connector
   config'i (`dbz-mssql-students`, `jdbc-mssql-scheduled`) doğrudan
   `apicurio.registry.url: http://apicurio-registry:8080/...` referans verir.
   Registry olmadan connector `CREATED` durumuna geçer ama ilk mesajı Avro'ya
   çevirmeye çalıştığı anda task `FAILED` olur — "önce ayakta olmalı" soyut bir
   tercih değil, ilk `PUT` çağrısında patlayan somut bir bağımlılıktır.
3. **Kafka Connect (`12-kafka-connect.yaml`, `spec.build`) sıradaki** — her
   connector bir `KafkaConnector` CR'ı olarak bu cluster'a "sunulur"; build
   tamamlanmadan (`KafkaConnectBuild` Ready) `SqlServerConnector`,
   `MongoDbConnector`, `JdbcSourceConnector`, `IcebergSinkConnector`
   sınıfları classpath'te yoktur — connector apply edilse bile
   `NoClassDefFoundError` ile `FAILED` düşer.
4. **Bucket + namespace bootstrap, herhangi bir source connector'dan önce** —
   Iceberg sink `auto-create-enabled: true` ile tabloyu **ilk mesaj geldiğinde**
   yaratır; namespace'in `location` override'ı (`s3://src-mssql-ogrenci/warehouse`
   gibi) önceden `CREATE NAMESPACE` ile verilmemişse tablo yanlış (varsayılan
   `depo` bucket'ının kökü) konuma yazılır — Model X'in (kaynak-başına bucket)
   sessizce bozulduğu, sonradan fark edilmesi zor bir hata. Bu yüzden
   `scaffold-source.sh` (veya elle `aws s3 mb` + `CREATE NAMESPACE`)
   **connector'dan önce** çalışır.
5. **Iceberg Sink, kaynak connector'lardan önce apply edilir** — sink zaten
   `topics.regex: "cdc\\..*|jdbc\\..*|mongo\\..*"` ile *tüm* ingest
   topic'lerini dinliyor; source connector başladığında tüketici zaten hazır
   ve `RUNNING` olmalı ki ilk mesajlar commit edilebilsin (aksi halde mesajlar
   topic'te birikir, sink başlayınca "geç ama kayıpsız" işlenir — daha büyük
   ilk-commit gecikmesi, üretimde istenmez).
6. **Source connector'lar (`dbz-mssql-students`, `jdbc-mssql-scheduled`,
   `dbz-mongo-lms`) sırada** — artık hem hedef (sink RUNNING) hem hedef
   konum (bucket/namespace) hazır; şimdi güvenle veri akıtabilirler.
7. **nginx yolu (FluentBit + Spark) bağımsız bir üretici zinciridir** — Iceberg
   sink'i kullanmaz (Spark doğrudan yazar), ama yine de Kafka'nın **external
   listener**'ının (9094, route+SCRAM) hazır olmasına ve Nessie/Trino
   warehouse'unun ayakta olmasına muhtaçtır; bu yüzden ingest zincirinin ana
   gövdesinden sonra, ondan tamamen bağımsız olarak devreye girer.
8. **Bakım (Spark `iceberg-maintenance` CronJob) en sonda** — `rewrite_data_files`/
   `expire_snapshots`/`remove_orphan_files` taradığı tabloların önce
   var olması gerekir (auto-create ile); veri akışı bir süre oturduktan sonra
   devreye almak, "boş katalogda gezinen" anlamsız ilk çalıştırmaları önler.

---

## 3. Bileşen bileşen derinlemesine

### 3.1 Kafka + external listener

**Rolü:** İngest omurgasının merkezi tamponu. Üç listener farklı amaca
hizmet eder — birbirine karıştırılırsa güvenlik veya bağlantı hatası çıkar:

| Listener | Port | Auth | Kim kullanır |
|---|---|---|---|
| `plain` | 9092 | yok, cluster-içi | yalnızca debug (`oc exec` ile broker pod içinden) |
| `tls` (internal) | 9093 | SCRAM-SHA-512 + TLS | Connect, Debezium schema-history, Spark (nginx consumer) |
| `external` (Route) | 9094 (Route TLS terminasyonuyla dışarıdan **443**) | SCRAM-SHA-512 + TLS | nginx VM'lerindeki FluentBit ajanları |

**Kritik config** (`chart/templates/03-kafka-strimzi.yaml`):
- `KafkaNodePool` — `controller` (3 replica) + `broker` (3 replica), KRaft
  (ZooKeeper yok); broker `storage.type: jbod`, StorageClass boşsa küme
  varsayılanı (`kafka.nodePools.broker.storage.class`).
- Cluster-genelinde RF=3 / ISR=2: `default.replication.factor: 3`,
  `min.insync.replicas: 2`, `offsets.topic.replication.factor: 3`.
- Ingest'e özel `KafkaUser`'lar (bu dosyada): `connect` (connect-cluster
  internal topic'leri + tüm ingest/DLQ topic'leri için
  `Read/Write/Create/Describe`), `debezium-src` (`schema-history.*` prefix),
  `fluentbit` (yalnızca `nginx-access` `Write/Describe`). `spark-nginx`
  KafkaUser'ı (`nginx-access` `Read` + `spark-` consumer group prefix `Read`)
  bu dosyada değil, `14-nginx-ingest/spark-nginx-streaming.yaml` içinde
  tanımlıdır (Spark streaming job'uyla birlikte gelir).
- `connect-cluster-configs/offsets/status` topic'leri: RF=3,
  `cleanup.policy: compact` — Connect'in kendi state'i.

**Elle deneyin:**
```bash
NS=example
# External listener Route olarak Admitted mi?
oc get kafka kafka -n $NS -o jsonpath='{.status.listeners}' | jq
# Topic listesi (ingest + internal)
oc get kafkatopic -n $NS
# Cluster-içi debug tüketici (yalnızca plain/9092, auth yok — prod'da sadece debug!)
oc exec -it kafka-broker-0 -n $NS -- \
  bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic cdc.mssql1.dbo.students --from-beginning
```
> Broker pod adı Strimzi `KafkaNodePool` ile `<cluster>-<pool>-<idx>`
> şeklindedir → **`kafka-broker-0`**; `kafka-0` DEĞİL (README'de aynı not var).

**Nasıl bozulur — nasıl anlarsın:**
- Broker pod'larından biri (veya birden fazlası) düşerse ve `min.insync.replicas: 2`
  sağlanamazsa üreticiler (Connect, FluentBit) `NotEnoughReplicasException`
  ile hata verir. Belirti: `oc get pods -n $NS -l strimzi.io/cluster=kafka`
  içinde broker `Running` değil; connector task loglarında
  `NotEnoughReplicas` görürsünüz.
- Consumer lag (Iceberg sink veya Spark tüketemiyor): `oc exec -it kafka-broker-0
  -n $NS -- bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092
  --describe --group connect-iceberg-sink-cdc` — `LAG` sütunu sürekli
  büyüyorsa sink tarafında bir sorun var demektir (bkz. §3.4).

### 3.2 Apicurio Registry

**Rolü:** İlişkisel (MSSQL) kaynakların Avro şemasını saklayan, sürüm/uyumluluk
yöneten tek doğruluk kaynağı. Mongo bu yolu kullanmaz (ham JSON, registry
bypass — bkz. §3.5).

**Kritik config** (`chart/templates/11-apicurio-registry.yaml`):
- `APICURIO_STORAGE_KIND=sql` + `APICURIO_STORAGE_SQL_KIND=postgresql` →
  ayrı CNPG cluster'ı `pg-apicurio` (3 instance, WAL+PITR backup
  `s3://backups/pg-apicurio`) — şemalar CNPG'nin backup hikâyesine dahil.
- `Deployment` 2 replica + `PodDisruptionBudget(minAvailable: 1)`.
- Readiness/Liveness: `/apis/registry/v2/system/info`.
- Connector tarafında (`13-connectors/dbz-mssql-students.yaml`,
  `jdbc-mssql-scheduled.yaml`): `key/value.converter:
  io.apicurio.registry.utils.converter.AvroConverter`,
  `apicurio.registry.url: http://apicurio-registry:8080/apis/registry/v2`,
  `apicurio.registry.auto-register: true`.

**Elle deneyin:**
```bash
oc rollout status deploy/apicurio-registry -n example --timeout=300s
# Cluster-içinden system/info (Registry ayakta mı, sürüm ne)
oc run probe --rm -i --restart=Never --image=curlimages/curl -n example -- \
  curl -s http://apicurio-registry:8080/apis/registry/v2/system/info
# Kayıtlı şemaları ara (Debezium/Aiven JDBC ilk mesajı gönderdikten sonra dolar)
oc run probe --rm -i --restart=Never --image=curlimages/curl -n example -- \
  curl -s "http://apicurio-registry:8080/apis/registry/v2/search/artifacts?name=students"
```

**Nasıl bozulur — nasıl anlarsın:**
- Registry erişilemez durumdaysa (pod down, CNPG `pg-apicurio` unhealthy):
  Debezium/Aiven JDBC connector task'ları `RUNNING`'den `FAILED`'e düşer;
  `oc get kafkaconnector dbz-mssql-students -n example -o jsonpath='{.status.connectorStatus.tasks}'`
  trace'inde registry'ye bağlanamama hatası görülür.
- Kaynakta **uyumsuz** (BACKWARD ihlali) bir şema değişikliği olursa (ör. bir
  kolon tipini değiştirmek) `auto-register` reddeder → connector task
  `FAILED`, trace'te "şema uyumsuzluğu / incompatible schema" mesajı.

### 3.3 Kafka Connect (`spec.build`)

**Rolü:** Tüm source + sink connector'ları barındıran tek ingest platformu.
Custom plugin imajı Strimzi `spec.build` ile **deklaratif** derlenir —
Dockerfile yazıp elle build/push yok, `pip install` yok.

**Kritik config** (`chart/templates/12-kafka-connect.yaml`):
- `bootstrapServers: kafka-kafka-bootstrap:9093`, `tls.trustedCertificates`
  (kafka-cluster-ca-cert), `authentication.type: scram-sha-512`,
  `username: connect`.
- `config.storage.topic/offset.storage.topic/status.storage.topic` →
  `connect-cluster-configs/offsets/status` (RF=3, §3.1'de tanımlı).
- `build.output` → OpenShift internal registry
  (`image-registry.openshift-image-registry.svc:5000/example/connect-lakehouse:1.0`),
  `pushSecret: connect-build-push-secret`.
- `build.plugins`: `debezium-sqlserver` + `debezium-mongodb` (2.7.3.Final),
  `aiven-jdbc` (6.10.0 + mssql-jdbc 12.8.1 + postgresql 42.7.4 jar'ları),
  `iceberg-sink` (iceberg-kafka-connect-runtime 1.6.1), `apicurio-avro-converter`
  (2.6.5.Final).
- `externalConfiguration.volumes`: `mssql`, `mongo`, `s3` (secret adları
  `mssql`/`mongo`/`s3-credentials`) → connector'lar bunları
  `${file:/opt/kafka/external-configuration/<ad>/<key>:key}` söz dizimiyle okur.
- `metricsConfig` → JMX Prometheus exporter (`connect-metrics` ConfigMap).

**Elle deneyin:**
```bash
oc apply -f 12-kafka-connect.yaml
oc get kafkaconnectbuild -n example                       # build ilerlemesi
oc wait kafkaconnect/connect -n example --for=condition=Ready --timeout=600s
# Yüklü plugin sınıfları (Debezium/Aiven JDBC/Iceberg sink görünmeli)
oc get svc -n example | grep connect                       # gerçek Service adını bul (tipik: connect-connect-api)
oc run probe --rm -i --restart=Never --image=curlimages/curl -n example -- \
  curl -s http://connect-connect-api:8083/connector-plugins | jq '.[].class'
```

**Nasıl bozulur — nasıl anlarsın:**
- Artifact URL/checksum bozuksa build `Failed` düşer: `oc get kafkaconnectbuild
  -n example` → `STATUS: Failed`; ayrıntı için
  `oc logs -l strimzi.io/kind=KafkaConnectBuild -n example` (Docker build
  log'unda `curl`/zip indirme hatası görülür).
- `pushSecret` eksik/yanlışsa: build tamamlanır ama registry'ye push
  reddedilir ("unauthorized"/"denied"); aynı log komutuyla görülür.
- Bir connector `/connector-plugins` listesinde yoksa (ör. build eski kaldı,
  yeni plugin eklenip build tetiklenmedi) → connector `oc apply` edildiğinde
  `FAILED` + trace'te `ClassNotFoundException`.

### 3.4 Iceberg Sink

**Rolü:** Tüm ingest topic'lerini (`cdc.*`, `jdbc.*`, `mongo.*`) **tek**
connector üzerinden tüketen fan-out sink; her mesajı, kaynak-tarafı SMT'nin
set ettiği `_target_table` alanına bakarak doğru Iceberg namespace/tabloya
yönlendirir (Model X'in uygulayıcısı — kaynak-başına bucket, tek Nessie
katalog).

**Kritik config** (`chart/templates/13-connectors.yaml`, `iceberg-sink-cdc` girdisi):
- `topics.regex: "cdc\\..*|jdbc\\..*|mongo\\..*"` — yeni bir kaynak
  eklendiğinde bu connector'a **dokunmaya gerek yok**, regex otomatik yakalar.
- `iceberg.catalog.type: rest`, `iceberg.catalog.uri:
  http://nessie:19120/iceberg/v1`, `iceberg.catalog.warehouse: "warehouse"`
  (Nessie'nin `?warehouse=warehouse` adlı, fiziksel karşılığı
  `s3://depo/warehouse` olan varsayılan warehouse'u — Trino'daki `lakehouse`
  katalogunun **aynısı**; bkz. `06-trino-ha.yaml` `lakehouse.properties`).
- `iceberg.tables.dynamic-enabled: true` + `route-field: "_target_table"` +
  `auto-create-enabled: true` + `evolve-schema-enabled: true` +
  `upsert-enabled: true`.
- `iceberg.control.commit.interval-ms: 60000` (60 sn'de bir commit).
- `errors.tolerance: all`, DLQ: `iceberg-sink.dlq`.

> **Not:** Tüm üç örnek kaynak (`mssql_ogrenci.students`, `mongo_lms.enrollments`,
> ileride eklenecek `mssql_ogrenci.courses`) **aynı** `iceberg.catalog.warehouse:
> "warehouse"` üzerinden, yani Trino'nun **`lakehouse`** katalogunda görünür
> (`rawlake` katalogu — `rawdata` warehouse'u — bu sink instance'ında
> kullanılmıyor; spec'in bahsettiği ayrı Bronze/`rawlake` katmanı ileri bir
> genişleme, bu POC'te tek sink/tek warehouse yeterli).

**Elle deneyin:**
```bash
oc get kafkaconnector iceberg-sink-cdc -n example
oc get kafkaconnector iceberg-sink-cdc -n example -o jsonpath='{.status.connectorStatus.tasks}'
# Commit lag / consumer lag (Kafka Connect grup adı: connect-<connector-adı>)
oc exec -it kafka-broker-0 -n example -- bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group connect-iceberg-sink-cdc
# DLQ oku
oc exec -it kafka-broker-0 -n example -- bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic iceberg-sink.dlq --from-beginning
```

**Nasıl bozulur — nasıl anlarsın:**
- Nessie/S3 erişilemezse commit'ler retry döngüsüne girer; `LAG` sütunu
  büyümeye devam eder, connector `RUNNING` görünse de veri Trino'ya
  yansımaz — Connect pod log'unda (`oc logs deploy/connect-connect -n example`
  veya `oc logs -l strimzi.io/name=connect-connect -n example`) tekrarlayan
  commit hatası görülür.
- Kaynak connector'da `route` SMT unutulursa (`_target_table` set edilmezse)
  mesaj hangi tabloya gideceğini bilemez → `errors.tolerance: all` sayesinde
  connector çökmez ama mesaj **`iceberg-sink.dlq`**'ya düşer — büyüyen DLQ,
  bu hatanın işaretidir.

### 3.5 Debezium (CDC)

**Rolü:** Kaynağın transaction log'unu (MSSQL: CDC capture tabloları) ya da
change stream'ini (Mongo: `rs0` replica set) okuyarak insert/update/**hard-delete**'i
near-realtime yakalar — polling yapmaz, delete'i görebilen tek yoldur.

**Kritik config:**

*MSSQL* (`chart/templates/13-connectors.yaml`, `dbz-mssql-students` girdisi):
- `database.names: OgrenciDB`, `table.include.list: dbo.students`,
  `topic.prefix: cdc.mssql1` → üretilen topic: **`cdc.mssql1.dbo.students`**.
- `schema.history.internal.kafka.topic: schema-history.mssql1` (kaynağa özel
  şema tarihçesi — `debezium-src` KafkaUser'ının ACL'i bu prefix'i kapsar).
- `key/value.converter`: Apicurio `AvroConverter`, `auto-register: true`.
- `transforms: unwrap,route` — `unwrap` (`ExtractNewRecordState`,
  `delete.handling.mode: rewrite`, `drop.tombstones: false`) Debezium
  zarfını açar ve delete'i korur; `route` (`InsertField$Value`,
  `static.field: _target_table`, `static.value: mssql_ogrenci.students`)
  sink'in yönlendireceği hedefi damgalar.
- DLQ: `cdc.mssql1.dbo.students.dlq`.

*Mongo* (`chart/templates/13-connectors.yaml`, `dbz-mongo-lms` girdisi):
- `mongodb.connection.string: mongodb://{{MONGO_HOST}}:27017/?replicaSet=rs0`
  (replica set **zorunlu** — change streams önkoşulu).
- `collection.include.list: lms.enrollments`, `topic.prefix: mongo.lms`,
  `capture.mode: change_streams_update_full`.
- `key/value.converter: JsonConverter`, `schemas.enable: false` — **ham JSON**,
  Apicurio'yu bypass eder (spec §0.7/§5.2: Mongo tipli değil, Bronze).
- `_target_table: mongo_lms.enrollments` → beklenen topic **`mongo.lms.enrollments`**
  (DLQ adı `mongo.lms.enrollments.dlq` olarak statik atanmış; asıl üretilen
  topic adını gerçek kümede `oc get kafkatopic` ile doğrulayın — Debezium'un
  Mongo topic adlandırması `topic.prefix` + veritabanı + koleksiyon
  birleşimidir, `topic.prefix` burada zaten `mongo.lms` olduğundan üretilen
  ad beklenenden uzun çıkabilir; bu, "asla varsayma, doğrula" ilkesinin
  canlı bir örneğidir — **elle deneyin** adımında mutlaka kontrol edin).

**Elle deneyin:**
```bash
# (Bir defalık, DBA) MSSQL önkoşulu:
#   EXEC sys.sp_cdc_enable_db;
#   EXEC sys.sp_cdc_enable_table @source_schema='dbo', @source_name='students', @role_name=NULL;
#   (SQL Server Agent servisinin ÇALIŞIYOR olması şart.)

# MSSQL'de bir satır ekle/güncelle/sil, sonra topic'i izleyin:
oc exec -it kafka-broker-0 -n example -- bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic cdc.mssql1.dbo.students --from-beginning

# Üretilen gerçek Mongo topic adını doğrula (varsayma!):
oc get kafkatopic -n example | grep -i mongo

# Connector durumu / snapshot mı streaming mi:
oc get kafkaconnector dbz-mssql-students -n example -o jsonpath='{.status.connectorStatus.connector.state}'
oc logs -l strimzi.io/name=connect-connect -n example | grep -i "snapshot\|streaming"
```

**Nasıl bozulur — nasıl anlarsın:**
- **SQL Server Agent durursa**, CDC capture job çalışmaz; DB'de değişiklik
  olsa da topic sessiz kalır — connector `RUNNING` görünmesine rağmen veri
  akmaz. Belirti: yukarıdaki tüketici komutu yeni mesaj göstermez;
  Connect log'unda LSN ilerlemediğine dair uyarı.
- **Mongo replica set değilse**, connector başlarken hemen düşer:
  `oc get kafkaconnector dbz-mongo-lms -n example -o jsonpath='{.status.connectorStatus.tasks}'`
  trace'inde "yalnızca replica set üzerinde çalışır" tarzı bir hata.
- **Snapshot vs streaming** ayrımı: ilk çalıştırmada connector önce mevcut
  tabloyu tarar (snapshot — büyük tablo ise uzun sürebilir), sonra
  streaming'e geçer; log'da bu geçişi görürsünüz, restart'ta (§6) snapshot
  **tekrarlanmaz**.

### 3.6 Aiven JDBC (scheduled)

**Rolü:** CDC altyapısı gerekmeyen, periyodik/bilinen-SQL kaynaklar için
basit alternatif — delete umursanmıyorsa (yalnızca insert/update).
Kaynak tarafında sadece **read-only kullanıcı** + bir **incrementing/timestamp
kolonu** yeterlidir; `sp_cdc_enable_*` gerekmez.

**Kritik config** (`chart/templates/13-connectors.yaml`, `jdbc-mssql-scheduled` girdisi):
- `mode: timestamp+incrementing` — **her iki kolon da** gerekli:
  `incrementing.column.name: course_id` (monotonik PK) **ve**
  `timestamp.column.name: updated_at` (delta penceresi için).
- `table.whitelist: courses`, `topic.prefix: jdbc.mssql1.dbo.` → üretilen topic
  (bu **statik, elle yazılmış örnek** connector için) **`jdbc.mssql1.dbo.courses`**.
  > Dikkat: `templates/scaffold-source.sh` ile **yeni** bir scheduled kaynak
  > oluşturursanız topic prefix deseni farklıdır —
  > `jdbc.<kaynak>.<db-küçükharfli>.` (ör. `--db OgrenciDB` →
  > `jdbc.mssql1.ogrencidb.`). Yani scaffold ile üretilecek yeni bir
  > `mssql1`/`OgrenciDB` kaynağı `jdbc.mssql1.ogrencidb.<tablo>` üretirken, bu
  > repodaki hazır örnek connector `dbo` şema adını kullanıyor — ikisi de
  > geçerli, ama **birbirinin klonu değil**; hangi yoldan geldiğini bilmeden
  > topic adını tahmin etmeyin, `oc get kafkatopic` ile doğrulayın.
- `poll.interval.ms: 3600000` (saatlik — scheduled kadans).
- `value.converter`: Apicurio `AvroConverter` (ilişkisel → yine Avro).
- `transforms.route` → `_target_table: mssql_ogrenci.courses`.

**Elle deneyin:**
```bash
# courses tablosunda bir satırı güncelle (updated_at'i de günceller varsayımıyla),
# sonra poll aralığını beklemeden topic'i izleyin (bir sonraki poll'da dolar):
oc exec -it kafka-broker-0 -n example -- bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic jdbc.mssql1.dbo.courses --from-beginning

# Saklanan offset'i (son çekilen timestamp/incrementing değeri) gözlemleyin:
oc exec -it kafka-broker-0 -n example -- bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic connect-cluster-offsets --from-beginning | grep -i mssql1
```

**Nasıl bozulur — nasıl anlarsın (monoton kolon tuzağı):**
- Uygulama `updated_at` kolonunu her UPDATE'te güncellemiyorsa (ör. yalnızca
  belirli alanlar değiştiğinde tetikleniyorsa), satır **gerçekten değişmiş
  olsa da** connector onu asla göremez — connector `RUNNING` kalır, hata
  vermez, **sessizce veri kaybı** yaşanır. Bu, connector status'unda
  görünmez; tek tespit yolu MSSQL'deki satır sayısı/son değişiklik ile
  Trino'daki karşılığını **karşılaştırmaktır**.
- **Timezone farkı** (MSSQL sunucu saat dilimi ≠ Connect JVM saat dilimi)
  delta penceresinin sınırını kaydırır — sınıra yakın satırlar atlanabilir
  veya tekrar çekilebilir; POC'de saat dilimleri eşitlenmeli/`connection.url`
  üzerinden netleştirilmeli (bkz. §7).
- **Delete yakalanmaz** — bu bir "bozulma" değil, tasarımın doğal sınırıdır:
  MSSQL'de bir satır silinirse Trino'da **kalmaya devam eder**. Delete
  gerekiyorsa CDC yoluna geçin.

### 3.7 nginx: FluentBit + Spark

**Rolü:** nginx erişim logu bir veritabanında değil, VM'deki bir dosyada
yaşar. **FluentBit**, nginx VM'sine kurulan bir edge ajandır (OpenShift
DaemonSet **değil**); log'u tail edip Kafka'nın **external listener**'ına
(TLS+SCRAM) yazar. **Spark Structured Streaming** (7/24 çalışan
`SparkApplication`) bu ham satırı ayrıştırır, KVKK gereği client IP'yi
hash'ler ve Iceberg'e yazar — burada Iceberg **sink** kullanılmaz, Spark
doğrudan yazar (Karar Belgesi'nin istisnası).

**Kritik config:**

*FluentBit* (`chart/templates/14-nginx-ingest.yaml`, FluentBit ConfigMap):
- `INPUT tail` → `/var/log/nginx/access.log`.
- `OUTPUT kafka` → `Topics nginx-access`, `Brokers {{KAFKA_EXTERNAL_BOOTSTRAP}}:443`
  (Route TLS terminasyonu üzerinden 443; arkada gerçek external listener 9094).
- `rdkafka.security.protocol: SASL_SSL`, `sasl.mechanisms: SCRAM-SHA-512`,
  `sasl.username: fluentbit` (KafkaUser `fluentbit` ile eşleşir, yalnızca
  `nginx-access` `Write/Describe` ACL'i var), `ssl.ca.location:
  /etc/fluent-bit/kafka-ca.crt`.

*Spark* (`chart/templates/14-nginx-ingest.yaml`, SparkApplication):
- `SparkApplication/nginx-streaming`, `restartPolicy.type: Always`,
  `onFailureRetries: 5` (streaming job — çökerse yeniden başlar).
- Consumer kimliği: `KafkaUser/spark-nginx` (SCRAM-SHA-512, `nginx-access`
  `Read` + `spark-` prefix consumer group `Read`).
- **Checkpoint kalıcı olmalı**: `s3://nginx-web/_checkpoints/access_log`
  (S3, **`emptyDir` DEĞİL** — README'nin sorun giderme bölümünde
  açıkça uyarılıyor). Dikkat: bu değer `SparkApplication` CR'ında değil,
  **job kaynağında** hardcoded — `tools/jobs/nginx_streaming.py`
  (`.option("checkpointLocation", "s3://nginx-web/_checkpoints/access_log")`).

**Elle deneyin:**
```bash
# nginx VM'sinde bir istek üret ve log'a düştüğünü doğrula:
curl -s -o /dev/null http://<nginx-vm-host>/
tail -f /var/log/nginx/access.log

# FluentBit ajanı çalışıyor mu:
systemctl status fluent-bit

# Topic'e ulaştı mı (küme içinden):
oc exec -it kafka-broker-0 -n example -- bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic nginx-access --from-beginning

# Spark streaming job durumu:
oc get sparkapplication nginx-streaming -n example
oc logs -f $(oc get pods -n example -l sparkoperator.k8s.io/app-name=nginx-streaming,spark-role=driver -o name) -n example

# Trino'da satır sayısı artıyor mu:
trino --execute "SELECT count(*) FROM lakehouse.web.access_log"
```

**Nasıl bozulur — nasıl anlarsın:**
- **Checkpoint tuzağı**: checkpoint yanlışlıkla `emptyDir`'e/kalıcı olmayan
  bir volume'a konursa, driver pod'u yeniden başladığında (crash, node
  drain) Spark nerede kaldığını bilemez — ya baştan okur (yinelenen kayıt)
  ya da bir aralığı atlar (veri kaybı). Doğrulama iki yoldan yapılır (bu
  değer job kaynağında hardcoded, `SparkApplication` CR'ında DEĞİL — CR'da
  `grep checkpointLocation` BOŞ döner, bu normaldir):
  ```bash
  # 1) Job kaynağında ayarın kalıcı S3 yolunu gösterdiğini teyit et:
  grep checkpointLocation tools/jobs/nginx_streaming.py
  # → .option("checkpointLocation", "s3://nginx-web/_checkpoints/access_log")
  # 2) Çalışırken S3'te checkpoint dizininin gerçekten oluştuğunu teyit et:
  aws s3 ls s3://nginx-web/_checkpoints/access_log/
  ```
- **FluentBit durursa**, gerçek trafik olsa da `nginx-access` topic'i
  sessiz kalır. Belirti: `systemctl status fluent-bit` `failed`/`inactive`;
  `journalctl -u fluent-bit` içinde SCRAM auth hatası (parola/sertifika
  süresi) veya bağlantı zaman aşımı.

---

## 4. Uçtan uca canlı demo

Amaç: MSSQL'de bir öğrenci kaydını ekle/güncelle/sil, Mongo'da bir kaydı
değiştir, nginx'e istek gönder → hepsi Trino'da **tek sorguda** birleşsin.

### 4.1 MSSQL — insert/update/delete (CDC yolu)

```sql
-- MSSQL, OgrenciDB.dbo.students
INSERT INTO dbo.students (id, ad, soyad) VALUES (9001, 'Ayşe', 'Yılmaz');
UPDATE dbo.students SET soyad = 'Demir' WHERE id = 9001;
DELETE FROM dbo.students WHERE id = 9001;
```
Her adımdan sonra topic'i izleyin (`cdc.mssql1.dbo.students`) ve aradan Trino'yu sorgulayın:
```bash
oc exec -it kafka-broker-0 -n example -- bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic cdc.mssql1.dbo.students --from-beginning
trino --execute "SELECT * FROM lakehouse.mssql_ogrenci.students WHERE id = 9001"
```
Insert/update sonrası satır görünür; **delete sonrası satır Trino'dan da
kaybolur** (equality-delete, `delete.handling.mode: rewrite` + sink
`upsert-enabled: true` sayesinde — bu §3.5/§3.4'te anlatılan mekanizmanın
canlı ispatıdır).

### 4.2 Mongo — doküman değişikliği (CDC yolu, ham JSON)

```javascript
// mongosh, lms veritabanı
db.enrollments.insertOne({_id: "enr-9001", student_id: 9001, course_id: "CS101", status: "active"});
db.enrollments.updateOne({_id: "enr-9001"}, {$set: {status: "completed"}});
```
```bash
oc get kafkatopic -n example | grep -i mongo   # üretilen gerçek topic adını doğrula
trino --execute "SELECT json_extract_scalar(document_payload, '\$.status') AS durum
                  FROM lakehouse.mongo_lms.enrollments
                  WHERE json_extract_scalar(document_payload, '\$._id') = 'enr-9001'"
```
(`document_payload` — Bronze/ham JSON kolonu; spec §5.2. `_id` mesaj key'idir,
delete/update bunun üzerinden equality-delete'e dönüşür — bkz. §7.)

### 4.3 nginx — istek üret

```bash
curl -s -o /dev/null http://<nginx-vm-host>/ogrenci/9001
```
```bash
trino --execute "SELECT count(*) FROM lakehouse.web.access_log WHERE ts > current_timestamp - interval '5' minute"
```

### 4.4 Tek raporda JOIN (federasyon doğrulaması)

```sql
-- Trino: örnek/gösterim amaçlı JOIN — gerçek kolon adlarını kendi kaynak
-- şemanıza göre uyarlayın (Iceberg tablo şemaları Avro/JSON kaynaktan
-- runtime'da türetilir; bu repoda sabit bir DDL yoktur).
SELECT
    s.id                                                    AS ogrenci_id,
    s.ad, s.soyad,
    json_extract_scalar(e.document_payload, '$.course_id')  AS ders,
    json_extract_scalar(e.document_payload, '$.status')     AS kayit_durumu,
    count(a.*)                                              AS son_5dk_istek
FROM lakehouse.mssql_ogrenci.students s
LEFT JOIN lakehouse.mongo_lms.enrollments e
       ON json_extract_scalar(e.document_payload, '$.student_id') = CAST(s.id AS varchar)
LEFT JOIN lakehouse.web.access_log a
       ON a.ts > current_timestamp - interval '5' minute
GROUP BY 1,2,3,4,5;
```
Bu tek sorgu, **üç farklı kaynaktan üç farklı yolla** (Debezium CDC ilişkisel,
Debezium CDC Mongo/JSON, Spark streaming/nginx) gelen verinin **tek Nessie
katalogu + tek Trino katalogu** (`lakehouse`) altında sorunsuz JOIN
edilebildiğini kanıtlar — bu, tüm ingest omurgasının "neden" sorusunun
cevabıdır.

---

## 5. Yeni kaynak ekleme atölyesi

Katılımcı, `dbo.students`/`dbo.courses` dışında **yeni bir tablo**
(ör. `dbo.grades` — `mssql1` kaynağının **scheduled** yolu) ekleyecek. Adımlar
README'nin "Yeni kaynak ekle (runbook)" bölümüyle birebir aynıdır.

**0. Tip seç:** `grades` tablosu için delete önemsiz, periyodik senkron
yeterli → `scheduled` (Aiven JDBC), CDC feature gerekmez.

**1. Kaynak önkoşulu:** read-only kullanıcı zaten var (`mssql` secret'ı
paylaşılıyor); tabloda bir incrementing kolon (`grade_id`) ve bir timestamp
kolonu (`updated_at`) olmalı.

**2. Credential secret'ı** (yeni kaynak tipi mssql/mongo dışı **değilse**,
mevcut `mssql` secret'ı zaten yeterli — yeni secret gerekmiyor):
```bash
oc get secret mssql -n example   # zaten mevcut (§ "Ön hazırlık")
```

**3. Scaffold ile üret** (önce `--dry-run` ile çıktıyı incele):
```bash
cd tools
./templates/scaffold-source.sh --dry-run \
  --source mssql1 --kind scheduled --type mssql --db OgrenciDB \
  --table dbo.grades --target-ns mssql_ogrenci --target-table grades \
  --incrementing-col grade_id --timestamp-col updated_at
```
Çıktıda görmeniz gerekenler: `aws s3 mb s3://src-mssql-ogrenci` (bucket zaten
varsa atlanır — `mssql_ogrenci` namespace'i `students`/`courses` ile paylaşılıyor),
bir `KafkaTopic` CR'ı, doldurulmuş bir `KafkaConnector` CR'ı
(`source-scheduled-jdbc.yaml` şablonundan, topic prefix
`jdbc.mssql1.ogrencidb.` — §3.6'daki scaffold-vs-statik-örnek farkına dikkat),
ve bir namespace DDL'i. Sorunsuzsa `--dry-run`'ı kaldırıp gerçek uygulamayı
yapın:
```bash
./templates/scaffold-source.sh \
  --source mssql1 --kind scheduled --type mssql --db OgrenciDB \
  --table dbo.grades --target-ns mssql_ogrenci --target-table grades \
  --incrementing-col grade_id --timestamp-col updated_at
```

**4. Namespace DDL'ini Trino'da çalıştırın** (script `--dry-run` çıktısından
kopyalanır; `mssql_ogrenci` namespace'i zaten varsa `IF NOT EXISTS` no-op'tur).

**5. Doğrula:**
```bash
oc get kafkaconnector -n example                     # yeni connector RUNNING
trino --execute "SELECT count(*) FROM lakehouse.mssql_ogrenci.grades"
```

**Katılımcıya vurgulanacak nokta:** Iceberg sink'e (`iceberg-sink-cdc`)
**hiç dokunulmadı** — `topics.regex` yeni `jdbc.mssql1.ogrencidb.grades`
topic'ini otomatik yakaladı. "Yeni kaynak eklemek connector tanımlamaktan
ibarettir" prensibinin somut ispatı budur.

> **Yeni bir kaynak TİPİ** (mssql/mongo dışında, ör. başka bir Postgres
> instance'ı) ekleniyorsa scaffold yetmez: `12-kafka-connect.yaml`
> `externalConfiguration.volumes` listesine yeni `{name, secret}` girdisi
> elle eklenmeli (scaffold bunu otomatik yapmaz — README'de açıkça belirtilir).

---

## 6. Operasyon

### İzleme
Şu an için mevcut olan tek "kanca" `KafkaConnect.spec.metricsConfig`
(JMX Prometheus exporter, `connect-metrics` ConfigMap) — bu ham metrik
endpoint'ini açar. **Tam Grafana/PodMonitor bağlantısı bu alt-projenin
kapsamı dışında** (spec §9'da "alt-proje D" olarak işaretli); bugün için
metrikleri elle gözlemlemek isterseniz:
```bash
oc port-forward svc/connect-connect-api 8083:8083 -n example &
curl -s http://localhost:8083/connectors/iceberg-sink-cdc/status | jq
```
CNPG cluster'ları `pg-nessie` (`02-postgres-ha.yaml`) ve `pg-apicurio`
(`11-apicurio-registry.yaml`) zaten `enablePodMonitor: true` ile geliyor —
Prometheus operator kümede kuruluysa bunlar otomatik scrape edilir.

### DLQ okuma
DLQ Kafka Connect'te aslında **sink-tarafı** bir özelliktir; bu omurgada asıl
DLQ Iceberg sink'tedir (`iceberg-sink.dlq`). Ek olarak iki **CDC** source
connector'ı (`dbz-mssql-students`, `dbz-mongo-lms`) kendi `errors.deadletterqueue`
ayarlarıyla ayrı DLQ topic'leri tanımlar. **`jdbc-mssql-scheduled` (Aiven JDBC)
connector'ında DLQ tanımlı DEĞİLDİR** — dolayısıyla ona ait bir `.dlq` topic'i
aramayın. Hepsi aynı komut kalıbıyla okunur (internal `plain` listener, yalnızca
debug):
```bash
# Iceberg sink (routing/commit hataları) — asıl DLQ
oc exec -it kafka-broker-0 -n example -- bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic iceberg-sink.dlq --from-beginning
# MSSQL CDC (dbz-mssql-students)
oc exec -it kafka-broker-0 -n example -- bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic cdc.mssql1.dbo.students.dlq --from-beginning
# Mongo CDC (dbz-mongo-lms)
oc exec -it kafka-broker-0 -n example -- bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic mongo.lms.enrollments.dlq --from-beginning
```

### Restart davranışı (offset kalıcılığı)
```bash
oc delete pod -l strimzi.io/cluster=connect,strimzi.io/kind=KafkaConnect -n example
```
Connect pod'ları yeniden ayağa kalktığında: CDC connector'lar **re-snapshot
yapmadan** kaldığı yerden devam eder (Debezium pozisyonu `connect-cluster-offsets`
topic'inde saklanır); Aiven JDBC son timestamp/incrementing değerinden devam
eder; Iceberg sink son commit'ten sonraki mesajları yeniden işler (en-fazla-bir-kez
değil en-az-bir-kez semantiği — upsert idempotent olduğu için sorun değil).

### Compaction / bakım
```bash
oc get scheduledsparkapplication iceberg-maintenance -n example
```
`iceberg_maintenance.py` (bkz. README "Kurumsal registry — custom image build")
katalogdaki **tüm** namespace/tabloyu dinamik tarar (`SHOW NAMESPACES` +
`SHOW TABLES`), her biri için `rewrite_data_files` + `expire_snapshots` +
`remove_orphan_files` çağırır — hardcoded tablo listesi yok, yeni eklenen
her kaynak otomatik dahil olur.

### Sorun giderme (genel)
- `oc get kafkaconnector <ad> -n example -o jsonpath='{.status.connectorStatus.tasks}'`
  → `FAILED` task'ların trace'i (ilk bakılacak yer, her zaman).
- Build fail: `oc logs -l strimzi.io/kind=KafkaConnectBuild -n example`.
- **`s3://` vs `s3a://`**: Iceberg sink `S3FileIO` çıplak `s3://` kullanır;
  Spark (nginx streaming + bakım job'u) Hadoop S3A `s3a://` kullanır; Trino
  yine `s3://` bekler (bkz. §7) — şema karışırsa "Warehouse not known"
  hatası (README'nin genel sorun giderme bölümünde de aynı tuzak var).
- **Listener karışıklığı**: Connect/Debezium schema-history 9093 (TLS+SCRAM)
  kullanır; broker pod içi konsol araçları yalnızca `plain`/9092 (auth yok,
  cluster-içi debug); nginx VM'leri 9094 (external route + SCRAM) kullanır —
  yanlış port/listener seçmek "connection refused" veya SASL auth hatası
  verir.

### External-Kafka kaynak onboarding (event/stream tipi, dış Kafka)

Bir `stream`+`kafka` kaynağı **dış** (customer/harici) bir Kafka kümesine
işaret ediyorsa (`kafka_bootstrap` set — bkz. Plan B1 Task 2), dedike Iceberg
sink connector'ı `consumer.override.*` ile kendi input consumer'ını o dış
kümeye yönlendirir (`connector.client.config.override.policy: All`,
`12-kafka-connect.yaml`, spec.config — Kafka 3.0+ varsayılanı, regresyona
karşı burada açıkça set edilir). Onboarding'den ÖNCE operatörün sağlaması
gereken iki önkoşul:

1. **Kaynak başına SASL secret'ı (`user`/`pass`).** Add-source akışı,
   kimlik bilgisi girildiğinde bu secret'ı otomatik oluşturur (CLI'da
   `scaffold-source.sh`, Console'da add-source sihirbazı — aynı `_cred()`
   mekanizması diğer kaynak tipleri için kullandığı gibi); operatörün elle
   bir şey yapması gerekmez, yalnızca kimlik bilgisini (kullanıcı/parola)
   sağlaması yeterlidir.
2. **`ext-kafka-ca` truststore secret'ı — HER dış-Kafka kaynağı için
   zorunlu, operatör tarafından onboarding'den ÖNCE provizyon edilir.**
   Mevcut render (`_kafka_consumer_override`) dış Kafka için KOŞULSUZ olarak
   `consumer.override.security.protocol=SASL_SSL` + `consumer.override.ssl.
   truststore.*` üretir (yalnızca "özel CA" durumu değil — B1'de dış Kafka =
   SASL_SSL varsayılır). Bu, `kafka-ca` ile AYNI mekanizmadır: bir PKCS12
   truststore (`ca.p12`/`ca.password` anahtarları), `/mnt/external-configuration/
   ext-kafka-ca` yoluna mount edilir; dış kümenin CA'sını (public/well-known
   olsa bile bir `ca.p12` bundle'ı gerekir) taşır. Bu secret onboarding
   öncesi cluster'da hazır olmalıdır — yoksa sink'in truststore referansı
   çözülemez ve connector TLS handshake'inde FAILED'e düşer.
   > **Follow-up (B1 kapsamı dışı, takip ediliyor):** (a) chart'a opsiyonel
   > `ext-kafka-ca` volume/mount plumbing'i (bugün operatör manuel mount
   > sağlamalı); (b) `security.protocol`/mekanizma parametrik hale getirme
   > (SASL_PLAINTEXT / farklı SASL / TLS-yok dış küme desteği); (c) dış-Kafka
   > yolunun canlı e2e doğrulaması (T7 yalnızca in-cluster'ı doğrular).

Not: bu iki önkoşul customer-bağımsızdır — hiçbir gerçek müşteri adı/domain
gerekmez, yalnızca genel `ext-kafka-ca` isimlendirme konvansiyonu kullanılır.

---

## 7. SSS / tuzaklar

**S: Spark streaming checkpoint neden `emptyDir` olamaz?**
C: `emptyDir` pod'la birlikte silinir; driver restart'ında (crash, node
drain, deploy) Spark nginx akışında nereden devam edeceğini bilemez —
ya yinelenen kayıt (en baştan okur) ya da veri kaybı (offset'i unutur)
olur. Checkpoint **kalıcı** depoda olmalı: `s3://nginx-web/_checkpoints/access_log`
(S3). Bu değer `SparkApplication` CR'ında değil, **job kaynağında**
hardcoded'dır; doğrulama iki yoldan yapılır: `grep checkpointLocation
tools/jobs/nginx_streaming.py` (kalıcı S3 yolunu gösterir) ve
çalışırken `aws s3 ls s3://nginx-web/_checkpoints/access_log/` (dizinin gerçekten
oluştuğunu teyit eder). `oc get sparkapplication ... -o yaml | grep
checkpointLocation` BOŞ döner — ayar CR'da olmadığı için normaldir.

**S: Mongo'da `_id` neden bu kadar önemli?**
C: Debezium Mongo connector'da `_id` mesajın **key**'idir; Iceberg sink'in
equality-delete/upsert mekanizması identifier kolonuna (burada `_id`)
dayanır. `_id` mesaj anahtarından düşerse (yanlış SMT/converter config'i)
sink hangi satırı update/delete edeceğini bilemez — sessizce yanlış satırı
güncelleyebilir veya delete'i hiç uygulayamayabilir.

**S: CDC'yi her kaynakta kullanabilir miyim, önkoşulları neler?**
C: Hayır, kaynağa göre değişir:
- **MSSQL:** `EXEC sys.sp_cdc_enable_db;` + tablo başına
  `sp_cdc_enable_table`; **SQL Server Agent çalışıyor olmalı** (CDC capture
  job'ları Agent üzerinden çalışır — Agent durursa CDC sessizce durur).
- **PostgreSQL:** `wal_level=logical` + bir replication slot + bir
  publication.
- **MongoDB:** **Replica Set** (`rs0`) şart — `change_streams_update_full`
  capture mode replica set olmadan çalışmaz, connector başlarken hata verir.

**S: `s3://` ile `s3a://` arasındaki fark ne, neden karıştırmamalıyım?**
C: İkisi de S3 uyumlu bir endpoint.e gider ama farklı istemci
kütüphaneleri kullanır: Iceberg Kafka Connect sink **`S3FileIO`** (çıplak
`s3://`), Spark **Hadoop S3A** (`s3a://`) bekler; Trino da `s3://` şemasını
kullanır (`06-trino-ha.yaml`). Bir config'te yanlış şema kullanmak
(ör. Spark job'una `s3://` vermek) "no filesystem for scheme" veya
"Warehouse not known" hatasına yol açar (README'nin ana sorun giderme
bölümünde aynı örnek var — CTAS "Warehouse not known").

**S: Aiven JDBC scheduled'da hangi timezone/precision tuzakları var?**
C: `timestamp+incrementing` modu **hem** monotonik bir incrementing kolon
**hem** her yazmada güncellenen bir timestamp kolonu ister — uygulama
`updated_at`'i her UPDATE'te dokunmuyorsa değişiklik sessizce kaçırılır (bkz.
§3.6). Ayrıca MSSQL sunucu saat dilimi ile Kafka Connect JVM'inin saat dilimi
farklıysa delta penceresinin sınırındaki satırlar atlanabilir/tekrarlanabilir
— POC'de saat dilimlerini eşitleyin veya `connection.url`'e açık timezone
parametresi ekleyin (POC-DOĞRULA — sürücüye göre değişir).

**S: Yeni bir kaynak eklerken en sık yapılan hata nedir?**
C: Bucket/namespace bootstrap'ini (`scaffold-source.sh` veya elle `CREATE
NAMESPACE ... WITH (location=...)`) connector'dan **sonra** yapmak — sink
`auto-create-enabled` ile tabloyu ilk mesajda oluşturur ve namespace'in
`location` override'ı yoksa **yanlış (varsayılan) bucket'a** yazar. Sıra
her zaman: bootstrap → sink zaten RUNNING → source connector.

**S: Bir connector `RUNNING` görünüyor ama veri gelmiyor, ilk ne kontrol
ederim?**
C: Sırasıyla: (1) `oc get kafkaconnector <ad> -n example -o jsonpath=
'{.status.connectorStatus.tasks}'` (task seviyesinde FAILED var mı), (2) ilgili
`.dlq` topic'i doluyor mu, (3) kaynağın kendi önkoşulu (SQL Server Agent,
replica set, `updated_at` tetiklemesi) sağlanıyor mu — connector durumu
"sağlıklı" görünse de kaynak-tarafı önkoşul eksikse veri sessizce akmaz.
