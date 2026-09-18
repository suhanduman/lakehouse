# Lakehouse v2 — Deklaratif, Minimal-Kod Yeniden Tasarım — Design

**Tarih:** 2026-09-10
**Durum:** Brainstorm onaylandı (5 bölüm) + spec onaylandı (kullanıcı 2026-09-10); açık noktalar kapatıldı → writing-plans.
**Sınıflandırma:** Architectural — mevcut ürün (Console + elle şablonlanmış chart + özel imajlar) yerine, şartnamenin istediği biçimde **CR/YAML + upstream chart/operatör + 3 küçük Spark işi + runbook** ile yeniden inşa.
**Bağlayıcı girdiler:** `KatipCelebi-BigData-Sartnamesi-v5.md` (lokal, gitignored) · `docs/reviews/2026-09-10-architecture-reassessment/00-KARAR.md` + 7 bağımsız rapor (kanıt).
**Kullanıcı ilkeleri (aynen):** *"gereksiz herşey silinsin · en basit şekli ile ne nasıl yapılıyorsa o şekilde · fazla karmaşıklığa, overkill'e, hack'leyerek oldurmaya gerek yok · bütün bileşenler açık kaynak, mümkünse en son sürüm · baked/prebuilt imaja girilmemeli · air-gap düşünülmeyecek · kurulumda her şey otomatik olmak zorunda değil; ayarlar dokümante edilir ya da ConfigMap olarak hazırlanır."*

---

## 1. Amaç ve kapsam

Müşteri ihtiyacı: **nginx erişim logları + MongoDB + PostgreSQL + MSSQL** kaynaklarından, sorguları belli veriyi bir data lakehouse'a almak; şartname Bölüm 3'e uymak.

Başarı ölçütü: şartname kabul testleri (§264) — Kafka üret/tüket, Iceberg yaz/oku (**MERGE INTO + time travel**), Trino federe sorgu, Spark batch + streaming iş, Superset dashboard, iki not defterinde SQL/Python — **kind'da uçtan uca CI ile** ve OpenShift/vanilla k8s'te runbook'la tekrarlanabilir.

Kapsam dışı (şartnamede yok, YAGNI): Camel http/mqtt/rabbitmq lane'leri, scheduled-JDBC, kafka-ingest lane'i, Console/UI, per-pipeline bucket, Apicurio (opsiyonel bileşen — runbook'ta "nasıl eklenir"), air-gap/Nexus, multi-tenancy, Oracle/MySQL (MySQL Debezium şablonu istenirse 20 satır).

## 2. Şartname → tasarım eşlemesi (yalnız tasarımı belirleyen maddeler)

| Madde | Gereksinim | v2'de karşılığı |
|---|---|---|
| a, J.2.1, J.3.2 | tümü açık kaynak; her bileşenin sürüm/lisansı listelenir | `runbooks/versions.md` (bileşen · sürüm · lisans · kaynak) |
| e, f, A.4 | OCI imaj + CRD yaşam döngüsü; **deklaratif YAML/CRD**; Operator tercih | Strimzi/CNPG/spark-operator/Keycloak operatörleri + ArgoCD app-of-apps; Console YOK |
| A.6 | toplama → omurga → dağıtık motor → Iceberg V2 → katalog → MPP SQL → BI | Fluent Bit/Debezium → Kafka → Spark → Iceberg 1.11 → Polaris → Trino → Superset |
| C.2.1(e) | CDC **connector framework** formatında; MSSQL/PG/MySQL/Mongo konnektörleri | Debezium on Kafka Connect (Strimzi) — OLake/memiiso bu maddeyi karşılamaz |
| C.3 | hafif log ajanı doğrudan omurgaya | Fluent Bit → Kafka |
| D(e), D(f) | MERGE INTO; compaction/expire/manifest **deklaratif planlama** | `merge_cdc.py`; 3 `ScheduledSparkApplication` |
| H.1.1 | Schema Registry **opsiyonel** | yok; JSON `schemas.enable=true` |
| H.3.1 | pipeline'lar CronJob/motor zamanlayıcı ile | ScheduledSparkApplication (+ Connect sürekli) |
| F.1, teslimat h | **iki farklı** web not defteri, kullanıcı-izole, PyIceberg önyüklü | JupyterHub (z2jh) + Zeppelin |
| G.1.1, F.3.1 | tüm kullanıcı servisleri AD ile OIDC/LDAPS | Keycloak (AD federasyonu) → Trino/Superset/Jupyter OIDC; Zeppelin Shiro-LDAPS |
| G.5, G.6 | Prometheus/Loki; Velero yedek; katalog DB WAL+PITR | chart `metrics.enabled` + minimal PrometheusRule; Velero Schedule; CNPG ScheduledBackup |
| B.3.1 | Iceberg V2 tüm özellikler, time travel | Polaris (Nessie'nin tek-snapshot metadata'sı time-travel'ı kırıyordu — rapor 05) |

## 3. Bileşenler ve sürümler (2026-09-10 kararlı sürümler; kurulumda güncellenir)

| Bileşen | Sürüm | Lisans | Nasıl kurulur |
|---|---|---|---|
| Strimzi (Kafka 4.x, KRaft) | 1.2.0 | Apache-2.0 | operatör (OLM/chart) + `Kafka`, `KafkaConnect`, `KafkaUser` CR |
| Debezium pg / sqlserver / mongodb | 3.6.x | Apache-2.0 | `KafkaConnect.spec.build` Maven zip artefaktı |
| Apache Iceberg Kafka Connect sink | 1.11.0 | Apache-2.0 | `spec.build` **maven** artefaktları (F0 S2 ile sabitlendi): `iceberg-kafka-connect`, `iceberg-kafka-connect-transforms`, `iceberg-parquet`, `iceberg-orc`, `iceberg-aws`, `iceberg-aws-bundle` + `org.apache.hadoop:hadoop-client-api`/`-runtime` 3.4.3 (transitive'ler Strimzi tarafından çekilir); build ≈ 10 dk |
| Apache Polaris (REST katalog) | 1.7.0 | Apache-2.0 | resmi Helm chart + CNPG Postgres |
| CloudNativePG | 1.30.x | Apache-2.0 | operatör + `Cluster` CR (Polaris, Keycloak, Superset DB'leri) |
| Apache Spark | 4.1.x | Apache-2.0 | resmi `apache/spark:4.1.x-python3` imajı; Iceberg runtime `spark.jars.packages=org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0,org.apache.iceberg:iceberg-aws-bundle:1.11.0` |
| Kubeflow spark-operator | 2.5.x | Apache-2.0 | resmi chart; `ScheduledSparkApplication` |
| Trino | 483 | Apache-2.0 | resmi `trinodb/charts` **1.42.2**; HTTPS 8443 zorunlu (cert-manager `trino-tls`, `CombinedPEM`) — kimlik doğrulama TLS ister; `OAUTH2,PASSWORD` |
| Apache Superset | 6.1.0-dev | Apache-2.0 | **Superset Kubernetes Operator 0.2.0** + `Superset` CR (resmi Helm chart `deprecated`); resmi `-dev` etiketi sürücüleri (psycopg2/trino/authlib) içerir |
| JupyterHub (z2jh) | 4.4.2 | BSD-3 | resmi chart; singleuser = resmi `quay.io/jupyter/pyspark-notebook:spark-4.1.2` + `postStart pip install pyiceberg trino`; `startTimeout 1200` (soğuk imaj ~4,5 dk), singleuser egress yalnız Trino/Polaris |
| Apache Zeppelin | 0.12.1 | Apache-2.0 | resmi imaj, tek **Deployment + PVC** (chart yok); `ZEPPELIN_CONFIG_FS_DIR=/data/conf` + interpreter.json tohumu, `ZEPPELIN_RUN_MODE=local` |
| cert-manager | v1.21.2 | Apache-2.0 | `Application` (OCI chart, `cert-manager` ns); `ClusterIssuer`/`Issuer` + `Certificate` glue'da (iç kök `lakehouse-ca`) |
| Keycloak | 26.7.x | Apache-2.0 | operatör + `Keycloak`/`KeycloakRealmImport` |
| Fluent Bit | 5.1.x | Apache-2.0 | müşteri sunucusunda ajan (config dosyası teslim) |
| ArgoCD (OpenShift GitOps) | platformun | Apache-2.0 | app-of-apps |
| Velero / OADP, kube-prometheus-stack, Loki | dev: Velero chart 12.1.0 (1.18.1) + kube-prometheus-stack 91.4.1; prod: OADP + OpenShift user-workload monitoring + OpenShift Logging (LokiStack) | Apache-2.0 (Grafana AGPL-3.0, yalnız dev) | CR/values; tam liste `runbooks/versions.md` |

**Özel imaj: sıfır.** Kaldırılanlar: `images/connect` (Strimzi build), `images/spark-py` (resmi imaj + packages), `images/iceberg-tools`, `images/jupyterhub`, `images/jupyter`, `images/superset`, `images/dbt`, `images/zeppelin`, Console imajları.

## 4. Repo düzeni (yeni)

```
bootstrap/     bootstrap.sh: operatörleri kur (OLM Subscription varsa, yoksa upstream chart) + ArgoCD kök Application. Tek script, idempotent.
platform/      apps/<bileşen>.yaml (ArgoCD Application, upstream chart + targetRevision) · values/<bileşen>.yaml
glue/          Helm chart "lakehouse-glue": Kafka, KafkaConnect(spec.build), KafkaUser(connect/spark/trino…), CNPG Cluster×3,
               ScheduledSparkApplication×5 (silver-merge 15dk · maint-position-deletes 1s · maint-compact 6s · maint-expire-orphan-ttl 1g · mongo-bronze 5dk), KeycloakRealmImport, NetworkPolicy, Route|Ingress, Zeppelin Deployment,
               ConfigMap: jobs kodu + pipelines.yaml + Fluent Bit conf. `platform: openshift|vanilla` bayrağı Route↔Ingress seçer.
pipelines/     KafkaConnector şablonları (pg-source.yaml, mssql-source.yaml, mongo-source.yaml, iceberg-sink.yaml, nginx-sink.yaml) + examples/
jobs/          merge_cdc.py, merge_lib.py, iceberg_maintenance.py, mongo_bronze.py, s3_register_example.py + tests/ (Spark-siz pytest)
runbooks/      install.md · add-source.md · add-table.md · nginx-agent.md · s3-register.md · access-control.md · user-facing.md · upgrade.md · dr.md · troubleshooting.md · versions.md · acceptance-tests.md · scripts/{polaris-setup.sh,acceptance.sh} · dbt/ (referans örnek)
test/e2e/      kind: bootstrap→sync→pg fixture→CR apply→Bronze→merge→Trino assert→maintenance (GitHub Actions + lokal Podman/kind aynı script)
```
Silinir: `console/`, `chart/` (yerine `glue/`), `images/`, `tools/`, `gitops/`(→platform), `manual-install/`, `operators/`(→bootstrap), `docs/superpowers`, `.superpowers`, `docs/egitim` (yeniden yazılacak), eski workflow'lar.

## 5. Ingestion

### 5.1 Kafka + Connect
- `Kafka` CR: 3 broker KRaft, `tls` (9093, SASL_SSL+SCRAM) listener, `simple` authorization — mevcut değerler `glue/values.yaml`'a taşınır.
- `KafkaConnect` CR: `spec.build` (çıktı registry: OpenShift'te ImageStream, vanilla'da küme registry'si — `glue/values.yaml`), `plugins:` debezium-postgres/sqlserver/mongodb (`type: zip`, Maven Central URL), iceberg-sink (`type: maven` ×2). `config.providers=secrets` → **Strimzi `io.strimzi.kafka.KubernetesSecretConfigProvider`** (`${secrets:<ns>/<secret>:<key>}`; Connect ServiceAccount'a Secret okuma Role'ü glue chart'ta), mount YOK. Bu, eski `${directory:…}` canlı kusurunu (KARAR §2) yapısal olarak kapatır.
- Kafka listener'ları: `tls` (9093, küme içi, SCRAM) + `external` (Route/Ingress ya da NodePort, `platform` bayrağına göre) — Fluent Bit ajanları dışarıdan buna yazar.
- Topic'ler Connect'in kendi `topic.creation.default.*` ayarıyla (RF 3, partitions 6) otomatik; `KafkaTopic` CR'ı yalnız `debezium-signals`/`schema-history` gibi sabitler için.
- ACL'ler **chart'ta**: `glue/values.yaml` `sources:` listesindeki her kaynak için `connect` KafkaUser'a `topic prefix <source>.` + `group prefix connect-` ACL'i döngüyle render edilir. Runtime ACL mutasyonu YOK.

### 5.2 Relational CDC (pg, mssql)
Kaynak-DB başına **1 Debezium + 1 Iceberg sink** (N tablo = 2 CR). Şablon değerleri (korunan bilgi, R1/R2):

```yaml
# pipelines/pg-source.yaml (özet)
table.include.list: <schema.t1,schema.t2>
publication.autocreate.mode: filtered          # superuser gerekmez
signal.data.collection: <schema>.debezium_signal # zorunlu; incremental snapshot watermark'ı
signal.enabled.channels: source
time.precision.mode: connect
decimal.handling.mode: precise
schema.history.internal.store.only.captured.tables.ddl: "false"   # mssql
snapshot.mode: initial
key.converter / value.converter: org.apache.kafka.connect.json.JsonConverter
key/value.converter.schemas.enable: "true"
topic.prefix: <source>
database.password: ${secrets:<ns>/<source>-db:password}
```
```yaml
# pipelines/iceberg-sink.yaml (özet)
topics.regex: <source>\.<schema>\..*
transforms: dbz
transforms.dbz.type: org.apache.iceberg.connect.transforms.DebeziumTransform
transforms.dbz.cdc.target.pattern: <ns>_raw.{table}
iceberg.tables.dynamic-enabled: "true"
iceberg.tables.route-field: _cdc.target
iceberg.tables.auto-create-enabled: "true"
iceberg.tables.evolve-schema-enabled: "true"
iceberg.tables.default-partition-by: day(_cdc.ts)      # F0 S3 ile doğrulandı: partition spec `_cdc.ts_day: day(7)`
iceberg.tables.auto-create-props.write.metadata.delete-after-commit.enabled: "true"
iceberg.tables.auto-create-props.history.expire.max-snapshot-age-ms: "86400000"
iceberg.catalog.type: rest ; iceberg.catalog.uri: <polaris>/api/catalog ; oauth2 client-credentials (${secrets:…})
# commit aralığı SET EDİLMEZ (upstream 300 s). ZORUNLU (F0 S2): control consumer flap'ına karşı
iceberg.kafka.session.timeout.ms: "120000" ; iceberg.kafka.heartbeat.interval.ms: "15000"
iceberg.kafka.max.poll.interval.ms: "300000" ; iceberg.kafka.request.timeout.ms: "130000"
errors.tolerance: all ; errors.deadletterqueue.topic.name: <source>.dlq ; errors.log.enable: "true"
```
- Bronze satırı = iş kolonları + `_cdc{op,ts,offset,source,target,key}` (DELETE'te `before` satırı, `op=D`). `__op/__ts_ms/__deleted/__lsn/_target_table` sözleşmesi ve InsertField SMT zincirleri **gider**.
- **Tablo ekleme = runbook** (`add-table.md`): `table.include.list`'e ekle → CR apply → sinyal tablosuna `INSERT … 'execute-snapshot' … {"type":"incremental"}` → `KafkaConnector` status + Kafka-UI'dan izle. (Console'un onay döngüsü yerine belgeli adım; Debezium 3.x'te DBZ-8780 düzeltmesi varsa iki-sinyal tekrarı gerekmez — runbook'ta not.)
- DLQ gerçeği belgelenir: sink `ErrantRecordReporter` uygulamıyor → DLQ yalnız converter/SMT hatası; writer hatası task'ı durdurur → alert kuralı (§8).

### 5.3 MongoDB
Debezium mongo **ENDS'siz** (ham envelope; `after` = dokümanın JSON string'i) → `mongo_bronze.py` (ScheduledSparkApplication, 5 dk, `Trigger.AvailableNow`, checkpoint tablo location altında) → Bronze `(_id string, _doc string, _cdc_op, _cdc_ts, …)`; `_id` Kafka **key**'inden (silmede tek kaynak), `$oid` sarmalayıcısı açılır; tombstone düşürülür; `after` null + `op∈{c,u,r}` → karantina tablosu. Silver `(_id, _doc)`; tüketim `json_extract`. Gerekçe: rapor 03/07 — `MongoDebeziumTransform` şema çıkarır (B5/B6 kırılganlığı); OLake'in mongo varsayılanı da ham dokümandır. **Tek "kod" istisnası** (~150 satır).

### 5.4 nginx
Fluent Bit (ajan, müşteri sunucusu): `tail` → `parser nginx` (zaman ayrıştırması burada; TR-locale sorunu kalmaz) → `kafka` output (`timestamp_key ts`, `timestamp_format iso8601`, SASL_SSL/SCRAM dış listener, `storage.type filesystem` disk-buffer) → topic `nginx.access` → Iceberg sink: tek core SMT `TimestampConverter$Value` (`ts` ISO string → Timestamp; sink şemasız JSON'dan string'i timestamp'e kendisi çeviremez) → auto-create `nginx_raw.access_log`, `default-partition-by=day(ts)`. Spark streaming işi ve `14-nginx-ingest.yaml` **gider**.

### 5.5 S3 dosya yükleme
Şartname yalnız "referans örnek" ister → `jobs/s3_register_example.py` (CTAS) + `runbooks/` örneği; Console lane'i yok.

## 6. Silver ve bakım (Spark 4.1 / Iceberg 1.11)

- **`pipelines.yaml` ConfigMap** (tek deklaratif kayıt; yalnız entity/upsert tabloları — nginx gibi append-only tablolar listelenmez, merge görmez; mongo tabloları `keys: [_id]`):
  ```yaml
  pipelines:
    - bronze: erp_raw.orders
      keys: [order_id]
      write_mode: merge-on-read   # varsayılan; küçük tablo için copy-on-write seçilebilir
      bucket_count: 16
  ```
- `merge_cdc.py` (15 dk `ScheduledSparkApplication`): Silver `<ns>.<table>` yoksa **Spark DDL** ile yaratır — `CREATE TABLE … USING iceberg PARTITIONED BY (bucket(N, keys)) TBLPROPERTIES ('write.merge.mode'=…, 'write.update.mode'=…, 'write.delete.mode'=…, 'write.distribution-mode'='hash', 'write.metadata.delete-after-commit.enabled'='true')`; snapshot-id watermark (Silver tablo özelliği); latest-per-key `ROW_NUMBER() OVER (PARTITION BY keys ORDER BY _cdc.ts DESC, _cdc.offset DESC)`; `MERGE INTO … WHEN MATCHED AND s._cdc.op='D' THEN DELETE …`; şema-uzlaştırma (add / güvenli genişletme / uyumsuz→fail-loud, R1 haritası); Nessie'ye özgü retry gerekçesi kalkar ama genel commit-conflict retry kalır. pyiceberg ön-oluşturma, py4j identifier okuma, `_target_table` dışlaması **gider**.
- Spark 4 uyumu (**F0 S4 ile doğrulandı**): ANSI mode açık → merge/şema-genişletme SQL'inde açık `CAST`/`try_cast`; Bronze `updated_at` gibi timestamptz alanları string gelir → Silver'da `CAST(... AS TIMESTAMP)`. MoR MERGE position-delete üretir, `rewrite_position_delete_files`+compaction sıfırlar; time travel Polaris altında çalışır.
- **Bakım (D(f)):** `iceberg_maintenance.py` argümanla 3 CR: `--position-deletes` saatlik; `--compact` 6 saatte bir (`rewrite_data_files` `delete-file-threshold=5`, `remove-dangling-deletes=true`, `partial-progress`); `--expire-orphan-ttl` günlük (`expire_snapshots`, `remove_orphan_files older_than 3d`, Bronze `DELETE WHERE _cdc.ts < now()-30d`). `gc.enabled` ALTER hack'i yok. Yazma rejimi gerekçesi: rapor 04 (CoW ≈ 96× tablo/gün → MoR + 6 saatlik katlama ≈ 4×).

## 7. Katalog, sorgu, kullanıcı yüzü

- **Polaris 1.7** (**F0 S1 ile doğrulandı — kalır**): resmi chart (`persistence.type=relational-jdbc` → CNPG `-app` secret'ı; health 8182/API 8181) + `polaris-admin-tool bootstrap` Job; katalog/namespace/rol/grant/principal **tek YAML** ile `polaris setup apply setup.yaml` (credential'lar apply stdout'undan Secret'a; root principal `ROTATE_CREDENTIALS` yapamaz). Tek `lakehouse` katalog, `default_base_location s3://lakehouse/`; katalog konumları çakışamaz. **İki S3 modu (runbook):** STS'li (MinIO/AWS) → vended-credentials; STS'siz → `sts_unavailable: true` + Polaris pod'una `AWS_ACCESS_KEY_ID/SECRET` (`extraEnv`) + istemcilerde delegation header kapalı (`header.X-Iceberg-Access-Delegation=none`, Trino `iceberg.rest-catalog.vended-credentials-enabled=false`) + istemci `s3.*` anahtarları. Principal'lar `connect`, `spark`, `trino`, `notebooks`. Lakekeeper yedeği gerekmedi.
- **Trino 483** (chart 1.42.2): `iceberg.catalog.type=rest`, `iceberg.rest-catalog.security=OAUTH2` (`fs.s3.enabled`); `accessControl.type=configmap` + `rules.json` values'tan (satır-filtre/kolon-maske modeli taşındı, `refreshPeriod 60s`). **TLS zorunlu**: kimlik doğrulama yalnız HTTPS 8443'te (`tls-combined.pem`, cert-manager); `http-server.authentication.type=OAUTH2,PASSWORD` (son kullanıcı Keycloak Bearer/JWT, servis hesapları htpasswd `password.db`); `principal-field=preferred_username`; **gruplar group provider'dan** — dev dosya, prod LDAP (`platform/values/trino-ldap.yaml`); OAuth2'de grup talebi okunamaz. `oauth2.issuer` = `keycloak.hostname` + `/realms/lakehouse` (values kopyası, `runbooks/install.md`). Keycloak `trino` client'ında **audience mapper** şart. Resource groups **eklenmedi** (ihtiyaç kanıtlanmadı — F5). HA coordinator = chart değerleri.
- **Superset 6.1.0-dev**: **Superset Kubernetes Operator 0.2.0** + `Superset` CR (chart deprecated); `spec.config` ile Keycloak OAuth (`AUTH_ROLES_MAPPING` grup→rol) + Trino datasource deklaratif dosyadan (`legacy-import-datasources`, parola `SQLALCHEMY_CUSTOM_PASSWORD_STORE`); CNPG metadata DB (`metastore.uriFrom`); **Redis/Valkey YOK** (`SimpleCache`, worker/beat yok) — Alerts&Reports isteniyorsa Valkey + `celeryWorker` (`runbooks/user-facing.md`). `REQUESTS_CA_BUNDLE` yok: Keycloak sistem kökleriyle, Trino `connect_args.verify` ile doğrulanır.
- **JupyterHub** z2jh 4.4.2: `hub.config.GenericOAuthenticator` Keycloak (`allowed_groups`+`manage_groups`); `singleuser.image` resmi pyspark-notebook; `lifecycleHooks.postStart` `pip install pyiceberg[s3fs,pyarrow] trino`; kişisel PVC (F.1.2); singleuser egress **podSelector ile Trino/Polaris'e daraltıldı** (+dev MinIO).
- **Zeppelin 0.12.1**: `apache/zeppelin` imajı, Deployment + PVC; interpreter.json **Secret'tan tohumlanır** (`ZEPPELIN_CONFIG_FS_DIR=/data/conf`; Zeppelin dosyayı her açılışta yeniden yazar → ConfigMap/subPath olmaz), Trino JDBC bağımlılığı Maven Central'dan; `ZEPPELIN_RUN_MODE=local`; kimlik **Shiro + AD (LDAPS)**, OIDC yok (G.1.1).
- **Keycloak**: realm import (AD federasyonu, client'lar) taşınır. `hostname` **tam URL** (v2 API); realm sırları `spec.placeholders` ile Secret `keycloak-clients`'tan; redirect URI'ler bileşen hostname'lerinden türetilir. **Realm import mevcut realm'i GÜNCELLEMEZ** → değişiklik = realm sil + CR'ı yeniden uygula (`runbooks/install.md`).

## 8. İzleme, DR, güvenlik

**(F5'te uygulandı; bu bölüm canlı doğrulanmış hâli anlatır — `runbooks/{versions,upgrade,dr,troubleshooting,acceptance-tests}.md`.)**

- **İzleme kapsamı = boru hattı sağlığı** (kullanıcı kararı, 2026-09-18): Strimzi Kafka + Kafka Connect JMX
  (`metricsConfig`, Strimzi 1.2.0 örnek kuralları) + `Kafka.spec.kafkaExporter` (consumer lag), spark-operator
  (`prometheus.podMonitor.create`), **kube-state-metrics `customResourceState`** (SparkApplication `state` —
  StateSet, `terminationTime`; ScheduledSparkApplication `lastRun`) ve Polaris mgmt
  (`:8182` `/q/metrics` — Polaris chart'ının KENDİ `serviceMonitor`'ü; `platform/values/polaris.yaml`'da
  açıkça açılır, e2e `up{job=~".*polaris.*"}` ile doğrular). **Gösterim bileşenlerinin uygulama metriği
  toplanmaz** (Trino/JupyterHub/Zeppelin ServiceMonitor'ları kaldırıldı;
  Superset 6.1.0'da `/metrics` uç noktası zaten yok → kalıcı `down` hedef üretmemek için `spec.monitoring`
  yazılmaz). Grafana dashboard'ları: yalnız Strimzi Kafka + Strimzi Kafka Connect.
- **Yığın:** dev/vanilla'da kube-prometheus-stack 91.4.1 (`platform/apps/dev/40-monitoring.yaml`, ns
  `monitoring`, sync-wave `-1`; node-exporter/kubelet/Alertmanager kapalı); OpenShift'te **user-workload
  monitoring** (`enableUserWorkload: true`) — `lakehouse` ns'indeki PodMonitor/PrometheusRule otomatik alınır,
  bildirim platformundur. Metrik adları kind'da canlı doğrulandı (KARAR §5'teki "hiç doğrulanmamış kural adı"
  kusuru kapandı).
- **PrometheusRule `lakehouse` — 5 kural** (`glue/templates/monitoring.yaml`; her birinin `runbook`
  annotation'ı `runbooks/troubleshooting.md#…` bölümüne bağlanır):
  `LakehouseConnectTaskFailed` (`kafka_connect_connector_task_status{status="failed"} == 1`, critical) ·
  `LakehouseSinkStalled` (lag tabanlı: `kafka_consumergroup_lag{consumergroup=~"connect-sink-.*"}` >
  `monitoring.sinkLagThreshold`, 15 dk) · `LakehouseSilverMergeStale` (`time() - max(sparkapp_termination_time …
  COMPLETED)` > eşik; boş vektörde ateşlenmez) · `LakehouseSparkScheduledRunFailed`
  (`kube_customresource_sparkapp_state{state="FAILED"} == 1`) · `LakehouseSparkRunTooLong`
  (`rate(spark_application_success_execution_time_seconds_sum[6h]) / rate(…_count[6h])` — **küme geneli**,
  çünkü spark-operator 2.5.2 exporter'ı per-app etiket vermiyor).
- **Tablo düzeyi veri metrikleri** için Prometheus exporter'ı yoktur: Iceberg metadata tabloları
  (`"tbl$snapshots"`, `"tbl$files"`, `"tbl$history"`) Trino'dan sorgulanır ve istenirse bir Superset
  dashboard'una bağlanır (F6). Depolama (FlashBlade) metrikleri platformun exporter'ındadır.
- **Loglar:** platform Loki (G.5.2) — OpenShift Logging/LokiStack; vanilla'da `grafana/loki` + `grafana/alloy`
  (promtail EOL). Bu repo Loki dağıtmaz; sorgu örnekleri `runbooks/troubleshooting.md#loki`.
- **DR (G.6, kabul-kritik) — CNPG:** in-tree `barmanObjectStore` yerine **Barman Cloud eklentisi (CNPG-I)**
  (`plugin-barman-cloud` chart 0.8.0 = eklenti v0.15.0; in-tree alan CNPG 1.31'de kalkıyor). Tek
  `ObjectStore/lakehouse-backups` (`destinationPath: s3://<backup.s3.bucket>/cnpg`, `spec.retentionPolicy: 30d`,
  kimlik `backup-s3-creds`) + üç Cluster'da `plugins[isWALArchiver]` + üç `ScheduledBackup`
  (6 alanlı cron `0 0 2 * * *`, `immediate: true`). Yedek hedefi veri bucket'ından AYRIDIR. Restore provası
  `test/e2e/cnpg-restore.yaml` (PITR için `recoveryTarget.targetTime`).
- **DR — Velero:** dev'de chart 12.1.0 (Velero 1.18.1, `platform/apps/dev/40-velero.yaml`, BSL aynı bucket'ın
  `velero/` prefix'i), prod'da **OADP** (`velero.namespace: openshift-adp`, `DataProtectionApplication`).
  `Schedule/lakehouse-daily` (`0 3 * * *`, `ttl 720h`, `defaultVolumesToFsBackup: true`,
  `excludedResources: [replicasets, events, backups.postgresql.cnpg.io]` — **`pods` dışlanmaz**, aksi hâlde
  fs-backup hiç tetiklenmez). Pod hacmi dışlamaları annotation ile: CNPG `pgdata` (kurtarma yolu Barman PITR),
  Kafka `data-0` (kapsam dışı), dev MinIO `data`. **kind sınırı:** local-path PV = hostPath → fs-backup PVC
  içeriğini atlar; gerçek CSI depolamada (OpenShift) PVC'ler alınır, tercih edilen yol CSI snapshot + Data
  Mover'dır. Kapsam dışı: Kafka verisi (yeniden akıtma/MM2) ve Iceberg S3 verisi (S3 çoğaltması) — Polaris DB
  yalnız katalog metadata'sıdır.
- **Güvenlik:** NetworkPolicy default-deny (dev dâhil AÇIK; `allow-platform-namespaces` argocd/cnpg-system/
  ingress/monitoring/**velero** — cert-manager selector'ı YOK ve gerekmez: webhook çağrısı apiserver'ın host
  ağından gelir), cert-manager TLS, Secret'lar Git'e girmez (manuel ya da external-secrets
  — runbook), Strimzi ACL/SCRAM, Polaris RBAC, Trino `rules.json` (satır filtresi/kolon maskesi).
- **Kabul:** `runbooks/scripts/acceptance.sh` kurulu kümede dokuz e2e yolunu koşturup "KABUL" özeti basar;
  taze kümede aynı yollar `test/e2e/run.sh` (CI kapısı).

## 9. Test stratejisi (tersine dönüş)

- **Gerçek = kind e2e** (GitHub Actions 4-vCPU ubuntu + lokal Podman/kind, aynı `test/e2e/run.sh`): bootstrap → ArgoCD sync (veya CI'da `helm install` eşdeğeri) → Postgres fixture → `pipelines/examples/pg-*.yaml` apply → Bronze satır sayısı → `merge_cdc` → Trino `SELECT count(*)` == beklenen → UPDATE/DELETE → yeniden merge → Silver doğru → maintenance → time travel sorgusu. PR gate.
- `glue/` için az sayıda helm-unittest (render edilir, platform bayrağı Route↔Ingress, ACL döngüsü).
- `jobs/` pytest (saf fonksiyonlar: dedup SQL, reconcile plan, mongo envelope parse, DDL üretimi).
- Render-shape assertion yığını, Console testleri, 785 satırlık `helm-check.py` **gider**.

## 10. Spike'lar (Faz 0 — kod yazmadan önce, kind'da)

| # | Soru | Başarı | Başarısızsa |
|---|---|---|---|
| S1 | Polaris 1.7 + STS'siz S3 (MinIO) çalışıyor mu (#3742)? Trino/Spark/Connect üçü de okuyor-yazıyor mu? | 3 istemci aynı tabloyu görür | Lakekeeper 0.13 |
| S2 | Strimzi 1.2 `spec.build`: Debezium 3.6 zip + `iceberg-kafka-connect` maven + `iceberg-aws-bundle` → Connect RUNNING; sink Polaris'e yazıyor; namespace auto-create? | pg→Bronze satır | eksik jar'ı ek maven artefaktı olarak ekle (hâlâ build, imaj değil) |
| S3 | `DebeziumTransform` + `default-partition-by=day(_cdc.ts)` (nested) | partition'lı Bronze | Bronze partition'sız; TTL satır-delete |
| S4 | Spark 4.1 + Iceberg 1.11 + `spark.jars.packages` spark-operator'da; MoR MERGE + `rewrite_position_delete_files`; ANSI mode altında merge SQL | Silver doğru | init-container ile jar indir (hâlâ imaj değil) |

## 11. Geçiş / cutover

1. Yeni düzen **orphan branch `v2`**'de inşa edilir (eski `main` dokunulmaz).
2. e2e yeşil + runbook'lar tam → `v2` → `main` (force-push); eski `main` GitHub'dan ve lokalden silinir. **Silmeden önce** repo dışına `git bundle create ~/lakehouse-v1-final.bundle --all` (geri dönüş sigortası, sıfır maliyet). Geçmiş-yeniden-yazma komutları bu ortamda gate'e takılırsa adımlar kullanıcıya betik olarak verilir (Foundation Phase 6 emsali).
3. Hafıza/dokümanlar: eski spec/plan/kararlar silinir; `docs/reviews/2026-09-10-architecture-reassessment/` **kalır** (neden böyle yaptığımızın kanıtı); bu spec + runbook'lar tek otorite.
4. Müşteri verisi/kurulumu yok → veri geçişi yok.

## 12. Korunan bilgi (eski koddan taşınan, bedeli ödenmiş ayarlar — şablon/runbook'a yazılır)
Debezium: `time.precision.mode=connect`, `decimal.handling.mode=precise`, `publication.autocreate.mode=filtered`, sinyal tablosu zorunlu + `execute-snapshot incremental`, `store.only.captured.tables.ddl=false`, kaynak-DB başına tek connector (slot ekonomisi) · Sink: DLQ'nun yalnız converter/SMT katmanını koruduğu · Merge: latest-per-key ORDER BY (ts, offset), `WHEN MATCHED AND op='D' THEN DELETE`, `WHEN NOT MATCHED AND op<>'D' THEN INSERT`, güvenli-genişletme haritası (int→long, float→double, decimal(p,s)→decimal(p',s) p'≥p; timestamptz→string; money→decimal) · Camel byte[] dersi kapsam dışı kaldığı için taşınmaz · Yazma rejimi: MoR + 6 saatlik katlama, sink 300 s.

## 13. Dekompozisyon (plan fazları)
Tek plan, 6 faz, her faz kind'da yeşil olmadan sonraki başlamaz: **F0** spike'lar S1–S4 (scratch kind) → **F1** bootstrap + platform app'leri + glue (Kafka/Connect/CNPG/Polaris/Keycloak) → **F2** ingestion (pg/mssql şablonları, sink, ACL döngüsü, secrets provider) + `merge_cdc`/maintenance + e2e pg yolu → **F3** mongo + nginx + Spark 4 doğrulaması → **F4** Trino/Superset/JupyterHub/Zeppelin + OIDC + satır/kolon → **F5** izleme + DR + runbook'lar + versions.md + kabul-testi betiği → **F6** cutover (`v2`→`main`, bundle yedeği, eski dalların silinmesi). Eski `main`'e hiçbir faz dokunmaz.

## 14. Kapatılan açık noktalar (kullanıcı, 2026-09-10)
- **nginx IP: ham saklanır** — şartnamede KVKK/anonimleştirme/maskeleme maddesi yok (grep: kvkk|kişisel veri|anonim|maskele|ip adres → 0 eşleşme); istenirse sonradan Trino kolon-maskesi (`rules.json`). Lua filtresi yok.
- **dbt: runbook + örnek proje** (`runbooks/dbt/`: Gold model + CronJob örneği); chart bileşeni değil. **Düzeltme (F5):** resmi bir `dbt-trino` imajı YOKTUR → örnek, resmi `python:3.13-slim` imajı + `pip install dbt-trino==1.10.4` ile koşar (özel imaj yok kuralı korunur, PyPI erişimi gerekir).
- Superset/Zeppelin paylaşımlı servis hesabı kalır; satır/kolon güvenliği interaktif Trino (OIDC) kullanıcıları için.
