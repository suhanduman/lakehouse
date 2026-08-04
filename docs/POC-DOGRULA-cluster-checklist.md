# Küme-Zamanı Doğrulama Checklist'i (POC-DOĞRULA)

**Kapsam:** İngest omurgası (alt-proje A) canlı OpenShift + kurumsal S3 nesne deposu kümesine
deploy edilirken doğrulanacak kalemler. Bu repodaki manifestler statik + arayüz
düzeyinde doğrulandı (`bash tools/validate.sh`), ancak sürüm-bağımlı config
anahtarları ve credential/TLS wiring yalnızca gerçek kümede teyit edilebilir.

Kaynak: ingest spec §15 + kurulum runbook'u (`tools/README.md`) + eğitim
dokümanı (`docs/egitim/ingest-omurgasi-egitim.md`).

---

## A. Sürüm & imaj
- [ ] Strimzi `KafkaConnect.spec.build` custom imajı derleniyor ve internal registry'ye push oluyor (`connect-build-push-secret` mevcut).
- [ ] Plugin sürümleri erişilebilir + Kafka 3.7.0 ile uyumlu: Debezium 2.7.3.Final (sqlserver+mongodb), Aiven JDBC 6.10.0, mssql-jdbc 12.8.1.jre11, postgresql 42.7.4, Iceberg kafka-connect-runtime 1.9.0, Apicurio converter 2.6.5.Final.
- [ ] Kafka broker ve Connect `version` **3.7.0** hizalı (Strimzi operator support matrix).
- [ ] Apicurio Registry imajı (3.0.6) — **v3 env değişken adları** (`APICURIO_*`) kullanılan sürümle doğru (v2'den farklı).

## B. Iceberg Sink (EN YÜKSEK RİSK — spec §15.1)
- [ ] Iceberg sink v1.6.x **config anahtar isimleri** (`iceberg.catalog.*`, `iceberg.tables.*`) kullanılan sürümle bire bir doğru.
- [ ] CDC **delete → equality-delete** uçtan uca çalışıyor: MSSQL/Mongo'da fiziksel silme → Trino'da satır kayboluyor.
- [ ] `iceberg.tables.dynamic-enabled` + `route-field: _target_table` doğru namespace/tabloya yazıyor.
- [ ] `upsert-enabled` + identifier kolonları (PK) upsert'i doğru yapıyor.
- [ ] **Namespace location override (Model X):** her kaynak fiziksel olarak kendi bucket'ına yazıyor (`src-<kaynak>`).
- [ ] `iceberg.control.commit.interval-ms` + control topic (sink'in default control topic'i) oluşuyor ve `connect` ACL'i buna izin veriyor.

## C. Credential / config provider
- [ ] `DirectoryConfigProvider` tanımlı (`config.providers: directory`) ve `${directory:...}` referansları çözülüyor.
- [ ] externalConfiguration volume'ları mount edildi: `mssql`, `mongo`, `s3`, `debezium-src`, `kafka-ca`.
- [ ] Secret **key adları = mount edilen dosya adları**: s3 → `access-key-id`/`secret-access-key`; DB → `user`/`pass`; debezium-src → `password`; kafka-ca → `ca.p12`/`ca.password`.

## D. Kafka auth / TLS
- [ ] 9093 **SCRAM-SHA-512 + TLS**: `connect`, `debezium-src`, `spark-nginx` user'ları bağlanabiliyor.
- [ ] **Debezium schema-history** producer+consumer SASL_SSL + truststore: `kafka-cluster-ca-cert` key adları (`ca.p12`/`ca.password`) + mount yolu (`/opt/kafka/external-configuration/kafka-ca/`) doğru.
- [ ] **Spark→Kafka** (`nginx_streaming.py`) truststore + jaas aynı şekilde çözülüyor.
- [ ] **External listener 9094** (Route): nginx VM'lerinden FluentBit erişebiliyor (SASL_SSL/SCRAM + CA), port/route açık.
- [ ] **`connect` ACL daraltıldı** (production sıkılaştırma): empty-prefix yerine `cdc.`/`jdbc.`/`mongo.`/`schema-history.`/`connect-cluster-`/`nginx-access`/`*.dlq` + Iceberg sink control topic prefix'i.

## E. Kaynak-tarafı önkoşullar (DBA — dış VM'ler)
- [ ] **MSSQL CDC:** `sp_cdc_enable_db` + tablo başına `sp_cdc_enable_table`; SQL Server Agent çalışıyor.
- [ ] **MSSQL scheduled (JDBC):** read-only kullanıcı + monoton `incrementing`/`timestamp` kolon.
- [ ] **MongoDB CDC:** replica set aktif; `_id` mesaj key'i olarak taşınıyor.
- [ ] **PostgreSQL CDC (gerekirse):** `wal_level=logical` + replication slot + publication.
- [ ] **Ağ:** Connect → dış MSSQL/Mongo egress açık (NetworkPolicy egress + kurumsal firewall).

## F. Depolama / katalog
- [ ] S3 endpoint **checksum** uyumu (gerekirse `AWS_REQUEST_CHECKSUM_CALCULATION=when_required` / `AWS_RESPONSE_CHECKSUM_VALIDATION=when_required`).
- [ ] Nessie `/iceberg` REST endpoint + warehouse (`s3://.../warehouse`) doğru; `?warehouse=` config 200 dönüyor.
- [ ] Bucket'lar mevcut: `src-mssql-ogrenci`, `src-mongo-lms`, `nginx-web`, `backups` (`scaffold-source.sh` `aws s3 mb` yetkisi yoksa manuel oluşturuldu).
- [ ] `s3://` vs `s3a://` şeması Trino / Spark / sink arasında tutarlı.

## G. Kalıcılık / restart
- [ ] Connect internal topic'leri (`connect-cluster-*`) gerçekten RF=3, 3 broker'a dağılı.
- [ ] Connect pod restart → Debezium re-snapshot YAPMADAN kaldığı yerden (offset topic).
- [ ] Spark streaming checkpoint S3'te (`s3://nginx-web/_checkpoints/access_log`) → nginx job restart offset'ten devam.
- [ ] Apicurio şemaları CNPG WAL+PITR ile yedekli.

## H. Bakım / operasyon
- [ ] `iceberg-maintenance` ScheduledSparkApplication saatlik çalışıyor, tüm katalog/namespace'i dinamik tarıyor (`[maintain] ...` logları).
- [ ] Compaction sonrası equality-delete dosya sayısı düşüyor, Trino okuma süresi korunuyor.
- [ ] DLQ `iceberg-sink.dlq` bozuk mesajda doluyor, sink çökmüyor.
- [ ] PodMonitor/metrikler Prometheus'a düşüyor (Debezium lag, sink commit lag, consumer lag).

## I. Fonksiyonel kabul (spec §12)
- [ ] MSSQL insert/update/delete → Bronze `rawlake.mssql_ogrenci_raw.students` değişim günlüğüne (changelog) ekleniyor; `silver-merge` koşusundan sonra Silver `lakehouse.mssql_ogrenci.students` güncel durumu yansıtıyor (delete satırı siliyor).
- [ ] Mongo doc ekle/güncelle/sil → Bronze `rawlake.mongo_lms_raw.enrollments`'a ekleniyor; `silver-merge` koşusundan sonra Silver `lakehouse.mongo_lms.enrollments` güncel durumu yansıtıyor (`_id` ile upsert/delete).
- [ ] nginx isteği → `lakehouse.web.access_log` satır artıyor, IP sha256 hash'li.
- [ ] Federasyon: tek Trino sorgusunda `mssql_ogrenci` + `mongo_lms` + `web` JOIN sonuç dönüyor.

## J. Bilinen residual / ayrı temizlik
- [ ] `jdbc-mssql-scheduled` topic isim konvansiyonu (`dbo`) vs scaffold (`ogrencidb`) — routing'i bozmaz (sink `_target_table`), istenirse birleştir.
- [ ] `dbz-mongo-lms` topic prefix `mongo.lms` + collection `lms.enrollments` → 4-segment topic olabilir; routing'i bozmaz, istenirse `topic.prefix: mongo` yapılır.
- [ ] README AI bölümü (`hr.employees` örnekleri) hâlâ AI'yı dokümante ediyor — AI prod dışı (`optional/`); README AI temizliği ayrı iş.

## K. İzleme / Monitoring

> microk8s smoke bu kalemleri KAPSAYAMAZ: microk8s'te UWM YOKTUR (yerel test
> prometheus datasource'u bearer token istemez), ve OIDC/Route yolları
> OpenShift'e özgüdür. Aşağıdakiler yalnızca gerçek OpenShift + UWM kümesinde
> doğrulanabilir.

- [ ] **UWM ön koşulu:** `openshift-monitoring/cluster-monitoring-config` içinde
      `enableUserWorkloadMonitoring: true`; `openshift-user-workload-monitoring`
      ns'inde `prometheus-user-workload-*` pod'ları Running. `monitoring.coreos.com/v1`
      CRD'leri kayıtlı (prereq-check `--set prereqCheck.enabled=true` ile bunu görüyor).
- [ ] **Grafana→Thanos süresi-dolmayan token:** `grafana-sa-token`
      (`kubernetes.io/service-account-token`) Secret'ı token controller tarafından
      doldurulur; Grafana datasource UWM Thanos querier'a (`:9091`) `Authorization: Bearer`
      ile bağlanır ve **1 saat sonra 401 atmaz** (token süresi-dolmayan üretilir —
      k8s 1.33). Thanos'a karşı 401 senaryosu yalnızca OpenShift'te tetiklenir,
      bu yüzden gerçek kümede teyit gerekir.
- [ ] **Grafana OIDC login:** `grafana` confidential Keycloak client secret'ı
      ile `grafana-oidc` Secret'ı TEK kaynaktan (`monitoring.grafana.oidc.clientSecret`
      → ExternalSecret/Vault) gelir; Grafana'ya Keycloak üzerinden OIDC ile giriş
      çalışır (client secret uyuşmazlığı = login kırık DEĞİL).
- [ ] **Trino /metrics scrape auth:** Trino `/metrics` endpoint'i (yerleşik, enable
      flag'i YOK) Web UI ile AYNI auth ile korunur. OIDC açıkken scrape HTTPS + sistem-
      bilgisi okuma yetkili bir kullanıcı gerektirir; trino ServiceMonitor scrape auth'u
      (`X-Trino-User`/basic-auth + gerekli izin) buna göre wire edilmeli — aksi halde
      hedef 401 verir. (microk8s'te endpoint'in var olduğu 401 ile doğrulandı;
      auth'lu scrape OpenShift-zamanı kalem.)
- [ ] **§9 metrik adı teyidi (sürüm-bağımlı):** microk8s'te DOĞRULANAN (var):
      `kafka_server_brokertopicmetrics_messagesin_total`,
      `kafka_server_replicamanager_underreplicatedpartitions`,
      `kafka_controller_kafkacontroller_offlinepartitionscount`, `cnpg_backends_total`,
      `cnpg_pg_replication_lag`, `kube_pod_container_status_restarts_total`,
      `nessie_storage_persist_total`. OpenShift'te TEYİT EDİLECEK (microk8s'te
      kaynak yoktu): `trino_execution_querymanager_*` (Trino scrape auth sonrası),
      `kafka_connect_connector_status` (Connect açıkken).
- [ ] **`KafkaConsumerLagHigh` alert'i / consumer-lag kaynağı:** broker JMX
      exporter consumer-group lag'i SUNMAZ; chart bu yüzden Strimzi built-in
      `kafkaExporter`'ı açar (03-kafka-strimzi.yaml, monitoring gate'i
      altında) → `kafka-kafka-exporter` Deployment'ı `kafka_consumergroup_lag`'i
      yayar ve mevcut kafka PodMonitor (aynı label/port) onu scrape eder.
      OpenShift/UWM'de teyit: `kafka-kafka-exporter` pod'u Running; gerçek
      tüketici grupları için `KafkaConsumerLagHigh` verisi/firing (ör.
      `sum(kafka_consumergroup_lag) by (consumergroup)`).
- [ ] **Alert firing:** en az bir PrometheusRule alert'i (ör. connector FAILED veya
      pod restart) UWM Alertmanager'da görünüyor / route ediliyor.
- [ ] **Grafana Route:** `components.routesTls=true` (OpenShift) iken Grafana Route'u
      cert-manager edge TLS ile açılıyor; `grafana.<domain>` erişilebilir. (microk8s'te
      Route CRD yok → Route routesTls=false ile atlanır, Grafana Service/port-forward
      ile erişilir.)

## L. İngest canlı doğrulama — OpenShift'te kalan kalemler

> pg CDC uçtan uca (INSERT+UPDATE) canlı kanıtlanmıştır; mssql/mongo/
> pg-scheduled/Spark için OpenShift'te canlı doğrulama gerekir.

### Mimari/config notları (mevcut chart ve imajda yansır)

- Strimzi v1 API'de `KafkaConnect.spec.externalConfiguration` yoktur
  (v1beta2-only); external volume mount'ları `spec.template.pod.volumes` +
  `spec.template.connectContainer.volumeMounts` ile tanımlanır (mount
  yolları aynı kalır).
- `KafkaConnect` v1'de `spec.groupId`/`configStorageTopic`/
  `offsetStorageTopic`/`statusStorageTopic` top-level alanlardır (`spec.config`
  altında değil).
- v1'in varsayılan build builder'ı Buildah'dır (Kaniko değil); bu yüzden
  `spec.build.output.additionalPushOptions` (`--tls-verify=false`) kullanılır,
  `additionalKanikoOptions` değil.
- Apache Iceberg `kafka-connect-runtime` Maven Central'da yayınlanmadığından
  in-cluster `spec.build` ile derlenemez; bunun yerine önceden-build bir
  `spec.image` kullanılır (`images/connect/Dockerfile`, multi-stage, Iceberg
  kaynaktan derlenir). İmaj `debezium-postgres` plugin'ini de içerir.
- DLQ topic RF'i parametrik: prod'da 3, tek-broker dilimlerde (ör.
  microk8s-ingest) 1.
- Worker=0 dilimlerde (ör. microk8s-ingest) Trino `include-coordinator`
  true olmalıdır ki coordinator worker rolünü de alsın.
- `connect` KafkaUser'ı `transactionalId` ACL'i + `cg-control` tüketici
  grubu ACL'ini içerir (Iceberg sink kontrol topic'i için gerekli).
- `iceberg-sink-cdc` Avro mesajlar için explicit Apicurio Avro converter
  config'i kullanır (worker-default JSON converter'a güvenilmez).
- Nessie REST katalog URI'si sink ve Trino'nun beklediği path olan
  `/iceberg/`'dir (`/v1` değil).
- Mongo JSON mesajları, Avro-converter'lı `iceberg-sink-cdc`'den ayrı, JSON
  converter kullanan bir `iceberg-sink-mongo` connector'ı üzerinden işlenir
  (fan-out sink tek-converter varsayımıyla çalışmaz).

### KALAN (OpenShift'te doğrulanacak)

- [ ] **Trino `/metrics` scrape auth (bkz. §K):** OIDC açık OpenShift'te
      auth'lu scrape wire edilmeli.
- [ ] **mssql CDC canlı** (INSERT/UPDATE/DELETE, OpenShift + gerçek MSSQL).
- [ ] **mongo CDC canlı** (`iceberg-sink-mongo` JSON sink ile).
- [ ] **pg-scheduled (Aiven JDBC batch) canlı.**
- [ ] **Spark canlı** (`sparkOperator` açma + trivial SparkPi + gerçek
      `iceberg-maintenance` ScheduledSparkApplication) — `spark-py` imaj
      build/push bekliyor.
- [ ] **`render_service`/`k8s_service.py` sabit `kafka.strimzi.io/v1beta2`
      apiVersion** — v1-only bir kümede Console'un render ettiği
      KafkaConnector/KafkaTopic CR'ları reddedilir. Düzeltme GitOps (B-v2)
      kapsamına alınmıştır.
- [ ] **Per-source credential-volume otomasyonu** — şu an her kaynak-TİPİ
      (mssql/mongo/s3/debezium-src) için sabit tek bir volume/secret adı var;
      birden fazla aynı-tip kaynak eklendiğinde (örn. iki mssql instance)
      mount adı çakışır. Kaynak-adına göre per-source adlandırma gerekiyor.
- [ ] **Scheduled-JDBC delete semantiği** — HWM (yüksek-su-işareti) tabanlı
      batch okuma fiziksel silmeleri hiç yakalamaz; delete fidelity
      gerekiyorsa CDC (Debezium) kullanılmalı (README'de not edildi).
      OpenShift'te gerçek batch job'larıyla teyit gerekir.
