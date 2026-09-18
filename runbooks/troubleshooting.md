# Sorun giderme

Tek yer: kurulum, boru hattı, kullanıcı yüzü, izleme ve DR sorunları. PrometheusRule'ların `runbook`
annotation'ları doğrudan buradaki bölümlere (`#connect`, `#sink`, `#silver-merge`, `#spark`,
`#spark-duration`) işaret eder.

İlk bakılacaklar:
```bash
kubectl -n argocd get applications -o wide        # Synced/Healthy mi
kubectl -n lakehouse get pods -o wide             # CrashLoop / CreateContainerConfigError / Pending
kubectl -n lakehouse get kafka,kafkaconnect,kafkaconnector,cluster,keycloak,superset,sparkapplication
kubectl -n lakehouse get events --sort-by=.lastTimestamp | tail -30
```

---

## Alarmlar (PrometheusRule `lakehouse`)

Beş kural vardır (`glue/templates/monitoring.yaml`). Durumları (dev; prod'da platformun Alertmanager/Console'u):
```bash
kubectl -n monitoring port-forward svc/monitoring-kube-prometheus-prometheus 19090:9090 &
curl -s localhost:19090/api/v1/rules | jq -r '.data.groups[].rules[] | "\(.name)\t\(.health)\t\(.state)"'
curl -s localhost:19090/api/v1/alerts | jq -r '.data.alerts[] | select(.labels.alertname|startswith("Lakehouse"))'
```

### <a id="connect"></a>LakehouseConnectTaskFailed — Connect task FAILED

`kafka_connect_connector_task_status{status="failed"} == 1` (5 dk, **critical**).

**Ne bak**
```bash
kubectl -n lakehouse get kafkaconnector
kubectl -n lakehouse get kafkaconnector <ad> -o jsonpath='{.status.connectorStatus.tasks[0].trace}'
kubectl -n lakehouse logs -l strimzi.io/kind=KafkaConnect --tail=200
```
**Ne yap**
- Geçici hata (kaynak DB yeniden başladı, ağ): `kubectl -n lakehouse annotate kafkaconnector <ad> strimzi.io/restart=true`.
- Kimlik/yetki hatası: `<source>-db` Secret'ı ve `runbooks/add-source.md` 1. adımı (replication rolü,
  publication, sinyal tablosu, CDC etkinleştirme).
- Converter/SMT hatası: `errors.tolerance: all` olduğundan kayıt `<prefix>.dlq` topic'ine düşer; oradan okuyun.
- Task başarısızlığından sonra **consumer konumu ileri kalmış olabilir** (veri boşluğu):
  `spec.state: stopped` → `kafka-consumer-groups.sh --group connect-<sink> --reset-offsets --to-earliest --execute` → `running`.

### <a id="sink"></a>LakehouseSinkStalled — Iceberg sink lag'i büyüyor

`sum by (consumergroup, topic) (kafka_consumergroup_lag{consumergroup=~"connect-sink-.*"}) > monitoring.sinkLagThreshold`
(15 dk, **warning**). Eşik: `glue/values.yaml` → `monitoring.sinkLagThreshold` (varsayılan 1000).

**Ne bak**
```bash
kubectl -n lakehouse exec lakehouse-dual-role-0 -c kafka -- \
  bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group connect-sink-<kaynak>
kubectl -n lakehouse get kafkaconnector sink-<kaynak> -o jsonpath='{.status.connectorStatus.tasks[0]}'
kubectl -n lakehouse logs -l strimzi.io/kind=KafkaConnect --tail=200 | grep -i 'commit\|iceberg\|s3'
```
**Ne yap**
- Task `RUNNING` ama commit yoksa: Iceberg katalog/S3 erişimi → [Polaris/S3 403](#polaris-403).
- Gerçek yük artışı: `sources[].sinkTasks` ve `connect.replicas` artırılır (commit aralığı upstream 300 s;
  dev'de `connect.sinkCommitIntervalMs: "30000"`).
- Uzun kesinti sonrası backlog: lag düşüyor mu izleyin (`increase(kafka_consumergroup_lag[30m]) < 0`).
- `connect-sink-<kaynak>-coord` grupları sink'in kontrol topic'ini okur ve bu kurala dâhildir; kalıcı `-coord`
  lag'i koordinatörün takıldığını gösterir → connector restart.

### <a id="silver-merge"></a>LakehouseSilverMergeStale — silver-merge eskidi

`time() - max(kube_customresource_sparkapp_termination_time{name=~"silver-merge-.*"} and on (name) kube_customresource_sparkapp_state{state="COMPLETED"} == 1) > monitoring.silverMergeStaleSeconds`
(10 dk, **warning**). Eşik: prod 2700 s (15 dk'lık cron → 45 dk), dev 172800 s (gecelik cron).

**Ne bak**
```bash
kubectl -n lakehouse get scheduledsparkapplication silver-merge -o custom-columns='SUSPEND:.spec.suspend,SONKOŞU:.status.lastRun,SONRAKİ:.status.nextRun'
kubectl -n lakehouse get sparkapplication | grep silver-merge
kubectl -n lakehouse logs <silver-merge-…>-driver --tail=100
```
**Ne yap**
- SSA askıya alınmış olabilir → [Askıda kalan ScheduledSparkApplication](#ssa-suspend).
- Koşu FAILED ise → [#spark](#spark).
- Kural boş vektörde **ateşlenmez** (`max()` sonuçsuz döner) — hiç `silver-merge-*` CR'ı yoksa alarm gelmez.
  "Hiç koşmuyor" durumu ayrı bakılır: `time() - kube_customresource_ssa_last_run{name="silver-merge"}`.

### <a id="spark"></a>LakehouseSparkScheduledRunFailed — zamanlanmış Spark koşusu FAILED

`kube_customresource_sparkapp_state{state="FAILED", name=~"(silver-merge|maint-.*|mongo-bronze)-[0-9]+"} == 1`
(1 dk, **warning**).

**Ne bak**
```bash
kubectl -n lakehouse get sparkapplication
kubectl -n lakehouse describe sparkapplication <ad> | tail -30
kubectl -n lakehouse logs <ad>-driver --tail=200
```
**Ne yap** — sık kökler:

| Belirti | Kök | Çözüm |
|---|---|---|
| `UnresolvedAddressException` / Ivy hatası | Maven Central'a çıkış yok | [İç Maven aynası](#maven) |
| `SchemaConflict` (silver-merge) | Silver kolon tipi güvenli genişletilemiyor | Elle `ALTER TABLE` ya da yeni kolon (`runbooks/add-table.md`) |
| `KAFKA_JAAS` yok (mongo-bronze) | `KafkaUser spark` yalnız mongodb kaynağı varken oluşur | Kaynağı ekleyin ya da işi kapatın |
| OOM / uzun GC (mongo-bronze, uzun kesinti sonrası) | Backlog `collect()` ile driver'a sığmıyor | Tek koşu için `spark.driver.memory` artırın ya da `lakehouse.kafka.offsets` tablo özelliğini elle ilerletin (offset yalnız başarıda ilerler) |
| Driver `CreateContainerConfigError` | `polaris-spark` Secret'ı yok | `runbooks/scripts/polaris-setup.sh` koşmamış |
| Driver 20+ dk `Pending` | Düğümde CPU/bellek isteği karşılanmıyor | `spark.driver.coreRequest` / `spark.executor.coreRequest` (dev'de 500m); düğüm kapasitesi |
| 403 `The Access Key Id you provided does not exist` | Vended credential süresi/önbelleği | [Polaris/S3 403](#polaris-403) |

**FAILED CR'lar kendiliğinden kaybolmaz** — kural, CR silinene kadar ateşlenmeye devam eder. Kök neden
giderildikten sonra: `kubectl -n lakehouse delete sparkapplication <ad>`.

### <a id="spark-duration"></a>LakehouseSparkRunTooLong — Spark koşuları yavaşladı

`rate(spark_application_success_execution_time_seconds_sum[6h]) / rate(spark_application_success_execution_time_seconds_count[6h]) > monitoring.sparkRunMaxSeconds`
(10 dk, **warning**; varsayılan eşik 1800 s).

**Kural küme genelidir, tek bir işe ait değildir:** spark-operator 2.5.2'nin exporter'ı **per-app etiket
yaymaz** (tüm seriler `app_type="Unknown"`, canlı doğrulandı) → son 6 saatte tamamlanan tüm Spark koşularının
ortalama süresi ölçülür.

**Ne bak**
```bash
kubectl -n lakehouse get sparkapplication -o custom-columns='AD:.metadata.name,DURUM:.status.applicationState.state,BASLANGIC:.status.lastSubmissionAttemptTime,BITIS:.status.terminationTime'
kubectl -n lakehouse logs <ad>-driver --tail=200 | grep -E 'incremental|FALLBACK|full|MERGE_OK|MAINT_OK'
```
**Ne yap**
- `FALLBACK full` görülüyorsa artımlı okuma penceresi kaçmıştır (uzun kesinti); bir sonraki koşuda normale
  döner, sürekli tekrar ediyorsa `pipelines[].casts` / `_cdc.ts` ve bakım snapshot'larını inceleyin.
- Veri büyüdüyse boyutlandırmayı yükseltin: `glue/values.yaml` → `spark.driver`/`spark.executor` "büyük tier"
  (driver 4g/2 core, 2×4g executor, `shufflePartitions: 200`) — `runbooks/install.md` adım 1.
- Bakım işleri veri hacmiyle büyür; eşiği (`monitoring.sparkRunMaxSeconds`) gerçekçi bir değere çekmek meşru
  bir çözümdür.

---

## Kurulum / ArgoCD

- **Connect `Build` uzun ya da başarısız:** `kubectl -n lakehouse logs -l strimzi.io/kind=KafkaConnect --tail=100`.
  Build normalde ~10 dk; registry'ye itilemiyorsa `connect.buildPushSecret` gerekir.
- **ArgoCD PVC wave kilidi (ArgoCD'ye özgü sınıf):** `application/glue` uzun süre `OutOfSync/Progressing` kalır,
  controller log'u `waiting for healthy state of /PersistentVolumeClaim/<ad>` der. Neden: StorageClass
  `WaitForFirstConsumer` ise PVC'yi bağlayacak pod **sonraki** wave'dedir → PVC `Bound` olmaz → wave ilerlemez.
  Kural: **geç wave'de tüketilen her PVC, tüketicisiyle AYNI wave'de olmalı** (`glue/templates/zeppelin.yaml`
  PVC + Deployment ikisi de `sync-wave: "2"`; unittest kilitler). Teşhis:
  ```bash
  kubectl -n argocd get app glue -o json | jq -r '.status.resources[]? | select((.status!="Synced") or ((.health.status // "Healthy")!="Healthy")) | "\(.kind)/\(.name) sync=\(.status) health=\(.health.status // "-")"'
  kubectl -n lakehouse get pvc
  ```
  Helm modunda wave kavramı yoktur → bu hata **yalnız ArgoCD yolunda** görünür.
- **İmaj çekim süreleri (taze düğüm, ilk kurulum):** `apache/zeppelin:0.12.1` ~2,7 GB → sıralı çekimde 5–14 dk;
  `quay.io/jupyter/pyspark-notebook:spark-4.1.2` ~1,94 GB → soğuk çekim **273,8 s**; Connect build ~10 dk.
  Karşı önlemler repoda: `test/e2e/kind.sh` → `serializeImagePulls: false` + `maxParallelImagePulls: 4`;
  Zeppelin `progressDeadlineSeconds: 1800`; JupyterHub `singleuser.startTimeout: 1200`; bootstrap'ta glue
  `--wait 40m`; e2e'de `wait_app glue 1 2700s`. "Pod sağlıklı ama Deployment `ProgressDeadlineExceeded`"
  durumu bu sınıftandır.
- **`job/polaris-bootstrap` `BackoffLimitExceeded`:** taze kümede CNPG ~4-5 dk sürer; `backoffLimit: 12` bunun
  içindir. Tükenmişse `kubectl -n lakehouse delete job polaris-bootstrap` + glue sync (Job idempotent).
- **NetworkPolicy:** prod'da açıktır; pre-ship OpenShift'te doğrulanana kadar `networkPolicy.enabled=false`
  ile kurup sonra açabilirsiniz. Dev/kind'da açıktır (kindnetd enforce eder). İzinli platform namespace'leri
  `glue/templates/networkpolicy.yaml` → `allow-platform-namespaces` (argocd, cert-manager, cnpg-system,
  monitoring, velero…); listede olmayan bir namespace'ten gelen istek sessizce zaman aşımına uğrar.
- **Polaris:** health `:8182/q/health`, API `:8181`; bootstrap log'u `kubectl -n lakehouse logs job/polaris-bootstrap`.

### <a id="polaris-403"></a>Polaris / S3 403 (vended credentials)

Belirti: Spark ya da Connect `ForbiddenException: The Access Key Id you provided does not exist in our records`
(403).

- **Dev/kind'da tipik kök:** VM uykusu / saat sıçraması — Polaris'in vended-credential **önbelleği**
  (varsayılan 1800 s) ile STS kimliğinin ömrü (3600 s) arasındaki pencerede önbellekteki kimlik geçersizleşir.
  Hızlı çözüm: `kubectl -n lakehouse rollout restart deploy/minio deploy/polaris`.
- Kalıcı seçenekler: Polaris önbellek süresini düşürmek (`platform/values/polaris.yaml` → `extraEnv`,
  `STORAGE_CREDENTIAL_CACHE_DURATION_SECONDS`; ayar adını kurulu Polaris sürümünde doğrulayın) **ya da**
  vending'i kapatmak: `glue/values.yaml` → `s3.vendedCredentials: false` + `platform/polaris/setup.yaml` →
  `sts_unavailable: true` (istemcilerde `X-Iceberg-Access-Delegation=none`, Trino
  `iceberg.rest-catalog.vended-credentials-enabled=false`).
- **Prod'da S3 STS yoksa bu sınıf hata hiç görülmez** (kimlik doğrudan `s3-creds`'ten gelir).
- Karıştırmayın: `DROP TABLE` sırasındaki `Unable to purge entity … set DROP_WITH_PURGE_ENABLED` 403'ü
  katalog property'si eksikliğidir (`polaris.config.drop-with-purge.enabled`) — var olan katalogda
  `polaris catalogs update --set-property …` ile eklenir (`runbooks/install.md` adım 4).

---

## Boru hattı (Bronze / Silver / nginx)

- **Bronze boş kalıyor:** connector Ready mi (`kubectl -n lakehouse get kafkaconnector`), topic oluşmuş mu
  (`kafka-topics.sh --list`), sink task trace'i ne diyor. İlk commit ≤ 5 dk (upstream commit aralığı 300 s).
- **`silver-merge` `SchemaConflict`:** Silver kolon tipi güvenli genişletilemiyor → manuel `ALTER TABLE` ya da
  yeni kolon.
- **`mongo-bronze` `KAFKA_JAAS` yok:** `KafkaUser spark` yalnız mongodb kaynağı tanımlıyken oluşur.
- **`mongo-bronze` OOM / uzun kesinti:** backlog `collect()` ile driver'a sığmıyor → tek koşu için
  `spark.driver.memory` artırın ya da `lakehouse.kafka.offsets` özelliğini elle ilerletin.
- **nginx:** `nginx.dlq` doluysa `ts` dönüşümü başarısız olmuştur (Fluent Bit lua filtresi) —
  `runbooks/nginx-agent.md`.
- **DLQ gerçeği:** Iceberg sink `ErrantRecordReporter` uygulamaz → `<prefix>.dlq` yalnız converter/SMT
  hatalarını alır; yazma hatası task'ı durdurur (bu yüzden [#connect](#connect) kuralı `critical`).

### <a id="ssa-suspend"></a>Askıda kalan ScheduledSparkApplication (`suspend: true`)

`test/e2e/lib.sh` → `run_spark_once` hedef SSA'yı `suspend: true` yapar ve **geri açmaz** (F3'ten kalan
davranış). Bir e2e/kabul koşusundan sonra aynı kümede zamanlanmış işler sessizce durur; gecikmiş
`LakehouseSilverMergeStale` alarmının en sık nedeni budur.

```bash
kubectl -n lakehouse get scheduledsparkapplication -o custom-columns='AD:.metadata.name,SUSPEND:.spec.suspend,SONKOSU:.status.lastRun'
for s in silver-merge mongo-bronze maint-position-deletes maint-compact maint-expire-orphan-ttl; do
  kubectl -n lakehouse patch scheduledsparkapplication "$s" --type=merge -p '{"spec":{"suspend":false}}' || true
done
```
Ayrıca **`schedule` değişikliği `status.nextRun`'ı yeniden hesaplatmaz** (spark-operator 2.5.2, canlı
doğrulandı): cron'u değiştirdikten sonra hemen koşması bekleniyorsa CR'ı silip yeniden yaratın.

### <a id="maven"></a>İç Maven aynası (kapalı/kısıtlı ağ)

Spark işleri **her koşuda** `spark.jars.packages` ile Iceberg runtime + AWS bundle'ı `repo1.maven.org`'dan
çözer (`/tmp/.ivy2`, pod ömürlük). Dışarı erişim yoksa işler `UnresolvedAddressException`/Ivy hatasıyla FAILED
olur. Çözüm — iç ayna tanımını values'a koyun:

```yaml
# platform/values/glue.yaml
spark:
  ivySettingsXml: |
    <ivysettings>
      <settings defaultResolver="mirror"/>
      <resolvers>
        <ibiblio name="mirror" m2compatible="true" root="https://nexus.musteri.example.com/repository/maven-public/"/>
      </resolvers>
    </ivysettings>
```
Boşken (varsayılan) hiçbir ek kaynak render edilmez, Maven Central kullanılır. Doluysa glue
`ConfigMap/spark-ivysettings` üretir, tüm SparkApplication'lara `/opt/ivy/ivysettings.xml` olarak mount eder ve
`spark.jars.ivySettings` bu yolu gösterir:
```bash
kubectl -n lakehouse get configmap spark-ivysettings -o jsonpath='{.data.ivysettings\.xml}' | head
kubectl -n lakehouse get scheduledsparkapplication silver-merge -o jsonpath='{.spec.template.sparkConf.spark\.jars\.ivySettings}{"\n"}'
```
Aynı sınıftan diğer egress ihtiyaçları: Zeppelin'in Trino JDBC indirmesi (Maven Central), JupyterHub
`postStart` `pip install` (PyPI), dbt örneği (PyPI) → kapalı ağda iç PyPI aynası ve PVC'ye önceden konmuş jar
gerekir.

---

## Kullanıcı yüzü (Trino, Superset, JupyterHub, Zeppelin)

- **Trino pod'u `CreateContainerConfigError`:** `polaris-trino` Secret'ı yok →
  `runbooks/scripts/polaris-setup.sh` koşmamış.
- **Trino `401` / `Authentication failed`:** HTTP 8080'de kimlik doğrulama YOKTUR (yalnız probe/iç trafik);
  istemciler **8443 HTTPS** kullanmalı ve `lakehouse-ca`'ya güvenmelidir. Bearer reddediliyorsa token'ın `aud`
  talebinde `trino` yoktur → realm'in `trino` client'ındaki audience mapper (`runbooks/install.md` → "Realm
  değişikliği").
- **Trino `Access Denied`:** `rules.json` **ilk eşleşen kurala** bakar — `runbooks/access-control.md`.
  Kullanıcının grubu görünmüyorsa group provider (dev: `auth.groups`; prod: `platform/values/trino-ldap.yaml`).
- **Superset `phase: Initializing` + `LifecycleComplete=False (TaskFailed: Migrate)`:** `superset-migrate`
  Job'ı CNPG hazır olmadan koştu (`psycopg2.OperationalError: connection refused`). Şablon bunu
  `lifecycle.migrate.maxRetries: 120` ile karşılar (operator varsayılanı 3 deneme ≈ 15 s). Kalıcı düştüyse
  CR'ı yeniden tetikleyin (`spec.lifecycle.migrate.trigger` değerini değiştirin) — `forceReload` benzeri bir
  alan yoktur. Teşhis: `kubectl -n lakehouse describe superset superset` +
  `kubectl -n lakehouse logs job/superset-migrate`.
- **`superset-migrate` yetim pod'ları:** yeniden denemeler `Error`/`Completed` pod'ları bırakır ve
  `kubectl get pods` çıktısını kirletir (zararsız). Temizlik:
  ```bash
  kubectl -n lakehouse delete pod -l job-name=superset-migrate --field-selector=status.phase!=Running
  ```
  Job'ın kendisini silmek CR'ı yeniden tetiklemez — yukarıdaki `trigger` yolunu kullanın.
- **JupyterHub spawn zaman aşımı:** soğuk `pyspark-notebook` çekimi ~4,5 dk →
  `singleuser.startTimeout: 1200`. NetworkPolicy uygulayan CNI'da not defteri yalnız Trino 8443 / Polaris 8181
  (+dev MinIO 9000) ve genel internete çıkabilir.
- **Zeppelin paragrafı `Interpreter Setting 'jdbc' is not ready … DOWNLOADING_DEPENDENCIES`:** açılışta
  Maven Central'dan `io.trino:trino-jdbc` indiriliyor (~64 s; `/data/local-repo` PVC'de kalıcı) — bekleyin.
  Kapalı ağda bu adım BAŞARISIZ olur: jar'ı PVC'ye koyup bağımlılığı `local: true` yapmak gerekir
  ([İç Maven aynası](#maven)).
- **Zeppelin interpreter ayarı/parolası değişmiyor:** `zeppelin-interpreter` Secret'ı yalnız TOHUM'dur; PVC'de
  dosya varsa etkisizdir →
  `kubectl -n lakehouse exec deploy/zeppelin -- rm /data/conf/interpreter.json && kubectl -n lakehouse rollout restart deploy/zeppelin`
  (UI'daki tüm interpreter değişiklikleri sıfırlanır).
- **Zeppelin: giriş başarılı ama `/api/notebook` 401:** rol kapısı
  (`[urls]` son kuralı `anyofroles[admin, analyst, student]`) — kullanıcı `lakehouse-*` gruplarının hiçbirinde
  değildir (`runbooks/access-control.md`).

---

## <a id="dr"></a>İzleme ve DR

- **Prometheus hedefi `down` / metrik yok:**
  ```bash
  curl -s localhost:19090/api/v1/targets | jq -r '.data.activeTargets[] | "\(.labels.job)\t\(.health)\t\(.lastError)"'
  kubectl -n lakehouse get podmonitor,servicemonitor
  ```
  İzleme kapsamı **bilerek dardır**: Kafka/Connect JMX (+`kafkaExporter` consumer lag), spark-operator,
  kube-state-metrics (`customResourceState`) ve Polaris mgmt. Trino/Superset/JupyterHub/Zeppelin **uygulama
  metriği toplanmaz** (Superset 6.1.0'da `/metrics` uç noktası zaten yoktur) — bu hedeflerin listede olmaması
  hata değildir.
- **Alarm ateşleniyor ama bildirim gelmiyor:** dev yığınında `alertmanager: false`'tur (kurallar
  değerlendirilir, bildirim gitmez). Prod'da bildirim OpenShift user-workload monitoring'in Alertmanager'ındadır.
- **CNPG `ContinuousArchiving=False`:**
  ```bash
  kubectl -n lakehouse get cluster <db> -o jsonpath='{.status.conditions}' | jq
  kubectl -n lakehouse logs <db>-1 -c plugin-barman-cloud --tail=50
  kubectl -n lakehouse get objectstore lakehouse-backups -o yaml | head -30
  ```
  Sık kökler: hedef bucket yok (`NoSuchBucket`; dev'de `minio-bucket-init` Job'ı koşmamış),
  `backup-s3-creds` yanlış/eksik, endpoint yanlış, `cnpg-system/plugin-barman-cloud` Deployment'ı ayakta değil.
  Bucket yaratıldıktan sonra `ScheduledBackup`'ları silip yeniden yaratmak `immediate` yedeği tetikler.
- **Velero `Backup` `PartiallyFailed`:**
  ```bash
  kubectl -n velero describe backups.velero.io <ad> | tail -40
  kubectl -n velero logs deploy/velero --tail=100
  kubectl -n velero get podvolumebackups.velero.io -l velero.io/backup-name=<ad>
  ```
  kind'da beklenen kısmi durum: `local-path` PV'leri hostPath olduğu için fs-backup onları atlar
  (`is a hostPath volume which is not supported for pod volume backup, skipping`) — **ortam sınırı**,
  yapılandırma hatası değil (`runbooks/dr.md` §5.4).
- **Velero yedeğinde hiç `PodVolumeBackup` yok:** `excludedResources` listesinde **`pods`** varsa fs-backup hiç
  tetiklenmez (canlı A/B ile doğrulandı) — `glue/values.yaml` → `velero.excludedResources` içinde `pods`
  **olmamalıdır**.
- **`kubectl get backup` yanlış nesneyi gösteriyor:** kısa ad CNPG'ye (`backups.postgresql.cnpg.io`) çözülür;
  Velero için daima **`backups.velero.io`**. Yedek silmek S3'ü temizlemez → `DeleteBackupRequest`
  (`runbooks/dr.md` §5.5).

---

## <a id="loki"></a>Loglar (Loki) — şartname G.5.2

Log toplama **platformun sorumluluğundadır**; bu repo bir Loki dağıtımı içermez. İki desteklenen yol:

### OpenShift (tercih edilen)

**OpenShift Logging** operatörü + **Loki Operator** → `LokiStack` CR'ı (`openshift-logging` namespace'i) ve
`ClusterLogForwarder` ile toplama; boyutlandırma şablonları (`1x.demo`, `1x.extra-small`, `1x.small`, …) ve
kurulum adımları OpenShift Logging belgelerindedir. Loglar Console'un **Observe → Logs** sekmesinden
sorgulanır. Bu kurulumda yapılacak tek şey `lakehouse` namespace'inin toplama kapsamında olduğunu doğrulamaktır.

### Vanilla Kubernetes

`grafana/loki` chart'ı + **`grafana/alloy`** ajanı. **Promtail EOL'dir** (yeni özellik almıyor) → yeni
kurulumlarda Alloy kullanılır. Grafana'ya Loki datasource'u eklenir; dev izleme yığını (kube-prometheus-stack)
Loki içermez — ayrı bir Application olarak eklenir.

### Sorgu örnekleri (LogQL)

```logql
# Connect task hatası (alarmla birlikte bakılır)
{namespace="lakehouse", pod=~"connect-connect-.*"} |= "ERROR"
# silver-merge driver'ının son koşusu
{namespace="lakehouse", pod=~"silver-merge-.*-driver"} |~ "MERGE_OK|FALLBACK|Exception"
# Trino sorgu hataları
{namespace="lakehouse", pod=~"trino-coordinator-.*"} |= "Query failed"
# Iceberg sink commit'leri (lag alarmının teyidi)
{namespace="lakehouse", pod=~"connect-connect-.*"} |= "Commit complete"
# Yedekler
{namespace="lakehouse", container="plugin-barman-cloud"} |= "archive"
{namespace="velero"} |= "level=error"
```

Canlı sorunlarda `kubectl logs` her zaman en hızlı yoldur; Loki, **pod öldükten sonra** log'a bakmak ve zaman
aralığı üzerinden korelasyon kurmak için gereklidir (Spark driver pod'ları tek seferlik koşularda silinir).
