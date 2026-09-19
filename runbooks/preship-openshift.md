# OpenShift pre-ship kontrol listesi (canlı doğrulama) + PDB/MM2 kararları

F1–F5 boyunca biriken "yalnız gerçek OpenShift kümesinde doğrulanabilir" maddelerin **tek listesi**. Dev/kind
kümesinde karşılığı olmayan ya da kind'ın sınırlarına takılan her şey buradadır; hepsi `[ ]` kutulu, her madde
**komut + beklenen çıktı + kaynak not** taşır.

**Nasıl kullanılır**
1. Pre-ship OpenShift ortamı hazır olunca kurulumu `docs/30-kurulum.md` ile yapın (`platform: openshift`,
   `platform/values/glue.yaml`).
2. Bu listeyi baştan sona koşun; her maddeyi kutusunu işaretleyerek ve **ölçülen çıktıyı** yanına yazarak kapatın
   (kanıt = çıktı, "bakıldı" değil).
3. Kapanış kanıtı: `scripts/acceptance.sh` 9/9 + bu listede açık kutu kalmaması
   (`runbooks/acceptance-tests.md`).

**Kapsam dışı:** kind/CI'da zaten yeşil olan her şey (10 e2e işareti) — onların kanıtı
dahili planlama notlarında (F1–F5) kayıtlıdır. Bu liste yalnız **eksik kalan canlı kanıtı** izler.

Komutlarda `oc` ve `kubectl` birbirinin yerine kullanılabilir; OpenShift'e özgü nesnelerde (`Route`, `DPA`)
`oc` yazılmıştır.

---

## 1. Kimlik ve ağ

- [ ] **1.1 Tarayıcı OIDC akışı (Superset, Trino Web UI `/ui`, JupyterHub)**
  ```bash
  oc -n lakehouse get route superset trino jupyterhub zeppelin keycloak \
    -o custom-columns='AD:.metadata.name,HOST:.spec.host,TLS:.spec.tls.termination'
  ```
  Beklenen: beş Route da müşteri alan adıyla; tarayıcıdan her birine girişte Keycloak'a yönlenme, geri dönüşte
  oturum açılması ve grup→rol eşlemesinin tutması (`lakehouse-analysts` → Superset `Alpha`, JupyterHub
  `allowed_groups`, Zeppelin `groupRolesMap`).
  *Kaynak: F4 notu "Açık kalanlar (F5/pre-ship)" — dev'de `keycloak.hostname` küme içi URL olduğu için tarayıcı
  akışı kanıtlanamaz, e2e yalnız password-grant Bearer ile kanıtlıyor (`runbooks/user-facing.md` "Dev/kind sınırı").*

- [ ] **1.2 Trino Route `reencrypt` (`tls.caBundle` dolu)**
  ```bash
  oc -n lakehouse get route trino -o jsonpath='{.spec.tls.termination}{"\n"}'   # reencrypt
  oc -n lakehouse get route trino -o jsonpath='{.spec.tls.destinationCACertificate}' | head -1
  curl -sSI "https://$(oc -n lakehouse get route trino -o jsonpath='{.spec.host}')/v1/info"
  ```
  Beklenen: `reencrypt`, `-----BEGIN CERTIFICATE-----` ile başlayan PEM ve router'ın Trino'ya bağlanabilmesi
  (gerçek HTTP yanıtı, TLS hatası değil). `tls.caBundle` boşsa Route `passthrough` olur ve **tarayıcı**
  `lakehouse-ca`'ya güvenmek zorunda kalır.
  *Kaynak: F4 notu açık kalanlar; `glue/templates/route.yaml`, `platform/values/glue.yaml` → `tls.caBundle`,
  `docs/30-kurulum.md` §5.11.*

- [ ] **1.3 JupyterHub hub → Keycloak: router sertifikasına güven**
  ```bash
  oc -n lakehouse get deploy hub -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="REQUESTS_CA_BUNDLE")]}{"\n"}'
  oc -n lakehouse exec deploy/hub -- python -c \
    "import requests;print(requests.get('https://<keycloak-host>/realms/lakehouse/.well-known/openid-configuration').status_code)"
  ```
  Beklenen: ilk komut **boş** (hub pod'unda `REQUESTS_CA_BUNDLE` bilerek YOKTUR — Keycloak edge Route'unun
  sertifikası kurumsal/genel CA ile doğrulanır, iç CA'ya daraltmak akışı kırar); discovery çağrısı **200**.
  *Kaynak: F4 notu 9. satır (TLS güven ayrımı) + final fix dalgası I5; `platform/values/jupyterhub.yaml`.*

- [ ] **1.4 Trino LDAP group provider AD'ye karşı canlı**
  ```bash
  oc -n lakehouse exec deploy/trino-coordinator -- grep -c group-provider.name=ldap /etc/trino/group-provider.properties
  oc -n lakehouse logs deploy/trino-coordinator | grep -i "ldap\|group" | tail -20
  ```
  Beklenen: `1`; bir AD kullanıcısının token'ıyla `SHOW SCHEMAS FROM lakehouse` ve `rules.json` grup kurallarının
  uygulanması. Trino grubu **CN** olarak alır → AD grubunun CN'i `lakehouse-analysts` gibi olmalıdır.
  *Kaynak: F4 notu 3. satır + açık kalanlar; `platform/values/trino-ldap.yaml`, `runbooks/access-control.md`
  "LDAP group provider (prod)".*

- [ ] **1.5 Keycloak LDAP federasyonu `bindCredential` (Secret'tan, Git'e girmeden)**
  ```bash
  oc -n lakehouse get keycloakrealmimport lakehouse -o jsonpath='{.status.conditions[?(@.type=="Done")].status}{"\n"}'
  oc -n lakehouse get secret keycloak-clients -o jsonpath='{.data.ldap-bind}' | wc -c
  ```
  Beklenen: `True`; Secret anahtarı dolu ve realm JSON'unda düz parola YOK (değerler `spec.placeholders` ile
  Secret'tan enjekte edilir). AD federasyonu Keycloak Console → User Federation'da bağlanıyor.
  *Kaynak: F1 notu 13. satır (realm import düz değer ister → yer tutucu mekanizması); `docs/30-kurulum.md`
  §5.6.*

- [ ] **1.6 Sertifika yenilemesi kesintisiz mi (cert-manager → Trino)**
  ```bash
  oc -n lakehouse get certificate trino-tls \
    -o custom-columns='READY:.status.conditions[0].status,NOTAFTER:.status.notAfter,RENEWAL:.status.renewalTime'
  oc -n lakehouse get pod -l app.kubernetes.io/name=trino \
    -o custom-columns='POD:.metadata.name,RESTARTS:.status.containerStatuses[0].restartCount'
  ```
  Beklenen: `trino-tls` (`duration: 2160h`, `renewBefore: 360h`) kendiliğinden yenilenir; yenileme anında
  coordinator/worker **restart sayısı artmaz** ve açık oturumlar düşmez (Trino'nun
  `http-server.https.ssl-context.refresh-time` varsayılanı 1 dk'dır — biz ayarlamıyoruz). Prova için sertifikayı
  elle yeniletip (cert-manager renew) restart sayacını yeniden okuyun.
  *Kaynak: F4 notu açık kalanlar; `glue/templates/tls.yaml`, `runbooks/user-facing.md` "Bilinen açıklar".*

- [ ] **1.7 NetworkPolicy OVN altında: apiserver → webhook yolu**
  ```bash
  oc get validatingwebhookconfiguration,mutatingwebhookconfiguration \
    -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{range .webhooks[*]}{.clientConfig.service.namespace}/{.clientConfig.service.name}{" "}{end}{"\n"}{end}'
  oc -n lakehouse get networkpolicy
  oc -n lakehouse apply --dry-run=server -f test/e2e/cnpg-restore.yaml
  oc -n lakehouse get sparkapplication
  ```
  Beklenen: `lakehouse` ns'ine bakan bir webhook Service'i **yok** (webhook'lar `cnpg-system`, spark-operator ve
  cert-manager ns'lerindedir) → yalnız-Ingress politikaları apiserver→webhook yolunu etkilemez;
  `--dry-run=server` apply **kabul edilir** ve SparkApplication'lar normal koşar. Reddedilirse
  (`failed calling webhook …`) apiserver'ı geçiren bir `allow-apiserver` kuralı
  `glue/templates/networkpolicy.yaml`'a eklenir.
  *Kaynak: F5 notu "Açık kalanlar (F6/pre-ship)" (apiserver→webhook OVN'de) + `glue/templates/networkpolicy.yaml`
  başındaki not; dev'de kindnetd enforce eder (F4 10. satır, F5 22. satır).*

- [ ] **1.8 Kafka dış listener Route (`kafka.externalListener: true`)**
  ```bash
  oc -n lakehouse get kafka lakehouse -o jsonpath='{range .status.listeners[*]}{.name}{"\t"}{.bootstrapServers}{"\n"}{end}'
  oc -n lakehouse get route -l strimzi.io/cluster=lakehouse
  ```
  Beklenen: `external` listener'ı için bootstrap Route'u (TLS passthrough) + broker başına birer Route; dışarıdan
  `SCRAM-SHA-512` ile bağlanılabiliyor. kind'da bu yol **NodePort**'tur ve e2e yalnız varlığını doğrular.
  *Kaynak: F3 notu 9. satır; `glue/templates/kafka.yaml` (`type: route` yalnız openshift),
  `platform/values/glue.yaml` yorumlu `kafka: {externalListener: true}`.*

- [ ] **1.9 Fluent Bit ajanı gerçek nginx sunucusunda**
  ```bash
  # nginx sunucusunda
  systemctl status fluent-bit
  # kümede: topic'e kayıt düşüyor mu
  oc -n lakehouse exec lakehouse-dual-role-0 -- bin/kafka-topics.sh --bootstrap-server localhost:9093 \
    --command-config /tmp/client.properties --describe --topic nginx.access
  ```
  Beklenen: ajan dış Route'a SASL_SSL/SCRAM ile bağlanır, `nginx.access` topic'ine kayıt düşer, `nginx_raw`
  tablosunda satır sayısı artar, DLQ (`nginx.dlq`) boş kalır.
  *Kaynak: F3 notu 7. satır (küme içi ajan kanıtlandı) + F3 "Açık kalanlar" (gerçek sunucuda kurulum);
  `runbooks/nginx-agent.md`.*

---

## 2. İzleme ve loglar

- [ ] **2.1 User Workload Monitoring açık**
  ```bash
  oc -n openshift-monitoring get cm cluster-monitoring-config -o yaml
  oc -n openshift-user-workload-monitoring get pods
  ```
  Beklenen: ConfigMap'in `data` altındaki config.yaml gövdesinde `enableUserWorkload: true`; `prometheus-user-workload-*`,
  `prometheus-operator-*`, `thanos-ruler-*` pod'ları `Running`.
  *Kaynak: F5 notu "Açık kalanlar (F6/pre-ship)"; `docs/20-on-kosullar.md` madde 9.1.*

- [ ] **2.2 glue'nun izleme nesneleri UWM tarafından toplanıyor (Polaris dâhil)**
  ```bash
  oc -n lakehouse get podmonitor,servicemonitor,prometheusrule
  TOKEN=$(oc whoami -t); HOST=$(oc -n openshift-monitoring get route thanos-querier -o jsonpath='{.spec.host}')
  curl -sSk -H "Authorization: Bearer $TOKEN" \
    --data-urlencode 'query=up{namespace="lakehouse"} == 1' "https://$HOST/api/v1/query" \
    | jq -r '.data.result[].metric.job' | sort | uniq -c
  ```
  Beklenen: `PodMonitor/kafka-resources-metrics`, Polaris chart'ının `ServiceMonitor`'ü (yönetim portu 8182
  `/q/metrics`, job `polaris-mgmt`), spark-operator PodMonitor ve `PrometheusRule/lakehouse` listelenir; sorgu
  **en az** şu job'ları döndürür: `kafka-resources-metrics` (3 hedef — 3 broker + kafka-exporter),
  `spark-operator`, `polaris-mgmt`, `kube-state-metrics`. Superset ile Trino/hub/Zeppelin hedefleri **bilerek
  yoktur** (Superset 6.1.0'da `/metrics` uç noktası yok; izleme kapsamı boru hattı sağlığına daraltıldı).
  *Kaynak: F5 notu 4/5/6. satırlar ve final fix dalgası (`OK up .*polaris.* (1)`); `platform/values/polaris.yaml`
  (`serviceMonitor.enabled`), `glue/templates/monitoring.yaml`.*

- [ ] **2.3 Beş alarm kuralı UWM'de sağlıklı**
  ```bash
  curl -sSk -H "Authorization: Bearer $TOKEN" "https://$HOST/api/v1/rules" \
    | jq -r '.data.groups[] | select(.name=="lakehouse") | .rules[] | "\(.name)\t\(.health)"'
  ```
  Beklenen: `LakehouseConnectTaskFailed`, `LakehouseSinkStalled`, `LakehouseSilverMergeStale`,
  `LakehouseSparkScheduledRunFailed`, `LakehouseSparkRunTooLong` → beşi de `ok` ve hiçbiri boş yere `firing`
  değil. Prod'da `monitoring.silverMergeStaleSeconds` gerçek `silver-merge` cron aralığına göre ayarlanmış olmalı.
  *Kaynak: F5 notu 9. satır; `glue/templates/monitoring.yaml`, `runbooks/troubleshooting.md` "Alarmlar".*

- [ ] **2.4 Alertmanager yönlendirmesi/bildirimi (platformun işi)**
  ```bash
  oc -n openshift-monitoring get secret alertmanager-main -o jsonpath='{.data.alertmanager\.yaml}' | base64 -d | head -40
  ```
  Beklenen: `Lakehouse*` alarmlarını yakalayan bir route/receiver (e-posta, Slack, ITSM…) tanımlı ve test
  bildirimi ulaşıyor. Dev'de Alertmanager kapalıdır — kurallar değerlendirilir ama **hiçbir yere gitmez**.
  *Kaynak: F5 notu "Açık kalanlar (F6/pre-ship)".*

- [ ] **2.5 LokiStack / ClusterLogForwarder — `lakehouse` kapsamda**
  ```bash
  oc -n openshift-logging get lokistack,clusterlogforwarder
  oc -n openshift-logging get clusterlogforwarder -o yaml | grep -i -A5 "inputs\|namespaces"
  ```
  Beklenen: LokiStack `Ready`; forwarder girdisi `lakehouse` namespace'ini kapsıyor; Console → Observe → Logs
  içinde `runbooks/troubleshooting.md` §Loglar'daki LogQL örnekleri (Connect `ERROR`, silver-merge driver,
  Trino "Query failed", Barman `archive`) sonuç veriyor. **Bu repoda Loki dağıtımı yoktur** — kurulum platformun.
  *Kaynak: F5 notu "Açık kalanlar (F6/pre-ship)"; `runbooks/troubleshooting.md` §Loglar (Loki).*

- [ ] **2.6 e2e izleme yolunun UWM uyarlaması (script olduğu gibi koşmaz)**
  `test/e2e/monitoring-path.sh` kube-prometheus-stack nesne adlarını **varsayılan** alır ama hepsi ezilebilir
  (`PROM_STS`, `PROM_SVC`, `GRAFANA_SVC`, `GRAFANA_SECRET`) ve Grafana iddiaları `GRAFANA_SKIP=1` ile atlanır
  (UWM'de Grafana platform tarafındadır):
  ```bash
  PROM_STS=prometheus-user-workload PROM_SVC=prometheus-user-workload GRAFANA_SKIP=1 \
    scripts/acceptance.sh --mon-ns openshift-user-workload-monitoring --velero-ns openshift-adp
  ```
  UWM Prometheus'u yetkilendirme ister (port-forward'lu düz `curl` 401 alabilir): script bu hâlde düşerse
  izleme kanıtı 2.2/2.3 maddeleriyle (thanos-querier + token) elle alınır — kural/hedef adları aynıdır.
  *Kaynak: canlı kod okuması (`test/e2e/monitoring-path.sh`) + F5 notu (kube-prometheus-stack DEV-ONLY).*

---

## 3. Yedekleme / DR (OADP)

- [ ] **3.1 `DataProtectionApplication` alan adları kurulu sürümle doğrulandı**
  ```bash
  oc explain dataprotectionapplication.spec --recursive | head -60
  oc -n openshift-adp get dpa -o custom-columns='AD:.metadata.name,RECONCILED:.status.conditions[?(@.type=="Reconciled")].status'
  ```
  Beklenen: `runbooks/dr.md` §6'daki eşleme tablosunun alanları (`backupLocations[].velero.objectStorage`,
  `configuration.velero.defaultPlugins`, `configuration.nodeAgent`) kurulu OADP sürümünde birebir var; DPA
  `Reconciled=True`.
  *Kaynak: F5 notu "Açık kalanlar (F6/pre-ship)"; `runbooks/dr.md` §6.*

- [ ] **3.2 BSL `Available` → ancak sonra glue'da `velero.enabled: true`**
  ```bash
  oc -n openshift-adp get backupstoragelocation
  # sonra: platform/values/glue.yaml -> velero: {enabled: true, namespace: openshift-adp}
  oc -n openshift-adp get schedules.velero.io lakehouse-daily
  ```
  Beklenen: BSL `Available`; glue açıldıktan sonra `Schedule/lakehouse-daily` **`openshift-adp`** ns'inde doğar.
  Sıra ters olursa `Schedule` CRD'si yokken glue Degraded olur.
  *Kaynak: `runbooks/dr.md` §6, `docs/20-on-kosullar.md` madde 9.2; `platform/values/glue.yaml`.*

- [ ] **3.3 PVC içeriği gerçekten yedekleniyor (CSI snapshot + Data Mover)**
  ```bash
  oc get volumesnapshotclass
  oc -n openshift-adp get podvolumebackups.velero.io
  oc -n openshift-adp get backups.velero.io \
    -o custom-columns='AD:.metadata.name,FAZ:.status.phase,OGE:.status.progress.itemsBackedUp'
  ```
  Beklenen: JupyterHub `hub-db-dir`, kullanıcı PVC'leri ve Zeppelin `zeppelin-data` için PodVolumeBackup ya da
  DataUpload üretiliyor (kind'da hostPath olduğu için **hiç üretilmiyordu**). Tercih edilen yol
  `Backup.spec.snapshotVolumes` + `spec.snapshotMoveData`, `defaultPlugins` listesinde `csi`.
  *Kaynak: F5 notu 19. satır + "Açık kalanlar (F6/pre-ship)"; `runbooks/dr.md` §5.3–5.4.*

- [ ] **3.4 fs-backup dışlama annotation'ları canlıda (Kafka `data-0`, CNPG `pgdata`)**
  ```bash
  oc -n lakehouse get pod lakehouse-dual-role-0 \
    -o jsonpath='{.metadata.annotations.backup\.velero\.io/backup-volumes-excludes}{"\n"}'
  oc -n lakehouse get pod -l cnpg.io/cluster=polaris-db \
    -o jsonpath='{.items[0].metadata.annotations.backup\.velero\.io/backup-volumes-excludes}{"\n"}'
  ```
  Beklenen: sırasıyla `data-0` ve `pgdata` (annotation **pod hacim adını** alır, PVC adını değil).
  *Kaynak: F5 notu 18. satır; `runbooks/dr.md` §1 kapsam tablosu.*

- [ ] **3.5 CNPG Barman restore provası prod bucket'ında**
  ```bash
  oc -n lakehouse get cluster \
    -o custom-columns='AD:.metadata.name,INSTANCES:.spec.instances,ARSIV:.status.conditions[?(@.type=="ContinuousArchiving")].status'
  oc -n lakehouse get backups.postgresql.cnpg.io \
    -o custom-columns='AD:.metadata.name,FAZ:.status.phase,YONTEM:.status.method'
  # prova: test/e2e/cnpg-restore.yaml desenini prod ObjectStore'a uyarlayıp AYRI bir Cluster'a geri yükleyin
  ```
  Beklenen: üç Cluster'da `ContinuousArchiving=True`, günlük base backup `completed` / `method: plugin`; geri
  yüklenen kümede `polaris_schema` altında 8 tablo sayılıyor. Restore kümesi prova sonunda **silinir**.
  *Kaynak: F5 notu 13/14/15. satırlar; `runbooks/dr.md` §4, `test/e2e/cnpg-restore.yaml`.*

- [ ] **3.6 Uçtan uca kabul koşusu OpenShift ns adlarıyla (HİÇ KOŞMADI)**
  ```bash
  scripts/acceptance.sh --ns lakehouse --mon-ns openshift-user-workload-monitoring --velero-ns openshift-adp
  ```
  Beklenen: `KABUL: 9/9 yol geçti`. **Uyarı:** izleme yolu için 2.6 maddesi geçerlidir; DR yolu
  (`test/e2e/dr-path.sh`) ns'i `VELERO_NS` ile doğru alır ve Velero nesnelerini **tam adla**
  (`backups.velero.io`) sorgular — kısa `backup` adı kümede CNPG'ye çözülür.
  *Kaynak: F5 notu "Açık kalanlar (F6/pre-ship)" + 20. satır; `runbooks/acceptance-tests.md`.*

---

## 4. Kaynaklar, imajlar, dış bağımlılıklar

- [ ] **4.1 MSSQL kaynağı canlı (şimdiye dek yalnız render edildi)**
  ```bash
  oc -n lakehouse get kafkaconnector \
    -o custom-columns='AD:.metadata.name,HAZIR:.status.conditions[?(@.type=="Ready")].status'
  oc -n lakehouse get kafkaconnector <kaynak>-source -o jsonpath='{.spec.config.database\.encrypt}{"\n"}'
  oc -n lakehouse logs deploy/connect-connect | grep -i "schema.history\|truststore\|encrypt" | tail
  ```
  Beklenen: connector `Ready=True`; `database.encrypt=true` (varsayılan — `trustServerCertificate` YALNIZ özel
  CA'lı laboratuvarda `true`, üretimde truststore `extraConfig` ile verilir); `schema.history.internal.*`
  SASL_SSL + PEM truststore yolu (`/opt/kafka/connect-certs/lakehouse-cluster-ca-cert/ca.crt`) çalışıyor.
  *Kaynak: F2 notu 11. ve 15. satırlar; `glue/templates/connectors.yaml`, `runbooks/add-source.md`.*

- [ ] **4.2 Connect `buildImage` iç registry'ye itiliyor**
  ```bash
  oc -n lakehouse get kafkaconnect connect -o jsonpath='{.spec.build.output}{"\n"}'
  oc -n lakehouse get secret connect-push -o jsonpath='{.type}{"\n"}'     # kubernetes.io/dockerconfigjson
  oc -n lakehouse get kafkaconnect connect -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}{"\n"}'
  ```
  Beklenen: `image-registry.openshift-image-registry.svc:5000/<ns>/connect:<sürüm>` (`:latest` **yasak** —
  digest sabitlenemez), `connect-push` Secret'ı mevcut, build (~10 dk) sonunda `Ready=True`.
  *Kaynak: `platform/values/glue.yaml` (`connect.buildImage`/`buildPushSecret`), `docs/20-on-kosullar.md` madde
  5, `runbooks/upgrade.md` §2.3.*

- [ ] **4.3 Superset imajı müşteri aynasında ve etiketi sabit**
  ```bash
  oc -n lakehouse get superset superset -o jsonpath='{.spec.image}{"\n"}'
  oc -n lakehouse get pod -l instance=superset -o jsonpath='{.items[0].status.containerStatuses[0].imageID}{"\n"}'
  ```
  Beklenen: imaj müşteri aynasından çekiliyor (varsayılan `apachesuperset.docker.scarf.sh/apache/superset`
  **yönlendiricisi**
  değil) ve `6.1.0-dev` etiketi aynada *tag immutability* ile kilitli — operator `spec.image` **digest kabul
  etmez** (`repository:tag`), dolayısıyla etiketin taşınmaması aynanın sorumluluğudur. `imageID` digest'i kayda
  geçirilir; yükseltmede sürücülerin (psycopg2/trino/authlib) hâlâ imajda olduğu doğrulanır.
  *Kaynak: F4 notu 8. satır + açık kalanlar, F5 notu "Açık kalanlar (F6/pre-ship)"; `runbooks/versions.md`.*

- [ ] **4.4 Maven Central egress ya da iç ayna**
  ```bash
  oc -n lakehouse get configmap spark-ivysettings -o jsonpath='{.data.ivysettings\.xml}' | head
  oc -n lakehouse get scheduledsparkapplication silver-merge \
    -o jsonpath='{.spec.template.sparkConf.spark\.jars\.ivySettings}{"\n"}'
  ```
  Beklenen: ya `repo1.maven.org`'a sürekli erişim var (ConfigMap yok), ya da `spark.ivySettingsXml` dolu ve tüm
  Spark işleri `/opt/ivy/ivysettings.xml`'i gösteriyor. Aynı sınıftan diğer egress ihtiyaçları: Zeppelin'in
  Trino JDBC indirmesi (Maven), JupyterHub `postStart` ve dbt örneği (PyPI) → kapalı ağda iç PyPI aynası da şart.
  *Kaynak: F2/F4 notları; `runbooks/troubleshooting.md` "İç Maven aynası", `docs/20-on-kosullar.md` madde 10.*

- [ ] **4.5 Depolama sınıfları ve boyutlandırma**
  ```bash
  oc get storageclass
  oc -n lakehouse get pvc -o custom-columns='AD:.metadata.name,SC:.spec.storageClassName,BOYUT:.spec.resources.requests.storage'
  ```
  Beklenen: `kafka.storageClass` ve `cnpg.*.storageClass` ortamın **CSI** sınıfıyla doldurulmuş (3.3 maddesi
  buna bağlıdır), PVC boyutları veri hacmine uygun; tablo/veri büyüklüğü küçük tier'ı aşıyorsa
  `platform/values/glue.yaml`'daki yorumlu büyük tier `spark` bloğu açılır.
  *Kaynak: `docs/10-planlama.md` §2 (boyutlandırma); F0 S4 tier kararı.*

- [ ] **4.6 Superset Alerts & Reports kararı**
  Şu an **kapalı**. Açılacaksa Valkey (broker) + `celeryWorker` + headless tarayıcı üçlüsü gerekir; karar
  pre-ship'te müşteriyle verilir, açılırsa canlı kanıt bu maddenin altına yazılır.
  *Kaynak: F4 notu açık kalanlar; `runbooks/user-facing.md` "Alerts & Reports (varsayılan KAPALI)".*

- [ ] **4.7 Veri metrikleri dashboard'u: SQL müşteri tablolarına uyarlandı → import → render**
  ```bash
  curl -s 'https://<superset-host>/api/v1/dashboard/?q=(filters:!((col:slug,opr:eq,value:iceberg-metadata)))' | jq '.count'
  ```
  Beklenen: `count` **1**.
  *Kaynak: `runbooks/data-metrics.md` (PROD UYARISI, §3, §4).*

---

## 5. Kararlar (F6'da alındı)

### 5.1 PodDisruptionBudget yaratılmaz

**Karar:** glue hiçbir bileşen için `PodDisruptionBudget` üretmez.

**Gerekçe**
1. **PDB'si olması gerekenlerinki zaten operatörlerinden gelir.** Prod'da CNPG kümeleri `instances: 2`
   (`platform/values/glue.yaml` → `cnpg.{polarisDb,keycloakDb,supersetDb}`) ile koşar ve CNPG kendi PDB'lerini
   yönetir; Kafka `replicas: 3` (`glue/values.yaml` → `kafka.replicas`, `min.insync.replicas: 2`) ile koşar ve
   PDB'yi Strimzi yönetir. Elle eklenecek bir PDB bunlarla çakışır.
2. **Geri kalan her şey tek replikalıdır.** Kafka Connect (`connect.replicas: 1`), Trino coordinator (chart
   gereği tek), Polaris, Superset (`superset.replicas: 1`), JupyterHub hub ve Zeppelin
   (`glue/templates/zeppelin.yaml` → `replicas: 1`) — her iki değer setinde de 1. Tek replikalı bir iş yüküne
   `minAvailable: 1` vermek **düğüm boşaltmayı kilitler**: yama/yükseltme sırasında `oc adm drain` hiç
   ilerlemez. `maxUnavailable: 1` ise zaten mevcut davranıştır → PDB hiçbir şey eklemez.
3. Trino worker'ları (`server.workers: 2`, `platform/values/trino.yaml`) durumsuzdur; düşen sorgu yeniden
   koşturulur, veri kaybı olmaz.

**Ne zaman yeniden ele alınır:** bir bileşende HA isteniyorsa önce **replika sayısı** artırılır (Trino ikinci
coordinator'ı desteklemez; Connect/Polaris/Superset ölçeklenebilir) ve PDB kararı o değişiklikle **birlikte**
verilir. Tek başına PDB eklemek erişilebilirlik kazandırmaz, yalnız bakımı zorlaştırır. Özet tablo:
`runbooks/versions.md`.

### 5.2 Kafka verisi DR kapsamı dışıdır — dönüş yolu yeniden akıtma (MirrorMaker 2 yok)

**Karar:** Kafka topic verisi yedeklenmez (`data-0` hacmi annotation ile fs-backup'tan dışlanır) ve
**MirrorMaker 2 kurulmaz**.

**Gerekçe:** Kafka bu mimaride **taşıyıcıdır**, kayıt sistemi değil — kalıcı gerçek kaynak veritabanlarında
(Debezium kaynakları) ve Iceberg/S3'tedir. Broker verisi kaybolursa doğru dönüş yolu Debezium'un yeniden
snapshot'ıdır (tablo bazında artımlı snapshot sinyali: `runbooks/add-table.md`); bu, yedekten dönen bayat
offset/şema geçmişiyle çalışmaktan daha tutarlıdır. MirrorMaker 2 bir yedekleme aracı **değil**, çoklu-site
çoğaltma aracıdır: ikinci bir aktif küme gereksinimi doğmadan kurulması ek broker'lar, ayrı ACL/offset çevirisi
ve sürekli çift trafik demektir — tek site kurulumda maliyeti faydasından büyüktür.

**Ne zaman yeniden ele alınır:** müşteri ikinci bir site/küme (aktif-aktif ya da sıcak yedek) isterse MM2 o
başlıkla değerlendirilir. Kapsam tablosu ve tek cümlelik karar: `runbooks/dr.md` §1.

### 5.3 Superset `migrate` DB kapısı `maxRetries: 120` ile kalır

**Karar:** `Superset.spec.lifecycle.migrate.maxRetries: 120` korunur; operator'ün lifecycle Job'u için ek bir
bekleme/init mekanizması eklenmez.

**Gerekçe:** Superset operator'ün Migrate görevi CNPG Cluster'ı hazır olmadan başlar ve varsayılan 3 denemeyi
**14 saniyede** tüketip kalıcı `TaskFailed` durumunda kalır (F4 notu 15. satır, canlı). Operator API'si CR'ı
`Cluster`'a bağlayacak bir bağımlılık alanı sunmaz; elimizdeki tek deklaratif kaldıraç deneme sayısıdır. 120
deneme taze kümede CNPG'nin hazır olma süresini (~4 dk) rahatça kapsar ve canlı kanıtlanmıştır (ilk deneme
`Error`, sonraki denemede başarı). Pre-ship teyidi: `oc -n lakehouse get jobs -l instance=superset` →
`superset-migrate` **Complete**.

---

## 6. Kabul

- [ ] **6.1 Kabul koşusu ve kanıt paketi**
  `runbooks/acceptance-tests.md` (madde ↔ kanıt tablosu) + `scripts/acceptance.sh` çıktısı; 3.6
  maddesindeki ns bayraklarıyla koşulur ve çıktı müşteriye teslim edilen kanıt paketine konur.

- [ ] **6.2 Bu listede açık kutu kalmadı**
  Kapatılamayan madde varsa kabul öncesi **yazılı** olarak riskiyle birlikte kayda geçirilir.

> **Kapsam dışı:** kabul demosunun v2'ye uyarlanması (`~/Desktop/kc-kabul-demo`, v1 formatında) ayrı bir plandır,
> F6'ya dâhil değildir.
