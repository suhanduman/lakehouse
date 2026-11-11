# 90 — Kabul testleri (şartname maddesi ↔ kanıt)

**Bu bölümde:** kabulün nasıl yapıldığı — hangi şartname maddesinin kanıtını hangi test
yolunun ürettiği, `scripts/acceptance.sh` orkestratörünün kullanımı, beklenen çıktı, süre
beklentileri ve kabul koşusuna özgü sık durumlar.
**Süre:** okuma 15 dakika; koşu kurulu bir kümede 25–30 dakika.
**Gereken yetki:** `$LAKEHOUSE_NS` ad alanında yönetici; izleme ve yedekleme ad
alanlarında okuma.
**Nerede çalıştırılır:** `[bastion]` — **`kubectl`**, `oc`, `jq`, `python3` ve `polaris`
CLI'si kurulu yönetim makinesi. `kubectl` **zorunludur**: `scripts/acceptance.sh` ve
`scripts/polaris-setup.sh` küme çağrılarını doğrudan `kubectl` ile yapar.

Kabul, **kümede koşan kanıtla** yapılır: her madde için bir e2e yolu vardır ve her yol kendi
iddialarını yüksek sesle basar (`OK …` satırları, sonunda `E2E … OK`). Kabul koşusunun adım
adım anlatımı [40-kurulum-sonrasi](../40-kurulum-sonrasi.md) §7'dedir; bu sayfa başvuru
tablosudur.

> **Betiğin OpenShift üzerinde uçtan uca koşusu henüz yapılmamıştır
> (OpenShift'te doğrulanır).** Bugüne kadar bütün yollar kind/CI ortamında geçmiştir;
> OpenShift'e özgü olan, §3'teki bayrak ve ortam değişkeni kümesidir.

---

## 1. Ön koşullar

- Kurulum tamam ([30-kurulum](../30-kurulum.md)) ve kurulum sonrası adımları bitmiş
  ([40-kurulum-sonrasi](../40-kurulum-sonrasi.md)); `scripts/polaris-setup.sh` koşmuş
  olmalıdır.
- glue değerlerinde **demo kaynaklar açık** olmalıdır (`sources`, `pipelines`,
  `nginx.enabled` — `platform/values/glue-dev.yaml` kalıbı). Üretim değerlerinde bu üçü
  kapalıdır; kaynaklar kapalıyken pg, mongo ve nginx yolları bekledikleri connector'ları
  bulamaz.
- Yerelde **`kubectl`**, `jq`, `python3` ve `polaris` CLI'si
  (`pip install 'apache-polaris==1.7.0'`). `kubectl` yerine yalnız `oc` kurulu olması
  **yetmez**: `scripts/acceptance.sh` ve `scripts/polaris-setup.sh` `kubectl` ikilisini adıyla
  çağırır, aksi hâlde `kubectl: command not found` ile dururlar.
- **Ad alanı `lakehouse`'tur.** Ad alanı adı üründe sabittir
  ([30-kurulum](../30-kurulum.md) §4.2); e2e yol betikleri (`test/e2e/*.sh`) ve fixture
  manifest'leri de bu ad alanına **sabittir** (`NS=lakehouse` / `metadata.namespace`).
  `--ns` bayrağı yalnız `scripts/acceptance.sh`'in kendi adımlarını (ön kontrol,
  `polaris-setup`) taşır; başka bir değer verilirse betik yüksek sesle `UYARI --ns=…` basar
  ve koşu büyük olasılıkla düşer.
- Betik **kurulum yapmaz**: kind yaratmaz, `bootstrap/bootstrap.sh` çağırmaz. Taze bir
  kümede aynı yolları `test/e2e/run.sh` koşturur; CI kapısı budur
  (`.github/workflows/e2e.yaml`).

### 1.1 Üretim (OpenShift) kurulumunda kabul — hazırlık

Üretimde üç şey geliştirme kümesinden farklıdır ve kabulden **önce** hazırlanır.

**1) AD'de üç test hesabı.** Testler gerçek kullanıcı gibi giriş yapar; üretimde Keycloak'ta
geliştirme kullanıcıları yoktur. AD ekibinden şunları isteyin (adlar örnektir):

| Hesap | AD grubu | Testte ne yapar |
|---|---|---|
| `kabul-analist` | `lakehouse-analysts` | tablolar filtresiz, `sandbox`'a yazar |
| `kabul-kullanici` | `lakehouse-users` | satır filtresi ve kolon maskesi uygulanır, yazamaz |
| `kabul-grupsuz` | hiçbir `lakehouse-*` grubu | Zeppelin'e girer ama veri göremez |

Kabulden sonra hesaplar silinebilir.

**2) Demo kaynaklar.** Kabul, küçük test veritabanlarıyla (PostgreSQL `shop`, MongoDB `crm`)
ve bir nginx günlüğüyle çalışır. Bunları açmak için `platform/values/site/glue.yaml` içindeki
iki satırı (`sources: []` ve `pipelines: []`) aşağıdaki blokla **değiştirin**, sonra commit edip
itin. Bu, müşteri kaynağı eklerken izlenen yordamın aynısıdır
([50-isletme/yeni-kaynak-ve-pipeline.md](../50-isletme/yeni-kaynak-ve-pipeline.md)).

```yaml
# KABUL TESTİ demo kaynakları — kabulden sonra bu blok geri alınır (§1.2)
sources:
- name: shop
  type: postgres
  host: demo-pg-rw.lakehouse.svc
  port: 5432
  database: shop
  tables: [public.orders]
  signalTable: public.debezium_signal
- name: crm
  type: mongodb
  connectionString: "mongodb://demo-mongo.lakehouse.svc:27017/?replicaSet=rs0"
  collections: [crm.customers]
pipelines:
- {bronze: shop_raw.orders, keys: [id], bucket_count: 4, casts: {updated_at: timestamp}}
- {bronze: crm_raw.customers, keys: [_id], bucket_count: 4}
nginx: {enabled: true, partitions: 3}
```

`[bastion]`

```bash
scripts/check-site.sh
git add platform/values/site/glue.yaml
git commit -m "kabul: demo kaynaklar"
git push origin main
```

ArgoCD'nin bağlayıcıları yaratması birkaç dakika sürer. Hazır olduğunu görmek için:
`oc -n "$LAKEHOUSE_NS" get kafkaconnector` — `dbz-shop`, `sink-shop`, `dbz-crm` ve
`sink-nginx` listelenmelidir. `scripts/acceptance.sh` bunlar yoksa başlamaz ve bu bölümü
gösterir.

**3) Router sertifikasının kökü.** Testlerin bir kısmı (Superset, JupyterHub ve Trino
girişleri) yönetim makinesinden arayüzlere tarayıcı gibi bağlanır. Router herkesçe bilinen bir
CA kullanmıyorsa kökünü alın ([30-kurulum](../30-kurulum.md) §7.3'teki `~/router-ca.crt`).

### 1.2 Kabulden sonra

`scripts/acceptance.sh` bittiğinde zamanlanmış Spark işlerini **kendisi** geri açar (özetten
önce `zamanlanmış Spark işleri geri açıldı` satırı). Demo kaynakları kaldırmak için §1.1'deki
bloğu `sources: []` / `pipelines: []` olarak geri alıp itin, ardından test veritabanlarını
silin:

`[bastion]`

```bash
oc delete -f test/e2e/pg-fixture.yaml -f test/e2e/mongo-fixture.yaml \
  -f test/e2e/nginx-fixture.yaml --ignore-not-found
```

Demo tabloları (`shop`, `crm`, `nginx_raw` ve `_raw` ad alanları) katalogda kalır; silinmesi
[50-isletme/kaynak-veya-tablo-silme.md](../50-isletme/kaynak-veya-tablo-silme.md)'dedir.

---

## 2. Madde ↔ kanıt tablosu

| Şartname | Gereksinim | Kanıt (yol / komut) | Beklenen |
|---|---|---|---|
| **a, J.2.1, J.3.2** | tümü açık kaynak; sürüm/lisans listesi güncel | [surumler-ve-lisanslar.md](surumler-ve-lisanslar.md) + `oc -n $ARGOCD_NS get applications -o custom-columns='APP:.metadata.name,CHART:.spec.source.chart,REV:.spec.source.targetRevision'` | Tablodaki sürümler kümedekilerle birebir; izin verici lisanslar (AGPL/SSPL yalnız DEV-ONLY satırlarında) |
| **A.6** | toplama → omurga → dağıtık motor → Iceberg V2 → katalog → MPP SQL → BI zinciri | `test/e2e/pg-path.sh` → `test/e2e/trino-path.sh` → `test/e2e/superset-path.sh` | `E2E F2 OK`, `E2E F4 TRINO OK`, `E2E F4 SUPERSET OK`; Superset içinden `select count(*) from shop.orders` = 3 |
| **C.2.1(e)** | CDC, **connector framework** biçiminde (MSSQL/PG/Mongo) | `test/e2e/pg-path.sh` (Debezium pg) + `test/e2e/mongo-path.sh` (Debezium mongodb); `oc -n $LAKEHOUSE_NS get kafkaconnector` | `dbz-shop`, `sink-shop`, `dbz-crm` Ready; `E2E F2 OK`, `E2E F3 MONGO OK` (mongodb'de Bronze'u Iceberg sink değil `mongo-bronze` Spark işi yazar). MSSQL aynı şablondan gelir (`glue/templates/connectors.yaml` → `type: sqlserver`) |
| **C.3** | hafif log ajanı doğrudan omurgaya | `test/e2e/nginx-path.sh` (Fluent Bit 5.1.2 → Kafka dış dinleyicisi → `nginx.access` → sink) | `E2E F3 NGINX OK`; `nginx_raw.access_log` satırları geldi |
| **D(e)** | **MERGE INTO** (upsert + delete) | `test/e2e/pg-path.sh` "MERGE #1/#2/#3" adımları (`run_spark_once silver-merge`) | Kaynakta UPDATE/DELETE sonrası Silver 2 satır (`id=1:status=shipped`, `!id=2`); sonraki INSERT'te 3 satır |
| **D(f)** | compaction / expire / manifest bakımı **deklaratif planlama** | `oc -n $LAKEHOUSE_NS get scheduledsparkapplication` + `test/e2e/pg-path.sh` "bakım" adımı | 5 SSA (`silver-merge`, `mongo-bronze`, `maint-position-deletes`, `maint-compact`, `maint-expire-orphan-ttl`); bakım koşularında `MAINT_OK` (+ compact'te `rewrite_manifests`) |
| **B.3.1** | Iceberg V2, **time travel** | `test/e2e/pg-path.sh` → `verify shop.orders 2 --exact --prev-rows 3` | Silme öncesi snapshot hâlâ okunabilir ve 3 satırlı |
| **H.3.1** | pipeline'lar CronJob/motor zamanlayıcısıyla | `oc -n $LAKEHOUSE_NS get scheduledsparkapplication -o custom-columns='AD:.metadata.name,CRON:.spec.schedule,SUSPEND:.spec.suspend'` | Beş iş cron'lu ve `suspend=false` (kabul koşusundan **sonra** geri açılır — §5) |
| **H.1.1** | Schema Registry **opsiyonel** | `oc -n $LAKEHOUSE_NS get kafkaconnector dbz-shop -o jsonpath='{.spec.config}'` | JSON converter + `schemas.enable=true`; ayrı registry bileşeni yok |
| **F.1** | **iki farklı** web not defteri, kullanıcı-izole, PyIceberg önyüklü | `test/e2e/jupyterhub-path.sh` + `test/e2e/zeppelin-path.sh` | `E2E F4 JUPYTERHUB OK` (pod içinde pyiceberg → Polaris, trino TLS), `E2E F4 ZEPPELIN OK` (`%jdbc` → `shop.orders` = 3); JupyterHub'da kullanıcı başına PVC |
| **G.1.1, F.3.1** | tüm kullanıcı servisleri AD ile OIDC/LDAPS | `test/e2e/trino-path.sh` (Keycloak Bearer + `rules.json`), `test/e2e/superset-path.sh` (giriş sayfasında keycloak), `test/e2e/jupyterhub-path.sh`, `test/e2e/zeppelin-path.sh` (Shiro/AD rol kapısı) | `analyst1` 3 satır; `user1` 2 satır + `remote` maskeli; rolsüz kullanıcı Zeppelin'de 401. Gerçek tarayıcı OIDC akışı pre-ship OpenShift'te ([40-kurulum-sonrasi](../40-kurulum-sonrasi.md) §5) |
| **G.5** | Prometheus (+ Loki) | `test/e2e/monitoring-path.sh` | `E2E F5 MONITORING OK`: hedefler `up`, `kafka_consumergroup_lag` serileri, **6 alarm kuralı** `health: ok`, 0 ateşlenen alarm, 2 Grafana dashboard (yalnız dev). Loki platformdadır |
| **G.6** | Velero yedeği + katalog DB WAL/PITR | `test/e2e/dr-path.sh` | `E2E F5 DR OK`: 3 DB `ContinuousArchiving=True`, ≥3 `Backup completed`, ayrı kümeye restore + tablo sayımı, Velero backup (node-agent fs-backup) + işaret ConfigMap restore'u |
| **e, f, A.4** | OCI imaj + CRD yaşam döngüsü; deklaratif YAML/CRD; Operator tercihi | `oc -n $ARGOCD_NS get applications`; `oc -n $LAKEHOUSE_NS get kafka,kafkaconnect,cluster,keycloak,superset,sparkapplication` | Her bileşen bir CR/Application; özel imaj/Dockerfile yok (Connect imajı Strimzi `spec.build` ile kümede üretilir) |

---

## 3. Koşu ve çıktı

Kurulu bir kümede, geliştirme/vanilla adlandırmasıyla:

`[bastion]`

```bash
PATH="$PWD/.venv/bin:$PATH" scripts/acceptance.sh --ns "$LAKEHOUSE_NS"
```

OpenShift'te betik **gerçek kullanıcı gibi** giriş yapar ve izleme kanıtını kümenin
Thanos Querier'ından alır; bu yüzden birkaç ortam değişkeni verilir. AD test hesaplarının
parolaları komut satırına **yazılmaz** (kabuk geçmişine düşer); bir kez sorulur:

`[bastion]`

```bash
read -rsp "kabul-analist parolası: " ACC_ANALYST_PASS; echo
read -rsp "kabul-kullanici parolası: " ACC_USER_PASS; echo
read -rsp "kabul-grupsuz parolası: " ACC_NOGROUP_PASS; echo
export ACC_ANALYST_PASS ACC_USER_PASS ACC_NOGROUP_PASS
export ACC_FLOW=authcode ACC_SA=zeppelin \
  ACC_ANALYST_USER=kabul-analist ACC_USER_USER=kabul-kullanici ACC_NOGROUP_USER=kabul-grupsuz \
  ACC_CA_FILE=~/router-ca.crt GRAFANA_SKIP=1 \
  PROM_URL="https://$(oc -n openshift-monitoring get route thanos-querier -o jsonpath='{.spec.host}')" \
  PROM_TOKEN="$(oc whoami -t)"
PATH="$PWD/.venv/bin:$PATH" scripts/acceptance.sh --argocd-ns "$ARGOCD_NS" \
  --mon-ns openshift-user-workload-monitoring --velero-ns openshift-adp
```

Ne işe yararlar:

| Değişken | Anlamı |
|---|---|
| `ACC_FLOW=authcode` | Testler Keycloak'a tarayıcı gibi girer (üretimde parola akışı bilerek kapalıdır) |
| `ACC_*_USER` / `ACC_*_PASS` | §1.1'deki üç AD test hesabı (adlar örnektir, kendi hesaplarınızı yazın) |
| `ACC_SA=zeppelin` | Trino servis hesabı; üretimde `e2e` hesabı yoktur, aynı yetkideki `zeppelin` kullanılır |
| `ACC_CA_FILE` | Router sertifikasının kökü ([30-kurulum](../30-kurulum.md) §7.3); Superset/JupyterHub girişleri ve Thanos için |
| `PROM_URL`, `PROM_TOKEN` | İzleme yolu sorguları Thanos Querier'a oturum jetonuyla gider; `oc` oturumunuzun `cluster-monitoring-view` yetkisi olmalıdır |
| `GRAFANA_SKIP=1` | Ürün OpenShift'te Grafana kurmaz; yalnız dashboard iddiaları atlanır |

OADP kurulu değilse (felaket kurtarma bu kurulumda yoksa) sona `--skip dr` ekleyin;
özet atlanan yolu `ATLANAN:` satırında yazar.

**Beklenen çıktı** (`E2E F5 DR OK` dışındaki `E2E … OK` satırları OpenShift provasında
alınmış gerçek satırlardır; prova OADP'siz, `--skip dr` ile koşuldu ve yollar ürün
hatalarının düzeltilmesi sırasında üç koşuya bölündü. `E2E F5 DR OK` satırı ve `9/9` özeti
OADP kurulu bir kümede beklenen biçimdir, OpenShift'te henüz alınmadı; özet satırlarının
biçimi `scripts/acceptance.sh`'ten gelir):

```text
== ön kontrol
== kaynak DB fixture'ları (pg + mongo)
== polaris-setup (idempotent)

=== pg-path.sh
E2E F2 OK

=== KABUL ÖZETİ ===
E2E F2 OK
E2E F3 MONGO OK
E2E F3 NGINX OK
E2E F4 TRINO OK
E2E F4 SUPERSET OK
E2E F4 JUPYTERHUB OK
E2E F4 ZEPPELIN OK
E2E F5 MONITORING OK
E2E F5 DR OK
KABUL: 9/9 yol geçti (ns=lakehouse, mon-ns=openshift-user-workload-monitoring, velero-ns=openshift-adp)
```

Koşu sırasında Bronze beklemeleri sink'in commit aralığına göre uzar: üretimde aralık 300
saniyedir, ilk veri en geç iki turda görünür; betik bu yüzden 15 dakikaya kadar bekler
(geliştirme kümesinde 10 dakika). Kurulu bir kümede bütün koşu **40–50 dakika** sürer.

**Ters giderse:** bir yol düşerse betik orada durur, çıkış kodu **1** olur ve özet
`KABUL BAŞARISIZ` satırıyla biter (düşen yolun adını yazar). Tamamlanan yol sayısı
dokuzun altında kalırsa özet `KABUL EKSİK` der. Koşunun başında
`UYARI --ns=… : e2e yol script'leri ve fixture'lar 'lakehouse' ns'ine sabitlidir` satırını
görüyorsanız §1'deki ad alanı koşulu sağlanmamıştır — devam etmeyin, önce ad alanını
düzeltin. `kubectl: command not found` → `kubectl` kurulu değildir (§1).
`polaris CLI yok` → `PATH` önekini vermemişsinizdir. Belirtiden yola çıkan teşhis tablosu
[50-isletme/sorun-giderme.md](../50-isletme/sorun-giderme.md) §3'tedir; yola özgü sık
durumlar §5'tedir.

---

## 4. Süre beklentisi (ölçümler)

| Ortam | Süre | Not |
|---|---|---|
| Taze kind/Podman, kurulum + tüm yollar (`test/e2e/run.sh`) | ~54 dk | İmaj çekimi baskındır |
| CI (GitHub Actions, 4 vCPU, ArgoCD modu) | ~43 dk | `e2e-kind` işi |
| Kurulu kümede yalnız `scripts/acceptance.sh` | ~25–30 dk | izleme yolu ~3 dk (bir dakikalık SSA koşusunu bekler), DR yolu 3,5–4 dk |

---

## 5. Kabul koşusuna özgü sık durumlar

- **Kabulü kullanılmış bir kümede yeniden koşmak** (önceki koşu yarıda kaldı ya da yollar
  daha önce elle denendi): yollar **taze** demo durumu bekler — kaynakta 3 satır, boş Bronze
  ve Silver tabloları, offset'siz bağlayıcılar. Yalnız test veritabanını silmek **yetmez**:
  bağlayıcılar eski konumlarını hatırlar (PostgreSQL bağlayıcısı yeni veritabanında
  `The connector is trying to read change stream starting at …` ile düşer, MongoDB
  bağlayıcısı snapshot almaz) ve Bronze'daki eski satırlar sayımları bozar. Sıfırlama, demo
  kaynakları için [kaynak-veya-tablo-silme.md](../50-isletme/kaynak-veya-tablo-silme.md)
  yordamıdır; OpenShift provasında şu sırayla uygulandı ve ardından kabul baştan geçti:
  1. `shop` ve `crm` girdilerine `state: stopped` → commit → `STOPPED` → offset'leri sil
     (§3).
  2. §1.1 bloğunu geri al (`shop`, `crm`, iki `pipelines` satırı, `nginx`) → commit; ArgoCD
     bağlayıcıları, `mongo-bronze` işini ve `nginx.access` konusunu kaldırır (§4).
  3. Kalan konuları ve sink tüketici gruplarını sil (§5): `shop.*`, `crm.*`,
     `__debezium-heartbeat.shop|crm`, `nginx.dlq`; `connect-sink-shop`, `connect-sink-nginx`
     ve `-coord` eşleri.
  4. Tabloları tek seferlik Spark işiyle `PURGE` ile düşür (§6.2): `shop_raw.orders`,
     `shop.orders`, `crm_raw.customers`, `crm_raw.customers__quarantine`, `crm.customers`,
     `nginx_raw.access_log`.
  5. Test veritabanlarını sil ve yeniden uygula (§1.2'deki `oc delete -f …`, sonra
     `oc apply -f test/e2e/pg-fixture.yaml -f test/e2e/mongo-fixture.yaml`).
  6. §1.1 bloğunu **`state` satırı olmadan** geri ekle → commit; bağlayıcılar günlüğe
     `No previous offset found` ve `Snapshot completed` yazar. Sonra
     `scripts/acceptance.sh`.
- **Kabul koşusundan sonra zamanlanmış işler duruyor:** koşu sırasında işler askıya alınır;
  `scripts/acceptance.sh` bitişte (başarılı ya da başarısız) hepsini kendisi geri açar ve
  son satırda `zamanlanmış Spark işleri geri açıldı: ...` basar. Bu satırı **görmediyseniz**
  (koşu yarıda öldü: bilgisayar kapandı, bağlantı koptu) işler askıda kalmıştır;
  [sorun-giderme.md §6.1](../50-isletme/sorun-giderme.md#ssa-suspend) ile kontrol edip geri
  açın.
- **`monitoring-path.sh` "ateşlenen Lakehouse alarmı" ile düşüyor:** kümede eski `FAILED`
  `SparkApplication` CR'ları vardır; kök nedeni giderip CR'ları silin. Belgelenmiş
  `E2E_EXPECT_NO_FIRING=0` kaçışı vardır ama **kabul koşusunda kullanılmaz**
  (varsayılan `1`).
- **`dr-path.sh` Velero adımı kind'da:** PVC içerikleri fs-backup'a girmez (hostPath
  sınırı) — beklenen davranıştır, yol yine geçer. PVC yedeği kanıtı gerçek CSI
  depolamasında alınır.
- **OpenShift'te `--velero-ns openshift-adp` zorunludur:** `dr-path.sh`'in yedek/restore
  manifest'leri `metadata.namespace` **taşımaz** ve `kubectl -n "$VELERO_NS" apply` ile
  uygulanır, yani bayrak gerçekten etkilidir.

---

## 6. İzleme yolunun OpenShift ayarları

`test/e2e/monitoring-path.sh` varsayılan olarak geliştirme kümesinin Prometheus'una
port-forward ile bağlanır. OpenShift'te kullanıcı iş yükü izlemesinin Prometheus'u
yetkilendirme ister ve pod dışına açık değildir; bu yüzden `PROM_URL` verilince sorgular
kümenin Thanos Querier'ına gider (§3). Thanos, platform izlemesinin `kube-state-metrics`
serilerini de gösterir; iki Spark alarmı (`LakehouseSilverMergeStale`,
`LakehouseSparkScheduledRunFailed`) bu serilere dayanır.

| Değişken | Geliştirme (varsayılan) | OpenShift |
|---|---|---|
| `PROM_URL` | boş (port-forward) | `https://<thanos-querier Route'u>` |
| `PROM_TOKEN` | boş | `oc whoami -t` |
| `ACC_CA_FILE` | boş | router kökü (`~/router-ca.crt`) |
| `GRAFANA_SKIP` | boş | `1` |
| `PROM_STS`, `PROM_SVC`, `GRAFANA_SVC`, `GRAFANA_SECRET` | kube-prometheus-stack adları | kullanılmaz (`PROM_URL` verilince) |

`GRAFANA_SKIP=1` **yalnız** dashboard iddialarını atlar; hedef, metrik, kural ve alarm
iddiaları yüksek sesle koşmaya devam eder.

**Beklenen çıktı** (OpenShift provasında alınmış gerçek çıktıdan kısaltılmış):

```text
OK up .*kafka-resources-metrics.* (5)
OK up .*spark-operator.* (2)
OK up .*kube-state-metrics.* (2)
OK up .*polaris.* (1)
OK 6 kural yüklü
OK LakehouseSilverMergeStale health ok
OK LakehouseSparkScheduledRunFailed health ok
OK LakehouseDlqRecords health ok
OK ateşlenen alarm yok
UYARI GRAFANA_SKIP=1 -> Grafana dashboard iddiaları ATLANDI (UWM'de Grafana platform tarafındadır)
E2E F5 MONITORING OK
```

---

## 7. Kabul demosu

Müşteriye yapılan kabul **demosu** (sunum akışı, örnek senaryolar) bu depoda değildir; ayrı
tutulan demo paketi bu sürümle birlikte yeniden üretilecektir. Kabulün teknik kanıtı
yukarıdaki tablodur.

---

## Kontrol listesi

- [ ] Ön koşullar (§1) sağlandı: kurulum tamam, `polaris-setup` koştu, demo kaynaklar açık,
      `polaris` CLI'si kurulu.
- [ ] OpenShift'te §3'teki ortam değişkenleri ve bayraklar verildi (AD test hesapları, Thanos).
- [ ] `KABUL: 9/9 yol geçti` satırı alındı.
- [ ] Kabul koşusundan sonra beş `ScheduledSparkApplication` geri açıldı.
- [ ] Sürüm tablosu kümedeki fiilî sürümlerle karşılaştırıldı
      ([surumler-ve-lisanslar.md](surumler-ve-lisanslar.md) son bölüm).

## Sonraki bölüm

[40-kurulum-sonrasi.md](../40-kurulum-sonrasi.md) §7 — kabul koşusunun kurulum akışındaki
yeri. Sürüm ve lisans kanıtı: [surumler-ve-lisanslar.md](surumler-ve-lisanslar.md).
