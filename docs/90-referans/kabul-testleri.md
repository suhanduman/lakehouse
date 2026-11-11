# 90 — Kabul testleri (şartname maddesi ↔ kanıt)

Bu sayfa kabulün nasıl yapıldığını anlatır: hangi şartname maddesinin kanıtını hangi
testin ürettiği, `scripts/acceptance.sh` betiğinin nasıl çalıştırıldığı, ne çıktı verdiği
ve ne kadar sürdüğü. Kabulün kurulum akışındaki yeri
[40-kurulum-sonrasi](../40-kurulum-sonrasi.md) §7'dedir; bu sayfa başvuru tablosudur.

**Ne kadar sürer:** okuma 15 dakika. Hazırlık (§1.1) yarım gün, koşu 40–50 dakika.

**Hangi yetkiler gerekir:** `$LAKEHOUSE_NS` ad alanında yönetici; izleme ve yedekleme ad
alanlarında okuma; kurumun lakehouse Git deposunda `main` dalına yazma.

**Komutlar nerede çalışır:** `[bastion]` — `kubectl`, `oc`, `jq`, `python3` ve `polaris`
araçlarının kurulu olduğu, `oc login` ile kümeye bağlı ve Git deposunun bir kopyasının
(`~/lakehouse`) bulunduğu yönetim makinesi.

Kabul **kümede çalışan testlerle** yapılır. Her şartname maddesi için bir test vardır;
her test neyi doğruladığını tek tek yazar (`OK …` satırları) ve sonunda `E2E … OK`
satırını basar.

---

## 1. Ön koşullar

- Kurulum ([30-kurulum](../30-kurulum.md)) ve kurulum sonrası adımlar
  ([40-kurulum-sonrasi](../40-kurulum-sonrasi.md)) bitmiş olmalıdır.
- Kabul için küçük **demo kaynaklar** açılır (§1.1). Kurumun kendi kaynaklarıyla birlikte
  çalışabilirler.
- Yönetim makinesinde `kubectl`, `jq`, `python3` ve `polaris` aracı kurulu olmalıdır.
  `polaris` aracı kurulumda `.venv` klasörüne kurulur
  ([40-kurulum-sonrasi](../40-kurulum-sonrasi.md) §2.1). Yalnız `oc` kurulu olması
  **yetmez**: betik `kubectl` komutunu adıyla çağırır.
- Ad alanı `lakehouse` olmalıdır; testler ve test veritabanları bu ada bağlıdır
  ([30-kurulum](../30-kurulum.md) §4.2).
- Betik **kurulum yapmaz**; kurulu bir kümede çalışır.

### 1.1 Kabulden önce hazırlık

Kabulden önce üç şey hazırlanır.

**1) AD'de üç test hesabı.** Testler gerçek kullanıcı gibi giriş yapar. AD ekibinden şu
hesapları isteyin (adlar örnektir):

| Hesap | AD grubu | Testte ne yapar |
|---|---|---|
| `kabul-analist` | `lakehouse-analysts` | tabloları filtresiz görür, `sandbox`'a yazar |
| `kabul-kullanici` | `lakehouse-users` | satır filtresi ve kolon maskesi uygulanır, yazamaz |
| `kabul-grupsuz` | hiçbir `lakehouse-*` grubu | Zeppelin'e girer ama veri göremez |

Kabulden sonra hesaplar silinebilir.

**2) Demo kaynaklar.** Kabul, küçük test veritabanlarıyla (PostgreSQL `shop`, MongoDB `crm`)
ve bir nginx günlüğüyle çalışır. Önce test veritabanlarını kurun:

`[bastion]`

```bash
cd ~/lakehouse
oc apply -f test/e2e/pg-fixture.yaml -f test/e2e/mongo-fixture.yaml
```

Sonra kaynakları site değerlerine ekleyin:

```bash
vi platform/values/site/glue.yaml
```

Dosyada `sources:` ve `pipelines:` satırlarını bulun:

- Liste boşsa (`sources: []`, `pipelines: []`) bu iki satırı aşağıdaki blokla
  **değiştirin**.
- Listede kurumun kendi kaynakları varsa `- name: shop` ve `- name: crm` öğelerini
  `sources:` listesinin, iki `- {bronze: …}` satırını `pipelines:` listesinin sonuna ekleyin.
- `nginx:` satırı dosyada yoksa ekleyin; varsa `enabled: true` yapın.

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

Dosyayı kaydedin, denetleyin ve gönderin:

`[bastion]`

```bash
bash scripts/check-site.sh
git diff
git add platform/values/site/glue.yaml
git commit -m "kabul: demo kaynaklar"
git push origin main
```

**Beklenen çıktı** (ilk komut):

```text
check-site: OK
```

ArgoCD'nin bağlayıcıları kurması birkaç dakika sürer. Hazır olduklarını görmek için:

```bash
oc -n "$LAKEHOUSE_NS" get kafkaconnector dbz-shop sink-shop dbz-crm sink-nginx
```

Dört satırın `READY` sütunu `True` olmalıdır. Bunlar yoksa `scripts/acceptance.sh`
başlamaz ve bu bölümü gösterir.

**3) Router sertifikasının kökü.** Testlerin bir kısmı (Superset, JupyterHub ve Trino
girişleri) arayüzlere tarayıcı gibi bağlanır. Router'ın sertifikası herkesçe bilinen bir
CA'dan gelmiyorsa kökünü alın ([30-kurulum](../30-kurulum.md) §7.3'teki
`~/router-ca.crt`).

### 1.2 Kabulden sonra

`scripts/acceptance.sh` bittiğinde, koşu sırasında askıya aldığı zamanlanmış Spark
işlerini **kendisi** geri açar (özetten önce `zamanlanmış Spark işleri geri açıldı`
satırı).

Demo kaynakları kaldırmak için §1.1'de eklediğiniz satırları site değerlerinden silin
(boş kalan listeyi `sources: []` / `pipelines: []` yapın), commit edip gönderin. Sonra
test veritabanlarını silin:

`[bastion]`

```bash
oc delete -f test/e2e/pg-fixture.yaml -f test/e2e/mongo-fixture.yaml \
  -f test/e2e/nginx-fixture.yaml --ignore-not-found
```

Demo tabloları (`shop`, `crm`, `nginx_raw` ve `_raw` ad alanları) katalogda kalır;
silinmeleri [50-isletme/kaynak-veya-tablo-silme.md](../50-isletme/kaynak-veya-tablo-silme.md)'dedir.

---

## 2. Madde ↔ kanıt tablosu

Kanıt sütunundaki adlar testlerin dosya adlarıdır (`test/e2e/` klasöründe). Beklenen
sütunundaki `E2E … OK` satırları testlerin kendi yazdığı satırlardır.

| Şartname | Gereksinim | Kanıt (test / komut) | Beklenen |
|---|---|---|---|
| **a, J.2.1, J.3.2** | tümü açık kaynak; sürüm ve lisans listesi güncel | [surumler-ve-lisanslar.md](surumler-ve-lisanslar.md) + `oc -n $ARGOCD_NS get applications -o custom-columns='APP:.metadata.name,CHART:.spec.source.chart,REV:.spec.source.targetRevision'` | Tablodaki sürümler kümedekilerle aynı; lisanslar izin verici |
| **A.6** | toplama → omurga → dağıtık motor → Iceberg V2 → katalog → MPP SQL → BI zinciri | `pg-path.sh` → `trino-path.sh` → `superset-path.sh` | `E2E F2 OK`, `E2E F4 TRINO OK`, `E2E F4 SUPERSET OK`; Superset içinden `select count(*) from shop.orders` = 3 |
| **C.2.1(e)** | CDC, bağlayıcı çerçevesi (connector framework) biçiminde (MSSQL/PG/Mongo) | `pg-path.sh` (PostgreSQL) + `mongo-path.sh` (MongoDB); `oc -n $LAKEHOUSE_NS get kafkaconnector` | `dbz-shop`, `sink-shop`, `dbz-crm` hazır; `E2E F2 OK`, `E2E F3 MONGO OK`. SQL Server aynı kalıpla çalışır ([50-isletme/yeni-kaynak-ve-pipeline.md](../50-isletme/yeni-kaynak-ve-pipeline.md) §3.2) |
| **C.3** | hafif günlük ajanı doğrudan omurgaya | `nginx-path.sh` (Fluent Bit → Kafka dış adresi → `nginx.access` → Iceberg) | `E2E F3 NGINX OK`; `nginx_raw.access_log` satırları geldi |
| **D(e)** | **MERGE INTO** (ekle/güncelle + sil) | `pg-path.sh` içindeki "MERGE #1/#2/#3" adımları | Kaynakta UPDATE/DELETE sonrası Silver 2 satır (`id=1:status=shipped`, `!id=2`); sonraki INSERT'te 3 satır |
| **D(f)** | sıkıştırma, eski sürüm temizliği ve manifest bakımı **tanımla planlanmış** | `oc -n $LAKEHOUSE_NS get scheduledsparkapplication` + `pg-path.sh` "bakım" adımı | Beş zamanlı iş (`silver-merge`, `mongo-bronze`, `maint-position-deletes`, `maint-compact`, `maint-expire-orphan-ttl`); bakım koşularında `MAINT_OK` |
| **B.3.1** | Iceberg V2, **zaman yolculuğu** | `pg-path.sh` → `verify shop.orders 2 --exact --prev-rows 3` | Silme öncesi sürüm hâlâ okunabilir ve 3 satırlı |
| **H.3.1** | pipeline'lar zamanlayıcıyla | `oc -n $LAKEHOUSE_NS get scheduledsparkapplication -o custom-columns='AD:.metadata.name,CRON:.spec.schedule,SUSPEND:.spec.suspend'` | Beş iş cron'lu ve `suspend=false` (kabul koşusundan **sonra**; §5) |
| **H.1.1** | Schema Registry **isteğe bağlı** | `oc -n $LAKEHOUSE_NS get kafkaconnector dbz-shop -o jsonpath='{.spec.config}'` | JSON dönüştürücü + `schemas.enable=true`; ayrı bir şema kayıt bileşeni yok |
| **F.1** | **iki farklı** web not defteri, kullanıcıya ayrık, PyIceberg hazır | `jupyterhub-path.sh` + `zeppelin-path.sh` | `E2E F4 JUPYTERHUB OK` (pod içinde PyIceberg → katalog, Trino TLS), `E2E F4 ZEPPELIN OK` (`shop.orders` = 3); JupyterHub'da kullanıcı başına disk |
| **G.1.1, F.3.1** | bütün kullanıcı servisleri AD ile (OIDC/LDAPS) | `trino-path.sh`, `superset-path.sh`, `jupyterhub-path.sh`, `zeppelin-path.sh` | analist 3 satır görür; kullanıcı 2 satır görür ve `remote` maskelidir; grubu olmayan Zeppelin'de veri göremez |
| **G.5** | Prometheus (+ Loki) | `monitoring-path.sh` | `E2E F5 MONITORING OK`: hedefler ayakta, Kafka gecikme serileri var, **altı alarm kuralı** sağlıklı, ateşlenen alarm yok. Loki platformdadır |
| **G.6** | yedek (OADP) + katalog veritabanı sürekli arşiv (PITR) | `dr-path.sh` | `E2E F5 DR OK`: 3 veritabanında `ContinuousArchiving=True`, en az 3 tamamlanmış yedek, ayrı bir kümeye geri yükleme ve tablo sayımı, OADP yedeği ve geri yüklemesi |
| **e, f, A.4** | OCI imaj + CRD yaşam döngüsü; tanımlayıcı YAML/CRD; Operator tercihi | `oc -n $ARGOCD_NS get applications`; `oc -n $LAKEHOUSE_NS get kafka,kafkaconnect,cluster,keycloak,superset,sparkapplication` | Her bileşen bir özel kaynak (CR) ya da ArgoCD uygulaması; Connect imajı kümede, Strimzi tarafından üretilir |

---

## 3. Koşu ve çıktı

Betik AD test hesaplarıyla gerçek kullanıcı gibi giriş yapar ve izleme kanıtını kümenin
Thanos Querier'ından alır; bunun için birkaç değişken verilir. Test hesaplarının
parolaları komut satırına **yazılmaz** (kabuk geçmişine düşer); her biri bir kez sorulur.
Parolayı yazıp Enter'a basın:

`[bastion]`

```bash
cd ~/lakehouse
read -rsp "kabul-analist parolası: " ACC_ANALYST_PASS; echo
read -rsp "kabul-kullanici parolası: " ACC_USER_PASS; echo
read -rsp "kabul-grupsuz parolası: " ACC_NOGROUP_PASS; echo
export ACC_ANALYST_PASS ACC_USER_PASS ACC_NOGROUP_PASS
```

Sonra değişkenleri tanımlayıp betiği çalıştırın:

```bash
export ACC_FLOW=authcode ACC_SA=zeppelin \
  ACC_ANALYST_USER=kabul-analist ACC_USER_USER=kabul-kullanici ACC_NOGROUP_USER=kabul-grupsuz \
  ACC_CA_FILE=~/router-ca.crt GRAFANA_SKIP=1 \
  PROM_URL="https://$(oc -n openshift-monitoring get route thanos-querier -o jsonpath='{.spec.host}')" \
  PROM_TOKEN="$(oc whoami -t)"
PATH="$PWD/.venv/bin:$PATH" scripts/acceptance.sh --argocd-ns "$ARGOCD_NS" \
  --mon-ns openshift-user-workload-monitoring --velero-ns openshift-adp
```

Değişkenlerin anlamı:

| Değişken | Anlamı |
|---|---|
| `ACC_FLOW=authcode` | Testler Keycloak'a tarayıcı gibi girer (parola akışı bilerek kapalıdır) |
| `ACC_*_USER` / `ACC_*_PASS` | §1.1'deki üç AD test hesabı (kendi hesap adlarınızı yazın) |
| `ACC_SA=zeppelin` | Testlerin kullandığı Trino servis hesabı |
| `ACC_CA_FILE` | Router sertifikasının kökü ([30-kurulum](../30-kurulum.md) §7.3) |
| `PROM_URL`, `PROM_TOKEN` | İzleme sorguları Thanos Querier'a oturum jetonuyla gider; `oc` oturumunuzun `cluster-monitoring-view` yetkisi olmalıdır |
| `GRAFANA_SKIP=1` | Ürün OpenShift'te Grafana kurmaz; yalnız pano denetimleri atlanır |

OADP kurulu değilse sona `--skip dr` ekleyin; özet atlanan testi `ATLANAN:` satırında
yazar.

Betik çalışırken iki Secret yaratır (`polaris-smoke-cred`, `e2e-acceptance`) ve onları bu
bölümü gösteren bir etiketle işaretler (`lakehouse.io/yonetim=manual-kabul-testleri-3`).
Kabulden sonra kümede kalabilirler.

**Beklenen çıktı** (örnek — satırların biçimi betikten gelir; her testin ara satırları
kısaltılmıştır):

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
KABUL: 9/9 yol geçti (ns=lakehouse, mon-ns=openshift-user-workload-monitoring, velero-ns=openshift-adp, argocd-ns=openshift-gitops)
```

Iceberg yazıcısı veriyi beş dakikada bir yazdığı için Bronze beklemeleri uzundur; betik
her Bronze denetiminde 15 dakikaya kadar bekler.

**Ters giderse:**

- Bir test düşerse betik orada durur, çıkış kodu **1** olur ve özet `KABUL BAŞARISIZ`
  satırıyla biter; satır düşen testin adını yazar. Belirtiden yola çıkan teşhis
  [50-isletme/sorun-giderme.md](../50-isletme/sorun-giderme.md) §3'tedir; kabule özgü
  durumlar §5'tedir.
- Tamamlanan test sayısı dokuzun altında kalırsa özet `KABUL EKSİK` der.
- `UYARI --ns=… : e2e yol script'leri ve fixture'lar 'lakehouse' ns'ine sabitlidir`
  satırı ad alanının `lakehouse` olmadığını gösterir (§1); devam etmeyin.
- `kubectl: command not found` → `kubectl` kurulu değildir (§1).
- `polaris CLI yok` → komuttaki `PATH="$PWD/.venv/bin:$PATH"` önekini vermemişsinizdir
  ya da `~/lakehouse` klasöründe değilsiniz.

---

## 4. Ne kadar sürer

| Ne | Süre | Not |
|---|---|---|
| §1.1 hazırlığı | yarım gün | çoğu AD hesaplarının açılmasını beklemektir |
| `scripts/acceptance.sh` | 40–50 dk | çoğu Bronze beklemeleridir; izleme testi ~3 dk, yedek testi ~4 dk |

---

## 5. Kabul koşusuna özgü sık durumlar

- **Kabulü daha önce kullanılmış bir kümede yeniden koşmak** (önceki koşu yarıda kaldı ya
  da testler daha önce elle denendi): testler **taze** demo durumu bekler — kaynakta 3
  satır, boş Bronze ve Silver tabloları, konumu sıfırlanmış bağlayıcılar. Yalnız test
  veritabanını silmek **yetmez**: bağlayıcılar eski konumlarını hatırlar ve Bronze'daki
  eski satırlar sayımları bozar. Demo kaynaklarını
  [kaynak-veya-tablo-silme.md](../50-isletme/kaynak-veya-tablo-silme.md) yordamıyla bu
  sırayla sıfırlayın:
  1. `shop` ve `crm` girdilerine `state: stopped` ekleyin → commit → `STOPPED` → konumları
     silin (silme belgesi §3).
  2. §1.1'de eklediğiniz satırları kaldırın → commit; ArgoCD bağlayıcıları, `mongo-bronze`
     işini ve `nginx.access` konusunu kaldırır (silme belgesi §4).
  3. Kalan Kafka konularını ve tüketici gruplarını silin (silme belgesi §5): `shop.*`,
     `crm.*`, `__debezium-heartbeat.shop`, `__debezium-heartbeat.crm`, `nginx.dlq`;
     `connect-sink-shop`, `connect-sink-nginx` ve `-coord` ile biten eşleri.
  4. Tabloları tek seferlik Spark işiyle verisiyle silin (silme belgesi §6.2):
     `shop_raw.orders`, `shop.orders`, `crm_raw.customers`, `crm_raw.customers__quarantine`,
     `crm.customers`, `nginx_raw.access_log`.
  5. Test veritabanlarını silip yeniden kurun (§1.2'deki `oc delete -f …`, sonra §1.1'deki
     `oc apply -f …`).
  6. §1.1 satırlarını **`state` satırı olmadan** geri ekleyin → commit. Bağlayıcılar
     günlüğe `No previous offset found` ve `Snapshot completed` yazar. Sonra
     `scripts/acceptance.sh`'i çalıştırın.
- **Kabulden sonra zamanlanmış işler çalışmıyor:** betik koşu sırasında işleri askıya alır
  ve bitişte (başarılı ya da başarısız) geri açar; son satırda
  `zamanlanmış Spark işleri geri açıldı: ...` yazar. Bu satırı **görmediyseniz** (koşu
  yarıda kesildi: bilgisayar kapandı, bağlantı koptu) işler askıda kalmıştır;
  [sorun-giderme.md §6.1](../50-isletme/sorun-giderme.md#ssa-suspend) ile kontrol edip geri
  açın.
- **`monitoring-path.sh` "ateşlenen Lakehouse alarmı" ile düşüyor:** kümede ateşlenen bir
  alarm vardır (çoğu zaman eski, başarısız bir Spark koşusu). Alarmın nedenini
  [sorun-giderme.md](../50-isletme/sorun-giderme.md) §4 ile giderin, sonra betiği yeniden
  çalıştırın. Kabul, ateşlenen alarm varken bilerek geçmez.
- **`--velero-ns openshift-adp` zorunludur:** yedek testi yedekleme ve geri yükleme
  nesnelerini bu ad alanında yaratır.

---

## 6. İzleme testinin ayarları

İzleme testi (`monitoring-path.sh`), `PROM_URL` verildiğinde sorgularını kümenin Thanos
Querier'ına gönderir (§3). Thanos, platform izlemesinin `kube-state-metrics` serilerini de
gösterir; iki Spark alarmı (`LakehouseSilverMergeStale`, `LakehouseSparkScheduledRunFailed`)
bu serilere dayanır.

| Değişken | Değer |
|---|---|
| `PROM_URL` | `https://` + `thanos-querier` Route'unun adresi |
| `PROM_TOKEN` | `oc whoami -t` |
| `ACC_CA_FILE` | router kökü (`~/router-ca.crt`) |
| `GRAFANA_SKIP` | `1` |

`GRAFANA_SKIP=1` **yalnız** pano denetimlerini atlar; hedef, metrik, kural ve alarm
denetimleri çalışmaya devam eder.

**Beklenen çıktı** (kısaltılmış; parantez içi, ayakta olan hedef sayısıdır — Kafka
broker'ları ve iki Connect işçisi birlikte 6 hedeftir):

```text
OK up .*kafka-resources-metrics.* (6)
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

Müşteriye yapılan kabul **demosu** (üniversite örnek verisi, iki şartname senaryosu ve
Superset panosu) bu depoda değildir; kendi kılavuzuyla birlikte ayrı bir paket olarak
verilir. Kabulün teknik kanıtı yukarıdaki tablodur.

---

## Kontrol listesi

- [ ] §1 sağlandı: kurulum tamam, `polaris` aracı kurulu, ad alanı `lakehouse`.
- [ ] §1.1 hazırlığı yapıldı: üç AD test hesabı, demo kaynaklar, router kökü.
- [ ] §3'teki değişkenler ve bayraklar verildi.
- [ ] `KABUL: 9/9 yol geçti` satırı alındı (OADP yoksa `--skip dr` ile 8 test).
- [ ] Kabul koşusundan sonra beş zamanlı iş geri açıldı (`suspend=false`).
- [ ] Sürüm tablosu kümedeki sürümlerle karşılaştırıldı
      ([surumler-ve-lisanslar.md](surumler-ve-lisanslar.md) son bölüm).
- [ ] Kabulden sonra demo kaynaklar kaldırıldı (§1.2).

## Sonraki bölüm

[40-kurulum-sonrasi.md](../40-kurulum-sonrasi.md) §7 — kabulün kurulum akışındaki yeri.
Sürüm ve lisans kanıtı: [surumler-ve-lisanslar.md](surumler-ve-lisanslar.md).
