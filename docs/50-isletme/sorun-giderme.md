# Sorun giderme

**Bu bölümde:** kurulumdan işletmeye kadar karşılaşılan belirtilerin tek listesi — önce
bir **belirti tablosu**, sonra her belirtinin komut komut teşhisi ve çözümü. Altı alarmın
her biri buradaki kendi bölümüne bağlanır: alarm bildiriminin içindeki `runbook`
açıklaması doğrudan bu sayfanın ilgili başlığını gösterir.
**Süre:** ilk bakış 2–5 dakika; kök nedene göre değişir.
**Gereken yetki:** `$LAKEHOUSE_NS` ad alanında okuma; bazı çözümlerde aynı ad alanında
`annotate`/`patch`/`delete pod` yetkisi. Kalıcı düzeltmeler her zaman Git üzerinden
yapılır ([değişiklik nasıl uygulanır](degisiklik-nasil-uygulanir.md)).
**Nerede çalıştırılır:** `[bastion]` — `oc login` ile kümeye girilmiş yönetim makinesi.

---

## 1. Değişkenleri yükleyin

`[bastion]`

```bash
cd ~/lakehouse
set -a; . install/lakehouse.env; set +a
echo "argocd=$ARGOCD_NS lakehouse=$LAKEHOUSE_NS"
```

**Beklenen çıktı** (örnek — kendi değerlerinizle):

```text
argocd=openshift-gitops lakehouse=lakehouse
```

**Ters giderse:** boş satır görüyorsanız dosya yüklenmemiştir
([30-kurulum](../30-kurulum.md) §1).

---

## 2. İlk bakılacaklar

Belirti ne olursa olsun bu dört komut önce koşturulur; sorunların çoğu burada görünür.

`[bastion]`

```bash
oc -n "$ARGOCD_NS" get applications -o wide
oc -n "$LAKEHOUSE_NS" get pods -o wide
oc -n "$LAKEHOUSE_NS" get kafka,kafkaconnect,kafkaconnector,cluster,keycloak,superset,sparkapplication
oc -n "$LAKEHOUSE_NS" get events --sort-by=.lastTimestamp | tail -30
```

**Beklenen çıktı** (örnek — sağlıklı kurulumda): bütün uygulamalar `Synced Healthy`,
hiçbir pod `CrashLoopBackOff` / `CreateContainerConfigError` / `Pending` değil, bütün
özel kaynakların `READY` sütunu `True`.

**Ters giderse:** `applications` diye bir kaynak yoksa kurulum ArgoCD'siz "helm
modundadır" ([30-kurulum](../30-kurulum.md) "Lokal deneme (kind)"); diğer üç komut aynen
geçerlidir.

---

## 3. Belirti tablosu

| Belirti | Nereye bakılır |
|---|---|
| Alarm: `LakehouseConnectTaskFailed` | [§4.1 Connect görevi FAILED](#connect) |
| Alarm: `LakehouseSinkStalled` | [§4.2 Iceberg sink lag'i büyüyor](#sink) |
| Alarm: `LakehouseSilverMergeStale` | [§4.3 silver-merge eskidi](#silver-merge) |
| Alarm: `LakehouseSparkScheduledRunFailed` | [§4.4 Zamanlanmış Spark koşusu FAILED](#spark) |
| Alarm: `LakehouseSparkRunTooLong` | [§4.5 Spark koşuları yavaşladı](#spark-duration) |
| Alarm: `LakehouseDlqRecords` | [§4.6 Hatalı kayıt konusuna kayıt düştü](#dlq) |
| Silver güncellenmiyor, bakım işleri koşmuyor, hata da yok | [§6.1 Askıda kalan zamanlı iş](#ssa-suspend) |
| Spark ya da Connect `403 The Access Key Id you provided does not exist` | [§5.1 Polaris / S3 403](#polaris-403) |
| Spark işi `UnresolvedAddressException` ya da Ivy hatası veriyor | [§6.2 İç Maven aynası](#maven) |
| Bronze tablo boş kalıyor, satır gelmiyor | [§6 Boru hattı](#boru-hatti) |
| Karantina (`__quarantine`) tablosu doluyor | [yeni-kaynak-ve-pipeline.md](yeni-kaynak-ve-pipeline.md) §3.3 ve §9.2 |
| `nginx.dlq` konusunda kayıt birikiyor | [yeni-kaynak-ve-pipeline.md](yeni-kaynak-ve-pipeline.md) §10 |
| Silver tablosunda `SchemaConflict` | [mevcut-kaynaga-tablo-ekleme.md](mevcut-kaynaga-tablo-ekleme.md) |
| `glue` uygulaması `OutOfSync` ya da `Degraded` kalıyor | [§5 Kurulum ve eşitleme](#kurulum) |
| Connect `Build` uzun sürüyor ya da `unauthorized` veriyor | [§5 Kurulum ve eşitleme](#kurulum) |
| Trino `401` ya da `Authentication failed` | [§7 Kullanıcı yüzü](#kullanici-yuzu) |
| Not defterinden Trino'ya `CA cert does not include key usage extension` | [§7 Kullanıcı yüzü](#kullanici-yuzu) |
| Analist/kullanıcı Trino'da `Access Denied: Cannot access catalog lakehouse` (servis hesapları çalışıyor) | [§7 Kullanıcı yüzü](#kullanici-yuzu) |
| Trino `Access Denied` | [kullanici-ve-yetki.md](kullanici-ve-yetki.md) §5 |
| Superset `phase: Initializing`, şema geçiş görevi düşüyor | [§7 Kullanıcı yüzü](#kullanici-yuzu) |
| JupyterHub not defteri açılmıyor (spawn zaman aşımı) | [§7 Kullanıcı yüzü](#kullanici-yuzu) |
| Zeppelin `jdbc` yorumlayıcısı hazır değil | [§7 Kullanıcı yüzü](#kullanici-yuzu) |
| Zeppelin girişi başarılı ama not defteri listesi 401 | [kullanici-ve-yetki.md](kullanici-ve-yetki.md) §3 |
| Prometheus hedefi `down`, metrik yok | [§8 İzleme ve yedekler](#dr) |
| CNPG `ContinuousArchiving=False` | [§8 İzleme ve yedekler](#dr) |
| Velero yedeği `PartiallyFailed` | [§8 İzleme ve yedekler](#dr) |
| `oc get backup` yanlış nesneyi gösteriyor | [§8 İzleme ve yedekler](#dr) |
| Pod öldü, günlüğüne artık bakılamıyor | [§9 Günlükler (Loki)](#loki) |

---

## 4. Alarmlar

Kümede **tek** `PrometheusRule` vardır (`lakehouse`) ve içinde **beş** kural bulunur.
Alarmların anlamı, eşiği ve nereden ayarlandığı
[izleme-ve-alarmlar.md](izleme-ve-alarmlar.md) sayfasındadır; burada yalnız "ateşlendi, ne
yapmalı" adımları vardır.

Kuralların o anki durumu:

`[bastion]`

```bash
TOKEN=$(oc whoami -t)
HOST=$(oc -n openshift-monitoring get route thanos-querier -o jsonpath='{.spec.host}')
curl -sSk -H "Authorization: Bearer $TOKEN" "https://$HOST/api/v1/rules" \
  | jq -r '.data.groups[] | select(.name=="lakehouse") | .rules[] | "\(.name)\t\(.health)\t\(.state)"'
```

**Beklenen çıktı** (kind kümesindeki Prometheus'tan alınmış gerçek çıktı; OpenShift'te
aynı satırlar thanos-querier üzerinden gelir):

```text
LakehouseConnectTaskFailed	ok	inactive
LakehouseSinkStalled	ok	inactive
LakehouseSilverMergeStale	ok	inactive
LakehouseSparkScheduledRunFailed	ok	inactive
LakehouseSparkRunTooLong	ok	inactive
LakehouseDlqRecords	ok	inactive
```

**Ters giderse:** `health` sütunu `err` ise kural ifadesi değerlendirilemiyordur (çoğu
zaman metrik kaynağı eksiktir — [§8](#dr)). Liste boşsa kullanıcı iş yükü izlemesi kapalı
ya da kural nesnesi kümede yoktur ([20-on-kosullar](../20-on-kosullar.md) madde 9.1).

<a id="connect"></a>
### 4.1 `LakehouseConnectTaskFailed` — Connect görevi FAILED

`kafka_connect_connector_task_status{status="failed"} == 1`, 5 dakika, **critical**.

**Ne bak**

`[bastion]`

```bash
CONNECTOR=dbz-shop        # alarmın özet satırındaki bağlayıcı adı
oc -n "$LAKEHOUSE_NS" get kafkaconnector
oc -n "$LAKEHOUSE_NS" get kafkaconnector "$CONNECTOR" \
  -o jsonpath='{.status.connectorStatus.tasks[0].trace}'
oc -n "$LAKEHOUSE_NS" logs -l strimzi.io/kind=KafkaConnect --tail=200
```

**Beklenen çıktı** (kind kümesinde alınmış gerçek çıktı — ilk komut, sağlıklı hâl; alarm
sırasında ilgili satırın `READY` sütunu `False` olur):

```text
NAME         CLUSTER   CONNECTOR CLASS                                      MAX TASKS   READY
dbz-crm      connect   io.debezium.connector.mongodb.MongoDbConnector       1           True
dbz-shop     connect   io.debezium.connector.postgresql.PostgresConnector   1           True
sink-nginx   connect   org.apache.iceberg.connect.IcebergSinkConnector      1           True
sink-shop    connect   org.apache.iceberg.connect.IcebergSinkConnector      1           True
```

**Ne yap**

- **Geçici hata** (kaynak veritabanı yeniden başladı, ağ koptu): görevi yeniden başlatın.

  `[bastion]`

  ```bash
  oc -n "$LAKEHOUSE_NS" annotate kafkaconnector dbz-shop strimzi.io/restart=true
  ```

- **Kimlik ya da yetki hatası:** kaynağın Secret'ı (kaynak adı + `-db`; depodaki örnekte
  `erp-db`) ve kaynak hazırlığı —
  [yeni-kaynak-ve-pipeline.md](yeni-kaynak-ve-pipeline.md) Adım 3 (çoğaltma rolü,
  publication, sinyal tablosu, CDC etkinleştirme) ile Adım 4 (Secret).
- **Converter ya da SMT hatası:** hatalı kayıtlara katlanma açık olduğu için kayıt, kaynağın
  konu önekiyle başlayan DLQ konusuna düşer (`shop` kaynağı için `shop.dlq`); oradan okuyun.
- **Görev düştükten sonra tüketici konumu ileri kalmış olabilir** (veri boşluğu).
  Bağlayıcıyı `spec.state: stopped` yapıp tüketici grubunun konumunu başa alın, sonra
  `running` durumuna döndürün; grup adı `connect-` öneki + sink bağlayıcısının adıdır
  (`connect-sink-shop`).

**Ters giderse:** `trace` boşsa görev hiç başlamamıştır — Connect pod'unun günlüğüne bakın;
imaj yapımı yarım kalmış olabilir ([§5](#kurulum)).

<a id="sink"></a>
### 4.2 `LakehouseSinkStalled` — Iceberg sink lag'i büyüyor

`sum by (consumergroup, topic) (kafka_consumergroup_lag{consumergroup=~"connect-sink-.*"})`
eşiği aştı; 15 dakika, **warning**. Eşik: `monitoring.sinkLagThreshold` (varsayılan 1000).

**Ne bak**

`[bastion]`

```bash
scripts/kafka-cli.sh kafka-consumer-groups.sh --describe --group connect-sink-shop   # kafka-admin kimliğiyle, TLS 9093
oc -n "$LAKEHOUSE_NS" get kafkaconnector sink-shop \
  -o jsonpath='{.status.connectorStatus.tasks[0]}'
oc -n "$LAKEHOUSE_NS" logs -l strimzi.io/kind=KafkaConnect --tail=200 | grep -i 'commit\|iceberg\|s3'
```

**Önce `LAG` sütununa bakın — teşhisi o belirler:**

| `LAG` | Anlamı | Ne yap |
|---|---|---|
| `0` | Boru hattı **sağlıklı ve boşta**: tüketilecek yeni kayıt yok | Bir şey yapmayın; aşağıdaki nota bakın |
| `> 0` ama her ölçümde **azalıyor** | Birikim eriyor | Bekleyin; kesinti sonrası normaldir |
| `> 0` ve **sabit** (iki ölçüm arası değişmiyor) | Sink tüketiyor ama **commit edemiyor** | Aşağıdaki "Bronze tablosu var ama boş" akışı |

> **`committed to 0 table(s)` bir arıza belirtisi DEĞİLDİR.** Sink koordinatörü
> `iceberg.control.commit.interval-ms` (geliştirmede 30 sn, üretimde 300 sn) periyoduyla
> **her zaman** bir commit turu başlatır; o turda yazılacak yeni veri yoksa günlüğe
> `Coordinator … completed commit …, committed to 0 table(s)` ve `Commit timeout reached`
> satırlarını yazar. Boşta duran sağlıklı bir kurulumda bu satırlar **sürekli** akar.
> Ölçüt satırın kendisi değil, **`LAG` ile birlikte** okunmasıdır.

**Ne yap**

- Görev `RUNNING` ama commit yoksa: katalog ya da S3 erişimi bozuktur →
  [§5.1 Polaris / S3 403](#polaris-403).
- Gerçek yük artışıysa kaynağın `sinkTasks` değeri ve `connect.replicas` artırılır (sink'in
  commit aralığı üretimde 300 saniyedir, ilk birikme normaldir).
- Uzun kesintiden sonraki birikimde lag'in **düştüğünü** doğrulamak yeter; azalıyorsa
  beklenir.
- `connect-sink-shop-coord` biçimindeki gruplar sink'in kendi denetim konusunu okur ve bu
  kurala **dâhildir**; kalıcı `-coord` lag'i koordinatörün takıldığını gösterir →
  bağlayıcıyı yeniden başlatın.

**Ters giderse:** komut `lakehouse-dual-role-0` pod'unu bulamazsa pod adları farklıdır;
`oc -n "$LAKEHOUSE_NS" get pods -l strimzi.io/cluster=lakehouse` ile listeleyin.

<a id="sink-bos-tablo"></a>
#### Bronze tablosu var ama boş (`LAG` sabit, snapshot yok)

Belirti: yeni tablonun Bronze karşılığı **yaratılmış**, S3'te veri dosyaları bile var, ama
`select count(*)` `0` döndürüyor ve tüketici gecikmesi küçük bir sayıda (ör. `2`) **donmuş**.
Nedeni şudur: Iceberg sink tüketici offset'lerini **ancak Iceberg commit'i başarılı olursa**
işler. Commit tabloyu kapsamadığı sürece offset ilerlemez — bu yüzden `LAG` sonsuza kadar
aynı sayıda kalır ve yazılan parquet dosyaları hiçbir snapshot'a girmez.

`strimzi.io/restart=true` ve Connect pod'unu yeniden başlatmak bu durumu **çözmez**;
sıradaki adımlar şunlardır — **sırayla**:

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" get kafkaconnector sink-shop \
  -o jsonpath='{range .status.connectorStatus.tasks[*]}{.state}{"\n"}{.trace}{"\n"}{end}'
oc -n "$LAKEHOUSE_NS" logs -l strimzi.io/kind=KafkaConnect --tail=500 \
  | grep -iE "NoSuchTable|Forbidden|403|AccessDenied|schema|Exception"
scripts/kafka-cli.sh kafka-consumer-groups.sh --describe --group connect-sink-shop-coord   # kafka-admin kimliğiyle, TLS 9093
```

**Beklenen çıktı:** birinci komut `RUNNING` ve **boş** bir `trace` verir (görev ayakta;
hata görev düzeyinde değildir). İkinci komut **sessizse** sorun yetki/şema değil,
koordinatörün commit turudur. Üçüncü komutun `LAG` sütunu sürekli büyüyorsa koordinatör
denetim konusunu okuyamıyordur.

**Sıradaki adım — sebebe göre:**

- İkinci komut `NoSuchTableException` / `403` / `AccessDenied` bastıysa: sink'in Polaris
  principal'ı **yeni Bronze ad alanında tablo yaratamıyordur** →
  [§5.1 Polaris / S3 403](#polaris-403). En sık neden, kaynağın Bronze ad alanının
  Polaris'te hiç açılmamış olmasıdır
  ([yeni-kaynak-ve-pipeline.md](yeni-kaynak-ve-pipeline.md) §5).
- İkinci komut şema hatası bastıysa (`evolve-schema`): Bronze tablosu kaynağın yeni
  kolonlarıyla uyuşmuyordur; tabloyu düşürüp sink'in yeniden yaratmasına izin verin
  ([kaynak-veya-tablo-silme.md](kaynak-veya-tablo-silme.md) §6.2).
- Üçüncü komutun `-coord` lag'i büyüyorsa: koordinatör takılmıştır → Connect pod'unu
  tamamen yeniden yaratın (`oc -n "$LAKEHOUSE_NS" delete pod connect-connect-0`) ve lag'i
  yeniden ölçün.
- Üçü de temizse ve `LAG` hâlâ sabitse: bu, **ürün düzeyinde açık bir kusurdur**. Kanıt
  paketini (üç komutun çıktısı + `oc get kafkaconnector sink-shop -o yaml`) toplayıp
  [pre-ship kontrol listesine](../90-referans/pre-ship-kontrol-listesi.md) yazın. Veriyi
  kurtarmak için Bronze tablosunu düşürüp kaynağı yeniden snapshot'layın
  ([mevcut-kaynaga-tablo-ekleme.md](mevcut-kaynaga-tablo-ekleme.md) §6).

<a id="silver-merge"></a>
### 4.3 `LakehouseSilverMergeStale` — silver-merge eskidi

En son **başarılı** `silver-merge` koşusunun üzerinden eşikten uzun zaman geçti; 10 dakika,
**warning**. Eşik `monitoring.silverMergeStaleSeconds` (üretimde 2700 saniye = 45 dakika).

**Ne bak**

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" get scheduledsparkapplication silver-merge \
  -o jsonpath='suspend={.spec.suspend} sonKosu={.status.lastRun} sonraki={.status.nextRun}{"\n"}'
oc -n "$LAKEHOUSE_NS" get sparkapplication | grep silver-merge
```

**Beklenen çıktı** (kind kümesinde alınmış gerçek çıktı — işler kabul koşusundan sonra
askıda kaldığı için `suspend=true`. Sağlıklı kurulumda `suspend` **boş ya da `false`**
olmalıdır: ürün bu alanı hiç yazmaz, taze kurulumda boş basar; `false` ancak elle geri
açmadan sonra görünür. `sonKosu` da dolu olmalıdır):

```text
suspend=true sonKosu=null sonraki=2026-09-19T03:00:00Z
```

**Ne yap**

- `suspend=true` ise iş askıya alınmıştır → [§6.1](#ssa-suspend). Gecikmiş bu alarmın
  **en sık nedeni** budur. (Boş ya da `false` normaldir; ürün bu alanı hiç yazmaz.)
- Son koşu `FAILED` ise → [§4.4](#spark).
- Kural boş veride **ateşlenmez**: hiç `silver-merge-*` koşusu yoksa alarm gelmez. "Hiç
  koşmuyor" durumu ayrı bakılır (`sonraki` alanı boşsa cron hesaplanmamıştır).
- Kural, son başarılı koşunun sürücü pod'unun yaşına bakar. Koşular 24 saat sonra
  kendiliğinden silinir; iş bir günden uzun durursa alarm bu yüzden **söner** — sorun
  geçmiş sayılmaz, yukarıdaki komutla bakın.

**Ters giderse:** cron değiştirildiği hâlde `sonraki` alanı eski kalıyorsa zamanlayıcı
yeniden hesaplamamıştır ([§6.1](#ssa-suspend) sonundaki not).

<a id="spark"></a>
### 4.4 `LakehouseSparkScheduledRunFailed` — zamanlanmış Spark koşusu FAILED

Kural **yalnız ürünün kendi zamanlı işlerini** izler: `silver-merge`, `maint-` ile başlayan
bakım işleri ve `mongo-bronze`. Kendi yazdığınız Spark uygulamaları bu alarma **girmez**
([yeni-spark-uygulamasi.md](yeni-spark-uygulamasi.md)). 1 dakika, **warning**.

Alarm **iş başına bir tanedir**: aynı işin birden çok koşusu başarısız olsa da tek alarm
gelir; özet işin adını ve başarısız koşu sayısını gösterir, örneğin
`Zamanlanmış Spark işi başarısız: maint-expire-orphan-ttl (1 koşu)`.

**Ne bak**

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" get sparkapplication | grep -E 'NAME|FAILED'
APP=silver-merge-1758700000        # yukarıdaki listede FAILED görünen ad
oc -n "$LAKEHOUSE_NS" describe sparkapplication "$APP" | tail -30
oc -n "$LAKEHOUSE_NS" logs "$APP-driver" --tail=200
```

**Ne yap** — sık kökler:

| Belirti | Kök | Çözüm |
|---|---|---|
| `UnresolvedAddressException` / Ivy hatası | Maven Central'a çıkış yok | [§6.2 İç Maven aynası](#maven) |
| `SchemaConflict` (silver-merge) | Silver kolon tipi güvenli genişletilemiyor | Elle `ALTER TABLE` ya da yeni kolon ([mevcut-kaynaga-tablo-ekleme.md](mevcut-kaynaga-tablo-ekleme.md)) |
| `KAFKA_JAAS` yok (mongo-bronze) | `spark` adlı Kafka kullanıcısı yalnız MongoDB kaynağı tanımlıyken oluşur | Kaynağı ekleyin ya da işi kapatın |
| Bellek hatası / uzun çöp toplama (mongo-bronze, uzun kesinti sonrası) | Birikmiş kayıtlar sürücüye sığmıyor | Tek koşuluk `spark.driver.memory` artırın ya da Bronze tablosundaki Kafka konum özelliğini elle ilerletin (konum yalnız başarıda ilerler) |
| Sürücü `CreateContainerConfigError` | `polaris-spark` Secret'ı yok | `scripts/polaris-setup.sh` koşmamıştır ([40-kurulum-sonrasi](../40-kurulum-sonrasi.md) §2) |
| Sürücü 20+ dakika `Pending` | Düğümde CPU/bellek isteği karşılanmıyor | `spark.driver.coreRequest` / `spark.executor.coreRequest`; düğüm kapasitesi |
| `403 The Access Key Id you provided does not exist` | Geçici S3 kimliğinin süresi/önbelleği | [§5.1 Polaris / S3 403](#polaris-403) |

**Alarm, başarısız koşu silinene kadar açık kalır.** Ürün koşuları 24 saat sonra kendiliğinden
siler; daha erken susturmak için, kök neden giderildikten sonra nesneyi silin (sürücü pod'u
ve günlüğü de silinir — günlüğü önce alın):

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" delete sparkapplication silver-merge-1758700000
```

**Ters giderse:** sürücü pod'u çoktan silinmişse günlüğe yalnız Loki üzerinden bakılabilir
([§9](#loki)).

<a id="spark-duration"></a>
### 4.5 `LakehouseSparkRunTooLong` — Spark koşuları yavaşladı

Son 6 saatte tamamlanan Spark koşularının **ortalama** süresi eşiği aştı; 10 dakika,
**warning**; eşik `monitoring.sparkRunMaxSeconds` (varsayılan 1800 saniye).

**Kural küme geneldir, tek bir işe ait değildir.** spark-operator'ün ölçüm yayını iş başına
etiket üretmez; bu yüzden kural "son 6 saatteki bütün koşuların ortalaması" olarak
yazılmıştır.

**Ne bak**

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" get sparkapplication -o custom-columns='AD:.metadata.name,DURUM:.status.applicationState.state,BASLANGIC:.status.lastSubmissionAttemptTime,BITIS:.status.terminationTime'
oc -n "$LAKEHOUSE_NS" logs -l spark-role=driver --tail=200 \
  | grep -E 'incremental|FALLBACK|full|MERGE_OK|MAINT_OK'
```

**Ne yap**

- `FALLBACK full` satırı görünüyorsa artımlı okuma penceresi kaçmıştır (uzun kesinti); bir
  sonraki koşuda normale döner. Sürekli tekrar ediyorsa boru hattı tanımlarını ve bakım
  işlerinin son koşularını inceleyin.
- Veri büyüdüyse boyutlandırmayı yükseltin: `platform/values/site/glue.yaml` içindeki
  `spark` bloğu ([10-planlama](../10-planlama.md) §2, büyük kademe).
- Bakım işleri veri hacmiyle birlikte büyür; eşiği gerçekçi bir değere çekmek de meşru bir
  çözümdür ([izleme-ve-alarmlar.md](izleme-ve-alarmlar.md) §4).

**Ters giderse:** hiç koşu yokken kural **ateşlenmez** (ortalama hesaplanamaz); "hiç
koşmuyor" durumu [§4.3](#silver-merge) ile bakılır.

---

<a id="dlq"></a>
### 4.6 `LakehouseDlqRecords` — hatalı kayıt konusuna kayıt düştü

Bir sink'in işleyemediği kayıtlar (JSON olmayan, veri sözleşmesine uymayan) kaynağın ya da
akışın hatalı kayıt konusuna (adı `.dlq` ile biter) gider ve **tabloya girmez**. Son bir saatte
böyle bir kayıt geldiyse alarm konunun adıyla uyarır; 1 dakika, **warning**. Yeni kayıt
gelmezse bir saat sonra kendiliğinden kapanır.

**Ne bak** — alarmdaki konuyu yazın (örnekte `sensors.dlq`):

`[bastion]`

```bash
scripts/kafka-cli.sh kafka-get-offsets.sh --topic sensors.dlq
scripts/kafka-cli.sh kafka-console-consumer.sh --topic sensors.dlq --from-beginning \
  --max-messages 10 --timeout-ms 15000
```

**Beklenen çıktı** (OpenShift provasından gerçek çıktı — ilk satır konudaki kayıt sayısı,
ardından kayıtların kendisi):

```text
sensors.dlq:0:3
bu JSON değil
bu JSON değil
{"device_id": "sensor-9", "room": "C-301", "temperature_c": "yine bozuk", "humidity_pct": 40}
```

**Ne yap**

- **JSON akışı** (akış adı + `.dlq`, örneğin `sensors.dlq`): kaydı gönderen uygulama ya da
  cihaz hatalı veri üretiyordur; kayıtları uygulama ekibine iletin. Alan tipi bilerek
  değiştiyse veri sözleşmesini güncelleyin ([json-akisi-ekleme.md](json-akisi-ekleme.md) Adım 7).
- **nginx** (`nginx.dlq`): ajan JSON olmayan bir ileti göndermiştir; ajanın günlüğüne bakın
  ([yeni-kaynak-ve-pipeline.md](yeni-kaynak-ve-pipeline.md) Adım 10.4).
- **Veritabanı kaynağı** (kaynak adı + `.dlq`, örneğin `erp.dlq`): bağlayıcı olağan dışı
  bir kayıt üretmiştir; kaydı ve Connect günlüğünü ([§4.1](#connect)) inceleyin.

Hatalı kayıt konusundaki kayıtlar konunun saklama süresi (7 gün) dolunca kendiliğinden
silinir; elle temizlemek gerekmez.

<a id="kurulum"></a>
## 5. Kurulum ve eşitleme

- **Connect imaj yapımı uzun ya da başarısız.** Yapım normalde ~10 dakika sürer.

  `[bastion]`

  ```bash
  oc -n "$LAKEHOUSE_NS" logs -l strimzi.io/kind=KafkaConnect --tail=100
  ```

  `unauthorized` görüyorsanız `connect-push` Secret'ındaki jeton dolmuştur ya da yanlıştır
  ([yukseltme.md](yukseltme.md) §6).

- **PVC eşitleme kilidi (yalnız ArgoCD yolunda).** `glue` uygulaması uzun süre
  `OutOfSync/Progressing` kalır ve günlükte bir `PersistentVolumeClaim` için "sağlıklı
  olmasını bekliyorum" satırı görünür. Neden: StorageClass diski **ilk tüketicide**
  bağlıyorsa, diski bağlayacak pod sonraki dalgadadır → disk `Bound` olmaz → dalga
  ilerlemez. Kural: geç dalgada tüketilen her disk, tüketicisiyle **aynı** dalgada
  olmalıdır. Teşhis:

  `[bastion]`

  ```bash
  oc -n "$ARGOCD_NS" get app glue -o json \
    | jq -r '.status.resources[]? | select((.status!="Synced") or ((.health.status // "Healthy")!="Healthy")) | "\(.kind)/\(.name) sync=\(.status) health=\(.health.status // "-")"'
  oc -n "$LAKEHOUSE_NS" get pvc
  ```

- **İmaj çekme süreleri (taze düğüm, ilk kurulum).** Zeppelin imajı ~2,7 GB, not defteri
  imajı ~1,94 GB, Connect yapımı ~10 dakikadır; ilk kurulumda "pod sağlıklı ama dağıtım
  zaman aşımına uğradı" görmek bu sınıftandır. Ürün bekleme sürelerini buna göre uzatır;
  beklemek doğru davranıştır.

- **`polaris-bootstrap` işi deneme hakkını tüketti.** Taze kümede PostgreSQL 4–5 dakikada
  hazır olur; deneme hakkı bunun içindir. Tükenmişse işi silip eşitlemeyi tekrarlayın (iş
  yeniden koşmaya elverişlidir).

  `[bastion]`

  ```bash
  oc -n "$LAKEHOUSE_NS" delete job polaris-bootstrap
  oc -n "$ARGOCD_NS" patch application glue --type merge -p '{"operation":{"sync":{}}}'
  ```

- **Ağ politikaları.** Üretimde açıktır; listede olmayan bir ad alanından gelen istek
  sessizce zaman aşımına uğrar. İzinli platform ad alanları
  [90-referans/port-ve-servisler.md](../90-referans/port-ve-servisler.md) §5'tedir.
  Bir bağlantı sorununun ağ politikasından mı geldiğini **ayırt etmek** için site
  değerlerinde `networkPolicy.enabled: false` ile geçici olarak kapatılabilir; bu **yalnız
  teşhis içindir**, üretimde açık kalır ve nedeni bulunur bulunmaz geri açılır
  ([90-referans/pre-ship-kontrol-listesi.md](../90-referans/pre-ship-kontrol-listesi.md)
  madde 1.7).

- **Polaris.** Sağlık ucu `:8182/q/health`, API `:8181`; kurulum günlüğü
  `oc -n "$LAKEHOUSE_NS" logs job/polaris-bootstrap`.

- **Pod hiç oluşmuyor: `unable to validate against any security context constraint`.**
  OpenShift, sabit bir kullanıcı numarası (UID) isteyen pod'ları reddeder. Ürünün değer
  dosyaları bütün bileşenlerde bunu kapatır; bu hatayı görüyorsanız ya ürün dosyaları
  (`platform/values/*.yaml`, `platform/apps/*.yaml`) değiştirilmiştir ya da sonradan
  eklenen bir chart sabit UID istiyordur. Hangi değerin reddedildiğini olaylarda görürsünüz:

  `[bastion]`

  ```bash
  oc get events -A --field-selector reason=FailedCreate | grep -i "security context"
  ```

  `runAsUser: Invalid value: 1000: must be in the ranges` gibi bir satır, o chart'ın
  `runAsUser`/`runAsGroup`/`fsGroup` değerinin `null` yapılması gerektiğini gösterir.

<a id="polaris-403"></a>
### 5.1 Polaris / S3 403 (geçici kimlikler)

Belirti: Spark ya da Connect günlüğünde
`ForbiddenException: The Access Key Id you provided does not exist in our records` (403).

- **Üretimde S3 tarafında geçici kimlik servisi yoksa bu sınıf hata hiç görülmez** — kimlik
  doğrudan `s3-creds` Secret'ından gelir (`s3.vendedCredentials: false`).
- Geçici kimlik dağıtımı açıkken kök neden, Polaris'in kimlik önbelleği ile kimliğin ömrü
  arasındaki penceredir. Hızlı çözüm Polaris'i yeniden başlatmaktır:

  `[bastion]`

  ```bash
  oc -n "$LAKEHOUSE_NS" rollout restart deploy/polaris
  ```

- Kalıcı seçenek: `platform/values/site/glue.yaml` → `s3.vendedCredentials: false` ve
  `platform/polaris/setup.yaml` içinde aynı kararın karşılığı
  ([30-kurulum](../30-kurulum.md) §4.3).
- **Karıştırmayın:** `DROP TABLE` sırasındaki "veriyle birlikte silme kapalı" 403'ü bir
  katalog özelliği eksikliğidir, kimlik sorunu değildir
  ([kaynak-veya-tablo-silme.md](kaynak-veya-tablo-silme.md) Adım 7).

---

<a id="boru-hatti"></a>
## 6. Boru hattı (Bronze / Silver / nginx)

- **Bronze boş kalıyor:** bağlayıcı `READY True` mi, konu oluşmuş mu, sink görevinin `trace`
  alanı ne diyor. İlk commit 5 dakikaya kadar sürebilir.
- **`silver-merge` `SchemaConflict`:** Silver kolon tipi güvenli genişletilemiyor → elle
  `ALTER TABLE` ya da yeni kolon.
- **`mongo-bronze` `KAFKA_JAAS` yok:** `spark` adlı Kafka kullanıcısı yalnız MongoDB kaynağı
  tanımlıyken oluşur.
- **`mongo-bronze` bellek hatası:** uzun kesinti sonrası birikim sürücüye sığmıyor.
- **Karantina tablosu doluyor:** MongoDB Bronze'unda bozuk zarf, anahtarsız ya da zaman
  damgasız kayıtlar `__quarantine` tablosuna yazılır; `reason` sütunu nedeni söyler. Ayrıntı
  ve temizleme: [yeni-kaynak-ve-pipeline.md](yeni-kaynak-ve-pipeline.md) §3.3 ve §9.2.
- **nginx:** `nginx.dlq` konusu doluysa zaman damgası dönüşümü başarısız olmuştur (ajanın
  ayrıştırma ifadesi kurumun günlük biçimine uymuyordur) —
  [yeni-kaynak-ve-pipeline.md](yeni-kaynak-ve-pipeline.md) §10.
- **DLQ gerçeği:** Iceberg sink hatalı kaydı DLQ'ya **yazmaz**; DLQ yalnız converter/SMT
  hatalarını alır. Yazma hatası görevi durdurur — [§4.1](#connect) kuralının `critical`
  olmasının nedeni budur.

<a id="ssa-suspend"></a>
### 6.1 Askıda kalan zamanlı Spark işi

**Ne demek?** Silver güncellemesi (`silver-merge`), MongoDB aktarımı (`mongo-bronze`) ve üç
bakım işi (`maint-...`) kümede **zamanlı Spark işi** olarak tanımlıdır; her biri kendi
saatinde kendiliğinden koşar. Her işin bir **askı** anahtarı vardır (`spec.suspend`).
Anahtar `true` olursa iş **hiç koşmaz**: hata vermez, pod açmaz, alarm da hemen gelmez.
Veri sessizce eskir.

**Neden askıya alınır?** Kabul testi (`scripts/acceptance.sh`) bir işi o anda elle bir kez
koşturmak için önce zamanlı hâlini askıya alır; çakışma olmasın diye. Betik bitince
(başarılı ya da başarısız) hepsini **kendisi geri açar**. Ama betik yarıda **öldürülürse**
(bilgisayar kapandı, bağlantı koptu, terminal kapatıldı) geri açma adımı çalışmaz ve işler
askıda kalır. ArgoCD bunu **düzeltmez**: askı anahtarı Git'teki tanımda yer almadığı için
ArgoCD bu farkı görmez.

**Kontrol:**

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" get scheduledsparkapplication \
  -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.spec.suspend}{"\t"}{.status.lastRun}{"\n"}{end}'
```

**Beklenen çıktı** (OpenShift provasında alınmış gerçek çıktı; sütunlar: ad, askı, son
koşu). İkinci sütun **boş ya da `false`** olmalıdır; `true` gören satır askıdadır.
`null` son koşu, işin henüz saatinin gelmediğini gösterir (örneğin günde bir koşan iş):

```text
maint-compact	false	null
maint-expire-orphan-ttl	false	null
maint-position-deletes	false	2026-11-08T16:05:05Z
mongo-bronze		2026-11-08T16:35:00Z
silver-merge	false	2026-11-08T16:30:00Z
```

**Geri açma:** aşağıdaki komut kümedeki **bütün** zamanlı işlerin askısını kaldırır (kendi
eklediğiniz işler dahil; kabul testinin geçici `e2e-` işleri hariç):

`[bastion]`

```bash
for s in $(oc -n "$LAKEHOUSE_NS" get scheduledsparkapplication -o name); do
  case "$s" in */e2e-*) continue ;; esac
  oc -n "$LAKEHOUSE_NS" patch "$s" --type=merge -p '{"spec":{"suspend":false}}'
done
```

**Beklenen çıktı** (OpenShift provasında alınmış gerçek çıktı; her iş için bir satır.
Zaten açık olan iş `(no change)` basar, bu da doğrudur):

```text
scheduledsparkapplication.sparkoperator.k8s.io/maint-compact patched (no change)
scheduledsparkapplication.sparkoperator.k8s.io/maint-expire-orphan-ttl patched (no change)
scheduledsparkapplication.sparkoperator.k8s.io/maint-position-deletes patched (no change)
scheduledsparkapplication.sparkoperator.k8s.io/mongo-bronze patched
scheduledsparkapplication.sparkoperator.k8s.io/silver-merge patched (no change)
```

Askıdan çıkan iş, kaçırdığı koşuyu **hemen** yapar (provada geri açmadan birkaç saniye
sonra `silver-merge` koştu); bir sonraki saati beklemek gerekmez. Ardından kontrol
komutunu tekrar çalıştırıp ikinci sütunun `false` olduğunu doğrulayın.

**Ters giderse:** cron değerini değiştirmek bir sonraki koşu zamanını yeniden hesaplatmaz;
cron'u değiştirdikten sonra işin hemen koşması bekleniyorsa nesneyi silip yeniden yaratın
(GitOps'ta: değişikliği itip eşitlemeyi zorlayın).

<a id="maven"></a>
### 6.2 İç Maven aynası (kapalı ya da kısıtlı ağ)

Spark işleri **her koşuda** Iceberg çalışma zamanını ve AWS paketini Maven Central'dan
çözer. Dışarı erişim yoksa işler `UnresolvedAddressException` ya da Ivy hatasıyla `FAILED`
olur. Çözüm, iç ayna tanımını değerlere koymaktır: `platform/values/site/glue.yaml`
dosyasının sonundaki yorumlu `spark.ivySettingsXml` bloğunu açın ve aynanın adresini kendi
sunucunuzunkiyle değiştirin
([90-referans/values-anahtarlari.md](../90-referans/values-anahtarlari.md) §1). Anahtar
boşken (varsayılan) hiçbir ek nesne üretilmez ve Maven Central kullanılır.

Doluysa `glue` bir ConfigMap üretir, bütün Spark işlerine bağlar ve Spark'ın ayar yolunu ona
çevirir:

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" get configmap spark-ivysettings -o jsonpath='{.data.ivysettings\.xml}' | head
oc -n "$LAKEHOUSE_NS" get scheduledsparkapplication silver-merge \
  -o jsonpath='{.spec.template.sparkConf.spark\.jars\.ivySettings}{"\n"}'
```

**Beklenen çıktı** (örnek — ikinci komut, ayna açıkken):

```text
/opt/ivy/ivysettings.xml
```

Aynı sınıftan diğer dış erişim ihtiyaçları: Zeppelin'in Trino JDBC sürücüsünü indirmesi
(Maven), JupyterHub'ın ek paket kurulumu ve dbt örneği (PyPI). Kapalı ağda iç PyPI aynası da
gerekir ([20-on-kosullar](../20-on-kosullar.md) madde 10).

**Ters giderse:** ConfigMap yoksa anahtar hâlâ boştur; eşitlemenin tamamlandığını
doğrulayın.

---

<a id="kullanici-yuzu"></a>
## 7. Kullanıcı yüzü (Trino, Superset, JupyterHub, Zeppelin)

- **Trino pod'u `CreateContainerConfigError`:** `polaris-trino` Secret'ı yok →
  `scripts/polaris-setup.sh` koşmamıştır ([40-kurulum-sonrasi](../40-kurulum-sonrasi.md) §2).
- **Trino `401` / `Authentication failed`:** 8080 portunda kimlik doğrulama **yoktur**
  (yalnız iç trafik ve sağlık yoklaması); istemciler **8443 HTTPS** kullanmalı ve
  `lakehouse-ca`'ya güvenmelidir. Jeton reddediliyorsa jetonun hedef kitlesinde `trino`
  yoktur — realm'in `trino` istemcisindeki eşleyiciye bakın
  ([30-kurulum](../30-kurulum.md) "Realm içeriğini sonradan değiştirmek").
- **Trino `Access Denied`:** erişim kuralları **ilk eşleşen kurala** bakar; kullanıcının
  grubu görünmüyorsa grup sağlayıcısı devrede değildir —
  [kullanici-ve-yetki.md](kullanici-ve-yetki.md) §5.
- **Superset `phase: Initializing`, şema geçiş görevi düşüyor:** geçiş işi veritabanı hazır
  olmadan koşmuştur. Ürün bunu yüksek deneme sayısıyla karşılar; kalıcı düştüyse CR'ı
  yeniden tetikleyin (`spec.lifecycle.migrate.trigger` değerini değiştirin). Teşhis:

  `[bastion]`

  ```bash
  oc -n "$LAKEHOUSE_NS" describe superset superset | tail -30
  oc -n "$LAKEHOUSE_NS" logs job/superset-migrate --tail=30
  ```

- **`superset-migrate` artık pod'ları:** yeniden denemeler `Error`/`Completed` pod'ları
  bırakır, zararsızdır. Temizlik:

  `[bastion]`

  ```bash
  oc -n "$LAKEHOUSE_NS" delete pod -l job-name=superset-migrate \
    --field-selector=status.phase!=Running
  ```

- **JupyterHub not defteri açılmıyor (spawn zaman aşımı):** soğuk imaj çekimi ~4,5 dakika
  sürer; ürünün bekleme süresi buna göre ayarlıdır. Not defteri yalnız Trino 8443 ve Polaris
  8181 uçlarına ve dış ağa çıkabilir
  ([90-referans/port-ve-servisler.md](../90-referans/port-ve-servisler.md) §5).
- **Zeppelin paragrafı "yorumlayıcı hazır değil, bağımlılıklar iniyor":** açılışta Trino JDBC
  sürücüsü Maven'den iner (~1 dakika, sonra kalıcıdır) — bekleyin. Kapalı ağda bu adım
  **başarısız** olur: sürücüyü diske koyup bağımlılığı yerel yapmak gerekir ([§6.2](#maven)).
- **Zeppelin yorumlayıcı ayarı ya da parolası değişmiyor:** `zeppelin-interpreter` Secret'ı
  yalnız bir **tohumdur**; diskte dosya varsa etkisizdir. Ayar arayüzden değiştirilir
  ([40-kurulum-sonrasi](../40-kurulum-sonrasi.md) §5.6).
- **Zeppelin girişi başarılı ama not defteri listesi 401:** rol kapısı devrededir — kullanıcı
  üç `lakehouse-*` grubunun hiçbirinde değildir
  ([kullanici-ve-yetki.md](kullanici-ve-yetki.md) §3).
- **Giriş döngüsü ("Sign in with Keycloak" aynı sayfaya döner):** uygulama, Keycloak'la
  sunucu tarafında konuşamıyordur. Günlükte nedenine bakın:

  `[bastion]`

  ```bash
  oc -n "$LAKEHOUSE_NS" logs deploy/superset-web-server --tail=200 | grep -i "oauth"
  ```

  `CERTIFICATE_VERIFY_FAILED` ve `keycloak-...apps...` adresi görüyorsanız uygulama Keycloak'a
  **dış** adresten gidiyordur; üründe sunucu tarafı çağrılar küme içi adrese
  (`keycloak-service.lakehouse.svc:8080`) gider. Keycloak CR'ında
  `spec.hostname.backchannelDynamic: true` olduğunu ve site dosyalarının değiştirilmediğini
  doğrulayın (`scripts/check-site.sh`).
- **Keycloak girişi `Invalid username or password`, Keycloak günlüğünde `User returned from
  LDAP has null username`:** realm'in AD kullanıcı adı eşlemesi yoktur ya da eşlemeler AD
  sağlayıcısına bağlı değildir. Ürünün realm tanımı bunu içerir; eski bir sürümle kurulduysa
  realm'i yenileyin ([30-kurulum](../30-kurulum.md) "Realm içeriğini sonradan değiştirmek").
- **Zeppelin'e hiç kimse giremiyor, günlükte `LDAP: error code 49 ... data 52e`:** Zeppelin
  servis hesabıyla AD'ye bağlanamıyordur. En sık neden: `zeppelin-shiro` Secret'ında
  `systemUsername` satırına **DN** yazılmıştır; kısa ad (sAMAccountName) olmalıdır
  ([30-kurulum](../30-kurulum.md) §5.9). Satırı görmek için:

  `[bastion]`

  ```bash
  oc -n "$LAKEHOUSE_NS" get secret zeppelin-shiro -o jsonpath='{.data.shiro\.ini}' \
    | base64 -d | grep systemUsername
  ```

  Düzeltmek için §5.9'u `LDAP_BIND_USER` doluyken yeniden çalıştırın (önce
  `oc -n "$LAKEHOUSE_NS" delete secret zeppelin-shiro`), sonra
  `oc -n "$LAKEHOUSE_NS" rollout restart deploy/zeppelin`.
- **Not defterinden S3'e bağlanırken zaman aşımı (`TimeoutError`):** S3 iç ağdadır ve not
  defterlerinin güvenlik politikası iç ağ adreslerini kapatır. `platform/values/site/glue.yaml`
  içindeki `s3.egressCidrs` S3'ün veri IP'lerini içermelidir
  ([30-kurulum](../30-kurulum.md) §4.1); politikanın kümede olduğunu doğrulayın:
  `oc -n "$LAKEHOUSE_NS" get networkpolicy notebook-s3-egress -o yaml`.
- **Analist ya da son kullanıcı Trino'da `Access Denied: Cannot access catalog lakehouse`
  alıyor, Superset ve Zeppelin (servis hesapları) çalışıyor:** Trino kullanıcının AD
  gruplarını okuyamıyordur; yetki kuralları gruplara bağlı olduğu için grupsuz kullanıcı
  hiçbir şey göremez. Coordinator günlüğüne bakın:

  `[bastion]`

  ```bash
  oc -n "$LAKEHOUSE_NS" logs deploy/trino-coordinator -c trino-coordinator --since=1h \
    | grep -c "SSLContext was not set"
  grep -n "ldap.ssl.truststore.path" platform/values/site/trino.yaml
  ```

  Sayı `0`'dan büyükse ve ikinci komut bir satır gösteriyorsa site dosyanız eski bir
  sürümden kalmıştır: Trino 483'te `ldap.ssl.truststore.path` satırının açtığı SSL yolu
  aralıklı olarak düşer (OpenShift provasında görüldü). Satırı silin, commit edip itin;
  AD kök CA'sı ürünün kendi değer dosyasıyla JVM güven deposuna eklenir
  ([30-kurulum](../30-kurulum.md) §5.13). Coordinator yeniden açıldıktan sonra
  `oc -n "$LAKEHOUSE_NS" logs deploy/trino-coordinator -c s3-truststore` çıktısında
  `AD CA eklendi` satırı görünmelidir. Günlükte başka bir LDAP hatası varsa (`PKIX`,
  `Invalid credentials`) sorun AD kökü ya da bağlama hesabıdır.

- **Not defterinden Trino'ya bağlanırken `CA cert does not include key usage extension`:**
  `lakehouse-ca` kök sertifikası `Key Usage` uzantısı olmadan üretilmiştir (kılavuzun eski
  sürümündeki komut). Güncel Python bu CA'yı kabul etmez. Bakmak için:

  `[bastion]`

  ```bash
  oc -n "$LAKEHOUSE_NS" get secret lakehouse-ca -o jsonpath='{.data.tls\.crt}' \
    | base64 -d | openssl x509 -noout -ext keyUsage
  ```

  Çıktı boşsa CA'yı yenileyin. Aşağıdaki adımlar bastion'da depo kökünde çalıştırılır;
  kullanıcı oturumları birkaç dakika kesilir:

  1. Yeni CA'yı [30-kurulum](../30-kurulum.md) §5.11'deki `openssl req` komutuyla üretin ve
     Secret'ı değiştirin:

     ```bash
     oc -n "$LAKEHOUSE_NS" create secret tls lakehouse-ca \
       --cert=/tmp/ca.crt --key=/tmp/ca.key --dry-run=client -o yaml | oc apply -f -
     ```

  2. Trino sunucu sertifikasını yeni CA ile yeniden imzalatın:
     `oc -n "$LAKEHOUSE_NS" delete secret trino-tls`. Yaklaşık 20 saniye içinde
     `oc -n "$LAKEHOUSE_NS" get certificate trino-tls` `True` göstermelidir.
  3. `platform/values/site/glue.yaml` içindeki `tls.caBundle` değerini yeni PEM'le
     değiştirin (aynı dosyada `caBundle: "..."` satırının tamamı), commit edip itin.
     ArgoCD Trino Route'unu günceller.
  4. Sertifikayı bellekte tutan bileşenleri yeniden başlatın:

     ```bash
     oc -n "$LAKEHOUSE_NS" rollout restart \
       deploy/trino-coordinator deploy/superset-web-server deploy/zeppelin
     ```

     Not defterleri dosyayı her bağlantıda yeniden okur; yeniden başlatmak gerekmez.
  5. Eski `~/lakehouse-ca.crt` dosyasını yenisiyle değiştirin, `shred -u /tmp/ca.key`.

---

<a id="dr"></a>
## 8. İzleme ve yedekler

- **Prometheus hedefi `down` ya da metrik yok.** Konsolda **Observe → Targets**, komut
  satırında:

  `[bastion]`

  ```bash
  oc -n "$LAKEHOUSE_NS" get podmonitor,servicemonitor,prometheusrule
  ```

  İzleme kapsamı **bilerek dardır**: Kafka/Connect, tüketici gecikmesi, spark-operator ve
  Polaris. Trino, Superset, JupyterHub ve Zeppelin için **uygulama metriği toplanmaz** — bu
  hedeflerin listede olmaması hata değildir
  ([izleme-ve-alarmlar.md](izleme-ve-alarmlar.md) §2).

- **Alarm ateşleniyor ama bildirim gelmiyor.** Bildirimi platformun Alertmanager'ı gönderir;
  yönlendirme tanımlı değilse kural ateşlenir ama hiçbir yere gitmez
  ([izleme-ve-alarmlar.md](izleme-ve-alarmlar.md) §5).

- **CNPG `ContinuousArchiving=False`.**

  `[bastion]`

  ```bash
  oc -n "$LAKEHOUSE_NS" get cluster polaris-db -o jsonpath='{.status.conditions}' | jq
  oc -n "$LAKEHOUSE_NS" logs polaris-db-1 -c plugin-barman-cloud --tail=50
  oc -n "$LAKEHOUSE_NS" get objectstore lakehouse-backups -o yaml | head -30
  ```

  Sık kökler: hedef bucket yok, `backup-s3-creds` yanlış ya da eksik, uç nokta yanlış, S3'ün
  kök CA'sı verilmemiş (günlükte `SSL validation failed`), Barman eklentisinin dağıtımı
  ayakta değil. Bucket yaratıldıktan sonra `ScheduledBackup` nesnelerini
  silip yeniden yaratmak ilk yedeği hemen tetikler
  ([yedek-ve-geri-donus.md](yedek-ve-geri-donus.md) §3).

- **S3'e bağlanırken `CERTIFICATE_VERIFY_FAILED` ya da `PKIX path building failed`.** S3
  sertifikası kurumun kendi CA'sıyla imzalıdır ya da self-signed'dır ve ürüne bu CA
  verilmemiştir: `s3-ca` Secret'ını yaratın ve site değerine `s3.caSecret: "s3-ca"` yazın
  ([30-kurulum](../30-kurulum.md) §4.1 ve §5.14). **Kurulumdan sonra** yaptıysanız bileşenler
  CA'yı ancak yeniden başlayınca alır:

  `[bastion]`

  ```bash
  T=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  for c in polaris-db keycloak-db superset-db; do
    oc -n "$LAKEHOUSE_NS" annotate cluster.postgresql.cnpg.io "$c" \
      kubectl.kubernetes.io/restartedAt="$T" --overwrite
  done
  oc -n "$LAKEHOUSE_NS" rollout restart deploy/polaris deploy/trino-coordinator deploy/trino-worker
  ```

  Kafka Connect, `s3.caSecret` değiştiğinde Strimzi tarafından kendiliğinden yeniden
  başlatılır; Spark işleri her koşuda yeni pod açtığı için ek iş gerekmez. Açık not defterleri
  kullanıcı oturumu kapatıp yeniden açınca yeni ayarı alır.

- **Velero yedeği `PartiallyFailed`.**

  `[bastion]`

  ```bash
  oc -n openshift-adp describe backups.velero.io lakehouse-daily-20260924081157 | tail -40
  oc -n openshift-adp logs deploy/velero --tail=100
  ```

  Geliştirme kümesinde kısmi durum **beklenen** sonuçtur: kind'ın diskleri dosya sistemi
  yedeğine girmez. Üretimde (CSI depolama) girer
  ([yedek-ve-geri-donus.md](yedek-ve-geri-donus.md) §6).

- **Yedekte hiç dosya sistemi yedeği yok:** dışlama listesinde `pods` varsa dosya sistemi
  yedeği hiç tetiklenmez; `velero.excludedResources` içinde `pods` **olmamalıdır**.

- **`oc get backup` yanlış nesneyi gösteriyor.** Kümede hem PostgreSQL'in hem Velero'nun
  `Backup` nesnesi vardır; kısa ad sessizce PostgreSQL'inkini seçer. Velero için **daima**
  tam ad yazın: `backups.velero.io`. Yedeği silmek S3'ü temizlemez
  ([yedek-ve-geri-donus.md](yedek-ve-geri-donus.md) §6.4).

---

<a id="loki"></a>
## 9. Günlükler (Loki)

Günlük toplama **platformun sorumluluğundadır**; bu ürün bir Loki dağıtımı içermez ve hiçbir
bileşeniyle Loki'ye bağımlı değildir ([20-on-kosullar](../20-on-kosullar.md) madde 9.3).

OpenShift'te yol **OpenShift Logging + Loki Operator**'dür: `LokiStack` kurulur,
`ClusterLogForwarder` ile toplama açılır ve günlükler konsolun **Observe → Logs**
sekmesinden sorgulanır. Bu kurulumda yapılacak tek şey `$LAKEHOUSE_NS` ad alanının toplama
kapsamında olduğunu doğrulamaktır:

`[bastion]`

```bash
oc -n openshift-logging get lokistack,clusterlogforwarder
```

**Beklenen çıktı** (örnek — OpenShift'e özgü, **OpenShift'te doğrulanır**): `LokiStack`
`Ready` ve `ClusterLogForwarder` girdisi `lakehouse` ad alanını kapsıyor.

Sık kullanılan sorgular (konsolun **Logs** sekmesine yapıştırılır):

```text
{namespace="lakehouse", pod=~"connect-connect-.*"} |= "ERROR"
{namespace="lakehouse", pod=~"silver-merge-.*-driver"} |~ "MERGE_OK|FALLBACK|Exception"
{namespace="lakehouse", pod=~"trino-coordinator-.*"} |= "Query failed"
{namespace="lakehouse", container="plugin-barman-cloud"} |= "archive"
```

Canlı sorunlarda `oc logs` her zaman en hızlı yoldur. Loki, **pod öldükten sonra** günlüğe
bakmak ve zaman aralığı üzerinden ilişki kurmak için gereklidir — tek seferlik Spark sürücü
pod'ları koşu bitince silinir.

**Ters giderse:** `the server doesn't have a resource type "lokistack"` → OpenShift Logging
kurulu değildir. Bu bir hata değildir; günlükler `oc logs` ile okunur.

---

## Kontrol listesi

- [ ] Belirti, §3 tablosunda bir satıra bağlandı.
- [ ] §2'deki dört komut koşturuldu ve çıktısı kaydedildi.
- [ ] Alarm ateşlendiyse ilgili bölümün "Ne bak" komutları koşturuldu.
- [ ] Kök neden giderildikten sonra `FAILED` Spark nesneleri silindi (alarm ancak o zaman
      düşer).
- [ ] Kabul ya da e2e koşusundan sonra zamanlanmış işler geri açıldı (§6.1).
- [ ] Kalıcı düzeltme kümede değil, Git'te yapıldı
      ([değişiklik nasıl uygulanır](degisiklik-nasil-uygulanir.md)).

## Sonraki bölüm

Alarm eşiklerinin anlamı ve nereden değiştirildiği:
[izleme-ve-alarmlar.md](izleme-ve-alarmlar.md). Her sabah ve her hafta koşturulacak sabit
kontroller: [gunluk-haftalik-kontroller.md](gunluk-haftalik-kontroller.md). Yedek ve geri
dönüş: [yedek-ve-geri-donus.md](yedek-ve-geri-donus.md).
