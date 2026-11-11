# Sorun giderme

Bu sayfa, kurulumdan günlük işletmeye kadar karşılaşılan sorunların tek listesidir. Önce
bir **belirti tablosu** (§3) vardır; her satır sizi o belirtinin bölümüne götürür. Her
bölümde sırayla **ne bakılır**, **ne yapılır** ve **ters giderse** ne olacağı yazar.

Lakehouse'un altı alarmının her biri buradaki kendi bölümüne bağlıdır: alarm bildirimindeki
`runbook` açıklaması doğrudan o başlığı gösterir.

**Ne kadar sürer:** ilk bakış 2–5 dakika; çözüm süresi soruna göre değişir.

**Hangi yetkiler gerekir:** `$LAKEHOUSE_NS` ad alanında okuma. Bazı çözümler aynı ad
alanında `annotate`, `patch` ya da pod silme yetkisi ister. Kalıcı düzeltmeler her zaman
Git üzerinden yapılır ([değişiklik nasıl uygulanır](degisiklik-nasil-uygulanir.md)).

**Komutlar nerede çalışır:** `[bastion]` — `oc login` ile kümeye bağlı, Git deposunun
bir kopyasının (`~/lakehouse`) bulunduğu yönetim makinesi.

---

## 1. Değişkenleri yükleyin

`[bastion]`

```bash
cd ~/lakehouse
set -a; . install/lakehouse.env; set +a
echo "argocd=$ARGOCD_NS lakehouse=$LAKEHOUSE_NS"
```

**Beklenen çıktı** (kendi değerlerinizle):

```text
argocd=openshift-gitops lakehouse=lakehouse
```

**Ters giderse:** eşittir işaretlerinden sonrası boşsa `install/lakehouse.env` dosyası
yoktur ya da başka bir klasördesiniz ([30-kurulum](../30-kurulum.md) §1).

---

## 2. İlk bakılacaklar

Belirti ne olursa olsun önce bu dört komutu çalıştırın; sorunların çoğu burada görünür.

`[bastion]`

```bash
oc -n "$ARGOCD_NS" get applications -o wide
oc -n "$LAKEHOUSE_NS" get pods -o wide
oc -n "$LAKEHOUSE_NS" get kafka,kafkaconnect,kafkaconnector,cluster,keycloak,superset,sparkapplication
oc -n "$LAKEHOUSE_NS" get events --sort-by=.lastTimestamp | tail -30
```

**Beklenen çıktı** (sağlıklı bir kurulumda):

- bütün uygulamalar `Synced Healthy`;
- hiçbir pod `CrashLoopBackOff`, `CreateContainerConfigError` ya da `Pending` değil;
- listelenen nesnelerin `READY` sütunu `True`.

**Ters giderse:** `the server doesn't have a resource type "applications"` görürseniz
`$ARGOCD_NS` değişkeni yanlış ad alanını gösteriyordur ya da OpenShift GitOps kurulu
değildir ([20-on-kosullar](../20-on-kosullar.md) madde 2).

---

## 3. Belirti tablosu

| Ne görüyorsunuz | Nereye bakın |
|---|---|
| Alarm: `LakehouseConnectTaskFailed` | [§4.1 Connect görevi hata verdi](#connect) |
| Alarm: `LakehouseSinkStalled` | [§4.2 Iceberg yazıcısı geride kalıyor](#sink) |
| Alarm: `LakehouseSilverMergeStale` | [§4.3 Silver güncellenmiyor](#silver-merge) |
| Alarm: `LakehouseSparkScheduledRunFailed` | [§4.4 Zamanlı Spark işi başarısız](#spark) |
| Alarm: `LakehouseSparkRunTooLong` | [§4.5 Spark işleri yavaşladı](#spark-duration) |
| Alarm: `LakehouseDlqRecords` | [§4.6 Hatalı kayıt konusuna kayıt düştü](#dlq) |
| Connect pod'unun `RESTARTS` sayısı artıyor | [§4.1 Connect pod'u yeniden başlıyor](#connect-oom) |
| Bağlayıcı `RUNNING` ama görev listesi boş | [§4.1 Görev listesi boş](#connect-gorev-yok) |
| Silver güncellenmiyor, bakım işleri çalışmıyor, hata da yok | [§6.1 Askıda kalan zamanlı iş](#ssa-suspend) |
| Spark ya da Connect günlüğünde `403 The Access Key Id you provided does not exist` | [§5.1 Polaris / S3 403](#polaris-403) |
| Spark işi `unresolved dependency` ya da Ivy hatası veriyor | [§6.2 Maven kütüphaneleri](#maven) |
| Bronze tablosu boş kalıyor, satır gelmiyor | [§6 Veri akışı](#boru-hatti) |
| Karantina (`__quarantine`) tablosu doluyor | [yeni-kaynak-ve-pipeline.md](yeni-kaynak-ve-pipeline.md) §3.3 ve §9.2 |
| `nginx.dlq` konusunda kayıt birikiyor | [yeni-kaynak-ve-pipeline.md](yeni-kaynak-ve-pipeline.md) §10 |
| Silver tablosunda `SchemaConflict` | [mevcut-kaynaga-tablo-ekleme.md](mevcut-kaynaga-tablo-ekleme.md) |
| `glue` uygulaması `OutOfSync` ya da `Degraded` kalıyor | [§5 Kurulum ve eşitleme](#kurulum) |
| Connect imaj yapımı uzun sürüyor ya da `unauthorized` veriyor | [§5 Kurulum ve eşitleme](#kurulum) |
| Trino `401` ya da `Authentication failed` | [§7 Kullanıcı araçları](#kullanici-yuzu) |
| Not defterinden Trino'ya `CA cert does not include key usage extension` | [§7 Kullanıcı araçları](#kullanici-yuzu) |
| Analist ya da kullanıcı Trino'da `Access Denied: Cannot access catalog lakehouse` alıyor (Superset ve Zeppelin çalışıyor) | [§7 Kullanıcı araçları](#kullanici-yuzu) |
| Trino `Access Denied` | [kullanici-ve-yetki.md](kullanici-ve-yetki.md) §5 |
| Superset açılmıyor (`phase: Initializing`) | [§7 Kullanıcı araçları](#kullanici-yuzu) |
| JupyterHub not defteri açılmıyor (zaman aşımı) | [§7 Kullanıcı araçları](#kullanici-yuzu) |
| Zeppelin `jdbc` yorumlayıcısı hazır değil | [§7 Kullanıcı araçları](#kullanici-yuzu) |
| Zeppelin'e girişte not defteri listesi 401 veriyor | [kullanici-ve-yetki.md](kullanici-ve-yetki.md) §3 |
| İzleme hedefi `down`, metrik yok | [§8 İzleme ve yedekler](#dr) |
| Veritabanı yedeği `ContinuousArchiving=False` | [§8 İzleme ve yedekler](#dr) |
| Velero yedeği `PartiallyFailed` | [§8 İzleme ve yedekler](#dr) |
| `oc get backup` yanlış nesneyi gösteriyor | [§8 İzleme ve yedekler](#dr) |
| Pod silinmiş, günlüğüne artık bakılamıyor | [§9 Günlükler (Loki)](#loki) |

---

## 4. Alarmlar

Kümede tek bir alarm kural nesnesi (`PrometheusRule lakehouse`) ve içinde altı kural
vardır. Alarmların anlamı, eşikleri ve nereden değiştirildikleri
[izleme-ve-alarmlar.md](izleme-ve-alarmlar.md) sayfasındadır; burada yalnız "alarm geldi,
ne yapmalı" adımları vardır.

Kuralların o anki durumunu görmek için:

`[bastion]`

```bash
TOKEN=$(oc whoami -t)
HOST=$(oc -n openshift-monitoring get route thanos-querier -o jsonpath='{.spec.host}')
curl -sSk -H "Authorization: Bearer $TOKEN" "https://$HOST/api/v1/rules" \
  | jq -r '.data.groups[] | select(.name=="lakehouse") | .rules[] | "\(.name)\t\(.health)\t\(.state)"'
```

**Beklenen çıktı** (sağlıklı, alarmsız bir kurulumda; satırların sırası değişebilir):

```text
LakehouseConnectTaskFailed	ok	inactive
LakehouseDlqRecords	ok	inactive
LakehouseSilverMergeStale	ok	inactive
LakehouseSinkStalled	ok	inactive
LakehouseSparkRunTooLong	ok	inactive
LakehouseSparkScheduledRunFailed	ok	inactive
```

Üçüncü sütun alarm gelmişse `firing` olur.

**Ters giderse:**

- İkinci sütun `err` ise kural hesaplanamıyordur; çoğu zaman bir ölçüm kaynağı eksiktir
  ([§8](#dr)).
- Liste boşsa kullanıcı iş yükü izlemesi kapalıdır ya da kural nesnesi kümede yoktur
  ([20-on-kosullar](../20-on-kosullar.md) madde 9.1).

<a id="connect"></a>
### 4.1 `LakehouseConnectTaskFailed` — Connect görevi hata verdi

Bir bağlayıcının görevi 5 dakikadır `FAILED` durumunda. Önem: **critical**.

**Ne bakılır**

`[bastion]`

```bash
CONNECTOR=dbz-shop        # alarmın özet satırındaki bağlayıcı adı
oc -n "$LAKEHOUSE_NS" get kafkaconnector
oc -n "$LAKEHOUSE_NS" get kafkaconnector "$CONNECTOR" \
  -o jsonpath='{.status.connectorStatus.tasks[0].trace}'
oc -n "$LAKEHOUSE_NS" logs -l strimzi.io/kind=KafkaConnect --tail=200
```

**Beklenen çıktı** (ilk komut, sağlıklı hâl; alarm sırasında ilgili satırın `READY`
sütunu `False` olur):

```text
NAME              CLUSTER   CONNECTOR CLASS                                      MAX TASKS   READY
dbz-crm           connect   io.debezium.connector.mongodb.MongoDbConnector       1           True
dbz-erp           connect   io.debezium.connector.postgresql.PostgresConnector   1           True
dbz-fin           connect   io.debezium.connector.sqlserver.SqlServerConnector   1           True
dbz-ogr           connect   io.debezium.connector.sqlserver.SqlServerConnector   1           True
dbz-shop          connect   io.debezium.connector.postgresql.PostgresConnector   1           True
```

İkinci komut görevin hata mesajını (`trace`) yazar; çözüm bu mesaja göre seçilir.

**Ne yapılır**

- **Geçici bir hata** (kaynak veritabanı yeniden başladı, ağ bir süre koptu): görevi
  yeniden başlatın.

  `[bastion]`

  ```bash
  oc -n "$LAKEHOUSE_NS" annotate kafkaconnector "$CONNECTOR" strimzi.io/restart=true
  ```

- **Kimlik ya da yetki hatası:** kaynağın Secret'ını (kaynak adı + `-db`, ör. `erp-db`) ve
  kaynak veritabanındaki hazırlığı kontrol edin:
  [yeni-kaynak-ve-pipeline.md](yeni-kaynak-ve-pipeline.md) Adım 3 (CDC kullanıcısı,
  publication, sinyal tablosu) ve Adım 4 (Secret).
- **Dönüştürme hatası:** bozuk kayıt görevi durdurmaz, kaynağın hatalı kayıt konusuna
  gider (`shop` kaynağı için `shop.dlq`); kaydı oradan okuyun ([§4.6](#dlq)).
- **Görev uzun süre kapalı kaldıysa** Iceberg yazıcısının konumu ilerlemiş olabilir ve
  arada kayıt atlanmış olabilir. Bu durumda kaynağı yeniden okutmak gerekir
  ([mevcut-kaynaga-tablo-ekleme.md](mevcut-kaynaga-tablo-ekleme.md) §6).

**Ters giderse:** `trace` boşsa görev hiç başlamamıştır. Connect günlüğüne bakın; imaj
yapımı yarım kalmış olabilir ([§5](#kurulum)).

<a id="connect-oom"></a>
#### Connect pod'u yeniden başlıyor (bellek yetmiyor)

Connect pod'larının `RESTARTS` sütunu artıyorsa son kapanmanın nedenine bakın:

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" get pod -l strimzi.io/kind=KafkaConnect
oc -n "$LAKEHOUSE_NS" get pod -l strimzi.io/kind=KafkaConnect \
  -o jsonpath='{range .items[*]}{.metadata.name}{" "}{.status.containerStatuses[0].lastState.terminated.reason}{"\n"}{end}'
```

**Beklenen çıktı** (bellek yetmediğinde):

```text
connect-connect-0 OOMKilled
```

`OOMKilled`, pod'un bellek sınırını aştığını gösterir. Bağlayıcı o sırada bir tablonun ilk
okumasını yapıyorsa okuma baştan alınır ve Bronze'a bazı kayıtlar iki kez yazılır; Silver
bunları ayıklar. Belleği artırın: `platform/values/site/glue.yaml` dosyasına
`connect.resources` ve `connect.heap` ayarlarını ekleyin. Yığın (`heap`) sınırın yaklaşık
%60'ı olmalıdır. Örneğin 6 GiB için:

```yaml
connect:
  resources:
    requests: {cpu: 500m, memory: 6Gi}
    limits: {memory: 6Gi}
  heap: 3600m
```

Ürünün varsayılanı işçi başına 4 GiB'tır ([10-planlama](../10-planlama.md) §5).

<a id="connect-gorev-yok"></a>
#### Bağlayıcı çalışıyor ama görev listesi boş

Birden fazla Connect işçisi varken, işçilerden biri yeniden başladıktan sonra bir
bağlayıcının durumu `RUNNING` görünür ama görev listesi boş kalabilir. Görev aslında başka
bir işçide çalışıyordur; işçilerden biri onun durum kaydını kaçırmıştır. Her işçiye ayrı
ayrı sorun:

`[bastion]`

```bash
CONNECTOR=sink-nginx        # görev listesi boş görünen bağlayıcı
for p in $(oc -n "$LAKEHOUSE_NS" get pod -l strimzi.io/kind=KafkaConnect -o name); do
  echo "$p: $(oc -n "$LAKEHOUSE_NS" exec "${p#pod/}" -- curl -s http://localhost:8083/connectors/$CONNECTOR/status | jq -c .tasks)"
done
```

**Beklenen çıktı** (bu durumda):

```text
pod/connect-connect-0: []
pod/connect-connect-1: [{"id":0,"state":"RUNNING","worker_id":"connect-connect-1.connect-connect.lakehouse.svc:8083","version":"1.11.0"}]
```

Görevi yeniden başlatmak durum kaydını yeniler; birkaç saniye sonra iki işçi de aynı şeyi
gösterir:

```bash
oc -n "$LAKEHOUSE_NS" exec connect-connect-0 -- \
  curl -s -o /dev/null -w '%{http_code}\n' -X POST http://localhost:8083/connectors/$CONNECTOR/tasks/0/restart
```

**Beklenen çıktı:**

```text
204
```

<a id="sink"></a>
### 4.2 `LakehouseSinkStalled` — Iceberg yazıcısı geride kalıyor

Bir Iceberg yazıcısının (sink) okumadığı kayıt sayısı 15 dakikadır eşiğin üstünde. Önem:
**warning**. Eşik `monitoring.sinkLagThreshold` ayarındadır (varsayılan 1000).

**Ne bakılır**

`[bastion]`

```bash
scripts/kafka-cli.sh kafka-consumer-groups.sh --describe --group connect-sink-shop
oc -n "$LAKEHOUSE_NS" get kafkaconnector sink-shop \
  -o jsonpath='{.status.connectorStatus.tasks[0]}'
oc -n "$LAKEHOUSE_NS" logs -l strimzi.io/kind=KafkaConnect --tail=200 | grep -i 'commit\|iceberg\|s3'
```

Grup adı `connect-sink-` ve kaynağın adıdır. Önce birinci komutun **`LAG`** sütununa
bakın; ne yapılacağını o belirler:

| `LAG` | Anlamı | Ne yapılır |
|---|---|---|
| `0` | Her şey yolunda; okunacak yeni kayıt yok | Bir şey yapmayın |
| sıfırdan büyük ama her bakışta **azalıyor** | Birikim eriyor | Bekleyin; bir kesintiden sonra normaldir |
| sıfırdan büyük ve **sabit** | Yazıcı okuyor ama tabloya yazamıyor | Aşağıdaki "Bronze tablosu var ama boş" adımları |

> **`committed to 0 table(s)` satırı hata değildir.** Yazıcı beş dakikada bir yazma turu
> başlatır; o turda yeni kayıt yoksa günlüğe `committed to 0 table(s)` ve
> `Commit timeout reached` yazar. Boşta duran sağlıklı bir kurulumda bu satırlar sürekli
> görünür. Bu satırları her zaman `LAG` sütunuyla birlikte okuyun.

**Ne yapılır**

- Görev `RUNNING` ama tabloya yazılmıyorsa katalog ya da S3 erişimi bozuktur →
  [§5.1 Polaris / S3 403](#polaris-403).
- Gerçek bir yük artışıysa kaynağın `sinkTasks` değerini ve `connect.replicas` sayısını
  artırın. Yazıcı beş dakikada bir yazdığı için kısa birikmeler normaldir.
- Uzun bir kesintiden sonra `LAG`'in **azaldığını** görmek yeterlidir; bekleyin.
- `-coord` ile biten grup (ör. `connect-sink-shop-coord`) yazıcının kendi denetim
  konusudur. Bu grupta kalıcı birikme varsa yazıcının koordinatörü takılmıştır →
  bağlayıcıyı yeniden başlatın ([§4.1](#connect)).

**Ters giderse:** `kafka-cli.sh` broker pod'unu bulamazsa pod adları farklıdır;
`oc -n "$LAKEHOUSE_NS" get pods -l strimzi.io/cluster=lakehouse` ile listeleyin.

<a id="sink-bos-tablo"></a>
#### Bronze tablosu var ama boş (`LAG` sabit)

Belirti: yeni tablonun Bronze karşılığı **yaratılmış**, S3'te veri dosyaları bile var, ama
`select count(*)` `0` döndürüyor ve `LAG` küçük bir sayıda (ör. `2`) **donmuş**. Yazıcı
okuma konumunu ancak tabloya yazma başarılı olursa ilerletir; yazma başarısız olduğu sürece
`LAG` aynı kalır ve yazılan dosyalar tabloya bağlanmaz.

Bağlayıcıyı ya da Connect pod'unu yeniden başlatmak bunu çözmez. Şu üç komutu çalıştırın:

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" get kafkaconnector sink-shop \
  -o jsonpath='{range .status.connectorStatus.tasks[*]}{.state}{"\n"}{.trace}{"\n"}{end}'
oc -n "$LAKEHOUSE_NS" logs -l strimzi.io/kind=KafkaConnect --tail=500 \
  | grep -iE "NoSuchTable|Forbidden|403|AccessDenied|schema|Exception"
scripts/kafka-cli.sh kafka-consumer-groups.sh --describe --group connect-sink-shop-coord
```

**Beklenen çıktı:** birinci komut `RUNNING` ve boş bir hata mesajı verir. İkinci komut bir
şey yazmazsa sorun yetki ya da şema değildir. Üçüncü komutun `LAG` sütunu sürekli
büyüyorsa koordinatör takılmıştır.

**Ne yapılır** — hangi komut ne gösterdiyse:

- İkinci komut `NoSuchTableException`, `403` ya da `AccessDenied` yazdıysa yazıcı yeni
  Bronze ad alanında tablo yaratamıyordur → [§5.1](#polaris-403). En sık neden, kaynağın
  Bronze ad alanının katalogda hiç açılmamış olmasıdır
  ([yeni-kaynak-ve-pipeline.md](yeni-kaynak-ve-pipeline.md) §5).
- İkinci komut bir şema hatası yazdıysa Bronze tablosu kaynağın yeni kolonlarıyla
  uyuşmuyordur. Tabloyu silin; yazıcı onu yeniden yaratır
  ([kaynak-veya-tablo-silme.md](kaynak-veya-tablo-silme.md) §6.2).
- Üçüncü komutta `LAG` büyüyorsa koordinatörün çalıştığı Connect pod'unu silin; Strimzi
  onu yeniden açar. Sonra `LAG`'e tekrar bakın:

  ```bash
  oc -n "$LAKEHOUSE_NS" delete pod connect-connect-0
  ```

- Üçü de temizse ve `LAG` hâlâ sabitse üç komutun çıktısını ve
  `oc -n "$LAKEHOUSE_NS" get kafkaconnector sink-shop -o yaml` çıktısını ürün ekibine
  iletin. Veriyi kurtarmak için Bronze tablosunu silip kaynağı yeniden okutun
  ([mevcut-kaynaga-tablo-ekleme.md](mevcut-kaynaga-tablo-ekleme.md) §6).

<a id="silver-merge"></a>
### 4.3 `LakehouseSilverMergeStale` — Silver güncellenmiyor

Silver tablolarını güncelleyen `silver-merge` işinin son başarılı çalışmasının üzerinden
eşikten uzun zaman geçti. Önem: **warning**. Eşik `monitoring.silverMergeStaleSeconds`
ayarındadır (varsayılan 2700 saniye = 45 dakika).

**Ne bakılır**

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" get scheduledsparkapplication silver-merge \
  -o jsonpath='suspend={.spec.suspend} sonKosu={.status.lastRun} sonraki={.status.nextRun}{"\n"}'
oc -n "$LAKEHOUSE_NS" get sparkapplication | grep silver-merge
```

**Beklenen çıktı** (ilk satır, sağlıklı hâl):

```text
suspend=false sonKosu=2026-11-11T17:15:00Z sonraki=2026-11-11T17:30:00Z
```

`suspend` boş ya da `false` olmalı; `sonKosu` son 15 dakika içinde olmalıdır.

**Ne yapılır**

- `suspend=true` ise iş askıya alınmıştır → [§6.1](#ssa-suspend). Bu alarmın en sık nedeni
  budur.
- Son çalışma `FAILED` ise → [§4.4](#spark).
- Hiç `silver-merge` çalışması yoksa alarm gelmez. `sonraki` alanı boşsa iş hiç
  zamanlanmamıştır.
- Çalışmalar 24 saat sonra kendiliğinden silinir. İş bir günden uzun durursa alarm bu
  yüzden kapanır; bu, sorunun geçtiği anlamına gelmez. Yukarıdaki komutla bakın.

**Ters giderse:** zamanlama (`cron`) değiştirildiği hâlde `sonraki` alanı eski kalıyorsa
[§6.1](#ssa-suspend) sonundaki nota bakın.

<a id="spark"></a>
### 4.4 `LakehouseSparkScheduledRunFailed` — zamanlı Spark işi başarısız

Ürünün kendi zamanlı işlerinden biri başarısız oldu: `silver-merge`, `maint-` ile başlayan
bakım işleri ya da `mongo-bronze`. Önem: **warning**. Kendi yazdığınız Spark işleri bu
alarma girmez ([yeni-spark-uygulamasi.md](yeni-spark-uygulamasi.md)).

Alarm **iş başına bir tanedir**: aynı işin birden çok çalışması başarısız olsa da tek
alarm gelir. Özet satırı işin adını ve başarısız çalışma sayısını söyler, örneğin
`Zamanlanmış Spark işi başarısız: maint-expire-orphan-ttl (1 koşu)`.

Bir çalışma Spark sürücü pod'u **hiç açılmadan** düşerse (gönderim hatası) özet satırında
iş adı yerine `gönderim hatası` yazar, örneğin
`Zamanlanmış Spark işi başarısız: gönderim hatası (5 koşu)`. En sık nedeni bir Maven
kütüphanesinin indirilememesidir ([§6.2](#maven)). Bu sayı son bir saatin toplamıdır ve
kendi Spark işlerinizin gönderim hatalarını da içerir; hata yinelenmezse alarm bir saat
sonra kendiliğinden kapanır.

**Ne bakılır**

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" get sparkapplication | grep -E 'NAME|FAILED'
APP=silver-merge-1794398400001535062        # yukarıdaki listede FAILED görünen ad
oc -n "$LAKEHOUSE_NS" get sparkapplication "$APP" \
  -o jsonpath='{.status.applicationState.errorMessage}' | tail -5
oc -n "$LAKEHOUSE_NS" logs "$APP-driver" --tail=200
```

Hata mesajı çalışma nesnesindedir; sürücü pod'u açıldıysa günlüğü de ayrıca okunur. Gönderim
hatasında sürücü pod'u yoktur ve son komut `NotFound` verir.

**Ne yapılır** — sık nedenler:

| Hata mesajında | Nedeni | Çözüm |
|---|---|---|
| `unresolved dependency` ya da Ivy hatası | kütüphane önbellekte yok ve önbellek Maven Central'a (ya da kurum aynasına) erişemiyor | [§6.2 Maven kütüphaneleri](#maven) |
| `SchemaConflict` (`silver-merge`) | Silver'daki bir kolonun tipi güvenle genişletilemiyor | [mevcut-kaynaga-tablo-ekleme.md](mevcut-kaynaga-tablo-ekleme.md) |
| `KAFKA_JAAS` yok (`mongo-bronze`) | `spark` Kafka kullanıcısı yalnız bir MongoDB kaynağı tanımlıyken vardır | MongoDB kaynağını ekleyin ya da işi kapatın |
| bellek hatası (`mongo-bronze`, uzun bir kesintiden sonra) | birikmiş kayıtlar sürücü belleğine sığmıyor | `spark.driver.memory` değerini artırın |
| sürücü `CreateContainerConfigError` | `polaris-spark` Secret'ı yok | katalog kurulum betiğini çalıştırın ([40-kurulum-sonrasi](../40-kurulum-sonrasi.md) §2) |
| sürücü 20 dakikadan uzun `Pending` | düğümlerde yeterli CPU ya da bellek yok | `spark.driver.coreRequest`, `spark.executor.coreRequest`; düğüm kapasitesi |
| `403 The Access Key Id you provided does not exist` | geçici S3 kimliğinin süresi | [§5.1 Polaris / S3 403](#polaris-403) |

**Alarm, başarısız çalışma silinene kadar açık kalır.** Ürün çalışmaları 24 saat sonra
kendiliğinden siler. Daha önce kapatmak için, nedeni giderdikten sonra çalışmayı silin.
Sürücü pod'u ve günlüğü de silinir; gerekiyorsa günlüğü önce kaydedin:

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" delete sparkapplication "$APP"
```

**Ters giderse:** sürücü pod'u çoktan silindiyse günlüğe yalnız Loki üzerinden
bakılabilir ([§9](#loki)).

<a id="spark-duration"></a>
### 4.5 `LakehouseSparkRunTooLong` — Spark işleri yavaşladı

Son 6 saatte biten Spark işlerinin **ortalama** süresi eşiği aştı. Önem: **warning**.
Eşik `monitoring.sparkRunMaxSeconds` ayarındadır (varsayılan 1800 saniye).

Bu alarm tek bir işe değil, bütün işlerin ortalamasına bakar; çünkü Spark ölçümleri iş
adını taşımaz.

**Ne bakılır**

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" get sparkapplication -o custom-columns='AD:.metadata.name,DURUM:.status.applicationState.state,BASLANGIC:.status.lastSubmissionAttemptTime,BITIS:.status.terminationTime'
oc -n "$LAKEHOUSE_NS" logs -l spark-role=driver --tail=200 \
  | grep -E 'incremental|FALLBACK|full|MERGE_OK|MAINT_OK'
```

Birinci komut her çalışmanın başlangıç ve bitiş saatini verir; uzun sürenleri buradan
bulursunuz.

**Ne yapılır**

- `FALLBACK full` satırı görünüyorsa `silver-merge` uzun bir kesintiden sonra tabloyu
  baştan birleştirmiştir; bir sonraki çalışmada normale döner. Sürekli görünüyorsa
  bakım işlerinin son çalışmalarına bakın.
- Veri büyüdüyse Spark kaynaklarını artırın: `platform/values/site/glue.yaml` içindeki
  `spark` bloğu ([10-planlama](../10-planlama.md) §2).
- Bakım işleri veri hacmiyle birlikte uzar; eşiği gerçekçi bir değere çekmek de doğru bir
  çözümdür ([izleme-ve-alarmlar.md](izleme-ve-alarmlar.md) §4).

**Ters giderse:** hiç çalışma yokken bu alarm gelmez. "İşler hiç çalışmıyor" durumuna
[§4.3](#silver-merge) ile bakın.

<a id="dlq"></a>
### 4.6 `LakehouseDlqRecords` — hatalı kayıt konusuna kayıt düştü

Bir yazıcının işleyemediği kayıtlar (JSON olmayan ya da veri sözleşmesine uymayan) tabloya
girmez; kaynağın ya da akışın hatalı kayıt konusuna (adı `.dlq` ile biter) gider. Son bir
saatte böyle bir kayıt geldiyse alarm konunun adıyla uyarır. Önem: **warning**. Yeni kayıt
gelmezse alarm bir saat sonra kendiliğinden kapanır.

**Ne bakılır** — alarmdaki konunun adını yazın (örnekte `sensors.dlq`):

`[bastion]`

```bash
scripts/kafka-cli.sh kafka-get-offsets.sh --topic sensors.dlq
scripts/kafka-cli.sh kafka-console-consumer.sh --topic sensors.dlq --from-beginning \
  --max-messages 10 --timeout-ms 15000
```

**Beklenen çıktı** (ilk satır konudaki kayıt sayısı, ardından kayıtların kendisi):

```text
sensors.dlq:0:3
bu JSON değil
bu JSON değil
{"device_id": "sensor-9", "room": "C-301", "temperature_c": "yine bozuk", "humidity_pct": 40}
```

**Ne yapılır**

- **JSON akışı** (akış adı + `.dlq`, ör. `sensors.dlq`): kaydı gönderen uygulama ya da cihaz
  hatalı veri üretiyordur; kayıtları uygulama ekibine iletin. Bir alanın tipi bilerek
  değiştiyse veri sözleşmesini güncelleyin ([json-akisi-ekleme.md](json-akisi-ekleme.md)
  Adım 7).
- **nginx** (`nginx.dlq`): ajan JSON olmayan bir ileti göndermiştir; ajanın günlüğüne bakın
  ([yeni-kaynak-ve-pipeline.md](yeni-kaynak-ve-pipeline.md) Adım 10.4).
- **Veritabanı kaynağı** (kaynak adı + `.dlq`, ör. `erp.dlq`): bağlayıcı olağan dışı bir
  kayıt üretmiştir; kaydı ve Connect günlüğünü ([§4.1](#connect)) inceleyin.

Hatalı kayıt konusundaki kayıtlar 7 gün sonra kendiliğinden silinir; elle temizlemek
gerekmez.

---

<a id="kurulum"></a>
## 5. Kurulum ve eşitleme

- **Connect imaj yapımı uzun sürüyor ya da başarısız.** Yapım normalde 10 dakika kadar
  sürer. Günlüğe bakın:

  `[bastion]`

  ```bash
  oc -n "$LAKEHOUSE_NS" logs -l strimzi.io/kind=KafkaConnect --tail=100
  ```

  `unauthorized` görüyorsanız `connect-push` Secret'ındaki erişim anahtarının süresi
  dolmuştur ya da anahtar yanlıştır ([yukseltme.md](yukseltme.md) §6).

- **`glue` uygulaması uzun süre `OutOfSync Progressing` kalıyor.** Bazı depolama
  sınıfları diski ancak onu kullanacak pod açılınca bağlar. Disk ile pod'u farklı sırada
  kuran bir değişiklik eşitlemeyi kilitleyebilir. Hangi nesnenin beklediğini görün:

  `[bastion]`

  ```bash
  oc -n "$ARGOCD_NS" get app glue -o json \
    | jq -r '.status.resources[]? | select((.status!="Synced") or ((.health.status // "Healthy")!="Healthy")) | "\(.kind)/\(.name) sync=\(.status) health=\(.health.status // "-")"'
  oc -n "$LAKEHOUSE_NS" get pvc
  ```

  `Pending` kalan bir disk (`PersistentVolumeClaim`) görürseniz ürün ekibine iletin.

- **İlk kurulumda "pod sağlıklı ama zaman aşımı".** Zeppelin imajı 2,7 GB, not defteri
  imajı yaklaşık 2 GB'tır ve Connect imajının yapımı 10 dakika kadar sürer. İlk kurulumda
  bu süreler beklenir; ürün bekleme sürelerini buna göre ayarlar. Beklemek doğru
  davranıştır.

- **`polaris-bootstrap` işi deneme hakkını tüketti.** İlk kurulumda veritabanı 4–5 dakikada
  hazır olur; işin deneme hakkı bunun içindir. Hakkı tükendiyse işi silip eşitlemeyi
  tekrarlayın; iş yeniden çalıştırılabilir:

  `[bastion]`

  ```bash
  oc -n "$LAKEHOUSE_NS" delete job polaris-bootstrap
  oc -n "$ARGOCD_NS" patch application glue --type merge -p '{"operation":{"sync":{}}}'
  ```

- **Bir bağlantı zaman aşımına uğruyor.** Ürünün ağ politikaları yalnız izin verilen ad
  alanlarından gelen isteklere izin verir; listede olmayan bir yerden gelen istek sessizce
  zaman aşımına uğrar. İzin verilen ad alanları
  [90-referans/port-ve-servisler.md](../90-referans/port-ve-servisler.md) §5'tedir.
  Sorunun ağ politikasından gelip gelmediğini anlamak için site ayarlarında
  `networkPolicy.enabled: false` ile politikaları geçici olarak kapatabilirsiniz. Bu
  **yalnız teşhis içindir**; neden bulunur bulunmaz politikaları geri açın.

- **Sertifika yenilenmiyor.** Trino'nun iç sertifikasını (`trino-tls`) sertifika
  yöneticisi (cert-manager) bitişinden önce kendiliğinden yeniler. Durumuna ve son yenileme
  isteğine bakın:

  `[bastion]`

  ```bash
  oc -n "$LAKEHOUSE_NS" get certificate trino-tls \
    -o jsonpath='{range .status.conditions[*]}{.type}={.status} {.reason}{"\n"}{end}{.status.notAfter} {.status.renewalTime}{"\n"}'
  oc -n "$LAKEHOUSE_NS" get certificaterequest
  ```

  **Beklenen çıktı** (sağlıklı hâl; ilk satır `Ready=True`, ikinci satırda bitiş ve
  yenileme tarihleri):

  ```text
  Ready=True Ready
  2027-02-06T16:02:51Z 2027-01-22T16:02:51Z
  NAME          APPROVED   DENIED   READY   ISSUER         REQUESTER                                         AGE
  trino-tls-2   True                True    lakehouse-ca   system:serviceaccount:cert-manager:cert-manager   3d1h
  ```

  `Ready=False` görürseniz ikinci sözcük nedeni söyler. Yenileme isteği `READY` `False`
  kalıyorsa sertifikayı imzalayan `lakehouse-ca` Secret'ı bozulmuş ya da süresi dolmuş
  olabilir (§7, `lakehouse-ca` maddesi). Sertifika yöneticisinin kendi günlüğü:
  `oc -n cert-manager logs deploy/cert-manager --tail=50`.

- **Polaris'e bakmak.** Sağlık ucu `:8182/q/health`, API `:8181`. Katalog kurulum işinin
  günlüğü: `oc -n "$LAKEHOUSE_NS" logs job/polaris-bootstrap`.

- **Pod hiç oluşmuyor: `unable to validate against any security context constraint`.**
  OpenShift, sabit bir kullanıcı numarası (UID) isteyen pod'ları reddeder. Ürünün ayar
  dosyaları bunu bütün bileşenlerde kapatır. Bu hatayı görüyorsanız ya ürün dosyaları
  (`platform/values/*.yaml`, `platform/apps/*.yaml`) değiştirilmiştir ya da sonradan
  eklenen bir bileşen sabit UID istiyordur. Hangi değerin reddedildiğini olaylarda
  görürsünüz:

  `[bastion]`

  ```bash
  oc get events -A --field-selector reason=FailedCreate | grep -i "security context"
  ```

  `runAsUser: Invalid value: 1000: must be in the ranges` gibi bir satır, o bileşenin
  `runAsUser`, `runAsGroup` ya da `fsGroup` ayarının kaldırılması gerektiğini gösterir.

<a id="polaris-403"></a>
### 5.1 Polaris / S3 403 (geçici kimlikler)

Belirti: Spark ya da Connect günlüğünde
`ForbiddenException: The Access Key Id you provided does not exist in our records` (403).

- S3 sisteminizde geçici kimlik servisi yoksa (`s3.vendedCredentials: false`) bu hata
  görülmez; kimlik doğrudan `s3-creds` Secret'ından gelir.
- Geçici kimlikler açıkken neden, Polaris'in kimlikleri önbellekte tuttuğu süre ile
  kimliğin geçerlilik süresi arasındaki farktır. Hızlı çözüm Polaris'i yeniden
  başlatmaktır:

  `[bastion]`

  ```bash
  oc -n "$LAKEHOUSE_NS" rollout restart deploy/polaris
  ```

- Kalıcı çözüm geçici kimlikleri kapatmaktır: `platform/values/site/glue.yaml` içinde
  `s3.vendedCredentials: false` ve `platform/polaris/setup.yaml` içinde aynı ayarın
  karşılığı ([30-kurulum](../30-kurulum.md) §4.3).
- **Karıştırmayın:** tablo silerken alınan "veriyle birlikte silme kapalı" 403'ü bir
  katalog ayarı eksikliğidir, kimlik sorunu değildir
  ([kaynak-veya-tablo-silme.md](kaynak-veya-tablo-silme.md) Adım 7).

---

<a id="boru-hatti"></a>
## 6. Veri akışı (Bronze / Silver / nginx)

- **Bronze boş kalıyor:** bağlayıcının `READY` sütunu `True` mu, Kafka konusu oluşmuş mu,
  yazıcı görevinin hata mesajı (`trace`) ne diyor? İlk yazma, bağlayıcı açıldıktan sonra
  10–15 dakika sürebilir. Bronze tablosu var ama boşsa → [§4.2](#sink-bos-tablo).
- **`silver-merge` `SchemaConflict` veriyor:** Silver'daki bir kolonun tipi güvenle
  genişletilemiyor →
  [mevcut-kaynaga-tablo-ekleme.md](mevcut-kaynaga-tablo-ekleme.md).
- **`mongo-bronze` `KAFKA_JAAS` hatası:** `spark` Kafka kullanıcısı yalnız bir MongoDB
  kaynağı tanımlıyken vardır.
- **`mongo-bronze` bellek hatası:** uzun bir kesintiden sonra birikmiş kayıtlar sürücü
  belleğine sığmıyor; `spark.driver.memory` değerini artırın.
- **Karantina tablosu doluyor:** MongoDB Bronze'unda bozuk, anahtarsız ya da zaman
  damgasız kayıtlar `__quarantine` tablosuna yazılır; `reason` sütunu nedeni söyler.
  Ayrıntı ve temizleme: [yeni-kaynak-ve-pipeline.md](yeni-kaynak-ve-pipeline.md) §3.3 ve
  §9.2.
- **`nginx.dlq` doluyor:** ajanın gönderdiği kayıt JSON değildir ya da zaman alanı
  okunamıyordur; çoğunlukla ajanın günlük biçimi kurumun nginx biçimine uymuyordur
  ([yeni-kaynak-ve-pipeline.md](yeni-kaynak-ve-pipeline.md) §10).
- **Hatalı kayıt konusu her hatayı almaz:** yalnız dönüştürme aşamasındaki hatalı kayıtlar
  `.dlq` konusuna gider. Tabloya yazma sırasında oluşan bir hata görevi durdurur; bu
  yüzden [§4.1](#connect) alarmı `critical`'dır.

<a id="ssa-suspend"></a>
### 6.1 Askıda kalan zamanlı Spark işi

Silver güncellemesi (`silver-merge`), MongoDB aktarımı (`mongo-bronze`) ve üç bakım işi
(`maint-...`) kümede **zamanlı Spark işi** olarak tanımlıdır; her biri kendi saatinde
kendiliğinden çalışır. Her işin bir **askı** ayarı vardır (`spec.suspend`). Ayar `true`
olursa iş hiç çalışmaz: hata vermez, pod açmaz, alarm da hemen gelmez. Veri sessizce
eskir.

**Ne zaman olur?** Kabul testi (`scripts/acceptance.sh`) bir işi elle bir kez çalıştırmak
için önce zamanlı hâlini askıya alır ve bitince geri açar. Betik yarıda kesilirse
(bilgisayar kapandı, bağlantı koptu) geri açma adımı çalışmaz ve işler askıda kalır.
ArgoCD bunu düzeltmez, çünkü askı ayarı Git'teki tanımda yoktur.

**Kontrol:**

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" get scheduledsparkapplication \
  -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.spec.suspend}{"\t"}{.status.lastRun}{"\n"}{end}'
```

**Beklenen çıktı** (sütunlar: ad, askı, son çalışma; kendi zamanlı işleriniz de listede
görünür):

```text
gold-kabul	false	2026-11-11T16:50:00Z
maint-compact	false	2026-11-11T12:00:00Z
maint-expire-orphan-ttl	false	2026-11-11T03:30:00Z
maint-position-deletes	false	2026-11-11T17:00:00Z
mongo-bronze	false	2026-11-11T17:15:00Z
silver-merge	false	2026-11-11T17:15:00Z
```

İkinci sütun boş ya da `false` olmalıdır; `true` olan satır askıdadır. Üçüncü sütun
`null` ise işin saati henüz gelmemiştir (ör. günde bir çalışan bir iş).

**Geri açma:** aşağıdaki komut bütün zamanlı işlerin askısını kaldırır (kendi işleriniz
dahil; kabul testinin geçici `e2e-` işleri hariç):

`[bastion]`

```bash
for s in $(oc -n "$LAKEHOUSE_NS" get scheduledsparkapplication -o name); do
  case "$s" in */e2e-*) continue ;; esac
  oc -n "$LAKEHOUSE_NS" patch "$s" --type=merge -p '{"spec":{"suspend":false}}'
done
```

**Beklenen çıktı** (her iş için bir satır; zaten açık olan iş `(no change)` yazar):

```text
scheduledsparkapplication.sparkoperator.k8s.io/maint-compact patched (no change)
scheduledsparkapplication.sparkoperator.k8s.io/maint-expire-orphan-ttl patched (no change)
scheduledsparkapplication.sparkoperator.k8s.io/maint-position-deletes patched (no change)
scheduledsparkapplication.sparkoperator.k8s.io/mongo-bronze patched
scheduledsparkapplication.sparkoperator.k8s.io/silver-merge patched (no change)
```

Askıdan çıkan iş kaçırdığı çalışmayı birkaç saniye içinde yapar; bir sonraki saati
beklemek gerekmez. Kontrol komutunu tekrar çalıştırıp ikinci sütunun `false` olduğunu
doğrulayın.

**Ters giderse:** bir işin zamanlamasını (`cron`) değiştirmek bir sonraki çalışma
zamanını hemen yeniden hesaplatmaz. İşin yeni saatine hemen geçmesini istiyorsanız
değişikliği Git'e gönderin ve eşitlemeyi beklemeden uygulatın
([değişiklik nasıl uygulanır](degisiklik-nasil-uygulanir.md) §5.1).

<a id="maven"></a>
### 6.2 Maven kütüphaneleri (küme içi önbellek ve kurum aynası)

Spark işleri her çalışmada Iceberg ve Kafka kütüphanelerini **küme içindeki Maven
önbelleğinden** (`maven-proxy` pod'u) alır. Önbellek bir kütüphaneyi Maven Central'dan
yalnız ilk istendiğinde indirir ve diskinde saklar; sonraki çalışmalar internete çıkmaz.
İnternet erişimi kesilse de işler çalışmaya devam eder. Ürün yükseltmesinde Iceberg ya da
Spark sürümü değişirse yeni sürüm ilk çalışmada bir kez indirilir; elle bir şey yapılmaz.

Önbelleğin çalıştığını ve işlerin onu kullandığını görmek için:

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" get pod -l app=maven-proxy
oc -n "$LAKEHOUSE_NS" exec deploy/maven-proxy -- du -sh /app/data/repositories/releases
DRIVER=$(oc -n "$LAKEHOUSE_NS" get pod -o name | grep -E 'silver-merge-[0-9]+-driver' | tail -1)
oc -n "$LAKEHOUSE_NS" logs "$DRIVER" | grep "in lakehouse-maven" | head -3
```

**Beklenen çıktı** (pod adı, yaşı ve disk kullanımı sizde farklıdır):

```text
NAME                           READY   STATUS    RESTARTS   AGE
maven-proxy-646879d475-z4sm2   1/1     Running   0          5h1m
233M	/app/data/repositories/releases
	found org.apache.iceberg#iceberg-spark-runtime-4.1_2.13;1.11.0 in lakehouse-maven
	found org.apache.iceberg#iceberg-aws-bundle;1.11.0 in lakehouse-maven
```

**Kurumun kendi Maven aynası varsa** (Nexus, Artifactory; kapalı ağda zorunlu), önbelleğin
kaynağını ona çevirin. `platform/values/site/glue.yaml` dosyasına ekleyin:

```yaml
mavenProxy:
  upstream: https://nexus.kurum.local/repository/maven-public
```

Önbelleği hiç kullanmadan Spark işlerini doğrudan kurum aynasına da bağlayabilirsiniz:
`mavenProxy.enabled: false` yapın ve `spark.ivySettingsXml` ayarına aynanın
`ivysettings.xml` içeriğini yazın
([90-referans/values-anahtarlari.md](../90-referans/values-anahtarlari.md) §1). Bu
durumda kurum aynasına her çalışmada erişilir. Kendi `ivysettings.xml` içeriğinize
aşağıdaki çıktıdaki `<caches lockStrategy="artifact-lock-nio"/>` satırını da koyun: aynı
anda başlayan Spark işleri ortak bir kütüphane klasörü kullanır ve bu satır olmadan
bazıları `unresolved dependency` ile düşebilir.

İki yolda da ürün, Spark işlerinin okuduğu `spark-ivysettings` ConfigMap'ini üretir:

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" get configmap spark-ivysettings -o jsonpath='{.data.ivysettings\.xml}{"\n"}'
```

**Beklenen çıktı** (varsayılan, küme içi önbellek):

```text
<ivysettings>
  <settings defaultResolver="lakehouse-maven"/>
  <caches lockStrategy="artifact-lock-nio"/>
  <resolvers>
    <ibiblio name="lakehouse-maven" m2compatible="true" root="http://maven-proxy.lakehouse.svc:8080/releases/"/>
  </resolvers>
</ivysettings>
```

**Ters giderse:**

- **Spark işi sürücü pod'u açılmadan `FAILED` oluyor ve hata mesajında `unresolved
  dependency` geçiyor.** İstenen kütüphane önbellekte yoktur ve önbellek onu
  kaynağından indirememiştir. Bu çoğunlukla bir sürüm yükseltmesinden sonraki ilk
  çalışmada, dış erişim kapalıyken olur. Hata mesajı çalışma nesnesindedir:

  `[bastion]`

  ```bash
  oc -n "$LAKEHOUSE_NS" get sparkapplication | grep FAILED
  APP=silver-merge-1794398400001535062     # yukarıdaki listede FAILED görünen ad
  oc -n "$LAKEHOUSE_NS" get sparkapplication "$APP" \
    -o jsonpath='{.status.applicationState.errorMessage}' | grep unresolved
  ```

  **Beklenen çıktı** (bu durumda):

  ```text
  Exception in thread "main" java.lang.RuntimeException: [unresolved dependency: org.apache.iceberg#iceberg-core;1.10.0: not found]
  ```

  Önbellek pod'undan kaynağa erişimi sınayın:

  `[bastion]`

  ```bash
  oc -n "$LAKEHOUSE_NS" exec deploy/maven-proxy -- \
    curl -s -o /dev/null -w '%{http_code}\n' --max-time 10 https://repo1.maven.org/maven2/
  ```

  `200` dışında bir değer (ör. `000`) erişim olmadığını gösterir. Güvenlik duvarında Maven
  Central'ı açın ya da `mavenProxy.upstream` ile kurum aynasını tanıtın. Erişim geldikten
  sonraki ilk çalışma kütüphaneyi indirir.
- **`maven-proxy` pod'u çalışmıyor** (`Pending`, `CrashLoopBackOff`): Spark işleri de
  kütüphanelerini alamaz. Pod'un olaylarına bakın:
  `oc -n "$LAKEHOUSE_NS" describe pod -l app=maven-proxy | tail -20`. `Pending` çoğunlukla
  diskin (`maven-proxy-data`) bağlanamadığını gösterir.
- **`spark-ivysettings` ConfigMap'i yok:** hem önbellek hem kurum aynası kapalıdır ya da
  eşitleme henüz bitmemiştir; `oc -n "$ARGOCD_NS" get application glue` ile bakın.

Aynı türden başka dış erişim ihtiyaçları da vardır: Zeppelin, Trino sürücüsünü ilk
açılışta Maven Central'dan indirir; JupyterHub'da ek paket kurulumu ve dbt örneği PyPI'a
çıkar. Kapalı ağda iç PyPI aynası da gerekir
([20-on-kosullar](../20-on-kosullar.md) madde 10).

---

<a id="kullanici-yuzu"></a>
## 7. Kullanıcı araçları (Trino, Superset, JupyterHub, Zeppelin)

- **Trino pod'u `CreateContainerConfigError`:** `polaris-trino` Secret'ı yoktur; katalog
  kurulum betiği çalıştırılmamıştır ([40-kurulum-sonrasi](../40-kurulum-sonrasi.md) §2).

- **Trino `401` ya da `Authentication failed`:** Trino'nun 8080 portu yalnız küme içi
  trafik içindir ve kimlik sormaz. İstemciler **8443 (HTTPS)** portunu kullanmalı ve
  `lakehouse-ca` sertifikasına güvenmelidir. Oturum jetonu reddediliyorsa jetonun hedefinde
  `trino` yoktur; Keycloak'taki `trino` istemcisinin ayarına bakın
  ([30-kurulum](../30-kurulum.md) §8).

- **Trino `Access Denied`:** erişim kuralları yukarıdan aşağı okunur ve ilk eşleşen kural
  geçerlidir. Kullanıcının grubu görünmüyorsa Trino AD gruplarını okuyamıyordur
  ([kullanici-ve-yetki.md](kullanici-ve-yetki.md) §5).

- **Superset açılmıyor (`phase: Initializing`), veritabanı hazırlık işi düşüyor:** hazırlık
  işi veritabanı açılmadan çalışmıştır. Ürün bunu yüksek deneme sayısıyla karşılar. Bakmak
  için:

  `[bastion]`

  ```bash
  oc -n "$LAKEHOUSE_NS" describe superset superset | tail -30
  oc -n "$LAKEHOUSE_NS" logs job/superset-migrate --tail=30
  ```

  İş kalıcı olarak düştüyse `Superset` nesnesinde `spec.lifecycle.migrate.trigger`
  değerini değiştirerek yeniden tetikleyin.

- **`superset-migrate` artığı pod'lar:** yeniden denemeler `Error` ya da `Completed`
  durumunda pod'lar bırakır; zararsızdır. Temizlemek için:

  `[bastion]`

  ```bash
  oc -n "$LAKEHOUSE_NS" delete pod -l job-name=superset-migrate \
    --field-selector=status.phase!=Running
  ```

- **JupyterHub not defteri açılmıyor (zaman aşımı):** imaj ilk kez çekilirken 4–5 dakika
  sürer; ürünün bekleme süresi buna göre ayarlıdır, bekleyin. Not defterleri yalnız Trino
  (8443), Polaris (8181) ve dış ağa çıkabilir
  ([90-referans/port-ve-servisler.md](../90-referans/port-ve-servisler.md) §5).

- **Zeppelin "yorumlayıcı hazır değil, bağımlılıklar iniyor":** Zeppelin ilk açılışta Trino
  sürücüsünü Maven Central'dan indirir (1 dakika kadar; sonra diskte kalır). Bekleyin.
  Kapalı ağda bu adım başarısız olur; sürücünün iç aynadan inmesi gerekir ([§6.2](#maven)).

- **Zeppelin yorumlayıcı ayarı ya da parolası değişmiyor:** `zeppelin-interpreter` Secret'ı
  yalnız ilk açılışta kullanılan bir başlangıç değeridir; diskte ayar dosyası varsa
  etkisizdir. Ayarı Zeppelin arayüzünden değiştirin
  ([40-kurulum-sonrasi](../40-kurulum-sonrasi.md) §5.6).

- **Zeppelin'e girişte not defteri listesi 401 veriyor:** kullanıcı üç `lakehouse-*`
  grubunun hiçbirinde değildir ([kullanici-ve-yetki.md](kullanici-ve-yetki.md) §3).

- **Giriş döngüsü ("Sign in with Keycloak" aynı sayfaya dönüyor):** uygulama Keycloak'la
  konuşamıyordur. Günlükte nedenine bakın:

  `[bastion]`

  ```bash
  oc -n "$LAKEHOUSE_NS" logs deploy/superset-web-server --tail=200 | grep -i "oauth"
  ```

  `CERTIFICATE_VERIFY_FAILED` ile birlikte `keycloak-...apps...` adresi görüyorsanız
  uygulama Keycloak'a dış adresten gitmeye çalışıyordur; ürün bu çağrıları küme içi adrese
  (`keycloak-service.lakehouse.svc:8080`) yönlendirir. Keycloak nesnesinde
  `spec.hostname.backchannelDynamic: true` olduğunu ve site dosyalarının değiştirilmediğini
  doğrulayın (`scripts/check-site.sh`).

- **Keycloak girişi `Invalid username or password`, Keycloak günlüğünde `User returned from
  LDAP has null username`:** Keycloak'ta AD kullanıcı adı eşlemesi eksiktir. Ürünün realm
  tanımı bu eşlemeyi içerir; realm'i yenileyin ([30-kurulum](../30-kurulum.md) §8).

- **Zeppelin'e hiç kimse giremiyor, günlükte `LDAP: error code 49 ... data 52e`:** Zeppelin
  servis hesabıyla AD'ye bağlanamıyordur. En sık neden: `zeppelin-shiro` Secret'ında
  `systemUsername` satırına tam ad (DN) yazılmıştır; kısa kullanıcı adı (sAMAccountName)
  olmalıdır ([30-kurulum](../30-kurulum.md) §5.9). Satırı görmek için:

  `[bastion]`

  ```bash
  oc -n "$LAKEHOUSE_NS" get secret zeppelin-shiro -o jsonpath='{.data.shiro\.ini}' \
    | base64 -d | grep systemUsername
  ```

  Düzeltmek için önce `oc -n "$LAKEHOUSE_NS" delete secret zeppelin-shiro` ile Secret'ı
  silin, [30-kurulum](../30-kurulum.md) §5.9'u `LDAP_BIND_USER` kısa adla doluyken tekrar
  çalıştırın, sonra `oc -n "$LAKEHOUSE_NS" rollout restart deploy/zeppelin`.

- **Not defterinden S3'e bağlanırken zaman aşımı (`TimeoutError`):** S3 iç ağdadır ve not
  defterlerinin ağ politikası iç ağ adreslerini kapatır. `platform/values/site/glue.yaml`
  içindeki `s3.egressCidrs` ayarı S3'ün IP adreslerini içermelidir
  ([30-kurulum](../30-kurulum.md) §4.1). Politikanın kümede olduğunu doğrulayın:
  `oc -n "$LAKEHOUSE_NS" get networkpolicy notebook-s3-egress -o yaml`.

- **Analist ya da kullanıcı Trino'da `Access Denied: Cannot access catalog lakehouse`
  alıyor, Superset ve Zeppelin çalışıyor:** Trino kullanıcının AD gruplarını
  okuyamıyordur. Erişim kuralları gruplara bağlı olduğu için grupsuz kullanıcı hiçbir şey
  göremez. Superset ve Zeppelin servis hesabıyla bağlandığı için etkilenmez. Bakmak için:

  `[bastion]`

  ```bash
  oc -n "$LAKEHOUSE_NS" logs deploy/trino-coordinator -c trino-coordinator --since=1h \
    | grep -c "SSLContext was not set"
  grep -n "ldap.ssl.truststore.path" platform/values/site/trino.yaml
  ```

  Birinci komut `0`'dan büyük bir sayı veriyorsa ve ikinci komut bir satır gösteriyorsa
  site dosyanızda `ldap.ssl.truststore.path` satırı vardır. Bu satırla Trino'nun AD
  bağlantısı aralıklı olarak düşer. Satırı silin, değişikliği Git'e gönderin; AD kök
  sertifikası Trino'ya ürünün kendi ayarıyla eklenir ([30-kurulum](../30-kurulum.md)
  §5.13). Trino yeniden açıldıktan sonra
  `oc -n "$LAKEHOUSE_NS" logs deploy/trino-coordinator -c s3-truststore` çıktısında
  `AD CA eklendi` satırı görünmelidir. Günlükte başka bir LDAP hatası varsa (`PKIX`,
  `Invalid credentials`) sorun AD kök sertifikası ya da bağlanma hesabıdır.

- **Not defterinden Trino'ya bağlanırken `CA cert does not include key usage extension`:**
  `lakehouse-ca` kök sertifikası `Key Usage` uzantısı olmadan üretilmiştir; güncel Python
  bu sertifikayı kabul etmez. Bakmak için:

  `[bastion]`

  ```bash
  oc -n "$LAKEHOUSE_NS" get secret lakehouse-ca -o jsonpath='{.data.tls\.crt}' \
    | base64 -d | openssl x509 -noout -ext keyUsage
  ```

  Çıktı boşsa sertifikayı yenileyin. Adımlar bastion'da, deponun kök klasöründe
  çalıştırılır; kullanıcı oturumları birkaç dakika kesilir:

  1. Yeni sertifikayı [30-kurulum](../30-kurulum.md) §5.11'deki `openssl req` komutuyla
     üretin ve Secret'ı değiştirin:

     ```bash
     oc -n "$LAKEHOUSE_NS" create secret tls lakehouse-ca \
       --cert=/tmp/ca.crt --key=/tmp/ca.key --dry-run=client -o yaml | oc apply -f -
     oc -n "$LAKEHOUSE_NS" label secret lakehouse-ca \
       lakehouse.io/yonetim=manual-30-kurulum-5.11 --overwrite
     ```

  2. Trino'nun sunucu sertifikasını yeni kök sertifikayla yeniden imzalatın:
     `oc -n "$LAKEHOUSE_NS" delete secret trino-tls`. 20 saniye kadar sonra
     `oc -n "$LAKEHOUSE_NS" get certificate trino-tls` `True` göstermelidir.
  3. `platform/values/site/glue.yaml` içindeki `tls.caBundle` değerini yeni sertifikanın
     içeriğiyle değiştirin (`caBundle: "..."` satırının tamamı) ve Git'e gönderin. ArgoCD
     Trino adresinin ayarını günceller.
  4. Sertifikayı bellekte tutan bileşenleri yeniden başlatın:

     ```bash
     oc -n "$LAKEHOUSE_NS" rollout restart \
       deploy/trino-coordinator deploy/superset-web-server deploy/zeppelin
     ```

     Not defterleri sertifikayı her bağlantıda yeniden okur; yeniden başlatmak gerekmez.
  5. Eski `~/lakehouse-ca.crt` dosyasını yenisiyle değiştirin ve özel anahtarı silin:
     `shred -u /tmp/ca.key`.

---

<a id="dr"></a>
## 8. İzleme ve yedekler

- **İzleme hedefi `down` ya da metrik yok.** OpenShift konsolunda **Observe → Targets**
  sayfasına bakın. Komut satırından:

  `[bastion]`

  ```bash
  oc -n "$LAKEHOUSE_NS" get podmonitor,servicemonitor,prometheusrule
  ```

  İzleme kapsamı bilerek dardır: Kafka ve Connect, okunmayan kayıt sayısı (lag),
  spark-operator ve Polaris. Trino, Superset, JupyterHub ve Zeppelin için uygulama ölçümü
  toplanmaz; bu hedeflerin listede olmaması hata değildir
  ([izleme-ve-alarmlar.md](izleme-ve-alarmlar.md) §2).

- **Alarm geliyor ama bildirim ulaşmıyor.** Bildirimi OpenShift'in Alertmanager'ı
  gönderir; bildirim yönlendirmesi tanımlı değilse alarm kümede görünür ama kimseye
  gitmez ([izleme-ve-alarmlar.md](izleme-ve-alarmlar.md) §5).

- **Veritabanı yedeği `ContinuousArchiving=False`.**

  `[bastion]`

  ```bash
  oc -n "$LAKEHOUSE_NS" get cluster polaris-db -o jsonpath='{.status.conditions}' | jq
  oc -n "$LAKEHOUSE_NS" logs polaris-db-1 -c plugin-barman-cloud --tail=50
  oc -n "$LAKEHOUSE_NS" get objectstore lakehouse-backups -o yaml | head -30
  ```

  Sık nedenler: yedek bucket'ı yok, `backup-s3-creds` Secret'ı yanlış ya da eksik, S3 adresi
  yanlış, S3'ün kök sertifikası verilmemiş (günlükte `SSL validation failed`) ya da yedek
  eklentisinin pod'u çalışmıyor. Bucket yaratıldıktan sonra `ScheduledBackup` nesnelerini
  silip yeniden yaratmak ilk yedeği hemen başlatır
  ([yedek-ve-geri-donus.md](yedek-ve-geri-donus.md) §3).

- **S3'e bağlanırken `CERTIFICATE_VERIFY_FAILED` ya da `PKIX path building failed`.** S3'ün
  sertifikası kurumun kendi kök sertifikasıyla imzalıdır ve bu kök sertifika ürüne
  verilmemiştir. `s3-ca` Secret'ını yaratın ve site ayarına `s3.caSecret: "s3-ca"` yazın
  ([30-kurulum](../30-kurulum.md) §4.1 ve §5.14). Bunu kurulumdan **sonra** yaptıysanız
  bileşenler sertifikayı ancak yeniden başlayınca alır:

  `[bastion]`

  ```bash
  T=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  for c in polaris-db keycloak-db superset-db; do
    oc -n "$LAKEHOUSE_NS" annotate cluster.postgresql.cnpg.io "$c" \
      kubectl.kubernetes.io/restartedAt="$T" --overwrite
  done
  oc -n "$LAKEHOUSE_NS" rollout restart deploy/polaris deploy/trino-coordinator deploy/trino-worker
  ```

  Kafka Connect, `s3.caSecret` değişince kendiliğinden yeniden başlar. Spark işleri her
  çalışmada yeni pod açtığı için ek bir şey gerekmez. Açık not defterleri, kullanıcı
  sunucusunu kapatıp yeniden açınca yeni ayarı alır.

- **Velero yedeği `PartiallyFailed`.**

  `[bastion]`

  ```bash
  oc -n openshift-adp describe backups.velero.io lakehouse-daily-20260924081157 | tail -40
  oc -n openshift-adp logs deploy/velero --tail=100
  ```

  Yedek adını `oc -n openshift-adp get backups.velero.io` ile bulun. Hata satırları hangi
  nesnenin ya da diskin yedeklenemediğini söyler; diskler için depolama sınıfının anlık
  görüntü (snapshot) desteğini kontrol edin
  ([yedek-ve-geri-donus.md](yedek-ve-geri-donus.md) §6).

- **Yedekte hiç disk yedeği yok:** `velero.excludedResources` listesinde `pods` varsa
  disk yedeği hiç başlamaz; bu listede `pods` **olmamalıdır**.

- **`oc get backup` yanlış nesneyi gösteriyor.** Kümede hem PostgreSQL'in hem Velero'nun
  `Backup` adlı nesnesi vardır; kısa ad sessizce PostgreSQL'inkini seçer. Velero için her
  zaman tam adı yazın: `backups.velero.io`. Bir Velero yedeğini silmek S3'teki yedek
  dosyalarını temizlemez ([yedek-ve-geri-donus.md](yedek-ve-geri-donus.md) §6.4).

---

<a id="loki"></a>
## 9. Günlükler (Loki)

Günlüklerin toplanması OpenShift platformunun işidir; lakehouse kendi Loki'sini kurmaz ve
Loki olmadan da çalışır ([20-on-kosullar](../20-on-kosullar.md) madde 9.3).

OpenShift'te bunun yolu **OpenShift Logging + Loki Operator**'dür: `LokiStack` kurulur,
`ClusterLogForwarder` ile toplama açılır ve günlükler konsolun **Observe → Logs**
sayfasından sorgulanır. Lakehouse açısından yapılacak tek şey `$LAKEHOUSE_NS` ad alanının
toplanan ad alanları arasında olduğunu doğrulamaktır:

`[bastion]`

```bash
oc -n openshift-logging get lokistack,clusterlogforwarder
```

**Beklenen çıktı** (örnek): `LokiStack` `Ready` ve `ClusterLogForwarder` girdisi
`lakehouse` ad alanını kapsıyor.

Sık kullanılan sorgular (konsolun **Logs** sayfasına yapıştırılır):

```text
{namespace="lakehouse", pod=~"connect-connect-.*"} |= "ERROR"
{namespace="lakehouse", pod=~"silver-merge-.*-driver"} |~ "MERGE_OK|FALLBACK|Exception"
{namespace="lakehouse", pod=~"trino-coordinator-.*"} |= "Query failed"
{namespace="lakehouse", container="plugin-barman-cloud"} |= "archive"
```

Canlı sorunlarda `oc logs` her zaman en hızlı yoldur. Loki, pod silindikten sonra
günlüğüne bakmak ve farklı bileşenlerin günlüklerini aynı zaman aralığında karşılaştırmak
için gereklidir; Spark sürücü pod'ları iş bitince silinir.

**Ters giderse:** `the server doesn't have a resource type "lokistack"` görürseniz
OpenShift Logging kurulu değildir. Bu bir hata değildir; günlükleri `oc logs` ile okuyun.

---

## Kontrol listesi

- [ ] Belirti §3'teki tabloda bir satırla eşleşti.
- [ ] §2'deki dört komut çalıştırıldı ve çıktıları kaydedildi.
- [ ] Alarm geldiyse ilgili bölümün "Ne bakılır" komutları çalıştırıldı.
- [ ] Neden giderildikten sonra `FAILED` Spark çalışmaları silindi (alarm ancak o zaman
      kapanır).
- [ ] Kabul testinden sonra zamanlı işlerin askıda olmadığı doğrulandı (§6.1).
- [ ] Kalıcı düzeltme kümede değil, Git'te yapıldı
      ([değişiklik nasıl uygulanır](degisiklik-nasil-uygulanir.md)).

## Sonraki bölüm

Alarm eşiklerinin anlamı ve nereden değiştirildikleri:
[izleme-ve-alarmlar.md](izleme-ve-alarmlar.md). Her sabah ve her hafta yapılacak
kontroller: [gunluk-haftalik-kontroller.md](gunluk-haftalik-kontroller.md). Yedek ve geri
dönüş: [yedek-ve-geri-donus.md](yedek-ve-geri-donus.md).
