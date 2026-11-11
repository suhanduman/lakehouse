# İzleme ve alarmlar

Bu sayfa lakehouse'un neyi izlediğini ve neyi bilerek izlemediğini, altı alarmın ne
anlama geldiğini ve alarm gelince ne yapılacağını, eşiklerin nasıl değiştirileceğini ve
bildirimlerin nereye gittiğini anlatır.

**Ne kadar sürer:** ilk okuma 20 dakika; bir eşiği değiştirmek 10 dakika.

**Hangi yetkiler gerekir:** `$LAKEHOUSE_NS` ad alanında okuma ve OpenShift konsolunda
**Observe** menüsüne erişim. Eşik değiştirmek için Git deposunda `main` dalına yazma.

**Komutlar nerede çalışır:** `[bastion]` — `oc login` ile kümeye bağlı yönetim makinesi.

> **Lakehouse kendi Prometheus'unu kurmaz.** Metrikleri OpenShift'in **kullanıcı iş yükü
> izlemesi** toplar. Bu özellik kapalıysa izleme nesneleri kümede durur ama kimse okumaz
> ve altı alarmın hiçbiri çalışmaz ([20-on-kosullar](../20-on-kosullar.md) madde 9.1).

---

## 1. Değişkenleri yükleyin

Her yeni terminalde bir kez çalıştırın.

`[bastion]`

```bash
cd ~/lakehouse
set -a; . install/lakehouse.env; set +a
echo "lakehouse=$LAKEHOUSE_NS"
```

**Beklenen çıktı** (kendi değerinizle):

```text
lakehouse=lakehouse
```

**Ters giderse:** eşittir işaretinden sonrası boşsa dosya yüklenmemiştir
([30-kurulum](../30-kurulum.md) §1).

---

## 2. Neler izleniyor, neler izlenmiyor

İzleme, verinin kaynaktan tablolara akışına odaklanır. Kullanıcı araçlarının
(Trino, Superset, JupyterHub, Zeppelin) metrikleri bilerek toplanmaz: bunlardan alarm
üretilmez, yalnız hedef listesi kalabalıklaşırdı.

| Toplanır | Toplanmaz |
|---|---|
| Kafka broker'ları ve Kafka Connect işçileri | Trino |
| Tüketici gecikmesi (verinin ne kadar geride olduğu) | Superset |
| Spark işlerinin sayaçları ve süreleri | JupyterHub |
| Polaris katalog sunucusu | Zeppelin |

Trino, Superset, JupyterHub ve Zeppelin'in hedef listesinde görünmemesi bir hata değildir.

Lakehouse'un kümeye yazdığı izleme nesnelerini görmek için:

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" get podmonitor,servicemonitor,prometheusrule
```

**Beklenen çıktı** (`AGE` sütunu sizde farklıdır):

```text
NAME                                                         AGE
podmonitor.monitoring.coreos.com/kafka-resources-metrics     3d5h
podmonitor.monitoring.coreos.com/spark-operator-podmonitor   3d5h

NAME                                           AGE
servicemonitor.monitoring.coreos.com/polaris   3d5h

NAME                                             AGE
prometheusrule.monitoring.coreos.com/lakehouse   3d5h
```

- `kafka-resources-metrics`, Kafka broker'larını ve Kafka Connect işçilerini birlikte
  toplar. Connect'in her işçisi (varsayılan olarak iki) ayrı bir hedeftir.
- `spark-operator-podmonitor`, Spark işlerinin sayaçlarını toplar.
- `polaris`, katalog sunucusunu toplar.
- `lakehouse` adlı tek kural nesnesinin içinde altı alarm vardır (§3).

**Ters giderse:**

- `No resources found` görürseniz izleme kapatılmıştır (`monitoring.enabled`, §4) ya da
  ArgoCD değişikliği henüz uygulamamıştır.
- `the server doesn't have a resource type "podmonitor"` görürseniz kümede kullanıcı iş
  yükü izlemesi hiç açılmamıştır ([20-on-kosullar](../20-on-kosullar.md) madde 9.1).

### 2.1 Metriklerin toplandığını doğrulayın

Konsolda **Observe → Targets** sayfasını açın ve ad alanı süzgecine `$LAKEHOUSE_NS`
değerini yazın. Hedeflerin hepsi **Up** olmalıdır. Aynı bilgiyi komut satırından görmek
için:

`[bastion]`

```bash
TOKEN=$(oc whoami -t)
HOST=$(oc -n openshift-monitoring get route thanos-querier -o jsonpath='{.spec.host}')
curl -sSk -H "Authorization: Bearer $TOKEN" \
  --data-urlencode "query=up{namespace=\"$LAKEHOUSE_NS\"} == 1" "https://$HOST/api/v1/query" \
  | jq -r '.data.result[].metric.job' | sort | uniq -c
```

**Beklenen çıktı** (üç Kafka broker'ı, iki Connect işçisi ve gecikme ölçeri ilk satırda
toplam 6 hedef olarak görünür):

```text
     6 lakehouse/kafka-resources-metrics
     2 lakehouse/spark-operator-podmonitor
     1 polaris-mgmt
```

İki alarm (`LakehouseSilverMergeStale` ve `LakehouseSparkScheduledRunFailed`) Spark
pod'larının durumunu OpenShift'in kendi izlemesinden okur. Bu ölçümleri OpenShift zaten
toplar; ayrıca bir şey kurmanız gerekmez ve yukarıdaki listede görünmezler.

**Ters giderse:**

- Hiç satır dönmüyorsa kullanıcı iş yükü izlemesi kapalıdır.
- Bir satır beklenenden azsa ilgili pod'lardan biri çalışmıyordur:
  `oc -n "$LAKEHOUSE_NS" get pods` ile bakın.

---

## 3. Altı alarm: ne anlama gelir, ne yapılır

| Alarm | Ne demek | Şiddet | Ne yapılır |
|---|---|---|---|
| `LakehouseConnectTaskFailed` | Bir bağlayıcının görevi durdu; o kaynaktan **veri gelmiyor** | critical | [sorun-giderme.md §4.1](sorun-giderme.md#connect) |
| `LakehouseSinkStalled` | Veri Kafka'ya geliyor ama **tablolara yazılmıyor**; birikim eşiği aştı | warning | [sorun-giderme.md §4.2](sorun-giderme.md#sink) |
| `LakehouseSilverMergeStale` | Silver tablolar uzun süredir güncellenmedi; son başarılı birleştirme eski | warning | [sorun-giderme.md §4.3](sorun-giderme.md#silver-merge) |
| `LakehouseSparkScheduledRunFailed` | Lakehouse'un zamanlı Spark işlerinden biri başarısız oldu | warning | [sorun-giderme.md §4.4](sorun-giderme.md#spark) |
| `LakehouseSparkRunTooLong` | Son 6 saatteki Spark işlerinin ortalama süresi eşiği aştı | warning | [sorun-giderme.md §4.5](sorun-giderme.md#spark-duration) |
| `LakehouseDlqRecords` | Son bir saatte bir hatalı kayıt konusuna (`….dlq`) kayıt düştü; bu kayıtlar **tablolara girmedi** | warning | [sorun-giderme.md §4.6](sorun-giderme.md#dlq) |

Her alarm bildiriminde bir `runbook` alanı vardır; bu alan yukarıdaki sorun giderme
başlığını gösterir.

`LakehouseSparkScheduledRunFailed` hakkında iki not:

- Alarm **iş başına bir tanedir.** Aynı işin birkaç çalışması başarısız olsa da tek alarm
  gelir; özette işin adı ve başarısız çalışma sayısı yazar.
- Bir çalışma Spark başlamadan düşerse (ör. kütüphaneler indirilemediğinde) özette iş
  adı yerine `gönderim hatası` yazar.

**Bilinmesi gereken üç sınır:**

1. **`LakehouseSparkScheduledRunFailed` yalnız lakehouse'un kendi işlerini izler**
   (`silver-merge`, `maint-` ile başlayan bakım işleri, `mongo-bronze`). Kendi yazdığınız
   Spark işleri bu alarma girmez; onları izlemek
   [yeni-spark-uygulamasi.md](yeni-spark-uygulamasi.md) sayfasında anlatılır. Tek istisna
   `gönderim hatası`dır: Spark'ın bu sayacı iş adı taşımadığı için bütün işler birlikte
   sayılır.
2. **`LakehouseSparkRunTooLong` bütün işlerin ortalamasına bakar.** Spark operatörü bu
   ölçümü iş başına ayırmaz; tek bir işin uzaması ortalamayı ancak çok uzarsa yükseltir.
3. **Alarmlar veri yokken çalışmaz.** Henüz hiç kaynak ya da Spark işi olmayan bir
   kurulumda alarmlar `inactive` kalır. "Hiç çalışmıyor" durumunu alarmlar değil, haftalık
   kontrol yakalar ([gunluk-haftalik-kontroller.md](gunluk-haftalik-kontroller.md) §3).

### 3.1 Alarm kurallarının sağlığına bakın

Konsolda **Observe → Alerting → Alerting rules** sayfasında ad alanına göre süzün. Bir
sorun yokken altısı da **Inactive** görünür. Komut satırından:

`[bastion]`

```bash
TOKEN=$(oc whoami -t)
HOST=$(oc -n openshift-monitoring get route thanos-querier -o jsonpath='{.spec.host}')
curl -sSk -H "Authorization: Bearer $TOKEN" "https://$HOST/api/v1/rules" \
  | jq -r '.data.groups[] | select(.name=="lakehouse") | .rules[] | "\(.name)\t\(.health)\t\(.state)"'
```

**Beklenen çıktı** (sorun yokken; ikinci sütun `ok` olmalıdır):

```text
LakehouseConnectTaskFailed	ok	inactive
LakehouseDlqRecords	ok	inactive
LakehouseSilverMergeStale	ok	inactive
LakehouseSinkStalled	ok	inactive
LakehouseSparkRunTooLong	ok	inactive
LakehouseSparkScheduledRunFailed	ok	inactive
```

Üçüncü sütun `firing` olan bir alarm şu anda ateşleniyordur; §3'teki tablodan ne
yapılacağına bakın.

**Ters giderse:**

- İkinci sütun `err` ise alarm hesaplanamıyordur; çoğu zaman bir metrik kaynağı
  toplanmıyordur (§2.1).
- Liste boşsa alarm kuralları okunmuyordur; kullanıcı iş yükü izlemesinin açık olduğunu
  doğrulayın.

---

## 4. Eşikleri değiştirmek

Eşikler `platform/values/site/glue.yaml` dosyasındaki `monitoring` ayarlarıyla değişir.

| Ayar | Neyi belirler | Varsayılan | Ne zaman değiştirilir |
|---|---|---|---|
| `monitoring.silverMergeStaleSeconds` | `LakehouseSilverMergeStale` eşiği (saniye) | 2700 (45 dakika) | Silver birleştirme sıklığını değiştirdiyseniz **mutlaka**: eşik, aralığın en az üç katı olmalıdır |
| `monitoring.sinkLagThreshold` | `LakehouseSinkStalled` eşiği (birikmiş kayıt) | 1000 | Normal yükte sürekli ateşleniyorsa; önce gerçek birikimi ölçün |
| `monitoring.sparkRunMaxSeconds` | `LakehouseSparkRunTooLong` eşiği (saniye) | 1800 (30 dakika) | Veri büyüdükçe işler doğal olarak uzar |
| `monitoring.enabled` | İzleme nesnelerinin hiç yaratılıp yaratılmayacağı | `true` | Üretimde kapatılmaz |

Örnek: Silver birleştirmenin eskime eşiğini 90 dakikaya çıkarmak. Dosyayı açın:

`[bastion]`

```bash
git pull --rebase origin main
vi platform/values/site/glue.yaml
```

Dosyada `monitoring:` bölümü yoksa dosyanın sonuna şu iki satırı ekleyin; varsa yalnız
ikinci satırı o bölümün altına ekleyin:

```yaml
monitoring:
  silverMergeStaleSeconds: 5400
```

Kaydedin, denetleyin ve gönderin:

```bash
bash scripts/check-site.sh
git diff
git add platform/values/site/glue.yaml
git commit -m "site: silver-merge eskime eşiği 90 dakika"
git push origin main
```

ArgoCD değişikliği uyguladıktan sonra
([değişiklik nasıl uygulanır](degisiklik-nasil-uygulanir.md) §5) yeni eşiğin kurala
geçtiğini doğrulayın:

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" get prometheusrule lakehouse \
  -o jsonpath='{.spec.groups[0].rules[2].expr}{"\n"}' | tail -c 12
```

**Beklenen çıktı** (satırın sonundaki sayı geçerli eşiktir; değişiklikten önce `2700`,
sonra `5400` yazar):

```text
= 1) > 5400
```

**Ters giderse:** sayı hâlâ eski değerse ArgoCD değişikliği henüz uygulamamıştır
([değişiklik nasıl uygulanır](degisiklik-nasil-uygulanir.md) §5.1).

> **Eşiği yükseltmek de bir çözümdür.** Sürekli ateşlenen ve kimsenin bakmadığı bir alarm,
> hiç olmayan bir alarmdan kötüdür. Ama önce nedenine bakın: birikim gerçekse eşiği değil,
> kaynakları artırmak gerekir ([sorun-giderme.md](sorun-giderme.md) §4.2).

---

## 5. Bildirimler nereye gider

Lakehouse kendisi bildirim göndermez. Alarmlar kümede hesaplanır; e-posta, mesaj ya da
çağrı olarak kime gideceğini **OpenShift'in Alertmanager'ı** belirler. Bir yönlendirme
tanımlı değilse alarm konsolda görünür ama kimseye ulaşmaz.

Kurulumda platform ekibinden şunu isteyin: adı `Lakehouse` ile başlayan alarmları
yakalayan bir yönlendirme kuralı ve bir alıcı (e-posta, mesajlaşma ya da çağrı sistemi).
Kontrol maddesi:
[90-referans/pre-ship-kontrol-listesi.md](../90-referans/pre-ship-kontrol-listesi.md)
madde 2.4.

---

## 6. Günlükler

Metrikler ve günlükler ayrı şeylerdir. Lakehouse günlük toplama sistemi kurmaz;
OpenShift'te günlükler **Observe → Logs** menüsünden okunur ve bu özelliğin kurulumu
platform ekibinin işidir. Kapsam ve sık kullanılan sorgular:
[sorun-giderme.md](sorun-giderme.md) §9.

---

## 7. Tablo ölçümleri burada değildir

Satır sayısı, dosya sayısı, tablo boyutu ve son yazma zamanı gibi **veri** ölçümleri
Prometheus'ta yoktur; tablonun kendi bilgilerinden SQL ile okunur ve "Tablo sağlığı"
panosunda görünür: [veri-metrikleri.md](veri-metrikleri.md).

Tabloların güncel kalıp kalmadığına dair alarm ise buradadır:
`LakehouseSilverMergeStale`.

---

## Kontrol listesi

- [ ] Kullanıcı iş yükü izlemesi açık; izleme nesneleri kümede (§2).
- [ ] **Observe → Targets** listesinde hedefler **Up** (§2.1).
- [ ] Altı alarm **Observe → Alerting** altında görünüyor ve ikinci sütun `ok` (§3.1).
- [ ] Silver birleştirme sıklığı değiştirildiyse `monitoring.silverMergeStaleSeconds` de
      güncellendi (§4).
- [ ] `Lakehouse` alarmları için Alertmanager'da bir yönlendirme ve alıcı var; deneme
      bildirimi ulaştı (§5).
- [ ] Ekip, Trino, Superset, JupyterHub ve Zeppelin'in bilerek izlenmediğini biliyor (§2).

## Sonraki bölüm

Alarm gelince izlenecek adımlar: [sorun-giderme.md](sorun-giderme.md) §4. Alarm
beklemeden yapılan düzenli kontroller:
[gunluk-haftalik-kontroller.md](gunluk-haftalik-kontroller.md). Tablo ölçümleri:
[veri-metrikleri.md](veri-metrikleri.md).
