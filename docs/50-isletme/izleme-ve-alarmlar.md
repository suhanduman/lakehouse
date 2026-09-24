# İzleme ve alarmlar

**Bu bölümde:** ürünün izleme kapsamı (ve **bilerek** dışarıda bıraktıkları), kümeye yazdığı
izleme nesneleri, beş alarm kuralının anlamı ve karşılık gelen eylem, eşiklerin nereden
değiştirileceği, bildirimin nereden gittiği ve OpenShift konsolunda nereye bakılacağı.
**Süre:** ilk okuma 20 dakika; eşik değiştirme 10 dakika (düzenleme + eşitleme).
**Gereken yetki:** `$LAKEHOUSE_NS` ad alanında okuma; konsolda **Observe** sekmesine erişim;
eşik değiştirmek için Git deposunda `main` dalına yazma.
**Nerede çalıştırılır:** `[bastion]` — `oc login` ile kümeye girilmiş yönetim makinesi.

> **Ürün kendi Prometheus'unu kurmaz.** Metrikleri OpenShift'in **kullanıcı iş yükü
> izlemesi** toplar. Kapalıysa izleme nesneleri kümede durur ama kimse okumaz: beş alarmın
> hiçbiri çalışmaz ([20-on-kosullar](../20-on-kosullar.md) madde 9.1).

---

## 1. Değişkenleri yükleyin

`[bastion]`

```bash
cd ~/lakehouse
set -a; . install/lakehouse.env; set +a
echo "lakehouse=$LAKEHOUSE_NS"
```

**Beklenen çıktı** (örnek — kendi değerinizle):

```text
lakehouse=lakehouse
```

**Ters giderse:** boş satır görüyorsanız dosya yüklenmemiştir
([30-kurulum](../30-kurulum.md) §1).

---

## 2. Kapsam: ne ölçülüyor, ne ölçülmüyor

İzleme kapsamı **boru hattı sağlığına** daraltılmıştır. Bu bilinçli bir karardır: gösterim
araçlarının uygulama metriklerini toplamak alarm üretmez, yalnız hedef listesini şişirir.

| Toplanır | Toplanmaz |
|---|---|
| Kafka broker'ları ve Kafka Connect (JMX) | Trino |
| Tüketici grubu gecikmesi (`kafka_consumergroup_lag`) | Superset (bu sürümde metrik ucu **yoktur**) |
| spark-operator (Spark koşu sayaçları ve süreleri) | JupyterHub hub |
| Polaris yönetim ucu | Zeppelin |

Trino, Superset, JupyterHub ve Zeppelin hedeflerinin listede **olmaması hata değildir**.

Kümeye yazılan izleme nesneleri:

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" get podmonitor,servicemonitor,prometheusrule
```

**Beklenen çıktı** (kind kümesinde alınmış gerçek çıktı; `AGE` sütunu sizde farklıdır):

```text
NAME                                                         AGE
podmonitor.monitoring.coreos.com/kafka-resources-metrics     5d16h
podmonitor.monitoring.coreos.com/spark-operator-podmonitor   5d16h

NAME                                           AGE
servicemonitor.monitoring.coreos.com/polaris   5d15h

NAME                                             AGE
prometheusrule.monitoring.coreos.com/lakehouse   5d16h
```

- **İki** toplayıcı ürünün kendisinden gelir: `kafka-resources-metrics` broker'ları ve Kafka
  Connect'i birlikte toplar (tüketici gecikmesini yayan bileşen de aynı seçiciye düşer),
  `spark-operator-podmonitor` ise spark-operator chart'ından gelir.
- `polaris` toplayıcısı Polaris chart'ının kendi nesnesidir.
- `prometheusrule/lakehouse` **tek** nesnedir ve içinde **beş** alarm kuralı vardır.

**Ters giderse:** `No resources found` → `monitoring.enabled` kapalıdır ya da eşitleme
yapılmamıştır. `the server doesn't have a resource type "podmonitor"` → kümede izleme
tanımları yoktur; OpenShift'te bu, kullanıcı iş yükü izlemesinin hiç açılmadığı anlamına
gelir.

### 2.1 Hedeflerin toplandığını doğrulama

Konsolda **Observe → Targets** sayfasını açıp ad alanı süzgecine `$LAKEHOUSE_NS` yazın;
beklenen, toplayıcılara karşılık gelen hedeflerin **Up** olmasıdır. Komut satırından:

`[bastion]`

```bash
TOKEN=$(oc whoami -t)
HOST=$(oc -n openshift-monitoring get route thanos-querier -o jsonpath='{.spec.host}')
curl -sSk -H "Authorization: Bearer $TOKEN" \
  --data-urlencode 'query=up{namespace="lakehouse"} == 1' "https://$HOST/api/v1/query" \
  | jq -r '.data.result[].metric.job' | sort | uniq -c
```

**Beklenen çıktı** (kind kümesindeki Prometheus'tan alınmış gerçek çıktı; OpenShift'te iş
adları da ad alanı önekiyle gelir):

```text
     3 lakehouse/kafka-resources-metrics
     2 lakehouse/spark-operator-podmonitor
     1 polaris-mgmt
```

Geliştirme yığınında bu listeye bir de `kube-state-metrics` satırı eklenir; o bileşen
**yalnız geliştirme kümesinde** kurulur, OpenShift'te görünmemesi normaldir.

**Ters giderse:** hiç satır dönmüyorsa kullanıcı iş yükü izlemesi kapalıdır. Tek bir hedef
`Down` ise ilgili pod'un metrik portu kapalıdır; `oc -n "$LAKEHOUSE_NS" get pods` ile pod'un
ayakta olduğunu doğrulayın.

Aynı iddiaları komut satırından koşturan kabul yolu `test/e2e/monitoring-path.sh`'tir. Betik
geliştirme yığınının nesne adlarını **varsayılan** alır; OpenShift'te üç ortam değişkeniyle
(`PROM_STS`, `PROM_SVC`, `GRAFANA_SKIP`) uyarlanır ve ad alanı bayraklarıyla koşturulur —
tam komut [90-referans/kabul-testleri.md](../90-referans/kabul-testleri.md) §3'tedir.

---

## 3. Beş alarm: anlamı ve karşılık gelen eylem

| Alarm | Ne demek | Şiddet | İlk eylem |
|---|---|---|---|
| `LakehouseConnectTaskFailed` | Bir bağlayıcı görevi düştü; o kaynaktan **veri akmıyor** | critical | [sorun-giderme.md §4.1](sorun-giderme.md#connect) |
| `LakehouseSinkStalled` | Iceberg'e yazma gecikmesi eşiği aştı; veri geliyor ama **tabloya işlenmiyor** | warning | [sorun-giderme.md §4.2](sorun-giderme.md#sink) |
| `LakehouseSilverMergeStale` | En son **başarılı** Silver birleştirmesinin üzerinden çok zaman geçti; Silver tablolar **eskiyor** | warning | [sorun-giderme.md §4.3](sorun-giderme.md#silver-merge) |
| `LakehouseSparkScheduledRunFailed` | Ürünün zamanlı Spark işlerinden biri `FAILED` bitti | warning | [sorun-giderme.md §4.4](sorun-giderme.md#spark) |
| `LakehouseSparkRunTooLong` | Son 6 saatte tamamlanan Spark koşularının **ortalama** süresi eşiği aştı | warning | [sorun-giderme.md §4.5](sorun-giderme.md#spark-duration) |

Alarm bildiriminin içinde bir `runbook` açıklaması vardır; değeri doğrudan yukarıdaki
sayfanın ilgili başlığını gösterir.

**Üç tasarım kararı — "eksik" sanılmamalı:**

1. **`LakehouseSparkScheduledRunFailed` yalnız ürünün kendi işlerini kapsar**
   (`silver-merge`, `maint-` ile başlayan bakım işleri, `mongo-bronze`). Kendi yazdığınız
   Spark uygulamaları bu alarma **girmez**; onların izlenmesi
   [yeni-spark-uygulamasi.md](yeni-spark-uygulamasi.md) sayfasındadır.
2. **`LakehouseSparkRunTooLong` küme geneldir.** spark-operator'ün ölçüm yayını iş başına
   etiket üretmez; kural bu yüzden tek bir işi değil, penceredeki bütün koşuların
   ortalamasını ölçer.
3. **Kurallar veri yokken ateşlenmez.** Taze bir kümede hiç Spark koşusu ya da bağlayıcı
   yoksa kurallar sonuç üretmez ve `inactive` kalır. Bu, "hiç koşmuyor" durumunu
   yakalamadıkları anlamına gelir — o durum haftalık kontrol listesindeki elle bakışla
   yakalanır ([gunluk-haftalik-kontroller.md](gunluk-haftalik-kontroller.md) §3).

### 3.1 Kuralların sağlığını görmek

Konsolda **Observe → Alerting → Alerting rules** sayfasında ad alanına göre süzün; taze bir
kurulumda beşinin de **Inactive** olması beklenir. Komut satırından:

`[bastion]`

```bash
TOKEN=$(oc whoami -t)
HOST=$(oc -n openshift-monitoring get route thanos-querier -o jsonpath='{.spec.host}')
curl -sSk -H "Authorization: Bearer $TOKEN" "https://$HOST/api/v1/rules" \
  | jq -r '.data.groups[] | select(.name=="lakehouse") | .rules[] | "\(.name)\t\(.health)\t\(.state)"'
```

**Beklenen çıktı** (kind kümesindeki Prometheus'tan alınmış gerçek çıktı):

```text
LakehouseConnectTaskFailed	ok	inactive
LakehouseSinkStalled	ok	inactive
LakehouseSilverMergeStale	ok	inactive
LakehouseSparkScheduledRunFailed	ok	inactive
LakehouseSparkRunTooLong	ok	inactive
```

**Ters giderse:** `health` `err` ise kural ifadesi değerlendirilemiyordur — metrik kaynağı
eksiktir (§2). Liste boşsa kural nesnesi toplanmıyordur.

---

## 4. Eşikler ve nereden değiştirilir

Beş anahtar vardır; hepsi `glue` değerlerindedir ve müşteriye özel hâli
`platform/values/site/glue.yaml` dosyasına yazılır.

| Anahtar | Neyi belirler | Ürün varsayılanı | Ne zaman değiştirilir |
|---|---|---|---|
| `monitoring.silverMergeStaleSeconds` | `LakehouseSilverMergeStale` eşiği (saniye) | 2700 (45 dakika) | Silver birleştirme cron'u değiştirilirse **mutlaka**: eşik, aralığın en az üç katı olmalıdır |
| `monitoring.sinkLagThreshold` | `LakehouseSinkStalled` eşiği (birikmiş kayıt) | 1000 | Normal yükte sürekli ateşleniyorsa; önce gerçek birikimi ölçün |
| `monitoring.sparkRunMaxSeconds` | `LakehouseSparkRunTooLong` eşiği (saniye) | 1800 | Veri hacmi büyüdükçe koşular doğal olarak uzar |
| `monitoring.enabled` | Bütün izleme nesnelerinin üretilmesi | `true` | Kapatmak yalnız izlemesiz bir deneme kurulumunda anlamlıdır |
| `monitoring.namespace` | Metrikleri toplayan Prometheus'un ad alanı; ağ politikasındaki toplama izni bundan üretilir | `monitoring` | **OpenShift'te değiştirilmez**: kullanıcı iş yükü izlemesi sabit `openshift-*` ad alanlarındadır ve şablon onu bilir. Anahtar geliştirme/vanilla yığını içindir |

Değiştirme yolu her zaman aynıdır:

`[bastion]`

```bash
${EDITOR:-vi} platform/values/site/glue.yaml
bash scripts/check-site.sh
git add platform/values/site/glue.yaml
git commit -m "site: silver-merge eskime esigi 90 dk"
git push origin main
```

Eklenen satır (örnek):

```yaml
monitoring:
  silverMergeStaleSeconds: 5400
```

Eşitleme bittikten sonra kuralın yeni eşiğe geçtiğini doğrulayın:

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" get prometheusrule lakehouse \
  -o jsonpath='{.spec.groups[0].rules[2].expr}{"\n"}' | tail -c 12
```

**Beklenen çıktı** (örnek — ifadenin sonu yeni eşiği gösterir):

```text
> 5400
```

**Ters giderse:** ifade eski değeri gösteriyorsa eşitleme tamamlanmamıştır
([değişiklik nasıl uygulanır](degisiklik-nasil-uygulanir.md) §5).

> **Eşiği yükseltmek meşru bir çözümdür.** Sürekli ateşlenen ve hiç kimsenin bakmadığı bir
> alarm, hiç olmayan alarmdan daha kötüdür. Ama önce nedenini ölçün: birikim gerçekse eşik
> değil, kaynak artırılmalıdır ([sorun-giderme.md](sorun-giderme.md) §4.2).

---

## 5. Bildirim nereye gider

Ürün bildirim göndermez. Kurallar kümede değerlendirilir; bildirimin yönlendirilmesi
**platformun Alertmanager'ının** işidir. Yönlendirme tanımlı değilse alarm ateşlenir ama
hiçbir yere gitmez — konsolda görünür, e-posta ya da mesaj gelmez.

Kurulum sırasında platform ekibinden istenecek olan şudur: `Lakehouse` ile başlayan alarmları
yakalayan bir yönlendirme kuralı ve bir alıcı (e-posta, mesajlaşma, çağrı sistemi).
Doğrulama ve kapanış maddesi:
[90-referans/pre-ship-kontrol-listesi.md](../90-referans/pre-ship-kontrol-listesi.md) madde
2.4.

Geliştirme kümesinde Alertmanager **kapalıdır**; orada kuralların ateşlendiği yalnız sorgu
arayüzünden görülür.

---

## 6. Günlükler

Metrik ile günlük ayrı konulardır. Ürün bir günlük toplama yığını kurmaz; OpenShift'te
günlükler **Observe → Logs** sekmesinden okunur ve kurulum platformun işidir. Kapsam, kurulum
ve sık kullanılan sorgular: [sorun-giderme.md](sorun-giderme.md) §9.

---

## 7. Tablo düzeyi metrikler burada değildir

Satır sayısı, dosya sayısı, tablo boyutu ve son yazma zamanı gibi **veri** metrikleri
Prometheus'ta yoktur; onlar tablonun kendi meta verisindedir ve SQL ile okunur. Ayrı bir
sayfadadır: [veri-metrikleri.md](veri-metrikleri.md).

Tek bağlantı noktası: tablo tazeliği için alarm **Spark tarafından** gelir
(`LakehouseSilverMergeStale`), veri metriklerinden değil.

---

## Kontrol listesi

- [ ] Kullanıcı iş yükü izlemesi açık; izleme nesneleri kümede.
- [ ] **Observe → Targets** listesinde hedefler `Up`.
- [ ] Beş kural da **Observe → Alerting** altında listeleniyor ve `health` `ok`.
- [ ] Silver birleştirme cron'u değiştirildiyse `monitoring.silverMergeStaleSeconds` de
      güncellendi.
- [ ] `Lakehouse` ile başlayan alarmlar için Alertmanager'da bir yönlendirme ve alıcı
      tanımlı; test bildirimi ulaştı.
- [ ] Trino/Superset/JupyterHub/Zeppelin hedeflerinin **bilerek yok** olduğu ekipçe
      biliniyor.

## Sonraki bölüm

Alarm ateşlendiğinde izlenecek adımlar: [sorun-giderme.md](sorun-giderme.md) §4. Alarm
beklemeden yapılan sabit kontroller:
[gunluk-haftalik-kontroller.md](gunluk-haftalik-kontroller.md). Tablo düzeyi ölçümler:
[veri-metrikleri.md](veri-metrikleri.md).
