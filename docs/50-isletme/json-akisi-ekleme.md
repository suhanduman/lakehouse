# JSON olay akışı ekleme

**Bu bölümde:** bir uygulamanın ya da IoT cihazlarının Kafka'ya JSON kayıt gönderdiği bir
akışı ürüne bağlamak — site değerlerine tek girdi, üreticiye verilecek bağlantı bilgileri,
örnek bir üretici, tablonun doğrulanması, veri sözleşmesi, saklama süresi, bozuk kayıtların
ne olduğu ve akışın kaldırılması.
**Süre:** küme tarafı 15 dakika; ilk satırlar tabloya 5 dakika içinde düşer.
**Gereken yetki:** kurumun Git deposunda `main` dalına yazma; `$LAKEHOUSE_NS` ad alanında
Secret okuma; Polaris kök kimliği (`polaris-root` Secret'ı) ad alanı eklemek için.
**Nerede çalıştırılır:** `[bastion]` küme ve Git komutları; `[üretici]` kaydı gönderen
uygulamanın ya da ağ geçidinin koştuğu makine; `[pod]` doğrulama sorgusu.

**Bu bölümde örnek akış adı `sensors`'tür** (bina sensörlerinden sıcaklık ve nem ölçümleri).

| Addan türeyen | Örnekte |
|---|---|
| Kafka konusu | `sensors.events` |
| Üreticinin Kafka kullanıcısı | `stream-sensors` (yalnız bu konuya yazabilir) |
| Bronze tablosu | `sensors_raw.events` |
| Hatalı kayıt konusu | `sensors.dlq` |

Akış adı küçük harfle başlar, yalnız `a-z`, `0-9` ve `_` içerir, bir kaynağın adıyla ya da
`nginx` ile aynı olamaz.

```text
uygulama / IoT ağ geçidi -> Kafka (TLS + kullanıcı/parola) -> sensors.events -> sink -> sensors_raw.events
```

**Tablo nasıl tutulur:**

- Her akış **tek bir tabloya** yazar; cihaz ya da uygulama başına ayrı tablo açılmaz.
  Kayıtları ayırmak için kayıtta bir alan bulunur (örnekte `device_id`, `room`).
- Kolonlar gelen JSON'dan çıkarılır; yeni bir alan geldiğinde tabloya kendiliğinden yeni
  kolon eklenir. İç içe nesneler iç içe kolon olur (örnekte `battery.level_pct`).
- Ürün her kayda `_kafka_ts` kolonunu ekler: kaydın Kafka'ya vardığı an. Tablo bu
  kolona göre güne bölünür; sorgularda tarih süzgeci için bu kolonu kullanın. Kaydın kendi
  zaman alanı (örnekte `measured_at`) olduğu gibi saklanır.
- Olaylar yalnız eklenir, güncellenmez; Silver katmanı yoktur. Özetler ve raporlar bu
  tablodan üretilir.

---

## 1. Değişkenleri yükleyin

`[bastion]`

```bash
cd ~/lakehouse                      # depoyu `git clone` ile nereye indirdiyseniz orası
set -a; . install/lakehouse.env; set +a
echo "ns=$LAKEHOUSE_NS argocd=$ARGOCD_NS"
```

**Beklenen çıktı** (örnek — kendi değerlerinizle):

```text
ns=lakehouse argocd=openshift-gitops
```

---

## 2. Polaris ad alanını açın

Sink tabloyu kendiliğinden yaratır ama **ad alanını yaratmaz**.
`platform/polaris/setup.yaml` içindeki `namespaces:` listesine bir satır ekleyin:

```yaml
    namespaces:
      - name: sensors_raw
```

`[bastion]`

```bash
PATH="$PWD/.venv/bin:$PATH" scripts/polaris-setup.sh --ns "$LAKEHOUSE_NS" | grep sensors_raw
```

**Beklenen çıktı** (OpenShift provasında alınmış gerçek çıktı; tarih sizde farklıdır):

```text
2026-11-11 13:19:48,285 INFO Creating namespace: 'sensors_raw' in catalog: 'lakehouse'
2026-11-11 13:19:48,463 INFO Namespace 'sensors_raw' created successfully in catalog 'lakehouse'.
```

---

## 3. Akışı site değerlerine ekleyin

`platform/values/site/glue.yaml` dosyasına `streams` girdisi eklenir:

```yaml
streams:
- name: sensors
  retentionDays: 365          # bu günden eski satırlar her gece silinir; yazılmazsa süresiz saklanır
  fields:                     # veri sözleşmesi — Adım 7
    temperature_c: float64
    humidity_pct: int64
```

| Alan | Zorunlu | Anlamı |
|---|---|---|
| `name` | evet | akışın adı |
| `retentionDays` | hayır | saklama süresi (gün); yazılmazsa süresiz |
| `fields` | hayır, **önerilir** | alan adı ve tipi: `int32`, `int64`, `float32`, `float64`, `boolean`, `string` (Adım 7) |
| `partitions` | hayır | konunun bölüm sayısı (varsayılan 3); çok yüksek hacimde artırılır |
| `table` | hayır | Bronze tablosunun adı (varsayılan: akış adı + `_raw.events`) |
| `state` | hayır | `running` (varsayılan) ya da `stopped` — Adım 8 |

Üretici kümenin **dışındaysa** (uygulama sunucusu, IoT ağ geçidi) Kafka'nın dış dinleyicisi
açık olmalıdır: dosyada `kafka: {externalListener: true}` satırı yoksa ekleyin
([yeni-kaynak-ve-pipeline.md](yeni-kaynak-ve-pipeline.md) Adım 10.1 aynı satırı anlatır).

`[bastion]`

```bash
bash scripts/check-site.sh
git add platform/polaris/setup.yaml platform/values/site/glue.yaml
git commit -m "site: sensors JSON akisi"
git push origin main
```

**Beklenen çıktı:** `check-site: OK`. Adım 2'yi atladıysanız denetim şu uyarıyı verir:

```text
UYARI: streams[sensors]: 'sensors_raw' ad alanı platform/polaris/setup.yaml namespaces listesinde yok (ekleyip scripts/polaris-setup.sh'i koşturun)
```

---

## 4. Küme tarafının hazır olduğunu doğrulayın

Eşitleme birkaç dakika sürer. Üç nesne hazır olmalıdır: konu, üreticinin kullanıcısı ve
sink.

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" wait kafkatopic/sensors.events kafkauser/stream-sensors \
  kafkaconnector/sink-sensors --for=condition=Ready --timeout=300s
```

**Beklenen çıktı** (OpenShift provasında alınmış gerçek çıktı):

```text
kafkatopic.kafka.strimzi.io/sensors.events condition met
kafkauser.kafka.strimzi.io/stream-sensors condition met
kafkaconnector.kafka.strimzi.io/sink-sensors condition met
```

**Ters giderse:** `timed out` → `oc -n "$ARGOCD_NS" get application glue` ile eşitlemenin
bittiğini doğrulayın; bitmişse `oc -n "$LAKEHOUSE_NS" describe kafkaconnector sink-sensors`
çıktısının sonundaki hata iletisine bakın.

---

## 5. Üreticiye bağlantı bilgilerini verin

Üretici dört bilgiye ihtiyaç duyar: Kafka adresi, Kafka'nın CA sertifikası, kullanıcı adı
ve parola.

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" get kafka lakehouse \
  -o jsonpath='{.status.listeners[?(@.name=="external")].bootstrapServers}{"\n"}'
oc -n "$LAKEHOUSE_NS" get secret lakehouse-cluster-ca-cert \
  -o jsonpath='{.data.ca\.crt}' | base64 -d > kafka-ca.crt
oc -n "$LAKEHOUSE_NS" get secret stream-sensors -o jsonpath='{.data.password}' | base64 -d; echo
```

**Beklenen çıktı** (parola örnektir; ortadaki komut ekrana bir şey basmaz, CA'yı
`kafka-ca.crt` dosyasına yazar):

```text
lakehouse-kafka-bootstrap-lakehouse.apps.ocp.example.net:443
4f6b2c1d9e8a7350b1c4d2e6f0a9837c
```

Kullanıcı adı `stream-sensors`'tür. Dört bilgiyi uygulama ekibine güvenli bir kanalla
iletin. Bu kullanıcı **yalnız** `sensors.events` konusuna yazabilir; başka bir konuya
yazmayı denerse `TOPIC_AUTHORIZATION_FAILED` hatası alır, hiçbir şey okuyamaz.

---

## 6. Örnek bir üreticiyle deneyin

Uygulama ekibinin dilindeki herhangi bir Kafka istemcisi kullanılabilir. Bağlantı ayarları
her dilde aynıdır: `SASL_SSL`, `SCRAM-SHA-512`, kullanıcı adı ve parola, CA dosyası.
Aşağıdaki Python örneği (`pip install confluent-kafka`) üç ölçüm gönderir.

`[üretici]`

```python
import json, os
from confluent_kafka import Producer

p = Producer({
    "bootstrap.servers": "lakehouse-kafka-bootstrap-lakehouse.apps.ocp.example.net:443",
    "security.protocol": "SASL_SSL", "sasl.mechanism": "SCRAM-SHA-512",
    "sasl.username": "stream-sensors", "sasl.password": os.environ["KAFKA_PASSWORD"],
    "ssl.ca.location": "kafka-ca.crt", "acks": "all"})
for i in range(3):
    olcum = {"device_id": f"sensor-{i + 1}", "room": "A-101",
             "measured_at": "2026-11-11T10:35:00+00:00",
             "temperature_c": 21.5 + i, "humidity_pct": 40 + i,
             "battery": {"level_pct": 90, "charging": False}}
    p.produce("sensors.events", key=olcum["device_id"], value=json.dumps(olcum))
print("iletilemeyen:", p.flush(30))
```

**Beklenen çıktı:**

```text
iletilemeyen: 0
```

`key` olarak cihaz kimliği verilmesi, aynı cihazın kayıtlarının sırayla işlenmesini sağlar.

Kayıtlar beş dakika içinde tabloya düşer. Not defterinden (Trino bağlantısı:
[yeni-kaynak-ve-pipeline.md](yeni-kaynak-ve-pipeline.md) Adım 9.2):

`[pod]`

```python
cur = conn.cursor()
cur.execute("""select room, count(*) adet, round(avg(temperature_c), 2) ort_sicaklik
               from sensors_raw.events group by room order by room""")
cur.fetchall()
```

**Beklenen çıktı** (provada dört odadan 200 ölçüm gönderilmişti — gerçek değerler):

```text
[['A-101', 50, 22.5], ['A-102', 50, 21.72], ['B-201', 50, 22.34], ['Lab-3', 50, 22.12]]
```

**Ters giderse:** satır gelmiyorsa önce `sensors.dlq` konusuna bakın (Adım 7); orada da
yoksa sink görevinin durumuna bakın (Adım 8).

---

## 7. Veri sözleşmesi: hatalı kayıtlar ne olur

JSON şemasızdır: bir cihaz bir gün sıcaklığı sayı yerine metin olarak gönderebilir.
Ürünün davranışı alanın `fields` listesinde bildirilip bildirilmediğine bağlıdır:

| Kayıt | `fields`'ta bildirilmiş alan | Bildirilmemiş alan |
|---|---|---|
| Doğru tipte | tabloya yazılır | tabloya yazılır |
| Tipe çevrilebilen metin (`"19.75"`) | sayıya çevrilir, yazılır | ilk gelen tipe göre kolon açılır; sonraki farklı tip **akışı durdurur** |
| Tipe uymayan değer (`"yirmi iki"`) | `sensors.dlq` konusuna gider, **akış sürer** | sink görevi **durur**, sonraki bütün kayıtlar bekler (Adım 8) |
| JSON olmayan kayıt | `sensors.dlq` konusuna gider, akış sürer | aynı |

Bu yüzden akıştaki **bütün sayısal ve mantıksal alanları** `fields` altında bildirin. Yeni
bir alan eklendiğinde listeye de ekleyin. Metin alanlarının bildirilmesi gerekmez.

Hatalı kayıtlara bakmak için:

`[bastion]`

```bash
scripts/kafka-cli.sh kafka-get-offsets.sh --topic sensors.dlq
scripts/kafka-cli.sh kafka-console-consumer.sh --topic sensors.dlq --from-beginning \
  --max-messages 10 --timeout-ms 15000
```

**Beklenen çıktı** (provadan gerçek çıktı — ilk satır konudaki kayıt sayısıdır; JSON olmayan
kayıt iki kez görünür, çünkü akış Adım 8'deki gibi durdurulup yeniden başlatılınca o kayıt
yeniden işlendi):

```text
sensors.dlq:0:3
bu JSON değil
bu JSON değil
{"device_id": "sensor-9", "room": "C-301", "temperature_c": "yine bozuk", "humidity_pct": 40}
```

Hatalı kayıtları kaynağında düzeltip yeniden göndermek uygulama ekibinin işidir; ürün
onları tabloya almaz.

---

## 8. Akış durursa: bozuk kaydı atlayın

Bildirilmemiş bir alanın tipi değiştiyse sink görevi durur ve aynı kayıtta takılı kalır.
Belirti: tabloya yeni satır gelmez ve görev `FAILED` görünür.

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" exec connect-connect-0 -- \
  curl -s localhost:8083/connectors/sink-sensors/status \
  | jq -r '.tasks[0].state, (.tasks[0].trace // "" | split("\n") | map(select(test("Caused by"))) | last)'
```

**Beklenen çıktı** (provadan gerçek çıktı):

```text
FAILED
Caused by: java.lang.NumberFormatException: For input string: "yirmi iki"
```

Akışı yeniden başlatmak dört adımdır.

**8.1 Akışı durdurun.** Site dosyasında akışa `state: stopped` satırını ekleyip itin
(Adım 3'teki gibi). Eşitlemeden sonra:

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" exec connect-connect-0 -- \
  curl -s localhost:8083/connectors/sink-sensors/status | jq -r .connector.state
```

**Beklenen çıktı:** `STOPPED`

**8.2 Bekleyen kayıtları bulun.** `LAG` sütunu sıfırdan büyük olan bölümlerde işlenmemiş
kayıtlar vardır; ilki takılan kayıttır:

`[bastion]`

```bash
scripts/kafka-cli.sh kafka-consumer-groups.sh --describe --group connect-sink-sensors
```

**Beklenen çıktı** (provadan seçilmiş sütunlar — 1 numaralı bölümde 100. kayıt):

```text
TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
sensors.events  0          100             100             0
sensors.events  1          100             102             2
```

Kaydı görmek için:

`[bastion]`

```bash
scripts/kafka-cli.sh kafka-console-consumer.sh --topic sensors.events --partition 1 \
  --offset 100 --max-messages 1 --timeout-ms 15000
```

**Beklenen çıktı:**

```text
{"device_id": "sensor-9", "room": "C-301", "temperature_c": "yirmi iki", "humidity_pct": 40}
```

**8.3 Konumu bozuk kaydın bir sonrasına alın.** Yalnız o kayıt atlanır; arkasındaki kayıtlar
işlenir:

`[bastion]`

```bash
scripts/kafka-cli.sh kafka-consumer-groups.sh --group connect-sink-sensors \
  --reset-offsets --topic sensors.events:1 --to-offset 101 --execute
```

**Beklenen çıktı:**

```text
GROUP                TOPIC           PARTITION  NEW-OFFSET
connect-sink-sensors sensors.events  1          101
```

**8.4 Akışı yeniden başlatın.** `state: stopped` satırını silip itin. Aynı sorunun bir daha
akışı durdurmaması için o alanı `fields` listesine de ekleyin (Adım 7). Görev `RUNNING`
olur ve bekleyen kayıtlar beş dakika içinde tabloya düşer.

**Ters giderse:** 8.3 grubun etkin olduğunu söyleyen bir hatayla reddedilirse akış henüz
durmamıştır; 8.1'deki çıktının `STOPPED` olduğunu doğrulayın.

---

## 9. Saklama süresi

`retentionDays` verilmişse her gece 03:30'da (UTC) koşan bakım işi `_kafka_ts`'si bu süreden
eski satırları siler. Süre sonradan değiştirilebilir; bir sonraki gece geçerli olur.
Verilmezse tablo süresiz büyür. nginx günlük tablosu için aynı ayar
`nginx.retentionDays` anahtarıdır.

Akışın tablosu "Tablo sağlığı" panosunda kendiliğinden görünür
([veri-metrikleri.md](veri-metrikleri.md) §4); büyüme ve dosya sayısı oradan izlenir.

---

## 10. Akışı kaldırmak

1. Site dosyasından `streams` girdisini silip itin. ArgoCD konuyu (içindeki kayıtlarla),
   üreticinin kullanıcısını ve sink'i kaldırır; üretici artık bağlanamaz.
2. Tabloyu verisiyle silmek isterseniz tek seferlik Spark işiyle `DROP TABLE … PURGE`
   ([kaynak-veya-tablo-silme.md](kaynak-veya-tablo-silme.md) Adım 6.2).
3. `sensors.dlq` konusu ve `connect-sink-sensors` tüketici grubu kalır; aynı adla yeniden
   kurmayacaksanız silin (aynı sayfa Adım 5).

---

## Kontrol listesi

- [ ] Polaris'te akışın ad alanı (örnekte `sensors_raw`) açıldı.
- [ ] `streams` girdisi itildi; konu, kullanıcı ve sink `Ready`.
- [ ] Sayısal ve mantıksal alanlar `fields` altında bildirildi.
- [ ] Saklama süresi kurumla konuşulup `retentionDays` yazıldı (ya da bilinçli olarak süresiz).
- [ ] Üreticiye dört bağlantı bilgisi güvenli kanalla verildi; örnek kayıtlar tabloda görüldü.
- [ ] Hatalı kayıt konusunu (örnekte `sensors.dlq`) kimin, ne sıklıkla izleyeceği belirlendi.

## Sonraki bölüm

Veritabanı kaynakları için: [yeni-kaynak-ve-pipeline.md](yeni-kaynak-ve-pipeline.md).
Tablonun izlenmesi: [veri-metrikleri.md](veri-metrikleri.md).
