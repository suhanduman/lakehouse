# JSON olay akışı ekleme

Bu sayfa, bir uygulamanın ya da IoT cihazlarının Kafka'ya JSON kayıt gönderdiği bir akışı
lakehouse'a bağlamayı anlatır. Site değerlerine tek bir girdi eklenir; ürün Kafka konusunu,
üreticinin kullanıcısını ve kayıtları tabloya yazan bağlayıcıyı kendisi kurar. Sayfa ayrıca
üreticiye verilecek bağlantı bilgilerini, örnek bir üreticiyi, veri sözleşmesini, saklama
süresini, bozuk kayıtları ve akışın kaldırılmasını anlatır.

**Ne kadar sürer:** küme tarafı 15 dakika. Gönderilen ilk kayıtlar tabloya 5 dakika içinde
düşer.

**Hangi yetkiler gerekir:** kurumun lakehouse Git deposunda `main` dalına yazma,
`$LAKEHOUSE_NS` ad alanında Secret okuma, ad alanı açmak için Polaris kök kimliği
(`polaris-root` Secret'ı).

**Komutlar nerede çalışır:**

- `[bastion]` — küme ve Git komutları
- `[üretici]` — kaydı gönderen uygulamanın ya da ağ geçidinin çalıştığı makine
- `[pod]` — JupyterHub not defteri (doğrulama sorgusu)

**Bu sayfadaki örnek akışın adı `sensors`'tür** (bina sensörlerinden sıcaklık ve nem
ölçümleri). Akışın adından şunlar türer:

| Ne | Örnekte |
|---|---|
| Kafka konusu | `sensors.events` |
| Üreticinin Kafka kullanıcısı | `stream-sensors` (yalnız bu konuya yazabilir) |
| Bronze tablosu | `sensors_raw.events` |
| Hatalı kayıt konusu | `sensors.dlq` |

Akış adı küçük harfle başlar ve yalnız `a-z`, `0-9` ve `_` içerir. Bir kaynağın adıyla ya
da `nginx` ile aynı olamaz.

```text
uygulama / IoT ağ geçidi -> Kafka (TLS + kullanıcı/parola) -> sensors.events -> bağlayıcı -> sensors_raw.events
```

**Tablo nasıl tutulur:**

- Her akış **tek bir tabloya** yazar; cihaz ya da uygulama başına ayrı tablo açılmaz.
  Kayıtları ayırmak için kayıtta bir alan bulunur (örnekte `device_id`, `room`).
- Kolonlar gelen JSON'dan çıkarılır. Yeni bir alan geldiğinde tabloya kendiliğinden yeni
  bir kolon eklenir. İç içe nesneler iç içe kolon olur (örnekte `battery.level_pct`).
- Ürün her kayda `_kafka_ts` kolonunu ekler: kaydın Kafka'ya vardığı an. Tablo bu kolona
  göre günlere bölünür; sorgularda tarih süzgeci için bu kolonu kullanın. Kaydın kendi
  zaman alanı (örnekte `measured_at`) olduğu gibi saklanır.
- Kayıtlar yalnız eklenir, güncellenmez; bu yüzden Silver katmanı yoktur. Özetler ve
  raporlar bu tablodan üretilir.

---

## 1. Değişkenleri yükleyin

Her yeni terminalde bir kez çalıştırın.

`[bastion]`

```bash
cd ~/lakehouse
set -a; . install/lakehouse.env; set +a
echo "ns=$LAKEHOUSE_NS argocd=$ARGOCD_NS"
```

**Beklenen çıktı** (kendi değerlerinizle):

```text
ns=lakehouse argocd=openshift-gitops
```

---

## 2. Katalogda ad alanını açın

Bağlayıcı tabloyu kendiliğinden yaratır ama **ad alanını yaratmaz**. Ad alanını önce
açmak gerekir.

`platform/polaris/setup.yaml` dosyasını açın:

`[bastion]`

```bash
vi platform/polaris/setup.yaml
```

`namespaces:` listesinin son satırının altına şu satırı ekleyin (baştaki boşluk listedeki
diğer satırlarla aynı olmalıdır):

```yaml
      - name: sensors_raw
```

Kaydedin ve ad alanını katalogda yaratın:

```bash
PATH="$PWD/.venv/bin:$PATH" scripts/polaris-setup.sh --ns "$LAKEHOUSE_NS" | grep sensors_raw
```

**Beklenen çıktı** (satır başlarındaki tarih ve saat sizde farklıdır):

```text
2026-11-11 13:19:48,285 INFO Creating namespace: 'sensors_raw' in catalog: 'lakehouse'
2026-11-11 13:19:48,463 INFO Namespace 'sensors_raw' created successfully in catalog 'lakehouse'.
```

---

## 3. Akışı site değerlerine ekleyin

`platform/values/site/glue.yaml` dosyasını açın:

`[bastion]`

```bash
vi platform/values/site/glue.yaml
```

Dosyada `streams:` satırı yoksa dosyanın sonuna şu bloğu ekleyin. `streams: []` satırı
varsa o satırı silip yerine bu bloğu yazın. `streams:` altında başka akışlar varsa yalnız
`- name: sensors` ile başlayan kısmı listenin sonuna ekleyin.

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

Üretici kümenin **dışındaysa** (uygulama sunucusu, IoT ağ geçidi) Kafka'nın dışarıya açık
olması gerekir. Dosyada `kafka: {externalListener: true}` satırı yoksa onu da ekleyin
([yeni-kaynak-ve-pipeline.md](yeni-kaynak-ve-pipeline.md) Adım 10.1 aynı satırı anlatır).

Kaydedin, denetleyin ve gönderin:

`[bastion]`

```bash
bash scripts/check-site.sh
git diff
git add platform/polaris/setup.yaml platform/values/site/glue.yaml
git commit -m "site: sensors JSON akışı"
git push origin main
```

**Beklenen çıktı:** `check-site: OK`.

**Ters giderse:** Adım 2'yi atladıysanız denetim şu uyarıyı verir; Adım 2'yi yapın ve
betiği tekrar çalıştırın:

```text
UYARI: streams[sensors]: 'sensors_raw' ad alanı platform/polaris/setup.yaml namespaces listesinde yok (ekleyip scripts/polaris-setup.sh'i koşturun)
```

---

## 4. Küme tarafının hazır olduğunu kontrol edin

ArgoCD değişikliği birkaç dakika içinde uygular. Üç nesne hazır olmalıdır: Kafka konusu,
üreticinin kullanıcısı ve bağlayıcı.

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" wait kafkatopic/sensors.events kafkauser/stream-sensors \
  kafkaconnector/sink-sensors --for=condition=Ready --timeout=300s
```

**Beklenen çıktı:**

```text
kafkatopic.kafka.strimzi.io/sensors.events condition met
kafkauser.kafka.strimzi.io/stream-sensors condition met
kafkaconnector.kafka.strimzi.io/sink-sensors condition met
```

**Ters giderse:**

- `timed out` ya da `NotFound` görürseniz ArgoCD henüz uygulamamıştır.
  `oc -n "$ARGOCD_NS" get application glue` çıktısının `Synced Healthy` olmasını bekleyip
  komutu tekrarlayın.
- Uygulama bitmiş ama bağlayıcı hazır değilse hatanın nedenini görün:
  `oc -n "$LAKEHOUSE_NS" describe kafkaconnector sink-sensors` (çıktının son satırları).

---

## 5. Üreticiye bağlantı bilgilerini verin

Üreticinin dört bilgiye ihtiyacı vardır: Kafka adresi, Kafka'nın CA sertifikası,
kullanıcı adı ve parola.

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" get kafka lakehouse \
  -o jsonpath='{.status.listeners[?(@.name=="external")].bootstrapServers}{"\n"}'
oc -n "$LAKEHOUSE_NS" get secret lakehouse-cluster-ca-cert \
  -o jsonpath='{.data.ca\.crt}' | base64 -d > kafka-ca.crt
oc -n "$LAKEHOUSE_NS" get secret stream-sensors -o jsonpath='{.data.password}' | base64 -d; echo
```

**Beklenen çıktı** (örnek: adres ve parola sizde farklıdır). Ortadaki komut ekrana bir şey
yazmaz, CA sertifikasını `kafka-ca.crt` dosyasına kaydeder:

```text
lakehouse-kafka-bootstrap-lakehouse.apps.ocp.example.net:443
4f6b2c1d9e8a7350b1c4d2e6f0a9837c
```

Kullanıcı adı `stream-sensors`'tür. Dört bilgiyi uygulama ekibine güvenli bir kanalla
iletin. Bu kullanıcı **yalnız** `sensors.events` konusuna yazabilir. Başka bir konuya
yazmaya çalışırsa `TOPIC_AUTHORIZATION_FAILED` hatası alır; hiçbir konudan okuyamaz.

Parolayı taşıyan `stream-sensors` Secret'ını elle yaratmazsınız: Kafka operatörü onu
Git'teki akış tanımından üretir. Silinirse operatör yeniden üretir ve parola değişir;
üreticiye yeni parolayı vermeniz gerekir. Bir nesnenin nereden geldiğini
`scripts/nesne-kaynagi.sh` ile görebilirsiniz
([değişiklik nasıl uygulanır](degisiklik-nasil-uygulanir.md) §7):

```bash
scripts/nesne-kaynagi.sh secret/stream-sensors
```

**Beklenen çıktı:**

```text
secret/stream-sensors                        TÜREV      KafkaUser/stream-sensors -> ArgoCD uygulaması: glue
```

---

## 6. Örnek bir üreticiyle deneyin

Uygulama ekibi kendi dilindeki herhangi bir Kafka istemcisini kullanabilir. Bağlantı
ayarları her dilde aynıdır: `SASL_SSL`, `SCRAM-SHA-512`, kullanıcı adı, parola ve CA
dosyası. Aşağıdaki Python örneği üç ölçüm gönderir. Önce kitaplığı kurun ve parolayı
değişkene verin (parola ekranda görünmez):

`[üretici]`

```bash
pip install confluent-kafka
read -rs KAFKA_PASSWORD && export KAFKA_PASSWORD
```

Sonra şu programı çalıştırın (`bootstrap.servers` satırına Adım 5'teki adresi yazın):

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

`key` olarak cihaz kimliğinin verilmesi, aynı cihazın kayıtlarının sırasıyla işlenmesini
sağlar.

Kayıtlar beş dakika içinde tabloya düşer. Bir not defterinde kontrol edin (Trino
bağlantısının nasıl kurulduğu:
[yeni-kaynak-ve-pipeline.md](yeni-kaynak-ve-pipeline.md) Adım 9.2):

`[pod]`

```python
cur = conn.cursor()
cur.execute("""select room, count(*) adet, round(avg(temperature_c), 2) ort_sicaklik
               from sensors_raw.events group by room order by room""")
cur.fetchall()
```

**Beklenen çıktı** (örnek: dört odadan 200 ölçüm gönderilmiş bir akış):

```text
[['A-101', 50, 22.5], ['A-102', 50, 21.72], ['B-201', 50, 22.34], ['Lab-3', 50, 22.12]]
```

**Ters giderse:** satır gelmiyorsa önce `sensors.dlq` konusuna bakın (Adım 7). Orada da
kayıt yoksa bağlayıcının durumuna bakın (Adım 8).

---

## 7. Veri sözleşmesi: hatalı kayıtlar ne olur

JSON'da şema yoktur: bir cihaz bir gün sıcaklığı sayı yerine metin olarak gönderebilir.
Ürünün ne yapacağı, alanın `fields` listesinde bildirilip bildirilmediğine bağlıdır:

| Kayıt | `fields`'ta bildirilmiş alan | Bildirilmemiş alan |
|---|---|---|
| Doğru tipte | tabloya yazılır | tabloya yazılır |
| Sayıya çevrilebilen metin (`"19.75"`) | sayıya çevrilir, yazılır | ilk gelen tipe göre kolon açılır; sonraki farklı tip **akışı durdurur** |
| Tipe uymayan değer (`"yirmi iki"`) | `sensors.dlq` konusuna gider, **akış sürer** | bağlayıcı **durur**, sonraki bütün kayıtlar bekler (Adım 8) |
| JSON olmayan kayıt | `sensors.dlq` konusuna gider, akış sürer | aynı |

Bu yüzden akıştaki **bütün sayısal ve mantıksal (true/false) alanları** `fields` altında
bildirin. Akışa yeni bir sayısal alan eklendiğinde onu da listeye ekleyin. Metin
alanlarının bildirilmesi gerekmez.

`sensors.dlq` konusuna yeni bir kayıt düştüğünde `LakehouseDlqRecords` alarmı gelir
([izleme-ve-alarmlar.md](izleme-ve-alarmlar.md) §3). Hatalı kayıtlara bakmak için:

`[bastion]`

```bash
scripts/kafka-cli.sh kafka-get-offsets.sh --topic sensors.dlq
scripts/kafka-cli.sh kafka-console-consumer.sh --topic sensors.dlq --from-beginning \
  --max-messages 10 --timeout-ms 15000
```

**Beklenen çıktı** (örnek: konuda üç hatalı kayıt olan bir akış). İlk satırın sonundaki
sayı konudaki kayıt sayısıdır. Aynı kayıt iki kez görünebilir: akış Adım 8'deki gibi
durdurulup yeniden başlatıldığında bekleyen kayıtlar bir kez daha işlenir.

```text
sensors.dlq:0:3
bu JSON değil
bu JSON değil
{"device_id": "sensor-9", "room": "C-301", "temperature_c": "yine bozuk", "humidity_pct": 40}
```

Hatalı kayıtları kaynakta düzeltip yeniden göndermek uygulama ekibinin işidir; ürün onları
tabloya almaz.

---

## 8. Akış durursa: bozuk kaydı atlayın

Bildirilmemiş bir alanın tipi değiştiyse bağlayıcı durur ve aynı kayıtta takılı kalır.
Belirti: tabloya yeni satır gelmez ve bağlayıcının görevi `FAILED` görünür.

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" exec connect-connect-0 -- \
  curl -s localhost:8083/connectors/sink-sensors/status \
  | jq -r '.tasks[0].state, (.tasks[0].trace // "" | split("\n") | map(select(test("Caused by"))) | last)'
```

**Beklenen çıktı** (bu durumda):

```text
FAILED
Caused by: java.lang.NumberFormatException: For input string: "yirmi iki"
```

Akışı yeniden yürütmek dört adımdır.

### 8.1 Akışı durdurun

`platform/values/site/glue.yaml` dosyasında akışın girdisine şu satırı ekleyin:

```yaml
- name: sensors
  state: stopped
  # diğer satırlar aynen kalır
```

Adım 3'teki gibi kaydedip gönderin. ArgoCD uyguladıktan sonra (bir iki dakika):

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" exec connect-connect-0 -- \
  curl -s localhost:8083/connectors/sink-sensors/status | jq -r .connector.state
```

**Beklenen çıktı:**

```text
STOPPED
```

`RUNNING` görürseniz ArgoCD henüz uygulamamıştır; bir dakika sonra tekrar deneyin.

### 8.2 Takılan kaydı bulun

`LAG` sütunu sıfırdan büyük olan bölümlerde işlenmemiş kayıtlar vardır. O bölümdeki
`CURRENT-OFFSET` değeri takılan kaydın konumudur:

`[bastion]`

```bash
scripts/kafka-cli.sh kafka-consumer-groups.sh --describe --group connect-sink-sensors
```

**Beklenen çıktı** (örnek; seçilmiş sütunlar — 1 numaralı bölümde 100. kayıt takılı):

```text
TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
sensors.events  0          100             100             0
sensors.events  1          100             102             2
```

Takılan kaydı görmek için bölüm ve konum numarasını yazın:

```bash
scripts/kafka-cli.sh kafka-console-consumer.sh --topic sensors.events --partition 1 \
  --offset 100 --max-messages 1 --timeout-ms 15000
```

**Beklenen çıktı:**

```text
{"device_id": "sensor-9", "room": "C-301", "temperature_c": "yirmi iki", "humidity_pct": 40}
```

### 8.3 Konumu bozuk kaydın bir sonrasına alın

Yalnız bozuk kayıt atlanır; arkasındaki kayıtlar işlenir. Komuttaki `:1` bölüm numarası,
`101` ise takılan konumun bir fazlasıdır:

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

**Ters giderse:** komut grubun etkin olduğunu söyleyen bir hatayla reddedilirse akış henüz
durmamıştır; 8.1'deki çıktının `STOPPED` olduğunu kontrol edin.

### 8.4 Akışı yeniden başlatın

`state: stopped` satırını silip gönderin. Aynı sorunun akışı bir daha durdurmaması için o
alanı `fields` listesine de ekleyin (Adım 7). Bağlayıcının görevi `RUNNING` olur ve bekleyen
kayıtlar beş dakika içinde tabloya düşer.

---

## 9. Saklama süresi

`retentionDays` verilmişse, her gece 03:30'da (UTC) çalışan bakım işi `_kafka_ts` değeri bu
süreden eski satırları siler. Süre sonradan değiştirilebilir; değişiklik bir sonraki gece
geçerli olur. `retentionDays` verilmezse tablo süresiz büyür. nginx günlük tablosu için
aynı ayar `nginx.retentionDays`'dir.

Akışın tablosu "Tablo sağlığı" panosunda kendiliğinden görünür
([veri-metrikleri.md](veri-metrikleri.md) §4); tablonun büyümesi ve dosya sayısı oradan
izlenir.

---

## 10. Akışı kaldırmak

1. `platform/values/site/glue.yaml` dosyasından akışın girdisini silip gönderin. ArgoCD
   Kafka konusunu (içindeki kayıtlarla birlikte), üreticinin kullanıcısını ve bağlayıcıyı
   kaldırır; üretici artık bağlanamaz.
2. Tabloyu verisiyle silmek isterseniz tek seferlik Spark işiyle `DROP TABLE … PURGE`
   çalıştırın ([kaynak-veya-tablo-silme.md](kaynak-veya-tablo-silme.md) Adım 6.2).
3. `sensors.dlq` konusu ve `connect-sink-sensors` tüketici grubu kalır. Aynı adla yeniden
   kurmayacaksanız bunları da silin (aynı sayfanın Adım 5'i).

---

## Kontrol listesi

- [ ] Katalogda akışın ad alanı (örnekte `sensors_raw`) açıldı.
- [ ] `streams` girdisi gönderildi; konu, kullanıcı ve bağlayıcı `Ready`.
- [ ] Sayısal ve mantıksal alanlar `fields` altında bildirildi.
- [ ] Saklama süresi kurumla konuşulup `retentionDays` yazıldı (ya da bilerek süresiz
      bırakıldı).
- [ ] Üreticiye dört bağlantı bilgisi güvenli kanalla verildi; örnek kayıtlar tabloda
      görüldü.
- [ ] Hatalı kayıt konusunu (örnekte `sensors.dlq`) kimin izleyeceği belirlendi.

## Sonraki bölüm

Veritabanı kaynakları için: [yeni-kaynak-ve-pipeline.md](yeni-kaynak-ve-pipeline.md).
Tabloların izlenmesi için: [veri-metrikleri.md](veri-metrikleri.md).
