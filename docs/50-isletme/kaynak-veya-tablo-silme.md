# Kaynak ya da tablo kaldırma

Bu sayfa, bir tabloyu ya da bir kaynak veritabanının tamamını lakehouse'tan **verisiyle
birlikte** kaldırmayı adım adım anlatır: ayar dosyasından çıkarmak, geride kalan Kafka
konularını ve Iceberg tablolarını silmek, S3'te dosya kalmadığını doğrulamak ve kaynak
veritabanındaki CDC izlerini temizlemek.

**Ne kadar sürer:** 30 dakika ve tablo silme işinin süresi (küçük tablolarda birkaç
dakika).

**Hangi yetkiler gerekir:** kurumun lakehouse Git deposunda `main` dalına yazma;
`$LAKEHOUSE_NS` ad alanında nesne yaratma ve silme; kaynak veritabanında CDC ayarlarını
değiştirebilen bir hesap; kaynağın tamamı kalkıyorsa Polaris yönetici kimliği
(`polaris-root` Secret'ı).

**Komutlar nerede çalışır:** `[bastion]` küme ve Git komutları; `[kaynak DB]` kaynak
veritabanına bağlanabilen makine; `[pod]` JupyterHub not defteri hücresi.

Sayfada iki örnek kullanılır. **Tek tablo** örneği, `shop` kaynağının `public.customers`
tablosudur (Bronze `shop_raw.customers`, Silver `shop.customers`). **Kaynağın tamamı**
örneği, [yeni-kaynak-ve-pipeline.md](yeni-kaynak-ve-pipeline.md) sayfasındaki `erp`
kaynağıdır.

> **Bu sayfadaki adımlar geri alınamaz.** `PURGE` ile silinen bir Iceberg tablosunun
> hem katalog kaydı hem S3'teki dosyaları gider. Silinen bir Kafka konusundaki olaylar
> gider. `git revert` yalnız **ayarı** geri getirir, **veriyi getirmez**: tablo boş olarak
> yeniden yaratılır ve kaynaktan yeniden okunarak dolar.
>
> **Başlamadan önce:**
>
> 1. Tabloyu kimsenin kullanmadığını doğrulayın: Superset panoları, kayıtlı sorgular,
>    kurumun kendi Spark işleri.
> 2. Saklamak istediğiniz bir şey varsa önce kopyasını alın. S3'teki veri dosyaları ürünün
>    yedeğine dahil değildir. En kolayı, `CREATE TABLE … AS SELECT` ile `sandbox` altına
>    kopyalamaktır.

Adımların sırası:

```text
(3: aynı adla yeniden eklenecekse konumu sıfırla) -> 4: ayardan çıkar ve gönder
  -> 5: Kafka konuları -> 6: Iceberg tabloları -> 7: S3 kontrolü -> 8: kaynak veritabanı
  -> 9: kaynağın tamamı kalkıyorsa kalan izler
```

---

## 1. Değişkenleri yükleyin

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

**Ters giderse:** eşittir işaretlerinden sonrası boşsa `install/lakehouse.env` yüklenmemiştir
([30-kurulum](../30-kurulum.md) §1).

---

## 2. Neyi kaldırdığınıza karar verin

İki durum vardır; yapılacak adımlar buna göre değişir.

| | Tek tablo | Kaynağın tamamı |
|---|---|---|
| Ayar dosyasında silinen | `tables` (MongoDB'de `collections`) listesinden bir satır ve varsa o tablonun `pipelines` satırı | `sources` listesindeki öğenin tamamı ve o kaynağın bütün `pipelines` satırları |
| Kalkan bağlayıcı | yok (bağlayıcı yalnız yeniden başlar) | `dbz-` ve `sink-` bağlayıcıları |
| Silinecek Iceberg tabloları | o tablonun Bronze ve Silver tabloları | kaynağın bütün tabloları |
| Kaynak veritabanında | tablo düzeyinde temizlik | kaynak düzeyinde temizlik |
| Ek temizlik | yok | Secret, Polaris ad alanları, Superset referansları (Adım 9) |
| Uygulanacak adımlar | 1, 2, 4, 5, 6, 7, 8 | hepsi |

**Silver'ı olmayan tablolar.** `pipelines` listesinde yer almayan bir tablo yalnız
Bronze'dadır; Adım 6'da yalnız Bronze tablosu silinir.

**MongoDB kaynaklarında karantina tablosu.** Her koleksiyonun Bronze adının sonuna
`__quarantine` eklenmiş ikinci bir tablosu vardır
([yeni-kaynak-ve-pipeline.md](yeni-kaynak-ve-pipeline.md) §3.3); o da silinir.

---

## 3. Aynı adla yeniden ekleyecekseniz: önce konumu sıfırlayın

Bu adım **yalnız** kaynağın tamamı kalkıyorsa **ve** ileride aynı kaynak adını yeniden
kullanacaksanız gerekir. Örneğin kabul testini aynı kümede ikinci kez koşmak bu duruma
girer. Tek tablo kaldırırken bu adımı atlayın.

**Neden gerekir?** Kafka Connect, bağlayıcının kaynakta nerede kaldığını (konumunu)
bağlayıcının **adıyla** saklar. Aynı adla yeniden kurulan bir bağlayıcı bu konumu bulur,
kaynağı zaten okumuş sayar ve mevcut veriyi okumaz: tablolarınız boş kalır.

Konumu silmek için bağlayıcının **kümede hâlâ var olması** ve **durmuş olması** gerekir.
Bu yüzden bu adım Adım 4'ten önce yapılır.

> **Bağlayıcıyı elle değil, Git'ten durdurun.** Bağlayıcının durumu Git'te yazılıdır ve
> küme onu sürekli o duruma getirir. Kafka Connect'e elle "dur" komutu gönderirseniz
> birkaç saniye içinde yeniden başlatılır; bağlayıcı kaynağın tamamını yeniden okur ve
> konumu yeniden yazar.

### 3.1 Bağlayıcıyı durdurun

`platform/values/site/glue.yaml` dosyasını açın:

`[bastion]`

```bash
vi platform/values/site/glue.yaml
```

Kaldıracağınız kaynağın `sources` öğesinin son satırının altına, başındaki iki boşlukla
birlikte şu satırı ekleyin:

```yaml
  state: stopped
```

Örneğin:

```yaml
sources:
- name: erp
  type: postgres
  host: erp-db.kurum.example.net
  port: 5432
  database: erp
  tables: [public.orders]
  signalTable: public.debezium_signal
  state: stopped
```

Kaydedin, denetleyin ve gönderin:

```bash
bash scripts/check-site.sh
git add platform/values/site/glue.yaml
git commit -m "site: erp kaynağı durduruldu"
git push origin main
oc -n "$ARGOCD_NS" annotate application glue argocd.argoproj.io/refresh=normal --overwrite
```

Bir dakika sonra bağlayıcının durduğunu kontrol edin:

```bash
CONNECTOR=dbz-erp
oc -n "$LAKEHOUSE_NS" exec connect-connect-0 -- \
  curl -s http://localhost:8083/connectors/$CONNECTOR/status | jq -r .connector.state
```

**Beklenen çıktı:**

```text
STOPPED
```

`RUNNING` görürseniz ArgoCD değişikliği henüz uygulamamıştır; bir dakika sonra tekrar
deneyin.

### 3.2 Konumu silin

`STOPPED` gördükten sonra:

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" exec connect-connect-0 -- \
  curl -s -X DELETE http://localhost:8083/connectors/$CONNECTOR/offsets
echo
oc -n "$LAKEHOUSE_NS" exec connect-connect-0 -- \
  curl -s http://localhost:8083/connectors/$CONNECTOR/offsets
```

**Beklenen çıktı:**

```text
{"message":"The Connect framework-managed offsets for this connector have been reset successfully. However, if this connector manages offsets externally, they will need to be manually reset in the system that the connector uses."}
{"offsets":[]}
```

Son satırdaki boş liste, konumun silindiğini gösterir.

> **`state: stopped` satırını silmeyin.** Konumu silinmiş bir bağlayıcıyı yeniden
> başlatırsanız kaynağın tamamını ikinci kez okur ve Bronze tablolarına aynı kayıtlar
> tekrar yazılır. Bağlayıcı Adım 4'te kaynağın tamamıyla birlikte, durmuş hâlde kaldırılır.

**Bu adımı yapmak istemiyorsanız:** kaynağı ileride **başka bir adla** ekleyin. Bağlayıcının
adı değişince eski konum bulunmaz ve kaynak baştan okunur. Bu durumda Kafka konularının ve
Bronze ad alanının adı da değişir.

**Ters giderse:**

- `{"error_code":404,"message":"Unknown connector dbz-erp"}`: bağlayıcı kümede yoktur,
  yani Adım 4 zaten yapılmıştır. Tek çıkış, kaynağı başka bir adla eklemektir.
- `409` görürseniz bağlayıcı henüz durmamıştır. 3.1'deki kontrolün `STOPPED`
  gösterdiğinden emin olup silme komutunu tekrarlayın.

---

## 4. Ayar dosyasından çıkarın ve gönderin

`platform/values/site/glue.yaml` dosyasını açın:

`[bastion]`

```bash
vi platform/values/site/glue.yaml
```

**Tek tablo kaldırıyorsanız:** tablonun adını kaynağın `tables` listesinden silin; tablonun
`pipelines` satırı varsa onu da silin. Örneğin `shop` kaynağından `public.customers`
kaldırıldıktan sonra:

```yaml
sources:
- name: shop
  type: postgres
  host: shop-db.kurum.example.net
  port: 5432
  database: shop
  tables: [public.orders]                       # public.customers silindi
  signalTable: public.debezium_signal
pipelines:
- {bronze: shop_raw.orders, keys: [id], bucket_count: 4, casts: {updated_at: timestamp}}
# shop_raw.customers satırı silindi
```

**Kaynağın tamamını kaldırıyorsanız:** `sources` listesinden kaynağın öğesinin tamamını
(`- name: erp` satırından Adım 3'te eklediğiniz `state: stopped` satırına kadar) ve
`pipelines` listesinden `erp_raw.` ile başlayan bütün satırları silin. Liste boş kaldıysa
başlık satırını `sources: []` ya da `pipelines: []` yapın.

Kaydedin, denetleyin ve gönderin:

```bash
bash scripts/check-site.sh
git diff
git add platform/values/site/glue.yaml
git commit -m "site: shop kaynağından public.customers çıkarıldı"
git push origin main
oc -n "$ARGOCD_NS" annotate application glue argocd.argoproj.io/refresh=normal --overwrite
```

`git diff` çıktısında yalnız sildiğiniz satırlar (`-` ile başlayan) görünmelidir.
Birkaç dakika sonra uygulamanın bittiğini kontrol edin:

```bash
oc -n "$ARGOCD_NS" get application glue \
  -o jsonpath='{.status.sync.status} {.status.health.status}{"\n"}'
```

**Beklenen çıktı:**

```text
Synced Healthy
```

Tek tablo kaldırıldıysa Debezium bağlayıcısı yeniden başlar. Kaynağın tamamı kaldırıldıysa
iki bağlayıcı da kümeden silinir; kontrol edin:

```bash
oc -n "$LAKEHOUSE_NS" get kafkaconnector dbz-erp sink-erp
```

**Beklenen çıktı** (örnek — `erp` kaynağının tamamı kaldırıldığında):

```text
Error from server (NotFound): kafkaconnectors.kafka.strimzi.io "dbz-erp" not found
Error from server (NotFound): kafkaconnectors.kafka.strimzi.io "sink-erp" not found
```

### 4.1 Neyin kendiliğinden kalktığı, neyin kaldığı

Bu ayrım, sayfanın geri kalan adımlarının nedenidir.

| Nesne | Gönderdikten sonra |
|---|---|
| `dbz-` ve `sink-` bağlayıcıları (kaynağın tamamı kalkarsa) | **kendiliğinden silinir** |
| Kafka erişim izinleri, Connect'in Secret okuma izni | **kendiliğinden daralır** |
| `silver-merge` işi (son pipeline da kalktıysa) | **kendiliğinden silinir** |
| Kafka konuları | **kalır** — Adım 5 |
| Iceberg tabloları ve S3 dosyaları | **kalır** — Adım 6 ve 7 |
| Kaynak veritabanındaki CDC ayarları | **kalır** — Adım 8 |
| Kaynağın Secret'ı ve Polaris ad alanları | **kalır** — Adım 9 |

**Ters giderse:** uygulama `Synced Degraded` gösteriyorsa bağlayıcı yeni ayarla
açılamamıştır. `oc -n "$LAKEHOUSE_NS" get kafkaconnector` çıktısına bakın
([yeni-kaynak-ve-pipeline.md](yeni-kaynak-ve-pipeline.md) §9.4).

---

## 5. Kafka konularını silin

Bağlayıcı kalktığında yarattığı Kafka konuları yerinde kalır. Konuların içindeki olaylar
saklama süresi dolunca kendiliğinden silinir (varsayılan bir hafta); bu adımı yer açmak
ya da aynı adı hemen yeniden kullanmak için yaparsınız.

Kafka komutları ürünle gelen `scripts/kafka-cli.sh` betiğiyle çalıştırılır. Betik
komutu Kafka pod'unun içinde, ürünün yönetici Kafka kimliğiyle çalıştırır; siz yalnız
komutun kendi seçeneklerini yazarsınız.

Önce kaynağın konularını listeleyin:

`[bastion]`

```bash
scripts/kafka-cli.sh kafka-topics.sh --list | grep -E '^shop\.|\.shop$'
```

**Beklenen çıktı** (`shop` kaynağı; tablo başına bir konu ve kaynağın iki iç konusu):

```text
__debezium-heartbeat.shop
shop.dlq
shop.public.customers
shop.public.orders
```

**Tek tablo kaldırıyorsanız** yalnız o tablonun konusunu silin:

```bash
scripts/kafka-cli.sh kafka-topics.sh --delete --topic shop.public.customers
```

Silme komutu başarılı olduğunda bir şey yazmaz.

**Kaynağın tamamını kaldırıyorsanız** listedeki bütün konuları silin. Konu adlarını
listede gördüğünüz gibi `for` satırına yazın; SQL Server kaynaklarında listede ayrıca
`schema-history.` ile başlayan bir konu da vardır:

```bash
for t in __debezium-heartbeat.erp erp.dlq erp.public.orders; do
  scripts/kafka-cli.sh kafka-topics.sh --delete --topic "$t" && echo "silindi: $t"
done
```

**Beklenen çıktı** (örnek — `erp` kaynağı için; her konu için bir satır):

```text
silindi: __debezium-heartbeat.erp
silindi: erp.dlq
silindi: erp.public.orders
```

Konuların gittiğini kontrol edin; bu komut hiçbir satır yazmamalıdır:

```bash
scripts/kafka-cli.sh kafka-topics.sh --list | grep -E '^erp\.|\.erp$'
```

Kaynağın tamamı kalktıysa Iceberg yazıcısının iki tüketici grubunu da silin:

```bash
scripts/kafka-cli.sh kafka-consumer-groups.sh --delete \
  --group connect-sink-erp --group connect-sink-erp-coord
```

**Beklenen çıktı** (grupların sırası değişebilir):

```text
Deletion of requested consumer groups ('connect-sink-erp-coord', 'connect-sink-erp') was successful.
```

**Ters giderse:**

- `GroupIdNotFoundException`: o grup kendiliğinden silinmiştir, sorun değildir.
- `This server does not host this topic-partition`: konu adı yanlış yazılmıştır; liste
  çıktısıyla karşılaştırın.
- `nginx.access` gibi ürünün kendi tanımladığı konular bu adımla silinmez; nginx akışı
  kapatıldığında (`nginx.enabled: false`) kendiliğinden kalkar.

---

## 6. Iceberg tablolarını verisiyle silin

Tablonun hangi ad alanında olduğuna göre silme yolu değişir.

| Tablo nerede | Kim silebilir | Nasıl |
|---|---|---|
| `sandbox` | `lakehouse-analysts` grubu | Trino'da `DROP TABLE` (6.1) |
| Bronze ve Silver | ürünün Spark kimliği | tek seferlik Spark işi (6.2) |

### 6.1 `sandbox` tabloları

`sandbox` şemasının sahibi analist grubudur; bu yol yalnız `lakehouse-analysts`
grubundaki kullanıcılar içindir. Bir JupyterHub not defterinde önce Trino bağlantısını
kurun ([40-kurulum-sonrasi](../40-kurulum-sonrasi.md) §5 ile aynı hücre):

`[pod]` (JupyterHub not defteri hücresi)

```python
import os, trino
conn = trino.dbapi.connect(host=os.environ["TRINO_HOST"], port=8443, http_scheme="https",
                           verify="/etc/lakehouse-ca/tls.crt",
                           auth=trino.auth.OAuth2Authentication(), catalog="lakehouse")
```

Sonra tabloyu silin:

```python
cur = conn.cursor(); cur.execute("drop table sandbox.gunluk_ozet"); cur.fetchall()
```

**Beklenen çıktı:**

```text
[]
```

Trino tabloyu dosyalarıyla birlikte siler. Kontrolü Adım 7'dedir.

**Ters giderse:**

- `Access Denied: Cannot drop table …`: kullanıcı `lakehouse-analysts` grubunda değildir.
- `Failed to drop table …`: tablo `sandbox` dışındadır; 6.2'yi kullanın.

### 6.2 Bronze ve Silver tabloları

Trino, ürünün Bronze ve Silver ad alanlarına yazamaz. Bu tablolar ürünün Spark
kimliğiyle çalışan tek seferlik bir işle silinir. İşin üç parçası vardır: tabloları silen
küçük bir Python dosyası, onu kümeye taşıyan bir ConfigMap ve işi tanımlayan bir dosya.

**1. Python dosyasını yazın.** `TABLOLAR` listesine silinecek tabloları
`ad_alanı.tablo` biçiminde yazın. MongoDB koleksiyonlarında `__quarantine` tablosunu da
ekleyin.

`[bastion]`

```bash
mkdir -p ~/tablo-sil && cd ~/tablo-sil
cat > tablo_sil.py <<'EOF'
import os
from pyspark.sql import SparkSession
CATALOG = "lakehouse"
TABLOLAR = ["shop_raw.customers", "shop.customers"]
builder = SparkSession.builder.appName("tablo-sil")
cred = os.environ.get("POLARIS_CREDENTIAL")
if cred:
    builder = builder.config(f"spark.sql.catalog.{CATALOG}.credential", cred)
spark = builder.getOrCreate()
for t in TABLOLAR:
    spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{t} PURGE")
    print("silindi:", t)
print("DROP_OK", len(TABLOLAR))
spark.stop()
EOF
```

`PURGE` sözcüğü önemlidir: o olmadan tablo yalnız katalogdan düşer, S3'teki dosyaları
yerinde kalır.

**2. İş tanımını üretin.** Tanım dosyasını ürünün betiği üretir; kümeye özel ayarlar (S3
adresi, sertifika, katalog) kümedeki ürün işinden kopyalanır
([yeni-spark-uygulamasi.md](yeni-spark-uygulamasi.md) §4):

```bash
~/lakehouse/scripts/spark-is-tanimi.sh --ad tablo-sil --dosya tablo_sil.py > tablo-sil.yaml
```

**3. İşi çalıştırın.**

```bash
oc -n "$LAKEHOUSE_NS" create configmap tablo-sil --from-file=tablo_sil.py
oc -n "$LAKEHOUSE_NS" label configmap tablo-sil lakehouse.io/yonetim=manual-kaynak-veya-tablo-silme-6.2
oc apply -f tablo-sil.yaml
```

**Beklenen çıktı** (örnek):

```text
configmap/tablo-sil created
configmap/tablo-sil labeled
sparkapplication.sparkoperator.k8s.io/tablo-sil created
```

İş bir iki dakika sürer. Durumuna bakın; `STATUS` sütunu `COMPLETED` olana kadar bir
dakikada bir tekrarlayın:

```bash
oc -n "$LAKEHOUSE_NS" get sparkapplication tablo-sil
```

**Beklenen çıktı** (iş bittiğinde; tarihler sizde farklıdır):

```text
NAME        SUSPEND   STATUS      ATTEMPTS   START                  FINISH                 AGE
tablo-sil             COMPLETED   1          2026-11-11T14:53:01Z   2026-11-11T14:54:16Z   83s
```

Sonucu görün ve işi temizleyin. `SparkApplication` silinince ConfigMap kümede kalır;
ikisini de silin:

```bash
oc -n "$LAKEHOUSE_NS" logs tablo-sil-driver | grep DROP_OK
oc -n "$LAKEHOUSE_NS" delete sparkapplication tablo-sil
oc -n "$LAKEHOUSE_NS" delete configmap tablo-sil
```

**Beklenen çıktı** (örnek — sayı, listedeki tablo sayısıdır):

```text
DROP_OK 2
sparkapplication.sparkoperator.k8s.io "tablo-sil" deleted from lakehouse namespace
configmap "tablo-sil" deleted from lakehouse namespace
```

**Ters giderse:**

- Durum `FAILED` ise sürücü günlüğüne bakın:
  `oc -n "$LAKEHOUSE_NS" logs tablo-sil-driver | tail -50`. `PKIX` ya da `SSL` geçen bir
  hata, tanım dosyasının elle yazılıp sertifika satırlarının unutulduğunu gösterir;
  dosyayı 2. adımdaki betikle yeniden üretin.
- `AlreadyExists` görürseniz önceki bir denemeden kalan nesneler vardır; önce iki `delete`
  komutunu çalıştırın.
- Tablo listede hâlâ duruyorsa adını yanlış yazmışsınızdır: `IF EXISTS` yanlış adı hata
  vermeden geçer.

### 6.3 Silinen tabloların klasörleri kendiliğinden temizlenmez

Ürünün gecelik bakım işi (`maint-expire-orphan-ttl`) **var olan** tabloların klasörlerinde
hiçbir tabloya ait olmayan eski dosyaları toplar. Silinmiş bir tablonun klasörü artık bir
tabloya ait olmadığı için bu işin kapsamında değildir. O klasörde kalan dosyalar Adım 7'de
kontrol edilir ve gerekirse elle silinir.

---

## 7. S3'te dosya kalmadığını kontrol edin

`PURGE` tablonun katalogda kayıtlı bütün dosyalarını siler. Ama Iceberg yazıcısının
yazıp henüz tabloya bağlamadığı dosyalar katalogda kayıtlı değildir; bu dosyalar klasörde
kalabilir.

Bu adım kurumun S3 istemcisini (`aws`) ve lakehouse bucket'ında okuma–silme yetkisi olan
bir S3 kimliğini ister.

`[bastion]`

```bash
aws --endpoint-url "$S3_ENDPOINT" s3 ls "s3://$S3_BUCKET_DATA/shop_raw/customers/" --recursive
aws --endpoint-url "$S3_ENDPOINT" s3 ls "s3://$S3_BUCKET_DATA/shop/customers/" --recursive
```

**Beklenen çıktı:** iki komut da bir şey yazmaz; klasörler boştur.

**Klasörde dosya kaldıysa** listeye bakın:

- Yalnız `data/` altında birkaç `.parquet` dosyası varsa ve listede `metadata/` geçen bir
  satır **yoksa**, bunlar tabloya hiç bağlanmamış dosyalardır. Güvenle silinir:

  ```bash
  aws --endpoint-url "$S3_ENDPOINT" s3 rm "s3://$S3_BUCKET_DATA/shop_raw/customers/" --recursive
  ```

- Listede `metadata/` geçiyorsa **silmeyin**: tablo `PURGE` olmadan silinmiş ya da hiç
  silinmemiştir. Adım 6'yı kontrol edin.

**Bu silme geri alınamaz.** Yalnız silinmiş bir tablonun klasöründe ve yalnız yukarıdaki
listede `metadata/` olmadığını gördükten sonra çalıştırın.

---

## 8. Kaynak veritabanını temizleyin

Lakehouse tarafı bittikten sonra kaynak veritabanında CDC izleri kalır. Temizlenmezse
kaynak sunucu zarar görebilir: örneğin PostgreSQL'de kimsenin okumadığı bir çoğaltma
yuvası eski değişiklik günlüklerinin silinmesini engeller ve disk dolar.

### 8.1 PostgreSQL

**Tek tablo kaldırıldıysa** tabloyu publication'dan çıkarın ve CDC kullanıcısının okuma
iznini geri alın:

`[kaynak DB]`

```sql
ALTER PUBLICATION dbz_shop_pub DROP TABLE public.customers;
REVOKE SELECT ON public.customers FROM dbz;
```

**Beklenen çıktı:**

```text
ALTER PUBLICATION
REVOKE
```

**Kaynağın tamamı kaldırıldıysa** çoğaltma yuvasını ve publication'ı silin. Önce
bakın; yuvanın adı `debezium_` ile, publication'ın adı `dbz_` ile başlar ve ardından
kaynak adı gelir:

`[kaynak DB]`

```sql
SELECT slot_name, plugin, active FROM pg_replication_slots;
SELECT pubname FROM pg_publication;
```

**Beklenen çıktı** (`shop` kaynağı için):

```text
   slot_name   |  plugin  | active 
---------------+----------+--------
 debezium_shop | pgoutput | t
(1 row)

   pubname    
--------------
 dbz_shop_pub
(1 row)
```

`active` sütunu `f` olduğunda (bağlayıcı kalktığında) silin:

```sql
SELECT pg_drop_replication_slot('debezium_erp');
DROP PUBLICATION dbz_erp_pub;
```

**Beklenen çıktı** (örnek):

```text
 pg_drop_replication_slot 
--------------------------
 
(1 row)

DROP PUBLICATION
```

CDC kullanıcısı başka bir kaynak için kullanılmıyorsa onu da kaldırabilirsiniz. Sinyal
tablosu (`debezium_signal`) artık kullanılmaz; kurum isterse silebilir.

**Ters giderse:**

- `replication slot "debezium_erp" is active for PID …`: bağlayıcı hâlâ bağlıdır; Adım 4'te
  bağlayıcının kümeden silindiğini kontrol edin.
- `replication slot "debezium_erp" does not exist`: yuva zaten yoktur.

### 8.2 SQL Server

SQL Server'da CDC tablo düzeyinde kapatılır; kaynağın tamamı kalkıyorsa veritabanı
düzeyinde de kapatılır.

`[kaynak DB]`

```sql
-- kaldırılan her tablo için
EXEC sys.sp_cdc_disable_table @source_schema = N'dbo', @source_name = N'customers',
                              @capture_instance = N'all';

-- kaynağın tamamı kalkıyorsa (yakalama ve temizleme işlerini de durdurur)
EXEC sys.sp_cdc_disable_db;
```

Bu yordamlar başarılı olduğunda bir şey yazmaz. `sp_cdc_disable_table` çağrılmazsa CDC
değişiklik tabloları dolmaya devam eder ve kaynak veritabanı büyür.

**Ters giderse:**

- `The specified '@capture_instance' is not valid`: tabloda CDC zaten kapalıdır.
  Açık olanları şu sorguyla görürsünüz: `SELECT capture_instance FROM cdc.change_tables;`
- Yetki hatası: bağlandığınız hesap `db_owner` değildir.

### 8.3 MongoDB

MongoDB'de kaynak tarafında yapılacak bir şey yoktur; CDC sunucuda kalıcı bir iz
bırakmaz. İsterseniz CDC kullanıcısının yetkisini geri alırsınız.

---

## 9. Kaynağın tamamı kalkıyorsa: kalan izler

### 9.1 Secret

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" delete secret erp-db
```

**Beklenen çıktı** (örnek — `erp` kaynağı için):

```text
secret "erp-db" deleted from lakehouse namespace
```

Secret'ı Adım 4'ten **önce** silmeyin: bağlayıcı kümedeyken parolası kaybolursa hata
verir ve `glue` uygulaması gereksiz yere `Degraded` görünür.

**Ters giderse:** `NotFound`: Secret zaten silinmiştir. Erken sildiyseniz
[yeni-kaynak-ve-pipeline.md](yeni-kaynak-ve-pipeline.md) §4 ile yeniden yaratın, Adım 4'ün
bitmesini bekleyin, sonra silin.

### 9.2 Polaris ad alanları

Kaynağın Bronze ve Silver ad alanları katalogda kalır. Ad alanı içinde tablo varken
silinemez; Adım 6 bitmiş olmalıdır.

Önce ad alanlarını dosyadan çıkarın. `platform/polaris/setup.yaml` dosyasını açın,
`namespaces:` listesinden `erp_raw` ve `erp` satırlarını silin, sonra gönderin:

`[bastion]`

```bash
vi platform/polaris/setup.yaml
git add platform/polaris/setup.yaml
git commit -m "polaris: erp ad alanları çıkarıldı"
git push origin main
```

Kurulum betiği ad alanı silmez, yalnız eksikleri yaratır; katalogdan silmek ayrı bir
adımdır. Polaris yönetici kimliği `polaris-root` Secret'ından okunur:

```bash
CLIENT_ID=$(oc -n "$LAKEHOUSE_NS" get secret polaris-root -o jsonpath='{.data.clientId}' | base64 -d)
CLIENT_SECRET=$(oc -n "$LAKEHOUSE_NS" get secret polaris-root -o jsonpath='{.data.clientSecret}' | base64 -d)
export CLIENT_ID CLIENT_SECRET
oc -n "$LAKEHOUSE_NS" port-forward svc/polaris 8181:8181 >/dev/null 2>&1 &
PF=$!
sleep 5
for ns in erp_raw erp; do
  PATH="$PWD/.venv/bin:$PATH" polaris namespaces delete --catalog lakehouse "$ns"
done
PATH="$PWD/.venv/bin:$PATH" polaris namespaces list --catalog lakehouse
kill "$PF"
unset CLIENT_ID CLIENT_SECRET
```

**Beklenen çıktı:** silme komutları bir şey yazmaz. Son komut katalogda kalan ad
alanlarını listeler; sildikleriniz listede **olmamalıdır**. Örneğin:

```text
{"namespace": "nginx_raw"}
{"namespace": "shop"}
{"namespace": "shop_raw"}
{"namespace": "sandbox"}
{"namespace": "ops"}
```

**Ters giderse:**

- `Namespace … is not empty`: ad alanında hâlâ tablo vardır; Adım 6'yı kontrol edin.
- Bağlantı hatası: port yönlendirmesi açılmamıştır; birkaç saniye bekleyip son üç komutu
  tekrarlayın.
- `polaris: command not found`: `.venv` klasörü yoktur
  ([40-kurulum-sonrasi](../40-kurulum-sonrasi.md) §2.1).

### 9.3 Superset ve diğer referanslar

Silinen tabloları kullanan Superset veri setleri, grafikleri ve panoları artık hata
verir. Superset'te **Datasets** listesinden o tablolara bağlı veri setlerini, **Charts**
listesinden onları kullanan grafikleri silin; panoları gözden geçirin. Aynı kontrolü
kurumun kendi JupyterHub not defterleri, Zeppelin notları ve Spark işleri için yapın; ürün
bunları göremez.

---

## 10. Sık karşılaşılan durumlar

| Ne görüyorsunuz | Neden | Ne yapılır |
|---|---|---|
| Bağlayıcı kalktı ama Kafka konuları duruyor | konular bağlayıcıyla birlikte silinmez | Adım 5 (ya da saklama süresinin dolmasını bekleyin) |
| Trino'da `Access Denied: Cannot drop table …` | `sandbox`'ta kullanıcı analist grubunda değil; başka ad alanında Trino silemez | Adım 6.1 ya da 6.2 |
| Tablo katalogdan gitti ama S3'te dosyalar duruyor | tabloya bağlanmamış dosyalar ya da `PURGE` unutulmuş | Adım 7 |
| Kaynak PostgreSQL'in diski doluyor | okunmayan çoğaltma yuvası eski günlükleri tutuyor | Adım 8.1 |
| `pg_drop_replication_slot` yuvanın etkin olduğunu söylüyor | bağlayıcı hâlâ bağlı | Adım 4'te bağlayıcının silindiğini kontrol edin |
| Kaynak aynı adla yeniden eklendi, tablolar boş kaldı | eski konum duruyor | Adım 3 (bağlayıcı hâlâ varken) ya da başka bir kaynak adı |
| `glue` uygulaması `Degraded`, bağlayıcı kimlik hatası veriyor | Secret bağlayıcıdan önce silindi | Adım 9.1 Ters giderse |
| Superset panosu tablo bulunamadı diyor | pano silinen tabloya bakıyor | Adım 9.3 |
| Polaris ad alanı silinemiyor | içinde tablo kalmış | Adım 6 |

Bütün belirtiler: [sorun-giderme.md](sorun-giderme.md) §3.

---

## Kontrol listesi

- [ ] Tablonun ya da kaynağın kullanılmadığı doğrulandı; saklanacak veri varsa kopyası
      alındı.
- [ ] (Aynı ad yeniden kullanılacaksa) bağlayıcı kalkmadan önce durduruldu ve konumu
      silindi.
- [ ] Tablo ya da kaynak ayar dosyasından çıkarıldı; `glue` uygulaması `Synced Healthy`.
- [ ] Kafka konuları silindi ya da saklama süresine bırakıldı.
- [ ] Bronze ve Silver tabloları `PURGE` ile silindi; tablo silme işi ve ConfigMap'i
      kaldırıldı.
- [ ] S3'te tablo klasörleri boş.
- [ ] Kaynak veritabanında CDC izleri temizlendi.
- [ ] (Kaynağın tamamı) Secret silindi, Polaris ad alanları kaldırıldı, Superset ve not
      defteri referansları temizlendi.

## Sonraki bölüm

Yeni bir kaynak bağlamak: [yeni-kaynak-ve-pipeline.md](yeni-kaynak-ve-pipeline.md).
Kendi Spark işinizi eklemek: [yeni-spark-uygulamasi.md](yeni-spark-uygulamasi.md).
Değişikliğin Git'ten kümeye yolu:
[değişiklik nasıl uygulanır](degisiklik-nasil-uygulanir.md).
