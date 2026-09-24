# Kaynak ya da tablo kaldırma

**Bu bölümde:** bir tabloyu ya da bir kaynağın tamamını üründen **verisiyle birlikte**
çıkarmanın tam sırası — values dosyasından çıkarma ve ArgoCD'nin kaldırdıkları, geride
kalan Kafka konuları, Iceberg tablolarının veriyle silinmesi, S3 klasörünün boşaldığının
doğrulanması, kaynak veritabanındaki CDC izlerinin temizlenmesi, aynı adla yeniden
ekleyecekseniz Debezium offset'lerinin sıfırlanması ve kaynağın tamamı kalkıyorsa Secret,
Polaris ad alanı ve pano referansları.
**Süre:** 30 dakika + Spark silme işinin koşu süresi (küçük tablolarda dakikalar).
**Gereken yetki:** kurumun Git deposunda `main` dalına yazma; `$LAKEHOUSE_NS` ad alanında
`apply` ve `delete`; kaynak veritabanında çoğaltma yuvasını düşürebilecek hesap; Polaris
kök kimliği (`polaris-root` Secret'ı) yalnız ad alanı kaldırılacaksa.
**Nerede çalıştırılır:** `[bastion]` küme ve Git komutları için; `[kaynak DB]` kaynak
veritabanı istemcisinin koştuğu makine; `[pod]` JupyterHub not defteri hücresi.

**Bu bölümde iki işlenmiş örnek vardır.** *Tablo kaldırma* örneği `shop` kaynağının
`public.customers` tablosudur (Bronze `shop_raw.customers`, Silver `shop.customers`); bu
örnek geliştirme kümesinde birebir koşturulmuştur ve aşağıdaki çıktıların çoğu o koşudan
alınmıştır. *Kaynağın tamamını kaldırma* örneği
[yeni-kaynak-ve-pipeline.md](yeni-kaynak-ve-pipeline.md) bölümündeki `erp` kaynağıdır; o
kaynak geliştirme kümesinde kurulu olmadığı için Adım 9'un çıktıları "örnek" etiketiyle
verilmiştir.

> ## Geri dönüş yoktur
>
> Bu bölümdeki adımlar **silme** adımlarıdır. Iceberg tablosu `PURGE` ile düşürüldüğünde
> katalog kaydı da S3'teki veri dosyaları da gider; Kafka konusu silindiğinde içindeki
> olaylar gider; çoğaltma yuvası düşürüldüğünde kaynaktaki değişiklik günlüğü ilerler ve
> aradaki değişiklikler bir daha okunamaz. `git revert` yalnız **yapılandırmayı** geri
> getirir, **veriyi getirmez**: values dosyası eski hâline dönse bile tablo boş olarak
> yeniden yaratılır ve ancak yeni bir snapshot ile dolar.
>
> **Başlamadan önce:** (1) tablonun gerçekten kimse tarafından kullanılmadığını doğrulayın
> (Superset panoları, kayıtlı sorgular, kurumun kendi Spark ve dbt işleri); (2) katalog
> kayıtları PostgreSQL'de durur ve ürünün yedeğine dâhildir, ama **S3'teki veri dosyaları
> ayrıca yedeklenmez** — saklamak istediğiniz bir şey varsa silmeden önce kendi kopyanızı
> alın (en kolayı `CREATE TABLE … AS SELECT` ile `sandbox` altına kopyalamaktır).

Sıra:

```text
(offset sıfırlama) -> values'tan çıkar -> commit -> ArgoCD prune -> Kafka konuları
  -> Iceberg tabloları (PURGE) -> S3 boş mu -> kaynak DB temizliği -> (kaynağın tamamı)
```

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

**Ters giderse:** boş satır görürseniz dosya yüklenmemiştir
([30-kurulum](../30-kurulum.md) §1).

---

## 2. Neyi kaldırdığınıza karar verin

İki durum vardır ve adımların kapsamı buna göre değişir.

| | Tek tablo | Kaynağın tamamı |
|---|---|---|
| Values'ta değişen | `sources[].tables` (ya da `collections`) listesinden bir satır + varsa `pipelines` girdisi | tüm `sources[]` girdisi + o kaynağın bütün `pipelines` girdileri |
| Kalkan bağlayıcı | yok (bağlayıcı yalnız yeniden başlar) | `dbz-` ve `sink-` bağlayıcıları |
| Silinecek Iceberg tablosu | Bronze + Silver (+ varsa karantina) | kaynağın bütün tabloları |
| Kaynak DB temizliği | tablo düzeyi (SQL Server'da CDC kapatma; PostgreSQL'de bir şey gerekmez) | çoğaltma yuvası + publication (PostgreSQL), veritabanı düzeyi CDC (SQL Server) |
| Ek temizlik | yok | Secret, Polaris ad alanı, pano referansları (Adım 9) |
| İzlenecek adımlar | 1, 2, 4, 5, 6, 7, 8 (Adım 3 gerekmez) | hepsi |

**Append-only tablolarda Silver yoktur.** `pipelines` listesinde yer almayan bir tablo
yalnız Bronze'da durur; Adım 6'da tek tablo düşürülür.

**MongoDB kaynaklarında karantina tablosu da vardır.** Her koleksiyonun Bronze adının
sonuna `__quarantine` eklenmiş ikinci bir tablosu bulunur
([yeni-kaynak-ve-pipeline.md](yeni-kaynak-ve-pipeline.md) §3.3); o da düşürülür.

---

## 3. Aynı adla yeniden ekleyecekseniz: önce offset'leri sıfırlayın

Bu adım **yalnız kaynağın tamamı kalkıyorsa ve** ileride aynı kaynak adı (aynı konu ön
eki) yeniden kullanılacaksa gerekir. Tek tablo çıkarırken bağlayıcı yerinde kaldığı için
offset'lere dokunulmaz; o tabloyu ileride geri eklerseniz geçmiş satırları **artımlı
snapshot** getirir
([mevcut-kaynaga-tablo-ekleme.md](mevcut-kaynaga-tablo-ekleme.md) §6).

Kafka Connect, bağlayıcının nerede kaldığını `connect-offsets` konusunda bağlayıcının
**adıyla** saklar; aynı adla kurulan yeni bir bağlayıcı bu konumu bulur,
baştan okumuş sayılır ve **snapshot almaz** — tablolarınız boş kalır.

**Sıra önemlidir: bu adım Adım 4'ten önce yapılır.** Offset uçlarının çalışması için
bağlayıcının kümede **hâlâ var olması** gerekir; bağlayıcı prune edildikten sonra aynı
çağrı `404` döner.

Üç çağrı sırayla yapılır: durdur, offset'leri sil, sonucu doğrula.

`[bastion]`

```bash
POD=connect-connect-0
API=http://localhost:8083/connectors/dbz-shop
oc -n "$LAKEHOUSE_NS" exec "$POD" -- \
  curl -s -o /dev/null -w 'stop: %{http_code}\n' -X PUT "$API/stop"
oc -n "$LAKEHOUSE_NS" exec "$POD" -- curl -s -X DELETE "$API/offsets"
echo
oc -n "$LAKEHOUSE_NS" exec "$POD" -- curl -s "$API/offsets"
```

**Beklenen çıktı** (kind kümesinde alınmış gerçek çıktı; komutlar aynı kümedeki
`dbz-crm` bağlayıcısında koşturulmuştur, yanıtların biçimi bağlayıcı adından bağımsızdır):

```text
stop: 204
{"message":"The Connect framework-managed offsets for this connector have been reset successfully. However, if this connector manages offsets externally, they will need to be manually reset in the system that the connector uses."}
{"offsets":[]}
```

Son satırdaki boş liste, bağlayıcının konumunun gerçekten silindiğini gösterir. Bu uçlar
Kafka Connect 3.6 ve sonrasında vardır; kümedeki sürümü
`oc -n "$LAKEHOUSE_NS" exec "$POD" -- curl -s http://localhost:8083/` çıktısındaki
`version` alanından görebilirsiniz (ürün Kafka 4.3.1 ile gelir).

**Yapmak istemiyorsanız alternatif:** kaynağı yeniden eklerken **başka bir ad** verin ya
da `sources[].topicPrefix` alanına farklı bir değer yazın. Bağlayıcı adı ve konu ön eki
değişince Connect eski konumu bulamaz ve snapshot normal biçimde alınır. Bu yol offset
silmeye göre daha güvenlidir; karşılığında Kafka konularının ve Bronze ad alanının adı da
değişir.

**Ters giderse:** `{"error_code":404,"message":"Unknown connector dbz-shop"}` → bağlayıcı
kümede yoktur; Adım 4'ü zaten yapmışsınızdır. Bu durumda tek çıkış yolu yukarıdaki
alternatiftir (farklı ad ya da farklı konu ön eki). `409` alıyorsanız bağlayıcı `STOPPED`
durumuna geçmemiştir; birkaç saniye bekleyip silme çağrısını tekrarlayın.

---

## 4. Values'tan çıkarın, commit edin, eşitlemeyi izleyin

Ortak döngü: [değişiklik nasıl uygulanır](degisiklik-nasil-uygulanir.md).

`platform/values/site/glue.yaml` dosyasında tablo adı `tables` listesinden, varsa
`pipelines` girdisi de listeden **silinir**. Kaynağın tamamı kalkıyorsa `sources` altındaki
girdinin bütünü ve o kaynağa ait bütün `pipelines` satırları silinir.

Aşağıdaki parça, bu bölümün provasının koştuğu **geliştirme** kurulumundan alınmıştır
(`platform/values/glue-dev.yaml`); `host` bu yüzden küme içi bir servis adıdır. Üretimde
aynı alanlar `platform/values/site/glue.yaml` dosyasında, kaynak sunucunun gerçek DNS
adıyla durur.

```yaml
sources:
- name: shop
  type: postgres
  host: demo-pg-rw.lakehouse.svc                # üretimde kaynak sunucunun DNS adı
  port: 5432
  database: shop
  tables: [public.orders]                       # public.customers satırdan çıkarıldı
  signalTable: public.debezium_signal
pipelines:
- {bronze: shop_raw.orders, keys: [id], bucket_count: 4, casts: {updated_at: timestamp}}
# shop_raw.customers girdisi silindi
```

`[bastion]`

```bash
bash scripts/check-site.sh
git add platform/values/site/glue.yaml
git commit -m "site: shop kaynagindan public.customers cikarildi"
git push origin main
```

**Beklenen çıktı** (örnek — OpenShift'e özgü; eşitleme bittiğinde):

```text
Synced Healthy
```

Eşitleme sırasında Debezium bağlayıcısı **yeniden başlar**; PostgreSQL'de publication da
kendiliğinden daralır. Kaynağın tamamı kalktıysa `dbz-` ve `sink-` bağlayıcıları prune
edilir.

### 4.1 ArgoCD'nin kaldırdıkları ve kaldırmadıkları

Bu ayrım bölümün geri kalanının nedenidir.

| Nesne | Commit'ten sonra ne olur |
|---|---|
| `dbz-` ve `sink-` bağlayıcıları (kaynağın tamamı kalkarsa) | **silinir** (prune) |
| `connect` kullanıcısının konu ön eki ACL'i | **kendiliğinden daralır** (render'dan düşer) |
| Pipeline tanımlarının ConfigMap'i ve `silver-merge` zamanlı işi (son pipeline da kalktıysa) | **silinir** |
| Connect'in Secret okuma rolündeki Secret adı | **kendiliğinden daralır** |
| Kafka konuları (kaynağın konuları, `.dlq`, şema geçmişi, heartbeat) | **kalır** — Adım 5 |
| Iceberg tabloları ve S3 dosyaları | **kalır** — Adım 6 |
| Kaynak veritabanındaki yuva, publication, CDC tabloları | **kalır** — Adım 8 |
| Kaynağın Secret'ı ve Polaris ad alanları | **kalır** — Adım 9 |

**Ters giderse:** `Synced Degraded` → bağlayıcı yeni yapılandırmayla açılamamıştır;
`oc -n "$LAKEHOUSE_NS" get kafkaconnector` çıktısına ve bağlayıcının hata izine bakın
([yeni-kaynak-ve-pipeline.md](yeni-kaynak-ve-pipeline.md) §9.4).

---

## 5. Kafka konularını silin

**Kaynakların konuları `KafkaTopic` nesnesi değildir.** Ürün yalnız nginx akışının
konusunu sabit bir `KafkaTopic` olarak tanımlar; Debezium'un konularını bağlayıcının
kendisi yaratır. Yani bağlayıcı kalktığında konu yerinde kalır ve içindeki olaylar saklama
süresi dolana kadar durur.

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" get kafkatopic
```

**Beklenen çıktı** (kind kümesinde alınmış gerçek çıktı; kümede iki kaynak ve nginx akışı
çalışırken bile yalnız nginx'in konusu bir nesne olarak görünür):

```text
NAME           CLUSTER     PARTITIONS   REPLICATION FACTOR   READY
nginx.access   lakehouse   3            1                    True
```

Konuyu gerçekten silmek için broker'ın kendi aracı kullanılır:

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" exec lakehouse-dual-role-0 -c kafka -- bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list | grep '^shop\.'
oc -n "$LAKEHOUSE_NS" exec lakehouse-dual-role-0 -c kafka -- bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --delete --topic shop.public.customers
```

**Beklenen çıktı** (kind kümesinde alınmış gerçek liste; silme komutu başarılı olduğunda
hiçbir şey basmaz):

```text
shop.dlq
shop.public.customers
shop.public.debezium_signal
shop.public.orders
```

Kaynağın tamamı kalkıyorsa aynı ön ekle başlayan bütün konular ve yanlarındaki iki iç konu
da düşer: `schema-history.` + kaynak adı (yalnız SQL Server) ve `__debezium-heartbeat.` +
kaynak adı. `nginx.access` konusu ise bir `KafkaTopic` olduğundan values'ta
`nginx.enabled: false` yapıldığında **topic operatörü tarafından verisiyle birlikte**
kaldırılır; elle silinmez.

**Acele etmeyin.** Konuları hemen silmek zorunda değilsiniz: saklama süresi dolduğunda
veri kendiliğinden gider (Kafka'nın varsayılanı bir haftadır). Elle silmek yalnız yer
kazanmak ya da aynı adı hemen yeniden kullanmak istediğinizde gereklidir.

**Ters giderse:** `This server does not host this topic-partition` → konu adını yanlış
yazmışsınızdır; `--list` çıktısıyla karşılaştırın. Silme komutu takılıyorsa bağlayıcı hâlâ
konuyu tüketiyordur; Adım 4'ün eşitlemesinin bittiğini doğrulayın.

---

## 6. Iceberg tablolarını verisiyle silin

Ürünün kataloğunda **iki farklı yetki dünyası** vardır ve tablonun hangi ad alanında
olduğuna göre silme yolu değişir.

| Tablo nerede | Kim silebilir | Nasıl |
|---|---|---|
| `sandbox` (analistlerin ve kendi işlerinizin alanı) | analist grubu | Trino'da doğrudan `DROP TABLE` |
| Bronze ve Silver (ürünün ad alanları) | ürünün `spark` kimliği | tek seferlik Spark işi (Adım 6.2) |

### 6.1 `sandbox` tabloları — Trino

Bu yol yalnız **analist** grubundaki kullanıcılar içindir: `sandbox` şemasının sahipliği o
gruba verilmiştir. Salt okuma yetkisiyle bağlanan servis hesapları ve sıradan kullanıcılar
`Access Denied: Cannot drop table …` alır.

Aşağıdaki hücreler bir Trino bağlantısı (`conn`) bekler; not defterinde bir kez kurulur
ve bu bölümün geri kalanında yeniden kullanılır
([40-kurulum-sonrasi](../40-kurulum-sonrasi.md) §5 ile aynı hücre):

`[pod]` (JupyterHub not defteri hücresi)

```python
import os, trino
conn = trino.dbapi.connect(host=os.environ["TRINO_HOST"], port=8443, http_scheme="https",
                           verify="/etc/lakehouse-ca/tls.crt",
                           auth=trino.auth.OAuth2Authentication(), catalog="lakehouse")
```

`[pod]` (JupyterHub not defteri hücresi)

```python
cur = conn.cursor(); cur.execute("drop table sandbox.gunluk_ozet"); cur.fetchall()
```

**Beklenen çıktı** (kind kümesinde gerçekten koşturuldu — `analyst1` kullanıcısıyla; Trino
komut satırı `DROP TABLE` satırını basar, Python istemcisi boş bir liste döndürür):

```text
[]
```

Katalogda "veriyle birlikte silme" açıktır (`platform/polaris/setup.yaml` içindeki
`polaris.config.drop-with-purge.enabled`): Trino silme isteğini veriyi de silme bayrağıyla
gönderir ve S3 dosyaları aynı anda gider. Doğrulama Adım 7'dedir.

**Ters giderse:** `Access Denied: Cannot drop table …` → kullanıcı analist grubunda
değildir. `Failed to drop table …` → tablo `sandbox` dışındadır; Adım 6.2'ye geçin.

### 6.2 Bronze ve Silver tabloları — tek seferlik Spark işi

Trino ürünün ad alanlarına **yazamaz**: Polaris'teki `trino` kimliği yalnız okuma ve
`sandbox` yazma rollerindedir. Yönetici bir kullanıcıyla denerseniz Trino'nun kendi erişim
kuralı sizi geçirir ama Polaris isteği reddeder ve sorgu
`Failed to drop table` ile düşer.

**Beklenen çıktı** (kind kümesinde alınmış gerçek Polaris günlük satırı — yönetici
grubundaki bir kullanıcının Bronze tablosunu düşürme denemesi):

```text
Authorization denied for principal 'trino' on operation 'DROP_TABLE_WITH_PURGE': missing TABLE_DROP on TABLE_LIKE 'customers', TABLE_WRITE_DATA on TABLE_LIKE 'customers'
```

Doğru yol, ürünün `spark` kimliğiyle koşan tek seferlik bir Spark işidir; o kimlik
katalogda içerik yönetme yetkisine sahiptir. Betik
[yeni-spark-uygulamasi.md](yeni-spark-uygulamasi.md) §3'teki iskeletle yazılır ve gövdesi
üç satırdır:

```python
spark.sql("DROP TABLE IF EXISTS lakehouse.shop_raw.customers PURGE")
spark.sql("DROP TABLE IF EXISTS lakehouse.shop.customers PURGE")
print("DROP_OK")
```

**`PURGE` sözcüğü zorunludur.** `PURGE` olmadan tablo yalnız katalogdan düşer; S3'teki veri
ve metadata dosyaları **yerinde kalır** ve kimsenin bakmadığı bir yer kaplar. Aynı nedenle
Polaris komut satırındaki `tables delete` alt komutu da kullanılmaz: o komut tablonun
yalnız **kaydını** katalogdan çıkarır, dosyalara dokunmaz.

Betik bir ConfigMap'e konur ve tek seferlik CR elle uygulanır; adım adım yordam
[yeni-spark-uygulamasi.md](yeni-spark-uygulamasi.md) §5.2'dedir. Koşu bitince:

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" logs tablo-sil-driver --tail=200 | grep DROP_OK
oc -n "$LAKEHOUSE_NS" delete sparkapplication tablo-sil --ignore-not-found
```

**Beklenen çıktı** (kind kümesinde alınmış gerçek satır):

```text
DROP_OK
```

Tabloların gittiğini katalogdan doğrulayın:

`[pod]` (JupyterHub not defteri hücresi)

```python
cur = conn.cursor(); cur.execute("show tables from shop_raw"); cur.fetchall()
```

**Beklenen çıktı** (kind provasından gerçek — `customers` listeden düşmüştür):

```text
[['orders']]
```

**Ters giderse:** iş `FAILED` olduysa sürücü günlüğüne bakın
([yeni-spark-uygulamasi.md](yeni-spark-uygulamasi.md) §7); `403` kimlik satırının
eksikliğidir, `404` tablonun zaten silinmiş olduğunu söyler. Tablo listede duruyorsa
betikte tablo adını yanlış yazmışsınızdır — `IF EXISTS` yanlış adı sessizce geçer.

### 6.3 Yetim dosyalar

Bir tablo düşürüldükten sonra hiçbir tabloya ait olmayan dosyalar kalabilir (yarım kalmış
yazmalar, daha önce `PURGE` olmadan yapılmış silmeler). Ürünün gecelik bakım işi bunları
`remove_orphan_files` ile toplar:

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" get scheduledsparkapplication maint-expire-orphan-ttl
```

**Beklenen çıktı** (kind kümesinde alınmış gerçek çıktı; üretimde `SUSPEND` sütunu `false`
olur ve iş gecelik koşar):

```text
NAME                      SCHEDULE     TIMEZONE   SUSPEND   LAST RUN   LAST RUN NAME   AGE
maint-expire-orphan-ttl   45 2 * * *              true                                 5d14h
```

Bakım işinin yetim dosya eşiği varsayılan olarak üç gündür: **üç günden eski** dosyaları
toplar. Eşik ürünün varsayılanıdır (`glue/values.yaml` içinde `spark.maintenance`
başlığı); gerekirse site dosyasında ezilir. Yeni sildiğiniz bir tablonun artıkları o
yüzden hemen kaybolmaz; bir sonraki hafta bakın.

**Ters giderse:** Spark işi `403` ile düşerse betiğin `POLARIS_CREDENTIAL` satırı eksiktir
([yeni-spark-uygulamasi.md](yeni-spark-uygulamasi.md) §3). Tablo bulunamadı hatası
tablonun zaten silindiğini söyler; `IF EXISTS` ile yazıldığında bu hata çıkmaz.

---

## 7. S3 klasörünün boşaldığını doğrulayın

`PURGE` ile silme dosyaları gerçekten kaldırdı mı? Ürünün veri bucket'ında tablonun
klasörüne bakılır.

**Üretimde** bakılacak yer kurumun kendi S3'üdür ve kurumun kendi istemcisi kullanılır:

`[bastion]`

```bash
aws --endpoint-url "$S3_ENDPOINT" s3 ls "s3://$S3_BUCKET_DATA/shop_raw/"
aws --endpoint-url "$S3_ENDPOINT" s3 ls "s3://$S3_BUCKET_DATA/shop_raw/customers/" --recursive
```

**Geliştirme kurulumunda** depolama küme içindeki MinIO'dur; aynı sorgu onun kendi
istemcisiyle yapılır (kök kimliği pod'un ortam değişkenlerindedir):

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" exec deploy/minio -- sh -c \
  'mc alias set local http://localhost:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null
   mc ls local/lakehouse/shop_raw/
   mc ls --recursive local/lakehouse/shop_raw/customers/'
```

**Beklenen çıktı** (kind kümesinde alınmış gerçek çıktı; ilk listede yalnız `orders`
kalmıştır, ikinci liste hiçbir satır basmaz — klasör boştur):

```text
[2026-09-24 10:55:43 UTC]     0B orders/
```

Silmeden **önce** aynı komut `customers/` satırını da basıyordu; karşılaştırma yapabilmek
için silme işinden önce bir kez çalıştırmanız yararlıdır.

**Ters giderse:** dosyalar duruyorsa `PURGE` yazılmamıştır (Adım 6.2) ya da katalogda
veriyle birlikte silme kapalıdır. İkincisi `platform/polaris/setup.yaml` içindeki
`polaris.config.drop-with-purge.enabled` anahtarıyla açılır; **var olan** bir katalogda bu
özelliğin ayrıca `polaris catalogs update` ile uygulanması gerekir. Dosyalar katalog kaydı
olmadan kalmışsa Adım 6.3'teki yetim toplama işi onları alır.

---

## 8. Kaynak veritabanını temizleyin

Küme tarafındaki her şey kalktıktan sonra kaynakta izler kalır. Bunlar temizlenmezse —
özellikle PostgreSQL'de — **kaynak sunucu zarar görür**: kimsenin okumadığı bir çoğaltma
yuvası WAL dosyalarının silinmesini engeller ve disk dolar.

### 8.1 PostgreSQL

Yuva ve publication yalnız **kaynağın tamamı** kalktığında düşürülür; tek tablo
çıkarıldığında publication zaten kendiliğinden daralmıştır.

Önce ne olduğuna bakın:

`[kaynak DB]`

```sql
SELECT slot_name, plugin, active FROM pg_replication_slots;
SELECT pubname FROM pg_publication;
```

**Beklenen çıktı** (kind kümesindeki PostgreSQL'den alınmış gerçek çıktı; yuva adı
`debezium_` + kaynak adı, publication adı `dbz_` + kaynak adı + `_pub` biçimindedir):

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

`active` sütunu `f` olduğunda — yani bağlayıcı gerçekten kalktığında — düşürülür:

`[kaynak DB]`

```sql
SELECT pg_drop_replication_slot('debezium_erp');
DROP PUBLICATION dbz_erp_pub;
```

**Beklenen çıktı** (örnek — `psql` fonksiyonun boş dönüşünü, ardından komut etiketini
basar):

```text
 pg_drop_replication_slot 
--------------------------
 
(1 row)

DROP PUBLICATION
```

CDC rolü başka bir kaynak için kullanılmıyorsa o da kaldırılır; sinyal tablosu kurum
isterse durabilir, ürün için bir anlamı kalmaz.

**Ters giderse:** `replication slot "debezium_erp" is active for PID …` → bağlayıcı hâlâ
bağlıdır; Adım 4'ün eşitlemesinin bittiğini ve bağlayıcı nesnesinin gerçekten silindiğini
doğrulayın. `replication slot "debezium_erp" does not exist` → yuva zaten yoktur, bir şey
yapmanız gerekmez.

### 8.2 SQL Server

SQL Server'da CDC tablo düzeyinde kapatılır; kaynağın tamamı kalkıyorsa veritabanı
düzeyindeki CDC de kapatılabilir.

`[kaynak DB]`

```sql
-- kaldırılan her tablo için
EXEC sys.sp_cdc_disable_table @source_schema = N'dbo', @source_name = N'customers',
                              @capture_instance = N'all';

-- kaynağın tamamı kalkıyorsa (yakalama ve temizleme işlerini de durdurur)
EXEC sys.sp_cdc_disable_db;
```

**Beklenen çıktı** (örnek — yordamlar sessizdir):

```text
Commands completed successfully.
```

`sp_cdc_disable_table` çağrılmazsa CDC değişiklik tabloları dolmaya devam eder ve kaynak
veritabanı büyür.

**Ters giderse:** `The specified '@capture_instance' is not valid` → tabloda CDC zaten
kapalıdır ya da yakalama örneğinin adı farklıdır; adları
`SELECT capture_instance FROM cdc.change_tables;` ile listeleyin. Yetki hatası alıyorsanız
bağlandığınız hesap `db_owner` değildir.

### 8.3 MongoDB

MongoDB'de kaynak tarafında **yapılacak bir şey yoktur**: Debezium change stream okur,
sunucuda kalıcı bir nesne bırakmaz. İsterseniz yalnız CDC kullanıcısının yetkisini geri
alırsınız.

---

## 9. Kaynağın tamamı kalkıyorsa: kalan izler

Bu adım yalnız bir kaynağın tamamı üründen çıkarıldığında yapılır. Örnek kaynak `erp`'dir.

### 9.1 Secret

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" delete secret erp-db
```

**Beklenen çıktı** (örnek):

```text
secret "erp-db" deleted
```

Secret'ı **Adım 4'ten önce silmeyin**: bağlayıcı hâlâ ayaktayken kimlik bilgisi kaybolursa
prune edilmeden önce hata durumuna düşer ve `glue` uygulaması gereksiz yere `Degraded`
görünür.

**Ters giderse:** `NotFound` → Secret zaten silinmiştir, bir şey yapmanız gerekmez.
Secret'ı erken sildiyseniz aynı adla ve aynı iki anahtarla (`username`, `password`) geri
yaratın ([yeni-kaynak-ve-pipeline.md](yeni-kaynak-ve-pipeline.md) §4), eşitlemenin
bitmesini bekleyin ve sonra silin.

### 9.2 Polaris ad alanları

Kaynağın Bronze ve Silver ad alanları katalogda kalır. **Boş olmadıkça silinemezler**;
yani Adım 6 bitmiş olmalıdır. İki iş vardır: dosyadan çıkarmak ve katalogdan silmek.

Önce `platform/polaris/setup.yaml` içindeki `namespaces:` listesinden iki satır çıkarılır
ve commit edilir. **Kurulum betiği ad alanı silmez** — idempotenttir, yalnız eksikleri
yaratır; bu yüzden katalogdan kaldırma ayrıca yapılır. Kök kimliği `polaris-root`
Secret'ından okunur:

`[bastion]`

```bash
POLARIS_ID=$(oc -n "$LAKEHOUSE_NS" get secret polaris-root \
  -o jsonpath='{.data.clientId}' | base64 -d)
POLARIS_SECRET=$(oc -n "$LAKEHOUSE_NS" get secret polaris-root \
  -o jsonpath='{.data.clientSecret}' | base64 -d)
oc -n "$LAKEHOUSE_NS" port-forward svc/polaris 8181:8181 &
PATH="$PWD/.venv/bin:$PATH" polaris --client-id "$POLARIS_ID" --client-secret "$POLARIS_SECRET" \
  namespaces delete --catalog lakehouse erp_raw
PATH="$PWD/.venv/bin:$PATH" polaris --client-id "$POLARIS_ID" --client-secret "$POLARIS_SECRET" \
  namespaces list --catalog lakehouse
```

`polaris` komut satırı aracı `scripts/polaris-setup.sh` ile aynı sanal ortamdan gelir
([40-kurulum-sonrasi](../40-kurulum-sonrasi.md) §2.1). Silme komutunun kendisi başarılı
olduğunda hiçbir şey basmaz; son satır ad alanının listeden düştüğünü gösterir. İş bitince
arka planda bıraktığınız port yönlendirmesini kapatın (`kill %1`).

**Beklenen çıktı** (kind kümesinde alınmış gerçek `namespaces list` çıktısı — o kümede
`erp` kaynağı hiç kurulmadığı için listede yalnız oradaki ad alanları vardır; sizde
silinen iki ad alanı listede **bulunmamalıdır**):

```text
{"namespace": "nginx_raw"}
{"namespace": "shop"}
{"namespace": "shop_raw"}
{"namespace": "smoke_vended"}
{"namespace": "sandbox"}
{"namespace": "crm"}
{"namespace": "crm_raw"}
```

**Ters giderse:** ad alanı boş değilse silme reddedilir; içinde kalan tabloyu
`namespaces summarize` alt komutuyla bulup Adım 6'ya dönün. Bağlantı hatası alıyorsanız
port yönlendirmesi ayakta değildir.

### 9.3 Kendiliğinden kalkanlar

- **Kafka ACL'leri.** Kaynağın konu ön ekine verilmiş izinler `sources` listesinden
  üretilir; girdi silinince render'dan da düşerler. Elle bir şey yapılmaz
  (`oc -n "$LAKEHOUSE_NS" get kafkauser connect -o yaml` ile doğrulanabilir).
- **Connect'in Secret okuma izni.** Kaynağın Secret adına verilen okuma hakkı aynı şekilde
  kendiliğinden daralır.

### 9.4 Pano ve sorgu referansları

Kaldırılan tablolar Superset'te veri kümesi, grafik ve pano olarak referans edilmiş
olabilir; artık olmayan bir tabloya bakan grafik hata verir. Superset arayüzünde **Data →
Datasets** listesinden ilgili kayıtlar silinir, panolardaki grafikler gözden geçirilir.
Aynı denetim kurumun kendi JupyterHub not defterleri, Zeppelin notları ve dbt modelleri
için de gerekir — bunları ürün göremez.

---

## 10. Sık durumlar

| Belirti | Neden | Çözüm |
|---|---|---|
| Bağlayıcı prune edildi ama Kafka konuları duruyor | kaynak konuları `KafkaTopic` nesnesi değildir | Adım 5 (ya da saklama süresi dolsun) |
| Trino'da `Access Denied: Cannot drop table …` | `sandbox` tablosunda: kullanıcı analist grubunda değil; başka ad alanında: hiçbir Trino kullanıcısı düşüremez | Adım 6.1 (yetki) ya da Adım 6.2 (Spark işi) |
| Trino'da `Failed to drop table …`, Polaris günlüğünde yetki reddi | Trino'nun katalog kimliği yalnız `sandbox`'a yazabilir | Adım 6.2'deki Spark işi |
| Tablo katalogdan gitti ama S3'te klasör duruyor | `PURGE` yazılmadı ya da katalogda veriyle silme kapalı | Adım 7'nin "Ters giderse" satırı |
| Kaynak PostgreSQL'in diski doluyor | kimsenin okumadığı çoğaltma yuvası WAL'ı tutuyor | Adım 8.1 |
| `pg_drop_replication_slot` yuvanın etkin olduğunu söylüyor | bağlayıcı hâlâ bağlı | Adım 4 eşitlemesinin bittiğini doğrulayın |
| Kaynak aynı adla yeniden eklendi, tablolar boş kaldı | eski offset'ler duruyor, snapshot atlandı | Adım 3 (bağlayıcı varken) ya da farklı konu ön eki |
| `glue` uygulaması `Degraded`, bağlayıcı kimlik hatası veriyor | Secret bağlayıcıdan önce silindi | Secret'ı geri yaratın, sonra sırayı izleyin (Adım 9.1) |
| Superset panosu tablo bulunamadı diyor | pano silinen tabloya bakıyor | Adım 9.4 |
| Polaris ad alanı silinemiyor | içinde tablo kalmış | Adım 6'yı tamamlayın |

Belirtilerin tam tablosu: [sorun-giderme.md](sorun-giderme.md) §3.

---

## Kontrol listesi

- [ ] Tablonun ya da kaynağın kullanılmadığı doğrulandı; saklanacak veri varsa kopyası
      alındı.
- [ ] (Aynı ad yeniden kullanılacaksa) bağlayıcı **kalkmadan önce** offset'ler sıfırlandı.
- [ ] `tables`/`collections` ve `pipelines` girdileri values'tan çıkarıldı; commit itildi;
      `glue` uygulaması `Synced Healthy`.
- [ ] Kaynağın Kafka konuları silindi ya da saklama süresine bırakıldı.
- [ ] Bronze, Silver (ve varsa karantina) tabloları `PURGE` ile düşürüldü.
- [ ] S3'te tablonun klasörü boş.
- [ ] Kaynakta çoğaltma yuvası ve publication düşürüldü (PostgreSQL) ya da CDC kapatıldı
      (SQL Server).
- [ ] (Kaynağın tamamı) Secret silindi, Polaris ad alanları kaldırıldı, pano ve not defteri
      referansları temizlendi.

## Sonraki bölüm

Kendi Spark uygulamanızı eklemek:
[yeni-spark-uygulamasi.md](yeni-spark-uygulamasi.md). Yeni bir kaynak bağlamak:
[yeni-kaynak-ve-pipeline.md](yeni-kaynak-ve-pipeline.md).
[kullanici-ve-yetki.md](kullanici-ve-yetki.md) — yetki yönetimi;
[yedek-ve-geri-donus.md](yedek-ve-geri-donus.md) — yedek ve geri dönüş;
[sorun-giderme.md](sorun-giderme.md) — belirti tablosu. Ortak GitOps döngüsü her zaman
aynıdır:
[değişiklik nasıl uygulanır](degisiklik-nasil-uygulanir.md).
