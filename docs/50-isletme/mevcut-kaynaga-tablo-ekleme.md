# Mevcut kaynağa tablo ekleme

Bu sayfa, lakehouse'a zaten bağlı olan bir kaynak veritabanından yeni bir tabloyu da
almayı anlatır. Kaynakta tabloya izin verilir, tablo ayar dosyasına eklenir, tablodaki
var olan satırlar **artımlı snapshot** ile getirilir ve sonuç Bronze ile Silver'da
kontrol edilir.

**Ne kadar sürer:** 15 dakika, artı var olan satırların okunma süresi. Küçük tablolarda
bu birkaç saniye, on milyonlarca satırlık tablolarda saatler sürer; bu sırada akış durmaz.

**Hangi yetkiler gerekir:** kaynak veritabanında izin verme ya da CDC açma; kurumun
lakehouse Git deposunda `main` dalına yazma; `$LAKEHOUSE_NS` ad alanında okuma.

**Komutlar nerede çalışır:** `[bastion]` küme ve Git komutları için; `[kaynak DB]`
kaynak veritabanına bağlanan istemci; `[pod]` JupyterHub not defteri hücresi.

**Örnek adlar:** bu sayfada kaynağın adı `shop`, eklenen tablo `public.customers`'tır.
Komutlarda kendi kaynağınızın ve tablonuzun adını yazın.

**Bu sayfada yapılmayanlar:** kaynağın CDC hazırlığı, kaynağın Secret'ı ve katalog ad
alanları zaten vardır. Yeni tablo var olan Bronze ad alanına (`shop_raw`) yazılır, Silver
tablosu var olan `shop` ad alanında yaratılır. Kaynak ilk kez bağlanıyorsa
[yeni-kaynak-ve-pipeline.md](yeni-kaynak-ve-pipeline.md) izlenir.

Yolun tamamı:

```text
kaynakta izin -> ayar dosyası (tables + pipelines) -> commit + push -> ArgoCD uygular
              -> artımlı snapshot sinyali -> Bronze -> silver-merge -> Silver
```

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

**Ters giderse:** eşittir işaretlerinden sonrası boşsa `install/lakehouse.env` dosyası
yoktur ya da başka bir klasördesiniz ([30-kurulum](../30-kurulum.md) §1).

---

## 2. Kaynakta tabloyu yakalamaya hazırlayın

Bu adımı kaynak veritabanının yöneticisi yapar. Tablonun sahibi değişmez.

**PostgreSQL:** iki komut gerekir. Birincisi CDC kullanıcısına tabloyu okuma izni verir,
ikincisi tabloyu publication'a (CDC'nin izlediği tablo listesine) ekler.

`[kaynak DB]` — PostgreSQL

```sql
GRANT SELECT ON public.customers TO dbz;
ALTER PUBLICATION dbz_shop_pub ADD TABLE public.customers;
```

Buradaki iki adı kendi kaynağınıza göre değiştirin:

- `dbz`: kaynağın CDC kullanıcısı. Kaynağın Secret'ında yazılıdır:

  `[bastion]`

  ```bash
  oc -n "$LAKEHOUSE_NS" get secret shop-db -o jsonpath='{.data.username}' | base64 -d; echo
  ```

- `dbz_shop_pub`: kaynağın publication'ı. Adı her zaman `dbz_` + kaynak adı + `_pub`
  biçimindedir.

**Beklenen çıktı** (PostgreSQL):

```text
GRANT
ALTER PUBLICATION
```

**Ters giderse:**

- `must be owner of publication`: publication'ı yaratan yönetici hesabıyla bağlanıp
  tekrarlayın.
- `relation "customers" is already member of publication "dbz_shop_pub"`: tablo zaten
  eklidir, bir şey yapmanıza gerek yoktur.
- Publication'daki tabloları görmek için:
  `SELECT tablename FROM pg_publication_tables WHERE pubname = 'dbz_shop_pub';`

**SQL Server:** tabloda CDC'yi açın.

`[kaynak DB]` — SQL Server

```sql
EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'customers',
                             @role_name = NULL;
```

CDC kullanıcısının tabloyu okuma izni, şemanın tamamına verildiği için zaten vardır.

**MongoDB:** bu adım yoktur. Okuma izni veritabanı düzeyinde zaten verilmiştir.

---

## 3. Ayar dosyasına tabloyu ekleyin

`platform/values/site/glue.yaml` dosyasında iki yer değişir:

1. İlgili kaynağın `tables` listesine tablonun adı eklenir.
2. Tablonun Silver'da da olması isteniyorsa `pipelines` listesine bir satır eklenir.
   Silver, her kaydın **güncel** hâlini tutar; bu yüzden birincil anahtarı olan ve
   satırları güncellenen tablolar içindir. Yalnız yeni satır eklenen olay ya da günlük
   tablolarına `pipelines` satırı yazılmaz; bunlar Bronze'da kalır ve oradan sorgulanır.

Dosyayı açın:

`[bastion]`

```bash
git pull --rebase origin main
vi platform/values/site/glue.yaml
```

Örneğin kaynak ve pipeline önce şöyleyse:

```yaml
sources:
- name: shop
  type: postgres
  host: shop-db.kurum.example.net
  port: 5432
  database: shop
  tables: [public.orders]
  signalTable: public.debezium_signal
pipelines:
- {bronze: shop_raw.orders, keys: [id], bucket_count: 4, casts: {updated_at: timestamp}}
```

sonra şöyle olur:

```yaml
sources:
- name: shop
  type: postgres
  host: shop-db.kurum.example.net
  port: 5432
  database: shop
  tables: [public.orders, public.customers]     # eklenen: public.customers
  signalTable: public.debezium_signal
pipelines:
- {bronze: shop_raw.orders, keys: [id], bucket_count: 4, casts: {updated_at: timestamp}}
- {bronze: shop_raw.customers, keys: [id], bucket_count: 4, casts: {updated_at: timestamp}}
```

**`casts` satırını başka bir tablodan kopyalamayın.** `casts` yalnız **o tabloda
gerçekten bulunan** kolonları sayabilir. Örnekteki `customers` tablosunda da bir
`updated_at` kolonu olduğu için iki satır aynı görünüyor. Tabloda olmayan bir kolon
yazılırsa `silver-merge` işi `casts bilinmeyen kolon(lar)` hatası verir. Zaman kolonu
olmayan bir tabloda `casts` alanı hiç yazılmaz.

**MongoDB kaynaklarında** `tables` yerine `collections` listesine `db.koleksiyon`
yazılır. Pipeline satırında `keys` her zaman `[_id]`'dir ve `casts` kullanılmaz:

```yaml
- {bronze: shop_raw.customers, keys: [_id], bucket_count: 4}
```

`pipelines` alanlarının anlamı ve `keys`, `bucket_count`, `casts` değerlerinin nasıl
seçileceği: [yeni-kaynak-ve-pipeline.md](yeni-kaynak-ve-pipeline.md) §7.

---

## 4. Değişikliği Git'e gönderin

Dosyayı kaydedip kapattıktan sonra denetleyin ve gönderin. Yolun ayrıntıları:
[değişiklik nasıl uygulanır](degisiklik-nasil-uygulanir.md).

`[bastion]`

```bash
bash scripts/check-site.sh
git diff
git add platform/values/site/glue.yaml
git commit -m "site: shop kaynağına public.customers eklendi"
git push origin main
```

`check-site.sh` `check-site: OK` yazmalı, `git diff` yalnız eklediğiniz satırları
göstermelidir.

ArgoCD değişikliği birkaç dakika içinde uygular. Durumu görmek için:

`[bastion]`

```bash
oc -n "$ARGOCD_NS" get application glue \
  -o jsonpath='{.status.sync.status} {.status.health.status}{"\n"}'
```

**Beklenen çıktı** (uygulama bittiğinde):

```text
Synced Healthy
```

Uygulama sırasında kaynağın bağlayıcısı yeniden başlar, çünkü ayarı değişmiştir. Akış
birkaç saniye durur ve kaldığı yerden devam eder.

**Ters giderse:** `Synced Degraded` görürseniz bağlayıcı yeni ayarla açılamamıştır;
§8'deki tabloya bakın.

---

## 5. Bağlayıcının yeni tabloyu aldığını doğrulayın

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" get kafkaconnector dbz-shop \
  -o jsonpath='{.spec.config.table\.include\.list}{"\n"}'
```

**Beklenen çıktı** (örnek; listenin sonundaki sinyal tablosunu ürün kendisi ekler):

```text
public.orders,public.customers,public.debezium_signal
```

PostgreSQL'de tablonun publication'da olduğunu kaynakta da görün:

`[kaynak DB]`

```sql
SELECT tablename FROM pg_publication_tables WHERE pubname = 'dbz_shop_pub' ORDER BY 1;
```

**Beklenen çıktı:**

```text
    tablename    
-----------------
 customers
 debezium_signal
 orders
(3 rows)
```

**Ters giderse:**

- Bağlayıcının listesinde yeni tablo yoksa ArgoCD değişikliği henüz uygulamamıştır;
  birkaç dakika sonra tekrar bakın ya da
  [değişiklik nasıl uygulanır](degisiklik-nasil-uygulanir.md) §5.1 ile hemen
  uygulatın.
- Tablo publication'da yoksa §2'deki `ALTER PUBLICATION` komutu çalıştırılmamıştır.
  Bu durumda bağlayıcı hata vermez ama tablonun değişiklikleri **hiç gelmez**.

---

## 6. Var olan satırları artımlı snapshot ile getirin

Bağlayıcı artık tablodaki **yeni** değişiklikleri yakalar. Tablodaki **var olan**
satırlar ise henüz gelmemiştir. Bunları getirmek için kaynağın sinyal tablosuna bir satır
yazılır; bağlayıcı tabloyu parça parça okur ve bu sırada akışı durdurmaz.

`[kaynak DB]` — PostgreSQL

```sql
INSERT INTO public.debezium_signal (id, type, data)
VALUES (gen_random_uuid()::text, 'execute-snapshot',
        '{"data-collections": ["public.customers"], "type": "incremental"}');
```

`[kaynak DB]` — SQL Server (tablo adının başında **veritabanının adı** vardır; burada
`erp`)

```sql
INSERT INTO dbo.debezium_signal (id, type, data)
VALUES (CONVERT(varchar(42), NEWID()), 'execute-snapshot',
        '{"data-collections": ["erp.dbo.customers"], "type": "incremental"}');
```

`[kaynak DB]` — MongoDB (kaynağın ayarında `signalCollection` tanımlı olmalıdır)

```js
db.getSiblingDB("shop").debezium_signal.insertOne({
  type: "execute-snapshot",
  data: {"data-collections": ["shop.customers"], "type": "incremental"}
})
```

İlerlemeyi Connect günlüklerinden izleyin. Komut bütün Connect pod'larının günlüğünü
okur; bağlayıcı hangi pod'da çalışıyorsa satırlar oradan gelir.

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" logs -l strimzi.io/kind=KafkaConnect --tail=500 \
  | grep -iE "incremental snapshot|'INCREMENTAL' snapshot"
```

**Beklenen çıktı** (örnek; iki satırlık bir tablo için, bu yüzden okuma ilk parçada
bitmiştir):

```text
2026-09-24 09:17:52 INFO  [debezium-postgresconnector-shop-change-event-source-coordinator] ExecuteSnapshot:64 - Requested 'INCREMENTAL' snapshot of data collections '[public.customers]' with additional conditions '[]' and surrogate key 'PK of table will be used'
2026-09-24 09:17:52 INFO  [debezium-postgresconnector-shop-change-event-source-coordinator] AbstractIncrementalSnapshotChangeEventSource:325 - No data returned by the query, incremental snapshotting of table 'public.customers' finished
```

Aramada `No incremental snapshot in progress, no action needed on start` satırları da
çıkabilir. Bunlar bağlayıcıların açılışta yazdığı bilgi satırlarıdır, sorun değildir.

**Ters giderse:**

- Hiç `Requested 'INCREMENTAL' snapshot` satırı yoksa sinyal satırı yazılmamıştır ya da
  sinyal tablosu kaynağın `signalTable` ayarındakinden farklıdır.
- `Requested` satırı var ama satırlar gelmiyorsa tablonun birincil anahtarı yoktur.
  Sinyalin `data` alanına anahtar olarak kullanılacak kolonu `"surrogate-key"` ile
  verin.

---

## 7. Bronze ve Silver'ı doğrulayın

Bronze tablosu, Iceberg yazıcısının ilk yazmasından sonra kendiliğinden yaratılır.
Yazıcı beş dakikada bir yazar; **10 dakika** bekleyin, sonra sayın:

`[pod]` (JupyterHub not defteri hücresi)

```python
import os, trino
conn = trino.dbapi.connect(host=os.environ["TRINO_HOST"], port=8443, http_scheme="https",
                           verify="/etc/lakehouse-ca/tls.crt",
                           auth=trino.auth.OAuth2Authentication(), catalog="lakehouse")
cur = conn.cursor(); cur.execute("select count(*) from shop_raw.customers"); cur.fetchall()
```

**Beklenen çıktı** (örnek; iki satırlık bir tablo):

```text
[[2]]
```

Aynı sayımı bastion'dan da yapabilirsiniz:
[90-referans/oc-hizli-basvuru.md §16](../90-referans/oc-hizli-basvuru.md#trino-sql).

**Ters giderse:** 10 dakika sonra hâlâ `[[0]]` geliyorsa tablo yaratılmış ama içine
henüz hiç yazılmamıştır. Bu beklemeyle düzelmez:
[sorun-giderme.md §4.2](sorun-giderme.md#sink) "Bronze tablosu var ama boş".

Silver tablosunu, bir sonraki `silver-merge` çalışması (15 dakikada bir) yaratır.
Çalışmanın yeni tabloyu işlediğini günlükten görün:

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" logs -l spark-role=driver --tail=2000 | grep -E "MERGE_OK|->"
```

**Beklenen çıktı** (örnek; ilk çalışmada Silver tablosu henüz olmadığı için tablonun
tamamı okunur ve Silver tablosu yaratılır):

```text
[shop_raw.customers] -> lakehouse.shop.customers: FALLBACK full, 2 anahtar, snapshot None -> 2806037388819634651
MERGE_OK
```

Sonra Silver'ı sayın:

`[pod]` (JupyterHub not defteri hücresi)

```python
cur = conn.cursor(); cur.execute("select count(*) from shop.customers"); cur.fetchall()
```

**Beklenen çıktı** (örnek; Silver'daki sayı kaynaktaki satır sayısına eşittir):

```text
[[2]]
```

**Ters giderse:**

- Bronze dolu ama Silver tablosu yoksa `pipelines` satırı eksiktir ya da `silver-merge`
  işi askıdadır. `oc -n "$LAKEHOUSE_NS" get scheduledsparkapplication silver-merge`
  çıktısındaki `SUSPEND` sütunu `false` olmalıdır.
- Silver'daki sayı kaynaktakinden azsa okuma hâlâ sürüyordur; §6'daki günlüğe bakın.

---

## 8. Sık karşılaşılan durumlar

| Ne görüyorsunuz | Neden | Ne yapmalı |
|---|---|---|
| Bağlayıcının tablo listesi eski hâlinde | ArgoCD değişikliği henüz uygulamadı | birkaç dakika bekleyin ya da hemen uygulatın ([değişiklik nasıl uygulanır](degisiklik-nasil-uygulanir.md) §5.1) |
| Yeni tablonun değişiklikleri Bronze'a hiç gelmiyor, hata da yok | tablo publication'a eklenmedi | §2, `ALTER PUBLICATION … ADD TABLE` |
| Bağlayıcı `permission denied for table` hatası veriyor | CDC kullanıcısına okuma izni verilmedi | §2, `GRANT SELECT` |
| Bronze tablosu yaratılmıyor | yazıcı henüz ilk yazmasını yapmadı | 10 dakika bekleyin |
| Bronze'da yalnız yeni değişiklikler var, eski satırlar yok | artımlı snapshot sinyali yazılmadı | §6 |
| Snapshot satırları gelmiyor, tabloda birincil anahtar yok | bağlayıcı anahtar kolonu bulamıyor | sinyale `surrogate-key` ekleyin (§6) |
| Silver tablosu yaratılmıyor | `pipelines` satırı yok ya da iş askıda | §3; `SUSPEND` sütunu (§7) |
| Silver'da `SchemaConflict` hatası | kaynakta bir kolonun tipi uyumsuz biçimde değişti | [sorun-giderme.md §4.4](sorun-giderme.md#spark) |

Bütün belirtiler ve çözümleri: [sorun-giderme.md](sorun-giderme.md) §3.

---

## Kontrol listesi

- [ ] Kaynakta CDC kullanıcısının tabloyu okuma izni var (PostgreSQL'de tablo
      publication'a eklendi; SQL Server'da tabloda CDC açıldı).
- [ ] Tablo `tables` listesine eklendi; Silver'da olması gerekiyorsa `pipelines` satırı
      da yazıldı.
- [ ] Değişiklik Git'e gönderildi; `glue` uygulaması `Synced Healthy`.
- [ ] Bağlayıcının tablo listesi yeni tabloyu içeriyor.
- [ ] Artımlı snapshot sinyali yazıldı ve günlükte `Requested 'INCREMENTAL' snapshot`
      satırı görüldü.
- [ ] Bronze tablosunda satırlar var.
- [ ] Bir `silver-merge` çalışmasından sonra Silver tablosu var ve satır sayısı kaynakla
      aynı.

## Sonraki bölüm

- [kaynak-veya-tablo-silme.md](kaynak-veya-tablo-silme.md) — kaynak ya da tablo kaldırmak
- [yeni-spark-uygulamasi.md](yeni-spark-uygulamasi.md) — kendi Spark işiniz
- [kullanici-ve-yetki.md](kullanici-ve-yetki.md) — yetki yönetimi
- [sorun-giderme.md](sorun-giderme.md) — sorun giderme

Değişikliğin kümeye ulaştığı yol her zaman aynıdır:
[değişiklik nasıl uygulanır](degisiklik-nasil-uygulanir.md).
