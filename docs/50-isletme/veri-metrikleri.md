# Veri metrikleri

Bu sayfa tablolar hakkındaki ölçümleri anlatır: satır sayısı, dosya sayısı, tablo boyutu,
son yazma zamanı ve tablonun sakladığı sürüm sayısı. Bu ölçümlerin nereden okunduğu, hazır
SQL sorguları, ürünle gelen "Tablo sağlığı" panosu ve bir tablonun geçmişine nasıl
bakılacağı buradadır.

**Ne kadar sürer:** sorguları çalıştırmak birkaç dakika; panoyu okumak için ek bir iş
yoktur.

**Hangi yetkiler gerekir:** Trino'da ilgili tabloları okuma yetkisi (`lakehouse-users`,
`lakehouse-analysts` ve `lakehouse-admins` gruplarının hepsinde vardır); panoyu açmak için
Superset'e bu gruplardan biriyle giriş.

**Komutlar nerede çalışır:** `[SQL Lab]` — Superset → **SQL** → **SQL Lab** ekranı
(veritabanı: `lakehouse`). Aynı sorgular Zeppelin'de ya da bir JupyterHub not defterinde
de çalışır.

> **Bu ölçümlerden alarm kurulamaz.** Ölçümler sorgu anında tablonun kendi bilgisinden
> hesaplanır; izleme sistemine (Prometheus) gitmez. Bileşen sağlığı ve alarmlar ayrı bir
> sayfadadır: [izleme-ve-alarmlar.md](izleme-ve-alarmlar.md).

---

## 1. Ölçümler nereden gelir

Her Iceberg tablosu kendi geçmişini ve dosya listesini kendi içinde tutar. Bu bilgi iki
yoldan okunur:

1. **Tablonun bilgi tabloları:** her tablonun yanında, adının sonuna bir ek getirilerek
   okunan hazır tablolar vardır (§2, §3). Anlık değer verirler.
2. **"Tablo sağlığı" panosu:** ürünün bakım işi saatte bir bütün tabloları ölçer ve
   sonucu `ops.table_health` tablosuna yazar. Superset'teki hazır pano bu tabloyu okur
   (§4). Panonun Superset'e alınması kurulum sonrasında yapılır
   ([40-kurulum-sonrasi](../40-kurulum-sonrasi.md) §4).

Depolamanın kendi kapasite ölçümleri (disk doluluğu, bucket boyutu) bu ürünün değil,
depolama sisteminin kendi izlemesindedir.

---

## 2. Üç bilgi tablosu

| Ek | Ne verir |
|---|---|
| `$snapshots` | her yazmanın zamanı, türü ve o andaki toplam satır, dosya ve boyut |
| `$files` | tablonun şu anki dosyaları: sayısı, boyutu, biçimi |
| `$history` | hangi sürümün tablonun şu anki hâline giden zincirde olduğu |

**Yazım kuralı:** tablo adı ekle birlikte **çift tırnak içinde** yazılır:
`lakehouse.shop."orders$snapshots"`. SQL Lab, Zeppelin ve not defterlerinde bu yazım
olduğu gibi çalışır.

---

## 3. Hazır sorgular

Aşağıdaki sorgularda `erp.orders` yerine kendi tablonuzu yazın.

### 3.1 Yazma geçmişi

Her yazma için bir satır: ne zaman, hangi tür işlem, tablonun o andaki satır, dosya ve
boyut değerleri.

`[SQL Lab]`

```sql
SELECT committed_at, snapshot_id, operation,
       CAST(summary['total-records'] AS bigint)      AS kayit,
       CAST(summary['total-data-files'] AS bigint)   AS veri_dosyasi,
       CAST(summary['total-delete-files'] AS bigint) AS silme_dosyasi,
       CAST(summary['total-files-size'] AS bigint)/1048576.0 AS boyut_mib
FROM lakehouse.erp."orders$snapshots"
ORDER BY committed_at DESC LIMIT 20;
```

**Beklenen çıktı** (örnek bir kurulumdan, ilk üç satır; saatler SQL Lab'in saat dilimine
göre gösterilir):

```text
committed_at             snapshot_id          operation  kayit  veri_dosyasi  silme_dosyasi  boyut_mib
2026-11-10 06:45:39.420  1591194843561688809  overwrite  201    17            0              0.028
2026-11-09 15:15:35.410  5844366843191718990  overwrite  201    17            0              0.028
2026-11-09 15:00:44.300  4783927385307107571  replace    201    17            0              0.028
```

`replace` satırları bakım işinin dosyaları birleştirdiği yazmalardır; veri değişmez.

### 3.2 Dosya dağılımı

Çok sayıda küçük dosya okumayı yavaşlatır. Bu sorgu tablonun şu anki dosya sayısını ve
ortalama dosya boyutunu verir:

`[SQL Lab]`

```sql
SELECT file_format, count(*) AS dosya, sum(record_count) AS kayit,
       sum(file_size_in_bytes)/1048576.0 AS boyut_mib,
       avg(file_size_in_bytes)/1048576.0 AS ort_dosya_mib
FROM lakehouse.erp."orders$files"
GROUP BY file_format;
```

**Beklenen çıktı** (örnek bir kurulumdan):

```text
file_format  dosya  kayit  boyut_mib  ort_dosya_mib
PARQUET      17     201    0.028      0.002
```

### 3.3 Sürüm zinciri

`[SQL Lab]`

```sql
SELECT made_current_at, snapshot_id, parent_id, is_current_ancestor
FROM lakehouse.erp."orders$history"
ORDER BY made_current_at DESC LIMIT 20;
```

**Beklenen çıktı** (örnek bir kurulumdan, ilk üç satır):

```text
made_current_at          snapshot_id          parent_id            is_current_ancestor
2026-11-10 06:45:39.420  1591194843561688809  5844366843191718990  true
2026-11-09 15:15:35.410  5844366843191718990  4783927385307107571  true
2026-11-09 15:00:44.300  4783927385307107571  7819370712077934399  true
```

`is_current_ancestor` değeri `false` olan bir satır, geri alınmış bir sürümü gösterir.

**Okuma notları:**

- Ürünün bakım işleri (`maint-compact`, `maint-position-deletes`) dosya sayısını ve silme
  dosyası sayısını düşürür. Etkisini aynı sorguları önce ve sonra çalıştırarak
  görebilirsiniz.
- `$files` tablonun bütün dosya listesini okur; çok büyük tablolarda yavaştır. Pano bu
  yüzden yalnız `$snapshots` bilgisini kullanır.

**Ters giderse:**

- `TABLE_NOT_FOUND`: tablo adı yanlıştır ya da çift tırnak unutulmuştur.
- `Access Denied`: kullanıcının o tabloyu okuma yetkisi yoktur
  ([kullanici-ve-yetki.md](kullanici-ve-yetki.md) §5).

---

## 4. "Tablo sağlığı" panosu ve `ops.table_health`

Ürünün saatlik bakım işi (`maint-position-deletes`) işini bitirince katalogdaki **bütün**
tabloları ölçer ve her tablo için `ops.table_health` tablosuna bir satır ekler. Pano
yalnız bu tabloyu okur. Bu yüzden hangi kaynaklar ve tablolar olursa olsun kendiliğinden
çalışır: yeni bir tablo, bir sonraki saatlik ölçümde panoda görünür. Panoda ya da Git'te
değiştirilecek bir şey yoktur.

| Kolon | Anlamı |
|---|---|
| `olcum_zamani` | ölçümün alındığı an (UTC) |
| `tablo` | `ad_alanı.tablo` |
| `katman` | `bronze` (`_raw` ile biten ad alanları), `silver` (pipeline'ların Silver ad alanları), `sandbox`, `diger` |
| `son_degisiklik` | tabloya en son yazılan an |
| `kayit` | satır sayısı |
| `veri_dosyasi` | veri dosyası sayısı |
| `delete_dosyasi` | silinen ya da güncellenen satırları işaretleyen dosya sayısı |
| `boyut_mib` | tablonun dosyalarının toplam boyutu (MiB) |
| `ort_dosya_mib` | ortalama dosya boyutu (MiB) — küçük dosya göstergesi |
| `snapshot_sayisi` | tablonun sakladığı sürüm (snapshot) sayısı. Tabloya her yazmada bir sürüm oluşur; gece bakım işi Silver ve Gold'da 7, Bronze'da 1 günden eskileri siler. Veri sürekli geliyorsa sayı gün içinde artar, gece düşer. Günlerce durmadan artıyorsa bakım işi çalışmıyordur (§5, [sorun-giderme.md](sorun-giderme.md) §6.1) |

Ölçümler 90 gün saklanır; daha eskilerini bakım işi siler. Tabloyu doğrudan da
sorgulayabilirsiniz:

`[SQL Lab]`

```sql
SELECT tablo, katman, kayit, veri_dosyasi, ort_dosya_mib, son_degisiklik
FROM lakehouse.ops.table_health
WHERE olcum_zamani = (SELECT max(olcum_zamani) FROM lakehouse.ops.table_health)
ORDER BY katman, tablo;
```

**Beklenen çıktı** (örnek bir kurulumdan seçilmiş satırlar):

```text
tablo                katman  kayit   veri_dosyasi  ort_dosya_mib  son_degisiklik
erp_raw.orders       bronze  201     2             0.004          2026-11-10 03:31:05
ogr_raw.students     bronze  300000  1             3.705          2026-11-10 12:00:49
gold.kabul_senaryo2  diger   5       1             0.001          2026-11-11 16:50:50
erp.orders           silver  201     17            0.002          2026-11-10 03:45:39
ogr.students         silver  300000  12            0.229          2026-11-10 12:15:39
```

Panoda dört grafik vardır:

| Grafik | Neye bakılır |
|---|---|
| Tablo sağlığı — son durum | her tablonun son ölçümü (yukarıdaki sorgunun aynısı) |
| Ortalama dosya boyutu (MiB) | değer küçükse tabloda çok sayıda küçük dosya vardır. Büyük ve sık okunan bir tabloda değer sürekli küçük kalıyorsa sıkıştırma işinin (`maint-compact`) çalıştığını kontrol edin |
| Kayıt sayısı — zaman içinde | tabloların büyümesi; yatay kalan bir çizgi akışın durduğunu gösterebilir |
| Veri dosyası sayısı — zaman içinde | sıkıştırmadan sonra düşmelidir; sürekli artıyorsa bakım işlerine bakın |

**Okuma ipuçları:**

- `son_degisiklik` beklenenden eskiyse o tabloya veri gelmiyordur. Bronze için
  bağlayıcıya, Silver için `silver-merge` işine bakın
  ([sorun-giderme.md](sorun-giderme.md)).
- `delete_dosyasi` yüksek kalan bir Silver tablosu yavaş okunur; saatlik
  `maint-position-deletes` işi bu dosyaları düzenli olarak birleştirir.
- Pano **boşsa** henüz hiçbir pipeline tanımlanmamıştır (bakım işleri ilk pipeline ile
  açılır) ya da ilk saatlik ölçüm henüz alınmamıştır.

---

## 5. Bir tablonun geçmişi: zaman yolculuğu ve Bronze

"Dün bu tablo nasıldı?" ve "bu kayda ne oldu?" sorularının iki ayrı cevabı vardır.

| Soru | Nereye bakılır | Ne kadar geriye |
|---|---|---|
| Tablo geçmişteki bir anda nasıldı; rapor neden değişti? | **zaman yolculuğu** (Silver ve Gold tabloları) | 7 gün |
| Bu kayda ne zaman, hangi sırayla ne oldu? | **Bronze** tablosu | 30 gün |
| Daha eskisi | kurumun kendi arşiv (Gold) tablosu ya da yedek | — |

### 5.1 Zaman yolculuğu

Iceberg her yazmada tablonun yeni bir sürümünü (snapshot) tutar. Zaman yolculuğu,
tablonun geçmişteki bir sürümünü sorgulamaktır. Ürünün bakım işi Silver ve Gold
tablolarının sürümlerini **7 gün** saklar; daha eski bir ana gidilemez. Bronze
tablolarının sürümleri yalnız 1 gün saklanır: Bronze zaten her değişikliği satır olarak
tutar (§5.2), sürümlerine gerek yoktur.

Önce tablonun sürümlerini listeleyin:

`[SQL Lab]`

```sql
SELECT committed_at, snapshot_id, operation
FROM lakehouse.shop."orders$snapshots"
ORDER BY committed_at;
```

**Beklenen çıktı** (örnek bir kurulumdan, ilk iki satır):

```text
committed_at             snapshot_id          operation
2026-11-08 23:25:02.620  1662536647525854434  append
2026-11-08 23:36:08.581  2157470344130132194  overwrite
```

Bir sürümü numarasıyla sorgulayın (`FOR VERSION AS OF`):

`[SQL Lab]`

```sql
SELECT id, status, amount
FROM lakehouse.shop.orders FOR VERSION AS OF 2157470344130132194
ORDER BY id;
```

ya da bir anı vererek sorgulayın (`FOR TIMESTAMP AS OF`):

```sql
SELECT id, status, amount
FROM lakehouse.shop.orders FOR TIMESTAMP AS OF TIMESTAMP '2026-11-08 23:40:00 Europe/Istanbul'
ORDER BY id;
```

**Beklenen çıktı** (örnek bir kurulumdan; iki sorgu da aynı sürümü gösterir):

```text
id  status   amount
1   shipped  10.50
3   new      3.00
```

Tablonun şu anki hâliyle karşılaştırın:

```sql
SELECT id, status, amount FROM lakehouse.shop.orders ORDER BY id;
```

**Beklenen çıktı** (örnek bir kurulumdan; 4 numaralı sipariş o sürümden sonra eklenmiş):

```text
id  status   amount
1   shipped  10.50
3   new      3.00
4   new      1.00
```

**Ters giderse:** sorgu sürümün bulunamadığını söyleyen bir hatayla durursa, istediğiniz
an 7 günlük saklama süresinden eskidir ya da tablo o anda henüz yoktu. Var olan sürümleri
yukarıdaki `$snapshots` sorgusuyla görün.

### 5.2 Bronze: her değişikliğin kaydı

Silver tablosu, kaynağın **şu anki** hâlidir ve 15 dakikada bir güncellenir. Aynı kayıt bu
15 dakika içinde birkaç kez değişirse Silver'ın sürümlerinde yalnız son hâli görünür.

Bronze tablosu ise kaynaktaki **her** değişikliği ayrı bir satır olarak tutar. `_cdc.op`
kolonu değişikliğin türünü (`I` ekleme, `U` güncelleme, `D` silme), `_cdc.ts` kolonu
zamanını (UTC) verir. Bronze 30 gün saklanır.

`[SQL Lab]`

```sql
SELECT id, _cdc.op AS islem, _cdc.ts AS zaman, status, amount
FROM lakehouse.shop_raw.orders
ORDER BY id, _cdc.ts;
```

**Beklenen çıktı** (örnek bir kurulumdan; zamanlar UTC):

```text
id  islem  zaman                        status   amount
1   I      2026-11-08 20:07:05.040 UTC  new      10.50
1   U      2026-11-08 20:29:32.762 UTC  shipped  10.50
2   I      2026-11-08 20:07:05.041 UTC  paid     99.99
2   D      2026-11-08 20:29:32.764 UTC           0.00
3   I      2026-11-08 20:07:05.042 UTC  new      3.00
4   I      2026-11-08 20:42:10.469 UTC  new      1.00
```

1 numaralı siparişin önce eklendiğini, sonra `shipped` durumuna geçtiğini; 2 numaralının
eklenip silindiğini görüyorsunuz. Silme satırında yalnız `id` anlamlıdır; diğer kolonlar
boş ya da sıfırdır.

---

## 6. Sınırlar

- **Pano saatliktir.** Son saatlik ölçümü gösterir; anlık değer için §3'teki sorguları
  kullanın.
- **Alarm kurulamaz.** Tablo tazeliği alarmı Spark işlerinden gelir
  (`LakehouseSilverMergeStale`, [izleme-ve-alarmlar.md](izleme-ve-alarmlar.md) §3).
- **Gösterilen boyut tablonun dosyalarının toplamıdır.** Depolamanın kopyalarla birlikte
  harcadığı gerçek alan depolama sisteminin kendi izlemesindedir.
- **Pano kullanıcıya göre değişmez.** Superset Trino'ya ortak bir servis hesabıyla
  bağlanır; satır filtresi ve kolon maskesi burada **uygulanmaz**
  ([kullanici-ve-yetki.md](kullanici-ve-yetki.md) §7). Ölçümler veri içermediği için sorun
  değildir; panoya veri sorgusu eklerken bunu hatırlayın.
- **Panoyu arayüzden değiştirmek kalıcı değildir.** Panoyu yeniden içe aktarmak ürünün
  hâlini geri yazar; kendi panolarınızı ayrı bir adla kaydedin.

---

## Kontrol listesi

- [ ] Pano içe aktarıldı ve açılıyor ([40-kurulum-sonrasi](../40-kurulum-sonrasi.md) §4.3).
- [ ] Panoda kurumun tabloları görünüyor (ilk pipeline'dan en geç bir saat sonra).
- [ ] Bakım işlerinin etkisi (dosya sayısında düşüş) grafikte en az bir kez görüldü.
- [ ] Ekip, zaman yolculuğunun 7 gün, Bronze'un 30 gün geriye gittiğini biliyor.
- [ ] Ekip, bu ölçümlerden alarm kurulamayacağını biliyor.

## Sonraki bölüm

Bileşen sağlığı ve alarmlar: [izleme-ve-alarmlar.md](izleme-ve-alarmlar.md). Bu ölçümlerin
haftalık kontrollerdeki yeri:
[gunluk-haftalik-kontroller.md](gunluk-haftalik-kontroller.md) §4.
