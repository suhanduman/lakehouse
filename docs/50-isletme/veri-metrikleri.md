# Veri metrikleri

**Bu bölümde:** tablo düzeyinde ölçümlerin (satır sayısı, dosya sayısı, tablo boyutu, son
yazma zamanı, anlık görüntü sayısı) nereden okunduğu; hazır SQL'ler; ürünle gelen Superset
panosunun ("Tablo sağlığı") verisini nereden aldığı ve nasıl okunacağı; sınırlar.
**Süre:** SQL'leri koşturmak dakikalar; panoyu okumak için ek iş yoktur.
**Gereken yetki:** Trino'da ilgili tablolarda `SELECT`; panoyu açmak için Superset'te
`lakehouse-*` gruplarından birine üyelik.
**Nerede çalıştırılır:** SQL'ler `[pod]` — Superset SQL Lab, Zeppelin ya da bir not defteri;
dosya düzenleme ve içe aktarma `[bastion]`.

> **Bu metrikler Prometheus'ta yoktur.** Alarm kurulamaz; ölçüm **sorgu anında** hesaplanır.
> Bileşen sağlığı ve alarmlar ayrı bir sayfadadır:
> [izleme-ve-alarmlar.md](izleme-ve-alarmlar.md).

---

## 1. Neden ayrı bir exporter yok

Iceberg'in ya da katalog sunucusunun tablo metrikleri için bir Prometheus exporter'ı
**yoktur**: bu bilgi tablonun kendi meta verisindedir ve SQL ile okunur. Özel imaj ya da
exporter yazmak bu üründe kapsam dışıdır, dolayısıyla teslim iki parçadır:

1. Trino'nun Iceberg **meta veri tabloları** — §3'teki SQL'ler;
2. bütün tabloların durumunu saatte bir tek bir tabloya (`ops.table_health`) yazan bakım
   adımı ve bu tabloyu okuyan **hazır Superset panosu** ("Tablo sağlığı",
   `glue/files/superset/tablo-sagligi/`). Pano kümede bir ConfigMap olarak gelir; içe aktarma
   adımı **kurulum sonrası** bölümündedir ([40-kurulum-sonrasi](../40-kurulum-sonrasi.md) §4).

Depolamanın kendi kapasite ve kova metrikleri platformun kendi exporter'ındadır, bu üründe
değildir.

---

## 2. Üç meta veri tablosu

Her Iceberg tablosunun yanında, adının sonuna bir sonek eklenerek okunan sanal tabloları
vardır. Bu üçü kullanılır:

| Sanal tablo | Ne verir |
|---|---|
| `$snapshots` | Her yazma işleminin zamanı, türü ve özet sayaçları (satır, dosya, boyut) |
| `$files` | Güncel dosya dağılımı: dosya sayısı, boyutu, biçimi — küçük dosya sorununu görmek için |
| `$history` | Hangi anlık görüntünün güncel zincirde olduğu (geri alma izleri) |

**Yazım kuralı:** tablo adı **tırnak içinde** ve sonek dolar işaretiyle yazılır —
`lakehouse.shop."orders$snapshots"`. Dolar işareti kabuk değişkeni gibi göründüğü için komut
satırından çalıştırırken **tek tırnaklı** bir belge (heredoc) kullanın; Superset SQL Lab ve
Zeppelin'de böyle bir sorun yoktur.

---

## 3. Hazır SQL'ler

`[pod]` — Superset SQL Lab, Zeppelin `%jdbc` ya da not defteri:

```sql
-- 1) Yazma geçmişi: son yazma, işlem türü, özet sayaçlar
SELECT committed_at, snapshot_id, operation,
       CAST(summary['total-records'] AS bigint)      AS kayit,
       CAST(summary['total-data-files'] AS bigint)   AS veri_dosyasi,
       CAST(summary['total-delete-files'] AS bigint) AS silme_dosyasi,
       CAST(summary['total-files-size'] AS bigint)/1048576.0 AS boyut_mib
FROM lakehouse.shop."orders$snapshots"
ORDER BY committed_at DESC LIMIT 20;
```

```sql
-- 2) Güncel dosya dağılımı: küçük dosya sorununu görmek için
SELECT file_format, count(*) AS dosya, sum(record_count) AS kayit,
       sum(file_size_in_bytes)/1048576.0 AS boyut_mib,
       avg(file_size_in_bytes)/1048576.0 AS ort_dosya_mib
FROM lakehouse.shop."orders$files"
GROUP BY file_format;
```

```sql
-- 3) Geçmiş: hangi anlık görüntü güncel zincirde
SELECT made_current_at, snapshot_id, parent_id, is_current_ancestor
FROM lakehouse.shop."orders$history"
ORDER BY made_current_at DESC LIMIT 20;
```

**Beklenen sonuç:** birinci sorgu her yazma için bir satır döndürür ve `kayit` sütunu
tablonun o andaki toplam satır sayısıdır; ikinci sorgu tek bir `PARQUET` satırı döndürür
(dosya sayısı ve ortalama dosya boyutuyla); üçüncü sorguda en üstteki satırın
`is_current_ancestor` değeri `true` olur.

Okuma notları:

- `$files` tablosundaki `content` sütunu dosya türünü ayırır: `0` veri, `0` dışındakiler
  silme kayıtlarıdır. Silme kayıtlarının birikmesi okuma maliyetini artırır.
- Bakım işleri (`maint-compact`, `maint-position-deletes`, `maint-expire-orphan-ttl`) bu
  sayıları düşürür; etkilerini aynı SQL'le **önce/sonra** ölçebilirsiniz.
- `$files` bütün güncel dosya listesini tarar: çok büyük tablolarda maliyetlidir. Pano bu
  yüzden yalnız `$snapshots` özetlerini kullanır (§4).

**Ters giderse:** `TABLE_NOT_FOUND` alıyorsanız ya tablo adı yanlıştır ya da tırnak
kullanılmamıştır. `Access Denied` alıyorsanız kullanıcının o tabloda `SELECT` yetkisi yoktur
([kullanici-ve-yetki.md](kullanici-ve-yetki.md) §5).

---

## 4. "Tablo sağlığı" panosu ve `ops.table_health`

Bakım işi (`maint-position-deletes`, saatte bir) işini bitirince katalogdaki **bütün**
tabloları dolaşır ve her tablo için `ops.table_health` tablosuna bir satır ekler. Pano
yalnız bu tabloyu okur; bu yüzden hangi kaynaklar ve tablolar olursa olsun kurulumdan sonra
kendiliğinden çalışır. Yeni bir kaynak ya da tablo eklediğinizde bir sonraki saatlik
ölçümde panoda görünür; panoda ya da depoda değiştirilecek bir şey yoktur.

| Kolon | Anlamı |
|---|---|
| `olcum_zamani` | ölçümün alındığı an (UTC) |
| `tablo` | `ad_alanı.tablo` |
| `katman` | `bronze` (`_raw` ad alanları), `silver` (pipeline'ların Silver ad alanları), `sandbox`, `diger` |
| `son_degisiklik` | tabloya en son yazılan an |
| `kayit` | satır sayısı |
| `veri_dosyasi` | veri dosyası sayısı |
| `delete_dosyasi` | silinen satırları işaretleyen dosya sayısı (güncellenen tablolarda olur) |
| `boyut_mib` | tablonun dosyalarının toplam boyutu (MiB) |
| `ort_dosya_mib` | ortalama dosya boyutu (MiB) — **küçük dosya göstergesi** |
| `snapshot_sayisi` | tablonun sakladığı sürüm sayısı |

Ölçümler 90 gün saklanır, daha eskileri bakım işinin kendisi siler. Tablodan doğrudan da
sorgulanabilir:

`[pod]` (Superset SQL Lab ya da not defteri)

```sql
SELECT tablo, katman, kayit, veri_dosyasi, ort_dosya_mib, son_degisiklik
FROM lakehouse.ops.table_health
WHERE olcum_zamani = (SELECT max(olcum_zamani) FROM lakehouse.ops.table_health)
ORDER BY katman, tablo;
```

**Beklenen çıktı** (OpenShift provasından seçilmiş gerçek satırlar):

```text
tablo              katman   kayit   veri_dosyasi  ort_dosya_mib  son_degisiklik
erp_raw.orders     bronze   201     2             0.004          2026-11-10 03:31:05
ogr_raw.students   bronze   300000  1             3.705          2026-11-10 12:00:49
erp.orders         silver   201     17            0.002          2026-11-10 03:45:39
ogr.students       silver   300000  12            0.229          2026-11-10 12:00:40
```

Panoda dört grafik vardır:

| Grafik | Neye bakılır |
|---|---|
| Tablo sağlığı — son durum | her tablonun son ölçümü (yukarıdaki sorgunun aynısı) |
| Ortalama dosya boyutu (MiB) | küçük değer = çok sayıda küçük dosya; büyük ve sık okunan bir tabloda değer sürekli küçükse sıkıştırma işinin (`maint-compact`) koştuğunu doğrulayın |
| Kayıt sayısı — zaman içinde | tabloların büyüme eğrisi; düz kalan bir eğri akışın durduğunu gösterebilir |
| Veri dosyası sayısı — zaman içinde | sıkıştırmadan sonra düşmeli; sürekli artıyorsa bakım işlerine bakın |

**Okuma ipuçları:**

- `son_degisiklik` beklenenden eskiyse o tabloya veri gelmiyordur — Bronze için bağlayıcıya,
  Silver için `silver-merge` işine bakın ([sorun-giderme.md](sorun-giderme.md)).
- `delete_dosyasi` yüksek kalan bir Silver tablo okumada yavaşlar; saatlik
  `maint-position-deletes` işi bunu düzenli olarak katlar.
- Pano **boşsa**: kurulumda henüz hiçbir pipeline yoktur (bakım işleri ilk pipeline ile
  açılır) ya da ilk saatlik ölçüm henüz alınmamıştır.

---

## 5. Sınırlar

- **Ölçüm saatliktir.** Pano anlık değil, son saatlik ölçümü gösterir; anlık değer için §3'teki
  SQL'ler kullanılır.
- **Prometheus serisi üretilmez, alarm kurulamaz.** Tablo tazeliği alarmı Spark tarafından
  gelir (`LakehouseSilverMergeStale`, [izleme-ve-alarmlar.md](izleme-ve-alarmlar.md) §3).
- **Gösterilen boyut mantıksaldır.** Tablonun dosya boyutlarının toplamıdır; depolamanın
  fiziksel ya da çoğaltılmış tüketimi değildir. O rakam depolama sisteminin kendi
  izlemesindedir.
- **Pano kimlik taşımaz.** Superset Trino'ya paylaşımlı servis hesabıyla bağlanır; satır
  filtresi ve kolon maskesi burada **uygulanmaz**
  ([kullanici-ve-yetki.md](kullanici-ve-yetki.md) §7). Ölçümler veri içermediği için bu bir
  sorun değildir, ama panoya veri sorgusu eklerken akılda tutulmalıdır.
- **Panoyu arayüzden değiştirmek kalıcı değildir.** Bir sonraki içe aktarma ürünün hâlini
  geri yazar; kendi panolarınızı ayrı adla kaydedin.

---

## Kontrol listesi

- [ ] Pano içe aktarıldı ve açılıyor ([40-kurulum-sonrasi](../40-kurulum-sonrasi.md) §4.3).
- [ ] Panoda kurumun tabloları görünüyor (ilk pipeline'dan en geç bir saat sonra).
- [ ] Bakım işlerinin etkisi (dosya sayısında düşüş) en az bir kez grafikte görüldü.
- [ ] Bu metriklerden **alarm üretilemeyeceği** ekipçe biliniyor.

## Sonraki bölüm

Bileşen sağlığı ve altı alarm: [izleme-ve-alarmlar.md](izleme-ve-alarmlar.md). Bu ölçümlerin
haftalık kontrol listesindeki yeri:
[gunluk-haftalik-kontroller.md](gunluk-haftalik-kontroller.md) §4.
