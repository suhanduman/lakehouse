# Veri metrikleri

**Bu bölümde:** tablo düzeyinde ölçümlerin (satır sayısı, dosya sayısı, tablo boyutu, son
yazma zamanı, anlık görüntü sayısı) nereden okunduğu; hazır SQL'ler; ürünle gelen Superset
panosunun sanal veri seti SQL'i; panoya yeni tablo ekleme ve sınırlar.
**Süre:** SQL'leri koşturmak dakikalar; panoya tablo eklemek 15–20 dakika (düzenleme +
eşitleme + yeniden içe aktarma).
**Gereken yetki:** Trino'da ilgili tablolarda `SELECT`; panoyu değiştirmek için Git
deposunda `main` dalına yazma ve Superset'te `Admin` rolü.
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
2. bunları tek bir tabloda toplayan **hazır Superset panosu**
   (`glue/files/superset/iceberg-metadata/`), kümede bir ConfigMap olarak gelir ve Superset
   pod'una bağlanır. İçe aktarma adımı **kurulum sonrası** bölümündedir
   ([40-kurulum-sonrasi](../40-kurulum-sonrasi.md) §4); burada tekrarlanmaz.

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

## 4. Panonun sanal veri seti SQL'i

Pano tek bir sanal veri setine dayanır; dosyası
`glue/files/superset/iceberg-metadata/datasets/lakehouse/iceberg_table_health.yaml`
içindeki `sql:` alanıdır. Ürünle gelen hâli birebir şudur:

```sql
WITH t AS (
  SELECT 'shop.orders' AS tbl, * FROM lakehouse.shop."orders$snapshots"
  UNION ALL SELECT 'crm.customers', * FROM lakehouse.crm."customers$snapshots"
  UNION ALL SELECT 'nginx_raw.access_log', * FROM lakehouse.nginx_raw."access_log$snapshots"
)
SELECT tbl,
       max(committed_at)                                                        AS son_commit,
       count(*)                                                                 AS snapshot_sayisi,
       max_by(CAST(summary['total-records'] AS bigint), committed_at)           AS kayit,
       max_by(CAST(summary['total-data-files'] AS bigint), committed_at)        AS veri_dosyasi,
       max_by(CAST(summary['total-delete-files'] AS bigint), committed_at)      AS silme_dosyasi,
       max_by(CAST(summary['total-files-size'] AS bigint), committed_at)/1048576.0 AS boyut_mib
FROM t GROUP BY tbl ORDER BY tbl
```

Okuma anahtarı: `max_by(x, committed_at)` **en güncel yazmanın** sayacıdır; `count(*)` anlık
görüntü sayısıdır (eskileri temizleyen bakım işinden sonra düşer). `summary` bir metin
eşlemesi olduğu için her sayaç `CAST(... AS bigint)` ister.

> **Üretimde bu SQL olduğu gibi çalışmaz.** Üç tablo adı geliştirme demosuna sabittir
> (`shop.orders`, `crm.customers`, `nginx_raw.access_log`); üretim kurulumunda bu tablolar
> yoktur ve pano boş kalır. **İçe aktarmadan önce** SQL'i kendi tablolarınıza uyarlayın —
> uyarının tamamı ve içe aktarma adımı [40-kurulum-sonrasi](../40-kurulum-sonrasi.md)
> §4'tedir.

---

## 5. Panoya yeni tablo ekleme

Panonun tablo listesi SQL'in içinde sabittir: meta veri tabloları katalogdan kendiliğinden
keşfedilemez, her tablo ayrı bir `$snapshots` ilişkisidir.

1. `glue/files/superset/iceberg-metadata/datasets/lakehouse/iceberg_table_health.yaml`
   dosyasındaki `sql:` alanına bir satır ekleyin:

   ```sql
     UNION ALL SELECT 'erp.invoices', * FROM lakehouse.erp."invoices$snapshots"
   ```

   Sıra önemsizdir; `SELECT ..., *` biçimindeki kolon sırası bütün `$snapshots` tablolarında
   aynıdır.

2. Değişikliği itin:

   `[bastion]`

   ```bash
   git add glue/files/superset/iceberg-metadata/datasets/lakehouse/iceberg_table_health.yaml
   git commit -m "veri metrikleri: erp.invoices panoya eklendi"
   git push origin main
   ```

   **Beklenen çıktı** (örnek — kurumun Git sunucusunun adresi ve nesne sayıları farklıdır):

   ```text
   To ssh://git.kurum.example.net/lakehouse.git
      4ff2cc4..9a13b7c  main -> main
   ```

   Eşitleme ConfigMap'i günceller ve dosya pod'da kendiliğinden tazelenir (~1 dakika).

3. İçe aktarmayı **yeniden** koşun ([40-kurulum-sonrasi](../40-kurulum-sonrasi.md) §4.2). Ek
   bir bayrak gerekmez: içe aktarma nesneleri kimliklerine göre **her zaman** üzerine yazar.

**Ters giderse:** pano güncellenmediyse ya ConfigMap henüz yansımamıştır (bir dakika
bekleyin) ya da içe aktarma tekrarlanmamıştır. Panoyu arayüzden düzenlemek de mümkündür ama
**kalıcı değildir**: bir sonraki içe aktarma onu ezer, bu yüzden kalıcı değişiklik depoda
yapılır.

---

## 6. Sınırlar

- **Pano dosyaları depoda birer şablondur.** İçlerinde küme ve ad alanına göre doldurulan
  ifadeler vardır; pano canlı bir Superset'ten yeniden üretilirse bu ifadeler elle geri
  konmalıdır, aksi hâlde geliştirme kümesinin değerleri depoya sabitlenir.
- **Tablo adları kuruluma bağlıdır.** Farklı bir kurulumda SQL'deki demo tabloları yerine
  kendi tablolarınız yazılır (§5).
- **Prometheus serisi üretilmez, alarm kurulamaz.** Tablo tazeliği alarmı Spark tarafından
  gelir (`LakehouseSilverMergeStale`, [izleme-ve-alarmlar.md](izleme-ve-alarmlar.md) §3).
- **`$files` maliyetlidir** (bütün güncel dosyaları tarar); pano yalnız `$snapshots`
  özetlerini kullanır.
- **Gösterilen boyut mantıksaldır.** `total-files-size` tablonun dosya boyutlarının
  toplamıdır; depolamanın fiziksel ya da çoğaltılmış tüketimi değildir. O rakam platformun
  depolama exporter'ındadır.
- **Bu pano kimlik taşımaz.** Superset Trino'ya paylaşımlı servis hesabıyla bağlanır; satır
  filtresi ve kolon maskesi burada **uygulanmaz**
  ([kullanici-ve-yetki.md](kullanici-ve-yetki.md) §7). Meta veri sayaçları veri içermediği
  için bu pratikte sorun değildir, ama panoya veri sorgusu eklerken akılda tutulmalıdır.

---

## Kontrol listesi

- [ ] Sanal veri setinin SQL'i müşterinin gerçek tablolarına uyarlandı (demo tabloları
      kalmadı).
- [ ] Pano içe aktarıldı ve açılıyor ([40-kurulum-sonrasi](../40-kurulum-sonrasi.md) §4.3).
- [ ] Yeni bir tablo eklendiğinde SQL güncellendi, commit edildi ve içe aktarma tekrarlandı.
- [ ] Bakım işlerinin etkisi (dosya ve anlık görüntü sayısında düşüş) en az bir kez ölçüldü.
- [ ] Bu metriklerden **alarm üretilemeyeceği** ekipçe biliniyor.

## Sonraki bölüm

Bileşen sağlığı ve beş alarm: [izleme-ve-alarmlar.md](izleme-ve-alarmlar.md). Bu ölçümlerin
haftalık kontrol listesindeki yeri:
[gunluk-haftalik-kontroller.md](gunluk-haftalik-kontroller.md) §4.
