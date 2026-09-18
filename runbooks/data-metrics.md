# Tablo düzeyi veri metrikleri (Iceberg metadata + Superset dashboard)

## 1. Amaç ve neden exporter yok

Şartname izleme maddesi (G.5) bileşen sağlığını Prometheus ile karşılar (`runbooks/troubleshooting.md`,
`glue/templates/monitoring.yaml`). **Tablo düzeyinde** veri metrikleri (satır sayısı, dosya sayısı, tablo
boyutu, son commit zamanı, snapshot sayısı) için Iceberg'in veya Polaris'in bir Prometheus exporter'ı
**yoktur** — bu bilgi tablonun kendi metadata'sındadır ve SQL ile okunur (spec §8). Özel imaj/exporter
yazmak genel kısıtla yasak olduğundan teslim şu iki parçadır:

1. Trino'nun Iceberg **metadata tabloları** (`"tbl$snapshots"`, `"tbl$files"`, `"tbl$history"`) — aşağıdaki
   SQL'ler;
2. bunları tek tabloda toplayan **Superset dashboard asset bundle'ı**:
   `glue/files/superset/iceberg-metadata/` (glue chart'ında `superset-assets` ConfigMap'i olarak paketlenir ve
   Superset web pod'unda `/app/assets` altına mount edilir). Import adımı §3'tedir.

Depolama tarafı (FlashBlade kova/kapasite metrikleri) platformun kendi exporter'ındadır, bu repoda değildir.

## 2. Trino metadata SQL'leri

Katalog `lakehouse`; tablo adı tırnak içinde `"<tablo>$<metadata>"` biçiminde yazılır (dolar işareti kabuk
değişkeni gibi görünür → **tek tırnaklı** heredoc'ta çalıştırın). Zeppelin `%jdbc`, Superset SQL Lab veya
`trino` CLI ile:

```sql
-- snapshot geçmişi: son commit, işlem türü, özet sayaçlar
SELECT committed_at, snapshot_id, operation,
       CAST(summary['total-records'] AS bigint)     AS kayit,
       CAST(summary['total-data-files'] AS bigint)  AS veri_dosyasi,
       CAST(summary['total-delete-files'] AS bigint) AS delete_dosyasi,
       CAST(summary['total-files-size'] AS bigint)/1048576.0 AS boyut_mib
FROM lakehouse.shop."orders$snapshots" ORDER BY committed_at DESC LIMIT 20;

-- güncel dosya dağılımı: küçük dosya sorununu görmek için (compaction kararı)
SELECT file_format, count(*) AS dosya, sum(record_count) AS kayit,
       sum(file_size_in_bytes)/1048576.0 AS boyut_mib,
       avg(file_size_in_bytes)/1048576.0 AS ort_dosya_mib
FROM lakehouse.shop."orders$files" GROUP BY file_format;

-- history: hangi snapshot güncel ata zincirinde (rollback izleri)
SELECT made_current_at, snapshot_id, parent_id, is_current_ancestor
FROM lakehouse.shop."orders$history" ORDER BY made_current_at DESC LIMIT 20;
```

`$files`'ta `content` sütunu 0=veri, 1=position delete, 2=equality delete ayrımını verir (MoR borcu için
`WHERE content <> 0`). Bakım işleri (`maint-compact`, `maint-position-deletes`,
`maint-expire-orphan-ttl`) bu sayıları düşürür — etkilerini aynı SQL'le önce/sonra ölçebilirsiniz.

### Dashboard'un sanal veri seti SQL'i

`glue/files/superset/iceberg-metadata/datasets/lakehouse/iceberg_table_health.yaml` içindeki `sql:` alanı
birebir budur:

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
       max_by(CAST(summary['total-delete-files'] AS bigint), committed_at)      AS delete_dosyasi,
       max_by(CAST(summary['total-files-size'] AS bigint), committed_at)/1048576.0 AS boyut_mib
FROM t GROUP BY tbl ORDER BY tbl
```

`max_by(x, committed_at)` = **en güncel snapshot'ın** sayacı; `count(*)` snapshot sayısıdır (expire işi
sonrası düşer). `summary` MAP(varchar,varchar) olduğu için her sayaç `CAST(... AS bigint)` ister.

## 3. Dashboard'u import etme

Bundle chart ile gelir; Superset'e **bir kez** import edilir (import, Superset metastore'una yazar — GitOps
sync'i tekrarlamaz). ConfigMap anahtarları alt dizin taşıyamadığı için dosya adları `dizin__dosya` biçimindedir;
ilk adım bunları dizin ağacına geri açıp zip'ler:

`kubectl exec` heredoc'u **`-i` ister** (stdin aktarılmazsa script sessizce hiçbir şey yapmaz, zip oluşmaz):

```bash
NS=lakehouse
kubectl -n $NS exec -i deploy/superset-web-server -- python3 - <<'PY'
import pathlib, shutil
src = pathlib.Path('/app/assets'); dst = pathlib.Path('/tmp/iceberg-metadata')
shutil.rmtree(dst, ignore_errors=True)
for f in src.glob('iceberg-metadata__*'):
    p = dst / f.name.split('__', 1)[1].replace('__', '/')
    p.parent.mkdir(parents=True, exist_ok=True); p.write_text(f.read_text())
shutil.make_archive('/tmp/iceberg-metadata', 'zip', '/tmp', 'iceberg-metadata')
print('zip hazır')
PY
kubectl -n $NS exec deploy/superset-web-server -- superset import-dashboards -p /tmp/iceberg-metadata.zip -u <yonetici>
```

- `-u <yonetici>` **zorunludur** (`import-dashboards --help`, Superset 6.1.0): dashboard'ların sahibi olacak
  **mevcut** Superset kullanıcısı. Kullanıcılar OIDC ile yaratılır (`AUTH_ROLES_MAPPING`), yani ilgili yönetici
  Keycloak ile **en az bir kez giriş yapmış** olmalıdır; aksi hâlde komut kullanıcıyı bulamaz. Dev kümede bu
  `admin1`'dir.
- Bundle içindeki `databases/lakehouse.yaml` mevcut `lakehouse` veritabanına **adla** bağlanır
  (`superset legacy-import-datasources -p /app/configs/trino.yaml` ile kurulan datasource —
  `runbooks/user-facing.md`). Parola bundle'da **yoktur**: Trino parolası `SQLALCHEMY_CUSTOM_PASSWORD_STORE`
  ile env'den gelir, bu yüzden import parola sormaz.
- Import, mevcut `lakehouse` veritabanını bundle'daki `uuid` ile eşleştirir; datasource zaten kuruluysa yeni
  bağlantı yaratılmaz.
- Doğrulama (dashboard listesi):
  ```bash
  kubectl -n $NS exec deploy/superset-web-server -- python3 -c "
  from superset.app import create_app
  with create_app().app_context():
      from superset import db
      from superset.models.dashboard import Dashboard
      print([(d.id, d.dashboard_title, d.slug) for d in db.session.query(Dashboard).all()])"
  ```
  Beklenen: `Iceberg metadata` / slug `iceberg-metadata`. Arayüzde **Dashboards → Iceberg metadata**.

## 4. Yeni tablo ekleme

Dashboard'un tablo listesi sanal veri setinin SQL'inde sabittir (metadata tabloları katalogdan
otomatik keşfedilemez — her tablo ayrı bir `"tbl$snapshots"` ilişkisidir).

1. `glue/files/superset/iceberg-metadata/datasets/lakehouse/iceberg_table_health.yaml` içindeki `sql:`
   alanına bir satır ekleyin:
   ```sql
     UNION ALL SELECT 'erp.invoices', * FROM lakehouse.erp."invoices$snapshots"
   ```
   (sıra önemsiz; `SELECT ... , *` kolon sırası tüm `$snapshots` tablolarında aynıdır.)
2. Commit → ArgoCD sync (ConfigMap güncellenir, pod'da `/app/assets` kendiliğinden tazelenir; kubelet
   ConfigMap yansımasını ~1 dk içinde günceller).
3. §3'teki import'u **yeniden** koşun — ek bayrak gerekmez: `import-dashboards` komutu nesneleri uuid'ye göre
   **her zaman üzerine yazar** (Superset 6.1.0 `cli/importexport.py`: `ImportDashboardsCommand(contents,
   overwrite=True)`; komutun `--overwrite` bayrağı **yoktur**, o bayrak yalnız eski `import-directory`
   komutundadır).
   Alternatif (hızlı, kalıcı değil): Superset arayüzünde veri setini düzenleyip SQL'i elle güncelleyin —
   bir sonraki import bunu ezer, bu yüzden kalıcı değişiklik repoda yapılmalıdır.

## 5. Sınırlar

- Silver/ham tablo adları values'a bağlıdır (`glue/values.yaml` `sources`/`pipelines`): farklı bir kurulumda
  SQL'deki üç demo tablosu yerine kendi tablolarınızı yazın (§4).
- Metrikler **sorgu anında** hesaplanır; Prometheus serisi üretilmez, alarm kurulamaz. Tablo tazeliği alarmı
  Spark tarafından gelir (`LakehouseSilverMergeStale`, `glue/templates/monitoring.yaml`).
- `$files` tüm güncel manifest'leri tarar: çok büyük tablolarda maliyetlidir, dashboard bu yüzden yalnız
  `$snapshots` özetlerini kullanır.
- Depolama (FlashBlade) kapasite/kova metrikleri platformun exporter'ındadır; bu dashboard tablo mantıksal
  boyutunu (`total-files-size`) gösterir, fiziksel/replika tüketimi değil.
