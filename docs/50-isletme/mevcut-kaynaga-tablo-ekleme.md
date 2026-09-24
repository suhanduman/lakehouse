# Mevcut kaynağa tablo ekleme

**Bu bölümde:** zaten bağlı olan bir kaynağa yeni bir tablo eklemek — kaynaktaki izin
düzeltmesi, `sources[].tables` ve `pipelines` girdileri, bağlayıcının yeni tabloyu
aldığının doğrulanması, **artımlı snapshot** ile geçmiş satırların geri doldurulması ve
Bronze/Silver kontrolü.
**Süre:** 15 dakika + snapshot süresi (küçük tablolarda saniyeler, on milyonlarca satırda
saatler; bu sırada akış durmaz).
**Gereken yetki:** kaynak veritabanında tablo sahipliğini değiştirme ya da CDC açma;
kurumun Git deposunda `main` dalına yazma; `$LAKEHOUSE_NS` ad alanında okuma.
**Nerede çalıştırılır:** `[bastion]` küme ve Git komutları için; `[kaynak DB]` kaynak
veritabanı istemcisinin koştuğu makine; `[pod]` JupyterHub not defteri hücresi.

**Bu bölümde örnek kaynak adı `shop`, eklenen tablo `public.customers`'tır; kendi
kaynağınızın ve tablonuzun adını yazın.** Bu örnek geliştirme kümesinde birebir
koşturulmuştur; aşağıdaki çıktıların çoğu o koşudan alınmıştır.

**Bu bölümde yapılmayanlar.** Kaynak veritabanının CDC hazırlığı, kaynağın Secret'ı ve
Polaris ad alanları **zaten vardır**; tekrarlanmaz. Yeni tablo var olan Bronze ad alanına
(`shop_raw`) düşer, Silver tablosu var olan `shop` ad alanında yaratılır. Bunlar ilk kez
yapılacaksa [yeni-kaynak-ve-pipeline.md](yeni-kaynak-ve-pipeline.md) izlenir.

```text
kaynakta izin -> values (tables + pipelines) -> commit/push -> sync
              -> artımlı snapshot sinyali -> Bronze -> silver-merge -> Silver
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

## 2. Kaynakta tabloyu yakalamaya hazırlayın

Yeni tablo, kaynağın CDC kullanıcısının erişebildiği ve — PostgreSQL'de — **sahibi
olduğu** bir tablo olmalıdır. Publication `filtered` modda yönetildiği için Debezium
tabloyu publication'a kendisi ekler, ama bunu ancak tablo sahibiyse yapabilir.

`[kaynak DB]` — PostgreSQL

```sql
ALTER TABLE public.customers OWNER TO debezium;
```

`[kaynak DB]` — SQL Server (tablo düzeyinde CDC ayrıca açılır)

```sql
EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'customers',
                             @role_name = NULL;
```

MongoDB'de bu adım yoktur: koleksiyon yalnız `collections` listesine eklenir; okuma
yetkisi veritabanı düzeyinde zaten verilmiştir.

**Beklenen çıktı** (PostgreSQL):

```text
ALTER TABLE
```

**Ters giderse:** `must be owner of table` → bağlandığınız hesap tablonun sahibi
değildir. Sahipliği devretmek istemiyorsanız publication'ı elle yönetmeniz
(`ALTER PUBLICATION ... ADD TABLE`) ve kaynağa
`extraConfig: {publication.autocreate.mode: disabled}` eklemeniz gerekir.

---

## 3. Values dosyasına iki satır ekleyin

`platform/values/site/glue.yaml` dosyasında ilgili kaynağın `tables` listesine tablo
eklenir. Tablo **entity** tablosuysa (birincil anahtarı olan, güncellenen bir tablo)
`pipelines` listesine de bir satır girer; append-only bir olay tablosuysa yalnız Bronze'da
kalır ve `pipelines` girdisi yazılmaz.

Aşağıdaki parça, bu bölümün provasının koştuğu **geliştirme** kurulumundan alınmıştır
(`platform/values/glue-dev.yaml`); `host` bu yüzden küme içi bir servis adıdır. Üretimde
aynı alanlar `platform/values/site/glue.yaml` dosyasında, kaynak sunucunun gerçek adıyla
durur.

```yaml
sources:
- name: shop
  type: postgres
  host: demo-pg-rw.lakehouse.svc                # üretimde kaynak sunucunun DNS adı
  port: 5432
  database: shop
  tables: [public.orders, public.customers]     # eklenen: public.customers
  signalTable: public.debezium_signal
pipelines:
- {bronze: shop_raw.orders, keys: [id], bucket_count: 4, casts: {updated_at: timestamp}}
- {bronze: shop_raw.customers, keys: [id], bucket_count: 4, casts: {updated_at: timestamp}}
```

**`casts` körlemesine kopyalanmaz.** Yeni satırdaki `casts` yalnız **o tabloda gerçekten
bulunan** kolonları sayabilir: örnekteki `public.customers` tablosunun bir `updated_at`
kolonu vardır, bu yüzden satır `orders` ile aynı görünür. Tabloda olmayan bir kolon
yazılırsa `silver-merge` `casts bilinmeyen kolon(lar)` hatasıyla düşer. Zaman kolonu
olmayan bir tabloda `casts` alanı hiç yazılmaz.

MongoDB kaynaklarında `tables` yerine `collections` listesine `db.koleksiyon` yazılır,
`keys` her zaman `[_id]`'dir ve `casts` kullanılmaz (Bronze'da iş kolonu yoktur; belge
`_doc` içinde JSON metni olarak durur —
[yeni-kaynak-ve-pipeline.md](yeni-kaynak-ve-pipeline.md) §3.3).

`pipelines` alanlarının anlamı ve `keys`, `bucket_count`, `casts` seçim ölçütleri:
[yeni-kaynak-ve-pipeline.md](yeni-kaynak-ve-pipeline.md) §7.

**Ters giderse:** `casts bilinmeyen kolon(lar)` hatası, `casts` içinde Bronze'da olmayan
bir kolon adı bulunduğunu söyler; kolon adını kaynaktaki yazımıyla girin.

---

## 4. Commit edin, itin, eşitlemeyi izleyin

Ortak döngü: [değişiklik nasıl uygulanır](degisiklik-nasil-uygulanir.md).

`[bastion]`

```bash
bash scripts/check-site.sh
git add platform/values/site/glue.yaml
git commit -m "site: shop kaynagina public.customers eklendi"
git push origin main
```

Eşitleme sırasında Debezium bağlayıcısı **yeniden başlar** (yapılandırması değişmiştir);
akış birkaç saniye duraklar ve kaldığı yerden sürer. PostgreSQL'de publication da
kendiliğinden güncellenir.

**Beklenen çıktı** (örnek — OpenShift'e özgü; eşitleme bittiğinde):

```text
Synced Healthy
```

**Ters giderse:** `Synced Degraded` → bağlayıcı yeni yapılandırmayla açılamamıştır;
Adım 8'e bakın.

---

## 5. Bağlayıcının yeni tabloyu aldığını doğrulayın

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" get kafkaconnector dbz-shop \
  -o jsonpath='{.spec.config.table\.include\.list}{"\n"}'
```

**Beklenen çıktı** (kind kümesinde alınmış gerçek çıktı; sinyal tablosunu listeye chart
kendisi ekler):

```text
public.orders,public.customers,public.debezium_signal
```

PostgreSQL'de publication'ın güncellendiği Connect günlüğünde de görülür:

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" logs connect-connect-0 --tail=500 | grep -i "Publication"
```

**Beklenen çıktı** (kind kümesinde alınmış gerçek satır):

```text
2026-09-24 09:16:50 INFO  [debezium-postgresconnector-shop-change-event-source-coordinator] PostgresReplicationConnection:521 - Updating Publication with statement 'ALTER PUBLICATION dbz_shop_pub SET TABLE "public"."customers", "public"."debezium_signal", "public"."orders";'
```

**Ters giderse:** liste eski hâlindeyse eşitleme tamamlanmamıştır. Publication satırının
ardından bir yetki hatası geliyorsa Adım 2 yapılmamıştır.

---

## 6. Geçmiş satırları artımlı snapshot ile doldurun

Bağlayıcı artık tablodaki **yeni** değişiklikleri yakalar; ama tablodaki **var olan**
satırlar Kafka'ya hiç gitmemiştir. Bunları getirmek için bağlayıcıyı durdurup baştan
snapshot almak gerekmez — kaynağın sinyal tablosuna bir satır yazılır ve Debezium tabloyu
parça parça, akışı durdurmadan geri doldurur.

`[kaynak DB]` — PostgreSQL

```sql
INSERT INTO public.debezium_signal (id, type, data)
VALUES ('adhoc-1', 'execute-snapshot',
        '{"data-collections": ["public.customers"], "type": "incremental"}');
```

`[kaynak DB]` — SQL Server (aynı içerik, `dbo` şemasındaki sinyal tablosuna)

```sql
INSERT INTO dbo.debezium_signal (id, type, data)
VALUES (CONVERT(varchar(42), NEWID()), 'execute-snapshot',
        '{"data-collections": ["dbo.customers"], "type": "incremental"}');
```

`[kaynak DB]` — MongoDB (kaynakta `signalCollection` tanımlı olmalıdır)

```js
db.getSiblingDB("shop").debezium_signal.insertOne({
  type: "execute-snapshot",
  data: {"data-collections": ["shop.customers"], "type": "incremental"}
})
```

`id` alanı yalnız ayırt edici olmalıdır; aynı değerle ikinci kez yazarsanız satır
çakışır. Arka arkaya snapshot alacaksanız her seferinde başka bir değer (PostgreSQL'de
`gen_random_uuid()::text`) kullanın.

İlerlemeyi Connect günlüğünden izleyin:

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" logs connect-connect-0 --tail=500 | grep -i "incremental snapshot"
```

**Beklenen çıktı** (kind kümesinde alınmış gerçek satırlar; tablo iki satırlıydı, bu
yüzden ilk parçadan sonra snapshot hemen bitti):

```text
2026-09-24 09:17:52 INFO  [debezium-postgresconnector-shop-change-event-source-coordinator] ExecuteSnapshot:64 - Requested 'INCREMENTAL' snapshot of data collections '[public.customers]' with additional conditions '[]' and surrogate key 'PK of table will be used'
2026-09-24 09:17:52 INFO  [debezium-postgresconnector-shop-change-event-source-coordinator] AbstractIncrementalSnapshotChangeEventSource:325 - No data returned by the query, incremental snapshotting of table 'public.customers' finished
```

**Ters giderse:** günlükte hiç snapshot satırı yoksa sinyal kaynağa yazılmamış ya da
sinyal tablosu `signalTable` değerinden farklı bir yerde olabilir. `Requested` satırı var
ama satır gelmiyorsa tablonun birincil anahtarı yoktur; o durumda sinyalin `data` alanına
`"surrogate-key"` eklenmesi gerekir.

---

## 7. Bronze ve Silver'ı doğrulayın

Bronze tablosu, sink'in ilk commit'inden sonra kendiliğinden yaratılır (üretimde 300
saniyelik commit aralığı, geliştirme kurulumunda 30 saniye).

`[pod]` (JupyterHub not defteri hücresi)

```python
import os, trino
conn = trino.dbapi.connect(host=os.environ["TRINO_HOST"], port=8443, http_scheme="https",
                           verify="/etc/lakehouse-ca/tls.crt",
                           auth=trino.auth.OAuth2Authentication(), catalog="lakehouse")
cur = conn.cursor(); cur.execute("select count(*) from shop_raw.customers"); cur.fetchall()
```

**Beklenen çıktı** (satır sayısı kind provasından gerçektir — tablo iki satırlıydı; blok,
Trino istemcisinin dönüş biçimidir):

```text
[[2]]
```

Silver tablosu bir sonraki `silver-merge` koşusunda Spark tarafından yaratılır. Koşunun
yeni pipeline'ı gördüğü sürücü günlüğünden okunur:

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" logs -l spark-role=driver --tail=2000 | grep -E "MERGE_OK|->"
```

**Beklenen çıktı** (kind kümesindeki `silver-merge` sürücüsünden alınmış gerçek satırlar;
ilk koşuda Silver tablosu henüz yok olduğu için tam okuma yapılır ve tablo yaratılır):

```text
[shop_raw.customers] -> lakehouse.shop.customers: FALLBACK full, 2 anahtar, snapshot None -> 2806037388819634651
MERGE_OK
```

`[pod]` (JupyterHub not defteri hücresi)

```python
cur = conn.cursor(); cur.execute("select count(*) from shop.customers"); cur.fetchall()
```

**Beklenen çıktı** (satır sayısı kind provasından gerçektir; Silver, kaynağın **güncel**
hâline eşittir):

```text
[[2]]
```

**Ters giderse:** Bronze doluyken Silver tablosu yoksa `pipelines` girdisi eksiktir ya da
`silver-merge` askıdadır (`oc -n "$LAKEHOUSE_NS" get scheduledsparkapplication` çıktısındaki
`SUSPEND` sütunu). Silver satır sayısı kaynaktan azsa snapshot hâlâ sürüyordur.

---

## 8. Sık durumlar

| Belirti | Neden | Çözüm |
|---|---|---|
| `table.include.list` eski hâlinde | eşitleme tamamlanmadı | ArgoCD durumu; hard refresh |
| Günlükte publication yetki hatası | tablo sahibi CDC rolü değil | Adım 2 |
| Bronze tablosu yaratılmıyor | denetim tüketicisi henüz katılmadı ya da commit aralığı dolmadı | 5 dakika bekleyin |
| Bronze'da yalnız yeni değişiklikler var, geçmiş yok | artımlı snapshot sinyali yazılmadı | Adım 6 |
| Snapshot satırları gelmiyor, tabloda birincil anahtar yok | sinyal anahtar sütununu bulamıyor | sinyale `surrogate-key` ekleyin |
| Silver tablosu yaratılmıyor | `pipelines` girdisi yok ya da iş askıda | Adım 3; `SUSPEND` sütunu |
| Silver'da `SchemaConflict` | kolon tipi güvenli genişletilemiyor | Silver'da elle `ALTER TABLE` ya da yeni kolon |

Belirtilerin tam tablosu: [sorun-giderme.md](sorun-giderme.md) §3.

---

## Kontrol listesi

- [ ] Kaynakta tablo CDC kullanıcısının erişimine (PostgreSQL'de sahipliğine) açıldı.
- [ ] `tables` listesine tablo eklendi; entity tablosuysa `pipelines` girdisi de yazıldı.
- [ ] Commit itildi; `glue` uygulaması `Synced Healthy`.
- [ ] `table.include.list` yeni tabloyu içeriyor.
- [ ] Artımlı snapshot sinyali yazıldı ve günlükte `Requested 'INCREMENTAL' snapshot`
      satırı görüldü.
- [ ] Bronze tablosunda satırlar var.
- [ ] Bir `silver-merge` koşusundan sonra Silver tablosu var ve satır sayısı kaynağın
      güncel hâline eşit.

## Sonraki bölüm

[kaynak-veya-tablo-silme.md](kaynak-veya-tablo-silme.md) — kaynak ya da tablo çıkarmak;
[yeni-spark-uygulamasi.md](yeni-spark-uygulamasi.md) — kendi Spark uygulamanız;
[kullanici-ve-yetki.md](kullanici-ve-yetki.md) — yetki yönetimi;
[yedek-ve-geri-donus.md](yedek-ve-geri-donus.md) — yedek ve geri dönüş;
[izleme-ve-alarmlar.md](izleme-ve-alarmlar.md) ve
[sorun-giderme.md](sorun-giderme.md) — izleme ve sorun giderme. Değişikliğin kümeye
geçtiği ortak yol her zaman aynıdır:
[değişiklik nasıl uygulanır](degisiklik-nasil-uygulanir.md).
