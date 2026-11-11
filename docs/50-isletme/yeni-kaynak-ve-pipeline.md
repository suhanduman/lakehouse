# Yeni kaynak ve pipeline ekleme

**Bu bölümde:** ürüne yeni bir kaynak veritabanı (PostgreSQL, SQL Server ya da MongoDB)
bağlamanın tam yolu — kaynak tarafındaki CDC hazırlığı, kaynağın Secret'ı, Polaris ad
alanları, `platform/values/site/glue.yaml` içindeki `sources` ve `pipelines` girdileri,
doğrulama ve sık hatalar. Son alt bölüm, veritabanı olmayan tek akışı, nginx erişim
günlüğünü (Fluent Bit ajanı) anlatır.
**Süre:** kaynak hazırlığı (DBA) 30–60 dakika; küme tarafı 20 dakika; ilk Bronze
satırları 5 dakikaya kadar, ilk Silver tablosu bir sonraki `silver-merge` koşusunda
(üretimde 15 dakikada bir).
**Gereken yetki:** kaynak veritabanında yönetici; `$LAKEHOUSE_NS` ad alanında Secret
yaratma; kurumun Git deposunda `main` dalına yazma; Polaris kök kimliği
(`polaris-root` Secret'ı) ad alanı eklemek için.
**Nerede çalıştırılır:** `[bastion]` küme ve Git komutları için; `[kaynak DB]` kaynak
veritabanı istemcisinin (`psql`, `sqlcmd`, `mongosh`) koştuğu makine; `[pod]` JupyterHub
not defteri hücresi (doğrulama sorguları); `[nginx ajan sunucusu]` yalnız son alt bölümde.

**Bu bölümde örnek kaynak adı `erp`'dir; kendi kaynağınızın adını yazın.** Örnekte
PostgreSQL veritabanı `erp`, yakalanan tablo `public.orders`, Kubernetes Secret'ı
`erp-db`, Bronze ad alanı `erp_raw`, Silver ad alanı `erp`'tir. SQL Server ve MongoDB alt
bölümleri **aynı** `erp` adını kullanır; yalnız kaynak tarafındaki komutlar değişir.
Ad bir kez seçilir ve sonradan değiştirilmez: Kafka konu adları, çoğaltma yuvası,
Secret adı ve iki katalog ad alanı bu addan türer.

| Addan türeyen | Nerede | Örnekte |
|---|---|---|
| Kafka konu ön eki | Debezium `topic.prefix` (varsayılan = kaynak adı) | `erp.public.orders` |
| PostgreSQL çoğaltma yuvası | `slot.name` | `debezium_erp` |
| PostgreSQL publication | `publication.name` | `dbz_erp_pub` |
| Kubernetes Secret | kaynak adı + `-db` | `erp-db` |
| Bronze ad alanı (katalog) | `bronzeNamespace` (varsayılan: kaynak adı + `_raw`) | `erp_raw` |
| Silver ad alanı (katalog) | Bronze adından `_raw` düşürülerek | `erp` |
| Bağlayıcılar | `dbz-` + ad ve `sink-` + ad | `dbz-erp`, `sink-erp` |

Akış (PostgreSQL ve SQL Server):

```text
kaynak DB --Debezium--> Kafka --Iceberg sink--> erp_raw.orders --silver-merge--> erp.orders
```

MongoDB'de Iceberg sink yoktur: Bronze'u beş dakikada bir koşan `mongo-bronze` Spark işi
yazar, Silver yine `silver-merge` ile üretilir
([00-genel-bakis](../00-genel-bakis.md) §3.2).

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

**Ters giderse:** boş satır görürseniz dosya yüklenmemiştir; şablon
`install/lakehouse.env.example`, doldurma [30-kurulum](../30-kurulum.md) §1.

---

## 2. CDC kullanıcısının parolasını üretin

Parola Git'e **girmez**; bu kabuk oturumunda üretilir, kaynakta ve Secret'ta aynı değer
kullanılır. Oturumun sonunda `unset` ile silinir (Adım 4).

`[bastion]`

```bash
ERP_DB_PASSWORD="$(openssl rand -hex 24)"
export ERP_DB_PASSWORD
printf '%s\n' "$ERP_DB_PASSWORD"    # DBA'ya güvenli kanaldan iletin; ekranı sonra temizleyin
```

**Beklenen çıktı** (örnek — her koşuda başka bir değer; 48 onaltılık karakter):

```text
9f4c1a77b0e2d5386ca41fbb0d7e2c93a6155e80f2bb47d1
```

**Ters giderse:** `openssl: command not found` → makinede OpenSSL yoktur; parolayı
kurumun parola kasasında üretip aynı değişkene okutun (`read -r -s ERP_DB_PASSWORD`).

---

## 3. Kaynak veritabanını hazırlayın

Bu adım **kaynak veritabanı yöneticisinin** işidir ve kümede değil, kaynakta yapılır.
Kaynak türüne göre yalnız bir alt bölüm uygulanır.

**Önce parolayı bu makineye alın.** Adım 2'deki `ERP_DB_PASSWORD` değişkeni yönetim
makinesinin kabuğundadır; aşağıdaki komutlar kaynak veritabanı istemcisinin koştuğu
**başka bir makinede** çalışır ve aynı adlı değişkeni orada da bekler. Veritabanı
yöneticisi, Adım 2'de kendisine güvenli kanalla iletilen parolayı bu bloğa yazar
(`read -r -s` parolayı ekrana basmaz ve kabuk geçmişine düşürmez):

`[kaynak DB]`

```bash
read -r -s ERP_DB_PASSWORD
export ERP_DB_PASSWORD
```

Parolayı yazıp `Enter`'a bastığınızda ekranda hiçbir şey görünmez; bu normaldir. Oturumun
sonunda `unset ERP_DB_PASSWORD` ile silin.

**Ters giderse:** bu bloğu atlarsanız değişken **boştur** ve aşağıdaki komutlar sessizce
**parolasız** bir hesap yaratır; bağlayıcı sonra `password authentication failed for user
"debezium"` ile düşer (Adım 9.4).

### 3.1 PostgreSQL

Debezium, PostgreSQL'in mantıksal çoğaltma günlüğünü okur. Üç şey gerekir: `logical`
seviyesinde WAL, çoğaltma yetkili bir rol ve bir **sinyal tablosu** (artımlı snapshot
için; ürün bu tabloyu zorunlu tutar).

**3.1.1 — WAL seviyesi.** `wal_level` değişikliği **yeniden başlatma** ister; bakım
penceresi planlayın. Yönetilen bir PostgreSQL (CNPG, RDS, Cloud SQL) kullanıyorsanız bu
üç parametre sunucunun kendi yapılandırmasından ayarlanır.

`[kaynak DB]`

```sql
ALTER SYSTEM SET wal_level = 'logical';
ALTER SYSTEM SET max_replication_slots = 10;
ALTER SYSTEM SET max_wal_senders = 10;
```

Yeniden başlatmadan sonra `SHOW wal_level;` ile doğrulayın.

**Beklenen çıktı** (kind kümesindeki PostgreSQL'den alınmış gerçek çıktı):

```text
 wal_level 
-----------
 logical
(1 row)
```

**Ters giderse:** `logical` yerine `replica` görüyorsanız sunucu yeniden başlamamıştır ya
da `postgresql.conf` içindeki değer `ALTER SYSTEM`'i eziyordur. `wal_level` düzelmeden
bağlayıcı çoğaltma yuvasını açamaz.

**3.1.2 — CDC rolü, sinyal tablosu ve publication.** Bu adımı kaynak veritabanının
yöneticisi yapar. Tabloların sahipliği **değişmez**; CDC rolü (`debezium`) yalnız okur:

- `debezium` rolü: oturum açma ve çoğaltma (`REPLICATION`) yetkisi.
- Yakalanacak tablolarda yalnız `SELECT`.
- Sinyal tablosunda okuma ve yazma (ürün artımlı snapshot sırasında buraya işaret satırı yazar).
- **Publication**: hangi tabloların değişikliklerinin dışarı verileceğini söyleyen liste.
  Adı `dbz_` + kaynak adı + `_pub` olmalıdır (bu örnekte `dbz_erp_pub`); ürün bu adla arar.

Aşağıdaki blok `erp` veritabanına bağlanır ve parolayı Adım 2'deki değişkenden alır.
Yakalanacak tabloları `GRANT SELECT` ve `CREATE PUBLICATION` satırlarında virgülle
sıralayın; sinyal tablosu publication'da **bulunmalıdır**.

`[kaynak DB]`

```bash
psql -h erp-db.kurum.example.net -U postgres -d erp -v parola="$ERP_DB_PASSWORD" <<'SQL'
CREATE ROLE debezium WITH REPLICATION LOGIN PASSWORD :'parola';
GRANT CONNECT ON DATABASE erp TO debezium;
GRANT USAGE ON SCHEMA public TO debezium;
GRANT SELECT ON public.orders TO debezium;
CREATE TABLE public.debezium_signal (id varchar(42) PRIMARY KEY, type varchar(32) NOT NULL,
                                     data varchar(2048));
GRANT SELECT, INSERT, UPDATE, DELETE ON public.debezium_signal TO debezium;
CREATE PUBLICATION dbz_erp_pub FOR TABLE public.orders, public.debezium_signal;
SQL
```

**Beklenen çıktı** (`psql` her ifade için bir satır basar):

```text
CREATE ROLE
GRANT
GRANT
GRANT
CREATE TABLE
GRANT
CREATE PUBLICATION
```

Sonradan bu kaynağa tablo eklemek de yöneticinin iki satırıdır
([mevcut-kaynaga-tablo-ekleme.md](mevcut-kaynaga-tablo-ekleme.md) Adım 2).

**Ters giderse:** `permission denied to create role` → bağlandığınız hesap yeterli yetkiye
sahip değildir. `role "debezium" already exists` → rol vardır; parolayı yenilemek için
`ALTER ROLE debezium WITH PASSWORD :'parola';` kullanın. Sinyal tablosu zaten varsa
`CREATE TABLE` satırını çıkarın; şeması **aynı** olmalıdır. `publication "dbz_erp_pub"
already exists` → publication vardır; içeriğini
`SELECT tablename FROM pg_publication_tables WHERE pubname = 'dbz_erp_pub';` ile görün ve
eksik tabloları `ALTER PUBLICATION dbz_erp_pub ADD TABLE …;` ile ekleyin.

### 3.2 SQL Server

SQL Server'da CDC hem veritabanı hem tablo düzeyinde açılır ve **SQL Server Agent
servisinin çalışıyor olması şarttır** — yakalama işleri Agent üzerinde koşar. Bu adımı
kaynak veritabanının yöneticisi yapar; tabloların sahipliği değişmez.

Aşağıdaki iki dosyayı `erp` veritabanında, `sqlcmd` ile sırayla çalıştırın. `$(parola)`
`sqlcmd`'in değişkenidir; parola Adım 2'deki kabuk değişkeninden gelir.

**`cdc-ac.sql`** — CDC'yi açar, sinyal tablosunu yaratır. Sinyal tablosunda da CDC
**açılır**: SQL Server'da ürün sinyalleri bu tablonun değişikliklerinden okur.

```sql
-- veritabanı düzeyi (bir kez)
EXEC sys.sp_cdc_enable_db;

-- yakalanacak her tablo için
EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'orders',
                             @role_name = NULL;

-- artımlı snapshot sinyal tablosu (CDC'si de açılır)
CREATE TABLE dbo.debezium_signal (id varchar(42) PRIMARY KEY, type varchar(32) NOT NULL,
                                  data varchar(2048));
EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'debezium_signal',
                             @role_name = NULL;
GO
```

**`cdc-kullanici.sql`** — CDC kullanıcısı: yakalanan tablolarda ve `cdc` şemasında
yalnız okuma, sinyal tablosunda yazma (ürün artımlı snapshot sırasında buraya işaret
satırı yazar).

```sql
CREATE LOGIN debezium WITH PASSWORD = '$(parola)';
CREATE USER debezium FOR LOGIN debezium;
GRANT SELECT ON SCHEMA::dbo TO debezium;
GRANT SELECT ON SCHEMA::cdc TO debezium;
GRANT INSERT, UPDATE, DELETE ON dbo.debezium_signal TO debezium;
GO
```

`[kaynak DB]`

```bash
sqlcmd -S erp-db.kurum.example.net -U sa -d erp -i cdc-ac.sql
sqlcmd -S erp-db.kurum.example.net -U sa -d erp -v parola="$ERP_DB_PASSWORD" -i cdc-kullanici.sql
```

**Beklenen çıktı** (SQL Server 2022 üzerinde alınmış gerçek çıktı; ikinci komut bir şey
basmaz). İlk satır yalnız bilgi notudur, CLR kapalıyken her zaman görünür:

```text
Update mask evaluation will be disabled in net_changes_function because the CLR configuration option is disabled.
Job 'cdc.erp_capture' started successfully.
Job 'cdc.erp_cleanup' started successfully.
Update mask evaluation will be disabled in net_changes_function because the CLR configuration option is disabled.
```

**SQL Server'da adlar veritabanıyla yazılır.** Kafka konusu `erp.erp.dbo.orders`
biçimindedir (önek, veritabanı, şema, tablo); artımlı snapshot sinyalinde de tablo
`erp.dbo.orders` diye verilir ([mevcut-kaynaga-tablo-ekleme.md](mevcut-kaynaga-tablo-ekleme.md)
Adım 6). Ürün bunu kendisi ayarlar; `sources` girdisinde tablolar yine `dbo.orders`
biçiminde yazılır.

**TLS zorunludur.** Bağlayıcı `database.encrypt=true` ile açılır. Sunucunun sertifikası
kurumsal bir CA'dan geliyorsa ve Connect o CA'ya güvenmiyorsa iki seçenek vardır:

- **Üretim:** truststore'u `sources[].extraConfig` ile verin (`database.trustStore` ve
  `database.trustStorePassword` anahtarları).
- **Yalnız deneme ortamı:** `sources[].trustServerCertificate: true`. Bu ayar sertifika
  **doğrulamasını kapatır**; üretimde kullanılmaz.

**Günlükte görülebilecek iki uyarı zararsızdır:** `Cannot query the status of the SQL
Server Agent` ve `Exception caught while querying sys.dm_cdc_log_scan_sessions`. Bağlayıcı
bunları yalnız izleme için sorar; CDC kullanıcısı sunucu düzeyinde yetkiye sahip olmadığı
için yanıt alamaz, akış etkilenmez.

**Ters giderse:** `SQLServerAgent is not currently running` → Agent kapalıdır, CDC
tabloları dolmaz. `The driver could not establish a secure connection ... certificate`
→ yukarıdaki truststore maddesi. Artımlı snapshot sinyali yazıldığı hâlde günlükte
`Requested 'INCREMENTAL' snapshot` satırı yoksa sinyal tablosunda CDC açılmamıştır.

### 3.3 MongoDB

Debezium MongoDB bağlayıcısı **change stream** kullanır; bu yüzden sunucu bir **replica
set** olmalıdır (tek üyeli `rs0` yeterlidir, tek başına `mongod` çalışmaz).

`[kaynak DB]`

```bash
mongosh "mongodb://erp-db.kurum.example.net:27017/?replicaSet=rs0" --quiet --eval '
  db.getSiblingDB("admin").createUser({
    user: "debezium",
    pwd: process.env.ERP_DB_PASSWORD,
    roles: [{role: "read", db: "erp"},
            {role: "read", db: "config"},
            {role: "clusterMonitor", db: "admin"}]
  })'
```

**Beklenen çıktı** (örnek):

```text
{ ok: 1 }
```

Sinyal koleksiyonu MongoDB'de **isteğe bağlıdır** (`signalCollection`); yalnız artımlı
snapshot kullanacaksanız gerekir ve sıradan bir koleksiyondur — önceden yaratmanız
gerekmez.

**Ters giderse:** `not running with --replSet` → sunucu replica set değildir;
`command createUser requires authentication` → yönetici kimliğiyle bağlanmadınız.

#### MongoDB tablolarının şekli pg/mssql'den farklıdır

Belgeler şemasız olduğu için Bronze ve Silver tabloları kolon kolon açılmaz; koleksiyon
başına **iki** iş kolonu vardır (`glue/jobs/mongo_bronze.py`):

| Kolon | Tip | İçerik |
|---|---|---|
| `_id` | `string` | belgenin `_id` alanı, metne çevrilmiş |
| `_doc` | `string` | **belgenin tamamı JSON metni olarak**; silme olaylarında silinmeden önceki hâli |
| `_cdc` | struct | pg/mssql ile aynı: `op`, `ts`, `offset`, `source`, `target`, `key` |

Alanlara Trino'da `json_extract(_doc, '$.alan')` ile erişilir; tek bir metin/sayı değeri
istendiğinde `json_extract_scalar` daha kullanışlıdır (`json_extract` sonucu yine JSON
değeridir). Örnek sorgu Adım 9.2'dedir. Kolon kolon sorgulanabilir bir Gold tablosu
isteniyorsa bu dönüşüm kurumun kendi dbt/Spark işiyle yapılır.

Ayrıca her koleksiyon için bir **karantina** tablosu yaratılır: Bronze adının sonuna
`__quarantine` eklenir (ör. `erp_raw.orders__quarantine`). İşlenemeyen kayıt buraya
`_key`, `_value`, `reason`, `ts`, `partition`, `offset` ile düşer; `reason` dört değerden
biridir (`glue/jobs/mongo_lib.py`):

| `reason` | Anlamı |
|---|---|
| `bad-envelope` | Debezium zarfı çözülemedi ya da `op` tanınmadı |
| `no-key` | olayın anahtarından `_id` çıkarılamadı |
| `no-ts` | zaman damgası (`ts_ms`) yok ya da sayı değil |
| `after-null` | ekleme/güncelleme olayında `after` belgesi boş |

Karantina tablosunun **boş kalması beklenir**; dolmaya başlarsa kaynak tarafındaki
belgelerde ya da bağlayıcı ayarlarında bir sorun var demektir.

---

## 4. Kaynağın Secret'ını yaratın

Bağlayıcı kimlik bilgisini values'tan değil, `erp-db` Secret'ından okur (`username` ve
`password` anahtarları). Secret **Git'e girmez**.

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" create secret generic erp-db \
  --from-literal=username=debezium \
  --from-literal=password="$ERP_DB_PASSWORD"
unset ERP_DB_PASSWORD
```

**Beklenen çıktı** (örnek):

```text
secret/erp-db created
```

**Ters giderse:** `already exists` → Secret vardır; değeri değiştirmek için
`oc -n "$LAKEHOUSE_NS" delete secret erp-db` sonrası komutu tekrarlayın (bağlayıcı
yeniden başlar). Secret adı **kaynak adı + `-db`** olmak zorundadır: Connect'in Secret
okuma yetkisi bu ada göre verilir.

---

## 5. Polaris ad alanlarını açın

Iceberg sink tabloyu kendiliğinden yaratır ama **ad alanını yaratmaz**. `erp_raw`
(Bronze) ve `erp` (Silver) katalog ad alanları önceden var olmalıdır.

`platform/polaris/setup.yaml` içindeki `namespaces:` listesine iki satır eklenir:

```yaml
    namespaces:
      - name: erp_raw
      - name: erp
```

Sonra kurulum betiği yeniden koşturulur; **idempotenttir**, var olan nesneleri bozmaz:

`[bastion]`

```bash
PATH="$PWD/.venv/bin:$PATH" scripts/polaris-setup.sh --ns "$LAKEHOUSE_NS"
```

**Beklenen çıktı** (kind kümesinde alınmış gerçek çıktıdan seçilmiş satırlar — var olan
ad alanları "already exists" ile geçilir, yenisi yaratılır; son satır yeni principal
üretilmediğini söyler):

```text
2026-09-24 12:35:07,150 INFO Creating namespace: 'crm' in catalog: 'lakehouse'
2026-09-24 12:35:07,187 INFO Namespace 'crm' already exists in catalog 'lakehouse'.
2026-09-24 12:35:07,244 INFO Creating namespace: 'shop_raw' in catalog: 'lakehouse'
2026-09-24 12:35:07,261 INFO Namespace 'shop_raw' already exists in catalog 'lakehouse'.
Yeni principal yok (idempotent çalıştırma).
```

**Ters giderse:** `polaris CLI yok` → sanal ortam yüklenmemiştir
([40-kurulum-sonrasi](../40-kurulum-sonrasi.md) §2.1). Yetki hatası alıyorsanız
`polaris-root` Secret'ı okunamıyordur. Ad alanı açılmadan devam ederseniz sink, ilk
yazmada ad alanı bulunamadı hatasıyla durur (Adım 9.4 tablosu).

---

## 6. `sources` girdisini yazın

Artık tek değişiklik `platform/values/site/glue.yaml` dosyasındadır. PostgreSQL örneği:

```yaml
sources:
- name: erp                               # Secret, konu ön eki ve ad alanı adları bundan türer
  type: postgres                          # postgres | sqlserver | mongodb
  host: erp-db.kurum.example.net
  port: 5432
  database: erp                           # pg: database.dbname; mssql: database.names
  tables: [public.orders]                 # yakalanacak tabloların tam adı (şema dâhil)
  signalTable: public.debezium_signal     # ZORUNLU (pg/mssql): artımlı snapshot sinyal tablosu
```

MongoDB örneği — `connectionString` **kimlik bilgisi içermez**, kullanıcı adı ve parola
yine `erp-db` Secret'ından okunur:

```yaml
sources:
- name: erp
  type: mongodb
  connectionString: "mongodb://erp-db.kurum.example.net:27017/?replicaSet=rs0"
  collections: [erp.orders]               # db.koleksiyon biçiminde
  signalCollection: erp.debezium_signal   # isteğe bağlı
```

Alanların tamamı:

| Alan | Zorunlu | Anlamı |
|---|---|---|
| `name` | evet | kaynağın adı; Secret, konu ön eki, yuva, publication ve ad alanları bundan türer |
| `type` | evet | `postgres`, `sqlserver` ya da `mongodb`; başka değer chart'ı hata ile durdurur |
| `host`, `port` | pg/mssql | kaynak sunucunun adresi ve portu |
| `database` | pg/mssql | pg: `database.dbname`; mssql: `database.names` |
| `tables` | pg/mssql | yakalanacak tablolar; sinyal tablosu listeye **eklenmez**, chart kendisi ekler |
| `signalTable` | pg/mssql | sinyal tablosunun tam adı; Iceberg'e yazılmaz (sink onu dışlar) |
| `connectionString` | mongodb | kimlik bilgisi **içermez**; replica set adı sorgu parametresinde |
| `collections` | mongodb | `db.koleksiyon` biçiminde yakalanacak koleksiyonlar |
| `signalCollection` | hayır | mongodb'de artımlı snapshot sinyal koleksiyonu |
| `topicPrefix` | hayır | Kafka konu ön eki; varsayılan `name` |
| `bronzeNamespace` | hayır | Bronze ad alanı; varsayılan `name` + `_raw` |
| `sinkTasks` | hayır | Iceberg sink görev sayısı (varsayılan 1); yüksek hacimli kaynakta artırılır |
| `initialLoad` | hayır | tablolardaki var olan verinin nasıl geleceği: `snapshot` (varsayılan; bağlayıcı açılırken okur) ya da `incremental` (parça parça, kaldığı yerden süren yükleme — büyük tablolar). Ayrıntı Adım 6.1 |
| `state` | hayır | Debezium bağlayıcısının durumu: `running` (varsayılan), `paused` ya da `stopped`. Kaynağı Git'ten durdurmanın tek kalıcı yolu budur — Kafka Connect'e elle `stop` gönderilirse Strimzi birkaç saniyede geri açar. Offset sıfırlama bunu ister ([kaynak-veya-tablo-silme.md](kaynak-veya-tablo-silme.md) §3) |
| `trustServerCertificate` | hayır | yalnız `sqlserver`; `true` sertifika doğrulamasını **kapatır** (laboratuvar) |
| `extraConfig` | hayır | Debezium bağlayıcısına eklenen ya da onu ezen anahtarlar |
| `sinkExtraConfig` | hayır | Iceberg sink bağlayıcısına eklenen ya da onu ezen anahtarlar |

**Ters giderse:** `signalTable zorunlu` uyarısı → alan yazılmamıştır.
`postgres|sqlserver|mongodb olmalı` → desteklenmeyen tür. İkisi de `helm template`
aşamasında görülür ([değişiklik nasıl uygulanır](degisiklik-nasil-uygulanir.md) §3.2).

### 6.1 İlk yükleme: tablolardaki var olan veri nasıl gelir

Kaynak eklendiğinde tablolarda **zaten** veri vardır. Ürün bu veriyi iki yoldan biriyle
getirir; seçim `sources` girdisindeki `initialLoad` alanıyla yapılır.

| | `snapshot` (varsayılan) | `incremental` |
|---|---|---|
| Nasıl çalışır | Bağlayıcı açılırken tabloları baştan sona okur, sonra değişiklikleri izlemeye geçer | Bağlayıcı yalnız değişiklikleri izleyerek açılır; var olan satırlar sizin göndereceğiniz bir istekle **parça parça** (varsayılan 1.024 satır) okunur |
| Yarıda kalırsa | Yükleme **baştan** başlar (veri kaybolmaz; Bronze'a o ana kadarki satırlar ikinci kez yazılır, Silver yine doğru olur) | **Kaldığı parçadan** sürer |
| Kaynak tabloya etkisi | SQL Server'da okuma süresince satırlar okuma kilidinde kalır | Kilit yoktur; yükleme sırasında canlı değişiklikler de akar |
| Duraklatma / iptal | Yoktur | Vardır (aşağıda) |
| Ne zaman | Binlerce, yüz binlerce satır; okuma dakikalar sürer | Milyonlarca satır, saatler sürecek yükleme ya da kilitlenmemesi gereken tablolar |

Tablolarınız küçükse hiçbir şey yazmanıza gerek yoktur. Büyük tablolar için girdiye bir
satır eklenir:

```yaml
- name: erp
  type: sqlserver
  # ... diğer alanlar aynen kalır
  initialLoad: incremental
```

**SQL Server ve `snapshot` için not:** okuma kilidini istemiyorsanız ama `snapshot` yolunu
kullanmak istiyorsanız, veritabanı yöneticisi veritabanında *snapshot isolation*'ı açabilir
(`ALTER DATABASE erp SET ALLOW_SNAPSHOT_ISOLATION ON;`) ve girdiye
`extraConfig: {snapshot.isolation.mode: snapshot}` eklenir. Okuma tutarlı bir kopyadan
yapılır, yazmalar beklemez.

#### Artımlı yüklemeyi başlatın

`initialLoad: incremental` ile eklenen kaynakta bağlayıcı `RUNNING` olduktan sonra
(Adım 9.1) kaynağın sinyal tablosuna bir istek satırı yazılır. Birden fazla tablo aynı
satırda virgülle verilebilir.

`[kaynak DB]` — SQL Server (tablo adının başında **veritabanı adı** vardır)

```sql
INSERT INTO dbo.debezium_signal (id, type, data)
VALUES (CONVERT(varchar(42), NEWID()), 'execute-snapshot',
        '{"data-collections": ["erp.dbo.orders"], "type": "incremental"}');
```

`[kaynak DB]` — PostgreSQL

```sql
INSERT INTO public.debezium_signal (id, type, data)
VALUES (gen_random_uuid()::text, 'execute-snapshot',
        '{"data-collections": ["public.orders"], "type": "incremental"}');
```

MongoDB'de istek kaynağın `signalCollection` koleksiyonuna yazılır
([mevcut-kaynaga-tablo-ekleme.md](mevcut-kaynaga-tablo-ekleme.md) Adım 6).

**Beklenen çıktı:** `(1 rows affected)` (SQL Server) ya da `INSERT 0 1` (PostgreSQL).

#### İlerlemeyi izleyin

Bağlayıcının günlüğü yüklemenin başladığını ve bittiğini yazar:

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" logs connect-connect-0 --since=1h \
  | grep -E "'INCREMENTAL' snapshot|incremental snapshotting of table|Incremental snapshot"
```

Kaç satırın Kafka'ya ulaştığı konunun uç konumlarının toplamıdır (SQL Server'da konu adı
önek, veritabanı, şema ve tablodur):

`[bastion]`

```bash
scripts/kafka-cli.sh kafka-get-offsets.sh --topic erp.erp.dbo.orders \
  | awk -F: '{s+=$3} END {print s}'
```

**Beklenen çıktı** (gerçek çıktılar: günlük satırı bir SQL Server kaynağından, sayı
300.000 satırlık bir SQL Server tablosunda istekten 40 saniye sonra alındı — sayı her
1.024 satırda bir artar):

```text
2026-11-09 11:19:29 INFO  [debezium-sqlserverconnector-fin-change-event-source-coordinator] ExecuteSnapshot:64 - Requested 'INCREMENTAL' snapshot of data collections '[fin.dbo.invoices]' …
7168
```

Yükleme bitince günlükte `incremental snapshotting of table '…' finished` satırı görünür
ve sayı kaynak tablonun satır sayısına ulaşır. Bronze ve Silver tablolardaki sayıyı Adım
9.2'deki not defteri hücresiyle karşılaştırın (Silver bir `silver-merge` koşusundan sonra
güncellenir).

**Örnek sonuç** (300.000 satırlık SQL Server tablosu; yükleme sürerken Connect pod'u bilerek
silindi, yeniden açılınca kaldığı yerden sürdü — gerçek sayılar):

```text
konudaki kayıt              : 300000
Bronze  count / count(distinct id) : 300000 / 300000
Silver  count / count(distinct id) : 300000 / 300000
```

#### Duraklatın, sürdürün, iptal edin

Kaynak sunucunun yoğun olduğu saatlerde yüklemeyi durdurup sonra sürdürebilirsiniz. İstek
yine sinyal tablosuna yazılır (SQL Server örneği; PostgreSQL'de `id` için
`gen_random_uuid()::text` kullanın):

`[kaynak DB]`

```sql
-- duraklat
INSERT INTO dbo.debezium_signal (id, type, data) VALUES (CONVERT(varchar(42), NEWID()), 'pause-snapshot', NULL);
-- kaldığı yerden sürdür
INSERT INTO dbo.debezium_signal (id, type, data) VALUES (CONVERT(varchar(42), NEWID()), 'resume-snapshot', NULL);
-- iptal et (yeniden başlatmak için önce bu, sonra yeni bir execute-snapshot isteği)
INSERT INTO dbo.debezium_signal (id, type, data)
VALUES (CONVERT(varchar(42), NEWID()), 'stop-snapshot',
        '{"data-collections": ["erp.dbo.orders"], "type": "incremental"}');
```

**Beklenen çıktı** (günlükte; duraklatma ve sürdürme için gerçek satırlar — duraklatılmışken
konudaki sayı artmaz, sürdürülünce artmaya devam eder):

```text
… AbstractIncrementalSnapshotChangeEventSource:257 - Incremental snapshot was paused.
… AbstractIncrementalSnapshotContext:131 - Resuming incremental snapshot
```

#### Bağlayıcı yarıda kalırsa

Bir şey yapmanız gerekmez. Connect yeniden açıldığında (pod yeniden başladı, düğüm
değişti) artımlı yükleme son tamamlanan parçadan kendiliğinden sürer; günlükte şu satır
görünür:

```text
… AbstractIncrementalSnapshotChangeEventSource:235 - Incremental snapshot in progress, need to read new chunk on start
```

Yarıda kalan parça yeniden okunduğu için Bronze'a birkaç yüz satır ikinci kez yazılabilir;
Silver birincil anahtara göre tek satır tuttuğu için sonuç doğrudur.

**Baştan yüklemek** gerekirse (örneğin Bronze tablosunu düşürdünüz): önce `stop-snapshot`,
sonra yeni bir `execute-snapshot` isteği gönderin.

#### Bağlayıcı uzun süre kapalı kalırsa

Değişiklikler kaynakta bir süre bekletilir; bu süre aşılırsa kaçırılan değişiklikler
bir daha okunamaz:

- **SQL Server:** CDC değişiklik tablolarını varsayılan olarak **3 gün** saklar. Bağlayıcı
  bundan uzun süre kapalı kalırsa o tablo artımlı yüklemeyle yeniden getirilir. Süreyi
  veritabanı yöneticisi uzatabilir (`EXEC sys.sp_cdc_change_job @job_type = N'cleanup',
  @retention = 10080;` — dakika cinsinden, bu örnekte 7 gün).
- **PostgreSQL:** ters durum: bağlayıcı kapalıyken sunucu değişiklik günlüğünü (WAL)
  siler**mez**, disk dolar. Bağlayıcının uzun süre kapalı kalacağı biliniyorsa ya da kaynak
  kaldırılacaksa çoğaltma yuvası düşürülür
  ([kaynak-veya-tablo-silme.md](kaynak-veya-tablo-silme.md) Adım 8).

İki durumu da izleme alarmı `LakehouseConnectTaskFailed` erken haber verir
([izleme-ve-alarmlar.md](izleme-ve-alarmlar.md)).

---

## 7. `pipelines` girdisini yazın

`sources` Bronze'u doldurur; Silver tabloları **yalnız** `pipelines` listesine yazılan
tablolar için üretilir. Bir tabloyu Silver'a taşımak için birincil anahtarı bilmek
gerekir.

```yaml
pipelines:
- bronze: erp_raw.orders                  # Bronze ad alanı + tablo; Silver adı: erp.orders
  keys: [id]                              # birincil anahtar kolon(lar)ı
  bucket_count: 16                        # bölümleme kova sayısı
  casts: {updated_at: timestamp}          # Bronze'da metin gelen zaman kolonları
```

| Alan | Varsayılan | Anlamı ve seçim ölçütü |
|---|---|---|
| `bronze` | — | Bronze tablonun tam adı; Silver adı `_raw` düşürülerek türetilir |
| `keys` | — | birincil anahtar; boş olamaz. Bileşik anahtarda **kardinalitesi en yüksek kolonu başa** yazın: bölümleme yalnız ilk anahtarla yapılır |
| `write_mode` | `merge-on-read` | büyük tablolarda varsayılan kalsın; küçük ve çok okunan tablolarda `copy-on-write` okumayı hızlandırır |
| `bucket_count` | `16` | `bucket(bucket_count, keys[0])`. Kabaca her kovaya 100–500 MB düşecek şekilde seçin: milyon satır altı tablolarda 4–8, on milyonlarda 16–32 |
| `casts` | `{}` | kolon → Spark tipi. Zaman dilimli zaman kolonları Bronze'a **metin** olarak gelir; Silver'da zaman tipi isteniyorsa buraya yazılır |
| `silver` | türetilir | Silver tablo adını elle vermek için; ad kuralından sapmak gerekmedikçe kullanılmaz |

**Append-only tablolar listelenmez.** Silver, kaynağın güncel hâline eşit olan
birleştirilmiş katmandır; günlük ve olay tabloları yalnız Bronze'da kalır ve oradan
sorgulanır.

**MongoDB pipeline'ı.** Koleksiyonlarda anahtar her zaman `keys: [_id]`'dir ve `casts`
kullanılmaz: Bronze'da iş kolonu yoktur, belge `_doc` içinde JSON metni olarak durur
(Adım 3.3). Silver de aynı `(_id, _doc)` şeklini taşır.

```yaml
pipelines:
- {bronze: erp_raw.orders, keys: [_id], bucket_count: 16}
```

**Ters giderse:** `'bronze' ve boş olmayan 'keys' zorunlu` → anahtar verilmemiştir.
`casts bilinmeyen kolon(lar)` → `casts` içinde Bronze'da olmayan bir kolon adı vardır.

---

## 8. Commit edin, itin, eşitlemeyi izleyin

Bu adımın tamamı ortak döngüdür:
[değişiklik nasıl uygulanır](degisiklik-nasil-uygulanir.md).

`[bastion]`

```bash
bash scripts/check-site.sh
git add platform/values/site/glue.yaml platform/polaris/setup.yaml
git commit -m "site: erp kaynagi ve pipeline eklendi"
git push origin main
```

**ArgoCD'siz (helm/kind) kurulumda:** bkz.
[30-kurulum.md kind kutusu](../30-kurulum.md#kind-gun2) (`helm upgrade`) — site dosyası bu
modda okunmaz.


ArgoCD eşitlediğinde şunlar yaratılır ya da güncellenir: `KafkaUser/connect` ACL'leri
(kaynağın konu ön eki için), `Role/connect-secrets-reader` (Connect'in `erp-db`
Secret'ını okuyabilmesi için; `glue/templates/kafka-connect.yaml`),
`KafkaConnector/dbz-erp` ve `KafkaConnector/sink-erp`, `ConfigMap/lakehouse-jobs`
(Silver pipeline tanımları) ve — ilk pipeline eklendiğinde —
`ScheduledSparkApplication/silver-merge` ile üç bakım işi.

**Beklenen çıktı** (örnek — OpenShift'e özgü; eşitleme bittiğinde):

```text
Synced Healthy
```

**Ters giderse:** `Synced Degraded` → nesneler uygulandı ama bağlayıcı sağlıksızdır;
Adım 9.4'e geçin.

---

## 9. Doğrulayın

### 9.1 Bağlayıcılar

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" get kafkaconnector
```

**Beklenen çıktı** (kind kümesinde alınmış gerçek çıktı; oradaki kaynağın adı `shop`,
sizde `dbz-erp` ve `sink-erp` satırlarını görürsünüz):

```text
NAME         CLUSTER   CONNECTOR CLASS                                      MAX TASKS   READY
dbz-shop     connect   io.debezium.connector.postgresql.PostgresConnector   1           True
sink-shop    connect   org.apache.iceberg.connect.IcebergSinkConnector      1           True
```

Görev düzeyinde durum:

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" get kafkaconnector dbz-erp \
  -o jsonpath='{.status.connectorStatus.tasks[0]}{"\n"}'
```

**Beklenen çıktı** (kind kümesinde alınmış gerçek çıktı; `shop` kaynağının görevi):

```text
{"id":0,"state":"RUNNING","version":"3.6.2.Final","worker_id":"connect-connect-0.connect-connect.lakehouse.svc:8083"}
```

PostgreSQL kaynaklarında yuvanın gerçekten açıldığını kaynak tarafında da görebilirsiniz:

`[kaynak DB]`

```sql
SELECT slot_name, plugin, active FROM pg_replication_slots;
```

**Beklenen çıktı** (kind kümesindeki PostgreSQL'den alınmış gerçek çıktı; yuva adı
`debezium_` + kaynak adıdır):

```text
   slot_name   |  plugin  | active 
---------------+----------+--------
 debezium_shop | pgoutput | t
(1 row)
```

**Ters giderse:** `READY` sütunu boş ya da `False` → Adım 9.4 tablosuna bakın.
`active` sütunu `f` ise bağlayıcı yuvaya bağlı değildir (görev düşmüştür).

### 9.2 Bronze

İlk satırların görünmesi iki şeyi bekler: Iceberg sink'in denetim tüketicisinin gruba
katılması (ölçülen ~2 dakika) ve ilk commit aralığının dolması (üretimde 300 saniye,
geliştirme kurulumunda 30 saniye). Yani **ilk beş dakika boş tablo normaldir.**

`[pod]` (JupyterHub not defteri hücresi)

```python
import os, trino
conn = trino.dbapi.connect(host=os.environ["TRINO_HOST"], port=8443, http_scheme="https",
                           verify="/etc/lakehouse-ca/tls.crt",
                           auth=trino.auth.OAuth2Authentication(), catalog="lakehouse")
cur = conn.cursor(); cur.execute("select count(*) from erp_raw.orders"); cur.fetchall()
```

Hücre ilk çalıştığında bir giriş adresi basar ve bekler: adresi tarayıcıda açıp kendi AD
hesabınızla Keycloak'a girin, hücre sonra kendiliğinden devam eder. Hücre
`CA cert does not include key usage extension` hatası verirse iç kök CA eski komutla
üretilmiştir: [sorun-giderme.md](sorun-giderme.md) §7.

**Beklenen çıktı** (örnek — kaynak tablonuzdaki olay sayısı):

```text
[[12483]]
```

Bronze satırı, iş kolonlarının yanında bir `_cdc` yapısı taşır: `op` (`I`, `U` ya da
`D`), `ts`, `offset`, `source`, `target`, `key`. Silme olayları da satır olarak durur;
bu yüzden Bronze sayısı kaynaktakinden **büyüktür**.

**MongoDB kaynaklarında sorgu farklıdır.** Bronze'da iş kolonu yoktur; belge `_doc`
içinde JSON metni olarak durur (Adım 3.3), bu yüzden alanlara `json_extract` ile
bakılır. Aşağıdaki hücre hem satır sayısını hem bir alanın gerçekten geldiğini gösterir:

`[pod]` (JupyterHub not defteri hücresi)

```python
cur = conn.cursor()
cur.execute("""select _id,
                      json_extract_scalar(_doc, '$.status') as status,
                      _cdc.op
               from erp_raw.orders
               order by _cdc.ts desc
               limit 5""")
cur.fetchall()
```

**Beklenen çıktı** (örnek — kendi belgelerinizin alanlarıyla):

```text
[['66f0c1a2e4b09a7d3c5f1234', 'new', 'I'], ['66f0c1a2e4b09a7d3c5f1235', 'paid', 'U']]
```

MongoDB kaynağında ayrıca **karantina tablosunun boş olduğunu** doğrulayın; dolu bir
tablo, işlenemeyen olay demektir (`reason`: `bad-envelope`, `no-key`, `no-ts`,
`after-null` — Adım 3.3):

`[pod]` (JupyterHub not defteri hücresi)

```python
cur = conn.cursor()
cur.execute("select reason, count(*) from erp_raw.orders__quarantine group by 1")
cur.fetchall()
```

**Beklenen çıktı** (örnek — sağlıklı bir akışta karantina boştur):

```text
[]
```

**Ters giderse:** beş dakika sonra hâlâ 0 satır varsa Connect günlüğünde snapshot
satırlarını arayın:
`oc -n "$LAKEHOUSE_NS" logs connect-connect-0 --tail=200 | grep -i snapshot`. MongoDB'de
Bronze'u Iceberg sink değil, beş dakikada bir koşan `mongo-bronze` Spark işi yazar:
`oc -n "$LAKEHOUSE_NS" get scheduledsparkapplication mongo-bronze` ile askıda olmadığını
doğrulayın.

### 9.3 Silver

Silver tablosu ilk `silver-merge` koşusunda Spark tarafından yaratılır (üretimde 15
dakikada bir). Zamanlamayı görmek için:

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" get scheduledsparkapplication silver-merge
```

**Beklenen çıktı** (OpenShift provasında alınmış gerçek çıktı):

```text
NAME           SCHEDULE       TIMEZONE   SUSPEND   LAST RUN   LAST RUN NAME                      AGE
silver-merge   */15 * * * *              false     59s        silver-merge-1794154500000843588   115m
```

`SUSPEND` sütunu `false` olmalıdır.

Koşu bittikten sonra Silver'ı sorgulayın:

`[pod]` (JupyterHub not defteri hücresi)

```python
cur = conn.cursor(); cur.execute("select count(*) from erp.orders"); cur.fetchall()
```

**Beklenen çıktı** (örnek — kaynaktaki **güncel** satır sayısına eşit olmalıdır):

```text
[[9741]]
```

**Ters giderse:** tablo bulunamadı hatası → henüz hiç `silver-merge` koşmamış ya da
`pipelines` girdisi eksiktir. `SUSPEND` sütunu `true` ise iş askıdadır: kabul testi
koşarken işleri askıya alır ve bitince geri açar, ancak test yarıda kesilirse (bağlantı
koptu, dizüstü kapandı) askıda kalırlar. ArgoCD bunu düzeltmez; geri açmak için
[sorun-giderme.md](sorun-giderme.md) §6.1. Açıldıktan sonra kaçırılan koşu hemen yapılır.

### 9.4 Sık hatalar

| Belirti | Neden | Çözüm |
|---|---|---|
| `dbz-erp` FAILED, `password authentication failed for user "debezium"` (mssql/mongo: `Login failed` / `Authentication failed`) | kaynakta hesap **boş parolayla** yaratılmış: Adım 3'ün başındaki `read -r -s ERP_DB_PASSWORD` bloğu atlanmış ya da `erp-db` Secret'ındaki değer farklı | parolayı kaynakta yenileyin (`ALTER ROLE debezium WITH PASSWORD :'parola';`) ve `erp-db` Secret'ını aynı değerle yeniden yaratın (Adım 4) |
| `dbz-erp` FAILED, izde `must be superuser or replication role` | rolde `REPLICATION` yok | Adım 3.1.2'deki `CREATE ROLE ... REPLICATION` |
| `dbz-erp` FAILED, `replication slot ... already exists and is active` | aynı adla başka bir bağlayıcı yuvayı tutuyor | eski kaynağı kaldırın ya da kaynağa başka bir ad verin |
| `dbz-erp` FAILED, publication hatası ve rol tablo sahibi değil | `filtered` publication yaratılamıyor | tabloların sahibini CDC rolüne alın (Adım 3.1.2) |
| `dbz-erp` FAILED, sertifika güven hatası (SQL Server) | TLS güven zinciri eksik | truststore'u `extraConfig` ile verin (Adım 3.2) |
| `dbz-erp` FAILED, `not running with --replSet` (MongoDB) | kaynak replica set değil | Adım 3.3 |
| Bağlayıcılar `True` ama Bronze boş | denetim tüketicisi henüz katılmadı ya da ilk commit aralığı dolmadı | 5 dakika bekleyin; sonra Connect günlüğüne bakın |
| Sink FAILED, ad alanı bulunamadı ya da Polaris yetki hatası | Bronze ad alanı katalogda yok | Adım 5'i koşturun |
| Sink FAILED, `TopicAuthorizationException` | ACL kaynağın konu ön ekini kapsamıyor | `topicPrefix` ile Kafka konusunun ön eki aynı mı |
| Bronze doluyor, Silver güncellenmiyor | `silver-merge` askıda ya da düşüyor | `SUSPEND` sütunu; `LakehouseSilverMergeStale` alarmı |
| Silver'da `SchemaConflict` | kolon tipi güvenli genişletilemiyor | Silver'da elle `ALTER TABLE` ya da yeni kolon |

Bağlayıcıyı yeniden başlatmak (deklaratif; nesneyi silmez):

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" annotate kafkaconnector dbz-erp strimzi.io/restart=true --overwrite
```

Hata izini okumak:

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" get kafkaconnector dbz-erp \
  -o jsonpath='{.status.connectorStatus.tasks[0].trace}{"\n"}' | head -30
```

**Ölü mektup kuyruğu hakkında bir uyarı:** Iceberg sink hatalı kaydı kuyruğa **yazmaz**;
sonu `.dlq` olan konu yalnız dönüştürücü ve SMT hatalarını alır. Yazma hatası görevi
durdurur ve izleme alarmıyla görünür.

Bağlayıcı yeniden başlatmaya rağmen düzelmiyor ve tüketici konumu ileriye kaymışsa
(kaydın işlenmeden geçilmesi) **offset sıfırlama** gerekir; bu, veri yinelemesine yol
açabildiği için ayrı bir yordamdır ve
[sorun-giderme.md](sorun-giderme.md) §4.1'de anlatılır. Belirtilerin tam tablosu da
oradadır (§3).

---

## 10. nginx erişim günlüğü akışı

Bu akış veritabanı değildir: müşterinin web sunucusunda koşan **Fluent Bit 5.1.2** ajanı
erişim günlüğünü doğrudan Kafka'ya yazar, Iceberg sink de `nginx_raw.access_log`
tablosuna aktarır. `sources` listesine bir şey eklenmez.

```text
access.log -> Fluent Bit -> Kafka dış dinleyici (TLS + SCRAM) -> sink -> nginx_raw.access_log
```

**Birden fazla nginx sunucusu** varsa hepsi aynı konuya ve aynı tabloya yazar; satırın
hangi sunucudan geldiği `server` kolonundadır. Sunucu başına ayrı tablo açılmaz: sorgular
`where server = '…'` ile süzülür, yeni sunucu eklemek küme tarafında hiçbir değişiklik
gerektirmez (Adım 10.5). Günlükler yalnız eklenir, hiç güncellenmez; bu yüzden Silver
katmanı yoktur, özetler doğrudan bu tablodan üretilir.

Tablo `day(ts)` ile bölümlenir; `ts` log satırının kendi zamanı (milisaniye), `_fb_ts`
ajanın alım zamanıdır. **Ham IP saklanır**; maskeleme istenirse sink tarafında bir SMT ile
eklenir.

### 10.1 Küme tarafını açın

`platform/values/site/glue.yaml` dosyasında iki anahtar açılır. `nginx:` satırı dosyada
zaten vardır, `enabled` değerini `true` yapın. `kafka:` satırı şablonda **yoktur**; `nginx:`
satırının hemen üstüne ekleyin:

```yaml
kafka: {externalListener: true}
nginx: {enabled: true, partitions: 3}
```

Dış dinleyiciyi açmak Kafka broker'larını sırayla yeniden başlatır (provada ~2,5 dakika);
bu sırada boru hattı durmaz.

Commit ve push sonrası ArgoCD şunları yaratır: `KafkaTopic/nginx.access`,
`KafkaUser/fluentbit` (yalnız `nginx.` ön ekine yazma yetkisi),
`KafkaConnector/sink-nginx` ve OpenShift'te her broker için birer Route.

**Ön koşul:** ajan sunucularından kümeye `443/TCP` açık, `$APPS_DOMAIN` altındaki adlar
çözülebilir olmalıdır ([20-on-kosullar](../20-on-kosullar.md) §8).

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" get kafkatopic nginx.access
```

**Beklenen çıktı** (OpenShift provasında alınmış gerçek çıktı):

```text
NAME           CLUSTER     PARTITIONS   REPLICATION FACTOR   READY
nginx.access   lakehouse   3            3                    True
```

**Ters giderse:** konu görünmüyorsa `nginx.enabled` hâlâ `false`'tur ya da eşitleme
tamamlanmamıştır.

### 10.2 Ajanın ihtiyaç duyduğu üç değeri alın

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" get kafka lakehouse \
  -o jsonpath='{.status.listeners[?(@.name=="external")].bootstrapServers}{"\n"}'
oc -n "$LAKEHOUSE_NS" get secret lakehouse-cluster-ca-cert \
  -o jsonpath='{.data.ca\.crt}' | base64 -d > kafka-ca.crt
oc -n "$LAKEHOUSE_NS" get secret fluentbit -o jsonpath='{.data.password}' | base64 -d; echo
```

**Beklenen çıktı** (OpenShift provasında doğrulandı; parola örnektir): blok üç komut
içerir ama ekrana **iki** satır basar — ortadaki komut hiçbir şey yazdırmaz, CA'yı
`kafka-ca.crt` dosyasına yazar (`ls -l kafka-ca.crt` ile doğrulayın). Bu, Kafka'nın kendi
CA'sıdır; kurulumdaki `lakehouse-ca` (Trino) ile karıştırmayın. Birinci satır
Route host'u ve 443, ikinci satır ajanın SCRAM parolasıdır:

```text
lakehouse-kafka-bootstrap-lakehouse.apps.ocp.example.net:443
4f6b2c1d9e8a7350b1c4d2e6f0a9837c
```

Üçü de ajan sunucusuna güvenli bir kanalla taşınır: bootstrap adresi, `kafka-ca.crt`
dosyası ve parola. Ajan sunucusu `$APPS_DOMAIN` altındaki **dört** adı çözebilmelidir:
bootstrap ve her broker için birer Route (`oc -n "$LAKEHOUSE_NS" get route | grep dual-role`).

**Ters giderse:** bootstrap satırı boş dönerse dış dinleyici açılmamıştır (Adım 10.1).
ArgoCD'siz vanilla kümelerde dinleyici tipi `nodeport`'tur; adres
`oc -n "$LAKEHOUSE_NS" get svc lakehouse-kafka-external-bootstrap` çıktısındaki düğüm
portundan kurulur.

### 10.3 Ajanı kurun

Fluent Bit 5.1.2 paketi kurulur, ardından depodaki iki yapılandırma dosyası kopyalanır:
`agents/fluent-bit/fluent-bit.conf` ve `agents/fluent-bit/parsers.conf` →
`/etc/fluent-bit/`.

`[nginx ajan sunucusu]`

```bash
install -m 0644 fluent-bit.conf parsers.conf /etc/fluent-bit/
install -m 0644 kafka-ca.crt /etc/fluent-bit/kafka-ca.crt
cat > /etc/default/fluent-bit <<EOF
KAFKA_BOOTSTRAP=$KAFKA_BOOTSTRAP
KAFKA_PASSWORD=$KAFKA_PASSWORD
KAFKA_CA=/etc/fluent-bit/kafka-ca.crt
NGINX_ACCESS_LOG=/var/log/nginx/access.log
NGINX_SERVER_NAME=$(hostname -s)
READ_FROM_HEAD=off
EOF
chmod 600 /etc/default/fluent-bit
systemctl enable --now fluent-bit
```

`KAFKA_BOOTSTRAP` ve `KAFKA_PASSWORD` kabuk değişkenlerine Adım 10.2'de alınan iki değer
yazılır. `NGINX_SERVER_NAME` bu sunucunun tablodaki adıdır (`server` kolonu); komut
sunucunun kısa adını yazar, isterseniz `web01` gibi kendi adınızı verin. Her sunucuda
**farklı** olmalıdır. Dosya parola içerdiği için yalnız `root` tarafından okunabilir olmalıdır.
`READ_FROM_HEAD=off` ajanın yalnız **yeni** satırları göndermesini sağlar; var olan
dosyanın tamamını geri yüklemek isterseniz ilk açılışta `on` yapın.

**Beklenen çıktı** (örnek — `systemctl status fluent-bit` ilk satırları):

```text
● fluent-bit.service - Fluent Bit
     Active: active (running)
```

**Ters giderse:** `journalctl -u fluent-bit` çıktısında yetki hatası → parola yanlıştır;
sertifika hatası → CA dosyası yanlış yolda ya da bozuktur; dosya bulunamadı →
`NGINX_ACCESS_LOG` yolu yanlıştır.

### 10.4 Doğrulayın ve bilinmesi gerekenler

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" get kafkaconnector sink-nginx
```

**Beklenen çıktı** (OpenShift provasında alınmış gerçek çıktı):

```text
NAME         CLUSTER   CONNECTOR CLASS                                   MAX TASKS   READY
sink-nginx   connect   org.apache.iceberg.connect.IcebergSinkConnector   1           True
```

Satırlar beş dakika içinde `nginx_raw.access_log` tablosuna düşer; sorgu Adım 9.2'deki
not defteri hücresiyle aynıdır, yalnız tablo adı değişir.

- **Saklama süresi.** Günlük tablosu varsayılan olarak süresiz büyür. Kurumun saklama
  kuralına göre `nginx` girdisine `retentionDays: 365` gibi bir değer yazılırsa her gece bu
  günden eski satırlar silinir.
- **Disk tamponu.** Ajan `storage.type filesystem` kullanır: Kafka kesilirse 1 GB'a kadar
  yerelde biriktirir, bağlantı dönünce gönderir.
- **Günlük biçimi.** `parsers.conf` içindeki düzenli ifade nginx'in `combined` biçimini ve
  nginx'in **varsayılan** `main` biçimini (sonunda `"$http_x_forwarded_for"` olan; resmi
  imajda ve RHEL paketinde gelen) tanır. Sondaki ek tırnaklı alanlar yok sayılır. Kurum
  bambaşka bir `log_format` kullanıyorsa ifade güncellenir; uymayan satır kaybolmaz, aşağıdaki
  "ayrıştırılamayan satır" maddesine bakın.
- **Proxy arkasında.** `remote` alanı proxy'nin IP'sidir. Gerçek istemci IP'si için
  nginx'te `real_ip` modülü ya da `X-Forwarded-For` taşıyan bir `log_format` gerekir; bu
  durumda `parsers.conf` de güncellenir.
- **Zaman.** Ay adları ajanda `%b` ile çözülür; sunucunun yerel dili sorun çıkarmaz.
- **Ayrıştırılamayan satır.** Biçime uymayan satır **kaybolmaz**: tabloya iş kolonları
  (`remote`, `path`, `code` …) boş, satırın tamamı `log` kolonunda olarak yazılır; `ts`
  ajanın okuma zamanıdır. `log` kolonu tabloda ancak ilk böyle satırda belirir. Varsa
  şöyle bulunur (Adım 9.2'deki not defteri hücresiyle):

  ```python
  cur = conn.cursor()
  cur.execute("select ts, log from nginx_raw.access_log where remote is null limit 5")
  cur.fetchall()
  ```

  Satır görürseniz `parsers.conf` ifadesini o satıra uydurun; ajan yeniden başlatılınca
  yeni satırlar doğru kolonlara düşer (eski satırlar olduğu gibi kalır).
- **`nginx.dlq`.** Sink'in hiç okuyamadığı kayıtlar (JSON olmayan ileti gibi) bu konuya
  düşer; ajan doğru kurulduğunda boş kalır.

**Ters giderse:** `sink-nginx` `True` ama tablo boşsa ajan yazamıyordur;
`journalctl -u fluent-bit` çıktısına ve `fluentbit` Secret'ındaki parolaya bakın.
`oc -n "$LAKEHOUSE_NS" get kafkatopic nginx.dlq` bir konu gösteriyorsa yukarıdaki
madde geçerlidir.

### 10.5 Yeni bir nginx sunucusu eklemek

Küme tarafında bir şey değişmez: yeni sunucuya Adım 10.3 aynen uygulanır, yalnız
`NGINX_SERVER_NAME` o sunucunun adı olur. Bootstrap adresi, `kafka-ca.crt` ve parola
bütün sunucularda aynıdır (Adım 10.2'deki değerler).

Birkaç dakika sonra yeni sunucunun satırları aynı tabloda görünür (Adım 9.2'deki not
defteri hücresiyle):

```python
cur = conn.cursor()
cur.execute("select server, count(*) from nginx_raw.access_log group by server order by 1")
cur.fetchall()
```

**Beklenen çıktı** (iki sunucuyla — `web01` ve `web02` — alınmış gerçek çıktı; her
sunucu bir satır, yanında o sunucudan gelen istek sayısı):

```text
[['web01', 3], ['web02', 2]]
```

**Ters giderse:** yeni sunucu listede yoksa o sunucuda `journalctl -u fluent-bit`
çıktısına bakın (Adım 10.3'ün "Ters giderse" maddeleri). İki sunucu aynı adla görünüyorsa
`NGINX_SERVER_NAME` değerleri aynıdır; birini değiştirip ajanı yeniden başlatın.


---

## Kontrol listesi

- [ ] Kaynakta CDC hazırlığı yapıldı (WAL ya da CDC açık, rol ve yetkiler verildi).
- [ ] Sinyal tablosu (pg/mssql) kaynakta var ve şeması doğru.
- [ ] `erp-db` Secret'ı kümede; parola Git'e girmedi ve kabuk değişkeni silindi.
- [ ] `platform/polaris/setup.yaml` içinde iki yeni ad alanı var ve betik koşturuldu.
- [ ] `sources` ve `pipelines` girdileri `platform/values/site/glue.yaml` dosyasında.
- [ ] Commit itildi; `glue` uygulaması `Synced Healthy`.
- [ ] `dbz-` ve `sink-` bağlayıcılarının ikisi de `READY True`.
- [ ] Bronze tabloda satır var; bir `silver-merge` koşusundan sonra Silver tablo var.
- [ ] (nginx kullanılacaksa) dış dinleyici açık, ajan her sunucuda kendi `NGINX_SERVER_NAME`
      değeriyle koşuyor, `sink-nginx` `READY True`.

## Sonraki bölüm

Uygulamalardan ya da IoT cihazlarından Kafka'ya JSON gönderen akışlar için:
[json-akisi-ekleme.md](json-akisi-ekleme.md).

[mevcut-kaynaga-tablo-ekleme.md](mevcut-kaynaga-tablo-ekleme.md) — var olan bir kaynağa
tablo eklemek; kaynak hazırlığı ve Polaris adımları tekrarlanmaz, yerine **artımlı
snapshot** ile geçmiş satırların geri doldurulması anlatılır. Ortak GitOps döngüsü:
[değişiklik nasıl uygulanır](degisiklik-nasil-uygulanir.md).
