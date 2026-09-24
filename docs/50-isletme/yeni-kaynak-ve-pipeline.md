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

**3.1.2 — Rol, yetkiler ve sinyal tablosu.** Ürün publication'ı **kendisi** yaratır
(`publication.autocreate.mode=filtered`); bunun için superuser gerekmez ama **yakalanacak
tabloların sahibi CDC rolü olmalıdır**. Aşağıdaki blok `erp` veritabanına bağlanır ve
parolayı Adım 2'deki değişkenden alır.

`[kaynak DB]`

```bash
psql -h erp-db.kurum.example.net -U postgres -d erp -v parola="$ERP_DB_PASSWORD" <<'SQL'
CREATE ROLE debezium WITH REPLICATION LOGIN PASSWORD :'parola';
GRANT CONNECT ON DATABASE erp TO debezium;
GRANT CREATE ON DATABASE erp TO debezium;
GRANT USAGE, CREATE ON SCHEMA public TO debezium;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO debezium;
CREATE TABLE public.debezium_signal (id varchar(42) PRIMARY KEY, type varchar(32) NOT NULL,
                                     data varchar(2048));
ALTER TABLE public.debezium_signal OWNER TO debezium;
ALTER TABLE public.orders OWNER TO debezium;
SQL
```

`ALTER TABLE ... OWNER TO debezium` satırı **yakalanacak her tablo için** tekrarlanır.
Sahipliği devretmek istemiyorsanız tek seçenek publication'ı elle yaratıp yönetmektir;
o durumda `sources` girdisine `extraConfig: {publication.autocreate.mode: disabled}`
eklenir ve publication'a tablo eklemek kaynak yöneticisinin işi olur.

**Beklenen çıktı** (`psql` her ifade için bir satır basar):

```text
CREATE ROLE
GRANT
GRANT
GRANT
GRANT
CREATE TABLE
ALTER TABLE
ALTER TABLE
```

**Ters giderse:** `permission denied to create role` → bağlandığınız hesap yeterli yetkiye
sahip değildir. `role "debezium" already exists` → rol vardır; parolayı yenilemek için
`ALTER ROLE debezium WITH PASSWORD :'parola';` kullanın. Sinyal tablosu zaten varsa
`CREATE TABLE` satırını çıkarın; şeması **aynı** olmalıdır.

### 3.2 SQL Server

SQL Server'da CDC hem veritabanı hem tablo düzeyinde açılır ve **SQL Server Agent
servisinin çalışıyor olması şarttır** — yakalama işleri Agent üzerinde koşar.

`[kaynak DB]`

```sql
-- veritabanı düzeyi (bir kez)
EXEC sys.sp_cdc_enable_db;

-- yakalanacak her tablo için
EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'orders',
                             @role_name = NULL;

-- artımlı snapshot sinyal tablosu
CREATE TABLE dbo.debezium_signal (id varchar(42) PRIMARY KEY, type varchar(32) NOT NULL,
                                  data varchar(2048));
```

**Beklenen çıktı** (örnek — `sp_cdc_enable_table` yakalama işlerini başlatır):

```text
Job 'cdc.erp_capture' started successfully.
Job 'cdc.erp_cleanup' started successfully.
```

CDC kullanıcısı ayrıca yaratılır; en az yetki, yakalanan tablolara ve `cdc` şemasına
`SELECT`'tir. `$(parola)`, `sqlcmd`'in değişkenidir: dosya
`sqlcmd -S erp-db.kurum.example.net -U sa -d erp -v parola="$ERP_DB_PASSWORD" -i hazirlik.sql`
biçiminde çalıştırılır.

`[kaynak DB]`

```sql
CREATE LOGIN debezium WITH PASSWORD = '$(parola)';
CREATE USER debezium FOR LOGIN debezium;
GRANT SELECT ON SCHEMA::dbo TO debezium;
GRANT SELECT ON SCHEMA::cdc TO debezium;
```

**TLS zorunludur.** Bağlayıcı `database.encrypt=true` ile açılır
(`glue/templates/connectors.yaml`). Sunucunun sertifikası kurumsal bir CA'dan geliyorsa ve
Connect imajı o CA'ya güvenmiyorsa iki seçenek vardır:

- **Üretim:** truststore'u `sources[].extraConfig` ile verin (`database.trustStore` ve
  `database.trustStorePassword` anahtarları).
- **Yalnız laboratuvar:** `sources[].trustServerCertificate: true`. Bu ayar sertifika
  **doğrulamasını kapatır**; üretimde kullanılmaz.

**Ters giderse:** `SQLServerAgent is not currently running` → Agent kapalıdır, CDC
tabloları dolmaz. `The driver could not establish a secure connection ... certificate`
→ yukarıdaki truststore maddesi.

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
| `trustServerCertificate` | hayır | yalnız `sqlserver`; `true` sertifika doğrulamasını **kapatır** (laboratuvar) |
| `extraConfig` | hayır | Debezium bağlayıcısına eklenen ya da onu ezen anahtarlar |
| `sinkExtraConfig` | hayır | Iceberg sink bağlayıcısına eklenen ya da onu ezen anahtarlar |

**Ters giderse:** `signalTable zorunlu` uyarısı → alan yazılmamıştır.
`postgres|sqlserver|mongodb olmalı` → desteklenmeyen tür. İkisi de `helm template`
aşamasında görülür ([değişiklik nasıl uygulanır](degisiklik-nasil-uygulanir.md) §3).

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

**Beklenen çıktı** (kind kümesinde alınmış gerçek çıktı; oradaki cron gecelik
`0 3 * * *`'tır ve işler önceki kabul koşusundan **askıda** kalmıştır. Üretimde
`SCHEDULE` sütunu `*/15 * * * *`, `SUSPEND` sütunu `false` olur ve `LAST RUN` dolar):

```text
NAME           SCHEDULE    TIMEZONE   SUSPEND   LAST RUN   LAST RUN NAME   AGE
silver-merge   0 3 * * *              true                                 5d14h
```

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
`pipelines` girdisi eksiktir. `SUSPEND` sütunu `true` ise iş askıdadır (kabul testi
askıya alır ve kendiliğinden geri açmaz).

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
açabildiği için ayrı bir yordamdır ve işletme bölümündeki sorun giderme sayfasında
anlatılır. Belirtilerin tam tablosu da oradadır.

---

## 10. nginx erişim günlüğü akışı

Bu akış veritabanı değildir: müşterinin web sunucusunda koşan **Fluent Bit 5.1.2** ajanı
erişim günlüğünü doğrudan Kafka'ya yazar, Iceberg sink de `nginx_raw.access_log`
tablosuna aktarır. `sources` listesine bir şey eklenmez.

```text
access.log -> Fluent Bit -> Kafka dış dinleyici (TLS + SCRAM) -> sink -> nginx_raw.access_log
```

Tablo `day(ts)` ile bölümlenir; `ts` log satırının kendi zamanı (milisaniye), `_fb_ts`
ajanın alım zamanıdır. **Ham IP saklanır**; maskeleme istenirse sink tarafında bir SMT ile
eklenir.

### 10.1 Küme tarafını açın

`platform/values/site/glue.yaml` dosyasında iki anahtar açılır:

```yaml
kafka: {externalListener: true}
nginx: {enabled: true}
```

Commit ve push sonrası ArgoCD şunları yaratır: `KafkaTopic/nginx.access`,
`KafkaUser/fluentbit` (yalnız `nginx.` ön ekine yazma yetkisi),
`KafkaConnector/sink-nginx` ve OpenShift'te her broker için birer Route.

**Ön koşul:** ajan sunucularından kümeye `443/TCP` açık, `$APPS_DOMAIN` altındaki adlar
çözülebilir olmalıdır ([20-on-kosullar](../20-on-kosullar.md) §8).

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" get kafkatopic nginx.access
```

**Beklenen çıktı** (kind kümesinde alınmış gerçek çıktı; üretimde çoğaltma sayısı 3'tür):

```text
NAME           CLUSTER     PARTITIONS   REPLICATION FACTOR   READY
nginx.access   lakehouse   3            1                    True
```

**Ters giderse:** konu görünmüyorsa `nginx.enabled` hâlâ `false`'tur ya da eşitleme
tamamlanmamıştır.

### 10.2 Ajanın ihtiyaç duyduğu üç değeri alın

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" get kafka lakehouse \
  -o jsonpath='{.status.listeners[?(@.name=="external")].bootstrapServers}{"\n"}'
oc -n "$LAKEHOUSE_NS" get secret lakehouse-cluster-ca-cert \
  -o jsonpath='{.data.ca\.crt}' | base64 -d > lakehouse-ca.crt
oc -n "$LAKEHOUSE_NS" get secret fluentbit -o jsonpath='{.data.password}' | base64 -d; echo
```

**Beklenen çıktı** (örnek — OpenShift'e özgü; ilk satır Route host'u ve 443, ikinci satır
ajanın SCRAM parolasıdır):

```text
lakehouse-kafka-bootstrap-lakehouse.apps.ocp.example.net:443
4f6b2c1d9e8a7350b1c4d2e6f0a9837c
```

Üçü de ajan sunucusuna güvenli bir kanalla taşınır: bootstrap adresi, `lakehouse-ca.crt`
dosyası ve parola.

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
install -m 0644 lakehouse-ca.crt /etc/fluent-bit/lakehouse-ca.crt
cat > /etc/default/fluent-bit <<EOF
KAFKA_BOOTSTRAP=$KAFKA_BOOTSTRAP
KAFKA_PASSWORD=$KAFKA_PASSWORD
KAFKA_CA=/etc/fluent-bit/lakehouse-ca.crt
NGINX_ACCESS_LOG=/var/log/nginx/access.log
READ_FROM_HEAD=off
EOF
chmod 600 /etc/default/fluent-bit
systemctl enable --now fluent-bit
```

`KAFKA_BOOTSTRAP` ve `KAFKA_PASSWORD` kabuk değişkenlerine Adım 10.2'de alınan iki değer
yazılır. Dosya parola içerdiği için yalnız `root` tarafından okunabilir olmalıdır.
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

**Beklenen çıktı** (kind kümesinde alınmış gerçek çıktı):

```text
NAME         CLUSTER   CONNECTOR CLASS                                   MAX TASKS   READY
sink-nginx   connect   org.apache.iceberg.connect.IcebergSinkConnector   1           True
```

Satırlar beş dakika içinde `nginx_raw.access_log` tablosuna düşer; sorgu Adım 9.2'deki
not defteri hücresiyle aynıdır, yalnız tablo adı değişir.

- **Disk tamponu.** Ajan `storage.type filesystem` kullanır: Kafka kesilirse 1 GB'a kadar
  yerelde biriktirir, bağlantı dönünce gönderir.
- **Günlük biçimi.** `parsers.conf` içindeki düzenli ifade nginx'in `combined` biçimini
  bekler. Kurum kendi `log_format` tanımını kullanıyorsa ifade güncellenir.
- **Proxy arkasında.** `remote` alanı proxy'nin IP'sidir. Gerçek istemci IP'si için
  nginx'te `real_ip` modülü ya da `X-Forwarded-For` taşıyan bir `log_format` gerekir; bu
  durumda `parsers.conf` de güncellenir.
- **Zaman.** Ay adları ajanda `%b` ile çözülür; sunucunun yerel dili sorun çıkarmaz.
- **`nginx.dlq` doluysa `ts` üretilememiştir.** `ts` alanını ajandaki Lua filtresi
  (`agents/fluent-bit/fluent-bit.conf` → `[FILTER] name lua`, `call add_ts`) epoch
  milisaniye olarak ekler; sink bunu `TimestampConverter` ile zaman tipine çevirir.
  Satır ayrıştırılamazsa (`parsers.conf` düzenli ifadesi tutmazsa) `ts` oluşmaz,
  dönüşüm başarısız olur ve kayıt `nginx.dlq` konusuna düşer. Çözüm ajan tarafındadır:
  `parsers.conf` ifadesini kurumun `log_format` tanımına uydurun.

**Ters giderse:** `sink-nginx` `True` ama tablo boşsa ajan yazamıyordur;
`journalctl -u fluent-bit` çıktısına ve `fluentbit` Secret'ındaki parolaya bakın.
`oc -n "$LAKEHOUSE_NS" get kafkatopic nginx.dlq` bir konu gösteriyorsa yukarıdaki
madde geçerlidir.

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
- [ ] (nginx kullanılacaksa) dış dinleyici açık, ajan koşuyor, `sink-nginx` `READY True`.

## Sonraki bölüm

[mevcut-kaynaga-tablo-ekleme.md](mevcut-kaynaga-tablo-ekleme.md) — var olan bir kaynağa
tablo eklemek; kaynak hazırlığı ve Polaris adımları tekrarlanmaz, yerine **artımlı
snapshot** ile geçmiş satırların geri doldurulması anlatılır. Ortak GitOps döngüsü:
[değişiklik nasıl uygulanır](degisiklik-nasil-uygulanir.md).
