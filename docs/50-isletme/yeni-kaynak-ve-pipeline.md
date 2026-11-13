# Yeni kaynak ve pipeline ekleme

Bu sayfa, lakehouse'a yeni bir kaynak veritabanı (PostgreSQL, SQL Server ya da MongoDB)
bağlamayı baştan sona anlatır: kaynak veritabanında değişiklik yakalamanın (CDC)
açılması, parolanın kümeye kaydedilmesi, katalog ad alanlarının açılması, ayar dosyasına
kaynağın ve tabloların yazılması ve sonucun doğrulanması. Son bölüm, veritabanı olmayan
nginx erişim günlüğü akışını ve günlükten üretilen özet tabloları anlatır.

**Ne kadar sürer:** kaynak veritabanının hazırlanması 30–60 dakika, küme tarafı 20
dakika. İlk Bronze satırları 5–15 dakika içinde, ilk Silver tablosu bir sonraki
`silver-merge` çalışmasında (15 dakikada bir) görünür.

**Hangi yetkiler gerekir:**

- kaynak veritabanında yönetici hesabı (veritabanı yöneticisi yapar);
- `$LAKEHOUSE_NS` ad alanında Secret yaratma;
- kurumun lakehouse Git deposunda `main` dalına yazma.

**Komutlar nerede çalışır:**

- `[bastion]` — kümeye `oc` ile bağlı ve Git deposuna erişen yönetim makinesi;
- `[kaynak DB]` — kaynak veritabanının istemcisinin (`psql`, `sqlcmd`, `mongosh`)
  çalıştığı makine;
- `[pod]` — JupyterHub'da bir not defteri hücresi (doğrulama sorguları);
- `[nginx ajan sunucusu]` — yalnız son bölümde, nginx'in çalıştığı sunucu.

**Bu sayfadaki örnek kaynağın adı `erp`'dir; komutlarda kendi kaynağınızın adını
yazın.** Örnekte PostgreSQL veritabanı `erp`, yakalanan tablo `public.orders`'tır. SQL
Server ve MongoDB bölümleri de aynı `erp` adını kullanır; yalnız kaynak veritabanındaki
komutlar değişir.

Kaynağın adı bir kez seçilir ve sonradan değiştirilmez; aşağıdaki adların hepsi ondan
türer:

| Addan türeyen | Örnekte |
|---|---|
| Kafka konusu | `erp.public.orders` (SQL Server'da `erp.erp.dbo.orders`) |
| PostgreSQL çoğaltma yuvası | `debezium_erp` |
| PostgreSQL publication | `dbz_erp_pub` |
| Parolanın tutulduğu Secret | `erp-db` |
| Bronze ad alanı (katalogda) | `erp_raw` |
| Silver ad alanı (katalogda) | `erp` |
| Bağlayıcılar | `dbz-erp` (kaynaktan okur), `sink-erp` (Bronze'a yazar) |

Verinin yolu (PostgreSQL ve SQL Server):

```text
kaynak DB --Debezium--> Kafka --Iceberg sink--> erp_raw.orders --silver-merge--> erp.orders
```

MongoDB'de Bronze'u bağlayıcı değil, beş dakikada bir çalışan `mongo-bronze` Spark işi
yazar; Silver yine `silver-merge` ile üretilir
([00-genel-bakis](../00-genel-bakis.md) §3.2).

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
yoktur ya da başka bir klasördesiniz. Dosya kurulumda doldurulur
([30-kurulum](../30-kurulum.md) §1).

---

## 2. CDC kullanıcısının parolasını üretin

Lakehouse, kaynak veritabanına kendine ait bir kullanıcıyla bağlanır. Bu kullanıcının
parolası burada üretilir; aynı değer hem kaynak veritabanında hem kümede kullanılır.
Parola Git'e yazılmaz.

`[bastion]`

```bash
ERP_DB_PASSWORD="$(openssl rand -hex 24)"
export ERP_DB_PASSWORD
printf '%s\n' "$ERP_DB_PASSWORD"
```

Ekrana yazılan parolayı veritabanı yöneticisine güvenli bir kanalla iletin, sonra ekranı
temizleyin (`clear`).

**Beklenen çıktı** (her çalıştırmada başka bir değer; 48 karakter):

```text
9f4c1a77b0e2d5386ca41fbb0d7e2c93a6155e80f2bb47d1
```

**Ters giderse:** `openssl: command not found` görürseniz parolayı kurumun parola
kasasında üretin ve `read -r -s ERP_DB_PASSWORD` ile değişkene okutun.

---

## 3. Kaynak veritabanını hazırlayın

Bu adım **kaynak veritabanının yöneticisinin** işidir ve kaynak veritabanında yapılır.
Kaynağın türüne göre aşağıdaki üç alt bölümden **yalnız biri** uygulanır.

**Önce parolayı bu makineye alın.** Adım 2'deki parola yönetim makinesindedir;
aşağıdaki komutlar başka bir makinede çalışır ve aynı adlı değişkeni orada da bekler.
Veritabanı yöneticisi kendisine iletilen parolayı şu komutla girer:

`[kaynak DB]`

```bash
read -r -s ERP_DB_PASSWORD
export ERP_DB_PASSWORD
```

Parolayı yazıp `Enter`'a basın. Ekranda hiçbir şey görünmez; bu normaldir. İş bitince
`unset ERP_DB_PASSWORD` ile silin.

**Ters giderse:** bu adımı atlarsanız değişken boş kalır ve aşağıdaki komutlar
**parolasız** bir kullanıcı yaratır. Bağlayıcı sonra `password authentication failed`
hatası verir (Adım 9.4).

### 3.1 PostgreSQL

Lakehouse, PostgreSQL'in değişiklik günlüğünü (mantıksal çoğaltma) okur. Bunun için
üç şey gerekir: değişiklik günlüğünün `logical` seviyesinde tutulması, çoğaltma yetkisi
olan bir kullanıcı ve bir **sinyal tablosu**. Sinyal tablosu, var olan verinin sonradan
parça parça yüklenmesi (artımlı yükleme) için kullanılır ve zorunludur.

**3.1.1 — Değişiklik günlüğü seviyesi.** Bu ayar veritabanının **yeniden başlatılmasını**
gerektirir; bir bakım penceresi planlayın. Yönetilen bir PostgreSQL hizmeti (RDS, Cloud
SQL ve benzeri) kullanıyorsanız bu üç ayar hizmetin kendi panelinden yapılır.

`[kaynak DB]`

```sql
ALTER SYSTEM SET wal_level = 'logical';
ALTER SYSTEM SET max_replication_slots = 10;
ALTER SYSTEM SET max_wal_senders = 10;
```

Veritabanı yeniden başladıktan sonra kontrol edin:

```sql
SHOW wal_level;
```

**Beklenen çıktı:**

```text
 wal_level 
-----------
 logical
(1 row)
```

**Ters giderse:** `logical` yerine `replica` görüyorsanız veritabanı yeniden
başlamamıştır ya da `postgresql.conf` dosyasındaki değer bu ayarı eziyordur. Bu ayar
düzelmeden bağlayıcı çalışamaz.

**3.1.2 — CDC kullanıcısı, sinyal tablosu ve publication.** Tabloların sahibi
değişmez; CDC kullanıcısı (`debezium`) yalnız okur. Aşağıdaki komutlar şunları yapar:

- `debezium` kullanıcısını oturum açma ve çoğaltma yetkisiyle yaratır;
- yakalanacak tablolarda yalnız okuma (`SELECT`) yetkisi verir;
- sinyal tablosunu yaratır ve kullanıcıya bu tabloda okuma-yazma yetkisi verir;
- **publication** yaratır: hangi tabloların değişikliklerinin dışarı verileceğini
  söyleyen liste. Adı `dbz_` + kaynak adı + `_pub` olmalıdır (burada `dbz_erp_pub`);
  lakehouse bu adla arar.

Yakalanacak tabloları `GRANT SELECT` ve `CREATE PUBLICATION` satırlarında virgülle
sıralayın. Sinyal tablosu publication'da **bulunmalıdır**.

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

**Beklenen çıktı** (`psql` her komut için bir satır yazar):

```text
CREATE ROLE
GRANT
GRANT
GRANT
CREATE TABLE
GRANT
CREATE PUBLICATION
```

Publication'ın içeriğini kontrol edin:

```sql
SELECT tablename FROM pg_publication_tables WHERE pubname = 'dbz_erp_pub';
```

**Beklenen çıktı** (yakaladığınız tablolar ve `debezium_signal`; aşağıdaki çıktı
`orders` ve `customers` tablolarını yakalayan bir kaynaktan):

```text
    tablename    
-----------------
 orders
 debezium_signal
 customers
(3 rows)
```

Bu kaynağa sonradan tablo eklemek de yöneticinin iki satırıdır
([mevcut-kaynaga-tablo-ekleme.md](mevcut-kaynaga-tablo-ekleme.md) Adım 2).

**Ters giderse:**

- `permission denied to create role` — bağlandığınız hesabın yetkisi yetmiyor.
- `role "debezium" already exists` — kullanıcı zaten var; parolasını değiştirmek için
  `ALTER ROLE debezium WITH PASSWORD :'parola';` kullanın.
- Sinyal tablosu zaten varsa `CREATE TABLE` satırını çıkarın; tablonun kolonları
  yukarıdakilerle aynı olmalıdır.
- `publication "dbz_erp_pub" already exists` — publication zaten var; eksik tabloları
  `ALTER PUBLICATION dbz_erp_pub ADD TABLE …;` ile ekleyin.

### 3.2 SQL Server

SQL Server'da CDC hem veritabanında hem her tabloda ayrıca açılır. **SQL Server Agent
servisi çalışıyor olmalıdır**; değişiklikleri yakalayan işler onun üzerinde çalışır.
Tabloların sahibi değişmez.

Aşağıdaki iki dosyayı `erp` veritabanında `sqlcmd` ile sırayla çalıştırın.

**`cdc-ac.sql`** — CDC'yi açar ve sinyal tablosunu yaratır. Sinyal tablosunda da CDC
**açılır**: SQL Server'da lakehouse sinyalleri bu tablonun değişikliklerinden okur.

```sql
-- veritabanı düzeyinde (bir kez)
EXEC sys.sp_cdc_enable_db;

-- yakalanacak her tablo için
EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'orders',
                             @role_name = NULL;

-- sinyal tablosu (CDC'si de açılır)
CREATE TABLE dbo.debezium_signal (id varchar(42) PRIMARY KEY, type varchar(32) NOT NULL,
                                  data varchar(2048));
EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'debezium_signal',
                             @role_name = NULL;
GO
```

**`cdc-kullanici.sql`** — CDC kullanıcısını yaratır: yakalanan tablolarda ve `cdc`
şemasında yalnız okuma, sinyal tablosunda yazma yetkisi verir. `$(parola)`, `sqlcmd`'e
komut satırından verilen değişkendir.

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

**Beklenen çıktı** (ikinci komut bir şey yazmaz). `Update mask evaluation …` satırı bir
bilgi notudur; CDC açılan her tablo için bir kez görünür:

```text
Update mask evaluation will be disabled in net_changes_function because the CLR configuration option is disabled.
Job 'cdc.erp_capture' started successfully.
Job 'cdc.erp_cleanup' started successfully.
Update mask evaluation will be disabled in net_changes_function because the CLR configuration option is disabled.
```

**SQL Server'da adlarda veritabanı da geçer.** Kafka konusu `erp.erp.dbo.orders`
biçimindedir: kaynak adı, veritabanı, şema, tablo. Artımlı yükleme isteğinde de tablo
`erp.dbo.orders` diye yazılır (Adım 6.1). Ayar dosyasında ise tablolar yine `dbo.orders`
biçiminde yazılır; gerisini lakehouse ayarlar.

**Bağlantı şifrelidir.** Bağlayıcı SQL Server'a TLS ile bağlanır. Sunucunun sertifikası
kurumsal bir CA'dan geliyorsa ve Kafka Connect o CA'ya güvenmiyorsa iki seçenek vardır:

- **Üretim:** CA'yı içeren bir truststore'u `sources[].extraConfig` ile verin
  (`database.trustStore` ve `database.trustStorePassword` ayarları).
- **Yalnız deneme ortamı:** `sources[].trustServerCertificate: true`. Bu ayar sertifika
  **doğrulamasını kapatır**; üretimde kullanılmaz.

**Günlükte görülebilecek iki uyarı zararsızdır:** `Cannot query the status of the SQL
Server Agent` ve `Exception caught while querying sys.dm_cdc_log_scan_sessions`.
Bağlayıcı bu bilgileri yalnız izleme için sorar; CDC kullanıcısının sunucu düzeyinde
yetkisi olmadığı için yanıt alamaz. Veri akışı etkilenmez.

**Ters giderse:**

- `SQLServerAgent is not currently running` — Agent kapalıdır; değişiklikler
  yakalanmaz. Agent'ı başlatın.
- `The driver could not establish a secure connection ... certificate` — yukarıdaki
  truststore maddesine bakın.
- Artımlı yükleme isteği yazıldığı hâlde günlükte `Requested 'INCREMENTAL' snapshot`
  satırı görünmüyorsa sinyal tablosunda CDC açılmamıştır.

### 3.3 MongoDB

MongoDB bağlayıcısı **change stream** kullanır; bu yüzden sunucu bir **replica set**
olmalıdır. Tek üyeli bir replica set (`rs0`) yeterlidir; tek başına çalışan bir `mongod`
yetmez.

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

MongoDB'de sinyal koleksiyonu **isteğe bağlıdır** (`signalCollection`). Yalnız artımlı
yükleme kullanacaksanız gerekir; sıradan bir koleksiyondur ve önceden yaratılması
gerekmez.

**Ters giderse:**

- `not running with --replSet` — sunucu bir replica set değildir.
- `command createUser requires authentication` — yönetici hesabıyla bağlanmadınız.

#### MongoDB tablolarının biçimi farklıdır

MongoDB belgelerinin sabit bir kolon yapısı olmadığı için Bronze ve Silver tablolarında
kolonlar açılmaz. Her koleksiyonun tablosunda **iki** veri kolonu vardır:

| Kolon | Tip | İçerik |
|---|---|---|
| `_id` | `string` | belgenin `_id` alanı, metne çevrilmiş |
| `_doc` | `string` | **belgenin tamamı JSON metni olarak**; silinen belgelerde silinmeden önceki hâli |
| `_cdc` | yapı | değişikliğin bilgileri: `op`, `ts`, `offset`, `source`, `target`, `key` |

Belgenin alanlarına Trino'da `json_extract_scalar(_doc, '$.alan')` ile ulaşılır
(örnek sorgu Adım 9.2'de). Alanları ayrı kolonlar olarak sorgulamak istiyorsanız bu
dönüşüm kendi Gold işinizle yapılır
([yeni-spark-uygulamasi.md](yeni-spark-uygulamasi.md)).

Her koleksiyon için ayrıca bir **karantina** tablosu yaratılır: Bronze adının sonuna
`__quarantine` eklenir (ör. `erp_raw.orders__quarantine`). İşlenemeyen bir değişiklik
buraya düşer; `reason` kolonu nedenini söyler:

| `reason` | Anlamı |
|---|---|
| `bad-envelope` | değişiklik kaydı çözülemedi ya da işlem türü tanınmadı |
| `no-key` | kayıttan `_id` çıkarılamadı |
| `no-ts` | kaydın zaman bilgisi yok ya da sayı değil |
| `after-null` | ekleme ya da güncelleme kaydında belgenin kendisi boş |

Karantina tablosunun **boş kalması beklenir**. Dolmaya başlarsa kaynak veritabanındaki
belgelerde ya da bağlayıcı ayarlarında bir sorun vardır.

---

## 4. Kaynağın Secret'ını yaratın

Bağlayıcı, kaynak veritabanına bağlanırken kullanıcı adını ve parolayı kümedeki `erp-db`
Secret'ından okur. Secret Git'e girmez.

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" create secret generic erp-db \
  --from-literal=username=debezium \
  --from-literal=password="$ERP_DB_PASSWORD"
oc -n "$LAKEHOUSE_NS" label secret erp-db lakehouse.io/yonetim=manual-yeni-kaynak-ve-pipeline-4
unset ERP_DB_PASSWORD
```

**Beklenen çıktı:**

```text
secret/erp-db created
secret/erp-db labeled
```

İkinci komut, Secret'ı bu adımda elle yaratıldığını gösteren etiketle işaretler
([değişiklik nasıl uygulanır](degisiklik-nasil-uygulanir.md) §7).

**Ters giderse:**

- `already exists` — Secret zaten var. Değerini değiştirmek için önce
  `oc -n "$LAKEHOUSE_NS" delete secret erp-db` çalıştırın, sonra bu adımın komutlarını
  tekrarlayın. Bağlayıcı yeni parolayla yeniden başlar.
- Secret'ın adı **kaynak adı + `-db`** olmalıdır; Kafka Connect'in okuma izni bu ada
  göre verilir.

---

## 5. Katalog ad alanlarını açın

Bronze tablosu ilk veri geldiğinde kendiliğinden yaratılır, ama tablonun **ad alanı**
önceden var olmalıdır. Bu örnekte iki ad alanı gerekir: `erp_raw` (Bronze) ve `erp`
(Silver).

`platform/polaris/setup.yaml` dosyasını açın:

`[bastion]`

```bash
vi platform/polaris/setup.yaml
```

Dosyanın sonuna doğru `namespaces:` listesini bulun ve listenin son satırının altına iki
satır ekleyin. Satırların başındaki boşluk, listedeki diğer satırlarla aynı olmalıdır:

```yaml
      - name: erp_raw
      - name: erp
```

Dosyayı kaydedin ve kurulum betiğini çalıştırın. Betik var olan ad alanlarına dokunmaz,
yalnız eksikleri yaratır:

`[bastion]`

```bash
PATH="$PWD/.venv/bin:$PATH" scripts/polaris-setup.sh --ns "$LAKEHOUSE_NS"
```

**Beklenen çıktı:** çıktı uzundur. Her ad alanı için bir `Creating namespace` satırı
görünür; zaten var olanlar hemen ardından `already exists` ile geçilir. Son satır yeni
bir kullanıcı yaratılmadığını söyler. Örnek satırlar (tarih ve saat sizde farklıdır):

```text
2026-11-11 16:01:03,098 INFO Creating namespace: 'universite' in catalog: 'lakehouse'
2026-11-11 16:01:03,319 INFO Namespace 'universite' already exists in catalog 'lakehouse'.
Yeni principal yok (idempotent çalıştırma).
```

**Ters giderse:**

- `polaris CLI yok` — betiğin kullandığı Python ortamı kurulmamıştır
  ([40-kurulum-sonrasi](../40-kurulum-sonrasi.md) §2.1).
- Yetki hatası — betik `polaris-root` Secret'ını okuyamıyordur.
- Bu adım atlanırsa Bronze'a yazan bağlayıcı ilk yazmada "ad alanı bulunamadı" hatasıyla
  durur (Adım 9.4).

---

## 6. `sources` girdisini yazın

Bundan sonraki değişiklik `platform/values/site/glue.yaml` dosyasındadır. Dosyayı açın:

`[bastion]`

```bash
vi platform/values/site/glue.yaml
```

`sources:` listesini bulun ve listenin son öğesinin altına kaynağınızın girdisini
ekleyin. Liste boşsa (`sources: []`) `[]` işaretini silip girdiyi başlığın altına yazın.

PostgreSQL örneği:

```yaml
sources:
- name: erp                               # Secret, Kafka konusu ve ad alanı adları bundan türer
  type: postgres                          # postgres | sqlserver | mongodb
  host: erp-db.kurum.example.net
  port: 5432
  database: erp
  tables: [public.orders]                 # yakalanacak tabloların tam adı (şema dâhil)
  signalTable: public.debezium_signal     # zorunlu (PostgreSQL ve SQL Server)
```

SQL Server'da `type: sqlserver`, `port: 1433` ve tablolar `dbo.orders` biçiminde
yazılır.

MongoDB örneği — bağlantı adresinde kullanıcı adı ve parola **bulunmaz**; onlar yine
`erp-db` Secret'ından okunur:

```yaml
sources:
- name: erp
  type: mongodb
  connectionString: "mongodb://erp-db.kurum.example.net:27017/?replicaSet=rs0"
  collections: [erp.orders]               # veritabanı.koleksiyon biçiminde
  signalCollection: erp.debezium_signal   # isteğe bağlı
```

Bütün alanlar:

| Alan | Zorunlu | Anlamı |
|---|---|---|
| `name` | evet | kaynağın adı; Secret, Kafka konusu, yuva, publication ve ad alanları bundan türer |
| `type` | evet | `postgres`, `sqlserver` ya da `mongodb` |
| `host`, `port` | PostgreSQL, SQL Server | kaynak sunucunun adresi ve portu |
| `database` | PostgreSQL, SQL Server | veritabanının adı |
| `tables` | PostgreSQL, SQL Server | yakalanacak tablolar; sinyal tablosu bu listeye **yazılmaz** |
| `signalTable` | PostgreSQL, SQL Server | sinyal tablosunun tam adı; bu tablo Bronze'a yazılmaz |
| `connectionString` | MongoDB | bağlantı adresi; kullanıcı adı ve parola içermez |
| `collections` | MongoDB | `veritabanı.koleksiyon` biçiminde yakalanacak koleksiyonlar |
| `signalCollection` | hayır | MongoDB'de artımlı yükleme için sinyal koleksiyonu |
| `topicPrefix` | hayır | Kafka konularının ön eki; varsayılan `name` |
| `bronzeNamespace` | hayır | Bronze ad alanı; varsayılan `name` + `_raw` |
| `sinkTasks` | hayır | Bronze'a yazan görev sayısı (varsayılan 1); çok yoğun kaynaklarda artırılır |
| `initialLoad` | hayır | tablolardaki var olan verinin nasıl geleceği: `snapshot` (varsayılan) ya da `incremental` (Adım 6.1) |
| `state` | hayır | bağlayıcının durumu: `running` (varsayılan), `paused` ya da `stopped`. Kaynağı kalıcı olarak durdurmanın yolu budur; Kafka Connect'e elle gönderilen durdurma birkaç saniye içinde geri alınır ([kaynak-veya-tablo-silme.md](kaynak-veya-tablo-silme.md) §3) |
| `trustServerCertificate` | hayır | yalnız SQL Server; `true` sertifika doğrulamasını **kapatır** (deneme ortamı) |
| `extraConfig` | hayır | bağlayıcıya eklenecek ya da onu değiştirecek ayarlar |
| `sinkExtraConfig` | hayır | Bronze'a yazan bağlayıcıya eklenecek ya da onu değiştirecek ayarlar |

Dosyayı kaydedin. Göndermeyi Adım 8'de, `pipelines` girdisiyle birlikte yapacaksınız.

**Ters giderse:** Adım 8'deki denetim `signalTable zorunlu` ya da
`postgres|sqlserver|mongodb olmalı` hatası verirse ilgili alan eksik ya da yanlıştır.

### 6.1 İlk yükleme: tablolardaki var olan veri nasıl gelir

Kaynak eklendiğinde tablolarda zaten veri vardır. Lakehouse bu veriyi iki yoldan biriyle
getirir; seçim `sources` girdisindeki `initialLoad` alanıyla yapılır.

| | `snapshot` (varsayılan) | `incremental` |
|---|---|---|
| Nasıl çalışır | Bağlayıcı açılırken tabloları baştan sona okur, sonra değişiklikleri izlemeye geçer | Bağlayıcı yalnız değişiklikleri izleyerek açılır; var olan satırlar sizin göndereceğiniz bir istekle **parça parça** (1.024 satırlık parçalar) okunur |
| Yarıda kalırsa | Okuma **baştan** başlar. Veri kaybolmaz; Bronze'a o ana kadar okunan satırlar ikinci kez yazılır, Silver yine doğru olur | **Kaldığı parçadan** devam eder |
| Kaynak tabloya etkisi | SQL Server'da okuma süresince satırlar okuma kilidinde kalır | Kilit yoktur; yükleme sırasında canlı değişiklikler de akar |
| Duraklatma, iptal | yoktur | vardır (aşağıda) |
| Ne zaman seçilir | binlerce, yüz binlerce satır; okuma dakikalar sürer | milyonlarca satır, saatler sürecek yükleme ya da kilitlenmemesi gereken tablolar |

Tablolarınız küçükse bir şey yazmanız gerekmez. Büyük tablolar için kaynağın girdisine bir
satır eklenir:

```yaml
- name: erp
  type: sqlserver
  # ... diğer alanlar aynen kalır
  initialLoad: incremental
```

**SQL Server'da `snapshot` ile kilitten kaçınmak:** veritabanı yöneticisi veritabanında
*snapshot isolation*'ı açabilir (`ALTER DATABASE erp SET ALLOW_SNAPSHOT_ISOLATION ON;`)
ve kaynağın girdisine `extraConfig: {snapshot.isolation.mode: snapshot}` eklenir. Okuma
tutarlı bir kopyadan yapılır, kaynağa yazanlar beklemez.

#### Artımlı yüklemeyi başlatın

`initialLoad: incremental` ile eklenen bir kaynakta, bağlayıcı çalışmaya başladıktan
sonra (Adım 9.1) kaynağın sinyal tablosuna bir istek satırı yazılır. Birden fazla tablo
aynı satırda virgülle verilebilir.

`[kaynak DB]` — SQL Server (tablo adının başında **veritabanı adı** vardır):

```sql
INSERT INTO dbo.debezium_signal (id, type, data)
VALUES (CONVERT(varchar(42), NEWID()), 'execute-snapshot',
        '{"data-collections": ["erp.dbo.orders"], "type": "incremental"}');
```

`[kaynak DB]` — PostgreSQL:

```sql
INSERT INTO public.debezium_signal (id, type, data)
VALUES (gen_random_uuid()::text, 'execute-snapshot',
        '{"data-collections": ["public.orders"], "type": "incremental"}');
```

MongoDB'de istek kaynağın `signalCollection` koleksiyonuna yazılır
([mevcut-kaynaga-tablo-ekleme.md](mevcut-kaynaga-tablo-ekleme.md) Adım 6).

**Beklenen çıktı:** SQL Server'da `(1 rows affected)`, PostgreSQL'de `INSERT 0 1`.

#### İlerlemeyi izleyin

Bağlayıcının günlüğü yüklemenin başladığını ve bittiğini yazar:

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" logs -l strimzi.io/kind=KafkaConnect --since=1h --tail=-1 \
  | grep -E "'INCREMENTAL' snapshot|incremental snapshotting of table|Incremental snapshot"
```

Kafka'ya o ana kadar kaç satır ulaştığını görmek için (SQL Server'da konu adı kaynak
adı, veritabanı, şema ve tablodan oluşur):

`[bastion]`

```bash
scripts/kafka-cli.sh kafka-get-offsets.sh --topic erp.erp.dbo.orders \
  | awk -F: '{s+=$3} END {print s}'
```

**Beklenen çıktı** (örnek: `fin` adlı bir SQL Server kaynağında, 300.000 satırlık bir
tabloda istekten 40 saniye sonra; sayı her 1.024 satırda bir artar):

```text
2026-11-09 11:19:29 INFO  [debezium-sqlserverconnector-fin-change-event-source-coordinator] ExecuteSnapshot:64 - Requested 'INCREMENTAL' snapshot of data collections '[fin.dbo.invoices]' …
7168
```

Yükleme bitince günlükte `incremental snapshotting of table '…' finished` satırı görünür
ve sayı kaynak tablonun satır sayısına ulaşır. Bronze ve Silver'daki sayıları Adım 9.2
ve 9.3'teki sorgularla karşılaştırın (Silver bir `silver-merge` çalışmasından sonra
güncellenir).

**Örnek sonuç:** 300.000 satırlık bir SQL Server tablosu yüklenirken Kafka Connect
yeniden başlatıldı; yükleme kaldığı yerden devam etti ve sayılar tuttu:

```text
konudaki kayıt              : 300000
Bronze  count / count(distinct id) : 300000 / 300000
Silver  count / count(distinct id) : 300000 / 300000
```

#### Duraklatın, sürdürün, iptal edin

Kaynak sunucunun yoğun olduğu saatlerde yüklemeyi durdurup sonra sürdürebilirsiniz.
İstek yine sinyal tablosuna yazılır. Aşağıdaki örnek SQL Server içindir; PostgreSQL'de
`id` için `gen_random_uuid()::text` kullanın:

`[kaynak DB]`

```sql
-- duraklat
INSERT INTO dbo.debezium_signal (id, type, data) VALUES (CONVERT(varchar(42), NEWID()), 'pause-snapshot', NULL);
-- kaldığı yerden sürdür
INSERT INTO dbo.debezium_signal (id, type, data) VALUES (CONVERT(varchar(42), NEWID()), 'resume-snapshot', NULL);
-- iptal et (baştan başlatmak için önce bu, sonra yeni bir execute-snapshot isteği)
INSERT INTO dbo.debezium_signal (id, type, data)
VALUES (CONVERT(varchar(42), NEWID()), 'stop-snapshot',
        '{"data-collections": ["erp.dbo.orders"], "type": "incremental"}');
```

**Beklenen çıktı** (bağlayıcının günlüğünde; duraklatılmışken Kafka'daki sayı artmaz,
sürdürülünce artmaya devam eder):

```text
… AbstractIncrementalSnapshotChangeEventSource:257 - Incremental snapshot was paused.
… AbstractIncrementalSnapshotContext:131 - Resuming incremental snapshot
```

#### Bağlayıcı yarıda kalırsa

Bir şey yapmanız gerekmez. Kafka Connect yeniden açıldığında (pod yeniden başladı, düğüm
değişti) artımlı yükleme son tamamlanan parçadan kendiliğinden devam eder. Günlükte şu
satır görünür:

```text
… AbstractIncrementalSnapshotChangeEventSource:235 - Incremental snapshot in progress, need to read new chunk on start
```

Yarıda kalan parça yeniden okunduğu için Bronze'a birkaç yüz satır ikinci kez
yazılabilir. Silver her kayıt için yalnız bir satır tuttuğu için sonuç doğrudur.

**Baştan yüklemek** gerekirse (örneğin Bronze tablosunu sildiyseniz): önce
`stop-snapshot`, sonra yeni bir `execute-snapshot` isteği gönderin.

#### Bağlayıcı uzun süre kapalı kalırsa

Kaynak veritabanı değişiklikleri bir süre saklar; bu süre aşılırsa kaçırılan
değişiklikler bir daha okunamaz:

- **SQL Server:** CDC değişiklikleri varsayılan olarak **3 gün** saklar. Bağlayıcı daha
  uzun süre kapalı kalırsa o tablo artımlı yüklemeyle yeniden getirilir. Süreyi
  veritabanı yöneticisi uzatabilir:
  `EXEC sys.sp_cdc_change_job @job_type = N'cleanup', @retention = 10080;` (dakika
  cinsinden; bu örnekte 7 gün).
- **PostgreSQL:** tersi olur: bağlayıcı kapalıyken sunucu değişiklik günlüğünü
  **silmez** ve disk dolar. Bağlayıcının uzun süre kapalı kalacağı biliniyorsa ya da
  kaynak kaldırılacaksa çoğaltma yuvası silinir
  ([kaynak-veya-tablo-silme.md](kaynak-veya-tablo-silme.md) Adım 8).

İki durumda da `LakehouseConnectTaskFailed` alarmı erken haber verir
([izleme-ve-alarmlar.md](izleme-ve-alarmlar.md)).

---

## 7. `pipelines` girdisini yazın

`sources` girdisi Bronze'u doldurur. Silver tablosu ise **yalnız** `pipelines` listesine
yazılan tablolar için üretilir. Bir tabloyu Silver'a almak için tablonun birincil
anahtarını bilmek gerekir.

Aynı dosyada (`platform/values/site/glue.yaml`) `pipelines:` listesini bulun ve son
öğesinin altına ekleyin. Liste boşsa (`pipelines: []`) `[]` işaretini silin.

```yaml
pipelines:
- bronze: erp_raw.orders                  # Bronze tablosu; Silver adı: erp.orders
  keys: [id]                              # birincil anahtar kolon(lar)ı
  bucket_count: 16                        # bölümleme kova sayısı
  casts: {updated_at: timestamp}          # yalnız gerekirse: metin gelen zaman kolonları
```

| Alan | Varsayılan | Anlamı ve nasıl seçilir |
|---|---|---|
| `bronze` | — | Bronze tablosunun tam adı; Silver adı `_raw` atılarak bulunur |
| `keys` | — | birincil anahtar; boş olamaz. Birden fazla kolonluk anahtarda **en çok farklı değer alan kolonu başa** yazın: bölümleme yalnız ilk kolonla yapılır |
| `write_mode` | `merge-on-read` | büyük tablolarda değiştirmeyin; küçük ve çok okunan tablolarda `copy-on-write` okumayı hızlandırır |
| `bucket_count` | `16` | tablonun bölüneceği parça sayısı. Her parçaya kabaca 100–500 MB düşecek şekilde seçin: milyonun altında satırlı tablolarda 4–8, on milyonlarda 16–32 |
| `casts` | `{}` | kolonların Silver'daki tipini değiştirmek için (kolon → tip) |
| `silver` | türetilir | Silver tablo adını elle vermek için; gerekmedikçe kullanmayın |

**Kolon tipleri ve `casts`.** PostgreSQL ve SQL Server'da tarih (`date`), mantıksal
(SQL Server `bit`, PostgreSQL `boolean`) ve ondalık (`decimal`, `numeric`) kolonlar
Bronze'a aynı tiple gelir; bunlar için `casts` gerekmez. Yalnız **saat dilimli** zaman
kolonları (PostgreSQL `timestamptz`, SQL Server `datetimeoffset`) Bronze'a metin olarak
gelir. Silver'da bu kolonların zaman tipinde olmasını istiyorsanız `casts` ile
belirtin: `casts: {updated_at: timestamp}`.

**Yalnız eklenen tablolar listeye yazılmaz.** Silver, kaynak tablonun güncel hâlini
tutar. Günlük ve olay tabloları gibi satırları hiç güncellenmeyen tablolar yalnız
Bronze'da kalır ve oradan sorgulanır.

**MongoDB.** Koleksiyonlarda anahtar her zaman `keys: [_id]`'dir ve `casts` kullanılmaz:
belge `_doc` kolonunda JSON metni olarak durur (Adım 3.3). Silver da aynı `_id`, `_doc`
biçimindedir.

```yaml
pipelines:
- {bronze: erp_raw.orders, keys: [_id], bucket_count: 16}
```

**Ters giderse:** Adım 8'deki denetim `'bronze' ve boş olmayan 'keys' zorunlu` hatası
verirse anahtar yazılmamıştır. `casts bilinmeyen kolon(lar)` hatası, `casts` içinde
Bronze'da olmayan bir kolon adı olduğunu gösterir.

---

## 8. Değişikliği Git'e gönderin

Adım 5'te `setup.yaml` dosyasını, Adım 6 ve 7'de `glue.yaml` dosyasını değiştirdiniz.
İkisini birlikte gönderin. Bu adımın ayrıntısı
[değişiklik nasıl uygulanır](degisiklik-nasil-uygulanir.md) sayfasındadır.

`[bastion]`

```bash
bash scripts/check-site.sh
git diff
git add platform/values/site/glue.yaml platform/polaris/setup.yaml
git commit -m "site: erp kaynağı ve pipeline eklendi"
git push origin main
```

`check-site.sh` `check-site: OK` yazmalıdır; `git diff` çıktısında yalnız sizin
eklediğiniz satırlar görünmelidir.

ArgoCD değişikliği birkaç dakika içinde uygular ve şunları yaratır:

- `dbz-erp` ve `sink-erp` bağlayıcıları;
- Kafka Connect'in `erp-db` Secret'ını okuma izni ve kaynağın Kafka konularına yazma
  izni;
- Silver tablolarının tanımı ve — ilk pipeline'da — `silver-merge` işi ile bakım işleri.

Uygulamanın bittiğini kontrol edin:

`[bastion]`

```bash
oc -n "$ARGOCD_NS" get application glue \
  -o jsonpath='{.status.sync.status} {.status.health.status}{"\n"}'
```

**Beklenen çıktı:**

```text
Synced Healthy
```

**Ters giderse:** `Synced Degraded` görürseniz nesneler kuruldu ama bağlayıcı
sağlıksızdır; Adım 9.4'e geçin.

---

## 9. Doğrulayın

### 9.1 Bağlayıcılar

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" get kafkaconnector dbz-erp sink-erp
```

**Beklenen çıktı** (iki satırda da `READY` sütunu `True`):

```text
NAME       CLUSTER   CONNECTOR CLASS                                      MAX TASKS   READY
dbz-erp    connect   io.debezium.connector.postgresql.PostgresConnector   1           True
sink-erp   connect   org.apache.iceberg.connect.IcebergSinkConnector      1           True
```

Bağlayıcının görevinin durumu:

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" get kafkaconnector dbz-erp \
  -o jsonpath='{.status.connectorStatus.tasks[0]}{"\n"}'
```

**Beklenen çıktı** (`state` alanı `RUNNING`; `worker_id` görevin çalıştığı Kafka Connect
pod'udur):

```text
{"id":0,"state":"RUNNING","version":"3.6.2.Final","worker_id":"connect-connect-1.connect-connect.lakehouse.svc:8083"}
```

PostgreSQL kaynaklarında çoğaltma yuvasının açıldığını kaynak veritabanında da
görebilirsiniz:

`[kaynak DB]`

```sql
SELECT slot_name, plugin, active FROM pg_replication_slots;
```

**Beklenen çıktı** (yuvanın adı `debezium_` + kaynak adıdır):

```text
  slot_name   |  plugin  | active 
--------------+----------+--------
 debezium_erp | pgoutput | t
(1 row)
```

**Ters giderse:** `READY` sütunu boş ya da `False` ise Adım 9.4'teki tabloya bakın.
`active` sütunu `f` ise bağlayıcı yuvaya bağlı değildir (görevi durmuştur).

### 9.2 Bronze

Bronze'a yazan bağlayıcı verileri **beş dakikada bir** tabloya işler. İlk satırların
görünmesi bağlayıcı açıldıktan sonra **5–15 dakika** sürebilir; bu sürede tablonun boş
ya da hiç olmaması normaldir.

Sorgular bir JupyterHub not defteri hücresinde çalıştırılır. Önce bağlantıyı kurun:

`[pod]` (JupyterHub not defteri hücresi)

```python
import os, trino
conn = trino.dbapi.connect(host=os.environ["TRINO_HOST"], port=8443, http_scheme="https",
                           verify="/etc/lakehouse-ca/tls.crt",
                           auth=trino.auth.OAuth2Authentication(), catalog="lakehouse")
cur = conn.cursor(); cur.execute("select count(*) from erp_raw.orders"); cur.fetchall()
```

Hücre ilk çalıştığında bir giriş adresi yazar ve bekler. Adresi tarayıcıda açıp kendi AD
hesabınızla girin; hücre sonra kendiliğinden devam eder. Hücre `CA cert does not include
key usage extension` hatası verirse [sorun-giderme.md](sorun-giderme.md) §7'ye bakın.

**Beklenen çıktı** (kaynak tablonuzdaki satır sayısı; bu örnekte 201):

```text
[[201]]
```

Bronze bir **değişiklik günlüğüdür**: her satırın yanında değişikliğin bilgilerini
taşıyan bir `_cdc` yapısı vardır (`op`: `I` ekleme, `U` güncelleme, `D` silme; `ts`
zamanı). Kaynakta güncellenen ya da silinen satırlar Bronze'da ayrı satırlar olarak
durur; bu yüzden zamanla Bronze'daki satır sayısı kaynaktakinden **büyük** olur.
Bağlayıcının ilk okuması yarıda kalıp baştan alınırsa da bazı satırlar iki kez yazılır.
Bronze'u kaynakla karşılaştırırken bu yüzden toplam satır yerine **farklı anahtar
sayısına** bakın: `select count(distinct id) from erp_raw.orders`.

**MongoDB kaynaklarında sorgu farklıdır.** Belge `_doc` kolonunda JSON metni olarak
durduğu için (Adım 3.3) alanlara `json_extract_scalar` ile bakılır. Aşağıdaki hücre son
değişiklikleri ve bir alanın değerini gösterir:

`[pod]` (JupyterHub not defteri hücresi)

```python
cur = conn.cursor()
cur.execute("""select _id,
                      json_extract_scalar(_doc, '$.name') as name,
                      _cdc.op
               from erp_raw.orders
               order by _cdc.ts desc
               limit 3""")
cur.fetchall()
```

**Beklenen çıktı** (örnek: `name` alanı olan bir koleksiyondan; silinmiş belgelerde alan
boş gelebilir):

```text
[['000000000000000000000004', 'Deniz', 'I'], ['000000000000000000000002', None, 'D'], ['000000000000000000000001', 'Ada', 'U']]
```

MongoDB kaynağında ayrıca **karantina tablosunun boş olduğunu** kontrol edin:

`[pod]` (JupyterHub not defteri hücresi)

```python
cur = conn.cursor()
cur.execute("select reason, count(*) from erp_raw.orders__quarantine group by 1")
cur.fetchall()
```

**Beklenen çıktı** (sağlıklı bir akışta karantina boştur):

```text
[]
```

**Ters giderse:** 15 dakika sonra hâlâ satır yoksa Kafka Connect günlüğünde ilk okumanın
satırlarını arayın:

```bash
oc -n "$LAKEHOUSE_NS" logs -l strimzi.io/kind=KafkaConnect --tail=200 | grep -i snapshot
```

MongoDB'de Bronze'u `mongo-bronze` işi yazar; işin askıda olmadığını kontrol edin:
`oc -n "$LAKEHOUSE_NS" get scheduledsparkapplication mongo-bronze` (`SUSPEND` sütunu
`false` olmalıdır).

### 9.3 Silver

Silver tablosu, Bronze'a veri geldikten sonraki ilk `silver-merge` çalışmasında yaratılır.
İş 15 dakikada bir çalışır. Durumunu görmek için:

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" get scheduledsparkapplication silver-merge
```

**Beklenen çıktı** (`SUSPEND` sütunu `false`; `LAST RUN` son çalışmadan bu yana geçen
süre):

```text
NAME           SCHEDULE       TIMEZONE   SUSPEND   LAST RUN   LAST RUN NAME                      AGE
silver-merge   */15 * * * *              false     6m41s      silver-merge-1794415500000843808   3d2h
```

Çalışma bittikten sonra Silver'ı sorgulayın:

`[pod]` (JupyterHub not defteri hücresi)

```python
cur = conn.cursor(); cur.execute("select count(*) from erp.orders"); cur.fetchall()
```

**Beklenen çıktı** (kaynak tablodaki **güncel** satır sayısı; bu örnekte 201):

```text
[[201]]
```

**Ters giderse:**

- Tablo bulunamadı hatası — `silver-merge` henüz çalışmamıştır ya da `pipelines` girdisi
  eksiktir.
- `SUSPEND` sütunu `true` — iş askıdadır. Kabul testi çalışırken işleri askıya alır ve
  bitince geri açar; test yarıda kesilirse askıda kalırlar. ArgoCD bunu düzeltmez; geri
  açmak için [sorun-giderme.md](sorun-giderme.md) §6.1. Açıldıktan sonra kaçırılan
  çalışma hemen yapılır.

### 9.4 Sık karşılaşılan hatalar

| Ne görüyorsunuz | Neden | Ne yapılır |
|---|---|---|
| `dbz-erp` hata veriyor: `password authentication failed for user "debezium"` (SQL Server'da `Login failed`, MongoDB'de `Authentication failed`) | kaynaktaki kullanıcı parolasız ya da başka bir parolayla yaratılmış (Adım 3'ün başı atlanmış) ya da `erp-db` Secret'ındaki değer farklı | parolayı kaynakta yenileyin (`ALTER ROLE debezium WITH PASSWORD :'parola';`) ve `erp-db` Secret'ını aynı değerle yeniden yaratın (Adım 4) |
| `dbz-erp` hata veriyor: `must be superuser or replication role` | kullanıcının çoğaltma yetkisi yok | Adım 3.1.2'deki `CREATE ROLE ... REPLICATION` |
| `dbz-erp` hata veriyor: `replication slot ... already exists and is active` | aynı adlı yuvayı başka bir bağlayıcı kullanıyor | eski kaynağı kaldırın ya da kaynağa başka bir ad verin |
| `dbz-erp` hata veriyor ve hata metninde `dbz_erp_pub` geçiyor | publication yok ya da yakalanan bir tablo publication'da değil | Adım 3.1.2'deki `CREATE PUBLICATION` / `ALTER PUBLICATION ... ADD TABLE` |
| `dbz-erp` hata veriyor: sertifika hatası (SQL Server) | sunucunun sertifikasına güvenilmiyor | truststore'u `extraConfig` ile verin (Adım 3.2) |
| `dbz-erp` hata veriyor: `not running with --replSet` (MongoDB) | kaynak bir replica set değil | Adım 3.3 |
| Bağlayıcılar `True` ama Bronze boş | ilk yazma henüz olmadı | 15 dakika bekleyin; sonra Kafka Connect günlüğüne bakın (Adım 9.2) |
| `sink-erp` hata veriyor: ad alanı bulunamadı ya da katalog yetki hatası | Bronze ad alanı katalogda yok | Adım 5 |
| `sink-erp` hata veriyor: `TopicAuthorizationException` | Kafka izni kaynağın konularını kapsamıyor | `topicPrefix` alanı ile Kafka konularının ön eki aynı mı, kontrol edin |
| Bronze doluyor, Silver güncellenmiyor | `silver-merge` askıda ya da hata veriyor | `SUSPEND` sütunu (Adım 9.3); `LakehouseSilverMergeStale` alarmı |
| Silver'da `SchemaConflict` | bir kolonun tipi Silver'da güvenle genişletilemiyor | [mevcut-kaynaga-tablo-ekleme.md](mevcut-kaynaga-tablo-ekleme.md) |

Bağlayıcıyı yeniden başlatmak (nesneyi silmez):

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" annotate kafkaconnector dbz-erp strimzi.io/restart=true --overwrite
```

Hatanın ayrıntısını okumak:

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" get kafkaconnector dbz-erp \
  -o jsonpath='{.status.connectorStatus.tasks[0].trace}{"\n"}' | head -30
```

**Hatalı kayıtlar hakkında:** Bronze'a yazan bağlayıcı, okuyamadığı ya da
dönüştüremediği kaydı adı `.dlq` ile biten konuya (ör. `erp.dlq`) bırakır ve devam eder.
Tabloya yazma sırasında oluşan bir hata ise görevi durdurur ve alarmla görünür
([sorun-giderme.md](sorun-giderme.md) §4.1).

Bütün belirtiler ve çözümleri: [sorun-giderme.md](sorun-giderme.md) §3.

---

## 10. nginx erişim günlüğü akışı

Bu akış bir veritabanı değildir. Web sunucusunda çalışan **Fluent Bit 5.1.2** ajanı nginx
erişim günlüğünü doğrudan Kafka'ya gönderir; bir bağlayıcı da satırları
`nginx_raw.access_log` tablosuna yazar. `sources` listesine bir şey eklenmez.

```text
access.log -> Fluent Bit -> Kafka (dış adres, TLS + parola) -> bağlayıcı -> nginx_raw.access_log
```

**Birden fazla nginx sunucusu** varsa hepsi aynı tabloya yazar; satırın hangi sunucudan
geldiği `server` kolonundadır. Sunucu başına ayrı tablo açılmaz; sorgular
`where server = '…'` ile süzülür. Yeni bir sunucu eklemek kümede bir değişiklik
gerektirmez (Adım 10.5). Günlük satırları yalnız eklenir, hiç güncellenmez; bu yüzden
Silver katmanı yoktur, raporlar doğrudan bu tablodan üretilir.

Tablo günlere bölünür (`ts` kolonuna göre). `ts` günlük satırının kendi zamanı, `_fb_ts`
ajanın satırı okuduğu zamandır. İstemci IP adresi (`remote` kolonu) olduğu gibi saklanır;
kimin görebileceği Trino erişim kurallarıyla sınırlanır
([kullanici-ve-yetki.md](kullanici-ve-yetki.md)).

### 10.1 Küme tarafını açın

`platform/values/site/glue.yaml` dosyasında iki satır gerekir. `nginx:` satırı dosyada
zaten vardır; `enabled` değerini `true` yapın. `kafka:` satırı yoksa `nginx:` satırının
hemen üstüne ekleyin:

```yaml
kafka: {externalListener: true}
nginx: {enabled: true, partitions: 3}
```

İlk satır Kafka'yı küme dışından (ajanlardan) erişilebilir yapar. Bu değişiklik Kafka
sunucularını sırayla yeniden başlatır (2–3 dakika); bu sırada veri akışı durmaz.

Değişikliği Adım 8'deki gibi gönderin. ArgoCD şunları yaratır: `nginx.access` Kafka
konusu, ajanların kullanacağı `fluentbit` Kafka kullanıcısı (yalnız `nginx.` ile başlayan
konulara yazabilir), `sink-nginx` bağlayıcısı ve Kafka'nın dış adresleri.

**Ön koşul:** ajan sunucularından kümeye `443/TCP` açık olmalı ve `$APPS_DOMAIN` altındaki
adlar çözülebilmelidir ([20-on-kosullar](../20-on-kosullar.md) §8).

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" get kafkatopic nginx.access
```

**Beklenen çıktı:**

```text
NAME           CLUSTER     PARTITIONS   REPLICATION FACTOR   READY
nginx.access   lakehouse   3            3                    True
```

**Ters giderse:** konu görünmüyorsa `nginx.enabled` hâlâ `false`'tur ya da ArgoCD henüz
uygulamamıştır.

### 10.2 Ajanın ihtiyaç duyduğu üç değeri alın

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" get kafka lakehouse \
  -o jsonpath='{.status.listeners[?(@.name=="external")].bootstrapServers}{"\n"}'
oc -n "$LAKEHOUSE_NS" get secret lakehouse-cluster-ca-cert \
  -o jsonpath='{.data.ca\.crt}' | base64 -d > kafka-ca.crt
oc -n "$LAKEHOUSE_NS" get secret fluentbit -o jsonpath='{.data.password}' | base64 -d; echo
```

**Beklenen çıktı** (adres ve parola sizde farklıdır): üç komut ekrana **iki** satır
yazar. Birinci satır Kafka'nın dış adresidir, ikinci satır ajanın parolasıdır. Ortadaki
komut ekrana bir şey yazmaz; Kafka'nın sertifikasını `kafka-ca.crt` dosyasına kaydeder
(`ls -l kafka-ca.crt` ile kontrol edin).

```text
lakehouse-kafka-bootstrap-lakehouse.apps.ocp.example.net:443
4f6b2c1d9e8a7350b1c4d2e6f0a9837c
```

Bu sertifika Kafka'nın kendi sertifikasıdır; kurulumdaki `lakehouse-ca` ile
karıştırmayın.

Üç değeri (adres, `kafka-ca.crt` dosyası, parola) ajan sunucusuna güvenli bir kanalla
taşıyın. Ajan sunucusu `$APPS_DOMAIN` altındaki **dört** adı çözebilmelidir: dış adres
ve her Kafka sunucusu için birer ad. Adları görmek için:

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" get route | grep -E 'bootstrap|dual-role' | awk '{print $1}'
```

**Beklenen çıktı:**

```text
lakehouse-dual-role-0
lakehouse-dual-role-1
lakehouse-dual-role-2
lakehouse-kafka-bootstrap
```

**Ters giderse:** dış adres satırı boşsa Kafka'nın dış adresi açılmamıştır (Adım 10.1).

### 10.3 Ajanı kurun

Web sunucusuna Fluent Bit 5.1.2 paketini kurun. Depodaki iki ayar dosyasını
(`agents/fluent-bit/fluent-bit.conf` ve `agents/fluent-bit/parsers.conf`) ve Adım
10.2'deki `kafka-ca.crt` dosyasını sunucuya kopyalayın.

Aşağıdaki komutları sunucuda, bu üç dosyanın bulunduğu klasörde çalıştırın:

`[nginx ajan sunucusu]`

```bash
read -r KAFKA_BOOTSTRAP
read -r -s KAFKA_PASSWORD
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

İlk iki komuttan sonra sırasıyla Adım 10.2'deki adresi ve parolayı yazıp `Enter`'a basın
(parola ekranda görünmez).

- `NGINX_SERVER_NAME` bu sunucunun tablodaki adıdır (`server` kolonu). Komut sunucunun
  kısa adını yazar; isterseniz `web01` gibi kendi adınızı verin. Her sunucuda **farklı**
  olmalıdır.
- `READ_FROM_HEAD=off`, ajanın yalnız **yeni** satırları göndermesini sağlar. Dosyadaki
  eski satırları da göndermek isterseniz ilk açılışta `on` yapın.
- Dosya parola içerdiği için yalnız `root` okuyabilir (`chmod 600`).

Ajanın çalıştığını kontrol edin:

```bash
systemctl status fluent-bit
```

**Beklenen çıktı** (örnek — ilk satırlar):

```text
● fluent-bit.service - Fluent Bit
     Active: active (running)
```

**Ters giderse:** `journalctl -u fluent-bit` çıktısına bakın:

- yetki hatası — parola yanlıştır;
- sertifika hatası — `kafka-ca.crt` dosyası yanlış yerde ya da bozuktur;
- dosya bulunamadı — `NGINX_ACCESS_LOG` yolu yanlıştır.

### 10.4 Doğrulayın

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" get kafkaconnector sink-nginx
```

**Beklenen çıktı:**

```text
NAME         CLUSTER   CONNECTOR CLASS                                   MAX TASKS   READY
sink-nginx   connect   org.apache.iceberg.connect.IcebergSinkConnector   1           True
```

Satırlar 5–15 dakika içinde `nginx_raw.access_log` tablosuna düşer. Sorgu Adım 9.2'deki
not defteri hücresiyle aynıdır; yalnız tablo adını değiştirin.

**Bilinmesi gerekenler:**

- **Saklama süresi.** Günlük tablosu varsayılan olarak süresiz büyür. Kurumun saklama
  kuralına göre `nginx` girdisine `retentionDays: 365` gibi bir değer yazarsanız her gece
  bu süreden eski satırlar silinir.
- **Bağlantı kesilirse.** Ajan Kafka'ya ulaşamazsa satırları sunucunun diskinde 1 GB'a
  kadar biriktirir ve bağlantı gelince gönderir.
- **Günlük biçimi.** Ajan nginx'in `combined` biçimini ve nginx'in varsayılan `main`
  biçimini (sonunda `"$http_x_forwarded_for"` olan) tanır. Kurumunuz farklı bir
  `log_format` kullanıyorsa `parsers.conf` dosyasındaki ifade buna göre güncellenir.
- **Proxy arkasındaki sunucular.** `remote` kolonu proxy'nin IP adresidir. Gerçek
  istemci adresi için nginx'te `real_ip` modülü ya da `X-Forwarded-For` alanını yazan bir
  `log_format` kullanılır; o durumda `parsers.conf` da güncellenir.
- **Ayrıştırılamayan satırlar kaybolmaz.** Biçime uymayan bir satır tabloya yine yazılır:
  `remote`, `path`, `code` gibi kolonlar boş kalır ve satırın tamamı `log` kolonunda durur.
  Bu satırları şöyle bulursunuz (Adım 9.2'deki not defteri hücresiyle):

  ```python
  cur = conn.cursor()
  cur.execute("select ts, log from nginx_raw.access_log where remote is null limit 5")
  cur.fetchall()
  ```

  Satır görürseniz `parsers.conf` dosyasındaki ifadeyi o satırın biçimine uydurun ve
  ajanı yeniden başlatın; yeni satırlar doğru kolonlara yazılır.
- **`nginx.dlq` konusu.** Bağlayıcının hiç okuyamadığı kayıtlar (JSON olmayan bir ileti
  gibi) bu konuya düşer; ajan doğru kurulduğunda boş kalır. Kayıt düşerse
  `LakehouseDlqRecords` alarmı gelir.

**Ters giderse:** `sink-nginx` `True` görünüyor ama tablo boşsa ajan Kafka'ya
yazamıyordur. Ajan sunucusunda `journalctl -u fluent-bit` çıktısına bakın ve parolanın
`fluentbit` Secret'ındakiyle aynı olduğunu kontrol edin.

### 10.5 Yeni bir nginx sunucusu eklemek

Kümede bir şey değişmez: yeni sunucuya Adım 10.3 aynen uygulanır, yalnız
`NGINX_SERVER_NAME` o sunucunun adı olur. Adres, `kafka-ca.crt` ve parola bütün
sunucularda aynıdır.

Birkaç dakika sonra yeni sunucunun satırları aynı tabloda görünür:

`[pod]` (JupyterHub not defteri hücresi)

```python
cur = conn.cursor()
cur.execute("select server, count(*) from nginx_raw.access_log group by server order by 1")
cur.fetchall()
```

**Beklenen çıktı** (iki sunucuyla — `web01` ve `web02`; her sunucu için bir satır ve o
sunucudan gelen istek sayısı):

```text
[['web01', 3], ['web02', 2]]
```

**Ters giderse:** yeni sunucu listede yoksa o sunucuda `journalctl -u fluent-bit`
çıktısına bakın (Adım 10.3). İki sunucu aynı adla görünüyorsa `NGINX_SERVER_NAME`
değerleri aynıdır; birini değiştirip ajanı yeniden başlatın
(`systemctl restart fluent-bit`).

### 10.6 Özet tablolar ve Web trafiği panosu

`nginx_raw.access_log` her isteğin ayrı bir satır olduğu ham günlüktür. Raporlama ve uzun
dönem eğilim için ürünle hazır bir Gold işi ve bir Superset panosu gelir. İş saatte bir
çalışır ve üç tablo üretir:

| Tablo | Ne tutar |
|---|---|
| `gold.nginx_saatlik` | saat ve sunucu bazında istek sayısı, 2xx/3xx/4xx/5xx sayıları, tekil IP, aktarılan bayt, bot istekleri |
| `gold.nginx_ip_gunluk` | gün ve IP bazında istek, 4xx/5xx, başarısız giriş (401), yasak (403), bulunamadı (404), farklı sayfa sayısı, ilk ve son görülme |
| `gold.nginx_sayfa_gunluk` | gün ve sayfa bazında istek ve hata; adresteki sayılar `{id}` ile gruplanır (`/ogrenci/123` → `/ogrenci/{id}`) |

Gün ve saat Türkiye saatiyledir. Tablolar güne göre bölümlenir; ilk koşu bütün geçmişi
hesaplar, sonraki koşular son üç günü yeniden hesaplar.

Buradaki "hata" HTTP durum kodudur: 4xx istemci hatası (yanlış parola 401, yetkisiz erişim
403, olmayan sayfa 404), 5xx sunucu hatasıdır. nginx'in kendi uyarı ve hata kayıtları
(`[warn]`, `[error]`) ayrı bir dosyada (`error.log`) tutulur ve bu tablolara girmez.

**IP adresi kişisel veridir.** `gold.nginx_ip_gunluk` tablosunun `ip` kolonu, ham tablodaki
`remote` kolonu gibi `lakehouse-users` grubuna maskeli (`x.x.x.x`) gösterilir; kural
ürünün Trino kurallarında hazırdır. Kendi kurallarınızı `platform/values/site/trino.yaml`
dosyasına yazdıysanız ([kullanici-ve-yetki.md](kullanici-ve-yetki.md) §5) şu satırı da
oraya ekleyin:

```json
{"group": "lakehouse-users", "schema": "gold", "table": "nginx_ip_gunluk", "privileges": ["SELECT"],
 "columns": [{"name": "ip", "mask": "'x.x.x.x'"}]},
```

**1) İşi `custom/` klasörüne koyun.** Kodu örnekler klasöründen kopyalayın ve zamanlı işin
tanım dosyasını üretin (her saatin 5. dakikası):

`[bastion]`

```bash
cd ~/lakehouse
git pull
cp custom/examples/gold_nginx.py custom/
scripts/spark-is-tanimi.sh --ad gold-nginx --dosya gold_nginx.py --zaman "5 * * * *" > custom/gold-nginx.yaml
vi custom/kustomization.yaml
```

Dosyada `resources:` ve `configMapGenerator:` satırlarını bulun. Liste boşsa
(`resources: []`) satırı aşağıdaki blokla değiştirin; listede başka işler varsa yalnız
`- gold-nginx.yaml` satırını `resources:` listesine, `- {name: gold-nginx, …}` satırını
`configMapGenerator:` listesine ekleyin:

```yaml
resources:
- gold-nginx.yaml
configMapGenerator:
- {name: gold-nginx, files: [gold_nginx.py], options: {disableNameSuffixHash: true}}
```

**2) Gönderin.**

```bash
git add custom/gold_nginx.py custom/gold-nginx.yaml custom/kustomization.yaml
git commit -m "custom: nginx özet tabloları (gold-nginx)"
git push origin main
oc -n "$ARGOCD_NS" annotate application custom argocd.argoproj.io/refresh=normal --overwrite
```

Bir dakika sonra:

```bash
oc -n "$LAKEHOUSE_NS" get scheduledsparkapplication gold-nginx
```

**Beklenen çıktı:**

```text
NAME         SCHEDULE    TIMEZONE   SUSPEND   LAST RUN   LAST RUN NAME   AGE
gold-nginx   5 * * * *                                                   40s
```

**3) İlk koşu.** İş saatin 5. dakikasında kendiliğinden çalışır. Beklemek istemezseniz
aynı işi bir kez elle çalıştırın:

```bash
scripts/spark-is-tanimi.sh --ad gold-nginx-ilk --dosya gold_nginx.py --configmap gold-nginx > /tmp/gold-nginx-ilk.yaml
oc apply -f /tmp/gold-nginx-ilk.yaml
oc -n "$LAKEHOUSE_NS" get sparkapplication gold-nginx-ilk -w
```

Durum `COMPLETED` olunca Ctrl+C ile çıkın ve sonucu okuyun:

```bash
oc -n "$LAKEHOUSE_NS" logs gold-nginx-ilk-driver | grep GOLD_OK
oc -n "$LAKEHOUSE_NS" delete sparkapplication gold-nginx-ilk
```

**Beklenen çıktı** (sayılar sizin trafiğinize göre değişir):

```text
GOLD_OK nginx_saatlik 340 satır (tüm geçmiş)
GOLD_OK nginx_ip_gunluk 1241 satır (tüm geçmiş)
GOLD_OK nginx_sayfa_gunluk 183 satır (tüm geçmiş)
```

**4) Panoyu içe aktarın.**

1. Superset'e `lakehouse-admins` grubundaki bir AD hesabıyla girin.
2. Üst menüden **Dashboards**'a gidin. Sağ üstteki içe aktarma simgesine (**Import
   dashboards**) tıklayın.
3. Deponuzdaki `custom/examples/superset/web-trafigi-panosu.zip` dosyasını seçin ve
   **Import**'a basın. Superset `lakehouse` veritabanının üzerine yazmak isteyip
   istemediğinizi sorarsa onaylayın; paketteki bağlantı ürünün kendi Trino bağlantısıdır.
4. Listeden **Web trafiği (nginx)** panosunu açın.

| Grafik | Ne gösterir |
|---|---|
| Web — toplam istek, Web — hata oranı | bütün dönemin istek sayısı ve 4xx + 5xx payı |
| Web — saatlik istek, durum sınıfına göre | her saatin istekleri, 2xx/3xx/4xx/5xx olarak yığılmış |
| Web — sunucu bazında saatlik istek | her nginx sunucusunun yükü |
| Web — şüpheli IP'ler | en çok 4xx alan on IP: çok 404 alan genelde açık arayan bir tarama aracıdır, çok 401 alan parola denemesidir |
| Web — en çok hata veren sayfalar | kırık bağlantılar ve hata veren uygulama sayfaları |

Pano Superset'in servis hesabıyla sorgular; IP maskesi panoda değil, kullanıcının
Trino'ya kendi kimliğiyle bağlandığı yerlerde uygulanır ([kullanici-ve-yetki.md](kullanici-ve-yetki.md)
§7). Panoyu `lakehouse-users` grubuna açacaksanız şüpheli IP grafiğini panodan çıkarın ya
da o grafiği yalnız yöneticilere gösterin.

**Saklama.** Özet tablolar ham günlükten çok küçüktür ve süresiz tutulabilir. Ham tabloyu
kurumun saklama kuralına göre sınırlamak (`nginx` girdisinde `retentionDays`, §10.4)
özetleri etkilemez: silinen ham günlerin özetleri Gold'da kalır.

**Ters giderse:**

- `GOLD_OK` satırları yok, günlükte `Table does not exist: nginx_raw.access_log` var →
  nginx akışı henüz veri yazmamıştır (§10.4).
- Panoda grafikler boş → Gold tabloları henüz yoktur; ilk koşuyu bekleyin ya da 3. adımı
  uygulayın.

---

## Kontrol listesi

- [ ] Kaynak veritabanında CDC hazırlandı (PostgreSQL'de `wal_level = logical`, SQL
      Server'da CDC açık), kullanıcı ve yetkiler verildi.
- [ ] Sinyal tablosu (PostgreSQL, SQL Server) kaynakta var; PostgreSQL'de publication'da.
- [ ] `erp-db` Secret'ı kümede ve etiketli; parola Git'e girmedi, değişkenler silindi.
- [ ] `platform/polaris/setup.yaml` içinde iki yeni ad alanı var ve betik çalıştırıldı.
- [ ] `sources` ve `pipelines` girdileri `platform/values/site/glue.yaml` dosyasında.
- [ ] Değişiklik gönderildi; `glue` uygulaması `Synced Healthy`.
- [ ] `dbz-` ve `sink-` bağlayıcılarının ikisi de `READY True`.
- [ ] Bronze tablosunda satır var; bir `silver-merge` çalışmasından sonra Silver tablosu
      var ve satır sayısı kaynakla aynı.
- [ ] (nginx kullanılıyorsa) Kafka dış adresi açık, ajan her sunucuda kendi
      `NGINX_SERVER_NAME` değeriyle çalışıyor, `sink-nginx` `READY True`.
- [ ] (nginx özetleri isteniyorsa) `gold-nginx` işi `custom/` klasöründe, ilk koşu `GOLD_OK`
      yazdı; **Web trafiği (nginx)** panosu içe aktarıldı (§10.6).

## Sonraki bölüm

- [mevcut-kaynaga-tablo-ekleme.md](mevcut-kaynaga-tablo-ekleme.md) — var olan bir kaynağa
  tablo eklemek ve geçmiş satırları artımlı yüklemeyle getirmek.
- [json-akisi-ekleme.md](json-akisi-ekleme.md) — uygulamalardan ya da IoT cihazlarından
  JSON olay akışı eklemek.
- [değişiklik nasıl uygulanır](degisiklik-nasil-uygulanir.md) — değişikliğin Git'ten
  kümeye yolu.
