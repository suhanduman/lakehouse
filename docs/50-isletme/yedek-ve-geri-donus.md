# Yedek ve geri dönüş

Bu sayfa neyin yedeklendiğini ve neyin bilerek yedeklenmediğini, yedeklerin ne zaman
alındığını ve ne kadar saklandığını anlatır. Ardından haftalık yedek kontrolünü,
veritabanını geçmiş bir zamana döndürmeyi, ad alanını ve diskleri geri yüklemeyi ve
ad alanı yedeğinin OpenShift'te nasıl açıldığını adım adım gösterir.

**Ne kadar sürer:** haftalık kontrol 5 dakika; veritabanı geri dönüş denemesi 20–30
dakika; tam ad alanı geri yükleme denemesi yarım gün.

**Hangi yetkiler gerekir:** `$LAKEHOUSE_NS` ve yedek ad alanında (`openshift-adp`)
okuma; geri yükleme denemesi sırasında bu ad alanlarında nesne yaratma ve silme; Git
deposunda `main` dalına yazma.

**Komutlar nerede çalışır:** `[bastion]` — `oc login` ile kümeye bağlı yönetim makinesi.

> **İki ayrı yedek vardır ve biri diğerinin yerini tutmaz.** PostgreSQL veritabanları
> (katalog, Keycloak, Superset) **sürekli yedekle** korunur ve geçmiş bir zamana
> döndürülebilir. Kubernetes nesneleri ve not defteri diskleri **ad alanı yedeğiyle**
> korunur.

---

## 1. Değişkenleri yükleyin

`[bastion]`

```bash
cd ~/lakehouse
set -a; . install/lakehouse.env; set +a
echo "lakehouse=$LAKEHOUSE_NS yedek kovasi=$S3_BUCKET_BACKUP"
```

**Beklenen çıktı** (kendi değerlerinizle):

```text
lakehouse=lakehouse yedek kovasi=lakehouse-backups
```

**Ters giderse:** eşittir işaretinden sonrası boşsa `install/lakehouse.env` dosyası
doldurulmamıştır ([30-kurulum](../30-kurulum.md) §1).

---

## 2. Ne yedekleniyor, ne yedeklenmiyor

| Veri | Yedek var mı | Nasıl | Nasıl geri döner |
|---|---|---|---|
| **Katalog veritabanı** (`polaris-db`) | var | sürekli yedek + günlük tam yedek | geçmiş bir zamana dönüş (§5) |
| **Keycloak** (`keycloak-db`) | var | aynı | aynı (realm notu: §5.4) |
| **Superset** (`superset-db`: panolar, grafikler, bağlantılar) | var | aynı | aynı |
| **Kubernetes nesneleri** (`$LAKEHOUSE_NS` ad alanındaki her şey) | var | günlük ad alanı yedeği | ad alanı geri yükleme (§6.1) |
| **Not defteri diskleri** (JupyterHub, Zeppelin) | var | ad alanı yedeğinin disk yedeği | disk geri yükleme (§6.3) |
| **PostgreSQL diskleri** | **bilerek yok** | disk yedeğinden çıkarıldı | geçmiş bir zamana dönüş (§5) — tutarlı tek yol budur |
| **Kafka verisi** | **bilerek yok** | disk yedeğinden çıkarıldı | kaynaklardan yeniden okuma (§2.1) |
| **Iceberg tabloları** (S3'teki dosyalar) | **bu ürünün kapsamında değil** | — | kurumun S3 depolamasının kendi çoğaltması |

> **Iceberg tabloları için önemli.** Katalog veritabanını geri yüklemek tabloları geri
> getirmez; tabloların dosyaları S3'tedir. Tersi de geçerlidir: S3 duruyor ama katalog
> kaybolmuşsa tablolar "yok" görünür. İkisi **aynı zamana** döndürülmelidir. S3
> çoğaltması gecikmeliyse, dönüş zamanını S3'ün tutarlı olduğu ana göre seçin.

Bir geri dönüşten sonra kümedeki nesnelerin bir kısmı kendiliğinden geri gelir, bir
kısmı gelmez:

- **Git'teki nesneler** (ArgoCD'nin kurduğu her şey) Git'ten yeniden kurulur; yedekten
  gelmeleri gerekmez.
- **Elle yaratılan Secret'lar** (S3 anahtarları, kaynak veritabanı parolaları,
  sertifikalar …) Git'te yoktur. Bunlar ad alanı yedeğinden (§6.2) ya da yaratıldıkları
  belge adımı tekrarlanarak gelir.

Hangi Secret'ın elle yaratıldığını ve hangi belgenin hangi adımında yaratıldığını şu
komut gösterir ([değişiklik nasıl uygulanır](degisiklik-nasil-uygulanir.md) §7):

```bash
scripts/nesne-kaynagi.sh --liste secret | grep ELLE
```

### 2.1 Kafka verisi neden yedeklenmiyor

Kafka bu üründe veriyi bir yerden bir yere **taşır**; verinin asıl yeri kaynak
veritabanları ve S3'teki Iceberg tablolarıdır. Kafka'nın verisi kaybolursa doğru yol,
kaynaktan veriyi **yeniden okumaktır**
([mevcut-kaynaga-tablo-ekleme.md](mevcut-kaynaga-tablo-ekleme.md)). Bu, eski bir
yedekten dönen konumlarla çalışmaktan daha güvenlidir. İkinci bir kümeye sürekli
çoğaltma bu ürünün kapsamında değildir; o bir yedekleme aracı değil, ayrı bir mimari
karardır
([90-referans/pre-ship-kontrol-listesi.md](../90-referans/pre-ship-kontrol-listesi.md)
§5.2).

---

## 3. Ne zaman alınır, ne kadar saklanır, nereye yazılır

| | PostgreSQL sürekli yedeği | Ad alanı yedeği |
|---|---|---|
| Zaman | her gün 02:00 (değer: `"0 0 2 * * *"` — **altı alanlı**, ilk alan saniye) | her gün 03:00 (değer: `"0 3 * * *"` — beş alanlı) |
| Ayar | `backup.schedule` | `velero.schedule` |
| Saklama | 30 gün (`backup.retentionPolicy: 30d`) | 30 gün (`velero.ttl: 720h`) |
| Nereye | `$S3_BUCKET_BACKUP` kovasında `cnpg/` klasörü | aynı kovada `velero/` klasörü |
| Kimlik | `backup-s3-creds` Secret'ı | OADP'nin `cloud-credentials` Secret'ı |
| İlk yedek | kurulumla hemen | ilk zamanlanmış saatte |
| Kümedeki nesneler | `ScheduledBackup/{polaris,keycloak,superset}-db-daily` | `Schedule/lakehouse-daily` |

**Yedek kovası veri kovasından ayrı olmalıdır.** Aynı kovada tutulursa veri kovasını
kaybettiğinizde yedeği de kaybedersiniz ([30-kurulum](../30-kurulum.md) §5.3).

**Saati ya da saklama süresini değiştirmek için** `platform/values/site/glue.yaml`
dosyasındaki `backup:` bloğuna `schedule` ya da `retentionPolicy` satırı eklenir; örnek:

```yaml
backup:
  schedule: "0 30 1 * * *"      # her gün 01:30
  retentionPolicy: "60d"
```

Bütün ayarlar ve biçim kuralları:
[90-referans/values-anahtarlari.md](../90-referans/values-anahtarlari.md) §1.
Değişikliğin Git'e gönderilmesi:
[değişiklik nasıl uygulanır](degisiklik-nasil-uygulanir.md).

**İki yedek aynı kovayı paylaşıyorsa** (varsayılan) her biri yalnız kendi klasörünü
yönetir; biri diğerinin dosyalarına dokunmaz. Kovada otomatik silme kuralı (yaşam
döngüsü kuralı) tanımlayacaksanız iki klasörü **ayrı ayrı** ele alın. Aksi hâlde kural,
PostgreSQL yedeğinin hâlâ ihtiyaç duyduğu dosyaları silip geri dönüşü imkânsız hâle
getirebilir. En güvenlisi yedekler için ayrı bir kova ve ayrı bir hesaptır.

---

## 4. Haftalık yedek kontrolü

Yedekler için alarm yoktur; durumları haftada bir elle kontrol edilir. Aynı kontrol her
yükseltmeden önce de yapılır ([yukseltme.md](yukseltme.md) §2).

**PostgreSQL yedekleri:**

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" get cluster \
  -o custom-columns='DB:.metadata.name,ARSIV:.status.conditions[?(@.type=="ContinuousArchiving")].status'
oc -n "$LAKEHOUSE_NS" get backups.postgresql.cnpg.io --sort-by=.metadata.creationTimestamp \
  -o custom-columns='AD:.metadata.name,FAZ:.status.phase,YONTEM:.status.method' | tail -3
oc -n "$LAKEHOUSE_NS" get scheduledbackup
```

**Beklenen çıktı** (OpenShift kurulumundan; tarihler sizde farklıdır):

```text
DB            ARSIV
keycloak-db   True
polaris-db    True
superset-db   True
polaris-db-daily-20261111020000    completed   plugin
keycloak-db-daily-20261111020000   completed   plugin
superset-db-daily-20261111020000   completed   plugin
NAME                AGE    CLUSTER       LAST BACKUP
keycloak-db-daily   3d5h   keycloak-db   15h
polaris-db-daily    3d5h   polaris-db    15h
superset-db-daily   3d5h   superset-db   15h
```

Üç veritabanında da `ARSIV` sütunu `True`, son yedekler `completed` ve `LAST BACKUP`
sütunu bir günden yeni olmalıdır.

**Ad alanı yedeği:** komutlarda tam ad (`backups.velero.io`) yazılmalıdır. Kısa ad
`backup` PostgreSQL'in yedek nesnesine gider ve ad alanı yedeği "yok" görünür.

`[bastion]`

```bash
oc -n openshift-adp get backups.velero.io \
  -o custom-columns='AD:.metadata.name,FAZ:.status.phase,OGE:.status.progress.itemsBackedUp,BITIS:.status.completionTimestamp'
oc -n openshift-adp get backupstoragelocation
oc -n openshift-adp get schedule lakehouse-daily
```

**Beklenen çıktı** (örnek):

```text
AD                               FAZ         OGE   BITIS
lakehouse-daily-20261109030012   Completed   462   2026-11-09T03:02:36Z
lakehouse-daily-20261110030008   Completed   470   2026-11-10T03:02:41Z
lakehouse-daily-20261111030010   Completed   752   2026-11-11T03:02:33Z
NAME              PHASE       LAST VALIDATED   AGE     DEFAULT
lakehouse-dpa-1   Available   27s              5d16h   true
NAME              STATUS    SCHEDULE    LASTBACKUP   AGE     PAUSED
lakehouse-daily   Enabled   0 3 * * *   3h36m        5d16h
```

**Ters giderse:**

- `ARSIV` sütunu `False`: veritabanı yedeğini S3'e yazamıyordur
  ([sorun-giderme.md](sorun-giderme.md) §8).
- Bir yedek `failed`: `oc -n "$LAKEHOUSE_NS" describe backups.postgresql.cnpg.io` ve
  ardından yedeğin adı yazılarak çalıştırılan komutun son satırları nedeni gösterir.
  Bir sonraki günlük yedek başarılıysa sürekli yedek kesilmemiştir.
- Yedek konumu `Available` değil: kova adı, kimlik ya da S3 adresi yanlıştır (§7.1).
- `schedule lakehouse-daily` bulunamıyor: ad alanı yedeği açılmamıştır (§7).

---

## 5. PostgreSQL'i geçmiş bir zamana döndürmek

Çalışan veritabanına dokunulmaz. Yedekten **ayrı** bir veritabanı kurulur, içeriği
kontrol edilir; gerekiyorsa uygulama sonra ona yönlendirilir (§5.3). Hazır tanım dosyası
depodadır: `test/e2e/cnpg-restore.yaml` (katalog veritabanı `polaris-db` için).

### 5.1 Son yedeğe dönüş

`[bastion]`

```bash
oc apply -f test/e2e/cnpg-restore.yaml
oc -n "$LAKEHOUSE_NS" wait --for=condition=Ready cluster/polaris-db-restore --timeout=600s
oc -n "$LAKEHOUSE_NS" exec polaris-db-restore-1 -c postgres -- \
  psql -U postgres -d polaris -Atc \
  "select count(*) from information_schema.tables where table_schema not in ('pg_catalog','information_schema')"
```

**Beklenen çıktı** (örnek — son satırdaki tablo sayısı katalog sürümüne göre değişir):

```text
cluster.postgresql.cnpg.io/polaris-db-restore created
cluster.postgresql.cnpg.io/polaris-db-restore condition met
8
```

Yeni veritabanı genellikle bir dakikadan kısa sürede hazır olur. Son komut, geri
yüklenen veritabanındaki tabloları sayar; sıfırdan büyük bir sayı verinin geldiğini
gösterir.

**Ters giderse:** veritabanı `Ready` olmuyorsa günlüğüne bakın:
`oc -n "$LAKEHOUSE_NS" logs polaris-db-restore-1 --all-containers | tail -30`. En sık
neden, yedek kovasına erişilememesidir.

### 5.2 Belirli bir zamana dönüş

Bir olaydan (ör. yanlışlıkla silinen bir tablo) **hemen önceki** ana dönmek için tanım
dosyasının bir kopyasına hedef zaman eklenir:

`[bastion]`

```bash
cp test/e2e/cnpg-restore.yaml /tmp/polaris-db-restore.yaml
vi /tmp/polaris-db-restore.yaml
```

Dosyadaki `bootstrap:` bloğunu şöyle değiştirin; zamanı olaydan birkaç dakika öncesine,
UTC olarak yazın:

```yaml
  bootstrap:
    recovery:
      source: polaris-db
      recoveryTarget:
        targetTime: "2026-11-10 09:30:00+00"
```

Sonra §5.1'deki komutları çalıştırın; yalnız ilk komutta depodaki dosya yerine bu
kopyayı verin:

```bash
oc apply -f /tmp/polaris-db-restore.yaml
```

Bu deneme veritabanı kendi yedeğini almaz (tanımında yedek ayarı bilerek yoktur). Alsaydı
çalışan veritabanıyla aynı klasöre yazıp onun yedeğini bozardı.

**Ters giderse:** hedef zaman yedekte yoksa geri yükleme durur. Saklama süresinin (30
gün) dışında bir zaman seçmiş olabilirsiniz.

### 5.3 Uygulamayı geri yüklenen veritabanına geçirmek

Deneme veritabanının içeriği doğruysa, asıl veritabanı **aynı adla** yedekten yeniden
kurulur ve uygulama yeniden başlatılır:

| Veritabanı | Uygulamayı yeniden başlatma |
|---|---|
| `polaris-db` | `oc -n "$LAKEHOUSE_NS" rollout restart deploy/polaris` |
| `keycloak-db` | `oc -n "$LAKEHOUSE_NS" rollout restart statefulset/keycloak` |
| `superset-db` | `oc -n "$LAKEHOUSE_NS" delete pod -l app.kubernetes.io/component=web-server` |

Sıra şöyledir:

1. Asıl veritabanını silin.
2. Aynı adla, tanım dosyasındaki `bootstrap` bloğu ve yedek ayarı (`plugins`) ile
   yeniden yaratın. Yedek ayarı bu sefer **olmalıdır**, çünkü bu artık asıl
   veritabanıdır.
3. Tablodaki komutla uygulamayı yeniden başlatın.

Deneme veritabanını `-restore` adıyla kalıcı kullanmayın: parola Secret'ları ve yedek
klasörü asıl ada bağlıdır.

### 5.4 Keycloak notu

`keycloak-db` geri yüklenince realm, kullanıcılar ve istemciler veritabanıyla birlikte
geri gelir; realm'i yeniden içe aktarmak gerekmez ([30-kurulum](../30-kurulum.md) §8).
Kullanıcılar zaten AD'den gelir; Keycloak'ta yapılan rol ve eşleme ayarları
veritabanından döner. Realm'i sıfırdan kurmak, yöneticilerin Keycloak arayüzünde elle
yaptığı her şeyi siler.

### 5.5 Denemeyi kapatın

Deneme bittiğinde deneme veritabanını silin; yoksa disk ve bellek boşuna kullanılır.

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" delete cluster polaris-db-restore
```

**Beklenen çıktı:**

```text
cluster.postgresql.cnpg.io "polaris-db-restore" deleted from lakehouse namespace
```

---

## 6. Ad alanını ve diskleri geri yüklemek

Bu bölümdeki bütün komutlarda tam adlar kullanılır: `backups.velero.io`,
`restores.velero.io`, `podvolumebackups.velero.io`. Örneklerdeki yedek adı
(`lakehouse-daily-20261111030010`) yerine §4'teki listeden kendi yedeğinizin adını
yazın.

### 6.1 Tam ad alanı geri yükleme (felaket durumunda)

`[bastion]`

```bash
oc -n openshift-adp get backups.velero.io
oc apply -f - <<'YAML'
apiVersion: velero.io/v1
kind: Restore
metadata: {name: lakehouse-full, namespace: openshift-adp}
spec:
  backupName: lakehouse-daily-20261111030010
  includedNamespaces: [lakehouse]
  existingResourcePolicy: update
YAML
oc -n openshift-adp get restores.velero.io lakehouse-full -o jsonpath='{.status.phase}{"\n"}'
```

**Beklenen çıktı** (örnek; son komutu `Completed` görünene kadar tekrarlayın):

```text
restore.velero.io/lakehouse-full created
Completed
```

**Sıra önemlidir:**

1. Önce operatörler ve ArgoCD (kurulum: [30-kurulum](../30-kurulum.md) §6).
2. Sonra ad alanı geri yüklemesi (bu adım).
3. En son PostgreSQL veritabanları (§5).

ArgoCD Git'teki nesneleri zaten yeniden kurar. Ad alanı yedeği asıl olarak **Git'te
olmayanlar** için değerlidir: kullanıcı diskleri, Zeppelin not defterleri ve elle
yaratılan Secret'lar (§2).

**Ters giderse:** `backupName` bulunamıyorsa adı ilk komutun çıktısından aynen kopyalayın.

### 6.2 Yalnız Secret'ları geri yükleme

`[bastion]`

```bash
oc apply -f - <<'YAML'
apiVersion: velero.io/v1
kind: Restore
metadata: {name: secrets-only, namespace: openshift-adp}
spec:
  backupName: lakehouse-daily-20261111030010
  includedNamespaces: [lakehouse]
  includedResources: [secrets]
  existingResourcePolicy: update
YAML
```

**Beklenen çıktı** (örnek):

```text
restore.velero.io/secrets-only created
```

Geri yükleme tek bir Secret'ı adıyla seçemez; tür ve etiketle daraltılır. Yalnız elle
yaratılan Secret'ları geri yüklemek için etiket seçicisi eklenebilir: elle yaratılan
Secret'ların hepsi `lakehouse.io/yonetim` etiketini taşır. Tanımda `spec` altına şu
satırı ekleyin:

```yaml
  labelSelector: {matchExpressions: [{key: lakehouse.io/yonetim, operator: Exists}]}
```

### 6.3 Disk içeriğini geri yükleme

Disk yedeği, diski **pod yaratılırken** doldurur. Bu yüzden önce disk silinir, sonra
geri yüklenir. Dört adım sırayla yapılır; hiçbiri atlanmaz.

> **Bu yordam bir diski siler — okumadan başlamayın.** Örnek, Zeppelin'in
> `zeppelin-data` diskidir. İki koşul sağlanmazsa not defterleri **geri gelmez**:
> (1) seçtiğiniz yedekte bu diskin yedeği gerçekten vardır (Adım 1);
> (2) disk silinmeden önce ArgoCD'nin kendiliğinden düzeltmesi kapatılmıştır (Adım 2).
> İkincisi atlanırsa ArgoCD silinen diski saniyeler içinde **boş** olarak yeniden
> yaratır; geri yükleme var olan bir diske dokunmadığı için hiçbir şey yapmaz ve elinizde
> boş bir disk kalır. Bu yordamı önce bir deneme ortamında uygulayın.

**Adım 1 — Yedekte bu disk var mı?** Yoksa **durun**.

`[bastion]`

```bash
oc -n openshift-adp get podvolumebackups.velero.io \
  -l velero.io/backup-name=lakehouse-daily-20261111030010 \
  -o custom-columns='POD:.spec.pod.name,HACIM:.spec.volume,FAZ:.status.phase' | grep -i zeppelin
```

**Beklenen çıktı** (örnek):

```text
zeppelin-6d8f7c9b5-x2k4p   data   Completed
```

`HACIM` sütunu pod'daki hacmin adıdır: Zeppelin'in `data` hacmi `zeppelin-data`
diskidir.

**Ters giderse:** satır yoksa bu yedekte diskin içeriği **yoktur**; diski silmeyin. Disk
yedeği CSI anlık görüntüsüyle alınıyorsa (§6.5) aynı kontrol
`oc -n openshift-adp get datauploads.velero.io` ile yapılır.

**Adım 2 — ArgoCD'nin kendiliğinden düzeltmesini kapatın.** ArgoCD silinen diski hemen
Git'teki hâliyle, yani boş olarak geri koyar. Bu ayar geçici olarak kapatılır:

`[bastion]`

```bash
oc -n "$ARGOCD_NS" patch application glue --type merge \
  -p '{"spec":{"syncPolicy":{"automated":null}}}'
oc -n "$ARGOCD_NS" get application glue -o jsonpath='automated={.spec.syncPolicy.automated}{"\n"}'
```

**Beklenen çıktı** (örnek):

```text
application.argoproj.io/glue patched
automated=
```

**Ters giderse:** ikinci satırda `automated=` sonrasında hâlâ bir değer görünüyorsa ayar
kapanmamıştır; **devam etmeyin**.

**Adım 3 — Zeppelin'i durdurun, diski silin, geri yükleyin.**

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" scale deploy/zeppelin --replicas=0
oc -n "$LAKEHOUSE_NS" delete pvc zeppelin-data
oc apply -f - <<'YAML'
apiVersion: velero.io/v1
kind: Restore
metadata: {name: zeppelin-pvc, namespace: openshift-adp}
spec:
  backupName: lakehouse-daily-20261111030010
  includedNamespaces: [lakehouse]
  includedResources: [persistentvolumeclaims, persistentvolumes, pods]
YAML
oc -n openshift-adp get restores.velero.io zeppelin-pvc -o jsonpath='{.status.phase}{"\n"}'
oc -n "$LAKEHOUSE_NS" get pvc zeppelin-data
oc -n "$LAKEHOUSE_NS" scale deploy/zeppelin --replicas=1
oc -n "$LAKEHOUSE_NS" exec deploy/zeppelin -c zeppelin -- ls /data
```

**Beklenen çıktı** (örnek): geri yükleme `Completed`, disk `Bound`; son komut Zeppelin
diskinin klasörlerini listeler:

```text
conf
local-repo
lost+found
notebook
```

Bu geri yükleme ad alanındaki **bütün** diskleri kapsar, ama var olan disklere dokunmaz;
yalnız silinmiş olan disk yeniden yaratılır. Adım 2 bu yüzden zorunludur.

**Adım 4 — ArgoCD'nin kendiliğinden düzeltmesini geri açın.** Disk `Bound` ve pod
`Running` olduktan sonra:

`[bastion]`

```bash
oc -n "$ARGOCD_NS" patch application glue --type merge \
  -p '{"spec":{"syncPolicy":{"automated":{"prune":true,"selfHeal":true}}}}'
oc -n "$ARGOCD_NS" get application glue \
  -o jsonpath='{.status.sync.status} {.status.health.status}{"\n"}'
```

**Beklenen çıktı** (örnek):

```text
application.argoproj.io/glue patched
Synced Healthy
```

Bu adım unutulursa ArgoCD kümeyi artık Git'teki hâlinde tutmaz ve elle yapılan her
değişiklik kümede kalır. Denemenin son adımı her zaman budur.

**Ters giderse:** geri yükleme `Completed` ama klasör boşsa iki neden olabilir: disk
ArgoCD tarafından boş olarak yeniden yaratıldı (Adım 2 atlandı ya da tutmadı) ya da
yedekte diskin içeriği yoktu (Adım 1).

### 6.4 İki tuzak

1. **`oc get backup` PostgreSQL yedeklerini gösterir.** Kümede iki tür `Backup` nesnesi
   vardır; kısa ad PostgreSQL'inkini seçer ve ad alanı yedeği "yok" görünür. Her zaman
   `backups.velero.io` yazın.
2. **Yedek nesnesini `oc delete` ile silmek S3'teki yedeği silmez.** Sonra aynı adla
   alınan yedek "zaten var" hatası verir. Bir yedeği silmenin doğru yolu bir silme isteği
   yaratmaktır:

   `[bastion]`

   ```bash
   oc apply -f - <<'YAML'
   apiVersion: velero.io/v1
   kind: DeleteBackupRequest
   metadata: {name: eski-yedek-sil, namespace: openshift-adp}
   spec: {backupName: lakehouse-daily-20261109030012}
   YAML
   oc -n openshift-adp get backups.velero.io lakehouse-daily-20261109030012
   ```

   **Beklenen çıktı** (örnek — yedek birkaç saniye içinde kaybolur):

   ```text
   deletebackuprequest.velero.io/eski-yedek-sil created
   Error from server (NotFound): backups.velero.io "lakehouse-daily-20261109030012" not found
   ```

### 6.5 Disk yedeğinin türü

Diskler iki yoldan yedeklenebilir:

- **Dosya sistemi yedeği:** diskteki dosyalar tek tek kopyalanır. Ek ayar gerektirmez.
- **CSI anlık görüntüsü:** depolama sisteminin kendi anlık görüntüsü alınıp yedek
  kovasına taşınır. Daha hızlı ve daha tutarlıdır; **önerilen yoldur.** Depolama
  sınıfınızın anlık görüntü desteklemesi gerekir. Açmak için §7.1'deki tanımda eklenti
  listesinde `csi` bulunmalı; zamanlanmış yedekte `snapshotVolumes` ve
  `snapshotMoveData` açık olmalıdır.

Hangi yolu kullanırsanız kullanın, disk geri yüklemesini (§6.3) kurulumdan sonra bir kez
deneme ortamında uygulayarak doğrulayın
([90-referans/pre-ship-kontrol-listesi.md](../90-referans/pre-ship-kontrol-listesi.md)
madde 3.3).

---

## 7. Ad alanı yedeğini açmak (OpenShift OADP)

Ad alanı yedeğini OpenShift'in **OADP** operatörü sağlar; operatörün kurulumu
[20-on-kosullar](../20-on-kosullar.md) madde 9.2'dedir. Sıra şöyledir ve tersine
çevrilmez:

1. OADP'nin yedek konumunu tanımlayın (§7.1).
2. Sonra ürünün ayarında ad alanı yedeğini açın (§7.2).

Ters sırada yapılırsa `glue` uygulaması hata verir, çünkü kuracağı yedek nesnesinin türü
kümede henüz yoktur.

### 7.1 Yedek konumunu tanımlayın

Önce `cloud-credentials` Secret'ı OADP ad alanında hazır olmalıdır; yedek kovasının S3
anahtarlarını taşır ([90-referans/secret-listesi.md](../90-referans/secret-listesi.md)
§2). Sonra yedek konumunu tanımlayın. Aşağıdaki `bucket`, `region` ve `s3Url` değerlerini
kendi `$S3_BUCKET_BACKUP`, `$S3_REGION` ve `$S3_ENDPOINT` değerlerinizle değiştirin:

`[bastion]`

```bash
oc apply -f - <<'YAML'
apiVersion: oadp.openshift.io/v1alpha1
kind: DataProtectionApplication
metadata: {name: lakehouse-dpa, namespace: openshift-adp}
spec:
  configuration:
    velero: {defaultPlugins: [aws, openshift, csi]}
    nodeAgent: {enable: true, uploaderType: kopia}
  backupLocations:
  - velero:
      provider: aws
      default: true
      objectStorage: {bucket: lakehouse-backups, prefix: velero}
      config: {region: us-east-1, s3Url: "https://s3.kurum.example.net", s3ForcePathStyle: "true"}
      credential: {name: cloud-credentials, key: cloud}
YAML
oc -n openshift-adp get dpa \
  -o custom-columns='AD:.metadata.name,UYGULANDI:.status.conditions[?(@.type=="Reconciled")].status'
oc -n openshift-adp get backupstoragelocation
```

**Beklenen çıktı** (örnek):

```text
dataprotectionapplication.oadp.openshift.io/lakehouse-dpa created
AD              UYGULANDI
lakehouse-dpa   True
NAME              PHASE       LAST VALIDATED   AGE
lakehouse-dpa-1   Available   10s              30s
```

**Ters giderse:**

- Tanım reddedilirse alan adları kurulu OADP sürümünde farklı olabilir. Doğru alanları
  kümeden okuyun: `oc explain dataprotectionapplication.spec --recursive | head -60`.
- Konum `Available` olmuyorsa kimlik, kova ya da S3 adresi yanlıştır. Nedeni şu komut
  yazar: `oc -n openshift-adp logs deploy/velero --tail=50`.

### 7.2 Ad alanı yedeğini açın

Konum `Available` olduktan **sonra**, `platform/values/site/glue.yaml` dosyasını açın:

`[bastion]`

```bash
git pull --rebase origin main
vi platform/values/site/glue.yaml
```

Dosyanın sonuna şu satırı ekleyin (dosyada zaten bir `velero:` satırı varsa onu bununla
değiştirin):

```yaml
velero: {enabled: true, namespace: openshift-adp}
```

Kaydedip denetleyin ve gönderin:

```bash
bash scripts/check-site.sh
git diff
git add platform/values/site/glue.yaml
git commit -m "site: ad alanı yedeği açıldı (OADP)"
git push origin main
```

ArgoCD değişikliği uyguladıktan sonra
([değişiklik nasıl uygulanır](degisiklik-nasil-uygulanir.md) §5) zamanlanmış yedek
OADP ad alanında görünür:

`[bastion]`

```bash
oc -n openshift-adp get schedules.velero.io lakehouse-daily
```

**Beklenen çıktı** (örnek; `LASTBACKUP` ilk yedekten sonra dolar):

```text
NAME              STATUS    SCHEDULE    LASTBACKUP   AGE
lakehouse-daily   Enabled   0 3 * * *                1m
```

**Ters giderse:** `glue` uygulaması "böyle bir tür yok" hatası veriyorsa OADP henüz
kurulmamıştır. Satırı geri alın (`git revert`), operatörü kurun, sonra satırı yeniden
ekleyin.

> **Kapalıyken:** `velero.enabled: false` iken (varsayılan) zamanlanmış yedek **ve**
> Kafka ile PostgreSQL disklerini yedekten çıkaran ayarlar hiç kurulmaz.

---

## 8. Deneme takvimi

Yedeğin işe yaradığı ancak geri yükleyerek görülür. Önerilen takvim:

| Ne sıklıkla | Ne denenir | Nasıl |
|---|---|---|
| Her kurulumda ve kabulde | PostgreSQL yedek + geri yükleme, ad alanı yedeği + geri yükleme | kabul testleri ([90-referans/kabul-testleri.md](../90-referans/kabul-testleri.md)) |
| Haftada bir | yedeklerin durumu | §4 ([gunluk-haftalik-kontroller.md](gunluk-haftalik-kontroller.md) §4) |
| **Üç ayda bir** | katalog veritabanını dünkü bir zamana döndürme, tablo sayımı, deneme veritabanını silme | §5 |
| **Üç ayda bir** | bir diski (Zeppelin ya da bir kullanıcı diski) geri yükleme — dört adımın tamamı | §6.3 |
| **Yılda bir** | ayrı bir ad alanına ya da kümeye tam geri yükleme ve geçen sürenin ölçülmesi | §6.1 |

**Veri kaybı penceresi:** PostgreSQL için birkaç dakika (sürekli yedek); ad alanı
yedeği için en çok 24 saat (günlük yedek). Kurtarma hedefleri kurumla bu değerler
üzerinden konuşulur.

---

## Kontrol listesi

- [ ] Üç PostgreSQL veritabanında da `ARSIV` sütunu `True`.
- [ ] Son günlük yedekler `completed`; zamanlanmış yedek nesneleri duruyor.
- [ ] Yedek kovası veri kovasından ayrı; kovadaki otomatik silme kuralı iki klasörü ayrı
      ele alıyor.
- [ ] Ad alanı yedeği `Completed`; yedek konumu `Available`.
- [ ] Ekipteki herkes `oc get backup` yerine `backups.velero.io` yazılacağını biliyor.
- [ ] Üç ayda bir zaman noktası denemesi yapıldı ve deneme veritabanı silindi.
- [ ] Disk geri yükleme denemesinde sıra izlendi: yedekte disk var (§6.3 Adım 1),
      ArgoCD'nin kendiliğinden düzeltmesi kapatıldı (Adım 2), disk **ancak ondan sonra**
      silindi.
- [ ] Denemeden sonra ArgoCD'nin kendiliğinden düzeltmesi geri açıldı ve uygulama
      `Synced Healthy` (§6.3 Adım 4).
- [ ] Kafka verisinin ve S3'teki tablo dosyalarının bu yedeklerin kapsamında olmadığı ve
      nasıl geri döndükleri yazılı olarak kayda geçti.

## Sonraki bölüm

Yükseltmeden önce yedeklerin sağlam olması gerekir: [yukseltme.md](yukseltme.md).
Haftalık kontrol listesindeki yeri:
[gunluk-haftalik-kontroller.md](gunluk-haftalik-kontroller.md). Yedek hataları:
[sorun-giderme.md](sorun-giderme.md) §8.
