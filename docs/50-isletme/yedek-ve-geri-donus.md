# Yedek ve geri dönüş

**Bu bölümde:** neyin yedeklendiği ve neyin **bilerek** yedeklenmediği, iki yedek
mekanizmasının (PostgreSQL sürekli yedeği ve ad alanı yedeği) zamanlaması ile saklama
süresi, haftalık sağlık kontrolü, PostgreSQL'i zaman noktasına döndürme provası, ad alanı
ve disk içeriği geri yükleme yordamları, OpenShift'te OADP kurulumu ve prova takvimi.
**Süre:** sağlık kontrolü 5 dakika; PostgreSQL geri dönüş provası 20–30 dakika; tam ad
alanı provası yarım gün.
**Gereken yetki:** `$LAKEHOUSE_NS` ve yedek ad alanında (`openshift-adp`) okuma; prova
sırasında aynı ad alanlarında nesne yaratma ve silme; Git deposunda `main` dalına yazma.
**Nerede çalıştırılır:** `[bastion]` — `oc login` ile kümeye girilmiş yönetim makinesi.

> **İki mekanizma, iki farklı kapsam.** PostgreSQL veritabanları (Polaris kataloğu,
> Keycloak, Superset) **sürekli yedek + zaman noktasına dönüş** ile korunur. Kubernetes
> nesneleri ve not defteri diskleri **ad alanı yedeği** ile korunur. İkisi birbirinin
> yerine geçmez.

---

## 1. Değişkenleri yükleyin

`[bastion]`

```bash
cd ~/lakehouse
set -a; . install/lakehouse.env; set +a
echo "lakehouse=$LAKEHOUSE_NS yedek kovasi=$S3_BUCKET_BACKUP"
```

**Beklenen çıktı** (örnek — kendi değerlerinizle):

```text
lakehouse=lakehouse yedek kovasi=lakehouse-backups
```

**Ters giderse:** ikinci alan boşsa `install/lakehouse.env` doldurulmamıştır
([30-kurulum](../30-kurulum.md) §1).

---

## 2. Kapsam: ne yedekleniyor, ne yedeklenmiyor

| Veri | Yedek | Nasıl | Geri dönüş yolu |
|---|---|---|---|
| **Polaris katalog verisi** (`polaris-db`) | var | Sürekli WAL arşivi + günlük tam yedek | Zaman noktasına dönüş (§5) |
| **Keycloak** (`keycloak-db`) | var | aynı | aynı (+ realm notu §5.4) |
| **Superset** (`superset-db`: panolar, grafikler, bağlantılar) | var | aynı | aynı |
| **Kubernetes nesneleri** (`$LAKEHOUSE_NS`: özel kaynaklar, ConfigMap, Secret, Deployment…) | var | Günlük ad alanı yedeği | Ad alanı geri yükleme (§6.1) |
| **Not defteri diskleri** (JupyterHub hub ve kullanıcı diskleri, Zeppelin diski) | var (gerçek CSI depolamada) | Ad alanı yedeğinin dosya sistemi yedeği | Disk geri yükleme (§6.3) |
| **PostgreSQL diskleri** | **bilerek dışlandı** | pod açıklamasıyla dosya sistemi yedeğinden çıkarıldı | Dönüş yolu **zaman noktasına dönüştür** (tutarlılık) |
| **Kafka verisi** | **kapsam dışı** | açıklamayla dışlandı | Kaynaklardan **yeniden akıtma** (§2.1) |
| **Iceberg verisi ve tablo dosyaları** (S3) | **kapsam dışı** | — | **S3'ün kendi çoğaltması** (müşteri depolaması) |

> **Iceberg için kritik.** Polaris veritabanını geri yüklemek tabloları geri getirmez —
> tablo **dosyaları** S3'tedir. Tersi de doğrudur: S3 duruyor ama katalog verisi kayıpsa
> tablolar "yok" görünür. İkisi **aynı zaman penceresine** getirilmelidir; S3 çoğaltması
> gecikmeliyse dönüş hedefini S3'ün tutarlı olduğu ana seçin.

### 2.1 Kafka verisi neden yedeklenmiyor

Kafka bu mimaride **taşıyıcıdır**, kayıt sistemi değil: kalıcı gerçek, kaynak
veritabanlarında ve Iceberg/S3'tedir. Broker verisi kaybolursa doğru dönüş yolu kaynaktan
**yeniden snapshot** almaktır
([mevcut-kaynaga-tablo-ekleme.md](mevcut-kaynaga-tablo-ekleme.md)); bu, yedekten dönen bayat
konum ve şema geçmişiyle çalışmaktan daha tutarlıdır. İkinci bir site (çoklu-küme çoğaltma)
kurulmaz: o bir yedekleme aracı değil, aktif-aktif mimari kararıdır ve bu kapsamın
dışındadır. Kararın gerekçesi:
[90-referans/pre-ship-kontrol-listesi.md](../90-referans/pre-ship-kontrol-listesi.md) §5.2.

---

## 3. Zamanlama, saklama ve hedefler

| | PostgreSQL (sürekli yedek) | Ad alanı yedeği |
|---|---|---|
| Zamanlama | `"0 0 2 * * *"` — **altı alanlı** cron (saniye dâhil) → her gün 02:00 | `"0 3 * * *"` — beş alanlı → her gün 03:00 |
| Nerede tanımlı | `platform/values/site/glue.yaml` → `backup.schedule` | `glue/values.yaml` → `velero.schedule` |
| Saklama | `backup.retentionPolicy: 30d` | `velero.ttl: 720h` (30 gün) |
| Hedef | `$S3_BUCKET_BACKUP` kovasının `cnpg/` öneki | aynı kovanın `velero/` öneki |
| Kimlik | `backup-s3-creds` Secret'ı | OADP'nin `cloud-credentials` Secret'ı |
| İlk yedek | Nesne yaratılır yaratılmaz | İlk zamanlanan koşuda |
| Ürettiği nesneler | `ScheduledBackup/{polaris,keycloak,superset}-db-daily` | `Schedule/lakehouse-daily` |

**Yedek kovası veri kovasından ayrı olmalıdır** — geri yükleme hedefiyle kaynağı aynı kovaya
koymak, kaynağı kaybettiğinizde yedeği de kaybetmek demektir
([30-kurulum](../30-kurulum.md) §5.3).

**Aynı kovada iki önek kullanılıyorsa** (varsayılan) sürekli yedek yalnız `cnpg/` altını, ad
alanı yedeği yalnız `velero/` altını yönetir; biri diğerinin dosyalarına dokunmaz. Ancak
depolama tarafında bir yaşam döngüsü kuralı (otomatik silme) tanımlanacaksa **iki önek ayrı
ayrı** ele alınmalıdır: aksi hâlde kural, sürekli yedeğin hâlâ ihtiyaç duyduğu WAL
dosyalarını silip dönüş zincirini kırabilir. Önerilen: yedekler için ayrı bir kova ve ayrı
bir hesap.

---

## 4. Haftalık sağlık kontrolü

Yedekler için **alarm kuralı yoktur**; sağlık elle denetlenir. Aynı komutlar her
yükseltmeden önce de koşturulur ([yukseltme.md](yukseltme.md) §2).

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" get cluster \
  -o custom-columns='DB:.metadata.name,ARSIV:.status.conditions[?(@.type=="ContinuousArchiving")].status'
oc -n "$LAKEHOUSE_NS" get backups.postgresql.cnpg.io \
  -o custom-columns='AD:.metadata.name,FAZ:.status.phase,YONTEM:.status.method' | tail -5
oc -n "$LAKEHOUSE_NS" get scheduledbackup
```

**Beklenen çıktı** (kind kümesinde alınmış gerçek çıktı; `demo-pg` yalnız geliştirme
kümesindeki örnek kaynak veritabanıdır, üretimde ilk tabloda üç satır olur):

```text
DB            ARSIV
demo-pg       True
keycloak-db   True
polaris-db    True
superset-db   True
AD                                 FAZ         YONTEM
polaris-db-daily-20260921020000    completed   plugin
superset-db-daily-20260918193824   completed   plugin
superset-db-daily-20260919020000   completed   plugin
superset-db-daily-20260920020000   completed   plugin
superset-db-daily-20260921020000   completed   plugin
NAME                AGE     CLUSTER       LAST BACKUP
keycloak-db-daily   5d16h   keycloak-db   3d9h
polaris-db-daily    5d16h   polaris-db    3d9h
superset-db-daily   5d16h   superset-db   3d9h
```

Ad alanı yedeği tarafı — **tam nitelikli ad zorunludur**, kısa `backup` adı kümede
PostgreSQL'in kendi yedek nesnesine çözülür:

`[bastion]`

```bash
oc -n openshift-adp get backups.velero.io \
  -o custom-columns='AD:.metadata.name,FAZ:.status.phase,OGE:.status.progress.itemsBackedUp,BITIS:.status.completionTimestamp'
oc -n openshift-adp get backupstoragelocation
oc -n openshift-adp get schedule lakehouse-daily
```

**Beklenen çıktı** (kind kümesinde alınmış gerçek çıktı; orada ad alanı `velero`, üretimde
`openshift-adp`'dir):

```text
AD                               FAZ         OGE   BITIS
lakehouse-daily-20260919070630   Completed   436   2026-09-19T07:09:39Z
lakehouse-daily-20260920095302   Completed   462   2026-09-20T10:00:36Z
lakehouse-daily-20260924081157   Completed   752   2026-09-24T08:14:33Z
NAME      PHASE       LAST VALIDATED   AGE     DEFAULT
default   Available   27s              5d16h   true
NAME              STATUS    SCHEDULE    LASTBACKUP   AGE     PAUSED
lakehouse-daily   Enabled   0 3 * * *   3h36m        5d16h
```

**Ters giderse:** `ARSIV` sütunu `False` ise WAL arşivi çalışmıyordur
([sorun-giderme.md](sorun-giderme.md) §8). Depolama konumu `Available` değilse kova, kimlik
ya da uç nokta yanlıştır. `Schedule` bulunamıyorsa ad alanı yedeği hiç açılmamıştır (§7).

---

## 5. PostgreSQL geri dönüş provası

**Kaynak kümeye dokunulmaz.** Yedekten **ayrı** bir veritabanı kümesi ayağa kaldırılır,
doğrulanır, sonra gerekiyorsa uygulama ona yönlendirilir. Şablon:
`test/e2e/cnpg-restore.yaml`.

### 5.1 Son yedeğe dönüş

`[bastion]`

```bash
oc apply -f test/e2e/cnpg-restore.yaml
oc -n "$LAKEHOUSE_NS" wait --for=condition=Ready cluster/polaris-db-restore --timeout=600s
oc -n "$LAKEHOUSE_NS" exec polaris-db-restore-1 -c postgres -- \
  psql -U postgres -d polaris -Atc \
  "select count(*) from information_schema.tables where table_schema not in ('pg_catalog','information_schema')"
```

**Beklenen çıktı** (örnek — tablo sayısı Polaris sürümüne göre değişir):

```text
cluster.postgresql.cnpg.io/polaris-db-restore created
cluster.postgresql.cnpg.io/polaris-db-restore condition met
8
```

Polaris tabloları `polaris_schema` şemasındadır (`public` **değil**); bu yüzden sayım sistem
şemaları dışındaki her şeyi sayar. Ölçüm (geliştirme kümesi): tam yedek 8–12 saniye, geri
yüklenen küme hazır ~35 saniye.

**Ters giderse:** küme `Ready` olmuyorsa geri yükleme günlüğüne bakın
(`oc -n "$LAKEHOUSE_NS" logs polaris-db-restore-1 --all-containers`); en sık kök, yedek
kovasına erişememektir.

### 5.2 Belirli bir zamana dönüş

Olaydan **hemen öncesine** dönmek için şablona bir hedef zaman eklenir.
`test/e2e/cnpg-restore.yaml` kopyalanır ve geri yükleme bloğu şu hâle getirilir:

```yaml
  bootstrap:
    recovery:
      source: polaris-db
      recoveryTarget:
        targetTime: "2026-09-21 09:30:00+00"       # olaydan hemen ÖNCESİ (UTC)
```

**`plugins` alanı geri yükleme kümesinde bilerek yoktur:** bu küme WAL arşivlemez.
Arşivleseydi kaynakla aynı klasöre yazıp kaynağın yedek zincirini bozardı. Geri yükleme
kalıcı hâle gelecekse (§5.3) o zaman eklenir.

**Ters giderse:** hedef zaman arşivde yoksa geri yükleme durur; saklama süresinin
(varsayılan 30 gün) dışına çıkmış olabilirsiniz.

### 5.3 Uygulamayı geri yüklenmiş veritabanına yönlendirme

| Veritabanı | Bağlantı nereden gelir | Yapılacak |
|---|---|---|
| `polaris-db` | Polaris'in bağlantı Secret'ı | Doğruladıktan sonra: eski kümeyi silin → geri yükleme kümesini **orijinal adla** yeniden yaratın (aynı geri yükleme bloğu + `plugins`) → `oc -n "$LAKEHOUSE_NS" rollout restart deploy/polaris` |
| `keycloak-db` | Keycloak CR'ının veritabanı bloğu | Aynı desen → `oc -n "$LAKEHOUSE_NS" rollout restart statefulset/keycloak` |
| `superset-db` | Superset CR'ının bağlantı alanı | Aynı desen → `oc -n "$LAKEHOUSE_NS" delete pod -l app.kubernetes.io/component=web-server` |

Kümeyi `-restore` adıyla kalıcı kullanmak **önerilmez**: Secret adları, değer
dosyalarındaki kopyalar ve yedek klasörü orijinal ada bağlıdır.

### 5.4 Keycloak realm notu

`keycloak-db` geri yüklemesi realm'i, kullanıcıları ve istemcileri **veritabanı
seviyesinde** geri getirir; realm içe aktarımının yeniden koşmasına gerek yoktur (zaten var
olan realm'i güncellemez — [30-kurulum](../30-kurulum.md) "Realm içeriğini sonradan
değiştirmek"). AD federasyonu varsa kullanıcılar zaten AD'den akar; yerel rol ve eşlemeler
veritabanından gelir. Realm'i sıfırdan kurmak **kullanıcıların arayüzde elle yaptığı her şeyi
siler**.

### 5.5 Provayı kapatma

Prova bittiğinde geri yükleme kümesi **silinir**; aksi hâlde disk ve bellek boşuna durur.

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" delete cluster polaris-db-restore
```

**Beklenen çıktı** (örnek):

```text
cluster.postgresql.cnpg.io "polaris-db-restore" deleted
```

---

## 6. Ad alanı ve disk geri yükleme

Bütün komutlarda **tam nitelikli ad** kullanılır: `backups.velero.io`, `restores.velero.io`,
`podvolumebackups.velero.io`.

### 6.1 Tam ad alanı geri yükleme (felaket senaryosu)

`[bastion]`

```bash
oc -n openshift-adp get backups.velero.io
oc apply -f - <<'YAML'
apiVersion: velero.io/v1
kind: Restore
metadata: {name: lakehouse-full, namespace: openshift-adp}
spec:
  backupName: lakehouse-daily-20260924081157
  includedNamespaces: [lakehouse]
  existingResourcePolicy: update
YAML
oc -n openshift-adp get restores.velero.io lakehouse-full -o jsonpath='{.status.phase}{"\n"}'
```

**Beklenen çıktı** (örnek):

```text
restore.velero.io/lakehouse-full created
Completed
```

**Sıra önemlidir:** önce operatörler ve özel kaynak tanımları (`bootstrap/bootstrap.sh`),
sonra ad alanı geri yüklemesi, en son PostgreSQL kümeleri (§5). Eşitleme, `glue`
uygulamasının ürettiği nesneleri zaten Git'ten geri koyar; ad alanı yedeği asıl olarak
**Git'te olmayanlar** için değerlidir: kullanıcı diskleri, Zeppelin not defterleri, elle
yaratılan Secret'lar ([90-referans/secret-listesi.md](../90-referans/secret-listesi.md) §1).

**Ters giderse:** yedek adı bulunamıyorsa adı ilk komuttan birebir kopyalayın; ad alanı
üretimde `openshift-adp`, geliştirme kümesinde `velero`'dur.

### 6.2 Yalnız Secret'ları geri yükleme

`[bastion]`

```bash
oc apply -f - <<'YAML'
apiVersion: velero.io/v1
kind: Restore
metadata: {name: secrets-only, namespace: openshift-adp}
spec:
  backupName: lakehouse-daily-20260924081157
  includedNamespaces: [lakehouse]
  includedResources: [secrets]
  existingResourcePolicy: update
YAML
```

**Beklenen çıktı** (örnek):

```text
restore.velero.io/secrets-only created
```

Ad ile filtreleme **yoktur**; daraltma yalnız kaynak türü ve etiketle yapılır.

### 6.3 Disk içeriği geri yükleme

Dosya sistemi yedeği hedef diski **pod yaratılırken** doldurur → iş yükü önce durdurulur:

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" scale deploy/zeppelin --replicas=0
oc -n "$LAKEHOUSE_NS" delete pvc zeppelin-data
oc apply -f - <<'YAML'
apiVersion: velero.io/v1
kind: Restore
metadata: {name: zeppelin-pvc, namespace: openshift-adp}
spec:
  backupName: lakehouse-daily-20260924081157
  includedNamespaces: [lakehouse]
  includedResources: [persistentvolumeclaims, persistentvolumes, pods]
YAML
oc -n "$LAKEHOUSE_NS" scale deploy/zeppelin --replicas=1
oc -n "$LAKEHOUSE_NS" exec deploy/zeppelin -- ls /data | head
```

**Kapsam notu:** ad ile filtre olmadığı ve `zeppelin-data` diskinde etiket bulunmadığı için
yukarıdaki geri yükleme ad alanındaki **bütün** diskleri kapsar. Var olan nesnelere
dokunulmaz (varsayılan davranış mevcut olanı atlar), yalnız silinmiş olan disk yeniden
yaratılır. Tek bir diski hedeflemek istiyorsanız **yedek alınmadan önce** diski etiketleyin
ve geri yüklemeye bir etiket seçicisi ekleyin.

Hangi disklerin gerçekten yedeklendiğini görmek için:

`[bastion]`

```bash
oc -n openshift-adp get podvolumebackups.velero.io \
  -l velero.io/backup-name=lakehouse-daily-20260924081157 \
  -o custom-columns='POD:.spec.pod.name,HACIM:.spec.volume,FAZ:.status.phase'
```

`HACIM` sütunu **pod hacim adıdır**, disk (PVC) adı değil.

**Ters giderse:** geri yükleme `Completed` ama dizin boşsa disk hiç yedeklenmemiştir —
§6.5'teki geliştirme kümesi sınırına bakın.

### 6.4 İki tuzak

1. **`oc get backup` PostgreSQL'e çözülür.** Kümede hem PostgreSQL'in hem ad alanı yedeğinin
   `Backup` nesnesi vardır; kısa ad sessizce PostgreSQL'i seçer ve yedek "yok" görünür. Daima
   `backups.velero.io` yazın.
2. **Yedek nesnesini silmek S3'ü temizlemez.** Aynı adla ikinci bir yedek "nesne depolamada
   zaten var" diyerek başarısız olur. Doğru silme yolu bir silme isteği yaratmaktır:

   `[bastion]`

   ```bash
   oc apply -f - <<'YAML'
   apiVersion: velero.io/v1
   kind: DeleteBackupRequest
   metadata: {name: eski-yedek-sil, namespace: openshift-adp}
   spec: {backupName: lakehouse-daily-20260919070630}
   YAML
   oc -n openshift-adp get backups.velero.io lakehouse-daily-20260919070630
   ```

   **Beklenen çıktı** (örnek — nesne kaybolana kadar birkaç saniye geçer):

   ```text
   deletebackuprequest.velero.io/eski-yedek-sil created
   Error from server (NotFound): backups.velero.io "lakehouse-daily-20260919070630" not found
   ```

### 6.5 Geliştirme kümesi sınırı ≠ üretim

kind kümesinin disk sağlayıcısı düğüm üzerindeki dizinleri kullanır ve dosya sistemi yedeği
bu tür hacimleri **atlar**. Sonuç: **geliştirme kümesinde disk içerikleri yedeklenmez**;
yalnız geçici hacimler yedek üretir. Üretimde (OpenShift CSI depolama) diskler yedeğe girer;
**tercih edilen** yol ise CSI anlık görüntüsü + veri taşıyıcıdır (`Backup` nesnesinde
`snapshotVolumes` ve `snapshotMoveData`, OADP eklenti listesinde `csi`). **Disk içeriği
yedeği, gerçek OpenShift ortamında canlı doğrulanacak açık kalemdir**
([90-referans/pre-ship-kontrol-listesi.md](../90-referans/pre-ship-kontrol-listesi.md) madde
3.3).

---

## 7. OpenShift: OADP kurulumu ve ad alanı yedeğinin açılması

Üretimde ad alanı yedeğini **OADP operatörü** sağlar
([20-on-kosullar](../20-on-kosullar.md) madde 9.2). Sıra şudur ve **ters çevrilemez**: önce
operatör ve `DataProtectionApplication`, sonra site değerinde açma. Ters sırada glue
`Degraded` olur, çünkü yazacağı nesnenin tanımı kümede yoktur.

### 7.1 `DataProtectionApplication` uygulayın

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
      config: {region: us-east-1, s3Url: "https://s3.example.com", s3ForcePathStyle: "true"}
      credential: {name: cloud-credentials, key: cloud}
YAML
oc -n openshift-adp get dpa \
  -o custom-columns='AD:.metadata.name,UYGULANDI:.status.conditions[?(@.type=="Reconciled")].status'
oc -n openshift-adp get backupstoragelocation
```

`bucket`, `region` ve `s3Url` değerleri `$S3_BUCKET_BACKUP`, `$S3_REGION` ve `$S3_ENDPOINT`
ile aynı olmalıdır. `cloud-credentials` Secret'ı yedek S3 anahtar çiftini taşır
([90-referans/secret-listesi.md](../90-referans/secret-listesi.md) §2).

**Beklenen çıktı** (örnek — OpenShift'e özgü, **OpenShift'te doğrulanır**):

```text
dataprotectionapplication.oadp.openshift.io/lakehouse-dpa created
AD              UYGULANDI
lakehouse-dpa   True
NAME              PHASE       LAST VALIDATED   AGE
lakehouse-dpa-1   Available   10s              30s
```

**Ters giderse:** alan adları kurulu OADP sürümüne göre değişebilir; şemayı kümeden okuyun:

`[bastion]`

```bash
oc explain dataprotectionapplication.spec --recursive | head -60
```

Depolama konumu `Available` olmuyorsa kimlik, kova ya da uç nokta yanlıştır;
`oc -n openshift-adp logs deploy/velero --tail=50` nedeni yazar.

### 7.2 Site değerinde açın

Depolama konumu `Available` olduktan **sonra** `platform/values/site/glue.yaml` dosyasına şu
satır eklenir:

```yaml
velero: {enabled: true, namespace: openshift-adp}
```

`[bastion]`

```bash
${EDITOR:-vi} platform/values/site/glue.yaml
bash scripts/check-site.sh
git add platform/values/site/glue.yaml
git commit -m "site: ad alani yedegi acildi (OADP)"
git push origin main
```

Eşitleme bittikten sonra zamanlı yedek nesnesi **OADP ad alanında** doğar:

`[bastion]`

```bash
oc -n openshift-adp get schedules.velero.io lakehouse-daily
```

**Beklenen çıktı** (örnek):

```text
NAME              STATUS    SCHEDULE    LASTBACKUP   AGE
lakehouse-daily   Enabled   0 3 * * *                1m
```

**Ters giderse:** `glue` uygulaması "böyle bir tür yok" hatasıyla `Degraded` olduysa OADP
henüz kurulmamıştır; değeri geri kapatın, operatörü kurun, sonra yeniden açın.

> **Kapalıyken ne olur?** `velero.enabled: false` iken zamanlı yedek nesnesi **ve** Kafka ile
> PostgreSQL disklerinin dışlama açıklamaları hiç üretilmez. Yani dışlama kuralları da bu
> anahtarın arkasındadır.

---

## 8. Prova takvimi

| Sıklık | Prova | Kanıt |
|---|---|---|
| Her kurulum ve her kabul koşusu | PostgreSQL yedek + geri yükleme, ad alanı yedeği + geri yükleme | `scripts/acceptance.sh` → `E2E F5 DR OK` ([90-referans/kabul-testleri.md](../90-referans/kabul-testleri.md)) |
| Haftalık | Arşiv ve son yedek sağlığı | §4 komutları ([gunluk-haftalik-kontroller.md](gunluk-haftalik-kontroller.md) §4) |
| **Çeyreklik** | **Zaman noktasına dönüş provası** (Polaris veritabanı): dünkü bir zamana geri yükleme, tablo sayımı, kümeyi silme | §5 |
| **Çeyreklik** | **Disk geri yükleme provası** (Zeppelin ya da bir kullanıcı diski) | §6.3 — geliştirme kümesinde anlamsız (§6.5), üretimde yapılır |
| **Yılda bir** | **Tam ad alanı geri yükleme** provası (ayrı bir ad alanı ya da küme) + kurtarma süresi ölçümü | §6.1 |

Ölçülen değerler (geliştirme kümesi, 2026-09-18): PostgreSQL tam yedeği **8–12 saniye** ·
geri yüklenen küme hazır **~35 saniye** · ad alanı yedeği **2 dakika 22 saniye – 2 dakika 26
saniye** · uçtan uca DR yolu **3 dakika 32 saniye – 3 dakika 58 saniye**. Pratik **veri kaybı
penceresi**: PostgreSQL için WAL arşivleme gecikmesi (dakikalar), ad alanı yedeği için 24
saat (günlük zamanlama). Üretim hedefleri müşteriyle bu ölçümler üzerinden konuşulur.

---

## Kontrol listesi

- [ ] Üç PostgreSQL kümesinde de sürekli arşivleme `True`.
- [ ] Son günlük yedekler `completed`; zamanlı yedek nesneleri duruyor.
- [ ] Yedek kovası veri kovasından ayrı; yaşam döngüsü kuralı iki öneki ayrı ele alıyor.
- [ ] Ad alanı yedeği `Completed`; depolama konumu `Available`.
- [ ] `oc get backup` yerine daima `backups.velero.io` yazıldığı ekipçe biliniyor.
- [ ] Çeyreklik zaman noktası provası yapıldı ve geri yükleme kümesi silindi.
- [ ] Kafka verisinin ve S3'teki tablo dosyalarının kapsam dışı olduğu, dönüş yollarının ne
      olduğu yazılı olarak kayda geçti.

## Sonraki bölüm

Yükseltmeden önce yedek durumunun yeşil olması gerekir: [yukseltme.md](yukseltme.md). Yedek
sağlığının haftalık kontrol listesindeki yeri:
[gunluk-haftalik-kontroller.md](gunluk-haftalik-kontroller.md). Yedek hataları:
[sorun-giderme.md](sorun-giderme.md) §8.
