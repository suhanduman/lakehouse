# Yedekleme ve felaketten dönüş (DR) — şartname G.6

İki bağımsız yedek mekanizması vardır ve **kapsamları farklıdır**:

- **CNPG Barman Cloud eklentisi** → Postgres veritabanları (Polaris katalog metadata'sı, Keycloak, Superset):
  sürekli WAL arşivi + günlük base backup → **PITR** (zaman noktasına dönüş).
- **Velero** (dev'de chart, prod'da **OADP**) → `lakehouse` namespace'inin Kubernetes nesneleri + node-agent
  dosya sistemi yedeği (Kopia) ile PVC içerikleri.

Uçtan uca kanıt her kurulum/CI koşusunda alınır: `test/e2e/dr-path.sh` → `E2E F5 DR OK`.

## 1. Kapsam tablosu (ne yedekleniyor, ne yedeklenmiyor)

| Veri | Yedek | Nasıl | Geri dönüş |
|---|---|---|---|
| **Polaris katalog metadata'sı** (`polaris-db`) | ✅ PITR | Barman WAL + günlük base backup | `test/e2e/cnpg-restore.yaml` deseni (§4) |
| **Keycloak** (`keycloak-db`) | ✅ PITR | aynı | aynı (+ realm notu §4.4) |
| **Superset metadata'sı** (`superset-db`: dashboard/chart/bağlantı) | ✅ PITR | aynı | aynı |
| **Kubernetes nesneleri** (`lakehouse` ns: CR'lar, ConfigMap, Secret, Deployment…) | ✅ | Velero `Schedule/lakehouse-daily` | `Restore` (§5) |
| **Not defteri PVC'leri** (JupyterHub `hub-db-dir` + kullanıcı PVC'leri, Zeppelin `zeppelin-data`) | ✅ (gerçek CSI depolamada) | Velero node-agent fs-backup (Kopia) | `Restore` + PVC (§5.3); **kind'da çalışmaz** → §5.4 |
| **Postgres PVC'leri (`pgdata`)** | ⛔ bilerek dışlandı | pod annotation `backup.velero.io/backup-volumes-excludes: pgdata` | Postgres'in dönüş yolu **Barman PITR**'dır (tutarlılık) |
| **Kafka verisi** (`data-0`) | ❌ kapsam dışı | annotation ile fs-backup'tan dışlandı | Kaynaklardan **yeniden akıtma** (Debezium snapshot) ya da MirrorMaker 2 ile ikinci kümeye çoğaltma |
| **Iceberg verisi + metadata dosyaları** (`s3://lakehouse/`) | ❌ kapsam dışı | — | **S3'ün kendi çoğaltması** (müşteri S3'ü / FlashBlade replikasyonu) |
| **MinIO verisi (dev)** | ❌ | annotation `…backup-volumes-excludes: data` | DEV-ONLY, yedeklenmez |

> **Iceberg için kritik:** Polaris DB'yi geri yüklemek tabloları geri getirmez — tablo **dosyaları** S3'tedir.
> Tersi de doğrudur: S3 duruyor ama Polaris DB kayıpsa tablolar "yok" görünür. İkisi **aynı zaman penceresine**
> getirilmelidir; S3 çoğaltması gecikmeliyse PITR hedefini S3'ün tutarlı olduğu ana seçin.

## 2. Zamanlama, saklama, hedefler

| | CNPG (Barman) | Velero |
|---|---|---|
| Zamanlama | `0 0 2 * * *` — **6 alanlı** cron (saniye dâhil) → her gün 02:00 | `0 3 * * *` — 5 alanlı → her gün 03:00 |
| Nerede | `glue/values.yaml` → `backup.schedule` | `glue/values.yaml` → `velero.schedule` |
| Saklama | `backup.retentionPolicy: 30d` (ObjectStore **`spec.retentionPolicy`**) | `velero.ttl: 720h` (30 gün) |
| Hedef | `s3://<backup.s3.bucket>/cnpg` (ObjectStore `lakehouse-backups`) | `s3://<bucket>/velero` (BackupStorageLocation `default`) |
| Kimlik | Secret **`backup-s3-creds`** (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`) | dev: `velero-s3-creds` (chart üretir); prod: OADP'nin `cloud-credentials` Secret'ı |
| İlk yedek | `immediate: true` → ScheduledBackup yaratılır yaratılmaz | Schedule'ın ilk tetiklenmesinde |
| Nesneler | `ScheduledBackup/{polaris,keycloak,superset}-db-daily` | `Schedule/lakehouse-daily` |

`backup-s3-creds` prod'da **kurulumdan önce** yaratılır (dev'de MinIO şablonu üretir):
```bash
kubectl -n lakehouse create secret generic backup-s3-creds \
  --from-literal=AWS_ACCESS_KEY_ID=… --from-literal=AWS_SECRET_ACCESS_KEY=…
```

**Aynı bucket, iki prefix (dev varsayılanı):** Barman yalnız `cnpg/` altını yönetir (retention `30d`), Velero
`velero/` altını (`ttl 720h`); Barman retention'ı Velero yedeklerine **dokunmaz**. Prod'da müşteri S3'ünde
bucket lifecycle politikası kurulacaksa iki prefix **ayrı ayrı** ele alınmalıdır — aksi hâlde lifecycle,
Barman'ın hâlâ ihtiyaç duyduğu WAL'ları silip PITR zincirini kırabilir. Öneri: yedekler için **veri
bucket'ından ayrı bir bucket/hesap**.

Prod değerleri (`platform/values/glue.yaml`):
```yaml
backup:
  enabled: true
  s3: {endpoint: https://s3.musteri.example.com, bucket: lakehouse-backups, region: eu-central-1, secret: backup-s3-creds}
  schedule: "0 0 2 * * *"
  retentionPolicy: "30d"
velero: {enabled: true, namespace: openshift-adp}    # OADP kurulduktan SONRA açılır (§6)
```
`velero.enabled: false` iken `Schedule` **ve** Kafka/MinIO/CNPG `pgdata` dışlama annotation'ları hiç render edilmez; OADP
kurulmadan açılırsa glue "no matches for kind Schedule" ile Degraded olur.

## 3. Sağlık kontrolü (haftalık + her yükseltmeden önce)

```bash
# WAL arşivi çalışıyor mu (3 DB)
kubectl -n lakehouse get cluster -o custom-columns='DB:.metadata.name,ARCHIVING:.status.conditions[?(@.type=="ContinuousArchiving")].status'
# Son yedekler
kubectl -n lakehouse get backups.postgresql.cnpg.io -o wide
kubectl -n lakehouse get scheduledbackup
# Velero (TUZAK: kısa `backup` adı CNPG'ye çözülür -> TAM ad kullanın)
kubectl -n velero get backups.velero.io            # prod: -n openshift-adp
kubectl -n velero get backupstoragelocation        # PHASE=Available
kubectl -n velero get schedule lakehouse-daily
```
Sorun çıkarsa: `runbooks/troubleshooting.md#dr`.

## 4. CNPG PITR — geri yükleme prosedürü

Kaynak kümeye **dokunulmaz**: yedekten AYRI bir `Cluster` ayağa kaldırılır, doğrulanır, sonra uygulama ona
yönlendirilir. Şablon: `test/e2e/cnpg-restore.yaml`.

### 4.1 Son yedeğe dönüş

```bash
kubectl apply -f test/e2e/cnpg-restore.yaml           # Cluster/polaris-db-restore
kubectl -n lakehouse wait --for=condition=Ready cluster/polaris-db-restore --timeout=600s
kubectl -n lakehouse exec polaris-db-restore-1 -c postgres -- \
  psql -U postgres -d polaris -Atc "select count(*) from information_schema.tables where table_schema not in ('pg_catalog','information_schema')"
```
Polaris tabloları **`polaris_schema`** şemasındadır (`public` DEĞİL) — bu yüzden sayım sistem şemaları
dışındaki her şeyi sayar. Ölçüm (dev/kind): base backup 8–12 sn, restore kümesi `Ready` ~35 sn.

### 4.2 Zaman noktasına dönüş (PITR) — `recoveryTarget.targetTime`

```yaml
apiVersion: postgresql.cnpg.io/v1
kind: Cluster
metadata: {name: polaris-db-restore, namespace: lakehouse}
spec:
  instances: 1
  storage: {size: 10Gi}
  bootstrap:
    recovery:
      source: polaris-db
      recoveryTarget:
        targetTime: "2026-09-18 09:30:00+00"       # olaydan hemen ÖNCESİ (UTC)
  externalClusters:
  - name: polaris-db
    plugin:
      name: barman-cloud.cloudnative-pg.io
      # serverName = ORİJİNAL küme adı (ObjectStore destinationPath altındaki klasör)
      parameters: {barmanObjectName: lakehouse-backups, serverName: polaris-db}
```
**`plugins` alanı restore kümesinde BİLEREK YOKTUR**: bu küme WAL arşivlemez; arşivleseydi aynı `serverName`
altına yazıp kaynağın yedek zincirini bozardı. Restore kalıcı hâle gelecekse (§4.3) `plugins` eklenir.

### 4.3 Uygulamayı geri yüklenmiş DB'ye yönlendirme

| DB | Bağlantı nereden | Yapılacak |
|---|---|---|
| `polaris-db` | Polaris chart `persistence.type=relational-jdbc` → CNPG `-app` Secret'ı | Doğruladıktan sonra: eski `Cluster`'ı sil → restore kümesini **orijinal adla** (`polaris-db`) yeniden yarat (aynı `bootstrap.recovery` + `plugins`) → `kubectl -n lakehouse rollout restart deploy/polaris` |
| `keycloak-db` | `Keycloak` CR `db` bloğu (glue) | aynı desen → `kubectl -n lakehouse rollout restart statefulset/keycloak` |
| `superset-db` | `Superset` CR `metastore.uriFrom` | aynı desen → `kubectl -n lakehouse delete pod -l app.kubernetes.io/component=web-server` |

Ad değiştirerek (`polaris-db-restore`) kalıcı kullanmak **önerilmez**: Secret adları (`polaris-db-app`),
values kopyaları ve ObjectStore `serverName` klasörü orijinal ada bağlıdır.

### 4.4 Keycloak realm notu

`keycloak-db` restore'u realm'i, kullanıcıları, client'ları **veritabanı seviyesinde** geri getirir;
`KeycloakRealmImport` CR'ının yeniden koşmasına gerek yoktur (zaten mevcut realm'i güncellemez —
`runbooks/install.md` → "Realm değişikliği"). AD federasyonu varsa kullanıcılar zaten AD'den akar; yerel
rol/eşlemeler DB'den gelir. Realm'i sıfırdan kurmak **kullanıcıların UI'da elle yaptığı her şeyi siler**.

## 5. Velero — geri yükleme prosedürleri

Tüm komutlarda **tam nitelikli ad**: `backups.velero.io`, `restores.velero.io`, `podvolumebackups.velero.io`.

### 5.1 Tam namespace geri yükleme (felaket senaryosu)

```bash
kubectl -n velero get backups.velero.io               # hangi yedek?
kubectl apply -f - <<'EOF'
apiVersion: velero.io/v1
kind: Restore
metadata: {name: lakehouse-full, namespace: velero}
spec:
  backupName: lakehouse-daily-20260918030012
  includedNamespaces: [lakehouse]
  existingResourcePolicy: update
EOF
kubectl -n velero get restores.velero.io lakehouse-full -o jsonpath='{.status.phase}{"\n"}'
```
**Sıra:** önce operatörler + CRD'ler (`bootstrap/bootstrap.sh`), sonra namespace restore'u, en son Postgres
kümeleri **Barman PITR** ile (§4). ArgoCD `selfHeal` glue'nun ürettiği nesneleri zaten Git'ten geri koyar;
Velero restore'u asıl olarak **Git'te olmayanlar** için değerlidir: kullanıcı PVC'leri, Zeppelin not
defterleri, elle yaratılan Secret'lar (`keycloak-clients`, `trino-service-accounts`, `lakehouse-ca`,
`polaris-*` credential'ları).

### 5.2 Seçili kaynak geri yükleme

```bash
kubectl apply -f - <<'EOF'
apiVersion: velero.io/v1
kind: Restore
metadata: {name: secrets-only, namespace: velero}
spec:
  backupName: lakehouse-daily-20260918030012
  includedNamespaces: [lakehouse]
  includedResources: [secrets]
  existingResourcePolicy: update
EOF
```
Velero'da **ad ile filtre yoktur**; daraltma `includedResources` + `labelSelector` ile yapılır
(`test/e2e/velero-restore.yaml` bunu `e2e: dr-marker` etiketiyle gösterir).

### 5.3 PVC içeriği geri yükleme (fs-backup / Kopia)

fs-backup restore'u hedef PVC'yi **pod yaratılırken** doldurur → iş yükü önce durdurulur:

```bash
kubectl -n lakehouse scale deploy/zeppelin --replicas=0
kubectl -n lakehouse delete pvc zeppelin-data
kubectl apply -f - <<'EOF'
apiVersion: velero.io/v1
kind: Restore
metadata: {name: zeppelin-pvc, namespace: velero}
spec:
  backupName: lakehouse-daily-20260918030012
  includedNamespaces: [lakehouse]
  includedResources: [persistentvolumeclaims, persistentvolumes, pods]
EOF
kubectl -n lakehouse scale deploy/zeppelin --replicas=1
kubectl -n lakehouse exec deploy/zeppelin -- ls /data | head
```
**Kapsam notu:** Velero'da **ad ile filtre yoktur** ve `zeppelin-data` PVC'sinde etiket bulunmaz
(`glue/templates/zeppelin.yaml` PVC'ye yalnız annotation koyar) → yukarıdaki Restore namespace'teki **tüm**
PVC/pod'ları kapsar. Var olan nesnelere dokunulmaz (`existingResourcePolicy` varsayılanı: mevcut olanı atla),
yalnız silinmiş olan PVC yeniden yaratılır. Tek bir PVC'yi hedeflemek istiyorsanız yedek ALINMADAN ÖNCE
PVC'yi etiketleyin (`kubectl -n lakehouse label pvc zeppelin-data app=zeppelin`) ve Restore'a
`labelSelector: {matchLabels: {app: zeppelin}}` ekleyin.
Hangi hacimlerin gerçekten yedeklendiği:
```bash
kubectl -n velero get podvolumebackups.velero.io -l velero.io/backup-name=<yedek> \
  -o custom-columns='POD:.spec.pod.name,VOLUME:.spec.volume,PHASE:.status.phase'
```
`.spec.volume` **pod hacim adıdır**, PVC adı değil: zeppelin `data`→PVC `zeppelin-data`, hub `pvc`→
`hub-db-dir`, minio `data`→`minio-data`, CNPG `pgdata`, Kafka `data-0`.

### 5.4 kind sınırı ≠ prod (MUTLAKA okuyun)

kind'ın `local-path` provisioner'ı **hostPath PV** üretir ve Velero fs-backup hostPath hacimleri **atlar**
(`… is a hostPath volume which is not supported for pod volume backup, skipping` — canlı log). Sonuç:
**kind'da PVC içerikleri yedeklenmez**; yalnız `emptyDir` hacimleri PodVolumeBackup üretir (canlı: 22 PVB —
`strimzi-tmp`, `scratch-data`, `shm`, `plugins`, `temp-dir` …). Prod'da (OpenShift CSI depolama) PVC'ler
fs-backup'a girer; **tercih edilen** yol ise CSI snapshot + Data Mover'dır (`Backup.spec.snapshotVolumes` +
`spec.snapshotMoveData`; OADP `defaultPlugins` listesinde `csi`). **PVC içerik yedeği pre-ship OpenShift
ortamında canlı doğrulanacak açık kalemdir.**

### 5.5 İki tuzak (canlı yaşandı)

1. **`kubectl get backup` CNPG'ye çözülür.** Kümede hem `backups.postgresql.cnpg.io` hem `backups.velero.io`
   vardır; kısa ad sessizce CNPG'yi seçer, Velero yedeği "NotFound" görünür. Daima `backups.velero.io` yazın.
2. **`kubectl delete backups.velero.io <ad>` nesneyi S3'ten SİLMEZ.** Aynı adla ikinci yedek
   `backup already exists in object storage` ile `Failed` olur. Doğru silme:
   ```bash
   kubectl apply -f - <<'EOF'
   apiVersion: velero.io/v1
   kind: DeleteBackupRequest
   metadata: {name: eski-yedek-del, namespace: velero}
   spec: {backupName: eski-yedek}
   EOF
   kubectl -n velero get backups.velero.io eski-yedek    # kaybolmasını bekleyin
   ```
   (`velero backup delete <ad>` CLI'si aynı isteği üretir.)

## 6. OpenShift: OADP eşlemesi

Prod'da bu Velero chart'ı **KURULMAZ**; OADP operatörü (`openshift-adp`) aynı `velero.io/v1` CRD'lerini
sağlar. glue tarafında tek değişiklik: `velero: {enabled: true, namespace: openshift-adp}`
(`platform/values/glue.yaml`'da hazır, kapalı).

| `platform/values/velero-dev.yaml` | OADP `DataProtectionApplication` karşılığı |
|---|---|
| `configuration.backupStorageLocation[0]` | `spec.backupLocations[0].velero` (`provider`, `objectStorage.{bucket,prefix}`, `config.{region,s3Url,s3ForcePathStyle}`) |
| `initContainers[velero-plugin-for-aws]` | `spec.configuration.velero.defaultPlugins: [aws, openshift]` (+ CSI için `csi`) |
| `deployNodeAgent: true` | `spec.configuration.nodeAgent: {enable: true, uploaderType: kopia}` |
| `credentials.{useSecret,name,secretContents.cloud}` | `openshift-adp` ns'inde **`cloud-credentials`** Secret'ı (anahtar `cloud`, aynı INI biçimi) |
| `snapshotsEnabled: false`, `volumeSnapshotLocation: []` | CSI varsa `spec.snapshotLocations` doldurulur; yoksa boş |
| `configuration.defaultVolumesToFsBackup: true` | **DPA'da karşılığı yoktur** — bu alan `Backup`/`Schedule` spec'indedir; glue'nun `Schedule/lakehouse-daily`i onu zaten taşır |

```yaml
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
      config: {region: eu-central-1, s3Url: "https://s3.musteri.example.com", s3ForcePathStyle: "true"}
      credential: {name: cloud-credentials, key: cloud}
```
Alan adları kurulu **OADP sürümüyle** doğrulanır: `kubectl explain dataprotectionapplication.spec --recursive`.
DPA `Reconciled` olduktan sonra `kubectl -n openshift-adp get backupstoragelocation` → `Available`; ardından
glue'da `velero.enabled: true` açılır ve `Schedule/lakehouse-daily` `openshift-adp` ns'inde doğar.

## 7. Prova takvimi ve kabul kanıtı

| Sıklık | Prova | Kanıt |
|---|---|---|
| Her CI koşusu / her kurulum | CNPG yedek + restore, Velero backup + restore | `test/e2e/dr-path.sh` → `E2E F5 DR OK` (`runbooks/scripts/acceptance.sh` ile de koşar) |
| Haftalık | Arşiv + son yedek sağlığı | §3 komutları (yedekler için PrometheusRule YOKTUR → kontrol listesi elle) |
| **Çeyreklik** | **Polaris DB PITR provası**: dünkü bir zaman damgasına restore, tablo sayımı, restore kümesini silme | §4.2 |
| **Çeyreklik** | **Velero PVC restore provası** (Zeppelin ya da bir kullanıcı not defteri PVC'si) | §5.3 — kind'da anlamsız (§5.4), pre-ship/prod kümesinde |
| **Yılda bir** | **Tam namespace restore** provası (ayrı namespace/küme) + RTO/RPO ölçümü | §5.1 + bootstrap |

Ölçülen değerler (dev/kind, 2026-09-18): CNPG base backup **8–12 sn** · restore kümesi Ready **~35 sn** ·
Velero namespace yedeği **2 dk 22 sn – 2 dk 26 sn** (809–853 öğe, 22 PodVolumeBackup) · işaret ConfigMap
restore'u **8 sn** · `dr-path.sh` uçtan uca **3 dk 32 sn – 3 dk 58 sn**. Pratik **RPO**: CNPG için WAL
arşivleme gecikmesi (dakikalar), Velero için 24 saat (günlük Schedule). Prod RTO/RPO hedefleri müşteriyle
bu ölçümler üzerinden konuşulur.
