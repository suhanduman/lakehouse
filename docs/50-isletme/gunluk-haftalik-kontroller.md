# Günlük ve haftalık kontroller

Bu sayfa, alarm beklemeden düzenli yapılan kontrolleri anlatır: her sabah beş kontrol,
haftada bir altı kontrol daha. Her kontrol için komut, beklenen çıktı ve bir sapma
görüldüğünde gidilecek yer verilir.

**Ne kadar sürer:** günlük tur 5 dakika, haftalık tur 15 dakika.

**Hangi yetkiler gerekir:** `$LAKEHOUSE_NS` ve `$ARGOCD_NS` ad alanlarında okuma; yedek ad
alanında (`openshift-adp`) okuma. Hiçbir kontrol kümede bir şey değiştirmez.

**Komutlar nerede çalışır:** `[bastion]` — `oc login` ile kümeye bağlı, Git deposunun
kopyasının (`~/lakehouse`) bulunduğu yönetim makinesi.

> **Alarmlar neden yetmez?** Alarmlar bir şeyin **bozulduğunu** yakalar, **hiç
> çalışmadığını** yakalamaz: ölçülecek veri yoksa alarm da çıkmaz
> ([izleme-ve-alarmlar.md](izleme-ve-alarmlar.md) §3). Sessizce durmuş bir veri akışı
> ancak bu kontrollerle görülür. Yedekler için hiç alarm yoktur.

---

## 1. Değişkenleri yükleyin

Her yeni terminalde bir kez çalıştırın.

`[bastion]`

```bash
cd ~/lakehouse
set -a; . install/lakehouse.env; set +a
echo "argocd=$ARGOCD_NS lakehouse=$LAKEHOUSE_NS"
```

**Beklenen çıktı** (kendi değerlerinizle):

```text
argocd=openshift-gitops lakehouse=lakehouse
```

**Ters giderse:** eşittir işaretlerinden sonrası boşsa `install/lakehouse.env` dosyası
yüklenmemiştir ([30-kurulum](../30-kurulum.md) §1).

---

## 2. Günlük tur — beş kontrol

Her sabah ilk iş bu beş kontrolü yapın. Komutların hepsi yalnız okur.

### 2.1 Çalan alarm var mı

En kolayı OpenShift konsolunda **Observe → Alerting** sayfasıdır; ad alanına göre
süzün. Komut satırından:

`[bastion]`

```bash
TOKEN=$(oc whoami -t)
HOST=$(oc -n openshift-monitoring get route thanos-querier -o jsonpath='{.spec.host}')
curl -sSk -H "Authorization: Bearer $TOKEN" "https://$HOST/api/v1/rules" \
  | jq -r '.data.groups[] | select(.name=="lakehouse") | .rules[] | "\(.name)\t\(.health)\t\(.state)"'
```

**Beklenen çıktı:** altı alarmın hepsi `ok` ve `inactive` (çalmıyor):

```text
LakehouseConnectTaskFailed	ok	inactive
LakehouseDlqRecords	ok	inactive
LakehouseSilverMergeStale	ok	inactive
LakehouseSinkStalled	ok	inactive
LakehouseSparkRunTooLong	ok	inactive
LakehouseSparkScheduledRunFailed	ok	inactive
```

**Ters giderse:**

- Bir satırda `firing` görürseniz o alarmın çözümüne gidin
  ([sorun-giderme.md](sorun-giderme.md) §4).
- Komut hiçbir satır yazmazsa sorun izlemenin kendisindedir: alarm kuralları
  okunmuyordur ([izleme-ve-alarmlar.md](izleme-ve-alarmlar.md) §2).

### 2.2 Bağlayıcılar çalışıyor ve veri akıyor mu

Üç komut çalıştırın. Her biri farklı bir şeyi gösterir; biri ötekinin yerine geçmez.

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" get kafkaconnector
oc -n "$LAKEHOUSE_NS" get kafkaconnector \
  -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.status.connectorStatus.connector.state}{"\t"}{range .status.connectorStatus.tasks[*]}{.state}{" "}{end}{"\n"}{end}'
scripts/kafka-cli.sh kafka-consumer-groups.sh --describe --group connect-sink-shop
```

Son komuttaki `connect-sink-shop`, `shop` kaynağının Iceberg yazıcısıdır; kendi kaynak
adınızı yazın (`connect-sink-` + kaynak adı).

**Beklenen çıktı** (satır sayısı kaynak sayınıza göre değişir; aşağıdaki çıktının ortası
ve sağdaki sütunlar kısaltılmıştır):

```text
NAME              CLUSTER   CONNECTOR CLASS                                      MAX TASKS   READY
dbz-crm           connect   io.debezium.connector.mongodb.MongoDbConnector       1           True
dbz-erp           connect   io.debezium.connector.postgresql.PostgresConnector   1           True
dbz-fin           connect   io.debezium.connector.sqlserver.SqlServerConnector   1           True
…
sink-shop         connect   org.apache.iceberg.connect.IcebergSinkConnector      1           True

dbz-crm	RUNNING	RUNNING
dbz-erp	RUNNING	RUNNING
dbz-fin	RUNNING	RUNNING
…
sink-shop	RUNNING	RUNNING

GROUP             TOPIC              PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG  …
connect-sink-shop shop.public.orders 0          2               2               0    …
connect-sink-shop shop.public.orders 1          -               0               -    …
connect-sink-shop shop.public.orders 2          1               1               0    …
```

Neye bakılır:

- **Birinci komut:** her satırın `READY` sütunu `True` olmalıdır. Bu yalnız bağlayıcının
  kurulduğunu söyler, veri taşıdığını söylemez.
- **İkinci komut:** hem bağlayıcı hem görev `RUNNING` olmalıdır.
- **Üçüncü komut:** `LAG` sütunu `0` olmalı ya da azalıyor olmalıdır. `LAG`, yazıcının
  henüz işlemediği kayıt sayısıdır. Hiç kayıt gelmemiş bölümlerde `-` görünür; bu
  normaldir.

**Ters giderse:**

- `READY` `False` ya da görev `FAILED` ise:
  [sorun-giderme.md §4.1](sorun-giderme.md#connect).
- `LAG` her gün aynı sayıda duruyor ve azalmıyorsa:
  [sorun-giderme.md §4.2](sorun-giderme.md#sink).
- Beklediğiniz bir bağlayıcı listede hiç yoksa kaynak tanımı kümeye ulaşmamıştır
  ([yeni-kaynak-ve-pipeline.md](yeni-kaynak-ve-pipeline.md) §8).
- Bir kaynak görevi `RUNNING` görünüyor ama günlerdir hiç veri gelmiyorsa bağlayıcı
  kaynağa bağlanamayıp sürekli yeniden deniyor olabilir. Debezium bu durumda
  `RUNNING` kalır. Connect günlüğünde bakın:

  `[bastion]`

  ```bash
  oc -n "$LAKEHOUSE_NS" logs -l strimzi.io/kind=KafkaConnect --tail=500 \
    | grep -i "failed to poll records"
  ```

  Satır çıkarsa kaynak veritabanına erişimi ve parolayı kontrol edin
  ([sorun-giderme.md §4.1](sorun-giderme.md#connect)).

### 2.3 Zamanlı Spark işleri çalışıyor mu

Silver tablolarını (`silver-merge`) ve bakım işlerini (`maint-…`) zamanlı Spark işleri
üretir.

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" get scheduledsparkapplication \
  -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.spec.suspend}{"\t"}{.status.lastRun}{"\n"}{end}'
oc -n "$LAKEHOUSE_NS" get pod -l app=maven-proxy
```

Birinci komutun sütunları: iş adı, askıda mı, son çalışma zamanı (UTC). İkinci komut,
Spark işlerinin kütüphanelerini aldığı Maven önbelleğinin pod'unu gösterir.

**Beklenen çıktı** (`gold-kabul` gibi kendi işleriniz de listede görünür):

```text
gold-kabul	false	2026-11-11T16:50:00Z
maint-compact	false	2026-11-11T12:00:00Z
maint-expire-orphan-ttl	false	2026-11-11T03:30:00Z
maint-position-deletes	false	2026-11-11T17:00:00Z
mongo-bronze	false	2026-11-11T17:15:00Z
silver-merge	false	2026-11-11T17:15:00Z
NAME                           READY   STATUS    RESTARTS   AGE
maven-proxy-646879d475-z4sm2   1/1     Running   0          5h2m
```

Neye bakılır:

- İkinci sütun her satırda `false` ya da boş olmalıdır.
- `silver-merge`'in son çalışması son 15 dakika içinde olmalıdır. Saatlik, altı saatlik
  ve günlük işlerde son çalışma o kadar eski olabilir.
- `maven-proxy` pod'u `Running` olmalıdır.

**Ters giderse:**

- İkinci sütun `true` ise iş askıdadır: hiç çalışmaz ve hata da vermez. Askıdan
  çıkarın: [sorun-giderme.md §6.1](sorun-giderme.md#ssa-suspend).
- `silver-merge`'in son çalışması eskiyse:
  [sorun-giderme.md §4.3](sorun-giderme.md#silver-merge).
- `maven-proxy` çalışmıyorsa Spark işleri kütüphanelerini alamaz:
  [sorun-giderme.md §6.2](sorun-giderme.md#maven).

### 2.4 Dün gece yedek alındı mı

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" get cluster \
  -o custom-columns='DB:.metadata.name,ARSIV:.status.conditions[?(@.type=="ContinuousArchiving")].status'
oc -n "$LAKEHOUSE_NS" get scheduledbackup
```

**Beklenen çıktı:**

```text
DB            ARSIV
keycloak-db   True
polaris-db    True
superset-db   True
NAME                AGE    CLUSTER       LAST BACKUP
keycloak-db-daily   3d5h   keycloak-db   15h
polaris-db-daily    3d5h   polaris-db    15h
superset-db-daily   3d5h   superset-db   15h
```

`ARSIV` her satırda `True` olmalı, `LAST BACKUP` 24 saatten küçük olmalıdır.

**Ters giderse:** `ARSIV` `False` ya da son yedek bir günden eskiyse:
[yedek-ve-geri-donus.md](yedek-ve-geri-donus.md) §4 ve
[sorun-giderme.md](sorun-giderme.md) §8.

### 2.5 Diskler doluyor mu

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" get pvc
oc -n "$LAKEHOUSE_NS" exec deploy/zeppelin -c zeppelin -- df -h /data
```

Birinci komut ad alanındaki bütün diskleri ve boyutlarını, ikincisi bir diskin doluluğunu
gösterir.

**Beklenen çıktı** (ikinci komut):

```text
Filesystem                                     Size  Used Avail Use% Mounted on
/dev/mapper/3624a937062c8a0bd13094f9e00011814  9.8G   11M  9.8G   1% /data
```

`Use%` sütunu **%80'in altında** olmalıdır. Aynı bakışı Kafka ve veritabanı pod'ları için
de yapın.

**Ters giderse:**

- %80'in üzerindeki bir disk için saklama süresini kısaltın ya da diski büyütün
  ([10-planlama](../10-planlama.md) §2).
- Birinci komutta `Pending` görünen bir disk varsa StorageClass yanlış ya da kota
  dolmuştur ([20-on-kosullar](../20-on-kosullar.md) madde 3).

---

## 3. Günlük turun özeti

| # | Kontrol | Sapma görülürse |
|---|---|---|
| 1 | Hiçbir alarm çalmıyor, altı kural `ok` | [sorun-giderme.md](sorun-giderme.md) §4 |
| 2 | Bağlayıcılar `READY True`, görevler `RUNNING`, `LAG` `0` ya da azalıyor | görev: [sorun-giderme.md §4.1](sorun-giderme.md#connect); `LAG`: [§4.2](sorun-giderme.md#sink) |
| 3 | Zamanlı işlerin hiçbiri askıda değil, `silver-merge` taze, `maven-proxy` çalışıyor | [sorun-giderme.md §6.1](sorun-giderme.md#ssa-suspend) |
| 4 | Veritabanlarında arşiv `True`, son yedek 24 saatten yeni | [yedek-ve-geri-donus.md](yedek-ve-geri-donus.md) §4 |
| 5 | Hiçbir disk %80'in üzerinde değil | [10-planlama](../10-planlama.md) §2 |

---

## 4. Haftalık tur — altı kontrol

### 4.1 Ad alanı yedekleri tamamlanıyor mu

`[bastion]`

```bash
oc -n openshift-adp get backups.velero.io \
  -o custom-columns='AD:.metadata.name,FAZ:.status.phase,OGE:.status.progress.itemsBackedUp,BITIS:.status.completionTimestamp'
oc -n openshift-adp get backupstoragelocation
```

**Beklenen çıktı** (örnek):

```text
AD                               FAZ         OGE   BITIS
lakehouse-daily-20260919070630   Completed   436   2026-09-19T07:09:39Z
lakehouse-daily-20260920095302   Completed   462   2026-09-20T10:00:36Z
lakehouse-daily-20260924081157   Completed   752   2026-09-24T08:14:33Z
NAME      PHASE       LAST VALIDATED   AGE     DEFAULT
default   Available   27s              5d16h   true
```

Son yedeğin `FAZ` değeri `Completed` olmalı ve tarihi son bir gün içinde olmalıdır.
Depolama konumunun `PHASE` değeri `Available` olmalıdır.

**Ters giderse:**

- `PartiallyFailed` ya da `Failed` görürseniz, veya depolama konumu `Available` değilse:
  [yedek-ve-geri-donus.md](yedek-ve-geri-donus.md) §4 ve
  [sorun-giderme.md](sorun-giderme.md) §8.
- `the server doesn't have a resource type "backups"` görürseniz OADP kurulu değildir
  ([20-on-kosullar](../20-on-kosullar.md) madde 9).
- Komutta kısa `backup` adı yerine tam adı (`backups.velero.io`) kullanın; kısa ad başka
  bir kaynakla karışabilir.

### 4.2 Bakım işleri başarıyla bitiyor mu

Bakım işleri küçük dosyaları birleştirir, silinen kayıtları tablolara uygular ve eski
sürümleri temizler. Çalışmazlarsa sorgular yavaşlar ve depolama büyür.

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" get sparkapplication \
  -o custom-columns='AD:.metadata.name,DURUM:.status.applicationState.state,BITIS:.status.terminationTime' \
  | grep maint
```

**Beklenen çıktı:** son çalışmaların hepsi `COMPLETED`:

```text
maint-compact-1794333600001357863            COMPLETED   2026-11-10T18:00:45Z
maint-compact-1794376800001522530            COMPLETED   2026-11-11T06:00:45Z
maint-position-deletes-1794405600002077480   COMPLETED   2026-11-11T14:01:01Z
maint-position-deletes-1794409200001741459   COMPLETED   2026-11-11T15:00:57Z
maint-position-deletes-1794412800001246628   COMPLETED   2026-11-11T16:00:59Z
maint-position-deletes-1794416400001330020   COMPLETED   2026-11-11T17:01:03Z
```

Bakımın etkisini **Tablo sağlığı** panosunda görürsünüz: büyük ve sık güncellenen
tablolarda veri dosyası sayısı bakımdan sonra düşmelidir
([veri-metrikleri.md](veri-metrikleri.md) §4).

**Ters giderse:**

- Hiç `maint-` satırı yoksa işler askıda olabilir (§2.3).
- `FAILED` görürseniz: [sorun-giderme.md §4.4](sorun-giderme.md#spark).

### 4.3 Sertifikaların süresi

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" get certificate \
  -o custom-columns='AD:.metadata.name,HAZIR:.status.conditions[0].status,BITIS:.status.notAfter,YENILEME:.status.renewalTime'
```

**Beklenen çıktı:**

```text
AD          HAZIR   BITIS                  YENILEME
trino-tls   True    2027-02-06T16:02:51Z   2027-01-22T16:02:51Z
```

`HAZIR` `True` olmalı, `YENILEME` tarihi bugünden sonra ve `BITIS` tarihinden önce
olmalıdır. Yenileme kendiliğinden yapılır; açık oturumlar düşmez.

**Ters giderse:**

- `HAZIR` `False` ya da `YENILEME` boşsa sertifika yöneticisi çalışmıyordur:
  `oc -n cert-manager get pods`.
- `YENILEME` tarihi geçtiği hâlde `BITIS` ilerlemiyorsa sertifika yenilenmemiştir:
  [sorun-giderme.md](sorun-giderme.md) §5.

### 4.4 Connect imajı jetonunun süresi

Connect imajını kümenin iç registry'sine gönderen jetonun süresi sınırlıdır. Süresi
dolarsa ürün yükseltmesinde Connect imajı yeniden üretilemez.

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" get secret connect-push \
  -o jsonpath='{.data.\.dockerconfigjson}' | base64 -d \
  | python3 -c 'import base64, json, sys, datetime
cfg = json.load(sys.stdin)["auths"]
tok = list(cfg.values())[0]["password"]
t = tok.split(".")[1]; t += "=" * (-len(t) % 4)
print("jeton bitisi:", datetime.datetime.fromtimestamp(
    json.loads(base64.urlsafe_b64decode(t))["exp"], datetime.timezone.utc))'
```

**Beklenen çıktı** (tarih bugünden en az bir ay sonra olmalıdır):

```text
jeton bitisi: 2027-11-08 11:36:57+00:00
```

**Ters giderse:**

- Bir aydan az kaldıysa jetonu yenileyin ([yukseltme.md](yukseltme.md) §6).
- Komut hata verirse Secret bir kullanıcı adı ve parolayla yaratılmıştır (kurumsal
  registry); süreyi registry'yi yöneten ekibe sorun.

### 4.5 Küme Git'teki hâlinde mi

`[bastion]`

```bash
oc -n "$ARGOCD_NS" get applications
```

**Beklenen çıktı:** her satırda `Synced` ve `Healthy`:

```text
NAME                SYNC STATUS   HEALTH STATUS
cert-manager        Synced        Healthy
cnpg                Synced        Healthy
cnpg-barman         Synced        Healthy
custom              Synced        Healthy
glue                Synced        Healthy
jupyterhub          Synced        Healthy
keycloak-operator   Synced        Healthy
lakehouse-root      Synced        Healthy
polaris             Synced        Healthy
spark-operator      Synced        Healthy
strimzi             Synced        Healthy
superset-operator   Synced        Healthy
trino               Synced        Healthy
```

`OutOfSync` görünen bir satır, kümede elle bir değişiklik yapıldığını ya da uygulamanın
takıldığını gösterir. ArgoCD elle yapılan değişiklikleri birkaç dakikada geri aldığı
için uzun süre `OutOfSync` kalan bir satır gerçek bir sorundur.

**Ters giderse:**

- Uzun süre `OutOfSync` ya da `Degraded` kalan bir satır için:
  [değişiklik nasıl uygulanır](degisiklik-nasil-uygulanir.md) §8.
- `the server doesn't have a resource type "applications"` görürseniz `$ARGOCD_NS`
  değişkeni yanlış ad alanını gösteriyordur.

### 4.6 Belgesiz elle yaratılmış Secret var mı

Elle yaratılan her Secret, yaratıldığı belgenin adımını gösteren bir etiket taşır
([değişiklik nasıl uygulanır](degisiklik-nasil-uygulanir.md) §7). Etiketsiz bir Secret,
belgelerin dışında yaratılmıştır ve silinirse nasıl geri getirileceği bilinmez.

`[bastion]`

```bash
scripts/nesne-kaynagi.sh --liste secret | awk '$2=="ETİKETSİZ"'
```

**Beklenen çıktı:** hiçbir satır yoktur.

**Ters giderse:** listede bir Secret görürseniz kimin ve ne için yarattığını ekibinizle
doğrulayın. Kullanılıyorsa onu yaratan adımı belgeleyin ve etiketleyin; kullanılmıyorsa
silin.

---

## 5. Haftalık turun özeti

| # | Kontrol | Sapma görülürse |
|---|---|---|
| 1 | Son ad alanı yedeği `Completed`, depolama konumu `Available` | [yedek-ve-geri-donus.md](yedek-ve-geri-donus.md) §4 |
| 2 | Bakım işleri `COMPLETED`; dosya sayıları bakımdan sonra düşüyor | [veri-metrikleri.md](veri-metrikleri.md) §4 |
| 3 | Sertifikalar `True`, yenileme tarihi ileride | [sorun-giderme.md](sorun-giderme.md) §5 |
| 4 | Connect imajı jetonunun bitişine bir aydan fazla var | [yukseltme.md](yukseltme.md) §6 |
| 5 | Bütün uygulamalar `Synced Healthy` | [değişiklik nasıl uygulanır](degisiklik-nasil-uygulanir.md) §8 |
| 6 | Etiketsiz elle yaratılmış Secret yok | [değişiklik nasıl uygulanır](degisiklik-nasil-uygulanir.md) §7 |

---

## 6. Daha seyrek yapılanlar

| Sıklık | İş | Nerede |
|---|---|---|
| Üç ayda bir | Veritabanı geri dönüş denemesi | [yedek-ve-geri-donus.md](yedek-ve-geri-donus.md) §5 |
| Üç ayda bir | Disk geri yükleme denemesi | [yedek-ve-geri-donus.md](yedek-ve-geri-donus.md) §6.3 |
| Yılda bir | Ad alanının tamamını geri yükleme denemesi | [yedek-ve-geri-donus.md](yedek-ve-geri-donus.md) §6.1 |
| Yeni sürüm çıktıkça | Ürün yükseltmesi | [yukseltme.md](yukseltme.md) |
| Kadro değiştikçe | Grup ve yetki gözden geçirmesi | [kullanici-ve-yetki.md](kullanici-ve-yetki.md) |

---

## Kontrol listesi

- [ ] Günlük turun beş kontrolü bir yere (nöbet defteri, iş takip sistemi) yazıldı ve her
      sabah yapılıyor.
- [ ] Haftalık turun altı kontrolü takvimde sabit bir güne bağlandı.
- [ ] Sapma görüldüğünde gidilecek sayfayı herkes biliyor
      ([sorun-giderme.md](sorun-giderme.md)).
- [ ] Sertifika ve jeton bitiş tarihleri kurumun takvimine yazıldı.
- [ ] Üç aylık ve yıllık geri yükleme denemelerinin tarihleri planlandı.

## Sonraki bölüm

Bir sapma bulunduğunda: [sorun-giderme.md](sorun-giderme.md). Alarmların anlamı:
[izleme-ve-alarmlar.md](izleme-ve-alarmlar.md). Yedeklerin ayrıntısı:
[yedek-ve-geri-donus.md](yedek-ve-geri-donus.md).
