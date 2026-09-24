# Günlük ve haftalık kontroller

**Bu bölümde:** alarm beklemeden yapılan sabit kontroller — her sabah beş komut, haftada bir
beş komut daha; her birinin ne doğruladığı, beklenen çıktısı ve sapma görülünce nereye
bakılacağı.
**Süre:** günlük tur 5 dakika; haftalık tur 15 dakika.
**Gereken yetki:** `$LAKEHOUSE_NS` ad alanında okuma; yedek ad alanında (`openshift-adp`)
okuma. Hiçbir kontrol kümede değişiklik yapmaz.
**Nerede çalıştırılır:** `[bastion]` — `oc login` ile kümeye girilmiş yönetim makinesi.

> **Neden alarm yetmiyor?** Beş alarm kuralı "bozuldu" durumunu yakalar, "hiç çalışmıyor"
> durumunu değil: veri yokken kurallar ateşlenmez
> ([izleme-ve-alarmlar.md](izleme-ve-alarmlar.md) §3). Sessizce duran bir boru hattı ancak bu
> listeyle görülür. Yedekler için **hiç** alarm kuralı yoktur.

---

## 1. Değişkenleri yükleyin

`[bastion]`

```bash
cd ~/lakehouse
set -a; . install/lakehouse.env; set +a
echo "lakehouse=$LAKEHOUSE_NS"
```

**Beklenen çıktı** (örnek — kendi değerinizle):

```text
lakehouse=lakehouse
```

**Ters giderse:** boş satır görüyorsanız dosya yüklenmemiştir
([30-kurulum](../30-kurulum.md) §1).

---

## 2. Günlük tur — beş kontrol

Sabah ilk iş bu beş komut koşturulur. Hepsi salt okumadır.

### 2.1 Ateşlenen alarm var mı

Konsolda **Observe → Alerting** sayfasında ad alanına göre süzmek yeterlidir. Komut
satırından:

`[bastion]`

```bash
TOKEN=$(oc whoami -t)
HOST=$(oc -n openshift-monitoring get route thanos-querier -o jsonpath='{.spec.host}')
curl -sSk -H "Authorization: Bearer $TOKEN" "https://$HOST/api/v1/rules" \
  | jq -r '.data.groups[] | select(.name=="lakehouse") | .rules[] | "\(.name)\t\(.health)\t\(.state)"'
```

**Beklenen çıktı** (geliştirme kümesindeki Prometheus'tan alınmış gerçek çıktı — beşi de
`ok` ve `inactive`; yukarıdaki komut **üretim** yoludur, kind karşılığı hemen aşağıdadır):

```text
LakehouseConnectTaskFailed	ok	inactive
LakehouseSinkStalled	ok	inactive
LakehouseSilverMergeStale	ok	inactive
LakehouseSparkScheduledRunFailed	ok	inactive
LakehouseSparkRunTooLong	ok	inactive
```

**Geliştirme (kind) kümesinde:** `thanos-querier` Route'u **yoktur** (komut
`the server doesn't have a resource type "route"` verir). Aynı listeyi `monitoring` ad
alanındaki Prometheus'tan alın — yukarıdaki gerçek çıktı da oradan alınmıştır:

```bash
kubectl -n monitoring port-forward svc/monitoring-kube-prometheus-prometheus 9090:9090 &
curl -sS localhost:9090/api/v1/rules \
  | jq -r '.data.groups[] | select(.name=="lakehouse") | .rules[] | "\(.name)\t\(.health)\t\(.state)"'
```

**Ters giderse:** `firing` gören satır için [sorun-giderme.md](sorun-giderme.md) §4'teki
karşılığına gidin. Liste boş dönüyorsa **asıl sorun izlemededir**: kurallar toplanmıyor
demektir ([izleme-ve-alarmlar.md](izleme-ve-alarmlar.md) §2).

### 2.2 Bütün bağlayıcılar hazır mı

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" get kafkaconnector
oc -n "$LAKEHOUSE_NS" get kafkaconnector \
  -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.status.connectorStatus.connector.state}{"\t"}{range .status.connectorStatus.tasks[*]}{.state}{" "}{end}{"\n"}{end}'
oc -n "$LAKEHOUSE_NS" exec lakehouse-dual-role-0 -c kafka -- \
  bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --describe --group connect-sink-shop
```

**Beklenen çıktı** (kind kümesinde alınmış gerçek çıktı; sizde kaynak sayısına göre satır
sayısı değişir — **her satırın `READY` sütunu `True`, ikinci komutta hem bağlayıcı hem
görev durumu `RUNNING`, üçüncü komutta `LAG` sütunu `0` ya da azalıyor olmalıdır**):

```text
NAME         CLUSTER   CONNECTOR CLASS                                      MAX TASKS   READY
dbz-crm      connect   io.debezium.connector.mongodb.MongoDbConnector       1           True
dbz-shop     connect   io.debezium.connector.postgresql.PostgresConnector   1           True
sink-nginx   connect   org.apache.iceberg.connect.IcebergSinkConnector      1           True
sink-shop    connect   org.apache.iceberg.connect.IcebergSinkConnector      1           True

dbz-crm	RUNNING	RUNNING
dbz-shop	RUNNING	RUNNING
sink-nginx	RUNNING	RUNNING
sink-shop	RUNNING	RUNNING

GROUP              TOPIC               PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
connect-sink-shop  shop.public.orders  0          2               2               0
connect-sink-shop  shop.public.orders  1          3               3               0
connect-sink-shop  shop.public.orders  2          1               1               0
```

**Üç komut da gereklidir; biri ötekinin yerine geçmez:**

- `READY True` yalnız **Strimzi'nin bağlayıcıyı Connect'e yazabildiğini** söyler; görevin
  veri taşıdığını söylemez.
- İkinci komut görev durumunu verir; `FAILED` gören satır için
  [sorun-giderme.md §4.1](sorun-giderme.md#connect). **Dikkat:** Debezium kaynak görevi
  kaynağa ulaşamadığında sonsuz yeniden deneme döngüsüne girer ve durumu `RUNNING`
  **kalır** — bu yüzden üçüncü komut ve §2.1'deki alarm turu şarttır.
- Üçüncü komut boru hattının gerçekten aktığını gösterir. `LAG` sabit bir sayıda donmuşsa
  (azalmıyorsa) [sorun-giderme.md §4.2](sorun-giderme.md#sink). Sink adını kendi
  kaynağınıza göre yazın (`connect-sink-` + kaynak adı); kaynak sayısı çoksa hepsini
  `--all-groups` ile listeleyip `connect-sink` satırlarını süzün.

**Ters giderse:** `False` gören satır için
[sorun-giderme.md §4.1](sorun-giderme.md#connect). Beklediğiniz bir bağlayıcı **listede hiç
yoksa** kaynak tanımı eşitlenmemiştir
([yeni-kaynak-ve-pipeline.md](yeni-kaynak-ve-pipeline.md)). Bir kaynak görevi günlerdir
`RUNNING` ama hedef tabloya hiç satır gelmiyorsa Connect günlüğünde yeniden deneme
döngüsünü arayın:

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" logs -l strimzi.io/kind=KafkaConnect --tail=500 \
  | grep -i "failed to poll records"
```

### 2.3 Silver birleştirme koşuyor mu

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" get scheduledsparkapplication \
  -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.spec.suspend}{"\t"}{.status.lastRun}{"\n"}{end}'
```

**Beklenen çıktı:** alanlar sırasıyla ad, askıya alınma ve son koşudur. İkinci alan her
satırda **boş ya da `false`** olmalıdır — ürün bu alanı hiç yazmaz, bu yüzden taze kurulumda
**boş** basar; `false` değeri ancak bir askıya almadan sonra elle geri açıldığında görünür.
`silver-merge` satırının üçüncü alanı son cron aralığı içinde olmalıdır. Aşağıdaki çıktı kind
kümesinde alınmış **gerçek** çıktıdır ve tam da **istenmeyen** hâli gösterir — bir kabul
koşusundan sonra bütün işler askıda kalmıştır (`ornek-rapor-zamanli`
[yeni-spark-uygulamasi.md](yeni-spark-uygulamasi.md) sayfasının örnek işidir):

```text
maint-compact	true	null
maint-expire-orphan-ttl	true	null
maint-position-deletes	true	null
mongo-bronze	true	null
ornek-rapor-zamanli	true	
silver-merge	true	null
```

**Ters giderse:** ikinci alan **`true`** ise işleri geri açın —
[sorun-giderme.md §6.1](sorun-giderme.md#ssa-suspend). Son koşu eskiyse
[sorun-giderme.md §4.3](sorun-giderme.md#silver-merge).

### 2.4 Dün gece yedek alındı mı

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" get cluster \
  -o custom-columns='DB:.metadata.name,ARSIV:.status.conditions[?(@.type=="ContinuousArchiving")].status'
oc -n "$LAKEHOUSE_NS" get scheduledbackup
```

**Beklenen çıktı** (kind kümesinde alınmış gerçek çıktı; `demo-pg` yalnız geliştirme
kümesinin örnek kaynağıdır — sağlıklı bir kurulumda `LAST BACKUP` sütunu **24 saatten
küçük** olmalıdır, aşağıdaki değerler kümenin birkaç gündür beklemede olmasındandır):

```text
DB            ARSIV
demo-pg       True
keycloak-db   True
polaris-db    True
superset-db   True
NAME                AGE     CLUSTER       LAST BACKUP
keycloak-db-daily   5d16h   keycloak-db   3d9h
polaris-db-daily    5d16h   polaris-db    3d9h
superset-db-daily   5d16h   superset-db   3d9h
```

**Ters giderse:** `ARSIV` `False` ya da `LAST BACKUP` bir günden eskiyse
[yedek-ve-geri-donus.md](yedek-ve-geri-donus.md) §4 ve
[sorun-giderme.md](sorun-giderme.md) §8.

### 2.5 Diskler doluyor mu

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" get pvc
oc -n "$LAKEHOUSE_NS" exec deploy/zeppelin -c zeppelin -- df -h /data
```

**Beklenen çıktı** (kind kümesinde alınmış gerçek çıktı — ikinci komut; geliştirme kümesinde
bütün diskler düğümün tek dosya sisteminden gelir, üretimde her disk kendi boyutunu
gösterir):

```text
Filesystem      Size  Used Avail Use% Mounted on
/dev/vda4        93G   64G   29G  69% /data
```

Aynı bakışı Kafka ve veritabanı pod'ları için de yapın. **Eşik: %80.** Üzerine çıkan bir disk
için ya saklama süresi kısaltılır ya da disk büyütülür
([10-planlama](../10-planlama.md) §2).

**Ters giderse:** bir disk `Pending` kalıyorsa StorageClass yanlış ya da kota dolmuştur
([20-on-kosullar](../20-on-kosullar.md) madde 3).

---

## 3. Günlük turun özeti

| # | Kontrol | Sapma görülürse |
|---|---|---|
| 1 | Ateşlenen alarm yok, beş kural da `ok` | [sorun-giderme.md](sorun-giderme.md) §4 |
| 2 | Bağlayıcılar `READY True`, **görevleri `RUNNING`**, sink `LAG`'i `0` ya da azalıyor | `READY`/görev: [sorun-giderme.md §4.1](sorun-giderme.md#connect); sabit `LAG`: [§4.2](sorun-giderme.md#sink) |
| 3 | Zamanlı işlerin hiçbiri askıda değil, son koşu taze | [sorun-giderme.md §6.1](sorun-giderme.md#ssa-suspend) |
| 4 | Veritabanlarında arşiv `True`, son yedek 24 saatten yeni | [yedek-ve-geri-donus.md](yedek-ve-geri-donus.md) §4 |
| 5 | Hiçbir disk %80'in üzerinde değil | [10-planlama](../10-planlama.md) §2 |

---

## 4. Haftalık tur — beş kontrol

### 4.1 Ad alanı yedekleri tamamlanıyor mu

`[bastion]`

```bash
oc -n openshift-adp get backups.velero.io \
  -o custom-columns='AD:.metadata.name,FAZ:.status.phase,OGE:.status.progress.itemsBackedUp,BITIS:.status.completionTimestamp'
oc -n openshift-adp get backupstoragelocation
```

**Beklenen çıktı** (kind kümesinde alınmış gerçek çıktı; orada ad alanı `velero`, üretimde
`openshift-adp`'dir — son yedeğin `FAZ` değeri `Completed` ve tarihi son bir gün içinde
olmalıdır):

```text
AD                               FAZ         OGE   BITIS
lakehouse-daily-20260919070630   Completed   436   2026-09-19T07:09:39Z
lakehouse-daily-20260920095302   Completed   462   2026-09-20T10:00:36Z
lakehouse-daily-20260924081157   Completed   752   2026-09-24T08:14:33Z
NAME      PHASE       LAST VALIDATED   AGE     DEFAULT
default   Available   27s              5d16h   true
```

**Ters giderse:** `PartiallyFailed` ya da depolama konumu `Available` değilse
[yedek-ve-geri-donus.md](yedek-ve-geri-donus.md) §4 ve
[sorun-giderme.md](sorun-giderme.md) §8. **Kısa `backup` adını kullanmayın**, tam ad
zorunludur.

### 4.2 Bakım işleri çalışıyor mu, tablolar şişiyor mu

Bakım işleri küçük dosyaları birleştirir, silme kayıtlarını uygular ve eski anlık görüntüleri
temizler. Çalışmazlarsa sorgular yavaşlar ve depolama büyür.

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" get sparkapplication \
  -o custom-columns='AD:.metadata.name,DURUM:.status.applicationState.state,BITIS:.status.terminationTime' \
  | grep maint
```

**Beklenen çıktı** (örnek — son koşuların hepsi `COMPLETED`):

```text
maint-compact-1758699000             COMPLETED   2026-09-24T03:12:41Z
maint-expire-orphan-ttl-1758698100   COMPLETED   2026-09-24T03:05:18Z
maint-position-deletes-1758700800    COMPLETED   2026-09-24T03:40:02Z
```

Aynı hafta içinde en önemli tablolarda anlık görüntü ve dosya sayısına da bakın; sayılar
bakım koşularından sonra **düşmelidir** ([veri-metrikleri.md](veri-metrikleri.md) §3).

**Ters giderse:** hiç `maint-` satırı yoksa işler askıdadır (§2.3). `FAILED` varsa
[sorun-giderme.md §4.4](sorun-giderme.md#spark).

### 4.3 Sertifikaların süresi

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" get certificate \
  -o custom-columns='AD:.metadata.name,HAZIR:.status.conditions[0].status,BITIS:.status.notAfter,YENILEME:.status.renewalTime'
```

**Beklenen çıktı** (kind kümesinde alınmış gerçek çıktı — `YENILEME` tarihi **bugünden
sonra** ve `BITIS` tarihinden öncedir; yenileme kendiliğinden olur, açık oturumlar düşmez):

```text
AD             HAZIR   BITIS                  YENILEME
lakehouse-ca   True    2027-09-19T19:38:23Z   2027-07-21T19:38:23Z
trino-tls      True    2026-12-17T19:38:23Z   2026-12-02T19:38:23Z
```

**Ters giderse:** `HAZIR` `False` ya da `YENILEME` boşsa sertifika yöneticisi çalışmıyordur;
`oc -n cert-manager get pods` ile bakın. Yenileme tarihi geçtiği hâlde `BITIS` ilerlemiyorsa
sertifika yenilenmemiştir.

### 4.4 Jetonların süresi

Connect imajını iç kayıt defterine iten jeton **bir yıllıktır** ve dolduğunda sürüm
yükseltmesi durur.

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

**Beklenen çıktı** (örnek — tarih bugünden en az bir ay sonra olmalıdır):

```text
jeton bitisi: 2027-09-19 08:41:12+00:00
```

**Ters giderse:** bir aydan az kaldıysa jetonu yenileyin ([yukseltme.md](yukseltme.md) §6).
Komut hata verirse Secret farklı bir kimlik türüyle yaratılmış olabilir (kurumsal kayıt
defteri kullanıcı adı ve parolası); o durumda süre ilgili ekibe sorulur.

### 4.5 Küme Git'teki hâlinde mi

`[bastion]`

```bash
oc -n "$ARGOCD_NS" get applications
```

**Beklenen çıktı** (örnek — OpenShift'e özgü; **bütün** satırlarda iki sözcük de bu hâlini
almalıdır):

```text
NAME               SYNC STATUS   HEALTH STATUS
cert-manager       Synced        Healthy
cnpg               Synced        Healthy
glue               Synced        Healthy
jupyterhub         Synced        Healthy
polaris            Synced        Healthy
trino              Synced        Healthy
```

`OutOfSync` bir satır, kümede elle bir değişiklik yapıldığı ya da eşitlemenin takıldığı
anlamına gelir. Kendini iyileştirme açık olduğu için elle yapılan değişiklikler zaten geri
alınır; kalıcı `OutOfSync` gerçek bir sorundur
([değişiklik nasıl uygulanır](degisiklik-nasil-uygulanir.md) §7).

**Ters giderse:** `applications` diye bir kaynak yoksa kurulum ArgoCD'siz "helm modundadır";
orada bu kontrolün karşılığı `helm -n "$LAKEHOUSE_NS" list` çıktısının beklenen sürümleri
göstermesidir.

---

## 5. Haftalık turun özeti

| # | Kontrol | Sapma görülürse |
|---|---|---|
| 1 | Son ad alanı yedeği `Completed`, depolama konumu `Available` | [yedek-ve-geri-donus.md](yedek-ve-geri-donus.md) §4 |
| 2 | Bakım işleri `COMPLETED`; dosya ve anlık görüntü sayıları düşüyor | [veri-metrikleri.md](veri-metrikleri.md) §3 |
| 3 | Sertifikalar `True`, yenileme tarihi ileride | [sorun-giderme.md](sorun-giderme.md) §5 |
| 4 | Connect itme jetonunun bitişine bir aydan fazla var | [yukseltme.md](yukseltme.md) §6 |
| 5 | Bütün uygulamalar `Synced Healthy` | [değişiklik nasıl uygulanır](degisiklik-nasil-uygulanir.md) §7 |

---

## 6. Daha seyrek yapılanlar

| Sıklık | İş | Nerede |
|---|---|---|
| Çeyreklik | Veritabanı geri dönüş provası | [yedek-ve-geri-donus.md](yedek-ve-geri-donus.md) §5 |
| Çeyreklik | Disk geri yükleme provası | [yedek-ve-geri-donus.md](yedek-ve-geri-donus.md) §6.3 |
| Yılda bir | Tam ad alanı geri yükleme provası | [yedek-ve-geri-donus.md](yedek-ve-geri-donus.md) §6.1 |
| Sürüm çıktıkça | Bileşen yükseltmesi | [yukseltme.md](yukseltme.md) |
| Değişiklik oldukça | Grup ve yetki gözden geçirmesi | [kullanici-ve-yetki.md](kullanici-ve-yetki.md) |

---

## Kontrol listesi

- [ ] Günlük turun beş komutu bir yere (nöbet defteri, bilet sistemi) yazıldı ve her sabah
      koşturuluyor.
- [ ] Haftalık turun beş komutu takvimde sabit bir güne bağlandı.
- [ ] Sapma görüldüğünde gidilecek sayfa herkesçe biliniyor
      ([sorun-giderme.md](sorun-giderme.md)).
- [ ] Sertifika ve jeton bitiş tarihleri kurumun takvimine yazıldı.
- [ ] Çeyreklik ve yıllık provaların tarihleri planlandı.

## Sonraki bölüm

Sapma bulunduğunda: [sorun-giderme.md](sorun-giderme.md). Alarmların anlamı ve eşikleri:
[izleme-ve-alarmlar.md](izleme-ve-alarmlar.md). Yedeklerin ayrıntısı:
[yedek-ve-geri-donus.md](yedek-ve-geri-donus.md).
