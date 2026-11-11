# 90 — `oc` hızlı başvuru

Lakehouse'u işletirken en sık gereken komutlar bu sayfadadır. Her komut için ne zaman
kullanılacağı, komutun kendisi ve ne görmeniz gerektiği yazar. Amaç, bir belirti
karşısında hangi komutu çalıştıracağınızı aramadan bulmaktır.

**Ne kadar sürer:** okuması 10 dakika.

**Hangi yetkiler gerekir:** `$LAKEHOUSE_NS` ve `$ARGOCD_NS` ad alanlarında okuma. 12, 13
ve 14 numaralı komutlar yazma yetkisi ister.

**Komutlar nerede çalışır:** `[bastion]` — `oc login` ile kümeye bağlı ve Git deposunun
kopyasının (`~/lakehouse`) bulunduğu yönetim makinesi.

Bütün komutlar `$LAKEHOUSE_NS` ve `$ARGOCD_NS` değişkenlerini kullanır. Her yeni
terminalde önce şunu çalıştırın:

`[bastion]`

```bash
cd ~/lakehouse
set -a; . install/lakehouse.env; set +a
```

> **Bu sayfa teşhis etmez, komut verir.** Belirtiden yola çıkan teşhis tablosu
> [50-isletme/sorun-giderme.md](../50-isletme/sorun-giderme.md) §3'tedir.

---

## 1. Her şey Git'teki hâliyle mi

**Ne zaman:** her değişiklikten sonra ve "kümede neden hâlâ eski hâl var?" sorusunda ilk
komut.

`[bastion]`

```bash
oc -n "$ARGOCD_NS" get applications
```

**Beklenen çıktı:** bütün satırlarda `Synced` ve `Healthy`.

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

`OutOfSync`, Git'te henüz uygulanmamış bir değişiklik olduğunu gösterir. `Degraded`, kurulan
nesnelerden birinin sağlıksız olduğunu gösterir
([50-isletme/degisiklik-nasil-uygulanir.md](../50-isletme/degisiklik-nasil-uygulanir.md) §8).

---

## 2. Bir pod neden başlamıyor

**Ne zaman:** pod `Pending`, `CreateContainerConfigError`, `ImagePullBackOff` ya da
`CrashLoopBackOff` durumundayken. Asıl cevap çıktının sonundaki `Events` bölümündedir.

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" describe pod -l app.kubernetes.io/name=trino | tail -30
```

**Ne görmelisiniz:** `Events` bölümünde okunur bir neden: `secret "…" not found`,
`pod has unbound immediate PersistentVolumeClaims`, `Failed to pull image …` gibi.

---

## 3. Bir bileşenin günlüğü

**Ne zaman:** pod ayakta ama iş yapmıyorsa.

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" logs deploy/trino-coordinator --tail=50
```

**Ne görmelisiniz:** uygulamanın kendi günlük satırları. Çıktı boşsa konteyner hiç
başlamamıştır; 2 numaraya dönün.

Bazı ayrıntılar:

- Kafka Connect birden fazla pod'da çalışır. Bir bağlayıcının günlüğü herhangi birinde
  olabilir, bu yüzden hepsini birlikte okuyun:

  ```bash
  oc -n "$LAKEHOUSE_NS" logs -l strimzi.io/kind=KafkaConnect --tail=200 --prefix
  ```

  `--prefix` her satırın başına hangi pod'dan geldiğini yazar.
- `--previous` çöküp yeniden başlayan bir konteynerin bir önceki günlüğünü verir.
- `-c` ile pod'daki belirli bir konteyner seçilir (ör. `-c zeppelin`).

---

## 4. Güncelleme bitti mi

**Ne zaman:** bir ayar değişikliğinden ya da parola yenilemesinden sonra yeni pod'un
gerçekten açıldığını beklemek için. Komut iş bitene kadar bekler.

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" rollout status deploy/superset-web-server --timeout=600s
```

**Beklenen çıktı:**

```text
deployment "superset-web-server" successfully rolled out
```

Zaman aşımı, yeni pod'un açılamadığını gösterir; 2 numaraya bakın.

---

## 5. Bağlayıcılar, zamanlı Spark işleri, veritabanları

**Ne zaman:** "veri akmıyor", "gece işi çalışmamış" ya da "veritabanı sağlıklı mı"
sorularında. Üçü tek bakışta görülür.

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" get kafkaconnector
oc -n "$LAKEHOUSE_NS" get scheduledsparkapplication
oc -n "$LAKEHOUSE_NS" get cluster
```

**Beklenen çıktı** (örnek, seçilmiş satırlar; bağlayıcı adları sizin
kaynaklarınızdır):

```text
NAME              CLUSTER   CONNECTOR CLASS                                      MAX TASKS   READY
dbz-erp           connect   io.debezium.connector.postgresql.PostgresConnector   1           True
dbz-fin           connect   io.debezium.connector.sqlserver.SqlServerConnector   1           True
sink-erp          connect   org.apache.iceberg.connect.IcebergSinkConnector      1           True
sink-fin          connect   org.apache.iceberg.connect.IcebergSinkConnector      1           True
sink-nginx        connect   org.apache.iceberg.connect.IcebergSinkConnector      1           True

NAME                      SCHEDULE        TIMEZONE   SUSPEND   LAST RUN   LAST RUN NAME                                 AGE
maint-compact             0 */6 * * *                false     5h34m      maint-compact-1794398400001603671             3d3h
maint-expire-orphan-ttl   30 3 * * *                 false     14h        maint-expire-orphan-ttl-1794367800002123397   3d3h
maint-position-deletes    0 * * * *                  false     34m        maint-position-deletes-1794416400001330020    3d3h
mongo-bronze              */5 * * * *                false     4m20s      mongo-bronze-1794418200001993673              2d21h
silver-merge              */15 * * * *               false     4m20s      silver-merge-1794418200001887874              3d3h

NAME          AGE    INSTANCES   READY   STATUS                     PRIMARY
keycloak-db   3d5h   2           2       Cluster in healthy state   keycloak-db-1
polaris-db    3d5h   2           2       Cluster in healthy state   polaris-db-1
superset-db   3d5h   2           2       Cluster in healthy state   superset-db-1
```

Üç şey bulgudur: `READY` sütunu `False` olan bir bağlayıcı, `SUSPEND` sütunu `true` olan
bir zamanlı iş ve `READY` sayısı `INSTANCES` sayısına eşit olmayan bir veritabanı.

---

## 6. Pod'un içinde komut çalıştırmak

**Ne zaman:** bir dosyanın gerçekten bağlandığını, bir ortam değişkeninin geldiğini ya da
bir adrese erişilebildiğini konteynerin **içinden** görmek gerektiğinde.

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" exec deploy/zeppelin -c zeppelin -- ls /etc/lakehouse-ca
```

**Beklenen çıktı:**

```text
tls.crt
```

`No such file or directory`, dosyanın bağlanmadığını gösterir. Komuta heredoc ya da boru
(`|`) ile veri gönderiyorsanız `exec`'ten sonra `-i` yazın.

---

## 7. Küme içindeki bir servise yönetim makinesinden bağlanmak

**Ne zaman:** dışarıya açık adresi olmayan bir servise (ör. Polaris API'si, PostgreSQL)
yönetim makinesinden bağlanmak gerektiğinde. Komut açık kalır; işiniz bitince `Ctrl+C`
ile kapatın.

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" port-forward svc/polaris 8181:8181
```

**Beklenen çıktı:**

```text
Forwarding from 127.0.0.1:8181 -> 8181
```

`unable to forward port because pod is not running`, servisin arkasında çalışan bir pod
olmadığını gösterir.

---

## 8. Ad alanında en son ne oldu

**Ne zaman:** "az önce bir şey bozuldu" durumunda. Zamana göre sıralı olaylar çoğu zaman
nedeni doğrudan söyler.

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" get events --sort-by=.lastTimestamp | tail -20
```

**Ne görmelisiniz:** son yirmi olay. Önce `Warning` tipli satırları okuyun
(`FailedScheduling`, `BackOff`, `Unhealthy`). Spark işlerinin `SparkDriverCompleted` gibi
`Normal` satırları her koşuda yazılır ve olağandır.

---

## 9. Bellek ve işlemci kullanımı

**Ne zaman:** bir pod'un bellek yetmediği için kapandığından (`OOMKilled`) ya da düğümde
yer kalmadığından şüphelendiğinizde.

`[bastion]`

```bash
oc adm top pods -n "$LAKEHOUSE_NS" -l strimzi.io/kind=KafkaConnect
```

**Beklenen çıktı** (değerler sizde farklıdır; `-l …` kısmını silerseniz bütün pod'lar
listelenir):

```text
NAME                CPU(cores)   MEMORY(bytes)
connect-connect-0   116m         3176Mi
connect-connect-1   61m          3059Mi
```

Bellek kullanımının sınıra yakın olması tek başına sorun değildir; Kafka Connect'in Java
yığını açılışta ayrıldığı için kullanım baştan yüksek görünür. Pod'un yeniden başlama
sayısı artıyorsa [50-isletme/sorun-giderme.md](../50-isletme/sorun-giderme.md) §4.1'e
bakın.

---

## 10. Arayüz adresleri

**Ne zaman:** kullanıcılara adres vermeden önce ve bir arayüz açılmadığında. `TLS` sütunu,
sertifika sorunlarında ilk ipucudur.

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" get route \
  -o custom-columns='AD:.metadata.name,HOST:.spec.host,TLS:.spec.tls.termination'
```

**Beklenen çıktı** (örnek; alan adı sizde farklıdır):

```text
AD                          HOST                                                               TLS
jupyterhub                  jupyterhub-lakehouse.apps.ocpphy.solusbilisim.com                  edge
keycloak                    keycloak-lakehouse.apps.ocpphy.solusbilisim.com                    edge
lakehouse-dual-role-0       lakehouse-dual-role-0-lakehouse.apps.ocpphy.solusbilisim.com       passthrough
lakehouse-dual-role-1       lakehouse-dual-role-1-lakehouse.apps.ocpphy.solusbilisim.com       passthrough
lakehouse-dual-role-2       lakehouse-dual-role-2-lakehouse.apps.ocpphy.solusbilisim.com       passthrough
lakehouse-kafka-bootstrap   lakehouse-kafka-bootstrap-lakehouse.apps.ocpphy.solusbilisim.com   passthrough
polaris                     polaris-lakehouse.apps.ocpphy.solusbilisim.com                     edge
superset                    superset-lakehouse.apps.ocpphy.solusbilisim.com                    edge
trino                       trino-lakehouse.apps.ocpphy.solusbilisim.com                       reencrypt
zeppelin                    zeppelin-lakehouse.apps.ocpphy.solusbilisim.com                    edge
```

`lakehouse-…` ile başlayan dört satır yalnız Kafka'nın dış adresi açıksa vardır (nginx
günlük akışı ya da olay akışları için). Ayrıntı:
[port-ve-servisler.md](port-ve-servisler.md) §1.

---

## 11. Bir Secret'ın içindeki değeri okumak

**Ne zaman:** bir parolanın ya da kimliğin kümeye doğru yazıldığını doğrulamak
gerektiğinde. **Çıktı bir sırdır:** ekranı paylaşırken ya da günlüğe yapıştırırken dikkat
edin.

Önce yalnız anahtar adlarına bakın; bu komut değerleri göstermez:

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" get secret polaris-root -o json | jq -r '.data | keys[]'
```

**Beklenen çıktı:**

```text
clientId
clientSecret
```

Bir değeri gerçekten okumak gerekiyorsa:

```bash
oc -n "$LAKEHOUSE_NS" get secret polaris-root -o jsonpath='{.data.clientId}' | base64 -d
echo
```

**Ne görmelisiniz:** tek satır düz metin. `Error from server (NotFound)`, Secret'ın hiç
yaratılmadığını gösterir ([secret-listesi.md](secret-listesi.md)).

---

## 12. Tek seferlik bir Spark işini elle çalıştırmak

**Ne zaman:** zamanlanmış koşuyu beklemeden bir Spark işini hemen çalıştırmak
gerektiğinde. Tek seferlik iş **Git'e konmaz**: ArgoCD onu her silindiğinde yeniden
yaratır ve iş tekrar tekrar çalışır. Bu yüzden elle uygulanır.

Aşağıdaki örnek, depodaki örnek programı (`custom/examples/ornek_rapor.py`) çalıştırır.
Tanım dosyasını `scripts/spark-is-tanimi.sh` üretir; kümeye özel ayarlar kümedeki ürün
işinden kopyalanır
([50-isletme/yeni-spark-uygulamasi.md](../50-isletme/yeni-spark-uygulamasi.md) §4). İş,
Python kodunu bir ConfigMap'ten okur; bu yüzden önce ConfigMap yaratılır.

`[bastion]`

```bash
cd ~/lakehouse
scripts/spark-is-tanimi.sh --ad ornek-rapor --dosya ornek_rapor.py > /tmp/ornek-rapor.yaml
oc -n "$LAKEHOUSE_NS" create configmap ornek-rapor --from-file=custom/examples/ornek_rapor.py
oc -n "$LAKEHOUSE_NS" label configmap ornek-rapor lakehouse.io/yonetim=manual-yeni-spark-uygulamasi-5.2
oc -n "$LAKEHOUSE_NS" delete sparkapplication ornek-rapor --ignore-not-found
oc apply -f /tmp/ornek-rapor.yaml
```

**Beklenen çıktı** (örnek):

```text
configmap/ornek-rapor created
configmap/ornek-rapor labeled
sparkapplication.sparkoperator.k8s.io/ornek-rapor created
```

İlerlemeyi görmek için: `oc -n "$LAKEHOUSE_NS" get sparkapplication ornek-rapor`. İşi
tekrar çalıştırmak için son iki satırı tekrarlamanız yeterlidir; ConfigMap yerinde kalır.
İşiniz bittiğinde ConfigMap'i silin:
`oc -n "$LAKEHOUSE_NS" delete configmap ornek-rapor`.

Tam yordam: [50-isletme/yeni-spark-uygulamasi.md](../50-isletme/yeni-spark-uygulamasi.md)
§5.2.

**Ters giderse:** `configmaps "ornek-rapor" already exists` görürseniz ConfigMap önceki bir
koşudan kalmıştır; ilk iki satırı atlayıp devam edin.

---

## 13. ArgoCD'ye Git'i hemen okutmak

**Ne zaman:** Git'e gönderilen bir değişiklik birkaç dakika içinde uygulanmadığında. Bu
komut kümede bir şeyi değiştirmez, yalnız beklemeyi kısaltır.

`[bastion]`

```bash
oc -n "$ARGOCD_NS" annotate application glue argocd.argoproj.io/refresh=hard --overwrite
```

**Beklenen çıktı:**

```text
application.argoproj.io/glue annotated
```

Birkaç saniye içinde 1 numaradaki `SYNC STATUS` güncellenir. Kendi `custom/` işleriniz
için `glue` yerine `custom` yazın.

---

## 14. Bir bileşeni yeniden başlatmak

**Ne zaman:** yalnız açılışta okunan bir ayar değiştiğinde (ör. Zeppelin'in giriş ayarı,
JupyterHub ayarları). `rollout restart`, `oc delete pod`'a göre daha az kesinti yaratır.

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" rollout restart deploy/zeppelin
oc -n "$LAKEHOUSE_NS" rollout status deploy/zeppelin --timeout=600s
```

**Beklenen çıktı:**

```text
deployment.apps/zeppelin restarted
deployment "zeppelin" successfully rolled out
```

Keycloak bir StatefulSet'tir; onun için `deploy/` yerine `sts/` yazılır
(`sts/keycloak`).

---

## 15. Pod'dan dosya almak, pod'a dosya koymak

**Ne zaman:** bir dosyayı incelemek için yönetim makinesine almak gerektiğinde. Konteynerde
`tar` bulunmalıdır.

`[bastion]`

```bash
POD=$(oc -n "$LAKEHOUSE_NS" get pod -l app=zeppelin -o jsonpath='{.items[0].metadata.name}')
oc -n "$LAKEHOUSE_NS" cp -c zeppelin "$POD":/data/conf ./zeppelin-conf
ls ./zeppelin-conf
```

**Beklenen çıktı** (ilk satır `tar`'ın bilgi notudur):

```text
tar: Removing leading `/' from member names
interpreter.json
notebook-authorization.json
```

Dosya koymak için kaynak ile hedefin yerini değiştirin. `tar: command not found`
görürseniz konteynerde `tar` yoktur; o durumda `oc exec … -- cat DOSYA > yerel-dosya`
kullanın.

---

<a id="trino-sql"></a>
## 16. Trino'da komut satırından SQL çalıştırmak

**Ne zaman:** bir tablonun dolduğunu (satır sayısı, `$snapshots`) tarayıcı açmadan
doğrulamak gerektiğinde. Komut, Trino'nun kendi pod'undaki komut satırı aracını `superset`
servis hesabıyla kullanır.

`[bastion]`

```bash
SP=$(oc -n "$LAKEHOUSE_NS" get secret trino-service-accounts \
  -o jsonpath='{.data.superset}' | base64 -d)
oc -n "$LAKEHOUSE_NS" exec deploy/trino-coordinator -c trino-coordinator -- \
  env TRINO_PASSWORD="$SP" trino --server https://trino.lakehouse.svc:8443 \
  --truststore-path /etc/trino/tls/ca.crt --user superset --password \
  --execute 'select count(*) from lakehouse.shop_raw.orders'
unset SP
```

**Beklenen çıktı** (araç her değeri tırnak içinde yazar):

```text
"6"
```

Bu yolun üç sınırı vardır:

- **Yalnız okur.** Tablo yaratan ya da silen ifadeler `Access Denied` ile reddedilir.
- **Satır filtresi ve kolon maskesi uygulanmaz.** Servis hesabı bir kullanıcı grubunda
  değildir. Bir kullanıcının gerçekte ne gördüğünü sınamak için o kullanıcıyla giriş
  yapılır ([50-isletme/kullanici-ve-yetki.md](../50-isletme/kullanici-ve-yetki.md) §7).
- **`--server` adresi küme içi servis adıdır.** Sertifika bu ada verilmiştir; `localhost`
  yazarsanız sertifika hatası alırsınız.

**Ters giderse:**

- `Authentication failed`: Secret'taki parola ile Trino'nun parola dosyası uyuşmuyordur
  ([secret-listesi.md](secret-listesi.md) madde 7).
- `executable file not found`: Trino imajında komut satırı aracı yoktur; aynı sorguyu
  Superset → **SQL Lab**'da çalıştırın
  ([50-isletme/veri-metrikleri.md](../50-isletme/veri-metrikleri.md) §3).

---

## 17. Bir nesnenin nereden geldiğini görmek

**Ne zaman:** bir nesneyi silmeden ya da değiştirmeden önce; silerseniz geri gelip
gelmeyeceğini bilmek için.

`[bastion]`

```bash
scripts/nesne-kaynagi.sh secret/connect-push kafkaconnect/connect secret/connect
```

**Beklenen çıktı:**

```text
secret/connect-push                          ELLE       docs/20-on-kosullar.md §5 — İç registry ve Connect imajını gönderecek hesap
kafkaconnect/connect                         GIT        ArgoCD uygulaması: glue
secret/connect                               TÜREV      KafkaUser/connect -> ArgoCD uygulaması: glue
```

`GIT` ve `TÜREV` nesneler silinirse kendiliğinden geri gelir. `ELLE` nesneler gelmez;
üçüncü sütundaki belge adımı tekrarlanır. Bir türün bütün nesneleri için
`scripts/nesne-kaynagi.sh --liste secret`. Ayrıntı:
[50-isletme/degisiklik-nasil-uygulanir.md](../50-isletme/degisiklik-nasil-uygulanir.md) §7.

---

## 18. Kafka konularına ve tüketici gruplarına bakmak

**Ne zaman:** bir konunun var olduğunu, bir tüketici grubunun geride kalıp kalmadığını
görmek ya da bir konuyu silmek gerektiğinde. `scripts/kafka-cli.sh`, Kafka'nın kendi
araçlarını ürünün yönetici kimliğiyle çalıştırır; bağlantı ayarlarını kendisi verir, siz
yalnız aracın kendi seçeneklerini yazarsınız.

`[bastion]`

```bash
scripts/kafka-cli.sh kafka-topics.sh --list | grep -E '^shop\.|^nginx'
scripts/kafka-cli.sh kafka-consumer-groups.sh --list | grep sink-shop
```

**Beklenen çıktı** (örnek; adlar sizin kaynaklarınızdır):

```text
nginx.access
nginx.dlq
shop.dlq
shop.public.orders
connect-sink-shop
connect-sink-shop-coord
```

Bir grubun ne kadar geride olduğunu görmek için:
`scripts/kafka-cli.sh kafka-consumer-groups.sh --describe --group connect-sink-shop`
(`LAG` sütunu). Konu silme yordamı:
[50-isletme/kaynak-veya-tablo-silme.md](../50-isletme/kaynak-veya-tablo-silme.md) §5.

---

## Kontrol listesi

- [ ] `install/lakehouse.env` yüklendi; `$LAKEHOUSE_NS` ve `$ARGOCD_NS` dolu.
- [ ] Teşhise 1, 5 ve 8 numaralı komutlarla başlanacağı biliniyor.
- [ ] 11 numaralı komutun çıktısının sır olduğu ve paylaşılmayacağı biliniyor.
- [ ] 12 numaralı komutun Git dışında, bilerek yapılan bir elle müdahale olduğu biliniyor.
- [ ] 16 numaralı komutun yalnız okuduğu ve satır filtresi ile kolon maskesi uygulamadığı
      biliniyor.
- [ ] Bir nesneyi silmeden önce 17 numaralı komutla nereden geldiğine bakılıyor.

## Sonraki bölüm

[port-ve-servisler.md](port-ve-servisler.md) — hangi servisin hangi portta olduğu. Secret
adları ve anahtarları: [secret-listesi.md](secret-listesi.md).
