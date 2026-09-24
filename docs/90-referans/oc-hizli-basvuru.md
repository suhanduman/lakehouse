# 90 — `oc` hızlı başvuru

**Bu bölümde:** bu kurulumu işletirken en sık gereken **on beş** komut; her biri için ne
zaman kullanılacağı, komutun kendisi ve beklenen çıktı. Amaç, bir belirti karşısında hangi
komutun çalıştırılacağını aramadan bulmaktır.
**Süre:** okuma 10 dakika.
**Gereken yetki:** `$LAKEHOUSE_NS` ve `$ARGOCD_NS` ad alanlarında okuma; 12–15 arası
komutlar yazma yetkisi ister.
**Nerede çalıştırılır:** `[bastion]` — `oc login` ile kümeye girilmiş yönetim makinesi.

Bütün komutlar `$LAKEHOUSE_NS` ve `$ARGOCD_NS` değişkenlerini kullanır; her oturumda önce
`set -a; . install/lakehouse.env; set +a` çalıştırın. Geliştirme (kind) kümesinde `oc`
yerine `kubectl` yazılır; komutların karşılıkları birebir aynıdır (`oc adm top` →
`kubectl top`). Tek istisna 10 numaradır: kind'da Route yoktur.

> **Bu sayfa teşhis etmez, komut verir.** Belirtiden yola çıkan teşhis tablosu işletme
> bölümündeki sorun giderme sayfasındadır.

---

## 1. GitOps durumu: her şey Git'teki hâliyle mi

**Ne zaman:** her değişiklikten sonra ve "kümede neden hâlâ eski hâli var?" sorusunda ilk
komut.

`[bastion]`

```bash
oc -n "$ARGOCD_NS" get applications
```

**Beklenen:** bütün satırlarda `SYNC STATUS=Synced`, `HEALTH STATUS=Healthy`. `OutOfSync` =
Git'te uygulanmamış bir commit vardır; `Degraded` = uygulanan kaynaklardan biri
sağlıksızdır.

---

## 2. Bir pod neden başlamıyor

**Ne zaman:** pod `Pending`, `CreateContainerConfigError`, `ImagePullBackOff` ya da
`CrashLoopBackOff` durumundayken. Çıktının **son** bölümü (`Events`) asıl cevabı verir.

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" describe pod -l app.kubernetes.io/name=trino | tail -30
```

**Beklenen:** `Events` bölümünde okunur bir neden — `secret "…" not found`,
`pod has unbound immediate PersistentVolumeClaims`, `Failed to pull image …`.

---

## 3. Konteyner günlüğü

**Ne zaman:** pod ayakta ama iş yapmıyorsa. `-c` ile initContainer ya da yan konteyner
seçilir; `--previous` çöken önceki konteynerin günlüğünü verir.

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" logs deploy/trino-coordinator --tail=50
```

**Beklenen:** uygulamanın kendi açılış günlüğü. Boş çıktı = konteyner henüz hiç
başlamamıştır (2 numaraya dönün).

---

## 4. Güncelleme bitti mi

**Ne zaman:** bir values değişikliğinden ya da Secret rotasyonundan sonra yeni pod'un
gerçekten ayağa kalktığını beklemek için. Komut rollout bitene kadar bloklar.

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" rollout status deploy/superset-web-server --timeout=600s
```

**Beklenen:** `deployment "superset-web-server" successfully rolled out`. Zaman aşımı = yeni
pod hazır olamıyordur (2 numara).

---

## 5. Ürünün kendi nesneleri: connector, zamanlı Spark işi, veritabanı

**Ne zaman:** "veri akmıyor", "gece işi koşmamış" ya da "veritabanı sağlıklı mı"
sorularında. Üç CR ailesi tek bakışta görülür.

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" get kafkaconnector
oc -n "$LAKEHOUSE_NS" get scheduledsparkapplication
oc -n "$LAKEHOUSE_NS" get cluster
```

**Beklenen çıktı** (geliştirme kümesinden alınmış gerçek çıktı; üretimde adlar sizin
kaynaklarınızdır, `SUSPEND` sütunu `false` ve `INSTANCES` 2 olmalıdır):

```text
NAME         CLUSTER   CONNECTOR CLASS                                      MAX TASKS   READY
dbz-crm      connect   io.debezium.connector.mongodb.MongoDbConnector       1           True
dbz-shop     connect   io.debezium.connector.postgresql.PostgresConnector   1           True
sink-nginx   connect   org.apache.iceberg.connect.IcebergSinkConnector      1           True
sink-shop    connect   org.apache.iceberg.connect.IcebergSinkConnector      1           True

NAME                      SCHEDULE     TIMEZONE   SUSPEND   LAST RUN   LAST RUN NAME   AGE
maint-compact             30 2 * * *              false                                5d
maint-expire-orphan-ttl   45 2 * * *              false                                5d
maint-position-deletes    15 2 * * *              false                                5d
mongo-bronze              5 3 * * *               false                                5d
silver-merge              0 3 * * *               false                                5d

NAME          AGE   INSTANCES   READY   STATUS                     PRIMARY
keycloak-db   5d    2           2       Cluster in healthy state   keycloak-db-1
polaris-db    5d    2           2       Cluster in healthy state   polaris-db-1
superset-db   5d    2           2       Cluster in healthy state   superset-db-1
```

`READY=False` bir connector, `SUSPEND=true` bir zamanlı iş ve `READY` sütunu `INSTANCES` ile
eşleşmeyen bir veritabanı — üçü de bulgudur.

---

## 6. Pod içinde komut çalıştırma

**Ne zaman:** bir dosyanın gerçekten mount edildiğini, bir ortam değişkeninin geldiğini ya
da ağ erişiminin açık olduğunu konteynerin **içinden** görmek gerektiğinde. Heredoc veya
boru ile veri gönderiyorsanız `-i` bayrağı zorunludur.

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" exec deploy/zeppelin -- ls /etc/lakehouse-ca
```

**Beklenen:** dosya listesi (`tls.crt`). `No such file or directory` = mount yapılmamıştır.

---

## 7. Küme içi bir servise yerelden erişme

**Ne zaman:** Route'u olmayan bir servise (Polaris API'si, PostgreSQL) yönetim makinesinden
bağlanmak gerektiğinde. Komut ön planda çalışır; `Ctrl+C` ile kapatılır.

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" port-forward svc/polaris 8181:8181
```

**Beklenen:** `Forwarding from 127.0.0.1:8181 -> 8181`. `unable to forward port because pod
is not running` = hedef servisin arkasında ayakta pod yoktur.

---

## 8. Ad alanında en son ne oldu

**Ne zaman:** "az önce bir şey bozuldu" durumunda; zamana göre sıralı olay listesi çoğu
zaman kök nedeni doğrudan söyler.

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" get events --sort-by=.lastTimestamp | tail -20
```

**Beklenen:** son yirmi olay. `Warning` tipli satırlar (`FailedScheduling`, `BackOff`,
`Unhealthy`) önce okunur.

---

## 9. Kaynak tüketimi

**Ne zaman:** bir pod'un `OOMKilled` olduğundan ya da düğümde yer kalmadığından
şüphelenildiğinde. Komut küme metrik toplayıcısını gerektirir.

`[bastion]`

```bash
oc adm top pods -n "$LAKEHOUSE_NS"
```

**Beklenen:** pod başına CPU ve bellek kullanımı. `error: Metrics API not available` =
kümede metrik toplayıcı yoktur (OpenShift'te normalde vardır).

---

## 10. Arayüz adresleri

**Ne zaman:** kullanıcıya adres vermeden önce ve bir arayüz açılmadığında; `TLS` sütunu
sertifika sorunlarının ilk ipucudur.

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" get route \
  -o custom-columns='AD:.metadata.name,HOST:.spec.host,TLS:.spec.tls.termination'
```

**Beklenen:** altı Route ve adresleri — ayrıntı [port-ve-servisler.md](port-ve-servisler.md)
§1.

---

## 11. Bir Secret'ın içindeki değeri okuma

**Ne zaman:** bir parolanın ya da kimliğin kümede doğru yazıldığını doğrulamak
gerektiğinde. **Çıktı sırdır:** terminal geçmişine ve paylaşılan günlüklere düşmemelidir.

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" get secret polaris-root -o jsonpath='{.data.clientId}' | base64 -d
echo
```

**Beklenen:** tek satır düz metin. `Error from server (NotFound)` = Secret yaratılmamıştır
([secret-listesi.md](secret-listesi.md)). Yalnız anahtar adlarını görmek için:
`oc -n "$LAKEHOUSE_NS" get secret polaris-root -o jsonpath='{.data}' | tr ',' '\n'`.

---

## 12. Tek seferlik bir Spark işini elle koşturma

**Ne zaman:** zamanlı işi beklemeden bir Spark uygulamasını hemen çalıştırmak gerektiğinde.
Tek seferlik `SparkApplication` **GitOps'a uygun değildir** (ArgoCD `selfHeal` onu tekrar
tekrar yaratır), bu yüzden bilinçli olarak elle uygulanır. Sıra `custom/README.md`'deki
yordamın aynısıdır: CR, Python kodunu `ornek-rapor` ConfigMap'inden mount eder, o yüzden
ConfigMap kümede **önce** olmalıdır.

`[bastion]`

```bash
oc apply -k custom/examples
oc -n "$LAKEHOUSE_NS" delete sparkapplication ornek-rapor --ignore-not-found
oc apply -f custom/examples/spark-tek-seferlik.yaml
```

**Beklenen:**

```text
configmap/ornek-rapor created
scheduledsparkapplication.sparkoperator.k8s.io/ornek-rapor-zamanli created
sparkapplication.sparkoperator.k8s.io/ornek-rapor created
```

İlerleme: `oc -n "$LAKEHOUSE_NS" get sparkapplication`.

**Bilerek olan iki yan etki:**

1. İlk komut yalnız ConfigMap'i değil, `custom/examples/kustomization.yaml`'daki bütün
   kaynakları uygular — bugün bu, **`ornek-rapor-zamanli` adlı bir
   `ScheduledSparkApplication`**'dır (gecelik cron `0 4 * * *`). Örnek `suspend: true` ile
   gelir, yani kendiliğinden koşmaz; yine de kümede **GitOps dışında** duran bir nesnedir.
   İstemiyorsanız ya yalnız ConfigMap'i uygulayın
   (`oc -n "$LAKEHOUSE_NS" create configmap ornek-rapor
   --from-file=custom/examples/ornek_rapor.py`) ya da işiniz bitince silin
   (`oc -n "$LAKEHOUSE_NS" delete scheduledsparkapplication ornek-rapor-zamanli`).
2. Bu örnek dosyalar **ad alanına sabittir**: `custom/examples/kustomization.yaml`
   `namespace: lakehouse` der ve `custom/examples/spark-tek-seferlik.yaml` da
   `metadata.namespace: lakehouse` taşır. `$LAKEHOUSE_NS` başka bir değerse komutlar yine
   `lakehouse` ad alanına yazar — `-n` bayrağı bunu **ezmez**. Farklı bir ad alanı
   kullanıyorsanız dosyaları kendi `custom/` klasörünüze kopyalayıp ad alanını
   değiştirin.

---

## 13. ArgoCD'yi Git'i baştan okumaya zorlama

**Ne zaman:** Git'e itilen bir değişiklik birkaç dakikada uygulamaya yansımadığında. `hard`
yenileme önbelleği atlar ve depoyu baştan okur.

`[bastion]`

```bash
oc -n "$ARGOCD_NS" annotate application glue argocd.argoproj.io/refresh=hard --overwrite
```

**Beklenen:** `application.argoproj.io/glue annotated`; birkaç saniye içinde `SYNC STATUS`
güncellenir. Bazı değişiklikler yalnız yenilemeyle yetinmez, kaynağın silinmesini de ister —
işletme bölümündeki ilgili kılavuza bakın.

---

## 14. Bir bileşeni yeniden başlatma

**Ne zaman:** yalnız açılışta okunan bir yapılandırma (Zeppelin `shiro.ini`, Trino'nun dosya
tabanlı grup listesi) değiştiğinde. `rollout restart` kesintiyi en aza indirdiği için
`delete pod` yerine tercih edilir.

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" rollout restart deploy/zeppelin
oc -n "$LAKEHOUSE_NS" rollout status deploy/zeppelin --timeout=600s
```

**Beklenen:** `deployment.apps/zeppelin restarted` ve ardından `successfully rolled out`.
StatefulSet'ler için (`keycloak`) `deploy/` yerine `sts/` yazılır.

---

## 15. Pod'dan dosya alma, pod'a dosya koyma

**Ne zaman:** bir günlük dosyasını ya da üretilmiş bir çıktıyı incelemek için dışarı almak
gerektiğinde. Konteynerde `tar` bulunmalıdır.

`[bastion]`

```bash
POD=$(oc -n "$LAKEHOUSE_NS" get pod -l app.kubernetes.io/name=superset \
  -o jsonpath='{.items[0].metadata.name}')
oc -n "$LAKEHOUSE_NS" cp "$POD":/app/assets ./superset-assets
```

**Beklenen:** yerelde `./superset-assets` dizini. `tar: command not found` = konteynerde
`tar` yoktur; o durumda `oc exec … -- cat DOSYA > yerel-dosya` kullanın.

---

## Kontrol listesi

- [ ] `install/lakehouse.env` yüklendi (`$LAKEHOUSE_NS`, `$ARGOCD_NS` dolu).
- [ ] Teşhise her zaman 1, 5 ve 8 numaralı komutlarla başlanacağı biliniyor.
- [ ] 11 numaralı komutun çıktısının **sır** olduğu ve paylaşılmayacağı biliniyor.
- [ ] 12 numaralı komutun GitOps dışı, bilinçli bir elle müdahale olduğu biliniyor.

## Sonraki bölüm

[port-ve-servisler.md](port-ve-servisler.md) — hangi servisin hangi portta olduğu. Secret
adları ve anahtarları: [secret-listesi.md](secret-listesi.md).
