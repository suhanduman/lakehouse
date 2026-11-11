# Değişiklik nasıl uygulanır

Lakehouse'ta yapılan her ayar değişikliği aynı yoldan geçer: Git'teki bir dosya
değiştirilir, değişiklik Git'e gönderilir ve ArgoCD onu kümeye uygular. Yeni kaynak
eklemek, tablo eklemek, kullanıcı yetkisi vermek ya da yedek ayarını değiştirmek bu
yolun örnekleridir. Diğer işletme sayfaları yalnız "hangi satır değişir" sorusunu
yanıtlar; satırın kümeye nasıl ulaştığı bu sayfadadır.

**Ne kadar sürer:** düzenleme ve gönderme 5–10 dakika. ArgoCD'nin uygulaması 1–5
dakika; Kafka Connect yeniden başlıyorsa 10 dakikaya kadar.

**Hangi yetkiler gerekir:** kurumun lakehouse Git deposunda `main` dalına yazma ve
`$ARGOCD_NS` ad alanında okuma. Kümede elle değişiklik yetkisi gerekmez.

**Komutlar nerede çalışır:** `[bastion]` — `git`, `oc` ve `helm` araçlarının kurulu
olduğu, `oc login` ile kümeye bağlı ve Git deposunun bir kopyasının (`~/lakehouse`)
bulunduğu yönetim makinesi.

Yolun tamamı:

```text
dosyayı düzenle -> check-site.sh ile denetle -> commit + push -> ArgoCD kümeye uygular
                                                             -> git revert ile geri alınır
```

> **Kümeyi elle değiştirmeyin.** ArgoCD Git'teki hâli kümede sürekli korur. `oc edit`
> ile yapılan bir değişiklik birkaç dakika içinde Git'teki hâline geri döner; Git'te
> karşılığı olmayan bir nesne silinir. Kalıcı olan yalnız Git'e yazılandır. Hangi
> nesnelerin bu korumanın içinde olduğu §7'dedir.

---

## 1. Değişkenleri yükleyin

Her yeni terminalde bir kez çalıştırın. Komutlardaki `$ARGOCD_NS` ve `$LAKEHOUSE_NS`
buradan gelir.

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
yoktur ya da başka bir klasördesiniz. Dosya kurulumda doldurulur
([30-kurulum](../30-kurulum.md) §1).

---

## 2. Hangi dosya değişir

Kuruma özel bütün ayarlar `platform/values/site/` klasöründeki üç dosyadadır.
`platform/values/` klasöründeki diğer dosyalar ve `glue/values.yaml` ürünün
varsayılanlarıdır: bunlara dokunmayın, ürün yükseltmesinde üzerlerine yazılır.

| Dosya | Ne için | Sık değişen ayarlar |
|---|---|---|
| `platform/values/site/glue.yaml` | kaynaklar, Kafka, katalog, kimlik, yedek | `sources`, `pipelines`, `nginx.enabled`, `s3.endpoint`, `backup.s3.bucket` |
| `platform/values/site/trino.yaml` | Trino erişim kuralları | satır ve kolon kuralları |
| `platform/values/site/jupyterhub.yaml` | not defteri ortamı | disk boyutu, kaynak sınırları |

Bütün ayarların listesi ve varsayılanları:
[90-referans/values-anahtarlari.md](../90-referans/values-anahtarlari.md).

**Parolalar Git'e yazılmaz.** Parola, anahtar ve sertifikalar kümede `Secret` olarak
yaratılır; ayar dosyaları yalnız Secret'ın **adını** taşır. Bütün Secret'ların listesi:
[90-referans/secret-listesi.md](../90-referans/secret-listesi.md).

---

## 3. Değişikliği yapın ve denetleyin

### 3.1 Dosyayı düzenleyin

Önce deponun en güncel hâlini alın, sonra dosyayı açın:

`[bastion]`

```bash
git pull --rebase origin main
vi platform/values/site/glue.yaml
```

Dosyayı kaydedip kapattıktan sonra denetleme betiğini çalıştırın:

```bash
bash scripts/check-site.sh
```

Betik üç site dosyasını birlikte denetler: eksik ya da birbiriyle çelişen bir ayar ve
kurulumdan kalmış bir örnek değer arar.

**Beklenen çıktı:**

```text
check-site: OK
```

**Ters giderse:** `HATA` ile başlayan bir satır görürseniz değişikliği göndermeyin.
Satır, hangi dosyadaki hangi ayarın sorunlu olduğunu söyler; düzeltip betiği tekrar
çalıştırın. `UYARI` satırları göndermeyi engellemez, yalnız dikkat çeker.

### 3.2 Kümeye ne gideceğine bakın (isteğe bağlı)

Kaynak eklemek ya da çıkarmak gibi kümede nesne sayısını değiştiren bir değişiklikte,
göndermeden önce ürünün kümede neler yaratacağını görebilirsiniz:

`[bastion]`

```bash
helm template glue ./glue -n "$LAKEHOUSE_NS" \
  -f platform/values/glue.yaml -f platform/values/site/glue.yaml \
  | grep -E '^kind: ' | sort | uniq -c | sort -rn
```

**Beklenen çıktı** (örnek: hiç kaynak tanımlanmamış bir kurulum; kaynak ekledikçe
`ConfigMap`, `KafkaConnector` ve `KafkaUser` satırları artar):

```text
   9 kind: ConfigMap
   6 kind: Route
   3 kind: ScheduledBackup
   3 kind: NetworkPolicy
   3 kind: Cluster
   2 kind: Service
   2 kind: RoleBinding
   2 kind: Role
   2 kind: PersistentVolumeClaim
   2 kind: KafkaUser
   2 kind: Deployment
   1 kind: Superset
   1 kind: PrometheusRule
   1 kind: PodMonitor
   1 kind: ObjectStore
   1 kind: KeycloakRealmImport
   1 kind: Keycloak
   1 kind: KafkaNodePool
   1 kind: KafkaConnect
   1 kind: Kafka
   1 kind: Job
   1 kind: Issuer
   1 kind: Certificate
```

**Ters giderse:** komut `Error: ... required` ile durursa zorunlu bir ayar eksiktir (ör.
bir kaynağın `signalTable` alanı); hata satırı eksik ayarın adını verir.

---

## 4. Değişikliği Git'e gönderin

Değişiklik kümeye yalnız bu yoldan ulaşır.

`[bastion]`

```bash
git diff
git add platform/values/site/glue.yaml
git commit -m "site: erp kaynağı eklendi"
git push origin main
```

`git diff` çıktısında yalnız sizin değiştirdiğiniz satırlar görünmelidir. Commit
mesajına ne değiştiğini kısaca yazın; geri almak gerektiğinde bu mesajla bulunur.

**Beklenen çıktı** (son satırlar; Git sunucusunun adresi ve kimlikler sizde farklıdır):

```text
To ssh://git.kurum.example.net/lakehouse.git
   4ff2cc4..9a13b7c  main -> main
```

**Ters giderse:** `rejected ... non-fast-forward` görürseniz sizden sonra başka biri de
değişiklik göndermiştir. `git pull --rebase origin main` çalıştırın ve `git push`'u
tekrarlayın. Çakışma çıkarsa elle çözmeden önce diğer değişikliği yapan kişiyle konuşun.

---

## 5. ArgoCD'nin uygulamasını izleyin

ArgoCD Git deposuna en geç üç dakikada bir bakar ve değişikliği gördüğünde kendiliğinden
uygular. Durumu görmek için:

`[bastion]`

```bash
oc -n "$ARGOCD_NS" get application glue \
  -o jsonpath='{.status.sync.status} {.status.health.status}{"\n"}'
```

**Beklenen çıktı** (uygulama bittiğinde):

```text
Synced Healthy
```

Uygulama sürerken sırayla `OutOfSync Progressing`, `Synced Progressing` ve sonunda
`Synced Healthy` görülür. Kafka Connect'in yeniden başlaması gereken değişikliklerde
(kaynak eklemek ya da çıkarmak) `Progressing` birkaç dakika sürer.

**Ters giderse:** beş dakika sonra hâlâ `OutOfSync` görünüyorsa değişiklik ArgoCD'ye
ulaşmamıştır. `git log origin/main -1` ile commit'inizin depoda olduğunu kontrol edin;
sonra §5.1 ile ArgoCD'den depoyu hemen okumasını isteyin.

### 5.1 Beklemeden uygulatmak

ArgoCD'nin depoyu hemen okumasını isteyebilirsiniz. Bu komut kümede bir şeyi
değiştirmez, yalnız bekleme süresini kısaltır:

`[bastion]`

```bash
oc -n "$ARGOCD_NS" annotate application glue argocd.argoproj.io/refresh=hard --overwrite
```

**Beklenen çıktı:**

```text
application.argoproj.io/glue annotated
```

`custom/` klasöründeki kendi işleriniz `custom` adlı ayrı bir uygulamadadır; o klasörü
değiştirdiyseniz komutta `glue` yerine `custom` yazın.

**Ters giderse:** komuttan sonra da uygulama başlamıyorsa ArgoCD Git deposuna
erişemiyordur. Depo adresi ve erişim anahtarı `$ARGOCD_NS` ad alanındaki
`lakehouse-repo` Secret'ındadır ([30-kurulum](../30-kurulum.md) §3).

### 5.2 JupyterHub ayarını değiştirdiyseniz hub'ı yeniden başlatın

JupyterHub ayarları (`platform/values/site/jupyterhub.yaml`: disk boyutu, kaynak
sınırları, not defteri imajı) kümeye yazılır ama hub kendiliğinden yeniden başlamaz.
Böylece ilgisiz bir değişiklik (ör. kaynak eklemek) kullanıcıların açık oturumlarını
kesmez. JupyterHub ayarını değiştirdiyseniz, ArgoCD uygulamayı bitirdikten sonra hub'ı
bir kez yeniden başlatın:

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" rollout restart deploy/hub
oc -n "$LAKEHOUSE_NS" rollout status deploy/hub --timeout=300s
```

**Beklenen çıktı** (aradaki `Waiting for deployment …` satırlarının sayısı değişebilir):

```text
deployment.apps/hub restarted
Waiting for deployment "hub" rollout to finish: 0 of 1 updated replicas are available...
deployment "hub" successfully rolled out
```

Hub yeniden başlarken (yarım dakika kadar) yeni giriş yapılamaz; açık not defterleri
çalışmaya devam eder. Yeni ayar, kullanıcının bir sonraki not defteri sunucusunda geçerli
olur: Jupyter'de **File → Hub Control Panel → Stop My Server**, sonra **Start My
Server**.

---

## 6. Değişikliği geri alın

Bir değişiklik kümede değil, Git'te geri alınır. `git revert`, değişikliğin tersini
yapan yeni bir commit yazar; ArgoCD de eski hâli aynı yoldan uygular.

`[bastion]`

```bash
git pull --rebase origin main
git revert --no-edit HEAD
git push origin main
```

**Beklenen çıktı** (ilk satırlar; commit kimliği sizde farklıdır):

```text
[main 3c9f0a2] Revert "site: erp kaynağı eklendi"
 1 file changed, 6 deletions(-)
```

`HEAD` en son commit'tir. Daha eski bir değişikliği geri almak için `HEAD` yerine o
commit'in kimliğini yazın; kimliği şu komutla bulursunuz:

```bash
git log --oneline -- platform/values/site/
```

`git reset` ve `git push --force` kullanmayın: ArgoCD'nin izlediği dalın geçmişi bozulur.

**Ters giderse:** `your local changes would be overwritten` görürseniz kopyanızda
kaydedilmemiş bir düzenleme vardır. `git status` ile bakın; değişikliği bırakmak
istemiyorsanız `git stash` ile kenara alın.

> **Git ile geri gelmeyen iki şey var.** Silinmiş bir Iceberg tablosu `git revert` ile
> geri gelmez; veri yedekten döner ([yedek-ve-geri-donus.md](yedek-ve-geri-donus.md)).
> Listeden çıkarılıp geri eklenen bir kaynak ise verisini baştan okur
> ([kaynak-veya-tablo-silme.md](kaynak-veya-tablo-silme.md) §3).

---

## 7. ArgoCD neyi korur

Kümedeki nesnelerin hepsi Git'ten gelmez. Bir nesneyi silmeden ya da değiştirmeden önce
nereden geldiğini bilmek, sonucunu önceden bilmek demektir.

| Nesne nereden gelir | Örnek | Silinir ya da elle değiştirilirse |
|---|---|---|
| **Git** — ArgoCD kurar | `KafkaConnect`, `KafkaConnector`, Deployment'lar, zamanlı Spark işleri | birkaç dakika içinde Git'teki hâline döner |
| **Türev** — bir operatör, Git'teki bir nesneden üretir | Kafka ve Connect pod'ları, Kafka kullanıcı parolaları, veritabanı pod'ları, sertifika Secret'ları | operatör yeniden üretir |
| **Platform** — OpenShift üretir | servis hesaplarının imaj çekme Secret'ları | OpenShift yeniden üretir |
| **Elle** — bir belgedeki adımla yaratılır | parolaları taşıyan Secret'lar (`s3-creds`, `polaris-root`, kaynakların `erp-db` gibi Secret'ları …) | **kimse geri getirmez**; yaratıldığı adım tekrarlanır |

Elle yaratılan nesneler, yaratıldıkları belgenin adını ve adımını taşıyan bir etiketle
işaretlenir. Etiketin adı `lakehouse.io/yonetim`, değeri `manual-` ile başlar, ardından
belgenin adı ve adım numarası gelir. Örneğin `manual-yeni-kaynak-ve-pipeline-4`, nesnenin
`yeni-kaynak-ve-pipeline.md` belgesinin 4. adımında yaratıldığını söyler.

Bir nesnenin nereden geldiğini şu betikle görürsünüz; betiğe bir ya da birden fazla
nesne adı verilir:

`[bastion]`

```bash
scripts/nesne-kaynagi.sh secret/universite-db kafkaconnect/connect secret/connect pod/connect-connect-0
```

**Beklenen çıktı** (örnek bir kurulumdan; `universite-db` orada bir kaynağın Secret'ıdır):

```text
secret/universite-db                         ELLE       docs/50-isletme/yeni-kaynak-ve-pipeline.md §4 — Kaynağın Secret'ını yaratın
kafkaconnect/connect                         GIT        ArgoCD uygulaması: glue
secret/connect                               TÜREV      KafkaUser/connect -> ArgoCD uygulaması: glue
pod/connect-connect-0                        TÜREV      StrimziPodSet/connect-connect -> KafkaConnect/connect -> ArgoCD uygulaması: glue
```

Bir türün ad alanındaki bütün nesnelerini tek seferde görmek için:

```bash
scripts/nesne-kaynagi.sh --liste secret
```

İkinci sütun `ELLE` olan bir nesneyi sildiyseniz, üçüncü sütundaki belge adımını
tekrarlayarak yeniden yaratın. `ETİKETSİZ` görünen nesneler belgelerin dışında elle
yaratılmıştır; kimin ve neden yarattığını ekibinizle doğrulayın.

---

## 8. Uygulama takılırsa

| Ne görüyorsunuz | Olası neden | Nereye bakın |
|---|---|---|
| `OutOfSync` kalıyor | ArgoCD Git deposunu okuyamıyor | `lakehouse-repo` Secret'ı (§5.1 Ters giderse) |
| `OutOfSync Progressing` dakikalarca sürüyor | bir pod açılamıyor | `oc -n "$LAKEHOUSE_NS" get pods` |
| `Synced Degraded` | nesne kuruldu ama sağlıksız (ör. bağlayıcı hata verdi) | `oc -n "$LAKEHOUSE_NS" get kafkaconnector` |
| `ComparisonError` ya da `rpc error` | ayar dosyasında eksik ya da hatalı bir ayar | §3.2'deki `helm template` komutu |
| Değişiklik uygulandı, sonra kendiliğinden geri döndü | nesne `oc edit` ile elle değiştirilmiş; ArgoCD Git'teki hâli geri yazdı | değişikliği Git'te yapın (§3) |

Bütün belirtiler ve çözümleri: [sorun-giderme.md](sorun-giderme.md) §3.

---

## Kontrol listesi

- [ ] Değişiklik yalnız `platform/values/site/` altındaki bir dosyada yapıldı.
- [ ] `scripts/check-site.sh` `check-site: OK` yazdı.
- [ ] Commit kurumun deposundaki `main` dalına gönderildi.
- [ ] `oc -n "$ARGOCD_NS" get application glue` `Synced Healthy` gösteriyor.
- [ ] Değişikliğin kümedeki sonucu (yeni nesne, yeni tablo, yeni ayar) görüldü.
- [ ] Ekipteki herkes geri almanın `git revert` ile yapıldığını, `oc edit` ile
      yapılmadığını biliyor.

## Sonraki bölüm

İşletme sayfalarını baştan sona okumak isteyenler için sıra:

1. [degisiklik-nasil-uygulanir.md](degisiklik-nasil-uygulanir.md) — değişikliğin yolu (bu sayfa)
2. [yeni-kaynak-ve-pipeline.md](yeni-kaynak-ve-pipeline.md) — yeni kaynak, nginx günlükleri
3. [mevcut-kaynaga-tablo-ekleme.md](mevcut-kaynaga-tablo-ekleme.md) — var olan kaynağa tablo eklemek
4. [kaynak-veya-tablo-silme.md](kaynak-veya-tablo-silme.md) — kaynak ya da tablo kaldırmak
5. [json-akisi-ekleme.md](json-akisi-ekleme.md) — uygulama ve IoT olay akışları
6. [yeni-spark-uygulamasi.md](yeni-spark-uygulamasi.md) — kendi Spark işiniz (`custom/`)
7. [kullanici-ve-yetki.md](kullanici-ve-yetki.md) — AD grupları ve erişim kuralları
8. [veri-metrikleri.md](veri-metrikleri.md) — tablo ölçümleri ve "Tablo sağlığı" panosu
9. [izleme-ve-alarmlar.md](izleme-ve-alarmlar.md) — izleme ve alarmlar
10. [gunluk-haftalik-kontroller.md](gunluk-haftalik-kontroller.md) — günlük ve haftalık kontroller
11. [sorun-giderme.md](sorun-giderme.md) — belirti, neden, çözüm
12. [yedek-ve-geri-donus.md](yedek-ve-geri-donus.md) — yedekler ve geri dönüş
13. [yukseltme.md](yukseltme.md) — sürüm yükseltme

Başvuru sayfaları ([90-referans/](../90-referans/oc-hizli-basvuru.md)) bu sıraya
girmez; ihtiyaç duyulduğunda açılır.
