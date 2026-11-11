# Yeni Spark uygulaması ekleme

Bu sayfa, ürünün kendi dosyalarına dokunmadan kendi Spark işinizi kurulumun parçası
yapmayı anlatır: betiği yazmak, onu çalıştıracak tanım dosyasını hazırlamak, Git'e
göndermek, koşuyu izlemek ve sonucu Trino'da görmek. Aynı yol, Spark dışındaki kendi
Kubernetes nesneleriniz için de geçerlidir (§11). §12, Gold katmanının bu yolla nasıl
yazıldığını ve Superset'te hangi tür veri setiyle kullanıldığını anlatır. Ek A, S3'te
hazır duran dosyaları tek seferde kataloğa kaydetmeyi anlatır.

**Ne kadar sürer:** ilk uygulamada 30 dakika; sonrakilerde 10 dakika ve koşu süresi.

**Hangi yetkiler gerekir:** kurumun lakehouse Git deposunda `main` dalına yazma;
`$LAKEHOUSE_NS` ad alanında okuma ve — yalnız tek seferlik işler için — nesne yaratma
(`oc apply`); sonucu sorgulamak için Trino'da `sandbox` şemasına yazabilen analist grubu
üyeliği.

**Komutlar nerede çalışır:**

- `[bastion]` — `git` ve `oc` araçlarının kurulu olduğu, `oc login` ile kümeye bağlı ve
  Git deposunun bir kopyasının (`~/lakehouse`) bulunduğu yönetim makinesi.
- `[pod]` — JupyterHub'da bir not defteri hücresi.

Sayfa boyunca örnek iş `gunluk_ozet.py`'dir: Silver `shop.orders` tablosunu gün bazında
özetler ve sonucu `sandbox.gunluk_ozet` tablosuna yazar. Başlangıç noktası depoda hazır
duran `custom/examples/ornek_rapor.py` dosyasıdır; kendi dosyanız onun kopyası olarak
başlar.

```text
betik (.py) -> ConfigMap -> tanım dosyası (SparkApplication) -> sürücü pod'u -> Iceberg tablosu
              zamanlı iş: commit + push, ArgoCD uygular   |   tek seferlik iş: elle oc apply
```

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
yüklenmemiştir ([30-kurulum](../30-kurulum.md) §1).

---

## 2. `custom/` klasörü nasıl çalışır

Git deposunun kökündeki `custom/` klasörü **size** aittir. Ürünün dosyaları (`glue/` ve
`platform/`) her ürün sürümünde yenilenir; `custom/` klasörüne ise dokunulmaz.

ArgoCD bu klasörü `custom` adlı ayrı bir uygulamayla izler ve ürün bileşenleri
kurulduktan **sonra** uygular. Kural ürünün geri kalanıyla aynıdır: Git'e ne koyarsanız
kümede o vardır; elle yapılan değişiklik geri alınır; Git'ten sildiğiniz nesne kümeden de
silinir — klasördeki son nesneyi silseniz bile.

Ürün kurulduğunda `custom/kustomization.yaml` dosyası boştur (`resources: []`). Bir
dosyayı kümeye göndermek için onu bu listeye yazarsınız:

```yaml
resources:
- spark-gunluk-ozet-zamanli.yaml
```

**Yeni bir ArgoCD uygulaması eklemeyin.** İhtiyacınız olan her nesne — `SparkApplication`,
`ScheduledSparkApplication`, `CronJob`, `ConfigMap`, `KafkaTopic` — var olan `custom`
uygulamasının altına girer.

Depoda hazır duran örnekler:

| Dosya | Ne yapar |
|---|---|
| `custom/examples/ornek_rapor.py` | Silver `shop.orders` → durum bazında sipariş sayısı → `sandbox.ornek_rapor` |
| `custom/examples/spark-tek-seferlik.yaml` | `SparkApplication` — tek seferlik koşu (uygulandığı anda çalışır) |
| `custom/examples/spark-zamanli.yaml` | `ScheduledSparkApplication` — aynı iş, her gece (örnekte askıda: `suspend: true`) |
| `custom/examples/dbt-cronjob.yaml` | Spark dışı örnek: dbt ile Gold tablosu üreten gecelik iş |
| `custom/examples/kustomization.yaml` | yukarıdakilerden hangilerinin uygulanacağı ve `.py` dosyasından ConfigMap üretimi |

Klasörün kısa anlatımı: [custom/README.md](../../custom/README.md).

**Tek dosyalık betik için kendi imajınız gerekmez.** Python kodu imaja gömülmez:
`custom/kustomization.yaml` içindeki `configMapGenerator` satırı `.py` dosyasından bir
ConfigMap üretir, tanım dosyası bu ConfigMap'i pod'da `/opt/job` altına bağlar. Çalışan
imaj ürünün kendi Spark imajıdır.

### 2.1 Sınır: ConfigMap en çok 1 MiB'dir

Kubernetes, bir ConfigMap'teki bütün dosyaların toplamını 1 MiB (1.048.576 bayt) ile
sınırlar; daha büyüğü yaratılmaz. Tek dosyalık bir PySpark betiği bu sınırın çok
altındadır (bu sayfadaki örnek 1 KB'dir). Sınıra takılanlar; birden çok modüle bölünmüş
paketler, depoya konmuş üçüncü parti kütüphaneler ve veri dosyası taşıyan işlerdir.

Büyük uygulamalar için iki yol vardır.

**(a) Kendi Spark imajınız.** Ürünün imajından türetilir, paketiniz içine kopyalanır ve
kurumun iç registry'sine gönderilir. Tanım dosyasında yalnız `image` satırı değişir.

```dockerfile
FROM apache/spark:4.1.0-java21-python3
COPY benim_paketim/ /opt/job/benim_paketim/
```

Tanım dosyasında:
`image: image-registry.openshift-image-registry.svc:5000/lakehouse/benim-spark:1.0` ve
`mainApplicationFile: local:///opt/job/benim_paketim/ana.py`. Ürün yükseltmesinde taban
imajın sürümünü de yükseltmeniz gerekir (§4.1).

**(b) Paketi S3'ten vermek (`pyFiles`).** Paket `.zip` olarak S3'e konur ve tanım
dosyasının `deps` bloğunda gösterilir. Spark onu sürücüye ve executor'lara kendisi
dağıtır; imaj üretmeye gerek kalmaz.

```yaml
spec:                            # zamanlı işte: spec.template altında
  mainApplicationFile: local:///opt/job/gunluk_ozet.py   # giriş dosyası yine ConfigMap'ten
  deps:
    pyFiles:
    - s3a://kurum-veri/spark-paketleri/benim_paketim.zip
```

Alanın adını kümenizde görebilirsiniz:

`[bastion]`

```bash
oc explain sparkapplication.spec.deps.pyFiles
```

**Beklenen çıktı** (zamanlı işte aynı alan
`scheduledsparkapplication.spec.template.deps.pyFiles` yolundadır):

```text
GROUP:      sparkoperator.k8s.io
KIND:       SparkApplication
VERSION:    v1beta2

FIELD: pyFiles <[]string>


DESCRIPTION:
    PyFiles is a list of Python files the Spark application depends on.
```

S3'teki paketi okuyabilmek için işin S3 kimliği gerekir. Katalogun her tablo için verdiği
geçici S3 anahtarı yalnız katalog tablolarında geçerlidir; paket dosyası için tanım
dosyasında yorum satırı olarak duran `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` çifti
açılır (§4.1).

**Ters giderse:**

- ConfigMap yaratılırken boyut hatası alırsanız §10'daki ilgili satıra bakın.
- `ModuleNotFoundError` alırsanız paket sürücüye ulaşmamıştır: (a) yolunda `COPY`
  hedefini, (b) yolunda `pyFiles` satırını ve S3 kimliğini kontrol edin.

---

## 3. Betiği yazın

`custom/examples/ornek_rapor.py` dosyasını `custom/` klasörüne `gunluk_ozet.py` adıyla
kopyalayın ve düzenleyin:

`[bastion]`

```bash
cp custom/examples/ornek_rapor.py custom/gunluk_ozet.py
vi custom/gunluk_ozet.py
```

Betikte **yalnız veri mantığı** bulunur. Katalog, S3 ve Iceberg ayarlarının hepsi tanım
dosyasından gelir.

**Tek istisna katalog kimliğidir.** Kimlik dosyalara yazılmaz: `polaris-spark`
Secret'ından `POLARIS_CREDENTIAL` ortam değişkenine, oradan da betikte katalog ayarına
verilir. Aşağıdaki örnekte `cred` ile başlayan dört satır **zorunludur**; olmazsa katalog
`401` hatası verir.

```python
"""Günlük özet: Silver shop.orders -> gün bazında adet ve tutar -> sandbox.gunluk_ozet."""
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

CATALOG = "lakehouse"

builder = SparkSession.builder.appName("gunluk-ozet")
cred = os.environ.get("POLARIS_CREDENTIAL")          # Secret'tan gelir; dosyaya yazılmaz
if cred:
    builder = builder.config(f"spark.sql.catalog.{CATALOG}.credential", cred)
spark = builder.getOrCreate()

spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.sandbox")
ozet = (spark.table(f"{CATALOG}.shop.orders")
        .withColumn("gun", F.to_date("updated_at"))
        .groupBy("gun")
        .agg(F.count("*").alias("adet"), F.sum("amount").alias("tutar")))
# createOrReplace: iş yeniden koşarsa tablo baştan yazılır, sonuç değişmez
ozet.writeTo(f"{CATALOG}.sandbox.gunluk_ozet").using("iceberg").createOrReplace()
print("GUNLUK_OZET_OK", ozet.count())
spark.stop()
```

Dört kural:

- **Okuma.** Ürünün tabloları `spark.table("lakehouse.shop.orders")` biçiminde okunur.
  SQL tercih ederseniz `spark.sql("select … from lakehouse.shop.orders")` aynı işi yapar.
- **Yazma yeri.** Kendi çıktılarınızı `sandbox` ad alanında (rapor tabloları için §12'deki
  `gold` ad alanında) tutun. Ürünün Bronze ve Silver ad alanlarına yazmayın: Silver'ı
  üreten ürün işi bozulur.
- **Tekrar koşabilirlik.** `createOrReplace()` tabloyu baştan yazar; iş ikinci kez
  koşarsa sonuç aynı kalır. Yalnız değişenleri yazmak istiyorsanız `MERGE INTO`
  kullanın (aşağıda).
- **Bitiş satırı.** Betiğin sonunda kendi işaret satırınızı yazdırın
  (`GUNLUK_OZET_OK …`). Koşunun başarılı olup olmadığı günlükte bu satırla aranır.

**Yalnız değişenleri yazmak — `MERGE INTO`.** Hedef tabloyu baştan yazmak yerine
değişen satırları güncellemek ve yenileri eklemek için:

```python
yeni.createOrReplaceTempView("yeni")
spark.sql(f"""
    MERGE INTO {CATALOG}.sandbox.gunluk_ozet t
    USING yeni s ON t.gun = s.gun
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")
```

`MERGE INTO` için hedef tablonun var olması gerekir. İlk koşuda tablo yoksa önce
`createOrReplace()` ile yaratın ya da `CREATE TABLE IF NOT EXISTS` yazın.

**Ters giderse:** `Table or view not found` görürseniz tablo adının başında katalog adı
eksiktir ya da ad alanını yanlış yazmışsınızdır. Var olan ad alanlarını
`spark.sql("show namespaces in lakehouse").show()` ile listeleyin.

---

## 4. Tanım dosyasını hazırlayın: tek seferlik mi, zamanlı mı

İki örnek dosyadan biri kopyalanır. Seçim, işin **tekrar edip etmediğine** göre yapılır:

| | `SparkApplication` (tek seferlik) | `ScheduledSparkApplication` (zamanlı) |
|---|---|---|
| Ne zaman koşar | uygulandığı **anda** | `schedule` satırındaki zamana göre (saatler UTC) |
| Nasıl uygulanır | elle, `oc apply -f` ile | Git'e gönderilerek (ArgoCD uygular) |
| Ne için | bir kerelik doldurma, deneme | düzenli rapor, düzenli dönüşüm |
| Örnek dosya | `custom/examples/spark-tek-seferlik.yaml` | `custom/examples/spark-zamanli.yaml` |

> **Tek seferlik işi Git'e koymayın.** `SparkApplication` uygulandığı anda koşar. Git'te
> durursa ArgoCD onu her eşitlemede yeniden yaratır ve iş **kendiliğinden tekrar koşar**;
> başarısız bir koşu da `custom` uygulamasını `Degraded` gösterir. Tek seferlik işler elle
> uygulanır (§5.2), tekrar edenler Git'ten geçer (§5.1).

Kopyalanan dosyada **üç** şey değişir: işin adı, çalışacak `.py` dosyasının yolu ve kodun
geldiği ConfigMap'in adı.

> **İki dosyada bu üç alan aynı yerde durmaz.** Tek seferlik dosyada işin tanımı
> doğrudan `spec:` altındadır. Zamanlı dosyada `spec:` yalnız zamanlama ayarlarını taşır;
> işin tanımının tamamı bir düzey aşağıda, **`spec.template:`** altındadır. Alan yanlış
> düzeye yazılırsa küme dosyayı reddeder.

**Tek seferlik iş:**

`[bastion]`

```bash
cp custom/examples/spark-tek-seferlik.yaml custom/spark-gunluk-ozet-tek.yaml
vi custom/spark-gunluk-ozet-tek.yaml
```

Değişecek satırlar:

```yaml
metadata:
  name: gunluk-ozet                             # sürücü pod'unun adı gunluk-ozet-driver olur
spec:
  mainApplicationFile: local:///opt/job/gunluk_ozet.py
  volumes:
  - name: job
    configMap: {name: gunluk-ozet}              # ConfigMap'in adı
```

**Zamanlı iş:**

```bash
cp custom/examples/spark-zamanli.yaml custom/spark-gunluk-ozet-zamanli.yaml
vi custom/spark-gunluk-ozet-zamanli.yaml
```

Değişecek satırlar; aynı üç alan burada `template:` altındadır:

```yaml
metadata:
  name: gunluk-ozet-zamanli        # her koşu bu adın sonuna zaman damgası eklenerek adlanır
spec:
  schedule: "0 4 * * *"          # her gece 04:00 UTC
  suspend: false                 # örnekte true; işin gerçekten zamanlanması için false
  concurrencyPolicy: Forbid      # önceki koşu bitmeden yenisi başlamaz
  successfulRunHistoryLimit: 3
  failedRunHistoryLimit: 3
  template:                      # buradan aşağısı tek seferlik dosyanın spec: içeriğidir
    mainApplicationFile: local:///opt/job/gunluk_ozet.py
    volumes:
    - name: job
      configMap: {name: gunluk-ozet}
    # sparkConf, driver, executor ve diğer her şey de template: altında kalır
```

**Ters giderse:** zamanlı dosyada alanları `template:` altına indirmeyi unutursanız küme
dosyayı reddeder ve yanlış yerdeki alanların adlarını söyler (örnek):

```text
Error from server (BadRequest): error when creating "yanlis-zamanli.yaml": ScheduledSparkApplication in version "v1beta2" cannot be handled as a ScheduledSparkApplication: strict decoding error: unknown field "spec.mainApplicationFile", unknown field "spec.volumes"
```

**Zamanlı iş için ConfigMap satırı.** `.py` dosyasının kümeye ulaşması için
`custom/kustomization.yaml` dosyasına ConfigMap üretim satırı da eklenir. Dosyayı açın:

```bash
vi custom/kustomization.yaml
```

`resources: []` satırını şu satırlarla değiştirin (dosyada başka kaynaklar varsa yalnız
eksik satırları ilgili listenin sonuna ekleyin):

```yaml
resources:
- spark-gunluk-ozet-zamanli.yaml
configMapGenerator:
- {name: gunluk-ozet, files: [gunluk_ozet.py], options: {disableNameSuffixHash: true}}
```

`disableNameSuffixHash: true` gereklidir: tanım dosyası ConfigMap'i sabit adıyla arar,
adın sonuna rastgele ek gelmemelidir.

**Saat seçerken ürünün işleriyle çakıştırmayın.** Ürünün bakım işleri ve `silver-merge`
aynı düğümlerde çalışır; kendi işinizi onların arasına değil, sonrasına koyun. Ürünün iş
saatlerini §6'daki `oc get scheduledsparkapplication` çıktısında görürsünüz.

### 4.1 Değiştirilmeyen ve kuruma özel satırlar

Örnek dosyalardaki `sparkConf`, `driver` ve `executor` blokları ürünün kendi Spark
işleriyle aynıdır: aynı Iceberg sürümü, aynı katalog, aynı servis hesabı. Çoğu satır
olduğu gibi kalır. Kurumunuza göre değişecek satırlar dosyada `# SİTE` ile işaretlidir ve
`platform/values/site/glue.yaml` dosyasındaki değerlerle aynı olmalıdır:

| Tanım dosyasındaki satır | Neyle aynı olmalı |
|---|---|
| `spark.sql.catalog.lakehouse.s3.endpoint` | site dosyasında `s3.endpoint` |
| `spark.sql.catalog.lakehouse.client.region` | site dosyasında `s3.region` |
| `spark.sql.catalog.lakehouse.s3.path-style-access` | `s3.pathStyleAccess` (site dosyasında yoksa ürünün varsayılanı) |
| `spark.sql.catalog.lakehouse.header.X-Iceberg-Access-Delegation` | site dosyasında `s3.vendedCredentials` açıksa `vended-credentials`, kapalıysa `none` |
| `image`, `sparkVersion`, `spark.jars.packages` içindeki Iceberg sürümü | ürün sürümleri; ürün yükseltmesinde birlikte güncellenir |
| `ivysettings` geçen dört satır | Spark kütüphaneleri küme içi Maven önbelleğinden gelir; olduğu gibi kalır |
| `s3-ca` geçen **yorumlu** satırlar (iki `extraJavaOptions`, iki bağlama, bir birim) | site dosyasında `s3.caSecret` doluysa beşi birden açılır ([30-kurulum](../30-kurulum.md) §5.14); açılmazsa iş S3'e ilk istekte `PKIX path building failed` ile düşer |

Bu değerleri ürünün kendi işinden görmek en kolay yoldur:

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" get scheduledsparkapplication silver-merge -o json \
  | jq -r '.spec.template.sparkConf | to_entries[] | select(.key|test("s3|region|Delegation|extraJava")) | "\(.key): \(.value)"'
```

Çıktıdaki her satır, tanım dosyanızdaki aynı adlı satırla aynı değeri taşımalıdır.

**Ürün yükseltmesinde sürümleri siz güncellersiniz.** Spark ya da Iceberg sürümü
değişirse `custom/` altındaki dosyalarınızdaki `image`, `sparkVersion` ve
`spark.jars.packages` satırlarını da güncelleyin; ürün sizin dosyalarınıza dokunmaz.

**S3 anahtarı.** Ürün varsayılan olarak her tablo için katalogdan geçici S3 anahtarı
alır; tanım dosyasında S3 anahtarı bulunmaz. Kurumunuzda bu özellik kapalıysa
(`s3.vendedCredentials: false`) delegation satırı `none` yapılır ve `driver` ile
`executor` bloklarındaki yorumlu `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` satırları
açılır; değerler `s3-creds` Secret'ından **adıyla** okunur.

> **Parolalar Git'e girmez.** Tanım dosyaları Secret'lara yalnız adlarıyla başvurur
> (`secretKeyRef: {name: polaris-spark, key: credential}`). Bu kural `custom/` için de
> geçerlidir ([90-referans/secret-listesi.md](../90-referans/secret-listesi.md)).

---

## 5. Uygulayın

### 5.1 Zamanlı iş — Git'e gönderin

Değişikliğin genel yolu: [değişiklik nasıl uygulanır](degisiklik-nasil-uygulanir.md).

`[bastion]`

```bash
git diff
git add custom/gunluk_ozet.py custom/spark-gunluk-ozet-zamanli.yaml custom/kustomization.yaml
git commit -m "custom: gunluk-ozet zamanlı Spark işi"
git push origin main
oc -n "$ARGOCD_NS" annotate application custom argocd.argoproj.io/refresh=normal --overwrite
```

Son satır ArgoCD'nin depoyu beklemeden okumasını sağlar. Bir dakika sonra durumu kontrol
edin:

```bash
oc -n "$ARGOCD_NS" get application custom \
  -o jsonpath='{.status.sync.status} {.status.health.status}{"\n"}'
oc -n "$LAKEHOUSE_NS" get scheduledsparkapplication gunluk-ozet-zamanli
```

**Beklenen çıktı:** ilk satır `Synced Healthy`; ikinci komut işinizi listeler.

**Ters giderse:**

- `Synced Degraded`: içerideki bir nesne sağlıksızdır. Hangisi olduğunu
  `oc -n "$ARGOCD_NS" describe application custom | tail -30` söyler.
- Beş dakika sonra hâlâ `OutOfSync`: değişiklik Git'e gitmemiştir; `git log origin/main
  -1` ile kontrol edin.

### 5.2 Tek seferlik iş — elle uygulayın

Tanım dosyası Python kodunu ConfigMap'ten alır; bu yüzden **önce ConfigMap** yaratılır.
ConfigMap'i elle yarattığınız için etiketle işaretleyin
([değişiklik nasıl uygulanır](degisiklik-nasil-uygulanir.md) §7).

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" create configmap gunluk-ozet --from-file=custom/gunluk_ozet.py
oc -n "$LAKEHOUSE_NS" label configmap gunluk-ozet lakehouse.io/yonetim=manual-yeni-spark-uygulamasi-5.2
oc -n "$LAKEHOUSE_NS" delete sparkapplication gunluk-ozet --ignore-not-found
oc apply -f custom/spark-gunluk-ozet-tek.yaml
```

Son komutta `-n` yoktur: ad alanı dosyanın içindeki `namespace: lakehouse` satırından
gelir. Ad alanı adı üründe sabittir ([30-kurulum.md §4.2](../30-kurulum.md#ad-alani-sabit)).

Üçüncü satır, aynı adla duran eski bir koşuyu siler; eski (özellikle `FAILED`) bir
`SparkApplication` yerinde dururken aynı adla yeni koşu başlamaz.

**Beklenen çıktı** (örnek; eski koşu yoksa `delete` satırı bir şey yazmaz):

```text
configmap/gunluk-ozet created
configmap/gunluk-ozet labeled
sparkapplication.sparkoperator.k8s.io/gunluk-ozet created
```

Depodaki hazır örneği olduğu gibi denemenin kısa yolu
[90-referans/oc-hizli-basvuru.md](../90-referans/oc-hizli-basvuru.md) §12'dedir.

**Ters giderse:** sürücü pod'unun olaylarında ConfigMap bulunamadı hatası varsa ilk satır
atlanmıştır.

**İşiniz bitince ikisini birden silin.** `SparkApplication` silindiğinde ConfigMap kümede
kalır:

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" delete sparkapplication gunluk-ozet --ignore-not-found
oc -n "$LAKEHOUSE_NS" delete configmap gunluk-ozet --ignore-not-found
```

---

## 6. Koşuyu izleyin

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" get sparkapplication gunluk-ozet
```

Ad yazmadan `oc -n "$LAKEHOUSE_NS" get sparkapplication` komutu bütün koşuları
listeler.

**Beklenen çıktı** (örnek; koşu bittikten sonra. İş sürerken `STATUS` sütunu `RUNNING`
olur ve `FINISH` sütunu boştur):

```text
NAME          SUSPEND   STATUS      ATTEMPTS   START                  FINISH                 AGE
gunluk-ozet             COMPLETED   1          2026-09-24T10:26:26Z   2026-09-24T10:41:22Z   21m
```

`STATUS` sütunundaki değerler:

| Durum | Anlamı | Ne yapılır |
|---|---|---|
| `SUBMITTED` | iş gönderildi, sürücü pod'u henüz başlamadı | bekleyin |
| `RUNNING` | sürücü çalışıyor | bekleyin; günlüğe bakın |
| `COMPLETED` | iş başarıyla bitti | sonucu kontrol edin (§8) |
| `FAILED` | iş hata ile bitti | sürücü günlüğü (§7) ve §10 tablosu |
| `SUBMISSION_FAILED` | iş hiç başlayamadı (tanım dosyası hatalı, imaj yok, kota) | `oc -n "$LAKEHOUSE_NS" describe sparkapplication gunluk-ozet` |
| `INVALIDATING`, `PENDING_RERUN` | zamanlı işin sonraki koşusu hazırlanıyor | bekleyin |

Zamanlı işin her koşusu ayrı bir `SparkApplication` olarak görünür; adı zamanlı işin
adıyla başlar ve sonunda bir zaman damgası taşır. Zamanlı işlerin kendisi ayrı listelenir:

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" get scheduledsparkapplication
```

**Beklenen çıktı** (OpenShift provasından; `gold-kabul` bir kurumun kendi Gold işidir —
§12 —, diğerleri ürünün işleridir):

```text
NAME                      SCHEDULE        TIMEZONE   SUSPEND   LAST RUN   LAST RUN NAME                                 AGE
gold-kabul                20,50 * * * *              false     13m        gold-kabul-1794415800001835116                105m
maint-compact             0 */6 * * *                false     5h3m       maint-compact-1794398400001603671             3d2h
maint-expire-orphan-ttl   30 3 * * *                 false     13h        maint-expire-orphan-ttl-1794367800002123397   3d2h
maint-position-deletes    0 * * * *                  false     3m10s      maint-position-deletes-1794416400001330020    3d2h
mongo-bronze              */5 * * * *                false     3m10s      mongo-bronze-1794416400001451475              2d21h
silver-merge              */15 * * * *               false     3m10s      silver-merge-1794416400001234349              3d2h
```

**Koşu kaydı 24 saat durur.** Örneklerde `timeToLiveSeconds: 86400` vardır: koşu ve
sürücü pod'u bittikten 24 saat sonra silinir. Günlüğü bu süre içinde alın ya da değeri
büyütün.

**Ters giderse:** liste boşsa tanım dosyası uygulanmamıştır. Sürücü pod'u `Pending`
kalıyorsa düğümde yeterli CPU ya da bellek yoktur (§9).

---

## 7. Sürücü günlüğünü okuyun

Sürücü pod'unun adı, işin adının sonuna `-driver` eklenerek oluşur.

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" logs gunluk-ozet-driver --tail=2000 | grep -E "GUNLUK_OZET_OK|Exception"
```

**Beklenen çıktı** (örnek; sayı, yazılan satır adedidir):

```text
GUNLUK_OZET_OK 1
```

> **İlk koşuda bir `Exception` satırı daha görürsünüz; bu normaldir.** Hedef tablo henüz
> yokken `createOrReplace` önce tabloyu arar, bulamaz ve bunu günlüğe yazar:
>
> ```text
> org.apache.iceberg.exceptions.RESTException: Unable to process (code: 404, type: NoSuchTableException): Table does not exist: sandbox.gunluk_ozet
> ```
>
> Önemli olan `_OK` satırının **olmasıdır**; yoksa koşu gerçekten başarısız olmuştur.

Koşu sürerken günlüğü canlı izlemek için `--tail=20 -f` kullanın. Günlüğün başında
Spark'ın kütüphaneleri küme içi Maven önbelleğinden aldığını gösteren satırlar
(`found ... in lakehouse-maven`) görünür; bu normaldir.

**Ters giderse:** sürücü pod'u bulunamıyorsa koşu 24 saatten eskidir ve silinmiştir; işi
tekrar koşturun. Günlükte başka bir `Exception` varsa §10 tablosuna bakın.

---

## 8. Sonucu Trino'da kontrol edin

Yazdığınız tablo, ürünün tabloları gibi Trino, Superset, JupyterHub ve Zeppelin'de
görünür.

`[pod]` (JupyterHub not defteri hücresi)

```python
import os, trino
conn = trino.dbapi.connect(host=os.environ["TRINO_HOST"], port=8443, http_scheme="https",
                           verify="/etc/lakehouse-ca/tls.crt",
                           auth=trino.auth.OAuth2Authentication(), catalog="lakehouse")
cur = conn.cursor()
cur.execute("select gun, adet, tutar from sandbox.gunluk_ozet order by gun")
cur.fetchall()
```

**Beklenen çıktı** (örnek; değerler sizin verinize göre değişir):

```text
[[datetime.date(2026, 9, 18), 3, Decimal('14.50')]]
```

**Ters giderse:**

- Tablo bulunamadı: iş `COMPLETED` olmamıştır ya da tabloyu başka bir ad alanına
  yazmıştır.
- `Access Denied`: kullanıcınız `sandbox` şemasına erişen analist grubunda değildir.

---

## 9. Kaynak ayarı

Örnek dosyalardaki değerler küçük işler içindir. Büyük tablolarda işin ihtiyacına göre
büyütülür.

| Alan | Anlamı | Nasıl seçilir |
|---|---|---|
| `driver.cores` | Spark'ın sürücüye ayırdığı çekirdek sayısı (tam sayı) | sonucu sürücüde toplamayan işlerde 1 yeter |
| `driver.coreRequest` | düğümden istenen CPU (kesirli olabilir) | dar düğümde `500m`; boşsa `cores` kadar istenir |
| `driver.memory` | sürücünün belleği | sonucu sürücüde toplamıyorsanız 1–2 GB |
| `executor.instances` | paralel çalışan executor sayısı | tablo boyutuyla artar |
| `executor.memory` | her executor'ın belleği | bellek hatası görülene kadar büyütülmez; önce `spark.sql.shuffle.partitions` artırılır |
| `spark.sql.shuffle.partitions` | ara sonuçların bölündüğü parça sayısı | küçük kümede 8; her parça 100–200 MB olacak şekilde |

`cores` ile `coreRequest` farklı şeylerdir: ilki Spark'ın içindeki paralellik, ikincisi
düğümden istenen yerdir. Dar bir düğümde `coreRequest` yüksekse pod `Pending` kalır.

---

## 10. İzleme ve hata tablosu

**Ürünün alarmları sizin işlerinizi kapsamaz.** "Zamanlı Spark işi başarısız" alarmı
yalnız ürünün kendi işlerini (`silver-merge`, bakım işleri, `mongo-bronze`) izler;
`custom/` altındaki işlerinizin başarısız koşusu bu alarmı tetiklemez. Tek istisna, iş
hiç başlayamadığında (gönderim hatası) çıkan alarmdır: o sayaç bütün işleri birlikte
sayar ([izleme-ve-alarmlar.md](izleme-ve-alarmlar.md)). Kendi işlerinizin durumunu
`oc -n "$LAKEHOUSE_NS" get sparkapplication` ile, zamanlı işlerde ayrıca `custom`
uygulamasının durumuyla izleyin. İşinize özel alarm istiyorsanız kurumun kendi izleme
kuralını yazarsınız.

| Ne görüyorsunuz | Neden | Ne yapılır |
|---|---|---|
| `Ivy settings file /opt/ivy/ivysettings.xml does not exist` | küme içi Maven önbelleği kapalı ve kurum aynası tanımlı değil | tanım dosyasındaki dört `ivysettings` satırını silin |
| `ClassNotFoundException: org.apache.iceberg.spark.SparkCatalog` ya da `unresolved dependency` | `ivysettings` satırları eksik ya da önbellek kütüphaneyi indiremedi | §4.1'deki satırları kontrol edin; önbellek için [sorun-giderme.md](sorun-giderme.md) §6.2 |
| İlk koşu her zamankinden uzun, günlükte indirme satırları | kütüphaneler sürücüye ilk kez iniyor | bekleyin |
| `401 Unauthorized` (katalog) | betikte `cred` satırları yok ya da tanım dosyasında `POLARIS_CREDENTIAL` tanımlı değil | §3'teki dört satır ve tanım dosyasındaki `secretKeyRef` |
| `403 Forbidden` (katalog) | o ad alanına yazma yetkisi yok | `sandbox` ya da `gold` ad alanına yazın |
| `NoSuchNamespaceException` | hedef ad alanı katalogda yok | betiğe `CREATE NAMESPACE IF NOT EXISTS` satırını ekleyin |
| `OutOfMemoryError` ya da executor'ın bellek aşımıyla durdurulması | veri belleğe sığmıyor | önce `spark.sql.shuffle.partitions`, sonra `executor.memory` ve `memoryOverhead` |
| `ImagePullBackOff` | küme imajı çekemiyor | imajı iç registry'ye kopyalayın, `image` satırını değiştirin |
| Sürücü ya da executor pod'u `Pending` | düğümde istenen CPU/bellek yok | `coreRequest` ve `memory` değerlerini düşürün (§9) |
| `SUBMISSION_FAILED` | tanım dosyası şemaya uymuyor ya da servis hesabı yok | `oc -n "$LAKEHOUSE_NS" describe sparkapplication gunluk-ozet` |
| Pod başlamıyor, olaylarda ConfigMap bulunamadı | ConfigMap yaratılmadı | §5.2'nin ilk satırı |
| `Too long: may not be more than 1048576 bytes` | betikler 1 MiB ConfigMap sınırını aşıyor | §2.1: kendi imajınız ya da `pyFiles` |
| `ModuleNotFoundError` (kendi modülünüz) | paket sürücüye ulaşmadı | §2.1 |
| `custom` uygulaması `Degraded` | içinde `FAILED` bir `SparkApplication` duruyor | tek seferlik işi Git'ten çıkarın (§4 kutusu); başarısız koşuyu silin |

Bir tanım dosyasını kümeye yazmadan denemek için `--dry-run=server` kullanılır; küme
dosyayı gerçekten kontrol eder ama nesne yaratmaz:

`[bastion]`

```bash
oc apply -f custom/spark-gunluk-ozet-tek.yaml --dry-run=server
```

**Beklenen çıktı** (örnek):

```text
sparkapplication.sparkoperator.k8s.io/gunluk-ozet configured (server dry run)
```

---

## 11. Spark dışı nesneler

Aynı klasör ve aynı yol, Spark'la ilgisi olmayan nesneler için de geçerlidir: bir
`CronJob`, ek bir `ConfigMap`, kendi `KafkaTopic`'iniz. Dosya `custom/` klasörüne konur,
`custom/kustomization.yaml` içindeki `resources:` listesine yazılır ve Git'e gönderilir.

Depodaki `custom/examples/dbt-cronjob.yaml` bunun çalışan bir örneğidir: Trino üzerinden
Gold tablosu üreten bir dbt koşusunu her gece çalıştırır. Örnek ayrıca bir `dbt-project`
ConfigMap'i ve `dbt-trino` Secret'ı ister
([examples/dbt/README.md](../../examples/dbt/README.md)).

Kural §2'dekiyle aynıdır: yeni bir ArgoCD uygulaması eklenmez; her şey `custom`
uygulamasının altına girer.

---

## 12. Gold katmanı ve Superset veri setleri

### 12.1 Bronze, Silver, Gold

Lakehouse'ta veri üç katmandan geçer:

| Katman | Ne tutar | Kim üretir |
|---|---|---|
| **Bronze** (ör. `erp_raw`) | kaynaktaki her değişikliğin kaydı: ekleme, güncelleme, silme | ürün (Kafka Connect), kendiliğinden |
| **Silver** (ör. `erp`) | her kaydın güncel hâli; kaynak tablonun bir kopyası | ürün (`silver-merge`, 15 dakikada bir) |
| **Gold** (ör. `gold`) | iş kuralı uygulanmış, rapora hazır tablolar | **kurum** — ürünle gelmez |

Gold tabloları kurumun sorularına göre yazılır: "birim bazında öğrenci sayısı", "yıl
bazında yayın sayısı" gibi. Önerilen yol bu sayfadaki **zamanlı Spark işidir**:

1. Katalogda `gold` adlı bir ad alanı açın: `platform/polaris/setup.yaml` dosyasındaki
   `namespaces:` listesine `- name: gold` satırını ekleyip katalog kurulum betiğini
   çalıştırın ([yeni-kaynak-ve-pipeline.md](yeni-kaynak-ve-pipeline.md) §5).
2. Silver tablolarından okuyup sonucu `gold` ad alanına yazan bir betik yazın (§3).
3. Betiği zamanlı iş olarak Git'e gönderin (§4, §5.1). Silver 15 dakikada bir
   güncellendiği için Gold işini saatte bir ya da iki kez koşturmak yeterlidir.

Betiğin özü kısadır; Silver'dan SQL ile okuyup sonucu `createOrReplace` ile yazar.
`createOrReplace` her koşuda tabloyu baştan yazar, bu yüzden iş istendiği kadar tekrar
koşturulabilir:

```python
rapor = spark.sql(f"""
    SELECT b.birim_adi, count(DISTINCT o.ogrenci_id) AS ogrenci_sayisi
    FROM {CATALOG}.universite.ogrenci_akademik_program o
    JOIN {CATALOG}.universite.program p ON p.program_id = o.program_id
    JOIN {CATALOG}.universite.birim b   ON b.birim_id   = p.akademik_birim_id
    WHERE o.ogrenci_durumu = 'Aktif'
    GROUP BY b.birim_adi
""")
rapor.writeTo(f"{CATALOG}.gold.birim_ogrenci").using("iceberg").createOrReplace()
print("GOLD_OK", rapor.count())
```

Gold'u dbt ile yazmayı tercih ederseniz §11'deki dbt örneği aynı işi SQL dosyalarıyla
yapar.

### 12.2 Superset'te fiziksel ve sanal veri seti

Superset'te her grafik bir **veri setine** dayanır. İki tür veri seti vardır:

| | Fiziksel veri seti | Sanal (virtual) veri seti |
|---|---|---|
| Nedir | katalogdaki bir tabloya doğrudan bakar (ör. bir Gold tablosu) | Superset'te kaydedilmiş bir SQL sorgusudur; **tablo değildir** |
| Ne zaman hesaplanır | tablo, Gold işi koştuğunda bir kez yazılır; grafik hazır sonucu okur | sorgu her grafik açılışında Trino'da yeniden çalışır |
| Nerede durur | katalogda; hesap kuralı Git'te (Gold işi) | yalnız Superset'in kendi veritabanında |
| Kim görür | Trino'ya bağlanan her araç (Superset, JupyterHub, Zeppelin) | yalnız Superset |

Hangisini seçmeli:

- **Sanal veri seti:** hızlı bir deneme, tek bir panoda kullanılan küçük bir sorgu.
- **Gold tablo + fiziksel veri seti:** birden çok panoda kullanılan, hesaplaması ağır
  olan ya da başka araçların da göreceği her sonuç. Hesap kuralı Git'te durur ve
  sürümlenir; panolar hızlı açılır.

Bir sanal veri seti zamanla önemli hâle gelirse sorgusu bir Gold işine taşınır ve
veri seti fiziksel olanla değiştirilir.

**Örnek.** Kabul gösteriminde iki rapor bu yolla kuruldu: zamanlı bir Spark işi Silver
`universite.*` tablolarından `gold.kabul_senaryo1` ve `gold.kabul_senaryo2` tablolarını
saatte iki kez yazar; Superset panosu bu iki tabloyu fiziksel veri seti olarak kullanır.
Tabloları katalogda görmek için:

`[pod]` (JupyterHub not defteri hücresi; bağlantı §8'deki gibi)

```python
cur = conn.cursor(); cur.execute("show tables from gold"); cur.fetchall()
```

**Beklenen çıktı** (OpenShift provasından; sizde kendi Gold tablolarınız görünür):

```text
[['kabul_senaryo1'], ['kabul_senaryo2']]
```

---

## Ek A. S3'te duran dosyaları tek seferde kataloğa kaydetme

Kaynak sistemi olmayan, S3'te hazır duran CSV ya da Parquet dosyalarını doğrudan bir
Iceberg tablosuna yazmak için ürünle gelen bir betik vardır: `s3_register_example.py`.
Betik ürünün kendi iş dosyalarıyla birlikte zaten kümededir; bu yüzden ConfigMap
yaratmanıza gerek yoktur, yalnız tek seferlik bir iş tanımı gerekir.

En kolay yol, ürünün `silver-merge` işinin tanımından tek seferlik bir iş türetmektir;
böylece katalog, kimlik ve S3 ayarları ürünün işiyle birebir aynı olur:

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" get scheduledsparkapplication silver-merge -o json \
  | jq '{apiVersion:"sparkoperator.k8s.io/v1beta2", kind:"SparkApplication",
         metadata:{name:"s3-register-once", namespace:.metadata.namespace},
         spec:(.spec.template + {
           mainApplicationFile:"local:///opt/job/s3_register_example.py",
           arguments:["--source","s3://kurum-veri/musteri-dosyalari/","--format","csv",
                      "--table","sandbox.musteri_dosyalari","--header","true"]})}' \
  | oc apply -f -
```

`--source` dosyaların S3 yolu, `--table` hedef tablonun ad alanıyla birlikte tam adıdır.
Uygulamadan önce aynı komutun sonunu `| oc apply --dry-run=server -f -` yaparak
denetleyebilirsiniz.

Koşuyu izleyin ve sonucu görün:

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" get sparkapplication s3-register-once
oc -n "$LAKEHOUSE_NS" logs s3-register-once-driver --tail=2000 | grep S3_REGISTER_OK
```

**Beklenen çıktı** (örnek; satır sayısı dosyalarınıza göre değişir):

```text
S3_REGISTER_OK s3://kurum-veri/musteri-dosyalari/ -> lakehouse.sandbox.musteri_dosyalari (10432 satır)
```

İş bitince tanımını silin. Silinecek bir ConfigMap yoktur; iş ürünün kendi iş
dosyalarını kullanır.

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" delete sparkapplication s3-register-once
```

Bilinmesi gereken üç şey:

- **Hedef tablo baştan yazılır.** Tablo varsa içeriği tamamen değişir. Yeni dosyaları
  eklemek gerekiyorsa §3'teki `MERGE INTO` kalıbını kullanan kendi işinizi yazın.
- **CSV başlığı.** `--format csv` için `--header true` (varsayılan) ilk satırı kolon
  adı sayar; `false` saymaz. `parquet` için bu ayar yok sayılır.
- **Ad alanı.** Hedef ad alanı yoksa yaratılır. Kendi yüklemelerinizi `sandbox` altında
  tutun.

---

## Kontrol listesi

- [ ] Betik `custom/` klasöründe ve yalnız veri mantığı içeriyor; `cred` satırları
      yerinde.
- [ ] Tanım dosyası kopyalandı; ad, `mainApplicationFile` ve ConfigMap adı değiştirildi —
      **zamanlı dosyada bu üç alan `spec.template:` altında**.
- [ ] Zamanlı işte `configMapGenerator` satırı `custom/kustomization.yaml` dosyasında.
- [ ] `# SİTE` işaretli satırlar `platform/values/site/glue.yaml` ile aynı.
- [ ] Tekrar eden iş zamanlı; tek seferlik iş Git'e **konmadı**.
- [ ] İşin saati ürünün bakım ve `silver-merge` koşularıyla çakışmıyor.
- [ ] Koşu `COMPLETED`; sürücü günlüğünde kendi bitiş satırınız var.
- [ ] Sonuç tablosu Trino'dan okunabiliyor.
- [ ] Ürün yükseltmesinden sonra `image`, `sparkVersion` ve Iceberg sürümü güncellendi.

## Sonraki bölüm

[kullanici-ve-yetki.md](kullanici-ve-yetki.md) — kimin hangi veriyi göreceği (sıranın
tamamı: [değişiklik nasıl uygulanır](degisiklik-nasil-uygulanir.md) "Sonraki bölüm").

İlgili sayfalar: yazdığınız tabloyu verisiyle birlikte kaldırmak için
[kaynak-veya-tablo-silme.md](kaynak-veya-tablo-silme.md) §6.2; alarmlar için
[izleme-ve-alarmlar.md](izleme-ve-alarmlar.md); belirti tablosu için
[sorun-giderme.md](sorun-giderme.md).
