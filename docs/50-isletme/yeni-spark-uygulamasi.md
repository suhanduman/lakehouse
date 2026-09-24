# Yeni Spark uygulaması ekleme

**Bu bölümde:** ürün chart'ına dokunmadan kendi Spark işinizi — ve aynı yolla diğer özel
Kubernetes kaynaklarınızı — kurulumun parçası yapmanın tam yolu: `custom/` klasörünün
GitOps mekanizması, PySpark betiğinin iskeleti, `SparkApplication` ve
`ScheduledSparkApplication` CR'larının uyarlanması, koşunun izlenmesi, sonucun Trino'da
görülmesi, kaynak ayarı, izleme ve hata tablosu. Son bölüm (Ek A), kaynak sistemi olmayan
bir S3 klasörünü tek seferde kataloğa kaydetmeyi anlatır.
**Süre:** ilk uygulamada 30 dakika; sonrakilerde 10 dakika + koşu süresi. Kümede Maven
önbelleği boşken ilk koşuda Iceberg paketlerinin inmesi birkaç dakika sürer.
**Gereken yetki:** kurumun Git deposunda `main` dalına yazma; `$LAKEHOUSE_NS` ad alanında
okuma ve — yalnız tek seferlik işler için — `apply`; Trino'da `sandbox` şemasına yazma
(analist grubu).
**Nerede çalıştırılır:** `[bastion]` küme ve Git komutları için; `[pod]` JupyterHub not
defteri hücresi (sonucun sorgulanması).

**Bu bölümün işlenmiş örneği `gunluk_ozet.py`'dir:** Silver `shop.orders` tablosunu gün
bazında özetler ve sonucu `sandbox.gunluk_ozet` tablosuna yazar. Örnek geliştirme
kümesinde birebir koşturulmuştur; aşağıdaki çıktıların çoğu o koşudan alınmıştır. Başlangıç
noktası depoda hazır duran `custom/examples/ornek_rapor.py` örneğidir; kendi dosyanız onun
kopyası olarak başlar.

```text
betik (.py) -> ConfigMap -> CR (SparkApplication) -> sürücü pod'u -> Iceberg tablosu
                         commit + push -> ArgoCD (zamanlı)  |  elle apply (tek seferlik)
```

---

## 1. Değişkenleri yükleyin

`[bastion]`

```bash
cd ~/lakehouse                      # depoyu `git clone` ile nereye indirdiyseniz orası
set -a; . install/lakehouse.env; set +a
echo "ns=$LAKEHOUSE_NS argocd=$ARGOCD_NS"
```

**Beklenen çıktı** (örnek — kendi değerlerinizle):

```text
ns=lakehouse argocd=openshift-gitops
```

**Ters giderse:** boş satır görürseniz dosya yüklenmemiştir
([30-kurulum](../30-kurulum.md) §1).

---

## 2. `custom/` klasörü nasıl çalışır

Deponun kökündeki `custom/` klasörü **size** aittir. Ürün chart'ı (`glue/`) ve
`platform/` altındaki dosyalar ürünün kendisidir; bir sonraki sürümde üzerlerine yazılır.
Kendi kaynaklarınız yalnız `custom/` içinde durur ve ürün yükseltmesinden etkilenmez.

ArgoCD bu klasörü `custom` adlı ayrı bir Application ile izler
(`platform/apps/15-custom.yaml`, sync-wave 4 → ürün bileşenleri kurulduktan **sonra**).
Kural `glue` uygulamasıyla aynıdır: Git'e ne koyarsanız kümede o vardır, elle yapılan
değişiklik geri alınır (`selfHeal`), Git'ten sildiğiniz kaynak kümeden de silinir
(`prune`).

Ürün kurulumunda `custom/kustomization.yaml` **bilerek boştur** (`resources: []`);
kaynaksız bir Application da `Synced Healthy` kalır. Bir dosya eklemek iki satırlık bir
iştir:

```yaml
resources:
- spark-gunluk-ozet-zamanli.yaml
```

**Müşteri yeni bir ArgoCD Application eklemez.** Uygulama listesi bootstrap'ın parçasıdır;
yeni bir Application istemek ürün tarafında bir yama gerektirir. İhtiyacınız olan her özel
nesne — `SparkApplication`, `ScheduledSparkApplication`, `CronJob`, `ConfigMap`,
`KafkaTopic` — var olan `custom` Application'ının altına girer.

Klasörün kendi kısa anlatımı ve hazır örnekleri:
[custom/README.md](../../custom/README.md).

| Dosya | Ne yapar |
|---|---|
| `custom/examples/ornek_rapor.py` | Silver `shop.orders` → durum bazında sipariş sayısı → `sandbox.ornek_rapor` |
| `custom/examples/spark-tek-seferlik.yaml` | `SparkApplication` — tek seferlik koşu (uygulandığı anda çalışır) |
| `custom/examples/spark-zamanli.yaml` | `ScheduledSparkApplication` — aynı iş, gecelik cron (örnekte `suspend: true`) |
| `custom/examples/dbt-cronjob.yaml` | Spark dışı örnek: dbt → Gold `CronJob`'ı |
| `custom/examples/kustomization.yaml` | yukarıdakilerden hangilerinin uygulanacağı + `.py` dosyasından ConfigMap üretimi |

**Tek dosyalık betikler için özel imaj gerekmez.** Python kodu bir imaja gömülmez:
`kustomization.yaml` içindeki `configMapGenerator` `.py` dosyasından bir ConfigMap üretir,
CR onu `/opt/job` altına bağlar ve `mainApplicationFile` o yolu gösterir. Çalışan imaj
ürünün resmi Spark imajıdır.

### 2.1 Sınır: ConfigMap 1 MiB'dir

ConfigMap'in **tamamı** (bütün anahtarların toplamı) 1 MiB'ı (1.048.576 bayt) aşamaz; bu
Kubernetes'in kendi sınırıdır ve aşıldığında nesne hiç yaratılmaz. Tek dosyalık bir PySpark
betiği bu sınırın çok altında kalır — bu bölümün örneği 1 KB'dir. Sınıra takılan
uygulamalar şunlardır: birden çok modüle bölünmüş paketler, depoya gömülmüş üçüncü parti
kütüphaneler, veri dosyası taşıyan işler.

Büyük uygulamalar için iki desteklenen yol vardır.

**(a) Kendi Spark imajınız.** Ürünün imajından türetip paketinizi içine kopyalar ve
kurumun iç registry'sine (`$INTERNAL_REGISTRY`) itersiniz; CR'da yalnız `image` satırı
değişir. Kapalı ağda zaten iç registry kullanıldığı için ek bir ağ izni gerekmez.

```dockerfile
FROM apache/spark:4.1.0-java21-python3
COPY benim_paketim/ /opt/job/benim_paketim/
```

CR'da: `image: image-registry.openshift-image-registry.svc:5000/lakehouse/benim-spark:1.0`
ve `mainApplicationFile: local:///opt/job/benim_paketim/ana.py`. Ürün yükseltmesinde
taban imajın sürümünü de yükseltmeniz gerekir (Adım 4.1'deki sürüm senkronu notu).

**(b) S3'ten `pyFiles`.** Paketi bir `.zip` olarak S3'e koyar ve CR'ın `deps` bloğunda
gösterirsiniz; Spark onu sürücü ve executor'ların `PYTHONPATH`'ine ekler. İmaj üretmeden
çok modüllü uygulama çalıştırmanın yoludur.

```yaml
spec:                            # zamanlı CR'da: spec.template altında
  mainApplicationFile: local:///opt/job/gunluk_ozet.py   # giriş noktası yine ConfigMap'ten
  deps:
    pyFiles:
    - s3a://kurum-veri/spark-paketleri/benim_paketim.zip
```

Alan adını kümenizde doğrulayabilirsiniz:

`[bastion]`

```bash
oc explain sparkapplication.spec.deps.pyFiles
```

**Beklenen çıktı** (kind kümesinde alınmış gerçek çıktı; zamanlı CR'da aynı alan
`scheduledsparkapplication.spec.template.deps.pyFiles` yolundadır):

```text
GROUP:      sparkoperator.k8s.io
KIND:       SparkApplication
VERSION:    v1beta2

FIELD: pyFiles <[]string>


DESCRIPTION:
    PyFiles is a list of Python files the Spark application depends on.
```

`s3a://` yolunu okuyabilmek için sürücü ve executor'ın S3 kimliğine ihtiyacı vardır: iş
zaten `vended-credentials` ile çalışıyorsa Polaris'in verdiği geçici anahtar **yalnız
katalog tabloları içindir**, paket dosyası için ayrıca `s3-creds` Secret'ındaki
`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` çiftini (Adım 4.1'de yorumlu duran satırlar)
açmanız gerekir.

**Ters giderse:** ConfigMap'i uygularken boyut hatası alıyorsanız Adım 10'daki ilgili
satıra bakın; `ModuleNotFoundError` alıyorsanız paket sürücüye ulaşmamıştır — (a) yolunda
`COPY` hedefini, (b) yolunda `pyFiles` girdisini ve S3 kimliğini denetleyin.

---

## 3. Betiği yazın

Kendi dosyanız `custom/examples/ornek_rapor.py` kopyasıyla başlar ve `custom/` klasörüne
`gunluk_ozet.py` adıyla konur. Betikte **yalnız veri mantığı** vardır: katalog, S3 ve
Iceberg ayarlarının hepsi CR'daki `sparkConf` bloğundan gelir.

**Tek istisna kimlik bilgisidir.** Polaris kimliği (kullanıcı ve parola çifti) YAML'a
girmez: `polaris-spark` Secret'ından `POLARIS_CREDENTIAL` ortam değişkenine, oradan da
betikte katalog ayarına verilir. Aşağıdaki dört satır **zorunludur**; yoksa katalog çağrısı
`401` ile döner.

```python
"""Günlük özet: Silver shop.orders -> gün bazında adet ve tutar -> sandbox.gunluk_ozet."""
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

CATALOG = "lakehouse"

builder = SparkSession.builder.appName("gunluk-ozet")
cred = os.environ.get("POLARIS_CREDENTIAL")          # Secret -> env; YAML'a girmez
if cred:
    builder = builder.config(f"spark.sql.catalog.{CATALOG}.credential", cred)
spark = builder.getOrCreate()

spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.sandbox")
ozet = (spark.table(f"{CATALOG}.shop.orders")
        .withColumn("gun", F.to_date("updated_at"))
        .groupBy("gun")
        .agg(F.count("*").alias("adet"), F.sum("amount").alias("tutar")))
# createOrReplace: iş yeniden koşarsa tablo baştan yazılır (idempotent)
ozet.writeTo(f"{CATALOG}.sandbox.gunluk_ozet").using("iceberg").createOrReplace()
print("GUNLUK_OZET_OK", ozet.count())
spark.stop()
```

Dört kural:

- **Okuma.** Ürünün tabloları `spark.table("lakehouse.shop.orders")` biçiminde okunur; SQL
  yazmayı tercih ederseniz `spark.sql("select … from lakehouse.shop.orders")` aynı işi
  yapar. `spark.sql.defaultCatalog` zaten `lakehouse`'tur, yani katalog adı yazılmadan da
  çalışır — ama açık yazmak okunurluğu artırır.
- **Yazma yeri.** Kendi çıktılarınızı `sandbox` ad alanında tutun. Betik ürünün `spark`
  kimliğiyle koştuğu için katalogda her yere yazabilir; ürünün Bronze ve Silver ad
  alanlarına elle yazmak `silver-merge` işini bozar.
- **Tekrar koşabilirlik.** `createOrReplace()` tabloyu baştan yazar, yani iş ikinci kez
  koşarsa sonuç değişmez. Artımlı yazma gerekiyorsa `MERGE INTO` kullanın (aşağıda).
- **Bitiş satırı.** Sonuna kendi işaret satırınızı basın (`GUNLUK_OZET_OK …`); koşu
  günlüğünde tek bir `grep` ile aranır ve izleme kolaylaşır.

**Artımlı yazma — `MERGE INTO`.** Hedef tabloyu baştan yazmak yerine yalnız değişenleri
işlemek istiyorsanız kalıp şudur (ürünün `silver-merge` işi de bu kalıbı kullanır):

```python
yeni.createOrReplaceTempView("yeni")
spark.sql(f"""
    MERGE INTO {CATALOG}.sandbox.gunluk_ozet t
    USING yeni s ON t.gun = s.gun
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")
```

`MERGE INTO` hedef tablonun **var olmasını** ister; ilk koşuda tablo yoksa önce
`createOrReplace()` ile yaratın (ya da `CREATE TABLE IF NOT EXISTS` yazın).

**Ters giderse:** `Table or view not found` → tablo adının başında katalog adı eksik ya da
şema adını yanlış yazmışsınızdır; kullanılabilir adları
`spark.sql("show namespaces in lakehouse").show()` ile listeleyin.

---

## 4. CR'ı yazın: tek seferlik mi, zamanlı mı

İki dosyadan biri kopyalanır. Seçim işin **tekrar edip etmediğine** göre yapılır:

| | `SparkApplication` (tek seferlik) | `ScheduledSparkApplication` (zamanlı) |
|---|---|---|
| Ne zaman koşar | uygulandığı **anda** | `schedule` cron'una göre (küme saat dilimi UTC) |
| GitOps'a uygun mu | **hayır** — elle `oc apply -f` | **evet** — commit + push |
| Kullanım | geri doldurma, tek seferlik dönüşüm, deneme | gecelik/saatlik rapor, düzenli dönüşüm |
| Örnek dosya | `custom/examples/spark-tek-seferlik.yaml` | `custom/examples/spark-zamanli.yaml` |

> **Tek seferlik iş GitOps'a konmaz.** `SparkApplication` uygulandığı anda koşar. ArgoCD
> altında tutulursa eşitleme anında başlar (veri hazır olmayabilir), `timeToLiveSeconds`
> dolup CR silindiğinde `selfHeal` onu yeniden yaratır ve iş **kendiliğinden tekrar
> koşar**. Üstelik başarısız bir koşu `custom` Application'ını `Degraded` gösterir. Bu
> yüzden depodaki `custom/examples/kustomization.yaml` dosyasında tek seferlik örnek
> **yorumludur**; tek seferlik işler elle uygulanır, tekrar edenler GitOps'tan geçer.

Kopyalanan dosyada **üç** şey değişir: CR'ın adı, çalıştırılacak `.py` dosyasının yolu ve
kodun geldiği ConfigMap'in adı.

> **İki dosyada bu üç alan aynı düzeyde durmaz.** Tek seferlik `SparkApplication`'da işin
> tanımı doğrudan `spec:` altındadır. Zamanlı `ScheduledSparkApplication`'da `spec:`
> **yalnız zamanlama alanlarını** taşır; işin tanımının tamamı (`mainApplicationFile`,
> `sparkConf`, `driver`, `executor`, `volumes`) bir düzey aşağıda, **`spec.template:`**
> altındadır. Alanı yanlış düzeye yazarsanız API sunucusu nesneyi reddeder.

**Tek seferlik** — `custom/examples/spark-tek-seferlik.yaml` kopyası, kendi deponuzda
`spark-gunluk-ozet-tek.yaml`:

```yaml
metadata:
  name: gunluk-ozet                             # sürücü pod'u gunluk-ozet-driver olur
spec:
  mainApplicationFile: local:///opt/job/gunluk_ozet.py
  volumes:
  - name: job
    configMap: {name: gunluk-ozet}              # configMapGenerator'ın ürettiği ad
```

**Zamanlı** — `custom/examples/spark-zamanli.yaml` kopyası, kendi deponuzda
`spark-gunluk-ozet-zamanli.yaml`. Aynı üç alan burada `template:` altındadır:

```yaml
metadata:
  name: gunluk-ozet-zamanli        # koşular bu adın sonuna zaman damgası eklenerek doğar
spec:
  schedule: "0 4 * * *"          # gecelik 04:00 UTC — bakım ve silver-merge koşularından sonra
  suspend: false                 # örnekte `true`; işi gerçekten zamanlamak için false yapın
  concurrencyPolicy: Forbid      # önceki koşu bitmeden yenisi başlamaz (uzun işlerde şart)
  successfulRunHistoryLimit: 3
  failedRunHistoryLimit: 3
  template:                      # BURADAN AŞAĞISI tek seferlik dosyanın `spec:` içeriğidir
    mainApplicationFile: local:///opt/job/gunluk_ozet.py
    volumes:
    - name: job
      configMap: {name: gunluk-ozet}
    # sparkConf, driver, executor ve geri kalan her şey de template'in altında kalır
```

**Ters giderse:** alanları zamanlı dosyada `template:` altına indirmeyi unutursanız API
sunucusu nesneyi geri çevirir — hata, hangi alanların yanlış düzeyde olduğunu adlarıyla
söyler (kind kümesinde `oc apply --dry-run=server` ile alınmış gerçek satır):

```text
Error from server (BadRequest): error when creating "yanlis-zamanli.yaml": ScheduledSparkApplication in version "v1beta2" cannot be handled as a ScheduledSparkApplication: strict decoding error: unknown field "spec.mainApplicationFile", unknown field "spec.volumes"
```

ConfigMap'i üreten satır `custom/kustomization.yaml` dosyasına eklenir; `.py` dosyası
kümeye buradan gider:

```yaml
configMapGenerator:
- {name: gunluk-ozet, files: [gunluk_ozet.py], options: {disableNameSuffixHash: true}}
```

`disableNameSuffixHash: true` **bilerek** vardır: CR ConfigMap'e sabit adla baktığı için
üretilen adın sonuna karma eklenmemelidir.

**Cron saatini ürünün işleriyle çakıştırmayın.** Ürünün gecelik bakım işleri ve
`silver-merge` koşuları aynı düğümlerde çalışır; kendi işinizi onların arasına değil,
sonrasına koyun (ürünün varsayılan koşu saatleri `glue/values.yaml` içinde
`spark.schedules` başlığındadır).

### 4.1 Dokunulmayan ve siteye özel satırlar

Örnek dosyalardaki `sparkConf`, `driver` ve `executor` blokları ürünün kendi
`ScheduledSparkApplication` nesnesinin `helm template` çıktısından **birebir** alınmıştır;
ürün işleriyle aynı Iceberg sürümü, aynı katalog ve aynı servis hesabı
(`spark-operator-spark`) kullanılır. Çoğu satır olduğu gibi kalır. Değiştirilmesi gereken
satırlar dosyalarda `# SİTE` ile işaretlidir ve **üretimde
`platform/values/site/glue.yaml` ile aynı** olmalıdır:

| CR'daki `sparkConf` anahtarı | Karşılığı |
|---|---|
| `spark.sql.catalog.lakehouse.uri` | `iceberg.catalogUri` (ürün varsayılanı `glue/values.yaml`) |
| `spark.sql.catalog.lakehouse.warehouse` | `iceberg.warehouse` |
| `spark.sql.catalog.lakehouse.s3.endpoint` | site `s3.endpoint` (`$S3_ENDPOINT`) |
| `spark.sql.catalog.lakehouse.client.region` | site `s3.region` (`$S3_REGION`) |
| `spark.sql.catalog.lakehouse.s3.path-style-access` | `s3.pathStyleAccess` (ürün varsayılanı `glue/values.yaml`; gerekirse site dosyasında ezilir) |
| `spark.sql.catalog.lakehouse.header.X-Iceberg-Access-Delegation` | site `s3.vendedCredentials`: açıksa `vended-credentials`, kapalıysa `none` |
| `image`, `sparkVersion`, `spark.jars.packages` içindeki Iceberg sürümü | ürün sürümleri: `glue/values.yaml` `spark.image`, `spark.version`, `versions.iceberg` |

**Sürüm senkronu elle yapılır.** Ürün yükseltmesinde Spark ya da Iceberg sürümü
değişirse `custom/` altındaki kendi CR'larınızdaki `image`, `sparkVersion` ve
`spark.jars.packages` satırlarını da güncellemeniz gerekir; ürün chart'ı sizin
dosyalarınıza dokunmaz.

**S3 kimliği iki yoldan gelir.** Ürün `vended-credentials` ile çalışır: Polaris her
tablo için geçici S3 anahtarı verir ve CR'da S3 anahtarı **bulunmaz**. Sitenizde STS yoksa
(`s3.vendedCredentials: false`) delegation başlığı `none` yapılır ve `driver` ile
`executor` bloklarındaki yorumlu `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` çifti
açılır; bu değerler de `s3-creds` Secret'ından **adıyla** okunur.

> **Sırlar Git'e girmez.** CR'larda Secret'lara yalnız adlarıyla referans verilir
> (`secretKeyRef: {name: polaris-spark, key: credential}`). Bu kural `custom/` için de
> geçerlidir ([90-referans/secret-listesi.md](../90-referans/secret-listesi.md)).

---

## 5. Uygulayın

### 5.1 Zamanlı iş — commit + push

Ortak döngü: [değişiklik nasıl uygulanır](degisiklik-nasil-uygulanir.md).

`[bastion]`

```bash
git add custom/
git commit -m "custom: gunluk-ozet zamanli spark isi"
git push origin main
```

ArgoCD `custom` uygulamasını en geç üç dakikada bir yoklar.

`[bastion]`

```bash
oc -n "$ARGOCD_NS" get application custom \
  -o jsonpath='{.status.sync.status} {.status.health.status}{"\n"}'
```

**Beklenen çıktı** (örnek — OpenShift'e özgü; eşitleme bittiğinde):

```text
Synced Healthy
```

**Ters giderse:** `Synced Degraded` → içerideki bir kaynak sağlıksızdır; hangisi olduğunu
`oc -n "$ARGOCD_NS" describe application custom | tail -30` söyler. `OutOfSync` beş
dakikadan uzun sürüyorsa push kurumun deposuna gitmemiştir.

### 5.2 Tek seferlik iş — elle uygulama

CR, Python kodunu ConfigMap'ten bağlar; **ConfigMap kümede önce olmalıdır**. Üretimde
`custom/` klasörü boş olduğundan ConfigMap'i de siz uygularsınız:

**Ad alanı dosyadan gelir.** Örnek CR'lar `metadata.namespace: lakehouse` taşır ve
`oc apply -f` komutundaki `-n` bayrağı bunu **ezmez**. `$LAKEHOUSE_NS` değeriniz `lakehouse`
değilse son satırı çalıştırmadan **önce** dosyadaki `namespace:` alanını kendi ad alanınıza
çevirin; bu yüzden aşağıdaki üçüncü komutta `-n` bilerek yoktur.

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" create configmap gunluk-ozet --from-file=custom/gunluk_ozet.py
oc -n "$LAKEHOUSE_NS" delete sparkapplication gunluk-ozet --ignore-not-found
oc apply -f custom/spark-gunluk-ozet-tek.yaml     # ad alanı dosyadaki namespace: alanından
```

**Beklenen çıktı** (kind kümesinde alınmış gerçek çıktı):

```text
configmap/gunluk-ozet created
sparkapplication.sparkoperator.k8s.io/gunluk-ozet created
```

Aradaki `delete` satırı **bilerek** vardır: aynı adla duran eski (özellikle `FAILED`) bir
`SparkApplication` yeniden koşmaz, önce silinmesi gerekir.

Depodaki hazır örneği olduğu gibi denemek isterseniz aynı yordamın kısa yolu
[90-referans/oc-hizli-basvuru.md](../90-referans/oc-hizli-basvuru.md) §12'dedir; oradaki
komutun iki bilinçli yan etkisi de aynı başlıkta yazılıdır.

**Ters giderse:** sürücü pod'unun olay listesinde ConfigMap bulunamadı hatası varsa ilk
satır atlanmıştır — ya da ConfigMap ile CR farklı ad alanlarına düşmüştür (yukarıdaki ad
alanı notu).

---

## 6. Koşuyu izleyin

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" get sparkapplication gunluk-ozet
```

Adı yazmadan `oc -n "$LAKEHOUSE_NS" get sparkapplication` komutu kümedeki bütün koşuları
listeler.

**Beklenen çıktı** (kind kümesinde alınmış gerçek çıktı; koşu bittikten sonra. İş
sürerken `STATUS` sütunu `RUNNING` olur ve `FINISH` sütunu boş görünür):

```text
NAME          SUSPEND   STATUS      ATTEMPTS   START                  FINISH                 AGE
gunluk-ozet             COMPLETED   1          2026-09-24T10:26:26Z   2026-09-24T10:41:22Z   21m
```

`STATUS` sütunundaki değerler ve anlamları:

| Durum | Anlamı | Ne yapılır |
|---|---|---|
| `SUBMITTED` | operatör `spark-submit`'i çalıştırdı, sürücü pod'u henüz başlamadı | bekleyin |
| `RUNNING` | sürücü çalışıyor | bekleyin; günlüğe bakın |
| `COMPLETED` | iş başarıyla bitti | sonucu doğrulayın (Adım 8) |
| `FAILED` | sürücü sıfırdan farklı bir kodla çıktı | sürücü günlüğü (Adım 7) + Adım 10 tablosu |
| `SUBMISSION_FAILED` | `spark-submit` hiç başlayamadı (CR geçersiz, imaj yok, kota) | `oc describe sparkapplication` |
| `INVALIDATING`, `PENDING_RERUN` | zamanlı işin bir sonraki koşusu hazırlanıyor | bekleyin |

Zamanlı işin koşuları ayrı `SparkApplication` nesneleri olarak görünür; adları zamanlı işin
adıyla başlar ve sonlarında bir zaman damgası taşır. Zamanlı işin kendisi ayrı listelenir:

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" get scheduledsparkapplication
```

**Beklenen çıktı** (kind kümesinde alınmış gerçek çıktı; `ornek-rapor-zamanli` depodaki
örnektir ve `suspend: true` ile gelir, diğerleri ürünün işleridir — geliştirme kümesinde
hepsi askıdadır, üretimde `SUSPEND` sütunu `false` olur):

```text
NAME                      SCHEDULE     TIMEZONE   SUSPEND   LAST RUN   LAST RUN NAME   AGE
maint-compact             30 2 * * *              true                                 5d14h
maint-expire-orphan-ttl   45 2 * * *              true                                 5d14h
maint-position-deletes    15 2 * * *              true                                 5d14h
mongo-bronze              5 3 * * *               true                                 5d14h
ornek-rapor-zamanli       0 4 * * *               true                                 4d19h
silver-merge              0 3 * * *               true                                 5d14h
```

**`timeToLiveSeconds: 86400` unutulmaz.** Örneklerde CR (ve sürücü pod'u) koşu bittikten
24 saat sonra silinir; günlüğü o süre içinde almanız gerekir. Daha uzun saklamak
istiyorsanız değeri büyütün ya da günlüğü kurumun günlük toplayıcısına yönlendirin.

**Ters giderse:** liste boşsa CR uygulanmamıştır. `oc -n "$LAKEHOUSE_NS" get pods`
çıktısında sürücü pod'u `Pending` kalıyorsa düğümde CPU ya da bellek yoktur (Adım 9).

---

## 7. Sürücü günlüğünü okuyun

Sürücü pod'unun adı CR adının sonuna `-driver` eklenerek oluşur.

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" logs gunluk-ozet-driver --tail=2000 | grep -E "GUNLUK_OZET_OK|Exception"
```

**Beklenen çıktı** (kind kümesinde alınmış gerçek satır; sayı, yazılan satır adedidir):

```text
GUNLUK_OZET_OK 1
```

Koşu sürerken günlüğü canlı izlemek için `--tail=20 -f` kullanılır. İlk koşuda günlüğün
başında Iceberg paketlerinin Maven'den indiği satırlar görünür; bu normaldir ve ikinci
koşuda önbellekten gelir.

**Ters giderse:** sürücü pod'u bulunamıyorsa `timeToLiveSeconds` süresi dolmuş ve pod
silinmiştir; koşuyu tekrarlayın. Günlükte `Exception` satırı varsa Adım 10 tablosuna bakın.

---

## 8. Sonucu Trino'da doğrulayın

Yazdığınız tablo, ürünün tabloları gibi Trino, Superset, JupyterHub ve Zeppelin'den
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

**Beklenen çıktı** (değerler kind provasından gerçektir — sorgu kümedeki Trino'ya
koşturuldu ve tek satır döndü, geliştirme kümesindeki üç siparişin hepsi aynı güne
düşmüştü; blok, Python istemcisinin bu satırı döndürme biçimidir):

```text
[[datetime.date(2026, 9, 18), 3, Decimal('14.50')]]
```

**Ters giderse:** tablo bulunamadı hatası → iş `COMPLETED` olmamış ya da tabloyu başka bir
ad alanına yazmıştır. `Access Denied` → kullanıcınız `sandbox` şemasına yazma yetkisi olan
gruba (analist) üye değildir.

---

## 9. Kaynak ayarı

Örnekler geliştirme kümesinin dar düğümlerine göre ayarlanmıştır. Üretimde işin gerçek
ihtiyacına göre büyütülür.

| Alan | Anlamı | Seçim ölçütü |
|---|---|---|
| `driver.cores` | Spark'ın sürücüye ayırdığı **çekirdek sayısı** (tam sayı) | sonucu sürücüde toplamayan işlerde 1 yeter |
| `driver.coreRequest` | Kubernetes **CPU isteği** (kesirli olabilir) | dar düğümde `500m`; boş bırakılırsa `cores` kadar istenir |
| `driver.memory` | sürücü JVM yığını | sonucu sürücüye toplamıyorsanız 1–2 GB |
| `executor.instances` | paralel executor sayısı | tablo boyutuyla artar; `bucket_count` değerinden fazlası boşa gider |
| `executor.memory` | executor JVM yığını | bellek hatası görülene kadar büyütülmez; önce `spark.sql.shuffle.partitions` artırılır |
| `spark.sql.shuffle.partitions` | karıştırma parça sayısı | küçük kümede 8; her parça 100–200 MB olacak şekilde |

`cores` ile `coreRequest` **ayrı** şeylerdir: ilki Spark'ın iç paralelliği, ikincisi
zamanlayıcının düğümde aradığı yerdir. Dar bir düğümde `coreRequest` yüksek kalırsa pod
`Pending` takılır.

---

## 10. İzleme ve hata tablosu

**Ürünün alarmları sizin işlerinizi kapsamaz.** `LakehouseSparkScheduledRunFailed` alarmı
yalnız ürünün zamanlı işlerini (`silver-merge`, `maint-` ile başlayanlar ve `mongo-bronze`)
hedefler; `custom/` altındaki işleriniz bu alarmı **tetiklemez**. Küme genelinde çalışan
tek alarm `LakehouseSparkRunTooLong`'dur (başarılı koşuların son altı saatlik ortalaması
eşiği aşarsa) ve o da iş bazında ayrım yapmaz. Kendi işlerinizin durumu
`oc -n "$LAKEHOUSE_NS" get sparkapplication` çıktısından, zamanlı işlerde ayrıca `custom`
Application'ının sağlığından izlenir. İşinize özel alarm istiyorsanız kurumun kendi izleme
kuralı yazılır; alarmların tam listesi ve hedefleri işletme bölümündeki izleme sayfasında
anlatılır.

| Belirti | Neden | Çözüm |
|---|---|---|
| `ClassNotFoundException: org.apache.iceberg.spark.SparkCatalog` ya da paket indirme hatası | sürücü Maven Central'a çıkamıyor | kümeden Maven Central erişimini açın ya da iç Maven aynasını `spark.ivySettingsXml` ile tanıtın (ürün işleri de aynı yolu kullanır) |
| İlk koşu dakikalarca `RUNNING`, günlükte Maven'den indirme satırları | Iceberg paketleri ilk kez iniyor | bekleyin; ikinci koşu önbellekten gelir |
| `401 Unauthorized` (Polaris) | betikte `POLARIS_CREDENTIAL` → katalog `credential` ayarı satırı yok ya da CR'da env tanımlı değil | Adım 3'teki dört satır ve CR'daki `secretKeyRef` |
| `403 Forbidden` (Polaris) | kimlik doğru ama o ad alanına yazma yetkisi yok | `sandbox` ad alanına yazın; başka bir ad alanı gerekiyorsa yetki sayfasındaki rol tablosuna bakın |
| `NoSuchNamespaceException` | hedef ad alanı katalogda yok | betikte `CREATE NAMESPACE IF NOT EXISTS` satırı |
| `OutOfMemoryError` ya da executor'ın bellek aşımıyla öldürülmesi | veri executor belleğine sığmıyor | önce `spark.sql.shuffle.partitions`, sonra `executor.memory` ve `memoryOverhead` |
| `ImagePullBackOff` | küme imaj registry'sine çıkamıyor | imajı iç registry'ye kopyalayın ve `image` satırını değiştirin |
| Sürücü ya da executor pod'u `Pending` | düğümde istenen CPU/bellek yok | `coreRequest` ve `memory` değerlerini düşürün (Adım 9) |
| `SUBMISSION_FAILED` | CR şemaya uymuyor ya da servis hesabı yok | `oc -n "$LAKEHOUSE_NS" describe sparkapplication gunluk-ozet` |
| Pod başlamıyor, olay listesinde ConfigMap bulunamadı hatası | ConfigMap uygulanmadı | Adım 5.2'nin ilk satırı |
| ConfigMap uygulanmıyor: `Too long: may not be more than 1048576 bytes` | betik(ler) 1 MiB ConfigMap sınırını aşıyor | Adım 2.1: kendi imajınız ya da `deps.pyFiles` |
| `ModuleNotFoundError` (kendi modülünüz) | çok modüllü uygulama ConfigMap'e sığmıyor ya da `pyFiles` eksik | Adım 2.1 |
| `custom` Application `Degraded` | içeride `FAILED` bir `SparkApplication` duruyor | tek seferlik işi GitOps'tan çıkarın (Adım 4 kutusu); başarısız CR'ı silin |

Uygulamadan önce manifest'i kümeye yazmadan denemek için `--dry-run=server` kullanılır;
CRD şeması ve admission webhook'u gerçekten devreye girer, nesne yaratılmaz:

`[bastion]`

```bash
oc apply -f custom/spark-gunluk-ozet-tek.yaml --dry-run=server
```

**Beklenen çıktı** (örnek):

```text
sparkapplication.sparkoperator.k8s.io/gunluk-ozet configured (server dry run)
```

---

## 11. Spark dışı kaynaklar

Aynı klasör ve aynı döngü, Spark'la ilgisi olmayan nesneler için de geçerlidir: bir
`CronJob`, ek bir `ConfigMap`, kendi `KafkaTopic`'iniz. Dosya `custom/` klasörüne konur,
`custom/kustomization.yaml` içindeki listeye yazılır, commit edilir.

Depodaki `custom/examples/dbt-cronjob.yaml` bunun çalışan örneğidir: Trino üzerinden Gold
katmanı üreten bir dbt koşusunu gecelik `CronJob` olarak kurar. Örnek kustomization'da
**yorumludur**, çünkü ayrıca bir `dbt-project` ConfigMap'i ve `dbt-trino` Secret'ı ister
([examples/dbt/README.md](../../examples/dbt/README.md)).

Tek sınır Adım 2'deki kuraldır: yeni bir ArgoCD **Application** eklenmez; her şey var olan
`custom` Application'ının altına girer.

---

## Ek A. S3'te duran dosyaları tek seferde kataloğa kaydetme

Kaynak sistemi olmayan, S3'te hazır duran düz CSV ya da Parquet dosyalarını doğrudan bir
Iceberg tablosuna yazmak için ürünle birlikte gelen bir betik vardır:
`glue/jobs/s3_register_example.py`. Debezium/Kafka yolundan geçmez, kabul testinin parçası
değildir ve ürünün iş ConfigMap'i (`lakehouse-jobs`, `glue/jobs/` altındaki bütün `.py`
dosyalarını taşır) ile zaten kümededir — yani ConfigMap üretmenize gerek yoktur, yalnız
tek seferlik bir CR gerekir.

En kolay yol, var olan bir zamanlı işin şablonundan tek seferlik CR türetmektir; böylece
`sparkConf`, kimlik ve S3 ayarları ürünün işiyle birebir aynı olur:

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

`--source` S3 yolu, `--table` hedef tablonun ad alanı ve tablo adından oluşan tam adıdır.
Uygulamadan önce aynı komutu `| oc apply --dry-run=server -f -` ile koşturarak manifest'i
doğrulayabilirsiniz; CRD şeması ve admission webhook'u gerçekten devreye girer.

İzleme ve sonuç:

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" get sparkapplication s3-register-once
oc -n "$LAKEHOUSE_NS" logs s3-register-once-driver --tail=2000 | grep S3_REGISTER_OK
```

**Beklenen çıktı** (örnek — bitiş satırının biçimi `glue/jobs/s3_register_example.py`
dosyasındaki `print` çağrısından; satır sayısı sizin dosyanıza göre değişir):

```text
S3_REGISTER_OK s3://kurum-veri/musteri-dosyalari/ -> lakehouse.sandbox.musteri_dosyalari (10432 satır)
```

İş bitince CR elle silinir:

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" delete sparkapplication s3-register-once
```

Bilinmesi gereken üç şey:

- **Hedef tablo baştan yazılır.** Betik `createOrReplace()` kullanır: tablo varsa
  **tamamen değiştirilir**. Tekrar koşum idempotenttir ama ekleme (`INSERT`) değildir.
  Artımlı yükleme gerekiyorsa bu betik değil, Adım 3'teki `MERGE INTO` kalıbı kullanılır.
- **CSV başlığı.** `--format csv` için `--header true|false` başlık satırını denetler
  (varsayılan `true`). `parquet` için `--header` yoksayılır.
- **Ad alanı.** Hedef ad alanı yoksa yaratılır (`CREATE NAMESPACE IF NOT EXISTS`); kendi
  yüklemelerinizi `sandbox` altında tutun.

---

## Kontrol listesi

- [ ] Betik `custom/` klasöründe ve yalnız veri mantığı içeriyor; `POLARIS_CREDENTIAL`
      satırları yerinde.
- [ ] CR kopyalandı; ad, `mainApplicationFile` ve `volumes` altındaki ConfigMap adı
      değiştirildi — **zamanlı dosyada bu üç alan `spec.template:` altındadır**, tek
      seferlik dosyada doğrudan `spec:` altında.
- [ ] `configMapGenerator` girdisi `custom/kustomization.yaml` dosyasına eklendi.
- [ ] `# SİTE` işaretli satırlar `platform/values/site/glue.yaml` ile aynı.
- [ ] Tekrar eden iş `ScheduledSparkApplication`; tek seferlik iş GitOps'a **konmadı**.
- [ ] Cron saati ürünün bakım ve `silver-merge` koşularıyla çakışmıyor.
- [ ] Koşu `COMPLETED`; sürücü günlüğünde kendi bitiş satırınız var.
- [ ] Sonuç tablosu Trino'dan okunabiliyor ve `sandbox` ad alanında.
- [ ] Ürün yükseltmesinden sonra `image`, `sparkVersion` ve Iceberg sürümü elle
      güncellendi.

## Sonraki bölüm

Bir kaynağı ya da tabloyu — kendi yazdığınız tablolar dâhil — verisiyle birlikte kaldırmak:
[kaynak-veya-tablo-silme.md](kaynak-veya-tablo-silme.md).
[kullanici-ve-yetki.md](kullanici-ve-yetki.md) — yetki yönetimi;
[izleme-ve-alarmlar.md](izleme-ve-alarmlar.md) — alarmlar ve eşikler;
[sorun-giderme.md](sorun-giderme.md) — belirti tablosu. Ortak GitOps döngüsü her zaman
aynıdır:
[değişiklik nasıl uygulanır](degisiklik-nasil-uygulanir.md).
