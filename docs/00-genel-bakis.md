# 00 — Genel bakış

**Bu bölümde:** ürünün ne yaptığı, hangi bileşenlerden oluştuğu, verinin kaynaktan rapora kadar izlediği yedi akış ve baştan sona kullanılan kavramların sözlüğü.
**Süre:** 30–40 dakika (okuma).
**Gereken yetki:** yok — bu bölümde komut çalıştırılmaz.
**Nerede çalıştırılır:** komut yok; okuma bölümüdür. İlk komut kurulum öncesi denetimlerle birlikte 20-on-kosullar.md bölümünde gelir.

---

## 1. Tek cümlede

Bu ürün, kurumun işletim veritabanlarındaki (PostgreSQL, SQL Server, MongoDB) ve web sunucularındaki değişiklikleri **sürekli** olarak açık formatlı bir veri gölüne (Apache Iceberg tabloları) taşır ve bu veriyi SQL, pano ve not defteri üzerinden, kurumun Active Directory kimlikleriyle sorgulanabilir kılar.

Üç özellik ürünün tamamını belirler:

1. **Kod yazılmaz, değer yazılır.** Yeni bir kaynak veya tablo eklemek, bir YAML dosyasına birkaç satır eklemektir; bağlayıcıları, Kafka konularını, tabloları ve zamanlanmış işleri Helm şablonları üretir.
2. **Her şey Git'ten gelir.** Kümede elle değişiklik yapılmaz; Git'e yazılır, ArgoCD kümeyi Git'e eşitler. Böylece "kümede ne var" sorusunun cevabı her zaman depodadır.
3. **Özel imaj yoktur.** Bütün konteyner imajları üreticinin resmi imajlarıdır; Kafka Connect imajı bile kümede, Strimzi'nin kendi yapı mekanizmasıyla üretilir. Dockerfile bakımı diye bir iş yoktur.

---

## 2. Bileşenler

Aşağıdaki tablo kurulumdan sonra kümede duran her şeyi listeler. "Yönetim" sütunu o bileşenin **hangi dosyadan** yönetildiğini söyler: yükseltme, boyut değişikliği veya ayar değişikliği o dosyada yapılır.

| Bileşen | Rol | Nerede çalışır | Yönetim (Application / chart / değer dosyası) |
|---|---|---|---|
| **ArgoCD** (OpenShift GitOps) | Git'teki istenen durumu kümeye uygular, sapmayı gösterir | `openshift-gitops` ad alanı | `bootstrap/bootstrap.sh` + `platform/root-app.yaml` |
| **cert-manager** | Küme içi sertifikalar (Trino TLS, iç kök CA) | `cert-manager` ad alanı | `platform/apps/00-cert-manager.yaml` |
| **Strimzi Kafka Operator** | Kafka ve Kafka Connect kümelerini CR'lardan kurar | `lakehouse` | `platform/apps/00-strimzi.yaml` |
| **Apache Kafka** (KRaft) | Değişiklik olaylarının dayanıklı tamponu | `lakehouse` | `glue/values.yaml` → `kafka` |
| **Kafka Connect** (Debezium + Iceberg sink) | Kaynak DB'den okur, Iceberg tablosuna yazar | `lakehouse` | `glue/templates/kafka-connect.yaml`, `glue/templates/connectors.yaml` |
| **CloudNativePG (CNPG)** | Polaris, Keycloak ve Superset'in PostgreSQL veritabanları | `lakehouse` (operatör `cnpg-system`) | `platform/apps/00-cnpg.yaml`, `glue/values.yaml` → `cnpg` |
| **Barman Cloud eklentisi** | CNPG veritabanlarının S3'e sürekli yedeği (PITR) | `cnpg-system` | `platform/apps/00-cnpg-barman.yaml`, `glue/values.yaml` → `backup` |
| **Apache Polaris** | Iceberg REST kataloğu: tablo listesi, yetki, S3 kimlik dağıtımı | `lakehouse` | `platform/apps/20-polaris.yaml`, `platform/values/polaris.yaml`, `platform/polaris/setup.yaml` |
| **Spark Operator** | `SparkApplication` CR'larını gerçek Spark koşularına çevirir | `lakehouse` | `platform/apps/00-spark-operator.yaml` |
| **Spark işleri** | Silver birleştirme, Iceberg bakımı, MongoDB Bronze yükleme | `lakehouse` (geçici pod'lar) | `glue/templates/spark-jobs.yaml`, `glue/jobs/` |
| **Trino** | SQL sorgu motoru (tek giriş kapısı); satır/kolon yetkilendirmesi | `lakehouse` | `platform/apps/30-trino.yaml`, `platform/values/trino.yaml`, `platform/values/site/trino.yaml` |
| **Apache Superset** | Pano ve grafik arayüzü | `lakehouse` | `platform/apps/00-superset-operator.yaml`, `glue/templates/superset.yaml` |
| **JupyterHub** | Kişisel Python not defteri ortamı (PySpark, PyIceberg) | `lakehouse` | `platform/apps/30-jupyterhub.yaml`, `platform/values/jupyterhub.yaml`, `platform/values/site/jupyterhub.yaml` |
| **Apache Zeppelin** | Paylaşımlı SQL not defteri (Trino JDBC) | `lakehouse` | `glue/templates/zeppelin.yaml`, `glue/values.yaml` → `zeppelin` |
| **Keycloak** | Kimlik sağlayıcı: AD'yi okur, tüm arayüzlere tek oturum açma verir | `lakehouse` (operatör `lakehouse`) | `platform/keycloak-operator/kustomization.yaml`, `glue/templates/keycloak-realm.yaml` |
| **Fluent Bit** | nginx erişim günlüğünü Kafka'ya gönderen ajan | müşterinin web sunucusu (küme dışı) | `agents/fluent-bit/` |
| **OADP / Velero** | Ad alanı ve kalıcı disk yedeği | `openshift-adp` | `glue/values.yaml` → `velero` |
| **Kullanıcı iş yükü izleme (UWM)** | Metrik toplama ve alarm | `openshift-user-workload-monitoring` | `glue/templates/monitoring.yaml` |
| **Müşteri kaynakları** | Kurumun kendi Spark uygulamaları, CronJob'ları | `lakehouse` | `custom/` klasörü, `custom/README.md` |

Sözlükteki karşılıkları: [Route](#sozluk), [ArgoCD Application](#sozluk), [Helm values](#sozluk), [Secret](#sozluk).

---

## 3. Veri akışları

Yedi akış vardır. Her diyagramın altındaki paragraf, diyagramda görünmeyen ama işletme sırasında bilmeniz gereken şeyi anlatır.

### 3.1 CDC akışı — PostgreSQL ve SQL Server

```
 [kaynak DB]            [lakehouse ad alanı]                                [S3]
 PostgreSQL  --WAL-->  Debezium ------> Kafka konusu ------> Iceberg sink ------> Bronze tablosu
 SQL Server  --CT -->  (Kafka Connect)   shop.public.orders   (Kafka Connect)      shop_raw.orders
                             |                                                        |
                             +-- ilk yükleme: snapshot ---------------------------->--+
```

PostgreSQL'de [Debezium](#sozluk) veritabanının yazma-öncesi günlüğünü (WAL) mantıksal çoğaltma yuvasından okur; SQL Server'da Change Tracking/CDC tablolarını okur. Bağlayıcı ilk kez açıldığında tablonun tamamını okuyup Kafka'ya basar (bu ilk tam okumaya [snapshot](#sozluk) denir), sonra yalnız değişiklikleri akıtır. Iceberg [sink](#sozluk)'i aynı Kafka Connect kümesinde çalışır ve olayları toplu hâlde [Bronze](#sozluk) tablosuna yazar; Bronze tablosu kaynağın **tarihçesidir**, güncel hâli değildir (her `INSERT`/`UPDATE`/`DELETE` ayrı satırdır). Sink varsayılan olarak 300 saniyede bir yazar, yani yeni bir kaydın Bronze'da görünmesi en fazla ~5 dakika sürer. Mevcut bir kaynağa sonradan tablo eklendiğinde bütün bağlayıcıyı durdurmaya gerek yoktur: sinyal tablosuna bir satır yazılır ve [artımlı snapshot](#sozluk) yalnız o tabloyu geriye dönük doldurur.

### 3.2 MongoDB akışı

```
 [kaynak DB]          [lakehouse ad alanı]                                    [S3]
 MongoDB  --oplog-->  Debezium ------> Kafka konusu ------> Spark mongo-bronze ----> Bronze tablosu
 (replicaSet)        (Kafka Connect)   crm.crm.customers   (5 dakikada bir)          crm_raw.customers
```

MongoDB'de belgeler şemasızdır, bu yüzden Iceberg sink'i doğrudan kullanılamaz: arada `glue/jobs/mongo_bronze.py` adlı bir [Spark](#sozluk) işi vardır. Bu iş beş dakikada bir Kafka'dan okur, belgeleri düzleştirip Bronze tablosuna yazar ve nerede kaldığını Kafka tüketici grubunda saklar. Diğer iki fark: MongoDB kaynağı `replicaSet` modunda çalışmalıdır (tek düğümlü bir `replicaSet` yeterlidir) ve koleksiyon başına bir Bronze tablosu oluşur. Gecikme bu yüzden CDC akışından biraz yüksektir (en fazla ~5 dakika + Spark koşu süresi).

### 3.3 nginx erişim günlüğü akışı

```
 [müşteri web sunucusu]           [lakehouse ad alanı]                        [S3]
 nginx access.log --> Fluent Bit --TLS/Route--> Kafka (dış dinleyici) --> Iceberg sink --> nginx_raw.access_log
```

Bu akış küme dışından başlar: web sunucusuna kurulan Fluent Bit ajanı erişim günlüğünü satır satır okuyup Kafka'nın **dış dinleyicisine** (OpenShift Route üzerinden, TLS ve SCRAM kimlik doğrulaması ile) gönderir. Kümede geri kalan yol CDC akışıyla aynıdır: Iceberg sink'i olayları `nginx_raw.access_log` tablosuna yazar. Akış varsayılan olarak **kapalıdır**; açmak için `glue/values.yaml` içindeki `nginx.enabled` ve `kafka.externalListener` anahtarlarının **ikisi birden** açılmalıdır (biri açık, öteki kapalı olursa ajan bağlanamaz). Bu tablo IP adresi içerdiği için Trino yetki kurallarında `remote` kolonu `lakehouse-users` grubuna maskelenmiş gösterilir.

### 3.4 Silver birleştirme ve bakım işleri

```
                          Bronze (tarihçe)                Silver (güncel hâl)          Gold (iş kuralı)
 shop_raw.orders  --> silver-merge (15 dk) -->  shop.orders  --> dbt / Spark --> gold.orders_daily
                              |
   bakım: maint-position-deletes (saatlik) · maint-compact (6 saat) · maint-expire-orphan-ttl (günlük 03:30)
          mongo-bronze (5 dk, yalnız MongoDB kaynağı varsa)
```

`silver-merge` işi on beş dakikada bir Bronze'daki yeni olayları okur ve birincil anahtara göre [Silver](#sozluk) tablosuna `MERGE` eder; sonuç, kaynaktaki tablonun **güncel** kopyasıdır ve son kullanıcıların sorguladığı tablo budur. Bu birleştirme varsayılan olarak [merge-on-read](#sozluk) yazar: yazma hızlıdır ama okuma zamanla yavaşlar, çünkü silme dosyaları birikir. Üç bakım işi bu bedeli geri alır — `maint-position-deletes` silme dosyalarını katlar, `maint-compact` küçük dosyaları birleştirir ([compaction](#sozluk)), `maint-expire-orphan-ttl` eski [Iceberg snapshot](#sozluk)'larını (7 günden eski), sahipsiz dosyaları (3 günden eski) ve Bronze'daki 30 günden eski tarihçeyi siler. [Gold](#sozluk) katmanı ürünün parçası değildir: kurumun kendi iş kuralları `custom/` klasöründeki `dbt` CronJob'ı ya da Spark uygulamasıyla yazılır (`examples/dbt/`, `custom/examples/dbt-cronjob.yaml`).

### 3.5 Kimlik akışı

```
 Active Directory --LDAPS--> Keycloak (realm: lakehouse) --OIDC--> Trino / Superset / JupyterHub
        |                          |                                       |
        +--- gruplar --------------+---- lakehouse-admins ---------------->+ tam yetki
                                   +---- lakehouse-analysts -------------->+ okuma + sandbox yazma
                                   +---- lakehouse-users ----------------->+ okuma (filtre/maske ile)
 Active Directory --LDAPS--> Trino group provider (gruplar ikinci kez, doğrudan AD'den)
```

Kullanıcı hesabı ve parolası yalnız Active Directory'de tutulur; ürün parola saklamaz. [Keycloak](#sozluk), AD'yi LDAPS üzerinden okur ve `lakehouse` adlı [realm](#sozluk)'inde üç arayüze ([OIDC](#sozluk) ile) tek oturum açma sağlar. Yetkilendirme grup adına bakar: `lakehouse-admins` (tam yetki), `lakehouse-analysts` (her şeyi okur, `sandbox` şemasına yazar), `lakehouse-users` (okur; bazı kolonlar maskeli, bazı satırlar filtrelidir). Trino grupları Keycloak jetonundan **değil**, doğrudan AD'den kendi grup sağlayıcısıyla okur — bu yüzden AD bağlama bilgisi iki yerde yapılandırılır ve `scripts/check-site.sh` ikisinin aynı kaldığını denetler. Superset ve Zeppelin'in Trino'ya bağlanmakta kullandığı servis hesapları ise AD'de değil, Trino'nun kendi parola dosyasındadır.

### 3.6 Yedek akışı

```
 CNPG (polaris-db, keycloak-db, superset-db) --Barman Cloud--> S3 $S3_BUCKET_BACKUP  (sürekli WAL + gece 02:00, 30 gün)
 lakehouse ad alanı (CR'lar + kalıcı diskler) --OADP/Velero--> yedek deposu          (gece 03:00, 30 gün)
 Iceberg verisi (S3 $S3_BUCKET_DATA) --> yedeklenmez: S3 tarafında sürüm/çoğaltma ile korunur
```

Üç ayrı koruma vardır ve üçü farklı şeyi kurtarır. [CNPG](#sozluk) veritabanları [Barman](#sozluk) Cloud eklentisiyle sürekli olarak yedek bucket'ına yazar; bu, zaman noktasına geri dönüşü (PITR) mümkün kılar — katalog ya da kimlik veritabanı bozulursa buradan dönülür. [Velero/OADP](#sozluk) ad alanının tamamının (CR'lar ve kalıcı diskler) günlük anlık görüntüsünü alır; yanlışlıkla silinen bir nesne buradan gelir. Iceberg verisinin kendisi **kopyalanmaz**: terabaytlarca veriyi ikinci kez yazmak yerine S3 tarafının kendi koruması (sürümleme, çoğaltma) kullanılır, buna karşılık Iceberg'in kendi [Iceberg snapshot](#sozluk)'ları son yedi günlük yanlışlıkla silmeyi tablo düzeyinde geri alabilir. Yedek bucket'ı veri bucket'ından ayrı olmalıdır; `scripts/check-site.sh` aynı olmadıklarını denetler.

### 3.7 İzleme akışı

```
 Kafka Connect / Kafka / Spark / Polaris --metrik--> UWM Prometheus --> PrometheusRule (5 alarm) --> Alertmanager
                                                                   \
                                                                    +--> OpenShift konsolu (Gözlem > Metrikler)
```

İzleme kapsamı bilerek dardır: soru "boru hattı sağlam mı", "Trino ne kadar hızlı" değil. OpenShift'in kullanıcı iş yükü izleme yığını (UWM) `lakehouse` ad alanındaki metrik tanımlarını kendiliğinden toplar; ayrı bir Prometheus kurulmaz. Beş alarm vardır: bağlayıcı görevi düştü (`LakehouseConnectTaskFailed`), sink geride kaldı (`LakehouseSinkStalled`), Silver birleştirmesi bayatladı (`LakehouseSilverMergeStale`), zamanlanmış Spark koşusu başarısız (`LakehouseSparkScheduledRunFailed`), Spark koşusu çok uzun sürüyor (`LakehouseSparkRunTooLong`). Eşikler `glue/values.yaml` içindeki `monitoring` bloğundadır ve kurumun veri hacmine göre kurulumdan sonra ayarlanır.

---

## 4. Kavramlar sözlüğü {#sozluk}

Belgelerin tamamında bu anlamlarla kullanılır. Ürün adları özgün hâlleriyle yazılır.

| Terim | Anlamı |
|---|---|
| **CDC** (Change Data Capture) | Kaynak veritabanındaki her ekleme/güncelleme/silmeyi, veritabanını sorgulamadan, günlüğünden okuyarak yakalama tekniği. Kaynağa neredeyse yük bindirmez. |
| **Debezium** | CDC'yi yapan açık kaynak bağlayıcı ailesi (PostgreSQL, SQL Server, MongoDB). Kafka Connect içinde bir eklenti olarak çalışır. |
| **snapshot** (ilk tam okuma) | Bağlayıcı ilk açıldığında tablonun o anki tamamını okuyup Kafka'ya basması. Büyük tablolarda saatler sürebilir; bu sırada değişiklikler de yakalanır. |
| **artımlı snapshot** | Çalışan bir bağlayıcıyı durdurmadan, yalnız belirli bir tabloyu parça parça geri doldurma. Kaynaktaki sinyal tablosuna bir satır yazılarak tetiklenir. |
| **Kafka topic (konu)** | Kafka'daki adlandırılmış, sıralı olay akışı. Her kaynak tablosu için bir konu oluşur. |
| **Kafka Connect** | Kafka'ya veri taşıyan/çıkaran bağlayıcıları barındıran çalışma zamanı. Bu üründe hem Debezium hem Iceberg sink'i burada koşar. |
| **sink** | Kafka'dan **okuyup** dışarı yazan bağlayıcı. Buradaki sink, olayları Iceberg tablolarına yazar. |
| **Bronze** | Kaynaktan geldiği gibi, hiç yorumlanmamış ham tarihçe katmanı. Her olay ayrı satırdır; ad kuralı: kaynak adı + `_raw`. |
| **Silver** | Bronze'un birincil anahtara göre birleştirilmiş, kaynağın güncel hâline eşit katman. Kullanıcıların sorguladığı katman budur. |
| **Gold** | İş kuralı uygulanmış, raporlamaya hazır türetilmiş tablolar. Ürünle gelmez; kurum kendi dbt/Spark işleriyle yazar. |
| **Apache Iceberg** | Tabloların S3 üzerinde dosya olarak durduğu açık tablo formatı. Şema değişikliği, zaman yolculuğu ve `MERGE` gibi işlemleri dosya düzeyinde yönetir. |
| **Iceberg snapshot** | Tablonun belirli bir andaki tam hâlinin kaydı. Eski snapshot'lar korunduğu sürece o ana geri dönülebilir; `maint-expire-orphan-ttl` 7 günden eskileri siler. |
| **MoR** (merge-on-read) | Silmeleri/güncellemeleri ayrı "silme dosyası" olarak yazıp okuma anında uygulama. Yazma hızlı, okuma yavaştır; karşıtı copy-on-write'tır (yazma yavaş, okuma hızlı). |
| **compaction** | Çok sayıda küçük veri dosyasını birkaç büyük dosyada birleştirme bakımı. Sorgu hızını ve S3 istek sayısını doğrudan etkiler. |
| **Apache Polaris** | Iceberg REST kataloğu: hangi tablonun nerede olduğunu, kimin neye erişebileceğini bilir ve istemcilere geçici S3 kimliği dağıtır. |
| **katalog** | Tablo isimlerinin fiziksel dosyalara eşlendiği kayıt defteri. Bu kurulumda katalog adı `lakehouse`'tur. |
| **namespace (ad alanı — katalogda)** | Katalog içindeki tablo grubu; SQL'deki şema karşılığıdır (`shop_raw`, `shop`, `gold`). Kubernetes ad alanıyla karıştırılmamalıdır. |
| **Apache Spark** | Dağıtık veri işleme motoru. Bu üründe Silver birleştirmesi, Iceberg bakımı ve MongoDB Bronze yüklemesi Spark işleridir. |
| **SparkApplication** | Bir Spark koşusunu tanımlayan Kubernetes CR'ı. Uygulandığında Spark Operator sürücü ve çalıştırıcı pod'larını yaratır; bir kez koşar. |
| **ScheduledSparkApplication** | Aynı şeyin zamanlanmış hâli: cron ifadesine göre tekrar tekrar `SparkApplication` üretir. |
| **Trino** | Dağıtık SQL sorgu motoru. Tek sorgu kapısıdır: Superset, Zeppelin ve not defterleri de Trino'ya bağlanır, böylece yetki kuralları tek yerde uygulanır. |
| **Apache Superset** | Pano ve grafik arayüzü; SQL bilmeyen kullanıcıların yüzü. |
| **JupyterHub** | Her kullanıcıya kendi kalıcı diskli Python not defteri ortamını veren sunucu (PySpark, PyIceberg, Trino istemcisi kurulu). |
| **Apache Zeppelin** | Paylaşımlı SQL not defteri; Trino'ya JDBC ile bağlanır. |
| **Keycloak** | Kimlik sağlayıcı. Active Directory'yi okur, arayüzlere tek oturum açma verir, grup bilgisini jetona koyar. |
| **OIDC** (OpenID Connect) | Uygulamaların "bu kullanıcı kim" sorusunu kimlik sağlayıcıya sorduğu standart. Tarayıcı Keycloak'a yönlendirilir, geri dönerken imzalı bir jeton taşır. |
| **realm** | Keycloak'ta bağımsız bir kullanıcı/istemci/rol evreni. Bu kurulumda tek realm vardır: `lakehouse`. |
| **Active Directory (AD)** | Kurumun kullanıcı ve grup dizini. Gün-1'den zorunludur; kullanıcı ve parola tek kaynak olarak burada durur. |
| **LDAPS** | Dizine TLS üzerinden erişim protokolü (636 portu). Şifresiz LDAP kullanılmaz. |
| **Route** | OpenShift'te bir servisi küme dışına açan nesne. Adresler bileşen adı + `-` + ad alanı + `.` + uygulama alan adı biçiminde türetilir. |
| **ArgoCD Application** | "Şu Git yolundaki manifestler şu ad alanında dursun" diyen CR. Bu kurulumda her bileşenin bir Application'ı vardır. |
| **sync (eşitleme)** | ArgoCD'nin Git'teki hâli kümeye uygulaması. `Synced` = küme Git ile aynı, `OutOfSync` = fark var, `Healthy` = nesneler sağlıklı. |
| **Helm values** | Bir chart'ın davranışını belirleyen değer dosyası. Ürün varsayılanları `platform/values/` altında, müşteriye özel değerler yalnız `platform/values/site/` altındadır. |
| **Secret** | Kubernetes'te parola/anahtar saklayan nesne. Git'e **girmez**; kurulum sırasında komutla yaratılır. |
| **StorageClass** | Kümenin hangi depolamadan disk vereceğini belirleyen sınıf. Boş bırakılırsa kümenin varsayılanı kullanılır. |
| **PVC** (PersistentVolumeClaim) | Bir pod'un "şu boyutta kalıcı disk istiyorum" talebi. Kafka, veritabanları, Zeppelin ve not defterleri PVC kullanır. |
| **CNPG** (CloudNativePG) | PostgreSQL'i Kubernetes'te işleten operatör. Polaris, Keycloak ve Superset veritabanları CNPG kümeleridir. |
| **Barman Cloud** | CNPG'nin S3'e sürekli yedek alan eklentisi; zaman noktasına geri dönüşü (PITR) sağlar. |
| **Velero / OADP** | Ad alanı ve kalıcı disk yedekleme aracı. OADP, Red Hat'in OpenShift için paketlediği Velero'dur. |
| **UWM** (kullanıcı iş yükü izleme) | OpenShift'in kullanıcı ad alanlarındaki metrikleri toplayan izleme yığını. Ayrı Prometheus kurulmaz. |
| **PrometheusRule** | Alarm tanımını taşıyan CR. Bu üründe beş alarm tanımlıdır. |

---

## Kontrol listesi

Bu bölümü bitirdiğinizde aşağıdakileri **yazılı kaynağa bakmadan** söyleyebilmelisiniz:

- [ ] Kaynak veritabanındaki bir `UPDATE`, hangi beş durakta hangi sırayla ilerler.
- [ ] Bronze ile Silver arasındaki fark nedir; son kullanıcı hangisini sorgular.
- [ ] Üç bakım işi hangi problemi çözer ve hangi sıklıkta koşar.
- [ ] Bir kullanıcının Superset'e girebilmesi için hangi iki sistemde (AD ve Keycloak) ne olması gerekir.
- [ ] Hangi üç veri yedeklenir, hangisi yedeklenmez ve neden.
- [ ] Kümeye elle `oc apply` yapmak yerine neyin yapılması gerektiği.

## Sonraki bölüm

10-planlama.md — boyutlandırma, ağ/port özeti, doldurmanız gereken değerlerin çalışma sayfası ve hangi ekipten neyi isteyeceğiniz.
