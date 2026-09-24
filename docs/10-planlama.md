# 10 — Planlama

**Bu bölümde:** kuruluma başlamadan önce verilmesi gereken kararlar, bileşen başına kaynak ihtiyacı (küçük/orta/büyük), ağ ve port özeti, doldurmanız gereken değerlerin çalışma sayfası, gerçekçi süre tahmini ve hangi işin hangi ekipte olduğu.
**Süre:** 45 dakika okuma + değerlerin ilgili ekiplerden toplanması (pratikte 2–5 iş günü bekleme).
**Gereken yetki:** iki komut için kümede `oc` ile okuma yetkisi (`cluster-reader` yeterlidir). Geri kalanı okumadır.
**Nerede çalıştırılır:** `[bastion]` — `oc` kurulu ve kümeye giriş yapılmış yönetim makinesi.

---

## 1. Mimari kararlar (verilmiş kararlar, kısa gerekçeleriyle)

Bu kararlar ürünle birlikte gelir; kurulumda yeniden tartışılmaz. Sonuçları bütün belgeleri etkilediği için burada topluca verilir.

| Karar | Sonuç |
|---|---|
| **Kurulum yolu OpenShift GitOps (ArgoCD)'tur.** | Kümede elle `oc apply` yapılmaz. Kurulum da, gün-2 değişikliği de Git'e yazmakla olur. Helm ile doğrudan kurulum yalnız geliştiricilerin dizüstü kümesi (kind) içindir. |
| **Depo müşterinin kendi Git sunucusuna kopyalanır.** | ArgoCD sizin kopyanızı okur (`$GIT_REPO_URL`). Ürün güncellemeleri bu kopyaya birleştirilir; böylece kurumun değişiklikleri kaybolmaz. |
| **Müşteriye özel değerler tek dizindedir: `platform/values/site/`.** | `platform/values/site/glue.yaml`, `platform/values/site/trino.yaml`, `platform/values/site/jupyterhub.yaml` — düzenlediğiniz tek yer burasıdır. Ürün varsayılanları `platform/values/` altında kalır ve yükseltmelerde üstüne yazılır. Üç dosyanın tutarlılığını `scripts/check-site.sh` denetler. |
| **Adresler türetilir.** | Bileşen adresi = bileşen adı + `-` + `$LAKEHOUSE_NS` + `.` + `$APPS_DOMAIN`. Yani yalnız `appsDomain` doldurulur; Keycloak yönlendirme adresleri de aynı kaynaktan üretilir. Kurumsal bir DNS adı isteyen bileşen için `platform/values/site/glue.yaml` içinde tek satır açılır. |
| **Active Directory gün-1'den zorunludur.** | AD'siz bir başlangıç tasarlanmamıştır. Kurulum günü AD bilgileri hazır değilse kurulum başlamaz. |
| **Gruplar sabittir:** `lakehouse-admins`, `lakehouse-analysts`, `lakehouse-users`. | Bu üç grup AD'de kurulumdan **önce** açılmış olmalıdır. Yetki kuralları (Trino, Superset, JupyterHub) bu adlara göre yazılıdır. |
| **Özel konteyner imajı yoktur.** | Bütün imajlar üreticinin resmi imajlarıdır. Bağlantısız (air-gapped) kurulumda aynalanacak imaj listesi bu yüzden sabittir ve sürüm tablosundan okunur. |
| **Kurumun kendi kaynakları `custom/` klasöründedir.** | Kendi Spark uygulamanız, CronJob'ınız ya da ConfigMap'iniz ürün chart'ına dokunmadan buraya eklenir; ayrı bir ArgoCD Application izler. Örnekler: `custom/examples/spark-tek-seferlik.yaml`, `custom/examples/spark-zamanli.yaml`, `custom/examples/dbt-cronjob.yaml`. |

---

## 2. Boyutlandırma

### 2.1 Üç kademe ne demek

| Kademe | Kim için | Kabaca |
|---|---|---|
| **Küçük** | pilot, tek departman; toplam 1–2 kaynak veritabanı, en çok ~50 tablo, günde birkaç milyon değişiklik olayı; 10'a kadar eşzamanlı kullanıcı | **Deponun bugünkü varsayılanları** (`glue/values.yaml` + `platform/values/`). Hiçbir şey değiştirmeden kurulur. |
| **Orta** | üretim, birkaç kaynak sistem; ~500 tabloya kadar, günde on milyonlarca olay; 25–50 eşzamanlı kullanıcı | Depodaki yorumlu "büyük tier" bloğu (`platform/values/glue.yaml` içinde kapalı duran `spark` bloğu) açılır, disk ve replika sayıları artırılır. |
| **Büyük** | kurum geneli; binlerce tablo, sürekli yüksek hacim; 100+ kullanıcı | Orta kademenin çalıştırıcı sayısı ve bellek katları. **Bu sütun bir başlangıç noktasıdır, ölçüm değildir**: bir ay çalıştırıp gerçek kullanımı ölçmeden bu değerlere sabitlenmeyin. |

> Küçük sütunu depodaki gerçek varsayılanlardır. Orta ve büyük sütunlar **başlangıç noktasıdır**; üretimde ilk ayın metriklerine (bkz. [50-isletme/izleme-ve-alarmlar.md](50-isletme/izleme-ve-alarmlar.md)) bakılarak düzeltilir.

### 2.2 Bileşen başına istek/limit

"İstek" (request), pod'un düğümde yer ayırtmak için beyan ettiği asgari kaynaktır; kapasite planı bunun üzerinden yapılır.

> **Değişiklik nereye yazılır.** Aşağıdaki "Küçük" sütunu `glue/values.yaml` ve `platform/values/*.yaml`
> dosyalarındaki ürün varsayılanlarını gösterir; bu dosyalar **yükseltmede üzerine yazılır**, bu yüzden
> orada değiştirilmez. Boyutlandırma ezmeleri müşteriye özel site dosyalarına yazılır — ArgoCD bu
> dosyaları en SON yükler ve ürün varsayılanlarını ezerler: glue chart'ı için
> `platform/values/site/glue.yaml`, Trino için `platform/values/site/trino.yaml`, JupyterHub için
> `platform/values/site/jupyterhub.yaml`. Anahtar site dosyasında yoksa, tablodaki yolu aynen yazarak
> eklersiniz.

| Bileşen | Küçük (bugünkü varsayılan) | Orta (başlangıç) | Büyük (başlangıç) | Nerede değiştirilir |
|---|---|---|---|---|
| **Kafka broker** (3 adet) | CPU/RAM isteği **tanımlı değil** (kümenin varsayılanı geçerlidir); disk **100Gi/broker** | disk 250Gi/broker | disk 500Gi/broker, 5 broker | `platform/values/site/glue.yaml` → `kafka.storageSize`, `kafka.replicas` |
| **Kafka Exporter** | 10m CPU / 64Mi (limit 128Mi) | aynı | aynı | şablonda sabit, değiştirilmez (`glue/templates/kafka.yaml`) |
| **Kafka Connect** (Debezium + Iceberg sink) | 1 kopya, 500m CPU / 1536Mi (limit 2Gi) | 2 kopya, 1 CPU / 3Gi (limit 4Gi) | 3 kopya, 2 CPU / 6Gi (limit 8Gi) | `platform/values/site/glue.yaml` → `connect.replicas`, `connect.resources` |
| **Spark sürücü** | 1 çekirdek / 2g + 512m ek | 2 çekirdek / 4g + 1g ek | 2 çekirdek / 4g + 1g ek | `platform/values/site/glue.yaml` → `spark.driver` |
| **Spark çalıştırıcı** | 1 adet × 1 çekirdek / 2g + 512m ek | 2 adet × 2 çekirdek / 4g + 1g ek | 4 adet × 2 çekirdek / 8g + 2g ek | `platform/values/site/glue.yaml` → `spark.executor` |
| **Spark shuffle bölümü** | 8 | 200 | 400 | `platform/values/site/glue.yaml` → `spark.shufflePartitions` |
| **PostgreSQL** (polaris-db, keycloak-db, superset-db) | üretimde **2 kopya**/küme, 10Gi disk/kopya | 20Gi disk/kopya | 50Gi disk/kopya | `platform/values/site/glue.yaml` → `cnpg.*.instances`, `cnpg.*.storageSize` |
| **Polaris** | chart varsayılanı (istek tanımlı değil) | aynı | 2 kopya | site dosyası **yok**: `platform/values/polaris.yaml` düzenlenir, yükseltmede elle birleştirilir |
| **Trino koordinatör** | JVM yığını 8G, sorgu başına düğüm belleği 1GB | aynı | JVM yığını 16G | `platform/values/site/trino.yaml` → `coordinator.jvm` |
| **Trino işçi** | **2 işçi**, JVM yığını 8G | 4 işçi | 8 işçi, JVM yığını 16G | `platform/values/site/trino.yaml` → `server.workers`, `worker.jvm` |
| **Superset** | 1 kopya, 250m CPU / 768Mi (limit 2Gi) | 2 kopya | 3 kopya, 500m CPU / 1536Mi | `platform/values/site/glue.yaml` → `superset.replicas`, `superset.resources` |
| **JupyterHub hub + proxy** | 100m/256Mi + 100m/128Mi | aynı | 200m/512Mi | `platform/values/site/jupyterhub.yaml` → `hub.resources`, `proxy.chp.resources` |
| **JupyterHub kullanıcı pod'u** | garanti 0,5 CPU / 1G, limit 2 CPU / 4G, 10Gi disk | garanti 1 CPU / 2G, limit 4 CPU / 8G, 20Gi disk | garanti 2 CPU / 4G, limit 8 CPU / 16G, 50Gi disk | `platform/values/site/jupyterhub.yaml` → `singleuser` |
| **Zeppelin** | 250m CPU / 1Gi (limit 2560Mi), 10Gi disk, JVM 1024m | 500m / 2Gi (limit 4Gi), JVM 2048m | 1 CPU / 4Gi (limit 8Gi), JVM 4096m | `platform/values/site/glue.yaml` → `zeppelin.resources`, `zeppelin.mem`, `zeppelin.intpMem` |
| **Keycloak** | operatör varsayılanı | aynı | 2 kopya | şablonda sabit, değiştirilmez (`glue/templates/keycloak.yaml`) |

**Dikkat edilecek üç nokta.**

1. **Trino'nun JVM yığını Kubernetes isteği değildir.** Değer dosyasında istek/limit yoktur ama her Trino pod'u 8 GiB yığın ayırır; koordinatör + 2 işçi için **düğümlerde en az 3 × 10 GiB boş bellek** bulunmalıdır. Küçük kademede bile bu böyledir.
2. **Kafka diski geriye dönük doldurmayı da karşılamalıdır.** İlk tam okuma (snapshot) sırasında kaynak tabloların tamamı Kafka'dan geçer. Kaynak veritabanının toplam boyutunun **en az yarısı kadar** boş Kafka diski planlayın.
3. **JupyterHub kapasitesi kullanıcı sayısıyla çarpılır.** Eşzamanlı 20 kullanıcı, küçük kademede 20 × 1G garanti bellek ve 20 × 10Gi kalıcı disk demektir. Not defteri diskleri kullanıcı ayrılsa da silinmez.

### 2.3 Kaba toplam (kabaca, kurulum anı)

| Kademe | Düğümlerden istenen CPU | Düğümlerden istenen RAM | Kalıcı disk (kullanıcı diskleri hariç) |
|---|---|---|---|
| Küçük | ~8 vCPU | ~48 GiB | ~370 GiB |
| Orta | ~20 vCPU | ~96 GiB | ~800 GiB |
| Büyük | ~48 vCPU | ~200 GiB | ~2 TiB |

Bu satırlar Iceberg verisini **içermez**: veri S3'tedir ve tabloların boyutu kaynak sistemlere bağlıdır. S3 tarafında kaynak veritabanlarının toplam boyutunun 2–3 katını planlayın (Bronze tarihçesi + Silver kopyası + bakım sırasında geçici dosyalar).

---

## 3. Ön ölçüm: kümenin size söyleyeceği iki değer

Bu iki değeri şimdi alın; [30-kurulum.md](30-kurulum.md) bölümünde `$APPS_DOMAIN` ve `$STORAGE_CLASS` olarak kullanılacaklar.

### Adım 1 — Uygulama alan adını öğrenin

`[bastion]`

```bash
oc get ingresses.config cluster -o jsonpath='{.spec.domain}'
```

**Beklenen çıktı** (örnek — değer kümenize göre değişir):

```text
apps.ocp.example.net
```

**Ters giderse:** komut `Error from server (Forbidden)` derse hesabınızda küme düzeyinde okuma yetkisi yoktur; küme yöneticisinden `cluster-reader` isteyin ya da değeri doğrudan ondan alın. Boş çıktı, kümeye giriş yapılmadığı anlamına gelir (`oc whoami` ile doğrulayın).

### Adım 2 — Kullanılabilir StorageClass'ları listeleyin

`[bastion]`

```bash
oc get storageclass
```

**Beklenen çıktı** (örnek — değer kümenize göre değişir; sınıf ve sağlayıcı adları
depolama satıcınıza bağlıdır, aşağıdaki iki satır yalnızca biçimi gösterir):

```text
NAME                   PROVISIONER   RECLAIMPOLICY   VOLUMEBINDINGMODE   ALLOWVOLUMEEXPANSION   AGE
pure-block (default)   pure-csi      Delete          Immediate           true                   41d
pure-file              pure-csi      Delete          Immediate           true                   41d
```

`(default)` işaretli sınıf varsa `$STORAGE_CLASS` boş bırakılabilir. Sınıf **ReadWriteOnce** blok depolama sağlamalı ve **disk büyütmeyi desteklemelidir** (`ALLOWVOLUMEEXPANSION: true`) — aksi hâlde Kafka diskini büyütmek için pod'ları yeniden yaratmak gerekir.

**Ters giderse:** hiç StorageClass yoksa küme depolamasız kurulmuştur; bu ürün kalıcı disk olmadan kurulamaz, platform ekibine başvurun.

---

## 4. Ağ ve portlar (özet)

Ayrıntılı liste (her servis, port, yön, protokol) [90-referans/port-ve-servisler.md](90-referans/port-ve-servisler.md) dosyasındadır. Planlama için gereken kadarı:

| Yön | Kimden | Kime | Port / protokol | Ne için |
|---|---|---|---|---|
| Küme → dışarı | Kafka Connect, Spark | Kaynak PostgreSQL | 5432/TCP | CDC okuması |
| Küme → dışarı | Kafka Connect, Spark | Kaynak SQL Server | 1433/TCP | CDC okuması |
| Küme → dışarı | Kafka Connect, Spark | Kaynak MongoDB | 27017/TCP | CDC okuması |
| Küme → dışarı | Polaris, Connect, Spark, not defterleri | S3 (`$S3_ENDPOINT`) | 443/HTTPS | Iceberg verisi ve yedekler |
| Küme → dışarı | Keycloak, Trino | Active Directory (`$LDAP_URL`) | 636/LDAPS | kimlik ve grup okuması |
| Küme → dışarı | Spark, JupyterHub, dbt örneği | Maven Central, PyPI | 443/HTTPS | çalışma anında kütüphane indirme (kapalı ağda iç ayna gerekir) |
| Dışarıdan → küme | Tarayıcılar | Trino, Superset, JupyterHub, Zeppelin, Keycloak Route'ları | 443/HTTPS | kullanıcı arayüzleri |
| Dışarıdan → küme | Fluent Bit ajanları (yalnız nginx akışı açıksa) | Kafka dış dinleyicisi (Route) | 443/TLS + SCRAM | erişim günlüğü |
| Küme içi | Superset, Zeppelin, JupyterHub, Spark | Trino | 8443/HTTPS | SQL |
| Küme içi | Trino, Spark, not defterleri | Polaris | 8181/HTTP | Iceberg kataloğu |
| Küme içi | Polaris, Keycloak, Superset | kendi PostgreSQL kümeleri | 5432/TCP | veritabanı |

**DNS ve sertifika:** bütün arayüz adresleri `$APPS_DOMAIN` altındaki joker DNS kaydından gelir; ek DNS kaydı **gerekmez**. Trino kendi TLS'ini sonlandırdığı için Route'u `passthrough`'tur: tarayıcıların iç kök CA'ya güvenmesi gerekir. Kurumsal CA kullanılacaksa sertifika talebi bu aşamada açılmalıdır (teslim süresi çoğu kurumda kurulumun önündeki en uzun beklemedir).

---

## 5. Değerler çalışma sayfası

Aşağıdaki tablo `install/lakehouse.env.example` dosyasının okunabilir hâlidir. Kurulum gününden **önce** bütün satırların "alındı" sütunu işaretlenmiş olmalıdır. Şablonu kopyalayıp doldurun:

`[bastion]`

```bash
cp install/lakehouse.env.example install/lakehouse.env && chmod 600 install/lakehouse.env
```

**Beklenen çıktı:** komut sessizce biter (çıkış kodu 0). Dosya izni `-rw-------` olmalıdır (`ls -l install/lakehouse.env` ile doğrulayın).

**Ters giderse:** `Permission denied` alırsanız depoyu yazma izniniz olan bir dizine klonlayın. Dosya zaten varsa üzerine yazmaz — `cp -i` uyarısını dikkate alın.

| Değişken | Kimden alınır | Nasıl / örnek | Alındı |
|---|---|---|---|
| `GIT_REPO_URL` | Git yöneticisi | müşteri deposunun klonlama adresi; `git ls-remote` ile doğrulanır | ☐ |
| `GIT_HTTPS_TOKEN` | Git yöneticisi | yalnız HTTPS depoda; salt okuma erişim jetonu (SSH kullanılıyorsa boş) | ☐ |
| `ARGOCD_NS` | — (sabit) | `openshift-gitops` | ☐ |
| `LAKEHOUSE_NS` | lakehouse kurulumcusu | `lakehouse` (kurumsal ad standardı varsa değiştirilir) | ☐ |
| `APPS_DOMAIN` | küme (bkz. Adım 1) | `oc get ingresses.config cluster -o jsonpath='{.spec.domain}'` | ☐ |
| `STORAGE_CLASS` | küme (bkz. Adım 2) | `oc get storageclass`; varsayılan varsa boş bırakılabilir | ☐ |
| `INTERNAL_REGISTRY` | — (sabit) | `image-registry.openshift-image-registry.svc:5000` | ☐ |
| `S3_ENDPOINT` | depolama ekibi | `https://` ile başlamalı | ☐ |
| `S3_BUCKET_DATA` | depolama ekibi | Iceberg ambarı | ☐ |
| `S3_BUCKET_BACKUP` | depolama ekibi | veri bucket'ından **ayrı** olmalı | ☐ |
| `S3_REGION` | depolama ekibi | verilmezse `us-east-1` | ☐ |
| `S3_ACCESS_KEY` / `S3_SECRET_KEY` | depolama ekibi | veri bucket'ı için anahtar çifti | ☐ |
| `S3_BACKUP_ACCESS_KEY` / `S3_BACKUP_SECRET_KEY` | depolama ekibi | yedek bucket'ı için **ayrı** anahtar çifti | ☐ |
| `LDAP_URL` | AD ekibi | `ldaps://` ve 636; şifresiz LDAP kabul edilmez | ☐ |
| `LDAP_BIND_DN` | AD ekibi | yalnız okuma yetkili servis hesabının tam DN'i | ☐ |
| `LDAP_BIND_PASSWORD` | AD ekibi | kasadan; values dosyalarına yazılmaz | ☐ |
| `LDAP_USERS_DN` | AD ekibi | kullanıcıların arandığı alt ağaç | ☐ |
| `LDAP_GROUPS_DN` | AD ekibi | üç lakehouse grubunun bulunduğu alt ağaç | ☐ |
| `LDAP_CA_FILE` | AD ekibi | AD sertifikasını imzalayan CA'nın PEM dosyası | ☐ |
| `AD_UPN_SUFFIX` | AD ekibi | kullanıcı adlarının UPN son eki (`@` dâhil); Zeppelin AD girişinde kullanılır | ☐ |
| `KEYCLOAK_ADMIN_PASSWORD` | lakehouse kurulumcusu | isteğe bağlı; boş bırakılırsa kurulum `openssl rand -hex 24` ile üretir | ☐ |

Ayrıca AD ekibinden **değer değil, iş** istenir: `lakehouse-admins`, `lakehouse-analysts`, `lakehouse-users` gruplarının açılması ve ilk yöneticinin `lakehouse-admins` grubuna eklenmesi.

---

## 6. Süre tahmini

| Aşama | Süre | Not |
|---|---|---|
| Değerlerin toplanması (AD, depolama, DNS/sertifika) | **2–5 iş günü** | Kurulumun önündeki en uzun bekleme; ilk gün başlatın. |
| Ön koşulların doğrulanması ve operatörlerin kurulması | 1–2 saat | [20-on-kosullar.md](20-on-kosullar.md). |
| Deponun kopyalanması, site değerlerinin doldurulması, Secret'ların yaratılması | 1 saat | `scripts/check-site.sh` yeşile dönene kadar. |
| ArgoCD bootstrap ve bileşenlerin ayağa kalkması | **40–60 dakika** | Ağın hızına bağlı: Kafka Connect imajı kümede üretilir (~10 dk), not defteri imajı ~4,5 dakikada çekilir. |
| Polaris kataloğu, ilk giriş, kabul testi | 1 saat | `scripts/polaris-setup.sh` ve `scripts/acceptance.sh`. |
| İlk kaynak veritabanının bağlanması ve geriye dönük doldurma | tablo boyutuna bağlı: dakikalar–saatler | İlk tam okuma kaynağı meşgul eder; iş saatleri dışına planlayın. |

Toplam: değerler hazırsa **bir iş günü**; değerler beklenerek **bir hafta**.

---

## 7. Sorumluluk matrisi

**Y** = yapan, **D** = destek/onay veren.

| İş | Platform (OpenShift) ekibi | AD ekibi | Depolama ekibi | Lakehouse kurulumcusu |
|---|---|---|---|---|
| Kümede ad alanı ve kotaların açılması | **Y** | | | D |
| OpenShift GitOps operatörünün kurulması | **Y** | | | D |
| StorageClass sağlanması ve kapasitenin ayrılması | **Y** | | D | D |
| Küme → S3 / AD / kaynak DB güvenlik duvarı izinleri | **Y** | D | D | D |
| Joker DNS ve (gerekiyorsa) kurumsal sertifika | **Y** | | | D |
| `lakehouse-admins` / `lakehouse-analysts` / `lakehouse-users` gruplarının açılması | | **Y** | | D |
| Yalnız okuma yetkili AD servis hesabı ve LDAPS CA'sı | | **Y** | | D |
| S3 bucket'ları (veri + yedek) ve iki ayrı anahtar çifti | | | **Y** | D |
| Deponun müşteri Git sunucusuna kopyalanması | D | | | **Y** |
| `platform/values/site/` dosyalarının doldurulması | | | | **Y** |
| Secret'ların kümede yaratılması | D | | | **Y** |
| Bootstrap ve doğrulama | | | | **Y** |
| Kaynak veritabanlarında CDC'nin açılması (yuva/Change Tracking, sinyal tablosu) | | | | **Y** (kaynak DB yöneticisiyle) |
| Gün-2: kaynak/tablo ekleme, kullanıcı yetkisi, yükseltme | D | D | | **Y** |
| Yedeklerin izlenmesi ve geri dönüş tatbikatı | D | | D | **Y** |

---

## Kontrol listesi

- [ ] Kurumun hangi kademeye (küçük/orta/büyük) girdiğine karar verildi ve düğüm kapasitesi platform ekibiyle doğrulandı.
- [ ] Trino için düğümlerde en az 3 × 10 GiB boş bellek olduğu teyit edildi.
- [ ] Kafka diski, kaynak veritabanlarının toplam boyutunun en az yarısı olacak şekilde planlandı.
- [ ] `oc get ingresses.config cluster` ve `oc get storageclass` çıktıları alındı.
- [ ] `install/lakehouse.env` kopyalandı, izni 600 yapıldı ve Git'e girmediği `git status` ile doğrulandı.
- [ ] Çalışma sayfasındaki bütün değerler ilgili ekiplerden istendi; AD grupları açılmak üzere talep edildi.
- [ ] Güvenlik duvarı izinleri (S3, AD, kaynak veritabanları, gerekiyorsa PyPI/Maven) talep edildi.
- [ ] Sorumluluk matrisi ilgili ekiplere gönderildi ve kurulum günü için randevu alındı.

## Sonraki bölüm

[20-on-kosullar.md](20-on-kosullar.md) — kümede neyin kurulu olması gerektiği, operatörlerin kurulumu ve kuruluma başlamadan önceki doğrulamalar.
