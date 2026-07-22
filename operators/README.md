# Operatör ön-koşulları (OLM bootstrap)

Bu klasör, Lakehouse **Helm chart'ının** (`chart/`) ihtiyaç
duyduğu **cluster-scoped OperatorHub/OLM operatörlerini** kurar. Bunlar
`helm install`/`helm upgrade` **ÖNCESİNDE**, tek seferlik ve namespace'ten
bağımsız bir bootstrap adımı olarak uygulanır — chart'ın kendisi bunları
KURMAZ: namespaced bir Helm release, cluster-scoped OLM Subscription/
OperatorGroup'u temiz şekilde yönetemez.

Kurulum akışı:

```
1. operators/  (bu klasör)         ── cluster-scoped, ÖNCE
        │  oc apply -f operators/
        │  oc get csv -A → hepsi Succeeded
        ▼
2. Secret'lar (ExternalSecret/Vault veya `oc create secret`)
        ▼
3. helm install lakehouse chart/ -n example -f values-prod.yaml
        │  (chart/templates/_prereq-check.tpl + prereq-check-job.yaml bu
        │   operatörlerin CRD'lerinin gerçekten kurulu olduğunu doğrular
        │   — bkz. "Chart prereq-check ile ilişki" aşağıda)
        ▼
4. NOTES.txt → route'lar + doğrulama komutları
```

## Kapsanan operatörler

`tools/README.md`'nin "Ön koşullar" tablosundaki 9 operatörden
bu chart'ın kapsadığı **6 tanesi** (kalan 3'ü — NVIDIA GPU Operator,
OpenShift Logging/Loki, Prometheus/Grafana — bu chart'ın CR'larının
doğrudan bağımlı olmadığı, ayrı alt-projelerin/kapsam-dışı bileşenlerin
ön-koşuludur):

| Dosya                          | Operatör                  | Namespace (README tablosu) | Sağladığı CRD grubu (bu chart'ta kullanılan) |
|--------------------------------|----------------------------|------------------------------|-----------------------------------------------|
| `00-cert-manager.yaml`         | cert-manager               | `cert-manager`               | `cert-manager.io/v1` |
| `01-external-secrets.yaml`     | ExternalSecrets Operator   | `external-secrets`            | `external-secrets.io/v1beta1` |
| `02-cloudnativepg.yaml`        | CloudNativePG (CNPG)       | `cnpg-system`                 | `postgresql.cnpg.io/v1` |
| `03-strimzi.yaml`              | Strimzi (Kafka)            | `openshift-operators`         | `kafka.strimzi.io/v1` (chart default, parametrized via `versions.strimziApi`) |
| `04-spark-operator.yaml`       | Spark Operator             | `spark-system`                | `sparkoperator.k8s.io/v1beta2` |
| `05-keycloak-operator.yaml`    | Keycloak Operator          | `keycloak-system`             | `k8s.keycloak.org/v2alpha1` |

Her dosya kendi `Namespace` + (gerekiyorsa) `OperatorGroup` + `Subscription`
CR'larını birlikte taşır — tek dosya = tek operatör = tek uygulanabilir
birim. `03-strimzi.yaml` istisnadır: kendi namespace'i/OperatorGroup'u YOK,
çünkü `openshift-operators` her OpenShift kümesinde varsayılan olarak gelen
paylaşımlı bir namespace + global OperatorGroup'tur (bkz. o dosyanın başlık
yorumu).

## Kurulum

```bash
# Hepsini tek seferde uygula (dosya-içi sıralama her operatör için zaten
# Namespace -> OperatorGroup -> Subscription; operatörler birbirinden
# BAĞIMSIZDIR, OLM seviyesinde aralarında bir kurulum-sırası zorunluluğu
# yoktur — bu yüzden tek `oc apply -f` yeterlidir):
oc apply -f operators/
```

Önerilen (ama zorunlu olmayan, yalnızca okunabilirlik/troubleshooting
kolaylığı için) mantıksal sıra — dosya adlarındaki `00-05` numaralandırması
bunu yansıtır:

1. **cert-manager** + **ExternalSecrets Operator** — temel/genel-amaçlı
   cluster-wide yardımcı operatörler, diğer her şeyden önce kurulsa da olur.
2. **CloudNativePG** + **Strimzi** — veri-düzlemi operatörleri (Postgres,
   Kafka); chart'ın CNPG `Cluster`/Strimzi `Kafka` CR'ları bunlara bağımlı.
3. **Spark Operator** — SparkApplication CRD'si.
4. **Keycloak Operator** — en sona bırakılması mantıklı, çünkü kendi CNPG
   Postgres cluster'ını (`pg-keycloak`) barındıracak; CNPG'nin
   zaten kurulu/`Succeeded` olması operasyonel olarak faydalı (yine de OLM
   operatör-KURULUMU seviyesinde birbirine bağımlı değiller — yalnızca
   chart'ın SONRAKİ CR'ları CNPG'ye bağımlı).

Her operatörü tek tek de uygulayabilirsiniz (`oc apply -f
operators/02-cloudnativepg.yaml`), sıra önemli değildir.

## Doğrulama

```bash
# Her Subscription bir InstallPlan üretir, o da bir CSV (ClusterServiceVersion)
# oluşturur; CSV'nin PHASE'i "Succeeded" olduğunda operatör tam kurulu demektir.
oc get subscriptions.operators.coreos.com -A | \
  grep -E "cert-manager|external-secrets|cloudnative-pg|strimzi|spark-operator|keycloak"

oc get csv -A | \
  grep -E "cert-manager|external-secrets|cloudnative-pg|strimzi|spark-operator|keycloak"
# Her satırda PHASE = Succeeded beklenir. "Pending"/"InstallReady" ise henüz
# reconcile olmamıştır (birkaç dakika bekleyip tekrar kontrol edin);
# "Failed" ise `oc describe csv <ad> -n <namespace>` ile nedenine bakın.

oc get installplan -A   # onaylanmamış (Manual) bir InstallPlan kaldıysa görünür

# CRD'lerin gerçekten registered olduğunu (chart/templates/prereq-check-job.yaml
# ile birebir aynı liste) doğrudan doğrula:
oc get crd kafkas.kafka.strimzi.io \
           clusters.postgresql.cnpg.io \
           sparkapplications.sparkoperator.k8s.io \
           keycloaks.k8s.keycloak.org \
           externalsecrets.external-secrets.io \
           certificates.cert-manager.io
# Her biri için CONDITIONS -> Established: True beklenir.
```

Bu doğrulama adımları, `tools/README.md`'nin kendi "Küme
operatörlerini doğrulama" bölümüyle tutarlıdır (aynı `oc get subscriptions`
deseni) — bu dosya onun Helm-chart-tabanlı kuruluma özel, `operators/`
klasörüyle bire-bir eşleşen genişletilmiş halidir.

## Kurulum sıralaması (chart içi kaynaklar)

`operators/` + secret'lar hazır olduktan sonra `helm install` chart'ın TÜM
kaynaklarını **tek geçişte** uygular. Chart, kaynaklar arası kritik sırayı
`helm.sh/hook` / `helm.sh/hook-weight` annotasyonlarıyla **ZORLAMAZ** —
bilinçli bir tercihtir: CNPG `Cluster`, Strimzi `Kafka`/`KafkaConnect`/
`KafkaConnector`, Keycloak CR'ları gibi stateful, operatör-yönetimli
kaynakları birer Helm hook'una çevirmek onları release yönetiminin dışına
çıkarır (normal `helm upgrade` diff/patch, `helm rollback` ve
`helm get manifest` semantiğini kaybederler; hook-delete-policy'ye göre her
upgrade'de yeniden yaratılıp silinebilirler) — kalıcı platform kaynakları
için bir anti-pattern. Chart'taki tek GERÇEK hook, tek-seferlik
`chart/templates/prereq-check-job.yaml` doğrulama Job'ıdır (`pre-install`/
`pre-upgrade`) — kısa ömürlü bir kontrol için hook semantiği doğrudur.

Sıra bunun yerine üç mekanizmadan kendiliğinden oturur:
1. **Helm'in native kind sıralaması** — Namespace/ResourceQuota/LimitRange/
   NetworkPolicy/StorageClass/ServiceAccount/Secret/ConfigMap → Service →
   Deployment → Job, her release'de otomatik.
2. **Operatör reconcile'ı** — Strimzi/CNPG/Keycloak/Spark operatörleri kendi
   CR'larını asenkron olarak sağlar (Service'ler, Secret'lar dahil).
3. **Pod readiness + restart** — tüketiciler (Nessie←`pg-nessie`,
   Connect←Kafka+Apicurio) bağımlılıkları hazır olana kadar readinessProbe
   ile bekler / yeniden başlar.

Mantıksal kademe (bilgi amaçlı — chart/templates/NOTES.txt "INSTALL ORDERING"
ile aynı):

```
1. namespace / quota / limits / NetworkPolicy / StorageClass   (temel)
2. CNPG Postgres cluster'ları + Kafka omurgası + Apicurio        (stateful)
3. Nessie + Trino + Kafka Connect + Spark + Keycloak uygulaması  (2'ye bağımlı)
4. örnek connector'lar + Console + Kafka UI + Route'lar          (en dış)
```

> **ÖNEMLİ — Trino/Keycloak sırası, adım 3'ün geri kalanıyla DEĞİŞTİRİLEBİLİR
> DEĞİL:** Nessie'nin OIDC kontrolü LAZY'dir (Quarkus) — issuer henüz erişilir
> olmasa bile pod `Running` kalır, yalnızca gerçek API çağrılarında 500 döner
> (yumuşak bozulma). Trino coordinator'ı ise başlangıçta EAGER OIDC discovery
> yapar — issuer henüz erişilir değilse coordinator process'i exception
> fırlatıp CRASH-LOOP'a girer, Nessie gibi nazikçe yeniden denemez (sert
> hata). Sonuç: `components.keycloak=true` iken Keycloak'ın (CR'ları + Route)
> tam yakınsamasını ve OIDC issuer URL'sinin çözülebilir olmasını BEKLEYİN,
> Trino'nun aynı anda paralel yakınsayacağını varsaymayın.

İlk dakikalarda geçici `CrashLoopBackOff` BEKLENİR (bağımlılık henüz hazır
değil) — hata değildir. Yakınsamayı izleyin:

```bash
watch oc -n <ns> get pods,cluster,kafka,kafkaconnect
```

> POC-DOĞRULA (plan "Açık POC-DOĞRULA"): operatör-reconcile + readiness'in
> sert veri-bağımlılıklarında (özellikle Connect←Kafka+Apicurio) her zaman
> yeterli olup olmadığı canlı kümede doğrulanacak; gerekirse küçük bir `wait`
> init-container/Job eklenir (yine hook değil, normal kaynak olarak).

## Chart prereq-check ile ilişki

Bu operatörlerin `oc apply -f operators/` ile kurulmuş + `Succeeded` olması,
chart'ın kendi CR'larını (Kafka, CNPG Cluster, SparkApplication, Keycloak,
Certificate, ...) sorunsuz reconcile edebilmesi için bir **ön-koşuldur**,
ama Helm'in kendisi bunu otomatik olarak bilemez/zorlayamaz (Helm namespaced
bir release; bu operatörler cluster-scoped). Bu yüzden `chart/` içinde İKİ
bağımsız, tamamlayıcı bir "prereq-check" mekanizması vardır:

1. `chart/templates/_prereq-check.tpl` — `helm install`/`helm upgrade`
   sırasında `.Capabilities.APIVersions.Has` ile yukarıdaki 6 CRD grubunun
   kümede kayıtlı olup olmadığını template-zamanında kontrol eder; eksikse
   açık bir hata mesajıyla (`operators/README.md`'ye — yani bu dosyaya —
   yönlendirerek) `helm install`'i durdurur.
2. `chart/templates/prereq-check-job.yaml` — bir `pre-install`/`pre-upgrade`
   hook Job'u; kümenin içinden gerçek bir `kubectl get crd` ile aynı 6 CRD'yi
   tekrar (ve daha güvenilir şekilde) doğrular.

Her ikisi de tek bir values anahtarıyla açılır (`prereqCheck.enabled`,
varsayılan `false` — bkz. `chart/values.yaml`'daki uzun yorum: bu varsayılan,
`helm template`'in canlı bir kümeye bağlı OLMADAN da (CI, bu repo'nun kendi
`tools/validate.sh --helm` test koşumu) render olabilmesi için
kasıtlı olarak kapalıdır). **Gerçek bir kurulumda**, `operators/` uygulanıp
her CSV `Succeeded` olduktan sonra:

```bash
helm install lakehouse chart/ -f values-prod.yaml -n example \
  --set prereqCheck.enabled=true
```

ile bu iki kontrolü de etkinleştirin — eksik bir operatör varsa `helm
install` net bir hata mesajıyla (hangi operatör, hangi CRD, bu README'ye
yönlendirme) hemen durur; hepsi kuruluysa sorunsuz devam eder.

## Önemli notlar / POC-DOĞRULA özet

Her operatör dosyasının kendi başlık yorumunda ayrıntılı POC-DOĞRULA notu
vardır (paket adı/kanal/katalog kaynağı gerçek bir OperatorHub kataloğuna
karşı bu depoda DOĞRULANMAMIŞTIR — bu depo bir kümeye bağlı değildir).
Özet olarak, kurulumdan önce her operatör için şunu çalıştırıp gerçek
paket adı/kanalı teyit edin:

```bash
oc get packagemanifests -n openshift-marketplace | \
  grep -iE "cert-manager|external-secrets|cloudnative-pg|strimzi|amq-streams|spark|keycloak"
```

En belirsiz olanı **Spark Operator**'dır (bkz. `04-spark-operator.yaml`
başlık yorumu) — bazı kümelerde OLM paketi olarak hiç mevcut olmayabilir ve
bunun yerine kendi Helm chart'ı ile (`helm install spark-operator
spark-operator/spark-operator`) kurulması gerekebilir; bu durumda bu dosya
o kümenin runbook'unda atlanır/değiştirilir.
