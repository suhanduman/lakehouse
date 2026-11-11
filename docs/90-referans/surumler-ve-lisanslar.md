# 90 — Sürümler ve lisanslar

Lakehouse'u oluşturan bütün bileşenlerin sürümü, lisansı, nereden geldiği ve sürümün
depoda hangi dosyada sabitlendiği bu sayfadadır. Kopya sayıları, lisans özeti ve listenin
güncel tutulması da buradadır.

**Ne kadar sürer:** okuması 15 dakika.

**Hangi yetkiler gerekir:** kümedeki gerçek sürümleri görmek için `$ARGOCD_NS` ve
`$LAKEHOUSE_NS` ad alanlarında okuma.

**Komutlar nerede çalışır:** `[bastion]` — `oc login` ile kümeye bağlı ve Git deposunun
kopyasının (`~/lakehouse`) bulunduğu yönetim makinesi.

Şartnamenin **a** maddesi bütün bileşenlerin açık kaynak olmasını, **J.2.1** kullanılan
ürünlerin sürüm ve lisans listesini, **J.3.2** ise bu listenin güncel tutulmasını ister.
Aşağıdaki tablolarda **Nerede sabitlenir** sütunu, sürümün depoda hangi dosyada
yazdığını gösterir; yükseltme o dosyada yapılır.

**Üründe özel imaj yoktur.** Kurulan bütün imajlar bileşenlerin kendi resmî imajlarıdır.
Kafka Connect imajı, Strimzi'nin imaj üretme özelliğiyle kümenin içinde, resmî Debezium ve
Iceberg dosyalarından üretilir.

Depodaki bütün sürüm satırlarını görmek için:

`[bastion]`

```bash
cd ~/lakehouse
grep -rn "targetRevision\|imageTag\|image:" platform/apps platform/values glue/values.yaml
```

**Ne görmelisiniz:** basılan her satır aşağıdaki tablolardan birinde geçen bir sürümdür.
Tabloda karşılığı olmayan bir satır, bu sayfanın güncellenmediğini gösterir.

---

## 1. Platform ve operatörler

| Bileşen | Sürüm | Lisans | Nereden gelir | Nerede sabitlenir | Not |
|---|---|---|---|---|---|
| OpenShift GitOps (ArgoCD) | OpenShift'in sağladığı sürüm (ör. 1.22.3) | Apache-2.0 | Red Hat operatör kataloğu | kümede ön koşul ([20-on-kosullar](../20-on-kosullar.md) madde 2) | ürün yalnız kök uygulamayı ekler |
| cert-manager | v1.21.2 | Apache-2.0 | `quay.io/jetstack/charts/cert-manager` | `platform/apps/00-cert-manager.yaml` | Trino'nun iç sertifikasını üretir |
| Strimzi Kafka operatörü | 1.2.0 | Apache-2.0 | `quay.io/strimzi-helm/strimzi-kafka-operator` | `platform/apps/00-strimzi.yaml` | Kafka imajı `quay.io/strimzi/kafka:1.2.0-kafka-4.3.1` |
| Apache Kafka | 4.3.1 (meta veri sürümü `4.3-IV0`) | Apache-2.0 | Strimzi imajı | `glue/values.yaml` → `versions.kafka`, `versions.kafkaMetadata` | yükseltme iki adımlıdır ([50-isletme/yukseltme.md](../50-isletme/yukseltme.md)) |
| CloudNativePG operatörü | chart 0.29.0 (operatör 1.30.0) | Apache-2.0 | `https://cloudnative-pg.github.io/charts` | `platform/apps/00-cnpg.yaml` | PostgreSQL imajı operatörün varsayılanıdır (`ghcr.io/cloudnative-pg/postgresql:18.4`); PostgreSQL'in lisansı **PostgreSQL License**'tır |
| CNPG Barman Cloud eklentisi | chart 0.8.0 (eklenti v0.15.0) | Apache-2.0 | `cnpg/plugin-barman-cloud` | `platform/apps/00-cnpg-barman.yaml` | veritabanlarının S3'e sürekli yedeği |
| Kubeflow spark-operator | 2.5.2 | Apache-2.0 | `https://kubeflow.github.io/spark-operator` | `platform/apps/00-spark-operator.yaml` | |
| Keycloak operatörü ve Keycloak | 26.7.3 | Apache-2.0 | `github.com/keycloak/keycloak-k8s-resources` (26.7.3) | `platform/keycloak-operator/kustomization.yaml` | operatör ve sunucu aynı sürümde ilerler |
| Apache Superset Kubernetes operatörü | 0.2.0 | Apache-2.0 | `ghcr.io/apache/superset-kubernetes-operator/charts/superset-operator` | `platform/apps/00-superset-operator.yaml` | API sürümü henüz `v1alpha1`'dir; yükseltmelerde sürüm notları okunmalıdır |

---

## 2. Veri bileşenleri

| Bileşen | Sürüm | Lisans | Nereden gelir | Nerede sabitlenir | Not |
|---|---|---|---|---|---|
| Debezium (PostgreSQL, SQL Server, MongoDB) | 3.6.2.Final | Apache-2.0 | Maven Central | `glue/values.yaml` → `versions.debezium` | değişince Connect imajı yeniden üretilir (10 dakika kadar) |
| Apache Iceberg Kafka Connect sink | 1.11.0 | Apache-2.0 | Maven Central | `glue/values.yaml` → `versions.iceberg` | aynı değer Spark işlerinin Iceberg sürümünü de belirler |
| Hadoop istemcisi (Connect imajı) | 3.4.3 | Apache-2.0 | Maven Central | `glue/values.yaml` → `versions.hadoopClient` | Iceberg sink'in gereksinimi |
| Apache Polaris (Iceberg kataloğu) | 1.7.0 | Apache-2.0 | `https://downloads.apache.org/polaris/helm-chart`, `apache/polaris:1.7.0` | `platform/apps/20-polaris.yaml`, `platform/values/polaris.yaml` | katalog veritabanı `polaris-db`; ilk kurulum işi `apache/polaris-admin-tool:1.7.0` |
| `apache-polaris` komut satırı aracı | 1.7.0 | Apache-2.0 | PyPI | `scripts/polaris-setup.sh` | katalog ve yetki kurulumu ([40-kurulum-sonrasi](../40-kurulum-sonrasi.md) §2) |
| Apache Spark | 4.1.0 | Apache-2.0 | `apache/spark:4.1.0-java21-python3` | `glue/values.yaml` → `spark.image` | kütüphaneler küme içi Maven önbelleğinden gelir |
| Trino | 483 (chart 1.42.2) | Apache-2.0 | `https://trinodb.github.io/charts`, `trinodb/trino:483` | `platform/apps/30-trino.yaml`, `platform/values/trino.yaml` | chart'ın varsayılan imajı 480'dir; ürün 483'e sabitler |
| Apache Superset | 6.1.0 (imaj `apache/superset:6.1.0-dev`) | Apache-2.0 | resmî imaj | `glue/values.yaml` → `superset.imageTag` | `-dev` imajı veritabanı sürücülerini içerir. İmaj `apachesuperset.docker.scarf.sh` adresinden çekilir; bu adres Docker Hub'a yönlendirir |
| JupyterHub (z2jh) | chart 4.4.2, JupyterHub 5.5.2 | BSD-3-Clause | `https://hub.jupyter.org/helm-chart/` | `platform/apps/30-jupyterhub.yaml`, `platform/values/jupyterhub.yaml` | chart'la birlikte gelen imajlar: `configurable-http-proxy:5.2.0`, `kube-scheduler:v1.30.14`, `pause:3.10.1` |
| Not defteri imajı | `quay.io/jupyter/pyspark-notebook:spark-4.1.2` | BSD-3-Clause | quay.io | `platform/values/jupyterhub.yaml` → `singleuser.image` | ilk çekim 4–5 dakika sürer |
| Apache Zeppelin | 0.12.1 | Apache-2.0 | `apache/zeppelin:0.12.1` | `glue/values.yaml` → `zeppelin.image` | |
| Reposilite (küme içi Maven önbelleği) | 3.6.4 | Apache-2.0 | `dzikoysk/reposilite:3.6.4` | `glue/values.yaml` → `mavenProxy.image` | kapatmak için `mavenProxy.enabled: false` |
| Trino JDBC sürücüsü (Zeppelin) | 483 | Apache-2.0 | Maven Central | `glue/values.yaml` → `zeppelin.trinoJdbcVersion` | Trino ile birlikte yükseltilir |
| Fluent Bit | 5.1.2 | Apache-2.0 | kurumun web sunucusuna kurulan paket | `agents/fluent-bit/` | küme bileşeni değildir; nginx sunucularındaki ajandır |

---

## 3. İzleme ve yedekleme

İzleme ve ad alanı yedeği OpenShift'in kendi bileşenleriyle yapılır; ürün bunlar için ayrı
bir yazılım kurmaz.

| Bileşen | Sürüm | Lisans | Nereden gelir | Not |
|---|---|---|---|---|
| OpenShift izlemesi (kullanıcı iş yükü izlemesi) | OpenShift'in sürümü | Apache-2.0 | OpenShift'in parçası | ürünün ölçüm toplama ve alarm tanımlarını okur ([40-kurulum-sonrasi](../40-kurulum-sonrasi.md) §6) |
| OADP (OpenShift API for Data Protection) | OpenShift'in sağladığı sürüm | Apache-2.0 | Red Hat operatör kataloğu | isteğe bağlı; ad alanı ve disk yedeği ([50-isletme/yedek-ve-geri-donus.md](../50-isletme/yedek-ve-geri-donus.md)) |

---

## 4. İstemci kütüphaneleri (küme bileşeni değil)

| Bileşen | Sürüm | Lisans | Nereden gelir | Nerede sabitlenir | Not |
|---|---|---|---|---|---|
| PyIceberg | `>=0.10,<0.11` | Apache-2.0 | PyPI | `platform/values/jupyterhub.yaml` | not defterlerine açılışta kurulur |
| trino (Python istemcisi) | `>=0.339,<0.340` | Apache-2.0 | PyPI | `platform/values/jupyterhub.yaml` | not defterlerine açılışta kurulur |
| dbt-trino | 1.10.4 | Apache-2.0 | PyPI | `examples/dbt/` (örnek) | Gold katmanını dbt ile yazmak isteyenler için örnek |

---

## 5. Yönetim makinesindeki araçlar

Bunlar kümeye kurulmaz; kurulum ve işletme komutlarını çalıştıran yönetim makinesinde
bulunur. Sürümleri kurumun kendi standardıdır; listenin tamamı ve hangi adımda gerektikleri
[20-on-kosullar](../20-on-kosullar.md) başındaki tablodadır.

| Araç | Lisans | Ne için |
|---|---|---|
| `oc`, `kubectl` | Apache-2.0 | küme komutları |
| `helm` | Apache-2.0 | değişikliği göndermeden önce kümeye ne gideceğini görmek ([50-isletme/degisiklik-nasil-uygulanir.md](../50-isletme/degisiklik-nasil-uygulanir.md) §3.2) |
| `git` | GPL-2.0 | ürünün Git deposu (yalnız yönetim makinesinde kullanılır, teslim edilmez) |
| `jq`, `python3` | MIT / PSF | betikler ve doğrulama komutları |

---

## 6. Kopya sayıları ve PodDisruptionBudget

Ürün kendisi hiçbir bileşen için `PodDisruptionBudget` yazmaz. Kafka, Kafka Connect ve
PostgreSQL için operatörleri (Strimzi, CloudNativePG) kendi bütçelerini yaratır.

| Bileşen | Kopya sayısı | Nerede değişir | PodDisruptionBudget |
|---|---|---|---|
| Kafka | 3 | `glue/values.yaml` → `kafka.replicas` (`min.insync.replicas: 2`) | Strimzi yönetir |
| PostgreSQL (`polaris-db`, `keycloak-db`, `superset-db`) | 2 | `platform/values/glue.yaml` → `cnpg.*.instances` | CloudNativePG yönetir |
| Kafka Connect | 2 | `glue/values.yaml` → `connect.replicas` | Strimzi yönetir (`maxUnavailable: 1`: düğüm boşaltılırken pod'lar tek tek iner, görevleri diğer pod'a geçer) |
| Trino koordinatörü / çalışanlar | 1 / 2 | `platform/values/trino.yaml` → `server.workers` | yok — koordinatör tektir, çalışanlar durum tutmaz |
| Polaris | 1 | `platform/values/polaris.yaml` | yok — tek kopya |
| Superset | 1 | `glue/values.yaml` → `superset.replicas` | yok — tek kopya |
| JupyterHub hub / Zeppelin | 1 / 1 | `platform/values/jupyterhub.yaml`, `glue/values.yaml` → `zeppelin` | yok — tek kopya |

**Neden tek kopyalı bileşenlere bütçe yok:** tek kopyalı bir bileşene "en az bir tane ayakta
kalsın" demek düğüm boşaltmayı (`oc adm drain`) kalıcı olarak kilitler. Yüksek erişilebilirlik
istenirse önce kopya sayısı artırılır, bütçe kararı o değişiklikle birlikte verilir
([pre-ship-kontrol-listesi.md](pre-ship-kontrol-listesi.md) §5.1).

---

## 7. Lisans özeti

- Kümeye kurulan bütün bileşenler **Apache-2.0**, **BSD-3-Clause** ya da **PostgreSQL
  License** lisanslıdır (izin verici lisanslar).
- Kurulumda **AGPL-3.0** ya da **SSPL** lisanslı bir bileşen yoktur.
- Kaynak kodu teslim edilen her şey (ürünün chart'ı, Spark işleri, belgeler) ürünün Git
  deposundadır. Üçüncü taraf imajlarında değişiklik yapılmaz; türev eser yükümlülüğü doğmaz.

---

## 8. Listeyi güncel tutma

1. Yükseltme [50-isletme/yukseltme.md](../50-isletme/yukseltme.md) sayfasındaki sırayla
   yapılır. **Sürüm değiştiren her değişiklikte bu sayfadaki satır da güncellenir.**
2. Kurulumdan ve her yükseltmeden sonra kümedeki gerçek sürümler doğrulanır:

`[bastion]`

```bash
oc -n "$ARGOCD_NS" get applications -o json | jq -r '.items[] | .metadata.name as $n
  | ([.spec.source] + (.spec.sources // []))[] | select(. != null and .chart != null)
  | "\($n)\t\(.chart)\t\(.targetRevision)"' | column -t
oc -n "$LAKEHOUSE_NS" get kafka lakehouse -o jsonpath='{.spec.kafka.version}{"\n"}'
oc -n "$LAKEHOUSE_NS" get pods -o jsonpath='{range .items[*]}{range .spec.containers[*]}{.image}{"\n"}{end}{end}' \
  | sed 's/@sha256.*//' | sort -u
```

**Beklenen çıktı** (örnek; üçüncü komutun çıktısından seçilmiş satırlar):

```text
cert-manager       cert-manager            v1.21.2
cnpg               cloudnative-pg          0.29.0
cnpg-barman        plugin-barman-cloud     0.8.0
jupyterhub         jupyterhub              4.4.2
polaris            polaris                 1.7.0
spark-operator     spark-operator          2.5.2
strimzi            strimzi-kafka-operator  1.2.0
superset-operator  superset-operator       0.2.0
trino              trino                   1.42.2
4.3.1
apachesuperset.docker.scarf.sh/apache/superset:6.1.0-dev
docker.io/apache/polaris:1.7.0
docker.io/apache/spark:4.1.0-java21-python3
docker.io/apache/zeppelin:0.12.1
docker.io/dzikoysk/reposilite:3.6.4
docker.io/trinodb/trino:483
ghcr.io/cloudnative-pg/postgresql:18.4-system-trixie
quay.io/keycloak/keycloak:26.7.3
quay.io/strimzi/kafka:1.2.0-kafka-4.3.1
```

Git'ten kurulan uygulamaların (`glue`, `custom`, `keycloak-operator`) ilk komutta satırı
yoktur; onların sürümü kümenin eşitlendiği Git commit'idir:
`oc -n "$ARGOCD_NS" get application glue -o jsonpath='{.status.sync.revision}{"\n"}'`.

**Ters giderse:** bir sürüm bu sayfadakiyle uyuşmuyorsa ya küme eski bir commit'ten
eşitlenmiştir ya da bu sayfa güncellenmemiştir. İkisi de J.3.2 açısından düzeltilmesi
gereken bir bulgudur.

---

## Kontrol listesi

- [ ] Tablolardaki sürümler kümedeki gerçek sürümlerle aynı (§8).
- [ ] Kurulumda AGPL-3.0 ya da SSPL lisanslı bir bileşen yok (§7).
- [ ] Sürüm değiştiren her değişiklikte bu sayfa da güncellendi.
- [ ] Hiçbir bileşen için özel imaj üretilmedi (baştaki `grep` denetimi).

## Sonraki bölüm

[kabul-testleri.md](kabul-testleri.md) — şartname maddelerinin karşılığı ve kabul kanıtı.
Portlar ve servisler: [port-ve-servisler.md](port-ve-servisler.md).
