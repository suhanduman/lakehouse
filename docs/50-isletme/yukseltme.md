# Yükseltme

Bu sayfa bir bileşenin sürümünü nasıl yükselteceğinizi anlatır: hangi dosyada hangi
satır değişir, bileşenler hangi sırayla yükseltilir, Kafka neden iki adımda yükseltilir,
Kafka Connect imajının etiketi neden değişmelidir ve bir yükseltme nasıl geri alınır.

**Ne kadar sürer:** tek bir bileşen için 30–60 dakika. Kafka Connect eklentileri
değişiyorsa imaj yeniden üretildiği için 10 dakika daha. Ürünün otomatik testleri
(CI) 40–55 dakika sürer.

**Hangi yetkiler gerekir:** kurumun lakehouse Git deposunda `main` dalına yazma;
`$ARGOCD_NS` ve `$LAKEHOUSE_NS` ad alanlarında okuma. Kümede elle değişiklik yetkisi
gerekmez.

**Komutlar nerede çalışır:** `[bastion]` — `git`, `oc` ve `helm` araçlarının kurulu
olduğu, kümeye bağlı yönetim makinesi.

> **Kümede `helm upgrade` çalıştırılmaz.** Yükseltme de her değişiklik gibi Git'ten
> geçer: dosyada sürümü değiştirin, commit edip gönderin, ArgoCD kümeye uygular
> ([değişiklik nasıl uygulanır](degisiklik-nasil-uygulanir.md)). JupyterHub sürümü ya da
> not defteri imajı değiştiyse ArgoCD uygulamayı bitirdikten sonra hub'ı bir kez yeniden
> başlatın (aynı sayfa §5.2).

---

## 1. Değişkenleri yükleyin

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
yüklenmemiştir ([30-kurulum](../30-kurulum.md) §1).

---

## 2. Başlamadan önce: beş kural

1. **Her değişiklikte tek bir bileşen yükseltin.** Kafka gibi iki adımlı olanlar
   dışında, birden fazla bileşeni aynı anda yükseltirseniz sorun çıktığında hangisinin
   sorun çıkardığını anlayamazsınız.
2. **Önce operatörler, sonra uygulamalar.** Yeni bir operatör eski uygulama
   tanımlarını okuyabilir; tersi her zaman doğru değildir.
3. **Sürüm listesini aynı değişiklikte güncelleyin**
   ([90-referans/surumler-ve-lisanslar.md](../90-referans/surumler-ve-lisanslar.md) §8).
4. **Yedekler sağlıklı olmalı.** Üç veritabanında da sürekli arşivleme `True` olmalı
   ([yedek-ve-geri-donus.md](yedek-ve-geri-donus.md) §4).
5. **Üreticinin sürüm notlarını okuyun** — özellikle Kafka, PostgreSQL operatörü ve
   Superset operatörü için.

Yedeklerin durumunu kontrol edin:

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" get cluster \
  -o custom-columns='DB:.metadata.name,ARSIV:.status.conditions[?(@.type=="ContinuousArchiving")].status'
```

**Beklenen çıktı** (ürünün üç veritabanı; kümenizde başka PostgreSQL kümeleri varsa
onlar da listelenir):

```text
DB            ARSIV
keycloak-db   True
polaris-db    True
superset-db   True
```

**Ters giderse:** bir satır `False` ise yükseltmeye başlamayın; önce arşivlemeyi
düzeltin ([sorun-giderme.md](sorun-giderme.md) §8).

---

## 3. Sıra ve hangi dosya değişir

Bileşenleri bu tablodaki sırayla yükseltin:

| # | Bileşen | Değişecek dosya | Kümede ne olur |
|---|---|---|---|
| 1 | cert-manager | `platform/apps/00-cert-manager.yaml` | Tanımlar güncellenir; sertifikalar yeniden imzalanmaz |
| 2 | Kafka operatörü (Strimzi) | `platform/apps/00-strimzi.yaml` | Kafka ve Connect pod'ları sırayla yeniden başlar |
| 3 | **Kafka** (iki adım) | `glue/values.yaml` → `versions.kafka`, `versions.kafkaMetadata` | Broker'lar ve Connect iki kez sırayla yeniden başlar (§4.2) |
| 4 | PostgreSQL operatörü | `platform/apps/00-cnpg.yaml` | Veritabanı pod'ları sırayla yeniden başlar |
| 5 | PostgreSQL yedek eklentisi | `platform/apps/00-cnpg-barman.yaml` | Veritabanı pod'larındaki yardımcı kap yenilenir |
| 6 | spark-operator | `platform/apps/00-spark-operator.yaml` | Çalışmakta olan Spark işleri etkilenmez |
| 7 | Keycloak operatörü ve Keycloak | `platform/keycloak-operator/kustomization.yaml` | Operatör ve Keycloak yeniden başlar; realm yeniden içe aktarılmaz (§4.6) |
| 8 | Superset operatörü | `platform/apps/00-superset-operator.yaml` | Tanımdaki alan adları değişebilir (§4.5) |
| 9 | Connect eklentileri (Debezium, Iceberg, Hadoop) | `glue/values.yaml` → `versions.*` **ve** `connect.buildImage` etiketi | Connect imajı yeniden üretilir (~10 dakika) |
| 10 | Polaris | `platform/apps/20-polaris.yaml` ve `glue/values.yaml` → `versions.polarisAdminTool` | Polaris yeniden başlar; veritabanı şemasını yönetim aracı günceller |
| 11 | Trino | `platform/values/trino.yaml` (`image.tag`) ve `platform/apps/30-trino.yaml` (chart sürümü) | Koordinatör ve işçiler yeniden başlar |
| 12 | Superset | `glue/values.yaml` → `superset.imageTag` | Veritabanı şema işi yeniden çalışır, web sunucusu yeniden başlar |
| 13 | JupyterHub ve not defteri imajı | `platform/apps/30-jupyterhub.yaml`, `platform/values/jupyterhub.yaml` | Hub yeniden başlar; **açık not defteri oturumları kapanır** |
| 14 | Zeppelin | `glue/values.yaml` → `zeppelin.image` ve JDBC sürümü | Zeppelin yeniden başlar |

**Trino ve JupyterHub chart sürümleri iki yerde yazılıdır:** uygulama dosyasında
(`platform/apps/30-trino.yaml`, `platform/apps/30-jupyterhub.yaml`) ve ürünün otomatik
testlerinde (`.github/workflows/e2e.yaml`). Birini değiştirip diğerini unutursanız
testler kümede çalışmayacak bir sürümü sınar. İkisini aynı değişiklikte güncelleyin.

---

## 4. Adım adım

### 4.1 Operatörler

Önce istediğiniz sürümün yayımlanmış olduğunu doğrulayın. Örnek PostgreSQL
operatörü içindir:

`[bastion]`

```bash
helm show chart cnpg/cloudnative-pg --version 0.30.0 | grep -E '^(version|appVersion)'
```

**Beklenen çıktı** (örnek — sürüm yayımlanmışsa):

```text
version: 0.30.0
appVersion: 1.27.0
```

Sürüm varsa ilgili dosyadaki `targetRevision` satırını değiştirin, commit edip
gönderin. ArgoCD uyguladıktan sonra:

`[bastion]`

```bash
oc -n "$ARGOCD_NS" get applications
oc -n cnpg-system get pods
```

**Beklenen çıktı** (örnek): bütün uygulamalar `Synced Healthy`, yeni operatör pod'u
`Running`.

**Ters giderse:** `helm show chart` sürümü bulamazsa yerel chart listesi eskidir:
`helm repo update` çalıştırıp tekrar deneyin.

### 4.2 Kafka: iki ayrı adım

Kafka'nın **meta veri sürümü geri alınamaz**. Broker sürümüyle meta veri sürümünü aynı
değişiklikte yükseltirseniz geri dönüş yolu kapanır. Bu yüzden iki ayrı değişiklik
yapılır.

**Adım 1 — yalnız broker sürümü.** `glue/values.yaml` dosyasında:

```yaml
versions:
  kafka: "4.4.0"            # yeni sürüm
  kafkaMetadata: "4.3-IV0"  # eski değer kalır
```

Commit edip gönderin. ArgoCD uyguladıktan sonra:

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" get kafka lakehouse -o jsonpath='{.status.conditions}'
oc -n "$LAKEHOUSE_NS" get pods -l strimzi.io/cluster=lakehouse
oc -n "$LAKEHOUSE_NS" get kafkaconnect connect -o jsonpath='{.status.conditions}'
```

**Beklenen çıktı** (örnek): Kafka ve Connect koşullarında `Ready` `True`; bütün
broker pod'ları yeniden başlamış ve `Running`.

Kafka Connect'in sürümü de aynı `versions.kafka` değerinden gelir, bu yüzden bu adımda
birlikte yükseltilir. Connect iki işçiyle çalışır; Strimzi işçileri tek tek yeniden
başlatır ve yeniden başlayan işçinin görevleri birkaç saniye içinde diğer işçiye geçer.
Veri akışı yalnız kısa bir süre duraksar.

**Adım 2 — ayrı bir değişiklikte meta veri sürümü** (geri alınamaz):

```yaml
versions:
  kafkaMetadata: "4.4-IV0"
```

Adım 1'de sorun çıkarsa yalnız `versions.kafka` geri alınır; veri kaybı olmaz.

**Ters giderse:** broker'lar yeniden başlarken takılırsa Adım 2'ye geçmeyin.
`oc -n "$LAKEHOUSE_NS" get pods -l strimzi.io/cluster=lakehouse` bütün pod'ları
`Running` gösterene kadar bekleyin.

### 4.3 Kafka Connect eklentileri: etiketi de değiştirin

Kafka Connect imajı Debezium, Iceberg ve Hadoop eklentilerini içerir ve kümenin iç
imaj deposunda tutulur. İmajın adresi Git'te yazılıdır. Eklenti sürümlerini
değiştirdiğinizde imajın **etiketini de** değiştirin; aksi hâlde Strimzi imajı yeniden
üretmez.

`glue/values.yaml` dosyasında eklenti sürümleri:

```yaml
versions: {debezium: "3.7.0.Final", iceberg: "1.12.0"}
```

`platform/values/site/glue.yaml` dosyasında imaj etiketi (örnekte `2.0.0` → `2.1.0`):

```yaml
connect:
  buildImage: image-registry.openshift-image-registry.svc:5000/lakehouse/connect:2.1.0
```

İkisini aynı commit'te gönderin. İmaj üretimi 10 dakika kadar sürer. Sonra:

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" get kafkaconnect connect -o jsonpath='{.status.conditions}'
oc -n "$LAKEHOUSE_NS" logs -l strimzi.io/kind=KafkaConnect --tail=50
oc -n "$LAKEHOUSE_NS" get kafkaconnector
```

**Beklenen çıktı** (örnek — üçüncü komut; kendi kaynaklarınızın bağlayıcıları
listelenir, hepsinin `READY` sütunu `True`):

```text
NAME         CLUSTER   CONNECTOR CLASS                                      MAX TASKS   READY
dbz-erp      connect   io.debezium.connector.postgresql.PostgresConnector   1           True
sink-erp     connect   org.apache.iceberg.connect.IcebergSinkConnector      1           True
sink-nginx   connect   org.apache.iceberg.connect.IcebergSinkConnector      1           True
```

**Iceberg sürümü Spark işlerini de etkiler.** Spark işleri Iceberg kütüphanelerini küme
içindeki Maven önbelleğinden alır. Sürüm değiştiğinde önbellek yeni sürümü bir sonraki
koşuda bir kez indirir; o koşu her zamankinden 1 dakika kadar uzun sürer. Bu sırada
önbelleğin kaynağına (Maven Central ya da kurumun Maven aynası) erişim olmalıdır
([sorun-giderme.md](sorun-giderme.md) §6.2).

**Kendi Spark işlerinizi yeniden üretin.** Ürün kendi işlerini günceller ama `custom/`
altındaki dosyalarınıza dokunmaz. Spark ya da Iceberg sürümü değiştiyse, eşitleme bittikten
sonra her işinizin tanım dosyasını `scripts/spark-is-tanimi.sh` ile aynı argümanlarla
yeniden üretin ve Git'e gönderin ([yeni-spark-uygulamasi.md](yeni-spark-uygulamasi.md)
§4.1).

**Ters giderse:** etiketi değiştirmediyseniz Strimzi imajı yeniden üretmez ve yeni
eklentiler **sessizce** yüklenmez: bağlayıcılar eski sürümle çalışmaya devam eder.
İmaj üretimi `unauthorized` ile düşüyorsa imajı gönderen hesabın jetonu dolmuştur (§6).

### 4.4 Polaris

Polaris'in chart sürümü ve yönetim aracının sürümü birlikte yükseltilir; veritabanı
şemasını yönetim aracı günceller. Başlamadan önce Polaris veritabanının yedeğinin
sağlıklı olduğunu kontrol edin (§2). ArgoCD uyguladıktan sonra:

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" rollout status deploy/polaris --timeout=600s
oc -n "$LAKEHOUSE_NS" exec deploy/polaris -- curl -sf localhost:8182/q/health | head -3
PATH="$PWD/.venv/bin:$PATH" scripts/polaris-setup.sh --ns "$LAKEHOUSE_NS"
```

**Beklenen çıktı** (ilk iki komut):

```text
deployment "polaris" successfully rolled out
{
    "status": "UP",
    "checks": [
```

Üçüncü komut katalog kurulumunu yeniden uygular. Birden fazla çalıştırılabilir: yalnız
eksik olanı yaratır, var olana dokunmaz ([40-kurulum-sonrasi](../40-kurulum-sonrasi.md)
§2).

**Ters giderse:** `"status": "DOWN"` görürseniz Polaris veritabanına bağlanamıyordur;
`oc -n "$LAKEHOUSE_NS" logs deploy/polaris --tail=50` ile hatayı görün.

### 4.5 Trino, Superset, JupyterHub, Zeppelin

ArgoCD uyguladıktan sonra:

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" rollout status deploy/trino-coordinator --timeout=900s
oc -n "$LAKEHOUSE_NS" get superset superset -o jsonpath='{.status.phase}{"\n"}'
oc -n "$LAKEHOUSE_NS" logs job/superset-migrate --tail=30
oc -n "$LAKEHOUSE_NS" get pods -l component=singleuser-server
oc -n "$LAKEHOUSE_NS" rollout status deploy/zeppelin --timeout=600s
```

**Beklenen çıktı** (örnek): Trino ve Zeppelin `successfully rolled out`, Superset
`Running`, şema işinin günlüğünde hata yok.

Dikkat edilecekler:

- **Trino:** chart sürümü ile imaj etiketini birlikte yükseltin.
- **Superset:** imaj etiketi değişince veritabanı şema işi yeniden çalışır.
- **JupyterHub:** açık not defteri oturumları kapanır; kullanıcıları önceden uyarın.
- **Zeppelin:** yeni JDBC sürücüsü ilk açılışta indirilir (1 dakika kadar) ve diskte
  kalır. Kapalı ağda sürücüye erişim önceden sağlanmalıdır.
- **Superset operatörü:** tanımdaki alan adları sürümler arasında değişebilir.
  Operatörü yükseltmeden önce kümedeki tanımı okuyun ve ürünün şablon testlerini
  çalıştırın:

  `[bastion]`

  ```bash
  oc get crd supersets.superset.apache.org -o jsonpath='{.spec.versions[*].name}{"\n"}'
  oc explain superset.spec --recursive | head -60
  helm unittest glue
  ```

**Superset imajının etiketi.** Üründe kullanılan etiket (`6.1.0-dev`) veritabanı ve
kimlik sürücülerini içerdiği için seçilmiştir; düz sürüm etiketinde bu sürücüler yoktur.
Kurumun imaj aynasında bu etiket **sabit** kalmalıdır: operatör imajı yalnız
`depo:etiket` biçiminde kabul eder, bu yüzden etiketin değişmemesi aynanın
sorumluluğudur
([90-referans/pre-ship-kontrol-listesi.md](../90-referans/pre-ship-kontrol-listesi.md)
madde 4.3). Yeni bir etikete geçmeden önce sürücülerin o etikette de bulunduğunu
doğrulayın:

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" exec deploy/superset-web-server -- \
  python -c "import psycopg2, trino, authlib; print('ok')"
```

**Beklenen çıktı:**

```text
ok
```

**Ters giderse:** `ModuleNotFoundError` görürseniz o etikete geçmeyin; o imajla
Superset'in Trino bağlantısı ve girişi çalışmaz.

### 4.6 Keycloak: realm yeniden içe aktarılmaz

Keycloak'ın sürümünü yükseltmek realm'e dokunmaz. Realm'in **içeriği** değişecekse
[30-kurulum](../30-kurulum.md) §8'deki yordam uygulanır; bu yordam realm'i silip
yeniden içe aktarır ve kullanıcıların arayüzde elle yaptığı ayarlar kaybolur. AD
kullanıcıları yeniden gelir, yine de önce kullanıcı listesinin bir kopyasını alın:

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" exec keycloak-0 -- \
  /opt/keycloak/bin/kcadm.sh get users -r lakehouse --config /tmp/kcadm.config > users.json
```

**Ters giderse:** komut oturum hatası verirse önce yönetici oturumu açın
([30-kurulum](../30-kurulum.md) §8; oradaki `config credentials` komutu da aynı
`--config /tmp/kcadm.config` seçeneğini kullanır).

---

## 5. Bir sürüm etiketine sabitlemek

Uygulamalar varsayılan olarak Git deposunun `main` dalını izler. Kümeyi belirli bir
sürüm etiketine sabitlemek isterseniz önce etiketi yaratın:

`[bastion]`

```bash
git tag -a v2.1.0 -m "sürüm 2.1.0"
git push origin v2.1.0
```

Sonra `platform/root-app.yaml` dosyasını açın ve `targetRevision: main` satırını
`targetRevision: v2.1.0` yapın:

```bash
vi platform/root-app.yaml
git add platform/root-app.yaml
git commit -m "küme v2.1.0 sürümüne sabitlendi"
git push origin main
```

ArgoCD uyguladıktan sonra kümenin hangi commit'te olduğunu görün:

```bash
oc -n "$ARGOCD_NS" get applications -o wide
oc -n "$ARGOCD_NS" get application glue -o jsonpath='{.status.sync.revision}{"\n"}'
```

**Beklenen çıktı** (örnek — son komut; etiketin gösterdiği commit kimliği):

```text
9a13b7c1f4e2d5a8b3c6e9f0a1b2c3d4e5f60718
```

**Ters giderse:** bir uygulama hâlâ `main`'i gösteriyorsa `platform/apps/` altındaki
dosyalardan birinde `targetRevision` satırı değiştirilmemiştir.

---

## 6. Connect imajını gönderen hesabın jetonunu yenilemek

Kafka Connect imajını kümenin iç imaj deposuna gönderen hesap **bir yıllık** bir jeton
kullanır ([20-on-kosullar](../20-on-kosullar.md) madde 5). Jetonun süresi dolduğunda
çalışan Connect etkilenmez, ama yeni imaj üretimi `unauthorized` ile düşer: eklenti
yükseltmesi yapılamaz. Bitiş tarihini kurumun takvimine yazın ve süre dolmadan şu
komutlarla yenileyin:

`[bastion]`

```bash
TOKEN=$(oc -n "$LAKEHOUSE_NS" create token connect-build --duration=8760h)
oc -n "$LAKEHOUSE_NS" delete secret connect-push
oc -n "$LAKEHOUSE_NS" create secret docker-registry connect-push \
  --docker-server="$INTERNAL_REGISTRY" --docker-username=connect-build \
  --docker-password="$TOKEN"
oc -n "$LAKEHOUSE_NS" label secret connect-push lakehouse.io/yonetim=manual-20-on-kosullar-5 --overwrite
python3 -c 'import base64, json, sys, datetime
t = sys.argv[1].split(".")[1]; t += "=" * (-len(t) % 4)
print("jeton bitisi:", datetime.datetime.fromtimestamp(
    json.loads(base64.urlsafe_b64decode(t))["exp"], datetime.timezone.utc))' "$TOKEN"
unset TOKEN
```

**Beklenen çıktı** (örnek — son satırdaki tarih jetonun gerçek bitiş anıdır; küme daha
kısa bir üst sınır uyguluyorsa süre kısalır):

```text
secret "connect-push" deleted from lakehouse namespace
secret/connect-push created
secret/connect-push labeled
jeton bitisi: 2027-09-24 09:12:44+00:00
```

Etiket, Secret'ın elle yaratıldığını ve yordamın hangi belgede olduğunu gösterir
([değişiklik nasıl uygulanır](degisiklik-nasil-uygulanir.md) §7).

**Ters giderse:** jeton yenilendikten sonra imaj üretimi hâlâ `unauthorized` veriyorsa
`connect-build` hesabının gönderme yetkisi kaldırılmış olabilir
([20-on-kosullar](../20-on-kosullar.md) madde 5).

---

## 7. Göndermeden önce: testler

Ürünün Git deposuna yapılan her gönderimde otomatik testler (CI) iki iş çalıştırır:

| İş | Ne sınar |
|---|---|
| `helm-unittest` | şablon testleri, Spark iş kodunun testleri, belge denetimi, site ayarlarının tutarlılığı, şablonların üretim ve geliştirme ayarlarıyla oluşturulması |
| `e2e-kind` | geçici bir kümede kurulumun tamamı ve uçtan uca veri yolları |

Yalnız belge değiştiren gönderimlerde `e2e-kind` atlanır. Belge dışında tek bir dosya
bile değiştiyse iki iş de çalışır.

Göndermeden önce aynı denetimlerin hızlı olanlarını kendi makinenizde çalıştırın:

`[bastion]`

```bash
helm unittest glue
helm template glue ./glue -f platform/values/glue.yaml -f platform/values/site/glue.yaml >/dev/null
bash scripts/check-site.sh
```

**Beklenen çıktı** (ilk komutun özet satırlarından ikisi; ikinci komut bir şey yazmaz;
üçüncü `check-site: OK`):

```text
Charts:      1 passed, 1 total
Test Suites: 16 passed, 16 total
check-site: OK
```

Yükseltilmiş bir kümede kabul testlerini yeniden çalıştırmak için:
[90-referans/kabul-testleri.md](../90-referans/kabul-testleri.md).

**Ters giderse:** `helm unittest` bir testte düşerse yükseltme bir şablonu bozmuştur;
hata satırı hangi testin hangi alanı beklediğini söyler.

---

## 8. Geri alma

| Durum | Ne yapılır |
|---|---|
| Chart ya da imaj sürümü sorunlu | `git revert` ile geri alın ve gönderin; ArgoCD eski sürüme döner |
| ArgoCD uygulaması yarıda kaldı | Uygulamayı yeniden tetikleyin ([değişiklik nasıl uygulanır](degisiklik-nasil-uygulanir.md) §5.1); hâlâ `Degraded` ise uygulamanın durumuna bakın |
| Kafka broker sürümü (§4.2 Adım 1) | Geri alınabilir (meta veri sürümü eski kaldıkça) |
| Kafka meta veri sürümü (§4.2 Adım 2) | **Geri alınamaz** — tek yol kümeyi yeniden kurup verileri kaynaklardan yeniden okumaktır ([yedek-ve-geri-donus.md](yedek-ve-geri-donus.md) §2.1) |
| PostgreSQL şema güncellemesi bozuldu | Yükseltmeden önceki ana dönüş ([yedek-ve-geri-donus.md](yedek-ve-geri-donus.md) §5.2) |
| Superset operatörü tanımı reddediyor | Operatörü eski sürüme geri alın |
| Not defteri diski kayboldu | Disk geri yükleme ([yedek-ve-geri-donus.md](yedek-ve-geri-donus.md) §6.3) |

Geri alma komutları:

`[bastion]`

```bash
git pull --rebase origin main
git revert --no-edit HEAD
git push origin main
```

**Beklenen çıktı** (örnek — ilk satırlar):

```text
[main 3c9f0a2] Revert "sürüm: trino 484"
 2 files changed, 2 insertions(+), 2 deletions(-)
```

Geri aldıktan sonra sürüm listesindeki satırı da eski değere döndürün; liste her zaman
kümedeki gerçek sürümleri göstermelidir
([90-referans/surumler-ve-lisanslar.md](../90-referans/surumler-ve-lisanslar.md) §8).

---

## Kontrol listesi

- [ ] Yedekler sağlıklı (üç veritabanında sürekli arşivleme `True`).
- [ ] Tek bir bileşen yükseltiliyor ve §3'teki sıraya uyuldu.
- [ ] Kafka'da broker sürümü ve meta veri sürümü **ayrı** değişikliklerde yükseltildi.
- [ ] Connect eklentisi değiştiyse `connect.buildImage` etiketi de değişti.
- [ ] Trino ya da JupyterHub chart sürümü hem uygulama dosyasında hem otomatik
      testlerde güncellendi.
- [ ] Sürüm listesi aynı değişiklikte güncellendi.
- [ ] Spark ya da Iceberg sürümü değiştiyse `custom/` altındaki iş tanımları
      `scripts/spark-is-tanimi.sh` ile yeniden üretildi.
- [ ] Otomatik testlerin iki işi de başarılı; ArgoCD `Synced Healthy` gösteriyor.

## Sonraki bölüm

Yükseltmeden sonra ilk bakılacak yer:
[gunluk-haftalik-kontroller.md](gunluk-haftalik-kontroller.md). Alarmlar:
[izleme-ve-alarmlar.md](izleme-ve-alarmlar.md). Sorunlar:
[sorun-giderme.md](sorun-giderme.md).
