# Yükseltme

**Bu bölümde:** bir bileşenin sürümünü yükseltmenin tek yolu — hangi dosyada hangi satırın
değiştiği, bileşenlerin hangi **sırayla** yükseltildiği, Kafka'nın iki adımlı kuralı, Kafka
Connect imajının etiket kuralı, Superset imajının müşteri aynasındaki yeri, Connect itme
jetonunun yenilenmesi, birleştirme öncesi kapı (CI) ve geri alma yolları.
**Süre:** tek bileşen için 30–60 dakika (Connect eklentileri ~10 dakikalık imaj yapımıyla
birlikte daha uzun); CI kapısı 40–55 dakika.
**Gereken yetki:** Git deposunda `main` dalına yazma; `$ARGOCD_NS` ve `$LAKEHOUSE_NS` ad
alanlarında okuma. Kümede elle sürüm değiştirme yetkisi **gerekmez** ve istenmez.
**Nerede çalıştırılır:** `[bastion]` — `git`, `oc` ve `helm` kurulu yönetim makinesi.

> **Kümede `helm upgrade` yoktur.** Yükseltme = **dosyada sürümü değiştir → commit → CI
> yeşil → eşitleme**. Tek istisna ArgoCD'siz kurulmuş geliştirme kümeleridir; orada da aynı
> sıra geçerlidir ([değişiklik nasıl uygulanır](degisiklik-nasil-uygulanir.md) §5.2).

---

## 1. Değişkenleri yükleyin

`[bastion]`

```bash
cd ~/lakehouse
set -a; . install/lakehouse.env; set +a
echo "argocd=$ARGOCD_NS lakehouse=$LAKEHOUSE_NS"
```

**Beklenen çıktı** (örnek — kendi değerlerinizle):

```text
argocd=openshift-gitops lakehouse=lakehouse
```

**Ters giderse:** boş satır görüyorsanız dosya yüklenmemiştir
([30-kurulum](../30-kurulum.md) §1).

---

## 2. Başlamadan önce: beş kural

1. **Tek değişiklik = tek bileşen.** (Kafka gibi iki adımlı olanlar hariç.) Karışık
   yükseltmede hangi bileşenin kırdığı anlaşılamaz ve CI koşusu 40–55 dakika sürer.
2. **Sıra: önce operatörler, sonra uygulamalar.** Operatörün yeni sürümü eski uygulama
   tanımlarını okuyabilir; tersi garanti değildir.
3. **Her yükseltmede sürüm listesi aynı değişiklikte güncellenir**
   ([90-referans/surumler-ve-lisanslar.md](../90-referans/surumler-ve-lisanslar.md) §8).
4. **Yedek durumu yeşil olmalı.** Üç veritabanında da sürekli arşivleme `True`, son yedek
   `completed` ([yedek-ve-geri-donus.md](yedek-ve-geri-donus.md) §4).
5. **Üreticinin sürüm notları okunur** — özellikle Kafka, PostgreSQL operatörü ve Superset
   operatörü için.

Yedek durumunu doğrulayın:

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" get cluster \
  -o custom-columns='DB:.metadata.name,ARSIV:.status.conditions[?(@.type=="ContinuousArchiving")].status'
```

**Beklenen çıktı** (kind kümesinde alınmış gerçek çıktı; `demo-pg` yalnız geliştirme
kümesinin örnek kaynağıdır):

```text
DB            ARSIV
demo-pg       True
keycloak-db   True
polaris-db    True
superset-db   True
```

**Ters giderse:** bir satır `False` ise **yükseltmeyin**; önce arşivi düzeltin
([sorun-giderme.md](sorun-giderme.md) §8).

---

## 3. Sıra ve hangi dosya değişir

| # | Bileşen | Değişecek dosya | Kümedeki etkisi |
|---|---|---|---|
| 1 | cert-manager | `platform/apps/00-cert-manager.yaml` | Tanımlar güncellenir; sertifikalar yeniden imzalanmaz |
| 2 | Kafka operatörü (Strimzi) | `platform/apps/00-strimzi.yaml` | Kafka ve Connect pod'larında sırayla yeniden başlatma |
| 3 | **Kafka** (iki adım) | `glue/values.yaml` → `versions.kafka`, `versions.kafkaMetadata` | Broker ve Connect iki kez sırayla yeniden başlar (§4.2) |
| 4 | PostgreSQL operatörü | `platform/apps/00-cnpg.yaml` | Veritabanı pod'larında sırayla yeniden başlatma |
| 5 | PostgreSQL yedek eklentisi | `platform/apps/00-cnpg-barman.yaml` | Veritabanı pod'larındaki yardımcı kap yenilenir |
| 6 | spark-operator | `platform/apps/00-spark-operator.yaml` | Koşan Spark işleri etkilenmez |
| 7 | Keycloak operatörü + Keycloak | `platform/keycloak-operator/kustomization.yaml` | Operatör + Keycloak yeniden başlar; realm **içe aktarılmaz** (§4.6) |
| 8 | Superset operatörü | `platform/apps/00-superset-operator.yaml` | Alan adları kırılabilir (§4.5) |
| 9 | Connect eklentileri (Debezium, Iceberg, Hadoop) | `glue/values.yaml` → `versions.*` **+ `connect.buildImage` etiketi** | **Yeniden imaj yapımı** (~10 dakika) |
| 10 | Polaris | `platform/apps/20-polaris.yaml` + `glue/values.yaml` → `versions.polarisAdminTool` | Polaris yeniden başlar; şema geçişi yönetim aracıyla yapılır |
| 11 | Trino | `platform/values/trino.yaml` (`image.tag`) + `platform/apps/30-trino.yaml` (chart sürümü) | Koordinatör ve çalışanlar yeniden başlar |
| 12 | Superset | `glue/values.yaml` → `superset.imageTag` | Şema geçiş işi yeniden koşar + web yeniden başlar |
| 13 | JupyterHub + not defteri imajı | `platform/apps/30-jupyterhub.yaml`, `platform/values/jupyterhub.yaml` | Hub yeniden başlar; **koşan not defteri oturumları durur** |
| 14 | Zeppelin | `glue/values.yaml` → `zeppelin.image` ve JDBC sürümü | Zeppelin yeniden başlar |
| 15 | İzleme / yedek (geliştirme yığını) | `platform/apps/dev/*.yaml` | Yalnız geliştirme kümesi; üretimde karşılığı platform izlemesi ve OADP'dir |

**Chart sürümleri iki yerde birden geçer.** Trino ve JupyterHub'ın chart sürümü hem
`platform/apps/30-trino.yaml` / `platform/apps/30-jupyterhub.yaml` dosyasında hem de CI'ın
render adımında yazılıdır (`.github/workflows/e2e.yaml`). Birini değiştirip diğerini
unutursanız CI, kümede koşmayacak bir sürümü doğrular. **İkisini aynı değişiklikte
güncelleyin.**

**Geliştirme yığınının MinIO/`mc` imajları elle derlenir.** Yukarıdaki tablodaki hiçbir satır
bu ikisini kapsamaz, çünkü üretimde kurulmazlar: MinIO yalnız kind/dev kümesinin S3'üdür.
MinIO Inc. topluluk imajlarını geri çektiğinden `glue/templates/minio.yaml` artık bir kayıt
defteri etiketini değil, **kaynaktan derlenmiş kendi AGPL-3.0 aynamızı** gösterir. MinIO ya da
`mc` sürümünü yükseltmek, yeni etiketi şablona yazmadan önce o etiketi derleyip itmeyi
gerektirir: `MINIO_TAG=... MC_TAG=... scripts/build-minio-mirror.sh`. Betik fork'ları etiketten
klonlar, commit'i doğrular ve iki mimarili manifest listesini iter; yükümlülüklerin ayrıntısı
`docs/90-referans/surumler-ve-lisanslar.md` §5'tedir. **Ayna itilmeden şablonu güncellerseniz**
dev kümesi ve CI e2e koşusu `ImagePullBackOff` ile durur.

---

## 4. Adım adım

### 4.1 Operatör chart'ı

`[bastion]`

```bash
helm show chart cnpg/cloudnative-pg --version 0.30.0 | grep -E '^(version|appVersion)'
```

**Beklenen çıktı** (örnek — istenen sürüm gerçekten yayımlanmışsa):

```text
version: 0.30.0
appVersion: 1.27.0
```

Sürüm varsa ilgili dosyadaki `targetRevision` değiştirilir, commit + push edilir; eşitleme
sonrası:

`[bastion]`

```bash
oc -n "$ARGOCD_NS" get applications
oc -n cnpg-system get pods
```

**Beklenen çıktı** (örnek): bütün uygulamalar `Synced Healthy`, yeni operatör pod'u
`Running`.

**Ters giderse:** `helm show chart` sürümü bulamazsa depo dizini eskidir
(`helm repo update`).

### 4.2 Kafka: **iki ayrı adım**

Kafka'nın **meta veri sürümü geri alınamaz**. Broker sürümüyle meta veri sürümü aynı
değişiklikte yükseltilirse geri dönüş yolu kapanır. Bu yüzden iki ayrı değişiklik yapılır:

**Adım 1** — yalnız broker sürümü (`glue/values.yaml`):

```yaml
versions:
  kafka: "4.4.0"            # YENİ
  kafkaMetadata: "4.3-IV0"  # ESKİ kalır
```

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" get kafka lakehouse -o jsonpath='{.status.conditions}'
oc -n "$LAKEHOUSE_NS" get pods -l strimzi.io/cluster=lakehouse
oc -n "$LAKEHOUSE_NS" get kafkaconnect connect -o jsonpath='{.status.conditions}'
```

**Beklenen çıktı** (örnek): Kafka ve Connect koşullarında `Ready` `True`; bütün broker
pod'ları yeniden başlamış ve `Running`.

**Adım 2** — ayrı bir değişiklikte meta veri sürümü (**geri alınamaz**):

```yaml
versions:
  kafkaMetadata: "4.4-IV0"
```

Adım 1'de sorun çıkarsa yalnız `versions.kafka` geri alınır; veri kaybı olmaz. Connect'in
sürümü de aynı `versions.kafka` değerinden geldiği için Adım 1'de birlikte taşınır.

**Ters giderse:** broker'lar sırayla yeniden başlarken takılırsa ikinci adıma **geçmeyin**;
`oc -n "$LAKEHOUSE_NS" get pods -l strimzi.io/cluster=lakehouse` hepsini `Running` gösterene
kadar durun.

### 4.3 Kafka Connect: etiket değişmeden yeni eklenti yüklenmez

Connect imajının adresi Git'te **açıkça** verilir ve kimse onu kendiliğinden değiştirmez.
Debezium/Iceberg/Hadoop sürümüyle birlikte **etiketi de** değiştirin:

```yaml
# glue/values.yaml
versions: {debezium: "3.7.0.Final", iceberg: "1.12.0"}
```

```yaml
# platform/values/site/glue.yaml
connect:
  buildImage: image-registry.openshift-image-registry.svc:5000/lakehouse/connect:2.1.0
```

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" get kafkaconnect connect -o jsonpath='{.status.conditions}'
oc -n "$LAKEHOUSE_NS" logs -l strimzi.io/kind=KafkaConnect --tail=50
oc -n "$LAKEHOUSE_NS" get kafkaconnector
```

**Beklenen çıktı** (kind kümesinde alınmış gerçek çıktı — üçüncü komut, imaj yapımı
bittikten sonra):

```text
NAME         CLUSTER   CONNECTOR CLASS                                      MAX TASKS   READY
dbz-crm      connect   io.debezium.connector.mongodb.MongoDbConnector       1           True
dbz-shop     connect   io.debezium.connector.postgresql.PostgresConnector   1           True
sink-nginx   connect   org.apache.iceberg.connect.IcebergSinkConnector      1           True
sink-shop    connect   org.apache.iceberg.connect.IcebergSinkConnector      1           True
```

Iceberg sürümü hem sink'i hem Spark işlerini besler → Spark işleri bir sonraki koşuda yeni
çalışma zamanını Maven'den çeker. **Kapalı ağda önce iç ayna gerekir**
([sorun-giderme.md](sorun-giderme.md) §6.2).

**Ters giderse:** etiket değişmediyse Strimzi imajı yeniden üretmez ve yeni eklenti
**sessizce** yüklenmez — bağlayıcılar eski sürümle koşmaya devam eder.

### 4.4 Polaris

Chart sürümü ve yönetim aracı sürümü **birlikte** yükseltilir (şema geçişini yönetim aracı
yapar). Önce Polaris veritabanının yedeğini doğrulayın (§2).

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" rollout status deploy/polaris --timeout=600s
oc -n "$LAKEHOUSE_NS" exec deploy/polaris -- curl -sf localhost:8182/q/health | head -c 200
bash scripts/polaris-setup.sh --setup platform/polaris/setup.yaml
```

**Beklenen çıktı** (örnek — dağıtım tamamlandıktan sonra sağlık yanıtının başı):

```text
deployment "polaris" successfully rolled out
{"status":"UP","checks":[
```

Betik **yeniden koşmaya elverişlidir**: yalnız eksik nesneleri yaratır
([40-kurulum-sonrasi](../40-kurulum-sonrasi.md) §2).

### 4.5 Trino, Superset, JupyterHub, Zeppelin

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" rollout status deploy/trino-coordinator --timeout=900s
oc -n "$LAKEHOUSE_NS" get superset superset -o jsonpath='{.status.phase}{"\n"}'
oc -n "$LAKEHOUSE_NS" logs job/superset-migrate --tail=30
oc -n "$LAKEHOUSE_NS" get pods -l component=singleuser-server
oc -n "$LAKEHOUSE_NS" rollout status deploy/zeppelin --timeout=600s
```

**Beklenen çıktı** (örnek): Trino ve Zeppelin dağıtımları tamamlandı, Superset `Running`,
şema geçiş günlüğü hatasız.

Dikkat edilecekler:

- **Trino:** chart sürümü ile imaj etiketi birlikte yükseltilir (değer dosyası chart
  varsayılanını ezer).
- **Superset:** imaj etiketi değişince şema geçiş işi yeniden koşar.
- **JupyterHub:** koşan not defteri oturumları **durur** → kullanıcıları önceden uyarın.
- **Zeppelin:** yeni JDBC sürücüsü ilk açılışta Maven'den iner (~1 dakika) ve diskte kalıcı
  olur; kapalı ağda önceden konmalıdır.
- **Superset operatörü:** alan adları sürümler arasında kırılabilir. Operatör
  yükseltmesinden önce şemayı kümeden okuyun ve şablon testlerini koşturun:

  `[bastion]`

  ```bash
  oc get crd supersets.superset.apache.org -o jsonpath='{.spec.versions[*].name}{"\n"}'
  oc explain superset.spec --recursive | head -60
  helm unittest glue
  ```

**Superset imajının etiketi.** Üründe kullanılan etiket (`6.1.0-dev`) veritabanı ve kimlik
sürücülerini içerdiği için seçilmiştir; düz sürüm etiketinde bu sürücüler **yoktur** ve kap
içine sonradan kurulamaz. Etiket müşteri aynasında **taşınmaz** (sabit) olmalıdır: operatör
sindirim (digest) değeri kabul etmez, yalnız `depo:etiket` alır — dolayısıyla etiketin sabit
kalması aynanın sorumluluğudur
([90-referans/pre-ship-kontrol-listesi.md](../90-referans/pre-ship-kontrol-listesi.md) madde
4.3). Yükseltmeden önce sürücülerin yeni etikette de bulunduğunu doğrulayın:

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" exec deploy/superset-web-server -- \
  python -c "import psycopg2, trino, authlib; print('ok')"
```

**Beklenen çıktı** (örnek):

```text
ok
```

**Ters giderse:** `ModuleNotFoundError` alırsanız etiketi **değiştirmeyin**; o imajla
Superset'in Trino bağlantısı ve girişi çalışmaz.

### 4.6 Keycloak: realm içe aktarımı var olan realm'i güncellemez

Keycloak'ın sürümünü yükseltmek realm'e dokunmaz. Realm **içeriği** değişecekse
[30-kurulum](../30-kurulum.md) "Realm içeriğini sonradan değiştirmek" bölümündeki
sil-yeniden-içe-aktar yordamı uygulanır ve **kullanıcıların arayüzde elle yaptığı her şey
gider**. AD federasyonu varsa kullanıcılar yeniden akar; yine de önce bir kopya alın:

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" exec keycloak-0 -- \
  /opt/keycloak/bin/kcadm.sh get users -r lakehouse > users.json
```

**Ters giderse:** komut oturum hatası verirse önce yönetici oturumu açılmalıdır
([30-kurulum](../30-kurulum.md) "Realm içeriğini sonradan değiştirmek").

---

## 5. Sürüm etiketine sabitleme

Uygulamalar varsayılan olarak `main` dalını izler. Bir sürüm etiketine sabitlemek
isterseniz:

`[bastion]`

```bash
git tag -a v2.1.0 -m "surum 2.1.0" && git push origin v2.1.0
${EDITOR:-vi} platform/root-app.yaml       # targetRevision: main -> v2.1.0
oc -n "$ARGOCD_NS" get applications -o wide
oc -n "$ARGOCD_NS" get app glue -o jsonpath='{.status.sync.revision}{"\n"}'
```

**Beklenen çıktı** (örnek — son komut, beklenen commit kimliği):

```text
9a13b7c1f4e2d5a8b3c6e9f0a1b2c3d4e5f60718
```

**Ters giderse:** bir uygulama hâlâ `main` gösteriyorsa `platform/apps/*.yaml`
dosyalarındaki `targetRevision` satırlarından biri atlanmıştır.

---

## 6. Connect itme jetonunun yenilenmesi

Connect imajını iç kayıt defterine iten hesap **bir yıllık** bir jeton kullanır
([20-on-kosullar](../20-on-kosullar.md) madde 5). Jeton dolduğunda **çalışan Connect
etkilenmez**, ama yeni bir imaj yapımı `unauthorized` ile düşer — yani **yükseltme durur**.
Bitiş tarihi kurumun takvimine yazılır ve jeton aynı komutlarla yenilenir:

`[bastion]`

```bash
TOKEN=$(oc -n "$LAKEHOUSE_NS" create token connect-build --duration=8760h)
oc -n "$LAKEHOUSE_NS" delete secret connect-push
oc -n "$LAKEHOUSE_NS" create secret docker-registry connect-push \
  --docker-server="$INTERNAL_REGISTRY" --docker-username=connect-build \
  --docker-password="$TOKEN"
python3 -c 'import base64, json, sys, datetime
t = sys.argv[1].split(".")[1]; t += "=" * (-len(t) % 4)
print("jeton bitisi:", datetime.datetime.fromtimestamp(
    json.loads(base64.urlsafe_b64decode(t))["exp"], datetime.timezone.utc))' "$TOKEN"
unset TOKEN
```

**Beklenen çıktı** (örnek — son satırdaki tarih jetonun **gerçek** bitiş anıdır; küme daha
kısa bir üst sınır uyguluyorsa süre kısalır):

```text
secret "connect-push" deleted
secret/connect-push created
jeton bitisi: 2027-09-24 09:12:44+00:00
```

**Ters giderse:** jeton yenilendikten sonra imaj yapımı hâlâ `unauthorized` veriyorsa
`connect-build` hesabının itme rolü kaldırılmış olabilir
([20-on-kosullar](../20-on-kosullar.md) madde 5).

---

## 7. Birleştirme öncesi kapı (CI)

Depoya yapılan her itişte `.github/workflows/e2e.yaml` iki iş koşturur:

| İş | Ne doğrular |
|---|---|
| `helm-unittest` | Şablon birim testleri, Spark iş kodunun sözdizimi ve birim testleri, belge kapısı, site tutarlılığı (`scripts/check-site.sh`), üretim ve geliştirme render'ları, Trino ve JupyterHub render'ları |
| `e2e-kind` | Geçici bir kümede tam kurulum ve dokuz yolun tamamı |

**Yalnız belge değiştiren itişlerde `e2e-kind` atlanır** (bir saatlik koşu hiçbir kod yolunu
kanıtlamaz); belge kapısı zaten `helm-unittest` içindedir. Değişiklikte belge dışında tek bir
dosya bile varsa uzun koşu çalışır.

Yerel ön kontrol — itmeden önce:

`[bastion]`

```bash
helm unittest glue
helm template glue -f platform/values/glue.yaml -f platform/values/site/glue.yaml >/dev/null
bash scripts/check-site.sh
```

**Beklenen çıktı** (örnek — ilk komutun özet satırları; ikinci komut sessizdir):

```text
Charts:      1 passed, 1 total
Test Suites: 13 passed, 13 total
```

Kurulu bir kümede yükseltme sonrası kabul kanıtı: `scripts/acceptance.sh`
([90-referans/kabul-testleri.md](../90-referans/kabul-testleri.md)).

**Ters giderse:** `helm unittest` düşerse yükseltme bir şablon alanını kırmıştır; hata satırı
hangi testin hangi alanı beklediğini söyler.

---

## 8. Geri alma

| Durum | Yol |
|---|---|
| Chart ya da imaj sürümü kötü | `git revert` → push → eşitleme eski sürüme döner |
| Eşitleme yarıda kaldı | Eşitlemeyi yeniden tetikleyin; hâlâ `Degraded` ise uygulamanın koşullarına bakın |
| Kafka broker sürümü (adım 1) | Geri alınabilir (meta veri sürümü eski kaldığı sürece) |
| Kafka meta veri sürümü (adım 2) | **GERİ ALINAMAZ** — dönüş yolu kümeyi yeniden kurup konuları kaynaklardan üretmektir ([yedek-ve-geri-donus.md](yedek-ve-geri-donus.md) §2.1) |
| PostgreSQL şema geçişi bozuldu | Zaman noktasına dönüş: yükseltmeden önceki ana geri yükleme ([yedek-ve-geri-donus.md](yedek-ve-geri-donus.md) §5.2) |
| Superset operatörü tanımı reddediyor | Operatörü geri alın (eski tanımla uyumlu kalır) |
| Not defteri ya da disk kaybı | Disk geri yükleme ([yedek-ve-geri-donus.md](yedek-ve-geri-donus.md) §6.3) |

`[bastion]`

```bash
git pull --rebase origin main
git revert --no-edit HEAD
git push origin main
```

**Beklenen çıktı** (örnek):

```text
[main 3c9f0a2] Revert "surum: trino 484"
 2 files changed, 2 insertions(+), 2 deletions(-)
```

Geri aldıktan sonra sürüm listesindeki satırı da eski değere döndürün — liste her zaman
kümedeki gerçeği göstermelidir
([90-referans/surumler-ve-lisanslar.md](../90-referans/surumler-ve-lisanslar.md) §8).

---

## Kontrol listesi

- [ ] Yedek durumu yeşil (üç veritabanında sürekli arşivleme `True`).
- [ ] Tek bileşen yükseltiliyor; sıra tablosundaki yerine uyuldu.
- [ ] Kafka'da broker sürümü ve meta veri sürümü **ayrı** değişikliklerde.
- [ ] Connect eklentisi değiştiyse `connect.buildImage` etiketi de değişti.
- [ ] Trino/JupyterHub chart sürümü hem uygulama dosyasında hem CI render adımında
      güncellendi.
- [ ] Sürüm listesi aynı değişiklikte güncellendi.
- [ ] CI'ın iki işi de yeşil; ardından eşitleme `Synced Healthy`.
- [ ] Yükseltmeden sonra kabul koşusu yapıldıysa zamanlanmış Spark işleri geri açıldı.

## Sonraki bölüm

Yükseltme sonrası ilk bakılacak yer:
[gunluk-haftalik-kontroller.md](gunluk-haftalik-kontroller.md). Alarm eşikleri:
[izleme-ve-alarmlar.md](izleme-ve-alarmlar.md). Hata belirtileri:
[sorun-giderme.md](sorun-giderme.md).
