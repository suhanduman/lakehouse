# Helm Kurulum — Eğitim Eki

**Kapsam:** Lakehouse platformunun Helm-tabanlı kurulumu (alt-proje C —
"Helm Paketleme"). Bu doküman `docs/egitim/ingest-omurgasi-egitim.md` ve
`docs/egitim/console-egitim-eki.md`'nin **kurulum-mekaniği eki**dir: o iki
doküman "her bileşen ne yapar, nasıl doğrulanır" sorusunu cevaplar; bu
doküman "chart nasıl kurulur/yükseltilir/geri alınır ve neden bu sırada"
sorusunu cevaplar. Spec: `docs/superpowers/specs/2026-07-18-helm-packaging-design.md`.
Kök `README.md`'deki "HELM İLE KURULUM" bölümünün eğitim/derinlemesine
versiyonudur — komutlar birebir tutarlıdır.

---

## 1. Chart yapısı

```
chart/
├── Chart.yaml                 # name: lakehouse; version (platform semver); appVersion
├── values.yaml                # varsayılanlar + component.<x>.enabled + tüm config
├── values-prod.example.yaml   # ortam örneği (domain, registry, secret modu…)
├── templates/
│   ├── _helpers.tpl           # TEK kaynak: isim/label/host/servis-DNS helper'ları
│   ├── _prereq-check.tpl      # operatör-CRD ön-koşul kontrolü (template-time)
│   ├── prereq-check-job.yaml  # aynı kontrolün pre-install/pre-upgrade hook Job hali
│   ├── 00-namespace.yaml … 14-nginx-ingest.yaml   # bileşen template'leri
│   ├── console/                # Console + Kafka UI + routes-oidc (alt-proje B)
│   └── NOTES.txt               # kurulum sonrası çıktı (route'lar + doğrulama)
├── scripts/helm-check.py      # render → parse + no-{{}} + ns + servis-kapalılık kontrolü
└── tests/                      # helm unittest (*_test.yaml) + integrity.sh (tam kapalılık)
```

Tek chart — subchart YOK (tek platform; subchart'ın bağımsız-versiyonlama
faydası burada gerekmiyor). Her bileşen `values.yaml`'da bir `enabled` flag
altında (`components.<x>`, `connectors.examples.enabled`, `components.ai`
vb.) — `false` yaparsanız o bileşenin TÜM kaynakları render'dan çıkar.

**`_helpers.tpl` neden kritik:** her kanonik servis adı (`lakehouse.svc.*`)
yalnızca burada tanımlıdır — hem o servisi **üreten** template (ör.
`03-kafka-strimzi.yaml`'daki `Kafka` CR'ının Strimzi'nin ürettiği
`kafka-kafka-bootstrap` Service'i) hem onu **tüketen** her template (ör.
`12-kafka-connect.yaml`'daki `bootstrapServers`) **aynı**
`lakehouse.svc.kafkaBootstrap` helper'ını çağırır. Böylece bir gün bootstrap
adı değişirse tek yerden değişir ve dangling referans **yapısal olarak**
imkânsız hale gelir (bkz. §5 "servis-bütünlük").

---

## 2. values — ortam farkı tek dosyada

`values.yaml` (varsayılanlar) hiçbir zaman elle düzenlenmez; ortam-özel
her fark bir `values-<ortam>.yaml` dosyasında `--set`/`-f` ile üzerine
yazılır:

```bash
cp chart/values-prod.example.yaml chart/values-staging.yaml
# domain, imageRegistry, replica sayıları, secrets.mode, component
# enabled flag'lerini ortamınıza göre düzenleyin
helm install lakehouse ./chart -n staging -f chart/values-staging.yaml
```

Önemli anahtarlar:
- `namespace` (varsayılan `example`) — yalnızca **bilgi/dokümantasyon**
  amaçlıdır; template'ler gerçek namespace'i HER ZAMAN `.Release.Namespace`
  üzerinden (yani `helm install -n <ns>` argümanından) alır, bu değerden
  DEĞİL (bkz. §4).
- `global.domain`, `global.imageRegistry` — route host'ları ve internal
  image referanslarının tek kaynağı.
- `secrets.mode: external|manual` — bkz. kök README "Ön koşullar / Secret'lar".
- `components.*` — her bileşen için `true`/`false`; `connectors.examples.enabled`
  (varsayılan `false`) örnek connector'ları açar/kapatır; `components.ai`/
  `.superset`/`.jupyter` varsayılan `false` — bunlar bu chart'ın henüz
  Deployment/Service'ini şu an sağlamadığı, yalnızca Route iskeletinin
  hazır olduğu takip-bileşenleridir (`true` yapmayın, backing workload
  yok — Route 503 döner).
- `versions.*` — Debezium/Iceberg/Apicurio/Aiven-JDBC/Kafka sürüm pin'leri
  **tek yerde**.

---

## 3. `helm install` / `upgrade` / `rollback`

```bash
helm install lakehouse ./chart -n example -f chart/values-prod.example.yaml --create-namespace
helm upgrade  lakehouse ./chart -n example -f chart/values-prod.example.yaml
helm rollback lakehouse <revision> -n example
helm history  lakehouse -n example
```

**Rollback tek komuttur** (`helm rollback`) — release'in TAMAMI tek çağrıyla
önceki revizyona döner; her servisi `oc rollout undo` ile tek tek geri almaya
veya numaralı YAML'ları `oc delete -f` ile silmeye gerek yoktur (bkz.
`tools/README.md`'nin "Geri alma" bölümü, manuel adım-adım referansı için).

**Stateful uyarı:** CNPG/Kafka gibi operatör-yönetimli, veri taşıyan
kaynaklarda `helm rollback` yalnızca CR **spec**'ini eski haline döndürür —
veri migrasyonu/uyumluluğu operatörün sorumluluğundadır, Helm bunu
otomatik yönetmez (bkz. spec §7, §11 madde 5 — POC-DOĞRULA).

---

## 4. Namespace-farkındalık

**Kritik tasarım kararı (spec §3.1):** hiçbir template'te sabit `example`
string'i YOK. Her `metadata.namespace` ve her namespace'e bağlı referans
`{{ include "lakehouse.namespace" . }}` (= `.Release.Namespace`) kullanır.
Bunun pratik sonucu: **aynı chart, aynı `-f` dosyasıyla, farklı `-n <ns>`
argümanlarıyla farklı namespace'lere kurulabilir** — hiçbir template
düzenlemesi gerekmez. Bu, chart'ın kendi test döngüsünün de temelidir:
`chart/tests/integrity.sh` tam chart'ı hem `-n example` hem `-n
lakehouse-test` render eder ve ikisinde de (a) tüm namespace'lerin release
namespace'iyle eşleştiğini, (b) render'a başka bir namespace adının
(`example`) sabit sızmadığını doğrular.

---

## 5. Sıralama mantığı — neden-önce-sonra (operatör-reconcile)

Chart **Helm hook'ları ile** kritik sırayı zorlamaz (bilinçli tercih —
bkz. `operators/README.md` "Kurulum sıralaması (chart içi kaynaklar)"):
CNPG `Cluster`, Strimzi `Kafka`/`KafkaConnect`, Keycloak gibi kalıcı,
operatör-yönetimli kaynakları Helm hook'una çevirmek onları normal
`helm upgrade` diff/patch ve `helm rollback` semantiğinin **dışına**
çıkarır (hook-delete-policy'ye göre her upgrade'de silinip yeniden
yaratılabilirler) — kalıcı platform kaynakları için bir anti-pattern.
Chart'taki **tek gerçek hook**, tek-seferlik `prereq-check-job.yaml`'dır
(`pre-install`/`pre-upgrade` — kısa ömürlü bir doğrulama için hook doğru
kullanımdır).

Sıra bunun yerine **üç mekanizmadan kendiliğinden oturur**:

1. **Helm'in native kind sıralaması** — Namespace/ResourceQuota/
   LimitRange/NetworkPolicy/StorageClass/ServiceAccount/Secret/ConfigMap →
   Service → Deployment → Job, her release'de otomatik uygulanır.
2. **Operatör-reconcile** — Strimzi/CNPG/Keycloak/Spark operatörleri kendi
   CR'larını asenkron sağlar (kendi Service/Secret'ları dahil).
3. **Pod readiness + restart** — tüketiciler (Nessie ← `pg-nessie`,
   Kafka Connect ← Kafka + Apicurio) bağımlılıkları hazır olana kadar
   readinessProbe ile bekler / gerekirse yeniden başlar.

Mantıksal kademe (bilgi amaçlı, `chart/templates/NOTES.txt`'in
"INSTALL ORDERING" bölümüyle aynı):

```
1. namespace / quota / limits / NetworkPolicy / StorageClass    (temel)
2. CNPG Postgres cluster'ları + Kafka omurgası + Apicurio         (stateful)
3. Nessie + Trino + Kafka Connect + Spark + Keycloak uygulaması   (2'ye bağımlı)
4. örnek connector'lar + Console + Kafka UI + Route'lar           (en dış)
```

**İlk dakikalarda geçici `CrashLoopBackOff` BEKLENİR** (bağımlılık henüz
hazır değil, ör. Kafka Connect Kafka broker'ları ayağa kalkmadan önce
dener) — bu bir hata değildir. Yakınsamayı izleyin:

```bash
watch oc -n example get pods,cluster,kafka,kafkaconnect
```

> **Açık POC-DOĞRULA:** bu üç mekanizmanın (özellikle Connect ← Kafka +
> Apicurio gibi sert veri-bağımlılıklarında) her zaman yeterli olup
> olmadığı gerçek kümede doğrulanacak; yetersiz kalırsa küçük bir `wait`
> init-container/Job eklenir (yine hook değil, normal kaynak olarak).

---

## 6. Servis-bütünlük (helm-check)

Bu, chart'ın **kabul kriteridir** (spec §3.1/§8), her kurulumdan/upgrade'den
önce çalıştırılabilir statik bir güvencedir:

```bash
helm template chart/ -f chart/values-prod.example.yaml -n example \
  | python3 chart/scripts/helm-check.py - --release-namespace example \
      --no-stray example --service-closure
```

`helm-check.py` şunu doğrular:
- **(a) Namespace tutarlılığı:** render'daki HER `metadata.namespace`,
  `-n`'e verdiğiniz release namespace'e eşit.
- **(b) Sabit-namespace sızıntısı yok:** `--no-stray example` verildiğinde,
  başka bir namespace'e (`-n lakehouse-test` gibi) render edilmiş çıktıda
  hiçbir yerde sabit `example` string'i kalmamış olmalı (değer olarak
  bilerek seçilmediyse).
- **(c) Servis-referans grafiği KAPALI:** render'daki her tüketici
  referansı (Kafka bootstrap sunucuları, `*_URL`/host alanları,
  `.svc.cluster.local` FQDN'ler, Route `spec.to.name`, NetworkPolicy
  `podSelector` hedefleri) bu AYNI render'ın ürettiği bir Service/pod-etiket
  kümesiyle eşleşir — üretilen-Service kümesi ⊇ tüketilen-referans kümesi,
  yani **dangling referans yok**.
- **(d) `{{ }}` kalmamış:** render'da hiç işlenmemiş Helm template sözdizimi
  kalmadığı doğrulanır.

`chart/tests/integrity.sh` bu kontrolü tam chart üzerinde **iki
namespace'te** (`example`, `lakehouse-test`) ve birkaç toggle kombinasyonunda
(`connectors.examples.enabled=true/false`) otomatik çalıştırır — "test'in
testi" olarak, script bilerek dangling bir referans ekleyip fail ettiğini,
sonra düzeltip PASS ettiğini de doğrular.

**Neden önemli:** "operatörler + secret'lar hazırsa, `helm install` seçilen
namespace'e kurulunca her bileşen birbirinin Service'ini doğru görür ve
çalışır" beklentisinin **statik güvencesidir** — `[KÜME]` runtime testleri
(ingest/Console eğitim dokümanlarındaki `oc exec`/`trino --execute`
komutları) bunu **tamamlar**, yerini almaz.

`tools/validate.sh --helm` bu kontrolün tek-namespace/varsayılan-
toggle sürümünü çalıştırır (CI/hızlı-döngü için); `helm lint chart/` ile
birlikte her migration adımından sonra çalıştırılması beklenen minimum
test döngüsüdür:

```bash
helm lint chart/
bash tools/validate.sh --helm example chart/values-prod.example.yaml
bash chart/tests/integrity.sh
```

---

## 7. Sık sorulan sorular

**S: `values.yaml`'ı mı yoksa `values-prod.example.yaml`'ı mı düzenlemeliyim?**
Hiçbirini elle "canlı" olarak düzenlemeyin — `values.yaml` varsayılanları,
`values-prod.example.yaml` bir ÖRNEKtir. Kendi ortamınız için onu kopyalayıp
(`cp chart/values-prod.example.yaml chart/values-<ortam>.yaml`) o kopyayı
düzenleyin ve `-f` ile verin.

**S: Yeni bir kaynak/connector eklemek için chart'ı mı düzenlerim?**
Hayır — CLI'dan `tools/templates/scaffold-source.sh` veya GUI'den
Console (`docs/egitim/console-egitim-eki.md`) kullanın; bunlar chart'ın
**dışında**, Kubernetes API'sine doğrudan yazan self-servis yollardır.
Chart'ı yalnızca **kalıcı/örnek** connector tanımlarını (`connectors.*`
values, `chart/templates/13-connectors.yaml`) değiştirmek istediğinizde
düzenleyip `helm upgrade` ile uygularsınız.

**S: `legacy-manifests/`'teki bir dosyayı `oc apply` ile uygulayabilir miyim?**
Hayır. Helm'in yönettiği bir namespace'e karşı bunu yapmak kaynak
çakışması/drift oluşturur. `legacy-manifests/` yalnızca referans/tarihsel
amaçlıdır (bkz. `legacy-manifests/README.md`); düzenlemek gerekiyorsa
`chart/templates/`'teki karşılığını düzenleyip `helm upgrade` kullanın.

**S: `helm install --dry-run=server` kullanabilir miyim?**
Kullanabilirsiniz ama operatör CRD'lerinin kümede kurulu olmasını
gerektirir (`--dry-run=server` sunucu tarafında şema doğrular) — bu depo
CI'ında/kümesiz ortamda CRD yoktur, bu yüzden `helm template` (client-side
render, CRD gerektirmez) + `helm-check.py` kullanılır. Gerçek kümede
`--dry-run=server` ek bir doğrulama katmanı olarak POC-DOĞRULA listesindedir.
