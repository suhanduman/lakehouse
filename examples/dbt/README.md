# dbt referans örneği (Gold katmanı) — kurulumun parçası DEĞİL

Şartname §14 kararı: **dbt bir chart bileşeni değildir**; Gold modelleri isteyen ekipler için burada çalışan
bir referans proje + CronJob örneği bulunur. Kopyalayın, kendi modellerinizle genişletin.

- `dbt_project.yml` — proje tanımı (`+materialized: table` → Iceberg tablosu)
- `profiles.yml` — Trino bağlantısı (servis hesabı, HTTPS 8443, `lakehouse-ca`)
- `models/gold/orders_daily.sql` — `shop.orders`'tan günlük özet
- `cronjob.yaml` — `python:3.13-slim` + `pip install dbt-trino==1.10.4` + `dbt run`

`lakehouse-ca` Secret'ından pod'a **yalnız `tls.crt`** mount edilir (`volumes[].secret.items`); CA'nın özel
anahtarı (`tls.key`) hiçbir iş yüküne verilmez — chart'taki Superset/Zeppelin mount'ları da aynı kalıptadır.

## Neden özel imaj yok

**Resmi bir `dbt-trino` imajı yoktur** ve bu kurulumun global kuralı "özel/baked imaj yok"tur. Bu yüzden örnek
CronJob resmi `python:3.13-slim` imajını kullanır ve `dbt-trino` her koşuda `pip` ile kurulur:

- **PyPI erişimi gerekir** (kapalı ağda iç PyPI aynası; `pip install -i https://nexus…/simple`).
- Kurulum her koşuda ~20–40 sn ekler; kabul edilebilir bir maliyettir (günlük iş).
- Sürümü **sabitleyin** (`dbt-trino==1.10.4`) — `latest` kurmak sessiz kırılma demektir
  (`docs/90-referans/surumler-ve-lisanslar.md`).

Bunu kalıcı bir üretim bileşeni yapacaksanız doğru yol, müşterinin kendi imaj deposunda dbt imajını üretip
`image:` alanını oraya çevirmektir (bu repo öyle bir imaj üretmez).

## Kurulum adımları (bir kez)

### 1. Trino servis hesabı `dbt`

`password.db` bcrypt htpasswd dosyasıdır ve `trino-service-accounts` Secret'ından gelir
(`docs/30-kurulum.md` §5.7). Mevcut hesaplara `dbt`'yi ekleyin:

```bash
DBT_PW="$(openssl rand -hex 16)"
kubectl -n lakehouse get secret trino-service-accounts -o jsonpath='{.data.password\.db}' | base64 -d > password.db
htpasswd -nbBC 10 dbt "$DBT_PW" >> password.db
kubectl -n lakehouse create secret generic trino-service-accounts \
  --from-file=password.db \
  --from-literal=superset="$(kubectl -n lakehouse get secret trino-service-accounts -o jsonpath='{.data.superset}' | base64 -d)" \
  --from-literal=zeppelin="$(kubectl -n lakehouse get secret trino-service-accounts -o jsonpath='{.data.zeppelin}' | base64 -d)" \
  --dry-run=client -o yaml | kubectl apply -f -
rm -f password.db
kubectl -n lakehouse rollout restart deploy/trino-coordinator      # password.db mount'u yenilensin

# dbt'nin kendi Secret'ı (CronJob bunu okur)
kubectl -n lakehouse create secret generic dbt-trino \
  --from-literal=username=dbt --from-literal=password="$DBT_PW"
```
Dev/kind'da `components.devSecrets=true` iken `trino-service-accounts` glue tarafından üretilir ve `dbt`
hesabı **yoktur** — dev'de denemek için yukarıdaki adımı elle uygulayın (glue sync'i Secret'ı geri yazar;
kalıcı dev kullanımı için `glue/templates/dev-secrets.yaml` genişletilmelidir).

### 2. Trino yetkileri (`rules.json`)

Servis hesapları varsayılan olarak **yalnız okuyabilir**. `dbt`'nin `gold` şemasına yazabilmesi için
`platform/values/trino.yaml` → `accessControl.rules` içine **genel servis hesabı kuralından ÖNCE** ekleyin
(ilk eşleşen kural kazanır — `docs/50-isletme/kullanici-ve-yetki.md` §5):

```json
"catalogs": [
  {"user": "dbt", "catalog": "lakehouse|system", "allow": "all"},
  … mevcut kurallar …
],
"schemas": [
  {"user": "dbt", "schema": "gold", "owner": true},
  … mevcut kurallar …
],
"tables": [
  {"user": "dbt", "schema": "gold", "privileges": ["SELECT", "INSERT", "DELETE", "UPDATE", "OWNERSHIP"]},
  {"user": "dbt", "privileges": ["SELECT"]},
  … mevcut kurallar …
]
```
ConfigMap değişikliği pod'a yayıldıktan sonra `refreshPeriod: 60s` ile okunur (≤ 2 dk, restart gerekmez).

### 3. Polaris `gold` namespace'i ve yazma yetkisi

Trino, Polaris'e **`trino` principal'ı** ile bağlanır; bu principal `readers` + `sandbox_writers` rollerine
sahiptir ve yazma yetkisi **yalnız `sandbox` namespace'inde** tanımlıdır. `gold` için ikisi de gerekir:

1. `platform/polaris/setup.yaml` → `namespaces:` listesine `- name: gold` ekleyin ve
   `scripts/polaris-setup.sh --setup platform/polaris/setup.yaml` çalıştırın (idempotent; yalnız
   EKSİK nesneleri yaratır).
2. Katalog rolüne `gold` ayrıcalıklarını verin: `setup.yaml` → `lakehouse_sandbox` rolünün
   `privileges.namespace` haritasına `gold` ekleyip aynı script'i tekrar koşturmak **hem yeni hem de var olan**
   kurulumda yeter. `polaris setup apply` var olan bir katalog rolünü yeniden YARATMAZ ("Skipping creation for
   already existing catalog role") ama ayrıcalık grant'larını o rol için yine de uygular — CLI 1.7.0 kaynağında
   doğrulandı (apache_polaris paketi, cli/command/setup.py, `_create_catalog_roles`: "Grant privileges" bloğu
   rolün var olup olmamasından bağımsız koşar; grant'lar idempotenttir).
   Tek seferlik/ad-hoc bir yetkilendirme için aynı işi CLI de yapar:
   ```bash
   for p in NAMESPACE_READ_PROPERTIES TABLE_LIST TABLE_CREATE TABLE_DROP \
            TABLE_READ_PROPERTIES TABLE_WRITE_PROPERTIES TABLE_READ_DATA TABLE_WRITE_DATA; do
     polaris privileges namespace grant "$p" --namespace gold --catalog lakehouse --catalog-role lakehouse_sandbox
   done
   polaris privileges list --catalog lakehouse --catalog-role lakehouse_sandbox    # doğrulama
   ```
   (`polaris` CLI'si `polaris-setup.sh`'in kullandığı port-forward'ı gerektirir:
   `kubectl -n lakehouse port-forward svc/polaris 8181:8181` + `CLIENT_ID`/`CLIENT_SECRET` env'leri.)

   Ayrım isteniyorsa `lakehouse_sandbox` yerine ayrı bir katalog rolü (`lakehouse_gold`) + ayrı bir principal
   açılabilir; o durumda Trino'nun katalog kimliği de değişir (tek katalog dosyası, tek principal — bu
   kurulumda ayrı principal ek karmaşıklıktır).

### 4. Proje ConfigMap'i ve CronJob

```bash
kubectl -n lakehouse create configmap dbt-project \
  --from-file=dbt_project.yml=examples/dbt/dbt_project.yml \
  --from-file=profiles.yml=examples/dbt/profiles.yml \
  --from-file=orders_daily.sql=examples/dbt/models/gold/orders_daily.sql \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -f examples/dbt/cronjob.yaml
```
ConfigMap anahtarları **düzdür** (alt dizin taşımaz); CronJob başlangıçta `models/gold/` düzenini
`/tmp/project` altında kurar (`cronjob.yaml` `args`).

### 5. Elle bir koşu + doğrulama

```bash
kubectl -n lakehouse create job dbt-gold-manual --from=cronjob/dbt-gold
kubectl -n lakehouse logs job/dbt-gold-manual -f      # "Completed successfully" beklenir
# Sonuç (Trino CLI resmi imajda mevcuttur; parola TRINO_PASSWORD env'inden okunur -> istem çıkmaz):
DBT_PW=$(kubectl -n lakehouse get secret dbt-trino -o jsonpath='{.data.password}' | base64 -d)
kubectl -n lakehouse exec deploy/trino-coordinator -- env TRINO_PASSWORD="$DBT_PW" \
  trino --server https://localhost:8443 --insecure --user dbt --password \
        --execute "select * from lakehouse.gold.orders_daily order by gun"
```
Beklenen (e2e fixture verisiyle): `shop.orders`'ın günlük/durum bazlı özeti, ör.
`2026-09-18 | new | 2 | 13.50`.

## Sınırlar ve notlar

- **e2e'ye dâhil değildir**: `test/e2e/run.sh` ve `scripts/acceptance.sh` bu CronJob'u koşturmaz.
- **Satır filtresi / kolon maskesi UYGULANMAZ**: dbt bir servis hesabıdır (HTTP Basic). Kullanıcı bazlı
  güvenlik interaktif OIDC oturumları içindir (`docs/50-isletme/kullanici-ve-yetki.md` §7).
- **`materialized: table`** her koşuda tabloyu yeniden yazar (`CREATE OR REPLACE`). Artımlı ihtiyaçlar için
  dbt'nin `incremental` materyalizasyonu Iceberg'de çalışır ama `unique_key` + merge stratejisi ayrıca
  tasarlanmalıdır — bu referans örnek onu kapsamaz.
- **Bakım:** Gold tabloları da `maint-*` işlerinin kapsamına girer (`maintain_namespaces`) — yeni namespace
  eklerken `glue` değerlerindeki bakım kapsamını gözden geçirin.
- **Kapalı ağ:** pip (PyPI) erişimi zorunludur; Maven tarafındaki eşdeğer sorun ve çözümü
  `docs/50-isletme/sorun-giderme.md` §6.2.
