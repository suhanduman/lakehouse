# 40 — Kurulum sonrası

**Bu bölümde:** bileşenler ayağa kalktıktan sonra kurulumu **kullanılabilir** hâle getiren
adımlar — Polaris kataloğunun yaratılması, Superset'e Trino bağlantısının ve Iceberg
metadata panosunun aktarılması, dört arayüze Active Directory hesabıyla ilk giriş, izleme
hedeflerinin doğrulanması ve kabul testi. Bölüm sonunda kurulum **teslim edilebilir**
durumdadır.
**Süre:** komut çalıştırma 30–45 dakika; kabul testi ayrıca 25–30 dakika.
**Gereken yetki:** `$LAKEHOUSE_NS` ad alanında yönetici (Secret okuma/yazma, `exec`,
`port-forward`); izleme hedeflerini görmek için kümede okuma yetkisi. Tarayıcı adımları
için `lakehouse-admins` grubunda bir Active Directory hesabı.
**Nerede çalıştırılır:** `[bastion]` — `oc`, **`kubectl`**, `git`, `python3` kurulu ve
`oc login` ile kümeye girilmiş yönetim makinesi. `kubectl` ayrıca gereklidir:
`scripts/polaris-setup.sh` (Adım 2) ve `scripts/acceptance.sh` (Adım 7) küme çağrılarını
doğrudan `kubectl` ile yapar; yalnız `oc` kurulu bir makinede
`kubectl: command not found` ile dururlar. (`oc`, `kubectl`'in üst kümesidir ama ikili
adı farklıdır; `oc` istemcisiyle gelen `kubectl` ikilisini `PATH`'e koymak da yeterlidir.)
Adım 5.4'teki iki `[pod]` bloğu yönetim makinesinde değil, JupyterHub not defteri
hücresinde çalıştırılır.

> **Bu bölüme başlamadan önce [30-kurulum](30-kurulum.md) kontrol listesinin bütün
> maddeleri işaretli olmalıdır.** Tek istisna `trino` uygulamasıdır: o, `polaris-trino`
> Secret'ını beklediği için henüz `Healthy` değildir ve bu normaldir — Adım 2 onu
> tamamlar.

Bu bölümün akışı:

```text
Polaris kataloğu -> Trino ayağa kalkar -> Superset datasource -> pano
                 -> ilk girişler -> izleme hedefleri -> kabul testi
```

---

## 1. Değişkenleri yükleyin

[30-kurulum](30-kurulum.md) §1'deki dosya bu bölümde de kullanılır; her yeni terminal
oturumunda yeniden yüklenmelidir.

`[bastion]`

```bash
cd ~/lakehouse                      # depoyu `git clone` ile nereye indirdiyseniz orası
set -a; . install/lakehouse.env; set +a
echo "ns=$LAKEHOUSE_NS domain=$APPS_DOMAIN"
```

**Beklenen çıktı** (örnek — kendi değerlerinizle):

```text
ns=lakehouse domain=apps.ocp.example.net
```

**Ters giderse:** `ns= domain=` gibi boş bir satır görürseniz dosya yüklenmemiştir; yolu ve
`install/lakehouse.env` dosyasının varlığını kontrol edin. Dosyayı sildiyseniz
(`shred -u`) değerleri kurumun kasasından geri alın; bu bölüm `$LAKEHOUSE_NS` ve
`$APPS_DOMAIN` olmadan yürütülemez.

---

## 2. Polaris kataloğunu kurun

**Neden:** Polaris kurulumu boş gelir. Iceberg kataloğu (`lakehouse`), namespace'ler,
roller ve motorların kullanacağı **principal**'lar (servis kimlikleri) kurulumdan sonra bir
kez yaratılır. Bu adım `platform/polaris/setup.yaml` dosyasını okur —
[30-kurulum](30-kurulum.md) §4'te müşterinin S3 uç noktasını ve veri bucket'ını gösterecek
şekilde düzenlenmiş olmalıdır.

Adım **idempotenttir**: yeniden çalıştırılabilir, var olan nesneleri bozmaz. Yeni bir kaynak
eklenip yeni bir namespace gerektiğinde aynı komut tekrar koşturulur.

### 2.1 `polaris` CLI'sini kurun

CLI PyPI'dan gelir; kümeye kurulan bir bileşen değildir, yalnız yönetim makinesinde çalışır.
Sanal ortam kullanmak gerekir: güncel dağıtımlarda sistem Python'ına kurmak
`externally-managed-environment` hatası verir.

`[bastion]`

```bash
python3 -m venv .venv
.venv/bin/pip install 'apache-polaris==1.7.0'
.venv/bin/polaris --version
```

**Beklenen çıktı** (son satır; öncesinde `pip` indirme satırları akar):

```text
polaris 1.7.0
```

**Ters giderse:** `No module named venv` → Python'ın venv paketi eksiktir (RHEL/Fedora:
`sudo dnf install -y python3-pip`). `Could not find a version that satisfies the
requirement` → makinede PyPI erişimi yoktur ([20-on-kosullar](20-on-kosullar.md) madde 10);
kapalı ağda iç PyPI aynasını `pip install -i` ile verin. Sürüm **sabitlenmelidir**:
kurulumun sunucu tarafı Polaris 1.7.0'dır.

### 2.2 Katalog kurulumunu uygulayın

Betik `polaris-root` Secret'ından kök kimliği okur, `svc/polaris` için geçici bir
`port-forward` açar ve `setup.yaml`'ı uygular. Yarattığı her principal için `polaris-`
önekli bir Secret'ı kümeye yazar; **credential'ları ekrana basmaz**.

`[bastion]`

```bash
PATH="$PWD/.venv/bin:$PATH" scripts/polaris-setup.sh \
  --setup platform/polaris/setup.yaml --ns "$LAKEHOUSE_NS"
```

**Beklenen çıktı** (CI'nın taze kurulumundan alınmış gerçek çıktının kısaltılmışı; aradaki
ayrıcalık satırları çıkarılmıştır):

```text
2026-09-19 21:02:52,804 INFO === Starting Setup Apply Process ===
2026-09-19 21:02:52,983 INFO Creating principal role: writers
2026-09-19 21:02:53,044 INFO Creating principal role: readers
2026-09-19 21:02:53,062 INFO Creating principal role: sandbox_writers
2026-09-19 21:02:53,097 INFO Creating principal: connect
2026-09-19 21:02:53,174 INFO Creating principal: spark
2026-09-19 21:02:53,217 INFO Creating principal: trino
2026-09-19 21:02:53,253 INFO Creating principal: notebooks
2026-09-19 21:02:53,323 INFO Creating catalog: lakehouse
2026-09-19 21:02:53,528 INFO Creating namespace: 'crm' in catalog: 'lakehouse'
2026-09-19 21:02:53,644 INFO Creating namespace: 'crm_raw' in catalog: 'lakehouse'
2026-09-19 21:02:53,671 INFO Creating namespace: 'nginx_raw' in catalog: 'lakehouse'
2026-09-19 21:02:53,692 INFO Creating namespace: 'sandbox' in catalog: 'lakehouse'
2026-09-19 21:02:53,715 INFO Creating namespace: 'shop' in catalog: 'lakehouse'
2026-09-19 21:02:53,736 INFO Creating namespace: 'shop_raw' in catalog: 'lakehouse'
2026-09-19 21:02:53,777 INFO Creating catalog role: lakehouse_admin in catalog: lakehouse
2026-09-19 21:02:53,841 INFO Creating catalog role: lakehouse_read in catalog: lakehouse
2026-09-19 21:02:54,002 INFO Creating catalog role: lakehouse_sandbox in catalog: lakehouse
2026-09-19 21:02:54,166 INFO === Setup Apply Process Completed Successfully ===
secret/polaris-connect created
secret/polaris-spark created
secret/polaris-trino created
secret/polaris-notebooks created
Secret yazıldı: polaris-connect
Secret yazıldı: polaris-spark
Secret yazıldı: polaris-trino
Secret yazıldı: polaris-notebooks
```

Komutu ikinci kez çalıştırdığınızda yeni principal yaratılmaz; betik
`Yeni principal yok (idempotent çalıştırma).` satırını basar ve Secret'lara dokunmaz.
Yaratılan namespace'ler `platform/polaris/setup.yaml` dosyasındaki listedir; demo
kaynaklarının kapalı olduğu bir üretim kurulumunda o listeyi kendi şemalarınıza göre
düzenleyin.

**Ters giderse:** `polaris CLI yok` → Adım 2.1'deki `PATH` önekini unutmuşsunuzdur.
`Error from server (NotFound): secrets "polaris-root" not found` → Secret yaratılmamıştır
([90-referans/secret-listesi.md](90-referans/secret-listesi.md) 4. satır).
`Connection refused` → `svc/polaris` henüz hazır değildir
(`oc -n "$LAKEHOUSE_NS" rollout status deploy/polaris`).
`principal/credential sayısı uyuşmuyor` → önceki koşu yarıda kesilmiştir; Polaris'te
kısmen yaratılmış principal'ları `polaris principals list` ile görüp elle silin, sonra
komutu tekrarlayın.

### 2.3 Trino'nun ayağa kalktığını doğrulayın

`polaris-trino` Secret'ı yazıldığı anda Trino pod'unun eksik girdisi tamamlanır; ArgoCD
sonraki sync'te uygulamayı `Healthy` yapar.

`[bastion]`

```bash
oc -n "$ARGOCD_NS" wait application/trino \
  --for=jsonpath='{.status.health.status}'=Healthy --timeout=900s
oc -n "$LAKEHOUSE_NS" rollout status deploy/trino-coordinator --timeout=600s
```

**Beklenen çıktı** (örnek):

```text
application.argoproj.io/trino condition met
deployment "trino-coordinator" successfully rolled out
```

**Ters giderse:** uygulama `Degraded` kalıyorsa pod'un olaylarına bakın
(`oc -n "$LAKEHOUSE_NS" describe pod -l app.kubernetes.io/name=trino | tail -30`).
`CreateContainerConfigError` → `polaris-trino` Secret'ı yazılmamıştır (Adım 2.2 çıktısını
kontrol edin). ArgoCD değişikliği görmüyorsa sync'i elle tetikleyin:
`oc -n "$ARGOCD_NS" patch application trino --type merge -p '{"operation":{"sync":{}}}'`.

### 2.4 Katalog özelliklerini sonradan değiştirmek

`polaris setup apply` katalog **properties**'ini yalnız katalog **yaratılırken** yazar. Var
olan bir katalogda `setup.yaml`'daki `properties:` bloğunu değiştirip betiği tekrar
koşturmak hiçbir şey yapmaz. Bunun pratikteki tek örneği `drop-with-purge.enabled`
bayrağıdır: kapalıyken Trino'nun `DROP TABLE` isteğini Polaris `403` ile reddeder. Var olan
bir kurulumda açmak için:

`[bastion]`

```bash
CLIENT_ID=$(oc -n "$LAKEHOUSE_NS" get secret polaris-root \
  -o jsonpath='{.data.clientId}' | base64 -d)
CLIENT_SECRET=$(oc -n "$LAKEHOUSE_NS" get secret polaris-root \
  -o jsonpath='{.data.clientSecret}' | base64 -d)
export CLIENT_ID CLIENT_SECRET
oc -n "$LAKEHOUSE_NS" port-forward svc/polaris 8181:8181 >/dev/null 2>&1 &
PF=$!
sleep 5
.venv/bin/polaris catalogs update lakehouse \
  --set-property polaris.config.drop-with-purge.enabled=true
kill "$PF"
echo "katalog ozelligi guncellendi"
```

**Beklenen çıktı** (`polaris catalogs update` başarıda hiçbir şey basmaz; görülen tek satır
betiğin kendi onayıdır):

```text
katalog ozelligi guncellendi
```

**Ters giderse:** `401 Unauthorized` → `polaris-root` Secret'ındaki kimlik değişmiştir.
`Connection refused` → `port-forward` daha hazır değildir, `sleep` süresini artırın.
`kill: No such process` → `port-forward` zaten düşmüştür, zararsızdır. Tablo ve kaynak
silme yordamının tamamı işletme bölümündeki kaynak veya tablo silme kılavuzundadır.

---

## 3. Superset'e Trino bağlantısını aktarın

**Neden:** Superset'in `lakehouse` veritabanı tanımı depoda **deklaratiftir**
(`ConfigMap/superset-datasources` → pod içinde `/app/configs` altındaki `trino.yaml`), ama
Superset bağlantıları kendi metastore'unda tutar. Tanım kurulum başına **bir kez** içe aktarılır.
Parola dosyada değildir: `SQLALCHEMY_CUSTOM_PASSWORD_STORE` onu `TRINO_PASSWORD` ortam
değişkeninden okur.

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" rollout status deploy/superset-web-server --timeout=600s
oc -n "$LAKEHOUSE_NS" exec deploy/superset-web-server -- \
  superset legacy-import-datasources -p /app/configs/trino.yaml
```

**Beklenen çıktı** (CI koşusundan alınmış gerçek satırlar; aralarında Superset'in açılış
günlüğü akar):

```text
deployment "superset-web-server" successfully rolled out
INFO:superset.commands.dataset.importers.v0:Importing dataset from file trino.yaml
INFO:superset.commands.dataset.importers.v0:Importing 1 databases
```

Sonuç: SQL Lab'da `lakehouse` veritabanı görünür. Yazma kapalıdır (`allow_dml: false`);
CTAS/CVAS açıktır ama hedef şema **`sandbox`** olmak zorundadır. Komut idempotenttir: aynı
`database_name` güncellenir, ikinci bir satır açılmaz — bağlantı ayarı değiştiğinde tekrar
çalıştırın.

**Ters giderse:** `Error: No such command 'legacy-import-datasources'` → yanlış konteynere
bağlanmışsınızdır; `deploy/superset-web-server` dışındaki pod'larda bu komut yoktur.
`FileNotFoundError` → `superset-datasources` ConfigMap'i mount
edilmemiştir (`oc -n "$LAKEHOUSE_NS" get cm superset-datasources`). `legacy-` öneksiz
`import-datasources` komutu **kullanılmaz**: Superset 6.x'te o komut yalnız v1 ZIP kabul
eder ve bu YAML'ı reddeder.

---

## 4. Iceberg metadata panosunu içe aktarın

**Neden:** tablo düzeyinde veri metrikleri (satır sayısı, dosya sayısı, boyut, son commit)
için Iceberg'in Prometheus exporter'ı yoktur; bu bilgi tablonun kendi metadata'sındadır ve
SQL ile okunur. Ürün, bu sorguları toplayan hazır bir Superset pano paketi getirir
(`glue/files/superset/iceberg-metadata/`, kümede `superset-assets` ConfigMap'i →
`/app/assets`). Paket kurulum başına **bir kez** içe aktarılır; GitOps sync'i bunu
tekrarlamaz, çünkü import Superset'in metastore'una yazar.

> **PROD UYARISI — önce SQL'i uyarlayın.** Panonun sanal veri seti SQL'i geliştirme demo
> tablolarına (`shop.orders`, `crm.customers`, `nginx_raw.access_log`) sabittir. Üretim
> kurulumunda (`sources: []`, `pipelines: []`) bu tablolar **yoktur** ve SQL olduğu gibi
> içe aktarılırsa pano `TABLE_NOT_FOUND` ile boş kalır. Import'tan **önce**
> `glue/files/superset/iceberg-metadata/datasets/lakehouse/iceberg_table_health.yaml`
> içindeki `sql:` alanını kendi Silver/ham tablolarınıza göre düzenleyin, commit + push
> edin ve ArgoCD'nin ConfigMap'i güncellemesini bekleyin. Uyarlamayacaksanız panoyu hiç
> aktarmayın; kurulumun geri kalanı panodan bağımsızdır.

### 4.1 Yönetici hesabının var olduğunu doğrulayın

Import komutu panolara bir **sahip** ister ve bu, Superset'te **var olan** bir kullanıcı
olmalıdır. Superset kullanıcıları Keycloak ile ilk girişte yaratılır (`AUTH_ROLES_MAPPING`),
yani `lakehouse-admins` grubundaki yöneticinin arayüze **en az bir kez** girmiş olması
gerekir (Adım 5.3).

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" exec deploy/superset-web-server -- \
  superset fab list-users 2>/dev/null | tail -5
```

**Beklenen çıktı:** başlığın altında `username:… | email:… | role:[Admin]` biçiminde en az
bir satır. Aşağıdaki, hiç kimsenin giriş yapmadığı bir kümeden alınmış gerçek çıktıdır —
**bu hâlde import edilemez** (Superset'in ayrıntılı günlüğü stderr'e gider, `2>/dev/null`
onu susturur):

```text
Loaded your LOCAL configuration at [/app/pythonpath/superset_config.py]
List of users
-------------
```

**Ters giderse:** liste boşsa önce Adım 5.3'ü yapın (yönetici Keycloak ile bir kez girsin),
sonra buraya dönün. Komut `Refusing to start due to insecure SECRET_KEY` derse
`superset-secret` Secret'ı eksiktir
([90-referans/secret-listesi.md](90-referans/secret-listesi.md) 9. satır).

### 4.2 Paketi zip'leyip içe aktarın

ConfigMap anahtarları alt dizin taşıyamaz, bu yüzden dosyalar pod'da düz bir listedir ve
dizin ayracı yerine çift alt çizgi kullanılır — `/app/assets` içeriği aynen şudur:

```text
iceberg-metadata__charts__iceberg_table_health_table.yaml
iceberg-metadata__dashboards__iceberg_metadata.yaml
iceberg-metadata__databases__lakehouse.yaml
iceberg-metadata__datasets__lakehouse__iceberg_table_health.yaml
iceberg-metadata__metadata.yaml
```

Aşağıdaki ilk komut bu adları dizin ağacına geri açıp zip'ler. `oc exec` heredoc'u
**`-i` ister**: stdin aktarılmazsa betik sessizce hiçbir şey yapmaz ve zip oluşmaz.

`[bastion]` (üçüncü komut `oc exec` ile Superset konteynerinde koşar; ilk ikisi yönetim
makinesinde)

```bash
SUPERSET_ADMIN=$(oc -n "$LAKEHOUSE_NS" exec deploy/superset-web-server -- \
  superset fab list-users 2>/dev/null | grep 'role:.*Admin' \
  | sed -n 's/^username:\([^ ]*\).*/\1/p' | head -1)
echo "sahip: $SUPERSET_ADMIN"
oc -n "$LAKEHOUSE_NS" exec -i deploy/superset-web-server -- python3 - <<'PY'
import pathlib, shutil
src = pathlib.Path('/app/assets'); dst = pathlib.Path('/tmp/iceberg-metadata')
shutil.rmtree(dst, ignore_errors=True)
for f in src.glob('iceberg-metadata__*'):
    p = dst / f.name.split('__', 1)[1].replace('__', '/')
    p.parent.mkdir(parents=True, exist_ok=True); p.write_text(f.read_text())
shutil.make_archive('/tmp/iceberg-metadata', 'zip', '/tmp', 'iceberg-metadata')
print('zip hazır')
PY
oc -n "$LAKEHOUSE_NS" exec deploy/superset-web-server -- \
  superset import-dashboards -p /tmp/iceberg-metadata.zip -u "$SUPERSET_ADMIN"
```

**Beklenen çıktı** (örnek — `sahip:` satırında kendi yöneticinizin kullanıcı adı çıkar;
import komutu başarıda ek satır basmaz):

```text
sahip: ayse.yilmaz
zip hazır
```

`-u` bayrağı **zorunludur**. `--overwrite` diye bir bayrak **yoktur**: Superset 6.1.0
nesneleri her zaman `uuid`'ye göre üzerine yazar. Ürünün datasource'u ile paketteki
veritabanı tanımı **aynı sabit uuid'yi** taşır (tek kaynak: `glue/templates/_helpers.tpl`),
bu yüzden Adım 3'te yaratılan `lakehouse` bağlantısı bozulmaz, aynı değerlerle güncellenir.
Sıra önemlidir: **önce** Adım 3 (datasource), **sonra** bu adım.

**Ters giderse:** `sahip:` satırı boşsa yönetici hesabı henüz yoktur (Adım 4.1) ya da
otomatik çıkarma tutmamıştır; o durumda değeri elle atayın
(`SUPERSET_ADMIN=kullanici.adi`) ve son komutu tekrar çalıştırın. `User not found` → yazılan
hesap Superset'e hiç girmemiştir. Komut sessizce biter ama pano görünmezse zip oluşmamıştır:
`oc exec` çağrısında `-i` bayrağını atlamışsınızdır. Pano açılıyor ama boşsa veri seti ile
veritabanının uuid'si eşleşmemiştir — paketi canlı bir Superset'ten yeniden ürettiyseniz
Helm ifadeleri kaybolmuştur, depodaki hâline dönün.

### 4.3 Panoyu doğrulayın

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" exec deploy/superset-web-server -- python3 -c "
from superset.app import create_app
with create_app().app_context():
    from superset import db
    from superset.models.dashboard import Dashboard
    print([(d.dashboard_title, d.slug) for d in db.session.query(Dashboard).all()])" \
  2>/dev/null | tail -1
```

**Beklenen çıktı** (örnek):

```text
[('Iceberg metadata', 'iceberg-metadata')]
```

Arayüzde **Dashboards → Iceberg metadata**. Panonun SQL'i ve yeni tablo ekleme yordamı
işletme bölümündeki veri metrikleri kılavuzundadır.

**Ters giderse:** boş liste `[]` → import çalışmamıştır (Adım 4.2 çıktısına dönün). Pano
açılıyor ama hücreler hata veriyorsa SQL hâlâ demo tablolarını gösteriyordur (yukarıdaki
PROD UYARISI).

---

## 5. İlk giriş kontrol listesi

**Neden:** dört arayüzün de Active Directory kimliğiyle açıldığını kullanıcılar davet
edilmeden önce **kurulumcu** doğrular. Adresler `.env` değerlerinizden türetilir.

Tarayıcı akışlarının tamamı üretim kümesinde geçerlidir; geliştirme (kind) kümesinde
Keycloak'ın issuer adresi küme içi olduğu için tarayıcıyla OIDC girişi yapılamaz. Bu yüzden
aşağıdaki adımların ekran sonuçları **(OpenShift'te doğrulanır)** işaretlidir.

### 5.1 Adresleri yazdırın

`[bastion]`

```bash
for c in keycloak trino superset jupyterhub zeppelin; do
  echo "https://$c-$LAKEHOUSE_NS.$APPS_DOMAIN"
done
```

**Beklenen çıktı** (örnek — kendi `$APPS_DOMAIN` değerinizle):

```text
https://keycloak-lakehouse.apps.ocp.example.net
https://trino-lakehouse.apps.ocp.example.net
https://superset-lakehouse.apps.ocp.example.net
https://jupyterhub-lakehouse.apps.ocp.example.net
https://zeppelin-lakehouse.apps.ocp.example.net
```

`platform/values/site/glue.yaml` içinde kurumsal bir `hostname` ezmesi yaptıysanız gerçek
adresleri `oc -n "$LAKEHOUSE_NS" get route` ile alın. Adreslerin ve portların tamamı:
[90-referans/port-ve-servisler.md](90-referans/port-ve-servisler.md).

**Ters giderse:** adresler doğru görünüyor ama tarayıcı açılmıyorsa Route'lar
oluşmamıştır ya da DNS çözülmüyordur ([30-kurulum](30-kurulum.md) §7.3).

### 5.2 Dört arayüz, dört giriş

| Arayüz | Adres | Kimlik | Hangi AD grubu girer | Beklenen ekran |
|---|---|---|---|---|
| Trino Web UI | `https://trino-$LAKEHOUSE_NS.$APPS_DOMAIN/ui/` | Keycloak (OIDC) | `lakehouse-admins`, `lakehouse-analysts` | Sorgu listesi; sağ üstte kullanıcı adınız |
| Superset | `https://superset-$LAKEHOUSE_NS.$APPS_DOMAIN` | Keycloak (OIDC) | üçü de | Giriş sayfasında **Sign in with keycloak** düğmesi, sonra boş pano listesi |
| JupyterHub | `https://jupyterhub-$LAKEHOUSE_NS.$APPS_DOMAIN` | Keycloak (OIDC) | üçü de | **Start My Server** → not defteri sunucusu (ilk açılış ~5 dakika) |
| Zeppelin | `https://zeppelin-$LAKEHOUSE_NS.$APPS_DOMAIN` | **Doğrudan AD (LDAPS/Shiro)** | üçü de | Kullanıcı adı/parola formu, sonra Notebook listesi |

**Zeppelin Keycloak kullanmaz.** 0.12 sürümünde OIDC realm'i yoktur; giriş Shiro ile
doğrudan Active Directory'ye yapılır ve kullanıcı adının sonuna `$AD_UPN_SUFFIX` eklenir.
`lakehouse-*` gruplarının hiçbirinde olmayan bir hesap, parolası doğru olsa bile `401`
alır — bu bilerek böyledir, çünkü Zeppelin Trino'ya paylaşımlı bir servis hesabıyla
bağlanır.

Rol eşlemeleri ürün varsayılanıdır: Superset'te `lakehouse-admins → Admin`,
`lakehouse-analysts → Alpha`, `lakehouse-users → Gamma`; JupyterHub'da üç grup da girer,
hub yöneticisi yalnız `lakehouse-admins`'tir.

### 5.3 Superset'e yönetici olarak ilk giriş

Bu giriş **Adım 4.1'in ön koşuludur**: Superset yöneticisi ancak ilk girişte yaratılır.

1. `https://superset-$LAKEHOUSE_NS.$APPS_DOMAIN` adresini açın.
2. **Sign in with keycloak** düğmesine basın, AD hesabınızla oturum açın.
3. Sağ üstte **Settings** menüsünün göründüğünü doğrulayın (yalnız `Admin` rolünde çıkar).
4. **SQL → SQL Lab** → veritabanı listesinde `lakehouse` seçilebiliyor olmalıdır.

**Beklenen sonuç (OpenShift'te doğrulanır):** SQL Lab'da `select 1` sorgusu sonuç döndürür;
`Settings → List Users` listesinde kendi hesabınız `Admin` rolüyle görünür.

**Ters giderse:** giriş sayfasında keycloak düğmesi yoksa `OIDC_CLIENT_SECRET` ya da realm
içe aktarımı eksiktir ([30-kurulum](30-kurulum.md) §5.6). Giriş oluyor ama **Settings**
görünmüyorsa AD hesabınız `lakehouse-admins` grubunda değildir; grup üyeliği düzeltildikten
sonra **çıkış yapıp yeniden girin** (rol eşlemesi her girişte yeniden hesaplanır).
`redirect_uri` hatası → Keycloak client'ında Route adresi kayıtlı değildir; kurumsal DNS
ezmesi yaptıysanız realm'i o adresle yeniden üretmeniz gerekir
([30-kurulum](30-kurulum.md) "Realm içeriğini sonradan değiştirmek").

### 5.4 Trino, JupyterHub ve Zeppelin

1. **Trino Web UI** — `https://trino-$LAKEHOUSE_NS.$APPS_DOMAIN/ui/`: Keycloak'a
   yönlendirir, dönüşte sorgu listesi açılır. Tarayıcı sertifika uyarısı veriyorsa Trino
   Route'u `passthrough` modundadır ve kurumun tarayıcıları iç kök CA'ya
   (`~/lakehouse-ca.crt`) güvenmiyordur; CA'yı dağıtın ya da `tls.caBundle` doldurup
   Route'u `reencrypt` yapın ([30-kurulum](30-kurulum.md) §5.11).
2. **JupyterHub** — giriş sonrası **Start My Server**. Not defteri imajı ilk çekimde ~4,5
   dakika sürer; sunucu açıldıktan sonra `pyiceberg` ve `trino` paketleri **her** açılışta
   yeniden kurulur (~1 dakika, PyPI erişimi gerekir). Her kullanıcıya **kişisel bir disk**
   (PVC) yaratılır: üretimde 10 Gi (`platform/values/jupyterhub.yaml` →
   `singleuser.storage.capacity`), geliştirme kümesinde 1 Gi
   (`platform/values/jupyterhub-dev.yaml`). Hazır ortam değişkenleri: `POLARIS_URI`,
   `POLARIS_CREDENTIAL` (paylaşımlı `notebooks` principal'ı), `TRINO_HOST` ve
   `S3_ENDPOINT`.
3. **Zeppelin** — giriş sonrası yeni bir not açıp `%jdbc` ile sorgu çalıştırın. İlk açılışta
   Trino JDBC sürücüsü Maven Central'dan indirilir (ölçülen ~64 saniye; disk üzerinde
   kalıcıdır). O sırada `Interpreter Setting 'jdbc' is not ready … DOWNLOADING_DEPENDENCIES`
   alırsanız bekleyin.

#### Not defterinden ilk sorgu

Aşağıdaki iki hücre, bir not defterinin ürüne bağlanmasının **tam** yoludur; analistlere
verilecek başlangıç örneği budur. Ortam değişkenleri yukarıda sayılanlardır, kullanıcı
bunları yazmaz.

`[pod]` (JupyterHub not defteri hücresi)

```python
# PyIceberg -> Polaris (paylaşımlı notebooks principal'ı; yazma yalnız sandbox namespace'inde)
import os
from pyiceberg.catalog import load_catalog
cat = load_catalog("lakehouse", type="rest", uri=os.environ["POLARIS_URI"],
                   warehouse="lakehouse", credential=os.environ["POLARIS_CREDENTIAL"],
                   scope="PRINCIPAL_ROLE:ALL")
cat.list_namespaces()
tbl = cat.load_table("shop.orders"); tbl.scan(limit=10).to_pandas()
```

`[pod]` (JupyterHub not defteri hücresi)

```python
# Trino: KENDİ kimliğinizle -> satır filtresi ve kolon maskesi UYGULANIR.
# Tarayıcıda Keycloak onayı istenir; token yerel olarak saklanır (geliştirme kümesinde
# çalışmaz, bkz. Adım 5 girişi).
import os, trino
conn = trino.dbapi.connect(host=os.environ["TRINO_HOST"], port=8443, http_scheme="https",
                           verify="/etc/lakehouse-ca/tls.crt",
                           auth=trino.auth.OAuth2Authentication(), catalog="lakehouse")
cur = conn.cursor(); cur.execute("select * from shop.orders limit 10"); cur.fetchall()
```

`verify=` yolu **açıkça** verilir: `REQUESTS_CA_BUNDLE` bilerek tanımlı değildir
(Adım 5.6, 2. madde). PyIceberg → Polaris çağrısı küme içinde düz HTTP olduğu için CA
istemez. Tarayıcısız/otomatik işlerde `OAuth2Authentication` yerine
`trino.auth.BasicAuthentication` ile bir **servis hesabı** kullanılır; o durumda satır ve
kolon kuralları kullanıcı bazında işlemez (Adım 5.6, 1. madde).

`shop.orders` geliştirme demo tablosudur; üretimde kendi Silver tablonuzun adını yazın.

**Beklenen sonuç (OpenShift'te doğrulanır):** üç arayüzde de kendi AD kullanıcı adınızla
oturum açılır; JupyterHub'da kişisel bir disk (PVC) yaratılır ve yukarıdaki iki hücre
sonuç döndürür, Zeppelin'de `%jdbc` sorgusu satır basar.

**Ters giderse:** JupyterHub'da sunucu zaman aşımıyla düşerse imaj çekimi 20 dakikayı
aşmıştır (kapalı ağda iç imaj aynası gerekir). Zeppelin'de `401` alan kullanıcı
`lakehouse-*` gruplarının hiçbirinde değildir. Not defterinden Trino'ya bağlanırken
`SSLCertVerificationError` alırsanız CA'yı açıkça verin:
`verify="/etc/lakehouse-ca/tls.crt"`.

### 5.5 Grup değişikliği ne zaman etkili olur

Üretimde Trino grupları **doğrudan AD'den** okunur (`group-provider.name=ldap`,
`platform/values/site/trino.yaml`); AD'de yapılan üyelik değişikliği kullanıcının sonraki
oturumunda geçerlidir. Geliştirme/kind kurulumunda ise **dosya tabanlı** grup sağlayıcısı
kullanılır (`platform/values/trino-dev.yaml` → `auth.groups`) ve bu dosyanın yenileme
periyodu **yoktur**: Trino onu yalnız açılışta okur. Orada bir grup adı değiştirildiğinde
`trino` release'inin `helm upgrade` ile güncellenmesi **ve** ardından
`oc -n "$LAKEHOUSE_NS" rollout restart deploy/trino-coordinator` çalıştırılması gerekir;
aksi hâlde eski üyelik yürürlükte kalır. Kullanıcı ekleme, grup ↔ rol eşlemesi, satır
filtresi ve kolon maskesi örnekleri işletme bölümündeki kullanıcı ve yetki sayfasındadır.

### 5.6 İlk günden bilinmesi gereken dört davranış

Aşağıdakiler bilinçli tasarım kararlarıdır; "eksik yapılandırma" sanılıp değiştirilmemelidir.

1. **Kimlik Trino'ya yalnız doğrudan bağlantılarda taşınır.** Superset ve Zeppelin, Trino'ya
   paylaşımlı **servis hesabıyla** bağlanır; o araçlarda satır filtresi ve kolon maskesi
   **uygulanmaz**, sınırlama araç düzeyinde (Superset rolleri, Zeppelin not izinleri) yapılır.
   Kullanıcının kendi kimliğiyle bağlanması için Trino Web UI, OIDC destekli bir JDBC/CLI
   istemcisi ya da not defterinde `trino.auth.OAuth2Authentication` kullanılır.
2. **`REQUESTS_CA_BUNDLE` bilerek tanımlı değildir** (Superset ve JupyterHub). Tek kök
   dayatmak, Keycloak ve PyPI gibi dış HTTPS çağrılarını kırardı. İç CA yine
   `/etc/lakehouse-ca/tls.crt` yolunda mount'ludur ve Trino'ya bağlanırken **açıkça**
   verilir.
3. **Zeppelin'in `zeppelin-interpreter` Secret'ı yalnız bir tohumdur.** initContainer onu
   diske **yalnız dosya yokken** kopyalar; Zeppelin her açılışta dosyayı kendisi yeniden
   yazar. Bu yüzden Secret'ı güncellemek tek başına etkisizdir: bağlantı ayarı arayüzden
   (**Interpreter → jdbc**) değiştirilir.
4. **Zeppelin notları varsayılan olarak özeldir** (`ZEPPELIN_NOTEBOOK_PUBLIC=false`).
   Paylaşım not bazında arayüzden verilir; bu değer değişirse var olan notların izinleri
   geriye dönük **değişmez**. Superset'in **Alerts & Reports** özelliği de varsayılan olarak
   kapalıdır (kurulum Valkey/worker olmadan çalışır).

---

## 6. İzleme hedeflerini doğrulayın

**Neden:** ürün kendi Prometheus'unu kurmaz; OpenShift'in **kullanıcı iş yükü izlemesi**
(UWM) `$LAKEHOUSE_NS` ad alanındaki izleme nesnelerini kendiliğinden toplar
([20-on-kosullar](20-on-kosullar.md) madde 9.1). Nesneler kümede olsa da UWM kapalıysa
kimse okumaz: beş alarmın hiçbiri çalışmaz.

### 6.1 Nesneler kümede mi

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" get podmonitor,servicemonitor,prometheusrule
```

**Beklenen çıktı** (kind kümesinden alınmış gerçek çıktı; `AGE` sütunu sizde farklıdır):

```text
NAME                                                         AGE
podmonitor.monitoring.coreos.com/kafka-resources-metrics     5d12h
podmonitor.monitoring.coreos.com/spark-operator-podmonitor   5d12h

NAME                                           AGE
servicemonitor.monitoring.coreos.com/polaris   5d12h

NAME                                             AGE
prometheusrule.monitoring.coreos.com/lakehouse   5d12h
```

`kafka-resources-metrics` Kafka broker'larını, Kafka Connect'i ve kafka-exporter'ı birlikte
toplar; `spark-operator-podmonitor` spark-operator chart'ından, `polaris` ServiceMonitor'ü
Polaris chart'ından gelir. `prometheusrule/lakehouse` **tek** nesnedir ve içinde **beş**
alarm kuralı vardır: `LakehouseConnectTaskFailed`, `LakehouseSinkStalled`,
`LakehouseSilverMergeStale`, `LakehouseSparkScheduledRunFailed`, `LakehouseSparkRunTooLong`.

**Ters giderse:** `No resources found` → `monitoring.enabled` kapalıdır ya da glue henüz
sync olmamıştır. `the server doesn't have a resource type "podmonitor"` → kümede Prometheus
Operator CRD'leri yoktur; OpenShift'te bu, UWM'nin hiç açılmadığı anlamına gelir.

### 6.2 Hedefler toplanıyor mu

OpenShift konsolunda **Observe → Targets** sayfasını açıp ad alanı süzgecine
`$LAKEHOUSE_NS` yazın; beklenen, yukarıdaki üç toplayıcıya karşılık gelen hedeflerin **Up**
olmasıdır. Kuralları **Observe → Alerting → Alerting rules** sayfasında ad alanına göre
süzerek görürsünüz; taze bir kurulumda beşinin de **Inactive** olması beklenir (kurallar
bilerek "veri yokken ateşlemez" biçiminde yazılmıştır).

**Beklenen sonuç (OpenShift'te doğrulanır):** `Observe → Targets` listesinde
`lakehouse/kafka-resources-metrics`, `lakehouse/spark-operator-podmonitor` ve
`lakehouse/polaris` hedefleri **Up**; `Observe → Alerting` altında beş Lakehouse kuralı
listeleniyor ve hiçbiri **Firing** değil.

Aynı iddiaları komut satırından koşturan e2e yolu `test/e2e/monitoring-path.sh`'tir; CI
koşusunda ürettiği gerçek satırlar (aşağıdaki `kube-state-metrics` satırı **yalnız
geliştirme yığınında** çıkar — OpenShift'te kube-state-metrics bu kurulumla gelmez, o
satırı görmezsiniz):

```text
OK up .*kafka-resources-metrics.* (3)
OK up .*spark-operator.* (2)
OK up .*kube-state-metrics.* (1)
OK up .*polaris.* (1)
OK 5 kural yüklü
OK ateşlenen alarm yok
```

**Ters giderse:** hedefler görünmüyorsa UWM açık değildir
([20-on-kosullar](20-on-kosullar.md) madde 9.1). Hedef `Down` ise ilgili pod'un metrik portu
kapalıdır; `oc -n "$LAKEHOUSE_NS" get pods` ile pod'un ayakta olduğunu doğrulayın. Alarm ve
eşik yorumlarının tamamı işletme bölümündeki izleme ve alarmlar sayfasındadır.

---

## 7. Kabul testini koşturun

**Neden:** şartname maddelerinin kanıtı kümede koşan testlerle verilir.
`scripts/acceptance.sh` dokuz e2e yolunu sırayla koşturur ve sonunda **KABUL** özeti basar.
Betik kurulum **yapmaz**; zaten kurulu bir kümede çalışır.

**Ön koşul — ad alanı:** e2e yol betikleri ve fixture manifest'leri `lakehouse` ad alanına
**sabittir** (`test/e2e/*.sh`, `test/e2e/*-fixture.yaml`). `$LAKEHOUSE_NS` başka bir değere
ayarlandıysa `--ns` bayrağı yalnız betiğin kendi adımlarını taşır; yollar yine `lakehouse`
arar ve koşu düşer. Ayrıntı: [90-referans/kabul-testleri.md](90-referans/kabul-testleri.md)
§1.

**Ön koşul — demo kaynaklar:** `sources`, `pipelines` ve `nginx.enabled` açık olmalıdır. Bu
üçü üretim değerlerinde **kapalıdır**; kabul koşusu bu yüzden ya demo değerleriyle
kurulmuş bir doğrulama kümesinde ya da müşteri kaynakları tanımlandıktan sonra kendi
tablolarınızla yapılır. Madde ↔ kanıt tablosu, süre beklentileri ve yola özgü sık durumlar:
[90-referans/kabul-testleri.md](90-referans/kabul-testleri.md).

`[bastion]`

```bash
PATH="$PWD/.venv/bin:$PATH" PROM_STS=prometheus-user-workload \
  PROM_SVC=prometheus-user-workload GRAFANA_SKIP=1 \
  scripts/acceptance.sh --ns "$LAKEHOUSE_NS" \
  --mon-ns openshift-user-workload-monitoring --velero-ns openshift-adp
```

**Beklenen çıktı** (son bölüm; `E2E … OK` satırlarının her biri CI koşusundan alınmış
gerçektir, özet satırının biçimi `scripts/acceptance.sh`'ten gelir):

```text
=== KABUL ÖZETİ ===
E2E F2 OK
E2E F3 MONGO OK
E2E F3 NGINX OK
E2E F4 TRINO OK
E2E F4 SUPERSET OK
E2E F4 JUPYTERHUB OK
E2E F4 ZEPPELIN OK
E2E F5 MONITORING OK
E2E F5 DR OK
KABUL: 9/9 yol geçti (ns=lakehouse, mon-ns=openshift-user-workload-monitoring, velero-ns=openshift-adp)
```

Üç ortam değişkeni ve iki bayrak OpenShift'e özgüdür: UWM'nin Prometheus nesneleri farklı
adlandırılır ve **Grafana yoktur** (`GRAFANA_SKIP=1` yalnız pano iddialarını atlar; hedef,
metrik, kural ve alarm iddiaları koşmaya devam eder). Ad alanı yedeği OADP ile alındığı için
Velero ad alanı `openshift-adp`'dir.

Betiğin bu bayraklarla uçtan uca koşusu **(OpenShift'te doğrulanır)**: bugüne kadar yalnız
kind/CI ortamında tamamlanmıştır.

**Ters giderse:** bir yol düşerse betik orada durur, çıkış kodu `1` olur ve özet
`KABUL BAŞARISIZ` satırıyla biter; yola özgü düzeltmeler
[90-referans/kabul-testleri.md](90-referans/kabul-testleri.md) dosyasındadır. **Kabul
koşusundan sonra zamanlanmış Spark işlerini geri açmayı unutmayın:** koşu onları askıya alır
ve kendiliğinden geri açmaz.

---

## Kontrol listesi — kurulum tamam

Hepsi işaretliyse kurulum teslim edilebilir:

- [ ] `scripts/polaris-setup.sh` hatasız bitti; `lakehouse` kataloğu ve `setup.yaml`'daki
      namespace'ler yaratıldı.
- [ ] Dört principal Secret'ı kümede: `polaris-connect`, `polaris-spark`, `polaris-trino`,
      `polaris-notebooks`.
- [ ] `trino` Application'ı `Healthy`; `deploy/trino-coordinator` rollout'u tamam.
- [ ] Superset'te `lakehouse` veritabanı SQL Lab'da görünüyor
      (`legacy-import-datasources` koştu).
- [ ] Iceberg metadata panosu içe aktarıldı **ya da** bilinçli olarak atlandı; aktarıldıysa
      sanal veri seti SQL'i müşterinin tablolarına uyarlanmıştı.
- [ ] Dört arayüze de (Trino UI, Superset, JupyterHub, Zeppelin) AD hesabıyla girildi.
- [ ] `lakehouse-admins` grubundaki yönetici Superset'te `Admin` rolüyle görünüyor.
- [ ] `oc get podmonitor,servicemonitor,prometheusrule` iki PodMonitor, bir ServiceMonitor
      ve bir PrometheusRule listeliyor.
- [ ] Konsolda **Observe → Targets** hedefleri `Up`; beş alarm kuralı listede ve hiçbiri
      ateşlenmiş değil.
- [ ] Kabul testi koşturulduysa `KABUL: 9/9 yol geçti` satırı alındı ve zamanlanmış Spark
      işleri geri açıldı.
- [ ] `install/lakehouse.env` kurumun kasasına alındı ya da silindi
      ([90-referans/secret-listesi.md](90-referans/secret-listesi.md)).

## Sonraki bölüm

Gün-2 işleri (kaynak ve tablo ekleme, kullanıcı ve yetki, yedek ve geri dönüş, yükseltme,
izleme ve alarmlar, veri metrikleri, günlük/haftalık kontroller, sorun giderme)
`docs/50-isletme/` altındaki kılavuzlardadır. Başvuru tabloları:
[90-referans/kabul-testleri.md](90-referans/kabul-testleri.md),
[90-referans/port-ve-servisler.md](90-referans/port-ve-servisler.md),
[90-referans/surumler-ve-lisanslar.md](90-referans/surumler-ve-lisanslar.md),
[90-referans/oc-hizli-basvuru.md](90-referans/oc-hizli-basvuru.md),
[90-referans/secret-listesi.md](90-referans/secret-listesi.md),
[90-referans/values-anahtarlari.md](90-referans/values-anahtarlari.md).
