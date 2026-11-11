# 40 — Kurulum sonrası

Bileşenler kurulduktan sonra lakehouse'u kullanılır hâle getiren adımlar bu bölümdedir:
katalog kurulur, Superset'e Trino bağlantısı ve "Tablo sağlığı" panosu eklenir, dört
arayüze Active Directory hesabıyla ilk kez girilir, izleme doğrulanır ve kabul testi
çalıştırılır. Bölümün sonunda kurulum teslim edilebilir durumdadır.

**Ne kadar sürer:** komutlar 30–45 dakika; kabul testi ayrıca 25–30 dakika.

**Hangi yetkiler gerekir:**

- `$LAKEHOUSE_NS` ad alanında yönetici yetkisi (Secret okuma ve yazma, pod'a komut
  gönderme, `port-forward`)
- izleme hedeflerini görmek için kümede okuma yetkisi
- tarayıcı adımları için `lakehouse-admins` grubunda bir Active Directory hesabı

**Komutlar nerede çalışır:**

- `[bastion]` — `oc`, `kubectl`, `git` ve `python3` araçlarının kurulu olduğu ve `oc login`
  ile kümeye bağlı yönetim makinesi. `kubectl` ayrıca gerekir: §2 ve §7'deki betikler küme
  komutlarını `kubectl` ile çalıştırır. `oc` ile gelen `kubectl` dosyasını `PATH`'e koymak
  yeterlidir.
- `[pod]` — §5.4'teki iki blok yönetim makinesinde değil, JupyterHub not defteri
  hücresinde çalışır.

> **Başlamadan önce [30-kurulum](30-kurulum.md) kontrol listesinin bütün maddeleri
> işaretli olmalıdır.** Tek istisna `trino` uygulamasıdır: `polaris-trino` Secret'ını
> beklediği için henüz `Healthy` değildir. Bu normaldir; §2 onu tamamlar.

Bölümün akışı:

```text
katalog -> Trino açılır -> Superset bağlantısı -> pano
        -> ilk girişler -> izleme -> kabul testi
```

---

## 1. Değişkenleri yükleyin

[30-kurulum](30-kurulum.md) §1'de doldurulan dosya bu bölümde de kullanılır. Her yeni
terminalde bir kez çalıştırın:

`[bastion]`

```bash
cd ~/lakehouse
set -a; . install/lakehouse.env; set +a
echo "ns=$LAKEHOUSE_NS domain=$APPS_DOMAIN"
```

**Beklenen çıktı** (kendi değerlerinizle):

```text
ns=lakehouse domain=apps.ocp.example.net
```

**Ters giderse:** `ns= domain=` gibi boş bir satır görürseniz dosya yüklenmemiştir.
Bulunduğunuz klasörü ve `install/lakehouse.env` dosyasının orada olduğunu kontrol edin.
Dosyayı kurulumdan sonra sildiyseniz değerleri kurumun parola kasasından geri alın.

---

## 2. Polaris kataloğunu kurun

Polaris (Iceberg kataloğu) boş kurulur. Katalog (`lakehouse`), ad alanları, roller ve
bileşenlerin kullanacağı servis kimlikleri (**principal**) kurulumdan sonra bir kez
yaratılır. Bu adım `platform/polaris/setup.yaml` dosyasını okur; dosya
[30-kurulum](30-kurulum.md) §4'te kurumun S3 adresine ve veri bucket'ına göre
düzenlenmiş olmalıdır.

Adım tekrar çalıştırılabilir; var olan nesneleri bozmaz. İleride yeni bir ad alanı
gerektiğinde (ör. yeni bir kaynak eklenince) aynı komut yeniden çalıştırılır.

### 2.1 `polaris` komut satırı aracını kurun

Araç yalnız yönetim makinesinde çalışır, kümeye kurulmaz. Python sanal ortamına kurulur
(güncel dağıtımlarda sistem Python'ına kurmak `externally-managed-environment` hatası
verir).

`[bastion]`

```bash
python3 -m venv .venv
.venv/bin/pip install -q --disable-pip-version-check 'apache-polaris==1.7.0'
.venv/bin/polaris --version
```

**Beklenen çıktı:**

```text
polaris 1.7.0
```

Sürüm sabittir: kümedeki Polaris sunucusu 1.7.0'dır.

**Ters giderse:**

- `No module named venv` → Python'ın sanal ortam paketi eksiktir. RHEL'de:
  `sudo dnf install -y python3-pip`.
- `Could not find a version that satisfies the requirement` → makine PyPI'a erişemiyordur
  ([20-on-kosullar](20-on-kosullar.md) madde 10). Kapalı ağda kurumun PyPI aynasının
  adresini `pip install -i` ile verin.

### 2.2 Kataloğu kurun

Betik, kök kimliği `polaris-root` Secret'ından okur, Polaris'e geçici bir bağlantı açar
ve `setup.yaml` dosyasını uygular. Yarattığı her servis kimliği için `polaris-` ile
başlayan bir Secret'ı kümeye yazar. Kimlik bilgilerini ekrana yazmaz.

`[bastion]`

```bash
PATH="$PWD/.venv/bin:$PATH" scripts/polaris-setup.sh \
  --setup platform/polaris/setup.yaml --ns "$LAKEHOUSE_NS"
```

**Beklenen çıktı** (ilk kurulumda; kısaltılmıştır, aradaki yetki satırları çıkarıldı):

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

Yaratılan ad alanları `platform/polaris/setup.yaml` dosyasındaki listedir; sizin
listeniz kendi kaynaklarınıza göre farklıdır.

Komutu ikinci kez çalıştırdığınızda yeni kimlik yaratılmaz, son satır şudur:

```text
Yeni principal yok (idempotent çalıştırma).
```

Betik dört `polaris-…` Secret'ını `lakehouse.io/yonetim=manual-40-kurulum-sonrasi-2.2`
etiketiyle işaretler. Bu Secret'lar Git'te değildir; silinirlerse bu adım tekrarlanarak
yeniden yaratılırlar
([50-isletme/degisiklik-nasil-uygulanir.md](50-isletme/degisiklik-nasil-uygulanir.md) §7).

**Ters giderse:**

- `polaris CLI yok` → komutun başındaki `PATH=...` kısmını unutmuşsunuzdur.
- `secrets "polaris-root" not found` → kök kimlik Secret'ı yaratılmamıştır
  ([90-referans/secret-listesi.md](90-referans/secret-listesi.md)).
- `Connection refused` → Polaris henüz hazır değildir. Şunu bekleyip tekrar deneyin:
  `oc -n "$LAKEHOUSE_NS" rollout status deploy/polaris`.
- `principal/credential sayısı uyuşmuyor` → önceki çalıştırma yarıda kesilmiştir. Polaris'te
  yarım kalan kimlikleri `.venv/bin/polaris principals list` ile görüp silin, sonra komutu
  tekrarlayın.

### 2.3 Trino'nun açıldığını doğrulayın

`polaris-trino` Secret'ı yazılınca Trino'nun eksik ayarı tamamlanır ve ArgoCD uygulamayı
`Healthy` yapar:

`[bastion]`

```bash
oc -n "$ARGOCD_NS" wait application/trino \
  --for=jsonpath='{.status.health.status}'=Healthy --timeout=900s
oc -n "$LAKEHOUSE_NS" rollout status deploy/trino-coordinator --timeout=600s
```

**Beklenen çıktı:**

```text
application.argoproj.io/trino condition met
deployment "trino-coordinator" successfully rolled out
```

**Ters giderse:**

- Uygulama `Degraded` kalıyorsa pod'un olaylarına bakın:
  `oc -n "$LAKEHOUSE_NS" describe pod -l app.kubernetes.io/name=trino | tail -30`.
- `CreateContainerConfigError` → `polaris-trino` Secret'ı yazılmamıştır; §2.2'nin
  çıktısını kontrol edin.
- ArgoCD değişikliği görmüyorsa uygulamayı hemen eşitlemesini isteyin:
  `oc -n "$ARGOCD_NS" patch application trino --type merge -p '{"operation":{"sync":{}}}'`.

### 2.4 Katalog ayarını sonradan değiştirmek

`setup.yaml` dosyasındaki `properties:` bloğu yalnız katalog **ilk yaratılırken**
uygulanır. Var olan bir katalogda o bloğu değiştirip betiği tekrar çalıştırmak bir şey
değiştirmez. Bunun pratikteki tek örneği tablo silme ayarıdır
(`drop-with-purge.enabled`): kapalıysa Polaris, Trino'nun `DROP TABLE` isteğini `403` ile
reddeder. Var olan bir kurulumda açmak için:

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
unset CLIENT_ID CLIENT_SECRET
echo "katalog ozelligi guncellendi"
```

**Beklenen çıktı** (`polaris catalogs update` başarılı olunca bir şey yazmaz; görünen tek
satır son komuttur):

```text
katalog ozelligi guncellendi
```

**Ters giderse:**

- `401 Unauthorized` → `polaris-root` Secret'ındaki kimlik değişmiştir.
- `Connection refused` → bağlantı henüz hazır değildir; `sleep 5` yerine `sleep 10` yazıp
  tekrar deneyin.
- `kill: No such process` → bağlantı zaten kapanmıştır; zararsızdır.

---

## 3. Superset'e Trino bağlantısını ekleyin

Superset'in Trino bağlantısı depoda hazır bir dosya olarak gelir ve Superset pod'una
bağlanır. Superset bağlantıları kendi veritabanında tuttuğu için bu dosya kurulumdan sonra
**bir kez** içe aktarılır. Bağlantının parolası dosyada değildir; Superset onu bir ortam
değişkeninden okur.

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" rollout status deploy/superset-web-server --timeout=600s
oc -n "$LAKEHOUSE_NS" exec deploy/superset-web-server -- \
  superset legacy-import-datasources -p /app/configs/trino.yaml
```

**Beklenen çıktı** (aranacak satırlar; arada Superset'in açılış günlüğü akar):

```text
deployment "superset-web-server" successfully rolled out
INFO:superset.commands.dataset.importers.v0:Importing dataset from file trino.yaml
INFO:superset.commands.dataset.importers.v0:Importing 1 databases
```

Bundan sonra SQL Lab'de `lakehouse` veritabanı görünür. Bağlantıdan veri değiştirilemez;
`CREATE TABLE AS` ile tablo yalnız `sandbox` şemasında yaratılabilir. Komut tekrar
çalıştırılabilir: aynı bağlantıyı günceller, ikinci bir bağlantı açmaz.

**Ters giderse:**

- `No such command 'legacy-import-datasources'` → komutu yanlış pod'da çalıştırmışsınızdır;
  yalnız `deploy/superset-web-server` bu komutu tanır.
- `FileNotFoundError` → bağlantı dosyası pod'a bağlanmamıştır;
  `oc -n "$LAKEHOUSE_NS" get cm superset-datasources` ile varlığına bakın.
- Komutun başındaki `legacy-` kısmını silmeyin: öneksiz `import-datasources` bu dosyayı
  kabul etmez.

---

## 4. "Tablo sağlığı" panosunu içe aktarın

Ürünle hazır bir Superset panosu gelir: bütün tabloların satır sayısını, dosya sayısını,
boyutunu, ortalama dosya boyutunu ve son yazma zamanını tek ekranda gösterir. Verisini
bakım işi saatte bir `ops.table_health` tablosuna yazar; pano kurumun tablolarını
kendiliğinden gösterir, uyarlanacak bir şey yoktur
([50-isletme/veri-metrikleri.md](50-isletme/veri-metrikleri.md) §4). Pano kurulumdan sonra
**bir kez** içe aktarılır.

İlk pipeline eklenip ilk saatlik ölçüm alınana kadar pano boş görünür; bu normaldir.

### 4.1 Yönetici hesabının Superset'te olduğunu doğrulayın

İçe aktarma, panoya bir **sahip** ister ve bu, Superset'te var olan bir kullanıcı olmalıdır.
Superset kullanıcıları ilk girişte yaratılır; yani `lakehouse-admins` grubundaki bir
yöneticinin Superset'e **en az bir kez** girmiş olması gerekir (§5.3).

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" exec deploy/superset-web-server -- \
  superset fab list-users 2>/dev/null | tail -5
```

**Beklenen çıktı:** rolünde `Admin` geçen en az bir satır (e-posta adresleri burada
kısaltılmıştır):

```text
Loaded your LOCAL configuration at [/app/pythonpath/superset_config.py]
List of users
-------------
username:lanalyst | email:… | role:[Public, Alpha, Gamma]
username:ladmin | email:… | role:[Admin, Public, Gamma]
```

**Ters giderse:**

- `List of users` başlığının altı boşsa henüz kimse Superset'e girmemiştir. Önce §5.3'ü
  yapın, sonra buraya dönün.
- `Refusing to start due to insecure SECRET_KEY` → `superset-secret` Secret'ı eksiktir
  ([90-referans/secret-listesi.md](90-referans/secret-listesi.md)).

### 4.2 Panoyu içe aktarın

Panonun dosyaları Superset pod'unda `/app/assets` klasöründedir. Aşağıdaki komutlar önce
sahip olacak yöneticinin adını bulur, sonra dosyaları bir zip paketinde toplar ve paketi
içe aktarır. Komutların hepsini birlikte kopyalayıp yapıştırın:

`[bastion]`

```bash
SUPERSET_ADMIN=$(oc -n "$LAKEHOUSE_NS" exec deploy/superset-web-server -- \
  superset fab list-users 2>/dev/null | grep 'role:.*Admin' \
  | sed -n 's/^username:\([^ ]*\).*/\1/p' | head -1)
echo "sahip: $SUPERSET_ADMIN"
oc -n "$LAKEHOUSE_NS" exec -i deploy/superset-web-server -- python3 - <<'PY'
import pathlib, shutil
src = pathlib.Path('/app/assets'); dst = pathlib.Path('/tmp/tablo-sagligi')
shutil.rmtree(dst, ignore_errors=True)
for f in src.glob('tablo-sagligi__*'):
    p = dst / f.name.split('__', 1)[1].replace('__', '/')
    p.parent.mkdir(parents=True, exist_ok=True); p.write_text(f.read_text())
shutil.make_archive('/tmp/tablo-sagligi', 'zip', '/tmp', 'tablo-sagligi')
print('zip hazır')
PY
oc -n "$LAKEHOUSE_NS" exec deploy/superset-web-server -- \
  superset import-dashboards -p /tmp/tablo-sagligi.zip -u "$SUPERSET_ADMIN"
```

**Beklenen çıktı** (`sahip:` satırında sizin yöneticinizin kullanıcı adı yazar; içe
aktarma komutu ardından birkaç `SAWarning` uyarısı yazabilir, bunlar zararsızdır):

```text
sahip: ladmin
zip hazır
```

İçe aktarma, Superset'te aynı panoyu bulursa üzerine yazar; komutlar tekrar
çalıştırılabilir. Pano §3'te eklenen `lakehouse` bağlantısını kullanır, bu yüzden sıra
önemlidir: önce §3, sonra bu adım.

**Ters giderse:**

- `sahip:` satırı boşsa yönetici Superset'e henüz girmemiştir (§4.1). Girdikten sonra
  komutları tekrarlayın; ya da adı elle verin (`SUPERSET_ADMIN=kullanici.adi`) ve yalnız son
  komutu çalıştırın.
- `User not found` → verilen kullanıcı Superset'e hiç girmemiştir.
- Komutlar hatasız bitti ama pano görünmüyorsa zip oluşmamıştır: ikinci komuttaki
  `exec -i` kısmındaki `-i` atlanmış olabilir.

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

**Beklenen çıktı:** listede `Tablo sağlığı` vardır. Aşağıdaki çıktı, ayrıca bir kabul
demosu panosu yüklenmiş bir kurulumdandır; yeni kurulumda yalnız `Tablo sağlığı` görünür:

```text
[('Kabul demosu — şartname 9.18', 'kabul-demosu'), ('Tablo sağlığı', 'tablo-sagligi')]
```

Arayüzde: **Dashboards → Tablo sağlığı**. Grafiklerin nasıl okunacağı
[50-isletme/veri-metrikleri.md](50-isletme/veri-metrikleri.md) §4'tedir.

**Ters giderse:** boş liste `[]` görürseniz içe aktarma çalışmamıştır; §4.2'nin çıktısına
dönün.

---

## 5. Arayüzlere ilk giriş

Kullanıcılar davet edilmeden önce, dört arayüzün de Active Directory hesabıyla açıldığını
kurulumu yapan kişi doğrular.

### 5.1 Adresleri yazdırın

`[bastion]`

```bash
for c in keycloak trino superset jupyterhub zeppelin; do
  echo "https://$c-$LAKEHOUSE_NS.$APPS_DOMAIN"
done
```

**Beklenen çıktı** (kendi `$APPS_DOMAIN` değerinizle):

```text
https://keycloak-lakehouse.apps.ocp.example.net
https://trino-lakehouse.apps.ocp.example.net
https://superset-lakehouse.apps.ocp.example.net
https://jupyterhub-lakehouse.apps.ocp.example.net
https://zeppelin-lakehouse.apps.ocp.example.net
```

`platform/values/site/glue.yaml` içinde kurumsal bir adres (`hostname`) verdiyseniz gerçek
adresleri `oc -n "$LAKEHOUSE_NS" get route` ile alın. Bütün adresler ve portlar:
[90-referans/port-ve-servisler.md](90-referans/port-ve-servisler.md).

**Ters giderse:** adresler doğru görünüyor ama tarayıcı açmıyorsa ya Route'lar
yaratılmamıştır ya da adres DNS'te çözülmüyordur ([30-kurulum](30-kurulum.md) §7.3).

### 5.2 Hangi arayüze kim girer

| Arayüz | Adres | Giriş | Hangi AD grupları girer | Girişten sonra ne görülür |
|---|---|---|---|---|
| Trino Web UI | `https://trino-$LAKEHOUSE_NS.$APPS_DOMAIN/ui/` | Keycloak | `lakehouse-admins`, `lakehouse-analysts` | sorgu listesi; sağ üstte kullanıcı adınız |
| Superset | `https://superset-$LAKEHOUSE_NS.$APPS_DOMAIN` | Keycloak | üçü de | **Sign in with keycloak** düğmesi, sonra pano listesi |
| JupyterHub | `https://jupyterhub-$LAKEHOUSE_NS.$APPS_DOMAIN` | Keycloak | üçü de | **Start My Server** → not defteri (ilk açılış ~5 dakika) |
| Zeppelin | `https://zeppelin-$LAKEHOUSE_NS.$APPS_DOMAIN` | doğrudan AD | üçü de | kullanıcı adı ve parola formu, sonra not listesi |

**Zeppelin Keycloak kullanmaz;** kullanıcı adı ve parola doğrudan Active Directory'ye
sorulur. `lakehouse-*` gruplarının hiçbirinde olmayan bir hesap, parolası doğru olsa bile
giremez (`401`).

Gruplar ve roller: Superset'te `lakehouse-admins` → `Admin`, `lakehouse-analysts` →
`Alpha`, `lakehouse-users` → `Gamma`. JupyterHub'a üç grup da girer; hub yöneticisi
yalnız `lakehouse-admins` grubundakilerdir.

### 5.3 Superset'e yönetici olarak girin

Bu giriş §4.1'in ön koşuludur: Superset yöneticisi ancak ilk girişte yaratılır.

1. `https://superset-$LAKEHOUSE_NS.$APPS_DOMAIN` adresini açın.
2. **Sign in with keycloak** düğmesine basın ve AD hesabınızla girin.
3. Sağ üstte **Settings** menüsünün göründüğünü kontrol edin (yalnız `Admin` rolünde
   görünür).
4. **SQL → SQL Lab** ekranında veritabanı listesinden `lakehouse` seçilebilmelidir.

**Beklenen sonuç:** SQL Lab'de `select 1` sorgusu sonuç döndürür; **Settings → List
Users** listesinde hesabınız `Admin` rolüyle görünür.

**Ters giderse:**

- Giriş sayfasında keycloak düğmesi yoksa Keycloak istemci ayarı eksiktir
  ([30-kurulum](30-kurulum.md) §5.6).
- Giriş oluyor ama **Settings** görünmüyorsa hesabınız `lakehouse-admins` grubunda
  değildir. Grup üyeliği düzeltildikten sonra çıkış yapıp yeniden girin; rol her girişte
  yeniden hesaplanır.
- `redirect_uri` hatası → Keycloak'ta Superset'in adresi kayıtlı değildir. Kurumsal bir
  adres kullanıyorsanız [30-kurulum](30-kurulum.md) §8 bölümüne bakın.

### 5.4 Trino, JupyterHub ve Zeppelin

1. **Trino Web UI** — `https://trino-$LAKEHOUSE_NS.$APPS_DOMAIN/ui/` adresi sizi
   Keycloak'a yönlendirir; dönüşte sorgu listesi açılır. Tarayıcı sertifika uyarısı
   veriyorsa kurumun tarayıcıları lakehouse'un iç sertifika otoritesine güvenmiyordur;
   çözümü [30-kurulum](30-kurulum.md) §5.11'dedir.
2. **JupyterHub** — girişten sonra **Start My Server**'a basın. İlk açılışta not defteri
   imajı indirilir (~4,5 dakika). Sonra her açılışta `pyiceberg` ve `trino` paketleri
   kurulur (~1 dakika, PyPI erişimi gerekir). Her kullanıcıya kişisel bir disk yaratılır
   (varsayılan 10 Gi). Not defterinde şu ortam değişkenleri hazırdır: `POLARIS_URI`,
   `POLARIS_CREDENTIAL`, `TRINO_HOST` ve `S3_ENDPOINT`.
3. **Zeppelin** — girişten sonra yeni bir not açın ve `%jdbc` ile bir sorgu çalıştırın.
   İlk sorguda Trino sürücüsü indirilir (yaklaşık bir dakika, sonra diskte kalır). Bu
   sırada `Interpreter Setting 'jdbc' is not ready … DOWNLOADING_DEPENDENCIES` görürseniz
   bekleyip tekrar çalıştırın.

#### Not defterinden ilk sorgu

Aşağıdaki iki hücre, bir not defterinin lakehouse'a bağlanmasının tam yoludur;
analistlere başlangıç örneği olarak verilebilir. Ortam değişkenleri hazırdır, kullanıcı
bir şey yazmaz.

`[pod]` (JupyterHub not defteri hücresi)

```python
# PyIceberg -> Polaris kataloğu (yazma yalnız sandbox ad alanında)
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
# Trino: kendi kimliğinizle -> satır ve kolon kuralları uygulanır.
# Tarayıcıda Keycloak onayı istenir.
import os, trino
conn = trino.dbapi.connect(host=os.environ["TRINO_HOST"], port=8443, http_scheme="https",
                           verify="/etc/lakehouse-ca/tls.crt",
                           auth=trino.auth.OAuth2Authentication(), catalog="lakehouse")
cur = conn.cursor(); cur.execute("select * from shop.orders limit 10"); cur.fetchall()
```

`shop.orders` yerine kendi Silver tablolarınızdan birinin adını yazın.

`verify=` satırı lakehouse'un iç sertifikasını gösterir ve yazılması gerekir (§5.6, 2.
madde). Tarayıcı açılamayan otomatik işlerde `OAuth2Authentication` yerine bir servis
hesabıyla `trino.auth.BasicAuthentication` kullanılır; o durumda satır ve kolon kuralları
kullanıcıya göre işlemez (§5.6, 1. madde).

**Beklenen sonuç:** üç arayüzde de kendi AD kullanıcı adınızla oturum açılır; JupyterHub'da
iki hücre sonuç döndürür; Zeppelin'de `%jdbc` sorgusu satır yazar.

**Ters giderse:**

- JupyterHub sunucusu zaman aşımıyla kapanırsa imaj indirmesi 20 dakikayı geçmiştir;
  kapalı ağda kurumun imaj aynası gerekir.
- Zeppelin'de `401` alan kullanıcı `lakehouse-*` gruplarının hiçbirinde değildir.
- Not defterinden Trino'ya bağlanırken `SSLCertVerificationError` alırsanız `verify=`
  satırının yazıldığını kontrol edin.

### 5.5 Grup değişikliği ne zaman geçerli olur

Trino grupları doğrudan Active Directory'den okur. AD'de yapılan bir üyelik değişikliği
kullanıcının bir sonraki oturumunda geçerli olur. Superset'te rol her girişte yeniden
hesaplanır; değişikliğin görünmesi için kullanıcının çıkış yapıp yeniden girmesi yeterlidir.
Kullanıcı ekleme, grup ve rol eşlemesi, satır ve kolon kuralı örnekleri
[50-isletme/kullanici-ve-yetki.md](50-isletme/kullanici-ve-yetki.md) sayfasındadır.

### 5.6 İlk günden bilinmesi gereken dört davranış

Aşağıdakiler bilerek böyle yapılmıştır; eksik ayar sanılıp değiştirilmemelidir.

1. **Kullanıcı kimliği Trino'ya yalnız doğrudan bağlantılarda gider.** Superset ve
   Zeppelin Trino'ya ortak bir **servis hesabıyla** bağlanır; bu araçlarda satır ve kolon
   kuralları uygulanmaz. Erişim araçların kendi içinde sınırlanır (Superset rolleri,
   Zeppelin not izinleri). Kullanıcının kendi kimliğiyle sorgu çalıştırması için Trino Web
   UI, Keycloak destekli bir JDBC istemcisi ya da not defterindeki
   `trino.auth.OAuth2Authentication` kullanılır.
2. **İç sertifika otoritesi her yerde varsayılan yapılmamıştır** (Superset ve JupyterHub).
   Tek bir otoriteyi her bağlantıya dayatmak, Keycloak ve PyPI gibi dış HTTPS
   bağlantılarını bozardı. Otoritenin sertifikası `/etc/lakehouse-ca/tls.crt` dosyasındadır
   ve Trino'ya bağlanırken açıkça verilir.
3. **Zeppelin'in Trino bağlantı ayarı arayüzden değiştirilir.** `zeppelin-interpreter`
   Secret'ı yalnız ilk açılışta kullanılır; Zeppelin bundan sonra ayarı kendi diskinde
   tutar. Secret'ı güncellemek tek başına etkisizdir; ayar **Interpreter → jdbc**
   ekranından değiştirilir.
4. **Zeppelin notları varsayılan olarak yalnız sahibine açıktır.** Paylaşım not bazında
   arayüzden verilir. Superset'in **Alerts & Reports** özelliği de varsayılan olarak
   kapalıdır.

---

## 6. İzlemeyi doğrulayın

Lakehouse kendi Prometheus'unu kurmaz; OpenShift'in **kullanıcı iş yükü izlemesi**
`$LAKEHOUSE_NS` ad alanındaki izleme tanımlarını kendiliğinden toplar
([20-on-kosullar](20-on-kosullar.md) madde 9.1). Bu izleme kapalıysa tanımlar kümede olsa
da kimse okumaz ve alarmların hiçbiri çalışmaz.

### 6.1 İzleme tanımları kümede mi

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" get podmonitor,servicemonitor,prometheusrule
```

**Beklenen çıktı** (`AGE` sütunu sizde farklıdır):

```text
NAME                                                         AGE
podmonitor.monitoring.coreos.com/kafka-resources-metrics     3d4h
podmonitor.monitoring.coreos.com/spark-operator-podmonitor   3d4h

NAME                                           AGE
servicemonitor.monitoring.coreos.com/polaris   3d4h

NAME                                             AGE
prometheusrule.monitoring.coreos.com/lakehouse   3d4h
```

`kafka-resources-metrics` Kafka'yı, Kafka Connect'i ve kafka-exporter'ı toplar;
`spark-operator-podmonitor` Spark işlerini, `polaris` katalog sunucusunu toplar.
`prometheusrule/lakehouse` altı alarmı içerir: `LakehouseConnectTaskFailed`,
`LakehouseSinkStalled`, `LakehouseSilverMergeStale`, `LakehouseSparkScheduledRunFailed`,
`LakehouseSparkRunTooLong` ve `LakehouseDlqRecords`.

**Ters giderse:**

- `No resources found` → izleme ayarı kapalıdır ya da ArgoCD `glue` uygulamasını henüz
  bitirmemiştir.
- `the server doesn't have a resource type "podmonitor"` → kümede kullanıcı iş yükü
  izlemesi hiç açılmamıştır.

### 6.2 İzleme verisi toplanıyor mu

OpenShift konsolunda **Observe → Targets** sayfasını açın ve ad alanı süzgecine
`$LAKEHOUSE_NS` değerini yazın. Hepsi **Up** olmalıdır:

- `lakehouse/kafka-resources-metrics` (Kafka broker'ları, Kafka Connect işçileri ve
  kafka-exporter)
- `lakehouse/spark-operator-podmonitor` (2 hedef)
- konsolda `polaris-mgmt` adıyla görünen Polaris hedefi

Alarmları **Observe → Alerting → Alerting rules** sayfasında ad alanına göre süzerek
görürsünüz. Yeni bir kurulumda altısının da **Inactive** olması beklenir: alarmlar veri
yokken çalmaz.

Aynı bilgiyi komut satırından da alabilirsiniz:

`[bastion]`

```bash
H=$(oc -n openshift-monitoring get route thanos-querier -o jsonpath='{.spec.host}')
curl -sS --cacert ~/router-ca.crt -H "Authorization: Bearer $(oc whoami -t)" \
  --data-urlencode "query=count by (job) (up{namespace=\"$LAKEHOUSE_NS\"} == 1)" \
  "https://$H/api/v1/query" | jq -r '.data.result[] | "\(.metric.job) \(.value[1])"'
```

**Beklenen çıktı** (satırların sırası değişebilir; ilk satırdaki sayı Kafka broker'ı ve
Connect işçisi sayınıza göre değişir):

```text
lakehouse/kafka-resources-metrics 5
polaris-mgmt 1
lakehouse/spark-operator-podmonitor 2
```

`~/router-ca.crt` dosyası [30-kurulum](30-kurulum.md) §7.3'te alınmıştır. Kümenin
router'ı kurumsal bir sertifika kullanıyorsa onun yerine kurumun kök sertifikasını verin.

**Ters giderse:** hedefler hiç görünmüyorsa kullanıcı iş yükü izlemesi açık değildir
([20-on-kosullar](20-on-kosullar.md) madde 9.1). Bir hedef `Down` ise ilgili pod'un
çalıştığını `oc -n "$LAKEHOUSE_NS" get pods` ile kontrol edin. Alarmların anlamı ve
eşikleri [50-isletme/izleme-ve-alarmlar.md](50-isletme/izleme-ve-alarmlar.md)
sayfasındadır.

---

## 7. Kabul testini çalıştırın

Şartname maddelerinin kanıtı kümede çalışan testlerle verilir. `scripts/acceptance.sh`
dokuz test yolunu sırayla çalıştırır ve sonunda bir özet yazar. Betik kurulum yapmaz;
kurulu bir kümede çalışır.

**Ön koşullar:**

- Ad alanı `lakehouse` olmalıdır; test dosyaları bu ada göre yazılmıştır
  ([90-referans/kabul-testleri.md](90-referans/kabul-testleri.md) §1).
- Test, demo kaynaklarını (`sources`, `pipelines`, `nginx.enabled`) kullanır. Bunlar
  üretim ayarlarında kapalıdır; kabul testi bu yüzden ya demo ayarlarıyla kurulmuş bir
  doğrulama kümesinde ya da kurumun kaynakları tanımlandıktan sonra yapılır. Madde–kanıt
  tablosu, süreler ve sık durumlar:
  [90-referans/kabul-testleri.md](90-referans/kabul-testleri.md).

`[bastion]`

```bash
PATH="$PWD/.venv/bin:$PATH" PROM_STS=prometheus-user-workload \
  PROM_SVC=prometheus-user-workload GRAFANA_SKIP=1 \
  scripts/acceptance.sh --ns "$LAKEHOUSE_NS" \
  --mon-ns openshift-user-workload-monitoring --velero-ns openshift-adp
```

Komuttaki üç ortam değişkeni ve iki ayar OpenShift içindir: kullanıcı iş yükü izlemesinin
Prometheus'u farklı adlandırılır, Grafana yoktur (`GRAFANA_SKIP=1` yalnız pano
denetimlerini atlar) ve ad alanı yedeği OADP ile alınır (`openshift-adp`).

**Beklenen çıktı** (son bölüm; her satır bir test yolunun sonucudur):

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
KABUL: 9/9 yol geçti (ns=lakehouse, mon-ns=openshift-user-workload-monitoring, velero-ns=openshift-adp, argocd-ns=openshift-gitops)
```

**Ters giderse:** bir yol başarısız olursa betik orada durur ve özet `KABUL BAŞARISIZ`
satırıyla biter. Yola göre çözümler
[90-referans/kabul-testleri.md](90-referans/kabul-testleri.md) sayfasındadır.

> **Testten sonra zamanlı Spark işlerini geri açın.** Test bu işleri askıya alır ve
> kendiliğinden geri açmaz ([90-referans/kabul-testleri.md](90-referans/kabul-testleri.md)).

---

## Kontrol listesi — kurulum tamam

Hepsi işaretliyse kurulum teslim edilebilir:

- [ ] `scripts/polaris-setup.sh` hatasız bitti; `lakehouse` kataloğu ve `setup.yaml`'daki
      ad alanları yaratıldı.
- [ ] Dört Secret kümede: `polaris-connect`, `polaris-spark`, `polaris-trino`,
      `polaris-notebooks`.
- [ ] `trino` uygulaması `Healthy`; `deploy/trino-coordinator` açıldı.
- [ ] Superset'te `lakehouse` veritabanı SQL Lab'de görünüyor.
- [ ] "Tablo sağlığı" panosu içe aktarıldı ve açılıyor.
- [ ] Dört arayüze de (Trino UI, Superset, JupyterHub, Zeppelin) AD hesabıyla girildi.
- [ ] `lakehouse-admins` grubundaki yönetici Superset'te `Admin` rolüyle görünüyor.
- [ ] `oc get podmonitor,servicemonitor,prometheusrule` iki PodMonitor, bir ServiceMonitor
      ve bir PrometheusRule listeliyor.
- [ ] Konsolda **Observe → Targets** hedefleri `Up`; altı alarm listede ve hiçbiri
      çalmıyor.
- [ ] Kabul testi çalıştırıldıysa `KABUL: 9/9 yol geçti` satırı alındı ve zamanlı Spark
      işleri geri açıldı.
- [ ] `install/lakehouse.env` kurumun parola kasasına alındı ya da silindi
      ([90-referans/secret-listesi.md](90-referans/secret-listesi.md)).

## Sonraki bölüm

Kurulumdan sonraki günlük işler `docs/50-isletme/` klasöründedir. İlk okunacak sayfa her
değişikliğin izlediği yolu anlatır:
[50-isletme/degisiklik-nasil-uygulanir.md](50-isletme/degisiklik-nasil-uygulanir.md).
Diğer sayfaların okuma sırası o sayfanın sonundadır. Başvuru tabloları
`docs/90-referans/` klasöründedir:
[kabul testleri](90-referans/kabul-testleri.md),
[portlar ve servisler](90-referans/port-ve-servisler.md),
[sürümler ve lisanslar](90-referans/surumler-ve-lisanslar.md),
[oc hızlı başvuru](90-referans/oc-hizli-basvuru.md),
[Secret listesi](90-referans/secret-listesi.md),
[ayar anahtarları](90-referans/values-anahtarlari.md),
[teslim öncesi kontrol listesi](90-referans/pre-ship-kontrol-listesi.md).
