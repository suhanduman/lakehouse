# 30 — Kurulum

**Bu bölümde:** ürünün sıfırdan kurulması — depoyu kurumun kendi Git sunucusuna alma,
ArgoCD'ye depo kimliği verme, site değerlerini doldurma, Secret'ları yaratma,
`bootstrap/bootstrap.sh` ile başlatma ve kurulumun doğrulanması. Bölüm sonunda bütün
bileşenler ayakta olur; Polaris kataloğu, ilk giriş ve kabul testi bir sonraki bölümdedir.
**Süre:** komut çalıştırma 1–1,5 saat; ardından bileşenlerin ayağa kalkması **40–60
dakika** (ağın hızına bağlı; Kafka Connect imajı kümede üretilir, tek başına ~10 dakika).
**Gereken yetki:** kümede **cluster-admin**, `$ARGOCD_NS` ve `$LAKEHOUSE_NS` ad
alanlarında yazma, kurumun Git sunucusunda depo açma/itme yetkisi.
**Nerede çalıştırılır:** `[bastion]` — `oc`, `git`, `openssl`, `htpasswd`, `python3` kurulu
ve kümeye `oc login` ile girilmiş yönetim makinesi.

> **Başlamadan önce [20-on-kosullar](20-on-kosullar.md) kontrol listesinin on maddesi de
> işaretli olmalıdır.** Özellikle madde 2 (GitOps operatörü), madde 5 (`connect-push`
> Secret'ı ve `$LAKEHOUSE_NS` ad alanı) ve madde 6 (AD grupları) bu bölümün girdisidir.
> Eksik bir ön koşulla başlanan kurulum yarıda kalır; geri almak yeniden kurmaktan
> pahalıdır.

`htpasswd` komutu RHEL/Fedora'da `httpd-tools` paketiyle gelir
(`sudo dnf install -y httpd-tools`), Debian/Ubuntu'da `apache2-utils` ile.

**Kurulumun sırası neden önemli:** ArgoCD Git'teki hâli kümeye uygular. Secret'lar Git'e
girmediği için **kümede önceden** bulunmalıdır; eksik Secret'la başlatılan sync, pod'ları
`CreateContainerConfigError` durumunda bekletir. Bu yüzden akış şudur:

```text
Git kopyası -> ArgoCD depo kimliği -> site değerleri (Git'e it) -> Secret'lar (kümeye)
            -> bootstrap -> doğrulama
```

---

## 1. Değişkenleri yükleyin

[10-planlama](10-planlama.md) §5'te kopyalayıp doldurduğunuz dosyayı her yeni terminal
oturumunda yüklemeniz gerekir. **Bu bölümdeki bütün komutlar aynı terminal oturumunda
çalıştırılmalıdır:** Adım 5.7'de üretilen parola Adım 5.10'da yeniden kullanılır.

`[bastion]`

```bash
cd ~/lakehouse                      # depoyu `git clone` ile nereye indirdiyseniz orası
set -a; . install/lakehouse.env; set +a
for v in GIT_REPO_URL ARGOCD_NS LAKEHOUSE_NS APPS_DOMAIN INTERNAL_REGISTRY \
         S3_ENDPOINT S3_BUCKET_DATA S3_BUCKET_BACKUP S3_REGION \
         S3_ACCESS_KEY S3_SECRET_KEY S3_BACKUP_ACCESS_KEY S3_BACKUP_SECRET_KEY \
         LDAP_URL LDAP_BIND_DN LDAP_BIND_PASSWORD LDAP_USERS_DN LDAP_GROUPS_DN \
         LDAP_CA_FILE AD_UPN_SUFFIX; do
  eval "deger=\${$v:-}"
  [ -n "$deger" ] || echo "EKSIK: $v"
done
echo "degisken denetimi bitti"
```

**Beklenen çıktı** (tek satır; `EKSIK:` ile başlayan hiçbir satır olmamalıdır):

```text
degisken denetimi bitti
```

`STORAGE_CLASS` ve `KEYCLOAK_ADMIN_PASSWORD` bilerek listede değildir: ikisi de boş
kalabilir (kümenin varsayılan StorageClass'ı kullanılır, Keycloak parolası üretilir).

**Ters giderse:** `EKSIK:` satırı gördüğünüz her değişken için
[10-planlama](10-planlama.md) §5'teki çalışma sayfasına dönün ve değeri ilgili ekipten
alın. `No such file or directory` → dosya kopyalanmamıştır
(`cp install/lakehouse.env.example install/lakehouse.env`).

---

## 2. Depoyu kurumun Git sunucusuna kopyalayın

**Neden:** ArgoCD kurulumu ve bundan sonraki **bütün** değişiklikleri Git'ten okur. Ürünün
genel deposu değil, **sizin kopyanız** tek doğru kaynaktır: site değerlerini oraya
yazacaksınız. Boş bir depo Git sunucunuzda önceden açılmış olmalıdır (`$GIT_REPO_URL`).

`[bastion]`

```bash
git clone --mirror https://github.com/suhanduman/lakehouse.git ~/lakehouse-mirror.git
git -C ~/lakehouse-mirror.git push --mirror "$GIT_REPO_URL"
git -C ~/lakehouse remote set-url origin "$GIT_REPO_URL"
git -C ~/lakehouse fetch origin
git -C ~/lakehouse status -sb | head -1
```

**Beklenen çıktı** (örnek — ilk iki komut ağ hızına göre 10–60 saniye sürer):

```text
Cloning into bare repository '/home/kurulum/lakehouse-mirror.git'...
To ssh://git.kurum.example.net/veri/lakehouse.git
 * [new branch]      main -> main
## main...origin/main
```

Son satırda `ahead`/`behind` yazmamalıdır: yerel kopyanız ile kurumun deposu aynı
noktadadır.

**Ters giderse:** `Permission denied (publickey)` → Git sunucusunda itme yetkiniz yok ya
da SSH anahtarınız tanımlı değil; `git ls-remote "$GIT_REPO_URL"` ile ayrı ayrı sınayın.
`remote rejected` / `pre-receive hook declined` → hedef depo boş değil ya da korumalı
daldır; Git yöneticisinden boş bir depo isteyin. `Repository not found` → `$GIT_REPO_URL`
yanlış yazılmıştır. Kurum genel internete kapalıysa depoyu internete açık bir makinede
klonlayıp `git bundle` ile taşıyın.

---

## 3. ArgoCD'ye depo kimliği verin

**Neden:** ArgoCD `$GIT_REPO_URL` adresini okuyabilmelidir. Kimlik bilgisi, `$ARGOCD_NS`
ad alanında `argocd.argoproj.io/secret-type: repository` etiketli bir Secret ile verilir.
Aşağıdaki komut yalnız bu kurulum için bir SSH anahtar çifti üretir; **açık anahtarı** Git
sunucunuzda deponun dağıtım anahtarı (deploy key) listesine eklemeniz gerekir; salt okuma
yetkisi yeter.

### 3.1 Anahtarı üretin ve açık kısmını Git sunucusuna tanıtın

`[bastion]`

```bash
ssh-keygen -t ed25519 -N "" -C "argocd-lakehouse" -f "$HOME/.ssh/lakehouse_deploy"
cat "$HOME/.ssh/lakehouse_deploy.pub"
```

**Beklenen çıktı** (örnek — son satırı Git sunucusunun dağıtım anahtarı ekranına
yapıştırın):

```text
Generating public/private ed25519 key pair.
Your identification has been saved in /home/kurulum/.ssh/lakehouse_deploy
ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIKk3s2vQ0m3Q6l8y9m1Yb2d4e6f8g0h2 argocd-lakehouse
```

**Ters giderse:** dosya zaten varsa `ssh-keygen` üzerine yazmayı sorar; var olan anahtarı
kullanacaksanız bu adımı atlayın. `$GIT_REPO_URL` HTTPS ile başlıyorsa SSH anahtarı
gerekmez — Adım 3.2'deki HTTPS seçeneğini kullanın.

### 3.2 Secret'ı yaratın

`[bastion]`

```bash
oc -n "$ARGOCD_NS" create secret generic lakehouse-repo \
  --from-literal=type=git \
  --from-literal=url="$GIT_REPO_URL" \
  --from-file=sshPrivateKey="$HOME/.ssh/lakehouse_deploy"
oc -n "$ARGOCD_NS" label secret lakehouse-repo argocd.argoproj.io/secret-type=repository
```

**Beklenen çıktı:**

```text
secret/lakehouse-repo created
secret/lakehouse-repo labeled
```

**HTTPS kullanıyorsanız** `--from-file=sshPrivateKey=...` satırı yerine kullanıcı adı ve
jeton verin (jeton salt okuma yetkisiyle yeter):

`[bastion]`

```bash
oc -n "$ARGOCD_NS" create secret generic lakehouse-repo \
  --from-literal=type=git --from-literal=url="$GIT_REPO_URL" \
  --from-literal=username=argocd --from-literal=password="$GIT_HTTPS_TOKEN"
oc -n "$ARGOCD_NS" label secret lakehouse-repo argocd.argoproj.io/secret-type=repository
```

**Beklenen çıktı** (SSH varyantıyla aynı):

```text
secret/lakehouse-repo created
secret/lakehouse-repo labeled
```

**Ters giderse:** `$GIT_HTTPS_TOKEN` boşsa Secret parolasız yaratılır ve ArgoCD
`authentication required` der; jetonu doldurup Secret'ı silip yeniden yaratın. Git
sunucusu kullanıcı adı yerine yalnız jeton kabul ediyorsa `username` değerini o
sunucunun istediği sabit değere çevirin (çoğu sunucuda herhangi bir değer kabul edilir).

`$GIT_HTTPS_TOKEN` yalnız HTTPS kullananlar içindir; `install/lakehouse.env` dosyasında
vardır ve SSH kullanıyorsanız boş bırakılır.

**Ters giderse:** `AlreadyExists` → Secret vardır; silip
(`oc -n "$ARGOCD_NS" delete secret lakehouse-repo`) yeniden yaratın. Etiket eksik kalırsa
ArgoCD Secret'ı **hiç görmez** ve kök Application `repository not accessible` hatası
verir. ArgoCD `unknown host` derse Git sunucunuzun SSH ana bilgisayar anahtarını
`$ARGOCD_NS` ad alanındaki `argocd-ssh-known-hosts-cm` ConfigMap'ine ekleyin
(`ssh-keyscan git.kurum.example.net` çıktısı).

---

## 4. Site değerlerini doldurun

**Neden:** müşteriye özel her değer tek dizindedir: `platform/values/site/`. Anahtarların
tam listesi ve anlamları
[90-referans/values-anahtarlari.md](90-referans/values-anahtarlari.md) dosyasındadır.
Ürün varsayılanlarına (`platform/values/glue.yaml` ve yanındakiler) **dokunulmaz**.

### 4.1 Üç site dosyasını değişkenlerden doldurun

Şablonlardaki örnek değerler (`apps.ocp.example.net`, `https://s3.example.com`, örnek AD
DN'leri) tek komutla gerçek değerlerinizle değiştirilir.

`[bastion]`

```bash
cd ~/lakehouse
for f in glue trino jupyterhub; do
  sed -e "s#apps.ocp.example.net#$APPS_DOMAIN#g" \
      -e "s#https://s3.example.com#$S3_ENDPOINT#g" \
      -e "s#lakehouse-backups#$S3_BUCKET_BACKUP#g" \
      -e "s#ldaps://ad.example.com:636#$LDAP_URL#g" \
      -e "s#OU=Users,DC=example,DC=com#$LDAP_USERS_DN#g" \
      -e "s#OU=Groups,DC=example,DC=com#$LDAP_GROUPS_DN#g" \
      -e "s#CN=svc-lakehouse,OU=Service,DC=example,DC=com#$LDAP_BIND_DN#g" \
      -e "s#us-east-1#$S3_REGION#g" \
      -e "s#svc:5000/lakehouse/connect#svc:5000/$LAKEHOUSE_NS/connect#g" \
      -e "s#-lakehouse\.#-$LAKEHOUSE_NS.#g" \
      -e "s#polaris\.lakehouse\.svc#polaris.$LAKEHOUSE_NS.svc#g" \
      "platform/values/site/$f.yaml" > "platform/values/site/$f.new"
  mv "platform/values/site/$f.new" "platform/values/site/$f.yaml"
done
if [ -n "${STORAGE_CLASS:-}" ]; then
  sed -e "s#storageClass: \"\"#storageClass: \"$STORAGE_CLASS\"#g" \
      platform/values/site/glue.yaml > platform/values/site/glue.new
  mv platform/values/site/glue.new platform/values/site/glue.yaml
fi
grep -nE 'apps\.ocp\.example\.net|s3\.example\.com|ad\.example\.com|DC=example,DC=com' \
  platform/values/site/*.yaml || echo "ornek deger kalmadi"
```

**Beklenen çıktı:**

```text
ornek deger kalmadi
```

`$INTERNAL_REGISTRY` değerini varsayılandan (küme içi registry adresi) değiştirdiyseniz
`connect.buildImage` satırını elle düzeltin. İmaj etiketi (`:2.0.0`) ürünün sürümüdür ve
her yükseltmede artırılır; `:latest` **kullanılamaz**.

**Ters giderse:** `ornek deger kalmadi` yerine satırlar basılıyorsa bir değişken boş
kalmıştır (Adım 1'i tekrarlayın); basılan satırın `#` ile başlayıp başlamadığına da
bakın. Denetim bilerek **yalnız ürün şablonundaki dört literal yer tutucuyu** arar
(`apps.ocp.example.net`, `s3.example.com`, `ad.example.com`, `DC=example,DC=com`);
`example.net` geçen gerçek bir kurum alan adınız varsa yanlış alarm vermez.
Dosyaları bozduysanız `git checkout -- platform/values/site/` ile şablonlara dönüp
baştan başlayın.

### 4.2 Ad alanı adı `lakehouse` değilse

Yalnız `$LAKEHOUSE_NS` değerini değiştirdiyseniz gereklidir.

`[bastion]`

```bash
if [ "$LAKEHOUSE_NS" != "lakehouse" ]; then
  printf 'namespace: %s\n' "$LAKEHOUSE_NS" >> platform/values/site/glue.yaml
fi
grep -c "^namespace:" platform/values/site/glue.yaml
```

**Beklenen çıktı** (`$LAKEHOUSE_NS` varsayılansa `0`, değiştirdiyseniz `1`):

```text
0
```

**Ters giderse:** `1`'den büyük bir sayı görürseniz satır birden fazla eklenmiştir;
fazlalıkları silin — Helm son değeri alır ama dosyanın okunabilirliği bozulur.

### 4.3 Depolamada STS yoksa

[20-on-kosullar](20-on-kosullar.md) madde 4'te depolama ekibine sorduğunuz sorunun cevabı
"STS yok" ise geçici kimlik dağıtımı kapatılır.
**STS varsa bu adımı hiç çalıştırmayın.**

`[bastion]`

```bash
sed -e "s#vendedCredentials: true#vendedCredentials: false#" \
    platform/values/site/glue.yaml > platform/values/site/glue.new
mv platform/values/site/glue.new platform/values/site/glue.yaml
sed -e "s#vended-credentials-enabled=true#vended-credentials-enabled=false#" \
    platform/values/site/trino.yaml > platform/values/site/trino.new
mv platform/values/site/trino.new platform/values/site/trino.yaml
grep -n "vendedCredentials\|vended-credentials" platform/values/site/*.yaml
```

**Beklenen çıktı** (örnek — iki satır da `false`):

```text
platform/values/site/glue.yaml:12:  vendedCredentials: false
platform/values/site/trino.yaml:44:    iceberg.rest-catalog.vended-credentials-enabled=false
```

**Ters giderse:** iki dosya farklı değer taşıyorsa `scripts/check-site.sh` Adım 4.5'te
hata verir.

### 4.4 Polaris katalog tanımını düzenleyin

`platform/polaris/setup.yaml` Polaris sunucusunun kendi katalog tanımıdır ve site
dizininde değildir; depoda geliştirme kümesinin küme içi MinIO adresini taşır.

`[bastion]`

```bash
sed -e "s#http://minio.lakehouse.svc:9000#$S3_ENDPOINT#g" \
    -e "s#s3://lakehouse/#s3://$S3_BUCKET_DATA/#g" \
    -e "s#region: us-east-1#region: $S3_REGION#" \
    platform/polaris/setup.yaml > platform/polaris/setup.new
mv platform/polaris/setup.new platform/polaris/setup.yaml
grep -nE "endpoint|base_location|allowed_locations|region:" platform/polaris/setup.yaml
```

**Beklenen çıktı** (örnek):

```text
    default_base_location: s3://lakehouse/
    allowed_locations: [s3://lakehouse/]
    region: us-east-1
    endpoint: https://s3.example.com
    endpoint_internal: https://s3.example.com
```

STS yoksa aynı katalog bloğuna `sts_unavailable: true` satırını da ekleyin
(`endpoint_internal` satırının altına, aynı girinti ile).

**Ters giderse:** `s3://` satırları hâlâ `lakehouse` diyorsa veri bucket'ınızın adı zaten
`lakehouse`'tur, sorun yoktur. Girintiyi bozduysanız
`git checkout -- platform/polaris/setup.yaml` ile geri alın.

### 4.5 Tutarlılığı denetleyin

**Neden:** aynı değer (Keycloak adresi, S3 uç noktası, LDAP DN'leri) birden fazla dosyada
tekrarlanır — ArgoCD'nin çok kaynaklı Application'larında değer dosyaları arasında
şablonlama yoktur. Betik bu kopyaların birbirini tuttuğunu ve ürün satırlarının
eksilmediğini denetler.

`[bastion]`

```bash
scripts/check-site.sh
```

**Beklenen çıktı** (kind kümesinde alınan gerçek çıktı):

```text
check-site: OK
```

**Ters giderse:** her `HATA:` satırı hangi dosyada hangi değerin beklendiğini yazar;
[90-referans/values-anahtarlari.md](90-referans/values-anahtarlari.md) §5'teki kopya
tablosundan hangi değerin kaynak olduğunu bulup düzeltin. `UYARI:` satırı Polaris
dosyasının uç noktasının uyuşmadığını söyler — Adım 4.4'ü atlamışsınızdır;
bu **uyarı** kurulumu durdurmaz ama Polaris yanlış depolamayı gösterir.
`ModuleNotFoundError: yaml` → `pip3 install --user PyYAML`.

### 4.6 Git'e itin

`[bastion]`

```bash
git add platform/values/site platform/polaris/setup.yaml
git commit -m "site: kurulum degerleri"
git push origin main
```

**Beklenen çıktı** (örnek):

```text
[main 4f2a9c1] site: kurulum degerleri
 4 files changed, 31 insertions(+), 31 deletions(-)
To ssh://git.kurum.example.net/veri/lakehouse.git
   1c18c86..4f2a9c1  main -> main
```

`tls.caBundle` şimdilik boştur; Adım 5.11'de doldurulup **ikinci kez** itilecektir.

**Ters giderse:** `nothing to commit` → dosyalar değişmemiştir, Adım 4.1'i çalıştırmamış
olabilirsiniz. `Updates were rejected` → uzak dalda başka bir değişiklik vardır,
`git pull --rebase origin main` sonra tekrar itin. **`install/lakehouse.env` dosyasının
commit'e girmediğini** `git show --stat HEAD` ile doğrulayın; dosya `.gitignore` içinde
listelidir ama kontrol etmek ucuzdur.

---

## 5. Ad alanı ve Secret'lar

**Neden:** Secret'lar Git'e girmez; kurulumdan **önce** kümede bulunmalıdır. Tam liste,
her Secret'ı hangi bileşenin okuduğu ve rotasyon yordamları
[90-referans/secret-listesi.md](90-referans/secret-listesi.md) dosyasındadır.

Ad alanı [20-on-kosullar](20-on-kosullar.md) madde 5'te açıldı. Emin olmak için:

`[bastion]`

```bash
oc create namespace "$LAKEHOUSE_NS" --dry-run=client -o yaml | oc apply -f -
oc project "$LAKEHOUSE_NS"
```

**Beklenen çıktı** (örnek):

```text
namespace/lakehouse configured
Now using project "lakehouse" on server "https://api.ocp.example.net:6443".
```

**Ters giderse:** `forbidden` → ad alanı açma yetkiniz yoktur, cluster-admin ile girin.

### 5.1 `connect-push` — yalnız doğrulama

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" get secret connect-push -o go-template='{{.type}}{{"\n"}}'
```

**Beklenen çıktı:**

```text
kubernetes.io/dockerconfigjson
```

**Ters giderse:** `NotFound` → [20-on-kosullar](20-on-kosullar.md) madde 5'e dönün;
Secret o bölümde yaratılır. Tip `Opaque` görünüyorsa Secret yanlış komutla
yaratılmıştır: silip madde 5'teki `oc create secret docker-registry` komutunu tekrarlayın.

### 5.2 `s3-creds` — veri S3 anahtarları

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" create secret generic s3-creds \
  --from-literal=AWS_ACCESS_KEY_ID="$S3_ACCESS_KEY" \
  --from-literal=AWS_SECRET_ACCESS_KEY="$S3_SECRET_KEY"
```

**Beklenen çıktı:**

```text
secret/s3-creds created
```

**Ters giderse:** `AlreadyExists` → `oc -n "$LAKEHOUSE_NS" delete secret s3-creds` sonra
tekrarlayın. Anahtar **adları** sözleşmedir (`AWS_ACCESS_KEY_ID`,
`AWS_SECRET_ACCESS_KEY`): büyük/küçük harf dâhil birebir yazılmalıdır.

### 5.3 `backup-s3-creds` — yedek S3 anahtarları

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" create secret generic backup-s3-creds \
  --from-literal=AWS_ACCESS_KEY_ID="$S3_BACKUP_ACCESS_KEY" \
  --from-literal=AWS_SECRET_ACCESS_KEY="$S3_BACKUP_SECRET_KEY"
```

**Beklenen çıktı:**

```text
secret/backup-s3-creds created
```

**Ters giderse:** veri anahtarlarını buraya yazmayın: yedeklerin **ayrı** bir anahtar
çiftiyle korunması tasarımın parçasıdır. Yanlış yazdıysanız Secret'ı silip tekrarlayın;
aksi hâlde PostgreSQL yedekleri kurulumdan sonra sessizce başarısız olur ve hatayı ancak
geri dönüş denemesinde görürsünüz.

### 5.4 `polaris-root` — Polaris kök kimliği

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" create secret generic polaris-root \
  --from-literal=clientId=root \
  --from-literal=clientSecret="$(openssl rand -hex 24)"
```

**Beklenen çıktı:**

```text
secret/polaris-root created
```

**Ters giderse:** bu kimlik Polaris'in yönetim kimliğidir ve yalnız kurulum ile
`scripts/polaris-setup.sh` tarafından kullanılır; kimseye dağıtılmaz. Kaybederseniz
Secret'ı silip yeniden yaratmak **yetmez** (değer Polaris veritabanına ilk açılışta
yazılır), veritabanını yeniden kurmak gerekir.

### 5.5 `keycloak-admin` — Keycloak yönetici hesabı

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" create secret generic keycloak-admin \
  --from-literal=username=admin \
  --from-literal=password="${KEYCLOAK_ADMIN_PASSWORD:-$(openssl rand -hex 24)}"
oc -n "$LAKEHOUSE_NS" get secret keycloak-admin \
  -o jsonpath='{.data.password}' | base64 -d | head -c 8; echo " ... (ilk 8 karakter)"
```

**Beklenen çıktı** (örnek — parolayı **şimdi** kurumun parola kasasına kaydedin):

```text
secret/keycloak-admin created
9f3c1ab2 ... (ilk 8 karakter)
```

**Ters giderse:** `KEYCLOAK_ADMIN_PASSWORD` boşsa parola üretilir ve yalnız Secret'ta
kalır; tamamını okumak için son komuttaki `| head -c 8` kısmını kaldırın. Bu hesap
gündelik kullanım için değildir — kullanıcılar AD'den gelir, yönetici hesabı yalnız
Keycloak arayüzündeki bakım içindir.

### 5.6 `keycloak-clients` — client sırları ve AD bağlanma parolası

**Realm içe aktarımından önce var olmalıdır:** `ldap-bind` anahtarı realm dosyasına
yer tutucu olarak enjekte edilir ve `KeycloakRealmImport` mevcut bir realm'i
**güncellemez**. Eksik başlarsanız AD federasyonu realm'e hiç girmez ve realm'i silip
yeniden içe aktarmak gerekir.

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" create secret generic keycloak-clients \
  --from-literal=trino="$(openssl rand -hex 24)" \
  --from-literal=superset="$(openssl rand -hex 24)" \
  --from-literal=jupyterhub="$(openssl rand -hex 24)" \
  --from-literal=ldap-bind="$LDAP_BIND_PASSWORD"
```

**Beklenen çıktı:**

```text
secret/keycloak-clients created
```

**Ters giderse:** dört anahtarın dördü de zorunludur; eksik anahtar realm içe aktarımını
düşürür. `ldap-bind` değeri AD servis hesabının parolasıdır ve değer dosyalarına **asla**
yazılmaz.

### 5.7 `trino-service-accounts` — Superset ve Zeppelin'in Trino parolaları

**Neden:** Superset ve Zeppelin, Trino'ya kullanıcı adına değil kendi servis hesaplarıyla
bağlanır. Trino parolaları `password.db` adlı bir htpasswd dosyasından okur (bcrypt);
Superset ve Zeppelin ise aynı parolanın **düz** hâline ihtiyaç duyar. İkisi aynı Secret'ta
durur ve **aynı parolalardan** üretilmek zorundadır.

`[bastion]`

```bash
SUPERSET_TRINO_PW="$(openssl rand -hex 16)"
ZEPPELIN_TRINO_PW="$(openssl rand -hex 16)"
htpasswd -nbBC 10 superset "$SUPERSET_TRINO_PW" >  /tmp/password.db
htpasswd -nbBC 10 zeppelin "$ZEPPELIN_TRINO_PW" >> /tmp/password.db
oc -n "$LAKEHOUSE_NS" create secret generic trino-service-accounts \
  --from-file=password.db=/tmp/password.db \
  --from-literal=superset="$SUPERSET_TRINO_PW" \
  --from-literal=zeppelin="$ZEPPELIN_TRINO_PW"
shred -u /tmp/password.db
```

**Beklenen çıktı:**

```text
secret/trino-service-accounts created
```

`$ZEPPELIN_TRINO_PW` **bu terminal oturumunda kalmalıdır**: Adım 5.10'da yeniden
kullanılır. Oturumu kapatırsanız Adım 5.7 ile 5.10'u birlikte tekrarlayın.

**Ters giderse:** `htpasswd: command not found` → `httpd-tools` paketini kurun.
Kurulumdan sonra Superset ya da Zeppelin Trino'ya `Access Denied` ile bağlanamıyorsa düz
parolalar `password.db` içindekilerle uyuşmuyordur: bu adımı baştan çalıştırıp Adım
5.10'u da tekrarlayın. `-C 10` bayrağı bcrypt maliyetini belirler; Trino en az 8 ister.

### 5.8 `trino-shared-secret` ve `superset-secret`

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" create secret generic trino-shared-secret \
  --from-literal=secret="$(openssl rand -hex 32)"
oc -n "$LAKEHOUSE_NS" create secret generic superset-secret \
  --from-literal=secret-key="$(openssl rand -hex 32)"
```

**Beklenen çıktı:**

```text
secret/trino-shared-secret created
secret/superset-secret created
```

**Ters giderse:** ilki Trino coordinator ile worker'lar arasındaki iç iletişimi imzalar
(eksikse worker'lar kümeye katılamaz), ikincisi Superset'in oturum çerezlerini imzalar
(sonradan değişirse açık oturumlar düşer). İkisi de yalnız küme içinde kalır.

### 5.9 `zeppelin-shiro` — Zeppelin'in AD girişi

**Neden:** Zeppelin, Keycloak'ı **kullanmaz**; AD'ye doğrudan LDAPS ile bağlanır ve yetkiyi
AD grup üyeliğinden okur. Şablon `examples/zeppelin/shiro-ad.ini` dosyasıdır; aşağıdaki
komut beş satırını ve üç grup DN'ini değerlerinizle doldurur. AD parolası her karakteri
içerebildiği için doldurma `sed` ile değil `python3` ile yapılır.

`[bastion]`

```bash
python3 - > /tmp/shiro.ini <<'PY'
import os, pathlib
src = pathlib.Path("examples/zeppelin/shiro-ad.ini").read_text()
repl = {"activeDirectoryRealm.systemUsername":  os.environ["LDAP_BIND_DN"],
        "activeDirectoryRealm.systemPassword":  os.environ["LDAP_BIND_PASSWORD"],
        "activeDirectoryRealm.searchBase":      os.environ["LDAP_USERS_DN"],
        "activeDirectoryRealm.url":             os.environ["LDAP_URL"],
        "activeDirectoryRealm.principalSuffix": os.environ["AD_UPN_SUFFIX"]}
out = []
for line in src.splitlines():
    key = line.split("=", 1)[0].strip()
    out.append(f"{key} = {repl[key]}" if key in repl else line)
print("\n".join(out).replace("OU=Groups,DC=example,DC=com", os.environ["LDAP_GROUPS_DN"]))
PY
grep -c "DC=example,DC=com" /tmp/shiro.ini
oc -n "$LAKEHOUSE_NS" create secret generic zeppelin-shiro \
  --from-file=shiro.ini=/tmp/shiro.ini
shred -u /tmp/shiro.ini
```

**Beklenen çıktı** (ilk satır: şablonda örnek DN kalmadı):

```text
0
secret/zeppelin-shiro created
```

Zeppelin, AD'ye Keycloak'ın kullandığı servis hesabıyla bağlanır. Ayrı bir hesap
isteniyorsa geçici dosyadaki `systemUsername` ve `systemPassword` satırlarını
Secret'ı yaratmadan önce elle değiştirin.

**Ters giderse:** `KeyError` → ilgili `$LDAP_*` ya da `$AD_UPN_SUFFIX` değişkeni boştur
(Adım 1). `grep -c` sıfırdan büyük bir sayı basıyorsa `$LDAP_GROUPS_DN` şablondakinden
farklı bir biçimde yazılmıştır; dosyayı silmeden önce elle kontrol edin. Kurulumdan sonra
Zeppelin girişi `PKIX path building failed` derse AD sertifikasının kök CA'sı güven
deposunda yoktur — Adım 5.13.

### 5.10 `zeppelin-interpreter` — Zeppelin'in Trino bağlantısı

**Neden:** `glue/files/zeppelin/interpreter.json` ürün içinde bir **şablondur**; üretimde
beş yer tutucusu doldurulup düz JSON hâline getirilir. Zeppelin bu dosyayı yalnız ilk
açılışta tohum olarak okur.

`[bastion]`

```bash
JDBC_BASE="jdbc:trino://trino.$LAKEHOUSE_NS.svc:8443/lakehouse?SSL=true"
JDBC_TRUST="SSLTrustStorePath=/etc/lakehouse-ca/tls.crt"
sed -e "s|{{ .url }}|$JDBC_BASE\&$JDBC_TRUST|" \
    -e 's|{{ .user }}|zeppelin|' \
    -e "s|{{ .password }}|$ZEPPELIN_TRINO_PW|" \
    -e 's|{{ .Values.zeppelin.trinoJdbcVersion }}|483|' \
    -e 's|{{ "{{applicationId}}" }}|{{applicationId}}|' \
    glue/files/zeppelin/interpreter.json > /tmp/interpreter.json
python3 -c 'import json; json.load(open("/tmp/interpreter.json")); print("gecerli JSON")'
oc -n "$LAKEHOUSE_NS" create secret generic zeppelin-interpreter \
  --from-file=interpreter.json=/tmp/interpreter.json
shred -u /tmp/interpreter.json
```

**Beklenen çıktı** (kind kümesinde doğrulanmış gerçek çıktı):

```text
gecerli JSON
secret/zeppelin-interpreter created
```

**Ters giderse:** `JSONDecodeError` → `$ZEPPELIN_TRINO_PW` boş kalmıştır (Adım 5.7'yi
tekrarlayın). Dosyada kalan tek `{{applicationId}}` **Zeppelin'in kendi yer tutucusudur**,
doldurulmaz.

### 5.11 `lakehouse-ca` — iç kök CA ve `tls.caBundle`

**Neden:** Trino kendi TLS'ini sonlandırır; sunucu sertifikasını cert-manager,
`Issuer/lakehouse-ca` üzerinden bu kök CA ile imzalar. Aynı kök, Superset ve Zeppelin
pod'larında Trino'ya güvenmek için de kullanılır. Kök sertifikanın PEM içeriği ayrıca site
değerlerine yazılır: dolu olduğunda Trino Route'u `passthrough` yerine `reencrypt` olur ve
OpenShift router'ı geçerli bir sertifika sunar.

`[bastion]`

```bash
openssl req -x509 -newkey rsa:4096 -days 3650 -nodes \
  -subj "/CN=lakehouse-ca" -keyout /tmp/ca.key -out /tmp/ca.crt
oc -n "$LAKEHOUSE_NS" create secret tls lakehouse-ca --cert=/tmp/ca.crt --key=/tmp/ca.key
python3 - <<'PY'
import json, pathlib
ca = pathlib.Path("/tmp/ca.crt").read_text()
p = pathlib.Path("platform/values/site/glue.yaml")
s = p.read_text()
assert 'tls: {caBundle: ""}' in s, "caBundle satiri bulunamadi"
p.write_text(s.replace('tls: {caBundle: ""}', "tls: {caBundle: %s}" % json.dumps(ca)))
PY
python3 -c "import yaml, sys
d = yaml.safe_load(open('platform/values/site/glue.yaml'))
print(d['tls']['caBundle'].splitlines()[0])"
cp /tmp/ca.crt ~/lakehouse-ca.crt
shred -u /tmp/ca.key
```

**Beklenen çıktı** (`openssl` ayrıca birkaç satır nokta basar; kind kümesinde
doğrulanmış):

```text
secret/lakehouse-ca created
-----BEGIN CERTIFICATE-----
```

`~/lakehouse-ca.crt` dosyasını **saklayın**: tarayıcıların ve JDBC istemcilerinin güven
deposuna bu sertifika eklenir.

**Kurumsal PKI'nız varsa** kendi ara CA'nızın sertifika ve anahtarını aynı Secret'a koyun
(`oc create secret tls lakehouse-ca --cert=ara-ca.crt --key=ara-ca.key`); cert-manager
onunla imzalar ve `tls.caBundle` yine **doğrulayıcı zincir** olur.

Değişikliği Git'e itin:

`[bastion]`

```bash
scripts/check-site.sh
git add platform/values/site/glue.yaml
git commit -m "site: lakehouse-ca caBundle"
git push origin main
```

**Beklenen çıktı** (örnek):

```text
check-site: OK
[main a7c3d90] site: lakehouse-ca caBundle
 1 file changed, 1 insertion(+), 1 deletion(-)
To ssh://git.kurum.example.net/veri/lakehouse.git
   4f2a9c1..a7c3d90  main -> main
```

**Ters giderse:** `caBundle satiri bulunamadi` → dosyadaki `tls: {caBundle: ""}` satırı
değiştirilmiştir; `git checkout -- platform/values/site/glue.yaml` ile şablona dönüp Adım
4.1'den itibaren tekrarlayın. `caBundle` **boş bırakılırsa** kurulum yine çalışır ama
Trino Route'u `passthrough` olur ve tarayıcıların kök CA'ya güvenmesi zorunlu hâle gelir.

### 5.12 `jupyterhub-secrets` — oturum çerezi ve kimlik saklama anahtarları

**Neden:** JupyterHub chart'ı bu iki değeri values içinde bulamazsa **her render'da
yeniden üretir**; kurulumdan sonra yeniden üretilirse bütün kullanıcı çerezleri
geçersizleşir ve saklanan kimlik bilgileri çözülemez. Anahtar **adları** chart'ın
sözleşmesidir, harfi harfine yazılır.

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" create secret generic jupyterhub-secrets \
  --from-literal=hub.config.JupyterHub.cookie_secret="$(openssl rand -hex 32)" \
  --from-literal=hub.config.CryptKeeper.keys="$(openssl rand -hex 32)"
```

**Beklenen çıktı:**

```text
secret/jupyterhub-secrets created
```

**Bu Secret'ta yalnız iki anahtar vardır.** Hub ile proxy arasındaki kimlik jetonu burada
**değildir**: chart onu kendi ürettiği `hub` Secret'ına yazar, ArgoCD `ignoreDifferences`
ile dondurur ve iki pod da oradan okur. Üçüncü bir anahtar eklemek hiçbir işe yaramaz.
Jetonun döndürülmesi
[90-referans/secret-listesi.md](90-referans/secret-listesi.md) §4'tedir.

**Ters giderse:** anahtar adlarında bir harf bile yanlışsa chart değeri bulamaz ve yine
rastgele üretir; adları Adım 5.14'teki listeyle karşılaştırın.

### 5.13 `ad-ca` — AD kök CA'sı (koşullu)

**Ne zaman gerekir:** AD sunucusunun sertifikasını imzalayan kök CA, konteyner imajlarının
varsayılan güven deposunda yoksa Keycloak, Trino ve Zeppelin AD'ye LDAPS ile bağlanamaz
(`PKIX path building failed`). Kurumsal (özel) bir CA kullanan her AD'de bu adım
**zorunludur**; sertifika herkesçe bilinen bir CA'dan alınmışsa gerekmez.
Kökü [20-on-kosullar](20-on-kosullar.md) madde 6'da `$LDAP_CA_FILE` olarak almıştınız.

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" create secret generic ad-ca --from-file=ca.crt="$LDAP_CA_FILE"
```

**Beklenen çıktı:**

```text
secret/ad-ca created
```

#### Bu Secret'ı kim okur

Chart bu Secret'ı **kendiliğinden** üç bileşene birden bağlar. Elle mount eklemeniz, özel
konteyner imajı üretmeniz ya da çalışan bir konteynerin `cacerts` dosyasını düzenlemeniz
**gerekmez ve yasaktır** (pod her döndüğünde kaybolur).

| Bileşen | Nasıl bağlanır | Konteynerdeki yol | Hangi dosya yapar |
|---|---|---|---|
| **Keycloak** — AD kullanıcı federasyonu (LDAPS) | Keycloak CR `spec.truststores`; operatör Secret'taki PEM'leri bu dizine bağlar, Keycloak onları **sistem güven deposuyla birleştirir** | `/opt/keycloak/conf/truststores/` | `glue/templates/keycloak.yaml` |
| **Trino coordinator** — LDAP grup sağlayıcısı (AD grupları) | Secret volume + `ldap.ssl.truststore.path` (Trino PEM güven deposu kabul eder) | `/etc/trino/ad-ca/ca.crt` | `platform/values/trino-ldap.yaml` (mount) + `platform/values/site/trino.yaml` (satır) |
| **Zeppelin** — Shiro `ActiveDirectoryGroupRealm` | `ad-truststore` initContainer'ı imajın **kendi** `cacerts` dosyasını kopyalar, kökü `keytool` ile ekler; sunucu ve yorumlayıcı JVM'leri `-Djavax.net.ssl.trustStore` ile bu kopyayı kullanır | `/truststore/ad-truststore.p12` | `glue/templates/zeppelin.yaml` |

Üçünü birden açıp kapatan **tek** ayar `platform/values/site/glue.yaml` içindeki
`keycloak.ldap.caSecret`'tir (şablonda `ad-ca` yazılıdır).

**Bu Secret'a ihtiyacınız yoksa** (AD sertifikanız herkesçe bilinen bir CA'dan geliyorsa)
tam olarak iki satır değiştirin:

1. `platform/values/site/glue.yaml` → `keycloak.ldap.caSecret: ""`
2. `platform/values/site/trino.yaml` → `group-provider.properties` bloğundan
   `ldap.ssl.truststore.path` satırını **silin**

`platform/values/trino-ldap.yaml` dosyasına **dokunmayın**: oradaki Secret bağlaması
`optional: true`'dur, yani `ad-ca` yoksa Trino yine sorunsuz açılır (bağlama sessizce boş
kalır). `scripts/check-site.sh` yukarıdaki iki adımın **birlikte** yapıldığını denetler:
`caSecret` doluyken `ldap.ssl.truststore.path` satırı bulunmalı, boşken bulunmamalıdır.

Zeppelin'de kopya alınmasının nedeni: yalnız AD kökünü içeren bir güven deposu JVM'in
varsayılanını **ezer** ve Maven Central'dan inen Trino JDBC sürücüsü ile Keycloak/Trino
TLS bağlantıları kırılır. Depo parolası (`changeit`) sır değildir: depo yalnız açık
sertifika taşır.

#### Doğrulama (Adım 6 bootstrap'tan **sonra** çalıştırın)

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" exec keycloak-0 -- ls /opt/keycloak/conf/truststores
oc -n "$LAKEHOUSE_NS" get deploy/trino-coordinator \
  -o jsonpath='{.spec.template.spec.containers[0].volumeMounts[?(@.name=="ad-ca")].mountPath}{"\n"}'
oc -n "$LAKEHOUSE_NS" logs deploy/zeppelin -c ad-truststore --tail=1
```

**Beklenen çıktı** (birinci ve üçüncü satır kind kümesinden alınmış gerçek çıktıdır;
ikinci satır üretim değer dosyalarıyla üretilmiştir — geliştirme kümesi LDAP grup
sağlayıcısını kullanmadığı için orada **boş** döner):

```text
secret-ad-ca
/etc/trino/ad-ca
147
```

Son satır, Zeppelin'in güven deposundaki **toplam** sertifika sayısıdır: imajın
varsayılan kökleri **artı** sizin AD kökünüz; onlarca olması beklenir. `1` görürseniz
depoda yalnız AD kökü vardır (kopyalama yapılmamış) ve bu, dış TLS bağlantılarını kırar. AD kökünün gerçekten içeride olduğunu görmek için:

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" exec deploy/zeppelin -c zeppelin -- \
  keytool -list -keystore /truststore/ad-truststore.p12 -storetype PKCS12 \
  -storepass changeit | grep ad-ca
```

**Beklenen çıktı** (tarih kurulum gününüzü gösterir):

```text
ad-ca, Sep 19, 2026, trustedCertEntry,
```

**Ters giderse:** `error reading ...: no such file` → `$LDAP_CA_FILE` yolu yanlıştır.
Dosyanın gerçekten bir kök CA olduğunu [20-on-kosullar](20-on-kosullar.md) madde 6'daki
`openssl x509 -noout -subject -issuer` komutuyla doğrulayın.
Kurulumdan sonra herhangi bir bileşenin günlüğünde `PKIX path building failed` ya da
`unable to find valid certification path to requested target` görürseniz: bu Secret ya
eksiktir, ya **yanlış kökü** taşımaktadır, ya da ilgili bileşen onu okumamıştır —
yukarıdaki "Doğrulama" başlığındaki komutları sırayla çalıştırın. Zeppelin'in
`ad-truststore` initContainer'ı `Error` durumundaysa pod hiç açılmaz: `ca.crt` PEM
değildir (`openssl x509 -inform der` ile çevirin).
Uçtan uca LDAPS el sıkışması yalnız gerçek bir AD ile kanıtlanabilir
**(OpenShift'te doğrulanır)** — geliştirme kümesinde AD yoktur, orada yalnız bağlama ve
güven deposu üretimi doğrulanır.

### 5.14 Hepsini birden doğrulayın

`[bastion]`

```bash
for s in connect-push s3-creds backup-s3-creds polaris-root keycloak-admin \
         keycloak-clients trino-service-accounts trino-shared-secret superset-secret \
         zeppelin-shiro zeppelin-interpreter lakehouse-ca jupyterhub-secrets ad-ca; do
  printf '%-24s %s\n' "$s" \
    "$(oc -n "$LAKEHOUSE_NS" get secret "$s" \
        -o go-template='{{range $k,$v := .data}}{{$k}} {{end}}' 2>/dev/null || echo EKSIK)"
done
```

**Beklenen çıktı** (kind kümesinden alınmış gerçek çıktı; üretimde ilk satır
`.dockerconfigjson` gösterir):

```text
connect-push             .dockerconfigjson
s3-creds                 AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY
backup-s3-creds          AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY
polaris-root             clientId clientSecret
keycloak-admin           password username
keycloak-clients         jupyterhub ldap-bind superset trino
trino-service-accounts   password.db superset zeppelin
trino-shared-secret      secret
superset-secret          secret-key
zeppelin-shiro           shiro.ini
zeppelin-interpreter     interpreter.json
lakehouse-ca             tls.crt tls.key
jupyterhub-secrets       hub.config.CryptKeeper.keys hub.config.JupyterHub.cookie_secret
ad-ca                    ca.crt
```

Son satır (`ad-ca`) **koşulludur**: AD sertifikanız konteynerlerin zaten güvendiği bir
kökten geliyorsa §5.13'ü atlamış olursunuz ve burada `EKSIK` yazması normaldir. Özel bir
kurumsal CA kullanan AD'de ise **dolu olmalıdır**.

**Ters giderse:** `EKSIK` yazan her satır için ilgili alt adımı tekrarlayın. Bu tabloyu
bootstrap'tan **önce** eksiksiz görmelisiniz: eksik bir Secret'la başlatılan kurulumda
ilgili pod `CreateContainerConfigError` durumunda bekler (Secret sonradan yaratılınca
kubelet kendiliğinden toparlar, ama tanı koymak zaman alır).

---

## 6. Bootstrap

**Neden:** `bootstrap/bootstrap.sh` ArgoCD'ye tek bir **kök Application** yazar; kök,
`platform/apps/` altındaki bütün alt Application'ları üretir ve hepsinin depo adresini ve
revizyonunu her uzlaşmada aynı değere sabitler. OpenShift'te ArgoCD'yi GitOps operatörü
yönettiği için betik yukarı akış ArgoCD manifestini **uygulamaz**.

`[bastion]`

```bash
bootstrap/bootstrap.sh --env prod --mode argocd \
  --repo "$GIT_REPO_URL" --revision main --argocd-ns "$ARGOCD_NS"
```

**Beklenen çıktı** (örnek):

```text
ArgoCD zaten kurulu (openshift-gitops) — upstream manifest uygulanmıyor
secret/strimzi-helm created
secret/jetstack-helm created
secret/superset-operator-helm created
application.argoproj.io/lakehouse-root created
OK: ArgoCD v3.5.2 + lakehouse-root (env=prod, repo=...@main) uygulandı
İzle: kubectl -n openshift-gitops get applications
```

İlk satır **beklenen** satırdır: ArgoCD'yi operatör yönetir. Bu satır yazmıyorsa betik
yukarı akış ArgoCD'sini kurmaya kalkmıştır — hemen durdurun ve
[20-on-kosullar](20-on-kosullar.md) madde 2'ye dönün.

`--revision main` yerine bir etiket ya da commit kimliği verebilirsiniz; ArgoCD o noktayı
uygular ve orada kalır.

**Ters giderse:** `bilinmeyen argüman` → bayrak adını yanlış yazdınız. `Unauthorized` →
`oc login` oturumunuz düşmüştür. Kök Application `repository not accessible` derse Adım
3'teki Secret'ın etiketi eksiktir.

### 6.1 Uygulamaların ayağa kalkmasını izleyin

`[bastion]`

```bash
watch -n 30 "oc -n $ARGOCD_NS get applications"
```

**Beklenen çıktı** (örnek — 40–60 dakika sonra; `watch` ekranından `Ctrl-C` ile
çıkılır):

```text
NAME                SYNC STATUS   HEALTH STATUS
lakehouse-root      Synced        Healthy
cert-manager        Synced        Healthy
cnpg                Synced        Healthy
cnpg-barman         Synced        Healthy
keycloak-operator   Synced        Healthy
spark-operator      Synced        Healthy
strimzi             Synced        Healthy
superset-operator   Synced        Healthy
glue                Synced        Healthy
custom              Synced        Healthy
polaris             Synced        Healthy
jupyterhub          Synced        Healthy
trino               OutOfSync     Progressing
```

**Süre beklentileri.** cert-manager ve operatörler 2–5 dakika; `glue` en ağır uygulamadır
(Kafka Connect imajının kümede üretilmesi ~10 dakika, üç PostgreSQL kümesinin ilk kurulumu,
Zeppelin ve not defteri imajlarının çekilmesi) ve 30–45 dakika sürebilir.

**`trino` bu aşamada Healthy olamaz ve bu normaldir.** Trino pod'u `polaris-trino`
Secret'ını bekler; o Secret Polaris kataloğu kurulurken yazılır (bir sonraki bölüm).

**İzleme ve yedekleme uygulamaları listede yoktur.** `monitoring` ve `velero`
Application'ları yalnız geliştirme kümesinde bulunur; OpenShift'te izlemeyi platformun
kendi kullanıcı iş yükü izlemesi, ad alanı yedeğini OADP yapar.

**Ters giderse:** bir uygulama uzun süre `Progressing` kalıyorsa
`oc -n "$ARGOCD_NS" get application ADI -o jsonpath='{.status.conditions}'` çıktısına ve
`oc -n "$LAKEHOUSE_NS" get pods` listesine bakın. `ImagePullBackOff` → egress listesi
kapalıdır ([20-on-kosullar](20-on-kosullar.md) madde 10). `CreateContainerConfigError` →
bir Secret eksiktir (Adım 5.14). PVC'ler `Pending` kalıyorsa StorageClass yoktur (madde 3).
Ayrıntılı belirti tablosu: docs/50-isletme/sorun-giderme.md (bu bölüm bir sonraki görevde
eklenir).

---

## 7. Kurulumun doğrulanması

Aşağıdaki zincir bileşenleri bağımlılık sırasıyla bekler. Her komut bir öncekinin
tamamlanmasını şart koşar; ilki takılırsa sonrakileri denemeyin.

### 7.1 Kafka, Connect, veritabanları, Polaris, Keycloak

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" wait kafka/lakehouse --for=condition=Ready --timeout=900s
oc -n "$LAKEHOUSE_NS" wait kafkaconnect/connect --for=condition=Ready --timeout=1800s
oc -n "$LAKEHOUSE_NS" get cluster.postgresql.cnpg.io
oc -n "$LAKEHOUSE_NS" rollout status deploy/polaris --timeout=600s
oc -n "$LAKEHOUSE_NS" wait keycloak/keycloak --for=condition=Ready --timeout=900s
oc -n "$LAKEHOUSE_NS" wait keycloakrealmimport/lakehouse-realm --for=condition=Done --timeout=600s
```

Sıra `test/e2e/run.sh` ile aynıdır: her sürümde uçtan uca sınanan zincir budur.

**Beklenen çıktı** (kind kümesinde alınmış gerçek çıktı; üretimde `INSTANCES` değeri
**2** olur — birincil artı yedek):

```text
kafka.kafka.strimzi.io/lakehouse condition met
kafkaconnect.kafka.strimzi.io/connect condition met
NAME          AGE   INSTANCES   READY   STATUS                     PRIMARY
keycloak-db   22h   1           1       Cluster in healthy state   keycloak-db-1
polaris-db    22h   1           1       Cluster in healthy state   polaris-db-1
superset-db   22h   1           1       Cluster in healthy state   superset-db-1
deployment "polaris" successfully rolled out
keycloak.k8s.keycloak.org/keycloak condition met
keycloakrealmimport.k8s.keycloak.org/lakehouse-realm condition met
```

**Ters giderse:** `kafka` beklemede kalıyorsa broker pod'larının PVC'leri bağlanmamıştır.
`kafkaconnect` en uzun süren adımdır: imaj üretimi
`oc -n "$LAKEHOUSE_NS" get pods | grep connect-build` ile izlenir; `unauthorized` →
`connect-push` jetonu geçersizdir. `keycloakrealmimport` `Done` olmuyorsa
`oc -n "$LAKEHOUSE_NS" logs job/lakehouse-realm` çıktısında eksik yer tutucu arayın
(Adım 5.6). Belirti tablosu: docs/50-isletme/sorun-giderme.md (bu bölüm bir sonraki
görevde eklenir).

### 7.2 Hiçbir pod hata durumunda değil

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" get pods \
  --field-selector=status.phase!=Running,status.phase!=Succeeded
```

**Beklenen çıktı** (örnek — Trino pod'u dışında hiçbir şey kalmamalıdır):

```text
NAME                       READY   STATUS                       RESTARTS   AGE
trino-coordinator-0        0/1     CreateContainerConfigError   0          12m
```

**Ters giderse:** listedeki her pod için `oc -n "$LAKEHOUSE_NS" describe pod ADI`
çıktısının `Events` bölümüne bakın. Trino dışındaki bir pod `CreateContainerConfigError`
veriyorsa Adım 5.14'ü tekrarlayın.

### 7.3 Route'lar ve dış erişim

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" get routes
for r in keycloak superset jupyterhub zeppelin; do
  host=$(oc -n "$LAKEHOUSE_NS" get route "$r" -o jsonpath='{.spec.host}')
  printf '%-12s %-44s %s\n' "$r" "$host" \
    "$(curl -sS -o /dev/null -w '%{http_code}' --max-time 15 "https://$host/")"
done
```

Adres **kümeden** okunur, `$APPS_DOMAIN` ile elle kurulmaz: site değerlerinde
`trino.hostname` gibi bir kurumsal ad ezmesi kullandıysanız döngü yine doğru adresi
dener.

**Beklenen çıktı** (örnek — `200` ya da `302` "ayakta" demektir; oturum açma
yönlendirmeleri `302` verir):

```text
NAME         HOST/PORT                                    PORT    TERMINATION
keycloak     keycloak-lakehouse.apps.ocp.example.net      http    edge
superset     superset-lakehouse.apps.ocp.example.net      http    edge
jupyterhub   jupyterhub-lakehouse.apps.ocp.example.net    http    edge
zeppelin     zeppelin-lakehouse.apps.ocp.example.net      http    edge
trino        trino-lakehouse.apps.ocp.example.net         https   reencrypt
keycloak     keycloak-lakehouse.apps.ocp.example.net      302
superset     superset-lakehouse.apps.ocp.example.net      200
jupyterhub   jupyterhub-lakehouse.apps.ocp.example.net    302
zeppelin     zeppelin-lakehouse.apps.ocp.example.net      200
```

Trino Route'unun sonlandırması `tls.caBundle` doluysa `reencrypt`, boşsa `passthrough`
olur. Trino'yu bu listede sınamayın: kimlik doğrulaması zorunlu olduğu için `401` döner,
bu da beklenen davranıştır.

**Ters giderse:** `NotFound` → glue henüz Route'ları yazmamıştır (Adım 6.1). `000` → DNS
çözülmüyor ya da güvenlik duvarı kapalıdır ([20-on-kosullar](20-on-kosullar.md) madde 7);
kurumsal ad ezmesi kullandıysanız o ad için CNAME kaydının açıldığını doğrulayın.
`503` → Route arkasındaki pod henüz hazır değildir, birkaç dakika bekleyin.
`curl: (60) SSL certificate problem` → router sertifikası kurumsal bir CA ile
imzalıdır; `--cacert` ile CA'yı verin ya da tarayıcıdan deneyin.

---

## Realm içeriğini sonradan değiştirmek

`KeycloakRealmImport` realm'i yalnız **yoksa** içe aktarır.
`glue/templates/keycloak-realm.yaml` değişse bile çalışan realm'e dokunmaz: CR `Done`
görünür, değişiklik uygulanmaz. Bu yüzden `keycloak-clients` Secret'ı ve AD ayarları
kurulumdan **önce** doğru olmalıdır.

Yine de realm'i yenilemek gerekirse (**kullanıcıların Keycloak arayüzünde elle yaptığı her
şey gider**; AD federasyonu varsa kullanıcılar tekrar akar):

`[bastion]`

```bash
KC_ADMIN=$(oc -n "$LAKEHOUSE_NS" get secret keycloak-admin \
  -o jsonpath='{.data.username}' | base64 -d)
KC_PASS=$(oc -n "$LAKEHOUSE_NS" get secret keycloak-admin \
  -o jsonpath='{.data.password}' | base64 -d)
oc -n "$LAKEHOUSE_NS" delete keycloakrealmimport lakehouse-realm
oc -n "$LAKEHOUSE_NS" exec keycloak-0 -- /opt/keycloak/bin/kcadm.sh config credentials \
  --server http://localhost:8080 --realm master --user "$KC_ADMIN" --password "$KC_PASS"
oc -n "$LAKEHOUSE_NS" exec keycloak-0 -- /opt/keycloak/bin/kcadm.sh delete realms/lakehouse
oc -n "$ARGOCD_NS" patch application glue --type merge -p '{"operation":{"sync":{}}}'
oc -n "$LAKEHOUSE_NS" wait keycloakrealmimport/lakehouse-realm \
  --for=condition=Done --timeout=600s
```

**Beklenen çıktı** (örnek):

```text
keycloakrealmimport.k8s.keycloak.org "lakehouse-realm" deleted
Logging into http://localhost:8080 as user admin of realm master
application.argoproj.io/glue patched
keycloakrealmimport.k8s.keycloak.org/lakehouse-realm condition met
```

**Ters giderse:** `kcadm.sh` oturumu pod yeniden başlatıldığında kaybolur,
`config credentials` komutunu tekrarlayın. Aynı kural Polaris kataloğu için de geçerlidir:
`polaris setup apply` katalog özelliklerini yalnız katalog **yaratılırken** yazar, var
olan bir katalogda `polaris catalogs update --set-property ...` gerekir. Kullanıcı ve
yetki yönetiminin tamamı docs/50-isletme/kullanici-ve-yetki.md dosyasındadır (bu bölüm bir
sonraki görevde eklenir).

---

## Lokal deneme (kind)

Kurulumu üretim kümesinde yapmadan önce **aynı adımların** büyük bölümü bir dizüstünde
kind kümesinde denenebilir. Geliştirme kümesi ArgoCD'siz "helm modunda" da kurulabilir;
komutlar `oc` yerine `kubectl` kullanır.

`[bastion]`

```bash
export KIND_EXPERIMENTAL_PROVIDER=podman
test/e2e/kind.sh
bootstrap/bootstrap.sh --env dev --mode helm
```

**Beklenen çıktı** (örnek — arada onlarca `helm upgrade` satırı akar; önemli olan bu iki
satırdır. Taze bir dizüstü kümesinde imaj çekimleriyle birlikte 40 dakikayı bulur):

```text
OK: kind-lakehouse hazır
OK: helm modunda kuruldu (env=dev)
```

**Ters giderse:** `command not found: kind` → kind ≥ 0.33 kurun (Podman 6 uyumu bu
sürümle gelir). `helm upgrade` zaman aşımına uğrarsa imaj çekimi sürüyordur;
`kubectl -n lakehouse get pods` ile bakıp komutu tekrarlayın — betik idempotenttir.
Küme zaten ayaktaysa `test/e2e/kind.sh` onu **silmez**, var olan kümeyi kullanır.

**Bu bölümün hangi adımları kind'da geçerlidir:**

| Adım | kind'da | Not |
|---|---|---|
| 1 — değişkenler | aynı | `install/lakehouse.env` aynı biçimde yüklenir |
| 2–3 — Git kopyası ve ArgoCD depo kimliği | yalnız ArgoCD modunda | helm modunda ArgoCD yoktur |
| 4 — site değerleri | aynı | `scripts/check-site.sh` aynı denetimi yapar |
| 5 — Secret'lar | **atlanır** | `components.devSecrets: true` iken glue chart'ı sentetik Secret'ları kendisi üretir (`glue/templates/dev-secrets.yaml`). **Üretimde bu adımların hepsi elle yapılır.** |
| 6 — bootstrap | `--env dev --mode helm` | ArgoCD yerine doğrudan `helm upgrade --install` |
| 7 — doğrulama | aynı | aynı bekleme komutları çalışır |

Bu bölümdeki "Beklenen çıktı" bloklarının bir kısmı doğrudan çalışan bir kind
kümesinden alınmıştır: Adım 4.5 (`check-site: OK`), Adım 5.10 (`gecerli JSON`),
Adım 5.14 (Secret anahtar listesi) ve Adım 7.1 (bekleme zinciri). OpenShift'e özgü çıktılar
(`oc get routes`, ArgoCD Application tablosu, bootstrap günlüğü) "örnek" olarak
işaretlenmiştir.

Site değerlerinin gerçekten uygulandığını kurmadan da görebilirsiniz — chart'ı yalnız
render edip host adlarına bakın:

`[bastion]`

```bash
helm template glue ./glue -n "$LAKEHOUSE_NS" \
  -f platform/values/glue.yaml -f platform/values/site/glue.yaml \
  | grep -E "^  host: |termination:"
```

**Beklenen çıktı** (kind kümesinde alınmış gerçek çıktı; alan adı kendi `$APPS_DOMAIN`
değerinizle çıkar):

```text
  host: keycloak-lakehouse.apps.ocp.kurum.example.net
    termination: edge
  host: trino-lakehouse.apps.ocp.kurum.example.net
    termination: reencrypt
  host: superset-lakehouse.apps.ocp.kurum.example.net
    termination: edge
  host: jupyterhub-lakehouse.apps.ocp.kurum.example.net
    termination: edge
  host: zeppelin-lakehouse.apps.ocp.kurum.example.net
    termination: edge
```

**Ters giderse:** `appsDomain ya da ... hostname verilmeli` hatası → `appsDomain` boştur
(Adım 4.1). `termination: passthrough` görüyorsanız `tls.caBundle` boştur (Adım 5.11); bu
bir hata değildir ama üretimde `reencrypt` tercih edilir.

---

## Kontrol listesi

- [ ] `install/lakehouse.env` yüklendi ve `EKSIK:` satırı basılmadı.
- [ ] Depo kurumun Git sunucusuna kopyalandı; yerel kopyanın `origin` adresi oraya bakıyor.
- [ ] `$ARGOCD_NS` ad alanında `lakehouse-repo` Secret'ı var ve
      `argocd.argoproj.io/secret-type=repository` etiketi taşıyor.
- [ ] `platform/values/site/` altındaki üç dosyada örnek değer kalmadı;
      `scripts/check-site.sh` hatasız ve uyarısız bitiyor.
- [ ] `platform/polaris/setup.yaml` müşterinin S3 adresini ve veri bucket'ını gösteriyor.
- [ ] Site değerleri ve `tls.caBundle` Git'e itildi.
- [ ] Adım 5.14 tablosundaki 13 Secret'ın hepsi anahtarlarıyla listeleniyor.
- [ ] `bootstrap/bootstrap.sh` "ArgoCD zaten kurulu" satırını bastı ve kök Application
      yaratıldı.
- [ ] `trino` dışındaki bütün Application'lar `Synced/Healthy`.
- [ ] Adım 7.1 zincirindeki altı komut da hatasız bitti.
- [ ] Route'lar `200`/`302` dönüyor.
- [ ] Keycloak yönetici parolası ve `~/lakehouse-ca.crt` kurumun kasasına alındı.

## Sonraki bölüm

docs/40-kurulum-sonrasi.md — Polaris kataloğunun kurulması (`scripts/polaris-setup.sh`),
Superset'e Trino bağlantısının içe aktarılması, AD kullanıcısıyla ilk giriş, izleme
hedefleri ve kabul testi (bu bölüm bir sonraki görevde eklenir). Başvuru tabloları:
[90-referans/secret-listesi.md](90-referans/secret-listesi.md) ve
[90-referans/values-anahtarlari.md](90-referans/values-anahtarlari.md).
