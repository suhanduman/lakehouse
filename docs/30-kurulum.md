# 30 — Kurulum

Bu bölüm ürünü sıfırdan kurar: depo kurumun Git sunucusuna alınır, ArgoCD'ye depoyu
okuma izni verilir, kuruma özel ayarlar doldurulur, parolalar kümeye Secret olarak
yazılır, kurulum başlatılır ve bileşenlerin ayağa kalktığı doğrulanır. Polaris kataloğu,
ilk giriş ve kabul testi bir sonraki bölümdedir.

**Ne kadar sürer:** komutları çalıştırmak 1–1,5 saat. Ardından bileşenlerin ayağa
kalkması 40–60 dakika (ağın hızına bağlıdır; Kafka Connect imajı kümede üretilir ve tek
başına 10 dakika kadar sürer).

**Hangi yetkiler gerekir:** kümede **cluster-admin**; `$ARGOCD_NS` ve `$LAKEHOUSE_NS` ad
alanlarında yazma; kurumun Git sunucusunda depo açma ve gönderme.

**Komutlar nerede çalışır:** `[bastion]` — `oc`, `git`, `openssl`, `htpasswd` ve
`python3` araçlarının kurulu olduğu, `oc login` ile kümeye bağlı yönetim makinesi.
`htpasswd` RHEL/Fedora'da `httpd-tools` paketiyle (`sudo dnf install -y httpd-tools`),
Debian/Ubuntu'da `apache2-utils` paketiyle gelir.

> **Başlamadan önce [20-on-kosullar](20-on-kosullar.md) kontrol listesinin bütün
> maddeleri işaretli olmalıdır.** Özellikle madde 2 (GitOps operatörü), madde 5
> (`connect-push` Secret'ı ve `$LAKEHOUSE_NS` ad alanı) ve madde 6 (AD grupları) bu
> bölümde kullanılır. Eksik bir ön koşulla başlanan kurulum yarıda kalır.

**Adımların sırası önemlidir.** ArgoCD Git'teki hâli kümeye uygular. Parolalar Git'e
girmez, bu yüzden kurulum başlamadan önce kümede bulunmaları gerekir; eksik bir Secret
varsa ilgili pod `CreateContainerConfigError` durumunda bekler. Sıra şudur:

```text
Git kopyası -> ArgoCD'ye depo izni -> kuruma özel ayarlar (Git'e) -> Secret'lar (kümeye)
            -> kurulumu başlat -> doğrula
```

---

## 1. Değişkenleri yükleyin

[10-planlama](10-planlama.md) §5'te doldurduğunuz `install/lakehouse.env` dosyasını
yükleyin. **Bu bölümdeki bütün komutları aynı terminalde çalıştırın:** Adım 5.7'de
üretilen bir parola Adım 5.10'da yeniden kullanılır.

`[bastion]`

```bash
cd ~/lakehouse
set -a; . install/lakehouse.env; set +a
for v in GIT_REPO_URL ARGOCD_NS LAKEHOUSE_NS APPS_DOMAIN INTERNAL_REGISTRY \
         S3_ENDPOINT S3_BUCKET_DATA S3_BUCKET_BACKUP S3_REGION \
         S3_ACCESS_KEY S3_SECRET_KEY S3_BACKUP_ACCESS_KEY S3_BACKUP_SECRET_KEY \
         LDAP_URL LDAP_BIND_DN LDAP_BIND_USER LDAP_BIND_PASSWORD LDAP_USERS_DN \
         LDAP_GROUPS_DN LDAP_CA_FILE AD_UPN_SUFFIX; do
  eval "deger=\${$v:-}"
  [ -n "$deger" ] || echo "EKSIK: $v"
done
echo "degisken denetimi bitti"
```

**Beklenen çıktı** (tek satır; `EKSIK:` ile başlayan satır olmamalıdır):

```text
degisken denetimi bitti
```

`STORAGE_CLASS`, `KEYCLOAK_ADMIN_PASSWORD`, `S3_CA_FILE` ve `S3_EGRESS_CIDRS` bu listede
yoktur, çünkü boş kalabilirler: boşsa kümenin varsayılan StorageClass'ı kullanılır,
Keycloak parolası üretilir, S3 sertifikasının herkesçe bilinen bir CA'dan geldiği ve
S3'ün iç ağda olmadığı kabul edilir.

**Ters giderse:**

- `EKSIK:` satırı: o değişkeni [10-planlama](10-planlama.md) §5'teki çalışma sayfasından
  doldurun.
- `No such file or directory`: dosya kopyalanmamıştır;
  `cp install/lakehouse.env.example install/lakehouse.env` ile kopyalayıp doldurun.
- Dosya yüklenirken `command not found` gibi bir satır: bir değerde boşluk vardır ve
  tırnak içinde değildir. O değeri çift tırnağa alın (ör.
  `LDAP_BIND_DN="CN=svc lakehouse,..."`) ve dosyayı yeniden yükleyin.

---

## 2. Depoyu kurumun Git sunucusuna kopyalayın

ArgoCD kurulumu ve bundan sonraki bütün değişiklikleri Git'ten okur. Okuduğu yer
ürünün genel deposu değil, **kurumun kendi kopyasıdır**; kuruma özel ayarlar oraya
yazılır. Git sunucunuzda önceden boş bir depo açılmış olmalıdır (`$GIT_REPO_URL`).

`[bastion]`

```bash
git clone --mirror https://github.com/suhanduman/lakehouse.git ~/lakehouse-mirror.git
git -C ~/lakehouse-mirror.git push --mirror "$GIT_REPO_URL"
git -C ~/lakehouse remote set-url origin "$GIT_REPO_URL"
git -C ~/lakehouse fetch origin
git -C ~/lakehouse status -sb | head -1
```

**Beklenen çıktı** (örnek; ilk iki komut ağ hızına göre 10–60 saniye sürer):

```text
Cloning into bare repository '/home/kurulum/lakehouse-mirror.git'...
To ssh://git.kurum.example.net/veri/lakehouse.git
 * [new branch]      main -> main
## main...origin/main
```

Son satırda `ahead` ya da `behind` yazmamalıdır: yerel kopya ile kurumun deposu aynı
noktadadır.

**Ters giderse:**

- `Permission denied (publickey)`: Git sunucusunda gönderme yetkiniz yoktur ya da SSH
  anahtarınız tanımlı değildir. `git ls-remote "$GIT_REPO_URL"` ile ayrıca deneyin.
- `remote rejected` ya da `pre-receive hook declined`: hedef depo boş değildir ya da dal
  korumalıdır; Git yöneticisinden boş bir depo isteyin.
- `Repository not found`: `$GIT_REPO_URL` yanlış yazılmıştır.
- Kurum ağı internete kapalıysa depoyu internete açık bir makinede klonlayıp
  `git bundle` ile taşıyın.

---

## 3. ArgoCD'ye depoyu okuma izni verin

ArgoCD `$GIT_REPO_URL` adresini okuyabilmelidir. İzin, `$ARGOCD_NS` ad alanında özel bir
etiket taşıyan bir Secret ile verilir. Aşağıdaki komutlar yalnız bu kurulum için bir SSH
anahtar çifti üretir; anahtarın **açık** kısmını Git sunucunuzda deponun dağıtım
anahtarları (deploy key) listesine eklersiniz. Salt okuma yetkisi yeterlidir.

### 3.1 Anahtarı üretin ve açık kısmını Git sunucusuna tanıtın

`[bastion]`

```bash
ssh-keygen -t ed25519 -N "" -C "argocd-lakehouse" -f "$HOME/.ssh/lakehouse_deploy"
cat "$HOME/.ssh/lakehouse_deploy.pub"
```

**Beklenen çıktı** (örnek; son satırı Git sunucusunun dağıtım anahtarı ekranına
yapıştırın):

```text
Generating public/private ed25519 key pair.
Your identification has been saved in /home/kurulum/.ssh/lakehouse_deploy
ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIKk3s2vQ0m3Q6l8y9m1Yb2d4e6f8g0h2 argocd-lakehouse
```

**Ters giderse:** dosya zaten varsa `ssh-keygen` üzerine yazmayı sorar; var olan
anahtarı kullanacaksanız bu adımı atlayın. `$GIT_REPO_URL` `https://` ile başlıyorsa SSH
anahtarı gerekmez; Adım 3.2'deki HTTPS komutunu kullanın.

### 3.2 Secret'ı yaratın

`[bastion]`

```bash
oc -n "$ARGOCD_NS" create secret generic lakehouse-repo \
  --from-literal=type=git \
  --from-literal=url="$GIT_REPO_URL" \
  --from-file=sshPrivateKey="$HOME/.ssh/lakehouse_deploy"
oc -n "$ARGOCD_NS" label secret lakehouse-repo argocd.argoproj.io/secret-type=repository \
  lakehouse.io/yonetim=manual-30-kurulum-3.2
```

**Beklenen çıktı:**

```text
secret/lakehouse-repo created
secret/lakehouse-repo labeled
```

İlk etiket ArgoCD'nin Secret'ı tanıması içindir. İkincisi Secret'ın bu adımda elle
yaratıldığını kaydeder
([değişiklik nasıl uygulanır](50-isletme/degisiklik-nasil-uygulanir.md) §7).

**HTTPS kullanıyorsanız** SSH anahtarı yerine bir kullanıcı adı ve erişim jetonu verin
(salt okuma yetkili jeton yeterlidir). `$GIT_HTTPS_TOKEN` `install/lakehouse.env`
dosyasındadır; SSH kullananlar onu boş bırakır.

`[bastion]`

```bash
oc -n "$ARGOCD_NS" create secret generic lakehouse-repo \
  --from-literal=type=git --from-literal=url="$GIT_REPO_URL" \
  --from-literal=username=argocd --from-literal=password="$GIT_HTTPS_TOKEN"
oc -n "$ARGOCD_NS" label secret lakehouse-repo argocd.argoproj.io/secret-type=repository \
  lakehouse.io/yonetim=manual-30-kurulum-3.2
```

**Beklenen çıktı** (SSH komutlarıyla aynı):

```text
secret/lakehouse-repo created
secret/lakehouse-repo labeled
```

**Ters giderse:**

- `AlreadyExists`: Secret vardır. `oc -n "$ARGOCD_NS" delete secret lakehouse-repo` ile
  silip iki komutu tekrarlayın.
- ArgoCD `authentication required` diyorsa `$GIT_HTTPS_TOKEN` boştur; jetonu doldurup
  Secret'ı silin ve yeniden yaratın. Git sunucunuz kullanıcı adı olarak belirli bir değer
  istiyorsa `username` değerini ona çevirin.
- Kök uygulama `repository not accessible` diyorsa `argocd.argoproj.io/secret-type`
  etiketi eksiktir; ArgoCD Secret'ı görmez.
- ArgoCD `unknown host` diyorsa Git sunucusunun SSH anahtarını `$ARGOCD_NS` ad
  alanındaki `argocd-ssh-known-hosts-cm` ConfigMap'ine ekleyin
  (`ssh-keyscan git.kurum.example.net` çıktısı).

---

## 4. Kuruma özel ayarları doldurun

Kuruma özel bütün ayarlar `platform/values/site/` klasöründedir. Ayarların listesi ve
anlamları [90-referans/values-anahtarlari.md](90-referans/values-anahtarlari.md)
dosyasındadır. Ürünün varsayılan dosyalarına (`platform/values/glue.yaml` ve yanındakiler)
dokunmayın.

### 4.1 Üç ayar dosyasını değişkenlerden doldurun

Dosyalardaki örnek değerler (`apps.ocp.example.net`, `https://s3.example.com`, örnek AD
adresleri) tek komutla gerçek değerlerinizle değiştirilir.

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

Kafka Connect imajının adresi (`connect.buildImage`) küme içi registry'yi gösterir.
`$INTERNAL_REGISTRY` başka bir registry ise bu satırı elle düzeltin. İmaj etiketi
(`:2.0.0`) ürünün sürümüdür ve her yükseltmede artırılır; `:latest` kullanılamaz.

**Ters giderse:** `ornek deger kalmadi` yerine dosya satırları yazılıyorsa bir değişken
boştur; Adım 1'i tekrarlayın. Denetim yalnız şu dört örnek değeri arar:
`apps.ocp.example.net`, `s3.example.com`, `ad.example.com`, `DC=example,DC=com`. Dosyaları
bozduysanız `git checkout -- platform/values/site/` ile ilk hâline dönüp baştan başlayın.

**S3 kök CA'sı ve S3 IP adresleri.** Bu iki ayarı aşağıdaki komut yazar.
`$S3_CA_FILE` boşsa `caSecret` boş kalır (S3 sertifikası herkesçe bilinen bir CA'dandır).
`$S3_EGRESS_CIDRS` virgülle ayrılmış bir listedir; boşsa liste boş kalır.

`[bastion]`

```bash
python3 - <<'PY'
import os, pathlib, json
p = pathlib.Path("platform/values/site/glue.yaml")
s = p.read_text()
ca = '"s3-ca"' if os.environ.get("S3_CA_FILE") else '""'
cidrs = [c.strip() for c in os.environ.get("S3_EGRESS_CIDRS", "").split(",") if c.strip()]
assert '  caSecret: ""' in s and "  egressCidrs: []" in s, "satirlar bulunamadi"
s = s.replace('  caSecret: ""  ', f"  caSecret: {ca}  ", 1)
s = s.replace("  egressCidrs: []", f"  egressCidrs: {json.dumps(cidrs)}", 1)
p.write_text(s)
PY
grep -nE "^  (caSecret|egressCidrs):" platform/values/site/glue.yaml
```

**Beklenen çıktı** (örnek: S3 sertifikası kurumsal CA'lı ve tek IP adresli bir kurulum):

```text
16:  caSecret: "s3-ca"                       # S3 sertifikası ÖZEL CA ile imzalıysa ...
17:  egressCidrs: ["10.20.30.40/32"]         # $S3_EGRESS_CIDRS — S3 veri IP'leri ...
```

`caSecret` doluysa Adım 5.14'te aynı adlı Secret'ı yaratırsınız.

<a id="ad-alani-sabit"></a>
### 4.2 Ad alanının adı değiştirilmez

Bu adımda yapılacak bir iş yoktur; okuyup geçin.

> **`$LAKEHOUSE_NS` her zaman `lakehouse`'tur.** Bu ad, kurumun dokunmadığı ürün
> dosyalarının içinde yazılıdır:
>
> - `platform/apps/*.yaml` — her uygulamanın kurulacağı ad alanı;
> - `platform/values/jupyterhub.yaml` — not defterlerinin Polaris ve Trino adresleri
>   (`polaris.lakehouse.svc`, `trino.lakehouse.svc`);
> - `custom/examples/*` — örnek dosyalardaki `namespace: lakehouse` satırları.
>
> `install/lakehouse.env`'deki değeri tek başına değiştirirseniz kurulum iki ad alanına
> bölünür: Trino Polaris'in Secret'ını bulamaz, not defterleri yanlış adrese bakar.
> Kurumunuzun ad alanı standardı başka bir ad gerektiriyorsa bu bir ürün değişikliğidir;
> ürün ekibine başvurun. Kitaptaki komutlar `$LAKEHOUSE_NS` değişkenini kullanmayı
> sürdürür; değeri `lakehouse`'tur.

### 4.3 Depolamada STS yoksa

[20-on-kosullar](20-on-kosullar.md) madde 4'te depolama ekibinden "STS yok" yanıtını
aldıysanız geçici kimlik dağıtımını kapatın. **STS varsa bu adımı atlayın.**

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

**Beklenen çıktı** (örnek; iki satırda da `false`):

```text
platform/values/site/glue.yaml:12:  vendedCredentials: false
platform/values/site/trino.yaml:44:    iceberg.rest-catalog.vended-credentials-enabled=false
```

**Ters giderse:** iki dosya farklı değer gösteriyorsa Adım 4.5'teki denetim hata verir;
komutları tekrarlayın.

### 4.4 Polaris katalog tanımını düzenleyin

`platform/polaris/setup.yaml` Polaris kataloğunun tanımıdır. `platform/values/site/`
klasöründe değildir ve depoda örnek S3 adresleri taşır. Aşağıdaki komut onları kendi
değerlerinizle değiştirir:

`[bastion]`

```bash
sed -e "s#http://minio.lakehouse.svc:9000#$S3_ENDPOINT#g" \
    -e "s#s3://lakehouse/#s3://$S3_BUCKET_DATA/#g" \
    -e "s#region: us-east-1#region: $S3_REGION#" \
    platform/polaris/setup.yaml > platform/polaris/setup.new
mv platform/polaris/setup.new platform/polaris/setup.yaml
grep -nE "endpoint|base_location|allowed_locations|region:" platform/polaris/setup.yaml
```

**Beklenen çıktı** (örnek: `S3_BUCKET_DATA=kurum-veri`, `S3_REGION=eu-central-1`,
`S3_ENDPOINT=https://s3.kurum.example.net` değerleriyle; siz kendi değerlerinizi
görmelisiniz):

```text
    default_base_location: s3://kurum-veri/
    allowed_locations: [s3://kurum-veri/]
    region: eu-central-1
    endpoint: https://s3.kurum.example.net
    endpoint_internal: https://s3.kurum.example.net
```

STS yoksa (Adım 4.3) aynı bloğa `endpoint_internal` satırının altına, aynı girintiyle
`sts_unavailable: true` satırını ekleyin.

**Ters giderse:** çıktıda hâlâ `region: us-east-1` ya da `minio.lakehouse.svc`
görüyorsanız ilgili `$S3_*` değişkeni boştur (Adım 1). `s3://lakehouse/` görüyorsanız ve
veri bucket'ınızın adı gerçekten `lakehouse` ise sorun yoktur. Girintiyi bozduysanız
`git checkout -- platform/polaris/setup.yaml` ile geri alın.

### 4.5 Ayarları denetleyin

Bazı ayarlar (Keycloak adresi, S3 adresi, AD adresleri) birden fazla dosyada geçer. Betik
bu kopyaların birbirini tuttuğunu ve ürünün gerektirdiği satırların silinmediğini
denetler.

`[bastion]`

```bash
scripts/check-site.sh
```

**Beklenen çıktı:**

```text
check-site: OK
```

**Ters giderse:** her `HATA:` satırı hangi dosyada hangi değerin beklendiğini yazar;
düzeltip betiği tekrar çalıştırın. Hangi dosyadaki değerin asıl olduğunu
[90-referans/values-anahtarlari.md](90-referans/values-anahtarlari.md) §5'teki tablo
söyler. `UYARI:` satırı Polaris dosyasındaki adresin uyuşmadığını söyler; Adım 4.4'ü
atlamışsınızdır. `ModuleNotFoundError: yaml` → `pip3 install --user PyYAML`.

### 4.6 Git'e gönderin

`[bastion]`

```bash
git add platform/values/site platform/polaris/setup.yaml
git commit -m "site: kurulum degerleri"
git push origin main
git show --stat HEAD | grep -c lakehouse.env
```

**Beklenen çıktı** (örnek; commit kimlikleri sizde farklıdır):

```text
[main 4f2a9c1] site: kurulum degerleri
 4 files changed, 31 insertions(+), 31 deletions(-)
To ssh://git.kurum.example.net/veri/lakehouse.git
   1c18c86..4f2a9c1  main -> main
0
```

Son satırdaki `0`, parolaları taşıyan `install/lakehouse.env` dosyasının commit'e
girmediğini gösterir. `tls.caBundle` ayarı şimdilik boştur; Adım 5.11'de doldurulup
ikinci kez gönderilir.

**Ters giderse:**

- `nothing to commit`: dosyalar değişmemiştir; Adım 4.1'i çalıştırmamış olabilirsiniz.
- `Updates were rejected`: depoda başka bir değişiklik vardır;
  `git pull --rebase origin main` çalıştırıp `git push`'u tekrarlayın.
- Son satır `0` değilse `install/lakehouse.env` commit'e girmiştir. Parolaları içeren
  dosya artık depodadır: Git yöneticinizle birlikte geçmişten temizleyin ve dosyadaki
  bütün parolaları değiştirin.

---

## 5. Ad alanı ve Secret'lar

Parolalar Git'e girmez; kurulum başlamadan önce kümede Secret olarak bulunmalıdır. Her
Secret'ı hangi bileşenin okuduğu ve parolaların nasıl yenileneceği
[90-referans/secret-listesi.md](90-referans/secret-listesi.md) dosyasındadır.

Bu bölümde yaratılan her Secret'a `lakehouse.io/yonetim` etiketi eklenir. Etiketin değeri
Secret'ın yaratıldığı adımı gösterir: örneğin `manual-30-kurulum-5.2`, bu belgenin 5.2.
adımıdır. Secret silinirse o adımı tekrarlayarak yeniden yaratırsınız
([değişiklik nasıl uygulanır](50-isletme/degisiklik-nasil-uygulanir.md) §7).

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

**Ters giderse:** `forbidden` → ad alanı açma yetkiniz yoktur; cluster-admin yetkili bir
hesapla girin.

### 5.1 `connect-push` — yalnız kontrol

Bu Secret [20-on-kosullar](20-on-kosullar.md) madde 5'te yaratıldı. Burada yalnız
varlığını kontrol edin:

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" get secret connect-push -o go-template='{{.type}}{{"\n"}}'
```

**Beklenen çıktı:**

```text
kubernetes.io/dockerconfigjson
```

**Ters giderse:** `NotFound` → [20-on-kosullar](20-on-kosullar.md) madde 5'e dönün. Tip
`Opaque` görünüyorsa Secret yanlış komutla yaratılmıştır; silip madde 5'teki
`oc create secret docker-registry` komutunu tekrarlayın.

### 5.2 `s3-creds` — veri S3 anahtarları

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" create secret generic s3-creds \
  --from-literal=AWS_ACCESS_KEY_ID="$S3_ACCESS_KEY" \
  --from-literal=AWS_SECRET_ACCESS_KEY="$S3_SECRET_KEY"
oc -n "$LAKEHOUSE_NS" label secret s3-creds lakehouse.io/yonetim=manual-30-kurulum-5.2
```

**Beklenen çıktı:**

```text
secret/s3-creds created
secret/s3-creds labeled
```

**Ters giderse:** `AlreadyExists` → `oc -n "$LAKEHOUSE_NS" delete secret s3-creds` ile
silip tekrarlayın. Anahtar adları (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
büyük/küçük harf dahil aynen yazılmalıdır.

### 5.3 `backup-s3-creds` — yedek S3 anahtarları

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" create secret generic backup-s3-creds \
  --from-literal=AWS_ACCESS_KEY_ID="$S3_BACKUP_ACCESS_KEY" \
  --from-literal=AWS_SECRET_ACCESS_KEY="$S3_BACKUP_SECRET_KEY"
oc -n "$LAKEHOUSE_NS" label secret backup-s3-creds lakehouse.io/yonetim=manual-30-kurulum-5.3
```

**Beklenen çıktı:**

```text
secret/backup-s3-creds created
secret/backup-s3-creds labeled
```

**Ters giderse:** buraya veri anahtarlarını yazmayın; yedekler kasıtlı olarak **ayrı**
bir anahtar çiftiyle korunur. Anahtarları yanlış yazdıysanız Secret'ı silip tekrarlayın.
Yanlış anahtarla veritabanı yedekleri sessizce başarısız olur; bunu Adım 7.1'deki yedek
kontrolü gösterir.

### 5.4 `polaris-root` — Polaris yönetici kimliği

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" create secret generic polaris-root \
  --from-literal=clientId=root \
  --from-literal=clientSecret="$(openssl rand -hex 24)"
oc -n "$LAKEHOUSE_NS" label secret polaris-root lakehouse.io/yonetim=manual-30-kurulum-5.4
```

**Beklenen çıktı:**

```text
secret/polaris-root created
secret/polaris-root labeled
```

Bu kimlik Polaris kataloğunun yönetici kimliğidir. Yalnız kurulum ve
`scripts/polaris-setup.sh` kullanır; kimseye verilmez.

**Ters giderse:** bu Secret'ı kurulumdan sonra silmeyin. Değer Polaris veritabanına ilk
açılışta yazılır; Secret'ı yeniden yaratmak yetmez, veritabanını yeniden kurmak gerekir.

### 5.5 `keycloak-admin` — Keycloak yönetici hesabı

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" create secret generic keycloak-admin \
  --from-literal=username=admin \
  --from-literal=password="${KEYCLOAK_ADMIN_PASSWORD:-$(openssl rand -hex 24)}"
oc -n "$LAKEHOUSE_NS" label secret keycloak-admin lakehouse.io/yonetim=manual-30-kurulum-5.5
oc -n "$LAKEHOUSE_NS" get secret keycloak-admin \
  -o jsonpath='{.data.password}' | base64 -d | head -c 8; echo " ... (ilk 8 karakter)"
```

**Beklenen çıktı** (örnek; parolayı **şimdi** kurumun parola kasasına kaydedin):

```text
secret/keycloak-admin created
secret/keycloak-admin labeled
9f3c1ab2 ... (ilk 8 karakter)
```

`KEYCLOAK_ADMIN_PASSWORD` boşsa parola üretilir ve yalnız Secret'ta durur. Tamamını
görmek için son komuttan `| head -c 8` kısmını silin. Bu hesap günlük kullanım için
değildir: kullanıcılar AD'den gelir, yönetici hesabı yalnız Keycloak bakımı içindir.

### 5.6 `keycloak-clients` — uygulama sırları ve AD hesabının parolası

Bu Secret, Keycloak yapılandırması ilk kez yüklenmeden önce kümede olmalıdır. Keycloak
yapılandırmayı yalnız bir kez yükler; eksik başlarsanız AD bağlantısı hiç kurulmaz ve
yapılandırmanın silinip yeniden yüklenmesi gerekir (§8).

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" create secret generic keycloak-clients \
  --from-literal=trino="$(openssl rand -hex 24)" \
  --from-literal=superset="$(openssl rand -hex 24)" \
  --from-literal=jupyterhub="$(openssl rand -hex 24)" \
  --from-literal=ldap-bind="$LDAP_BIND_PASSWORD"
oc -n "$LAKEHOUSE_NS" label secret keycloak-clients lakehouse.io/yonetim=manual-30-kurulum-5.6
```

**Beklenen çıktı:**

```text
secret/keycloak-clients created
secret/keycloak-clients labeled
```

**Ters giderse:** dört anahtarın dördü de gereklidir. `ldap-bind` AD servis hesabının
parolasıdır; ayar dosyalarına asla yazılmaz.

### 5.7 `trino-service-accounts` — Superset ve Zeppelin'in Trino parolaları

Superset ve Zeppelin Trino'ya kullanıcının kimliğiyle değil, kendi servis hesaplarıyla
bağlanır. Trino parolaları şifrelenmiş bir dosyadan (`password.db`) okur; Superset ve
Zeppelin ise aynı parolaların düz hâline ihtiyaç duyar. İkisi aynı Secret'ta durur ve
aynı parolalardan üretilmelidir.

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
oc -n "$LAKEHOUSE_NS" label secret trino-service-accounts \
  lakehouse.io/yonetim=manual-30-kurulum-5.7
shred -u /tmp/password.db
```

**Beklenen çıktı:**

```text
secret/trino-service-accounts created
secret/trino-service-accounts labeled
```

`$ZEPPELIN_TRINO_PW` Adım 5.10'da yeniden kullanılır; terminali kapatmayın. Kapattıysanız
Adım 5.7 ile 5.10'u birlikte tekrarlayın.

**Ters giderse:** `htpasswd: command not found` → `httpd-tools` paketini kurun. Kurulumdan
sonra Superset ya da Zeppelin Trino'ya `Access Denied` ile bağlanamıyorsa düz parolalar
`password.db` ile uyuşmuyordur; bu adımı ve Adım 5.10'u baştan tekrarlayın.

### 5.8 `trino-shared-secret` ve `superset-secret`

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" create secret generic trino-shared-secret \
  --from-literal=secret="$(openssl rand -hex 32)"
oc -n "$LAKEHOUSE_NS" create secret generic superset-secret \
  --from-literal=secret-key="$(openssl rand -hex 32)"
oc -n "$LAKEHOUSE_NS" label secret trino-shared-secret superset-secret \
  lakehouse.io/yonetim=manual-30-kurulum-5.8
```

**Beklenen çıktı:**

```text
secret/trino-shared-secret created
secret/superset-secret created
secret/trino-shared-secret labeled
secret/superset-secret labeled
```

İlki Trino sunucularının kendi aralarındaki iletişimi imzalar; eksikse Trino işçileri
koordinatöre katılamaz. İkincisi Superset oturumlarını imzalar; sonradan değişirse açık
oturumlar kapanır. İkisi de yalnız küme içinde kullanılır.

### 5.9 `zeppelin-shiro` — Zeppelin'in AD girişi

Zeppelin Keycloak'ı kullanmaz; AD'ye doğrudan bağlanır ve yetkiyi AD grup üyeliğinden
okur. Ayarların şablonu `examples/zeppelin/shiro-ad.ini` dosyasıdır. Aşağıdaki komut
şablonu değerlerinizle doldurur. AD parolası her karakteri içerebildiği için doldurma
`python3` ile yapılır.

`[bastion]`

```bash
python3 - > /tmp/shiro.ini <<'PY'
import os, pathlib
src = pathlib.Path("examples/zeppelin/shiro-ad.ini").read_text()
repl = {"activeDirectoryRealm.systemUsername":  os.environ["LDAP_BIND_USER"],
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
oc -n "$LAKEHOUSE_NS" label secret zeppelin-shiro lakehouse.io/yonetim=manual-30-kurulum-5.9
shred -u /tmp/shiro.ini
```

**Beklenen çıktı** (ilk satır: şablonda örnek değer kalmadı):

```text
0
secret/zeppelin-shiro created
secret/zeppelin-shiro labeled
```

Zeppelin AD'ye Keycloak'ın kullandığı servis hesabıyla bağlanır, ama hesabın tam adresini
(DN) değil **kısa adını** (`$LDAP_BIND_USER`, ör. `svc-lakehouse`) kullanır. Zeppelin
`$AD_UPN_SUFFIX` değerini hesap adının sonuna ekler; DN yazılırsa geçersiz bir ad oluşur ve
**hiç kimse** giriş yapamaz (`LDAP: error code 49 ... data 52e`). Ayrı bir servis hesabı
kullanmak istiyorsanız Secret'ı yaratmadan önce geçici dosyadaki `systemUsername` ve
`systemPassword` satırlarını elle değiştirin.

**Ters giderse:**

- `KeyError`: ilgili `$LDAP_*` ya da `$AD_UPN_SUFFIX` değişkeni boştur (Adım 1).
- `grep -c` sıfırdan büyük bir sayı yazıyorsa `$LDAP_GROUPS_DN` şablondaki biçimden
  farklıdır; Secret'ı yaratmadan önce geçici dosyayı açıp grup adreslerine bakın.
- Kurulumdan sonra Zeppelin girişi `PKIX path building failed` derse AD sertifikasının
  kök CA'sı eksiktir; Adım 5.13.

### 5.10 `zeppelin-interpreter` — Zeppelin'in Trino bağlantısı

Zeppelin'in Trino bağlantı ayarı ürün içinde bir şablondur
(`glue/files/zeppelin/interpreter.json`). Aşağıdaki komut şablonu doldurur. Zeppelin bu
dosyayı yalnız ilk açılışta okur.

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
oc -n "$LAKEHOUSE_NS" label secret zeppelin-interpreter \
  lakehouse.io/yonetim=manual-30-kurulum-5.10
shred -u /tmp/interpreter.json
```

**Beklenen çıktı:**

```text
gecerli JSON
secret/zeppelin-interpreter created
secret/zeppelin-interpreter labeled
```

**Ters giderse:** `JSONDecodeError` → `$ZEPPELIN_TRINO_PW` boştur; Adım 5.7'yi tekrarlayın.
Dosyada kalan tek `{{applicationId}}` Zeppelin'in kendi değişkenidir; doldurulmaz.

### 5.11 `lakehouse-ca` — iç kök sertifika ve `tls.caBundle`

Trino bağlantılarını kendisi şifreler. Trino'nun sertifikasını cert-manager bu kök
sertifikayla imzalar. Aynı kök, Superset, Zeppelin ve not defterlerinin Trino'ya güvenmesi
için de kullanılır. Kök sertifika ayrıca ayar dosyasına (`tls.caBundle`) yazılır; böylece
OpenShift Trino'nun dış adresinde de geçerli bir sertifika sunar.

`[bastion]`

```bash
openssl req -x509 -newkey rsa:4096 -days 3650 -nodes \
  -subj "/CN=lakehouse-ca" \
  -addext "basicConstraints=critical,CA:TRUE" \
  -addext "keyUsage=critical,keyCertSign,cRLSign" \
  -keyout /tmp/ca.key -out /tmp/ca.crt
openssl x509 -in /tmp/ca.crt -noout -ext keyUsage
oc -n "$LAKEHOUSE_NS" create secret tls lakehouse-ca --cert=/tmp/ca.crt --key=/tmp/ca.key
oc -n "$LAKEHOUSE_NS" label secret lakehouse-ca lakehouse.io/yonetim=manual-30-kurulum-5.11
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

**Beklenen çıktı** (`openssl` bunlardan önce birkaç satır nokta ve artı işareti yazar):

```text
X509v3 Key Usage: critical
    Certificate Sign, CRL Sign
secret/lakehouse-ca created
secret/lakehouse-ca labeled
-----BEGIN CERTIFICATE-----
```

İki `-addext` satırını silmeyin. `Key Usage` bilgisi olmayan bir kök sertifikayı not
defterlerindeki Python reddeder ve not defterleri Trino'ya
`CA cert does not include key usage extension` hatasıyla bağlanamaz.

`~/lakehouse-ca.crt` dosyasını saklayın: kullanıcıların tarayıcılarına ve JDBC
istemcilerine bu sertifika eklenir.

**Kurumsal sertifika altyapınız varsa** kök yerine kendi ara CA'nızın sertifikasını ve
anahtarını kullanabilirsiniz
(`oc create secret tls lakehouse-ca --cert=ara-ca.crt --key=ara-ca.key`, ardından aynı
`oc label` komutu). Ara CA sertifikasında `Key Usage: Certificate Sign` bulunmalıdır;
yukarıdaki `openssl x509` komutuyla bakın.

Değişikliği Git'e gönderin:

`[bastion]`

```bash
scripts/check-site.sh
git add platform/values/site/glue.yaml
git commit -m "site: lakehouse-ca caBundle"
git push origin main
```

**Beklenen çıktı** (örnek; commit kimlikleri sizde farklıdır):

```text
check-site: OK
[main a7c3d90] site: lakehouse-ca caBundle
 1 file changed, 1 insertion(+), 1 deletion(-)
To ssh://git.kurum.example.net/veri/lakehouse.git
   4f2a9c1..a7c3d90  main -> main
```

**Ters giderse:** `caBundle satiri bulunamadi` → dosyadaki `tls: {caBundle: ""}` satırı
değişmiştir. `git checkout -- platform/values/site/glue.yaml` ile ilk hâline dönüp Adım
4.1'den itibaren tekrarlayın. `caBundle` boş kalırsa kurulum yine çalışır, ama Trino'nun dış
adresi bu kök sertifikayı sunar ve tarayıcıların ona güvenmesi gerekir.

### 5.12 `jupyterhub-secrets` — oturum ve kimlik saklama anahtarları

JupyterHub bu iki değeri bulamazsa her güncellemede yenisini üretir. Kurulumdan sonra
değişirlerse bütün kullanıcı oturumları kapanır ve saklanan kimlik bilgileri okunamaz.
Anahtar adları JupyterHub'ın beklediği adlardır; harfi harfine yazılır.

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" create secret generic jupyterhub-secrets \
  --from-literal=hub.config.JupyterHub.cookie_secret="$(openssl rand -hex 32)" \
  --from-literal=hub.config.CryptKeeper.keys="$(openssl rand -hex 32)"
oc -n "$LAKEHOUSE_NS" label secret jupyterhub-secrets \
  lakehouse.io/yonetim=manual-30-kurulum-5.12
```

**Beklenen çıktı:**

```text
secret/jupyterhub-secrets created
secret/jupyterhub-secrets labeled
```

Bu Secret'ta yalnız iki anahtar vardır. Hub ile proxy arasındaki jetonu JupyterHub kendi
`hub` Secret'ında üretir ve saklar; buraya eklenmez. O jetonun yenilenmesi
[90-referans/secret-listesi.md](90-referans/secret-listesi.md) §4'tedir.

**Ters giderse:** anahtar adlarında bir harf bile yanlışsa JupyterHub değeri bulamaz ve
yine kendisi üretir; adları Adım 5.15'teki listeyle karşılaştırın.

### 5.13 `ad-ca` — AD'nin kök sertifikası (koşullu)

**Ne zaman gerekir:** AD sunucusunun sertifikası kurumun kendi CA'sıyla imzalıysa
Keycloak, Trino ve Zeppelin AD'ye güvenli bağlanamaz (`PKIX path building failed`). Bu
durumda bu adım **zorunludur**. Sertifika herkesçe bilinen bir CA'dan alınmışsa gerekmez.
Kök sertifikayı [20-on-kosullar](20-on-kosullar.md) madde 6'da `$LDAP_CA_FILE` olarak
almıştınız.

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" create secret generic ad-ca --from-file=ca.crt="$LDAP_CA_FILE"
oc -n "$LAKEHOUSE_NS" label secret ad-ca lakehouse.io/yonetim=manual-30-kurulum-5.13
```

**Beklenen çıktı:**

```text
secret/ad-ca created
secret/ad-ca labeled
```

**Bu Secret'ı kim okur.** Ürün bu Secret'ı üç bileşene kendiliğinden bağlar. Elle bir şey
eklemeniz, özel imaj üretmeniz ya da çalışan bir konteynerin içini düzenlemeniz
gerekmez.

| Bileşen | Ne için | Konteynerdeki yol |
|---|---|---|
| Keycloak | AD kullanıcılarını okumak | `/opt/keycloak/conf/truststores/` |
| Trino | kullanıcıların AD gruplarını okumak | `/etc/trino/s3-truststore/truststore.p12` |
| Zeppelin | AD ile giriş | `/truststore/ad-truststore.p12` |

Üçünü birden açıp kapatan tek ayar `platform/values/site/glue.yaml` içindeki
`keycloak.ldap.caSecret` ayarıdır (değeri `ad-ca`).

**Bu Secret'a ihtiyacınız yoksa** (AD sertifikası herkesçe bilinen bir CA'dan geliyorsa)
`platform/values/site/glue.yaml` dosyasında `keycloak.ldap.caSecret: ""` yazın. Trino
için ayrıca bir şey yapılmaz; Secret yoksa Trino onu atlar.

> **Trino ayarlarına `ldap.ssl.truststore.path` satırı eklemeyin.** Bu satırla Trino
> kullanıcıların AD gruplarını zaman zaman okuyamaz; analistler
> `Access Denied: Cannot access catalog lakehouse` hatası alır. AD kök sertifikası bu
> yüzden Trino'ya Java'nın güven deposu üzerinden verilir. `scripts/check-site.sh` bu
> satırı görürse hata verir.

#### Doğrulama (Adım 6'dan **sonra** çalıştırın)

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" exec keycloak-0 -- ls /opt/keycloak/conf/truststores
oc -n "$LAKEHOUSE_NS" logs deploy/trino-coordinator -c s3-truststore
oc -n "$LAKEHOUSE_NS" logs deploy/zeppelin -c ad-truststore --tail=1
```

**Beklenen çıktı** (S3 sertifikanız kurumsal CA'lı değilse ikinci satır
`JDK varsayilan` olur):

```text
secret-ad-ca
S3 CA truststore
Certificate was added to keystore
AD CA eklendi
147
```

Son satır, Zeppelin'in güven deposundaki **toplam** sertifika sayısıdır: Java'nın
varsayılan kökleri **ve** AD kökünüz. Onlarca olması beklenir. `1` görürseniz depoda yalnız
AD kökü vardır; bu durumda Zeppelin'in internetteki adreslere TLS bağlantıları kırılır. AD
kökünün depoda olduğunu görmek için:

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" exec deploy/zeppelin -c zeppelin -- \
  keytool -list -keystore /truststore/ad-truststore.p12 -storetype PKCS12 \
  -storepass changeit | grep ad-ca
```

**Beklenen çıktı** (tarih, Zeppelin pod'unun son açıldığı gündür):

```text
ad-ca, Nov 11, 2026, trustedCertEntry,
```

`changeit` bir parola değildir; Java güven depolarının herkesçe bilinen varsayılanıdır.
Depoda yalnız açık sertifikalar vardır.

**Ters giderse:**

- `error reading ...: no such file`: `$LDAP_CA_FILE` yolu yanlıştır. Dosyanın bir kök
  sertifika olduğunu [20-on-kosullar](20-on-kosullar.md) madde 6'daki
  `openssl x509 -noout -subject -issuer` komutuyla kontrol edin.
- Bir bileşenin günlüğünde `PKIX path building failed` ya da
  `unable to find valid certification path to requested target`: Secret eksiktir, yanlış
  kökü taşıyordur ya da bileşen onu okumamıştır. Yukarıdaki doğrulama komutlarını sırayla
  çalıştırın.
- Zeppelin pod'u açılmıyor ve `ad-truststore` adımı `Error` durumundaysa `ca.crt` PEM
  biçiminde değildir; `openssl x509 -inform der -in ad-ca.cer -out ad-ca.pem` ile çevirip
  Secret'ı yeniden yaratın.

<a id="s3-ca"></a>
### 5.14 `s3-ca` — S3'ün kök sertifikası (koşullu)

**Ne zaman gerekir:** S3 sertifikası kurumun kendi CA'sıyla imzalıysa ya da kendinden
imzalıysa (bazı depolama cihazları fabrikadan böyle gelir). Yani `$S3_CA_FILE` doluysa ve
Adım 4.1'de `caSecret: "s3-ca"` yazıldıysa. Sertifika herkesçe bilinen bir CA'dan
geliyorsa **bu adımı atlayın**.

**Ne yapar:** S3'e bağlanan her bileşen (yedekleme, Kafka Connect, Spark, Polaris, Trino,
not defterleri) S3'ün kök sertifikasını bu Secret'tan alır. Secret'ta iki dosya vardır:

- `ca.crt` — depolama ekibinin verdiği kök sertifika, olduğu gibi;
- `truststore.p12` — Java'nın güvendiği bütün kökler **ve** bu sertifika. Java ile
  çalışan bileşenler bunu kullanır. Herkesçe bilinen kökler de içinde olduğu için bu
  bileşenlerin internet bağlantıları bozulmaz.

`truststore.p12` dosyasını Java ile gelen `keytool` aracı üretir (RHEL/Fedora:
`sudo dnf install -y java-21-openjdk-headless`). Aşağıdaki komutları **sırayla, aynı
terminalde** çalıştırın:

`[bastion]`

```bash
openssl x509 -in "$S3_CA_FILE" -noout -subject -issuer -enddate
JHOME=$(java -XshowSettings:properties -version 2>&1 | sed -n 's/^ *java.home = //p')
rm -f s3-truststore.p12
keytool -importkeystore -noprompt -srckeystore "$JHOME/lib/security/cacerts" \
  -srcstorepass changeit -destkeystore s3-truststore.p12 -deststoretype PKCS12 \
  -deststorepass changeit
keytool -importcert -noprompt -alias s3-ca -file "$S3_CA_FILE" \
  -keystore s3-truststore.p12 -storepass changeit
keytool -list -keystore s3-truststore.p12 -storepass changeit | grep -E "contains|^s3-ca"
oc -n "$LAKEHOUSE_NS" create secret generic s3-ca \
  --from-file=ca.crt="$S3_CA_FILE" --from-file=truststore.p12=s3-truststore.p12
oc -n "$LAKEHOUSE_NS" label secret s3-ca lakehouse.io/yonetim=manual-30-kurulum-5.14
rm -f s3-truststore.p12
```

**Beklenen çıktı** (sertifikanın adı, tarih ve sayılar sizde farklıdır; önemli olan
`s3-ca, ... trustedCertEntry` satırıdır):

```text
subject=CN=ingress-operator@1794001995
issuer=CN=ingress-operator@1794001995
notAfter=Nov  5 21:53:15 2028 GMT
Import command completed:  110 entries successfully imported, 0 entries failed or cancelled
Certificate was added to keystore
Your keystore contains 111 entries
s3-ca, 8 Kas 2026, trustedCertEntry,
secret/s3-ca created
secret/s3-ca labeled
```

**Ters giderse:**

- `keytool: command not found`: Java kurulu değildir; yukarıdaki `dnf` komutunu
  çalıştırın.
- `java.home` boş geliyorsa `JHOME` değişkenini elle verin (ör. `JHOME=/usr/lib/jvm/jre`).
- `Keystore was tampered with, or password was incorrect`: Java'nın varsayılan güven
  deposunun parolası farklıdır; çoğu dağıtımda `changeit`'tir, değilse dağıtımınızın
  belgesine bakın.
- `openssl x509` `unable to load certificate` diyorsa dosya PEM biçiminde değildir;
  `openssl x509 -inform der -in s3-ca.cer -out s3-ca.pem` ile çevirin.
- Secret'ı kurulumdan **sonra** yaratırsanız ya da değiştirirseniz bileşenleri yeniden
  başlatmak gerekir ([50-isletme/sorun-giderme.md](50-isletme/sorun-giderme.md)).

### 5.15 Hepsini birden kontrol edin

`[bastion]`

```bash
for s in connect-push s3-creds backup-s3-creds polaris-root keycloak-admin \
         keycloak-clients trino-service-accounts trino-shared-secret superset-secret \
         zeppelin-shiro zeppelin-interpreter lakehouse-ca jupyterhub-secrets ad-ca s3-ca; do
  printf '%-24s %s\n' "$s" \
    "$(oc -n "$LAKEHOUSE_NS" get secret "$s" \
        -o go-template='{{range $k,$v := .data}}{{$k}} {{end}}' 2>/dev/null || echo EKSIK)"
done
```

**Beklenen çıktı:**

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
s3-ca                    ca.crt truststore.p12
```

Son iki satır koşulludur:

- `ad-ca`: AD sertifikanız herkesçe bilinen bir CA'dan geliyorsa Adım 5.13'ü
  atlamışsınızdır ve `EKSIK` yazması normaldir. Kurumsal CA kullanan bir AD'de dolu
  olmalıdır.
- `s3-ca`: `platform/values/site/glue.yaml` dosyasında `caSecret: "s3-ca"` yazıyorsa dolu
  olmalıdır (Adım 5.14); boşsa `EKSIK` normaldir.

**Ters giderse:** `EKSIK` yazan her zorunlu Secret için ilgili adımı tekrarlayın. Bu
listeyi Adım 6'dan **önce** eksiksiz görmelisiniz; eksik bir Secret'la başlayan kurulumda
ilgili pod `CreateContainerConfigError` durumunda bekler.

---

## 6. Kurulumu başlatın

`bootstrap/bootstrap.sh` ArgoCD'ye tek bir **kök uygulama** yazar. Kök uygulama
`platform/apps/` klasöründeki bütün bileşen uygulamalarını yaratır ve hepsinin aynı Git
deposunu ve aynı dalı okumasını sağlar. OpenShift'te ArgoCD'yi GitOps operatörü yönettiği
için betik ArgoCD'yi ayrıca kurmaz.

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

İlk satır mutlaka görünmelidir: ArgoCD'yi operatör yönetir. Bu satır yoksa betik ArgoCD'yi
kendisi kurmaya başlamıştır; hemen durdurun (`Ctrl-C`) ve
[20-on-kosullar](20-on-kosullar.md) madde 2'ye dönün.

Çıktıda iki `Warning:` satırı da görebilirsiniz (`missing the
kubectl.kubernetes.io/last-applied-configuration annotation` ve `prefer a domain-qualified
finalizer name`); ikisi de zararsızdır. Son satırdaki `ArgoCD v3.5.2` betiğin kendi sürüm
bilgisidir; OpenShift'te çalışan ArgoCD, GitOps operatörünün sürümüdür.

`--revision main` yerine bir etiket ya da commit kimliği verebilirsiniz; ArgoCD o noktayı
uygular ve orada kalır.

**Ters giderse:** `bilinmeyen argüman` → bir seçeneğin adı yanlış yazılmıştır.
`Unauthorized` → `oc login` oturumunuz sona ermiştir. Kök uygulama
`repository not accessible` diyorsa Adım 3.2'deki etiket eksiktir.

### 6.1 Uygulamaların ayağa kalkmasını izleyin

`[bastion]`

```bash
watch -n 30 "oc -n $ARGOCD_NS get applications"
```

`watch` ekranı her 30 saniyede yenilenir; çıkmak için `Ctrl-C`.

**Beklenen çıktı** (örnek; 40–60 dakika sonra, `trino` dışındaki uygulamalar hazır
olduğunda):

```text
NAME                SYNC STATUS   HEALTH STATUS
cert-manager        Synced        Healthy
cnpg                Synced        Healthy
cnpg-barman         Synced        Healthy
custom              Synced        Healthy
glue                Synced        Healthy
jupyterhub          Synced        Healthy
keycloak-operator   Synced        Healthy
lakehouse-root      Synced        Healthy
polaris             Synced        Healthy
spark-operator      Synced        Healthy
strimzi             Synced        Healthy
superset-operator   Synced        Healthy
trino               OutOfSync     Progressing
```

**Ne kadar sürer:** cert-manager ve operatörler 2–5 dakikada hazır olur. `glue` en uzun
sürendir (Kafka Connect imajının üretilmesi, üç PostgreSQL veritabanının kurulması,
imajların indirilmesi): 30–45 dakika.

**`trino` bu aşamada hazır olamaz; bu normaldir.** Trino, Polaris kataloğu kurulurken
yaratılan `polaris-trino` Secret'ını bekler. Katalog bir sonraki bölümde kurulur; o zaman
`trino` da `Synced Healthy` olur.

İzleme ve yedekleme için ayrı bir uygulama yoktur: OpenShift'in kendi izlemesi ve OADP
kullanılır.

**Ters giderse:** bir uygulama uzun süre `Progressing` kalıyorsa nedeni şu iki komutun
çıktısındadır (`ADI` yerine uygulamanın adını yazın):

```bash
oc -n "$ARGOCD_NS" get application ADI -o jsonpath='{.status.conditions}{"\n"}'
oc -n "$LAKEHOUSE_NS" get pods
```

| Ne görüyorsunuz | Neden |
|---|---|
| pod `ImagePullBackOff` | kümenin dış erişim izinleri eksiktir ([20-on-kosullar](20-on-kosullar.md) madde 10) |
| pod `CreateContainerConfigError` | bir Secret eksiktir (Adım 5.15) |
| PVC `Pending` | StorageClass yoktur ([20-on-kosullar](20-on-kosullar.md) madde 3) |
| pod `ContainerCreating`'de kalıyor, olaylarda `does not contain driver` | pod depolama sürücüsü olmayan bir düğüme düşmüştür ([20-on-kosullar](20-on-kosullar.md) madde 3.1) |

İlk kurulumda iki ArgoCD durumu sık görülür:

- **`SYNC STATUS` sütununda `Unknown`**, koşullarda `DeadlineExceeded` ya da
  `failed to generate manifest`: ArgoCD bileşenin paketini internetten indiremedi. Önce
  [20-on-kosullar](20-on-kosullar.md) madde 10'daki erişimi kontrol edin. Erişim varsa
  hata geçicidir; uygulamayı yenileyin:

  `[bastion]`

  ```bash
  oc -n "$ARGOCD_NS" annotate application ADI argocd.argoproj.io/refresh=hard --overwrite
  ```

- **Bir düzeltmeyi Git'e gönderdiniz ama uygulama hâlâ eski hatayı veriyor:** ArgoCD
  başarısız bir uygulamayı, başladığı eski commit ile denemeye devam eder. Süren işlemi
  durdurun; ArgoCD yeni commit ile yeniden başlar:

  `[bastion]`

  ```bash
  oc -n "$ARGOCD_NS" patch application ADI --type merge \
    -p '{"status":{"operationState":{"phase":"Terminating"}}}'
  ```

Bütün belirtiler: [50-isletme/sorun-giderme.md](50-isletme/sorun-giderme.md) §3.

---

## 7. Kurulumu doğrulayın

Aşağıdaki komutlar bileşenleri birbirine bağımlı oldukları sırayla bekler. Biri takılırsa
sonrakileri çalıştırmayın; önce takılanı çözün.

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

**Beklenen çıktı** (`AGE` sütunu sizde farklıdır; her veritabanında 2 kopya çalışır):

```text
kafka.kafka.strimzi.io/lakehouse condition met
kafkaconnect.kafka.strimzi.io/connect condition met
NAME          AGE     INSTANCES   READY   STATUS                     PRIMARY
keycloak-db   3d4h    2           2       Cluster in healthy state   keycloak-db-1
polaris-db    3d4h    2           2       Cluster in healthy state   polaris-db-1
superset-db   3d4h    2           2       Cluster in healthy state   superset-db-1
deployment "polaris" successfully rolled out
keycloak.k8s.keycloak.org/keycloak condition met
keycloakrealmimport.k8s.keycloak.org/lakehouse-realm condition met
```

**Ters giderse:**

- `kafka` beklemede kalıyorsa Kafka pod'larının diskleri (PVC) bağlanmamıştır.
- `kafkaconnect` en uzun süren adımdır, çünkü önce imajı üretilir. İlerlemeyi
  `oc -n "$LAKEHOUSE_NS" get pods | grep connect-build` ile izleyin. `unauthorized` hatası
  `connect-push` jetonunun geçersiz olduğunu gösterir.
- `keycloakrealmimport` `Done` olmuyorsa
  `oc -n "$LAKEHOUSE_NS" logs job/lakehouse-realm` çıktısına bakın; çoğu zaman Adım 5.6'daki
  Secret eksiktir.

Bütün belirtiler: [50-isletme/sorun-giderme.md](50-isletme/sorun-giderme.md) §3.

**Yedekler gerçekten S3'e gidiyor mu?** `Cluster in healthy state` yalnız veritabanının
çalıştığını söyler. S3'e ulaşılamıyorsa (yanlış anahtar, eksik kök sertifika) veritabanı
sağlıklı görünür ama hiç yedek alınmaz. Bunu ayrıca kontrol edin (ilk yedeğin yazılması
birkaç dakika sürebilir):

`[bastion]`

```bash
for c in polaris-db keycloak-db superset-db; do
  printf '%-12s %s\n' "$c" "$(oc -n "$LAKEHOUSE_NS" get cluster.postgresql.cnpg.io "$c" \
    -o jsonpath='{.status.conditions[?(@.type=="ContinuousArchiving")].reason}')"
done
```

**Beklenen çıktı:**

```text
polaris-db   ContinuousArchivingSuccess
keycloak-db  ContinuousArchivingSuccess
superset-db  ContinuousArchivingSuccess
```

**Ters giderse:** `ContinuousArchivingFailing` görürseniz nedenine bakın:

```bash
oc -n "$LAKEHOUSE_NS" logs polaris-db-1 -c plugin-barman-cloud --tail=50 | grep ERROR
```

- `SSL validation failed ... CERTIFICATE_VERIFY_FAILED`: S3'ün kök sertifikası eksiktir
  (Adım 4.1 `caSecret` ve Adım 5.14).
- `AccessDenied`: `backup-s3-creds` anahtarının yedek bucket'ına yetkisi yoktur (Adım 5.3).

Kök sertifikayı ya da anahtarı sonradan düzelttiyseniz veritabanı pod'larını yeniden
başlatın:

`[bastion]`

```bash
T=$(date -u +%Y-%m-%dT%H:%M:%SZ)
for c in polaris-db keycloak-db superset-db; do
  oc -n "$LAKEHOUSE_NS" annotate cluster.postgresql.cnpg.io "$c" \
    kubectl.kubernetes.io/restartedAt="$T" --overwrite
done
```

### 7.2 Hata durumunda pod kalmadı

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" get pods \
  --field-selector=status.phase!=Running,status.phase!=Succeeded
```

**Beklenen çıktı** (örnek; bu aşamada yalnız Trino pod'ları listelenmelidir):

```text
NAME                                 READY   STATUS                       RESTARTS   AGE
trino-coordinator-6bd58cb48-6txtr    0/1     CreateContainerConfigError   0          12m
trino-worker-5fd66b56b7-zcfxk        0/1     CreateContainerConfigError   0          12m
trino-worker-5fd66b56b7-nxtz9        0/1     CreateContainerConfigError   0          12m
```

Trino pod'ları Polaris kataloğu kurulana kadar bekler
([40-kurulum-sonrasi](40-kurulum-sonrasi.md) §2).

**Ters giderse:** listedeki her pod için `oc -n "$LAKEHOUSE_NS" describe pod ADI`
çıktısının `Events` bölümüne bakın. Trino dışında bir pod `CreateContainerConfigError`
gösteriyorsa Adım 5.15'i tekrarlayın.

### 7.3 Dış adresler

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" get routes
for r in keycloak superset jupyterhub zeppelin; do
  host=$(oc -n "$LAKEHOUSE_NS" get route "$r" -o jsonpath='{.spec.host}')
  printf '%-12s %-44s %s\n' "$r" "$host" \
    "$(curl -sS -o /dev/null -w '%{http_code}' --max-time 15 "https://$host/")"
done
```

Adresler kümeden okunur; `trino.hostname` gibi ayarlarla kurumsal bir ad verdiyseniz
döngü yine doğru adresi dener.

**Beklenen çıktı** (alan adı sizinkidir. `200` ve `302` "çalışıyor" demektir; giriş
sayfasına yönlendiren adresler `302` verir):

```text
NAME                        HOST/PORT                                                          PATH   SERVICES                             PORT    TERMINATION   WILDCARD
jupyterhub                  jupyterhub-lakehouse.apps.ocpphy.solusbilisim.com                         proxy-public                         http    edge          None
keycloak                    keycloak-lakehouse.apps.ocpphy.solusbilisim.com                           keycloak-service                     http    edge          None
lakehouse-dual-role-0       lakehouse-dual-role-0-lakehouse.apps.ocpphy.solusbilisim.com              lakehouse-dual-role-0                9094    passthrough   None
lakehouse-dual-role-1       lakehouse-dual-role-1-lakehouse.apps.ocpphy.solusbilisim.com              lakehouse-dual-role-1                9094    passthrough   None
lakehouse-dual-role-2       lakehouse-dual-role-2-lakehouse.apps.ocpphy.solusbilisim.com              lakehouse-dual-role-2                9094    passthrough   None
lakehouse-kafka-bootstrap   lakehouse-kafka-bootstrap-lakehouse.apps.ocpphy.solusbilisim.com          lakehouse-kafka-external-bootstrap   9094    passthrough   None
polaris                     polaris-lakehouse.apps.ocpphy.solusbilisim.com                            polaris                              8181    edge          None
superset                    superset-lakehouse.apps.ocpphy.solusbilisim.com                           superset-web-server                  http    edge          None
trino                       trino-lakehouse.apps.ocpphy.solusbilisim.com                              trino                                https   reencrypt     None
zeppelin                    zeppelin-lakehouse.apps.ocpphy.solusbilisim.com                           zeppelin                             http    edge          None
keycloak     keycloak-lakehouse.apps.ocpphy.solusbilisim.com 302
superset     superset-lakehouse.apps.ocpphy.solusbilisim.com 302
jupyterhub   jupyterhub-lakehouse.apps.ocpphy.solusbilisim.com 302
zeppelin     zeppelin-lakehouse.apps.ocpphy.solusbilisim.com 200
```

`lakehouse-kafka-bootstrap` ve `lakehouse-dual-role-*` satırları Kafka'nın dış
adresleridir ve yalnız nginx günlükleri ya da JSON olay akışları açıksa görünür. Trino'nun
adresi `tls.caBundle` doluysa `reencrypt`, boşsa `passthrough` olur. Trino'yu bu döngüde
denemeyin: giriş zorunlu olduğu için `401` döner. `polaris` adresi katalog API'sidir,
kullanıcı arayüzü değildir.

**Ters giderse:**

| Ne görüyorsunuz | Neden |
|---|---|
| `NotFound` | `glue` uygulaması adresleri henüz yaratmamıştır (Adım 6.1) |
| `000` | DNS çözülmüyor ya da güvenlik duvarı kapalı ([20-on-kosullar](20-on-kosullar.md) madde 7); kurumsal ad kullandıysanız DNS kaydını kontrol edin |
| `503` | adresin arkasındaki pod henüz hazır değil; birkaç dakika bekleyin |
| `000` ve öncesinde `curl: (60) SSL certificate problem` | OpenShift'in dış adres sertifikası herkesçe bilinen bir CA'dan değil (yeni kümelerde olağandır) |

Son durumda OpenShift'in kendi kök sertifikasını alıp `curl`'e verin:

`[bastion]`

```bash
oc -n openshift-ingress-operator get secret router-ca \
  -o jsonpath='{.data.tls\.crt}' | base64 -d > ~/router-ca.crt
for r in keycloak superset jupyterhub zeppelin; do
  host=$(oc -n "$LAKEHOUSE_NS" get route "$r" -o jsonpath='{.spec.host}')
  printf '%-12s %s\n' "$r" "$(curl -sS -o /dev/null -w '%{http_code}' --max-time 15 \
    --cacert ~/router-ca.crt "https://$host/")"
done
```

OpenShift kurumsal bir sertifika kullanıyorsa `~/router-ca.crt` yerine kurumun kök
sertifikasını verin. Tarayıcılar da aynı uyarıyı verir; kalıcı çözüm OpenShift'e kurumsal
sertifika takılmasıdır (platform ekibinin işi). Ürünün bileşenleri bu sertifikaya ihtiyaç
duymaz; birbirleriyle küme içinden konuşurlar.

---

## 8. Keycloak yapılandırmasını sonradan değiştirmek

Keycloak'ın yapılandırması (`lakehouse` realm'i) yalnız **ilk kurulumda**, realm yokken
yüklenir. Ürün dosyası sonradan değişse bile çalışan realm değişmez. Bu yüzden
`keycloak-clients` Secret'ı ve AD ayarları kurulumdan önce doğru olmalıdır.

Realm'i yine de yeniden yüklemek gerekirse aşağıdaki komutları çalıştırın. **Keycloak
arayüzünde elle yapılmış bütün değişiklikler silinir;** AD kullanıcıları yeniden okunur.

`[bastion]`

```bash
KC_ADMIN=$(oc -n "$LAKEHOUSE_NS" get secret keycloak-admin \
  -o jsonpath='{.data.username}' | base64 -d)
KC_PASS=$(oc -n "$LAKEHOUSE_NS" get secret keycloak-admin \
  -o jsonpath='{.data.password}' | base64 -d)
oc -n "$LAKEHOUSE_NS" delete keycloakrealmimport lakehouse-realm
oc -n "$LAKEHOUSE_NS" exec keycloak-0 -- /opt/keycloak/bin/kcadm.sh config credentials \
  --config /tmp/kcadm.config --server http://localhost:8080 --realm master \
  --user "$KC_ADMIN" --password "$KC_PASS"
oc -n "$LAKEHOUSE_NS" exec keycloak-0 -- /opt/keycloak/bin/kcadm.sh delete realms/lakehouse \
  --config /tmp/kcadm.config && echo "realm silindi"
oc -n "$ARGOCD_NS" patch application glue --type merge -p '{"operation":{"sync":{}}}'
oc -n "$LAKEHOUSE_NS" wait keycloakrealmimport/lakehouse-realm \
  --for=condition=Done --timeout=600s
```

**Beklenen çıktı** (örnek):

```text
keycloakrealmimport.k8s.keycloak.org "lakehouse-realm" deleted from lakehouse namespace
Logging into http://localhost:8080 as user admin of realm master
realm silindi
application.argoproj.io/glue patched
keycloakrealmimport.k8s.keycloak.org/lakehouse-realm condition met
```

**`realm silindi` satırını mutlaka görün.** Bu satır yoksa realm silinmemiştir ve son
komutun `condition met` demesi yanıltıcıdır: yükleme hiçbir şey yapmadan biter, eski realm
yerinde kalır.

**Ters giderse:** `Failed to create config file` hatası `--config /tmp/kcadm.config`
seçeneğinin eksik olduğunu gösterir. `kcadm.sh` oturumu Keycloak pod'u yeniden başladığında
kapanır; `config credentials` komutunu tekrarlayın. Kullanıcı ve yetki yönetiminin
tamamı [50-isletme/kullanici-ve-yetki.md](50-isletme/kullanici-ve-yetki.md) sayfasındadır.

---

## Kontrol listesi

- [ ] `install/lakehouse.env` yüklendi ve `EKSIK:` satırı yazılmadı.
- [ ] Depo kurumun Git sunucusuna kopyalandı; yerel kopyanın `origin` adresi oraya bakıyor.
- [ ] `$ARGOCD_NS` ad alanında `lakehouse-repo` Secret'ı var ve
      `argocd.argoproj.io/secret-type=repository` etiketini taşıyor.
- [ ] `platform/values/site/` altındaki üç dosyada örnek değer kalmadı;
      `scripts/check-site.sh` `check-site: OK` yazıyor.
- [ ] `platform/polaris/setup.yaml` kurumun S3 adresini ve veri bucket'ını gösteriyor.
- [ ] Ayarlar ve `tls.caBundle` Git'e gönderildi.
- [ ] Adım 5.15'teki 13 zorunlu Secret'ın hepsi anahtarlarıyla listeleniyor; AD kurumsal
      CA'lıysa `ad-ca`, S3 kurumsal CA'lıysa `s3-ca` da listede.
- [ ] `bootstrap/bootstrap.sh` "ArgoCD zaten kurulu" satırını yazdı ve kök uygulama yaratıldı.
- [ ] `trino` dışındaki bütün uygulamalar `Synced Healthy`.
- [ ] Adım 7.1'deki altı komut hatasız bitti ve üç veritabanı yedeği
      `ContinuousArchivingSuccess` gösteriyor.
- [ ] Dış adresler `200` ya da `302` dönüyor.
- [ ] Keycloak yönetici parolası ve `~/lakehouse-ca.crt` kurumun kasasına alındı.

## Sonraki bölüm

[40-kurulum-sonrasi.md](40-kurulum-sonrasi.md) — Polaris kataloğunun kurulması, Superset'e
Trino bağlantısının eklenmesi, AD kullanıcısıyla ilk giriş, izleme ve kabul testi.

Başvuru tabloları:
[90-referans/secret-listesi.md](90-referans/secret-listesi.md),
[90-referans/values-anahtarlari.md](90-referans/values-anahtarlari.md),
[90-referans/port-ve-servisler.md](90-referans/port-ve-servisler.md),
[90-referans/surumler-ve-lisanslar.md](90-referans/surumler-ve-lisanslar.md),
[90-referans/kabul-testleri.md](90-referans/kabul-testleri.md) ve
[90-referans/oc-hizli-basvuru.md](90-referans/oc-hizli-basvuru.md).
