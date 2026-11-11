# 20 — Ön koşullar

Bu bölüm, kuruluma başlamadan önce hazır olması gereken on maddeyi anlatır: küme,
depolama, Active Directory, ağ ve izleme. Her madde için neden gerektiği, kimin işi
olduğu, nasıl doğrulanacağı ve doğrulama hata verirse ne yapılacağı yazılıdır.

**Ne kadar sürer:** değerler [10-planlama](10-planlama.md) §5'teki çalışma sayfasına
toplanmışsa 1–2 saat. GitOps operatörünün kurulması tek başına 5–10 dakikadır. AD ve S3
maddeleri başka ekiplerin işi olduğu için takvimde günler sürebilir.

**Hangi yetkiler gerekir:** kümede **cluster-admin** (operatör kurmak, küme düzeyinde rol
vermek, izleme ayarı yapmak için). S3 ve AD maddelerini ilgili ekipler yapar; siz bu
bölümde yalnız doğrularsınız.

**Komutlar nerede çalışır:** `[bastion]` — kümeye `oc login` ile bağlı yönetim makinesi.
Tek istisna madde 8'dir: oradaki iki komut nginx'in koştuğu web sunucusunda
(`[nginx ajan sunucusu]`) çalışır.

Yönetim makinesinde kurulu olması gereken araçlar (bütün belgeler için):

| Araç | Nerede gerekir |
|---|---|
| `oc` | her bölüm |
| `kubectl` | `scripts/polaris-setup.sh` ve `scripts/acceptance.sh` bu aracı adıyla çağırır ([40-kurulum-sonrasi](40-kurulum-sonrasi.md) §2 ve §7); yalnız `oc` kuruluysa `kubectl: command not found` hatası alınır |
| `aws` | madde 4 (S3 doğrulaması) |
| `ldapsearch` | madde 6 (Active Directory doğrulaması) |
| `getent` | madde 7 (DNS doğrulaması) |
| `curl` | madde 4, madde 10 ve [30-kurulum](30-kurulum.md) §7.3 |
| `openssl` | [30-kurulum](30-kurulum.md) §5 (rastgele parola üretimi, sertifika incelemesi) |
| `python3` | [30-kurulum](30-kurulum.md) §5, `scripts/polaris-setup.sh` |
| `git` | [30-kurulum](30-kurulum.md) §2–§4 |
| `htpasswd` | [30-kurulum](30-kurulum.md) §5.7 (RHEL/Fedora: `httpd-tools`, Debian/Ubuntu: `apache2-utils`) |
| `jq` | kabul testi ([90-referans/kabul-testleri.md](90-referans/kabul-testleri.md) §1) ve işletme komutları |
| `keytool` | yalnız S3 sertifikası kurumun kendi CA'sıyla imzalıysa: [30-kurulum](30-kurulum.md) §5.14 (RHEL/Fedora: `sudo dnf install -y java-21-openjdk-headless`) |

> **Yönetim makineniz macOS ise** üç fark vardır:
>
> - `getent` yoktur; yerine `dscacheutil -q host -a name ADRES` kullanın.
> - `shred` yoktur; yerine `rm -P DOSYA` kullanın.
> - macOS ile gelen `ldapsearch` özel CA ayarını (`LDAPTLS_CACERT`) yok sayar ve
>   `Can't contact LDAP server` der. `brew install openldap` ile gelen istemciyi kullanın:
>   `export PATH="$(brew --prefix openldap)/bin:$PATH"`.
>
> `oc` için: `brew install openshift-cli`.

---

## Başlamadan önce: değişkenleri yükleyin

Bu bölümdeki komutlar `install/lakehouse.env` dosyasındaki değişkenleri kullanır (dosyayı
[10-planlama](10-planlama.md) §5'te doldurdunuz). Her yeni terminalde bir kez çalıştırın:

`[bastion]`

```bash
cd ~/lakehouse
set -a; . install/lakehouse.env; set +a
echo "$ARGOCD_NS / $LAKEHOUSE_NS / $APPS_DOMAIN"
```

**Beklenen çıktı** (değerler sizin dosyanızdan gelir):

```text
openshift-gitops / lakehouse / apps.ocp.example.net
```

**Ters giderse:**

- `cd` komutu `No such file or directory` derse depo başka bir klasördedir. Bulmak için:
  `find "$HOME" -maxdepth 3 -name lakehouse.env.example`.
- Çıktı boşsa dosya yüklenmemiştir: `ls -l install/lakehouse.env` ile dosyanın
  varlığını kontrol edin.
- Yalnız üçüncü değer boşsa `APPS_DOMAIN` doldurulmamıştır; [10-planlama](10-planlama.md)
  §3 Adım 1'e dönün.

---

## Ön koşul olmayanlar

> **Şu operatörleri OperatorHub'dan kurmayın:** Strimzi (Kafka), CloudNativePG ve Barman
> Cloud eklentisi, cert-manager, spark-operator, Keycloak operatörü, Superset operatörü.
> Ürün bunları ArgoCD ile kendisi kurar. Aynı operatörü bir de OperatorHub'dan kurmak,
> aynı nesne türlerinin iki sahibi olması demektir: sürümler çakışır, ArgoCD sürekli
> `OutOfSync` kalır ve yükseltme yapılamaz.
>
> Şunlar da ön koşul **değildir**: ayrı bir Kafka, PostgreSQL ya da Trino kurulumu; ayrı
> bir Prometheus/Grafana (OpenShift'in kendi izlemesi kullanılır); özel konteyner imajı
> üretmek (Kafka Connect imajı kümede üretilir); MinIO (üretimde kurumun S3'ü kullanılır).
>
> **Ön koşul olan tek operatör OpenShift GitOps'tur** (madde 2). OADP isteğe bağlıdır
> (madde 9).

---

## 1. OpenShift kümesi, `oc` ve cluster-admin

**Neden:** kurulum sırasında yeni nesne türleri tanımlanır, ad alanları açılır ve küme
düzeyinde roller verilir. Bunlar proje yöneticisi yetkisiyle yapılamaz.

**Kimin işi:** platform (OpenShift) ekibi hesabı verir; kurulumcu doğrular.

`[bastion]`

```bash
oc whoami
oc version
oc auth can-i '*' '*' --all-namespaces
```

**Beklenen çıktı** (sürümler kümenize göre değişir):

```text
kube:admin
Client Version: 4.22.18
Kustomize Version: v5.7.1
Server Version: 4.22.20
Kubernetes Version: v1.35.9
yes
```

Kubernetes sürümü **1.33 ya da üstü** olmalıdır; OpenShift'te bu 4.20 ve üstü sürümlerdir.
Son satır `yes` değilse hesabınız cluster-admin değildir ve kuruluma başlanmamalıdır:
madde 2 ve madde 9 cluster-admin olmadan yapılamaz.

**Ters giderse:**

- `oc: command not found` → `oc` kurulu değil. OpenShift web konsolunda sağ üstteki
  **?** menüsünden **Command line tools** sayfasına gidip indirin.
- `You must be logged in` → `oc login` ile girin.

### 1.1 `oc login` sertifika hatası verirse

`oc login` **`x509: certificate signed by unknown authority`** derse kümenin API
sertifikası, kümenin kendi ürettiği bir CA ile imzalıdır. Bu, yeni kurulmuş kümelerde
olağandır. Güvenlik denetimini kapatmayın (`--insecure-skip-tls-verify` kullanmayın);
CA'yı bir kez indirip `oc`'ye tanıtın.

İlk üç komut CA'yı indirir ve parmak izini gösterir. **Parmak izini platform ekibiyle
karşılaştırın**; aynıysa son iki komutla girin. Giriş jetonunu web konsolunda sağ üstte
kullanıcı adınız → **Copy login command** sayfasından alın (`sha256~` ile başlar).
`read` komutu jetonu ekrana yazmadan ister; yapıştırıp Enter'a basın.

`[bastion]`

```bash
API=$(echo "$APPS_DOMAIN" | sed 's/^apps\./api./'):6443
echo | openssl s_client -connect "$API" -showcerts 2>/dev/null \
  | awk '/BEGIN CERT/{n++} n==2' | sed '/END CERT/q' > ~/kume-api-ca.crt
openssl x509 -in ~/kume-api-ca.crt -noout -subject -fingerprint -sha256
read -rs -p "Jeton: " OC_TOKEN; echo
oc login --token="$OC_TOKEN" --server="https://$API" \
  --certificate-authority="$HOME/kume-api-ca.crt"
```

**Beklenen çıktı** (örnek):

```text
subject=OU=openshift, CN=kube-apiserver-lb-signer
sha256 Fingerprint=28:73:2B:3C:...
Logged into "https://api.ocp.example.net:6443" as "kube:admin" using the token provided.
```

---

## 2. OpenShift GitOps operatörü (ArgoCD)

**Neden:** ürün GitOps ile kurulur: kümeye elle bir şey uygulanmaz, ArgoCD Git'teki hâli
kümeye uygular. OpenShift'te ArgoCD'yi **OpenShift GitOps operatörü** sağlar. Kurulum
betiği (`bootstrap/bootstrap.sh`) operatörün kurduğu ArgoCD'yi bulursa onu kullanır.
Operatör kurulu değilse betik kendi ArgoCD'sini kurmaya kalkar ve sonradan kurulacak
operatörle çakışır.

**Kimin işi:** platform ekibi (cluster-admin).

### 2.1 Operatörü kurun

`[bastion]`

```bash
oc apply -f - <<'YAML'
apiVersion: operators.coreos.com/v1alpha1
kind: Subscription
metadata:
  name: openshift-gitops-operator
  namespace: openshift-operators
spec:
  channel: latest
  installPlanApproval: Automatic
  name: openshift-gitops-operator
  source: redhat-operators
  sourceNamespace: openshift-marketplace
YAML
```

**Beklenen çıktı:**

```text
subscription.operators.coreos.com/openshift-gitops-operator created
```

Operatör 1–3 dakika içinde kurulur ve `openshift-gitops` ad alanında bir ArgoCD
örneğini kendisi yaratır. Kontrol edin:

`[bastion]`

```bash
oc get csv -n "$ARGOCD_NS" | grep -i gitops
oc -n "$ARGOCD_NS" get deploy openshift-gitops-server
```

`csv` (ClusterServiceVersion), OpenShift'in kurduğu her operatör sürümü için tuttuğu
kayıttır; kurulumun bitip bitmediği `PHASE` sütunundan okunur.

**Beklenen çıktı** (operatör sürümü kümenize göre değişir):

```text
openshift-gitops-operator.v1.22.3   Red Hat OpenShift GitOps   1.22.3   openshift-gitops-operator.v1.22.2   Succeeded
NAME                      READY   UP-TO-DATE   AVAILABLE   AGE
openshift-gitops-server   1/1     1            1           3d5h
```

Satırın sonundaki `PHASE` **Succeeded** olmalıdır. İlk dakikada `No resources found`
görürseniz bir dakika sonra tekrar bakın.

**Ters giderse:**

- `no matches for kind "Subscription"` → küme OpenShift değildir; bu belge yalnız
  OpenShift'i kapsar.
- `PHASE` uzun süre `Pending` kalıyorsa operatör kataloğuna erişilemiyordur.
  `oc -n openshift-marketplace get catalogsource redhat-operators` ve
  `oc -n openshift-operators get installplan` çıktılarına bakın. İnternete kapalı
  kümelerde katalog aynası gerekir.

### 2.2 ArgoCD'ye küme yetkisi verin

**Neden:** ArgoCD bu kurulumda operatörleri de kurar (Strimzi, CloudNativePG,
cert-manager, spark-operator, Keycloak, Superset). Operatör kurmak, küme düzeyinde yeni
nesne türleri ve roller tanımlamak demektir; ArgoCD bunu ancak cluster-admin yetkisiyle
yapabilir. Yetki yoksa ilk uygulama `cannot create resource
"customresourcedefinitions"` hatasıyla durur.

Önce yetkinin olup olmadığına bakın:

`[bastion]`

```bash
oc auth can-i create customresourcedefinitions \
  --as="system:serviceaccount:$ARGOCD_NS:openshift-gitops-argocd-application-controller"
```

**Beklenen çıktı:** `yes` ya da `no`. Üstte `Warning: resource
'customresourcedefinitions' is not namespace scoped` satırı da görünebilir; zararsızdır,
yalnız son satıra bakın. OpenShift GitOps'un güncel sürümlerinde bu yetki kendiliğinden
verilmez ve ilk kurulumda çıktı şudur:

```text
no
```

Çıktı `no` ise yetkiyi verin:

`[bastion]`

```bash
oc adm policy add-cluster-role-to-user cluster-admin \
  -z openshift-gitops-argocd-application-controller -n "$ARGOCD_NS"
```

**Beklenen çıktı:**

```text
clusterrole.rbac.authorization.k8s.io/cluster-admin added: "openshift-gitops-argocd-application-controller"
```

Sonra ilk komutu tekrar çalıştırın; çıktı `yes` olmalıdır.

**Ters giderse:** `serviceaccount ... not found` derse ArgoCD örneği henüz yaratılmamıştır
(§2.1'deki `openshift-gitops-server` hazır olana kadar bekleyin) ya da servis hesabının
adı farklıdır. Gerçek adı `oc -n "$ARGOCD_NS" get sa` ile bulun ve `-z` değerine onu
yazın. Kurumunuz ArgoCD'ye cluster-admin vermiyorsa kurulum bu hâliyle yapılamaz.

---

## 3. Depolama sınıfı (StorageClass)

**Neden:** Kafka, üç PostgreSQL veritabanı, Zeppelin, Maven önbelleği ve her kullanıcının
not defteri kalıcı disk ister. Diskleri bir
[StorageClass](00-genel-bakis.md#4-kavramlar-sözlüğü) sağlar; kümede kullanılabilir bir
sınıf yoksa pod'lar `Pending` durumunda bekler.

**Kimin işi:** platform ekibi sağlar; kurulumcu doğrular ve `$STORAGE_CLASS` değerini
doldurur.

`[bastion]`

```bash
oc get storageclass
```

**Beklenen çıktı** (sınıf adları kümenize göre değişir):

```text
NAME                            PROVISIONER        RECLAIMPOLICY   VOLUMEBINDINGMODE   ALLOWVOLUMEEXPANSION   AGE
px-fa-direct-access (default)   pxd.portworx.com   Delete          Immediate           true                   4d
px-fb-direct-access-nfsv3       pxd.portworx.com   Delete          Immediate           true                   4d
px-fb-direct-access-nfsv4       pxd.portworx.com   Delete          Immediate           true                   4d
```

Kullanılacak sınıf **ReadWriteOnce** blok depolama vermeli ve `ALLOWVOLUMEEXPANSION`
sütunu **true** olmalıdır; Kafka diskini sonradan büyütmek sık yapılan bir işlemdir.
`(default)` işaretli bir sınıf varsa `$STORAGE_CLASS` boş bırakılabilir.

**Ters giderse:** liste boşsa kümede depolama yoktur; platform ekibine başvurun.
Varsayılan sınıf yoksa `$STORAGE_CLASS` doldurulmak **zorundadır**; boş kalırsa diskler
bağlanmaz.

### 3.1 Depolama sürücüsü her düğümde var mı

**Neden:** bir sınıfın diski, yalnız o depolamanın sürücüsünün kurulu olduğu düğümlerde
bağlanabilir. Bazı kümelerde bir düğüm depolamaya bağlı değildir; disk kullanan bir pod
oraya düşerse `ContainerCreating` durumunda kalır.

`[bastion]`

```bash
oc get storageclass -o jsonpath='{range .items[*]}{.metadata.name}{" -> "}{.provisioner}{"\n"}{end}'
oc get csinode -o jsonpath='{range .items[*]}{.metadata.name}{": "}{.spec.drivers[*].name}{"\n"}{end}'
```

**Beklenen çıktı** (örnek): ilk komut her sınıfın sürücüsünü, ikinci komut her düğümdeki
sürücüleri listeler. Kullanacağınız sınıfın sürücüsü (örnekte `pxd.portworx.com`) **her
düğüm satırında** görünmelidir:

```text
px-fa-direct-access -> pxd.portworx.com
worker-1.ocp.example.net: pxd.portworx.com
worker-2.ocp.example.net: pxd.portworx.com
```

**Ters giderse:** sürücüsü olmayan bir düğüm varsa ve platform ekibi oraya sürücü
kuramıyorsa, lakehouse pod'larını yalnız sürücülü düğümlere yerleştirin. Aşağıdaki
komutlar sürücülü düğümleri etiketler ve yalnız `$LAKEHOUSE_NS` ad alanına "yalnız bu
etiketli düğümler" kuralını koyar; kümedeki başka bir işi etkilemez. Düğüm adlarını kendi
çıktınızdan yazın:

`[bastion]`

```bash
oc label node worker-1.ocp.example.net worker-2.ocp.example.net lakehouse.io/storage=csi
oc create namespace "$LAKEHOUSE_NS" --dry-run=client -o yaml | oc apply -f -
oc annotate namespace "$LAKEHOUSE_NS" openshift.io/node-selector=lakehouse.io/storage=csi --overwrite
```

**Beklenen çıktı:**

```text
node/worker-1.ocp.example.net labeled
node/worker-2.ocp.example.net labeled
namespace/lakehouse created
namespace/lakehouse annotated
```

Bunu ürünü kurmadan **önce** yapın: kural yalnız sonradan yaratılan pod'lara uygulanır.

---

## 4. S3: iki bucket, iki anahtar çifti

**Neden:** tablo verisi (Bronze, Silver, Gold) `$S3_BUCKET_DATA` bucket'ında durur.
PostgreSQL yedekleri **ayrı** bir bucket'a (`$S3_BUCKET_BACKUP`) ve **ayrı** bir anahtar
çiftiyle yazılır. Böylece veri anahtarı ele geçse bile yedekler silinemez.

**Kimin işi:** depolama ekibi bucket'ları ve anahtarları yaratır; kurulumcu doğrular.

Depolama ekibinden bucket ve anahtarların yanında iki bilgi daha isteyin:

1. **S3 sertifikasının kök CA'sı** (`$S3_CA_FILE`). S3 sunucusunun sertifikası kurumun
   kendi CA'sıyla ya da kendinden imzalıysa ürünün bu CA'ya güvenmesi gerekir.
   Sertifika herkesçe bilinen bir CA'dan geliyorsa bu bilgi gerekmez.
2. **S3'ün veri IP adresleri** (`$S3_EGRESS_CIDRS`, ör. `10.20.30.40/32`). Not defterleri
   S3'e doğrudan bağlanır; ağ kuralları bu adresleri açıkça izin listesine yazar.

CA gerekip gerekmediğini kendiniz de görebilirsiniz:

`[bastion]`

```bash
curl -sS -o /dev/null -w '%{http_code}\n' "$S3_ENDPOINT"
getent hosts "$(echo "$S3_ENDPOINT" | sed -E 's#^https?://##; s#[:/].*##')"
```

**Beklenen çıktı** (örnek): ilk satır bir HTTP kodudur (ör. `403`); bu durumda CA
gerekmez. İlk satırda `SSL certificate problem` yazıyorsa CA gerekir. İkinci satır S3
adının IP adresidir.

```text
403
10.20.30.40     s3.kurum.example.net
```

CA dosyası gerekiyorsa bu bölümdeki `aws` komutlarından önce tanıtın (gerekmiyorsa bu
adımı atlayın):

`[bastion]`

```bash
export AWS_CA_BUNDLE="$S3_CA_FILE"
```

### 4.1 Okuma doğrulaması

`[bastion]`

```bash
AWS_ACCESS_KEY_ID=$S3_ACCESS_KEY AWS_SECRET_ACCESS_KEY=$S3_SECRET_KEY \
  aws --endpoint-url "$S3_ENDPOINT" --region "$S3_REGION" s3 ls "s3://$S3_BUCKET_DATA"
AWS_ACCESS_KEY_ID=$S3_BACKUP_ACCESS_KEY AWS_SECRET_ACCESS_KEY=$S3_BACKUP_SECRET_KEY \
  aws --endpoint-url "$S3_ENDPOINT" --region "$S3_REGION" s3 ls "s3://$S3_BUCKET_BACKUP"
```

**Beklenen çıktı:** iki komut da **hata vermeden** biter. Yeni bucket boşsa hiç satır
yazılmaz; önemli olan hata satırı olmamasıdır. Dolu bir bucket'ta örnek:

```text
                           PRE warehouse/
```

### 4.2 Yazma doğrulaması

`[bastion]`

```bash
echo ok > /tmp/lakehouse-on-kosul.txt
export AWS_ACCESS_KEY_ID=$S3_ACCESS_KEY AWS_SECRET_ACCESS_KEY=$S3_SECRET_KEY
aws --endpoint-url "$S3_ENDPOINT" s3 cp /tmp/lakehouse-on-kosul.txt \
  "s3://$S3_BUCKET_DATA/_on-kosul-testi.txt"
aws --endpoint-url "$S3_ENDPOINT" s3 rm "s3://$S3_BUCKET_DATA/_on-kosul-testi.txt"
export AWS_ACCESS_KEY_ID=$S3_BACKUP_ACCESS_KEY AWS_SECRET_ACCESS_KEY=$S3_BACKUP_SECRET_KEY
aws --endpoint-url "$S3_ENDPOINT" s3 cp /tmp/lakehouse-on-kosul.txt \
  "s3://$S3_BUCKET_BACKUP/_on-kosul-testi.txt"
aws --endpoint-url "$S3_ENDPOINT" s3 rm "s3://$S3_BUCKET_BACKUP/_on-kosul-testi.txt"
unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY
```

**Beklenen çıktı** (bucket adları sizinkilerdir; dört satır: veri bucket'ına yaz ve sil,
yedek bucket'ına yaz ve sil):

```text
upload: /tmp/lakehouse-on-kosul.txt to s3://lakehouse/_on-kosul-testi.txt
delete: s3://lakehouse/_on-kosul-testi.txt
upload: /tmp/lakehouse-on-kosul.txt to s3://lakehouse-backups/_on-kosul-testi.txt
delete: s3://lakehouse-backups/_on-kosul-testi.txt
```

**İki anahtar çifti de yazabilmelidir.** Yedek anahtarı yalnız okuyabiliyorsa
PostgreSQL yedekleri kurulumdan sonra sessizce başarısız olur.

**Geçici kimlik (STS) desteğini sorun.** Ürün, uygulamalara kalıcı S3 anahtarı yerine
kısa ömürlü kimlik dağıtabilir; bunun için S3'ün STS desteklemesi gerekir. Depolama
ekibine STS'in açık olup olmadığını sorun ve cevabı not edin. Cevap kurulumda
`s3.vendedCredentials` ayarına yazılır ([30-kurulum](30-kurulum.md) §4.3); şimdi bir şey
yazmanız gerekmez.

**Ters giderse:**

- `Could not connect to the endpoint URL` → `$S3_ENDPOINT` yanlış ya da güvenlik duvarı
  443 portunu kapatıyor.
- `SSL validation failed` → S3'ün sertifikası özel bir CA ile imzalı. Depolama ekibinden
  kök CA'yı isteyin, `$S3_CA_FILE` yoluna kaydedin ve yukarıdaki
  `export AWS_CA_BUNDLE=...` satırını çalıştırın. Kümedeki bileşenlerin aynı CA'ya
  güvenmesi kurulumda sağlanır ([30-kurulum](30-kurulum.md) §5.14).
- `AccessDenied` → anahtar çifti o bucket'a yetkili değil. Yedek anahtarını veri
  bucket'ında denemediğinizden emin olun.
- `NoSuchBucket` → bucket henüz açılmamış.

---

## 5. İç registry ve Connect imajını gönderecek hesap

**Neden:** Kafka Connect imajı (kaynak bağlayıcıları ve Iceberg yazıcısıyla birlikte)
**kümede** üretilir; elle imaj üretmeniz gerekmez. Strimzi imajı üretir ve
`$INTERNAL_REGISTRY/$LAKEHOUSE_NS/connect:<sürüm>` adresine gönderir. Bunun için
`connect-push` adlı bir Secret gerekir. Secret yoksa imaj üretimi `unauthorized` hatası
verir ve Kafka Connect hiç çalışmaz.

**Kimin işi:** kurulumcu (ad alanında yönetici yetkisiyle).

`[bastion]`

```bash
oc create namespace "$LAKEHOUSE_NS" --dry-run=client -o yaml | oc apply -f -
oc registry info --internal
oc -n "$LAKEHOUSE_NS" create sa connect-build
oc -n "$LAKEHOUSE_NS" policy add-role-to-user system:image-builder -z connect-build
TOKEN=$(oc -n "$LAKEHOUSE_NS" create token connect-build --duration=8760h)
oc -n "$LAKEHOUSE_NS" create secret docker-registry connect-push \
  --docker-server="$INTERNAL_REGISTRY" --docker-username=connect-build \
  --docker-password="$TOKEN"
oc -n "$LAKEHOUSE_NS" label secret connect-push lakehouse.io/yonetim=manual-20-on-kosullar-5
python3 -c 'import base64, json, sys, datetime
t = sys.argv[1].split(".")[1]; t += "=" * (-len(t) % 4)
print("jeton bitisi:", datetime.datetime.fromtimestamp(
    json.loads(base64.urlsafe_b64decode(t))["exp"], datetime.timezone.utc))' "$TOKEN"
unset TOKEN
```

**Beklenen çıktı** (son satırdaki tarih jetonun gerçek bitiş anıdır):

```text
namespace/lakehouse created
image-registry.openshift-image-registry.svc:5000
serviceaccount/connect-build created
clusterrole.rbac.authorization.k8s.io/system:image-builder added: "connect-build"
secret/connect-push created
secret/connect-push labeled
jeton bitisi: 2027-09-19 08:41:12+00:00
```

İkinci satırdaki adres `$INTERNAL_REGISTRY` değeriyle **birebir aynı** olmalıdır.
`--internal` seçeneği şarttır: o olmadan komut registry'nin dışarıya açık adresini verir,
Strimzi ise küme içindeki adresi kullanır. Komut `Command "info" is deprecated`
uyarısını da yazabilir; zararsızdır.

`label` satırı Secret'ı "bu belgenin 5. maddesinde elle yaratıldı" diye işaretler; bu
işaret Secret'ın nereden geldiğini sonradan bulmayı kolaylaştırır
([degisiklik-nasil-uygulanir.md](50-isletme/degisiklik-nasil-uygulanir.md) §7).

**Jetonun ömrü.** Komut bir yıllık bir jeton ister. Küme daha kısa bir üst sınır
uyguluyorsa jetonu kısaltır; gerçek süre `jeton bitisi:` satırındaki tarihtir. Jeton
dolduğunda Connect imajının **yeni** üretimi `unauthorized` verir; çalışan Connect
etkilenmez, ama sürüm yükseltmesi yapılamaz. Bitiş tarihini kurumun takvimine yazın.
Yenileme adımı: [50-isletme/yukseltme.md](50-isletme/yukseltme.md) §6.

**Ters giderse:**

- Jeton komutu süreyle ilgili hata verirse küme daha kısa bir üst sınır uyguluyordur;
  komutu `--duration=24h` ile tekrarlayın ve yenileme sıklığını ona göre planlayın.
- `Error from server (AlreadyExists)` → nesne zaten var. Secret içinse önce
  `oc -n "$LAKEHOUSE_NS" delete secret connect-push` ile silin, sonra komutları
  tekrarlayın.
- `oc registry info` **`the integrated registry has not been configured`** derse iç
  registry kapalıdır (§5.1).

### 5.1 İç registry kapalıysa

Bare-metal ve "assisted installer" kurulumlarında iç registry varsayılan olarak kapalıdır.
Durumuna bakın:

`[bastion]`

```bash
oc get configs.imageregistry.operator.openshift.io cluster -o jsonpath='{.spec.managementState}{"\n"}'
```

Çıktı `Removed` ise platform ekibi registry'yi açar. Registry'nin diski için 100 GiB
yeterlidir. Disk ReadWriteOnce türündeyse (madde 3'teki blok depolama) registry tek kopya
çalışmalıdır; aşağıdaki komutlar bunu ayarlar:

`[bastion]`

```bash
oc apply -f - <<YAML
apiVersion: v1
kind: PersistentVolumeClaim
metadata: {name: image-registry-storage, namespace: openshift-image-registry}
spec:
  accessModes: [ReadWriteOnce]
  resources: {requests: {storage: 100Gi}}
  storageClassName: ${STORAGE_CLASS:-$(oc get sc -o jsonpath='{.items[?(@.metadata.annotations.storageclass\.kubernetes\.io/is-default-class=="true")].metadata.name}')}
YAML
oc patch configs.imageregistry.operator.openshift.io cluster --type merge -p \
  '{"spec":{"managementState":"Managed","replicas":1,"rolloutStrategy":"Recreate","storage":{"pvc":{"claim":"image-registry-storage"}}}}'
oc -n openshift-image-registry rollout status deploy/image-registry --timeout=300s
oc registry info --internal
```

**Beklenen çıktı** (son satır):

```text
image-registry.openshift-image-registry.svc:5000
```

Registry pod'u `ContainerCreating` durumunda kalıyor ve olaylarında `does not contain
driver` yazıyorsa pod, depolama sürücüsü olmayan bir düğüme düşmüştür. Madde 3.1'deki
etiketi koyduktan sonra registry'yi o düğümlere yönlendirin:

`[bastion]`

```bash
oc patch configs.imageregistry.operator.openshift.io cluster --type merge \
  -p '{"spec":{"nodeSelector":{"lakehouse.io/storage":"csi"}}}'
```

Kurumsal bir registry de kullanılabilir. O durumda `$INTERNAL_REGISTRY` yerine kurumsal
registry'nin adresi yazılır ve `connect-push` Secret'ı oraya gönderme yetkisi olan bir
kullanıcı ve parolayla yaratılır.

### 5.2 Connect imajı üretildikten sonra

Kurulumdan sonra, Connect imajı ilk kez üretildiğinde imajın kaydını kontrol edin:

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" get imagestream connect
```

**Beklenen çıktı:**

```text
NAME      IMAGE REPOSITORY                                                     TAGS    UPDATED
connect   image-registry.openshift-image-registry.svc:5000/lakehouse/connect   2.0.0   3 days ago
```

Kayıt ilk gönderimde kendiliğinden yaratılır. `NotFound` görürseniz elle yaratın:
`oc -n "$LAKEHOUSE_NS" create imagestream connect`.

---

## 6. Active Directory: üç grup, üyeler, servis hesabı, LDAPS CA

**Neden:** kullanıcılar ve gruplar Active Directory'den (AD) gelir. Keycloak kullanıcıları
ve grupları AD'den okur, Zeppelin ise AD'ye doğrudan bağlanır. Yetki kuralları **grup
adlarına** göre yazılmıştır; bu yüzden üç grup birebir bu adlarla açılmalıdır:

| AD grubu | Kim | Üründeki karşılığı |
|---|---|---|
| `lakehouse-admins` | platform ve veri yöneticileri | her şeye yetki; Superset ve Trino yönetimi |
| `lakehouse-analysts` | analistler | bütün tabloları okuma, pano yapma, `sandbox` şemasına yazma |
| `lakehouse-users` | son kullanıcılar | kendilerine açılan tabloları okuma |

**Kimin işi:** AD ekibi grupları açar, üyeleri ekler, yalnız okuma yetkili bir servis
hesabı ve kök CA dosyasını verir; kurulumcu doğrular. Parolaların kümeye kaydedilmesi bu
bölümde değil, [30-kurulum](30-kurulum.md) §5'tedir.

Önce `ldapsearch` aracına AD'nin kök CA'sını tanıtın ve grupları arayın. Kurumsal
AD'lerde sertifika neredeyse her zaman kurumun kendi CA'sıyla imzalıdır; CA
tanıtılmazsa `ldapsearch` `Can't contact LDAP server (-1)` der.

`[bastion]`

```bash
export LDAPTLS_CACERT="$LDAP_CA_FILE"
ldapsearch -H "$LDAP_URL" -D "$LDAP_BIND_DN" -w "$LDAP_BIND_PASSWORD" \
  -b "$LDAP_GROUPS_DN" "(cn=lakehouse-*)" cn
```

**Beklenen çıktı** (kısaltılmış — üç `cn:` satırı görünmelidir):

```text
# lakehouse-admins, Groups, example.com
cn: lakehouse-admins
# lakehouse-analysts, Groups, example.com
cn: lakehouse-analysts
# lakehouse-users, Groups, example.com
cn: lakehouse-users
result: 0 Success
```

Üyeliği de kontrol edin. `lakehouse-admins` grubunda en az bir kişi olmalıdır; yoksa
kurulumdan sonra hiçbir arayüze yönetici olarak girilemez.

`[bastion]`

```bash
ldapsearch -H "$LDAP_URL" -D "$LDAP_BIND_DN" -w "$LDAP_BIND_PASSWORD" \
  -b "$LDAP_USERS_DN" "(memberOf=CN=lakehouse-admins,$LDAP_GROUPS_DN)" sAMAccountName
```

**Beklenen çıktı:** en az bir `sAMAccountName:` satırı ve `result: 0 Success`.

Servis hesabının **kısa adını** (`$LDAP_BIND_USER`) da kontrol edin. Zeppelin AD'ye bu
adın sonuna `$AD_UPN_SUFFIX` ekleyerek bağlanır; değer yanlışsa Zeppelin'e kimse giremez.

`[bastion]`

```bash
ldapsearch -LLL -H "$LDAP_URL" -D "$LDAP_BIND_DN" -w "$LDAP_BIND_PASSWORD" \
  -b "$LDAP_BIND_DN" -s base sAMAccountName userPrincipalName
echo "beklenen: $LDAP_BIND_USER$AD_UPN_SUFFIX"
```

**Beklenen çıktı** (örnek — `userPrincipalName` ile son satır **aynı** olmalıdır):

```text
dn: CN=svc lakehouse,OU=Service,DC=example,DC=com
sAMAccountName: svc-lakehouse
userPrincipalName: svc-lakehouse@example.com
beklenen: svc-lakehouse@example.com
```

### 6.1 Kök CA dosyası

AD sertifikasını imzalayan **kök CA'nın PEM dosyasını** AD ekibi verir; `$LDAP_CA_FILE`
bu dosyayı göstermelidir. Sunucudan indirilen sertifika zinciri bunun yerine geçmez: o
zincirin ilk sertifikası sunucunun kendi sertifikasıdır ve kök CA çoğu zaman zincirde
yoktur. Elinizdeki dosyanın gerçekten kök CA olduğunu kontrol edin:

`[bastion]`

```bash
openssl x509 -in "$LDAP_CA_FILE" -noout -subject -issuer -enddate
openssl x509 -in "$LDAP_CA_FILE" -noout -text | grep -A1 "Basic Constraints"
```

**Beklenen çıktı** (örnek — kök CA'da `subject` ile `issuer` aynıdır ve `CA:TRUE`
görünür):

```text
subject=DC=com, DC=example, CN=example-CA
issuer=DC=com, DC=example, CN=example-CA
notAfter=Mar 14 09:21:07 2031 GMT
            X509v3 Basic Constraints: critical
                CA:TRUE
```

Hangi CA'yı isteyeceğinizi bilmiyorsanız sunucunun gönderdiği zincire bakın. Bu çıktı
CA dosyasının **yerine geçmez**, yalnız imzalayan CA'nın adını söyler:

`[bastion]`

```bash
host_port="${LDAP_URL#ldaps://}"
case "$host_port" in *:*) ;; *) host_port="$host_port:636";; esac
openssl s_client -showcerts -connect "$host_port" </dev/null > ad-chain.pem 2>&1
grep -E "^(depth|verify return|subject=|issuer=)" ad-chain.pem | head -6
```

**Beklenen çıktı** (örnek — `issuer` satırındaki ad, AD ekibinden istenecek CA'dır):

```text
depth=1 DC = com, DC = example, CN = example-CA
verify return:1
depth=0 CN = ad.example.com
subject=CN = ad.example.com
issuer=DC = com, DC = example, CN = example-CA
```

Bu kök CA dosyası kurulumda `ad-ca` Secret'ına kaydedilir ve ürün onu Keycloak, Trino ve
Zeppelin'e kendisi bağlar ([30-kurulum](30-kurulum.md) §5.13).

**Ters giderse:**

- `Can't contact LDAP server` → 636 portu kapalı ya da `$LDAP_URL` yanlış.
- `Invalid credentials (49)` → servis hesabının adı ya da parolası yanlış, ya da
  parolanın süresi dolmuş.
- Üçten az `cn:` satırı → gruplar açılmamış ya da `$LDAP_GROUPS_DN` yanlış yeri
  gösteriyor. Grupların tam yerini (DN) AD ekibiyle karşılaştırın.
- `memberOf` sorgusu boş → gruplara henüz üye eklenmemiş.
- `unable to load certificate` → dosya PEM biçiminde değil (AD ekibi çoğu zaman `.cer`
  verir). Çevirmek için: `openssl x509 -inform der -in ad-ca.cer -out ad-ca.pem`.
- `CA:TRUE` yerine `CA:FALSE` → dosya sunucunun sertifikasıdır, kök CA değil. AD
  ekibinden doğru dosyayı isteyin.
- Şifresiz LDAP (`ldap://`, port 389) kabul edilmez: ürün servis hesabının parolasını ağda
  açık göndermez.

---

## 7. DNS ve sertifika: `$APPS_DOMAIN`

**Neden:** bütün arayüzler OpenShift [Route](00-genel-bakis.md#4-kavramlar-sözlüğü)'u ile
açılır ve adresleri kendiliğinden türetilir: bileşen adı + `-` + `$LAKEHOUSE_NS` + `.` +
`$APPS_DOMAIN` (ör. `superset-lakehouse.apps.ocp.example.net`). Bu yüzden ayrı DNS kaydı
gerekmez; kümenin joker (`*.apps…`) kaydı yeter. Oturum açma adresleri de aynı değerden
üretilir; `$APPS_DOMAIN` yanlışsa giriş sayfası döngüye girer.

**Kimin işi:** platform ekibi (joker DNS, gerekiyorsa kurumsal sertifika); kurulumcu
doğrular.

`[bastion]`

```bash
oc get ingresses.config cluster -o jsonpath='{.spec.domain}{"\n"}'
getent hosts "console-openshift-console.$APPS_DOMAIN" | head -1
```

**Beklenen çıktı** (örnek):

```text
apps.ocp.example.net
10.20.30.40    console-openshift-console.apps.ocp.example.net
```

İlk satır `install/lakehouse.env` içindeki `$APPS_DOMAIN` ile aynı olmalıdır. İkinci
satır, joker adın **kullanıcıların ağından** çözüldüğünü gösterir.

**Kurumsal ad ve sertifika.** Bir arayüz için `trino.kurum.example.net` gibi kurumsal bir
ad isteniyorsa, o ad için kümenin joker adına giden bir **CNAME** kaydı açılır ve kurulumda
ilgili `hostname` ayarı doldurulur. Kurumsal CA'dan sertifika alınacaksa talebi **şimdi**
açın: çoğu kurumda sertifika teslimi kurulumun önündeki en uzun beklemedir.

**Ters giderse:** `getent` boş dönerse joker ad kullanıcıların ağından çözülmüyordur ve
arayüzlere kimse erişemez. Kurulum yine yapılabilir, ama kabul testinden önce DNS
düzeltilmelidir.

---

## 8. (İsteğe bağlı) nginx erişim günlükleri: ağ erişimi

**Neden:** yalnız nginx erişim günlüklerini toplayacaksanız gerekir; veritabanı akışları
bu maddeye ihtiyaç duymaz. Web sunucularına kurulan Fluent Bit ajanları kümenin
**dışındadır** ve Kafka'ya doğrudan yazar. Kafka bunun için kümenin dışına, her broker
için bir Route ile açılır; trafik TLS ve kullanıcı adı/parola ile korunur.

**Kimin işi:** platform ekibi (ağ izni); kurulumcu (dinleyicinin açılması).

Şimdi hazır olması gereken üç şey:

1. Ajan sunucularından kümeye `443/TCP` açık olmalıdır.
2. Ajan sunucuları `$APPS_DOMAIN` altındaki adları çözebilmelidir.
3. Ajan sunucuları, kümenin Route sertifikasını imzalayan CA'ya güvenmelidir.

İlk ikisini ajan sunucusunda (kümede değil, nginx'in çalıştığı web sunucusunda)
kontrol edin:

`[nginx ajan sunucusu]`

```bash
getent hosts "console-openshift-console.$APPS_DOMAIN" | head -1
timeout 5 bash -c "</dev/tcp/console-openshift-console.$APPS_DOMAIN/443" && echo "443 acik"
```

**Beklenen çıktı** (örnek):

```text
10.20.30.40    console-openshift-console.apps.ocp.example.net
443 acik
```

Kafka'nın dışarı açılması, ajanın kurulumu ve CA dosyasının ajana verilmesi işletme
bölümündedir: [50-isletme/yeni-kaynak-ve-pipeline.md](50-isletme/yeni-kaynak-ve-pipeline.md)
§10.

**Ters giderse:** ikinci komut `443 acik` yazmadan biterse güvenlik duvarı kapalıdır ve
ajanlar kurulamaz. nginx günlüklerini toplamayacaksanız bu maddeyi atlayın ve kontrol
listesinde "gerekmiyor" diye işaretleyin.

---

## 9. (İsteğe bağlı) İzleme, yedekleme ve günlükler

### 9.1 Kullanıcı iş yükü izlemesi — önerilir

**Neden:** ürün kendi Prometheus'unu kurmaz. Kafka, Connect, Spark ve Polaris ölçümlerini
OpenShift'in kendi izleme sistemi toplar. Bunun için **kullanıcı iş yükü izlemesinin**
(User Workload Monitoring) açık olması gerekir. Kapalıysa ürünün izleme tanımları kümede
durur ama kimse okumaz ve alarmların hiçbiri çalışmaz.

**Kimin işi:** platform ekibi (cluster-admin).

Önce izleme ayarının var olup olmadığına bakın:

`[bastion]`

```bash
oc -n openshift-monitoring get cm cluster-monitoring-config -o yaml
```

Çıktı `NotFound` ise ayarı aşağıdaki komutla yaratın. Ayar zaten **varsa** bu komutu
**çalıştırmayın** (Ters giderse bölümüne bakın):

`[bastion]`

```bash
oc apply -f - <<'YAML'
apiVersion: v1
kind: ConfigMap
metadata:
  name: cluster-monitoring-config
  namespace: openshift-monitoring
data:
  config.yaml: |
    enableUserWorkload: true
YAML
```

Birkaç dakika sonra izleme pod'larının geldiğini kontrol edin:

`[bastion]`

```bash
oc -n openshift-user-workload-monitoring get pods
```

**Beklenen çıktı** (pod adlarının sonları sizde farklıdır):

```text
NAME                                   READY   STATUS    RESTARTS   AGE
prometheus-operator-57b466777c-rnhj8   2/2     Running   0          3d4h
prometheus-user-workload-0             6/6     Running   0          3d4h
prometheus-user-workload-1             6/6     Running   0          3d4h
thanos-ruler-user-workload-0           4/4     Running   0          3d4h
thanos-ruler-user-workload-1           4/4     Running   0          3d4h
```

**Ters giderse:** ayar zaten varsa `oc apply` içindeki bütün ayarları değiştirir ve
kümenin diğer izleme ayarlarını siler. Bu durumda
`oc -n openshift-monitoring edit cm cluster-monitoring-config` ile dosyayı açın ve
`config.yaml` altına yalnız `enableUserWorkload: true` satırını ekleyin. Pod'lar birkaç
dakika sonra da gelmiyorsa `config.yaml` içeriğinin geçerli YAML olduğunu kontrol edin.

### 9.2 OADP (yedekleme) — ad alanı yedeği alınacaksa

**Neden:** ad alanındaki nesnelerin ve disklerin yedeği OpenShift'te **OADP**
operatörüyle alınır. Ürün bu operatörün nesne türlerini kullanır; OADP kurulmadan yedek
ayarı açılırsa ürün `Degraded` olur. Doğru sıra: önce OADP, sonra ürünün yedek ayarı.
PostgreSQL veritabanlarının sürekli yedeği ise OADP'den bağımsızdır ve ürünle birlikte
gelir.

**Kimin işi:** platform ekibi (operatör), kurulumcu (ayarlar).

Şimdi yalnız şu karar verilir: **ad alanı yedeği alınacak mı?** Alınacaksa OADP operatörü
OperatorHub'dan `openshift-adp` ad alanına kurulur. Kurulu olup olmadığına bakın:

`[bastion]`

```bash
oc get csv -n openshift-adp
```

**Beklenen çıktı:** operatör kuruluysa `PHASE` sütunu `Succeeded` olan bir satır (örnek):

```text
NAME                   DISPLAY   VERSION   PHASE
oadp-operator.v1.5.2   OADP      1.5.2     Succeeded
```

Operatör kurulu değilse çıktı şudur:

```text
No resources found in openshift-adp namespace.
```

Ad alanı yedeği **almayacaksanız** bu normaldir; yedek ayarı kapalı kalır ve bir şey
yapmanız gerekmez. Alacaksanız operatörü kurun; kurulum ayrıntıları yedekleme
bölümündedir: [50-isletme/yedek-ve-geri-donus.md](50-isletme/yedek-ve-geri-donus.md) §7.

**Ters giderse:** `PHASE` `Succeeded` olmuyorsa §2.1'deki "Ters giderse" maddesiyle aynı
yoldan operatör kataloğuna bakın.

### 9.3 Günlükler (Loki) — bilgi

OpenShift Logging (Loki) ön koşul **değildir**; ürün ona bağlı değildir. Kümede zaten
varsa pod günlükleri web konsolundan aranabilir ve sorun gidermeyi kolaylaştırır. Yoksa
`oc logs` yeterlidir.

---

## 10. Dışarıya erişim (internet adresleri)

**Neden:** kurulum ve bazı işlemler internetten imaj, kütüphane ve paket indirir.
Aşağıdaki adreslerin her biri ya doğrudan ya da kurumsal bir ayna üzerinden erişilebilir
olmalıdır.

**Kimin işi:** platform ve ağ ekibi açar; kurulumcu doğrular.

| Adres | Ne için | Ne zaman |
|---|---|---|
| `$GIT_REPO_URL` (kurumun Git sunucusu) | ArgoCD | sürekli |
| `repo1.maven.org` (Maven Central) | Kafka Connect imajının üretimi; Spark kütüphaneleri (küme içi Maven önbelleği üzerinden); Zeppelin'in Trino sürücüsü | kurulum, sürüm yükseltmesi |
| `pypi.org`, `files.pythonhosted.org` | not defteri paketleri, `apache-polaris` komut satırı aracı | kurulum ve not defteri açılışı |
| `quay.io` | Strimzi, Kafka ve Connect imajları; cert-manager; not defteri imajı | kurulum, imaj çekimi |
| `ghcr.io` | Superset operatörü, CloudNativePG ve yedek eklentisi, spark-operator | kurulum |
| `docker.io` | `apache/spark`, `apache/zeppelin`, `apache/polaris`, `apache/superset`, `trinodb/trino`, `dzikoysk/reposilite` | kurulum, imaj çekimi |
| `apachesuperset.docker.scarf.sh` | Superset imajının varsayılan adresi (yönlendirici) | kurulum — kurumsal ayna kullanılırsa gerekmez |
| `registry.redhat.io` ve `redhat-operators` kataloğu | OpenShift GitOps ve (isteğe bağlı) OADP operatörleri | operatör kurulumu |
| `cloudnative-pg.io`, `kubeflow.github.io`, `trinodb.github.io`, `hub.jupyter.org`, `downloads.apache.org` | ArgoCD'nin kurulum paketleri (Helm chart depoları) | kurulum, sürüm yükseltmesi |
| `registry.k8s.io` | JupyterHub'ın yardımcı imajı | kurulum ve her yeni düğüm |
| `github.com` (`keycloak/keycloak-k8s-resources`) | Keycloak operatörünün tanımları | kurulum, sürüm yükseltmesi |

**Spark kütüphaneleri sürekli internet gerektirmez.** Ürün kümenin içinde bir Maven
önbelleği çalıştırır (`maven-proxy`). Önbellek bir kütüphaneyi Maven Central'dan yalnız
ilk istendiğinde indirir ve saklar; sonraki Spark işleri kütüphaneyi önbellekten alır.
İnternet erişimi yalnız kurulumda ve bir sürüm yükseltmesinden sonraki ilk Spark işinde
gerekir.

**Docker Hub için kurumsal bir ayna önerilir.** Ürünün altı imajı `docker.io`'dan gelir
(yukarıdaki tablo). Docker Hub, kimlik doğrulaması yapmayan istemciler için çekme sayısını
sınırlar; kümedeki bütün düğümler aynı dış IP adresinden çıkıyorsa sınıra kısa sürede
ulaşılır ve pod'lar `ImagePullBackOff` / `toomanyrequests` hatasıyla açılamaz. Bunu önlemek
için kurumsal bir registry aynası (Quay, Nexus, Artifactory ya da OpenShift'in imaj aynası
ayarı) kullanın ya da kümeye Docker Hub hesabıyla çekme yetkisi tanımlayın. Platform
ekibiyle kurulumdan önce karar verin.

**Adrese göre izin veren güvenlik duvarlarında** yukarıdaki adresler yetmez; imaj
depoları kimlik doğrulama ve dosya dağıtımı için ayrı adresler kullanır. Şunlar da
açılmalıdır:

- `docker.io` için: `registry-1.docker.io`, `auth.docker.io`,
  `production.cloudflare.docker.com`
- `quay.io` için: `cdn01.quay.io`, `cdn02.quay.io`, `cdn03.quay.io`
- `ghcr.io` için: `pkg-containers.githubusercontent.com`
- `github.com` için: `objects.githubusercontent.com`

Yönetim makinesinden hızlı bir kontrol:

`[bastion]`

```bash
for h in repo1.maven.org pypi.org quay.io ghcr.io docker.io downloads.apache.org; do
  code=$(curl -sS -o /dev/null -w '%{http_code}' --max-time 10 "https://$h/")
  echo "$h $code"
done
```

**Beklenen çıktı** (örnek — 200, 301 ya da 403 "erişilebiliyor" demektir; `000`
bağlantının hiç kurulamadığını gösterir ve `curl` ayrıca bir hata satırı yazar):

```text
repo1.maven.org 200
pypi.org 200
quay.io 200
ghcr.io 200
docker.io 301
downloads.apache.org 200
```

Bu kontrol yönetim makinesinden yapılır; asıl önemli olan **kümedeki pod'ların**
erişimidir. Küme dışarıya bir vekil sunucu (proxy) üzerinden çıkıyorsa
`oc get proxy cluster -o yaml` çıktısındaki ayarların yukarıdaki adresleri kapsadığını
platform ekibiyle kontrol edin.

**İnternete kapalı ağda kurulum** için üç ayna önceden hazır olmalıdır:

1. **İmaj aynası:** kurumsal registry.
2. **Maven aynası:** kurumun Nexus ya da Artifactory'si. Küme içi Maven önbelleğinin
   kaynağı olarak tanıtılır (`mavenProxy.upstream`;
   [sorun-giderme.md](50-isletme/sorun-giderme.md) §6.2).
3. **PyPI aynası:** not defteri paketleri ve komut satırı araçları için.

Aynalardan biri eksikse kurulum başlamaz ya da ilk Spark işinde hata verir.

---

## Kontrol listesi

- [ ] **1.** `oc whoami` çalışıyor, `oc auth can-i '*' '*' --all-namespaces` → `yes`,
      Kubernetes sürümü 1.33 ya da üstü.
- [ ] **2.** OpenShift GitOps operatörü `Succeeded`, `openshift-gitops-server` hazır,
      ArgoCD'nin küme yetkisi kontrolü `yes`.
- [ ] **3.** En az bir StorageClass var (`ALLOWVOLUMEEXPANSION: true`); varsayılan yoksa
      `$STORAGE_CLASS` dolduruldu. Sürücüsü her düğümde var; yoksa §3.1'deki düğüm kuralı
      kondu.
- [ ] **4.** İki bucket ve iki ayrı anahtar çifti; okuma **ve** yazma/silme testi hatasız.
      STS durumu depolama ekibinden soruldu. S3 sertifikası özel CA'lıysa kök CA
      `$S3_CA_FILE` yolunda; S3 veri IP'leri `$S3_EGRESS_CIDRS` değerinde.
- [ ] **5.** İç registry açık (`oc registry info --internal` adres veriyor);
      `connect-build` servis hesabı ve `connect-push` Secret'ı yaratıldı, Secret
      etiketlendi, jetonun bitiş tarihi takvime yazıldı.
- [ ] **6.** Üç AD grubu görünüyor, `lakehouse-admins` en az bir üyeli, servis hesabı
      çalışıyor, `$LDAP_BIND_USER` + `$AD_UPN_SUFFIX` servis hesabının
      `userPrincipalName`'iyle aynı, kök CA PEM dosyası `$LDAP_CA_FILE` yolunda.
- [ ] **7.** `$APPS_DOMAIN` kümeden alınan değerle aynı ve joker ad kullanıcıların
      ağından çözülüyor; kurumsal sertifika gerekiyorsa talebi açıldı.
- [ ] **8.** nginx günlükleri toplanacaksa ajan sunucularından 443 açık ve CA güveni
      var; toplanmayacaksa "gerekmiyor" işaretlendi.
- [ ] **9.** Kullanıcı iş yükü izlemesi açık (`prometheus-user-workload-0` çalışıyor);
      ad alanı yedeği alınacaksa OADP kararı verildi.
- [ ] **10.** İnternet adresleri açık ya da aynaları hazır; Docker Hub için ayna ya da
      hesap kararı verildi; küme vekil sunucusu bu adresleri kapsıyor.

## Sonraki bölüm

[30-kurulum.md](30-kurulum.md) — deponun kurumun Git sunucusuna kopyalanması, ayar
dosyalarının doldurulması, parolaların kümeye kaydedilmesi, kurulum betiğinin
çalıştırılması ve kurulumun doğrulanması.
