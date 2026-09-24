# 20 — Ön koşullar

**Bu bölümde:** kuruluma başlamadan önce kümede, depolamada, Active Directory'de ve
güvenlik duvarında hazır olması gereken **on madde**; her madde için nedeni, kimin işi
olduğu, doğrulama komutu, beklenen çıktı ve komut hata verdiğinde ne yapılacağı.
**Süre:** 1–2 saat (değerler [10-planlama](10-planlama.md) §5'teki çalışma sayfasına
toplanmışsa). GitOps operatörünün kurulması tek başına 5–10 dakikadır; AD ve S3
maddeleri başka ekiplerde olduğu için takvimde günlerle ölçülür.
**Gereken yetki:** kümede **cluster-admin** (operatör kurulumu, ClusterRoleBinding,
`openshift-monitoring` ad alanı). S3 ve AD maddelerini ilgili ekipler yapar; bu bölümde
siz yalnız **doğrularsınız**.
**Nerede çalıştırılır:** `[bastion]` — kümeye `oc login` ile girilmiş yönetim makinesi.
**Tek istisna madde 8'dir:** oradaki iki komut nginx ajanının kurulacağı web sunucusunda
(`[nginx ajan sunucusu]`) çalıştırılır.

Yönetim makinesinde kurulu olması gereken araçlar (kitabın tamamı için):

| Araç | Nerede gerekir |
|---|---|
| `oc` | her bölüm |
| `kubectl` | `scripts/polaris-setup.sh` ve `scripts/acceptance.sh` bu ikiliyi **adıyla** çağırır ([40-kurulum-sonrasi](40-kurulum-sonrasi.md) §2 ve §7); yalnız `oc` kurulu makinede `kubectl: command not found` alınır |
| `aws` | madde 4 (S3 doğrulaması) |
| `ldapsearch` | madde 6 (Active Directory doğrulaması) |
| `getent` | madde 7 (DNS doğrulaması) |
| `curl` | madde 4, madde 10 ve [30-kurulum](30-kurulum.md) §7.3 (Route denetimi) |
| `openssl` | [30-kurulum](30-kurulum.md) §5 (rastgele sır üretimi, sertifika incelemesi) |
| `python3` | [30-kurulum](30-kurulum.md) §5 (Secret doğrulaması), `scripts/polaris-setup.sh` |
| `git` | [30-kurulum](30-kurulum.md) §2–§4 |
| `htpasswd` | [30-kurulum](30-kurulum.md) §5.7 (RHEL/Fedora: `httpd-tools`, Debian/Ubuntu: `apache2-utils`) |
| `jq` | kabul koşusu ([90-referans/kabul-testleri.md](90-referans/kabul-testleri.md) §1) |

---

## Başlamadan önce: değişkenleri yükleyin

Bu bölümdeki bütün komutlar `install/lakehouse.env` dosyasındaki değişkenleri kullanır
(dosyayı 10-planlama §5'te kopyalayıp doldurdunuz). Her yeni terminal oturumunda:

`[bastion]`

```bash
cd ~/lakehouse                      # depoyu `git clone` ile nereye indirdiyseniz orası
set -a; . install/lakehouse.env; set +a
echo "$ARGOCD_NS / $LAKEHOUSE_NS / $APPS_DOMAIN"
```

**Beklenen çıktı** (örnek — değerler sizin dosyanızdan gelir):

```text
openshift-gitops / lakehouse / apps.ocp.example.net
```

**Ters giderse:** `cd` komutu `No such file or directory` derse depo başka bir dizindedir
(`find "$HOME" -maxdepth 3 -name lakehouse.env.example` ile bulabilirsiniz). Çıktı boşsa
dosya yüklenmemiştir (`ls -l install/lakehouse.env`), üçüncü
alan boşsa `APPS_DOMAIN` doldurulmamıştır — 10-planlama §3 Adım 1'e dönün.

---

## Neler ön koşul DEĞİL

> **OperatorHub'dan şu operatörleri KURMAYIN.** Ürün bunları ArgoCD ile kendisi kurar
> (`platform/apps/` altındaki Application'lar, sync-wave sırasıyla — sync-wave, ArgoCD'nin
> uygulama sırasıdır: küçük numaralı dalga önce uygulanır, operatörler 0. dalgadadır):
> **Strimzi** Kafka
> operatörü, **CloudNativePG** + **Barman Cloud** eklentisi, **cert-manager**,
> **spark-operator**, **Keycloak** operatörü, **Superset** operatörü. Aynı operatörü bir
> de OperatorHub'dan kurmak CRD'lerin iki sahibi olması demektir: sürümler çakışır,
> ArgoCD sürekli `OutOfSync` kalır ve yükseltme yolu kırılır.
>
> Aynı şekilde ön koşul **değildir**: Kafka/PostgreSQL/Trino kurulumları, ayrı bir
> Prometheus/Grafana yığını (OpenShift'in kendi izlemesi kullanılır), özel konteyner
> imajı üretimi (Kafka Connect imajı kümede üretilir) ve MinIO (yalnız geliştirici
> kümesinde vardır; üretimde müşterinin S3'ü kullanılır).
>
> **Ön koşul olan tek operatör:** OpenShift GitOps (madde 2). İsteğe bağlı olarak OADP
> (madde 9).

---

## 1. OpenShift kümesi, `oc` ve cluster-admin

**Neden:** kurulum boyunca CRD yaratmak, ad alanı açmak, küme düzeyinde rol bağlamak ve
`openshift-monitoring` ad alanına yazmak gerekir; bunların hiçbiri proje yöneticisi
yetkisiyle yapılamaz.
**Kimin işi:** platform (OpenShift) ekibi hesabı verir; doğrulamayı kurulumcu yapar.

`[bastion]`

```bash
oc whoami
oc version
oc auth can-i '*' '*' --all-namespaces
```

**Beklenen çıktı** (örnek — sürümler kümenize göre değişir):

```text
kube:admin
Client Version: 4.20.3
Kustomize Version: v5.7.1
Server Version: 4.20.3
Kubernetes Version: v1.33.4
yes
```

Sunucunun **Kubernetes sürümü 1.33 ya da üstü** olmalıdır (ürün bu tabanda geliştirilip
sınanmıştır); OpenShift'te bu 4.20 ve üstü sürümlere karşılık gelir
**(OpenShift'te doğrulanır)**. Son satır `yes` değilse hesabınız cluster-admin değildir.

**Ters giderse:** `oc: command not found` → `oc` istemcisi kurulu değil, OpenShift web
konsolunun sağ üstündeki "Command line tools" bağlantısından indirin. `You must be logged
in` → `oc login` ile girin. Son satır `no` ise kuruluma başlamayın: cluster-admin olmadan
madde 2 ve madde 9 uygulanamaz, yarım kalmış kurulum geri almaktan daha pahalıdır.

---

## 2. OpenShift GitOps operatörü (ArgoCD) ve controller yetkisi

**Neden:** bu ürünün kurulum yolu GitOps'tur: kümeye elle `oc apply` yapılmaz, ArgoCD
Git'teki hâli kümeye uygular. ArgoCD'yi OpenShift'te **platformun GitOps operatörü**
sağlar; `bootstrap/bootstrap.sh` ad alanında `openshift-gitops-server` ya da
`argocd-server` Deployment'ını görürse yukarı akış ArgoCD manifestini **uygulamaz**,
"ArgoCD zaten kurulu" diyip yalnız kök Application'ı yazar. Operatör kurulu değilse
betik yukarı akış ArgoCD'sini kurmaya kalkar ve operatörle çakışır.
**Kimin işi:** platform ekibi (cluster-admin).

### 2.1 Operatör aboneliği

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

Operatör kurulumu bitince (1–3 dakika) `openshift-gitops` ad alanını ve içinde bir ArgoCD
örneğini **kendisi** yaratır:

`[bastion]`

```bash
oc get csv -n "$ARGOCD_NS"
oc -n "$ARGOCD_NS" get deploy openshift-gitops-server
```

`csv` burada **ClusterServiceVersion** demektir: OpenShift'in operatör yöneticisi OLM,
kurduğu her operatör sürümü için bu nesneyi yaratır; kurulumun bitip bitmediği ondaki
`PHASE` alanından okunur.

**Beklenen çıktı** (örnek — operatör sürümü kümenize göre değişir):

```text
NAME                                DISPLAY                    VERSION   PHASE
openshift-gitops-operator.v1.18.0   Red Hat OpenShift GitOps   1.18.0    Succeeded
NAME                      READY   UP-TO-DATE   AVAILABLE   AGE
openshift-gitops-server   1/1     1            1           2m
```

`PHASE` **Succeeded** olmalıdır **(OpenShift'te doğrulanır)**.

**Ters giderse:** `no matches for kind "Subscription"` → küme OpenShift değildir (OLM
yok); vanilla Kubernetes kurulumu bu belgenin kapsamı dışındadır. `PHASE` uzun süre
`Pending` kalıyorsa katalog kaynağı kapalı olabilir:
`oc -n openshift-marketplace get catalogsource redhat-operators` çıktısına ve
`oc -n openshift-operators get installplan` satırlarına bakın; bağlantısız kümede ayna
katalog gerekir.

### 2.2 Controller'ın küme genelinde yetkisi

**Neden:** ArgoCD bu kurulumda **operatör kurar** (`platform/apps/` altındaki
Application'lar, sync-wave 0): Strimzi, CNPG, cert-manager, spark-operator, Keycloak,
Superset. Operatör kurmak CRD ve küme düzeyinde RBAC yaratmak demektir; ArgoCD'nin
uygulama denetleyicisi bunu ancak cluster-admin ile yapabilir. Yetki yoksa ilk sync
`cannot create resource "customresourcedefinitions"` hatasıyla durur.

OpenShift GitOps'un varsayılan `openshift-gitops` örneğinde bu yetki çoğu sürümde
operatör tarafından zaten verilir. **Önce doğrulayın:**

`[bastion]`

```bash
oc auth can-i create customresourcedefinitions \
  --as="system:serviceaccount:$ARGOCD_NS:openshift-gitops-argocd-application-controller"
```

**Beklenen çıktı:**

```text
yes
```

Çıktı `no` ise yetkiyi verin **(OpenShift'te doğrulanır — servis hesabı adı kurulu GitOps
sürümüyle birlikte doğrulanmalıdır)**:

`[bastion]`

```bash
oc adm policy add-cluster-role-to-user cluster-admin \
  -z openshift-gitops-argocd-application-controller -n "$ARGOCD_NS"
```

**Beklenen çıktı** (örnek):

```text
clusterrole.rbac.authorization.k8s.io/cluster-admin added:
  "openshift-gitops-argocd-application-controller"
```

**Ters giderse:** komut `serviceaccount ... not found` derse ArgoCD örneği henüz
yaratılmamıştır (2.1'deki `openshift-gitops-server` Deployment'ını bekleyin) ya da servis
hesabı adı farklıdır: `oc -n "$ARGOCD_NS" get sa` ile gerçek adı bulun, `-z` değerini ona
göre verin. Kurumunuz cluster-admin vermiyorsa kurulum bu hâliyle yapılamaz; daraltılmış
bir rol seti ürünle birlikte test edilmemiştir.

---

## 3. StorageClass

**Neden:** Kafka, üç PostgreSQL kümesi, Zeppelin ve her kullanıcının not defteri kalıcı
disk ister (bkz. [PVC](00-genel-bakis.md#4-kavramlar-sözlüğü)). Kümede kullanılabilir bir
[StorageClass](00-genel-bakis.md#4-kavramlar-sözlüğü) yoksa pod'lar `Pending` kalır.
**Kimin işi:** platform ekibi sağlar; kurulumcu doğrular ve `$STORAGE_CLASS`'ı doldurur.

`[bastion]`

```bash
oc get storageclass
```

**Beklenen çıktı** (örnek):

```text
NAME                                    PROVISIONER                          ALLOWVOLUMEEXPANSION
ocs-storagecluster-ceph-rbd (default)   openshift-storage.rbd.csi.ceph.com   true
thin-csi                                csi.vsphere.vmware.com               true
```

Sınıf **ReadWriteOnce** blok depolama vermeli ve `ALLOWVOLUMEEXPANSION` **true**
olmalıdır: Kafka diskini büyütmek sonradan en sık yapılan işlemdir. `(default)` işaretli
bir sınıf varsa `$STORAGE_CLASS` boş bırakılabilir.

**Ters giderse:** liste boşsa küme depolamasız kurulmuştur, platform ekibine başvurun.
Varsayılan sınıf yoksa `$STORAGE_CLASS` doldurulmak **zorundadır**; boş bırakılırsa
PVC'ler sınıfsız kalır ve bağlanmaz.

---

## 4. S3: iki bucket, iki anahtar çifti

**Neden:** Iceberg verisi (Bronze/Silver/Gold) `$S3_BUCKET_DATA` bucket'ında yaşar;
PostgreSQL yedekleri **ayrı** bir bucket'a (`$S3_BUCKET_BACKUP`) ve **ayrı** bir anahtar
çiftiyle yazılır. Ayrılığın nedeni: veri anahtarları sızsa bile yedeklerin silinememesi
ve geri yükleme hedefiyle kaynağın aynı olmaması.
**Kimin işi:** depolama ekibi bucket'ları ve anahtarları yaratır; kurulumcu doğrular.

### 4.1 Okuma doğrulaması

`[bastion]`

```bash
AWS_ACCESS_KEY_ID=$S3_ACCESS_KEY AWS_SECRET_ACCESS_KEY=$S3_SECRET_KEY \
  aws --endpoint-url "$S3_ENDPOINT" --region "$S3_REGION" s3 ls "s3://$S3_BUCKET_DATA"
AWS_ACCESS_KEY_ID=$S3_BACKUP_ACCESS_KEY AWS_SECRET_ACCESS_KEY=$S3_BACKUP_SECRET_KEY \
  aws --endpoint-url "$S3_ENDPOINT" --region "$S3_REGION" s3 ls "s3://$S3_BUCKET_BACKUP"
```

**Beklenen çıktı** (örnek — iki komut da **hatasız** biter; yeni bucket boşsa hiç satır
basılmaz, önemli olan hata satırı olmamasıdır):

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

**Beklenen çıktı** (örnek — dört satır: veri bucket'ı yaz/sil, yedek bucket'ı yaz/sil):

```text
upload: /tmp/lakehouse-on-kosul.txt to s3://lakehouse/_on-kosul-testi.txt
delete: s3://lakehouse/_on-kosul-testi.txt
upload: /tmp/lakehouse-on-kosul.txt to s3://lakehouse-backups/_on-kosul-testi.txt
delete: s3://lakehouse-backups/_on-kosul-testi.txt
```

**Her iki çift de yazabilmelidir.** Yedek anahtarı yalnız okuyabiliyorsa CNPG yedekleri
kurulumdan sonra sessizce başarısız olur.

**Geçici kimlik (STS) notu.** Polaris, istemcilere kalıcı anahtar yerine geçici S3 kimliği
dağıtabilir; bunun için depolamanın STS desteklemesi gerekir. FlashBlade'in bu kurulumdaki
sürümünde STS'in açık olup olmadığı **depolama ekibinden sorulur**: cevaba göre kurulumda
`platform/values/site/glue.yaml` dosyasındaki `s3.vendedCredentials` anahtarı `true`
(STS var) ya da `false` (STS yok) yapılır. Değeri şimdi yazmayın; satırın doldurulması
[30-kurulum](30-kurulum.md) §4.3'tedir.

**Ters giderse:** `Could not connect to the endpoint URL` → `$S3_ENDPOINT` yanlış ya da
güvenlik duvarı 443'ü kapatıyor. `SSL validation failed` → depolamanın sertifikası
kurumsal CA ile imzalı; CA'yı bastion'a ekleyin (`AWS_CA_BUNDLE`) ve kümenin de aynı
CA'ya güvendiğini platform ekibiyle doğrulayın. `AccessDenied` → anahtar çifti o bucket'a
yetkili değil; yedek anahtarını veri bucket'ında denemediğinizden emin olun.
`NoSuchBucket` → bucket açılmamıştır.

---

## 5. İç registry ve Connect imajını itecek hesap

**Neden:** Kafka Connect imajı (Debezium + Iceberg sink eklentileriyle) **kümede** üretilir
— Dockerfile ve elle imaj yapımı yoktur. Strimzi imajı üretip
`$INTERNAL_REGISTRY/$LAKEHOUSE_NS/connect:<sürüm>` adresine iter; itme için
`kubernetes.io/dockerconfigjson` tipinde bir Secret gerekir. Adı `connect-push`'tur ve
`platform/values/site/glue.yaml` dosyasında `connect.buildPushSecret` anahtarıyla
bağlanır. Secret yoksa build `unauthorized` ile başarısız olur, Connect hiç ayağa kalkmaz.
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
python3 -c 'import base64, json, sys, datetime
t = sys.argv[1].split(".")[1]; t += "=" * (-len(t) % 4)
print("jeton bitisi:", datetime.datetime.fromtimestamp(
    json.loads(base64.urlsafe_b64decode(t))["exp"], datetime.timezone.utc))' "$TOKEN"
unset TOKEN
```

**Beklenen çıktı** (örnek — son satırdaki tarih jetonun **gerçek** bitiş anıdır):

```text
namespace/lakehouse created
image-registry.openshift-image-registry.svc:5000
serviceaccount/connect-build created
clusterrole.rbac.authorization.k8s.io/system:image-builder added: "connect-build"
secret/connect-push created
jeton bitisi: 2027-09-19 08:41:12+00:00
```

`--internal` bayrağı **şarttır**: bayraksız `oc registry info`, registry dışarı
açılmışsa o Route'un genel adresini basar; Strimzi'nin ihtiyaç duyduğu
küme içi adres yalnız `--internal` ile gelir. Çıktı `$INTERNAL_REGISTRY` değeriyle
**birebir aynı** olmalıdır **(OpenShift'te doğrulanır)**.

**ImageStream notu (OpenShift'te doğrulanır).** `system:image-builder` rolü itme yetkisi
verir; ilk itişte hedef `ImageStream` nesnesinin kendiliğinden yaratılıp yaratılmadığı
küme ayarına bağlıdır. Connect build'inden sonra kontrol edin — nesne yoksa elle
yaratılır:

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" get imagestream connect
# yoksa: oc -n "$LAKEHOUSE_NS" create imagestream connect
```

**Beklenen çıktı** (örnek):

```text
NAME      IMAGE REPOSITORY                                             TAGS
connect   image-registry.openshift-image-registry.svc:5000/lakehouse/connect   2.0.0
```

**Jetonun ömrü ve yenilenmesi.** `oc create token ... --duration=8760h` bir yıllık
**bağlı (bound) servis hesabı jetonu** ister; kümenin izin verdiği üst sınır daha kısaysa
OpenShift jetonu kısaltır ve bunu uyarı olarak yazar **(OpenShift'te doğrulanır)** —
gerçek süre yukarıdaki `jeton bitisi:` satırından okunur, istenen süreden değil. Jeton
dolduğunda Connect imajının **yeni** build'i `unauthorized` verir; çalışan Connect pod'u
etkilenmez ama sürüm yükseltmesi durur. Bu yüzden jetonun bitiş tarihi kurumun takvimine
yazılır ve `connect-push` Secret'ı aynı komutlarla yeniden yaratılır; yenileme adımı
yükseltme bölümündedir: docs/50-isletme/yukseltme.md (bu bölüm bir sonraki görevde
eklenir).

**Ters giderse:** jeton komutu süreyle ilgili hata verirse küme daha kısa bir üst sınır
uyguluyordur; komutu `--duration=24h` ile tekrarlayıp yenileme sıklığını ona göre
planlayın. `Error from server (AlreadyExists)` → kaynak zaten var; Secret'ı silip
(`oc -n "$LAKEHOUSE_NS" delete secret connect-push`) komutu tekrarlayın.
`oc registry info` boş dönerse iç registry kümede açık değildir: platform ekibinden
`Image Registry` operatörünün `Managed` hâle getirilmesini isteyin. Ürün harici bir
registry ile de çalışır; o durumda `$INTERNAL_REGISTRY` yerine kurumsal registry adresi
ve oraya itme yetkili bir kullanıcı/parola ile aynı `connect-push` Secret'ı yaratılır.

---

## 6. Active Directory: üç grup, üyeler, bind hesabı, LDAPS CA

**Neden:** kimlik gün-1'den [AD](00-genel-bakis.md#4-kavramlar-sözlüğü)'dedir. Keycloak
kullanıcıları ve grupları AD'den okur (`platform/values/site/glue.yaml` içindeki
`keycloak.ldap.*` anahtarları; bind parolası `keycloak-clients` Secret'ının `ldap-bind`
anahtarına yazılır), Zeppelin ise AD'ye doğrudan Shiro ile bağlanır
(`examples/zeppelin/shiro-ad.ini`). Yetki kuralları **grup adlarına** göre yazılıdır, bu
yüzden üç grup birebir bu adlarla açılmalıdır:

| AD grubu | Kim | Üründeki karşılığı |
|---|---|---|
| `lakehouse-admins` | platform/veri yöneticileri | her şeye yetki; Superset ve Trino yönetimi |
| `lakehouse-analysts` | analistler | Silver/Gold okuma, pano yazma, `sandbox` yazma |
| `lakehouse-users` | son kullanıcılar | yalnız kendilerine açılan tabloları okuma |

**Kimin işi:** AD ekibi (grupları açar, üyeleri ekler, yalnız okuma yetkili servis hesabı
ile kök CA PEM dosyasını verir); kurulumcu doğrular. Secret'ların kümede yaratılması bu
bölümde **değildir**, [30-kurulum](30-kurulum.md) §5'tedir.

`[bastion]`

```bash
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

Üyeliği de doğrulayın: en az bir yönetici `lakehouse-admins` üyesi olmalıdır, aksi hâlde
kurulumdan sonra hiçbir arayüze yönetici olarak giremezsiniz.

`[bastion]`

```bash
ldapsearch -H "$LDAP_URL" -D "$LDAP_BIND_DN" -w "$LDAP_BIND_PASSWORD" \
  -b "$LDAP_USERS_DN" "(memberOf=CN=lakehouse-admins,$LDAP_GROUPS_DN)" sAMAccountName
```

**Beklenen çıktı:** en az bir `sAMAccountName:` satırı ve `result: 0 Success`.

Son olarak AD sertifikasını imzalayan **kök CA'nın PEM dosyası** gerekir; bu dosyayı
**AD ekibi verir** ve `$LDAP_CA_FILE` tam olarak onu göstermelidir. Sunucudan çekilen
zincir bunun yerine **geçmez**: zincirin ilk sertifikası AD sunucusunun kendi (leaf)
sertifikasıdır ve kök CA çoğu zaman zincire hiç konmaz. Elinizdeki dosyanın gerçekten
CA olduğunu doğrulayın:

`[bastion]`

```bash
openssl x509 -in "$LDAP_CA_FILE" -noout -subject -issuer -enddate
openssl x509 -in "$LDAP_CA_FILE" -noout -text | grep -A1 "Basic Constraints"
```

**Beklenen çıktı** (örnek — kök CA'da `subject` = `issuer` ve `CA:TRUE` görünür):

```text
subject=DC=com, DC=example, CN=example-CA
issuer=DC=com, DC=example, CN=example-CA
notAfter=Mar 14 09:21:07 2031 GMT
            X509v3 Basic Constraints: critical
                CA:TRUE
```

**Teşhis:** dosya elinizde yoksa ya da hangi CA'yı isteyeceğinizi bilmiyorsanız sunucunun
sunduğu zincire bakın. Bu çıktı `$LDAP_CA_FILE` **yerine geçmez**, yalnız imzalayan CA'nın
adını verir:

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

**Ters giderse:** `Can't contact LDAP server` → 636 portu kümeden ve bastion'dan kapalı ya
da `$LDAP_URL` yanlış. `Invalid credentials (49)` → bind DN/parola hatalı ya da hesabın
parolasının süresi dolmuş. Üçten az `cn:` satırı geliyorsa gruplar açılmamış ya da
`$LDAP_GROUPS_DN` yanlış alt ağacı gösteriyordur — AD ekibiyle grupların tam DN'ini
karşılaştırın. `memberOf` sorgusu boş dönüyorsa gruplara henüz üye eklenmemiştir.
`openssl x509` komutu `unable to load certificate` derse dosya PEM değildir (AD ekibi
çoğu zaman `.cer`/DER verir): `openssl x509 -inform der -in ad-ca.cer -out ad-ca.pem`
ile çevirin. `CA:TRUE` yerine `CA:FALSE` görüyorsanız elinizdeki dosya sunucu
sertifikasıdır, kök CA değildir — AD ekibinden doğrusunu isteyin.
Şifresiz LDAP (`ldap://`, 389) **kabul edilmez**: ürün bind parolasını ağdan açık
geçirmez.
Bu kök CA dosyası kurulumda `ad-ca` Secret'ı olur ve chart onu Keycloak, Trino
coordinator ve Zeppelin'e **kendisi** bağlar; tüketicilerin tam listesi ve doğrulama
komutları [30-kurulum](30-kurulum.md) §5.13'tedir. Kurulumdan sonra bu üç bileşenden
birinin günlüğünde `PKIX path building failed` (ya da `unable to find valid
certification path to requested target`) görürseniz elinizdeki dosya yanlış köktür ya da
Secret eksiktir — o tabloya dönün. Uçtan uca LDAPS el sıkışması yalnız gerçek AD ile
kanıtlanabilir **(OpenShift'te doğrulanır)**.

---

## 7. DNS ve sertifika: `$APPS_DOMAIN`

**Neden:** bütün kullanıcı arayüzleri OpenShift
[Route](00-genel-bakis.md#4-kavramlar-sözlüğü)'u ile açılır ve adresler **türetilir**:
bileşen adı + `-` + `$LAKEHOUSE_NS` + `.` + `$APPS_DOMAIN`. Bu yüzden ek DNS kaydı
gerekmez, kümenin joker kaydı yeter. Keycloak'ın yönlendirme adresleri de aynı kaynaktan
üretildiği için `$APPS_DOMAIN` yanlışsa oturum açma döngüye girer.
**Kimin işi:** platform ekibi (joker DNS ve gerekiyorsa kurumsal sertifika); kurulumcu
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

Birinci satır `install/lakehouse.env` içindeki `$APPS_DOMAIN` ile aynı olmalıdır; ikinci
satır joker DNS'in **kullanıcı ağından** çözüldüğünü gösterir.

**Kurumsal ad ve sertifika.** Bir bileşen için `trino.kurum.example.net` gibi kurumsal bir
ad isteniyorsa o ad için **CNAME** kaydı kümenin joker adına yönlendirilir ve
`platform/values/site/glue.yaml` içinde ilgili `hostname` satırı açılır. Trino kendi
TLS'ini sonlandırır (Route `passthrough`), bu yüzden tarayıcıların ürünün kök CA'sına
güvenmesi gerekir; kurumsal CA kullanılacaksa sertifika talebi **şimdi** açılmalıdır:
çoğu kurumda teslim süresi kurulumun önündeki en uzun beklemedir.

**Ters giderse:** `getent` boş dönerse joker DNS kullanıcı ağından çözülmüyordur ve
arayüzlere kimse erişemez. Kurulum yine de yapılabilir (küme içi adresler çalışır) ama
kabul testinden önce DNS düzeltilmelidir.

---

## 8. (İsteğe bağlı) nginx erişim günlüğü: Kafka dış dinleyicisi

**Neden:** yalnız nginx akışını kullanacaksanız gereklidir. Web sunucularındaki Fluent Bit
ajanları kümenin **dışındadır** ve Kafka'ya doğrudan yazar; bunun için Kafka'nın dış
dinleyicisi açılır. OpenShift'te dış dinleyici tipi `route`'tur
(`glue/templates/kafka.yaml`): Strimzi her broker için birer Route yaratır, trafik
TLS + [SCRAM](00-genel-bakis.md#4-kavramlar-sözlüğü) ile korunur. Veritabanı (CDC)
akışları bu maddeye ihtiyaç duymaz.
**Kimin işi:** platform ekibi (ağ izni) + kurulumcu (dinleyicinin açılması).

Ön koşul olarak **şimdi** yapılması gereken üç şey vardır:

1. Ajan sunucularından kümeye `443/TCP` açık olmalıdır (Route'lar 443'ten geçer).
2. Ajan sunucuları `$APPS_DOMAIN` altındaki adları çözebilmelidir.
3. Ajan sunucuları kümenin Route sertifikasını imzalayan CA'ya güvenmelidir.

Ajan sunucusunda (kümede değil, nginx'in koştuğu web sunucusunda):

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

Dinleyicinin açılması, ajan kurulumu ve konu/kullanıcı tanımları işletme bölümündedir:
docs/50-isletme/yeni-kaynak-ve-pipeline.md (bu bölüm bir sonraki görevde eklenir).

**Ters giderse:** komut son satırı yazmadan biterse güvenlik duvarı kapalıdır ve ajanlar
kurulamaz. nginx akışını kullanmayacaksanız bu maddeyi atlayın, kontrol listesinde
"gerekmiyor" olarak işaretleyin.

---

## 9. (İsteğe bağlı) İzleme, yedekleme ve günlükler

### 9.1 User Workload Monitoring (UWM) — önerilir

**Neden:** ürün kendi Prometheus'unu kurmaz. Kafka, Connect, Spark ve Polaris metriklerini
OpenShift'in kendi izleme yığını toplar; bunun için **kullanıcı iş yükü izlemesinin**
açık olması gerekir. Kapalıysa glue'nun `lakehouse` ad alanına yazdığı
`PodMonitor`/`ServiceMonitor`/`PrometheusRule` nesneleri kümede durur ama kimse okumaz:
beş alarmın hiçbiri çalışmaz.
**Kimin işi:** platform ekibi (cluster-admin).

Önce ConfigMap'in **var olup olmadığına** bakın; varsa üzerine yazmayın, düzenleyin:

`[bastion]`

```bash
oc -n openshift-monitoring get cm cluster-monitoring-config -o yaml
```

ConfigMap yoksa (`NotFound`) olduğu gibi uygulayın:

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

Doğrulama:

`[bastion]`

```bash
oc -n openshift-user-workload-monitoring get pods
```

**Beklenen çıktı** (örnek):

```text
NAME                                   READY   STATUS    RESTARTS   AGE
prometheus-operator-7d9c6f8b5-vq2sn    2/2     Running   0          3m
prometheus-user-workload-0             6/6     Running   0          2m
thanos-ruler-user-workload-0           4/4     Running   0          2m
```

**Ters giderse:** ConfigMap **zaten varsa** yukarıdaki `oc apply` `config.yaml`
gövdesinin tamamını değiştirir ve kümenin diğer izleme ayarlarını siler. Bu durumda
`oc -n openshift-monitoring edit cm cluster-monitoring-config` ile yalnız
`enableUserWorkload: true` satırını ekleyin. Pod'lar gelmiyorsa ad alanının açılması
birkaç dakika sürebilir; hâlâ boşsa `data.config.yaml` gövdesinin geçerli YAML olduğunu
kontrol edin **(OpenShift'te doğrulanır)**.

### 9.2 OADP (yedekleme) — yedek alınacaksa

**Neden:** ad alanı ve kalıcı disk yedekleri OpenShift'te **OADP** operatörüyle alınır
(Red Hat'in paketlediği Velero). Ürün `velero.io` CRD'lerine bir `Schedule` yazar; OADP
kurulu **değilken** bu açılırsa CRD bulunamaz ve glue `Degraded` olur. Bu yüzden sıra
şudur: önce OADP + `DataProtectionApplication`, sonra site değerlerinde açma.
PostgreSQL'in kendi sürekli yedeği (Barman Cloud, S3 yedek bucket'ı) OADP'den bağımsızdır
ve ürünle birlikte gelir; OADP ad alanı düzeyindeki nesneler içindir.
**Kimin işi:** platform ekibi (operatör), kurulumcu (değerler).

Ön koşul olarak yalnız şu karar verilir: **ad alanı yedeği alınacak mı?** Alınacaksa
OperatorHub'dan OADP operatörü `openshift-adp` ad alanına kurulur. Kurulu olup olmadığı:

`[bastion]`

```bash
oc get csv -n openshift-adp
```

**Beklenen çıktı** (örnek — kurulmuşsa `PHASE: Succeeded`):

```text
NAME                   DISPLAY   VERSION   PHASE
oadp-operator.v1.5.2   OADP      1.5.2     Succeeded
```

**Ters giderse:** `namespaces "openshift-adp" not found` ya da `No resources found` →
operatör kurulu değildir. Ad alanı yedeği **almayacaksanız** bu normaldir: site
değerlerinde `velero` kapalı kalır ve hiçbir şey yapmanız gerekmez. Alacaksanız operatörü
kurun; `PHASE` `Succeeded` değilse katalog kaynağına bakın (madde 2.1'deki ile aynı
teşhis). Kurulum adımları, `DataProtectionApplication` içeriği ve site değerinin açılması
yedekleme bölümündedir: docs/50-isletme/yedek-ve-geri-donus.md (bu bölüm bir sonraki
görevde eklenir).

### 9.3 Günlükler (Loki) — bilgi

OpenShift Logging (Loki) **ön koşul değildir**; ürün hiçbir bileşeniyle Loki'ye bağımlı
değildir. Kümede zaten varsa pod günlükleri konsoldan aranabilir, bu da sorun gidermeyi
kolaylaştırır. Yoksa `oc logs` yeterlidir.

---

## 10. Dış erişim (egress) alan adları

**Neden:** kurulum ve **çalışma zamanı** dışarıya erişir. Özellikle Spark işleri Iceberg
kütüphanelerini **her koşuda** Maven'den çözer: kapalı ağda bu işler `Ivy` hatasıyla
başarısız olur. Aşağıdaki listedeki her adres ya doğrudan ya da kurumsal ayna üzerinden
erişilebilir olmalıdır.
**Kimin işi:** platform/ağ ekibi açar; kurulumcu doğrular.

| Adres | Kim kullanır | Ne zaman |
|---|---|---|
| `$GIT_REPO_URL` (müşterinin Git sunucusu) | ArgoCD | sürekli (her sync) |
| `repo1.maven.org` (Maven Central) | Kafka Connect build'i; Spark işleri; Zeppelin JDBC sürücüsü | build anında ve **her Spark koşusunda** |
| `pypi.org`, `files.pythonhosted.org` | not defteri `postStart` (pyiceberg, trino), dbt örneği, `apache-polaris` CLI | pod açılışında ve kurulumda |
| `quay.io` | Strimzi chart'ı + Kafka/Connect imajları, cert-manager chart'ı + imajları, `quay.io/jupyter/pyspark-notebook` | kurulum ve imaj çekimi |
| `ghcr.io` | Superset operatör chart'ı, CloudNativePG + Barman Cloud eklentisi imajları, spark-operator imajı | kurulum |
| `docker.io` | `apache/spark`, `apache/zeppelin`, `apache/polaris`, `apache/superset`, `trinodb/trino` | kurulum ve imaj çekimi |
| `apachesuperset.docker.scarf.sh` | Superset operatörünün varsayılan imaj adresi (yönlendirici) | kurulum — kurumsal ayna kullanılırsa gerekmez |
| `registry.redhat.io` ve `redhat-operators` kataloğu | OpenShift GitOps operatörü, (isteğe bağlı) OADP | operatör kurulumu |
| `cloudnative-pg.github.io`, `kubeflow.github.io`, `trinodb.github.io`, `hub.jupyter.org`, `downloads.apache.org` | ArgoCD (Helm chart depoları) | kurulum ve her yükseltme |
| `registry.k8s.io` | JupyterHub prePuller'ının `pause` imajı (`platform/values/jupyterhub.yaml` → `prePuller`) | kurulum ve her düğüm eklendiğinde |
| `github.com` (`keycloak/keycloak-k8s-resources`) | ArgoCD (Keycloak operatörünün kustomize kaynağı) | kurulum ve her sync |

**Ana bilgisayar adına göre izin veren (allowlist) güvenlik duvarlarında yukarıdaki
adresler tek başına yetmez:** registry'ler kimlik doğrulama ve içerik dağıtımı için ayrı
adlar kullanır. En az şunlar da açılmalıdır: `registry-1.docker.io`, `auth.docker.io`,
`production.cloudflare.docker.com` (docker.io); `cdn01.quay.io`, `cdn02.quay.io`,
`cdn03.quay.io` (quay.io); `pkg-containers.githubusercontent.com` (ghcr.io);
`objects.githubusercontent.com` (github.com); `files.pythonhosted.org` (PyPI —
tabloda ayrıca listelendi). Mümkünse ad yerine kurumsal registry aynası kullanın.

Listenin türetildiği yerler: `platform/apps/` altındaki Application'lar (chart depoları),
`glue/values.yaml` ve `platform/values/` altındaki değer dosyaları (imajlar),
`glue/templates/kafka-connect.yaml` (Maven artefaktları) ve
`glue/templates/spark-jobs.yaml` (`spark.jars.packages`).

Bastion'dan hızlı bir kontrol:

`[bastion]`

```bash
for h in repo1.maven.org pypi.org quay.io ghcr.io docker.io downloads.apache.org; do
  code=$(curl -sS -o /dev/null -w '%{http_code}' --max-time 10 "https://$h/")
  echo "$h $code"
done
```

**Beklenen çıktı** (örnek — 200/301/403 "erişilebiliyor" demektir; `000` bağlantının hiç
kurulamadığını gösterir ve `curl` ayrıca hata satırı basar):

```text
repo1.maven.org 200
pypi.org 200
quay.io 200
ghcr.io 200
docker.io 301
downloads.apache.org 200
```

Bu kontrol **bastion'dan** yapılır; asıl önemli olan **kümedeki pod'ların** erişimidir.
Küme çıkışı vekil sunucudan (proxy) geçiyorsa `oc get proxy cluster -o yaml` çıktısındaki
ayarların yukarıdaki adresleri kapsadığını platform ekibiyle doğrulayın.

**Ters giderse:** kapalı ağda (air-gap) kurulum yapılacaksa iki ayna zorunludur: **iç Maven
aynası** (kurulumda `platform/values/site/glue.yaml` yanında `spark.ivySettingsXml`
değeriyle verilir; Spark işleri aynayı oradan okur) ve **iç PyPI aynası**. İmajlar için
kurumsal registry aynası kullanılır. Bu üç ayna hazırlanmadan kurulum başlatılmamalıdır:
eksiklik kurulumda değil, ilk Spark koşusunda ortaya çıkar.

---

## Kontrol listesi

- [ ] **1.** `oc whoami` çalışıyor, `oc auth can-i '*' '*' --all-namespaces` → `yes`,
      Kubernetes sürümü 1.33 ya da üstü.
- [ ] **2.** OpenShift GitOps operatörü `Succeeded`, `openshift-gitops-server` hazır ve
      uygulama denetleyicisi CRD yaratabiliyor.
- [ ] **3.** En az bir StorageClass var (`ALLOWVOLUMEEXPANSION: true`); varsayılan yoksa
      `$STORAGE_CLASS` dolduruldu.
- [ ] **4.** İki bucket ve iki ayrı anahtar çifti; okuma **ve** yazma/silme testi hatasız.
      STS durumu depolama ekibinden soruldu.
- [ ] **5.** `connect-build` servis hesabı `system:image-builder` yetkisiyle var,
      `connect-push` Secret'ı yaratıldı, jetonun bitiş tarihi takvime yazıldı.
- [ ] **6.** Üç AD grubu `ldapsearch` ile görünüyor, `lakehouse-admins` en az bir üyeli,
      bind hesabı çalışıyor, kök CA PEM dosyası `$LDAP_CA_FILE` yolunda.
- [ ] **7.** `$APPS_DOMAIN` kümeden alınan değerle aynı ve joker DNS kullanıcı ağından
      çözülüyor; kurumsal sertifika gerekiyorsa talebi açıldı.
- [ ] **8.** nginx akışı kullanılacaksa ajan sunucularından 443 açık ve CA güveni var;
      kullanılmayacaksa "gerekmiyor" işaretlendi.
- [ ] **9.** UWM açık (`prometheus-user-workload-0` çalışıyor); ad alanı yedeği alınacaksa
      OADP kararı verildi.
- [ ] **10.** Egress listesindeki adresler açık ya da aynaları hazır; küme vekil sunucusu
      bu adresleri kapsıyor.

## Sonraki bölüm

[30-kurulum.md](30-kurulum.md) — deponun müşterinin Git sunucusuna kopyalanması,
`platform/values/site/` dosyalarının doldurulması, Secret'ların yaratılması,
`bootstrap/bootstrap.sh` ile bootstrap ve kurulumun doğrulanması.
