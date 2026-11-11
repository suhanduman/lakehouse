# 90 — Secret listesi

Bu sayfa kümede bulunması gereken bütün `Secret` nesnelerinin başvuru tablosudur: adı,
içindeki anahtarlar, kimin okuduğu, nereden geldiği ve silinirse ne olacağı. Secret'ları
yaratan komutlar burada değil, tablolardaki belge adımlarındadır.

**Ne kadar sürer:** okuma 10 dakika.

**Hangi yetkiler gerekir:** `$LAKEHOUSE_NS` ad alanında Secret okuma.

**Komutlar nerede çalışır:** `[bastion]` — `oc` ile kümeye bağlı yönetim makinesi.

Secret'lar **Git'e girmez**. Depoda yalnız adları yazılıdır; değerler kurulum sırasında
komutla kümeye yazılır. Bir Secret'ın nereden geldiğini (Git, operatör ya da elle) şu
betik söyler; ayrıntısı
[değişiklik nasıl uygulanır](../50-isletme/degisiklik-nasil-uygulanir.md) §7'dedir:

`[bastion]`

```bash
scripts/nesne-kaynagi.sh --liste secret
```

---

## 1. Kurulumda elle yaratılanlar

Bu Secret'lar kurulum belgesindeki adımlarla yaratılır ve yaratıldıkları adımı taşıyan
`lakehouse.io/yonetim` etiketiyle işaretlenir. Hepsi `$LAKEHOUSE_NS` ad alanındadır;
`lakehouse-repo` ise `$ARGOCD_NS` ad alanındadır.

**Silinirse:** kimse geri getirmez. Tablodaki adım tekrarlanır (etiket komutu dahil).

| Secret | Anahtarlar | Kim okur | Değer nereden | Etiket (`lakehouse.io/yonetim`) |
|---|---|---|---|---|
| `lakehouse-repo` (`$ARGOCD_NS`) | `type`, `url`, `sshPrivateKey` | ArgoCD — kurumun Git deposunu okur | kurulumda üretilen SSH anahtarı | `manual-30-kurulum-3.2` |
| `connect-push` | `.dockerconfigjson` | Kafka Connect imajını iç registry'ye gönderen yapım | iç registry servis hesabının jetonu | `manual-20-on-kosullar-5` |
| `s3-creds` | `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` | Kafka Connect ve Spark işleri | depolama ekibinin verdiği anahtar çifti | `manual-30-kurulum-5.2` |
| `backup-s3-creds` | `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` | PostgreSQL yedekleri | depolama ekibinin verdiği **ayrı** anahtar çifti | `manual-30-kurulum-5.3` |
| `polaris-root` | `clientId`, `clientSecret` | Polaris kurulum işi ve `scripts/polaris-setup.sh` | kurulumda üretilir | `manual-30-kurulum-5.4` |
| `keycloak-admin` | `username`, `password` | Keycloak yönetici hesabı | kurulumda belirlenir ya da üretilir | `manual-30-kurulum-5.5` |
| `keycloak-clients` | `trino`, `superset`, `jupyterhub`, `ldap-bind` | Keycloak, Trino, Superset, JupyterHub | üç uygulama sırrı üretilir; `ldap-bind` AD ekibinin verdiği parola | `manual-30-kurulum-5.6` |
| `trino-service-accounts` | `password.db`, `superset`, `zeppelin` | Trino, Superset, Zeppelin | kurulumda üretilir; `password.db` aynı parolaların karma (hash) hâlidir | `manual-30-kurulum-5.7` |
| `trino-shared-secret` | `secret` | Trino sunucuları arası iletişim | kurulumda üretilir | `manual-30-kurulum-5.8` |
| `superset-secret` | `secret-key` | Superset oturum imzası | kurulumda üretilir | `manual-30-kurulum-5.8` |
| `zeppelin-shiro` | `shiro.ini` | Zeppelin — AD girişi ve rol kapısı | şablon AD değerleriyle doldurulur | `manual-30-kurulum-5.9` |
| `zeppelin-interpreter` | `interpreter.json` | Zeppelin — Trino bağlantısı | şablon; parola `trino-service-accounts`'taki `zeppelin` parolası | `manual-30-kurulum-5.10` |
| `lakehouse-ca` | `tls.crt`, `tls.key` | sertifika yöneticisi (Trino'nun iç sertifikasını imzalar); Superset ve Zeppelin | kurulumda üretilir ya da kurumsal ara CA | `manual-30-kurulum-5.11` |
| `jupyterhub-secrets` | `hub.config.JupyterHub.cookie_secret`, `hub.config.CryptKeeper.keys` | JupyterHub — oturum imzası ve kimlik saklama | kurulumda üretilir | `manual-30-kurulum-5.12` |
| `ad-ca` (koşullu) | `ca.crt` | Keycloak, Trino, Zeppelin — AD'ye LDAPS ile bağlanırken | AD ekibinin verdiği kök sertifika | `manual-30-kurulum-5.13` |
| `s3-ca` (koşullu) | `ca.crt`, `truststore.p12` | PostgreSQL yedekleri, Kafka Connect, Spark, Polaris, Trino, not defterleri — S3'e TLS ile bağlanırken | depolama ekibinin verdiği kök sertifika | `manual-30-kurulum-5.14` |

`ad-ca` yalnız AD'nin sertifikası kurumun kendi CA'sından geliyorsa gerekir. `s3-ca`
yalnız S3'ün sertifikası kurumun kendi CA'sından geliyorsa ya da kendinden imzalıysa
gerekir.

**`jupyterhub-secrets` tam olarak iki anahtar içerir.** Hub ile proxy arasındaki jeton bu
Secret'ta değildir: JupyterHub onu kendi `hub` Secret'ına yazar (§3). Buraya üçüncü bir
anahtar eklemeyin.

---

## 2. Sonradan elle yaratılanlar

Bunlar kurulumda yoktur; katalog kurulurken ya da bir kaynak eklenirken yaratılır.

| Secret | Anahtarlar | Kim okur | Kim yaratır | Etiket (`lakehouse.io/yonetim`) |
|---|---|---|---|---|
| `polaris-connect`, `polaris-spark`, `polaris-trino`, `polaris-notebooks` | `credential` | Kafka Connect, Spark, Trino, not defterleri — katalog kimlikleri | `scripts/polaris-setup.sh` ([40-kurulum-sonrasi](../40-kurulum-sonrasi.md) §2.2) | `manual-40-kurulum-sonrasi-2.2` |
| kaynak adı + `-db` (ör. `erp-db`) | `username`, `password` | Debezium bağlayıcısı (adı `dbz-` ile başlar) | elle ([yeni-kaynak-ve-pipeline.md](../50-isletme/yeni-kaynak-ve-pipeline.md) §4) | `manual-yeni-kaynak-ve-pipeline-4` |
| `cloud-credentials` (`openshift-adp` ad alanı) | `cloud` | OADP — ad alanı yedekleri | elle ([yedek-ve-geri-donus.md](../50-isletme/yedek-ve-geri-donus.md) §7) | — |

`polaris-*` Secret'ları silinirse `scripts/polaris-setup.sh` tekrar çalıştırılır; betik
eksik kimlikleri yeniden üretir ve etiketler.

---

## 3. Operatörlerin ürettikleri — elle yaratmayın

Aşağıdakileri bir operatör ya da bir chart, Git'teki bir nesneden üretir. Elle yaratılırsa
çakışır; silinirse yeniden üretilir.

| Secret | Üreten | Ne zaman oluşur |
|---|---|---|
| `connect`, `spark`, `fluentbit`, `kafka-admin`, `stream-` + akış adı | Strimzi — Kafka kullanıcılarının parolaları | Kafka kullanıcıları yaratılırken |
| `lakehouse-cluster-ca-cert`, `lakehouse-clients-ca-cert` ve diğer `lakehouse-*` sertifikaları | Strimzi — Kafka'nın sertifikaları | Kafka açılırken |
| `polaris-db-app`, `keycloak-db-app`, `superset-db-app` ve `-ca`, `-server`, `-replication` eşleri | CloudNativePG | PostgreSQL kümeleri açılırken |
| `trino-tls` | sertifika yöneticisi (`lakehouse-ca` imzalar) | `glue` eşitlemesinden bir dakika kadar sonra |
| `keycloak-lakehouse-realm` | Keycloak operatörü | realm içe aktarılınca |
| `spark-operator-webhook-certs` | spark-operator | operatör açılırken |
| `hub` | JupyterHub chart'ı (`jupyterhub` uygulaması) | ilk JupyterHub eşitlemesinde |

`nesne-kaynagi.sh` bunları `TÜREV` diye gösterir; `hub` ise ArgoCD'nin `jupyterhub`
uygulamasından geldiği için `GIT` görünür.

`kafka-admin`, Kafka'nın **yönetici** kimliğidir: `scripts/kafka-cli.sh` onu okur. Bu
Secret'ı okuyabilen kişi Kafka'da her şeyi yapabilir.

`stream-` ile başlayan Secret'lar bir JSON akışının üretici parolasıdır. Silinirse parola
değişir ve üreticilere yeni parola verilir
([json-akisi-ekleme.md](../50-isletme/json-akisi-ekleme.md) §5).

---

## 4. Doğrulama ve yenileme

### 4.1 Hepsi yerinde mi

`[bastion]`

```bash
for s in connect-push s3-creds backup-s3-creds polaris-root keycloak-admin \
         keycloak-clients trino-service-accounts trino-shared-secret superset-secret \
         zeppelin-shiro zeppelin-interpreter lakehouse-ca jupyterhub-secrets; do
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
```

**Ters giderse:**

- `EKSIK` yazan satır için §1'deki belge adımını tekrarlayın.
- Anahtar adları listeyle aynı değilse Secret yanlış yaratılmıştır. Bileşenler anahtarları
  adıyla arar: Secret'ı silin ve belge adımını baştan uygulayın.

### 4.2 Elle yaratılanların etiketi var mı

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" get secret -L lakehouse.io/yonetim | awk 'NR==1 || $NF ~ /^manual-/'
```

**Beklenen çıktı** (ilk satırlar; sütun genişliği ve `AGE` sizde farklıdır):

```text
NAME                                        TYPE                             DATA   AGE     YONETIM
ad-ca                                       Opaque                           1      3d5h    manual-30-kurulum-5.13
backup-s3-creds                             Opaque                           2      3d5h    manual-30-kurulum-5.3
connect-push                                kubernetes.io/dockerconfigjson   1      3d5h    manual-20-on-kosullar-5
```

§1 ve §2'deki her Secret bu listede olmalıdır. Etiketi eksik olanı belgesindeki etiket
komutuyla etiketleyin.

### 4.3 Bir Secret'ın anahtarlarını görmek

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" get secret polaris-root \
  -o go-template='{{range $k,$v := .data}}{{$k}}{{"\n"}}{{end}}'
```

**Beklenen çıktı:**

```text
clientId
clientSecret
```

Komut değerleri göstermez, yalnız anahtar adlarını gösterir. Bir değeri görmeniz
gerekiyorsa `-o jsonpath='{.data.clientId}' | base64 -d` kullanın ve çıktıyı terminal
geçmişinde bırakmayın.

**Ters giderse:** `NotFound` → Secret hiç yaratılmamıştır (§1).

### 4.4 Yenileme

Bir Secret'ı yenilerken belge adımındaki **etiket komutunu da** çalıştırın; yeniden
yaratılan Secret etiketsiz yaratılır.

| Secret | Nasıl yenilenir | Etkisi |
|---|---|---|
| `s3-creds`, `backup-s3-creds` | Secret'ı silip belge adımıyla yeniden yaratın | Spark işleri bir sonraki koşuda; Kafka Connect ve yedekler pod'ları yeniden başlayınca yeni anahtarı kullanır |
| `keycloak-clients` | Secret'ı güncelleyin, ardından realm'i yeniden içe aktarın ([30-kurulum](../30-kurulum.md) §8) | realm içe aktarımı var olan realm'i kendiliğinden güncellemez |
| `trino-service-accounts` | parolaları yeniden üretip Secret'ı yeniden yaratın; `zeppelin-interpreter`'ı da aynı parolayla yenileyin | parolalar `password.db` ile uyuşmazsa Superset ve Zeppelin Trino'ya bağlanamaz |
| `trino-shared-secret`, `superset-secret` | yeniden yaratıp ilgili pod'ları yeniden başlatın | açık oturumlar düşer |
| `jupyterhub-secrets` | yeniden yaratıp hub'ı yeniden başlatın | kullanıcı oturumları düşer; not defterlerindeki dosyalar etkilenmez |
| JupyterHub hub–proxy jetonu (`hub`) | [kullanici-ve-yetki.md](../50-isletme/kullanici-ve-yetki.md) §9'daki yordam | yalnız eşitlemek yetmez; yordam izlenmezse JupyterHub `403` verir |
| `lakehouse-ca` | yeni CA üretip Secret'ı değiştirin, site değerlerindeki `tls.caBundle`'ı güncelleyip Git'e gönderin | bütün istemcilerin güven deposu yenilenir; planlı kesinti gerektiren tek yenilemedir |

---

## Kontrol listesi

- [ ] §4.1'deki 13 Secret'ın hepsi var ve anahtar adları listeyle aynı.
- [ ] §4.2'de elle yaratılan her Secret'ın etiketi var.
- [ ] `trino-service-accounts` içindeki parolalar `password.db` ile aynı parolalardan
      üretildi.
- [ ] `keycloak-clients` realm içe aktarılmadan **önce** vardı.
- [ ] `install/lakehouse.env` dosyası kurulumdan sonra kasaya alındı ya da silindi.

## Sonraki bölüm

[30-kurulum.md](../30-kurulum.md) — Secret'ları yaratan komutlar. Ayar dosyalarındaki
anahtarlar: [values-anahtarlari.md](values-anahtarlari.md).
