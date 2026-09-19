# 90 — Secret listesi

**Bu bölümde:** kurulumda kümede bulunması gereken **bütün** Kubernetes
[Secret](../00-genel-bakis.md#4-kavramlar-sözlüğü) nesneleri: adı, içindeki anahtarlar,
hangi bileşenin okuduğu, değerin nereden geldiği ve doğrulama komutu. Yaratma komutları
bu sayfada değil, [30-kurulum](../30-kurulum.md) §5'tedir; bu sayfa başvuru tablosudur.
**Süre:** okuma 10 dakika.
**Gereken yetki:** `$LAKEHOUSE_NS` ad alanında yönetici (Secret okuma/yazma).
**Nerede çalıştırılır:** `[bastion]`.

Secret'lar **Git'e girmez**. Depoda yalnız *adları* ve hangi anahtarı taşıdıkları
yazılıdır
(`glue/values.yaml`, `platform/values/trino.yaml`, `platform/values/jupyterhub.yaml`);
değerler kurulum sırasında komutla kümeye yazılır.

> **Geliştirme kümesinde (kind) bu Secret'ların çoğu sahte olarak üretilir.**
> `components.devSecrets: true` iken glue chart'ı `glue/templates/dev-secrets.yaml`
> şablonuyla sentetik değerler yaratır. Üretimde bu anahtar `false`'tur ve chart hiçbir
> şey üretmez: eksik Secret, pod'u `CreateContainerConfigError` durumunda bekletir.

---

## 1. Kurulumdan önce elle yaratılanlar

Sıra [30-kurulum](../30-kurulum.md) §5'teki adım sırasıdır. Hepsi `$LAKEHOUSE_NS` ad
alanındadır.

| # | Secret | Anahtarlar | Kim okur | Değer nereden | Kurulum adımı |
|---|---|---|---|---|---|
| 1 | `connect-push` | `.dockerconfigjson` | Strimzi Kafka Connect build'i (`connect.buildPushSecret`) — imajı iç registry'ye iter | iç registry servis hesabı jetonu | [20-on-kosullar](../20-on-kosullar.md) madde 5'te yaratıldı; [30-kurulum](../30-kurulum.md) §5.1'de yalnız **doğrulanır** |
| 2 | `s3-creds` | `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` | Iceberg sink (Kafka Connect) ve Spark işleri | `$S3_ACCESS_KEY` / `$S3_SECRET_KEY` (depolama ekibi) | [30-kurulum](../30-kurulum.md) §5.2 |
| 3 | `backup-s3-creds` | `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` | CloudNativePG Barman Cloud eklentisi (`backup.s3.secret`) — PostgreSQL yedekleri | `$S3_BACKUP_ACCESS_KEY` / `$S3_BACKUP_SECRET_KEY` (**ayrı** anahtar çifti) | [30-kurulum](../30-kurulum.md) §5.3 |
| 4 | `polaris-root` | `clientId`, `clientSecret` | Polaris bootstrap Job'ı ve `scripts/polaris-setup.sh` (`polaris.rootCredentialSecret`) | kurulumda üretilir (`openssl rand -hex 24`) | [30-kurulum](../30-kurulum.md) §5.4 |
| 5 | `keycloak-admin` | `username`, `password` | Keycloak CR `bootstrapAdmin` (`keycloak.adminSecret`) | `$KEYCLOAK_ADMIN_PASSWORD`; boşsa üretilir | [30-kurulum](../30-kurulum.md) §5.5 |
| 6 | `keycloak-clients` | `trino`, `superset`, `jupyterhub`, `ldap-bind` | KeycloakRealmImport `spec.placeholders`; Trino `OIDC_CLIENT_SECRET` ve `LDAP_BIND_PASSWORD`; Superset `OIDC_CLIENT_SECRET`; JupyterHub `OAUTH_CLIENT_SECRET` | üç client sırrı üretilir; `ldap-bind` = `$LDAP_BIND_PASSWORD` (AD ekibi) | [30-kurulum](../30-kurulum.md) §5.6 |
| 7 | `trino-service-accounts` | `password.db`, `superset`, `zeppelin` | Trino `auth.passwordAuthSecret` (htpasswd dosyası); Superset `TRINO_PASSWORD`; Zeppelin interpreter tohumu | kurulumda üretilir; `password.db` = `htpasswd -nbBC 10` çıktısı, düz kopyalar **aynı** parolalar | [30-kurulum](../30-kurulum.md) §5.7 |
| 8 | `trino-shared-secret` | `secret` | Trino `internal-communication.shared-secret` (coordinator ↔ worker) | üretilir (`openssl rand -hex 32`) | [30-kurulum](../30-kurulum.md) §5.8 |
| 9 | `superset-secret` | `secret-key` | Superset `spec.secretKeyFrom` → `SECRET_KEY` (oturum çerezi imzası) | üretilir (`openssl rand -hex 32`) | [30-kurulum](../30-kurulum.md) §5.8 |
| 10 | `zeppelin-shiro` | `shiro.ini` | Zeppelin (konteyner içinde Shiro yapılandırması) — AD (LDAPS) girişi ve rol kapısı | `examples/zeppelin/shiro-ad.ini` şablonu AD değerleriyle doldurulur | [30-kurulum](../30-kurulum.md) §5.9 |
| 11 | `zeppelin-interpreter` | `interpreter.json` | Zeppelin initContainer'ı (PVC `/data/conf` tohumu) — Trino JDBC bağlantısı | `glue/files/zeppelin/interpreter.json` şablonu; parola §5.7'deki `zeppelin` parolası | [30-kurulum](../30-kurulum.md) §5.10 |
| 12 | `lakehouse-ca` | `tls.crt`, `tls.key` | cert-manager `Issuer/lakehouse-ca` (Trino sunucu sertifikasını imzalar); Superset ve Zeppelin pod'larında güven kökü | kurulumda üretilir (`openssl req -x509`) ya da kurumsal ara CA | [30-kurulum](../30-kurulum.md) §5.11 |
| 13 | `jupyterhub-secrets` | `hub.config.JupyterHub.cookie_secret`, `hub.config.CryptKeeper.keys` | JupyterHub `hub.existingSecret` — oturum çerezi imzası ve `auth_state` şifrelemesi | üretilir (her biri `openssl rand -hex 32`) | [30-kurulum](../30-kurulum.md) §5.12 |
| 14 | `ad-ca` | `ca.crt` | **Keycloak** (CR `spec.truststores` → `/opt/keycloak/conf/truststores`), **Trino coordinator** (`ldap.ssl.truststore.path=/etc/trino/ad-ca/ca.crt`), **Zeppelin** (`ad-truststore` initContainer'ı cacerts kopyasına ekler) — üçü de AD'ye LDAPS ile bağlanır | `$LDAP_CA_FILE` (AD ekibi) | [30-kurulum](../30-kurulum.md) §5.13 (koşullu: AD sertifikası özel bir kökten geliyorsa **zorunlu**) |

**`jupyterhub-secrets` tam olarak iki anahtar içerir.** Hub ile proxy arasındaki
`ConfigurableHTTPProxy.auth_token` **bu Secret'ta değildir** ve elle yaratılmaz: z2jh
chart'ı onu kendi ürettiği `hub` Secret'ına yazar, hem hub hem proxy pod'u oradan okur.
ArgoCD `ignoreDifferences` (`platform/apps/30-jupyterhub.yaml`) değeri re-sync'te dondurur.
Buraya üçüncü bir anahtar koymak **ölü anahtar** olur ve yanıltıcı bir rotasyon yolu
gösterir. Rotasyon yordamı §4'tedir.

---

## 2. Gün-2'de eklenenler

Bunlar kurulumda **yoktur**; ilk kaynak veritabanı bağlanırken ya da yedekleme açılırken
yaratılır.

| Secret | Anahtarlar | Kim okur | Değer nereden | Kılavuz |
|---|---|---|---|---|
| kaynak adı + `-db` (örnek: `shop-db`, `crm-db`) | `username`, `password` | Debezium bağlayıcısı (adı `dbz-` ile başlayan KafkaConnector) | kaynak veritabanı yöneticisi (CDC yetkili hesap) | docs/50-isletme/yeni-kaynak-ve-pipeline.md (bu bölüm bir sonraki görevde eklenir) |
| OADP bulut kimliği (`cloud-credentials`, ad alanı `openshift-adp`) | `cloud` | Velero/OADP | yedek S3 anahtar çifti | docs/50-isletme/yedek-ve-geri-donus.md (bu bölüm bir sonraki görevde eklenir) |

> Geliştirme kümesinde görülen `velero-s3-creds` Secret'ı **yalnız kind içindir**
> (`platform/values/velero-dev.yaml`); üretimde Velero chart'ı kurulmaz, yerine OADP
> operatörü ve onun kendi kimlik Secret'ı kullanılır.

---

## 3. Ürünün kendi ürettikleri — elle yaratmayın

Aşağıdakiler operatörler ya da betikler tarafından yazılır. Kurulumdan önce elle
yaratılırsa çakışır; kurulumun başında eksik görünmeleri bir hata değildir.

| Secret | Üreten | Ne zaman oluşur |
|---|---|---|
| `polaris-connect`, `polaris-spark`, `polaris-trino`, `polaris-notebooks` | `scripts/polaris-setup.sh` | Polaris kataloğu kurulurken (docs/40-kurulum-sonrasi.md — bu bölüm bir sonraki görevde eklenir) |
| `polaris-db-app`, `keycloak-db-app`, `superset-db-app` ve bunların `-ca` / `-server` / `-replication` eşleri | CloudNativePG | PostgreSQL kümeleri açılırken |
| `lakehouse-cluster-ca-cert`, `lakehouse-clients-ca-cert`, `connect`, `fluentbit` | Strimzi (Kafka CA'ları ve KafkaUser'lar) | Kafka ve Connect ayağa kalkarken |
| `trino-tls` | cert-manager (`Issuer/lakehouse-ca` imzalar) | glue sync'inden sonra ~1 dakika |
| `hub` | JupyterHub (z2jh) chart'ı | ilk JupyterHub sync'inde |
| `keycloak-lakehouse-realm` | Keycloak operatörü | realm içe aktarımı tamamlanınca |

**Polaris principal Secret'ları kurulumun sırasını belirler:** `trino` Application'ı
`polaris-trino` Secret'ı yazılana kadar Healthy olamaz (pod
`CreateContainerConfigError`'da bekler). Bu **normaldir** ve Polaris kataloğu
kurulduğunda kendiliğinden düzelir.

---

## 4. Doğrulama ve rotasyon

### Hepsi yerinde mi

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

**Beklenen çıktı** (kind kümesinden alınmış gerçek çıktı; üretimde `connect-push` de
dolu olur ve `trino-service-accounts` içinde `e2e` **bulunmaz** — o hesap yalnız
geliştirme kümesindedir):

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

**Ters giderse:** `EKSIK` yazan her satır için 30-kurulum §5'teki ilgili adımı
tekrarlayın. Anahtar adı listede olmayan bir Secret yanlış oluşturulmuştur: silip
(`oc -n "$LAKEHOUSE_NS" delete secret ADI`) komutu baştan çalıştırın — anahtar adları
sözleşmedir, bileşenler onları harfi harfine arar.

### Bir Secret'ın anahtarlarını tek tek görmek

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" get secret polaris-root \
  -o go-template='{{range $k,$v := .data}}{{$k}}{{"\n"}}{{end}}'
```

**Beklenen çıktı** (kind kümesinden alınmış gerçek çıktı):

```text
clientId
clientSecret
```

**Ters giderse:** `NotFound` → Secret hiç yaratılmamıştır. Komut değerleri **basmaz**;
bir değeri görmeniz gerekiyorsa `-o jsonpath='{.data.clientId}' | base64 -d` ekleyin ve
çıktıyı terminal geçmişinde bırakmayın.

### Rotasyon

| Secret | Nasıl döndürülür | Etkisi |
|---|---|---|
| `s3-creds`, `backup-s3-creds` | Secret'ı silip yeniden yaratın, sonra `oc -n "$LAKEHOUSE_NS" rollout restart deploy/connect-connect` | Connect ve Spark işleri yeni anahtarı alır |
| `keycloak-clients` | Secret'ı güncelleyin, ardından realm'i yeniden içe aktarın ([30-kurulum](../30-kurulum.md) "Realm içeriğini sonradan değiştirmek") | client sırları realm'e placeholder olarak girer; realm içe aktarımı mevcut realm'i **güncellemez** |
| `trino-service-accounts` | parolaları yeniden üretip Secret'ı yeniden yaratın; `zeppelin-interpreter`'ı da aynı parolayla yenileyin | Superset ve Zeppelin bağlantıları koparsa parolalar `password.db` ile uyuşmuyordur |
| `trino-shared-secret`, `superset-secret` | yeniden yaratıp ilgili pod'ları döndürün | açık oturumlar düşer |
| `jupyterhub-secrets` | yeniden yaratıp hub'ı döndürün | kullanıcı oturumları düşer, not defterlerindeki dosyalar etkilenmez |
| JupyterHub hub ↔ proxy token'ı (`hub` Secret'ı) | `oc -n "$LAKEHOUSE_NS" delete secret hub` → jupyterhub Application'a `argocd.argoproj.io/refresh=hard` annotation'ı → sync | **Yalnız sync yetmez:** ArgoCD önbellekteki eski değeri geri yazar, hub ile proxy farklı token'da kalır ve `HTTP 403` verir |
| `lakehouse-ca` | yeni CA üretip Secret'ı değiştirin, `tls.caBundle` değerini güncelleyip Git'e itin | bütün istemcilerin güven deposu yenilenmelidir; planlı kesinti isteyen tek rotasyon budur |

---

## Kontrol listesi

- [ ] Yukarıdaki 13 zorunlu Secret'ın hepsi `oc get secret` çıktısında ve anahtar adları
      listeyle birebir aynı.
- [ ] `trino-service-accounts` içindeki düz parolalar `password.db` içindeki bcrypt
      satırlarıyla **aynı** parolalardan üretildi.
- [ ] `keycloak-clients` Secret'ı realm içe aktarılmadan **önce** vardı.
- [ ] Üretim kümesinde `components.devSecrets` açılmadı (site değerlerinde bu anahtar hiç
      yoktur; ürün varsayılanı `false`).
- [ ] `install/lakehouse.env` dosyası kurulumdan sonra kasaya alındı ya da silindi.

## Sonraki bölüm

[30-kurulum.md](../30-kurulum.md) — Secret'ların yaratma komutları ve kurulumun tamamı.
Değer dosyalarındaki anahtarlar için
[values-anahtarlari.md](values-anahtarlari.md).
