# 90 — Değer anahtarları (site dosyaları)

**Bu bölümde:** müşterinin düzenlediği **tek dizin** olan `platform/values/site/` altındaki
üç dosyanın bütün anahtarları — anlamı, `install/lakehouse.env` karşılığı, örnek
değeri ve zorunlu olup olmadığı. Ayrıca `platform/polaris/setup.yaml` dosyasında kurulumcunun
değiştirdiği satırlar ve aynı değerin birden fazla dosyada tekrarlandığı yerler.
**Süre:** okuma 15 dakika.
**Gereken yetki:** yok (dosya düzenleme); değişikliği uygulamak için Git deposuna yazma.
**Nerede çalıştırılır:** `[bastion]` — dosyalar depo kopyanızda düzenlenir, Git'e itilir,
ArgoCD uygular.

**Ürün varsayılanlarına dokunulmaz.** `platform/values/glue.yaml`,
`platform/values/trino.yaml`, `platform/values/jupyterhub.yaml`,
`platform/values/polaris.yaml` ve `glue/values.yaml` ürünle gelir ve her yükseltmede
değişebilir. Müşteriye özel her değer yalnız `platform/values/site/` altındadır; ArgoCD
site dosyasını ürün dosyasından **sonra** yükler, bu yüzden site değeri kazanır.

Dosyaların doldurulması: [30-kurulum](../30-kurulum.md) §4. Tutarlılık denetimi:
`scripts/check-site.sh`.

---

## 1. `platform/values/site/glue.yaml`

Ürünün ana chart'ının (Kafka, Connect, Keycloak, Superset, Zeppelin, CNPG, yedek, TLS)
müşteriye özel değerleri. **20 anahtar.**

| Anahtar | Anlam | `.env` karşılığı | Örnek | Zorunlu mu |
|---|---|---|---|---|
| `appsDomain` | OpenShift uygulama alan adı; bütün Route adresleri bundan türetilir (bileşen adı + `-` + ad alanı + `.` + bu değer) | `$APPS_DOMAIN` | `apps.ocp.example.net` | **Evet** |
| `s3.endpoint` | Iceberg verisinin yazıldığı S3 uç noktası; istemciler (Iceberg sink, Spark) bunu kullanır. `https://` ile başlamak **zorundadır** | `$S3_ENDPOINT` | `https://s3.example.com` | **Evet** |
| `s3.region` | S3 bölge adı; S3 uyumlu depolamalarda çoğu zaman anlamsızdır ama istemciler zorunlu tutar | `$S3_REGION` | `us-east-1` | Hayır (varsayılan `us-east-1`) |
| `s3.vendedCredentials` | Polaris'in istemcilere geçici S3 kimliği dağıtması. Depolamada STS yoksa `false` yapın; o zaman istemciler `s3-creds` Secret'ını kullanır | — (depolama ekibine sorulur) | `true` | Hayır (varsayılan `true`) |
| `connect.buildImage` | Kafka Connect imajının kümede üretilip itileceği tam adres. `:latest` **kullanılamaz** (`scripts/check-site.sh` reddeder) | `$INTERNAL_REGISTRY` + `$LAKEHOUSE_NS` | `image-registry.openshift-image-registry.svc:5000/lakehouse/connect:2.0.0` | **Evet** |
| `connect.buildPushSecret` | İtme yetkisi olan `docker-registry` tipli Secret'ın adı | — (sabit) | `connect-push` | **Evet** |
| `keycloak.ldap.enabled` | Keycloak'ın Active Directory federasyonu. Gün-1'den **açık** olmalıdır | — (sabit) | `true` | **Evet** (`true`) |
| `keycloak.ldap.connectionUrl` | AD sunucusunun LDAPS adresi. Şifresiz LDAP kabul edilmez | `$LDAP_URL` | `ldaps://ad.example.com:636` | **Evet** |
| `keycloak.ldap.usersDn` | Kullanıcı nesnelerinin arandığı alt ağaç | `$LDAP_USERS_DN` | `OU=Users,DC=example,DC=com` | **Evet** |
| `keycloak.ldap.groupsDn` | Üç lakehouse grubunun bulunduğu alt ağaç | `$LDAP_GROUPS_DN` | `OU=Groups,DC=example,DC=com` | **Evet** |
| `keycloak.ldap.bindDn` | Yalnız okuma yetkili AD servis hesabının tam DN'i. **Parola burada değildir:** `keycloak-clients` Secret'ının `ldap-bind` anahtarındadır | `$LDAP_BIND_DN` | `CN=svc-lakehouse,OU=Service,DC=example,DC=com` | **Evet** |
| `cnpg.polarisDb.storageClass` | Polaris veritabanının diski. Boş bırakılırsa kümenin varsayılan StorageClass'ı kullanılır | `$STORAGE_CLASS` | `ocs-storagecluster-ceph-rbd` | Hayır |
| `cnpg.keycloakDb.storageClass` | Keycloak veritabanının diski | `$STORAGE_CLASS` | `ocs-storagecluster-ceph-rbd` | Hayır |
| `cnpg.supersetDb.storageClass` | Superset veritabanının diski | `$STORAGE_CLASS` | `ocs-storagecluster-ceph-rbd` | Hayır |
| `backup.s3.endpoint` | PostgreSQL yedeklerinin yazıldığı S3 uç noktası | `$S3_ENDPOINT` | `https://s3.example.com` | **Evet** |
| `backup.s3.bucket` | Yedek bucket'ı. Veri bucket'ından **AYRI** olmalıdır: geri yükleme hedefi ile kaynağı aynı olamaz | `$S3_BUCKET_BACKUP` | `lakehouse-backups` | **Evet** |
| `backup.s3.region` | Yedek bucket'ının bölgesi | `$S3_REGION` | `us-east-1` | Hayır (varsayılan `us-east-1`) |
| `tls.caBundle` | `lakehouse-ca` Secret'ındaki `tls.crt` dosyasının PEM içeriği. Doluysa Trino Route'u `reencrypt` olur (`destinationCACertificate`), boşsa `passthrough` | — (kurulumda üretilen CA) | `"-----BEGIN CERTIFICATE-----\n…"` | Hayır (önerilir) |
| `sources` | Kaynak veritabanları listesi. Kurulumda **boş** kalır; ilk kaynak gün-2'de eklenir | — | `[]` | Kurulumda boş |
| `pipelines` | Silver birleştirme tanımları. Kurulumda **boş** kalır | — | `[]` | Kurulumda boş |

### Bu dosyaya eklenebilen isteğe bağlı anahtarlar

Şablonda yorum satırı olarak durur ya da hiç yoktur; gerekmedikçe eklemeyin.

| Anahtar | Ne zaman eklenir | Örnek |
|---|---|---|
| `namespace` | Ürün, `$LAKEHOUSE_NS` olarak `lakehouse` dışında bir ad alanına kurulacaksa **zorunlu** | `namespace: veri-golu` |
| `trino.hostname`, `superset.hostname`, `jupyterhub.hostname`, `zeppelin.hostname` | Bir bileşen için kurumsal DNS adı isteniyorsa (o ad için CNAME kaydı kümenin joker adına yönlendirilir) | `trino: {hostname: trino.kurum.example.net}` |
| `keycloak.hostname` | Keycloak için kurumsal ad. **Çıplak host değil, TAM URL** yazılır; jetondaki `iss` değeri budur ve şemasız yazılırsa render hata verir | `keycloak: {hostname: https://sso.kurum.example.net}` |
| `kafka.externalListener` ve `nginx.enabled` | nginx erişim günlüğü akışı kullanılacaksa (ikisi birlikte) | `true` |
| `velero` | OADP operatörü kurulduktan **sonra** ad alanı yedeği açılacaksa | `velero: {enabled: true, namespace: openshift-adp}` |
| `spark` | Veri hacmi büyüdüğünde büyük kademe kaynakları (ürün dosyasında yorumlu blok) | `spark: {shufflePartitions: 200}` |
| `kafka.storageClass`, `kafka.storageSize` | Kafka diski varsayılandan (100 GiB) farklı olacaksa | `kafka: {storageSize: 500Gi}` |

---

## 2. `platform/values/site/trino.yaml`

Trino chart'ı ayrı bir ArgoCD Application'dır ve glue değerlerini **göremez**; bu yüzden
Keycloak, LDAP ve S3 değerleri burada tekrarlanır. **3 anahtar** — ama her biri çok satırlı
bir metin bloğudur.

> **Blokların tamamı buradadır, çünkü Helm çok satırlı metin bloklarını birleştirmez:**
> son yüklenen dosyanınki bütünüyle geçer. Bu yüzden bloklar ürün satırlarını da tekrar
> eder. Ürün satırlarının eksilmediğini `scripts/check-site.sh` denetler.
> **Blokların içine satır sonu yorumu yazmayın:** bloklar Java `.properties` dosyası
> olarak render edilir, `anahtar=değer   # not` yazarsanız not değerin parçası olur.

| Anahtar | Anlam | Zorunlu mu |
|---|---|---|
| `server.coordinatorExtraConfig` | Yalnız coordinator'a giden OAuth2 (Keycloak) ayarları | **Evet** |
| `coordinator.additionalConfigFiles.group-provider.properties` | Trino'nun grupları doğrudan AD'den okuması (LDAP group provider) | **Evet** |
| `catalogs.lakehouse` | `lakehouse` Iceberg kataloğunun tanımı (Polaris REST katalog + S3) | **Evet** |

### `server.coordinatorExtraConfig` içindeki doldurulan satır

| Satır | Anlam | `.env` karşılığı | Örnek |
|---|---|---|---|
| `http-server.authentication.oauth2.issuer` | Keycloak realm'inin tam adresi; jetondaki `iss` ile **birebir** aynı olmalıdır | `$APPS_DOMAIN` + `$LAKEHOUSE_NS` | `https://keycloak-lakehouse.apps.ocp.example.net/realms/lakehouse` |

Aynı bloktaki `oauth2.client-id=trino`, `oauth2.client-secret=${ENV:OIDC_CLIENT_SECRET}`,
`oauth2.principal-field=preferred_username`, `oauth2.scopes=openid` ve
`web-ui.authentication.type=oauth2` satırları **ürün satırlarıdır**; silmeyin.

### `group-provider.properties` içindeki doldurulan satırlar

| Satır | Anlam | `.env` karşılığı | Örnek |
|---|---|---|---|
| `ldap.url` | AD LDAPS adresi (glue dosyasındaki `keycloak.ldap.connectionUrl` ile **aynı**) | `$LDAP_URL` | `ldaps://ad.example.com:636` |
| `ldap.admin-user` | Bağlanma DN'i (glue `keycloak.ldap.bindDn` ile **aynı**) | `$LDAP_BIND_DN` | `CN=svc-lakehouse,OU=Service,DC=example,DC=com` |
| `ldap.user-base-dn` | Kullanıcı ağacı (glue `keycloak.ldap.usersDn` ile **aynı**) | `$LDAP_USERS_DN` | `OU=Users,DC=example,DC=com` |

Bağlanma **parolası** değer dosyasında değildir: `keycloak-clients` Secret'ının
`ldap-bind` anahtarı `LDAP_BIND_PASSWORD` ortam değişkeni olarak gelir
(`ldap.admin-password=${ENV:LDAP_BIND_PASSWORD}`). Kalan satırlar
(`group-provider.name=ldap`, `ldap.user-search-filter=(sAMAccountName={0})`,
`ldap.user-member-of-attribute=memberOf`, `ldap.group-name-attribute=cn`) ürün
satırlarıdır. AD şemanız `sAMAccountName` yerine başka bir nitelik kullanıyorsa arama
süzgecini burada değiştirin.

### `catalogs.lakehouse` içindeki doldurulan satırlar

| Satır | Anlam | `.env` karşılığı | Örnek |
|---|---|---|---|
| `iceberg.rest-catalog.uri` | Polaris'in küme içi adresi; ad alanı adını içerir | `$LAKEHOUSE_NS` | `http://polaris.lakehouse.svc:8181/api/catalog` |
| `s3.endpoint` | Veri S3'ü (glue `s3.endpoint` ile **aynı**) | `$S3_ENDPOINT` | `https://s3.example.com` |
| `s3.region` | Veri S3 bölgesi (glue `s3.region` ile **aynı**) | `$S3_REGION` | `us-east-1` |
| `iceberg.rest-catalog.vended-credentials-enabled` | glue `s3.vendedCredentials` ile **aynı** olmalıdır | — | `true` |

Diğer satırlar (`connector.name=iceberg`, `iceberg.catalog.type=rest`,
`iceberg.rest-catalog.warehouse=lakehouse`, `iceberg.rest-catalog.security=OAUTH2`,
`iceberg.rest-catalog.oauth2.credential=${ENV:POLARIS_CREDENTIAL}`,
`iceberg.rest-catalog.oauth2.scope=PRINCIPAL_ROLE:ALL`, `fs.s3.enabled=true`,
`s3.path-style-access=true`) ürün satırlarıdır.

> **`platform/values/trino-ldap.yaml` bilerek boştur.** Yalnız yorum içerir ve ArgoCD'nin
> değer dosyası listesinde durur (geliştirme kümesi bu listeyi başka bir dosyayla
> değiştirir). **Dokunmayın, silmeyin.**

---

## 3. `platform/values/site/jupyterhub.yaml`

JupyterHub da ayrı bir Application'dır; aşağıdaki beş değer glue dosyasındaki
değerlerden **türetilmiş kopyalardır**. `scripts/check-site.sh` eşitliği denetler.
**5 anahtar.**

| Anahtar | Anlam | Nereden türetilir | Örnek | Zorunlu mu |
|---|---|---|---|---|
| `hub.config.GenericOAuthenticator.oauth_callback_url` | Keycloak'ın kullanıcıyı geri göndereceği adres | `jupyterhub-` + `$LAKEHOUSE_NS` + `.` + `$APPS_DOMAIN` | `https://jupyterhub-lakehouse.apps.ocp.example.net/hub/oauth_callback` | **Evet** |
| `hub.config.GenericOAuthenticator.authorize_url` | Keycloak yetkilendirme uç noktası | Keycloak URL'i + `/realms/lakehouse/protocol/openid-connect/auth` | `https://keycloak-lakehouse.apps.ocp.example.net/realms/lakehouse/protocol/openid-connect/auth` | **Evet** |
| `hub.config.GenericOAuthenticator.token_url` | Jeton uç noktası | aynı kök + `/token` | `https://keycloak-lakehouse.apps.ocp.example.net/realms/lakehouse/protocol/openid-connect/token` | **Evet** |
| `hub.config.GenericOAuthenticator.userdata_url` | Kullanıcı bilgisi uç noktası | aynı kök + `/userinfo` | `https://keycloak-lakehouse.apps.ocp.example.net/realms/lakehouse/protocol/openid-connect/userinfo` | **Evet** |
| `singleuser.extraEnv.S3_ENDPOINT` | Not defterlerinin göreceği S3 adresi (glue `s3.endpoint` ile **aynı**) | `$S3_ENDPOINT` | `https://s3.example.com` | **Evet** |

---

## 4. `platform/polaris/setup.yaml` — kurulumcunun değiştirdiği satırlar

Bu dosya Polaris **sunucusunun** katalog tanımıdır ve `scripts/polaris-setup.sh`
tarafından uygulanır; site dizininde değildir ama kurulumda düzenlenir.

| Satır | Anlam | `.env` karşılığı | Örnek |
|---|---|---|---|
| `endpoint` | Polaris sunucusunun S3'e bağlanırken kullandığı adres | `$S3_ENDPOINT` | `https://s3.example.com` |
| `endpoint_internal` | Aynı adresin küme içi karşılığı; ayrı bir iç adres yoksa `endpoint` ile aynı yazılır | `$S3_ENDPOINT` | `https://s3.example.com` |
| `default_base_location` | Kataloğun kök yolu; veri bucket'ının adını içerir | `$S3_BUCKET_DATA` | `s3://lakehouse/` |
| `allowed_locations` | Kataloğun yazmasına izin verilen yollar; `default_base_location` ile aynı bucket | `$S3_BUCKET_DATA` | `[s3://lakehouse/]` |
| `region` | Bucket'ın bölgesi | `$S3_REGION` | `us-east-1` |
| `sts_unavailable` | Depolamada STS **yoksa** eklenir; glue `s3.vendedCredentials: false` ile birlikte kullanılır | — | `true` |

`scripts/check-site.sh` bu dosyadaki `endpoint` uyuşmazlığını **HATA değil UYARI** olarak
raporlar (geliştirme kümesinde bilerek küme içi MinIO adresini taşır). Uyarıyı görüp
geçmeyin: üretimde bu satırlar doldurulmazsa Polaris kataloğu yanlış depolamayı gösterir.

**Katalog `properties` yalnız katalog YARATILIRKEN yazılır.** `polaris setup apply` var
olan bir katalogda `properties` bloğunu güncellemez; sonradan değiştirmek için
`polaris catalogs update --set-property ...` gerekir.

---

## 5. Aynı değerin birden fazla yerde olduğu noktalar

ArgoCD çok kaynaklı Application'larda değer dosyaları arasında şablonlama yoktur;
aşağıdaki kopyalar elle tutulur ve `scripts/check-site.sh` ile denetlenir. Birini
değiştirirken **hepsini** değiştirin.

| Tek doğru kaynak | Kopyası nerede | Denetleyen |
|---|---|---|
| `platform/values/site/glue.yaml` → `appsDomain` | `platform/values/site/trino.yaml` oauth2 issuer; `platform/values/site/jupyterhub.yaml` dört URL | `scripts/check-site.sh` |
| `platform/values/site/glue.yaml` → `s3.endpoint` | `platform/values/site/trino.yaml` `s3.endpoint`; `platform/values/site/jupyterhub.yaml` `S3_ENDPOINT`; `platform/polaris/setup.yaml` `endpoint` | `scripts/check-site.sh` (Polaris için uyarı) |
| `platform/values/site/glue.yaml` → `s3.region` | `platform/values/site/trino.yaml` `s3.region`; `platform/polaris/setup.yaml` `region` | `scripts/check-site.sh` |
| `platform/values/site/glue.yaml` → `s3.vendedCredentials` | `platform/values/site/trino.yaml` `iceberg.rest-catalog.vended-credentials-enabled` | `scripts/check-site.sh` |
| `platform/values/site/glue.yaml` → `keycloak.ldap.connectionUrl` / `bindDn` / `usersDn` | `platform/values/site/trino.yaml` `ldap.url` / `ldap.admin-user` / `ldap.user-base-dn`; `zeppelin-shiro` Secret'ındaki Shiro dosyası | `scripts/check-site.sh` (Shiro hariç) |
| `$LAKEHOUSE_NS` | `platform/values/site/trino.yaml` Polaris adresi; türetilen bütün host adları | `scripts/check-site.sh` |

Trino, Superset ve JupyterHub'ın Keycloak'taki yönlendirme adresleri host adlarından
**türetilir**, elle girilmez (`glue/templates/keycloak-realm.yaml`). Ek bir adres
gerekiyorsa `keycloak.extraRedirectUris` listesine eklenir.

> **Polaris'in Route ile dışarı açıldığı kurulum desteklenmez.**
> `scripts/check-site.sh` Polaris'in küme içi adresini (`http://polaris.` + ad alanı +
> `.svc:8181/api/catalog`) varsayar.

---

## Kontrol listesi

- [ ] `platform/values/site/` altındaki üç dosyada `example.com` / `example.net` kalıntısı
      yok.
- [ ] `scripts/check-site.sh` hatasız bitiyor ve Polaris uyarısı da giderilmiş.
- [ ] Zorunlu işaretli her anahtar dolu; isteğe bağlı olanlardan yalnız gerekenler
      eklenmiş.
- [ ] `platform/values/` altındaki ürün dosyalarına dokunulmadı (`git diff` yalnız
      `site/` ve `platform/polaris/setup.yaml` gösteriyor).
- [ ] Değişiklikler Git'e itildi; ArgoCD uygulamaları `Synced`.

## Sonraki bölüm

[30-kurulum.md](../30-kurulum.md) §4 — bu dosyaların komutla doldurulması ve Git'e
itilmesi. Secret'lar için [secret-listesi.md](secret-listesi.md).
