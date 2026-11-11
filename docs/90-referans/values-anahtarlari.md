# 90 — Değer anahtarları (site dosyaları)

Bu sayfa kuruma özel ayarların tutulduğu `platform/values/site/` klasöründeki üç
dosyanın bütün ayarlarını listeler: ne işe yaradığını, `install/lakehouse.env`
dosyasındaki karşılığını, örnek değerini ve zorunlu olup olmadığını. Ayrıca kurulumda
`platform/polaris/setup.yaml` dosyasında değiştirilen satırları ve aynı değerin birden
fazla dosyada geçtiği yerleri gösterir.

**Ne kadar sürer:** okuma 15 dakika.

**Hangi yetkiler gerekir:** dosyayı değiştirmek için kurumun Git deposuna yazma.

**Komutlar nerede çalışır:** `[bastion]` — dosyalar depo kopyanızda düzenlenir, Git'e
gönderilir, ArgoCD uygular
([değişiklik nasıl uygulanır](../50-isletme/degisiklik-nasil-uygulanir.md)).

**Ürün varsayılanlarına dokunmayın.** `platform/values/` klasöründeki diğer dosyalar ve
`glue/values.yaml` ürünle gelir; her yükseltmede değişebilir. Kuruma özel her değer
yalnız `platform/values/site/` altındadır. ArgoCD site dosyasını ürün dosyasından
**sonra** okur, bu yüzden site dosyasındaki değer geçerli olur.

Dosyaların ilk doldurulması: [30-kurulum](../30-kurulum.md) §4. Denetim:
`bash scripts/check-site.sh`.

---

## 1. `platform/values/site/glue.yaml`

Ürünün ana bileşenlerinin (Kafka, Connect, Keycloak, Superset, Zeppelin, PostgreSQL,
yedek, TLS) kuruma özel değerleri. Kurulumda doldurulan **22 anahtar** aşağıdadır.

| Anahtar | Anlam | `.env` karşılığı | Örnek | Zorunlu mu |
|---|---|---|---|---|
| `appsDomain` | OpenShift uygulama alan adı; bütün Route adresleri bundan türetilir (bileşen adı + `-` + ad alanı + `.` + bu değer) | `$APPS_DOMAIN` | `apps.ocp.example.net` | **Evet** |
| `argocdNamespace` | ArgoCD'nin koştuğu ad alanı; `allow-platform-namespaces` NetworkPolicy'sinin ArgoCD seçicisi bundan üretilir. OpenShift GitOps `openshift-gitops` kullanır. Değer `$ARGOCD_NS` ile aynı değilse seçici hiçbir ad alanıyla eşleşmez. `scripts/check-site.sh` bu değeri kurulumun `$ARGOCD_NS`'i ile çapraz denetler | `$ARGOCD_NS` | `openshift-gitops` | **Evet** (OpenShift'te) |
| `s3.endpoint` | Iceberg verisinin yazıldığı S3 uç noktası; istemciler (Iceberg sink, Spark) bunu kullanır. `https://` ile başlamak **zorundadır** | `$S3_ENDPOINT` | `https://s3.example.com` | **Evet** |
| `s3.region` | S3 bölge adı; S3 uyumlu depolamalarda çoğu zaman anlamsızdır ama istemciler zorunlu tutar | `$S3_REGION` | `us-east-1` | Hayır (varsayılan `us-east-1`) |
| `s3.vendedCredentials` | Polaris'in istemcilere geçici S3 kimliği dağıtması. Depolamada STS yoksa `false` yapın; o zaman istemciler `s3-creds` Secret'ını kullanır | — (depolama ekibine sorulur) | `true` | Hayır (varsayılan `true`) |
| `s3.caSecret` | S3 sertifikası kurumun kendi CA'sıyla imzalıysa ya da self-signed ise o CA'yı taşıyan Secret'ın adı. Yedekleme, Kafka Connect ve Spark bu CA'ya güvenir; Polaris, Trino ve not defterleri aynı Secret'ı kendiliğinden bulur | `"s3-ca"` ([30-kurulum](../30-kurulum.md) §5.14) ya da boş | `"s3-ca"` | Hayır (S3 sertifikası herkesçe bilinen bir CA'dansa boş) |
| `s3.egressCidrs` | Not defterlerinin S3'e bağlanabileceği adresler (S3'ün veri IP'leri). Boşsa not defterleri iç ağdaki S3'e **ulaşamaz** | `$S3_EGRESS_CIDRS` (liste: `["10.20.30.40/32"]`) | `["10.20.30.40/32"]` | Pratikte evet (S3 iç ağdaysa) |
| `connect.buildImage` | Kafka Connect imajının kümede üretilip itileceği tam adres. `:latest` **kullanılamaz** (`scripts/check-site.sh` reddeder) | `$INTERNAL_REGISTRY` + `$LAKEHOUSE_NS` | `image-registry.openshift-image-registry.svc:5000/lakehouse/connect:2.0.0` | **Evet** |
| `connect.buildPushSecret` | İtme yetkisi olan `docker-registry` tipli Secret'ın adı | — (sabit) | `connect-push` | **Evet** |
| `keycloak.ldap.enabled` | Keycloak'ın Active Directory federasyonu. Gün-1'den **açık** olmalıdır | — (sabit) | `true` | **Evet** (`true`) |
| `keycloak.ldap.connectionUrl` | AD sunucusunun LDAPS adresi. Şifresiz LDAP kabul edilmez | `$LDAP_URL` | `ldaps://ad.example.com:636` | **Evet** |
| `keycloak.ldap.usersDn` | Kullanıcı nesnelerinin arandığı alt ağaç | `$LDAP_USERS_DN` | `OU=Users,DC=example,DC=com` | **Evet** |
| `keycloak.ldap.groupsDn` | Üç lakehouse grubunun bulunduğu alt ağaç | `$LDAP_GROUPS_DN` | `OU=Groups,DC=example,DC=com` | **Evet** |
| `keycloak.ldap.bindDn` | Yalnız okuma yetkili AD servis hesabının tam DN'i. **Parola burada değildir:** `keycloak-clients` Secret'ının `ldap-bind` anahtarındadır | `$LDAP_BIND_DN` | `CN=svc-lakehouse,OU=Service,DC=example,DC=com` | **Evet** |
| `keycloak.ldap.caSecret` | AD kök CA'sını taşıyan Secret'ın **adı** (anahtar `ca.crt`, PEM). Bu tek anahtar üç tüketiciyi birden açar: Keycloak CR `spec.truststores`, Zeppelin `ad-truststore` initContainer'ı ve Trino coordinator'ın JVM güven deposu (`platform/values/trino-ldap.yaml` initContainer'ı). AD sertifikası konteynerlerin zaten güvendiği bir kökten geliyorsa `""` yapın; Trino tarafında başka bir şey değişmez | — (Secret adı sabit) | `ad-ca` | Hayır (özel CA'lı AD'de **evet**) |
| `cnpg.polarisDb.storageClass` | Polaris veritabanının diski. Boş bırakılırsa kümenin varsayılan StorageClass'ı kullanılır | `$STORAGE_CLASS` | `ocs-storagecluster-ceph-rbd` | Hayır |
| `cnpg.keycloakDb.storageClass` | Keycloak veritabanının diski | `$STORAGE_CLASS` | `ocs-storagecluster-ceph-rbd` | Hayır |
| `cnpg.supersetDb.storageClass` | Superset veritabanının diski | `$STORAGE_CLASS` | `ocs-storagecluster-ceph-rbd` | Hayır |
| `backup.s3.endpoint` | PostgreSQL yedeklerinin yazıldığı S3 uç noktası | `$S3_ENDPOINT` | `https://s3.example.com` | **Evet** |
| `backup.s3.bucket` | Yedek bucket'ı. Veri bucket'ından **AYRI** olmalıdır: geri yükleme hedefi ile kaynağı aynı olamaz | `$S3_BUCKET_BACKUP` | `lakehouse-backups` | **Evet** |
| `backup.s3.region` | Yedek bucket'ının bölgesi | `$S3_REGION` | `us-east-1` | Hayır (varsayılan `us-east-1`) |
| `tls.caBundle` | `lakehouse-ca` Secret'ındaki `tls.crt` dosyasının PEM içeriği. Doluysa Trino Route'u `reencrypt` olur (`destinationCACertificate`), boşsa `passthrough` | — (kurulumda üretilen CA) | `"-----BEGIN CERTIFICATE-----\n…"` | Hayır (önerilir) |
| `sources` | Kaynak veritabanları listesi. Kurulumda **boş** kalır; kaynaklar sonradan eklenir. Alanların tamamı (`initialLoad`, `state` dahil): [yeni-kaynak-ve-pipeline.md](../50-isletme/yeni-kaynak-ve-pipeline.md) §6 | — | `[]` | Kurulumda boş |
| `pipelines` | Silver tabloları üretilecek Bronze tabloları. Kurulumda **boş** kalır. Alanlar: [yeni-kaynak-ve-pipeline.md](../50-isletme/yeni-kaynak-ve-pipeline.md) §7 | — | `[]` | Kurulumda boş |

### Bu dosyaya eklenebilen isteğe bağlı anahtarlar

Şablonda yorum satırı olarak durur ya da hiç yoktur; gerekmedikçe eklemeyin. Dosyada
aynı başlık (ör. `spark:`) zaten varsa yeni satırı o başlığın altına yazın; aynı başlığı
ikinci kez açmayın.

| Anahtar | Ne zaman eklenir | Örnek |
|---|---|---|
| `namespace` | **Eklemeyin.** Ad alanı adı üründe sabittir (`lakehouse`): `platform/apps/*.yaml` `destination.namespace`, `platform/values/jupyterhub.yaml` (`POLARIS_URI`, `TRINO_HOST`) ve `custom/examples/*` bu adı taşır; Trino ve JupyterHub alt chart'ları `.Release.Namespace` kullanır. Bu anahtarı tek başına değiştirmek kurulumu iki ad alanına böler. Başka bir ad desteklemek için sayılan dosyaların parametreleştirilmesi + e2e gerekir — ayrı bir kod görevidir | — |
| `trino.hostname`, `superset.hostname`, `jupyterhub.hostname`, `zeppelin.hostname` | Bir bileşen için kurumsal DNS adı isteniyorsa (o ad için CNAME kaydı kümenin joker adına yönlendirilir) | `trino: {hostname: trino.kurum.example.net}` |
| `keycloak.hostname` | Keycloak için kurumsal ad. **Çıplak host değil, TAM URL** yazılır; jetondaki `iss` değeri budur ve şemasız yazılırsa render hata verir | `keycloak: {hostname: https://sso.kurum.example.net}` |
| `kafka.externalListener` ve `nginx.enabled` | nginx erişim günlüğü akışı kullanılacaksa (ikisi birlikte) | `true` |
| `nginx.retentionDays` | nginx günlük tablosunda satırların kaç gün tutulacağı. Varsayılan `0` (süresiz); bakım işi eski satırları her gece siler | `nginx: {retentionDays: 90}` |
| `streams` | Uygulama ve IoT cihazlarından JSON olay akışları. Alanlar: [json-akisi-ekleme.md](../50-isletme/json-akisi-ekleme.md) §3 | `streams: [{name: sensors, retentionDays: 365}]` |
| `connect.replicas`, `connect.resources`, `connect.heap` | Kafka Connect işçi sayısı ve belleği. Varsayılan 2 işçi × 4 GiB, JVM yığını `2400m`. Yığın, bellek sınırının yaklaşık %60'ı olmalıdır; boyutlar [10-planlama](../10-planlama.md) §2.2'dedir | `connect: {resources: {requests: {cpu: 500m, memory: 6Gi}, limits: {memory: 6Gi}}, heap: 3600m}` |
| `monitoring.silverMergeStaleSeconds` | `LakehouseSilverMergeStale` alarmının eşiği (saniye). Varsayılan `2700` (45 dakika) | `monitoring: {silverMergeStaleSeconds: 5400}` |
| `monitoring.sinkLagThreshold` | `LakehouseSinkStalled` alarmının eşiği (bekleyen kayıt sayısı). Varsayılan `1000` | `monitoring: {sinkLagThreshold: 5000}` |
| `monitoring.sparkRunMaxSeconds` | `LakehouseSparkRunTooLong` alarmının eşiği (saniye). Varsayılan `1800` | `monitoring: {sparkRunMaxSeconds: 3600}` |
| `spark.maintenance` | Bakım işinin saklama süreleri. Varsayılan: snapshot'lar 7 gün (`snapshotDays`), yetim dosyalar 3 gün (`orphanDays`), Bronze satırları 30 gün (`bronzeTtlDays`) | `spark: {maintenance: {snapshotDays: 7, orphanDays: 3, bronzeTtlDays: 60}}` |
| `spark.schedules` | Zamanlı işlerin çalışma saatleri (cron, UTC). Varsayılan: `silverMerge` 15 dakikada bir, `positionDeletes` saatte bir, `compact` 6 saatte bir, `expireOrphanTtl` her gece 03:30, `mongoBronze` 5 dakikada bir | `spark: {schedules: {silverMerge: "*/30 * * * *"}}` |
| `velero` | OADP operatörü kurulduktan **sonra** ad alanı yedeği açılacaksa ([yedek-ve-geri-donus.md](../50-isletme/yedek-ve-geri-donus.md) §7) | `velero: {enabled: true, namespace: openshift-adp}` |
| `spark.driver`, `spark.executor`, `spark.shufflePartitions` | Veri hacmi büyüdüğünde Spark işlerinin kaynakları ([10-planlama](../10-planlama.md) §2.2) | `spark: {shufflePartitions: 200}` |
| `kafka.storageClass`, `kafka.storageSize` | Kafka diski varsayılandan (100 GiB) farklı olacaksa | `kafka: {storageSize: 500Gi}` |
| `cnpg.polarisDb.instances`, `cnpg.keycloakDb.instances`, `cnpg.supersetDb.instances` | PostgreSQL kopya sayısı. Ürün varsayılanı üretimde **2** (birincil + yedek); tek düğümlü bir laboratuvarda 1'e çekilir | `cnpg: {polarisDb: {instances: 1}}` |
| `cnpg.polarisDb.storageSize`, `cnpg.keycloakDb.storageSize`, `cnpg.supersetDb.storageSize` | PostgreSQL disk boyutu. Varsayılan **10Gi**; orta kademede 20Gi, büyükte 50Gi | `cnpg: {keycloakDb: {storageSize: 20Gi}}` |
| `backup.schedule` | CNPG yedeğinin cron'u. Varsayılan `"0 0 2 * * *"` (her gün 02:00). **Altı alanlıdır** (saniye dâhil) — beş alan yazarsanız CNPG reddeder | `backup: {schedule: "0 30 1 * * *"}` |
| `backup.retentionPolicy` | Yedeklerin saklanma süresi (barman-cloud). Varsayılan `30d` | `backup: {retentionPolicy: "90d"}` |
| `mavenProxy.enabled` | Küme içi Maven önbelleği (Reposilite, tek pod + disk). Varsayılan `true`: Spark işleri kütüphaneleri her koşuda buradan çözer, önbellek Maven Central'a yalnız ilk istekte çıkar | `mavenProxy: {enabled: false}` (kurum aynası kullanılacaksa) |
| `mavenProxy.upstream` | Önbelleğin kütüphaneleri çektiği adres. Varsayılan `https://repo1.maven.org/maven2` | `mavenProxy: {upstream: "https://nexus.kurum.local/repository/maven-central"}` |
| `mavenProxy.storageSize` / `mavenProxy.storageClass` | Önbellek diski. Varsayılan `5Gi`, kümenin varsayılan StorageClass'ı (bir Iceberg sürümünün kütüphaneleri ~190 MB) | `mavenProxy: {storageSize: 10Gi}` |
| `spark.ivySettingsXml` | Kurumun kendi Maven aynası (Nexus, Artifactory) Spark işlerine **doğrudan** tanıtılacaksa `ivysettings.xml` içeriği; `mavenProxy.enabled: false` ile birlikte kullanılır. Doluysa önbelleğin yerine geçer. İçine `<caches lockStrategy="artifact-lock-nio"/>` satırını koyun (eşzamanlı işler; [sorun-giderme.md](../50-isletme/sorun-giderme.md) §6.2) | çok satırlı XML bloğu |

---

## 2. `platform/values/site/trino.yaml`

Trino chart'ı ayrı bir ArgoCD Application'dır ve glue değerlerini **göremez**; bu yüzden
Keycloak, LDAP ve S3 değerleri burada tekrarlanır. **3 zorunlu anahtar** — ama her biri çok
satırlı bir metin bloğudur. Gerektiğinde eklenen isteğe bağlı anahtarlar aşağıdaki ikinci
tablodadır.

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

### Bu dosyaya eklenebilen isteğe bağlı anahtarlar

Şablonda yoktur; gerekmedikçe eklemeyin. Dördü de Trino chart'ı 1.42.2'nin kendi
anahtarlarıdır (`helm show values trino/trino --version 1.42.2`).

| Anahtar | Ne zaman eklenir | Örnek |
|---|---|---|
| `accessControl.rules."rules.json"` | Trino erişim kurallarına kendi satırınızı eklerken. Helm çok satırlı bloğu **bütünüyle ezer**: ürünün satırlarını da taşımak zorundasınız; `scripts/check-site.sh` servis hesabı satırlarının düşmediğini denetler. Adım adım: [50-isletme/kullanici-ve-yetki.md](../50-isletme/kullanici-ve-yetki.md) §5.2 | `accessControl: {type: configmap, rules: {rules.json: \|- …}}` |
| `coordinator.jvm` | Koordinatörün JVM yığını büyütülürken (ürün varsayılanı `maxHeapSize: 8G`; büyük kademede 16G — [10-planlama](../10-planlama.md) §2.2) | `coordinator: {jvm: {maxHeapSize: "16G"}}` |
| `server.workers` | İşçi sayısı değiştirilirken (ürün varsayılanı **2**; orta kademede 4, büyükte 8) | `server: {workers: 4}` |
| `worker.jvm` | İşçilerin JVM yığını büyütülürken (ürün varsayılanı `maxHeapSize: 8G`) | `worker: {jvm: {maxHeapSize: "16G"}}` |

> **JVM yığını Kubernetes isteği değildir.** `coordinator.jvm`/`worker.jvm` değerini
> büyütürken düğümde o kadar boş bellek bulunduğunu doğrulayın
> ([10-planlama](../10-planlama.md) §2.2 "Dikkat edilecek üç nokta").

### `server.coordinatorExtraConfig` içindeki doldurulan satırlar

| Satır | Anlam | `.env` karşılığı | Örnek |
|---|---|---|---|
| `http-server.authentication.oauth2.issuer` | Keycloak realm'inin tam adresi; jetondaki `iss` ile **birebir** aynı olmalıdır | `$APPS_DOMAIN` + `$LAKEHOUSE_NS` | `https://keycloak-lakehouse.apps.ocp.example.net/realms/lakehouse` |
| `http-server.authentication.oauth2.auth-url` | Tarayıcının giriş için yönlendirildiği adres: issuer + `/protocol/openid-connect/auth` | `$APPS_DOMAIN` + `$LAKEHOUSE_NS` | `https://keycloak-lakehouse.apps.ocp.example.net/realms/lakehouse/protocol/openid-connect/auth` |

Aynı bloktaki `oauth2.client-id=trino`, `oauth2.client-secret=${ENV:OIDC_CLIENT_SECRET}`,
`oauth2.principal-field=preferred_username`, `oauth2.scopes=openid`,
`web-ui.authentication.type=oauth2`, `oauth2.oidc.discovery=false` ve küme içi Keycloak'ı
gösteren `oauth2.token-url`, `oauth2.jwks-url`, `oauth2.userinfo-url` satırları **ürün
satırlarıdır**; silmeyin, değiştirmeyin. Trino'nun sunucu tarafı çağrıları bu sayede router'dan
geçmez ve router sertifikasına güvenmek gerekmez.

### `group-provider.properties` içindeki doldurulan satırlar

| Satır | Anlam | `.env` karşılığı | Örnek |
|---|---|---|---|
| `ldap.url` | AD LDAPS adresi (glue dosyasındaki `keycloak.ldap.connectionUrl` ile **aynı**) | `$LDAP_URL` | `ldaps://ad.example.com:636` |
| `ldap.admin-user` | Bağlanma DN'i (glue `keycloak.ldap.bindDn` ile **aynı**) | `$LDAP_BIND_DN` | `CN=svc-lakehouse,OU=Service,DC=example,DC=com` |
| `ldap.user-base-dn` | Kullanıcı ağacı (glue `keycloak.ldap.usersDn` ile **aynı**) | `$LDAP_USERS_DN` | `OU=Users,DC=example,DC=com` |
| `ldap.ssl.truststore.path` | **Bu satır yoktur ve eklenmez.** Bu Trino sürümünde satır, aralıklı `SSLContext was not set` hatasına ve kullanıcının gruplarının boş gelmesine yol açar. AD kök sertifikası Trino'nun Java güven deposuna ürünün kendi ayarıyla eklenir; `scripts/check-site.sh` satırı görürse durur | — | — |

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

> **`platform/values/trino-ldap.yaml` ürün dosyasıdır; silmeyin.** Bu dosya `ad-ca`
> Secret'ını Trino'nun Java güven deposuna ekler. Secret yoksa adım atlanır, Trino yine
> açılır. `group-provider.properties` bloğu bu dosyada değil, site dosyasındadır. AD
> sertifikanız herkesçe bilinen bir CA'dan geliyorsa değiştireceğiniz yer
> `platform/values/site/glue.yaml` → `keycloak.ldap.caSecret` satırıdır.

---

## 3. `platform/values/site/jupyterhub.yaml`

JupyterHub da ayrı bir Application'dır; aşağıdaki üç değer glue dosyasındaki
değerlerden **türetilmiş kopyalardır**. `scripts/check-site.sh` eşitliği denetler.
**3 anahtar.** Jeton ve kullanıcı bilgisi adresleri (`token_url`, `userdata_url`) burada
**değildir**: hub onları sunucu tarafında çağırır ve küme içi Keycloak'a gider, ürün dosyasında
sabittir (`platform/values/jupyterhub.yaml`).

| Anahtar | Anlam | Nereden türetilir | Örnek | Zorunlu mu |
|---|---|---|---|---|
| `hub.config.GenericOAuthenticator.oauth_callback_url` | Keycloak'ın kullanıcıyı geri göndereceği adres | `jupyterhub-` + `$LAKEHOUSE_NS` + `.` + `$APPS_DOMAIN` | `https://jupyterhub-lakehouse.apps.ocp.example.net/hub/oauth_callback` | **Evet** |
| `hub.config.GenericOAuthenticator.authorize_url` | Keycloak yetkilendirme uç noktası | Keycloak URL'i + `/realms/lakehouse/protocol/openid-connect/auth` | `https://keycloak-lakehouse.apps.ocp.example.net/realms/lakehouse/protocol/openid-connect/auth` | **Evet** |
| `singleuser.extraEnv.S3_ENDPOINT` | Not defterlerinin göreceği S3 adresi (glue `s3.endpoint` ile **aynı**) | `$S3_ENDPOINT` | `https://s3.example.com` | **Evet** |

---

## 4. `platform/polaris/setup.yaml` — kurulumcunun değiştirdiği satırlar

Bu dosya Polaris kataloğunun tanımıdır ve `scripts/polaris-setup.sh` ile uygulanır.
Site klasöründe değildir ama kurulumda düzenlenir. Yeni bir kaynak ya da akış eklerken
`namespaces:` listesine de satır eklenir
([yeni-kaynak-ve-pipeline.md](../50-isletme/yeni-kaynak-ve-pipeline.md) §5).

| Satır | Anlam | `.env` karşılığı | Örnek |
|---|---|---|---|
| `endpoint` | Polaris sunucusunun S3'e bağlanırken kullandığı adres | `$S3_ENDPOINT` | `https://s3.example.com` |
| `endpoint_internal` | Aynı adresin küme içi karşılığı; ayrı bir iç adres yoksa `endpoint` ile aynı yazılır | `$S3_ENDPOINT` | `https://s3.example.com` |
| `default_base_location` | Kataloğun kök yolu; veri bucket'ının adını içerir | `$S3_BUCKET_DATA` | `s3://lakehouse/` |
| `allowed_locations` | Kataloğun yazmasına izin verilen yollar; `default_base_location` ile aynı bucket | `$S3_BUCKET_DATA` | `[s3://lakehouse/]` |
| `region` | Bucket'ın bölgesi | `$S3_REGION` | `us-east-1` |
| `sts_unavailable` | Depolamada STS **yoksa** eklenir; glue `s3.vendedCredentials: false` ile birlikte kullanılır | — | `true` |

`scripts/check-site.sh` bu dosyadaki `endpoint` uyuşmazlığını **HATA değil UYARI** olarak
gösterir. Uyarıyı görüp geçmeyin: bu satırlar doldurulmazsa Polaris kataloğu yanlış
depolamayı gösterir.

**Katalog `properties` yalnız katalog YARATILIRKEN yazılır.** `polaris setup apply` var
olan bir katalogda `properties` bloğunu güncellemez; sonradan değiştirmek için
`polaris catalogs update --set-property ...` gerekir.

---

## 5. Aynı değerin birden fazla yerde olduğu noktalar

Trino ve JupyterHub ayrı uygulamalardır ve glue dosyasındaki değerleri göremez. Bu yüzden
aşağıdaki değerler birden fazla dosyada elle tutulur; `scripts/check-site.sh` hepsinin
aynı olduğunu denetler. Birini değiştirirken **hepsini** değiştirin.

| Tek doğru kaynak | Kopyası nerede | Denetleyen |
|---|---|---|
| `platform/values/site/glue.yaml` → `appsDomain` | `platform/values/site/trino.yaml` oauth2 issuer ve auth-url; `platform/values/site/jupyterhub.yaml` iki URL | `scripts/check-site.sh` |
| `platform/values/site/glue.yaml` → `s3.endpoint` | `platform/values/site/trino.yaml` `s3.endpoint`; `platform/values/site/jupyterhub.yaml` `S3_ENDPOINT`; `platform/polaris/setup.yaml` `endpoint` | `scripts/check-site.sh` (Polaris için uyarı) |
| `platform/values/site/glue.yaml` → `s3.region` | `platform/values/site/trino.yaml` `s3.region`; `platform/polaris/setup.yaml` `region` | `scripts/check-site.sh` |
| `platform/values/site/glue.yaml` → `s3.vendedCredentials` | `platform/values/site/trino.yaml` `iceberg.rest-catalog.vended-credentials-enabled` | `scripts/check-site.sh` |
| `platform/values/site/glue.yaml` → `keycloak.ldap.connectionUrl` / `bindDn` / `usersDn` | `platform/values/site/trino.yaml` `ldap.url` / `ldap.admin-user` / `ldap.user-base-dn`; `zeppelin-shiro` Secret'ındaki Shiro dosyası | `scripts/check-site.sh` (Shiro hariç) |
| `platform/values/site/glue.yaml` → `keycloak.ldap.caSecret` | Trino tarafında karşılığı yoktur: `platform/values/trino-ldap.yaml` Secret'ı varsa kendiliğinden JVM güven deposuna ekler | `scripts/check-site.sh` (`ldap.ssl.truststore.path` satırı **bulunmamalı**) |
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
