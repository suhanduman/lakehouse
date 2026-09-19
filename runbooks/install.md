# Kurulum (F1–F4: platform + Polaris + kullanıcı yüzü)

## Ön koşullar
- Kubernetes ≥ 1.33 (OpenShift veya vanilla), `kubectl`, `helm` ≥ 3.14, internet (Maven Central, Docker Hub, quay.io, downloads.apache.org, GitHub).
- S3 uyumlu depolama + bucket (`lakehouse`) **ve yedekler için AYRI bir bucket** (`backup.s3.bucket`, öneri `lakehouse-backups`; CNPG PITR — F5). Kind/dev için `components.minio=true` küme içi MinIO kurar (her iki bucket'ı da açar, PVC'li).
- Secret'lar (Git'e girmez; kurulumdan önce `lakehouse` ns'inde): `polaris-root` (`clientId`, `clientSecret`), `keycloak-admin` (`username`, `password`), `connect-push` (`kubernetes.io/dockerconfigjson`; Connect build çıktısını registry'ye iter — `connect.buildPushSecret`), `s3-creds` (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`) — her ortamda aynı isim: dev'de `components.minio=true` iken glue chart'ın MinIO şablonu tarafından üretilir, prod'da kurulumdan önce operatör tarafından oluşturulur.
- `backup-s3-creds` (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`) — **CNPG yedeklerinin** S3 kimliği (F5, `backup.s3.secret`). Veri bucket'ından AYRI hedef/hesap olmalı; dev'de `components.minio=true` iken MinIO şablonu üretir, prod'da kurulumdan ÖNCE yaratılır:
  `kubectl -n lakehouse create secret generic backup-s3-creds --from-literal=AWS_ACCESS_KEY_ID=… --from-literal=AWS_SECRET_ACCESS_KEY=…`
- Kaynak DB kimlik bilgileri (F2): `<source>-db` Secret'ları (`username`, `password`).
- **Spark işleri her koşuda `spark.jars.packages`'i Maven Central'dan (`repo1.maven.org`) çözer** (Iceberg runtime + AWS bundle; driver/executor pod'ları `/tmp/.ivy2`'ye indirir, pod ömürlük). Yani `silver-merge`, `mongo-bronze` ve 3 bakım işi için **sürekli dışarı erişim** gerekir; kapalı ağda işler `UnresolvedAddressException`/`Ivy` hatasıyla FAILED olur. Kısıtlı ağda **iç Maven aynası** tanımlayın: `glue/values.yaml` → `spark.ivySettingsXml` (dolu olduğunda glue `ConfigMap/spark-ivysettings` üretir, tüm Spark işlerine `/opt/ivy/ivysettings.xml` olarak mount eder ve `spark.jars.ivySettings` onu gösterir) — örnek ve doğrulama: `runbooks/troubleshooting.md#maven`. Aynı sınıftan PyPI ihtiyaçları (JupyterHub `postStart`, dbt örneği) için iç PyPI aynası gerekir.

## Adımlar
1. Ortama göre düzenlenecek dosyalar (hepsi; biri atlanırsa ilgili bileşen `*.lakehouse.example.com` ile açılır):
   - `platform/values/glue.yaml` — `keycloak.hostname` (tam URL) + `trino/superset/jupyterhub/zeppelin.hostname`, `tls.caBundle`, `s3.endpoint`, `platform`, `route`, **`connect.buildImage`** = Connect build çıktısının gerçek `registry/repo:tag`'i (zorunlu, `:latest` kullanma; gerekiyorsa `connect.buildPushSecret`), HA/depolama için `cnpg.*.instances` ve `kafka.storageClass`/`cnpg.*.storageClass`.
     Ayrıca **`backup.s3`** (F5 CNPG PITR): `endpoint`/`bucket` = veri bucket'ından AYRI yedek hedefi, `secret: backup-s3-creds`, `retentionPolicy` (varsayılan `30d`), `schedule` (6 alanlı cron, varsayılan her gün 02:00).
   - `platform/values/polaris.yaml` — depolama/DB ayarları (S3 kimliği `s3-creds` Secret'ından gelir).
   - `platform/polaris/setup.yaml` — katalog `endpoint`/`endpoint_internal` (S3 endpoint'i), gerekiyorsa `sts_unavailable`.
   - `platform/values/trino.yaml` — `http-server.authentication.oauth2.issuer` (= `<keycloak.hostname>/realms/lakehouse`) ve `fs.s3.*` endpoint'i; ayrıca `accessControl.rules` (grup kuralları, `runbooks/access-control.md`).
   - `platform/values/trino-ldap.yaml` — **yalnız prod**: AD group provider (`ldap.url`, bind DN, `user-base-dn`, `user-search-filter`, `memberOf`/`cn`).
   - `platform/values/jupyterhub.yaml` — Keycloak URL'leri (`authorize_url`/`token_url`/`userdata_url`), `oauth_callback_url`, `S3_ENDPOINT`.
   - `runbooks/zeppelin/shiro-ad.ini` — AD realm'i (bu dosya Secret olarak yaratılır, aşağıdaki "F4 Secret'ları" 4. adımı).
   Boyutlandırma: varsayılan `spark.*` değerleri küçük tier içindir (driver 2g/1 core, 1 executor, `shufflePartitions: 8`); tablolar/veri büyüdüğünde `platform/values/glue.yaml`'daki yorumlu büyük tier bloğunu aç (driver 4g/2 core, 2×4g executor, `shufflePartitions: 200`).
2. **F4 Secret'ları** (aşağıdaki bölüm) kurulumdan ÖNCE yaratılmalı — `components.devSecrets` yalnız dev'de `true`; prod'da chart bunları üretmez ve eksik Secret pod'u `CreateContainerConfigError`'da bırakır.
3. `bootstrap/bootstrap.sh --env prod` (ArgoCD `v3.5.2` kurar; `quay.io/strimzi-helm`, `quay.io/jetstack/charts` ve `ghcr.io/apache/superset-kubernetes-operator/charts` OCI Helm repository'lerini ArgoCD'ye kaydeder; kök + alt Application'ları — **cert-manager**, Strimzi, CNPG, **cnpg-barman** (Barman Cloud yedek eklentisi), keycloak-operator, spark-operator, **superset-operator**, glue, Polaris, **Trino**, **JupyterHub** — uygular). İzle: `kubectl -n argocd get applications` → hepsi `Synced/Healthy`. Connect imaj build'i ~10 dk.
   `trino` Application'ı polaris-setup'tan ÖNCE Healthy olamaz: pod `polaris-trino` Secret'ını bekler (bu normaldir, adım 4'ten sonra kendiliğinden düzelir).
   **İzleme/DR Application'ları (F5):** `platform/apps/dev/40-monitoring.yaml` (kube-prometheus-stack 91.4.1, ns `monitoring`, sync-wave `-1`) ve `platform/apps/dev/40-velero.yaml` (Velero chart 12.1.0, ns `velero`) **yalnız dev overlay'inde** vardır (`platform/apps/dev/`); prod overlay'i ikisini de içermez.
   - **OpenShift'te izleme:** kube-prometheus-stack KURULMAZ; platformun **user-workload monitoring**'i açılır (`openshift-monitoring` ns'indeki `cluster-monitoring-config` ConfigMap'inde `enableUserWorkload: true`). glue'nun `lakehouse` ns'ine yazdığı PodMonitor/ServiceMonitor/PrometheusRule nesneleri otomatik alınır; Alertmanager/bildirim platformundur. `monitoring.enabled` (glue) açık kalır — yalnız CRD'lerin kaynağı değişir.
   - **OpenShift'te yedek:** Velero chart'ı yerine **OADP** operatörü kurulur (ns `openshift-adp`), `DataProtectionApplication` uygulanır ve ancak ondan sonra glue'da `velero: {enabled: true, namespace: openshift-adp}` açılır (aksi hâlde `Schedule` CRD'si yokken glue Degraded olur). Ayrıntı ve alan eşlemesi: `runbooks/dr.md` §6.
   - **CNPG yedekleri** her iki ortamda da `cnpg-barman` Application'ı (Barman Cloud eklentisi) + `backup.*` değerleriyle çalışır.
4. Polaris kataloğu: `pip install 'apache-polaris==1.7.0'` → `runbooks/scripts/polaris-setup.sh` (katalog `lakehouse`, namespace'ler — `sandbox` dâhil —, roller — `writers`/`readers`/**`sandbox_writers`** + katalog rolü **`lakehouse_sandbox`** —, principal'lar; `polaris-connect/-spark/-trino/-notebooks` Secret'ları yazılır). İdempotent; var olan katalogda yalnız EKSİK nesneler yaratılır (katalog `properties` GÜNCELLENMEZ).
   - S3'te STS yoksa `setup.yaml`'da `sts_unavailable: true`; istemcilerde vending kapalı (Connect `iceberg.catalog.header.X-Iceberg-Access-Delegation=none` + `s3.*` anahtarları; Trino `iceberg.rest-catalog.vended-credentials-enabled=false`; Spark `header.X-Iceberg-Access-Delegation=none`).
5. Doğrulama: `test/e2e/polaris-smoke/job.yaml` (küme içi pyiceberg yaz/oku) — `test/e2e/run.sh` aynı adımları otomatik yapar.
6. Kaynak ekleme: `runbooks/add-source.md`; kullanıcı yüzü (Superset/notebook/Zeppelin) `runbooks/user-facing.md`; yetkilendirme `runbooks/access-control.md`
7. Kabul kanıtı: `runbooks/scripts/acceptance.sh` + `runbooks/acceptance-tests.md`. Ayrıca: sürüm/lisans listesi `runbooks/versions.md` · yükseltme `runbooks/upgrade.md` · yedek/DR `runbooks/dr.md` · sorun giderme `runbooks/troubleshooting.md` · dbt örneği `runbooks/dbt/`

## Lokal geliştirme (kind + Podman/Docker)
`test/e2e/kind.sh && bootstrap/bootstrap.sh --env dev --mode helm` (yerel chart, ArgoCD'siz) veya `--mode argocd --revision main` (ArgoCD GitHub'dan çeker → değişiklikler push'lu olmalı; `--revision` varsayılanı `main`, dal/etiket/commit SHA'sı verilebilir).
Not: `--repo/--revision` kök Application'a uygulanır ve kök, `spec.source.kustomize.patches` ile alt Application'ların (`glue`, `keycloak-operator`, `polaris`) `repoURL`/`targetRevision`'ını **her reconcile'da** aynı repo/revizyona sabitler — yani bootstrap edilen revizyon (PR'da commit SHA'sı) uçtan uca test edilir. `platform/root-app.yaml` belgelenen statik varsayılandır (dev@main); bootstrap.sh onu değil, parametreli eşdeğerini uygular.
Podman: kind ≥ 0.33 (Podman 6 uyumu); `kind.sh` düğüm pids limitini yükseltir (Spark için).

## F4 Secret'ları (kullanıcı yüzü; kurulumdan ÖNCE, `lakehouse` ns'inde)

Dev/kind'da `components.devSecrets=true` ile glue chart bunların **sahtelerini** üretir (`glue/templates/dev-secrets.yaml`) — prod'da `false`'tur ve hepsi elle yaratılır. Git'e girmezler.

| Secret | Anahtarlar | Kim tüketir |
|---|---|---|
| `keycloak-clients` | `trino`, `superset`, `jupyterhub`, `ldap-bind` | KeycloakRealmImport `placeholders`, Trino `OIDC_CLIENT_SECRET`/`LDAP_BIND_PASSWORD`, Superset `OIDC_CLIENT_SECRET`, JupyterHub `OAUTH_CLIENT_SECRET` |
| `trino-service-accounts` | `password.db` (htpasswd bcrypt) + düz parolalar `superset`, `zeppelin` | Trino `auth.passwordAuthSecret`; Superset `TRINO_PASSWORD`; Zeppelin interpreter tohumu |
| `trino-shared-secret` | `secret` | Trino `internal-communication.shared-secret` |
| `superset-secret` | `secret-key` | Superset `spec.secretKeyFrom` (SECRET_KEY) |
| `zeppelin-shiro` | `shiro.ini` | Zeppelin `/opt/zeppelin/conf/shiro.ini` |
| `zeppelin-interpreter` | `interpreter.json` | Zeppelin initContainer tohumu (PVC `/data/conf`) |
| `lakehouse-ca` | `tls.crt`, `tls.key` | cert-manager `Issuer/lakehouse-ca` (trino-tls'i imzalar) + istemcilerin güven kökü |

```bash
# 1) Keycloak client sırları (her biri rastgele, Keycloak realm'e placeholder olarak enjekte edilir)
kubectl -n lakehouse create secret generic keycloak-clients \
  --from-literal=trino="$(openssl rand -hex 24)" \
  --from-literal=superset="$(openssl rand -hex 24)" \
  --from-literal=jupyterhub="$(openssl rand -hex 24)" \
  --from-literal=ldap-bind='<AD bind parolası>'          # LDAP federasyonu kapalıysa: unused

# 2) Trino servis hesapları — password.db (bcrypt cost 10) + aynı parolaların düz kopyaları
#    (Superset/Zeppelin Basic auth ile bağlanır; parolalar password.db'dekiyle AYNI olmalı)
SUPERSET_PW="$(openssl rand -hex 16)"; ZEPPELIN_PW="$(openssl rand -hex 16)"
htpasswd -nbBC 10 superset "$SUPERSET_PW" >  password.db
htpasswd -nbBC 10 zeppelin "$ZEPPELIN_PW" >> password.db
kubectl -n lakehouse create secret generic trino-service-accounts \
  --from-file=password.db --from-literal=superset="$SUPERSET_PW" --from-literal=zeppelin="$ZEPPELIN_PW"
rm -f password.db     # (dev'de ayrıca `e2e` hesabı vardır; prod'da YARATILMAZ)

# 3) Trino düğümler arası paylaşılan sır + Superset SECRET_KEY
kubectl -n lakehouse create secret generic trino-shared-secret --from-literal=secret="$(openssl rand -hex 32)"
kubectl -n lakehouse create secret generic superset-secret --from-literal=secret-key="$(openssl rand -hex 32)"

# 4) Zeppelin Shiro (AD/LDAPS): şablonu doldur, sonra Secret yap
cp runbooks/zeppelin/shiro-ad.ini shiro.ini    # DC=/OU=, systemUsername/Password, ldaps ana bilgisayarı DEĞİŞTİR
kubectl -n lakehouse create secret generic zeppelin-shiro --from-file=shiro.ini=shiro.ini

# 5) Zeppelin interpreter tohumu: glue/files/zeppelin/interpreter.json bir Helm şablonudur;
#    prod'da 5 yer tutucuyu elle doldurup düz JSON yap (Helm'e sokma).
sed -e "s|{{ .url }}|jdbc:trino://trino.lakehouse.svc:8443/lakehouse?SSL=true\&SSLTrustStorePath=/etc/lakehouse-ca/tls.crt|" \
    -e 's|{{ .user }}|zeppelin|' \
    -e "s|{{ .password }}|$ZEPPELIN_PW|" \
    -e 's|{{ .Values.zeppelin.trinoJdbcVersion }}|483|' \
    -e 's|{{ "{{applicationId}}" }}|{{applicationId}}|' \
    glue/files/zeppelin/interpreter.json > interpreter.json
python3 -c 'import json; json.load(open("interpreter.json"))'   # geçerli JSON mu? (kalan tek {{ }} Zeppelin'in kendi {{applicationId}} yer tutucusudur)
kubectl -n lakehouse create secret generic zeppelin-interpreter --from-file=interpreter.json
rm -f interpreter.json shiro.ini

# 6) lakehouse-ca: iç kök CA (cert-manager Issuer/lakehouse-ca bunu kullanıp trino-tls'i imzalar)
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes -days 3650 \
  -subj /CN=lakehouse-ca -keyout tls.key -out tls.crt
kubectl -n lakehouse create secret tls lakehouse-ca --cert=tls.crt --key=tls.key
# tls.caBundle (platform/values/glue.yaml) = tls.crt DOSYASININ İÇERİĞİ (PEM, girintili blok):
#   tls: {selfSignedCA: false, caBundle: "-----BEGIN CERTIFICATE-----\n…"}
# Bu değer OpenShift'te Trino Route'unu `passthrough` yerine `reencrypt` (destinationCACertificate) yapar.
rm -f tls.key     # tls.crt'yi sakla: caBundle ve istemci güven deposu için gerekir
```

Kurumsal PKI'nız varsa `lakehouse-ca` yerine kurumsal ara CA'nın cert+key'ini aynı Secret'a koyun — cert-manager `Issuer/lakehouse-ca` onunla imzalar; `tls.caBundle` yine **doğrulayıcı zincir** olur.

### Kurumsal kök CA (Zeppelin/Trino'nun dışa güveni)
Zeppelin AD'ye LDAPS ile bağlanır; sunucu sertifikası kurumsal bir kök tarafından imzalıysa ve bu kök JVM `cacerts`'inde yoksa bağlanma `PKIX path building failed` ile düşer. Özel imaj YOK kuralı gereği kökü çalışma zamanında ekleyemiyoruz; seçenekler: (a) kökü kümenin varsayılan trust bundle'ına aldırıp (OpenShift `config.openshift.io/inject-trusted-cabundle`) Zeppelin Deployment'ına mount etmek, (b) AD sertifikasını genel bir CA'dan aldırmak. Pre-ship OpenShift ortamında doğrulanacak açık kalem.

## Hostname kopyaları (aynı değerin birden fazla dosyada olması ZORUNLU)

ArgoCD çok-kaynaklı Application'larda values dosyaları arasında şablonlama yok; aşağıdaki kopyalar elle tutulur. Değiştirirken **hepsi** güncellenir:

| Kaynak (tek doğru) | Kopyası | Nerede |
|---|---|---|
| glue `keycloak.hostname` (tam URL) | `http-server.authentication.oauth2.issuer` = `<keycloak.hostname>/realms/lakehouse` | `platform/values/trino.yaml` (`coordinatorExtraConfig`) |
| glue `keycloak.hostname` | `authorize_url` / `token_url` / `userdata_url` = `<keycloak.hostname>/realms/lakehouse/protocol/openid-connect/{auth,token,userinfo}` | `platform/values/jupyterhub.yaml` |
| glue `jupyterhub.hostname` | `oauth_callback_url` = `https://<jupyterhub.hostname>/hub/oauth_callback` | `platform/values/jupyterhub.yaml` |
| glue `trino.hostname` | Trino Route/Ingress host + Keycloak `trino` client redirect URI (`/oauth2/callback`) | glue `route.yaml`/`ingress.yaml` + `keycloak-realm.yaml` (TÜRETİLİR, elle değil) |
| glue `superset.hostname` / `zeppelin.hostname` | Route/Ingress host + `superset` client redirect URI (`/oauth-authorized/keycloak`) | aynı (TÜRETİLİR) |
| **S3 endpoint** (tek doğru: müşteri S3'ü) | glue `s3.endpoint` (sink/Spark istemcileri) · Polaris katalog `endpoint`/`endpoint_internal` · Trino `fs.s3.*` endpoint'i · JupyterHub `S3_ENDPOINT` | `platform/values/glue.yaml` · `platform/polaris/setup.yaml` · `platform/values/trino.yaml` · `platform/values/jupyterhub.yaml` |

Superset/Trino/JupyterHub redirect URI'leri realm şablonunda hostname'lerden türetilir; ek URI gerekiyorsa `keycloak.extraRedirectUris.<client>` listesine eklenir.

### Vanilla Kubernetes'te Ingress TLS (operatörün sorumluluğu)

OpenShift'te `route.enabled=true` ile Route TLS'i kendisi sonlandırır (`edge`, Trino'da `passthrough`/`reencrypt`).
**Vanilla Kubernetes'te glue'nun ürettiği Ingress'lerde `spec.tls` YOKTUR** (`glue/templates/ingress.yaml`): trafik ingress controller'a
düz HTTP gelir. Bu, üretimde kabul edilemez — Zeppelin/Superset form parolaları ve oturum çerezleri (`cookie.secure = true`
olduğundan çerez HTTP'de hiç gönderilmez, giriş döngüye girer) açık akar. Kurulum sırasında TLS'i **operatör** sağlar:
ingress controller'da varsayılan sertifika, ya da her host için bir `Secret` + controller'a özgü annotation'lar
(cert-manager `cert-manager.io/cluster-issuer` + `ingressClassName`). Şablona `ingress.tls` bloğu eklemek **hâlâ açık kalemdir** (F5'te yapılmadı; pre-ship)
(şu an kurulumda Ingress objeleri elle `kubectl edit` ile değil, controller/varsayılan sertifika ile TLS'lenmelidir).

## İlk giriş

Ön koşul: tarayıcı `lakehouse-ca`'ya güveniyor olmalı (Trino Route `passthrough`; `tls.caBundle` doluysa `reencrypt` ve router sertifikası geçerlidir). Kullanıcı Keycloak'ta `lakehouse-admins` / `lakehouse-analysts` / `lakehouse-students` gruplarından birinde olmalı (AD federasyonu ile gelir).

| Bileşen | URL | Akış |
|---|---|---|
| Trino Web UI | `https://<trino.hostname>/ui` | `web-ui.authentication.type=oauth2` → Keycloak'a yönlendirir; kullanıcı adı `preferred_username` |
| Superset | `https://<superset.hostname>/login/` | "Keycloak" (anahtar ikonu) butonu; rol `AUTH_ROLES_MAPPING` ile grup→(Admin/Alpha/Gamma), her girişte senkronlanır |
| JupyterHub | `https://<jupyterhub.hostname>/` | "Sign in with Keycloak"; yalnız `allowed_groups` üyeleri; `lakehouse-admins` hub yöneticisi. İlk spawn soğuk imajda ~5 dk (`startTimeout: 1200`) |
| Zeppelin | `https://<zeppelin.hostname>/` | **Keycloak DEĞİL**: Shiro + AD (LDAPS) — kullanıcı adı/parola formu (`runbooks/zeppelin/shiro-ad.ini`) |

Superset'te Trino bağlantısı tek seferlik bir komutla içe aktarılır — `runbooks/user-facing.md`.

## F3 → F4 yükseltme sırası (var olan bir kurulumu güncellerken)

Temiz kurulum değil, **çalışan bir F3 kümesini** F4'e taşıyorsanız sıra önemlidir; ArgoCD sync'i doğrudan tetiklemek
pod'ları eksik Secret'la `CreateContainerConfigError`'a sokar ve realm/issuer değişiklikleri sessizce uygulanmaz.

1. **Yedi yeni Secret, glue sync'inden ÖNCE.** Yukarıdaki "F4 Secret'ları" bölümünün 1–6 numaralı komutlarını çalıştırın
   (`keycloak-clients`, `trino-service-accounts`, `trino-shared-secret`, `superset-secret`, `zeppelin-shiro`,
   `zeppelin-interpreter`, `lakehouse-ca`). Eksik bir Secret'la sync edilirse Trino/Superset/Zeppelin pod'ları
   `CreateContainerConfigError`'da bekler; Secret sonradan yaratılınca kubelet kendiliğinden toparlar
   (`kubectl -n lakehouse get pods` ile doğrulayın).
2. **Realm'i yeniden içe aktarın.** `KeycloakRealmImport` mevcut realm'i GÜNCELLEMEZ — F4'ün yeni client'ları
   (`trino`, `superset`, `jupyterhub` + audience/groups mapper'ları) eski realm'e girmez. Aşağıdaki
   "Realm değişikliği" bölümünü uygulayın. **Kullanıcıların Keycloak UI'ında elle yaptığı her şey gider**
   (elle açılmış client'lar, kullanıcılar, rol eşlemeleri) — AD federasyonu varsa kullanıcılar tekrar akar,
   yoksa önce `kcadm.sh get users -r lakehouse > users.json` ile yedek alın.
3. **`keycloak.hostname` artık TAM URL'dir** (v2: `https://keycloak.<domain>`, eskiden yalnız host adı). Değeri
   `platform/values/glue.yaml`'da güncelleyin; token `iss`'i değiştiği için Keycloak'ı yeniden başlatın ve
   Trino'nun `oauth2.issuer`'ı (`platform/values/trino.yaml`) ile **birebir aynı** olduğunu doğrulayın:
   ```bash
   kubectl -n lakehouse rollout restart statefulset/keycloak
   kubectl -n lakehouse rollout status statefulset/keycloak --timeout=600s
   ```
4. **`polaris-setup.sh`'i tekrar çalıştırın.** F4 `sandbox` namespace'ini, `lakehouse_sandbox` katalog rolünü,
   `sandbox_writers` principal rolünü ve `notebooks` principal'ını ekler; script idempotenttir ve yalnız EKSİK
   nesneleri yaratır. **Uyarı:** katalog `properties` (ör. `polaris.config.drop-with-purge.enabled`) yalnız katalog
   YARATILIRKEN yazılır → var olan kurulumda ayrıca uygulanmalıdır:
   ```bash
   runbooks/scripts/polaris-setup.sh --setup platform/polaris/setup.yaml
   polaris catalogs update --set-property polaris.config.drop-with-purge.enabled=true lakehouse
   ```
5. **Doğrulama:**
   ```bash
   kubectl -n argocd get applications                      # hepsi Synced/Healthy (trino, jupyterhub dâhil)
   kubectl -n lakehouse get pods -o wide                   # CreateContainerConfigError kalmamalı
   kubectl -n lakehouse get secret polaris-trino polaris-notebooks   # polaris-setup yazdı mı
   kubectl -n lakehouse get certificate trino-tls          # READY=True (cert-manager)
   kubectl -n lakehouse exec deploy/trino-coordinator -- \
     curl -sk https://localhost:8443/v1/info | head -c 200 # Trino HTTPS ayakta
   ```
   Uçtan uca kanıt için `test/e2e/trino-path.sh`, `superset-path.sh`, `jupyterhub-path.sh`, `zeppelin-path.sh`
   (dev/kind kalıbı; prod'da tarayıcı akışları elle doğrulanır — `runbooks/user-facing.md`).

## Realm değişikliği (KeycloakRealmImport mevcut realm'i GÜNCELLEMEZ)

`KeycloakRealmImport` yalnız realm **yoksa** içe aktarır; `glue/templates/keycloak-realm.yaml` değişse bile çalışan realm'e dokunmaz (CR `Done` görünür, değişiklik uygulanmaz). Değişikliği uygulamak (kullanıcıların Keycloak içinde elle yaptığı her şey gider):

```bash
kubectl -n lakehouse delete keycloakrealmimport lakehouse-realm
kubectl -n lakehouse exec keycloak-0 -- /opt/keycloak/bin/kcadm.sh config credentials \
  --server http://localhost:8080 --realm master --user "$KC_ADMIN" --password "$KC_PASS"
kubectl -n lakehouse exec keycloak-0 -- /opt/keycloak/bin/kcadm.sh delete realms/lakehouse
# ArgoCD (ya da helm upgrade) CR'ı yeniden uygular:
argocd app sync glue    # ya da CLI'sız: kubectl -n argocd patch app glue --type merge -p '{"operation":{"sync":{}}}'
                        # helm modunda: helm upgrade --install glue ./glue -n lakehouse -f <values>
kubectl -n lakehouse wait keycloakrealmimport/lakehouse-realm --for=condition=Done --timeout=600s
```
Not: `kcadm.sh` oturumu pod yeniden başlatıldığında kaybolur (`config credentials`'ı tekrar çalıştırın). `KC_ADMIN/KC_PASS` = `keycloak-admin` Secret'ı.

## Sorun giderme

Kurulum, boru hattı, kullanıcı yüzü, izleme ve DR sorunlarının tamamı tek yerde: **`runbooks/troubleshooting.md`**
(PrometheusRule'ların `runbook` annotation'ları da oraya işaret eder). Sık başlıklar:

- Connect build/task hataları, offset reset → `runbooks/troubleshooting.md#connect`
- Iceberg sink lag'i → `#sink` · Spark işleri → `#spark`, `#spark-duration` · silver-merge eskimesi → `#silver-merge`
- Polaris/S3 403 (vended credentials) → `#polaris-403` · iç Maven aynası → `#maven`
- ArgoCD PVC wave kilidi, imaj çekim süreleri, NetworkPolicy → "Kurulum / ArgoCD" bölümü
- Trino/Superset/JupyterHub/Zeppelin → "Kullanıcı yüzü" bölümü · yedekler → `#dr` · loglar (Loki) → `#loki`
