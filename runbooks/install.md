# Kurulum (F1–F4: platform + Polaris + kullanıcı yüzü)

## Ön koşullar
- Kubernetes ≥ 1.33 (OpenShift veya vanilla), `kubectl`, `helm` ≥ 3.14, internet (Maven Central, Docker Hub, quay.io, downloads.apache.org, GitHub).
- S3 uyumlu depolama + bucket (`lakehouse`). Kind/dev için `components.minio=true` küme içi MinIO kurar.
- Secret'lar (Git'e girmez; kurulumdan önce `lakehouse` ns'inde): `polaris-root` (`clientId`, `clientSecret`), `keycloak-admin` (`username`, `password`), `connect-push` (`kubernetes.io/dockerconfigjson`; Connect build çıktısını registry'ye iter — `connect.buildPushSecret`), `s3-creds` (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`) — her ortamda aynı isim: dev'de `components.minio=true` iken glue chart'ın MinIO şablonu tarafından üretilir, prod'da kurulumdan önce operatör tarafından oluşturulur.
- Kaynak DB kimlik bilgileri (F2): `<source>-db` Secret'ları (`username`, `password`).
- **Spark işleri her koşuda `spark.jars.packages`'i Maven Central'dan (`repo1.maven.org`) çözer** (Iceberg runtime + AWS bundle; driver/executor pod'ları `/tmp/.ivy2`'ye indirir, pod ömürlük). Yani `silver-merge`, `mongo-bronze` ve 3 bakım işi için **sürekli dışarı erişim** gerekir; kapalı ağda işler `UnresolvedAddressException`/`Ivy` hatasıyla FAILED olur. Kısıtlı ağ seçenekleri (F5 işi, bu sürümde uygulanmadı): iç Maven aynası (`spark.jars.ivySettings` ile) ya da `spark.jars.ivy`'yi kalıcı bir PVC'ye alıp tek seferlik ısıtma.

## Adımlar
1. `platform/values/glue.yaml` ve `polaris.yaml`'ı ortama göre düzenle (hostname, S3 endpoint, `platform`, `route`, **`connect.buildImage`** = Connect build çıktısının gerçek `registry/repo:tag`'i — zorunlu, `:latest` kullanma; gerekiyorsa `connect.buildPushSecret`; HA/depolama için `cnpg.*.instances` ve `kafka.storageClass`/`cnpg.*.storageClass`); `platform/polaris/setup.yaml`'da S3 endpoint'i.
   Boyutlandırma: varsayılan `spark.*` değerleri küçük tier içindir (driver 2g/1 core, 1 executor, `shufflePartitions: 8`); tablolar/veri büyüdüğünde `platform/values/glue.yaml`'daki yorumlu büyük tier bloğunu aç (driver 4g/2 core, 2×4g executor, `shufflePartitions: 200`).
2. **F4 Secret'ları** (aşağıdaki bölüm) kurulumdan ÖNCE yaratılmalı — `components.devSecrets` yalnız dev'de `true`; prod'da chart bunları üretmez ve eksik Secret pod'u `CreateContainerConfigError`'da bırakır.
3. `bootstrap/bootstrap.sh --env prod` (ArgoCD `v3.5.2` kurar; `quay.io/strimzi-helm`, `quay.io/jetstack/charts` ve `ghcr.io/apache/superset-kubernetes-operator/charts` OCI Helm repository'lerini ArgoCD'ye kaydeder; kök + alt Application'ları — **cert-manager**, Strimzi, CNPG, keycloak-operator, spark-operator, **superset-operator**, glue, Polaris, **Trino**, **JupyterHub** — uygular). İzle: `kubectl -n argocd get applications` → hepsi `Synced/Healthy`. Connect imaj build'i ~10 dk.
   `trino` Application'ı polaris-setup'tan ÖNCE Healthy olamaz: pod `polaris-trino` Secret'ını bekler (bu normaldir, adım 4'ten sonra kendiliğinden düzelir).
4. Polaris kataloğu: `pip install 'apache-polaris==1.7.0'` → `runbooks/scripts/polaris-setup.sh` (katalog `lakehouse`, namespace'ler — `sandbox` dâhil —, roller — `writers`/`readers`/**`sandbox_writers`** + katalog rolü **`lakehouse_sandbox`** —, principal'lar; `polaris-connect/-spark/-trino/-notebooks` Secret'ları yazılır). İdempotent; var olan katalogda yalnız EKSİK nesneler yaratılır (katalog `properties` GÜNCELLENMEZ).
   - S3'te STS yoksa `setup.yaml`'da `sts_unavailable: true`; istemcilerde vending kapalı (Connect `iceberg.catalog.header.X-Iceberg-Access-Delegation=none` + `s3.*` anahtarları; Trino `iceberg.rest-catalog.vended-credentials-enabled=false`; Spark `header.X-Iceberg-Access-Delegation=none`).
5. Doğrulama: `test/e2e/polaris-smoke/job.yaml` (küme içi pyiceberg yaz/oku) — `test/e2e/run.sh` aynı adımları otomatik yapar.
6. Kaynak ekleme: `runbooks/add-source.md`; kullanıcı yüzü (Superset/notebook/Zeppelin) `runbooks/user-facing.md`; yetkilendirme `runbooks/access-control.md`

## Lokal geliştirme (kind + Podman/Docker)
`test/e2e/kind.sh && bootstrap/bootstrap.sh --env dev --mode helm` (yerel chart, ArgoCD'siz) veya `--mode argocd --revision <dal>` (ArgoCD GitHub'dan çeker → değişiklikler push'lu olmalı).
Not: `--repo/--revision` kök Application'a uygulanır ve kök, `spec.source.kustomize.patches` ile alt Application'ların (`glue`, `keycloak-operator`, `polaris`) `repoURL`/`targetRevision`'ını **her reconcile'da** aynı repo/revizyona sabitler — yani bootstrap edilen revizyon (PR'da commit SHA'sı) uçtan uca test edilir. `platform/root-app.yaml` belgelenen statik varsayılandır (dev@v2); bootstrap.sh onu değil, parametreli eşdeğerini uygular.
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

Superset/Trino/JupyterHub redirect URI'leri realm şablonunda hostname'lerden türetilir; ek URI gerekiyorsa `keycloak.extraRedirectUris.<client>` listesine eklenir.

## İlk giriş

Ön koşul: tarayıcı `lakehouse-ca`'ya güveniyor olmalı (Trino Route `passthrough`; `tls.caBundle` doluysa `reencrypt` ve router sertifikası geçerlidir). Kullanıcı Keycloak'ta `lakehouse-admins` / `lakehouse-analysts` / `lakehouse-students` gruplarından birinde olmalı (AD federasyonu ile gelir).

| Bileşen | URL | Akış |
|---|---|---|
| Trino Web UI | `https://<trino.hostname>/ui` | `web-ui.authentication.type=oauth2` → Keycloak'a yönlendirir; kullanıcı adı `preferred_username` |
| Superset | `https://<superset.hostname>/login/` | "Keycloak" (anahtar ikonu) butonu; rol `AUTH_ROLES_MAPPING` ile grup→(Admin/Alpha/Gamma), her girişte senkronlanır |
| JupyterHub | `https://<jupyterhub.hostname>/` | "Sign in with Keycloak"; yalnız `allowed_groups` üyeleri; `lakehouse-admins` hub yöneticisi. İlk spawn soğuk imajda ~5 dk (`startTimeout: 1200`) |
| Zeppelin | `https://<zeppelin.hostname>/` | **Keycloak DEĞİL**: Shiro + AD (LDAPS) — kullanıcı adı/parola formu (`runbooks/zeppelin/shiro-ad.ini`) |

Superset'te Trino bağlantısı tek seferlik bir komutla içe aktarılır — `runbooks/user-facing.md`.

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
- Connect `Build` uzun/başarısız: `kubectl -n lakehouse logs -l strimzi.io/kind=KafkaConnect --tail=100`.
- Connector task FAILED / Bronze boş: `kubectl -n lakehouse get kafkaconnector <ad> -o jsonpath='{.status.connectorStatus.tasks[0].trace}'`; task başarısızlığından sonra consumer konumu ileri kalabilir → `spec.state: stopped` → `kafka-consumer-groups.sh --group connect-<sink> --reset-offsets --to-earliest --execute` → `running`.
- NetworkPolicy prod'da açık; pre-ship OpenShift'te doğrulanmadan önce `networkPolicy.enabled=false` ile kur, sonra aç.
- Polaris health `:8182/q/health`, API `:8181`; bootstrap Job log'u `kubectl -n lakehouse logs job/polaris-bootstrap`.
- Spark işi FAILED: `kubectl -n lakehouse get sparkapplication`; `kubectl -n lakehouse logs <ad>-driver`.
- `silver-merge` `SchemaConflict`: Silver kolon tipi güvenli genişletilemiyor → manuel `ALTER TABLE` ya da yeni kolon.
- `mongo-bronze` `KAFKA_JAAS` yok → KafkaUser spark yalnız mongodb kaynağı varken oluşur.
- mongo-bronze OOM/GC uzun kesinti sonrası (backlog `collect()` ile driver'a sığmaz): tek koşu için `spark.driver.memory` artır ya da `lakehouse.kafka.offsets` özelliğini elle ilerlet; offsets yalnız başarıda ilerler.
- nginx: `nginx.dlq` doluysa `ts` dönüşümü başarısız (Fluent Bit lua filtresi eksik).

### F4 (kullanıcı yüzü)
- Trino pod'u `CreateContainerConfigError`: `polaris-trino` Secret'ı yok → `runbooks/scripts/polaris-setup.sh` koşmamış.
- Trino `401`/`Authentication failed`: HTTP 8080'de kimlik doğrulama YOK (yalnız probe/iç); istemciler **8443 HTTPS** kullanmalı ve `lakehouse-ca`'ya güvenmeli. Bearer reddediliyorsa `aud` içinde `trino` yoktur → realm'in `trino` client'ındaki audience mapper (realm değişikliği bölümü).
- Trino `Access Denied`: `rules.json` ilk eşleşen kurala bakar — `runbooks/access-control.md`. Kullanıcının grubu görünmüyorsa group provider (dev: `auth.groups`; prod: `platform/values/trino-ldap.yaml`).
- Superset `phase != Running`: `kubectl -n lakehouse describe superset superset` + lifecycle Job log'ları (`migrate`/`init`); metastore `superset-db-app` Secret'ı CNPG'den gelir.
- JupyterHub spawn zaman aşımı: soğuk `pyspark-notebook` çekimi ~4,5 dk (ölçüm) — `singleuser.startTimeout: 1200` bunun içindir; NetworkPolicy uygulayan CNI'da notebook yalnız Trino 8443 / Polaris 8181 (+dev MinIO 9000) + genel internete çıkabilir.
- Zeppelin paragrafı `Interpreter Setting 'jdbc' is not ready … DOWNLOADING_DEPENDENCIES`: açılışta Maven Central'dan `io.trino:trino-jdbc` indiriliyor (~1 dk, PVC'de kalıcı). Kapalı ağda bu adım BAŞARISIZ olur (jar'ı PVC'ye koyup `local: true` gerekir).
- Zeppelin interpreter ayarı/parola değişmiyor: `zeppelin-interpreter` Secret'ı yalnız TOHUM'dur; PVC'de dosya varsa etkisizdir → `kubectl -n lakehouse exec deploy/zeppelin -- rm /data/conf/interpreter.json && kubectl -n lakehouse rollout restart deploy/zeppelin` (UI'daki tüm interpreter değişiklikleri sıfırlanır).
