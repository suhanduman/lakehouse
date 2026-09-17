# Lakehouse v2 — F4 Kullanıcı Yüzü (Trino + Superset + JupyterHub + Zeppelin + OIDC + satır/kolon güvenliği) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Kurulum sonunda AD/Keycloak kullanıcıları Trino'ya OIDC ile, Superset'e ve JupyterHub'a Keycloak OAuth ile, Zeppelin'e Shiro (prod: AD LDAPS) ile girsin; Trino `rules.json` ile grup bazlı satır filtresi + kolon maskesi + `sandbox` şemasına yazma uygulansın; Superset/Zeppelin paylaşımlı servis hesabıyla Trino'ya bağlansın; not defterleri PyIceberg/Trino istemcisiyle Polaris'e ve Trino'ya erişsin; hepsi kind e2e'de (lokal + GitHub Actions) canlı doğrulansın.

**Architecture:** F1–F3 çizgisi: upstream bileşenler ArgoCD `Application` + resmi chart/operator (Trino chart 1.42.2, z2jh 4.4.2, cert-manager 1.21.2, Superset **Kubernetes Operator** 0.2.0), yapıştırıcı CR'lar `glue` chart'ında (Superset CR, CNPG `superset-db`, cert-manager Issuer/Certificate, Zeppelin Deployment, Route/Ingress, dev-only sentetik Secret'lar). **Trino'da kimlik doğrulama TLS ister** (upstream doğrulandı: düz HTTP'de PASSWORD/OAUTH2 hiç çalışmaz) → cert-manager ile küme-içi CA + Trino için tek PEM (`tls-combined.pem`); istemciler CA'yı `lakehouse-ca` Secret'ından alır. Trino kimlik: `OAUTH2` (interaktif, Keycloak) + `PASSWORD` (dosya; servis hesapları superset/zeppelin/e2e). Gruplar: Trino'da OAuth2 `groups` claim'i **yok** → dev: dosya group provider, prod: LDAP group provider (Trino ≥473). Keycloak realm sırları `KeycloakRealmImport.spec.placeholders` ile Secret'tan. Sıfır özel imaj: Superset sürücüleri (`trino[sqlalchemy]`, `authlib`) operator'ün `bootstrapScript`'iyle, notebook paketleri `postStart pip`, Zeppelin Trino JDBC'si interpreter `dependencies` (Maven) ile gelir.

**Tech Stack:** Trino **483** (chart `trino/trino` **1.42.2**, `image.tag: "483"`) · Apache Superset **6.1.0** via `superset-operator` **0.2.0** (`oci://ghcr.io/apache/superset-kubernetes-operator/charts/superset-operator`, CRD `superset.apache.org/v1alpha1 Superset`) · JupyterHub z2jh **4.4.2** (JupyterHub 5.5.2, OAuthenticator 17.4.0) + `quay.io/jupyter/pyspark-notebook:spark-4.1.2` · Apache Zeppelin **0.12.1** (`apache/zeppelin:0.12.1`) · cert-manager **v1.21.2** (`oci://quay.io/jetstack/charts/cert-manager`) · Keycloak 26.7.3 (mevcut) · Polaris 1.7.0 (mevcut) · pyiceberg 0.10, trino-python-client 0.339 (e2e)

**Spec:** `docs/specs/2026-09-10-lakehouse-v2-design.md` §2 (F.1, G.1.1, F.3.1), §3, §7, §8 (cert-manager TLS, Polaris RBAC), §9, §13 F4, §14 (paylaşımlı servis hesabı; satır/kolon güvenliği interaktif Trino kullanıcıları için) · **Bulgular:** `docs/plans/2026-09-10-f0-findings.md` (F1 notu: ArgoCD OCI repository Secret, kustomize patch'leri; F2/F3 notları: e2e yeniden-koşu semantiği, verify Job, run_spark_once) · **Backlog (hafıza, 2026-09-16):** Trino `rules.json` kullanıcı/grup bazlı satır/kolon + `sandbox` namespace yazma (Polaris RBAC motor bazlı, analist ayrımı Trino'da); connect ACL sıkılaştırma (mongodb prefix'te Read gereksiz).

## Global Constraints

- Özel imaj YOK, hack YOK, runtime mutasyon YOK. Kod yalnız e2e kontrol script'leri (`test/e2e/**`). Her bileşen upstream chart/operator + values ya da `glue` şablonu.
- Sürümler sabit: Trino chart `1.42.2` + `image.tag: "483"`; superset-operator `0.2.0`, Superset imaj `6.1.0`; z2jh `4.4.2`; pyspark-notebook `spark-4.1.2`; Zeppelin `0.12.1`; cert-manager `v1.21.2`; Trino JDBC `io.trino:trino-jdbc:483`; pip: `trino[sqlalchemy]>=0.339,<0.340`, `authlib>=1.6,<2`, `pyiceberg[s3fs,pyarrow]>=0.10,<0.11`.
- Tek namespace `lakehouse` (cert-manager kendi `cert-manager` ns'inde, küme-geneli).
- **Secret sözleşmesi (kurulumdan önce; dev'de `components.devSecrets=true` ile glue üretir):** `keycloak-clients` (`trino`, `superset`, `jupyterhub`, `ldap-bind`) · `trino-service-accounts` (`password.db` htpasswd bcrypt cost≥8; `superset`, `zeppelin`, `e2e` düz parolalar) · `trino-shared-secret` (`secret`) · `superset-secret` (`secret-key`) · `zeppelin-shiro` (`shiro.ini`) · `zeppelin-interpreter` (`interpreter.json`) · `lakehouse-ca` (`tls.crt`, `tls.key`; dev'de cert-manager selfsigned üretir). Mevcutlar: `polaris-root`, `keycloak-admin`, `s3-creds`, `connect-push`, `<source>-db`.
- Keycloak `hostname` **tam URL** (v2): dev `http://keycloak-service.lakehouse.svc:8080` (token `iss` küme içinden çözülür; dev'de tarayıcı akışı küme dışından çalışmaz — belgelenir), prod `https://keycloak.<domain>`. Trino `oauth2.issuer` = `<keycloak.hostname>/realms/lakehouse` (aynı değer; iki dosyada — belgelenir).
- Trino: `http-server.authentication.type=OAUTH2,PASSWORD`; HTTPS 8443 (`tls-combined.pem`), HTTP 8080 yalnız probe/iç; `internal-communication.shared-secret` Secret'tan (`${ENV:…}`); OAuth2 özellikleri **yalnız coordinator** (`server.coordinatorExtraConfig`); `principal-field=preferred_username`; Keycloak `trino` client'ında **audience mapper** (`aud` içinde `trino` olmadan Bearer reddedilir).
- Gruplar/roller (Keycloak grubu = Trino grubu = AD grubu adı): `lakehouse-admins`, `lakehouse-analysts`, `lakehouse-students`. Superset rol eşlemesi `groups` userinfo claim'inden (`AUTH_ROLES_MAPPING`), JupyterHub `allowed_groups` (+`manage_groups`, `auth_state_groups_key: oauth_user.groups`).
- Polaris: `sandbox` namespace + `lakehouse_sandbox` katalog rolü (namespace kapsamlı) → principal role `sandbox_writers` (principals `trino`, `notebooks`). Analist/öğrenci ayrımı **Trino rules.json'da**.
- Testler: helm-unittest (render), e2e = kind gerçek yol (`trino-path.sh`, `superset-path.sh`, `jupyterhub-path.sh`, `zeppelin-path.sh`; pg/mongo/nginx yollarından SONRA, Silver sayıları onlara dayanır). F2/F3 e2e kuralları geçerli (taze küme; `sync.revision` beklemesi).
- CI: `ubuntu-latest` public repo = 4 vCPU/16 GB; `timeout-minutes: 90`; kaynak fotoğrafı her koşuda (`if: always()`).
- Commit sonu: `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`. Dal `v2`. Shell komutlarında `--verify` / `--no-verify` metni kullanılmaz (hook).

## Spec'e göre kararlar (plan yazarının ruling'leri — upstream 2026-09-17 doğrulamalarına dayanır)

| # | Karar | Gerekçe (doğrulanmış) |
|---|---|---|
| R1 | **Superset: resmi Helm chart yerine Apache Superset Kubernetes Operator 0.2.0** (`Superset` CR glue'da) | Chart `Chart.yaml: deprecated: true`, README "use the official Apache Superset Kubernetes Operator"; `supersetNode.connections`/`init.initscript` deprecated, alt chart'lar `bitnamilegacy` imajlarında. Operator: `bootstrapScript`, ham `config`, Secret-ref'li sırlar, Valkey/Redis **opsiyonel**, `lifecycle.migrate/init`. Şartname A.4 Operator tercihini de karşılar. Risk: API `v1alpha1` (3 sürüm, Haz–Ağu 2026) → sürüm sabit, `runbooks/versions.md`'de (F5) işaretlenir; Task 4 ilk adımı canlı deneme, başarısızlıkta belgelenmiş geri dönüş chart 0.22.8 |
| R2 | **Trino native HTTPS + cert-manager** (`Certificate.additionalOutputFormats: CombinedPEM` → `tls-combined.pem`), prod dış erişim Route **reencrypt** (`destinationCACertificate` = `tls.caBundle`), dev/e2e küme içi `https://trino:8443` + `lakehouse-ca` | `allow-insecure-over-http=true` düz HTTP'de yalnız `InsecureAuthenticator` çalıştırır (Basic-parola reddedilir, Bearer yok sayılır); Trino JDBC parola için `SSL=true` şart, python istemci `TrinoAuthError`. cert-manager spec §8'de zaten var; `AdditionalCertificateOutputFormats` 1.15'ten beri beta-açık. Trino PEM key+cert tek dosya kabul eder; `http-server.https.ssl-context.refresh-time` 1m ile sertifika yenilenir |
| R3 | Trino grupları: dev **dosya** group provider (`auth.groups`), prod **LDAP** group provider (`ldap.admin-user/admin-password`, Trino ≥473) | `http-server.authentication.oauth2.groups-field` **yok** (OAuth2Config); grup yalnız group provider'dan |
| R4 | Trino OAuth2 + PASSWORD birlikte; servis hesapları (superset, zeppelin, e2e) `password.db` (htpasswd bcrypt cost 10) | Çoklu tip virgülle desteklenir, sırayla denenir. Superset `Secure Extra`/URI parolası → `BasicAuthentication`, Zeppelin JDBC `user/password` → dosya kimliği. Spec §14 paylaşımlı servis hesabı |
| R5 | Keycloak: realm sırları `spec.placeholders` (Secret `keycloak-clients`), `trino` client'ında audience mapper + (dev) `directAccessGrantsEnabled` + dev kullanıcılar (`keycloak.devUsers`), `superset` client'ında `groups` mapper (userinfo); redirect URI'ler **hostname'lerden türetilir** | Placeholders yalnız Secret, aynı ns; realm import mevcut realm'i **güncellemez** (değişiklik = realm sil/yeniden import, runbook). FAB keycloak provider `role_keys = userinfo.groups`; Trino `aud` ∈ {client-id} ∪ additional-audiences. Bind parolası `${LDAP_BIND_CREDENTIAL}` placeholder'ı → `keycloak.ldap.bindCredential` values'tan kalkar (F1 notu kapanır) |
| R6 | Zeppelin: `ZEPPELIN_CONFIG_FS_DIR=/data/conf` (PVC) + initContainer `cp` (dosya yoksa) ile `interpreter.json` tohumu (Secret `zeppelin-interpreter`), `shiro.ini` Secret subPath, notebook PVC, Trino JDBC `dependencies` Maven'den | Zeppelin her açılışta `interpreter.json`'ı atomik yazar (temp+rename) → salt-okunur ConfigMap/subPath açılışı kırar; `zeppelin.config.fs.dir` interpreter.json yolunu ayırır. `${env:}` Shiro enterpolasyonu belgesiz → parola dosyanın içinde, dosya Secret'ta |
| R7 | Zeppelin kimlik: prod `ActiveDirectoryGroupRealm` (LDAPS), dev Shiro `[users]`; OIDC yok | 0.12'de OIDC/pac4j realm yok (yalnız AD/LDAP/PAM/Knox/Kerberos); şartname G.1.1 LDAPS'i kabul eder (spec §7) |
| R8 | JupyterHub: `GenericOAuthenticator`, client secret `OAUTH_CLIENT_SECRET` env'den (`hub.extraEnv` valueFrom), `allowed_groups`+`manage_groups`+`auth_state_groups_key`, hub DB `sqlite-pvc` (varsayılan) | OAuthenticator 17.x env varsayılanları; `allowed_groups` `manage_groups` olmadan ValueError; `claim_groups_key` deprecated |
| R9 | Superset Redis'siz: `valkey` alanı yok, `SimpleCache`, worker/beat yok; Alerts&Reports istenirse Valkey + worker (runbook) | Operator Valkey'i opsiyonel tutar; spec §7 "Alerts&Reports opsiyonel" |
| R10 | Superset Trino bağlantısı **deklaratif dosya + tek runbook komutu**: `superset legacy-import-datasources -p /app/configs/trino.yaml`; parola `SQLALCHEMY_CUSTOM_PASSWORD_STORE` ile env'den | 6.x `import-datasources` yalnız v1 ZIP; `legacy-import-datasources` v0 YAML; `SQLALCHEMY_CUSTOM_PASSWORD_STORE` 6.x'te var → URI'de parola yok |
| R11 | e2e Trino doğrulaması küme içi Python Job (`trino` istemcisi): servis hesabı Basic + Keycloak **password grant** ile Bearer (analyst1/student1) → satır filtresi/kolon maskesi/sandbox yazma | Tarayıcı yok; OAuth2Authenticator Bearer JWT'yi doğrudan doğrular (JWKS, iss, aud) |
| R12 | Trino dev: `server.workers: 0` + `coordinator.config.nodeScheduler.includeCoordinator: true`, heap 1536M, `query.maxMemoryPerNode 512MB`; prod: 2 worker, 8G | Chart `workers: 0`'da include-coordinator'ı **otomatik** açmaz; `max-memory-per-node + headroom < heap` şartı |
| R13 | Route/Ingress hepsi glue'da (`trino` reencrypt/`backend-protocol: HTTPS`, diğerleri edge/HTTP); upstream chart ingress'leri kapalı | OpenShift Route tek yerde; operator'de Route tipi yok |
| R14 | Hostname'ler her bileşenin kendi bloğunda (`trino.hostname`, `superset.hostname`, `jupyterhub.hostname`, `zeppelin.hostname`, `keycloak.hostname`); z2jh values ve trino values'ta zorunlu kopyalar (`oauth_callback_url`, `oauth2.issuer`) runbook'ta listelenir | ArgoCD çok-kaynaklı values arası şablonlama yok; kopya sayısı 2, belgelenir |
| R15 | F3 kalanı: connect KafkaUser mongodb prefix'inde `Read` kaldırılır (bu planda, Task 7); ortak sink helper + DLQ headers **F5** | Sink config kararları F5'te toplu (saklama/DLQ) |
| R16 | Spec §3/§7 satırları bu kararlarla güncellenir (Task 7) | Spec otoriter kalsın |

---

## Dosya yapısı

```
platform/apps/00-cert-manager.yaml            Application: oci quay.io/jetstack/charts cert-manager v1.21.2 (ns cert-manager, crds.enabled)
platform/apps/00-superset-operator.yaml       Application: oci ghcr.io/apache/superset-kubernetes-operator/charts superset-operator 0.2.0 (ns lakehouse)
platform/apps/30-trino.yaml                   Application: trino/trino 1.42.2, valueFiles [$values/platform/values/trino.yaml] (wave 2)
platform/apps/30-jupyterhub.yaml              Application: jupyterhub 4.4.2, valueFiles [$values/platform/values/jupyterhub.yaml] (wave 2)
platform/apps/kustomization.yaml              + 4 kaynak
platform/envs/dev/kustomization.yaml          trino/jupyterhub Application'larına -dev valueFiles patch'i
platform/values/{trino,trino-dev,jupyterhub,jupyterhub-dev}.yaml
platform/polaris/setup.yaml                   sandbox namespace, lakehouse_sandbox rolü, sandbox_writers principal role
bootstrap/bootstrap.sh                        OCI repository Secret'ları (jetstack, ghcr superset), kök patch'leri (trino, jupyterhub), helm modu 4 yeni kurulum
glue/templates/_helpers.tpl                   glue.host (URL→host)
glue/templates/tls.yaml                       selfsigned bootstrap (dev) → Issuer lakehouse-ca → Certificate trino-tls (CombinedPEM)
glue/templates/dev-secrets.yaml               components.devSecrets: keycloak-clients, trino-service-accounts, trino-shared-secret, superset-secret, zeppelin-shiro, zeppelin-interpreter
glue/templates/keycloak.yaml                  hostname URL
glue/templates/keycloak-realm.yaml            placeholders, mappers, directAccessGrants, devUsers, türetilmiş redirectUris
glue/templates/cnpg.yaml                      + superset-db
glue/templates/superset.yaml                  Superset CR + ConfigMap superset-datasources
glue/templates/zeppelin.yaml                  PVC, Deployment (initContainer tohum), Service
glue/templates/ingress.yaml, route.yaml       trino (reencrypt / backend HTTPS), superset, jupyterhub (proxy-public), zeppelin
glue/templates/kafka-users.yaml               mongodb prefix Read kaldırılır (Task 7)
glue/files/zeppelin/interpreter.json          jdbc → Trino tohumu (tpl: url/user/password)
glue/values.yaml                              tls, trino, superset, jupyterhub, zeppelin, cnpg.supersetDb, components.devSecrets, keycloak.{clientsSecret,devUsers,extraRedirectUris}
platform/values/glue.yaml, glue-dev.yaml      prod/dev değerleri
glue/tests/{tls_test,keycloak_test,superset_test,zeppelin_test,platform_test,mongo_test}.yaml
runbooks/zeppelin/shiro-ad.ini                prod AD şablonu
runbooks/install.md                           F4 bölümü (Secret listesi, hostname kopyaları, ilk giriş)
runbooks/access-control.md                    rules.json modeli, gruplar, LDAP group provider, sandbox
runbooks/user-facing.md                       Superset DB import, Zeppelin interpreter/shiro, JupyterHub, dev'de tarayıcı sınırı
test/e2e/lib.sh                               run_check_job
test/e2e/trino-check/{check.py,job.yaml}      Basic + Bearer sorguları, filtre/maske/sandbox
test/e2e/{trino-path,superset-path,jupyterhub-path,zeppelin-path}.sh
test/e2e/run.sh                               app bekleme listesi, trino beklemesi polaris-setup SONRASI, 4 yol
.github/workflows/e2e.yaml                    timeout 90, teşhis + kaynak fotoğrafı
docs/specs/2026-09-10-lakehouse-v2-design.md  §3/§7 güncellemesi
docs/plans/2026-09-10-f0-findings.md          "F4 notu"
README.md                                     "Şu an"
```

---

### Task 1: Platform ön koşulları — cert-manager, TLS zinciri, dev Secret'ları, taze küme

**Files:**
- Create: `platform/apps/00-cert-manager.yaml`, `glue/templates/tls.yaml`, `glue/templates/dev-secrets.yaml`, `glue/tests/tls_test.yaml`
- Modify: `platform/apps/kustomization.yaml`, `bootstrap/bootstrap.sh`, `glue/values.yaml`, `platform/values/glue-dev.yaml`, `glue/templates/_helpers.tpl`, `test/e2e/run.sh`

**Interfaces:**
- Produces: Secret `lakehouse-ca` (`tls.crt` = CA sertifikası; dev'de cert-manager üretir), `Issuer/lakehouse-ca` (ns), Secret `trino-tls` (`tls-combined.pem`, `tls.crt`, `tls.key`, `ca.crt`), dev Secret'ları (Global Constraints listesi), helper `glue.host`.
- Dev sabitleri (yalnız kind): keycloak-clients `trino=trino-dev-secret`, `superset=superset-dev-secret`, `jupyterhub=jupyterhub-dev-secret`, `ldap-bind=unused`; trino-service-accounts `superset=superset-dev`, `zeppelin=zeppelin-dev`, `e2e=e2e-dev`; trino-shared-secret `secret=dev-shared-secret-0123456789abcdef`; superset-secret `secret-key=dev-superset-secret-key-0123456789`; Zeppelin dev kullanıcıları `analyst1/analyst1-dev`, `student1/student1-dev`.

- [ ] **Step 1: cert-manager Application**

`platform/apps/00-cert-manager.yaml`:
```yaml
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata: {name: cert-manager, namespace: argocd, annotations: {argocd.argoproj.io/sync-wave: "0"}}
spec:
  project: default
  source:
    repoURL: quay.io/jetstack/charts          # OCI: argocd ns'inde repository Secret gerekir (bootstrap.sh, F1 notu)
    chart: cert-manager
    targetRevision: v1.21.2
    helm:
      valuesObject:
        crds: {enabled: true, keep: true}
  destination: {server: https://kubernetes.default.svc, namespace: cert-manager}
  syncPolicy:
    automated: {prune: true, selfHeal: true}
    syncOptions: [CreateNamespace=true, ServerSideApply=true]
```
`platform/apps/kustomization.yaml` resources listesine `- 00-cert-manager.yaml` ekle (00-strimzi'den önce).

- [ ] **Step 2: bootstrap.sh — OCI repository Secret'ları ve helm modu**

Strimzi Secret bloğunun altına (aynı heredoc kalıbıyla) iki Secret daha: `name: jetstack-helm, url: quay.io/jetstack/charts` ve `name: superset-operator-helm, url: ghcr.io/apache/superset-kubernetes-operator/charts` (`type: helm`, `enableOCI: "true"`). Helm modunda, strimzi satırından ÖNCE:
```bash
  helm upgrade --install cert-manager oci://quay.io/jetstack/charts/cert-manager --version v1.21.2 -n cert-manager --create-namespace --set crds.enabled=true --wait --timeout 5m
```
(Superset operator/Trino/JupyterHub helm satırları kendi task'larında eklenir.)

- [ ] **Step 3: values + helper**

`glue/values.yaml`'a ekle:
```yaml
tls:
  selfSignedCA: false         # dev/kind: true -> ClusterIssuer selfsigned + CA Certificate -> Secret lakehouse-ca. Prod: false, lakehouse-ca (tls.crt, tls.key) kurulumdan önce
  caBundle: ""                # prod: Route reencrypt destinationCACertificate (PEM, lakehouse-ca tls.crt). Boş -> passthrough
trino:
  hostname: trino.lakehouse.example.com
components:
  minio: false
  devSecrets: false           # DEV-ONLY: keycloak-clients, trino-service-accounts, trino-shared-secret, superset-secret, zeppelin-shiro, zeppelin-interpreter (sentetik)
```
`_helpers.tpl`:
```
{{- define "glue.host" -}}{{ regexReplaceAll "^https?://([^/:]+).*$" . "${1}" }}{{- end -}}
```
`platform/values/glue-dev.yaml`: `tls: {selfSignedCA: true}`, `trino: {hostname: trino.127.0.0.1.nip.io}`, `components: {minio: true, devSecrets: true}`.

- [ ] **Step 4: tls.yaml**

```yaml
{{- $ns := include "glue.ns" . }}
{{- if .Values.tls.selfSignedCA }}
# DEV-ONLY: kendinden imzalı kök -> lakehouse-ca Secret'ı (prod'da bu Secret kurulumdan önce yaratılır, runbooks/install.md)
apiVersion: cert-manager.io/v1
kind: ClusterIssuer
metadata: {name: lakehouse-selfsigned}
spec: {selfSigned: {}}
---
apiVersion: cert-manager.io/v1
kind: Certificate
metadata: {name: lakehouse-ca, namespace: {{ $ns }}, annotations: {argocd.argoproj.io/sync-wave: "0"}}
spec:
  isCA: true
  commonName: lakehouse-ca
  secretName: lakehouse-ca
  duration: 87600h
  privateKey: {algorithm: ECDSA, size: 256}
  issuerRef: {name: lakehouse-selfsigned, kind: ClusterIssuer, group: cert-manager.io}
---
{{- end }}
apiVersion: cert-manager.io/v1
kind: Issuer
metadata: {name: lakehouse-ca, namespace: {{ $ns }}, annotations: {argocd.argoproj.io/sync-wave: "0"}}
spec: {ca: {secretName: lakehouse-ca}}
---
# Trino HTTPS (kimlik doğrulama TLS ister; plan R2). tls-combined.pem = key + cert tek PEM (Trino keystore.path)
apiVersion: cert-manager.io/v1
kind: Certificate
metadata: {name: trino-tls, namespace: {{ $ns }}, annotations: {argocd.argoproj.io/sync-wave: "1"}}
spec:
  secretName: trino-tls
  duration: 2160h
  renewBefore: 360h
  dnsNames: [trino, trino.{{ $ns }}.svc, trino.{{ $ns }}.svc.cluster.local, {{ .Values.trino.hostname }}]
  additionalOutputFormats: [{type: CombinedPEM}]
  issuerRef: {name: lakehouse-ca, kind: Issuer}
```

- [ ] **Step 5: dev-secrets.yaml** (bu task'ta ilk 5 Secret; `zeppelin-interpreter` Task 6'da eklenir)

```yaml
{{- if .Values.components.devSecrets }}
{{- $ns := include "glue.ns" . }}
# DEV-ONLY (kind/e2e): prod'da bu Secret'lar kurulumdan önce elle yaratılır (runbooks/install.md "F4 Secret'ları")
apiVersion: v1
kind: Secret
metadata: {name: keycloak-clients, namespace: {{ $ns }}}
stringData: {trino: trino-dev-secret, superset: superset-dev-secret, jupyterhub: jupyterhub-dev-secret, ldap-bind: unused}
---
apiVersion: v1
kind: Secret
metadata: {name: trino-service-accounts, namespace: {{ $ns }}}
stringData:
  # htpasswd -nbBC 10 <user> <parola> (Trino bcrypt cost >= 8)
  password.db: |
    superset:$2y$10$l1mHFPzDlBEaa1QOYeO9wuMBYA8SZZiQwCmGmofclvjn5ymLYTwpq
    zeppelin:$2y$10$GHuAFPKV47ZLNd923aalLOw3/NxpTMOzyzCBqn5McN6hjLus2.2oW
    e2e:$2y$10$IltLLdZsWhmkbReSgxef5uXsoW.ug2WRYtdreKb5SIa7YslepUVqe
  superset: superset-dev
  zeppelin: zeppelin-dev
  e2e: e2e-dev
---
apiVersion: v1
kind: Secret
metadata: {name: trino-shared-secret, namespace: {{ $ns }}}
stringData: {secret: dev-shared-secret-0123456789abcdef}
---
apiVersion: v1
kind: Secret
metadata: {name: superset-secret, namespace: {{ $ns }}}
stringData: {secret-key: dev-superset-secret-key-0123456789}
---
apiVersion: v1
kind: Secret
metadata: {name: zeppelin-shiro, namespace: {{ $ns }}}
stringData:
  shiro.ini: |
    [users]
    analyst1 = analyst1-dev, analyst
    student1 = student1-dev, student
    [main]
    sessionManager = org.apache.shiro.web.session.mgt.DefaultWebSessionManager
    securityManager.sessionManager = $sessionManager
    securityManager.sessionManager.globalSessionTimeout = 86400000
    shiro.loginUrl = /api/login
    [roles]
    analyst = *
    student = *
    [urls]
    /api/version = anon
    /api/cluster/address = anon
    /api/interpreter/** = authc, roles[analyst]
    /api/notebook-repositories/** = authc, roles[analyst]
    /api/configurations/** = authc, roles[analyst]
    /api/credential/** = authc, roles[analyst]
    /** = authc
{{- end }}
```

- [ ] **Step 6: helm-unittest**

`glue/tests/tls_test.yaml`:
```yaml
suite: tls + dev secrets
templates: [tls.yaml, dev-secrets.yaml]
tests:
  - it: prod renders only Issuer and trino Certificate
    template: tls.yaml
    asserts:
      - hasDocuments: {count: 2}
      - containsDocument: {kind: Issuer, apiVersion: cert-manager.io/v1, name: lakehouse-ca}
      - containsDocument: {kind: Certificate, apiVersion: cert-manager.io/v1, name: trino-tls}
  - it: dev adds selfsigned bootstrap and combined PEM
    template: tls.yaml
    set: {tls.selfSignedCA: true}
    asserts:
      - hasDocuments: {count: 4}
      - containsDocument: {kind: ClusterIssuer, apiVersion: cert-manager.io/v1, name: lakehouse-selfsigned}
      - equal: {path: spec.additionalOutputFormats[0].type, value: CombinedPEM, documentIndex: 3}
  - it: dev secrets off by default
    template: dev-secrets.yaml
    asserts:
      - hasDocuments: {count: 0}
  - it: dev secrets render password.db
    template: dev-secrets.yaml
    set: {components.devSecrets: true}
    asserts:
      - containsDocument: {kind: Secret, apiVersion: v1, name: trino-service-accounts}
      - matchRegex: {path: stringData["password.db"], pattern: "e2e:\\$2y\\$10\\$", documentIndex: 1}
```
Run: `helm unittest glue` → tümü PASS; `helm template glue -f platform/values/glue-dev.yaml >/dev/null` ve `-f platform/values/glue.yaml` hatasız.

- [ ] **Step 7: run.sh — app listesi**

`for app in strimzi …` listesine `cert-manager` ekle (başa). Helm modu değişikliği bootstrap.sh'ta.

- [ ] **Step 8: Taze küme + F3 e2e (helm modu) — F4 tabanı**

Mevcut kind kümesi mutasyona uğramış (5 gün, uyku sonrası STS/Maven hataları — 2026-09-17 bulgusu): `kind delete cluster --name lakehouse`, ardından
```bash
nohup test/e2e/run.sh --mode helm > /tmp/e2e-f4-base.log 2>&1 & disown
```
(Bash arka planı 10 dk'da kesilir → nohup + `tail -F`, F1 süreç notu.) Beklenen: `E2E F1 OK`, `E2E F2 OK`, `E2E F3 … OK` satırları ve `kubectl -n lakehouse get secret lakehouse-ca trino-tls keycloak-clients trino-service-accounts` dördü var; `kubectl -n lakehouse get certificate` ikisi `READY True`; `kubectl -n lakehouse get secret trino-tls -o jsonpath='{.data.tls-combined\.pem}' | base64 -d | grep -c 'BEGIN'` → 2 (key + cert).

- [ ] **Step 9: Commit**

```bash
git add platform/apps/00-cert-manager.yaml platform/apps/kustomization.yaml bootstrap/bootstrap.sh glue/ platform/values/glue-dev.yaml test/e2e/run.sh
git commit -m "feat(platform): cert-manager + lakehouse-ca issuer + trino-tls (CombinedPEM); dev-only secrets template

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 2: Keycloak — hostname URL, placeholders, mapper'lar, dev kullanıcılar, türetilmiş redirect URI'ler

**Files:**
- Modify: `glue/templates/keycloak.yaml`, `glue/templates/keycloak-realm.yaml`, `glue/templates/ingress.yaml`, `glue/templates/route.yaml`, `glue/values.yaml`, `platform/values/glue.yaml`, `platform/values/glue-dev.yaml`, `glue/tests/keycloak_test.yaml`

**Interfaces:**
- Consumes: Secret `keycloak-clients` (Task 1), `glue.host`.
- Produces: realm `lakehouse` client'ları `trino` (secret `${TRINO_CLIENT_SECRET}`, audience mapper `trino`, `directAccessGrantsEnabled` = `keycloak.devUsers` doluysa), `superset` (`groups` mapper: userinfo+access+id), `jupyterhub` (mevcut `groups` mapper), `polaris`; dev kullanıcılar `admin1/admin1-dev` (lakehouse-admins), `analyst1/analyst1-dev` (lakehouse-analysts), `student1/student1-dev` (lakehouse-students). Token endpoint (dev): `http://keycloak-service.lakehouse.svc:8080/realms/lakehouse/protocol/openid-connect/token`.

- [ ] **Step 1: values**

`glue/values.yaml` `keycloak:` bloğu:
```yaml
keycloak:
  hostname: https://keycloak.lakehouse.example.com   # TAM URL (hostname v2): token iss = bu değer; Trino oauth2.issuer (platform/values/trino.yaml) ile aynı olmalı
  realm: {name: lakehouse, displayName: Lakehouse}
  adminSecret: keycloak-admin
  clientsSecret: keycloak-clients     # keys: trino, superset, jupyterhub, ldap-bind -> realm import placeholders (spec.placeholders)
  ldap:
    enabled: false
    connectionUrl: ldaps://ad.example.com:636
    usersDn: "OU=Users,DC=example,DC=com"
    groupsDn: "OU=Groups,DC=example,DC=com"
    bindDn: "CN=svc-lakehouse,OU=Service,DC=example,DC=com"
    # bindCredential values'ta DEĞİL: keycloak-clients Secret'ının ldap-bind anahtarı (${LDAP_BIND_CREDENTIAL})
  extraRedirectUris: {trino: [], superset: [], jupyterhub: [], polaris: []}   # türetilenlere ek (ör. lokal port-forward)
  devUsers: []                        # DEV-ONLY: [{username, password, group}] -> statik parolalı kullanıcılar + trino client'ında password grant
superset:
  hostname: superset.lakehouse.example.com
jupyterhub:
  hostname: jupyterhub.lakehouse.example.com
zeppelin:
  hostname: zeppelin.lakehouse.example.com
```
Eski `keycloak.clients.*.redirectUris` ve `keycloak.ldap.bindCredential` alanları silinir. `platform/values/glue.yaml`: `keycloak: {hostname: https://keycloak.lakehouse.example.com}`. `platform/values/glue-dev.yaml`:
```yaml
keycloak:
  hostname: http://keycloak-service.lakehouse.svc:8080   # dev: iss küme içinden çözülür (Trino OIDC discovery); tarayıcı akışı küme dışından çalışmaz
  devUsers:
  - {username: admin1,   password: admin1-dev,   group: lakehouse-admins}
  - {username: analyst1, password: analyst1-dev, group: lakehouse-analysts}
  - {username: student1, password: student1-dev, group: lakehouse-students}
superset:   {hostname: superset.127.0.0.1.nip.io}
jupyterhub: {hostname: jupyterhub.127.0.0.1.nip.io}
zeppelin:   {hostname: zeppelin.127.0.0.1.nip.io}
```

- [ ] **Step 2: keycloak.yaml + ingress/route host**

`keycloak.yaml`: `hostname: {hostname: {{ .Values.keycloak.hostname }}, strict: …}` aynen (URL kabul edilir); `ingress.yaml`/`route.yaml`'da `host: {{ include "glue.host" .Values.keycloak.hostname }}`.

- [ ] **Step 3: keycloak-realm.yaml**

`spec:` altına:
```yaml
  placeholders:
    TRINO_CLIENT_SECRET:      {secret: {name: {{ .Values.keycloak.clientsSecret }}, key: trino}}
    SUPERSET_CLIENT_SECRET:   {secret: {name: {{ .Values.keycloak.clientsSecret }}, key: superset}}
    JUPYTERHUB_CLIENT_SECRET: {secret: {name: {{ .Values.keycloak.clientsSecret }}, key: jupyterhub}}
    LDAP_BIND_CREDENTIAL:     {secret: {name: {{ .Values.keycloak.clientsSecret }}, key: ldap-bind}}
```
LDAP bloğunda `bindCredential: ["${LDAP_BIND_CREDENTIAL}"]`. Client'lar:
```yaml
    clients:
    - clientId: trino
      enabled: true
      protocol: "openid-connect"
      publicClient: false
      secret: "${TRINO_CLIENT_SECRET}"
      standardFlowEnabled: true
      directAccessGrantsEnabled: {{ if .Values.keycloak.devUsers }}true{{ else }}false{{ end }}   # yalnız dev: e2e password grant
      redirectUris: {{ toJson (concat (list (printf "https://%s/oauth2/callback" .Values.trino.hostname)) .Values.keycloak.extraRedirectUris.trino) }}
      webOrigins: ["+"]
      attributes: {access.token.lifespan: "3600"}
      protocolMappers:
      - name: trino-audience          # Trino OAuth2: aud içinde client-id olmalı (Bearer JWT doğrulaması)
        protocol: "openid-connect"
        protocolMapper: "oidc-audience-mapper"
        config: {"included.client.audience": "trino", "access.token.claim": "true", "id.token.claim": "false"}
    - clientId: superset
      enabled: true
      protocol: "openid-connect"
      publicClient: false
      secret: "${SUPERSET_CLIENT_SECRET}"
      standardFlowEnabled: true
      redirectUris: {{ toJson (concat (list (printf "https://%s/oauth-authorized/keycloak" .Values.superset.hostname)) .Values.keycloak.extraRedirectUris.superset) }}
      webOrigins: ["+"]
      protocolMappers:
      - name: groups                  # FAB keycloak provider: role_keys = userinfo.groups
        protocol: "openid-connect"
        protocolMapper: "oidc-group-membership-mapper"
        config: {"claim.name": "groups", "full.path": "false", "id.token.claim": "true", "access.token.claim": "true", "userinfo.token.claim": "true"}
    - clientId: jupyterhub
      enabled: true
      protocol: "openid-connect"
      publicClient: false
      secret: "${JUPYTERHUB_CLIENT_SECRET}"
      standardFlowEnabled: true
      redirectUris: {{ toJson (concat (list (printf "https://%s/hub/oauth_callback" .Values.jupyterhub.hostname)) .Values.keycloak.extraRedirectUris.jupyterhub) }}
      webOrigins: ["+"]
      protocolMappers: (mevcut groups mapper aynen)
    - clientId: polaris
      … (mevcut; redirectUris: {{ toJson .Values.keycloak.extraRedirectUris.polaris }})
```
Dev kullanıcılar (roles/groups bloklarından sonra):
```yaml
    {{- with .Values.keycloak.devUsers }}
    # DEV-ONLY statik kullanıcılar (e2e password grant); prod'da kullanıcılar AD federasyonundan gelir
    users:
    {{- range . }}
    - username: {{ .username }}
      enabled: true
      email: {{ printf "%s@example.com" .username }}
      firstName: {{ .username | title }}
      lastName: Dev
      emailVerified: true
      credentials: [{type: password, value: {{ .password | quote }}, temporary: false}]
      groups: [{{ printf "/%s" .group | quote }}]
    {{- end }}
    {{- end }}
```

- [ ] **Step 4: unittest**

`glue/tests/keycloak_test.yaml`'daki hostname assert'ini `https://keycloak.lakehouse.example.com` yap; ekle:
```yaml
  - it: realm import carries placeholders and trino audience mapper
    template: keycloak-realm.yaml
    asserts:
      - equal: {path: spec.placeholders.TRINO_CLIENT_SECRET.secret.key, value: trino}
      - contains: {path: spec.realm.clients[0].protocolMappers, content: {name: trino-audience}, any: true}
      - equal: {path: spec.realm.clients[0].directAccessGrantsEnabled, value: false}
      - contains: {path: spec.realm.clients[0].redirectUris, content: https://trino.lakehouse.example.com/oauth2/callback}
  - it: dev users enable password grant
    template: keycloak-realm.yaml
    set: {keycloak.devUsers: [{username: u1, password: p1, group: lakehouse-students}]}
    asserts:
      - equal: {path: spec.realm.clients[0].directAccessGrantsEnabled, value: true}
      - equal: {path: spec.realm.users[0].groups[0], value: /lakehouse-students}
  - it: ingress host strips scheme
    template: ingress.yaml
    set: {ingress.enabled: true}
    asserts:
      - equal: {path: spec.rules[0].host, value: keycloak.lakehouse.example.com}
```
Run: `helm unittest glue` PASS.

- [ ] **Step 5: Canlı doğrulama (Task 1 kümesi, helm modu)**

Realm import mevcut realm'i güncellemez → `kubectl -n lakehouse delete keycloakrealmimport lakehouse-realm` + Keycloak'ta realm'i sil:
```bash
kubectl -n lakehouse exec keycloak-0 -- /opt/keycloak/bin/kcadm.sh config credentials --server http://localhost:8080 --realm master --user admin --password admin
kubectl -n lakehouse exec keycloak-0 -- /opt/keycloak/bin/kcadm.sh delete realms/lakehouse
```
Sonra `helm upgrade --install glue glue -n lakehouse -f platform/values/glue-dev.yaml --wait --timeout 10m`; `kubectl -n lakehouse wait keycloakrealmimport/lakehouse-realm --for=condition=Done --timeout=600s`. Password grant:
```bash
kubectl -n lakehouse run kc-tok --rm -i --restart=Never --image=python:3.13-slim -- python -c "
import json,base64,urllib.request,urllib.parse
d=urllib.parse.urlencode({'grant_type':'password','client_id':'trino','client_secret':'trino-dev-secret','username':'student1','password':'student1-dev','scope':'openid'}).encode()
t=json.load(urllib.request.urlopen('http://keycloak-service.lakehouse.svc:8080/realms/lakehouse/protocol/openid-connect/token',d))['access_token']
p=t.split('.')[1]; c=json.loads(base64.urlsafe_b64decode(p+'=='))
print({k:c.get(k) for k in ('iss','aud','preferred_username','azp')})"
```
Beklenen: `iss == http://keycloak-service.lakehouse.svc:8080/realms/lakehouse`, `aud` içinde `trino`, `preferred_username == student1`. Bulguyu (iss tam metni, aud listesi) F4 notu için kaydet.

- [ ] **Step 6: Commit**

```bash
git add glue/ platform/values/
git commit -m "feat(keycloak): hostname URL, realm placeholders from Secret, trino audience mapper, superset groups claim, derived redirect URIs, dev users

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 3: Polaris sandbox + Trino (OIDC, PASSWORD, rules.json, REST katalog) + e2e trino-path

**Files:**
- Create: `platform/apps/30-trino.yaml`, `platform/values/trino.yaml`, `platform/values/trino-dev.yaml`, `test/e2e/trino-check/check.py`, `test/e2e/trino-check/job.yaml`, `test/e2e/trino-path.sh`
- Modify: `platform/polaris/setup.yaml`, `platform/apps/kustomization.yaml`, `platform/envs/dev/kustomization.yaml`, `bootstrap/bootstrap.sh`, `glue/templates/route.yaml`, `glue/templates/ingress.yaml`, `glue/tests/platform_test.yaml`, `test/e2e/lib.sh`, `test/e2e/run.sh`

**Interfaces:**
- Consumes: `trino-tls`, `trino-shared-secret`, `trino-service-accounts`, `keycloak-clients/trino`, `polaris-trino` (polaris-setup.sh yazar), `lakehouse-ca`.
- Produces: Service `trino` 8080 (http, probe) + 8443 (https); Trino kullanıcıları: servis `superset|zeppelin|e2e` (SELECT), gruplar `lakehouse-admins` (her şey), `lakehouse-analysts` (okuma + `sandbox` şeması sahibi), `lakehouse-students` (okuma; `shop.orders` filtresi `status <> 'shipped'`; `nginx_raw.access_log.remote` maskesi `'x.x.x.x'`). `lib.sh: run_check_job <dir> <name> [args…]`.

- [ ] **Step 1: Polaris setup.yaml**

```yaml
principal_roles: [writers, readers, sandbox_writers]
principals:
  connect:   {type: service, roles: [writers]}
  spark:     {type: service, roles: [writers]}
  trino:     {type: service, roles: [readers, sandbox_writers]}
  notebooks: {type: service, roles: [readers, sandbox_writers]}
catalogs:
  - name: lakehouse
    … (mevcut)
    roles:
      lakehouse_admin: … (mevcut)
      lakehouse_read: … (mevcut)
      lakehouse_sandbox:                   # motor bazlı yazma yalnız sandbox namespace'inde; analist/öğrenci ayrımı Trino rules.json'da
        assign_to: [sandbox_writers]
        privileges:
          namespace:
            sandbox: [NAMESPACE_READ_PROPERTIES, TABLE_LIST, TABLE_CREATE, TABLE_DROP, TABLE_READ_PROPERTIES, TABLE_WRITE_PROPERTIES, TABLE_READ_DATA, TABLE_WRITE_DATA]
    namespaces:
      … (mevcut)
      - name: sandbox
```
Doğrula: `PATH=.venv/bin:$PATH polaris setup apply --dry-run platform/polaris/setup.yaml` hatasız (privilege adları CLI tarafından kabul edilmeli; reddedilen ad varsa Polaris 1.7 `PolarisPrivilege` listesinden düzelt ve F4 notuna yaz). Canlı: `runbooks/scripts/polaris-setup.sh` (idempotent) → çıktı "Creating principal role sandbox_writers" ve grant satırları; `polaris-trino` Secret'ı değişmez.

- [ ] **Step 2: Trino Application + dev patch + bootstrap**

`platform/apps/30-trino.yaml`:
```yaml
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata: {name: trino, namespace: argocd, annotations: {argocd.argoproj.io/sync-wave: "2"}}
spec:
  project: default
  sources:
  - repoURL: https://trinodb.github.io/charts
    chart: trino
    targetRevision: 1.42.2
    helm:
      valueFiles: [$values/platform/values/trino.yaml]
  - repoURL: https://github.com/suhanduman/lakehouse.git
    targetRevision: v2
    ref: values
  destination: {server: https://kubernetes.default.svc, namespace: lakehouse}
  syncPolicy:
    automated: {prune: true, selfHeal: true}
    syncOptions: [CreateNamespace=true, ServerSideApply=true]
```
`platform/envs/dev/kustomization.yaml` patches:
```yaml
- target: {kind: Application, name: trino}
  patch: |-
    - op: add
      path: /spec/sources/0/helm/valueFiles/-
      value: $values/platform/values/trino-dev.yaml
```
`bootstrap.sh` kök patch'lerine `trino` için `/spec/sources/1/repoURL` ve `/spec/sources/1/targetRevision` (polaris kalıbı). Helm modu, polaris satırından SONRA:
```bash
  helm repo add trino https://trinodb.github.io/charts >/dev/null 2>&1 || true; helm repo update trino >/dev/null
  TRINO_VALUES=(-f "$ROOT/platform/values/trino.yaml"); [[ "$ENV" == "dev" ]] && TRINO_VALUES+=(-f "$ROOT/platform/values/trino-dev.yaml")
  helm upgrade --install trino trino/trino --version 1.42.2 -n lakehouse "${TRINO_VALUES[@]}"     # --wait YOK: pod polaris-trino Secret'ını bekler (polaris-setup sonrası)
```

- [ ] **Step 3: platform/values/trino.yaml (prod)**

```yaml
image: {tag: "483"}
server:
  workers: 2
  config:
    authenticationType: OAUTH2,PASSWORD
    https: {enabled: true, port: 8443, keystore: {path: /etc/trino/tls/tls-combined.pem}}   # cert-manager trino-tls (glue tls.yaml)
    query: {maxMemory: 4GB}
  # yalnız coordinator: OAuth2 (Keycloak). issuer = glue keycloak.hostname + /realms/<realm> (KOPYA — runbooks/install.md)
  coordinatorExtraConfig: |
    http-server.authentication.oauth2.issuer=https://keycloak.lakehouse.example.com/realms/lakehouse
    http-server.authentication.oauth2.client-id=trino
    http-server.authentication.oauth2.client-secret=${ENV:OIDC_CLIENT_SECRET}
    http-server.authentication.oauth2.principal-field=preferred_username
    http-server.authentication.oauth2.scopes=openid
    web-ui.authentication.type=oauth2
additionalConfigProperties:
- internal-communication.shared-secret=${ENV:TRINO_SHARED_SECRET}
env:
- {name: TRINO_SHARED_SECRET, valueFrom: {secretKeyRef: {name: trino-shared-secret, key: secret}}}
- {name: OIDC_CLIENT_SECRET, valueFrom: {secretKeyRef: {name: keycloak-clients, key: trino}}}
- {name: POLARIS_CREDENTIAL, valueFrom: {secretKeyRef: {name: polaris-trino, key: credential}}}   # polaris-setup.sh yazar; yoksa pod CreateContainerConfigError ile bekler
- {name: LDAP_BIND_PASSWORD, valueFrom: {secretKeyRef: {name: keycloak-clients, key: ldap-bind}}}
coordinator:
  jvm: {maxHeapSize: 8G}
  config: {query: {maxMemoryPerNode: 1GB}, nodeScheduler: {includeCoordinator: false}}
  additionalExposedPorts: {https: {servicePort: 8443, name: https, port: 8443, protocol: TCP}}
  secretMounts:
  - {name: trino-tls, secretName: trino-tls, path: /etc/trino/tls}
  additionalConfigFiles:
    # prod: gruplar AD'den (Trino >= 473 LDAP group provider). Dev overlay bunu dosya provider'ıyla EZER (auth.groups).
    group-provider.properties: |
      group-provider.name=ldap
      ldap.url=ldaps://ad.example.com:636
      ldap.admin-user=CN=svc-lakehouse,OU=Service,DC=example,DC=com
      ldap.admin-password=${ENV:LDAP_BIND_PASSWORD}
      ldap.user-base-dn=OU=Users,DC=example,DC=com
      ldap.user-search-filter=(sAMAccountName={0})
      ldap.user-member-of-attribute=memberOf
      ldap.group-name-attribute=cn
worker:
  jvm: {maxHeapSize: 8G}
  config: {query: {maxMemoryPerNode: 1GB}}
auth:
  passwordAuthSecret: trino-service-accounts     # key password.db (htpasswd bcrypt): superset, zeppelin (+ dev: e2e)
catalogs:
  lakehouse: |
    connector.name=iceberg
    iceberg.catalog.type=rest
    iceberg.rest-catalog.uri=http://polaris.lakehouse.svc:8181/api/catalog
    iceberg.rest-catalog.warehouse=lakehouse
    iceberg.rest-catalog.security=OAUTH2
    iceberg.rest-catalog.oauth2.credential=${ENV:POLARIS_CREDENTIAL}
    iceberg.rest-catalog.oauth2.scope=PRINCIPAL_ROLE:ALL
    iceberg.rest-catalog.vended-credentials-enabled=true
    fs.native-s3.enabled=true
    s3.endpoint=https://s3.example.com
    s3.region=us-east-1
    s3.path-style-access=true
accessControl:
  type: configmap
  refreshPeriod: 60s
  rules:
    rules.json: |-
      {
        "catalogs": [
          {"group": "lakehouse-admins|lakehouse-analysts", "catalog": "lakehouse|system", "allow": "all"},
          {"group": "lakehouse-students", "catalog": "lakehouse|system", "allow": "read-only"},
          {"user": "superset|zeppelin|e2e", "catalog": "lakehouse|system", "allow": "read-only"}
        ],
        "schemas": [
          {"group": "lakehouse-admins", "owner": true},
          {"group": "lakehouse-analysts", "schema": "sandbox", "owner": true},
          {"owner": false}
        ],
        "tables": [
          {"group": "lakehouse-admins", "privileges": ["SELECT", "INSERT", "DELETE", "UPDATE", "OWNERSHIP"]},
          {"group": "lakehouse-analysts", "schema": "sandbox", "privileges": ["SELECT", "INSERT", "DELETE", "UPDATE", "OWNERSHIP"]},
          {"group": "lakehouse-analysts", "privileges": ["SELECT"]},
          {"group": "lakehouse-students", "schema": "shop", "table": "orders", "privileges": ["SELECT"], "filter": "status <> 'shipped'"},
          {"group": "lakehouse-students", "schema": "nginx_raw", "table": "access_log", "privileges": ["SELECT"],
           "columns": [{"name": "remote", "mask": "'x.x.x.x'"}]},
          {"group": "lakehouse-students", "privileges": ["SELECT"]},
          {"user": "superset|zeppelin|e2e", "privileges": ["SELECT"]},
          {"catalog": "system", "privileges": ["SELECT"]}
        ]
      }
```
`platform/values/trino-dev.yaml`:
```yaml
server:
  workers: 0
  config: {query: {maxMemory: 1GB}}
  coordinatorExtraConfig: |
    http-server.authentication.oauth2.issuer=http://keycloak-service.lakehouse.svc:8080/realms/lakehouse
    http-server.authentication.oauth2.client-id=trino
    http-server.authentication.oauth2.client-secret=${ENV:OIDC_CLIENT_SECRET}
    http-server.authentication.oauth2.principal-field=preferred_username
    http-server.authentication.oauth2.scopes=openid
    web-ui.authentication.type=oauth2
coordinator:
  jvm: {maxHeapSize: 1536M}
  config: {query: {maxMemoryPerNode: 512MB}, nodeScheduler: {includeCoordinator: true}}
  resources: {requests: {cpu: 500m, memory: 1536Mi}, limits: {memory: 2560Mi}}
  additionalConfigFiles: {}          # LDAP group provider'ı kapat -> auth.groups (dosya) devreye girer
auth:
  groups: |
    lakehouse-admins:admin1
    lakehouse-analysts:analyst1
    lakehouse-students:student1
catalogs:
  lakehouse: |
    connector.name=iceberg
    iceberg.catalog.type=rest
    iceberg.rest-catalog.uri=http://polaris.lakehouse.svc:8181/api/catalog
    iceberg.rest-catalog.warehouse=lakehouse
    iceberg.rest-catalog.security=OAUTH2
    iceberg.rest-catalog.oauth2.credential=${ENV:POLARIS_CREDENTIAL}
    iceberg.rest-catalog.oauth2.scope=PRINCIPAL_ROLE:ALL
    iceberg.rest-catalog.vended-credentials-enabled=true
    fs.native-s3.enabled=true
    s3.endpoint=http://minio.lakehouse.svc:9000
    s3.region=us-east-1
    s3.path-style-access=true
```
Render kontrolü: `helm template trino trino/trino --version 1.42.2 -f platform/values/trino.yaml -f platform/values/trino-dev.yaml | grep -E 'include-coordinator|shared-secret|oauth2.issuer|password-file|group-file|https.enabled|group-provider.name'` → `include-coordinator=true`, `shared-secret=${ENV:TRINO_SHARED_SECRET}`, dev issuer, `file.password-file=/etc/trino/auth/password/password.db`, `file.group-file=/etc/trino/auth/group/group.db`, `https.enabled=true`, `group-provider.name=file` (LDAP satırı YOK). Helm map birleştirmesi `additionalConfigFiles: {}` ile prod anahtarını silmiyorsa (LDAP satırı görünüyorsa): LDAP provider'ı `platform/values/trino.yaml`'dan çıkar, `platform/values/trino-ldap.yaml` ayrı dosyaya al ve prod Application `valueFiles`'ına ekle (dev patch eklemez); bulguyu F4 notuna yaz.

- [ ] **Step 4: Route/Ingress (glue)**

`route.yaml`'a (openshift):
```yaml
---
apiVersion: route.openshift.io/v1
kind: Route
metadata: {name: trino, namespace: {{ include "glue.ns" . }}}
spec:
  host: {{ .Values.trino.hostname }}
  to: {kind: Service, name: trino}
  port: {targetPort: https}
  tls:
  {{- if .Values.tls.caBundle }}
    termination: reencrypt
    destinationCACertificate: {{ .Values.tls.caBundle | quote }}
  {{- else }}
    termination: passthrough        # tarayıcı lakehouse-ca'ya güvenmeli (runbooks/install.md)
  {{- end }}
```
`ingress.yaml`'a (vanilla): ayrı Ingress `trino`, host `.Values.trino.hostname`, backend `{service: {name: trino, port: {number: 8443}}}`, `annotations: {nginx.ingress.kubernetes.io/backend-protocol: HTTPS}`. `platform_test.yaml`'a: `route.yaml` `platform: openshift, route.enabled: true` → `containsDocument Route trino`; `tls.caBundle` boşken `spec.tls.termination == passthrough`, `tls.caBundle: "PEM"` iken `reencrypt`.

- [ ] **Step 5: e2e — lib.sh `run_check_job`, trino-check Job, trino-path.sh**

`lib.sh`:
```bash
run_check_job() {  # run_check_job <dir> <name> [args…] -> dir/*.py ConfigMap lakehouse-<name>, dir/job.yaml (JOBNAME, CHECK_ARGS) Job; "^OK" satırları
  local dir="$1" base="$2"; shift 2; local name="$base-$(date +%s)-$RANDOM" q="" a
  kubectl -n "$NS" create configmap "lakehouse-$base" --from-file="$dir/check.py" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
  for a in "$@"; do q+="'$a' "; done
  sed -e "s#JOBNAME#$name#" -e "s#CHECK_ARGS#$q#" "$dir/job.yaml" | kubectl apply -f - >/dev/null
  kubectl -n "$NS" wait --for=condition=complete "job/$name" --timeout=900s >/dev/null || { kubectl -n "$NS" logs "job/$name" --tail=60; return 1; }
  kubectl -n "$NS" logs "job/$name" | grep "^OK"
}
```
`test/e2e/trino-check/job.yaml`:
```yaml
apiVersion: batch/v1
kind: Job
metadata: {name: JOBNAME, namespace: lakehouse}
spec:
  backoffLimit: 0
  ttlSecondsAfterFinished: 900
  template:
    spec:
      restartPolicy: Never
      containers:
      - name: check
        image: python:3.13-slim
        command: ["/bin/sh","-c"]
        args: ["set -e; pip install -q 'trino>=0.339,<0.340' 'requests>=2.32,<3'; python /work/check.py CHECK_ARGS"]
        env:
        - {name: TRINO_HOST, value: trino.lakehouse.svc}
        - {name: CA_FILE, value: /etc/lakehouse-ca/tls.crt}
        - {name: KC_TOKEN_URL, value: http://keycloak-service.lakehouse.svc:8080/realms/lakehouse/protocol/openid-connect/token}
        - {name: KC_CLIENT_SECRET, valueFrom: {secretKeyRef: {name: keycloak-clients, key: trino}}}
        - {name: E2E_PASSWORD, valueFrom: {secretKeyRef: {name: trino-service-accounts, key: e2e}}}
        volumeMounts: [{name: work, mountPath: /work}, {name: ca, mountPath: /etc/lakehouse-ca, readOnly: true}]
      volumes:
      - {name: work, configMap: {name: lakehouse-trino-check}}
      - {name: ca, secret: {secretName: lakehouse-ca, items: [{key: tls.crt, path: tls.crt}]}}
```
`test/e2e/trino-check/check.py`:
```python
"""check.py <shop_rows> <crm_rows> <shop_rows_student>
Trino'ya küme içinden: (1) servis hesabı e2e (Basic) sayımlar + sandbox yazma reddi; (2) analyst1 (Keycloak password grant -> Bearer)
tam okuma, nginx remote maskesiz, sandbox'a CTAS + DROP; (3) student1: shop.orders satır filtresi, remote maskesi 'x.x.x.x', sandbox yazma reddi."""
import os
import re
import sys

import requests
import trino
from trino.auth import BasicAuthentication, JWTAuthentication

HOST, CA = os.environ["TRINO_HOST"], os.environ["CA_FILE"]
shop_rows, crm_rows, shop_rows_student = (int(a) for a in sys.argv[1:4])


def conn(auth):
    return trino.dbapi.connect(host=HOST, port=8443, http_scheme="https", verify=CA, auth=auth, catalog="lakehouse", schema="shop")


def q(c, sql):
    cur = c.cursor()
    cur.execute(sql)
    return cur.fetchall()


def token(user, password):
    r = requests.post(os.environ["KC_TOKEN_URL"], timeout=30, data={
        "grant_type": "password", "client_id": "trino", "client_secret": os.environ["KC_CLIENT_SECRET"],
        "username": user, "password": password, "scope": "openid"})
    r.raise_for_status()
    return r.json()["access_token"]


def expect(cond, msg):
    if not cond:
        print("HATA", msg)
        sys.exit(1)
    print("OK", msg)


def expect_denied(c, sql, msg):
    try:
        q(c, sql)
    except trino.exceptions.TrinoUserError as e:
        expect("Access Denied" in str(e), f"{msg} ({str(e)[:80]})")
        return
    print("HATA", msg, "-> reddedilmedi")
    sys.exit(1)


svc = conn(BasicAuthentication("e2e", os.environ["E2E_PASSWORD"]))
expect(q(svc, "select count(*) from shop.orders")[0][0] == shop_rows, f"e2e shop.orders == {shop_rows}")
expect(q(svc, "select count(*) from crm.customers")[0][0] == crm_rows, f"e2e crm.customers == {crm_rows}")
expect(q(svc, "select count(*) from nginx_raw.access_log")[0][0] >= 3, "e2e nginx_raw.access_log >= 3")
expect_denied(svc, "create table sandbox.e2e_svc as select 1 x", "e2e servis hesabı sandbox yazma reddi")

an = conn(JWTAuthentication(token("analyst1", "analyst1-dev")))
expect(q(an, "select count(*) from shop.orders")[0][0] == shop_rows, "analyst1 shop.orders filtresiz")
remote = q(an, "select remote from nginx_raw.access_log limit 1")[0][0]
expect(re.fullmatch(r"\d+\.\d+\.\d+\.\d+", remote or "") is not None, f"analyst1 remote maskesiz ({remote})")
q(an, "drop table if exists sandbox.e2e_orders")
q(an, "create table sandbox.e2e_orders as select * from shop.orders")
expect(q(an, "select count(*) from sandbox.e2e_orders")[0][0] == shop_rows, "analyst1 sandbox CTAS (Polaris sandbox_writers + Trino owner)")
q(an, "drop table sandbox.e2e_orders")
print("OK analyst1 sandbox DROP")

st = conn(JWTAuthentication(token("student1", "student1-dev")))
expect(q(st, "select count(*) from shop.orders")[0][0] == shop_rows_student, f"student1 satır filtresi status <> 'shipped' -> {shop_rows_student}")
expect(q(st, "select remote from nginx_raw.access_log limit 1")[0][0] == "x.x.x.x", "student1 remote maskesi")
expect_denied(st, "create table sandbox.e2e_student as select 1 x", "student1 sandbox yazma reddi")
print("OK TRINO_OK")
```
`test/e2e/trino-path.sh`:
```bash
#!/usr/bin/env bash
# e2e F4 Trino yolu: pg/mongo/nginx yollarından SONRA (Silver: shop.orders 3 [id1 shipped, id3 new, id4 new] -> öğrenci 2; crm.customers 3).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"; NS=lakehouse
# shellcheck source=/dev/null
source "$ROOT/test/e2e/lib.sh"
echo "== Trino hazır (polaris-trino Secret'ı polaris-setup ile geldi)"
kubectl -n "$NS" rollout status deploy/trino-coordinator --timeout=900s
echo "== servis hesabı + OIDC (analyst1/student1) + rules.json + sandbox"
run_check_job "$ROOT/test/e2e/trino-check" trino-check 3 3 2
echo "E2E F4 TRINO OK"
```
`run.sh`: app döngüsünden `trino`yu ÇIKAR (Secret bekler); `polaris-setup.sh` satırından sonra:
```bash
[[ "$MODE" == "argocd" ]] && kubectl -n argocd wait application/trino --for=jsonpath='{.status.health.status}'=Healthy --timeout=900s
```
`nginx-path.sh` satırından sonra `"$ROOT/test/e2e/trino-path.sh"`.

- [ ] **Step 6: Canlı doğrulama (Task 1 kümesi)**

`bootstrap/bootstrap.sh --env dev --mode helm` (idempotent; yeni satırlar Trino'yu kurar) → `PATH=.venv/bin:$PATH runbooks/scripts/polaris-setup.sh` → `kubectl -n lakehouse rollout status deploy/trino-coordinator --timeout=900s`. Log kontrolü: `kubectl -n lakehouse logs deploy/trino-coordinator | grep -E 'SERVER STARTED|oauth2|Configuration property.*was not used|ERROR'` — "was not used" → yanlış yere düşen özellik (coordinatorExtraConfig ↔ additionalConfigProperties), values'ta düzelt. **Probe kontrolü:** chart liveness `/v1/info` HTTP 8080'de, readiness/startup `health-check` script'i; HTTPS+auth ile bunlar 403 verirse pod Ready olmaz → `kubectl -n lakehouse describe pod -l app.kubernetes.io/component=coordinator | grep -A3 -iE 'liveness|readiness|startup'`. Beklenen: `/v1/info` kimliksiz servis edilir (sorun yok). Aksi hâlde: `http-server.authentication.allow-insecure-over-http=true` **kullanılmaz** (kimliksiz `X-Trino-User` erişimi açar); durumu bulgu olarak yaz, kullanıcıya raporla (chart probe'u kimlikli HTTPS'e izin vermiyorsa upstream issue + geçici olarak `coordinator.livenessProbe.failureThreshold` yükseltilmez — çözüm yoksa iş burada durur). Sonra `test/e2e/trino-path.sh`. Olası hatalar ve düzeltme yeri: `aud` uyuşmazlığı → Keycloak audience mapper (Task 2); `iss` uyuşmazlığı → `keycloak.hostname` ↔ `oauth2.issuer`; `Access Denied` → rules.json sırası (ilk eşleşme kazanır); Polaris `Forbidden` sandbox → privilege listesi (Step 1). Her düzeltme values/setup.yaml'da.

- [ ] **Step 7: Commit**

```bash
git add platform/ bootstrap/bootstrap.sh glue/ test/e2e/
git commit -m "feat(trino): Trino 483 chart app — OAUTH2+PASSWORD over TLS, Polaris REST catalog, rules.json row filter/column mask, sandbox namespace; e2e trino-path

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 4: Superset — operator, CR, CNPG, Keycloak OAuth, Trino bağlantısı + e2e

**Files:**
- Create: `platform/apps/00-superset-operator.yaml`, `glue/templates/superset.yaml`, `glue/tests/superset_test.yaml`, `test/e2e/superset-path.sh`
- Modify: `platform/apps/kustomization.yaml`, `bootstrap/bootstrap.sh`, `glue/templates/cnpg.yaml`, `glue/templates/route.yaml`, `glue/templates/ingress.yaml`, `glue/values.yaml`, `platform/values/glue.yaml`, `platform/values/glue-dev.yaml`, `glue/tests/platform_test.yaml`, `test/e2e/run.sh`

**Interfaces:**
- Consumes: `superset-secret/secret-key`, `superset-db-app/uri` (CNPG), `keycloak-clients/superset`, `trino-service-accounts/superset`, `lakehouse-ca`.
- Produces: `Superset/superset` CR (phase Running), web Service (adı ve etiketleri operator'den — Step 2'de bulunur; Route/Ingress ve e2e bu ada bağlanır), ConfigMap `superset-datasources` (`trino.yaml`).

- [ ] **Step 1: Operator Application + bootstrap**

`platform/apps/00-superset-operator.yaml`: cert-manager kalıbı; `repoURL: ghcr.io/apache/superset-kubernetes-operator/charts`, `chart: superset-operator`, `targetRevision: 0.2.0`, namespace `lakehouse`, wave 0, `valuesObject` yok. kustomization'a ekle. bootstrap.sh helm modu (glue'dan ÖNCE):
```bash
  helm upgrade --install superset-operator oci://ghcr.io/apache/superset-kubernetes-operator/charts/superset-operator --version 0.2.0 -n lakehouse --wait --timeout 5m
```
run.sh app listesine `superset-operator`.

- [ ] **Step 2: CRD'yi canlı incele (tasarım sabitlenmeden)**

Operatörü kur (helm satırı), sonra:
```bash
kubectl explain superset.spec.webServer --recursive | head -80
kubectl explain superset.spec.lifecycle.init --recursive
kubectl explain superset.spec.metastore --recursive
kubectl explain superset.spec.networking --recursive | head -30
```
Not al: podTemplate'te `env`/`volumes`/`volumeMounts`/`resources` yolu; Production ortamında `init.adminUser` zorunlu mu ve parola için `passwordFrom` alanı; CR'ın ürettiği web Service adı ve pod etiketleri (`kubectl -n lakehouse get svc,pod -l app.kubernetes.io/instance=superset` — geçici bir minimal CR uygulayıp gör, sonra sil). Step 3'teki CR bu bulgulara göre uyarlanır (alan yolları operator 0.2.0 tiplerinden: `spec.image.tag`, `spec.replicas`, `spec.environment`, `spec.secretKeyFrom`, `spec.metastore.uriFrom`, `spec.config`, `spec.bootstrapScript`, `spec.webServer.podTemplate`, `spec.lifecycle.{migrate,init}`); sapmalar F4 notuna.

- [ ] **Step 3: values + cnpg + superset.yaml**

`glue/values.yaml`:
```yaml
superset:
  enabled: true
  hostname: superset.lakehouse.example.com
  imageTag: "6.1.0"
  replicas: 1
  secretName: superset-secret          # key: secret-key (SUPERSET SECRET_KEY; Git'e girmez)
  resources: {requests: {cpu: 250m, memory: 768Mi}, limits: {memory: 2Gi}}
  roleMapping: {lakehouse-admins: [Admin], lakehouse-analysts: [Alpha], lakehouse-students: [Gamma]}
cnpg:
  supersetDb: {instances: 1, storageSize: 10Gi, storageClass: ""}
```
`cnpg.yaml`: üçüncü Cluster `superset-db` (`{{- if .Values.superset.enabled }}`, `initdb: {database: superset, owner: superset}`, `cnpg.supersetDb.*`). `platform/values/glue.yaml`: `cnpg.supersetDb: {instances: 2}`; dev: `cnpg.supersetDb: {instances: 1, storageSize: 2Gi}`, `superset.resources: {requests: {cpu: 200m, memory: 512Mi}, limits: {memory: 1536Mi}}`.

`glue/templates/superset.yaml`:
```yaml
{{- if .Values.superset.enabled }}
{{- $ns := include "glue.ns" . }}
apiVersion: v1
kind: ConfigMap
metadata: {name: superset-datasources, namespace: {{ $ns }}}
data:
  # superset legacy-import-datasources -p /app/configs/trino.yaml  (runbooks/user-facing.md; parola SQLALCHEMY_CUSTOM_PASSWORD_STORE ile env'den)
  trino.yaml: |
    databases:
    - database_name: lakehouse
      sqlalchemy_uri: trino://superset@trino.{{ $ns }}.svc:8443/lakehouse
      expose_in_sqllab: true
      allow_ctas: true
      allow_cvas: true
      allow_dml: false
      extra: '{"engine_params": {"connect_args": {"http_scheme": "https", "verify": "/etc/lakehouse-ca/tls.crt"}}}'
      tables: []
---
apiVersion: superset.apache.org/v1alpha1
kind: Superset
metadata:
  name: superset
  namespace: {{ $ns }}
  annotations: {argocd.argoproj.io/sync-wave: "2"}
spec:
  environment: Production                  # sırlar yalnız *From Secret referansı
  image: {tag: {{ .Values.superset.imageTag | quote }}}
  replicas: {{ .Values.superset.replicas }}
  secretKeyFrom: {name: {{ .Values.superset.secretName }}, key: secret-key}
  metastore:
    uriFrom: {name: superset-db-app, key: uri}      # CNPG: postgresql://superset:…@superset-db-rw:5432/superset
  # Valkey/Redis YOK (plan R9): SimpleCache, worker/beat yok. Alerts&Reports için runbooks/user-facing.md
  bootstrapScript: |
    #!/bin/bash
    pip install --no-cache-dir 'trino[sqlalchemy]>=0.339,<0.340' 'authlib>=1.6,<2'
  config: |
    import os
    from flask_appbuilder.security.manager import AUTH_OAUTH
    ENABLE_PROXY_FIX = True
    CACHE_CONFIG = {"CACHE_TYPE": "SimpleCache", "CACHE_DEFAULT_TIMEOUT": 300}
    DATA_CACHE_CONFIG = CACHE_CONFIG
    FILTER_STATE_CACHE_CONFIG = CACHE_CONFIG
    EXPLORE_FORM_DATA_CACHE_CONFIG = CACHE_CONFIG
    AUTH_TYPE = AUTH_OAUTH
    AUTH_USER_REGISTRATION = True
    AUTH_USER_REGISTRATION_ROLE = "Public"
    AUTH_ROLES_SYNC_AT_LOGIN = True
    AUTH_ROLES_MAPPING = {{ toJson .Values.superset.roleMapping }}
    _KC = os.environ["KEYCLOAK_URL"] + "/realms/" + os.environ["KEYCLOAK_REALM"]
    OAUTH_PROVIDERS = [{
        "name": "keycloak", "icon": "fa-key", "token_key": "access_token",
        "remote_app": {
            "client_id": "superset", "client_secret": os.environ["OIDC_CLIENT_SECRET"],
            "api_base_url": _KC + "/protocol/",                 # FAB: GET openid-connect/userinfo (göreli yol)
            "server_metadata_url": _KC + "/.well-known/openid-configuration",
            "client_kwargs": {"scope": "openid email profile"},
        }}]
    def _trino_password(url):                                 # URI'de parola yok; Trino servis hesabı env'den
        return os.environ["TRINO_PASSWORD"] if url.drivername.startswith("trino") else None
    SQLALCHEMY_CUSTOM_PASSWORD_STORE = _trino_password
  webServer:
    podTemplate:
      spec:
        containers:
        - name: superset
          resources: {{ toJson .Values.superset.resources }}
          env:
          - {name: KEYCLOAK_URL, value: {{ .Values.keycloak.hostname | quote }}}
          - {name: KEYCLOAK_REALM, value: {{ .Values.keycloak.realm.name | quote }}}
          - {name: OIDC_CLIENT_SECRET, valueFrom: {secretKeyRef: {name: {{ .Values.keycloak.clientsSecret }}, key: superset}}}
          - {name: TRINO_PASSWORD, valueFrom: {secretKeyRef: {name: trino-service-accounts, key: superset}}}
          - {name: REQUESTS_CA_BUNDLE, value: /etc/lakehouse-ca/tls.crt}
          volumeMounts:
          - {name: lakehouse-ca, mountPath: /etc/lakehouse-ca, readOnly: true}
          - {name: datasources, mountPath: /app/configs, readOnly: true}
        volumes:
        - {name: lakehouse-ca, secret: {secretName: lakehouse-ca, items: [{key: tls.crt, path: tls.crt}]}}
        - {name: datasources, configMap: {name: superset-datasources}}
  lifecycle:
    migrate: {}
    init: {}                                  # admin kullanıcı yok: yönetici AUTH_ROLES_MAPPING (lakehouse-admins -> Admin) ile OIDC'den gelir
{{- end }}
```
Step 2 bulgularına göre: `podTemplate` yolu / konteyner adı / `init: {}` kabulü düzeltilir (ör. `init` `adminUser` zorunluysa Production'da `adminUser.passwordFrom: {name: superset-secret, key: admin-password}` ve dev-secrets `superset-secret`'a `admin-password: admin1-dev` eklenir). Route/Ingress: `superset.hostname` → web Service (Step 2 adı, port 8088), edge/HTTP. `platform_test.yaml` "renders two CNPG clusters" → üç; `superset.enabled: false` ile iki.

- [ ] **Step 4: unittest**

`glue/tests/superset_test.yaml`:
```yaml
suite: superset
templates: [superset.yaml]
tests:
  - it: renders Superset CR in Production mode with secret refs
    asserts:
      - containsDocument: {kind: Superset, apiVersion: superset.apache.org/v1alpha1, name: superset}
      - equal: {path: spec.environment, value: Production, documentIndex: 1}
      - equal: {path: spec.metastore.uriFrom.name, value: superset-db-app, documentIndex: 1}
      - matchRegex: {path: spec.bootstrapScript, pattern: "trino\\[sqlalchemy\\]", documentIndex: 1}
      - matchRegex: {path: spec.config, pattern: "AUTH_TYPE = AUTH_OAUTH", documentIndex: 1}
  - it: datasources file targets trino over https
    asserts:
      - matchRegex: {path: data["trino.yaml"], pattern: "trino://superset@trino.lakehouse.svc:8443/lakehouse", documentIndex: 0}
  - it: disabled renders nothing
    set: {superset.enabled: false}
    asserts:
      - hasDocuments: {count: 0}
```

- [ ] **Step 5: e2e superset-path.sh**

```bash
#!/usr/bin/env bash
# e2e F4 Superset yolu: CR Running -> /health -> login sayfasında keycloak -> Trino bağlantısı (legacy-import + test-db + sorgu; TLS + servis hesabı)
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"; NS=lakehouse
echo "== Superset CR"
for _ in $(seq 1 90); do [[ "$(kubectl -n "$NS" get superset superset -o jsonpath='{.status.phase}' 2>/dev/null)" == "Running" ]] && break; sleep 10; done
[[ "$(kubectl -n "$NS" get superset superset -o jsonpath='{.status.phase}')" == "Running" ]] || { kubectl -n "$NS" describe superset superset | tail -30; exit 1; }
# etiketler Task 4 Step 2'de doğrulanır (operator 0.2.0: app.kubernetes.io/instance=<cr>, component=web-server bekleniyor)
POD=$(kubectl -n "$NS" get pod -l app.kubernetes.io/instance=superset,app.kubernetes.io/component=web-server -o jsonpath='{.items[0].metadata.name}')
kubectl -n "$NS" wait --for=condition=Ready "pod/$POD" --timeout=600s
echo "== health + login"
kubectl -n "$NS" exec "$POD" -- python -c "
import urllib.request as u
assert u.urlopen('http://localhost:8088/health').read() == b'OK'
html = u.urlopen('http://localhost:8088/login/').read().decode()
assert 'keycloak' in html.lower(), html[:500]
print('OK superset health + keycloak login')"
echo "== Trino bağlantısı (deklaratif import + test-db + sorgu)"
kubectl -n "$NS" exec "$POD" -- superset legacy-import-datasources -p /app/configs/trino.yaml
kubectl -n "$NS" exec "$POD" -- sh -c 'superset test-db "trino://superset:${TRINO_PASSWORD}@trino.lakehouse.svc:8443/lakehouse" -c "{\"http_scheme\": \"https\", \"verify\": \"/etc/lakehouse-ca/tls.crt\"}"' | tail -20
kubectl -n "$NS" exec "$POD" -- python -c "
from superset.app import create_app
app = create_app()
with app.app_context():
    from superset import db
    from superset.models.core import Database
    d = db.session.query(Database).filter_by(database_name='lakehouse').one()
    with d.get_sqla_engine() as e:
        n = e.connect().execute(__import__('sqlalchemy').text('select count(*) from shop.orders')).scalar()
    assert n == 3, n
    print('OK superset -> trino shop.orders == 3 (SQLALCHEMY_CUSTOM_PASSWORD_STORE + TLS)')"
echo "E2E F4 SUPERSET OK"
```
`run.sh`: trino-path'ten sonra `superset-path.sh`.

- [ ] **Step 6: Canlı doğrulama** — `bootstrap.sh --env dev --mode helm` (operator + glue upgrade), `kubectl -n lakehouse get superset -w` → Running; `superset-path.sh`. Olası: `bootstrapScript` pip süresi (pod başına ~1 dk readiness gecikmesi), `legacy-import-datasources` şema farkı (`extra` string ↔ dict), `get_sqla_engine` API adı (6.x'te `get_sqla_engine_with_context` ise onu kullan). Tam OIDC tarayıcı akışı dev'de mümkün değil (Keycloak hostname küme içi) → pre-ship; login sayfasındaki provider butonu ve `server_metadata_url` erişimi (`kubectl logs` — Authlib metadata hatası yok) yeterli.

- [ ] **Step 7: Commit**

```bash
git add platform/ bootstrap/bootstrap.sh glue/ test/e2e/
git commit -m "feat(superset): Apache Superset Kubernetes Operator 0.2.0 + Superset CR (Keycloak OAuth, Trino over TLS via service account, no Redis), CNPG superset-db; e2e superset-path

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 5: JupyterHub (z2jh) — Keycloak OAuth, pyspark-notebook + PyIceberg/Trino, e2e spawn

**Files:**
- Create: `platform/apps/30-jupyterhub.yaml`, `platform/values/jupyterhub.yaml`, `platform/values/jupyterhub-dev.yaml`, `test/e2e/jupyterhub-path.sh`
- Modify: `platform/apps/kustomization.yaml`, `platform/envs/dev/kustomization.yaml`, `bootstrap/bootstrap.sh`, `glue/templates/route.yaml`, `glue/templates/ingress.yaml`, `test/e2e/run.sh`

**Interfaces:**
- Consumes: `keycloak-clients/jupyterhub`, `polaris-notebooks`, `lakehouse-ca`, `trino-service-accounts/e2e` (yalnız e2e).
- Produces: Service `proxy-public` (80), hub `/hub/health`; dev e2e servisi `e2e` (token `e2e-dev-token-0123456789abcdef0123456789abcdef`, roller `admin:users`, `admin:servers`, `read:servers`).

- [ ] **Step 1: Application + dev patch + bootstrap**

`platform/apps/30-jupyterhub.yaml`: Trino kalıbı; `repoURL: https://hub.jupyter.org/helm-chart/`, `chart: jupyterhub`, `targetRevision: 4.4.2`, `valueFiles: [$values/platform/values/jupyterhub.yaml]`, wave 2. Dev patch (Task 3 Step 2 kalıbı) `$values/platform/values/jupyterhub-dev.yaml` ekler. bootstrap kök patch'i (`/spec/sources/1/…`). Helm modu:
```bash
  helm repo add jupyterhub https://hub.jupyter.org/helm-chart/ >/dev/null 2>&1 || true; helm repo update jupyterhub >/dev/null
  JH_VALUES=(-f "$ROOT/platform/values/jupyterhub.yaml"); [[ "$ENV" == "dev" ]] && JH_VALUES+=(-f "$ROOT/platform/values/jupyterhub-dev.yaml")
  helm upgrade --install jupyterhub jupyterhub/jupyterhub --version 4.4.2 -n lakehouse "${JH_VALUES[@]}" --wait --timeout 10m
```
run.sh app listesine `jupyterhub`.

- [ ] **Step 2: values**

`platform/values/jupyterhub.yaml`:
```yaml
# Keycloak OAuth (client secret env'den), pyspark-notebook + PyIceberg/Trino, kişisel PVC (F.1.2).
# Hostname KOPYALARI (runbooks/install.md): oauth_callback_url <- glue jupyterhub.hostname; authorize/token/userdata_url <- glue keycloak.hostname
hub:
  config:
    JupyterHub: {authenticator_class: generic-oauth}
    Authenticator: {enable_auth_state: true}
    GenericOAuthenticator:
      oauth_callback_url: https://jupyterhub.lakehouse.example.com/hub/oauth_callback
      authorize_url: https://keycloak.lakehouse.example.com/realms/lakehouse/protocol/openid-connect/auth
      token_url: https://keycloak.lakehouse.example.com/realms/lakehouse/protocol/openid-connect/token
      userdata_url: https://keycloak.lakehouse.example.com/realms/lakehouse/protocol/openid-connect/userinfo
      login_service: Keycloak
      username_claim: preferred_username
      scope: [openid, profile, email]
      manage_groups: true
      auth_state_groups_key: oauth_user.groups
      allowed_groups: [lakehouse-admins, lakehouse-analysts, lakehouse-students]
      admin_groups: [lakehouse-admins]
  extraEnv:
    OAUTH_CLIENT_ID: {value: jupyterhub}
    OAUTH_CLIENT_SECRET: {valueFrom: {secretKeyRef: {name: keycloak-clients, key: jupyterhub}}}
  resources: {requests: {cpu: 100m, memory: 256Mi}, limits: {memory: 1Gi}}
proxy:
  service: {type: ClusterIP}          # dış erişim glue Route/Ingress (proxy-public:80)
  chp: {resources: {requests: {cpu: 100m, memory: 128Mi}, limits: {memory: 512Mi}}}
singleuser:
  image: {name: quay.io/jupyter/pyspark-notebook, tag: spark-4.1.2}
  lifecycleHooks:
    postStart:
      exec: {command: ["sh", "-c", "pip install -q 'pyiceberg[s3fs,pyarrow]>=0.10,<0.11' 'trino>=0.339,<0.340'"]}   # her spawn'da (~1 dk, PyPI egress) — özel imaj yok
  storage:
    type: dynamic
    capacity: 10Gi
    extraVolumes: [{name: lakehouse-ca, secret: {secretName: lakehouse-ca, items: [{key: tls.crt, path: tls.crt}]}}]
    extraVolumeMounts: [{name: lakehouse-ca, mountPath: /etc/lakehouse-ca, readOnly: true}]
  memory: {guarantee: 1G, limit: 4G}
  cpu: {guarantee: 0.5, limit: 2}
  extraEnv:
    POLARIS_URI: http://polaris.lakehouse.svc:8181/api/catalog
    POLARIS_CREDENTIAL: {valueFrom: {secretKeyRef: {name: polaris-notebooks, key: credential}}}   # paylaşımlı notebooks principal (spec §14)
    S3_ENDPOINT: https://s3.example.com
    TRINO_HOST: trino.lakehouse.svc
    REQUESTS_CA_BUNDLE: /etc/lakehouse-ca/tls.crt
scheduling: {userScheduler: {enabled: true}}
prePuller: {hook: {enabled: true}, continuous: {enabled: true}}
ingress: {enabled: false}
```
`platform/values/jupyterhub-dev.yaml`:
```yaml
hub:
  config:
    GenericOAuthenticator:
      oauth_callback_url: http://jupyterhub.127.0.0.1.nip.io/hub/oauth_callback
      authorize_url: http://keycloak-service.lakehouse.svc:8080/realms/lakehouse/protocol/openid-connect/auth
      token_url: http://keycloak-service.lakehouse.svc:8080/realms/lakehouse/protocol/openid-connect/token
      userdata_url: http://keycloak-service.lakehouse.svc:8080/realms/lakehouse/protocol/openid-connect/userinfo
  services:
    e2e: {apiToken: e2e-dev-token-0123456789abcdef0123456789abcdef}   # DEV-ONLY: e2e REST (kullanıcı yarat + sunucu başlat)
  loadRoles:
    e2e: {description: e2e harness, scopes: [admin:users, admin:servers, read:servers], services: [e2e]}
singleuser:
  memory: {guarantee: 512M, limit: 2G}
  cpu: {guarantee: 0.2, limit: 1}
  storage: {capacity: 1Gi}
  extraEnv: {S3_ENDPOINT: http://minio.lakehouse.svc:9000}
scheduling: {userScheduler: {enabled: false}}
prePuller: {hook: {enabled: false}, continuous: {enabled: false}}   # kind: sync'i imaj çekişine bağlama; spawn'da çekilir
```
Route/Ingress (glue): `jupyterhub.hostname` → `proxy-public:80`, edge/HTTP. Render kontrolü: `helm template jupyterhub jupyterhub/jupyterhub --version 4.4.2 -f platform/values/jupyterhub.yaml -f platform/values/jupyterhub-dev.yaml | grep -E 'allowed_groups|OAUTH_CLIENT_SECRET|e2e-dev-token' ` → üçü de var.

- [ ] **Step 3: e2e jupyterhub-path.sh**

```bash
#!/usr/bin/env bash
# e2e F4 JupyterHub yolu: hub health -> e2e servisi ile kullanıcı yarat + sunucu başlat (gerçek pyspark-notebook imajı + postStart pip)
# -> pod içinde pyiceberg (Polaris notebooks principal) + trino (TLS) -> sunucuyu durdur, kullanıcıyı sil.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"; NS=lakehouse; TOK=e2e-dev-token-0123456789abcdef0123456789abcdef
kubectl -n "$NS" rollout status deploy/hub --timeout=600s; kubectl -n "$NS" rollout status deploy/proxy --timeout=600s
kubectl -n "$NS" port-forward svc/proxy-public 18080:80 >/dev/null 2>&1 & PF=$!; trap 'kill $PF 2>/dev/null' EXIT; sleep 3
H() { curl -sS -H "Authorization: token $TOK" "$@"; }
[[ "$(curl -s -o /dev/null -w '%{http_code}' localhost:18080/hub/health)" == "200" ]] && echo "OK hub health"
H -X POST localhost:18080/hub/api/users/e2e -o /dev/null -w 'user create %{http_code}\n'
H -X POST localhost:18080/hub/api/users/e2e/server -o /dev/null -w 'server start %{http_code}\n'
for _ in $(seq 1 90); do [[ "$(H localhost:18080/hub/api/users/e2e | jq -r '.servers[""].ready')" == "true" ]] && break; sleep 10; done
[[ "$(H localhost:18080/hub/api/users/e2e | jq -r '.servers[""].ready')" == "true" ]] || { kubectl -n "$NS" describe pod jupyter-e2e | tail -30; kubectl -n "$NS" logs deploy/hub --tail=40; exit 1; }
echo "OK jupyter-e2e ready (imaj + postStart pip)"
E2E_PW=$(kubectl -n "$NS" get secret trino-service-accounts -o jsonpath='{.data.e2e}' | base64 -d)
kubectl -n "$NS" exec jupyter-e2e -- python -c "
import os, trino
from pyiceberg.catalog import load_catalog
c = load_catalog('lakehouse', type='rest', uri=os.environ['POLARIS_URI'], warehouse='lakehouse', credential=os.environ['POLARIS_CREDENTIAL'], scope='PRINCIPAL_ROLE:ALL')
assert ('shop',) in c.list_namespaces(), c.list_namespaces(); print('OK pyiceberg -> Polaris (notebooks principal)')
conn = trino.dbapi.connect(host=os.environ['TRINO_HOST'], port=8443, http_scheme='https', verify='/etc/lakehouse-ca/tls.crt', auth=trino.auth.BasicAuthentication('e2e', '$E2E_PW'), catalog='lakehouse')
cur = conn.cursor(); cur.execute('select count(*) from shop.orders'); assert cur.fetchone()[0] == 3; print('OK trino client (TLS) shop.orders == 3')"
H -X DELETE localhost:18080/hub/api/users/e2e/server -o /dev/null -w 'server stop %{http_code}\n'
for _ in $(seq 1 30); do [[ "$(H localhost:18080/hub/api/users/e2e | jq -r '.servers | length')" == "0" ]] && break; sleep 5; done
H -X DELETE localhost:18080/hub/api/users/e2e -o /dev/null -w 'user delete %{http_code}\n'
echo "E2E F4 JUPYTERHUB OK"
```
`run.sh`: superset-path'ten sonra. (Not defteri kullanıcılarının Trino'ya kendi OIDC kimliğiyle erişimi `trino.auth.OAuth2Authentication` ile tarayıcı gerektirir → runbooks/user-facing.md; e2e servis hesabıyla TLS yolunu kanıtlar.)

- [ ] **Step 4: Canlı doğrulama** — helm modu kurulum, `jupyterhub-path.sh`. Olası: `allowed_groups`/`manage_groups` config hatası (hub CrashLoop → `kubectl -n lakehouse logs deploy/hub`), PVC bekleyen `jupyter-e2e` (kind default StorageClass `standard` var), postStart süresi (90×10 s bütçe), imaj çekme süresi (ilk kez ~3–4 GB).

- [ ] **Step 5: Commit**

```bash
git add platform/ bootstrap/bootstrap.sh glue/ test/e2e/
git commit -m "feat(jupyterhub): z2jh 4.4.2 app — Keycloak generic OAuth (groups), pyspark-notebook spark-4.1.2 + pyiceberg/trino postStart, personal PVC; e2e spawn path

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 6: Zeppelin — Deployment + PVC, Shiro, Trino JDBC interpreter tohumu + e2e

**Files:**
- Create: `glue/templates/zeppelin.yaml`, `glue/files/zeppelin/interpreter.json`, `glue/tests/zeppelin_test.yaml`, `runbooks/zeppelin/shiro-ad.ini`, `test/e2e/zeppelin-path.sh`
- Modify: `glue/templates/dev-secrets.yaml` (zeppelin-interpreter bloğu), `glue/templates/route.yaml`, `glue/templates/ingress.yaml`, `glue/values.yaml`, `platform/values/glue-dev.yaml`, `test/e2e/run.sh`

**Interfaces:**
- Consumes: `zeppelin-shiro/shiro.ini`, `zeppelin-interpreter/interpreter.json`, `lakehouse-ca`.
- Produces: Service `zeppelin:8080`; REST `POST /api/login` (userName/password), `/api/notebook`, `/api/notebook/run/{note}/{paragraph}`.

- [ ] **Step 1: interpreter.json tohumu — gerçek şemadan**

Zeppelin'in ürettiği dosyadan türet (uydurma değil):
```bash
podman run --rm -d --name z -e ZEPPELIN_ADDR=0.0.0.0 apache/zeppelin:0.12.1 && sleep 60 && podman exec z cat /opt/zeppelin/conf/interpreter.json > /tmp/interpreter.json; podman rm -f z
```
`jdbc` girdisini al; `glue/files/zeppelin/interpreter.json` = `{"interpreterSettings": {"jdbc": <girdi>}, "interpreterBindings": {}, "interpreterRepositories": <dosyadaki liste>}`; `jdbc.properties` içinde `default.driver.value = "io.trino.jdbc.TrinoDriver"`, `default.url.value = "{{ .url }}"`, `default.user.value = "{{ .user }}"`, `default.password.value = "{{ .password }}"`; `jdbc.dependencies = [{"groupArtifactVersion": "io.trino:trino-jdbc:{{ .Values.zeppelin.trinoJdbcVersion }}", "local": false}]`. Eksik interpreter'ları Zeppelin şablonlardan tamamlar (Step 5'te doğrulanır). dev-secrets.yaml'a ekle:
```yaml
---
apiVersion: v1
kind: Secret
metadata: {name: zeppelin-interpreter, namespace: {{ $ns }}}
stringData:
  interpreter.json: |
{{ tpl (.Files.Get "files/zeppelin/interpreter.json") (dict "Values" .Values "Template" .Template "url" (printf "jdbc:trino://trino.%s.svc:8443/lakehouse?SSL=true&SSLTrustStorePath=/etc/lakehouse-ca/tls.crt" $ns) "user" "zeppelin" "password" "zeppelin-dev") | indent 4 }}
```

- [ ] **Step 2: values + zeppelin.yaml**

`glue/values.yaml` `zeppelin:` bloğu (hostname Task 2'de eklendi):
```yaml
zeppelin:
  enabled: true
  hostname: zeppelin.lakehouse.example.com
  image: apache/zeppelin:0.12.1
  storageSize: 10Gi
  storageClass: ""
  mem: "-Xmx1024m"                 # ZEPPELIN_MEM (server JVM); dev: -Xmx768m
  intpMem: "-Xmx1024m"             # ZEPPELIN_INTP_MEM (interpreter JVM); dev: -Xmx512m
  trinoJdbcVersion: "483"          # interpreter dependencies (Maven Central, ilk çalıştırmada indirilir)
  shiroSecret: zeppelin-shiro                # key shiro.ini — prod: runbooks/zeppelin/shiro-ad.ini (AD LDAPS), dev: dev-secrets
  interpreterSecret: zeppelin-interpreter    # key interpreter.json — Trino JDBC url/user/password (files/zeppelin/interpreter.json şablonu)
  resources: {requests: {cpu: 250m, memory: 1Gi}, limits: {memory: 2560Mi}}
```
`glue/templates/zeppelin.yaml`:
```yaml
{{- if .Values.zeppelin.enabled }}
{{- $ns := include "glue.ns" . }}{{ $z := .Values.zeppelin }}
apiVersion: v1
kind: PersistentVolumeClaim
metadata: {name: zeppelin-data, namespace: {{ $ns }}}
spec:
  accessModes: [ReadWriteOnce]
  resources: {requests: {storage: {{ $z.storageSize }}}}
{{- with $z.storageClass }}
  storageClassName: {{ . | quote }}
{{- end }}
---
apiVersion: apps/v1
kind: Deployment
metadata: {name: zeppelin, namespace: {{ $ns }}, annotations: {argocd.argoproj.io/sync-wave: "2"}}
spec:
  replicas: 1
  strategy: {type: Recreate}                 # RWO PVC
  selector: {matchLabels: {app: zeppelin}}
  template:
    metadata: {labels: {app: zeppelin}}
    spec:
      {{- if ne (include "glue.isOpenShift" .) "true" }}
      securityContext: {fsGroup: 1000}       # imaj uid 1000 / gid 0; OpenShift SCC kendi gid'ini verir
      {{- end }}
      # interpreter.json her açılışta Zeppelin tarafından YENİDEN YAZILIR (atomik temp+rename) -> salt-okunur mount olmaz;
      # ZEPPELIN_CONFIG_FS_DIR PVC'de, tohum yalnız dosya yokken kopyalanır (plan R6). Tohumu değiştirmek: PVC'deki dosyayı sil ya da UI.
      initContainers:
      - name: seed
        image: {{ $z.image }}
        command: ["sh", "-c", "mkdir -p /data/conf /data/notebook /data/local-repo && ([ -f /data/conf/interpreter.json ] || cp /seed/interpreter.json /data/conf/interpreter.json)"]
        volumeMounts: [{name: data, mountPath: /data}, {name: seed, mountPath: /seed, readOnly: true}]
      containers:
      - name: zeppelin
        image: {{ $z.image }}
        ports: [{name: http, containerPort: 8080}]
        env:
        - {name: ZEPPELIN_ADDR, value: "0.0.0.0"}
        - {name: ZEPPELIN_PORT, value: "8080"}
        - {name: ZEPPELIN_NOTEBOOK_DIR, value: /data/notebook}
        - {name: ZEPPELIN_CONFIG_FS_DIR, value: /data/conf}
        - {name: ZEPPELIN_DEP_LOCALREPO, value: /data/local-repo}
        - {name: ZEPPELIN_MEM, value: {{ $z.mem | quote }}}
        - {name: ZEPPELIN_INTP_MEM, value: {{ $z.intpMem | quote }}}
        resources: {{ toJson $z.resources }}
        readinessProbe: {httpGet: {path: /api/version, port: http}, initialDelaySeconds: 20, periodSeconds: 10}
        livenessProbe: {httpGet: {path: /api/version, port: http}, initialDelaySeconds: 90, periodSeconds: 20}
        volumeMounts:
        - {name: data, mountPath: /data}
        - {name: shiro, mountPath: /opt/zeppelin/conf/shiro.ini, subPath: shiro.ini, readOnly: true}
        - {name: lakehouse-ca, mountPath: /etc/lakehouse-ca, readOnly: true}
      volumes:
      - {name: data, persistentVolumeClaim: {claimName: zeppelin-data}}
      - {name: seed, secret: {secretName: {{ $z.interpreterSecret }}}}
      - {name: shiro, secret: {secretName: {{ $z.shiroSecret }}}}
      - {name: lakehouse-ca, secret: {secretName: lakehouse-ca, items: [{key: tls.crt, path: tls.crt}]}}
---
apiVersion: v1
kind: Service
metadata: {name: zeppelin, namespace: {{ $ns }}}
spec: {selector: {app: zeppelin}, ports: [{name: http, port: 8080, targetPort: http}]}
{{- end }}
```
Dev (`glue-dev.yaml`): `zeppelin: {hostname: zeppelin.127.0.0.1.nip.io, mem: "-Xmx768m", intpMem: "-Xmx512m", storageSize: 2Gi, resources: {requests: {cpu: 200m, memory: 768Mi}, limits: {memory: 1536Mi}}}`. Route/Ingress `zeppelin.hostname` → `zeppelin:8080`, edge/HTTP. `runbooks/zeppelin/shiro-ad.ini`: shiro.ini.template AD bloğu (`activeDirectoryRealm = org.apache.zeppelin.realm.ActiveDirectoryGroupRealm`, `systemUsername/systemPassword`, `searchBase`, `url = ldaps://ad.example.com:636`, `groupRolesMap` `"CN=lakehouse-admins,…":"admin","CN=lakehouse-analysts,…":"analyst","CN=lakehouse-students,…":"student"`, `principalSuffix`, `authorizationCachingEnabled = false`) + dev'deki `[urls]` bloğu + `[roles]`; başında `kubectl -n lakehouse create secret generic zeppelin-shiro --from-file=shiro.ini=shiro-ad.ini` notu.

- [ ] **Step 3: unittest**

`glue/tests/zeppelin_test.yaml`:
```yaml
suite: zeppelin
templates: [zeppelin.yaml, dev-secrets.yaml]
tests:
  - it: deployment seeds interpreter.json into writable config dir
    template: zeppelin.yaml
    asserts:
      - containsDocument: {kind: Deployment, apiVersion: apps/v1, name: zeppelin}
      - contains: {path: spec.template.spec.containers[0].env, content: {name: ZEPPELIN_CONFIG_FS_DIR, value: /data/conf}, documentIndex: 1}
      - equal: {path: spec.template.spec.initContainers[0].name, value: seed, documentIndex: 1}
      - equal: {path: spec.template.spec.securityContext.fsGroup, value: 1000, documentIndex: 1}
  - it: openshift leaves fsGroup to SCC
    template: zeppelin.yaml
    set: {platform: openshift}
    asserts:
      - isNull: {path: spec.template.spec.securityContext, documentIndex: 1}
  - it: disabled renders nothing
    template: zeppelin.yaml
    set: {zeppelin.enabled: false}
    asserts:
      - hasDocuments: {count: 0}
  - it: dev interpreter seed targets trino jdbc over TLS
    template: dev-secrets.yaml
    set: {components.devSecrets: true}
    asserts:
      - matchRegex: {path: stringData["interpreter.json"], pattern: "io.trino:trino-jdbc:483", documentIndex: 5}
      - matchRegex: {path: stringData["interpreter.json"], pattern: "SSL=true", documentIndex: 5}
```

- [ ] **Step 4: e2e zeppelin-path.sh**

```bash
#!/usr/bin/env bash
# e2e F4 Zeppelin yolu: Shiro login (dev users) -> not + %jdbc paragrafı -> senkron çalıştır (Trino JDBC Maven'den iner, TLS + servis hesabı) -> sonuç 3
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"; NS=lakehouse
kubectl -n "$NS" rollout status deploy/zeppelin --timeout=900s
kubectl -n "$NS" port-forward svc/zeppelin 18081:8080 >/dev/null 2>&1 & PF=$!; trap 'kill $PF 2>/dev/null' EXIT; sleep 3
CJ=$(mktemp)
curl -sS -c "$CJ" -d 'userName=analyst1' -d 'password=analyst1-dev' localhost:18081/api/login | jq -e '.status=="OK"' >/dev/null && echo "OK zeppelin shiro login analyst1"
[[ "$(curl -sS -o /dev/null -w '%{http_code}' -d 'userName=student1' -d 'password=wrong' localhost:18081/api/login)" == "403" ]] && echo "OK zeppelin wrong password 403"
NOTE=$(curl -sS -b "$CJ" -H 'Content-Type: application/json' -d '{"name":"e2e/trino","paragraphs":[{"title":"count","text":"%jdbc select count(*) c from shop.orders"}]}' localhost:18081/api/notebook | jq -r .body)
PARA=$(curl -sS -b "$CJ" "localhost:18081/api/notebook/$NOTE" | jq -r '.body.paragraphs[0].id')
# ilk çalıştırma trino-jdbc'yi indirir (dakikalar); senkron run
OUT=$(curl -sS -b "$CJ" --max-time 900 -X POST "localhost:18081/api/notebook/run/$NOTE/$PARA")
echo "$OUT" | jq -e '.body.code=="SUCCESS"' >/dev/null || { echo "$OUT" | head -c 2000; kubectl -n "$NS" logs deploy/zeppelin --tail=60; exit 1; }
echo "$OUT" | jq -r '.body.msg[0].data' | grep -qx '3' && echo "OK zeppelin %jdbc -> trino shop.orders == 3"
curl -sS -b "$CJ" -X DELETE "localhost:18081/api/notebook/$NOTE" >/dev/null
echo "E2E F4 ZEPPELIN OK"
```
`run.sh`: jupyterhub-path'ten sonra.

- [ ] **Step 5: Canlı doğrulama** — glue upgrade; `kubectl -n lakehouse logs deploy/zeppelin | grep -iE 'interpreter.json|shiro|config.fs|error'` → interpreter ayarı `/data/conf/interpreter.json`'dan yükleniyor ve dosya açılıştan sonra tüm interpreter'ları içeriyor (`kubectl exec deploy/zeppelin -- python3 -c "import json;print(sorted(json.load(open('/data/conf/interpreter.json'))['interpreterSettings']))"`). Yol kullanılmıyorsa (uyarı "falls back to conf dir"): `zeppelin-site.xml` ConfigMap'inde `zeppelin.config.fs.dir=/data/conf` + `zeppelin.config.storage.class=org.apache.zeppelin.storage.LocalConfigStorage` ekle (subPath mount, salt-okunur; zeppelin-site.xml yazılmaz) ve F4 notuna yaz. Sonra `zeppelin-path.sh` (`TABLE` verisi `c\n3\n` biçiminde gelir).

- [ ] **Step 6: Commit**

```bash
git add glue/ runbooks/zeppelin/ test/e2e/ platform/values/glue-dev.yaml
git commit -m "feat(zeppelin): apache/zeppelin 0.12.1 deployment — Shiro from Secret, Trino JDBC interpreter seed on PVC (ZEPPELIN_CONFIG_FS_DIR), TLS trust; e2e zeppelin-path

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 7: Belgeler, spec güncellemesi, CI, F3 ACL sıkılaştırma, tam e2e (lokal + CI)

**Files:**
- Create: `runbooks/access-control.md`, `runbooks/user-facing.md`
- Modify: `runbooks/install.md`, `docs/specs/2026-09-10-lakehouse-v2-design.md` (§3, §7), `README.md`, `.github/workflows/e2e.yaml`, `glue/templates/kafka-users.yaml`, `glue/tests/mongo_test.yaml`, `docs/plans/2026-09-10-f0-findings.md`

- [ ] **Step 1: connect KafkaUser mongodb ACL** — `kafka-users.yaml`'da mongodb kaynaklarının topic prefix ACL'inden `Read` çıkar (Debezium yalnız yazar; sink yok); `mongo_test.yaml`'a assert: connect user ACL'lerinde mongodb prefix `operations == [Write, Describe, Create]`. `helm unittest glue` PASS. (Canlı: Step 5 tam e2e mongo yolu hâlâ geçer.)

- [ ] **Step 2: CI workflow** — `timeout-minutes: 90`; teşhis bloğuna:
```yaml
          kubectl -n lakehouse get superset,certificate,issuer -o wide || true
          kubectl -n lakehouse describe superset superset | tail -30 || true
          kubectl -n lakehouse logs deploy/trino-coordinator --tail=60 || true
          kubectl -n lakehouse logs deploy/hub --tail=40 || true
          kubectl -n lakehouse logs deploy/zeppelin --tail=40 || true
```
Yeni adım (her koşuda, `if: always()`) "kaynak fotoğrafı":
```yaml
      - name: kaynak fotoğrafı (F5 boyutlandırma girdisi)
        if: always()
        run: |
          free -m; df -h /
          kubectl describe node | sed -n '/Allocated resources/,/Events/p'
          kubectl -n lakehouse get pods -o wide | wc -l
```

- [ ] **Step 3: runbooks**

`runbooks/install.md` yeni bölümler: **"F4 Secret'ları"** (Global Constraints listesi; `htpasswd -nbBC 10 superset '<parola>' >> password.db` → `kubectl create secret generic trino-service-accounts --from-file=password.db --from-literal=superset=… --from-literal=zeppelin=…`; `lakehouse-ca`: `openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes -days 3650 -subj /CN=lakehouse-ca -keyout tls.key -out tls.crt` → `kubectl create secret tls lakehouse-ca --cert=tls.crt --key=tls.key`; `tls.caBundle` = tls.crt içeriği), **"Hostname kopyaları"** (glue `*.hostname` + `keycloak.hostname` ↔ `platform/values/trino.yaml` `oauth2.issuer` ↔ `platform/values/jupyterhub.yaml` 4 URL), **"İlk giriş"** (Trino Web UI `/ui` OIDC; Superset Keycloak butonu; JupyterHub; Zeppelin AD), **"Realm değişikliği"** (KeycloakRealmImport mevcut realm'i güncellemez → realm sil + CR yeniden uygula; Task 2 Step 5 komutları). `runbooks/access-control.md`: gruplar, rules.json anatomisi (ilk eşleşme kazanır; `filter`, `columns[].mask`), yeni grup/kural = values + ArgoCD sync (`refreshPeriod` 60 s), LDAP group provider alanları, sandbox (Polaris `lakehouse_sandbox` + Trino `owner`), servis hesapları (SELECT). `runbooks/user-facing.md`: Superset Trino bağlantısı komutu (tek sefer: `kubectl -n lakehouse exec <web pod> -- superset legacy-import-datasources -p /app/configs/trino.yaml`) + Alerts&Reports için Valkey + `celeryWorker` notu; JupyterHub'da `trino.auth.OAuth2Authentication` ve PyIceberg (`POLARIS_CREDENTIAL`) örnekleri; Zeppelin interpreter/shiro Secret'ları ve tohum semantiği; **dev sınırı**: Keycloak hostname küme içi URL → tarayıcı OIDC akışı yalnız prod/pre-ship'te; **bilinen açıklar**: Superset/Trino Web UI/JupyterHub tarayıcı OIDC akışları pre-ship OpenShift'te canlı doğrulanır (e2e Bearer/password grant ile kanıtlıyor), Route reencrypt canlı, LDAP group provider canlı.

- [ ] **Step 4: spec + README** — §3 tablosu: Trino "483 · chart 1.42.2 · HTTPS (cert-manager, kimlik TLS ister)"; Superset "6.1.0 · **Superset Kubernetes Operator 0.2.0** (Helm chart deprecated)"; cert-manager "v1.21.2 · Application (OCI)"; Zeppelin "0.12.1 · Deployment + PVC (`ZEPPELIN_CONFIG_FS_DIR`)". §7: Trino maddesine "TLS zorunlu; `OAUTH2,PASSWORD`; gruplar file/LDAP group provider (OAuth2 groups claim yok); issuer = keycloak.hostname"; Superset maddesi operator + Redis'siz; Keycloak maddesi "hostname tam URL; sırlar `placeholders`; realm güncellenmez". README "Şu an: F4 kullanıcı yüzü … plan `docs/plans/2026-09-17-lakehouse-v2-f4-user-facing.md`".

- [ ] **Step 5: Tam e2e lokal (taze küme, helm modu)** — `kind delete cluster --name lakehouse`; `nohup test/e2e/run.sh --mode helm > /tmp/e2e-f4.log 2>&1 & disown`; beklenen son satırlar sırasıyla `E2E F1 OK`, `E2E F2 OK`, mongo/nginx OK, `E2E F4 TRINO OK`, `E2E F4 SUPERSET OK`, `E2E F4 JUPYTERHUB OK`, `E2E F4 ZEPPELIN OK`. Süreyi ve `kubectl describe node | sed -n '/Allocated/,/Events/p'` çıktısını F4 notuna yaz.

- [ ] **Step 6: Push + CI** — `git push origin v2`; `gh run watch` (ArgoCD modu, taze runner). Kırmızıysa düzelt (values/şablon), yeniden push; yeşil run numarasını F4 notuna yaz. Bellek/disk sınırına takılırsa (OOMKilled/evicted/`no space`): önce dev kaynak isteklerini düşür (Trino heap 1280M + `maxMemoryPerNode 384MB`, Zeppelin `-Xmx512m`); yine olmazsa `jupyterhub-path.sh` spawn adımını `E2E_NOTEBOOK_SPAWN=0` ortam değişkeniyle CI'da atla, lokal e2e'de zorunlu tut ve bunu F4 notunda + kullanıcıya raporda açıkça yaz.

- [ ] **Step 7: F4 notu + commit**

`docs/plans/2026-09-10-f0-findings.md` sonuna "## F4 notu (tarih)" — tablo: Keycloak iss/aud canlı değerleri; Trino probe davranışı; chart `additionalConfigFiles` birleşme davranışı; Superset operator CRD sapmaları; Zeppelin config.fs.dir davranışı; CI süre + kaynak fotoğrafı; açık kalanlar (tarayıcı OIDC akışları pre-ship; Route reencrypt canlı; LDAP group provider canlı; sertifika yenileme gözlemi; Alerts&Reports; Superset dashboard export (F5/F6 kabul demosu); notebook kullanıcı-özel Trino OIDC; Superset operator v1alpha1 sürüm takibi).
```bash
git add -A
git commit -m "docs(f4): runbooks (install F4 secrets, access-control, user-facing), spec §3/§7 update, CI timeout+diagnostics, connect mongodb ACL tightening; F4 note

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
git push origin v2
```

---

## Self-review (plan yazarı)

- **Spec kapsamı:** §2 F.1/teslimat h (iki not defteri: Task 5, 6 ✓), G.1.1/F.3.1 (Trino/Superset/Jupyter OIDC: Task 2–5; Zeppelin LDAPS: Task 6 ✓), §7 Trino REST katalog OAUTH2 + `rules.json` (Task 3 ✓; **resource groups eklenmedi** — ihtiyaç kanıtlanmadan eklenmez, F5'e not), Superset (Task 4 ✓, operator sapması R1), JupyterHub PVC/pyiceberg (Task 5 ✓), Zeppelin Trino JDBC (Task 6 ✓), Keycloak realm (Task 2 ✓); §8 cert-manager TLS (Task 1 ✓), Polaris RBAC sandbox (Task 3 ✓); §9 e2e "Trino assert" (Task 3 ✓); §13 F4 ✓; §14 paylaşımlı servis hesabı ✓.
- **Placeholder taraması:** "canlı adımda doğrula" noktaları (Trino probe, chart map birleşmesi, Superset podTemplate/init şeması, Zeppelin config.fs.dir) kod yazılmadan bilinemeyen upstream davranışlarıdır; her biri için beklenen sonuç ve sapmada yapılacak somut değişiklik yazılıdır.
- **Ad tutarlılığı:** Secret adları (Global Constraints ↔ Task 1 dev-secrets ↔ Task 3–6 tüketimleri) aynı; dev kullanıcı/parolalar (Task 2 devUsers ↔ Task 3 check.py ↔ Task 6 zeppelin-path) aynı; e2e servis hesabı `e2e/e2e-dev` (Task 1 ↔ 3 ↔ 5); `lakehouse-ca` mount yolu `/etc/lakehouse-ca/tls.crt` her yerde; Trino Service `trino` 8443 (Task 3 `additionalExposedPorts` ↔ Task 4/5/6 URL'leri); hub e2e token (Task 5 values ↔ script) aynı; `run_check_job` `check.py` dosya adı (lib.sh ↔ trino-check dizini) aynı.
