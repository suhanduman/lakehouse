# B-v2 GitOps — Console pipeline'ları + platform chart'ı için deklaratif akış (PROD / OpenShift)

Bu klasör, platformu ve self-service Console'un ürettiği CDC akışlarını
**GitOps (OpenShift GitOps / ArgoCD)** ile deklaratif yöneten **prod-hedefli**
artefaktları içerir.

> **PROD-ONLY.** OpenShift GitOps operatörü yalnızca OpenShift'te kurulur;
> microk8s PoC'de OLM yoktur. microk8s'te Console **B-v1 (direct-apply)**
> modundadır. Buradaki manifestleri microk8s'te en fazla **render/CI**
> (`gitops/ci/`) ile doğrularız; canlı ArgoCD sync yalnızca OpenShift'tedir.

## ArgoCD Helm'in yerine geçmez — onu ÇALIŞTIRIR

ArgoCD bir dağıtım orkestratörüdür; Helm ise şablonlama/paketleme aracı. İkisi
birlikte:

- **Platform bileşenleri (Kafka, Trino, Nessie, Apicurio, S3, Console...)** →
  `chart/` **Helm chart'ı olarak kalır**. ArgoCD `Chart.yaml`'ı görünce arka
  planda `helm template` çalıştırır (native Helm desteği). **Bırakılan tek şey:**
  terminalden/CI'dan manuel `helm install/upgrade`. **Korunan:** chart dizin
  yapısı (`templates/`, `values.yaml`) birebir. Bkz. `gitops/apps/app-of-apps.yaml`
  → `lakehouse-platform` (Helm-source Application, `values-prod.yaml`).
- **Statik CDC akış manifestleri (pipeline repo)** → saf YAML. Bkz.
  `lakehouse-pipelines` (directory-source Application).

Böylece karmaşık altyapı Helm ile, basit pipeline'lar saf YAML ile, **tek
merkezden (ArgoCD)** yönetilir; drift `selfHeal` ile Git'e çekilir (SSoT).

## Neden GitOps (yalnızca backup değil)

- **SSoT + felaket kurtarma:** mimari Git'te; küme çökse ArgoCD'yi yeni kümeye
  kurup repo'yu gösterince akışlar birebir ayağa kalkar. Velero backup'ı
  **tamamlayıcı** bir DR sigortasıdır ama config yönetimi değildir (periyotlar
  arası Console eklemeleri kaybolabilir).
- **Sıralama garantisi:** Iceberg sink append-only çalıştığından tablo,
  `KafkaConnector`'dan ÖNCE identifier field ile yaratılmalı; ArgoCD
  **PreSync hook**'ları bu sırayı deklaratif kilitler (backup'tan geri
  yüklerken kurmak kırılgandır).
- **Audit + insan onayı:** Console API'ye yazmak yerine **PR** açar; tablo/şema/PK
  ve `identifier-field-ids` canlıya geçmeden gözden geçirilir.
- **Not:** `helm upgrade` Console'un out-of-band kaynaklarını prune etmez; GitOps'un
  gerekçesi silinme değil, SSoT + sıralama + audit'tir.

## Neden OpenShift GitOps operatörü (upstream ArgoCD DEĞİL)

`operators/06-openshift-gitops.yaml`. Şartname A.4 (Operator-pattern); OpenShift
SCC uyumu (least-privilege, non-root otomatik); native OAuth/Keycloak SSO; Red
Hat kurumsal desteği. Upstream ArgoCD'yi elle kurmak OpenShift'te anti-pattern
(SCC'leri manuel esnetmek gerekir).

## İki-repo modeli

| Repo | Sahip | İçerik |
|------|-------|--------|
| **Platform** | DevOps/SRE | `chart/` (Helm), `operators/`, `gitops/` |
| **Pipeline** | Console otomasyonu | Yalnızca CDC akış manifestleri; Console PR'ları BURAYA. |

Console token'ı YALNIZCA pipeline repo'suna yazar (blast-radius izolasyonu).

## Bir pipeline girdisinin anatomisi

`gitops/pipeline-template/example-pg-customers.yaml`:

| Sıra | Kaynak | Ne yapar |
|------|--------|----------|
| PreSync wave 0 | descriptor **ConfigMap** | şema + identifier (PK); PR diff'inde gözden geçirilir |
| PreSync wave 1 | pre-create **Job** | tabloyu identifier field ile yaratır (`images/iceberg-tools`); **fail-loud** → uyuşmazsa sync kilitlenir, connector asla apply edilmez |
| Sync | **KafkaTopic** + **KafkaConnector** | tablo hazır → veri akar, upsert doğru |

## CI (SCM-agnostik)

`gitops/ci/validate-pipeline.sh <dizin>` — her PR'da: `yamllint` + her descriptor
için `create_iceberg_table.py --dry-run` (Nessie/S3'e dokunmadan şema+identifier
doğrular). `images/iceberg-tools` içinde çalışır. SCM/CI kararı (GitLab CI /
Tekton / GitHub Actions) sonrası yalnızca sarmalanır; ardından **insan PR review**.

## Keycloak / AD SSO (ArgoCD)

`openshift-gitops` ns'indeki `ArgoCD` CR'ı OIDC (Keycloak/RH-SSO) için yamalanır:

```yaml
# oc -n openshift-gitops patch argocd openshift-gitops --type merge -p:
spec:
  sso:
    provider: dex
    dex:
      config: |
        connectors:
          - type: oidc
            id: keycloak
            name: Keycloak (AD)
            config:
              issuer: https://<keycloak-host>/realms/<realm>   # POC-DOĞRULA
              clientID: openshift-gitops
              clientSecret: {$oidc.keycloak.clientSecret}
              requestedScopes: ["openid", "profile", "email", "groups"]
  rbac:
    policy: |
      g, lakehouse-admins, role:admin      # AD grup → ArgoCD rolü (POC-DOĞRULA)
    scopes: "[groups]"
```

## Kurulum akışı (OpenShift, prod)

```
1. oc apply -f operators/            # 06-openshift-gitops dahil; csv → Succeeded
2. oc get argocd -n openshift-gitops # default instance; SSO+RBAC yaması (yukarı)
3. iceberg-tools imajını build+push  # images/iceberg-tools/Dockerfile
4. app-of-apps.yaml repoURL/valueFiles doldur → oc apply -f gitops/apps/app-of-apps.yaml
5. Console'u pipeline repo'suna PR açacak şekilde yapılandır (B-v2 modu).
```

## Prod hardening kontrol listesi (S3 / SCC / DNS)

S3 + OpenShift'e özgü, canlı-OpenShift'te doğrulanacak maddeler:

- [x] **S3 path-style** — chart'ta HER S3 istemcisinde açık: Trino
  (`s3.path-style-access=true`), Nessie (`...PATH_STYLE_ACCESS=true`), Iceberg
  sink, Spark (`fs.s3a.path.style.access`), Zeppelin. Bazı S3 nesne depoları sanal-host
  değil dizin-tabanlı model kullandığından zorunlu (yoksa "bucket bulunamadı").
- [x] **SCC restricted-v2 uyumu (chart-managed Deployment'lar)** — çerçeve:
  `global.security.restrictedSecurityContext` (varsayılan KAPALI; prod values
  AÇAR) + helper `_helpers.tpl` (`lakehouse.podSecurityContext`/
  `containerSecurityContext`: runAsNonRoot, drop ALL, seccomp RuntimeDefault,
  allowPrivilegeEscalation:false; **runAsUser SET ETMEZ** — SCC atar). Wired:
  **Trino (coord+worker), Nessie, Apicurio, Zeppelin, Console (backend+frontend),
  kafka-ui** (prod render: `runAsNonRoot` ×16; test: `security-hardening_test.yaml`).
  `anyuid` bypass YASAK. NOT: Console-frontend OpenShift'te non-root nginx imajı
  gerektirir (unprivileged).
- [ ] **SCC — operatör-yönetimli pod'lar** *(ops/CR)* — Kafka/Connect (Strimzi
  `spec.<x>.template.pod.securityContext`), CNPG (operatör non-root varsayılan)
  ve Spark (SparkApplication driver/executor securityContext) kendi CR
  alanlarıyla yapılandırılır; operatörler zaten restricted-uyumlu pod üretir,
  açık override opsiyoneldir. Canlı OpenShift'te doğrulanır.
- [~] **S3 internal-CA truststore** — çerçeve + **Trino referans wiring DONE**:
  `global.security.s3TrustBundle` + `00-s3-trust-bundle.yaml` (OpenShift
  trusted-CA injection ConfigMap) + `_helpers.tpl` 4 helper (`s3TrustVolumes`,
  `s3TrustStoreMount`, `s3TrustInitContainer`, `s3TrustJvmProps`). Trino (coord
  +worker) wired: init-container `s3-trust-init` sistem cacerts'i kopyalar +
  CA'yı `keytool` ile ekler → `/work/s3-trust/cacerts`; jvm.config'e
  `-Djavax.net.ssl.trustStore=...` (test: `security-hardening_test.yaml`).
  **Ön koşul (cluster-admin, tek seferlik):** S3 endpoint'in Root CA'sını cluster
  additional-trust'a ekle (Proxy `trustedCA` / image config) → OpenShift
  `s3-trust-bundle` ConfigMap'ine `ca-bundle.crt` enjekte eder.
  **Nessie de wired** (aynı 4 helper; init-container `s3-trust-init` + Quarkus
  `JAVA_OPTS_APPEND` trustStore; test'te jvm doğrulaması Trino'da). **Connect +
  Spark — init-container YAKLAŞIMI UYGULANAMAZ:**
  - **Kafka Connect (Strimzi):** KafkaConnect CR init-container hook sunmaz →
    CA, **imaj build anında** gömülür. `images/connect/Dockerfile` opsiyonel
    `S3_CA_CERT_B64` build-arg'ı ekler (boşsa no-op):
    `docker build --build-arg S3_CA_CERT_B64="$(base64 -w0 s3-root-ca.crt)"
    -f images/connect/Dockerfile ...` → CA imajın cacerts'ine `keytool` ile eklenir.
  - **Spark:** `spark-py` imajı repo dışı → ya aynı build-arg desenini o imaja
    uygula, ya da SparkApplication `spec.driver/executor.initContainers` +
    `spark.driver/executor.extraJavaOptions=-Djavax.net.ssl.trustStore=...`.
  Canlı doğrulama (SSLHandshake yok) OpenShift+S3 endpoint'inde.
- [ ] **CoreDNS Data-VIP** *(ops)* — OpenShift CoreDNS'in S3 endpoint LB
  IP'lerini (Data VIP) doğru çözdüğü teyit edilmeli.

## Açık kararlar (POC-DOĞRULA)

- **SCM/CI:** GitLab CI / Tekton / GitHub Actions → repo URL'leri + CI sarmalayıcı.
- **AppProject RBAC:** prod'da `project: default` yerine ayrı `AppProject`.
- **iceberg-tools imaj registry/tag** + PreSync Job `image:` referansı.
- OpenShift GitOps `channel` + Keycloak realm/client değerleri.
