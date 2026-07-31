# bootstrap/ — Layer 0: cluster'ı ArgoCD app-of-apps'e hazırlar

Bu klasördeki `bootstrap.sh`, temiz bir cluster'ı **"ArgoCD app-of-apps
uygulamaya hazır"** hâle getiren, tek, idempotent, platform-algılamalı bir
script'tir: namespace/RBAC, operatör seti, ArgoCD ve `generated` modundaki
tüm Secret'lar — sıfır manuel adım.

**Kapsam dışı (Layer 1 / Task 8):** app-of-apps'in kendisinin uygulanması
(`gitops/apps/app-of-apps.yaml`) ve chart'ın (`chart/`) `helm install`
edilmesi bu script'in işi DEĞİL — bootstrap, ArgoCD ayakta ve Secret'lar
mevcut olduğu anda durur.

## Kullanım

```bash
bootstrap/bootstrap.sh [--platform openshift|vanilla] [--namespace lakehouse]
                        [--secrets-mode generated|external|manual]
                        [--s3-endpoint URL] [--s3-secret-from-env]
                        [--dry-run] [--help]
```

Bayraklar:

| Bayrak | Varsayılan | Açıklama |
|---|---|---|
| `--platform` | auto-detect | `openshift` veya `vanilla`. Verilmezse `kubectl api-resources --api-group=route.openshift.io` ile algılanır (Route API'si varsa openshift) — **`--dry-run` ile birlikte verilmezse bu canlı algılama HİÇ ÇALIŞMAZ** (bkz. aşağıdaki `--dry-run` bölümü): bir uyarı yazdırılır ve `vanilla` varsayılır. |
| `--namespace` | `lakehouse` | Ürünün kurulacağı hedef namespace (yoksa oluşturulur). |
| `--secrets-mode` | `generated` | `generated` \| `external` \| `manual` — chart'ın `secrets.mode` değeriyle BİREBİR aynı sözleşme (bkz. `chart/values.yaml`). |
| `--s3-endpoint` | (boş → in-cluster MinIO) | `s3-credentials` Secret'ının `endpoint` alanı. |
| `--s3-secret-from-env` | kapalı | Verilirse `access-key-id`/`secret-access-key` rastgele ÜRETİLMEZ; ortam değişkenlerinden (`S3_ACCESS_KEY_ID`/`S3_SECRET_ACCESS_KEY`) okunur — asla CLI argümanı olarak alınmaz, asla loglanmaz. |
| `--dry-run` | kapalı | Sıralı eylem planını (operatörler → argocd → namespace → secrets) TAM komutlarıyla yazdırır; HİÇBİR ŞEY uygulamaz. `--platform` açıkça verildiğinde canlı bir cluster'a ihtiyaç duymaz. |
| `--help` | — | Bu tabloyu yazdırır. |

## Ne yapar (sıra: operatörler → ArgoCD → namespace → secrets)

1. **Operatörler** (idempotent):
   - **openshift**: `operators/*.yaml` (00→06, `06-openshift-gitops.yaml`
     dâhil) `kubectl apply -f` ile uygulanır, ardından gerekli CRD'lerin
     (`kafkas.kafka.strimzi.io`, `clusters.postgresql.cnpg.io`,
     `certificates.cert-manager.io`, `sparkapplications.sparkoperator.k8s.io`,
     `keycloaks.k8s.keycloak.org`, `externalsecrets.external-secrets.io`)
     `Established` olması `BOOTSTRAP_CRD_TIMEOUT` saniye (varsayılan 300)
     içinde beklenir.
   - **vanilla**: aynı operatörler `bootstrap/operators.env`'deki pinlenmiş
     Helm chart versiyonlarıyla `helm upgrade --install` edilir (Strimzi,
     CloudNativePG, cert-manager, Spark Operator; ExternalSecrets Operator
     SADECE `--secrets-mode=external` iken kurulur). **Asimetri:** Keycloak
     Operator'ın resmi bir Helm chart'ı yok — `operators.env`'de pinlenmiş
     bir manifest URL'i üzerinden `kubectl apply -f` ile kurulur (bkz. o
     dosyadaki yorum). Aynı CRD bekleme döngüsü burada da çalışır.
2. **ArgoCD**:
   - **openshift**: adım 1'de zaten uygulanmış olan
     `operators/06-openshift-gitops.yaml` (OLM-yönetimli, `openshift-gitops`
     namespace'inde varsayılan bir ArgoCD instance'ı otomatik yaratır) —
     yalnızca `openshift-gitops-server` Deployment'ının `Available`
     olması beklenir.
   - **vanilla**: `argo/argo-cd` Helm chart'ı (`operators.env`'deki pin),
     `argocd` namespace'ine, **`--set fullnameOverride=argocd` ile** kurulur;
     `argocd-server` Deployment'ının `Available` olması beklenir. Bu override
     ZORUNLU: onsuz release adı `argocd` + chart adı `argo-cd` chart'ın
     kendi `<release>-<chart>` şablonuyla `argocd-argo-cd` fullname'ini
     üretir — yani gerçek Deployment `argocd-argo-cd-server` olurdu,
     `argocd-server` DEĞİL, ve bekleme her zaman timeout ile başarısız
     olurdu. `fullnameOverride` bu ismi chart versiyonundan bağımsız,
     deterministik hâle getirir.
3. **Namespace + Secret'lar** (`bootstrap/lib/secrets.sh`):
   - Hedef namespace idempotent olarak oluşturulur (aynı get-or-create
     deseni Keycloak Operator'ın kendi namespace'i için de kullanılır —
     `--dry-run` DIŞINDA `kubectl get secret`/`get ns` her zaman
     ÖNCE kontrol edilir; `--dry-run` altında bu kontroller de HİÇ
     ÇALIŞMAZ, bkz. aşağıdaki `--dry-run` bölümü).
   - `generated` (varsayılan) — her PLATFORM-İÇİ Secret için
     generate-if-absent (asla var olanı yeniden ÜRETMEZ/ezmez — önce
     `kubectl get secret` kontrol edilir):
     - `s3-credentials` (chart varsayılanıyla aynı ad —
       `storage.s3.secretName`): `access-key-id` (20 karakter), `secret-access-key`
       (40 karakter), `endpoint`, `region` (varsayılan `us-east-1`).
       `endpoint`: `--s3-endpoint` verildiyse o; verilmediyse in-cluster
       MinIO servisi varsayılır (`http://minio.<namespace>.svc:9000`) —
       bu, `components.minio`'nun (Task 8) açılacağını VARSAYAR. Gerçek bir
       harici S3 için `--s3-endpoint <URL> --s3-secret-from-env` kullanın.
     - `oidc-credentials`: yalnızca BEST-EFFORT bir varsayım (Keycloak henüz
       ayakta değil) — `issuer-url`,
       `https://keycloak-service.<namespace>.svc:8443/realms/lakehouse`
       olarak üretilir: Service adı Keycloak Operator'ın gerçekten ürettiği
       `keycloak-service` (chart/templates/_helpers.tpl
       `lakehouse.svc.keycloak`), şema+port `https`/`8443` çünkü chart'ın
       Keycloak CR'ı `http.tlsSecret: keycloak-tls` set eder (TLS açık →
       Operator varsayılanı 8443, düz `http`/8080 DİNLEMEZ), realm yolu
       `.Values.keycloak.realm.name` varsayılanı `lakehouse`. Bu değerler
       chart'ın kendi varsayılanları değiştirilirse (`keycloak.realm.name`
       override edilirse) elle düzeltilmelidir. `trino-client-id` (`trino`)
       `chart/templates/10-keycloak.yaml`'daki sabit literal; `trino-client-secret`
       üretilir.
     - `keycloak-admin-credentials`: `username=admin` + üretilen parola.
     - `trino-internal-secret`: üretilen paylaşımlı sır.
     - Kaynak-DB Secret'ları (`pg`, `mssql`, `mongo`) **ÜRETİLMEZ** — bunlar
       bir kaynak onboard edildiğinde (Console) oluşturulan müşteri
       veritabanı kimlik bilgileridir; bootstrap yalnızca bunu bilgilendirici
       bir satırla belirtir.
   - `external` — bootstrap Secret açısından HİÇBİR ŞEY oluşturmaz/doğrulamaz;
     bir ExternalSecrets Operator + SecretStore/ClusterSecretStore'un
     out-of-band mevcut olması gerektiğini, chart'ın her Secret için bir
     `ExternalSecret` render ettiğini yazdırır.
   - `manual` — `chart/templates/NOTES.txt`'teki elle-kurulum envanterinin
     aynısını (aynı komutlar) yazdırır.
   - Secret değerleri `openssl rand` tabanlıdır; sır materyali stdout'a HİÇBİR
     ZAMAN yazdırılmaz — yalnızca `created <ad>` / `exists <ad> — skipped`.

## Idempotency sözleşmesi

Her adım yarım kalmış bir cluster'da yeniden çalıştırmaya güvenlidir:
operatör kurulumları `helm upgrade --install` / `kubectl apply` (ikisi de
doğal olarak idempotent), namespace/Secret oluşturma önce var olup
olmadığını kontrol eder ve varsa DOKUNMAZ (asla üzerine yazmaz/yeniden
üretmez).

## `--dry-run`

Sıralı planı (operatörler → argocd → namespace → secrets) TAM
komutlarıyla yazdırır; kubectl/helm'e HİÇBİR ÇAĞRI yapmaz (mutating VEYA
read-only — `kubectl get secret`/`get ns`/`api-resources` dâhil). Bu, canlı
bir cluster olmadan tamamen güvenle çalıştırılabilir olmasını garanti eder:

- `--platform` açıkça verildiğinde platform algılama adımı zaten atlanır.
- `--platform` VERİLMEDİĞİNDE de aynı garanti geçerlidir: canlı algılama
  probu (`kubectl api-resources`) `--dry-run` altında ÇALIŞTIRILMAZ — bunun
  yerine bir uyarı yazdırılır ve plan `vanilla` varsayılarak üretilir.
  `openshift` planını görmek için `--platform openshift`'i açıkça verin.
- Namespace/Secret adımları da aynı ilkeye uyar: `bootstrap::ensure_namespace`
  ve `bootstrap::create_secret_if_absent`, `--dry-run` altında `kubectl get
  ns`/`get secret` ile var olup olmadığını KONTROL ETMEDEN doğrudan "would
  ensure ..." planını yazdırıp döner.

## Environment değişkenleri

- `BOOTSTRAP_CRD_TIMEOUT` (varsayılan `300`) — CRD/Deployment bekleme
  döngülerinin saniye cinsinden zaman aşımı süresi.
- `S3_ACCESS_KEY_ID` / `S3_SECRET_ACCESS_KEY` — yalnızca
  `--s3-secret-from-env` ile okunur; CLI argümanı olarak ASLA verilmez.

## Bir sonraki adım — Layer 1 (bu script'in kapsamı DIŞINDA, bkz. bootstrap/app-of-apps.sh)

Bootstrap tamamlandığında (ArgoCD ayakta — OpenShift'te OLM'den gelen
`openshift-gitops` instance'ı, vanilla'da `argocd` namespace'indeki Helm
release — ve `--secrets-mode`'a göre Secret'lar mevcut), Layer 1'i
uygulamanın İKİ eşdeğer yolu var. Her iki yol da chart'ın vendor-generic
bucket-init Job'ını (`storage.createBuckets`, chart/templates/
18-bucket-init.yaml — `s3-credentials` Secret'ının işaret ettiği HERHANGİ
bir S3 endpoint'ine karşı `mc mb --ignore-existing` çalıştırır) OTOMATİK
tetikler; ayrı bir manuel "bucket oluştur" adımı gerekmez.

1. **ArgoCD app-of-apps (önerilen — GitOps, bkz. gitops/README.md)**:
   `bootstrap/app-of-apps.sh` çalıştırın:

   ```bash
   bootstrap/app-of-apps.sh --platform-repo <PLATFORM_REPO_URL> \
                             --pipeline-repo <PIPELINE_REPO_URL> \
                             [--argocd-namespace argocd|openshift-gitops] \
                             [--dry-run]
   ```

   Bu script `gitops/apps/app-of-apps.yaml`'daki `<PLATFORM_REPO_URL>` /
   `<PIPELINE_REPO_URL>` placeholder'larını VE `namespace:
   openshift-gitops` alanını (`--argocd-namespace` — vanilla'da `argocd`,
   varsayılan; OpenShift'te `openshift-gitops` verin) RENDERED bir
   KOPYAYA substitute eder — committed dosya asla mutate edilmez — sonra
   argocd-cm health-check patch'ini (`gitops/argocd/argocd-cm-health.yaml`)
   ve substitute edilmiş kök Application'ı (`lakehouse-root`) sırasıyla
   apply eder, son olarak health customization'ının pick up edilmesi için
   `argocd-repo-server`'ı restart eder. `--dry-run`, hiçbir şeyi
   uygulamadan (canlı bir cluster'a ihtiyaç duymadan) her iki placeholder
   da doldurulmuş TAM planı stdout'a yazdırır.

   ArgoCD, `lakehouse-platform` Application'ını (`argocd.argoproj.io/
   sync-wave: "0"`, chart'ı Helm-source olarak kurar — bucket-init Job'u
   `PostSync` hook'uyla burada çalışır) `lakehouse-pipelines`'tan
   (`sync-wave: "1"`) ÖNCE senkronize eder: pipeline manifestleri platform
   (Kafka Connect, Nessie catalog, S3 bucket'ları) hazır olmadan
   uygulanmaz.

2. **Düz Helm (ArgoCD'siz / tek-seferlik kurulum)**:

   ```bash
   helm install lakehouse chart/ -f <values-<env>.yaml>
   # örn: helm install lakehouse chart/ -f chart/values-prod.example.yaml
   ```

   Bu yol AYNI bucket-init Job'ını `helm.sh/hook: post-install` ile
   tetikler (ArgoCD'nin `PostSync`'inin Helm-native eşdeğeri) — iki yoldan
   hangisi seçilirse seçilsin ilk ingest'in yazacağı bucket'lar kurulum
   biter bitmez hazır olur.

Her iki yolda da dev/PoC/e2e-only MinIO (`components.minio`) VARSAYILAN
KAPALIDIR. Gerçek bir harici S3 endpoint'iniz yoksa VE bootstrap'ı
`--s3-endpoint` vermeden çalıştırdıysanız (bu durumda `s3-credentials.
endpoint`, üstteki `generated` modu notunda açıklandığı gibi in-cluster
MinIO'yu varsayar), chart kurulumuna `--set components.minio=true`
eklemeniz gerekir — aksi halde bucket-init Job'u hiçbir endpoint'e
ulaşamaz ve `backoffLimit` (6) tükenene kadar başarısız olur.

## Testler

```bash
bash bootstrap/tests/bootstrap_test.sh
shellcheck bootstrap/bootstrap.sh bootstrap/lib/secrets.sh bootstrap/tests/bootstrap_test.sh
```
