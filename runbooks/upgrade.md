# Yükseltme (sürüm bump'ları, sıra, geri alma)

Tüm sürümler Git'te sabittir (`runbooks/versions.md` "Kurulum yolu" sütunu). Yükseltme = **dosyada sürümü
değiştir → PR → CI yeşil → merge → ArgoCD sync**. Kümede elle `helm upgrade` YOKTUR (yalnız `--mode helm`
ile kurulan dev kümesinde; orada da aynı sıra geçerlidir).

## 0. Altın kurallar

1. **Tek PR = tek bileşen** (Kafka gibi iki adımlı olanlar hariç). Karışık bump'ta hangi bileşenin kırdığı
   anlaşılamaz; e2e 40–55 dk sürer.
2. **Sıra: operatörler önce, uygulamalar sonra.** Operatör CRD'leri eski uygulama CR'larını okuyabilir;
   tersi garanti değildir.
3. **Her bump'ta `runbooks/versions.md`'deki satır aynı PR'da güncellenir** (şartname J.3.2).
4. Yükseltme öncesi yedek durumu yeşil olmalı: `kubectl -n lakehouse get cluster` → hepsi
   `ContinuousArchiving=True`, son `Backup` `completed` (`runbooks/dr.md`).
5. Upstream sürüm notları okunur (özellikle Strimzi, CNPG, Superset operator `v1alpha1`).

## 1. Sıra (üstten alta)

| # | Bileşen | Değişecek dosya | Etki |
|---|---|---|---|
| 1 | cert-manager | `platform/apps/00-cert-manager.yaml` (`targetRevision`) | CRD; sertifikalar yeniden imzalanmaz |
| 2 | Strimzi operator | `platform/apps/00-strimzi.yaml` | CRD + Kafka/Connect pod'larında rolling restart |
| 3 | **Kafka** (`version` → `metadataVersion`, **iki adım**) | `glue/values.yaml` → `versions.kafka`, `versions.kafkaMetadata` | broker + Connect rolling restart ×2 |
| 4 | CloudNativePG operator | `platform/apps/00-cnpg.yaml` | CRD + Postgres instance rolling restart |
| 5 | Barman Cloud eklentisi | `platform/apps/00-cnpg-barman.yaml` | pg pod'larındaki sidecar yenilenir |
| 6 | spark-operator | `platform/apps/00-spark-operator.yaml` | CRD; koşan SparkApplication'lar etkilenmez |
| 7 | Keycloak operator + Keycloak | `platform/keycloak-operator/kustomization.yaml` (`ref=26.7.3`) | operatör + Keycloak StatefulSet; realm **import edilmez** (§4) |
| 8 | Superset operator | `platform/apps/00-superset-operator.yaml` | CRD `v1alpha1` — kırıcı alan değişikliği riski (§3) |
| 9 | Connect eklentileri (Debezium / Iceberg / Hadoop) | `glue/values.yaml` → `versions.*` **+ `connect.buildImage` etiketi** | **yeniden build** (~10 dk) |
| 10 | Polaris | `platform/apps/20-polaris.yaml` + `glue/values.yaml` → `versions.polarisAdminTool` | Polaris Deployment; şema migrasyonu admin-tool ile |
| 11 | Trino | `platform/values/trino.yaml` (`image.tag`) + `platform/apps/30-trino.yaml` (chart) | coordinator/worker restart |
| 12 | Superset | `glue/values.yaml` → `superset.imageTag` | `superset-migrate` Job + web restart |
| 13 | JupyterHub (z2jh) + notebook imajı | `platform/apps/30-jupyterhub.yaml`, `platform/values/jupyterhub.yaml` | hub restart; koşan not defteri pod'ları **durur** |
| 14 | Zeppelin | `glue/values.yaml` → `zeppelin.image`, `zeppelin.trinoJdbcVersion` | Deployment restart |
| 15 | İzleme / DR (dev) | `platform/apps/dev/40-monitoring.yaml`, `platform/apps/dev/40-velero.yaml` + values | dev-only; prod'da platform izlemesi / OADP |

## 2. Adım adım

### 2.1 Operatör chart'ı (cert-manager, Strimzi, CNPG, barman, spark-operator, superset-operator, monitoring, velero)

```bash
# 1) sürüm gerçekten var mı
helm show chart cnpg/cloudnative-pg --version 0.30.0 | grep -E '^(version|appVersion)'
# 2) Application'da targetRevision'ı değiştir -> commit -> PR
# 3) merge sonrası:
kubectl -n argocd get applications          # hepsi Synced/Healthy
kubectl -n cnpg-system get pods             # yeni operatör Running
```
OCI chart'larda (`strimzi`, `cert-manager`, `superset-operator`) repository Secret'ı `bootstrap.sh` tarafından
zaten kayıtlıdır; yalnız `targetRevision` değişir.

### 2.2 Kafka: **iki adım** (Strimzi kuralı)

Aynı PR'da hem `version` hem `metadataVersion` değiştirilirse geri dönüş yolu kapanır: `metadataVersion`
**geri alınamaz**.

```yaml
# Adım 1 — glue/values.yaml: yalnız broker sürümü
versions:
  kafka: "4.4.0"            # YENİ
  kafkaMetadata: "4.3-IV0"  # ESKİ kalır
```
```bash
kubectl -n lakehouse get kafka lakehouse -o jsonpath='{.status.conditions}'          # Ready=True
kubectl -n lakehouse get pods -l strimzi.io/cluster=lakehouse                        # rolling restart bitti mi
kubectl -n lakehouse get kafkaconnect connect -o jsonpath='{.status.conditions}'     # Connect de Ready
```
Ayrı bir PR'da:
```yaml
# Adım 2 — metadata sürümü (GERİ ALINAMAZ)
versions:
  kafkaMetadata: "4.4-IV0"
```
Adım 1'de sorun çıkarsa yalnız `versions.kafka` revert edilir; veri kaybı olmaz. `KafkaConnect.spec.version`
de aynı `versions.kafka` değerinden gelir → Connect adım 1'de zaten yeni sürüme taşınır.

### 2.3 Kafka Connect: `buildImage` etiketi değişmeden yeni eklenti YÜKLENMEZ

`spec.build.output.image` Git'te açıkça verilir ve Strimzi bunu yeniden yazmaz. Debezium/Iceberg/Hadoop
sürümüyle birlikte **etiketi de değiştirin**:

```yaml
# glue/values.yaml
versions: {debezium: "3.7.0.Final", iceberg: "1.12.0"}
# platform/values/glue.yaml
connect: {buildImage: "image-registry.openshift-image-registry.svc:5000/lakehouse/connect:2.1.0"}   # 2.0.0 -> 2.1.0
```
```bash
kubectl -n lakehouse get kafkaconnect connect -o jsonpath='{.status.conditions}'   # Ready (build ~10 dk)
kubectl -n lakehouse logs -l strimzi.io/kind=KafkaConnect --tail=50                # build/plugin hataları
kubectl -n lakehouse get kafkaconnector                                            # hepsi Ready
```
`versions.iceberg` hem sink'i hem Spark `spark.jars.packages`'ini besler → Spark işleri bir sonraki koşuda
yeni runtime'ı Maven'den çeker (kapalı ağda önce iç ayna: `runbooks/troubleshooting.md#maven`).

### 2.4 Polaris

Chart `targetRevision` ve `versions.polarisAdminTool` **birlikte** yükseltilir (admin-tool imajı şema
migrasyonunu yapar). Önce `polaris-db` yedeğini doğrulayın (`runbooks/dr.md`):

```bash
kubectl -n lakehouse rollout status deploy/polaris --timeout=600s
kubectl -n lakehouse exec deploy/polaris -- curl -sf localhost:8182/q/health | head -c 200
scripts/polaris-setup.sh --setup platform/polaris/setup.yaml   # idempotent; yalnız EKSİK nesneleri yaratır
```

### 2.5 Trino / Superset / JupyterHub / Zeppelin

```bash
# Trino: chart targetRevision + image.tag birlikte (chart varsayılan etiketini values ezer)
kubectl -n lakehouse rollout status deploy/trino-coordinator --timeout=900s
kubectl -n lakehouse exec deploy/trino-coordinator -- curl -sk https://localhost:8443/v1/info | head -c 200
# Superset: imageTag -> superset-migrate Job yeniden koşar
kubectl -n lakehouse get superset superset -o jsonpath='{.status.phase}{"\n"}'     # Running
kubectl -n lakehouse logs job/superset-migrate --tail=30
# JupyterHub: koşan not defterleri DURUR -> kullanıcıları önceden uyarın
kubectl -n lakehouse get pods -l component=singleuser-server
# Zeppelin: image + trinoJdbcVersion; yeni JDBC jar'ı ilk açılışta Maven'den iner (~1 dk, PVC'de kalıcı)
kubectl -n lakehouse rollout status deploy/zeppelin --timeout=600s
```
**Superset `-dev` etiketi:** yeni etikette sürücülerin hâlâ bulunduğunu yükseltmeden önce doğrulayın
(düz `6.x.y` etiketinde YOKTUR ve site-packages salt-okunurdur):
```bash
kubectl -n lakehouse exec deploy/superset-web-server -- python -c "import psycopg2, trino, authlib; print('ok')"
```

## 3. Superset operator `v1alpha1` uyarısı

Alan adları sürümler arasında **kırılabilir**. Operator yükseltmesinden önce CRD'yi kontrol edin ve
`glue/templates/superset.yaml`'ı gerekiyorsa güncelleyin:

```bash
kubectl get crd supersets.superset.apache.org -o jsonpath='{.spec.versions[*].name}{"\n"}'
kubectl explain superset.spec --recursive | head -60
helm unittest glue          # şablon testleri kırıcı alan değişikliğini yakalar
```

## 4. Keycloak: realm import mevcut realm'i GÜNCELLEMEZ

Keycloak sürümünü yükseltmek realm'e dokunmaz; `glue/templates/keycloak-realm.yaml` değişse bile çalışan
realm aynen kalır (CR `Done` görünür, değişiklik uygulanmaz). Realm içeriği değişecekse `docs/30-kurulum.md`
→ "Realm içeriğini sonradan değiştirmek" bölümündeki sil-yeniden-içe-aktar prosedürü uygulanır ve **kullanıcıların Keycloak
UI'ında elle yaptığı her şey gider**. AD federasyonu varsa kullanıcılar tekrar akar; yoksa önce yedek alın:

```bash
kubectl -n lakehouse exec keycloak-0 -- /opt/keycloak/bin/kcadm.sh get users -r lakehouse > users.json
```

## 5. ArgoCD `targetRevision` (uygulama kaynağı) bump'ı

Git kaynaklı Application'lar (`glue`, `polaris`, `trino`, `jupyterhub`, `keycloak-operator`; dev'de
`monitoring`/`velero`) `targetRevision: main` ile dalı izler. Bir etikete sabitlemek için:

```bash
git tag -a v2.1.0 -m "F5" && git push origin v2.1.0
# platform/apps/*.yaml + platform/root-app.yaml: targetRevision: main -> v2.1.0
kubectl -n argocd get applications -o wide                                   # Synced/Healthy
kubectl -n argocd get app glue -o jsonpath='{.status.sync.revision}{"\n"}'   # beklenen commit SHA'sı
```
`bootstrap.sh --revision <ref>` kök Application'ı ve alt Application'ların `repoURL`/`targetRevision`'ını
**her reconcile'da** o revizyona sabitler; kalıcı değişiklik için dosyaları da güncelleyin.

## 6. Kapı (CI) — merge etmeden önce

`.github/workflows/e2e.yaml` iki iş koşar, ikisi de yeşil olmalıdır:

| İş | Ne doğrular |
|---|---|
| `helm-unittest` | `helm unittest glue`, `python3 -m py_compile glue/jobs/*.py`, prod+dev `helm template`, prod Trino (LDAP dâhil) ve z2jh render'ı |
| `e2e-kind` | kind + ArgoCD ile tam kurulum ve tüm yollar (`E2E F1 OK` … `E2E F5 DR OK`) |

Yerel ön kontrol:
```bash
helm unittest glue
helm template glue -f platform/values/glue.yaml >/dev/null && helm template glue -f platform/values/glue-dev.yaml >/dev/null
```
Var olan bir kümede yükseltme sonrası kabul kanıtı: `scripts/acceptance.sh`
(`runbooks/acceptance-tests.md`).

## 7. Geri alma

| Durum | Yol |
|---|---|
| Chart/imaj sürümü kötü | `git revert <commit>` → push → ArgoCD eski sürüme döner (`kubectl -n argocd get applications`) |
| ArgoCD sync yarıda kaldı | `kubectl -n argocd patch app <ad> --type merge -p '{"operation":{"sync":{}}}'`; hâlâ Degraded ise `kubectl -n argocd get app <ad> -o json \| jq -r '.status.conditions[]?'` |
| Kafka `version` (adım 1) | revert edilebilir (metadataVersion eski kaldığı sürece) |
| Kafka `metadataVersion` (adım 2) | **GERİ ALINAMAZ** — dönüş yolu küme yeniden kurulumu + topic'lerin kaynaklardan yeniden üretimi (`runbooks/dr.md` "Kapsam dışı") |
| Postgres (Polaris/Keycloak/Superset) migrasyonu bozuldu | CNPG **PITR**: yükseltme öncesi zaman damgasına restore (`runbooks/dr.md` → `recoveryTarget.targetTime`) |
| Superset operator CR'ı reddediyor | Operator'ı revert edin (CR şeması eski CRD ile uyumlu kalır) |
| Not defteri/PVC kaybı | Velero restore (`runbooks/dr.md` → "Seçili kaynak / PVC geri yükleme") |

Revert'ten sonra `runbooks/versions.md` satırını da eski değere döndürün — liste her zaman kümedeki gerçeği
göstermelidir.
