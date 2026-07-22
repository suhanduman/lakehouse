# tools/ — chart-dışı operasyonel araçlar

**Kapsam:** `tools/`, Lakehouse'un **chart-dışı operasyonel
araç kutusudur** — self-servis scaffold CLI (`templates/scaffold-source.sh`
+ connector/kafkatopic/iceberg şablonları), Spark job'lar (`jobs/*.py`,
spark-py image'ına gömülü) ve statik manifest validator'ı (`validate.sh`
+ `.yamllint`). **Tek doğruluk kaynağı `../chart/`'tır** — kurulum, upgrade
ve rollback yalnızca `helm` ile yapılır (bkz. kök `README.md` →
"HELM İLE KURULUM"). Air-gapped/manuel/Helm'siz senaryolar için
`../manual-install/` altında chart'tan **üretilen** tek-dosya bir render
mevcuttur; bu klasörün kendisi (`tools/`) elle-yönetilen bir manifest seti
**DEĞİLDİR**.

> **NOT:** Bu dokümanın "Adım adım kurulum", "İNGEST OMURGASI →
> Kurulum sırası" ve "Self-Service Console → Kurulum" bölümlerinde geçen
> `oc apply -f 00-namespace.yaml` … `17-console-routes-oidc.yaml` gibi
> komutlar, chart'ın parametreli şablonlarıyla davranışsal olarak birebir
> örtüşen manuel bir kurulum sürecini anlatır. Bu YAML dosyalarının kendisi
> repoda bulunmaz; parametreli/canlı karşılıkları `../chart/templates/`'tedir.
> **Önerilen kurulum tek komut `helm install` iledir** — bkz. kök
> `README.md` → "HELM İLE KURULUM". Bu doküman; (a) davranış-parite
> referansı, (b) hâlâ geçerli olan operasyonel runbook'lar (AD stratejisi,
> imaj build, secret şema/anahtar isimleri, `templates/scaffold-source.sh`
> ile yeni kaynak ekleme, sorun giderme/DLQ komutları — bunların hepsi
> chart'ın ürettiği kaynaklarla da birebir çalışır, çünkü isim/namespace/
> davranış korunmuştur) için saklanır. Bu bölümlerdeki
> `oc apply -f <NN-...>.yaml` komutlarını **Helm'in yönettiği bir
> namespace'e karşı çalıştırmayın** — kaynak çakışması/drift oluşur.

Bu klasör, `poc-manifests/`'in üretim (**OpenShift + kurumsal S3 nesne deposu**)
dengidir. Pilotta (PoC) tek pod olarak çalışan servisler, üretimde HA
yapıda, operatörlerle yönetilen, **kurumsal Active Directory** ile kimlik
doğrulanan ve TLS ile şifreli hale gelir.

## İçindekiler

1. [Önemli uyarı](#önemli-uyarı)
2. [Klasör içeriği](#klasör-içeriği)
3. [Ön koşullar](#ön-koşullar)
4. [Active Directory kimlik doğrulama stratejisi](#active-directory-kimlik-doğrulama-stratejisi)
5. [Kurumsal registry — custom image build](#kurumsal-registry--custom-image-build)
6. [Secret'ları hazırlama](#secretları-hazırlama)
7. [Adım adım kurulum](#adım-adım-kurulum)
8. [İlk giriş ve doğrulama testleri](#ilk-giriş-ve-doğrulama-testleri)
9. [Sorun giderme](#sorun-giderme)
10. [Geri alma (rollback)](#geri-alma-rollback)
11. [İngest omurgası (Debezium CDC + JDBC scheduled + Spark + nginx)](#i̇ngest-omurgası-debezium-cdc--aiven-jdbc-scheduled--spark--nginx)
12. [Self-Service Console](#self-service-console)
13. [PoC → Prod ayrımları](#poc--prod-ayrımları)

---

## Önemli uyarı

Bu yaml'lar **şablondur**, hazır deploy edilmek için değil. Her kurumun
kendi OpenShift versiyonu, S3 endpoint'i, Active Directory yapısı,
Docker registry'si ve domain'i vardır. `{{PLACEHOLDER}}` görünen her
değer CI/CD pipeline'ınız veya GitOps tool'unuz (ArgoCD, Flux, Helm)
tarafından doldurulmalıdır.

Bu paket OpenShift 4.14+ üzerinde test için yazılmıştır. Daha eski
sürümlerde API versiyon uyarıları çıkabilir.

## Klasör içeriği

Bu klasör yalnızca **chart-dışı, hâlâ canlı** operasyonel parçaları içerir
(numaralı, `{{PLACEHOLDER}}`'lı manuel-kurulum YAML'ları repoda bulunmaz —
tek doğruluk kaynağı chart + git geçmişidir):

```
tools/
├── README.md                (bu dosya — kapsam + hâlâ geçerli runbook referansı)
├── validate.sh              Statik YAML/script doğrulama + `--helm` modu
│                            (helm lint + helm template + helm-check.py)
├── .yamllint                yamllint kuralları
├── templates/                CLI self-servis şablonları — HÂLÂ CANLI
│   ├── scaffold-source.sh    Yeni kaynak ekleme sihirbazı (bucket+KafkaTopic+
│   │                          KafkaConnector+Iceberg namespace üretir)
│   └── source-cdc-*.yaml, source-scheduled-*.yaml, kafkatopic.yaml
└── jobs/                      Spark job script'leri — HÂLÂ CANLI (spark-py
    ├── nginx_streaming.py      image'ına gömülür, chart `local://` path ile
    ├── mongo_scheduled_batch.py  referans verir; bunlar chart template'i
    └── iceberg_maintenance.py    DEĞİL, image build girdisidir)
```

`00-namespace.yaml` … `17-console-routes-oidc.yaml` + `optional/07-ai-gpu.yaml`
için parametreli/canlı karşılıklar `../chart/templates/`'tedir (bkz. o
dosyaların başlık yorumlarındaki referans notları).

**Bu pakette YOK — follow-up (Helm chart ile):**
- Superset HA — `helm install superset apache-superset/superset`
- JupyterHub — `helm install jupyterhub jupyterhub/jupyterhub`
- Airflow — `helm install airflow apache-airflow/airflow`
- Observability (Prometheus ServiceMonitor + Grafana dashboard) — her
  servisin kendi exporter'ı zaten aktif; standart ServiceMonitor pattern'i.

## Ön koşullar

### OpenShift küme bileşenleri (OperatorHub'dan)

| Operator                 | Namespace    | Rol                                 |
|--------------------------|--------------|-------------------------------------|
| CloudNativePG (CNPG)     | `cnpg-system`| Postgres HA — Nessie metadata DB   |
| Strimzi                  | `openshift-operators` | Kafka HA              |
| Spark Operator           | `spark-system` | SparkApplication CRD              |
| NVIDIA GPU Operator      | `nvidia-gpu-operator` | AI katmanı için GPU     |
| cert-manager             | `cert-manager` | TLS sertifikaları                 |
| OpenShift Logging / Loki | cluster       | Merkezi log                         |
| Prometheus / Grafana     | cluster       | OpenShift'in built-in monitoring'i |
| ExternalSecrets Operator | `external-secrets` | Vault/AD'dan Secret çekmek için|
| Keycloak Operator        | `keycloak-system` | AD → OIDC bridge              |

### Dış servisler

- **Kurumsal S3 nesne deposu** endpoint'i erişilebilir. Bucket'lar önceden
  oluşturulmuş olmalı: `depo`, `ham-veri`, `backups`.
- **Active Directory** erişilebilir, LDAPS (636/TCP) açık, bir
  service account oluşturulmuş (`svc-lakehouse-ad`).
- **Kurumsal Docker Registry** — Jupyter/AI UI/Spark için custom
  imajlar önceden build edilmiş (bkz. "Custom image build").
- **HashiCorp Vault veya Sealed Secrets** — secret'ları yönetmek için.

### Küme operatörlerini doğrulama

```bash
# OperatorHub'da yüklü operatörleri listele
oc get subscriptions.operators.coreos.com -A | \
  grep -E "cloudnative-pg|strimzi|spark-operator|gpu-operator|cert-manager|external-secrets"

# Vendor CSI driver / StorageClass(lar)ınız (chart varsayılanı `components.
# storage: true` iken lakehouse-block/lakehouse-file-rwx; örnek: Pure
# Storage PSO)
oc get storageclass
# Beklenen: lakehouse-block, lakehouse-file-rwx (ya da kendi StorageClass adlarınız)

# Test volume provision
cat <<EOF | oc apply -f - && oc get pvc test-pvc -n default
apiVersion: v1
kind: PersistentVolumeClaim
metadata: {name: test-pvc, namespace: default}
spec:
  accessModes: [ReadWriteOnce]
  resources: {requests: {storage: 1Gi}}
  storageClassName: lakehouse-block
EOF
oc delete pvc test-pvc -n default
```

## Active Directory kimlik doğrulama stratejisi

Kurumsal Active Directory **OIDC protokolü konuşmaz** (yalnızca LDAP/
Kerberos). Modern uygulamalar (Trino, Superset, Jupyter, AI UI) ise
OIDC ister. Arada **Keycloak**'ı federation broker olarak konumlandırıyoruz:
Keycloak AD'yi "user federation" olarak tanır, uygulamalara OIDC token
dağıtır. Bu bir tercih değil, AD'nin teknik sınırıdır — Keycloak ZORUNLU.

Üç ayrı mekanizma birlikte çalışır:

1. **AD → Keycloak (OIDC Provider)** — Trino, Superset, Jupyter, AI UI,
   Nessie için tercih edilen yol. `10-keycloak.yaml` bunu tanımlar;
   KeycloakRealmImport CR ile AD federation + 5 OIDC client deklaratif
   olarak kurulur (UI'dan manuel konfig yok).
2. **AD → doğrudan LDAPS (Shiro)** — **Zeppelin** için. Apache Shiro
   `ActiveDirectoryGroupRealm` AD'ye bind edip grup üyeliğinden rol
   çıkarır. Keycloak'ı atlar çünkü Zeppelin'in OIDC desteği henüz
   olgun değil. 09-zeppelin.yaml içinde yapılandırılmıştır.
3. **OpenShift OAuth Proxy (oauth2-proxy)** — ileride eklenecek,
   AuthN native olmayan servisler için sidecar proxy pattern'i.

### Servis bazında kullanılan yöntem

| Servis          | Yöntem                   | Nereden |
|-----------------|--------------------------|---------|
| Trino           | OIDC (Keycloak/AD FS)    | native `http-server.authentication.type=oauth2` |
| Superset        | OIDC (Keycloak)          | native Superset OIDC config |
| AI UI (Streamlit) | OIDC via oauth2-proxy   | sidecar, `X-Forwarded-User` header |
| JupyterHub      | OIDC (Keycloak)          | native Z2J `c.JupyterHub.authenticator_class = 'oauthenticator.oidc.OIDCAuthenticator'` |
| **Zeppelin**    | **LDAPS doğrudan AD**    | Apache Shiro ActiveDirectoryRealm (09-zeppelin.yaml) |
| Nessie          | OIDC (Keycloak)          | Quarkus OIDC |
| MinIO/S3 nesne deposu | IAM user (bucket policy) | statik credential (Vault) |

### AD tarafında hazırlık

Kurumsal AD yöneticisinden şunlar talep edilmeli:

1. **Service account**: `svc-lakehouse-ad`, salt-okuma hakkı
   (`OU=Users` ve `OU=Groups` alt OU'larını okuyabilir).
2. **Gruplar**: `lakehouse-admins`, `lakehouse-analysts`,
   `lakehouse-students` — rol eşlemesi için.
3. **LDAPS sertifikası**: AD DC'nin root CA bundle'ı (ca-bundle.pem)
   — cert-manager ile ClusterIssuer olarak sisteme ekleyin.

### Keycloak LDAP federation (özet)

Keycloak Admin UI → User Federation → LDAP → Add Provider:
- Vendor: Active Directory
- Connection URL: `ldaps://ad.example.com:636`
- Users DN: `OU=Users,DC=example,DC=com`
- Bind DN: `CN=svc-lakehouse-ad,OU=Service Accounts,...`
- Bind credential: [service account şifresi]
- Sync periods: Full 24h / Changed 1h

Sonrasında "Group Mapper" ile AD gruplarını Keycloak rollerine eşleyin.

Keycloak client'lar: `trino`, `superset`, `jupyter`, `ai-ui`, `nessie`
(her biri ayrı client ID, ayrı secret).

## Kurumsal registry — custom image build

PoC'de runtime `pip install` yapılıyor; üretimde bu YASAK. Her servis
için özel imaj build edip kurumsal registry'ye push edin.

### Jupyter custom image

```dockerfile
# images/jupyter/Dockerfile
FROM jupyter/all-spark-notebook:spark-3.5.0
USER root
RUN pip install --no-cache-dir \
      "pyiceberg[s3fs,pyarrow]==0.7.1" \
      pandas==2.2.2 numpy==1.26.4 \
      scikit-learn==1.5.1 xgboost==2.1.0 \
      matplotlib seaborn plotly
USER jovyan
```

### AI UI (Streamlit) custom image

```dockerfile
# images/ai-ui/Dockerfile
FROM python:3.11-slim
RUN pip install --no-cache-dir \
      streamlit==1.34.0 trino==0.328.0 \
      requests==2.32.3 pandas==2.2.2 sqlparse==0.5.0 \
      pyiceberg[pyarrow,s3fs] \
      authlib streamlit-oauth
WORKDIR /app
COPY app.py /app/app.py
EXPOSE 8888
ENTRYPOINT ["streamlit","run","app.py",\
            "--server.port=8888","--server.address=0.0.0.0"]
```

### Spark job image (streaming_job.py + iceberg_maintenance.py gömülü)

```dockerfile
# images/spark-py/Dockerfile
FROM apache/spark:3.5.1-java17
USER root
RUN pip install --no-cache-dir pyspark==3.5.1 pyiceberg[pyarrow,s3fs]
COPY jobs/ /opt/spark/jobs/
USER spark
```

Spark `iceberg_maintenance.py` örneği (katalogdaki tüm tabloları tarayıp
compaction + snapshot expire + orphan file temizliği yapar):

```python
# images/spark-py/jobs/iceberg_maintenance.py
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("IcebergMaintenance").getOrCreate()

for catalog in ["lakehouse", "rawlake"]:
    try:
        namespaces = [r.namespace for r in
                      spark.sql(f"SHOW NAMESPACES IN {catalog}").collect()]
    except Exception as e:
        print(f"[skip] {catalog}: {e}")
        continue
    for ns in namespaces:
        if ns == "information_schema":
            continue
        tables = [r.tableName for r in
                  spark.sql(f"SHOW TABLES IN {catalog}.{ns}").collect()]
        for t in tables:
            fq = f"{catalog}.{ns}.{t}"
            print(f"[maintain] {fq}")
            try:
                spark.sql(f"CALL {catalog}.system.rewrite_data_files(table => '{ns}.{t}')")
                spark.sql(f"CALL {catalog}.system.expire_snapshots(table => '{ns}.{t}')")
                spark.sql(f"CALL {catalog}.system.remove_orphan_files(table => '{ns}.{t}')")
            except Exception as e:
                print(f"[error] {fq}: {e}")
spark.stop()
```

### Zeppelin custom image (Spark gömülü — init-container YOK)

```dockerfile
# images/zeppelin/Dockerfile
FROM apache/zeppelin:0.12.0
USER root
RUN curl -sSfL https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz \
      | tar xz -C /opt && \
      mv /opt/spark-3.5.1-bin-hadoop3 /opt/spark && \
      chown -R 1000:1000 /opt/spark
ENV SPARK_HOME=/opt/spark
USER 1000
```

### Build + push (OpenShift internal registry)

```bash
# Her image için:
oc new-build --strategy=docker --name=jupyter-lakehouse --binary -n example
oc start-build jupyter-lakehouse --from-dir=./images/jupyter --follow -n example

oc new-build --strategy=docker --name=ai-ui --binary -n example
oc start-build ai-ui --from-dir=./images/ai-ui --follow -n example

oc new-build --strategy=docker --name=spark-py --binary -n example
oc start-build spark-py --from-dir=./images/spark-py --follow -n example

# ImageStream URL'leri al
oc get is -n example
# Yaml içindeki registry.apps.ocp.example.com/lakehouse/... yerlerini
# bu URL'lerle değiştirin (sed veya Kustomize ile).
```

## Secret'ları hazırlama

### 1. S3 credentials

```bash
oc create secret generic s3-credentials -n example \
  --from-literal=access-key-id="$S3_ACCESS" \
  --from-literal=secret-access-key="$S3_SECRET" \
  --from-literal=endpoint="https://s3.example.com" \
  --from-literal=region="us-east-1"
```

### 2. AD bind credentials

```bash
oc create secret generic ad-bind-credentials -n example \
  --from-literal=bind-dn="CN=svc-lakehouse-ad,OU=Service Accounts,DC=example,DC=com" \
  --from-literal=bind-password="$AD_PASS" \
  --from-literal=user-search-base="OU=Users,DC=example,DC=com" \
  --from-literal=group-search-base="OU=Groups,DC=example,DC=com" \
  --from-literal=ad-url="ldaps://ad.example.com:636"
```

### 3. OIDC credentials (Keycloak)

```bash
oc create secret generic oidc-credentials -n example \
  --from-literal=issuer-url="https://auth.example.com/realms/example" \
  --from-literal=trino-client-id="trino" \
  --from-literal=trino-client-secret="$TRINO_SEC" \
  --from-literal=superset-client-id="superset" \
  --from-literal=superset-client-secret="$SUP_SEC" \
  --from-literal=jupyter-client-id="jupyter" \
  --from-literal=jupyter-client-secret="$JUP_SEC"
```

**Not**: Üretimde `oc create secret` yerine ExternalSecret CR ile Vault'tan
çekin. Secret'lar git'e commit edilmez.

## Adım adım kurulum

```bash
NS=example
```

### 1. Namespace

```bash
oc apply -f 00-namespace.yaml
oc get namespace $NS                       # Active durumda olmalı
oc get resourcequota -n $NS                 # kota tanımlı
oc get networkpolicy -n $NS                 # 3 policy
```

### 2. Storage

```bash
oc apply -f 01-storage.yaml
oc get storageclass                          # e.g. lakehouse-block + lakehouse-file-rwx (or your own pre-existing StorageClass)
# Secret'ları doğrula (kısaltılmış değerler)
oc get secrets -n $NS | grep -E "s3-credentials|ad-bind|oidc|nessie-db"
```

### 3. Postgres HA (Nessie metadata için)

```bash
oc apply -f 02-postgres-ha.yaml
# İlk defa ~3-5 dk; 3 instance initdb + streaming replication kurar
oc wait --for=condition=Ready cluster.postgresql.cnpg.io/pg-nessie \
  -n $NS --timeout=600s
oc get cluster.postgresql.cnpg.io -n $NS
# CNPG otomatik üretilen secret'ı doğrula:
oc get secret pg-nessie-app -n $NS -o jsonpath='{.data.username}' | base64 -d
# → "nessie"
```

### 4. Kafka HA

```bash
oc apply -f 03-kafka-strimzi.yaml
oc wait kafka/kafka -n $NS --for=condition=Ready --timeout=600s
oc get kafka,kafkatopic,kafkauser -n $NS
# Topic'lerin yaratıldığını doğrula:
oc get kafkatopic -n $NS
# → nginx-access, lms-events, connect-cluster-configs/offsets/status
```

### 5. Nessie

```bash
oc apply -f 04-nessie-ha.yaml
oc rollout status deploy/nessie -n $NS --timeout=300s

# Iceberg REST endpoint doğrulaması (cluster içinden):
oc run probe --rm -i --restart=Never --image=curlimages/curl -n $NS -- \
  sh -c 'curl -s http://nessie:19120/iceberg/v1/config?warehouse=warehouse | head -c 200'
# → {"defaults":{"warehouse":"s3://depo/warehouse","prefix":"main"...

# İkinci warehouse doğrulaması:
oc run probe --rm -i --restart=Never --image=curlimages/curl -n $NS -- \
  sh -c 'curl -s http://nessie:19120/iceberg/v1/config?warehouse=rawdata | head -c 200'
# → {"defaults":{"warehouse":"s3://ham-veri/warehouse"...
```

### 6. Spark Operator şablonları

```bash
oc apply -f 05-spark-operator.yaml
# SparkApplication CR'ı henüz başlatma; önce image build tamamlanmalı.
oc get configmap spark-defaults -n $NS
oc get sparkapplications -n $NS
```

### 7. Trino HA

```bash
oc apply -f 06-trino-ha.yaml
oc rollout status deploy/trino-coordinator -n $NS --timeout=300s
oc rollout status deploy/trino-worker -n $NS --timeout=300s

# Basit sorgu:
oc exec -n $NS deploy/trino-coordinator -- trino --execute "SHOW CATALOGS"
# → lakehouse, rawlake, system, ...

# Worker sayısı:
oc exec -n $NS deploy/trino-coordinator -- \
  trino --execute "SELECT count(*) FROM system.runtime.nodes"
# → 4 (1 coordinator + 3 worker)
```

### 8. AI katmanı (GPU)

```bash
# Önce GPU node pool hazır mı kontrol et:
oc get nodes -l ai-node=true
# En az 1 node görmelisin. Yoksa MachineSet ile oluşturun.

oc apply -f 07-ai-gpu.yaml
oc rollout status deploy/ollama -n $NS --timeout=600s  # GPU schedule + image pull
oc logs -f job/ollama-pull-qwen14b -n $NS              # ~8 GB model download

# Model indirildi mi?
oc exec -n $NS deploy/ollama -- ollama list
# → qwen2.5-coder:14b  ...GB

# UI replica'ları
oc rollout status deploy/text-to-sql-ui -n $NS --timeout=300s
oc get pods -n $NS -l app=text-to-sql-ui
```

### 9. Zeppelin + Active Directory

```bash
oc apply -f 09-zeppelin.yaml
# Init container Spark kopyalar (~30 sn), ardından Zeppelin başlar (~2 dk)
oc rollout status deploy/zeppelin -n $NS --timeout=600s

# Shiro config okundu mu?
oc logs deploy/zeppelin -n $NS | grep -i "shiro\|realm"
# AD'ye bağlanabildiği mesajı görmelisin
```

### 10. Keycloak — OIDC Provider + AD federation

```bash
oc apply -f 10-keycloak.yaml
# Keycloak için ayrı Postgres cluster ~3-5 dk
oc wait --for=condition=Ready cluster.postgresql.cnpg.io/pg-keycloak \
  -n $NS --timeout=600s
# Keycloak 2 replica ayağa kalkar ~2 dk
oc rollout status deploy/keycloak -n $NS --timeout=600s
# Realm import operator tarafından otomatik uygulanır:
oc get keycloakrealmimport example-realm -n $NS \
  -o jsonpath='{.status.conditions}' | jq
# "Done: true" görmelisiniz.

# Trino/Superset/AI UI için client secret'larını Keycloak'tan alıp
# oidc-credentials Secret'ına yazın:
for app in trino superset jupyter ai-ui nessie; do
  # Keycloak Admin CLI (kcadm) ile secret çek:
  SECRET=$(oc exec -n $NS deploy/keycloak -- /opt/keycloak/bin/kcadm.sh \
    get "clients?clientId=$app" --realm=example --fields=secret -q | jq -r '.[0].secret')
  echo "$app secret: $SECRET"
  # Sonra oidc-credentials Secret'ına ekleyin (ExternalSecret pattern'i ile)
done
```

### 11. OpenShift Routes + TLS

```bash
oc apply -f 08-routes-tls.yaml
# cert-manager TLS sertifikalarını birkaç dakika içinde üretir
oc get routes -n $NS
# STATUS: Admitted ve HOST alanı dolmuş olmalı.

# Sertifika durumu:
oc get certificate -n $NS
# Ready: True olmalı (birkaç dakika sonra)

# Dışarıdan test:
curl -v https://trino.lakehouse.example.com/v1/info
curl -v https://zeppelin.lakehouse.example.com/api/version
curl -v https://ai.lakehouse.example.com/_stcore/health
```

## İlk giriş ve doğrulama testleri

### Trino SQL — AD kullanıcısıyla

Tarayıcıda `https://trino.lakehouse.example.com` → OIDC login →
Keycloak sayfası → AD kullanıcı adı/şifre. Sonra Trino Web UI görünür.

Komut satırı:
```bash
trino --server https://trino.lakehouse.example.com \
      --external-authentication \
      --execute "SHOW CATALOGS"
```

### Zeppelin — AD grubuna göre erişim

`https://zeppelin.lakehouse.example.com` → Shiro login sayfası →
AD kullanıcı adı + şifre. Grup üyeliği:
- `lakehouse-admins` grubundaysanız → tüm interpreter'ları değiştirebilirsiniz.
- `lakehouse-analysts` → notebook oluştur + çalıştır.
- `lakehouse-students` → yalnızca okuma.

İlk Spark notebook'u:
```
%spark.sql
SHOW CATALOGS
```
sonuç: `lakehouse, rawlake, spark_catalog`.

### AI UI — doğal dilde sorgu

`https://ai.lakehouse.example.com` → OIDC login → ekran açılır.
Test sorusu: "Toplam kaç çalışan var?" → `SELECT count(*) FROM
lakehouse.hr.employees` → sayı döner.

### Veri pipeline doğrulaması

Pilottaki `hr.employees` verisi üretime taşınırken Spark CTAS veya
Debezium CDC ile doldurulur. İlk smoke test için küçük bir Iceberg
tablosu:

```bash
# Spark SQL paragrafı Zeppelin'de:
%spark.sql
CREATE NAMESPACE IF NOT EXISTS lakehouse.test;
CREATE TABLE lakehouse.test.smoke (id INT, note STRING) USING iceberg;
INSERT INTO lakehouse.test.smoke VALUES (1,'prod-ok');
SELECT * FROM lakehouse.test.smoke;
```

Trino'da da aynı sonucu görmelisiniz:
```sql
SELECT * FROM lakehouse.test.smoke;
```

## Sorun giderme

### Nessie /iceberg 404
- Warehouse env var'ı eksik. `oc get deploy nessie -o yaml | grep WAREHOUSE`
- Secret URN hatalı (hyphen'li secret adı); 04-nessie-ha.yaml'da
  `miniocreds` → `fbcreds` tutarlı mı.

### Trino AD login çalışmıyor
- OIDC redirect URI Keycloak'ta tanımlı mı?
  `https://trino.lakehouse.example.com/oauth2/callback`
- `oc logs deploy/trino-coordinator | grep -i oauth` loglara bakın.

### Zeppelin Shiro "Connection timeout"
- LDAPS 636 portu küme dışına açık mı? NetworkPolicy ekleme gerekebilir.
- AD root CA'sını Zeppelin truststore'una ekleyin (ConfigMap ile mount).

### GPU pod Pending
- `oc describe pod <ollama-pod>` — "insufficient nvidia.com/gpu".
- `oc get nodes -l ai-node=true` — GPU node var mı?
- NVIDIA GPU Operator `oc get clusterpolicy gpu-cluster-policy` Ready mi?

### CTAS "Warehouse not known"
- Spark config'inde `warehouse=s3://depo/warehouse` ile Nessie env'indeki
  değer birebir eşleşmeli — `s3://` vs `s3a://` karışıklığı.

### S3 upload "InvalidArgument checksum"
- AWS CLI v2.23+ default checksum MinIO-uyumsuz; export edin:
  ```bash
  export AWS_REQUEST_CHECKSUM_CALCULATION=when_required
  export AWS_RESPONSE_CHECKSUM_VALIDATION=when_required
  ```

## Geri alma (rollback)

> **Helm-tabanlı kurulumda** rollback tek komuttur: `helm rollback lakehouse
> <revision> -n example` (bkz. kök `README.md`); aşağıdaki manuel
> `oc rollout undo`/`oc delete -f <NN>.yaml` döngüsü yalnızca bu dokümanın
> "Adım adım kurulum" bölümüyle anlatılan manuel kurulum yolu içindir —
> bu numaralı YAML dosyaları repoda bulunmaz.

Bir servis bozulduğunda etki izole; diğerleri çalışmaya devam eder.

```bash
# Tek servis geri al:
oc rollout undo deploy/<servis-adı> -n example

# Nessie'yi önceki image'a döndür (örnek):
oc set image deploy/nessie nessie=ghcr.io/projectnessie/nessie:0.99.0 -n example

# Tam küme temizliği, manuel kurulumlar için
# (TÜM VERİ KAYBEDİLİR — yalnızca dev namespace'te). Helm-tabanlı kurulumda
# bunun yerine tek komut yeterlidir:
helm uninstall lakehouse -n example
```

S3 bucket'ları (`depo`, `ham-veri`) bu komutla silinmez —
manuel silinmeli (veya tutulmalı).

## İNGEST OMURGASI (Debezium CDC + Aiven JDBC scheduled + Spark + nginx)

Bu bölüm, ilişkisel/CDC kaynaklarını (MSSQL, MongoDB) ve web erişim
loglarını (nginx) Kafka üzerinden Iceberg'e akıtan "ingest omurgası"nı
kurar: Apicurio Registry (Avro şema deposu) → Kafka Connect (Strimzi
`spec.build` ile derlenen custom imaj) → kaynak konnektörleri (Debezium
CDC + Aiven JDBC scheduled) → Iceberg fan-out sink → Spark Structured
Streaming (nginx) + Iceberg bakım (Spark). Yeni bir kaynak eklemek
`templates/scaffold-source.sh` ile şablonlaştırılmıştır.

### Ön hazırlık

1. **Registry push secret** (Kafka Connect `spec.build.output.pushSecret`
   OpenShift internal registry'ye push edebilmek için gerekir —
   `12-kafka-connect.yaml` içinde `connect-build-push-secret` adıyla
   referanslanır):
   ```bash
   oc create secret docker-registry connect-build-push-secret \
     --docker-server=image-registry.openshift-image-registry.svc:5000 \
     --docker-username=$(oc whoami) --docker-password=$(oc whoami -t) -n example
   ```
2. **Kaynak external-config secret'ları** — `12-kafka-connect.yaml` içindeki
   `externalConfiguration.volumes` bloğu, her konnektörün
   `${file:/opt/kafka/external-configuration/<ad>/<key>:key}`
   söz dizimiyle okuduğu `user`/`pass` anahtarlarını mount eder:
   ```bash
   oc create secret generic mssql --from-literal=user=... --from-literal=pass=... -n example
   oc create secret generic mongo --from-literal=user=... --from-literal=pass=... -n example
   ```
   (`s3-credentials` zaten `access-key-id`/`secret-access-key`
   anahtarlarıyla mevcut — bkz. "Secret'ları hazırlama".) **Yeni bir kaynak
   tipi** (mssql/mongo dışında, ör. başka bir MSSQL/PG instance'ı) ilk kez
   ekleniyorsa, secret'ı oluşturmanın yanında `externalConfiguration.volumes`
   listesine de yeni bir `{name, secret}` girdisi eklenmeli —
   `scaffold-source.sh` bunu otomatik yapmaz. Helm-tabanlı kurulumda bu liste
   `chart/templates/12-kafka-connect.yaml` içinde tanımlıdır (bkz. dosyanın
   başlık yorumu); bu template düzenlenip `helm upgrade` ile uygulanır —
   doğrudan `oc apply` ile uygulanmaz.

### Kurulum sırası

1. `oc apply -f 11-apicurio-registry.yaml`
   ```bash
   oc wait --for=condition=Ready cluster.postgresql.cnpg.io/pg-apicurio -n example --timeout=600s
   oc rollout status deploy/apicurio-registry -n example --timeout=300s
   ```
2. `oc apply -f 03-kafka-strimzi.yaml` (`connect`/`debezium-src`/
   `fluentbit` KafkaUser'larını + `connect-cluster-*` internal
   topic'lerini içerir) →
   ```bash
   oc get kafkatopic -n example   # connect-cluster-configs/offsets/status → RF=3
   oc get kafka kafka -n example -o jsonpath='{.status.listeners}'  # external (9094, route) Admitted
   ```
3. `oc apply -f 12-kafka-connect.yaml` →
   ```bash
   oc wait kafkaconnect/connect -n example --for=condition=Ready --timeout=600s
   # build birkaç dk sürer (Debezium MSSQL/Mongo + Aiven JDBC + Iceberg sink + Apicurio converter)
   oc get kafkaconnectbuild -n example
   ```
4. Bucket + namespace: temsilî kaynaklar için `templates/scaffold-source.sh`
   (aşağıdaki "Yeni kaynak ekle" runbook'u) veya elle `aws s3 mb` +
   `CREATE NAMESPACE`.
5. `oc apply -f 13-connectors/iceberg-sink-cdc.yaml` →
   `oc get kafkaconnector iceberg-sink-cdc -n example` → `RUNNING`.
6. `oc apply -f 13-connectors/dbz-mssql-students.yaml -f 13-connectors/jdbc-mssql-scheduled.yaml -f 13-connectors/dbz-mongo-lms.yaml` →
   `oc get kafkaconnector -n example` → hepsi `RUNNING`.
7. nginx: `14-nginx-ingest/README.md` (VM ajanı — FluentBit kurulumu) +
   `oc apply -f 14-nginx-ingest/spark-nginx-streaming.yaml`
   (`SparkApplication/nginx-streaming` + `KafkaUser/spark-nginx`).
8. `oc apply -f 05-spark-operator.yaml` →
   `oc get scheduledsparkapplication iceberg-maintenance -n example`.

### Yeni kaynak ekle (runbook)

0. Tip seç: `cdc` (update/delete, near-realtime) veya `scheduled`
   (periyodik, bilinen SQL; CDC feature GEREKMEZ).
1. Kaynak önkoşulu:
   - **CDC-MSSQL**: `EXEC sys.sp_cdc_enable_db;` + tablo için
     `sp_cdc_enable_table`; SQL Server Agent açık olmalı.
   - **CDC-Mongo**: replica set (`rs0`) şart — `change_streams_update_full`
     capture mode için. **CDC-PG**: `wal_level=logical` + replication
     slot + publication.
   - **SCHEDULED**: read-only kullanıcı + incrementing veya timestamp
     kolonu (`mode: timestamp+incrementing` gibi) yeterli.
2. Credential secret:
   ```bash
   oc create secret generic <source> --from-literal=user=... --from-literal=pass=... -n example
   ```
   Yeni kaynak tipi mssql/mongo dışındaysa "Ön hazırlık"taki
   `externalConfiguration.volumes` notunu uygulayın.
3. Scaffold:
   ```bash
   templates/scaffold-source.sh --source mssql1 --kind cdc --type mssql --db OgrenciDB \
     --table dbo.students --target-ns mssql_ogrenci --target-table students
   ```
   (bucket + KafkaTopic + connector CR + namespace DDL üretir;
   `--dry-run` ile önce çıktıyı görün.) Örnek üretilen isimler:
   topic `cdc.mssql1.dbo.students` (CDC) veya `jdbc.mssql1.ogrencidb.<tablo>`
   (scheduled — JDBC prefix `jdbc.<source>.<db-küçükharf>.` üretir; `--db
   OgrenciDB` → `jdbc.mssql1.ogrencidb.`), Iceberg hedefi
   `lakehouse.mssql_ogrenci.students`. Topic adı yalnızca gözlem içindir;
   Iceberg sink yönlendirmesi topic adına değil, SMT ile set edilen
   `_target_table` alanına göredir.
4. Namespace DDL'ini Trino'da çalıştırın (scaffold `--dry-run` çıktısı).
5. Doğrula:
   ```bash
   oc get kafkaconnector -n example                 # yeni connector RUNNING
   trino --execute "SELECT count(*) FROM lakehouse.mssql_ogrenci.students"
   ```

### Doğrulama (kabul)

- **Fonksiyonel**: MSSQL insert/update/delete → Trino'da yansır (CDC'de
  delete dahil — `dbz-mssql-students` SMT `ExtractNewRecordState` ile
  `delete.handling.mode: rewrite`).
- **Federasyon**: kaynaklar arası JOIN —
  ```sql
  SELECT ... FROM lakehouse.mssql_ogrenci.students s
    JOIN lakehouse.web.access_log a ON ...
  ```
- **Restart**: `oc delete pod -l strimzi.io/cluster=connect,strimzi.io/kind=KafkaConnect -n example`
  → CDC re-snapshot yapmadan devam eder (offset topic'lerinden resume).
- **DLQ**: (broker pod içinden, internal `plain` listener 9092, auth yok)
  ```bash
  oc exec -it kafka-broker-0 -n example -- bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 --topic iceberg-sink.dlq --from-beginning
  ```
  (Kafka broker pod adı Strimzi KafkaNodePool ile `<cluster>-<pool>-<idx>`
  şeklindedir → `kafka-broker-0`; `kafka-0` DEĞİL.)

### Sorun giderme

- `oc get kafkaconnector <ad> -n example -o jsonpath='{.status.connectorStatus.tasks}'`
  → `FAILED` task'ların trace'i.
- Build fail: `oc logs -l strimzi.io/kind=KafkaConnectBuild -n example`.
- Checkpoint tuzağı: Spark streaming checkpoint S3'te olmalı
  (`s3://nginx-web/_checkpoints/access_log`), `emptyDir` DEĞİL.
- `s3://` vs `s3a://`: Trino/Spark/Iceberg sink aynı şemayı kullanmalı
  (Spark `s3a://` — Hadoop S3A; Iceberg sink `S3FileIO` çıplak `s3://`).
- Internal listener karışıklığı: Kafka Connect + Debezium schema-history
  9093 (TLS + SCRAM) kullanır; broker pod içi konsol araçları (`plain`,
  9092, auth yok) yalnızca cluster-içi debug için kullanılabilir. nginx
  VM ajanları (FluentBit) 9094 (external route + SCRAM) üzerinden bağlanır.

## Self-Service Console

Bu bölüm, ingest omurgasına (yukarıdaki "İNGEST OMURGASI" bölümü) bir
**self-servis yönetim arayüzü** ekler: analistler/öğrenciler artık her
yeni kaynak için `oc apply`/`templates/scaffold-source.sh` çalıştırmak veya
YAML yazmak zorunda değil — **Lakehouse Console** (FastAPI backend
`console-backend` + React/Vite frontend `console-frontend`, statik nginx
üzerinden servis edilir) üzerinden aynı işlemleri bir web arayüzünden yapar.
Ayrıca operasyon/derinlemesine-inceleme için gömülü bir OSS arayüz —
**Kafka UI** (`kafbat-ui`, `provectus/kafka-ui`'nin community fork'u,
Apache-2.0) — devreye girer.

**Mimari not — GitOps'a bağımlı DEĞİL:** Console backend, `KafkaConnector`/
`KafkaTopic` CR'lerini kendi `lakehouse-console` ServiceAccount'ıyla
**doğrudan Kubernetes API'sine** (`kafka.strimzi.io` CRD'leri + `Secret`
CRUD) yazar (bkz. `console/backend/app/services/k8s_service.py`); ingest
omurgasının geri kalanının aksine (tek doğruluk kaynağı `chart/templates/
13-connectors.yaml` + `chart/values.yaml` `connectors.*`'dır) Console'un
ürettiği kaynaklar Git'e/chart'a commit edilmeden kümeye uygulanır — bu
bilinçli bir tasarım kararıdır (self-servis hız), ama demek ki **Console
üzerinden eklenen kaynaklar chart'ın
`values`'ıyla otomatik senkron değildir**; periyodik olarak
`oc get kafkaconnector -n example` çıktısını `helm get manifest lakehouse -n example`
ile karşılaştırıp drift'i chart `values`'ına geri yansıtın.

### Kurulum

> Helm-tabanlı kurulumda adım 1 (imaj build) aynen geçerlidir; adım 2'deki
> `oc apply -f 15/16/17-*.yaml` yerine `console.enabled`/`kafkaUi.enabled`
> ile `helm install`/`helm upgrade` kullanılır (chart karşılıkları:
> `chart/templates/console/{console,kafka-ui,routes-oidc}.yaml`). Aşağıdaki
> adımlar manuel kurulum süreci + davranış referansıdır.

1. **İmajları build edin** ("Kurumsal registry — custom image build"
   bölümündeki desenle birebir aynı — `oc new-build`/`oc start-build`):
   ```bash
   oc new-build --strategy=docker --name=console-backend --binary -n example
   oc start-build console-backend --from-dir=./console/backend --follow -n example

   oc new-build --strategy=docker --name=console-frontend --binary -n example
   oc start-build console-frontend --from-dir=./console/frontend --follow -n example
   ```
   `15-lakehouse-console.yaml` bu imajları
   `image-registry.openshift-image-registry.svc:5000/example/console-backend:1.0`
   ve `.../console-frontend:1.0` adlarıyla referans verir — internal
   registry'de bu etiketlerin var olduğundan emin olun (`oc get is -n example`).
2. **Manifestleri sırayla uygulayın:**
   ```bash
   oc apply -f 15-lakehouse-console.yaml
   oc apply -f 16-kafka-ui.yaml
   oc apply -f 17-console-routes-oidc.yaml
   ```
   (`15-lakehouse-console.yaml`: `lakehouse-console` ServiceAccount +
   namespace-scoped Role/RoleBinding + backend/frontend Deployment/Service +
   Route + NetworkPolicy. `16-kafka-ui.yaml`: Kafka UI Deployment/Service +
   config + `KafkaUser`. `17-console-routes-oidc.yaml`: Kafka UI Route +
   her iki uygulamanın Keycloak OIDC client secret'ları — Console Route'u
   zaten `15-lakehouse-console.yaml` içinde tanımlı, burada TEKRARLANMAZ.)
3. **Keycloak client secret'ları:**
   - `lakehouse-console` client'ı **PUBLIC** (SPA + PKCE S256,
     `10-keycloak.yaml`'daki patch) — client-secret **gerekmez**, yalnızca
     `clientId: lakehouse-console` kullanılır (`15-lakehouse-console.yaml`
     `OIDC_AUDIENCE` alanıyla eşleşir).
   - `kafka-ui` client'ı **confidential** — secret'ı
     `17-console-routes-oidc.yaml` içindeki `kafka-ui-oidc-credentials`
     Secret'ının `client-secret` anahtarından okunur
     (`{{KAFKA_UI_OIDC_CLIENT_SECRET}}` placeholder'ını gerçek değerle
     doldurun/ExternalSecret ile besleyin).
   - Kafka UI'nin kendi SCRAM kimliği (`KafkaUser/kafka-ui`,
     `16-kafka-ui.yaml`) Strimzi tarafından otomatik üretilir — aynı adlı
     `kafka-ui` Secret'ının `password` anahtarını elle oluşturmanıza
     gerek yoktur.
4. **SA/RBAC doğrulaması** (least-privilege — `lakehouse-console`
   ServiceAccount'ının **yalnızca** `example` namespace'inde, **yalnızca**
   `kafkaconnectors`/`kafkatopics`/`secrets` üzerinde yetkisi olmalı,
   cluster-admin YOK, ClusterRole YOK):
   ```bash
   oc auth can-i --as=system:serviceaccount:example:lakehouse-console \
     create kafkaconnectors.kafka.strimzi.io -n example     # beklenen: yes
   oc auth can-i --as=system:serviceaccount:example:lakehouse-console \
     create kafkaconnectors.kafka.strimzi.io -n default   # beklenen: no
   ```

### Kullanım

- **Kaynak ekle:** Console'daki sihirbaz (`/sources/add`) formu doldurup
  önce **preview** (`POST /api/sources/preview` — hiçbir K8s/S3/Trino
  client'ı çağırmadan, yalnızca render edilecek `KafkaConnector`/
  `KafkaTopic` CR'lerini + namespace DDL'ini gösterir), sonra **apply**
  (`POST /api/sources` — `AddSourceOrchestrator` üzerinden gerçek
  bucket/namespace/connector'ı oluşturur) adımlarından geçer. Bu, CLI
  runbook'unun (`templates/scaffold-source.sh --dry-run` → gerçek çalıştırma)
  GUI karşılığıdır — aynı sonuca (bucket + `KafkaTopic` + `KafkaConnector` +
  Iceberg namespace) ulaşır.
- **İzle:** Durum panosu (`/status`, `GET /api/status`) her connector için
  `state`/`maintenance` (PAUSED)/`dlq` (herhangi bir task `FAILED`) sinyalini
  gösterir (consumer-lag şu an `None` — Kafka Connect REST API'si lag
  sağlamaz, bkz. "Açık POC-DOĞRULA"); daha derin inceleme (mesaj içeriği,
  consumer-group lag, şema, Kafka Connect worker durumu) için Kafka UI'ye
  (`https://kafka-ui.{{CORP_DOMAIN}}`) geçin.
- **Düzenle/pause/resume:** `PATCH /api/sources/{name}` (connector config
  patch), `POST /api/sources/{name}/pause` / `.../resume`.
- **Sil:** varsayılan mod **`pipeline_only`** (yalnızca `KafkaConnector`
  CR'ını siler, lakehouse'a zaten yazılmış veriye dokunmaz) — hem ADMIN hem
  ANALYST kullanabilir. **`with_data`** modu (KafkaTopic silme + Iceberg
  tablosunu `DROP` + kaynak bucket'ını boşaltma — geri alınamaz veri kaybı)
  **yalnızca ADMIN** rolüne açıktır ve frontend'de bu mod seçildiğinde
  onay butonu, operatör kaynağın **tam adını** yeniden yazana kadar devre
  dışı kalır (bkz. `console/frontend/src/components/DeleteModal.tsx`) —
  yanlışlıkla tıklamayla veri kaybını önleyen çift-onaylı bir tasarım.

### API özeti

| Endpoint (prefix) | Amaç | Gerekli rol (aksiyon) |
|---|---|---|
| `POST /api/sources` | yeni kaynak apply et | admin, analyst (`SOURCE_CREATE`) |
| `POST /api/sources/preview` | CR/DDL önizleme (hiçbir client çağrılmaz) | admin, analyst (`SOURCE_CREATE`) |
| `GET /api/sources[, /{name}]` | listele/detay | admin, analyst, student (`READ`) |
| `PATCH /api/sources/{name}` | config düzenle | admin, analyst (`SOURCE_EDIT`) |
| `POST /api/sources/{name}/pause`, `/resume` | duraklat/devam ettir | admin, analyst (`SOURCE_EDIT`) |
| `DELETE /api/sources/{name}?mode=pipeline_only` | pipeline sil (veri kalır) | admin, analyst (`SOURCE_DELETE_PIPELINE`) |
| `DELETE /api/sources/{name}?mode=with_data` | veriyle birlikte sil | **yalnızca admin** (`SOURCE_DELETE_WITH_DATA`) |
| `GET /api/tables`, `POST /api/tables`, `DELETE /api/tables/{fqn}` | Iceberg namespace/tablo listele/oluştur/sil | `READ` / `TABLE_CREATE` / **admin-only** `SOURCE_DELETE_WITH_DATA` |
| `GET /api/buckets`, `POST /api/buckets` | S3 bucket listele/oluştur | `READ` / `TABLE_CREATE` |
| `GET /api/schemas` | Apicurio şema listesi (salt-okunur) | `READ` |
| `GET /api/status` | connector sağlık panosu | `READ` |

Roller (`console/backend/app/services/authz.py` `_MATRIX`, AD/OIDC
`groups` claim'inden — `lakehouse-admins`/`lakehouse-analysts`/
`lakehouse-students` — birebir eşlenir): **admin** her aksiyona sahiptir;
**analyst** `SOURCE_DELETE_WITH_DATA` hariç her şeye sahiptir; **student**
yalnızca `READ`.

### Açık POC-DOĞRULA (kümede)

- `lakehouse-console` OIDC redirect URI'si (`https://console.{{CORP_DOMAIN}}/*`)
  + `groups` claim mapper gerçek Keycloak realm'inde teyit edilmeli.
- `kafbat-ui:v1.5.0` config şeması (`ssl.truststore-location`, ccompat path
  `/apis/ccompat/v7`, `SPRING_CONFIG_ADDITIONAL_LOCATION` env adı, RBAC
  `subjects[].type: role`) imajla teyit edilmeli — ayrıntılar
  `16-kafka-ui.yaml` dosya başı notunda.
- **Frontend'in OIDC/PKCE login akışı henüz kodda yok**:
  `console/frontend/src/App.tsx` içinde `CURRENT_USER_ROLE` şu an `"ADMIN"`
  olarak hardcode'lu; bu yalnızca frontend'in
  hangi UI elemanlarını (ör. "veriyle sil" seçeneği) gösterdiğini etkiler —
  **gerçek yetkilendirme her zaman backend'de** (`app/deps.py`
  `require_action`/`require_delete_mode`, OIDC bearer token doğrulaması ile)
  zorlanır, yani frontend'in rolü yanlış göstermesi bir güvenlik açığı
  değildir, ama gerçek küme kullanımına geçmeden önce frontend'in OIDC
  session'dan gerçek rolü okuyacak şekilde tamamlanması gerekir.

## PoC → Prod ayrımları

| Konu              | PoC (microk8s/MinIO)    | Üretim (OpenShift/S3)     |
|-------------------|-------------------------|---------------------------|
| Storage           | MinIO tek pod           | Kurumsal S3 nesne deposu  |
| Postgres          | Tek pod                 | CNPG HA (3 instance)      |
| Kafka             | Tek broker              | Strimzi 3 broker + SCRAM  |
| Nessie            | 1 replica               | 2 replica + HPA           |
| Spark             | Tek sandbox pod         | Spark Operator dynamic    |
| Trino             | Coordinator-only        | Coord + 3-5 worker        |
| Notebook          | Tek Jupyter pod         | Jupyter + Zeppelin + Hub  |
| AI / LLM          | qwen2.5-coder:3b CPU    | qwen2.5-coder:14b GPU     |
| Image bağımlılık  | Runtime pip install     | Kurumsal registry pin'li  |
| Secrets           | Plaintext env           | Vault / SealedSecrets     |
| Ingress           | NodePort                | Route + cert-manager + TLS|
| Kimlik doğrulama  | Yok                     | **AD (OIDC + LDAPS)**     |
| Monitoring        | Yok                     | Prometheus + Grafana      |
| Logging           | kubectl logs            | OpenShift Logging / Loki  |
| Backup            | Yok                     | Velero + S3 snapshot      |

## Üretim Kritik Ayarları ve Gerekçeleri

Aşağıdaki ayarlar üretim manifestlerinde uygulanmış durumdadır; her biri
için kısa gerekçe verilmiştir.

### Spark Dynamic Allocation — Shuffle Tracking (05-spark-operator.yaml)
Kubernetes üzerinde External Shuffle Service yoktur; dynamic allocation ile
executor kapatıldığında o executor'ın diskindeki shuffle verisi kaybolup
job'un crash olmasına yol açabilir. Bu yüzden `spark-defaults.conf` içine
`spark.dynamicAllocation.shuffleTracking.enabled=true` +
`.timeout=3600s` ayarlanmıştır; executor'lar shuffle verisi okunmayı
bekleyen süre boyunca canlı tutulur.

### Iceberg Bakım — Dinamik tarama (05-spark-operator.yaml)
Hardcoded tablo listesi ölçeklenemeyeceğinden `iceberg_maintenance.py`
PySpark döngüsü ile `SHOW NAMESPACES` + `SHOW TABLES` sonuçlarını dinamik
tarar; her tabloda `rewrite_data_files` + `expire_snapshots` +
`remove_orphan_files` çağırır. Dockerfile bölümünde tam kaynak kod.

### Trino — internal-communication shared-secret (06-trino-ha.yaml)
Coordinator↔worker arası trafiğin plaintext akmasını önlemek için
`internal-communication.shared-secret` zorunlu kılınmıştır; Secret'tan env
ile okunur. Bulk veri şifrelemesi için iki seçenek mevcuttur (yorum
satırlarında):
- Trino native internal TLS (keystore.p12 + cert-manager Certificate)
- OpenShift Service Mesh (Istio) PeerAuthentication STRICT — uygulama
  kodu değişmez, namespace-seviye mTLS.

### Trino — bellek tuning (06-trino-ha.yaml)
Worker Xmx 6G / query.max-memory-per-node 3GB olarak ayarlıdır; 8Gi pod
limiti içinde JVM overhead ve Iceberg memory-mapped dosyaları için ~2 GB
tampon bırakır (OOM riskini azaltır). Coordinator query.max-memory-per-node
1.5GB'dir — coordinator zaten yalnızca planlama yapar, büyük memory
gerekmez.

### Zeppelin — custom image, init-container yok (09-zeppelin.yaml)
Init-container kullanılmaz; Spark, `registry.apps.ocp.example.com/lakehouse/zeppelin:1.0`
custom imajı içinde build-time'da `/opt/spark` altında kuruludur — bu, her
pod restart'ında ~300 MB Spark'ın PVC'ye kopyalanmasını (gereksiz IO, yavaş
startup) önler. Dockerfile bölümünde yer alıyor.

### Postgres — archive_timeout notu (02-postgres-ha.yaml)
`archive_timeout` varsayılan (0) değerinde bırakılmıştır; düşük bir değer
(ör. 60s) S3'e sürekli küçük obje akışı yaratacağından bu tercih
edilmiştir; gerekçe manifestteki yorum satırlarında da belirtilir. `zstd`
WAL compression aktiftir.

## Referanslar

- Ana mimari raporu: proje mimari dokümanı (repo dışı)
- Pilot kurulum: pilot kurulum notları (repo dışı)
- AI katmanı detay: `../ai-demo/ai_demo.txt`
