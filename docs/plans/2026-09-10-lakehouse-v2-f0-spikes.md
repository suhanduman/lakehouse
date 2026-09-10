# Lakehouse v2 — F0 Spike'lar Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Lakehouse v2 tasarımının dört bilinmeyenini (S1 Polaris+STS'siz S3 · S2 Strimzi `spec.build` ile Iceberg sink · S3 `DebeziumTransform` + nested partition · S4 Spark 4.1 MoR MERGE + `spark.jars.packages`) yerel kind'da **çalışan kanıtla** cevaplamak ve F1/F2 planlarının dayanacağı bulgular dokümanını üretmek.

**Architecture:** Orphan `v2` dalında `test/spike/` altında idempotent shell script'leri + YAML manifestleri; her spike ayrı namespace; doğrulama `pyiceberg` (lokal venv) ile katalogdan okuma. Hiçbir özel imaj yok: Connect'i Strimzi kümede build eder (`ttl.sh`), Spark resmi imaj + Maven packages. Eski `main` dalına dokunulmaz.

**Tech Stack:** kind (Podman provider) · MinIO (dev S3) · CloudNativePG 1.30 · Apache Polaris 1.7.0 (Helm) · Strimzi 1.2.0 (Kafka 4.3.1) · Debezium 3.6.2 · Apache Iceberg 1.11.0 (kafka-connect + spark-runtime-4.1) · Kubeflow spark-operator 2.5.2 · `apache/spark:4.1.0-java21-python3` · `pyiceberg` + `apache-polaris` (pip)

**Spec:** `docs/specs/2026-09-10-lakehouse-v2-design.md` (§10 spike tablosu, §3 sürümler, §5–§6 hedef konfigürasyon)

## Global Constraints

- Özel/prebuilt imaj YOK; hack YOK (spec ilkeleri). Connect imajı yalnız `KafkaConnect.spec.build` ile üretilir.
- Sürümler (spec §3): Strimzi `1.2.0`, Kafka `4.3.1`, Debezium `3.6.2.Final`, Iceberg `1.11.0`, Polaris `1.7.0`, CNPG `1.30.0`, spark-operator `2.5.2`, Spark imajı `apache/spark:4.1.0-java21-python3`.
- Air-gap yok; internet (Maven Central, Docker Hub, quay, ttl.sh) kullanılabilir.
- Tüm script'ler `set -euo pipefail`, idempotent (tekrar çalıştırılabilir), `kubectl wait` ile doğrulanır; "gözle bak" adımı yok. Doğrulama bayrağı her script'te `--check`.
- Her spike sonunda **bulgu** `docs/plans/2026-09-10-f0-findings.md`'ye yazılır (ölçülen değer + karar); F1/F2 planları bu dosyadan beslenir.
- Commit'ler `v2` dalına; mesaj sonunda: `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`.
- Kaynak: Podman machine ≥ 6 CPU / 10 GB RAM önerilir (Kafka+Connect build+Polaris+Spark aynı anda). CI'da GitHub `ubuntu-latest` (4 vCPU/16 GB) yeterli.

---

## Dosya yapısı

```
test/spike/
  00-cluster.sh            kind (podman) + namespaces + MinIO + bucket
  10-cnpg.sh               CNPG operator + polaris-db + demo-pg Cluster'ları
  20-polaris.sh            Polaris helm + bootstrap Job + CLI setup + pyiceberg smoke (S1)
  20-polaris/values.yaml, bootstrap-job.yaml, setup.yaml, smoke.py
  30-connect.sh            Strimzi + Kafka + KafkaConnect(build) + KafkaConnector'lar + Bronze doğrulama (S2, S3)
  30-connect/kafka.yaml, connect.yaml, rbac.yaml, pg-source.yaml, iceberg-sink.yaml, iceberg-sink-partitioned.yaml, verify.py
  40-spark.sh              spark-operator + SparkApplication (MoR MERGE) + doğrulama (S4)
  40-spark/merge_spike.py, sparkapp.yaml
  90-teardown.sh
  requirements.txt         pyiceberg[s3fs,pyarrow] + apache-polaris
docs/plans/2026-09-10-f0-findings.md
```

---

### Task 1: Orphan `v2` dalı ve spike iskeleti

**Files:**
- Create: `.gitignore`, `README.md`, `test/spike/requirements.txt`, `test/spike/00-cluster.sh`, `docs/plans/2026-09-10-f0-findings.md`
- Keep (çalışma ağacından): `docs/specs/2026-09-10-lakehouse-v2-design.md`, `docs/reviews/2026-09-10-architecture-reassessment/*.md`, `docs/plans/2026-09-10-lakehouse-v2-f0-spikes.md`

**Interfaces:**
- Produces: `v2` dalı; `test/spike/00-cluster.sh` → kind cluster `lh-spike`, namespaces `minio polaris lakehouse spark`, MinIO servisi `http://minio.minio.svc:9000` (root: `minioadmin`/`minioadmin`), bucket `lakehouse`; Secret `minio-creds` (keys `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`) `polaris`, `lakehouse`, `spark` ns'lerinde.

- [ ] **Step 1: Orphan dal + eski dosyaları temizle**

```bash
cd /Users/suhanduman/Desktop/KÇ
git switch --orphan v2
git rm -rf --cached . >/dev/null 2>&1 || true
# çalışma ağacında eski dosyalar duruyor; yalnız taşınacakları bırak
find . -mindepth 1 -maxdepth 1 ! -name .git ! -name docs -exec rm -rf {} +
find docs -mindepth 1 -maxdepth 1 ! -name specs ! -name reviews ! -name plans -exec rm -rf {} +
ls docs   # beklenen: plans reviews specs
```

- [ ] **Step 2: `.gitignore`, `README.md`, `requirements.txt` yaz**

```bash
cat > .gitignore <<'EOF'
.venv/
__pycache__/
*.pyc
.DS_Store
test/spike/.state/
EOF
cat > README.md <<'EOF'
# lakehouse (v2)

Deklaratif, minimal-kod açık kaynak data lakehouse: Debezium (Strimzi Kafka Connect) → Apache Iceberg (Polaris REST katalog) → Spark MERGE → Trino/Superset/notebook.

- Tasarım: `docs/specs/2026-09-10-lakehouse-v2-design.md`
- Neden v2: `docs/reviews/2026-09-10-architecture-reassessment/00-KARAR.md`
- Şu an: F0 spike'ları (`test/spike/`, plan `docs/plans/2026-09-10-lakehouse-v2-f0-spikes.md`)
EOF
mkdir -p test/spike
cat > test/spike/requirements.txt <<'EOF'
pyiceberg[s3fs,pyarrow]>=0.10,<0.11
apache-polaris
EOF
```

- [ ] **Step 3: Doğrulama script'ini önce yaz (başarısız olmalı — küme yok)**

```bash
cat > test/spike/00-cluster.sh <<'EOF'
#!/usr/bin/env bash
# kind (podman) cluster + namespaces + MinIO. Idempotent.
set -euo pipefail
cd "$(dirname "$0")"
export KIND_EXPERIMENTAL_PROVIDER="${KIND_EXPERIMENTAL_PROVIDER:-podman}"
CLUSTER=lh-spike
NODE_IMAGE="${KIND_NODE_IMAGE:-kindest/node:v1.34.0}"   # Polaris chart k8s >=1.33 ister

if [[ "${1:-}" == "--check" ]]; then
  kubectl get ns minio polaris lakehouse spark >/dev/null
  kubectl -n minio rollout status deploy/minio --timeout=60s >/dev/null
  kubectl -n minio wait --for=condition=complete job/minio-bucket-init --timeout=120s >/dev/null
  for ns in polaris lakehouse spark; do kubectl -n "$ns" get secret minio-creds >/dev/null; done
  echo "OK: cluster+minio hazır"; exit 0
fi

if ! kind get clusters | grep -qx "$CLUSTER"; then
  kind create cluster --name "$CLUSTER" --image "$NODE_IMAGE" --wait 120s
fi
kubectl config use-context "kind-$CLUSTER" >/dev/null
for ns in minio polaris lakehouse spark; do kubectl create ns "$ns" --dry-run=client -o yaml | kubectl apply -f -; done

kubectl -n minio apply -f - <<'YAML'
apiVersion: apps/v1
kind: Deployment
metadata: {name: minio}
spec:
  replicas: 1
  selector: {matchLabels: {app: minio}}
  template:
    metadata: {labels: {app: minio}}
    spec:
      containers:
      - name: minio
        image: quay.io/minio/minio:latest
        args: ["server", "/data", "--console-address", ":9001"]
        env:
        - {name: MINIO_ROOT_USER, value: minioadmin}
        - {name: MINIO_ROOT_PASSWORD, value: minioadmin}
        ports: [{containerPort: 9000}, {containerPort: 9001}]
        volumeMounts: [{name: data, mountPath: /data}]
      volumes: [{name: data, emptyDir: {}}]
---
apiVersion: v1
kind: Service
metadata: {name: minio}
spec:
  selector: {app: minio}
  ports: [{name: api, port: 9000}, {name: console, port: 9001}]
---
apiVersion: batch/v1
kind: Job
metadata: {name: minio-bucket-init}
spec:
  backoffLimit: 10
  template:
    spec:
      restartPolicy: OnFailure
      containers:
      - name: mc
        image: quay.io/minio/mc:latest
        command: ["/bin/sh","-c"]
        args:
        - |
          until mc alias set m http://minio.minio.svc:9000 minioadmin minioadmin; do sleep 3; done
          mc mb -p m/lakehouse && mc ls m
YAML

for ns in polaris lakehouse spark; do
  kubectl -n "$ns" create secret generic minio-creds \
    --from-literal=AWS_ACCESS_KEY_ID=minioadmin --from-literal=AWS_SECRET_ACCESS_KEY=minioadmin \
    --dry-run=client -o yaml | kubectl apply -f -
done
kubectl -n minio rollout status deploy/minio --timeout=180s
kubectl -n minio wait --for=condition=complete job/minio-bucket-init --timeout=300s
"$0" --check
EOF
chmod +x test/spike/00-cluster.sh
test/spike/00-cluster.sh --check   # beklenen: HATA (context/namespace yok)
```

- [ ] **Step 4: Cluster'ı kur, doğrulama geçsin**

```bash
test/spike/00-cluster.sh
# beklenen son satır: OK: cluster+minio hazır
```
`kindest/node:v1.34.0` yoksa: `KIND_NODE_IMAGE=kindest/node:v1.33.4 test/spike/00-cluster.sh` (kind sürümünün desteklediği en yeni 1.33+/1.34+ imajı; kind release notlarındaki digest'li imajı kullan).

- [ ] **Step 5: Bulgular dosyası iskeleti**

```bash
cat > docs/plans/2026-09-10-f0-findings.md <<'EOF'
# F0 Spike Bulguları (2026-09-xx)

Her satır: ölçülen gerçek (komut çıktısı) → karar. "Çalıştı sanıyorum" yazılmaz; çıktı yapıştırılır.

| Spike | Soru | Sonuç (kanıt) | F1/F2 kararı |
|---|---|---|---|
| S1a | Polaris 1.7 + relational-jdbc (CNPG) + admin-tool bootstrap ayağa kalkıyor mu? | | |
| S1b | MinIO'ya vended-credentials ile pyiceberg yazma/okuma? | | |
| S1c | `stsUnavailable=true` + istemci statik anahtarlarıyla yazma/okuma? | | Polaris mi Lakekeeper mi |
| S1d | `polaris setup apply setup.yaml` katalog+principal+grant'i tek YAML'dan kuruyor mu? | | runbook biçimi |
| S2a | `spec.build` (Debezium zip + 4 Iceberg maven artefaktı) build süresi / eksik sınıf? | | ek artefakt listesi |
| S2b | `KubernetesSecretConfigProvider` ile `${secrets:...}` çözümleniyor mu? | | |
| S2c | Sink `auto-create` namespace'i de yaratıyor mu, yoksa önceden mi? | | runbook adımı |
| S2d | `DebeziumTransform`: `_cdc.op` değerleri (I/U/D?), `_cdc.ts` tipi, DELETE satırı `before`'dan mı? | | merge SQL |
| S3 | `default-partition-by=day(_cdc.ts)` (nested) kabul edildi mi? | | Bronze partition |
| S4a | spark-operator 2.5.2 + `apache/spark:4.1.0-java21-python3` + `spark.jars.packages` (Iceberg 1.11) çözümleniyor mu, süre? | | packages vs init-container |
| S4b | MoR MERGE (`WHEN MATCHED AND op='D' THEN DELETE`) + `rewrite_position_delete_files` + time travel çalıştı mı? ANSI mode CAST etkisi? | | |
EOF
```

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "chore(v2): orphan branch, spike skeleton, kind+minio bootstrap script

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 2: CNPG operatörü + `polaris-db` ve `demo-pg` Postgres kümeleri

**Files:**
- Create: `test/spike/10-cnpg.sh`

**Interfaces:**
- Produces: Secret `polaris/polaris-db-app` (CNPG standart anahtarları: `username`, `password`, `jdbc-uri`); Postgres `demo-pg-rw.lakehouse.svc:5432` db `shop`, kullanıcı `dbz`/`dbz` (replication, `public.orders` sahibi), tablo `public.orders(id, status, amount numeric(10,2), updated_at timestamptz)` 3 satır; Secret `lakehouse/dbz-pg-cred` (`username`, `password`).

- [ ] **Step 1: Doğrulama script'i önce (başarısız)**

```bash
cat > test/spike/10-cnpg.sh <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
CNPG_VERSION=1.30.0
if [[ "${1:-}" == "--check" ]]; then
  kubectl -n polaris wait --for=condition=Ready cluster/polaris-db --timeout=10s >/dev/null
  kubectl -n lakehouse wait --for=condition=Ready cluster/demo-pg --timeout=10s >/dev/null
  kubectl -n polaris get secret polaris-db-app -o jsonpath='{.data.jdbc-uri}' | base64 -d | grep -q '^jdbc:postgresql://'
  n=$(kubectl -n lakehouse exec demo-pg-1 -c postgres -- psql -U postgres -d shop -Atc "select count(*) from public.orders")
  [[ "$n" == "3" ]] || { echo "orders satır sayısı $n != 3"; exit 1; }
  kubectl -n lakehouse exec demo-pg-1 -c postgres -- psql -U postgres -d shop -Atc "show wal_level" | grep -qx logical
  echo "OK: cnpg + polaris-db + demo-pg hazır"; exit 0
fi

kubectl apply --server-side -f "https://raw.githubusercontent.com/cloudnative-pg/cloudnative-pg/release-1.30/releases/cnpg-${CNPG_VERSION}.yaml"
kubectl -n cnpg-system rollout status deploy/cnpg-controller-manager --timeout=180s

kubectl -n polaris apply -f - <<'YAML'
apiVersion: postgresql.cnpg.io/v1
kind: Cluster
metadata: {name: polaris-db}
spec:
  instances: 1
  storage: {size: 2Gi}
  bootstrap:
    initdb: {database: polaris, owner: polaris}
YAML

kubectl -n lakehouse apply -f - <<'YAML'
apiVersion: v1
kind: Secret
metadata: {name: dbz-pg-cred}
type: kubernetes.io/basic-auth
stringData: {username: dbz, password: dbz}
---
apiVersion: postgresql.cnpg.io/v1
kind: Cluster
metadata: {name: demo-pg}
spec:
  instances: 1
  storage: {size: 2Gi}
  postgresql:
    parameters:
      wal_level: logical
      max_wal_senders: "10"
      max_replication_slots: "10"
  managed:
    roles:
    - name: dbz
      ensure: present
      login: true
      replication: true
      passwordSecret: {name: dbz-pg-cred}
  bootstrap:
    initdb:
      database: shop
      owner: app
      postInitApplicationSQL:
      - CREATE TABLE public.orders (id bigserial PRIMARY KEY, status text NOT NULL, amount numeric(10,2) NOT NULL, updated_at timestamptz NOT NULL DEFAULT now());
      - INSERT INTO public.orders (status, amount) VALUES ('new', 10.50), ('paid', 99.99), ('new', 3.00);
      - ALTER TABLE public.orders OWNER TO dbz;
      - GRANT CREATE ON DATABASE shop TO dbz;
      - GRANT USAGE, CREATE ON SCHEMA public TO dbz;
YAML

kubectl -n polaris wait --for=condition=Ready cluster/polaris-db --timeout=300s
kubectl -n lakehouse wait --for=condition=Ready cluster/demo-pg --timeout=300s
"$0" --check
EOF
chmod +x test/spike/10-cnpg.sh
test/spike/10-cnpg.sh --check   # beklenen: HATA (cluster CRD yok)
```
Not: `postInitApplicationSQL` `app` sahipliğinde çalışır; `ALTER TABLE ... OWNER TO dbz` için rol önce var olmalı — CNPG `managed.roles`'ı initdb'den önce uygular; hata alınırsa bu iki satırı `--check`'ten önce `psql -U postgres` ile uygula ve bulguya yaz (Debezium `publication.autocreate.mode=filtered` tablo sahipliği ister).

- [ ] **Step 2: Kur ve doğrula**

```bash
test/spike/10-cnpg.sh
# beklenen: OK: cnpg + polaris-db + demo-pg hazır
```

- [ ] **Step 3: Commit**

```bash
git add test/spike/10-cnpg.sh
git commit -m "spike(f0): CNPG operator + polaris-db + demo-pg (logical wal, dbz role, orders seed)

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 3: S1 — Polaris 1.7 (relational-jdbc) + MinIO katalog + pyiceberg smoke

**Files:**
- Create: `test/spike/20-polaris.sh`, `test/spike/20-polaris/values.yaml`, `test/spike/20-polaris/bootstrap-job.yaml`, `test/spike/20-polaris/setup.yaml`, `test/spike/20-polaris/smoke.py`

**Interfaces:**
- Consumes: `polaris/polaris-db-app`, `polaris/minio-creds` (Task 1–2)
- Produces: Polaris `http://polaris.polaris.svc:8181` (küme içi) / `localhost:8181` (port-forward); root `root`/`s3cr3t` (realm `POLARIS`); katalog `lakehouse` (`s3://lakehouse/`, MinIO endpoint, path-style); principal'lar `connect`, `spark`, `trino` — client-id/secret'ları `test/spike/.state/polaris-<principal>.env` dosyalarında (`CLIENT_ID=…`/`CLIENT_SECRET=…` satırları) **ve** Secret `lakehouse/polaris-connect` (key `credential` = `<id>:<secret>`), `spark/polaris-spark` (aynı).

- [ ] **Step 1: Helm values + bootstrap Job + setup YAML**

```bash
mkdir -p test/spike/20-polaris test/spike/.state
cat > test/spike/20-polaris/values.yaml <<'EOF'
image:
  tag: "1.7.0"
persistence:
  type: relational-jdbc
  relationalJdbc:
    secret:
      name: polaris-db-app
      username: username
      password: password
      jdbcUrl: jdbc-uri
storage:
  secret:
    name: minio-creds
    awsAccessKeyId: AWS_ACCESS_KEY_ID
    awsSecretAccessKey: AWS_SECRET_ACCESS_KEY
extraEnv:
  - {name: AWS_REGION, value: us-east-1}
realmContext:
  realms: [POLARIS]
EOF

cat > test/spike/20-polaris/bootstrap-job.yaml <<'EOF'
# Realm + root credential bootstrap (chart bunu yapmaz; admin-tool imajı yapar).
apiVersion: batch/v1
kind: Job
metadata: {name: polaris-bootstrap, namespace: polaris}
spec:
  backoffLimit: 3
  template:
    spec:
      restartPolicy: Never
      containers:
      - name: bootstrap
        image: apache/polaris-admin-tool:1.7.0
        args: ["bootstrap", "--realm=POLARIS", "--credential=POLARIS,root,s3cr3t"]
        env:
        - {name: POLARIS_PERSISTENCE_TYPE, value: relational-jdbc}
        - {name: QUARKUS_DATASOURCE_JDBC_URL, valueFrom: {secretKeyRef: {name: polaris-db-app, key: jdbc-uri}}}
        - {name: QUARKUS_DATASOURCE_USERNAME, valueFrom: {secretKeyRef: {name: polaris-db-app, key: username}}}
        - {name: QUARKUS_DATASOURCE_PASSWORD, valueFrom: {secretKeyRef: {name: polaris-db-app, key: password}}}
EOF

# Deklaratif katalog/principal/grant tanımı — `polaris setup apply` (CLI) ile uygulanır.
# Şema CLI'nin setup-config biçimidir; ilk çalıştırmada `polaris setup --help` ile alan adlarını
# doğrula ve gerekiyorsa burada düzelt (S1d bulgusu).
cat > test/spike/20-polaris/setup.yaml <<'EOF'
catalogs:
  - name: lakehouse
    type: INTERNAL
    default-base-location: s3://lakehouse/
    storage:
      type: S3
      endpoint: http://minio.minio.svc:9000
      endpoint-internal: http://minio.minio.svc:9000
      path-style-access: true
      region: us-east-1
      allowed-locations: ["s3://lakehouse/"]
principals:
  - name: connect
  - name: spark
  - name: trino
principal-roles:
  - name: writers
    principals: [connect, spark]
  - name: readers
    principals: [trino]
catalog-roles:
  - name: lakehouse_admin
    catalog: lakehouse
    principal-roles: [writers]
    privileges: [CATALOG_MANAGE_CONTENT]
  - name: lakehouse_read
    catalog: lakehouse
    principal-roles: [readers]
    privileges: [CATALOG_READ_PROPERTIES, TABLE_READ_DATA, NAMESPACE_LIST, TABLE_LIST, VIEW_LIST]
EOF
```

- [ ] **Step 2: pyiceberg smoke testi (önce yaz; Polaris yokken başarısız)**

```bash
cat > test/spike/20-polaris/smoke.py <<'EOF'
"""S1b/S1c: Polaris'e namespace+tablo yarat, 2 satır yaz, geri oku.
Kullanım: python smoke.py <client_id> <client_secret> [--static-keys]
--static-keys: vended-credentials yerine istemcinin kendi MinIO anahtarları (stsUnavailable senaryosu)."""
import sys, pyarrow as pa
from pyiceberg.catalog import load_catalog

cid, csec = sys.argv[1], sys.argv[2]
static = "--static-keys" in sys.argv
props = {
    "type": "rest",
    "uri": "http://localhost:8181/api/catalog",
    "warehouse": "lakehouse",
    "credential": f"{cid}:{csec}",
    "scope": "PRINCIPAL_ROLE:ALL",
    "s3.endpoint": "http://localhost:9000",
    "s3.path-style-access": "true",
    "s3.region": "us-east-1",
}
if static:
    props.update({"s3.access-key-id": "minioadmin", "s3.secret-access-key": "minioadmin"})
else:
    props["header.X-Iceberg-Access-Delegation"] = "vended-credentials"
cat = load_catalog("lakehouse", **props)
ns = "smoke_static" if static else "smoke_vended"
cat.create_namespace_if_not_exists(ns)
schema = pa.schema([pa.field("id", pa.int64(), nullable=False), pa.field("name", pa.string())])
ident = f"{ns}.t"
if cat.table_exists(ident):
    cat.drop_table(ident)
t = cat.create_table(ident, schema=schema)
t.append(pa.Table.from_pylist([{"id": 1, "name": "a"}, {"id": 2, "name": "b"}], schema=schema))
rows = t.scan().to_arrow().num_rows
assert rows == 2, rows
print(f"OK: {ident} yazıldı/okundu, {rows} satır, mode={'static' if static else 'vended'}")
EOF
python3 -m venv .venv && .venv/bin/pip install -q -r test/spike/requirements.txt
.venv/bin/python test/spike/20-polaris/smoke.py x y   # beklenen: HATA (bağlantı yok)
```

- [ ] **Step 3: Kurulum script'i**

```bash
cat > test/spike/20-polaris.sh <<'EOF'
#!/usr/bin/env bash
# S1: Polaris 1.7 + CNPG + MinIO. Idempotent. Port-forward'lar arka planda açılır (.state/pf-*.pid).
set -euo pipefail
cd "$(dirname "$0")"
STATE=.state; mkdir -p "$STATE"
PY=../../.venv/bin/python; POLARIS=../../.venv/bin/polaris

pf() { # pf <ns> <svc> <local:remote> <pidfile>
  if [[ -f "$STATE/$4" ]] && kill -0 "$(cat "$STATE/$4")" 2>/dev/null; then return; fi
  kubectl -n "$1" port-forward "svc/$2" "$3" >/dev/null 2>&1 & echo $! > "$STATE/$4"; sleep 2
}

if [[ "${1:-}" == "--check" ]]; then
  curl -sf http://localhost:8181/q/health/ready >/dev/null
  set -a; source "$STATE/polaris-connect.env"; set +a
  $PY 20-polaris/smoke.py "$CLIENT_ID" "$CLIENT_SECRET"
  $PY 20-polaris/smoke.py "$CLIENT_ID" "$CLIENT_SECRET" --static-keys
  kubectl -n lakehouse get secret polaris-connect >/dev/null && kubectl -n spark get secret polaris-spark >/dev/null
  echo "OK: S1 Polaris hazır"; exit 0
fi

helm repo add polaris https://downloads.apache.org/polaris/helm-chart >/dev/null 2>&1 || true
helm repo update polaris >/dev/null
kubectl -n polaris apply -f 20-polaris/bootstrap-job.yaml
kubectl -n polaris wait --for=condition=complete job/polaris-bootstrap --timeout=300s
helm upgrade --install polaris polaris/polaris --version 1.7.0 -n polaris -f 20-polaris/values.yaml --wait --timeout 10m
pf polaris polaris 8181:8181 pf-polaris.pid
pf minio minio 9000:9000 pf-minio.pid
until curl -sf http://localhost:8181/q/health/ready >/dev/null; do sleep 3; done

export CLIENT_ID=root CLIENT_SECRET=s3cr3t
$POLARIS setup apply 20-polaris/setup.yaml 2>&1 | tee "$STATE/setup-apply.log"
# setup apply principal credential'larını döndürmezse principals create/rotate çıktısını yakala:
for p in connect spark trino; do
  if [[ ! -f "$STATE/polaris-$p.env" ]]; then
    out=$($POLARIS principals create "$p" 2>/dev/null || $POLARIS principals rotate-credentials "$p")
    id=$(echo "$out" | $PY -c 'import sys,json;print(json.load(sys.stdin)["clientId"])')
    sec=$(echo "$out" | $PY -c 'import sys,json;print(json.load(sys.stdin)["clientSecret"])')
    printf 'CLIENT_ID=%s\nCLIENT_SECRET=%s\n' "$id" "$sec" > "$STATE/polaris-$p.env"
  fi
done
set -a; source "$STATE/polaris-connect.env"; set +a
kubectl -n lakehouse create secret generic polaris-connect --from-literal=credential="$CLIENT_ID:$CLIENT_SECRET" --dry-run=client -o yaml | kubectl apply -f -
set -a; source "$STATE/polaris-spark.env"; set +a
kubectl -n spark create secret generic polaris-spark --from-literal=credential="$CLIENT_ID:$CLIENT_SECRET" --dry-run=client -o yaml | kubectl apply -f -
"$0" --check
EOF
chmod +x test/spike/20-polaris.sh
```

- [ ] **Step 4: Kur; S1a/S1b/S1d gözlemlerini bulguya yaz**

```bash
test/spike/20-polaris.sh 2>&1 | tee test/spike/.state/20.log
```
Kontrol noktaları ve olası düzeltmeler (bulgu tablosuna kanıtla yaz):
- Bootstrap Job `--help` isterse: `kubectl -n polaris logs job/polaris-bootstrap`; argüman adları 1.7 admin-tool'unkilerle eşleşmiyorsa düzelt (`bootstrap --realm --credential` beklenen).
- `polaris setup apply` şemayı reddederse: `polaris setup --help`; şema farklıysa `setup.yaml`'ı düzelt (bu S1d'nin cevabı). Hiç uygun değilse geçici olarak CLI komutlarına düş (`catalogs create --storage-type s3 --endpoint http://minio.minio.svc:9000 --path-style-access --default-base-location s3://lakehouse/ --region us-east-1 lakehouse` + `principals/principal-roles/catalog-roles create/grant` + `privileges catalog grant ... CATALOG_MANAGE_CONTENT`) ve bulguya "setup apply yok/şeması X" yaz.
- smoke vended modu MinIO'da başarısızsa (MinIO STS AssumeRole varsayılan açık olmalı): hata metnini bulguya yaz; `--static-keys` sonucu asıl S1c cevabıdır.

- [ ] **Step 5: S1c — `stsUnavailable=true` varyantı**

```bash
set -a; source test/spike/.state/polaris-connect.env; set +a
CLIENT_ID=root CLIENT_SECRET=s3cr3t .venv/bin/polaris catalogs update lakehouse --set-property sts-unavailable=true 2>&1 || \
CLIENT_ID=root CLIENT_SECRET=s3cr3t .venv/bin/polaris catalogs update lakehouse --sts-unavailable 2>&1
# hangi bayrak kabul edildiyse onu bulguya yaz; sonra:
.venv/bin/python test/spike/20-polaris/smoke.py "$CLIENT_ID" "$CLIENT_SECRET" --static-keys   # beklenen: OK
.venv/bin/python test/spike/20-polaris/smoke.py "$CLIENT_ID" "$CLIENT_SECRET"                 # vended: davranışı kaydet
```
Karar kuralı (bulguya yaz): `--static-keys` + `stsUnavailable=true` çalışıyorsa **Polaris kalır** (müşteri S3'ünde STS yoksa da çalışır). Çalışmıyorsa Lakekeeper spike'ı bu görevin altına eklenir (Task 3b).

- [ ] **Step 6: Commit**

```bash
git add test/spike/20-polaris.sh test/spike/20-polaris docs/plans/2026-09-10-f0-findings.md
git commit -m "spike(f0/S1): Polaris 1.7 relational-jdbc + MinIO catalog + declarative setup + pyiceberg smoke

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 4: S2 — Strimzi 1.2 + KafkaConnect `spec.build` + Debezium→Iceberg Bronze

**Files:**
- Create: `test/spike/30-connect.sh`, `test/spike/30-connect/kafka.yaml`, `test/spike/30-connect/rbac.yaml`, `test/spike/30-connect/connect.yaml`, `test/spike/30-connect/pg-source.yaml`, `test/spike/30-connect/iceberg-sink.yaml`, `test/spike/30-connect/verify.py`

**Interfaces:**
- Consumes: `demo-pg` + `lakehouse/dbz-pg-cred` (Task 2), `lakehouse/polaris-connect` Secret (Task 3), `lakehouse/minio-creds`
- Produces: Kafka `lakehouse-kafka-bootstrap.lakehouse.svc:9092` (plain — spike; prod'da tls/scram); KafkaConnect `connect` (SA `connect-connect`); topic'ler `shop.public.orders`; Bronze tablo `lakehouse.shop_raw.orders` (`_cdc` struct'lı); `verify.py <namespace> <table> <min_rows>` → satır sayısı + `_cdc.op` dağılımı yazdırır.

- [ ] **Step 1: Manifestler**

```bash
mkdir -p test/spike/30-connect
cat > test/spike/30-connect/kafka.yaml <<'EOF'
apiVersion: kafka.strimzi.io/v1
kind: KafkaNodePool
metadata:
  name: dual-role
  namespace: lakehouse
  labels: {strimzi.io/cluster: lakehouse}
spec:
  replicas: 1
  roles: [controller, broker]
  storage:
    type: jbod
    volumes: [{id: 0, type: persistent-claim, size: 5Gi, kraftMetadata: shared}]
---
apiVersion: kafka.strimzi.io/v1
kind: Kafka
metadata:
  name: lakehouse
  namespace: lakehouse
  annotations: {strimzi.io/node-pools: enabled, strimzi.io/kraft: enabled}
spec:
  kafka:
    version: 4.3.1
    metadataVersion: 4.3-IV0
    listeners:
    - {name: plain, port: 9092, type: internal, tls: false}
    config:
      offsets.topic.replication.factor: 1
      transaction.state.log.replication.factor: 1
      transaction.state.log.min.isr: 1
      default.replication.factor: 1
      min.insync.replicas: 1
  entityOperator: {topicOperator: {}, userOperator: {}}
EOF

cat > test/spike/30-connect/rbac.yaml <<'EOF'
# KubernetesSecretConfigProvider: Connect SA'nın Secret okuma izni (mount yerine).
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata: {name: connect-secrets-reader, namespace: lakehouse}
rules:
- apiGroups: [""]
  resources: [secrets]
  verbs: [get, list, watch]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata: {name: connect-secrets-reader, namespace: lakehouse}
subjects: [{kind: ServiceAccount, name: connect-connect, namespace: lakehouse}]
roleRef: {kind: Role, name: connect-secrets-reader, apiGroup: rbac.authorization.k8s.io}
EOF

cat > test/spike/30-connect/connect.yaml <<'EOF'
apiVersion: kafka.strimzi.io/v1
kind: KafkaConnect
metadata:
  name: connect
  namespace: lakehouse
  annotations: {strimzi.io/use-connector-resources: "true"}
spec:
  version: 4.3.1
  replicas: 1
  bootstrapServers: lakehouse-kafka-bootstrap:9092
  config:
    group.id: connect
    offset.storage.topic: connect-offsets
    config.storage.topic: connect-configs
    status.storage.topic: connect-status
    config.storage.replication.factor: -1
    offset.storage.replication.factor: -1
    status.storage.replication.factor: -1
    config.providers: secrets
    config.providers.secrets.class: io.strimzi.kafka.KubernetesSecretConfigProvider
  build:
    output:
      type: docker
      image: ttl.sh/lakehouse-connect-spike-CHANGEME:24h   # 30-connect.sh benzersiz UUID ile değiştirir
    plugins:
    - name: debezium-postgres
      artifacts:
      - type: zip
        url: https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/3.6.2.Final/debezium-connector-postgres-3.6.2.Final-plugin.zip
    - name: iceberg
      artifacts:
      - {type: maven, group: org.apache.iceberg, artifact: iceberg-kafka-connect, version: 1.11.0}
      - {type: maven, group: org.apache.iceberg, artifact: iceberg-kafka-connect-transforms, version: 1.11.0}
      - {type: maven, group: org.apache.iceberg, artifact: iceberg-parquet, version: 1.11.0}
      - {type: maven, group: org.apache.iceberg, artifact: iceberg-aws-bundle, version: 1.11.0}
  resources:
    requests: {cpu: 500m, memory: 1536Mi}
    limits: {memory: 2Gi}
EOF

cat > test/spike/30-connect/pg-source.yaml <<'EOF'
apiVersion: kafka.strimzi.io/v1
kind: KafkaConnector
metadata:
  name: dbz-shop
  namespace: lakehouse
  labels: {strimzi.io/cluster: connect}
spec:
  class: io.debezium.connector.postgresql.PostgresConnector
  tasksMax: 1
  config:
    database.hostname: demo-pg-rw.lakehouse.svc
    database.port: "5432"
    database.user: ${secrets:lakehouse/dbz-pg-cred:username}
    database.password: ${secrets:lakehouse/dbz-pg-cred:password}
    database.dbname: shop
    topic.prefix: shop
    table.include.list: public.orders
    plugin.name: pgoutput
    slot.name: debezium_shop
    publication.name: dbz_shop_pub
    publication.autocreate.mode: filtered
    snapshot.mode: initial
    time.precision.mode: connect
    decimal.handling.mode: precise
    key.converter: org.apache.kafka.connect.json.JsonConverter
    value.converter: org.apache.kafka.connect.json.JsonConverter
    key.converter.schemas.enable: "true"
    value.converter.schemas.enable: "true"
    topic.creation.default.replication.factor: "1"
    topic.creation.default.partitions: "3"
    tombstones.on.delete: "false"
EOF

cat > test/spike/30-connect/iceberg-sink.yaml <<'EOF'
apiVersion: kafka.strimzi.io/v1
kind: KafkaConnector
metadata:
  name: iceberg-shop
  namespace: lakehouse
  labels: {strimzi.io/cluster: connect}
spec:
  class: org.apache.iceberg.connect.IcebergSinkConnector
  tasksMax: 1
  config:
    topics.regex: shop\.public\..*
    key.converter: org.apache.kafka.connect.json.JsonConverter
    value.converter: org.apache.kafka.connect.json.JsonConverter
    key.converter.schemas.enable: "true"
    value.converter.schemas.enable: "true"
    transforms: dbz
    transforms.dbz.type: org.apache.iceberg.connect.transforms.DebeziumTransform
    transforms.dbz.cdc.target.pattern: shop_raw.{table}
    iceberg.tables.dynamic-enabled: "true"
    iceberg.tables.route-field: _cdc.target
    iceberg.tables.auto-create-enabled: "true"
    iceberg.tables.evolve-schema-enabled: "true"
    iceberg.tables.auto-create-props.write.metadata.delete-after-commit.enabled: "true"
    iceberg.catalog.type: rest
    iceberg.catalog.uri: http://polaris.polaris.svc:8181/api/catalog
    iceberg.catalog.warehouse: lakehouse
    iceberg.catalog.credential: ${secrets:lakehouse/polaris-connect:credential}
    iceberg.catalog.scope: PRINCIPAL_ROLE:ALL
    iceberg.catalog.io-impl: org.apache.iceberg.aws.s3.S3FileIO
    iceberg.catalog.s3.endpoint: http://minio.minio.svc:9000
    iceberg.catalog.s3.path-style-access: "true"
    iceberg.catalog.client.region: us-east-1
    iceberg.catalog.s3.access-key-id: ${secrets:lakehouse/minio-creds:AWS_ACCESS_KEY_ID}
    iceberg.catalog.s3.secret-access-key: ${secrets:lakehouse/minio-creds:AWS_SECRET_ACCESS_KEY}
    iceberg.control.commit.interval-ms: "30000"   # SADECE spike'ta kısa (varsayılan 300000); prod'da set edilmez
    errors.tolerance: all
    errors.log.enable: "true"
    errors.deadletterqueue.topic.name: shop.dlq
    errors.deadletterqueue.topic.replication.factor: "1"
EOF
```
Not: statik S3 anahtarları S1c sonucuna göre; S1b vended çalışıyorsa `s3.access-key-id/secret` satırlarını kaldırıp `iceberg.catalog.header.X-Iceberg-Access-Delegation=vended-credentials` ekleyerek ikinci kez dene ve ikisini de bulguya yaz.

- [ ] **Step 2: Doğrulama betiği (Bronze okuma) — önce yaz**

```bash
cat > test/spike/30-connect/verify.py <<'EOF'
"""verify.py <namespace> <table> <min_rows> — Bronze tabloyu Polaris'ten okur; satır sayısı ve _cdc.op dağılımı.
.state/polaris-connect.env dosyasından (CLIENT_ID=/CLIENT_SECRET= satırları) kimlik alır."""
import sys, collections
from pyiceberg.catalog import load_catalog
ns, tbl, min_rows = sys.argv[1], sys.argv[2], int(sys.argv[3])
env = dict(l.split("=", 1) for l in open("../.state/polaris-connect.env").read().strip().splitlines())
cat = load_catalog("lakehouse", type="rest", uri="http://localhost:8181/api/catalog", warehouse="lakehouse",
                   credential=f"{env['CLIENT_ID']}:{env['CLIENT_SECRET']}", scope="PRINCIPAL_ROLE:ALL",
                   **{"s3.endpoint": "http://localhost:9000", "s3.path-style-access": "true", "s3.region": "us-east-1",
                      "s3.access-key-id": "minioadmin", "s3.secret-access-key": "minioadmin"})
t = cat.load_table(f"{ns}.{tbl}")
print("schema:", t.schema())
print("partition spec:", t.spec())
rows = t.scan().to_arrow().to_pylist()
ops = collections.Counter((r.get("_cdc") or {}).get("op") for r in rows)
print(f"rows={len(rows)} ops={dict(ops)}")
print("örnek:", rows[0] if rows else None)
assert len(rows) >= min_rows, f"{len(rows)} < {min_rows}"
print("OK")
EOF
```

- [ ] **Step 3: Kurulum/doğrulama script'i**

```bash
cat > test/spike/30-connect.sh <<'EOF'
#!/usr/bin/env bash
# S2/S3: Strimzi 1.2 + Kafka + KafkaConnect(build) + Debezium pg -> Iceberg sink -> Bronze (Polaris/MinIO).
set -euo pipefail
cd "$(dirname "$0")"
PY=../../.venv/bin/python; STATE=.state; mkdir -p "$STATE"

if [[ "${1:-}" == "--check" ]]; then
  kubectl -n lakehouse wait kafkaconnector/dbz-shop --for=condition=Ready --timeout=10s >/dev/null
  kubectl -n lakehouse wait kafkaconnector/iceberg-shop --for=condition=Ready --timeout=10s >/dev/null
  (cd 30-connect && $PY verify.py shop_raw orders 3)
  echo "OK: S2 Bronze dolu"; exit 0
fi

helm upgrade --install strimzi oci://quay.io/strimzi-helm/strimzi-kafka-operator --version 1.2.0 \
  -n lakehouse --set watchNamespaces="{lakehouse}" --wait --timeout 5m
kubectl apply -f 30-connect/kafka.yaml
kubectl -n lakehouse wait kafka/lakehouse --for=condition=Ready --timeout=600s
kubectl apply -f 30-connect/rbac.yaml

[[ -f "$STATE/connect-image" ]] || echo "ttl.sh/lakehouse-connect-$(uuidgen | tr 'A-Z' 'a-z'):24h" > "$STATE/connect-image"
IMG=$(cat "$STATE/connect-image")
sed "s#ttl.sh/lakehouse-connect-spike-CHANGEME:24h#${IMG}#" 30-connect/connect.yaml | kubectl apply -f -
echo "build başladı: $(date +%T)"; T0=$(date +%s)
kubectl -n lakehouse wait kafkaconnect/connect --for=condition=Ready --timeout=1800s
echo "build+ready süresi: $(( $(date +%s) - T0 )) s" | tee "$STATE/s2-build-time.txt"

kubectl apply -f 30-connect/pg-source.yaml -f 30-connect/iceberg-sink.yaml
kubectl -n lakehouse wait kafkaconnector/dbz-shop --for=condition=Ready --timeout=300s
kubectl -n lakehouse wait kafkaconnector/iceberg-shop --for=condition=Ready --timeout=300s
echo "commit bekleniyor (30 s aralık)…"; sleep 75
"$0" --check
EOF
chmod +x test/spike/30-connect.sh
test/spike/30-connect.sh --check   # beklenen: HATA
```

- [ ] **Step 4: Kur; S2a/S2b/S2c/S2d bulgularını yaz**

```bash
test/spike/30-connect.sh 2>&1 | tee test/spike/.state/30.log
```
Kontrol ve kayıt:
- **S2a**: `cat test/spike/.state/s2-build-time.txt`; Connect pod log'unda `ClassNotFoundException`/`NoClassDefFoundError` var mı (`kubectl -n lakehouse logs deploy/connect-connect | grep -i -E "ClassNotFound|NoClassDef" | head`). Varsa eksik artefaktı (ör. `org.apache.parquet:parquet-hadoop`, `org.apache.hadoop:hadoop-common`) `connect.yaml`'a **maven artefaktı olarak** ekle, yeniden uygula, bulguya yaz. Asla jar kopyalama/hack yok.
- **S2b**: `kubectl -n lakehouse get kafkaconnector dbz-shop -o jsonpath='{.status.connectorStatus.connector.state}'` → `RUNNING`; log'da `${secrets:` çözümleme hatası yok.
- **S2c**: `shop_raw` namespace'i sink tarafından yaratıldı mı? (`.venv/bin/polaris --client-id root --client-secret s3cr3t namespaces list --catalog lakehouse`). Yaratılmadıysa sink log'unda `NoSuchNamespaceException` → `verify.py` öncesi `cat.create_namespace("shop_raw")` gerektiğini bulguya yaz (runbook adımı).
- **S2d**: `verify.py` çıktısındaki `schema`/`ops`/örnek satır: `_cdc.op` değer kümesi (I/U/D mi c/u/d mi), `_cdc.ts` tipi (timestamp mı long mu), `_cdc.key`/`_cdc.source` içeriği.

- [ ] **Step 5: UPDATE/DELETE akışı**

```bash
kubectl -n lakehouse exec demo-pg-1 -c postgres -- psql -U postgres -d shop -c \
  "UPDATE public.orders SET status='shipped', amount=amount+1 WHERE id=1; DELETE FROM public.orders WHERE id=2;"
sleep 75
(cd test/spike/30-connect && ../../../.venv/bin/python verify.py shop_raw orders 5)
# beklenen: rows=5, ops içinde 1 U(veya u) ve 1 D(veya d); DELETE satırında id=2 ve önceki değerler (before) dolu — bulguya yaz.
```

- [ ] **Step 6: Commit**

```bash
git add test/spike/30-connect.sh test/spike/30-connect docs/plans/2026-09-10-f0-findings.md
git commit -m "spike(f0/S2): Strimzi 1.2 spec.build (Debezium 3.6 + Iceberg 1.11 maven) -> Bronze via DebeziumTransform

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 5: S3 — `default-partition-by=day(_cdc.ts)` (nested alan) denemesi

**Files:**
- Create: `test/spike/30-connect/iceberg-sink-partitioned.yaml`

**Interfaces:**
- Consumes: Task 4 ortamı
- Produces: Bulgu S3 (partition spec çıktısı) → spec §5.2 Bronze partition kararı

- [ ] **Step 1: Ayrı hedef namespace'e ikinci sink**

```bash
sed -e 's/name: iceberg-shop$/name: iceberg-shop-part/' \
    -e 's#shop_raw.{table}#shop_part_raw.{table}#' \
    -e 's#shop.dlq#shop-part.dlq#' \
    test/spike/30-connect/iceberg-sink.yaml > test/spike/30-connect/iceberg-sink-partitioned.yaml
# partition satırını ekle (iceberg.tables.dynamic-enabled satırından sonra)
sed -i '' 's/    iceberg.tables.dynamic-enabled: "true"/    iceberg.tables.dynamic-enabled: "true"\n    iceberg.tables.default-partition-by: day(_cdc.ts)/' test/spike/30-connect/iceberg-sink-partitioned.yaml
grep -n "default-partition-by\|shop_part_raw" test/spike/30-connect/iceberg-sink-partitioned.yaml   # 2 satır görünmeli
kubectl apply -f test/spike/30-connect/iceberg-sink-partitioned.yaml
kubectl -n lakehouse wait kafkaconnector/iceberg-shop-part --for=condition=Ready --timeout=300s
sleep 75
```

- [ ] **Step 2: Partition spec'i oku ve kaydet**

```bash
(cd test/spike/30-connect && ../../../.venv/bin/python verify.py shop_part_raw orders 5) | grep -E "partition spec|rows=|OK"
```
Beklenen (başarı): `partition spec:` satırında `day` transform'lu bir alan (`_cdc.ts_day` gibi). Başarısızlık belirtileri: sink log'unda `Cannot find source column`/`IllegalArgumentException` ve/veya tablo `unpartitioned` (sink'in spec-hatasında sessizce partition'sız yarattığı bilinen davranış — rapor 03 §I.3). Sonucu bulgu S3'e yaz: **partition'lı** → spec §5.2 aynen; **partition'sız** → spec'te "Bronze partition'sız; TTL `DELETE WHERE _cdc.ts < …` satır-delete" notu.

- [ ] **Step 3: Commit**

```bash
git add test/spike/30-connect/iceberg-sink-partitioned.yaml docs/plans/2026-09-10-f0-findings.md
git commit -m "spike(f0/S3): default-partition-by on nested _cdc.ts - result recorded

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 6: S4 — spark-operator 2.5.2 + Spark 4.1 + Iceberg 1.11 MoR MERGE

**Files:**
- Create: `test/spike/40-spark.sh`, `test/spike/40-spark/merge_spike.py`, `test/spike/40-spark/sparkapp.yaml`

**Interfaces:**
- Consumes: Bronze `lakehouse.shop_raw.orders` (Task 4, ≥5 satır: 3 I + 1 U + 1 D), `spark/polaris-spark` Secret, `spark/minio-creds`
- Produces: Silver `lakehouse.shop.orders` (2 satır: id 1 shipped, id 3 new→closed); bulgular S4a/S4b

- [ ] **Step 1: PySpark işi (ConfigMap'e mount edilir)**

```bash
mkdir -p test/spike/40-spark
cat > test/spike/40-spark/merge_spike.py <<'EOF'
"""S4: Bronze(_cdc) -> Silver MoR MERGE, position-delete rewrite, time travel. Spark 4.1 / Iceberg 1.11."""
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("merge-spike").getOrCreate()
C = "lakehouse"

# Bronze'daki op değer kümesini keşfet (S2d bulgusu: I/U/D mi c/u/d mi)
ops = [r[0] for r in spark.sql(f"SELECT DISTINCT _cdc.op FROM {C}.shop_raw.orders").collect()]
print("BRONZE_OPS", ops)
DEL = "D" if "D" in ops else "d"

spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {C}.shop")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {C}.shop.orders (id BIGINT, status STRING, amount DECIMAL(10,2), updated_at TIMESTAMP)
USING iceberg PARTITIONED BY (bucket(4, id))
TBLPROPERTIES ('format-version'='2','write.merge.mode'='merge-on-read','write.update.mode'='merge-on-read',
  'write.delete.mode'='merge-on-read','write.distribution-mode'='hash','write.metadata.delete-after-commit.enabled'='true')""")

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW inc AS
SELECT id, status, amount, updated_at, _cdc.op AS op FROM (
  SELECT *, row_number() OVER (PARTITION BY id ORDER BY _cdc.ts DESC, _cdc.offset DESC) AS rn
  FROM {C}.shop_raw.orders) WHERE rn = 1""")
print("INC", spark.sql("SELECT id, status, op FROM inc ORDER BY id").collect())

spark.sql(f"""
MERGE INTO {C}.shop.orders t USING inc s ON t.id = s.id
WHEN MATCHED AND s.op = '{DEL}' THEN DELETE
WHEN MATCHED THEN UPDATE SET status = s.status, amount = s.amount, updated_at = s.updated_at
WHEN NOT MATCHED AND s.op <> '{DEL}' THEN INSERT (id, status, amount, updated_at) VALUES (s.id, s.status, s.amount, s.updated_at)""")
silver = spark.sql(f"SELECT id, status, amount FROM {C}.shop.orders ORDER BY id").collect()
print("SILVER", silver)
assert [r.id for r in silver] == [1, 3], silver
assert silver[0].status == "shipped", silver

snap_before = spark.sql(f"SELECT snapshot_id FROM {C}.shop.orders.snapshots ORDER BY committed_at DESC LIMIT 1").collect()[0][0]
# 2. tur: bir satır daha güncelle -> position delete dosyası oluşmalı (MoR)
spark.sql(f"UPDATE {C}.shop.orders SET status = 'closed' WHERE id = 3")
deletes = spark.sql(f"SELECT count(*) FROM {C}.shop.orders.files WHERE content = 1").collect()[0][0]   # 1 = position deletes
print("POSITION_DELETE_FILES_AFTER_UPDATE", deletes)
assert deletes >= 1, "MoR beklenirdi, delete dosyası yok (CoW mu çalıştı?)"

spark.sql(f"CALL {C}.system.rewrite_position_delete_files(table => 'shop.orders')")
spark.sql(f"CALL {C}.system.rewrite_data_files(table => 'shop.orders', options => map('delete-file-threshold','1'))")
deletes_after = spark.sql(f"SELECT count(*) FROM {C}.shop.orders.files WHERE content = 1").collect()[0][0]
print("POSITION_DELETE_FILES_AFTER_COMPACTION", deletes_after)

tt = spark.sql(f"SELECT status FROM {C}.shop.orders VERSION AS OF {snap_before} WHERE id = 3").collect()[0][0]
print("TIME_TRAVEL_id3_before_update", tt)
assert tt == "new", tt

# ANSI mode kontrolü (Spark 4 varsayılan): geçersiz CAST hata fırlatmalı
try:
    spark.sql("SELECT CAST('abc' AS INT)").collect(); print("ANSI_MODE", "off (CAST null döndü)")
except Exception as e:
    print("ANSI_MODE", "on (CAST hata:", type(e).__name__, ")")
print("S4_OK")
spark.stop()
EOF
```

- [ ] **Step 2: SparkApplication + kurulum script'i**

```bash
cat > test/spike/40-spark/sparkapp.yaml <<'EOF'
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata: {name: merge-spike, namespace: spark}
spec:
  type: Python
  pythonVersion: "3"
  mode: cluster
  image: apache/spark:4.1.0-java21-python3
  imagePullPolicy: IfNotPresent
  mainApplicationFile: local:///opt/job/merge_spike.py
  sparkVersion: 4.1.0
  restartPolicy: {type: Never}
  sparkConf:
    spark.jars.packages: org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0,org.apache.iceberg:iceberg-aws-bundle:1.11.0
    spark.jars.ivy: /tmp/.ivy2
    spark.sql.extensions: org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
    spark.sql.catalog.lakehouse: org.apache.iceberg.spark.SparkCatalog
    spark.sql.catalog.lakehouse.type: rest
    spark.sql.catalog.lakehouse.uri: http://polaris.polaris.svc:8181/api/catalog
    spark.sql.catalog.lakehouse.warehouse: lakehouse
    spark.sql.catalog.lakehouse.scope: PRINCIPAL_ROLE:ALL
    spark.sql.catalog.lakehouse.io-impl: org.apache.iceberg.aws.s3.S3FileIO
    spark.sql.catalog.lakehouse.s3.endpoint: http://minio.minio.svc:9000
    spark.sql.catalog.lakehouse.s3.path-style-access: "true"
    spark.sql.catalog.lakehouse.client.region: us-east-1
    spark.sql.defaultCatalog: lakehouse
  driver:
    cores: 1
    memory: 1g
    serviceAccount: spark-operator-spark
    env:
    - {name: AWS_REGION, value: us-east-1}
    - {name: AWS_ACCESS_KEY_ID, valueFrom: {secretKeyRef: {name: minio-creds, key: AWS_ACCESS_KEY_ID}}}
    - {name: AWS_SECRET_ACCESS_KEY, valueFrom: {secretKeyRef: {name: minio-creds, key: AWS_SECRET_ACCESS_KEY}}}
    volumeMounts: [{name: job, mountPath: /opt/job}]
  executor:
    cores: 1
    instances: 1
    memory: 1g
    env:
    - {name: AWS_REGION, value: us-east-1}
    - {name: AWS_ACCESS_KEY_ID, valueFrom: {secretKeyRef: {name: minio-creds, key: AWS_ACCESS_KEY_ID}}}
    - {name: AWS_SECRET_ACCESS_KEY, valueFrom: {secretKeyRef: {name: minio-creds, key: AWS_SECRET_ACCESS_KEY}}}
    volumeMounts: [{name: job, mountPath: /opt/job}]
  volumes:
  - name: job
    configMap: {name: merge-spike-job}
EOF

cat > test/spike/40-spark.sh <<'EOF'
#!/usr/bin/env bash
# S4: spark-operator 2.5.2 + Spark 4.1 (resmi imaj, Maven packages) -> MoR MERGE spike.
set -euo pipefail
cd "$(dirname "$0")"; STATE=.state

if [[ "${1:-}" == "--check" ]]; then
  st=$(kubectl -n spark get sparkapplication merge-spike -o jsonpath='{.status.applicationState.state}')
  [[ "$st" == "COMPLETED" ]] || { echo "durum: $st"; kubectl -n spark logs merge-spike-driver --tail=60; exit 1; }
  kubectl -n spark logs merge-spike-driver | grep -E "BRONZE_OPS|INC|SILVER|POSITION_DELETE|TIME_TRAVEL|ANSI_MODE|S4_OK" | tee "$STATE/s4-results.txt"
  grep -q S4_OK "$STATE/s4-results.txt"; echo "OK: S4"; exit 0
fi

helm repo add spark-operator https://kubeflow.github.io/spark-operator >/dev/null 2>&1 || true
helm repo update spark-operator >/dev/null
helm upgrade --install spark-operator spark-operator/spark-operator --version 2.5.2 -n spark \
  --set 'spark.jobNamespaces={spark}' --wait --timeout 5m
# Polaris credential'ını sparkConf'a enjekte et (Secret -> conf); prod'da glue chart aynı işi values ile yapar.
CRED=$(kubectl -n spark get secret polaris-spark -o jsonpath='{.data.credential}' | base64 -d)
kubectl -n spark create configmap merge-spike-job --from-file=40-spark/merge_spike.py --dry-run=client -o yaml | kubectl apply -f -
kubectl -n spark delete sparkapplication merge-spike --ignore-not-found
sed "s#spark.sql.catalog.lakehouse.scope: PRINCIPAL_ROLE:ALL#spark.sql.catalog.lakehouse.scope: PRINCIPAL_ROLE:ALL\n    spark.sql.catalog.lakehouse.credential: ${CRED}#" 40-spark/sparkapp.yaml | kubectl apply -f -
T0=$(date +%s)
for i in $(seq 1 120); do
  st=$(kubectl -n spark get sparkapplication merge-spike -o jsonpath='{.status.applicationState.state}' 2>/dev/null || true)
  [[ "$st" == "COMPLETED" || "$st" == "FAILED" ]] && break; sleep 10
done
echo "S4 çalışma süresi (packages çözümü dahil): $(( $(date +%s) - T0 )) s" | tee "$STATE/s4-runtime.txt"
"$0" --check
EOF
chmod +x test/spike/40-spark.sh
test/spike/40-spark.sh --check   # beklenen: HATA
```
ServiceAccount adı: chart 2.5.x `spark-operator-spark` (release adı `spark-operator` + `-spark`) SA'sını `spark.jobNamespaces` içinde yaratır; farklıysa `kubectl -n spark get sa` ile bul ve `sparkapp.yaml`'ı düzelt (bulguya yaz).

- [ ] **Step 3: Çalıştır; S4a/S4b bulgularını yaz**

```bash
test/spike/40-spark.sh 2>&1 | tee test/spike/.state/40.log
cat test/spike/.state/s4-results.txt test/spike/.state/s4-runtime.txt
```
Kayıt: packages çözümleme süresi (ilk çalıştırma, internet) → 3 dk'yı aşıyorsa F1'de "init-container ile jar önbelleği (hâlâ resmi imaj)" seçeneği notu; `ANSI_MODE on` ise merge şema-genişletme SQL'inde açık CAST/`try_cast` gereksinimi notu; `POSITION_DELETE_FILES_AFTER_UPDATE ≥ 1` MoR kanıtı; time travel çalıştı → kabul testi 264 için Polaris altında time travel kanıtı.

- [ ] **Step 4: Commit**

```bash
git add test/spike/40-spark.sh test/spike/40-spark docs/plans/2026-09-10-f0-findings.md
git commit -m "spike(f0/S4): spark-operator 2.5.2 + Spark 4.1 + Iceberg 1.11 MoR MERGE via Maven packages

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 7: Bulguların kapanışı, F1/F2 kararları, teardown

**Files:**
- Modify: `docs/plans/2026-09-10-f0-findings.md`, `docs/specs/2026-09-10-lakehouse-v2-design.md` (yalnız §5.2 partition satırı ve §7 katalog satırı, bulguya göre)
- Create: `test/spike/90-teardown.sh`, `test/spike/README.md`

- [ ] **Step 1: Bulgu tablosunu tamamla**

Her satırda "Sonuç (kanıt)" = komut çıktısından yapıştırılmış satır(lar) (`s2-build-time.txt`, `verify.py` çıktısı, `s4-results.txt`), "Karar" = F1/F2'ye giden tek cümle. Tablo altına **"F1/F2 için bağlayıcı kararlar"** listesi:
1. Katalog: Polaris (S1c OK) / Lakekeeper (S1c FAIL) — spec §7 güncellenir.
2. Connect artefakt listesi: S2a'da eklenen ek maven artefaktları dahil nihai `plugins:` bloğu (F1 glue chart'ına birebir taşınır).
3. Bronze partition: S3 sonucu — spec §5.2 satırı düzeltilir.
4. `_cdc.op` değer kümesi ve `_cdc.ts` tipi — F2 `merge_cdc.py` sabitleri.
5. Namespace ön-yaratma gerekiyor mu (S2c) — `runbooks/add-source.md` adımı.
6. Spark jar dağıtımı: `spark.jars.packages` (S4a süresi kabul edilebilir) / init-container — F2 CR şablonu.
7. ANSI mode etkisi — F2 şema-uzlaştırma SQL'i.

- [ ] **Step 2: Spec'i bulgulara göre güncelle** (yalnız etkilenen satırlar; başka değişiklik yok)

```bash
grep -n "default-partition-by=day(_cdc.ts)\|Polaris 1.7\*\*: resmi chart" docs/specs/2026-09-10-lakehouse-v2-design.md
# ilgili satırları S3/S1 sonucuna göre düzenle (Edit), değişikliği commit mesajında gerekçelendir
```

- [ ] **Step 3: Teardown + README**

```bash
cat > test/spike/90-teardown.sh <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
for f in .state/pf-*.pid; do [[ -f "$f" ]] && kill "$(cat "$f")" 2>/dev/null || true; done
KIND_EXPERIMENTAL_PROVIDER="${KIND_EXPERIMENTAL_PROVIDER:-podman}" kind delete cluster --name lh-spike
rm -rf .state
echo "OK: spike ortamı silindi"
EOF
chmod +x test/spike/90-teardown.sh
cat > test/spike/README.md <<'EOF'
# F0 spike'ları

Sıra: `00-cluster.sh` → `10-cnpg.sh` → `20-polaris.sh` → `30-connect.sh` → (S3 için `iceberg-sink-partitioned.yaml`) → `40-spark.sh` → `90-teardown.sh`.
Her script idempotent; `--check` yalnız doğrular. Python araçları: `python3 -m venv .venv && .venv/bin/pip install -r test/spike/requirements.txt` (repo kökünde).
Bulgular: `docs/plans/2026-09-10-f0-findings.md`. Gereksinim: Podman machine ≥ 6 CPU / 10 GB.
EOF
```

- [ ] **Step 4: Commit + dalı yayınla**

```bash
git add -A
git commit -m "spike(f0): findings closed, spec adjusted from evidence, teardown script

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
git push -u origin v2
```

---

## Self-review

**Spec coverage (§10 spike tablosu):** S1 → Task 3 (S1a–S1d dahil `stsUnavailable`); S2 → Task 4 (build, secrets provider, namespace, `_cdc` şekli, UPDATE/DELETE); S3 → Task 5; S4 → Task 6 (packages süresi, MoR delete dosyası kanıtı, `rewrite_position_delete_files`, time travel, ANSI). Spec §3 sürümleri Global Constraints'te; §5.2 şablon değerleri `pg-source.yaml`/`iceberg-sink.yaml`'da birebir (JSON+schemas, filtered publication, tip modları, DebeziumTransform, auto-create-props). F1–F6 bu planın kapsamı DIŞI (ayrı planlar, bulgu dosyasına bağlı).

**Placeholder taraması:** `CHANGEME` yalnız script'in `sed` ile değiştirdiği ttl.sh imaj adı (bilinçli, script içinde çözülür). `setup.yaml` şeması CLI `--help` ile doğrulanacak şekilde işaretli (spike'ın kendisi bunu ölçer; alternatif komutlar Task 3 Step 4'te yazılı). Başka TBD/TODO yok.

**Tutarlılık:** Namespace/Secret adları görevler arası aynı (`polaris-connect`/`polaris-spark` key `credential`; `minio-creds` keys `AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY`; `.state/polaris-<p>.env` `CLIENT_ID=/CLIENT_SECRET=`); `verify.py` `.state/polaris-connect.env` biçimini okur; Task 6 Bronze `shop_raw.orders` Task 4'ün `cdc.target.pattern`'ıyla aynı; `DEL` sabiti S2d bulgusuna dinamik uyar; tüm script'lerin doğrulama bayrağı `--check`.
