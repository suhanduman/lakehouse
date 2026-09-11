# Lakehouse v2 — F1 Platform (bootstrap + ArgoCD app-of-apps + glue) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Tek komutla (`bootstrap/bootstrap.sh`) ArgoCD kurulup app-of-apps ile **Strimzi + CNPG + Keycloak operatörleri**, **glue chart'ı** (Kafka, KafkaConnect `spec.build`, CNPG kümeleri, Keycloak + realm, MinIO[dev], NetworkPolicy, Route/Ingress) ve **Polaris** deklaratif olarak ayağa kalksın; kind'da e2e (GitHub Actions + lokal) Polaris smoke'una kadar yeşil olsun.

**Architecture:** Repo = konfigürasyon. `platform/` ArgoCD `Application` manifestleri (upstream chart'lar + bizim `glue` chart'ı; env farkları kustomize patch ile), `glue/` yalnız CR'lardan oluşan ince Helm chart'ı, `bootstrap/` ArgoCD'yi kuran tek script. F0 bulguları birebir taşınır (spec.build artefakt listesi, KubernetesSecretConfigProvider, Polaris bootstrap Job + `setup apply`). Kod yok; Helm şablonları ve iki bash script var.

**Tech Stack:** ArgoCD 3.5.2 · Strimzi 1.2.0 (Kafka 4.3.1) · CloudNativePG chart 0.29.0 (op 1.30.0) · Keycloak operator 26.7.3 · Apache Polaris chart 1.7.0 · MinIO (dev) · Helm 4 + helm-unittest · kind 0.33 (Podman/Docker) · GitHub Actions

**Spec:** `docs/specs/2026-09-10-lakehouse-v2-design.md` (§3 sürümler, §4 repo düzeni, §5.1 Kafka/Connect, §7 Polaris/Keycloak, §8 güvenlik, §9 test, §13 F1) · **Bulgular:** `docs/plans/2026-09-10-f0-findings.md` → "F1/F2 için bağlayıcı kararlar" (1, 2, 3, 6, 8, 9)

## Global Constraints

- Özel imaj YOK, hack YOK; her bileşen upstream chart/operatör + values. Kod yalnız `bootstrap/bootstrap.sh` ve `runbooks/scripts/polaris-setup.sh` (bash).
- Sürümler sabit: ArgoCD `v3.5.2`; Strimzi chart `1.2.0` / Kafka `4.3.1`; CNPG chart `0.29.0`; Keycloak operator manifestleri `26.7.3`; Polaris chart `1.7.0`; Debezium `3.6.2.Final`; Iceberg `1.11.0`; Hadoop client `3.4.3`.
- Tek uygulama namespace'i **`lakehouse`** (Kafka, Connect, CNPG kümeleri, Keycloak, Polaris, MinIO[dev]); operatörler: Strimzi → `lakehouse` (watch lakehouse), CNPG → `cnpg-system`, Keycloak operator → `lakehouse`, ArgoCD → `argocd`.
- `KafkaConnect` (Strimzi v1): `groupId/configStorageTopic/offsetStorageTopic/statusStorageTopic` **spec üst düzeyinde**; `config.providers=secrets` + `io.strimzi.kafka.KubernetesSecretConfigProvider` + SA `connect-connect`'e Secret okuma Role'ü; `spec.build.plugins` F0'daki nihai liste (Debezium pg/sqlserver/mongodb zip + iceberg-kafka-connect, -transforms, -parquet, -orc, -aws, -aws-bundle 1.11.0 + hadoop-client-api/-runtime 3.4.3).
- Polaris: `persistence.type=relational-jdbc` (CNPG `polaris-db-app` secret: `username/password/jdbc-uri`), health `8182`, API `8181`; bootstrap `apache/polaris-admin-tool:1.7.0 bootstrap --realm=POLARIS --credential=POLARIS,<id>,<secret>`; katalog/principal/grant `polaris setup apply` (runbook script'i; credential'lar apply stdout'undan Secret'a).
- `platform: vanilla|openshift` values bayrağı: Route ↔ Ingress; kind'da ingress kapalı.
- Air-gap yok. Testler: `glue` için helm-unittest (az), e2e = kind'da gerçek kurulum + Polaris smoke Job (küme içinde).
- Commit mesajı sonu: `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`. Dal: `v2`.

---

## Dosya yapısı

```
bootstrap/bootstrap.sh                      ArgoCD kur (pinli install.yaml) + root Application uygula; --env dev|prod --repo --revision --mode argocd|helm
platform/root-app.yaml                      Application lakehouse-root -> platform/envs/<env> (kustomize)
platform/apps/00-strimzi.yaml               helm OCI quay.io/strimzi-helm/strimzi-kafka-operator 1.2.0   (wave 0)
platform/apps/00-cnpg.yaml                  helm cnpg/cloudnative-pg 0.29.0                             (wave 0)
platform/apps/00-keycloak-operator.yaml     kustomize platform/keycloak-operator (uzak manifestler)        (wave 0)
platform/apps/10-glue.yaml                  bizim glue chart'ı + values/glue.yaml                        (wave 1)
platform/apps/20-polaris.yaml               helm polaris/polaris 1.7.0 + values/polaris.yaml             (wave 2)
platform/keycloak-operator/kustomization.yaml
platform/values/glue.yaml, polaris.yaml     prod varsayılanları
platform/values/glue-dev.yaml, polaris-dev.yaml   kind: 1 broker, MinIO açık, ingress kapalı
platform/envs/dev/kustomization.yaml        apps/*.yaml + patch: values dosyası -> glue-dev.yaml / polaris-dev.yaml
platform/envs/prod/kustomization.yaml
platform/polaris/setup.yaml                 katalog + namespace + rol + grant + principal (polaris setup apply)
glue/Chart.yaml, values.yaml
glue/templates/_helpers.tpl
glue/templates/kafka.yaml                   KafkaNodePool + Kafka (tls/scram listener; dev'de plain da)
glue/templates/kafka-connect.yaml           KafkaConnect spec.build + Role/RoleBinding (secrets reader)
glue/templates/kafka-users.yaml             KafkaUser connect (ACL: sources listesinden prefix'ler)
glue/templates/cnpg.yaml                    Cluster polaris-db, keycloak-db
glue/templates/polaris-bootstrap.yaml       admin-tool Job (CNPG'den sonra)
glue/templates/keycloak.yaml                Keycloak CR
glue/templates/keycloak-realm.yaml          KeycloakRealmImport (v1'den taşınan realm: AD/LDAPS federasyonu + client'lar)
glue/templates/minio.yaml                   dev-only MinIO Deployment/Service/bucket Job (components.minio)
glue/templates/networkpolicy.yaml           default-deny ingress + namespace-içi allow + platform ns istisnaları
glue/templates/route.yaml, ingress.yaml     platform bayrağına göre Keycloak/Polaris dış erişimi
glue/tests/*_test.yaml                      helm-unittest (3 dosya)
runbooks/install.md                         kurulum + Polaris setup + doğrulama + sorun giderme
runbooks/scripts/polaris-setup.sh           port-forward + setup apply + credential Secret'ları (F0 20-polaris.sh'ten)
test/e2e/kind.sh                            kind create (+Podman pids düzeltmesi, F0 S0d) + dev Secret'ları
test/e2e/run.sh                             kind -> bootstrap -> Application'lar Healthy -> polaris-setup -> smoke Job
test/e2e/polaris-smoke/{smoke.py,job.yaml}  F0'dan (küme içi pyiceberg)
.github/workflows/e2e.yaml                  PR/push(v2) -> helm-unittest + kind (Docker) -> test/e2e/run.sh
```

---

### Task 1: `glue` chart iskeleti + Kafka/KafkaConnect/KafkaUser şablonları (helm-unittest ile)

**Files:**
- Create: `glue/Chart.yaml`, `glue/values.yaml`, `glue/templates/_helpers.tpl`, `glue/templates/kafka.yaml`, `glue/templates/kafka-connect.yaml`, `glue/templates/kafka-users.yaml`, `glue/tests/kafka_test.yaml`

**Interfaces:**
- Produces: Helm chart `glue` (`helm template glue glue -f platform/values/glue-dev.yaml` render eder). Kaynak adları: Kafka `lakehouse` (bootstrap `lakehouse-kafka-bootstrap:9093` tls / `:9092` plain[dev]); KafkaConnect `connect` (SA `connect-connect`, `strimzi.io/use-connector-resources=true`); KafkaUser `connect` (Secret `connect`, key `password`); values anahtarları: `platform`, `namespace`, `versions.*`, `kafka.{replicas,storageSize,plainListener,externalListener,config}`, `connect.{replicas,buildImage,buildPushSecret,resources}`, `sources[]` (`{name, topicPrefix}`), `cnpg.*`, `polaris.*`, `keycloak.*`, `components.minio`, `minio.*`, `networkPolicy.enabled`, `ingress.*`, `route.*`.

- [ ] **Step 1: helm-unittest kur, chart iskeleti + BAŞARISIZ test**

```bash
helm plugin list | grep -q unittest || helm plugin install https://github.com/helm-unittest/helm-unittest
mkdir -p glue/templates glue/tests
cat > glue/Chart.yaml <<'EOF'
apiVersion: v2
name: glue
description: Lakehouse v2 — yalnız CR'lardan oluşan ince chart (Kafka, Connect, CNPG, Keycloak, Polaris bootstrap, NetworkPolicy, Route/Ingress). Upstream bileşenler ArgoCD Application'larıyla ayrı kurulur.
type: application
version: 0.1.0
appVersion: "2.0.0"
EOF
cat > glue/values.yaml <<'EOF'
# platform: vanilla | openshift  (Route <-> Ingress)
platform: vanilla
namespace: lakehouse

versions:
  kafka: "4.3.1"
  kafkaMetadata: "4.3-IV0"
  debezium: "3.6.2.Final"
  iceberg: "1.11.0"
  hadoopClient: "3.4.3"
  polarisAdminTool: "1.7.0"

kafka:
  replicas: 3
  storageSize: 100Gi
  plainListener: false          # dev/kind: true (9092, TLS'siz); prod: yalnız tls 9093 SCRAM
  externalListener: false       # Fluent Bit ajanları için (F3) — openshift: route, vanilla: nodeport
  config:
    default.replication.factor: 3
    min.insync.replicas: 2
    offsets.topic.replication.factor: 3
    transaction.state.log.replication.factor: 3
    transaction.state.log.min.isr: 2

connect:
  replicas: 1
  # spec.build çıktısı: küme registry'si (OpenShift: image-registry.openshift-image-registry.svc:5000/<ns>/connect:latest; dev: ttl.sh)
  buildImage: ttl.sh/lakehouse-connect:24h
  buildPushSecret: ""           # özel registry için docker-registry Secret adı; boş = anonim (ttl.sh)
  resources:
    requests: {cpu: 500m, memory: 1536Mi}
    limits: {memory: 2Gi}

# Kaynak DB'ler: KafkaUser ACL'leri + (F2) KafkaConnector şablonları buradan türetilir. topicPrefix = Debezium topic.prefix
sources: []
#  - name: shop
#    topicPrefix: shop

cnpg:
  polarisDb: {instances: 1, storageSize: 10Gi}
  keycloakDb: {instances: 1, storageSize: 10Gi}

polaris:
  realm: POLARIS
  rootCredentialSecret: polaris-root   # keys: clientId, clientSecret (Git'e girmez; kurulumdan önce yaratılır)

keycloak:
  hostname: keycloak.example.com
  realm:
    name: lakehouse
  adminSecret: keycloak-admin          # keys: username, password
  ldap:
    enabled: false
    connectionUrl: ldaps://ad.example.com:636
    usersDn: "OU=Users,DC=example,DC=com"
    bindDn: "CN=svc-lakehouse,OU=Service,DC=example,DC=com"
    bindCredential: ""                 # realm import düz değer ister; Secret'tan almak için F4 notu
  clients:                             # redirect URI'ler F4'te servis host'larıyla doldurulur
    trino:      {redirectUris: []}
    superset:   {redirectUris: []}
    jupyterhub: {redirectUris: []}
    polaris:    {redirectUris: []}

components:
  minio: false                  # DEV-ONLY tek-replika S3 (kind). Prod: müşterinin S3'ü
minio:
  rootUser: minioadmin
  rootPassword: minioadmin
  buckets: [lakehouse]

networkPolicy:
  enabled: true

ingress:
  enabled: false
  className: nginx
route:
  enabled: false                # platform=openshift iken true
EOF
cat > glue/templates/_helpers.tpl <<'EOF'
{{- define "glue.ns" -}}{{ .Values.namespace }}{{- end -}}
{{- define "glue.isOpenShift" -}}{{ eq .Values.platform "openshift" }}{{- end -}}
EOF
cat > glue/tests/kafka_test.yaml <<'EOF'
suite: kafka + connect
templates: [kafka.yaml, kafka-connect.yaml, kafka-users.yaml]
tests:
  - it: renders Kafka v1 with KRaft node pool
    template: kafka.yaml
    asserts:
      - hasDocuments: {count: 2}
      - documentIndex: 1
        equal: {path: apiVersion, value: kafka.strimzi.io/v1}
      - documentIndex: 1
        equal: {path: spec.kafka.version, value: "4.3.1"}
  - it: dev enables plain listener
    template: kafka.yaml
    set: {kafka.plainListener: true}
    asserts:
      - documentIndex: 1
        contains: {path: spec.kafka.listeners, content: {name: plain, port: 9092, type: internal, tls: false}}
  - it: KafkaConnect has v1 top-level storage topics, secrets provider and the F0 plugin list
    template: kafka-connect.yaml
    documentSelector: {path: kind, value: KafkaConnect}
    asserts:
      - equal: {path: spec.groupId, value: connect}
      - equal: {path: spec.configStorageTopic, value: connect-configs}
      - equal: {path: spec.config["config.providers.secrets.class"], value: io.strimzi.kafka.KubernetesSecretConfigProvider}
      - lengthEqual: {path: spec.build.plugins[3].artifacts, count: 8}
      - contains: {path: spec.build.plugins[3].artifacts, content: {type: maven, group: org.apache.iceberg, artifact: iceberg-orc, version: "1.11.0"}}
      - contains: {path: spec.build.plugins[3].artifacts, content: {type: maven, group: org.apache.hadoop, artifact: hadoop-client-runtime, version: "3.4.3"}}
  - it: connect KafkaUser gets prefix ACLs per source
    template: kafka-users.yaml
    set:
      sources: [{name: shop, topicPrefix: shop}, {name: hr, topicPrefix: hr}]
    asserts:
      - contains: {path: spec.authorization.acls, content: {resource: {type: topic, name: "shop.", patternType: prefix}, operations: [Read, Write, Describe, Create]}}
      - contains: {path: spec.authorization.acls, content: {resource: {type: topic, name: "hr.", patternType: prefix}, operations: [Read, Write, Describe, Create]}}
EOF
helm unittest glue 2>&1 | tail -3   # beklenen: HATA (template yok)
```

- [ ] **Step 2: Kafka şablonu**

```bash
cat > glue/templates/kafka.yaml <<'EOF'
apiVersion: kafka.strimzi.io/v1
kind: KafkaNodePool
metadata:
  name: dual-role
  namespace: {{ include "glue.ns" . }}
  labels: {strimzi.io/cluster: lakehouse}
  annotations: {argocd.argoproj.io/sync-wave: "0"}
spec:
  replicas: {{ .Values.kafka.replicas }}
  roles: [controller, broker]
  storage:
    type: jbod
    volumes: [{id: 0, type: persistent-claim, size: {{ .Values.kafka.storageSize }}, kraftMetadata: shared}]
---
apiVersion: kafka.strimzi.io/v1
kind: Kafka
metadata:
  name: lakehouse
  namespace: {{ include "glue.ns" . }}
  annotations:
    strimzi.io/node-pools: enabled
    strimzi.io/kraft: enabled
    argocd.argoproj.io/sync-wave: "0"
spec:
  kafka:
    version: {{ .Values.versions.kafka | quote }}
    metadataVersion: {{ .Values.versions.kafkaMetadata | quote }}
    listeners:
    - {name: tls, port: 9093, type: internal, tls: true, authentication: {type: scram-sha-512}}
{{- if .Values.kafka.plainListener }}
    - {name: plain, port: 9092, type: internal, tls: false}
{{- end }}
{{- if .Values.kafka.externalListener }}
    - {name: external, port: 9094, type: {{ ternary "route" "nodeport" (eq (include "glue.isOpenShift" .) "true") }}, tls: true, authentication: {type: scram-sha-512}}
{{- end }}
    authorization:
      type: simple
{{- if .Values.kafka.plainListener }}
      superUsers: [ANONYMOUS]      # yalnız dev: plain listener kimliksiz erişir
{{- end }}
    config:
{{ toYaml .Values.kafka.config | indent 6 }}
  entityOperator: {topicOperator: {}, userOperator: {}}
EOF
```

- [ ] **Step 3: KafkaConnect (spec.build + secrets provider RBAC) şablonu**

```bash
cat > glue/templates/kafka-connect.yaml <<'EOF'
# KubernetesSecretConfigProvider: Connect SA'nın Secret okuma izni (mount YOK) — F0 S2b
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata: {name: connect-secrets-reader, namespace: {{ include "glue.ns" . }}}
rules:
- apiGroups: [""]
  resources: [secrets]
  verbs: [get, list, watch]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata: {name: connect-secrets-reader, namespace: {{ include "glue.ns" . }}}
subjects: [{kind: ServiceAccount, name: connect-connect, namespace: {{ include "glue.ns" . }}}]
roleRef: {kind: Role, name: connect-secrets-reader, apiGroup: rbac.authorization.k8s.io}
---
apiVersion: kafka.strimzi.io/v1
kind: KafkaConnect
metadata:
  name: connect
  namespace: {{ include "glue.ns" . }}
  annotations:
    strimzi.io/use-connector-resources: "true"
    argocd.argoproj.io/sync-wave: "1"
spec:
  version: {{ .Values.versions.kafka | quote }}
  replicas: {{ .Values.connect.replicas }}
  bootstrapServers: lakehouse-kafka-bootstrap:9093
  tls:
    trustedCertificates:
    - {secretName: lakehouse-cluster-ca-cert, pattern: "*.crt"}
  authentication:
    type: scram-sha-512
    username: connect
    passwordSecret: {secretName: connect, password: password}
  # Strimzi v1: bu dört alan spec üst düzeyinde ZORUNLU (F0 S0c)
  groupId: connect
  configStorageTopic: connect-configs
  offsetStorageTopic: connect-offsets
  statusStorageTopic: connect-status
  config:
    config.storage.replication.factor: -1
    offset.storage.replication.factor: -1
    status.storage.replication.factor: -1
    config.providers: secrets
    config.providers.secrets.class: io.strimzi.kafka.KubernetesSecretConfigProvider
  build:
    output:
      type: docker
      image: {{ .Values.connect.buildImage }}
{{- if .Values.connect.buildPushSecret }}
      pushSecret: {{ .Values.connect.buildPushSecret }}
{{- end }}
    plugins:
    - name: debezium-postgres
      artifacts:
      - {type: zip, url: "https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/{{ .Values.versions.debezium }}/debezium-connector-postgres-{{ .Values.versions.debezium }}-plugin.zip"}
    - name: debezium-sqlserver
      artifacts:
      - {type: zip, url: "https://repo1.maven.org/maven2/io/debezium/debezium-connector-sqlserver/{{ .Values.versions.debezium }}/debezium-connector-sqlserver-{{ .Values.versions.debezium }}-plugin.zip"}
    - name: debezium-mongodb
      artifacts:
      - {type: zip, url: "https://repo1.maven.org/maven2/io/debezium/debezium-connector-mongodb/{{ .Values.versions.debezium }}/debezium-connector-mongodb-{{ .Values.versions.debezium }}-plugin.zip"}
    - name: iceberg   # F0 S2a ile sabitlenen nihai liste (Iceberg runtime zip'inin içeriğiyle örtüşür)
      artifacts:
      - {type: maven, group: org.apache.iceberg, artifact: iceberg-kafka-connect, version: {{ .Values.versions.iceberg | quote }}}
      - {type: maven, group: org.apache.iceberg, artifact: iceberg-kafka-connect-transforms, version: {{ .Values.versions.iceberg | quote }}}
      - {type: maven, group: org.apache.iceberg, artifact: iceberg-parquet, version: {{ .Values.versions.iceberg | quote }}}
      - {type: maven, group: org.apache.iceberg, artifact: iceberg-orc, version: {{ .Values.versions.iceberg | quote }}}
      - {type: maven, group: org.apache.iceberg, artifact: iceberg-aws, version: {{ .Values.versions.iceberg | quote }}}
      - {type: maven, group: org.apache.iceberg, artifact: iceberg-aws-bundle, version: {{ .Values.versions.iceberg | quote }}}
      - {type: maven, group: org.apache.hadoop, artifact: hadoop-client-api, version: {{ .Values.versions.hadoopClient | quote }}}
      - {type: maven, group: org.apache.hadoop, artifact: hadoop-client-runtime, version: {{ .Values.versions.hadoopClient | quote }}}
  resources:
{{ toYaml .Values.connect.resources | indent 4 }}
EOF
```

- [ ] **Step 4: KafkaUser `connect` (ACL'ler `sources` listesinden)**

```bash
cat > glue/templates/kafka-users.yaml <<'EOF'
# Connect'in Kafka kimliği. ACL'ler deklaratif: her kaynak (Debezium topic.prefix) için prefix ACL'i.
# Yeni kaynak = values.sources'a bir satır (runbooks/add-source.md); runtime ACL mutasyonu YOK.
apiVersion: kafka.strimzi.io/v1
kind: KafkaUser
metadata:
  name: connect
  namespace: {{ include "glue.ns" . }}
  labels: {strimzi.io/cluster: lakehouse}
  annotations: {argocd.argoproj.io/sync-wave: "0"}
spec:
  authentication: {type: scram-sha-512}
  authorization:
    type: simple
    acls:
    # Connect'in kendi iç topic'leri ve grubu
    - {resource: {type: topic, name: "connect-", patternType: prefix}, operations: [Read, Write, Describe, Create]}
    - {resource: {type: group, name: "connect", patternType: prefix}, operations: [Read]}
    # Iceberg sink coordinator control topic'leri + consumer grupları
    - {resource: {type: topic, name: "control-iceberg", patternType: prefix}, operations: [Read, Write, Describe, Create]}
    - {resource: {type: group, name: "cg-control-", patternType: prefix}, operations: [Read]}
    # Debezium schema-history + signal/notification topic'leri
    - {resource: {type: topic, name: "schema-history.", patternType: prefix}, operations: [Read, Write, Describe, Create]}
    - {resource: {type: topic, name: "debezium-", patternType: prefix}, operations: [Read, Write, Describe, Create]}
{{- range .Values.sources }}
    - {resource: {type: topic, name: "{{ .topicPrefix }}.", patternType: prefix}, operations: [Read, Write, Describe, Create]}
{{- end }}
    - {resource: {type: transactionalId, name: "connect", patternType: prefix}, operations: [Write, Describe]}
    - {resource: {type: cluster}, operations: [IdempotentWrite]}
EOF
helm unittest glue 2>&1 | tail -4   # beklenen: 4 test PASS
helm template glue glue --set kafka.plainListener=true --set 'sources[0].name=shop' --set 'sources[0].topicPrefix=shop' >/dev/null && echo "render ok"
```

- [ ] **Step 5: Commit**

```bash
git add glue && git commit -m "feat(glue): chart skeleton - Kafka (KRaft, scram/tls), KafkaConnect spec.build with F0 plugin list, connect KafkaUser ACLs from sources

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 2: CNPG kümeleri, Polaris bootstrap Job, MinIO (dev), NetworkPolicy, Route/Ingress

**Files:**
- Create: `glue/templates/cnpg.yaml`, `glue/templates/polaris-bootstrap.yaml`, `glue/templates/minio.yaml`, `glue/templates/networkpolicy.yaml`, `glue/templates/route.yaml`, `glue/templates/ingress.yaml`, `glue/tests/platform_test.yaml`

**Interfaces:**
- Consumes: values `cnpg.*`, `polaris.rootCredentialSecret`, `versions.polarisAdminTool`, `components.minio`, `minio.*`, `networkPolicy.enabled`, `ingress.*`, `route.*`, `platform`, `keycloak.hostname`.
- Produces: CNPG `Cluster/polaris-db` → Secret `polaris-db-app` (`username`, `password`, `jdbc-uri`); `Cluster/keycloak-db` → Secret `keycloak-db-app`; Job `polaris-bootstrap` (sync-wave 2, Sync hook); Service `minio:9000` (dev) + Secret `minio-creds` (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`) + bucket `lakehouse`; NetworkPolicy `default-deny-ingress`, `allow-same-namespace`, `allow-platform-namespaces`; Route/Ingress `keycloak` (+ Route `polaris`).

- [ ] **Step 1: Testleri yaz (BAŞARISIZ)**

```bash
cat > glue/tests/platform_test.yaml <<'EOF'
suite: platform pieces
templates: [cnpg.yaml, polaris-bootstrap.yaml, minio.yaml, networkpolicy.yaml, route.yaml, ingress.yaml]
tests:
  - it: renders two CNPG clusters
    template: cnpg.yaml
    asserts:
      - hasDocuments: {count: 2}
      - documentIndex: 0
        equal: {path: metadata.name, value: polaris-db}
      - documentIndex: 1
        equal: {path: metadata.name, value: keycloak-db}
  - it: polaris bootstrap job reads CNPG app secret and root credential secret
    template: polaris-bootstrap.yaml
    asserts:
      - equal: {path: spec.template.spec.containers[0].image, value: apache/polaris-admin-tool:1.7.0}
      - contains: {path: spec.template.spec.containers[0].env, content: {name: QUARKUS_DATASOURCE_JDBC_URL, valueFrom: {secretKeyRef: {name: polaris-db-app, key: jdbc-uri}}}}
      - equal: {path: metadata.annotations["argocd.argoproj.io/sync-wave"], value: "2"}
  - it: minio is absent unless components.minio
    template: minio.yaml
    asserts:
      - hasDocuments: {count: 0}
  - it: minio renders in dev
    template: minio.yaml
    set: {components.minio: true}
    asserts:
      - hasDocuments: {count: 4}
  - it: routes only on openshift
    template: route.yaml
    set: {platform: openshift, route.enabled: true}
    asserts:
      - hasDocuments: {count: 2}
  - it: ingress absent by default
    template: ingress.yaml
    asserts:
      - hasDocuments: {count: 0}
  - it: default-deny networkpolicy present
    template: networkpolicy.yaml
    documentIndex: 0
    asserts:
      - equal: {path: metadata.name, value: default-deny-ingress}
EOF
helm unittest glue 2>&1 | tail -3   # beklenen: HATA
```

- [ ] **Step 2: Şablonlar**

```bash
cat > glue/templates/cnpg.yaml <<'EOF'
apiVersion: postgresql.cnpg.io/v1
kind: Cluster
metadata:
  name: polaris-db
  namespace: {{ include "glue.ns" . }}
  annotations: {argocd.argoproj.io/sync-wave: "0"}
spec:
  instances: {{ .Values.cnpg.polarisDb.instances }}
  storage: {size: {{ .Values.cnpg.polarisDb.storageSize }}}
  bootstrap:
    initdb: {database: polaris, owner: polaris}
---
apiVersion: postgresql.cnpg.io/v1
kind: Cluster
metadata:
  name: keycloak-db
  namespace: {{ include "glue.ns" . }}
  annotations: {argocd.argoproj.io/sync-wave: "0"}
spec:
  instances: {{ .Values.cnpg.keycloakDb.instances }}
  storage: {size: {{ .Values.cnpg.keycloakDb.storageSize }}}
  bootstrap:
    initdb: {database: keycloak, owner: keycloak}
EOF

cat > glue/templates/polaris-bootstrap.yaml <<'EOF'
# Realm + root credential bootstrap (chart bunu yapmaz). Root credential Secret'ı ÖNCEDEN var olmalı
# (keys: clientId, clientSecret). Idempotent: mevcut realm'de no-op. Sync hook: her sync'te yeniden koşar, zararsız.
apiVersion: batch/v1
kind: Job
metadata:
  name: polaris-bootstrap
  namespace: {{ include "glue.ns" . }}
  annotations:
    argocd.argoproj.io/sync-wave: "2"
    argocd.argoproj.io/hook: Sync
    argocd.argoproj.io/hook-delete-policy: BeforeHookCreation
spec:
  backoffLimit: 6
  template:
    spec:
      restartPolicy: OnFailure
      containers:
      - name: bootstrap
        image: apache/polaris-admin-tool:{{ .Values.versions.polarisAdminTool }}
        # Credential env'den geldiği için shell genişletmesi gerekir; imajın java başlatıcısı /opt/jboss/container/java/run/run-java.sh
        # (F0'da doğrudan args ile çalıştı). Yol farklıysa: podman run --rm --entrypoint sh apache/polaris-admin-tool:1.7.0 -c 'ls /opt/jboss/container/java/run'
        command: ["/bin/sh", "-c"]
        args: ["exec /opt/jboss/container/java/run/run-java.sh bootstrap --realm={{ .Values.polaris.realm }} --credential={{ .Values.polaris.realm }},${ROOT_ID},${ROOT_SECRET}"]
        env:
        - {name: POLARIS_PERSISTENCE_TYPE, value: relational-jdbc}
        - {name: QUARKUS_DATASOURCE_JDBC_URL, valueFrom: {secretKeyRef: {name: polaris-db-app, key: jdbc-uri}}}
        - {name: QUARKUS_DATASOURCE_USERNAME, valueFrom: {secretKeyRef: {name: polaris-db-app, key: username}}}
        - {name: QUARKUS_DATASOURCE_PASSWORD, valueFrom: {secretKeyRef: {name: polaris-db-app, key: password}}}
        - {name: ROOT_ID, valueFrom: {secretKeyRef: {name: {{ .Values.polaris.rootCredentialSecret }}, key: clientId}}}
        - {name: ROOT_SECRET, valueFrom: {secretKeyRef: {name: {{ .Values.polaris.rootCredentialSecret }}, key: clientSecret}}}
EOF

cat > glue/templates/minio.yaml <<'EOF'
{{- if .Values.components.minio }}
# DEV-ONLY (kind/PoC): tek replika MinIO. Prod'da müşterinin S3'ü kullanılır; components.minio=false ile render edilmez.
apiVersion: v1
kind: Secret
metadata: {name: minio-creds, namespace: {{ include "glue.ns" . }}}
stringData:
  AWS_ACCESS_KEY_ID: {{ .Values.minio.rootUser }}
  AWS_SECRET_ACCESS_KEY: {{ .Values.minio.rootPassword }}
---
apiVersion: apps/v1
kind: Deployment
metadata: {name: minio, namespace: {{ include "glue.ns" . }}}
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
        - {name: MINIO_ROOT_USER, value: {{ .Values.minio.rootUser }}}
        - {name: MINIO_ROOT_PASSWORD, value: {{ .Values.minio.rootPassword }}}
        ports: [{containerPort: 9000}, {containerPort: 9001}]
        volumeMounts: [{name: data, mountPath: /data}]
      volumes: [{name: data, emptyDir: {}}]
---
apiVersion: v1
kind: Service
metadata: {name: minio, namespace: {{ include "glue.ns" . }}}
spec:
  selector: {app: minio}
  ports: [{name: api, port: 9000}, {name: console, port: 9001}]
---
apiVersion: batch/v1
kind: Job
metadata:
  name: minio-bucket-init
  namespace: {{ include "glue.ns" . }}
  annotations: {argocd.argoproj.io/hook: Sync, argocd.argoproj.io/hook-delete-policy: BeforeHookCreation}
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
          until mc alias set m http://minio.{{ include "glue.ns" . }}.svc:9000 {{ .Values.minio.rootUser }} {{ .Values.minio.rootPassword }}; do sleep 3; done
          {{- range .Values.minio.buckets }}
          mc mb -p m/{{ . }}
          {{- end }}
          mc ls m
{{- end }}
EOF

cat > glue/templates/networkpolicy.yaml <<'EOF'
{{- if .Values.networkPolicy.enabled }}
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata: {name: default-deny-ingress, namespace: {{ include "glue.ns" . }}}
spec:
  podSelector: {}
  policyTypes: [Ingress]
---
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata: {name: allow-same-namespace, namespace: {{ include "glue.ns" . }}}
spec:
  podSelector: {}
  ingress:
  - from: [{podSelector: {}}]
---
# Operatörler (cnpg-system), ArgoCD, ingress/router ve izleme namespace'leri
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata: {name: allow-platform-namespaces, namespace: {{ include "glue.ns" . }}}
spec:
  podSelector: {}
  ingress:
  - from:
    - namespaceSelector: {matchLabels: {kubernetes.io/metadata.name: cnpg-system}}
    - namespaceSelector: {matchLabels: {kubernetes.io/metadata.name: argocd}}
    - namespaceSelector: {matchLabels: {kubernetes.io/metadata.name: {{ ternary "openshift-ingress" "ingress-nginx" (eq (include "glue.isOpenShift" .) "true") }}}}
    - namespaceSelector: {matchLabels: {kubernetes.io/metadata.name: {{ ternary "openshift-monitoring" "monitoring" (eq (include "glue.isOpenShift" .) "true") }}}}
{{- end }}
EOF

cat > glue/templates/route.yaml <<'EOF'
{{- if and (eq (include "glue.isOpenShift" .) "true") .Values.route.enabled }}
apiVersion: route.openshift.io/v1
kind: Route
metadata: {name: keycloak, namespace: {{ include "glue.ns" . }}}
spec:
  host: {{ .Values.keycloak.hostname }}
  to: {kind: Service, name: keycloak-service}
  port: {targetPort: http}
  tls: {termination: edge}
---
apiVersion: route.openshift.io/v1
kind: Route
metadata: {name: polaris, namespace: {{ include "glue.ns" . }}}
spec:
  to: {kind: Service, name: polaris}
  port: {targetPort: 8181}
  tls: {termination: edge}
{{- end }}
EOF

cat > glue/templates/ingress.yaml <<'EOF'
{{- if .Values.ingress.enabled }}
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata: {name: keycloak, namespace: {{ include "glue.ns" . }}}
spec:
  ingressClassName: {{ .Values.ingress.className }}
  rules:
  - host: {{ .Values.keycloak.hostname }}
    http:
      paths:
      - path: /
        pathType: Prefix
        backend: {service: {name: keycloak-service, port: {number: 8080}}}
{{- end }}
EOF
helm unittest glue 2>&1 | tail -4   # beklenen: hepsi PASS
```

- [ ] **Step 3: Commit**

```bash
git add glue && git commit -m "feat(glue): CNPG clusters, Polaris bootstrap job, dev MinIO, NetworkPolicy, Route/Ingress (platform flag)

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 3: Keycloak CR + realm import (v1'den taşıma)

**Files:**
- Create: `glue/templates/keycloak.yaml`, `glue/templates/keycloak-realm.yaml`, `glue/tests/keycloak_test.yaml`

**Interfaces:**
- Consumes: `keycloak-db-app` Secret (Task 2), values `keycloak.*`, `platform`.
- Produces: `Keycloak/keycloak` (operator Service `keycloak-service` 8080 http), `KeycloakRealmImport/<realm>-realm` — realm `lakehouse`; client'lar `trino`, `superset`, `jupyterhub`, `polaris` (confidential, `redirectUris` values'tan); LDAP federasyonu `keycloak.ldap.enabled` ile.

- [ ] **Step 1: v1 realm'ini referans olarak çıkar (silinmiş dalda, git'ten)**

```bash
git show main:chart/templates/10-keycloak.yaml > /tmp/v1-keycloak.yaml
sed -n '155,200p' /tmp/v1-keycloak.yaml     # v1 Keycloak CR (db/hostname/http) — alan adlarını karşılaştır
sed -n '200,651p' /tmp/v1-keycloak.yaml | grep -n -E "clientId|providerId|redirectUris|protocolMappers|groups|roles" | head -40
```
Taşıma kuralı: realm JSON'unun **içeriği** (LDAP mapper'lar, client'lar, grup/rol eşlemeleri) aynen; Helm değişkenleri yeni `values.yaml` anahtarlarına (`keycloak.realm.name`, `keycloak.ldap.*`, `keycloak.clients.<id>.redirectUris`, `keycloak.hostname`) uyarlanır; v1'in `lakehouse.*` helper çağrıları kaldırılır; müşteri adı içeren literal kalmaz (`helm template glue glue | grep -i -E "katip|celebi|kç"` → 0).

- [ ] **Step 2: Test (BAŞARISIZ)**

```bash
cat > glue/tests/keycloak_test.yaml <<'EOF'
suite: keycloak
templates: [keycloak.yaml, keycloak-realm.yaml]
tests:
  - it: Keycloak CR uses CNPG secret and hostname
    template: keycloak.yaml
    asserts:
      - equal: {path: spec.db.vendor, value: postgres}
      - equal: {path: spec.db.usernameSecret.name, value: keycloak-db-app}
      - equal: {path: spec.hostname.hostname, value: keycloak.example.com}
  - it: realm import carries the four platform clients
    template: keycloak-realm.yaml
    asserts:
      - contains: {path: spec.realm.clients, content: {clientId: trino}, any: true}
      - contains: {path: spec.realm.clients, content: {clientId: superset}, any: true}
      - contains: {path: spec.realm.clients, content: {clientId: jupyterhub}, any: true}
      - contains: {path: spec.realm.clients, content: {clientId: polaris}, any: true}
  - it: ldap federation only when enabled
    template: keycloak-realm.yaml
    asserts:
      - isNull: {path: spec.realm.components}
  - it: ldap federation renders when enabled
    template: keycloak-realm.yaml
    set: {keycloak.ldap.enabled: true}
    asserts:
      - isNotNull: {path: spec.realm.components}
EOF
helm unittest glue 2>&1 | tail -3   # beklenen: HATA
```

- [ ] **Step 3: Şablonlar**

```bash
cat > glue/templates/keycloak.yaml <<'EOF'
apiVersion: k8s.keycloak.org/v2alpha1
kind: Keycloak
metadata:
  name: keycloak
  namespace: {{ include "glue.ns" . }}
  annotations: {argocd.argoproj.io/sync-wave: "1"}
spec:
  instances: 1
  db:
    vendor: postgres
    host: keycloak-db-rw
    port: 5432
    database: keycloak
    usernameSecret: {name: keycloak-db-app, key: username}
    passwordSecret: {name: keycloak-db-app, key: password}
  hostname:
    hostname: {{ .Values.keycloak.hostname }}
    strict: {{ eq (include "glue.isOpenShift" .) "true" }}
  http:
    httpEnabled: true            # TLS kenarda (Route edge / Ingress); kind'da düz HTTP
  proxy: {headers: xforwarded}
  bootstrapAdmin:
    user: {secret: {{ .Values.keycloak.adminSecret }}}   # keys: username, password
EOF
```
`glue/templates/keycloak-realm.yaml`: `/tmp/v1-keycloak.yaml` satır ~203'ten sona kadar olan `KeycloakRealmImport` bloğunu kopyala ve uyarla:
- `metadata.name: {{ printf "%s-realm" .Values.keycloak.realm.name }}`, `metadata.namespace: {{ include "glue.ns" . }}`, `annotations: {argocd.argoproj.io/sync-wave: "2"}`, `spec.keycloakCRName: keycloak`, `spec.realm.realm: {{ .Values.keycloak.realm.name }}`, `enabled: true`.
- LDAP `components` bloğu `{{- if .Values.keycloak.ldap.enabled }} … {{- end }}` içinde; `connectionUrl/usersDn/bindDn/bindCredential` değerleri `.Values.keycloak.ldap.*`'dan.
- `clients`: v1'deki her client tanımı korunur; `redirectUris` → `{{ toJson (index .Values.keycloak.clients "<id>").redirectUris }}`; client secret'ları realm import'ta düz metin istemez (operator import'u secret'sız client yaratır; F4'te `keycloak.clients.<id>.secret` values'tan verilecek).
Doğrulama:
```bash
helm unittest glue 2>&1 | tail -3      # hepsi PASS
helm template glue glue | grep -c -i -E "katip|celebi|kç"   # beklenen: 0
```

- [ ] **Step 4: Commit**

```bash
git add glue && git commit -m "feat(glue): Keycloak CR + realm import ported from v1 (LDAP federation gated, platform clients)

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 4: ArgoCD app-of-apps (`platform/`) + values + Polaris setup YAML

**Files:**
- Create: `platform/root-app.yaml`, `platform/apps/00-strimzi.yaml`, `platform/apps/00-cnpg.yaml`, `platform/apps/00-keycloak-operator.yaml`, `platform/apps/10-glue.yaml`, `platform/apps/20-polaris.yaml`, `platform/keycloak-operator/kustomization.yaml`, `platform/values/glue.yaml`, `platform/values/glue-dev.yaml`, `platform/values/polaris.yaml`, `platform/values/polaris-dev.yaml`, `platform/envs/dev/kustomization.yaml`, `platform/envs/prod/kustomization.yaml`, `platform/polaris/setup.yaml`

**Interfaces:**
- Consumes: `glue` chart (Task 1–3).
- Produces: `kubectl kustomize platform/envs/dev` → 5 `Application` (`strimzi`, `cnpg`, `keycloak-operator`, `glue`, `polaris`; sync-wave 0/0/0/1/2), hepsi repo `https://github.com/suhanduman/lakehouse.git` `targetRevision: v2` (bootstrap `--repo/--revision` ile ezer); `platform/polaris/setup.yaml` (katalog `lakehouse`, namespace'ler `shop_raw/shop/nginx_raw`, roller, principal'lar `connect/spark/trino/notebooks`).

- [ ] **Step 1: Application'lar**

```bash
mkdir -p platform/apps platform/values platform/envs/dev platform/envs/prod platform/keycloak-operator platform/polaris
cat > platform/root-app.yaml <<'EOF'
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata: {name: lakehouse-root, namespace: argocd, finalizers: [resources-finalizer.argocd.argoproj.io]}
spec:
  project: default
  source:
    repoURL: https://github.com/suhanduman/lakehouse.git
    targetRevision: v2
    path: platform/envs/dev        # bootstrap.sh --env ile değiştirilir
  destination: {server: https://kubernetes.default.svc, namespace: argocd}
  syncPolicy:
    automated: {prune: true, selfHeal: true}
EOF
cat > platform/apps/00-strimzi.yaml <<'EOF'
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata: {name: strimzi, namespace: argocd, annotations: {argocd.argoproj.io/sync-wave: "0"}}
spec:
  project: default
  source:
    repoURL: quay.io/strimzi-helm
    chart: strimzi-kafka-operator
    targetRevision: 1.2.0
    helm:
      valuesObject:
        watchNamespaces: [lakehouse]
  destination: {server: https://kubernetes.default.svc, namespace: lakehouse}
  syncPolicy:
    automated: {prune: true, selfHeal: true}
    syncOptions: [CreateNamespace=true, ServerSideApply=true]
EOF
cat > platform/apps/00-cnpg.yaml <<'EOF'
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata: {name: cnpg, namespace: argocd, annotations: {argocd.argoproj.io/sync-wave: "0"}}
spec:
  project: default
  source:
    repoURL: https://cloudnative-pg.github.io/charts
    chart: cloudnative-pg
    targetRevision: 0.29.0
  destination: {server: https://kubernetes.default.svc, namespace: cnpg-system}
  syncPolicy:
    automated: {prune: true, selfHeal: true}
    syncOptions: [CreateNamespace=true, ServerSideApply=true]
EOF
cat > platform/keycloak-operator/kustomization.yaml <<'EOF'
# Keycloak operatörünün resmi kubectl manifestleri (Helm chart yok); kustomize uzak kaynak = config, imaj değil.
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
namespace: lakehouse
resources:
- https://raw.githubusercontent.com/keycloak/keycloak-k8s-resources/26.7.3/kubernetes/keycloaks.k8s.keycloak.org-v1.yml
- https://raw.githubusercontent.com/keycloak/keycloak-k8s-resources/26.7.3/kubernetes/keycloakrealmimports.k8s.keycloak.org-v1.yml
- https://raw.githubusercontent.com/keycloak/keycloak-k8s-resources/26.7.3/kubernetes/kubernetes.yml
EOF
cat > platform/apps/00-keycloak-operator.yaml <<'EOF'
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata: {name: keycloak-operator, namespace: argocd, annotations: {argocd.argoproj.io/sync-wave: "0"}}
spec:
  project: default
  source:
    repoURL: https://github.com/suhanduman/lakehouse.git
    targetRevision: v2
    path: platform/keycloak-operator
  destination: {server: https://kubernetes.default.svc, namespace: lakehouse}
  syncPolicy:
    automated: {prune: true, selfHeal: true}
    syncOptions: [CreateNamespace=true, ServerSideApply=true]
EOF
cat > platform/apps/10-glue.yaml <<'EOF'
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata: {name: glue, namespace: argocd, annotations: {argocd.argoproj.io/sync-wave: "1"}}
spec:
  project: default
  source:
    repoURL: https://github.com/suhanduman/lakehouse.git
    targetRevision: v2
    path: glue
    helm:
      valueFiles: [../platform/values/glue.yaml]
  destination: {server: https://kubernetes.default.svc, namespace: lakehouse}
  syncPolicy:
    automated: {prune: true, selfHeal: true}
    syncOptions: [CreateNamespace=true, ServerSideApply=true, RespectIgnoreDifferences=true]
  ignoreDifferences:
  - group: kafka.strimzi.io
    kind: KafkaConnect
    jsonPointers: [/spec/build/output/image]   # Strimzi digest ile yeniden yazar
EOF
cat > platform/apps/20-polaris.yaml <<'EOF'
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata: {name: polaris, namespace: argocd, annotations: {argocd.argoproj.io/sync-wave: "2"}}
spec:
  project: default
  sources:
  - repoURL: https://downloads.apache.org/polaris/helm-chart
    chart: polaris
    targetRevision: 1.7.0
    helm:
      valueFiles: [$values/platform/values/polaris.yaml]
  - repoURL: https://github.com/suhanduman/lakehouse.git
    targetRevision: v2
    ref: values
  destination: {server: https://kubernetes.default.svc, namespace: lakehouse}
  syncPolicy:
    automated: {prune: true, selfHeal: true}
    syncOptions: [CreateNamespace=true, ServerSideApply=true]
EOF
```

- [ ] **Step 2: Values (prod varsayılan + dev) ve env kustomizasyonları**

```bash
cat > platform/values/glue.yaml <<'EOF'
# prod varsayılanları (glue/values.yaml üstüne): müşteri S3'ü, 3 broker, Route (openshift) — hostname/S3 kurulumda doldurulur
platform: openshift
route: {enabled: true}
keycloak:
  hostname: keycloak.lakehouse.example.com
EOF
cat > platform/values/glue-dev.yaml <<'EOF'
# kind / lokal geliştirme
platform: vanilla
kafka:
  replicas: 1
  storageSize: 5Gi
  plainListener: true
  config:
    default.replication.factor: 1
    min.insync.replicas: 1
    offsets.topic.replication.factor: 1
    transaction.state.log.replication.factor: 1
    transaction.state.log.min.isr: 1
connect:
  buildImage: ttl.sh/lakehouse-connect-dev:24h
cnpg:
  polarisDb: {instances: 1, storageSize: 2Gi}
  keycloakDb: {instances: 1, storageSize: 2Gi}
components: {minio: true}
networkPolicy: {enabled: false}      # kind'ın varsayılan CNI'si NetworkPolicy enforce etmez; prod'da true
keycloak:
  hostname: keycloak.127.0.0.1.nip.io
sources:
- {name: shop, topicPrefix: shop}
EOF
cat > platform/values/polaris.yaml <<'EOF'
image: {tag: "1.7.0"}
persistence:
  type: relational-jdbc
  relationalJdbc:
    secret: {name: polaris-db-app, username: username, password: password, jdbcUrl: jdbc-uri}
realmContext: {realms: [POLARIS]}
extraEnv:
- {name: AWS_REGION, value: us-east-1}
# STS'siz S3 (sts_unavailable) için sunucunun kendi anahtarları (F0 S1c): kurulumda s3-creds Secret'ı (AWS_ACCESS_KEY_ID/SECRET)
- {name: AWS_ACCESS_KEY_ID, valueFrom: {secretKeyRef: {name: s3-creds, key: AWS_ACCESS_KEY_ID}}}
- {name: AWS_SECRET_ACCESS_KEY, valueFrom: {secretKeyRef: {name: s3-creds, key: AWS_SECRET_ACCESS_KEY}}}
storage:
  secret: {name: s3-creds, awsAccessKeyId: AWS_ACCESS_KEY_ID, awsSecretAccessKey: AWS_SECRET_ACCESS_KEY}
EOF
cat > platform/values/polaris-dev.yaml <<'EOF'
image: {tag: "1.7.0"}
persistence:
  type: relational-jdbc
  relationalJdbc:
    secret: {name: polaris-db-app, username: username, password: password, jdbcUrl: jdbc-uri}
realmContext: {realms: [POLARIS]}
extraEnv:
- {name: AWS_REGION, value: us-east-1}
- {name: AWS_ACCESS_KEY_ID, valueFrom: {secretKeyRef: {name: minio-creds, key: AWS_ACCESS_KEY_ID}}}
- {name: AWS_SECRET_ACCESS_KEY, valueFrom: {secretKeyRef: {name: minio-creds, key: AWS_SECRET_ACCESS_KEY}}}
storage:
  secret: {name: minio-creds, awsAccessKeyId: AWS_ACCESS_KEY_ID, awsSecretAccessKey: AWS_SECRET_ACCESS_KEY}
EOF
cat > platform/envs/dev/kustomization.yaml <<'EOF'
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
- ../../apps/00-strimzi.yaml
- ../../apps/00-cnpg.yaml
- ../../apps/00-keycloak-operator.yaml
- ../../apps/10-glue.yaml
- ../../apps/20-polaris.yaml
patches:
- target: {kind: Application, name: glue}
  patch: |-
    - op: replace
      path: /spec/source/helm/valueFiles
      value: [../platform/values/glue-dev.yaml]
- target: {kind: Application, name: polaris}
  patch: |-
    - op: replace
      path: /spec/sources/0/helm/valueFiles
      value: [$values/platform/values/polaris-dev.yaml]
EOF
cat > platform/envs/prod/kustomization.yaml <<'EOF'
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
- ../../apps/00-strimzi.yaml
- ../../apps/00-cnpg.yaml
- ../../apps/00-keycloak-operator.yaml
- ../../apps/10-glue.yaml
- ../../apps/20-polaris.yaml
EOF
cat > platform/polaris/setup.yaml <<'EOF'
# `polaris setup apply` (apache-polaris CLI 1.7.0) — katalog/namespace/rol/grant/principal tek YAML. Şema = `setup export`.
# S3 modu: STS varsa (MinIO/AWS) bu hâliyle; STS yoksa `sts_unavailable: true` ekle (F0 S1c) ve istemcilerde delegation header'ı kapat.
principal_roles: [writers, readers]
principals:
  connect:   {type: service, roles: [writers]}
  spark:     {type: service, roles: [writers]}
  trino:     {type: service, roles: [readers]}
  notebooks: {type: service, roles: [readers]}
catalogs:
  - name: lakehouse
    type: internal
    storage_type: s3
    default_base_location: s3://lakehouse/
    allowed_locations: [s3://lakehouse/]
    region: us-east-1
    endpoint: http://minio.lakehouse.svc:9000            # prod: müşteri S3 endpoint'i (kurulumda değiştir)
    endpoint_internal: http://minio.lakehouse.svc:9000
    path_style_access: true
    roles:
      lakehouse_admin:
        assign_to: [writers]
        privileges: {catalog: [CATALOG_MANAGE_CONTENT]}
      lakehouse_read:
        assign_to: [readers]
        privileges: {catalog: [CATALOG_READ_PROPERTIES, NAMESPACE_LIST, TABLE_LIST, TABLE_READ_PROPERTIES, TABLE_READ_DATA, VIEW_LIST, VIEW_READ_PROPERTIES]}
    namespaces:
      - name: shop_raw
      - name: shop
      - name: nginx_raw
EOF
kubectl kustomize platform/envs/dev | grep -E "^  name:|sync-wave|valueFiles|glue-dev|polaris-dev" | head -20
# beklenen: 5 Application, glue -> glue-dev.yaml, polaris -> polaris-dev.yaml
```

- [ ] **Step 3: Commit**

```bash
git add platform && git commit -m "feat(platform): ArgoCD app-of-apps (strimzi, cnpg, keycloak-operator, glue, polaris) with dev/prod kustomize overlays and declarative Polaris setup

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 5: `bootstrap/bootstrap.sh` + `runbooks/scripts/polaris-setup.sh` + `runbooks/install.md`

**Files:**
- Create: `bootstrap/bootstrap.sh`, `runbooks/scripts/polaris-setup.sh`, `runbooks/install.md`

**Interfaces:**
- Produces: `bootstrap/bootstrap.sh --env dev|prod [--repo URL] [--revision REF] [--mode argocd|helm]` → ArgoCD `v3.5.2` kurulu, `lakehouse-root` + 5 alt Application uygulanmış (mode helm: ArgoCD'siz, aynı chart'lar `helm upgrade --install` ile — lokal hızlı döngü); `polaris-setup.sh [--setup FILE] [--ns NS]` → port-forward + `polaris setup apply` + Secret'lar `polaris-connect`, `polaris-spark`, `polaris-trino`, `polaris-notebooks` (key `credential` = `id:secret`) namespace `lakehouse`.

- [ ] **Step 1: bootstrap.sh**

```bash
mkdir -p bootstrap runbooks/scripts
cat > bootstrap/bootstrap.sh <<'EOF'
#!/usr/bin/env bash
# Lakehouse v2 bootstrap: ArgoCD'yi kur ve app-of-apps kök Application'ını uygula. Idempotent.
#   bootstrap/bootstrap.sh --env dev|prod [--repo URL] [--revision REF] [--mode argocd|helm]
# --mode helm : ArgoCD'siz lokal döngü (aynı chart'lar helm ile; ArgoCD yolu CI'da doğrulanır)
set -euo pipefail
ARGOCD_VERSION=v3.5.2
ENV=dev; REPO=https://github.com/suhanduman/lakehouse.git; REVISION=v2; MODE=argocd
while [[ $# -gt 0 ]]; do case "$1" in
  --env) ENV="$2"; shift 2;; --repo) REPO="$2"; shift 2;; --revision) REVISION="$2"; shift 2;; --mode) MODE="$2"; shift 2;;
  *) echo "bilinmeyen argüman: $1"; exit 2;; esac; done
ROOT="$(cd "$(dirname "$0")/.." && pwd)"

if [[ "$MODE" == "helm" ]]; then
  helm upgrade --install strimzi oci://quay.io/strimzi-helm/strimzi-kafka-operator --version 1.2.0 -n lakehouse --create-namespace --set watchNamespaces="{lakehouse}" --wait
  helm repo add cnpg https://cloudnative-pg.github.io/charts >/dev/null 2>&1 || true; helm repo update cnpg >/dev/null
  helm upgrade --install cnpg cnpg/cloudnative-pg --version 0.29.0 -n cnpg-system --create-namespace --wait
  kubectl apply -k "$ROOT/platform/keycloak-operator"
  helm upgrade --install glue "$ROOT/glue" -n lakehouse -f "$ROOT/platform/values/glue-${ENV}.yaml" --wait --timeout 25m
  kubectl -n lakehouse wait --for=condition=Ready cluster/polaris-db --timeout=600s
  kubectl -n lakehouse wait --for=condition=complete job/polaris-bootstrap --timeout=600s
  helm repo add polaris https://downloads.apache.org/polaris/helm-chart >/dev/null 2>&1 || true; helm repo update polaris >/dev/null
  helm upgrade --install polaris polaris/polaris --version 1.7.0 -n lakehouse -f "$ROOT/platform/values/polaris-${ENV}.yaml" --wait --timeout 10m
  echo "OK: helm modunda kuruldu (env=$ENV)"; exit 0
fi

kubectl create ns argocd --dry-run=client -o yaml | kubectl apply -f - >/dev/null
kubectl apply -n argocd --server-side -f "https://raw.githubusercontent.com/argoproj/argo-cd/${ARGOCD_VERSION}/manifests/install.yaml"
kubectl -n argocd rollout status deploy/argocd-server --timeout=300s
kubectl -n argocd rollout status deploy/argocd-repo-server --timeout=300s
kubectl -n argocd rollout status statefulset/argocd-application-controller --timeout=300s
# Kök Application: env yolu + repo/revizyon (fork/PR için). Alt Application'lar da aynı repo/revizyona baksın diye
# kustomize çıktısı bir kez doğrudan uygulanır; sonraki senkronları kök Application yönetir.
sed -e "s#path: platform/envs/dev#path: platform/envs/${ENV}#" \
    -e "s#repoURL: https://github.com/suhanduman/lakehouse.git#repoURL: ${REPO}#" \
    -e "s#targetRevision: v2#targetRevision: ${REVISION}#" "$ROOT/platform/root-app.yaml" | kubectl apply -f -
kubectl kustomize "$ROOT/platform/envs/${ENV}" \
  | sed -e "s#repoURL: https://github.com/suhanduman/lakehouse.git#repoURL: ${REPO}#" -e "s#targetRevision: v2#targetRevision: ${REVISION}#" \
  | kubectl apply -f -
echo "OK: ArgoCD ${ARGOCD_VERSION} + lakehouse-root (env=${ENV}, repo=${REPO}@${REVISION}) uygulandı"
echo "İzle: kubectl -n argocd get applications"
EOF
chmod +x bootstrap/bootstrap.sh; bash -n bootstrap/bootstrap.sh && echo syntax-ok
```

- [ ] **Step 2: polaris-setup.sh (F0 `20-polaris.sh` mantığı, credential'lar Secret'a)**

```bash
cat > runbooks/scripts/polaris-setup.sh <<'EOF'
#!/usr/bin/env bash
# Polaris katalog/rol/principal kurulumu (kurulumdan sonra bir kez; idempotent).
#   runbooks/scripts/polaris-setup.sh [--setup platform/polaris/setup.yaml] [--ns lakehouse]
# Gereksinim: pip install apache-polaris ; root credential Secret'ı (polaris-root: clientId/clientSecret) kümede.
set -euo pipefail
SETUP=platform/polaris/setup.yaml; NS=lakehouse
while [[ $# -gt 0 ]]; do case "$1" in --setup) SETUP="$2"; shift 2;; --ns) NS="$2"; shift 2;; *) echo "bilinmeyen argüman: $1"; exit 2;; esac; done
command -v polaris >/dev/null || { echo "polaris CLI yok: pip install apache-polaris"; exit 1; }
CLIENT_ID=$(kubectl -n "$NS" get secret polaris-root -o jsonpath='{.data.clientId}' | base64 -d)
CLIENT_SECRET=$(kubectl -n "$NS" get secret polaris-root -o jsonpath='{.data.clientSecret}' | base64 -d)
export CLIENT_ID CLIENT_SECRET
kubectl -n "$NS" port-forward svc/polaris 8181:8181 >/dev/null 2>&1 & PF=$!; trap 'kill $PF 2>/dev/null' EXIT; sleep 3
LOG=$(mktemp)
polaris setup apply "$SETUP" 2>&1 | tee "$LOG"
# setup apply yeni principal'lar için {"clientId","clientSecret"} basar (yaratma sırasıyla); root rotate EDEMEZ -> hemen Secret'a yaz
python3 - "$LOG" "$NS" <<'PY'
import sys, re, json, subprocess
log = open(sys.argv[1]).read(); ns = sys.argv[2]
names = re.findall(r"Creating principal: ([A-Za-z0-9_-]+)", log)
creds = [json.loads(l) for l in log.splitlines() if l.startswith('{"clientId"')]
assert len(names) == len(creds), f"principal/credential sayısı uyuşmuyor: {names} vs {len(creds)}"
for n, c in zip(names, creds):
    manifest = subprocess.run(["kubectl", "-n", ns, "create", "secret", "generic", f"polaris-{n}",
                               f"--from-literal=credential={c['clientId']}:{c['clientSecret']}", "--dry-run=client", "-o", "yaml"],
                              check=True, capture_output=True, text=True).stdout
    subprocess.run(["kubectl", "apply", "-f", "-"], input=manifest, check=True, text=True)
    print("Secret yazıldı:", f"polaris-{n}")
if not names: print("Yeni principal yok (idempotent çalıştırma).")
PY
EOF
chmod +x runbooks/scripts/polaris-setup.sh; bash -n runbooks/scripts/polaris-setup.sh && echo syntax-ok
```

- [ ] **Step 3: runbooks/install.md**

```bash
cat > runbooks/install.md <<'EOF'
# Kurulum (F1 kapsamı: platform + Polaris)

## Ön koşullar
- Kubernetes ≥ 1.33 (OpenShift veya vanilla), `kubectl`, `helm` ≥ 3.14, internet (Maven Central, Docker Hub, quay.io, downloads.apache.org, GitHub).
- S3 uyumlu depolama + bucket (`lakehouse`). Kind/dev için `components.minio=true` küme içi MinIO kurar.
- Secret'lar (Git'e girmez; kurulumdan önce `lakehouse` ns'inde): `polaris-root` (`clientId`, `clientSecret`), `keycloak-admin` (`username`, `password`), prod'da `s3-creds` (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`).
- Kaynak DB kimlik bilgileri (F2): `<source>-db` Secret'ları (`username`, `password`).

## Adımlar
1. `platform/values/glue.yaml` ve `polaris.yaml`'ı ortama göre düzenle (hostname, S3 endpoint, `platform`, `route`); `platform/polaris/setup.yaml`'da S3 endpoint'i.
2. `bootstrap/bootstrap.sh --env prod` (ArgoCD `v3.5.2` kurar, kök + alt Application'ları uygular). İzle: `kubectl -n argocd get applications` → hepsi `Synced/Healthy`. Connect imaj build'i ~10 dk.
3. Polaris kataloğu: `pip install apache-polaris` → `runbooks/scripts/polaris-setup.sh` (katalog `lakehouse`, namespace'ler, roller, principal'lar; `polaris-connect/-spark/-trino/-notebooks` Secret'ları yazılır).
   - S3'te STS yoksa `setup.yaml`'da `sts_unavailable: true`; istemcilerde vending kapalı (Connect `iceberg.catalog.header.X-Iceberg-Access-Delegation=none` + `s3.*` anahtarları; Trino `iceberg.rest-catalog.vended-credentials-enabled=false`; Spark `header.X-Iceberg-Access-Delegation=none`).
4. Doğrulama: `test/e2e/polaris-smoke/job.yaml` (küme içi pyiceberg yaz/oku) — `test/e2e/run.sh` aynı adımları otomatik yapar.

## Lokal geliştirme (kind + Podman/Docker)
`test/e2e/kind.sh && bootstrap/bootstrap.sh --env dev --mode helm` (yerel chart, ArgoCD'siz) veya `--mode argocd --revision <dal>` (ArgoCD GitHub'dan çeker → değişiklikler push'lu olmalı).
Podman: kind ≥ 0.33 (Podman 6 uyumu); `kind.sh` düğüm pids limitini yükseltir (Spark için).

## Sorun giderme
- Connect `Build` uzun/başarısız: `kubectl -n lakehouse logs -l strimzi.io/kind=KafkaConnect --tail=100`.
- Connector task FAILED / Bronze boş: `kubectl -n lakehouse get kafkaconnector <ad> -o jsonpath='{.status.connectorStatus.tasks[0].trace}'`; task başarısızlığından sonra consumer konumu ileri kalabilir → `spec.state: stopped` → `kafka-consumer-groups.sh --group connect-<sink> --reset-offsets --to-earliest --execute` → `running`.
- Polaris health `:8182/q/health`, API `:8181`; bootstrap Job log'u `kubectl -n lakehouse logs job/polaris-bootstrap`.
EOF
```

- [ ] **Step 4: Commit**

```bash
git add bootstrap runbooks && git commit -m "feat(bootstrap): ArgoCD v3.5.2 app-of-apps bootstrap (argocd|helm modes), Polaris setup script, install runbook

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 6: e2e — kind script, run.sh, Polaris smoke Job, GitHub Actions

**Files:**
- Create: `test/e2e/kind.sh`, `test/e2e/run.sh`, `test/e2e/polaris-smoke/smoke.py`, `test/e2e/polaris-smoke/job.yaml`, `.github/workflows/e2e.yaml`
- Modify: `README.md` ("Şu an" satırı → F1)

**Interfaces:**
- Consumes: Task 4–5.
- Produces: `test/e2e/run.sh [--mode argocd|helm] [--repo URL] [--revision REF]` → exit 0 = tüm Application'lar Healthy + Kafka/Connect Ready + Polaris smoke OK; CI iş akışı `e2e` PR ve `v2` push'unda.

- [ ] **Step 1: kind.sh (F0 `00-cluster.sh`'ten; MinIO glue'da)**

```bash
mkdir -p test/e2e/polaris-smoke
cat > test/e2e/kind.sh <<'EOF'
#!/usr/bin/env bash
# kind cluster (Podman veya Docker). Idempotent. Podman'da Spark için pids limiti yükseltilir (F0 S0d).
set -euo pipefail
CLUSTER="${KIND_CLUSTER:-lakehouse}"
PROVIDER="${KIND_EXPERIMENTAL_PROVIDER:-}"
if ! kind get clusters 2>/dev/null | grep -qx "$CLUSTER"; then kind create cluster --name "$CLUSTER" --wait 120s; fi
kubectl config use-context "kind-$CLUSTER" >/dev/null
if [[ "$PROVIDER" == "podman" ]]; then
  podman update --pids-limit 8192 "${CLUSTER}-control-plane" >/dev/null 2>&1 || true
  podman exec "${CLUSTER}-control-plane" sh -c 'mkdir -p /etc/systemd/system.conf.d && printf "[Manager]\nDefaultTasksMax=infinity\n" > /etc/systemd/system.conf.d/tasksmax.conf && systemctl daemon-reexec' >/dev/null 2>&1 || true
fi
kubectl create ns lakehouse --dry-run=client -o yaml | kubectl apply -f - >/dev/null
# dev Secret'ları (Git'e girmez; kind için sentetik)
kubectl -n lakehouse create secret generic polaris-root --from-literal=clientId=root --from-literal=clientSecret=s3cr3t --dry-run=client -o yaml | kubectl apply -f - >/dev/null
kubectl -n lakehouse create secret generic keycloak-admin --from-literal=username=admin --from-literal=password=admin --dry-run=client -o yaml | kubectl apply -f - >/dev/null
echo "OK: kind-$CLUSTER hazır"
EOF
chmod +x test/e2e/kind.sh
```

- [ ] **Step 2: Polaris smoke (F0 `smoke.py` + Job) ve run.sh**

```bash
git show HEAD:test/spike/20-polaris/smoke.py > test/e2e/polaris-smoke/smoke.py
cat > test/e2e/polaris-smoke/job.yaml <<'EOF'
apiVersion: batch/v1
kind: Job
metadata: {name: polaris-smoke, namespace: lakehouse}
spec:
  backoffLimit: 0
  ttlSecondsAfterFinished: 600
  template:
    spec:
      restartPolicy: Never
      containers:
      - name: smoke
        image: python:3.13-slim
        command: ["/bin/sh","-c"]
        args: ["set -e; pip install -q 'pyiceberg[s3fs,pyarrow]>=0.10,<0.11'; python /work/smoke.py \"$CLIENT_ID\" \"$CLIENT_SECRET\""]
        env:
        - {name: POLARIS_URI, value: http://polaris.lakehouse.svc:8181/api/catalog}
        - {name: S3_ENDPOINT, value: http://minio.lakehouse.svc:9000}
        - {name: CLIENT_ID, valueFrom: {secretKeyRef: {name: polaris-smoke-cred, key: CLIENT_ID}}}
        - {name: CLIENT_SECRET, valueFrom: {secretKeyRef: {name: polaris-smoke-cred, key: CLIENT_SECRET}}}
        volumeMounts: [{name: work, mountPath: /work}]
      volumes:
      - name: work
        configMap: {name: polaris-smoke}
EOF
cat > test/e2e/run.sh <<'EOF'
#!/usr/bin/env bash
# e2e (F1): kind -> bootstrap -> Application'lar Healthy -> polaris-setup -> smoke Job. CI ve lokal aynı.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"; MODE=argocd; REVISION="${REVISION:-v2}"; REPO="${REPO:-https://github.com/suhanduman/lakehouse.git}"
while [[ $# -gt 0 ]]; do case "$1" in --mode) MODE="$2"; shift 2;; --revision) REVISION="$2"; shift 2;; --repo) REPO="$2"; shift 2;; *) echo "bilinmeyen argüman: $1"; exit 2;; esac; done
"$ROOT/test/e2e/kind.sh"
"$ROOT/bootstrap/bootstrap.sh" --env dev --mode "$MODE" --repo "$REPO" --revision "$REVISION"
if [[ "$MODE" == "argocd" ]]; then
  for app in strimzi cnpg keycloak-operator glue polaris; do
    echo "bekleniyor: application/$app"
    kubectl -n argocd wait application/"$app" --for=jsonpath='{.status.health.status}'=Healthy --timeout=1800s
    kubectl -n argocd wait application/"$app" --for=jsonpath='{.status.sync.status}'=Synced --timeout=300s
  done
fi
kubectl -n lakehouse wait kafka/lakehouse --for=condition=Ready --timeout=900s
kubectl -n lakehouse wait kafkaconnect/connect --for=condition=Ready --timeout=1800s
kubectl -n lakehouse rollout status deploy/polaris --timeout=600s
python3 -m venv "$ROOT/.venv" >/dev/null 2>&1 || true; "$ROOT/.venv/bin/pip" install -q apache-polaris
PATH="$ROOT/.venv/bin:$PATH" "$ROOT/runbooks/scripts/polaris-setup.sh" --setup "$ROOT/platform/polaris/setup.yaml"
# smoke: connect principal'ının credential'ıyla küme içinden yaz/oku
CRED=$(kubectl -n lakehouse get secret polaris-connect -o jsonpath='{.data.credential}' | base64 -d)
kubectl -n lakehouse create secret generic polaris-smoke-cred --from-literal=CLIENT_ID="${CRED%%:*}" --from-literal=CLIENT_SECRET="${CRED#*:}" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
kubectl -n lakehouse create configmap polaris-smoke --from-file="$ROOT/test/e2e/polaris-smoke/smoke.py" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
kubectl -n lakehouse delete job polaris-smoke --ignore-not-found >/dev/null
kubectl apply -f "$ROOT/test/e2e/polaris-smoke/job.yaml"
kubectl -n lakehouse wait --for=condition=complete job/polaris-smoke --timeout=600s || { kubectl -n lakehouse logs job/polaris-smoke --tail=30; exit 1; }
kubectl -n lakehouse logs job/polaris-smoke | grep "^OK"
echo "E2E F1 OK"
EOF
chmod +x test/e2e/run.sh; bash -n test/e2e/run.sh && echo syntax-ok
```

- [ ] **Step 3: GitHub Actions**

```bash
mkdir -p .github/workflows
cat > .github/workflows/e2e.yaml <<'EOF'
name: e2e
on:
  push: {branches: [v2]}
  pull_request: {branches: [v2]}
jobs:
  helm-unittest:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: azure/setup-helm@v4
      - run: helm plugin install https://github.com/helm-unittest/helm-unittest && helm unittest glue
  e2e-kind:
    runs-on: ubuntu-latest
    timeout-minutes: 60
    steps:
      - uses: actions/checkout@v4
      - uses: azure/setup-helm@v4
      - uses: helm/kind-action@v1
        with: {version: v0.33.0, cluster_name: lakehouse, install_only: true}
      - name: e2e (ArgoCD, bu commit)
        run: KIND_CLUSTER=lakehouse test/e2e/run.sh --mode argocd --repo "https://github.com/${{ github.repository }}.git" --revision "${{ github.event.pull_request.head.sha || github.sha }}"
      - name: teşhis (başarısızlıkta)
        if: failure()
        run: |
          kubectl -n argocd get applications -o wide || true
          kubectl -n lakehouse get pods -o wide || true
          kubectl -n lakehouse get kafkaconnect,kafkaconnector,cluster,keycloak || true
          kubectl -n lakehouse describe kafkaconnect connect | tail -40 || true
EOF
sed -i '' 's#- Şu an: F0 spike.*#- Şu an: F1 platform (bootstrap + ArgoCD app-of-apps + glue) — plan `docs/plans/2026-09-11-lakehouse-v2-f1-platform.md`; F0 bulguları `docs/plans/2026-09-10-f0-findings.md`#' README.md
```

- [ ] **Step 4: Lokal e2e (helm modu) — gerçek kanıt**

```bash
test/spike/90-teardown.sh || true      # F0 kümesi kaynak tüketiyor; F1 kümesi 'lakehouse'
KIND_EXPERIMENTAL_PROVIDER=podman KIND_CLUSTER=lakehouse test/e2e/run.sh --mode helm 2>&1 | tail -15
# beklenen son satır: E2E F1 OK   (ilk çalıştırma ~25 dk: Connect build ~10 dk + Keycloak + Polaris)
```
Başarısızlıkta durma noktaları: (a) admin-tool Job komutu (Task 2 notu), (b) Keycloak CR alanları (operator 26.7 API) — `kubectl -n lakehouse describe keycloak keycloak`, (c) Polaris chart Secret anahtar adları. Düzelt → aynı komut (idempotent).

- [ ] **Step 5: Commit + push → CI (ArgoCD modu) yeşil**

```bash
git add test/e2e .github README.md && git commit -m "test(e2e): kind + bootstrap + Application health + Polaris smoke; GitHub Actions e2e (ArgoCD mode)

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
git push
gh run watch --exit-status "$(gh run list --workflow e2e --branch v2 --limit 1 --json databaseId -q '.[0].databaseId')"
```

---

### Task 7: F1 kapanışı — spike dizinini emekliye ayır, bulgu/plan güncelle

**Files:**
- Delete: `test/spike/` (içerik `glue/`, `platform/`, `test/e2e/` içine taşındı; F2 için gereken sink/Spark YAML'ları F0 bulgularından ve git geçmişinden alınır)
- Modify: `docs/plans/2026-09-10-f0-findings.md` (F1 notu)

- [ ] **Step 1: Kaldır, notu yaz, commit + push**

```bash
git rm -r -q test/spike
python3 - <<'EOF'
p='docs/plans/2026-09-10-f0-findings.md'; s=open(p).read()
s+="\n\n## F1 notu\nSpike script'leri `glue/`, `platform/`, `test/e2e/` olarak ürünleşti; `test/spike/` kaldırıldı (git geçmişinde, son hâli commit 90d1202). F1 e2e'de yeniden doğrulananlar / değişenler bu bölüme yazılır (admin-tool Job komutu, Keycloak 26.7 CR alanları, ArgoCD health/sync-wave davranışı, Connect build süresi).\n"
open(p,'w').write(s)
EOF
git add -A && git commit -m "chore(v2): retire F0 spike scripts (productized into glue/platform/e2e), add F1 note

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
git push
```

---

## Self-review

**Spec coverage:** §4 repo düzeni → bootstrap/platform/glue/runbooks/test/e2e (Task 1–6; `pipelines/`, `jobs/` F2). §5.1 Kafka (tls/scram, ACL'ler chart'ta, `KubernetesSecretConfigProvider`, spec.build listesi) → Task 1. §7 Polaris (chart + bootstrap + setup apply, iki S3 modu) → Task 2/4/5; Keycloak realm → Task 3. §8 NetworkPolicy default-deny → Task 2 (kind'da kapalı, prod açık). §9 test (helm-unittest az + kind e2e gerçek + CI) → Task 1–3 testleri, Task 6. §13 F1 kapsamı tam; Trino/Superset/Jupyter/Zeppelin (F4), Spark/ingestion (F2), nginx/mongo (F3), DR/izleme (F5) bilinçli dışarıda.

**Placeholder taraması:** Task 3 realm import v1'den mekanik taşıma — kaynak (`git show main:chart/templates/10-keycloak.yaml`) ve dönüşüm kuralları verildi; 450 satır JSON plana gömülmedi. Task 2 admin-tool komut yolu için doğrulama komutu verildi. TBD/TODO yok.

**Tutarlılık:** Secret adları (`polaris-db-app`, `keycloak-db-app`, `polaris-root`, `keycloak-admin`, `minio-creds`, `s3-creds`, `polaris-<principal>` key `credential`, `polaris-smoke-cred`) Task 2/4/5/6 boyunca aynı; namespace `lakehouse` her yerde; Application adları `strimzi cnpg keycloak-operator glue polaris` Task 4 ↔ Task 6 `run.sh`; values dosya adları `glue-dev.yaml`/`polaris-dev.yaml` Task 4 kustomize patch ↔ Task 5 helm modu; `sources[].topicPrefix` Task 1 ACL ↔ Task 4 dev values; Keycloak Service adı `keycloak-service` Task 2 (Route/Ingress) ↔ operator varsayılanı.
