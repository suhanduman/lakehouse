# Kabul testleri (şartname maddesi ↔ kanıt)

Kabul, **kümede koşan kanıtla** yapılır: her madde için bir e2e yolu vardır ve yol kendi iddialarını yüksek
sesle basar (`OK …` satırları, sonunda `E2E … OK`). Orkestratör:

```bash
scripts/acceptance.sh                                                 # kurulu kümede: 9 yol + KABUL özeti
scripts/acceptance.sh --mon-ns monitoring --velero-ns openshift-adp   # OpenShift/OADP
```
Taze kümede (kind + bootstrap dâhil) aynı yolları `test/e2e/run.sh` koşturur; CI kapısı budur
(`.github/workflows/e2e.yaml`).

**Ön koşullar:** kurulum tamam (`runbooks/install.md`), `polaris-setup.sh` koşmuş, glue values'ında demo
kaynaklar açık (`sources`, `pipelines`, `nginx.enabled` — `platform/values/glue-dev.yaml` kalıbı) ve yerelde
`kubectl`, `jq`, `python3`, `polaris` CLI. Script kind/bootstrap **yapmaz**.

## Madde ↔ kanıt tablosu

| Şartname | Gereksinim | Kanıt (yol / komut) | Beklenen |
|---|---|---|---|
| **a, J.2.1, J.3.2** | tümü açık kaynak; sürüm/lisans listesi güncel | `runbooks/versions.md` + `kubectl -n argocd get applications -o custom-columns='APP:.metadata.name,CHART:.spec.source.chart,REV:.spec.source.targetRevision'` | Tablodaki sürümler kümedekilerle birebir; izin verici lisanslar (AGPL/SSPL yalnız DEV-ONLY satırlarında) |
| **A.6** | toplama → omurga → dağıtık motor → Iceberg V2 → katalog → MPP SQL → BI zinciri | `test/e2e/pg-path.sh` → `test/e2e/trino-path.sh` → `test/e2e/superset-path.sh` | `E2E F2 OK`, `E2E F4 TRINO OK`, `E2E F4 SUPERSET OK`; Superset içinden `select count(*) from shop.orders` = 3 |
| **C.2.1(e)** | CDC, **connector framework** biçiminde (MSSQL/PG/Mongo) | `test/e2e/pg-path.sh` (Debezium pg) + `test/e2e/mongo-path.sh` (Debezium mongodb); `kubectl -n lakehouse get kafkaconnector` | `dbz-shop`, `sink-shop`, `dbz-crm` Ready; `E2E F2 OK`, `E2E F3 MONGO OK` (mongodb'de Bronze'u Iceberg sink değil `mongo-bronze` Spark işi yazar — `runbooks/add-source.md`). MSSQL aynı şablondan gelir (`glue/templates/connectors.yaml` → `type: sqlserver`; kurulum `runbooks/add-source.md`) |
| **C.3** | hafif log ajanı doğrudan omurgaya | `test/e2e/nginx-path.sh` (Fluent Bit 5.1.2 → Kafka dış listener → `nginx.access` → sink) | `E2E F3 NGINX OK`; `nginx_raw.access_log` satırları geldi |
| **D(e)** | **MERGE INTO** (upsert + delete) | `test/e2e/pg-path.sh` "MERGE #1/#2/#3" adımları (`run_spark_once silver-merge`) | Kaynakta UPDATE/DELETE sonrası Silver 2 satır (`id=1:status=shipped`, `!id=2`); sonraki INSERT'te 3 satır |
| **D(f)** | compaction / expire / manifest bakımı **deklaratif planlama** | `kubectl -n lakehouse get scheduledsparkapplication` + `test/e2e/pg-path.sh` "bakım" adımı | 5 SSA (`silver-merge`, `mongo-bronze`, `maint-position-deletes`, `maint-compact`, `maint-expire-orphan-ttl`); bakım koşularında `MAINT_OK` (+ compact'te `rewrite_manifests`) |
| **B.3.1** | Iceberg V2, **time travel** | `test/e2e/pg-path.sh` → `verify shop.orders 2 --exact --prev-rows 3` | Silme öncesi snapshot hâlâ okunabilir ve 3 satırlı |
| **H.3.1** | pipeline'lar CronJob/motor zamanlayıcısıyla | `kubectl -n lakehouse get scheduledsparkapplication -o custom-columns='AD:.metadata.name,CRON:.spec.schedule,SUSPEND:.spec.suspend'` | Beş iş cron'lu ve `suspend=false` (kabul koşusundan sonra `runbooks/troubleshooting.md#ssa-suspend`) |
| **H.1.1** | Schema Registry **opsiyonel** | `kubectl -n lakehouse get kafkaconnector dbz-shop -o jsonpath='{.spec.config}'` | JSON converter + `schemas.enable=true`; ayrı registry bileşeni yok |
| **F.1** | **iki farklı** web not defteri, kullanıcı-izole, PyIceberg önyüklü | `test/e2e/jupyterhub-path.sh` + `test/e2e/zeppelin-path.sh` | `E2E F4 JUPYTERHUB OK` (pod içinde pyiceberg → Polaris, trino TLS), `E2E F4 ZEPPELIN OK` (`%jdbc` → `shop.orders` = 3); JupyterHub'da kullanıcı başına PVC |
| **G.1.1, F.3.1** | tüm kullanıcı servisleri AD ile OIDC/LDAPS | `test/e2e/trino-path.sh` (Keycloak Bearer + `rules.json`), `test/e2e/superset-path.sh` (login sayfasında keycloak), `test/e2e/jupyterhub-path.sh`, `test/e2e/zeppelin-path.sh` (Shiro/AD rol kapısı) | `analyst1` 3 satır; `student1` 2 satır + `remote` maskeli; rolsüz kullanıcı Zeppelin'de 401. Gerçek tarayıcı OIDC akışı pre-ship OpenShift'te (`runbooks/user-facing.md`) |
| **G.5** | Prometheus (+ Loki) | `test/e2e/monitoring-path.sh` | `E2E F5 MONITORING OK`: hedefler `up`, `kafka_consumergroup_lag` serileri, **5** PrometheusRule `health: ok`, 0 ateşlenen alarm, 2 Grafana dashboard. Loki platformdadır → `runbooks/troubleshooting.md#loki` |
| **G.6** | Velero yedeği + katalog DB WAL/PITR | `test/e2e/dr-path.sh` | `E2E F5 DR OK`: 3 DB `ContinuousArchiving=True`, ≥3 `Backup completed`, ayrı kümeye restore + tablo sayımı, Velero backup (node-agent fs-backup) + işaret ConfigMap restore'u |
| **e, f, A.4** | OCI imaj + CRD yaşam döngüsü; deklaratif YAML/CRD; Operator tercihi | `kubectl -n argocd get applications`; `kubectl -n lakehouse get kafka,kafkaconnect,cluster,keycloak,superset,sparkapplication` | Her bileşen bir CR/Application; özel imaj/Dockerfile yok (Connect imajı Strimzi `spec.build` ile kümede üretilir) |

## Koşu ve çıktı

```text
$ scripts/acceptance.sh
== ön kontrol
== kaynak DB fixture'ları (pg + mongo)
== polaris-setup (idempotent)

=== pg-path.sh
…
E2E F2 OK
…
=== KABUL ÖZETİ ===
E2E F2 OK
E2E F3 MONGO OK
E2E F3 NGINX OK
E2E F4 TRINO OK
E2E F4 SUPERSET OK
E2E F4 JUPYTERHUB OK
E2E F4 ZEPPELIN OK
E2E F5 MONITORING OK
E2E F5 DR OK
KABUL: 9/9 yol geçti (ns=lakehouse, mon-ns=monitoring, velero-ns=velero)
```
Bir yol düşerse script orada durur, çıkış kodu **1** olur ve özet `KABUL BAŞARISIZ: <yol>` der; teşhis için
`runbooks/troubleshooting.md`.

## Süre beklentisi (ölçümler)

| Ortam | Süre | Not |
|---|---|---|
| Taze kind/Podman, kurulum + tüm yollar (`test/e2e/run.sh`) | ~54 dk | İmaj çekimi baskın (`runbooks/troubleshooting.md` → imaj süreleri) |
| CI (GitHub Actions, 4 vCPU, ArgoCD modu) | ~43 dk | `e2e-kind` işi |
| Kurulu kümede yalnız `acceptance.sh` | toplam ~25–30 dk | monitoring yolu ~3 dk (bir dakikalık SSA koşusunu bekler), DR yolu 3,5–4 dk |

## Kabul koşusuna özgü sık durumlar

- **`pg-path.sh` "public.orders 3 satır bekleniyordu" ile düşüyor:** fixture kaynağı önceki koşuda mutasyona
  uğramış. `kubectl -n lakehouse delete cluster/demo-pg` + `kubectl -n lakehouse delete pvc -l cnpg.io/cluster=demo-pg`,
  sonra `acceptance.sh`'i tekrar koşturun (fixture yeniden uygulanır).
- **Kabul koşusundan sonra zamanlanmış işler durur:** `run_spark_once` SSA'ları askıya alır ve geri açmaz →
  `runbooks/troubleshooting.md#ssa-suspend` (gerçek kurulumda kabul sonrası MUTLAKA geri açın).
- **`monitoring-path.sh` "ateşlenen Lakehouse alarmı" ile düşüyor:** kümede eski FAILED `SparkApplication`
  CR'ları vardır; kök nedeni giderip CR'ları silin. Belgelenmiş `E2E_EXPECT_NO_FIRING=0` kaçışı vardır ama
  **kabul koşusunda kullanılmaz** (varsayılan `1`).
- **`dr-path.sh` Velero adımı kind'da:** PVC içerikleri fs-backup'a girmez (hostPath sınırı) — beklenen
  davranıştır, yol yine geçer (`runbooks/dr.md` §5.4). PVC yedeği kanıtı gerçek CSI depolamada alınır.
- **OpenShift:** `--velero-ns openshift-adp` zorunludur; `dr-path.sh`'in yedek/restore manifest'leri
  (`test/e2e/velero-*.yaml`) `metadata.namespace` TAŞIMAZ ve `kubectl -n "$VELERO_NS" apply` ile
  uygulanır, yani bayrak gerçekten etkilidir.

## İzleme yolunun nesne adları (OpenShift UWM)

`monitoring-path.sh` varsayılan olarak kube-prometheus-stack (dev/CI) nesne adlarını kullanır; adlar dört
değişkenle ezilebilir ve varsayılanlar değişmediği için CI davranışı aynıdır: `PROM_STS`
(varsayılan `prometheus-monitoring-kube-prometheus-prometheus`), `PROM_SVC`
(`monitoring-kube-prometheus-prometheus`), `GRAFANA_SVC` ve `GRAFANA_SECRET` (ikisi de `monitoring-grafana`).
OpenShift user-workload monitoring'de Grafana yoktur → `GRAFANA_SKIP=1` yalnız dashboard iddialarını atlar,
hedef/metrik/kural/alarm iddiaları yüksek sesle koşmaya devam eder:
```bash
PROM_STS=prometheus-user-workload PROM_SVC=prometheus-user-workload GRAFANA_SKIP=1 \
  scripts/acceptance.sh --mon-ns openshift-user-workload-monitoring --velero-ns openshift-adp
```
UWM'nin Prometheus servisi yetkilendirme ister; erişilemiyorsa izleme kanıtı thanos-querier üzerinden elle
alınır (`runbooks/preship-openshift.md` §2.2/2.3) — kural/hedef adları aynıdır.
