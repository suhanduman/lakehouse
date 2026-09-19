#!/usr/bin/env bash
# e2e F5 izleme yolu: Prometheus hedefleri + metrik adları + PrometheusRule health/alarm + Grafana dashboard'ları.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"; NS=lakehouse; MON="${MON_NS:-monitoring}"
# E2E_EXPECT_NO_FIRING=1 (varsayılan): "0 firing Lakehouse alarmı" iddiasını uygular. Bu makinede F4 gecesinden
# kalan FAILED cron koşuları (maint-compact/maint-expire-orphan-ttl) LakehouseSparkScheduledRunFailed'i
# GERÇEKTEN ateşleyebilir — bu, kural adının/metriklerin doğru çalıştığının POZİTİF kanıtıdır, bug değil.
# Assert'i sessizce zayıflatmak yerine belgelenmiş bir değişkenle atlıyoruz; Task 7'nin taze kümesinde varsayılan
# açık kalır ve gerçek anlamda "0 firing" doğrular.
E2E_EXPECT_NO_FIRING="${E2E_EXPECT_NO_FIRING:-1}"
# İzleme yığınının NESNE ADLARI değişkenlerle ezilebilir: varsayılanlar kube-prometheus-stack (dev/CI) adlarıdır,
# yani CI davranışı DEĞİŞMEZ. OpenShift user-workload monitoring'de adlar başkadır ve Grafana platform tarafındadır:
#   PROM_STS=prometheus-user-workload PROM_SVC=prometheus-user-workload GRAFANA_SKIP=1 \
#     scripts/acceptance.sh --mon-ns openshift-user-workload-monitoring
# GRAFANA_SKIP=1 yalnız Grafana dashboard iddialarını atlar; diğer tüm iddialar yüksek sesle koşmaya devam eder.
PROM_STS="${PROM_STS:-prometheus-monitoring-kube-prometheus-prometheus}"
PROM_SVC="${PROM_SVC:-monitoring-kube-prometheus-prometheus}"
GRAFANA_SVC="${GRAFANA_SVC:-monitoring-grafana}"
GRAFANA_SECRET="${GRAFANA_SECRET:-monitoring-grafana}"
GRAFANA_SKIP="${GRAFANA_SKIP:-0}"
kubectl -n "$MON" rollout status "statefulset/$PROM_STS" --timeout=600s
PF2=""
kubectl -n "$MON" port-forward "svc/$PROM_SVC" 19090:9090 >/dev/null 2>&1 & PF=$!; trap 'kill $PF $PF2 2>/dev/null' EXIT; sleep 3
Q() { curl -sS "localhost:19090/api/v1/query" --data-urlencode "query=$1" | jq -r '.data.result | length'; }
UP() { local job="$1" n; for _ in $(seq 1 30); do n=$(Q "up{job=~\"$job\"} == 1"); [[ "$n" -ge 1 ]] && { echo "OK up $job ($n)"; return; }; sleep 10; done; echo "HATA up $job"; exit 1; }
# Trino/hub/zeppelin BİLEREK YOK: izleme kapsamı "boru hattı sağlığı"na daraltıldı (Task 2b, kullanıcı kararı
# 2026-09-18) — gösterim bileşenlerinin uygulama metriği toplanmaz (ServiceMonitor'lar kaldırıldı).
UP ".*kafka-resources-metrics.*"
# kafka-exporter AYNI PodMonitor'e (kafka-resources-metrics) düşüyor (job adı aynı, canlı doğrulandı) — burada
# consumer lag serisinin GERÇEKTEN üretildiğini (yalnız hedef "up" değil) aşağıdaki M() ile ayrıca kanıtlıyoruz.
UP ".*spark-operator.*"
# superset YOK: Superset 6.1.0'da /metrics uç noktası yoktur (glue/templates/superset.yaml notu) — hedef açılsaydı KALICI down olurdu
UP ".*kube-state-metrics.*"
# Polaris yönetim portu (8182 /q/metrics): chart'ın kendi ServiceMonitor'ü (platform/values/polaris.yaml
# serviceMonitor.enabled) -> hedef job adı mgmt Service'inden gelir (polaris-mgmt).
UP ".*polaris.*"

# kube-state-metrics CustomResourceState metrikleri ancak İLGİLİ CR VARSA üretilir. lib.sh'in run_spark_once'ı
# ScheduledSparkApplication'ı askıya alır (status.lastRun HİÇ yazılmaz) ve ürettiği SparkApplication'ı siler
# -> taze kümede her iki metrik ailesi de BOŞ olur. Bu yüzden e2e'ye ait, dakikalık zamanlanmış kendi CR'ımız
# yaratılıyor: tek koşu hem status.lastRun (ssa_last_run) hem de COMPLETED bir SparkApplication (sparkapp_state,
# termination_time) üretir. Şablon en kısa iş: maint-position-deletes (veri bağımlılığı pg-path'te karşılanmış).
# Mevcut CR'ın schedule'ını değiştirmek İŞE YARAMAZ: spark-operator 2.5.2 status.nextRun'ı yeniden hesaplamaz
# (canlı: */1'e çekilen maint-position-deletes 6 dk boyunca koşmadı, nextRun ertesi güne sabit kaldı).
SSA=e2e-monitoring
# PF2 (Grafana port-forward, aşağıda başlatılır) da bu trap'e dahil: değişken EXIT anında okunur (fonksiyon
# çağrısı, string değil) -> henüz atanmamışsa "" (yukarıda tanımlı), atanmışsa gerçek PID kullanılır.
cleanup() { kubectl -n "$NS" delete scheduledsparkapplication "$SSA" --ignore-not-found >/dev/null 2>&1 || true; kill $PF $PF2 2>/dev/null || true; }
trap cleanup EXIT
kubectl -n "$NS" delete scheduledsparkapplication "$SSA" --ignore-not-found --wait=true >/dev/null
kubectl -n "$NS" get scheduledsparkapplication maint-position-deletes -o json \
  | jq --arg n "$SSA" '{apiVersion:"sparkoperator.k8s.io/v1beta2",kind:"ScheduledSparkApplication",metadata:{name:$n,namespace:.metadata.namespace},spec:(.spec|.schedule="*/1 * * * *"|del(.suspend))}' \
  | kubectl apply -f - >/dev/null
RUN=""
for _ in $(seq 1 24); do RUN=$(kubectl -n "$NS" get scheduledsparkapplication "$SSA" -o jsonpath='{.status.lastRunName}'); [[ -n "$RUN" ]] && break; sleep 10; done
[[ -n "$RUN" ]] || { echo "HATA $SSA 4 dk içinde zamanlanmış koşu üretmedi"; kubectl -n "$NS" describe scheduledsparkapplication "$SSA" | tail -20; exit 1; }
# İLK koşu yakalanır yakalanmaz askıya al: aksi hâlde */1 cron'u iddialar sürerken dakikada bir driver+executor
# çifti daha açar (4 vCPU CI düğümünde ~1 CPU / 2,5 GiB her biri) ve successfulRunHistoryLimit üç yeni başarılı
# koşudan sonra iddiaların dayandığı $RUN SparkApplication'ını BUDAR. Tek koşu üç metrik ailesi için de yeter;
# status.lastRun askıya almadan etkilenmez (ssa_last_run kalır).
kubectl -n "$NS" patch scheduledsparkapplication "$SSA" --type=merge -p '{"spec":{"suspend":true}}' >/dev/null
echo "OK zamanlanmış koşu: $RUN (SSA askıya alındı)"
ST=""
for _ in $(seq 1 90); do ST=$(kubectl -n "$NS" get sparkapplication "$RUN" -o jsonpath='{.status.applicationState.state}' 2>/dev/null || true); [[ "$ST" == COMPLETED || "$ST" == FAILED ]] && break; sleep 10; done
[[ "$ST" == COMPLETED ]] || { echo "HATA $RUN durumu: $ST"; kubectl -n "$NS" logs "$RUN-driver" --tail=40 2>/dev/null || true; exit 1; }
echo "OK $RUN COMPLETED"

M() { local q="$1" n; for _ in $(seq 1 12); do n=$(Q "$q"); [[ "$n" -ge 1 ]] && { echo "OK metrik $q ($n seri)"; return; }; sleep 10; done; echo "HATA metrik yok: $q"; exit 1; }
M 'kafka_connect_connector_task_status{status="running"}'
M 'kafka_connect_sink_task_offset_commit_completion_total'
# kafka-exporter (glue kafka.yaml Kafka.spec.kafkaExporter): consumer lag, gerçek sink connector grupları
# (connect-sink-shop/-shop-coord/-nginx/-nginx-coord — canlı doğrulandı, kafka-consumer-groups.sh --list).
# LakehouseSinkStalled kuralının dayandığı seri budur (glue monitoring.yaml).
M 'kafka_consumergroup_lag{consumergroup=~"connect-sink-.*"}'
# StateSet HER durumu (0/1) yayar -> seri varlığı yetmez, değeri 1 olmalı (yüksek sesli)
M "kube_customresource_sparkapp_state{name=\"$RUN\",state=\"COMPLETED\"} == 1"
M "kube_customresource_sparkapp_termination_time{name=\"$RUN\"} > 0"
# RFC3339 -> epoch saniye dönüşümü: değer şimdiki zamana yakın olmalı (1 saatten yeni)
M "time() - kube_customresource_ssa_last_run{name=\"$SSA\"} < 3600"
M 'spark_application_success_count'
# LakehouseSparkRunTooLong'un TEK girdisi (glue/templates/monitoring.yaml): upstream bu aileyi yeniden
# adlandırırsa kural sessizce ölür ve /api/v1/rules yine "health: ok" der (boş vektör hata değildir) ->
# serinin VARLIĞINI ayrıca iddia ediyoruz. _count yeterli: _sum onunla aynı summary'den gelir.
M 'spark_application_success_execution_time_seconds_count'

echo "== kurallar"
n=$(curl -sS localhost:19090/api/v1/rules | jq '[.data.groups[] | select(.name=="lakehouse") | .rules[]] | length'); [[ "$n" == "5" ]] || { echo "HATA kural sayısı $n"; exit 1; }; echo "OK 5 kural yüklü"
for a in LakehouseConnectTaskFailed LakehouseSinkStalled LakehouseSilverMergeStale LakehouseSparkScheduledRunFailed LakehouseSparkRunTooLong; do
  st=$(curl -sS localhost:19090/api/v1/rules | jq -r ".data.groups[].rules[] | select(.name==\"$a\") | .health"); [[ "$st" == "ok" ]] || { echo "HATA $a health=$st"; exit 1; }; echo "OK $a health ok"
done
# Ateşlenen Lakehouse alarmları HER ZAMAN raporlanır (pozitif kanıt olabilir): önce listele, sonra
# E2E_EXPECT_NO_FIRING=1 ise assert et. Bu kümede F4'ten kalan FAILED cron koşuları (maint-compact,
# maint-expire-orphan-ttl) LakehouseSparkScheduledRunFailed'i meşru biçimde ateşleyebilir.
firing_json=$(curl -sS localhost:19090/api/v1/alerts | jq '[.data.alerts[] | select(.labels.alertname | startswith("Lakehouse")) | select(.state=="firing")]')
firing=$(jq 'length' <<<"$firing_json")
echo "== ateşlenen Lakehouse alarmları ($firing)"; jq -c '.[] | {alertname: .labels.alertname, labels, activeAt}' <<<"$firing_json"
if [[ "$E2E_EXPECT_NO_FIRING" == "1" ]]; then
  [[ "$firing" == "0" ]] || { echo "HATA ateşlenen Lakehouse alarmı (E2E_EXPECT_NO_FIRING=1)"; exit 1; }
  echo "OK ateşlenen alarm yok"
else
  echo "UYARI E2E_EXPECT_NO_FIRING=0 -> 'ateşlenen alarm yok' iddiası ATLANDI (belgelenmiş, varsayılan değil)"
fi

echo "== grafana dashboard'ları"
# Yalnız Strimzi (kapsam kontrolör kararıyla daraltıldı: izleme pipeline health odaklı, sunum bileşeni
# olan Trino dashboard'u (20208) kaldırıldı — glue/templates/monitoring.yaml, glue/files/dashboards/README.md).
if [[ "$GRAFANA_SKIP" == "1" ]]; then
  echo "UYARI GRAFANA_SKIP=1 -> Grafana dashboard iddiaları ATLANDI (UWM'de Grafana platform tarafındadır)"
else
  kubectl -n "$MON" port-forward "svc/$GRAFANA_SVC" 13000:80 >/dev/null 2>&1 & PF2=$!; sleep 3
  GP=$(kubectl -n "$MON" get secret "$GRAFANA_SECRET" -o jsonpath='{.data.admin-password}' | base64 -d)
  titles=$(curl -sS -u "admin:$GP" 'localhost:13000/api/search?type=dash-db' | jq -r '.[].title'); kill $PF2 2>/dev/null || true
  for t in "Strimzi Kafka" "Strimzi Kafka Connect"; do grep -qi "$t" <<<"$titles" || { echo "HATA dashboard yok: $t ($titles)"; exit 1; }; echo "OK dashboard $t"; done
fi
echo "E2E F5 MONITORING OK"
