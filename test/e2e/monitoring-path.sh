#!/usr/bin/env bash
# e2e F5 izleme yolu (1/2): Prometheus hedefleri up, metrik adları mevcut. Kurallar/dashboard'lar Task 2'de eklenir.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"; NS=lakehouse; MON="${MON_NS:-monitoring}"
kubectl -n "$MON" rollout status statefulset/prometheus-monitoring-kube-prometheus-prometheus --timeout=600s
kubectl -n "$MON" port-forward svc/monitoring-kube-prometheus-prometheus 19090:9090 >/dev/null 2>&1 & PF=$!; trap 'kill $PF 2>/dev/null' EXIT; sleep 3
Q() { curl -sS "localhost:19090/api/v1/query" --data-urlencode "query=$1" | jq -r '.data.result | length'; }
UP() { local job="$1" n; for _ in $(seq 1 30); do n=$(Q "up{job=~\"$job\"} == 1"); [[ "$n" -ge 1 ]] && { echo "OK up $job ($n)"; return; }; sleep 10; done; echo "HATA up $job"; exit 1; }
UP ".*kafka-resources-metrics.*"
UP ".*spark-operator.*"
UP ".*trino.*"
# superset YOK: Superset 6.1.0'da /metrics uç noktası yoktur (glue/templates/superset.yaml notu) — hedef açılsaydı KALICI down olurdu
UP ".*hub.*"
UP ".*zeppelin.*"
UP ".*kube-state-metrics.*"

# kube-state-metrics CustomResourceState metrikleri ancak İLGİLİ CR VARSA üretilir. lib.sh'in run_spark_once'ı
# ScheduledSparkApplication'ı askıya alır (status.lastRun HİÇ yazılmaz) ve ürettiği SparkApplication'ı siler
# -> taze kümede her iki metrik ailesi de BOŞ olur. Bu yüzden e2e'ye ait, dakikalık zamanlanmış kendi CR'ımız
# yaratılıyor: tek koşu hem status.lastRun (ssa_last_run) hem de COMPLETED bir SparkApplication (sparkapp_state,
# termination_time) üretir. Şablon en kısa iş: maint-position-deletes (veri bağımlılığı pg-path'te karşılanmış).
# Mevcut CR'ın schedule'ını değiştirmek İŞE YARAMAZ: spark-operator 2.5.2 status.nextRun'ı yeniden hesaplamaz
# (canlı: */1'e çekilen maint-position-deletes 6 dk boyunca koşmadı, nextRun ertesi güne sabit kaldı).
SSA=e2e-monitoring
cleanup() { kubectl -n "$NS" delete scheduledsparkapplication "$SSA" --ignore-not-found >/dev/null 2>&1 || true; kill $PF 2>/dev/null || true; }
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
# StateSet HER durumu (0/1) yayar -> seri varlığı yetmez, değeri 1 olmalı (yüksek sesli)
M "kube_customresource_sparkapp_state{name=\"$RUN\",state=\"COMPLETED\"} == 1"
M "kube_customresource_sparkapp_termination_time{name=\"$RUN\"} > 0"
# RFC3339 -> epoch saniye dönüşümü: değer şimdiki zamana yakın olmalı (1 saatten yeni)
M "time() - kube_customresource_ssa_last_run{name=\"$SSA\"} < 3600"
M 'spark_application_success_count'
M 'trino_execution_QueryManager_RunningQueries'
echo "E2E F5 MONITORING OK"
