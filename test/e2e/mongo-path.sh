#!/usr/bin/env bash
# e2e F3 mongo yolu: fixture -> dbz-crm Ready -> mongo-bronze (Bronze 3 I) -> silver-merge (Silver 3) -> update/delete/insert -> tekrar.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"; NS=lakehouse
# shellcheck source=/dev/null
source "$ROOT/test/e2e/lib.sh"
MONGO() { kubectl -n "$NS" exec deploy/demo-mongo -- mongosh --quiet -u dbz -p dbz --authenticationDatabase admin --eval "$1"; }

echo "== mongo fixture"
# E2E_MONGO_IMAGE (yalnız CI): MongoDB 8.x çekirdek 6.19+ üzerinde açılmayı reddeder (SERVER-121912) ve GitHub
# runner'ları bu çekirdekte; CI 7.0 ile koşar. Boşsa fixture olduğu gibi (8.0) uygulanır — OpenShift kabulü 8.0'ı sınar.
sed -E "${E2E_MONGO_IMAGE:+s#image: .*mongo:8\.0#image: $E2E_MONGO_IMAGE#}" "$ROOT/test/e2e/mongo-fixture.yaml" | kubectl apply -f - >/dev/null
kubectl -n "$NS" wait --for=condition=complete job/demo-mongo-init --timeout=600s || {
  # Zaman aşımında nedeni görünür kıl (CI 42492979613 yalnız "timed out" basıyordu): imaj çekimi (Docker Hub
  # sınırı), izin (OpenShift UID) ya da replica set başlatma?
  kubectl -n "$NS" get pods -l app=demo-mongo -o wide; kubectl -n "$NS" describe pod -l app=demo-mongo | tail -25
  kubectl -n "$NS" logs deploy/demo-mongo --tail=30 2>&1 || true; kubectl -n "$NS" logs job/demo-mongo-init --tail=30 2>&1 || true
  exit 1; }
echo "== dbz-crm"
# Kafka araçları scripts/kafka-cli.sh ile (kafka-admin, TLS 9093): üretimde localhost:9092 (plain) dinleyicisi yoktur
kubectl -n "$NS" wait kafkaconnector/dbz-crm --for=condition=Ready --timeout=600s
for _ in $(seq 1 60); do "$ROOT/scripts/kafka-cli.sh" kafka-topics.sh --list 2>/dev/null | grep -qx "crm.crm.customers" && break; sleep 5; done
echo "== mongo-bronze #1"
run_spark_once mongo-bronze
verify crm_raw.customers 3 --exact --wait 60 '_id=000000000000000000000001' '~_doc=Ada'
echo "== silver-merge (crm)"
run_spark_once silver-merge
verify crm.customers 3 --exact --wait 60 '_id=000000000000000000000002' '~_doc=Ankara'
echo "== mongo update/delete/insert"
# shellcheck disable=SC2016  # $set mongosh operatörü, kabuk değişkeni değil
MONGO 'const c = db.getSiblingDB("crm").customers; c.updateOne({_id: ObjectId("000000000000000000000001")}, {$set: {tier: "platinum", tags: ["x"]}}); c.deleteOne({_id: ObjectId("000000000000000000000002")}); c.insertOne({_id: ObjectId("000000000000000000000004"), name: "Deniz", nested: {deep: {v: 1}}});' >/dev/null
# sabit sleep yerine bariyer: 3 mutasyon (U/D/I) topic'e düşene kadar end-offset toplamını yokla (F3-K)
sum=0
for _ in $(seq 1 60); do
  sum=$("$ROOT/scripts/kafka-cli.sh" kafka-get-offsets.sh --topic crm.crm.customers 2>/dev/null | awk -F: '{s+=$3} END {print s+0}')
  [[ "$sum" -ge 6 ]] && break
  sleep 2
done
[[ "$sum" -ge 6 ]] || { echo "crm.crm.customers end-offset toplamı 120 s içinde 6'ya ulaşmadı (görülen: $sum)"; exit 1; }
echo "== mongo-bronze #2"
run_spark_once mongo-bronze
verify crm_raw.customers 6 --exact --wait 60 '~_doc=platinum' '_id=000000000000000000000004'
echo "== silver-merge (crm) #2"
run_spark_once silver-merge
verify crm.customers 3 --exact --wait 60 '_id=000000000000000000000001:~_doc=platinum' '!_id=000000000000000000000002' '_id=000000000000000000000004'
echo "== karantina boş"
verify crm_raw.customers__quarantine 0 --exact
echo "E2E F3 MONGO OK"
