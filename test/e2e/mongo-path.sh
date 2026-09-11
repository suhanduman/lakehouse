#!/usr/bin/env bash
# e2e F3 mongo yolu: fixture -> dbz-crm Ready -> mongo-bronze (Bronze 3 I) -> silver-merge (Silver 3) -> update/delete/insert -> tekrar.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"; NS=lakehouse
# shellcheck source=/dev/null
source "$ROOT/test/e2e/lib.sh"
MONGO() { kubectl -n "$NS" exec deploy/demo-mongo -- mongosh --quiet -u dbz -p dbz --authenticationDatabase admin --eval "$1"; }

echo "== mongo fixture"
kubectl apply -f "$ROOT/test/e2e/mongo-fixture.yaml" >/dev/null
kubectl -n "$NS" wait --for=condition=complete job/demo-mongo-init --timeout=600s
echo "== dbz-crm"
kubectl -n "$NS" wait kafkaconnector/dbz-crm --for=condition=Ready --timeout=600s
for _ in $(seq 1 60); do kubectl -n "$NS" exec lakehouse-dual-role-0 -c kafka -- bin/kafka-topics.sh --bootstrap-server localhost:9092 --list 2>/dev/null | grep -qx "crm.crm.customers" && break; sleep 5; done
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
  sum=$(kubectl -n "$NS" exec lakehouse-dual-role-0 -c kafka -- bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic crm.crm.customers 2>/dev/null | awk -F: '{s+=$3} END {print s+0}')
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
