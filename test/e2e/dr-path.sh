#!/usr/bin/env bash
# e2e F5 DR yolu: (1) CNPG Barman Cloud eklentisi — ScheduledBackup immediate -> Backup completed -> ayrı kümeye
# geri yükleme -> psql sayımı; (2) Velero (Task 4).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"; NS=lakehouse

echo "== CNPG sürekli WAL arşivi"
for _ in $(seq 1 60); do
  ok=1; for c in polaris-db keycloak-db superset-db; do
    [[ "$(kubectl -n "$NS" get cluster "$c" -o jsonpath='{.status.conditions[?(@.type=="ContinuousArchiving")].status}' 2>/dev/null)" == "True" ]] || ok=""
  done; [[ -n "$ok" ]] && break; sleep 10
done
for c in polaris-db keycloak-db superset-db; do
  s=$(kubectl -n "$NS" get cluster "$c" -o jsonpath='{.status.conditions[?(@.type=="ContinuousArchiving")].status}')
  [[ "$s" == "True" ]] || { kubectl -n "$NS" get cluster "$c" -o jsonpath='{.status.conditions}'; echo; echo "HATA $c ContinuousArchiving=$s"; exit 1; }
  echo "OK $c ContinuousArchiving=True"
done

echo "== CNPG yedek"
# Etiket canlı doğrulandı: CNPG, ScheduledBackup'ın ürettiği Backup'a cnpg.io/scheduled-backup=<ad> koyar
b=""; for _ in $(seq 1 60); do b=$(kubectl -n "$NS" get backup -l cnpg.io/scheduled-backup=polaris-db-daily -o jsonpath='{.items[0].status.phase}' 2>/dev/null || true); [[ "$b" == "completed" ]] && break; sleep 10; done
[[ "$b" == "completed" ]] || { kubectl -n "$NS" get backup -o wide; kubectl -n "$NS" describe backup | tail -30; echo "HATA polaris-db yedeği tamamlanmadı (${b:-yok})"; exit 1; }; echo "OK polaris-db Backup completed"
c=$(kubectl -n "$NS" get backup -o jsonpath='{range .items[*]}{.status.phase}{"\n"}{end}' | grep -c completed); [[ "$c" -ge 3 ]] || { kubectl -n "$NS" get backup -o wide; echo "HATA 3 DB yedeği tamamlanmadı ($c)"; exit 1; }; echo "OK $c yedek completed"

echo "== restore provası (polaris-db-restore)"
kubectl -n "$NS" delete cluster polaris-db-restore --ignore-not-found --wait=true >/dev/null
kubectl apply -f "$ROOT/test/e2e/cnpg-restore.yaml"
kubectl -n "$NS" wait --for=condition=Ready cluster/polaris-db-restore --timeout=600s || { kubectl -n "$NS" get pod -l cnpg.io/cluster=polaris-db-restore; kubectl -n "$NS" logs -l cnpg.io/cluster=polaris-db-restore --tail=40 --all-containers; exit 1; }
# Polaris şeması `polaris_schema` (public DEĞİL — canlı doğrulandı): sistem şemaları dışındaki tüm tabloları say
n=$(kubectl -n "$NS" exec polaris-db-restore-1 -c postgres -- psql -U postgres -d polaris -Atc "select count(*) from information_schema.tables where table_schema not in ('pg_catalog','information_schema')")
[[ "$n" -gt 0 ]] || { kubectl -n "$NS" exec polaris-db-restore-1 -c postgres -- psql -U postgres -d polaris -Atc "\dn"; echo "HATA restore DB boş"; exit 1; }; echo "OK restore: polaris veritabanında $n tablo"
kubectl -n "$NS" delete cluster polaris-db-restore --wait=false
echo "E2E F5 DR OK"
