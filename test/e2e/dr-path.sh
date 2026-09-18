#!/usr/bin/env bash
# e2e F5 DR yolu: (1) CNPG Barman Cloud eklentisi — ScheduledBackup immediate -> Backup completed -> ayrı kümeye
# geri yükleme -> psql sayımı; (2) Velero: namespace yedeği (node-agent fs-backup) -> işaret ConfigMap'i restore.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"; NS=lakehouse; VELERO_NS="${VELERO_NS:-velero}"

echo "== CNPG sürekli WAL arşivi"
# Bu dosyadaki TEK biçim: yoklama döngüsü SESSİZDİR (`2>/dev/null || true` — nesne/koşul henüz olmayabilir),
# hemen ardından gelen iddia YÜKSEK SESLİDİR (kubectl hatası + conditions dökümü + exit 1). Aynı kalıp
# CNPG Backup (satır ~21) ve Velero backup/restore yoklamalarında da kullanılıyor.
for _ in $(seq 1 60); do
  ok=1; for c in polaris-db keycloak-db superset-db; do
    [[ "$(kubectl -n "$NS" get cluster "$c" -o jsonpath='{.status.conditions[?(@.type=="ContinuousArchiving")].status}' 2>/dev/null || true)" == "True" ]] || ok=""
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

echo "== Velero yedek"
# glue-DIŞI işaret nesnesi: ArgoCD selfHeal glue'nun ürettiği bir ConfigMap'i siler silmez geri koyardı ->
# restore kanıtı yarışa girerdi. Etiket e2e=dr-marker: Restore'un labelSelector'ı (Velero'da ad filtresi YOK).
kubectl apply -f - >/dev/null <<EOF
apiVersion: v1
kind: ConfigMap
metadata: {name: e2e-dr-marker, namespace: $NS, labels: {e2e: dr-marker}}
data: {k: v}
EOF
# TUZAK 1: `backup` kısa adı CNPG'nin postgresql.cnpg.io/Backup'ına çözülür (canlı) -> her yerde tam nitelikli ad.
# TUZAK 2: `kubectl delete backups.velero.io` nesneyi S3'ten SİLMEZ; aynı adla yeni yedek
# "backup already exists in object storage" ile Failed olur (canlı) -> DeleteBackupRequest ile sil ve kaybolmasını bekle.
if kubectl -n "$VELERO_NS" get backups.velero.io e2e-lakehouse >/dev/null 2>&1; then
  kubectl apply -f - >/dev/null <<EOF
apiVersion: velero.io/v1
kind: DeleteBackupRequest
metadata: {name: e2e-lakehouse-del, namespace: $VELERO_NS}
spec: {backupName: e2e-lakehouse}
EOF
  for _ in $(seq 1 36); do kubectl -n "$VELERO_NS" get backups.velero.io e2e-lakehouse >/dev/null 2>&1 || break; sleep 5; done
  kubectl -n "$VELERO_NS" get backups.velero.io e2e-lakehouse >/dev/null 2>&1 && { echo "HATA eski e2e-lakehouse yedeği silinemedi"; exit 1; }
  kubectl -n "$VELERO_NS" delete deletebackuprequests.velero.io e2e-lakehouse-del --ignore-not-found >/dev/null
fi
# Dışlama annotation'ları pod'lara gerçekten indi mi (glue kafka.yaml / cnpg.yaml inheritedMetadata)
for pod_vol in "lakehouse-dual-role-0=data-0" "polaris-db-1=pgdata"; do
  a=$(kubectl -n "$NS" get pod "${pod_vol%%=*}" -o jsonpath='{.metadata.annotations.backup\.velero\.io/backup-volumes-excludes}' 2>/dev/null)
  [[ "$a" == "${pod_vol##*=}" ]] || { echo "HATA ${pod_vol%%=*} dışlama annotation'ı '${a:-yok}' (beklenen ${pod_vol##*=})"; exit 1; }
done; echo "OK Kafka/CNPG pod'larında fs-backup dışlama annotation'ı var"
kubectl -n "$VELERO_NS" apply -f "$ROOT/test/e2e/velero-backup.yaml"
p=""; for _ in $(seq 1 60); do p=$(kubectl -n "$VELERO_NS" get backups.velero.io e2e-lakehouse -o jsonpath='{.status.phase}' 2>/dev/null || true); [[ "$p" =~ ^(Completed|PartiallyFailed|Failed|FailedValidation)$ ]] && break; sleep 10; done
[[ "$p" == "Completed" ]] || { kubectl -n "$VELERO_NS" describe backups.velero.io e2e-lakehouse | tail -40; echo "HATA Velero backup ${p:-yok}"; exit 1; }; echo "OK Velero backup Completed"
# .spec.volume = POD hacim adı (PVC adı değil). kind NOTU: local-path PV'leri hostPath'tir ve Velero fs-backup
# hostPath hacimleri ATLAR ("is a hostPath volume ... skipping", canlı) -> kind'da yalnız emptyDir hacimleri
# (strimzi-tmp, scratch-data, shm, plugins, temp-dir …) PodVolumeBackup üretir; zeppelin-data/hub-db-dir PVC'leri
# ÜRETMEZ. Gerçek CSI depolamada (OpenShift) PVC'ler de alınır — runbooks/dr.md.
pv=$(kubectl -n "$VELERO_NS" get podvolumebackups.velero.io -l velero.io/backup-name=e2e-lakehouse -o jsonpath='{range .items[*]}{.spec.volume}={.status.phase} {end}'); echo "PodVolumeBackups: $pv"
grep -q 'Completed' <<<"$pv" || { echo "HATA node-agent fs-backup hiç koşmadı (emptyDir hacimleri bekleniyor)"; exit 1; }; echo "OK fs-backup (node-agent, Kopia) çalıştı"
if grep -qE '(^| )(pgdata|data-0)=' <<<"$pv"; then echo "HATA Kafka/CNPG hacmi yedeğe girdi"; exit 1; fi; echo "OK Kafka/CNPG hacimleri dışlandı"

echo "== Velero restore provası (e2e-dr-marker)"
kubectl -n "$NS" delete configmap e2e-dr-marker
kubectl -n "$VELERO_NS" delete restores.velero.io e2e-marker --ignore-not-found >/dev/null
kubectl -n "$VELERO_NS" apply -f "$ROOT/test/e2e/velero-restore.yaml"
r=""; for _ in $(seq 1 30); do r=$(kubectl -n "$VELERO_NS" get restores.velero.io e2e-marker -o jsonpath='{.status.phase}' 2>/dev/null || true); [[ "$r" =~ ^(Completed|PartiallyFailed|Failed|FailedValidation)$ ]] && break; sleep 5; done
[[ "$r" == "Completed" ]] || { kubectl -n "$VELERO_NS" describe restores.velero.io e2e-marker | tail -30; echo "HATA restore ${r:-yok}"; exit 1; }
kubectl -n "$NS" get configmap e2e-dr-marker -o name >/dev/null || { echo "HATA marker geri gelmedi"; exit 1; }; echo "OK Velero restore: e2e-dr-marker geri geldi"
kubectl -n "$NS" delete configmap e2e-dr-marker --ignore-not-found >/dev/null
echo "E2E F5 DR OK"
