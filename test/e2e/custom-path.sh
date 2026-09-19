#!/usr/bin/env bash
# e2e CUSTOM yolu (spec C4/K9): custom/ mekanizması — müşteri kendi CR'ını ürün chart'ına dokunmadan,
# GitOps ile ekleyebiliyor mu? ArgoCD (ya da helm modunda `kubectl apply -k`) custom/examples'ı dağıtır;
# bu script örnek Spark uygulamasını koşturup Silver shop.orders'tan sandbox.ornek_rapor'u üretmesini doğrular.
# pg-path.sh'tan SONRA çağrılır (Silver shop.orders gerekir).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"; NS=lakehouse
# shellcheck source=/dev/null
source "$ROOT/test/e2e/lib.sh"

echo "== custom/ GitOps ile geldi mi (ConfigMap + zamanlı SSA)"
kubectl -n "$NS" get configmap ornek-rapor >/dev/null || { echo "HATA: ConfigMap ornek-rapor yok (custom/examples uygulanmadı)"; exit 1; }
kubectl -n "$NS" get scheduledsparkapplication ornek-rapor-zamanli >/dev/null \
  || { echo "HATA: ScheduledSparkApplication ornek-rapor-zamanli yok (custom/examples uygulanmadı)"; exit 1; }
# örnek bilerek askıda (suspend: true): kurulumda kendiliğinden koşmamalı
susp=$(kubectl -n "$NS" get scheduledsparkapplication ornek-rapor-zamanli -o jsonpath='{.spec.suspend}')
[[ "$susp" == "true" ]] || { echo "HATA: ornek-rapor-zamanli askıda değil (suspend=$susp)"; exit 1; }

echo "== tek seferlik örnek (custom/examples/spark-tek-seferlik.yaml)"
# GitOps'a girmez (kustomization.yaml'da yorumlu, gerekçesi orada): elle uygulanır. Önceki koşudan kalan CR
# yeniden koşmaz -> önce sil.
kubectl -n "$NS" delete sparkapplication ornek-rapor --ignore-not-found --wait=true >/dev/null
kubectl apply -f "$ROOT/custom/examples/spark-tek-seferlik.yaml" >/dev/null
st=""
for _ in $(seq 1 120); do            # 20 dk: imaj + Iceberg paketlerinin indirilmesi dahil
  st=$(kubectl -n "$NS" get sparkapplication ornek-rapor -o jsonpath='{.status.applicationState.state}' 2>/dev/null || true)
  [[ "$st" == "COMPLETED" || "$st" == "FAILED" ]] && break; sleep 10
done
if [[ "$st" != "COMPLETED" ]]; then
  echo "HATA: ornek-rapor COMPLETED olmadı (durum: ${st:-yok})"
  kubectl -n "$NS" describe sparkapplication ornek-rapor | tail -30
  kubectl -n "$NS" logs ornek-rapor-driver --tail=40 || true
  exit 1
fi
# log bir DOSYAYA alınır: `kubectl logs | grep -q` kalıbı grep ilk eşleşmede çıkınca kubectl'e SIGPIPE
# gönderir ve `pipefail` yüzünden script başarısız olurdu (repo genelinde bilinen tuzak).
drv=$(mktemp); kubectl -n "$NS" logs ornek-rapor-driver --tail=2000 >"$drv"
grep ORNEK_RAPOR_OK "$drv" || { echo "HATA: driver log'unda ORNEK_RAPOR_OK yok"; tail -40 "$drv"; exit 1; }

echo "== sandbox.ornek_rapor"
# shop.orders'ta en az bir durum (status) vardır -> rapor en az 1 satır; 'new' pg-path fixture'ından gelir
verify sandbox.ornek_rapor 1 'status=new'
echo "E2E CUSTOM OK"
