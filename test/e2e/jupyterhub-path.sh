#!/usr/bin/env bash
# e2e F4 JupyterHub yolu: hub health -> e2e servisi ile kullanıcı yarat + sunucu başlat (gerçek pyspark-notebook imajı + postStart pip)
# -> pod içinde pyiceberg (Polaris notebooks principal) + trino (TLS) -> sunucuyu durdur, kullanıcıyı sil.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"; NS=lakehouse; TOK=e2e-dev-token-0123456789abcdef0123456789abcdef

# --- YALNIZ ArgoCD modunda: proxy token'ının re-sync'te SABİT kaldığını doğrula ---
# Kusur (CI 35451232616): z2jh 4.4.2 `hub.config.ConfigurableHTTPProxy.auth_token`'ı values'ta pinlenmemişse
# HER `helm template` çağrısında yeniden üretir (chart'ın `lookup` kaçışı ArgoCD yolunda çalışmaz). Re-render
# `hub` Secret'ını yeni token'la yazınca pod'lardan yalnız biri dönüyor, hub ile proxy farklı token'da kalıyor
# ve hub "api_request to proxy failed: HTTP 403: Forbidden" veriyordu (singleuser pod'u hiç yaratılmıyordu).
# Düzeltme platform/apps/30-jupyterhub.yaml: ignoreDifferences + RespectIgnoreDifferences=true.
# Buradaki HARD REFRESH manifest önbelleğini atlar, yani chart'ı GERÇEKTEN yeniden render ettirir — düzeltme
# olmasaydı token değişirdi. İki tur koşulur ki tek turluk bir rastlantı "düzeldi" sanılmasın.
# Helm modunda (bootstrap --mode helm) ArgoCD yoktur: orada chart'ın `lookup`u değeri zaten korur -> atlanır.
JH_TOKEN_PATH='{.data.hub\.config\.ConfigurableHTTPProxy\.auth_token}'
if kubectl -n argocd get application jupyterhub >/dev/null 2>&1; then
  TOK_BEFORE=$(kubectl -n "$NS" get secret hub -o jsonpath="$JH_TOKEN_PATH")
  [[ -n "$TOK_BEFORE" ]] || { echo "HATA: hub Secret'ında auth_token yok"; exit 1; }
  PODS_BEFORE=$(kubectl -n "$NS" get pod -l app.kubernetes.io/name=jupyterhub -o name | sort | tr '\n' ' ')
  for round in 1 2; do
    kubectl -n argocd annotate application jupyterhub argocd.argoproj.io/refresh=hard --overwrite >/dev/null
    settled=0; sync=""; health=""
    for _ in $(seq 1 60); do      # 60 x 5s = 300s üst sınır
      # ArgoCD refresh'i bitirince annotation'ı KENDİSİ siler; sync/health ancak ondan sonra anlamlıdır.
      ann=$(kubectl -n argocd get application jupyterhub -o jsonpath='{.metadata.annotations.argocd\.argoproj\.io/refresh}' 2>/dev/null || true)
      sync=$(kubectl -n argocd get application jupyterhub -o jsonpath='{.status.sync.status}' 2>/dev/null || true)
      health=$(kubectl -n argocd get application jupyterhub -o jsonpath='{.status.health.status}' 2>/dev/null || true)
      [[ -z "$ann" && "$sync" == "Synced" && "$health" == "Healthy" ]] && { settled=1; break; }
      sleep 5
    done
    [[ "$settled" == 1 ]] || { echo "HATA: jupyterhub Application $round. hard refresh sonrası 300s'de Synced/Healthy olmadı (sync=$sync health=$health)"; exit 1; }
  done
  TOK_AFTER=$(kubectl -n "$NS" get secret hub -o jsonpath="$JH_TOKEN_PATH")
  PODS_AFTER=$(kubectl -n "$NS" get pod -l app.kubernetes.io/name=jupyterhub -o name | sort | tr '\n' ' ')
  [[ "$TOK_BEFORE" == "$TOK_AFTER" ]] || {
    echo "HATA: hub Secret'ındaki proxy token'ı iki hard refresh sonrasında DEĞİŞTİ -> hub/proxy 403 kusuru geri geldi"
    echo "      (platform/apps/30-jupyterhub.yaml: ignoreDifferences ya da RespectIgnoreDifferences=true kaybolmuş olabilir)"; exit 1; }
  echo "OK jupyterhub hub Secret auth_token re-sync sonrası değişmedi"
  if [[ "$PODS_BEFORE" == "$PODS_AFTER" ]]; then echo "   pod'lar dönmedi: $PODS_BEFORE"
  else echo "   pod'lar döndü ama token aynı kaldı -> ÖNCE: $PODS_BEFORE SONRA: $PODS_AFTER"; fi
else
  echo "ArgoCD yok, atlandı: hub Secret auth_token re-sync denetimi (helm modu; chart'ın lookup'ı değeri korur)"
fi

kubectl -n "$NS" rollout status deploy/hub --timeout=600s; kubectl -n "$NS" rollout status deploy/proxy --timeout=600s
kubectl -n "$NS" port-forward svc/proxy-public 18080:80 >/dev/null 2>&1 & PF=$!; trap 'kill $PF 2>/dev/null' EXIT; sleep 3
H() { curl -sS -H "Authorization: token $TOK" "$@"; }
[[ "$(curl -s -o /dev/null -w '%{http_code}' localhost:18080/hub/health)" == "200" ]] || { echo "HATA hub health"; exit 1; }; echo "OK hub health"
# Hub REST kodları doğrulanır: kullanıcı yarat 201 (yeni) ya da 409 (taze kümede olmayan ama tekrar koşuda kalan
# kullanıcı — idempotent), sunucu başlat 201 (hazır) ya da 202 (beklemede), sunucu durdur 204 ya da 202. Eskiden
# kod yalnız yazdırılıyordu -> 403 gibi hatalar sessizce geçip koşu ilerideki belirsiz bir adımda düşüyordu.
HC() { local what="$1" allowed="$2"; shift 2; local code; code=$(H "$@" -o /dev/null -w '%{http_code}'); echo "$what $code"
  [[ ",$allowed," == *",$code,"* ]] || { echo "HATA jupyterhub $what: beklenmeyen HTTP $code"; exit 1; }; }
HC 'user create'  201,409 -X POST localhost:18080/hub/api/users/e2e
HC 'server start' 201,202 -X POST localhost:18080/hub/api/users/e2e/server
for _ in $(seq 1 90); do [[ "$(H localhost:18080/hub/api/users/e2e | jq -r '.servers[""].ready')" == "true" ]] && break; sleep 10; done
[[ "$(H localhost:18080/hub/api/users/e2e | jq -r '.servers[""].ready')" == "true" ]] || { kubectl -n "$NS" describe pod jupyter-e2e | tail -30; kubectl -n "$NS" logs deploy/hub --tail=40; exit 1; }
echo "OK jupyter-e2e ready (imaj + postStart pip)"
E2E_PW=$(kubectl -n "$NS" get secret trino-service-accounts -o jsonpath='{.data.e2e}' | base64 -d)
kubectl -n "$NS" exec jupyter-e2e -- python -c "
import os, trino
from pyiceberg.catalog import load_catalog
c = load_catalog('lakehouse', type='rest', uri=os.environ['POLARIS_URI'], warehouse='lakehouse', credential=os.environ['POLARIS_CREDENTIAL'], scope='PRINCIPAL_ROLE:ALL')
assert ('shop',) in c.list_namespaces(), c.list_namespaces(); print('OK pyiceberg -> Polaris (notebooks principal)')
conn = trino.dbapi.connect(host=os.environ['TRINO_HOST'], port=8443, http_scheme='https', verify='/etc/lakehouse-ca/tls.crt', auth=trino.auth.BasicAuthentication('e2e', '$E2E_PW'), catalog='lakehouse')
cur = conn.cursor(); cur.execute('select count(*) from shop.orders'); assert cur.fetchone()[0] == 3; print('OK trino client (TLS) shop.orders == 3')"
HC 'server stop' 202,204 -X DELETE localhost:18080/hub/api/users/e2e/server
for _ in $(seq 1 30); do [[ "$(H localhost:18080/hub/api/users/e2e | jq -r '.servers | length')" == "0" ]] && break; sleep 5; done
# artık ASSERT EDİLİR: token sahibi servis "e2e-admin" oldu (jupyterhub-dev.yaml), hedef kullanıcı "e2e" —
# Hub'ın "kendini silme" reddi (eski 400 Cannot delete yourself) devreye girmez (F5 fix, F4 notu kapandı).
HC 'user delete' 204 -X DELETE localhost:18080/hub/api/users/e2e
echo "E2E F4 JUPYTERHUB OK"
