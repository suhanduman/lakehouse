#!/usr/bin/env bash
# e2e F4 JupyterHub yolu: hub health -> e2e servisi ile kullanıcı yarat + sunucu başlat (gerçek pyspark-notebook imajı + postStart pip)
# -> pod içinde pyiceberg (Polaris notebooks principal) + trino (TLS) -> sunucuyu durdur, kullanıcıyı sil.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"; NS=lakehouse; TOK=e2e-dev-token-0123456789abcdef0123456789abcdef
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
