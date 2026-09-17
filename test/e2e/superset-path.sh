#!/usr/bin/env bash
# e2e F4 Superset yolu: CR Running -> /health -> login sayfasında keycloak -> Trino bağlantısı (legacy-import + test-db + sorgu; TLS + servis hesabı)
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"; NS=lakehouse
echo "== Superset CR"
for _ in $(seq 1 90); do [[ "$(kubectl -n "$NS" get superset superset -o jsonpath='{.status.phase}' 2>/dev/null)" == "Running" ]] && break; sleep 10; done
[[ "$(kubectl -n "$NS" get superset superset -o jsonpath='{.status.phase}')" == "Running" ]] || { kubectl -n "$NS" describe superset superset | tail -30; exit 1; }
# Etiketler operator 0.2.0'dan doğrulandı (Task 4 Step 2): web Deployment/Service <CR adı>-web-server, pod etiketi instance=<CR adı>
POD=$(kubectl -n "$NS" get pod -l app.kubernetes.io/name=superset,app.kubernetes.io/instance=superset,app.kubernetes.io/component=web-server -o jsonpath='{.items[0].metadata.name}')
kubectl -n "$NS" wait --for=condition=Ready "pod/$POD" --timeout=600s
echo "== health + login"
kubectl -n "$NS" exec "$POD" -- python -c "
import urllib.request as u
assert u.urlopen('http://localhost:8088/health').read() == b'OK'
html = u.urlopen('http://localhost:8088/login/').read().decode()
assert 'keycloak' in html.lower(), html[:500]
print('OK superset health + keycloak login')"
echo "== Trino bağlantısı (deklaratif import + test-db + sorgu)"
kubectl -n "$NS" exec "$POD" -- superset legacy-import-datasources -p /app/configs/trino.yaml
# -c create_engine() kwargs'ı: TLS ayarları connect_args altında olmalı (düz http_scheme/verify TypeError verir)
kubectl -n "$NS" exec "$POD" -- sh -c 'superset test-db "trino://superset:${TRINO_PASSWORD}@trino.lakehouse.svc:8443/lakehouse" -c "{\"connect_args\": {\"http_scheme\": \"https\", \"verify\": \"/etc/lakehouse-ca/tls.crt\"}}"' | tail -12
kubectl -n "$NS" exec "$POD" -- python -c "
from superset.app import create_app
app = create_app()
with app.app_context():
    from superset import db
    from superset.models.core import Database
    d = db.session.query(Database).filter_by(database_name='lakehouse').one()
    with d.get_sqla_engine() as e:
        n = e.connect().execute(__import__('sqlalchemy').text('select count(*) from shop.orders')).scalar()
    assert n == 3, n
    print('OK superset -> trino shop.orders == 3 (SQLALCHEMY_CUSTOM_PASSWORD_STORE + TLS)')"
echo "E2E F4 SUPERSET OK"
