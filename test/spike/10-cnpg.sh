#!/usr/bin/env bash
set -euo pipefail
SELF="$(cd "$(dirname "$0")" && pwd)/$(basename "$0")"
CNPG_VERSION=1.30.0
if [[ "${1:-}" == "--check" ]]; then
  kubectl -n polaris wait --for=condition=Ready cluster/polaris-db --timeout=10s >/dev/null
  kubectl -n lakehouse wait --for=condition=Ready cluster/demo-pg --timeout=10s >/dev/null
  kubectl -n polaris get secret polaris-db-app -o jsonpath='{.data.jdbc-uri}' | base64 -d | grep -q '^jdbc:postgresql://'
  n=$(kubectl -n lakehouse exec demo-pg-1 -c postgres -- psql -U postgres -d shop -Atc "select count(*) from public.orders")
  [[ "$n" == "3" ]] || { echo "orders satır sayısı $n != 3"; exit 1; }
  kubectl -n lakehouse exec demo-pg-1 -c postgres -- psql -U postgres -d shop -Atc "show wal_level" | grep -qx logical
  echo "OK: cnpg + polaris-db + demo-pg hazır"; exit 0
fi

kubectl apply --server-side -f "https://raw.githubusercontent.com/cloudnative-pg/cloudnative-pg/release-1.30/releases/cnpg-${CNPG_VERSION}.yaml"
kubectl -n cnpg-system rollout status deploy/cnpg-controller-manager --timeout=180s

kubectl -n polaris apply -f - <<'YAML'
apiVersion: postgresql.cnpg.io/v1
kind: Cluster
metadata: {name: polaris-db}
spec:
  instances: 1
  storage: {size: 2Gi}
  bootstrap:
    initdb: {database: polaris, owner: polaris}
YAML

kubectl -n lakehouse apply -f - <<'YAML'
apiVersion: v1
kind: Secret
metadata: {name: dbz-pg-cred}
type: kubernetes.io/basic-auth
stringData: {username: dbz, password: dbz}
---
apiVersion: postgresql.cnpg.io/v1
kind: Cluster
metadata: {name: demo-pg}
spec:
  instances: 1
  storage: {size: 2Gi}
  postgresql:
    parameters:
      wal_level: logical
      max_wal_senders: "10"
      max_replication_slots: "10"
  managed:
    roles:
    - name: dbz
      ensure: present
      login: true
      replication: true
      passwordSecret: {name: dbz-pg-cred}
  bootstrap:
    initdb:
      database: shop
      owner: app
      postInitApplicationSQL:
      # rol burada yaratılır: managed.roles initdb'den SONRA uygulanıyor (canlı bulgu S0), OWNER TO için rol önce gerekir
      - CREATE ROLE dbz LOGIN REPLICATION PASSWORD 'dbz';
      - CREATE TABLE public.orders (id bigserial PRIMARY KEY, status text NOT NULL, amount numeric(10,2) NOT NULL, updated_at timestamptz NOT NULL DEFAULT now());
      - INSERT INTO public.orders (status, amount) VALUES ('new', 10.50), ('paid', 99.99), ('new', 3.00);
      - ALTER TABLE public.orders OWNER TO dbz;
      - GRANT CREATE ON DATABASE shop TO dbz;
      - GRANT USAGE, CREATE ON SCHEMA public TO dbz;
YAML

kubectl -n polaris wait --for=condition=Ready cluster/polaris-db --timeout=300s
kubectl -n lakehouse wait --for=condition=Ready cluster/demo-pg --timeout=300s
"$SELF" --check
