"""S1b/S1c: Polaris'e namespace+tablo yarat, 2 satır yaz, geri oku.
Kullanım: python smoke.py <client_id> <client_secret> [--static-keys]
--static-keys: vended-credentials yerine istemcinin kendi MinIO anahtarları (stsUnavailable senaryosu)."""
import os, sys, pyarrow as pa
from pyiceberg.catalog import load_catalog

cid, csec = sys.argv[1], sys.argv[2]
static = "--static-keys" in sys.argv
props = {
    "type": "rest",
    "uri": os.environ.get("POLARIS_URI", "http://localhost:8181/api/catalog"),
    "warehouse": os.environ.get("WAREHOUSE", "lakehouse"),
    "credential": f"{cid}:{csec}",
    "scope": "PRINCIPAL_ROLE:ALL",
    "s3.endpoint": os.environ.get("S3_ENDPOINT", "http://localhost:9000"),
    "s3.path-style-access": "true",
    "s3.region": "us-east-1",
}
if static:
    props.update({"s3.access-key-id": "minioadmin", "s3.secret-access-key": "minioadmin"})
    # stsUnavailable katalogda istemci vending İSTEMEMELİ; pyiceberg header'ı setdefault ile ekler -> override
    if "DELEGATION_HEADER" in os.environ:
        props["header.X-Iceberg-Access-Delegation"] = os.environ["DELEGATION_HEADER"]
else:
    props["header.X-Iceberg-Access-Delegation"] = "vended-credentials"
cat = load_catalog("lakehouse", **props)
ns = "smoke_static" if static else "smoke_vended"
cat.create_namespace_if_not_exists(ns)
schema = pa.schema([pa.field("id", pa.int64(), nullable=False), pa.field("name", pa.string())])
ident = f"{ns}.t"
if cat.table_exists(ident):
    cat.drop_table(ident)
t = cat.create_table(ident, schema=schema)
t.append(pa.Table.from_pylist([{"id": 1, "name": "a"}, {"id": 2, "name": "b"}], schema=schema))
rows = t.scan().to_arrow().num_rows
assert rows == 2, rows
print(f"OK: {ident} yazıldı/okundu, {rows} satır, mode={'static' if static else 'vended'}")
