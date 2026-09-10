"""verify.py <namespace> <table> <min_rows> — Bronze tabloyu Polaris'ten okur; satır sayısı ve _cdc.op dağılımı.
Küme içinde (Job) koşar: POLARIS_URI / S3_ENDPOINT / CLIENT_ID / CLIENT_SECRET env'lerinden."""
import os, sys, collections
from pyiceberg.catalog import load_catalog
ns, tbl, min_rows = sys.argv[1], sys.argv[2], int(sys.argv[3])
cat = load_catalog("lakehouse", type="rest", uri=os.environ["POLARIS_URI"], warehouse="lakehouse",
                   credential=f"{os.environ['CLIENT_ID']}:{os.environ['CLIENT_SECRET']}", scope="PRINCIPAL_ROLE:ALL",
                   **{"s3.endpoint": os.environ["S3_ENDPOINT"], "s3.path-style-access": "true", "s3.region": "us-east-1"})
t = cat.load_table(f"{ns}.{tbl}")
print("schema:", t.schema())
print("partition spec:", t.spec())
rows = t.scan().to_arrow().to_pylist()
ops = collections.Counter((r.get("_cdc") or {}).get("op") for r in rows)
print(f"rows={len(rows)} ops={dict(ops)}")
print("örnek:", rows[0] if rows else None)
for r in rows:
    if (r.get("_cdc") or {}).get("op") in ("U","D","u","d"): print("cdc-satır:", r)
assert len(rows) >= min_rows, f"{len(rows)} < {min_rows}"
print("OK")
