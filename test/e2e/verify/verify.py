"""verify.py <ns.table> <min_rows> [--wait SANİYE] [k=v:k=v ...] ['!k=v' ...]
Polaris'ten pyiceberg ile okur (küme içi Job; F0 karar 9). k=v:k=v -> tüm eşleşen bir satır OLMALI; !k=v -> böyle satır OLMAMALI.
--wait: koşullar sağlanana kadar 15 s aralıkla tekrar dener (sink commit / merge gecikmesi)."""
import collections
import os
import sys
import time

from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchTableError

args = sys.argv[1:]
table, min_rows = args[0], int(args[1])
rest = args[2:]
wait = 0
if rest and rest[0] == "--wait":
    wait = int(rest[1])
    rest = rest[2:]
must = [dict(kv.split("=", 1) for kv in c.split(":")) for c in rest if not c.startswith("!")]
must_not = [dict(kv.split("=", 1) for kv in c[1:].split(":")) for c in rest if c.startswith("!")]
cat = load_catalog("lakehouse", type="rest", uri=os.environ["POLARIS_URI"], warehouse="lakehouse",
                   credential=f"{os.environ['CLIENT_ID']}:{os.environ['CLIENT_SECRET']}", scope="PRINCIPAL_ROLE:ALL",
                   **{"header.X-Iceberg-Access-Delegation": "vended-credentials", "s3.endpoint": os.environ["S3_ENDPOINT"],
                      "s3.path-style-access": "true", "s3.region": "us-east-1"})


def match(row, cond):
    return all(str(row.get(k)) == v for k, v in cond.items())


deadline = time.time() + wait
while True:
    try:
        rows = cat.load_table(table).scan().to_arrow().to_pylist()
    except NoSuchTableError:
        rows = None
    problems = []
    if rows is None:
        problems.append("tablo yok")
    else:
        if len(rows) < min_rows:
            problems.append(f"{len(rows)} < {min_rows} satır")
        problems += [f"eksik satır {c}" for c in must if not any(match(r, c) for r in rows)]
        problems += [f"olmaması gereken satır {c}" for c in must_not if any(match(r, c) for r in rows)]
    if not problems:
        break
    if time.time() >= deadline:
        print(f"HATA {table}: {problems}")
        print("satırlar:", rows)
        sys.exit(1)
    print(f"bekleniyor {table}: {problems}")
    time.sleep(15)
ops = collections.Counter((r.get("_cdc") or {}).get("op") for r in rows) if rows and "_cdc" in rows[0] else {}
print(f"OK {table}: rows={len(rows)} ops={dict(ops)}")
