"""verify.py <ns.table> <min_rows> [--wait SANİYE] [--exact] [--prev-rows N] [k=v:~k2=s ...] ['!k=v' ...]
Polaris'ten pyiceberg ile okur (küme içi Job; F0 karar 9). k=v:k=v -> tüm eşleşen bir satır OLMALI; !k=v -> böyle satır OLMAMALI.
~k=s -> alan değeri s'yi İÇERİR (str; JSON _doc / timestamp için); ':' ile birleşik satırda parça başına kullanılabilir.
--wait: koşullar sağlanana kadar 15 s aralıkla tekrar dener (sink commit / merge gecikmesi).
--exact: satır sayısı min_rows'a EŞİT olmalı (fazlası da hata; MERGE sonrası Silver'da kesin sayı).
--prev-rows N: mevcut snapshot'ın EBEVEYNİ (parent_snapshot_id) taranır ve satır sayısı N olmalı — spec §9 zaman-yolculuğu."""
import collections
import os
import sys
import time

from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchTableError

args = sys.argv[1:]
table, min_rows = args[0], int(args[1])
rest = args[2:]
wait, exact, prev_rows = 0, False, None
while rest and rest[0].startswith("--"):        # bayraklar koşullardan önce (koşullar 'k=v' ya da '!k=v')
    if rest[0] == "--wait":
        wait, rest = int(rest[1]), rest[2:]
    elif rest[0] == "--exact":
        exact, rest = True, rest[1:]
    elif rest[0] == "--prev-rows":
        prev_rows, rest = int(rest[1]), rest[2:]
    else:
        print(f"bilinmeyen bayrak: {rest[0]}")
        sys.exit(2)
def cond(c):                                    # "k=v:~k2=s" -> [(k, v, exact), (k2, s, contains)]
    return [(kv.lstrip("~").split("=", 1)[0], kv.split("=", 1)[1], not kv.startswith("~")) for kv in c.split(":")]


must = [cond(c) for c in rest if not c.startswith("!")]
must_not = [cond(c[1:]) for c in rest if c.startswith("!")]
cat = load_catalog("lakehouse", type="rest", uri=os.environ["POLARIS_URI"], warehouse="lakehouse",
                   credential=f"{os.environ['CLIENT_ID']}:{os.environ['CLIENT_SECRET']}", scope="PRINCIPAL_ROLE:ALL",
                   **{"header.X-Iceberg-Access-Delegation": "vended-credentials", "s3.endpoint": os.environ["S3_ENDPOINT"],
                      "s3.path-style-access": "true", "s3.region": "us-east-1"})


def match(row, parts):
    return all(str(row.get(k)) == v if exact else v in str(row.get(k) or "") for k, v, exact in parts)


deadline = time.time() + wait
tbl = None
while True:
    try:
        tbl = cat.load_table(table)
        rows = tbl.scan().to_arrow().to_pylist()
    except NoSuchTableError:
        rows = None
    problems = []
    if rows is None:
        problems.append("tablo yok")
    else:
        if exact and len(rows) != min_rows:
            problems.append(f"{len(rows)} != {min_rows} satır (--exact)")
        elif len(rows) < min_rows:
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
prev = ""
if prev_rows is not None:                       # zaman yolculuğu: bir önceki snapshot hâlâ okunabilir olmalı (spec §9)
    snap = tbl.current_snapshot()
    parent = snap.parent_snapshot_id if snap else None
    if parent is None:
        print(f"HATA {table}: ebeveyn snapshot yok (--prev-rows {prev_rows})")
        sys.exit(1)
    n_prev = len(tbl.scan(snapshot_id=parent).to_arrow().to_pylist())
    if n_prev != prev_rows:
        print(f"HATA {table}: ebeveyn snapshot {parent} satır sayısı {n_prev} != {prev_rows}")
        sys.exit(1)
    prev = f" prev({parent})={n_prev}"
ops = collections.Counter((r.get("_cdc") or {}).get("op") for r in rows) if rows and "_cdc" in rows[0] else {}
print(f"OK {table}: rows={len(rows)}{prev} ops={dict(ops)}")
