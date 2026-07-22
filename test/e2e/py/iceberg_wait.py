#!/usr/bin/env python3
"""Poll an Iceberg (Bronze) table until its current snapshot holds at least
--min-rows records — the "CDC rows actually landed" gate of the kind e2e.

Metadata-only on purpose: reads the current snapshot's `total-records`
summary via the Nessie Iceberg REST catalog, so it needs neither pyarrow nor
S3 data access (Bronze is append-only, so total-records == row count).

Env (set on the e2e tools pod, see test/e2e/manifests/tools-pod.yaml):
  NESSIE_URI    Iceberg REST base, e.g. http://nessie:19120/iceberg/
  S3_ENDPOINT / S3_ACCESS_KEY / S3_SECRET_KEY  (passed through to the
                catalog for consistency; not used by a metadata-only poll)

Usage: iceberg_wait.py --table depo_raw.customers --min-rows 5 [--timeout 600]
"""
from __future__ import annotations

import argparse
import os
import sys
import time

from pyiceberg.catalog.rest import RestCatalog


def _catalog(warehouse: str) -> RestCatalog:
    props = {
        "uri": os.environ["NESSIE_URI"],
        "warehouse": warehouse,
        "s3.endpoint": os.environ.get("S3_ENDPOINT"),
        "s3.access-key-id": os.environ.get("S3_ACCESS_KEY"),
        "s3.secret-access-key": os.environ.get("S3_SECRET_KEY"),
        "s3.path-style-access": "true",
    }
    return RestCatalog("e2e", **{k: v for k, v in props.items() if v})


def _total_records(snapshot) -> int | None:
    summary = snapshot.summary
    if summary is None:
        return None
    # pyiceberg Summary supports dict-style access; fall back to the
    # underlying additional_properties dict across versions.
    try:
        value = summary["total-records"]
    except Exception:  # noqa: BLE001
        value = getattr(summary, "additional_properties", {}).get("total-records")
    return int(value) if value is not None else None


def main() -> int:
    p = argparse.ArgumentParser(description="Wait for an Iceberg table to reach a row count.")
    p.add_argument("--table", required=True, help="namespace.table (e.g. depo_raw.customers)")
    p.add_argument("--warehouse", default="rawdata")
    p.add_argument("--min-rows", type=int, required=True)
    p.add_argument("--timeout", type=int, default=600)
    args = p.parse_args()

    deadline = time.monotonic() + args.timeout
    last = "table not loadable yet"
    while time.monotonic() < deadline:
        try:
            table = _catalog(args.warehouse).load_table(args.table)
            snapshot = table.current_snapshot()
            if snapshot is None:
                last = "no snapshot yet (sink has not committed)"
            else:
                total = _total_records(snapshot)
                last = f"total-records={total}"
                if total is not None and total >= args.min_rows:
                    print(f"iceberg_wait: OK — {args.table} {last} (>= {args.min_rows})")
                    return 0
        except Exception as e:  # noqa: BLE001 — keep polling until the deadline
            last = f"error: {e}"
        print(f"iceberg_wait: {args.table}: {last} — waiting...", flush=True)
        time.sleep(10)

    print(f"FATAL: {args.table} never reached {args.min_rows} rows "
          f"within {args.timeout}s (last: {last})", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
