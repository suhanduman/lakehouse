#!/usr/bin/env python3
"""No-S3-residue check for the kind e2e: every object in the Iceberg buckets
must be tracked by CURRENT Iceberg table metadata.

Tracked set = for every table in both warehouses ("warehouse" -> bucket
depo, "rawdata" -> bucket ham-veri): the current metadata.json + every
metadata-log entry, every snapshot's manifest list, every manifest, every
data file referenced by any manifest entry (live or deleted), and any
statistics (puffin) files. Anything listed in those buckets but NOT in the
tracked set is residue — files the platform wrote and then lost track of
(exactly what remove_orphan_files exists to prevent accumulating).

Deliberately checks only the two Iceberg warehouse buckets: `backups`
(CNPG barman WAL archive) and `nginx-web` (streaming checkpoints) are
non-Iceberg by design.

Runs in the in-cluster tools pod. Deps: pyiceberg[s3fs] (installed by
test/e2e/lib.sh e2e::tools_pip). Env: NESSIE_URI, S3_ENDPOINT,
S3_ACCESS_KEY, S3_SECRET_KEY (test/e2e/manifests/tools-pod.yaml).
"""
from __future__ import annotations

import os
import sys

import s3fs
from pyiceberg.catalog.rest import RestCatalog

# warehouse name -> bucket its `s3://<bucket>/warehouse` location lives in
# (chart/values.yaml `nessie.catalog.*.location`).
WAREHOUSE_BUCKETS = {"warehouse": "depo", "rawdata": "ham-veri"}


def _catalog(warehouse: str) -> RestCatalog:
    props = {
        "uri": os.environ["NESSIE_URI"],
        "warehouse": warehouse,
        "s3.endpoint": os.environ.get("S3_ENDPOINT"),
        "s3.access-key-id": os.environ.get("S3_ACCESS_KEY"),
        "s3.secret-access-key": os.environ.get("S3_SECRET_KEY"),
        "s3.path-style-access": "true",
    }
    return RestCatalog(f"e2e-{warehouse}", **{k: v for k, v in props.items() if v})


def _norm(path: str) -> str:
    """s3://bucket/key (or s3a://) -> 'bucket/key'."""
    if "://" in path:
        return path.split("://", 1)[1]
    return path.lstrip("/")


def _tracked_files(table) -> set[str]:
    tracked: set[str] = set()
    md = table.metadata
    tracked.add(_norm(table.metadata_location))
    for entry in md.metadata_log:
        tracked.add(_norm(entry.metadata_file))
    for snap in md.snapshots:
        tracked.add(_norm(snap.manifest_list))
        for manifest in snap.manifests(table.io):
            tracked.add(_norm(manifest.manifest_path))
            for me in manifest.fetch_manifest_entry(table.io, discard_deleted=False):
                tracked.add(_norm(me.data_file.file_path))
    for stat in getattr(md, "statistics", None) or []:
        tracked.add(_norm(stat.statistics_path))
    for pstat in getattr(md, "partition_statistics", None) or []:
        tracked.add(_norm(pstat.statistics_path))
    return tracked


def main() -> int:
    tracked: set[str] = set()
    table_count = 0
    for warehouse in WAREHOUSE_BUCKETS:
        cat = _catalog(warehouse)
        for ns in cat.list_namespaces():
            for ident in cat.list_tables(ns):
                table = cat.load_table(ident)
                tracked |= _tracked_files(table)
                table_count += 1
    print(f"s3_residue_check: {table_count} table(s), {len(tracked)} tracked file(s)")

    fs = s3fs.S3FileSystem(
        key=os.environ.get("S3_ACCESS_KEY"),
        secret=os.environ.get("S3_SECRET_KEY"),
        client_kwargs={"endpoint_url": os.environ["S3_ENDPOINT"]},
    )
    residue: list[str] = []
    listed = 0
    for bucket in WAREHOUSE_BUCKETS.values():
        try:
            keys = fs.find(bucket)
        except FileNotFoundError:
            keys = []
        for key in keys:
            listed += 1
            if _norm(key) not in tracked:
                residue.append(key)

    print(f"s3_residue_check: {listed} object(s) listed in {sorted(WAREHOUSE_BUCKETS.values())}")
    if residue:
        print(f"FATAL: {len(residue)} untracked object(s) — S3 residue:", file=sys.stderr)
        for key in sorted(residue):
            print(f"  RESIDUE: {key}", file=sys.stderr)
        return 1
    print("s3_residue_check: OK — no residue (every object is Iceberg-tracked)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
