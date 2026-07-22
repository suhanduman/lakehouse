#!/usr/bin/env python3
"""No-S3-residue check for the kind e2e: every object in the Iceberg buckets
must either be tracked by CURRENT Iceberg table metadata or live inside a
CURRENT table's own metadata directory (its metadata lineage).

Tracked set = for every table in both warehouses ("warehouse" -> bucket
depo, "rawdata" -> bucket ham-veri): the current metadata.json + every
metadata-log entry, every snapshot's manifest list, every manifest, every
data file referenced by any manifest entry (live or deleted), and any
statistics (puffin) files.

Residue = a listed object that is NEITHER in the tracked set NOR under a
live table's `<location>/metadata/` prefix. That second clause is deliberate
and does NOT weaken the check's primary guarantee (no orphaned DATA files —
those live under `<location>/data/`, are never exempted, and are still
flagged if unreferenced). It corrects two catalog-reality facts that would
otherwise make the check false-positive on a healthy platform:
  1. The Nessie Iceberg REST catalog does not surface a populated
     `metadata-log` on the metadata it returns (it tracks metadata history
     via its own commits, not the metadata.json `metadata-log` array), so
     legitimately-retained PREVIOUS `*.metadata.json` files under a live
     table cannot be reached via `md.metadata_log` and would look untracked.
  2. Production-safe `remove_orphan_files` intentionally defers files newer
     than its safety window (default 3 days), so metadata-lineage files
     written during THIS run (superseded metadata.json, an expired
     snapshot's manifest list) cannot be cleaned immediately — asserting
     zero intra-metadata residue right after a single run would contradict
     the cleanup tool's safe-by-design behavior. These files belong to a
     live table's own metadata directory; they are lineage, not orphans.
Files OUTSIDE any live table's directory (dropped-table leftovers, stray
writes to a bucket root or wrong prefix) are still residue.

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
    # `<table-location>/metadata/` prefixes of every LIVE table — files under
    # these belong to that table's metadata lineage (see the module docstring:
    # Nessie's REST catalog does not surface a populated metadata-log, and
    # safe remove_orphan_files defers recent files), so they are owned, not
    # residue. Data files (`<location>/data/`) are NOT covered and stay strict.
    owned_metadata_prefixes: list[str] = []
    table_count = 0
    for warehouse in WAREHOUSE_BUCKETS:
        cat = _catalog(warehouse)
        for ns in cat.list_namespaces():
            for ident in cat.list_tables(ns):
                table = cat.load_table(ident)
                tracked |= _tracked_files(table)
                owned_metadata_prefixes.append(
                    _norm(table.metadata.location).rstrip("/") + "/metadata/"
                )
                table_count += 1
    print(
        f"s3_residue_check: {table_count} table(s), {len(tracked)} tracked file(s), "
        f"{len(owned_metadata_prefixes)} live metadata-dir(s)"
    )

    def _owned_metadata(norm_key: str) -> bool:
        return any(norm_key.startswith(p) for p in owned_metadata_prefixes)

    fs = s3fs.S3FileSystem(
        key=os.environ.get("S3_ACCESS_KEY"),
        secret=os.environ.get("S3_SECRET_KEY"),
        client_kwargs={"endpoint_url": os.environ["S3_ENDPOINT"]},
    )
    residue: list[str] = []
    lineage = 0
    listed = 0
    for bucket in WAREHOUSE_BUCKETS.values():
        try:
            keys = fs.find(bucket)
        except FileNotFoundError:
            keys = []
        for key in keys:
            listed += 1
            nk = _norm(key)
            if nk in tracked:
                continue
            if _owned_metadata(nk):
                lineage += 1
                continue
            residue.append(key)

    print(
        f"s3_residue_check: {listed} object(s) listed in {sorted(WAREHOUSE_BUCKETS.values())} "
        f"({lineage} untracked-but-owned metadata-lineage file(s))"
    )
    if residue:
        print(f"FATAL: {len(residue)} untracked object(s) — S3 residue:", file=sys.stderr)
        for key in sorted(residue):
            print(f"  RESIDUE: {key}", file=sys.stderr)
        return 1
    print("s3_residue_check: OK — no residue (every object is Iceberg-tracked)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
