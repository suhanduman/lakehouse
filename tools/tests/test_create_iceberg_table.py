import json
import subprocess
import sys
from pathlib import Path

import pytest

pytest.importorskip("pyiceberg")  # tool depends on pyiceberg; skip if unavailable locally

from tools import create_iceberg_table as c

REPO_ROOT = Path(__file__).resolve().parents[2]


def test_bronze_metadata_cols_exact_types():
    cols = {d["name"]: d["type"] for d in c.BRONZE_METADATA_COLS}
    assert cols == {"__op": "string", "__ts_ms": "timestamp", "__deleted": "string",
                    "__lsn": "long", "__kafka_offset": "long", "__kafka_partition": "int"}


def test_build_partition_spec_day_ts_ms():
    business = [{"name": "id", "type": "int"}, {"name": "name", "type": "string"}]
    schema = c._build_schema(business + c.BRONZE_METADATA_COLS, identifier=[])
    spec = c._build_partition_spec(schema, "__ts_ms")
    assert len(spec.fields) == 1
    pf = spec.fields[0]
    assert "day" in str(pf.transform).lower()
    ts_ms_id = schema.find_field("__ts_ms").field_id
    assert pf.source_id == ts_ms_id


def test_build_partition_spec_none_is_unpartitioned():
    schema = c._build_schema([{"name": "id", "type": "int"}], identifier=["id"])
    spec = c._build_partition_spec(schema, None)
    assert len(spec.fields) == 0


# ---------------------------------------------------------------------------
# Silver scale-ready layout: bucket(N, id) partition + identifier sort order +
# write.distribution-mode=hash + configurable write mode (2026-08-03 Task 3
# lockstep with console/backend/app/services/iceberg_service.py's Task 2
# _silver_partition_spec/_silver_sort_order/create_table changes). Verified
# via the real CLI end-to-end (subprocess, --dry-run) so the argparse
# plumbing (--bucket-count/--write-mode) is exercised too, not just the
# builder functions in isolation.
# ---------------------------------------------------------------------------
def test_build_silver_partition_spec_bucket_per_identifier_col():
    schema = c._build_schema(
        [{"name": "id", "type": "int"}, {"name": "name", "type": "string"}], identifier=["id"]
    )
    spec = c._build_silver_partition_spec(schema, ["id"], bucket_count=8)
    assert [f.name for f in spec.fields] == ["id_bucket"]
    pf = spec.fields[0]
    assert "bucket" in str(pf.transform).lower()
    assert pf.source_id == schema.find_field("id").field_id


def test_build_silver_partition_spec_composite_identifier():
    schema = c._build_schema(
        [{"name": "a", "type": "int"}, {"name": "b", "type": "string"}], identifier=["a", "b"]
    )
    spec = c._build_silver_partition_spec(schema, ["a", "b"], bucket_count=8)
    assert [f.name for f in spec.fields] == ["a_bucket", "b_bucket"]


def test_build_silver_sort_order_by_identifier():
    schema = c._build_schema(
        [{"name": "id", "type": "int"}, {"name": "name", "type": "string"}], identifier=["id"]
    )
    sort_order = c._build_silver_sort_order(schema, ["id"])
    assert [f.source_id for f in sort_order.fields] == [schema.find_field("id").field_id]


def _run_dry_run(tmp_path, descriptor: dict, extra_args=None):
    descriptor_path = tmp_path / "descriptor.json"
    descriptor_path.write_text(json.dumps(descriptor))
    return subprocess.run(
        [sys.executable, "tools/create_iceberg_table.py", "--descriptor", str(descriptor_path),
         "--dry-run", *(extra_args or [])],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )


_SILVER_DESCRIPTOR = {
    "namespace": "depo",
    "table": "customers",
    "identifier": ["id"],
    "layer": "silver",
    "columns": [
        {"name": "id", "type": "int"},
        {"name": "name", "type": "string"},
    ],
}


def test_main_silver_dry_run_shows_bucket_partition_hash_and_256mb(tmp_path):
    result = _run_dry_run(tmp_path, _SILVER_DESCRIPTOR)
    assert result.returncode == 0, result.stderr
    assert "partition=['id_bucket']" in result.stdout
    assert "'write.distribution-mode': 'hash'" in result.stdout
    assert "'write.target-file-size-bytes': '268435456'" in result.stdout


def test_main_silver_dry_run_write_mode_defaults_to_copy_on_write(tmp_path):
    result = _run_dry_run(tmp_path, _SILVER_DESCRIPTOR)
    assert result.returncode == 0, result.stderr
    assert "'write.merge.mode': 'copy-on-write'" in result.stdout
    assert "'write.update.mode': 'copy-on-write'" in result.stdout
    assert "'write.delete.mode': 'copy-on-write'" in result.stdout


def test_main_silver_dry_run_write_mode_merge_on_read(tmp_path):
    result = _run_dry_run(tmp_path, _SILVER_DESCRIPTOR, extra_args=["--write-mode", "merge-on-read"])
    assert result.returncode == 0, result.stderr
    assert "'write.merge.mode': 'merge-on-read'" in result.stdout
    assert "'write.update.mode': 'merge-on-read'" in result.stdout
    assert "'write.delete.mode': 'merge-on-read'" in result.stdout


def test_main_silver_dry_run_bucket_count_override(tmp_path):
    result = _run_dry_run(tmp_path, _SILVER_DESCRIPTOR, extra_args=["--bucket-count", "4"])
    assert result.returncode == 0, result.stderr
    # bucket count itself isn't printed by name, but the partition field name
    # (id_bucket) is unaffected by count — assert the run at least succeeds
    # and still yields a bucket partition on the identifier column.
    assert "partition=['id_bucket']" in result.stdout


def test_main_descriptor_layer_bronze_honored_end_to_end(tmp_path):
    # Final-review Fix 1: the GitOps Bronze PreSync Job feeds a descriptor with
    # `layer: bronze` to `--descriptor`, but main() never read that key — it
    # silently defaulted to args.layer="silver", so Bronze got created
    # UNPARTITIONED in the default (Silver) warehouse instead of
    # day(__ts_ms)/rawdata. This runs the real CLI end-to-end (subprocess,
    # --dry-run) with a bronze descriptor and asserts the descriptor's `layer`
    # key alone (no --layer flag) drives bronze handling.
    descriptor = {
        "namespace": "depo_raw",
        "table": "customers",
        "identifier": [],
        "layer": "bronze",
        "columns": [
            {"name": "id", "type": "int"},
            {"name": "name", "type": "string"},
        ],
    }
    descriptor_path = tmp_path / "bronze-descriptor.json"
    descriptor_path.write_text(json.dumps(descriptor))

    result = subprocess.run(
        [sys.executable, "tools/create_iceberg_table.py", "--descriptor", str(descriptor_path), "--dry-run"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    assert "layer=bronze" in result.stdout
    assert "warehouse=rawdata" in result.stdout
    assert "partition=['__ts_ms_day']" in result.stdout
