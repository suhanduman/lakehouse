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
