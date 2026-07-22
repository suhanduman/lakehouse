from tools.jobs import merge_cdc as mc


def test_defaults_run_all():
    a = mc.parse_args([])
    assert a.reset_watermark is None and a.restate is None
    assert a.bronze_suffix == "_raw"
    assert a.bronze_catalog == "rawlake" and a.silver_catalog == "lakehouse"


def test_reset_watermark_with_from_snapshot():
    a = mc.parse_args(["--reset-watermark", "depo.customers", "--from-snapshot", "42"])
    assert a.reset_watermark == "depo.customers"
    assert a.from_snapshot == 42


def test_restate():
    a = mc.parse_args(["--restate", "depo.customers"])
    assert a.restate == "depo.customers"


def test_constants():
    assert mc.WATERMARK_PROP == "app.bronze-merged-snapshot"
    assert mc.AUDIT_TABLE == "lakehouse.ops.merge_runs"
