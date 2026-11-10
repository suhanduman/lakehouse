from datetime import datetime

import health_lib as hl


def test_layer_of():
    silver = {"shop", "fin"}
    assert hl.layer_of("shop_raw", silver) == "bronze"
    assert hl.layer_of("nginx_raw", silver) == "bronze"
    assert hl.layer_of("shop", silver) == "silver"
    assert hl.layer_of("sandbox", silver) == "sandbox"
    assert hl.layer_of("ops", silver) == "diger"


def test_summarize_takes_latest_snapshot_and_computes_average_file_size():
    t1, t2 = datetime(2026, 11, 10, 10), datetime(2026, 11, 10, 11)
    snaps = [(t2, {"total-records": "300", "total-data-files": "4", "total-delete-files": "1",
                   "total-files-size": str(8 * 1048576)}),
             (t1, {"total-records": "100", "total-data-files": "1", "total-files-size": "1048576"})]
    r = hl.summarize("fin.invoices", "silver", snaps, t2)
    assert r["son_degisiklik"] == t2 and r["snapshot_sayisi"] == 2
    assert (r["kayit"], r["veri_dosyasi"], r["delete_dosyasi"]) == (300, 4, 1)
    assert r["boyut_mib"] == 8.0 and r["ort_dosya_mib"] == 2.0


def test_summarize_empty_table_and_missing_fields():
    assert hl.summarize("a.b", "diger", [], datetime(2026, 1, 1)) is None
    r = hl.summarize("a.b", "diger", [(datetime(2026, 1, 1), {"total-records": "x"})], datetime(2026, 1, 1))
    assert r["kayit"] is None and r["veri_dosyasi"] is None and r["ort_dosya_mib"] is None
