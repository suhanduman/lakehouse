"""Tablo sağlığı ölçümü için Spark'sız yardımcılar (birim testli: tests/test_health_lib.py).

iceberg_maintenance.py saatlik koşusunun sonunda katalogdaki HER tabloyu dolaşır ve ops.table_health tablosuna
tablo başına bir satır ekler; Superset'teki "Tablo sağlığı" panosu yalnız bu tabloyu okur -> pano her kurulumda,
hangi tablolar olursa olsun çalışır (sabit tablo listesi yok)."""
from __future__ import annotations

HEALTH_NAMESPACE = "ops"
HEALTH_TABLE = "table_health"
HEALTH_RETENTION_DAYS = 90

HEALTH_DDL_COLUMNS = (
    "olcum_zamani TIMESTAMP, tablo STRING, katman STRING, son_degisiklik TIMESTAMP, snapshot_sayisi BIGINT, "
    "kayit BIGINT, veri_dosyasi BIGINT, delete_dosyasi BIGINT, boyut_mib DOUBLE, ort_dosya_mib DOUBLE"
)


def layer_of(namespace: str, silver_namespaces: set[str]) -> str:
    """Tablonun katmanı: Bronze (_raw ad alanı), Silver (bir pipeline'ın Silver ad alanı), sandbox ya da diğer."""
    if namespace.endswith("_raw"):
        return "bronze"
    if namespace in silver_namespaces:
        return "silver"
    if namespace == "sandbox":
        return "sandbox"
    return "diger"


def _int(summary: dict, key: str) -> int | None:
    v = (summary or {}).get(key)
    try:
        return int(v) if v is not None else None
    except (TypeError, ValueError):
        return None


def summarize(table: str, layer: str, snapshots: list[tuple], measured_at) -> dict | None:
    """snapshots: (committed_at, summary-dict) listesi. En son snapshot'ın özetinden tablonun durumunu çıkarır.
    Hiç snapshot yoksa (boş yaratılmış tablo) None — satır yazılmaz."""
    if not snapshots:
        return None
    last_at, last_sum = max(snapshots, key=lambda s: s[0])
    files = _int(last_sum, "total-data-files")
    size = _int(last_sum, "total-files-size")
    size_mib = round(size / 1048576.0, 3) if size is not None else None
    avg_mib = round(size_mib / files, 3) if size_mib is not None and files else None
    return {
        "olcum_zamani": measured_at,
        "tablo": table,
        "katman": layer,
        "son_degisiklik": last_at,
        "snapshot_sayisi": len(snapshots),
        "kayit": _int(last_sum, "total-records"),
        "veri_dosyasi": files,
        "delete_dosyasi": _int(last_sum, "total-delete-files"),
        "boyut_mib": size_mib,
        "ort_dosya_mib": avg_mib,
    }
