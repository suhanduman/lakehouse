"""merge_lib — Spark'sız saf fonksiyonlar (pytest). merge_cdc.py ve iceberg_maintenance.py kullanır.
Sözleşme (F0 S2d): Bronze = iş kolonları + _cdc{op∈{I,U,D}, ts, offset, source, target, key}; Silver = iş kolonları (casts uygulanmış).
Alan listeleri: [(kolon_adı, spark_tipi_simpleString)]."""
from __future__ import annotations

import re
from dataclasses import dataclass, field

CDC_COL = "_cdc"
DELETE_OP = "D"
WRITE_MODES = ("merge-on-read", "copy-on-write")


@dataclass(frozen=True)
class Pipeline:
    bronze: str                               # ör. shop_raw.orders
    keys: list[str]
    write_mode: str = "merge-on-read"
    bucket_count: int = 16
    casts: dict[str, str] = field(default_factory=dict)   # kolon -> Spark tipi (ör. updated_at: timestamp) — S2d: timestamptz string gelir
    silver: str | None = None                 # varsayılan: silver_name(bronze)


def parse_pipelines(doc: dict) -> list[Pipeline]:
    out: list[Pipeline] = []
    for p in (doc or {}).get("pipelines") or []:
        if not p.get("bronze") or not p.get("keys"):
            raise ValueError(f"pipeline {p}: 'bronze' ve boş olmayan 'keys' zorunlu")
        wm = p.get("write_mode", "merge-on-read")
        if wm not in WRITE_MODES:
            raise ValueError(f"{p['bronze']}: write_mode {wm!r} — {WRITE_MODES} olmalı")
        out.append(Pipeline(bronze=p["bronze"], keys=list(p["keys"]), write_mode=wm,
                            bucket_count=int(p.get("bucket_count", 16)), casts=dict(p.get("casts") or {}), silver=p.get("silver")))
    return out


def silver_name(bronze: str) -> str:
    """<ns>_raw.<t> -> <ns>.<t>"""
    ns, tbl = bronze.rsplit(".", 1)
    if not ns.endswith("_raw"):
        raise ValueError(f"{bronze}: Bronze namespace '_raw' ile bitmeli ya da pipeline'da 'silver' verilmeli")
    return f"{ns[:-4]}.{tbl}"


def q(ident: str) -> str:
    return "`" + ident.replace("`", "``") + "`"


def business_columns(fields: list[tuple[str, str]]) -> list[tuple[str, str]]:
    return [(n, t) for n, t in fields if n != CDC_COL]


def silver_columns(bronze_fields: list[tuple[str, str]], casts: dict[str, str]) -> list[tuple[str, str]]:
    cols = business_columns(bronze_fields)
    unknown = set(casts) - {n for n, _ in cols}
    if unknown:
        raise ValueError(f"casts bilinmeyen kolon(lar): {sorted(unknown)}")
    return [(n, casts.get(n, t)) for n, t in cols]


def create_silver_sql(silver: str, columns: list[tuple[str, str]], keys: list[str], write_mode: str, bucket_count: int) -> str:
    names = {n for n, _ in columns}
    missing = [k for k in keys if k not in names]
    if missing:
        raise ValueError(f"{silver}: anahtar kolon(lar) Bronze'da yok: {missing}")
    cols = ", ".join(f"{q(n)} {t}" for n, t in columns)
    props = {"format-version": "2", "write.merge.mode": write_mode, "write.update.mode": write_mode,
             "write.delete.mode": write_mode, "write.distribution-mode": "hash",
             "write.metadata.delete-after-commit.enabled": "true"}
    tbl = ", ".join(f"'{k}'='{v}'" for k, v in props.items())
    return (f"CREATE TABLE IF NOT EXISTS {silver} ({cols}) USING iceberg "
            f"PARTITIONED BY (bucket({bucket_count}, {q(keys[0])})) TBLPROPERTIES ({tbl})")


def dedup_select_sql(source_view: str, columns: list[tuple[str, str]], keys: list[str], casts: dict[str, str]) -> str:
    """Anahtar başına son durum (ORDER BY _cdc.ts DESC, _cdc.offset DESC); casts açık CAST (Spark 4 ANSI)."""
    sel = ", ".join(f"CAST({q(n)} AS {casts[n]}) AS {q(n)}" if n in casts else q(n) for n, _ in columns)
    part = ", ".join(q(k) for k in keys)
    return (f"SELECT {sel}, {CDC_COL}.op AS __op FROM (SELECT *, row_number() OVER (PARTITION BY {part} "
            f"ORDER BY {CDC_COL}.ts DESC, {CDC_COL}.offset DESC) AS __rn FROM {source_view}) WHERE __rn = 1")


def merge_sql(silver: str, inc_view: str, columns: list[tuple[str, str]], keys: list[str]) -> str:
    on = " AND ".join(f"t.{q(k)} = s.{q(k)}" for k in keys)
    non_keys = [n for n, _ in columns if n not in keys] or [k for k in keys]
    upd = ", ".join(f"{q(n)} = s.{q(n)}" for n in non_keys)
    names = ", ".join(q(n) for n, _ in columns)
    vals = ", ".join(f"s.{q(n)}" for n, _ in columns)
    return (f"MERGE INTO {silver} t USING {inc_view} s ON {on} "
            f"WHEN MATCHED AND s.__op = '{DELETE_OP}' THEN DELETE "
            f"WHEN MATCHED THEN UPDATE SET {upd} "
            f"WHEN NOT MATCHED AND s.__op <> '{DELETE_OP}' THEN INSERT ({names}) VALUES ({vals})")


# --- şema uzlaştırma (spec §12 güvenli-genişletme haritası) ---
_DEC = re.compile(r"decimal\((\d+),\s*(\d+)\)")
_WIDEN = {("tinyint", "smallint"), ("tinyint", "int"), ("tinyint", "bigint"), ("smallint", "int"),
          ("smallint", "bigint"), ("int", "bigint"), ("float", "double")}


class SchemaConflict(Exception):
    """Silver kolon tipi güvenli genişletilemez — manuel migrasyon (fail-loud)."""


def can_widen(src: str, dst: str) -> bool:
    if src == dst or (src, dst) in _WIDEN:
        return True
    a, b = _DEC.fullmatch(src), _DEC.fullmatch(dst)
    return bool(a and b and int(a[2]) == int(b[2]) and int(b[1]) >= int(a[1]))


def plan_schema_changes(silver: str, silver_fields: list[tuple[str, str]], target_fields: list[tuple[str, str]]) -> list[str]:
    """Silver'ı hedefe (Bronze iş kolonları + casts) getiren DDL'ler: ADD COLUMN / güvenli ALTER TYPE; aksi SchemaConflict.
    Silver'da olup hedefte olmayan kolonlara dokunulmaz."""
    have = dict(silver_fields)
    ddl: list[str] = []
    for n, t in target_fields:
        if n not in have:
            ddl.append(f"ALTER TABLE {silver} ADD COLUMN {q(n)} {t}")
        elif have[n] == t:
            continue
        elif can_widen(have[n], t):
            ddl.append(f"ALTER TABLE {silver} ALTER COLUMN {q(n)} TYPE {t}")
        else:
            raise SchemaConflict(f"{silver}.{n}: {have[n]} -> {t} güvenli genişletme değil (manuel migrasyon gerekir)")
    return ddl
