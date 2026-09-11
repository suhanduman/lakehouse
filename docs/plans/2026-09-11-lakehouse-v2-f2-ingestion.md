# Lakehouse v2 — F2 Ingestion (pg/mssql CDC → Bronze → Silver MERGE + bakım) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `glue/values.yaml` içindeki `sources:` listesinden kaynak-DB başına **1 Debezium + 1 Iceberg sink** `KafkaConnector` çifti, `pipelines:` listesinden **4 `ScheduledSparkApplication`** (silver-merge 15 dk + 3 bakım) deklaratif olarak üretilsin; `merge_cdc.py`/`iceberg_maintenance.py` Spark 4.1 + Iceberg 1.11 ile Polaris altında çalışsın; kind e2e **pg yolu** (fixture → Bronze → MERGE → UPDATE/DELETE → MERGE → bakım) lokal + GitHub Actions'ta yeşil olsun.

**Architecture:** F1'in "repo = konfigürasyon" çizgisi sürer: connector'lar ve Spark işleri **glue chart şablonu** (values'tan render), tek "kod" `glue/jobs/*.py` (Spark driver + saf fonksiyon kütüphanesi, pytest ile Spark'sız test). Spark resmi imaj + `spark.jars.packages`; Polaris credential'ı **Secret → env → `SparkSession.builder.config`** (YAML'a hiç girmez). Bronze→Silver watermark = Silver `TBLPROPERTIES` içinde Bronze snapshot-id (artımlı okuma; olmazsa tam okuma; MERGE idempotent). F0 bulguları birebir taşınır (S2 sink timeout'ları, S2d Bronze şekli, S4 ANSI CAST, küçük-tier Spark ayarları).

**Tech Stack:** Strimzi 1.2.0 (`KafkaConnector` CR) · Debezium 3.6.2.Final (pg `pgoutput`, sqlserver) · Apache Iceberg Kafka Connect sink 1.11.0 (`DebeziumTransform`) · Apache Polaris 1.7.0 (REST) · Kubeflow spark-operator **2.5.2** (`ScheduledSparkApplication`) · Spark **4.1.0** (`apache/spark:4.1.0-java21-python3`) + `iceberg-spark-runtime-4.1_2.13:1.11.0` · CNPG (e2e fixture) · pyiceberg 0.10 (e2e doğrulama) · helm-unittest · pytest

**Spec:** `docs/specs/2026-09-10-lakehouse-v2-design.md` (§5.1–5.2 ingestion, §6 Silver/bakım, §9 test, §12 korunan bilgi, §13 F2) · **Bulgular:** `docs/plans/2026-09-10-f0-findings.md` → "F1/F2 için bağlayıcı kararlar" (2, 3, 4, 5, 6, 7, 8, 9) + "F1 notu" (Role `resourceNames`, `s3-creds`, `polaris-*` Secret'ları, kök `kustomize.patches`)

## Global Constraints

- Özel imaj YOK, hack YOK. Kod yalnız `glue/jobs/*.py` (Spark driver'ları + `merge_lib.py`) ve mevcut iki bash script'e eklemeler (`bootstrap/bootstrap.sh` helm modu, `test/e2e/*.sh`).
- Sürümler sabit: Debezium `3.6.2.Final`; Iceberg `1.11.0`; spark-operator chart `2.5.2`; Spark `4.1.0` imaj `apache/spark:4.1.0-java21-python3`; `spark.jars.packages=org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0,org.apache.iceberg:iceberg-aws-bundle:1.11.0`; pyiceberg `>=0.10,<0.11`.
- Tek uygulama namespace'i **`lakehouse`**; spark-operator da `lakehouse`'a kurulur (`spark.jobNamespaces: [lakehouse]`), Spark driver SA `spark-operator-spark`.
- Kaynak-DB başına **1 Debezium + 1 sink** (`dbz-<name>`, `sink-<name>`); Secret adı **`<name>-db`** (`username`, `password`) — F1 Role `resourceNames` bu adı okur. Sink credential `polaris-connect` (`credential`), Spark credential `polaris-spark` (`credential`) — F1 `polaris-setup.sh` yazar.
- Debezium korunan ayarlar (spec §12, **değiştirilmez**): `time.precision.mode=connect`, `decimal.handling.mode=precise`, `publication.autocreate.mode=filtered`, `signal.data.collection=<schema>.debezium_signal` + `signal.enabled.channels=source`, `snapshot.mode=initial`, JSON converter `schemas.enable=true`, `tombstones.on.delete=false`, mssql `schema.history.internal.store.only.captured.tables.ddl=false`.
- Sink zorunlu ayarlar (F0 S2): `iceberg.kafka.session.timeout.ms=120000`, `heartbeat.interval.ms=15000`, `max.poll.interval.ms=300000`, `request.timeout.ms=130000`; `iceberg.tables.default-partition-by=day(_cdc.ts)`; `transforms.dbz.cdc.target.pattern=<name>_raw.{table}`; commit aralığı **prod'da set edilmez** (dev e2e'de 30 s).
- Bronze şekli (F0 S2d): iş kolonları + `_cdc{op∈{I,U,D}, ts, offset, source, target, key}`; timestamptz→string (Silver'da açık `CAST`, Spark 4 ANSI); kopyalar olabilir → latest-per-key `ORDER BY _cdc.ts DESC, _cdc.offset DESC`.
- Silver: MoR varsayılan (`write.merge/update/delete.mode=merge-on-read`, `write.distribution-mode=hash`, `format-version=2`, `write.metadata.delete-after-commit.enabled=true`), `PARTITIONED BY (bucket(N, <ilk anahtar>))`; `MERGE … WHEN MATCHED AND op='D' THEN DELETE / WHEN MATCHED THEN UPDATE / WHEN NOT MATCHED AND op<>'D' THEN INSERT`; şema: kolon ekleme + güvenli genişletme (int→bigint, float→double, decimal(p,s)→decimal(p',s) p'≥p), aksi **fail-loud**.
- Bakım (spec §6): `rewrite_position_delete_files` saatlik; `rewrite_data_files` 6 saatte bir (`delete-file-threshold=5`, `remove-dangling-deletes=true`, `partial-progress.enabled=true`); günlük `expire_snapshots` + `remove_orphan_files older_than 3d` + Bronze `DELETE WHERE _cdc.ts < now()-30d`.
- Küçük tier (kind/CI): `spark.sql.shuffle.partitions=8`, `spark.default.parallelism=8`, `memoryOverhead 512m`, `spark.jars.ivy=/tmp/.ivy2` (F0 S4).
- Testler: `glue` helm-unittest (render değerleri), `glue/jobs` pytest (saf fonksiyonlar), e2e = kind'da gerçek pg yolu (lokal Podman + GitHub Actions aynı `test/e2e/run.sh`). Sqlserver şablonu **yalnız render** ile doğrulanır (kind'da SQL Server yok; canlı doğrulama pre-ship OpenShift, F1 notu 12 gibi).
- Commit mesajı sonu: `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`. Dal: `v2`. Shell komutlarında `--verify` bayrağı kullanılmaz (hook).

## Spec'e göre kararlar (plan yazarının ruling'leri — uygulayıcı sorgulamaz, yorumcu bunlara göre değerlendirir)

| # | Karar | Gerekçe |
|---|---|---|
| P1 | Spec §4 `pipelines/` dizini **yok**; connector şablonları `glue/templates/connectors.yaml`, veri `values.sources[]` | F1 zaten ACL'leri `sources[]`'tan türetiyor; tek doğruluk kaynağı, ArgoCD tarafından uygulanır, "örnek dosya" kopyası tutulmaz |
| P2 | Spec §4 `jobs/` → **`glue/jobs/`** | Helm `.Files` chart dizini dışını okuyamaz; kod ConfigMap'e chart'tan girer. pytest `glue/jobs/tests/` |
| P3 | `pipelines.yaml` → ConfigMap'te **`pipelines.json`** (`toJson`) | Spark imajında PyYAML garanti değil; stdlib `json` yeter |
| P4 | Spark Polaris credential'ı: `driver.env POLARIS_CREDENTIAL ← Secret polaris-spark` → job `SparkSession.builder.config("spark.sql.catalog.lakehouse.credential", …)` | sparkConf düz metin; F0'da `sed` ile enjekte edilmişti (hack). Builder conf SparkConf'a girer → executor'lara da yayılır |
| P5 | Watermark: Silver `TBLPROPERTIES('lakehouse.bronze.snapshot-id')`; okuma `start-snapshot-id/end-snapshot-id` (yalnız append snapshot'ları; delete/replace atlanır), hata → `snapshot-id=cur` ile tam okuma | Spec §6 "snapshot-id watermark"; MERGE idempotent olduğu için tam okuma güvenli |
| P6 | Silver doğrulaması e2e'de **pyiceberg Job** ile (Trino F4) | F0 karar 9: doğrulama küme içinde |
| P7 | Connector/SSA CR'ları glue içinde **sync-wave 3** | Connect (1) ve Keycloak realm (2) sonrası; KafkaConnector health Strimzi yerleşik denetimiyle |
| P8 | Sink `topics.regex` = `<prefix>\.(?!dlq$).*` | Aynı prefix'li DLQ topic'ini sink'in kendisi tüketmesin |
| P9 | Dev'de zamanlanmış merge 30 dk / bakım günlük; e2e işleri **SSA `template`'inden tek seferlik `SparkApplication`** üretip koşturur | Deterministik e2e; kod tekrarı yok |
| P10 | Casting: `pipelines[].casts: {col: type}` açık (ör. `updated_at: timestamp`); verilmeyen kolon Bronze tipiyle Silver'a geçer | Bronze'da timestamptz string gelir (S2d); hangi string'in zaman olduğu bilinemez → deklaratif |

---

## Dosya yapısı

```
glue/jobs/merge_lib.py                  saf fonksiyonlar: pipelines parse, silver adı, DDL, dedup SELECT, MERGE, şema planı (pytest)
glue/jobs/merge_cdc.py                  Spark driver: pipelines.json → her pipeline için watermark/artımlı okuma/DDL/MERGE
glue/jobs/iceberg_maintenance.py        Spark driver: --mode position-deletes | compact | expire-orphan-ttl
glue/jobs/tests/test_merge_lib.py       pytest (Spark'sız)
glue/jobs/tests/conftest.py             sys.path
glue/templates/connectors.yaml          sources[] → KafkaConnector dbz-<name> + sink-<name>  (pg | sqlserver)
glue/templates/spark-jobs.yaml          ConfigMap lakehouse-jobs (jobs/*.py + pipelines.json) + 4 ScheduledSparkApplication
glue/templates/kafka-connect.yaml       Role resourceNames += connect (+ s3-creds STS'siz modda)
glue/templates/kafka-users.yaml         topicPrefix | default name
glue/values.yaml                        connect.topicPartitions/sinkCommitIntervalMs, iceberg.*, s3.*, sources[] şeması, pipelines[], spark.*
glue/tests/connectors_test.yaml, spark_test.yaml
platform/apps/00-spark-operator.yaml    kubeflow spark-operator 2.5.2 (wave 0) + apps/kustomization.yaml
platform/values/glue-dev.yaml           sources: shop (demo-pg), pipelines: shop_raw.orders, küçük Spark, commit 30 s
platform/values/glue.yaml               s3.endpoint/iceberg placeholder yorumları
bootstrap/bootstrap.sh                  helm modu: spark-operator kurulumu
test/e2e/run.sh                         app listesine spark-operator; sonunda pg-path.sh
test/e2e/pg-path.sh                     fixture → connector Ready → Bronze → merge → UPDATE/DELETE → merge → bakım → "E2E F2 OK"
test/e2e/pg-fixture.yaml                Secret shop-db + CNPG demo-pg (wal_level logical, dbz rolü, orders + debezium_signal)
test/e2e/verify/verify.py, job.yaml     pyiceberg tablo doğrulama (satır sayısı + beklenen/olmayan satırlar; --wait)
.github/workflows/e2e.yaml              teşhis: kafkaconnector/sparkapplication durumları
runbooks/add-source.md, add-table.md    kaynak/tablo ekleme adımları (sinyal tablosu, execute-snapshot incremental)
runbooks/install.md                     spark-operator, <source>-db Secret'ları, pipelines
docs/plans/2026-09-10-f0-findings.md    "F2 notu" (canlı bulgular)
```

---

### Task 1: `glue/jobs/merge_lib.py` — saf fonksiyonlar (TDD, Spark'sız)

**Files:**
- Create: `glue/jobs/merge_lib.py`, `glue/jobs/tests/conftest.py`, `glue/jobs/tests/test_merge_lib.py`, `glue/jobs/requirements-dev.txt`

**Interfaces:**
- Produces (Task 2 kullanır): `Pipeline` dataclass (`bronze, keys, write_mode, bucket_count, casts, silver`), `parse_pipelines(doc) -> list[Pipeline]`, `silver_name(bronze) -> str`, `business_columns(fields)`, `silver_columns(bronze_fields, casts)`, `create_silver_sql(silver, columns, keys, write_mode, bucket_count)`, `dedup_select_sql(source_view, columns, keys, casts)`, `merge_sql(silver, inc_view, columns, keys)`, `plan_schema_changes(silver, silver_fields, target_fields) -> list[str]`, `can_widen(src, dst)`, `SchemaConflict`. Alan listeleri `list[tuple[str, str]]` = `(kolon, Spark simpleString tipi)`; `_cdc` kolonu `CDC_COL`, silme `DELETE_OP="D"`.

- [ ] **Step 1: Testleri yaz (BAŞARISIZ)**

```bash
mkdir -p glue/jobs/tests
cat > glue/jobs/requirements-dev.txt <<'EOF'
pytest==8.4.1
EOF
cat > glue/jobs/tests/conftest.py <<'EOF'
import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
EOF
cat > glue/jobs/tests/test_merge_lib.py <<'EOF'
import pytest
import merge_lib as ml

B = [("id", "bigint"), ("status", "string"), ("amount", "decimal(38,2)"), ("updated_at", "string"),
     ("_cdc", "struct<op:string,ts:timestamp,offset:bigint,source:string,target:string,key:struct<id:bigint>>")]
CASTS = {"updated_at": "timestamp"}


def test_parse_pipelines_defaults_and_validation():
    ps = ml.parse_pipelines({"pipelines": [{"bronze": "shop_raw.orders", "keys": ["id"]}]})
    assert ps[0].write_mode == "merge-on-read" and ps[0].bucket_count == 16 and ps[0].casts == {}
    with pytest.raises(ValueError):
        ml.parse_pipelines({"pipelines": [{"bronze": "x_raw.t"}]})            # keys yok
    with pytest.raises(ValueError):
        ml.parse_pipelines({"pipelines": [{"bronze": "x_raw.t", "keys": ["a"], "write_mode": "cow"}]})
    assert ml.parse_pipelines({}) == []


def test_silver_name_strips_raw_suffix():
    assert ml.silver_name("shop_raw.orders") == "shop.orders"
    with pytest.raises(ValueError):
        ml.silver_name("shop.orders")


def test_silver_columns_drop_cdc_and_apply_casts():
    cols = ml.silver_columns(B, CASTS)
    assert cols == [("id", "bigint"), ("status", "string"), ("amount", "decimal(38,2)"), ("updated_at", "timestamp")]
    with pytest.raises(ValueError):
        ml.silver_columns(B, {"nope": "int"})


def test_create_silver_sql():
    sql = ml.create_silver_sql("lakehouse.shop.orders", ml.silver_columns(B, CASTS), ["id"], "merge-on-read", 4)
    assert sql.startswith("CREATE TABLE IF NOT EXISTS lakehouse.shop.orders (`id` bigint, `status` string, `amount` decimal(38,2), `updated_at` timestamp) USING iceberg")
    assert "PARTITIONED BY (bucket(4, `id`))" in sql
    for prop in ("'format-version'='2'", "'write.merge.mode'='merge-on-read'", "'write.update.mode'='merge-on-read'",
                 "'write.delete.mode'='merge-on-read'", "'write.distribution-mode'='hash'",
                 "'write.metadata.delete-after-commit.enabled'='true'"):
        assert prop in sql
    with pytest.raises(ValueError):
        ml.create_silver_sql("s", ml.silver_columns(B, {}), ["missing"], "merge-on-read", 4)


def test_dedup_select_sql_latest_per_key_with_cast():
    sql = ml.dedup_select_sql("bronze_inc", ml.silver_columns(B, CASTS), ["id"], CASTS)
    assert "CAST(`updated_at` AS timestamp) AS `updated_at`" in sql
    assert "row_number() OVER (PARTITION BY `id` ORDER BY _cdc.ts DESC, _cdc.offset DESC) AS __rn" in sql
    assert sql.rstrip().endswith("WHERE __rn = 1") and "_cdc.op AS __op" in sql


def test_merge_sql_delete_update_insert():
    sql = ml.merge_sql("lakehouse.shop.orders", "inc", ml.silver_columns(B, CASTS), ["id"])
    assert sql.startswith("MERGE INTO lakehouse.shop.orders t USING inc s ON t.`id` = s.`id`")
    assert "WHEN MATCHED AND s.__op = 'D' THEN DELETE" in sql
    assert "WHEN MATCHED THEN UPDATE SET `status` = s.`status`, `amount` = s.`amount`, `updated_at` = s.`updated_at`" in sql
    assert "WHEN NOT MATCHED AND s.__op <> 'D' THEN INSERT (`id`, `status`, `amount`, `updated_at`) VALUES (s.`id`, s.`status`, s.`amount`, s.`updated_at`)" in sql


def test_merge_sql_composite_key():
    sql = ml.merge_sql("s", "inc", [("a", "int"), ("b", "int"), ("v", "string")], ["a", "b"])
    assert "ON t.`a` = s.`a` AND t.`b` = s.`b`" in sql and "UPDATE SET `v` = s.`v`" in sql


def test_can_widen_safe_map():
    assert ml.can_widen("int", "bigint") and ml.can_widen("float", "double") and ml.can_widen("smallint", "int")
    assert ml.can_widen("decimal(10,2)", "decimal(38,2)")
    assert not ml.can_widen("bigint", "int")
    assert not ml.can_widen("decimal(10,2)", "decimal(10,3)")
    assert not ml.can_widen("string", "timestamp")
    assert ml.can_widen("string", "string")


def test_plan_schema_changes_add_widen_conflict():
    silver = [("id", "bigint"), ("status", "string"), ("amount", "decimal(10,2)")]
    target = [("id", "bigint"), ("status", "string"), ("amount", "decimal(38,2)"), ("note", "string")]
    ddl = ml.plan_schema_changes("lakehouse.shop.orders", silver, target)
    assert ddl == ["ALTER TABLE lakehouse.shop.orders ALTER COLUMN `amount` TYPE decimal(38,2)",
                   "ALTER TABLE lakehouse.shop.orders ADD COLUMN `note` string"]
    with pytest.raises(ml.SchemaConflict):
        ml.plan_schema_changes("s", [("id", "bigint")], [("id", "string")])
    # Silver'da olup Bronze'da olmayan kolon dokunulmaz
    assert ml.plan_schema_changes("s", [("id", "bigint"), ("legacy", "string")], [("id", "bigint")]) == []
EOF
python3 -m venv .venv >/dev/null 2>&1 || true; .venv/bin/pip install -q -r glue/jobs/requirements-dev.txt
.venv/bin/python -m pytest glue/jobs/tests -q 2>&1 | tail -3   # beklenen: ModuleNotFoundError merge_lib
```

- [ ] **Step 2: `merge_lib.py`**

```bash
cat > glue/jobs/merge_lib.py <<'EOF'
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
EOF
.venv/bin/python -m pytest glue/jobs/tests -q 2>&1 | tail -3   # beklenen: 9 passed
```

- [ ] **Step 3: Commit**

```bash
git add glue/jobs && git commit -m "feat(jobs): merge_lib — pipelines parse, Silver DDL, dedup/MERGE SQL, safe schema widening (pytest)

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 2: `merge_cdc.py` + `iceberg_maintenance.py` (Spark driver'ları)

**Files:**
- Create: `glue/jobs/merge_cdc.py`, `glue/jobs/iceberg_maintenance.py`
- Modify: `glue/jobs/requirements-dev.txt` (+ `pyflakes==3.4.0`)

**Interfaces:**
- Consumes: `merge_lib` (Task 1); env `POLARIS_CREDENTIAL` (Secret'tan), `LAKEHOUSE_CATALOG` (varsayılan `lakehouse`), `PIPELINES_FILE` (varsayılan `/opt/job/pipelines.json`); sparkConf'ta katalog ayarları (credential HARİÇ — Task 3 şablonu).
- Produces: driver çıktısında `MERGE_OK` / `MAINT_OK` (e2e grep'ler); Silver `TBLPROPERTIES` `lakehouse.bronze.snapshot-id`; `iceberg_maintenance.py --mode position-deletes|compact|expire-orphan-ttl [--snapshot-days 7] [--orphan-days 3] [--bronze-ttl-days 30]`.
- Spark'sız test edilemez; e2e (Task 5) çalıştırır. Kontrol: `python3 -m py_compile` + `pyflakes`.

- [ ] **Step 1: `merge_cdc.py`**

```bash
cat > glue/jobs/merge_cdc.py <<'EOF'
"""merge_cdc — Bronze(_cdc) -> Silver MERGE (ScheduledSparkApplication silver-merge). Spark 4.1 / Iceberg 1.11 / Polaris REST.
Watermark: Silver TBLPROPERTIES 'lakehouse.bronze.snapshot-id' -> artımlı okuma (yalnız append snapshot'ları); yoksa/okunamazsa
tam okuma (snapshot-id=cur). MERGE idempotent (anahtar başına son durum) -> yeniden işleme güvenli. Bronze yoksa (henüz veri gelmedi) atlanır."""
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import merge_lib as ml  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql.utils import AnalysisException  # noqa: E402

WM_PROP = "lakehouse.bronze.snapshot-id"
CATALOG = os.environ.get("LAKEHOUSE_CATALOG", "lakehouse")
PIPELINES = os.environ.get("PIPELINES_FILE", "/opt/job/pipelines.json")


def session(app: str) -> SparkSession:
    b = SparkSession.builder.appName(app)
    cred = os.environ.get("POLARIS_CREDENTIAL")          # Secret -> env; YAML'a girmez (plan P4)
    if cred:
        b = b.config(f"spark.sql.catalog.{CATALOG}.credential", cred)
    return b.getOrCreate()


def fields(spark, table):
    return [(f.name, f.dataType.simpleString()) for f in spark.table(table).schema.fields]


def table_exists(spark, table) -> bool:
    try:
        spark.table(table)
        return True
    except AnalysisException:
        return False


def current_snapshot(spark, table):
    rows = spark.sql(f"SELECT snapshot_id FROM {table}.snapshots ORDER BY committed_at DESC LIMIT 1").collect()
    return int(rows[0][0]) if rows else None


def watermark(spark, silver):
    rows = spark.sql(f"SHOW TBLPROPERTIES {silver} ('{WM_PROP}')").collect()
    val = rows[0]["value"] if rows else None
    return int(val) if val and str(val).isdigit() else None


def load_bronze(spark, bronze, wm, cur):
    """(df, mod). Artımlı: (wm, cur] aralığındaki append snapshot'ları; delete/replace snapshot'ları Iceberg atlar.
    Herhangi bir hata (snapshot expire edilmiş, overwrite vb.) -> cur snapshot'ının tam okuması."""
    if wm is not None:
        try:
            df = spark.read.format("iceberg").option("start-snapshot-id", wm).option("end-snapshot-id", cur).load(bronze)
            df.schema  # plan tetikle (hata varsa burada çıkar)
            return df, "incremental"
        except Exception as e:  # noqa: BLE001
            print(f"[{bronze}] artımlı okuma başarısız ({type(e).__name__}: {str(e)[:160]}) -> tam okuma")
    return spark.read.format("iceberg").option("snapshot-id", cur).load(bronze), "full"


def with_commit_retry(fn, tries=3):
    for i in range(tries):
        try:
            return fn()
        except Exception as e:  # noqa: BLE001
            msg = f"{type(e).__name__}: {e}".lower()
            if i == tries - 1 or not ("commit" in msg or "conflict" in msg):
                raise
            print(f"commit çakışması, {5 * (i + 1)} s sonra tekrar ({i + 1}/{tries})")
            time.sleep(5 * (i + 1))


def run_pipeline(spark, p: ml.Pipeline) -> None:
    bronze = f"{CATALOG}.{p.bronze}"
    silver = f"{CATALOG}.{p.silver or ml.silver_name(p.bronze)}"
    if not table_exists(spark, bronze):
        print(f"[{p.bronze}] Bronze yok (henüz veri gelmedi) — atlandı")
        return
    cur = current_snapshot(spark, bronze)
    if cur is None:
        print(f"[{p.bronze}] Bronze'da snapshot yok — atlandı")
        return
    cols = ml.silver_columns(fields(spark, bronze), p.casts)
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {silver.rsplit('.', 1)[0]}")
    if not table_exists(spark, silver):
        ddl = ml.create_silver_sql(silver, cols, p.keys, p.write_mode, p.bucket_count)
        print(ddl)
        spark.sql(ddl)
        wm = None
    else:
        for ddl in ml.plan_schema_changes(silver, ml.business_columns(fields(spark, silver)), cols):
            print(ddl)
            spark.sql(ddl)
        wm = watermark(spark, silver)
    if wm == cur:
        print(f"[{p.bronze}] yeni snapshot yok (watermark {cur})")
        return
    df, mode = load_bronze(spark, bronze, wm, cur)
    df.createOrReplaceTempView("bronze_inc")
    spark.sql(ml.dedup_select_sql("bronze_inc", cols, p.keys, p.casts)).createOrReplaceTempView("inc")
    n = spark.table("inc").count()
    with_commit_retry(lambda: spark.sql(ml.merge_sql(silver, "inc", cols, p.keys)))
    spark.sql(f"ALTER TABLE {silver} SET TBLPROPERTIES ('{WM_PROP}'='{cur}')")
    print(f"[{p.bronze}] -> {silver}: {mode}, {n} anahtar, snapshot {wm} -> {cur}")


def main() -> None:
    with open(PIPELINES, encoding="utf-8") as f:
        pipes = ml.parse_pipelines(json.load(f))
    spark = session("silver-merge")
    failed = []
    for p in pipes:
        try:
            run_pipeline(spark, p)
        except Exception as e:  # noqa: BLE001 — bir pipeline diğerlerini engellemesin; sonunda fail-loud
            failed.append(p.bronze)
            print(f"[{p.bronze}] HATA {type(e).__name__}: {e}", file=sys.stderr)
    spark.stop()
    if failed:
        print(f"BAŞARISIZ pipeline'lar: {failed}", file=sys.stderr)
        sys.exit(1)
    print("MERGE_OK")


if __name__ == "__main__":
    main()
EOF
```

- [ ] **Step 2: `iceberg_maintenance.py`**

```bash
cat > glue/jobs/iceberg_maintenance.py <<'EOF'
"""iceberg_maintenance — spec §6 bakım (D(f) deklaratif): 3 ScheduledSparkApplication aynı dosyayı farklı --mode ile koşturur.
  --mode position-deletes   Silver: rewrite_position_delete_files (MoR delete dosyalarını katla)         saatlik
  --mode compact            Silver+Bronze: rewrite_data_files(delete-file-threshold=5, remove-dangling-deletes, partial-progress)  6 saat
  --mode expire-orphan-ttl  Silver+Bronze: expire_snapshots(--snapshot-days) + remove_orphan_files(--orphan-days); Bronze: DELETE _cdc.ts < now-ttl  günlük
Tablolar pipelines.json'dan (bronze + türetilen silver). Var olmayan tablo atlanır (henüz veri gelmemiş olabilir)."""
import argparse
import json
import os
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import merge_lib as ml  # noqa: E402
from merge_cdc import CATALOG, PIPELINES, session, table_exists  # noqa: E402


def ts_days_ago(days: int) -> str:
    return (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")


def call(spark, proc: str, table: str, extra: str = "") -> None:
    sql = f"CALL {CATALOG}.system.{proc}(table => '{table}'{extra})"
    print(sql)
    for r in spark.sql(sql).collect():
        print("  ", r.asDict())


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", required=True, choices=["position-deletes", "compact", "expire-orphan-ttl"])
    ap.add_argument("--snapshot-days", type=int, default=7)
    ap.add_argument("--orphan-days", type=int, default=3)
    ap.add_argument("--bronze-ttl-days", type=int, default=30)
    a = ap.parse_args()
    with open(PIPELINES, encoding="utf-8") as f:
        pipes = ml.parse_pipelines(json.load(f))
    silver = [p.silver or ml.silver_name(p.bronze) for p in pipes]
    bronze = [p.bronze for p in pipes]
    spark = session(f"maint-{a.mode}")
    failed = []
    for t in (silver if a.mode == "position-deletes" else silver + bronze):
        if not table_exists(spark, f"{CATALOG}.{t}"):
            print(f"[{t}] yok — atlandı")
            continue
        try:
            if a.mode == "position-deletes":
                call(spark, "rewrite_position_delete_files", t)
            elif a.mode == "compact":
                call(spark, "rewrite_data_files", t,
                     ", options => map('delete-file-threshold','5','remove-dangling-deletes','true','partial-progress.enabled','true')")
            else:
                call(spark, "expire_snapshots", t, f", older_than => TIMESTAMP '{ts_days_ago(a.snapshot_days)}', retain_last => 1")
                call(spark, "remove_orphan_files", t, f", older_than => TIMESTAMP '{ts_days_ago(a.orphan_days)}'")
                if t in bronze:
                    sql = f"DELETE FROM {CATALOG}.{t} WHERE _cdc.ts < current_timestamp() - INTERVAL {a.bronze_ttl_days} DAYS"
                    print(sql)
                    spark.sql(sql)
        except Exception as e:  # noqa: BLE001
            failed.append(t)
            print(f"[{t}] HATA {type(e).__name__}: {e}", file=sys.stderr)
    spark.stop()
    if failed:
        print(f"BAŞARISIZ: {failed}", file=sys.stderr)
        sys.exit(1)
    print("MAINT_OK")


if __name__ == "__main__":
    main()
EOF
echo "pyflakes==3.4.0" >> glue/jobs/requirements-dev.txt; .venv/bin/pip install -q -r glue/jobs/requirements-dev.txt
.venv/bin/python -m py_compile glue/jobs/merge_cdc.py glue/jobs/iceberg_maintenance.py && .venv/bin/pyflakes glue/jobs/*.py && echo lint-ok
.venv/bin/python -m pytest glue/jobs/tests -q 2>&1 | tail -2
```
Not: `pyflakes` lokalde pyspark yokken import'u sorgulamaz; çıktı boş olmalı.

- [ ] **Step 3: Commit**

```bash
git add glue/jobs && git commit -m "feat(jobs): merge_cdc (snapshot watermark, incremental/full, Silver DDL/MERGE) + iceberg_maintenance (3 modes)

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 3: glue — `sources[]` → KafkaConnector çifti, `pipelines[]` → ConfigMap + 4 ScheduledSparkApplication

**Files:**
- Create: `glue/templates/connectors.yaml`, `glue/templates/spark-jobs.yaml`, `glue/tests/connectors_test.yaml`, `glue/tests/spark_test.yaml`
- Modify: `glue/values.yaml` (yeni anahtarlar), `glue/templates/kafka-connect.yaml` (Role `resourceNames`), `glue/templates/kafka-users.yaml` (`topicPrefix | default .name`)

**Interfaces:**
- Consumes: `glue/jobs/*.py` (Task 1–2; `.Files.Glob "jobs/*.py"`), Secret'lar `<name>-db`, `polaris-connect`, `polaris-spark`, `connect` (KafkaUser), `s3-creds` (STS'siz).
- Produces: `KafkaConnector/dbz-<name>`, `KafkaConnector/sink-<name>` (wave 3); `ConfigMap/lakehouse-jobs` (`merge_cdc.py`, `merge_lib.py`, `iceberg_maintenance.py`, `pipelines.json`); `ScheduledSparkApplication/{silver-merge, maint-position-deletes, maint-compact, maint-expire-orphan-ttl}` (wave 3). values anahtarları aşağıda (Task 4 dev/prod values, Task 5 e2e bunları kullanır).

- [ ] **Step 1: values.yaml — yeni anahtarlar**

`glue/values.yaml`'da `connect:` bloğuna `topicPartitions` ve `sinkCommitIntervalMs` ekle; `sources: []` yorumlarını değiştir; dosya sonuna `iceberg/s3/pipelines/spark` ekle:

```yaml
connect:
  replicas: 1
  buildImage: ""
  buildPushSecret: ""
  topicPartitions: 6            # Debezium topic.creation.default.partitions (dev: 3)
  sinkCommitIntervalMs: ""      # Iceberg sink commit aralığı; BOŞ = upstream 300 s (prod). Yalnız dev/e2e: "30000"
  resources:
    requests: {cpu: 500m, memory: 1536Mi}
    limits: {memory: 2Gi}

# Kaynak DB'ler → KafkaUser ACL'leri + KafkaConnector çifti (dbz-<name>, sink-<name>). Secret: <name>-db (username, password).
# Bronze namespace <name>_raw (Polaris setup.yaml'da yaratılmış olmalı — runbooks/add-source.md)
sources: []
#  - name: shop                       # topic.prefix, slot/publication adı, Secret <name>-db
#    type: postgres                   # postgres | sqlserver
#    host: demo-pg-rw.lakehouse.svc
#    port: 5432
#    database: shop                   # pg: database.dbname; sqlserver: database.names
#    tables: [public.orders]          # table.include.list
#    signalTable: public.debezium_signal   # zorunlu: incremental snapshot sinyal tablosu (kaynakta yaratılır)
#    topicPrefix: shop                # opsiyonel, varsayılan name
#    bronzeNamespace: shop_raw        # opsiyonel, varsayılan <name>_raw
#    sinkTasks: 1
#    extraConfig: {}                  # Debezium'a ek/ezen anahtarlar
#    sinkExtraConfig: {}              # sink'e ek/ezen anahtarlar

iceberg:
  catalogUri: http://polaris.lakehouse.svc:8181/api/catalog
  warehouse: lakehouse

s3:                                   # istemciler (sink, Spark) için; Polaris sunucusu için platform/polaris/setup.yaml
  endpoint: http://minio.lakehouse.svc:9000     # prod: müşteri S3 endpoint'i
  region: us-east-1
  pathStyleAccess: true
  vendedCredentials: true             # STS'siz S3'te false → istemciler s3-creds Secret'ını kullanır, delegation header none (F0 S1c)

# Silver pipeline'ları (yalnız entity/upsert tabloları; append-only tablolar listelenmez). ConfigMap lakehouse-jobs/pipelines.json
pipelines: []
#  - bronze: shop_raw.orders
#    keys: [id]
#    write_mode: merge-on-read         # | copy-on-write (küçük tablo)
#    bucket_count: 16
#    casts: {updated_at: timestamp}    # Bronze'da string gelen timestamptz kolonları (F0 S2d)

spark:
  image: apache/spark:4.1.0-java21-python3
  version: "4.1.0"
  packages: org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0,org.apache.iceberg:iceberg-aws-bundle:1.11.0
  serviceAccount: spark-operator-spark          # spark-operator chart'ının jobNamespaces'ta yarattığı SA
  credentialSecret: polaris-spark               # key: credential (polaris-setup.sh yazar)
  driver:   {cores: 1, memory: 2g, memoryOverhead: 512m}
  executor: {instances: 1, cores: 1, memory: 2g, memoryOverhead: 512m}
  shufflePartitions: 8                          # küçük tier (F0 S4); büyük tier'da 200
  schedules:
    silverMerge: "*/15 * * * *"
    positionDeletes: "0 * * * *"
    compact: "0 */6 * * *"
    expireOrphanTtl: "30 3 * * *"
  maintenance: {snapshotDays: 7, orphanDays: 3, bronzeTtlDays: 30}
```

- [ ] **Step 2: Testler (BAŞARISIZ)**

```bash
cat > glue/tests/connectors_test.yaml <<'EOF'
suite: connectors
templates: [connectors.yaml]
tests:
  - it: no sources renders nothing
    asserts:
      - hasDocuments: {count: 0}
  - it: postgres source renders debezium + sink pair
    set:
      connect.topicPartitions: 3
      sources:
        - {name: shop, type: postgres, host: demo-pg-rw.lakehouse.svc, port: 5432, database: shop, tables: [public.orders, public.items], signalTable: public.debezium_signal}
    asserts:
      - hasDocuments: {count: 2}
      - documentIndex: 0
        equal: {path: metadata.name, value: dbz-shop}
      - documentIndex: 0
        equal: {path: spec.class, value: io.debezium.connector.postgresql.PostgresConnector}
      - documentIndex: 0
        equal: {path: 'spec.config["table.include.list"]', value: "public.orders,public.items"}
      - documentIndex: 0
        equal: {path: 'spec.config["database.password"]', value: "${secrets:lakehouse/shop-db:password}"}
      - documentIndex: 0
        equal: {path: 'spec.config["publication.autocreate.mode"]', value: filtered}
      - documentIndex: 0
        equal: {path: 'spec.config["signal.data.collection"]', value: public.debezium_signal}
      - documentIndex: 0
        equal: {path: 'spec.config["decimal.handling.mode"]', value: precise}
      - documentIndex: 0
        equal: {path: 'spec.config["topic.creation.default.partitions"]', value: "3"}
      - documentIndex: 0
        equal: {path: 'spec.config["slot.name"]', value: debezium_shop}
      - documentIndex: 1
        equal: {path: metadata.name, value: sink-shop}
      - documentIndex: 1
        equal: {path: spec.class, value: org.apache.iceberg.connect.IcebergSinkConnector}
      - documentIndex: 1
        equal: {path: 'spec.config["topics.regex"]', value: 'shop\.(?!dlq$).*'}
      - documentIndex: 1
        equal: {path: 'spec.config["transforms.dbz.cdc.target.pattern"]', value: "shop_raw.{table}"}
      - documentIndex: 1
        equal: {path: 'spec.config["iceberg.tables.default-partition-by"]', value: day(_cdc.ts)}
      - documentIndex: 1
        equal: {path: 'spec.config["iceberg.kafka.session.timeout.ms"]', value: "120000"}
      - documentIndex: 1
        equal: {path: 'spec.config["iceberg.catalog.credential"]', value: "${secrets:lakehouse/polaris-connect:credential}"}
      - documentIndex: 1
        equal: {path: 'spec.config["iceberg.catalog.header.X-Iceberg-Access-Delegation"]', value: vended-credentials}
      - documentIndex: 1
        equal: {path: 'spec.config["errors.deadletterqueue.topic.name"]', value: shop.dlq}
      - documentIndex: 1
        isNull: {path: 'spec.config["iceberg.control.commit.interval-ms"]'}
  - it: dev commit interval and no-STS mode
    set:
      connect.sinkCommitIntervalMs: "30000"
      s3.vendedCredentials: false
      sources:
        - {name: shop, type: postgres, host: h, port: 5432, database: shop, tables: [public.orders], signalTable: public.debezium_signal}
    asserts:
      - documentIndex: 1
        equal: {path: 'spec.config["iceberg.control.commit.interval-ms"]', value: "30000"}
      - documentIndex: 1
        equal: {path: 'spec.config["iceberg.catalog.header.X-Iceberg-Access-Delegation"]', value: none}
      - documentIndex: 1
        equal: {path: 'spec.config["iceberg.catalog.s3.access-key-id"]', value: "${secrets:lakehouse/s3-creds:AWS_ACCESS_KEY_ID}"}
  - it: sqlserver source
    set:
      sources:
        - {name: erp, type: sqlserver, host: mssql.example.com, port: 1433, database: ERP, tables: [dbo.Orders], signalTable: dbo.debezium_signal, extraConfig: {database.encrypt: "true"}}
    asserts:
      - documentIndex: 0
        equal: {path: spec.class, value: io.debezium.connector.sqlserver.SqlServerConnector}
      - documentIndex: 0
        equal: {path: 'spec.config["database.names"]', value: ERP}
      - documentIndex: 0
        equal: {path: 'spec.config["schema.history.internal.kafka.topic"]', value: schema-history.erp}
      - documentIndex: 0
        equal: {path: 'spec.config["schema.history.internal.store.only.captured.tables.ddl"]', value: "false"}
      - documentIndex: 0
        equal: {path: 'spec.config["schema.history.internal.producer.sasl.jaas.config"]', value: "${secrets:lakehouse/connect:sasl.jaas.config}"}
      - documentIndex: 0
        equal: {path: 'spec.config["database.encrypt"]', value: "true"}
  - it: unknown type fails
    set:
      sources: [{name: x, type: oracle, host: h, port: 1, database: d, tables: [a.b], signalTable: a.s}]
    asserts:
      - failedTemplate: {errorPattern: "postgres\\|sqlserver"}
EOF
cat > glue/tests/spark_test.yaml <<'EOF'
suite: spark jobs
templates: [spark-jobs.yaml]
tests:
  - it: configmap carries job code and pipelines.json
    set:
      pipelines: [{bronze: shop_raw.orders, keys: [id], bucket_count: 4, casts: {updated_at: timestamp}}]
    documentSelector: {path: kind, value: ConfigMap}
    asserts:
      - equal: {path: metadata.name, value: lakehouse-jobs}
      - isNotNull: {path: 'data["merge_cdc.py"]'}
      - isNotNull: {path: 'data["merge_lib.py"]'}
      - isNotNull: {path: 'data["iceberg_maintenance.py"]'}
      - matchRegex: {path: 'data["pipelines.json"]', pattern: '"bronze":"shop_raw.orders"'}
      - matchRegex: {path: 'data["pipelines.json"]', pattern: '"casts":\{"updated_at":"timestamp"\}'}
  - it: four scheduled spark applications with schedules and args
    asserts:
      - hasDocuments: {count: 5}
      - documentSelector: {path: metadata.name, value: silver-merge}
        equal: {path: spec.schedule, value: "*/15 * * * *"}
      - documentSelector: {path: metadata.name, value: silver-merge}
        equal: {path: spec.template.mainApplicationFile, value: local:///opt/job/merge_cdc.py}
      - documentSelector: {path: metadata.name, value: maint-compact}
        equal: {path: spec.template.arguments, value: ["--mode", "compact"]}
      - documentSelector: {path: metadata.name, value: maint-expire-orphan-ttl}
        equal: {path: spec.template.arguments, value: ["--mode", "expire-orphan-ttl", "--snapshot-days", "7", "--orphan-days", "3", "--bronze-ttl-days", "30"]}
      - documentSelector: {path: metadata.name, value: maint-position-deletes}
        equal: {path: spec.concurrencyPolicy, value: Forbid}
  - it: credential comes from Secret via env, never from sparkConf
    documentSelector: {path: metadata.name, value: silver-merge}
    asserts:
      - contains:
          path: spec.template.driver.env
          content: {name: POLARIS_CREDENTIAL, valueFrom: {secretKeyRef: {name: polaris-spark, key: credential}}}
      - isNull: {path: 'spec.template.sparkConf["spark.sql.catalog.lakehouse.credential"]'}
      - equal: {path: 'spec.template.sparkConf["spark.jars.packages"]', value: "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0,org.apache.iceberg:iceberg-aws-bundle:1.11.0"}
      - equal: {path: 'spec.template.sparkConf["spark.sql.shuffle.partitions"]', value: "8"}
      - equal: {path: spec.template.image, value: apache/spark:4.1.0-java21-python3}
      - equal: {path: spec.template.driver.serviceAccount, value: spark-operator-spark}
EOF
helm unittest glue 2>&1 | tail -3   # beklenen: HATA (şablon yok)
```

- [ ] **Step 3: `connectors.yaml`**

```bash
cat > glue/templates/connectors.yaml <<'EOF'
{{- /* Kaynak-DB başına 1 Debezium + 1 Iceberg sink (spec §5.2). Ayarlar spec §12 korunan bilgi + F0 S2/S2d/S3. Runtime mutasyon yok. */ -}}
{{- $ns := include "glue.ns" . }}
{{- $rf := index .Values.kafka.config "default.replication.factor" }}
{{- range .Values.sources }}
{{- $prefix := .topicPrefix | default .name }}
{{- $bronzeNs := .bronzeNamespace | default (printf "%s_raw" .name) }}
{{- if not (or (eq .type "postgres") (eq .type "sqlserver")) }}{{ fail (printf "sources[%s].type = %q; postgres|sqlserver olmalı" .name .type) }}{{ end }}
---
apiVersion: kafka.strimzi.io/v1
kind: KafkaConnector
metadata:
  name: dbz-{{ .name }}
  namespace: {{ $ns }}
  labels: {strimzi.io/cluster: connect}
  annotations: {argocd.argoproj.io/sync-wave: "3"}
spec:
  {{- if eq .type "postgres" }}
  class: io.debezium.connector.postgresql.PostgresConnector
  {{- else }}
  class: io.debezium.connector.sqlserver.SqlServerConnector
  {{- end }}
  tasksMax: 1
  config:
    database.hostname: {{ .host | quote }}
    database.port: {{ .port | quote }}
    database.user: ${secrets:{{ $ns }}/{{ .name }}-db:username}
    database.password: ${secrets:{{ $ns }}/{{ .name }}-db:password}
    topic.prefix: {{ $prefix | quote }}
    table.include.list: {{ join "," .tables | quote }}
    snapshot.mode: initial
    signal.data.collection: {{ required (printf "sources[%s].signalTable zorunlu (incremental snapshot)" .name) .signalTable | quote }}
    signal.enabled.channels: source
    time.precision.mode: connect
    decimal.handling.mode: precise
    tombstones.on.delete: "false"
    key.converter: org.apache.kafka.connect.json.JsonConverter
    value.converter: org.apache.kafka.connect.json.JsonConverter
    key.converter.schemas.enable: "true"
    value.converter.schemas.enable: "true"
    topic.creation.default.replication.factor: {{ $rf | quote }}
    topic.creation.default.partitions: {{ $.Values.connect.topicPartitions | quote }}
    {{- if eq .type "postgres" }}
    database.dbname: {{ .database | quote }}
    plugin.name: pgoutput
    slot.name: debezium_{{ .name }}
    publication.name: dbz_{{ .name }}_pub
    publication.autocreate.mode: filtered          # superuser gerekmez; tablo sahibi dbz rolü (runbooks/add-source.md)
    {{- else }}
    database.names: {{ .database | quote }}
    database.encrypt: "false"                      # prod: extraConfig ile "true" (+ database.trustServerCertificate gerekiyorsa)
    schema.history.internal.kafka.topic: schema-history.{{ .name }}
    schema.history.internal.kafka.bootstrap.servers: lakehouse-kafka-bootstrap:9093
    schema.history.internal.store.only.captured.tables.ddl: "false"
    {{- range $c := list "producer" "consumer" }}
    {{- /* Debezium'un kendi Kafka istemcileri worker ayarlarını devralmaz: Connect KafkaUser Secret'ı + Strimzi CA (PEM). Canlı doğrulama: pre-ship (kind'da SQL Server yok) */}}
    schema.history.internal.{{ $c }}.security.protocol: SASL_SSL
    schema.history.internal.{{ $c }}.sasl.mechanism: SCRAM-SHA-512
    schema.history.internal.{{ $c }}.sasl.jaas.config: ${secrets:{{ $ns }}/connect:sasl.jaas.config}
    schema.history.internal.{{ $c }}.ssl.truststore.type: PEM
    schema.history.internal.{{ $c }}.ssl.truststore.location: /opt/kafka/connect-certs/lakehouse-cluster-ca-cert/ca.crt
    {{- end }}
    {{- end }}
    {{- range $k, $v := .extraConfig }}
    {{ $k }}: {{ $v | quote }}
    {{- end }}
---
apiVersion: kafka.strimzi.io/v1
kind: KafkaConnector
metadata:
  name: sink-{{ .name }}
  namespace: {{ $ns }}
  labels: {strimzi.io/cluster: connect}
  annotations: {argocd.argoproj.io/sync-wave: "3"}
spec:
  class: org.apache.iceberg.connect.IcebergSinkConnector
  tasksMax: {{ .sinkTasks | default 1 }}
  config:
    topics.regex: {{ printf "%s\\.(?!dlq$).*" $prefix | quote }}
    key.converter: org.apache.kafka.connect.json.JsonConverter
    value.converter: org.apache.kafka.connect.json.JsonConverter
    key.converter.schemas.enable: "true"
    value.converter.schemas.enable: "true"
    transforms: dbz
    transforms.dbz.type: org.apache.iceberg.connect.transforms.DebeziumTransform
    transforms.dbz.cdc.target.pattern: {{ printf "%s.{table}" $bronzeNs | quote }}
    iceberg.tables.dynamic-enabled: "true"
    iceberg.tables.route-field: _cdc.target
    iceberg.tables.auto-create-enabled: "true"
    iceberg.tables.evolve-schema-enabled: "true"
    iceberg.tables.default-partition-by: day(_cdc.ts)
    iceberg.tables.auto-create-props.write.metadata.delete-after-commit.enabled: "true"
    iceberg.tables.auto-create-props.history.expire.max-snapshot-age-ms: "86400000"
    iceberg.catalog.type: rest
    iceberg.catalog.uri: {{ $.Values.iceberg.catalogUri | quote }}
    iceberg.catalog.warehouse: {{ $.Values.iceberg.warehouse | quote }}
    iceberg.catalog.credential: ${secrets:{{ $ns }}/polaris-connect:credential}
    iceberg.catalog.scope: PRINCIPAL_ROLE:ALL
    iceberg.catalog.io-impl: org.apache.iceberg.aws.s3.S3FileIO
    iceberg.catalog.s3.endpoint: {{ $.Values.s3.endpoint | quote }}
    iceberg.catalog.s3.path-style-access: {{ $.Values.s3.pathStyleAccess | quote }}
    iceberg.catalog.client.region: {{ $.Values.s3.region | quote }}
    {{- if $.Values.s3.vendedCredentials }}
    iceberg.catalog.header.X-Iceberg-Access-Delegation: vended-credentials
    {{- else }}
    iceberg.catalog.header.X-Iceberg-Access-Delegation: none
    iceberg.catalog.s3.access-key-id: ${secrets:{{ $ns }}/s3-creds:AWS_ACCESS_KEY_ID}
    iceberg.catalog.s3.secret-access-key: ${secrets:{{ $ns }}/s3-creds:AWS_SECRET_ACCESS_KEY}
    {{- end }}
    # F0 S2: control consumer flap'ına karşı ZORUNLU (veri yokken put() 60 s bloke -> 45 s session düşer -> "committed to 0 table(s)")
    iceberg.kafka.session.timeout.ms: "120000"
    iceberg.kafka.heartbeat.interval.ms: "15000"
    iceberg.kafka.max.poll.interval.ms: "300000"
    iceberg.kafka.request.timeout.ms: "130000"
    {{- with $.Values.connect.sinkCommitIntervalMs }}
    iceberg.control.commit.interval-ms: {{ . | quote }}
    {{- end }}
    errors.tolerance: all
    errors.log.enable: "true"
    errors.deadletterqueue.topic.name: {{ printf "%s.dlq" $prefix | quote }}
    errors.deadletterqueue.topic.replication.factor: {{ $rf | quote }}
    {{- range $k, $v := .sinkExtraConfig }}
    {{ $k }}: {{ $v | quote }}
    {{- end }}
{{- end }}
EOF
```

- [ ] **Step 4: `spark-jobs.yaml`**

```bash
cat > glue/templates/spark-jobs.yaml <<'EOF'
{{- /* jobs kodu + pipelines.json tek ConfigMap; 4 ScheduledSparkApplication (spec §6). Credential: Secret -> env (plan P4). */ -}}
{{- $ns := include "glue.ns" . }}
{{- $s := .Values.spark }}
apiVersion: v1
kind: ConfigMap
metadata:
  name: lakehouse-jobs
  namespace: {{ $ns }}
  annotations: {argocd.argoproj.io/sync-wave: "3"}
data:
{{ (.Files.Glob "jobs/*.py").AsConfig | indent 2 }}
  pipelines.json: {{ toJson (dict "pipelines" .Values.pipelines) | quote }}
{{- $m := $s.maintenance }}
{{- $jobs := list
  (dict "name" "silver-merge"            "file" "merge_cdc.py"           "schedule" $s.schedules.silverMerge     "args" list)
  (dict "name" "maint-position-deletes"  "file" "iceberg_maintenance.py" "schedule" $s.schedules.positionDeletes "args" (list "--mode" "position-deletes"))
  (dict "name" "maint-compact"           "file" "iceberg_maintenance.py" "schedule" $s.schedules.compact         "args" (list "--mode" "compact"))
  (dict "name" "maint-expire-orphan-ttl" "file" "iceberg_maintenance.py" "schedule" $s.schedules.expireOrphanTtl "args" (list "--mode" "expire-orphan-ttl" "--snapshot-days" (toString $m.snapshotDays) "--orphan-days" (toString $m.orphanDays) "--bronze-ttl-days" (toString $m.bronzeTtlDays))) }}
{{- range $job := $jobs }}
---
apiVersion: sparkoperator.k8s.io/v1beta2
kind: ScheduledSparkApplication
metadata:
  name: {{ $job.name }}
  namespace: {{ $ns }}
  annotations: {argocd.argoproj.io/sync-wave: "3"}
spec:
  schedule: {{ $job.schedule | quote }}
  concurrencyPolicy: Forbid
  successfulRunHistoryLimit: 3
  failedRunHistoryLimit: 3
  template:
    type: Python
    pythonVersion: "3"
    mode: cluster
    image: {{ $s.image }}
    imagePullPolicy: IfNotPresent
    mainApplicationFile: local:///opt/job/{{ $job.file }}
    {{- if $job.args }}
    arguments: {{ toJson $job.args }}
    {{- end }}
    sparkVersion: {{ $s.version | quote }}
    restartPolicy: {type: Never}
    timeToLiveSeconds: 86400
    sparkConf:
      spark.jars.packages: {{ $s.packages | quote }}
      spark.jars.ivy: /tmp/.ivy2
      spark.sql.extensions: org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
      spark.sql.catalog.lakehouse: org.apache.iceberg.spark.SparkCatalog
      spark.sql.catalog.lakehouse.type: rest
      spark.sql.catalog.lakehouse.uri: {{ $.Values.iceberg.catalogUri | quote }}
      spark.sql.catalog.lakehouse.warehouse: {{ $.Values.iceberg.warehouse | quote }}
      spark.sql.catalog.lakehouse.scope: PRINCIPAL_ROLE:ALL
      spark.sql.catalog.lakehouse.io-impl: org.apache.iceberg.aws.s3.S3FileIO
      spark.sql.catalog.lakehouse.s3.endpoint: {{ $.Values.s3.endpoint | quote }}
      spark.sql.catalog.lakehouse.s3.path-style-access: {{ $.Values.s3.pathStyleAccess | quote }}
      spark.sql.catalog.lakehouse.client.region: {{ $.Values.s3.region | quote }}
      spark.sql.catalog.lakehouse.header.X-Iceberg-Access-Delegation: {{ ternary "vended-credentials" "none" $.Values.s3.vendedCredentials }}
      spark.sql.defaultCatalog: lakehouse
      spark.sql.shuffle.partitions: {{ $s.shufflePartitions | quote }}
      spark.default.parallelism: {{ $s.shufflePartitions | quote }}
      spark.driver.memoryOverhead: {{ $s.driver.memoryOverhead }}
      spark.executor.memoryOverhead: {{ $s.executor.memoryOverhead }}
    driver:
      cores: {{ $s.driver.cores }}
      memory: {{ $s.driver.memory }}
      serviceAccount: {{ $s.serviceAccount }}
      env:
      - {name: AWS_REGION, value: {{ $.Values.s3.region | quote }}}
      - {name: POLARIS_CREDENTIAL, valueFrom: {secretKeyRef: {name: {{ $s.credentialSecret }}, key: credential}}}
      {{- if not $.Values.s3.vendedCredentials }}
      - {name: AWS_ACCESS_KEY_ID, valueFrom: {secretKeyRef: {name: s3-creds, key: AWS_ACCESS_KEY_ID}}}
      - {name: AWS_SECRET_ACCESS_KEY, valueFrom: {secretKeyRef: {name: s3-creds, key: AWS_SECRET_ACCESS_KEY}}}
      {{- end }}
      volumeMounts: [{name: job, mountPath: /opt/job}]
    executor:
      cores: {{ $s.executor.cores }}
      instances: {{ $s.executor.instances }}
      memory: {{ $s.executor.memory }}
      env:
      - {name: AWS_REGION, value: {{ $.Values.s3.region | quote }}}
      {{- if not $.Values.s3.vendedCredentials }}
      - {name: AWS_ACCESS_KEY_ID, valueFrom: {secretKeyRef: {name: s3-creds, key: AWS_ACCESS_KEY_ID}}}
      - {name: AWS_SECRET_ACCESS_KEY, valueFrom: {secretKeyRef: {name: s3-creds, key: AWS_SECRET_ACCESS_KEY}}}
      {{- end }}
      volumeMounts: [{name: job, mountPath: /opt/job}]
    volumes:
    - name: job
      configMap: {name: lakehouse-jobs}
{{- end }}
EOF
```

- [ ] **Step 5: Role + KafkaUser dokunuşları**

`glue/templates/kafka-connect.yaml` Role `resourceNames` listesine (mevcut `range .Values.sources` + `polaris-connect` satırlarının yanına) ekle:
```yaml
  - connect                       # sqlserver schema-history istemcileri: KafkaUser Secret'ındaki sasl.jaas.config
{{- if not .Values.s3.vendedCredentials }}
  - s3-creds                      # STS'siz S3: sink kendi anahtarlarını okur
{{- end }}
```
`glue/templates/kafka-users.yaml` döngüsünde `"{{ .topicPrefix }}."` → `"{{ .topicPrefix | default .name }}."`.

- [ ] **Step 6: Doğrula + commit**

```bash
helm unittest glue 2>&1 | grep -E "^Tests:|FAIL"        # beklenen: eskiler + yeniler PASS (connectors 5, spark 3)
helm lint glue 2>&1 | tail -2
helm template glue >/dev/null && helm template glue --set 'sources[0].name=shop' --set 'sources[0].type=postgres' --set 'sources[0].host=h' --set 'sources[0].port=5432' --set 'sources[0].database=shop' --set 'sources[0].tables[0]=public.orders' --set 'sources[0].signalTable=public.debezium_signal' | grep -c "kind: KafkaConnector"   # 2
helm template glue -f platform/values/glue.yaml >/dev/null && echo prod-render-ok
git add glue && git commit -m "feat(glue): sources[] -> Debezium+Iceberg sink KafkaConnector pairs; pipelines[] -> jobs ConfigMap + 4 ScheduledSparkApplication

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 4: platform — spark-operator Application, dev/prod values, bootstrap helm modu, runbook'lar

**Files:**
- Create: `platform/apps/00-spark-operator.yaml`, `runbooks/add-source.md`, `runbooks/add-table.md`
- Modify: `platform/apps/kustomization.yaml`, `platform/values/glue-dev.yaml`, `platform/values/glue.yaml`, `bootstrap/bootstrap.sh`, `test/e2e/run.sh` (Application listesi), `.github/workflows/e2e.yaml` (teşhis), `runbooks/install.md`

**Interfaces:**
- Consumes: Task 3 values anahtarları.
- Produces: `Application/spark-operator` (chart `spark-operator` 2.5.2, ns `lakehouse`, `spark.jobNamespaces: [lakehouse]`, wave 0); dev values `sources: [shop]`, `pipelines: [shop_raw.orders]` (Task 5 fixture bu adlarla); helm modunda spark-operator kurulu.

- [ ] **Step 1: Application + kustomization**

```bash
cat > platform/apps/00-spark-operator.yaml <<'EOF'
# Kubeflow spark-operator (spec §3): ScheduledSparkApplication; işler lakehouse ns'inde, driver SA spark-operator-spark
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata: {name: spark-operator, namespace: argocd, annotations: {argocd.argoproj.io/sync-wave: "0"}}
spec:
  project: default
  source:
    repoURL: https://kubeflow.github.io/spark-operator
    chart: spark-operator
    targetRevision: 2.5.2
    helm:
      valuesObject:
        spark:
          jobNamespaces: [lakehouse]
  destination: {server: https://kubernetes.default.svc, namespace: lakehouse}
  syncPolicy:
    automated: {prune: true, selfHeal: true}
    syncOptions: [CreateNamespace=true, ServerSideApply=true]
EOF
python3 - <<'EOF'
p='platform/apps/kustomization.yaml'; s=open(p).read()
assert '00-spark-operator.yaml' not in s
s=s.replace('- 00-keycloak-operator.yaml\n','- 00-keycloak-operator.yaml\n- 00-spark-operator.yaml\n'); open(p,'w').write(s)
EOF
kubectl kustomize platform/envs/dev | grep -c "kind: Application"   # 6
```

- [ ] **Step 2: dev/prod values**

`platform/values/glue-dev.yaml`: mevcut `connect:` bloğuna `topicPartitions: 3` ve `sinkCommitIntervalMs: "30000"` ekle (`buildImage` satırı kalır); mevcut `sources:` satırlarını (`- {name: shop, topicPrefix: shop}`) aşağıdakiyle değiştir; `pipelines` ve `spark` bloklarını ekle:
```yaml
sources:
- name: shop
  type: postgres
  host: demo-pg-rw.lakehouse.svc       # test/e2e/pg-fixture.yaml
  port: 5432
  database: shop
  tables: [public.orders]
  signalTable: public.debezium_signal
pipelines:
- {bronze: shop_raw.orders, keys: [id], bucket_count: 4, casts: {updated_at: timestamp}}
spark:
  driver:   {cores: 1, memory: 1536m, memoryOverhead: 512m}
  executor: {instances: 1, cores: 1, memory: 1g, memoryOverhead: 512m}
  schedules:                            # dev: seyrek; e2e işleri tek seferlik SparkApplication ile koşturur (plan P9)
    silverMerge: "*/30 * * * *"
    positionDeletes: "15 2 * * *"
    compact: "30 2 * * *"
    expireOrphanTtl: "45 2 * * *"
```
`platform/values/glue.yaml` sonuna:
```yaml
# Ingestion (F2): kaynaklar ve pipeline'lar kurulumda doldurulur — runbooks/add-source.md
s3:
  endpoint: https://s3.example.com     # müşteri S3 endpoint'i (platform/polaris/setup.yaml ile aynı)
  vendedCredentials: true              # STS yoksa false (F0 S1c)
sources: []
pipelines: []
```

- [ ] **Step 3: bootstrap helm modu + e2e Application listesi + teşhis**

`bootstrap/bootstrap.sh` helm bloğunda `kubectl apply -k "$ROOT/platform/keycloak-operator"` satırından sonra:
```bash
  helm repo add spark-operator https://kubeflow.github.io/spark-operator >/dev/null 2>&1 || true; helm repo update spark-operator >/dev/null
  helm upgrade --install spark-operator spark-operator/spark-operator --version 2.5.2 -n lakehouse --set 'spark.jobNamespaces={lakehouse}' --wait --timeout 5m
```
`test/e2e/run.sh`: `for app in strimzi cnpg keycloak-operator glue polaris` → `for app in strimzi cnpg keycloak-operator spark-operator glue polaris`.
`.github/workflows/e2e.yaml` teşhis adımına: `kubectl -n lakehouse get kafkaconnector -o wide || true`, `kubectl -n lakehouse get sparkapplication,scheduledsparkapplication || true`, `for p in $(kubectl -n lakehouse get pods -o name | grep -- -driver); do kubectl -n lakehouse logs "$p" --tail=40 || true; done`.

- [ ] **Step 4: Runbook'lar**

```bash
cat > runbooks/add-source.md <<'EOF'
# Kaynak DB ekleme (pg / mssql)

1. **Kaynakta hazırlık** (DBA):
   - pg: `wal_level=logical`; rol: `CREATE ROLE <u> LOGIN REPLICATION PASSWORD '…'`; yakalanacak tabloların sahibi bu rol olmalı (filtered publication) ya da `ALTER TABLE … OWNER TO <u>`; `GRANT CREATE ON DATABASE <db> TO <u>`;
     sinyal tablosu: `CREATE TABLE <schema>.debezium_signal (id varchar(42) PRIMARY KEY, type varchar(32) NOT NULL, data varchar(2048)); ALTER TABLE <schema>.debezium_signal OWNER TO <u>;`
   - mssql: `EXEC sys.sp_cdc_enable_db; EXEC sys.sp_cdc_enable_table @source_schema='dbo', @source_name='<t>', @role_name=NULL;` her tablo için; sinyal tablosu aynı şemayla; kullanıcıya `db_owner` ya da CDC şemasına SELECT.
2. **Secret** (Git'e girmez): `kubectl -n lakehouse create secret generic <name>-db --from-literal=username=<u> --from-literal=password=<p>`
3. **Polaris namespace'leri**: `platform/polaris/setup.yaml` → `namespaces:` listesine `<name>_raw` ve `<name>` ekle → `runbooks/scripts/polaris-setup.sh` (idempotent).
4. **values**: `platform/values/glue.yaml`
   ```yaml
   sources:
   - {name: <name>, type: postgres|sqlserver, host: <host>, port: <port>, database: <db>, tables: [<schema.t1>, <schema.t2>], signalTable: <schema>.debezium_signal}
   pipelines:
   - {bronze: <name>_raw.<t1>, keys: [<pk>], casts: {<timestamptz_kolon>: timestamp}}
   ```
   Commit + push → ArgoCD `glue` sync: `KafkaUser/connect` ACL'i, `Role/connect-secrets-reader`, `KafkaConnector/dbz-<name>` + `sink-<name>`, `ConfigMap/lakehouse-jobs` güncellenir.
5. **Doğrulama**: `kubectl -n lakehouse get kafkaconnector` (Ready), `kubectl -n lakehouse get kafkaconnector dbz-<name> -o jsonpath='{.status.connectorStatus.tasks[0]}'`; Bronze tablo `<name>_raw.<t>` ilk commit'ten (≤ 5 dk) sonra görünür; sonraki `silver-merge` çalışmasında Silver `<name>.<t>` yaratılır.

DLQ gerçeği: sink `ErrantRecordReporter` uygulamaz → `<prefix>.dlq` yalnız converter/SMT hatalarını alır; yazma hatası task'ı durdurur (izleme F5). Sorun giderme: task FAILED → `…tasks[0].trace`; yeniden başlatma `kubectl -n lakehouse annotate kafkaconnector <ad> strimzi.io/restart=true`; consumer konumu ileri kaldıysa `runbooks/install.md` sorun giderme (offset reset).
EOF
cat > runbooks/add-table.md <<'EOF'
# Var olan kaynağa tablo ekleme

1. Kaynakta tablonun sahibi/izinleri kaynak rolünde (pg: `ALTER TABLE <schema>.<t> OWNER TO <u>`; mssql: `sp_cdc_enable_table`).
2. `platform/values/glue.yaml` → ilgili `sources[].tables` listesine `<schema>.<t>` ekle; entity tablosuysa `pipelines` listesine `{bronze: <name>_raw.<t>, keys: [...]}`. Commit + push → ArgoCD sync (Debezium connector yeniden başlar, `table.include.list` güncellenir; pg'de filtered publication'a tablo otomatik eklenir).
3. Mevcut satırlar için **incremental snapshot** (kaynakta, sinyal tablosuna):
   ```sql
   INSERT INTO <schema>.debezium_signal (id, type, data)
   VALUES (gen_random_uuid()::text, 'execute-snapshot', '{"data-collections": ["<schema>.<t>"], "type": "incremental"}');
   ```
   (mssql: `NEWID()`.) İlerleme: `kubectl -n lakehouse logs deploy/connect-connect | grep -i "incremental snapshot"`. Debezium 3.x'te iki-sinyal tekrarı gerekmez (DBZ-8780 düzeltildi).
4. Bronze `<name>_raw.<t>` otomatik yaratılır; Silver bir sonraki `silver-merge` çalışmasında.
EOF
```
`runbooks/install.md`: "Adım 2" bileşen listesine spark-operator; Adımlar'a "5. Kaynak ekleme: `runbooks/add-source.md`"; sorun giderme'ye "Spark işi FAILED: `kubectl -n lakehouse get sparkapplication`; `kubectl -n lakehouse logs <ad>-driver`" ve "`silver-merge` `SchemaConflict`: Silver kolon tipi güvenli genişletilemiyor → manuel `ALTER TABLE` ya da yeni kolon".

- [ ] **Step 5: Doğrula + commit**

```bash
kubectl kustomize platform/envs/dev >/dev/null && kubectl kustomize platform/envs/prod >/dev/null
helm template glue -f platform/values/glue-dev.yaml | grep -E "kind: (KafkaConnector|ScheduledSparkApplication)" | sort | uniq -c   # 2 + 4
helm template glue -f platform/values/glue.yaml >/dev/null && helm unittest glue 2>&1 | grep -E "^Tests:"
bash -n bootstrap/bootstrap.sh test/e2e/run.sh && shellcheck bootstrap/bootstrap.sh test/e2e/run.sh
git add platform bootstrap test/e2e/run.sh .github runbooks && git commit -m "feat(platform): spark-operator 2.5.2 Application; dev shop source + pipeline; add-source/add-table runbooks

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 5: e2e pg yolu — fixture, doğrulama Job'ı, `pg-path.sh`; lokal + CI yeşil; F2 notu

**Files:**
- Create: `test/e2e/pg-fixture.yaml`, `test/e2e/verify/verify.py`, `test/e2e/verify/job.yaml`, `test/e2e/pg-path.sh`
- Modify: `test/e2e/run.sh` (sonunda `pg-path.sh`), `README.md` ("Şu an" satırı → F2), `docs/plans/2026-09-10-f0-findings.md` (F2 notu)

**Interfaces:**
- Consumes: dev values `sources[shop]`/`pipelines` (Task 4), SSA'lar (Task 3), `polaris-smoke-cred` Secret (F1 run.sh yaratır: connect principal — Bronze/Silver okuma yetkisi var).
- Produces: `run.sh` sonunda `E2E F2 OK`; `verify.py <ns.table> <min_rows> [--wait S] [k=v:k=v ...] [!k=v ...]`.

- [ ] **Step 1: Fixture + doğrulama Job'ı**

```bash
mkdir -p test/e2e/verify
cat > test/e2e/pg-fixture.yaml <<'EOF'
# e2e kaynak DB (F0 10-cnpg.sh'ten): CNPG, wal_level=logical, dbz rolü postInitApplicationSQL'de (managed.roles initdb'den SONRA — S0b),
# orders + debezium_signal tabloları dbz'nin. Secret adı <name>-db (Role resourceNames ile uyumlu).
apiVersion: v1
kind: Secret
metadata: {name: shop-db, namespace: lakehouse}
type: kubernetes.io/basic-auth
stringData: {username: dbz, password: dbz}
---
apiVersion: postgresql.cnpg.io/v1
kind: Cluster
metadata: {name: demo-pg, namespace: lakehouse}
spec:
  instances: 1
  storage: {size: 2Gi}
  postgresql:
    parameters: {wal_level: logical, max_wal_senders: "10", max_replication_slots: "10"}
  bootstrap:
    initdb:
      database: shop
      owner: app
      postInitApplicationSQL:
      - CREATE ROLE dbz LOGIN REPLICATION PASSWORD 'dbz';
      - CREATE TABLE public.orders (id bigserial PRIMARY KEY, status text NOT NULL, amount numeric(10,2) NOT NULL, updated_at timestamptz NOT NULL DEFAULT now());
      - INSERT INTO public.orders (status, amount) VALUES ('new', 10.50), ('paid', 99.99), ('new', 3.00);
      - CREATE TABLE public.debezium_signal (id varchar(42) PRIMARY KEY, type varchar(32) NOT NULL, data varchar(2048));
      - ALTER TABLE public.orders OWNER TO dbz;
      - ALTER TABLE public.debezium_signal OWNER TO dbz;
      - GRANT CREATE ON DATABASE shop TO dbz;
      - GRANT USAGE, CREATE ON SCHEMA public TO dbz;
EOF
cat > test/e2e/verify/verify.py <<'EOF'
"""verify.py <ns.table> <min_rows> [--wait SANİYE] [k=v:k=v ...] ['!k=v' ...]
Polaris'ten pyiceberg ile okur (küme içi Job; F0 karar 9). k=v:k=v -> tüm eşleşen bir satır OLMALI; !k=v -> böyle satır OLMAMALI.
--wait: koşullar sağlanana kadar 15 s aralıkla tekrar dener (sink commit / merge gecikmesi)."""
import collections
import os
import sys
import time

from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchTableError

args = sys.argv[1:]
table, min_rows = args[0], int(args[1])
rest = args[2:]
wait = 0
if rest and rest[0] == "--wait":
    wait = int(rest[1])
    rest = rest[2:]
must = [dict(kv.split("=", 1) for kv in c.split(":")) for c in rest if not c.startswith("!")]
must_not = [dict(kv.split("=", 1) for kv in c[1:].split(":")) for c in rest if c.startswith("!")]
cat = load_catalog("lakehouse", type="rest", uri=os.environ["POLARIS_URI"], warehouse="lakehouse",
                   credential=f"{os.environ['CLIENT_ID']}:{os.environ['CLIENT_SECRET']}", scope="PRINCIPAL_ROLE:ALL",
                   **{"header.X-Iceberg-Access-Delegation": "vended-credentials", "s3.endpoint": os.environ["S3_ENDPOINT"],
                      "s3.path-style-access": "true", "s3.region": "us-east-1"})


def match(row, cond):
    return all(str(row.get(k)) == v for k, v in cond.items())


deadline = time.time() + wait
while True:
    try:
        rows = cat.load_table(table).scan().to_arrow().to_pylist()
    except NoSuchTableError:
        rows = None
    problems = []
    if rows is None:
        problems.append("tablo yok")
    else:
        if len(rows) < min_rows:
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
ops = collections.Counter((r.get("_cdc") or {}).get("op") for r in rows) if rows and "_cdc" in rows[0] else {}
print(f"OK {table}: rows={len(rows)} ops={dict(ops)}")
EOF
cat > test/e2e/verify/job.yaml <<'EOF'
apiVersion: batch/v1
kind: Job
metadata: {name: JOBNAME, namespace: lakehouse}
spec:
  backoffLimit: 0
  ttlSecondsAfterFinished: 900
  template:
    spec:
      restartPolicy: Never
      containers:
      - name: verify
        image: python:3.13-slim
        command: ["/bin/sh","-c"]
        args: ["set -e; pip install -q 'pyiceberg[s3fs,pyarrow]>=0.10,<0.11'; python /work/verify.py VERIFY_ARGS"]
        env:
        - {name: POLARIS_URI, value: http://polaris.lakehouse.svc:8181/api/catalog}
        - {name: S3_ENDPOINT, value: http://minio.lakehouse.svc:9000}
        - {name: CLIENT_ID, valueFrom: {secretKeyRef: {name: polaris-smoke-cred, key: CLIENT_ID}}}
        - {name: CLIENT_SECRET, valueFrom: {secretKeyRef: {name: polaris-smoke-cred, key: CLIENT_SECRET}}}
        volumeMounts: [{name: work, mountPath: /work}]
      volumes:
      - name: work
        configMap: {name: lakehouse-verify}
EOF
```

- [ ] **Step 2: `pg-path.sh` + run.sh kancası**

```bash
cat > test/e2e/pg-path.sh <<'EOF'
#!/usr/bin/env bash
# e2e F2 pg yolu (spec §9): fixture -> connector Ready -> Bronze -> MERGE -> UPDATE/DELETE -> MERGE -> bakım. run.sh sonunda çağrılır.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"; NS=lakehouse
PSQL() { kubectl -n "$NS" exec demo-pg-1 -c postgres -- psql -U postgres -d shop -Atc "$1"; }

verify() {  # verify <ns.table> <min_rows> [ek argümanlar…]  -> küme içi pyiceberg Job
  local name="verify-$(date +%s)-$RANDOM"
  kubectl -n "$NS" create configmap lakehouse-verify --from-file="$ROOT/test/e2e/verify/verify.py" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
  sed -e "s#JOBNAME#$name#" -e "s#VERIFY_ARGS#$*#" "$ROOT/test/e2e/verify/job.yaml" | kubectl apply -f - >/dev/null
  kubectl -n "$NS" wait --for=condition=complete "job/$name" --timeout=900s >/dev/null || { kubectl -n "$NS" logs "job/$name" --tail=40; return 1; }
  kubectl -n "$NS" logs "job/$name" | grep "^OK"
}

run_spark_once() {  # run_spark_once <ScheduledSparkApplication adı> -> template'ten tek seferlik SparkApplication (plan P9)
  local ssa="$1" app="e2e-$1" st=""
  kubectl -n "$NS" get scheduledsparkapplication "$ssa" -o json \
    | jq --arg n "$app" '{apiVersion:"sparkoperator.k8s.io/v1beta2",kind:"SparkApplication",metadata:{name:$n,namespace:.metadata.namespace},spec:.spec.template}' \
    | kubectl apply -f - >/dev/null
  for _ in $(seq 1 120); do
    st=$(kubectl -n "$NS" get sparkapplication "$app" -o jsonpath='{.status.applicationState.state}' 2>/dev/null || true)
    [[ "$st" == "COMPLETED" || "$st" == "FAILED" ]] && break; sleep 10
  done
  kubectl -n "$NS" logs "$app-driver" --tail=200 2>/dev/null | grep -E "MERGE_OK|MAINT_OK|HATA|Exception|->" | tail -15 || true
  [[ "$st" == "COMPLETED" ]] || { echo "SparkApplication $app: $st"; kubectl -n "$NS" describe sparkapplication "$app" | tail -20; return 1; }
  kubectl -n "$NS" delete sparkapplication "$app" --ignore-not-found >/dev/null
}

echo "== fixture (CNPG demo-pg)"
kubectl apply -f "$ROOT/test/e2e/pg-fixture.yaml" >/dev/null
kubectl -n "$NS" wait --for=condition=Ready cluster/demo-pg --timeout=600s
[[ "$(PSQL 'select count(*) from public.orders')" == "3" ]]

echo "== connector'lar"
kubectl -n "$NS" wait kafkaconnector/dbz-shop --for=condition=Ready --timeout=600s
kubectl -n "$NS" wait kafkaconnector/sink-shop --for=condition=Ready --timeout=600s

echo "== Bronze (ilk snapshot: 3 satır op=I)"
verify shop_raw.orders 3 --wait 600 'id=1:status=new' 'id=2:status=paid'

echo "== MERGE #1"
run_spark_once silver-merge
verify shop.orders 3 --wait 60 'id=1:status=new' 'id=2:status=paid' 'id=3:status=new'

echo "== kaynakta UPDATE/DELETE"
PSQL "update public.orders set status='shipped' where id=1; delete from public.orders where id=2;" >/dev/null
verify shop_raw.orders 5 --wait 600 'id=1:status=shipped'

echo "== MERGE #2 (artımlı)"
run_spark_once silver-merge
verify shop.orders 2 --wait 60 'id=1:status=shipped' '!id=2' 'id=3:status=new'

echo "== bakım"
run_spark_once maint-position-deletes
run_spark_once maint-compact
run_spark_once maint-expire-orphan-ttl
verify shop.orders 2 'id=1:status=shipped' '!id=2'
echo "E2E F2 OK"
EOF
chmod +x test/e2e/pg-path.sh
python3 - <<'EOF'
p='test/e2e/run.sh'; s=open(p).read()
assert 'pg-path.sh' not in s
s=s.replace('echo "E2E F1 OK"\n','echo "E2E F1 OK"\n"$ROOT/test/e2e/pg-path.sh"\n'); open(p,'w').write(s)
EOF
bash -n test/e2e/pg-path.sh test/e2e/run.sh && shellcheck test/e2e/pg-path.sh test/e2e/run.sh && echo syntax-ok
.venv/bin/python -c 'import yaml; [list(yaml.safe_load_all(open(f))) for f in ["test/e2e/pg-fixture.yaml","test/e2e/verify/job.yaml"]]; print("yaml ok")'
```
Not: `verify` argümanları `!id=2` gibi tek tırnaklı geçer; `sed` `#` ayraçlı → argümanlarda `#` yok. `run_spark_once` SSA `template`'i `jq` ile alır (jq CI runner'da ve lokalde var). `.venv`'de pyyaml yoksa `pip install pyyaml`.

- [ ] **Step 3: Lokal e2e (gerçek kanıt)**

Seçenek A (mevcut kind `lakehouse` kümesi, ArgoCD modu — dal push'lu olmalı):
```bash
git push origin v2
LOG=/tmp/e2e-f2.log; nohup bash -c "KIND_EXPERIMENTAL_PROVIDER=podman KIND_CLUSTER=lakehouse test/e2e/run.sh --mode argocd --revision $(git rev-parse --short HEAD); echo exit=\$?" > "$LOG" 2>&1 &
# izle: tail -f /tmp/e2e-f2.log — beklenen: "E2E F1 OK" … "E2E F2 OK" exit=0 (spark-operator Application yeni; glue sync'i connector/SSA'ları wave 3'te uygular)
```
Seçenek B (taze küme, helm modu): `KIND_EXPERIMENTAL_PROVIDER=podman kind delete cluster --name lakehouse; KIND_EXPERIMENTAL_PROVIDER=podman KIND_CLUSTER=lakehouse test/e2e/run.sh --mode helm`.
Başarısızlıkta durma noktaları: (a) `dbz-shop` task trace (publication/rol izinleri) → `kubectl -n lakehouse get kafkaconnector dbz-shop -o jsonpath='{.status.connectorStatus.tasks[0].trace}'`; (b) sink `committed to 0 table(s)` → F0 S2 timeout'ları render'da var mı; (c) Spark driver `Py4JJavaError` → `kubectl -n lakehouse logs e2e-silver-merge-driver`; `POLARIS_CREDENTIAL` env geldi mi; (d) `verify` `tablo yok` → Polaris namespace `shop_raw`/`shop` setup.yaml'da var (F1 ✓). Düzelt → aynı komut (idempotent).

- [ ] **Step 4: README + F2 notu + commit + push → CI**

```bash
sed -i '' 's#- Şu an: F1 platform.*#- Şu an: F2 ingestion (pg/mssql CDC → Bronze → Silver MERGE + bakım) — plan `docs/plans/2026-09-11-lakehouse-v2-f2-ingestion.md`; bulgular `docs/plans/2026-09-10-f0-findings.md`#' README.md
```
`docs/plans/2026-09-10-f0-findings.md` sonuna `## F2 notu (tarih)` bölümü (tablo): canlı ölçümler (ilk Bronze commit süresi, merge süresi packages çözümüyle, artımlı okuma `incremental`/`full` hangisi çalıştı, bakım çıktıları), değişen/doğrulanan varsayımlar (P4 credential-env yolu, P5 incremental scan, KafkaConnector ArgoCD health), açık kalanlar (sqlserver şablonu canlı değil — schema-history TLS yolu `/opt/kafka/connect-certs/...` pre-ship'te doğrulanır; Bronze TTL delete snapshot'ının artımlı okumayı etkilememesi 30 gün sonra görülür).
```bash
git add -A && git commit -m "test(e2e): pg path — CNPG fixture, Bronze/Silver pyiceberg verify, one-off Spark runs from SSA templates; F2 note

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
git push origin v2
gh run watch --exit-status "$(gh run list --workflow e2e --branch v2 --limit 1 --json databaseId -q '.[0].databaseId')"
```
CI'da beklenen süre ~25 dk (F1 8 dk + fixture + sink ilk commit 30 s + 5 Spark koşusu × ~1.5 dk packages çözümü).

---

## Self-review

**Spec coverage:** §5.1 ACL döngüsü (F1'de; `topicPrefix|default` düzeltmesi Task 3) · §5.2 pg/mssql şablonu tüm korunan ayarlarla + sink zorunlu ayarlar (Task 3), tablo ekleme runbook + sinyal (Task 4), DLQ gerçeği (runbook) · §6 `pipelines` ConfigMap, `merge_cdc` (Spark DDL, snapshot watermark, latest-per-key, MERGE, şema uzlaştırma fail-loud, commit retry, ANSI CAST), bakım 3 CR (Task 1–3) · §9 e2e pg yolu (fixture → Bronze → merge → UPDATE/DELETE → merge → bakım; Trino F4 → pyiceberg) + helm-unittest + pytest (Task 1, 3, 5) · §12 korunan bilgi (şablon + `merge_lib`) · §13 F2 kapsamı tam; mongo/nginx (F3), Trino (F4) bilinçli dışarıda.

**Placeholder taraması:** kod blokları tam; `install.md`/`f0-findings.md` düzenlemeleri kısa metinle tarif edildi (uygulayıcı yazar) — TBD yok. `example.com` yalnız prod placeholder.

**Tutarlılık:** Secret adları `<name>-db` (Task 3 şablon ↔ Task 5 fixture `shop-db` ↔ F1 Role) · `polaris-connect`/`polaris-spark`/`polaris-smoke-cred` (F1 ↔ Task 3/5) · ConfigMap `lakehouse-jobs` + `pipelines.json` (Task 2 `PIPELINES` ↔ Task 3) · SSA adları `silver-merge`, `maint-position-deletes`, `maint-compact`, `maint-expire-orphan-ttl` (Task 3 ↔ Task 5 `run_spark_once`) · `--mode` değerleri (Task 2 argparse ↔ Task 3 args) · env `POLARIS_CREDENTIAL` (Task 2 ↔ Task 3) · Bronze `shop_raw.orders`/Silver `shop.orders` (dev pipelines ↔ Polaris setup.yaml namespace'leri ↔ Task 5 verify) · Application listesi `spark-operator` (Task 4 apps ↔ run.sh) · `MERGE_OK`/`MAINT_OK` (Task 2 ↔ Task 5 grep).
