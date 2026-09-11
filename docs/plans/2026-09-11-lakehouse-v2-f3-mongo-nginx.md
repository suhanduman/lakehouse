# Lakehouse v2 — F3 Mongo raw-JSON Bronze + nginx (Fluent Bit → Kafka → Iceberg) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `sources[]`'a `type: mongodb` kaynağı eklenince Debezium mongodb connector'ı (ENDS'siz ham envelope) + 5 dakikalık `mongo-bronze` Spark işi Bronze `(_id, _doc, _cdc)` tablosunu yazsın, Silver `(_id, _doc)` mevcut `silver-merge` ile üretilsin; `nginx.enabled=true` ile Fluent Bit ajan config'i → Kafka dış listener → Iceberg sink `nginx_raw.access_log` (`day(ts)`) çalışsın; kind e2e mongo + nginx yolları lokal ve GitHub Actions'ta yeşil olsun.

**Architecture:** F2 çizgisi: her şey `glue` chart şablonu + values; tek kod `glue/jobs/mongo_bronze.py` (+ saf `mongo_lib.py`, pytest). Mongo Bronze şeması pg Bronze ile **aynı `_cdc` struct**'ını taşır → Silver merge için sıfır yeni kod (`pipelines: [{bronze: crm_raw.customers, keys: [_id]}]`). Kafka'dan okuma Spark **batch** (offsets Bronze `TBLPROPERTIES`'te — checkpoint FS'e ihtiyaç yok; resmi Spark imajında s3a yok). nginx yolu **sıfır kod**: Fluent Bit `tail → parser nginx → lua (ts_ms) → kafka`, Strimzi `KafkaTopic` + `KafkaUser fluentbit` + Iceberg sink `TimestampConverter(unix ms)`.

**Tech Stack:** Debezium mongodb 3.6.2.Final (Connect build'de var) · Spark 4.1.0 + `spark-sql-kafka-0-10_2.13:4.1.0` (yalnız mongo-bronze işi) · Iceberg 1.11.0 · Strimzi 1.2 (`KafkaTopic`, `KafkaUser`, dış listener nodeport/route) · Fluent Bit **5.1.2** (`fluent/fluent-bit:5.1.2`) · MongoDB **8.0** (e2e fixture `mongo:8.0`, tek düğümlü replica set) · pyiceberg 0.10 (verify)

**Spec:** `docs/specs/2026-09-10-lakehouse-v2-design.md` §5.3 (MongoDB), §5.4 (nginx), §5.1 (dış listener), §6 (pipelines), §9 (e2e), §13 F3 · **Bulgular:** `docs/plans/2026-09-10-f0-findings.md` (F0 kararlar, F1 notu, **F2 notu**: ACL transactional id'leri, autoRestart, versionAsOf, prefix_listing, e2e yeniden-koşu semantiği, dev MinIO PVC'siz)

## Global Constraints

- Özel imaj YOK, hack YOK. Kod yalnız `glue/jobs/mongo_bronze.py` + `glue/jobs/mongo_lib.py` (pytest) ve e2e script'leri; Fluent Bit config **dosyadır** (`agents/fluent-bit/`), Lua 3 satırlık `record["ts"]` eklemesi config'in parçası.
- Sürümler sabit: Debezium `3.6.2.Final`; Iceberg `1.11.0`; Spark `4.1.0` (`spark-sql-kafka-0-10_2.13:4.1.0`); Fluent Bit `5.1.2`; mongo fixture `mongo:8.0`.
- Tek namespace `lakehouse`. Secret adları: mongo kaynağı `<name>-db` (`username`,`password`) → `mongodb.user/password`; Spark Kafka kimliği `KafkaUser/spark` → Secret `spark` (`sasl.jaas.config`); Fluent Bit kimliği `KafkaUser/fluentbit` → Secret `fluentbit` (`password`); küme CA `lakehouse-cluster-ca-cert` (`ca.crt`).
- Mongo Bronze şeması: `_id string, _doc string, _cdc struct<op string, ts timestamp, offset long, source string, target string, key struct<_id string>>`; `op ∈ {I,U,D}` (Debezium `c/r→I`, `u→U`, `d→D`); `_doc` = `after` (silmede `before` varsa o, yoksa null); `_id` **Kafka key'inden** (`$oid` sarmalayıcısı açılır); tombstone (value null) düşer; `after` null + `op∈{c,u,r}` → karantina `<ns>.<table>__quarantine(_key string, _value string, reason string, ts timestamp, partition int, offset long)`; partition `day(_cdc.ts)`.
- Kafka offset watermark: Bronze `TBLPROPERTIES('lakehouse.kafka.offsets')` = Spark `startingOffsets` JSON'u (`{"<topic>":{"0":n,…}}`); yoksa `earliest`; `endingOffsets=latest`; yazma sonrası özellik güncellenir (arada çökme → yinelenen Bronze satırı → Silver dedup absorbe eder; at-least-once, pg ile aynı).
- Debezium mongodb: `capture.mode=change_streams_update_full`, `snapshot.mode=initial`, JSON converter **`schemas.enable=false`** (tüketici bizim Spark işimiz; sink yok), `tombstones.on.delete=false`, `heartbeat.interval.ms=10000`, `topic.prefix=<name>`, `collection.include.list=<db.coll,…>`, opsiyonel `signal.data.collection` (incremental snapshot).
- nginx: topic `nginx.access` (`KafkaTopic`, partitions values), Fluent Bit `KafkaUser fluentbit` ACL yalnız `nginx.` prefix Write/Describe; sink `iceberg.tables=nginx_raw.access_log`, `value.converter.schemas.enable=false`, SMT `TimestampConverter$Value` (`field=ts`, `unix.precision=milliseconds`, `target.type=Timestamp`), `default-partition-by=day(ts)`; ham IP saklanır (kullanıcı kararı, spec §14).
- Dış listener: `kafka.externalListener=true` → `platform=openshift`: `route`, vanilla: `nodeport` (9094, TLS + SCRAM) — dev'de açık; e2e varlığını doğrular, Fluent Bit e2e'de küme içi `lakehouse-kafka-bootstrap:9093` ile aynı SASL_SSL/SCRAM/PEM yolunu kullanır.
- Testler: helm-unittest (render), pytest (`mongo_lib`), e2e = kind gerçek mongo + nginx yolu (lokal Podman + GitHub Actions aynı `run.sh`). F2 e2e yeniden-koşu kuralları geçerli (taze küme; `sync.revision` beklemesi; tek seferlik SparkApplication önce silinir).
- Commit sonu: `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`. Dal `v2`. Shell komutlarında `--verify` / `--no-verify` metni kullanılmaz (hook).

## Spec'e göre kararlar (plan yazarının ruling'leri)

| # | Karar | Gerekçe |
|---|---|---|
| Q1 | Mongo Bronze `_cdc` **struct**'ı pg Bronze ile aynı (spec'teki `_cdc_op/_cdc_ts` düz kolonlar yerine) | Silver merge `merge_cdc.py` değişmeden çalışır; `pipelines` sözleşmesi tek |
| Q2 | Kafka okuma Spark **batch** (`startingOffsets` Bronze TBLPROPERTIES'ten, `endingOffsets=latest`) — spec'teki `Trigger.AvailableNow` + checkpoint yerine | Structured Streaming checkpoint'i Hadoop FS ister (imajda s3a yok, PVC = ek durum); tablo özelliği watermark'ı F2'nin `snapshot-id` deseniyle aynı; at-least-once + dedup |
| Q3 | Debezium mongo converter'ları `schemas.enable=false` | Tüketici Spark; `payload` sarmalayıcısı olmadan JSON path'ler basit |
| Q4 | Spark→Kafka kimliği ayrı `KafkaUser spark` (Read/Describe yalnız mongo prefix'leri, group prefix `spark-lakehouse`), CA PEM mount | En az yetki; Connect kullanıcısı paylaşılmaz |
| Q5 | nginx zaman damgası: Fluent Bit **lua** `record["ts"] = math.floor(ts*1000)` (epoch ms tam sayı) + sink `TimestampConverter unix/milliseconds` — spec'teki `timestamp_format iso8601` yerine | Fluent Bit iso8601 mikrosaniye (`.000681Z`) üretir; Kafka `TimestampConverter` SimpleDateFormat mikrosaniyeyi milisaniye sanır (yanlış değer). Tam sayı ms kayıpsız ve yerel-bağımsız |
| Q6 | nginx sink `key.converter=StringConverter` (Fluent Bit key göndermez) | JsonConverter null key'de gürültü |
| Q7 | e2e Fluent Bit küme içinde (Deployment + örnek log ConfigMap) dahili `9093` listener'a yazar; dış listener yalnız varlık/port doğrulaması | kind NodePort'a dış erişim host ağına bağlı; SASL_SSL/PEM yolu aynı |
| Q8 | Mongo fixture kullanıcısı e2e'de `root` (dev); runbook Debezium en-az-yetki rollerini verir | Yetki labirenti e2e'yi kırılganlaştırmasın |
| Q9 | `mongo-bronze` SSA yalnız `sources[type=mongodb]` varken; paketler o işe özel (`spark-sql-kafka`) | Diğer işler Kafka jar'ı indirmesin |
| Q10 | §5.5 S3 dosya yükleme örneği (`s3_register_example.py`) **F5**'e (runbook fazı) | Şartname "referans örnek"; F3 kapsamı mongo+nginx |

---

## Dosya yapısı

```
glue/jobs/mongo_lib.py                    saf: key→_id ($oid aç), envelope→(op,ts_ms,after,before), sınıflandırma (bronze|quarantine|drop), offsets JSON
glue/jobs/mongo_bronze.py                 Spark: pipelines.json.mongo → Kafka batch (offset watermark) → Bronze append + karantina + TBLPROPERTIES
glue/jobs/tests/test_mongo_lib.py         pytest
glue/templates/connectors.yaml            type: mongodb dalı (Debezium mongo, sink YOK); nginx sink (values.nginx.enabled)
glue/templates/kafka-users.yaml           KafkaUser spark (mongodb kaynağı varsa), KafkaUser fluentbit (nginx.enabled)
glue/templates/kafka-topics.yaml          KafkaTopic nginx.access (nginx.enabled)
glue/templates/spark-jobs.yaml            pipelines.json.mongo; ScheduledSparkApplication mongo-bronze (Kafka paketi, spark Secret env, CA volume)
glue/values.yaml                          sources[].type mongodb alanları, nginx.*, spark.schedules.mongoBronze, spark.kafkaSecret
glue/tests/mongo_test.yaml, nginx_test.yaml
agents/fluent-bit/fluent-bit.conf, parsers.conf   müşteri ajanı (teslimat dosyası)
runbooks/nginx-agent.md                   ajan kurulumu (bootstrap host, CA, parola, systemd)
runbooks/add-source.md                    mongodb bölümü
platform/values/glue-dev.yaml             crm mongodb kaynağı + pipeline, nginx.enabled, externalListener
platform/polaris/setup.yaml               namespaces += crm_raw, crm
test/e2e/lib.sh                           verify(), run_spark_once() — pg-path/mongo-path/nginx-path ortak
test/e2e/mongo-fixture.yaml, mongo-path.sh          mongo:8.0 rs0 + seed → connector → mongo-bronze → silver-merge → update/delete → tekrar
test/e2e/nginx-fixture.yaml, nginx-path.sh          Fluent Bit Deployment + örnek log → sink → verify nginx_raw.access_log
test/e2e/verify/verify.py                 `~k=substr` (içerir) koşulu
test/e2e/run.sh                           mongo fixture apply; mongo-path.sh, nginx-path.sh çağrıları
.github/workflows/e2e.yaml                teşhis: kafkatopic, fluent-bit / demo-mongo log
docs/plans/2026-09-10-f0-findings.md      "F3 notu"
```

---

### Task 1: `mongo_lib.py` (saf) + `mongo_bronze.py` (Spark)

**Files:**
- Create: `glue/jobs/mongo_lib.py`, `glue/jobs/tests/test_mongo_lib.py`, `glue/jobs/mongo_bronze.py`

**Interfaces:**
- Consumes: `merge_cdc.session`, `merge_cdc.table_exists`, `merge_cdc.tbl_property`, `CATALOG`, `PIPELINES` (F2); env `KAFKA_BOOTSTRAP` (vars. `lakehouse-kafka-bootstrap:9093`), `KAFKA_JAAS` (Secret `spark` → `sasl.jaas.config`), `KAFKA_CA` (vars. `/etc/kafka-ca/ca.crt`), `POLARIS_CREDENTIAL`.
- Produces: `pipelines.json` sözleşmesi `"mongo": [{"topic": "<prefix>.<db>.<coll>", "bronze": "<ns>.<coll>"}]`; Bronze/karantina tabloları; `TBLPROPERTIES lakehouse.kafka.offsets`; çıktı `MONGO_OK`. `mongo_lib`: `mongo_id(key_json: str|None) -> str|None`, `parse_envelope(value_json: str|None) -> dict|None` (`{"op","ts_ms","after","before"}`), `classify(key, value) -> ("drop"|"quarantine"|"bronze", payload)`, `bronze_op(op) -> "I"|"U"|"D"`, `next_offsets(rows: list[(topic,partition,offset)]) -> dict` (Spark `startingOffsets` JSON şekli), `merge_offsets(prev: dict|None, new: dict) -> dict`, `offsets_json(dict) -> str`.

- [ ] **Step 1: Testler (BAŞARISIZ)**

```bash
cat > glue/jobs/tests/test_mongo_lib.py <<'EOF'
import json
import pytest
import mongo_lib as mg

KEY_OID = json.dumps({"id": json.dumps({"$oid": "000000000000000000000001"})})
KEY_STR = json.dumps({"id": json.dumps("cust-42")})
KEY_INT = json.dumps({"id": json.dumps(7)})
AFTER = json.dumps({"_id": {"$oid": "000000000000000000000001"}, "name": "Ada", "tags": ["a", "b"]})


def env(op, after=AFTER, before=None, ts=1757600000000):
    return json.dumps({"op": op, "ts_ms": ts, "after": after, "before": before, "source": {"db": "crm", "collection": "customers"}})


def test_mongo_id_unwraps_oid_and_keeps_scalars():
    assert mg.mongo_id(KEY_OID) == "000000000000000000000001"
    assert mg.mongo_id(KEY_STR) == "cust-42"
    assert mg.mongo_id(KEY_INT) == "7"
    assert mg.mongo_id(None) is None
    assert mg.mongo_id("not json") is None


def test_parse_envelope_shapes():
    e = mg.parse_envelope(env("u"))
    assert e["op"] == "u" and e["ts_ms"] == 1757600000000 and json.loads(e["after"])["name"] == "Ada" and e["before"] is None
    assert mg.parse_envelope(None) is None
    assert mg.parse_envelope("{}") is None            # op yok
    assert mg.parse_envelope("garbage") is None


def test_bronze_op_mapping():
    assert mg.bronze_op("c") == "I" and mg.bronze_op("r") == "I" and mg.bronze_op("u") == "U" and mg.bronze_op("d") == "D"
    with pytest.raises(ValueError):
        mg.bronze_op("x")


def test_classify_paths():
    kind, p = mg.classify(KEY_OID, env("c"))
    assert kind == "bronze" and p["_id"] == "000000000000000000000001" and p["op"] == "I" and json.loads(p["_doc"])["name"] == "Ada"
    kind, p = mg.classify(KEY_OID, env("d", after=None))
    assert kind == "bronze" and p["op"] == "D" and p["_doc"] is None
    kind, p = mg.classify(KEY_OID, env("d", after=None, before=AFTER))
    assert kind == "bronze" and p["op"] == "D" and json.loads(p["_doc"])["name"] == "Ada"
    kind, p = mg.classify(KEY_OID, None)               # tombstone
    assert kind == "drop"
    kind, p = mg.classify(KEY_OID, env("u", after=None))  # after yok ama silme değil -> karantina
    assert kind == "quarantine" and p["reason"] == "after-null"
    kind, p = mg.classify(None, env("c"))              # key yok -> _id yok -> karantina
    assert kind == "quarantine" and p["reason"] == "no-key"
    kind, p = mg.classify(KEY_OID, "garbage")
    assert kind == "quarantine" and p["reason"] == "bad-envelope"


def test_offsets_json_shapes():
    nxt = mg.next_offsets([("crm.crm.customers", 0, 4), ("crm.crm.customers", 0, 9), ("crm.crm.customers", 2, 1)])
    assert nxt == {"crm.crm.customers": {"0": 10, "2": 2}}
    merged = mg.merge_offsets({"crm.crm.customers": {"0": 3, "1": 5}}, nxt)
    assert merged == {"crm.crm.customers": {"0": 10, "1": 5, "2": 2}}
    assert mg.merge_offsets(None, nxt) == nxt
    assert json.loads(mg.offsets_json(merged)) == merged
EOF
.venv/bin/python -m pytest glue/jobs/tests/test_mongo_lib.py -q 2>&1 | tail -2   # beklenen: ModuleNotFoundError mongo_lib
```

- [ ] **Step 2: `mongo_lib.py`**

```bash
cat > glue/jobs/mongo_lib.py <<'EOF'
"""mongo_lib — Debezium MongoDB ham envelope (ENDS'siz, schemas.enable=false) için saf yardımcılar (pytest).
Spec §5.3: _id Kafka KEY'inden ($oid açılır; silmede tek kaynak), _doc = after (silmede before varsa), tombstone düşer,
after null + op∈{c,u,r} -> karantina. Bronze op sözleşmesi pg ile aynı: I/U/D (F0 S2d)."""
from __future__ import annotations

import json

OPS = {"c": "I", "r": "I", "u": "U", "d": "D"}


def _loads(s):
    try:
        return json.loads(s) if s is not None else None
    except (TypeError, ValueError):
        return None


def mongo_id(key_json: str | None) -> str | None:
    """Debezium mongo key: {"id": "<JSON string>"}; JSON string ObjectId için {"$oid": "..."} olabilir."""
    key = _loads(key_json)
    if not isinstance(key, dict) or "id" not in key:
        return None
    ident = key["id"]
    inner = _loads(ident) if isinstance(ident, str) else ident
    if isinstance(inner, dict) and "$oid" in inner:
        return str(inner["$oid"])
    if inner is None:
        return None
    return str(inner) if not isinstance(inner, (dict, list)) else json.dumps(inner, sort_keys=True)


def parse_envelope(value_json: str | None) -> dict | None:
    v = _loads(value_json)
    if not isinstance(v, dict) or "op" not in v:
        return None
    return {"op": v.get("op"), "ts_ms": v.get("ts_ms"), "after": v.get("after"), "before": v.get("before")}


def bronze_op(op: str) -> str:
    if op not in OPS:
        raise ValueError(f"bilinmeyen Debezium op: {op!r}")
    return OPS[op]


def classify(key_json: str | None, value_json: str | None) -> tuple[str, dict]:
    """("drop", {}) tombstone; ("quarantine", {reason}) bozuk/kimliksiz; ("bronze", {_id,_doc,op,ts_ms})."""
    if value_json is None:
        return "drop", {}
    env = parse_envelope(value_json)
    if env is None or env["op"] not in OPS:
        return "quarantine", {"reason": "bad-envelope"}
    _id = mongo_id(key_json)
    if _id is None:
        return "quarantine", {"reason": "no-key"}
    op = bronze_op(env["op"])
    if op != "D" and env["after"] is None:
        return "quarantine", {"reason": "after-null"}
    doc = env["after"] if op != "D" else env["before"]
    return "bronze", {"_id": _id, "_doc": doc, "op": op, "ts_ms": env["ts_ms"]}


def next_offsets(rows) -> dict:
    """[(topic, partition, offset)] -> Spark startingOffsets JSON şekli: bir sonraki okunacak offset (max+1)."""
    out: dict = {}
    for topic, part, off in rows:
        p = out.setdefault(topic, {})
        p[str(part)] = max(p.get(str(part), 0), int(off) + 1)
    return out


def merge_offsets(prev: dict | None, new: dict) -> dict:
    merged = {t: dict(p) for t, p in (prev or {}).items()}
    for t, parts in new.items():
        merged.setdefault(t, {}).update(parts)
    return merged


def offsets_json(offsets: dict) -> str:
    return json.dumps(offsets, sort_keys=True, separators=(",", ":"))
EOF
.venv/bin/python -m pytest glue/jobs/tests -q 2>&1 | tail -1   # 11 + 5 = 16 passed
```

- [ ] **Step 3: `mongo_bronze.py`**

```bash
cat > glue/jobs/mongo_bronze.py <<'EOF'
"""mongo_bronze — Debezium mongo topic'lerinden (ham envelope) Bronze (_id, _doc, _cdc) + karantina. ScheduledSparkApplication 5 dk.
Kafka okuma BATCH: startingOffsets = Bronze TBLPROPERTIES 'lakehouse.kafka.offsets' (yoksa earliest), endingOffsets = latest;
yazma sonrası özellik güncellenir (arada çökme -> yinelenen Bronze satırı -> Silver dedup absorbe eder; at-least-once).
Silver: mevcut silver-merge (pipelines: {bronze: <ns>.<coll>, keys: [_id]}) — Bronze _cdc struct'ı pg ile aynı (plan Q1)."""
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import mongo_lib as mg  # noqa: E402
from merge_cdc import CATALOG, PIPELINES, session, tbl_property  # noqa: E402
from pyspark.sql import Row  # noqa: E402
from pyspark.sql.functions import col, from_unixtime, lit, struct, to_timestamp  # noqa: E402

OFFSETS_PROP = "lakehouse.kafka.offsets"
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "lakehouse-kafka-bootstrap:9093")
CA_PATH = os.environ.get("KAFKA_CA", "/etc/kafka-ca/ca.crt")

BRONZE_DDL = ("CREATE TABLE IF NOT EXISTS {t} (`_id` string, `_doc` string, "
              "`_cdc` struct<op: string, ts: timestamp, offset: bigint, source: string, target: string, key: struct<_id: string>>) "
              "USING iceberg PARTITIONED BY (days(_cdc.ts)) "
              "TBLPROPERTIES ('format-version'='2', 'write.metadata.delete-after-commit.enabled'='true')")
QUAR_DDL = ("CREATE TABLE IF NOT EXISTS {t} (`_key` string, `_value` string, `reason` string, `ts` timestamp, "
            "`partition` int, `offset` bigint) USING iceberg PARTITIONED BY (days(ts)) TBLPROPERTIES ('format-version'='2')")


def kafka_reader(spark, topic, starting):
    opts = {"kafka.bootstrap.servers": BOOTSTRAP, "subscribe": topic, "startingOffsets": starting, "endingOffsets": "latest",
            "kafka.security.protocol": "SASL_SSL", "kafka.sasl.mechanism": "SCRAM-SHA-512",
            "kafka.sasl.jaas.config": os.environ["KAFKA_JAAS"], "kafka.ssl.truststore.type": "PEM",
            "kafka.ssl.truststore.location": CA_PATH, "kafka.group.id": f"spark-lakehouse-mongo-bronze-{topic}",
            "failOnDataLoss": "false"}   # retention ile silinmiş offset -> hata değil, kalan veriden devam
    return spark.read.format("kafka").options(**opts).load()


def run_topic(spark, topic: str, bronze: str) -> None:
    bt, qt = f"{CATALOG}.{bronze}", f"{CATALOG}.{bronze}__quarantine"
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {bt.rsplit('.', 1)[0]}")
    spark.sql(BRONZE_DDL.format(t=bt))
    spark.sql(QUAR_DDL.format(t=qt))
    prev = tbl_property(spark, bt, OFFSETS_PROP)
    starting = prev if prev else "earliest"
    raw = kafka_reader(spark, topic, starting).selectExpr("CAST(key AS STRING) AS k", "CAST(value AS STRING) AS v",
                                                          "topic", "partition", "offset", "timestamp AS kafka_ts")
    rows = raw.collect()   # mikro-batch: 5 dk'lık mongo değişimi driver'a sığar (spec §5.3: ~150 satır kod)
    if not rows:
        print(f"[{topic}] yeni kayıt yok (offsets={starting})")
        return
    bronze_rows, quar_rows = [], []
    for r in rows:
        kind, p = mg.classify(r.k, r.v)
        if kind == "drop":
            continue
        if kind == "quarantine":
            quar_rows.append(Row(_key=r.k, _value=r.v, reason=p["reason"], ts=r.kafka_ts, partition=int(r.partition), offset=int(r.offset)))
            continue
        bronze_rows.append(Row(_id=p["_id"], _doc=p["_doc"], op=p["op"], ts_ms=int(p["ts_ms"] or 0), offset=int(r.offset), source=r.topic))
    if bronze_rows:
        df = spark.createDataFrame(bronze_rows)
        df = df.select(col("_id"), col("_doc"),
                       struct(col("op"), to_timestamp(from_unixtime(col("ts_ms") / 1000)).alias("ts"), col("offset"),
                              col("source"), lit(bronze).alias("target"), struct(col("_id")).alias("key")).alias("_cdc"))
        df.writeTo(bt).append()
    if quar_rows:
        spark.createDataFrame(quar_rows).writeTo(qt).append()
    nxt = mg.merge_offsets(json.loads(prev) if prev else None, mg.next_offsets([(r.topic, r.partition, r.offset) for r in rows]))
    spark.sql(f"ALTER TABLE {bt} SET TBLPROPERTIES ('{OFFSETS_PROP}'='{mg.offsets_json(nxt)}')")
    print(f"[{topic}] -> {bt}: {len(bronze_rows)} bronze, {len(quar_rows)} karantina, offsets {starting} -> {mg.offsets_json(nxt)}")


def main() -> None:
    with open(PIPELINES, encoding="utf-8") as f:
        mongo = (json.load(f) or {}).get("mongo") or []
    spark = session("mongo-bronze")
    failed = []
    for m in mongo:
        try:
            run_topic(spark, m["topic"], m["bronze"])
        except Exception as e:  # noqa: BLE001
            failed.append(m["topic"])
            print(f"[{m['topic']}] HATA {type(e).__name__}: {e}", file=sys.stderr)
    spark.stop()
    if failed:
        print(f"BAŞARISIZ: {failed}", file=sys.stderr)
        sys.exit(1)
    print("MONGO_OK")


if __name__ == "__main__":
    main()
EOF
.venv/bin/python -m py_compile glue/jobs/mongo_bronze.py && .venv/bin/pyflakes glue/jobs/*.py && echo lint-ok
```
Notlar: `days(_cdc.ts)` Spark DDL'de Iceberg transform adı (sink tarafında `day(...)`; aynı partition alanı) — F0 S3 ile uyumlu. `ts_ms` null ise 0 → 1970 (karantina yerine görünür anomali; F5 alarmı). `from_unixtime(ts_ms/1000)` oturum saat dilimi UTC (F2-B) → doğru.

- [ ] **Step 4: Commit**

```bash
git add glue/jobs && git commit -m "feat(jobs): mongo_bronze — Debezium mongo raw envelope -> Bronze (_id,_doc,_cdc) + quarantine, Kafka offsets in TBLPROPERTIES

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 2: glue — `type: mongodb` kaynağı, `KafkaUser spark`, `mongo-bronze` ScheduledSparkApplication

**Files:**
- Modify: `glue/values.yaml`, `glue/templates/connectors.yaml`, `glue/templates/kafka-users.yaml`, `glue/templates/spark-jobs.yaml`
- Create: `glue/tests/mongo_test.yaml`

**Interfaces:**
- Consumes: Task 1 (`pipelines.json.mongo` sözleşmesi, env adları `KAFKA_JAAS`, `KAFKA_CA`, `KAFKA_BOOTSTRAP`).
- Produces: `KafkaConnector/dbz-<name>` (mongodb; sink yok), `KafkaUser/spark` + Secret `spark`, `ScheduledSparkApplication/mongo-bronze`, `pipelines.json` `"mongo": [...]`. Role `connect-secrets-reader` `resourceNames` mongo `<name>-db`'yi mevcut `range .Values.sources` ile zaten kapsar (doğrula, değişiklik yok).

- [ ] **Step 1: values**

`glue/values.yaml` `sources:` yorum bloğuna mongodb örneği; `spark.kafkaSecret`; `spark.schedules.mongoBronze`:
```yaml
#  - name: crm                        # mongodb: topic.prefix; topic'ler <prefix>.<db>.<coll>; Bronze <name>_raw.<coll>
#    type: mongodb
#    connectionString: mongodb://demo-mongo.lakehouse.svc:27017/?replicaSet=rs0   # kimlik bilgisi YOK: Secret <name>-db (username,password)
#    collections: [crm.customers]     # collection.include.list (db.coll)
#    signalCollection: crm.debezium_signal   # opsiyonel: incremental snapshot sinyal koleksiyonu
#    extraConfig: {}
```
```yaml
spark:
  kafkaSecret: spark                 # KafkaUser spark'ın Secret'ı (sasl.jaas.config) — mongo-bronze Kafka okuması
  schedules:
    mongoBronze: "*/5 * * * *"
```

- [ ] **Step 2: Testler (BAŞARISIZ)**

```bash
cat > glue/tests/mongo_test.yaml <<'EOF'
suite: mongodb source
templates: [connectors.yaml, kafka-users.yaml, spark-jobs.yaml]
tests:
  - it: mongodb source renders a Debezium connector and no sink
    template: connectors.yaml
    set:
      connect.buildImage: "ttl.sh/lakehouse-connect-test:24h"
      sources:
        - {name: crm, type: mongodb, connectionString: "mongodb://demo-mongo.lakehouse.svc:27017/?replicaSet=rs0", collections: [crm.customers, crm.orders]}
    asserts:
      - hasDocuments: {count: 1}
      - equal: {path: metadata.name, value: dbz-crm}
      - equal: {path: spec.class, value: io.debezium.connector.mongodb.MongoDbConnector}
      - equal: {path: 'spec.config["mongodb.connection.string"]', value: "mongodb://demo-mongo.lakehouse.svc:27017/?replicaSet=rs0"}
      - equal: {path: 'spec.config["mongodb.user"]', value: "${secrets:lakehouse/crm-db:username}"}
      - equal: {path: 'spec.config["collection.include.list"]', value: "crm.customers,crm.orders"}
      - equal: {path: 'spec.config["capture.mode"]', value: change_streams_update_full}
      - equal: {path: 'spec.config["value.converter.schemas.enable"]', value: "false"}
      - equal: {path: 'spec.config["topic.prefix"]', value: crm}
      - isNull: {path: 'spec.config["signal.data.collection"]'}
  - it: mongodb signal collection when given
    template: connectors.yaml
    set:
      connect.buildImage: "x"
      sources:
        - {name: crm, type: mongodb, connectionString: "mongodb://h/?replicaSet=rs0", collections: [crm.customers], signalCollection: crm.debezium_signal}
    asserts:
      - equal: {path: 'spec.config["signal.data.collection"]', value: crm.debezium_signal}
      - equal: {path: 'spec.config["signal.enabled.channels"]', value: source}
  - it: spark KafkaUser exists only with a mongodb source and reads its topics
    template: kafka-users.yaml
    set:
      connect.buildImage: "x"
      sources:
        - {name: crm, type: mongodb, connectionString: "mongodb://h/?replicaSet=rs0", collections: [crm.customers]}
    asserts:
      - hasDocuments: {count: 2}
      - documentIndex: 1
        equal: {path: metadata.name, value: spark}
      - documentIndex: 1
        contains: {path: spec.authorization.acls, content: {resource: {type: topic, name: "crm.", patternType: prefix}, operations: [Read, Describe]}}
      - documentIndex: 1
        contains: {path: spec.authorization.acls, content: {resource: {type: group, name: "spark-lakehouse", patternType: prefix}, operations: [Read]}}
  - it: no spark KafkaUser without mongodb sources
    template: kafka-users.yaml
    set: {connect.buildImage: "x", sources: [{name: shop, type: postgres, host: h, port: 5432, database: shop, tables: [public.orders], signalTable: public.debezium_signal}]}
    asserts:
      - hasDocuments: {count: 1}
  - it: pipelines.json carries mongo topics and mongo-bronze SSA renders with kafka package and secret
    template: spark-jobs.yaml
    set:
      connect.buildImage: "x"
      sources:
        - {name: crm, type: mongodb, connectionString: "mongodb://h/?replicaSet=rs0", collections: [crm.customers]}
      pipelines: [{bronze: crm_raw.customers, keys: [_id], bucket_count: 4}]
    asserts:
      - documentSelector: {path: kind, value: ConfigMap}
        matchRegex: {path: 'data["pipelines.json"]', pattern: '"mongo":\[\{"bronze":"crm_raw.customers","topic":"crm.crm.customers"\}\]'}
      - documentSelector: {path: metadata.name, value: mongo-bronze}
        equal: {path: spec.schedule, value: "*/5 * * * *"}
      - documentSelector: {path: metadata.name, value: mongo-bronze}
        matchRegex: {path: 'spec.template.sparkConf["spark.jars.packages"]', pattern: 'spark-sql-kafka-0-10_2.13:4.1.0'}
      - documentSelector: {path: metadata.name, value: mongo-bronze}
        contains: {path: spec.template.driver.env, content: {name: KAFKA_JAAS, valueFrom: {secretKeyRef: {name: spark, key: sasl.jaas.config}}}}
      - documentSelector: {path: metadata.name, value: mongo-bronze}
        contains: {path: spec.template.volumes, content: {name: kafka-ca, secret: {secretName: lakehouse-cluster-ca-cert, items: [{key: ca.crt, path: ca.crt}]}}}
      - documentSelector: {path: metadata.name, value: silver-merge}
        notContains: {path: spec.template.driver.env, content: {name: KAFKA_JAAS, valueFrom: {secretKeyRef: {name: spark, key: sasl.jaas.config}}}}
  - it: no mongo-bronze SSA without mongodb sources
    template: spark-jobs.yaml
    set: {connect.buildImage: "x", pipelines: [{bronze: shop_raw.orders, keys: [id]}]}
    asserts:
      - hasDocuments: {count: 5}
EOF
helm unittest glue 2>&1 | tail -3   # beklenen: HATA
```

- [ ] **Step 3: `connectors.yaml` mongodb dalı**

`range .Values.sources` içinde `type` denetimi `postgres|sqlserver|mongodb`; mongodb için `$cfg` farklı kurulur ve **sink render edilmez**. Mevcut pg/sqlserver dbz + sink bloğu `{{- else }}` içine alınır (`{{- end }}` eşleşmesini `helm template` ile doğrula):
```yaml
{{- if eq .type "mongodb" }}
{{- $cfg := dict
  "mongodb.connection.string" .connectionString
  "mongodb.user" (printf "${secrets:%s/%s-db:username}" $ns .name)
  "mongodb.password" (printf "${secrets:%s/%s-db:password}" $ns .name)
  "topic.prefix" $prefix
  "collection.include.list" (join "," .collections)
  "capture.mode" "change_streams_update_full"
  "snapshot.mode" "initial"
  "tombstones.on.delete" "false"
  "heartbeat.interval.ms" "10000"
  "key.converter" "org.apache.kafka.connect.json.JsonConverter"
  "value.converter" "org.apache.kafka.connect.json.JsonConverter"
  "key.converter.schemas.enable" "false"
  "value.converter.schemas.enable" "false"
  "topic.creation.default.replication.factor" $rf
  "topic.creation.default.partitions" $.Values.connect.topicPartitions }}
{{- with .signalCollection }}{{ $_ := set $cfg "signal.data.collection" . }}{{ $_ := set $cfg "signal.enabled.channels" "source" }}{{ end }}
{{- $_ := mergeOverwrite $cfg (.extraConfig | default (dict)) }}
---
apiVersion: kafka.strimzi.io/v1
kind: KafkaConnector
metadata:
  name: dbz-{{ .name }}
  namespace: {{ $ns }}
  labels: {strimzi.io/cluster: connect}
  annotations: {argocd.argoproj.io/sync-wave: "3"}
spec:
  class: io.debezium.connector.mongodb.MongoDbConnector
  tasksMax: 1
  autoRestart: {enabled: true}
  config:
    {{- range $k, $v := $cfg }}
    {{ $k }}: {{ $v | quote }}
    {{- end }}
{{- else }}
… (mevcut postgres/sqlserver dalı + sink, değişmeden)
{{- end }}
```

- [ ] **Step 4: `kafka-users.yaml` — `KafkaUser spark`**

Dosya sonuna (mongodb kaynağı varsa):
```yaml
{{- $mongo := list }}
{{- range .Values.sources }}{{ if eq .type "mongodb" }}{{ $mongo = append $mongo . }}{{ end }}{{ end }}
{{- if $mongo }}
---
# mongo-bronze Spark işinin Kafka kimliği: yalnız mongo prefix'lerini okur (plan Q4)
apiVersion: kafka.strimzi.io/v1
kind: KafkaUser
metadata:
  name: spark
  namespace: {{ include "glue.ns" . }}
  labels: {strimzi.io/cluster: lakehouse}
  annotations: {argocd.argoproj.io/sync-wave: "0"}
spec:
  authentication: {type: scram-sha-512}
  authorization:
    type: simple
    acls:
{{- range $mongo }}
    - {resource: {type: topic, name: "{{ .topicPrefix | default .name }}.", patternType: prefix}, operations: [Read, Describe]}
{{- end }}
    - {resource: {type: group, name: "spark-lakehouse", patternType: prefix}, operations: [Read]}
{{- end }}
```

- [ ] **Step 5: `spark-jobs.yaml` — `pipelines.json.mongo` + `mongo-bronze` SSA**

`$nsList`'in yanına:
```yaml
{{- $mongoList := list }}
{{- range .Values.sources }}{{ if eq .type "mongodb" }}
{{- $bn := .bronzeNamespace | default (printf "%s_raw" .name) }}{{ $pfx := .topicPrefix | default .name }}
{{- range .collections }}{{ $mongoList = append $mongoList (dict "topic" (printf "%s.%s" $pfx .) "bronze" (printf "%s.%s" $bn (last (splitList "." .)))) }}{{ end }}
{{- end }}{{ end }}
```
`pipelines.json`: `toJson (dict "pipelines" .Values.pipelines "bronze_namespaces" $nsList "mongo" $mongoList)`.
`$jobs` listesine (mongodb kaynağı varsa) `(dict "name" "mongo-bronze" "file" "mongo_bronze.py" "schedule" $s.schedules.mongoBronze "args" list "kafka" true)`. SSA şablonunda `$job.kafka` iken: `spark.jars.packages` sonuna `,org.apache.spark:spark-sql-kafka-0-10_2.13:{{ $s.version }}`; driver **ve executor** `env` += `KAFKA_JAAS` (`secretKeyRef {name: {{ $s.kafkaSecret }}, key: sasl.jaas.config}`), `KAFKA_BOOTSTRAP` (`lakehouse-kafka-bootstrap:9093`), `KAFKA_CA` (`/etc/kafka-ca/ca.crt`); `volumeMounts` += `{name: kafka-ca, mountPath: /etc/kafka-ca, readOnly: true}`; `volumes` += `{name: kafka-ca, secret: {secretName: lakehouse-cluster-ca-cert, items: [{key: ca.crt, path: ca.crt}]}}`. SSA döngüsü koşulu: merge/bakım işleri `.Values.pipelines` varken, `mongo-bronze` `$mongoList` varken (iki ayrı koşul; `pipelines` boş ama mongo kaynağı varsa yalnız `mongo-bronze` çıkar).

- [ ] **Step 6: Doğrula + commit**

```bash
helm unittest glue 2>&1 | grep -E "^Tests:|FAIL"
helm template glue -f platform/values/glue-dev.yaml >/dev/null && helm template glue -f platform/values/glue.yaml >/dev/null
helm template glue --set connect.buildImage=x --set 'sources[0].name=crm' --set 'sources[0].type=mongodb' --set 'sources[0].connectionString=mongodb://h/?replicaSet=rs0' --set 'sources[0].collections[0]=crm.customers' | grep -cE "^kind: (KafkaConnector|KafkaUser|ScheduledSparkApplication)"   # 1 dbz + 2 users + 1 SSA = 4
git add glue && git commit -m "feat(glue): mongodb source (Debezium raw envelope, no sink), KafkaUser spark, mongo-bronze ScheduledSparkApplication

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 3: glue nginx yolu — `KafkaTopic`, `KafkaUser fluentbit`, Iceberg sink; Fluent Bit ajan dosyaları + runbook

**Files:**
- Create: `glue/templates/kafka-topics.yaml`, `glue/tests/nginx_test.yaml`, `agents/fluent-bit/fluent-bit.conf`, `agents/fluent-bit/parsers.conf`, `runbooks/nginx-agent.md`
- Modify: `glue/values.yaml` (`nginx:`), `glue/templates/connectors.yaml` (nginx sink, `range` dışında), `glue/templates/kafka-users.yaml` (fluentbit)

**Interfaces:**
- Produces: `KafkaTopic/nginx.access`, `KafkaUser/fluentbit` (+ Secret `fluentbit`: `password`), `KafkaConnector/sink-nginx` → `nginx_raw.access_log`; ajan config'i `agents/fluent-bit/*` (`${KAFKA_BOOTSTRAP}`, `${KAFKA_PASSWORD}`, `${KAFKA_CA}`, `${NGINX_ACCESS_LOG}`, `${READ_FROM_HEAD}` env yer tutucuları). Task 5 e2e aynı dosyaları ConfigMap'e koyar (kopya yok).

- [ ] **Step 1: values**

```yaml
nginx:
  enabled: false                   # Fluent Bit ajanları (müşteri sunucusu) -> Kafka dış listener -> Iceberg sink
  topic: nginx.access
  partitions: 6
  table: nginx_raw.access_log      # Polaris namespace nginx_raw setup.yaml'da (F1'de var)
  sinkTasks: 1
  sinkExtraConfig: {}
```

- [ ] **Step 2: Testler (BAŞARISIZ)**

```bash
cat > glue/tests/nginx_test.yaml <<'EOF'
suite: nginx path
templates: [kafka-topics.yaml, kafka-users.yaml, connectors.yaml]
tests:
  - it: nothing renders when nginx disabled
    template: kafka-topics.yaml
    set: {connect.buildImage: "x"}
    asserts:
      - hasDocuments: {count: 0}
  - it: topic renders when enabled
    template: kafka-topics.yaml
    set: {connect.buildImage: "x", nginx.enabled: true}
    asserts:
      - hasDocuments: {count: 1}
      - equal: {path: metadata.name, value: nginx.access}
      - equal: {path: spec.partitions, value: 6}
      - equal: {path: spec.replicas, value: 3}
  - it: fluentbit user may only write the nginx prefix
    template: kafka-users.yaml
    set: {connect.buildImage: "x", nginx.enabled: true}
    documentSelector: {path: metadata.name, value: fluentbit}
    asserts:
      - equal: {path: spec.authorization.acls, value: [{resource: {type: topic, name: "nginx.", patternType: prefix}, operations: [Write, Describe]}]}
  - it: nginx sink converts ts and partitions by day
    template: connectors.yaml
    set: {connect.buildImage: "x", nginx.enabled: true}
    asserts:
      - hasDocuments: {count: 1}
      - equal: {path: metadata.name, value: sink-nginx}
      - equal: {path: 'spec.config["topics"]', value: nginx.access}
      - equal: {path: 'spec.config["iceberg.tables"]', value: nginx_raw.access_log}
      - equal: {path: 'spec.config["value.converter.schemas.enable"]', value: "false"}
      - equal: {path: 'spec.config["key.converter"]', value: org.apache.kafka.connect.storage.StringConverter}
      - equal: {path: 'spec.config["transforms.ts.type"]', value: org.apache.kafka.connect.transforms.TimestampConverter$Value}
      - equal: {path: 'spec.config["transforms.ts.field"]', value: ts}
      - equal: {path: 'spec.config["transforms.ts.unix.precision"]', value: milliseconds}
      - equal: {path: 'spec.config["transforms.ts.target.type"]', value: Timestamp}
      - equal: {path: 'spec.config["iceberg.tables.default-partition-by"]', value: day(ts)}
      - equal: {path: 'spec.config["iceberg.kafka.session.timeout.ms"]', value: "120000"}
      - equal: {path: 'spec.config["errors.deadletterqueue.topic.name"]', value: nginx.dlq}
      - isNull: {path: 'spec.config["transforms.dbz.type"]'}
EOF
helm unittest glue 2>&1 | tail -3
```

- [ ] **Step 3: Şablonlar**

`glue/templates/kafka-topics.yaml`:
```yaml
{{- if .Values.nginx.enabled }}
# Fluent Bit topic yaratamaz (ACL: yalnız Write) -> sabit KafkaTopic (spec §5.1: KafkaTopic yalnız sabitler için)
apiVersion: kafka.strimzi.io/v1
kind: KafkaTopic
metadata:
  name: {{ .Values.nginx.topic }}
  namespace: {{ include "glue.ns" . }}
  labels: {strimzi.io/cluster: lakehouse}
  annotations: {argocd.argoproj.io/sync-wave: "1"}
spec:
  partitions: {{ .Values.nginx.partitions }}
  replicas: {{ index .Values.kafka.config "default.replication.factor" }}
  config: {retention.ms: "604800000"}   # 7 gün: sink duraklarsa kayıpsız yakalama penceresi
{{- end }}
```
`kafka-users.yaml` sonuna (`nginx.enabled`): `KafkaUser fluentbit` (scram-sha-512, wave 0), ACL yalnız `[{topic prefix "nginx." [Write, Describe]}]`.
`connectors.yaml` sonuna (`range` dışında, `nginx.enabled`): `KafkaConnector sink-nginx` (wave 3, `autoRestart`, `tasksMax .Values.nginx.sinkTasks`), `$nginxCfg` dict: `"topics" .Values.nginx.topic`, `"key.converter" "org.apache.kafka.connect.storage.StringConverter"`, `"value.converter" "org.apache.kafka.connect.json.JsonConverter"`, `"value.converter.schemas.enable" "false"`, `"transforms" "ts"`, `"transforms.ts.type" "org.apache.kafka.connect.transforms.TimestampConverter$Value"`, `"transforms.ts.field" "ts"`, `"transforms.ts.unix.precision" "milliseconds"`, `"transforms.ts.target.type" "Timestamp"`, `"iceberg.tables" .Values.nginx.table`, `"iceberg.tables.auto-create-enabled" "true"`, `"iceberg.tables.evolve-schema-enabled" "true"`, `"iceberg.tables.default-partition-by" "day(ts)"`, `"iceberg.tables.auto-create-props.write.metadata.delete-after-commit.enabled" "true"`, katalog/S3/STS anahtarları + `iceberg.kafka.*` timeout'ları pg sink'iyle **aynı değerler** (kopyala; ortak helper F4), `"errors.tolerance" "all"`, `"errors.log.enable" "true"`, DLQ `nginx.dlq` (RF `$rf`), `mergeOverwrite $nginxCfg .Values.nginx.sinkExtraConfig`.

- [ ] **Step 4: Fluent Bit ajan dosyaları + runbook**

```bash
mkdir -p agents/fluent-bit
cat > agents/fluent-bit/fluent-bit.conf <<'EOF'
# Lakehouse nginx access-log ajanı (Fluent Bit 5.1). Müşteri sunucusuna kurulur; env: KAFKA_BOOTSTRAP, KAFKA_PASSWORD, KAFKA_CA,
# NGINX_ACCESS_LOG, READ_FROM_HEAD (runbooks/nginx-agent.md)
[SERVICE]
    flush            5
    log_level        info
    parsers_file     parsers.conf
    storage.path     /var/lib/fluent-bit/buffer
    storage.sync     normal
    storage.backlog.mem_limit 50M

[INPUT]
    name             tail
    tag              nginx.access
    path             ${NGINX_ACCESS_LOG}
    parser           nginx
    db               /var/lib/fluent-bit/tail.db
    read_from_head   ${READ_FROM_HEAD}
    storage.type     filesystem
    skip_long_lines  on
    refresh_interval 5

# ts = epoch milisaniye (tam sayı): sink TimestampConverter unix/milliseconds ile Timestamp'e çevirir (plan Q5)
[FILTER]
    name   lua
    match  nginx.*
    call   add_ts
    code   function add_ts(tag, ts, record) record["ts"] = math.floor(ts * 1000) return 2, ts, record end

[OUTPUT]
    name                          kafka
    match                         nginx.*
    brokers                       ${KAFKA_BOOTSTRAP}
    topics                        nginx.access
    format                        json
    timestamp_key                 _fb_ts
    rdkafka.security.protocol     SASL_SSL
    rdkafka.sasl.mechanisms       SCRAM-SHA-512
    rdkafka.sasl.username         fluentbit
    rdkafka.sasl.password         ${KAFKA_PASSWORD}
    rdkafka.ssl.ca.location       ${KAFKA_CA}
    rdkafka.request.required.acks 1
    rdkafka.compression.codec     lz4
    queue_full_retries            0
    storage.total_limit_size      1G
EOF
cat > agents/fluent-bit/parsers.conf <<'EOF'
[PARSER]
    Name        nginx
    Format      regex
    Regex       ^(?<remote>[^ ]*) (?<host>[^ ]*) (?<user>[^ ]*) \[(?<time>[^\]]*)\] "(?<method>\S+)(?: +(?<path>[^\"]*?)(?: +\S*)?)?" (?<code>[^ ]*) (?<size>[^ ]*)(?: "(?<referer>[^\"]*)" "(?<agent>[^\"]*)")?$
    Time_Key    time
    Time_Format %d/%b/%Y:%H:%M:%S %z
    Time_Keep   Off
    Types       code:integer size:integer
EOF
cat > runbooks/nginx-agent.md <<'EOF'
# nginx erişim logu ajanı (Fluent Bit 5.1)

Akış: `tail` → `parser nginx` (zaman ayrıştırması ajanda) → `lua` (`ts` = epoch ms) → Kafka `nginx.access` (SASL_SSL/SCRAM, dış listener) → Iceberg sink → `nginx_raw.access_log` (`day(ts)`), ham IP saklanır (şartnamede KVKK maddesi yok; istenirse maskeleme sink SMT'siyle eklenir).

1. Küme tarafı: `platform/values/glue.yaml` → `nginx.enabled: true`, `kafka.externalListener: true` → commit/push → ArgoCD (`KafkaTopic/nginx.access`, `KafkaUser/fluentbit`, `KafkaConnector/sink-nginx`).
2. Bağlantı bilgileri (kümeden):
   - bootstrap: OpenShift `kubectl -n lakehouse get kafka lakehouse -o jsonpath='{.status.listeners[?(@.name=="external")].bootstrapServers}'` (Route host:443); vanilla nodeport: `<düğüm-IP>:$(kubectl -n lakehouse get svc lakehouse-kafka-external-bootstrap -o jsonpath='{.spec.ports[0].nodePort}')`.
   - CA: `kubectl -n lakehouse get secret lakehouse-cluster-ca-cert -o jsonpath='{.data.ca\.crt}' | base64 -d > /etc/fluent-bit/lakehouse-ca.crt`
   - parola: `kubectl -n lakehouse get secret fluentbit -o jsonpath='{.data.password}' | base64 -d`
3. Sunucuda: Fluent Bit 5.1 paketi (fluentbit.io/install); `agents/fluent-bit/fluent-bit.conf` + `parsers.conf` → `/etc/fluent-bit/`; `/etc/default/fluent-bit`:
   `KAFKA_BOOTSTRAP=<host:port>` `KAFKA_PASSWORD=<parola>` `KAFKA_CA=/etc/fluent-bit/lakehouse-ca.crt` `NGINX_ACCESS_LOG=/var/log/nginx/access.log` `READ_FROM_HEAD=off`; `systemctl enable --now fluent-bit`.
4. Doğrulama: `kubectl -n lakehouse get kafkaconnector sink-nginx` Ready; ≤ 5 dk içinde `nginx_raw.access_log` (Trino F4 / pyiceberg). Ajan logu: `journalctl -u fluent-bit`.
Notlar: disk tamponu `storage.type filesystem` (Kafka kesintisinde 1G'a kadar); log formatı `combined` dışındaysa `parsers.conf` regex'i güncelle; TR-locale sorunu yok (ay adları ajanda `%b` ile çözülür).
EOF
```

- [ ] **Step 5: Doğrula + commit**

```bash
helm unittest glue 2>&1 | grep -E "^Tests:|FAIL" && helm lint glue 2>&1 | tail -1
helm template glue -f platform/values/glue.yaml >/dev/null && helm template glue --set connect.buildImage=x --set nginx.enabled=true | grep -cE "^kind: (KafkaTopic|KafkaUser|KafkaConnector)"   # 1 + 2 + 1 = 4
git add glue agents runbooks/nginx-agent.md && git commit -m "feat(glue): nginx path — KafkaTopic, KafkaUser fluentbit, Iceberg sink (TimestampConverter unix ms); Fluent Bit agent config + runbook

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 4: platform/dev values, Polaris namespace'leri, runbook'lar, CI teşhis

**Files:**
- Modify: `platform/values/glue-dev.yaml`, `platform/values/glue.yaml`, `platform/polaris/setup.yaml`, `runbooks/add-source.md`, `runbooks/install.md`, `.github/workflows/e2e.yaml`

- [ ] **Step 1: values + Polaris**

`platform/values/glue-dev.yaml`: `kafka.externalListener: true`; `sources` += `{name: crm, type: mongodb, connectionString: "mongodb://demo-mongo.lakehouse.svc:27017/?replicaSet=rs0", collections: [crm.customers]}`; `pipelines` += `{bronze: crm_raw.customers, keys: [_id], bucket_count: 4}`; `nginx: {enabled: true, partitions: 3}`; `spark.schedules.mongoBronze: "*/30 * * * *"` (e2e tek seferlik koşturur).
`platform/values/glue.yaml`: yorumlu `nginx.enabled: true` + `kafka.externalListener: true` örneği (prod'da Route).
`platform/polaris/setup.yaml` `namespaces` += `crm_raw`, `crm`.

- [ ] **Step 2: Runbook'lar + teşhis**

`runbooks/add-source.md`: **mongodb** bölümü — replica set şart (change streams); kullanıcı en az yetki: `db.getSiblingDB("admin").createUser({user, pwd, roles:[{role:"read", db:"<db>"}, {role:"read", db:"config"}, {role:"clusterMonitor", db:"admin"}]})` (Debezium mongodb belgesi); Secret `<name>-db`; `sources` girişi (`connectionString` kimlik bilgisiz, `collections`), Polaris namespace `<name>_raw` + `<name>`; `pipelines` `{bronze: <name>_raw.<coll>, keys: [_id]}`; Silver `(_id, _doc)` → tüketim `json_extract(_doc, '$.alan')` (Trino F4); karantina `<name>_raw.<coll>__quarantine` (`reason`: `after-null|no-key|bad-envelope`).
`runbooks/install.md`: Adım 2 iş listesine `mongo-bronze`; sorun giderme: "`mongo-bronze` `KAFKA_JAAS` yok → KafkaUser spark yalnız mongodb kaynağı varken oluşur"; "nginx: `nginx.dlq` doluysa `ts` dönüşümü başarısız (Fluent Bit lua filtresi eksik)".
`.github/workflows/e2e.yaml` teşhis: `kubectl -n lakehouse get kafkatopic,kafkauser || true`, `kubectl -n lakehouse logs deploy/fluent-bit --tail=40 || true`, `kubectl -n lakehouse logs deploy/demo-mongo --tail=20 || true`.

- [ ] **Step 3: Doğrula + commit**

```bash
helm template glue -f platform/values/glue-dev.yaml | grep -E "^kind: (KafkaConnector|KafkaTopic|KafkaUser|ScheduledSparkApplication)" | sort | uniq -c   # KafkaConnector 4, KafkaTopic 1, KafkaUser 3, SSA 5
helm unittest glue 2>&1 | grep -E "^Tests:" && kubectl kustomize platform/envs/dev >/dev/null && .venv/bin/python -c 'import yaml; yaml.safe_load(open("platform/polaris/setup.yaml")); print("ok")'
git add platform runbooks .github && git commit -m "feat(platform): dev mongodb source + nginx path, external listener, Polaris namespaces; mongodb add-source runbook

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 5: e2e mongo + nginx yolları; ortak `lib.sh`; canlı; F3 notu

**Files:**
- Create: `test/e2e/lib.sh`, `test/e2e/mongo-fixture.yaml`, `test/e2e/mongo-path.sh`, `test/e2e/nginx-fixture.yaml`, `test/e2e/nginx-path.sh`
- Modify: `test/e2e/pg-path.sh` (`verify`/`run_spark_once` → `lib.sh`), `test/e2e/verify/verify.py` (`~k=substr`), `test/e2e/run.sh`, `README.md`, `docs/plans/2026-09-10-f0-findings.md` (F3 notu)

- [ ] **Step 1: `lib.sh` + verify `~`**

`test/e2e/lib.sh`: `pg-path.sh`'teki `verify()` ve `run_spark_once()` fonksiyonlarını buraya taşı (`NS` ve `ROOT` çağıran script'te tanımlı); `run_spark_once` grep'ine `MONGO_OK` ekle. `pg-path.sh`: `source "$ROOT/test/e2e/lib.sh"` (fonksiyon kopyası kalmaz). `verify.py`: `~k=substr` koşulu — `str(row.get(k) or "")` içinde `substr`; `k=v:~k2=s` aynı satırda karışık kullanılabilir; belge satırı güncellenir.

- [ ] **Step 2: mongo fixture + path**

```bash
cat > test/e2e/mongo-fixture.yaml <<'EOF'
# e2e MongoDB 8.0 tek düğümlü replica set (Debezium change streams şartı) + seed. Secret adı <name>-db.
apiVersion: v1
kind: Secret
metadata: {name: crm-db, namespace: lakehouse}
type: kubernetes.io/basic-auth
stringData: {username: dbz, password: dbz}
---
apiVersion: apps/v1
kind: Deployment
metadata: {name: demo-mongo, namespace: lakehouse}
spec:
  replicas: 1
  selector: {matchLabels: {app: demo-mongo}}
  template:
    metadata: {labels: {app: demo-mongo}}
    spec:
      containers:
      - name: mongo
        image: mongo:8.0
        args: ["--replSet", "rs0", "--bind_ip_all"]
        ports: [{containerPort: 27017}]
        readinessProbe: {exec: {command: ["mongosh", "--quiet", "--eval", "db.adminCommand('ping').ok"]}, initialDelaySeconds: 5, periodSeconds: 5}
---
apiVersion: v1
kind: Service
metadata: {name: demo-mongo, namespace: lakehouse}
spec: {selector: {app: demo-mongo}, ports: [{port: 27017, targetPort: 27017}]}
---
apiVersion: batch/v1
kind: Job
metadata: {name: demo-mongo-init, namespace: lakehouse}
spec:
  backoffLimit: 10
  template:
    spec:
      restartPolicy: OnFailure
      containers:
      - name: init
        image: mongo:8.0
        command: ["/bin/sh", "-c"]
        args:
        - |
          set -e; H=demo-mongo.lakehouse.svc:27017
          until mongosh --quiet --host $H --eval 'db.adminCommand("ping").ok' >/dev/null 2>&1; do sleep 3; done
          mongosh --quiet --host $H --eval 'try { rs.status().ok } catch (e) { rs.initiate({_id:"rs0", members:[{_id:0, host:"demo-mongo.lakehouse.svc:27017"}]}) }'
          until mongosh --quiet --host $H --eval 'rs.isMaster().ismaster' | grep -q true; do sleep 2; done
          mongosh --quiet --host $H --eval 'if (!db.getSiblingDB("admin").getUser("dbz")) db.getSiblingDB("admin").createUser({user:"dbz", pwd:"dbz", roles:["root"]})'
          mongosh --quiet --host $H -u dbz -p dbz --authenticationDatabase admin --eval '
            const c = db.getSiblingDB("crm").customers;
            if (c.countDocuments() === 0) c.insertMany([
              {_id: ObjectId("000000000000000000000001"), name: "Ada", tier: "gold", tags: ["a","b"], addr: {city: "Izmir"}},
              {_id: ObjectId("000000000000000000000002"), name: "Bob", tier: "silver", tags: [], addr: {city: "Ankara"}},
              {_id: ObjectId("000000000000000000000003"), name: "Cem", tier: "gold", score: 4.5}]);'
          echo INIT_OK
EOF
cat > test/e2e/mongo-path.sh <<'EOF'
#!/usr/bin/env bash
# e2e F3 mongo yolu: fixture -> dbz-crm Ready -> mongo-bronze (Bronze 3 I) -> silver-merge (Silver 3) -> update/delete/insert -> tekrar.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"; NS=lakehouse
# shellcheck source=/dev/null
source "$ROOT/test/e2e/lib.sh"
MONGO() { kubectl -n "$NS" exec deploy/demo-mongo -- mongosh --quiet -u dbz -p dbz --authenticationDatabase admin --eval "$1"; }

echo "== mongo fixture"
kubectl apply -f "$ROOT/test/e2e/mongo-fixture.yaml" >/dev/null
kubectl -n "$NS" wait --for=condition=complete job/demo-mongo-init --timeout=600s
echo "== dbz-crm"
kubectl -n "$NS" wait kafkaconnector/dbz-crm --for=condition=Ready --timeout=600s
for _ in $(seq 1 60); do kubectl -n "$NS" exec lakehouse-dual-role-0 -c kafka -- bin/kafka-topics.sh --bootstrap-server localhost:9092 --list 2>/dev/null | grep -qx "crm.crm.customers" && break; sleep 5; done
echo "== mongo-bronze #1"
run_spark_once mongo-bronze
verify crm_raw.customers 3 --exact --wait 60 '_id=000000000000000000000001' '~_doc=Ada'
echo "== silver-merge (crm)"
run_spark_once silver-merge
verify crm.customers 3 --exact --wait 60 '_id=000000000000000000000002' '~_doc=Ankara'
echo "== mongo update/delete/insert"
MONGO 'const c = db.getSiblingDB("crm").customers; c.updateOne({_id: ObjectId("000000000000000000000001")}, {$set: {tier: "platinum", tags: ["x"]}}); c.deleteOne({_id: ObjectId("000000000000000000000002")}); c.insertOne({_id: ObjectId("000000000000000000000004"), name: "Deniz", nested: {deep: {v: 1}}});' >/dev/null
sleep 20
echo "== mongo-bronze #2"
run_spark_once mongo-bronze
verify crm_raw.customers 6 --exact --wait 60 '~_doc=platinum' '_id=000000000000000000000004'
echo "== silver-merge (crm) #2"
run_spark_once silver-merge
verify crm.customers 3 --exact --wait 60 '_id=000000000000000000000001:~_doc=platinum' '!_id=000000000000000000000002' '_id=000000000000000000000004'
echo "== karantina boş"
verify crm_raw.customers__quarantine 0 --exact
echo "E2E F3 MONGO OK"
EOF
chmod +x test/e2e/mongo-path.sh
```
Not: silver-merge tüm `pipelines`'ı işler (shop dahil) — shop Bronze'da yeni snapshot yoksa "yeni snapshot yok" der, sorun değil.

- [ ] **Step 3: nginx fixture + path**

`test/e2e/nginx-fixture.yaml`: ConfigMap `nginx-sample-log` (`access.log`: 3 satır combined format —
`10.0.0.1 - - [11/Sep/2026:13:52:24 +0000] "GET /index.html HTTP/1.1" 200 512 "-" "curl/8.0"`,
`10.0.0.2 - - [11/Sep/2026:13:52:25 +0000] "POST /api/orders HTTP/1.1" 201 87 "-" "Mozilla/5.0"`,
`10.0.0.3 - - [11/Sep/2026:13:52:26 +0000] "GET /missing HTTP/1.1" 404 153 "-" "curl/8.0"`),
Deployment `fluent-bit` (`fluent/fluent-bit:5.1.2`, `args: ["-c", "/fluent-bit/etc/fluent-bit.conf"]`, env `KAFKA_BOOTSTRAP=lakehouse-kafka-bootstrap:9093`, `KAFKA_PASSWORD` ← Secret `fluentbit` key `password`, `KAFKA_CA=/etc/kafka-ca/ca.crt`, `NGINX_ACCESS_LOG=/var/log/nginx/access.log`, `READ_FROM_HEAD=on`; volumes: ConfigMap `fluent-bit-config` → `/fluent-bit/etc/`, ConfigMap `nginx-sample-log` → `/var/log/nginx/`, Secret `lakehouse-cluster-ca-cert` (`ca.crt`) → `/etc/kafka-ca/`, emptyDir → `/var/lib/fluent-bit`). `fluent-bit-config` ConfigMap'ini `nginx-path.sh` `agents/fluent-bit/` dosyalarından yaratır (kopya yok).
```bash
cat > test/e2e/nginx-path.sh <<'EOF'
#!/usr/bin/env bash
# e2e F3 nginx yolu: dış listener var -> Fluent Bit (küme içi, aynı SASL_SSL/PEM config) -> nginx.access -> sink -> nginx_raw.access_log
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"; NS=lakehouse
# shellcheck source=/dev/null
source "$ROOT/test/e2e/lib.sh"
echo "== dış listener"
kubectl -n "$NS" get svc lakehouse-kafka-external-bootstrap -o jsonpath='{.spec.type} {.spec.ports[0].nodePort}{"\n"}' | grep -E "^NodePort [0-9]+"
kubectl -n "$NS" wait kafkatopic/nginx.access --for=condition=Ready --timeout=300s
kubectl -n "$NS" wait kafkaconnector/sink-nginx --for=condition=Ready --timeout=600s
echo "== fluent-bit"
kubectl -n "$NS" create configmap fluent-bit-config --from-file="$ROOT/agents/fluent-bit/fluent-bit.conf" --from-file="$ROOT/agents/fluent-bit/parsers.conf" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
kubectl apply -f "$ROOT/test/e2e/nginx-fixture.yaml" >/dev/null
kubectl -n "$NS" rollout status deploy/fluent-bit --timeout=300s
echo "== Bronze nginx_raw.access_log"
verify nginx_raw.access_log 3 --exact --wait 600 'remote=10.0.0.1:code=200' 'code=404' '~ts=2026-09-11 13:52:24'
echo "E2E F3 NGINX OK"
EOF
chmod +x test/e2e/nginx-path.sh
```
`~ts=…` beklentisi: pyiceberg timestamp'in `str()` biçimi `2026-09-11 13:52:24[+00:00]`; `~` (içerir) ile saniye doğruluğu kanıtlanır (Q5). Canlıda ilk HATA çıktısı gerçek biçimi basar; gerekirse düzelt.

- [ ] **Step 4: run.sh kancaları + README**

`run.sh`: pg fixture apply döngüsünün yanına `kubectl apply -f "$ROOT/test/e2e/mongo-fixture.yaml"` (idempotent; `dbz-crm` Secret `crm-db`'yi bekler — aksi glue Degraded, F2 notu 8); sonda `pg-path.sh` → `mongo-path.sh` → `nginx-path.sh`. `README.md` "Şu an" → F3. `bash -n` + `shellcheck` tüm e2e script'leri.

- [ ] **Step 5: Lokal canlı + CI**

Taze küme şart (F2 notu 9): `KIND_EXPERIMENTAL_PROVIDER=podman kind delete cluster --name lakehouse` → `KIND_EXPERIMENTAL_PROVIDER=podman KIND_CLUSTER=lakehouse test/e2e/run.sh --mode helm` (nohup + log). Durma noktaları: (a) `dbz-crm` task trace (mongo auth/replica set) — `kubectl -n lakehouse get kafkaconnector dbz-crm -o jsonpath='{.status.connectorStatus.tasks[0].trace}'`; (b) `mongo-bronze` Kafka SASL/PEM (`KAFKA_JAAS`, CA mount) — `kubectl -n lakehouse logs e2e-mongo-bronze-driver`; (c) Fluent Bit → Kafka auth (pod logunda `Authentication failed`/`SSL`); (d) `ts` biçimi (Q5) — `nginx.dlq` doluysa TimestampConverter başarısız. Düzelt → aynı komut (taze küme gerekiyorsa sil-yeniden). Sonra push → CI (`gh run watch`).

- [ ] **Step 6: F3 notu + commit + push**

`docs/plans/2026-09-10-f0-findings.md` → `## F3 notu`: mongo envelope gerçek şekli (key/`$oid`, `after` string), `mongo-bronze` süresi, Kafka batch offsets çalışması, nginx `ts` biçimi / Fluent Bit 5.1.2 lua, dış listener nodeport portu, Debezium mongo yetkileri (dev root), açık kalanlar (Route dış listener pre-ship; ajan gerçek sunucuda; mongo incremental snapshot sinyali canlı değil; dev MinIO PVC).
```bash
git add -A && git commit -m "test(e2e): mongo + nginx paths (shared lib.sh), verify ~contains; F3 note

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
git push origin v2
```

---

## Self-review

**Spec coverage:** §5.3 mongo (ENDS'siz, `_id` key'den, `$oid`, tombstone, karantina, Silver `(_id,_doc)`, 5 dk iş) → Task 1–2, 5; micro-batch mekanizması Q2 ile batch+offset (gerekçeli sapma) · §5.4 nginx (Fluent Bit tail/parser/kafka, disk buffer, SASL_SSL dış listener, topic `nginx.access`, sink TimestampConverter, `nginx_raw.access_log`, `day(ts)`) → Task 3–5; `iso8601` yerine epoch ms (Q5, gerekçeli) · §5.1 dış listener → Task 4 values + Task 5 doğrulama · §9 e2e → Task 5 · §13 F3 "Spark 4 doğrulaması" → F2'de canlı yapıldı; F3'te Spark 4 Kafka batch okuma da doğrulanır · §5.5 → F5 (Q10).

**Placeholder taraması:** kod blokları tam; `connectors.yaml`/`spark-jobs.yaml` değişiklikleri hedef yapı + tam anahtar listesiyle tarif edildi (dosyalar 140/90 satır; uygulayıcı okur). `~ts=…` beklentisi canlıda düzeltilecek diye işaretli (kasıtlı, kanıt odaklı). TBD yok.

**Tutarlılık:** `pipelines.json.mongo[].{topic,bronze}` (Task 1 ↔ Task 2 ↔ Task 5 `crm_raw.customers`) · env `KAFKA_JAAS/KAFKA_CA/KAFKA_BOOTSTRAP` (Task 1 ↔ Task 2) · Secret `spark` (`sasl.jaas.config`), `fluentbit` (`password`), `crm-db`, `lakehouse-cluster-ca-cert` (Task 2/3 ↔ Task 5) · SSA adı `mongo-bronze` (Task 2 ↔ Task 5) · `MONGO_OK` (Task 1 ↔ lib.sh grep) · nginx topic/table/`ts` alanı (Task 3 sink ↔ agent lua ↔ Task 5 verify) · ObjectId'ler `…0001..0004` (fixture ↔ verify) · `~` koşulu (verify.py ↔ mongo/nginx path).
