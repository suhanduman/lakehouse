"""mongo_bronze — Debezium mongo topic'lerinden (ham envelope) Bronze (_id, _doc, _cdc) + karantina. ScheduledSparkApplication 5 dk.
Kafka okuma BATCH: startingOffsets = Bronze TBLPROPERTIES 'lakehouse.kafka.offsets' (yoksa earliest), endingOffsets = latest;
yazma sonrası özellik güncellenir (arada çökme -> yinelenen Bronze satırı -> Silver dedup absorbe eder; at-least-once).
Silver: mevcut silver-merge (pipelines: {bronze: <ns>.<coll>, keys: [_id]}) — Bronze _cdc struct'ı pg ile aynı (plan Q1).
F3-A: Bronze/karantina DataFrame'leri açık StructType ile kurulur (createDataFrame(list[Row]) tip çıkarımı yerine) —
yalnız delete'lerden oluşan bir batch'te her `_doc` None olur ve çıkarım NullType üretir; Iceberg bunu reddeder."""
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import mongo_lib as mg  # noqa: E402
from merge_cdc import CATALOG, PIPELINES, session, tbl_property  # noqa: E402
from pyspark.sql import Row  # noqa: E402
from pyspark.sql.functions import col, from_unixtime, lit, struct, to_timestamp  # noqa: E402
from pyspark.sql.types import (  # noqa: E402
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

OFFSETS_PROP = "lakehouse.kafka.offsets"
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "lakehouse-kafka-bootstrap:9093")
CA_PATH = os.environ.get("KAFKA_CA", "/etc/kafka-ca/ca.crt")

BRONZE_DDL = ("CREATE TABLE IF NOT EXISTS {t} (`_id` string, `_doc` string, "
              "`_cdc` struct<op: string, ts: timestamp, offset: bigint, source: string, target: string, key: struct<_id: string>>) "
              "USING iceberg PARTITIONED BY (days(_cdc.ts)) "
              "TBLPROPERTIES ('format-version'='2', 'write.metadata.delete-after-commit.enabled'='true')")
QUAR_DDL = ("CREATE TABLE IF NOT EXISTS {t} (`_key` string, `_value` string, `reason` string, `ts` timestamp, "
            "`partition` int, `offset` bigint) USING iceberg PARTITIONED BY (days(ts)) TBLPROPERTIES ('format-version'='2')")

# F3-A: yalnız delete'lerden oluşan bir batch'te her `_doc` None olur; createDataFrame(list[Row]) tip çıkarımı bunu
# NullType sayar ve Iceberg yazımı reddeder — açık şema bu belirsizliği ortadan kaldırır.
BRONZE_RAW_SCHEMA = StructType([
    StructField("_id", StringType(), False),
    StructField("_doc", StringType(), True),
    StructField("op", StringType(), False),
    StructField("ts_ms", LongType(), False),
    StructField("offset", LongType(), False),
    StructField("source", StringType(), False),
])
QUAR_SCHEMA = StructType([
    StructField("_key", StringType(), True),
    StructField("_value", StringType(), True),
    StructField("reason", StringType(), False),
    StructField("ts", TimestampType(), False),
    StructField("partition", IntegerType(), False),
    StructField("offset", LongType(), False),
])


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
        df = spark.createDataFrame(bronze_rows, schema=BRONZE_RAW_SCHEMA)
        df = df.select(col("_id"), col("_doc"),
                       struct(col("op"), to_timestamp(from_unixtime(col("ts_ms") / 1000)).alias("ts"), col("offset"),
                              col("source"), lit(bronze).alias("target"), struct(col("_id")).alias("key")).alias("_cdc"))
        df.writeTo(bt).append()
    if quar_rows:
        spark.createDataFrame(quar_rows, schema=QUAR_SCHEMA).writeTo(qt).append()
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
