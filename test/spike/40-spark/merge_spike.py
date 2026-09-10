"""S4: Bronze(_cdc) -> Silver MoR MERGE, position-delete rewrite, time travel. Spark 4.1 / Iceberg 1.11."""
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("merge-spike").getOrCreate()
C = "lakehouse"

# Bronze'daki op değer kümesini keşfet (S2d bulgusu: I/U/D mi c/u/d mi)
ops = [r[0] for r in spark.sql(f"SELECT DISTINCT _cdc.op FROM {C}.shop_raw.orders").collect()]
print("BRONZE_OPS", ops)
DEL = "D" if "D" in ops else "d"

spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {C}.shop")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {C}.shop.orders (id BIGINT, status STRING, amount DECIMAL(38,2), updated_at TIMESTAMP)
USING iceberg PARTITIONED BY (bucket(4, id))
TBLPROPERTIES ('format-version'='2','write.merge.mode'='merge-on-read','write.update.mode'='merge-on-read',
  'write.delete.mode'='merge-on-read','write.distribution-mode'='hash','write.metadata.delete-after-commit.enabled'='true')""")

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW inc AS
SELECT id, status, amount, CAST(updated_at AS TIMESTAMP) AS updated_at, _cdc.op AS op FROM (
  SELECT *, row_number() OVER (PARTITION BY id ORDER BY _cdc.ts DESC, _cdc.offset DESC) AS rn
  FROM {C}.shop_raw.orders) WHERE rn = 1""")
print("INC", spark.sql("SELECT id, status, op FROM inc ORDER BY id").collect())

spark.sql(f"""
MERGE INTO {C}.shop.orders t USING inc s ON t.id = s.id
WHEN MATCHED AND s.op = '{DEL}' THEN DELETE
WHEN MATCHED THEN UPDATE SET status = s.status, amount = s.amount, updated_at = s.updated_at
WHEN NOT MATCHED AND s.op <> '{DEL}' THEN INSERT (id, status, amount, updated_at) VALUES (s.id, s.status, s.amount, s.updated_at)""")
silver = spark.sql(f"SELECT id, status, amount FROM {C}.shop.orders ORDER BY id").collect()
print("SILVER", silver)
assert [r.id for r in silver] == [1, 3, 4], silver   # id 2 silindi; id 4 = spike sırasında eklenen probe satırı
assert silver[0].status == "shipped", silver

snap_before = spark.sql(f"SELECT snapshot_id FROM {C}.shop.orders.snapshots ORDER BY committed_at DESC LIMIT 1").collect()[0][0]
# 2. tur: bir satır daha güncelle -> position delete dosyası oluşmalı (MoR); updated_at STRING geldiği için Silver'da CAST gerekir (S2d)
spark.sql(f"UPDATE {C}.shop.orders SET status = 'closed' WHERE id = 3")
deletes = spark.sql(f"SELECT count(*) FROM {C}.shop.orders.files WHERE content = 1").collect()[0][0]   # 1 = position deletes
print("POSITION_DELETE_FILES_AFTER_UPDATE", deletes)
assert deletes >= 1, "MoR beklenirdi, delete dosyası yok (CoW mu çalıştı?)"

spark.sql(f"CALL {C}.system.rewrite_position_delete_files(table => 'shop.orders')")
spark.sql(f"CALL {C}.system.rewrite_data_files(table => 'shop.orders', options => map('delete-file-threshold','1'))")
deletes_after = spark.sql(f"SELECT count(*) FROM {C}.shop.orders.files WHERE content = 1").collect()[0][0]
print("POSITION_DELETE_FILES_AFTER_COMPACTION", deletes_after)

tt = spark.sql(f"SELECT status FROM {C}.shop.orders VERSION AS OF {snap_before} WHERE id = 3").collect()[0][0]
print("TIME_TRAVEL_id3_before_update", tt)
assert tt == "new", tt

# ANSI mode kontrolü (Spark 4 varsayılan): geçersiz CAST hata fırlatmalı
try:
    spark.sql("SELECT CAST('abc' AS INT)").collect(); print("ANSI_MODE", "off (CAST null döndü)")
except Exception as e:
    print("ANSI_MODE", "on (CAST hata:", type(e).__name__, ")")
print("S4_OK")
spark.stop()
