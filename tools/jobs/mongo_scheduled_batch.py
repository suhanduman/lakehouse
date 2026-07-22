# Scheduled Mongo -> Iceberg batch job — delta alanı üzerinden yüksek-su-işareti
# (high-water mark) filtreli okuma, hedefe _id üzerinden MERGE.
#
# CronJob şablonu (tools/templates/source-scheduled-mongo-spark.yaml)
# bu script'i spark-submit ile şu argümanlarla çağırır:
#   --uri --db --collection --delta-field --target
import argparse

from pyspark.sql import SparkSession

parser = argparse.ArgumentParser()
for arg in ["uri", "db", "collection", "delta-field", "target"]:
    parser.add_argument(f"--{arg}", required=True)
args = parser.parse_args()

delta_field = args.delta_field
spark = SparkSession.builder.appName(f"mongo-batch-{args.collection}").getOrCreate()

try:
    hw = spark.sql(f"SELECT max(`{delta_field}`) AS hw FROM lakehouse.{args.target}").collect()[0]["hw"]
except Exception:
    hw = None  # hedef tablo henüz yoksa/boşsa: tam yükleme

reader = (spark.read.format("mongodb")
          .option("connection.uri", args.uri)
          .option("database", args.db)
          .option("collection", args.collection))
df = reader.load()
if hw is not None:
    df = df.filter(df[delta_field] > hw)

df.createOrReplaceTempView("delta")
spark.sql(f"""
    MERGE INTO lakehouse.{args.target} t USING delta s
    ON t._id = s._id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")
spark.stop()
