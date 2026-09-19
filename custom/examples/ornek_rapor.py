"""Örnek özel Spark uygulaması: Silver shop.orders -> durum bazında sipariş sayısı -> lakehouse.sandbox.ornek_rapor.
Iceberg/katalog ayarları SparkApplication CR'ındaki sparkConf'tan gelir; bu dosya YALNIZ veri mantığıdır.
TEK İSTİSNA kimlik bilgisidir: Polaris credential'ı (client_id:client_secret) YAML'a GİRMEZ -> Secret
polaris-spark -> env POLARIS_CREDENTIAL -> burada katalog config'ine verilir (glue/jobs/merge_cdc.py ile aynı kalıp).

Uyarlama: CATALOG sabit kalır (ürünün kataloğu); kaynak tabloyu, hesaplamayı ve hedef tabloyu değiştirin.
Yazma yalnız `sandbox` namespace'inde serbest DEĞİLDİR: `spark` principal'ı `writers` rolündedir
(platform/polaris/setup.yaml) -> katalogda her yere yazabilir. Kendi raporlarınızı sandbox'ta tutmanız
önerilir; ürünün Bronze/Silver namespace'lerine elle yazmak pipeline'ları bozar."""
import os

from pyspark.sql import SparkSession

CATALOG = "lakehouse"

builder = SparkSession.builder.appName("ornek-rapor")
cred = os.environ.get("POLARIS_CREDENTIAL")          # Secret -> env; YAML'a girmez
if cred:
    builder = builder.config(f"spark.sql.catalog.{CATALOG}.credential", cred)
spark = builder.getOrCreate()

spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.sandbox")
rapor = spark.table(f"{CATALOG}.shop.orders").groupBy("status").count().withColumnRenamed("count", "adet")
# createOrReplace: iş yeniden koşarsa tablo baştan yazılır (idempotent) -> e2e ve gecelik cron güvenli
rapor.writeTo(f"{CATALOG}.sandbox.ornek_rapor").using("iceberg").createOrReplace()
print("ORNEK_RAPOR_OK", rapor.count())
spark.stop()
