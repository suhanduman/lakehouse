from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, sha2, to_timestamp

spark = SparkSession.builder.appName("nginx-streaming").getOrCreate()

# POC-DOĞRULA: Kafka 9093 listener'ı SCRAM+TLS (spec §10). Truststore ve SCRAM
# parolası SparkApplication'ın mount ettiği secret dosyalarından okunur.
truststore_password = open("/mnt/kafka-ca/ca.password").read().strip()      # POC-DOĞRULA: Strimzi kafka-cluster-ca-cert secret
scram_password = open("/mnt/spark-nginx/password").read().strip()           # POC-DOĞRULA: KafkaUser spark-nginx secret
jaas = (
    'org.apache.kafka.common.security.scram.ScramLoginModule required '
    'username="spark-nginx" password="{}";'.format(scram_password)
)

raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", "kafka-kafka-bootstrap:9093")
       .option("subscribe", "nginx-access")
       .option("startingOffsets", "latest")
       .option("kafka.security.protocol", "SASL_SSL")                       # POC-DOĞRULA
       .option("kafka.sasl.mechanism", "SCRAM-SHA-512")                     # POC-DOĞRULA
       .option("kafka.sasl.jaas.config", jaas)                             # POC-DOĞRULA
       .option("kafka.ssl.truststore.location", "/mnt/kafka-ca/ca.p12")     # POC-DOĞRULA
       .option("kafka.ssl.truststore.password", truststore_password)        # POC-DOĞRULA
       .option("kafka.ssl.truststore.type", "PKCS12")                       # POC-DOĞRULA
       .load())
line = raw.selectExpr("CAST(value AS STRING) AS l")
p = r'^(\S+) \S+ \S+ \[([^\]]+)\] "(\S+) (\S+) [^"]*" (\d{3}) (\d+)'  # nginx combined
parsed = (line
    .withColumn("client_ip_hash", sha2(regexp_extract("l", p, 1), 256))  # KVKK: ham IP saklanmaz
    .withColumn("ts", to_timestamp(regexp_extract("l", p, 2), "dd/MMM/yyyy:HH:mm:ss Z"))
    .withColumn("method", regexp_extract("l", p, 3))
    .withColumn("path", regexp_extract("l", p, 4))
    .withColumn("status", regexp_extract("l", p, 5).cast("int"))
    .withColumn("bytes", regexp_extract("l", p, 6).cast("long"))
    .drop("l"))
(parsed.writeStream.format("iceberg")
    .outputMode("append")
    # checkpointLocation Spark'ın Hadoop FileSystem API'sini kullanır (Iceberg'in
    # S3FileIO'su DEĞİL) — bu yüzden şema `s3a://` olmalı, `s3://` DEĞİL. Iceberg
    # tablo path'leri (örn. `.toTable(...)` altındaki Nessie warehouse konumları,
    # `lakehouse.properties`/`rawlake.properties`'teki `s3://depo/...`) S3FileIO
    # üzerinden gider ve `s3://` şemasını kullanmaya devam eder — bu ikisi
    # KARIŞTIRILMAMALI (review hardening finding).
    .option("checkpointLocation", "s3a://nginx-web/_checkpoints/access_log")  # KALICI (emptyDir değil)
    .toTable("lakehouse.web.access_log"))
spark.streams.awaitAnyTermination()
