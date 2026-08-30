from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, sha2, to_timestamp

spark = SparkSession.builder.appName("nginx-streaming").getOrCreate()

# POC-VERIFY: the Kafka 9093 listener is SCRAM+TLS (spec §10). The truststore
# and SCRAM password are read from secret files mounted by the SparkApplication.
truststore_password = open("/mnt/kafka-ca/ca.password").read().strip()      # POC-VERIFY: Strimzi kafka-cluster-ca-cert secret
scram_password = open("/mnt/spark-nginx/password").read().strip()           # POC-VERIFY: KafkaUser spark-nginx secret
jaas = (
    'org.apache.kafka.common.security.scram.ScramLoginModule required '
    'username="spark-nginx" password="{}";'.format(scram_password)
)

raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", "kafka-kafka-bootstrap:9093")
       .option("subscribe", "nginx-access")
       .option("startingOffsets", "latest")
       .option("kafka.security.protocol", "SASL_SSL")                       # POC-VERIFY
       .option("kafka.sasl.mechanism", "SCRAM-SHA-512")                     # POC-VERIFY
       .option("kafka.sasl.jaas.config", jaas)                             # POC-VERIFY
       .option("kafka.ssl.truststore.location", "/mnt/kafka-ca/ca.p12")     # POC-VERIFY
       .option("kafka.ssl.truststore.password", truststore_password)        # POC-VERIFY
       .option("kafka.ssl.truststore.type", "PKCS12")                       # POC-VERIFY
       .load())
line = raw.selectExpr("CAST(value AS STRING) AS l")
p = r'^(\S+) \S+ \S+ \[([^\]]+)\] "(\S+) (\S+) [^"]*" (\d{3}) (\d+)'  # nginx combined
parsed = (line
    .withColumn("client_ip_hash", sha2(regexp_extract("l", p, 1), 256))  # KVKK (Turkish data-protection law): raw IP is never stored
    .withColumn("ts", to_timestamp(regexp_extract("l", p, 2), "dd/MMM/yyyy:HH:mm:ss Z"))
    .withColumn("method", regexp_extract("l", p, 3))
    .withColumn("path", regexp_extract("l", p, 4))
    .withColumn("status", regexp_extract("l", p, 5).cast("int"))
    .withColumn("bytes", regexp_extract("l", p, 6).cast("long"))
    .drop("l"))
(parsed.writeStream.format("iceberg")
    .outputMode("append")
    # checkpointLocation uses Spark's Hadoop FileSystem API (NOT Iceberg's
    # S3FileIO) — so its scheme must be `s3a://`, NOT `s3://`. Iceberg table
    # paths (e.g. the Nessie warehouse locations under `.toTable(...)`, or
    # `s3://depo/...` in `lakehouse.properties`/`rawlake.properties`) go
    # through S3FileIO and keep using the `s3://` scheme — the two must NOT
    # be confused (review hardening finding).
    .option("checkpointLocation", "s3a://nginx-web/_checkpoints/access_log")  # PERSISTENT (not emptyDir)
    .toTable("lakehouse.web.access_log"))
spark.streams.awaitAnyTermination()
