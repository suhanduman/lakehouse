# S3 dosyalarını Iceberg'e tek seferlik kaydetme (`s3_register_example.py`)

Bir Debezium/Kafka kaynağı olmadan S3'te duran düz CSV/Parquet dosyalarını doğrudan Iceberg tablosuna yazmak için
referans örnek. e2e'nin parçası değildir; `glue-jobs` ConfigMap'inde zaten mevcuttur (`glue/jobs/*.py` glob'u).

1. Var olan bir `ScheduledSparkApplication` şablonundan (`.spec.template`) tek seferlik `SparkApplication` türet
   (`test/e2e/lib.sh`'in `run_spark_once` mantığı) ve `mainApplicationFile`/`arguments`'ı değiştir:
   ```bash
   kubectl -n lakehouse get scheduledsparkapplication silver-merge -o json \
     | jq '{apiVersion:"sparkoperator.k8s.io/v1beta2", kind:"SparkApplication",
            metadata:{name:"s3-register-once", namespace:.metadata.namespace},
            spec:(.spec.template + {
              mainApplicationFile:"local:///opt/job/s3_register_example.py",
              arguments:["--source","s3://<bucket>/<prefix>/","--format","csv","--table","<ns>.<tbl>","--header","true"]
            })}' \
     | kubectl apply -f -
   ```
   Uygulamadan önce manifest'i doğrulamak için aynı komutu `| kubectl apply --dry-run=server -f -` ile
   koşturun (CRD şeması + admission webhook'u gerçekten devreye girer, nesne yaratılmaz).
2. İzle: `kubectl -n lakehouse get sparkapplication s3-register-once -w`; tamam olunca driver log'unda
   `S3_REGISTER_OK s3://... -> lakehouse.<ns>.<tbl> (<n> satır)` görülür.
3. Temizle: `kubectl -n lakehouse delete sparkapplication s3-register-once`.

Notlar:
- `--format csv` için `--header true|false` header satırını kontrol eder (varsayılan `true`); `parquet` için
  `--header` yoksayılır (Spark parquet reader tanımadığı seçeneği görmezden gelir).
- Hedef tablo `createOrReplace()` ile yazılır: **var olan tabloyu tamamen değiştirir** (idempotent tekrar koşum,
  ama INSERT/APPEND değil). Artımlı yükleme gerekiyorsa bu script değil `merge_cdc.py`/pipeline yolu kullanılmalı.
- Hedef namespace yoksa yaratılır (`CREATE NAMESPACE IF NOT EXISTS`); Iceberg REST kimlik bilgisi diğer Spark
  işleriyle aynı yoldan gelir (`POLARIS_CREDENTIAL` Secret → env, plan P4 — YAML'a girmez).
