# Spark job-code delivery (s3a)

Every Spark job the platform runs — the scheduled Iceberg maintenance
job, the Bronze→Silver CDC merge, the nginx-streaming ingest lane, and any
Console-rendered batch/s3 job — reads its own code from S3 at submit time
instead of from a path baked into the Spark image. This doc explains the
model, how to hot-fix a job without an image rebuild, and how a customer
adds and runs their own job on the platform.

## The model

Job code (`iceberg_maintenance.py`, `merge_cdc.py` + `merge_lib.py`,
`nginx_streaming.py`, `s3_register_table.py`, …) lives in a dedicated S3
bucket, separate from any data bucket:

```
s3a://<storage.jobsBucket>/<storage.jobsPrefix>/<job>.py
```

with defaults `jobsBucket: spark-jobs`, `jobsPrefix: jobs` (so
`s3a://spark-jobs/jobs/merge_cdc.py` out of the box).

There are two copies of this code, each with a distinct role, and they are
kept in sync automatically:

- **The image is the reproducible source of truth.** The `spark-py` image
  bakes the job scripts under `/opt/spark/jobs/` at build time (from
  `tools/jobs/*.py`). This is what gets reviewed, versioned, and released.
- **S3 is the runtime source.** Every `SparkApplication` /
  `ScheduledSparkApplication` the chart renders sets
  `mainApplicationFile: s3a://<jobsBucket>/<jobsPrefix>/<job>.py` — it does
  **not** reference the in-image path. spark-submit fetches the file from
  S3 at job-bootstrap time, before any job logic runs.

A Kubernetes Job, `lakehouse-jobs-seed`, bridges the two: on every
`helm install`/`helm upgrade` (and on the equivalent ArgoCD PostSync hook
in GitOps mode) it copies the image-baked `/opt/spark/jobs/*.py` into
`s3a://<jobsBucket>/<jobsPrefix>/`, **overwriting** whatever is already
there. This keeps S3 pinned to whatever the currently-installed image
ships — every install/upgrade re-asserts the image as ground truth. Disable
it with `storage.seedJobs: false` only if the bucket is seeded out-of-band
(e.g. an air-gapped pre-seed step) — most deployments should leave it on.

Why two copies instead of just running from the image directly? Because
S3-hosted code delivery is what makes the hot-fix workflow and the
customer self-service workflow below possible — SparkApplications don't
have to be rendered with the exact image the operator happens to be
running, and job code can change without a build/push/rollout cycle.

## Hot-fix workflow

Because the scheduled jobs re-read their `mainApplicationFile` from S3 on
every run, you can patch a job's behavior without rebuilding or
re-pushing the `spark-py` image:

```bash
# Edit merge_cdc.py locally, then overwrite the S3 copy directly:
mc cp merge_cdc.py s3/spark-jobs/jobs/merge_cdc.py
```

The next scheduled tick (`iceberg-maintenance` or `silver-merge`
`ScheduledSparkApplication`, whichever job you touched) picks up the
overwritten file with no image rebuild, no chart change, and no pod
restart.

This is a **transient** fix. `lakehouse-jobs-seed` overwrites the jobs
bucket from the image on every subsequent `helm upgrade` (or ArgoCD
PostSync), so a hot-fixed script reverts to whatever the image ships the
next time the chart is upgraded. Treat S3 hot-fixes as a stop-gap for
urgent issues between releases — land the durable fix in `tools/jobs/*.py`
and get it into the next image build, or it will silently disappear on the
next upgrade.

## Customer self-service: running your own job

A customer (or operator) can add and run a custom Spark job on the
platform without any chart change or image rebuild:

1. **Upload the script** to the jobs bucket, alongside the platform's own
   jobs:

   ```bash
   mc cp my_job.py s3/spark-jobs/jobs/my_job.py
   ```

2. **Author a `SparkApplication` or `ScheduledSparkApplication`** that
   points `mainApplicationFile` at it:

   ```yaml
   apiVersion: sparkoperator.k8s.io/v1beta2
   kind: SparkApplication
   metadata:
     name: my-job
     namespace: <lakehouse namespace>
   spec:
     type: Python
     mode: cluster
     image: <the platform's own spark-py image, same tag as the other jobs>
     mainApplicationFile: "s3a://spark-jobs/jobs/my_job.py"
     sparkVersion: "<platform sparkVersion>"
     sparkConf:
       # Reuse the platform's Iceberg catalog + S3 config so the job reads/
       # writes the same lakehouse/rawlake tables the other jobs use — copy
       # this block from an existing rendered SparkApplication (e.g.
       # iceberg-maintenance) rather than hand-rolling it.
       spark.sql.extensions: "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
       spark.hadoop.fs.s3a.path.style.access: "true"
       spark.sql.catalog.lakehouse: "org.apache.iceberg.spark.SparkCatalog"
       spark.sql.catalog.lakehouse.catalog-impl: "org.apache.iceberg.rest.RESTCatalog"
       # ... (catalog uri/warehouse/io-impl — same values as the platform jobs)
     driver:
       serviceAccount: spark-driver
       # ... AWS credential env, same secretKeyRef pattern as the platform jobs
     executor:
       # ...
   ```

   No new image is required: the same `spark-py` image the platform's own
   jobs use already has the Iceberg/Spark/Hadoop-AWS jars baked in — a
   custom job only needs its own script in S3, not its own image.

3. **Submit it** the same way as any other SparkApplication CR (`kubectl
   apply` directly, or via the platform's own GitOps/ArgoCD path if
   `deploy_mode: gitops` is in use).

If your job imports a sibling helper module (the way `merge_cdc.py` imports
`merge_lib.py`), upload the helper alongside the main script and list it
under `deps.pyFiles`, e.g.:

```yaml
mainApplicationFile: "s3a://spark-jobs/jobs/my_job.py"
deps:
  pyFiles:
    - "s3a://spark-jobs/jobs/my_job_lib.py"
```

spark-submit fetches every `pyFiles` entry from S3 at the same bootstrap
step as `mainApplicationFile`, so it needs no special handling beyond
listing it.

## Configuration

| Setting | Default | Purpose |
|---|---|---|
| `storage.jobsBucket` | `spark-jobs` | Bucket holding job code, separate from data buckets. |
| `storage.jobsPrefix` | `jobs` | Key prefix under that bucket (`s3a://<jobsBucket>/<jobsPrefix>/<job>.py`). |
| `storage.seedJobs` | `true` | Runs the `lakehouse-jobs-seed` Job on install/upgrade. Turn off only if the bucket is pre-seeded out-of-band. |
| `storage.s3.endpoint` | `""` (empty) | The Hadoop **`fs.s3a`** endpoint used to fetch `mainApplicationFile` (and `pyFiles`) from S3 at spark-submit bootstrap. |

`storage.s3.endpoint` needs special attention:

- **Leave it empty for real AWS S3.** An empty value lets `fs.s3a` fall
  back to AWS's default endpoint resolution — do not point it at an AWS
  regional endpoint explicitly.
- **Set it for MinIO or any non-AWS/custom S3.** The bootstrap fetch of
  `mainApplicationFile`/`pyFiles` happens **before any job code runs** — it
  is spark-submit itself resolving the `s3a://` URL via Hadoop's `fs.s3a`
  filesystem, not the job's own S3 client. The Hadoop-AWS library baked
  into the image is SDK v1, which does **not** read the
  `AWS_ENDPOINT_URL_S3` environment variable that the jobs' own runtime S3
  clients (e.g. Iceberg's `S3FileIO`) use. If `storage.s3.endpoint` is left
  empty while pointed at MinIO/a custom S3, the bootstrap fetch tries to
  reach AWS's default endpoint and fails before the job ever starts —
  this is a distinct, earlier failure point from anything the job's own
  code configures at runtime.

Both the chart's own SparkApplications and the Console-rendered batch/s3
lane read this same setting (chart: `chart/templates/_helpers.tpl`'s
`lakehouse.spark.baseConf`; Console: `SPARK_S3A_ENDPOINT`, fed from the
same `storage.s3.endpoint` value) — set it once and both paths pick it up.
