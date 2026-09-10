# 01 — Debezium Server + debezium-server-iceberg (memiiso): the "no Kafka, no Spark" path

Review date: 2026-09-10. Reviewer stance: independent, skeptical. No local repo files were read. Everything below is
from primary sources (memiiso master source code and docs, GitHub API, debezium.io 3.6 docs, Iceberg spec/releases,
Trino docs/release notes) unless marked **INFERENCE**. Version reviewed: memiiso **1.1.0.Final (2026-07-18)** =
Debezium 3.6.0.Final + Iceberg 1.11.0 + JDK 21.

---

## A. Architecture (components, hops, where state lives)

**What it is.** A Debezium Server *sink consumer* (`@Named("iceberg") IcebergChangeConsumer implements
DebeziumEngine.ChangeConsumer`). Debezium Server is a standalone Quarkus process that embeds the Debezium Engine and
runs **exactly one source connector per instance** (VERIFIED, Debezium 3.6 docs: "each instance of Debezium Server runs
exactly one connector"). So for the platform's four source systems you run four JVMs (PG, MSSQL, MySQL, MongoDB), each
replicating all included tables of its database.

**Hops.** Source DB WAL/binlog/CDC/oplog → Debezium Engine in-process queue (`max.queue.size`, default 8192) →
`handleBatch(records)` → group by destination (topic name = table) → convert to Iceberg `Record` → write Parquet
data (+ delete) files straight to S3 via Iceberg `FileIO` → one Iceberg commit **per table per batch** (`RowDelta` if
delete files exist, else `AppendFiles`) → `committer.markProcessed(record)` for each record + `markBatchFinished()` →
optional sleep (`BatchSizeWait`). Zero intermediate brokers. (VERIFIED: `IcebergChangeConsumer.handleBatch`,
`IcebergTableOperator.addToTablePerSchema`.)

**Where state lives.**
- Source offsets: `IcebergOffsetBackingStore` — a one-row Iceberg table (`_debezium_offset_storage`) rewritten with an
  `OverwriteFiles` commit on every offset flush (VERIFIED, `IcebergOffsetBackingStore.save()`, changed from
  delete+append to overwrite in PR #690, 2026-04). Alternative: Debezium's default `FileOffsetBackingStore` on a PVC.
- Schema history (MySQL/MSSQL/Oracle need it): `IcebergSchemaHistory` Iceberg table (`_debezium_database_history_storage`).
- Table metadata: any Iceberg catalog via `CatalogUtil.buildIcebergCatalog(name, props, hadoopConf)`; all
  `debezium.sink.iceberg.*` props are passed through verbatim. Nessie native (`type=nessie`, `iceberg-nessie` on
  classpath, shipped docker-compose example) and generic REST (`type=rest`, integration test `CatalogRest`) are both
  exercised in the test suite (VERIFIED: `examples/nessie/config/application.properties`,
  `IcebergChangeConsumerNessieCatalogTest`, `IcebergChangeConsumerRestCatalogTest`). Hive, JDBC, Glue also bundled.
- Nothing is kept in memory across batches except the engine queue; no per-table writer survives a batch (a
  persistent-writer PR #693 exists but was **closed unmerged**).

**Distribution.** `debezium-server-iceberg-dist` bundles source connectors: postgres, mysql, mariadb, mongodb, oracle,
sqlserver, db2 (+ debezium-scripting). Dockerfile = `registry.access.redhat.com/ubi8/openjdk-21`, exposes 8080/9000,
volumes `/debezium/config`, `/debezium/data` (VERIFIED). Images are published via GitHub Actions (#732 fixed "released
images stay pullable"); I could not list the ghcr packages (403), so the exact image coordinates are **INFERENCE**.

**Format requirement.** `debezium.format.value=connect` (default) or `json`, with `schemas.enable=true` on both key and
value; `ExtractNewRecordState` (`unwrap`) SMT is required for upsert mode. Without flattening only append mode with
`create-identifier-fields=false` works (VERIFIED, docs "Debezium Event Flattening"; Debezium 3.6 docs say the same).

---

## B. Upsert/delete strategy on the lake and write-amplification behavior

**Strategy: merge-on-read, never copy-on-write.** The delta writer (`BaseDeltaTaskWriter extends Iceberg
BaseTaskWriter`, inner `RowDataDeltaWriter extends BaseEqualityDeltaWriter`, `DeleteGranularity.PARTITION`) does, per
row (VERIFIED, `BaseDeltaTaskWriter.write`):
- pure INSERT of a key not seen before in this batch **and** `upsert-keep-deletes=false` → plain `write(row)` (no delete);
- DELETE with `keep-deletes=false` → `deleteKey(keyProjection)` only (hard delete, no tombstone);
- everything else (update, soft delete, or any insert when keep-deletes=true because a tombstone may already exist) →
  `deleteKey(key)` **then** `write(row)`.

`deleteKey` writes an **equality delete** containing only the identifier (PK) columns — changed in 2024 after issue
#403 showed the full "after" row was being written into the delete file, violating the spec and breaking filter
correctness (VERIFIED issue thread). For rows inserted earlier *in the same writer*, Iceberg's `BaseEqualityDeltaWriter`
emits **position deletes (v2)** or **deletion vectors (v3)** instead — that is Iceberg-core behaviour behind
`PartitioningDVWriter`/`useDv` (VERIFIED that the DV writer is wired in PR #720; that intra-batch overwrites become
positional deletes is **INFERENCE** from Iceberg core semantics).

**Format v3 is now the default for new tables** (`format-version=3` since 1.1.0; `upsert-use-dv` auto = true when
v>2). Note carefully what DVs do and do not change here: DVs replace *position-delete files*, not *equality deletes*.
The cross-batch update path (the 99% case: today's UPDATE hits a row written in some earlier file) still produces an
equality-delete file, because the writer has no idea which file/position holds the old row. So the read-side cost
profile is fundamentally "equality deletes", v2 or v3.

The maintainer is explicit that CoW is out of scope: "Copy-on-write is not supported to keep write performance, its
not suitable for streaming writes. to rewrite table content you need to run periodic maintenance jobs" (VERIFIED,
#630 comment, 2025-10-02).

**Write amplification on the write path: near zero.** A batch touching K rows of table T in P partitions produces at
most P data files + P equality-delete files (+ DVs) and one commit; no existing data file is rewritten. This is the
answer to "how does it avoid rewriting whole data files on small updates" — it doesn't rewrite anything; it defers.

**Where the amplification actually goes: reads and compaction.**
- Every scan of T must load all equality-delete files whose sequence number is newer than each data file in the same
  partition and anti-join them. Trino's history here is bad: #13092 (2022, "extremely/unusably slow", re-reading
  delete files per page), #17114 (2023, RowPredicate stack depth ∝ number of delete files), #18396 (2023–2024, delete
  files re-read per split), **#26059 (open, 2025-06, coordinator OOM on SELECT due to delete files)**. Trino 480
  (2026-03) "Avoid worker crashes when reading from tables with a larger number of equality deletes" and Trino 482
  (2026-06) "Reduce memory usage when reading tables with many equality delete files" show it is still being worked
  on (VERIFIED release notes).
- Compaction (`rewrite_data_files`) must eventually rewrite every data file that has any delete applied to it in
  order to drop the deletes — so the platform's *hourly* Spark job is where the full-file rewrites happen. Net bytes
  rewritten per day are of the same order as CoW MERGE, just batched hourly instead of every 15 min; what you gain is
  cheaper, faster ingest commits and fewer rewrites for hot rows updated many times between compactions (**INFERENCE**
  from Iceberg mechanics).
- Iceberg community direction: equality deletes were proposed for deprecation (Oct 2024, revived 2026 with a Flink
  "equality delete → DV conversion" design); Iceberg 1.11.0 already "Deprecate Position delete files with row data".
  Equality deletes are still valid in v3 today, but this sink's core write mechanism is the one the format community
  is trying to retire (spec text VERIFIED; deprecation-thread details from secondary sources = **INFERENCE**).

**Correctness caveats (VERIFIED from issues/code):**
- Partition column must be immutable. An UPDATE that changes a partition value is routed to the *new* partition and
  the equality delete is written there; the old row in the old partition survives → duplicate (#248, #630). Platform's
  `bucket(16, pk)` is safe because PK is immutable.
- `upsert-keep-deletes=true` by default → deleted rows remain with `__deleted=true`. Every downstream query must filter
  it or you get ghosts. (Debezium 3.6 docs describe the same default.)
- Tables without a PK/identifier fields silently fall back to append mode even with `upsert=true`
  (`IcebergTableWriterFactory`: "Table don't have Pk defined upsert is not possible falling back to append!").
  Pre-created tables (the Console does this via pyiceberg) must therefore have `identifier-field-ids` set, otherwise
  you get an ever-growing append table and no error (#294 history).

---

## C. Commit cadence, small-file strategy, who owns compaction

**Cadence.** One Iceberg commit per destination table per engine batch, plus one `OverwriteFiles` commit to the offset
table per offset flush (the shipped example sets `offset.flush.interval.ms=0` = every batch). Default
`batch-size-wait=NoBatchSizeWait` → commits whenever the engine hands over a batch (`max.batch.size` default 2048,
`poll.interval.ms` default 500 ms) → sub-second commits of tiny files under steady change flow. This is the
project's own #1 FAQ-level complaint (#229 "Problem with small datafiles", 2023).

**The "commit every 60 s" equivalent** is `MaxBatchSizeWait` (VERIFIED `MaxBatchSizeWait.waitMs`): after each batch
the consumer thread sleeps in `wait-interval-ms` steps until the engine's streaming queue holds ≥ `max.batch.size`
events or `max-wait-ms` (default 300000 ms) elapses. Example from the docs/blog: `max-wait-ms=60000`,
`max.batch.size=50000`, `max.queue.size=400000`. Properties of this mechanism:
- It is **global to the process**, not per table: while waiting, nothing is committed for any table; when the batch
  arrives it fans out into one commit per touched table, so a 60 s tick on a DB with 80 active tables yields ~80 data
  files + ~80 delete files + 80 commits. Small files scale with (tables × partitions × ticks), not with volume.
- It is **disabled while a snapshot is running** ("don't wait if snapshot process is running"), so initial and
  incremental snapshots commit every `max.batch.size` rows. A 10 M-row table snapshotted at the default 2048 →
  ~4900 commits and files; at 50000 → ~200 (**INFERENCE**, arithmetic). Incremental snapshots (chunk default 1024 rows)
  are worse: one file per chunk. PR #693 ("streaming snapshot flush with persistent writer", +2481 lines, 2026-04)
  tried to fix exactly this and was **closed without merge**.
- `write.target-file-size-bytes` only *splits* oversized output; it never merges. No size-based flush exists.
- Backlog behaviour: the queue is bounded in memory (`max.queue.size`, optional `max.queue.size.in.bytes`); when the
  sink is slow the engine stops polling and the source retains WAL/binlog. No disk buffer.

**Catalog commit rate.** Per source DB: (tables touched per tick + 1 offset commit) per tick. Four servers at a 60 s
tick with tens of active tables each → low hundreds of Nessie commits/minute in the worst minute; each commit is a
Nessie→Postgres round-trip with optimistic retry. Maintenance jobs on the same table will occasionally conflict and
retry (`commit.retry.num-retries` default 4). Nessie on Postgres should sustain this at the platform's scale, but it
is a new hot path for the catalog (**INFERENCE**).

**Who owns compaction: you do — always.** FAQ (VERIFIED): "To optimize read performance, you must run periodic table
maintenance jobs to compact data and rewrite the delete files. This is especially critical for upsert mode." Maintainer
to a user whose 15–20 GB Oracle DB became 300 GB→1 TB on S3 in three weeks (#466, 2025-01): "You need to run periodic
maintenance jobs in parallel". The sink does no `rewrite_data_files`, no `expire_snapshots`, no orphan cleanup, no
manifest rewrite. Only `write.metadata.delete-after-commit.enabled` / `previous-versions-max` pass-through trims the
metadata.json chain. Therefore the platform's hourly Spark maintenance job (or Trino `ALTER TABLE EXECUTE optimize /
expire_snapshots / remove_orphan_files`) is **not** deletable on this path; it becomes more important, because every
hour of skipped compaction accumulates equality deletes that Trino must apply at read time.

---

## D. Schema evolution and semi-structured (MongoDB) handling

**Relational schema evolution (VERIFIED docs + `IcebergTableOperator.applyFieldAddition`).**
- `allow-field-addition=true` (default): per batch, events are grouped by their Connect schema; for each group
  `updateSchema().unionByNameWith(newSchema).setIdentifierFields(...)` is applied and committed only if it changes
  the schema. New columns are added; safe widenings (int→long, float→double) are applied.
- Column dropped at source → stays in Iceberg, new rows null. Column renamed → old column goes null, new column added.
- **Incompatible type change → exception → the engine stops.** Because one Debezium Server serves the whole database,
  one table's `decimal→int` or `long→timestamp` change halts CDC for **every table of that DB** until an operator
  performs the manual procedure in `docs/migration.md` (rename the Iceberg column, restart, live with two columns +
  `COALESCE`). Historical trap (#580): timestamp columns created as `timestamptz` were re-derived as `timestamp` after
  an interrupted incremental snapshot → hard stop. There is no per-table isolation, no DLQ, no "skip and continue".
- Type mapping is the sink's own (`StructSchemaConverter`): Date→date, Timestamp/Micro/Nano→timestamp,
  ZonedTimestamp→timestamptz, **Time types disabled** ("not supported by spark"), decimals depend on
  `decimal.handling.mode`. The docs' reference table lists `debezium.source.decimal.handling.mode` default **`double`**
  and `time.precision.mode` default `isostring` — i.e. the documented baseline stores money as IEEE doubles. A
  `IcebergChangeConsumerDecimalTest` exists, so `precise` works, but you must set it deliberately. Given the platform's
  own correctness audit found "decimal-brick"/"temporal→int" defects, this is the same class of trap.

**MongoDB (VERIFIED `IcebergChangeConsumerMongodbTest`).** Works through `ExtractNewDocumentState` + a
`ReplaceField$Key` SMT renaming the key `id→_id` ("IMPORTANT !!! FIX MongoDbConnector KEY FIELD NAME"), and the test
runs with `allow-field-addition=false`. Two modes for nested data:
1. Struct mode: `ExtractNewDocumentState` infers a Connect schema per document; nested objects become Iceberg structs,
   arrays become lists, maps become maps (Struct/Map/Array cannot be PK). Schema is inferred *per event*, so
   heterogeneous documents produce many "different schema" groups per batch and repeated `unionByName` commits; arrays
   with mixed element types or a field that flips type between documents hit the incompatible-type hard stop above
   (**INFERENCE** from the converter code paths + Debezium's known schemaless-inference behaviour).
2. `nested-as-variant=true`: all nested data lands in an Iceberg **VARIANT** column, table forced to format v3,
   and identifier fields are **not** created ("Identifier fields are not supported when data consumed to variant
   fields") → **append-only**; no upsert for MongoDB in this mode. Reader support for VARIANT: Trino 481 (2026-05)
   "experimental support for the variant type for Iceberg v3 tables"; the platform's Spark 3.5.1 + Iceberg 1.9.0
   runtime predates Iceberg's Spark variant support (**INFERENCE**; needs verification against the platform's exact
   runtime).

---

## E. Snapshot/backfill, incremental, multi-table per source

- **Snapshots** are Debezium's own: `snapshot.mode` (initial/no_data/…), plus incremental snapshots via signals. Kafka
  signal channel is obviously unavailable; source-table and file channels work (#580 used the file channel). Snapshot
  events (`op=r`) flow through the same dedup path; PR #698 (2026-05) fixed dedup crashing on snapshot events with a
  missing `__op`. Snapshot files are small (see C).
- **Multi-table per source**: native — one server handles `table.include.list` of one DB; events are grouped by
  destination per batch; `concurrent-uploads>1` writes tables in parallel on virtual threads with a semaphore.
  **Critical history**: until PR #699 (merged 2026-05-05, "fix: re-throw exceptions in processTablesInParallel to
  prevent silent data loss"), a failed parallel table write was only *logged*, the batch was marked processed and the
  offset committed — "data for any table whose write failed is permanently lost". Whether the parallel path existed in
  1.0.0.Final (2025-11-27) and shipped with the bug is **INFERENCE** (likely yes; fix landed between 1.0.0 and 1.1.0).
- **Table naming**: `<namespace>.<prefix><topic with dots→underscores>`; `destination-regexp(-replace)` can fold
  partitioned source tables into one; nested namespaces with `.` supported since #695 (2026-05).
- **Partitioning**: `partition-by` globally or `partition-by.<destination>` per table with
  `year/month/day/hour/bucket/truncate` (PR #666, 2025-12). Invalid spec silently yields an unpartitioned table.
- **Backfill/replay**: there is no replay. If a Silver table is corrupted or a transform needs to be re-run, the only
  source of truth is the operational DB → re-snapshot (with the small-file storm from C). Contrast: today's Bronze
  changelog with 30-day TTL lets you re-MERGE for the last 30 days without touching the source.

---

## F. Ordering / exactly-once / dedup guarantees

- **Delivery = at-least-once**, by construction. Iceberg data commit and offset commit are two separate catalog
  commits; a crash between them replays the batch. Debezium engine docs (VERIFIED): "when an application restarts
  after a crash it may see up to n duplicates, where n is the size of the batches … n * m" with periodic flush. Debezium
  Server docs (VERIFIED, on the JDBC sink but runtime-general): "Debezium Server does not currently provide the same
  delivery guarantees that are available when running the connector in other runtimes such as Kafka Connect. In
  particular, features that depend on the runtime for offset management, exactly-once semantics, or automatic error
  handling and retries may behave differently or may not be available."
  - In **upsert mode** replay is idempotent (delete-by-key then re-insert of the same final state) → converges.
  - In **append mode** replay produces **duplicate rows** with no dedup key unless you add `__lsn`/offset columns and
    dedupe downstream. (The current Bronze design's `__lsn`/`__kafka_offset` columns would have to be re-derived from
    Debezium `source` fields; `__kafka_*` disappear entirely.)
- **Ordering**: a single engine thread delivers events in source-commit order; the per-table writer processes them in
  list order. Within-batch dedup keeps the **last** event per key (since PR #661, merged 2026-02-10, after the maintainer
  confirmed with Debezium devs that events arrive ordered). Before that, dedup re-sorted by `__source_ts` and then an op
  priority that made DELETE beat a later INSERT in the same ms — the reporter's `insert; delete; insert` produced the
  first insert's value. If you set `upsert-dedup-column`, the old ts+op-priority ordering is re-enabled (VERIFIED
  `compareByTsThenOp`). Cross-batch ordering is guaranteed by Iceberg sequence numbers (equality deletes only apply to
  older files). Parallel uploads are across tables, so per-key order is preserved.
- **Exactly-once**: no. **Transactional consistency across tables**: no (each table commits separately; a reader can
  see table A's batch before table B's). **DLQ / poison-pill tolerance**: none; an unconvertible event stops the engine.

---

## G. Operational surface: what is config vs code; UI; GitOps fit

- **100 % configuration.** One `application.properties` per source DB (source connector props with `debezium.source.`
  prefix, sink props, SMTs, Quarkus logging/metrics). Custom code is only needed for a non-default table mapper or a new
  `BatchSizeWait` (CDI beans). The platform's per-table KafkaConnector CR pairs (1 source + N Iceberg sinks) collapse
  into **one artifact per source DB**.
- **Kubernetes/GitOps.** Debezium ships an Operator with a `DebeziumServer` CR (`spec.image` overrides the version;
  `spec.sink.type/config`, `spec.source.class/config`, `spec.transforms`, `spec.runtime.storage` for the data PVC,
  JMX/metrics/OpenTelemetry blocks) (VERIFIED operator docs). Running the memiiso image via `spec.image` +
  `sink.type: iceberg` is therefore a plain CR in git → ArgoCD-friendly and arguably simpler than what the Console
  renders today. Caveat: the CR is `v1alpha1`; and running **more than one replica of the same server is fatal**
  (two engines reading the same slot/offsets and double-writing) — HA is "Deployment replicas=1 + K8s restart +
  at-least-once recovery" (**INFERENCE** from the offset model).
- **Debezium Management Platform (UI)**: "All Debezium Server sinks are available as destination", but it deploys the
  official Debezium Server image; the Iceberg sink lives outside that image, so UI-driven Iceberg pipelines would need
  a custom image in the platform — not verified to work (**INFERENCE**). Debezium's own 2026 "Kafka-less" blog
  (2026-07-06) showcases only the JDBC sink and says DS "is not intended to replace Kafka Connect".
- **Observability**: Debezium JMX metrics (snapshot/streaming MBeans are what `MaxBatchSizeWait` reads), Quarkus
  health on 8080, Quarkus management interface + **OpenLineage** emission per commit (#696, 2026-05) — a plus for the
  platform's lineage UI ambitions. Logs: "Committed N events to table!" per table per batch; progress line every 15 min.
- **Console impact**: the connector-rendering module would render `DebeziumServer` CRs instead of `KafkaConnector`
  CRs; table pre-creation via pyiceberg remains compatible (`loadIcebergTable` → else create) provided identifier
  fields and partition spec are set; DLQ topic management, sink-per-table lifecycle, Connect REST status polling and
  Kafka topic/ACL management would all disappear.

---

## H. Maturity / adoption / maintenance status (with dates and numbers)

All figures from the GitHub API on 2026-09-10 (VERIFIED):
- Repo created 2021-01-17. **327 stars, 72 forks, 8 watchers**, Apache-2.0, not archived, last push 2026-09-10
  (dependabot). Listed on iceberg.apache.org's vendors/integrations nav as "Memiiso Debezium".
- Releases: 22 total. 0.1.0.Alpha 2021-04 → 0.2.0.Final 2022-09 → 0.3.0.Final 2024-03 → 0.4–0.8 through 2024 →
  0.9.0.Final 2025-04 → 1.0.0.Alpha1 2025-05 → **1.0.0.Final 2025-11-27** → **1.1.0.Final 2026-07-18** → rolling
  `latest` 2026-07-31. Cadence: roughly one Final every 2–4 months since 2024; a 1.5-year gap 2022–2024.
- Contributors: **ismailsimsek 313 commits; next human 11 (racevedoo), 8 (kinolaev), 3, 3, 2, 2, then 1s;
  dependabot 163.** Of the last 30 commits (2026-01→2026-07): ~11 dependabot, ~10 maintainer, 9 from six other people
  (Ivan Senyk/MrIvv: 3 features incl. the data-loss fix; Stuart Lewis: 2 fixes; Jan Soubusta; Walid Boudaoud; Sergei
  Nikolaev; Davy Van Den Steen). 61 commits in the last 52 weeks. Verdict: **effectively a one-maintainer project with a
  thin but real 2026 contributor tail**; the maintainer's Debezium-blog byline (2021) says he is a data engineer in
  Munich, i.e. this is a side project, not a vendor product.
- Issues: 112 issues ever, **1 open** (#738), **7 opened in the last 12 months**. A stale-bot closes issues after
  180 + 14 days and PRs after 30 days, so "1 open" is hygiene, not health; 7 issues/year is a *low-usage* signal.
- The one open issue is the maintainer's own, 2026-09-06, "Release synchronisation with upstream Debezium": "current
  repo is usually behind debezium releases also iceberg"; "current code changes and debezium release upgrades are
  bundled". Today master pins Debezium 3.6.0 while upstream is 3.6.2/3.7.0.Beta1 (dependabot PRs #739/#743 open).
- Notable defect history: #403 (2024) equality-delete files written with full after-row (correctness); #248 (2023)
  duplicates on partitioned upsert (documented limitation); #661 (2025-12→2026-02) wrong dedup ordering; **#699
  (2026-05) silent permanent data loss with `concurrent-uploads>1`**; #690 (2026-04) offset store incompatible with
  Unity Catalog; #580 (2025-06) type flip after interrupted snapshot; #466 (2025-01) 20 GB DB → 1 TB S3 without
  compaction.
- Test suite: ~50 test classes incl. Testcontainers PG/MySQL/MongoDB sources, Nessie/REST/JDBC catalogs, MinIO,
  upsert v2/v3, variant, decimal, temporal, offset store, schema history (VERIFIED file list). No SQL Server or Oracle
  integration test in the tree.
- **Debezium's blessing**: partial. (a) 2021 guest post on debezium.io by the author. (b) Debezium **3.6 reference docs
  now contain an "Apache Iceberg" sink section**, but it states verbatim: "The Debezium Server sink for Apache Iceberg
  is a community-maintained, open-source project. Please note that it is maintained in a separate repository"
  (VERIFIED). (c) The official `debezium/debezium-server` repo at v3.7.0.Beta1 contains **no Iceberg module** (modules:
  kafka, kinesis, pubsub, pulsar, redis, http, nats-*, rabbitmq, rocketmq, eventhubs, infinispan, pravega, sns, sqs,
  milvus, qdrant, instructlab, jdbc, **databricks-zerobus, fluss**). So Debezium 3.x did **not** gain a first-party
  Iceberg sink; it gained JDBC, vector-DB, Databricks Zerobus and Fluss sinks. (d) No Red Hat support path.
- Public production users: none found with a case study. Issue reporters imply real deployments (Oracle 15–20 GB DB
  on S3; Unity Catalog; Polaris; GCS). Contributor affiliations suggest GoodData and Ledger (**INFERENCE** from
  GitHub handles).

---

## I. Implications for the platform: what could be deleted/replaced/reconfigured; what is lost; risks

**What could be deleted or turned into config (for the CDC lanes only):**
- Strimzi Kafka + Kafka Connect + the Iceberg Kafka Connect sink 1.9.0 + Apicurio (Avro) — *if and only if* nothing
  else needs Kafka. **The platform's Camel/nginx/mqtt/rabbitmq lanes are Kafka Connect lanes**, so Kafka and Connect
  stay unless those lanes are also redesigned. Realistic deletable set: the **Iceberg KC sink connectors** (one per
  table), **the Spark MERGE `ScheduledSparkApplication`** (Silver is written directly), the Bronze tables' TTL job,
  DLQ topics, and the Console's KafkaConnector source+sink rendering (replaced by one `DebeziumServer` CR per DB).
- What stays regardless: Nessie, S3, **Spark (or Trino) hourly maintenance — now mandatory and more frequent**,
  Trino, Superset, dbt, Keycloak, the Console for table pre-creation/lineage.
- Resulting topology per source DB: 1 Debezium Server pod (Deployment, replicas=1, small PVC or Iceberg-backed
  offsets) → Silver-shaped tables in Nessie. Four pods total for the university.

**What is lost:**
1. **Bronze as replayable changelog.** One server = one mode. You get *either* upsert (Silver) *or* append (Bronze),
   not both, unless you run two servers per DB — two replication slots / two CDC readers on the production database
   (doubling WAL retention risk on PG and CDC load on MSSQL). Re-materializing Silver after a bug means re-snapshotting
   the source.
2. Kafka's decoupling: multi-consumer fan-out (e.g., a future stream processor, search index, notifications),
   durable disk buffer during S3/Nessie outages (now the buffer is the source DB's WAL), consumer-group HA.
3. DLQ / `errors.tolerance` / per-table blast-radius isolation (the platform's F-spike deliberately built dedicated
   sinks per table; this path is the exact opposite: one engine per DB, one poison event or one incompatible DDL stops
   all tables of that DB).
4. Schema Registry contracts (Avro schemas as the interface for other consumers); schema evolution is now the sink's
   `unionByName` policy.
5. Kafka-derived metadata columns (`__kafka_offset`, `__kafka_partition`); `__lsn` would have to come from
   `source.lsn`/`source.change_lsn` via `add.fields`.
6. Exactly-once-ish semantics of the Kafka Connect Iceberg sink's coordinator (Kafka offsets committed with the
   Iceberg snapshot) → at-least-once; harmless in upsert mode, harmful in append mode.

**New risks:**
- **Read-path performance on equality deletes** in Trino (open OOM issue #26059; repeated fixes through Trino 482),
  which makes compaction frequency a correctness-adjacent SLO rather than housekeeping. The 15-min MERGE today yields
  clean CoW files; this path yields files + a growing delete set between compactions.
- **Format v3 by default** vs the platform's readers: Trino added "creating, writing to or deleting from Iceberg v3
  tables" and v3 support in `optimize/expire_snapshots/remove_orphan_files` only in **Trino 480 (2026-03-24)**, and
  the *current* connector docs (483) still say "Support for format version 3 is experimental … Version 3 support is
  experimental; row-level updates, deletes, and OPTIMIZE are not supported" — the docs and the release notes disagree,
  which is itself a signal. Trino 482 fixed "data loss caused by cleanup of a failed write deleting active data files
  when deletion vectors are enabled". The platform's Spark 3.5.1 + Iceberg **1.9.0** runtime is past Iceberg 1.8.0
  (2025-02-13) which added DV read/write in core, so it should read v3/DV tables, but the exact Spark-3.5 runtime
  support for v3 maintenance procedures must be verified (**INFERENCE**). Mitigation: pin `format-version=2` (documented
  option) — but then you are on plain equality + position deletes, the slowest read path.
- **Single-process blast radius and no HA** per source DB; restart = re-snapshot risk if offsets are lost (offsets in
  Iceberg mitigate; a PVC-file offset store on a rescheduled pod does not).
- **Dependency lag and bus factor**: the maintainer himself flags being behind Debezium/Iceberg; a critical
  silent-data-loss bug shipped and was fixed by an external contributor in 2026; Debezium security fixes (3.6.2) arrive
  when dependabot PRs get merged.
- **Documented defaults that are wrong for a finance/registrar database**: `decimal.handling.mode=double`,
  `upsert-keep-deletes=true` (soft deletes), `NoBatchSizeWait` (small files), Time types dropped.
- **Snapshot storms**: no batching during snapshot; no persistent writer; initial load of the university's DBs will
  create tens of thousands of small files and Nessie commits that the hourly job must then compact.
- **Catalog commit pressure** on Nessie/Postgres from (tables × ticks) + offset overwrites (**INFERENCE**, see C).
- **Ordering edge**: enabling `upsert-dedup-column` re-enables the older ts+op-priority reordering whose failure mode
  (#661) is documented; leave it unset.

---

## J. Evidence (links, file paths, quotes) — mark VERIFIED vs INFERENCE

VERIFIED (primary sources fetched 2026-09-10):
- Repo & API: https://github.com/memiiso/debezium-server-iceberg — stars 327, forks 72, watchers 8, created
  2021-01-17, pushed 2026-09-10; releases list (1.1.0.Final 2026-07-18, 1.0.0.Final 2025-11-27, …); contributors
  (ismailsimsek 313, dependabot 163, racevedoo 11, kinolaev 8 …); 112 issues total, 1 open, 7 created since 2025-09.
- `pom.xml`: `version.debezium=3.6.0.Final`, `version.iceberg=1.11.0`, Java 21, Spark 4.0.3 (tests only);
  `debezium-server-iceberg-dist/pom.xml`: connectors postgres/mysql/mariadb/mongodb/oracle/sqlserver/db2;
  sink pom: iceberg-core/data/parquet/orc/arrow, iceberg-nessie, aws/gcp/azure bundles, hive-metastore.
- `debezium-server-iceberg-sink/src/main/java/io/debezium/server/iceberg/IcebergChangeConsumer.java` — batch
  grouping, sequential/parallel table processing, `markProcessed` loop ("workaround! somehow offset is not saved to
  file unless we call committer.markProcessed per event"), `batchSizeWait.waitMs(...)`, table auto-create rules
  (variant ⇒ no identifier fields; format version selection).
- `.../tableoperator/IcebergTableOperator.java` — `deduplicateBatch`, `compareByTsThenOp` (returns -1 ⇒ keep last
  when `upsert-dedup-column` unset), `applyFieldAddition` (`unionByNameWith`), `addToTablePerSchema` (`RowDelta` vs
  `AppendFiles` commit), OpenLineage emit.
- `.../tableoperator/BaseDeltaTaskWriter.java` — `deleteKey` + `write` logic, `BaseEqualityDeltaWriter`,
  `DeleteGranularity.PARTITION`, `PartitioningDVWriter`.
- `.../tableoperator/IcebergTableWriterFactory.java` — append fallback when no identifier fields ("Table don't have
  Pk defined upsert is not possible falling back to append!"), `useDv = formatVersion > 2` default,
  `equalityFieldIds`/`equalityDeleteRowSchema` = identifier fields only.
- `.../batchsizewait/MaxBatchSizeWait.java` — "don't wait if snapshot process is running"; loop until queue ≥
  `max.batch.size` or `max-wait-ms`. `BatchConfig.java` — defaults `max-wait-ms=300000`, `wait-interval-ms=10000`,
  `batch-size-wait=NoBatchSizeWait`, `concurrent-uploads=1`.
- `.../IcebergConfig.java` — `upsert=false` default, `upsert-keep-deletes=true`, `format-version=3`,
  `nested-as-variant=false`, `allow-field-addition=true`, per-table `partition-by.<destination>`.
- `.../offset/IcebergOffsetBackingStore.java` — `OverwriteFiles ... overwriteByRowFilter(alwaysTrue())` per save.
- `.../converter/StructSchemaConverter.java` — type mapping; "Time type is disabled for the moment, it's not supported
  by spark"; Struct/Map/Array handling; variant branch.
- `docs/iceberg.md` (config table incl. `decimal.handling.mode` default `double`, `time.precision.mode` default
  `isostring`; upsert/dedup/keep-deletes; MaxBatchSizeWait; naming; partition-by; schema-change handling),
  `docs/faq.md` ("you must run periodic table maintenance jobs…"), `docs/migration.md` (incompatible type change
  procedure), `docs/icebergevents.md` (deprecated in favour of variant), `examples/nessie/config/application.properties`
  (NessieCatalog v2 API, MinIO, Iceberg-backed offsets/history, `offset.flush.interval.ms=0`).
- Tests: `IcebergChangeConsumerMongodbTest.java` (ExtractNewDocumentState + `ReplaceField$Key id:_id`,
  `allow-field-addition=false`), presence of Nessie/REST/JDBC catalog tests, `IcebergChangeConsumerUpsertV3Test`,
  `IcebergChangeConsumerVariantTest`, `IcebergChangeConsumerDecimalTest`.
- Issues/PRs: #403 (2024-08-30, equality delete content; fix = key-only, Flink/Tabular pattern), #248 (2023-10-31,
  partitioned upsert duplicates), #630 (2025-09-29; maintainer: "Copy-on-write is not supported…"), #229 (2023-09-07,
  small files), #466 (2025-01-02, 20 GB → 1 TB), #661 (merged 2026-02-10, keep-last dedup; maintainer "Confirmed that
  debezium delivers the events in order"), #690 (merged 2026-04, offset overwrite), #693 (closed **unmerged**, snapshot
  persistent writer), #698 (2026-05, snapshot `__op`), #699 (merged 2026-05-05, "Critical — silent permanent data
  loss. Affects all users with concurrent_uploads > 1"), #720 (merged 2026-07, v3 DV default), #738 (open 2026-09-06,
  release sync lag), #580 (2025-06, timestamptz→timestamp flip).
- Debezium 3.6 docs, Debezium Server page (https://debezium.io/documentation/reference/stable/operations/debezium-server.html):
  "each instance of Debezium Server runs exactly one connector"; Apache Iceberg sink section with quote "community-
  maintained, open-source project … maintained in a separate repository"; JDBC-sink paragraph on delivery guarantees.
  Debezium Engine page: "may see up to n duplicates … n * m duplicates". Debezium Operator page: `DebeziumServer`
  `spec.image`, `spec.sink.type/config`, runtime storage. Debezium Platform page: "All Debezium Server sinks are
  available as destination".
- debezium/debezium-server module list at v3.7.0.Beta1 (no iceberg module; databricks-zerobus, fluss present).
- Debezium blog 2021-10-20 (Ismail Simsek): "The upsert mode uses the Iceberg equality delete feature…";
  "deduplication is done on each batch and only the last version of the record kept"; MaxBatchSizeWait example
  (`max-wait-ms=60000`, `max.batch.size=50000`). Debezium blog 2026-07-06 (Kafka-less pipelines): JDBC only; "not
  intended to replace Kafka Connect".
- Iceberg spec (https://iceberg.apache.org/spec/): DVs v3; "Position delete files are deprecated in v3"; equality
  delete definition; "Row lineage does not track lineage for rows updated via Equality Deletes". Iceberg releases page:
  1.8.0 (2025-02-13) "Support for reading Deletion Vectors (#11481) … writing Deletion Vectors (#11476)"; 1.9.0
  2025-04-28; 1.11.0 (2026-05-19) "Deprecate Position delete files with row data (#14045)".
- Trino: connector docs (483) "supports Apache Iceberg table spec versions 1 and 2. Support for format version 3 is
  experimental" and "Version 3 support is experimental; row-level updates, deletes, and OPTIMIZE are not supported";
  release notes 480 (2026-03-24) v3 create/write/delete + optimize/expire/orphans + "Avoid worker crashes … larger
  number of equality deletes"; 481 variant experimental; 482 DV data-loss fix + equality-delete memory; 483 orphaned DV
  fix. Issues #13092 (2022), #17114 (2023), #18396 (2023–24), #26059 (open, 2025-06).

INFERENCE (not verified against a primary source, or derived by reasoning):
- Intra-batch overwrites become position deletes/DVs (Iceberg `BaseEqualityDeltaWriter` semantics).
- Snapshot file-count arithmetic; Nessie commit-rate estimate; net rewrite volume vs CoW MERGE.
- Whether 1.0.0.Final shipped the #699 data-loss bug.
- ghcr image coordinates; contributor employer affiliations; Debezium Platform ability to run a custom sink image.
- Spark 3.5.1 + Iceberg 1.9.0 read/maintenance support for v3/DV tables and VARIANT columns.
- Equality-delete deprecation thread details (from secondary blogs/search summaries; spec text itself verified).
- HA model (replicas must be 1) derived from the single-offset-row design.

---

## K. Your one-paragraph verdict

For *this* platform's scale — a handful of GB-sized operational databases at one university — debezium-server-iceberg
is a technically credible way to replace **the Iceberg Kafka Connect sinks plus the Spark MERGE job** for the CDC lanes:
four config-only pods, direct upsert into Silver-shaped tables, native Nessie/REST support, and a design that is
sane (equality-delete MoR, in-batch dedup, keep-last ordering, Iceberg-backed offsets). But it is **not** a credible
replacement for "Kafka + Connect + Spark" as a whole, and it is not a foundation I would sell to further customers.
Reasons, bluntly: (1) it is a one-maintainer side project that lags Debezium/Iceberg by the maintainer's own admission
and shipped a silent-data-loss bug fixed by an outsider in 2026; Debezium documents it but explicitly disclaims it;
(2) it does not remove Spark — it makes hourly compaction load-bearing for Trino query correctness/stability because
its whole write model is equality deletes, the one Iceberg feature the format community is trying to retire and the
one Trino still fixes OOMs for; (3) it forfeits the replayable Bronze changelog, DLQ, per-table isolation and multi-
consumer fan-out, and one incompatible DDL on one table halts CDC for an entire database; (4) since the Camel/nginx
lanes keep Kafka Connect alive anyway, the real deletion payoff is "N sink connectors + one Spark job", not the
streaming platform. Recommended posture: treat it as an **optional "lite" ingestion profile** for small single-tenant
installs (pin `format-version=2` or verify Trino ≥ 480 and Spark/Iceberg runtime v3 support first; set
`decimal.handling.mode=precise`, `upsert-keep-deletes=false` or filter, `MaxBatchSizeWait` ≈ 60 s, `concurrent-uploads
= 1`), and keep the Kafka Connect + Bronze + MERGE backbone as the product default — while stealing its two good ideas:
Silver written by an idempotent key-based delta writer instead of 15-minute CoW MERGE, and offsets/lineage as Iceberg
tables.
