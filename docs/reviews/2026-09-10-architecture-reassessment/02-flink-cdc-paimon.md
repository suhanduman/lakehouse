# 02 — Apache Flink CDC 3.x pipelines + Apache Paimon (and Flink → Iceberg)

Fresh-eyes architecture review, 2026-09-10. No local repository files were read. Every claim is tagged VERIFIED (primary source fetched: official docs, source code, GitHub API, ASF mailing-list API) or INFERENCE (my reasoning or a secondary source). Versions as of today: Flink CDC 3.6.0 (2026-03-31; master = 3.7-SNAPSHOT), Paimon 2.0.0 (2026-08-07), Iceberg 1.11.0 (2026-05-20), Flink 2.2.x / 1.20.x, Flink Kubernetes Operator 1.15.0 (2026-05-26).

Platform under review (for reference): Debezium 2.7.3 on Strimzi Kafka Connect → Kafka (topic per table) → Iceberg Kafka Connect sink 1.9.0 (append-only Bronze, 60 s commits) → Spark 3.5.1 ScheduledSparkApplication every 15 min running copy-on-write `MERGE INTO` Silver (bucket(16, pk)) → hourly Spark maintenance per table; Nessie (Iceberg REST) catalog; Trino/Superset/dbt.

---

## A. Architecture (components, hops, where state lives)

### A.1 Flink CDC 3.x "pipeline" model
- One YAML file = one Flink job: `source` → (optional `transform`, `route`) → `sink`, plus a `pipeline` block (`parallelism`, `schema.change.behavior`, `operator.uid.prefix`, `sink.partitioning.strategy`, …). VERIFIED (data-pipeline doc).
- Whole-database / multi-table by regex in `tables:` (e.g. `adb.\.*, bdb.user_table_[0-9]+`). One job can carry all tables of one source database into many sink tables (dynamic table creation in the sink). VERIFIED.
- Pipeline **sources** in released 3.6.x: **MySQL, PostgreSQL, Oracle**. **SQL Server** pipeline source exists only on master (FLINK-39252, merged 2026-06-16; unreleased). **MongoDB has no pipeline source** — only the legacy `mongodb-cdc` SQL/DataStream connector (docs list MongoDB 3.6–7.0). VERIFIED (overview table, source tree, commit log).
- Pipeline **sinks**: Paimon, Iceberg, Kafka, Hudi (3.6), Fluss, StarRocks, Doris, Elasticsearch, OceanBase, MaxCompute. VERIFIED.
- Legacy "Flink sources" (SQL connectors) exist for MySQL, PostgreSQL, SQL Server, Oracle, MongoDB, Db2, TiDB, Vitess, OceanBase — but these do not participate in the YAML pipeline / schema-evolution framework. VERIFIED.
- **Debezium embedded version: 1.9.8.Final** (root pom on master and release-3.6). Flink CDC back-ports fixes by overwriting Debezium classes. Upgrade to 2.x was discussed 2024-05-23 (blocker: Flink still defaulted to JDK 8; "the classes differ significantly between 2.x and 1.9"); FLINK-36605 "Upgrade Debezium version to 2.7.x" opened 2024-10-27; still 1.9.8 in Sept 2026 even though 3.6 moved to JDK 11. VERIFIED (pom, ASF list API).
- Legacy `postgres-cdc` docs list PostgreSQL 9.6–14 only; `sqlserver-cdc` 2012–2019. VERIFIED (docs). Whether PG 15/16/17 or SQL Server 2022 work is not documented → INFERENCE: probably works via pgoutput, but unsupported per docs.

### A.2 Runtime topology (Flink CDC → Paimon or Iceberg)
```
Source DB ──(Flink CDC source: incremental-snapshot framework, chunked, lock-free)──▶
  SchemaOperator (coordinates schema-change events with a SchemaRegistry in the JobManager)
  ──▶ PrePartition / BucketAssign (hash by table id + PK / Paimon bucket) ──▶
  Writer (per-table TaskWriter / Paimon StoreSinkWrite) ──▶ Committer (single, at checkpoint)
```
- **No Kafka hop.** Data goes DB → Flink → lake. Kafka appears only if you choose the Kafka pipeline sink (Debezium-JSON/Canal-JSON topics) and run a second pipeline/consumer. VERIFIED (connector list).
- **Where state lives**
  - Flink checkpoints (S3/MinIO via `state.checkpoints.dir`): source split assignments + WAL/binlog/LSN offsets, cached table schemas, writer committables. Small (MBs) for CDC-only jobs. VERIFIED that these are checkpointed; size is INFERENCE.
  - JobManager memory: schema registry, and for Paimon `sink.writer-coordinator` manifest cache (default 2 GB if enabled). VERIFIED (write-performance doc).
  - TaskManager memory/local disk: Paimon write buffers (`write-buffer-size`), compaction merge readers, dynamic-bucket key→bucket HASH index ("**100 million** entries in a partition takes up **1 GB** more memory"), `lookup` changelog/deletion-vector cache on local disk. VERIFIED.
  - Table state: Paimon snapshot/manifest files in the warehouse (+ optional Iceberg-compatible metadata); Iceberg sink → Iceberg snapshots; "Flink streaming write jobs rely on snapshot summary to keep the last committed checkpoint ID … expiring snapshots and deleting orphan files could possibly corrupt the state of the Flink job." VERIFIED (Iceberg flink-writes.md).
  - Flink HA metadata: Kubernetes ConfigMaps (operator-provided "Flink's Kubernetes HA services with standby JobManagers"). VERIFIED (operator overview).
- **Kubernetes deployment (Flink CDC on the operator)**: build a custom image bundling flink-cdc-dist + connector jars; mount the pipeline YAML as a ConfigMap; `FlinkDeployment` with `entryClass: org.apache.flink.cdc.cli.CliFrontend`, `args: ['--use-mini-cluster', <yaml>]`, `classloader.resolve-order: parent-first`. Doc notes: "Flink CDC submits a job to a remote Flink cluster by default, you should start a Standalone Flink cluster in the pod by `--use-mini-cluster` in Operator mode" and "submitting with **native application mode** is not supported for now." VERIFIED (deployment/kubernetes.md).

### A.3 Paimon table layout
Table (or partition) → N **buckets**; each bucket = one **LSM tree** (sorted runs across levels) + optional changelog files + optional deletion-vector index. "A bucket is the smallest storage unit for reads and writes, so the number of buckets limits the maximum processing parallelism… recommended data size in each bucket is about 200MB - 1GB." Bucket modes: fixed (`bucket=N`), **dynamic (default, `-1`)**, postpone (`-2`), cross-partition upsert (key-dynamic). "Dynamic Bucket only support single write job." VERIFIED.

### A.4 Flink → Iceberg direct (no Flink CDC)
Flink `IcebergSink`/`FlinkSink` with `upsert(true)` + equality-field columns, or the newer **Dynamic Iceberg Sink** (multi-table, schema-evolving, experimental). Flink CDC's Iceberg pipeline sink is a thin wrapper: `RowDataTaskWriterFactory(table, rowType, 256 MiB, parquet, {}, table.schema().identifierFieldIds(), upsert=true)` — file size and format are **hard-coded**. VERIFIED (IcebergWriter.java lines 63–65, 143–157).

---

## B. Upsert/delete strategy on the lake and write-amplification behavior

| Path | Mechanism | Per-update write cost | Read cost | Where amplification is paid |
|---|---|---|---|---|
| **Platform today**: Spark CoW `MERGE INTO` every 15 min | Rewrites every data file that contains ≥1 matched key | Whole files (up to 256 MiB) rewritten for a handful of changed rows; with bucket(16,pk) and random updates, most of the 16 buckets' files get rewritten each cycle | Best possible (plain files) | On every MERGE run, proportional to table size, not to change volume |
| **Paimon PK table, MOR (default)** | Append sorted L0 file per checkpoint; universal compaction merges sorted runs; deletes are `-D` rows merged away | Only changed rows are written; compaction rewrites each byte O(levels) times (RocksDB-style universal compaction) | Multi-way merge across sorted runs at read time; single-thread per bucket | Background/inline compaction, proportional to ingest volume |
| **Paimon PK table, MOW (`deletion-vectors.enabled`)** — *recommended by Paimon docs* | Writer looks up the old row in the LSM, marks it in a deletion vector for its file, writes the new row to L0 | Changed rows + small DV bitmaps; old data files untouched until compaction | Filter by DV, no merge; parallel-friendly | Lookup during write (needs local-disk cache) + compaction; "files with level 0 will only be visible after compaction. So by default, compaction is synchronous" |
| **Paimon COW** (`full-compaction.delta-commits=1`) | Full merge on every commit | "write amplification is very severe" — same class as Spark CoW MERGE, just more often | Best | Every commit |
| **Flink → Iceberg upsert** (also what Flink CDC's Iceberg sink does) | Per changed key: an **equality delete** row (+ position delete if the key was already written in the same checkpoint) and a new data row | Tiny writes; no file rewrite | Every reader must apply all accumulated equality deletes to all older data files — severe read amplification until compaction | Compaction (`rewriteDataFiles`, `convertEqualityDeletes` → DVs on v3 tables) |

Sources: Paimon table-mode.md ("MOR… Write performance: very good. Read performance: not so good"; "COW… write amplification is very severe"; "MOW… Write performance: good. Read performance: good"), compaction.md, Iceberg flink-writes.md `UPSERT` section. All VERIFIED.

Key mechanics behind "how Paimon avoids rewriting whole files on small updates" (question 5):
1. **Append-only write path**: each Flink checkpoint flushes the in-memory write buffer as a new sorted run (L0 file) — the update is a *new record*, never an in-place file edit. VERIFIED.
2. **Deferred merge**: duplicates for the same PK live in different sorted runs until compaction merges them by `merge-engine` (`deduplicate` default: keep latest; a latest `DELETE` removes the key; `ignore-delete` option). Compaction is triggered when sorted runs ≥ `num-sorted-run.compaction-trigger` (default 5) and **writes stall** at `num-sorted-run.stop-trigger` (default trigger+3). VERIFIED.
3. **Deletion vectors (MOW)** make the old row invisible with a bitmap instead of a rewrite, so readers do not need the merge; the rewrite happens later in compaction, amortised across many updates to the same file. VERIFIED.
4. **Ordering** for out-of-order/at-least-once input: `sequence.field` (e.g. a DB update timestamp or CDC `op_ts`) decides the winner; otherwise input order. VERIFIED.

Flink → Iceberg's equality deletes are the *other* way to avoid rewrites, but with a structural problem: they are keyed by value, so a reader must join every older data file against every newer equality-delete file. Two live correctness threads: apache/iceberg #15305 (open, 2026-02-12, Iceberg 1.10.1 + Flink 2.2.0: "data files and equality deletes are written at the same sequence number in the same snapshot" so co-committed rows are not removed) and #10431 (duplicates; closed via PR #10526). VERIFIED (issue pages). **And the format is retiring the feature: "[VOTE] Deprecate equality deletes in Iceberg V4 (forbid new writes)" passed 2026-08-18 — "7 binding +1's and 17 non-binding +1's, and no 0 or -1's"; spec PR to follow.** VERIFIED (lists.apache.org API). Flink CDC's Iceberg sink hard-codes `upsert=true` → it will need a redesign (DV-based) before any V4 table can be written. INFERENCE (consequence of the two verified facts).

---

## C. Commit cadence, small-file strategy, who owns compaction

- **Cadence = Flink checkpoint interval.** Every checkpoint → one Paimon snapshot (or one Iceberg snapshot) per table, containing one L0/data file per bucket (Paimon) or per writer subtask (Iceberg). "Paimon's write performance is closely related to checkpoint… Increase the checkpoint interval." VERIFIED (write-performance.md). Practical range 1–5 min; sub-minute cadence multiplies small files. INFERENCE for the numbers.
- **Neither pipeline sink is exactly-once**: "Not support exactly-once. The connector uses at-least-once + primary key table for idempotent writing" (Paimon) and identical wording for Iceberg. VERIFIED (both connector docs). PK-less tables are rejected outright ("Only support Paimon primary key table, so the source table must have primary keys"; Iceberg: "Tables with no primary key are not supported"). VERIFIED.
- **Paimon small files**: absorbed by the LSM. Compaction runs (a) *inline* in the writer (default; "Paimon writers will perform compaction as needed during writing records"; synchronous in MOW/`lookup` modes; "If there are too few buckets or resources, full-compaction may cause the checkpoint timeout, Flink's default checkpoint timeout is 10 minutes"), or (b) in a **dedicated compaction job** (`write-only=true` on the table — which also *disables snapshot expiration* in the writer — plus `compact_database` action / `sys.compact_database` procedure, a second long-running Flink job or a scheduled Spark/Flink batch). "There can only be one job working on the same partition's compaction." Snapshot, tag and partition expiration are table properties executed during commit/compaction — no hand-written job. VERIFIED (compaction.md, dedicated-compaction.mdx).
- **Flink CDC Iceberg sink small files**: optional `sink.compaction.enabled` → a `CompactionOperator` that, after `sink.compaction.commit.interval` commits of a table, runs `Actions.forTable(StreamExecutionEnvironment.createLocalEnvironment(), table).rewriteDataFiles().execute()` — i.e. an **in-process local mini-cluster rewrite inside the sink operator**, one table at a time; docs warn "If there are too many tables after enabling it, data flow may be blocked." No `expireSnapshots`, no orphan cleanup, no equality-delete → DV conversion. VERIFIED (CompactionOperator.java, IcebergDataSinkOptions.java). So with Flink CDC → Iceberg you **keep** an external Spark/Flink maintenance job (and must keep the Flink job's last snapshot, see A.2).
- **Native Flink `IcebergSink` (SinkV2, "currently an experimental feature")** offers post-commit maintenance: `rewriteDataFiles()`, `expireSnapshots()`, `deleteOrphanFiles()`, `convertEqualityDeletes()` (needs format-version ≥ 3) — but Flink CDC's sink does not use it. VERIFIED (flink-writes.md, flink-maintenance.md).

---

## D. Schema evolution and semi-structured (MongoDB) handling

- **Framework**: `schema.change.behavior` ∈ `exception | evolve | try_evolve | lenient (default) | ignore`. Events: `create.table, add.column, alter.column.type, rename.column, drop.column, truncate.table, drop.table`; sinks can `include/exclude.schema.changes`. Lenient "converts" changes (e.g. type change → rename+add) and blocks truncate/drop table by default "to avoid unexpected data loss"; `evolve` failures "trigger global failover"; `try_evolve` casting "isn't guaranteed to be lossless". VERIFIED (schema-evolution doc).
- **MySQL**: DDL parsed from binlog → full support. VERIFIED (by construction of the connector; docs).
- **PostgreSQL**: release-3.6 doc still says "Since the Postgres WAL log cannot parse table structure change records, Postgres CDC Pipeline Source does not support synchronizing table structure changes currently", while the 3.6.0 announcement claims "Support schema changes in the PostgreSQL pipeline connector" and master documents `schema-change.enabled` (default **false**; "infers schema change events (add column, drop column, rename column, alter column type) by comparing pgoutput Relation messages against the cached schema"; requires `pgoutput`). Commit FLINK-38959 landed 2026-03-21 with hotfixes 2026-03-25 and follow-ups through 2026-09-02. VERIFIED. Assessment: new, inference-based (a rename is indistinguishable from drop+add at the WAL level), off by default → INFERENCE: treat as beta.
- **SQL Server** (master only): `schema-change.enabled` "Whether to send schema change events so downstream sinks can synchronize table structure changes." VERIFIED.
- **Sinks**: Paimon and Iceberg pipeline sinks both advertise "Schema change synchronization" (add column etc.); Iceberg's `IcebergMetadataApplier` implements add/alter-type/rename/drop via the Iceberg API. VERIFIED (source). Iceberg dynamic sink warning: schema changes "can cause many conflicting commits to the Iceberg catalog and temporarily delay data processing". VERIFIED.
- **MongoDB**: not a pipeline source; the legacy connector "can only convert it to Flink's UPSERT changelog stream… we must declare `_id` as primary key"; MongoDB ≥ 6 pre-images enable `scan.full-changelog`. VERIFIED. **Paimon's own ingestion action** (`mongodb_sync_table/_database`, in paimon-flink-action) *does* handle Mongo with schema evolution, but: "we have set all field data types for synchronizing MongoDB to Paimon as String", "MongoDB schema information is parsed at one level" (top-level fields only; nested → JSON string), `_id` must be the PK. VERIFIED (mongo-cdc.md). Net: Mongo lands as a one-level, all-STRING table — arguably *worse* typing than Debezium's ExtractNewDocumentState + Avro on the current platform. INFERENCE.
- **Paimon CDC-ingestion actions (not Flink CDC YAML)** support a narrower evolution set: "the framework can not rename table, drop columns… `RENAME COLUMN` will add a new column"; type widening only. VERIFIED (cdc-ingestion/index.mdx). Transform: Flink CDC 3.6 added `VARIANT` type and JSON parsing functions in `transform`. VERIFIED (announcement).

---

## E. Snapshot/backfill, incremental, multi-table per source

- Incremental-snapshot framework (chunked by PK, lock-free, parallel) for MySQL/PG/SQL Server/Oracle/Mongo; options such as `scan.incremental.snapshot.chunk.size`, `scan.incremental.snapshot.backfill.skip` ("Skipping backfill may lead to replayed change log events with at-least-once semantics"), `scan.incremental.snapshot.unbounded-chunk-first.enabled` (OOM mitigation), `scan.incremental.close-idle-reader.enabled`. Startup modes: `initial | latest-offset | committed-offset | snapshot | timestamp` (varies by source). VERIFIED (postgres/sqlserver pipeline docs).
- Snapshot → stream handover is automatic; per-table gauges (`numTablesSnapshotted`, `numSnapshotSplitsRemaining`, `snapshotStartTime`, …). VERIFIED.
- **Multi-table per source: yes, but one database per job** for PostgreSQL ("All db values must be the same… CDC only supports connecting to one database") and SQL Server ("All entries in `tables` must belong to the same database"). PG needs one replication slot per job (`slot.name`); newly added tables in a running job depend on connector support (MySQL/Mongo document "scan newly added tables"; PG/SQL Server pipeline docs do not). VERIFIED for the quoted limits; PG new-table support is INFERENCE (not documented).
- Paimon-side backfill cost: during "snapshot / full synchronization phase you can unset `changelog-producer`/`full-compaction.delta-commits` and then enable them again in the incremental phase." VERIFIED (write-performance.md). Dynamic-bucket tables build the key index at start ("initialization takes a long time" for cross-partition mode). VERIFIED.
- Compared with the platform: today's "watermark as Iceberg table property + 15-min MERGE" disappears; the Flink checkpoint *is* the watermark. Re-snapshot of one table = restart that pipeline (or a new job for that table); with one job per database, a re-snapshot of a single table is awkward — INFERENCE.

---

## F. Ordering / exactly-once / dedup guarantees

- **End-to-end**: at-least-once into Paimon/Iceberg, made idempotent by PK upsert. Both docs state exactly-once is not supported. VERIFIED. After a failover, the same change may be re-applied; with `deduplicate` + monotone `sequence.field` this is harmless; with the default input-order merge, a replayed *older* record could win only if replay reorders — Paimon docs recommend `sequence.field` "when the input is out of order". VERIFIED (merge-engine index).
- **Per-key ordering**: pipeline `sink.partitioning.strategy` (`SINK_DEFINED | PRIMARY_KEY | TABLE_ID`) plus Paimon bucket assignment keep all changes of a PK on one writer subtask; Paimon's rule is one writer per bucket. Vanlightly's analysis (2024): "Writer concurrency is generally limited to one writer per bucket"; multiple writers per bucket cause "reordering of operations during merges". VERIFIED (secondary but rigorous source).
- **Commit atomicity on S3-compatible storage**: "for object storage such as OSS and S3, their `RENAME` does not have atomic semantic. We need to configure Hive or jdbc metastore and enable `lock.enabled` option for the catalog. Otherwise, there may be a chance of losing the snapshot." → a Paimon filesystem catalog on MinIO/S3 is **not safe** with any second writer (compaction job, Spark batch fix-up); you need Paimon's Hive/JDBC metastore lock or the Paimon REST catalog. VERIFIED (concurrency-control.md).
- **Flink → Iceberg upsert**: per-key ordering via hash distribution on equality fields; correctness gaps listed in B (#15305 open). VERIFIED.
- **Dedup of the CDC stream itself** (Debezium-style duplicates after connector restart): handled by PK merge, no separate dedup step. INFERENCE (follows from the design).

---

## G. Operational surface: what is config vs code; UI; GitOps fit

Config (YAML/table properties), no code:
- Source/sink/route/transform per database; schema-evolution policy; Paimon table properties (`bucket`, `deletion-vectors.enabled`, `changelog-producer`, `sequence.field`, `snapshot.time-retained`, `partition.expiration-time`, `record-level.expire-time`, `metadata.iceberg.storage`); Iceberg `table.properties.*`. VERIFIED.
- FlinkDeployment CR (operator): image, checkpoint dir, upgrade mode (`savepoint`), resources. VERIFIED.

Still code / hand-rolled:
- A **custom Docker image** per Flink CDC version bundling connector jars + JDBC drivers ("Build a custom Docker image"). VERIFIED.
- Console changes: today the Console renders `KafkaConnector` CRs and pre-creates Iceberg tables with pyiceberg. It would have to render `FlinkDeployment` + ConfigMap (pipeline YAML) instead, and either drop table pre-creation (Flink CDC auto-creates) or pre-create Paimon tables (pypaimon 2.0 exists; VERIFIED it was announced with Paimon 2.0.0 — its DDL capabilities were not verified → INFERENCE). Roughly the same amount of rendering code, different targets. INFERENCE.
- Dedicated compaction job(s) and, for Flink CDC → Iceberg, the existing Spark maintenance job stay. VERIFIED (C).
- UI: the Flink Web UI (per job) + operator status conditions; no product-style multi-pipeline UI in OSS Flink CDC. VERIFIED (operator overview mentions Conditions; Flink CDC ships only a CLI).
- GitOps: `FlinkDeployment` + ConfigMap are ordinary CRs → ArgoCD-friendly; upgrades are declarative (savepoint/last-state). This is a genuinely better fit than KafkaConnector-per-table because one CR covers a whole database. VERIFIED (operator features) / INFERENCE (fit).
- New operational duties: JobManager HA (2 replicas + k8s HA ConfigMaps), checkpoint storage hygiene (S3 bucket lifecycle for old checkpoints), savepoint-based upgrades of the *image* on every Flink/Flink CDC/connector bump, watching checkpoint duration/backpressure, TaskManager sizing for Paimon write buffers + lookup cache disk, and replication-slot bloat when a Flink job is down (same as today with Debezium). INFERENCE, standard Flink ops.
- Query side: Trino needs a **Paimon connector plugin** (out-of-tree build, see H) — plus per-Trino-version rebuilds; or read Paimon through the Iceberg connector via compatibility metadata (see I). VERIFIED.

---

## H. Maturity / adoption / maintenance status (dates and numbers)

| Component | Status (verified 2026-09-10) |
|---|---|
| Flink CDC | 3.6.0 released 2026-03-31 (announcement 2026-03-30); cadence ~2 releases/yr (3.3 Jan-2025, 3.4 May-2025, 3.5 Sep-2025). GitHub 6,472 stars, 111 open issues (JIRA is primary tracker), pushed 2026-09-09. Supports Flink 1.20.x and 2.2.x, JDK 11. Embedded Debezium **1.9.8.Final** (2022-era). |
| Flink CDC pipeline sources | MySQL since 3.0 (2023-12); PostgreSQL since 3.5 (2025-09), schema evolution since 3.6 (2026-03, off by default); Oracle 3.6; SQL Server master-only (2026-06); MongoDB none. |
| Flink CDC Iceberg sink | since 3.4 (2025-05); pinned Iceberg 1.10.1, "supports Iceberg 1.6–1.10"; hard-coded upsert/equality deletes, 256 MiB, parquet; catalogs documented: hadoop/hive/glue (+ `catalog-impl`). |
| Flink CDC Paimon sink | since 3.1 (2024-05); overview lists Paimon "0.6 … 1.3" — Paimon 2.0 not yet listed; metastore `filesystem` or `hive` only. |
| Apache Paimon | 2.0.0 released 2026-08-07 (1.x line through 1.4.2); 3,395 stars, 765 open issues, pushed daily. Top-level ASF project (graduated 2024). Ecosystem heavily Alibaba/Flink-centric (DLF token provider, MaxCompute). |
| paimon-trino | separate repo, 46 stars, no GitHub releases; last commit 2026-03-17 "Bump Trino to 476 and Paimon to 1.3.1"; docs still say "Paimon currently supports Trino 440"; current Trino is 483 (no Paimon connector in the distribution). trinodb/trino PR #30913 "Add Paimon 2.0.0 connector" (129 files, +68,660 lines) opened 2026-08-26 by an external contributor; only the CLA bot has replied; issue #24636 (2025-01-07) has 0 comments. |
| Flink Kubernetes Operator | 1.15.0 released 2026-05-26 (Flink 2.2 support, k8s Conditions); 1.16.0-rc3 tagged. 1,034 stars. Mature. |
| Apache Iceberg | 1.11.0 (2026-05-20), 1.10.2 (2026-05-18); Flink `IcebergSink` (SinkV2) "currently an experimental feature"; Dynamic Iceberg Sink experimental; equality deletes voted deprecated for V4 (2026-08-18). |
| Paimon ↔ Iceberg compat | `metadata.iceberg.storage = table-location | hadoop-catalog | hive-catalog | rest-catalog`; PK tables readable by Iceberg engines only after **full compaction** ("recommend full compaction to be performed once or twice per hour") unless DV mode (`deletion-vectors.bitmap64=true`, `metadata.iceberg.format-version=3`, Iceberg ≥ 1.8, JDK 11). Documented Iceberg readers: Trino Iceberg (Hive catalog), DuckDB, Athena. REST path warns it may "**drop the table and recreate the table**" if the existing Iceberg table is incompatible. |

Community signal: adoption is real inside the Alibaba/Flink ecosystem and Chinese internet companies (secondary, INFERENCE); outside it, the Trino/dbt/Superset side is thin. A May-2026 practitioner post concedes: "Trino has a community-maintained Paimon connector, but it lacks some of the more advanced Paimon table features. Engines like Dremio, DuckDB, and Snowflake don't have native Paimon integration." (secondary, quoted).

---

## I. Implications for the platform: what could be deleted/replaced/reconfigured; what is lost; risks

### I.1 Option P — Flink CDC YAML → Paimon (full replacement of ingest + Silver)
Could be **deleted**: Strimzi Kafka Connect, Debezium 2.7.3 connectors, per-table Kafka topics + DLQ topics, Apicurio schema registry, Iceberg Kafka Connect sink (one per table), the Spark `MERGE INTO` ScheduledSparkApplication, the hourly per-table maintenance job (replaced by Paimon table properties + at most one dedicated compaction job), the watermark bookkeeping, the Bronze/Silver split (Paimon PK table *is* Silver; `changelog-producer=input` yields a per-snapshot changelog if Bronze-like history is still wanted).
Must be **added**: Flink Kubernetes Operator; one FlinkDeployment (JM ×2 + TMs) per source database; S3 checkpoint bucket; custom Flink CDC image pipeline; Paimon catalog with locking (Hive/JDBC metastore with `lock.enabled`, or Paimon REST server) — Nessie cannot be that catalog; Paimon Trino plugin (out-of-tree, must track Trino version) **or** Iceberg-compat metadata + Nessie via `rest-catalog` (two catalogs to keep consistent); Console re-targeting; a Paimon/Flink skill set.
**Lost**: MongoDB as a first-class source (falls back to Paimon action with all-STRING top-level fields), SQL Server until Flink CDC 3.7 ships, Debezium 2.7.x behaviour (PG 15+ features, Mongo 7 pre-images via Debezium, well-known SMT ecosystem), Kafka as replay buffer / fan-out to other consumers, exactly-once, PK-less tables (rejected), Nessie branching for Silver tables, plain-Iceberg portability of Silver (Paimon is the primary format; Iceberg view is derived and lags by a compaction unless DV+v3), dbt models on Trino unaffected *if* the Trino read path works.
**Risks**: paimon-trino maintenance (single-digit contributors, version lag), Paimon 2.0 vs Flink CDC 3.6 compatibility not yet declared, S3 snapshot-loss without a lock, MOW synchronous compaction causing checkpoint timeouts on small clusters, dynamic-bucket single-writer constraint blocking ad-hoc Spark fixes, Debezium 1.9.8 regressions on newer PostgreSQL.

### I.2 Option I — Flink CDC YAML → Iceberg (keep Iceberg + Nessie + Trino)
Deletes the same Kafka/Debezium/Connect/Spark-MERGE layers, keeps the query stack untouched. But: Nessie/REST catalog support is undocumented (code path via `CatalogUtil.buildIcebergCatalog` resolves `type: rest`/`nessie`, so `catalog.properties.type: rest` + `uri` *should* work — INFERENCE, untested); writes are **equality deletes** (hard-coded), so read amplification lands on Trino until compaction; the built-in compaction is a per-table local-mini-cluster `rewriteDataFiles` with no snapshot expiry/orphan/DV conversion → the Spark maintenance job stays; correctness issue #15305 open; and **Iceberg V4 forbids new equality deletes** — this sink is on a deprecated write path. Not recommended as a target for a new platform.

### I.3 Option K — keep Debezium + Kafka, replace only the lake writer with Flink (Paimon `kafka_sync_database` with `debezium-json`/`debezium-avro`, or Flink CDC Kafka→Paimon)
Keeps MongoDB/SQL Server coverage and Debezium 2.7.3, deletes the Iceberg Connect sink + Spark MERGE, adds Flink + Paimon. Paimon's Kafka action supports "Canal Json, Debezium Json, Debezium Avro, … debezium-bson" formats. VERIFIED. Same Trino/catalog costs as I.1; more moving parts than either pure option.

### I.4 Option S — stay, fix write amplification inside Iceberg + Spark (cheapest)
The platform's actual pain is Spark **copy-on-write** MERGE every 15 min. Iceberg already offers merge-on-read MERGE (`write.merge.mode=merge-on-read`, position deletes; deletion vectors on v3 tables with Iceberg ≥ 1.8/Spark 3.5) plus periodic `rewrite_position_delete_files`/`rewrite_data_files` — configuration on the existing stack. INFERENCE (well-documented Iceberg features, not re-verified here). This removes most of the write amplification without changing a single component, and keeps Nessie/Trino/dbt/Debezium exactly as they are. At "GBs per table" this is very likely sufficient.

---

## J. Evidence (VERIFIED vs INFERENCE)

VERIFIED (fetched 2026-09-10):
- Flink CDC connector matrix + version table: `https://nightlies.apache.org/flink/flink-cdc-docs-stable/docs/connectors/pipeline-connectors/overview/` and raw `docs/content/docs/connectors/pipeline-connectors/overview.md` (master) — "3.6.x | 1.20.*, 2.2.* | MySQL, PostgreSQL, Oracle | StarRocks, Doris, Paimon, Kafka, Elasticsearch, OceanBase, MaxCompute, Iceberg, Fluss, Hudi"; Paimon "0.6, 0.7, 0.8, 0.9, 1.0, 1.1, 1.2, 1.3"; Iceberg "1.6, 1.7, 1.8, 1.9, 1.10".
- Source tree `flink-cdc-connect/flink-cdc-pipeline-connectors/` (GitHub API): modules incl. `flink-cdc-pipeline-connector-sqlserver`, no mongodb module; `flink-cdc-source-connectors/flink-connector-mongodb-cdc` legacy only.
- SQL Server pipeline commits: "2026-06-16 [FLINK-39252][pipeline-connector/sqlserver] Introduce SQLServer Pipeline Source connector".
- PostgreSQL schema evolution: release-3.6 `postgres.md` line 31 ("does not support synchronizing table structure changes currently"); master `postgres.md` `schema-change.enabled` option text; commits FLINK-38959 (2026-03-21), hotfix 2026-03-25, FLINK-40512 (2026-09-02); announcement `https://flink.apache.org/2026/03/30/apache-flink-cdc-3.6.0-release-announcement/`.
- Iceberg/Paimon sink limits: `iceberg.md` lines 258–260, `paimon.md` lines 147–149 ("Not support exactly-once. The connector uses at-least-once + primary key table for idempotent writing.").
- Iceberg sink code: `.../iceberg/sink/v2/IcebergWriter.java` (DEFAULT_FILE_FORMAT="parquet", DEFAULT_MAX_FILE_SIZE=256 MiB, `RowDataTaskWriterFactory(..., identifierFieldIds, true)`), `.../v2/compaction/CompactionOperator.java` (`Actions.forTable(StreamExecutionEnvironment.createLocalEnvironment(), …).rewriteDataFiles().execute()`), `IcebergDataSinkOptions.java` (`sink.compaction.enabled` — "If there are too many tables after enabling it, data flow may be blocked."), `IcebergMetadataApplier.java` (`CatalogUtil.buildIcebergCatalog`).
- Iceberg `core/src/main/java/org/apache/iceberg/CatalogUtil.java`: type constants `hadoop|hive|rest|glue|nessie|jdbc|bigquery`.
- Paimon sink code: `PaimonDataSinkOptions.java` ("Metastore of paimon catalog, supports filesystem and hive."), `bucket/BucketAssignOperator.java` (HASH_DYNAMIC, HASH_FIXED, BUCKET_UNAWARE, POSTPONE_MODE supported; KEY_DYNAMIC → "Unsupported bucket mode").
- Debezium version: `pom.xml` master and release-3.6 `<debezium.version>1.9.8.Final</debezium.version>`; ASF list API `dev@flink.apache.org` thread "[DISCUSS] Flink CDC Upgrade Debezium version to 2.x" (gongzhongqiang 2024-05-23; Leonard Xu reply "flink's default JDK version is still JDK1.8, it's a hard decision"); FLINK-36605 created 2024-10-27.
- Flink CDC k8s: raw `docs/content/docs/deployment/kubernetes.md` (custom image, `--use-mini-cluster`, `classloader.resolve-order: parent-first`, "native application mode is not supported for now").
- Schema evolution: `https://nightlies.apache.org/flink/flink-cdc-docs-stable/docs/core-concept/schema-evolution/`; data-pipeline options: raw `core-concept/data-pipeline.md`.
- MongoDB legacy connector: raw `connectors/flink-sources/mongodb-cdc.md` (lines 143–145, 504–510, 640–648).
- Paimon docs (raw `docs/docs/...` on master + site): `primary-key-table/index.md`, `data-distribution.md`, `compaction.md`, `table-mode.md`, `changelog-producer.md`, `sequence-rowkind.mdx`, `merge-engine/index.md`, `maintenance/write-performance.md`, `maintenance/dedicated-compaction.mdx`, `concepts/catalog.md`, `concepts/concurrency-control.md`, `concepts/rest/index.md`, `iceberg/index.md`, `iceberg/primary-key-table.mdx`, `iceberg/rest-catalog.mdx`, `iceberg/ecosystem.mdx` (site), `ecosystem/trino.md`, `cdc-ingestion/index.mdx`, `cdc-ingestion/mongo-cdc.md`, `cdc-ingestion/kafka-cdc.mdx`, `cdc-ingestion/postgres-cdc.md`.
- Paimon releases: GitHub API `release-2.0.0 2026-08-07`; announcement mail (Aug 6, 2026) via search. Repo stats via GitHub API.
- paimon-trino: GitHub API commits ("2026-03-17 Bump Trino to 476 and Paimon to 1.3.1 (#122)"), `pom.xml` (`trino.version 476`, `paimon.version 1.3.1`), README (stub), no releases; trinodb/trino PR #30913 (open, 2026-08-26, 129 files, +68,660) and issue #24636 (0 comments) via GitHub API; Trino connector list `https://trino.io/docs/current/connector.html` (no Paimon).
- Iceberg Flink: raw `docs/docs/flink-writes.md` (UPSERT constraints, distribution-mode limits, snapshot-expiry warning, SinkV2 experimental, post-commit maintenance, dynamic sink warning), `flink-maintenance.md`; issues `apache/iceberg#15305` (open), `#10431` (closed via #10526); releases via GitHub API.
- Iceberg V4 equality-delete vote: `lists.apache.org` API, dev@iceberg.apache.org, huaxin gao 2026-08-18 06:37 UTC: "The vote passes with 7 binding +1's and 17 non-binding +1's, and no 0 or -1's."; 2026-08-21 "I will submit a PR soon for the spec change."
- Flink Kubernetes Operator: releases/tags via GitHub API; `https://flink.apache.org/2026/05/26/apache-flink-kubernetes-operator-1.15.0-release-announcement/`; concepts overview page.
- Secondary (quoted as such): Jack Vanlightly, "Understanding Apache Paimon's Consistency Model Part 2" (2024-07-03); iceberglakehouse.com "When Paimon Beats Iceberg for Mutable Streams" (2026-05-24); Alibaba Cloud "Best Practices for Flink CDC YAML".

INFERENCE (not verified, reasoned or secondary):
- `catalog.properties.type: rest`/`nessie` working with Flink CDC's Iceberg sink (code path exists; not exercised; extra jars for Nessie).
- Checkpoint/state sizes; recommended checkpoint intervals; typical amplification ratios of the platform's CoW MERGE.
- Paimon 2.0.0 with Flink CDC 3.6 Paimon sink (compat not declared either way).
- Trino Iceberg connector reading Paimon DV+v3 metadata (Trino v3 DV read support not re-verified).
- Iceberg MoR MERGE / v3 DV writes on Spark 3.5.1 + Iceberg 1.9 as the "stay" fix (well-known features, not re-verified in this pass).
- PostgreSQL 15+/SQL Server 2022 compatibility of Debezium-1.9.8-based Flink CDC sources.
- Vote-passing consequence for Flink CDC Iceberg sink (needs redesign for V4).

---

## K. Verdict

For *this* platform — one university's operational databases at GB scale, a small team, on-prem Kubernetes, PostgreSQL + SQL Server + MySQL + **MongoDB**, an Iceberg/Nessie/Trino/dbt/Superset query stack already built — Flink CDC + Paimon is **not a credible drop-in replacement** for Kafka Connect + Spark MERGE today, and Flink CDC → Iceberg is a **worse** option than what exists. The technical thesis is sound and verifiable: Paimon's LSM with deletion vectors genuinely converts "rewrite 256 MiB files every 15 minutes" into "append changed rows + bitmaps, compact in the background", and one YAML per database with automatic table creation and schema evolution is a real simplification over one KafkaConnector CR per table. But the concrete state of the ecosystem kills it: two of the four required sources are not pipeline-grade (MongoDB never, SQL Server unreleased), the embedded Debezium is 1.9.8 from 2022 versus the platform's 2.7.3, the Paimon Trino connector is a 46-star out-of-tree plugin pinned to Trino 476 with an unreviewed 68k-line upstream PR, Paimon on S3 needs a lock-bearing catalog that Nessie cannot provide, exactly-once and PK-less tables are gone, the Iceberg view of Paimon lags a full compaction unless you commit to v3 deletion vectors, and the Flink CDC Iceberg sink hard-codes equality deletes that the Iceberg community voted on 2026-08-18 to forbid in V4. Running Flink (operator, HA JobManagers, checkpoint storage, savepoint upgrades, custom images, backpressure tuning) is an entire second streaming platform for a team that already runs Kafka, Kafka Connect, Spark, Trino and Nessie. The honest recommendation: keep the current pipeline, attack the real problem — copy-on-write MERGE — with Iceberg merge-on-read / deletion vectors and compaction tuning on the existing Spark stack (Option S), and re-evaluate Paimon in 12–18 months against three concrete gates: Flink CDC ships MongoDB and SQL Server pipeline sources on Debezium 2.x/3.x, a Paimon connector is merged into Trino proper, and Paimon's Iceberg REST publication is documented against Nessie or Polaris.
