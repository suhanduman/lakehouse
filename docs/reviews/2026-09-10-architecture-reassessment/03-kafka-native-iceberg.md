# 03 — Kafka-native Iceberg paths: Apache Iceberg Kafka Connect sink & "Iceberg topics"

Review date: 2026-09-10. Independent fresh-eyes review; no local repo files read. Sources are primary (apache/iceberg source at `main`, GitHub issues/PRs via authenticated `gh`, vendor docs). Items I could not confirm against a primary source are tagged **INFERENCE**.

Headline answer to the assignment's core question: **the Apache Iceberg Kafka Connect sink is append-only on every released version (1.9.0 … 1.11.0) and on `main` as of 2026-09-10.** Tabular's `iceberg.tables.upsert-mode-enabled` / `iceberg.tables.cdc-field` were deliberately *not* carried into Apache; the delta writers were removed at donation time and every attempt to add them back (issue #10842, PRs #12070, #14797, #18003) is open/blocked because a PMC-level decision rejects equality-delete-based upserts in the sink. No Kafka-native path with upsert semantics exists that is both open source and runs on-prem against Nessie **except AutoMQ Table Topics** (Apache-2.0, ports Tabular's delta writers) — which would replace the Kafka broker itself, not the connector. Spark MERGE stays.

---

## A. Architecture (components, hops, where state lives)

### A.1 Apache Iceberg Kafka Connect sink (what the platform runs today)
- **Components**: Kafka Connect tasks (workers) + one elected **coordinator** task per connector + a Kafka **control topic** (`iceberg.control.topic`, default `control-iceberg`) + the Iceberg catalog (Nessie REST). VERIFIED (`IcebergSinkConfig.java`, `channel/Coordinator.java`, `channel/Worker.java`, `channel/Channel.java`).
- **Hops**: Debezium → topic → sink task buffers rows into open Parquet files per table/partition → on `StartCommit` from coordinator, each worker completes files and sends `DataWritten`/`DataComplete` events via the control topic (transactional producer) → coordinator aggregates and performs **one Iceberg commit per table per commit cycle** (`table.newAppend()`; a `newRowDelta()` branch exists but is unreachable — see B). VERIFIED (`Coordinator.java` lines 122–215, 290–334).
- **Where state lives**: (i) consumer-group offsets in Kafka (committed by the coordinator via `sendOffsetsToTransaction`), (ii) the **authoritative offsets in the Iceberg snapshot summary** as property `kafka.connect.offsets.<control-topic>.<group-id>` (JSON map partition→offset) plus `kafka.connect.valid-through-ts`; on commit the coordinator validates the table's last committed offsets against expected ones (`offsetValidator`) and skips already-committed data files. VERIFIED (`Coordinator.java` 76–105, 262–290, 357+).
- **Everything downstream of Bronze** (Silver MERGE, watermark, compaction) is outside the sink; the sink knows nothing about PKs beyond writer configuration.

### A.2 "Iceberg topics" family (broker writes the table; no connector)
Two sub-architectures:
1. **Materialization** (separate copy, broker-side): Confluent Tableflow, WarpStream Tableflow, AutoMQ Table Topic, Redpanda Iceberg Topics. Broker (or a broker-side worker) consumes its own log and writes Parquet + Iceberg commits. State = broker-internal translation offsets + Iceberg snapshots (Redpanda additionally *tags* its snapshots to protect them from expiry — VERIFIED `iceberg_disable_snapshot_tagging` doc text).
2. **Zero-copy / shared tiering**: Bufstream Iceberg Archives; Aiven "Iceberg Topics" RemoteStorageManager plugin for Apache Kafka tiered storage. The Parquet files *are* the tiered log segments; Kafka reads reconstruct batches from them. VERIFIED (Aiven whitepaper; Bufstream "zero copy… no separate copy" statement via search snippet — INFERENCE on current Bufstream doc wording because buf.build docs now redirect to the CoreWeave acquisition post).

### A.3 Apache Kafka proper
No KIP for native Iceberg materialization exists. KIP-1150 (Diskless Topics) mentions an "Iceberg format" as a possible future pluggable log-format extension only; Aiven explicitly chose "not a KIP… only available as a plugin" for its tiered-storage code. VERIFIED (search of cwiki + Aiven blog wording).

### A.4 Strimzi
Nothing Iceberg-specific. Strimzi offers `KafkaConnect` with `build:` (pulls the connector artifact) and the `KafkaConnector` CR — i.e., exactly what the platform already uses. A `site:strimzi.io Iceberg` search returns no hits. VERIFIED (absence). AutoMQ v1.6.0 advertises "Strimzi support" (running AutoMQ brokers under the Strimzi operator) — only relevant if the broker is swapped (see I).

---

## B. Upsert/delete strategy on the lake and write-amplification behavior

### B.1 Apache sink = append-only (definitive)
Code-level proof on `main` (2026-09-10):
- `IcebergSinkConfig.newConfigDef()` has **no** `iceberg.tables.cdc-field` and **no** `iceberg.tables.upsert-mode-enabled`. It has `iceberg.tables.default-id-columns` / `iceberg.table.<t>.id-columns` (documented as "columns that identify a row (primary key)"). VERIFIED.
- `RecordUtils.createTableWriter()` builds a `GenericFileWriterFactory` **with** `equalityFieldIds` when identifier fields exist, but then wraps it in `UnpartitionedWriter` or the sink's own `PartitionedAppendWriter extends PartitionedFanoutWriter`. Both only ever call `RollingFileWriter` (data writer). `BaseTaskWriter.completedDeleteFiles` is populated only by `BaseEqualityDeltaWriter`, which the sink never instantiates. Result: `WriteResult.deleteFiles()` is always empty; the coordinator's `newRowDelta()` branch is dead code. VERIFIED (`RecordUtils.java` 114–176; core `UnpartitionedWriter.java`, `BaseTaskWriter.java`).
- The `kafka-connect/.../data/` package on `main` contains no `BaseDeltaTaskWriter`, `UnpartitionedDeltaWriter`, `PartitionedDeltaWriter`, `Operation`, `RecordWrapper` (all present in Tabular's repo). VERIFIED (directory listings of both repos).
- `IcebergWriter.write()` **ignores tombstones** ("// ignore tombstones... if (record.value() != null)"). Debezium delete envelopes (`op=d`) that survive the SMT are written as ordinary rows. VERIFIED.
- Consequence for users who copy Tabular configs: issue #15046 (2026-01, open) "lots of duplicates even if enabled upsert-mode"; issue #15351 (2026-02, closed without fix) "IcebergSinkConnector is treating updates and deletes as inserts… no setting to configure this". VERIFIED.
- The `id-columns` config therefore has **no runtime effect** in append mode beyond validating column names (`throw new IllegalArgumentException("ID column not found")`). Also note auto-create does *not* set identifier-field-ids (PR #15615, open since 2026-03, stale-pinged repeatedly). VERIFIED.

### B.2 Why it was removed and where the community is going
- Issue #10842 (opened 2024-08-01 by the sink's author bryanck): "The initial Kafka Connect sink submission did not include delta writer support that the Tabular version has, as there are performance concerns over relying on equality deletes." Auto-closed stale 2025-03, reopened; last comment 2026-09-09 ("migrating from Flink to KC and really looking for this feature"). VERIFIED.
- PR #12070 (ismailsimsek, 2025-01, +896/−42) and PR #14797 (t3hw, 2025-12, +2492/−24, "Delta Writer Support in DV Mode for in-batch deduplication") — both **open, changes requested**. bryanck's review 2025-12-08: "When the sink was contributed to this project, the community decided it was best to remove that functionality, as using it can result in severely degraded performance… there are alternative solutions that don't have the same issues." 2026-01-09: "This is something the Iceberg community has decided, including PMC members, so we'd need to get the community on board in order to proceed." VERIFIED. PR #14797's own description concedes: "DVs only help for in-batch deduplication. Out of batch deletes/updates fall back to equality deletes… periodically compacting the table when using CDC mode is mandatory."
- Newest attempt PR #18003 (2026-09-07, +1888, part 1 of 4): convert equality deletes to **deletion vectors inside the coordinator before commit** ("adds CDC writes to the Kafka Connect sink without exposing equality deletes on the table"), mirroring Flink's `ConvertEqualityDeletes` maintenance task (merged for Flink 2026-07-10, PRs #17142/#17156/#17113). Zero reviews as of 2026-09-08. VERIFIED.
- Spec direction: dev-list vote to **deprecate equality deletes in Iceberg V4 (forbid new writes)** reportedly passed with 7 binding +1 (secondary sources: dev.to/substack digests and the mail-archive subject "[VOTE] Deprecate equality deletes in Iceberg V4 (forbid new writes)"). Primary message fetch failed → **INFERENCE on counts/date; VERIFIED that the thread exists and the direction is deprecation.**

### B.3 Write amplification if a sink-side equality-delete upsert *did* exist (Tabular / AutoMQ semantics)
- Tabular README: "Enabling `iceberg.tables.upsert-mode-enabled` will cause all appends to be preceded by an equality delete." Every batch writes 1 data file + 1 equality-delete file per table-partition per task; **no read of existing data at write time** (that's the appeal: O(new rows) write cost). VERIFIED.
- Read side pays instead: every scan must load *all* equality-delete files whose sequence number > data file's and apply them as predicates. Trino #17114 (2023): 4.4M-row table, frequent checkpoints → 302 s query, 52 s after Trino consolidated delete sets (PR #17115) — still ~17× slower than append tables at that size. Iceberg #9363: Flink upsert mode "1 minute to query 5 million rows" vs "80 million rows in 3 seconds" with upsert off. VERIFIED (issue texts).
- Maintenance cost moves to compaction: `rewrite_data_files` must fully rewrite affected data files to drop equality deletes (there is no `rewrite_equality_deletes`); Trino #25584 (2025-04): OPTIMIZE on a CDC-fed table fails with "Cannot commit, found new delete for replaced data file" whenever compaction outlasts the sink's commit interval — later mitigated in Trino PR #25603. VERIFIED.
- Net: equality-delete upserts trade the platform's *bounded, batched* CoW amplification (Spark MERGE every 15 min rewrites only touched files of a bucket(16) table) for *unbounded read-side* amplification plus a mandatory, conflict-prone compaction loop. For GB-scale tables the CoW MERGE is the cheaper, more predictable choice — INFERENCE (reasoning), consistent with the maintainers' stance quoted above.

### B.4 Vendor materialization upsert
- **Confluent Tableflow** (Confluent Cloud only): UPSERT write mode — "the Kafka message key and partition number form the composite primary key"; deletes require tombstones; "maintains an additional index table"; limits "30 billion unique keys", key schema cannot evolve. Debezium requires `after.state.only=true` or Confluent Flink decoding. Internal file strategy not documented → INFERENCE that it is not plain equality deletes (index table suggests key→position resolution). VERIFIED for quoted statements.
- **AutoMQ Table Topic** (Apache-2.0 repo): `automq.table.topic.upsert.enable`, `automq.table.topic.cdc.field` (I/U/D), `automq.table.topic.id.columns`, `flatten_debezium` transform; "Deleted records are marked via a deletefile… equality delete". Source: `core/src/main/java/kafka/automq/table/worker/{BaseDeltaTaskWriter,PartitionedDeltaWriter,UnpartitionedDeltaWriter,Operation,RecordWrapper}.java` — the Tabular delta-writer design, ported. VERIFIED.
- **Redpanda, WarpStream, Bufstream, Aiven RSM**: append-only. WarpStream docs: "Tableflow currently supports append-only tables… rows will not be deduplicated." VERIFIED.

---

## C. Commit cadence, small-file strategy, who owns compaction

| Path | Commit cadence | Small-file behavior | Compaction owner |
|---|---|---|---|
| Apache KC sink | `iceberg.control.commit.interval-ms` default **300 000 ms (5 min)**; platform uses 60 s (5× more snapshots/files than default). Commit timeout 30 s → "partial commit" of whatever workers responded, retried. VERIFIED | Files per commit ≈ tasks × partitions-touched per table; 60 s × 1 task ≈ 1 440 files/day/table minimum, more with day(__ts_ms) boundaries. No file-size coalescing across commits; `write.target-file-size-bytes` is only an upper bound. **User** must run `rewrite_data_files`/`expire_snapshots`. INFERENCE on counts; VERIFIED on mechanism | User (the platform's hourly Spark job) |
| Redpanda Iceberg Topics | `iceberg_catalog_commit_interval_ms` default **1 min** (coordinator-wide); `iceberg_target_lag_ms` default 1 min with backpressure to producers. VERIFIED | Redpanda controls flush; does **automatic snapshot expiry** (`iceberg_disable_automatic_snapshot_expiry` default false) and tags snapshots; data-file compaction is **not** done by Redpanda ("you can configure Iceberg to run periodic compaction" — Redpanda blog wording, INFERENCE on exact doc text) | User for compaction |
| Confluent Tableflow | ~5 min freshness. VERIFIED | "automates table maintenance by compacting and cleaning up small files". VERIFIED | Confluent |
| WarpStream Tableflow | not stated | "Compaction and table maintenance is included out-of-the-box". VERIFIED | WarpStream |
| AutoMQ Table Topic | `automq.table.topic.commit.interval.ms` default **60 000 ms**. VERIFIED | Relies on catalog-side maintenance (S3 Tables) or user. INFERENCE | User |
| Aiven RSM / Bufstream zero-copy | On segment roll | File layout dictated by Kafka segments; plugin "not responsible for… compaction and snapshot expirations" (Aiven). Bufstream files are read-only for Iceberg tooling (rewrite would break Kafka reads). VERIFIED (Aiven), INFERENCE (Bufstream) | User / nobody |

Community guidance for the KC sink on small files is simply: longer commit interval + external compaction. There is no in-sink coalescing. The platform's 60 s is tighter than the default with no evident latency requirement (Silver is 15 min anyway) — a 5-min interval would cut Bronze file count ~5× for free. INFERENCE (recommendation).

---

## D. Schema evolution and semi-structured (MongoDB) handling

- **Apache sink**: `iceberg.tables.evolve-schema-enabled` adds missing columns and widens types on the fly: on a schema diff the writer flushes the current file, calls `SchemaUtils.applySchemaUpdates(table, updates)`, re-instantiates the writer, re-converts the row. Additive only (no drops/renames). Since 2026-06 also evolves when the new field's value is null (#16826). VERIFIED (`IcebergWriter.convertToRow`, commit log).
- **Type mapping** (`SchemaUtils.toIcebergType`): Connect STRUCT→struct, ARRAY→list, MAP→map, Decimal→`decimal(38, scale)` (precision forced to 38), Date/Time/Timestamp logical types→date/time/timestamptz; `timestamp_ns` support still an open PR (#17613). VARIANT supported in RecordConverter since 2026-04 (#15283) and "Variant shredding for Parquet writes" is in the 1.11 feature list. VERIFIED.
- **Schemaless JSON**: `JsonToMapTransform` SMT (requires `StringConverter`) infers primitives/arrays; nested objects become `Map<String,String>`; mixed-type arrays become `array<string>`; `json.root=true` collapses everything to one `payload map<string,string>`. VERIFIED (docs).
- **MongoDB**: `MongoDebeziumTransform` SMT exists in Apache transforms: "Debezium Mongo Connector generates the CDC before/after fields as BSON strings. This SMT converts those strings into typed SinkRecord Structs by inferring the schema from the BSON node types", with `array_handling_mode` (array vs document). Schema is inferred **per record**, so heterogeneous documents will trigger frequent schema evolution or conversion failures; with `evolve-schema-enabled` this is workable for additive drift only. VERIFIED (source), INFERENCE (operational consequence).
- **Debezium envelopes**: `DebeziumTransform` unwraps `after` (or `before` for `op=d`), maps ops to I/U/D in `_cdc.op`, adds `_cdc.ts/offset/source/target/key`; `cdc.target.pattern` default `{db}.{table}` — designed for the (absent) CDC feature. It is *not* required; Debezium's own `ExtractNewRecordState` plus `KafkaMetadataTransform` (topic/partition/offset/timestamp columns) is the more common stack — the platform's `__op/__ts_ms/__deleted/__kafka_offset/__kafka_partition` columns look like that stack. INFERENCE on platform's exact SMT chain.
- **Vendors**: Redpanda evolves the table when Schema Registry schema changes ("new fields require default values"); Aiven RSM: "Schema evolution is not supported at the moment"; Confluent: "does not support schemaless topics", no key-schema evolution in upsert mode; AutoMQ `IcebergTableManager.applySchemaChange` handles column adds + identifier-field changes. VERIFIED.

---

## E. Snapshot/backfill, incremental, multi-table per source

- **Apache sink** has no notion of snapshot vs streaming; Debezium `op=r` rows are treated like inserts (`mapOperation`: "c", "r" and any others → I). Backfill = re-consume from earlier offsets; because offsets are recorded in the snapshot summary and validated at commit, replaying an *older* offset range against the *same* table is refused/skipped by `offsetValidator` (it protects exactly-once, and thereby also makes intentional re-ingest into the same table awkward — you reset the consumer group and the property must be reconciled). VERIFIED (mechanism); INFERENCE (operational awkwardness).
- **Multi-table fan-out**: `iceberg.tables.dynamic-enabled` + `iceberg.tables.route-field`, or static `iceberg.tables` + per-table `route-regex`; one connector can serve many topics/tables, with one coordinator commit cycle across all tables (`Tasks.foreach(commitMap).stopOnFailure()` — a failure on one table aborts the cycle for all). The platform's *one sink per table* choice avoids the cross-table blast radius at the cost of one coordinator + control-topic consumer group per table. VERIFIED (code), INFERENCE (trade-off).
- **Vendors**: all are per-topic → per-table; none do snapshot-vs-CDC differentiation; Confluent handles Debezium only via `after.state.only` or a Flink decoding hop. VERIFIED.

---

## F. Ordering / exactly-once / dedup guarantees

- **Apache sink**: "Exactly-once delivery semantics… relies on KIP-447… requires Kafka 2.5 or later". Mechanism: transactional control-topic producer per task, offsets committed inside the transaction, snapshot-summary offsets validated on commit, table UUID validated (#14979, 2026-01). VERIFIED. **Caveats from the tracker**: coordinator zombie fencing is *not yet* stable — PR #17376 "Harden commit coordinator (fencing, recovery, dedup, retryable commits)" (2026-07, open, stale-warned), PR #18039 "use a stable coordinator transactional id to fence stale coordinators" (2026-09-10, open), PR #17925 "Ignore replayed control-topic records", PR #18006 "Read the control topic from earliest", PR #18012 "Fail incomplete commits for missing or replaced tables" — all open in Sep 2026. Issue #13593 (1.9.1 → reports still on 1.11.0-SNAPSHOT) commits stalling "Commit failed, will try again next cycle". Exactly-once is real in design but the coordinator is under active correctness repair. VERIFIED.
- **Ordering**: per Kafka partition; within a Bronze table, `__kafka_offset` gives a total order per partition; for a PK, Debezium keys by PK so all changes of a row land in one partition → per-PK order preserved. Dedup is not done by the sink (append-only) — it is the Silver MERGE's job (and the platform must dedupe within a 15-min batch by max(__ts_ms/__lsn, offset) before MERGE, since Iceberg MERGE rejects multiple source matches). INFERENCE on platform behavior.
- **Redpanda**: snapshot tags "help Redpanda ensure exactly-once delivery of records". **Aiven RSM**: "There are circumstances where the same offset may be uploaded multiple times… Kafka transactions are also not supported for now." **Confluent upsert**: duplicates possible if partition count changes. VERIFIED.

---

## G. Operational surface: config vs code; UI; GitOps fit

- **Apache sink**: 100% configuration via `KafkaConnector` CR — fits Strimzi/ArgoCD and the platform's Console renderer perfectly. Error handling: the sink does **not** implement `ErrantRecordReporter`; Kafka Connect's `errors.tolerance`/DLQ only covers converter/SMT stages, so a bad record that fails inside `IcebergWriter.write` throws `DataException` and stalls the task ("Currently Iceberg Kafka Connector stalls when it receives bad records" — PR #14618, open since 2025-11 with 18 review rounds). The platform's "DLQ topics" therefore protect only the deserialization stage. VERIFIED (PR text), INFERENCE (platform impact).
- **Redpanda**: cluster + topic properties, enterprise license key; Nessie is "tested but not regularly verified". **AutoMQ**: topic configs; requires replacing Kafka brokers with AutoMQ (Kafka fork on S3), i.e. Strimzi's Kafka CR is replaced by AutoMQ's chart/Strimzi mode. **Confluent/WarpStream Tableflow**: SaaS control plane, not GitOps-able on-prem. **Aiven RSM**: broker plugin + `remote.storage.*` configs on a vanilla Kafka; would work under Strimzi's tiered-storage config but is Avro-only, no schema evolution, no transactions. VERIFIED.
- No path provides a UI beyond the vendor consoles; the platform's Console stays regardless.

---

## H. Maturity / adoption / maintenance status (with dates and numbers)

- **Apache Iceberg KC sink**: releases 1.9.0 (2025-04-28), 1.9.1 (05-28), 1.9.2 (07-18), 1.10.0 (2025-09-11), 1.10.1 (12-22), 1.10.2 (2026-05-18), 1.11.0 (2026-05-20). 89 commits touching `kafka-connect/` since 1.9.0; substantive ones: GenericFileWriterFactory switch (2025-10), offset/UUID validation (2025-11, 2026-01), SMT-topic offset fix (2026-04), VARIANT (2026-04), surface commit failures (2026-05), partial-commit metric + bounded retry (2026-06/07), decimal inference fix (2026-08). Feature PRs (delta writer, DLQ, identifier-ids on auto-create, coordinator hardening) languish 6–20 months with contributors publicly asking why nothing is reviewed (PR #15615: "I really do not understand why its not getting reviewed"). Review bandwidth is the bottleneck; bryanck is effectively the gatekeeper. VERIFIED.
- **Tabular/Databricks connector**: README banner "THIS REPOSITORY IS NOT MAINTAINED"; last release v0.6.19 (2024-06-06); 284 stars; last push 2025-07-03. Dead. VERIFIED.
- **getindata/kafka-connect-iceberg-sink** (Debezium-oriented, upsert): archived, last release 0.4.0 (2023-05). Dead. VERIFIED.
- **memiiso/debezium-server-iceberg** (not Kafka; Debezium Server → Iceberg directly, upsert via equality deletes on v2 or **DVs on v3** by default, in-batch dedup): 327 stars, 1.0.0.Final 2025-11-27, pushed 2026-09-10, Apache-2.0. Alive, single-maintainer-ish. VERIFIED.
- **Redpanda**: v26.2.2 (2026-08-22), 12.5k stars, source-available; Iceberg Topics = **enterprise license**. VERIFIED.
- **AutoMQ**: 10.7k stars, Apache-2.0, 1.7.5-rc0 (2026-09-02), pushed 2026-09-09; Table Topic incl. upsert/CDC in the OSS tree; catalogs `rest, glue, hive, nessie, tablebucket`. VERIFIED.
- **Confluent Tableflow**: GA on Confluent Cloud; upsert materialization Oct 2025; "not available for Confluent Platform", not on GCP. VERIFIED.
- **WarpStream Tableflow**: BYOC with WarpStream Cloud control plane; append-only; proprietary. VERIFIED.
- **Bufstream**: acquired by CoreWeave 2026-05-08 and "added… to its internal platform as part of the W&B Models and Weave product lines"; buf.build docs redirect to the announcement. Effectively withdrawn as a standalone product. VERIFIED.
- **Aiven Iceberg Topics RSM**: Apache-2.0, repo v1.1.1 (2025-10-07), pushed 2026-08-01; whitepaper lists no schema evolution, no transactions, possible duplicates. Experimental. VERIFIED.
- Independent critique (Jack Vanlightly, 2025-10-15) of zero-copy: "Parquet file writing is far more expensive than log segment uploads"; recommends materialization via Connect/Flink. VERIFIED.

---

## I. Implications for the platform

### I.1 What could be deleted/replaced — honestly, nothing in the Kafka layer
- **Spark MERGE cannot be removed by the Apache sink** on any released or `main` version. Configuring `id-columns` or copying `upsert-mode-enabled` from old Tabular blog posts silently does nothing (issues #15046/#15351). If the Console exposes such knobs, remove them.
- **AutoMQ Table Topic is the only OSS on-prem path with upsert**, and it requires replacing Strimzi-managed Kafka 4.2 with AutoMQ brokers (Kafka fork, S3-backed WAL). That is a broker migration to gain equality-delete upserts the Iceberg PMC is deprecating in V4 — a bad trade for a small team. Do not.
- **Redpanda** would also mean swapping the broker, needs an enterprise license, is append-only, and Nessie is "not regularly verified". No.
- **Confluent/WarpStream Tableflow**: not deployable on-prem. **Bufstream**: gone. **Aiven RSM**: Avro-only, no schema evolution, no transactions; and it is *append-only* anyway. None removes MERGE.

### I.2 What would be lost if a sink-side upsert were adopted anyway (e.g., AutoMQ or a patched fork with PR #14797)
- The **replayable Bronze changelog** (30-day raw CDC with `__op/__lsn/__kafka_offset`) — the only place the platform can re-derive Silver after a bad MERGE, a schema mistake, or a late-arriving fix. A single upsert table has no history except snapshots (which compaction/expiry erase).
- **Watermark-based recovery**: today's Silver watermark table property makes MERGE idempotent and restartable; a sink-side upsert has no equivalent user-visible checkpoint other than Kafka offsets in snapshot metadata.
- **Trino read predictability**: every Silver query would carry unbounded equality-delete predicates until compaction; Trino compaction (`OPTIMIZE`) will conflict with 60 s sink commits (Trino #25584 pattern). Superset dashboards on Silver would regress hardest.
- **MongoDB**: nested-doc PK/identity semantics under equality deletes on inferred schemas is untested territory in every option above.

### I.3 Reconfigurations that *are* worth doing (low risk)
1. Raise `iceberg.control.commit.interval-ms` from 60 000 toward the 300 000 default (Silver runs every 15 min; 5-min Bronze commits lose nothing) → ~5× fewer Bronze files/snapshots and less `rewrite_data_files` work. INFERENCE (recommendation).
2. Treat "DLQ" claims carefully: converter/SMT failures go to DLQ; writer failures stall the task (no `ErrantRecordReporter` until PR #14618 merges). Add alerting on stalled tasks / `partial commit` metric (#16433). VERIFIED mechanism.
3. Track PR #17376/#18039 (coordinator fencing) and upgrade promptly when they land; issue #13593-style stalls are real on 1.9.x–1.11.0.
4. Pre-create Bronze tables (the platform already does via pyiceberg) — good, since auto-create does not set identifier fields (#15615) and the sink's partition-spec fallback silently creates *unpartitioned* tables on spec errors (`IcebergWriterFactory.autoCreateTable` catch → `PartitionSpec.unpartitioned()`). VERIFIED.
5. Watch PR #18003 (equality→DV conversion in the coordinator). If parts 1–4 merge (optimistically 1.12/1.13, 2027) the sink could write CDC as **deletion vectors on v3 tables**, which Trino reads efficiently. That is the only credible future in which MERGE becomes optional; it is not here. INFERENCE (timeline).

### I.4 Risks
- Betting product architecture on upstream review bandwidth that has left a +181-line DLQ PR unmerged for 10 months.
- Any fork carrying Tabular's delta writers inherits the V4 equality-delete deprecation.
- Vendor "Iceberg topics" lock the table format/catalog choice to the broker vendor; two are cloud-only, one is gone, one is enterprise-licensed.

---

## J. Evidence (VERIFIED vs INFERENCE)

Primary sources (fetched 2026-09-10):
- `https://raw.githubusercontent.com/apache/iceberg/main/kafka-connect/kafka-connect/src/main/java/org/apache/iceberg/connect/IcebergSinkConfig.java` — no `upsert`/`cdc-field` keys; `COMMIT_INTERVAL_MS_DEFAULT = 300_000`; `COMMIT_TIMEOUT_MS_DEFAULT = 30_000`; `DEFAULT_CONTROL_TOPIC = "control-iceberg"`. VERIFIED.
- `.../connect/data/RecordUtils.java` lines 114–176: `equalityFieldIds` set on `GenericFileWriterFactory` but writers are `UnpartitionedWriter` / `PartitionedAppendWriter`. VERIFIED.
- `.../connect/data/` listing: no delta writers/Operation; `https://api.github.com/repos/databricks/iceberg-kafka-connect/contents/kafka-connect/src/main/java/io/tabular/iceberg/connect/data` shows `BaseDeltaTaskWriter, PartitionedDeltaWriter, UnpartitionedDeltaWriter, Operation, RecordWrapper`. VERIFIED.
- `core/src/main/java/org/apache/iceberg/io/UnpartitionedWriter.java`, `BaseTaskWriter.java` — delete files only from `BaseEqualityDeltaWriter`. VERIFIED.
- `.../connect/data/IcebergWriter.java` — "// ignore tombstones..."; schema-evolution flush/re-init loop. VERIFIED.
- `.../connect/channel/Coordinator.java` — `kafka.connect.offsets.%s.%s`, `kafka.connect.valid-through-ts`, `newAppend()` vs dead `newRowDelta()` branch, `offsetValidator`, `commitMaxConsecutiveFailures`. VERIFIED.
- `.../connect/channel/Channel.java` — transactional producer, `sendOffsetsToTransaction`. VERIFIED.
- `.../kafka-connect-transforms/.../{DebeziumTransform,CdcConstants,MongoDebeziumTransform,JsonToMapTransform,KafkaMetadataTransform}.java`. VERIFIED.
- Docs `https://raw.githubusercontent.com/apache/iceberg/main/docs/docs/kafka-connect.md` — feature list ("Exactly-once delivery semantics", "Variant shredding"), config table, "relies on KIP-447", SMT sections. VERIFIED.
- Tabular README `https://raw.githubusercontent.com/databricks/iceberg-kafka-connect/main/README.md` — "THIS REPOSITORY IS NOT MAINTAINED…", `iceberg.tables.cdc-field`, `iceberg.tables.upsert-mode-enabled`, "all appends… preceded by an equality delete… require an Iceberg V2 table with identity fields". VERIFIED.
- GitHub (via `gh`): issues apache/iceberg #10842 (OPEN, 2024-08-01→2026-09-09), #15046 (OPEN), #15351 (CLOSED 2026-02-18, no fix), #12914 (stale-closed), #15615 (OPEN), #13593 (OPEN); PRs #12070 (OPEN), #14797 (OPEN, CHANGES_REQUESTED by bryanck 2025-12-08; comment 2026-01-09), #18003 (OPEN 2026-09-07), #17376, #18039, #17925, #18006, #18012, #14618 (all OPEN); Flink `ConvertEqualityDeletes` #17142 merged 2026-07-10. Releases API: tags/dates listed in H. VERIFIED.
- `repos/apache/iceberg/commits?path=kafka-connect&since=2025-04-28` → 89 commits (list in scratch). VERIFIED.
- Repo metadata via `gh repo view`: getindata (archived, 0.4.0 2023-05-25), memiiso (327★, 1.0.0.Final 2025-11-27), databricks/iceberg-kafka-connect (v0.6.19 2024-06-06), Aiven-Open/tiered-storage-for-apache-kafka (Apache-2.0, v1.1.1 2025-10-07), AutoMQ/automq (Apache-2.0, 10 682★, 1.7.5-rc0 2026-09-02), redpanda (v26.2.2 2026-08-22). VERIFIED.
- AutoMQ source `core/src/main/java/kafka/automq/table/worker/IcebergWriter.java` (Apache-2.0 header; `deltaWrite = cdcField || upsertEnable`; `UnpartitionedDeltaWriter/PartitionedDeltaWriter`) and docs `https://docs.automq.com/automq/table-topic/table-topic-configuration` (commit interval 60 000 ms; catalog types `rest, glue, hive, nessie, tablebucket`; "equality delete"). VERIFIED.
- Redpanda docs: about-iceberg-topics ("This feature requires an enterprise license"; DLQ table `<topic>~dlq`), use-iceberg-catalogs ("Apache Polaris, Dremio Nessie… have been tested but are not regularly verified"), cluster-properties (`iceberg_catalog_commit_interval_ms` default 1 minute; `iceberg_target_lag_ms` default 1 minute; `iceberg_disable_automatic_snapshot_expiry`; `iceberg_disable_snapshot_tagging` "…exactly-once delivery"). VERIFIED.
- Confluent docs overview / write-modes / materialize-cdc ("Tableflow is not available for Confluent Platform"; UPSERT composite key = message key + partition; 30 billion keys; tombstones for deletes; `after.state.only`). VERIFIED.
- WarpStream docs `https://docs.warpstream.com/warpstream/tableflow/tableflow` ("Tableflow currently supports append-only tables"; BYOC + WarpStream Cloud control plane; own read-only REST catalog). VERIFIED.
- Bufstream: `https://buf.build/blog/coreweave-acquires-bufstream` (2026-05-08); all `buf.build/docs/bufstream/*` URLs redirect there. VERIFIED. Bufstream technical claims (zero-copy, read-only files) — from search snippets only → INFERENCE.
- Aiven whitepaper `https://github.com/Aiven-Open/tiered-storage-for-apache-kafka/blob/main/iceberg_whitepaper.md` — quotes in A.2/D/F. VERIFIED. Aiven blog "not a Kafka KIP… only available as a plugin". VERIFIED (search snippet of aiven.io blog).
- Trino #17114 (302 s→52 s), Iceberg #9363, Trino #25584/#25603. VERIFIED (issue pages).
- Jack Vanlightly 2025-10-15 post. VERIFIED.
- Equality-delete V4 deprecation vote counts/date — secondary digests only (mail-archive fetch failed). INFERENCE on specifics.
- Anything about the platform's actual SMT chain, Console knobs, or MERGE dedup logic — INFERENCE (no local files read, by brief).

---

## K. Verdict

No. There is no Kafka-native path that removes Spark MERGE for this platform. The Apache Iceberg Kafka Connect sink is append-only by explicit PMC decision — the Tabular `upsert-mode-enabled`/`cdc-field` code was stripped at donation, every re-add PR since 2024 is blocked by the sink's author citing equality-delete read performance, and the Iceberg community has moved to deprecate equality deletes in V4; the only live hope (PR #18003, equality→deletion-vector conversion inside the coordinator) had zero reviews three days after opening, in a module where a 181-line DLQ patch has waited ten months. Among "Iceberg topics", Confluent and WarpStream Tableflow are cloud-only, Bufstream was absorbed by CoreWeave in May 2026, Redpanda is enterprise-licensed and append-only with Nessie "not regularly verified", Aiven's RSM plugin is Avro-only with no schema evolution and no transactions, and Apache Kafka has no KIP. The single OSS on-prem option with upsert — AutoMQ Table Topics — would mean replacing Strimzi Kafka with a Kafka fork to get exactly the equality-delete merge-on-read design that hurts Trino and that Iceberg is retiring. For GB-scale tables, a 15-minute copy-on-write MERGE from a replayable Bronze changelog is the cheaper, safer, more debuggable design; keep it, lengthen the Bronze commit interval toward the 5-minute default, fix the DLQ/stall expectation, and revisit only if PR #18003's four parts merge and the sink can write deletion vectors on v3 tables.
