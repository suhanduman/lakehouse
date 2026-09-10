# 06 — Could Spark be removed? Trino MERGE / dbt-trino as the Silver engine, Trino EXECUTE as maintenance

Fresh-eyes review, 2026-09-10. Sources: Trino 483 docs (current), trinodb/trino GitHub issues/PRs (state checked via GitHub API today), dbt-trino source on `master` (1.10.3, 2026-07-27) and its 1.9 changelog, apache/iceberg `main` kafka-connect docs + source, dbt-core docs. No local repository files were read. Every claim is tagged **[V]** (verified against a primary source) or **[I]** (inference).

---

## A. Trino MERGE INTO on Iceberg for CDC — mechanics, write mode, correctness/perf, verified limitations

**Mechanics**
- Trino supports `MERGE INTO ... USING ... ON ... WHEN MATCHED [AND cond] THEN DELETE | UPDATE SET ... WHEN NOT MATCHED [AND cond] THEN INSERT`. WHEN clauses are evaluated in order, first match wins. **The query fails if one target row matches more than one source row** — so the source must already be deduplicated to one row per key (the ROW_NUMBER step is mandatory, not optional). [V — trino.io/docs/current/sql/merge.html]
- Trino Iceberg row-level DML (DELETE/UPDATE/MERGE) on v2 tables is **merge-on-read only**: it writes **position delete files**, never rewrites data files. Trino **does not honor `write.merge.mode` / `write.delete.mode` / `write.update.mode`** — the `copy-on-write` value is simply not implemented. Issue #17272 "Support copy-on-write mode for Iceberg write" has been open since 2023-04-27; the implementing PR #28958 (opened 2026-04-01, last touched 2026-09-03) is still **open/unmerged** as of today. [V — docs: "Tables using v2 ... support deletion of individual rows by writing position delete files"; #17272 open; PR #28958 open]
- Equality deletes: Trino reads them but never writes them. [V — docs only mention `equality_ids` in `$files`; roadmap #27371 lists "equality delete file conversion" as *planned*]
- Whole-partition (metadata-only) DELETE: the docs say it happens only when the WHERE hits *identity-transformed* partition columns. But PR #12795 (merged 2022-06-20, Trino 387) extended predicate subsumption to non-identity transforms **when the predicate aligns with partition boundaries** (e.g. `__ts_ms < TIMESTAMP '2026-08-11 00:00:00'` on a `day(__ts_ms)` table). So a TTL delete on Bronze *can* be metadata-only in Trino **if and only if the cutoff is aligned to a day boundary**; a mis-aligned cutoff (e.g. `now() - interval '30' day`) will produce position delete files on Bronze. [V for the PR semantics; **I** that Trino's planner treats the specific `day()`-aligned timestamp literal as subsumable — must be tested]
- Format v3 (deletion vectors): experimental in Trino; **row-level DML and OPTIMIZE are unsupported on v3 tables**. Not an option. [V — docs]

**Correctness — GitHub issues (state checked 2026-09-10)**

| Issue | Title | State | Relevance |
|---|---|---|---|
| #26853 | MERGE conflict results in Iceberg table corruption (v472, Glue; failed commit left a snapshot whose metadata files were deleted from S3; all queries failed until manual restore) | **OPEN** since 2025-10-06, no fix linked | A MERGE colliding with another writer (e.g. hourly OPTIMIZE on the same Silver table) is exactly the pattern a 15-min cron creates. Catalog was Glue; whether Nessie/REST is exposed is **[I]**. |
| #26496 | MERGE on S3 fails intermittently with `FileAlreadyExistsException` writing position delete files (v468–476) | closed 2026-01-26, fixed by PR #27330 in **Trino 479** | Requires Trino ≥ 479. Workaround before that was `task_max_writer_count=1`. |
| #29344 | Row-level DML on Iceberg v2 fails `IllegalStateException: driverInstance count already set` (v480) | closed 2026-05-11 — user-side misconfiguration of `scale-writers`/`task.scale-writers` | MERGE is sensitive to writer-scaling config; not a Trino bug. |
| #18393 | Correctness: concurrent DELETE + OPTIMIZE could delete already-rewritten files (used `DeleteFiles` without existence validation) | closed, fixed PR #18533 (2023-08) | Fixed, but it is the archetype of the MERGE-vs-OPTIMIZE race this design would run every hour. |
| #22672 | 10–15 concurrent MERGEs into different partitions fail with metadata-location mismatch (v451) | closed 2025-01-31 via PR #24855 — commit retry count configurable (`max_commit_retry` table property, **Trino 470**) | Concurrency on the *same* table is retry-based, not lock-based; MERGE vs. maintenance is not serialised by anything. |
| #21619 | MERGE with partial INSERT column list wrote unreadable Parquet | closed, fixed PR #21697 (2024-04) | Historical; MERGE-path bugs surface late (data written, read fails). |
| #23313 | MERGE scans the whole target table | closed 2024-09-16 — maintainer: dynamic filtering *does* apply to the MERGE target, but on small data it finishes before the DF arrives; set `iceberg.dynamic-filtering.wait-timeout=10s` | Without DF or explicit partition predicates in `ON`, every 15-min MERGE reads the full Silver table. At GB scale acceptable; not free. |

**Bucketed target tables (Silver = `bucket(16, pk)`)**
- Trino writes `partitioning = ARRAY['bucket(pk, 16)']` (Trino's argument order is `bucket(col, n)`, opposite to Iceberg/Spark's `bucket(n, col)`). [V — docs]
- `iceberg.bucket-execution=true` (default) uses bucketing to avoid exchanges; a `ClassCastException` (Integer→Long) on partitioned+bucketed tables was fixed by PR #25750 (merged 2025-05-08). [V — #25125/#25750]
- Trino has **no equivalent of Spark's `write.distribution-mode`**; sorted writing (`sorted_by`) is a **per-file local sort only**; multiple writers produce overlapping min/max ranges. Issue #26112 "Iceberg table sorted_by not working" is **open** (maintainer confirmed the behaviour 2025-09-17; workaround is a global `ORDER BY` with `task_scale_writers_enabled=false`). So the Silver identifier sort order degrades to per-file sort under Trino. [V]
- Position deletes are *partition-local*. With 16 bucket partitions, one MERGE touching all buckets writes ≥16 position-delete files (one per touched data file). 96 MERGEs/day × N tables = thousands of delete files/day unless compacted. [V that deletes are per data file; **I** on the count]

**Memory / spill**
- MERGE = (window-dedup of the increment) ⋈ (target scan) → MergeWriter. Spill-to-disk exists for joins, aggregations, order-by, window (window spill "does not work in all cases"), is described by the docs as **legacy**, does not guarantee success, and can be "orders of magnitude" slower. Trino recommends fault-tolerant execution (FTE) instead, which needs an exchange manager (S3 spooling). Whether FTE covers Iceberg MERGE writes is not explicit in the FTE page [I]. [V — admin/spill.html, admin/fault-tolerant-execution.html]
- Trino MERGE also has to detect the "more than one source row matched" condition, an extra stage over the join. [I — follows from the documented error semantics]

**Net for A:** Trino MERGE is functionally sufficient for CDC upserts *including* `WHEN MATCHED AND __deleted THEN DELETE`, but it is MoR-only, sensitive to concurrent writers, and its file-layout controls (distribution, global sort, CoW) are weaker than Spark's. Minimum viable version is **Trino ≥ 479**.

---

## B. Incremental reads from Bronze without Spark — options and their precision

Trino has **no incremental scan** in the Spark sense (`start-snapshot-id`/`end-snapshot-id` on a table read). Issue #8780 "Incremental read support for Iceberg tables" is **open since 2021-08-04**. What exists:

1. **`system.table_changes(schema_name, table_name, start_snapshot_id, end_snapshot_id)`** (from #17154, closed 2023-10-26). Emits rows with `_change_type` (insert/delete), `_change_version_id`, `_change_timestamp`, `_change_ordinal`. Two hard limits: **"Tables with delete files are not supported"** and it reports per-snapshot, not net, changes. [V — docs]
   - Consequence: Bronze must stay strictly append-only. The 30-day TTL delete must be metadata-only (day-aligned cutoff, see A) or run as Spark CoW; **one mis-aligned Trino DELETE puts a position-delete file on Bronze and `table_changes` stops working for that table** until the file is compacted away. The Iceberg Kafka Connect sink is append-only (E), so it never creates delete files. [V sink is append-only; I on the operational consequence]
   - Precision: exact, snapshot-based — same as the Spark watermark. The watermark (last processed snapshot id) still has to be stored somewhere Trino can read it: a control table. Trino only exposes a fixed set of table properties via `ALTER TABLE SET PROPERTIES`, so "arbitrary property on the Silver table" is not available [I — docs list a fixed property set].

2. **`$entries` / `$all_entries` + `$path` filter** — select data files whose manifest entry has `status = 1 (ADDED)` and `snapshot_id` in the snapshots after the watermark, then `SELECT ... FROM bronze WHERE "$path" IN (...)`. Exact, and tolerant of delete files on *other* partitions. Whether Trino prunes splits on a `$path IN (...)` predicate (vs. scanning everything and filtering) is **[I]** — it does accept `$path`/`$file_modified_time` in WHERE (docs) and uses them for OPTIMIZE scoping. Two-step SQL, fiddly but doable in a dbt model with a pre-hook.

3. **Kafka high-water mark**: `WHERE (__kafka_partition, __kafka_offset) > last seen per partition`. Exact *if* tracked per partition (topic partition counts differ per table; a single scalar max offset is wrong). Requires keeping `__kafka_partition/__kafka_offset` in Silver or in a control table. [I — offset semantics are Kafka's]

4. **`__ts_ms` high-water mark with lookback** (`__ts_ms > watermark - interval 'x'`): simplest, dbt-idiomatic, and **imprecise**: `__ts_ms` is the *source* commit time; Debezium can deliver rows out of `__ts_ms` order (snapshot vs streaming phases, SQL Server LSN order vs commit-time, MongoDB), so a lookback window must be tuned per source and a late row outside the window is silently lost. Idempotency is fine (MERGE with latest-wins), completeness is not guaranteed. [I]

5. **`$file_modified_time`** metadata column: `WHERE "$file_modified_time" > watermark_ts`. File-level, cheap, commit-time based; equivalent in precision to the Spark snapshot watermark *if* the sink is the only Bronze writer (it is — 5-min commit interval). Clock skew is the sink JVM's clock at file write vs Trino's; use a safety overlap. [V that the column exists and is filterable; I on operational equivalence]

**Verdict for B:** Options 1/2/5 give snapshot-grade precision; 4 (the "natural dbt way") does not. Any of them is more SQL than Spark's `option("start-snapshot-id", …)` and none stores the watermark atomically with the MERGE.

---

## C. dbt-trino incremental merge as the Silver engine — design sketch, what is lost vs Spark

**What dbt-trino 1.10.x actually generates (read from source, `dbt/include/trino/macros/materializations/incremental.sql`)** [V]
- Strategies: `append` (default), `delete+insert`, `merge`, `insert_overwrite` (Hive), and `microbatch` (added in 1.9 via PR #453; the dbt docs support table is stale and omits it).
- `trino__get_merge_sql` emits exactly: `MERGE INTO target USING tmp ON (k1 = k1) AND (k2 = k2) WHEN MATCHED THEN UPDATE SET <cols> WHEN NOT MATCHED THEN INSERT ...`. **There is no DELETE branch and no `AND` condition on WHEN MATCHED.** `merge_update_columns`/`merge_exclude_columns` and `incremental_predicates` are honoured (predicates are ANDed into the `ON`).
- The `merge`/`append` path materialises the model SQL as a **view** (`views_enabled`), so the increment query runs *inside* the MERGE (single statement; good for atomicity). `delete+insert` uses a temp table + two statements (not atomic across the two).
- `microbatch` = `DELETE WHERE event_time in [start,end); INSERT` per batch — a window replace, **not an upsert**; wrong tool for CDC into a keyed Silver.
- `on_schema_change` uses dbt-core's generic `process_schema_changes` (`ignore` default, `fail`, `append_new_columns`, `sync_all_columns`). `sync_all_columns` on a **type change** was broken on Trino (#326: dbt's `alter_column_type` emits an `UPDATE` Trino rejects, then `DROP COLUMN ... CASCADE` which Trino lacks). Issue closed 2023 with a volunteer to fix; current behaviour **[I]** — test it. dbt-core's generic type-change path is add-new-column/copy/drop-old, which in Iceberg means a **new column id and loss of column history/stats** — categorically worse than `ALTER COLUMN SET DATA TYPE`, which Trino supports natively for int→bigint, real→double, decimal precision widening [V].

**Design sketch — "silver-merge as a generated dbt project"**
```
generated_dbt/
  dbt_project.yml
  macros/get_incremental_cdc_merge_sql.sql   # custom strategy, one macro, hand-written once
  macros/reconcile_schema.sql                # pre-hook: diff Bronze vs Silver types, emit ALTER COLUMN SET DATA TYPE or fail
  models/silver/<db>__<table>.sql            # one file per Bronze table, GENERATED by the console
  models/silver/schema.yml                   # unique_key, partitioning, sorted_by per model
  models/control/watermarks.sql              # control table; post-hook advances snapshot id per model
```
Custom strategy (dbt supports `get_incremental_<name>_sql(arg_dict)`; the dict carries `target_relation`, `temp_relation`, `unique_key`, `dest_columns`, `incremental_predicates`) [V — dbt docs]:
```sql
merge into {{ target }} t using {{ source }} s on <pk equality>
when matched and s.__deleted = true  then delete
when matched and (s.__ts_ms, s.__lsn, s.__kafka_offset) > (t.__ts_ms, t.__lsn, t.__kafka_offset) then update set ...
when not matched and s.__deleted = false then insert (...)
```
(The latest-wins guard makes replays idempotent, which is what allows the watermark to be committed non-atomically in a post-hook. Row-constructor comparison inside MERGE is [I].) The model body is the dedup:
```sql
with inc as (select * from {{ source('bronze', this.name) }} where <watermark predicate from B>),
ranked as (select *, row_number() over (partition by <pk> order by __ts_ms desc, __lsn desc, __kafka_offset desc) rn from inc)
select * from ranked where rn = 1
```
Scheduling: k8s CronJob every 15 min running `dbt run --select tag:silver` (Argo Workflows only if per-model retries/fan-out visualisation is wanted). dbt already exists on the platform (Gold models), so the image and the Keycloak/Trino profile exist; the console would regenerate the `models/silver/` tree from its pipeline registry. Parallelism = dbt `threads` (N models concurrently, each one Trino query); per-query resources via a Trino resource group for the dbt service user.

**What is lost vs the Spark job**

| Capability | Spark today | dbt-trino replacement | Loss |
|---|---|---|---|
| Watermark | snapshot id in Silver table property, exact | `table_changes`/`$entries`/`$file_modified_time` + control table (B) | more SQL; non-atomic watermark commit → relies on idempotent MERGE |
| Dedup + MERGE | one job, CoW | one MERGE, MoR only | delete files on Silver every 15 min; no CoW (#17272 open) |
| `__deleted` handling | native in MERGE | custom strategy macro | one macro, but it is *your* code inside dbt's incremental machinery |
| Schema reconciliation (add / safe widen / fail-loud) | explicit policy in code; `ALTER COLUMN TYPE` widening | `on_schema_change: append_new_columns` handles adds; widening = `fail` + a human, or a custom pre-hook that diffs types and emits `ALTER COLUMN SET DATA TYPE` | fail-loud achievable; safe widening = custom Jinja/SQL |
| Bucket-aware write (SPJ / distribution) | `write.distribution-mode=hash`, storage-partitioned join | Trino: bucket-aware execution for reads/joins; **no write distribution control**, local sort only (#26112 open) | more, smaller files per bucket; sort order degraded |
| Parallelism control | executors/cores per job | dbt `threads` + Trino resource group | coarser, shared with ad-hoc users |
| Failure isolation / observability | one Spark app per run; History Server | dbt run log; Trino query history | comparable |
| Cost of one increment | Spark start-up (~30–60 s) | dbt parse (~s) + one Trino query | dbt/Trino faster at GB scale [I] |

**Verdict for C:** feasible, and at GB scale the SQL would probably be *faster* than a Spark app start-up. The catch is not the merge — it is what MoR does to Silver afterwards (D), and that schema-widening becomes bespoke macro code that replaces bespoke Spark code one-for-one.

---

## D. Maintenance via Trino EXECUTE — capabilities, limits, delete-file folding

Trino 483 `ALTER TABLE … EXECUTE` procedures [V — docs]:
- `optimize(file_size_threshold => '100MB')`, optional `WHERE` on partition columns or `$file_modified_time`/`$path`. Per partition it rewrites when: >1 file below threshold, **or "at least one data file, with delete files attached, is present"**. So OPTIMIZE *does* fold position deletes into rewritten data files — but only for the partitions it fully rewrites.
- `optimize_manifests`, `expire_snapshots(retention_threshold, retain_last, clean_expired_metadata)`, `remove_orphan_files(retention_threshold)`, `drop_extended_stats`.
- Guard rails: `retention_threshold >= iceberg.expire-snapshots.min-retention` and `>= iceberg.remove-orphan-files.min-retention`, both default **7d**; both are catalog config you can lower (users in discussion #25211 run with `60s`). Same knob class exists in Spark — parity once configured.
- **No `rewrite_position_delete_files`.** Trino's roadmap #27371 (2025-11-19) lists "dangling delete files removal" and "equality delete conversion" as *planned*; discussion #25211 confirms "Trino doesn't expose rewrite_position_delete_files procedure".
- **Dangling delete files are still not cleaned in Trino 483**: issue #24086 (closed 2025-01-30 as related to PR #23801, which only cleans position deletes when OPTIMIZE runs *without* `$path`/`$file_modified_time` predicates) has new "still occurs" reports on **Trino 479 (2026-04-28) and Trino 483 (2026-09-09)** with a plain `optimize(file_size_threshold => '128MB')`. Users report writing their own zombie-file cleanup. [V — issue comments]
- Delete-file *thresholds* (Spark's `delete-file-threshold`, `min-input-files`) do not exist: issue #16574 **open** since 2023-03-15.
- `$partitions` lacked delete-file metrics until #28910 (closed 2026-05-01, so roughly Trino ≥ 481/482 [I on exact version]) — before that you could not even see where deletes were piling up.
- OPTIMIZE has its own reports: coordinator OOM on ORC optimize (#24794, 2025-01), optimize fails for multiple partitions (#27136), concurrency error when optimizing (#25584). [V titles only]

**Could the hourly Spark maintenance become a Trino-SQL CronJob?**
- TTL delete: yes, as `DELETE FROM bronze WHERE __ts_ms < <day-aligned literal>` — must be day-aligned to stay metadata-only (A/B).
- `rewrite_data_files` → `optimize`: yes. For Silver it must run **without** predicates so deletes are folded; with 16 bucket partitions and every-15-min MoR MERGEs, *every* partition has deletes every hour, so hourly OPTIMIZE rewrites the whole Silver table hourly — i.e. you re-implement copy-on-write, one hour late, as a full-table rewrite. Fine at GBs; quadratic pain later.
- `rewrite_position_delete_files`: **no equivalent**. Dangling deletes accumulate (#24086 live on 483).
- `expire_snapshots` / `remove_orphan_files`: yes.
- Write conflicts: hourly OPTIMIZE and the 15-min MERGE will overlap; Trino retries commits (`max_commit_retry`, Trino ≥ 470) but #26853 (open) shows a lost MERGE conflict can leave a table unreadable. No scheduler-level mutual exclusion exists unless you build it (running maintenance as dbt `run-operation` inside the same dbt invocation is one way to serialise per table).

**Verdict for D:** 4 of 5 maintenance steps map; the 5th (delete-file rewrite) is missing and is exactly the one that a MoR-only writer makes mandatory. Trino maintenance is adequate for tables *written by Spark CoW or by the append-only sink*; it is inadequate as the sole maintenance for tables *written by Trino MERGE*.

---

## E. Replacing the other Spark lanes (nginx streaming, S3 batch)

**nginx access log (Kafka → Iceberg via Spark Structured Streaming)**
- Replace with a **second Apache Iceberg Kafka Connect sink connector** on the Strimzi Connect cluster that already runs the CDC sink. Verified capabilities of the *Apache* sink (`apache/iceberg` `main`): commit coordination, exactly-once (KIP-447), multi-table fan-out/dynamic routing, auto-create + schema evolution, `iceberg.tables.default-partition-by`, `write-props.*`, commit interval default 5 min. **The Apache sink is append-only**: its writer classes are `IcebergWriter`, `PartitionedAppendWriter`, `NoOpWriter` — no delta/equality-delete writer; the config has `id-columns` but **no `upsert-mode`/`cdc-field`** (those existed in Tabular's fork; the SMT docs that still say "for use by the sink's CDC feature" are stale). Append-only is exactly right for logs. [V — source listing + `IcebergSinkConfig.java`]
- Schema-less JSON works: `RecordConverter` accepts `Map` values and infers Iceberg types (`SchemaUtils.inferIcebergType`); `JsonToMapTransform` exists for irregular JSON. The nginx line must already be JSON in Kafka (Fluent Bit/Vector, or nginx `log_format escape=json`) — the sink does not parse raw text; if today's Spark job does regex parsing, that moves to the log shipper or an SMT. [V converter; I about where parsing happens today]
- Trino Kafka connector: batch reads of topics as tables (`_partition_offset`, `_timestamp` columns); "topics can be live" but it is a polling table, not a stream — usable as `INSERT INTO iceberg SELECT … WHERE _partition_offset > hwm` in a CronJob, but that re-implements a sink badly. [V — docs] Flink: strictly more moving parts than the sink; not justified for one log table. [I]

**spark-batch (S3 files → Iceberg CTAS)**
- Trino CTAS from S3 needs the **Hive connector**, which requires an HMS Thrift service or Glue (`hive.metastore = thrift | glue` are the only documented types; `FileHiveMetastore.java` still exists in `plugin/trino-hive` but is undocumented/test-only). Nessie/Iceberg REST is not a Hive metastore. **So the "Trino CTAS" path adds a Hive Metastore component (plus its RDBMS)** to a platform that has none today. [V — object-storage/metastores.html; GitHub code search]
- Parquet/ORC-only alternative without HMS: `ALTER TABLE t EXECUTE add_files(location => 's3://…', format => 'PARQUET')` (`iceberg.add-files-procedure.enabled=true`) or `register_table`. **CSV/JSON not supported** by `add_files`. [V — docs]
- Trino has no `read_files()`/`read_parquet()` table function. [V — absent from docs]
- Realistic replacement: keep a tiny loader (Python + pyarrow/duckdb → Parquet → `add_files`) or accept HMS. Either is new code/components, not a deletion.

---

## F. "No-Spark" architecture: components before/after, code deleted, config added, risks

**Before (Spark-bearing parts only):** spark-operator, custom Spark image (Iceberg runtime, S3A, Nessie, Kafka connector), 3 `ScheduledSparkApplication`s (silver-merge 15 min, maintenance hourly, spark-batch), 1 long-running Structured Streaming `SparkApplication` (nginx), Spark History Server (+ event-log bucket), Spark-side Keycloak/S3 credentials, plus the job code for merge / schema-reconcile / maintenance / streaming / CTAS.

**After (no Spark):**
- *Removed:* spark-operator, custom image + its build pipeline, 4 Spark apps, History Server, all Spark job code.
- *Added:* (1) generated dbt project for Silver + 1 custom strategy macro + 1 schema-widening pre-hook macro + watermark control table; a 15-min CronJob running `dbt run` (dbt image already exists for Gold). (2) Trino maintenance SQL runner (CronJob with trino-cli, or dbt `run-operation`), including your own zombie-delete-file cleanup until Trino ships one. (3) Second Iceberg sink connector for nginx + JSON formatting at the shipper. (4) For S3 batch: **either** a Hive Metastore (new stateful component) **or** a small Parquet loader + `add_files` (new code) **or** dropping CSV/JSON support from the lane. (5) A mutual-exclusion mechanism between MERGE and OPTIMIZE per table. (6) Trino pinned ≥ 479 (FileAlreadyExists fix) — realistically current (483).
- *Config-vs-code shift:* per-table pipeline definition becomes dbt YAML/SQL that the console generates (good: reviewable, diff-able, same toolchain as Gold). The engine logic (custom merge strategy, widening, watermark, delete-file hygiene, writer exclusion) stays **code**, just Jinja-SQL instead of PySpark.
- *Component count:* roughly −6 Spark-side pieces, +2 to +4 new pieces. Net fewer *containers*, but the deleted pieces were battle-tested for exactly these Iceberg operations and the added pieces route the platform's hardest correctness path (upsert + compaction) through Trino's least mature surface (MoR-only DML, no delete-file rewrite).

**Sketch**
```
Debezium (Strimzi KC) ─► Kafka ─► Iceberg KC sink (append) ─► Bronze (day(__ts_ms), append-only, no delete files ever)
nginx shipper (JSON) ─► Kafka ─► Iceberg KC sink #2 (append) ─► logs table
CronJob 15m: dbt run (Trino) ─ custom cdc_merge strategy ─► Silver (bucket(pk,16), MoR deletes) ─ post-hook advances watermarks table
CronJob 1h : Trino SQL ─ DELETE bronze (day-aligned) ; optimize (no predicate) ; optimize_manifests ; expire_snapshots ; remove_orphan_files ; custom dangling-delete sweep
S3 batch  : add_files (Parquet/ORC) │ or Hive connector + HMS (new) │ or small loader (new)
Query     : Trino HA (now also the only writer) · Superset · dbt Gold · Zeppelin/Jupyter · Nessie · Keycloak
```

**Risks, ranked**
1. **Delete-file debt with no compactor** — MoR-only MERGE every 15 min into 16-bucket tables + no `rewrite_position_delete_files` + dangling deletes not removed by OPTIMIZE on Trino 483 (#24086 live). Mitigation = hourly full-table OPTIMIZE (works at GBs; a wall at TBs — which is where the product ambition points).
2. **Writer conflicts** — MERGE ∥ OPTIMIZE on the same table; #26853 open shows a conflict can strand a table. Needs explicit serialisation you must build.
3. **Bronze incremental-read fragility** — `table_changes` dies on any delete file; TTL must be metadata-only forever, and one careless `DELETE` breaks Silver for that table.
4. **Schema widening** — dbt's `sync_all_columns` type path is not Iceberg-aware (drop/add = new column id); you rewrite the Spark reconciliation policy as a macro.
5. **File layout** — no write distribution/global sort in Trino (#26112 open); more small files per bucket → more OPTIMIZE work.
6. **Shared engine** — the same Trino that serves Superset/dbt-Gold/notebooks now carries all write load; resource groups isolate CPU/memory, not coordinator planning load or catalog commit contention (#26563 slow planning with stats).
7. **Product dimension** — for a *product*, "Silver runs wherever the customer's Trino allows DML" has a smaller blast radius than "we ship a Spark image", but every customer then hits the Trino gaps above; Spark's Iceberg procedures are the reference implementation every Iceberg maintenance guide assumes.

---

## G. Evidence — VERIFIED vs INFERENCE

**VERIFIED (primary sources, read 2026-09-10)**
- Trino 483 Iceberg docs: MoR position deletes; whole-partition DELETE sentence; `optimize` conditions incl. "data file with delete files attached"; `expire_snapshots`/`remove_orphan_files` 7d floors and config names; `table_changes` signature + "Tables with delete files are not supported" + per-snapshot semantics; `add_files` Parquet/ORC only; v3 no DML/OPTIMIZE; `$path`/`$file_modified_time`/`$partition` metadata columns; `bucket(col, n)` syntax; `sorted_by` per-file; type-widening list; `iceberg.bucket-execution` default true; MV incremental refresh is append-delta (not upsert).
- Trino MERGE docs: syntax incl. `WHEN MATCHED AND … THEN DELETE`; ordered WHEN evaluation; fails on multi-match.
- Trino spill docs: legacy, joins/agg/order/window only, no guarantee. FTE docs: exchange manager requirement.
- Metastores docs: `thrift`/`glue` only; Iceberg catalog types incl. `rest`/`nessie`. `FileHiveMetastore.java` present in `plugin/trino-hive` (GitHub code search) but undocumented.
- GitHub states (API): #8780 open; #17154 closed 2023-10-26; #17272 open, PR #28958 open (created 2026-04-01, updated 2026-09-03); #23313 closed with DF wait-timeout advice; #26853 open; #26496 closed 2026-01-26 fixed by PR #27330 (Trino 479); #29344 closed (user config); #16574 open; #22672 closed via PR #24855 (Trino 470, `max_commit_retry`); #24086 closed 2025-01-30 but re-reported on 479 (2026-04-28) and 483 (2026-09-09); #18673 open; #7905 closed via PR #12795 (2022-06-20, boundary-aligned non-identity predicates subsumed); #12617 closed via PR #12704 (2022); #28910 closed 2026-05-01; #26112 open (local-sort-only confirmed by maintainer 2025-09-17); PR #23801 merged 2024-10-17 (position-delete cleanup only without `$path`/`$file_modified_time` predicates); PR #18533 merged 2023-08-09; PR #25750 merged 2025-05-08; roadmap #27371 (2025-11-19): dangling-delete removal + equality-delete conversion *planned*, only cross-partition parallelism *done*.
- Discussion #25211: no `rewrite_position_delete_files` in Trino; users run `expire_snapshots(retention_threshold => '60s')` after lowering min-retention.
- dbt-trino `master` (1.10.3, 2026-07-27): `trino__get_merge_sql` has no DELETE branch / no WHEN-MATCHED condition; `incremental_predicates`, `merge_update_columns`, `merge_exclude_columns`, `on_schema_change` via dbt-core `process_schema_changes`; `trino__get_incremental_microbatch_sql` = DELETE window + INSERT; `views_enabled` tmp-view path for merge/append. 1.9 changelog: "Microbatch incremental strategy (#453)". Issue #326 (sync_all_columns type change broken, closed 2023).
- dbt-core docs: custom `get_incremental_<name>_sql(arg_dict)` mechanism; dbt-trino built-ins listed as append/delete+insert/merge/insert_overwrite.
- apache/iceberg `main` kafka-connect: features list (exactly-once, fan-out, auto-create/evolve; no upsert); config table has `id-columns` but no `upsert`/`cdc-field`; `data` package contains only append writers; `RecordConverter` handles `Map` values with type inference; `JsonToMapTransform` docs; commit interval default 300 000 ms.
- Trino Kafka connector docs: table-style batch access with `_partition_offset`/`_timestamp`, live topics.
- Starburst blog (2022-11): recommends running update/delete/merge "in serial" against a single table.

**INFERENCE (not verified; test before relying on it)**
- That Trino's planner subsumes a `day()`-boundary-aligned `__ts_ms <` literal into a metadata-only DELETE on Bronze (PR #12795 semantics suggest yes; the docs sentence suggests no).
- That `$path IN (…)` predicates prune splits rather than filter post-scan.
- Delete-file counts per MERGE on a 16-bucket table; hourly full-table OPTIMIZE cost curve.
- Whether #26853 (Glue) reproduces with Nessie/REST catalogs.
- Whether FTE covers Iceberg MERGE writes; whether Trino's MERGE multi-match check adds a materialised stage.
- Row-constructor comparison inside a `WHEN MATCHED AND (a,b,c) > (x,y,z)` clause.
- Current behaviour of dbt-trino `sync_all_columns` on type change (2023 bug; no fix commit located).
- That the current nginx Spark job does regex parsing (vs. JSON already in Kafka).
- dbt parse/run overhead vs Spark app start-up at this scale.
- Exact Trino version in which #28910 (`$partitions` delete metrics) shipped.

---

## H. Verdict

**No — do not drop Spark from this platform now, and do not plan the product around dropping it.** The Silver merge itself could move to dbt-trino with one custom strategy macro and would probably run faster than a Spark job at university-scale data; that part is real and tempting. But it drags the platform's most correctness-critical path onto a Trino write surface that is merge-on-read-only (copy-on-write has been an open request since 2023 and its PR is still unmerged), has no delete-file rewrite procedure, still leaves dangling delete files behind on Trino 483 per a report filed *yesterday*, has an open MERGE-conflict-corrupts-table issue, and cannot control write distribution or global sort. Every one of those gaps is closed today by the Spark job you already run, and the Spark maintenance job is the reference implementation the whole Iceberg ecosystem documents. The "no-Spark" design also does not delete complexity so much as relocate it: watermarking, schema widening, delete-file hygiene and writer mutual exclusion all become bespoke Jinja/SQL plus a CronJob, the S3 batch lane needs a Hive Metastore or new loader code, and the shared query engine inherits all write load. The honest simplification available is narrower: **replace the Spark Structured Streaming nginx job with a second Iceberg Kafka Connect sink connector** (verified append-only, exactly-once, schema-inferring — strictly fewer moving parts), retire the Spark History Server if nobody reads it, and keep Spark for Silver MERGE (CoW), schema reconciliation, maintenance (incl. `rewrite_position_delete_files`) and the S3 batch CTAS. Revisit when Trino merges CoW (#28958) *and* ships position-delete rewrite / dangling-delete removal (#27371) — those two together remove most of the argument for Spark at this scale; until then a small team is better served by one boring Spark image than by four clever workarounds.
