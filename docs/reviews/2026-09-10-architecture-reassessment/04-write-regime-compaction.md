# 04 — Iceberg CDC-upsert write regime: CoW vs MoR, commit cadence, compaction, Amoro

Independent architecture review, 2026-09-10. Sources are primary (Iceberg 1.9.0 docs/source at tag `apache-iceberg-1.9.0`, Trino 483 docs, Nessie spec/kernel docs, Amoro docs, Apple VLDB'24 paper, Tabular/Ryan Blue CDC series). Each claim is tagged **VERIFIED** (read in a primary source) or **INFERENCE** (my reasoning / not verifiable). No local repository files were read.

---

## A. CoW vs MoR for CDC upserts — mechanics, verified guidance, Trino read implications

### A.1 What copy-on-write MERGE actually rewrites — **VERIFIED**

Iceberg 1.9.0 Spark docs: *"Iceberg supports `MERGE INTO` by rewriting data files that contain rows that need to be updated in an `overwrite` commit."* and *"`MERGE INTO` can rewrite only affected data files"* (spark-writes.md). The default for all three row-level modes is CoW: `write.merge.mode = copy-on-write`, `write.delete.mode = copy-on-write`, `write.update.mode = copy-on-write` (configuration.md, "merge-on-read (v2 only)").

Apple's VLDB 2024 paper (Okolnychyi et al., the people who wrote this code path) is explicit about the failure mode: *"The main downside is its inefficiency in handling sparse updates, as all unmatched rows in modified data files must be copied over, leading to increased write amplification. Updating a record in each data file of the table requires rewriting the entire table, making this strategy impractical for sparse changes at scale."* (§3.1). And the reason MoR exists: *"Given that changes can be scattered across all data files and rewriting the entire table to handle a small set of changes is impractical at scale, we also added support for lazy materialization. This strategy is called merge-on-read."*

**So the platform's reasoning is confirmed, with a quantitative refinement.** For F data files and N distinct uniformly-random keys, the expected fraction of files touched is `1 − (1 − 1/F)^N ≈ 1 − e^(−N/F)`. At N = F you rewrite ~63 % of the table; at N = 3F ~95 %; at N = 5F >99 %. "N > F ⇒ whole table" is directionally right; "N ≥ 3F ⇒ whole table" is the accurate statement. **Crucially, PK-bucketing does not help**: `bucket(16, pk)` spreads random keys evenly over all 16 buckets, so every bucket is touched by any increment of a few dozen keys; bucketing helps the MERGE join (storage-partitioned join, no shuffle) — not write amplification. (INFERENCE from the math; the file-touch model is standard.)

### A.2 What MoR MERGE writes with Spark 3.5 / Iceberg 1.9 — **VERIFIED from source**

- Spark row-level MoR writes go through `SparkPositionDeltaWrite` (`spark/v3.5/.../source/SparkPositionDeltaWrite.java`), which uses `ClusteredPositionDeleteWriter` / `PositionDelete` and commits a `RowDelta`. **Spark never writes equality deletes**; equality deletes are the Flink/streaming-writer encoding.
- Delete granularity: the core `TableProperties.DELETE_GRANULARITY_DEFAULT` is `partition`, **but `SparkWriteConf.deleteGranularity()` defaults to `DeleteGranularity.FILE`** in 1.9.0 (`.defaultValue(DeleteGranularity.FILE)`). So on this stack, each MoR MERGE run produces **one position-delete file per touched data file** (plus one new data file per touched bucket for the updated/inserted rows). Apple paper §3.2.2: file granularity *"ensures only information required to read a data file will be loaded during a scan. However, it also increases the total number of delete files in the table and may require a more aggressive approach for delete file compaction."*
- Format v2 required (`merge-on-read (v2 only)`). V3 deletion vectors (one Puffin bitmap per data file, replaced not appended) exist in Iceberg 1.10+/1.11 (see D/E), but open-source Trino's v3 write status is unclear (INFERENCE, see A.4) — stay on v2 for now.

### A.3 Authoritative guidance on when to choose which — **VERIFIED**

- **Ryan Blue (Tabular), "Zen and the art of CDC performance"** (retrieved via Wayback): *"CDC pipelines are notoriously bad because updates are frequent and distributed across the whole table."* *"deltas are a way to defer work — it still must be done eventually."* Two real benefits of MoR: *"Deltas allow the write job to complete more quickly, making data available sooner and helping to reduce the chances of a write conflict"* and *"deltas from multiple commits can be rewritten in batches; deltas assist in reducing work when combined with compaction, but can easily be a performance disaster when used alone."* Best practices: *"pick a default commit frequency that works for most tables. This is usually between 5 and 15 minutes."* *"Running compaction once every 10 MERGE commits already reduces write amplification by about 10x."* *"Committing every minute instead of every 10 produces 10x the number of deltas and requires 10x more maintenance."* Anti-pattern: *"If maintaining a compaction process is a problem, then using deltas is just not a good strategy."* Also a pragmatic trick: *"change the MERGE strategy periodically to use copy-on-write rather than merge-on-read. That combines compaction and an update into one commit."*
- **Ryan Blue, "The CDC MERGE Pattern"**: *"Updating the mirror table twice as often roughly doubles the amount of work needed to keep it up-to-date."*
- **Dremio (CoW vs MoR blog)** recommendation table: "Nightly batch, read during day → CoW"; "Streaming CDC every minute → MoR or Deletion Vectors"; "High-frequency MERGE INTO → Deletion Vectors (V3)"; *"After compaction, reads are as fast as CoW."*
- **IOMETE**: MoR for "High-frequency streaming upserts or CDC ingestion (Kafka, Debezium, database logs)"; CoW for "Analytical workloads with batch updates (hourly or daily)"; compaction *"Daily for high-churn streaming systems and weekly for moderate workloads."* Caveat: *"If you use MOR just because 'it seems faster,' you are probably optimizing the wrong thing."*
- **Ryft (CDC strategies)**: *"For CDC workloads, Merge-on-Read is usually the better choice because it keeps the ingestion pipeline fast"* — conditional on robust compaction.

Consensus: for a 15-minute MERGE cadence on a table whose increments touch most files, MoR + scheduled compaction is the recommended regime; CoW is the recommended regime when merges are infrequent (hourly/daily) or the table is small enough that a full rewrite is cheap.

### A.4 Trino read implications — **VERIFIED unless marked**

- Trino 483 docs: *"Tables using v2 of the Iceberg specification support deletion of individual rows by writing position delete files."* Trino itself writes position deletes for its own DELETE/UPDATE/MERGE (there is no CoW option in Trino).
- Position deletes are cheap to apply per file (a per-split bitmap); equality deletes force every data file in scope to be checked against predicates. Iceberg dev list (Trino maintainers): *"from the perspective of engines like Trino, equality deletes bring little value and add lot complications"*; V4 discussion proposes deprecating equality deletes. Since Spark writes only position deletes, the platform avoids the bad case by construction.
- Historical Trino pain: issue #13092 ("Iceberg scanning with Delete Files is extremely/unusably slow") — root cause *"DeleteFilter will re-open and re-read the split's delete files for each page"*; fixed via PR #13219 (2022). Residual cost today is proportional to **number of delete files applicable per data file** (S3 round-trips) and delete rows held in memory per split. With file-granularity deletes and R MERGE runs between compactions, each data file carries ≤ R small delete files. Keep R ≲ 10–25 (Blue's "once every 10 commits").
- Trino `table_changes` function: *"Tables with delete files are not supported."* — irrelevant for Silver (consumers read current state), relevant if you ever want Trino-side CDC-of-Silver.
- Deletion vectors / v3 in OSS Trino: docs 483 allow `format_version = 3` (mentioned for VARIANT); DV read/write status in OSS Trino not stated in docs; a GitHub discussion (#29775, June 2026) asking for clarification has no maintainer answer. **INFERENCE: treat OSS Trino v3/DV as unproven; do not move Silver to v3 until verified on the cluster.**

---

## B. Quantified write-amplification model

### Assumptions (stated)
- Silver table size S; target file 256 MiB → F = max(16, 4·S) files (bucket(16) forces ≥16 files; at S = 2 GB files are 128 MiB).
- Increment: N **distinct** random PKs per 15-minute run, 96 runs/day; keys uniform over buckets/files (worst realistic case for CoW; hot-key skew only helps).
- CoW bytes written/run = touched_files × file_size, touched = F·(1 − (1 − 1/F)^N). Reads are ignored (add ~1× for read I/O).
- MoR write/run ≈ N × (300 B row + 40 B position-delete record) — negligible.
- Compaction = `rewrite_data_files` with **`delete-file-threshold = 1`** (as today): every data file with ≥1 attached delete is rewritten. File-granularity deletes (Spark default) attach to exactly the touched files; partition-granularity deletes attach to all files in the bucket (bounds cover the whole partition).
- "Hourly" = 4 runs of N keys accumulated; "daily" = 96 runs.

### Results — GB written per day (rewrite volume; excludes read I/O)

| S (GB) | F | N/run | CoW files touched/run | CoW GB/run | **CoW GB/day** | CoW ×table/day | MoR raw writes GB/day | **MoR + hourly compaction (thr=1, file-gran) GB/day** | MoR + hourly, partition-gran | **MoR + daily compaction GB/day** |
|---|---|---|---|---|---|---|---|---|---|---|
| 2 | 16 | 100 | 16.0 | 2.0 | **192** | 96× | 0.00 | 48 | 48 | **2** |
| 2 | 16 | 1,000 | 16.0 | 2.0 | **192** | 96× | 0.03 | 48 | 48 | **2** |
| 2 | 16 | 100,000 | 16.0 | 2.0 | **192** | 96× | 3.0 | 51 | 51 | **5** |
| 20 | 80 | 100 | 57.3 | 14.3 | **1,374** | 69× | 0.00 | 477 | 480 | **20** |
| 20 | 80 | 1,000 | 80.0 | 20.0 | **1,920** | 96× | 0.03 | 480 | 480 | **20** |
| 20 | 80 | 100,000 | 80.0 | 20.0 | **1,920** | 96× | 3.0 | 483 | 483 | **23** |
| 200 | 800 | 100 | 94.1 | 23.5 | **2,257** | 11× | 0.00 | 1,890 | 4,800 | **200** |
| 200 | 800 | 1,000 | 571 | 143 | **13,703** | 69× | 0.03 | 4,768 | 4,800 | **200** |
| 200 | 800 | 100,000 | 800 | 200 | **19,200** | 96× | 3.0 | 4,803 | 4,803 | **203** |

### Reading the table
1. **Today's CoW regime rewrites the whole Silver table nearly every 15 minutes** for any table receiving ≥ ~3·F distinct keys per run. For a 2 GB table that is 192 GB/day of writes — ~2.2 MB/s, trivial for MinIO/S3; the real cost is **Spark job time** (a full 2 GB shuffle-join-rewrite every 15 min) not bytes. For 20 GB it is ~1.9 TB/day (~22 MB/s sustained) and each run must finish a 20 GB rewrite inside its 15-minute slot — workable on a small cluster, but the job now runs near-continuously. For 200 GB it is 2–19 TB/day and **cannot finish in 15 minutes on a small cluster: the schedule collapses.**
2. **MoR with hourly compaction at `delete-file-threshold=1` only buys 4×** (it rewrites every touched file hourly instead of every 15 min). That is the cadence ratio, nothing more. Write amplification is governed by **how often you fold deltas**, not by the mode.
3. **MoR with daily compaction ≈ 1× table/day** — 96× cheaper than today. Intermediate cadences scale linearly: every 4 h ≈ 6× table/day; every 6 h ≈ 4×.
4. The price of deferring: between compactions each data file accumulates ≤ (runs since last compaction) position-delete files. Daily compaction ⇒ up to 96 delete files per data file, ~96 × 16 = 1,536 small data files + up to 96 × min(N, F) delete files per table per day → **too many for Trino**. Compaction every 4–6 h (16–24 runs) keeps ≤ 24 delete files/data file, ≤ 400 small files/table — acceptable, and a `rewrite_position_delete_files` pass in between (cheap: touches only delete files) consolidates them further.
5. **Break-even rule of thumb (INFERENCE):** CoW is fine while `S × 96 runs` fits comfortably in Spark budget, i.e. S ≲ 5 GB per table and total Silver volume × 96 ≲ what the Spark executors can rewrite per day. Beyond ~10–20 GB per table, or ~50 GB total Silver, switch to MoR + 4–6 h compaction.

---

## C. Commit cadence, metadata/manifest growth, Nessie single-branch semantics

### C.1 Bronze sink cadence — **VERIFIED**
- Apache Iceberg Kafka Connect sink default `iceberg.control.commit.interval-ms = 300,000 (5 min)`; the platform runs 60 s — 5× the default rate.
- Coordinator commits **once per table per interval** (single coordinator, `AppendFiles`/`RowDelta`), and **skips tables with nothing to write**: `"Nothing to commit to table {}, skipping"` (Coordinator.java). So idle tables do not generate snapshots; active tables generate up to 1,440 snapshots/day.
- Each commit writes ≥1 data file **per sink task** that received records, one new manifest, one manifest list, one new `metadata.json`. Design doc: *"Excessive numbers of snapshots can lead to bloated metadata files and performance issues"* — this is exactly why the sink centralizes commits; it does not make 60 s free.
- Community numbers (LakeOps): *"Kafka Connect with 5-minute commit intervals against 50 partitions produces 14,400 [files/day]"*; *"A table with 5-minute commits generates 8,640 snapshots per month"*; recommendation *"Set checkpoint/commit intervals to 5 minutes for analytical workloads. Sub-minute freshness is rarely needed and creates 5x more files."* Blue: 5–15 min default.
- **For this platform the 60 s interval buys nothing**: Silver is refreshed every 15 min, so Bronze freshness below 15 min only benefits direct Bronze queries. Going to 5 min cuts Bronze files, snapshots, manifests, metadata.json writes and Nessie commits by 5×.

### C.2 Metadata growth — **VERIFIED**
- `write.metadata.delete-after-commit.enabled` default **false**; `write.metadata.previous-versions-max` default 100. Maintenance doc: *"Tables with frequent commits, like those written by streaming jobs, may need to regularly clean metadata files."* and *"this will only delete metadata files that are tracked in the metadata log and will not delete orphaned metadata files."*
- What happens today (source-verified): `DeleteOrphanFilesSparkAction` protects only `ReachableFileUtil.metadataFileLocations(table, recursive=false)` — i.e. the current metadata.json plus the ≤100 tracked previous ones. Older, untracked `metadata.json` files are **orphan candidates** and are deleted by the hourly `remove_orphan_files` once older than `older_than` (default 3 days). So metadata.json accumulation is bounded at ~3 days × 1,440 ≈ 4,300 files/table — not unbounded, but silly. Set `delete-after-commit.enabled=true` and `previous-versions-max` ≈ 50–100 and stop relying on orphan cleanup for it.
- Snapshot count inside metadata.json: `history.expire.max-snapshot-age-ms` default **5 days**, `min-snapshots-to-keep` 1. At 1,440 commits/day and hourly expire with the default age, metadata.json carries ~7,200 snapshot entries (INFERENCE: ~2 MB JSON that every reader and every commit re-reads). For Bronze (a changelog with its own 30-day TTL) set snapshot age to 1 day (or 2 days at 5-min commits); Silver can keep 3–5 days for time travel.
- Manifests: `commit.manifest-merge.enabled=true`, `commit.manifest.min-count-to-merge=100`, `commit.manifest.target-size-bytes=8 MB` — regular `AppendFiles` (which the sink uses) **auto-merges manifests** once >100 accumulate, so manifest count per snapshot is bounded without `rewrite_manifests`. The absence of `rewrite_manifests` is therefore not a Bronze problem; it is a mild Silver nicety (re-cluster manifests by bucket; weekly is plenty). INFERENCE: with hourly compaction rewriting most of Silver, manifests are effectively rewritten anyway.

### C.3 Nessie single-branch commit semantics — **VERIFIED**
- Nessie spec: *"A Content Key must only occur once in a Nessie commit."* Put operations carry an expected state; mismatch ⇒ `NessieConflictException`. The spec's design intent, verbatim: *"You shouldn't have to update your reference before transacting on table A because it just happened to update table B whilst you were preparing your transaction."* ⇒ **commits to different tables on the same branch do not logically conflict.**
- Nessie maintainer (Ajantha Bhat, Nessie Google Group): *"At Nessie, conflict detection is table-level on a given branch."* Two writers on the **same** table (e.g. the Silver MERGE and the hourly maintenance job) do conflict at Nessie; the Iceberg client then refreshes and retries (`commit.retry.num-retries`, default 4) and Iceberg's own validation decides (append vs. rewrite of disjoint files is compatible; `rewrite_data_files` vs. a MERGE that deleted from the same files is not — that is the classic compaction/MERGE conflict, independent of Nessie).
- Kernel doc: commits are applied with a CAS on the branch pointer inside a **server-side retry loop** (`CommitRetry.commitRetry(...)` in `versioned/storage/common/.../logic/CommitRetry.java`); *"Concurrent commits against different branches are 'faster' than concurrent commits against a single branch"* and *"Concurrent commits against the same table ... are slower than concurrent commits against different tables."* Throughput: *"many hundred to many thousand commits per second, depending on the performance of the backend database."* Design target quoted in Nessie material: 100,000 tables each changing every 5 minutes ≈ 333 commits/s.
- Platform load: T active tables × 1,440/day. Even 500 active tables ⇒ 720k commits/day ≈ **8.3 commits/s** — two orders of magnitude under the design point. On Postgres-backed Nessie this is a non-issue. **"All tables on one branch" is not a contention problem; per-table branches would add operational complexity (merges, GC per ref) and buy nothing here.** Branches are for isolation (write-audit-publish, per-pipeline promotion), not throughput.
- One real single-branch cost (INFERENCE): the branch commit log grows by every commit of every table (~1,440 × T/day). Nessie's GC/repository maintenance must run; check that Nessie GC is scheduled — out of scope here but flagged.

---

## D. Compaction / maintenance tooling landscape

| Tool | What it does | Catalog fit (Nessie/REST) | k8s deployment reality | Maturity (2026-09) | OSS? |
|---|---|---|---|---|---|
| **Iceberg Spark procedures** (today) | `rewrite_data_files` (bin-pack/sort, `delete-file-threshold` default 2,147,483,647, `min-input-files` 5, `remove-dangling-deletes` false, `partial-progress`), `rewrite_position_delete_files` (drops dangling deletes), `rewrite_manifests`, `expire_snapshots` (default 5 d / keep 1), `remove_orphan_files` (default older than 3 d) — VERIFIED | Any Iceberg catalog | Already running (ScheduledSparkApplication) | Reference implementation | Yes |
| **Trino `ALTER TABLE … EXECUTE`** | `optimize` (rewrites files < `file_size_threshold` 100 MB **or with delete files attached**, per partition; with no path/mtime predicate it also drops the partition's position deletes — VERIFIED docs + issue #24086), `optimize_manifests`, `expire_snapshots` (server min-retention default 7 d, configurable `iceberg.expire-snapshots.min-retention`), `remove_orphan_files` (same 7 d floor), `drop_extended_stats`. **No `rewrite_position_delete_files`** (discussion #25211: *"Trino doesn't expose rewrite_position_delete_files procedure"*). TTL delete is plain SQL DELETE. | Nessie supported natively and via REST | Nothing to deploy; runs on the Trino cluster (use resource groups) | Stable, widely used | Yes |
| **Apache Amoro (incubating)** | AMS service that watches tables and schedules self-optimizing: *minor* (fragments <16 MB → segments; *"eq-delete files to pos-delete files"*), *major* (fold deletes, dedupe), *full*; triggers `self-optimizing.minor.trigger.file-count=12`, `minor.trigger.interval=1 h`, `major.trigger.duplicate-ratio=0.1`, `target-size=128 MB`, `quota=0.5`; plus `table-expire.enabled=true` (`snapshot.keep.duration=12 h`), `clean-orphan-file.enabled=false` by default, `clean-dangling-delete-files.enabled=true`, optional `data-expire.*` (TTL). Metrics/UI, optimizer groups. — VERIFIED docs | Iceberg via *Custom* catalog: `catalog-impl=org.apache.iceberg.rest.RESTCatalog` or `org.apache.iceberg.nessie.NessieCatalog` — VERIFIED docs | Helm chart in repo (`charts/amoro`), images `apache/amoro`, `apache/amoro-flink-optimizer`, `apache/amoro-spark-optimizer`; needs its own RDBMS (Derby default, MySQL/Postgres for prod); optimizers as Flink/Spark/local/"kubernetes" containers; resources must be sized explicitly (`memory`, `cpu.factor`). So: **+1 stateful Java service, +1 DB schema, +1 optimizer fleet.** | 0.8.1-incubating 2025-09-11; 0.9.0-incubating at rc8, vote open (Aug–Sep 2026); "nearing graduation" per Aug-2026 incubator report, but shepherd notes *"a number of issues on the private mailing list that have not been addressed even after multiple months."* ~1.2k stars; Iceberg **v1/v2 only** documented (v3/DV: nothing found — INFERENCE unsupported). Trino integration only for Amoro's own Mixed format (Trino 406). | Yes (ASF) |
| **OLake Fusion** | Per-table cron compaction with tiers (Lite/Medium/Full), `expire_snapshots`, `remove_orphan_files`, `rewrite_manifests`, metrics; claims ~2× faster than Spark compaction (vendor benchmark) | Not stated in the fetched page — INFERENCE: standard Iceberg catalogs incl. REST | Docker/Kubernetes per vendor blog | New (blog Apr 2026); part of `datazip-inc/olake` (Apache-2.0, ~1.4k stars) | Yes |
| **Ryft** | Managed, workload-aware Iceberg maintenance (compaction, expiry, orphan, manifests, re-sort by query pattern) | Cloud catalogs | SaaS/agent | Commercial | No |
| **Upsolver (now Qlik)** | "Adaptive Iceberg Optimizer"; acquired by Qlik (2025) | REST/Hive | Cloud / AWS VPC | Commercial | No (free "Iceberg Table Analyzer" tool only) |
| **Dremio / Tabular(Databricks)-style managed compaction** | Catalog-integrated background optimization | Their catalogs | Their platforms | Commercial | No |

**Can Trino replace the Spark maintenance job?** Technically almost: `optimize` covers small files *and* files-with-deletes (which is what `delete-file-threshold=1` does today), `expire_snapshots` + `remove_orphan_files` exist, TTL is SQL. Missing: `rewrite_position_delete_files`, `remove-dangling-deletes`, `partial-progress`, per-file-group sizing knobs, and the 7-day min-retention floors must be lowered by config if you want shorter retention. It is a viable simplification for GB-scale tables and it lets dbt own maintenance as post-hooks. Counter-argument: it puts compaction load on the BI query cluster (mitigable with resource groups) and Spark is already deployed for MERGE. Verdict: **optional**, not a fix for anything that is broken.

**Amoro?** Good design (it is the only OSS "continuous optimizer" with delete-aware minor/major planning), correct catalog fit, but for a small team with dozens–hundreds of GB-scale tables it adds a stateful control plane, a DB, an optimizer fleet, an incubating release cadence (~2 releases/yr) and a v3 blind spot. **Not now.** Revisit when (a) table count makes per-table Spark scheduling unmanageable (~>200 tables), or (b) compaction needs to be need-driven rather than cron-driven and you don't want to write that planner yourself.

---

## E. Platform today vs recommended

| Parameter | Today | Recommended | Why |
|---|---|---|---|
| Silver `write.merge.mode` / `write.delete.mode` / `write.update.mode` | copy-on-write (Iceberg default) | **merge-on-read** for Silver tables ≳ 5–10 GB or with increments touching most files; CoW stays fine for small (≤ 2–5 GB) tables | §B: CoW = ~96× table rewritten/day at 15-min cadence; MoR + 4–6 h compaction = 4–6×. Apple/Blue/Dremio all say MoR for frequent sparse upserts |
| Silver format-version | v2 (assumed) | v2 now; **v3 deletion vectors later** once OSS Trino read/write of DVs is verified on your cluster (Iceberg ≥1.10/1.11 on the Spark side) | DVs bound delete files to one per data file; OSS Trino status unverified (INFERENCE) |
| Delete granularity | Spark default `file` (1.9.0) | keep `file` | Trino loads only the deletes relevant to a split; cost is number of delete files per data file, which compaction bounds |
| MERGE cadence | 15 min | keep 15 min (Blue: 5–15 min default); make it per-table configurable | Cadence is the biggest lever on total work; 15 min is already the sane end of the range |
| `rewrite_data_files` cadence | hourly, `delete-file-threshold=1` | **every 4–6 h** (or trigger from `$files`/`$partitions` delete-file counts), `delete-file-threshold` 5–10, `min-input-files` 5 (default), `remove-dangling-deletes=true`, `partial-progress.enabled=true`; run `rewrite_position_delete_files` **hourly** (cheap, delete files only) | thr=1 hourly makes MoR only 4× better than CoW; Blue: compact every ~10 merges; keep ≤ ~25 delete files/data file between compactions |
| Maintenance order | TTL delete → rewrite_data_files → rewrite_position_delete_files → expire → remove_orphan (hourly, all of it) | rewrite_data_files(+dangling) → rewrite_position_delete_files → expire_snapshots (daily) → remove_orphan_files (**daily**, `older_than` ≥ 3 d, never hourly); Bronze TTL delete daily, with a predicate that hits whole `day(__ts_ms)` partitions | Orphan removal is an S3 LIST of the entire table prefix each run; hourly is pure cost. TTL on partition boundaries is metadata-only; a mis-aligned predicate turns it into row deletes |
| `rewrite_manifests` | never | weekly on Silver (optional) | Auto manifest-merge (`min-count-to-merge=100`) already bounds Bronze; Silver benefits only from re-clustering by bucket |
| Bronze sink commit interval | 60 s | **300 s** (the sink's default) unless a documented sub-15-min Bronze-direct SLA exists | Silver refreshes every 15 min; 60 s only multiplies files/snapshots/metadata/Nessie commits by 5 |
| `write.metadata.delete-after-commit.enabled` | false (default) | **true**, `previous-versions-max` 50–100, on every table (Bronze and Silver) | Otherwise metadata.json accumulates until orphan cleanup (3 d) catches it — verified source behaviour, but rely on the intended knob |
| `history.expire.max-snapshot-age-ms` | 5 d default | Bronze **1 d** (2 d at 5-min commits), Silver 3–5 d | Bronze is a changelog with its own 30-d TTL; thousands of snapshots bloat every metadata.json read |
| Catalog branches | one Nessie `main` | **keep `main`** for all pipelines; use branches only for WAP/promotion if ever needed | Different tables on one branch do not conflict; CAS contention at <10 commits/s is nil (Nessie designed for 333/s) |
| Maintenance tool | hand-written Spark job | keep Spark now; optionally move expire/orphan/optimize to Trino `EXECUTE` via dbt post-hooks; **Amoro not yet** | Spark works and is already there; Amoro = extra control plane for a small team, v3 blind spot |
| Bronze compaction | (implied by hourly job on all tables) | bin-pack Bronze daily, only current-day partition (`where` on `__ts_ms`) | Bronze is append-only, day-partitioned; older partitions never change |

---

## F. Evidence

**VERIFIED (read in primary source)**
- Iceberg 1.9.0 `docs/docs/spark-writes.md`: MERGE INTO rewrites data files that contain matched rows in an overwrite commit. https://github.com/apache/iceberg/blob/apache-iceberg-1.9.0/docs/docs/spark-writes.md
- Iceberg 1.9.0 `docs/docs/configuration.md`: defaults for `write.merge/delete/update.mode = copy-on-write`, `write.metadata.delete-after-commit.enabled=false`, `previous-versions-max=100`, `commit.manifest.min-count-to-merge=100`, `commit.manifest-merge.enabled=true`, `history.expire.max-snapshot-age-ms=5 days`. https://github.com/apache/iceberg/blob/apache-iceberg-1.9.0/docs/docs/configuration.md
- Iceberg 1.9.0 `docs/docs/spark-procedures.md`: `delete-file-threshold` default 2147483647, `min-input-files` 5, `remove-dangling-deletes` false, `rewrite_position_delete_files` semantics, `expire_snapshots` 5 d, `remove_orphan_files` 3 d. https://github.com/apache/iceberg/blob/apache-iceberg-1.9.0/docs/docs/spark-procedures.md
- Iceberg 1.9.0 `docs/docs/maintenance.md`: metadata file retention quotes. https://github.com/apache/iceberg/blob/apache-iceberg-1.9.0/docs/docs/maintenance.md
- Iceberg 1.9.0 `docs/docs/kafka-connect.md`: `iceberg.control.commit.interval-ms` default 300,000. https://github.com/apache/iceberg/blob/apache-iceberg-1.9.0/docs/docs/kafka-connect.md
- Iceberg 1.9.0 `kafka-connect/.../channel/Coordinator.java`: skip empty commits; `newAppend()`/`newRowDelta()`. https://github.com/apache/iceberg/blob/apache-iceberg-1.9.0/kafka-connect/kafka-connect/src/main/java/org/apache/iceberg/connect/channel/Coordinator.java
- Iceberg 1.9.0 `SparkWriteConf.deleteGranularity()` default `FILE`; `TableProperties.DELETE_GRANULARITY_DEFAULT = PARTITION` (core); `SparkPositionDeltaWrite` uses position deletes only. https://github.com/apache/iceberg/blob/apache-iceberg-1.9.0/spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/SparkWriteConf.java
- Iceberg 1.9.0 `BaseSparkAction.otherMetadataFileDS` → `ReachableFileUtil.metadataFileLocations(table, false)`; `DeleteOrphanFilesSparkAction` uses it (untracked metadata.json are orphan candidates). https://github.com/apache/iceberg/blob/apache-iceberg-1.9.0/spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/actions/BaseSparkAction.java
- Kafka Connect sink design doc (Tabular/Databricks): commit coordination rationale. https://github.com/databricks/iceberg-kafka-connect/blob/main/docs/design.md
- Apple VLDB'24, Okolnychyi et al., "Petabyte-Scale Row-Level Operations in Data Lakehouses" §3.1–3.2. https://vldb.org/pvldb/vol17/p4159-okolnychyi.pdf
- Ryan Blue, "Zen and the art of CDC performance" (Tabular; Wayback copy). https://web.archive.org/web/2024/https://tabular.io/blog/cdc-zen-art-of-cdc-performance/
- Ryan Blue, "The CDC MERGE Pattern" (LinkedIn mirror). https://www.linkedin.com/pulse/cdc-merge-pattern-tabular-io
- Trino 483 Iceberg connector docs: `optimize` conditions, `optimize_manifests`, `expire_snapshots`/`remove_orphan_files` 7 d min-retention, position deletes, `table_changes` limitation. https://trino.io/docs/current/connector/iceberg.html
- Trino discussion #25211 (optimize does not expose rewrite_position_delete_files); issue #24086 (position deletes cleaned in OPTIMIZE only for whole-partition predicates; otherwise removed when min data sequence number passes); issue #13092 / PR #13219 (delete-file re-read fix). https://github.com/trinodb/trino/discussions/25211 https://github.com/trinodb/trino/issues/24086 https://github.com/trinodb/trino/issues/13092
- Nessie spec (content key uniqueness, expected-state conflicts, table A/B quote). https://projectnessie.org/develop/spec/
- Nessie commit kernel (CAS + retry loop, throughput statements). https://projectnessie.org/develop/kernel/ ; `CommitRetry.java` https://github.com/projectnessie/nessie/blob/main/versioned/storage/common/src/main/java/org/projectnessie/versioned/storage/common/logic/CommitRetry.java
- Nessie Google Group, Ajantha Bhat: "conflict detection is table-level on a given branch." https://groups.google.com/g/projectnessie/c/KL5ceKT-SP0
- Amoro docs: self-optimizing, configurations (defaults), managing-catalogs (RESTCatalog / NessieCatalog), deployment-on-kubernetes (Helm, images, DB), managing-optimizers. https://amoro.apache.org/docs/latest/self-optimizing/ https://amoro.apache.org/docs/latest/configurations/ https://amoro.apache.org/docs/latest/managing-catalogs/ https://amoro.apache.org/docs/latest/deployment-on-kubernetes/
- Amoro releases/tags (0.8.1-incubating 2025-09-11; v0.9.0-rc8, no final tag as of 2026-09-10) via GitHub API; ASF Incubator report Aug 2026. https://github.com/apache/amoro/releases https://cwiki.apache.org/confluence/spaces/INCUBATOR/pages/446070925/August2026
- Dremio, "Row-Level Changes on the Lakehouse: CoW vs MoR". https://www.dremio.com/blog/row-level-changes-on-the-lakehouse-copy-on-write-vs-merge-on-read-in-apache-iceberg/
- IOMETE, "MoR vs CoW in the Real World". https://iomete.com/resources/blog/merge-on-read-vs-copy-on-write
- Ryft, "CDC Strategies in Apache Iceberg". https://www.ryft.io/blog/cdc-strategies-in-apache-iceberg
- LakeOps, "Kafka to Iceberg Compaction — Done Right" (file/manifest counts). https://lakeops.dev/blog/kafka-to-iceberg-compaction
- OLake Fusion blog (Apache-2.0, tiers, maintenance ops). https://olake.io/blog/apache-iceberg-table-maintenance-olake-fusion/
- Qlik acquires Upsolver (commercial). https://www.qlik.com/us/news/company/press-room/press-releases/qlik-acquires-upsolver-to-deliver-low-latency-ingestion-and-optimization-for-apache-iceberg

**INFERENCE (not verified in a primary source)**
- The file-touch probability model and all numbers in §B (standard combinatorics; row/delete byte sizes assumed).
- Break-even thresholds (5–10 GB per table, ~50 GB total Silver) for CoW → MoR.
- OSS Trino's deletion-vector (v3) read/write support status; Amoro's lack of v3 support.
- Nessie branch commit-log growth needing GC scheduling.
- metadata.json size estimate (~2 MB at ~7,200 snapshots).
- OLake Fusion catalog support.

---

## G. Verdict

The current regime is **an acceptable default with the wrong knob settings for anything but small tables, and a clear scaling cliff that is already visible at ~20 GB and fatal at ~200 GB.** CoW MERGE every 15 minutes rewrites essentially the whole Silver table 96 times a day as soon as an increment carries more than ~3× as many distinct keys as the table has files — and PK-bucketing does nothing to prevent that. At today's GB-scale it "works" only because 2 GB × 96 is cheap; the Spark job, not the object store, is what will run out of headroom first. The hourly compaction with `delete-file-threshold=1` is the second half of the same mistake: even if you flip Silver to merge-on-read, that setting re-creates 24 full rewrites a day, so MoR would look like a 4× win instead of the 20–100× it should be. Nothing about Nessie, the single branch, or the 60 s sink interval is a correctness or contention problem — they are just 5× more files, snapshots, metadata.json rewrites and commits than the Silver cadence can ever use. **Minimal fix, in order: (1) set `write.merge.mode`/`write.delete.mode`/`write.update.mode = merge-on-read` on Silver tables (v2, Spark's default file-granularity deletes); (2) change `rewrite_data_files` to every 4–6 h with `delete-file-threshold` 5–10 and `remove-dangling-deletes=true`, keep `rewrite_position_delete_files` hourly, move `remove_orphan_files` and `expire_snapshots` to daily; (3) set the sink commit interval to 5 min and `write.metadata.delete-after-commit.enabled=true` everywhere, with a 1-day snapshot age on Bronze.** Leave Nessie on one branch, leave Spark as the maintenance engine, and put Amoro and Iceberg v3 deletion vectors on the scaling roadmap rather than in the product today.
