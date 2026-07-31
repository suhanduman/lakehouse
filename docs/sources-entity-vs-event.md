# Sources: entity vs. event

Every ingest source registered with the lakehouse console has a
**disposition** — `entity` or `event` — that decides what Bronze→Silver does
with each record. This doc explains the difference, when to pick each for an
existing Kafka topic, the exact semantics of `stream/kafka` **entity**
sources (including the optional delete opt-in), the ordering guarantee you
need to make upserts deterministic, and the `columns`/`identifier`
declaration these sources require. It is a semantics reference for
operators declaring sources, not an API/CR reference.

## The two dispositions

| | `event` (append-only) | `entity` (upsert) |
|---|---|---|
| Bronze | every message becomes a new row (an append-only log) | every message becomes a new row (an append-only log) |
| Silver | **no merge** — Bronze *is* the queryable history | the scheduled Spark `merge_cdc` job MERGEs the Bronze increment into Silver, producing **current state** per key |
| Use for | logs, IoT telemetry, click/metric events, anything where every message is its own fact and history matters | records that represent updates to a long-lived entity (an order, a customer, a device's latest reading) where you want "what does this row look like *now*" |

Both dispositions land in Bronze identically — a `stream/kafka` source
always gets its own dedicated Iceberg sink connector, and every record is
appended. The disposition only changes what happens **after** Bronze: `event`
stops there; `entity` schedules a `merge_cdc` MERGE that folds the Bronze
increment into a Silver table keyed by `identifier`.

### Picking a disposition for an existing Kafka topic

Ask: *does a later message on this topic supersede an earlier one for the
same key, or is every message an independent fact?*

- **Keyed entity-update stream** (e.g. an `orders` topic where each message
  is the current state of order `id`, produced whenever the order changes) →
  **entity**. You want Silver to hold one row per `id` with the latest
  values.
- **Log/IoT/event stream** (e.g. sensor readings, page-view events, audit
  log entries) → **event**. Every message is a fact you want to keep; there
  is no "latest version of this row" — a "latest reading per device" view,
  if you need one, is a query concern, not an ingest concern. Do not use
  `entity` here: MERGEing away every intermediate reading and keeping only
  the last one per key throws away exactly the history you're ingesting
  events for.

If you're unsure, default to `event`: an append-only Bronze table never
loses data and can always be reprocessed into a different shape later.
`entity` is the deliberate choice when Silver having *current state* per key
is the actual goal.

## entity-from-Kafka semantics (v1)

Declaring a `stream/kafka` source with `disposition: entity` requires an
`identifier` (the entity's primary-key column(s), used both as the Iceberg
table's identifier field and as the MERGE join key). Beyond that, v1
behaves as follows.

### Default: upsert-only — deletes are not captured

With no further configuration, an `entity` Kafka source is **upsert-only**:
every message MERGEs into Silver as an insert-or-update against
`identifier`, and there is no way to remove a row. This is the *same
contract* as a scheduled-JDBC `entity` source (a plain poller with no
delete signal) — it is not a gap specific to Kafka, it's the baseline
behavior for any entity source that doesn't carry an explicit delete
signal.

If your use case needs deletes to propagate, either supply `delete_field`
(below) or use a CDC source (Debezium) instead, which carries delete events
natively in its envelope (`__op == "d"`).

### Optional: `delete_field` (boolean delete opt-in)

If the upstream topic already carries a boolean "is this record deleted"
flag on every message, declare it as `delete_field: <field name>` on the
source. The dedicated Iceberg sink's SMT chain renames that field to the
`__deleted` Bronze metadata column, and `merge_cdc`'s generated MERGE SQL
includes `WHEN MATCHED AND __deleted THEN DELETE` — so a record with the
flag set to `true` deletes the matching Silver row on the next merge tick,
and a later record for the same key with the flag `false` re-inserts it
(upsert semantics apply to deletes too: last write wins).

Two hard requirements, both because of how the SMT rename and the MERGE's
three-valued boolean logic work:

- **The field must be a genuine boolean, present on *every* record.** If a
  record omits it, the renamed `__deleted` column is `NULL` for that row —
  and `NULL` in a `WHEN MATCHED AND __deleted` / `WHEN NOT MATCHED` boolean
  test is neither true nor false, so SQL's three-valued logic silently
  **skips** that record's insert-or-delete instead of erroring. A record
  missing the flag doesn't fail loudly; it just never reaches Silver. Make
  sure your producer stamps the flag on every message, including inserts
  and plain updates (`false` for those).
- **The field must NOT be listed in `columns`.** It is *consumed* by the
  sink's rename transform — it becomes the `__deleted` metadata column, not
  a Silver data column. Declaring it in `columns` as well is redundant at
  best and a schema conflict at worst.

### Future (v2): value-equality deletes

A common CDC-flavored convention is a discriminator field with a delete
*value* rather than a dedicated boolean — e.g. `op == "d"`, or any
arbitrary `field == value` meaning "delete this key" (Debezium's own
envelope does this with `__op`). Supporting an **arbitrary value-equality**
delete rule for existing-Kafka entity sources is a planned v2 item; it
needs a custom Single Message Transform (the stock Kafka Connect SMTs can
rename/insert/drop fields, but can't conditionally derive a boolean from
"does field X equal value Y"). Until that lands, `delete_field` (a
dedicated boolean field) is the only supported delete signal for
existing-Kafka entity sources.

## Ordering: key your topic by the entity PK

`merge_cdc` needs a deterministic "latest record per key" to MERGE
correctly. Its dedup ORDER BY is `__ts_ms DESC, __lsn DESC NULLS LAST,
__kafka_offset DESC` — for a Kafka entity source, there is no `__lsn`
(that's a CDC/Debezium-only column, always null here), so the effective
tie-break is **`__ts_ms` (the record timestamp, stamped by the Iceberg sink
at ingest), then `__kafka_offset`**.

This is reliable **only if all records for a given key land on the same
Kafka partition**, which Kafka guarantees when the topic is **keyed by the
entity's primary key** (the default partitioner routes by key, and
per-partition offsets are strictly monotonic, so "biggest offset for this
key" is an unambiguous "most recent"). Recommendation: **produce to the
topic keyed by the entity PK** (the same column(s) you declare as
`identifier`).

**Caveat — unkeyed or non-PK-keyed topics:** if records for one entity can
land on *different* partitions (topic not keyed by PK, or a key that isn't
the PK, or a repartitioning of the topic), there is no cross-partition
offset ordering — offsets are only monotonic within a partition. In that
case the dedup falls back to `__ts_ms` as the primary sort key, which is a
wall-clock timestamp: it can suffer clock skew, coarse resolution (multiple
records landing in the same millisecond), and reordering in transit. It is
**not a reliable global order** the way per-key offset monotonicity is.
Treat entity sources on unkeyed topics as best-effort last-writer-wins, not
a correctness guarantee — if you need a hard guarantee and can't key the
topic by PK, prefer an `event` disposition (keep every record) or a CDC
source instead.

## `columns` / `identifier`: user-declared, not introspected

Kafka topics are schemaless from the platform's point of view — messages
are arbitrary JSON, and there is no schema registry lookup or payload
sampling to infer a table shape. Because of that, both `columns` (the
target Iceberg schema: name/type/required per field) and `identifier` (the
primary-key column(s) for the MERGE join and Iceberg identifier field) must
be **declared by the operator** when registering the source — the platform
pre-creates the Silver table from `columns`/`identifier` before the
connector runs; it does not discover them from the topic's data. Get these
right up front: `identifier` in particular has to match the field(s) your
producer actually uses as the entity's stable key, or the MERGE will treat
different logical states of the same entity as unrelated rows (or, if it's
a strict superset/subset of the true key, collapse rows that should stay
distinct).
