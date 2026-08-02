import { useEffect, useState } from "react";
import {
  listBuckets,
  listTables,
  type BucketEntry,
  type TableEntry,
  type TablesResponse,
} from "../api/client";

function errorMessage(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}

/** Shared used/orphan badge for a table or bucket entry (identical
 * `used`/`pipeline`/`role`/`hint` shape on both `TableEntry` and
 * `BucketEntry`, per app.routers.tables/buckets's `_owned_map`): a used
 * entry names the owning pipeline + its role (authoritative/bronze/silver);
 * an orphan renders the backend's `_ORPHAN_HINT` explaining why it's
 * unowned (leftover from a deleted pipeline, or hand-created). */
function UsedBadge({
  entry,
}: {
  entry: Pick<TableEntry | BucketEntry, "used" | "pipeline" | "role" | "hint">;
}) {
  if (entry.used) {
    return (
      <span>
        {" "}
        · used by <strong>{entry.pipeline}</strong> ({entry.role})
      </span>
    );
  }
  return (
    <span>
      {" "}
      · <em>orphan</em>: {entry.hint}
    </span>
  );
}

/**
 * Tables & buckets view, per .superpowers/sdd/2026-08-02-pipeline-map-lineage/
 * task-5-brief.md: lists Iceberg namespaces/tables (`GET /api/tables`) and S3
 * buckets (`GET /api/buckets`) side by side, each entry now labeled
 * used/orphan with its record/object count (Task 3's enriched endpoints).
 * The two calls are independent and load (and can fail) independently, so
 * each gets its own loading/error state rather than one call blocking the
 * other's render. Fetch-on-load + a manual Refresh button re-fetches both --
 * deliberately no polling/interval (same on-demand pattern as PipelineMap).
 */
export default function TablesBuckets() {
  const [tables, setTables] = useState<TablesResponse | null>(null);
  const [tablesError, setTablesError] = useState<string | null>(null);

  const [buckets, setBuckets] = useState<BucketEntry[] | null>(null);
  const [bucketsError, setBucketsError] = useState<string | null>(null);

  function loadTables() {
    let cancelled = false;
    listTables()
      .then((data) => {
        if (!cancelled) {
          setTablesError(null);
          setTables(data);
        }
      })
      .catch((err: unknown) => {
        if (!cancelled) setTablesError(errorMessage(err));
      });
    return () => {
      cancelled = true;
    };
  }

  function loadBuckets() {
    let cancelled = false;
    listBuckets()
      .then((data) => {
        if (!cancelled) {
          setBucketsError(null);
          setBuckets(data);
        }
      })
      .catch((err: unknown) => {
        if (!cancelled) setBucketsError(errorMessage(err));
      });
    return () => {
      cancelled = true;
    };
  }

  function refresh() {
    loadTables();
    loadBuckets();
  }

  useEffect(() => loadTables(), []);
  useEffect(() => loadBuckets(), []);

  return (
    <div>
      <button type="button" onClick={refresh}>
        Refresh
      </button>

      <section>
        <h2>Tables</h2>
        {tablesError && <p role="alert">Failed to load tables: {tablesError}</p>}
        {!tablesError && tables === null && <p>Loading tables…</p>}
        {!tablesError && tables !== null && tables.namespaces.length === 0 && (
          <p>No namespaces found.</p>
        )}
        {!tablesError && tables !== null && tables.namespaces.length > 0 && (
          <ul>
            {tables.namespaces.map((ns) => (
              <li key={ns.name}>
                {ns.name}
                <ul>
                  {ns.tables.map((entry) => (
                    <li key={entry.table}>
                      <span>{entry.table}</span> —{" "}
                      {entry.records !== null ? entry.records : "—"} records
                      <UsedBadge entry={entry} />
                    </li>
                  ))}
                </ul>
              </li>
            ))}
          </ul>
        )}
      </section>

      <section>
        <h2>Buckets</h2>
        {bucketsError && <p role="alert">Failed to load buckets: {bucketsError}</p>}
        {!bucketsError && buckets === null && <p>Loading buckets…</p>}
        {!bucketsError && buckets !== null && buckets.length === 0 && (
          <p>No buckets found.</p>
        )}
        {!bucketsError && buckets !== null && buckets.length > 0 && (
          <ul>
            {buckets.map((entry) => (
              <li key={entry.name}>
                <span>{entry.name}</span> —{" "}
                {entry.objects !== null
                  ? `${entry.objects.count}${entry.objects.capped ? "+" : ""}`
                  : "—"}{" "}
                objects
                <UsedBadge entry={entry} />
              </li>
            ))}
          </ul>
        )}
      </section>
    </div>
  );
}
