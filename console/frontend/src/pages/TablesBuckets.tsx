import { useEffect, useState } from "react";
import { listBuckets, listTables, type TablesResponse } from "../api/client";

function errorMessage(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}

/**
 * Tables & buckets view, per docs/superpowers/sdd/task-14-brief.md: lists
 * Iceberg namespaces/tables (`GET /api/tables`) and S3 buckets
 * (`GET /api/buckets`) side by side. The two calls are independent and load
 * (and can fail) independently, so each gets its own loading/error state
 * rather than one call blocking the other's render.
 */
export default function TablesBuckets() {
  const [tables, setTables] = useState<TablesResponse | null>(null);
  const [tablesError, setTablesError] = useState<string | null>(null);

  const [buckets, setBuckets] = useState<string[] | null>(null);
  const [bucketsError, setBucketsError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    listTables()
      .then((data) => {
        if (!cancelled) setTables(data);
      })
      .catch((err: unknown) => {
        if (!cancelled) setTablesError(errorMessage(err));
      });
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    let cancelled = false;
    listBuckets()
      .then((data) => {
        if (!cancelled) setBuckets(data);
      })
      .catch((err: unknown) => {
        if (!cancelled) setBucketsError(errorMessage(err));
      });
    return () => {
      cancelled = true;
    };
  }, []);

  return (
    <div>
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
                  {ns.tables.map((table) => (
                    <li key={table}>{table}</li>
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
            {buckets.map((bucket) => (
              <li key={bucket}>{bucket}</li>
            ))}
          </ul>
        )}
      </section>
    </div>
  );
}
