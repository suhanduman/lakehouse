import { useEffect, useState } from "react";
import { listSchemas } from "../api/client";

function errorMessage(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}

/** Best-effort display fields out of Apicurio's schema-search response; the
 * client returns `Record<string, unknown>[]` (`app.services.connect_service
 * .ApicurioClient.list_schemas()`'s shape isn't pinned down further than
 * "a list of dicts" in the router), so field access here is defensive. */
interface SchemaLike {
  name?: unknown;
  id?: unknown;
  type?: unknown;
}

export default function Schemas() {
  const [schemas, setSchemas] = useState<Record<string, unknown>[] | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    listSchemas()
      .then((data) => {
        if (!cancelled) setSchemas(data);
      })
      .catch((err: unknown) => {
        if (!cancelled) setError(errorMessage(err));
      });
    return () => {
      cancelled = true;
    };
  }, []);

  if (error) {
    return <p role="alert">Failed to load schemas: {error}</p>;
  }

  if (schemas === null) {
    return <p>Loading schemas…</p>;
  }

  if (schemas.length === 0) {
    return <p>No schemas registered yet.</p>;
  }

  return (
    <table>
      <thead>
        <tr>
          <th>Name</th>
          <th>ID</th>
          <th>Type</th>
        </tr>
      </thead>
      <tbody>
        {schemas.map((schema, idx) => {
          const s = schema as SchemaLike;
          const name = typeof s.name === "string" ? s.name : `schema-${idx}`;
          return (
            <tr key={name}>
              <td>{name}</td>
              <td>{String(s.id ?? "")}</td>
              <td>{String(s.type ?? "")}</td>
            </tr>
          );
        })}
      </tbody>
    </table>
  );
}
