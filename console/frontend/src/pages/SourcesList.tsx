import { useEffect, useState } from "react";
import { listSources, type Source } from "../api/client";

/** Friendly label for a source's `cr_kind` (the underlying CR type), shown
 * next to `class` in the Type column so connector vs. spark-batch sources
 * are visually distinguishable in the merged list. */
function crKindLabel(crKind: string): string {
  switch (crKind) {
    case "ScheduledSparkApplication":
      return "Spark Batch";
    case "KafkaConnector":
      return "Kafka Connector";
    default:
      return crKind;
  }
}

export default function SourcesList() {
  const [sources, setSources] = useState<Source[] | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    listSources()
      .then((data) => {
        if (!cancelled) setSources(data);
      })
      .catch((err: unknown) => {
        if (!cancelled) {
          setError(err instanceof Error ? err.message : String(err));
        }
      });
    return () => {
      cancelled = true;
    };
  }, []);

  if (error) {
    return <p role="alert">Failed to load sources: {error}</p>;
  }

  if (sources === null) {
    return <p>Loading sources…</p>;
  }

  if (sources.length === 0) {
    return <p>No sources registered yet.</p>;
  }

  return (
    <table>
      <thead>
        <tr>
          <th>Name</th>
          <th>Type</th>
          <th>Status</th>
        </tr>
      </thead>
      <tbody>
        {sources.map((source) => (
          <tr key={source.name}>
            <td>{source.name}</td>
            <td>
              {source.class} <em>{crKindLabel(source.cr_kind)}</em>
            </td>
            <td>{source.paused ? "PAUSED" : source.state ?? "UNKNOWN"}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}
