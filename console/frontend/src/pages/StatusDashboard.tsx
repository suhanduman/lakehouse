import { useEffect, useState } from "react";
import { getStatus, type StatusResponse } from "../api/client";

function errorMessage(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}

function yesNo(value: boolean | null): string {
  if (value === null) return "Unknown";
  return value ? "Yes" : "No";
}

/**
 * Unified status dashboard, per docs/superpowers/sdd/task-14-brief.md:
 * renders `GET /api/status`'s per-connector state/dlq/maintenance signals
 * (`app.routers.status`). The endpoint degrades gracefully rather than
 * erroring (`{"connectors": [], "reachable": false, "error": ...}` when the
 * Connect worker itself is unreachable), so that degraded state is
 * surfaced as its own banner distinct from a network-level fetch failure.
 */
export default function StatusDashboard() {
  const [status, setStatus] = useState<StatusResponse | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    getStatus()
      .then((data) => {
        if (!cancelled) setStatus(data);
      })
      .catch((err: unknown) => {
        if (!cancelled) setError(errorMessage(err));
      });
    return () => {
      cancelled = true;
    };
  }, []);

  if (error) {
    return <p role="alert">Failed to load status: {error}</p>;
  }

  if (status === null) {
    return <p>Loading status…</p>;
  }

  return (
    <div>
      {!status.reachable && (
        <p role="alert">
          Kafka Connect unreachable{status.error ? `: ${status.error}` : ""}
        </p>
      )}

      {status.connectors.length === 0 ? (
        <p>No connectors found.</p>
      ) : (
        <table>
          <thead>
            <tr>
              <th>Name</th>
              <th>State</th>
              <th>Maintenance</th>
              <th>DLQ</th>
              <th>Lag</th>
              <th>Reachable</th>
            </tr>
          </thead>
          <tbody>
            {status.connectors.map((entry) => (
              <tr key={entry.name}>
                <td>{entry.name}</td>
                <td>{entry.state ?? "UNKNOWN"}</td>
                <td>{yesNo(entry.maintenance)}</td>
                <td>{yesNo(entry.dlq)}</td>
                <td>{entry.lag ?? "—"}</td>
                <td>{entry.reachable ? "Yes" : "No"}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}
