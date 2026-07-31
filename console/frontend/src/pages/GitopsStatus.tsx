import { useEffect, useState } from "react";
import { getGitopsStatus, type GitopsStatusResponse } from "../api/client";

function errorMessage(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}

/** GitOps status page: renders GET /gitops/status (B-I). In direct mode the
 * API returns {mode:"direct"} and the page shows an informational note. */
export default function GitopsStatus() {
  const [data, setData] = useState<GitopsStatusResponse | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    getGitopsStatus()
      .then((d) => {
        if (!cancelled) setData(d);
      })
      .catch((err: unknown) => {
        if (!cancelled) setError(errorMessage(err));
      });
    return () => {
      cancelled = true;
    };
  }, []);

  if (error) return <p role="alert">Failed to load GitOps status: {error}</p>;
  if (data === null) return <p>Loading GitOps status…</p>;

  if (data.mode === "direct") {
    return (
      <p>
        GitOps is not enabled (direct mode). Sources are applied directly to the
        cluster.
      </p>
    );
  }
  if (!data.application) {
    return (
      <p>No ArgoCD Application status yet (pipeline not synced / ArgoCD not reachable).</p>
    );
  }

  return (
    <div>
      <p>
        Application: <span>{data.application.sync ?? "Unknown"}</span> /{" "}
        <span>{data.application.health ?? "Unknown"}</span>
      </p>

      {data.sources.length === 0 ? (
        <p>No per-source status.</p>
      ) : (
        <table>
          <thead>
            <tr>
              <th>Source</th>
              <th>Sync</th>
              <th>Health</th>
              <th>Resources</th>
            </tr>
          </thead>
          <tbody>
            {data.sources.map((s) => (
              <tr key={s.source}>
                <td>{s.source}</td>
                <td>{s.sync}</td>
                <td>{s.health}</td>
                <td>
                  <ul>
                    {s.resources.map((r) => (
                      <li key={`${r.kind}/${r.name}`}>
                        {r.kind}/{r.name}: {r.status ?? "—"} / {r.health ?? "—"}
                      </li>
                    ))}
                  </ul>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}

      <h3>Drift</h3>
      {data.outOfSync.length === 0 ? (
        <p>No drift — everything Synced.</p>
      ) : (
        <ul>
          {data.outOfSync.map((r) => (
            <li key={`${r.kind}/${r.name}`}>
              {r.kind}/{r.name}: {r.health ?? "—"}
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}
