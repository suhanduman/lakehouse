import { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { getConnectorDlq, getPipelines, type Pipeline, type PipelineNode } from "../api/client";

function errorMessage(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}

interface Dot {
  symbol: string;
  color: string;
}

/** Color-coded status dot for connector/sink/merge chips. Unrecognized/empty
 * state (including the backend's own "unknown" fallback from
 * `_safe_status`) renders as an unfilled circle rather than guessing. */
function statusDot(state: string): Dot {
  switch (state.toUpperCase()) {
    case "RUNNING":
      return { symbol: "●", color: "green" };
    case "PAUSED":
      return { symbol: "⏸", color: "#b8860b" };
    case "FAILED":
      return { symbol: "✖", color: "red" };
    default:
      return { symbol: "○", color: "gray" };
  }
}

function StatusDot({ state }: { state: string }) {
  const dot = statusDot(state);
  return (
    <span title={state} style={{ color: dot.color }}>
      {" "}
      {dot.symbol}
    </span>
  );
}

/** Node types that carry a `state` (per `pipeline_topology._assemble_one`:
 * only connector/sink/merge CRs have a live Kafka Connect / Spark status to
 * report; source/topic/bronze/silver/buckets are static facts). */
function nodeState(node: PipelineNode): string | null {
  if (node.type === "connector" || node.type === "sink" || node.type === "merge") {
    return node.state;
  }
  return null;
}

function nodeLabel(node: PipelineNode): string {
  switch (node.type) {
    case "source":
      return `Source: ${node.name}`;
    case "connector":
      return node.name ?? node.kind ?? "connector";
    case "topic":
      return `Topic: ${node.name}`;
    case "sink":
      return node.name ?? "sink";
    case "bronze":
      return `Bronze: ${node.fqn}`;
    case "merge":
      return node.name;
    case "silver":
      return `Silver: ${node.fqn}`;
    case "buckets":
      return `Buckets: ${node.buckets.join(", ")}`;
    default:
      return "node";
  }
}

/** The connector whose dead-letter queue represents this pipeline's dropped
 * records: the `sink` node (event/entity pipelines with a separate sink
 * connector), or -- for kafka-ingest pipelines that have no separate sink --
 * the `connector` node. `null` when neither is present/named. */
function dlqCarryingConnectorName(pipeline: Pipeline): string | null {
  const sink = pipeline.nodes.find((n): n is Extract<PipelineNode, { type: "sink" }> => n.type === "sink");
  if (sink) return sink.name;
  const connector = pipeline.nodes.find(
    (n): n is Extract<PipelineNode, { type: "connector" }> => n.type === "connector",
  );
  return connector?.name ?? null;
}

function DlqBadge({ dlqName, count }: { dlqName: string | null; count: number | null | undefined }) {
  if (count == null || !dlqName) return null;
  if (count === 0) return <span> ✓ 0</span>;
  return (
    <Link to={`/sources/${encodeURIComponent(dlqName)}`}>
      <strong> ⚠ {count} dropped</strong>
    </Link>
  );
}

function PipelineRow({
  pipeline,
  dlqCount,
}: {
  pipeline: Pipeline;
  dlqCount: number | null | undefined;
}) {
  const [copied, setCopied] = useState(false);
  const dlqName = dlqCarryingConnectorName(pipeline);

  function handleCopy() {
    if (!pipeline.authoritative) return;
    navigator.clipboard.writeText(pipeline.authoritative.fqn);
    setCopied(true);
  }

  return (
    <li>
      <div>
        <strong>{pipeline.name}</strong>
        {pipeline.disposition && <span> [{pipeline.disposition}]</span>}
        <DlqBadge dlqName={dlqName} count={dlqCount} />
      </div>

      {pipeline.error ? (
        <p role="alert">Pipeline error: {pipeline.error}</p>
      ) : (
        <p>
          {pipeline.nodes.map((node, i) => {
            const state = nodeState(node);
            return (
              <span key={i}>
                {i > 0 ? " → " : ""}
                {nodeLabel(node)}
                {state !== null && <StatusDot state={state} />}
              </span>
            );
          })}
        </p>
      )}

      {pipeline.authoritative && (
        <p>
          <span aria-hidden="true">⭐</span> Authoritative:{" "}
          <code>{pipeline.authoritative.fqn}</code>{" "}
          <button type="button" onClick={handleCopy}>
            Copy FQN
          </button>
          {copied && <span> Copied</span>}
        </p>
      )}
    </li>
  );
}

/**
 * Pipeline Map: renders `GET /api/pipelines` (`app.services.pipeline_topology`)
 * as one horizontal end-to-end flow row per pipeline, plus its copyable
 * authoritative query-table FQN. Fetch-on-load + a manual Refresh button --
 * deliberately no polling/interval (per the plan, this is an on-demand view,
 * not a live-updating dashboard like StatusDashboard could become later).
 */
export default function PipelineMap() {
  const [pipelines, setPipelines] = useState<Pipeline[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [dlqCounts, setDlqCounts] = useState<Record<string, number | null>>({});

  function load() {
    let cancelled = false;
    getPipelines()
      .then((d) => {
        if (!cancelled) {
          setError(null);
          setPipelines(d.pipelines);
          setDlqCounts({});
          for (const p of d.pipelines) {
            const dlqName = dlqCarryingConnectorName(p);
            if (!dlqName) continue;
            getConnectorDlq(dlqName, 0)
              .then((res) => {
                if (!cancelled) {
                  setDlqCounts((prev) => ({ ...prev, [p.name]: res.count ?? null }));
                }
              })
              .catch(() => {});
          }
        }
      })
      .catch((err: unknown) => {
        if (!cancelled) setError(errorMessage(err));
      });
    return () => {
      cancelled = true;
    };
  }

  useEffect(() => {
    return load();
  }, []);

  return (
    <div>
      <h2>Pipeline Map</h2>
      <button type="button" onClick={load}>
        Refresh
      </button>

      {error && <p role="alert">Failed to load pipelines: {error}</p>}

      {pipelines === null ? (
        <p>Loading pipelines…</p>
      ) : pipelines.length === 0 ? (
        <p>No pipelines yet.</p>
      ) : (
        <ul>
          {pipelines.map((p) => (
            <PipelineRow key={p.name} pipeline={p} dlqCount={dlqCounts[p.name]} />
          ))}
        </ul>
      )}

      <div>
        <h3>Legend</h3>
        <ul>
          <li>
            <span style={{ color: "green" }}>●</span> RUNNING
          </li>
          <li>
            <span style={{ color: "#b8860b" }}>⏸</span> PAUSED
          </li>
          <li>
            <span style={{ color: "red" }}>✖</span> FAILED
          </li>
          <li>
            <span style={{ color: "gray" }}>○</span> unknown
          </li>
        </ul>
        <p>
          <span aria-hidden="true">⭐</span> marks the authoritative table to query for that
          pipeline (Silver if merged, otherwise Bronze/rawlake).
        </p>
      </div>
    </div>
  );
}
