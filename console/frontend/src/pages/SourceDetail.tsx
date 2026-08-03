import { useEffect, useState } from "react";
import { useNavigate, useParams } from "react-router-dom";
import DeleteModal, { type Role } from "../components/DeleteModal";
import IngestConfigPanel from "../components/IngestConfigPanel";
import {
  ApiError,
  editSparkSource,
  getConnectorDebug,
  getConnectorDlq,
  getIngestConfig,
  getSource,
  getSourceConnectors,
  getStatus,
  pauseSource,
  patchSource,
  restartConnector,
  resumeSource,
  type ConnectorDebug,
  type ConnectorRef,
  type DeleteSourceResult,
  type DlqResponse,
  type GitopsRemediation,
  type IngestConfig,
  type Source,
  type SourceSpec,
  type StatusEntry,
} from "../api/client";

export type { Role };

interface SourceDetailProps {
  /** Caller's role (resolved from the OIDC session upstream in App.tsx) --
   * gates the delete modal's "veriyle birlikte" (with-data) option per
   * app.services.authz's SOURCE_DELETE_WITH_DATA action. */
  role: Role;
}

function errorMessage(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}

/** Status dot for a connector row's `state` -- green when healthy, red for
 * FAILED (or anything else unrecognized, since an unknown state is worth
 * flagging rather than quietly treating as fine), grey while paused. */
function connectorStatusDot(state: string | null): string {
  if (state === "RUNNING") return "\u{1F7E2}"; // 🟢
  if (state === "PAUSED") return "\u{26AA}"; // ⚪
  return "\u{1F534}"; // 🔴 (FAILED / unknown)
}

/** Options for the spark edit form's `file_format` select -- mirrors
 * AddSourceWizard's FILE_FORMAT_OPTIONS / app.models.SourceSpec.file_format's
 * literal union, so the field can only ever hold one of these three values
 * (no free-text "csv"/"orc" reaching the PATCH body). */
const FILE_FORMAT_OPTIONS = ["parquet", "json", "avro"] as const;

/**
 * Source detail view, per docs/superpowers/sdd/task-13-brief.md: shows the
 * connector's config/status/lag/dlq (Source <- getSource, lag/dlq <-
 * getStatus's matching StatusEntry) with edit/pause/resume/delete actions.
 */
export default function SourceDetail({ role }: SourceDetailProps) {
  const { name = "" } = useParams<{ name: string }>();
  const navigate = useNavigate();

  const [source, setSource] = useState<Source | null>(null);
  const [statusEntry, setStatusEntry] = useState<StatusEntry | null>(null);
  const [loadError, setLoadError] = useState<string | null>(null);

  const [actionPending, setActionPending] = useState(false);
  const [actionError, setActionError] = useState<string | null>(null);

  const [editing, setEditing] = useState(false);
  const [configText, setConfigText] = useState("{}");
  const [editError, setEditError] = useState<string | null>(null);

  // Spark-batch edit form fields (source.cr_kind === "ScheduledSparkApplication"),
  // seeded from source.spark when the Edit button is clicked.
  const [cron, setCron] = useState("");
  const [s3Bucket, setS3Bucket] = useState("");
  const [s3Prefix, setS3Prefix] = useState("");
  const [fileFormat, setFileFormat] = useState<SourceSpec["file_format"]>("parquet");

  const [deleteModalOpen, setDeleteModalOpen] = useState(false);
  const [deleted, setDeleted] = useState<DeleteSourceResult | null>(null);

  const [connectors, setConnectors] = useState<ConnectorRef[]>([]);
  const [debug, setDebug] = useState<ConnectorDebug | null>(null);

  // Dead-letter records section (Task 5): a single active-DLQ state per the
  // existing `debug`/`handleDebug` pattern above -- only one connector's DLQ
  // view is shown at a time, lazy-fetched on click, never polled.
  // `dlqConnName` (unlike ConnectorDebug, DlqResponse carries no `name`
  // field) labels which connector the currently-shown `dlq` belongs to.
  const [dlq, setDlq] = useState<DlqResponse | null>(null);
  const [dlqConnName, setDlqConnName] = useState<string | null>(null);
  const [restartNotice, setRestartNotice] = useState<string | null>(null);
  const [remediation, setRemediation] = useState<GitopsRemediation | null>(null);
  const [copied, setCopied] = useState(false);

  // Ingestion/collector config section (Task 9): lazy-loaded on first open,
  // never polled. `ingestUnavailable` is set when the backend 400s (the
  // reliable "not a kafka-ingest source" signal per get_ingest_config --
  // cr_kind alone can't distinguish a kafka-ingest KafkaConnector from any
  // other), and is distinct from ingestError (an actual failure, which must
  // not block the rest of the page).
  const [ingestOpen, setIngestOpen] = useState(false);
  const [ingestConfig, setIngestConfig] = useState<IngestConfig | null>(null);
  const [ingestUnavailable, setIngestUnavailable] = useState(false);
  const [ingestError, setIngestError] = useState<string | null>(null);
  const [ingestLoading, setIngestLoading] = useState(false);

  useEffect(() => {
    let cancelled = false;
    getSource(name)
      .then((src) => {
        if (cancelled) return;
        setSource(src);
        getSourceConnectors(name)
          .then((r) => {
            if (!cancelled) setConnectors(r.connectors);
          })
          .catch(() => {
            // Connector list is a nice-to-have enrichment (restart/debug
            // controls); a failure here shouldn't block showing the source.
          });
        return getStatus()
          .then((statusResp) => {
            if (cancelled) return;
            setStatusEntry(statusResp.connectors.find((c) => c.name === name) ?? null);
          })
          .catch(() => {
            // Status is a nice-to-have enrichment; a failure here shouldn't
            // block showing the source itself.
          });
      })
      .catch((err: unknown) => {
        if (!cancelled) setLoadError(errorMessage(err));
      });
    return () => {
      cancelled = true;
    };
  }, [name]);

  /** Shared by handlePause/handleResume: a gitops-mode source can't be
   * paused/resumed directly (the Console isn't the source of truth -- a
   * git repo reconciled by ArgoCD is), so that request 409s with a
   * `{detail: {message, remediation}}` body instead of applying the change.
   * Surface that as an actionable "edit this file" recipe instead of the
   * generic error message. */
  function handleGitopsBlockedError(err: unknown): void {
    if (err instanceof ApiError && err.status === 409) {
      try {
        const rem = JSON.parse(err.body)?.detail?.remediation as GitopsRemediation | undefined;
        if (rem) {
          setRemediation(rem);
          return;
        }
      } catch {
        // body wasn't the expected JSON shape -- fall through to the
        // generic error message below.
      }
    }
    setActionError(errorMessage(err));
  }

  async function handlePause() {
    setActionError(null);
    setRemediation(null);
    setActionPending(true);
    try {
      const result = await pauseSource(name);
      setSource((prev) => (prev ? { ...prev, paused: result.paused } : prev));
    } catch (err) {
      handleGitopsBlockedError(err);
    } finally {
      setActionPending(false);
    }
  }

  async function handleResume() {
    setActionError(null);
    setRemediation(null);
    setActionPending(true);
    try {
      const result = await resumeSource(name);
      setSource((prev) => (prev ? { ...prev, paused: result.paused } : prev));
    } catch (err) {
      handleGitopsBlockedError(err);
    } finally {
      setActionPending(false);
    }
  }

  /** Returns true on success, false on failure (having already set
   * actionError) -- the return value is additive: every existing
   * single-connector call site (the per-row Restart/Only-failed buttons)
   * ignores it, so their behavior is unchanged. handleRestartPipeline below
   * uses it to stop the loop and skip the combined confirmation as soon as
   * one connector fails. */
  async function handleRestart(connName: string, onlyFailed = false): Promise<boolean> {
    setActionError(null);
    setRestartNotice(null);
    setActionPending(true);
    try {
      await restartConnector(connName, { only_failed: onlyFailed });
      setRestartNotice(`Restart triggered: ${connName}`);
      return true;
    } catch (err) {
      setActionError(
        err instanceof ApiError && err.status === 409
          ? "Rebalance in progress — try again shortly."
          : errorMessage(err),
      );
      return false;
    } finally {
      setActionPending(false);
    }
  }

  /** Restarts every connector in the pipeline sequentially, then shows ONE
   * combined confirmation naming all of them -- calling handleRestart in a
   * loop and leaving its per-call `restartNotice` in place would have each
   * call overwrite the last, so only the final connector's "Restart
   * triggered" line would ever be visible. If any connector fails (e.g.
   * 409 rebalance-in-progress), handleRestart has already set actionError
   * (surfaced the same way a single-connector restart would); the loop
   * stops there rather than piling on a misleading combined confirmation
   * for connectors that were never reached. */
  async function handleRestartPipeline() {
    const restarted: string[] = [];
    for (const c of connectors) {
      // eslint-disable-next-line no-await-in-loop -- connectors within one
      // pipeline must restart sequentially, not all at once.
      const ok = await handleRestart(c.name);
      if (!ok) return;
      restarted.push(c.name);
    }
    setRestartNotice(`Restart triggered: ${restarted.join(", ")}`);
  }

  async function handleDebug(connName: string) {
    setActionError(null);
    try {
      setDebug(await getConnectorDebug(connName));
    } catch (err) {
      setActionError(errorMessage(err));
    }
  }

  async function handleDlq(connName: string) {
    setActionError(null);
    try {
      const result = await getConnectorDlq(connName);
      setDlqConnName(connName);
      setDlq(result);
    } catch (err) {
      setActionError(errorMessage(err));
    }
  }

  async function handleSaveEdit() {
    setEditError(null);
    let config: Record<string, unknown>;
    try {
      config = JSON.parse(configText) as Record<string, unknown>;
    } catch {
      setEditError("Invalid JSON");
      return;
    }
    setActionPending(true);
    try {
      await patchSource(name, config);
      setEditing(false);
    } catch (err) {
      setEditError(errorMessage(err));
    } finally {
      setActionPending(false);
    }
  }

  async function handleSaveSparkEdit() {
    if (!source?.spark) return;
    setEditError(null);
    setActionPending(true);
    try {
      const spec: SourceSpec = {
        source: source.spark.source,
        kind: "batch",
        type: "s3",
        db: "-",
        table: "-",
        target_ns: source.spark.target_ns,
        target_table: source.spark.target_table,
        s3_bucket: s3Bucket,
        s3_prefix: s3Prefix,
        file_format: fileFormat,
        cron,
      };
      await editSparkSource(name, spec);
      setEditing(false);
    } catch (err) {
      setEditError(errorMessage(err));
    } finally {
      setActionPending(false);
    }
  }

  function handleToggleEdit() {
    if (!editing && source?.cr_kind === "ScheduledSparkApplication" && source.spark) {
      setCron(source.spark.cron);
      setS3Bucket(source.spark.s3_bucket);
      setS3Prefix(source.spark.s3_prefix);
      // source.spark.file_format is the round-tripped backend annotation
      // value (typed as plain `string` on Source since it mirrors the raw
      // CR annotation) -- narrowing here is safe because it was only ever
      // written by a prior spec that already passed through this same
      // constrained select.
      setFileFormat(source.spark.file_format as SourceSpec["file_format"]);
    }
    setEditing((v) => !v);
  }

  /** Toggles the ingestion/collector config section; fetches lazily on the
   * first open only (subsequent toggles just show/hide what's already
   * loaded -- no re-fetch, no polling). */
  function handleToggleIngest() {
    const opening = !ingestOpen;
    setIngestOpen(opening);
    if (opening && ingestConfig === null && !ingestUnavailable && !ingestLoading) {
      setIngestLoading(true);
      setIngestError(null);
      getIngestConfig(name)
        .then((cfg) => setIngestConfig(cfg))
        .catch((err: unknown) => {
          if (err instanceof ApiError && err.status === 400) {
            setIngestUnavailable(true);
          } else {
            setIngestError(errorMessage(err));
          }
        })
        .finally(() => setIngestLoading(false));
    }
  }

  function handleDeleted(result: DeleteSourceResult) {
    setDeleteModalOpen(false);
    setDeleted(result);
    navigate("/");
  }

  if (loadError) {
    return <p role="alert">Failed to load source: {loadError}</p>;
  }

  if (!source) {
    return <p>Loading source…</p>;
  }

  return (
    <div>
      <h2>{source.name}</h2>
      <dl>
        <dt>Class</dt>
        <dd>{source.class}</dd>
        <dt>Status</dt>
        <dd>{source.paused ? "PAUSED" : source.state ?? "UNKNOWN"}</dd>
        <dt>Lag</dt>
        <dd>{statusEntry?.lag ?? "—"}</dd>
        <dt>DLQ</dt>
        <dd>{statusEntry?.dlq ? "yes" : "no"}</dd>
      </dl>

      {deleted && <p>Source “{deleted.name}” deleted ({deleted.mode}).</p>}

      {actionError && <p role="alert">{actionError}</p>}

      <button type="button" onClick={handleToggleEdit} disabled={actionPending}>
        Edit
      </button>
      {source.paused ? (
        <button type="button" onClick={handleResume} disabled={actionPending}>
          Resume
        </button>
      ) : (
        <button type="button" onClick={handlePause} disabled={actionPending}>
          Pause
        </button>
      )}
      <button type="button" onClick={() => setDeleteModalOpen(true)} disabled={actionPending}>
        Delete source
      </button>

      {restartNotice && <p role="status">{restartNotice}</p>}

      {connectors.length > 0 && (
        <section aria-label="connectors">
          <h3>Connectors</h3>
          {connectors.length > 1 && (
            <button type="button" onClick={handleRestartPipeline} disabled={actionPending}>
              Restart pipeline
            </button>
          )}
          <ul>
            {connectors.map((c) => (
              <li key={c.name}>
                <span aria-hidden="true">{connectorStatusDot(c.state)}</span> {c.role}: {c.name}{" "}
                ({c.state ?? "unknown"})
                <button
                  type="button"
                  onClick={() => handleRestart(c.name)}
                  disabled={actionPending}
                >
                  Restart
                </button>
                <button
                  type="button"
                  onClick={() => handleRestart(c.name, true)}
                  disabled={actionPending}
                >
                  Only failed tasks
                </button>
                <button
                  type="button"
                  className={c.state === "FAILED" ? "danger" : undefined}
                  onClick={() => handleDebug(c.name)}
                  disabled={actionPending}
                >
                  Debug
                </button>
                <button
                  type="button"
                  onClick={() => handleDlq(c.name)}
                  disabled={actionPending}
                >
                  Dead-letter records
                </button>
              </li>
            ))}
          </ul>
        </section>
      )}

      {debug && (
        <section aria-label="debug">
          <h3>
            Debug: {debug.name} ({debug.state})
          </h3>
          <table>
            <thead>
              <tr>
                <th>task</th>
                <th>state</th>
                <th>worker</th>
              </tr>
            </thead>
            <tbody>
              {debug.tasks.map((t) => (
                <tr key={t.id ?? "?"}>
                  <td>{t.id}</td>
                  <td>{t.state}</td>
                  <td>{t.worker_id}</td>
                </tr>
              ))}
            </tbody>
          </table>
          {debug.tasks
            .filter((t) => t.trace)
            .map((t) => (
              <details key={`tr-${t.id}`} open={t.state === "FAILED"}>
                <summary>trace (task {t.id})</summary>
                <pre>{t.trace}</pre>
              </details>
            ))}
          <h4>How to inspect deeper</h4>
          <p>Search terms: {debug.logs_hint.search_terms.join(", ")}</p>
          <code>{debug.logs_hint.oc_command}</code>
          <button
            type="button"
            onClick={() => {
              navigator.clipboard.writeText(debug.logs_hint.oc_command);
              setCopied(true);
            }}
          >
            Copy
          </button>
          {copied && <span> Copied</span>}
          {debug.logs_hint.external_link && (
            <p>
              <a href={debug.logs_hint.external_link} target="_blank" rel="noreferrer">
                Open in logging ↗
              </a>
            </p>
          )}
        </section>
      )}

      {dlq && (
        <section aria-label="dlq">
          <h3>Dead-letter records{dlqConnName ? `: ${dlqConnName}` : ""}</h3>
          {!dlq.has_dlq && <p>{dlq.hint}</p>}
          {dlq.has_dlq && dlq.count === null && <p>Kafka not reachable — try again</p>}
          {dlq.has_dlq && dlq.count === 0 && (
            <p>✓ No dropped records — this pipeline is clean</p>
          )}
          {dlq.has_dlq && typeof dlq.count === "number" && dlq.count > 0 && (
            <>
              {dlq.returned !== undefined && dlq.returned < dlq.count && (
                <p>
                  showing last {dlq.returned} of {dlq.count}
                </p>
              )}
              <table>
                <thead>
                  <tr>
                    <th>time</th>
                    <th>error</th>
                    <th>source</th>
                  </tr>
                </thead>
                <tbody>
                  {(dlq.records ?? []).map((r, i) => (
                    <tr key={i}>
                      <td>{r.ts ? new Date(r.ts).toISOString() : "—"}</td>
                      <td>
                        <details>
                          <summary>
                            {r.error_class}: {r.error_message}
                          </summary>
                          <figure>
                            <pre>{r.value_preview}</pre>
                            <figcaption>sample — may contain data</figcaption>
                          </figure>
                        </details>
                      </td>
                      <td>
                        {r.source_topic}:{r.source_partition}@{r.source_offset}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </>
          )}
        </section>
      )}

      {remediation && (
        <section role="alert" aria-label="remediation">
          <h3>Do this in the pipeline repo / ArgoCD</h3>
          <p>
            {remediation.repo} — {remediation.path}
          </p>
          <p>
            Set <code>{remediation.field}</code> = <code>{String(remediation.value)}</code>
          </p>
          <ol>
            {remediation.steps.map((s, i) => (
              <li key={i}>{s}</li>
            ))}
          </ol>
        </section>
      )}

      {editing && source.cr_kind === "ScheduledSparkApplication" ? (
        <div>
          <label htmlFor="spark-cron">Cron</label>
          <input id="spark-cron" value={cron} onChange={(e) => setCron(e.target.value)} />

          <label htmlFor="spark-s3-bucket">S3 Bucket</label>
          <input
            id="spark-s3-bucket"
            value={s3Bucket}
            onChange={(e) => setS3Bucket(e.target.value)}
          />

          <label htmlFor="spark-s3-prefix">S3 Prefix</label>
          <input
            id="spark-s3-prefix"
            value={s3Prefix}
            onChange={(e) => setS3Prefix(e.target.value)}
          />

          <label htmlFor="spark-file-format">File Format</label>
          <select
            id="spark-file-format"
            value={fileFormat}
            onChange={(e) => setFileFormat(e.target.value as SourceSpec["file_format"])}
          >
            {FILE_FORMAT_OPTIONS.map((fmt) => (
              <option key={fmt} value={fmt}>
                {fmt}
              </option>
            ))}
          </select>

          {editError && <p role="alert">{editError}</p>}
          <button type="button" onClick={handleSaveSparkEdit} disabled={actionPending}>
            Save
          </button>
          <button type="button" onClick={() => setEditing(false)} disabled={actionPending}>
            Cancel
          </button>
        </div>
      ) : (
        editing && (
          <div>
            <label htmlFor="config-json">Config (JSON)</label>
            <textarea
              id="config-json"
              value={configText}
              onChange={(e) => setConfigText(e.target.value)}
            />
            {editError && <p role="alert">{editError}</p>}
            <button type="button" onClick={handleSaveEdit} disabled={actionPending}>
              Save
            </button>
            <button type="button" onClick={() => setEditing(false)} disabled={actionPending}>
              Cancel
            </button>
          </div>
        )
      )}

      <section aria-label="ingest-config">
        <button type="button" onClick={handleToggleIngest}>
          {ingestOpen ? "Hide" : "Show"} ingestion / collector config
        </button>
        {ingestOpen && (
          <>
            {ingestLoading && <p>Loading…</p>}
            {ingestUnavailable && <p>Only available for kafka-ingest sources</p>}
            {ingestError && <p role="alert">{ingestError}</p>}
            {ingestConfig && <IngestConfigPanel config={ingestConfig} />}
          </>
        )}
      </section>

      {deleteModalOpen && (
        <DeleteModal
          sourceName={name}
          role={role}
          onCancel={() => setDeleteModalOpen(false)}
          onDeleted={handleDeleted}
        />
      )}
    </div>
  );
}
