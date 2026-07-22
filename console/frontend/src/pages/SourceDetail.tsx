import { useEffect, useState } from "react";
import { useNavigate, useParams } from "react-router-dom";
import DeleteModal, { type Role } from "../components/DeleteModal";
import {
  getSource,
  getStatus,
  pauseSource,
  patchSource,
  resumeSource,
  type DeleteSourceResult,
  type Source,
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

  const [deleteModalOpen, setDeleteModalOpen] = useState(false);
  const [deleted, setDeleted] = useState<DeleteSourceResult | null>(null);

  useEffect(() => {
    let cancelled = false;
    getSource(name)
      .then((src) => {
        if (cancelled) return;
        setSource(src);
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

  async function handlePause() {
    setActionError(null);
    setActionPending(true);
    try {
      const result = await pauseSource(name);
      setSource((prev) => (prev ? { ...prev, paused: result.paused } : prev));
    } catch (err) {
      setActionError(errorMessage(err));
    } finally {
      setActionPending(false);
    }
  }

  async function handleResume() {
    setActionError(null);
    setActionPending(true);
    try {
      const result = await resumeSource(name);
      setSource((prev) => (prev ? { ...prev, paused: result.paused } : prev));
    } catch (err) {
      setActionError(errorMessage(err));
    } finally {
      setActionPending(false);
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

      <button type="button" onClick={() => setEditing((v) => !v)} disabled={actionPending}>
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

      {editing && (
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
      )}

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
