import { useState } from "react";
import { deleteSource, type DeleteMode, type DeleteSourceResult } from "../api/client";

/**
 * Roles mirror `app.services.authz.Role` (backend): ADMIN/ANALYST/STUDENT.
 * Only ADMIN may perform `SOURCE_DELETE_WITH_DATA` (see authz.py's
 * `_MATRIX` -- ANALYST has every source action except that one, STUDENT is
 * READ-only), so this is the single source of truth the with-data radio's
 * visibility is gated on.
 */
export type Role = "ADMIN" | "ANALYST" | "STUDENT";

export interface DeleteModalProps {
  sourceName: string;
  role: Role;
  onCancel: () => void;
  onDeleted: (result: DeleteSourceResult) => void;
}

function errorMessage(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}

/**
 * Data-safety delete confirmation, per docs/superpowers/sdd/task-13-brief.md:
 *   - default mode is "pipeline_only" (destroys nothing but the connector).
 *   - "with_data" (drops the Iceberg table / empties the bucket / deletes
 *     the topic -- see DeleteSourceResult) is only offered to ADMIN, and
 *     even then the confirm button stays disabled until the operator types
 *     the exact source name, so a stray click can't nuke data.
 */
export default function DeleteModal({
  sourceName,
  role,
  onCancel,
  onDeleted,
}: DeleteModalProps) {
  const [mode, setMode] = useState<DeleteMode>("pipeline_only");
  const [confirmText, setConfirmText] = useState("");
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const isAdmin = role === "ADMIN";
  const withDataChosen = isAdmin && mode === "with_data";
  const nameConfirmed = confirmText === sourceName;
  const confirmDisabled = submitting || (withDataChosen && !nameConfirmed);

  async function handleConfirm() {
    setError(null);
    setSubmitting(true);
    try {
      const result = await deleteSource(sourceName, mode);
      onDeleted(result);
    } catch (err) {
      setError(errorMessage(err));
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <div role="dialog" aria-label={`Delete ${sourceName}`}>
      <h3>Delete “{sourceName}”</h3>
      <fieldset>
        <legend>Delete mode</legend>
        <label>
          <input
            type="radio"
            name="delete-mode"
            value="pipeline_only"
            checked={mode === "pipeline_only"}
            onChange={() => setMode("pipeline_only")}
          />
          Yalnızca pipeline
        </label>
        {isAdmin && (
          <label>
            <input
              type="radio"
              name="delete-mode"
              value="with_data"
              checked={mode === "with_data"}
              onChange={() => setMode("with_data")}
            />
            Veriyle birlikte
          </label>
        )}
      </fieldset>

      {withDataChosen && (
        <div>
          <label htmlFor="delete-confirm-name">
            Type “{sourceName}” to confirm permanent data deletion
          </label>
          <input
            id="delete-confirm-name"
            value={confirmText}
            onChange={(e) => setConfirmText(e.target.value)}
          />
        </div>
      )}

      {error && <p role="alert">Delete failed: {error}</p>}

      <button type="button" onClick={onCancel} disabled={submitting}>
        Cancel
      </button>
      <button type="button" onClick={handleConfirm} disabled={confirmDisabled}>
        {submitting ? "Deleting…" : "Delete"}
      </button>
    </div>
  );
}
