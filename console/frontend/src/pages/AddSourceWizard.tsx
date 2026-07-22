import { useState } from "react";
import {
  createSource,
  previewSource,
  type CreateSourceResult,
  type SourceCredentials,
  type SourceSpec,
} from "../api/client";

/**
 * Multi-step "add source" form, per docs/superpowers/sdd/task-12-brief.md:
 *   1. tip seç (cdc/scheduled x mssql/pg/mongo)
 *   2. bağlantı + credential
 *   3. tablo/kolon + delta/cdc opsiyon
 *   4. hedef ns/table
 *   5. önizleme -- POST /api/sources/preview (render only, no apply)
 *   6. uygula -- POST /api/sources (createSource)
 *
 * Field visibility per step 2/3 mirrors app.models.SourceSpec's own
 * kind/type-conditional requirements (see `_validate_kind_type_requirements`
 * in console/backend/app/models.py) so the assembled spec is accepted by the
 * backend without a second round-trip of 422s.
 */

type Kind = SourceSpec["kind"];
type Type = SourceSpec["type"];

interface FormState {
  source: string;
  kind: Kind;
  type: Type;
  db_host: string;
  jdbc_url: string;
  mongo_uri: string;
  db: string;
  table: string;
  incrementing_col: string;
  timestamp_col: string;
  poll_ms: string;
  cron: string;
  target_ns: string;
  target_table: string;
  user: string;
  password: string;
}

const INITIAL_STATE: FormState = {
  source: "",
  kind: "cdc",
  type: "mssql",
  db_host: "",
  jdbc_url: "",
  mongo_uri: "",
  db: "",
  table: "",
  incrementing_col: "",
  timestamp_col: "",
  poll_ms: "",
  cron: "",
  target_ns: "",
  target_table: "",
  user: "",
  password: "",
};

const STEP_TITLES = [
  "1. Source type",
  "2. Connection & credentials",
  "3. Table & options",
  "4. Target",
  "5. Preview",
  "6. Submit",
];

const LAST_STEP = STEP_TITLES.length;

/** form -> app.models.SourceSpec, including only the kind/type-relevant
 * optional fields (mirrors render_service's own dispatch on (kind, type)). */
function buildSpec(form: FormState): SourceSpec {
  const spec: SourceSpec = {
    source: form.source,
    kind: form.kind,
    type: form.type,
    db: form.db,
    table: form.table,
    target_ns: form.target_ns,
    target_table: form.target_table,
  };
  if (form.kind === "cdc" && form.type !== "mongo") {
    spec.db_host = form.db_host;
  }
  if (form.kind === "cdc" && form.type === "mongo") {
    spec.mongo_uri = form.mongo_uri;
  }
  if (form.kind === "scheduled" && form.type !== "mongo") {
    spec.jdbc_url = form.jdbc_url;
    spec.incrementing_col = form.incrementing_col;
    if (form.timestamp_col) {
      spec.timestamp_col = form.timestamp_col;
    }
    if (form.poll_ms) {
      spec.poll_ms = Number(form.poll_ms);
    }
  }
  if (form.kind === "scheduled" && form.type === "mongo") {
    spec.cron = form.cron;
  }
  return spec;
}

function buildCredentials(form: FormState): SourceCredentials {
  return { user: form.user, password: form.password };
}

function errorMessage(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}

export default function AddSourceWizard() {
  const [step, setStep] = useState(1);
  const [form, setForm] = useState<FormState>(INITIAL_STATE);

  const [preview, setPreview] = useState<any>(null);
  const [previewError, setPreviewError] = useState<string | null>(null);
  const [previewLoading, setPreviewLoading] = useState(false);

  const [submitResult, setSubmitResult] = useState<CreateSourceResult | null>(null);
  const [submitError, setSubmitError] = useState<string | null>(null);
  const [submitLoading, setSubmitLoading] = useState(false);

  function set<K extends keyof FormState>(key: K, value: FormState[K]) {
    setForm((prev) => ({ ...prev, [key]: value }));
  }

  function goNext() {
    setStep((s) => Math.min(LAST_STEP, s + 1));
  }

  function goBack() {
    setStep((s) => Math.max(1, s - 1));
  }

  async function handlePreview() {
    setPreviewError(null);
    setPreviewLoading(true);
    try {
      const result = await previewSource(buildSpec(form));
      setPreview(result);
    } catch (err) {
      setPreview(null);
      setPreviewError(errorMessage(err));
    } finally {
      setPreviewLoading(false);
    }
  }

  async function handleSubmit() {
    setSubmitError(null);
    setSubmitResult(null);
    setSubmitLoading(true);
    try {
      // A resolved promise here only means the HTTP request succeeded
      // (201 or 207) -- it does NOT mean the source was created. The
      // add-source pipeline can fail in-band (rollback ran, or
      // unsupported scheduled+mongo) and still return a 2xx-range status
      // with `ok: false` in the body (see client.createSource's
      // docstring), so `result.ok` -- not just "the request didn't
      // throw" -- decides success vs. failure here.
      const result = await createSource(buildSpec(form), buildCredentials(form));
      setSubmitResult(result);
    } catch (err) {
      setSubmitResult(null);
      setSubmitError(errorMessage(err));
    } finally {
      setSubmitLoading(false);
    }
  }

  return (
    <div>
      <h2>Add source</h2>
      <p>{STEP_TITLES[step - 1]}</p>

      {step === 1 && (
        <fieldset>
          <div>
            <label htmlFor="kind">Kind</label>
            <select
              id="kind"
              value={form.kind}
              onChange={(e) => set("kind", e.target.value as Kind)}
            >
              <option value="cdc">cdc</option>
              <option value="scheduled">scheduled</option>
            </select>
          </div>
          <div>
            <label htmlFor="type">Type</label>
            <select
              id="type"
              value={form.type}
              onChange={(e) => set("type", e.target.value as Type)}
            >
              <option value="mssql">mssql</option>
              <option value="pg">pg</option>
              <option value="mongo">mongo</option>
            </select>
          </div>
          <button type="button" onClick={goNext}>
            Next
          </button>
        </fieldset>
      )}

      {step === 2 && (
        <fieldset>
          <div>
            <label htmlFor="source">Source name</label>
            <input
              id="source"
              value={form.source}
              onChange={(e) => set("source", e.target.value)}
            />
          </div>
          {form.kind === "cdc" && form.type !== "mongo" && (
            <div>
              <label htmlFor="db_host">Database host</label>
              <input
                id="db_host"
                value={form.db_host}
                onChange={(e) => set("db_host", e.target.value)}
              />
            </div>
          )}
          {form.kind === "cdc" && form.type === "mongo" && (
            <div>
              <label htmlFor="mongo_uri">Mongo URI</label>
              <input
                id="mongo_uri"
                value={form.mongo_uri}
                onChange={(e) => set("mongo_uri", e.target.value)}
              />
            </div>
          )}
          {form.kind === "scheduled" && form.type !== "mongo" && (
            <div>
              <label htmlFor="jdbc_url">JDBC URL</label>
              <input
                id="jdbc_url"
                value={form.jdbc_url}
                onChange={(e) => set("jdbc_url", e.target.value)}
              />
            </div>
          )}
          <div>
            <label htmlFor="user">Username</label>
            <input
              id="user"
              value={form.user}
              onChange={(e) => set("user", e.target.value)}
            />
          </div>
          <div>
            <label htmlFor="password">Password</label>
            <input
              id="password"
              type="password"
              value={form.password}
              onChange={(e) => set("password", e.target.value)}
            />
          </div>
          <button type="button" onClick={goBack}>
            Back
          </button>
          <button type="button" onClick={goNext}>
            Next
          </button>
        </fieldset>
      )}

      {step === 3 && (
        <fieldset>
          <div>
            <label htmlFor="db">Database</label>
            <input id="db" value={form.db} onChange={(e) => set("db", e.target.value)} />
          </div>
          <div>
            <label htmlFor="table">Table</label>
            <input
              id="table"
              value={form.table}
              onChange={(e) => set("table", e.target.value)}
            />
          </div>
          {form.kind === "scheduled" && form.type !== "mongo" && (
            <>
              <div>
                <label htmlFor="incrementing_col">Incrementing column</label>
                <input
                  id="incrementing_col"
                  value={form.incrementing_col}
                  onChange={(e) => set("incrementing_col", e.target.value)}
                />
              </div>
              <div>
                <label htmlFor="timestamp_col">Timestamp column (delta, optional)</label>
                <input
                  id="timestamp_col"
                  value={form.timestamp_col}
                  onChange={(e) => set("timestamp_col", e.target.value)}
                />
              </div>
              <div>
                <label htmlFor="poll_ms">Poll interval (ms, optional)</label>
                <input
                  id="poll_ms"
                  value={form.poll_ms}
                  onChange={(e) => set("poll_ms", e.target.value)}
                />
              </div>
            </>
          )}
          {form.kind === "scheduled" && form.type === "mongo" && (
            <div>
              <label htmlFor="cron">Cron schedule</label>
              <input
                id="cron"
                value={form.cron}
                onChange={(e) => set("cron", e.target.value)}
              />
            </div>
          )}
          <button type="button" onClick={goBack}>
            Back
          </button>
          <button type="button" onClick={goNext}>
            Next
          </button>
        </fieldset>
      )}

      {step === 4 && (
        <fieldset>
          <div>
            <label htmlFor="target_ns">Target namespace</label>
            <input
              id="target_ns"
              value={form.target_ns}
              onChange={(e) => set("target_ns", e.target.value)}
            />
          </div>
          <div>
            <label htmlFor="target_table">Target table</label>
            <input
              id="target_table"
              value={form.target_table}
              onChange={(e) => set("target_table", e.target.value)}
            />
          </div>
          <button type="button" onClick={goBack}>
            Back
          </button>
          <button type="button" onClick={goNext}>
            Next
          </button>
        </fieldset>
      )}

      {step === 5 && (
        <fieldset>
          <button type="button" onClick={handlePreview} disabled={previewLoading}>
            {previewLoading ? "Fetching preview…" : "Fetch preview"}
          </button>
          {previewError && <p role="alert">Preview failed: {previewError}</p>}
          {preview && (
            <div data-testid="preview-result">
              <p>
                Connector class:{" "}
                <strong>
                  {preview.connector?.spec?.class ?? "(none — Spark batch path)"}
                </strong>
              </p>
              <p>
                Kafka topic: <strong>{preview.kafka_topic?.metadata?.name ?? "(none)"}</strong>
              </p>
              <p>
                Bucket: <strong>{preview.bucket}</strong>
              </p>
              <pre>{preview.namespace_ddl}</pre>
            </div>
          )}
          <button type="button" onClick={goBack}>
            Back
          </button>
          <button type="button" onClick={goNext} disabled={!preview}>
            Next
          </button>
        </fieldset>
      )}

      {step === 6 && (
        <fieldset>
          <button type="button" onClick={handleSubmit} disabled={submitLoading}>
            {submitLoading ? "Creating…" : "Create source"}
          </button>
          {submitError && <p role="alert">Create failed: {submitError}</p>}
          {submitResult && submitResult.ok && <p>Source created.</p>}
          {submitResult && !submitResult.ok && (
            <div role="alert" data-testid="create-failed">
              <p>Source creation failed.</p>
              <ul>
                {submitResult.steps
                  .filter((s) => !s.ok)
                  .map((s) => (
                    <li key={s.name}>
                      {s.name}: {s.detail}
                    </li>
                  ))}
              </ul>
            </div>
          )}
          <button type="button" onClick={goBack}>
            Back
          </button>
        </fieldset>
      )}
    </div>
  );
}
