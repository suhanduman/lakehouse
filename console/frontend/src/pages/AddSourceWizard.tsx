import { useEffect, useState } from "react";
import {
  createSource,
  getSourceTypes,
  previewSource,
  type CreateSourceResult,
  type SourceCredentials,
  type SourceSpec,
  type SourceTypeDescriptor,
} from "../api/client";

/**
 * Multi-step "add source" form, per docs/superpowers/sdd/task-12-brief.md:
 *   1. tip seç -- registry-driven (Plan B1 Task 6): fetched from
 *      `GET /api/sources/types` (app/source_types.py) rather than a
 *      hard-coded cdc/scheduled x mssql/pg/mongo list, so a new descriptor
 *      registered on the backend (e.g. stream/kafka) shows up here with no
 *      frontend edit.
 *   2. bağlantı + credential
 *   3. tablo/kolon + delta/cdc opsiyon
 *   4. hedef ns/table
 *   5. önizleme -- POST /api/sources/preview (render only, no apply)
 *   6. uygula -- POST /api/sources (createSource)
 *
 * Field visibility per step 2/3 mirrors app.models.SourceSpec's own
 * kind/type-conditional requirements (see `_validate_kind_type_requirements`
 * in console/backend/app/models.py) -- driven here by the *selected
 * descriptor's* `required_fields` (from the registry) rather than
 * hard-coded (kind, type) conditionals, plus a small built-in map from
 * field name -> {step, label} for the fields the form knows how to render.
 * The disposition selector (shown when the descriptor allows more than one
 * disposition) and the `kafka_bootstrap` field (shown when the descriptor
 * is `needs_bootstrap`) are likewise descriptor-driven so the assembled
 * spec is accepted by the backend without a second round-trip of 422s.
 */

type Kind = SourceSpec["kind"];
type Type = SourceSpec["type"];
type Disposition = "" | NonNullable<SourceSpec["disposition"]>;

/** Field name (app.models.SourceSpec attribute) -> where/how the wizard
 * renders an input for it, keyed by the exact strings that show up in a
 * descriptor's `required_fields`. A required field name the registry
 * introduces that isn't in this map still renders -- as a generic input,
 * via `unknownRequiredFields`/`extraFields` below -- rather than silently
 * vanishing. */
const FIELD_META: Record<string, { step: 2 | 3; label: string }> = {
  db_host: { step: 2, label: "Database host" },
  mongo_uri: { step: 2, label: "Mongo URI" },
  jdbc_url: { step: 2, label: "JDBC URL" },
  incrementing_col: { step: 3, label: "Incrementing column" },
  cron: { step: 3, label: "Cron schedule" },
  s3_bucket: { step: 2, label: "S3 bucket" },
  s3_prefix: { step: 2, label: "S3 prefix" },
};

/** Options for the `file_format` select (batch/s3's fourth required
 * field) -- mirrors app.models.SourceSpec.file_format's literal union. */
const FILE_FORMAT_OPTIONS = ["parquet", "json", "avro"] as const;

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
  s3_bucket: string;
  s3_prefix: string;
  file_format: string;
  target_ns: string;
  target_table: string;
  user: string;
  password: string;
  disposition: Disposition;
  kafka_bootstrap: string;
  // Values for any required field the registry lists that FIELD_META
  // doesn't know a dedicated input for -- keeps a brand-new source type
  // fully usable (not just visible) without a frontend edit.
  extraFields: Record<string, string>;
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
  s3_bucket: "",
  s3_prefix: "",
  file_format: "",
  target_ns: "",
  target_table: "",
  user: "",
  password: "",
  disposition: "",
  kafka_bootstrap: "",
  extraFields: {},
};

/** Field names buildSpec/the form render a dedicated, labeled input for.
 * Anything in a descriptor's required_fields outside this set falls back
 * to a generic input (see `extraFields`) instead of silently not
 * rendering. */
const KNOWN_SPEC_FIELDS = new Set([
  "db_host",
  "mongo_uri",
  "jdbc_url",
  "incrementing_col",
  "cron",
  "s3_bucket",
  "s3_prefix",
  "file_format",
]);

/** "some_field" -> "Some field", for the generic-fallback input's label. */
function humanizeFieldName(field: string): string {
  const spaced = field.replace(/_/g, " ");
  return spaced.charAt(0).toUpperCase() + spaced.slice(1);
}

const STEP_TITLES = [
  "1. Source type",
  "2. Connection & credentials",
  "3. Table & options",
  "4. Target",
  "5. Preview",
  "6. Submit",
];

const LAST_STEP = STEP_TITLES.length;

/** form -> app.models.SourceSpec, including only the fields the *selected
 * descriptor* actually requires (mirrors render_service's own dispatch on
 * (kind, type), but reads the requirement from the registry descriptor
 * instead of re-deriving it from a hard-coded (kind, type) conditional).
 * `disposition`/`kafka_bootstrap` -- like the optional `timestamp_col`/
 * `poll_ms` companions -- are included only when the user actually set
 * them, so an unset override doesn't get sent and clobber the backend's
 * own default (see SourceSpec.effective_disposition()). */
function buildSpec(form: FormState, descriptor: SourceTypeDescriptor | undefined): SourceSpec {
  const spec: SourceSpec = {
    source: form.source,
    kind: form.kind,
    type: form.type,
    db: form.db,
    table: form.table,
    target_ns: form.target_ns,
    target_table: form.target_table,
  };
  const required = descriptor?.required_fields ?? [];
  if (required.includes("db_host")) {
    spec.db_host = form.db_host;
  }
  if (required.includes("mongo_uri")) {
    spec.mongo_uri = form.mongo_uri;
  }
  if (required.includes("jdbc_url")) {
    spec.jdbc_url = form.jdbc_url;
  }
  if (required.includes("incrementing_col")) {
    spec.incrementing_col = form.incrementing_col;
    if (form.timestamp_col) {
      spec.timestamp_col = form.timestamp_col;
    }
    if (form.poll_ms) {
      spec.poll_ms = Number(form.poll_ms);
    }
  }
  if (required.includes("cron")) {
    spec.cron = form.cron;
  }
  if (required.includes("s3_bucket")) {
    spec.s3_bucket = form.s3_bucket;
  }
  if (required.includes("s3_prefix")) {
    spec.s3_prefix = form.s3_prefix;
  }
  if (required.includes("file_format")) {
    spec.file_format = form.file_format as SourceSpec["file_format"];
  }
  for (const field of required) {
    if (!KNOWN_SPEC_FIELDS.has(field) && form.extraFields[field]) {
      (spec as unknown as Record<string, unknown>)[field] = form.extraFields[field];
    }
  }
  if (form.disposition) {
    spec.disposition = form.disposition;
  }
  if (form.kafka_bootstrap) {
    spec.kafka_bootstrap = form.kafka_bootstrap;
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

  const [types, setTypes] = useState<SourceTypeDescriptor[]>([]);
  const [typesError, setTypesError] = useState<string | null>(null);

  const [preview, setPreview] = useState<any>(null);
  const [previewError, setPreviewError] = useState<string | null>(null);
  const [previewLoading, setPreviewLoading] = useState(false);

  const [submitResult, setSubmitResult] = useState<CreateSourceResult | null>(null);
  const [submitError, setSubmitError] = useState<string | null>(null);
  const [submitLoading, setSubmitLoading] = useState(false);

  // Fetch the source-type registry once on mount -- the whole point of
  // this task is that Step 1's options, and steps 2/3's field visibility,
  // come from here rather than a hard-coded list.
  useEffect(() => {
    let cancelled = false;
    getSourceTypes()
      .then((descriptors) => {
        if (!cancelled) {
          setTypes(descriptors);
        }
      })
      .catch((err) => {
        if (!cancelled) {
          setTypesError(errorMessage(err));
        }
      });
    return () => {
      cancelled = true;
    };
  }, []);

  // Unique kinds, and the types available under the currently-selected
  // kind, in registry order (not alphabetized/hard-coded).
  const kinds: string[] = [];
  for (const t of types) {
    if (!kinds.includes(t.kind)) {
      kinds.push(t.kind);
    }
  }
  const typesForKind: string[] = [];
  for (const t of types) {
    if (t.kind === form.kind && !typesForKind.includes(t.type)) {
      typesForKind.push(t.type);
    }
  }
  const selectedDescriptor = types.find((t) => t.kind === form.kind && t.type === form.type);
  const requiredFields = selectedDescriptor?.required_fields ?? [];
  const unknownRequiredFields = requiredFields.filter((f) => !KNOWN_SPEC_FIELDS.has(f));
  const showDisposition = (selectedDescriptor?.dispositions.length ?? 0) > 1;
  const showKafkaBootstrap = selectedDescriptor?.needs_bootstrap ?? false;

  function set<K extends keyof FormState>(key: K, value: FormState[K]) {
    setForm((prev) => ({ ...prev, [key]: value }));
  }

  function setExtraField(field: string, value: string) {
    setForm((prev) => ({ ...prev, extraFields: { ...prev.extraFields, [field]: value } }));
  }

  // Switching kind/type can change which dispositions/fields are valid (or
  // make the disposition selector / kafka_bootstrap input disappear) --
  // reset any explicit override so a stale choice never rides along into
  // buildSpec() for the new type (e.g. a kafka_bootstrap value entered
  // under stream/kafka must not survive a switch back to cdc/mssql).
  function handleKindChange(newKind: string) {
    const firstType = types.find((t) => t.kind === newKind)?.type ?? form.type;
    setForm((prev) => ({
      ...prev,
      kind: newKind as Kind,
      type: firstType as Type,
      disposition: "",
      kafka_bootstrap: "",
    }));
  }

  function handleTypeChange(newType: string) {
    setForm((prev) => ({ ...prev, type: newType as Type, disposition: "", kafka_bootstrap: "" }));
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
      const result = await previewSource(buildSpec(form, selectedDescriptor));
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
      const result = await createSource(buildSpec(form, selectedDescriptor), buildCredentials(form));
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
          {typesError && <p role="alert">Failed to load source types: {typesError}</p>}
          <div>
            <label htmlFor="kind">Kind</label>
            <select
              id="kind"
              value={form.kind}
              onChange={(e) => handleKindChange(e.target.value)}
            >
              {kinds.map((k) => (
                <option key={k} value={k}>
                  {k}
                </option>
              ))}
            </select>
          </div>
          <div>
            <label htmlFor="type">Type</label>
            <select
              id="type"
              value={form.type}
              onChange={(e) => handleTypeChange(e.target.value)}
            >
              {typesForKind.map((t) => (
                <option key={t} value={t}>
                  {t}
                </option>
              ))}
            </select>
          </div>
          {showDisposition && (
            <div>
              <label htmlFor="disposition">Disposition</label>
              <select
                id="disposition"
                value={form.disposition}
                onChange={(e) => set("disposition", e.target.value as Disposition)}
              >
                <option value="">(default: {selectedDescriptor?.disposition})</option>
                {selectedDescriptor?.dispositions.map((d) => (
                  <option key={d} value={d}>
                    {d}
                  </option>
                ))}
              </select>
            </div>
          )}
          {showKafkaBootstrap && (
            <div>
              <label htmlFor="kafka_bootstrap">Kafka bootstrap (external, optional)</label>
              <input
                id="kafka_bootstrap"
                value={form.kafka_bootstrap}
                onChange={(e) => set("kafka_bootstrap", e.target.value)}
              />
            </div>
          )}
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
          {requiredFields.includes("db_host") && (
            <div>
              <label htmlFor="db_host">{FIELD_META.db_host.label}</label>
              <input
                id="db_host"
                value={form.db_host}
                onChange={(e) => set("db_host", e.target.value)}
              />
            </div>
          )}
          {requiredFields.includes("mongo_uri") && (
            <div>
              <label htmlFor="mongo_uri">{FIELD_META.mongo_uri.label}</label>
              <input
                id="mongo_uri"
                value={form.mongo_uri}
                onChange={(e) => set("mongo_uri", e.target.value)}
              />
            </div>
          )}
          {requiredFields.includes("jdbc_url") && (
            <div>
              <label htmlFor="jdbc_url">{FIELD_META.jdbc_url.label}</label>
              <input
                id="jdbc_url"
                value={form.jdbc_url}
                onChange={(e) => set("jdbc_url", e.target.value)}
              />
            </div>
          )}
          {requiredFields.includes("s3_bucket") && (
            <div>
              <label htmlFor="s3_bucket">{FIELD_META.s3_bucket.label}</label>
              <input
                id="s3_bucket"
                value={form.s3_bucket}
                onChange={(e) => set("s3_bucket", e.target.value)}
              />
            </div>
          )}
          {requiredFields.includes("s3_prefix") && (
            <div>
              <label htmlFor="s3_prefix">{FIELD_META.s3_prefix.label}</label>
              <input
                id="s3_prefix"
                value={form.s3_prefix}
                onChange={(e) => set("s3_prefix", e.target.value)}
              />
            </div>
          )}
          {requiredFields.includes("file_format") && (
            <div>
              <label htmlFor="file_format">File format</label>
              <select
                id="file_format"
                value={form.file_format}
                onChange={(e) => set("file_format", e.target.value)}
              >
                <option value="">(select a format)</option>
                {FILE_FORMAT_OPTIONS.map((fmt) => (
                  <option key={fmt} value={fmt}>
                    {fmt}
                  </option>
                ))}
              </select>
            </div>
          )}
          {unknownRequiredFields.map((field) => (
            <div key={field}>
              <label htmlFor={`extra-${field}`}>{humanizeFieldName(field)}</label>
              <input
                id={`extra-${field}`}
                value={form.extraFields[field] ?? ""}
                onChange={(e) => setExtraField(field, e.target.value)}
              />
            </div>
          ))}
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
          {requiredFields.includes("incrementing_col") && (
            <>
              <div>
                <label htmlFor="incrementing_col">{FIELD_META.incrementing_col.label}</label>
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
          {requiredFields.includes("cron") && (
            <div>
              <label htmlFor="cron">{FIELD_META.cron.label}</label>
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
