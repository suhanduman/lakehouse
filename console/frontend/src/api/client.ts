/**
 * Typed fetch client for the Lakehouse Console backend (`console/backend`).
 *
 * Response shapes are taken directly from the FastAPI routers, not guessed:
 *   - Source  <- app.routers.sources._summary()      (name/class/paused/state)
 *   - Table/Bucket/Schema/Status <- their respective routers' return shapes.
 *
 * Every function here is a real fetch wrapper (none throw-as-stub); the ones
 * beyond `listSources`/`getSource` are thin pass-throughs that later tasks
 * (12-15) will wire richer UI around, but they hit the real endpoint today.
 */

const BASE = "/api";

export type DeleteMode = "pipeline_only" | "with_data";

/** Mirrors app.routers.sources._summary(): the KafkaConnector CR projected
 * down to what the console UI needs. */
export interface Source {
  name: string;
  class: string;
  paused: boolean;
  state: string | null;
}

/** Mirrors app.models.SourceSpec. */
export interface SourceSpec {
  source: string;
  kind: "cdc" | "scheduled";
  type: "mssql" | "pg" | "mongo";
  db: string;
  table: string;
  target_ns: string;
  target_table: string;
  db_host?: string;
  jdbc_url?: string;
  mongo_uri?: string;
  incrementing_col?: string;
  timestamp_col?: string;
  poll_ms?: number;
  cron?: string;
}

/** Mirrors app.models.SourceCredentials. */
export interface SourceCredentials {
  user: string;
  password: string;
}

/** Mirrors app.orchestrator.StepResult. */
export interface SourceStepResult {
  name: string;
  ok: boolean;
  detail: string;
}

/** Mirrors app.orchestrator.AddSourceResult (asdict()'d by the
 * `create_source` router). `ok: false` is an IN-BAND pipeline failure --
 * the HTTP request itself succeeded (201 or 207, both res.ok under
 * fetch's 200-299 range), but the add-source pipeline rolled back or
 * refused (e.g. unsupported scheduled+mongo). Callers MUST check `ok`,
 * not just that the request didn't throw. */
export interface CreateSourceResult {
  steps: SourceStepResult[];
  ok: boolean;
}

export interface DeleteSourceResult {
  ok: boolean;
  name: string;
  mode: DeleteMode;
  dropped_table?: string;
  emptied_bucket?: string;
  deleted_topic?: string | null;
}

export interface TableNamespace {
  name: string;
  tables: string[];
}

export interface TablesResponse {
  catalog: string;
  namespaces: TableNamespace[];
}

export interface StatusEntry {
  name: string;
  state: string | null;
  tasks?: string[];
  maintenance: boolean;
  dlq: boolean | null;
  lag: number | null;
  reachable: boolean;
  error?: string;
}

export interface StatusResponse {
  connectors: StatusEntry[];
  reachable: boolean;
  error?: string;
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`${BASE}${path}`, {
    headers: { "Content-Type": "application/json" },
    ...init,
  });
  if (!res.ok) {
    const detail = await res.text().catch(() => "");
    throw new Error(`${init?.method ?? "GET"} ${path} failed: ${res.status} ${detail}`);
  }
  if (res.status === 204) {
    return undefined as T;
  }
  return (await res.json()) as T;
}

// --------------------------------------------------------------------------
// Sources
// --------------------------------------------------------------------------

/** GET /api/sources -> the list of registered sources. */
export async function listSources(): Promise<Source[]> {
  const data = await request<{ sources: Source[] }>("/sources");
  return data.sources;
}

/** GET /api/sources/{name} -> a single source's summary. */
export async function getSource(name: string): Promise<Source> {
  return request<Source>(`/sources/${encodeURIComponent(name)}`);
}

/** POST /api/sources -> register + provision a new source.
 *
 * Returns on both 201 (ok:true) and 207 Multi-Status (ok:false, an in-band
 * pipeline failure) -- `Response.ok` covers the whole 200-299 range, so
 * `request()` treats 207 as a normal success and returns its JSON body
 * rather than throwing. Callers MUST inspect `result.ok`/`result.steps`
 * themselves; a resolved promise here does NOT mean the source was
 * created. */
export async function createSource(
  spec: SourceSpec,
  credentials: SourceCredentials,
): Promise<CreateSourceResult> {
  return request<CreateSourceResult>("/sources", {
    method: "POST",
    body: JSON.stringify({ spec, credentials }),
  });
}

/** POST /api/sources/preview -> dry-run a spec without provisioning
 * anything (no such route exists in the backend yet as of Task 11; this
 * calls the endpoint later tasks are expected to add so callers can start
 * integrating against a stable client surface now). */
export async function previewSource(spec: SourceSpec): Promise<unknown> {
  return request<unknown>("/sources/preview", {
    method: "POST",
    body: JSON.stringify({ spec }),
  });
}

/** PATCH /api/sources/{name} -> edit connector config. */
export async function patchSource(
  name: string,
  config: Record<string, unknown>,
): Promise<{ ok: boolean; name: string }> {
  return request<{ ok: boolean; name: string }>(`/sources/${encodeURIComponent(name)}`, {
    method: "PATCH",
    body: JSON.stringify({ config }),
  });
}

/** POST /api/sources/{name}/pause */
export async function pauseSource(
  name: string,
): Promise<{ ok: boolean; name: string; paused: boolean }> {
  return request(`/sources/${encodeURIComponent(name)}/pause`, { method: "POST" });
}

/** POST /api/sources/{name}/resume */
export async function resumeSource(
  name: string,
): Promise<{ ok: boolean; name: string; paused: boolean }> {
  return request(`/sources/${encodeURIComponent(name)}/resume`, { method: "POST" });
}

/** DELETE /api/sources/{name}?mode=... */
export async function deleteSource(
  name: string,
  mode: DeleteMode = "pipeline_only",
): Promise<DeleteSourceResult> {
  return request<DeleteSourceResult>(
    `/sources/${encodeURIComponent(name)}?mode=${encodeURIComponent(mode)}`,
    { method: "DELETE" },
  );
}

// --------------------------------------------------------------------------
// Tables / Buckets / Schemas / Status
// --------------------------------------------------------------------------

/** GET /api/tables -> Trino/Iceberg namespaces + tables. */
export async function listTables(catalog = "lakehouse"): Promise<TablesResponse> {
  return request<TablesResponse>(`/tables?catalog=${encodeURIComponent(catalog)}`);
}

/** GET /api/buckets -> S3 bucket names. */
export async function listBuckets(): Promise<string[]> {
  const data = await request<{ buckets: string[] }>("/buckets");
  return data.buckets;
}

/** GET /api/schemas -> Apicurio registry schemas. */
export async function listSchemas(): Promise<Record<string, unknown>[]> {
  const data = await request<{ schemas: Record<string, unknown>[] }>("/schemas");
  return data.schemas;
}

/** GET /api/status -> unified connector health view. */
export async function getStatus(): Promise<StatusResponse> {
  return request<StatusResponse>("/status");
}
