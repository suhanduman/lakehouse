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

/** Mirrors app.routers.sources._summary(): the KafkaConnector/
 * ScheduledSparkApplication CR projected down to what the console UI needs.
 * `cr_kind` distinguishes the two ("KafkaConnector" | "ScheduledSparkApplication");
 * `spark` carries the round-tripped spark-batch annotation fields (`null`
 * for connectors). */
export interface Source {
  name: string;
  class: string;
  paused: boolean;
  state: string | null;
  cr_kind: string;
  spark?: {
    source: string;
    target_ns: string;
    target_table: string;
    s3_bucket: string;
    s3_prefix: string;
    file_format: string;
    cron: string;
  } | null;
}

/** Mirrors app.models.SourceSpec. `kind`/`type` are widened to cover every
 * (kind, type) pair the backend registry (app/source_types.py) knows about
 * -- not just the three the wizard used to hard-code -- since the wizard
 * now renders its option list from `getSourceTypes()` rather than a
 * static union-typed select. */
export interface SourceSpec {
  source: string;
  kind: "cdc" | "scheduled" | "stream" | "batch";
  type: "mssql" | "pg" | "mongo" | "mysql" | "kafka" | "s3" | "http" | "mqtt" | "rabbitmq";
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
  s3_bucket?: string;
  s3_prefix?: string;
  file_format?: "parquet" | "json" | "avro";
  http_url?: string;
  mqtt_broker?: string;
  mqtt_topic?: string;
  rabbitmq_uri?: string;
  rabbitmq_queue?: string;
  disposition?: "entity" | "event";
  kafka_bootstrap?: string;
  columns?: { name: string; type: string }[];
  identifier?: string[];
  delete_field?: string;
}

/** Mirrors the dict shape `app.routers.sources.list_source_types()` returns
 * per entry (itself projected from `app.source_types.SourceType`): the
 * single source of truth for which (kind, type) pairs exist, what fields
 * each requires, which Bronze->Silver dispositions it allows, and whether
 * it needs an external Kafka bootstrap. The add-source wizard renders its
 * type list and per-type fields from this instead of a hard-coded list. */
export interface SourceTypeDescriptor {
  id: string;
  kind: string;
  type: string;
  lane: string;
  disposition: "entity" | "event";
  dispositions: ("entity" | "event")[];
  required_fields: string[];
  needs_bootstrap: boolean;
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

/** Mirrors app.routers.sources.preview_source()'s dict shape: a dry-run of
 * the CRs (and per-pipeline Bronze/Silver buckets + Silver namespace DDL,
 * Sub-project B-v2) an add-source call would produce, without touching a
 * cluster/bucket/warehouse. `connector`/`kafka_topic` are `null` for the
 * spark-batch lane (no KafkaConnector/KafkaTopic CR to render there);
 * otherwise each is a full K8s manifest (apiVersion/kind/metadata/spec) --
 * only the paths the wizard's preview step actually reads are typed here. */
export interface PreviewResult {
  bronze_bucket: string;
  silver_bucket: string;
  namespace_ddl: string;
  connector: { spec?: { class?: string } } | null;
  kafka_topic: { metadata?: { name?: string } } | null;
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

/** Mirrors app.routers.sources.delete_source()'s dict shape across both
 * lanes it can take: the spark-batch branch still returns the original
 * singular `dropped_table`/`emptied_bucket` (rawlake-only, no Silver
 * table/bucket to speak of), while the CDC/kafka/camel branch (Sub-project
 * B-v2: per-pipeline Bronze+Silver) returns the plural `dropped_tables`/
 * `deleted_buckets` (Bronze changelog + Silver merge target/bucket, the
 * latter a no-op pair for an event-lane source with no Silver side). `ref`
 * is the git commit ref returned by the gitops-mode branch
 * (`orchestrator.git_writer.remove_source`); absent in direct mode. */
export interface DeleteSourceResult {
  ok: boolean;
  name: string;
  mode: DeleteMode;
  dropped_table?: string;
  emptied_bucket?: string;
  dropped_tables?: string[];
  deleted_buckets?: string[];
  deleted_topic?: string | null;
  ref?: string;
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

export interface GitopsResource {
  kind: string;
  name: string;
  status: string | null;
  health: string | null;
}
export interface GitopsSource {
  source: string;
  sync: string;
  health: string;
  resources: GitopsResource[];
}
export interface GitopsStatusResponse {
  mode: "direct" | "gitops";
  application: { sync: string | null; health: string | null } | null;
  sources: GitopsSource[];
  outOfSync: GitopsResource[];
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

/** GET /api/sources/types -> the source-type registry (app.source_types),
 * for the add-source wizard to render its kind/type list, per-type
 * required fields, disposition options, and bootstrap requirement instead
 * of hard-coding them. */
export async function getSourceTypes(): Promise<SourceTypeDescriptor[]> {
  const data = await request<{ types: SourceTypeDescriptor[] }>("/sources/types");
  return data.types;
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
 * anything (render-only route added in app.routers.sources.preview_source;
 * it takes no K8s/S3/Trino dependency, so it can never reach a live
 * cluster/bucket/warehouse no matter what the caller sends). */
export async function previewSource(spec: SourceSpec): Promise<PreviewResult> {
  return request<PreviewResult>("/sources/preview", {
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

/** PATCH /api/sources/{name} with a full spec -> re-render a spark-batch source. */
export async function editSparkSource(
  name: string,
  spec: SourceSpec,
): Promise<{ ok: boolean; name: string }> {
  return request<{ ok: boolean; name: string }>(`/sources/${encodeURIComponent(name)}`, {
    method: "PATCH",
    body: JSON.stringify({ spec }),
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

/** GET /api/gitops/status -> per-source ArgoCD sync/health (gitops mode);
 * {mode:"direct"} when the Console is in direct-apply mode. */
export async function getGitopsStatus(): Promise<GitopsStatusResponse> {
  return request<GitopsStatusResponse>("/gitops/status");
}

// --------------------------------------------------------------------------
// Pipelines (Pipeline Map)
// --------------------------------------------------------------------------

/** Mirrors `app.services.pipeline_topology._assemble_one`'s per-node dicts
 * (Task 2). Discriminated on `type`; each variant carries exactly the keys
 * that node's builder emits -- e.g. only `connector`/`sink`/`merge` carry a
 * `state` (from `_safe_status`, always a string, degrading to `"unknown"`
 * rather than ever being absent/null). */
export type PipelineNode =
  | { type: "source"; name: string }
  | { type: "connector"; name: string | null; kind: string | null; state: string }
  | { type: "topic"; name: string }
  | { type: "sink"; name: string | null; state: string }
  | { type: "bronze"; fqn: string }
  | { type: "merge"; name: string; state: string }
  | { type: "silver"; fqn: string }
  | { type: "buckets"; buckets: string[] };

/** Mirrors `app.services.pipeline_topology._assemble_one`/`_batch_pipeline`'s
 * top-level dict. On the error path (`_assemble_one`'s `except` branch) the
 * backend omits `disposition`/`authoritative`/`owned_tables`/`owned_buckets`
 * entirely and sets `error` -- those fields are optional here to match. */
export interface Pipeline {
  name: string;
  cr_kind: string | null;
  disposition?: "entity" | "event" | "batch";
  authoritative?: { fqn: string; layer: string };
  nodes: PipelineNode[];
  owned_tables?: string[];
  owned_buckets?: string[];
  error?: string;
}

export interface PipelinesResponse {
  pipelines: Pipeline[];
}

/** GET /api/pipelines -> one topology entry per pipeline (grouped by the
 * `lakehouse.solus.dev/source` annotation), for the Pipeline Map page. */
export async function getPipelines(): Promise<PipelinesResponse> {
  return request<PipelinesResponse>("/pipelines");
}
