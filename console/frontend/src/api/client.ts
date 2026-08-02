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
  create_topic?: boolean;
  topic_partitions?: number;
  topic_replication_factor?: number;
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

/** Mirrors app.models.IngestSnippets from ingest-config endpoint. */
export interface IngestSnippets {
  fluentbit: string;
  vector: string;
  logstash: string;
  generic: string;
}

/** Mirrors app.models.IngestProducer from ingest-config endpoint. */
export interface IngestProducer {
  user: string;
  mechanism: string;
  password: string | null;
  secret_ref: string;
}

/** Mirrors app.routers.sources.get_ingest_config()'s dict shape. */
export interface IngestConfig {
  external_bootstrap: string;
  topic: string;
  disposition: "event" | "entity";
  // Optional[str] backend-side -- the pipeline's authoritative table may not
  // be resolvable yet (e.g. Silver merge not provisioned), so callers must
  // null-check rather than assume a resolved FQN.
  authoritative_fqn: string | null;
  producer: IngestProducer;
  expected_json: Record<string, unknown> | null;
  snippets: IngestSnippets;
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
  /** Composite connector name (e.g. `kafka-ingest-<source>-<target_table>`)
   * for the connector this add-source run created, when applicable -- see
   * app.orchestrator.AddSourceResult.connector_name. Absent/null on the
   * gitops and spark-batch lanes (no KafkaConnector created) or on in-band
   * failure. Callers should fetch ingestion config by THIS name rather than
   * the bare source id, which is ambiguous when one source id owns multiple
   * kafka-ingest target tables. */
  connector_name?: string | null;
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

/** Mirrors app.routers.tables.list_tables()'s per-table dict (Task 3):
 * `used` distinguishes tables owned by an active pipeline's `owned_tables`
 * (in which case `pipeline`/`role` are set) from orphans (leftover from a
 * deleted pipeline, or hand-created -- `hint` explains which). `records` is
 * `null` when `IcebergService.table_stats` couldn't resolve a snapshot
 * (empty/never-committed table), never a missing key. */
export interface TableEntry {
  table: string;
  used: boolean;
  pipeline?: string;
  role?: "authoritative" | "bronze" | "silver";
  hint?: string;
  records: number | null;
}

export interface TableNamespace {
  name: string;
  tables: TableEntry[];
}

export interface TablesResponse {
  catalog: string;
  namespaces: TableNamespace[];
}

/** Mirrors app.routers.buckets.list_buckets()'s per-bucket dict (Task 3):
 * same `used`/`pipeline`/`role`/`hint` used/orphan labeling as `TableEntry`,
 * plus `objects` -- the full `{count, capped}` dict from
 * `S3Service.object_count` (a bounded/paginated count; `capped: true` means
 * the real count may exceed `count`, so the UI should render it as "N+").
 * `null` when the router's `object_count` call itself failed (e.g. a
 * non-NoSuchBucket ClientError -- AccessDenied, a transient timeout) --
 * mirrors `TableEntry.records`'s `null`-on-no-snapshot degrade-gracefully
 * shape, so the UI can render "—" the same way. */
export interface BucketEntry {
  name: string;
  used: boolean;
  pipeline?: string;
  role?: "authoritative" | "bronze" | "silver";
  hint?: string;
  objects: { count: number; capped: boolean } | null;
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

/** Thrown by `request()` on a non-ok HTTP response. Carries the raw status
 * code + response body text alongside the same message string a plain
 * `Error` used to have, so callers that only care about the message keep
 * working unmodified while callers that need to branch on status (e.g. a
 * 409 rebalance-in-progress or a gitops-remediation payload) can do so
 * without re-parsing the message string. */
export class ApiError extends Error {
  status: number;
  body: string;
  constructor(message: string, status: number, body: string) {
    super(message);
    this.name = "ApiError";
    this.status = status;
    this.body = body;
  }
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`${BASE}${path}`, {
    headers: { "Content-Type": "application/json" },
    ...init,
  });
  if (!res.ok) {
    const detail = await res.text().catch(() => "");
    throw new ApiError(
      `${init?.method ?? "GET"} ${path} failed: ${res.status} ${detail}`,
      res.status,
      detail,
    );
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

/** GET /api/sources/{name}/ingest-config -> Kafka log ingestion config for a source. */
export async function getIngestConfig(name: string): Promise<IngestConfig> {
  return request(`/sources/${encodeURIComponent(name)}/ingest-config`);
}

// --------------------------------------------------------------------------
// Connectors (restart / debug)
// --------------------------------------------------------------------------

/** Mirrors app.routers.sources.source_connectors()'s per-connector dict
 * (Task 3/4): the KafkaConnector CR(s) a source's pipeline owns -- a CDC/
 * stream source has one `role:"source"` connector, an event-lane pipeline
 * also has a `role:"sink"` connector. `kind`/`state` are `null` when the
 * backend couldn't resolve the connector class/status. */
export interface ConnectorRef {
  name: string;
  role: "source" | "sink";
  kind: string | null;
  state: string | null;
}

/** Mirrors a single entry of app.routers.connectors.connector_debug()'s
 * `tasks` list -- one Kafka Connect task's id/state/worker + its most recent
 * trace (`null` when the task has no error trace to show). */
export interface ConnectorTask {
  id: number | null;
  state: string | null;
  worker_id: string | null;
  trace: string | null;
}

/** Mirrors app.routers.connectors.connector_debug()'s `logs_hint` dict:
 * a ready-to-run `oc logs` recipe (never fetched by the Console itself --
 * Console never proxies pod logs) plus an optional deep link into an
 * external logging UI (`null` when none is configured). */
export interface LogsHint {
  namespace: string;
  connect_pods_selector: string;
  search_terms: string[];
  oc_command: string;
  external_link: string | null;
}

/** Mirrors app.routers.connectors.connector_debug()'s top-level dict. */
export interface ConnectorDebug {
  name: string;
  state: string;
  tasks: ConnectorTask[];
  logs_hint: LogsHint;
}

/** Mirrors the `remediation` dict nested in the 409 response body a
 * gitops-mode pause/resume returns (`{detail: {message, remediation}}`) --
 * the actionable "edit this file in the pipeline repo" recipe shown in
 * place of the generic error when Console can't apply the change directly. */
export interface GitopsRemediation {
  reason: string;
  where: string;
  repo: string;
  path: string;
  field: string;
  value: string | boolean;
  steps: string[];
}

/** GET /api/sources/{name}/connectors -> the KafkaConnector CR(s) (source
 * and, for event-lane pipelines, sink) that belong to this source. */
export async function getSourceConnectors(name: string): Promise<{ connectors: ConnectorRef[] }> {
  return request(`/sources/${encodeURIComponent(name)}/connectors`);
}

/** GET /api/connectors/{name}/debug -> per-task state/trace + a log-search
 * recipe for a single connector. */
export async function getConnectorDebug(name: string): Promise<ConnectorDebug> {
  return request(`/connectors/${encodeURIComponent(name)}/debug`);
}

/** POST /api/connectors/{name}/restart -> restart a connector (optionally
 * its tasks, optionally only the failed ones). */
export async function restartConnector(
  name: string,
  opts: { include_tasks?: boolean; only_failed?: boolean } = {},
): Promise<{ ok: boolean; name: string }> {
  return request(`/connectors/${encodeURIComponent(name)}/restart`, {
    method: "POST",
    body: JSON.stringify(opts),
  });
}

// --------------------------------------------------------------------------
// Tables / Buckets / Schemas / Status
// --------------------------------------------------------------------------

/** GET /api/tables -> Trino/Iceberg namespaces + tables. */
export async function listTables(catalog = "lakehouse"): Promise<TablesResponse> {
  return request<TablesResponse>(`/tables?catalog=${encodeURIComponent(catalog)}`);
}

/** GET /api/buckets -> S3 buckets with used/orphan labeling + object counts. */
export async function listBuckets(): Promise<BucketEntry[]> {
  const data = await request<{ buckets: BucketEntry[] }>("/buckets");
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
