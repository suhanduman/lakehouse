# 07 — Self-audit: over-engineering and hand-rolled-what-should-be-config

Independent read of `/Users/suhanduman/Desktop/KÇ` @ `main` (0cb3316), 2026-09-10.
Scope rules honored: no `docs/superpowers/`, `.superpowers/`, memory or spec/plan files were read; comments were treated as claims. Every LOC number below was produced with `find | xargs wc -l` (VERIFIED). "VERIFIED" = I read the code / rendered the chart / ran the render functions / fetched upstream source. "INFERENCE" = reasoned from evidence but not executed against a cluster.

Repo-wide non-test code footprint (VERIFIED):

| Area | Non-test LOC | Test LOC | Tests |
|---|---|---|---|
| Console backend (`console/backend/app`) | 7,902 | 13,684 | 721 collected (`pytest --collect-only`) |
| Console frontend (`console/frontend/src`) | 4,587 | 3,197 | 120 `it()` (vitest) |
| Helm chart templates (`chart/templates`) | 9,317 (7,881 root + 806 console + 630 monitoring) | 8,926 | 392 `it:` in 36 helm-unittest suites + 4 shell gates |
| Chart values/scripts | 3,518 values + 890 scripts (`helm-check.py` 785) | — | — |
| Spark jobs (`tools/jobs`) | 806 | 691 | 44 |
| `tools/create_iceberg_table.py` | 467 | 190 | 12 (skipped in CI: pyiceberg not installed) |
| Other `tools/` (templates, scaffold, validate) | 462 | — | — |
| Images (`images/*/Dockerfile` + `lakehouse_nb.py`) | ~490 | ~150 | 13 |
| GitOps (`gitops/`) | 534 | — | — |
| Bootstrap (`bootstrap/`) | 973 | 633 | shell suites |
| Operators (`operators/`) | 583 | — | — |
| E2E (`test/e2e`) | 1,398 | — | stage-1 on PR, stage-2 manual only |
| `manual-install/manifests/lakehouse-example.yaml` | 4,673 (rendered artifact, 188 KB) | — | — |

Rendered default chart: 5,336 lines, ~130 resources (18 ConfigMap, 9 Deployment, 8 Service, 7 KafkaTopic, 6 KafkaUser, 7 NetworkPolicy, 3 CNPG Cluster, 3 Job, 2 ScheduledSparkApplication, 1 Keycloak + 1 KeycloakRealmImport, 1 Kafka, 1 KafkaConnect …). `Chart.yaml` has **no `dependencies:`** — zero sub-charts (VERIFIED).

---

## A. Subsystem inventory

| Subsystem | LOC | What it does | Off-the-shelf / config alternative | Verdict | Why |
|---|---|---|---|---|---|
| `app/services/render_service.py` | 1,458 | Pure builders: `SourceSpec` → 8 source-connector shapes + 2 Iceberg-sink shapes + KafkaTopic + KafkaUser + namespace DDL + signal-table DDL + connection-test configs + 4 log-shipper snippets | Static `iceberg.tables=<ns>_raw.<t>` instead of `_target_table` InsertField + dynamic routing; Iceberg's own `KafkaMetadataTransform`/`DebeziumTransform`; Strimzi `KubernetesSecretConfigProvider` instead of DirectoryConfigProvider mount coupling | SIMPLIFY (~-400 LOC) | The connector-key generation itself is legitimate (485 keys across 13 CR shapes cannot be a Helm template a customer edits by hand), but ~30% of it exists to service self-inflicted choices (route SMT, control-topic tuning constants, 7 parallel connection-test builders). |
| `app/orchestrator.py` | 898 (~250 docstring) | 8-step add-source pipeline with reverse rollback, restorative undo, RUNNING poll, incremental-snapshot backfill + confirmation, gitops branch | Only the apply+verify+backfill core is irreducible; table pre-create, per-pipeline buckets, ACL/producer steps, Trino namespace DDL, merge-patch-null restore are all downstream of earlier choices | SIMPLIFY (~-350 LOC) | With the table/bucket/ACL steps removed the pipeline is secret → topic → connector → sinks → verify → backfill. |
| `app/models.py` + `source_types.py` | 339 + 118 | Pydantic spec + validation; registry of 10 (kind,type) lanes | None (this is the product's domain model) | KEEP, trim flat-field shim | 8 `_only_table()` property shims + before-validator exist only for pre-R2 payload compat (`models.py:23-27, 217-236, 306-336`). |
| `app/routers/sources.py` | 1,668 | 21 routes: create/preview/test-connection/list/get/connectors/ingest-config/credentials/edit/pause/resume/stop/start/enable-snapshots/snapshot/snapshot-stop/snapshot-progress/delete/delete-table + 20 CR-introspection helpers | Debezium UI is archived (Sept 2025); Debezium Platform targets Debezium Server, not Strimzi Connect — no drop-in. Kafka Connect REST covers pause/resume/restart/status; Strimzi `spec.state` covers the rest | SIMPLIFY (~-500 LOC) | Half the file recovers `(source, ns, table, topic)` back out of CR annotations/`transforms.route.static.value` (`_target_ns_table`, `_topics_from_config`, `_kafka_ingest_topic`, `_spark_target`, `_sink_crs_of`…). That is the cost of having no state store of its own and encoding identity into CR names/SMT values. |
| `app/services/k8s_service.py` | 493 | CR apply/patch/delete, secret CRUD, KafkaUser ACL read-modify-write with resourceVersion retry | Server-side apply (one PATCH, field-manager) replaces create→409→merge-patch dance and makes "restore previous body" trivial | SIMPLIFY | `ensure_user_acl/remove_user_acl` (60 LOC) disappear if the ACL model is fixed (see E-3). |
| `app/services/iceberg_service.py` + `tools/create_iceberg_table.py` | 332 + 467 | pyiceberg pre-create of Bronze (day partition, metadata cols) and Silver (identifier, bucket(16,id), sort order, CoW props); SQL→Iceberg type map **duplicated verbatim** in both files | Iceberg sink `iceberg.tables.default-partition-by=day(__ts_ms)` (VERIFIED `SchemaUtils.createPartitionSpec` handles `day()`/`bucket()`); Spark DDL `CREATE TABLE … PARTITIONED BY (bucket(16,id))` + `ALTER TABLE … SET IDENTIFIER FIELDS` (VERIFIED in Iceberg 1.9.0 spark-ddl docs; extensions already on in `_helpers.tpl:304`) | REPLACE-WITH-config/Spark DDL (~-800 LOC + image + PreSync Job/CM) | Whole subsystem exists because MERGE reads the key from Iceberg identifier fields (`merge_cdc.py:138-162`, a py4j reflection hack). A table property `app.merge-key` set by the sink/Console removes the need. See B/E-5. |
| `app/services/gitops_render.py` + `git_writer.py` | 250 + 139 | Fileset per source (PreSync ConfigMap+Job ×2 layers ×N tables + topic + connector + sinks); shell-git commit with askpass | ArgoCD ApplicationSet cannot render connector configs — a git writer is the right shape. Kustomize/Helm cannot replace it because the per-source content is computed | KEEP core; shrinks ~120 LOC when pre-create Jobs go | 4 of the 6-7 files per table are pre-create envelope (`gitops_render.py:226-242`). |
| `app/services/pipeline_topology.py` | 316 | Re-derives pipelines from CRs for the map UI | None | KEEP | Pure/testable; but complexity is again "recover identity from CRs". |
| `snapshot_notifications.py` + `kafka_consumer_service.py` + `kafka_producer_service.py` | 138 + 146 + 31 | Read Debezium notification topic, confirm incremental snapshot started; DLQ peek | Debezium source-signal table channel (an `INSERT` — they already mandate the table exists) | KEEP (necessary correctness), consider channel swap | See E-7. |
| `deps.py`, `authz.py`, `config.py`, `main.py` | 389 + 79 + 79 + 90 | JWT/JWKS, RBAC actions, settings | oauth2-proxy sidecar could drop JWKS code (~100 LOC) | KEEP | Small. |
| `connect_service.py`, `trino_service.py`, `s3_service.py`, `logs_hint.py` | 75 + 70 + 124 + 42 | Thin wrappers | — | KEEP | Thin, injectable. |
| Frontend (`AddSourceWizard.tsx` 1,631, `SourceDetail.tsx` 1,066, `client.ts` 756, rest ~1,100) | 4,587 | 4-step wizard, source detail with snapshot/DLQ/ingest-config panels, pipeline map | None off-the-shelf (Debezium UI archived, Kafka-UI already deployed for raw connector view) | KEEP; SIMPLIFY wizard (~-400) | `buildSpec` (155 LOC) mirrors backend validation; `FIELD_META`, `SIGNAL_TABLE_DDL` duplicate backend truths (`AddSourceWizard.tsx:54,365`). |
| Chart: Kafka/Connect (`03-kafka-strimzi.yaml` 349, `12-kafka-connect.yaml` 197, `20-connect-acl-seed.yaml` 130) | 676 | Strimzi CRs, fixed users/topics, ACL seed Job | Strimzi CRs are the correct off-the-shelf layer | KEEP CRs; DELETE seed Job (see E-3) | The Job + "runtime-owned ACLs" pattern is a workaround for a self-created conflict. |
| Chart: Trino (`06-trino-ha.yaml`) | 726 | Hand Deployments (coord + workers), ConfigMap with jvm/config/catalogs/rules/resource-groups, TLS, HPA, PDB | `trinodb/charts` (official, v1.42.x) | REPLACE-WITH-subchart (~-550) | Everything here maps to `server.config`, `catalogs`, `additionalConfigProperties`, `accessControl`, `resourceGroups` values. |
| Chart: Superset (`19-superset.yaml` 624 + `_superset.tpl` 101) | 725 | Deployments (web/worker/beat/redis), CNPG, init Job, token-refresher sidecar | `apache/superset` official chart (has redis/pg/worker/beat/init) | REPLACE-WITH-subchart (~-450) | `superset_config.py` and the Trino JWT refresher sidecar are the only custom parts; both fit `extraConfigs`/`extraContainers`. |
| Chart: JupyterHub (`23-jupyterhub.yaml`) | 695 | Hand Hub + CHP proxy Deployments, RBAC, ConfigMap `jupyterhub_config.py`; custom `images/jupyterhub` image | `jupyterhub/zero-to-jupyterhub-k8s` (official; ships hub, proxy, RBAC, KubeSpawner, GenericOAuthenticator, idle culler) | REPLACE-WITH-subchart (~-550 + image) | The chart literally re-implements z2jh's core with a python-slim image (`images/jupyterhub/Dockerfile`). `hub.extraConfig` takes the pre_spawn_hook verbatim. |
| Chart: Nessie (`04-nessie-ha.yaml` 311 + `22-nessie-machine-auth.yaml` 87) | 398 | Hand Deployment/Service/PDB/HPA + OAuth client secret plumbing | `projectnessie` official chart (`charts.projectnessie.org`) | REPLACE-WITH-subchart (~-220) | Env/JDBC/OIDC config are first-class chart values. |
| Chart: Keycloak (`10-keycloak.yaml`) | 651 | Operator CRs (`Keycloak`, `KeycloakRealmImport` with AD federation + ~10 clients) | Already operator-based — this is config | KEEP | 400+ lines is realm JSON-as-YAML; unavoidable content. |
| Chart: Zeppelin (`_zeppelin.tpl` 617 + `09-zeppelin.yaml` 48) | 665 | Hand Deployment with 3 containers, PVC, interpreter ConfigMaps | No maintained official chart | KEEP (or drop Zeppelin — JupyterHub covers the use case) | Two notebook stacks (Zeppelin admin + JupyterHub per-user) is itself the over-engineering. |
| Chart: Apicurio (`11-apicurio-registry.yaml` 191 + `24-apicurio-compat-seed.yaml` 73) | 264 | Hand Deployment + CNPG + curl Job to set compatibility | Community chart (`eshepelyuk/apicurio-registry-helm`) or Apicurio Operator | SIMPLIFY (~-120) | Small; the compat-seed Job is fine. |
| Chart: Gitea (`16-gitea.yaml`), MinIO (`17-minio.yaml`, `18*`), Spark History (`21`), dbt (`20-dbt.yaml`), Grafana (`monitoring/grafana.yaml` 323), Kafka-UI (`console/kafka-ui.yaml` 245) | 173+229+101+111+323+245 = 1,182 | Hand Deployments for dev/aux components | `gitea-charts/gitea`, `minio/minio`, `grafana/grafana`, `kafbat/kafka-ui` charts | REPLACE-WITH-subchart / DELETE from prod chart (~-800) | Gitea+MinIO are dev conveniences living in the prod chart behind flags. |
| Chart: cross-cutting (`_helpers.tpl` 434, `08*-routes/ingress` 553, `00-namespace` 155, `01-storage` 62, `15-external-secrets` 105, `prereq-check*` 272, `NOTES.txt` 293, `05-spark-operator` 179, `14-nginx-ingest` 163, `02-postgres-ha` 124, `console/console.yaml` 513, monitoring rules/monitors 307) | ~3,160 | Namespacing, routes, ESO, quota, Spark CRs, console | Mostly necessary glue | KEEP; SIMPLIFY prereq-check (272) → `helm` `.Capabilities` checks are already there | `NOTES.txt` 293 lines is documentation in a template. |
| `chart/scripts/helm-check.py` | 785 | Custom render linter: namespace check, stray `{{`, service-reference closure, Route/Ingress/NetworkPolicy selector closure | `kubeconform` + `kube-linter` + `helm lint` cover 2 of 4 checks; service-closure is genuinely custom | SIMPLIFY (~-300) | The closure check is valuable precisely because everything is hand-templated; it shrinks with sub-charts. |
| `tools/jobs/merge_cdc.py` + `merge_lib.py` | 352 + 184 | Dynamic Bronze→Silver MERGE: snapshot-id watermark, ROW_NUMBER dedup, schema reconcile (add/widen/conflict), CoW MERGE, audit table, FAIR-pool parallelism, Nessie conflict retry | dbt-trino incremental `merge` (dbt + Trino are both already in the stack: `20-dbt.yaml`, `examples/dbt-gold`) with `on_schema_change: append_new_columns`, Console-generated one model per table into the pipelines repo | REPLACE-WITH-dbt-trino is viable; KEEP if snapshot-id watermark + type-widening detection are considered must-haves | See D. |
| `tools/jobs/iceberg_maintenance.py` + `maintenance_lib.py` | 109 + 18 | Hourly: `gc.enabled=true` ALTER, Bronze TTL DELETE, rewrite_data_files, rewrite_position_delete_files, expire_snapshots, remove_orphan_files, per table | Trino `ALTER TABLE … EXECUTE optimize / expire_snapshots / remove_orphan_files` (no Spark needed); Apache Amoro (too heavy for this scale) | SIMPLIFY | Small already; the hourly `ALTER TABLE SET TBLPROPERTIES gc.enabled` per table is a needless commit per table per hour (E-4). |
| `tools/jobs/nginx_streaming.py` | 143 | Long-running Spark Structured Streaming: Kafka → regex parse → peppered sha2(IP) → Iceberg | Parse + hash at the shipper (Vector VRL `sha2()`, Fluent Bit nginx parser + Lua) → the Console's existing `stream/kafka` lane (Iceberg sink) | REPLACE-WITH-shipper-config (~-143 + a permanently-reserved Spark driver) | A whole SparkApplication + KafkaUser + ConfigMap (`14-nginx-ingest.yaml` 163) for one topic. Hashing at the edge is also stronger for KVKK (raw IPs never enter Kafka). |
| `images/connect` (124), `images/spark-py` (74), `images/iceberg-tools` (31), `images/jupyterhub` (46), `images/jupyter` (175 py + nb) | ~450 | Pre-built plugin image (Iceberg runtime built from source), Spark with baked jars, pyiceberg tool, Hub, notebook helper | Connect image is necessary (Apache publishes no Iceberg KC runtime binary — claim consistent with Strimzi `spec.build` list needing a zip); spark-py necessary for air-gap | KEEP connect/spark-py; DELETE iceberg-tools + jupyterhub images with their subsystems | — |
| `tools/templates/*.yaml` + `scaffold-source.sh` + `validate.sh` | 462 | Hand YAML templates of the same connectors the Console renders | Superseded by `render_service` (it cites them for "parity") | DELETE (~-460) | Two sources of truth; templates still carry stale `13-connectors.yaml` references. |
| `gitops/`, `operators/`, `bootstrap/` | 534 + 583 + 973 | App-of-apps, OLM subscriptions, Layer-0 script | Standard; `operators/*.yaml` are 80% comments | KEEP | Fine. |
| `manual-install/manifests/lakehouse-example.yaml` | 4,673 | A committed `helm template` output | `helm template` | DELETE from repo | Build artifact under version control; drifts by construction. |

---

## B. The Console trio: render_service / orchestrator / models

**Size:** 1,458 + 898 + 339 (+118 registry) = 2,813 LOC non-test; 1,818 + 2,064 + 549 = 4,431 LOC tests.

**What it generates (VERIFIED by executing the renderers in the project venv):**

| Lane | Source class | keys | source SMT chain | Sink keys | sink SMT chain |
|---|---|---|---|---|---|
| cdc-mssql | SqlServerConnector | 54 | `unwrap,tsconv` | 36 | `route,kafkameta` |
| cdc-pg | PostgresConnector | 42 | `unwrap,tsconv` | 36 | `route,kafkameta` |
| cdc-mysql | MySqlConnector | 54 | `unwrap,tsconv` | 36 | `route,kafkameta` |
| cdc-mongo | MongoDbConnector | 35 | `unwrap,route,tsconv` | 33 | `kafkameta` |
| scheduled-jdbc | Aiven JdbcSourceConnector | 30 | `route,setop,setdel,tsfield,tsconv` | 33 | `kafkameta` |
| kafka-ingest | (sink only) | — | — | 46 | `route,setop,setdel,tsms,tsconv,kafkameta` |
| camel-http/mqtt/rabbitmq | Camel source | 4-7 | (none, byte[]) | 46 | `route,setop,setdel,tsms,tsconv,kafkameta` |

13 CR shapes, **485 config keys**, **7 distinct SMT chains**, 6 SMT classes. Every key is a plain string in a Python dict; no Helm/Kustomize could produce this per customer-source without a generator, so *some* renderer is necessary.

**Where it re-implements what Debezium / the Iceberg sink already do:**

1. **Routing.** Every dedicated sink reads exactly one topic and writes exactly one table (`render_cdc_sinks`, `render_service.py:1101-1150`), yet it enables `iceberg.tables.dynamic-enabled=true` + `route-field=_target_table` and stamps `_target_table` via an InsertField SMT (`_route_transform`, `:161-169`). Static `iceberg.tables=<ns>_raw.<t>` does the same with zero SMTs. Cost of the current design: `_target_table` leaks into Bronze as a data column (VERIFIED: `merge_lib.py:381` must exclude it; `test_merge_lib.py:33-41` exists to keep it excluded), `_route_on_sink` ownership logic (`:1085-1098`), `pipeline_topology._route_cr` "whichever CR has the key" resolution, and `sources._target_ns_table` fallback parsing. Dynamic routing was the right tool when there was one shared sink (the stale `chart/templates/13-connectors.yaml` references in `merge_lib.py:376`, `create_iceberg_table.py`, `test/e2e/fixtures/pg-cdc-connector.yaml:6` confirm that era; the file no longer exists — VERIFIED); it was kept after the move to dedicated sinks.
2. **Metadata SMTs.** `kafkameta` (InsertField offset/partition) duplicates the sink's own `KafkaMetadataTransform`; `unwrap + add.fields + tsconv (+route)` duplicates Iceberg's `DebeziumTransform` (`_cdc.op/_cdc.ts/_cdc.offset/_cdc.key/_cdc.target`, VERIFIED in Iceberg 1.9.0 docs). Adopting `DebeziumTransform` + `route-field=_cdc.target` would even allow ONE fan-out sink per source for R2 multi-table instead of N sinks — the design the sink was built for. What is lost: per-table sink status/blast-radius in the UI, and the `__ts_ms`-as-Timestamp contract that Bronze partitioning and the MERGE ordering rely on (`_cdc.ts` is also a timestamp, so the MERGE would need a rename, not a redesign).
3. **Credentials.** `_cred()` renders `${directory:/mnt/external-configuration/<source>:<key>}` (`:112-118`) and the orchestrator creates a Secret named `<source>` (`orchestrator.py:628-640`). **Nothing in the Console patches `KafkaConnect.spec.template.pod.volumes`** (VERIFIED: grep of `console/backend/app` for `external-configuration|KafkaConnect|volumes` outside render_service finds no writer), and the chart mounts only fixed names `mssql/pg/mongo/s3/debezium-src/kafka-ca(/ext-kafka-ca/nessie-oauth)` (`12-kafka-connect.yaml:136-160`) while `models._RESERVED_SOURCE_NAMES` forbids naming a source `pg`/`mssql`/`mongo`. So in direct mode a source named e.g. `pgshop` renders a placeholder that has no directory to resolve from — INFERENCE: connector fails at start (DirectoryConfigProvider throws), which `verify` would report as FAILED and roll back. 721 backend tests are green against fakes; `docs/POC-DOGRULA-cluster-checklist.md:177` already lists this ("Per-source credential-volume otomasyonu … mount adı çakışır") as open. The config alternative is Strimzi's `KubernetesSecretConfigProvider` (`${secrets:<ns>/<name>:<key>}`, RBAC on Secrets, no mounts) — one worker-config line replaces the whole mount contract and the "LOAD-BEARING do not rename a volume" header comment.
4. **Seven `_connection_test_*` builders (`:1226-1330`)** duplicate the real renderers' connection keys with literal creds. Reusing `render_connector()` and substituting placeholders would delete ~90 LOC and eliminate a drift surface.
5. **`_iceberg_control_tuning` (`:843-859`)** hardcodes 7 sink tuning values (60 s commit, 120 s session, etc.) as Python string constants; these are cluster-sizing knobs and belong in `settings`/chart values like `DLQ_REPLICATION_FACTOR` already almost does.

**Orchestrator: necessary vs. inherited.** Irreducible: secret → topic → connector → sinks → RUNNING poll → incremental-snapshot backfill with confirmation (Debezium really does not snapshot a table added to `table.include.list`; the confirmation loop `_arm_backfill` `:479-544` and `snapshot_started` `:490-530` guard a real Debezium 2.7.3 signal-loss window). Inherited from earlier choices: `uniqueness`/`bucket`/`namespace`/`table` steps per table (`:667-707`, pre-create + per-pipeline buckets), `acl`/`ingest-topic`/`producer` steps (`:728-772`, ACL model), `_restore_body_for_undo` merge-patch-null logic (`:162-223`, because `K8sService._apply` degrades to JSON merge patch), and a duplicate namespace creation (Trino `CREATE NAMESPACE … WITH location` at `:695-697` **and** pyiceberg `create_namespace(ns, {"location": …})` inside `iceberg_service.create_table:255-259` — two engines asserting the same namespace/location).

**Could most of it be Helm/Kustomize or a Debezium UI?** No. Debezium UI is archived (2025-09-17) and its successor Debezium Platform manages Debezium Server, not Strimzi `KafkaConnector` CRs (VERIFIED via GitHub). Helm cannot introspect a source DB or confirm a snapshot. What a generator must keep: validation (`models.py`), multi-table connector sharing + restorative rollback (R2 is forced by Postgres replication-slot economics, `render_service.py:519-524` — legitimate), GitOps write path, backfill confirmation. What it does not need: routing SMT, table pre-create, buckets, ACL mutation, mount coupling.

---

## C. Chart analysis — hand-templated vs. wrappable

**Wrapping upstream (operator CRs — the correct layer, ~1,700 LOC):** Strimzi `Kafka/KafkaNodePool/KafkaUser/KafkaTopic/KafkaConnect` (03, 12), CNPG `Cluster/ScheduledBackup` (02, 11, 19, 10), Keycloak operator `Keycloak/KeycloakRealmImport` (10), spark-operator `(Scheduled)SparkApplication` (05, 14), cert-manager `Certificate`, ESO `ExternalSecret` (15). These are configuration, not re-templating.

**Re-templating upstream manifests by hand (VERIFIED by `kind:` inventory per file):**

| Component | Hand LOC | Official/community chart | Est. wrapper LOC | Deletable |
|---|---|---|---|---|
| Trino | 726 | `trinodb/charts` (official) | ~170 | ~550 |
| Superset | 725 | `apache/superset` (official) | ~250 (config.py + refresher sidecar) | ~450 |
| JupyterHub (+ `images/jupyterhub`) | 695 (+46) | `jupyterhub/zero-to-jupyterhub-k8s` (official) | ~150 | ~550 |
| Nessie | 398 | `projectnessie` (official) | ~150 | ~220 |
| Grafana | 323 | `grafana/grafana` | ~90 | ~230 |
| Kafka-UI | 245 | `kafbat/kafka-ui` | ~50 | ~190 |
| Apicurio | 264 | community `eshepelyuk/apicurio-registry-helm` (no official chart; operator exists) | ~120 | ~140 |
| Gitea | 173 | `gitea-charts/gitea` | ~30 | ~140 |
| MinIO (+bucket-init) | 229 | `minio/minio` (or keep as dev-only) | ~60 | ~170 |
| Zeppelin | 665 | none maintained | — | 0 (or 665 if Zeppelin is dropped in favor of JupyterHub) |
| Spark History | 101 | none official | — | 0 |
| **Total** | **4,544** | | **~1,070** | **~2,650 (≈28% of templates)** |

Plus `chart/tests/*.yaml` assertions on those hand-templated Deployments (jupyter 47, superset 30, trino* 38, nessie-auth 40 ≈ 155 of 392 `it:`) that would become moot, and `helm-check.py --service-closure` shrinks because fixed `lakehouse.svc.*` names (`_helpers.tpl:96-121`) stop being load-bearing.

**What blocks a straight swap (honest):** OpenShift `restricted-v2` SCC compliance (`lakehouse.podSecurityContext` helpers are threaded into every hand Deployment), Route-vs-Ingress dual rendering (`08`/`08b`), the `values-*.yaml` sizing-tier contract (`sizing-tiers_test.sh`), and umbrella-chart version churn. These are real but they are the *same* problems every platform team solves with sub-chart `values:` blocks and a `global.security` passthrough — not reasons to own 700 lines of Trino Deployment YAML. Risk is highest for Trino (resource groups, rules.json, dual-catalog config) and lowest for Gitea/MinIO/Grafana.

**Other chart observations (VERIFIED):** `NOTES.txt` 293 lines; `prereq-check-job.yaml` + `_prereq-check.tpl` 272 lines of Job-based CRD checks when `.Capabilities.APIVersions.Has` already exists in the same file; comment density in `12-kafka-connect.yaml`, `03-kafka-strimzi.yaml`, `23-jupyterhub.yaml` is 40-60% — many comments narrate task history ("Task 5", "Task 7", "R2") rather than behavior.

---

## D. Spark jobs

| Job | LOC | Does | Duplicates engine feature? | Tests |
|---|---|---|---|---|
| `merge_cdc.py` + `merge_lib.py` | 536 | Discover `rawlake.<ns>_raw.*`; per table: DESCRIBE both, `reconcile_plan` (add/promote/conflict), `ALTER TABLE ADD/ALTER COLUMN`, read identifier via py4j (`:138-162`), incremental scan from snapshot-id watermark with full-read fallback (`:165-183`), `ROW_NUMBER()` latest-per-key (`merge_lib.py:471-483`), `MERGE INTO … WHEN MATCHED AND __deleted THEN DELETE` (`:518-538`), audit row, ThreadPool + FAIR pools, Nessie commit-conflict retry (`:495-515`) | **Dedup + MERGE + schema-add**: dbt-trino/dbt-spark incremental `merge` with `unique_key` + `on_schema_change: append_new_columns` does this declaratively; Console can generate one 10-line model per table into the pipelines repo (dbt is already deployed: `20-dbt.yaml`, `images/dbt`). **Watermark**: dbt would use a `max(__ts_ms)` high-watermark; snapshot-id incremental scan is the Iceberg-native, engine-provided feature used correctly here — but it is only an optimization (the code says so and falls back to a full read). **Type widening detection** (`is_safe_promotion`) has no dbt equivalent — this is genuinely earned code. **Identifier lookup via JVM reflection** is a self-inflicted hack (see E-5). | 31 tests, all Spark-free with monkeypatched `_merge_one`/`_discover_bronze` (`test_merge_cdc.py:1-7`). The SQL strings are asserted textually; MERGE semantics (three-valued `__deleted`, CoW vs MoR) are never executed. |
| `iceberg_maintenance.py` + `maintenance_lib.py` | 127 | Discover all tables; per table: `ALTER … gc.enabled=true`, Bronze TTL `DELETE WHERE __ts_ms < …`, `rewrite_data_files(delete-file-threshold=1)`, `rewrite_position_delete_files`, `expire_snapshots`, `remove_orphan_files` | Trino Iceberg `ALTER TABLE … EXECUTE optimize/expire_snapshots/remove_orphan_files` could do this without Spark (INFERENCE on Nessie+gc.enabled interplay). Amoro would replace it wholesale but adds an AMS + optimizer fleet — not proportionate. | 9 tests asserting SQL strings. |
| `nginx_streaming.py` | 143 | Structured Streaming Kafka→regex→sha2(pepper‖ip)→Iceberg append | Log shipper (Vector `parse_nginx_log()` + `sha2()`, or Fluent Bit nginx parser + Lua) + the Console's existing `stream/kafka` Iceberg-sink lane | 4 tests on `parse_line` regex only; the Spark expression tree (`_month_num_expr`, `to_timestamp`) is untested. |
| `create_iceberg_table.py` | 467 | See A | Iceberg sink `default-partition-by` for Bronze; Spark DDL for Silver | 12 tests; **skipped in CI** (`ci.yaml`: pyiceberg deliberately not installed). |

Verdict: the Spark tier is not gratuitous — Iceberg CDC MERGE with delete handling is exactly the gap the Apache sink leaves (VERIFIED: `iceberg.tables.upsert-mode-enabled`/`cdc-field` are absent in 1.9.0 and `id-columns` is parsed but consumed by no writer). The over-engineering is around it: pre-create tooling, py4j identifier lookup, a streaming job for one log topic, and duplicated type maps.

---

## E. Workaround smell list

| # | Mechanism | Problem it solves | Self-inflicted? | Simpler alternative |
|---|---|---|---|---|
| 1 | Per-connector control topic `control-<name>` + 60 s commit / 120 s session / 130 s request timeouts (`render_service.py:843-859`) | Two dedicated sinks sharing the default `control-iceberg` "cross-read each other's commit events"; consumer-group flapping on small clusters | Partly: N dedicated sinks (choice A) multiply coordinators. Upstream default is one shared topic (VERIFIED `IcebergSinkConfig.DEFAULT_CONTROL_TOPIC`) | Keep per-connector topics (cheap), but move the 7 numbers to values; note 60 s vs upstream 300 s means 5× snapshots → more compaction load. |
| 2 | Per-pipeline S3 buckets `bronze-<ns>`/`silver-<ns>` (`bronze_bucket_name`, orchestrator `:686-693`, delete `_teardown_targets`) | Isolation + `with_data` teardown; S3 residue check | Yes — Iceberg namespace `location` (which they already set) gives per-pipeline prefix isolation in one bucket | One bucket per layer + prefix per namespace; teardown = `DROP TABLE … PURGE` + prefix delete. Removes bucket-create IAM from the Console. |
| 3 | Runtime-owned `connect` ACLs: chart KafkaUser without `acls` (`03-kafka-strimzi.yaml:281-292`), merge-add seed Job (`20-connect-acl-seed.yaml`), `ensure_user_acl/remove_user_acl` (`k8s_service.py:435-493`), orchestrator `acl` step + delete-side revocation | `stream/kafka` sink must read a customer-named in-cluster topic that no prefix ACL covers; ArgoCD selfHeal would clobber runtime edits | Yes, a chain: shared `connect` principal for customer topics → runtime CR mutation → conflict with declarative KafkaUser → seed Job. External Kafka never needed it (`consumer.override.*` uses the customer's creds) | Per-source KafkaUser (already rendered for producers) + `consumer.override.sasl.jaas.config` with that user, or require in-cluster ingest topics under the existing `kafka-ingest-` prefix. `connect` ACLs go back into the chart; Job + 4 code sites vanish. |
| 4 | `ALTER TABLE … SET TBLPROPERTIES ('gc.enabled'='true')` every hour per table (`iceberg_maintenance.py:609`) | Nessie serves `gc.enabled=false`; expire/orphan fail | Yes — the property can be set once at create (they already pass properties at create) | Set at create (or in `autoCreateProps` via `iceberg.table-default.gc.enabled`); drop the ALTER. |
| 5 | pyiceberg pre-create of Bronze+Silver to pin identifier fields (`iceberg_service.py`, `create_iceberg_table.py`, `images/iceberg-tools`, PreSync CM+Job in `gitops_render.py:59-161`, orchestrator `table` step) + `_identifier_fields` py4j reflection (`merge_cdc.py:138-162`) | Sink auto-create sets no identifier (VERIFIED upstream: `id-columns` unused) and "Nessie REST rejects set-identifier-fields" (unverified claim) | Yes — the MERGE key was routed through Iceberg identifier fields, which nothing else (CoW Spark MERGE, Trino) consumes | Carry the key as a table property (`app.merge-key`) set by Console on Bronze (or in the sink's `autoCreateProps`); Bronze partition via `iceberg.tables.default-partition-by=day(__ts_ms)` (VERIFIED `SchemaUtils` supports `day()`); Silver created by `merge_cdc` on first run with Spark DDL (`PARTITIONED BY (bucket(16,id))`, `WRITE ORDERED BY`, extensions already enabled). Lost: fail-loud identifier check *before* the connector starts (moves to first MERGE). |
| 6 | Restorative rollback with explicit `null`s (`orchestrator._restore_body_for_undo:162-223`) | `K8sService._apply` hits 409 and falls back to JSON merge patch, which cannot drop keys | Yes — merge patch chosen for apply | Server-side apply (single PATCH `application/apply-patch+yaml`, field manager `console`) or PUT with resourceVersion; restore = re-apply prev body. |
| 7 | Kafka signal channel + notification sink on every CDC connector (2× SASL blocks per connector, `debezium-signals`/`debezium-notifications` topics, 2 KafkaUsers' ACLs, producer/consumer services, `snapshot_notifications.py`) | Trigger + confirm incremental snapshot for tables added later; UI progress | Half: the *confirmation* is a real Debezium 2.7.3 hazard; the *Kafka channel* choice is what drags in topics/ACLs/JAAS. The source-table channel is already mandatory (`signal_data_collection` required, `models.py:280-289`) | Keep confirmation; consider the source channel (Console or DBA `INSERT`) and drop the Kafka signal plumbing — or upgrade Debezium ≥3.x where DBZ-8780 is fixed and drop the two-signal retry. |
| 8 | `_target_table` InsertField + dynamic routing on single-topic sinks | Historical shared sink | Yes | Static `iceberg.tables` (B-1). |
| 9 | DirectoryConfigProvider + fixed volume mounts (`12-kafka-connect.yaml:136-160`) vs per-source Secret names | Secrets in connector config without literals | Yes — and currently incoherent (B-3): no code adds a mount per source | `KubernetesSecretConfigProvider` (`${secrets:ns/name:key}`), no mounts, one RBAC Role. |
| 10 | Duplicate SQL→Iceberg type maps kept "in lockstep" (`iceberg_service.py:60-100` ≈ `create_iceberg_table.py:118-160`) | Console venv cannot import `tools/` (pytest.ini pythonpath) | Yes — packaging | One module, imported by both (the Console image already copies `tools/`). |
| 11 | Namespace created twice (Trino DDL with location + pyiceberg `create_namespace(location)`) | — | Yes | Pick one. |
| 12 | Committed `manual-install/manifests/lakehouse-example.yaml` (188 KB) | Offline install without Helm | Partly | `helm template` in CI release artifact, not in git. |

---

## F. Test surface

| Area | Tests | LOC | Nature | Risk |
|---|---|---|---|---|
| `test_render_service.py` | 135 | 1,818 | 315 dict-key equality assertions on rendered connector config | **Config-render tests.** They pin what Python emits, not what Debezium/the sink accept. Zero tests run a connector. The mount/placeholder incoherence in B-3 is invisible here by construction. |
| `test_orchestrator.py` + `_gitops` | 77 | 2,294 | Call-order fakes (`FakeK8s.calls`), rollback interleaving, restorative undo, backfill event ordering | Good behavioral tests *of the orchestrator's own state machine*; still fakes for k8s/S3/Trino/Kafka. |
| `test_sources_*` (9 files) + `test_other_routers`, `test_smoke_all_routes` | ~170 | ~4,300 | FastAPI TestClient over fakes | Route contract tests; fine. |
| `test_k8s_service.py`, `test_s3_service.py`, `test_iceberg_service.py`, `test_trino_service.py`, `test_connect_service.py`, `test_kafka_*` | ~120 | ~2,000 | Wrapper behavior over fake clients | `test_iceberg_service` uses a fake catalog; pyiceberg schema construction against real Nessie is never exercised in CI. |
| `test_models.py`, `test_source_types.py`, `test_pipeline_topology.py`, `test_gitops_render.py`, `test_git_writer.py` | ~90 | ~1,500 | Pure | `test_git_writer` runs real `git` on a temp repo — the one integration-ish test. |
| Frontend | 120 | 3,197 | RTL with mocked API | Fine. |
| Chart (`helm unittest`) | 392 | 8,926 | `hasDocuments count`, `equal path/value`, `fail` guards | **Pure render tests**; `jupyter_test.yaml` asserts `hasDocuments: 14` with a comment doing arithmetic — brittle by design. `integrity.sh` + `helm-check.py --service-closure` is the strongest chart gate. |
| `tools/jobs/tests` | 44 | 691 | SQL-string assertions; Spark monkeypatched | MERGE/DELETE/ALTER semantics never executed; `nginx_streaming` Spark expression tree untested. |
| `tools/tests/test_create_iceberg_table.py` | 12 | 190 | pyiceberg schema build | **Skipped in CI** (`ci.yaml` comment). |
| E2E (`test/e2e`) | stage 1 + stage 2 | 1,398 | Stage 1 (install smoke) on PR; stage 2 (pg CDC → Bronze → merge → Trino query) **manual `workflow_dispatch` only** and uses a **hand-written connector fixture** (`fixtures/pg-cdc-connector.yaml`), not Console output (VERIFIED: no `api/sources` call in `run.sh`) | Console-rendered connectors have never been exercised by CI against a live Connect. |

Ratio: ~24 kLOC of tests for ~14 kLOC of Console+jobs code, ~85% of which assert rendered shapes against fakes. The suite is excellent at catching regressions in *what the code emits* and blind to *whether the emitted thing works* — precisely the class of the B-3 finding and of the audit items the team's own earlier reviews (R1 temporal/decimal, R2 multi-table, B3 Camel byte[]) had to discover live.

---

## G. Top-5 simplifications + must-not-touch

| # | Simplification | LOC removed (est.) | Risk | What is lost |
|---|---|---|---|---|
| 1 | **Replace pyiceberg pre-create with config + Spark DDL** (E-5): key as table property, Bronze partition via sink `default-partition-by`, Silver created by `merge_cdc` first run | ~1,100 (iceberg_service 332, create_iceberg_table 467, iceberg-tools image, gitops PreSync CM/Job ~120, orchestrator table/uniqueness ~80, tables router create ~40, py4j `_identifier_fields` 25) + ~650 tests | Medium: Nessie REST behavior for `CREATE TABLE … PARTITIONED BY bucket` via Spark must be live-verified once; identifier-mismatch fail-loud moves from onboarding to first merge | Pre-connector schema validation; identifier fields on Silver (unused by CoW Spark/Trino) |
| 2 | **Sub-chart Trino, Superset, JupyterHub, Nessie, Grafana, Kafka-UI, Gitea, MinIO** (C) | ~2,650 templates + ~150 chart tests + `images/jupyterhub` | Medium-high for Trino/Superset (SCC, dual catalogs, JWT sidecar), low for the rest; do it component-by-component | Byte-level control of every Deployment; some helm-check closure guarantees |
| 3 | **Static `iceberg.tables` + `KubernetesSecretConfigProvider` + move sink tuning to settings** (B-1, B-3, B-5) | ~400 (route SMT, `_route_on_sink`, `_route_cr`, `_target_ns_table` fallbacks, `_connection_test_*` dedup, mount header) | Low (config-only on the Connect worker + renderer); fixes a live defect | `_target_table`-based recovery of ns/table from CRs (annotations already exist for this) |
| 4 | **Chart-declared `connect` ACLs; per-source consumer identity** (E-3) | ~330 (seed Job 130, `ensure/remove_user_acl` 60, orchestrator acl/producer steps ~50, delete-side revocation ~30, tests ~60) | Low | Runtime ACL mutation (a feature nobody should want under ArgoCD selfHeal) |
| 5 | **Retire `nginx_streaming.py` + `14-nginx-ingest.yaml` in favor of shipper-side parse/hash + existing `stream/kafka` lane; delete `tools/templates/*`, `manual-install/manifests`, per-table `gc.enabled` ALTER** | ~143 + 163 + 460 + 4,673 (artifact) | Low; KVKK posture improves (raw IP never leaves the host) | A Spark example job in the repo |

Optional 6th, higher risk: replace `merge_cdc`/`merge_lib` with Console-generated dbt-trino incremental models (~540 LOC + 487 tests). Lost: snapshot-id watermark, type-widening conflict detection, Nessie commit-retry — I would not do this before 1-5 are done.

**Must NOT be simplified (encodes hard-won correctness, inferred from tests and error paths, not comments):**
- One Debezium connector per source DB with `table.include.list` (`render_service.py:519-524`) and the restorative (not destructive) undo of a shared connector (`orchestrator.py:792-811`, `test_orchestrator.py` partial-failure cases) — replication-slot economics and blast radius are real.
- Incremental-snapshot backfill **with confirmation** and the `aggregate_type` "incremental"/terminal-negative filter (`snapshot_notifications.py:490-530`) — the false-positive class it guards is documented in the test names.
- `time.precision.mode` / `decimal.handling.mode` on every Debezium source (`:583-592`) and the `money`/`timestamptz`→string mappings (`iceberg_service.py:72-90`) — these encode the R1 type-fidelity findings; keep them wherever the type map ends up living.
- `publication.autocreate.mode=filtered` (`:546`), `schema.history.internal.store.only.captured.tables.ddl=false` (`:561-566`), `snapshot.mode` conditional rendering — each prevents a silent-loss mode when a table is added later.
- `merge_lib.latest_per_key_sql` ORDER BY `__ts_ms, __lsn NULLS LAST, __kafka_offset` and `CAST(__deleted AS BOOLEAN)` + `WHEN NOT MATCHED AND NOT deleted` (`merge_lib.py:471-538`) — deterministic tie-break and three-valued-logic correctness.
- `_read_increment` full-read fallback (`merge_cdc.py:165-183`) and `run_with_retry(is_commit_conflict)` — Nessie/Iceberg realities.
- `_k8s_name`/`_k8s_topic_name` crc32 collision suffixes and `TableSpec._target_ns_is_canonical` — injective naming is what makes "delete this pipeline's data" safe.
- `_source_error_config` (source: tolerate+log, DLQ is sink-only) and `errors.deadletterqueue.*` on sinks; `SCHEMA_AUTO_REGISTER` default `true` (R3 silent 100 % drop otherwise).
- Camel sources emitting `byte[]` with the medallion SMT chain moved to the dedicated sink (`:934-960`) — live-found; InsertField on byte[] silently drops.
- `images/connect` building the Iceberg KC runtime from source and `images/spark-py` baking jars (air-gap, reproducibility).

---

## H. Evidence index

**VERIFIED (read/rendered/executed):**
- All LOC tables — `wc -l` over `find` (excluding `.venv`, `node_modules`, tests as noted).
- `Chart.yaml` has no `dependencies` block; `helm template` default render = 5,336 lines, kind histogram as listed.
- 13 CR shapes / 485 config keys / 7 SMT chains — executed `render_connector`/`render_sinks` for 7 specs in `console/backend/.venv`.
- 721 backend tests, 70 tools tests — `pytest --collect-only`; chart 392 `it:` — grep; frontend 120 — grep of `it(`.
- `create_iceberg_table` tests skipped in CI — `.github/workflows/ci.yaml` python job comment; E2E stage-2 manual only and fixture-driven — `.github/workflows/e2e.yaml:4-19`, `test/e2e/run.sh`, `test/e2e/fixtures/pg-cdc-connector.yaml:1-16`.
- Iceberg 1.9.0 sink: `iceberg.tables.default-id-columns`/`id-columns` parsed in `IcebergSinkConfig.java:56,72` but consumed by no writer (`IcebergWriterFactory`, `IcebergWriter`, `SchemaUtils`, `Utilities`, `RecordProjection` — 0 hits); `autoCreateTable` uses `partitionBy()` (`IcebergWriterFactory.java:91-113`); `SchemaUtils.createPartitionSpec` supports `month/day/hour/bucket(...)` (`SchemaUtils.java:72,162-185`); no `upsert-mode-enabled`/`cdc-field` in docs; default control topic `control-iceberg`, commit interval 300 s; SMTs `DebeziumTransform`, `KafkaMetadataTransform` documented.
- Iceberg 1.9.0 Spark DDL: `ALTER TABLE … SET/DROP IDENTIFIER FIELDS`, `WRITE ORDERED BY`, `WRITE DISTRIBUTED BY PARTITION` (extensions), `PARTITIONED BY (bucket(16,id))`.
- Debezium UI archived 2025-09-17; Debezium Platform (conductor/stage) targets Debezium Server.
- Official charts exist: trinodb/charts, apache/superset, projectnessie, zero-to-jupyterhub; community: eshepelyuk apicurio.
- Console never patches `KafkaConnect` volumes; chart mounts fixed names; `_RESERVED_SOURCE_NAMES` forbids `pg/mssql/mongo`; `docs/POC-DOGRULA-cluster-checklist.md:177` lists the gap.
- `chart/templates/13-connectors.yaml` does not exist; referenced in `merge_lib.py`, `create_iceberg_table.py`, `tools/templates/source-scheduled-jdbc.yaml`, `test/e2e/fixtures/pg-cdc-connector.yaml`, `operators/03-strimzi.yaml`.
- Duplicate type maps (`iceberg_service.py` vs `create_iceberg_table.py`); duplicate namespace creation (orchestrator Trino DDL + pyiceberg).
- `_target_table` leak into Bronze — `merge_lib.py:381`, `test_merge_lib.py:33-41`.

**INFERENCE (not executed against a cluster):**
- Runtime failure mode of an unresolvable `${directory:…}` placeholder (connector FAILED at start rather than silent).
- "Nessie REST rejects set-identifier-fields (400)" — team claim; web search found related Nessie REST ALTER 400 issues but not this specific one.
- Trino `EXECUTE expire_snapshots`/`remove_orphan_files` behavior under Nessie `gc.enabled=false`.
- Sub-chart LOC savings and risk tiers; dbt-trino feasibility for the MERGE.
- Debezium ≥3.x fixing DBZ-8780 making the two-signal retry unnecessary.

---

## I. Verdict

This is a coherent platform with a thick layer of accumulated compensation on top. The load-bearing architecture — Strimzi/Debezium → append-only Iceberg Bronze → Spark MERGE Silver → Nessie/Trino, one connector per source DB with per-table sinks, GitOps write path, backfill-with-confirmation — is defensible and the correctness knobs in it (type modes, publication mode, dedup ordering, restorative rollback) were clearly paid for in live incidents and must stay. But roughly a third of the code and templates exists to service three early decisions that were never revisited: (1) transporting the MERGE key through Iceberg identifier fields, which spawned pyiceberg pre-create tooling, a PreSync Job/ConfigMap per layer per table, a dedicated image and a py4j reflection hack; (2) dynamic `_target_table` routing and DirectoryConfigProvider mounts inherited from the shared-sink era, the latter now incoherent with per-source secrets in a way 721 green fake-backed tests cannot see; and (3) hand-templating Trino, Superset, JupyterHub, Nessie and five auxiliaries (≈4.5 kLOC, ≈2.6 kLOC deletable) when maintained charts exist, which in turn justified a 785-line custom render linter and a large share of 392 render assertions. Fix the three decisions and the Console shrinks to the parts that genuinely need a generator; until then the test suite's confidence is mostly confidence in what Python prints, and the next silent-loss finding will again be found live, not in CI.
