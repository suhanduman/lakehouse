# Upgrading

Operational runbook for upgrading, downgrading, and rolling back the
Lakehouse platform. This is a runbook, not a design doc — see `README.md`
for install/architecture and `chart/values.yaml` for the full configuration
surface.

## SemVer contract

- **Chart `version`** (`chart/Chart.yaml`) is the Helm *packaging* version —
  it tracks changes to templates, defaults, and the values schema. It follows
  SemVer: breaking values-schema/template changes bump MAJOR, backward-
  compatible additions bump MINOR, fixes bump PATCH.
- **Chart `appVersion`** (`chart/Chart.yaml`) is the *platform release*
  version — the version of the assembled component set (Kafka/Strimzi,
  Nessie, Trino, Iceberg, CNPG Postgres, Apicurio, Kafka Connect, Keycloak,
  Console, ...) this chart version deploys. It is bumped whenever the pinned
  component set in `chart/values.yaml`'s `versions:` block changes in a way
  that changes platform behavior, independent of chart-packaging churn.
- **The git tag `vX.Y.Z` is the single source of truth for a release.** A
  tag corresponds 1:1 to a `chart/Chart.yaml` `version`. Tagging a commit
  and cutting a release are the same act.
- **OCI distribution**: on tag push, the release workflow
  (`.github/workflows/release.yaml`, Product Foundation Phase 5) packages
  and publishes the chart as an OCI artifact to `ghcr.io` (`helm push` to
  `oci://ghcr.io/<owner>/lakehouse/charts`), plus every product image and a
  GitHub Release. See `docs/RELEASING.md` for the full cut-a-release runbook
  and the OCI install command. Installing from a repo checkout
  (`helm install lakehouse ./chart ...`) still works and remains the path
  for local dev / pre-cutover ArgoCD.

## Component version pinning

`chart/values.yaml`'s `versions:` block is **the single source of truth**
for every pinned component image/library version this chart deploys. Do
not maintain a second copy of this table anywhere else — if you're reading
a stale version below, `chart/values.yaml` wins; update this table to match
it in the same PR that changes the block.

The current pinned set:

| Key | Pinned value | Component |
|---|---|---|
| `cnpgPostgresImage` | `ghcr.io/cloudnative-pg/postgresql:16.4` | CNPG Postgres (shared by `pg-nessie` and `pg-apicurio`/`pg-keycloak` clusters) |
| `apicurioImage` | `quay.io/apicurio/apicurio-registry:3.0.6` | Apicurio Registry |
| `strimziApi` | `v1` | Strimzi CRD apiVersion (`kafka.strimzi.io/v1`) — part of the coherent version triple below |
| `kafka` | `4.2.0` | Strimzi `Kafka` CR `spec.kafka.version` — coherent triple |
| `kafkaMetadata` | `4.2-IV1` | Strimzi KRaft `metadataVersion` — coherent triple |
| `nessieImage` | `ghcr.io/projectnessie/nessie:0.104.3` | Project Nessie |
| `trinoImage` | `trinodb/trino:439` | Trino |
| `sparkImageTag` | `1.0` | Internally-built Spark image tag (bronze ingest, Iceberg maintenance, silver-merge) |
| `sparkVersion` | `3.5.1` | Apache Spark |
| `icebergSparkRuntime` | `1.5.2` | `org.apache.iceberg:iceberg-spark-runtime-3.5_2.12` |
| `awsSdkBundle` | `2.25.11` | `software.amazon.awssdk:bundle` / `:url-connection-client` |
| `hadoopAws` | `3.3.4` | `org.apache.hadoop:hadoop-aws` |
| `connectImageTag` | `1.0` | Internally-built Kafka Connect image tag |
| `debezium` | `2.7.3` | `io.debezium` connectors (sqlserver/mongodb, `.Final` suffix appended in-template) |
| `aivenJdbc` | `6.10.0` | Aiven JDBC source connector |
| `mssqlJdbc` | `12.8.1.jre11` | `com.microsoft.sqlserver:mssql-jdbc` (bundled into the aiven-jdbc plugin dir) |
| `postgresqlJdbc` | `42.7.4` | `org.postgresql:postgresql` (bundled into the aiven-jdbc plugin dir) |
| `icebergKafkaConnect` | `1.6.1` | `org.apache.iceberg:iceberg-kafka-connect-runtime` (Iceberg sink plugin) |
| `apicurioConverter` | `2.6.5` | `io.apicurio` Registry Connect converter (`.Final` suffix appended in-template) |
| `zeppelinImage` | `registry.apps.ocp.example.com/lakehouse/zeppelin:1.0` | Apache Zeppelin (placeholder internal-registry ref — override per-cluster) |
| `keycloakImage` | `""` (Keycloak Operator default) | Keycloak |
| `consoleBackendImageTag` | `1.0` | Lakehouse Console backend |
| `consoleFrontendImageTag` | `1.0` | Lakehouse Console frontend |
| `kafkaUiImage` | `ghcr.io/kafbat/kafka-ui:v1.5.0` | Kafka UI |
| `minioImage` | `quay.io/minio/minio:RELEASE.2024-01-16T16-07-38Z` | MinIO (dev/PoC-only, `components.minio`) |
| `mcImage` | `quay.io/minio/mc:RELEASE.2024-01-13T08-44-48Z` | MinIO `mc` client (bucket-init Job) |

**Coherent Strimzi version triple.** `versions.strimziApi` / `versions.kafka`
/ `versions.kafkaMetadata` move together as a single unit and must match the
Strimzi operator actually installed on the target cluster. `chart/values.yaml`
documents the full compatibility reasoning above the `strimziApi` key — read
it before overriding any one of the three in isolation. In short: this
chart's default target is upstream community Strimzi (OLM `community-
operators` catalog, operator 0.51.0), which serves the `Kafka`/
`KafkaNodePool`/`KafkaTopic`/`KafkaUser`/`KafkaConnect`/`KafkaConnector` CRDs
as `kafka.strimzi.io/v1` only — the default triple (`v1` + `4.2.0` +
`4.2-IV1`) reconciles cleanly against it. Before every install/upgrade
against a new target cluster, re-verify:

```bash
oc get crd kafkas.kafka.strimzi.io -o jsonpath='{.spec.versions[*].name}'
oc logs deploy/strimzi-cluster-operator -n <ns> | grep -i "supported"
```

and set the triple to whatever that operator build actually supports.

## Upgrade runbook

### ArgoCD path (prod / OpenShift GitOps)

1. Bump `chart/Chart.yaml`'s `version` (packaging change) and/or
   `appVersion` (component-set change) and tag the commit `vX.Y.Z`.
2. In `gitops/apps/app-of-apps.yaml`, bump the `lakehouse-platform`
   Application's `targetRevision` to the new tag (or chart `version`, once
   the chart is OCI-published in Phase 5).
3. `argocd app sync lakehouse-platform` (or let auto-sync/`selfHeal` pick it
   up). ArgoCD's native Helm support runs `helm template` under the hood —
   no manual `helm upgrade` needed in this path.
4. **Sync-wave ordering matters.** `gitops/apps/app-of-apps.yaml` assigns
   `argocd.argoproj.io/sync-wave` annotations (wave `"0"` for the platform
   Application, wave `"1"` for downstream pipeline Applications) so the
   platform chart fully reconciles before pipeline-repo manifests apply
   against it. Do not remove or reorder these waves without understanding
   the dependency they encode (e.g. Iceberg table creation must precede its
   `KafkaConnector`).

### Plain-Helm path (no ArgoCD)

```bash
helm upgrade lakehouse chart/ -f <your-overlay.yaml> -n <namespace>
```

Layer your sizing-tier overlay (`chart/values-{dev,medium,large}.yaml`)
**before** any per-environment overlay on the command line (`-f
values-<tier>.yaml -f values-<env>.yaml` — last `-f` wins per-key).

### Pre-upgrade checklist

- **Backups current.** CNPG (`pg-nessie`, `pg-apicurio`, `pg-keycloak`)
  ships with barman WAL archiving + scheduled backups to
  `postgres.*.backup.destinationPath` (`s3://backups/...` by convention —
  see `chart/values-prod.example.yaml`). Confirm the most recent backup
  succeeded (`oc get backup -A` / CNPG operator status) before upgrading —
  Nessie's catalog metadata (and Apicurio/Keycloak state) lives only in
  these Postgres clusters.
- Confirm `helm diff` (or a dry-run `helm template` diff against the
  currently-installed manifest) shows only the changes you expect.
- Confirm the target Strimzi/CNPG operator versions on the cluster are
  compatible with the version pins you're about to apply (see the coherent
  triple note above).

### Post-upgrade verification

Run the suite gates against the upgraded chart:

```bash
helm lint chart
helm unittest chart
bash chart/tests/integrity.sh
bash chart/tests/sizing-tiers_test.sh
bash chart/tests/no-customer-identity_test.sh
```

Then a live data round-trip: produce a CDC event through a source connector,
confirm it lands bronze → is merged silver (`silver-merge`
`ScheduledSparkApplication`), and is queryable via Trino
(`SHOW CATALOGS; SELECT ...` against the affected table). A clean upgrade
does not interrupt this pipeline beyond the operators' own brief
rolling-restart windows.

### Rollback

```bash
# Plain Helm:
helm rollback lakehouse <revision> -n <namespace>

# ArgoCD:
argocd app history lakehouse-platform
argocd app rollback lakehouse-platform <history-id>
```

**Iceberg data is forward-compatible with chart rollback.** Rolling the
chart release back only reverts the CR specs Helm/ArgoCD manage
(replica counts, resource requests, image tags, ...); Iceberg table
snapshots already committed to the catalog are untouched — a chart rollback
never rewrites or deletes committed snapshots. Rolling back does **not**
roll back Postgres (Nessie catalog / Apicurio / Keycloak) state; if a bad
upgrade wrote schema changes into those, restore from the CNPG barman
backup instead of relying on chart rollback alone.

## Stateful-upgrade strategy

- **Iceberg format-version is pinned** (via `icebergSparkRuntime`/
  `icebergKafkaConnect` above) and does not silently drift on a chart
  upgrade — bump those keys deliberately when moving to a new Iceberg
  release.
- **Schema evolution is additive-only and automatic.** The silver-merge
  reconcile path (`sparkOperator.silverMerge`, `MERGE INTO
  lakehouse.<ns>.<table>` — see `chart/templates/13-connectors.yaml`'s
  comments on the downstream silver-merge job) accepts new/added columns
  from upstream CDC events without operator intervention. Unsafe type
  changes (e.g. a column narrowing, or an incompatible type swap upstream)
  are **not** silently coerced — the merge job fails loud rather than
  writing corrupt data; this is existing platform behavior, not something
  the chart upgrade path changes.
- **Strimzi (Kafka) and CNPG (Postgres) upgrades are operator-driven rolling
  upgrades.** This chart only changes the `Kafka`/`KafkaNodePool`/`Cluster`
  CR specs (e.g. `versions.kafka`); the Strimzi and CNPG operators perform
  the actual rolling pod replacement and version migration. The chart does
  not, and cannot, upgrade the operators themselves — that's an OLM/Helm
  operator-install concern outside this chart's scope (see
  `operators/README.md`).
- **The coherent Strimzi version triple is a hard constraint on every
  upgrade that touches Kafka.** Changing `versions.kafka` without also
  re-verifying `versions.strimziApi` and `versions.kafkaMetadata` against
  the installed Strimzi operator (see `chart/values.yaml`'s "COHERENT
  STRIMZI VERSION TRIPLE" comment block, quoted in summary above) is the
  single most common way to break a Kafka upgrade — the `Kafka` CR will
  simply fail to reconcile to `Ready=True`.
