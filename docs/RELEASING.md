# Releasing

Runbook for cutting a Lakehouse release. This is the Phase 5 release
workflow referenced by `docs/UPGRADING.md`'s SemVer contract and OCI
distribution note — see that doc for the version-numbering rules
(`chart/Chart.yaml`'s `version`/`appVersion`) this runbook assumes.

## What a release publishes

Pushing a `vX.Y.Z` tag triggers `.github/workflows/release.yaml`, which:

1. **Builds and pushes every product image to ghcr.io**, tagged both
   `X.Y.Z` and `latest`:
   - `ghcr.io/<owner>/lakehouse/connect-lakehouse` (`images/connect/Dockerfile`)
   - `ghcr.io/<owner>/lakehouse/iceberg-tools` (`images/iceberg-tools/Dockerfile`)
   - `ghcr.io/<owner>/lakehouse/console-backend` (`console/backend/Dockerfile`)
   - `ghcr.io/<owner>/lakehouse/console-frontend` (`console/frontend/Dockerfile`)

   (`<owner>` = the GitHub org/user the repo lives under.) The pinned
   internally-built Spark image (`versions.sparkImageTag` in
   `chart/values.yaml`) is now first-party too — built from
   `images/spark-py/Dockerfile` (same baked-jars, no-runtime-Ivy pattern as
   `images/connect/Dockerfile`) — but this workflow does not build/push it
   yet (its `images` job matrix does not include `spark-py`); until that
   wiring lands, `spark-py` is still produced out-of-band from that
   Dockerfile by whatever registry pipeline the target cluster already uses.
   See "Known limitation" below.

2. **Packages and pushes the Helm chart as an OCI artifact**:
   `helm package chart --version X.Y.Z --app-version X.Y.Z`, then
   `helm push` to `oci://ghcr.io/<owner>/lakehouse/charts`.

3. **Cuts a GitHub Release** for the tag, with auto-generated release notes
   and the packaged chart `.tgz` attached.

All of this happens in one workflow, `.github/workflows/release.yaml`. Every
image build reuses the GHA buildx cache that `e2e.yaml`'s connect-image job
keeps warm, so even the from-source Apache Iceberg compile
(`images/connect/Dockerfile`, ~20 min cold) is a fast cache replay.

## Cutting a release

1. Bump `chart/Chart.yaml`: `version` (packaging change) and/or
   `appVersion` (component-set change) — see `docs/UPGRADING.md`'s SemVer
   contract for which one to bump. If the component set changed, also
   update the pinned-versions table in `docs/UPGRADING.md` in the same PR.
2. Merge that change to `main` via the normal PR gate (`ci.yaml`).
3. Tag the merged commit and push the tag:
   ```bash
   git tag vX.Y.Z
   git push origin vX.Y.Z
   ```
   The tag is the single source of truth for a release — pushing it is the
   act of cutting one. There is no separate "publish" button.
4. Watch the `release` workflow run (`gh run watch` or the Actions tab).
   On success: four images are live on ghcr.io, the chart is published as
   an OCI artifact, and a GitHub Release exists for `vX.Y.Z`.

## Validating without publishing (`dry_run`)

Trigger the same workflow manually to validate the whole pipeline —
Dockerfile builds, chart packaging — without pushing any image, OCI chart,
or GitHub Release:

```bash
gh workflow run release.yaml -f dry_run=true --ref <branch-or-tag>
```

`dry_run` defaults to `true` on `workflow_dispatch`, so `-f dry_run=true` is
just being explicit. In this mode:
- images build with `push: false` (validates the Dockerfile builds, no
  registry write)
- the chart is packaged but not OCI-pushed — the `.tgz` is uploaded as a
  workflow artifact instead, downloadable from the run's summary page
- the GitHub Release job is skipped entirely

**Publishing for real always requires an actual `vX.Y.Z` tag ref**, even
under `workflow_dispatch` — running `workflow_dispatch` against a branch can
never publish, regardless of the `dry_run` value, because there's no tag to
derive a version from. The only way to get a real publish out of
`workflow_dispatch` is running it with `--ref <existing tag>` (e.g.
re-running a failed publish for a tag that already exists).

## Installing a released version

**Via Helm directly**, from the OCI chart:

```bash
helm install lakehouse oci://ghcr.io/<owner>/lakehouse/charts/lakehouse \
  --version X.Y.Z \
  -f chart/values-<tier>.yaml \
  -n <namespace>
```

The release workflow bakes release-coherent defaults into the packaged
chart before publishing it (see "Image/chart coherence" below), so
`connect-lakehouse`, `console-backend`, and `console-frontend` already
resolve to the exact ghcr.io images this same release published, at this
exact version — no `--set` needed for those three.

> **Known limitation — `spark-py` is first-party but not yet in this
> workflow's build matrix.** `spark-py` is now a first-party image, built
> from `images/spark-py/Dockerfile` (a stock Spark base with the Iceberg/AWS/
> Kafka/hadoop-aws jars baked in at build time — no runtime Ivy resolution;
> the versions baked MUST match `chart/values.yaml`'s `versions:` block —
> same convention as `images/connect/Dockerfile`). This release, however,
> does **not yet** build or publish it (the `images` job's matrix in
> `.github/workflows/release.yaml` does not include `spark-py` — tracked as
> an Ingestion-productization follow-up). The packaged chart's
> `global.imageRegistry` is baked to `ghcr.io/<owner>/lakehouse`, so a
> **plain default install of the released OCI chart will `ImagePullBackOff`**
> on every Spark `SparkApplication`/`ScheduledSparkApplication` (silver-merge,
> maintenance, `nginx-ingest`) — they resolve to
> `ghcr.io/<owner>/lakehouse/spark-py:1.0`, an image that was never pushed.
> Before installing, build `images/spark-py/Dockerfile` yourself (its default
> build args already match `chart/values.yaml`'s `versions:` block) and either:
> - push it to `ghcr.io/<owner>/lakehouse/spark-py:<your-tag>` (the same
>   registry this release already wired) and override only the tag:
>   ```bash
>   --set versions.sparkImageTag=<your-tag>
>   ```
> - or mirror it to a registry of your choosing and override both — note
>   this repoints `global.imageRegistry` for *every* image, so also mirror
>   `connect-lakehouse`/`console-backend`/`console-frontend` there (or set
>   their tags/registry back to the ghcr.io release build individually):
>   ```bash
>   --set global.imageRegistry=<your-registry>/lakehouse \
>   --set versions.sparkImageTag=<your-tag>
>   ```
>
> `iceberg-tools` is BYO the same way (see "Image/chart coherence" below
> for why it's excluded from the release's coherence contract entirely).

Because ghcr.io packages are private for a private repo, pulling requires
auth: `helm registry login ghcr.io -u <user> -p <token>` (a token with
`read:packages` scope) before the `helm install`/`helm pull`.

**Via ArgoCD**, bump the `lakehouse-platform` Application's `targetRevision`
in `gitops/apps/app-of-apps.yaml` to the new chart version once the
`Application` spec's `source` points at the OCI chart repo (see
`docs/UPGRADING.md`'s ArgoCD upgrade path for the full sync-wave-aware
procedure) — until that cutover, the ArgoCD path continues to track a git
ref against a repo checkout, not the OCI artifact.

**Before that cutover, or for local dev**, install straight from a repo
checkout as always: `helm install lakehouse ./chart ...`. Nothing about this
workflow changes that path — it's additive. Unlike the OCI chart above, a
git-source install does **not** get the release-coherent rewrite (that only
happens inside the release workflow's ephemeral, workspace-only copy) — it
still resolves images against `chart/values.yaml`'s in-repo placeholders
(`global.imageRegistry: image-registry.openshift-image-registry.svc:5000/
lakehouse`, per-component tags `"1.0"`). To point a git-source install at
this release's actual published images, override them explicitly:

```bash
helm install lakehouse ./chart \
  --set global.imageRegistry=ghcr.io/<owner>/lakehouse \
  --set versions.connectImageTag=X.Y.Z \
  --set versions.consoleBackendImageTag=X.Y.Z \
  --set versions.consoleFrontendImageTag=X.Y.Z \
  ...
```

## Image/chart coherence

`helm package` only rewrites `Chart.yaml`'s `version`/`appVersion` — it does
not touch `values.yaml`. Left alone, the packaged chart's *defaults* would
still resolve images at `chart/values.yaml`'s in-repo placeholders, not the
ghcr.io images this workflow just built. To keep the published OCI chart
self-consistent, the release workflow rewrites its checked-out (ephemeral,
never-committed) copy of `chart/values.yaml` immediately before
`helm package`, setting `global.imageRegistry` to
`ghcr.io/<owner>/lakehouse` and `versions.connectImageTag` /
`versions.consoleBackendImageTag` / `versions.consoleFrontendImageTag` to
the release version — on both the dry-run and real-publish paths, so a
dry-run artifact is representative. `chart/values.yaml` in git is never
modified by this. An OpenShift customer who mirrors images to an internal
registry still overrides `global.imageRegistry` (and the tags) themselves
exactly as before — ghcr.io is simply the correct *default* for the
artifact this workflow itself publishes.

**This coherence contract covers exactly three images**:
`connect-lakehouse`, `console-backend`, `console-frontend` — the ones this
workflow actually builds and pushes. It deliberately does **not** touch
`versions.sparkImageTag` (`spark-py`, rendered by
`chart/templates/05-spark-operator.yaml` and
`chart/templates/14-nginx-ingest.yaml`): since `global.imageRegistry` is
rewritten globally but `sparkImageTag` is left at its repo default (`"1.0"`),
the packaged chart still resolves `spark-py` under the *new* ghcr.io
registry at the *old* tag — an image that was never built there. This is
the "Known limitation" called out under "Installing a released version"
above, not an oversight in the bake step: fixing it means either building
`spark-py` in this workflow (a separate design decision, not yet made) or
requiring every install to override it, which the docs above do.

`images/iceberg-tools` is published to ghcr.io by this workflow but is
**not** part of this coherence contract either — no chart template
references its image; it's only consumed by the illustrative
`gitops/pipeline-template/example-*.yaml` manifests, which use their own
illustrative image refs (e.g. the OpenShift internal registry), independent
of anything this chart or release workflow controls. Its ghcr.io publish is
still useful as a versioned artifact, just not something the chart's
defaults reason about.

## Upgrade testing (not part of this pipeline)

This workflow validates that a release *builds and publishes*. It does
**not** run an upgrade test against a live cluster (installing an old
version, then upgrading in place) — that needs a real cluster and doesn't
fit the free private-runner constraint any more than `e2e.yaml`'s Stage 2
does. Validate upgrades using `docs/UPGRADING.md`'s "Post-upgrade
verification" gates and live data round-trip against a real (OpenShift or
kind) cluster before/after bumping the tag consumers point at.
