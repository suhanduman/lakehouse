"""AddSourceOrchestrator: the "add source" multi-step pipeline with rollback.

Step order (exact): secret -> bucket -> namespace -> table -> topic ->
connector -> verify. Each step is idempotent on its own (K8sService.apply_*/
create_secret and S3Service.create_bucket already tolerate "already exists"
— see their docstrings), so re-running add_source against an already-
provisioned source is a cheap no-op all the way through.

"table" (medallion): pre-creates via IcebergService before the connector
ever applies. entity sources (CDC/JDBC) pre-create BOTH layers — Bronze
(`<ns>_raw`, partitioned changelog, no identifier, rawdata warehouse) first,
then Silver (`<ns>`, current-state, identifier/PK) — fail-loud on either.
event sources (e.g. stream+kafka) pre-create Bronze ONLY, as a metadata
skeleton (no identifier/columns required) — no Silver, so silver-merge skips
them; see `SourceSpec.effective_disposition()`.

Rollback: if any step raises, already-completed steps are undone in REVERSE
order via a stack of undo callables, the failure is recorded as a
StepResult, and the whole thing returns AddSourceResult(ok=False) — the
exception is never re-raised to the caller.

Rollback scope (deliberate, see brief): only the Kafka-native objects this
pipeline creates -- secret, topic, connector -- are undone. The S3 bucket and
Trino namespace are NOT deleted on rollback:
  - both are cheap to leave behind (the next retry's create_bucket /
    "CREATE NAMESPACE IF NOT EXISTS" is a no-op against them), and
  - unlike a KafkaConnector/KafkaTopic/Secret, a bucket may already hold
    data (or be about to receive data, or be shared by another in-flight
    source registration), so an automatic delete on a failed later step
    risks destroying storage-layer state for no benefit.
The brief lists the rollback set as "delete connector/topic/secret" — bucket
and namespace are intentionally outside that set.

Verify (spec §4 step 6, "Durum RUNNING doğrula"): the verify step polls
k8s.get_status a bounded number of times (verify_attempts, with verify_delay
seconds between tries via the injected sleep callable) and classifies the
result:
  - state == "RUNNING"  -> step ok
  - state == "FAILED"   -> step fails -> full rollback (connector+topic+secret)
  - still pending / status not yet populated after all attempts -> step ok,
    but detail = "PENDING — reconciliation ongoing" and the connector is
    deliberately NOT rolled back (a freshly-applied Strimzi KafkaConnector
    routinely has an empty status for a moment; tearing down a healthy
    connector just because reconciliation hasn't reported yet would be wrong).
verify_attempts / verify_delay / sleep are constructor-injectable so tests
run with zero delay.

For Camel lanes (a dedicated sink alongside the source, see step 5b below),
verify polls BOTH `ctx["connector_name"]` (the source) and `ctx["sink_name"]`
(the dedicated sink), applying the same RUNNING/FAILED/pending classification
to each independently. This matters because for these lanes the SINK — not
the source — is what actually writes to Bronze (it deserializes the source's
raw JSON and applies the medallion SMTs); a source that reaches RUNNING while
its sink is stuck or FAILED would otherwise report `ok=True` while silently
writing no data. Either one reporting FAILED fails the whole verify step
(full rollback, including the sink); either one still pending after all
attempts (with the other RUNNING or also pending) yields the same
ok=True/PENDING outcome as the single-target case. Lanes without a dedicated
sink (`ctx` has no "sink_name") behave exactly as before — only the source is
polled.

Scope note (Plan B2): any source on the registry's spark-batch lane does not
go through Kafka/medallion pre-create at all. This orchestrator checks
`source_types.get(spec.kind, spec.type).lane` up front and branches on
`descriptor.render_key`:
  - render_key == "" (today: scheduled+mongo — design doc 5.2b, not yet
    wired to a Spark renderer) -> a single clear "validate" StepResult
    failure, before touching any resource. render_service.render_spark_job
    would raise NotImplementedError for this descriptor; this check avoids
    letting that escape as an unhandled crash.
  - render_key set (today: batch-s3) -> a minimal ONE-step pipeline: render
    the Spark CR (`render.render_spark_job`) and apply it
    (`k8s.apply_spark_job`) — no secret/bucket/namespace/table(Bronze/
    Silver)/topic/connector/verify; the Spark job itself owns writing its
    output table.
This is registry-driven (not a hard-coded (kind, type) check), so it
generalizes to any future spark-batch source without an orchestrator change;
kafka-connect-source sources (including stream+kafka) are NOT touched by
either branch.

Lane/disposition-aware steps (Plan B1): the secret step is skipped when
`creds.user`/`creds.password` are both empty (in-cluster Kafka needs no
external creds); the topic step is skipped when the descriptor's
`topic_key == ""` (kafka-ingest consumes an existing topic, creates none);
the table step always pre-creates Bronze but only pre-creates Silver — and
only requires `spec.identifier`/`spec.columns` — when
`spec.effective_disposition() == "entity"`. event sources get a Bronze-only
skeleton (metadata columns + day(__ts_ms) partition; no business columns
required) and are append-only (no Silver, so silver-merge skips them).
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

from app import source_types
from app.models import SourceCredentials, SourceSpec
from app.services.gitops_render import render_pipeline_fileset
from app.services.render_service import BRONZE_NAMESPACE_SUFFIX

VERIFY_PENDING_DETAIL = "PENDING — reconciliation ongoing"


def _resolve_identifier(spec: SourceSpec):
    """Iceberg pre-create için identifier (PK) kolonları. Öncelik: açık
    `spec.identifier` → mongo ise `_id` → scheduled `incrementing_col`. Hiçbiri
    yoksa None (orchestrator fail-loud olur — CDC relational için PK açıkça
    verilmeli)."""
    if spec.identifier:
        return list(spec.identifier)
    if spec.type == "mongo":
        return ["_id"]
    if spec.incrementing_col:
        return [spec.incrementing_col]
    return None


@dataclass
class StepResult:
    name: str
    ok: bool
    detail: str = ""


@dataclass
class AddSourceResult:
    steps: List[StepResult] = field(default_factory=list)
    ok: bool = True


class AddSourceOrchestrator:
    """Runs the add-source pipeline against injected service wrappers.

    `k8s`/`s3`/`trino` are the service-layer wrappers
    (K8sService/S3Service/TrinoService, or hand-rolled fakes in tests).
    `render` is the render_service module (or a fake exposing the same pure
    functions) — it is a plain dependency, not a stateful client, so that
    tests can substitute a fake renderer if needed.

    `verify_attempts`/`verify_delay`/`sleep` tune the verify step's bounded
    poll (see module docstring) and are injectable so tests run at 0 delay.
    """

    def __init__(
        self,
        k8s: Any,
        s3: Any,
        trino: Any,
        render: Any,
        iceberg: Any = None,
        verify_attempts: int = 5,
        verify_delay: float = 2.0,
        sleep: Callable[[float], None] = time.sleep,
        spark_image: str = "",
        s3_secret_name: str = "s3-credentials",
        deploy_mode: str = "direct",
        git_writer: Any = None,
        jobs_bucket: str = "spark-jobs",
        jobs_prefix: str = "jobs",
        s3_endpoint: str = "",
    ) -> None:
        self.k8s = k8s
        self.s3 = s3
        self.trino = trino
        self.render = render
        # IcebergService — CDC hedef tablosunu identifier field ile connector'dan
        # önce pre-create eder. None ise "table" adımı fail-loud olur.
        self.iceberg = iceberg
        self.verify_attempts = max(1, verify_attempts)
        self.verify_delay = verify_delay
        self.sleep = sleep
        # spark-batch lane (Plan B2): render_spark_job/apply_spark_job inputs.
        self.spark_image = spark_image
        self.s3_secret_name = s3_secret_name
        # s3a job-code delivery (2026-07-31-s3a-job-code-delivery Task 4):
        # render_spark_job's mainApplicationFile bucket/prefix + optional
        # Hadoop fs.s3a endpoint -- same spark-batch lane as above.
        self.jobs_bucket = jobs_bucket
        self.jobs_prefix = jobs_prefix
        self.s3_endpoint = s3_endpoint
        # GitOps write-path (Task 4): when deploy_mode == "gitops", add_source/
        # edit_spark_source commit rendered manifests via git_writer instead of
        # applying to the cluster. deploy_mode default "direct" + git_writer
        # default None preserve the pre-existing (B-v1) behavior byte-for-byte.
        self.deploy_mode = deploy_mode
        self.git_writer = git_writer

    def _gitops_write(self, spec: SourceSpec) -> AddSourceResult:
        """gitops mode: render the full pipeline fileset (Task 2) and commit it
        via the injected GitWriter (Task 3) instead of applying anything to
        the cluster. Shared by add_source and edit_spark_source."""
        from app.config import settings

        fileset = render_pipeline_fileset(
            spec, spark_image=self.spark_image, s3_secret_name=self.s3_secret_name,
            namespace=settings.namespace,
            jobs_bucket=self.jobs_bucket, jobs_prefix=self.jobs_prefix,
            s3_endpoint=self.s3_endpoint,
        )
        res = self.git_writer.write_source(spec.source, fileset)
        return AddSourceResult(
            steps=[
                StepResult(
                    name="gitops-commit", ok=res.committed,
                    detail=f"{res.ref} ({len(res.files)} files)",
                )
            ],
            ok=res.committed,
        )

    def edit_spark_source(self, spec: SourceSpec):
        """Re-render the ScheduledSparkApplication from an updated spec and
        apply it (create-or-patch; the CR name is stable across an edit).

        gitops mode: commits the re-rendered fileset via GitWriter instead
        (returns an AddSourceResult; direct mode keeps returning the raw
        apply_spark_job status dict, unchanged). Symmetric with add_source's
        gitops branch: a failed commit is caught and returned as
        AddSourceResult(ok=False) rather than propagating (which would
        otherwise surface as an unhandled 500)."""
        if self.deploy_mode == "gitops":
            try:
                return self._gitops_write(spec)
            except Exception as exc:  # noqa: BLE001 - convert failure into a StepResult
                return AddSourceResult(
                    steps=[StepResult(name="gitops-commit", ok=False, detail=str(exc))],
                    ok=False,
                )
        body = self.render.render_spark_job(
            spec, self.spark_image, self.s3_secret_name,
            jobs_bucket=self.jobs_bucket, jobs_prefix=self.jobs_prefix,
            s3_endpoint=self.s3_endpoint,
        )
        return self.k8s.apply_spark_job(body)

    def add_source(self, spec: SourceSpec, creds: SourceCredentials) -> AddSourceResult:
        if self.deploy_mode == "gitops":
            try:
                return self._gitops_write(spec)
            except Exception as exc:  # noqa: BLE001 - convert failure into a StepResult
                return AddSourceResult(
                    steps=[StepResult(name="gitops-commit", ok=False, detail=str(exc))],
                    ok=False,
                )
        descriptor = source_types.get(spec.kind, spec.type)
        if descriptor.lane == "spark-batch":
            if not descriptor.render_key:
                # No spark renderer wired for this source type yet (e.g.
                # scheduled-mongo) -- reject up front, exactly as Plan B1.
                return AddSourceResult(
                    steps=[
                        StepResult(
                            name="validate",
                            ok=False,
                            detail=(
                                f"{descriptor.id} requires the Spark batch path, not yet "
                                "supported in Console"
                            ),
                        )
                    ],
                    ok=False,
                )
            # Spark-batch source WITH a spark renderer (e.g. batch-s3): a
            # minimal, single-step pipeline -- render the Spark CR and apply
            # it. Deliberately NO secret/bucket/namespace/table(Bronze/Silver)
            # /topic/connector/verify: this lane doesn't touch Kafka or
            # medallion pre-create at all, the Spark job itself owns writing
            # its output table.
            sb_steps: List[StepResult] = []
            try:
                body = self.render.render_spark_job(
                    spec, self.spark_image, self.s3_secret_name,
                    jobs_bucket=self.jobs_bucket, jobs_prefix=self.jobs_prefix,
                    s3_endpoint=self.s3_endpoint,
                )
                self.k8s.apply_spark_job(body)
                sb_steps.append(StepResult(name="spark-job", ok=True, detail=body["metadata"]["name"]))
                return AddSourceResult(steps=sb_steps, ok=True)
            except Exception as exc:  # noqa: BLE001 - convert failure into a StepResult
                sb_steps.append(StepResult(name="spark-job", ok=False, detail=str(exc)))
                return AddSourceResult(steps=sb_steps, ok=False)
        disposition = spec.effective_disposition()

        steps: List[StepResult] = []
        rollback: List[Callable[[], None]] = []
        ctx: Dict[str, Any] = {}

        def run(
            name: str,
            action: Callable[[], Optional[str]],
            undo: Optional[Callable[[], None]] = None,
        ) -> bool:
            try:
                detail = action()
            except Exception as exc:  # noqa: BLE001 - convert any step failure into a StepResult
                steps.append(StepResult(name=name, ok=False, detail=str(exc)))
                return False
            steps.append(StepResult(name=name, ok=True, detail=detail or ""))
            if undo is not None:
                rollback.append(undo)
            return True

        def fail() -> AddSourceResult:
            for undo in reversed(rollback):
                try:
                    undo()
                except Exception:  # noqa: BLE001 - rollback is best-effort
                    pass
            return AddSourceResult(steps=steps, ok=False)

        # 1. secret -- skipped when no external creds are supplied (in-cluster
        # Kafka needs none; external Kafka/DB creds -> secret created as before).
        needs_secret = bool(creds.user) or bool(creds.password)
        if needs_secret:
            secret_name = spec.source

            def _create_secret() -> Optional[str]:
                self.k8s.create_secret(secret_name, {"user": creds.user, "pass": creds.password})
                return None

            if not run("secret", _create_secret, lambda: self.k8s.delete_secret(secret_name)):
                return fail()

        # 2. bucket -- no undo pushed, see module docstring "Rollback scope"
        bucket = self.render.bucket_name(spec.target_ns)

        def _create_bucket() -> Optional[str]:
            self.s3.create_bucket(bucket)
            return None

        if not run("bucket", _create_bucket):
            return fail()

        # 3. namespace -- no undo pushed, see module docstring "Rollback scope"
        def _run_ns_ddl() -> Optional[str]:
            self.trino.run_ddl(self.render.render_namespace_ddl(spec.target_ns, bucket))
            return None

        if not run("namespace", _run_ns_ddl):
            return fail()

        # 3b. table pre-create — BOTH medallion layers, identifier field (PK)
        # ile. Trino DDL DEĞİL; IcebergService (pyiceberg → Nessie REST).
        # Sink'in dinamik-routing auto-create'i identifier koymaz → upsert
        # sessizce append-only'e düşer, her UPDATE duplicate satır bırakır. Bu
        # yüzden tablo(lar), connector'dan (dolayısıyla veriden) önce
        # yaratılmalı: önce Bronze (<ns>_raw — partitioned changelog,
        # identifier YOK, rawdata warehouse), sonra Silver (<ns> —
        # current-state, identifier ile). Boş+idempotent olduğu için undo YOK
        # (namespace/bucket ile aynı "Rollback scope" gerekçesi) — herhangi
        # biri fail-loud olursa "table" adımı fail olur ve
        # secret/bucket/namespace'ten sonra henüz hiçbir topic/connector apply
        # edilmediği için rollback sadece secret'ı geri alır (bkz. undo
        # stack). Şema veya PK eksikse fail-loud → connector asla apply
        # edilmez.
        def _precreate_table() -> Optional[str]:
            if self.iceberg is None:
                raise RuntimeError(
                    "IcebergService enjekte edilmedi — tablo identifier field ile "
                    "pre-create edilemez (upsert append-only'e düşer)"
                )
            # location=None: namespace + konumu yukarıdaki Trino adımı kurdu;
            # iceberg yalnızca tabloyu (identifier field ile) yaratır.
            cols = [c.model_dump() for c in spec.columns] if spec.columns else []
            if disposition == "entity":
                # Validate BEFORE any Iceberg write: a misconfigured entity
                # source must create NOTHING (Plan A invariant).
                # create_table's idempotency check (layer="bronze") compares
                # only identifier fields — always []==[] → trivially equal —
                # and never reconciles columns. If Bronze were created here
                # first with empty/wrong columns, a later resubmit with the
                # correct spec.columns would find the table "already exists"
                # and silently skip adding the business columns -> permanent,
                # silent Bronze/Silver schema divergence. So: check first,
                # write second.
                identifier = _resolve_identifier(spec)
                if not identifier:
                    raise RuntimeError(
                        f"{spec.kind}+{spec.type}: identifier (PK) gerekli — spec.identifier "
                        "verin (sink auto-create identifier koymaz → upsert append-only)"
                    )
                if not spec.columns:
                    raise RuntimeError(
                        "spec.columns gerekli (tablo şeması) — Console form/discovery ile "
                        "doldurun ya da create_iceberg_table.py kullanın"
                    )
                bronze_fq = self.iceberg.create_table(
                    f"{spec.target_ns}{BRONZE_NAMESPACE_SUFFIX}", spec.target_table, cols, [],
                    location=None, layer="bronze",
                )
                silver_fq = self.iceberg.create_table(
                    spec.target_ns, spec.target_table, cols, identifier,
                    location=None, layer="silver",
                )
                return f"{bronze_fq} + {silver_fq} (identifier={identifier})"
            # event/append-only: Bronze skeleton only (columns may be empty —
            # metadata cols + day(__ts_ms) partition come from IcebergService
            # itself; no business-column requirement here). No Silver ->
            # silver-merge skips this source entirely.
            bronze_fq = self.iceberg.create_table(
                f"{spec.target_ns}{BRONZE_NAMESPACE_SUFFIX}", spec.target_table, cols, [],
                location=None, layer="bronze",
            )
            return f"{bronze_fq} (event/append-only, no Silver)"

        if not run("table", _precreate_table):
            return fail()

        # 4. topic -- skipped when the descriptor has no topic_key (kafka-ingest
        # consumes an existing topic; creates none).
        if descriptor.topic_key:
            def _apply_topic() -> Optional[str]:
                ctx["topic_name"] = self.render.topic_name(spec)
                ctx["topic_body"] = self.render.render_kafka_topic(ctx["topic_name"])
                self.k8s.apply_topic(ctx["topic_body"])
                return None

            if not run(
                "topic",
                _apply_topic,
                lambda: self.k8s.delete_topic(ctx.get("topic_body", {}).get("metadata", {}).get("name")),
            ):
                return fail()

        # 5. connector
        def _apply_connector() -> Optional[str]:
            ctx["connector_body"] = self.render.render_connector(spec)
            ctx["connector_name"] = ctx["connector_body"]["metadata"]["name"]
            self.k8s.apply_connector(ctx["connector_body"])
            return None

        if not run(
            "connector",
            _apply_connector,
            lambda: self.k8s.delete_connector(ctx.get("connector_name")),
        ):
            return fail()

        # 5b. dedicated sink -- Camel lanes produce raw JSON at the source; the
        # medallion transform runs on this per-source Iceberg sink (spec
        # 2026-07-30-b3-camel-sink-side-transform). Lanes without a dedicated
        # sink (kafka-ingest IS a sink) skip this.
        if self.render.has_dedicated_sink(spec):
            def _apply_sink() -> Optional[str]:
                ctx["sink_body"] = self.render.render_sink(spec)
                ctx["sink_name"] = ctx["sink_body"]["metadata"]["name"]
                self.k8s.apply_connector(ctx["sink_body"])
                return None
            if not run("sink", _apply_sink,
                       lambda: self.k8s.delete_connector(ctx.get("sink_name"))):
                return fail()

        # 6. verify -- bounded poll for RUNNING (spec §4 step 6); see docstring.
        # For Camel lanes (dedicated sink present via ctx["sink_name"]) both
        # the source AND the sink are polled -- the sink is what actually
        # writes to Bronze, so a stuck/FAILED sink must fail this step too.
        def _poll_until_running(name: Optional[str]) -> str:
            for attempt in range(self.verify_attempts):
                status = self.k8s.get_status(name)
                state = (status or {}).get("connector", {}).get("state")
                if state == "RUNNING":
                    return ""
                if state == "FAILED":
                    raise RuntimeError(f"connector {name} reported FAILED state")
                # pending / status not yet populated -> wait and retry
                if attempt < self.verify_attempts - 1:
                    self.sleep(self.verify_delay)
            # Exhausted attempts without RUNNING or FAILED: treat as ok-but-
            # pending. Do NOT roll back a connector that simply hasn't
            # reconciled to RUNNING yet.
            return VERIFY_PENDING_DETAIL

        def _verify() -> Optional[str]:
            detail = _poll_until_running(ctx.get("connector_name"))
            sink_name = ctx.get("sink_name")
            if sink_name is not None:
                sink_detail = _poll_until_running(sink_name)
                if not detail:
                    detail = sink_detail
            return detail or None

        if not run("verify", _verify):
            return fail()

        return AddSourceResult(steps=steps, ok=True)
