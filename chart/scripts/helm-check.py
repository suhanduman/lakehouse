#!/usr/bin/env python3
"""helm-check.py — Lakehouse Helm render checker.

Validates a rendered Helm manifest (i.e. the output of `helm template`)
against the plan's namespace-awareness + service-reference-closure
constraints (see docs/superpowers/specs/2026-07-18-helm-packaging-design.md
§3.1/§8):

  (a) namespace check     — every doc's `metadata.namespace` (when present)
                             equals --release-namespace.
  (b) no stray templating — the render contains no leftover `{{` / `}}`
                             (i.e. every Helm template directive resolved).
                             Exempts recognized DOWNSTREAM template tokens that
                             are supposed to survive Helm rendering verbatim:
                             PrometheusRule annotations
                             (chart/templates/monitoring/prometheusrules.yaml)
                             legitimately carry literal Prometheus/Alertmanager
                             templating (`{{ $labels.x }}`, `{{ $value }}`,
                             `{{ $externalLabels.x }}`) into the final manifest,
                             for Prometheus — not Helm — to expand at alert-fire
                             time. These are recognized (see
                             `_PROMETHEUS_TEMPLATE_RE`) and skipped. Likewise,
                             the `dbt-profiles` ConfigMap
                             (chart/templates/20-dbt.yaml) carries a literal
                             `{{ env_var('DBT_TRINO_JWT') }}` — dbt's OWN
                             Jinja, not Helm's — into `profiles.yml`, for dbt
                             to expand at `dbt build` time; recognized via
                             `_DBT_ENV_VAR_TEMPLATE_RE` and skipped. Any other
                             leftover Helm directive is still flagged.
  (c) --no-stray <word>   — the literal <word> does not appear anywhere in
                             the render, unless --release-namespace itself
                             equals <word> (typical use: `--no-stray example`
                             to catch hardcoded references to the dev/POC
                             namespace name leaking into a differently-named
                             release).
  (d) --service-closure   — every in-cluster service reference found in the
                             render (Kafka bootstrap servers, `*_URL`/`*uri`
                             hosts (URL-shaped values, e.g.
                             `iceberg.catalog.uri: http://nessie:19120/...`),
                             bare `*host`/`*server` hostname values (e.g.
                             `TRINO_HOST: trino-coordinator`), and
                             `*.svc.cluster.local` FQDNs) resolves to a
                             `kind: Service` that the SAME render actually
                             produces. Dangling references are reported.
                             A CNPG (CloudNativePG) `Cluster` CR also counts
                             as "producing" its operator-managed
                             `<name>-rw`/`-ro`/`-r` Services (see
                             `_collect_produced_services`) — CNPG creates
                             those at reconcile time, not as a literal
                             `kind: Service` in the chart's own render.
                             Likewise, a Strimzi `Kafka` CR counts as
                             producing `<name>-kafka-bootstrap` /
                             `-kafka-brokers` for the same reason.
                             A small `_RECOGNIZED_EXTERNAL_SERVICES` allowlist
                             also exempts fixed, platform-provided
                             cross-namespace services (currently just
                             `thanos-querier`, OpenShift user-workload-
                             monitoring's query endpoint) from dangling
                             classification: the chart cannot and must not
                             produce these itself, so they are treated like
                             a public FQDN or the S3 endpoint —
                             out of closure scope rather than dangling.

                             --service-closure additionally validates
                             (strengthening the original check):
                               - every `kind: Route` whose `spec.to.kind` is
                                 `Service` resolves `spec.to.name` against the
                                 SAME produced-Service set above. There is NO
                                 whitelist/exception list: follow-up-component
                                 Routes with no backing Service in this chart
                                 (superset/jupyter/ai-ui) are instead GATED
                                 OFF by their own `.Values.components.*` flags
                                 (all default false — see
                                 chart/templates/08-routes-tls.yaml's header),
                                 so a dangling Route simply never renders in a
                                 supported install shape. Any Route that DOES
                                 render must resolve to a produced Service or
                                 the render fails closed.
                               - the SAME check, mirrored for `kind: Ingress`
                                 (`platform: vanilla`'s Route equivalent, see
                                 chart/templates/08b-ingress-tls.yaml): every
                                 `spec.rules[].http.paths[].backend.service.
                                 name` resolves against the SAME
                                 produced-Service set. No whitelist here
                                 either — 08b-ingress-tls.yaml mirrors
                                 08-routes-tls.yaml's gating 1:1, so the same
                                 "gated off by default" reasoning applies.
                               - every `kind: NetworkPolicy`'s own
                                 `spec.podSelector` AND every peer
                                 `podSelector` inside `spec.ingress[].from[]`
                                 / `spec.egress[].to[]` resolves (as a label
                                 subset) against a pod-label-set this same
                                 render actually produces — either a literal
                                 Deployment/StatefulSet/DaemonSet pod
                                 template, or a recognized operator-managed
                                 CR's fixed pod-label convention (Strimzi
                                 `Kafka`/`KafkaConnect` `strimzi.io/cluster`+
                                 `strimzi.io/kind`, CNPG `Cluster`
                                 `cnpg.io/cluster`). An empty/omitted
                                 `matchLabels` (selects every pod in the
                                 namespace) always resolves.

Exit code is non-zero if any enabled check fails.

Usage:
    helm-check.py <rendered.yaml> --release-namespace <ns> \\
        [--no-stray example] [--service-closure]

    # or, piped (e.g. from validate.sh --helm):
    helm template chart/ -f chart/values-prod.example.yaml -n example \\
        | helm-check.py - --release-namespace example --service-closure --no-stray example
"""
from __future__ import annotations

import argparse
import re
import sys
from typing import Any, Dict, List, Optional, Set

import yaml

# Matches an in-cluster FQDN anywhere inside a string, e.g.
#   "pg-apicurio-rw.example.svc.cluster.local"
#   "kafka-kafka-bootstrap.lakehouse-test.svc.cluster.local:9093"
# Captures the leading short service name (group 1).
_FQDN_RE = re.compile(
    r"([a-zA-Z0-9](?:[a-zA-Z0-9\-]*[a-zA-Z0-9])?)"
    r"\.(?:[a-zA-Z0-9\-]+\.)*svc\.cluster\.local"
)

# host[:port] extraction from a URL-ish value (scheme://[user:pass@]host[:port]/...).
_URL_HOST_RE = re.compile(r"^[a-zA-Z][a-zA-Z0-9+.\-]*://([^/\s]+)")
_URL_HOST_FALLBACK_RE = re.compile(r"://([^/\s]+)")

# Genuine, unrendered Helm/Go-template action delimiters: `{{` / `}}` immediately
# followed/preceded by whitespace or the `-` whitespace-trim marker, e.g.
# `{{ .Values.x }}`, `{{- include "x" . -}}`. This deliberately does NOT match
# incidental adjacent braces produced by nested YAML flow mappings (this repo's
# manifests are written in a tight flow style with no space before a closing
# brace, e.g. `{matchLabels: {app: trino}}}` — legitimate YAML, not a stray
# template). Distinguishing on the mandatory inner whitespace of real Helm
# syntax avoids false-positiving on every nested `{...}` in the codebase.
_STRAY_TEMPLATE_OPEN_RE = re.compile(r"\{\{-?\s")
_STRAY_TEMPLATE_CLOSE_RE = re.compile(r"\s-?\}\}")

# Legitimate DOWNSTREAM template tokens that must survive Helm rendering
# verbatim and appear as literal `{{ ... }}` in the FINAL manifest — these are
# Prometheus/Alertmanager templating variables, not leftover Helm. Our
# PrometheusRule template (chart/templates/monitoring/prometheusrules.yaml)
# deliberately escapes them through Helm (via `{{` "..." `}}`) so that
# Prometheus, NOT Helm, expands them at alert-fire time — e.g.
# `summary: "connector FAILED ({{ $labels.connector }})"`. A genuine
# unresolved Helm directive never carries a Prometheus variable: Helm actions
# look like `{{ .Values.x }}` / `{{ include ... }}` / `{{- if ... }}`, whereas
# Prometheus templating uses `$labels` / `$value` / `$externalLabels`. This
# predicate matches ONLY the latter (a `{{ ... }}` token whose body contains
# one of those variables — including function-wrapped forms like
# `{{ humanize $value }}` / `{{ $value | humanizePercentage }}`), so
# `check_no_template_syntax` can strip these recognized tokens before scanning
# for real leftover Helm. Mirrors the `_RECOGNIZED_EXTERNAL_SERVICES` allowlist
# idea: teach the checker to recognize legitimate non-Helm content rather than
# blanket-flagging it.
#
# `$labels` / `$externalLabels` are Prometheus-specific names Helm charts never
# use, so they are broadly exempt. `$value`, however, is ALSO a legitimate Helm
# range/with loop variable — but Helm variable declarations always assign with
# `:=` (`{{- range $key, $value := .Values.foo }}`), whereas Prometheus's
# `{{ $value }}` / `{{ humanize $value }}` never do. The `$value` branch below
# therefore carries a `(?![^{}]*:=)` negative lookahead so a Helm assignment is
# NOT scrubbed and still fails closed as stray. `[^{}]*?` bounds keep each match
# inside a single `{{ ... }}` (no cross-token bleed); a leading/trailing `-`
# trim marker is just an ordinary `[^{}]` char and is matched incidentally.
_PROMETHEUS_TEMPLATE_RE = re.compile(
    r"\{\{[^{}]*?\$(?:labels|externalLabels)\b[^{}]*?\}\}"
    r"|\{\{(?![^{}]*:=)[^{}]*?\$value\b[^{}]*?\}\}"
)

# Legitimate DOWNSTREAM template token for dbt's own Jinja, not Helm's —
# `chart/templates/20-dbt.yaml`'s `dbt-profiles` ConfigMap deliberately
# escapes `jwt_token: "{{ env_var('DBT_TRINO_JWT') }}"` through Helm (via the
# `{{ `{{ env_var(...) }}` }}` raw-string trick) so the literal dbt Jinja call
# survives into `profiles.yml`, for dbt itself — not Helm — to expand at
# `dbt build` time (reading the per-run JWT the dbt-build CronJob exports). A
# genuine unresolved Helm directive never calls `env_var(...)` — that's
# dbt-specific Jinja — so this predicate matches only that signature. Same
# idea as `_PROMETHEUS_TEMPLATE_RE` above: teach the checker to recognize
# legitimate non-Helm content rather than blanket-flagging it.
_DBT_ENV_VAR_TEMPLATE_RE = re.compile(r"\{\{[^{}]*?\benv_var\([^{}]*?\}\}")


def _read_input(path: str) -> str:
    if path == "-":
        return sys.stdin.read()
    with open(path, "r", encoding="utf-8") as fh:
        return fh.read()


def _parse_docs(content: str) -> List[Any]:
    docs = []
    for doc in yaml.safe_load_all(content):
        if doc is None:
            continue
        docs.append(doc)
    return docs


# ---------------------------------------------------------------------------
# (a) namespace check
# ---------------------------------------------------------------------------
def check_namespace(docs: List[Any], release_namespace: str) -> List[str]:
    failures = []
    for doc in docs:
        if not isinstance(doc, dict):
            continue
        metadata = doc.get("metadata")
        if not isinstance(metadata, dict):
            continue
        if "namespace" not in metadata:
            continue
        ns = metadata["namespace"]
        if ns != release_namespace:
            kind = doc.get("kind", "<unknown-kind>")
            name = metadata.get("name", "<unknown-name>")
            failures.append(
                f"namespace mismatch: kind={kind} name={name} "
                f"namespace={ns!r} (expected {release_namespace!r})"
            )
    return failures


# ---------------------------------------------------------------------------
# (b) no stray {{ }}
# ---------------------------------------------------------------------------
def check_no_template_syntax(content: str) -> List[str]:
    failures = []
    for lineno, line in enumerate(content.splitlines(), start=1):
        # Strip recognized downstream Prometheus/Alertmanager template tokens
        # (`{{ $labels.x }}`, `{{ $value }}`, `{{ $externalLabels.x }}`) and
        # dbt's own `{{ env_var(...) }}` Jinja token first — these are
        # legitimate literal content in the FINAL render (see
        # `_PROMETHEUS_TEMPLATE_RE` / `_DBT_ENV_VAR_TEMPLATE_RE`), NOT
        # unresolved Helm. Scanning the scrubbed remainder means any OTHER
        # genuine leftover Helm directive on the same line is still detected
        # (we never blanket-skip the whole line).
        scrubbed = _PROMETHEUS_TEMPLATE_RE.sub("", line)
        scrubbed = _DBT_ENV_VAR_TEMPLATE_RE.sub("", scrubbed)
        if _STRAY_TEMPLATE_OPEN_RE.search(scrubbed) or _STRAY_TEMPLATE_CLOSE_RE.search(scrubbed):
            failures.append(f"stray template syntax at line {lineno}: {line.strip()}")
    return failures


# ---------------------------------------------------------------------------
# (c) --no-stray <word>
# ---------------------------------------------------------------------------
def check_no_stray_word(content: str, word: str, release_namespace: str) -> List[str]:
    if release_namespace == word:
        return []
    failures = []
    pattern = re.compile(r"\b" + re.escape(word) + r"\b")
    for lineno, line in enumerate(content.splitlines(), start=1):
        if pattern.search(line):
            failures.append(f"stray literal {word!r} at line {lineno}: {line.strip()}")
    return failures


# ---------------------------------------------------------------------------
# (d) --service-closure
# ---------------------------------------------------------------------------
# Platform-provided cross-namespace services that live OUTSIDE this chart's
# release namespace and are therefore never "produced" by our render, but ARE
# legitimate references (the chart cannot and must not create them). Analogous
# to how a public FQDN or the S3 endpoint is out of closure scope.
# `thanos-querier` is OpenShift user-workload-monitoring's query endpoint
# (fixed namespace `openshift-monitoring`), referenced by the Grafana
# datasource (chart/templates/monitoring/grafana.yaml). See
# docs/superpowers/specs/2026-07-19-monitoring-design.md §4/§9.
_RECOGNIZED_EXTERNAL_SERVICES = {"thanos-querier"}


def _collect_produced_services(docs: List[Any]) -> Set[str]:
    produced = set()
    for doc in docs:
        if not isinstance(doc, dict):
            continue
        metadata = doc.get("metadata")
        name = metadata.get("name") if isinstance(metadata, dict) else None
        if not isinstance(name, str):
            continue

        if doc.get("kind") == "Service":
            produced.add(name)
            continue

        # CNPG (CloudNativePG) `Cluster` CRs never render a literal
        # `kind: Service` in this chart — the CNPG operator reconciles the
        # Cluster CR at apply time and creates the read-write/read-only/
        # read-any Services itself, following its own fixed naming
        # convention (`<cluster-name>-rw` / `-ro` / `-r`). This is exactly
        # the convention `_helpers.tpl`'s canonical-name header already
        # documents for `pg-nessie-rw` / `pg-apicurio-rw` (verified against
        # chart/templates/02-postgres-ha.yaml + 11-apicurio-registry.yaml) and that `lakehouse.svc.pgNessieRw` /
        # `lakehouse.svc.pgApicurioRw` encode. Without this, every
        # CNPG-backed consumer (e.g. Apicurio's JDBC datasource host) would
        # false-fail as "dangling" even though the reference is correct and
        # will resolve once the operator reconciles the Cluster.
        api_version = doc.get("apiVersion")
        if (
            doc.get("kind") == "Cluster"
            and isinstance(api_version, str)
            and api_version.startswith("postgresql.cnpg.io/")
        ):
            produced.add(f"{name}-rw")
            produced.add(f"{name}-ro")
            produced.add(f"{name}-r")
            continue

        # Strimzi `Kafka` CR (KRaft or ZooKeeper mode) never renders a
        # literal `kind: Service` in this chart either — the Strimzi Cluster
        # Operator reconciles the `Kafka` CR at apply time and creates the
        # bootstrap/brokers Services itself, following its own fixed naming
        # convention (`<cluster-name>-kafka-bootstrap` / `-kafka-brokers`).
        # This mirrors the CNPG extension above and is exactly the
        # convention `_helpers.tpl`'s canonical-name header already
        # documents for `kafka-kafka-bootstrap` (verified against
        # chart/templates/03-kafka-strimzi.yaml) and that
        # `lakehouse.svc.kafkaBootstrap` encodes. Without this, every
        # Kafka-bootstrap consumer (Connect, Debezium, Spark, etc.) would
        # false-fail as "dangling" even though the reference is correct and
        # will resolve once the operator reconciles the `Kafka` CR.
        if (
            doc.get("kind") == "Kafka"
            and isinstance(api_version, str)
            and api_version.startswith("kafka.strimzi.io/")
        ):
            produced.add(f"{name}-kafka-bootstrap")
            produced.add(f"{name}-kafka-brokers")
            continue

        # Strimzi `KafkaConnect` CR: same reasoning as the `Kafka` CR above —
        # the Cluster Operator reconciles this CR and creates the REST API
        # Service itself, following its own fixed naming convention
        # (`<connect-name>-connect-api`). Verified against
        # chart/templates/12-kafka-connect.yaml (CR name `connect` ->
        # `connect-connect-api`, the exact name `lakehouse.svc.connect`
        # encodes in _helpers.tpl). Needed starting with the Console
        # component (chart/templates/console/console.yaml), the first
        # consumer to reference this host via a `*_URL`-style key.
        if (
            doc.get("kind") == "KafkaConnect"
            and isinstance(api_version, str)
            and api_version.startswith("kafka.strimzi.io/")
        ):
            produced.add(f"{name}-connect-api")
            continue

        # Keycloak Operator (`k8s.keycloak.org/v2alpha1`) `Keycloak` CR never
        # renders a literal `kind: Service` in this chart either — the
        # Operator reconciles the CR at apply time and creates the
        # `<name>-service` Service itself (fixed Operator convention,
        # verified against chart/templates/10-keycloak.yaml's own Route,
        # which targets `keycloak-service` for a `Keycloak` CR named
        # `keycloak`). Mirrors the CNPG/Strimzi extensions above;
        # `lakehouse.svc.keycloak` in _helpers.tpl encodes this convention.
        if (
            doc.get("kind") == "Keycloak"
            and isinstance(api_version, str)
            and api_version.startswith("k8s.keycloak.org/")
        ):
            produced.add(f"{name}-service")
    return produced


def _bootstrap_refs(key: str, value: str) -> List[str]:
    if "bootstrap" not in key.lower():
        return []
    refs = []
    for entry in value.split(","):
        host = entry.strip().split(":")[0].strip()
        if not host:
            continue
        refs.append(host.split(".")[0])
    return refs


def _url_refs(key: str, value: str) -> List[str]:
    lk = key.lower()
    # `*_url`/`url` (original), plus `*uri`/`uri` (e.g. `iceberg.catalog.uri`,
    # `iceberg.rest-catalog.uri`) -- same URL-host-extraction contract, just a
    # wider set of key-name conventions that carry a URL-shaped value.
    if not (lk.endswith("_url") or lk == "url" or lk.endswith("uri")):
        return []
    m = _URL_HOST_RE.match(value) or _URL_HOST_FALLBACK_RE.search(value)
    if not m:
        return []
    hostport = m.group(1)
    host = hostport.split("@")[-1]  # strip userinfo, if any
    host = host.split(":")[0]
    if "." in host:
        # FQDN-shaped host: only relevant if it's a cluster-internal
        # svc.cluster.local reference, which the generic FQDN scan already
        # catches regardless of key name. Anything else (e.g. a public
        # domain) is not an in-cluster Service reference.
        return []
    return [host]


# Key-name suffixes that conventionally hold a bare in-cluster hostname
# (NOT a URL -- no scheme, no path) -- e.g. `TRINO_HOST: trino-coordinator`.
# `"host"` on its own also matches `_host`/`dbHost`/etc.; likewise `"server"`
# matches `_server`. Deliberately case-insensitive (env-var keys are
# conventionally SCREAMING_SNAKE_CASE, e.g. `TRINO_HOST`).
_HOST_FIELD_KEY_SUFFIXES = ("host", "server")


def _host_field_refs(key: str, value: str) -> List[str]:
    lk = key.lower()
    if not lk.endswith(_HOST_FIELD_KEY_SUFFIXES):
        return []
    value = value.strip()
    # Reject anything that isn't a plain bare hostname: a URL/path (handled
    # by `_url_refs`/`_fqdn_refs` instead), whitespace, or an unresolved
    # runtime placeholder token (e.g. `${ENV:...}`, `{{TOKEN}}`) that isn't a
    # literal hostname at all.
    if not value or "/" in value or " " in value or "$" in value or "{" in value:
        return []
    host = value.split(":")[0].strip()
    if not host or "." in host:
        # Dotted value: either an external FQDN (out of scope, not an
        # in-cluster Service) or a svc.cluster.local FQDN the generic FQDN
        # scan already catches regardless of key name.
        return []
    return [host]


def _fqdn_refs(value: str) -> List[str]:
    return _FQDN_RE.findall(value)


def _collect_consumer_refs(docs: List[Any]) -> Dict[str, List[str]]:
    """Returns {service_name: [example context strings where referenced]}."""
    refs: Dict[str, List[str]] = {}

    def add(name: str, context: str) -> None:
        refs.setdefault(name, [])
        if len(refs[name]) < 3:
            refs[name].append(context)

    def visit_string(key: Optional[str], value: str) -> None:
        for name in _fqdn_refs(value):
            add(name, f"{key}={value!r}" if key else repr(value))
        if key is not None:
            for name in _bootstrap_refs(key, value):
                add(name, f"{key}={value!r}")
            for name in _url_refs(key, value):
                add(name, f"{key}={value!r}")
            for name in _host_field_refs(key, value):
                add(name, f"{key}={value!r}")

    def walk(node: Any, key: Optional[str] = None) -> None:
        if isinstance(node, dict):
            # Common k8s "env"-style shape: {name: FOO_URL, value: "..."}.
            # The interesting key (FOO_URL) is a *sibling* of the string we
            # care about, so handle this shape explicitly in addition to the
            # direct key:value walk below.
            name_field = node.get("name")
            value_field = node.get("value")
            if isinstance(name_field, str) and isinstance(value_field, str):
                visit_string(name_field, value_field)
            for k, v in node.items():
                if isinstance(v, str):
                    visit_string(k, v)
                walk(v, k)
        elif isinstance(node, list):
            for item in node:
                walk(item, key)
        elif isinstance(node, str):
            visit_string(key, node)

    for doc in docs:
        walk(doc)

    return refs


def check_service_closure(docs: List[Any]) -> List[str]:
    produced = _collect_produced_services(docs)
    referenced = _collect_consumer_refs(docs)
    dangling = sorted(set(referenced) - produced - _RECOGNIZED_EXTERNAL_SERVICES)
    failures = []
    for name in dangling:
        examples = "; ".join(referenced[name])
        failures.append(
            f"dangling service reference: {name!r} is referenced but no "
            f"`kind: Service` named {name!r} is produced by this render "
            f"(e.g. {examples})"
        )
    failures.extend(check_route_targets(docs, produced))
    failures.extend(check_ingress_targets(docs, produced))
    failures.extend(check_networkpolicy_peers(docs))
    return failures


# ---------------------------------------------------------------------------
# (d.1) --service-closure extension: Route `.spec.to.name` -> Service
# ---------------------------------------------------------------------------
# No whitelist: any follow-up-component Route whose backing Service this
# chart does not (yet) produce is GATED OFF by its own `.Values.components.*`
# flag (superset/jupyter/ai-ui — all default false, see
# chart/templates/08-routes-tls.yaml). A Route that renders in a supported
# install shape MUST therefore resolve to a produced Service, or the render
# fails closed.
def check_route_targets(docs: List[Any], produced_services: Set[str]) -> List[str]:
    failures = []
    for doc in docs:
        if not isinstance(doc, dict) or doc.get("kind") != "Route":
            continue
        spec = doc.get("spec")
        if not isinstance(spec, dict):
            continue
        to = spec.get("to")
        if not isinstance(to, dict) or to.get("kind") != "Service":
            continue
        name = to.get("name")
        if not isinstance(name, str):
            continue
        if name in produced_services:
            continue
        metadata = doc.get("metadata")
        route_name = metadata.get("name") if isinstance(metadata, dict) else "<unknown-name>"
        failures.append(
            f"dangling Route target: Route {route_name!r} spec.to.name={name!r} "
            f"but no `kind: Service` named {name!r} is produced by this render"
        )
    return failures


# ---------------------------------------------------------------------------
# (d.1b) --service-closure extension: Ingress backend.service.name -> Service
# ---------------------------------------------------------------------------
# Mirrors check_route_targets above for `platform: vanilla`'s Route
# equivalent (chart/templates/08b-ingress-tls.yaml). Same reasoning, same "no
# whitelist" stance: 08b-ingress-tls.yaml gates every Ingress on the SAME
# `.Values.components.*` flag as its Route counterpart in
# 08-routes-tls.yaml/10-keycloak.yaml/console/*.yaml, so a follow-up-component
# Ingress with no backing Service (superset/jupyter/ai-ui) is likewise gated
# off by default rather than whitelisted here.
def check_ingress_targets(docs: List[Any], produced_services: Set[str]) -> List[str]:
    failures = []
    for doc in docs:
        if not isinstance(doc, dict) or doc.get("kind") != "Ingress":
            continue
        spec = doc.get("spec")
        if not isinstance(spec, dict):
            continue
        metadata = doc.get("metadata")
        ingress_name = metadata.get("name") if isinstance(metadata, dict) else "<unknown-name>"
        for rule in spec.get("rules") or []:
            if not isinstance(rule, dict):
                continue
            http = rule.get("http")
            if not isinstance(http, dict):
                continue
            for path in http.get("paths") or []:
                if not isinstance(path, dict):
                    continue
                backend = path.get("backend")
                if not isinstance(backend, dict):
                    continue
                service = backend.get("service")
                if not isinstance(service, dict):
                    continue
                name = service.get("name")
                if not isinstance(name, str):
                    continue
                if name in produced_services:
                    continue
                failures.append(
                    f"dangling Ingress target: Ingress {ingress_name!r} "
                    f"backend.service.name={name!r} but no `kind: Service` "
                    f"named {name!r} is produced by this render"
                )
    return failures


# ---------------------------------------------------------------------------
# (d.2) --service-closure extension: NetworkPolicy peer podSelector resolution
# ---------------------------------------------------------------------------
def _collect_produced_pod_label_sets(docs: List[Any]) -> List[Dict[str, str]]:
    """Returns every pod-label-set this render will actually produce pods
    under — either literally (a Deployment/StatefulSet/DaemonSet pod
    template) or via a recognized operator CR's fixed pod-labeling
    convention (mirrors `_collect_produced_services`'s CNPG/Strimzi
    reasoning: the operator, not this chart, stamps these labels onto the
    pods it reconciles, but the convention is fixed and verifiable)."""
    produced: List[Dict[str, str]] = []
    for doc in docs:
        if not isinstance(doc, dict):
            continue
        kind = doc.get("kind")
        metadata = doc.get("metadata")
        name = metadata.get("name") if isinstance(metadata, dict) else None
        api_version = doc.get("apiVersion")

        if kind in ("Deployment", "StatefulSet", "DaemonSet", "ReplicaSet"):
            spec = doc.get("spec")
            labels = None
            if isinstance(spec, dict):
                template = spec.get("template")
                if isinstance(template, dict):
                    tmeta = template.get("metadata")
                    if isinstance(tmeta, dict) and isinstance(tmeta.get("labels"), dict):
                        labels = tmeta["labels"]
                if labels is None:
                    selector = spec.get("selector")
                    if isinstance(selector, dict) and isinstance(selector.get("matchLabels"), dict):
                        labels = selector["matchLabels"]
            if isinstance(labels, dict) and labels:
                produced.append({str(k): str(v) for k, v in labels.items()})
            continue

        if not isinstance(name, str) or not isinstance(api_version, str):
            continue

        # Strimzi `KafkaConnect` CR: Cluster Operator labels every Connect
        # pod `strimzi.io/cluster: <cr-name>` + `strimzi.io/kind: KafkaConnect`
        # (verified against the `console-backend-egress` NetworkPolicy in
        # chart/templates/console/console.yaml, which already targets this
        # exact label pair for the `connect` KafkaConnect CR).
        if kind == "KafkaConnect" and api_version.startswith("kafka.strimzi.io/"):
            produced.append({"strimzi.io/cluster": name, "strimzi.io/kind": "KafkaConnect"})
            continue

        # Strimzi `Kafka` CR: broker/controller pods are labeled
        # `strimzi.io/cluster: <cr-name>` + `strimzi.io/kind: Kafka` (same
        # Cluster Operator convention as KafkaConnect above).
        if kind == "Kafka" and api_version.startswith("kafka.strimzi.io/"):
            produced.append({"strimzi.io/cluster": name, "strimzi.io/kind": "Kafka"})
            continue

        # CNPG `Cluster` CR: the operator labels every instance pod
        # `cnpg.io/cluster: <cr-name>` (fixed CNPG convention, same
        # reasoning as `_collect_produced_services`'s `-rw`/`-ro`/`-r`
        # Service extension for the same CR kind).
        if kind == "Cluster" and api_version.startswith("postgresql.cnpg.io/"):
            produced.append({"cnpg.io/cluster": name})
            continue

    return produced


def _pod_label_selector_resolves(
    match_labels: Optional[Dict[str, Any]], produced: List[Dict[str, str]]
) -> bool:
    if not match_labels:
        # {} (or omitted) selects every pod in the namespace — always
        # resolves (there is always at least the release's own workloads).
        return True
    return any(
        all(produced_set.get(k) == v for k, v in match_labels.items())
        for produced_set in produced
    )


def check_networkpolicy_peers(docs: List[Any]) -> List[str]:
    produced = _collect_produced_pod_label_sets(docs)
    failures = []

    def describe(np_name: str, where: str, match_labels: Dict[str, Any]) -> str:
        return (
            f"dangling NetworkPolicy peer: NetworkPolicy {np_name!r} {where} "
            f"podSelector.matchLabels={match_labels!r} does not resolve to any "
            f"pod-label-set produced by this render"
        )

    for doc in docs:
        if not isinstance(doc, dict) or doc.get("kind") != "NetworkPolicy":
            continue
        metadata = doc.get("metadata")
        np_name = metadata.get("name") if isinstance(metadata, dict) else "<unknown-name>"
        spec = doc.get("spec")
        if not isinstance(spec, dict):
            continue

        own_selector = spec.get("podSelector")
        if isinstance(own_selector, dict):
            own_labels = own_selector.get("matchLabels")
            if isinstance(own_labels, dict) and not _pod_label_selector_resolves(own_labels, produced):
                failures.append(describe(np_name, "spec.podSelector", own_labels))

        for direction, peer_key in (("ingress", "from"), ("egress", "to")):
            rules = spec.get(direction)
            if not isinstance(rules, list):
                continue
            for rule in rules:
                if not isinstance(rule, dict):
                    continue
                peers = rule.get(peer_key)
                if not isinstance(peers, list):
                    continue
                for peer in peers:
                    if not isinstance(peer, dict):
                        continue
                    peer_selector = peer.get("podSelector")
                    if not isinstance(peer_selector, dict):
                        continue
                    peer_labels = peer_selector.get("matchLabels")
                    if isinstance(peer_labels, dict) and not _pod_label_selector_resolves(peer_labels, produced):
                        failures.append(
                            describe(np_name, f"spec.{direction}[].{peer_key}[]", peer_labels)
                        )

    return failures


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "rendered_file",
        nargs="?",
        default="-",
        help="Path to a rendered Helm manifest YAML file, or '-' to read stdin (default: -).",
    )
    parser.add_argument(
        "--release-namespace",
        required=True,
        help="Expected namespace for every resource in the render.",
    )
    parser.add_argument(
        "--no-stray",
        metavar="WORD",
        default=None,
        help="Fail if this literal word appears anywhere in the render "
        "(unless --release-namespace equals WORD).",
    )
    parser.add_argument(
        "--service-closure",
        action="store_true",
        help="Fail if any consumer-side service reference is not produced "
        "by a kind: Service in this same render.",
    )
    args = parser.parse_args(argv)

    content = _read_input(args.rendered_file)

    failures: List[str] = []

    # (b) must run on raw text regardless of whether YAML parsing succeeds,
    # since stray `{{ }}` usually means the file *isn't* valid YAML.
    failures.extend(check_no_template_syntax(content))

    try:
        docs = _parse_docs(content)
    except yaml.YAMLError as exc:
        print(f"FAIL: could not parse rendered YAML: {exc}", file=sys.stderr)
        for f in failures:
            print(f"FAIL: {f}", file=sys.stderr)
        return 1

    failures.extend(check_namespace(docs, args.release_namespace))

    if args.no_stray:
        failures.extend(check_no_stray_word(content, args.no_stray, args.release_namespace))

    if args.service_closure:
        failures.extend(check_service_closure(docs))

    if failures:
        for f in failures:
            print(f"FAIL: {f}", file=sys.stderr)
        print(f"helm-check: FAIL ({len(failures)} issue(s))", file=sys.stderr)
        return 1

    print(
        f"helm-check: OK ({len(docs)} doc(s), namespace={args.release_namespace!r}"
        f"{', no-stray=' + args.no_stray if args.no_stray else ''}"
        f"{', service-closure' if args.service_closure else ''})"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
