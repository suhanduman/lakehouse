"""Lane-aware source-type registry.

Single source of truth for every ingestion source the platform supports:
one `SourceType` descriptor per (kind, type) pair. `models.SourceSpec`
validation and `render_service` connector/topic dispatch both read this
registry, so adding a source is a descriptor here (+ a render branch in
render_service when its connector shape is genuinely new) rather than
edits scattered across models + render_service + orchestrator.

Pure module: stdlib only, no pydantic/k8s import, so it is safe to import
from both the pydantic models and the pure render functions without cycles.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Tuple

# Ingestion lanes (spec §Architecture). Plan A ships debezium-cdc +
# kafka-connect-source + the (already-existing) spark-batch path; Plan B adds
# more source types on the last two lanes.
LANES = ("debezium-cdc", "kafka-connect-source", "spark-batch")
# Bronze->Silver disposition. entity => merge_cdc upsert; event => append-only
# (no merge). Every source today is entity; Plan B introduces event sources.
DISPOSITIONS = ("entity", "event")


@dataclass(frozen=True)
class SourceType:
    id: str                              # stable slug, e.g. "cdc-mysql"
    kind: str                            # SourceSpec.kind
    type: str                            # SourceSpec.type
    lane: str                            # one of LANES
    disposition: str                     # one of DISPOSITIONS
    required_fields: Tuple[str, ...]     # SourceSpec field names that must be set
    render_key: str                      # key into render_service._RENDERERS ("" => no KafkaConnector)
    topic_key: str                       # key into render_service._TOPICS ("" => no Kafka topic)
    plugin: Tuple[str, ...] = ()         # Strimzi spec.build artifact URLs (Plan B)
    dispositions: Tuple[str, ...] = ()   # allowed set; empty => (disposition,)


REGISTRY: Dict[Tuple[str, str], SourceType] = {}


def register(st: SourceType) -> SourceType:
    assert st.lane in LANES, f"bad lane {st.lane!r}"
    assert st.disposition in DISPOSITIONS, f"bad disposition {st.disposition!r}"
    REGISTRY[(st.kind, st.type)] = st
    return st


def allowed_dispositions(st: "SourceType") -> Tuple[str, ...]:
    return st.dispositions or (st.disposition,)


def get(kind: str, type: str) -> SourceType:
    try:
        return REGISTRY[(kind, type)]
    except KeyError:
        raise KeyError(
            f"unknown source (kind={kind!r}, type={type!r}); "
            f"registered: {sorted(REGISTRY)}"
        )


def all_types() -> List[SourceType]:
    return list(REGISTRY.values())


def type_names() -> Tuple[str, ...]:
    seen, out = set(), []
    for st in REGISTRY.values():
        if st.type not in seen:
            seen.add(st.type)
            out.append(st.type)
    return tuple(out)


# --- the sources that exist today (behavior-preserving registration) ---
register(SourceType("cdc-mssql", "cdc", "mssql", "debezium-cdc", "entity",
                    ("db_host",), "cdc-relational", "cdc-relational"))
register(SourceType("cdc-pg", "cdc", "pg", "debezium-cdc", "entity",
                    ("db_host",), "cdc-relational", "cdc-relational"))
register(SourceType("cdc-mongo", "cdc", "mongo", "debezium-cdc", "entity",
                    ("mongo_uri",), "cdc-mongo", "cdc-mongo"))
register(SourceType("cdc-mysql", "cdc", "mysql", "debezium-cdc", "entity",
                    ("db_host",), "cdc-mysql", "cdc-mysql"))
register(SourceType("scheduled-jdbc-mssql", "scheduled", "mssql", "kafka-connect-source",
                    "entity", ("jdbc_url", "incrementing_col"), "scheduled-jdbc", "scheduled-jdbc"))
register(SourceType("scheduled-jdbc-pg", "scheduled", "pg", "kafka-connect-source",
                    "entity", ("jdbc_url", "incrementing_col"), "scheduled-jdbc", "scheduled-jdbc"))
# scheduled+mongo is a Spark batch CronJob (design doc 5.2b), NOT a KafkaConnector:
# render_key/topic_key are "" so render_service raises a clear NotImplementedError,
# exactly as today, and the orchestrator rejects it up front.
register(SourceType("scheduled-mongo", "scheduled", "mongo", "spark-batch", "entity",
                    ("cron",), "", ""))
# existing-Kafka: consume a topic (in-cluster or external) into its own Bronze
# via a DEDICATED Iceberg sink (see render_service._render_kafka_ingest). event
# disposition only in B1 (append-only; entity/upsert-from-Kafka is a later plan).
register(SourceType("stream-kafka", "stream", "kafka", "kafka-connect-source", "event",
                    (), "kafka-ingest", "", dispositions=("event",)))
