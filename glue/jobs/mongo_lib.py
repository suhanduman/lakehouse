"""mongo_lib — Debezium MongoDB ham envelope (ENDS'siz, schemas.enable=false) için saf yardımcılar (pytest).
Spec §5.3: _id Kafka KEY'inden ($oid açılır; silmede tek kaynak), _doc = after (silmede before varsa), tombstone düşer,
after null + op∈{c,u,r} -> karantina. Bronze op sözleşmesi pg ile aynı: I/U/D (F0 S2d)."""
from __future__ import annotations

import json

OPS = {"c": "I", "r": "I", "u": "U", "d": "D"}


def _loads(s):
    try:
        return json.loads(s) if s is not None else None
    except (TypeError, ValueError):
        return None


def mongo_id(key_json: str | None) -> str | None:
    """Debezium mongo key: {"id": "<JSON string>"}; JSON string ObjectId için {"$oid": "..."} olabilir."""
    key = _loads(key_json)
    if not isinstance(key, dict) or "id" not in key:
        return None
    ident = key["id"]
    inner = _loads(ident) if isinstance(ident, str) else ident
    if isinstance(inner, dict) and "$oid" in inner:
        return str(inner["$oid"])
    if inner is None:
        return None
    return str(inner) if not isinstance(inner, (dict, list)) else json.dumps(inner, sort_keys=True)


def parse_envelope(value_json: str | None) -> dict | None:
    v = _loads(value_json)
    if not isinstance(v, dict) or "op" not in v:
        return None
    return {"op": v.get("op"), "ts_ms": v.get("ts_ms"), "after": v.get("after"), "before": v.get("before")}


def bronze_op(op: str) -> str:
    if op not in OPS:
        raise ValueError(f"bilinmeyen Debezium op: {op!r}")
    return OPS[op]


def classify(key_json: str | None, value_json: str | None) -> tuple[str, dict]:
    """("drop", {}) tombstone; ("quarantine", {reason}) bozuk/kimliksiz; ("bronze", {_id,_doc,op,ts_ms})."""
    if value_json is None:
        return "drop", {}
    env = parse_envelope(value_json)
    if env is None or env["op"] not in OPS:
        return "quarantine", {"reason": "bad-envelope"}
    _id = mongo_id(key_json)
    if _id is None:
        return "quarantine", {"reason": "no-key"}
    if not isinstance(env["ts_ms"], int):
        return "quarantine", {"reason": "no-ts"}
    op = bronze_op(env["op"])
    if op != "D" and env["after"] is None:
        return "quarantine", {"reason": "after-null"}
    doc = env["after"] if op != "D" else env["before"]
    return "bronze", {"_id": _id, "_doc": doc, "op": op, "ts_ms": env["ts_ms"]}


def next_offsets(rows) -> dict:
    """[(topic, partition, offset)] -> Spark startingOffsets JSON şekli: bir sonraki okunacak offset (max+1)."""
    out: dict = {}
    for topic, part, off in rows:
        p = out.setdefault(topic, {})
        p[str(part)] = max(p.get(str(part), 0), int(off) + 1)
    return out


def merge_offsets(prev: dict | None, new: dict) -> dict:
    merged = {t: dict(p) for t, p in (prev or {}).items()}
    for t, parts in new.items():
        merged.setdefault(t, {}).update(parts)
    return merged


def offsets_json(offsets: dict) -> str:
    return json.dumps(offsets, sort_keys=True, separators=(",", ":"))
