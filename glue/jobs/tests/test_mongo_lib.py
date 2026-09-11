import json
import pytest
import mongo_lib as mg

KEY_OID = json.dumps({"id": json.dumps({"$oid": "000000000000000000000001"})})
KEY_STR = json.dumps({"id": json.dumps("cust-42")})
KEY_INT = json.dumps({"id": json.dumps(7)})
AFTER = json.dumps({"_id": {"$oid": "000000000000000000000001"}, "name": "Ada", "tags": ["a", "b"]})


def env(op, after=AFTER, before=None, ts=1757600000000):
    return json.dumps({"op": op, "ts_ms": ts, "after": after, "before": before, "source": {"db": "crm", "collection": "customers"}})


def test_mongo_id_unwraps_oid_and_keeps_scalars():
    assert mg.mongo_id(KEY_OID) == "000000000000000000000001"
    assert mg.mongo_id(KEY_STR) == "cust-42"
    assert mg.mongo_id(KEY_INT) == "7"
    assert mg.mongo_id(None) is None
    assert mg.mongo_id("not json") is None


def test_parse_envelope_shapes():
    e = mg.parse_envelope(env("u"))
    assert e["op"] == "u" and e["ts_ms"] == 1757600000000 and json.loads(e["after"])["name"] == "Ada" and e["before"] is None
    assert mg.parse_envelope(None) is None
    assert mg.parse_envelope("{}") is None            # op yok
    assert mg.parse_envelope("garbage") is None


def test_bronze_op_mapping():
    assert mg.bronze_op("c") == "I" and mg.bronze_op("r") == "I" and mg.bronze_op("u") == "U" and mg.bronze_op("d") == "D"
    with pytest.raises(ValueError):
        mg.bronze_op("x")


def test_classify_paths():
    kind, p = mg.classify(KEY_OID, env("c"))
    assert kind == "bronze" and p["_id"] == "000000000000000000000001" and p["op"] == "I" and json.loads(p["_doc"])["name"] == "Ada"
    kind, p = mg.classify(KEY_OID, env("d", after=None))
    assert kind == "bronze" and p["op"] == "D" and p["_doc"] is None
    kind, p = mg.classify(KEY_OID, env("d", after=None, before=AFTER))
    assert kind == "bronze" and p["op"] == "D" and json.loads(p["_doc"])["name"] == "Ada"
    kind, p = mg.classify(KEY_OID, None)               # tombstone
    assert kind == "drop"
    kind, p = mg.classify(KEY_OID, env("u", after=None))  # after yok ama silme değil -> karantina
    assert kind == "quarantine" and p["reason"] == "after-null"
    kind, p = mg.classify(None, env("c"))              # key yok -> _id yok -> karantina
    assert kind == "quarantine" and p["reason"] == "no-key"
    kind, p = mg.classify(KEY_OID, "garbage")
    assert kind == "quarantine" and p["reason"] == "bad-envelope"
    kind, p = mg.classify(KEY_OID, env("c", ts=None))
    assert kind == "quarantine" and p["reason"] == "no-ts"


def test_offsets_json_shapes():
    nxt = mg.next_offsets([("crm.crm.customers", 0, 4), ("crm.crm.customers", 0, 9), ("crm.crm.customers", 2, 1)])
    assert nxt == {"crm.crm.customers": {"0": 10, "2": 2}}
    merged = mg.merge_offsets({"crm.crm.customers": {"0": 3, "1": 5}}, nxt)
    assert merged == {"crm.crm.customers": {"0": 10, "1": 5, "2": 2}}
    assert mg.merge_offsets(None, nxt) == nxt
    assert json.loads(mg.offsets_json(merged)) == merged


def test_starting_offsets_covers_all_partitions():
    prev = mg.offsets_json({"crm.crm.customers": {"0": 2, "2": 1}})          # partition 1 hiç kayıt görmedi (F3 canlı)
    assert json.loads(mg.starting_offsets(prev, "crm.crm.customers", [0, 1, 2])) == {"crm.crm.customers": {"0": 2, "1": -2, "2": 1}}
    assert mg.starting_offsets(None, "crm.crm.customers", [0, 1]) == "earliest"
    assert mg.starting_offsets(prev, "crm.crm.orders", [0]) == "earliest"
