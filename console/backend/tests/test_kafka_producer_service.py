from app.services.kafka_producer_service import KafkaProducerService


class FakeProducer:
    def __init__(self, *, raise_on_send=False):
        self.sent = []; self.flushed = False; self.closed = False; self._raise = raise_on_send
    def send(self, topic, key=None, value=None):
        if self._raise: raise OSError("broker down")
        self.sent.append((topic, key, value))
    def flush(self, timeout=None): self.flushed = True
    def close(self, timeout=None): self.closed = True


def test_send_produces_key_and_json_value():
    fake = FakeProducer()
    svc = KafkaProducerService("b:9093", {}, producer_factory=lambda **kw: fake)
    ok = svc.send("debezium-signals", "cdc.pgd", {"type": "execute-snapshot", "data": {"type": "INCREMENTAL"}})
    assert ok is True
    topic, key, value = fake.sent[0]
    assert topic == "debezium-signals"
    assert key == b"cdc.pgd"                      # key serialized to bytes
    import json; assert json.loads(value) == {"type": "execute-snapshot", "data": {"type": "INCREMENTAL"}}
    assert fake.flushed and fake.closed


def test_send_returns_false_on_error():
    fake = FakeProducer(raise_on_send=True)
    svc = KafkaProducerService("b:9093", {}, producer_factory=lambda **kw: fake)
    assert svc.send("t", "k", {"a": 1}) is False


def test_send_returns_false_on_connect_error():
    def boom(**kw): raise OSError("unreachable")
    svc = KafkaProducerService("b:9093", {}, producer_factory=boom)
    assert svc.send("t", "k", {"a": 1}) is False
