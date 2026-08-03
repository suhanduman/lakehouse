from app.services.kafka_consumer_service import KafkaConsumerService


class _TP:  # stand-in for kafka.TopicPartition
    def __init__(self, topic, partition):
        self.topic, self.partition = topic, partition
    def __hash__(self): return hash((self.topic, self.partition))
    def __eq__(self, o): return (self.topic, self.partition) == (o.topic, o.partition)


class _Rec:
    def __init__(self, value, headers, ts=1730000000000):
        self.value = value; self.headers = headers; self.timestamp = ts


class FakeConsumer:
    def __init__(self, *, partitions, begin, end, records=None):
        self._partitions = partitions      # e.g. [0, 1]
        self._begin = begin; self._end = end  # {partition: offset}
        self._records = records or []
        self.assigned = []; self.sought = []; self.closed = False
    def partitions_for_topic(self, topic): return set(self._partitions)
    def assign(self, tps): self.assigned = list(tps)
    def beginning_offsets(self, tps): return {tp: self._begin[tp.partition] for tp in tps}
    def end_offsets(self, tps): return {tp: self._end[tp.partition] for tp in tps}
    def seek(self, tp, offset): self.sought.append((tp.partition, offset))
    def poll(self, timeout_ms=None, max_records=None):
        return {("tp", 0): list(self._records)} if self._records else {}
    def close(self): self.closed = True


def _svc(fake):
    return KafkaConsumerService("b:9093", {}, consumer_factory=lambda **kw: fake, tp_cls=_TP)


def test_topic_record_count_sums_end_minus_begin():
    fake = FakeConsumer(partitions=[0, 1], begin={0: 5, 1: 0}, end={0: 12, 1: 3})
    assert _svc(fake).topic_record_count("t") == (12 - 5) + (3 - 0)  # 10
    assert fake.closed is True


def test_topic_record_count_none_when_topic_absent():
    fake = FakeConsumer(partitions=[], begin={}, end={})
    assert _svc(fake).topic_record_count("missing") is None


def test_topic_record_count_none_on_connection_error():
    def boom(**kw): raise OSError("unreachable")
    svc = KafkaConsumerService("b:9093", {}, consumer_factory=boom, tp_cls=_TP)
    assert svc.topic_record_count("t") is None


def test_read_last_parses_dlq_headers_and_truncates_value():
    headers = [
        ("__connect.errors.exception.class.name", b"org.apache.kafka.connect.errors.DataException"),
        ("__connect.errors.exception.message", b"boom"),
        ("__connect.errors.topic", b"orders"),
        ("__connect.errors.partition", b"2"),
        ("__connect.errors.offset", b"99"),
    ]
    rec = _Rec(value=b"x" * 1000, headers=headers)
    fake = FakeConsumer(partitions=[0], begin={0: 0}, end={0: 1}, records=[rec])
    out = _svc(fake).read_last("t", 50)
    assert len(out) == 1
    r = out[0]
    assert r["error_class"].endswith("DataException")
    assert r["error_message"] == "boom"
    assert (r["source_topic"], r["source_partition"], r["source_offset"]) == ("orders", 2, 99)
    assert len(r["value_preview"]) <= 512


def test_read_last_empty_when_no_records():
    fake = FakeConsumer(partitions=[0], begin={0: 0}, end={0: 0})
    assert _svc(fake).read_last("t", 50) == []
