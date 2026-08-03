"""KafkaConsumerService: bounded, on-demand reads of a DLQ topic (count via
offset metadata; last-N records with Kafka Connect error headers parsed).

`consumer_factory` is injected (real: kafka.KafkaConsumer; tests: a fake), so
the offset math / header parsing is unit-tested without a live broker. Every
method builds a consumer, uses it, and closes it -- no standing consumer.
Never raises on a broker/topic problem: returns None / [] and lets the caller
degrade.
"""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional

_ERR = "__connect.errors."
_VALUE_PREVIEW_MAX = 512


def _h(headers, key: str) -> Optional[str]:
    for k, v in headers or []:
        if k == key:
            return v.decode("utf-8", "replace") if isinstance(v, (bytes, bytearray)) else str(v)
    return None


class KafkaConsumerService:
    def __init__(self, bootstrap: str, conn_kwargs: Dict[str, Any],
                 consumer_factory: Optional[Callable[..., Any]] = None,
                 tp_cls: Optional[Callable[[str, int], Any]] = None) -> None:
        self.bootstrap = bootstrap
        self.conn_kwargs = conn_kwargs
        if consumer_factory is None or tp_cls is None:
            from kafka import KafkaConsumer, TopicPartition  # lazy: import only in prod path
            consumer_factory = consumer_factory or (lambda **kw: KafkaConsumer(**kw))
            tp_cls = tp_cls or TopicPartition
        self._factory = consumer_factory
        self._tp = tp_cls

    def _consumer(self):
        return self._factory(bootstrap_servers=self.bootstrap, enable_auto_commit=False,
                             group_id=None, **self.conn_kwargs)

    def topic_record_count(self, topic: str) -> Optional[int]:
        try:
            c = self._consumer()
        except Exception:  # noqa: BLE001 -- broker unreachable -> degrade
            return None
        try:
            parts = c.partitions_for_topic(topic)
            if not parts:
                return None
            tps = [self._tp(topic, p) for p in parts]
            begin = c.beginning_offsets(tps)
            end = c.end_offsets(tps)
            return sum(end[tp] - begin[tp] for tp in tps)
        except Exception:  # noqa: BLE001
            return None
        finally:
            try: c.close()
            except Exception: pass

    def read_last(self, topic: str, limit: int) -> List[Dict[str, Any]]:
        if limit <= 0:
            return []
        try:
            c = self._consumer()
        except Exception:  # noqa: BLE001
            return []
        try:
            parts = c.partitions_for_topic(topic)
            if not parts:
                return []
            tps = [self._tp(topic, p) for p in parts]
            c.assign(tps)
            end = c.end_offsets(tps)
            begin = c.beginning_offsets(tps)
            per = max(1, limit // len(tps))
            for tp in tps:
                c.seek(tp, max(begin[tp], end[tp] - per))
            batches = c.poll(timeout_ms=2000, max_records=limit)
            out: List[Dict[str, Any]] = []
            for recs in (batches or {}).values():
                for rec in recs:
                    val = getattr(rec, "value", None)
                    preview = (val.decode("utf-8", "replace") if isinstance(val, (bytes, bytearray))
                               else ("" if val is None else str(val)))[:_VALUE_PREVIEW_MAX]
                    sp = _h(rec.headers, _ERR + "partition")
                    so = _h(rec.headers, _ERR + "offset")
                    out.append({
                        "ts": getattr(rec, "timestamp", None),
                        "error_class": _h(rec.headers, _ERR + "exception.class.name"),
                        "error_message": _h(rec.headers, _ERR + "exception.message"),
                        "source_topic": _h(rec.headers, _ERR + "topic"),
                        "source_partition": int(sp) if sp is not None and sp.isdigit() else None,
                        "source_offset": int(so) if so is not None and so.isdigit() else None,
                        "value_preview": preview,
                    })
            return out[-limit:]
        except Exception:  # noqa: BLE001
            return []
        finally:
            try: c.close()
            except Exception: pass

    def read_last_values(self, topic: str, limit: int) -> List[Dict[str, Any]]:
        """Last <=limit records' FULL decoded values + ts (untruncated), for
        JSON control messages (Debezium notifications) -- unlike `read_last`
        (DLQ-specific: truncates to `value_preview` and parses Connect error
        headers), this returns the whole value so callers can `json.loads`
        it themselves. Never raises -> []."""
        if limit <= 0:
            return []
        try:
            c = self._consumer()
        except Exception:  # noqa: BLE001
            return []
        try:
            parts = c.partitions_for_topic(topic)
            if not parts:
                return []
            tps = [self._tp(topic, p) for p in parts]
            c.assign(tps)
            end = c.end_offsets(tps)
            begin = c.beginning_offsets(tps)
            per = max(1, limit // len(tps))
            for tp in tps:
                c.seek(tp, max(begin[tp], end[tp] - per))
            batches = c.poll(timeout_ms=2000, max_records=limit)
            out: List[Dict[str, Any]] = []
            for recs in (batches or {}).values():
                for rec in recs:
                    key = getattr(rec, "key", None)
                    val = getattr(rec, "value", None)
                    out.append({
                        "ts": getattr(rec, "timestamp", None),
                        "key": (key.decode("utf-8", "replace") if isinstance(key, (bytes, bytearray))
                                else (None if key is None else str(key))),
                        "value": (val.decode("utf-8", "replace") if isinstance(val, (bytes, bytearray))
                                  else (None if val is None else str(val))),
                    })
            return out[-limit:]
        except Exception:  # noqa: BLE001
            return []
        finally:
            try: c.close()
            except Exception: pass
